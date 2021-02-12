use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use arrow::{
    datatypes::{Field, Schema, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    datasource::{datasource::Statistics, MemTable, TableProvider},
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_plan::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream},
};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use pin_project::pin_project;

pub struct LazyMemTable {
    inner: Arc<ArcSwap<State>>,
}

enum State {
    Lazy(Box<dyn TableProvider + Send + Sync>),
    Loaded(MemTable),
}

impl LazyMemTable {
    pub fn new<T>(provider: T) -> LazyMemTable
    where
        T: TableProvider + Send + Sync + 'static,
    {
        LazyMemTable {
            inner: Arc::new(ArcSwap::from_pointee(State::Lazy(Box::new(provider)))),
        }
    }
}

impl TableProvider for LazyMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match **self.inner.load() {
            State::Lazy(ref v) => v.schema(),
            State::Loaded(ref v) => v.schema(),
        }
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match **self.inner.load() {
            State::Lazy(ref v) => {
                let projected_schema = match projection {
                    Some(columns) => {
                        let projected_columns: Result<Vec<Field>> = columns
                            .iter()
                            .map(|i| {
                                if *i < v.schema().fields().len() {
                                    Ok(v.schema().field(*i).clone())
                                } else {
                                    Err(DataFusionError::Internal(
                                        "Projection index out of range".to_string(),
                                    ))
                                }
                            })
                            .collect();
                        Arc::new(Schema::new(projected_columns?))
                    }
                    None => v.schema().clone(),
                };

                Ok(Arc::new(LazyExec {
                    parent: self.inner.clone(),
                    projected_schema,
                    scan_args: (projection.clone(), batch_size, filters.to_vec()),
                }))
            }
            State::Loaded(ref v) => v.scan(projection, batch_size, filters),
        }
    }

    fn statistics(&self) -> Statistics {
        match **self.inner.load() {
            State::Lazy(ref v) => v.statistics(),
            State::Loaded(ref v) => v.statistics(),
        }
    }
}

struct LazyExec {
    parent: Arc<ArcSwap<State>>,
    projected_schema: SchemaRef,
    scan_args: (Option<Vec<usize>>, usize, Vec<Expr>),
}

impl fmt::Debug for LazyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyExec")
            .field("projected_schema", &self.projected_schema)
            .field("scan_args", &self.scan_args)
            .finish()
    }
}

#[async_trait]
impl ExecutionPlan for LazyExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(&self, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        match **self.parent.load() {
            State::Lazy(ref v) => {
                // this inlines MemTable::load as the compiler got confused
                // about the TableProvider not implimenting Send + Sync

                let exec = v.scan(&None, self.scan_args.1, &[])?;
                let partition_count = exec.output_partitioning().partition_count();

                let tasks = (0..partition_count)
                    .map(|part_i| {
                        let exec = exec.clone();
                        tokio::spawn(async move {
                            let stream = exec.execute(part_i).await?;
                            stream
                                .try_collect::<Vec<_>>()
                                .await
                                .map_err(DataFusionError::from)
                        })
                    })
                    .collect::<Vec<_>>();

                let mut data: Vec<Vec<RecordBatch>> =
                    Vec::with_capacity(exec.output_partitioning().partition_count());
                for task in tasks {
                    let result = task.await.expect("MemTable::load could not join task")?;
                    data.push(result);
                }

                let mem = MemTable::try_new(v.schema().clone(), data)?;

                self.parent.swap(Arc::new(State::Loaded(mem)));
                self.execute(0).await
            }
            State::Loaded(ref v) => {
                let exec = v.scan(&self.scan_args.0, self.scan_args.1, &self.scan_args.2)?;
                let partition_count = exec.output_partitioning().partition_count();

                let mut streams = Vec::with_capacity(partition_count);
                for i in 0..partition_count {
                    streams.push(exec.execute(i).await?);
                }
                Ok(Box::pin(CombinedStream {
                    schema: self.projected_schema.clone(),
                    inner: stream::iter(streams).flatten(),
                }))
            }
        }
    }
}

#[pin_project]
struct CombinedStream<T> {
    schema: SchemaRef,
    #[pin]
    inner: T,
}

impl<T> Stream for CombinedStream<T>
where
    T: Stream<Item = ArrowResult<RecordBatch>>,
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(ctx)
    }
}

impl<T> RecordBatchStream for CombinedStream<T>
where
    T: Stream<Item = ArrowResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
