use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    datasource::{datasource::Statistics, TableProvider},
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_plan::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream},
};
use futures::stream::{Fuse, Stream, StreamExt};
use mongodb::{
    bson::{Bson, Document},
    options::FindOptions,
    Collection, Cursor,
};
use mongodb_arrow::{DocumentsReader, MappedField, MappedSchema};
use tokio::sync::Mutex as TokioMutex;

pub struct MongoDbCollection {
    collection: Collection,
    mapped_schema: MappedSchema,
    schema: SchemaRef,
}

impl MongoDbCollection {
    pub fn new(collection: Collection, mapped_schema: MappedSchema) -> Self {
        Self {
            collection,
            mapped_schema: mapped_schema.clone(),
            schema: Arc::new(mapped_schema.into()),
        }
    }
}

impl TableProvider for MongoDbCollection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mapped_schema = match projection {
            Some(columns) => {
                let projected_columns: Result<Vec<MappedField>> = columns
                    .iter()
                    .map(|i| {
                        if *i < self.mapped_schema.fields().len() {
                            Ok(self.mapped_schema.field(*i).clone())
                        } else {
                            Err(DataFusionError::Internal(
                                "Projection index out of range".to_string(),
                            ))
                        }
                    })
                    .collect();
                MappedSchema::new_with_metadata(
                    self.mapped_schema.mongodb_collection().to_owned(),
                    projected_columns?,
                    self.mapped_schema.metadata().clone(),
                )
            }
            None => self.mapped_schema.clone(),
        };

        Ok(Arc::new(MongoExec {
            collection: self.collection.clone(),
            mapped_schema: Arc::new(mapped_schema.clone()),
            schema: Arc::new(mapped_schema.into()),
            batch_size,
        }))
    }

    fn statistics(&self) -> Statistics {
        Default::default()
    }
}

#[derive(Debug)]
struct MongoExec {
    collection: Collection,
    mapped_schema: Arc<MappedSchema>,
    schema: SchemaRef,
    batch_size: usize,
}

#[async_trait]
impl ExecutionPlan for MongoExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let filter = None;
        let options = FindOptions::builder()
            .projection(Some(mongodb_projection(self.mapped_schema.clone())))
            .batch_size(Some(self.batch_size as u32))
            .build();
        Ok(Box::pin(MongoStream {
            cursor: TokioMutex::new(
                self.collection
                    .find(filter, options)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    .fuse(),
            ),
            mapped_schema: self.mapped_schema.clone(),
            schema: self.schema.clone(),
            batch_size: self.batch_size,
        }))
    }
}

struct MongoStream {
    cursor: TokioMutex<Fuse<Cursor>>,
    mapped_schema: Arc<MappedSchema>,
    schema: SchemaRef,
    batch_size: usize,
}

impl Stream for MongoStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut guard = match Box::pin(self.cursor.lock()).as_mut().poll(ctx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(val) => val,
        };

        if guard.is_done() {
            return Poll::Ready(None);
        }

        let mut documents = Vec::with_capacity(self.batch_size);
        loop {
            match Pin::new(&mut *guard).poll_next(ctx) {
                Poll::Pending if documents.is_empty() => break Poll::Pending,
                Poll::Pending => {
                    break Poll::Ready(Some(
                        DocumentsReader::new(documents, self.mapped_schema.fields().clone())
                            .into_record_batch(),
                    ));
                }
                Poll::Ready(Some(Ok(val))) => documents.push(val),
                Poll::Ready(Some(Err(e))) => {
                    break Poll::Ready(Some(Err(
                        DataFusionError::Execution(e.to_string()).into_arrow_external_error()
                    )))
                }
                Poll::Ready(None) if documents.is_empty() => {
                    break Poll::Ready(None);
                }
                Poll::Ready(None) => {
                    break Poll::Ready(Some(
                        DocumentsReader::new(documents, self.mapped_schema.fields().clone())
                            .into_record_batch(),
                    ));
                }
            }
        }
    }
}

impl RecordBatchStream for MongoStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn mongodb_projection(schema: Arc<MappedSchema>) -> Document {
    let mut projection: Document = schema
        .fields()
        .iter()
        .map(|f| (f.mongodb_field().to_owned(), Bson::Int32(1)))
        .collect();
    // _id defaults to 1, rather than 0 like everything else, so if it's not
    // present we need to explicitly set it to 0
    projection.entry("_id".to_owned()).or_insert(Bson::Int32(0));
    projection
}
