mod datasource {
    pub mod mongodb;
}
mod bson_ext;

use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use datafusion::execution::context::ExecutionContext;
use rustyline::{error::ReadlineError, Editor};
use structopt::StructOpt;

use crate::datasource::mongodb::MongoDbCollection;

#[derive(StructOpt, Debug)]
pub struct Opts {
    /// MongoDB connection string
    #[structopt(default_value = "mongodb://localhost:27017", value_name = "URL")]
    pub mongodb: String,
    /// MongoDB database
    #[structopt(long, default_value = "test", value_name = "NAME")]
    pub db: String,
    /// Schmea directory
    #[structopt(short, long, default_value = "schema", value_name = "DIR")]
    pub schema: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::from_args();

    let mongodb_opts = mongodb::options::ClientOptions::parse(&opts.mongodb).await?;
    let client = mongodb::Client::with_options(mongodb_opts)?;
    let database = client.database(&opts.db);

    let mut context = ExecutionContext::new();

    for entry in opts.schema.read_dir()? {
        let path = entry?.path();
        let schema = read_schema(&path)?;
        let name = path
            .file_stem()
            .and_then(|e| e.to_str())
            .unwrap()
            .to_owned();
        let collection = database.collection(&name);
        let table = MongoDbCollection::new(collection, schema);
        context.register_table(&name, Box::new(table));
    }

    let mut rl = Editor::<()>::new();

    loop {
        let line = match rl.readline("> ") {
            Ok(l) => l,
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => break,
            Err(e) => return Err(e.into()),
        };

        let trimmed = line.trim_end();

        if trimmed == "quit" || trimmed == "exit" {
            break;
        }

        let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed);
        match query(&mut context, trimmed).await {
            Ok(r) => arrow::util::pretty::print_batches(&r)?,
            Err(e) => eprintln!("{}", e),
        }
    }

    Ok(())
}

async fn query(
    context: &mut ExecutionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    Ok(context.sql(sql)?.collect().await?)
}

fn read_schema<P: AsRef<Path>>(path: P) -> Result<Schema, Box<dyn std::error::Error>> {
    let file = File::open(path.as_ref())?;
    let buf_reader = BufReader::new(file);

    let schema = match path.as_ref().extension().and_then(|e| e.to_str()) {
        Some("yaml") | Some("yml") => Schema::from(&serde_yaml::from_reader(buf_reader)?)?,
        _ => Schema::from(&serde_json::from_reader(buf_reader)?)?,
    };

    // [TODO] error if schema uses any type we don't support

    Ok(schema)
}
