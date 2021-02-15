mod bson_ext;

use std::{collections::HashMap, ops::Deref};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        StructBuilder, TimestampNanosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};
use mongodb::bson::{document::ValueAccessError, Bson, Document};

use crate::bson_ext::BsonGetNested;

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct MappedField {
    field: Field,
    mongodb_field: String,
}

impl MappedField {
    pub fn new(mongodb_field: String, field: Field) -> Self {
        Self {
            mongodb_field,
            field,
        }
    }

    pub fn mongodb_field(&self) -> &str {
        &self.mongodb_field
    }
}

impl Deref for MappedField {
    type Target = Field;

    fn deref(&self) -> &Self::Target {
        &self.field
    }
}

impl From<MappedField> for Field {
    fn from(val: MappedField) -> Self {
        val.field
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedSchema {
    mongodb_collection: String,
    fields: Vec<MappedField>,
    metadata: HashMap<String, String>,
}

impl MappedSchema {
    pub fn new(mongodb_collection: String, fields: Vec<MappedField>) -> Self {
        Self::new_with_metadata(mongodb_collection, fields, HashMap::new())
    }

    pub fn new_with_metadata(
        mongodb_collection: String,
        fields: Vec<MappedField>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            mongodb_collection,
            fields,
            metadata: metadata,
        }
    }

    pub fn mongodb_collection(&self) -> &str {
        &self.mongodb_collection
    }

    pub fn fields(&self) -> &Vec<MappedField> {
        &self.fields
    }

    pub fn field(&self, i: usize) -> &MappedField {
        &self.fields[i]
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl From<MappedSchema> for Schema {
    fn from(val: MappedSchema) -> Self {
        Self::new_with_metadata(
            val.fields.iter().map(|f| f.field.clone()).collect(),
            val.metadata.clone(),
        )
    }
}

trait FromMongodb: ArrayBuilder + Sized {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self>;

    fn from_mongodb_boxed(
        field: &MappedField,
        documents: &[Document],
    ) -> Result<Box<dyn ArrayBuilder>> {
        Ok(Box::new(Self::from_mongodb(field, documents)?))
    }
}

macro_rules! ae {
    ($e:expr) => {
        return Err(ArrowError::from_external_error(Box::new($e)))
    };
}

// Error message to use with Result::expect() for the various Arrow builder
// methods. These methods return a Result, but as far as I can tell they won't
// ever error. Not sure if this will change, or there is a small subset that
// will error and it's to keep the API consistant? Anyway, if you ever see this
// message it's a bug and proper error handling needs to be implemented.
static INFALLIBLE: &str = "builder result expected to always be Ok(())";

impl FromMongodb for StringBuilder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested(field.mongodb_field()) {
                Ok(Bson::ObjectId(oid)) => {
                    builder.append_value(&oid.to_string()).expect(INFALLIBLE)
                }
                Ok(Bson::String(val)) | Ok(Bson::Symbol(val)) => {
                    builder.append_value(&val).expect(INFALLIBLE)
                }
                Ok(Bson::Null) => builder.append_null().expect(INFALLIBLE),
                Ok(_) => ae!(ValueAccessError::UnexpectedType),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

impl FromMongodb for TimestampNanosecondBuilder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested_datetime(field.mongodb_field()) {
                Ok(val) => builder
                    .append_value(val.timestamp_nanos())
                    .expect(INFALLIBLE),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

impl FromMongodb for Int32Builder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested_i32(field.mongodb_field()) {
                Ok(val) => builder.append_value(val).expect(INFALLIBLE),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

impl FromMongodb for Int64Builder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested_i64(field.mongodb_field()) {
                Ok(val) => builder.append_value(val).expect(INFALLIBLE),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

impl FromMongodb for Float64Builder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested_f64(field.mongodb_field()) {
                Ok(val) => builder.append_value(val).expect(INFALLIBLE),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

impl FromMongodb for BooleanBuilder {
    fn from_mongodb(field: &MappedField, documents: &[Document]) -> Result<Self> {
        let mut builder = Self::new(documents.len());
        for doc in documents {
            match doc.get_nested_bool(field.mongodb_field()) {
                Ok(val) => builder.append_value(val).expect(INFALLIBLE),
                Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Err(e) => ae!(e),
            };
        }
        Ok(builder)
    }
}

pub struct DocumentsReader {
    documents: Vec<Document>,
    fields: Vec<MappedField>,
}

impl DocumentsReader {
    pub fn new(documents: Vec<Document>, fields: Vec<MappedField>) -> DocumentsReader {
        DocumentsReader { documents, fields }
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        let builders = self
            .fields
            .iter()
            .map(|field| match field.data_type() {
                DataType::Utf8 => StringBuilder::from_mongodb_boxed(field, &self.documents),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    TimestampNanosecondBuilder::from_mongodb_boxed(field, &self.documents)
                }
                DataType::Int32 => Int32Builder::from_mongodb_boxed(field, &self.documents),
                DataType::Int64 => Int64Builder::from_mongodb_boxed(field, &self.documents),
                DataType::Float64 => Float64Builder::from_mongodb_boxed(field, &self.documents),
                DataType::Boolean => BooleanBuilder::from_mongodb_boxed(field, &self.documents),
                _ => ae!(ValueAccessError::UnexpectedType),
            })
            .collect::<Result<_>>()?;

        let mut builder =
            StructBuilder::new(self.fields.into_iter().map(Into::into).collect(), builders);

        for _ in self.documents {
            builder.append(true).expect(INFALLIBLE);
        }

        Ok(RecordBatch::from(&builder.finish()))
    }
}
