mod bson_ext;

use std::{collections::HashMap, ops::Deref};

use arrow::{
    array::{
        BooleanBuilder, Date64Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        StructBuilder,
    },
    datatypes::{DataType, DateUnit, Field, Schema},
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

pub struct DocumentsReader<'a> {
    documents: Vec<Document>,
    fields: &'a Vec<MappedField>,
}

// Error message to use with Result::expect() for the various Arrow builder
// methods. These methods return a Result, but as far as I can tell they won't
// ever error. Not sure if this will change, or there is a small subset that
// will error and it's to keep the API consistant? Anyway, if you ever see this
// message it's a bug and proper error handling needs to be implemented.
static INFALLIBLE: &str = "builder result expected to always be Ok(())";

impl DocumentsReader<'_> {
    pub fn new(documents: Vec<Document>, fields: &Vec<MappedField>) -> DocumentsReader {
        DocumentsReader { documents, fields }
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        let mut builder = StructBuilder::from_fields(
            self.fields.iter().map(|f| f.field.clone()).collect(),
            self.documents.len(),
        );

        for (i, field) in self.fields.into_iter().enumerate() {
            match field.data_type() {
                DataType::Utf8 => {
                    let field_builder = builder
                        .field_builder::<StringBuilder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested(field.mongodb_field()) {
                            Ok(Bson::ObjectId(oid)) => field_builder
                                .append_value(&oid.to_string())
                                .expect(INFALLIBLE),
                            Ok(Bson::String(val)) | Ok(Bson::Symbol(val)) => {
                                field_builder.append_value(&val).expect(INFALLIBLE)
                            }
                            Ok(Bson::Null) => field_builder.append_null().expect(INFALLIBLE),
                            Ok(_) => return arrow_error(ValueAccessError::UnexpectedType),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                DataType::Date64(DateUnit::Millisecond) => {
                    let field_builder = builder
                        .field_builder::<Date64Builder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_datetime(field.mongodb_field()) {
                            Ok(val) => field_builder
                                .append_value(val.timestamp_millis())
                                .expect(INFALLIBLE),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                DataType::Int32 => {
                    let field_builder = builder
                        .field_builder::<Int32Builder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_i32(field.mongodb_field()) {
                            Ok(val) => field_builder.append_value(val).expect(INFALLIBLE),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                DataType::Int64 => {
                    let field_builder = builder
                        .field_builder::<Int64Builder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_i64(field.mongodb_field()) {
                            Ok(val) => field_builder.append_value(val).expect(INFALLIBLE),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                DataType::Float64 => {
                    let field_builder = builder
                        .field_builder::<Float64Builder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_f64(field.mongodb_field()) {
                            Ok(val) => field_builder.append_value(val).expect(INFALLIBLE),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                DataType::Boolean => {
                    let field_builder = builder
                        .field_builder::<BooleanBuilder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_bool(field.mongodb_field()) {
                            Ok(val) => field_builder.append_value(val).expect(INFALLIBLE),
                            Err(ValueAccessError::NotPresent) if field.is_nullable() => {
                                field_builder.append_null().expect(INFALLIBLE)
                            }
                            Err(e) => return arrow_error(e),
                        };
                    }
                }
                _ => return arrow_error(ValueAccessError::UnexpectedType),
            }
        }

        for _ in 0..self.documents.len() {
            builder.append(true).expect(INFALLIBLE);
        }

        Ok(RecordBatch::from(&builder.finish()))
    }
}

#[inline]
fn arrow_error<T>(e: T) -> Result<RecordBatch>
where
    T: std::error::Error + Send + Sync + 'static,
{
    Err(ArrowError::from_external_error(Box::new(e)))
}
