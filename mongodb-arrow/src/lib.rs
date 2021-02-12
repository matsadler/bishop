mod bson_ext;

use arrow::{
    array::{
        BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, SchemaRef},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};
use mongodb::bson::{document::ValueAccessError, Bson, Document};

use crate::bson_ext::BsonGetNested;

pub struct DocumentsReader {
    documents: Vec<Document>,
    schema: SchemaRef,
}

// Error message to use with Result::expect() for the various Arrow builder
// methods. These methods return a Result, but as far as I can tell they won't
// ever error. Not sure if this will change, or there is a small subset that
// will error and it's to keep the API consistant? Anyway, if you ever see this
// message it's a bug and proper error handling needs to be implemented.
static INFALLIBLE: &str = "builder result expected to always be Ok(())";

impl DocumentsReader {
    pub fn new(documents: Vec<Document>, schema: SchemaRef) -> DocumentsReader {
        DocumentsReader { documents, schema }
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        let mut builder =
            StructBuilder::from_fields(self.schema.fields().clone(), self.documents.len());

        for (i, field) in self.schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Utf8 => {
                    let field_builder = builder
                        .field_builder::<StringBuilder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested(mongodb_name(field)) {
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
                DataType::Int32 => {
                    let field_builder = builder
                        .field_builder::<Int32Builder>(i)
                        .expect("field index out of range");
                    for doc in self.documents.iter() {
                        match doc.get_nested_i32(mongodb_name(field)) {
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
                        match doc.get_nested_i64(mongodb_name(field)) {
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
                        match doc.get_nested_f64(mongodb_name(field)) {
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
                        match doc.get_nested_bool(mongodb_name(field)) {
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

pub fn mongodb_name(field: &Field) -> &str {
    field
        .metadata()
        .as_ref()
        .and_then(|m| m.get("mongodb"))
        .unwrap_or_else(|| field.name())
}

#[inline]
fn arrow_error<T>(e: T) -> Result<RecordBatch>
where
    T: std::error::Error + Send + Sync + 'static,
{
    Err(ArrowError::from_external_error(Box::new(e)))
}
