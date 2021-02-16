mod bson_ext;

use std::{collections::HashMap, ops::Deref};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        StructArray, StructBuilder, TimestampNanosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
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
            metadata,
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

struct FieldInfo {
    index: usize,
    mongodb_field: String,
    data_type: DataType,
    is_nullable: bool,
}

pub struct DocumentBuilder {
    builder: StructBuilder,
    field_info: Vec<FieldInfo>,
}

// Error message to use with Result::expect() for the various Arrow builder
// methods. These methods return a Result, but as far as I can tell they won't
// ever error. Not sure if this will change, or there is a small subset that
// will error and it's to keep the API consistant? Anyway, if you ever see this
// message it's a bug and proper error handling needs to be implemented.
static INFALLIBLE: &str = "builder result expected to always be Ok(())";

macro_rules! append_value {
    ($builder_type:ty, $struct_builder:expr, $field:ident, $doc:ident, $errors:ident { $($p:pat => $e:expr,)+ }) => {
        {
            let builder = $struct_builder
                .field_builder::<$builder_type>($field.index)
                .expect("incorrect builder type for field");
            match $doc.get_nested(&$field.mongodb_field) {
                $(Ok($p) => builder.append_value($e).expect(INFALLIBLE),)+
                Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if $field.is_nullable => {
                    builder.append_null().expect(INFALLIBLE)
                }
                Ok(_) => {
                    builder.append_null().expect(INFALLIBLE);
                    $errors.push(ArrowError::from_external_error(Box::new(ValueAccessError::UnexpectedType)));
                }
                Err(e) => {
                    builder.append_null().expect(INFALLIBLE);
                    $errors.push(ArrowError::from_external_error(Box::new(e)));
                }
            }
        }
    };
}

impl DocumentBuilder {
    pub fn new(fields: Vec<MappedField>, capacity: usize) -> DocumentBuilder {
        let (fields, field_info) = fields
            .into_iter()
            .enumerate()
            .map(|(index, mapped_field)| {
                let info = FieldInfo {
                    index,
                    mongodb_field: mapped_field.mongodb_field,
                    data_type: mapped_field.field.data_type().clone(),
                    is_nullable: mapped_field.field.is_nullable(),
                };
                (mapped_field.field, info)
            })
            .unzip();
        let builder = StructBuilder::from_fields(fields, capacity);
        DocumentBuilder {
            builder,
            field_info,
        }
    }

    pub fn append_value(&mut self, doc: Document) -> Result<(), Vec<ArrowError>> {
        let mut errors = Vec::new();

        for field in self.field_info.iter() {
            match field.data_type {
                DataType::Utf8 => append_value!(StringBuilder, self.builder, field, doc, errors {
                    Bson::ObjectId(oid) => &oid.to_string(),
                    Bson::String(val) => &val,
                    Bson::Symbol(val) => &val,
                }),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    append_value!(TimestampNanosecondBuilder, self.builder, field, doc, errors {
                        Bson::DateTime(val) => val.timestamp_nanos(),
                    })
                }
                DataType::Int32 => append_value!(Int32Builder, self.builder, field, doc, errors {
                    Bson::Int32(val) => *val,
                }),
                DataType::Int64 => append_value!(Int64Builder, self.builder, field, doc, errors {
                    Bson::Int64(val) => *val,
                }),
                DataType::Float64 => {
                    append_value!(Float64Builder, self.builder, field, doc, errors {
                        Bson::Double(val) => *val,
                    })
                }
                DataType::Boolean => {
                    append_value!(BooleanBuilder, self.builder, field, doc, errors {
                        Bson::Boolean(val) => *val,
                    })
                }
                ref data_type => panic!(
                    "{} not supported in mongodb_arrow::DocumentBuilder",
                    data_type
                ),
            }
        }
        let success = errors.is_empty();
        self.builder.append(success).expect(INFALLIBLE);
        if success {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn is_empty(&self) -> bool {
        self.builder.len() == 0
    }

    pub fn finish(&mut self) -> StructArray {
        self.builder.finish()
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

    pub fn into_record_batch(self) -> Result<RecordBatch, ArrowError> {
        let mut builder = DocumentBuilder::new(self.fields, self.documents.len());
        for document in self.documents {
            builder
                .append_value(document)
                .map_err(|errors| errors.into_iter().next().expect("empty errors"))?;
        }
        Ok(RecordBatch::from(&builder.finish()))
    }
}
