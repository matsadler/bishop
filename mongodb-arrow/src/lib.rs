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
    is_nullable: bool,
    builder_type: BuilderType,
}

enum BuilderType {
    String,
    TimestampNanosecond,
    Int32,
    Int64,
    Float64,
    Boolean,
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

impl DocumentBuilder {
    pub fn new(fields: Vec<MappedField>, capacity: usize) -> Result<DocumentBuilder, ArrowError> {
        let field_info = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let builder_type = match field.data_type() {
                    DataType::Utf8 => BuilderType::String,
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        BuilderType::TimestampNanosecond
                    }
                    DataType::Int32 => BuilderType::Int32,
                    DataType::Int64 => BuilderType::Int64,
                    DataType::Float64 => BuilderType::Float64,
                    DataType::Boolean => BuilderType::Boolean,
                    data_type => {
                        return Err(ArrowError::SchemaError(format!(
                            "{} not supported in mongodb_arrow::DocumentBuilder",
                            data_type
                        )))
                    }
                };
                Ok(FieldInfo {
                    index,
                    mongodb_field: field.mongodb_field().to_owned(),
                    is_nullable: field.is_nullable(),
                    builder_type,
                })
            })
            .collect::<Result<_, _>>()?;
        let builder =
            StructBuilder::from_fields(fields.into_iter().map(Into::into).collect(), capacity);
        Ok(DocumentBuilder {
            builder,
            field_info,
        })
    }

    pub fn append_value(&mut self, doc: Document) -> Result<(), Vec<ArrowError>> {
        let mut errors = Vec::new();

        for field in self.field_info.iter() {
            match field.builder_type {
                BuilderType::String => {
                    if let Err(e) = handle_string(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
                BuilderType::TimestampNanosecond => {
                    if let Err(e) = handle_timestamp_nanosecond(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
                BuilderType::Int32 => {
                    if let Err(e) = handle_i32(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
                BuilderType::Int64 => {
                    if let Err(e) = handle_i64(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
                BuilderType::Float64 => {
                    if let Err(e) = handle_float64(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
                BuilderType::Boolean => {
                    if let Err(e) = handle_boolean(&mut self.builder, field, &doc) {
                        errors.push(e);
                    }
                }
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

    pub fn finish(&mut self) -> StructArray {
        self.builder.finish()
    }
}

macro_rules! ae {
    ($e:expr) => {
        return Err(ArrowError::from_external_error(Box::new($e)))
    };
}

static BUILDER_TYPE: &str = "incorrect builder type for field";

fn handle_string(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<StringBuilder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::ObjectId(oid)) => builder.append_value(&oid.to_string()).expect(INFALLIBLE),
        Ok(Bson::String(val)) | Ok(Bson::Symbol(val)) => {
            builder.append_value(&val).expect(INFALLIBLE)
        }
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
}

fn handle_timestamp_nanosecond(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<TimestampNanosecondBuilder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::DateTime(val)) => builder
            .append_value(val.timestamp_nanos())
            .expect(INFALLIBLE),
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
}

fn handle_i32(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<Int32Builder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::Int32(val)) => builder.append_value(*val).expect(INFALLIBLE),
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
}

fn handle_i64(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<Int64Builder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::Int64(val)) => builder.append_value(*val).expect(INFALLIBLE),
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
}

fn handle_float64(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<Float64Builder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::Double(val)) => builder.append_value(*val).expect(INFALLIBLE),
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
}

fn handle_boolean(
    builder: &mut StructBuilder,
    field: &FieldInfo,
    doc: &Document,
) -> Result<(), ArrowError> {
    let builder = builder
        .field_builder::<BooleanBuilder>(field.index)
        .expect(BUILDER_TYPE);
    match doc.get_nested(&field.mongodb_field) {
        Ok(Bson::Boolean(val)) => builder.append_value(*val).expect(INFALLIBLE),
        Ok(Bson::Null) | Err(ValueAccessError::NotPresent) if field.is_nullable => {
            builder.append_null().expect(INFALLIBLE)
        }
        Ok(_) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(ValueAccessError::UnexpectedType);
        }
        Err(e) => {
            builder.append_null().expect(INFALLIBLE);
            ae!(e);
        }
    };
    Ok(())
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
        let mut builder = DocumentBuilder::new(self.fields, self.documents.len())?;
        for document in self.documents {
            builder
                .append_value(document)
                .map_err(|errors| errors.into_iter().next().expect("empty errors"))?;
        }
        Ok(RecordBatch::from(&builder.finish()))
    }
}
