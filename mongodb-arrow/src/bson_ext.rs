use std::iter::Peekable;

use chrono::{offset::Utc, DateTime};
use mongodb::bson::{
    document::{ValueAccessError, ValueAccessResult},
    Bson, Document,
};

pub trait BsonGetNested {
    fn get_nested(&self, key: &str) -> ValueAccessResult<&Bson>;

    fn get_nested_bool(&self, key: &str) -> ValueAccessResult<bool> {
        match self.get_nested(key)? {
            &Bson::Boolean(v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn get_nested_i32(&self, key: &str) -> ValueAccessResult<i32> {
        match self.get_nested(key)? {
            &Bson::Int32(v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn get_nested_i64(&self, key: &str) -> ValueAccessResult<i64> {
        match self.get_nested(key)? {
            &Bson::Int64(v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn get_nested_f64(&self, key: &str) -> ValueAccessResult<f64> {
        match self.get_nested(key)? {
            &Bson::Double(v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn get_nested_str(&self, key: &str) -> ValueAccessResult<&str> {
        match self.get_nested(key)? {
            &Bson::String(ref v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn get_nested_datetime(&self, key: &str) -> ValueAccessResult<&DateTime<Utc>> {
        match self.get_nested(key)? {
            &Bson::DateTime(ref v) => Ok(v),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }
}

impl BsonGetNested for Document {
    fn get_nested(&self, key: &str) -> ValueAccessResult<&Bson> {
        get_nested(self, key.split(".").peekable())
    }
}

fn get_nested<'a, 'b, I>(
    document: &'a Document,
    mut key: Peekable<I>,
) -> ValueAccessResult<&'a Bson>
where
    I: Iterator<Item = &'b str>,
{
    let current = key.next().unwrap_or("");

    if key.peek().is_some() {
        get_nested(document.get_document(current)?, key)
    } else {
        match document.get(current) {
            Some(v) => Ok(v),
            None => Err(ValueAccessError::NotPresent),
        }
    }
}
