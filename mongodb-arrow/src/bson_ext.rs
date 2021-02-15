use std::iter::Peekable;

use mongodb::bson::{
    document::{ValueAccessError, ValueAccessResult},
    Bson, Document,
};

pub trait BsonGetNested {
    fn get_nested(&self, key: &str) -> ValueAccessResult<&Bson>;
}

impl BsonGetNested for Document {
    fn get_nested(&self, key: &str) -> ValueAccessResult<&Bson> {
        get_nested(self, key.split('.').peekable())
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
