use chrono::prelude::*;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum Type<'a> {
    String(&'a str),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    Float(f32),
    Double(f64),
    DateTime(DateTime<Local>),
}

impl<'a> ToString for Type<'a> {
    fn to_string(&self) -> String {
        match self {
            Type::String(x) => x.to_string(),
            Type::Int32(x) => x.to_string(),
            Type::Int64(x) => x.to_string(),
            Type::Bool(x) => x.to_string(),
            Type::Float(x) => x.to_string(),
            Type::Double(x) => x.to_string(),
            Type::DateTime(x) => x.to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Event<'a> {
    pub values: HashMap<&'a str, Type<'a>>,
    pub extra_text: Option<Vec<&'a str>>,
}
