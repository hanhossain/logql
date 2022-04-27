use chrono::prelude::*;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone, Serialize, PartialOrd)]
pub enum Type {
    String(String),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    Float(f32),
    Double(f64),
    DateTime(DateTime<Utc>),
}

impl ToString for Type {
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

impl From<&str> for Type {
    fn from(value: &str) -> Self {
        Type::String(value.to_string())
    }
}

impl From<i32> for Type {
    fn from(value: i32) -> Self {
        Type::Int32(value)
    }
}

impl From<i64> for Type {
    fn from(value: i64) -> Self {
        Type::Int64(value)
    }
}

impl From<bool> for Type {
    fn from(value: bool) -> Self {
        Type::Bool(value)
    }
}

impl From<f32> for Type {
    fn from(value: f32) -> Self {
        Type::Float(value)
    }
}

impl From<f64> for Type {
    fn from(value: f64) -> Self {
        Type::Double(value)
    }
}

impl From<DateTime<Utc>> for Type {
    fn from(value: DateTime<Utc>) -> Self {
        Type::DateTime(value)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct Event {
    pub values: HashMap<String, Type>,
    pub extra_text: Option<Vec<String>>,
}
