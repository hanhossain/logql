use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum Type<'a> {
    String(&'a str),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    Double(f64),
}

#[derive(Debug, PartialEq)]
pub struct Value<'a> {
    pub values: HashMap<&'a str, Type<'a>>,
    pub extra_text: Option<Vec<&'a str>>,
}
