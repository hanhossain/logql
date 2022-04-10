pub mod values;

use crate::error::Error;
use crate::parser::values::{Type, Value};
use crate::schema::{ColumnType, Schema};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

pub struct Parser {
    pub schema: Schema,
    pub regex: Regex,
}

impl Parser {
    /// Create a parser from a schema
    pub fn new(schema: Schema) -> Result<Parser, Error> {
        let regex = Regex::new(&schema.regex)?;
        let parser = Parser { schema, regex };

        parser.verify_columns_exist()?;
        Ok(parser)
    }

    /// Parse the capture groups into columns
    pub fn parse_line<'a>(&'a self, line: &'a str) -> Option<Value<'a>> {
        self.regex.captures(line).map(|captures| {
            let values = self
                .schema
                .columns
                .iter()
                .map(|column| {
                    let column_name = column.name.as_str();
                    let value = captures.name(column_name).unwrap().as_str();
                    let value = match column.r#type {
                        ColumnType::String => Type::String(value),
                        ColumnType::Int32 => Type::Int32(i32::from_str(value).unwrap()),
                    };

                    (column_name, value)
                })
                .collect::<HashMap<_, _>>();

            Value {
                values,
                extra_text: None,
            }
        })
    }

    /// Verify all columns exist as capture groups
    fn verify_columns_exist(&self) -> Result<(), Error> {
        let capture_names: HashSet<_> = self.regex.capture_names().flatten().collect();
        let non_existent_columns: Vec<_> = self
            .schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .filter(|x| !capture_names.contains(x))
            .map(str::to_string)
            .collect();

        if non_existent_columns.is_empty() {
            Ok(())
        } else {
            Err(Error::MissingColumns(non_existent_columns))
        }
    }
}

impl TryFrom<&str> for Parser {
    type Error = Error;

    /// Create a parser from a schema YAML definition
    fn try_from(schema: &str) -> Result<Self, Self::Error> {
        let schema = Schema::try_from(schema)?;
        Parser::new(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnType};

    #[test]
    fn create_parser() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                Column {
                    name: "index".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "string_value".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "double_value".to_string(),
                    r#type: ColumnType::String,
                },
            ],
        };

        let _parser = Parser::new(schema).unwrap();
    }

    #[test]
    fn verify_columns_exist() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                Column {
                    name: "index".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "string_value".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "double_value".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "unknown".to_string(),
                    r#type: ColumnType::String,
                },
            ],
        };

        assert!(Parser::new(schema).is_err());
    }

    #[test]
    fn parse_into_columns() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                Column {
                    name: "index".to_string(),
                    r#type: ColumnType::Int32,
                },
                Column {
                    name: "string_value".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "double_value".to_string(),
                    r#type: ColumnType::String,
                },
            ],
        };

        let line = "1234\tthis is some string\t3.14159";
        let parser = Parser::new(schema).unwrap();
        let parsed_value = parser.parse_line(line).unwrap();

        let mut expected_values = HashMap::new();
        expected_values.insert("index", Type::Int32(1234));
        expected_values.insert("string_value", Type::String("this is some string"));
        expected_values.insert("double_value", Type::String("3.14159"));

        let expected = Value {
            values: expected_values,
            extra_text: None,
        };

        assert_eq!(expected, parsed_value);
    }

    #[test]
    fn parse_into_columns_no_match() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                Column {
                    name: "index".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "string_value".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "double_value".to_string(),
                    r#type: ColumnType::String,
                },
            ],
        };

        let line = "1234\t3.14159";
        let parser = Parser::new(schema).unwrap();
        let map = parser.parse_line(line);
        assert_eq!(None, map);
    }
}
