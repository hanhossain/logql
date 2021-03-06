pub mod values;

use crate::error::Error;
use crate::parser::values::{Event, Type};
use crate::schema::{ColumnType, Schema};
use chrono::prelude::*;
use regex::Regex;
use std::collections::HashSet;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Parser {
    pub schema: Schema,
    pub regex: Regex,
    pub multiline_column: Option<String>,
}

impl Parser {
    /// Create a parser from a schema
    pub fn new(schema: Schema) -> Result<Parser, Error> {
        let regex = Regex::new(&schema.regex)?;
        let multiline_column = schema
            .columns
            .iter()
            .filter(|c| c.multiline)
            .map(|c| c.name.clone())
            .next();

        let parser = Parser {
            schema,
            regex,
            multiline_column,
        };

        parser.verify_columns_exist()?;
        Ok(parser)
    }

    /// Parse all lines
    pub fn parse<T: AsRef<str>>(&self, chunks: Vec<T>) -> Vec<Event> {
        let mut parsed = Vec::new();
        for chunk in chunks {
            for line in chunk.as_ref().lines() {
                if let Some(matched_result) = self.parse_line(line) {
                    parsed.push(matched_result);
                } else if self.multiline_column.is_some() {
                    // attempt to get extra lines only if multiline is enabled
                    if let Some(last) = parsed.last_mut() {
                        match last.extra_text.as_mut() {
                            None => last.extra_text = Some(vec![line.to_string()]),
                            Some(extra_text) => extra_text.push(line.to_string()),
                        }
                    }
                }
            }
        }

        parsed
    }

    /// Parse the capture groups into columns
    pub fn parse_line<'a>(&'a self, line: &'a str) -> Option<Event> {
        self.regex.captures(line).map(|captures| {
            let values = self
                .schema
                .columns
                .iter()
                .map(|column| {
                    let column_name = column.name.as_str();
                    let value = captures.name(column_name).unwrap().as_str();
                    let value = match column.r#type {
                        ColumnType::String => Type::String(value.to_string()),
                        ColumnType::Int32 => Type::Int32(i32::from_str(value).unwrap()),
                        ColumnType::Int64 => Type::Int64(i64::from_str(value).unwrap()),
                        ColumnType::Bool => Type::Bool(bool::from_str(value).unwrap()),
                        ColumnType::Float => Type::Float(f32::from_str(value).unwrap()),
                        ColumnType::Double => Type::Double(f64::from_str(value).unwrap()),
                        ColumnType::DateTime => Type::DateTime(DateTime::from_str(value).unwrap()),
                    };

                    (column_name.to_string(), value)
                })
                .collect();

            Event {
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
    use std::collections::HashMap;

    #[test]
    fn create_parser() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("index", ColumnType::String),
                Column::new("string_value", ColumnType::String),
                Column::new("double_value", ColumnType::String),
            ],
        };

        let _parser = Parser::new(schema).unwrap();
    }

    #[test]
    fn verify_columns_exist() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("index", ColumnType::String),
                Column::new("string_value", ColumnType::String),
                Column::new("double_value", ColumnType::String),
                Column::new("unknown", ColumnType::String),
            ],
        };

        assert!(Parser::new(schema).is_err());
    }

    #[test]
    fn parse_into_columns() {
        let schema = Schema {
            regex: "(?P<int_value>\\d+)\\t\
            (?P<string_value>.+)\\t\
            (?P<double_value>\\d+\\.\\d+)\\t\
            (?P<long_value>\\d+)\\t\
            (?P<bool_value>.+)\\t\
            (?P<float_value>\\d+\\.\\d+)\\t\
            (?P<timestamp>.+)"
                .to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("int_value", ColumnType::Int32),
                Column::new("string_value", ColumnType::String),
                Column::new("double_value", ColumnType::Double),
                Column::new("long_value", ColumnType::Int64),
                Column::new("bool_value", ColumnType::Bool),
                Column::new("float_value", ColumnType::Float),
                Column::new("timestamp", ColumnType::DateTime),
            ],
        };

        let int_value = 1234;
        let string_value = "this is some string";
        let double_value = 3.14;
        let long_value = i64::MAX;
        let bool_value = true;
        let float_value = 1.23;
        let timestamp = Utc::now();

        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            int_value, string_value, double_value, long_value, bool_value, float_value, timestamp
        );
        let parser = Parser::new(schema).unwrap();
        let parsed_value = parser.parse_line(&line).unwrap();

        let mut expected_values = HashMap::new();
        expected_values.insert("int_value".to_string(), Type::Int32(int_value));
        expected_values.insert(
            "string_value".to_string(),
            Type::String(string_value.to_string()),
        );
        expected_values.insert("double_value".to_string(), Type::Double(double_value));
        expected_values.insert("long_value".to_string(), Type::Int64(long_value));
        expected_values.insert("bool_value".to_string(), Type::Bool(bool_value));
        expected_values.insert("float_value".to_string(), Type::Float(float_value));
        expected_values.insert("timestamp".to_string(), Type::DateTime(timestamp));

        let expected = Event {
            values: expected_values,
            extra_text: None,
        };

        assert_eq!(expected, parsed_value);
    }

    #[test]
    fn parse_into_columns_no_match() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("index", ColumnType::String),
                Column::new("string_value", ColumnType::String),
                Column::new("double_value", ColumnType::String),
            ],
        };

        let line = "1234\t3.14159";
        let parser = Parser::new(schema).unwrap();
        let map = parser.parse_line(line);
        assert_eq!(None, map);
    }

    #[test]
    fn parse_lines_with_multiline_enabled() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("index", ColumnType::Int32),
                Column::multiline_string("string_value"),
                Column::new("double_value", ColumnType::String),
            ],
        };

        let line = "1234\tthis is some string\t3.14159\nthis is extra text";
        let parser = Parser::new(schema).unwrap();
        let parsed_result = parser.parse(vec![line]);

        let mut expected_values = HashMap::new();
        expected_values.insert("index".to_string(), Type::Int32(1234));
        expected_values.insert(
            "string_value".to_string(),
            Type::String("this is some string".to_string()),
        );
        expected_values.insert(
            "double_value".to_string(),
            Type::String("3.14159".to_string()),
        );

        let expected = vec![Event {
            values: expected_values,
            extra_text: Some(vec!["this is extra text".to_string()]),
        }];

        assert_eq!(expected, parsed_result);
    }

    #[test]
    fn parse_lines_with_multiline_disabled() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            filename: ".*".to_string(),
            table: "log".to_string(),
            columns: vec![
                Column::new("index", ColumnType::Int32),
                Column::new("string_value", ColumnType::String),
                Column::new("double_value", ColumnType::String),
            ],
        };

        let line = "1234\tthis is some string\t3.14159\nthis is extra text";
        let parser = Parser::new(schema).unwrap();
        let parsed_result = parser.parse(vec![line]);

        let mut expected_values = HashMap::new();
        expected_values.insert("index".to_string(), Type::Int32(1234));
        expected_values.insert(
            "string_value".to_string(),
            Type::String("this is some string".to_string()),
        );
        expected_values.insert(
            "double_value".to_string(),
            Type::String("3.14159".to_string()),
        );

        let expected = vec![Event {
            values: expected_values,
            extra_text: None,
        }];

        assert_eq!(expected, parsed_result);
    }
}
