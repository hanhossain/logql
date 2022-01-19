use crate::error::Error;
use regex::Regex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Deserialize)]
pub struct Schema {
    pub regex: String,
    pub columns: Vec<String>,
}

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
    pub fn parse_line<'a>(&'a self, line: &'a str) -> Option<HashMap<&'a str, &'a str>> {
        self.regex.captures(line).map(|captures| {
            self.schema
                .columns
                .iter()
                .map(|column| (column.as_str(), captures.name(column).unwrap().as_str()))
                .collect::<HashMap<_, _>>()
        })
    }

    /// Verify all columns exist as capture groups
    fn verify_columns_exist(&self) -> Result<(), Error> {
        let capture_names: HashSet<_> = self.regex.capture_names().flatten().collect();
        let non_existent_columns: Vec<_> = self
            .schema
            .columns
            .iter()
            .map(String::as_str)
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
        let schema: Schema = serde_yaml::from_str(schema)?;
        Parser::new(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_parser() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                "index".to_string(),
                "string_value".to_string(),
                "double_value".to_string(),
            ],
        };

        let _parser = Parser::new(schema).unwrap();
    }

    #[test]
    fn verify_columns_exist() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                "index".to_string(),
                "string_value".to_string(),
                "double_value".to_string(),
                "unknown".to_string(),
            ],
        };

        assert!(Parser::new(schema).is_err());
    }

    #[test]
    fn parse_into_columns() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                "index".to_string(),
                "string_value".to_string(),
                "double_value".to_string(),
            ],
        };

        let line = "1234\tthis is some string\t3.14159";
        let parser = Parser::new(schema).unwrap();
        let map = parser.parse_line(line).unwrap();
        assert_eq!("1234", map["index"]);
        assert_eq!("this is some string", map["string_value"]);
        assert_eq!("3.14159", map["double_value"]);
    }

    #[test]
    fn parse_into_columns_no_match() {
        let schema = Schema {
            regex: r"(?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)".to_string(),
            columns: vec![
                "index".to_string(),
                "string_value".to_string(),
                "double_value".to_string(),
            ],
        };

        let line = "1234\t3.14159";
        let parser = Parser::new(schema).unwrap();
        let map = parser.parse_line(line);
        assert_eq!(None, map);
    }
}
