use crate::error::Error;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashSet;

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

        if !non_existent_columns.is_empty() {
            Err(Error::MissingColumns(non_existent_columns))
        } else {
            Ok(())
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
}
