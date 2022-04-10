use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Schema {
    pub regex: String,
    pub columns: Vec<Column>,
}

impl Schema {
    /// Ensures only strings can be multiline enabled
    fn validate(&self) -> Result<(), Error> {
        for column in &self.columns {
            if column.multiline && column.r#type != ColumnType::String {
                return Err(Error::InvalidMultilineSchema(
                    column.name.clone(),
                    column.r#type,
                ));
            }
        }

        Ok(())
    }
}

impl TryFrom<&str> for Schema {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let schema: Schema = serde_yaml::from_str(value)?;
        schema.validate()?;
        Ok(schema)
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub r#type: ColumnType,
    #[serde(default)]
    pub multiline: bool,
}

impl Column {
    /// Creates a column definition
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Column {
        Column {
            name: name.into(),
            r#type: column_type,
            multiline: false,
        }
    }

    /// Creates a multiline string column definition
    pub fn multiline_string(name: impl Into<String>) -> Column {
        Column {
            name: name.into(),
            r#type: ColumnType::String,
            multiline: true,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Copy, Clone)]
pub enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "i32")]
    Int32,
    #[serde(alias = "i64")]
    Int64,
    #[serde(alias = "bool")]
    Bool,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = serde_yaml::to_string(self).unwrap();
        f.write_str(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_schema() {
        let raw = "
regex: '*'
columns:
    - name: string
      type: string
      multiline: true
    - name: i32
      type: i32
    - name: i64
      type: i64
";
        let schema = Schema::try_from(raw).unwrap();
        let expected = Schema {
            regex: "*".to_string(),
            columns: vec![
                Column::multiline_string("string"),
                Column::new("i32", ColumnType::Int32),
                Column::new("i64", ColumnType::Int64),
            ],
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn parse_invalid_multiline() {
        let cases = [("i32", ColumnType::Int32), ("i64", ColumnType::Int64)];

        for case in cases {
            let raw = format!(
                "
regex: '*'
columns:
    - name: string
      type: string
    - name: {}
      type: {}
      multiline: true
",
                case.0, case.0
            );

            let schema = Schema::try_from(raw.as_str());
            assert!(schema.is_err());
            if let Err(Error::InvalidMultilineSchema(name, r#type)) = schema {
                assert_eq!(case.0.to_owned(), name);
                assert_eq!(case.1, r#type);
            } else {
                panic!(
                    "Error should be Error::InvalidMultilineSchema. Actual error: {:?}",
                    schema.unwrap_err()
                );
            }
        }
    }
}
