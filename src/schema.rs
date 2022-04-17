use crate::error::Error;
use serde::Deserialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Schema {
    pub regex: String,
    pub table: String,
    pub columns: Vec<Column>,
}

impl Schema {
    /// Ensures
    /// - only strings can be multiline enabled
    /// - only one multiline column allowed
    fn validate(&self) -> Result<(), Error> {
        let mut multiline_enabled = false;

        for column in &self.columns {
            if column.multiline && column.r#type != ColumnType::String {
                return Err(Error::InvalidMultilineType(
                    column.name.clone(),
                    column.r#type,
                ));
            }

            if column.multiline {
                if multiline_enabled {
                    // found more than one multiline column
                    let columns = self
                        .columns
                        .iter()
                        .filter(|col| col.multiline)
                        .map(|col| col.name.clone())
                        .collect();
                    return Err(Error::TooManyMultilineColumns(columns));
                } else {
                    multiline_enabled = true;
                }
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

#[cfg(test)]
impl Column {
    /// Creates a column definition
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Column {
        Column {
            name: name.into(),
            r#type: column_type,
            multiline: false,
        }
    }

    /// Creates a multiline string definition
    pub fn multiline_string(name: impl Into<String>) -> Column {
        Column {
            name: name.into(),
            r#type: ColumnType::String,
            multiline: true,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Copy, Clone)]
pub enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "i32")]
    Int32,
    #[serde(alias = "i64")]
    Int64,
    #[serde(alias = "bool")]
    Bool,
    #[serde(alias = "f32")]
    Float,
    #[serde(alias = "f64")]
    Double,
    #[serde(alias = "datetime")]
    DateTime,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            ColumnType::String => "string",
            ColumnType::Int32 => "i32",
            ColumnType::Int64 => "i64",
            ColumnType::Bool => "bool",
            ColumnType::Float => "f32",
            ColumnType::Double => "f64",
            ColumnType::DateTime => "datetime",
        };
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
table: logs
columns:
    - name: string
      type: string
      multiline: true
    - name: i32
      type: i32
    - name: i64
      type: i64
    - name: bool
      type: bool
    - name: f32
      type: f32
    - name: f64
      type: f64
    - name: datetime
      type: datetime
";
        let schema = Schema::try_from(raw).unwrap();
        let expected = Schema {
            regex: "*".to_string(),
            table: "logs".to_string(),
            columns: vec![
                Column::multiline_string("string"),
                Column::new("i32", ColumnType::Int32),
                Column::new("i64", ColumnType::Int64),
                Column::new("bool", ColumnType::Bool),
                Column::new("f32", ColumnType::Float),
                Column::new("f64", ColumnType::Double),
                Column::new("datetime", ColumnType::DateTime),
            ],
        };

        assert_eq!(expected, schema);
    }

    #[test]
    fn parse_invalid_multiline() {
        let cases = [
            ("i32", ColumnType::Int32),
            ("i64", ColumnType::Int64),
            ("bool", ColumnType::Bool),
            ("f32", ColumnType::Float),
            ("f64", ColumnType::Double),
            ("datetime", ColumnType::DateTime),
        ];

        for case in cases {
            let raw = format!(
                "
regex: '*'
table: logs
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
            if let Err(Error::InvalidMultilineType(name, r#type)) = schema {
                assert_eq!(case.0.to_owned(), name);
                assert_eq!(case.1, r#type);
            } else {
                panic!(
                    "Error should be Error::InvalidMultilineType. Actual error: {:?}",
                    schema.unwrap_err()
                );
            }
        }
    }

    #[test]
    fn parse_invalid_multiple_multiline() {
        let raw = "
regex: '*'
table: logs
columns:
    - name: string1
      type: string
      multiline: true
    - name: string2
      type: string
      multiline: true
";
        let schema = Schema::try_from(raw);
        assert!(schema.is_err());
        if let Err(Error::TooManyMultilineColumns(columns)) = schema {
            assert_eq!(
                columns,
                vec![String::from("string1"), String::from("string2")]
            );
        } else {
            panic!(
                "Error should be Error::TooManyMultilineColumns. Actual error: {:?}",
                schema.unwrap_err()
            );
        }
    }
}
