use crate::error::Error;
use serde::Deserialize;

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Schema {
    pub regex: String,
    pub columns: Vec<Column>,
}

impl TryFrom<&str> for Schema {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let schema: Schema = serde_yaml::from_str(value)?;
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

#[derive(Debug, Deserialize, Eq, PartialEq)]
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
}
