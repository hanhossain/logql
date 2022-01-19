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
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "i32")]
    Int32,
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
    - name: i32
      type: i32
";
        let schema = Schema::try_from(raw).unwrap();
        let expected = Schema {
            regex: "*".to_string(),
            columns: vec![
                Column {
                    name: "string".to_string(),
                    r#type: ColumnType::String,
                },
                Column {
                    name: "i32".to_string(),
                    r#type: ColumnType::Int32,
                },
            ],
        };

        assert_eq!(expected, schema);
    }
}
