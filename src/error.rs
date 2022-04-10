use crate::schema::ColumnType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Column '{0}' is a '{1}' so it cannot be multiline. Only strings can be multiline.")]
    InvalidMultilineType(String, ColumnType),
    #[error("Invalid regex statement")]
    InvalidRegex(#[from] regex::Error),
    #[error("Schema failed to parse")]
    InvalidSchema(#[from] serde_yaml::Error),
    #[error(
    "All columns must correspond to named capture groups. Columns missing in capture groups: {0:?}"
    )]
    MissingColumns(Vec<String>),
    #[error("There can only be one multiline column. Multiline columns: {0:?}")]
    TooManyMultilineColumns(Vec<String>),
}
