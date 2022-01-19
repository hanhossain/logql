use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid regex statement")]
    InvalidRegex(#[from] regex::Error),
    #[error("Schema failed to parse")]
    InvalidSchema(#[from] serde_yaml::Error),
    #[error(
    "All columns must correspond to named capture groups. Columns missing in capture groups: {0:?}"
    )]
    MissingColumns(Vec<String>),
}
