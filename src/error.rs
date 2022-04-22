use crate::parser::values::Type;
use crate::schema::ColumnType;
use sqlparser::ast::Statement;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Column '{0}' is a '{1}' so it cannot be multiline. Only strings can be multiline.")]
    InvalidMultilineType(String, ColumnType),
    #[error("The SQL query was invalid. Query: {0:#?}")]
    InvalidQuery(Statement),
    #[error("Invalid regex statement")]
    InvalidRegex(#[from] regex::Error),
    #[error("Schema failed to parse")]
    InvalidSchema(#[from] serde_yaml::Error),
    #[error("The SQL was invalid.")]
    InvalidSqlQuery,
    #[error(
    "All columns must correspond to named capture groups. Columns missing in capture groups: {0:?}"
    )]
    MissingColumns(Vec<String>),
    #[error("Failed to parse SQL statement")]
    SqlParserError(#[from] sqlparser::parser::ParserError),
    #[error("There can only be one multiline column. Multiline columns: {0:?}")]
    TooManyMultilineColumns(Vec<String>),
    #[error("There are too many SQL statements. The max allowed is one statement.")]
    TooManySqlQueries,
    #[error("There was a type mismatch. Schema type = {0}. Value = {1:?}. Query: {2:?}")]
    TypeMismatch(ColumnType, Type, Statement),
}
