use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Schema {
    pub regex: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    pub r#type: ColumnType,
}

#[derive(Debug, Deserialize)]
pub enum ColumnType {
    #[serde(alias = "string")]
    String,
}
