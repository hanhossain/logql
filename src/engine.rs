use crate::error::Error;
use crate::parser::values::{Event, Type};
use crate::parser::Parser;
use crate::schema::ColumnType;
use chrono::prelude::*;
use comfy_table::{presets, ContentArrangement, Table};
use serde::Serialize;
use sqlparser::ast::{BinaryOperator, Expr, Offset, SelectItem, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use std::collections::HashMap;
use std::str::{FromStr, Lines};

pub struct Engine {
    parser: Parser,
    columns: Vec<String>,
    statement: Option<Statement>,
}

impl Engine {
    pub fn new(parser: Parser) -> Engine {
        let columns = parser
            .schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        Engine {
            parser,
            columns,
            statement: None,
        }
    }

    pub fn with_query(parser: Parser, query: String) -> Result<Engine, Error> {
        let dialect = GenericDialect {};
        let mut ast: Vec<Statement> = SqlParser::parse_sql(&dialect, query.as_str())?;
        match ast.len() {
            0 => return Err(Error::InvalidSqlQuery),
            1 => (),
            _ => return Err(Error::TooManySqlQueries),
        }

        let statement = ast.pop().unwrap();
        let mut engine = Engine::new(parser);
        engine.statement = Some(statement);
        Ok(engine)
    }

    pub fn execute<'a>(&'a self, lines: Lines<'a>) -> Result<TableResult, Error> {
        let events = self.parser.parse(lines);
        let table_result = TableResult {
            columns: self.columns.clone(),
            events,
            parser: self.parser.clone(),
            statement: self.statement.clone(),
        };
        table_result.process()
    }
}

#[derive(Serialize)]
pub struct TableResult {
    pub columns: Vec<String>,
    pub events: Vec<Event>,
    #[serde(skip)]
    parser: Parser,
    #[serde(skip)]
    statement: Option<Statement>,
}

impl TableResult {
    pub fn table(&self) -> Table {
        let mut table = self.create_table();
        self.populate_table(&mut table);
        table
    }

    fn process(self) -> Result<TableResult, Error> {
        self.filter()?.project()?.offset()?.limit()
    }

    fn offset(mut self) -> Result<TableResult, Error> {
        if let Some(statement) = &self.statement {
            if let Statement::Query(query) = statement {
                match &query.offset {
                    Some(Offset {
                        value: Expr::Value(Value::Number(offset, _)),
                        ..
                    }) => {
                        let offset = usize::from_str(offset.as_str()).unwrap();
                        if offset > self.events.len() {
                            self.events.clear();
                        } else {
                            self.events = self.events[offset..].to_vec().clone();
                        }
                    }
                    Some(_) => return Err(Error::InvalidQuery(statement.clone())),
                    None => (),
                }
            }
        }
        Ok(self)
    }

    fn limit(mut self) -> Result<TableResult, Error> {
        if let Some(statement) = &self.statement {
            if let Statement::Query(query) = statement {
                match &query.limit {
                    Some(Expr::Value(Value::Number(limit, _))) => {
                        let limit = usize::from_str(limit.as_str()).unwrap();
                        let end = limit.min(self.events.len());
                        self.events = self.events[..end].to_vec().clone();
                    }
                    Some(_) => return Err(Error::InvalidQuery(statement.clone())),
                    None => (),
                }
            }
        }

        Ok(self)
    }

    fn filter(self) -> Result<TableResult, Error> {
        if let Some(statement) = self.statement.clone() {
            if let Statement::Query(query) = &statement {
                return match &query.body {
                    SetExpr::Select(select) => match &select.selection {
                        None => Ok(self),
                        Some(Expr::BinaryOp { left, op, right }) => match (&**left, op, &**right) {
                            (Expr::Identifier(column), BinaryOperator::Eq, Expr::Value(value))
                            | (Expr::Value(value), BinaryOperator::Eq, Expr::Identifier(column)) => {
                                self.filter_column_equals_literal(column.value.as_str(), value)
                            }
                            (Expr::Identifier(column), BinaryOperator::Lt, Expr::Value(value)) => {
                                self.filter_column_less_than_literal(column.value.as_str(), value)
                            }
                            (
                                Expr::Identifier(column),
                                BinaryOperator::LtEq,
                                Expr::Value(value),
                            ) => self.filter_column_less_than_or_equal_to_literal(
                                column.value.as_str(),
                                value,
                            ),
                            (Expr::Identifier(column), BinaryOperator::Gt, Expr::Value(value)) => {
                                self.filter_column_greater_than_literal(
                                    column.value.as_str(),
                                    value,
                                )
                            }
                            (
                                Expr::Identifier(column),
                                BinaryOperator::GtEq,
                                Expr::Value(value),
                            ) => self.filter_column_greater_than_or_equal_to_literal(
                                column.value.as_str(),
                                value,
                            ),
                            _ => Err(Error::InvalidQuery(statement.clone())),
                        },
                        _ => Err(Error::InvalidQuery(statement.clone())),
                    },
                    _ => Err(Error::InvalidQuery(statement.clone())),
                };
            }
        }

        Ok(self)
    }

    fn get_schema_type_for_column(&self, column: &str) -> ColumnType {
        // TODO: this can easily be simplified so we don't have to do a linear search every time
        self.parser
            .schema
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .unwrap()
            .r#type
    }

    fn filter_column_with_literal<T: Fn(ColumnType, &Type, &Value) -> Result<bool, Error>>(
        mut self,
        column: &str,
        literal: &Value,
        filter: T,
    ) -> Result<TableResult, Error> {
        let schema_type = self.get_schema_type_for_column(column);
        let events = self.events;
        let mut filtered_events = Vec::new();

        for event in events {
            let event_type = event.values.get(column).unwrap();
            let should_keep = filter(schema_type, event_type, literal)?;
            if should_keep {
                filtered_events.push(event);
            }
        }

        self.events = filtered_events;
        Ok(self)
    }

    fn filter_column_equals_literal(
        self,
        column: &str,
        literal: &Value,
    ) -> Result<TableResult, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value == literal)
            }
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value == literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value == literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value == literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value == literal)
            }
            (ColumnType::Bool, Type::Bool(value), Value::Boolean(literal)) => Ok(value == literal),
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value == literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn filter_column_less_than_literal(
        self,
        column: &str,
        literal: &Value,
    ) -> Result<TableResult, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value < literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value < literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value < literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value < literal)
            }
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value < literal)
            }
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value < literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn filter_column_greater_than_literal(
        self,
        column: &str,
        literal: &Value,
    ) -> Result<TableResult, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value > literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value > literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value > literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value > literal)
            }
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value > literal)
            }
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value > literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn filter_column_less_than_or_equal_to_literal(
        self,
        column: &str,
        literal: &Value,
    ) -> Result<TableResult, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value <= literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value <= literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value <= literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value <= literal)
            }
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value <= literal)
            }
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value <= literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn filter_column_greater_than_or_equal_to_literal(
        self,
        column: &str,
        literal: &Value,
    ) -> Result<TableResult, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value >= literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value >= literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value >= literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value >= literal)
            }
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value >= literal)
            }
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value >= literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn project(mut self) -> Result<TableResult, Error> {
        if let Some(statement) = &self.statement {
            if let Statement::Query(query) = statement {
                return match &query.body {
                    SetExpr::Select(select) => {
                        let mut columns = None;
                        for event in self.events.iter_mut() {
                            let mut projected_values = HashMap::new();
                            let mut inner_columns = Vec::new();
                            for projection in &select.projection {
                                match projection {
                                    SelectItem::UnnamedExpr(unnamed_expr) => match unnamed_expr {
                                        Expr::Identifier(identifier) => {
                                            let value = event
                                                .values
                                                .remove(identifier.value.as_str())
                                                .unwrap();
                                            projected_values
                                                .insert(identifier.value.clone(), value);
                                            if columns.is_none() {
                                                inner_columns.push(identifier.value.clone());
                                            }
                                        }
                                        _ => return Err(Error::InvalidQuery(statement.clone())),
                                    },
                                    SelectItem::Wildcard => return Ok(self),
                                    SelectItem::ExprWithAlias {
                                        expr: Expr::Identifier(identifier),
                                        alias,
                                    } => {
                                        let value =
                                            event.values.remove(identifier.value.as_str()).unwrap();
                                        projected_values.insert(alias.value.clone(), value);
                                        if columns.is_none() {
                                            inner_columns.push(alias.value.clone());
                                        }
                                    }
                                    _ => return Err(Error::InvalidQuery(statement.clone())),
                                }
                            }
                            event.values = projected_values;
                            if columns.is_none() {
                                columns = Some(inner_columns);
                            }
                        }

                        if let Some(columns) = columns {
                            self.columns = columns;
                        }
                        Ok(self)
                    }
                    _ => Err(Error::InvalidQuery(statement.clone())),
                };
            }
        }

        Ok(self)
    }

    fn create_table(&self) -> Table {
        let mut table = Table::new();
        let header: Vec<_> = self.columns.iter().map(|c| c.to_owned()).collect();
        table
            .load_preset(presets::UTF8_FULL)
            .set_content_arrangement(ContentArrangement::DynamicFullWidth)
            .set_header(header);
        table
    }

    fn populate_table(&self, table: &mut Table) {
        let multiline_index = self
            .parser
            .multiline_column
            .as_ref()
            .map(|c| c.as_str())
            .map(|multiline_column| {
                self.columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.as_str() == multiline_column)
                    .map(|(idx, _)| idx)
                    .next()
            });
        for event in &self.events {
            let mut result: Vec<_> = self
                .columns
                .iter()
                .map(|c| &event.values[c])
                .map(|t| t.to_string())
                .collect();
            if let Some(extra_text) = &event.extra_text {
                if let Some(Some(multiline_index)) = multiline_index {
                    for text in extra_text {
                        let multiline_column = &mut result[multiline_index];
                        multiline_column.push('\n');
                        multiline_column.push_str(text);
                    }
                }
            }
            table.add_row(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::values::Type;
    use crate::schema::Schema;

    fn generate_events(source: &[&[(&str, &str)]]) -> Vec<Event> {
        source
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(k, v)| (k.to_string(), Type::String(v.to_string())))
                    .collect::<HashMap<_, _>>()
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect()
    }

    fn generate_typed_events(source: Vec<Vec<(&str, Type)>>) -> Vec<Event> {
        source
            .into_iter()
            .map(|row| {
                let values = row.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
                Event {
                    values,
                    extra_text: None,
                }
            })
            .collect()
    }

    #[test]
    fn create_engine() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let engine = Engine::new(parser.clone());
        let parser_columns: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(engine.columns, parser_columns);
    }

    #[test]
    fn create_with_broken_sql() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table";
        let error = Engine::with_query(parser, query.to_string()).err().unwrap();
        match error {
            Error::SqlParserError(_) => {}
            x => panic!(
                "Error should be Error::SqlParserError. Actual error {:?}",
                x
            ),
        }
    }

    #[test]
    fn create_with_empty_query() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let error = Engine::with_query(parser, "".to_string()).err().unwrap();
        match error {
            Error::InvalidSqlQuery => {}
            x => panic!(
                "Error should be Error::InvalidSqlQuery. Actual error {:?}",
                x
            ),
        }
    }

    #[test]
    fn sql_projection_wildcard() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one")].as_slice(),
                [("col1", "2"), ("col2", "two")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_projection_identifier_all() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)\t(?P<col3>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
    - name: col3
      type: string
";
        let source = "\
1\tone\tfirst
2\ttwo\tsecond
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT col1, col2, col3 FROM table1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one"), ("col3", "first")].as_slice(),
                [("col1", "2"), ("col2", "two"), ("col3", "second")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_projection_identifier_subset() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)\t(?P<col3>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
    - name: col3
      type: string
";
        let source = "\
1\tone\tfirst
2\ttwo\tsecond
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT col1, col3 FROM table1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col3".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col3", "first")].as_slice(),
                [("col1", "2"), ("col3", "second")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_projection_alias_all() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)\t(?P<col3>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
    - name: col3
      type: string
";
        let source = "\
1\tone\tfirst
2\ttwo\tsecond
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT col1 as column1, col2 as column2, col3 as column3 FROM table1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec![
                "column1".to_string(),
                "column2".to_string(),
                "column3".to_string()
            ]
        );

        let events = generate_events(
            [
                [("column1", "1"), ("column2", "one"), ("column3", "first")].as_slice(),
                [("column1", "2"), ("column2", "two"), ("column3", "second")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_projection_alias_subset() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)\t(?P<col3>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
    - name: col3
      type: string
";
        let source = "\
1\tone\tfirst
2\ttwo\tsecond
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT col1 as column1, col3 as column3 FROM table1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["column1".to_string(), "column3".to_string()]
        );

        let events = generate_events(
            [
                [("column1", "1"), ("column3", "first")].as_slice(),
                [("column1", "2"), ("column3", "second")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_limit_all() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 3";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one")].as_slice(),
                [("col1", "2"), ("col2", "two")].as_slice(),
                [("col1", "3"), ("col2", "three")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_limit_subset() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 2";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one")].as_slice(),
                [("col1", "2"), ("col2", "two")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_limit_greater_than_count() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 4";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one")].as_slice(),
                [("col1", "2"), ("col2", "two")].as_slice(),
                [("col1", "3"), ("col2", "three")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_offset() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 OFFSET 1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "2"), ("col2", "two")].as_slice(),
                [("col1", "3"), ("col2", "three")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_offset_greater_than_count() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 OFFSET 4";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        assert_eq!(table_result.events.len(), 0);
    }

    #[test]
    fn sql_limit_offset_all() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 3 OFFSET 0";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "1"), ("col2", "one")].as_slice(),
                [("col1", "2"), ("col2", "two")].as_slice(),
                [("col1", "3"), ("col2", "three")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_limit_offset_subset() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 2 OFFSET 1";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events = generate_events(
            [
                [("col1", "2"), ("col2", "two")].as_slice(),
                [("col1", "3"), ("col2", "three")].as_slice(),
            ]
            .as_slice(),
        );
        assert_eq!(table_result.events, events);
    }

    #[test]
    fn sql_limit_offset_greater_than_count() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
table: logs
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let source = "\
1\tone
2\ttwo
3\tthree
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1 LIMIT 2 OFFSET 3";
        let engine = Engine::with_query(parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        assert_eq!(table_result.events.len(), 0);
    }

    #[test]
    fn sql_where_column_equals_literal() {
        let schema = "\
regex: (?P<col1>.+)\
    \t(?P<col2>.+)\
    \t(?P<col3>.+)\
    \t(?P<col4>.+)\
    \t(?P<col5>.+)\
    \t(?P<col6>.+)\
    \t(?P<col7>.+)
table: logs
columns:
    - name: col1
      type: i32
    - name: col2
      type: string
    - name: col3
      type: f32
    - name: col4
      type: f64
    - name: col5
      type: i64
    - name: col6
      type: bool
    - name: col7
      type: datetime
";
        let source = "\
1\tone\t1.0\t1.0\t1234\tfalse\t2022-01-01T00:00:00Z
2\ttwo\t2.5\t3.1\t2147483647\ttrue\t2022-01-02T00:00:00Z
3\tthree\t3.0\t1.0\t567\tfalse\t2022-01-03T00:00:00Z
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();

        let events = generate_typed_events(vec![vec![
            ("col1", Type::Int32(2)),
            ("col2", Type::String("two".to_string())),
            ("col3", Type::Float(2.5)),
            ("col4", Type::Double(3.1)),
            ("col5", Type::Int64(2147483647)),
            ("col6", Type::Bool(true)),
            ("col7", Type::DateTime(Utc.ymd(2022, 1, 2).and_hms(0, 0, 0))),
        ]]);
        let columns: Vec<_> = vec!["col1", "col2", "col3", "col4", "col5", "col6", "col7"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        let queries = vec![
            "SELECT * FROM table1 WHERE col1 = 2",
            "SELECT * FROM table1 WHERE 2 = col1",
            "SELECT * FROM table1 WHERE col2 = 'two'",
            "SELECT * FROM table1 WHERE 'two' = col2",
            "SELECT * FROM table1 WHERE col3 = 2.5",
            "SELECT * FROM table1 WHERE 2.5 = col3",
            "SELECT * FROM table1 WHERE col4 = 3.1",
            "SELECT * FROM table1 WHERE 3.1 = col4",
            "SELECT * FROM table1 WHERE 3.1 = col4",
            "SELECT * FROM table1 WHERE col5 = 2147483647",
            "SELECT * FROM table1 WHERE 2147483647 = col5",
            "SELECT * FROM table1 WHERE col6 = true",
            "SELECT * FROM table1 WHERE true = col6",
            "SELECT * FROM table1 WHERE col7 = '2022-01-02T00:00:00Z'",
            "SELECT * FROM table1 WHERE '2022-01-02T00:00:00Z' = col7",
            "SELECT * FROM table1 WHERE col7 = '2022-01-02T01:00:00+01:00'",
            "SELECT * FROM table1 WHERE '2022-01-02T01:00:00+01:00' = col7",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.columns, columns);
            assert_eq!(table_result.events, events);
        }
    }

    #[test]
    fn sql_where_column_less_than_literal() {
        let schema = "\
regex: (?P<i32>.+)\t(?P<string>.+)\t(?P<i64>.+)\t(?P<f32>.+)\t(?P<f64>.+)\t(?P<datetime>.+)
table: logs
columns:
    - name: i32
      type: i32
    - name: string
      type: string
    - name: i64
      type: i64
    - name: f32
      type: f32
    - name: f64
      type: f64
    - name: datetime
      type: datetime
";
        let source = "\
1\ta\t1000\t1.1\t11.11\t2022-01-01T00:00:00Z
2\tb\t2000\t2.2\t22.22\t2022-01-02T00:00:00Z
3\tc\t3000\t3.3\t33.33\t2022-01-03T00:00:00Z
";

        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();

        let events = generate_typed_events(vec![vec![
            ("i32", Type::Int32(1)),
            ("string", Type::String("a".to_string())),
            ("i64", Type::Int64(1000)),
            ("f32", Type::Float(1.1)),
            ("f64", Type::Double(11.11)),
            (
                "datetime",
                Type::DateTime(Utc.ymd(2022, 1, 1).and_hms(0, 0, 0)),
            ),
        ]]);

        let queries = vec![
            "select * from logs where i32 < 2",
            "select * from logs where i64 < 2000",
            "select * from logs where f32 < 2.2",
            "select * from logs where f64 < 22.22",
            "select * from logs where datetime < '2022-01-02T00:00:00Z'",
            "select * from logs where string < 'b'",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.events, events);
        }
    }

    #[test]
    fn sql_where_column_less_than_or_equal_to_literal() {
        let schema = "\
regex: (?P<i32>.+)\t(?P<string>.+)\t(?P<i64>.+)\t(?P<f32>.+)\t(?P<f64>.+)\t(?P<datetime>.+)
table: logs
columns:
    - name: i32
      type: i32
    - name: string
      type: string
    - name: i64
      type: i64
    - name: f32
      type: f32
    - name: f64
      type: f64
    - name: datetime
      type: datetime
";
        let source = "\
1\ta\t1000\t1.1\t11.11\t2022-01-01T00:00:00Z
2\tb\t2000\t2.2\t22.22\t2022-01-02T00:00:00Z
3\tc\t3000\t3.3\t33.33\t2022-01-03T00:00:00Z
";

        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();

        let events = generate_typed_events(vec![
            vec![
                ("i32", Type::Int32(1)),
                ("string", Type::String("a".to_string())),
                ("i64", Type::Int64(1000)),
                ("f32", Type::Float(1.1)),
                ("f64", Type::Double(11.11)),
                (
                    "datetime",
                    Type::DateTime(Utc.ymd(2022, 1, 1).and_hms(0, 0, 0)),
                ),
            ],
            vec![
                ("i32", Type::Int32(2)),
                ("string", Type::String("b".to_string())),
                ("i64", Type::Int64(2000)),
                ("f32", Type::Float(2.2)),
                ("f64", Type::Double(22.22)),
                (
                    "datetime",
                    Type::DateTime(Utc.ymd(2022, 1, 2).and_hms(0, 0, 0)),
                ),
            ],
        ]);

        let queries = vec![
            "select * from logs where i32 <= 2",
            "select * from logs where i64 <= 2000",
            "select * from logs where f32 <= 2.2",
            "select * from logs where f64 <= 22.22",
            "select * from logs where datetime <= '2022-01-02T00:00:00Z'",
            "select * from logs where string <= 'b'",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.events, events);
        }
    }

    #[test]
    fn sql_where_column_greater_than_literal() {
        let schema = "\
regex: (?P<i32>.+)\t(?P<string>.+)\t(?P<i64>.+)\t(?P<f32>.+)\t(?P<f64>.+)\t(?P<datetime>.+)
table: logs
columns:
    - name: i32
      type: i32
    - name: string
      type: string
    - name: i64
      type: i64
    - name: f32
      type: f32
    - name: f64
      type: f64
    - name: datetime
      type: datetime
";
        let source = "\
1\ta\t1000\t1.1\t11.11\t2022-01-01T00:00:00Z
2\tb\t2000\t2.2\t22.22\t2022-01-02T00:00:00Z
3\tc\t3000\t3.3\t33.33\t2022-01-03T00:00:00Z
";

        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();

        let events = generate_typed_events(vec![vec![
            ("i32", Type::Int32(3)),
            ("string", Type::String("c".to_string())),
            ("i64", Type::Int64(3000)),
            ("f32", Type::Float(3.3)),
            ("f64", Type::Double(33.33)),
            (
                "datetime",
                Type::DateTime(Utc.ymd(2022, 1, 3).and_hms(0, 0, 0)),
            ),
        ]]);

        let queries = vec![
            "select * from logs where i32 > 2",
            "select * from logs where i64 > 2000",
            "select * from logs where f32 > 2.2",
            "select * from logs where f64 > 22.22",
            "select * from logs where datetime > '2022-01-02T00:00:00Z'",
            "select * from logs where string > 'b'",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.events, events);
        }
    }

    #[test]
    fn sql_where_column_greater_than_or_equal_to_literal() {
        let schema = "\
regex: (?P<i32>.+)\t(?P<string>.+)\t(?P<i64>.+)\t(?P<f32>.+)\t(?P<f64>.+)\t(?P<datetime>.+)
table: logs
columns:
    - name: i32
      type: i32
    - name: string
      type: string
    - name: i64
      type: i64
    - name: f32
      type: f32
    - name: f64
      type: f64
    - name: datetime
      type: datetime
";
        let source = "\
1\ta\t1000\t1.1\t11.11\t2022-01-01T00:00:00Z
2\tb\t2000\t2.2\t22.22\t2022-01-02T00:00:00Z
3\tc\t3000\t3.3\t33.33\t2022-01-03T00:00:00Z
";

        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();

        let events = generate_typed_events(vec![
            vec![
                ("i32", Type::Int32(2)),
                ("string", Type::String("b".to_string())),
                ("i64", Type::Int64(2000)),
                ("f32", Type::Float(2.2)),
                ("f64", Type::Double(22.22)),
                (
                    "datetime",
                    Type::DateTime(Utc.ymd(2022, 1, 2).and_hms(0, 0, 0)),
                ),
            ],
            vec![
                ("i32", Type::Int32(3)),
                ("string", Type::String("c".to_string())),
                ("i64", Type::Int64(3000)),
                ("f32", Type::Float(3.3)),
                ("f64", Type::Double(33.33)),
                (
                    "datetime",
                    Type::DateTime(Utc.ymd(2022, 1, 3).and_hms(0, 0, 0)),
                ),
            ],
        ]);

        let queries = vec![
            "select * from logs where i32 >= 2",
            "select * from logs where i64 >= 2000",
            "select * from logs where f32 >= 2.2",
            "select * from logs where f64 >= 22.22",
            "select * from logs where datetime >= '2022-01-02T00:00:00Z'",
            "select * from logs where string >= 'b'",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.events, events);
        }
    }
}
