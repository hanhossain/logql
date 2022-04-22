use crate::error::Error;
use crate::parser::values::{Event, Type};
use crate::parser::Parser;
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

    fn filter(mut self) -> Result<TableResult, Error> {
        if let Some(statement) = &self.statement {
            if let Statement::Query(query) = statement {
                return match &query.body {
                    SetExpr::Select(select) => match &select.selection {
                        None => Ok(self),
                        Some(Expr::BinaryOp { left, op, right }) => match (&**left, op, &**right) {
                            (Expr::Identifier(left), BinaryOperator::Eq, Expr::Value(right)) => {
                                match right {
                                    Value::SingleQuotedString(value) => {
                                        // filter all events where column == value
                                        let column = left.value.as_str();
                                        self.events = self
                                            .events
                                            .into_iter()
                                            .filter(|event| {
                                                let column_type = event.values.get(column).unwrap();
                                                if let Type::String(column_value) = column_type {
                                                    column_value == value
                                                } else {
                                                    false
                                                }
                                            })
                                            .collect();
                                        Ok(self)
                                    }
                                    _ => Err(Error::InvalidQuery(statement.clone())),
                                }
                            }
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

                        self.columns = columns.unwrap();
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
}
