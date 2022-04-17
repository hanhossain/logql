use crate::error::Error;
use crate::parser::values::Event;
use crate::parser::Parser;
use comfy_table::{presets, ContentArrangement, Table};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use std::collections::HashMap;
use std::str::Lines;

pub struct Engine<'a> {
    parser: &'a Parser,
    columns: Vec<String>,
    statement: Option<Statement>,
}

pub struct TableResult<'a> {
    pub columns: Vec<String>,
    pub events: Vec<Event>,
    parser: &'a Parser,
}

impl<'a> Engine<'a> {
    pub fn new(parser: &'a Parser) -> Engine<'a> {
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

    pub fn with_query(parser: &'a Parser, query: String) -> Result<Engine<'a>, Error> {
        let dialect = GenericDialect {};
        let mut ast: Vec<Statement> = SqlParser::parse_sql(&dialect, query.as_str())?;
        if ast.len() > 1 {
            return Err(Error::TooManySqlQueries);
        }

        let statement = ast.pop().unwrap();
        let mut engine = Engine::new(parser);
        engine.statement = Some(statement);
        Ok(engine)
    }

    pub fn execute(&self, lines: Lines<'a>) -> Result<TableResult, Error> {
        let events = self.parser.parse(lines);
        self.project_result(events)
    }

    fn project_result(&'a self, mut events: Vec<Event>) -> Result<TableResult, Error> {
        if let Some(statement) = &self.statement {
            if let Statement::Query(query) = statement {
                return match &query.body {
                    SetExpr::Select(select) => {
                        let mut columns = None;
                        for event in events.iter_mut() {
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
                                    SelectItem::Wildcard => {
                                        return Ok(TableResult {
                                            columns: self.columns.clone(),
                                            events,
                                            parser: self.parser,
                                        })
                                    }
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

                        Ok(TableResult {
                            columns: columns.unwrap(),
                            events,
                            parser: self.parser,
                        })
                    }
                    _ => Err(Error::InvalidQuery(statement.clone())),
                };
            }
        }

        Ok(TableResult {
            columns: self.columns.clone(),
            events,
            parser: self.parser,
        })
    }
}

impl<'a> TableResult<'a> {
    pub fn table(&self) -> Table {
        let mut table = self.create_table();
        self.populate_table(&mut table);
        table
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
        let engine = Engine::new(&parser);
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
        let error = Engine::with_query(&parser, query.to_string())
            .err()
            .unwrap();
        match error {
            Error::SqlParserError(_) => {}
            x => panic!(
                "Error should be Error::SqlParserError. Actual error {:?}",
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
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let events: Vec<_> = vec![("1", "one"), ("2", "two")]
            .iter()
            .map(|(col1, col2)| {
                let mut values = HashMap::new();
                values.insert("col1".to_string(), Type::String(col1.to_string()));
                values.insert("col2".to_string(), Type::String(col2.to_string()));
                values
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect();
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
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]
        );

        let events: Vec<_> = vec![("1", "one", "first"), ("2", "two", "second")]
            .iter()
            .map(|(col1, col2, col3)| {
                let mut values = HashMap::new();
                values.insert("col1".to_string(), Type::String(col1.to_string()));
                values.insert("col2".to_string(), Type::String(col2.to_string()));
                values.insert("col3".to_string(), Type::String(col3.to_string()));
                values
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect();
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
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["col1".to_string(), "col3".to_string()]
        );

        let events: Vec<_> = vec![("1", "one", "first"), ("2", "two", "second")]
            .iter()
            .map(|(col1, _, col3)| {
                let mut values = HashMap::new();
                values.insert("col1".to_string(), Type::String(col1.to_string()));
                values.insert("col3".to_string(), Type::String(col3.to_string()));
                values
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect();
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
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec![
                "column1".to_string(),
                "column2".to_string(),
                "column3".to_string()
            ]
        );

        let events: Vec<_> = vec![("1", "one", "first"), ("2", "two", "second")]
            .iter()
            .map(|(col1, col2, col3)| {
                let mut values = HashMap::new();
                values.insert("column1".to_string(), Type::String(col1.to_string()));
                values.insert("column2".to_string(), Type::String(col2.to_string()));
                values.insert("column3".to_string(), Type::String(col3.to_string()));
                values
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect();
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
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let table_result = engine.execute(source.lines()).unwrap();
        assert_eq!(
            table_result.columns,
            vec!["column1".to_string(), "column3".to_string()]
        );

        let events: Vec<_> = vec![("1", "one", "first"), ("2", "two", "second")]
            .iter()
            .map(|(col1, _, col3)| {
                let mut values = HashMap::new();
                values.insert("column1".to_string(), Type::String(col1.to_string()));
                values.insert("column3".to_string(), Type::String(col3.to_string()));
                values
            })
            .map(|values| Event {
                values,
                extra_text: None,
            })
            .collect();
        assert_eq!(table_result.events, events);
    }
}
