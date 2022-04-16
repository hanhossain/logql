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
    pub events: Vec<Event<'a>>,
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
        let columns = match &statement {
            Statement::Query(query) => match &query.body {
                SetExpr::Select(select) => {
                    let mut columns = Vec::new();

                    for projection in &select.projection {
                        let column = match projection {
                            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                                identifier.value.clone()
                            }
                            SelectItem::Wildcard => return Ok(Engine::new(parser)),
                            _ => return Err(Error::InvalidQuery(statement.clone())),
                        };

                        columns.push(column);
                    }

                    columns
                }
                _ => return Err(Error::InvalidQuery(statement.clone())),
            },
            _ => return Err(Error::InvalidQuery(statement.clone())),
        };

        Ok(Engine {
            parser,
            columns,
            statement: Some(statement),
        })
    }

    pub fn execute(&self, lines: Lines<'a>) -> Result<TableResult, Error> {
        let events = self.parser.parse(lines);
        self.project_result(events)
    }

    fn project_result(&'a self, mut events: Vec<Event<'a>>) -> Result<TableResult, Error> {
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
                                                .insert(identifier.value.as_str(), value);
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
        for event in &self.events {
            let mut result: Vec<_> = self
                .columns
                .iter()
                .map(|c| &event.values[&c.as_str()])
                .map(|t| t.to_string())
                .collect();
            if let Some(extra_text) = &event.extra_text {
                for text in extra_text {
                    let multiline_column = &mut result[self.parser.multiline_index.unwrap()];
                    multiline_column.push('\n');
                    multiline_column.push_str(text);
                }
            }
            table.add_row(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    #[test]
    fn create_engine() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
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
    fn sql_wildcard_projection() {
        let schema = "\
regex: (?P<col1>.+)\t(?P<col2>.+)
columns:
    - name: col1
      type: string
    - name: col2
      type: string
";
        let schema = Schema::try_from(schema).unwrap();
        let parser = Parser::new(schema).unwrap();
        let query = "SELECT * FROM table1";
        let engine = Engine::with_query(&parser, query.to_string()).unwrap();
        let parser_columns: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(engine.columns, parser_columns);
    }
}
