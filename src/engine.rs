use crate::error::Error;
use crate::parser::values::Event;
use crate::parser::Parser;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use std::str::Lines;

pub struct Engine<'a> {
    parser: &'a Parser,
    // TODO: execute needs to use the columns
    columns: Vec<String>,
}

pub struct EngineResult<'a> {
    pub columns: Vec<String>,
    pub events: Vec<Event<'a>>,
}

impl<'a> Engine<'a> {
    pub fn new(parser: &'a Parser) -> Engine<'a> {
        let columns = parser
            .schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        Engine { parser, columns }
    }

    pub fn with_query(parser: &'a Parser, query: &'a str) -> Result<Engine<'a>, Error> {
        let dialect = GenericDialect {};
        let mut ast: Vec<Statement> = SqlParser::parse_sql(&dialect, query)?;
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

        Ok(Engine { parser, columns })
    }

    pub fn execute(&self, lines: Lines<'a>) -> EngineResult {
        let events = self.parser.parse(lines);
        EngineResult {
            columns: self.columns.clone(),
            events,
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
        let error = Engine::with_query(&parser, query).err().unwrap();
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
        let engine = Engine::with_query(&parser, query).unwrap();
        let parser_columns: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(engine.columns, parser_columns);
    }
}
