use crate::engine::TableResult;
use crate::error::Error;
use crate::parser::values::Type;
use crate::schema::ColumnType;
use chrono::{DateTime, Utc};
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
use std::collections::HashSet;
use std::str::FromStr;

impl TableResult {
    pub fn filter(mut self) -> Result<TableResult, Error> {
        if let Some(statement) = self.statement.clone() {
            let indexes: Option<HashSet<_>> = match &statement {
                Statement::Query(query) => match &query.body {
                    SetExpr::Select(select) => match &select.selection {
                        None => Ok(None),
                        Some(expr) => Ok(Some(self.process_filter(&expr, &statement)?)),
                    },
                    _ => Err(Error::InvalidQuery(statement.clone())),
                },
                _ => Err(Error::InvalidQuery(statement)),
            }?;

            if let Some(indexes) = indexes {
                let events = std::mem::replace(&mut self.events, Vec::new());
                self.events = events
                    .into_iter()
                    .enumerate()
                    .filter(|(index, _)| indexes.contains(index))
                    .map(|(_, event)| event)
                    .collect();
            }
        }

        Ok(self)
    }

    fn process_filter(
        &mut self,
        expr: &Expr,
        statement: &Statement,
    ) -> Result<HashSet<usize>, Error> {
        match expr {
            Expr::BinaryOp { left, op, right } => self.filter_binary_op(left, op, right, statement),
            _ => Err(Error::InvalidQuery(statement.clone())),
        }
    }

    fn filter_binary_op(
        &mut self,
        left: &Box<Expr>,
        op: &BinaryOperator,
        right: &Box<Expr>,
        statement: &Statement,
    ) -> Result<HashSet<usize>, Error> {
        match (&**left, &**right) {
            (Expr::Identifier(column), Expr::Value(literal)) => {
                self.route_filter_column_with_literal(column.value.as_str(), literal, op)
            }
            (Expr::Value(literal), Expr::Identifier(column)) => {
                self.route_filter_literal_with_column(literal, column.value.as_str(), op)
            }
            _ => Err(Error::InvalidQuery(statement.clone())),
        }
    }

    fn route_filter_column_with_literal(
        &mut self,
        column: &str,
        literal: &Value,
        op: &BinaryOperator,
    ) -> Result<HashSet<usize>, Error> {
        match op {
            BinaryOperator::Eq => self.filter_column_equals_literal(column, literal),
            BinaryOperator::NotEq => self.filter_column_does_not_equal_literal(column, literal),
            BinaryOperator::Gt => self.filter_column_greater_than_literal(column, literal),
            BinaryOperator::Lt => self.filter_column_less_than_literal(column, literal),
            BinaryOperator::GtEq => {
                self.filter_column_greater_than_or_equal_to_literal(column, literal)
            }
            BinaryOperator::LtEq => {
                self.filter_column_less_than_or_equal_to_literal(column, literal)
            }
            _ => Err(Error::InvalidQuery(
                self.statement.as_ref().unwrap().clone(),
            )),
        }
    }

    fn route_filter_literal_with_column(
        &mut self,
        literal: &Value,
        column: &str,
        op: &BinaryOperator,
    ) -> Result<HashSet<usize>, Error> {
        match op {
            BinaryOperator::Eq => self.filter_column_equals_literal(column, literal),
            BinaryOperator::NotEq => self.filter_column_does_not_equal_literal(column, literal),
            BinaryOperator::Gt => self.filter_column_less_than_literal(column, literal),
            BinaryOperator::Lt => self.filter_column_greater_than_literal(column, literal),
            BinaryOperator::GtEq => {
                self.filter_column_less_than_or_equal_to_literal(column, literal)
            }
            BinaryOperator::LtEq => {
                self.filter_column_greater_than_or_equal_to_literal(column, literal)
            }
            _ => Err(Error::InvalidQuery(
                self.statement.as_ref().unwrap().clone(),
            )),
        }
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
        &self,
        column: &str,
        literal: &Value,
        filter: T,
    ) -> Result<HashSet<usize>, Error> {
        let schema_type = self.get_schema_type_for_column(column);
        let mut filtered_events = HashSet::new();

        for (index, event) in self.events.iter().enumerate() {
            let event_type = event.values.get(column).unwrap();
            let should_keep = filter(schema_type, event_type, literal)?;
            if should_keep {
                filtered_events.insert(index);
            }
        }

        Ok(filtered_events)
    }

    fn filter_column_equals_literal(
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
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

    fn filter_column_does_not_equal_literal(
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
        self.filter_column_with_literal(column, literal, |schema_type, event_type, literal| match (
            schema_type,
            event_type,
            literal,
        ) {
            (ColumnType::String, Type::String(value), Value::SingleQuotedString(literal)) => {
                Ok(value != literal)
            }
            (ColumnType::Int32, Type::Int32(value), Value::Number(literal, false)) => {
                let literal = i32::from_str(literal.as_str()).unwrap();
                Ok(*value != literal)
            }
            (ColumnType::Int64, Type::Int64(value), Value::Number(literal, false)) => {
                let literal = i64::from_str(literal.as_str()).unwrap();
                Ok(*value != literal)
            }
            (ColumnType::Float, Type::Float(value), Value::Number(literal, false)) => {
                let literal = f32::from_str(literal.as_str()).unwrap();
                Ok(*value != literal)
            }
            (ColumnType::Double, Type::Double(value), Value::Number(literal, false)) => {
                let literal = f64::from_str(literal.as_str()).unwrap();
                Ok(*value != literal)
            }
            (ColumnType::Bool, Type::Bool(value), Value::Boolean(literal)) => Ok(value != literal),
            (ColumnType::DateTime, Type::DateTime(value), Value::SingleQuotedString(literal)) => {
                let literal: DateTime<Utc> = literal.parse().unwrap();
                Ok(*value != literal)
            }
            _ => Err(Error::TypeMismatch(
                schema_type,
                event_type.clone(),
                literal.clone(),
            )),
        })
    }

    fn filter_column_less_than_literal(
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
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
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
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
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
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
        &mut self,
        column: &str,
        literal: &Value,
    ) -> Result<HashSet<usize>, Error> {
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
}

#[cfg(test)]
mod tests {
    use crate::engine::tests::generate_typed_events;
    use crate::parser::values::Type;
    use crate::schema::Schema;
    use crate::{Engine, Parser};
    use chrono::{TimeZone, Utc};

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
    fn sql_where_column_does_not_equal_literal() {
        let schema = "\
regex: (?P<i32>.+)\
    \t(?P<string>.+)\
    \t(?P<i64>.+)\
    \t(?P<f32>.+)\
    \t(?P<f64>.+)\
    \t(?P<datetime>.+)\
    \t(?P<bool>.+)
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
    - name: bool
      type: bool
";
        let source = "\
1\ta\t1000\t1.1\t11.11\t2022-01-01T00:00:00Z\ttrue
2\tb\t2000\t2.2\t22.22\t2022-01-02T00:00:00Z\tfalse
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
            ("bool", Type::Bool(true)),
        ]]);

        let queries = vec![
            // op: !=
            "select * from logs where i32 != 2",
            "select * from logs where 2 != i32",
            "select * from logs where i64 != 2000",
            "select * from logs where 2000 != i64",
            "select * from logs where f32 != 2.2",
            "select * from logs where 2.2 != f32",
            "select * from logs where f64 != 22.22",
            "select * from logs where 22.22 != f64",
            "select * from logs where datetime != '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' != datetime",
            "select * from logs where string != 'b'",
            "select * from logs where 'b' != string",
            // op: <>
            "select * from logs where i32 <> 2",
            "select * from logs where 2 <> i32",
            "select * from logs where i64 <> 2000",
            "select * from logs where 2000 <> i64",
            "select * from logs where f32 <> 2.2",
            "select * from logs where 2.2 <> f32",
            "select * from logs where f64 <> 22.22",
            "select * from logs where 22.22 <> f64",
            "select * from logs where datetime <> '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' <> datetime",
            "select * from logs where string <> 'b'",
            "select * from logs where 'b' <> string",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

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
            "select * from logs where 2 > i32",
            "select * from logs where i64 < 2000",
            "select * from logs where 2000 > i64",
            "select * from logs where f32 < 2.2",
            "select * from logs where 2.2 > f32",
            "select * from logs where f64 < 22.22",
            "select * from logs where 22.22 > f64",
            "select * from logs where datetime < '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' > datetime",
            "select * from logs where string < 'b'",
            "select * from logs where 'b' > string",
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
            "select * from logs where 2 >= i32",
            "select * from logs where i64 <= 2000",
            "select * from logs where 2000 >= i64",
            "select * from logs where f32 <= 2.2",
            "select * from logs where 2.2 >= f32",
            "select * from logs where f64 <= 22.22",
            "select * from logs where 22.22 >= f64",
            "select * from logs where datetime <= '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' >= datetime",
            "select * from logs where string <= 'b'",
            "select * from logs where 'b' >= string",
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
            "select * from logs where 2 < i32",
            "select * from logs where i64 > 2000",
            "select * from logs where 2000 < i64",
            "select * from logs where f32 > 2.2",
            "select * from logs where 2.2 < f32",
            "select * from logs where f64 > 22.22",
            "select * from logs where 22.22 < f64",
            "select * from logs where datetime > '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' < datetime",
            "select * from logs where string > 'b'",
            "select * from logs where 'b' < string",
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
            "select * from logs where 2 <= i32",
            "select * from logs where i64 >= 2000",
            "select * from logs where 2000 <= i64",
            "select * from logs where f32 >= 2.2",
            "select * from logs where 2.2 <= f32",
            "select * from logs where f64 >= 22.22",
            "select * from logs where 22.22 <= f64",
            "select * from logs where datetime >= '2022-01-02T00:00:00Z'",
            "select * from logs where '2022-01-02T00:00:00Z' <= datetime",
            "select * from logs where string >= 'b'",
            "select * from logs where 'b' <= string",
        ];

        for query in queries {
            let engine = Engine::with_query(parser.clone(), query.to_string()).unwrap();
            let table_result = engine.execute(source.lines()).unwrap();

            assert_eq!(table_result.events, events);
        }
    }
}
