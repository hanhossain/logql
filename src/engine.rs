use crate::parser::values::Event;
use crate::parser::Parser;
use std::str::Lines;

pub struct Engine<'a> {
    parser: &'a Parser,
}

impl<'a> Engine<'a> {
    pub fn new(parser: &'a Parser) -> Engine<'a> {
        Engine { parser }
    }

    pub fn execute(&self, lines: Lines<'a>) -> Vec<Event> {
        self.parser.parse(lines)
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
        let _engine = Engine::new(&parser);
    }
}
