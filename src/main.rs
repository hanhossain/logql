use crate::engine::Engine;
use crate::parser::values::Event;
use crate::parser::Parser;
use comfy_table::{presets, ContentArrangement, Table};

mod engine;
mod error;
mod parser;
mod schema;

fn main() -> anyhow::Result<()> {
    let source = "\
1\tfirst\t42.0\t2022-04-10T08:00:00Z\t4
2\tsecond\t3.14\t2022-04-10T09:00:00Z\t3
this should not match the regex therefore this should be part of the extra text
Lorem ipsum dolor sit amet, consectetur adipiscing elit. In non fringilla tortor, vitae bibendum \
nisl. Nullam quis auctor tellus. Cras nisi enim, vehicula semper luctus in, placerat at tellus. \
Aenean commodo est purus, aliquet fringilla turpis volutpat id. Nam tristique venenatis ex eu \
lobortis. Curabitur tempus mattis lorem, quis fringilla metus consectetur tristique. Suspendisse \
vitae euismod justo. Aenean sollicitudin gravida sapien id pharetra. Vivamus nec ex et metus \
gravida tempus et sit amet purus. Praesent bibendum varius imperdiet.
3\tthird\t10.1\t2022-04-10T10:00:00Z\t2
4\tfourth\t20.2\t2022-04-10T11:00:00Z\t4
nomatch 20 4
another extra line
5\tfifth\t11.1\t2022-04-10T12:00:00Z\t2
";

    let schema = r"
regex: (?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)\t(?P<timestamp>.+)\t(?P<log_level>\d+)
columns:
    - name: index
      type: i32
    - name: string_value
      type: string
      multiline: true
    - name: double_value
      type: f64
    - name: timestamp
      type: datetime
    - name: log_level
      type: i32
";

    let parser = Parser::try_from(schema)?;
    let engine = Engine::new(&parser);
    let events = engine.execute(source.lines());

    let mut table = create_table(&parser);
    populate_table(&mut table, events, &parser);

    println!("{table}");
    Ok(())
}

fn create_table(parser: &Parser) -> Table {
    let mut table = Table::new();
    let header: Vec<_> = parser
        .schema
        .columns
        .iter()
        .map(|c| c.name.to_owned())
        .collect();
    table
        .load_preset(presets::UTF8_FULL)
        .set_content_arrangement(ContentArrangement::DynamicFullWidth)
        .set_header(header);
    table
}

fn populate_table(table: &mut Table, events: Vec<Event>, parser: &Parser) {
    for event in events {
        let mut result: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| &event.values[&c.name.as_str()])
            .map(|t| t.to_string())
            .collect();
        if let Some(extra_text) = event.extra_text {
            for text in extra_text {
                let multiline_column = &mut result[parser.multiline_index.unwrap()];
                multiline_column.push('\n');
                multiline_column.push_str(text);
            }
        }
        table.add_row(result);
    }
}
