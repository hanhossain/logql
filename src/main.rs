use crate::parser::values::Type;
use crate::parser::Parser;
use comfy_table::{presets, ContentArrangement, Table};

mod error;
mod parser;
mod schema;

fn main() -> anyhow::Result<()> {
    let source = "\
1\tfirst\t42.0
2\tsecond\t3.14
this should not match the regex therefore this should be part of the extra text
Lorem ipsum dolor sit amet, consectetur adipiscing elit. In non fringilla tortor, vitae bibendum \
nisl. Nullam quis auctor tellus. Cras nisi enim, vehicula semper luctus in, placerat at tellus. \
Aenean commodo est purus, aliquet fringilla turpis volutpat id. Nam tristique venenatis ex eu \
lobortis. Curabitur tempus mattis lorem, quis fringilla metus consectetur tristique. Suspendisse \
vitae euismod justo. Aenean sollicitudin gravida sapien id pharetra. Vivamus nec ex et metus \
gravida tempus et sit amet purus. Praesent bibendum varius imperdiet.
3\tthird\t10.1
4\tfourth\t20.2
nomatch 20 4
another extra line
5\tfifth\t11.1
";

    let schema = r"
regex: (?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)
columns:
    - name: index
      type: i32
    - name: string_value
      type: string
      multiline: true
    - name: double_value
      type: string
";

    let parser = Parser::try_from(schema)?;
    let parsed = parser.parse(source.lines());

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

    for row in parsed {
        let mut result: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| &row.values[&c.name.as_str()])
            .map(|t| match t {
                Type::String(x) => x.to_string(),
                Type::Int32(x) => x.to_string(),
                Type::Int64(x) => x.to_string(),
                Type::Bool(x) => x.to_string(),
                Type::Double(x) => x.to_string(),
            })
            .collect();
        if let Some(extra_text) = row.extra_text {
            for text in extra_text {
                let multiline_column = &mut result[parser.multiline_index.unwrap()];
                multiline_column.push('\n');
                multiline_column.push_str(text);
            }
        }
        table.add_row(result);
    }

    println!("{table}");
    Ok(())
}
