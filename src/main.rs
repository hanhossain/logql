mod error;
mod parser;
mod schema;

use crate::parser::Parser;

fn main() -> anyhow::Result<()> {
    let source = "\
1\tfirst\t42.0
2\tsecond\t3.14
this\tshould not match\tthe regex
";

    let schema = r"
regex: (?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)
columns:
    - name: index
      type: string
    - name: string_value
      type: string
    - name: double_value
      type: string
";

    let parser = Parser::try_from(schema)?;
    let parsed: Vec<_> = source
        .lines()
        .filter_map(|line| parser.parse_line(line))
        .collect();

    println!("{:#?}", parsed);

    Ok(())
}
