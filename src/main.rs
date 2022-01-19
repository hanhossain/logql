mod error;
mod parser;

use crate::parser::Parser;
use std::collections::HashMap;

fn main() -> anyhow::Result<()> {
    let source = "\
1\tfirst\t42.0
2\tsecond\t3.14
this\tshould not match\tthe regex
";

    let schema = r"
regex: (?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)
columns:
    - index
    - string_value
    - double_value
    - something
";

    let parser = Parser::try_from(schema)?;

    let parsed: Vec<_> = source
        .lines()
        .filter_map(|line| parser.regex.captures(line))
        .map(|caps| {
            parser
                .schema
                .columns
                .iter()
                .map(|column| (column, caps.name(column).unwrap().as_str()))
                .collect::<HashMap<_, _>>()
        })
        .collect();

    println!("{:#?}", parsed);

    Ok(())
}
