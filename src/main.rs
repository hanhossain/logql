#![allow(dead_code)]
use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;

fn main() -> Result<()> {
    let source = "\
1\tfirst\t42.0
2\tsecond\t3.14
";

    let schema = r"
regex: (?P<index>\d+)\t(?P<string_value>.+)\t(?P<double_value>\d+\.\d+)
columns:
    - index
    - string_value
    - double_value
";

    println!("{}", source);
    println!("{}", schema);

    let schema: Schema = serde_yaml::from_str(schema)?;
    println!("{:#?}", schema);

    let re = Regex::new(&schema.regex)?;
    let parsed: Vec<_> = source
        .lines()
        .filter_map(|line| re.captures(line))
        .map(|caps| {
            schema
                .columns
                .iter()
                .map(|column| (column, caps.name(column).unwrap().as_str()))
                .collect::<HashMap<_, _>>()
        })
        .collect();

    println!("{:#?}", parsed);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct Schema {
    regex: String,
    columns: Vec<String>,
}
