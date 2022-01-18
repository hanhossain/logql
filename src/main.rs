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

    let mut parsed = Vec::new();
    let re = Regex::new(&schema.regex)?;
    for line in source.lines() {
        if let Some(caps) = re.captures(line) {
            let mut row = HashMap::new();

            for column in &schema.columns {
                row.insert(column, caps.name(column).map(|x| x.as_str()));
            }

            parsed.push(row);
        }
    }

    println!("{:#?}", parsed);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct Schema {
    regex: String,
    columns: Vec<String>,
}
