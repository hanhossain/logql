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

fn print_regex() -> Result<()> {
    let re = Regex::new(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})")?;
    let caps = re.captures("2010-03-14").unwrap();
    println!("captures: {:?}", caps);
    println!("year: {}", &caps["year"]);
    println!("month: {}", &caps["month"]);
    println!("day: {}", &caps["day"]);
    Ok(())
}

fn print_yaml() -> Result<()> {
    let yaml = "x: 1.0\ny: 2.0\n";
    let deserialized: HashMap<String, f64> = serde_yaml::from_str(yaml)?;
    println!("{:?}", deserialized);
    Ok(())
}
