use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

fn main() -> Result<()> {
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
";

    println!("{}", source);
    println!("{}", schema);

    let schema: Schema = serde_yaml::from_str(schema)?;
    println!("{:#?}", schema);

    let re = Regex::new(&schema.regex)?;

    // verify all columns exist as capture names
    let capture_names: HashSet<_> = re.capture_names().flatten().collect();
    let non_existent_columns: Vec<_> = schema
        .columns
        .iter()
        .map(String::as_str)
        .filter(|x| !capture_names.contains(x))
        .collect();

    assert!(
        non_existent_columns.is_empty(),
        "All columns must correspond to named capture groups. Columns missing capture groups: {:?}",
        non_existent_columns
    );

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
