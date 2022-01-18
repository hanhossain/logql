use anyhow::Result;
use regex::Regex;
use std::collections::HashMap;

fn main() -> Result<()> {
    print_regex()?;
    print_yaml()?;

    Ok(())
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
