use regex::Regex;

fn main() -> anyhow::Result<()> {
    let re = Regex::new(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})")?;
    let caps = re.captures("2010-03-14").unwrap();
    println!("captures: {:?}", caps);
    println!("year: {}", &caps["year"]);
    println!("month: {}", &caps["month"]);
    println!("day: {}", &caps["day"]);
    Ok(())
}
