use crate::parser::Parser;

mod error;
mod parser;
mod schema;

fn main() -> anyhow::Result<()> {
    let source = "\
1\tfirst\t42.0
2\tsecond\t3.14
this should not match the regex therefore this should be part of the extra text
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
      type: string
    - name: string_value
      type: string
    - name: double_value
      type: string
";

    let parser = Parser::try_from(schema)?;
    let mut parsed = Vec::new();
    for line in source.lines() {
        if let Some(matched_result) = parser.parse_line(line) {
            parsed.push(matched_result);
        } else if let Some(last) = parsed.last_mut() {
            match last.extra_text.as_mut() {
                None => last.extra_text = Some(vec![line]),
                Some(extra_text) => extra_text.push(line),
            }
        }
    }

    println!("{:#?}", parsed);

    Ok(())
}
