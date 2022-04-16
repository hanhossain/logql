use crate::engine::Engine;
use crate::parser::Parser;
use clap::Parser as ClapParser;

mod engine;
mod error;
mod parser;
mod schema;

#[derive(ClapParser, Debug)]
struct Config {
    #[clap(long)]
    source: String,
    #[clap(long)]
    schema: String,
    #[clap(long)]
    sql: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let config: Config = Config::parse();
    println!("{:#?}", config);
    let source = std::fs::read_to_string(config.source)?;
    let schema = std::fs::read_to_string(config.schema)?;

    println!("{}", source);
    println!("{}", schema);

    let parser = Parser::try_from(schema.as_str())?;
    let engine = match config.sql {
        Some(s) => Engine::with_query(&parser, s.clone()),
        None => Ok(Engine::new(&parser)),
    }?;

    let engine_result = engine.execute(source.lines())?;
    let table = engine_result.table();
    println!("{table}");
    Ok(())
}
