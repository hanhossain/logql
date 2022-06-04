use crate::engine::Engine;
use crate::parser::Parser;
use clap::Parser as ClapParser;
use std::fmt::Display;
use walkdir::WalkDir;

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
    #[clap(long)]
    no_print: bool,
    #[clap(long)]
    json: bool,
    #[clap(long)]
    json_headers: bool,
}

fn main() -> anyhow::Result<()> {
    let config: Config = Config::parse();
    let schema = std::fs::read_to_string(&config.schema)?;

    let parser = Parser::try_from(schema.as_str())?;
    let engine = match &config.sql {
        Some(s) => Engine::with_query(parser, s.clone()),
        None => Ok(Engine::new(parser)),
    }?;

    let metadata = std::fs::metadata(&config.source)?;

    let files = if metadata.is_file() {
        let raw = std::fs::read_to_string(&config.source)?;
        vec![raw]
    } else {
        let mut files = Vec::new();
        for entry in WalkDir::new(&config.source) {
            if let Ok(entry) = entry {
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    files.push(std::fs::read_to_string(entry.into_path())?);
                }
            }
        }
        files
    };

    let table_result = engine.execute(files)?;
    if !config.no_print {
        let output: Box<dyn Display> = match &config {
            Config { json: true, .. } => {
                Box::new(serde_json::to_string_pretty(&table_result.events)?)
            }
            Config {
                json_headers: true, ..
            } => Box::new(serde_json::to_string_pretty(&table_result)?),
            _ => Box::new(table_result.table()),
        };
        println!("{}", output);
    }
    Ok(())
}
