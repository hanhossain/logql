use crate::engine::Engine;
use crate::parser::Parser;
use clap::Parser as ClapParser;
use std::fmt::Display;
use std::io::{BufRead, BufReader, Read};
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

    let reader: Box<dyn BufRead> = if metadata.is_file() {
        let file = std::fs::File::open(&config.source)?;
        Box::new(BufReader::new(file))
    } else {
        let mut files = Vec::new();
        for entry in WalkDir::new(&config.source) {
            if let Ok(entry) = entry {
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    files.push(entry);
                }
            }
        }

        let mut file_iter = files.into_iter();

        let file1 = file_iter.next().expect("at least one file");
        let file1 = std::fs::File::open(file1.into_path())?;
        let reader1 = BufReader::new(file1);

        if let Some(file2) = file_iter.next() {
            let file2 = std::fs::File::open(file2.into_path())?;
            let mut combo_reader: Box<dyn BufRead> = Box::new(reader1.chain(BufReader::new(file2)));

            for file in file_iter {
                let file = std::fs::File::open(file.into_path())?;
                let reader = BufReader::new(file);
                combo_reader = Box::new(combo_reader.chain(reader));
            }

            Box::new(combo_reader)
        } else {
            Box::new(reader1)
        }
    };

    let table_result = engine.execute(reader.lines().filter_map(|l| l.ok()))?;
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
