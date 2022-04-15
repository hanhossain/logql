use crate::engine::Engine;
use crate::parser::values::Event;
use crate::parser::Parser;
use clap::Parser as ClapParser;
use comfy_table::{presets, ContentArrangement, Table};

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

    let engine_result = engine.execute(source.lines());

    let mut table = create_table(&parser);
    populate_table(&mut table, engine_result.events, &parser);

    println!("{table}");
    Ok(())
}

fn create_table(parser: &Parser) -> Table {
    let mut table = Table::new();
    let header: Vec<_> = parser
        .schema
        .columns
        .iter()
        .map(|c| c.name.to_owned())
        .collect();
    table
        .load_preset(presets::UTF8_FULL)
        .set_content_arrangement(ContentArrangement::DynamicFullWidth)
        .set_header(header);
    table
}

fn populate_table(table: &mut Table, events: Vec<Event>, parser: &Parser) {
    for event in events {
        let mut result: Vec<_> = parser
            .schema
            .columns
            .iter()
            .map(|c| &event.values[&c.name.as_str()])
            .map(|t| t.to_string())
            .collect();
        if let Some(extra_text) = event.extra_text {
            for text in extra_text {
                let multiline_column = &mut result[parser.multiline_index.unwrap()];
                multiline_column.push('\n');
                multiline_column.push_str(text);
            }
        }
        table.add_row(result);
    }
}
