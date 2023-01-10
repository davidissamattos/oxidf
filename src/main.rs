// oxidf main application
// Author: David Issa Mattos
// Mantainer: David Issa Mattos
use std::env;
mod compute;
mod input;
mod operations;
mod output;
mod parser;
mod steps;
mod utils;
use compute::compute_pipeline;
use parser::*;
use clap::Parser;


#[derive(Parser)]
#[command(author, version, about)]
struct CLI {
   // path to the TOML file
   #[clap(short = 'p', long)]
   path: String,
   // if running on verbose mode or not
   #[clap(short = 'v', long)]
   verbose: Option<bool>,
}

fn main() {
    let cli = CLI::parse();

    let path = cli.path;
    let messages = match cli.verbose {
        Some(c) => c,
        _ => false
    };
    if messages{
        println!("Reading toml file: {}", path);
    }

    let pipeline = parse_toml(path.as_str()).expect("Error parsing the toml file");
    validate_pipeline(&pipeline);
    compute_pipeline(&pipeline, messages);
}
