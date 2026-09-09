#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

use std::fs;
use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use trade_converter_rust::{ConversionOptions, convert_source};

#[derive(Parser)]
#[command(version, about = "Rust NRDToCSV to Parquet converter")]
struct Arguments {
    source: Option<PathBuf>,
    destination: Option<PathBuf>,
    #[arg(long)]
    tick_size: Option<String>,
    #[arg(long)]
    metrics: Option<PathBuf>,
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();
    match (arguments.source, arguments.destination) {
        (None, None) => {
            trade_converter_rust::gui::run().map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        (Some(source), Some(destination)) => {
            let logger = |message: &str| println!("{message}");
            let result = convert_source(
                &source,
                &destination,
                ConversionOptions {
                    tick_size: arguments.tick_size.as_deref(),
                    log: Some(&logger),
                    write_raw: true,
                },
            )?;
            let metrics = serde_json::to_string_pretty(&result.metrics)?;
            println!("{metrics}");
            if let Some(path) = arguments.metrics {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, format!("{metrics}\n"))?;
            }
        }
        _ => bail!("Source and Destination must be supplied together"),
    }
    Ok(())
}
