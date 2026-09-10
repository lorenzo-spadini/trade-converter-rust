use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Serialize;
use trade_converter_rust::{
    ConversionOptions, contract_from_folder_name, convert_day, merge_contract,
};

#[derive(Parser)]
#[command(
    version,
    about = "Rust NRDToCSV daily Parquet converter and merger",
    arg_required_else_help = true
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Convert one YYYYMMDD.csv file to an independently verified daily Parquet.
    ConvertDay {
        csv_path: PathBuf,
        output_dir: PathBuf,
        #[arg(long, allow_hyphen_values = true)]
        tick_size: String,
        #[arg(long)]
        metrics: Option<PathBuf>,
    },
    /// Merge verified daily Parquet files into one chronological contract Parquet.
    MergeContract {
        daily_dir: PathBuf,
        output_parquet: PathBuf,
        #[arg(long)]
        metrics: Option<PathBuf>,
    },
}

fn main() {
    let arguments = Arguments::parse();
    let exit_code = match arguments.command {
        Command::ConvertDay {
            csv_path,
            output_dir,
            tick_size,
            metrics,
        } => run_convert_day(&csv_path, &output_dir, &tick_size, metrics.as_deref()),
        Command::MergeContract {
            daily_dir,
            output_parquet,
            metrics,
        } => run_merge_contract(&daily_dir, &output_parquet, metrics.as_deref()),
    };
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run_convert_day(
    csv_path: &Path,
    output_dir: &Path,
    tick_size: &str,
    metrics_path: Option<&Path>,
) -> i32 {
    let (fallback_contract, fallback_date) = daily_identity_for_error(csv_path);
    let logger = |message: &str| eprintln!("{message}");
    match convert_day(
        csv_path,
        output_dir,
        ConversionOptions {
            tick_size,
            log: Some(&logger),
        },
    ) {
        Ok(result) => {
            if let Some(path) = metrics_path
                && let Err(error) = write_metrics(path, &result.metrics)
            {
                eprintln!(
                    "METRICS_WARNING|{}|{}|{}",
                    result.contract,
                    result.date,
                    sanitize_message(&format!("Cannot write metrics: {error:#}"))
                );
            }
            println!(
                "PARQUET_DONE|{}|{}|{}|{}|{}",
                result.contract,
                result.date,
                result.parquet_path.display(),
                result.validation_path.display(),
                result.rows_written
            );
            0
        }
        Err(error) => {
            eprintln!(
                "PARQUET_FAILED|{fallback_contract}|{fallback_date}|{}",
                sanitize_message(&format!("{error:#}"))
            );
            1
        }
    }
}

fn run_merge_contract(daily_dir: &Path, output_parquet: &Path, metrics_path: Option<&Path>) -> i32 {
    let fallback_contract = merge_contract_for_error(daily_dir);
    match merge_contract(daily_dir, output_parquet) {
        Ok(result) => {
            if let Some(path) = metrics_path
                && let Err(error) = write_metrics(path, &result.metrics)
            {
                eprintln!(
                    "METRICS_WARNING|{}|{}",
                    result.contract,
                    sanitize_message(&format!("Cannot write metrics: {error:#}"))
                );
            }
            println!(
                "MERGE_DONE|{}|{}|{}|{}",
                result.contract,
                result.output_path.display(),
                result.days_merged,
                result.rows_written
            );
            0
        }
        Err(error) => {
            eprintln!(
                "MERGE_FAILED|{fallback_contract}|{}",
                sanitize_message(&format!("{error:#}"))
            );
            1
        }
    }
}

fn write_metrics<T: Serialize>(path: &Path, metrics: &T) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("Cannot create metrics directory: {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(metrics)?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("Cannot write metrics file: {}", path.display()))?;
    Ok(())
}

fn daily_identity_for_error(csv_path: &Path) -> (String, String) {
    let contract = csv_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .and_then(|folder| contract_from_folder_name(folder).ok())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let date = csv_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()))
        .unwrap_or("UNKNOWN")
        .to_string();
    (contract, date)
}

fn merge_contract_for_error(daily_dir: &Path) -> String {
    daily_dir
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("UNKNOWN")
        .to_string()
}

fn sanitize_message(message: &str) -> String {
    message
        .chars()
        .map(|character| {
            if matches!(character, '\r' | '\n' | '|') {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
