use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use crate::parquet_writer::{BATCH_SIZE, TradesParquetWriter, trades_schema};

#[derive(Debug, Clone, Serialize)]
pub struct MergeMetrics {
    pub elapsed_seconds: f64,
    pub days_merged: usize,
    pub rows_merged: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
}

#[derive(Debug)]
pub struct MergeResult {
    pub contract: String,
    pub output_path: PathBuf,
    pub days_merged: usize,
    pub rows_written: u64,
    pub metrics: MergeMetrics,
}

#[derive(Debug)]
struct DailyInput {
    date: String,
    parquet_path: PathBuf,
    rows: u64,
    bytes: u64,
}

pub fn merge_contract(daily_dir: &Path, output_path: &Path) -> Result<MergeResult> {
    let started = Instant::now();
    let daily_dir = daily_dir
        .canonicalize()
        .with_context(|| format!("Daily directory does not exist: {}", daily_dir.display()))?;
    if !daily_dir.is_dir() {
        bail!("Daily input must be a directory");
    }
    let contract = contract_from_daily_dir(&daily_dir)?;
    let output_path = absolute_output_path(output_path)?;
    let expected_name = format!("{contract}.parquet");
    if output_path.file_name().and_then(|value| value.to_str()) != Some(&expected_name) {
        bail!("Final output filename must be exactly {expected_name}");
    }
    let partial_path = output_path.with_file_name(format!("{contract}.partial.parquet"));
    for path in [&output_path, &partial_path] {
        if path.exists() {
            bail!("Refusing to overwrite existing output: {}", path.display());
        }
    }

    let daily_paths = discover_daily_parquet_files(&daily_dir)?;
    if daily_paths.is_empty() {
        bail!(
            "No YYYYMMDD.parquet daily files found in {}",
            daily_dir.display()
        );
    }
    let mut inputs = Vec::with_capacity(daily_paths.len());
    for parquet_path in daily_paths {
        inputs.push(verify_daily_input(&parquet_path, &contract)?);
    }
    let expected_rows = inputs.iter().try_fold(0u64, |total, input| {
        total
            .checked_add(input.rows)
            .context("Merged row count exceeds u64")
    })?;
    let input_bytes = inputs.iter().try_fold(0u64, |total, input| {
        total
            .checked_add(input.bytes)
            .context("Merged input size exceeds u64")
    })?;

    let mut writer = TradesParquetWriter::create(&partial_path)?;
    let mut next_global_sequence = 1i64;
    let mut previous_timestamp = None;
    let mut rows_written = 0u64;

    for input in &inputs {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&input.parquet_path)?)?
            .with_batch_size(BATCH_SIZE);
        let mut reader = builder.build()?;
        let mut local_sequence = 1i64;
        let mut daily_rows = 0u64;
        for batch in &mut reader {
            let batch = batch?;
            validate_batch_order(
                &batch,
                &input.date,
                &mut local_sequence,
                &mut previous_timestamp,
            )?;
            let batch_len = i64::try_from(batch.num_rows())?;
            let end = next_global_sequence
                .checked_add(batch_len)
                .context("Global sequence_id exceeds i64")?;
            let sequence: ArrayRef =
                Arc::new(Int64Array::from_iter_values(next_global_sequence..end));
            let mut columns = batch.columns().to_vec();
            columns[0] = sequence;
            let merged_batch = RecordBatch::try_new(trades_schema(), columns)?;
            writer.append_record_batch(&merged_batch)?;
            next_global_sequence = end;
            let count = u64::try_from(batch.num_rows())?;
            daily_rows += count;
            rows_written += count;
        }
        if daily_rows != input.rows {
            bail!(
                "Daily {} yielded {daily_rows} rows but metadata declared {}",
                input.date,
                input.rows
            );
        }
    }
    writer.close()?;

    if rows_written != expected_rows {
        bail!("Merged {rows_written} rows but expected {expected_rows}");
    }
    verify_merged_contents(&partial_path, expected_rows)?;
    let expected_daily_paths = inputs
        .iter()
        .map(|input| input.parquet_path.clone())
        .collect::<Vec<_>>();
    let current_daily_paths = discover_daily_parquet_files(&daily_dir)?;
    if current_daily_paths != expected_daily_paths {
        bail!("The set of daily Parquet files changed during merge; retry with a stable directory");
    }
    fs::rename(&partial_path, &output_path)?;
    if let Err(error) = verify_merged_metadata(&output_path, expected_rows) {
        let _ = fs::remove_file(&output_path);
        return Err(error);
    }

    let metrics = MergeMetrics {
        elapsed_seconds: started.elapsed().as_secs_f64(),
        days_merged: inputs.len(),
        rows_merged: rows_written,
        input_bytes,
        output_bytes: output_path.metadata()?.len(),
    };
    Ok(MergeResult {
        contract,
        output_path,
        days_merged: inputs.len(),
        rows_written,
        metrics,
    })
}

pub fn discover_daily_parquet_files(daily_dir: &Path) -> Result<Vec<PathBuf>> {
    let shape = Regex::new(r"^(\d{8})\.parquet$")?;
    let mut files = Vec::new();
    for entry in fs::read_dir(daily_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if shape.is_match(name) {
            daily_date_from_parquet_path(&path)?;
            files.push(path);
        }
    }
    files.sort_by_key(|path| path.file_name().map(ToOwned::to_owned));
    Ok(files)
}

fn contract_from_daily_dir(daily_dir: &Path) -> Result<String> {
    let folder = daily_dir
        .file_name()
        .and_then(|value| value.to_str())
        .context("Daily directory name is not valid UTF-8")?;
    let pattern = Regex::new(r"^([A-Z0-9]+)_(0[1-9]|1[0-2])-(\d{2})$")?;
    if !pattern.is_match(folder) {
        bail!("Daily directory must be named like NQ_09-26, got {folder:?}");
    }
    Ok(folder.to_string())
}

fn daily_date_from_parquet_path(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("Daily Parquet filename is not valid UTF-8")?;
    let date = name
        .strip_suffix(".parquet")
        .context("Daily Parquet filename must end with .parquet")?;
    chrono::NaiveDate::parse_from_str(date, "%Y%m%d")
        .with_context(|| format!("Invalid daily Parquet date: {date}"))?;
    Ok(date.to_string())
}

fn absolute_output_path(output_path: &Path) -> Result<PathBuf> {
    let parent = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = parent.canonicalize().with_context(|| {
        format!(
            "Final output directory does not exist: {}",
            parent.display()
        )
    })?;
    let name = output_path
        .file_name()
        .context("Final output path must include a filename")?;
    Ok(parent.join(name))
}

fn verify_daily_input(path: &Path, contract: &str) -> Result<DailyInput> {
    let date = daily_date_from_parquet_path(path)?;
    let bytes = path.metadata()?.len();
    if bytes == 0 {
        bail!("Daily Parquet is empty: {}", path.display());
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .with_context(|| format!("Cannot read daily Parquet footer: {}", path.display()))?;
    if builder.schema().as_ref() != trades_schema().as_ref() {
        bail!("Daily {date} has an incompatible Arrow schema");
    }
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows())?;
    let validation_path = path.with_file_name(format!("{date}_validation.json"));
    let value: Value = serde_json::from_reader(BufReader::new(
        File::open(&validation_path)
            .with_context(|| format!("Missing validation JSON for daily {date}"))?,
    ))
    .with_context(|| format!("Invalid validation JSON for daily {date}"))?;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .context("Daily validation has no string status")?;
    if !matches!(status, "PASS" | "WARNING") {
        bail!("Daily {date} validation status {status:?} is not mergeable");
    }
    if value.get("contract").and_then(Value::as_str) != Some(contract) {
        bail!("Daily {date} validation contract does not match {contract}");
    }
    if value.get("date").and_then(Value::as_str) != Some(date.as_str()) {
        bail!("Daily {date} validation date does not match its filename");
    }
    let validation_rows = value
        .pointer("/totals/trades_written")
        .and_then(Value::as_u64)
        .context("Daily validation has no totals.trades_written")?;
    if validation_rows != rows {
        bail!("Daily {date} validation rows {validation_rows} do not match Parquet rows {rows}");
    }
    Ok(DailyInput {
        date,
        parquet_path: path.to_path_buf(),
        rows,
        bytes,
    })
}

fn validate_batch_order(
    batch: &RecordBatch,
    date: &str,
    local_sequence: &mut i64,
    previous_timestamp: &mut Option<i64>,
) -> Result<()> {
    let sequence = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("sequence_id is not Int64")?;
    let timestamps = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .context("timestamp_chicago is not timestamp[ns]")?;
    for index in 0..batch.num_rows() {
        if sequence.is_null(index) || timestamps.is_null(index) {
            bail!("Daily {date} contains null sequence_id or timestamp at row {index}");
        }
        let actual_sequence = sequence.value(index);
        if actual_sequence != *local_sequence {
            bail!(
                "Daily {date} sequence_id is not local and continuous: expected {}, got {actual_sequence}",
                *local_sequence
            );
        }
        *local_sequence += 1;
        let timestamp = timestamps.value(index);
        if previous_timestamp.is_some_and(|previous| timestamp < previous) {
            bail!("Daily {date} is not chronologically ordered at local row {actual_sequence}");
        }
        *previous_timestamp = Some(timestamp);
    }
    Ok(())
}

fn verify_merged_contents(path: &Path, expected_rows: u64) -> Result<()> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.with_batch_size(BATCH_SIZE);
    if builder.schema().as_ref() != trades_schema().as_ref() {
        bail!("Final Parquet schema does not match the TRADES schema");
    }
    let mut reader = builder.build()?;
    let mut expected_sequence = 1i64;
    let mut previous_timestamp = None;
    let mut rows = 0u64;
    for batch in &mut reader {
        let batch = batch?;
        validate_batch_order(
            &batch,
            "merged output",
            &mut expected_sequence,
            &mut previous_timestamp,
        )?;
        rows += u64::try_from(batch.num_rows())?;
    }
    if rows != expected_rows {
        bail!("Final Parquet contains {rows} rows but expected {expected_rows}");
    }
    if expected_sequence != i64::try_from(expected_rows)? + 1 {
        bail!("Final sequence_id does not end at {expected_rows}");
    }
    Ok(())
}

fn verify_merged_metadata(path: &Path, expected_rows: u64) -> Result<()> {
    if !path.is_file() || path.metadata()?.len() == 0 {
        bail!("Final Parquet does not exist or is empty");
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    if builder.schema().as_ref() != trades_schema().as_ref() {
        bail!("Final Parquet schema changed after rename");
    }
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows())?;
    if rows != expected_rows {
        bail!("Final Parquet metadata contains {rows} rows but expected {expected_rows}");
    }
    Ok(())
}
