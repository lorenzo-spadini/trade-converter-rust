use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use csv::{ByteRecord, ReaderBuilder};
use memory_stats::memory_stats;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use crate::decimal::{ExactDecimal, Price};
use crate::parquet_writer::{TradeRow, TradesParquetWriter, trades_schema};
use crate::timestamp::TimestampConverter;
use crate::validation::{FileValidation, Totals, ValidationDocument};

pub type LogCallback = dyn Fn(&str) + Send + Sync;

pub struct ConversionOptions<'a> {
    pub tick_size: &'a str,
    pub log: Option<&'a LogCallback>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    pub input_bytes: u64,
    pub input_mb: f64,
    pub rows: u64,
    pub events: u64,
    pub elapsed_seconds: f64,
    pub input_mb_per_second: f64,
    pub rows_per_second: f64,
    pub events_per_second: f64,
    pub peak_rss_bytes: usize,
    pub processing_seconds: f64,
    pub parquet_write_seconds: f64,
    pub parquet_bytes: u64,
    pub validation_bytes: u64,
    pub status: String,
}

#[derive(Debug)]
pub struct DailyConversionResult {
    pub contract: String,
    pub date: String,
    pub parquet_path: PathBuf,
    pub validation_path: PathBuf,
    pub rows_written: u64,
    pub metrics: Metrics,
}

#[derive(Debug)]
struct DailyOutputPaths {
    parquet: PathBuf,
    validation: PathBuf,
    partial_parquet: PathBuf,
    partial_validation: PathBuf,
}

pub fn convert_day(
    csv_path: &Path,
    output_dir: &Path,
    options: ConversionOptions<'_>,
) -> Result<DailyConversionResult> {
    let started = Instant::now();
    let csv_path = csv_path
        .canonicalize()
        .with_context(|| format!("Source CSV does not exist: {}", csv_path.display()))?;
    if !csv_path.is_file() {
        bail!("Source must be one CSV file: {}", csv_path.display());
    }
    let date = daily_date_from_csv_path(&csv_path)?;
    let parent = csv_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .context("Cannot determine the contract folder from the CSV parent")?;
    let contract = contract_from_folder_name(parent)?;
    let instrument = contract
        .split('_')
        .next()
        .context("Contract has no instrument")?
        .to_string();
    let output_dir = output_dir
        .canonicalize()
        .with_context(|| format!("Output directory does not exist: {}", output_dir.display()))?;
    if !output_dir.is_dir() {
        bail!("Output path must be an existing directory");
    }
    let tick_size = parse_tick_size(options.tick_size)?;
    let paths = daily_output_paths(&output_dir, &date);
    for path in [
        &paths.parquet,
        &paths.validation,
        &paths.partial_parquet,
        &paths.partial_validation,
    ] {
        if path.exists() {
            bail!("Refusing to overwrite existing output: {}", path.display());
        }
    }

    let log = |message: &str| {
        if let Some(callback) = options.log {
            callback(message);
        }
    };
    log(&format!("Converting {date} for {contract}"));
    log(&format!(
        "Tick alignment: using Tick Size {}",
        crate::decimal::format_exact(tick_size)
    ));

    let input_bytes = csv_path.metadata()?.len();
    let mut writer = TradesParquetWriter::create(&paths.partial_parquet)?;
    let file = File::open(&csv_path)?;
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::with_capacity(8 * 1024 * 1024, file));
    let mut record = ByteRecord::new();
    let mut validation = FileValidation::default();
    let mut timestamps = TimestampConverter::default();
    let mut current_bid: Option<Price> = None;
    let mut current_ask: Option<Price> = None;
    let mut sequence_id = 0i64;
    let mut source_line = 0i64;
    let mut processing_error = None;
    let processing_started = Instant::now();
    let mut peak_rss = memory_stats().map_or(0, |stats| stats.physical_mem);

    loop {
        let has_record = match reader.read_byte_record(&mut record) {
            Ok(value) => value,
            Err(error) => {
                processing_error = Some(format!(
                    "{}:{}: {}",
                    file_name(&csv_path),
                    source_line + 1,
                    error
                ));
                break;
            }
        };
        if !has_record {
            break;
        }
        source_line += 1;
        let result = process_record(
            &record,
            &csv_path,
            source_line,
            &instrument,
            tick_size,
            &mut timestamps,
            &mut validation,
            &mut writer,
            &mut current_bid,
            &mut current_ask,
            &mut sequence_id,
        );
        if let Err(error) = result {
            let message_text = error.to_string();
            let prefix = format!("{}:", file_name(&csv_path));
            processing_error = Some(if message_text.starts_with(&prefix) {
                message_text
            } else {
                format!("{}:{}: {message_text}", file_name(&csv_path), source_line)
            });
            break;
        }
        if source_line % 50_000 == 0
            && let Some(stats) = memory_stats()
        {
            peak_rss = peak_rss.max(stats.physical_mem);
        }
    }
    let processing_time = processing_started.elapsed();

    if validation.total_events == 0 && processing_error.is_none() {
        processing_error = Some(format!("{}: CSV contains no events", file_name(&csv_path)));
    }
    if let Some(message) = processing_error {
        validation.parse_errors += 1;
        validation.malformed_rows += 1;
        validation.errors.push(message.clone());
        let _ = writer.close();
        write_failure_validation(
            &paths,
            &csv_path,
            &contract,
            &instrument,
            &date,
            tick_size,
            &validation,
            &message,
        );
        bail!(message);
    }

    let parquet_write_time = writer.close()?;
    let mut totals = Totals::default();
    totals.add(&validation);
    let validation_document = ValidationDocument::new(
        csv_path.to_string_lossy().into_owned(),
        contract.clone(),
        instrument,
        date.clone(),
        tick_size,
        vec![validation.report(&csv_path)],
        totals.clone(),
        Vec::new(),
        Vec::new(),
        None,
    );
    let validation_text = serde_json::to_string_pretty(&validation_document)?;
    fs::write(&paths.partial_validation, validation_text)?;

    if validation_document.status == "FAIL" {
        bail!("Daily validation failed for {contract} {date}");
    }
    verify_daily_outputs(
        &paths.partial_parquet,
        &paths.partial_validation,
        &contract,
        &date,
        totals.trades_written,
    )?;

    fs::rename(&paths.partial_validation, &paths.validation)?;
    if let Err(error) = fs::rename(&paths.partial_parquet, &paths.parquet) {
        let _ = fs::remove_file(&paths.validation);
        return Err(error.into());
    }
    if let Err(error) = verify_daily_outputs(
        &paths.parquet,
        &paths.validation,
        &contract,
        &date,
        totals.trades_written,
    ) {
        let _ = fs::remove_file(&paths.parquet);
        let _ = fs::remove_file(&paths.validation);
        return Err(error);
    }

    let elapsed = started.elapsed().as_secs_f64();
    if let Some(stats) = memory_stats() {
        peak_rss = peak_rss.max(stats.physical_mem);
    }
    let metrics = Metrics {
        input_bytes,
        input_mb: input_bytes as f64 / 1_000_000.0,
        rows: totals.trades_written,
        events: totals.total_events,
        elapsed_seconds: elapsed,
        input_mb_per_second: input_bytes as f64 / 1_000_000.0 / elapsed,
        rows_per_second: totals.trades_written as f64 / elapsed,
        events_per_second: totals.total_events as f64 / elapsed,
        peak_rss_bytes: peak_rss,
        processing_seconds: processing_time.as_secs_f64(),
        parquet_write_seconds: parquet_write_time.as_secs_f64(),
        parquet_bytes: paths.parquet.metadata()?.len(),
        validation_bytes: paths.validation.metadata()?.len(),
        status: validation_document.status,
    };
    log(&format!(
        "Daily conversion complete: {} rows, {}",
        totals.trades_written, metrics.status
    ));

    Ok(DailyConversionResult {
        contract,
        date,
        parquet_path: paths.parquet,
        validation_path: paths.validation,
        rows_written: totals.trades_written,
        metrics,
    })
}

pub fn contract_from_folder_name(folder: &str) -> Result<String> {
    let pattern = Regex::new(r"^([A-Za-z0-9]+)\s+([A-Za-z]{3})(\d{2})$")?;
    let captures = pattern.captures(folder.trim()).ok_or_else(|| {
        anyhow::anyhow!("Cannot confidently detect futures contract from folder name {folder:?}")
    })?;
    let month = match captures[2].to_ascii_uppercase().as_str() {
        "JAN" => "01",
        "FEB" => "02",
        "MAR" => "03",
        "APR" => "04",
        "MAY" => "05",
        "JUN" => "06",
        "JUL" => "07",
        "AUG" => "08",
        "SEP" => "09",
        "OCT" => "10",
        "NOV" => "11",
        "DEC" => "12",
        value => bail!("Unknown futures month in folder name: {value:?}"),
    };
    Ok(format!(
        "{}_{}-{}",
        captures[1].to_ascii_uppercase(),
        month,
        &captures[3]
    ))
}

pub fn daily_date_from_csv_path(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("Daily CSV filename is not valid UTF-8")?;
    let captures = Regex::new(r"^(\d{8})\.csv$")?
        .captures(name)
        .ok_or_else(|| anyhow::anyhow!("Daily CSV filename must be exactly YYYYMMDD.csv"))?;
    let date = captures[1].to_string();
    chrono::NaiveDate::parse_from_str(&date, "%Y%m%d")
        .with_context(|| format!("Invalid daily CSV date: {date}"))?;
    Ok(date)
}

fn parse_tick_size(value: &str) -> Result<ExactDecimal> {
    let value = value.trim();
    if value.is_empty() {
        bail!("Tick Size is required");
    }
    let tick = ExactDecimal::parse(value).context("Invalid Tick Size")?;
    if !tick.is_positive() {
        bail!("Tick Size must be a finite number greater than zero");
    }
    Ok(tick)
}

fn daily_output_paths(output_dir: &Path, date: &str) -> DailyOutputPaths {
    DailyOutputPaths {
        parquet: output_dir.join(format!("{date}.parquet")),
        validation: output_dir.join(format!("{date}_validation.json")),
        partial_parquet: output_dir.join(format!("{date}.partial.parquet")),
        partial_validation: output_dir.join(format!("{date}_validation.partial.json")),
    }
}

#[allow(clippy::too_many_arguments)]
fn process_record(
    record: &ByteRecord,
    csv_path: &Path,
    source_line: i64,
    instrument: &str,
    tick_size: ExactDecimal,
    timestamps: &mut TimestampConverter,
    validation: &mut FileValidation,
    writer: &mut TradesParquetWriter,
    current_bid: &mut Option<Price>,
    current_ask: &mut Option<Price>,
    sequence_id: &mut i64,
) -> Result<()> {
    if record.is_empty() || record.iter().all(|field| trim_ascii(field).is_empty()) {
        bail!("blank row");
    }
    let mut event_kind = text(record.get(0), "event kind")?;
    if source_line == 1 {
        event_kind = event_kind.strip_prefix('\u{feff}').unwrap_or(event_kind);
    }
    match event_kind.trim() {
        "L1" => process_l1(
            record,
            &file_name(csv_path),
            source_line,
            instrument,
            tick_size,
            timestamps,
            validation,
            writer,
            current_bid,
            current_ask,
            sequence_id,
        ),
        "L2" => process_l2(
            record,
            &file_name(csv_path),
            source_line,
            tick_size,
            timestamps,
            validation,
        ),
        value => bail!("unknown event kind {value:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn process_l1(
    record: &ByteRecord,
    source_file: &str,
    source_line: i64,
    instrument: &str,
    tick_size: ExactDecimal,
    timestamps: &mut TimestampConverter,
    validation: &mut FileValidation,
    writer: &mut TradesParquetWriter,
    current_bid: &mut Option<Price>,
    current_ask: &mut Option<Price>,
    sequence_id: &mut i64,
) -> Result<()> {
    if record.len() != 6 {
        bail!(
            "{source_file}:{source_line}: L1 expected 6 columns, got {}",
            record.len()
        );
    }
    let market_data_type = parse_i16(record.get(1), "MarketDataType")?;
    if !(0..=10).contains(&market_data_type) {
        bail!("{source_file}:{source_line}: unknown L1 MarketDataType {market_data_type}");
    }
    let timestamp_text = text(record.get(2), "timestamp")?.trim();
    let offset_100ns = parse_i64(record.get(3), "offset100ns")?;
    let timestamp_ns = timestamps.convert(timestamp_text, offset_100ns)?;
    let price = Price::parse(text(record.get(4), "price")?)?;
    let volume = parse_i64(record.get(5), "volume")?;

    record_common_validation(
        validation,
        timestamp_ns,
        price,
        volume,
        tick_size,
        source_file,
        source_line,
    )?;
    validation.l1 += 1;
    validation.l1_by_type[market_data_type as usize] += 1;
    if market_data_type == 2 {
        validation.last += 1;
    } else if market_data_type == 5 {
        validation.daily_volume_count += 1;
        if validation
            .previous_daily_volume
            .is_some_and(|previous| volume < previous)
        {
            validation.daily_volume_decreases += 1;
        }
        validation.previous_daily_volume = Some(volume);
    }

    match market_data_type {
        1 => *current_bid = Some(price),
        0 => *current_ask = Some(price),
        2 => {
            if volume <= 0 {
                validation.invalid_size += 1;
            }
            let missing_bbo = current_bid.is_none() || current_ask.is_none();
            if missing_bbo {
                validation.missing_bbo_before_last += 1;
                validation.warning(format!(
                    "Missing BBO before Last at {source_file}:{source_line}"
                ));
            }
            let bid = current_bid.map(|value| value.min(price));
            let ask = current_ask.map(|value| value.max(price));
            if bid.zip(ask).is_some_and(|(bid, ask)| bid > ask) {
                validation.invalid_bbo += 1;
            }
            let aggressor = if missing_bbo {
                "UNKNOWN"
            } else if ask.is_some_and(|ask| price >= ask) {
                "BUY"
            } else if bid.is_some_and(|bid| price <= bid) {
                "SELL"
            } else {
                "UNKNOWN"
            };
            match aggressor {
                "BUY" => validation.buy += 1,
                "SELL" => validation.sell += 1,
                _ => validation.unknown += 1,
            }
            if bid
                .zip(ask)
                .is_some_and(|(bid, ask)| price < bid || price > ask)
            {
                validation.last_outside_bbo += 1;
            }
            *sequence_id += 1;
            validation.trades_written += 1;
            writer.append(TradeRow {
                sequence_id: *sequence_id,
                timestamp_ns,
                instrument,
                price,
                size: volume,
                bid,
                ask,
                aggressor,
            })?;
        }
        _ => {}
    }
    Ok(())
}

fn process_l2(
    record: &ByteRecord,
    source_file: &str,
    source_line: i64,
    tick_size: ExactDecimal,
    timestamps: &mut TimestampConverter,
    validation: &mut FileValidation,
) -> Result<()> {
    if record.len() != 9 {
        bail!(
            "{source_file}:{source_line}: L2 expected 9 columns, got {}",
            record.len()
        );
    }
    let market_data_type = parse_i16(record.get(1), "MarketDataType")?;
    if !matches!(market_data_type, 0 | 1) {
        bail!("{source_file}:{source_line}: unknown L2 MarketDataType {market_data_type}");
    }
    let timestamp_text = text(record.get(2), "timestamp")?.trim();
    let offset_100ns = parse_i64(record.get(3), "offset100ns")?;
    let timestamp_ns = timestamps.convert(timestamp_text, offset_100ns)?;
    let operation = parse_i8(record.get(4), "Operation")?;
    if !(0..=2).contains(&operation) {
        bail!("{source_file}:{source_line}: unknown L2 Operation {operation}");
    }
    let position = parse_i32(record.get(5), "Position")?;
    if position < 0 {
        bail!("{source_file}:{source_line}: invalid Position {position}");
    }
    text(record.get(6), "MarketMaker")?;
    let price = Price::parse(text(record.get(7), "price")?)?;
    let volume = parse_i64(record.get(8), "Volume")?;
    record_common_validation(
        validation,
        timestamp_ns,
        price,
        volume,
        tick_size,
        source_file,
        source_line,
    )?;
    validation.l2 += 1;
    Ok(())
}

fn record_common_validation(
    validation: &mut FileValidation,
    timestamp_ns: i64,
    price: Price,
    volume: i64,
    tick_size: ExactDecimal,
    source_file: &str,
    source_line: i64,
) -> Result<()> {
    validation.total_events += 1;
    if validation
        .previous_timestamp_ns
        .is_some_and(|previous| timestamp_ns < previous)
    {
        validation.backward_timestamps += 1;
    }
    validation.previous_timestamp_ns = Some(timestamp_ns);
    if volume < 0 {
        validation.invalid_size += 1;
    }
    if !price.is_aligned(tick_size)? {
        validation.tick_misaligned += 1;
        validation.warning(format!("Tick misalignment at {source_file}:{source_line}"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_failure_validation(
    paths: &DailyOutputPaths,
    source: &Path,
    contract: &str,
    instrument: &str,
    date: &str,
    tick_size: ExactDecimal,
    validation: &FileValidation,
    message: &str,
) {
    let mut totals = Totals::default();
    totals.add(validation);
    let document = ValidationDocument::new(
        source.to_string_lossy().into_owned(),
        contract.to_string(),
        instrument.to_string(),
        date.to_string(),
        tick_size,
        vec![validation.report(source)],
        totals,
        Vec::new(),
        vec![message.to_string()],
        Some("FAIL"),
    );
    if let Ok(text) = serde_json::to_string_pretty(&document) {
        let _ = fs::write(&paths.partial_validation, text);
    }
}

fn verify_daily_outputs(
    parquet_path: &Path,
    validation_path: &Path,
    contract: &str,
    date: &str,
    expected_rows: u64,
) -> Result<()> {
    if !parquet_path.is_file() || parquet_path.metadata()?.len() == 0 {
        bail!(
            "Daily Parquet does not exist or is empty: {}",
            parquet_path.display()
        );
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(parquet_path)?)
        .with_context(|| format!("Cannot read Parquet footer: {}", parquet_path.display()))?;
    if builder.schema().as_ref() != trades_schema().as_ref() {
        bail!("Daily Parquet schema does not match the TRADES schema");
    }
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows())?;
    if rows != expected_rows {
        bail!("Daily Parquet row count {rows} does not match rows_written {expected_rows}");
    }

    if !validation_path.is_file() {
        bail!(
            "Daily validation JSON is missing: {}",
            validation_path.display()
        );
    }
    let value: Value = serde_json::from_reader(BufReader::new(File::open(validation_path)?))
        .with_context(|| format!("Invalid validation JSON: {}", validation_path.display()))?;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .context("Validation JSON has no string status")?;
    if status == "FAIL" {
        bail!("Validation JSON status is FAIL");
    }
    if value.get("contract").and_then(Value::as_str) != Some(contract) {
        bail!("Validation JSON contract does not match {contract}");
    }
    if value.get("date").and_then(Value::as_str) != Some(date) {
        bail!("Validation JSON date does not match {date}");
    }
    let validation_rows = value
        .pointer("/totals/trades_written")
        .and_then(Value::as_u64)
        .context("Validation JSON has no totals.trades_written")?;
    if validation_rows != expected_rows {
        bail!("Validation row count {validation_rows} does not match {expected_rows}");
    }
    Ok(())
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn text<'a>(value: Option<&'a [u8]>, field: &str) -> Result<&'a str> {
    let bytes = value.ok_or_else(|| anyhow::anyhow!("missing {field}"))?;
    std::str::from_utf8(bytes).with_context(|| format!("invalid UTF-8 in {field}"))
}

fn trim_ascii(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn parse_i64(value: Option<&[u8]>, field: &str) -> Result<i64> {
    text(value, field)?
        .trim()
        .parse()
        .with_context(|| format!("invalid {field}"))
}

fn parse_i32(value: Option<&[u8]>, field: &str) -> Result<i32> {
    text(value, field)?
        .trim()
        .parse()
        .with_context(|| format!("invalid {field}"))
}

fn parse_i16(value: Option<&[u8]>, field: &str) -> Result<i16> {
    text(value, field)?
        .trim()
        .parse()
        .with_context(|| format!("invalid {field}"))
}

fn parse_i8(value: Option<&[u8]>, field: &str) -> Result<i8> {
    text(value, field)?
        .trim()
        .parse()
        .with_context(|| format!("invalid {field}"))
}
