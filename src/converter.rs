use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use csv::{ByteRecord, ReaderBuilder};
use memory_stats::memory_stats;
use regex::Regex;
use serde::Serialize;

use crate::decimal::{ExactDecimal, Price};
use crate::parquet_writer::{RawParquetWriter, RawRow, TradeRow, TradesParquetWriter};
use crate::timestamp::TimestampConverter;
use crate::validation::{FileReport, FileValidation, Totals, ValidationDocument};

pub type LogCallback = dyn Fn(&str) + Send + Sync;

#[derive(Default)]
pub struct ConversionOptions<'a> {
    pub tick_size: Option<&'a str>,
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
    pub csv_read_seconds: f64,
    pub processing_seconds: f64,
    pub parquet_write_seconds: f64,
    pub raw_bytes: u64,
    pub trades_bytes: u64,
    pub validation_bytes: u64,
    pub status: String,
}

#[derive(Debug)]
pub struct ConversionResult {
    pub raw_path: PathBuf,
    pub trades_path: PathBuf,
    pub validation_path: PathBuf,
    pub status: String,
    pub metrics: Metrics,
}

#[derive(Debug)]
struct OutputPaths {
    raw: PathBuf,
    trades: PathBuf,
    validation: PathBuf,
    partial_raw: PathBuf,
    partial_trades: PathBuf,
    partial_validation: PathBuf,
}

pub fn convert_source(
    source: &Path,
    destination: &Path,
    options: ConversionOptions<'_>,
) -> Result<ConversionResult> {
    let started = Instant::now();
    let source = source
        .canonicalize()
        .with_context(|| format!("Source does not exist: {}", source.display()))?;
    let destination = destination
        .canonicalize()
        .with_context(|| format!("Destination does not exist: {}", destination.display()))?;
    if !destination.is_dir() {
        bail!(
            "Destination must be an existing folder: {}",
            destination.display()
        );
    }
    let tick_size = match options
        .tick_size
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => {
            let tick = ExactDecimal::parse(value).context("Invalid Tick Size")?;
            if !tick.is_positive() {
                bail!("Tick Size must be a finite number greater than zero");
            }
            Some(tick)
        }
        None => None,
    };
    let csv_files = discover_csv_files(&source)?;
    if csv_files.is_empty() {
        bail!("No CSV files found in source");
    }
    let contract = detect_contract(&source)?;
    let instrument = contract.split('_').next().unwrap_or(&contract).to_string();
    let paths = output_paths(&destination, &contract);
    for path in [
        &paths.raw,
        &paths.trades,
        &paths.validation,
        &paths.partial_raw,
        &paths.partial_trades,
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
    log(&format!(
        "Found {} CSV file{}",
        csv_files.len(),
        if csv_files.len() == 1 { "" } else { "s" }
    ));
    log(&format!("Detected contract: {contract}"));
    match tick_size {
        Some(value) => log(&format!(
            "Tick alignment: using Tick Size {}",
            crate::decimal::format_exact(value)
        )),
        None => log("Tick alignment: not performed (Tick Size is empty)"),
    }

    let mut warnings = Vec::new();
    for path in &csv_files {
        if !is_daily_csv(path) {
            warnings.push(format!("Unexpected CSV filename: {}", file_name(path)));
        }
    }

    let mut raw_writer = RawParquetWriter::create(&paths.partial_raw)?;
    let mut trades_writer = TradesParquetWriter::create(&paths.partial_trades)?;
    let mut files = Vec::<FileReport>::new();
    let mut totals = Totals::default();
    let mut sequence_id = 0i64;
    let csv_read_time = Duration::ZERO;
    let mut processing_time = Duration::ZERO;
    let mut peak_rss = memory_stats().map_or(0, |stats| stats.physical_mem);

    for (file_index, csv_path) in csv_files.iter().enumerate() {
        log(&format!(
            "[{}/{}] Processing {}",
            file_index + 1,
            csv_files.len(),
            file_name(csv_path)
        ));
        let source_file = file_name(csv_path);
        let file = File::open(csv_path)?;
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
        let mut source_line = 0i64;

        let file_processing_started = Instant::now();
        loop {
            let has_record = match reader.read_byte_record(&mut record) {
                Ok(value) => value,
                Err(error) => {
                    let message = format!("{}:{}: {}", source_file, source_line + 1, error);
                    validation.parse_errors += 1;
                    validation.malformed_rows += 1;
                    validation.errors.push(message.clone());
                    files.push(validation.report(csv_path));
                    totals.add(&validation);
                    let _ = raw_writer.close();
                    let _ = trades_writer.close();
                    write_failure_validation(
                        &paths,
                        &source,
                        &contract,
                        &instrument,
                        tick_size,
                        files,
                        totals,
                        warnings,
                        &message,
                    );
                    bail!(message);
                }
            };
            if !has_record {
                break;
            }
            source_line += 1;
            if record.is_empty() || record.iter().all(|field| trim_ascii(field).is_empty()) {
                bail!("{}:{}: blank row", source_file, source_line);
            }
            let mut event_kind = text(record.get(0), "event kind")?;
            if source_line == 1 {
                event_kind = event_kind.strip_prefix('\u{feff}').unwrap_or(event_kind);
            }
            let event_kind = event_kind.trim();
            let process_result = match event_kind {
                "L1" => process_l1(
                    &record,
                    &source_file,
                    source_line,
                    &instrument,
                    tick_size,
                    &mut timestamps,
                    &mut validation,
                    &mut raw_writer,
                    &mut trades_writer,
                    &mut current_bid,
                    &mut current_ask,
                    &mut sequence_id,
                ),
                "L2" => process_l2(
                    &record,
                    &source_file,
                    source_line,
                    tick_size,
                    &mut timestamps,
                    &mut validation,
                    &mut raw_writer,
                ),
                _ => Err(anyhow::anyhow!(
                    "{}:{}: unknown event kind {:?}",
                    source_file,
                    source_line,
                    event_kind
                )),
            };
            if let Err(error) = process_result {
                let raw_message = error.to_string();
                let message = if raw_message.starts_with(&format!("{source_file}:")) {
                    raw_message
                } else {
                    format!("{source_file}:{source_line}: {raw_message}")
                };
                validation.parse_errors += 1;
                validation.malformed_rows += 1;
                validation.errors.push(message.clone());
                files.push(validation.report(csv_path));
                totals.add(&validation);
                let _ = raw_writer.close();
                let _ = trades_writer.close();
                write_failure_validation(
                    &paths,
                    &source,
                    &contract,
                    &instrument,
                    tick_size,
                    files,
                    totals,
                    warnings,
                    &message,
                );
                bail!(message);
            }
            if source_line % 50_000 == 0
                && let Some(stats) = memory_stats()
            {
                peak_rss = peak_rss.max(stats.physical_mem);
            }
        }
        processing_time += file_processing_started.elapsed();
        if validation.raw_rows == 0 {
            let message = format!("{}: CSV contains no events", source_file);
            validation.parse_errors += 1;
            validation.malformed_rows += 1;
            validation.errors.push(message.clone());
            files.push(validation.report(csv_path));
            totals.add(&validation);
            let _ = raw_writer.close();
            let _ = trades_writer.close();
            write_failure_validation(
                &paths,
                &source,
                &contract,
                &instrument,
                tick_size,
                files,
                totals,
                warnings,
                &message,
            );
            bail!(message);
        }
        let status = validation.status();
        log(&format!(
            "L1: {}  L2: {}  Trades: {}  Validation: {}",
            validation.l1, validation.l2, validation.trades_written, status
        ));
        files.push(validation.report(csv_path));
        totals.add(&validation);
    }

    let raw_write_time = raw_writer.close()?;
    let trades_write_time = trades_writer.close()?;
    let validation_document = ValidationDocument::new(
        source.to_string_lossy().into_owned(),
        contract.clone(),
        instrument,
        tick_size,
        files,
        totals.clone(),
        warnings,
        Vec::new(),
        None,
    );
    let validation_text = serde_json::to_string_pretty(&validation_document)?;
    fs::write(&paths.partial_validation, validation_text)?;
    fs::rename(&paths.partial_raw, &paths.raw)?;
    fs::rename(&paths.partial_trades, &paths.trades)?;
    fs::rename(&paths.partial_validation, &paths.validation)?;

    let elapsed = started.elapsed().as_secs_f64();
    let input_bytes = csv_files
        .iter()
        .try_fold(0u64, |sum, path| -> Result<u64> {
            Ok(sum + path.metadata()?.len())
        })?;
    if let Some(stats) = memory_stats() {
        peak_rss = peak_rss.max(stats.physical_mem);
    }
    let metrics = Metrics {
        input_bytes,
        input_mb: input_bytes as f64 / 1_000_000.0,
        rows: totals.raw_rows,
        events: totals.l1 + totals.l2,
        elapsed_seconds: elapsed,
        input_mb_per_second: input_bytes as f64 / 1_000_000.0 / elapsed,
        rows_per_second: totals.raw_rows as f64 / elapsed,
        events_per_second: (totals.l1 + totals.l2) as f64 / elapsed,
        peak_rss_bytes: peak_rss,
        csv_read_seconds: csv_read_time.as_secs_f64(),
        processing_seconds: processing_time.as_secs_f64(),
        parquet_write_seconds: (raw_write_time + trades_write_time).as_secs_f64(),
        raw_bytes: paths.raw.metadata()?.len(),
        trades_bytes: paths.trades.metadata()?.len(),
        validation_bytes: paths.validation.metadata()?.len(),
        status: validation_document.status.clone(),
    };
    log("Conversion complete.");
    log(&format!("RAW: {}", paths.raw.display()));
    log(&format!("TRADES: {}", paths.trades.display()));
    log(&format!("VALIDATION: {}", paths.validation.display()));
    log(&format!("Overall status: {}", metrics.status));
    Ok(ConversionResult {
        raw_path: paths.raw,
        trades_path: paths.trades,
        validation_path: paths.validation,
        status: metrics.status.clone(),
        metrics,
    })
}

#[allow(clippy::too_many_arguments)]
fn process_l1(
    record: &ByteRecord,
    source_file: &str,
    source_line: i64,
    instrument: &str,
    tick_size: Option<ExactDecimal>,
    timestamps: &mut TimestampConverter,
    validation: &mut FileValidation,
    raw_writer: &mut RawParquetWriter,
    trades_writer: &mut TradesParquetWriter,
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
    let source_timestamp = text(record.get(2), "timestamp")?;
    let timestamp_text = source_timestamp.trim();
    let offset_100ns = parse_i64(record.get(3), "offset100ns")?;
    let timestamp_ns = timestamps.convert(timestamp_text, offset_100ns)?;
    let price_text = text(record.get(4), "price")?;
    let price = Price::parse(price_text)?;
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

    raw_writer.append(RawRow {
        source_file,
        source_line,
        event_kind: "L1",
        market_data_type,
        source_timestamp,
        offset_100ns,
        timestamp_ns,
        price,
        price_text,
        volume,
        operation: None,
        position: None,
        market_maker: None,
    })?;

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
            trades_writer.append(TradeRow {
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
    tick_size: Option<ExactDecimal>,
    timestamps: &mut TimestampConverter,
    validation: &mut FileValidation,
    raw_writer: &mut RawParquetWriter,
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
    let source_timestamp = text(record.get(2), "timestamp")?;
    let offset_100ns = parse_i64(record.get(3), "offset100ns")?;
    let timestamp_ns = timestamps.convert(source_timestamp.trim(), offset_100ns)?;
    let operation = parse_i8(record.get(4), "Operation")?;
    if !(0..=2).contains(&operation) {
        bail!("{source_file}:{source_line}: unknown L2 Operation {operation}");
    }
    let position = parse_i32(record.get(5), "Position")?;
    if position < 0 {
        bail!("{source_file}:{source_line}: invalid Position {position}");
    }
    let market_maker = text(record.get(6), "MarketMaker")?;
    let price_text = text(record.get(7), "price")?;
    let price = Price::parse(price_text)?;
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
    raw_writer.append(RawRow {
        source_file,
        source_line,
        event_kind: "L2",
        market_data_type,
        source_timestamp,
        offset_100ns,
        timestamp_ns,
        price,
        price_text,
        volume,
        operation: Some(operation),
        position: Some(position),
        market_maker: Some(market_maker),
    })?;
    Ok(())
}

fn record_common_validation(
    validation: &mut FileValidation,
    timestamp_ns: i64,
    price: Price,
    volume: i64,
    tick_size: Option<ExactDecimal>,
    source_file: &str,
    source_line: i64,
) -> Result<()> {
    validation.raw_rows += 1;
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
    if let Some(tick) = tick_size
        && !price.is_aligned(tick)?
    {
        validation.tick_misaligned += 1;
        validation.warning(format!("Tick misalignment at {source_file}:{source_line}"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_failure_validation(
    paths: &OutputPaths,
    source: &Path,
    contract: &str,
    instrument: &str,
    tick_size: Option<ExactDecimal>,
    files: Vec<FileReport>,
    totals: Totals,
    warnings: Vec<String>,
    message: &str,
) {
    let document = ValidationDocument::new(
        source.to_string_lossy().into_owned(),
        contract.to_string(),
        instrument.to_string(),
        tick_size,
        files,
        totals,
        warnings,
        vec![message.to_string()],
        Some("FAIL"),
    );
    if let Ok(text) = serde_json::to_string_pretty(&document) {
        let _ = fs::write(&paths.partial_validation, text);
    }
}

pub fn discover_csv_files(source: &Path) -> Result<Vec<PathBuf>> {
    if source.is_file() {
        if source
            .extension()
            .is_none_or(|ext| !ext.eq_ignore_ascii_case("csv"))
        {
            bail!("Single-file source must be a .csv file");
        }
        return Ok(vec![source.to_path_buf()]);
    }
    if !source.is_dir() {
        bail!("Unsupported source: {}", source.display());
    }
    let mut files = fs::read_dir(source)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|path| {
        if is_daily_csv(path) {
            (0, file_name(path))
        } else {
            (1, file_name(path).to_lowercase())
        }
    });
    for pair in files.windows(2) {
        if is_daily_csv(&pair[0]) && file_stem(&pair[0]) == file_stem(&pair[1]) {
            bail!(
                "Duplicate daily CSV date {}: {:?} and {:?}",
                file_stem(&pair[0]),
                file_name(&pair[0]),
                file_name(&pair[1])
            );
        }
    }
    Ok(files)
}

fn detect_contract(source: &Path) -> Result<String> {
    if source.is_file() {
        return Ok(file_stem(source));
    }
    let folder = source.file_name().unwrap_or_default().to_string_lossy();
    let pattern = Regex::new(r"^([A-Za-z0-9]+)\s+([A-Za-z]{3})(\d{2})$")?;
    let captures = pattern.captures(folder.trim()).ok_or_else(|| {
        anyhow::anyhow!(
            "Cannot confidently detect futures contract from folder name: {:?}",
            folder
        )
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

fn output_paths(destination: &Path, contract: &str) -> OutputPaths {
    let raw = destination.join(format!("{contract}_RAW.parquet"));
    let trades = destination.join(format!("{contract}_TRADES.parquet"));
    let validation = destination.join(format!("{contract}_validation.json"));
    OutputPaths {
        partial_raw: destination.join(format!("{contract}_RAW.partial.parquet")),
        partial_trades: destination.join(format!("{contract}_TRADES.partial.parquet")),
        partial_validation: destination.join(format!("{contract}_validation.partial.json")),
        raw,
        trades,
        validation,
    }
}

fn is_daily_csv(path: &Path) -> bool {
    let stem = file_stem(path);
    stem.len() == 8 && stem.bytes().all(|byte| byte.is_ascii_digit())
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
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
