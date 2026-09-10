use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use tempfile::TempDir;
use trade_converter_rust::converter::{
    ConversionOptions, DailyConversionResult, contract_from_folder_name, convert_day,
    daily_date_from_csv_path,
};
use trade_converter_rust::merge::{discover_daily_parquet_files, merge_contract};
use trade_converter_rust::parquet_writer::trades_schema;
use trade_converter_rust::timestamp::TimestampConverter;

const TEST_TICK_SIZE: &str = "0.25";

fn test_options() -> ConversionOptions<'static> {
    ConversionOptions {
        tick_size: TEST_TICK_SIZE,
        log: None,
    }
}

fn write(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
}

fn read_parquet(path: &Path) -> RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    concat_batches(&schema, &batches).unwrap()
}

fn fixture_dirs() -> (TempDir, PathBuf, PathBuf) {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("NQ SEP26");
    let daily = temp.path().join("NQ_09-26");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&daily).unwrap();
    (temp, source, daily)
}

fn run_single(contents: &str, tick_size: &str) -> (TempDir, DailyConversionResult) {
    let (temp, source, daily) = fixture_dirs();
    let csv = source.join("20260818.csv");
    write(&csv, contents);
    let result = convert_day(
        &csv,
        &daily,
        ConversionOptions {
            tick_size,
            log: None,
        },
    )
    .unwrap();
    (temp, result)
}

fn daily_csv(date: &str, prices: &[i64]) -> String {
    let timestamp = format!("{date}120000");
    let mut csv = format!("L1;1;{timestamp};0;0;1\nL1;0;{timestamp};1;999999;1\n");
    for (index, price) in prices.iter().enumerate() {
        csv.push_str(&format!("L1;2;{timestamp};{};{price};1\n", index + 2));
    }
    csv
}

fn convert_fixture_day(source: &Path, daily: &Path, date: &str, prices: &[i64]) {
    let csv = source.join(format!("{date}.csv"));
    write(&csv, &daily_csv(date, prices));
    convert_day(&csv, daily, test_options()).unwrap();
}

fn final_output(temp: &TempDir) -> PathBuf {
    let final_dir = temp.path().join("final");
    fs::create_dir(&final_dir).unwrap();
    final_dir.join("NQ_09-26.parquet")
}

#[test]
fn parses_contract_and_strict_daily_filename() {
    assert_eq!(contract_from_folder_name("NQ SEP26").unwrap(), "NQ_09-26");
    assert_eq!(
        daily_date_from_csv_path(Path::new("20260615.csv")).unwrap(),
        "20260615"
    );
    assert!(daily_date_from_csv_path(Path::new("2026061.csv")).is_err());
    assert!(daily_date_from_csv_path(Path::new("20260230.csv")).is_err());
    assert!(daily_date_from_csv_path(Path::new("20260615.CSV")).is_err());
    assert!(contract_from_folder_name("20260615").is_err());
}

#[test]
fn daily_naming_schema_and_validation_identity_are_exact() {
    let (_temp, result) = run_single(
        "L1;1;20260818120000;0;100;1\nL1;0;20260818120000;1;101;1\nL1;2;20260818120000;2;100.5;2\n",
        TEST_TICK_SIZE,
    );
    assert_eq!(result.contract, "NQ_09-26");
    assert_eq!(result.date, "20260818");
    assert_eq!(result.parquet_path.file_name().unwrap(), "20260818.parquet");
    assert_eq!(
        result.validation_path.file_name().unwrap(),
        "20260818_validation.json"
    );
    assert_eq!(result.rows_written, 1);
    let mut output_names = fs::read_dir(result.parquet_path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    output_names.sort();
    assert_eq!(
        output_names,
        vec!["20260818.parquet", "20260818_validation.json"]
    );

    let schema = trades_schema();
    assert_eq!(schema.fields().len(), 8);
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec![
            "sequence_id",
            "timestamp_chicago",
            "instrument",
            "price",
            "size",
            "bid",
            "ask",
            "aggressor"
        ]
    );
    assert_eq!(
        schema
            .field_with_name("timestamp_chicago")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Chicago".into()))
    );
    for name in ["price", "bid", "ask"] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(38, 12)
        );
    }
    assert!(schema.fields().iter().all(|field| field.is_nullable()));

    let validation: Value =
        serde_json::from_slice(&fs::read(&result.validation_path).unwrap()).unwrap();
    assert_eq!(validation["status"], "PASS");
    assert_eq!(validation["contract"], "NQ_09-26");
    assert_eq!(validation["date"], "20260818");
    assert_eq!(validation["totals"]["trades_written"], 1);
    assert_eq!(validation["totals"]["total_events"], 3);
}

#[test]
fn bbo_aggressor_nulls_exact_prices_sequence_and_100ns_are_preserved() {
    let rows = [
        "L1;2;20260818120000;1;100,25;2",
        "L1;1;20260818120000;2;100,00;1",
        "L1;0;20260818120000;3;101,00;1",
        "L1;2;20260818120000;360000;101,00;3",
        "L1;2;20260818120000;360001;100,00;4",
        "L1;2;20260818120000;360002;100,50;5",
    ]
    .join("\n");
    let (_temp, result) = run_single(&(rows + "\n"), TEST_TICK_SIZE);
    let trades = read_parquet(&result.parquet_path);
    assert_eq!(trades.num_rows(), 4);

    let ids = trades
        .column_by_name("sequence_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3, 4]);
    let prices = trades
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(prices.value(0), 100_250_000_000_000);
    assert_eq!(prices.value(3), 100_500_000_000_000);
    let bid = trades
        .column_by_name("bid")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let ask = trades
        .column_by_name("ask")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(bid.is_null(0));
    assert!(ask.is_null(0));
    assert_eq!(bid.value(1), 100_000_000_000_000);
    assert_eq!(ask.value(1), 101_000_000_000_000);
    let aggressor = trades
        .column_by_name("aggressor")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        aggressor.iter().collect::<Vec<_>>(),
        vec![Some("UNKNOWN"), Some("BUY"), Some("SELL"), Some("UNKNOWN")]
    );
    let timestamps = trades
        .column_by_name("timestamp_chicago")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let mut expected = TimestampConverter::default();
    assert_eq!(
        timestamps.value(1),
        expected.convert("20260818120000", 360_000).unwrap()
    );
    assert_eq!(timestamps.value(1) % 1_000_000_000, 36_000_000);

    let validation: Value =
        serde_json::from_slice(&fs::read(result.validation_path).unwrap()).unwrap();
    assert_eq!(validation["status"], "WARNING");
    assert_eq!(validation["totals"]["missing_bbo_before_last"], 1);
    assert_eq!(validation["totals"]["buy"], 1);
    assert_eq!(validation["totals"]["sell"], 1);
    assert_eq!(validation["totals"]["unknown"], 2);
}

#[test]
fn each_daily_conversion_has_fresh_bbo_state() {
    let (_temp, source, daily) = fixture_dirs();
    write(
        &source.join("20260713.csv"),
        "L1;1;20260713120000;0;99;1\nL1;0;20260713120000;1;101;1\nL1;2;20260713120000;2;100;1\n",
    );
    write(
        &source.join("20260714.csv"),
        "L1;1;20260714120000;0;199;1\nL1;2;20260714120000;1;200;2\n",
    );
    convert_day(&source.join("20260713.csv"), &daily, test_options()).unwrap();
    let second = convert_day(&source.join("20260714.csv"), &daily, test_options()).unwrap();
    let trades = read_parquet(&second.parquet_path);
    let asks = trades
        .column_by_name("ask")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let aggressors = trades
        .column_by_name("aggressor")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(asks.is_null(0));
    assert_eq!(aggressors.value(0), "UNKNOWN");
}

#[test]
fn tick_size_changes_only_validation_and_l2_remains_stream_parseable() {
    let contents = concat!(
        "L2;0;20260818115959;3;0;1;\"MM;ONE\";100,10;5\n",
        "L1;1;20260818120000;0;100,10;1\n",
        "L1;0;20260818120000;1;101,10;1\n",
        "L1;2;20260818120000;2;100,10;1\n"
    );
    let (_temp_a, fine_tick) = run_single(contents, "0.01");
    let (_temp_b, coarse_tick) = run_single(contents, "0.25");
    assert_eq!(
        read_parquet(&fine_tick.parquet_path),
        read_parquet(&coarse_tick.parquet_path)
    );
    let fine: Value =
        serde_json::from_slice(&fs::read(fine_tick.validation_path).unwrap()).unwrap();
    let coarse: Value =
        serde_json::from_slice(&fs::read(coarse_tick.validation_path).unwrap()).unwrap();
    assert_eq!(fine["tick_size"], "0.01");
    assert_eq!(fine["tick_alignment"]["status"], "PERFORMED");
    assert_eq!(fine["totals"]["tick_misaligned"], 0);
    assert_eq!(coarse["tick_size"], "0.25");
    assert_eq!(coarse["tick_alignment"]["status"], "PERFORMED");
    assert_eq!(coarse["totals"]["tick_misaligned"], 4);
    assert_eq!(coarse["totals"]["l2"], 1);
}

#[test]
fn malformed_input_keeps_only_partial_outputs_and_overwrite_is_refused() {
    let (temp, source, daily) = fixture_dirs();
    let csv = source.join("20260818.csv");
    write(&csv, "L1;1;bad;0;100;1\n");
    assert!(convert_day(&csv, &daily, test_options()).is_err());
    assert!(!daily.join("20260818.parquet").exists());
    assert!(!daily.join("20260818_validation.json").exists());
    assert!(daily.join("20260818.partial.parquet").exists());
    let validation: Value =
        serde_json::from_slice(&fs::read(daily.join("20260818_validation.partial.json")).unwrap())
            .unwrap();
    assert_eq!(validation["status"], "FAIL");

    let (_keep, source, daily) = fixture_dirs();
    convert_fixture_day(&source, &daily, "20260818", &[100]);
    let good_csv = source.join("20260818.csv");
    let error = convert_day(&good_csv, &daily, test_options()).unwrap_err();
    assert!(error.to_string().contains("Refusing to overwrite"));
    drop(temp);
}

#[test]
fn merge_sorts_days_regenerates_global_sequence_and_sums_rows() {
    let (temp, source, daily) = fixture_dirs();
    convert_fixture_day(&source, &daily, "20260617", &[170]);
    convert_fixture_day(&source, &daily, "20260615", &[150, 151, 152]);
    convert_fixture_day(&source, &daily, "20260616", &[160, 161]);
    write(&daily.join("notes.parquet"), "ignored");
    write(&daily.join("20260618.partial.parquet"), "ignored");

    let discovered = discover_daily_parquet_files(&daily).unwrap();
    assert_eq!(
        discovered
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy())
            .collect::<Vec<_>>(),
        vec!["20260615.parquet", "20260616.parquet", "20260617.parquet"]
    );

    let output = final_output(&temp);
    let result = merge_contract(&daily, &output).unwrap();
    assert_eq!(result.contract, "NQ_09-26");
    assert_eq!(result.days_merged, 3);
    assert_eq!(result.rows_written, 6);
    let merged = read_parquet(&output);
    let ids = merged
        .column_by_name("sequence_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3, 4, 5, 6]);
    let prices = merged
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        prices.values(),
        &[
            150_000_000_000_000,
            151_000_000_000_000,
            152_000_000_000_000,
            160_000_000_000_000,
            161_000_000_000_000,
            170_000_000_000_000,
        ]
    );
    assert!(daily.join("20260615.parquet").exists());
    assert!(source.join("20260615.csv").exists());

    let error = merge_contract(&daily, &output).unwrap_err();
    assert!(error.to_string().contains("Refusing to overwrite"));
}

#[test]
fn merge_rejects_schema_mismatch_without_final_output() {
    let (temp, source, daily) = fixture_dirs();
    convert_fixture_day(&source, &daily, "20260615", &[150]);
    let path = daily.join("20260615.parquet");
    fs::remove_file(&path).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let output = final_output(&temp);
    let error = merge_contract(&daily, &output).unwrap_err();
    assert!(error.to_string().contains("incompatible Arrow schema"));
    assert!(!output.exists());
}

#[test]
fn merge_rejects_timestamp_regression_without_final_output() {
    let (temp, source, daily) = fixture_dirs();
    let csv = source.join("20260615.csv");
    write(
        &csv,
        concat!(
            "L1;1;20260615120000;0;99;1\n",
            "L1;0;20260615120000;1;101;1\n",
            "L1;2;20260615120001;0;100;1\n",
            "L1;2;20260615120000;2;100;1\n"
        ),
    );
    let daily_result = convert_day(&csv, &daily, test_options()).unwrap();
    assert_eq!(daily_result.metrics.status, "WARNING");

    let output = final_output(&temp);
    let error = merge_contract(&daily, &output).unwrap_err();
    assert!(error.to_string().contains("not chronologically ordered"));
    assert!(!output.exists());
    assert!(daily.join("20260615.parquet").exists());
}

#[test]
fn merge_rejects_timestamp_regression_across_daily_boundary() {
    let (temp, source, daily) = fixture_dirs();
    let first = source.join("20260615.csv");
    let second = source.join("20260616.csv");
    write(&first, &daily_csv("20260617", &[150]));
    write(&second, &daily_csv("20260616", &[160]));
    convert_day(&first, &daily, test_options()).unwrap();
    convert_day(&second, &daily, test_options()).unwrap();

    let output = final_output(&temp);
    let error = merge_contract(&daily, &output).unwrap_err();
    assert!(error.to_string().contains("not chronologically ordered"));
    assert!(!output.exists());
}

#[test]
fn cli_requires_a_strictly_positive_finite_tick_size() {
    let (temp, source, daily) = fixture_dirs();
    let csv = source.join("20260818.csv");
    write(&csv, &daily_csv("20260818", &[100]));
    let binary = env!("CARGO_BIN_EXE_trade-converter-rust");

    let no_subcommand = Command::new(binary).output().unwrap();
    assert!(!no_subcommand.status.success());
    let help = format!(
        "{}{}",
        String::from_utf8_lossy(&no_subcommand.stdout),
        String::from_utf8_lossy(&no_subcommand.stderr)
    );
    assert!(help.contains("Usage:"));

    let missing = Command::new(binary)
        .arg("convert-day")
        .arg(&csv)
        .arg(&daily)
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--tick-size"));

    for invalid in ["0", "-0.25", "NaN", "inf", "-inf"] {
        let output = Command::new(binary)
            .arg("convert-day")
            .arg(&csv)
            .arg(&daily)
            .arg("--tick-size")
            .arg(invalid)
            .output()
            .unwrap();
        assert!(!output.status.success(), "tick size {invalid} was accepted");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("PARQUET_FAILED|"),
            "tick size {invalid} did not reach exact core validation"
        );
    }

    let accepted = Command::new(binary)
        .arg("convert-day")
        .arg(&csv)
        .arg(&daily)
        .arg("--tick-size")
        .arg(TEST_TICK_SIZE)
        .output()
        .unwrap();
    assert!(accepted.status.success());
    assert!(daily.join("20260818.parquet").exists());
    drop(temp);
}

#[test]
fn cli_protocol_is_machine_readable_and_last_on_stdout() {
    let (temp, source, daily) = fixture_dirs();
    let csv = source.join("20260818.csv");
    write(&csv, &daily_csv("20260818", &[100]));
    let output = Command::new(env!("CARGO_BIN_EXE_trade-converter-rust"))
        .arg("convert-day")
        .arg(&csv)
        .arg(&daily)
        .arg("--tick-size")
        .arg(TEST_TICK_SIZE)
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let last = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .unwrap();
    assert!(last.starts_with("PARQUET_DONE|NQ_09-26|20260818|"));
    assert!(last.ends_with("|1"));

    let merged_path = final_output(&temp);
    let merged = Command::new(env!("CARGO_BIN_EXE_trade-converter-rust"))
        .arg("merge-contract")
        .arg(&daily)
        .arg(&merged_path)
        .output()
        .unwrap();
    assert!(merged.status.success());
    let stdout = String::from_utf8(merged.stdout).unwrap();
    let last = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .unwrap();
    assert!(last.starts_with("MERGE_DONE|NQ_09-26|"));
    assert!(last.ends_with("|1|1"));

    let merge_failed = Command::new(env!("CARGO_BIN_EXE_trade-converter-rust"))
        .arg("merge-contract")
        .arg(&daily)
        .arg(&merged_path)
        .output()
        .unwrap();
    assert!(!merge_failed.status.success());
    let stderr = String::from_utf8(merge_failed.stderr).unwrap();
    let last = stderr
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .unwrap();
    assert!(last.starts_with("MERGE_FAILED|NQ_09-26|"));

    let bad_csv = source.join("bad.csv");
    write(&bad_csv, "bad\n");
    let failed = Command::new(env!("CARGO_BIN_EXE_trade-converter-rust"))
        .arg("convert-day")
        .arg(&bad_csv)
        .arg(&daily)
        .arg("--tick-size")
        .arg(TEST_TICK_SIZE)
        .output()
        .unwrap();
    assert!(!failed.status.success());
    let stderr = String::from_utf8(failed.stderr).unwrap();
    let last = stderr
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .unwrap();
    assert!(last.starts_with("PARQUET_FAILED|NQ_09-26|UNKNOWN|"));
    assert!(!last.contains('\r'));
}
