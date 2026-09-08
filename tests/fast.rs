use std::fs::{self, File};
use std::path::Path;

use arrow::array::{Array, Decimal128Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use tempfile::TempDir;
use trade_converter_rust::converter::{ConversionOptions, convert_source, discover_csv_files};
use trade_converter_rust::parquet_writer::{BATCH_SIZE, raw_schema, trades_schema};
use trade_converter_rust::timestamp::TimestampConverter;

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

fn run_single(
    contents: &str,
    tick: Option<&str>,
) -> (TempDir, trade_converter_rust::ConversionResult) {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("20260818.csv");
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    write(&source, contents);
    let result = convert_source(
        &source,
        &output,
        ConversionOptions {
            tick_size: tick,
            log: None,
        },
    )
    .unwrap();
    (temp, result)
}

#[test]
fn schemas_match_the_golden_contract() {
    assert_eq!(raw_schema().fields().len(), 13);
    assert_eq!(trades_schema().fields().len(), 8);
    assert_eq!(
        raw_schema().field_with_name("price").unwrap().data_type(),
        &DataType::Decimal128(38, 12)
    );
    assert_eq!(
        trades_schema()
            .field_with_name("timestamp_chicago")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Chicago".into()))
    );
    assert!(
        raw_schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable())
    );
}

#[test]
fn l1_bbo_aggressor_missing_null_sequence_and_order() {
    let rows = [
        "L1;2;20260818120000;1;100,25;2",
        "L1;1;20260818120000;2;100,00;1",
        "L1;0;20260818120000;3;101,00;1",
        "L1;2;20260818120000;4;101,00;3",
        "L1;2;20260818120000;5;100,00;4",
        "L1;2;20260818120000;6;100,50;5",
    ]
    .join("\n");
    let (_temp, result) = run_single(&(rows + "\n"), None);
    let raw = read_parquet(&result.raw_path);
    let trades = read_parquet(&result.trades_path);
    assert_eq!(raw.num_rows(), 6);
    assert_eq!(trades.num_rows(), 4);
    let raw_lines = raw
        .column_by_name("source_line")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(raw_lines.values(), &[1, 2, 3, 4, 5, 6]);
    let ids = trades
        .column_by_name("sequence_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3, 4]);
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
    let validation: Value =
        serde_json::from_slice(&fs::read(result.validation_path).unwrap()).unwrap();
    assert_eq!(validation["totals"]["missing_bbo_before_last"], 1);
    assert_eq!(validation["totals"]["buy"], 1);
    assert_eq!(validation["totals"]["sell"], 1);
    assert_eq!(validation["totals"]["unknown"], 2);
}

#[test]
fn timestamp_keeps_100ns_and_filename_does_not_override_event_date() {
    let (_temp, result) = run_single("L1;1;20270101120000;360000;100;1\n", None);
    let raw = read_parquet(&result.raw_path);
    let timestamp = raw
        .column_by_name("timestamp_chicago")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let mut expected = TimestampConverter::default();
    assert_eq!(
        timestamp.value(0),
        expected.convert("20270101120000", 360_000).unwrap()
    );
    assert_eq!(timestamp.value(0) % 1_000_000_000, 36_000_000);
}

#[test]
fn l2_fields_and_quoted_market_maker_are_lossless() {
    let (_temp, result) = run_single(
        "L2;0;20260818120000;3;0;1;\"MM;ONE\";29894,00;5\nL2;1;20260818120001;4;2;2;;29893,75;6\n",
        None,
    );
    let raw = read_parquet(&result.raw_path);
    assert_eq!(raw.num_rows(), 2);
    let makers = raw
        .column_by_name("market_maker")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(makers.value(0), "MM;ONE");
    assert_eq!(makers.value(1), "");
    let validation: Value =
        serde_json::from_slice(&fs::read(result.validation_path).unwrap()).unwrap();
    assert_eq!(validation["totals"]["l2"], 2);
}

#[test]
fn tick_size_only_changes_validation() {
    let contents = "L1;1;20260818120000;0;100,10;1\n";
    let (_temp_a, without) = run_single(contents, None);
    let (_temp_b, with) = run_single(contents, Some("0.25"));
    let raw_a = read_parquet(&without.raw_path);
    let raw_b = read_parquet(&with.raw_path);
    assert_eq!(raw_a, raw_b);
    let no_tick: Value =
        serde_json::from_slice(&fs::read(without.validation_path).unwrap()).unwrap();
    let tick: Value = serde_json::from_slice(&fs::read(with.validation_path).unwrap()).unwrap();
    assert_eq!(no_tick["tick_alignment"]["status"], "NOT_PERFORMED");
    assert_eq!(tick["tick_size"], "0.25");
    assert_eq!(tick["totals"]["tick_misaligned"], 1);
}

#[test]
fn folder_order_year_boundary_and_sequence_are_continuous() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("NQ SEP26");
    let output = temp.path().join("output");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&output).unwrap();
    write(
        &source.join("20270101.csv"),
        "L1;2;20270101120000;0;100;1\n",
    );
    write(
        &source.join("20261231.csv"),
        "L1;1;20261231120000;0;99;1\nL1;0;20261231120000;1;101;1\nL1;2;20261231120000;2;101;1\n",
    );
    let files = discover_csv_files(&source).unwrap();
    assert_eq!(
        files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy())
            .collect::<Vec<_>>(),
        vec!["20261231.csv", "20270101.csv"]
    );
    let result = convert_source(&source, &output, ConversionOptions::default()).unwrap();
    assert_eq!(result.raw_path.file_name().unwrap(), "NQ_09-26_RAW.parquet");
    let trades = read_parquet(&result.trades_path);
    let ids = trades
        .column_by_name("sequence_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2]);
}

#[test]
fn folder_resets_bbo_at_each_csv_without_resetting_sequence() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("NQ SEP26");
    let output = temp.path().join("output");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&output).unwrap();
    write(
        &source.join("20260713.csv"),
        "L1;1;20260713120000;0;99;1\nL1;0;20260713120000;1;101;1\nL1;2;20260713120000;2;100;1\n",
    );
    write(
        &source.join("20260714.csv"),
        "L1;1;20260714120000;0;199;1\nL1;2;20260714120000;1;200;2\n",
    );

    let result = convert_source(&source, &output, ConversionOptions::default()).unwrap();
    let trades = read_parquet(&result.trades_path);
    let ids = trades
        .column_by_name("sequence_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let bids = trades
        .column_by_name("bid")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
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

    assert_eq!(ids.values(), &[1, 2]);
    assert_eq!(bids.value(1), 199_000_000_000_000);
    assert!(asks.is_null(1));
    assert_eq!(aggressors.value(1), "UNKNOWN");

    let validation: Value =
        serde_json::from_slice(&fs::read(result.validation_path).unwrap()).unwrap();
    assert_eq!(validation["totals"]["missing_bbo_before_last"], 1);
}

#[test]
fn malformed_input_never_creates_final_outputs() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("20260818.csv");
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    write(&source, "L1;1;bad;0;100;1\n");
    assert!(convert_source(&source, &output, ConversionOptions::default()).is_err());
    assert!(!output.join("20260818_RAW.parquet").exists());
    assert!(!output.join("20260818_TRADES.parquet").exists());
    assert!(output.join("20260818_RAW.partial.parquet").exists());
    let validation: Value =
        serde_json::from_slice(&fs::read(output.join("20260818_validation.partial.json")).unwrap())
            .unwrap();
    assert_eq!(validation["status"], "FAIL");
    assert_eq!(validation["totals"]["parse_errors"], 1);
    assert_eq!(validation["totals"]["malformed_rows"], 1);
}

#[test]
fn empty_input_is_reported_as_a_partial_failure() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("20260818.csv");
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    write(&source, "");

    assert!(convert_source(&source, &output, ConversionOptions::default()).is_err());
    assert!(!output.join("20260818_RAW.parquet").exists());
    assert!(!output.join("20260818_TRADES.parquet").exists());
    let validation: Value =
        serde_json::from_slice(&fs::read(output.join("20260818_validation.partial.json")).unwrap())
            .unwrap();
    assert_eq!(validation["status"], "FAIL");
    assert_eq!(validation["totals"]["parse_errors"], 1);
    assert_eq!(validation["totals"]["malformed_rows"], 1);
}

#[test]
fn streaming_crosses_batch_boundaries_without_reordering() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("20260818.csv");
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    let mut csv = String::with_capacity((BATCH_SIZE + 1) * 40);
    for line in 0..=BATCH_SIZE {
        csv.push_str(&format!(
            "L1;1;20260818120000;{};100;1\n",
            line % 10_000_000
        ));
    }
    write(&source, &csv);
    let result = convert_source(&source, &output, ConversionOptions::default()).unwrap();
    let raw = read_parquet(&result.raw_path);
    assert_eq!(raw.num_rows(), BATCH_SIZE + 1);
    let lines = raw
        .column_by_name("source_line")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(lines.value(BATCH_SIZE), (BATCH_SIZE + 1) as i64);
}
