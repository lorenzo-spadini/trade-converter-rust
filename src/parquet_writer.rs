use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow::array::{
    ArrayRef, Decimal128Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::decimal::{PRICE_PRECISION, PRICE_SCALE, Price};

pub const BATCH_SIZE: usize = 200_000;
const CHICAGO_TZ: &str = "America/Chicago";

pub fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_file", DataType::Utf8, true),
        Field::new("source_line", DataType::Int64, true),
        Field::new("event_kind", DataType::Utf8, true),
        Field::new("market_data_type", DataType::Int16, true),
        Field::new("source_timestamp", DataType::Utf8, true),
        Field::new("offset_100ns", DataType::Int64, true),
        Field::new(
            "timestamp_chicago",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(CHICAGO_TZ.into())),
            true,
        ),
        Field::new(
            "price",
            DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8),
            true,
        ),
        Field::new("price_text", DataType::Utf8, true),
        Field::new("volume", DataType::Int64, true),
        Field::new("operation", DataType::Int8, true),
        Field::new("position", DataType::Int32, true),
        Field::new("market_maker", DataType::Utf8, true),
    ]))
}

pub fn trades_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sequence_id", DataType::Int64, true),
        Field::new(
            "timestamp_chicago",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(CHICAGO_TZ.into())),
            true,
        ),
        Field::new("instrument", DataType::Utf8, true),
        Field::new(
            "price",
            DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8),
            true,
        ),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "bid",
            DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8),
            true,
        ),
        Field::new(
            "ask",
            DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8),
            true,
        ),
        Field::new("aggressor", DataType::Utf8, true),
    ]))
}

fn properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(BATCH_SIZE)
        .build()
}

pub struct RawRow<'a> {
    pub source_file: &'a str,
    pub source_line: i64,
    pub event_kind: &'a str,
    pub market_data_type: i16,
    pub source_timestamp: &'a str,
    pub offset_100ns: i64,
    pub timestamp_ns: i64,
    pub price: Price,
    pub price_text: &'a str,
    pub volume: i64,
    pub operation: Option<i8>,
    pub position: Option<i32>,
    pub market_maker: Option<&'a str>,
}

pub struct TradeRow<'a> {
    pub sequence_id: i64,
    pub timestamp_ns: i64,
    pub instrument: &'a str,
    pub price: Price,
    pub size: i64,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub aggressor: &'a str,
}

struct RawBuilders {
    source_file: StringBuilder,
    source_line: Int64Builder,
    event_kind: StringBuilder,
    market_data_type: Int16Builder,
    source_timestamp: StringBuilder,
    offset_100ns: Int64Builder,
    timestamp: TimestampNanosecondBuilder,
    price: Decimal128Builder,
    price_text: StringBuilder,
    volume: Int64Builder,
    operation: Int8Builder,
    position: Int32Builder,
    market_maker: StringBuilder,
    len: usize,
}

impl RawBuilders {
    fn new() -> Self {
        Self {
            source_file: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12),
            source_line: Int64Builder::with_capacity(BATCH_SIZE),
            event_kind: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 2),
            market_data_type: Int16Builder::with_capacity(BATCH_SIZE),
            source_timestamp: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 14),
            offset_100ns: Int64Builder::with_capacity(BATCH_SIZE),
            timestamp: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE).with_data_type(
                DataType::Timestamp(TimeUnit::Nanosecond, Some(CHICAGO_TZ.into())),
            ),
            price: Decimal128Builder::with_capacity(BATCH_SIZE).with_data_type(
                DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8),
            ),
            price_text: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10),
            volume: Int64Builder::with_capacity(BATCH_SIZE),
            operation: Int8Builder::with_capacity(BATCH_SIZE),
            position: Int32Builder::with_capacity(BATCH_SIZE),
            market_maker: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 4),
            len: 0,
        }
    }

    fn append(&mut self, row: RawRow<'_>) {
        self.source_file.append_value(row.source_file);
        self.source_line.append_value(row.source_line);
        self.event_kind.append_value(row.event_kind);
        self.market_data_type.append_value(row.market_data_type);
        self.source_timestamp.append_value(row.source_timestamp);
        self.offset_100ns.append_value(row.offset_100ns);
        self.timestamp.append_value(row.timestamp_ns);
        self.price.append_value(row.price.0);
        self.price_text.append_value(row.price_text);
        self.volume.append_value(row.volume);
        match row.operation {
            Some(value) => self.operation.append_value(value),
            None => self.operation.append_null(),
        }
        match row.position {
            Some(value) => self.position.append_value(value),
            None => self.position.append_null(),
        }
        match row.market_maker {
            Some(value) => self.market_maker.append_value(value),
            None => self.market_maker.append_null(),
        }
        self.len += 1;
    }

    fn finish(&mut self, schema: SchemaRef) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.source_file.finish()),
            Arc::new(self.source_line.finish()),
            Arc::new(self.event_kind.finish()),
            Arc::new(self.market_data_type.finish()),
            Arc::new(self.source_timestamp.finish()),
            Arc::new(self.offset_100ns.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.price_text.finish()),
            Arc::new(self.volume.finish()),
            Arc::new(self.operation.finish()),
            Arc::new(self.position.finish()),
            Arc::new(self.market_maker.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

struct TradeBuilders {
    sequence_id: Int64Builder,
    timestamp: TimestampNanosecondBuilder,
    instrument: StringBuilder,
    price: Decimal128Builder,
    size: Int64Builder,
    bid: Decimal128Builder,
    ask: Decimal128Builder,
    aggressor: StringBuilder,
    len: usize,
}

impl TradeBuilders {
    fn new() -> Self {
        let decimal_type = DataType::Decimal128(PRICE_PRECISION as u8, PRICE_SCALE as i8);
        Self {
            sequence_id: Int64Builder::with_capacity(BATCH_SIZE),
            timestamp: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE).with_data_type(
                DataType::Timestamp(TimeUnit::Nanosecond, Some(CHICAGO_TZ.into())),
            ),
            instrument: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8),
            price: Decimal128Builder::with_capacity(BATCH_SIZE)
                .with_data_type(decimal_type.clone()),
            size: Int64Builder::with_capacity(BATCH_SIZE),
            bid: Decimal128Builder::with_capacity(BATCH_SIZE).with_data_type(decimal_type.clone()),
            ask: Decimal128Builder::with_capacity(BATCH_SIZE).with_data_type(decimal_type),
            aggressor: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 7),
            len: 0,
        }
    }

    fn append(&mut self, row: TradeRow<'_>) {
        self.sequence_id.append_value(row.sequence_id);
        self.timestamp.append_value(row.timestamp_ns);
        self.instrument.append_value(row.instrument);
        self.price.append_value(row.price.0);
        self.size.append_value(row.size);
        match row.bid {
            Some(value) => self.bid.append_value(value.0),
            None => self.bid.append_null(),
        }
        match row.ask {
            Some(value) => self.ask.append_value(value.0),
            None => self.ask.append_null(),
        }
        self.aggressor.append_value(row.aggressor);
        self.len += 1;
    }

    fn finish(&mut self, schema: SchemaRef) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.sequence_id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.instrument.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.bid.finish()),
            Arc::new(self.ask.finish()),
            Arc::new(self.aggressor.finish()),
        ];
        self.len = 0;
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

pub struct RawParquetWriter {
    schema: SchemaRef,
    writer: ArrowWriter<BufWriter<File>>,
    builders: RawBuilders,
    pub write_time: Duration,
}

impl RawParquetWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let schema = raw_schema();
        let file = BufWriter::with_capacity(16 * 1024 * 1024, File::create(path)?);
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(properties()))?;
        Ok(Self {
            schema,
            writer,
            builders: RawBuilders::new(),
            write_time: Duration::ZERO,
        })
    }

    pub fn append(&mut self, row: RawRow<'_>) -> Result<()> {
        self.builders.append(row);
        if self.builders.len >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.builders.len == 0 {
            return Ok(());
        }
        let batch = self.builders.finish(self.schema.clone())?;
        let started = Instant::now();
        self.writer.write(&batch)?;
        self.write_time += started.elapsed();
        Ok(())
    }

    pub fn close(mut self) -> Result<Duration> {
        self.flush()?;
        let started = Instant::now();
        self.writer.close()?;
        self.write_time += started.elapsed();
        Ok(self.write_time)
    }
}

pub struct TradesParquetWriter {
    schema: SchemaRef,
    writer: ArrowWriter<BufWriter<File>>,
    builders: TradeBuilders,
    pub write_time: Duration,
}

impl TradesParquetWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let schema = trades_schema();
        let file = BufWriter::with_capacity(16 * 1024 * 1024, File::create(path)?);
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(properties()))?;
        Ok(Self {
            schema,
            writer,
            builders: TradeBuilders::new(),
            write_time: Duration::ZERO,
        })
    }

    pub fn append(&mut self, row: TradeRow<'_>) -> Result<()> {
        self.builders.append(row);
        if self.builders.len >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.builders.len == 0 {
            return Ok(());
        }
        let batch = self.builders.finish(self.schema.clone())?;
        let started = Instant::now();
        self.writer.write(&batch)?;
        self.write_time += started.elapsed();
        Ok(())
    }

    pub fn close(mut self) -> Result<Duration> {
        self.flush()?;
        let started = Instant::now();
        self.writer.close()?;
        self.write_time += started.elapsed();
        Ok(self.write_time)
    }
}
