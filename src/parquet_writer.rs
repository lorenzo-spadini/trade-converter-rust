use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use arrow::array::{
    ArrayRef, Decimal128Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::decimal::{PRICE_PRECISION, PRICE_SCALE, Price};

pub const BATCH_SIZE: usize = 200_000;
const CHICAGO_TZ: &str = "America/Chicago";

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

    pub fn append_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.flush()?;
        if batch.schema().as_ref() != self.schema.as_ref() {
            bail!("Record batch schema is incompatible with the TRADES schema");
        }
        let started = Instant::now();
        self.writer.write(batch)?;
        self.write_time += started.elapsed();
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
