use std::path::Path;

use serde::Serialize;

use crate::decimal::{ExactDecimal, format_exact};

#[derive(Debug, Default)]
pub struct FileValidation {
    pub total_events: u64,
    pub l1: u64,
    pub l2: u64,
    pub l1_by_type: [u64; 11],
    pub last: u64,
    pub trades_written: u64,
    pub buy: u64,
    pub sell: u64,
    pub unknown: u64,
    pub backward_timestamps: u64,
    pub invalid_size: u64,
    pub invalid_bbo: u64,
    pub last_outside_bbo: u64,
    pub missing_bbo_before_last: u64,
    pub malformed_rows: u64,
    pub parse_errors: u64,
    pub tick_misaligned: u64,
    pub daily_volume_count: u64,
    pub daily_volume_decreases: u64,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub previous_timestamp_ns: Option<i64>,
    pub previous_daily_volume: Option<i64>,
}

impl FileValidation {
    pub fn status(&self) -> &'static str {
        if !self.errors.is_empty()
            || self.parse_errors > 0
            || self.malformed_rows > 0
            || self.invalid_size > 0
            || self.invalid_bbo > 0
        {
            "FAIL"
        } else if !self.warnings.is_empty() || self.backward_timestamps > 0 {
            "WARNING"
        } else {
            "PASS"
        }
    }

    pub fn warning(&mut self, message: String) {
        if self.warnings.len() < 20 {
            self.warnings.push(message);
        }
    }

    pub fn report(&self, path: &Path) -> FileReport {
        let filename = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let date = if stem.len() == 8 && stem.bytes().all(|byte| byte.is_ascii_digit()) {
            Some(format!("{}-{}-{}", &stem[..4], &stem[4..6], &stem[6..8]))
        } else {
            None
        };
        FileReport {
            date,
            filename,
            status: self.status().to_string(),
            total_events: self.total_events,
            l1: self.l1,
            l2: self.l2,
            l1_by_type: L1Counts::from_array(self.l1_by_type),
            last: self.last,
            trades_written: self.trades_written,
            buy: self.buy,
            sell: self.sell,
            unknown: self.unknown,
            backward_timestamps: self.backward_timestamps,
            invalid_size: self.invalid_size,
            invalid_bbo: self.invalid_bbo,
            last_outside_bbo: self.last_outside_bbo,
            missing_bbo_before_last: self.missing_bbo_before_last,
            malformed_rows: self.malformed_rows,
            parse_errors: self.parse_errors,
            tick_misaligned: self.tick_misaligned,
            daily_volume_count: self.daily_volume_count,
            daily_volume_decreases: self.daily_volume_decreases,
            warnings: self.warnings.clone(),
            errors: self.errors.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct L1Counts {
    #[serde(rename = "0")]
    pub c0: u64,
    #[serde(rename = "1")]
    pub c1: u64,
    #[serde(rename = "2")]
    pub c2: u64,
    #[serde(rename = "3")]
    pub c3: u64,
    #[serde(rename = "4")]
    pub c4: u64,
    #[serde(rename = "5")]
    pub c5: u64,
    #[serde(rename = "6")]
    pub c6: u64,
    #[serde(rename = "7")]
    pub c7: u64,
    #[serde(rename = "8")]
    pub c8: u64,
    #[serde(rename = "9")]
    pub c9: u64,
    #[serde(rename = "10")]
    pub c10: u64,
}

impl L1Counts {
    fn from_array(value: [u64; 11]) -> Self {
        Self {
            c0: value[0],
            c1: value[1],
            c2: value[2],
            c3: value[3],
            c4: value[4],
            c5: value[5],
            c6: value[6],
            c7: value[7],
            c8: value[8],
            c9: value[9],
            c10: value[10],
        }
    }

    fn add_array(&mut self, value: [u64; 11]) {
        self.c0 += value[0];
        self.c1 += value[1];
        self.c2 += value[2];
        self.c3 += value[3];
        self.c4 += value[4];
        self.c5 += value[5];
        self.c6 += value[6];
        self.c7 += value[7];
        self.c8 += value[8];
        self.c9 += value[9];
        self.c10 += value[10];
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Totals {
    pub total_events: u64,
    pub l1: u64,
    pub l2: u64,
    pub l1_by_type: L1Counts,
    pub last: u64,
    pub trades_written: u64,
    pub buy: u64,
    pub sell: u64,
    pub unknown: u64,
    pub backward_timestamps: u64,
    pub invalid_size: u64,
    pub invalid_bbo: u64,
    pub last_outside_bbo: u64,
    pub missing_bbo_before_last: u64,
    pub malformed_rows: u64,
    pub parse_errors: u64,
    pub tick_misaligned: u64,
    pub daily_volume_count: u64,
    pub daily_volume_decreases: u64,
}

impl Totals {
    pub fn add(&mut self, file: &FileValidation) {
        self.total_events += file.total_events;
        self.l1 += file.l1;
        self.l2 += file.l2;
        self.l1_by_type.add_array(file.l1_by_type);
        self.last += file.last;
        self.trades_written += file.trades_written;
        self.buy += file.buy;
        self.sell += file.sell;
        self.unknown += file.unknown;
        self.backward_timestamps += file.backward_timestamps;
        self.invalid_size += file.invalid_size;
        self.invalid_bbo += file.invalid_bbo;
        self.last_outside_bbo += file.last_outside_bbo;
        self.missing_bbo_before_last += file.missing_bbo_before_last;
        self.malformed_rows += file.malformed_rows;
        self.parse_errors += file.parse_errors;
        self.tick_misaligned += file.tick_misaligned;
        self.daily_volume_count += file.daily_volume_count;
        self.daily_volume_decreases += file.daily_volume_decreases;
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FileReport {
    pub date: Option<String>,
    pub filename: String,
    pub status: String,
    pub total_events: u64,
    pub l1: u64,
    pub l2: u64,
    pub l1_by_type: L1Counts,
    pub last: u64,
    pub trades_written: u64,
    pub buy: u64,
    pub sell: u64,
    pub unknown: u64,
    pub backward_timestamps: u64,
    pub invalid_size: u64,
    pub invalid_bbo: u64,
    pub last_outside_bbo: u64,
    pub missing_bbo_before_last: u64,
    pub malformed_rows: u64,
    pub parse_errors: u64,
    pub tick_misaligned: u64,
    pub daily_volume_count: u64,
    pub daily_volume_decreases: u64,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TickAlignment {
    pub status: &'static str,
    pub performed: bool,
}

#[derive(Debug, Serialize)]
pub struct ValidationDocument {
    pub status: String,
    pub source: String,
    pub contract: String,
    pub instrument: String,
    pub date: String,
    pub source_timezone: &'static str,
    pub timezone: &'static str,
    pub compression: &'static str,
    pub tick_size: String,
    pub tick_alignment: TickAlignment,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub files: Vec<FileReport>,
    pub totals: Totals,
    pub notes: Vec<String>,
}

impl ValidationDocument {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: String,
        contract: String,
        instrument: String,
        date: String,
        tick_size: ExactDecimal,
        files: Vec<FileReport>,
        totals: Totals,
        warnings: Vec<String>,
        errors: Vec<String>,
        forced_status: Option<&str>,
    ) -> Self {
        let status = forced_status.map(str::to_string).unwrap_or_else(|| {
            if files.iter().any(|file| file.status == "FAIL") {
                "FAIL".to_string()
            } else if files.iter().any(|file| file.status == "WARNING") || !warnings.is_empty() {
                "WARNING".to_string()
            } else {
                "PASS".to_string()
            }
        });
        let tick_text = format_exact(tick_size);
        Self {
            status,
            source,
            contract,
            instrument,
            date,
            source_timezone: "Europe/Berlin",
            timezone: "America/Chicago",
            compression: "zstd",
            tick_size: tick_text.clone(),
            tick_alignment: TickAlignment {
                status: "PERFORMED",
                performed: true,
            },
            warnings,
            errors,
            files,
            totals,
            notes: vec![
                "Source timestamps are interpreted as Europe/Berlin using zoneinfo.".into(),
                "timestamp_chicago is stored as Arrow timestamp[ns, tz=America/Chicago].".into(),
                "NRDToCSV offset100ns is multiplied by 100 to preserve 100 ns precision.".into(),
                "Ambiguous Berlin DST fall-back seconds use zoneinfo fold=0 because CSV rows have no fold flag.".into(),
                "Nonexistent Berlin wall times fail instead of being normalized implicitly.".into(),
                "Missing BBO sides are stored as null; aggressor is UNKNOWN.".into(),
                "ReplayTradeExporter uses -1.7976931348623157E+308 (Double.MinValue) as its sentinel for uninitialized Bid/Ask; Parquet stores null for the same semantic state without contaminating the real-price domain.".into(),
                "Replay Double.MinValue matches Parquet null only when that BBO side was genuinely absent; no other value is normalized.".into(),
                format!("Tick alignment was performed with Tick Size {tick_text}."),
            ],
        }
    }
}
