use anyhow::{Result, bail};
use chrono::{LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Europe::Berlin;

#[derive(Default)]
pub struct TimestampConverter {
    last_source: String,
    last_base_ns: i64,
}

impl TimestampConverter {
    pub fn convert(&mut self, source: &str, offset_100ns: i64) -> Result<i64> {
        if !(0..=9_999_999).contains(&offset_100ns) {
            bail!("offset100ns outside one-second range: {offset_100ns}");
        }
        if self.last_source != source {
            let naive = NaiveDateTime::parse_from_str(source, "%Y%m%d%H%M%S")?;
            let local = match Berlin.from_local_datetime(&naive) {
                LocalResult::Single(value) => value,
                LocalResult::Ambiguous(first, second) => {
                    if first.timestamp() <= second.timestamp() {
                        first
                    } else {
                        second
                    }
                }
                LocalResult::None => {
                    bail!(
                        "nonexistent Europe/Berlin local time: {source}; no implicit DST normalization"
                    )
                }
            };
            self.last_source.clear();
            self.last_source.push_str(source);
            self.last_base_ns = local
                .timestamp()
                .checked_mul(1_000_000_000)
                .ok_or_else(|| anyhow::anyhow!("timestamp out of nanosecond range"))?;
        }
        self.last_base_ns
            .checked_add(offset_100ns * 100)
            .ok_or_else(|| anyhow::anyhow!("timestamp out of nanosecond range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_100ns_and_berlin_dst() {
        let mut converter = TimestampConverter::default();
        let base = converter.convert("20260818120000", 0).unwrap();
        assert_eq!(
            converter.convert("20260818120000", 360_000).unwrap() - base,
            36_000_000
        );
        assert!(converter.convert("20260329023000", 0).is_err());
    }

    #[test]
    fn ambiguous_time_uses_fold_zero_earlier_instant() {
        let mut converter = TimestampConverter::default();
        let actual = converter.convert("20261025023000", 0).unwrap();
        assert_eq!(actual, 1_792_888_200_000_000_000);
    }
}
