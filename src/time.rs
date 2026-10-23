//! Timestamp conversion helpers.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};

/// Current wall-clock time as microseconds since the Unix epoch.
pub fn now_us() -> anyhow::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates the Unix epoch")?;
    i64::try_from(elapsed.as_micros()).context("current timestamp exceeds i64 microseconds")
}

/// Current wall-clock time as a UTC datetime.
pub fn now_utc() -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(now_us()?).context("current timestamp outside chrono range")
}

/// Convert microseconds since the epoch into a UTC datetime.
pub fn utc_from_us(timestamp_us: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(timestamp_us)
        .with_context(|| format!("timestamp {timestamp_us} outside chrono range"))
}

/// Parse an RFC 3339 timestamp into microseconds.
pub fn parse_rfc3339_us(input: &str) -> anyhow::Result<i64> {
    let parsed = DateTime::parse_from_rfc3339(input)
        .with_context(|| format!("invalid RFC 3339 timestamp `{input}`"))?
        .with_timezone(&Utc);
    Ok(parsed.timestamp_micros())
}

/// Convert epoch milliseconds to microseconds with overflow checking.
pub fn epoch_ms_to_us(milliseconds: i64) -> anyhow::Result<i64> {
    milliseconds
        .checked_mul(1_000)
        .context("millisecond timestamp overflows microseconds")
}

/// Convert epoch seconds to microseconds with overflow checking.
pub fn epoch_secs_to_us(seconds: i64) -> anyhow::Result<i64> {
    seconds
        .checked_mul(1_000_000)
        .context("second timestamp overflows microseconds")
}

/// Parse a timestamp that may be either Unix milliseconds or RFC 3339.
pub fn parse_millis_or_rfc3339_us(input: &str) -> anyhow::Result<i64> {
    let text = input.trim();
    if text.is_empty() {
        bail!("empty timestamp");
    }
    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        let milliseconds: i64 = text
            .parse()
            .with_context(|| format!("invalid millisecond timestamp `{input}`"))?;
        return epoch_ms_to_us(milliseconds);
    }
    parse_rfc3339_us(text)
}

/// Format microseconds as RFC 3339 in UTC with microsecond precision.
#[must_use]
pub fn format_rfc3339_us(timestamp_us: i64) -> String {
    utc_from_us(timestamp_us)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        .unwrap_or_else(|_| format!("invalid-timestamp({timestamp_us})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamp_forms() {
        let expected = 1_782_753_357_257_000;
        assert_eq!(
            parse_millis_or_rfc3339_us("1782753357257").unwrap(),
            expected
        );
        assert_eq!(
            parse_millis_or_rfc3339_us("2026-06-29T17:15:57.257000Z").unwrap(),
            expected
        );
    }

    #[test]
    fn formats_round_trip() {
        let timestamp = 1_700_000_000_123_456;
        let text = format_rfc3339_us(timestamp);
        assert_eq!(parse_rfc3339_us(&text).unwrap(), timestamp);
    }
}
