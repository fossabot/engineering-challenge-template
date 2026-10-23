//! Exact decimal parsing for exchange wire values.
//!
//! The SBE challenge schema represents both dollars and contract counts as
//! signed 64-bit integers scaled by 1,000,000. Exchange APIs use decimal
//! strings with exchange-specific precision, so all conversion happens here
//! without floating point.

use anyhow::{Context, bail};

/// Number of fixed-point units in one dollar or one contract/share.
pub const SCALE: i64 = 1_000_000;
/// Number of decimal places represented by [`SCALE`].
pub const DECIMALS: usize = 6;

/// Parse a signed decimal string into millionths.
///
/// Accepts plain decimal notation only (no exponent, no thousands separators)
/// and rejects more than six fractional digits rather than silently rounding.
pub fn parse_micros(input: &str) -> anyhow::Result<i64> {
    let text = input.trim();
    if text.is_empty() {
        bail!("empty fixed-point value");
    }

    let (negative, unsigned) = match text.as_bytes()[0] {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if unsigned.is_empty() {
        bail!("missing digits in fixed-point value `{input}`");
    }

    let mut pieces = unsigned.split('.');
    let whole = pieces.next().unwrap_or_default();
    let fraction = pieces.next();
    if pieces.next().is_some() {
        bail!("multiple decimal points in fixed-point value `{input}`");
    }
    if whole.is_empty() && fraction.is_none_or(str::is_empty) {
        bail!("missing digits in fixed-point value `{input}`");
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid whole-number digits in fixed-point value `{input}`");
    }
    if let Some(fraction) = fraction {
        if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("invalid fractional digits in fixed-point value `{input}`");
        }
        if fraction.len() > DECIMALS {
            bail!(
                "fixed-point value `{input}` has {} fractional digits; maximum is {DECIMALS}",
                fraction.len()
            );
        }
    }

    let whole_value: i128 = if whole.is_empty() {
        0
    } else {
        whole
            .parse()
            .with_context(|| format!("fixed-point whole part out of range in `{input}`"))?
    };
    let mut fractional_value = 0i128;
    if let Some(fraction) = fraction {
        let scale = 10i128.pow((DECIMALS - fraction.len()) as u32);
        fractional_value = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse::<i128>()
                .with_context(|| format!("fixed-point fraction out of range in `{input}`"))?
                * scale
        };
    }

    let magnitude = whole_value
        .checked_mul(SCALE as i128)
        .and_then(|value| value.checked_add(fractional_value))
        .with_context(|| format!("fixed-point value `{input}` overflows"))?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).with_context(|| format!("fixed-point value `{input}` out of range"))
}

/// Parse a non-negative price in dollars and validate the schema's [0, 1]
/// probability range.
pub fn parse_price(input: &str) -> anyhow::Result<i64> {
    let price = parse_micros(input)?;
    anyhow::ensure!(
        (0..=SCALE).contains(&price),
        "price `{input}` outside [0, 1]"
    );
    Ok(price)
}

/// Parse a non-negative quantity.
pub fn parse_quantity(input: &str) -> anyhow::Result<i64> {
    let quantity = parse_micros(input)?;
    anyhow::ensure!(quantity >= 0, "negative quantity `{input}`");
    Ok(quantity)
}

/// Format millionths as a stable decimal string, primarily for diagnostics.
#[must_use]
pub fn format_micros(value: i64) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    let whole = magnitude / SCALE as u64;
    let fraction = magnitude % SCALE as u64;
    if fraction == 0 {
        format!("{sign}{whole}")
    } else {
        let fraction = format!("{fraction:0DECIMALS$}");
        format!("{sign}{whole}.{}", fraction.trim_end_matches('0'))
    }
}

/// Parse Kalshi's legacy integer-cent price.
pub fn cents_to_micros(cents: i64) -> anyhow::Result<i64> {
    cents
        .checked_mul(10_000)
        .context("cent price overflows fixed-point range")
}

/// Parse a whole-contract legacy quantity.
pub fn contracts_to_micros(contracts: i64) -> anyhow::Result<i64> {
    contracts
        .checked_mul(SCALE)
        .context("contract quantity overflows fixed-point range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_decimals() {
        assert_eq!(parse_micros("0").unwrap(), 0);
        assert_eq!(parse_micros("1").unwrap(), 1_000_000);
        assert_eq!(parse_micros("0.08").unwrap(), 80_000);
        assert_eq!(parse_micros("0.0800").unwrap(), 80_000);
        assert_eq!(parse_micros(".5").unwrap(), 500_000);
        assert_eq!(parse_micros("1.").unwrap(), 1_000_000);
        assert_eq!(parse_micros("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse_micros("300.00").unwrap(), 300_000_000);
        assert_eq!(parse_micros("219.217767").unwrap(), 219_217_767);
    }

    #[test]
    fn rejects_inexact_or_malformed_values() {
        for value in ["", ".", "1.0000001", "1e-3", "1,000", "nan", "--1"] {
            assert!(parse_micros(value).is_err(), "{value} should be rejected");
        }
    }

    #[test]
    fn validates_price_range() {
        assert_eq!(parse_price("0.0000").unwrap(), 0);
        assert_eq!(parse_price("1.0000").unwrap(), 1_000_000);
        assert!(parse_price("1.000001").is_err());
        assert!(parse_price("-0.1").is_err());
    }

    #[test]
    fn formats_cleanly() {
        assert_eq!(format_micros(80_000), "0.08");
        assert_eq!(format_micros(-54_000_000), "-54");
        assert_eq!(format_micros(219_217_767), "219.217767");
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_micros("9223372036855").is_err());
        assert!(cents_to_micros(i64::MAX).is_err());
    }
}
