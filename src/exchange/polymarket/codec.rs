//! Polymarket CLOB market-channel decoding.
//!
//! Polymarket publishes a full `book` and then absolute per-level
//! `price_change` updates. The decoder normalizes both into the same event
//! types used by every other adapter.

use std::{
    collections::BTreeMap,
    sync::{LazyLock, Mutex},
};

use anyhow::{Context, bail};
use serde_json::{Map, Value};
use tracing::warn;

use crate::{
    Side,
    fixed::{parse_micros, parse_price, parse_quantity},
    model::{BookSnapshot, EventMeta, ExchangeEvent, Level, LifecycleKind, QuantityChange},
    time::{epoch_ms_to_us, epoch_secs_to_us, parse_rfc3339_us},
};

/// Cap on distinct unknown `event_type` names tracked per process. Schema
/// drift must stay visible without letting a hostile feed grow memory or log
/// volume without bound.
const MAX_TRACKED_UNKNOWN_EVENT_TYPES: usize = 64;

static UNKNOWN_EVENT_TYPES: LazyLock<Mutex<BTreeMap<String, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// Record one unknown `event_type`. The first occurrence of each name logs a
/// warning; every occurrence is counted so tests and tooling can observe schema
/// drift. Unknown events still decode to no normalized events: a
/// forward-compatible feed must not be torn down by an additive change.
fn note_unknown_event_type(kind: &str) {
    let mut registry = UNKNOWN_EVENT_TYPES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let already_tracked = registry.contains_key(kind);
    if !already_tracked && registry.len() >= MAX_TRACKED_UNKNOWN_EVENT_TYPES {
        return;
    }
    let count = registry.entry(kind.to_owned()).or_insert(0);
    *count += 1;
    if *count == 1 {
        warn!(
            event_type = kind,
            "ignoring unknown Polymarket event_type; it is not part of the documented contract"
        );
    }
}

/// Snapshot of unknown `event_type` names observed so far, with counts.
#[cfg(test)]
pub(crate) fn unknown_event_types() -> BTreeMap<String, u64> {
    UNKNOWN_EVENT_TYPES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Decode one market-channel frame. Polymarket sometimes emits a JSON array
/// for the initial book dump; both object and array forms are accepted.
pub fn decode_polymarket_message(
    payload: &str,
    received_at_us: i64,
) -> anyhow::Result<Vec<ExchangeEvent>> {
    let value: Value = serde_json::from_str(payload).context("invalid Polymarket JSON")?;
    let mut events = Vec::new();
    match value {
        Value::Array(items) => {
            for item in items {
                let object = item
                    .as_object()
                    .context("Polymarket array item is not an object")?;
                events.extend(decode_object(object, received_at_us)?);
            }
        }
        Value::Object(object) => events.extend(decode_object(&object, received_at_us)?),
        _ => bail!("Polymarket payload is neither an object nor an array"),
    }
    Ok(events)
}

fn decode_object(
    object: &Map<String, Value>,
    received_at_us: i64,
) -> anyhow::Result<Vec<ExchangeEvent>> {
    let event_type = object
        .get("event_type")
        .and_then(Value::as_str)
        .context("Polymarket payload has no string `event_type`")?;
    let exchange_ts_us =
        parse_timestamp(object.get("timestamp")).context("invalid Polymarket timestamp")?;
    let meta = EventMeta {
        received_at_us,
        exchange_ts_us,
        seq: None,
        clock: None,
    };
    match event_type {
        "book" => {
            let native_symbol = required_str(object, "asset_id")?.to_owned();
            let mut book = BookSnapshot::default();
            parse_book_side(object, "bids", Side::Buy, &mut book)?;
            parse_book_side(object, "asks", Side::Sell, &mut book)?;
            Ok(vec![ExchangeEvent::Snapshot {
                native_symbol,
                book,
                meta,
            }])
        }
        "price_change" => {
            let changes = object
                .get("price_changes")
                .and_then(Value::as_array)
                .context("Polymarket price_change has no `price_changes` array")?;
            let mut events = Vec::with_capacity(changes.len());
            for change in changes {
                let change = change
                    .as_object()
                    .context("Polymarket price change is not an object")?;
                let native_symbol = change
                    .get("asset_id")
                    .or_else(|| object.get("asset_id"))
                    .and_then(Value::as_str)
                    .context("Polymarket price change has no asset_id")?
                    .to_owned();
                let price = parse_price(&scalar_string(
                    change
                        .get("price")
                        .context("Polymarket price change has no price")?,
                )?)?;
                let size = parse_quantity(&scalar_string(
                    change
                        .get("size")
                        .context("Polymarket price change has no size")?,
                )?)?;
                let side = match required_str(change, "side")?.to_ascii_uppercase().as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    other => bail!("unknown Polymarket side `{other}`"),
                };
                events.push(ExchangeEvent::LevelChange {
                    native_symbol,
                    side,
                    price,
                    change: QuantityChange::Absolute(size),
                    meta,
                });
            }
            Ok(events)
        }
        "tick_size_change" => {
            let native_symbol = required_str(object, "asset_id")?.to_owned();
            let new_tick_size = object
                .get("new_tick_size")
                .context("Polymarket tick_size_change has no new_tick_size")?;
            let tick_size_us = parse_micros(&scalar_string(new_tick_size)?)
                .context("invalid Polymarket new_tick_size")?;
            anyhow::ensure!(
                tick_size_us > 0,
                "Polymarket new_tick_size must be positive"
            );
            Ok(vec![ExchangeEvent::Lifecycle {
                native_symbol,
                kind: LifecycleKind::PriceStructureUpdated,
                close_time_us: None,
                title: None,
                tick_size_us: Some(tick_size_us),
                price_ranges: None,
                price_level_structure: None,
                meta,
            }])
        }
        "new_market" => {
            let title = object
                .get("question")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    object
                        .get("event_message")
                        .and_then(Value::as_object)
                        .and_then(|message| message.get("title"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                });
            lifecycle_for_assets(object, LifecycleKind::NewMarket, title, meta)
        }
        "market_resolved" => lifecycle_for_assets(object, LifecycleKind::Resolved, None, meta),
        // Documented, deliberately ignored event types. They carry no
        // order-book state for this collector.
        "best_bid_ask" | "last_trade_price" | "tick_size" => Ok(Vec::new()),
        // Unknown types are counted and logged (once per name) instead of
        // failing the feed, so additive protocol changes stay visible without
        // causing reconnect loops.
        other => {
            note_unknown_event_type(other);
            Ok(Vec::new())
        }
    }
}

fn lifecycle_for_assets(
    object: &Map<String, Value>,
    kind: LifecycleKind,
    title: Option<String>,
    meta: EventMeta,
) -> anyhow::Result<Vec<ExchangeEvent>> {
    let assets = object
        .get("assets_ids")
        .or_else(|| object.get("clob_token_ids"))
        .and_then(Value::as_array)
        .context("Polymarket lifecycle event has no asset list")?;
    let mut events = Vec::with_capacity(assets.len());
    for asset in assets {
        let native_symbol = asset
            .as_str()
            .context("Polymarket lifecycle asset id is not a string")?
            .to_owned();
        events.push(ExchangeEvent::Lifecycle {
            native_symbol,
            kind: kind.clone(),
            close_time_us: None,
            title: title.clone(),
            tick_size_us: None,
            price_ranges: None,
            price_level_structure: None,
            meta,
        });
    }
    Ok(events)
}

/// Parse one side of a `book` event. Both `bids` and `asks` are mandatory,
/// non-nullable arrays in the documented market-channel contract; an explicit
/// empty array is valid and means the side has no levels. A missing, null, or
/// non-array field is a protocol violation: accepting it would let a malformed
/// frame silently replace a valid book with a partial one.
fn parse_book_side(
    object: &Map<String, Value>,
    field: &str,
    side: Side,
    book: &mut BookSnapshot,
) -> anyhow::Result<()> {
    let value = object
        .get(field)
        .with_context(|| format!("Polymarket book has no `{field}` array"))?;
    let levels = value
        .as_array()
        .with_context(|| format!("Polymarket book `{field}` is not an array"))?;
    for level in levels {
        let level = level
            .as_object()
            .context("Polymarket book level is not an object")?;
        let price = parse_price(&scalar_string(
            level
                .get("price")
                .context("Polymarket book level has no price")?,
        )?)?;
        let quantity = parse_quantity(&scalar_string(
            level
                .get("size")
                .context("Polymarket book level has no size")?,
        )?)?;
        let entry = Level { price, quantity };
        match side {
            Side::Buy => book.bids.push(entry),
            Side::Sell => book.asks.push(entry),
        }
    }
    Ok(())
}

fn required_str<'a>(object: &'a Map<String, Value>, field: &str) -> anyhow::Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("Polymarket payload missing string `{field}`"))
}

fn scalar_string(value: &Value) -> anyhow::Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        _ => bail!("expected a string or number, found {value}"),
    }
}

/// Parse the documented `timestamp` field, which the market channel types as
/// `string | null` and may omit entirely.
///
/// Three outcomes are distinct by design:
/// - field absent or JSON `null` → `Ok(None)`; the venue contract permits
///   both, and callers fall back to the local receive time;
/// - a present, well-formed value → `Ok(Some(micros))`;
/// - a present but malformed or out-of-range value → `Err`. A corrupt
///   timestamp must surface as a decode error, never silently become the
///   local clock.
fn parse_timestamp(value: Option<&Value>) -> anyhow::Result<Option<i64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let text = match value {
        Value::String(text) => text.trim().to_owned(),
        Value::Number(number) => number.to_string(),
        other => bail!("Polymarket timestamp has unsupported type {other}"),
    };
    if text.is_empty() {
        bail!("Polymarket timestamp is empty");
    }
    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        let parsed = text.parse::<i64>();
        return match text.len() {
            0..=10 => parsed
                .map_err(anyhow::Error::from)
                .and_then(epoch_secs_to_us)
                .map(Some),
            _ => parsed
                .map_err(anyhow::Error::from)
                .and_then(epoch_ms_to_us)
                .map(Some),
        };
    }
    parse_rfc3339_us(&text).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_700_000_000_123_456;

    #[test]
    fn decodes_book_and_timestamp() {
        let payload = r#"{
            "event_type":"book","asset_id":"123","market":"0xabc",
            "bids":[{"price":"0.48","size":"30"}],
            "asks":[{"price":"0.52","size":"25"}],
            "timestamp":"1757908892351","hash":"0x1"
        }"#;
        let events = decode_polymarket_message(payload, NOW).unwrap();
        let ExchangeEvent::Snapshot {
            native_symbol,
            book,
            meta,
        } = &events[0]
        else {
            panic!("expected snapshot");
        };
        assert_eq!(native_symbol, "123");
        assert_eq!(
            book.bids[0],
            Level {
                price: 480_000,
                quantity: 30_000_000
            }
        );
        assert_eq!(
            book.asks[0],
            Level {
                price: 520_000,
                quantity: 25_000_000
            }
        );
        assert_eq!(meta.exchange_ts_us, Some(1_757_908_892_351_000));
    }

    #[test]
    fn decodes_absolute_price_changes() {
        let payload = r#"{
            "event_type":"price_change","market":"0xabc","timestamp":"1757908892351",
            "price_changes":[
              {"asset_id":"123","price":"0.5","size":"200","side":"BUY","hash":"h"},
              {"asset_id":"456","price":"0.4","size":"0","side":"SELL","hash":"h"}
            ]
        }"#;
        let events = decode_polymarket_message(payload, NOW).unwrap();
        assert_eq!(events.len(), 2);
        let ExchangeEvent::LevelChange {
            native_symbol,
            side,
            price,
            change,
            ..
        } = &events[0]
        else {
            panic!("expected level change");
        };
        assert_eq!(native_symbol, "123");
        assert_eq!(*side, Side::Buy);
        assert_eq!(*price, 500_000);
        assert_eq!(*change, QuantityChange::Absolute(200_000_000));
        let ExchangeEvent::LevelChange { change, .. } = &events[1] else {
            panic!("expected level change");
        };
        assert_eq!(*change, QuantityChange::Absolute(0));
    }

    #[test]
    fn decodes_array_payload_and_lifecycle() {
        let payload = r#"[
            {"event_type":"tick_size_change","asset_id":"123","market":"0xabc","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1757908892351"},
            {"event_type":"market_resolved","id":"m","market":"0xabc","assets_ids":["123","456"],"winning_asset_id":"123","winning_outcome":"Yes","timestamp":"1757908892351"}
        ]"#;
        let events = decode_polymarket_message(payload, NOW).unwrap();
        assert_eq!(events.len(), 3);
        let ExchangeEvent::Lifecycle {
            kind,
            tick_size_us,
            price_ranges,
            ..
        } = &events[0]
        else {
            panic!("expected tick-size lifecycle");
        };
        assert_eq!(*kind, LifecycleKind::PriceStructureUpdated);
        assert_eq!(*tick_size_us, Some(1_000));
        assert!(price_ranges.is_none());
        assert!(matches!(
            events[1],
            ExchangeEvent::Lifecycle {
                kind: LifecycleKind::Resolved,
                ..
            }
        ));
    }

    #[test]
    fn ignores_unknown_events_and_rejects_malformed_books() {
        let events =
            decode_polymarket_message(r#"{"event_type":"best_bid_ask","asset_id":"1"}"#, NOW)
                .unwrap();
        assert!(events.is_empty());
        assert!(decode_polymarket_message("not json", NOW).is_err());
        assert!(decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","bids":[{"price":"2","size":"1"}],"asks":[]}"#,
            NOW
        )
        .is_err());
    }

    #[test]
    fn book_requires_both_mandatory_sides() {
        // Both sides present: valid, including an explicitly empty side.
        let events = decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","bids":[{"price":"0.4","size":"10"}],"asks":[]}"#,
            NOW,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        // An entirely empty book is a legitimate "no offers" state.
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"book","asset_id":"1","bids":[],"asks":[]}"#,
                NOW
            )
            .is_ok()
        );
        // Missing side fields are protocol violations.
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"book","asset_id":"1","bids":[{"price":"0.4","size":"1"}]}"#,
                NOW
            )
            .is_err()
        );
        assert!(
            decode_polymarket_message(r#"{"event_type":"book","asset_id":"1","asks":[]}"#, NOW)
                .is_err()
        );
        // Null or non-array sides are protocol violations.
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"book","asset_id":"1","bids":null,"asks":[]}"#,
                NOW
            )
            .is_err()
        );
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"book","asset_id":"1","bids":{},"asks":[]}"#,
                NOW
            )
            .is_err()
        );
        // Malformed levels are rejected.
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"book","asset_id":"1","bids":[{"price":"0.4"}],"asks":[]}"#,
                NOW
            )
            .is_err()
        );
        assert!(decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","bids":[{"price":"0.4","size":"-1"}],"asks":[]}"#,
            NOW
        )
        .is_err());
        assert!(decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","bids":[{"price":"not-a-price","size":"1"}],"asks":[]}"#,
            NOW
        )
        .is_err());
    }

    #[test]
    fn timestamps_distinguish_absent_null_and_invalid() {
        // Absent timestamp: permitted by the venue contract.
        let events = decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","bids":[],"asks":[]}"#,
            NOW,
        )
        .unwrap();
        let ExchangeEvent::Snapshot { meta, .. } = &events[0] else {
            panic!("expected snapshot");
        };
        assert_eq!(meta.exchange_ts_us, None);
        // JSON null timestamp: also documented as permitted.
        let events = decode_polymarket_message(
            r#"{"event_type":"book","asset_id":"1","timestamp":null,"bids":[],"asks":[]}"#,
            NOW,
        )
        .unwrap();
        let ExchangeEvent::Snapshot { meta, .. } = &events[0] else {
            panic!("expected snapshot");
        };
        assert_eq!(meta.exchange_ts_us, None);

        let exchange_ts = |payload: &str| {
            let events = decode_polymarket_message(payload, NOW).unwrap();
            let ExchangeEvent::Snapshot { meta, .. } = &events[0] else {
                panic!("expected snapshot");
            };
            meta.exchange_ts_us
        };
        // Millisecond and second strings, and RFC 3339, are accepted.
        assert_eq!(
            exchange_ts(
                r#"{"event_type":"book","asset_id":"1","timestamp":"1757908892351","bids":[],"asks":[]}"#
            ),
            Some(1_757_908_892_351_000)
        );
        assert_eq!(
            exchange_ts(
                r#"{"event_type":"book","asset_id":"1","timestamp":1757908892351,"bids":[],"asks":[]}"#
            ),
            Some(1_757_908_892_351_000)
        );
        assert_eq!(
            exchange_ts(
                r#"{"event_type":"book","asset_id":"1","timestamp":"1700000000","bids":[],"asks":[]}"#
            ),
            Some(1_700_000_000_000_000)
        );
        assert_eq!(
            exchange_ts(
                r#"{"event_type":"book","asset_id":"1","timestamp":"2026-06-29T17:15:57.257Z","bids":[],"asks":[]}"#
            ),
            Some(1_782_753_357_257_000)
        );

        // Present but invalid timestamps are decode errors, never `None`.
        for timestamp in [
            r#""""#,
            r#""  ""#,
            r#""-5""#,
            r#""12.5""#,
            r#""abc""#,
            r#""9223372036854775807""#,
            r#"true"#,
            r#"{"ms":1}"#,
        ] {
            let payload = format!(
                r#"{{"event_type":"book","asset_id":"1","timestamp":{timestamp},"bids":[],"asks":[]}}"#
            );
            let error = decode_polymarket_message(&payload, NOW)
                .expect_err("invalid timestamp must be rejected");
            assert!(
                error.to_string().contains("timestamp"),
                "unexpected error for {timestamp}: {error:#}"
            );
        }
    }

    #[test]
    fn price_change_requires_array_and_valid_levels() {
        // `price_changes` is a mandatory array.
        assert!(
            decode_polymarket_message(r#"{"event_type":"price_change","asset_id":"1"}"#, NOW)
                .is_err()
        );
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"price_change","asset_id":"1","price_changes":null}"#,
                NOW
            )
            .is_err()
        );
        assert!(
            decode_polymarket_message(
                r#"{"event_type":"price_change","asset_id":"1","price_changes":{}}"#,
                NOW
            )
            .is_err()
        );
        assert!(decode_polymarket_message(
            r#"{"event_type":"price_change","asset_id":"1","price_changes":[{"asset_id":"1","price":"0.5"}]}"#,
            NOW
        )
        .is_err());
    }

    #[test]
    fn unknown_event_types_are_counted_but_not_fatal() {
        let events = decode_polymarket_message(
            r#"{"event_type":"totally_new_event_kind","asset_id":"1"}"#,
            NOW,
        )
        .unwrap();
        assert!(events.is_empty());
        let registry = unknown_event_types();
        let count = registry.get("totally_new_event_kind").copied().unwrap_or(0);
        assert!(count >= 1, "unknown event type must be counted");
    }
}
