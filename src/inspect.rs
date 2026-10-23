//! Offline inspection of an aligned SBE capture.

use std::{collections::BTreeSet, path::Path};

use anyhow::Context;
use serde::Serialize;
use serde_json::{Value, json};

use crate::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

#[derive(Debug, Serialize)]
pub struct InspectSummary {
    file: String,
    chunk_size: usize,
    messages: u64,
    snapshots: u64,
    price_level_updates: u64,
    unknown_messages: u64,
    symbols: usize,
    first_received_at_us: Option<i64>,
    last_received_at_us: Option<i64>,
    first_exchange_timestamp_us: Option<i64>,
    last_exchange_timestamp_us: Option<i64>,
    samples: Vec<Value>,
}

/// Inspect one capture. `chunk_size` is either an integer or `auto`.
pub fn run(input: &Path, chunk_size: &str, limit: usize) -> anyhow::Result<InspectSummary> {
    let mut reader = if chunk_size.eq_ignore_ascii_case("auto") {
        AlignedSbeReader::<OrderbookSchema>::open_auto(input)
            .with_context(|| format!("opening `{}` with auto chunk detection", input.display()))?
    } else {
        let chunk_size: usize = chunk_size
            .parse()
            .with_context(|| format!("invalid chunk size `{chunk_size}`"))?;
        AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(input, chunk_size)
            .with_context(|| format!("opening `{}`", input.display()))?
    };
    let mut summary = InspectSummary {
        file: input.display().to_string(),
        chunk_size: reader.chunk_size(),
        messages: 0,
        snapshots: 0,
        price_level_updates: 0,
        unknown_messages: 0,
        symbols: 0,
        first_received_at_us: None,
        last_received_at_us: None,
        first_exchange_timestamp_us: None,
        last_exchange_timestamp_us: None,
        samples: Vec::new(),
    };
    let mut symbols = BTreeSet::new();
    while let Some(message) = reader.read_msg()? {
        summary.messages = summary.messages.saturating_add(1);
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot =
                    SnapshotDomain::try_from_decoder(decoder).context("decoding SBE snapshot")?;
                summary.snapshots = summary.snapshots.saturating_add(1);
                symbols.insert(snapshot.symbol.clone());
                let received = snapshot.received_at.timestamp_micros();
                let exchange = snapshot.exchange_timestamp.timestamp_micros();
                update_bounds(&mut summary, received, exchange);
                if summary.samples.len() < limit {
                    summary.samples.push(snapshot_json(&snapshot));
                }
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)
                    .context("decoding SBE price-level update")?;
                summary.price_level_updates = summary.price_level_updates.saturating_add(1);
                symbols.insert(update.symbol.clone());
                let received = update.backtest_timestamp.timestamp_micros();
                let exchange = update.exchange_timestamp.timestamp_micros();
                update_bounds(&mut summary, received, exchange);
                if summary.samples.len() < limit {
                    summary.samples.push(update_json(&update));
                }
            }
            AnyMessage::Unknown { .. } => {
                summary.unknown_messages = summary.unknown_messages.saturating_add(1);
            }
        }
    }
    summary.symbols = symbols.len();
    Ok(summary)
}

fn update_bounds(summary: &mut InspectSummary, received: i64, exchange: i64) {
    summary.first_received_at_us = Some(
        summary
            .first_received_at_us
            .map_or(received, |value| value.min(received)),
    );
    summary.last_received_at_us = Some(
        summary
            .last_received_at_us
            .map_or(received, |value| value.max(received)),
    );
    summary.first_exchange_timestamp_us = Some(
        summary
            .first_exchange_timestamp_us
            .map_or(exchange, |value| value.min(exchange)),
    );
    summary.last_exchange_timestamp_us = Some(
        summary
            .last_exchange_timestamp_us
            .map_or(exchange, |value| value.max(exchange)),
    );
}

fn snapshot_json(snapshot: &SnapshotDomain) -> Value {
    json!({
        "type": "snapshot",
        "symbol": snapshot.symbol,
        "received_at_us": snapshot.received_at.timestamp_micros(),
        "exchange_timestamp_us": snapshot.exchange_timestamp.timestamp_micros(),
        "bids": snapshot.bids.iter().map(|level| json!({
            "price_level": level.price_level,
            "quantity": level.quantity,
        })).collect::<Vec<_>>(),
        "asks": snapshot.asks.iter().map(|level| json!({
            "price_level": level.price_level,
            "quantity": level.quantity,
        })).collect::<Vec<_>>(),
    })
}

fn update_json(update: &PriceLevelUpdateDomain) -> Value {
    json!({
        "type": "price_level_update",
        "symbol": update.symbol,
        "received_at_us": update.backtest_timestamp.timestamp_micros(),
        "exchange_timestamp_us": update.exchange_timestamp.timestamp_micros(),
        "price_level": update.price_level,
        "quantity": update.quantity,
        "side": match update.side {
            crate::Side::Buy => "Buy",
            crate::Side::Sell => "Sell",
        },
    })
}
