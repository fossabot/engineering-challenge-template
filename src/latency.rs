//! Measured discovery and websocket timestamp-latency experiment.
//!
//! The command never invents numbers: if no frames arrive, percentiles are
//! absent. Exchange timestamps are compared with the local receive clock, so
//! the result includes any exchange/local clock offset. The methodology is
//! documented in `docs/latency.md`.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use serde::Serialize;
use tokio::time::{MissedTickBehavior, interval, sleep_until};
use tracing::{info, warn};

use crate::{
    config::Settings,
    coordinator::build_exchange,
    model::{EventMeta, ExchangeEvent, SourceId},
    report::write_json_atomic,
    session::SessionFailure,
    time::now_us,
};

#[derive(Debug, Clone, Serialize)]
pub struct LatencyReport {
    pub exchange: String,
    pub environment: String,
    pub measured_at_us: i64,
    pub discovery_rtt_ms: f64,
    pub discovered_markets: usize,
    pub subscribed_markets: usize,
    pub duration_secs: u64,
    pub frames: u64,
    pub events: u64,
    pub bytes: u64,
    pub latency_samples: u64,
    pub negative_latency_samples: u64,
    pub latency_p50_us: Option<i64>,
    pub latency_p95_us: Option<i64>,
    pub latency_p99_us: Option<i64>,
    pub latency_min_us: Option<i64>,
    pub latency_max_us: Option<i64>,
    pub latency_mean_us: Option<i64>,
    pub error: Option<String>,
    pub notes: Vec<String>,
}

/// Measure discovery RTT and exchange-to-local timestamp latency for a short
/// live session.
pub async fn run(
    settings: Settings,
    max_markets: usize,
    duration_secs: u64,
    output: Option<&Path>,
) -> anyhow::Result<LatencyReport> {
    settings.validate()?;
    let exchange = build_exchange(&settings, true)?;
    let discovery_started = Instant::now();
    let markets = exchange
        .discover_markets(max_markets)
        .await
        .with_context(|| format!("discovering {} markets for latency run", exchange.name()))?;
    let discovery_rtt_ms = discovery_started.elapsed().as_secs_f64() * 1_000.0;
    if markets.is_empty() {
        bail!("{} returned no markets for latency run", exchange.name());
    }
    let wanted = if max_markets == 0 {
        markets.len()
    } else {
        max_markets.min(markets.len())
    };
    let desired: std::collections::BTreeSet<String> = markets
        .iter()
        .take(wanted)
        .map(|market| market.native_symbol.clone())
        .collect();
    let factory = Arc::clone(&exchange).session_factory();
    let mut session = factory
        .create_session(SourceId::new(0, 0))
        .await
        .context("creating latency session")?;
    session.connect().await.map_err(|failure| {
        anyhow::anyhow!("connecting latency session: {}", failure_text(&failure))
    })?;
    session
        .apply_subscriptions(&desired)
        .await
        .map_err(|failure| {
            anyhow::anyhow!("subscribing latency session: {}", failure_text(&failure))
        })?;
    info!(
        exchange = exchange.name(),
        markets = wanted,
        duration_secs,
        "latency session started"
    );

    let mut frames = 0u64;
    let mut events = 0u64;
    let mut bytes = 0u64;
    let mut negative_latency_samples = 0u64;
    let mut samples: Vec<i64> = Vec::new();
    let mut error: Option<String> = None;
    let mut keepalive = interval(Duration::from_secs(10));
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    keepalive.tick().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs.max(1));
    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            _ = keepalive.tick() => {
                if let Err(failure) = session.send_keepalive().await {
                    error = Some(format!("keepalive failed: {}", failure_text(&failure)));
                    break;
                }
            }
            frame = session.next_frame() => {
                match frame {
                    Ok(frame) => {
                        if frame.bytes > 0 {
                            frames = frames.saturating_add(1);
                            bytes = bytes.saturating_add(frame.bytes as u64);
                        }
                        for event in &frame.events {
                            events = events.saturating_add(1);
                            let Some(meta) = event_meta(event) else {
                                continue;
                            };
                            let Some(exchange_ts_us) = meta.exchange_ts_us else {
                                continue;
                            };
                            let latency = meta.received_at_us.saturating_sub(exchange_ts_us);
                            if latency < 0 {
                                negative_latency_samples = negative_latency_samples.saturating_add(1);
                                continue;
                            }
                            if latency <= 60_000_000 && samples.len() < 1_000_000 {
                                samples.push(latency);
                            }
                        }
                    }
                    Err(failure) => {
                        error = Some(format!("websocket read failed: {}", failure_text(&failure)));
                        break;
                    }
                }
            }
        }
    }
    session.close().await;
    samples.sort_unstable();
    let latency_samples = samples.len() as u64;
    let report = LatencyReport {
        exchange: exchange.name().to_owned(),
        environment: exchange.environment().to_owned(),
        measured_at_us: now_us().unwrap_or(0),
        discovery_rtt_ms,
        discovered_markets: markets.len(),
        subscribed_markets: wanted,
        duration_secs: duration_secs.max(1),
        frames,
        events,
        bytes,
        latency_samples,
        negative_latency_samples,
        latency_p50_us: percentile(&samples, 50),
        latency_p95_us: percentile(&samples, 95),
        latency_p99_us: percentile(&samples, 99),
        latency_min_us: samples.first().copied(),
        latency_max_us: samples.last().copied(),
        latency_mean_us: (!samples.is_empty()).then(|| {
            let sum: i128 = samples.iter().map(|value| i128::from(*value)).sum();
            i64::try_from(sum / samples.len() as i128).unwrap_or(i64::MAX)
        }),
        error,
        notes: vec![
            "discovery_rtt_ms is a local wall-clock measurement around one discovery call".to_owned(),
            "latency is local_receive_us - exchange_timestamp_us and includes exchange/local clock offset".to_owned(),
            "negative values are counted separately and excluded from percentiles".to_owned(),
            "no latency value is reported when no timestamped events arrived".to_owned(),
        ],
    };
    if let Some(path) = output {
        write_json_atomic(path, &report).context("writing latency report")?;
    }
    if let Some(error) = &report.error {
        warn!(error, "latency run ended with a transport error");
    }
    Ok(report)
}

fn event_meta(event: &ExchangeEvent) -> Option<&EventMeta> {
    match event {
        ExchangeEvent::Snapshot { meta, .. }
        | ExchangeEvent::LevelChange { meta, .. }
        | ExchangeEvent::Lifecycle { meta, .. } => Some(meta),
        ExchangeEvent::StreamReset { .. } => None,
    }
}

fn percentile(sorted: &[i64], percentile: u32) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let len = sorted.len() as u64;
    let rank = len.saturating_mul(u64::from(percentile)).saturating_add(99) / 100;
    let index = rank.saturating_sub(1).min(len.saturating_sub(1));
    sorted.get(usize::try_from(index).ok()?).copied()
}

fn failure_text(failure: &SessionFailure) -> String {
    match failure {
        SessionFailure::Reconnect(error)
        | SessionFailure::Decode(error)
        | SessionFailure::Protocol(error)
        | SessionFailure::Fatal(error) => format!("{error:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_nearest_rank_without_extrapolation() {
        assert_eq!(percentile(&[], 50), None);
        assert_eq!(percentile(&[5], 99), Some(5));
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), Some(3));
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 95), Some(5));
    }
}
