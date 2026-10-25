//! Lightweight Prometheus endpoint and shared counters.
//!
//! The collector avoids a metrics framework here: one lock-protected
//! snapshot is updated once per second, and the HTTP handler formats it on
//! demand. No data-path counter is ever blocked on the network.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::{debug, warn};

use crate::{
    model::{SourceId, SourceStatus},
    time::now_us,
};

/// Collector data-path counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectorStats {
    pub discovery_rounds: u64,
    pub discovered_symbols: u64,
    pub added_symbols: u64,
    pub removed_symbols: u64,
    pub exchange_snapshots: u64,
    pub periodic_snapshots: u64,
    pub failover_snapshots: u64,
    pub final_snapshots: u64,
    pub empty_snapshots: u64,
    pub deltas_emitted: u64,
    pub deltas_rejected: u64,
    pub sequence_resets: u64,
    /// Kalshi exchange-clock decreases observed on an active book. These are
    /// expected across exchange maintenance and start a new comparison epoch.
    pub clock_resets: u64,
    pub source_switches: u64,
    pub stale_probes: u64,
    pub stale_failovers: u64,
    pub crossed_snapshots_skipped: u64,
    pub duplicate_events: u64,
    pub lifecycle_events: u64,
    pub current_symbols: u64,
    pub shard_count: u64,
}

/// Counters owned by the writer thread.
#[derive(Debug, Default)]
pub struct WriterCounters {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub flushes: AtomicU64,
    pub errors: AtomicU64,
    pub queue_depth: AtomicU64,
}

impl WriterCounters {
    #[must_use]
    pub fn snapshot(&self) -> WriterStats {
        WriterStats {
            messages: self.messages.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriterStats {
    pub messages: u64,
    pub bytes: u64,
    pub flushes: u64,
    pub errors: u64,
    pub queue_depth: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub exchange: String,
    pub started_at_us: i64,
    pub updated_at_us: i64,
    pub core: CollectorStats,
    pub writer: WriterStats,
    pub sources: Vec<SourceStatus>,
}

/// Shared state read by the HTTP handler and run-report writer.
#[derive(Clone)]
pub struct MetricsRegistry {
    exchange: Arc<str>,
    started_at_us: i64,
    state: Arc<RwLock<MetricsSnapshot>>,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new(exchange: &str) -> Self {
        let now = now_us().unwrap_or(0);
        Self {
            exchange: Arc::from(exchange),
            started_at_us: now,
            state: Arc::new(RwLock::new(MetricsSnapshot {
                exchange: exchange.to_owned(),
                started_at_us: now,
                updated_at_us: now,
                core: CollectorStats::default(),
                writer: WriterStats::default(),
                sources: Vec::new(),
            })),
        }
    }

    pub fn update(
        &self,
        core: CollectorStats,
        writer: WriterStats,
        sources: &BTreeMap<SourceId, SourceStatus>,
    ) {
        let Ok(mut state) = self.state.write() else {
            return;
        };
        state.updated_at_us = now_us().unwrap_or(state.updated_at_us);
        state.core = core;
        state.writer = writer;
        state.sources = sources.values().cloned().collect();
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.state
            .read()
            .map(|state| state.clone())
            .unwrap_or_else(|_| MetricsSnapshot {
                exchange: self.exchange.to_string(),
                started_at_us: self.started_at_us,
                updated_at_us: self.started_at_us,
                core: CollectorStats::default(),
                writer: WriterStats::default(),
                sources: Vec::new(),
            })
    }

    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut out = String::with_capacity(4096);
        let core = &snapshot.core;
        macro_rules! metric {
            ($name:literal, $help:literal, $value:expr) => {
                out.push_str(concat!("# HELP ", $name, " ", $help, "\n"));
                out.push_str(concat!("# TYPE ", $name, " gauge\n"));
                out.push_str($name);
                out.push(' ');
                out.push_str(&$value.to_string());
                out.push('\n');
            };
        }
        metric!(
            "collector_symbols_active",
            "Symbols currently subscribed",
            core.current_symbols
        );
        metric!("collector_shards", "Configured shards", core.shard_count);
        metric!(
            "collector_exchange_snapshots_total",
            "Snapshots received from exchanges",
            core.exchange_snapshots
        );
        metric!(
            "collector_periodic_snapshots_total",
            "Locally generated periodic snapshots",
            core.periodic_snapshots
        );
        metric!(
            "collector_failover_snapshots_total",
            "Snapshots emitted on source switch",
            core.failover_snapshots
        );
        metric!(
            "collector_final_snapshots_total",
            "Snapshots emitted during shutdown",
            core.final_snapshots
        );
        metric!(
            "collector_empty_snapshots_total",
            "Empty snapshots emitted on removal",
            core.empty_snapshots
        );
        metric!(
            "collector_deltas_emitted_total",
            "Absolute price-level updates emitted",
            core.deltas_emitted
        );
        metric!(
            "collector_deltas_rejected_total",
            "Rejected inconsistent updates",
            core.deltas_rejected
        );
        metric!(
            "collector_sequence_resets_total",
            "Source resets caused by sequence/stream faults",
            core.sequence_resets
        );
        metric!(
            "collector_clock_resets_total",
            "Exchange clock regressions treated as maintenance epochs",
            core.clock_resets
        );
        metric!(
            "collector_source_switches_total",
            "Active-source failovers",
            core.source_switches
        );
        metric!(
            "collector_stale_probes_total",
            "Quiet-symbol probes",
            core.stale_probes
        );
        metric!(
            "collector_stale_failovers_total",
            "Failovers caused by failed probes",
            core.stale_failovers
        );
        metric!(
            "collector_crossed_snapshots_skipped_total",
            "Crossed snapshots skipped",
            core.crossed_snapshots_skipped
        );
        metric!(
            "collector_discovery_rounds_total",
            "Market discovery rounds",
            core.discovery_rounds
        );
        metric!(
            "collector_writer_messages_total",
            "SBE messages written",
            snapshot.writer.messages
        );
        metric!(
            "collector_writer_bytes_total",
            "Bytes written including padding",
            snapshot.writer.bytes
        );
        metric!(
            "collector_writer_errors_total",
            "Writer errors",
            snapshot.writer.errors
        );
        metric!(
            "collector_writer_queue_depth",
            "Messages waiting for the writer",
            snapshot.writer.queue_depth
        );
        metric!(
            "collector_sources_connected",
            "Connected websocket sources",
            snapshot.sources.iter().filter(|s| s.connected).count()
        );
        metric!(
            "collector_sources_total",
            "Configured websocket sources",
            snapshot.sources.len()
        );

        for source in &snapshot.sources {
            out.push_str(&format!(
                "collector_source_connected{{source=\"{}\"}} {}\n",
                source.source,
                u8::from(source.connected)
            ));
            out.push_str(&format!(
                "collector_source_reconnects{{source=\"{}\"}} {}\n",
                source.source, source.reconnects
            ));
            out.push_str(&format!(
                "collector_source_messages{{source=\"{}\"}} {}\n",
                source.source, source.messages
            ));
            out.push_str(&format!(
                "collector_source_decode_errors{{source=\"{}\"}} {}\n",
                source.source, source.decode_errors
            ));
            out.push_str(&format!(
                "collector_source_protocol_errors{{source=\"{}\"}} {}\n",
                source.source, source.protocol_errors
            ));
            if let Some(latency) = source.latency_ewma_us {
                out.push_str(&format!(
                    "collector_source_latency_ms{{source=\"{}\"}} {:.3}\n",
                    source.source,
                    latency as f64 / 1000.0
                ));
            }
        }
        out
    }

    #[must_use]
    pub fn uptime_seconds(&self) -> f64 {
        let now = now_us().unwrap_or(self.started_at_us);
        (now.saturating_sub(self.started_at_us)) as f64 / 1_000_000.0
    }
}

/// Serve on an already-bound listener. The coordinator binds during startup so
/// a port conflict fails the run instead of surfacing in a detached task.
pub async fn run_server_on(
    listener: TcpListener,
    registry: MetricsRegistry,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    debug!(address = %listener.local_addr()?, "metrics server listening");
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => {
                let Ok((mut socket, peer)) = accepted else {
                    continue;
                };
                let registry = registry.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut request)).await;
                    let request = String::from_utf8_lossy(&request);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let (status, content_type, body) = match path {
                        "/metrics" => (
                            "200 OK",
                            "text/plain; version=0.0.4; charset=utf-8",
                            registry.render_prometheus(),
                        ),
                        "/healthz" => (
                            "200 OK",
                            "application/json",
                            format!(
                                "{{\"status\":\"ok\",\"uptime_seconds\":{:.3}}}\n",
                                registry.uptime_seconds()
                            ),
                        ),
                        _ => ("404 Not Found", "text/plain", "not found\n".to_owned()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        warn!(%peer, "metrics client disconnected");
                    }
                });
            }
        }
    }
    Ok(())
}
