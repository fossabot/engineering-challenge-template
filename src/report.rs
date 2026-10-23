//! Run reports and market-metadata sidecars.
//!
//! These files are deliberately independent of the SBE stream: operators can
//! inspect what was collected, how the process ended, and which native exchange
//! identifiers correspond to each SBE symbol without decoding the binary file.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    config::{ExchangeKind, Settings},
    metrics::{CollectorStats, WriterStats},
    model::{MarketInfo, SourceStatus},
    time::now_us,
};

/// Stable report schema version.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

/// Safe, non-secret configuration values included in a run report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    pub exchange: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kalshi_environment: Option<String>,
    pub shard_count: usize,
    pub replica_count: usize,
    pub snapshot_interval_secs: u64,
    pub discovery_interval_secs: u64,
    pub reconnect_after_secs: u64,
    pub connection_idle_timeout_secs: u64,
    pub stale_probe_after_secs: u64,
    pub probe_interval_secs: u64,
    pub max_probes: u32,
    pub max_markets: usize,
    pub writer_queue: usize,
    pub flush_interval_secs: u64,
    pub output_chunk_size: usize,
}

impl ReportConfig {
    #[must_use]
    pub fn from_settings(settings: &Settings, shard_count: usize) -> Self {
        Self {
            exchange: settings.exchange.to_string(),
            kalshi_environment: matches!(settings.exchange, ExchangeKind::Kalshi).then(|| {
                match settings.kalshi.environment {
                    crate::config::KalshiEnvironment::Demo => "demo".to_owned(),
                    crate::config::KalshiEnvironment::Production => "production".to_owned(),
                }
            }),
            shard_count,
            replica_count: settings.collector.replicas,
            snapshot_interval_secs: settings.collector.snapshot_interval_secs,
            discovery_interval_secs: settings.collector.discovery_interval_secs,
            reconnect_after_secs: settings.collector.reconnect_after_secs,
            connection_idle_timeout_secs: settings.collector.connection_idle_timeout_secs,
            stale_probe_after_secs: settings.collector.stale_probe_after_secs,
            probe_interval_secs: settings.collector.probe_interval_secs,
            max_probes: settings.collector.max_probes,
            max_markets: settings.collector.max_markets,
            writer_queue: settings.collector.writer_queue,
            flush_interval_secs: settings.collector.flush_interval_secs,
            output_chunk_size: settings.output.chunk_size,
        }
    }
}

/// Machine-readable summary written at the end of a live or replay run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub schema_version: u32,
    pub tool: String,
    pub version: String,
    pub mode: String,
    pub exchange: String,
    pub environment: String,
    pub started_at_us: i64,
    pub ended_at_us: i64,
    pub shutdown_reason: String,
    pub output_path: String,
    pub shard_count: usize,
    pub replica_count: usize,
    pub discovered_symbols: usize,
    pub final_symbols: usize,
    pub core: CollectorStats,
    pub writer: WriterStats,
    pub sources: Vec<SourceStatus>,
    pub config: ReportConfig,
}

impl RunReport {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        mode: &str,
        exchange: &str,
        environment: &str,
        started_at_us: i64,
        ended_at_us: i64,
        shutdown_reason: &str,
        output_path: &str,
        shard_count: usize,
        replica_count: usize,
        discovered_symbols: usize,
        final_symbols: usize,
        core: CollectorStats,
        writer: WriterStats,
        sources: Vec<SourceStatus>,
        config: ReportConfig,
    ) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            tool: env!("CARGO_PKG_NAME").to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            mode: mode.to_owned(),
            exchange: exchange.to_owned(),
            environment: environment.to_owned(),
            started_at_us,
            ended_at_us,
            shutdown_reason: shutdown_reason.to_owned(),
            output_path: output_path.to_owned(),
            shard_count,
            replica_count,
            discovered_symbols,
            final_symbols,
            core,
            writer,
            sources,
            config,
        }
    }

    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        write_json_atomic(path, self)
    }
}

/// Metadata sidecar keyed by SBE symbol and native exchange symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetadata {
    pub schema_version: u32,
    pub generated_at_us: i64,
    pub exchange: String,
    pub count: usize,
    pub markets: Vec<MarketInfo>,
}

impl MarketMetadata {
    #[must_use]
    pub fn new(exchange: &str, markets: Vec<MarketInfo>) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at_us: now_us().unwrap_or(0),
            exchange: exchange.to_owned(),
            count: markets.len(),
            markets,
        }
    }

    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        write_json_atomic(path, self)
    }
}

/// Write JSON through a sibling temporary file and rename it into place. This
/// avoids exposing a truncated report if the process dies mid-write.
pub fn write_json_atomic<T: Serialize + ?Sized>(path: &Path, value: &T) -> anyhow::Result<()> {
    if path.as_os_str() == "-" {
        anyhow::bail!("JSON sidecar path must be a file, not stdout");
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating sidecar directory `{}`", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("sidecar path has no file name")?;
    let temporary = path.with_file_name(format!(
        ".{file_name}.tmp.{}.{}",
        std::process::id(),
        now_us().unwrap_or(0)
    ));
    let result = (|| -> anyhow::Result<()> {
        let file = File::create(&temporary)
            .with_context(|| format!("creating temporary sidecar `{}`", temporary.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)
            .with_context(|| format!("serializing sidecar `{}`", path.display()))?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        std::fs::rename(&temporary, path)
            .with_context(|| format!("installing sidecar `{}`", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Best-effort sidecar path resolution. Empty paths are treated as absent.
#[must_use]
pub fn sidecar_path(path: &Option<PathBuf>) -> Option<&Path> {
    path.as_deref().filter(|path| !path.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MarketInfo;

    fn market() -> MarketInfo {
        MarketInfo {
            exchange: "kalshi".to_owned(),
            native_symbol: "KXTEST".to_owned(),
            symbol: "kalshi:KXTEST".to_owned(),
            title: Some("Test".to_owned()),
            outcome: None,
            exchange_market_id: None,
            close_time_us: None,
            tick_size_us: Some(10_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
        }
    }

    #[test]
    fn writes_and_replaces_sidecar() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/report.json");
        let metadata = MarketMetadata::new("kalshi", vec![market()]);
        metadata.write(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let decoded: MarketMetadata = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded.count, 1);
        assert_eq!(decoded.markets[0].native_symbol, "KXTEST");
        metadata.write(&path).unwrap();
        assert!(path.is_file());
    }

    #[test]
    fn rejects_stdout_sidecar() {
        assert!(write_json_atomic(Path::new("-"), &serde_json::json!({})).is_err());
    }
}
