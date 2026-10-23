//! Configuration loading, CLI overrides, and validation.
//!
//! Precedence is: built-in defaults, then an optional TOML file, then
//! environment variables, then explicit CLI flags.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeKind {
    #[default]
    Kalshi,
    Polymarket,
}

impl std::fmt::Display for ExchangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kalshi => f.write_str("kalshi"),
            Self::Polymarket => f.write_str("polymarket"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum KalshiEnvironment {
    /// Safe default: demo credentials and demo endpoints unless production is
    /// explicitly selected in the config, CLI, or KALSHI_ENVIRONMENT.
    #[default]
    Demo,
    Production,
}

impl KalshiEnvironment {
    #[must_use]
    pub fn rest_base(self) -> &'static str {
        match self {
            Self::Production => "https://external-api.kalshi.com/trade-api/v2",
            Self::Demo => "https://demo-api.kalshi.co/trade-api/v2",
        }
    }

    #[must_use]
    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Production => "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
            Self::Demo => "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KalshiSettings {
    pub environment: KalshiEnvironment,
    pub key_id: String,
    pub private_key_path: Option<PathBuf>,
    /// Number of market tickers per orderbook subscription.
    pub batch_size: usize,
    /// REST page size for market discovery (Kalshi maximum: 1000).
    pub discovery_page_size: usize,
    pub request_timeout_secs: u64,
    /// Delay between discovery pages, to stay well inside rate limits.
    pub discovery_page_delay_ms: u64,
}

impl Default for KalshiSettings {
    fn default() -> Self {
        Self {
            environment: KalshiEnvironment::Demo,
            key_id: String::new(),
            private_key_path: None,
            batch_size: 100,
            discovery_page_size: 1000,
            request_timeout_secs: 20,
            discovery_page_delay_ms: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketSettings {
    pub gamma_url: String,
    pub ws_url: String,
    /// Number of asset ids per dynamic subscribe/unsubscribe frame. The
    /// initial snapshot subscription is always sent as one frame.
    pub batch_size: usize,
    /// Gamma API page size.
    pub discovery_page_size: usize,
    pub request_timeout_secs: u64,
    /// Delay between discovery pages.
    pub discovery_page_delay_ms: u64,
    /// Only include books that are currently accepting orders.
    pub require_accepting_orders: bool,
}

impl Default for PolymarketSettings {
    fn default() -> Self {
        Self {
            gamma_url: "https://gamma-api.polymarket.com".to_owned(),
            ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            batch_size: 500,
            discovery_page_size: 500,
            request_timeout_secs: 20,
            discovery_page_delay_ms: 50,
            require_accepting_orders: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorSettings {
    /// Number of shards. Set 0 to derive a shard count from the initial symbol
    /// count and `max_symbols_per_socket`.
    pub shards: usize,
    /// Redundant websocket connections per shard.
    pub replicas: usize,
    /// Soft capacity used to size automatic shards.
    pub max_symbols_per_socket: usize,
    /// Upper bound when deriving shards automatically.
    pub max_shards: usize,
    /// Full local snapshot cadence.
    pub snapshot_interval_secs: u64,
    /// Market discovery reconciliation cadence.
    pub discovery_interval_secs: u64,
    /// Refresh a source connection after this long, with jitter.
    pub reconnect_after_secs: u64,
    /// Reconnect if no frame (data or heartbeat) arrives for this long.
    pub connection_idle_timeout_secs: u64,
    /// Probe a quiet symbol after this long without an event.
    pub stale_probe_after_secs: u64,
    /// Minimum spacing between probes for one symbol.
    pub probe_interval_secs: u64,
    /// Failed probes before failing over/reconnecting.
    pub max_probes: u32,
    /// Emit final snapshots for live books during graceful shutdown.
    pub final_snapshots: bool,
    /// Bounded writer queue depth, in messages.
    pub writer_queue: usize,
    /// Flush cadence for partial SBE chunks.
    pub flush_interval_secs: u64,
    /// Optional cap on discovered symbols (0 means no cap). Useful for smoke
    /// tests and latency experiments.
    pub max_markets: usize,
    /// Optional run duration; 0 means run until SIGINT/SIGTERM.
    pub duration_secs: u64,
    /// Maximum accepted outbound writer backlog before the service fails.
    pub writer_send_timeout_secs: u64,
}

impl Default for CollectorSettings {
    fn default() -> Self {
        Self {
            shards: 4,
            replicas: 2,
            max_symbols_per_socket: 5000,
            max_shards: 64,
            snapshot_interval_secs: 60,
            discovery_interval_secs: 30,
            reconnect_after_secs: 1800,
            connection_idle_timeout_secs: 45,
            stale_probe_after_secs: 120,
            probe_interval_secs: 30,
            max_probes: 3,
            final_snapshots: true,
            writer_queue: 16_384,
            // Output-visibility contract: every interval that received any
            // message ends with a flush, so a downstream reader sees an
            // accepted delta within ~1 second instead of waiting for a full
            // chunk or shutdown. Each flush pads to the chunk boundary, so a
            // quiet-but-active capture trades padding bytes for low latency.
            flush_interval_secs: 1,
            max_markets: 0,
            duration_secs: 0,
            writer_send_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutputSettings {
    /// File path, or `-` for binary stdout.
    pub path: String,
    /// Aligned chunk size. Must be a power of two.
    pub chunk_size: usize,
    /// Optional path for a JSON market-metadata sidecar.
    pub metadata_path: Option<PathBuf>,
    /// Optional path for a JSON run report.
    pub report_path: Option<PathBuf>,
    /// Ask the OS to persist the output file on shutdown.
    pub sync_on_close: bool,
}

impl Default for OutputSettings {
    fn default() -> Self {
        Self {
            path: "-".to_owned(),
            chunk_size: 64 * 1024,
            metadata_path: None,
            report_path: None,
            sync_on_close: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsSettings {
    /// Optional `host:port` for Prometheus text and `/healthz`.
    pub listen: Option<SocketAddr>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub exchange: ExchangeKind,
    pub kalshi: KalshiSettings,
    pub polymarket: PolymarketSettings,
    pub collector: CollectorSettings,
    pub output: OutputSettings,
    pub metrics: MetricsSettings,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub exchange: ExchangeKind,
    pub kalshi: KalshiSettings,
    pub polymarket: PolymarketSettings,
    pub collector: CollectorSettings,
    pub output: OutputSettings,
    pub metrics: MetricsSettings,
}

impl Settings {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let file = match path {
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .with_context(|| format!("reading config `{}`", path.display()))?;
                toml::from_str::<FileConfig>(&text)
                    .with_context(|| format!("parsing config `{}`", path.display()))?
            }
            None => FileConfig::default(),
        };
        let mut settings = Self {
            exchange: file.exchange,
            kalshi: file.kalshi,
            polymarket: file.polymarket,
            collector: file.collector,
            output: file.output,
            metrics: file.metrics,
        };
        settings.apply_environment()?;
        settings.validate()?;
        Ok(settings)
    }

    fn apply_environment(&mut self) -> anyhow::Result<()> {
        if let Ok(exchange) = std::env::var("COLLECTOR_EXCHANGE") {
            self.exchange = match exchange.to_ascii_lowercase().as_str() {
                "kalshi" => ExchangeKind::Kalshi,
                "polymarket" => ExchangeKind::Polymarket,
                other => bail!("unknown COLLECTOR_EXCHANGE `{other}`"),
            };
        }
        if let Ok(value) = std::env::var("KALSHI_ENVIRONMENT") {
            self.kalshi.environment = match value.to_ascii_lowercase().as_str() {
                "production" | "prod" => KalshiEnvironment::Production,
                "demo" => KalshiEnvironment::Demo,
                other => bail!("unknown KALSHI_ENVIRONMENT `{other}`"),
            };
        }
        if let Ok(value) = std::env::var("KALSHI_API_KEY_ID") {
            self.kalshi.key_id = value;
        }
        if let Ok(value) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
            self.kalshi.private_key_path = Some(PathBuf::from(value));
        }
        if let Ok(value) = std::env::var("COLLECTOR_OUTPUT") {
            self.output.path = value;
        }
        if let Ok(value) = std::env::var("COLLECTOR_METRICS_LISTEN") {
            self.metrics.listen = Some(
                value
                    .parse()
                    .with_context(|| format!("invalid COLLECTOR_METRICS_LISTEN `{value}`"))?,
            );
        }
        Ok(())
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.collector.shards <= 256, "shards must be <= 256");
        anyhow::ensure!(self.collector.replicas >= 1, "replicas must be >= 1");
        anyhow::ensure!(self.collector.replicas <= 4, "replicas must be <= 4");
        anyhow::ensure!(
            self.collector.max_symbols_per_socket > 0,
            "max_symbols_per_socket must be > 0"
        );
        anyhow::ensure!(
            self.collector.max_shards >= self.collector.shards.max(1),
            "max_shards must be >= shards"
        );
        anyhow::ensure!(
            self.collector.snapshot_interval_secs >= 1,
            "snapshot_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.discovery_interval_secs >= 1,
            "discovery_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.reconnect_after_secs >= 10,
            "reconnect_after_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.connection_idle_timeout_secs >= 10,
            "connection_idle_timeout_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.stale_probe_after_secs >= 10,
            "stale_probe_after_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.probe_interval_secs >= 1,
            "probe_interval_secs must be >= 1"
        );
        anyhow::ensure!(self.collector.max_probes >= 1, "max_probes must be >= 1");
        anyhow::ensure!(
            self.collector.writer_queue >= 128,
            "writer_queue must be >= 128"
        );
        anyhow::ensure!(
            self.collector.flush_interval_secs >= 1,
            "flush_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            self.output.chunk_size >= 1024 && self.output.chunk_size.is_power_of_two(),
            "output.chunk_size must be a power of two >= 1024"
        );
        anyhow::ensure!(!self.output.path.is_empty(), "output path is empty");
        if let Some(path) = &self.output.metadata_path {
            anyhow::ensure!(
                path.as_os_str() != self.output.path.as_str(),
                "metadata_path must differ from output.path"
            );
            anyhow::ensure!(
                path.as_os_str() != "-",
                "metadata_path must be a file path, not stdout"
            );
        }
        if let Some(path) = &self.output.report_path {
            anyhow::ensure!(
                path.as_os_str() != self.output.path.as_str(),
                "report_path must differ from output.path"
            );
            anyhow::ensure!(
                path.as_os_str() != "-",
                "report_path must be a file path, not stdout"
            );
        }
        if let (Some(metadata), Some(report)) =
            (&self.output.metadata_path, &self.output.report_path)
        {
            anyhow::ensure!(
                metadata != report,
                "metadata_path and report_path must differ"
            );
        }
        anyhow::ensure!(
            self.kalshi.batch_size >= 1 && self.kalshi.batch_size <= 1000,
            "kalshi.batch_size must be in [1, 1000]"
        );
        anyhow::ensure!(
            self.kalshi.discovery_page_size >= 1 && self.kalshi.discovery_page_size <= 1000,
            "kalshi.discovery_page_size must be in [1, 1000]"
        );
        anyhow::ensure!(
            self.polymarket.batch_size >= 1,
            "polymarket.batch_size must be >= 1"
        );
        anyhow::ensure!(
            self.polymarket.discovery_page_size >= 1,
            "polymarket.discovery_page_size must be >= 1"
        );
        for (name, url) in [
            ("polymarket.gamma_url", &self.polymarket.gamma_url),
            ("polymarket.ws_url", &self.polymarket.ws_url),
        ] {
            url::Url::parse(url).with_context(|| format!("invalid {name}"))?;
        }
        url::Url::parse(self.kalshi.environment.rest_base())
            .context("invalid Kalshi REST base URL")?;
        url::Url::parse(self.kalshi.environment.ws_url())
            .context("invalid Kalshi websocket URL")?;
        Ok(())
    }

    #[must_use]
    pub fn discovery_interval(&self) -> Duration {
        Duration::from_secs(self.collector.discovery_interval_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kalshi_defaults_to_demo_not_production() {
        assert_eq!(KalshiEnvironment::default(), KalshiEnvironment::Demo);
        let settings = KalshiSettings::default();
        assert_eq!(settings.environment, KalshiEnvironment::Demo);
        assert!(settings.environment.ws_url().contains("demo"));
        assert!(settings.environment.rest_base().contains("demo"));
    }

    #[test]
    fn rejects_output_and_sidecar_path_collisions() {
        let mut settings = Settings::load(None).unwrap();
        settings.output.path = "capture.sbe".to_owned();
        settings.output.metadata_path = Some(PathBuf::from("capture.sbe"));
        assert!(settings.validate().is_err());

        settings.output.metadata_path = Some(PathBuf::from("metadata.json"));
        settings.output.report_path = Some(PathBuf::from("capture.sbe"));
        assert!(settings.validate().is_err());

        settings.output.report_path = Some(PathBuf::from("metadata.json"));
        assert!(settings.validate().is_err());

        settings.output.report_path = Some(PathBuf::from("-"));
        assert!(settings.validate().is_err());
    }
}
