//! Command-line surface. The binary is thin: every command
//! delegates to a library module with explicit inputs and outputs.

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};

use engineering_challenge::config::{ExchangeKind, KalshiEnvironment, Settings};

#[derive(Debug, Parser)]
#[command(
    name = "engineering-challenge",
    version,
    about = "Low-latency prediction-market order-book collector",
    long_about = "Collect live Kalshi (or public Polymarket) order books into the \
                  supplied FIX SBE schema, replay normalized fixtures, inspect \
                  captures, or measure live discovery and timestamp latency."
)]
pub struct Cli {
    /// TOML configuration file. Built-in defaults, file, environment, then CLI
    /// flags are applied in that order.
    #[arg(long, global = true, value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[arg(long, global = true, value_enum)]
    pub exchange: Option<ExchangeKind>,

    #[arg(long = "kalshi-environment", global = true, value_enum)]
    pub kalshi_environment: Option<KalshiEnvironment>,

    /// Kalshi API key id.
    #[arg(long = "kalshi-key-id", global = true)]
    pub kalshi_key_id: Option<String>,

    /// Path to a PEM-encoded Kalshi private key.
    #[arg(long = "kalshi-private-key", global = true, value_name = "FILE")]
    pub kalshi_private_key: Option<PathBuf>,

    /// Disable TCP_NODELAY on websocket sockets.
    #[arg(long = "no-tcp-nodelay", global = true)]
    pub no_tcp_nodelay: bool,

    /// Bind websocket connections to this local IP address.
    #[arg(long = "source-ip", global = true, value_name = "IP")]
    pub source_ip: Option<std::net::IpAddr>,

    /// Bind websocket connections to this local source port. The value is a
    /// base: each connection binds `base + shard * 256 + replica`.
    #[arg(long = "source-port", global = true, value_name = "PORT")]
    pub source_port: Option<u16>,

    /// Race this many simultaneous websocket handshakes and keep the first.
    #[arg(long = "connect-race", global = true, value_name = "N")]
    pub connect_race: Option<usize>,

    /// Output file, or `-` for binary stdout.
    #[arg(long, global = true, value_name = "FILE")]
    pub output: Option<String>,

    /// Aligned output chunk size. Defaults to 64 MiB, matching the template
    /// `AlignedSbeReader`. Must be a power of two of at least 1024 bytes.
    #[arg(long, global = true, value_name = "BYTES")]
    pub chunk_size: Option<usize>,

    #[arg(long, global = true)]
    pub shards: Option<usize>,

    #[arg(long, global = true)]
    pub replicas: Option<usize>,

    #[arg(long, global = true, value_name = "HOST:PORT")]
    pub metrics_listen: Option<SocketAddr>,

    #[arg(long, global = true, value_name = "FILE")]
    pub metadata: Option<PathBuf>,

    #[arg(long, global = true, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, global = true)]
    pub snapshot_interval_secs: Option<u64>,

    #[arg(long, global = true)]
    pub discovery_interval_secs: Option<u64>,

    /// Optional discovered-symbol cap. Zero means no cap.
    #[arg(long, global = true)]
    pub max_markets: Option<usize>,

    /// Optional live-run duration. Zero means run until SIGINT/SIGTERM.
    #[arg(long, global = true)]
    pub duration_secs: Option<u64>,

    /// Emit structured JSON logs to stderr.
    #[arg(long, global = true)]
    pub log_json: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Collect live order books (default command).
    Collect,
    /// Replay a normalized JSONL fixture through the collector core.
    Replay {
        /// Fixture path, or `-` for stdin.
        #[arg(value_name = "JSONL")]
        input: PathBuf,
    },
    /// Inspect an aligned SBE capture.
    Inspect {
        /// Capture path.
        #[arg(value_name = "SBE")]
        input: PathBuf,
        /// Chunk size in bytes, or `auto`.
        #[arg(long = "read-chunk-size", default_value = "auto")]
        read_chunk_size: String,
        /// Number of decoded messages to include as samples.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Verify that a capture's deltas reconstruct the next snapshots exactly.
    Verify {
        /// Capture path.
        #[arg(value_name = "SBE")]
        input: PathBuf,
        /// Chunk size in bytes, or `auto`.
        #[arg(long = "read-chunk-size", default_value = "auto")]
        read_chunk_size: String,
    },
    /// Measure discovery RTT and websocket timestamp latency.
    Latency {
        /// Optional path for the JSON latency report.
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
    },
    /// Discover and print the current live market set as JSON.
    Discover {
        /// Optional path for the JSON discovery result.
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
    },
}

impl Cli {
    pub fn apply_settings(&self, settings: &mut Settings) -> anyhow::Result<()> {
        if let Some(exchange) = self.exchange {
            settings.exchange = exchange;
        }
        if let Some(environment) = self.kalshi_environment {
            settings.kalshi.environment = environment;
        }
        if let Some(key_id) = &self.kalshi_key_id {
            settings.kalshi.key_id = key_id.clone();
        }
        if let Some(path) = &self.kalshi_private_key {
            settings.kalshi.private_key_path = Some(path.clone());
        }
        if self.no_tcp_nodelay {
            settings.network.tcp_nodelay = false;
        }
        if let Some(source_ip) = self.source_ip {
            settings.network.source_ip = Some(source_ip);
        }
        if let Some(source_port) = self.source_port {
            settings.network.source_port = Some(source_port);
        }
        if let Some(connect_race) = self.connect_race {
            settings.network.connect_race = connect_race;
        }
        if let Some(output) = &self.output {
            settings.output.path = output.clone();
        }
        if let Some(chunk_size) = self.chunk_size {
            settings.output.chunk_size = chunk_size;
        }
        if let Some(shards) = self.shards {
            settings.collector.shards = shards;
        }
        if let Some(replicas) = self.replicas {
            settings.collector.replicas = replicas;
        }
        if let Some(listen) = self.metrics_listen {
            settings.metrics.listen = Some(listen);
        }
        if let Some(path) = &self.metadata {
            settings.output.metadata_path = Some(path.clone());
        }
        if let Some(path) = &self.report {
            settings.output.report_path = Some(path.clone());
        }
        if let Some(seconds) = self.snapshot_interval_secs {
            settings.collector.snapshot_interval_secs = seconds;
        }
        if let Some(seconds) = self.discovery_interval_secs {
            settings.collector.discovery_interval_secs = seconds;
        }
        if let Some(max_markets) = self.max_markets {
            settings.collector.max_markets = max_markets;
        }
        if let Some(duration_secs) = self.duration_secs {
            settings.collector.duration_secs = duration_secs;
        }
        settings
            .validate()
            .context("validating command-line overrides")?;
        Ok(())
    }
}
