//! Collector binary entry point.

mod cli;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use engineering_challenge::{
    config::Settings, coordinator, inspect, latency, replay, report::write_json_atomic, verify,
};

use crate::cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_json)?;
    let mut settings = Settings::load(cli.config.as_deref())?;
    cli.apply_settings(&mut settings)?;
    match cli.command.unwrap_or(Command::Collect) {
        Command::Collect => {
            let report = coordinator::run(settings).await?;
            eprintln!(
                "collector stopped: {} SBE messages, {} bytes, {} symbols, reason={}",
                report.writer.messages,
                report.writer.bytes,
                report.final_symbols,
                report.shutdown_reason
            );
            Ok(())
        }
        Command::Replay { input } => {
            let report = replay::run(settings, &input)?;
            eprintln!(
                "replay complete: {} SBE messages, {} bytes, {} symbols",
                report.writer.messages, report.writer.bytes, report.final_symbols
            );
            Ok(())
        }
        Command::Inspect {
            input,
            read_chunk_size,
            limit,
        } => {
            let summary = inspect::run(&input, &read_chunk_size, limit)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            Ok(())
        }
        Command::Verify {
            input,
            read_chunk_size,
        } => {
            let summary = verify::run(&input, &read_chunk_size)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
            anyhow::ensure!(
                summary.valid,
                "capture verification failed: {} unknown message(s), {} delta(s) before a \
                 snapshot, {} snapshot mismatch(es)",
                summary.unknown_messages,
                summary.deltas_before_snapshot,
                summary.snapshot_mismatches
            );
            Ok(())
        }
        Command::Latency { out } => {
            let max_markets = if settings.collector.max_markets == 0 {
                10
            } else {
                settings.collector.max_markets
            };
            let duration_secs = if settings.collector.duration_secs == 0 {
                30
            } else {
                settings.collector.duration_secs
            };
            let report = latency::run(settings, max_markets, duration_secs, out.as_deref()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::Discover { out } => {
            let exchange = coordinator::build_exchange(&settings, false)?;
            let markets = exchange
                .discover_markets(settings.collector.max_markets)
                .await
                .with_context(|| format!("discovering {} markets", exchange.name()))?;
            let result = serde_json::json!({
                "schema_version": 1,
                "exchange": exchange.name(),
                "environment": exchange.environment(),
                "count": markets.len(),
                "markets": markets,
            });
            if let Some(path) = out.as_deref() {
                write_json_atomic(path, &result).context("writing discovery output")?;
            }
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
    }
}

fn init_tracing(json: bool) -> anyhow::Result<()> {
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter())
            .with_target(false)
            .with_writer(std::io::stderr)
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("initializing JSON tracing subscriber: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter())
            .with_target(false)
            .with_writer(std::io::stderr)
            .try_init()
            .map_err(|error| anyhow::anyhow!("initializing tracing subscriber: {error}"))?;
    }
    Ok(())
}
