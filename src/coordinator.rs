//! Live collector coordinator.
//!
//! One serialized writer owns the SBE file. Every websocket source is a worker
//! task supervised by [`crate::session::run_worker`]; this module feeds decoded
//! events into the synchronous [`CollectorCore`], dispatches subscription and
//! recovery controls, runs discovery, schedules periodic snapshots, and handles
//! graceful shutdown.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use futures_util::FutureExt as _;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout, timeout_at},
};
use tracing::{error, info, warn};

use crate::{
    collector::{CollectorCore, CoreConfig},
    config::{ExchangeKind, Settings},
    exchange::{Exchange, KalshiExchange, PolymarketExchange},
    metrics::{MetricsRegistry, run_server_on},
    model::{Control, MarketInfo, OutputMessage, SourceId},
    report::{MarketMetadata, ReportConfig, RunReport, sidecar_path},
    session::{WorkerConfig, WorkerEvent, run_worker},
    time::now_us,
    writer::WriterHandle,
};

/// Build the selected exchange adapter. `require_auth` is true for live
/// collection and false for public discovery-only commands.
pub fn build_exchange(
    settings: &Settings,
    require_auth: bool,
) -> anyhow::Result<Arc<dyn Exchange>> {
    match settings.exchange {
        ExchangeKind::Kalshi => Ok(Arc::new(KalshiExchange::new(
            settings.kalshi.clone(),
            settings.network.clone(),
            require_auth,
        )?)),
        ExchangeKind::Polymarket => Ok(Arc::new(PolymarketExchange::new(
            settings.polymarket.clone(),
            settings.network.clone(),
        )?)),
    }
}

/// Run a live collection until SIGINT/SIGTERM, the configured duration, or a
/// fatal source/writer error.
pub async fn run(settings: Settings) -> anyhow::Result<RunReport> {
    settings.validate()?;
    let started_at_us = now_us().context("reading wall clock at startup")?;
    let exchange = build_exchange(&settings, true)?;
    let initial_markets =
        discover_with_retry(&exchange, settings.collector.max_markets, exchange.name()).await?;
    if initial_markets.is_empty() {
        bail!(
            "{} returned no live, non-MVE markets; refusing to start an empty collector",
            exchange.name()
        );
    }
    let shard_count = derive_shard_count(&settings, initial_markets.len());
    let replica_count = settings.collector.replicas;
    let core_config = CoreConfig::from_settings(&settings, shard_count)?;
    let mut core = CollectorCore::new(core_config)?;
    // The initial discovery cannot produce removal snapshots, but applying it
    // here establishes shard assignments before workers are spawned.
    core.apply_discovery(started_at_us, initial_markets.clone())
        .context("applying initial market discovery")?;

    if let Some(path) = sidecar_path(&settings.output.metadata_path) {
        MarketMetadata::new(exchange.name(), core.market_infos())
            .write(path)
            .context("writing initial market metadata sidecar")?;
    }
    let writer = WriterHandle::start(
        &settings.output.path,
        settings.output.chunk_size,
        settings.collector.writer_queue,
        settings.output.sync_on_close,
    )?;
    let send_timeout = Duration::from_secs(settings.collector.writer_send_timeout_secs.max(1));
    let registry = MetricsRegistry::new(exchange.name());
    let (metrics_shutdown, metrics_handle) = start_metrics(&settings, &registry).await?;

    let factory = Arc::clone(&exchange).session_factory();
    let worker_config = WorkerConfig::from_settings(&settings.collector);
    let (worker_event_tx, mut worker_event_rx) = mpsc::channel::<WorkerEvent>(8192);
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut commands: BTreeMap<SourceId, mpsc::Sender<Control>> = BTreeMap::new();
    let mut handles: BTreeMap<SourceId, JoinHandle<()>> = BTreeMap::new();
    let mut active_sources = BTreeSet::new();
    for shard in 0..shard_count {
        for replica in 0..replica_count {
            let source = SourceId::new(shard as u16, replica as u16);
            let (command_tx, command_rx) = mpsc::channel::<Control>(256);
            commands.insert(source, command_tx);
            active_sources.insert(source);
            let handle = tokio::spawn(run_worker(
                Arc::clone(&factory),
                source,
                command_rx,
                worker_event_tx.clone(),
                worker_config.clone(),
                shutdown_tx.subscribe(),
            ));
            handles.insert(source, handle);
        }
    }
    drop(worker_event_tx);

    dispatch_controls(&mut core, &commands, send_timeout).await?;
    update_metrics(&registry, &core, &writer);
    info!(
        exchange = exchange.name(),
        environment = exchange.environment(),
        symbols = core.symbol_count(),
        shards = shard_count,
        replicas = replica_count,
        output = %settings.output.path,
        metadata = ?settings.output.metadata_path,
        report = ?settings.output.report_path,
        "collector started"
    );

    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut discovery_ticker = interval(settings.discovery_interval());
    discovery_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The initial discovery already ran; consume the immediate interval tick.
    discovery_ticker.tick().await;
    let mut flush_ticker = interval(Duration::from_secs(
        settings.collector.flush_interval_secs.max(1),
    ));
    flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    flush_ticker.tick().await;
    let (discovery_tx, mut discovery_rx) = mpsc::channel::<anyhow::Result<Vec<MarketInfo>>>(1);
    let mut discovery_in_flight = false;

    let signal = wait_for_shutdown_signal();
    tokio::pin!(signal);
    let duration_secs = settings.collector.duration_secs;
    let duration_sleep = async move {
        if duration_secs == 0 {
            std::future::pending::<()>().await;
        } else {
            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
        }
    };
    tokio::pin!(duration_sleep);

    let mut shutdown_reason = String::new();
    let mut fatal: Option<String> = None;
    loop {
        tokio::select! {
            reason = &mut signal => {
                shutdown_reason = reason;
                break;
            }
            _ = &mut duration_sleep => {
                shutdown_reason = format!("duration elapsed ({duration_secs}s)");
                break;
            }
            event = worker_event_rx.recv() => {
                match event {
                    Some(event) => {
                        if let Err(error) = process_worker_event(
                            event,
                            &mut core,
                            &writer,
                            &commands,
                            &mut active_sources,
                            send_timeout,
                        ).await {
                            fatal = Some(format!("{error:#}"));
                            break;
                        }
                        if active_sources.is_empty() {
                            fatal = Some("all websocket sources stopped".to_owned());
                            break;
                        }
                    }
                    None => {
                        fatal = Some("worker event channel closed unexpectedly".to_owned());
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                let now = now_us().context("reading wall clock for collector tick")?;
                let outputs = core.tick(now);
                if let Err(error) = emit_outputs(&writer, outputs, send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                if let Err(error) = dispatch_controls(&mut core, &commands, send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                update_metrics(&registry, &core, &writer);
            }
            _ = discovery_ticker.tick() => {
                // Run REST discovery off the event loop. Paginating thousands
                // of markets while holding this loop would stop draining
                // websocket events and can cause avoidable gaps.
                if !discovery_in_flight {
                    let exchange = Arc::clone(&exchange);
                    let max_markets = settings.collector.max_markets;
                    let result_tx = discovery_tx.clone();
                    discovery_in_flight = true;
                    tokio::spawn(async move {
                        let result = std::panic::AssertUnwindSafe(
                            exchange.discover_markets(max_markets),
                        )
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| {
                            Err(anyhow::anyhow!(
                                "market discovery task panicked"
                            ))
                        });
                        let _ = result_tx.send(result).await;
                    });
                }
            }
            result = discovery_rx.recv() => {
                discovery_in_flight = false;
                match result {
                    Some(Ok(markets)) if markets.is_empty() && core.symbol_count() > 0 => {
                        warn!(
                            current = core.symbol_count(),
                            "discovery returned no markets; keeping the current subscription set"
                        );
                    }
                    Some(Ok(markets)) => {
                        let now = now_us().context("reading wall clock for discovery")?;
                        match core.apply_discovery(now, markets) {
                            Ok(outputs) => {
                                if let Err(error) =
                                    emit_outputs(&writer, outputs, send_timeout).await
                                {
                                    fatal = Some(format!("{error:#}"));
                                    break;
                                }
                                if let Err(error) =
                                    dispatch_controls(&mut core, &commands, send_timeout).await
                                {
                                    fatal = Some(format!("{error:#}"));
                                    break;
                                }
                                if let Some(path) = sidecar_path(&settings.output.metadata_path)
                                    && let Err(error) = MarketMetadata::new(
                                        exchange.name(),
                                        core.market_infos(),
                                    )
                                    .write(path)
                                {
                                    warn!(
                                        error = %format!("{error:#}"),
                                        "failed to update metadata sidecar"
                                    );
                                }
                                update_metrics(&registry, &core, &writer);
                            }
                            Err(error) => {
                                warn!(
                                    error = %format!("{error:#}"),
                                    "invalid discovery result; retaining current subscriptions"
                                );
                            }
                        }
                    }
                    Some(Err(error)) => {
                        warn!(error = %format!("{error:#}"), "market discovery failed; retaining current subscriptions");
                    }
                    None => {
                        warn!("market discovery result channel closed; continuing");
                    }
                }
            }
            _ = flush_ticker.tick() => {
                if let Err(error) = writer.flush(send_timeout).await {
                    fatal = Some(format!("{error:#}"));
                    break;
                }
                update_metrics(&registry, &core, &writer);
            }
        }
    }

    if let Some(error) = fatal {
        shutdown_reason = format!("fatal: {error}");
        error!(error, "collector stopping after fatal error");
    }
    info!(reason = %shutdown_reason, "shutting down collector");
    let ended_at_us = now_us().unwrap_or(started_at_us);
    core.begin_shutdown();
    let mut shutdown_error: Option<anyhow::Error> = None;
    if settings.collector.final_snapshots {
        let outputs = core.finalize(ended_at_us);
        if let Err(error) = emit_outputs(&writer, outputs, send_timeout).await {
            shutdown_error = Some(error);
        }
    }
    // Broadcast first: workers blocked in a handshake or backoff still observe
    // shutdown. The per-source command is best-effort because a full queue must
    // never delay process shutdown.
    let _ = shutdown_tx.send(());
    for command in commands.values() {
        let _ = command.try_send(Control::Shutdown);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !active_sources.is_empty() && tokio::time::Instant::now() < deadline {
        match timeout_at(deadline, worker_event_rx.recv()).await {
            Ok(Some(WorkerEvent::Stopped { source })) => {
                active_sources.remove(&source);
            }
            Ok(Some(WorkerEvent::Status(status))) => {
                // Preserve final connectivity/counter state for the run
                // report, but suppress any post-final data output.
                let _ = core.handle_status(status);
            }
            Ok(Some(WorkerEvent::Events { .. } | WorkerEvent::Fatal { .. })) => {
                // Drop queued data after the final snapshot. Workers are
                // closing and must not block on the event channel.
            }
            Ok(None) | Err(_) => break,
        }
    }
    for handle in handles.into_values() {
        handle.abort();
    }
    if let Err(error) = writer.flush(send_timeout).await {
        shutdown_error.get_or_insert(error);
    }
    let stats_before_shutdown = writer.stats();
    let writer_stats = match writer.shutdown_with_stats(send_timeout).await {
        Ok(stats) => stats,
        Err(error) => {
            shutdown_error.get_or_insert(error);
            stats_before_shutdown
        }
    };
    stop_metrics(metrics_shutdown, metrics_handle).await;
    let core_stats = core.stats();
    let sources: Vec<_> = core.statuses().values().cloned().collect();
    let report = RunReport::new(
        "live",
        exchange.name(),
        exchange.environment(),
        started_at_us,
        ended_at_us,
        &shutdown_reason,
        &settings.output.path,
        shard_count,
        replica_count,
        core_stats.discovered_symbols as usize,
        core.symbol_count(),
        core_stats,
        writer_stats,
        sources,
        ReportConfig::from_settings(&settings, shard_count),
    );
    if let Some(path) = sidecar_path(&settings.output.metadata_path) {
        MarketMetadata::new(exchange.name(), core.market_infos())
            .write(path)
            .context("writing final market metadata sidecar")?;
    }
    if let Some(path) = sidecar_path(&settings.output.report_path) {
        report.write(path).context("writing run report")?;
    }
    if let Some(error) = shutdown_error {
        return Err(error);
    }
    Ok(report)
}

fn derive_shard_count(settings: &Settings, symbol_count: usize) -> usize {
    if settings.collector.shards > 0 {
        return settings.collector.shards.clamp(1, 256);
    }
    let per_socket = settings.collector.max_symbols_per_socket.max(1);
    let needed = symbol_count.div_ceil(per_socket).max(1);
    needed
        .min(settings.collector.max_shards.max(1))
        .clamp(1, 256)
}

async fn discover_with_retry(
    exchange: &Arc<dyn Exchange>,
    max_markets: usize,
    name: &str,
) -> anyhow::Result<Vec<MarketInfo>> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 0..5u32 {
        match exchange.discover_markets(max_markets).await {
            Ok(markets) => return Ok(markets),
            Err(error) => {
                warn!(
                    exchange = name,
                    attempt = attempt + 1,
                    error = %format!("{error:#}"),
                    "initial market discovery failed"
                );
                last_error = Some(error);
                if attempt < 4 {
                    let delay = 500u64.saturating_mul(1u64 << attempt);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("initial market discovery failed")))
}

async fn start_metrics(
    settings: &Settings,
    registry: &MetricsRegistry,
) -> anyhow::Result<(
    Option<broadcast::Sender<()>>,
    Option<JoinHandle<anyhow::Result<()>>>,
)> {
    let Some(address) = settings.metrics.listen else {
        return Ok((None, None));
    };
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("binding metrics listener {address}"))?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let registry = registry.clone();
    let handle = tokio::spawn(async move { run_server_on(listener, registry, shutdown_rx).await });
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn stop_metrics(
    shutdown: Option<broadcast::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
) {
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(());
    }
    if let Some(handle) = handle {
        match timeout(Duration::from_secs(2), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!(error = %format!("{error:#}"), "metrics server failed"),
            Ok(Err(error)) => warn!(error = %error, "metrics server task failed"),
            Err(_) => warn!("metrics server did not stop within two seconds"),
        }
    }
}

async fn process_worker_event(
    event: WorkerEvent,
    core: &mut CollectorCore,
    writer: &WriterHandle,
    commands: &BTreeMap<SourceId, mpsc::Sender<Control>>,
    active_sources: &mut BTreeSet<SourceId>,
    send_timeout: Duration,
) -> anyhow::Result<()> {
    match event {
        WorkerEvent::Events { source, events } => {
            for event in events {
                let outputs = core.handle_event(source, event);
                emit_outputs(writer, outputs, send_timeout).await?;
            }
            dispatch_controls(core, commands, send_timeout).await?;
        }
        WorkerEvent::Status(status) => {
            let outputs = core.handle_status(status);
            emit_outputs(writer, outputs, send_timeout).await?;
            dispatch_controls(core, commands, send_timeout).await?;
        }
        WorkerEvent::Fatal { source, error } => {
            bail!("source {source} reported a fatal error: {error}");
        }
        WorkerEvent::Stopped { source } => {
            active_sources.remove(&source);
        }
    }
    Ok(())
}

async fn emit_outputs(
    writer: &WriterHandle,
    outputs: Vec<OutputMessage>,
    timeout: Duration,
) -> anyhow::Result<()> {
    for output in outputs {
        writer.send(output, timeout).await?;
    }
    Ok(())
}

async fn dispatch_controls(
    core: &mut CollectorCore,
    commands: &BTreeMap<SourceId, mpsc::Sender<Control>>,
    timeout_duration: Duration,
) -> anyhow::Result<()> {
    for (source, control) in core.drain_controls() {
        let Some(sender) = commands.get(&source) else {
            bail!("no command channel for source {source}");
        };
        match timeout(timeout_duration, sender.send(control)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                bail!("source {source} command channel closed: {error}");
            }
            Err(_) => {
                bail!(
                    "source {source} command channel was backpressured for {} seconds",
                    timeout_duration.as_secs()
                );
            }
        }
    }
    Ok(())
}

fn update_metrics(registry: &MetricsRegistry, core: &CollectorCore, writer: &WriterHandle) {
    registry.update(core.stats(), writer.stats(), core.statuses());
}

async fn wait_for_shutdown_signal() -> String {
    #[cfg(unix)]
    {
        let mut terminate = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(signal) => signal,
            Err(error) => {
                warn!(error = %error, "could not install SIGTERM handler; only SIGINT will be handled");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT".to_owned();
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT".to_owned(),
            _ = terminate.recv() => "SIGTERM".to_owned(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT".to_owned()
    }
}
