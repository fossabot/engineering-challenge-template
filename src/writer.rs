//! Single serialized SBE writer thread.
//!
//! All exchange tasks produce normalized messages into a bounded channel. One
//! thread owns the aligned writer, preserving total ordering of writes and
//! making flush/shutdown deterministic.
//!
//! Shutdown is a state transition with a documented durability policy:
//! admission is closed (the queue's sender is dropped), the worker drains
//! every message it already accepted, flushes, syncs the file when
//! configured, and is joined before the handle reports a result. A full
//! queue can delay that transition but never silently abandon the worker.

use std::{
    fs::File,
    io::{BufWriter, Stdout},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use anyhow::Context;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::{
    metrics::{WriterCounters, WriterStats},
    model::OutputMessage,
    util::AlignedSbeWriter,
};

enum WriterCommand {
    Message(OutputMessage),
    Flush(oneshot::Sender<Result<(), String>>),
    Shutdown(oneshot::Sender<Result<(), String>>),
}

/// The byte sink owned by the writer thread. Splitting the sink behind a
/// trait keeps the worker's durability contract testable: fault injection
/// only needs another implementation, not a real filesystem failure.
trait MessageSink: Send + 'static {
    /// Append one encoded message. A `Err` return is fatal: the worker stops
    /// and the error is surfaced through the handle.
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()>;
    /// Make accepted messages visible to readers (padding the chunk).
    fn flush(&mut self) -> anyhow::Result<()>;
    /// Persist flushed bytes to the storage layer where the sink supports it.
    fn sync(&mut self) -> anyhow::Result<()>;
    fn bytes_written(&self) -> u64;
}

enum Sink {
    File(AlignedSbeWriter<BufWriter<File>>),
    Stdout(AlignedSbeWriter<BufWriter<Stdout>>),
}

impl Sink {
    fn open(path: &str, chunk_size: usize) -> anyhow::Result<Self> {
        if path == "-" {
            let writer = BufWriter::with_capacity(chunk_size, std::io::stdout());
            return Ok(Self::Stdout(AlignedSbeWriter::from_writer(
                writer, chunk_size,
            )?));
        }
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory `{}`", parent.display()))?;
        }
        let writer = AlignedSbeWriter::create(path, chunk_size)
            .with_context(|| format!("creating output `{path}`"))?;
        Ok(Self::File(writer))
    }
}

impl MessageSink for Sink {
    fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        match self {
            Self::File(writer) => match message {
                OutputMessage::Snapshot(snapshot) => writer.write_msg(snapshot),
                OutputMessage::PriceLevelUpdate(update) => writer.write_msg(update),
            },
            Self::Stdout(writer) => match message {
                OutputMessage::Snapshot(snapshot) => writer.write_msg(snapshot),
                OutputMessage::PriceLevelUpdate(update) => writer.write_msg(update),
            },
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Stdout(writer) => writer.flush(),
        }
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        if let Self::File(writer) = self {
            writer.sync_all()?;
        }
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        match self {
            Self::File(writer) => writer.bytes_written(),
            Self::Stdout(writer) => writer.bytes_written(),
        }
    }
}

/// Async handle used by the coordinator.
pub struct WriterHandle {
    sender: mpsc::Sender<WriterCommand>,
    counters: Arc<WriterCounters>,
    join: Option<std::thread::JoinHandle<()>>,
    output_path: String,
    fatal: Arc<Mutex<Option<String>>>,
}

impl WriterHandle {
    /// Start the writer thread. The output file is created immediately so an
    /// empty, valid file exists even if the process is interrupted during
    /// startup.
    pub fn start(
        path: &str,
        chunk_size: usize,
        queue: usize,
        sync_on_close: bool,
    ) -> anyhow::Result<Self> {
        let sink = Sink::open(path, chunk_size)?;
        Self::start_with_sink(Box::new(sink), queue, sync_on_close, path)
    }

    /// Start the writer thread over an injected sink. Production callers pass
    /// a file or stdout sink; tests pass fault-injecting sinks to exercise the
    /// durability contract.
    fn start_with_sink(
        sink: Box<dyn MessageSink>,
        queue: usize,
        sync_on_close: bool,
        path: &str,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(queue > 0, "writer queue must be positive");
        let counters = Arc::new(WriterCounters::default());
        let writer_counters = Arc::clone(&counters);
        let (sender, mut receiver) = mpsc::channel::<WriterCommand>(queue);
        let fatal = Arc::new(Mutex::new(None));
        let writer_fatal = Arc::clone(&fatal);
        let thread_name = "sbe-writer".to_owned();
        let join = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let mut sink = sink;
                let mut explicit_shutdown = false;
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        WriterCommand::Message(message) => {
                            writer_counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                            if let Err(error) = sink.write(&message) {
                                let text = format!("{error:#}");
                                error!(error = %text, "SBE writer failed");
                                set_fatal(&writer_fatal, &text);
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                // A failed write invalidates the sink; a final
                                // flush could mask the original error.
                                return;
                            }
                            writer_counters.messages.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                        }
                        WriterCommand::Flush(ack) => {
                            let result = sink.flush().map_err(|error| format!("{error:#}"));
                            if let Err(error) = &result {
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                set_fatal(&writer_fatal, error);
                            }
                            writer_counters.flushes.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                            let _ = ack.send(result);
                        }
                        WriterCommand::Shutdown(ack) => {
                            let mut result = sink.flush().map_err(|error| format!("{error:#}"));
                            if result.is_ok() && sync_on_close {
                                result = sink.sync().map_err(|error| format!("{error:#}"));
                            }
                            if let Err(error) = &result {
                                writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                                set_fatal(&writer_fatal, error);
                            }
                            writer_counters.flushes.fetch_add(1, Ordering::Relaxed);
                            writer_counters
                                .bytes
                                .store(sink.bytes_written(), Ordering::Relaxed);
                            let _ = ack.send(result);
                            explicit_shutdown = true;
                            break;
                        }
                    }
                }
                // The receiver returns `None` only when every sender was
                // dropped - for example when shutdown hit a full queue.
                // Closing admission is still a clean close: the queue is
                // empty by now, so drain-then-exit must uphold the same
                // durability contract as an explicit shutdown command.
                if !explicit_shutdown {
                    if let Err(error) = sink.flush() {
                        let text = format!("{error:#}");
                        error!(error = %text, "SBE writer final flush failed");
                        set_fatal(&writer_fatal, &text);
                        writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                    } else if sync_on_close && let Err(error) = sink.sync() {
                        let text = format!("{error:#}");
                        error!(error = %text, "SBE writer final sync failed");
                        set_fatal(&writer_fatal, &text);
                        writer_counters.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .context("spawning SBE writer thread")?;

        info!(path, "SBE writer started");
        Ok(Self {
            sender,
            counters,
            join: Some(join),
            output_path: if path == "-" {
                "stdout".to_owned()
            } else {
                PathBuf::from(path).display().to_string()
            },
            fatal,
        })
    }

    /// Send one message, waiting for queue capacity. A timeout prevents a
    /// wedged writer from deadlocking the whole collector forever.
    pub async fn send(&self, message: OutputMessage, timeout: Duration) -> anyhow::Result<()> {
        self.counters.queue_depth.fetch_add(1, Ordering::Relaxed);
        let send = self.sender.send(WriterCommand::Message(message));
        match tokio::time::timeout(timeout, send).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => {
                self.counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                anyhow::bail!("SBE writer stopped: {}", self.fatal_message())
            }
            Err(_) => {
                self.counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                anyhow::bail!("SBE writer queue full for {timeout:?}")
            }
        }
    }

    /// Flush the current partial chunk. This pads to the configured chunk size.
    pub async fn flush(&self, timeout: Duration) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        match tokio::time::timeout(timeout, self.sender.send(WriterCommand::Flush(tx))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                anyhow::bail!("SBE writer stopped: {}", self.fatal_message());
            }
            Err(_) => {
                anyhow::bail!("SBE writer flush enqueue timed out after {timeout:?}");
            }
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => anyhow::bail!("SBE writer flush failed: {error}"),
            Ok(Err(_)) => anyhow::bail!("SBE writer dropped flush acknowledgement"),
            Err(_) => anyhow::bail!("SBE writer flush timed out after {timeout:?}"),
        }
    }

    /// Flush, sync (for files), stop the writer thread, and return final
    /// counters. The counters are sampled after the worker terminates so a
    /// run report includes every accepted message.
    pub async fn shutdown_with_stats(self, timeout: Duration) -> anyhow::Result<WriterStats> {
        self.shutdown_inner(timeout).await
    }

    /// Flush, sync (for files), and stop the writer thread.
    pub async fn shutdown(self, timeout: Duration) -> anyhow::Result<()> {
        self.shutdown_with_stats(timeout).await.map(|_| ())
    }

    /// Durability-preserving shutdown.
    ///
    /// The transition has three bounded phases:
    ///
    /// 1. Deliver a `Shutdown` command (bounded by `timeout`). If the queue
    ///    is saturated the command is not delivered, but that must never
    ///    detach the worker.
    /// 2. Close admission by dropping the queue's sender. The worker then
    ///    drains every message it already accepted, flushes, syncs when
    ///    configured, and exits.
    /// 3. Join the worker (bounded by `timeout`). If it does not terminate
    ///    in time, return an explicit incomplete-durability error with exact
    ///    queued/written/error counts; the thread is detached because a
    ///    Rust thread cannot be killed, so the error is the cleanup policy.
    async fn shutdown_inner(self, timeout: Duration) -> anyhow::Result<WriterStats> {
        let Self {
            sender,
            counters,
            mut join,
            output_path: _,
            fatal,
        } = self;

        let (ack_tx, ack_rx) = oneshot::channel();
        let mut ack_result: Option<anyhow::Error> = None;
        let enqueue =
            tokio::time::timeout(timeout, sender.send(WriterCommand::Shutdown(ack_tx))).await;

        match enqueue {
            Ok(Ok(())) => match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(message))) => {
                    ack_result = Some(anyhow::anyhow!("SBE writer shutdown failed: {message}"));
                }
                Ok(Err(_)) => {
                    // The worker exited before answering (a fatal write error
                    // stopped it). The fatal slot records the cause.
                }
                Err(_) => {
                    // The worker is inside a long flush/sync. Closing
                    // admission lets it finish; the join bounds the wait.
                }
            },
            Ok(Err(_)) => {
                // The worker already stopped; the fatal slot records why.
            }
            Err(_) => {
                // Queue saturated: admission closes below and the worker
                // drains before exiting. This is exactly the backpressure
                // condition shutdown must survive.
            }
        }

        // Close admission. This is the only way to guarantee termination of
        // a worker whose queue is full: once every sender is dropped,
        // `blocking_recv` drains the queue and returns `None`.
        drop(sender);

        let joined = match join.take() {
            Some(handle) => {
                tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || handle.join()))
                    .await
            }
            None => Ok(Ok(Ok(()))),
        };

        let stats = counters.snapshot();
        let fatal_message = fatal
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "no writer error recorded".to_owned());

        let joined = match joined {
            Ok(Ok(joined)) => joined,
            Ok(Err(join_error)) => {
                return Err(anyhow::anyhow!(
                    "SBE writer join task failed: {join_error}; \
                     accepted={}, written={}, queued={}, errors={}",
                    stats.messages + stats.queue_depth,
                    stats.messages,
                    stats.queue_depth,
                    stats.errors
                ));
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "SBE writer did not terminate within {timeout:?}; \
                     durability is NOT guaranteed for the remaining messages; \
                     accepted={}, written={}, queued={}, errors={}, last_error={fatal_message}",
                    stats.messages + stats.queue_depth,
                    stats.messages,
                    stats.queue_depth,
                    stats.errors
                ));
            }
        };

        if let Err(panic) = joined {
            return Err(anyhow::anyhow!(
                "SBE writer thread panicked: {panic:?}; \
                 accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            ));
        }

        if let Some(error) = ack_result {
            return Err(error.context(format!(
                "accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            )));
        }
        if fatal_message != "no writer error recorded" {
            return Err(anyhow::anyhow!(
                "SBE writer stopped: {fatal_message}; \
                 accepted={}, written={}, queued={}, errors={}",
                stats.messages + stats.queue_depth,
                stats.messages,
                stats.queue_depth,
                stats.errors
            ));
        }
        Ok(stats)
    }

    #[must_use]
    pub fn stats(&self) -> WriterStats {
        self.counters.snapshot()
    }

    #[must_use]
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    #[must_use]
    pub fn fatal_message(&self) -> String {
        self.fatal
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "no writer error recorded".to_owned())
    }
}

fn set_fatal(slot: &Arc<Mutex<Option<String>>>, error: &str) {
    if let Ok(mut slot) = slot.lock()
        && slot.is_none()
    {
        *slot = Some(error.to_owned());
    }
}

/// Convenience used by replay to write a finite stream synchronously.
pub struct DirectWriter {
    sink: Sink,
    counters: WriterCounters,
    sync_on_close: bool,
}

impl DirectWriter {
    pub fn create(path: &str, chunk_size: usize, sync_on_close: bool) -> anyhow::Result<Self> {
        Ok(Self {
            sink: Sink::open(path, chunk_size)?,
            counters: WriterCounters::default(),
            sync_on_close,
        })
    }

    pub fn write(&mut self, message: &OutputMessage) -> anyhow::Result<()> {
        MessageSink::write(&mut self.sink, message)?;
        self.counters.messages.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        MessageSink::flush(&mut self.sink)?;
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        Ok(())
    }

    pub fn shutdown(mut self) -> anyhow::Result<WriterStats> {
        MessageSink::flush(&mut self.sink)?;
        if self.sync_on_close {
            MessageSink::sync(&mut self.sink)?;
        }
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .store(MessageSink::bytes_written(&self.sink), Ordering::Relaxed);
        Ok(self.counters.snapshot())
    }

    #[must_use]
    pub fn stats(&self) -> WriterStats {
        self.counters.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    const TINY_TIMEOUT: Duration = Duration::from_millis(100);
    const PATIENT_TIMEOUT: Duration = Duration::from_secs(10);

    fn update(symbol: &str) -> OutputMessage {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: 500_000,
            quantity: 1_000_000,
            side: Side::Buy,
            symbol: symbol.to_owned(),
        })
    }

    /// Sink whose failures are injected at exact points.
    struct FaultySink {
        fail_write_at: Option<usize>,
        fail_flush: bool,
        fail_sync: bool,
        writes: usize,
    }

    impl FaultySink {
        fn healthy() -> Self {
            Self {
                fail_write_at: None,
                fail_flush: false,
                fail_sync: false,
                writes: 0,
            }
        }
    }

    impl MessageSink for FaultySink {
        fn write(&mut self, _message: &OutputMessage) -> anyhow::Result<()> {
            self.writes += 1;
            if Some(self.writes) == self.fail_write_at {
                anyhow::bail!("injected write failure");
            }
            Ok(())
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            if self.fail_flush {
                anyhow::bail!("injected flush failure");
            }
            Ok(())
        }

        fn sync(&mut self) -> anyhow::Result<()> {
            if self.fail_sync {
                anyhow::bail!("injected sync failure");
            }
            Ok(())
        }

        fn bytes_written(&self) -> u64 {
            0
        }
    }

    async fn wait_for_fatal(writer: &WriterHandle) {
        for _ in 0..200 {
            if writer.fatal_message() != "no writer error recorded" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn normal_shutdown_drains_and_joins() {
        let writer =
            WriterHandle::start_with_sink(Box::new(FaultySink::healthy()), 16, true, "unused")
                .unwrap();
        for i in 0..4 {
            writer
                .send(update(&format!("sym-{i}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.messages, 4);
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.errors, 0);
    }

    #[tokio::test]
    async fn write_failure_surfaces_and_worker_joins() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_write_at: Some(1),
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        // The first send may still be accepted (queued); the worker fails on
        // it asynchronously.
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        wait_for_fatal(&writer).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            error.to_string().contains("injected write failure"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn flush_failure_on_shutdown_is_reported() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_flush: true,
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("injected flush failure"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn sync_failure_on_shutdown_is_reported() {
        let writer = WriterHandle::start_with_sink(
            Box::new(FaultySink {
                fail_sync: true,
                ..FaultySink::healthy()
            }),
            16,
            true,
            "unused",
        )
        .unwrap();
        let _ = writer.send(update("sym"), TINY_TIMEOUT).await;
        let error = writer.shutdown(PATIENT_TIMEOUT).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("injected sync failure"),
            "unexpected error: {error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_drains_saturated_queue_and_joins() {
        // A FIFO whose reader deliberately stalls blocks the worker's writes
        // once the pipe buffer fills, which saturates the bounded queue. The
        // old implementation returned an error immediately and dropped the
        // worker without joining; the contract now is: close admission, let
        // the reader drain, join the worker, and confirm every accepted
        // message was written.
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("blocked.sbe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let reader_path = fifo.clone();
        let reader = tokio::task::spawn_blocking(move || {
            // Opening the read end blocks until the writer opens the FIFO.
            let mut file = std::fs::File::open(&reader_path).unwrap();
            std::thread::sleep(Duration::from_millis(400));
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            bytes.len()
        });
        // Give the reader thread a moment to reach open().
        tokio::time::sleep(Duration::from_millis(50)).await;

        let writer = WriterHandle::start(
            fifo.to_str().unwrap(),
            1024,
            128,
            true, // sync on close; the drain path must sync too
        )
        .unwrap();

        // Fill the queue: keep sending until the bounded queue rejects.
        let mut accepted = 0usize;
        for i in 0..20_000 {
            match writer.send(update(&format!("sym-{i}")), TINY_TIMEOUT).await {
                Ok(()) => accepted += 1,
                Err(_) => break,
            }
        }
        assert!(
            accepted > 128,
            "expected the queue to saturate (accepted {accepted})"
        );

        // The reader wakes up around now and drains the pipe, so the worker
        // must finish draining, flush, sync, and exit within the budget.
        let stats = writer.shutdown_with_stats(PATIENT_TIMEOUT).await.unwrap();
        assert_eq!(stats.queue_depth, 0, "every accepted message must drain");
        assert_eq!(stats.messages as usize, accepted);
        assert_eq!(stats.errors, 0);
        let read_bytes = reader.await.unwrap();
        assert!(
            read_bytes >= accepted,
            "reader saw {read_bytes} bytes for {accepted} messages"
        );
    }

    #[tokio::test]
    async fn flush_makes_output_visible_to_reader() {
        // Output-visibility contract: an accepted message is invisible to an
        // independent reader until the next flush completes, and visible
        // immediately after it. The coordinator flushes every
        // `flush_interval_secs`, which bounds event-to-visible-output delay.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("visible.sbe");
        let path = path.to_str().unwrap();
        let writer = WriterHandle::start(path, 4096, 16, false).unwrap();
        for i in 0..3 {
            writer
                .send(update(&format!("sym-{i}")), TINY_TIMEOUT)
                .await
                .unwrap();
        }
        // Give the worker a moment to consume the queue into its chunk
        // buffer, then confirm nothing has reached the file yet.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);

        writer.flush(PATIENT_TIMEOUT).await.unwrap();
        let file_len = std::fs::metadata(path).unwrap().len();
        assert!(file_len > 0, "flushed chunk must be visible on disk");

        let mut reader =
            AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(path, 4096).unwrap();
        let mut decoded = 0;
        while reader.read_msg().unwrap().is_some() {
            decoded += 1;
        }
        assert_eq!(decoded, 3);
        writer.shutdown(PATIENT_TIMEOUT).await.unwrap();
    }
}
