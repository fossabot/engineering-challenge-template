# Prediction-market order-book collector

A Rust collector for live prediction-market order books. It writes the supplied
FIX SBE schema, maintains a book per symbol, discovers markets continuously, and
load-balances symbols across redundant websocket sources.

Kalshi is the primary exchange adapter. Polymarket's public CLOB market channel
is included as a working fallback and as a public, credential-free way to
validate the transport, sharding, snapshot, replay, and inspection paths.

## What it does

- Discovers live, non-MVE Kalshi markets and subscribes to every selected ticker.
- Reconciles discovery every 30 seconds by default, adding and removing symbols
  without restarting the process.
- Assigns symbols to a fixed number of shards at startup, then keeps the
  assignment stable. If `collector.shards = 0`, the initial shard count is
  derived from the symbol count and `max_symbols_per_socket`.
- Opens `collector.replicas` websocket connections per shard. Every replica
  maintains a full shadow book; one source is active per symbol.
- Writes a full snapshot every 60 seconds and on failover, recovery, removal,
  and shutdown. Deltas are written as they arrive.
- Recovers from sequence gaps by reconnecting/resubscribing and requesting fresh
  snapshots. A backup source is promoted only when its book is valid and at
  least as new as the last emitted clock (when the exchange supplies one).
  Kalshi clock regressions are treated as maintenance epochs; a still-valid
  lower-clock shadow is reconnected instead of being promoted.
- Handles SIGINT and SIGTERM: stops discovery, emits final snapshots, closes
  websockets, flushes and syncs the SBE file, and writes the sidecars.
- Exposes dependency-free Prometheus text metrics and `/healthz` when configured.
- Provides deterministic JSONL replay and an offline SBE inspector.

## Build and test

Rust is pinned in `rust-toolchain.toml` (1.99.1).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
```

The offline test suite covers decoding, fixed-point arithmetic, order books,
sharding/failover/snapshot scheduling, maintenance clock resets, malformed
discovery rejection, SBE sentinel validation, and an end-to-end replay fixture.
The public Polymarket network test is opt-in and ignored by default:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture
```

## Quick start: Kalshi

Kalshi websocket order-book subscriptions require an API key and a private key.
The collector supports Ed25519 PKCS#8, RSA PKCS#8, and RSA PKCS#1 PEM keys.
Credentials are read from the TOML file, environment, or CLI; no secrets are
written to reports or logs. The built-in environment default is `demo`;
`production` must be selected explicitly. Startup logs state the exchange,
environment, output, metadata, and report destinations.

```bash
export KALSHI_API_KEY_ID="your-key-id"
export KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/kalshi-private-key.pem"

# Demo is useful for validating the authenticated path.
cargo run --release -- \
  --exchange kalshi \
  --kalshi-environment demo \
  --output captures/kalshi-demo.sbe \
  --metadata captures/kalshi-demo-markets.json \
  --report captures/kalshi-demo-report.json \
  collect
```

For production, use `--kalshi-environment production` and the production key.
`KALSHI_PRIVATE_KEY_PEM` can be used instead of a path when a secret manager
injects the PEM directly.

Discovery-only access does not need credentials:

```bash
cargo run --release -- --exchange kalshi --kalshi-environment demo \
  --max-markets 10 discover
```

### Kalshi mapping

- Subscribes to `orderbook_delta`; `orderbook_snapshot` establishes the book
  and `orderbook_delta` applies changes.
- `market_lifecycle_v2` is subscribed once per connection for created,
  activated, deactivated, close-date, determined, settled, and metadata events.
- YES bids map to `Side::Buy`.
- NO bids map to YES asks at `1.00 - no_price` and `Side::Sell`.
- Modern `*_dollars_fp` fields and legacy cent/contract fields are both decoded.
- Per-subscription `seq` is validated. A gap drops the socket and resubscribes.
- `update_subscription` is used for add/delete and `get_snapshot` for recovery.
- `price_ranges` is consumed as the authoritative fixed-point grid; every band
  and its step are retained. `price_level_structure` is stored as a label only.
- `tick_size_change` / `price_level_structure_updated` lifecycle events update
  the in-memory and sidecar tick metadata. The supplied SBE schema is unchanged.

## Quick start: Polymarket public fallback

No credentials are required.

```bash
cargo run --release -- \
  --exchange polymarket \
  --max-markets 20 \
  --output captures/polymarket.sbe \
  --metadata captures/polymarket-markets.json \
  --report captures/polymarket-report.json \
  collect
```

Polymarket uses Gamma for discovery and the public CLOB market channel for data.
`book` events are snapshots; `price_change` events carry absolute per-level
sizes. There is no exchange sequence number, so protocol faults are recovered by
reconnecting and resubscribing, which yields fresh books.

## Commands

```text
collect    Live collection (default)
replay     Replay a normalized JSONL fixture through the same core
inspect    Decode an aligned SBE capture and print a JSON summary
latency    Measure discovery RTT and exchange-to-local timestamp latency
discover   Print the current live market set as JSON
```

Useful global flags include `--config`, `--output`, `--chunk-size`, `--shards`,
`--replicas`, `--snapshot-interval-secs`, `--discovery-interval-secs`,
`--max-markets`, `--duration-secs`, `--metrics-listen`, `--metadata`,
`--report`, and `--log-json`.

Examples:

```bash
# 60-second smoke run with a 16 KiB aligned chunk
cargo run --release -- --exchange polymarket --max-markets 10 \
  --duration-secs 60 --chunk-size 16384 --output /tmp/poly.sbe collect

# Inspect a capture; auto-detection is convenient for ad-hoc files
cargo run --release -- inspect /tmp/poly.sbe --read-chunk-size auto --limit 3

# Replay the checked-in deterministic fixture
cargo run --release -- --output /tmp/replay.sbe replay fixtures/sample_replay.jsonl

# One measured public-feed latency experiment
cargo run --release -- --exchange polymarket --max-markets 3 \
  --duration-secs 15 latency --out /tmp/polymarket-latency.json
```

## Architecture

```text
Exchange discovery ──► CollectorCore ──► serialized SBE writer
       │                  ▲   │
       │                  │   └── subscription / snapshot / reconnect controls
       ▼                  │
  source workers ─────────┘
  (one per shard replica)
```

- `src/exchange/` contains exchange-specific discovery, authentication, wire
  decoding, and `MarketDataSession` implementations.
- `src/session.rs` is the generic worker supervisor: connect, reconcile
  subscriptions, read/decode frames, heartbeat, periodic refresh, idle timeout,
  reconnect backoff, and status reporting.
- `src/collector/state.rs` is a synchronous, deterministic state machine. It
  owns shard assignment, per-source shadow books, active-source selection,
  sequence-reset handling, snapshot scheduling, removal snapshots, and stats.
  It has no network or wall-clock dependency; replay drives it with fixture
  timestamps.
- `src/coordinator.rs` owns live discovery, worker spawning, the event loop,
  signal handling, final snapshots, and sidecar/report writing.
- `src/writer.rs` owns exactly one writer thread. All SBE messages go through a
  bounded channel, preserving total write order.
- `src/util/aligned_sbe_writer.rs` and `aligned_sbe_reader.rs` implement the
  supplied aligned-chunk format. Messages never cross a chunk boundary; unused
  tail space is zero-padded.

### Sharding and redundancy

The shard count is fixed for the lifetime of a run. New symbols are assigned to
the least-loaded shard, so discovery churn does not move existing symbols. Every
shard has `replicas` websocket connections; all replicas receive the same symbol
set and maintain independent shadow books.

One source per symbol is active. A replica can take over only when its book is
valid and, if clocks are available, not older than the last emitted state. This
prevents a stale backup from silently regressing the output. If no backup is
ready, the core requests fresh snapshots and waits.

### Snapshot policy

- First exchange snapshot for a symbol is written immediately.
- Periodic local snapshots are scheduled every 60 seconds, spread
  deterministically across the interval using a stable symbol hash.
- Failover and recovery write a fresh full snapshot before deltas resume.
- Removed symbols receive an empty snapshot if they had been emitted.
- Graceful shutdown writes one final snapshot per live active book.

### Units and timestamps

The SBE schema uses millionths:

- price `1_000_000` = $1.00
- quantity `1_000_000` = one contract/share

`received_at` / `backtest_timestamp` is the local wall-clock time captured
immediately after a websocket frame is read and before decoding. The
`exchange_timestamp` is the best exchange-provided timestamp: Kalshi
`sending_ts_ms` / `ts_ms`, or Polymarket `timestamp`. When no exchange timestamp
exists, the local receive time is used.

## Output

The output file is an aligned SBE stream, 64 KiB chunks by default. It is safe
to inspect with:

```bash
cargo run --release -- inspect captures/polymarket.sbe --read-chunk-size auto
```

Two optional JSON sidecars are written atomically:

- `--metadata`: current market set, namespaced SBE symbol, native exchange id,
  title/outcome, close time, tick size, full `price_ranges`, and the
  human-readable `price_level_structure` when supplied.
- `--report`: run mode, environment, start/end timestamps, shutdown reason,
  non-secret configuration, core counters, writer counters, and final source
  status.

## Metrics

```bash
cargo run --release -- --metrics-listen 127.0.0.1:9090 ...
curl -s http://127.0.0.1:9090/metrics
curl -s http://127.0.0.1:9090/healthz
```

Metrics include symbols, snapshots, deltas, rejected deltas, sequence resets,
source switches, stale probes, writer queue depth, bytes, and per-source status.

## Replay and inspection

`fixtures/sample_replay.jsonl` is a deterministic fixture covering discovery,
two replicas, snapshots, absolute deltas, a source loss and failover, a sequence
reset and recovery, a lifecycle removal, periodic snapshots, and finalization.
The complete record schema is documented in
[docs/replay-format.md](docs/replay-format.md).

## Measured latency

A short public Polymarket run was measured on this machine (macOS, arm64) on
2026-10-23. The exact command was:

```bash
cargo run --release -- --exchange polymarket --max-markets 3 \
  --duration-secs 15 latency --out measurements/polymarket-latency-2026-10-23.json
```

Measured result from that one run:

| Metric | Value |
|---|---:|
| Discovery RTT | 1,725.944 ms |
| Frames | 17 |
| Timestamped events | 33 |
| p50 latency | 242,669 µs |
| p95 latency | 4,511,709 µs |
| p99 latency | 4,511,709 µs |
| mean latency | 615,049 µs |
| min / max | 183,544 / 4,511,709 µs |

These are measurements, not benchmarks or guarantees. Timestamp latency is
`local_receive_us - exchange_timestamp_us` and includes exchange/local clock
offset and the semantics of the exchange's event timestamp. Polymarket's initial
`book` snapshots can carry timestamps older than the local receive time, which
inflates the tail. The methodology and caveats are in
[docs/latency.md](docs/latency.md).

A 20-second public smoke capture with 4 symbols and 2 replicas produced 58 SBE
messages (8 snapshots and 50 deltas), with zero writer errors and zero rejected
deltas. The measured run report is checked in at
[measurements/polymarket-smoke-report.json](measurements/polymarket-smoke-report.json);
the exact reproduction command is in
[measurements/README.md](measurements/README.md).

## Configuration

See `config.example.toml` (Kalshi) and `config.polymarket.example.toml`.
Precedence is defaults, TOML, environment, then explicit CLI flags. Important
settings:

| Setting | Default | Meaning |
|---|---:|---|
| `collector.shards` | 4 | Fixed shard count; `0` derives it |
| `collector.replicas` | 2 | Redundant sockets per shard |
| `collector.snapshot_interval_secs` | 60 | Full local snapshot cadence |
| `collector.discovery_interval_secs` | 30 | Discovery reconciliation cadence |
| `collector.reconnect_after_secs` | 1800 | Periodic socket refresh |
| `collector.connection_idle_timeout_secs` | 45 | No-frame reconnect threshold |
| `collector.stale_probe_after_secs` | 120 | Quiet-symbol probe threshold |
| `collector.probe_interval_secs` | 30 | Probe spacing |
| `collector.max_probes` | 3 | Probes before reconnect/failover |
| `collector.final_snapshots` | true | Final snapshots on shutdown |
| `collector.writer_queue` | 16384 | Bounded writer queue |
| `collector.flush_interval_secs` | 1 | Output-visibility bound: every interval with traffic ends with a flush, so a downstream reader sees an accepted delta within about one second |
| `output.chunk_size` | 65536 | Power-of-two aligned chunk |
| `output.path` | `-` | File or binary stdout |

## Repository layout

```text
src/
  cli.rs              CLI definitions and overrides
  config.rs           TOML/env/CLI configuration and validation
  coordinator.rs      live orchestration, discovery, signals, reports
  collector/state.rs  deterministic core, sharding, shadow books, failover
  exchange/kalshi/    auth, discovery, codec, websocket session
  exchange/polymarket/ Gamma discovery, CLOB codec, websocket session
  session.rs          generic source supervisor
  writer.rs           single serialized SBE writer
  replay.rs           normalized JSONL replay
  inspect.rs          aligned SBE inspection
  latency.rs          measured discovery/timestamp latency
  report.rs           atomic run-report and metadata sidecars
  util/               supplied aligned SBE helpers and generated DTOs
fixtures/             deterministic replay fixture
docs/                 replay format and latency methodology
measurements/         exact measured reports from public runs
```

## Known limitations and notes

- Kalshi live validation requires credentials that are not checked into this
  repository. The authenticated path is implemented and covered by codec/auth
  tests; a demo live run is the next validation step once credentials are
  supplied.
- In this environment, production Kalshi REST/WS endpoints were not reachable
  reliably (connections reset), while the demo REST endpoint was reachable. The
  adapter supports both; use demo for reproducible local validation.
- Polymarket is a public, best-effort feed with no sequence numbers. Recovery is
  reconnect-and-resubscribe. Its event timestamps are not a send-time clock.
- `inspect --read-chunk-size auto` may select a smaller power-of-two chunk that
  also validates the file. Pass the writer's exact `--chunk-size` when the exact
  chunk size matters.
- The latency table is one short public-feed observation, not a controlled
  benchmark across networks or colocated hosts.

## License

MIT. The supplied SBE schema is unchanged.
