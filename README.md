# Prediction-market order-book collector

A Rust collector for live prediction-market order books. It writes the supplied
FIX SBE schema, maintains a book per symbol, discovers markets continuously, and
load-balances symbols across redundant websocket sources.

Kalshi is the primary exchange adapter. Polymarket's public CLOB market channel
is included as a working fallback and as a public, credential-free way to
validate the transport, sharding, snapshot, replay, and inspection paths.

See the [requirement-to-test matrix](docs/requirement-test-matrix.md) for the
challenge-to-evidence map, and
[final verification](docs/final-verification-2026-10-25.md) for the exact
offline gate results.

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
- Provides deterministic JSONL replay, an offline SBE inspector, and a `verify`
  command that proves a capture's deltas reconstruct the snapshots that follow
  them.

## Build and test

Rust is pinned in `rust-toolchain.toml` (1.99.1).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
```

The final local CI-equivalent run has **127 passing offline tests**: 118
library tests, 7 mock-exchange end-to-end tests, and 2 replay integration tests.
The public Polymarket network test is separate and ignored by default. Exact
commands, counts, and the CI status are recorded in
[`docs/final-verification-2026-10-25.md`](docs/final-verification-2026-10-25.md).

The offline suite covers decoding, fixed-point arithmetic, order books,
sharding/failover/snapshot scheduling, maintenance clock resets, malformed
discovery rejection, SBE sentinel validation, an end-to-end replay fixture, and
a loopback mock exchange that drives the real coordinator, adapters, sessions,
writer, and SBE reader. The public Polymarket network test is opt-in:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture
```

The GitHub Actions workflow runs the same gates, but no successful run has
executed from this environment because the account is billing-locked. The local
gates pass; this repository does **not** claim green CI.

## Quick start: Kalshi

Kalshi websocket order-book subscriptions require an API key and a private key.
The collector supports Ed25519 PKCS#8, RSA PKCS#8, and RSA PKCS#1 PEM keys.
Credentials are read from the TOML file, environment, or CLI; no secrets are
written to reports or logs. The built-in environment default is `production`
because the challenge targets live markets; use `--kalshi-environment demo`
for the demo exchange and demo credentials. Startup logs state the exchange,
environment, output, metadata, and report destinations.

```bash
export KALSHI_API_KEY_ID="your-key-id"
export KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/kalshi-private-key.pem"

cargo run --release -- \
  --exchange kalshi \
  --kalshi-environment production \
  --output captures/kalshi.sbe \
  --metadata captures/kalshi-markets.json \
  --report captures/kalshi-report.json \
  collect
```

`KALSHI_PRIVATE_KEY_PEM` can be used instead of a path when a secret manager
injects the PEM directly. For the simulated exchange, add
`--kalshi-environment demo` and use demo credentials.

Discovery-only access does not need credentials:

```bash
cargo run --release -- --exchange kalshi --max-markets 10 discover
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

Discovery uses Gamma's keyset pagination (`GET /markets/keyset`): every page is
fetched with `closed=false`, a `limit` of at most 100 records, and the previous
page's `next_cursor` as `after_cursor`. Offset-style pagination is never used,
because Gamma rejects deep offsets with HTTP 422, so this is also how the full
universe is enumerated. Cursor cycles are rejected, repeated cursors fail the
round, rate-limit responses are retried with `Retry-After`, and records missing
an explicit `acceptingOrders == true` are excluded under the default strict
policy (a missing value is unconfirmed, not tradable; set
`polymarket.require_accepting_orders = false` to keep unknown records while
retaining the other filters).

## Commands

```text
collect    Live collection (default)
replay     Replay a normalized JSONL fixture through the same core
inspect    Decode an aligned SBE capture and print a JSON summary
verify     Rebuild each book from deltas and check it against every snapshot
latency    Measure discovery RTT, exchange-to-local timestamp latency, and
           cross-connection arrival skew
discover   Print the current live market set as JSON
```

Useful global flags include `--config`, `--output`, `--chunk-size`, `--shards`,
`--replicas`, `--snapshot-interval-secs`, `--discovery-interval-secs`,
`--max-markets`, `--duration-secs`, `--metrics-listen`, `--metadata`,
`--report`, and `--log-json`. Transport flags apply to every websocket
connection: `--no-tcp-nodelay`, `--source-ip`, `--source-port`, and
`--connect-race` (see [Latency experiments](#latency-experiments-extension-4)).

Examples:

```bash
# 60-second smoke run with a 16 KiB aligned chunk
cargo run --release -- --exchange polymarket --max-markets 10 \
  --duration-secs 60 --chunk-size 16384 --output /tmp/poly.sbe collect

# Inspect a capture; auto-detection is convenient for ad-hoc files
cargo run --release -- inspect /tmp/poly.sbe --read-chunk-size auto --limit 3

# Verify the capture: rebuild each book from deltas and compare it with the
# next snapshot; a non-zero exit means the capture is not self-consistent
cargo run --release -- verify /tmp/poly.sbe

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

### Ordering and timestamp contract

- Receive timestamps are captured once per frame and serialized unchanged. The
  writer does not clamp or rewrite them.
- File order is the single writer's processing order across all sources. It is
  **not** globally sorted by timestamp. A frame from one source can be processed
  after a later-timestamped frame from another source, so cross-symbol receive
  timestamps may regress.
- Per symbol, the first full snapshot anchors the book; later deltas apply to
  the most recent snapshot in file order. Failover, recovery, removal, periodic,
  and final snapshots are emitted before any resumed deltas.
- Generated snapshots are timestamped from the triggering status or processing
  time. A failover snapshot uses the lost source's last-frame time (or the
  current clock when none exists). Because status and data events arrive on
  different worker channels, that generated timestamp can be a few microseconds
  older than the last delta written before it. It is still the actual evidence
  time, not a rewritten event timestamp.
- Durations, retries, and timeouts use monotonic time: session backoff and
  timeouts use `Instant`/Tokio timers, and the live collector's snapshot/probe
  tick uses `MonotonicClock` anchored at startup. Event receive timestamps use
  the wall clock. Replay uses fixture timestamps so the core remains
  deterministic.
- No bounded reordering buffer is implemented. Global chronological output is
  not a contract; a consumer that needs a sorted stream must buffer downstream
  with an explicit delay.

Deterministic tests:
`collector::state::tests::snapshot_anchors_delta_application_in_per_symbol_order`,
`collector::state::tests::failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times`,
`collector::state::tests::cross_symbol_output_order_is_processing_order_and_not_globally_sorted`,
and the E2E per-symbol anchor helpers.

## Output

The output file is an aligned SBE stream with 64 MiB chunks by default, the
same chunk size the template's `AlignedSbeReader::open()` expects. A periodic
flush writes only the encoded bytes, without padding; the final chunk is
zero-padded on a graceful close, so the closed capture is readable by both the
supplied helper and `inspect`. The same format is used for stdout
(`--output -`): redirect it to a file (`> capture.sbe`) when the template
reader will consume it.

A live capture may be read while it is being written: complete chunks decode
normally, and the partial final chunk decodes up to its last complete message.
It is safe to inspect with:

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

## Replay, inspection, and verification

`fixtures/sample_replay.jsonl` is a deterministic fixture covering discovery,
two replicas, snapshots, absolute deltas, a source loss and failover, a sequence
reset and recovery, a lifecycle removal, periodic snapshots, and finalization.
The complete record schema is documented in
[docs/replay-format.md](docs/replay-format.md).

`inspect` proves that every message decodes. `verify` goes further: it
reconstructs each symbol's book from the captured deltas and compares that state
with the next full snapshot in the file. A mismatch means the capture does not
describe one complete book history (a lost, duplicated, or out-of-order delta),
so the command exits non-zero and reports the offending symbols with samples.
`unknown_messages` (the schema allows template ids this collector never emits)
and deltas that arrive before their symbol's first snapshot also fail
verification; the check is deliberately strict because a silently inconsistent
backtest is worse than a failed capture. An empty snapshot is treated as a
retirement/empty-book boundary rather than a mismatch: it carries no level
evidence, so it cannot prove that a suppressed update was observed. Only a
later non-empty snapshot can expose a gap.

### Capture completeness and the mock exchange

`tests/e2e_mock_exchange.rs` is an independent oracle test, not another unit
test of the core. It starts loopback Gamma keyset and Polymarket WebSocket
servers, drives `coordinator::run_with_shutdown`, and reads the resulting SBE
file through the same reader used by `inspect`/`verify`. The mock exchange owns
the authoritative book and a full delivered-update history. The seven scenarios
are:

1. steady state: every delivered update and resulting book matches the mock
   history, not only the final book;
2. one socket loss: the failover snapshot anchors the shadow book and resumed
   deltas apply to it;
3. full outage: updates during the uncovered interval are absent and `verify`
   reports the mismatch instead of claiming completeness;
4. discovery churn: a removed symbol ends with an empty retirement snapshot and
   a new symbol starts with its own snapshot;
5. silent subscription: suppressed updates trigger a stale probe,
   reconnect/resubscribe, and a repair snapshot;
6. negative control: a missing update is detected when a later non-empty
   snapshot exposes it;
7. subscription scale: 1,000 symbols, 4 shards, 2 replicas, 8 WebSockets, 250
   symbols per socket, and 2,000 subscription entries.

The mock exchange is the completeness oracle for these scenarios. `verify` is a
capture self-consistency check, not a proof that no update was missed. The
full-outage and negative-control tests deliberately assert that `verify` is
invalid.

The scale test is the tested operating envelope, not a live capacity claim. The
548,942-token discovery sweep proves keyset discovery only; it does not prove
simultaneous subscription or collection at that fan-out. Configured bounds are
up to 256 shards, 4 replicas, `max_symbols_per_socket = 5,000`, and
`max_shards = 64` for automatic sharding.

## Latency experiments (Extension 4)

The `latency` command measures, without inventing values:

1. **Discovery RTT** — wall-clock time around one discovery call.
2. **Exchange-to-local timestamp latency** — `local_receive_us -
   exchange_timestamp_us` per event, reported as percentiles. This includes
   exchange/local clock offset and the venue's own timestamp semantics.
3. **Connection setup** — per-replica `replica_connect_ms` (TCP + TLS +
   websocket handshake, including any racing) and `replica_first_frame_ms`
   (subscription to first frame).
4. **Cross-connection arrival skew** — with `--replicas N`, every replica
   subscribes to the same symbols and identical events (same symbol, exchange
   timestamp, exchange clock, and event shape) are matched across connections.
   Each delta is measured against the earliest arrival, which is
   clock-independent: it compares when the same update reached two sockets on
   this host. Per-replica `replica_events` and `replica_first_arrivals` show
   whether one connection is consistently ahead.

The first two seconds after all subscriptions are established are excluded so
initial snapshot dumps do not dominate the skew distribution. Events only one
replica observes are counted as abandoned rather than silently dropped.

Transport experiments are first-class configuration (`[network]` in TOML or the
matching CLI flags) and apply to `collect` as well as `latency`:

- `tcp_nodelay` (default `true`) sets `TCP_NODELAY` on every websocket socket;
  the underlying library does not do this for you.
- `source_ip` / `source_port` bind the local side of every connection before
  connecting, which is how source-port pinning is measured. The configured port
  is a base: connection `shard * 256 + replica` binds `base + ordinal`, so
  parallel connections never collide. A fixed source port cannot be combined
  with racing.
- `connect_race` opens that many full TCP + TLS + websocket handshakes
  concurrently and keeps the first success; the losers are closed. Racing
  includes the websocket upgrade, not just the TCP SYN.

An authentication rejection (HTTP 401/403) is never raced away: it fails
immediately so credential errors stay diagnosable.

## Measured latency

Four Extension 4 experiments were measured on this machine (macOS, arm64) on
2026-10-24, each with five Polymarket tokens, two replicas, and a 60-second
window; the raw reports are checked in under `measurements/`:

| Metric | Baseline | `--connect-race 4` | `--source-port 51800` | `--no-tcp-nodelay` |
|---|---:|---:|---:|---:|
| Connect (per replica) | 1487 / 1007 ms | 636 / 759 ms | 3167 / 5313 ms | 1164 / 1432 ms |
| Matched cross-connection events | 176 | 216 | 354 | 396 |
| Arrival skew p50 | 65 µs | 59 µs | 67,845 µs | 10,543 µs |
| Arrival skew p95 | 27,004 µs | 5,496 µs | 74,093 µs | 15,280 µs |

These are four single 60-second observations, not benchmarks, guarantees, or
causal identification. The cross-connection arrival skew is the
clock-independent number: two sockets on this host usually saw the same update
within tens of microseconds, and racing four full handshakes selected a faster
connection in these runs. Source-port pinning was the *worst* configuration in
this sample; one hypothesis is that the venue's CDN hashes the source port onto
a worse origin path, but that mechanism was not isolated. Disabling
`TCP_NODELAY` correlated with a skew p50 of 10.5 ms instead of 65 µs; a
Nagle/delayed-ACK interaction is a hypothesis, not a proven cause. Timestamp
latency (`local_receive_us - exchange_timestamp_us`) remains dominated by
exchange/local clock offset and venue timestamp semantics. The methodology, all
caveats, and the full findings are in [docs/latency.md](docs/latency.md).

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
| `collector.flush_interval_secs` | 1 | Output-visibility bound: every interval with traffic ends with an unpadded flush, so a downstream reader sees an accepted delta within about one second |
| `network.tcp_nodelay` | true | `TCP_NODELAY` on every websocket socket |
| `network.source_ip` | none | Local IP to bind before connecting |
| `network.source_port` | none | Base local source port; connection `shard*256+replica` binds `base + ordinal`; not combinable with `connect_race > 1` |
| `network.connect_race` | 1 | Simultaneous full handshakes; the first success is kept |
| `polymarket.discovery_page_size` | 100 | Gamma keyset page size; the endpoint accepts at most 100 |
| `polymarket.require_accepting_orders` | true | Only an explicit `acceptingOrders == true` is tradable |
| `output.chunk_size` | 67108864 | Power-of-two chunk; defaults to 64 MiB for template-reader compatibility. Padding is written only on chunk rotation and at close |
| `output.path` | `-` | File or binary stdout |

## Repository layout

```text
src/
  cli.rs              CLI definitions and overrides
  config.rs           TOML/env/CLI configuration and validation
  coordinator.rs      live orchestration, discovery, signals, reports
  collector/state.rs  deterministic core, sharding, shadow books, failover
  exchange/kalshi/    auth, discovery, codec, websocket session
  exchange/polymarket/ Gamma keyset discovery, CLOB codec, websocket session
  session.rs          generic source supervisor
  writer.rs           single serialized SBE writer
  replay.rs           normalized JSONL replay
  inspect.rs          aligned SBE inspection
  verify.rs           capture self-consistency verification
  latency.rs          measured latency, connection setup, and cross-replica skew
  report.rs           atomic run-report and metadata sidecars
  time.rs             wall-clock timestamps and the monotonic scheduling clock
  ws.rs               websocket transport: TCP_NODELAY, source binding, racing
  util/               supplied aligned SBE helpers and generated DTOs
fixtures/             deterministic replay fixture
tests/                replay, opt-in live, and mock-exchange E2E tests
docs/                 replay format, latency methodology, requirement matrix,
                      and final verification evidence
measurements/         exact measured reports from public runs
```

## Known limitations and notes

- Kalshi live validation requires credentials that are not checked into this
  repository. The authenticated path is implemented and covered by codec/auth
  and mock-websocket tests; a live run is the next validation step once
  credentials are supplied.
- Kalshi endpoint reachability depends on the environment. The adapter
  supports production (the default) and demo; switch explicitly to demo for
  simulated validation.
- Polymarket is a public, best-effort feed with no sequence numbers. Recovery is
  reconnect-and-resubscribe. Its event timestamps are not a send-time clock.
- `inspect --read-chunk-size auto` streams candidate chunks from disk instead of
  reading the whole file into memory, and never considers a chunk larger than
  256 MiB; captures written with a larger chunk need an explicit
  `--read-chunk-size`. Auto-detection may select a smaller power-of-two chunk
  that also validates the file, so pass the writer's exact `--chunk-size` when
  the exact chunk size matters.
- The latency tables are four short public-feed observations, not controlled
  benchmarks across networks or colocated hosts. Source-port/CDN hashing and
  Nagle/delayed-ACK explanations are hypotheses, not causal findings.
- The 548,942-token full-universe sweep is a discovery result, not a
  simultaneous-subscription result. The tested subscription envelope is 1,000
  loopback symbols across 4 shards and 2 replicas; configured limits are not
  measured live capacity.
- Global chronological output is not a contract. File order is processing order
  and preserves actual receive timestamps; timestamp sorting can reorder
  cross-source events and is not equivalent to replaying the capture.
- GitHub Actions has not run successfully from this environment because the
  account is billing-locked. The local CI-equivalent gates pass; CI is not
  claimed green.

## License

MIT. The supplied SBE schema is unchanged.
