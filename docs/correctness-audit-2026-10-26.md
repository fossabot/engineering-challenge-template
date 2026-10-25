# Correctness audit: writer, verifier, and standing questions

Branch: `phase1/correctness-audit`, developed from the verified baseline
`d4ab66d`. The baseline is untouched; every change below is additive to this
branch. Scope: an independent audit of the serialized writer and the verifier,
followed by the seven standing correctness questions. Kalshi remains excluded
from required live validation; Polymarket is the reference venue.

Method: each critical path was read end to end, its invariants were written
down, and every invariant was either matched to an existing test, covered by a
new regression test, or recorded as a proof obligation. Where a defect was
found, the fix landed with the test that reproduces it.

Severity scale: moderate, low, none. No defect found above moderate.

## Writer invariants and evidence

The writer is one thread consuming a bounded channel (`src/writer.rs`); the
coordinator's only output paths are `WriterHandle::send`, periodic `flush`,
and `shutdown_with_stats`.

| Invariant | Mechanism | Evidence |
|---|---|---|
| W1 Total write order | One thread consumes one FIFO channel | Structural; end-to-end ordering is asserted by the mock-exchange steady-state comparison of every delivered update |
| W2 Shutdown durability | Three-phase shutdown: command, close admission, join; an expired join returns an explicit incomplete-durability error with accepted/written/queued counts | `writer::tests::{normal_shutdown_drains_and_joins,shutdown_drains_saturated_queue_and_joins,finish_failure_on_shutdown_is_reported,sync_failure_on_shutdown_is_reported,write_failure_surfaces_and_worker_joins}` |
| W3 Output visibility | Periodic flush writes only encoded bytes; shutdown pads the final chunk to the template's chunk size | `writer::tests::{flush_makes_output_visible_to_reader,default_chunk_output_is_readable_by_default_reader}` |
| W4 Saturation fails loudly | Bounded queue with a send timeout; accepted messages always drain before join | `writer::tests::shutdown_drains_saturated_queue_and_joins` and the new test below |
| W5 Failures surface | Every sink error sets a fatal slot and a counter; the handle reports it on the next interaction | The four fault-injection tests |
| W6 Non-regular outputs | fsync is skipped where the platform rejects it (FIFOs) | `writer::tests::fifo_sink_flushes_and_skips_invalid_fsync` |

### W-1: timed-out send could enqueue a message and wrap the depth counter

- Invariant: a `send` that reports a timeout must not have delivered its
  message, and `queue_depth` must equal the queued messages.
- Code: the old `WriterHandle::send` wrapped `mpsc::Sender::send` in a
  timeout. When capacity appeared concurrently with the timer, tokio could
  enqueue the message while the caller observed a timeout; the caller then
  decremented `queue_depth`, the worker decremented it again on dequeue, and
  the atomic counter wrapped. The caller also reported failure for a message
  that had been accepted.
- Counterexample: a full queue and a send timing out exactly as the writer
  dequeues; the message is written but the collector records a fatal error
  and an underflowed queue depth.
- Severity: moderate. No capture-order violation; the impact is a false
  failure report and a corrupted metric on the fatal path.
- Fix: capacity is reserved with `mpsc::Sender::reserve` before the message
  is handed over. A timed-out send never acquires a permit, so it provably
  enqueues nothing and leaves the counter untouched.
- Regression test: `writer::tests::saturated_send_times_out_without_queue_side_effects`
  holds the worker inside a stalled write, fills the queue, times out a
  fourth send, and asserts exactly the three accepted messages were written
  with `queue_depth == 0` and zero errors.
- Verification: test passes on this branch; the saturated-FIFO drain test
  still passes unchanged.

## Verifier invariants and evidence

`verify` reconstructs each symbol's book from captured deltas and compares it
with the next full snapshot (`src/verify.rs`).

| Invariant | Mechanism | Evidence |
|---|---|---|
| V1 Anchor before delta | A delta applies to the most recent snapshot per symbol in file order; unanchored deltas are counted and fail verification | `verify::tests::reports_a_delta_before_any_snapshot`; E2E `assert_per_symbol_anchor` |
| V2 Empty snapshots are boundaries | An empty snapshot re-anchors instead of comparing levels | `verify::tests::empty_snapshot_is_a_retirement_boundary_not_a_mismatch`; E2E discovery churn |
| V3 Mismatches fail verification | The rebuilt book must equal the next non-empty snapshot | `verify::tests::reports_a_snapshot_that_disagrees_with_reconstructed_deltas`; E2E negative control and full outage |
| V4 Undecodable content fails | Unknown template ids and invalid deltas are errors, never skipped | `verify::tests::rejects_an_invalid_delta`; `unknown_messages` fails validity |

### V-1: the format's sentinel defines the end of the stream

- Invariant: verify reports the content up to the aligned format's end
  sentinel and nothing after it.
- Proof: the reader stops at the first all-zero sentinel and rejects non-zero
  bytes after one
  (`util::aligned_sbe_reader::tests::rejects_non_zero_bytes_after_a_chunk_sentinel`).
  A capture whose interior was zeroed therefore verifies its prefix and
  reports valid; verify cannot distinguish that from a complete short capture
  because the expected message count lives outside the file.
- Severity: low. Requires an already-corrupt capture.
- Detection: compare the summary's `messages` with the run report's
  `writer.messages`. The report is written after the writer joins, so a
  mismatch is an unambiguous truncation signal.
- Regression test: `verify::tests::an_empty_capture_validates_with_zero_messages`
  pins the sentinel-stop behavior.
- Fix: none. The sentinel is the supplied format's end marker; verify must
  not invent an expected count. Proof obligation recorded.

### V-2: the missing-event blind spots of a self-consistency check

- Invariant: verify detects a missing update exactly when a later non-empty
  snapshot for that symbol disagrees with the reconstruction.
- Proof: two blind spots follow from the definition. A missing update with no
  later non-empty snapshot for that symbol, and a missing update whose level
  is later overwritten to the same absolute value, both leave a self-
  consistent capture. The detected case is proven by the E2E negative
  control; the retirement-boundary case is proven by the discovery-churn
  scenario.
- Severity: none. This is the documented design limit of `verify`; the
  completeness oracle for delivered streams is the mock exchange's full
  delivered-update history (E2E steady state).
- Fix: none; stated in `README.md` and the final verification record.

## The seven standing questions

### Q1. Can a malformed or out-of-order event corrupt the reconstructed book?

- Invariant: malformed input is rejected without mutating state; delivered
  updates apply in receive order.
- Code and proof: a Polymarket frame decodes all-or-nothing — one malformed
  `price_change` entry rejects the entire frame, so a partially applied batch
  cannot reach the core. Snapshots are built into a fresh book and swapped
  only after every level validates (`orderbook.rs::replace`); duplicate,
  negative, and out-of-range levels are rejected; crossed snapshots are
  rejected and trigger recovery. A delta arriving on an invalid or reset book
  is dropped, and verify counts such deltas as failures. Within one
  connection TCP preserves venue order; Polymarket updates are absolute
  per-level sizes. Cross-source reordering is confined to failover, which
  re-anchors with a full snapshot.
- Severity: none found.
- Regression test added:
  `exchange::polymarket::codec::tests::one_malformed_change_rejects_the_whole_frame`.
- Verification: test passes; existing coverage
  (`orderbook::tests::*`, `verify::tests::*`, E2E anchor assertions) unchanged.

### Q2. Can a reconnect create a gap between the snapshot and subsequent deltas?

- Invariant: every connect invalidates that source's books before any
  subscription, and deltas are dropped until a fresh snapshot anchors the
  book.
- Code and proof: `run_worker` emits `StreamReset{reason: Connected}` before
  `apply_subscriptions`; the core invalidates the source's books, and
  `handle_level_change` drops deltas while the book is invalid. Polymarket's
  reconnect resubscribes with `initial_dump`, which provides the anchor.
- Documented tradeoff: failover to a live shadow without a reconnect
  publishes the shadow's snapshot, which can lag the lost source's last
  emitted state; the capture stays self-consistent, and the lag is bounded by
  the stale-probe and periodic-reconnect paths.
- Severity: low, documented.
- Evidence: E2E `full_outage_recovery_exposes_the_uncovered_interval` and
  `silent_subscription_is_detected_and_repaired` assert the anchor discipline
  and the repair behavior.

### Q3. Can one failed shard leave the collector reporting healthy?

- Finding: yes. `/healthz` answered `{"status":"ok"}` unconditionally, so a
  shard whose replicas were both down left the process reporting healthy while
  its symbols received nothing. The Prometheus endpoint already exported
  `collector_sources_connected`; the health endpoint did not.
- Invariant: the health payload reflects source connectivity.
- Severity: moderate (operational blindness), fixed.
- Fix: `/healthz` now reports `status` as `starting` before any source has
  reported, `ok` when every configured source is connected, and `degraded`
  when at least one is down, with `sources_connected` and `sources_total`.
  The endpoint still answers 200 OK: it reports readiness, and a process that
  can answer is alive.
- Regression test: `metrics::tests::health_status_reflects_source_connectivity`
  covers all three states, including one failed shard of three.
- Verification: test passes; the endpoint's HTTP behavior is otherwise
  unchanged.

### Q4. Can shutdown report success before the output reaches durable storage?

- Invariant: shutdown success implies finish (pad + flush) and, when
  configured, fsync; every failure class is an explicit error.
- Code and proof: the worker answers the shutdown command only after
  `finish` and `sync` succeed; a lost worker is reported from the fatal slot;
  an expired join returns an explicit incomplete-durability error with exact
  counts; the process exits non-zero on any of them. The four fault tests and
  the saturated-FIFO drain test cover each path.
- Gap found: the run report did not record whether the capture was fsynced,
  so a reader could not distinguish a synced capture from a buffered one.
- Severity: low, fixed.
- Fix: `ReportConfig` now carries `output_sync_on_close`; reports written
  before the field existed decode with the collector default.
- Regression test: `report::tests::report_config_records_the_durability_policy`.
- Verification: test passes; `RunReport` schema change is additive.

### Q5. Can the verifier accept a capture that contains a missing event?

- Answer: within the documented bounds of V-2, yes — and no further. A missing
  update is detected whenever a later non-empty snapshot exists for the
  symbol (proven by the E2E negative control); the blind spots are exactly the
  two cases in V-2 and are inherent to a self-consistency check. `verify`
  also fails unknown messages, unanchored deltas, and invalid levels.
- Severity: none beyond the documented limit.
- Fix: none. The delivered-stream oracle remains the mock exchange comparison.

### Q6. Can discovery retire a market that is still active?

- Invariant: a market is retired only when it is absent from an accepted
  discovery round.
- Code and proof: a round is accepted as a whole or rejected as a whole;
  rejected rounds retain the previous set. Guards exist for empty results,
  pages of only-malformed records, cursor cycles, duplicate identities, and
  identity changes. No guard exists for a single market that one accepted
  round omits: such a market is retired for that round and resubscribed when
  it reappears. The recorded live rounds (including the 548,942-token sweep)
  reconciled without a spurious retirement, so there is no observed
  counterexample.
- Severity: low.
- Fix: none in this phase. A retirement debounce (require two consecutive
  absent rounds) is the candidate change if venue flapping is ever observed;
  without evidence it would be a semantics change, not a fix. Recorded as a
  proof obligation.

### Q7. Can queue saturation cause silent data loss?

- Invariant: saturation produces an explicit failure; every accepted message
  is drained before the writer joins.
- Code and proof: a full queue times out the sender with an explicit error,
  which fails the collector with a non-zero exit; the run report records the
  writer counters. Drain-before-join is proven by the saturated-FIFO test.
  The adjacent defect W-1 — a timed-out send that could still enqueue its
  message and wrap the depth counter — is the only saturation-path defect
  found, and it is fixed with the reserve-permit send plus its regression
  test.
- Severity: moderate (W-1), fixed. No silent-loss path remains.

## Changes in this audit

1. `WriterHandle::send` reserves capacity before sending; a timed-out send
   provably enqueues nothing (W-1, Q7).
2. `/healthz` reports `starting`/`ok`/`degraded` with connected and total
   source counts (Q3).
3. `ReportConfig` records `output_sync_on_close` (Q4).
4. New tests: the writer saturation-timeout test, the health-status test, the
   durability-policy test, the malformed-batch codec test, and the
   empty-capture verifier test.

## Verification on this branch

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **123 library tests + 7 mock-exchange E2E tests + 2 replay integration tests = 132 passed**, 1 live test ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |

## Phase 1 exit: invariant-to-evidence map

| Invariant | Evidence |
|---|---|
| Malformed input never mutates book state | `orderbook::tests::*`, codec tests, `one_malformed_change_rejects_the_whole_frame` |
| Deltas apply only after a snapshot anchor | `verify::tests::reports_a_delta_before_any_snapshot`, E2E `assert_per_symbol_anchor` |
| Reconnects reset and re-anchor before deltas resume | E2E full outage and silent-repair scenarios |
| Failover publishes the replacement source's full snapshot first | E2E single-socket-loss scenario, `failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times` |
| Missing updates are exposed by a later snapshot | E2E negative control |
| Missing updates without a later snapshot are unverifiable in-band | V-2 proof above; documented limit |
| Truncation at an interior sentinel is unverifiable in-band | V-1 proof above; message-count comparison with the run report |
| Shutdown succeeds only after finish and configured fsync | Writer fault tests, saturated-queue drain, `output_sync_on_close` in reports |
| Saturation fails loudly and drains accepted messages | Saturated-FIFO test, `saturated_send_times_out_without_queue_side_effects` |
| Health reflects source connectivity | `health_status_reflects_source_connectivity` |
| Retirement follows accepted discovery rounds only | `removal_emits_empty_snapshot`, identity tests; Q6 proof obligation for venue flapping |
| Retirement boundaries do not fail verification | `empty_snapshot_is_a_retirement_boundary_not_a_mismatch`, E2E discovery churn |

Open proof obligations: Q6 (venue-side transient omission), V-1 (external
message-count check), V-2 (inherent self-consistency limit). Phase 2 (fault
injection at the transport and discovery boundaries) starts from this branch.

## Phase 2: session, sharding, and timestamp hardening

The second review pass raised seven findings against the baseline. Each is
recorded here with its disposition on this branch. Severity scale: high,
moderate, low. The baseline commit `d4ab66d` is unchanged.

### F1: quiet-symbol probes recycled the entire Polymarket socket (high)

- Invariant: a quiet symbol is probed and repaired without disturbing the
  connection's other subscriptions.
- Code: `tick` queues a snapshot request after `stale_probe_after_secs`;
  `PolymarketSession::request_snapshot` returned `SessionFailure::Reconnect`,
  which the supervisor treats as dropping the socket. The first probe
  therefore recycled every subscription on the connection and forced a fresh
  initial dump, bypassing the `max_probes` budget.
- Counterexample: any shard with a symbol that has no `price_change` events
  for the probe threshold (120 s by default) — common in a large,
  low-activity universe — reconnected roughly every two minutes and produced
  a burst of re-anchoring snapshots.
- Fix: `request_snapshot` cycles the asset's subscription on the live
  connection (unsubscribe, then subscribe, batched by `batch_size`). Recovery
  is still only accepted when the venue's fresh book arrives and anchors the
  core state; a venue that sends nothing leaves the probe budget to escalate
  to the existing reconnect path.
- Regression tests:
  `exchange::polymarket::session::tests::request_snapshot_cycles_the_subscription_on_the_live_connection`
  (mock market-channel server asserts the unsubscribe/subscribe pair on the
  same connection and the fresh book that follows), and the E2E
  `silent_subscription_is_detected_and_repaired` now asserts the repair opens
  no new connection (`next_id` unchanged, zero source switches, zero
  reconnects).
- Live proof: `tests/live_polymarket.rs::resubscribing_an_asset_returns_a_fresh_book`
  ran against the public venue on this branch and passed: cycling a
  subscription produced a fresh authoritative book on the same connection.
  Both live tests passed in 13.0 s.
- Verification: unit, protocol, E2E, and live all pass.

### F2: failover snapshots could predate their content (high)

- Invariant: a generated failover snapshot's local timestamp must not precede
  the receive time of any update included in the replacement book.
- Code: `handle_status` derived the snapshot generation time from the lost
  source's `last_frame_us`, while `reseat` stamped the shadow book, which can
  contain newer updates.
- Counterexample: an active source whose last frame predates the shadow's
  last update (for example a half-open connection detected through the 45 s
  idle timeout) produced a snapshot whose `backtest_timestamp` preceded data
  it contained — look-ahead for a consumer replaying the timeline.
- Fix: `handle_status` takes an explicit generation time; the coordinator
  passes the wall clock, replay passes the record timestamp. The lost
  source's last-frame time stays in its `SourceStatus`, and the shadow's last
  exchange timestamp is preserved as the snapshot's exchange timestamp.
- Regression test: `failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times`
  now builds an active book at NOW and a shadow updated at NOW + 200, marks
  the active source down with `last_frame_us = NOW + 50` and generation time
  NOW + 300, then asserts the snapshot carries NOW + 300 (never the older
  frame time), that it is at or after the shadow's availability time, and
  that the shadow's exchange timestamp is preserved separately.
- Verification: test passes; replay and E2E paths updated.

### F3: websocket setup had no bounded deadline (high)

- Invariant: one connection attempt — DNS resolution, TCP connect, TLS
  negotiation, and the websocket upgrade, across every raced candidate —
  completes or fails within a configured deadline.
- Code: `run_worker` awaited `connect()` without a timeout, and `ws::connect`
  awaited resolution and the handshake race without an end-to-end bound.
- Counterexample: a server that accepts TCP but stalls the upgrade left the
  source worker unable to process shutdown or reconnect controls until the
  coordinator's outer shutdown timeout aborted it.
- Fix: `network.connect_timeout_secs` (default 30, validated 1–600) flows
  into `ConnectOptions::connect_timeout`; `ws::connect` bounds the whole
  attempt and returns a transient error that the supervisor retries with
  backoff.
- Regression tests: `ws::tests::stalled_handshake_is_bounded_by_the_connect_timeout`
  (localhost server accepts TCP, never upgrades) and
  `session::tests::stalled_connect_reports_status_and_stops_cleanly` (the
  worker reports the failure with `last_error` containing "timed out" and
  still stops promptly on shutdown).
- Verification: both tests pass; the setting is covered by validation tests.

### F4: the per-socket bound was advisory, not enforced (moderate)

- Invariant: `ceil(symbols / shards) <= max_symbols_per_socket` for every
  run; automatic sharding never silently exceeds `max_shards`.
- Code: `derive_shard_count` returned an explicit shard count without
  checking the bound, and in automatic mode capped the derived count at
  `max_shards` without failing when the cap still violated the per-socket
  bound.
- Counterexample: the full-universe discovery set (548,942 tokens) with the
  default `shards = 4` would have attempted initial subscription frames of
  roughly 137k asset ids per socket.
- Fix: `derive_shard_count` now returns a result and rejects an incompatible
  run at startup: explicit shard counts must satisfy the bound, and automatic
  sharding fails loudly when the required count exceeds `max_shards`.
- Regression tests: `coordinator::tests::{explicit_shard_count_must_satisfy_the_per_socket_bound,automatic_sharding_derives_the_required_count}`
  cover immediately below, at, and above both limits.
- Verification: tests pass; README and the requirement matrix state the
  enforced bound.

### F5: two timestamp bases fed one output timeline (moderate)

- Invariant: every serialized timestamp — event receive times and generated
  snapshots — comes from one clock; monotonic time is used only for
  durations, retries, and timeouts, and is never serialized.
- Code: receive timestamps came from `now_us()`, while generated periodic,
  removal, and final snapshots took `MonotonicClock::now_us()` (wall anchor
  plus `Instant` elapsed). A laptop sleep stalls the monotonic part and NTP
  steps move the wall part, so the two diverged.
- Fix: `MonotonicClock` is removed; the coordinator stamps all generated
  output with `now_us()`, and Tokio timers (already monotonic) provide the
  tick cadence. A backward wall step delays a due snapshot rather than
  emitting early; a forward step fires it immediately. Nothing is clamped.
- Regression test: `generated_snapshots_follow_the_supplied_wall_clock_across_a_step`
  exercises a backward step (no early emission) and a forward pass of the
  deadline (snapshot stamped with the supplied time).
- Verification: test passes; README and the requirement matrix state the
  single-timeline contract.

### F6: the pending-frame buffer buffered nothing (moderate)

- Invariant: dropping the supervisor's frame-read future mid-poll loses no
  frame.
- Code: `next_frame_buffered` stored the frame into `pending` and took it in
  the same synchronous step; no await separates the two, so the buffer never
  carried a frame across a suspension.
- Fix: the wrapper is removed; the `select!` calls the session read directly.
  Cancellation safety is a session-contract property: read state lives in the
  transport, a decoded data frame is returned without further await points,
  and Polymarket's pending PONG is carried across calls. Kalshi control-frame
  handling that does await is re-driven by its acknowledgement timeout.
- Regression test: `ws::tests::dropping_a_read_future_loses_no_frame` polls a
  read, drops it while pending, and asserts both frames still arrive in
  order.
- Verification: test passes; the E2E suite exercises the same supervisor
  select with the real adapters.

### F7: operational details corrected (low)

- `src/config.rs` described flushes as padding to the chunk boundary; the
  writer pads only on rotation and close. The comment now matches the writer.
- Prometheus output typed every metric as a gauge. Cumulative families are
  now emitted as `counter`, gauges remain `gauge`, and
  `collector_sources_total` was renamed `collector_sources_configured`
  because it is a configured count, not a cumulative total. Test:
  `metrics::tests::prometheus_types_split_gauges_and_counters`.
- Two weak `orderbook` assertions accepted any error; both now match the
  exact error variant (`DuplicateLevel` for duplicate asks,
  `NegativeQuantity` for a negative level).
- The health payload test now also covers every source failing (degraded,
  never ok); the readiness policy was already recorded in Phase 1.

## Phase 2 verification on this branch

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **130 library tests + 7 mock-exchange E2E tests + 2 replay integration tests = 139 passed**, 2 live tests ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored` | Pass: 2/2 against the public venue (discovery/subscribe smoke and the resubscribe probe) |

Wording sweep on this branch: zero matches for historical/grader wording,
unfinished markers, first-person prose, and intent-adverb/idiom candidates in
the tracked tree; the supplied template helper comments were rewritten in
third person.
