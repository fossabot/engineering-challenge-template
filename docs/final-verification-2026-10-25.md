# Final verification — 2026-10-25

One local CI-equivalent run of the submitted tree, at the final commit on
`fork/main`. The test counts quoted in the README come from this run; the
commit hash and ZIP digest are reported with the submission.

Environment: macOS (arm64), Rust 1.99.1 pinned by `rust-toolchain.toml`,
release profile for the binary.

## Gate results

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **118 library tests + 7 mock-exchange E2E tests + 2 replay integration tests = 127 passed**, 1 live test ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |

The ignored test is
`tests/live_polymarket.rs::discovers_and_subscribes_to_public_polymarket_books`;
it requires `RUN_LIVE_TESTS=1` and public network access.

## Mock-exchange end-to-end scenarios

`tests/e2e_mock_exchange.rs` drives `coordinator::run_with_shutdown` with the
real Polymarket discovery and session adapters, collector core, serialized SBE
writer, and SBE reader. The mock exchange owns an independent authoritative
book and a full delivered-update history.

| Test | Scenario | Oracle / assertion |
|---|---|---|
| `steady_state_history_matches_mock_exchange_truth` | One market, two replicas, seven updates | Every delivered update's side/price/quantity and resulting book match the mock history; the final reconstructed book and final snapshot match; `verify` is valid. |
| `single_socket_loss_fails_over_and_emits_anchoring_snapshot` | Active socket loss while the shadow is current | The failover snapshot equals the shadow truth at loss; resumed deltas apply to it; `verify` is valid. |
| `full_outage_recovery_exposes_the_uncovered_interval` | Both sockets lost; two updates occur while disconnected | The recovery snapshot equals the post-outage truth, but the capture is incomplete; `verify` reports a mismatch instead of claiming completeness. |
| `discovery_churn_retires_and_adds_symbols` | A market is removed and a new one added | The retired symbol ends with an empty retirement snapshot and no later delta; the new symbol starts with a snapshot and its own deltas; `verify` accepts the retirement boundary. |
| `silent_subscription_is_detected_and_repaired` | The mock stops delivering deltas without closing the socket | A stale probe triggers reconnect/resubscribe; the repair snapshot contains the unchanged truth; `verify` is valid. |
| `negative_control_missing_update_is_detected_by_verify` | Two updates are suppressed, then a forced snapshot exposes the gap | `verify` exits invalid with a mismatch sample. |
| `mock_subscription_scale_across_shards` | 1,000 symbols, 4 shards, 2 replicas | 8 loopback WebSockets, 250 symbols per socket, 2,000 subscription entries; every symbol's final book matches the mock, every symbol is snapshot-anchored, and `verify` is valid. |

The seven-test file was run three consecutive times immediately before the final
gate; all three runs passed 7/7 (about 12.5 seconds per run).

`verify` is only a capture self-consistency check; the oracle for capture
completeness under these scenarios is the mock exchange's full delivered-update
history plus the snapshots it sent at recovery boundaries. The full-outage and
negative-control tests assert that `verify` is invalid.

## Subscription-scale envelope

- **Tested:** 1,000 symbols, 4 shards, 2 replicas, 8 WebSockets, 250 symbols per
  socket, 2,000 subscription entries, loopback transport, 1,000 post-subscription
  updates, one capture, `verify` clean.
- **Not tested:** simultaneous live subscription to the 548,942 tokens found by
  the full-universe discovery sweep.
- **Configured limits:** up to 256 shards and 4 replicas per shard; automatic
  sharding uses `max_symbols_per_socket = 5,000` and `max_shards = 64`. These
  are configuration bounds, not measured live capacity.

## Timestamp and ordering contract

- `EventMeta.received_at_us` is captured once, immediately after a websocket
  frame is read and before decoding. It is serialized unchanged. There is no
  writer-level clamp.
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
  not a requirement or a contract; adding a reorder buffer would add latency
  and hide cross-source skew. A consumer that needs a sorted stream must buffer
  downstream with an explicit delay.

Deterministic evidence:
`collector::state::tests::snapshot_anchors_delta_application_in_per_symbol_order`,
`collector::state::tests::failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times`,
`collector::state::tests::cross_symbol_output_order_is_processing_order_and_not_globally_sorted`,
and the E2E `assert_per_symbol_anchor` / `assert_delta_receive_times_monotonic`
helpers.

## Live evidence retained (not part of this gate)

- Polymarket full-universe keyset discovery: 2,746 pages, 548,942 unique tokens,
  final page 78 records with no `next_cursor`, 9m52s, zero warnings.
- 90-second, ten-token, two-shard release capture: 493 SBE messages (33
  snapshots, 460 deltas), `verify` clean.
- Pristine template reader decoded 346/346 messages from a live capture.
- Four Extension 4 latency observations, each 60 seconds, five tokens, two
  replicas; raw reports in `measurements/`.

These are short public-feed observations. They are not a long soak, a live
Kalshi validation, a simultaneous full-universe subscription, or a controlled
latency benchmark.

## CI status

The local commands above are the CI-equivalent gates defined in
`.github/workflows/ci.yml`. The GitHub Actions workflow has **not** produced a
successful run from this environment: the latest inspected run failed before
any step executed, consistent with the account billing lock. This submission
does not claim green CI. The live public-feed workflow is manual
(`workflow_dispatch`) and was not triggered as part of this gate.

## Count reconciliation

| Document | Offline count stated |
|---|---|
| `README.md` | 127 (118 library + 7 E2E + 2 replay) |
| `docs/final-verification-2026-10-25.md` | 127 (118 library + 7 E2E + 2 replay) |

## Reproduction

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
```

The mock-exchange tests bind loopback ports only and do not contact an exchange.
The live test is separate:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture
```
