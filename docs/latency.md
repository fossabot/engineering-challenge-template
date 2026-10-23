# Latency experiment

## What is measured

The `latency` command measures two different things and never invents values:

1. **Discovery RTT**: wall-clock time around one `Exchange::discover_markets`
   call. This includes HTTP connection setup, pagination, and JSON decoding.
2. **Exchange-to-local timestamp latency**: for each decoded snapshot, delta, or
   lifecycle event that has an exchange timestamp:

   ```text
   latency_us = local_receive_at_us - exchange_timestamp_us
   ```

   `local_receive_at_us` is captured immediately after the websocket frame is
   read and before decoding. The result includes any exchange/local clock offset
   and whatever timestamp semantics the exchange uses.

Negative values are counted separately and excluded from percentiles. If no
timestamped events arrive, all latency fields are `null`.

Percentiles use nearest-rank selection on the collected samples, with no
interpolation or extrapolation.

## Command

Measured on 2026-10-23 on macOS 27.0.1 (arm64), using the release build:

```bash
cargo run --release -- --exchange polymarket --max-markets 3 \
  --duration-secs 15 latency \
  --out measurements/polymarket-latency-2026-10-23.json
```

The raw output is checked in at
[`measurements/polymarket-latency-2026-10-23.json`](../measurements/polymarket-latency-2026-10-23.json).
The measurement timestamp in that file is `2026-10-23T04:38:29Z`.

## Result of that run

| Metric | Measured value |
|---|---:|
| Discovery RTT | 1,725.944375 ms |
| Discovered markets (outcome tokens) | 3 |
| Subscribed markets | 3 |
| Duration | 15 s |
| Frames | 17 |
| Timestamped events | 33 |
| Bytes received | 26,880 |
| Latency samples | 33 |
| Negative samples | 0 |
| p50 | 242,669 µs |
| p95 | 4,511,709 µs |
| p99 | 4,511,709 µs |
| Mean | 615,049 µs |
| Min | 183,544 µs |
| Max | 4,511,709 µs |

This is one short public-feed observation. It is not a benchmark, a guarantee,
or a comparison against colocated infrastructure.

## Interpretation and caveats

- Polymarket's `book` snapshots can carry a timestamp older than the local
  receive time, especially for an initial dump. Those samples inflate the tail.
- Polymarket's `timestamp` is an exchange event timestamp, not a guaranteed
  send-time clock. A nonzero clock offset between the exchange and this host is
  included in every sample.
- The run uses the public internet from a laptop, not a colocated host.
- Discovery RTT includes Gamma pagination and JSON decoding; it is not a
  websocket handshake measurement.
- No Kalshi latency result is recorded here because live Kalshi order-book
  subscriptions require credentials that were not supplied. The same command
  works once `KALSHI_API_KEY_ID` and a private key are configured, and it should
  be run against demo first.

## Reproducing

The command writes the JSON report to `--out` and prints the same JSON to stdout.
Run it several times if you need a distribution across network conditions; do
not average separate runs unless you label that aggregate explicitly.
