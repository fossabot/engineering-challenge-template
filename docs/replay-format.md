# Normalized replay fixture format

`replay` consumes one JSON object per line (JSONL). Blank lines and lines whose
first non-whitespace character is `#` are ignored. Every record carries a local
`received_at_us` timestamp; timestamps must be non-decreasing across the file.

The non-decreasing rule is a replay-input convenience for a deterministic
event timeline; live SBE file order is the single writer's processing order and
can contain cross-symbol receive-timestamp regressions (see the
timestamp/ordering contract in the [README](../README.md#ordering-and-timestamp-contract)).

The replay path feeds the same `CollectorCore` used by live collection. It does
not sleep or contact an exchange. Snapshot scheduling, source selection, and
failover are therefore driven by fixture timestamps and are deterministic.

## Common conventions

- `received_at_us`: microseconds since the Unix epoch. Required on every record.
- `source`: `{"shard": 0, "replica": 0}`. Optional; defaults to shard 0,
  replica 0. Replay uses one shard and at least two replicas, so replica 0 and 1
  are both available.
- `native_symbol`: the exchange wire identifier (Kalshi ticker or Polymarket
  asset id). It must match a `native_symbol` in the most recent `discovery`.
- Prices and quantities use the SBE fixed-point scale: `1_000_000` = $1.00 or
  one contract/share.
- `exchange_ts_us`: optional exchange timestamp. If absent, the core uses the
  local `received_at_us` for the SBE `exchange_timestamp`.
- `seq` and `clock`: optional exchange sequence and clock values. The core does
  not enforce sequence numbers itself; the live exchange sessions do. They are
  preserved for failover freshness comparisons.
- `side` accepts `Buy`/`Sell` case-insensitively, plus `bid`/`ask` and
  `yes`/`no` aliases.

## Records

### `discovery`

Reconciles the symbol set. New symbols are assigned to the least-loaded shard;
missing symbols receive an empty snapshot if they had already been emitted.

```json
{"type":"discovery","received_at_us":1700000000000000,"markets":[
  {"exchange":"kalshi","native_symbol":"TEST-A","symbol":"kalshi:TEST-A",
   "title":"Test market A","outcome":null,"exchange_market_id":"TEST",
   "close_time_us":1700086400000000,"tick_size_us":10000}
]}
```

`MarketInfo` fields are required except the optional fields shown as `null`.

### `snapshot`

Replaces the source-local book for one symbol. A snapshot is a full state, not a
delta. A crossed or malformed snapshot is rejected by the core.

```json
{"type":"snapshot","received_at_us":1700000002000000,
 "source":{"shard":0,"replica":0},"native_symbol":"TEST-A",
 "bids":[{"price":400000,"quantity":100000000}],
 "asks":[{"price":600000,"quantity":80000000}],
 "exchange_ts_us":1700000001500000,"seq":1,"clock":1}
```

### `delta`

Changes one price level. Supply exactly one of:

- `quantity`: absolute replacement size (Polymarket semantics).
- `delta`: signed change to the current size (Kalshi semantics).

```json
{"type":"delta","received_at_us":1700000003000000,
 "source":{"shard":0,"replica":0},"native_symbol":"TEST-A",
 "side":"Buy","price":410000,"quantity":150000000,
 "exchange_ts_us":1700000002500000,"seq":2,"clock":2}
```

A delta for a source whose book is invalid is ignored. A delta that would make a
quantity negative invalidates that source's book and triggers recovery/failover.

### `lifecycle`

Applies an exchange lifecycle event. Terminal kinds (`determined`, `settled`,
`resolved`) and `deactivated` remove the symbol and emit an empty snapshot if it
was previously emitted. `close_date_updated` and metadata events update the
sidecar metadata held by the core.

```json
{"type":"lifecycle","received_at_us":1700000009000000,
 "source":{"shard":0,"replica":1},"native_symbol":"TEST-B",
 "kind":"deactivated","exchange_ts_us":1700000008500000}
```

Accepted kinds: `created`, `activated`, `deactivated`, `close_date_updated`,
`determined`, `settled`, `price_structure_updated`,
`price_level_structure_updated`, `metadata_updated`, `new_market`, `resolved`,
and any other string (treated as `Other`).

### `reset`

Invalidates the named symbols for one source and lets the core reseat them from
a healthy shadow book. If `native_symbols` is empty, every symbol assigned to
that source's shard is invalidated. Reasons: `connected`, `sequence_gap`,
`protocol_error`, `subscription_refresh`, `requested_refresh`.

```json
{"type":"reset","received_at_us":1700000006000000,
 "source":{"shard":0,"replica":1},"native_symbols":["TEST-A"],
 "reason":"sequence_gap"}
```

### `status`

Updates source connectivity. A transition from connected to disconnected
invalidates that source's books and fails over symbols for which it was active.

```json
{"type":"status","received_at_us":1700000004000000,
 "source":{"shard":0,"replica":0},"connected":false,
 "last_frame_us":1700000003000000,"last_error":"simulated disconnect"}
```

### `tick`

Runs periodic maintenance: local snapshots, stale probes, and failover.

```json
{"type":"tick","received_at_us":1700000068000000}
```

## End of file

After the last record, replay writes final snapshots for live active books when
`collector.final_snapshots = true` (the default), flushes the writer, and writes
the optional report/metadata sidecars.

## Example

The checked-in fixture is `fixtures/sample_replay.jsonl`. It covers discovery,
two replicas, a shadow snapshot, absolute deltas, a source loss and failover, a
sequence reset and recovery, a lifecycle removal, periodic snapshots, and final
snapshots.

```bash
cargo run --release -- \
  --output /tmp/replay.sbe \
  --report /tmp/replay-report.json \
  --metadata /tmp/replay-markets.json \
  replay fixtures/sample_replay.jsonl

cargo run --release -- inspect /tmp/replay.sbe --read-chunk-size auto
```
