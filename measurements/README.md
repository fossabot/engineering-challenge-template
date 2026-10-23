# Measurements

These files are raw output from actual local runs, not benchmark claims. The
four 2026-10-24 latency files are single 60-second observations each; the
source-port/CDN-hashing and Nagle/delayed-ACK explanations in `docs/latency.md`
are hypotheses, not causal findings. The final offline gate results are in
[`docs/final-verification-2026-10-25.md`](../docs/final-verification-2026-10-25.md).

- `remediation/`: raw run reports, metadata sidecars, and the Gamma offset-cap
  failure log from the 2026-10-24 validation runs. The smoke/scale reports used
  64 KiB chunks; later captures use the 64 MiB default.
- `polymarket-latency-2026-10-23.json`: release-build `latency` run with 3
  Polymarket outcome tokens for 15 seconds. Methodology and interpretation are
  in [`docs/latency.md`](../docs/latency.md). Predates cross-connection skew;
  retained for comparison.
- `polymarket-latency-{baseline,race4,portpin,nodelay-off}-2026-10-24.json`:
  the four Extension 4 observations (baseline, four-way handshake racing,
  pinned source ports, `TCP_NODELAY` off), each 5 tokens, 2 replicas, 60
  seconds, release build, one run per configuration. Findings and their
  evidence level are in [`docs/latency.md`](../docs/latency.md).
- `polymarket-smoke-report.json`: release-build 20-second `collect` smoke run
  with 4 Polymarket outcome tokens, 1 shard, and 2 replicas. It records 58 SBE
  messages (8 snapshots, 50 deltas), zero writer errors, and zero rejected
  deltas.
- `polymarket-smoke-markets.json`: metadata sidecar from that smoke run.

The corresponding SBE capture was written to `/tmp/ec-smoke/polymarket.sbe` and
is not checked in. Reproduce the run with:

```bash
cargo run --release -- \
  --exchange polymarket --max-markets 4 --shards 1 --replicas 2 \
  --duration-secs 20 \
  --output /tmp/ec-smoke/polymarket.sbe \
  --metadata /tmp/ec-smoke/polymarket-markets.json \
  --report /tmp/ec-smoke/polymarket-report.json \
  collect
```

Copy the resulting report and metadata sidecars into `measurements/` if you
need to refresh the checked-in artifacts.
