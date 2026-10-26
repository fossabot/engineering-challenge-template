#!/usr/bin/env python3
"""Check source/test inventory drift and architecture traceability without dependencies.

Run from any working directory. --update intentionally rewrites generated inventory
files after a reviewed source/test change; --check is read-only and is used by CI.
This is a structural/traceability gate, not a substitute for Mermaid's syntax parser.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/architecture.md"
SOURCE_INVENTORY = ROOT / "docs/architecture/source-inventory.md"
TEST_INVENTORY = ROOT / "docs/architecture/test-inventory.md"

# Every tracked Rust source module must have an explicitly reviewed responsibility.
SOURCE_RESPONSIBILITIES = {
    "src/cli.rs": "clap command model and CLI-to-settings overrides",
    "src/config.rs": "configuration defaults, deserialization, and validation",
    "src/coordinator.rs": "startup, orchestration loop, discovery reconciliation, controls, shutdown",
    "src/fixed.rs": "fixed-point price/quantity parsing and checked arithmetic",
    "src/inspect.rs": "offline SBE capture inspection command",
    "src/latency.rs": "latency experiment orchestration and result output",
    "src/lib.rs": "library module exports and public entry points",
    "src/main.rs": "process entry point and collect/discover/replay/inspect/verify/latency dispatch",
    "src/metrics.rs": "metrics registry, Prometheus text encoding, HTTP health/metrics endpoint",
    "src/model.rs": "normalized market, source, event, control, and output domain types",
    "src/orderbook.rs": "normalized order-book representation and mutation invariants",
    "src/replay.rs": "deterministic JSONL replay into the collector/output path",
    "src/report.rs": "market metadata and run-report sidecars; atomic JSON helper",
    "src/session.rs": "generic websocket session/factory contract and supervised worker lifecycle",
    "src/side.rs": "normalized buy/sell side semantics",
    "src/time.rs": "wall-clock and monotonic timing helpers",
    "src/verify.rs": "SBE delta-to-snapshot self-consistency verification",
    "src/writer.rs": "bounded writer queue, dedicated writer thread, SBE sink lifecycle",
    "src/ws.rs": "websocket connection options, source binding and connection experiments",
    "src/capacity.rs": "startup capacity plan and shard derivation with hard gates",
    "src/catalog.rs": "versioned discovery checkpoint with checksum, age, and counts",
    "src/persistence.rs": "latest-wins metadata/catalog background writer",
    "src/collector/mod.rs": "collector core module boundary",
    "src/collector/state.rs": "serialized per-symbol/source state machine, discovery and failover decisions",
    "src/exchange/mod.rs": "Exchange and adapter-independent session factory abstraction",
    "src/exchange/kalshi/auth.rs": "Kalshi request signing and auth header construction",
    "src/exchange/kalshi/codec.rs": "Kalshi wire-frame parsing and normalized events",
    "src/exchange/kalshi/discovery.rs": "Kalshi REST pagination, market eligibility and metadata conversion",
    "src/exchange/kalshi/mod.rs": "Kalshi adapter construction and Exchange implementation",
    "src/exchange/kalshi/session.rs": "Kalshi websocket protocol, subscriptions, acknowledgements and sequence recovery",
    "src/exchange/polymarket/codec.rs": "Polymarket CLOB payload validation and normalized events",
    "src/exchange/polymarket/discovery.rs": "Gamma keyset pagination, version-aware identifiers and outcome expansion",
    "src/exchange/polymarket/mod.rs": "Polymarket adapter construction and Exchange implementation",
    "src/exchange/polymarket/session.rs": "public CLOB websocket protocol and subscription/resubscription behavior",
    "src/util/aligned_sbe_reader.rs": "aligned SBE capture reader",
    "src/util/aligned_sbe_writer.rs": "aligned SBE chunk writer and sink helpers",
    "src/util/build.rs": "build-time SBE DTO/codec generation integration",
    "src/util/dtos.rs": "generated domain DTO bridge for supplied SBE schema",
    "src/util/mod.rs": "SBE utility module exports",
    "src/util/orderbook_schema.rs": "schema-facing book message definitions/adaptation",
    "src/util/sbe_yaml.rs": "supplied SBE schema loading/validation support",
}

# Exact test symbols mentioned as evidence in docs/architecture.md.
REQUIRED_TESTS = {
    "snapshot_anchors_delta_application_in_per_symbol_order",
    "cross_symbol_output_order_is_processing_order_and_not_globally_sorted",
    "shadow_replica_takes_over_after_source_loss",
    "failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times",
    "single_socket_loss_fails_over_and_emits_anchoring_snapshot",
    "discovery_adds_and_removes_with_controls",
    "discovery_churn_retires_and_adds_symbols",
    "normal_shutdown_drains_and_joins",
    "shutdown_drains_saturated_queue_and_joins",
    "finish_failure_on_shutdown_is_reported",
    "negative_control_missing_update_is_detected_by_verify",
    "explicit_shard_count_must_satisfy_the_per_socket_bound",
    "automatic_sharding_derives_the_required_count",
    "round_trips_and_reports_age",
    "rejects_a_modified_market_list",
    "writes_metadata_and_catalog_and_reports_success",
    "catalog_generation_time_is_independent_of_metadata_time",
    "latest_submission_wins_over_intermediate_versions",
    "submit_does_not_block_on_a_slow_write",
    "write_failure_is_reported_and_the_worker_survives",
}

# Anchors that make the main structural claims falsifiable against code.
REQUIRED_ANCHORS = {
    "src/main.rs": [
        "Command::Collect", "Command::Replay", "Command::Inspect",
        "Command::Verify", "Command::Latency", "Command::Discover",
    ],
    "src/coordinator.rs": [
        "run_with_shutdown", "discover_with_retry", "process_worker_event",
        "dispatch_controls", "tokio::select!", "core.finalize",
        "writer.shutdown_with_stats",
    ],
    "src/capacity.rs": [
        "derive_shard_count", "pub fn plan", "CapacityPlan",
    ],
    "src/collector/state.rs": [
        "apply_discovery", "handle_event", "handle_snapshot",
        "handle_level_change", "handle_lifecycle", "handle_stream_reset",
        "handle_status", "pub fn tick", "pub fn finalize", "fn reseat",
    ],
    "src/session.rs": [
        "pub trait MarketDataSession", "pub trait SessionFactory",
        "pub async fn run_worker", "backoff_or_stop", "handle_control",
    ],
    "src/writer.rs": [
        "pub struct WriterHandle", "pub async fn send", "shutdown_inner",
        "blocking_recv", "sync_on_close",
    ],
    "src/exchange/kalshi/auth.rs": ["pub struct KalshiAuth", "pub fn headers"],
    "src/exchange/kalshi/discovery.rs": ["fetch_markets", "fetch_page"],
    "src/exchange/kalshi/session.rs": ["struct SequenceTracker", "on_snapshot", "on_delta"],
    "src/exchange/polymarket/discovery.rs": ["fetch_markets", "fetch_page", "identifier_kind"],
    "src/exchange/polymarket/codec.rs": ["price_change", "unknown_event_types_are_counted_but_not_fatal"],
    "src/exchange/polymarket/session.rs": ["send_initial", "next_frame", "send_keepalive"],
    "src/verify.rs": ["pub fn run"],
    "src/util/build.rs": ["generate_str_to_out_dir", "GenerationConfig"],
}

TEST_RE = re.compile(
    r"#\[(?P<attr>(?:tokio::test|test)(?:[^\]]*)?)\]\s*"
    r"(?:(?:#\[[^\]]*\])\s*)*"
    r"(?:async\s+)?fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE | re.DOTALL,
)
MERMAID_RE = re.compile(r"```mermaid\s*\n(.*?)\n```", re.DOTALL)
NODE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\n]*\]|\([^\n]*\)|\{[^\n]*\})\s*$")
EDGE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+-->\s+([A-Za-z_][A-Za-z0-9_]*)\s*$")


def collect_sources() -> list[str]:
    return sorted(p.relative_to(ROOT).as_posix() for p in (ROOT / "src").rglob("*.rs"))


def collect_tests() -> list[tuple[str, str, str]]:
    records: list[tuple[str, str, str]] = []
    for base in (ROOT / "src", ROOT / "tests"):
        for path in sorted(base.rglob("*.rs")):
            rel = path.relative_to(ROOT).as_posix()
            content = path.read_text(encoding="utf-8")
            for match in TEST_RE.finditer(content):
                attr = match.group("attr")
                name = match.group("name")
                line = content.count("\n", 0, match.start()) + 1
                records.append((rel, name, f"{attr} (line {line})"))
    return sorted(records, key=lambda x: (x[0], x[1]))


def render_source_inventory() -> str:
    lines = [
        "<!-- Generated by scripts/check_architecture.py --update; do not hand-edit. -->",
        "# Rust source inventory", "",
        f"Tracked Rust source files: **{len(collect_sources())}**.", "",
        "| Source file | Responsibility |", "|---|---|",
    ]
    for path in collect_sources():
        description = SOURCE_RESPONSIBILITIES.get(path)
        if not description:
            description = "[REVIEW REQUIRED: add a reviewed responsibility in scripts/check_architecture.py]"
        lines.append(f"| `{path}` | {description} |")
    lines.append("")
    return "\n".join(lines)


def render_test_inventory() -> str:
    tests = collect_tests()
    lines = [
        "<!-- Generated by scripts/check_architecture.py --update; do not hand-edit. -->",
        "# Statically declared Rust test inventory", "",
        f"Test functions discovered from `#[test]` and `#[tokio::test]` attributes: **{len(tests)}**.",
        "", "This is a source inventory, not a claim that these tests passed in the current environment. Runtime test results belong in the dated verification record.",
        "", "| File | Test function | Attribute / line |", "|---|---|---|",
    ]
    for path, name, attr in tests:
        lines.append(f"| `{path}` | `{name}` | `{attr}` |")
    lines.append("")
    return "\n".join(lines)


def check_mermaid_structure(document: str) -> list[str]:
    """Check graph structure conservatively; GitHub Actions runs the full Mermaid parser."""
    errors: list[str] = []
    blocks = MERMAID_RE.findall(document)
    if len(blocks) < 7:
        errors.append(f"expected at least 7 Mermaid diagrams, found {len(blocks)}")
    # Mermaid edge variants used here: --> and -->|edge label|. Keep each edge on one line.
    arrow_re = re.compile(r"-->(?:\|[^|]+\|)?")
    side_re = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)(.*)$")
    shaped_re = re.compile(r"^\s*(?:\[[^\n]*\]|\([^\n]*\)|\{[^\n]*\})")
    for index, body in enumerate(blocks, start=1):
        lines = body.splitlines()
        if not lines or not re.match(r"^\s*flowchart\s+(?:LR|RL|TB|TD|BT)\s*$", lines[0]):
            errors.append(f"diagram {index}: first line must be a supported flowchart direction")
            continue
        declared: set[str] = set()
        edges: list[tuple[str, str, int]] = []
        for line_number, line in enumerate(lines[1:], start=2):
            stripped = line.strip()
            if not stripped or stripped.startswith("%%") or stripped.startswith("subgraph ") or stripped == "end":
                continue
            if arrow_re.search(line):
                parts = arrow_re.split(line, maxsplit=1)
                if len(parts) != 2:
                    errors.append(f"diagram {index}, line {line_number}: malformed edge: {stripped}")
                    continue
                lhs, rhs = parts
                left = side_re.match(lhs)
                right = side_re.match(rhs)
                if not left or not right:
                    errors.append(f"diagram {index}, line {line_number}: cannot parse edge endpoints: {stripped}")
                    continue
                source_id, source_tail = left.group(1), left.group(2)
                target_id, target_tail = right.group(1), right.group(2)
                if shaped_re.match(source_tail):
                    declared.add(source_id)
                if shaped_re.match(target_tail):
                    declared.add(target_id)
                # A bare endpoint must have been declared on another line.
                edges.append((source_id, target_id, line_number))
                continue
            node = NODE_RE.match(line)
            if node:
                declared.add(node.group(1))
                continue
            errors.append(f"diagram {index}, line {line_number}: unsupported/unparsed Mermaid statement: {stripped}")
        for source, target, line_number in edges:
            for node_id in (source, target):
                if node_id not in declared:
                    errors.append(f"diagram {index}, line {line_number}: edge references undeclared node {node_id}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="check generated inventories and architecture traceability")
    group.add_argument("--update", action="store_true", help="regenerate source/test inventories after review")
    args = parser.parse_args()

    errors: list[str] = []
    actual_sources = set(collect_sources())
    documented_sources = set(SOURCE_RESPONSIBILITIES)
    missing_docs = sorted(actual_sources - documented_sources)
    stale_docs = sorted(documented_sources - actual_sources)
    if missing_docs:
        errors.append("source files need reviewed responsibility mappings: " + ", ".join(missing_docs))
    if stale_docs:
        errors.append("source responsibility mappings point to removed files: " + ", ".join(stale_docs))

    actual_test_names = {name for _, name, _ in collect_tests()}
    missing_tests = sorted(REQUIRED_TESTS - actual_test_names)
    if missing_tests:
        errors.append("required evidence test(s) not found: " + ", ".join(missing_tests))

    if not DOC.exists():
        errors.append(f"missing architecture document: {DOC.relative_to(ROOT)}")
        document = ""
    else:
        document = DOC.read_text(encoding="utf-8")
    for anchor_path, anchors in REQUIRED_ANCHORS.items():
        file_path = ROOT / anchor_path
        if not file_path.exists():
            errors.append(f"required source anchor file is missing: {anchor_path}")
            continue
        source = file_path.read_text(encoding="utf-8")
        for anchor in anchors:
            if anchor not in source:
                errors.append(f"source anchor `{anchor}` not found in `{anchor_path}`")
    for test_name in REQUIRED_TESTS:
        if f"`{test_name}`" not in document:
            errors.append(f"required evidence test `{test_name}` is not cited in docs/architecture.md")
    for required_text in [
        "docs/architecture/source-inventory.md",
        "docs/architecture/test-inventory.md",
        "docs/requirement-test-matrix.md",
        "Current operational caveat",
        "not a production certification",
    ]:
        if required_text not in document:
            errors.append(f"architecture document is missing required traceability text: {required_text!r}")
    errors.extend(check_mermaid_structure(document))

    expected_files = {
        SOURCE_INVENTORY: render_source_inventory(),
        TEST_INVENTORY: render_test_inventory(),
    }
    if args.update:
        for path, expected in expected_files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected, encoding="utf-8")
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            return 1
        print(f"Updated inventories: {len(collect_sources())} Rust source files; {len(collect_tests())} attributed test functions.")
        return 0

    for path, expected in expected_files.items():
        if not path.exists():
            errors.append(f"missing generated inventory: {path.relative_to(ROOT)} (run --update after review)")
        elif path.read_text(encoding="utf-8") != expected:
            errors.append(f"stale inventory: {path.relative_to(ROOT)} (review changes, then run --update)")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"Architecture sync check passed: {len(collect_sources())} Rust source files, {len(collect_tests())} attributed test functions, and {len(MERMAID_RE.findall(document))} structurally checked Mermaid diagrams.")
    print("Note: actual Mermaid grammar is parsed in the separate GitHub Actions Mermaid-validation job.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
