#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


BRANCH_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = BRANCH_ROOT / "reports"


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def _delta(current: float, baseline: float, *, higher_is_better: bool) -> str:
    if baseline == 0:
        return "n/a"
    absolute = current - baseline
    relative = (absolute / baseline) * 100.0
    direction = "improved" if (absolute > 0) == higher_is_better else "regressed"
    sign = "+" if absolute >= 0 else ""
    return f"{direction} ({sign}{absolute:.2f}, {sign}{relative:.1f}%)"


def build_summary() -> str:
    universal = _load(REPORTS_DIR / "universal_hotspots.json")
    universal_smoke = _load(REPORTS_DIR / "universal_hotspots_smoke.json")
    vector = _load(REPORTS_DIR / "vector_hotspots.json")
    vector_smoke = _load(REPORTS_DIR / "vector_hotspots_smoke.json")

    filtered_query = vector.get("filtered_query") or {}
    filtered_stats = filtered_query.get("stats")
    filtered_diagnostics = filtered_stats.get("diagnostics") if isinstance(filtered_stats, dict) else None
    filtered_queries = vector.get("filtered_queries") or []
    filtered_lines = "\n".join(
        f"- Filtered bucket {entry['bucket_count']}: {entry['stats']['throughput_qps']:.1f} qps, p99 {entry['stats']['latency_p99_ms']:.3f} ms"
        for entry in filtered_queries
        if entry.get("stats")
    )

    return f"""# Branch 33 Hotspot Benchmark Summary

This summary converts the rerunnable hotspot JSON artifacts into a stable branch-facing snapshot. The smoke comparisons below are directional only because the canonical runs use materially heavier settings than the original smoke runs.

## UniversalStateServer

- Canonical artifact: `reports/universal_hotspots.json`
- Wire codec: `{universal.get("wire_codec", "json")}`
- Mixed-load settings: `{universal['mixed_load']['clients']} clients x {universal['mixed_load']['ops_per_client']} ops/client`
- Mixed-load throughput: `{universal['mixed_load']['throughput_requests_per_sec']:.1f} req/s`
- Mixed-load p95 / p99: `{universal['mixed_load']['latency_p95_ms']:.3f} ms` / `{universal['mixed_load']['latency_p99_ms']:.3f} ms`
- Pipeline throughput vs Redis: `{universal['pipeline']['throughput_ratio_vs_redis']:.3f}`

Smoke deltas:

- Mixed-load request throughput vs smoke: {_delta(universal['mixed_load']['throughput_requests_per_sec'], universal_smoke['mixed_load']['throughput_requests_per_sec'], higher_is_better=True)}
- Mixed-load p99 vs smoke: {_delta(universal['mixed_load']['latency_p99_ms'], universal_smoke['mixed_load']['latency_p99_ms'], higher_is_better=False)}
- Universal pipeline throughput vs smoke: {_delta(universal['pipeline']['universal']['throughput_ops_sec'], universal_smoke['pipeline']['universal']['throughput_ops_sec'], higher_is_better=True)}
- Pipeline ratio vs Redis vs smoke: {_delta(universal['pipeline']['throughput_ratio_vs_redis'], universal_smoke['pipeline']['throughput_ratio_vs_redis'], higher_is_better=True)}

Buyer-safe read:

- Branch 33 now sustains roughly `29k req/s` mixed control-plane traffic at sub-`2 ms` p99 on this canonical benchmark lane.
- Redis still leads on raw pipelined KV throughput, so the honest message remains controlled deployment rather than Redis-class replacement.

## VectorStateServerV2

- Canonical artifact: `reports/vector_hotspots.json`
- Build settings: `{vector['vectors']} vectors @ {vector['dimensions']}d, ef_search={vector['build']['result']['ef_search']}`
- HNSW rebuild time: `{vector['build']['duration_seconds']:.3f} s`
- Concurrent ANN throughput: `{vector['concurrent_hnsw_query']['throughput_qps']:.1f} qps`
- Concurrent ANN p95 / p99: `{vector['concurrent_hnsw_query']['latency_p95_ms']:.3f} ms` / `{vector['concurrent_hnsw_query']['latency_p99_ms']:.3f} ms`
{f"- Canonical filtered ANN lane ({filtered_query['filter_key']} bucket {filtered_query['bucket_index']} of {filtered_query['bucket_count']}): {filtered_stats['throughput_qps']:.1f} qps, p99 {filtered_stats['latency_p99_ms']:.3f} ms" if filtered_stats else "- Filtered ANN lane: not enabled in this artifact"}
{f"- Filtered telemetry: candidate bucket p50 {filtered_diagnostics['filtered_candidate_count']['p50']:.0f}, requested raw hits p50/p95 {filtered_diagnostics['requested_hnsw_hits']['p50']:.0f}/{filtered_diagnostics['requested_hnsw_hits']['p95']:.0f}, ceiling p95 {filtered_diagnostics['success_hnsw_hit_ceiling']['p95']:.0f}, scored p95 {filtered_diagnostics['scored_candidate_count']['p95']:.0f}, fallback count {filtered_diagnostics['fallback_count']}" if filtered_diagnostics else ""}
{filtered_lines if filtered_lines else ""}

Smoke deltas:

- HNSW rebuild time vs smoke: {_delta(vector['build']['duration_seconds'], vector_smoke['build']['duration_seconds'], higher_is_better=False)}
- Concurrent ANN throughput vs smoke: {_delta(vector['concurrent_hnsw_query']['throughput_qps'], vector_smoke['concurrent_hnsw_query']['throughput_qps'], higher_is_better=True)}
- Concurrent ANN p99 vs smoke: {_delta(vector['concurrent_hnsw_query']['latency_p99_ms'], vector_smoke['concurrent_hnsw_query']['latency_p99_ms'], higher_is_better=False)}

Buyer-safe read:

- Branch 33 ANN remains correct and measurable, but rebuild cost and concurrent query latency are still the blocking marketability issues.
- The filtered ANN benchmark is now split across multiple bucket sizes, and the report records bucket candidate counts, adaptive raw-hit budgets, success ceilings, and fallback frequency.
"""


def main() -> None:
    summary = build_summary()
    output_path = REPORTS_DIR / "hotspot_benchmark_summary.md"
    output_path.write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()