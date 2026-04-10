#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np

BRANCH_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
if str(BRANCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BRANCH_ROOT))

from tools.acp_logging import get_logger
from benchmark_vector_vs_competitors import VectorStateClient


log = get_logger("branch33_vector_hotspots")


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return float(ordered[index])


def _summarize_int_series(values: list[int]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(int(value) for value in values)
    return {
        "min": float(ordered[0]),
        "p50": _percentile([float(v) for v in ordered], 50.0),
        "p95": _percentile([float(v) for v in ordered], 95.0),
        "p99": _percentile([float(v) for v in ordered], 99.0),
        "max": float(ordered[-1]),
    }


def _parse_bucket_scenarios(raw: str | None, canonical_bucket_count: int) -> list[int]:
    values: list[int] = []
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            values.append(int(part))
    if not values:
        values = [2, 4, canonical_bucket_count, 16]
    values.append(canonical_bucket_count)
    return sorted({value for value in values if value > 1})


def _wait_for_server(host: str, port: int, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5) as sock:
                sock.sendall(b'{"cmd":"PING"}\n')
                payload = b""
                while not payload.endswith(b"\n"):
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    payload += chunk
                if payload:
                    return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"VectorStateServerV2 did not become ready on {host}:{port}")


def _start_vector_server(host: str, port: int, wal_path: Path) -> subprocess.Popen[bytes]:
    wal_path.unlink(missing_ok=True)
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "vector_server.cli",
            "serve",
            "--host",
            host,
            "--port",
            str(port),
            "--wal",
            str(wal_path),
        ],
        cwd=BRANCH_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_server(host, port)
    return proc


def _generate_vectors(num_vectors: int, dimensions: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    matrix = rng.standard_normal((num_vectors, dimensions)).astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return matrix / norms


def _load_collection(
    client: VectorStateClient,
    collection: str,
    vectors: np.ndarray,
    *,
    filter_key: str,
    filtered_buckets: int,
    bucket_scenarios: list[int],
) -> None:
    client.send({
        "cmd": "VECTOR_CREATE",
        "collection": collection,
        "dimensions": int(vectors.shape[1]),
        "metric": "cosine",
    })
    for index, vector in enumerate(vectors):
        client.send({
            "cmd": "VECTOR_UPSERT",
            "collection": collection,
            "id": f"vec_{index}",
            "vector": vector.tolist(),
            "metadata": {
                "index": index,
                filter_key: f"{filter_key}:{index % max(filtered_buckets, 1)}",
                **{
                    f"{filter_key}_b{bucket_count}": f"{filter_key}:{index % bucket_count}"
                    for bucket_count in bucket_scenarios
                },
            },
        })


def _benchmark_hnsw_build(
    client: VectorStateClient,
    collection: str,
    *,
    M: int,
    ef_construction: int,
    ef_search: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    response = client.send({
        "cmd": "VECTOR_BUILD_HNSW",
        "collection": collection,
        "M": M,
        "ef_construction": ef_construction,
        "ef_search": ef_search,
    })
    duration = time.perf_counter() - started
    return {
        "duration_seconds": duration,
        "result": response.get("result", {}),
    }


def _benchmark_concurrent_queries(
    host: str,
    port: int,
    collection: str,
    queries: np.ndarray,
    *,
    workers: int,
    top_k: int,
    ann_probe_count: int | None,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latencies_ms: list[float] = []
    errors = 0
    diagnostics_list: list[dict[str, Any]] = []

    def worker(worker_id: int, worker_queries: np.ndarray) -> dict[str, Any]:
        worker_latencies: list[float] = []
        worker_errors = 0
        worker_diagnostics: list[dict[str, Any]] = []
        client = VectorStateClient(host, port)
        client.connect()
        try:
            for query in worker_queries:
                started = time.perf_counter()
                try:
                    response = client.send({
                        "cmd": "VECTOR_QUERY",
                        "collection": collection,
                        "vector": query.tolist(),
                        "top_k": top_k,
                        "strategy": "hnsw",
                        "include_diagnostics": filters is not None,
                        **({"filters": filters} if filters is not None else {}),
                        **({"ann_probe_count": ann_probe_count} if ann_probe_count is not None else {}),
                    })
                    result = response.get("result")
                    if filters is not None and isinstance(result, dict):
                        diag = result.get("diagnostics")
                        if isinstance(diag, dict):
                            worker_diagnostics.append(diag)
                except Exception as exc:  # pragma: no cover - surfaced in report
                    worker_errors += 1
                    log.error("concurrent_hnsw_query_failed", worker_id=worker_id, error=str(exc))
                finally:
                    worker_latencies.append((time.perf_counter() - started) * 1000.0)
        finally:
            client.close()
        return {"latencies_ms": worker_latencies, "errors": worker_errors, "diagnostics": worker_diagnostics}

    query_chunks = np.array_split(queries, workers)
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(worker, worker_id, chunk)
            for worker_id, chunk in enumerate(query_chunks)
            if chunk.size > 0
        ]
        for future in as_completed(futures):
            worker_result = future.result()
            latencies_ms.extend(worker_result["latencies_ms"])
            errors += int(worker_result["errors"])
            diagnostics_list.extend(worker_result.get("diagnostics", []))
    duration = time.perf_counter() - started

    query_count = len(latencies_ms)
    result = {
        "workers": workers,
        "queries": query_count,
        "duration_seconds": duration,
        "throughput_qps": query_count / duration if duration > 0 else 0.0,
        "latency_p50_ms": statistics.median(latencies_ms) if latencies_ms else 0.0,
        "latency_p95_ms": _percentile(latencies_ms, 95.0),
        "latency_p99_ms": _percentile(latencies_ms, 99.0),
        "error_count": errors,
    }
    if diagnostics_list:
        fallback_reason_counts: dict[str, int] = {}
        for diag in diagnostics_list:
            reason = diag.get("fallback_reason")
            if reason:
                fallback_reason_counts[str(reason)] = fallback_reason_counts.get(str(reason), 0) + 1
        result["diagnostics"] = {
            "queries_with_diagnostics": len(diagnostics_list),
            "fallback_count": sum(1 for diag in diagnostics_list if bool(diag.get("fallback_used"))),
            "fallback_reason_counts": fallback_reason_counts,
            "requested_hnsw_hits": _summarize_int_series([
                int(diag.get("requested_hnsw_hits", 0)) for diag in diagnostics_list
            ]),
            "adapted_requested_hnsw_hits": _summarize_int_series([
                int(diag.get("adapted_requested_hnsw_hits", 0) or diag.get("requested_hnsw_hits", 0))
                for diag in diagnostics_list
            ]),
            "success_hnsw_hit_ceiling": _summarize_int_series([
                int(diag.get("success_hnsw_hit_ceiling", 0)) for diag in diagnostics_list
            ]),
            "active_candidate_count": _summarize_int_series([
                int(diag.get("active_candidate_count", 0)) for diag in diagnostics_list
            ]),
            "filtered_candidate_count": _summarize_int_series([
                int(diag.get("filtered_candidate_count", 0)) for diag in diagnostics_list
            ]),
            "hnsw_raw_hit_count": _summarize_int_series([
                int(diag.get("hnsw_raw_hit_count", 0)) for diag in diagnostics_list
            ]),
            "hnsw_filtered_hit_count": _summarize_int_series([
                int(diag.get("hnsw_filtered_hit_count", 0)) for diag in diagnostics_list
            ]),
            "fallback_candidate_count": _summarize_int_series([
                int(diag.get("fallback_candidate_count", 0)) for diag in diagnostics_list
            ]),
            "scored_candidate_count": _summarize_int_series([
                int(diag.get("scored_candidate_count", 0)) for diag in diagnostics_list
            ]),
        }
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure Branch 33 HNSW rebuild and concurrent ANN hotspots")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9644)
    parser.add_argument("--num-vectors", type=int, default=4000)
    parser.add_argument("--dimensions", type=int, default=128)
    parser.add_argument("--num-queries", type=int, default=512)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--M", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=200)
    parser.add_argument("--ef-search", type=int, default=64)
    parser.add_argument("--ann-probe-count", type=int, default=None)
    parser.add_argument("--filter-key", default="tenant")
    parser.add_argument("--filtered-buckets", type=int, default=8)
    parser.add_argument("--filtered-bucket-index", type=int, default=0)
    parser.add_argument("--filtered-bucket-scenarios", default=None)
    parser.add_argument(
        "--wal-path",
        type=Path,
        default=BRANCH_ROOT / "tmp_benchmark_outputs" / "vector_hotspots.wal",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=BRANCH_ROOT / "reports" / "vector_hotspots.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.wal_path.parent.mkdir(parents=True, exist_ok=True)
    bucket_scenarios = _parse_bucket_scenarios(args.filtered_bucket_scenarios, args.filtered_buckets)

    vectors = _generate_vectors(args.num_vectors, args.dimensions, args.seed)
    queries = _generate_vectors(args.num_queries, args.dimensions, args.seed + 1)

    server_proc = _start_vector_server(args.host, args.port, args.wal_path)
    try:
        client = VectorStateClient(args.host, args.port)
        client.connect()
        try:
            _load_collection(
                client,
                "hotspot_bench",
                vectors,
                filter_key=args.filter_key,
                filtered_buckets=args.filtered_buckets,
                bucket_scenarios=bucket_scenarios,
            )
            build_stats = _benchmark_hnsw_build(
                client,
                "hotspot_bench",
                M=args.M,
                ef_construction=args.ef_construction,
                ef_search=args.ef_search,
            )
        finally:
            client.close()

        query_stats = _benchmark_concurrent_queries(
            args.host,
            args.port,
            "hotspot_bench",
            queries,
            workers=args.workers,
            top_k=args.top_k,
            ann_probe_count=args.ann_probe_count,
        )
        filtered_queries: list[dict[str, Any]] = []
        canonical_filtered_query: dict[str, Any] | None = None
        for bucket_count in bucket_scenarios:
            scenario_filter_key = args.filter_key if bucket_count == args.filtered_buckets else f"{args.filter_key}_b{bucket_count}"
            scenario = {
                "filter_key": scenario_filter_key,
                "bucket_count": bucket_count,
                "bucket_index": args.filtered_bucket_index % bucket_count,
                "stats": _benchmark_concurrent_queries(
                    args.host,
                    args.port,
                    "hotspot_bench",
                    queries,
                    workers=args.workers,
                    top_k=args.top_k,
                    ann_probe_count=args.ann_probe_count,
                    filters={scenario_filter_key: f"{args.filter_key}:{args.filtered_bucket_index % bucket_count}"},
                ),
            }
            filtered_queries.append(scenario)
            if bucket_count == args.filtered_buckets:
                canonical_filtered_query = scenario
    finally:
        server_proc.terminate()
        server_proc.wait(timeout=5)

    report = {
        "timestamp": time.time(),
        "vectors": args.num_vectors,
        "dimensions": args.dimensions,
        "top_k": args.top_k,
        "build": build_stats,
        "concurrent_hnsw_query": query_stats,
        "filtered_query": canonical_filtered_query,
        "filtered_queries": filtered_queries,
    }
    args.output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()