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

BRANCH_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
if str(BRANCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BRANCH_ROOT))

from tools.acp_logging import get_logger
from benchmark_vs_competitors import (  # noqa: E402
    RedisClient,
    UniversalStateClient,
    benchmark_pipeline_kv_operations,
    check_redis_available,
)


log = get_logger("branch33_universal_hotspots")


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return float(ordered[index])


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
    raise RuntimeError(f"UniversalStateServer did not become ready on {host}:{port}")


def _start_universal_server(host: str, port: int, wal_path: Path, durability_mode: str) -> subprocess.Popen[bytes]:
    wal_path.unlink(missing_ok=True)
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "universal_server.cli",
            "serve",
            "--host",
            host,
            "--port",
            str(port),
            "--wal",
            str(wal_path),
            "--durability-mode",
            durability_mode,
        ],
        cwd=BRANCH_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_server(host, port)
    return proc


def _preload_keys(host: str, port: int, key_space: int, worker_count: int) -> None:
    client = UniversalStateClient(host, port, codec="compact")
    client.connect()
    try:
        for key_index in range(key_space):
            client.send({"cmd": "SET", "key": f"hot:{key_index}", "value": {"seed": key_index}})
        for worker_id in range(worker_count):
            client.send({"cmd": "SET", "key": f"counter:{worker_id}", "value": 0})
    finally:
        client.close()


def run_mixed_load_benchmark(
    host: str,
    port: int,
    *,
    clients: int,
    ops_per_client: int,
    key_space: int,
) -> dict[str, Any]:
    _preload_keys(host, port, key_space, clients)

    op_names = ["SET", "GET", "INCR", "MSET", "MGET", "MINCR"]

    def worker(worker_id: int) -> dict[str, Any]:
        latencies_ms: list[float] = []
        errors = 0
        logical_ops = 0
        client = UniversalStateClient(host, port, codec="compact")
        client.connect()
        try:
            for op_index in range(ops_per_client):
                slot = op_index % len(op_names)
                key_id = (worker_id * ops_per_client + op_index) % key_space
                started = time.perf_counter()
                try:
                    if slot == 0:
                        client.send({
                            "cmd": "SET",
                            "key": f"hot:{key_id}",
                            "value": {"worker": worker_id, "op": op_index, "kind": "set"},
                        })
                        logical_ops += 1
                    elif slot == 1:
                        client.send({"cmd": "GET", "key": f"hot:{key_id}"})
                        logical_ops += 1
                    elif slot == 2:
                        client.send({"cmd": "INCR", "key": f"counter:{worker_id % 32}", "amount": 1})
                        logical_ops += 1
                    elif slot == 3:
                        values = {
                            f"hot:{(key_id + offset) % key_space}": {
                                "worker": worker_id,
                                "op": op_index,
                                "offset": offset,
                            }
                            for offset in range(4)
                        }
                        client.send({"cmd": "MSET", "values": values})
                        logical_ops += len(values)
                    elif slot == 4:
                        keys = [f"hot:{(key_id + offset) % key_space}" for offset in range(4)]
                        client.send({"cmd": "MGET", "keys": keys})
                        logical_ops += len(keys)
                    else:
                        updates = [
                            {"key": f"counter:{(worker_id + offset) % 32}", "amount": 1}
                            for offset in range(4)
                        ]
                        client.send({"cmd": "MINCR", "updates": updates})
                        logical_ops += len(updates)
                except Exception as exc:  # pragma: no cover - surfaced in report
                    errors += 1
                    log.error("mixed_load_operation_failed", worker_id=worker_id, op_index=op_index, error=str(exc))
                finally:
                    latencies_ms.append((time.perf_counter() - started) * 1000.0)
        finally:
            client.close()
        return {"latencies_ms": latencies_ms, "errors": errors, "logical_ops": logical_ops}

    started = time.perf_counter()
    worker_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=clients) as executor:
        futures = [executor.submit(worker, worker_id) for worker_id in range(clients)]
        for future in as_completed(futures):
            worker_results.append(future.result())
    duration = time.perf_counter() - started

    latencies_ms = [
        latency
        for worker_result in worker_results
        for latency in worker_result["latencies_ms"]
    ]
    request_count = len(latencies_ms)
    logical_ops = sum(int(worker_result["logical_ops"]) for worker_result in worker_results)
    errors = sum(int(worker_result["errors"]) for worker_result in worker_results)

    return {
        "requests": request_count,
        "logical_ops": logical_ops,
        "clients": clients,
        "ops_per_client": ops_per_client,
        "duration_seconds": duration,
        "throughput_requests_per_sec": request_count / duration if duration > 0 else 0.0,
        "throughput_logical_ops_per_sec": logical_ops / duration if duration > 0 else 0.0,
        "latency_p50_ms": statistics.median(latencies_ms) if latencies_ms else 0.0,
        "latency_p95_ms": _percentile(latencies_ms, 95.0),
        "latency_p99_ms": _percentile(latencies_ms, 99.0),
        "error_count": errors,
    }


def run_pipeline_benchmarks(
    host: str,
    port: int,
    *,
    pipeline_batches: int,
    pipeline_batch_size: int,
) -> dict[str, Any]:
    universal = benchmark_pipeline_kv_operations(
        lambda: UniversalStateClient(host, port, codec="compact"),
        "UniversalStateServer",
        num_batches=pipeline_batches,
        batch_size=pipeline_batch_size,
    )
    result: dict[str, Any] = {"universal": universal.__dict__}
    if check_redis_available():
        redis = benchmark_pipeline_kv_operations(
            lambda: RedisClient("localhost", 6379),
            "Redis",
            num_batches=pipeline_batches,
            batch_size=pipeline_batch_size,
        )
        result["redis"] = redis.__dict__
        result["throughput_ratio_vs_redis"] = (
            universal.throughput_ops_sec / redis.throughput_ops_sec if redis.throughput_ops_sec > 0 else 0.0
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure Branch 33 Universal tail latency and pipeline hotspots")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9633)
    parser.add_argument("--clients", type=int, default=48)
    parser.add_argument("--ops-per-client", type=int, default=1500)
    parser.add_argument("--key-space", type=int, default=4096)
    parser.add_argument("--pipeline-batches", type=int, default=1000)
    parser.add_argument("--pipeline-batch-size", type=int, default=16)
    parser.add_argument("--durability-mode", choices=["safe", "fast", "strict"], default="fast")
    parser.add_argument(
        "--wal-path",
        type=Path,
        default=BRANCH_ROOT / "tmp_benchmark_outputs" / "universal_hotspots.wal",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=BRANCH_ROOT / "reports" / "universal_hotspots.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.wal_path.parent.mkdir(parents=True, exist_ok=True)

    server_proc = _start_universal_server(args.host, args.port, args.wal_path, args.durability_mode)
    try:
        mixed_load = run_mixed_load_benchmark(
            args.host,
            args.port,
            clients=args.clients,
            ops_per_client=args.ops_per_client,
            key_space=args.key_space,
        )
        pipeline = run_pipeline_benchmarks(
            args.host,
            args.port,
            pipeline_batches=args.pipeline_batches,
            pipeline_batch_size=args.pipeline_batch_size,
        )
    finally:
        server_proc.terminate()
        server_proc.wait(timeout=5)

    report = {
        "timestamp": time.time(),
        "host": args.host,
        "port": args.port,
        "durability_mode": args.durability_mode,
        "wire_codec": "compact",
        "mixed_load": mixed_load,
        "pipeline": pipeline,
    }
    args.output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()