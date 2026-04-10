from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any


def _run_engine(root: Path, iterations: int, durability_mode: str) -> tuple[float, "EngineStats"]:  # type: ignore[name-defined]
    from backend.state_engine import CompressedStateEngine

    wal = root / f"engine_{durability_mode}.wal"
    with CompressedStateEngine(wal, durability_mode=durability_mode) as engine:
        t0 = time.perf_counter()
        for index in range(iterations):
            key = f"k:{index % 2000}"
            engine.put(key, {"n": index, "tenant": "acme", "status": "ok"})
            _ = engine.get(key)
            _ = engine.incr("counter", amount=1)
        elapsed = max(time.perf_counter() - t0, 1e-9)
        engine.flush()
        stats = engine.stats()
    return elapsed, stats


def _run_sqlite(root: Path, iterations: int) -> tuple[float, int]:
    sqlite_file = root / "baseline.db"
    conn = sqlite3.connect(str(sqlite_file))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT);")
    conn.execute("CREATE TABLE ctr (k TEXT PRIMARY KEY, v INTEGER);")
    conn.execute("INSERT INTO ctr(k, v) VALUES('counter', 0);")
    conn.commit()

    t1 = time.perf_counter()
    for index in range(iterations):
        key = f"k:{index % 2000}"
        value = f'{{\"n\":{index},\"tenant\":\"acme\",\"status\":\"ok\"}}'
        conn.execute(
            "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        conn.execute("UPDATE ctr SET v = v + 1 WHERE k='counter'")
    conn.commit()
    sqlite_seconds = max(time.perf_counter() - t1, 1e-9)
    conn.close()
    return sqlite_seconds, os.path.getsize(sqlite_file)


def run_benchmark(iterations: int = 20_000) -> dict[str, Any]:
    """Run a durability-aware benchmark against a local SQLite baseline.

    The engine is measured in its three supported runtime durability modes:

    - ``safe``   : single-record writes, OS-buffered
    - ``fast``   : batched writes, 16 records or 50 ms
    - ``strict`` : single-record writes with per-record flush + fsync

    SQLite is measured with ``journal_mode=WAL`` and ``synchronous=NORMAL``.
    """
    with tempfile.TemporaryDirectory(prefix="ahana-universal-bench-") as td:
        root = Path(td)

        safe_s, safe_stats = _run_engine(root, iterations, durability_mode="safe")
        fast_s, fast_stats = _run_engine(root, iterations, durability_mode="fast")
        strict_s, strict_stats = _run_engine(root, iterations, durability_mode="strict")
        sqlite_s, sqlite_size = _run_sqlite(root, iterations)

        ops = iterations * 3
        modes = {
            "safe": {
                "seconds": round(safe_s, 6),
                "ops_per_sec": round(ops / safe_s, 2),
                "wal_size_bytes": safe_stats.wal_size_bytes,
                "compression_ratio": round(safe_stats.compression_ratio, 4),
                "note": "Single-record writes, OS-buffered",
            },
            "fast": {
                "seconds": round(fast_s, 6),
                "ops_per_sec": round(ops / fast_s, 2),
                "wal_size_bytes": fast_stats.wal_size_bytes,
                "compression_ratio": round(fast_stats.compression_ratio, 4),
                "note": "Batch writes (16 records or 50 ms)",
            },
            "strict": {
                "seconds": round(strict_s, 6),
                "ops_per_sec": round(ops / strict_s, 2),
                "wal_size_bytes": strict_stats.wal_size_bytes,
                "compression_ratio": round(strict_stats.compression_ratio, 4),
                "note": "Single-record writes with flush + fsync",
            },
        }
        return {
            "iterations": iterations,
            "modes": modes,
            "engine": modes["safe"],
            "engine_fast": modes["fast"],
            "engine_sync": modes["strict"],
            "sqlite": {
                "seconds": round(sqlite_s, 6),
                "ops_per_sec": round(ops / sqlite_s, 2),
                "db_size_bytes": sqlite_size,
            },
            "comparisons": {
                "fast_vs_safe_speedup": round(safe_s / fast_s, 3),
                "fast_vs_strict_speedup": round(strict_s / fast_s, 3),
                "fast_vs_sqlite_speedup": round(sqlite_s / fast_s, 3),
                "safe_vs_sqlite_speedup": round(sqlite_s / safe_s, 3),
                "strict_vs_sqlite_speedup": round(sqlite_s / strict_s, 3),
                "wal_reduction_fast_vs_safe": round(
                    safe_stats.wal_size_bytes / max(fast_stats.wal_size_bytes, 1), 3
                ),
                "wal_reduction_fast_vs_strict": round(
                    strict_stats.wal_size_bytes / max(fast_stats.wal_size_bytes, 1), 3
                ),
            },
        }
