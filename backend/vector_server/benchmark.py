from __future__ import annotations

import gzip
import bz2
from concurrent.futures import ThreadPoolExecutor
import json
import lzma
import logging
import tempfile
import time
import hashlib
from pathlib import Path
from typing import Any

import numpy as np

from .engine import VectorStateEngineV2

try:
    from tools.acp_logging import get_logger
except ModuleNotFoundError:  # pragma: no cover
    class _CompatLogger:
        def __init__(self, logger: logging.Logger) -> None:
            self._logger = logger

        def info(self, message: str, **fields: Any) -> None:
            if fields:
                self._logger.info("%s | %s", message, fields)
                return
            self._logger.info(message)

    def get_logger(name: str) -> _CompatLogger:
        logger = logging.getLogger(name)
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO)
        return _CompatLogger(logger)

try:
    import orjson as _orjson

    def _json_dumps(obj: Any) -> bytes:
        return _orjson.dumps(obj)
except ModuleNotFoundError:  # pragma: no cover
    def _json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")


log = get_logger("branch33_vector_benchmark")


def _build_vectors(count: int, dimensions: int, seed: int = 33) -> np.ndarray:
    rng = np.random.default_rng(seed)
    vectors = rng.normal(size=(count, dimensions)).astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return vectors / norms


def _cosine_scores(matrix: np.ndarray, query: np.ndarray) -> np.ndarray:
    return matrix @ query


def _compress_record(raw: bytes) -> bytes:
    try:
        import zstandard as zstd

        return zstd.ZstdCompressor(level=1).compress(raw)
    except ModuleNotFoundError:  # pragma: no cover
        return gzip.compress(raw, compresslevel=6)


def _maybe_import_zstd() -> Any | None:
    try:
        import zstandard as zstd

        return zstd
    except ModuleNotFoundError:  # pragma: no cover
        return None


def _vector_block_bytes(vectors: np.ndarray) -> bytes:
    return np.ascontiguousarray(vectors.astype(np.float32, copy=False)).tobytes()


def _float32_shuffle_bytes(vectors: np.ndarray) -> bytes:
    array = np.ascontiguousarray(vectors.astype(np.float32, copy=False))
    byte_view = array.view(np.uint8).reshape(array.shape[0], array.shape[1], 4)
    shuffled = np.transpose(byte_view, (2, 0, 1)).copy()
    return shuffled.tobytes()


def _float32_unshuffle_bytes(data: bytes, rows: int, dimensions: int) -> bytes:
    array = np.frombuffer(data, dtype=np.uint8)
    reshaped = array.reshape(4, rows, dimensions)
    unshuffled = np.transpose(reshaped, (1, 2, 0)).copy()
    return unshuffled.tobytes()


def _compress_none(raw: bytes) -> bytes:
    return raw


def _decompress_none(data: bytes) -> bytes:
    return data


def _compress_gzip(raw: bytes, *, level: int) -> bytes:
    return gzip.compress(raw, compresslevel=level)


def _decompress_gzip(data: bytes) -> bytes:
    return gzip.decompress(data)


def _compress_bz2(raw: bytes, *, level: int) -> bytes:
    return bz2.compress(raw, compresslevel=level)


def _decompress_bz2(data: bytes) -> bytes:
    return bz2.decompress(data)


def _compress_lzma(raw: bytes, *, preset: int) -> bytes:
    return lzma.compress(raw, preset=preset)


def _decompress_lzma(data: bytes) -> bytes:
    return lzma.decompress(data)


def _compress_zstd(raw: bytes, *, level: int) -> bytes:
    zstd = _maybe_import_zstd()
    if zstd is None:
        return gzip.compress(raw, compresslevel=6)
    return zstd.ZstdCompressor(level=level).compress(raw)


def _decompress_zstd(data: bytes) -> bytes:
    zstd = _maybe_import_zstd()
    if zstd is None:
        return gzip.decompress(data)
    return zstd.ZstdDecompressor().decompress(data)


def _profile_specs() -> list[dict[str, Any]]:
    return [
        {
            "name": "packed_raw",
            "transform": "packed_float32",
            "codec": "none",
            "encode": _compress_none,
            "decode": _decompress_none,
        },
        {
            "name": "packed_gzip_6",
            "transform": "packed_float32",
            "codec": "gzip-6",
            "encode": lambda raw: _compress_gzip(raw, level=6),
            "decode": _decompress_gzip,
        },
        {
            "name": "packed_bz2_9",
            "transform": "packed_float32",
            "codec": "bzip2-9",
            "encode": lambda raw: _compress_bz2(raw, level=9),
            "decode": _decompress_bz2,
        },
        {
            "name": "packed_lzma_9",
            "transform": "packed_float32",
            "codec": "lzma-9",
            "encode": lambda raw: _compress_lzma(raw, preset=9),
            "decode": _decompress_lzma,
        },
        {
            "name": "packed_zstd_1",
            "transform": "packed_float32",
            "codec": "zstd-1",
            "encode": lambda raw: _compress_zstd(raw, level=1),
            "decode": _decompress_zstd,
        },
        {
            "name": "packed_zstd_22",
            "transform": "packed_float32",
            "codec": "zstd-22",
            "encode": lambda raw: _compress_zstd(raw, level=22),
            "decode": _decompress_zstd,
        },
        {
            "name": "shuffle_zstd_22",
            "transform": "float32_shuffle",
            "codec": "zstd-22",
            "encode": lambda raw: _compress_zstd(raw, level=22),
            "decode": _decompress_zstd,
        },
        {
            "name": "shuffle_lzma_9",
            "transform": "float32_shuffle",
            "codec": "lzma-9",
            "encode": lambda raw: _compress_lzma(raw, preset=9),
            "decode": _decompress_lzma,
        },
    ]


def _run_single_compression_profile(
    spec: dict[str, Any],
    *,
    vectors: np.ndarray,
    packed_bytes: bytes,
    shuffled_bytes: bytes,
) -> dict[str, Any]:
    raw_bytes = shuffled_bytes if spec["transform"] == "float32_shuffle" else packed_bytes
    raw_size = len(raw_bytes)
    source_hash = hashlib.sha256(packed_bytes).hexdigest()

    encode_started = time.perf_counter()
    compressed = spec["encode"](raw_bytes)
    encode_seconds = max(time.perf_counter() - encode_started, 1e-9)

    decode_started = time.perf_counter()
    restored = spec["decode"](compressed)
    decode_seconds = max(time.perf_counter() - decode_started, 1e-9)

    if spec["transform"] == "float32_shuffle":
        restored = _float32_unshuffle_bytes(restored, int(vectors.shape[0]), int(vectors.shape[1]))

    restored_hash = hashlib.sha256(restored).hexdigest()
    lossless_match = restored == packed_bytes
    compressed_size = len(compressed)
    savings_pct = 100.0 * (1.0 - (compressed_size / max(raw_size, 1)))
    encode_mb_s = (raw_size / 1_048_576.0) / encode_seconds
    decode_mb_s = (raw_size / 1_048_576.0) / decode_seconds

    return {
        "profile": spec["name"],
        "transform": spec["transform"],
        "codec": spec["codec"],
        "input_bytes": raw_size,
        "compressed_bytes": compressed_size,
        "compression_ratio": round(raw_size / max(compressed_size, 1), 4),
        "savings_pct": round(savings_pct, 4),
        "encode_seconds": round(encode_seconds, 6),
        "decode_seconds": round(decode_seconds, 6),
        "encode_mb_s": round(encode_mb_s, 4),
        "decode_mb_s": round(decode_mb_s, 4),
        "lossless_match": lossless_match,
        "source_sha256": source_hash,
        "restored_sha256": restored_hash,
    }


def run_vector_acp_compression_matrix(
    *,
    vector_count: int = 10_000,
    dimensions: int = 768,
    seed: int = 33,
) -> dict[str, Any]:
    if vector_count <= 0:
        raise ValueError("vector_count must be > 0")
    if dimensions <= 0:
        raise ValueError("dimensions must be > 0")

    vectors = _build_vectors(vector_count, dimensions, seed=seed)
    packed_bytes = _vector_block_bytes(vectors)
    shuffled_bytes = _float32_shuffle_bytes(vectors)

    specs = _profile_specs()
    max_workers = min(len(specs), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _run_single_compression_profile,
                spec,
                vectors=vectors,
                packed_bytes=packed_bytes,
                shuffled_bytes=shuffled_bytes,
            )
            for spec in specs
        ]
        rows = [future.result() for future in futures]

    lossless_rows = [row for row in rows if row["lossless_match"]]
    compressed_rows = [row for row in lossless_rows if row["codec"] != "none"]
    best_by_ratio = max(lossless_rows, key=lambda row: (row["compression_ratio"], row["decode_mb_s"]))
    best_by_decode = max(lossless_rows, key=lambda row: (row["decode_mb_s"], row["compression_ratio"]))
    fastest_compressed = max(compressed_rows, key=lambda row: (row["decode_mb_s"], row["compression_ratio"]))
    best_balanced_compressed = max(
        compressed_rows,
        key=lambda row: (row["compression_ratio"] * row["decode_mb_s"], row["encode_mb_s"]),
    )

    return {
        "vector_count": vector_count,
        "dimensions": dimensions,
        "input_bytes": len(packed_bytes),
        "profiles": rows,
        "winners": {
            "hot_path": best_by_decode["profile"],
            "max_compression": best_by_ratio["profile"],
            "max_decode_speed": best_by_decode["profile"],
            "fastest_compressed": fastest_compressed["profile"],
            "balanced_compressed": best_balanced_compressed["profile"],
        },
        "recommended_profiles": {
            "hot_path": {
                "profile": best_by_decode["profile"],
                "transform": best_by_decode["transform"],
                "codec": best_by_decode["codec"],
                "compression_ratio": best_by_decode["compression_ratio"],
                "decode_mb_s": best_by_decode["decode_mb_s"],
                "savings_pct": best_by_decode["savings_pct"],
            },
            "warm_path": {
                "profile": best_balanced_compressed["profile"],
                "transform": best_balanced_compressed["transform"],
                "codec": best_balanced_compressed["codec"],
                "compression_ratio": best_balanced_compressed["compression_ratio"],
                "decode_mb_s": best_balanced_compressed["decode_mb_s"],
                "savings_pct": best_balanced_compressed["savings_pct"],
            },
            "cold_path": {
                "profile": best_by_ratio["profile"],
                "transform": best_by_ratio["transform"],
                "codec": best_by_ratio["codec"],
                "compression_ratio": best_by_ratio["compression_ratio"],
                "decode_mb_s": best_by_ratio["decode_mb_s"],
                "savings_pct": best_by_ratio["savings_pct"],
            },
        },
        "notes": {
            "scope": (
                "This ACP matrix measures lossless compression over contiguous packed float32 vector segments, "
                "which is the correct experiment for warm or cold vector-store blocks rather than the live upsert WAL path."
            ),
            "hot_path": "Keep live writes on the lighter packed exact tier; use compressed winners here for compacted segments or snapshots.",
        },
    }


def run_vector_acp_compression_suite(
    configs: list[tuple[int, int]] | None = None,
    *,
    seed: int = 33,
) -> list[dict[str, Any]]:
    if configs is None:
        configs = [(10_000, 384), (10_000, 768), (10_000, 1536)]

    max_workers = min(len(configs), 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                run_vector_acp_compression_matrix,
                vector_count=vector_count,
                dimensions=dimensions,
                seed=seed,
            )
            for vector_count, dimensions in configs
        ]
        return [future.result() for future in futures]


def _legacy_cosine(left: list[float], right: list[float]) -> float:
    left_array = np.asarray(left, dtype=np.float32)
    right_array = np.asarray(right, dtype=np.float32)
    left_norm = float(np.linalg.norm(left_array))
    right_norm = float(np.linalg.norm(right_array))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return float((left_array @ right_array) / (left_norm * right_norm))


def _run_legacy_json_exact(vectors: np.ndarray, query_count: int, top_k: int) -> dict[str, Any]:
    items: dict[str, dict[str, Any]] = {}
    wal_size_bytes = 0

    t0 = time.perf_counter()
    for index, vector in enumerate(vectors):
        item_id = f"chunk-{index}"
        vector_list = [float(value) for value in vector]
        items[item_id] = {
            "vector": vector_list,
            "metadata": {"tenant": "acme", "bucket": index % 8},
            "payload": {"text": f"chunk {index}"},
        }
        record = {
            "op": "upsert",
            "collection": "memory",
            "id": item_id,
            "vector": vector_list,
            "metadata": items[item_id]["metadata"],
            "payload": items[item_id]["payload"],
            "expires_at": None,
            "ts": 0.0,
        }
        wal_size_bytes += len(_compress_record(_json_dumps(record)))
    ingest_seconds = max(time.perf_counter() - t0, 1e-9)

    t1 = time.perf_counter()
    top_hit = None
    for query in vectors[:query_count]:
        query_list = [float(value) for value in query]
        hits = [
            (_legacy_cosine(query_list, item["vector"]), item_id)
            for item_id, item in items.items()
        ]
        hits.sort(key=lambda entry: entry[0], reverse=True)
        if top_hit is None and hits:
            top_hit = hits[0][1]
    query_seconds = max(time.perf_counter() - t1, 1e-9)

    return {
        "ingest_seconds": round(ingest_seconds, 6),
        "ingest_vectors_per_sec": round(len(vectors) / ingest_seconds, 2),
        "query_seconds": round(query_seconds, 6),
        "query_qps": round(query_count / query_seconds, 2),
        "query_top_k": top_k,
        "top_hit": top_hit,
        "wal_size_bytes": wal_size_bytes,
        "collections": 1,
        "vectors": len(items),
        "note": "legacy JSON float-array persistence plus Python-list exact scan",
    }


def _run_packed_exact(root: Path, vectors: np.ndarray, query_count: int, top_k: int) -> dict[str, Any]:
    wal = root / "vector_v2.wal"
    collection = "memory"

    with VectorStateEngineV2(wal) as engine:
        engine.create_collection(collection, int(vectors.shape[1]), metric="cosine")

        t0 = time.perf_counter()
        for index, vector in enumerate(vectors):
            engine.upsert(
                collection,
                f"chunk-{index}",
                vector.tolist(),
                metadata={"tenant": "acme", "bucket": index % 8},
                payload={"text": f"chunk {index}"},
            )
        ingest_seconds = max(time.perf_counter() - t0, 1e-9)
        engine.flush()

        query_vectors = vectors[:query_count]
        t1 = time.perf_counter()
        top_hit = None
        for query in query_vectors:
            hits = engine.query(collection, query.tolist(), top_k=top_k)
            if top_hit is None:
                top_hit = hits[0]["id"] if hits else None
        query_seconds = max(time.perf_counter() - t1, 1e-9)
        stats = engine.stats()

    return {
        "ingest_seconds": round(ingest_seconds, 6),
        "ingest_vectors_per_sec": round(len(vectors) / ingest_seconds, 2),
        "query_seconds": round(query_seconds, 6),
        "query_qps": round(query_count / query_seconds, 2),
        "query_top_k": top_k,
        "top_hit": top_hit,
        "wal_size_bytes": stats.wal_size_bytes,
        "collections": stats.collections,
        "vectors": stats.vectors,
        "note": "packed float32 persistence plus contiguous NumPy exact scan",
    }


def _run_numpy_exact(vectors: np.ndarray, query_count: int, top_k: int) -> dict[str, Any]:
    query_vectors = vectors[:query_count]
    t0 = time.perf_counter()
    top_hit = None
    for query in query_vectors:
        scores = _cosine_scores(vectors, query)
        best = np.argsort(scores)[-top_k:][::-1]
        if top_hit is None:
            top_hit = f"chunk-{int(best[0])}"
    query_seconds = max(time.perf_counter() - t0, 1e-9)
    return {
        "query_seconds": round(query_seconds, 6),
        "query_qps": round(query_count / query_seconds, 2),
        "query_top_k": top_k,
        "top_hit": top_hit,
        "note": "in-memory NumPy exact cosine scan",
    }


def _run_hnswlib(vectors: np.ndarray, query_count: int, top_k: int) -> dict[str, Any] | dict[str, str]:
    try:
        import hnswlib  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return {"status": "skipped", "reason": "hnswlib not installed"}

    index = hnswlib.Index(space="cosine", dim=int(vectors.shape[1]))
    index.init_index(max_elements=len(vectors), ef_construction=100, M=16)

    t0 = time.perf_counter()
    index.add_items(vectors, np.arange(len(vectors)))
    ingest_seconds = max(time.perf_counter() - t0, 1e-9)

    index.set_ef(max(50, top_k))
    query_vectors = vectors[:query_count]
    t1 = time.perf_counter()
    labels, _distances = index.knn_query(query_vectors, k=top_k)
    query_seconds = max(time.perf_counter() - t1, 1e-9)

    return {
        "status": "measured",
        "ingest_seconds": round(ingest_seconds, 6),
        "ingest_vectors_per_sec": round(len(vectors) / ingest_seconds, 2),
        "query_seconds": round(query_seconds, 6),
        "query_qps": round(query_count / query_seconds, 2),
        "query_top_k": top_k,
        "top_hit": int(labels[0][0]),
        "note": "hnswlib approximate cosine index",
    }


def run_vector_benchmark(
    *,
    vector_count: int = 2_000,
    dimensions: int = 128,
    query_count: int = 200,
    top_k: int = 5,
) -> dict[str, Any]:
    """Run a local benchmark for the separate vector-capable v2 runtime.

    This benchmark is intentionally separate from the existing Branch 33
    `SET + GET + INCR` benchmark. It measures vector ingest and vector query
    throughput on exact cosine search and compares that against runnable local
    baselines in the current environment.
    """

    if vector_count <= 0:
        raise ValueError("vector_count must be > 0")
    if dimensions <= 0:
        raise ValueError("dimensions must be > 0")
    if query_count <= 0 or query_count > vector_count:
        raise ValueError("query_count must be > 0 and <= vector_count")
    if top_k <= 0:
        raise ValueError("top_k must be > 0")

    log.info(
        "Running vector formulation benchmark",
        vector_count=vector_count,
        dimensions=dimensions,
        query_count=query_count,
        top_k=top_k,
    )

    vectors = _build_vectors(vector_count, dimensions)
    legacy_json = _run_legacy_json_exact(vectors, query_count, top_k)

    with tempfile.TemporaryDirectory(prefix="ahana-vector-v2-bench-") as td:
        root = Path(td)
        packed_exact = _run_packed_exact(root, vectors, query_count, top_k)

    numpy_exact = _run_numpy_exact(vectors, query_count, top_k)
    hnswlib_result = _run_hnswlib(vectors, query_count, top_k)

    comparisons: dict[str, Any] = {
        "packed_exact_vs_legacy_query_speedup": round(
            packed_exact["query_qps"] / max(legacy_json["query_qps"], 1e-9), 3
        ),
        "packed_exact_vs_legacy_ingest_speedup": round(
            packed_exact["ingest_vectors_per_sec"] / max(legacy_json["ingest_vectors_per_sec"], 1e-9), 3
        ),
        "legacy_vs_packed_wal_size_reduction": round(
            legacy_json["wal_size_bytes"] / max(packed_exact["wal_size_bytes"], 1), 3
        ),
        "numpy_exact_vs_packed_query_speedup": round(
            numpy_exact["query_qps"] / max(packed_exact["query_qps"], 1e-9), 3
        ),
        "packed_top_hit_matches_legacy": packed_exact["top_hit"] == legacy_json["top_hit"],
        "packed_top_hit_matches_numpy": packed_exact["top_hit"] == numpy_exact["top_hit"],
    }

    if hnswlib_result.get("status") == "measured":
        comparisons["hnswlib_vs_packed_query_speedup"] = round(
            hnswlib_result["query_qps"] / max(packed_exact["query_qps"], 1e-9), 3
        )
        comparisons["hnswlib_vs_packed_ingest_speedup"] = round(
            hnswlib_result["ingest_vectors_per_sec"] / max(packed_exact["ingest_vectors_per_sec"], 1e-9), 3
        )

    result = {
        "vector_count": vector_count,
        "dimensions": dimensions,
        "query_count": query_count,
        "top_k": top_k,
        "legacy_json_exact": legacy_json,
        "packed_exact": packed_exact,
        "v2": packed_exact,
        "numpy_exact": numpy_exact,
        "hnswlib": hnswlib_result,
        "comparisons": comparisons,
        "notes": {
            "comparison_boundary": (
                "The original Branch 33 benchmark measures key-value, counter, and WAL durability throughput. "
                "This vector benchmark measures semantic ingest and search, so cross-benchmark comparisons "
                "should be treated as workload context rather than an apples-to-apples winner table."
            ),
            "preferred_formulation": (
                "Packed float32 persistence plus contiguous NumPy exact scan is the first Ahana-aligned upgrade. "
                "It preserves exactness, removes JSON float-array overhead, and establishes a clean baseline for any later ANN accelerator."
            )
        },
    }

    log.info(
        "Completed vector formulation benchmark",
        vector_count=vector_count,
        dimensions=dimensions,
        packed_query_qps=packed_exact["query_qps"],
        legacy_query_qps=legacy_json["query_qps"],
        packed_wal_size_bytes=packed_exact["wal_size_bytes"],
        legacy_wal_size_bytes=legacy_json["wal_size_bytes"],
    )
    return result


def run_vector_formulation_matrix(
    configs: list[tuple[int, int]] | None = None,
    *,
    query_count: int = 200,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    if configs is None:
        configs = [(10_000, 384), (10_000, 768), (10_000, 1536)]

    results: list[dict[str, Any]] = []
    for vector_count, dimensions in configs:
        effective_query_count = min(query_count, vector_count)
        results.append(
            run_vector_benchmark(
                vector_count=vector_count,
                dimensions=dimensions,
                query_count=effective_query_count,
                top_k=top_k,
            )
        )
    return results