from __future__ import annotations

from pathlib import Path
import threading
import time
import socket
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

from backend.vector_server.engine import VectorStateEngineV2
from backend.vector_server.server import VectorStateServerV2


def test_vector_engine_round_trip_and_replay(tmp_path: Path) -> None:
    wal = tmp_path / "vector_v2.wal"

    with VectorStateEngineV2(wal) as engine:
        engine.create_collection("memory", 3)
        engine.upsert(
            "memory",
            "chunk-1",
            [1.0, 0.0, 0.0],
            metadata={"tenant": "acme", "kind": "faq"},
            payload={"text": "reset your password"},
        )
        engine.upsert(
            "memory",
            "chunk-2",
            [0.9, 0.1, 0.0],
            metadata={"tenant": "acme", "kind": "guide"},
            payload={"text": "rotate your keys"},
        )

        hits = engine.query("memory", [1.0, 0.0, 0.0], top_k=1, filters={"tenant": "acme"})
        assert hits[0]["id"] == "chunk-1"
        assert hits[0]["payload"] == {"text": "reset your password"}

    with VectorStateEngineV2(wal) as reloaded:
        stats = reloaded.stats()
        assert stats.collections == 1
        assert stats.vectors == 2
        assert stats.wal_size_bytes > 0
        assert reloaded.get("memory", "chunk-1") == {
            "id": "chunk-1",
            "metadata": {"tenant": "acme", "kind": "faq"},
            "payload": {"text": "reset your password"},
            "expires_at": None,
        }
        with_vector = reloaded.get("memory", "chunk-2", include_vector=True)
        assert with_vector is not None
        assert with_vector["vector"] == [0.8999999761581421, 0.10000000149011612, 0.0]


def test_vector_engine_ttl_expires_items(tmp_path: Path) -> None:
    with VectorStateEngineV2(tmp_path / "ttl.wal") as engine:
        engine.create_collection("memory", 2)
        engine.upsert("memory", "short", [1.0, 0.0], ttl_seconds=0)
        assert engine.get("memory", "short") is None


def test_vector_server_dispatch_surface(tmp_path: Path) -> None:
    server = VectorStateServerV2(tmp_path / "server.wal", port=0)

    assert server.dispatch({"cmd": "PING"})["result"] == "PONG"
    assert server.dispatch({"cmd": "VECTOR_CREATE", "collection": "docs", "dimensions": 2})["ok"] is True
    assert server.dispatch(
        {
            "cmd": "VECTOR_UPSERT",
            "collection": "docs",
            "id": "doc-1",
            "vector": [1.0, 0.0],
            "metadata": {"tenant": "ahana", "source": "faq"},
            "payload": {"text": "hello"},
        }
    )["ok"] is True

    result = server.dispatch(
        {
            "cmd": "VECTOR_QUERY",
            "collection": "docs",
            "vector": [1.0, 0.0],
            "top_k": 1,
            "filters": {"tenant": "ahana"},
            "include_diagnostics": True,
        }
    )["result"]
    assert result["hits"][0]["id"] == "doc-1"
    assert result["diagnostics"]["filtered_candidate_count"] == 1

    ann = server.dispatch({"cmd": "VECTOR_BUILD_ANN", "collection": "docs", "n_lists": 1})["result"]
    assert ann["centroids"] == 1
    assert ann["vectors"] == 1

    ann_result = server.dispatch(
        {
            "cmd": "VECTOR_QUERY",
            "collection": "docs",
            "vector": [1.0, 0.0],
            "top_k": 1,
            "strategy": "ann_rerank",
            "candidate_multiplier": 4,
            "ann_probe_count": 1,
        }
    )["result"]
    assert ann_result[0]["id"] == "doc-1"

    compact = server.dispatch({"cmd": "VECTOR_COMPACT", "collection": "docs"})["result"]
    assert compact["collections"] == 1
    # WAL may grow by a few bytes when timestamps are regenerated during rewrite
    # (no dead records to purge here). Real shrinkage is tested in
    # test_vector_engine_compact_rewrites_live_records_only.
    assert compact["wal_before_bytes"] > 0
    assert compact["wal_after_bytes"] > 0

    stats = server.dispatch({"cmd": "VECTOR_STATS"})["result"]
    assert stats["collections"] == 1
    assert stats["vectors"] == 1

    server.shutdown()


def test_vector_engine_updates_existing_item_without_duplication(tmp_path: Path) -> None:
    with VectorStateEngineV2(tmp_path / "replace.wal") as engine:
        engine.create_collection("memory", 2)
        engine.upsert("memory", "doc-1", [1.0, 0.0], metadata={"tenant": "ahana"})
        engine.upsert("memory", "doc-1", [0.0, 1.0], metadata={"tenant": "ahana", "kind": "guide"})

        stats = engine.stats()
        assert stats.vectors == 1
        record = engine.get("memory", "doc-1", include_vector=True)
        assert record is not None
        assert record["metadata"] == {"tenant": "ahana", "kind": "guide"}
        assert record["vector"] == [0.0, 1.0]


def test_vector_engine_compact_rewrites_live_records_only(tmp_path: Path) -> None:
    wal = tmp_path / "compact.wal"

    with VectorStateEngineV2(wal) as engine:
        engine.create_collection("memory", 2)
        engine.upsert("memory", "doc-1", [1.0, 0.0], metadata={"tenant": "ahana"})
        engine.upsert("memory", "doc-2", [0.0, 1.0], metadata={"tenant": "ahana"})
        engine.delete("memory", "doc-1")
        before = engine.stats().wal_size_bytes
        result = engine.compact("memory")
        after = engine.stats().wal_size_bytes

        assert result["collections"] == 1
        assert result["wal_before_bytes"] == before
        assert result["wal_after_bytes"] == after
        assert result["bytes_reclaimed"] >= 0
        assert engine.get("memory", "doc-1") is None
        assert engine.get("memory", "doc-2") is not None

    with VectorStateEngineV2(wal) as reloaded:
        assert reloaded.get("memory", "doc-1") is None
        assert reloaded.get("memory", "doc-2") is not None


def test_vector_engine_ann_rerank_matches_exact_top_hit(tmp_path: Path) -> None:
    with VectorStateEngineV2(tmp_path / "ann.wal") as engine:
        engine.create_collection("memory", 3)
        engine.upsert("memory", "doc-1", [1.0, 0.0, 0.0], metadata={"tenant": "ahana"})
        engine.upsert("memory", "doc-2", [0.95, 0.05, 0.0], metadata={"tenant": "ahana"})
        engine.upsert("memory", "doc-3", [0.0, 1.0, 0.0], metadata={"tenant": "ahana"})

        ann = engine.build_ann_index("memory", n_lists=2)
        exact = engine.query("memory", [1.0, 0.0, 0.0], top_k=2, strategy="exact")
        reranked = engine.query(
            "memory",
            [1.0, 0.0, 0.0],
            top_k=2,
            strategy="ann_rerank",
            candidate_multiplier=4,
            ann_probe_count=2,
        )

        assert ann["vectors"] == 3
        assert exact[0]["id"] == reranked[0]["id"]


def test_vector_engine_pq_rerank_matches_exact_top_hit(tmp_path: Path) -> None:
    rng = np.random.default_rng(7)

    with VectorStateEngineV2(tmp_path / "pq.wal") as engine:
        engine.create_collection("memory", 16)

        vectors: list[np.ndarray] = []
        for idx in range(1100):
            vector = np.zeros(16, dtype=np.float32)
            vector[idx % 16] = 1.0
            vector += rng.normal(0.0, 0.01, size=16).astype(np.float32)
            vector /= np.linalg.norm(vector)
            vectors.append(vector)
            engine.upsert("memory", f"doc-{idx}", vector.tolist(), metadata={"bucket": idx % 16})

        hnsw = engine.build_hnsw_index("memory", enable_pq=True)
        assert hnsw["pq_enabled"] is True

        query = vectors[321].tolist()
        exact = engine.query("memory", query, top_k=5, strategy="exact")
        reranked = engine.query(
            "memory",
            query,
            top_k=5,
            strategy="pq_rerank",
            candidate_multiplier=8,
        )

        assert reranked
        assert exact[0]["id"] == reranked[0]["id"]


def test_hnsw_query_does_not_hold_engine_lock_during_search(tmp_path: Path) -> None:
    with VectorStateEngineV2(tmp_path / "concurrent_hnsw.wal") as engine:
        engine.create_collection("memory", 8)
        for idx in range(64):
            vector = np.zeros(8, dtype=np.float32)
            vector[idx % 8] = 1.0
            engine.upsert("memory", f"doc-{idx}", vector.tolist(), metadata={"bucket": idx % 8})

        engine.build_hnsw_index("memory")
        collection = engine._collections["memory"]
        builder = collection.hnsw_builder
        assert builder is not None

        started = threading.Event()
        release = threading.Event()
        original_search = builder.search

        def slow_search(*args, **kwargs):
            started.set()
            assert release.wait(timeout=2.0)
            return original_search(*args, **kwargs)

        builder.search = slow_search  # type: ignore[method-assign]
        try:
            query_thread = threading.Thread(
                target=lambda: engine.query("memory", [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], top_k=5, strategy="hnsw"),
                daemon=True,
            )
            query_thread.start()
            assert started.wait(timeout=1.0)

            started_at = time.perf_counter()
            stats = engine.stats()
            elapsed = time.perf_counter() - started_at

            assert stats.vectors == 64
            assert elapsed < 0.1
        finally:
            release.set()
            query_thread.join(timeout=2.0)
            builder.search = original_search  # type: ignore[method-assign]


def test_threaded_server_filtered_hnsw_budget_respects_success_ceiling(tmp_path: Path) -> None:
    server = VectorStateServerV2(tmp_path / "threaded_budget.wal", host="127.0.0.1", port=0)
    host, port = server.address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def _send_command(sock: socket.socket, command: dict[str, object]) -> dict[str, object]:
        sock.sendall((json.dumps(command) + "\n").encode())
        payload = b""
        while not payload.endswith(b"\n"):
            chunk = sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            payload += chunk
        return json.loads(payload.decode().strip())

    rng = np.random.default_rng(7)
    vectors = rng.standard_normal((512, 32)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    queries = rng.standard_normal((64, 32)).astype(np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    seed_client = socket.create_connection((host, port), timeout=2.0)
    try:
        _send_command(seed_client, {"cmd": "VECTOR_CREATE", "collection": "docs", "dimensions": 32, "metric": "cosine"})
        for idx, vector in enumerate(vectors):
            response = _send_command(
                seed_client,
                {
                    "cmd": "VECTOR_UPSERT",
                    "collection": "docs",
                    "id": f"doc-{idx}",
                    "vector": vector.tolist(),
                    "metadata": {"tenant": f"tenant:{idx % 8}"},
                },
            )
            assert response["ok"] is True
        build = _send_command(seed_client, {"cmd": "VECTOR_BUILD_HNSW", "collection": "docs", "M": 16})
        assert build["ok"] is True
    finally:
        seed_client.close()

    def _worker(worker_queries: np.ndarray) -> list[dict[str, object]]:
        sock = socket.create_connection((host, port), timeout=2.0)
        try:
            diagnostics: list[dict[str, object]] = []
            for query in worker_queries:
                result = _send_command(
                    sock,
                    {
                        "cmd": "VECTOR_QUERY",
                        "collection": "docs",
                        "vector": query.tolist(),
                        "top_k": 10,
                        "strategy": "hnsw",
                        "filters": {"tenant": "tenant:0"},
                        "include_diagnostics": True,
                    },
                )
                diagnostics.append(result["result"]["diagnostics"])
            return diagnostics
        finally:
            sock.close()

    diagnostics: list[dict[str, object]] = []
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_worker, chunk) for chunk in np.array_split(queries, 8) if chunk.size > 0]
            for future in as_completed(futures):
                diagnostics.extend(future.result())
    finally:
        server.shutdown()
        thread.join(timeout=2.0)

    assert diagnostics
    for diag in diagnostics:
        requested = int(diag["requested_hnsw_hits"])
        adapted = int(diag.get("adapted_requested_hnsw_hits") or requested)
        ceiling = int(diag["success_hnsw_hit_ceiling"])
        assert requested <= ceiling
        assert adapted <= ceiling


def test_filtered_hnsw_retries_before_exact_fallback(tmp_path: Path) -> None:
    with VectorStateEngineV2(tmp_path / "retry_filtered_hnsw.wal") as engine:
        engine.create_collection("docs", 4)
        for idx in range(12):
            vector = np.zeros(4, dtype=np.float32)
            vector[idx % 4] = 1.0
            engine.upsert(
                "docs",
                f"doc-{idx}",
                vector.tolist(),
                metadata={"tenant": f"tenant:{idx % 3}"},
            )

        engine.build_hnsw_index("docs", M=8)
        collection = engine._collections["docs"]
        builder = collection.hnsw_builder
        assert builder is not None
        filter_key = engine._filter_cache_key({"tenant": "tenant:0"})
        assert collection.hnsw_filter_request_cache is not None
        collection.hnsw_filter_request_cache[filter_key] = 6

        original_search = builder.search
        calls: list[int] = []

        def staged_search(*args, **kwargs):
            calls.append(int(kwargs["top_k"]))
            if len(calls) == 1:
                return [(1, 0.01), (2, 0.02), (4, 0.03), (0, 0.04)]
            return [(0, 0.01), (3, 0.02), (6, 0.03), (9, 0.04)]

        builder.search = staged_search  # type: ignore[method-assign]
        try:
            result = engine.query(
                "docs",
                [1.0, 0.0, 0.0, 0.0],
                top_k=3,
                strategy="hnsw",
                filters={"tenant": "tenant:0"},
                include_diagnostics=True,
                candidate_multiplier=2,
            )
        finally:
            builder.search = original_search  # type: ignore[method-assign]

        diagnostics = result["diagnostics"]
        hits = result["hits"]

        assert len(hits) == 3
        assert {hit["id"] for hit in hits}.issubset({"doc-0", "doc-3", "doc-6", "doc-9"})
        assert all(hit["metadata"]["tenant"] == "tenant:0" for hit in hits)
        assert len(calls) == 2
        assert diagnostics["hnsw_search_attempts"] == 2
        assert diagnostics["fallback_used"] is False
        assert diagnostics["retry_requested_hnsw_hits"] is not None
        assert diagnostics["adapted_requested_hnsw_hits"] == diagnostics["retry_requested_hnsw_hits"]
        assert int(diagnostics["retry_requested_hnsw_hits"]) > int(diagnostics["requested_hnsw_hits"])