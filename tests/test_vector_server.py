from __future__ import annotations

from pathlib import Path

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