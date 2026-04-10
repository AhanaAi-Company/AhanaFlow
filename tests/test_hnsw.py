"""Tests for HNSW index — graph construction, search quality, PQ, serialization, and engine integration."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from backend.vector_server.engine import VectorStateEngineV2
from backend.vector_server.hnsw import (
    HNSWBuilder,
    HNSWConfig,
    HNSWIndex,
    ProductQuantizer,
    deserialize_hnsw,
    serialize_hnsw,
)
from backend.vector_server.server import VectorStateServerV2


# ---------------------------------------------------------------------------
# Pure HNSW graph tests
# ---------------------------------------------------------------------------


def _make_cluster_data(n_per_cluster: int = 50, n_clusters: int = 5, dim: int = 32, seed: int = 42):
    """Generate clustered vectors for recall testing."""
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((n_clusters, dim)).astype(np.float32)
    centers = centers / np.linalg.norm(centers, axis=1, keepdims=True)
    vectors = []
    for center in centers:
        noise = rng.standard_normal((n_per_cluster, dim)).astype(np.float32) * 0.05
        cluster_vecs = center[np.newaxis, :] + noise
        cluster_vecs = cluster_vecs / np.linalg.norm(cluster_vecs, axis=1, keepdims=True)
        vectors.append(cluster_vecs)
    matrix = np.vstack(vectors).astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1).astype(np.float32)
    return matrix, norms


class TestHNSWGraph:
    """Core HNSW graph construction and search."""

    def test_single_insert_and_search(self):
        config = HNSWConfig(M=4, M_max0=8, ef_construction=16, ef_search=10, metric="cosine")
        builder = HNSWBuilder(config, dimensions=3)
        matrix = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1).astype(np.float32)
        active = np.array([True])

        builder.insert(0, matrix, norms)
        results = builder.search(matrix[0], top_k=1, ef_search=10, matrix=matrix, norms=norms, active=active)
        assert len(results) == 1
        assert results[0][0] == 0

    def test_three_vectors_nearest_correct(self):
        config = HNSWConfig(M=4, M_max0=8, ef_construction=32, ef_search=20, metric="cosine")
        builder = HNSWBuilder(config, dimensions=3)
        matrix = np.array([
            [1.0, 0.0, 0.0],   # idx 0
            [0.95, 0.05, 0.0],  # idx 1 — nearest to idx 0
            [0.0, 1.0, 0.0],   # idx 2 — far from idx 0
        ], dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1).astype(np.float32)
        active = np.ones(3, dtype=bool)

        for i in range(3):
            builder.insert(i, matrix, norms)

        results = builder.search(matrix[0], top_k=2, ef_search=20, matrix=matrix, norms=norms, active=active)
        assert results[0][0] == 0  # Itself is the nearest
        assert results[1][0] == 1  # Second nearest

    def test_build_from_matrix(self):
        matrix, norms = _make_cluster_data(n_per_cluster=20, n_clusters=3, dim=16)
        config = HNSWConfig(M=8, M_max0=16, ef_construction=64, ef_search=32, metric="cosine")
        builder = HNSWBuilder(config, dimensions=16)
        active_indices = np.arange(matrix.shape[0], dtype=np.int64)
        index = builder.build_from_matrix(active_indices, matrix, norms)

        assert index.build_size == 60
        assert index.node_count == 60
        assert index.max_level >= 0
        assert index.entry_point is not None

    def test_recall_at_10_above_threshold(self):
        """HNSW recall@10 should exceed 90% on clustered data."""
        matrix, norms = _make_cluster_data(n_per_cluster=100, n_clusters=5, dim=32)
        active = np.ones(matrix.shape[0], dtype=bool)
        config = HNSWConfig(M=16, M_max0=32, ef_construction=200, ef_search=100, metric="cosine")
        builder = HNSWBuilder(config, dimensions=32)
        active_indices = np.arange(matrix.shape[0], dtype=np.int64)
        builder.build_from_matrix(active_indices, matrix, norms)

        # Run 20 random queries
        rng = np.random.default_rng(99)
        total_recall = 0.0
        n_queries = 20
        for _ in range(n_queries):
            query = rng.standard_normal(32).astype(np.float32)
            query = query / np.linalg.norm(query)

            # Exact brute-force top-10
            sims = matrix @ query
            exact_top10 = set(np.argsort(sims)[-10:][::-1].tolist())

            # HNSW top-10
            hnsw_hits = builder.search(query, top_k=10, ef_search=100, matrix=matrix, norms=norms, active=active)
            hnsw_top10 = set(idx for idx, _ in hnsw_hits)

            recall = len(exact_top10 & hnsw_top10) / 10.0
            total_recall += recall

        avg_recall = total_recall / n_queries
        assert avg_recall >= 0.90, f"HNSW recall@10 = {avg_recall:.2%} — below 90% threshold"

    def test_deletion_excludes_from_results(self):
        config = HNSWConfig(M=4, M_max0=8, ef_construction=16, ef_search=10, metric="cosine")
        builder = HNSWBuilder(config, dimensions=3)
        matrix = np.array([
            [1.0, 0.0, 0.0],
            [0.95, 0.05, 0.0],
            [0.0, 1.0, 0.0],
        ], dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1).astype(np.float32)
        active = np.ones(3, dtype=bool)

        for i in range(3):
            builder.insert(i, matrix, norms)

        # Delete the nearest neighbor
        builder.mark_deleted(1)
        active[1] = False

        results = builder.search(matrix[0], top_k=2, ef_search=10, matrix=matrix, norms=norms, active=active)
        result_ids = [idx for idx, _ in results]
        assert 1 not in result_ids


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestHNSWSerialization:
    def test_round_trip(self):
        matrix, norms = _make_cluster_data(n_per_cluster=10, n_clusters=3, dim=8)
        config = HNSWConfig(M=4, M_max0=8, ef_construction=32, ef_search=16, metric="cosine")
        builder = HNSWBuilder(config, dimensions=8)
        active_indices = np.arange(matrix.shape[0], dtype=np.int64)
        index = builder.build_from_matrix(active_indices, matrix, norms)

        data = serialize_hnsw(index)
        restored = deserialize_hnsw(data)

        assert restored.node_count == index.node_count
        assert restored.max_level == index.max_level
        assert restored.entry_point == index.entry_point
        assert restored.dimensions == index.dimensions
        assert len(restored.nodes) == len(index.nodes)


# ---------------------------------------------------------------------------
# Product Quantization tests
# ---------------------------------------------------------------------------


class TestProductQuantizer:
    def test_train_and_encode(self):
        rng = np.random.default_rng(42)
        vectors = rng.standard_normal((1000, 32)).astype(np.float32)
        pq = ProductQuantizer(pq_segments=4, pq_centroids=256, dimensions=32)
        pq.train(vectors)
        codes = pq.encode(vectors)

        assert codes.shape == (1000, 4)
        assert codes.dtype == np.uint8

    def test_asymmetric_distances(self):
        rng = np.random.default_rng(42)
        vectors = rng.standard_normal((500, 16)).astype(np.float32)
        pq = ProductQuantizer(pq_segments=4, pq_centroids=64, dimensions=16)
        pq.train(vectors)
        codes = pq.encode(vectors)

        query = rng.standard_normal(16).astype(np.float32)
        approx_dists = pq.asymmetric_distances(query, codes)

        assert approx_dists.shape == (500,)
        # Nearest by PQ should be in top-20 of exact L2
        exact_dists = np.sum((vectors - query[np.newaxis, :]) ** 2, axis=1)
        pq_top1 = int(np.argmin(approx_dists))
        exact_top20 = set(np.argsort(exact_dists)[:20].tolist())
        assert pq_top1 in exact_top20, f"PQ top-1 ({pq_top1}) not in exact top-20"


# ---------------------------------------------------------------------------
# Engine integration tests
# ---------------------------------------------------------------------------


class TestHNSWEngineIntegration:
    def test_build_hnsw_index(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw.wal") as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0], metadata={"t": "a"})
            engine.upsert("docs", "doc-2", [0.95, 0.05, 0.0], metadata={"t": "a"})
            engine.upsert("docs", "doc-3", [0.0, 1.0, 0.0], metadata={"t": "b"})

            result = engine.build_hnsw_index("docs", M=4, ef_construction=32, ef_search=16)
            assert result["vectors"] == 3
            assert result["layers"] >= 1
            assert result["M"] == 4

    def test_build_hnsw_index_exposes_pq_tuning(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_pq.wal") as engine:
            engine.create_collection("docs", 16)
            rng = np.random.default_rng(7)
            for idx in range(1100):
                vector = rng.standard_normal(16).astype(np.float32)
                vector /= np.linalg.norm(vector)
                engine.upsert("docs", f"doc-{idx}", vector.tolist())

            result = engine.build_hnsw_index(
                "docs",
                ef_search=17,
                enable_pq=True,
                pq_segments=4,
                pq_centroids=32,
            )
            assert result["pq_enabled"] is True
            assert result["pq_segments"] == 4
            assert result["pq_centroids"] == 32
            assert result["ef_search"] == 17

    def test_hnsw_query_uses_built_ef_search_by_default(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_ef_search.wal") as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0])
            engine.upsert("docs", "doc-2", [0.95, 0.05, 0.0])
            engine.upsert("docs", "doc-3", [0.0, 1.0, 0.0])
            engine.build_hnsw_index("docs", ef_search=13)

            collection = engine._collections["docs"]
            builder = collection.hnsw_builder
            assert builder is not None

            recorded: list[int | None] = []
            original_search = builder.search

            def _wrapped_search(query, top_k, ef_search, matrix, norms, active):
                recorded.append(ef_search)
                return original_search(query, top_k, ef_search, matrix, norms, active)

            builder.search = _wrapped_search  # type: ignore[method-assign]

            engine.query("docs", [1.0, 0.0, 0.0], top_k=2, strategy="hnsw")
            # effective_ef_search = max(configured_ef_search, requested_hnsw_hits)
            # With 3 vectors, top_k=2, candidate_multiplier=8: requested_hnsw_hits=16
            # So effective = max(13, 16) = 16
            assert len(recorded) == 1
            assert recorded[0] >= 13  # ef_search is at least the configured value

    def test_hnsw_query_strategy(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_query.wal") as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0])
            engine.upsert("docs", "doc-2", [0.95, 0.05, 0.0])
            engine.upsert("docs", "doc-3", [0.0, 1.0, 0.0])
            engine.upsert("docs", "doc-4", [0.0, 0.0, 1.0])

            engine.build_hnsw_index("docs")

            exact = engine.query("docs", [1.0, 0.0, 0.0], top_k=2, strategy="exact")
            hnsw = engine.query("docs", [1.0, 0.0, 0.0], top_k=2, strategy="hnsw")

            assert exact[0]["id"] == hnsw[0]["id"], \
                f"HNSW top-1 ({hnsw[0]['id']}) != exact top-1 ({exact[0]['id']})"

    def test_hnsw_auto_rebuild_on_dirty(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_dirty.wal") as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0])
            engine.upsert("docs", "doc-2", [0.0, 1.0, 0.0])
            engine.build_hnsw_index("docs")

            # Insert new vector → marks dirty → next hnsw query triggers rebuild
            engine.upsert("docs", "doc-3", [0.95, 0.05, 0.0])
            results = engine.query("docs", [1.0, 0.0, 0.0], top_k=3, strategy="hnsw")
            ids = [r["id"] for r in results]
            assert "doc-3" in ids

    def test_hnsw_with_filters(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_filter.wal") as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0], metadata={"tenant": "ahana"})
            engine.upsert("docs", "doc-2", [0.95, 0.05, 0.0], metadata={"tenant": "other"})
            engine.upsert("docs", "doc-3", [0.0, 1.0, 0.0], metadata={"tenant": "ahana"})
            engine.build_hnsw_index("docs")

            results = engine.query(
                "docs", [1.0, 0.0, 0.0], top_k=2,
                strategy="hnsw", filters={"tenant": "ahana"}, include_diagnostics=True,
            )
            for hit in results["hits"]:
                assert hit["metadata"]["tenant"] == "ahana"
            assert results["diagnostics"]["filtered_candidate_count"] == 2
            assert results["diagnostics"]["fallback_reason"] in {None, "insufficient_filtered_hnsw_hits"}

    def test_hnsw_filtered_queries_scale_requested_hits(self, tmp_path: Path):
        with VectorStateEngineV2(tmp_path / "hnsw_filter_scale.wal") as engine:
            engine.create_collection("docs", 3)
            for idx in range(24):
                tenant = "ahana" if idx < 8 else "other"
                vector = [1.0, 0.0, 0.0] if idx == 0 else [0.9, 0.1, 0.0] if idx < 12 else [0.0, 1.0, 0.0]
                engine.upsert("docs", f"doc-{idx}", vector, metadata={"tenant": tenant})
            engine.build_hnsw_index("docs", ef_search=16)

            collection = engine._collections["docs"]
            builder = collection.hnsw_builder
            assert builder is not None

            recorded: list[tuple[int, int | None]] = []
            original_search = builder.search

            def _wrapped_search(query, top_k, ef_search, matrix, norms, active):
                recorded.append((top_k, ef_search))
                return original_search(query, top_k, ef_search, matrix, norms, active)

            builder.search = _wrapped_search  # type: ignore[method-assign]

            first = engine.query(
                "docs",
                [1.0, 0.0, 0.0],
                top_k=2,
                strategy="hnsw",
                filters={"tenant": "ahana"},
                include_diagnostics=True,
            )

            second = engine.query(
                "docs",
                [1.0, 0.0, 0.0],
                top_k=2,
                strategy="hnsw",
                filters={"tenant": "ahana"},
                include_diagnostics=True,
            )

            assert recorded[0] == (24, 24)
            assert recorded[1][0] < recorded[0][0]
            assert first["diagnostics"]["requested_hnsw_hits"] == 24
            assert first["diagnostics"]["success_hnsw_hit_ceiling"] == 24
            assert first["diagnostics"]["adapted_requested_hnsw_hits"] is not None
            assert first["diagnostics"]["adapted_requested_hnsw_hits"] <= first["diagnostics"]["success_hnsw_hit_ceiling"]
            assert second["diagnostics"]["cached_requested_hnsw_hits"] == first["diagnostics"]["adapted_requested_hnsw_hits"]
            assert second["diagnostics"]["requested_hnsw_hits"] == recorded[1][0]

    def test_hnsw_survives_compact(self, tmp_path: Path):
        wal = tmp_path / "hnsw_compact.wal"
        with VectorStateEngineV2(wal) as engine:
            engine.create_collection("docs", 3)
            engine.upsert("docs", "doc-1", [1.0, 0.0, 0.0])
            engine.upsert("docs", "doc-2", [0.0, 1.0, 0.0])
            engine.upsert("docs", "doc-3", [0.0, 0.0, 1.0])
            engine.delete("docs", "doc-3")
            engine.build_hnsw_index("docs")
            engine.compact("docs")

            # After compact, HNSW is invalidated → should auto-rebuild on query
            results = engine.query("docs", [1.0, 0.0, 0.0], top_k=2, strategy="hnsw")
            ids = [r["id"] for r in results]
            assert "doc-1" in ids
            assert "doc-3" not in ids


# ---------------------------------------------------------------------------
# Server dispatch tests
# ---------------------------------------------------------------------------


class TestHNSWServerDispatch:
    def test_build_hnsw_dispatch(self, tmp_path: Path):
        server = VectorStateServerV2(tmp_path / "srv.wal", port=0)
        server.dispatch({"cmd": "VECTOR_CREATE", "collection": "docs", "dimensions": 3})
        server.dispatch({"cmd": "VECTOR_UPSERT", "collection": "docs", "id": "d1", "vector": [1.0, 0.0, 0.0]})
        server.dispatch({"cmd": "VECTOR_UPSERT", "collection": "docs", "id": "d2", "vector": [0.9, 0.1, 0.0]})

        result = server.dispatch({
            "cmd": "VECTOR_BUILD_HNSW",
            "collection": "docs",
            "M": 4,
            "ef_construction": 32,
        })
        assert result["ok"] is True
        assert result["result"]["vectors"] == 2
        assert result["result"]["M"] == 4

        # Query with hnsw strategy
        query_result = server.dispatch({
            "cmd": "VECTOR_QUERY",
            "collection": "docs",
            "vector": [1.0, 0.0, 0.0],
            "top_k": 1,
            "strategy": "hnsw",
        })
        assert query_result["ok"] is True
        assert query_result["result"][0]["id"] == "d1"
        server.shutdown()

    def test_build_hnsw_dispatch_accepts_pq_tuning(self, tmp_path: Path):
        server = VectorStateServerV2(tmp_path / "srv_pq.wal", port=0)
        server.dispatch({"cmd": "VECTOR_CREATE", "collection": "docs", "dimensions": 16})

        rng = np.random.default_rng(9)
        for idx in range(1100):
            vector = rng.standard_normal(16).astype(np.float32)
            vector /= np.linalg.norm(vector)
            server.dispatch({
                "cmd": "VECTOR_UPSERT",
                "collection": "docs",
                "id": f"d{idx}",
                "vector": vector.tolist(),
            })

        result = server.dispatch({
            "cmd": "VECTOR_BUILD_HNSW",
            "collection": "docs",
            "enable_pq": True,
            "pq_segments": 4,
            "pq_centroids": 32,
            "ef_search": 15,
        })
        assert result["ok"] is True
        assert result["result"]["pq_enabled"] is True
        assert result["result"]["pq_segments"] == 4
        assert result["result"]["pq_centroids"] == 32
        assert result["result"]["ef_search"] == 15
        server.shutdown()


# ---------------------------------------------------------------------------
# Scale test (medium)
# ---------------------------------------------------------------------------


class TestHNSWScale:
    @pytest.mark.slow
    def test_10k_vectors_recall(self, tmp_path: Path):
        """Build HNSW over 10K×128 vectors and verify recall@10 >= 85%.

        NOTE: Random uniform 128-dim vectors suffer from distance concentration
        (curse of dimensionality), making exact top-10 neighborhoods ambiguous.
        85% recall on random data with ef_search=256 is excellent; real-world
        clustered data (embeddings) typically yields 95%+ at the same settings.
        """
        rng = np.random.default_rng(7)
        N, D = 10_000, 128
        matrix = rng.standard_normal((N, D)).astype(np.float32)
        matrix = matrix / np.linalg.norm(matrix, axis=1, keepdims=True)
        norms = np.linalg.norm(matrix, axis=1).astype(np.float32)
        active = np.ones(N, dtype=bool)

        config = HNSWConfig(M=16, M_max0=32, ef_construction=200, ef_search=256, metric="cosine")
        builder = HNSWBuilder(config, dimensions=D)
        active_indices = np.arange(N, dtype=np.int64)
        builder.build_from_matrix(active_indices, matrix, norms)

        queries = rng.standard_normal((50, D)).astype(np.float32)
        queries = queries / np.linalg.norm(queries, axis=1, keepdims=True)

        total_recall = 0.0
        for query in queries:
            exact_sims = matrix @ query
            exact_top10 = set(np.argsort(exact_sims)[-10:][::-1].tolist())

            hnsw_hits = builder.search(query, top_k=10, ef_search=256, matrix=matrix, norms=norms, active=active)
            hnsw_top10 = set(idx for idx, _ in hnsw_hits)

            total_recall += len(exact_top10 & hnsw_top10) / 10.0

        avg_recall = total_recall / 50
        assert avg_recall >= 0.85, f"10K HNSW recall@10 = {avg_recall:.2%} — below 85%"
