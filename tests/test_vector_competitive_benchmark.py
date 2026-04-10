from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "benchmark_vector_vs_competitors.py"
    spec = importlib.util.spec_from_file_location("branch33_vector_competitive_benchmark", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_topk_indices_orders_neighbors() -> None:
    bench = _load_module()

    vectors = np.array(
        [
            [1.0, 0.0],
            [0.8, 0.2],
            [0.0, 1.0],
        ],
        dtype=np.float32,
    )
    queries = np.array([[1.0, 0.0]], dtype=np.float32)

    assert bench._exact_topk_indices(vectors, queries, k=2) == [[0, 1]]


def test_extract_neighbor_indices_prefers_metadata_index() -> None:
    bench = _load_module()

    hits = [
        {"id": "vec_7", "metadata": {"index": 4}},
        {"id": "vec_9", "metadata": {}},
        {"id": "other"},
    ]

    assert bench._extract_neighbor_indices(hits) == [4, 9]


def test_load_ann_claim_gate_uses_marketable_summary(tmp_path: Path) -> None:
    bench = _load_module()

    summary = tmp_path / "feature_benchmark_marketable_scale_summary.json"
    summary.write_text(
        '{"thresholds":{"min_speedup":1.05},"rows":[{"scale":1000,"strategy":"hnsw"}]}'
    )

    gate = bench._load_ann_claim_gate(tmp_path)
    assert gate["ann_claims_allowed"] is True
    assert gate["qualifying_rows"] == 1


def test_load_ann_claim_gate_blocks_when_higher_scale_summary_is_empty(tmp_path: Path) -> None:
    bench = _load_module()

    summary = tmp_path / "feature_benchmark_marketable_scale_summary.json"
    summary.write_text(
        '{"thresholds":{"min_speedup":1.05,"min_recall_at_k":0.9},"rows":[]}'
    )

    gate = bench._load_ann_claim_gate(tmp_path)
    assert gate["ann_claims_allowed"] is False
    assert gate["qualifying_rows"] == 0
    assert "marketable thresholds" in gate["reason"]


def test_benchmark_vector_search_sends_hnsw_strategy() -> None:
    bench = _load_module()

    class _Client:
        def __init__(self):
            self.commands = []

        def send(self, cmd):
            self.commands.append(cmd)
            return {"result": [{"id": "vec_0", "metadata": {"index": 0}}]}

    client = _Client()
    vectors = np.array([[1.0, 0.0]], dtype=np.float32)
    _p50, _p99, _latencies, retrieved = bench.benchmark_vector_search(
        client,
        vectors,
        "docs",
        k=1,
        strategy="hnsw",
    )

    assert client.commands[0]["strategy"] == "hnsw"
    assert retrieved == [[0]]