from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_benchmark_module():
    module_path = Path(__file__).resolve().parents[1] / "benchmark_all_features.py"
    spec = importlib.util.spec_from_file_location("branch33_benchmark_all_features", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_marketable_preset_filters_out_non_qualifying_scales(monkeypatch, tmp_path: Path) -> None:
    bench = _load_benchmark_module()

    class _DummyEngine:
        def __init__(self, _wal_path):
            self.scale = 0

        def create_collection(self, *args, **kwargs):
            return None

        def close(self):
            return None

    def _fake_insertion(engine, collection, scale, dimensions):
        engine.scale = scale
        return None, None, float(scale), 1.0

    def _fake_strategy_comparison(engine, collection, query_vecs, **kwargs):
        if engine.scale == 1000:
            return {
                "exact": {"p50_ms": 1.0},
                "build": {"hnsw_build_ms": 10.0},
                "hnsw": {"p50_ms": 0.7, "top1_match_rate": 0.96, "recall_at_k": 0.92, "mean_overlap": 9.2},
                "pq_rerank": {"p50_ms": 0.8, "top1_match_rate": 0.90, "recall_at_k": 0.95, "mean_overlap": 9.5},
            }
        return {
            "exact": {"p50_ms": 1.0},
            "build": {"hnsw_build_ms": 10.0},
            "hnsw": {"p50_ms": 1.2, "top1_match_rate": 0.99, "recall_at_k": 0.99, "mean_overlap": 9.9},
            "pq_rerank": {"p50_ms": 0.7, "top1_match_rate": 0.80, "recall_at_k": 0.85, "mean_overlap": 8.5},
        }

    monkeypatch.setattr(bench, "VectorStateEngineV2", _DummyEngine)
    monkeypatch.setattr(bench, "benchmark_insertion", _fake_insertion)
    monkeypatch.setattr(bench, "benchmark_strategy_comparison", _fake_strategy_comparison)

    report = bench.benchmark_marketable_scales(
        dimensions=128,
        top_k=10,
        query_repeat=3,
        scales=[1000, 3000],
        hnsw_m=16,
        hnsw_m_max0=32,
        hnsw_ef_construction=96,
        hnsw_ef_search=48,
        pq_segments=8,
        pq_centroids=64,
        hnsw_candidate_multiplier=6,
        pq_candidate_multiplier=6,
        min_speedup=1.05,
        min_top1=0.95,
        min_recall_at_k=0.9,
    )

    assert [run["scale"] for run in report["marketable_runs"]] == [1000]
    assert set(report["marketable_runs"][0]["qualifying_strategies"].keys()) == {"hnsw"}

    rows = bench.build_marketable_summary_rows(report)
    assert rows == [
        {
            "scale": 1000,
            "strategy": "hnsw",
            "exact_p50_ms": 1.0,
            "strategy_p50_ms": 0.7,
            "speedup_vs_exact": 1.4286,
            "top1_match_rate": 0.96,
            "recall_at_k": 0.92,
            "mean_overlap": 9.2,
        }
    ]

    summary_json, summary_csv = bench.write_marketable_scale_summaries(report, tmp_path)
    assert summary_json.exists()
    assert summary_csv.exists()