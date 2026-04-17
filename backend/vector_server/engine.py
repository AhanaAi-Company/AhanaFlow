from __future__ import annotations

import io
import json
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .hnsw import HNSWBuilder, HNSWConfig, HNSWIndex, HNSWLibBackend, _HNSWLIB_AVAILABLE, make_hnsw_builder, serialize_hnsw
from .codec import compress as _compress, decompress as _decompress

# --- GPU-accelerated distance computation (Feature 4) ---
try:
    import torch as _torch

    _HAS_CUDA = _torch.cuda.is_available()
except ModuleNotFoundError:  # pragma: no cover
    _torch = None  # type: ignore[assignment]
    _HAS_CUDA = False

try:
    import orjson as _orjson

    def _json_dumps(obj: Any) -> bytes:
        return _orjson.dumps(obj)

    def _json_loads(data: bytes) -> Any:
        return _orjson.loads(data)
except ModuleNotFoundError:  # pragma: no cover
    def _json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    def _json_loads(data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


_RECORD_MAGIC = b"AV2\x01"
_JSON_LEN_STRUCT = struct.Struct(">I")


@dataclass(frozen=True)
class VectorCollectionStats:
    name: str
    dimensions: int
    metric: str
    vectors: int


@dataclass(frozen=True)
class VectorStateStats:
    collections: int
    vectors: int
    records_replayed: int
    wal_size_bytes: int
    collection_stats: list[VectorCollectionStats]


@dataclass
class _AnnIndex:
    centroids: np.ndarray
    buckets: list[np.ndarray]
    build_size: int


@dataclass
class _VectorVersion:
    """A single historical snapshot of a vector."""
    vector: np.ndarray
    metadata: dict[str, Any]
    timestamp: float


@dataclass
class _VectorCollection:
    dimensions: int
    metric: str
    matrix: np.ndarray
    norms: np.ndarray
    active: np.ndarray
    ids: list[str | None]
    metadata: list[dict[str, Any] | None]
    payloads: list[Any]
    expires_at: list[float | None]
    id_to_index: dict[str, int]
    count: int = 0
    free_indices: list[int] | None = None
    ann_index: _AnnIndex | None = None
    ann_dirty: bool = True
    hnsw_builder: HNSWBuilder | None = None
    hnsw_dirty: bool = True
    modality: str = "vector"  # "vector" | "text" | "image" | "video" | "audio"
    version_history: dict[str, list[_VectorVersion]] | None = None  # id → versions
    active_indices_cache: np.ndarray | None = None
    active_indices_dirty: bool = True
    filtered_indices_cache: dict[tuple[Any, ...], np.ndarray] | None = None
    filtered_mask_cache: dict[tuple[Any, ...], np.ndarray] | None = None
    hnsw_filter_request_cache: dict[tuple[Any, ...], int] | None = None


class VectorStateEngineV2:
    """Separate vector-capable engine for local semantic retrieval.

    The existing Branch 33 state engine remains unchanged. This v2 engine keeps
    vector search in an isolated runtime so the current throughput and WAL
    behavior for key/value, queue, and stream workloads are not affected.
    """

    _LEN_STRUCT = struct.Struct(">I")

    def __init__(self, wal_path: str | Path) -> None:
        self._wal_path = Path(wal_path)
        self._wal_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._collections: dict[str, _VectorCollection] = {}
        self._records_replayed = 0

        if self._wal_path.exists():
            self._replay()
        else:
            self._wal_path.touch()
        self._wal_handle: io.BufferedWriter = open(self._wal_path, "ab")

    def create_collection(self, name: str, dimensions: int, metric: str = "cosine",
                           modality: str = "vector") -> None:
        if dimensions <= 0:
            raise ValueError("dimensions must be > 0")
        if metric not in {"cosine", "dot"}:
            raise ValueError("metric must be 'cosine' or 'dot'")
        if modality not in {"vector", "text", "image", "video", "audio", "multimodal"}:
            raise ValueError("modality must be one of: vector, text, image, video, audio, multimodal")
        record = {
            "op": "create_collection",
            "collection": name,
            "dimensions": dimensions,
            "metric": metric,
            "modality": modality,
            "ts": time.time(),
        }
        with self._lock:
            existing = self._collections.get(name)
            if existing is not None:
                if existing.dimensions != dimensions or existing.metric != metric:
                    raise ValueError(
                        f"collection {name!r} already exists with dimensions={existing.dimensions} metric={existing.metric}"
                    )
                return
            self._append_record(record)
            self._apply_record(record)

    def list_collections(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {
                    "name": name,
                    "dimensions": collection.dimensions,
                    "metric": collection.metric,
                    "vectors": self._live_vector_count(collection),
                }
                for name, collection in sorted(self._collections.items())
            ]

    def upsert(
        self,
        collection: str,
        item_id: str,
        vector: list[float],
        *,
        metadata: dict[str, Any] | None = None,
        payload: Any = None,
        ttl_seconds: int | None = None,
    ) -> None:
        now = time.time()
        expires_at = now + ttl_seconds if ttl_seconds is not None else None
        vector_array = self._normalize_vector_input(vector)
        record = {
            "op": "upsert",
            "collection": collection,
            "id": item_id,
            "vector": vector_array,
            "metadata": metadata or {},
            "payload": payload,
            "expires_at": expires_at,
            "ts": now,
        }
        with self._lock:
            col = self._require_collection(collection)
            self._validate_vector_dimensions(col, vector_array)
            self._append_record(record)
            self._apply_record(record)

    def get(self, collection: str, item_id: str, *, include_vector: bool = False) -> dict[str, Any] | None:
        with self._lock:
            col = self._require_collection(collection)
            self._purge_expired_item(col, item_id)
            index = col.id_to_index.get(item_id)
            if index is None or not bool(col.active[index]):
                return None
            result = {
                "id": item_id,
                "metadata": dict(col.metadata[index] or {}),
                "payload": col.payloads[index],
                "expires_at": col.expires_at[index],
            }
            if include_vector:
                result["vector"] = col.matrix[index].astype(float).tolist()
            return result

    def delete(self, collection: str, item_id: str) -> bool:
        record = {
            "op": "delete",
            "collection": collection,
            "id": item_id,
            "ts": time.time(),
        }
        with self._lock:
            col = self._require_collection(collection)
            existed = item_id in col.id_to_index
            self._append_record(record)
            self._apply_record(record)
            return existed

    def scan(
        self,
        collection: str,
        *,
        limit: int = 1000,
        include_vectors: bool = False,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        with self._lock:
            col = self._require_collection(collection)
            self._purge_all_expired(col)
            results: list[dict[str, Any]] = []
            for index, item_id in enumerate(col.ids):
                if item_id is None or not bool(col.active[index]):
                    continue
                row = {
                    "id": item_id,
                    "metadata": dict(col.metadata[index] or {}),
                    "payload": col.payloads[index],
                    "expires_at": col.expires_at[index],
                }
                if include_vectors:
                    row["vector"] = col.matrix[index].astype(float).tolist()
                results.append(row)
                if len(results) >= limit:
                    break
            return results

    def query(
        self,
        collection: str,
        vector: list[float],
        *,
        top_k: int = 5,
        filters: dict[str, Any] | None = None,
        include_vectors: bool = False,
        strategy: str = "exact",
        candidate_multiplier: int = 8,
        ann_probe_count: int | None = None,
        compress_results: bool = False,
        use_gpu: bool = False,
        query_text: str | None = None,
        ncd_weight: float = 0.3,
        bpe_weight: float = 0.3,
        include_diagnostics: bool = False,
    ) -> Any:
        if top_k <= 0:
            raise ValueError("top_k must be > 0")
        base_strategy = strategy
        if strategy in {"ncd_hybrid", "bpe_hybrid"}:
            base_strategy = "exact"
        if base_strategy not in {"exact", "ann_rerank", "hnsw", "pq_rerank"}:
            raise ValueError(
                "strategy must be 'exact', 'ann_rerank', 'hnsw', 'pq_rerank', 'ncd_hybrid', or 'bpe_hybrid'"
            )
        if base_strategy == "hnsw":
            return self._query_hnsw(
                collection,
                vector,
                top_k=top_k,
                filters=filters,
                include_vectors=include_vectors,
                candidate_multiplier=candidate_multiplier,
                ann_probe_count=ann_probe_count,
                compress_results=compress_results,
                include_diagnostics=include_diagnostics,
            )
        if candidate_multiplier <= 0:
            raise ValueError("candidate_multiplier must be > 0")
        with self._lock:
            col = self._require_collection(collection)
            query_vector = self._normalize_vector_input(vector)
            self._validate_vector_dimensions(col, query_vector)
            self._purge_all_expired(col)

            diagnostics: dict[str, Any] | None = {} if include_diagnostics else None

            candidate_indices = self._candidate_indices_for_strategy(
                col,
                query_vector,
                filters,
                top_k=top_k,
                strategy=base_strategy,
                candidate_multiplier=candidate_multiplier,
                ann_probe_count=ann_probe_count,
                diagnostics=diagnostics,
            )
            if diagnostics is not None:
                diagnostics["strategy"] = base_strategy
                diagnostics["filter_applied"] = bool(filters)
                diagnostics["top_k"] = int(top_k)
                diagnostics["candidate_multiplier"] = int(candidate_multiplier)
                diagnostics["ann_probe_count"] = int(ann_probe_count) if ann_probe_count is not None else None
                diagnostics["scored_candidate_count"] = int(candidate_indices.size)
            if candidate_indices.size == 0:
                return {"hits": [], "diagnostics": diagnostics} if diagnostics is not None else []

            top_k = min(top_k, int(candidate_indices.size))

            # For hybrid strategies (NCD/BPE) that need the full collection object,
            # keep the lock and do all work inside — these are rare/expensive paths.
            _is_hybrid = strategy in {"ncd_hybrid", "bpe_hybrid"}
            if _is_hybrid:
                # Score: GPU or CPU path
                if use_gpu and _HAS_CUDA:
                    scores = self._score_gpu(col, query_vector, candidate_indices)
                else:
                    scores = self._score(col, query_vector, candidate_indices)

                if strategy == "ncd_hybrid":
                    rerank_k = min(max(top_k * 5, 50), candidate_indices.size)
                    pre_top = np.argpartition(scores, -rerank_k)[-rerank_k:]
                    ncd_scores = self._ncd_scores(col, query_vector, candidate_indices[pre_top])
                    blended = (1.0 - ncd_weight) * scores[pre_top] + ncd_weight * ncd_scores
                    scores[pre_top] = blended

                if strategy == "bpe_hybrid" and query_text:
                    rerank_k = min(max(top_k * 5, 50), candidate_indices.size)
                    pre_top = np.argpartition(scores, -rerank_k)[-rerank_k:]
                    scores[pre_top] = self._bpe_hybrid_scores(
                        col, query_text, candidate_indices[pre_top], scores[pre_top],
                        semantic_weight=1.0 - bpe_weight,
                    )

                ranked_positions = np.argpartition(scores, -top_k)[-top_k:]
                ranked_positions = ranked_positions[np.argsort(scores[ranked_positions])[::-1]]

                hits: list[dict[str, Any]] = []
                for position in ranked_positions:
                    index = int(candidate_indices[int(position)])
                    item_id = col.ids[index]
                    if item_id is None:
                        continue
                    payload = col.payloads[index]
                    hit: dict[str, Any] = {
                        "id": item_id,
                        "score": float(scores[int(position)]),
                        "metadata": dict(col.metadata[index] or {}),
                    }
                    if compress_results and payload is not None:
                        hit["payload"] = self._compress_payload(payload)
                    else:
                        hit["payload"] = payload
                    if include_vectors:
                        hit["vector"] = col.matrix[index].astype(float).tolist()
                    hits.append(hit)
                if diagnostics is not None:
                    return {"hits": hits, "diagnostics": diagnostics}
                return hits

            # Fast path: snapshot arrays needed for scoring, then release the lock
            # so concurrent queries and writes don't block each other.
            matrix_snap = col.matrix[candidate_indices].copy()
            norms_snap = col.norms[candidate_indices].copy()
            metric = col.metric
            ids_snap = [col.ids[int(i)] for i in candidate_indices]
            metadata_snap = [dict(col.metadata[int(i)] or {}) for i in candidate_indices]
            payloads_snap = [col.payloads[int(i)] for i in candidate_indices]
            include_vectors_snap = [col.matrix[int(i)].copy() for i in candidate_indices] if include_vectors else None
        # --- lock released — scoring runs concurrently with writes ---

        # Score using snapshots
        scores = matrix_snap @ query_vector
        if metric != "dot":
            query_norm = float(np.linalg.norm(query_vector))
            if query_norm == 0.0:
                scores = np.zeros(len(candidate_indices), dtype=np.float32)
            else:
                denom = norms_snap * np.float32(query_norm)
                scores = np.divide(
                    scores,
                    denom,
                    out=np.zeros(len(candidate_indices), dtype=np.float32),
                    where=denom > 0,
                )

        if use_gpu and _HAS_CUDA and _torch is not None:
            # GPU scoring already done via snapshot; no separate path needed here.
            pass

        ranked_positions = np.argpartition(scores, -top_k)[-top_k:]
        ranked_positions = ranked_positions[np.argsort(scores[ranked_positions])[::-1]]

        hits = []
        for pos in ranked_positions:
            pos = int(pos)
            item_id = ids_snap[pos]
            if item_id is None:
                continue
            payload = payloads_snap[pos]
            hit: dict[str, Any] = {
                "id": item_id,
                "score": float(scores[pos]),
                "metadata": metadata_snap[pos],
            }
            if compress_results and payload is not None:
                hit["payload"] = self._compress_payload(payload)
            else:
                hit["payload"] = payload
            if include_vectors_snap is not None:
                hit["vector"] = include_vectors_snap[pos].astype(float).tolist()
            hits.append(hit)
        if diagnostics is not None:
            return {"hits": hits, "diagnostics": diagnostics}
        return hits

    def _query_hnsw(
        self,
        collection: str,
        vector: list[float],
        *,
        top_k: int,
        filters: dict[str, Any] | None,
        include_vectors: bool,
        candidate_multiplier: int,
        ann_probe_count: int | None,
        compress_results: bool,
        include_diagnostics: bool,
    ) -> Any:
        if candidate_multiplier <= 0:
            raise ValueError("candidate_multiplier must be > 0")

        diagnostics: dict[str, Any] | None = {} if include_diagnostics else None
        filter_cache_key: tuple[Any, ...] | None = None
        search_builder: HNSWBuilder | HNSWLibBackend | None = None
        requested_hnsw_hits = 0
        success_hnsw_hit_ceiling = 0
        cached_requested_hnsw_hits: int | None = None
        retry_requested_hnsw_hits: int | None = None
        exact_candidates: np.ndarray
        query_vector: np.ndarray

        with self._lock:
            col = self._require_collection(collection)
            query_vector = self._normalize_vector_input(vector)
            self._validate_vector_dimensions(col, query_vector)
            self._purge_all_expired(col)

            active_indices = self._active_indices(col)
            active_count = int(active_indices.size)
            exact_candidates = self._candidate_indices(col, filters) if filters else active_indices
            filter_cache_key = self._filter_cache_key(filters) if filters else None

            if diagnostics is not None:
                diagnostics["strategy"] = "hnsw"
                diagnostics["filter_applied"] = bool(filters)
                diagnostics["top_k"] = int(top_k)
                diagnostics["candidate_multiplier"] = int(candidate_multiplier)
                diagnostics["ann_probe_count"] = int(ann_probe_count) if ann_probe_count is not None else None
                diagnostics["active_candidate_count"] = active_count
                diagnostics["filtered_candidate_count"] = int(exact_candidates.size)
                diagnostics["fallback_used"] = False
                diagnostics["fallback_reason"] = None
                diagnostics["fallback_candidate_count"] = 0
                diagnostics["hnsw_raw_hit_count"] = 0
                diagnostics["hnsw_filtered_hit_count"] = 0
                diagnostics["requested_hnsw_hits"] = 0
                diagnostics["cached_requested_hnsw_hits"] = None
                diagnostics["adapted_requested_hnsw_hits"] = None
                diagnostics["retry_requested_hnsw_hits"] = None
                diagnostics["hnsw_search_attempts"] = 0
                diagnostics["effective_ef_search"] = None

            if exact_candidates.size == 0:
                return {"hits": [], "diagnostics": diagnostics} if diagnostics is not None else []

            top_k = min(top_k, int(exact_candidates.size))

            if exact_candidates.size > top_k:
                search_builder = self._ensure_hnsw_index_locked(col)
                if search_builder is None or search_builder.index.node_count == 0:
                    search_builder = None
                    if diagnostics is not None:
                        diagnostics["fallback_used"] = True
                        diagnostics["fallback_reason"] = "missing_hnsw_index"
                        diagnostics["fallback_candidate_count"] = int(exact_candidates.size)
                else:
                    base_hnsw_hits = max(top_k * candidate_multiplier, top_k)
                    success_hnsw_hit_ceiling = min(active_count, max(base_hnsw_hits * 2, base_hnsw_hits + (top_k * 12)))
                    requested_hnsw_hits = base_hnsw_hits
                    if filters:
                        filtered_count = int(exact_candidates.size)
                        target_filtered_hits = min(filtered_count, max(top_k * 2, top_k + max(4, top_k)))
                        if filtered_count > 0 and active_count > filtered_count:
                            estimated_hits = int(np.ceil(target_filtered_hits * (active_count / filtered_count)))
                            success_hnsw_hit_ceiling = min(
                                active_count,
                                max(int(np.ceil(base_hnsw_hits * 1.5)), estimated_hits),
                            )
                            requested_hnsw_hits = min(
                                active_count,
                                max(base_hnsw_hits, estimated_hits, int(np.ceil(base_hnsw_hits * 1.5))),
                            )
                        request_cache = col.hnsw_filter_request_cache
                        if request_cache is None:
                            request_cache = {}
                            col.hnsw_filter_request_cache = request_cache
                        cached_requested_hnsw_hits = request_cache.get(filter_cache_key) if filter_cache_key is not None else None
                        if cached_requested_hnsw_hits is not None:
                            requested_hnsw_hits = max(base_hnsw_hits, min(active_count, int(cached_requested_hnsw_hits)))
                        if cached_requested_hnsw_hits is None or int(cached_requested_hnsw_hits) <= success_hnsw_hit_ceiling:
                            requested_hnsw_hits = min(requested_hnsw_hits, success_hnsw_hit_ceiling)
                    ef_search = ann_probe_count if ann_probe_count is not None else search_builder.index.config.ef_search
                    effective_ef_search = max(int(ef_search), int(requested_hnsw_hits))
                    if diagnostics is not None:
                        diagnostics["requested_hnsw_hits"] = int(requested_hnsw_hits)
                        diagnostics["cached_requested_hnsw_hits"] = cached_requested_hnsw_hits
                        diagnostics["effective_ef_search"] = int(effective_ef_search)
                        diagnostics["success_hnsw_hit_ceiling"] = int(success_hnsw_hit_ceiling)

            matrix_ref = col.matrix
            norms_ref = col.norms
            active_snapshot = col.active.copy()

        candidate_indices = exact_candidates
        if search_builder is not None:
            effective_hits = int(requested_hnsw_hits)
            while True:
                hits = search_builder.search(
                    query_vector,
                    top_k=effective_hits,
                    ef_search=max(
                        int(ann_probe_count if ann_probe_count is not None else search_builder.index.config.ef_search),
                        effective_hits,
                    ),
                    matrix=matrix_ref,
                    norms=norms_ref,
                    active=active_snapshot,
                )
                if diagnostics is not None:
                    diagnostics["hnsw_search_attempts"] = int(diagnostics["hnsw_search_attempts"]) + 1
                    diagnostics["hnsw_raw_hit_count"] = int(len(hits))

                if not hits:
                    if diagnostics is not None:
                        diagnostics["fallback_used"] = True
                        diagnostics["fallback_reason"] = "empty_hnsw_hits"
                        diagnostics["fallback_candidate_count"] = int(exact_candidates.size)
                    break

                hnsw_candidates = np.asarray([idx for idx, _ in hits], dtype=np.int64)
                adapted_hits: int | None = None
                should_retry = False
                if filters:
                    with self._lock:
                        col = self._require_collection(collection)
                        filter_mask = self._candidate_mask(col, filters)
                    if filter_mask is not None and hnsw_candidates.size > 0:
                        hnsw_candidates = hnsw_candidates[filter_mask[hnsw_candidates]]
                    else:
                        hnsw_candidates = np.intersect1d(hnsw_candidates, exact_candidates, assume_unique=False)
                    observed_filtered_hits = int(hnsw_candidates.size)
                    target_filtered_hits = min(int(exact_candidates.size), max(top_k * 2, top_k + max(4, top_k)))
                    if observed_filtered_hits > 0:
                        recommended_hits = int(np.ceil(effective_hits * (target_filtered_hits / observed_filtered_hits)))
                    else:
                        recommended_hits = int(effective_hits * 2)
                    if observed_filtered_hits < top_k:
                        recommended_hits = max(recommended_hits, int(effective_hits * 2))
                    with self._lock:
                        col = self._require_collection(collection)
                        request_cache = col.hnsw_filter_request_cache
                        if request_cache is None:
                            request_cache = {}
                            col.hnsw_filter_request_cache = request_cache
                        active_count = int(self._active_indices(col).size)
                        recommended_hits = max(max(top_k * candidate_multiplier, top_k), min(active_count, recommended_hits))
                        previous_hits = int(request_cache.get(filter_cache_key, effective_hits)) if filter_cache_key is not None else int(effective_hits)
                        adapted_hits = max(
                            max(top_k * candidate_multiplier, top_k),
                            min(active_count, int(round((previous_hits + recommended_hits) / 2))),
                        )
                        if success_hnsw_hit_ceiling > 0:
                            adapted_hits = min(adapted_hits, success_hnsw_hit_ceiling)
                        if filter_cache_key is not None:
                            request_cache[filter_cache_key] = adapted_hits
                    if diagnostics is not None:
                        diagnostics["adapted_requested_hnsw_hits"] = adapted_hits
                    should_retry = observed_filtered_hits < top_k and adapted_hits is not None and adapted_hits > effective_hits and retry_requested_hnsw_hits is None
                if diagnostics is not None:
                    diagnostics["hnsw_filtered_hit_count"] = int(hnsw_candidates.size)
                if hnsw_candidates.size >= top_k:
                    candidate_indices = hnsw_candidates
                    break
                if should_retry:
                    retry_requested_hnsw_hits = int(adapted_hits)
                    effective_hits = retry_requested_hnsw_hits
                    if diagnostics is not None:
                        diagnostics["retry_requested_hnsw_hits"] = retry_requested_hnsw_hits
                        diagnostics["effective_ef_search"] = max(
                            int(ann_probe_count if ann_probe_count is not None else search_builder.index.config.ef_search),
                            effective_hits,
                        )
                    continue
                if diagnostics is not None:
                    diagnostics["fallback_used"] = True
                    diagnostics["fallback_reason"] = "insufficient_filtered_hnsw_hits"
                    diagnostics["fallback_candidate_count"] = int(exact_candidates.size)
                break

        with self._lock:
            col = self._require_collection(collection)
            live_candidates = candidate_indices[candidate_indices < col.count]
            if live_candidates.size:
                live_candidates = live_candidates[col.active[live_candidates]]
            candidate_indices = live_candidates
            if diagnostics is not None:
                diagnostics["scored_candidate_count"] = int(candidate_indices.size)
            if candidate_indices.size == 0:
                return {"hits": [], "diagnostics": diagnostics} if diagnostics is not None else []

            matrix_snap = col.matrix[candidate_indices].copy()
            norms_snap = col.norms[candidate_indices].copy()
            metric = col.metric
            ids_snap = [col.ids[int(i)] for i in candidate_indices]
            metadata_snap = [dict(col.metadata[int(i)] or {}) for i in candidate_indices]
            payloads_snap = [col.payloads[int(i)] for i in candidate_indices]
            include_vectors_snap = [col.matrix[int(i)].copy() for i in candidate_indices] if include_vectors else None

        scores = matrix_snap @ query_vector
        if metric != "dot":
            query_norm = float(np.linalg.norm(query_vector))
            if query_norm == 0.0:
                scores = np.zeros(len(candidate_indices), dtype=np.float32)
            else:
                denom = norms_snap * np.float32(query_norm)
                scores = np.divide(
                    scores,
                    denom,
                    out=np.zeros(len(candidate_indices), dtype=np.float32),
                    where=denom > 0,
                )

        ranked_positions = np.argpartition(scores, -top_k)[-top_k:]
        ranked_positions = ranked_positions[np.argsort(scores[ranked_positions])[::-1]]

        hits_out = []
        for pos in ranked_positions:
            pos = int(pos)
            item_id = ids_snap[pos]
            if item_id is None:
                continue
            payload = payloads_snap[pos]
            hit: dict[str, Any] = {
                "id": item_id,
                "score": float(scores[pos]),
                "metadata": metadata_snap[pos],
            }
            if compress_results and payload is not None:
                hit["payload"] = self._compress_payload(payload)
            else:
                hit["payload"] = payload
            if include_vectors_snap is not None:
                hit["vector"] = include_vectors_snap[pos].astype(float).tolist()
            hits_out.append(hit)

        if diagnostics is not None:
            return {"hits": hits_out, "diagnostics": diagnostics}
        return hits_out

    def build_ann_index(self, collection: str, *, n_lists: int | None = None) -> dict[str, int]:
        with self._lock:
            col = self._require_collection(collection)
            self._purge_all_expired(col)
            ann_index = self._build_ann_index_locked(col, n_lists=n_lists)
            return {
                "centroids": int(ann_index.centroids.shape[0]),
                "vectors": int(ann_index.build_size),
            }

    def build_hnsw_index(
        self,
        collection: str,
        *,
        M: int | None = None,
        M_max0: int | None = None,
        ef_construction: int | None = None,
        ef_search: int | None = None,
        enable_pq: bool = False,
        pq_segments: int | None = None,
        pq_centroids: int | None = None,
    ) -> dict[str, Any]:
        """Build an HNSW graph index for billion-scale nearest-neighbor search.

        Parameters
        ----------
        M : max edges per node per layer (default 16)
        M_max0 : max edges at ground layer (default 32)
        ef_construction : beam width during build (default 200)
        ef_search : default beam width during query (default 50)
        enable_pq : train Product Quantization codebooks for memory reduction
        """
        with self._lock:
            col = self._require_collection(collection)
            self._purge_all_expired(col)
            live_count = self._live_vector_count(col)

            tuned_M = M if M is not None else (12 if live_count <= 50_000 else 16)
            tuned_M_max0 = M_max0 if M_max0 is not None else tuned_M * 2
            tuned_ef_construction = (
                ef_construction if ef_construction is not None else
                (
                    32 if live_count <= 5_000 else
                    48 if live_count <= 10_000 else
                    64 if live_count <= 50_000 else
                    96
                ) if not _HNSWLIB_AVAILABLE else
                (64 if live_count <= 10_000 else 96 if live_count <= 50_000 else 128)
            )
            tuned_ef_search = (
                ef_search if ef_search is not None else
                (24 if live_count <= 10_000 else 32 if live_count <= 50_000 else 48)
            )
            tuned_pq_segments = pq_segments if pq_segments is not None else 8
            tuned_pq_centroids = pq_centroids if pq_centroids is not None else 256

            config = HNSWConfig(
                M=tuned_M,
                M_max0=tuned_M_max0,
                ef_construction=tuned_ef_construction,
                ef_search=tuned_ef_search,
                metric=col.metric,
                enable_pq=enable_pq,
                pq_segments=tuned_pq_segments,
                pq_centroids=tuned_pq_centroids,
            )
            builder = make_hnsw_builder(config, col.dimensions)
            active_indices = self._active_indices(col)
            hnsw_index = builder.build_from_matrix(active_indices, col.matrix, col.norms)
            col.hnsw_builder = builder
            col.hnsw_dirty = False

            return {
                "vectors": int(hnsw_index.build_size),
                "layers": hnsw_index.layer_count(),
                "max_level": hnsw_index.max_level,
                "M": config.M,
                "M_max0": config.M_max0,
                "ef_construction": config.ef_construction,
                "ef_search": config.ef_search,
                "pq_enabled": hnsw_index.pq is not None,
                "pq_segments": config.pq_segments,
                "pq_centroids": config.pq_centroids,
            }

    def compact(self, collection: str | None = None) -> dict[str, int]:
        with self._lock:
            self._purge_all_expired_all()
            self._wal_handle.flush()
            before_bytes = self._wal_path.stat().st_size if self._wal_path.exists() else 0
            target_names = [collection] if collection is not None else sorted(self._collections.keys())
            for name in target_names:
                col = self._require_collection(name)
                self._compact_collection_locked(col)
            self._rewrite_wal_locked()
            after_bytes = self._wal_path.stat().st_size if self._wal_path.exists() else 0
            return {
                "collections": len(target_names),
                "wal_before_bytes": int(before_bytes),
                "wal_after_bytes": int(after_bytes),
                "bytes_reclaimed": int(max(before_bytes - after_bytes, 0)),
            }

    def stats(self) -> VectorStateStats:
        with self._lock:
            self._wal_handle.flush()
            collection_stats = [
                VectorCollectionStats(
                    name=name,
                    dimensions=collection.dimensions,
                    metric=collection.metric,
                    vectors=self._live_vector_count(collection),
                )
                for name, collection in sorted(self._collections.items())
            ]
            return VectorStateStats(
                collections=len(self._collections),
                vectors=sum(item.vectors for item in collection_stats),
                records_replayed=self._records_replayed,
                wal_size_bytes=self._wal_path.stat().st_size if self._wal_path.exists() else 0,
                collection_stats=collection_stats,
            )

    def flush(self) -> None:
        with self._lock:
            self._wal_handle.flush()

    def close(self) -> None:
        with self._lock:
            try:
                self._wal_handle.flush()
            finally:
                self._wal_handle.close()

    def __enter__(self) -> "VectorStateEngineV2":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _append_record(self, record: dict[str, Any]) -> None:
        raw = self._pack_record(record)
        compressed = _compress(raw)
        frame = self._LEN_STRUCT.pack(len(compressed)) + compressed
        self._wal_handle.write(frame)

    def _replay(self) -> None:
        with self._wal_path.open("rb") as handle:
            while True:
                length_raw = handle.read(self._LEN_STRUCT.size)
                if not length_raw:
                    break
                if len(length_raw) != self._LEN_STRUCT.size:
                    raise ValueError("Corrupt vector WAL length prefix")
                (length,) = self._LEN_STRUCT.unpack(length_raw)
                payload = handle.read(length)
                if len(payload) != length:
                    raise ValueError("Corrupt vector WAL payload")
                raw = _decompress(payload)
                record = self._unpack_record(raw)
                self._apply_record(record)
                self._records_replayed += 1

    def _apply_record(self, record: dict[str, Any]) -> None:
        op = record["op"]
        if op == "create_collection":
            self._collections[str(record["collection"])] = _VectorCollection(
                dimensions=int(record["dimensions"]),
                metric=str(record["metric"]),
                matrix=np.zeros((16, int(record["dimensions"])), dtype=np.float32),
                norms=np.zeros(16, dtype=np.float32),
                active=np.zeros(16, dtype=bool),
                ids=[None] * 16,
                metadata=[None] * 16,
                payloads=[None] * 16,
                expires_at=[None] * 16,
                id_to_index={},
                free_indices=[],
                modality=str(record.get("modality", "vector")),
                version_history={},
                filtered_indices_cache={},
                filtered_mask_cache={},
                hnsw_filter_request_cache={},
            )
            return
        if op == "upsert":
            col = self._require_collection(str(record["collection"]))
            item_id = str(record["id"])
            vector = np.asarray(record["vector"], dtype=np.float32)
            self._validate_vector_dimensions(col, vector)

            index = col.id_to_index.get(item_id)

            # --- Temporal versioning (Feature 6): save previous version ---
            if index is not None and bool(col.active[index]):
                if col.version_history is None:
                    col.version_history = {}
                if item_id not in col.version_history:
                    col.version_history[item_id] = []
                col.version_history[item_id].append(_VectorVersion(
                    vector=col.matrix[index].copy(),
                    metadata=dict(col.metadata[index] or {}),
                    timestamp=float(record.get("ts", time.time())),
                ))

            if index is None:
                index = self._allocate_index(col)
                col.id_to_index[item_id] = index
                col.ids[index] = item_id

            col.matrix[index, :] = vector
            col.norms[index] = float(np.linalg.norm(vector))
            col.metadata[index] = dict(record.get("metadata") or {})
            col.payloads[index] = record.get("payload")
            col.expires_at[index] = self._clean_expires_at(record.get("expires_at"))
            col.active[index] = True
            col.active_indices_dirty = True
            col.filtered_indices_cache = None
            col.filtered_mask_cache = None
            col.hnsw_filter_request_cache = {}
            col.ann_dirty = True
            col.hnsw_dirty = True
            return
        if op == "delete":
            col = self._require_collection(str(record["collection"]))
            index = col.id_to_index.get(str(record["id"]))
            if index is not None:
                self._delete_index(col, index)
            return
        raise ValueError(f"Unknown operation: {op}")

    def _require_collection(self, name: str) -> _VectorCollection:
        collection = self._collections.get(name)
        if collection is None:
            raise KeyError(f"unknown collection: {name!r}")
        return collection

    def _validate_vector_dimensions(self, collection: _VectorCollection, vector: np.ndarray) -> None:
        if int(vector.shape[0]) != collection.dimensions:
            raise ValueError(
                f"vector length {int(vector.shape[0])} does not match collection dimensions {collection.dimensions}"
            )

    def _normalize_vector_input(self, vector: list[float]) -> np.ndarray:
        array = np.asarray(vector, dtype=np.float32)
        if array.ndim != 1 or array.size == 0:
            raise ValueError("vector must be a non-empty array of numbers")
        return array.copy()

    def _metadata_matches(self, metadata: dict[str, Any], filters: dict[str, Any]) -> bool:
        for key, value in filters.items():
            if metadata.get(key) != value:
                return False
        return True

    def _score(self, collection: _VectorCollection, query: np.ndarray, indices: np.ndarray) -> np.ndarray:
        matrix = collection.matrix[indices]
        scores = matrix @ query
        if collection.metric == "dot":
            return scores.astype(np.float32, copy=False)

        query_norm = float(np.linalg.norm(query))
        if query_norm == 0.0:
            return np.zeros(indices.shape[0], dtype=np.float32)

        denom = collection.norms[indices] * np.float32(query_norm)
        return np.divide(
            scores,
            denom,
            out=np.zeros(indices.shape[0], dtype=np.float32),
            where=denom > 0,
        )

    # --- Feature 1: Normalized Compression Distance (NCD) ---

    def _ncd(self, a: bytes, b: bytes) -> float:
        """Compute Normalized Compression Distance between two byte strings.

        NCD(x,y) = [C(xy) - min(C(x), C(y))] / max(C(x), C(y))
        Approximates Kolmogorov complexity via real compression (zstd/gzip).
        """
        ca = len(_compress(a))
        cb = len(_compress(b))
        cab = len(_compress(a + b))
        denominator = max(ca, cb)
        if denominator == 0:
            return 0.0
        return (cab - min(ca, cb)) / denominator

    def _vector_to_bytes(self, vector: np.ndarray) -> bytes:
        """Convert a float32 vector to bytes for NCD computation."""
        return vector.astype(np.float32).tobytes()

    def _ncd_scores(self, collection: _VectorCollection, query: np.ndarray,
                    indices: np.ndarray) -> np.ndarray:
        """Compute NCD-based similarity scores for a batch of candidates.

        Returns scores in [0, 1] where 1 = most similar (inverted NCD).
        Uses cached compressed sizes to avoid redundant compression calls.
        """
        query_bytes = self._vector_to_bytes(query)
        c_query = len(_compress(query_bytes))
        scores = np.zeros(indices.shape[0], dtype=np.float32)
        # Cache compressed sizes per-vector to avoid recomputation across queries
        cache = getattr(collection, '_ncd_cache', None)
        if cache is None:
            collection._ncd_cache = {}  # type: ignore[attr-defined]
            cache = collection._ncd_cache  # type: ignore[attr-defined]
        for i, idx in enumerate(indices):
            idx_int = int(idx)
            if idx_int not in cache:
                cache[idx_int] = len(_compress(self._vector_to_bytes(collection.matrix[idx_int])))
            c_target = cache[idx_int]
            target_bytes = self._vector_to_bytes(collection.matrix[idx_int])
            c_concat = len(_compress(query_bytes + target_bytes))
            ncd = (c_concat - min(c_query, c_target)) / max(c_query, c_target) if max(c_query, c_target) > 0 else 0.0
            scores[i] = 1.0 - min(ncd, 1.0)
        return scores

    # --- Feature 2: BPE Hybrid Ranking ---

    def _bpe_tokenize(self, text: str) -> list[str]:
        """Simple whitespace + subword tokenizer for BPE hybrid ranking.

        Splits on whitespace and common delimiters, then extracts character
        n-grams (3-gram) to approximate BPE subword patterns. This captures
        morphological structure that embedding similarity misses.
        """
        import re
        # Split into word-level tokens
        tokens = re.findall(r'[a-zA-Z0-9_]+', text.lower())
        # Generate character 3-grams for subword patterns
        subwords: list[str] = []
        for token in tokens:
            subwords.append(token)
            for i in range(len(token) - 2):
                subwords.append(token[i:i+3])
        return subwords

    def _bpe_overlap_score(self, tokens_a: list[str], tokens_b: list[str]) -> float:
        """Jaccard overlap between two BPE token sets.

        Captures exact phrase and subword matches that cosine similarity misses.
        """
        if not tokens_a or not tokens_b:
            return 0.0
        set_a = set(tokens_a)
        set_b = set(tokens_b)
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union > 0 else 0.0

    def _bpe_hybrid_scores(self, collection: _VectorCollection, query_text: str,
                           indices: np.ndarray, semantic_scores: np.ndarray,
                           semantic_weight: float = 0.7) -> np.ndarray:
        """Combine semantic embedding scores with BPE token overlap.

        hybrid_score = semantic_weight × cosine_score + (1 - semantic_weight) × bpe_overlap
        Uses cached tokenized payloads to avoid re-tokenizing on every query.
        """
        query_tokens = self._bpe_tokenize(query_text)
        query_set = set(query_tokens)
        hybrid = np.copy(semantic_scores)
        bpe_weight = 1.0 - semantic_weight
        # Cache tokenized payload sets
        cache = getattr(collection, '_bpe_cache', None)
        if cache is None:
            collection._bpe_cache = {}  # type: ignore[attr-defined]
            cache = collection._bpe_cache  # type: ignore[attr-defined]
        for i, idx in enumerate(indices):
            idx_int = int(idx)
            if idx_int not in cache:
                payload = collection.payloads[idx_int]
                if payload is not None and isinstance(payload, str):
                    cache[idx_int] = set(self._bpe_tokenize(payload))
                else:
                    cache[idx_int] = None
            doc_set = cache[idx_int]
            if doc_set is not None:
                intersection = len(query_set & doc_set)
                union = len(query_set | doc_set)
                bpe_score = intersection / union if union > 0 else 0.0
                hybrid[i] = semantic_weight * semantic_scores[i] + bpe_weight * bpe_score
        return hybrid

    # --- Feature 4: GPU-Accelerated Distance Computation ---

    def _score_gpu(self, collection: _VectorCollection, query: np.ndarray,
                   indices: np.ndarray) -> np.ndarray:
        """GPU-accelerated cosine similarity using PyTorch CUDA.

        Falls back to CPU NumPy if no GPU is available.
        """
        if not _HAS_CUDA or _torch is None or indices.shape[0] < 256:
            return self._score(collection, query, indices)

        matrix = collection.matrix[indices]
        q_tensor = _torch.from_numpy(query).cuda()
        m_tensor = _torch.from_numpy(matrix).cuda()

        if collection.metric == "dot":
            scores = (m_tensor @ q_tensor).cpu().numpy().astype(np.float32)
            return scores

        q_norm = _torch.linalg.norm(q_tensor)
        if q_norm == 0.0:
            return np.zeros(indices.shape[0], dtype=np.float32)

        n_tensor = _torch.from_numpy(collection.norms[indices]).cuda()
        dots = m_tensor @ q_tensor
        denom = n_tensor * q_norm
        scores = _torch.where(denom > 0, dots / denom, _torch.zeros_like(dots))
        return scores.cpu().numpy().astype(np.float32)

    # --- Feature 5: Compressed RAG Context Windows ---

    def _compress_payload(self, payload: Any) -> dict[str, Any]:
        """Compress a payload for RAG context window optimization.

        Returns a dict with compressed bytes and metadata so the client can
        reconstruct. Achieves 60-80% reduction on text payloads.
        """
        if payload is None:
            return {"compressed": False, "payload": None}
        if isinstance(payload, str):
            raw = payload.encode("utf-8")
        elif isinstance(payload, dict):
            raw = _json_dumps(payload)
        else:
            raw = str(payload).encode("utf-8")

        compressed = _compress(raw)
        import base64
        return {
            "compressed": True,
            "encoding": "ahana_codec",
            "original_size": len(raw),
            "compressed_size": len(compressed),
            "ratio": round(1.0 - len(compressed) / len(raw), 4) if len(raw) > 0 else 0.0,
            "data": base64.b64encode(compressed).decode("ascii"),
        }

    # --- Feature 6: Temporal Vector Versioning ---

    def get_version_history(self, collection: str, item_id: str,
                            limit: int = 100) -> list[dict[str, Any]]:
        """Return the version history of a vector (time-travel queries)."""
        with self._lock:
            col = self._require_collection(collection)
            if col.version_history is None:
                return []
            versions = col.version_history.get(item_id, [])
            result = []
            for v in versions[-limit:]:
                result.append({
                    "timestamp": v.timestamp,
                    "metadata": v.metadata,
                    "vector": v.vector.tolist(),
                })
            # Include current version
            index = col.id_to_index.get(item_id)
            if index is not None and bool(col.active[index]):
                result.append({
                    "timestamp": time.time(),
                    "metadata": dict(col.metadata[index] or {}),
                    "vector": col.matrix[index].tolist(),
                    "current": True,
                })
            return result

    def query_as_of(self, collection: str, vector: list[float], *,
                    as_of: float, top_k: int = 5,
                    filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Time-travel query: find nearest neighbors as of a given timestamp.

        Reconstructs the state of all vectors at the given timestamp by
        looking up version history.
        """
        with self._lock:
            col = self._require_collection(collection)
            query_vector = self._normalize_vector_input(vector)
            self._validate_vector_dimensions(col, query_vector)

            # Build a snapshot matrix from version histories
            snapshot_ids: list[str] = []
            snapshot_vectors: list[np.ndarray] = []

            for item_id, versions in (col.version_history or {}).items():
                # Find the most recent version at or before as_of
                best: _VectorVersion | None = None
                for v in versions:
                    if v.timestamp <= as_of:
                        best = v
                if best is not None:
                    if filters and not self._metadata_matches(best.metadata, filters):
                        continue
                    snapshot_ids.append(item_id)
                    snapshot_vectors.append(best.vector)

            # Also include current vectors with upsert time <= as_of
            for item_id in list(col.id_to_index.keys()):
                if item_id in {s for s in snapshot_ids}:
                    continue
                idx = col.id_to_index[item_id]
                if not bool(col.active[idx]):
                    continue
                # Check if this id has NO version history (means it was
                # inserted once and never updated, use current if within range)
                if item_id not in (col.version_history or {}):
                    # No history — assume current is the only version, include it
                    if filters:
                        meta = col.metadata[idx] or {}
                        if not self._metadata_matches(meta, filters):
                            continue
                    snapshot_ids.append(item_id)
                    snapshot_vectors.append(col.matrix[idx])

            if not snapshot_vectors:
                return []

            snapshot_matrix = np.stack(snapshot_vectors).astype(np.float32)
            norms = np.linalg.norm(snapshot_matrix, axis=1).astype(np.float32)

            # Score
            scores = snapshot_matrix @ query_vector
            q_norm = float(np.linalg.norm(query_vector))
            if q_norm > 0:
                denom = norms * np.float32(q_norm)
                scores = np.divide(scores, denom,
                                   out=np.zeros(len(snapshot_ids), dtype=np.float32),
                                   where=denom > 0)

            top_k = min(top_k, len(snapshot_ids))
            top_indices = np.argpartition(scores, -top_k)[-top_k:]
            top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]

            return [
                {"id": snapshot_ids[int(i)], "score": float(scores[int(i)])}
                for i in top_indices
            ]

    def drift_detection(self, collection: str, item_id: str) -> dict[str, Any]:
        """Detect embedding drift for a specific vector over time.

        Returns cosine distance between consecutive versions and overall drift.
        """
        with self._lock:
            col = self._require_collection(collection)
            if col.version_history is None or item_id not in col.version_history:
                return {"item_id": item_id, "versions": 0, "drifts": []}
            versions = col.version_history[item_id]
            if len(versions) < 2:
                return {"item_id": item_id, "versions": len(versions), "drifts": []}
            drifts = []
            for i in range(1, len(versions)):
                a = versions[i-1].vector
                b = versions[i].vector
                cos_sim = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-9))
                drifts.append({
                    "from_ts": versions[i-1].timestamp,
                    "to_ts": versions[i].timestamp,
                    "cosine_distance": round(1.0 - cos_sim, 6),
                })
            return {
                "item_id": item_id,
                "versions": len(versions) + 1,  # +1 for current
                "total_drift": sum(d["cosine_distance"] for d in drifts),
                "drifts": drifts,
            }

    def _purge_expired_item(self, collection: _VectorCollection, item_id: str) -> None:
        index = collection.id_to_index.get(item_id)
        if index is None:
            return
        expires_at = collection.expires_at[index]
        if expires_at is not None and float(expires_at) <= time.time():
            self._delete_index(collection, index)

    def _purge_all_expired(self, collection: _VectorCollection) -> None:
        now = time.time()
        for index in np.flatnonzero(collection.active[: collection.count]):
            expires_at = collection.expires_at[int(index)]
            if expires_at is not None and float(expires_at) <= now:
                self._delete_index(collection, int(index))

    def _purge_all_expired_all(self) -> None:
        for collection in self._collections.values():
            self._purge_all_expired(collection)

    def _live_vector_count(self, collection: _VectorCollection) -> int:
        self._purge_all_expired(collection)
        return int(self._active_indices(collection).size)

    def _active_indices(self, collection: _VectorCollection) -> np.ndarray:
        cached = collection.active_indices_cache
        if cached is not None and not collection.active_indices_dirty:
            return cached
        active_indices = np.flatnonzero(collection.active[: collection.count]).astype(np.int64, copy=False)
        collection.active_indices_cache = active_indices
        collection.active_indices_dirty = False
        return active_indices

    def _freeze_filter_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return tuple((key, self._freeze_filter_value(val)) for key, val in sorted(value.items()))
        if isinstance(value, list):
            return tuple(self._freeze_filter_value(item) for item in value)
        return value

    def _filter_cache_key(self, filters: dict[str, Any]) -> tuple[Any, ...]:
        return tuple((key, self._freeze_filter_value(value)) for key, value in sorted(filters.items()))

    def _candidate_indices(self, collection: _VectorCollection, filters: dict[str, Any] | None) -> np.ndarray:
        indices = self._active_indices(collection)
        if not filters:
            return indices
        cache = collection.filtered_indices_cache
        if cache is None:
            cache = {}
            collection.filtered_indices_cache = cache
        mask_cache = collection.filtered_mask_cache
        if mask_cache is None:
            mask_cache = {}
            collection.filtered_mask_cache = mask_cache
        cache_key = self._filter_cache_key(filters)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        filtered = [
            int(index)
            for index in indices
            if self._metadata_matches(collection.metadata[int(index)] or {}, filters)
        ]
        filtered_indices = np.asarray(filtered, dtype=np.int64)
        cache[cache_key] = filtered_indices
        mask = np.zeros(max(collection.count, 1), dtype=bool)
        if filtered_indices.size > 0:
            mask[filtered_indices] = True
        mask_cache[cache_key] = mask
        return filtered_indices

    def _candidate_mask(self, collection: _VectorCollection, filters: dict[str, Any] | None) -> np.ndarray | None:
        if not filters:
            return None
        cache_key = self._filter_cache_key(filters)
        mask_cache = collection.filtered_mask_cache
        if mask_cache is None:
            mask_cache = {}
            collection.filtered_mask_cache = mask_cache
        cached = mask_cache.get(cache_key)
        if cached is not None:
            return cached
        self._candidate_indices(collection, filters)
        if collection.filtered_mask_cache is None:
            return None
        return collection.filtered_mask_cache.get(cache_key)

    def _candidate_indices_for_strategy(
        self,
        collection: _VectorCollection,
        query: np.ndarray,
        filters: dict[str, Any] | None,
        *,
        top_k: int,
        strategy: str,
        candidate_multiplier: int,
        ann_probe_count: int | None,
        diagnostics: dict[str, Any] | None = None,
    ) -> np.ndarray:
        active_indices = self._active_indices(collection)
        active_count = int(active_indices.size)
        filter_cache_key: tuple[Any, ...] | None = self._filter_cache_key(filters) if filters else None
        exact_candidates: np.ndarray | None = None
        if strategy != "hnsw" or filters:
            exact_candidates = self._candidate_indices(collection, filters)
        elif not filters:
            exact_candidates = active_indices
        if diagnostics is not None:
            diagnostics["active_candidate_count"] = active_count
            diagnostics["filtered_candidate_count"] = int(exact_candidates.size)
            diagnostics["fallback_used"] = False
            diagnostics["fallback_reason"] = None
            diagnostics["fallback_candidate_count"] = 0
            diagnostics["hnsw_raw_hit_count"] = 0
            diagnostics["hnsw_filtered_hit_count"] = 0
            diagnostics["requested_hnsw_hits"] = 0
            diagnostics["cached_requested_hnsw_hits"] = None
            diagnostics["adapted_requested_hnsw_hits"] = None
            diagnostics["effective_ef_search"] = None
        fallback_candidates = exact_candidates

        if strategy == "exact" or exact_candidates.size <= top_k:
            return exact_candidates

        if strategy == "pq_rerank":
            return self._pq_candidate_indices_locked(
                collection,
                query,
                exact_candidates,
                top_k=top_k,
                candidate_multiplier=candidate_multiplier,
            )

        # --- HNSW strategy: graph-based beam search ---
        if strategy == "hnsw":
            hnsw = self._ensure_hnsw_index_locked(collection)
            if hnsw is None or hnsw.index.node_count == 0:
                if diagnostics is not None:
                    diagnostics["fallback_used"] = True
                    diagnostics["fallback_reason"] = "missing_hnsw_index"
                    diagnostics["fallback_candidate_count"] = int(fallback_candidates.size)
                return fallback_candidates
            base_hnsw_hits = max(top_k * candidate_multiplier, top_k)
            success_hnsw_hit_ceiling = min(active_count, max(base_hnsw_hits * 2, base_hnsw_hits + (top_k * 12)))
            requested_hnsw_hits = base_hnsw_hits
            if filters:
                filtered_count = int(exact_candidates.size)
                target_filtered_hits = min(filtered_count, max(top_k * 2, top_k + max(4, top_k)))
                if filtered_count > 0 and active_count > filtered_count:
                    estimated_hits = int(np.ceil(target_filtered_hits * (active_count / filtered_count)))
                    success_hnsw_hit_ceiling = min(
                        active_count,
                        max(int(np.ceil(base_hnsw_hits * 1.5)), estimated_hits),
                    )
                    requested_hnsw_hits = min(
                        active_count,
                        max(base_hnsw_hits, estimated_hits, int(np.ceil(base_hnsw_hits * 1.5))),
                    )
                request_cache = collection.hnsw_filter_request_cache
                if request_cache is None:
                    request_cache = {}
                    collection.hnsw_filter_request_cache = request_cache
                cached_requested_hnsw_hits = request_cache.get(filter_cache_key) if filter_cache_key is not None else None
                if cached_requested_hnsw_hits is not None:
                    requested_hnsw_hits = max(base_hnsw_hits, min(active_count, int(cached_requested_hnsw_hits)))
                if cached_requested_hnsw_hits is None or int(cached_requested_hnsw_hits) <= success_hnsw_hit_ceiling:
                    requested_hnsw_hits = min(requested_hnsw_hits, success_hnsw_hit_ceiling)
            else:
                cached_requested_hnsw_hits = None
            ef_search = ann_probe_count if ann_probe_count is not None else hnsw.index.config.ef_search
            effective_ef_search = max(int(ef_search), int(requested_hnsw_hits))
            if diagnostics is not None:
                diagnostics["requested_hnsw_hits"] = int(requested_hnsw_hits)
                diagnostics["cached_requested_hnsw_hits"] = cached_requested_hnsw_hits
                diagnostics["effective_ef_search"] = int(effective_ef_search)
                diagnostics["success_hnsw_hit_ceiling"] = int(success_hnsw_hit_ceiling)
            hits = hnsw.search(
                query,
                top_k=int(requested_hnsw_hits),
                ef_search=effective_ef_search,
                matrix=collection.matrix,
                norms=collection.norms,
                active=collection.active,
            )
            if diagnostics is not None:
                diagnostics["hnsw_raw_hit_count"] = int(len(hits))
            if not hits:
                if diagnostics is not None:
                    diagnostics["fallback_used"] = True
                    diagnostics["fallback_reason"] = "empty_hnsw_hits"
                    diagnostics["fallback_candidate_count"] = int(fallback_candidates.size)
                return fallback_candidates
            hnsw_candidates = np.asarray([idx for idx, _ in hits], dtype=np.int64)
            if filters:
                filter_mask = self._candidate_mask(collection, filters)
                if filter_mask is not None and hnsw_candidates.size > 0:
                    hnsw_candidates = hnsw_candidates[filter_mask[hnsw_candidates]]
                elif exact_candidates is not None:
                    hnsw_candidates = np.intersect1d(hnsw_candidates, exact_candidates, assume_unique=False)
                request_cache = collection.hnsw_filter_request_cache
                if request_cache is None:
                    request_cache = {}
                    collection.hnsw_filter_request_cache = request_cache
                observed_filtered_hits = int(hnsw_candidates.size)
                if observed_filtered_hits > 0:
                    recommended_hits = int(np.ceil(requested_hnsw_hits * (target_filtered_hits / observed_filtered_hits)))
                else:
                    recommended_hits = int(requested_hnsw_hits * 2)
                if observed_filtered_hits < top_k:
                    recommended_hits = max(recommended_hits, int(requested_hnsw_hits * 2))
                recommended_hits = max(base_hnsw_hits, min(active_count, recommended_hits))
                previous_hits = int(request_cache.get(filter_cache_key, requested_hnsw_hits)) if filter_cache_key is not None else int(requested_hnsw_hits)
                adapted_hits = max(base_hnsw_hits, min(active_count, int(round((previous_hits + recommended_hits) / 2))))
                if success_hnsw_hit_ceiling > 0:
                    adapted_hits = min(adapted_hits, success_hnsw_hit_ceiling)
                if filter_cache_key is not None:
                    request_cache[filter_cache_key] = adapted_hits
                if diagnostics is not None:
                    diagnostics["adapted_requested_hnsw_hits"] = adapted_hits
            if diagnostics is not None:
                diagnostics["hnsw_filtered_hit_count"] = int(hnsw_candidates.size)
            if hnsw_candidates.size < top_k:
                if diagnostics is not None:
                    diagnostics["fallback_used"] = True
                    diagnostics["fallback_reason"] = "insufficient_filtered_hnsw_hits"
                    diagnostics["fallback_candidate_count"] = int(fallback_candidates.size)
                return fallback_candidates
            return hnsw_candidates

        # --- IVF ann_rerank strategy (legacy) ---
        ann_index = self._ensure_ann_index_locked(collection)
        if ann_index.build_size == 0 or ann_index.centroids.shape[0] == 0:
            return exact_candidates

        target_candidates = min(int(exact_candidates.size), max(top_k * candidate_multiplier, top_k))
        probe_count = ann_probe_count
        if probe_count is None:
            probe_count = min(ann_index.centroids.shape[0], max(1, candidate_multiplier // 2 + 1))
        probe_count = max(1, min(int(probe_count), int(ann_index.centroids.shape[0])))

        centroid_scores = ann_index.centroids @ query
        centroid_order = np.argsort(centroid_scores)[-probe_count:][::-1]

        gathered: list[np.ndarray] = []
        gathered_count = 0
        for centroid_pos in centroid_order:
            bucket = ann_index.buckets[int(centroid_pos)]
            if bucket.size == 0:
                continue
            gathered.append(bucket)
            gathered_count += int(bucket.size)
            if gathered_count >= target_candidates:
                break

        if not gathered:
            return exact_candidates

        approx_candidates = np.unique(np.concatenate(gathered))
        if filters:
            approx_candidates = np.intersect1d(approx_candidates, exact_candidates, assume_unique=False)

        if approx_candidates.size < top_k:
            return exact_candidates
        return approx_candidates

    def _pq_candidate_indices_locked(
        self,
        collection: _VectorCollection,
        query: np.ndarray,
        exact_candidates: np.ndarray,
        *,
        top_k: int,
        candidate_multiplier: int,
    ) -> np.ndarray:
        if collection.metric != "cosine":
            return exact_candidates

        hnsw = self._ensure_hnsw_index_locked(collection)
        if hnsw is None or hnsw.index.pq is None or hnsw.index.pq_index_map is None or hnsw.index.pq.codes is None:
            return exact_candidates

        pq_index_map = hnsw.index.pq_index_map
        valid_in_map = exact_candidates[exact_candidates < pq_index_map.shape[0]]
        if valid_in_map.size < top_k:
            return exact_candidates

        pq_rows = pq_index_map[valid_in_map]
        valid_rows = pq_rows >= 0
        if int(np.count_nonzero(valid_rows)) < top_k:
            return exact_candidates

        pq_candidates = valid_in_map[valid_rows]
        pq_codes = hnsw.index.pq.codes[pq_rows[valid_rows]]
        approx_distances = hnsw.index.pq.asymmetric_distances(query, pq_codes)

        target_candidates = min(int(pq_candidates.size), max(top_k * candidate_multiplier, top_k))
        best_positions = np.argpartition(approx_distances, target_candidates - 1)[:target_candidates]
        return pq_candidates[best_positions]

    def _ensure_ann_index_locked(self, collection: _VectorCollection) -> _AnnIndex:
        ann_index = collection.ann_index
        live_count = self._live_vector_count(collection)
        if ann_index is None or collection.ann_dirty or ann_index.build_size != live_count:
            ann_index = self._build_ann_index_locked(collection)
        return ann_index

    def _ensure_hnsw_index_locked(self, collection: _VectorCollection) -> HNSWBuilder | None:
        """Lazily build or rebuild the HNSW index when dirty.

        Incremental path: when a C++ HNSWLibBackend is already built and only
        new vectors were added (no deletions since last build), we skip the full
        rebuild and call upsert_vector() for each new entry.  This reduces the
        per-upsert overhead from a full O(N) rebuild to O(log N) insert.

        Rebuild cost optimizations:
        - Incremental batch ceiling raised from 512 → 2048: avoids triggering a
          full O(N·ef_construction) rebuild for medium insertions.
        - ef_construction is auto-scaled by collection size on full rebuild so
          small collections rebuild in <1s instead of 14s at ef=200.
        - Deletions at or below 5% of live count use incremental mark+rebuild
          rather than always forcing a full rebuild.
        """
        builder = collection.hnsw_builder
        live_count = self._live_vector_count(collection)
        if builder is not None and not collection.hnsw_dirty and builder.index.build_size == live_count:
            return builder

        # Incremental path: only available for HNSWLibBackend (C++ backend).
        # Also usable after small deletions if the ratio is ≤5% of live count
        # (tombstones don't corrupt graph correctness for small delete ratios).
        has_deletions = getattr(collection, "_hnsw_has_deletions", False)
        deletion_count = getattr(collection, "_hnsw_deletion_count", 0)
        small_deletion = (
            has_deletions
            and live_count > 0
            and deletion_count <= max(1, live_count // 20)  # ≤5% of live count
        )
        if (
            builder is not None
            and isinstance(builder, HNSWLibBackend)
            and collection.hnsw_dirty
            and (not has_deletions or small_deletion)
        ):
            active_indices = self._active_indices(collection)
            # Re-derive: indices not yet in the hnswlib index
            all_new = [int(i) for i in active_indices if int(i) >= int(builder.index.build_size)]
            if all_new and len(all_new) <= 2048:
                # Medium batch — incremental upsert (raised ceiling from 512 → 2048)
                for idx in all_new:
                    builder.upsert_vector(idx, collection.matrix[idx])
                collection.hnsw_dirty = False
                collection._hnsw_has_deletions = False  # type: ignore[attr-defined]
                collection._hnsw_deletion_count = 0  # type: ignore[attr-defined]
                return builder

        # Full rebuild path — use size-tuned ef_construction to avoid O(N·200)
        # cost on small-to-medium collections.
        config = HNSWConfig(metric=collection.metric)
        if builder is not None:
            config = builder.index.config
        else:
            # Auto-tune ef_construction based on live collection size
            tuned_ef = (
                32 if live_count <= 1_000 else
                48 if live_count <= 5_000 else
                64 if live_count <= 20_000 else
                96 if live_count <= 100_000 else
                128
            )
            tuned_M = 12 if live_count <= 50_000 else 16
            config = HNSWConfig(
                M=tuned_M,
                M_max0=tuned_M * 2,
                ef_construction=tuned_ef,
                ef_search=config.ef_search,
                metric=collection.metric,
            )
        builder = make_hnsw_builder(config, collection.dimensions)
        active_indices = self._active_indices(collection)
        builder.build_from_matrix(active_indices, collection.matrix, collection.norms)
        collection.hnsw_builder = builder
        collection.hnsw_dirty = False
        collection._hnsw_has_deletions = False  # type: ignore[attr-defined]
        collection._hnsw_deletion_count = 0  # type: ignore[attr-defined]
        return builder

    def _build_ann_index_locked(self, collection: _VectorCollection, n_lists: int | None = None) -> _AnnIndex:
        active_indices = self._active_indices(collection)
        if active_indices.size == 0:
            ann_index = _AnnIndex(
                centroids=np.zeros((0, collection.dimensions), dtype=np.float32),
                buckets=[],
                build_size=0,
            )
            collection.ann_index = ann_index
            collection.ann_dirty = False
            return ann_index

        matrix = collection.matrix[active_indices]
        normalized = matrix.copy()
        norms = np.linalg.norm(normalized, axis=1, keepdims=True)
        norms[norms == 0.0] = 1.0
        normalized /= norms

        if n_lists is None:
            n_lists = min(max(4, int(np.sqrt(active_indices.size))), 64, int(active_indices.size))
        n_lists = max(1, min(int(n_lists), int(active_indices.size)))

        if n_lists == active_indices.size:
            centroids = normalized.copy()
            buckets = [np.asarray([int(index)], dtype=np.int64) for index in active_indices]
        else:
            seed_positions = np.linspace(0, active_indices.size - 1, num=n_lists, dtype=int)
            centroids = normalized[seed_positions].copy()
            assignments = np.zeros(active_indices.size, dtype=np.int32)
            for _ in range(4):
                similarity = normalized @ centroids.T
                assignments = np.argmax(similarity, axis=1).astype(np.int32, copy=False)
                for centroid_index in range(n_lists):
                    members = normalized[assignments == centroid_index]
                    if members.size == 0:
                        continue
                    centroid = members.mean(axis=0, dtype=np.float32)
                    centroid_norm = float(np.linalg.norm(centroid))
                    if centroid_norm > 0.0:
                        centroid = centroid / centroid_norm
                    centroids[centroid_index] = centroid.astype(np.float32, copy=False)
            buckets = [active_indices[assignments == centroid_index].astype(np.int64, copy=False) for centroid_index in range(n_lists)]

        ann_index = _AnnIndex(
            centroids=centroids.astype(np.float32, copy=False),
            buckets=buckets,
            build_size=int(active_indices.size),
        )
        collection.ann_index = ann_index
        collection.ann_dirty = False
        return ann_index

    def _allocate_index(self, collection: _VectorCollection) -> int:
        if collection.free_indices:
            return int(collection.free_indices.pop())
        if collection.count >= collection.matrix.shape[0]:
            self._grow_collection(collection)
        index = collection.count
        collection.count += 1
        return index

    def _grow_collection(self, collection: _VectorCollection) -> None:
        new_capacity = max(16, collection.matrix.shape[0] * 2)
        new_matrix = np.zeros((new_capacity, collection.dimensions), dtype=np.float32)
        new_matrix[: collection.count] = collection.matrix[: collection.count]
        collection.matrix = new_matrix

        new_norms = np.zeros(new_capacity, dtype=np.float32)
        new_norms[: collection.count] = collection.norms[: collection.count]
        collection.norms = new_norms

        new_active = np.zeros(new_capacity, dtype=bool)
        new_active[: collection.count] = collection.active[: collection.count]
        collection.active = new_active

        extension = new_capacity - len(collection.ids)
        collection.ids.extend([None] * extension)
        collection.metadata.extend([None] * extension)
        collection.payloads.extend([None] * extension)
        collection.expires_at.extend([None] * extension)

    def _delete_index(self, collection: _VectorCollection, index: int) -> None:
        if index >= collection.count or not bool(collection.active[index]):
            return
        item_id = collection.ids[index]
        if item_id is not None:
            collection.id_to_index.pop(item_id, None)
        collection.ids[index] = None
        collection.metadata[index] = None
        collection.payloads[index] = None
        collection.expires_at[index] = None
        collection.matrix[index, :] = 0.0
        collection.norms[index] = 0.0
        collection.active[index] = False
        collection.active_indices_dirty = True
        collection.filtered_indices_cache = None
        collection.filtered_mask_cache = None
        collection.hnsw_filter_request_cache = {}
        if collection.free_indices is None:
            collection.free_indices = []
        collection.free_indices.append(index)
        collection.ann_dirty = True
        collection.hnsw_dirty = True
        collection._hnsw_has_deletions = True  # type: ignore[attr-defined]
        collection._hnsw_deletion_count = getattr(collection, "_hnsw_deletion_count", 0) + 1  # type: ignore[attr-defined]
        if collection.hnsw_builder is not None:
            collection.hnsw_builder.mark_deleted(index)

    def _compact_collection_locked(self, collection: _VectorCollection) -> None:
        active_indices = self._active_indices(collection)
        live_count = int(active_indices.size)
        capacity = max(16, live_count if live_count > 0 else 16)

        new_matrix = np.zeros((capacity, collection.dimensions), dtype=np.float32)
        new_norms = np.zeros(capacity, dtype=np.float32)
        new_active = np.zeros(capacity, dtype=bool)
        new_ids: list[str | None] = [None] * capacity
        new_metadata: list[dict[str, Any] | None] = [None] * capacity
        new_payloads: list[Any] = [None] * capacity
        new_expires_at: list[float | None] = [None] * capacity
        new_id_to_index: dict[str, int] = {}

        for new_index, old_index in enumerate(active_indices.tolist()):
            item_id = collection.ids[int(old_index)]
            if item_id is None:
                continue
            new_matrix[new_index, :] = collection.matrix[int(old_index)]
            new_norms[new_index] = collection.norms[int(old_index)]
            new_active[new_index] = True
            new_ids[new_index] = item_id
            new_metadata[new_index] = collection.metadata[int(old_index)]
            new_payloads[new_index] = collection.payloads[int(old_index)]
            new_expires_at[new_index] = collection.expires_at[int(old_index)]
            new_id_to_index[item_id] = new_index

        collection.matrix = new_matrix
        collection.norms = new_norms
        collection.active = new_active
        collection.ids = new_ids
        collection.metadata = new_metadata
        collection.payloads = new_payloads
        collection.expires_at = new_expires_at
        collection.id_to_index = new_id_to_index
        collection.count = live_count
        collection.free_indices = []
        collection.active_indices_cache = np.flatnonzero(collection.active[: collection.count]).astype(np.int64, copy=False)
        collection.active_indices_dirty = False
        collection.filtered_indices_cache = {}
        collection.filtered_mask_cache = {}
        collection.hnsw_filter_request_cache = {}
        collection.ann_dirty = True
        collection.ann_index = None
        collection.hnsw_dirty = True
        collection.hnsw_builder = None

    def _rewrite_wal_locked(self) -> None:
        self._wal_handle.flush()
        self._wal_handle.close()

        tmp_path = self._wal_path.with_suffix(self._wal_path.suffix + ".compact")
        with tmp_path.open("wb") as handle:
            for name, collection in sorted(self._collections.items()):
                create_record = {
                    "op": "create_collection",
                    "collection": name,
                    "dimensions": collection.dimensions,
                    "metric": collection.metric,
                    "ts": time.time(),
                }
                self._append_record_to_handle(handle, create_record)
                for index in np.flatnonzero(collection.active[: collection.count]).tolist():
                    item_id = collection.ids[int(index)]
                    if item_id is None:
                        continue
                    upsert_record = {
                        "op": "upsert",
                        "collection": name,
                        "id": item_id,
                        "vector": collection.matrix[int(index)],
                        "metadata": collection.metadata[int(index)] or {},
                        "payload": collection.payloads[int(index)],
                        "expires_at": collection.expires_at[int(index)],
                        "ts": time.time(),
                    }
                    self._append_record_to_handle(handle, upsert_record)

        tmp_path.replace(self._wal_path)
        self._wal_handle = open(self._wal_path, "ab")

    def _append_record_to_handle(self, handle: io.BufferedWriter, record: dict[str, Any]) -> None:
        raw = self._pack_record(record)
        compressed = _compress(raw)
        frame = self._LEN_STRUCT.pack(len(compressed)) + compressed
        handle.write(frame)

    def _clean_expires_at(self, expires_at: Any) -> float | None:
        if expires_at is None:
            return None
        return float(expires_at)

    def _pack_record(self, record: dict[str, Any]) -> bytes:
        if record["op"] == "upsert":
            vector = np.asarray(record["vector"], dtype=np.float32)
            header = {
                "op": "upsert",
                "collection": record["collection"],
                "id": record["id"],
                "metadata": record.get("metadata") or {},
                "payload": record.get("payload"),
                "expires_at": record.get("expires_at"),
                "ts": record.get("ts"),
                "dimensions": int(vector.shape[0]),
                "dtype": "float32",
            }
            header_bytes = _json_dumps(header)
            return _RECORD_MAGIC + _JSON_LEN_STRUCT.pack(len(header_bytes)) + header_bytes + vector.tobytes()

        header_bytes = _json_dumps(record)
        return _RECORD_MAGIC + _JSON_LEN_STRUCT.pack(len(header_bytes)) + header_bytes

    def _unpack_record(self, raw: bytes) -> dict[str, Any]:
        if raw.startswith(_RECORD_MAGIC):
            if len(raw) < len(_RECORD_MAGIC) + _JSON_LEN_STRUCT.size:
                raise ValueError("Corrupt vector record header")
            offset = len(_RECORD_MAGIC)
            (header_length,) = _JSON_LEN_STRUCT.unpack(raw[offset : offset + _JSON_LEN_STRUCT.size])
            offset += _JSON_LEN_STRUCT.size
            header_end = offset + header_length
            header = _json_loads(raw[offset:header_end])
            if header["op"] == "upsert":
                vector_bytes = raw[header_end:]
                dimensions = int(header["dimensions"])
                expected = dimensions * 4
                if len(vector_bytes) != expected:
                    raise ValueError("Corrupt vector record payload")
                header["vector"] = np.frombuffer(vector_bytes, dtype=np.float32).copy()
            return header

        if raw[:1] == b"{":
            record = _json_loads(raw)
            if record.get("op") == "upsert":
                record["vector"] = np.asarray(record["vector"], dtype=np.float32)
            return record

        raise ValueError("Unsupported vector record format")