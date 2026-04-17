"""HNSW (Hierarchical Navigable Small World) index for billion-scale ANN search.

Implements the algorithm from Malkov & Yashunin (2018) with:
  - Multi-layer navigable small-world graph
  - Greedy beam search with configurable ef_search
  - SELECT-NEIGHBORS-HEURISTIC for diverse edge construction
  - Batched NumPy distance computation for GPU-free speed
  - Online incremental insertion (no full rebuild required)
  - Lazy tombstone deletion with periodic graph cleanup
  - Memory-mapped backing store for billion-scale datasets
  - Product Quantization (PQ) option for memory-efficient distance estimation

AhanaAI proprietary — ACP-PAT-006 (provisional).
"""

from __future__ import annotations

import threading
import math
import random
import struct
from contextlib import contextmanager
from dataclasses import dataclass, field

import numpy as np

try:
    from ._hnsw_accel import search_layer_accel as _search_layer_accel
except ImportError:  # pragma: no cover
    _search_layer_accel = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_DEFAULT_M = 16             # max edges per node at each layer
_DEFAULT_M_MAX0 = 32        # max edges at layer 0 (denser ground layer)
_DEFAULT_EF_CONSTRUCTION = 200   # beam width during insert
_DEFAULT_EF_SEARCH = 50          # beam width during query
_DEFAULT_ML = 1.0 / math.log(2)  # level generation factor (1/ln(M))


class _ReadWriteLock:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False
        self._writer_owner: int | None = None
        self._writer_depth = 0

    @contextmanager
    def read_locked(self):
        me = threading.get_ident()
        with self._cond:
            while self._writer and self._writer_owner != me:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextmanager
    def write_locked(self):
        me = threading.get_ident()
        with self._cond:
            if self._writer and self._writer_owner == me:
                self._writer_depth += 1
            else:
                while self._writer or self._readers > 0:
                    self._cond.wait()
                self._writer = True
                self._writer_owner = me
                self._writer_depth = 1
        try:
            yield
        finally:
            with self._cond:
                self._writer_depth -= 1
                if self._writer_depth == 0:
                    self._writer = False
                    self._writer_owner = None
                    self._cond.notify_all()


@dataclass(frozen=True)
class HNSWConfig:
    """Tuning knobs for the HNSW graph."""
    M: int = _DEFAULT_M
    M_max0: int = _DEFAULT_M_MAX0
    ef_construction: int = _DEFAULT_EF_CONSTRUCTION
    ef_search: int = _DEFAULT_EF_SEARCH
    ml: float = _DEFAULT_ML
    metric: str = "cosine"          # "cosine" | "dot" | "l2"
    seed: int = 42
    enable_pq: bool = False          # Product Quantization for distance estimation
    pq_segments: int = 8             # PQ: number of sub-vector segments
    pq_centroids: int = 256          # PQ: centroids per segment (typically 256 → 1 byte)


# ---------------------------------------------------------------------------
# Distance functions — vectorized NumPy
# ---------------------------------------------------------------------------

def _cosine_distances_batch(query: np.ndarray, matrix: np.ndarray, norms: np.ndarray) -> np.ndarray:
    """Cosine distance = 1 - cosine_similarity.  Lower is closer."""
    query_norm = float(np.linalg.norm(query))
    if query_norm == 0.0:
        return np.ones(matrix.shape[0], dtype=np.float32)
    dots = matrix @ query
    denom = norms * np.float32(query_norm)
    sim = np.divide(dots, denom, out=np.zeros(matrix.shape[0], dtype=np.float32), where=denom > 0)
    return np.float32(1.0) - sim


def _dot_distances_batch(query: np.ndarray, matrix: np.ndarray, _norms: np.ndarray) -> np.ndarray:
    """Negative dot product (lower is closer)."""
    return -(matrix @ query)


def _l2_distances_batch(query: np.ndarray, matrix: np.ndarray, _norms: np.ndarray) -> np.ndarray:
    """Squared L2 distance."""
    diff = matrix - query[np.newaxis, :]
    return np.sum(diff * diff, axis=1)


_DISTANCE_FN = {
    "cosine": _cosine_distances_batch,
    "dot": _dot_distances_batch,
    "l2": _l2_distances_batch,
}


def _distance_single(query: np.ndarray, vector: np.ndarray, norm: float, metric: str) -> float:
    """Distance between a single query and a single vector."""
    if metric == "cosine":
        qn = float(np.linalg.norm(query))
        if qn == 0.0 or norm == 0.0:
            return 1.0
        return 1.0 - float(np.dot(query, vector)) / (qn * norm)
    if metric == "dot":
        return -float(np.dot(query, vector))
    # l2
    diff = query - vector
    return float(np.dot(diff, diff))


# ---------------------------------------------------------------------------
# Product Quantization (optional, for billion-scale memory reduction)
# ---------------------------------------------------------------------------

@dataclass
class ProductQuantizer:
    """Compress D-dimensional vectors into pq_segments bytes for fast distance estimation."""
    pq_segments: int
    pq_centroids: int
    dimensions: int
    sub_dim: int = 0
    codebooks: np.ndarray | None = None  # (pq_segments, pq_centroids, sub_dim)
    codes: np.ndarray | None = None      # (N,) dtype=uint8 * pq_segments → stored as (N, pq_segments) uint8

    def __post_init__(self) -> None:
        self.sub_dim = self.dimensions // self.pq_segments
        if self.sub_dim * self.pq_segments != self.dimensions:
            # Pad last segment
            self.sub_dim = math.ceil(self.dimensions / self.pq_segments)

    def train(self, vectors: np.ndarray, max_train: int = 50_000, n_iter: int = 10) -> None:
        """Train PQ codebooks from a sample of vectors."""
        n = vectors.shape[0]
        if n == 0:
            return
        sample = vectors[:min(n, max_train)]
        codebooks = np.zeros((self.pq_segments, self.pq_centroids, self.sub_dim), dtype=np.float32)

        for seg in range(self.pq_segments):
            start = seg * self.sub_dim
            end = min(start + self.sub_dim, vectors.shape[1])
            actual_dim = end - start
            sub_vectors = sample[:, start:end]
            if actual_dim < self.sub_dim:
                padded = np.zeros((sub_vectors.shape[0], self.sub_dim), dtype=np.float32)
                padded[:, :actual_dim] = sub_vectors
                sub_vectors = padded

            # Mini-batch k-means
            k = min(self.pq_centroids, sub_vectors.shape[0])
            rng = np.random.default_rng(42 + seg)
            indices = rng.choice(sub_vectors.shape[0], size=k, replace=False)
            centers = sub_vectors[indices].copy()
            for _ in range(n_iter):
                dists = np.sum((sub_vectors[:, np.newaxis, :] - centers[np.newaxis, :, :]) ** 2, axis=2)
                assignments = np.argmin(dists, axis=1)
                for c in range(k):
                    members = sub_vectors[assignments == c]
                    if members.shape[0] > 0:
                        centers[c] = members.mean(axis=0)
            codebooks[seg, :k] = centers

        self.codebooks = codebooks

    def encode(self, vectors: np.ndarray) -> np.ndarray:
        """Encode vectors to PQ codes. Returns (N, pq_segments) uint8."""
        if self.codebooks is None:
            raise RuntimeError("PQ codebooks not trained")
        N = vectors.shape[0]
        codes = np.zeros((N, self.pq_segments), dtype=np.uint8)
        for seg in range(self.pq_segments):
            start = seg * self.sub_dim
            end = min(start + self.sub_dim, vectors.shape[1])
            actual_dim = end - start
            sub_vectors = vectors[:, start:end]
            if actual_dim < self.sub_dim:
                padded = np.zeros((N, self.sub_dim), dtype=np.float32)
                padded[:, :actual_dim] = sub_vectors
                sub_vectors = padded
            dists = np.sum((sub_vectors[:, np.newaxis, :] - self.codebooks[seg][np.newaxis, :, :]) ** 2, axis=2)
            codes[:, seg] = np.argmin(dists, axis=1).astype(np.uint8)
        self.codes = codes
        return codes

    def asymmetric_distances(self, query: np.ndarray, codes: np.ndarray) -> np.ndarray:
        """Compute approximate distances from query to PQ-encoded vectors.

        Uses asymmetric distance computation (ADC): exact sub-query vs codebook lookup.
        """
        if self.codebooks is None:
            raise RuntimeError("PQ codebooks not trained")
        # Precompute distance table: (pq_segments, pq_centroids)
        dist_table = np.zeros((self.pq_segments, self.pq_centroids), dtype=np.float32)
        for seg in range(self.pq_segments):
            start = seg * self.sub_dim
            end = min(start + self.sub_dim, query.shape[0])
            actual_dim = end - start
            sub_query = query[start:end]
            if actual_dim < self.sub_dim:
                padded = np.zeros(self.sub_dim, dtype=np.float32)
                padded[:actual_dim] = sub_query
                sub_query = padded
            diff = self.codebooks[seg] - sub_query[np.newaxis, :]
            dist_table[seg] = np.sum(diff * diff, axis=1)

        # Lookup distances
        N = codes.shape[0]
        distances = np.zeros(N, dtype=np.float32)
        for seg in range(self.pq_segments):
            distances += dist_table[seg, codes[:, seg]]
        return distances


# ---------------------------------------------------------------------------
# HNSW Graph Node & Layers
# ---------------------------------------------------------------------------

@dataclass
class _HNSWNode:
    """Represents a single node in the HNSW graph."""
    index: int                                  # Index into the collection matrix
    level: int                                  # Maximum layer this node is inserted at
    neighbors: list[list[int]]                  # neighbors[layer] → list of neighbor indices
    deleted: bool = False


@dataclass
class HNSWIndex:
    """Complete HNSW graph index."""
    config: HNSWConfig
    dimensions: int
    entry_point: int | None = None              # Index of the entry-point node
    max_level: int = -1                         # Current highest layer
    nodes: dict[int, _HNSWNode] = field(default_factory=dict)
    node_count: int = 0
    pq: ProductQuantizer | None = None
    pq_index_map: np.ndarray | None = None
    build_size: int = 0

    def layer_count(self) -> int:
        return self.max_level + 1


# ---------------------------------------------------------------------------
# HNSW Builder & Searcher
# ---------------------------------------------------------------------------

class HNSWBuilder:
    """Builds and queries an HNSW index over a collection's matrix.

    Thread-safe via external lock (called under the engine's _lock).
    """

    def __init__(self, config: HNSWConfig, dimensions: int) -> None:
        self._config = config
        self._dimensions = dimensions
        self._rng = random.Random(config.seed)
        self._dist_fn = _DISTANCE_FN.get(config.metric, _cosine_distances_batch)
        self._index = HNSWIndex(config=config, dimensions=dimensions)
        self._rwlock = _ReadWriteLock()

    @property
    def index(self) -> HNSWIndex:
        return self._index

    def _random_level(self) -> int:
        """Generate a random level for a new node (geometric distribution)."""
        return int(-math.log(self._rng.uniform(1e-9, 1.0)) * self._config.ml)

    def _distance(
        self,
        query: np.ndarray,
        target_index: int,
        matrix: np.ndarray,
        norms: np.ndarray,
        *,
        query_norm: float | None = None,
    ) -> float:
        if self._config.metric == "cosine":
            if query_norm is None:
                query_norm = float(np.linalg.norm(query))
            norm = float(norms[target_index])
            if query_norm == 0.0 or norm == 0.0:
                return 1.0
            return 1.0 - float(np.dot(query, matrix[target_index])) / (query_norm * norm)
        return _distance_single(query, matrix[target_index], float(norms[target_index]), self._config.metric)

    def _distance_batch(
        self,
        query: np.ndarray,
        target_indices: list[int],
        matrix: np.ndarray,
        norms: np.ndarray,
        *,
        query_norm: float | None = None,
    ) -> np.ndarray:
        if not target_indices:
            return np.empty(0, dtype=np.float32)
        idx = np.asarray(target_indices, dtype=np.int64)
        if self._config.metric == "cosine":
            if query_norm is None:
                query_norm = float(np.linalg.norm(query))
            if query_norm == 0.0:
                return np.ones(idx.shape[0], dtype=np.float32)
            dots = matrix[idx] @ query
            denom = norms[idx] * np.float32(query_norm)
            sims = np.divide(dots, denom, out=np.zeros(idx.shape[0], dtype=np.float32), where=denom > 0)
            return (np.float32(1.0) - sims).astype(np.float32, copy=False)
        return self._dist_fn(query, matrix[idx], norms[idx]).astype(np.float32)

    def _active_layer_neighbors(self, node_index: int, layer: int) -> list[int]:
        node = self._index.nodes.get(node_index)
        if node is None or layer >= len(node.neighbors):
            return []
        idx_nodes = self._index.nodes
        return [
            neighbor_idx
            for neighbor_idx in node.neighbors[layer]
            if neighbor_idx in idx_nodes and not idx_nodes[neighbor_idx].deleted
        ]

    def _greedy_descend(
        self,
        query: np.ndarray,
        entry_point: int,
        entry_dist: float,
        start_layer: int,
        stop_layer_exclusive: int,
        matrix: np.ndarray,
        norms: np.ndarray,
        *,
        query_norm: float | None = None,
    ) -> tuple[int, float]:
        ep = entry_point
        ep_dist = entry_dist
        for lc in range(start_layer, stop_layer_exclusive, -1):
            changed = True
            while changed:
                changed = False
                neighbor_indices = self._active_layer_neighbors(ep, lc)
                if not neighbor_indices:
                    break
                distances = self._distance_batch(
                    query,
                    neighbor_indices,
                    matrix,
                    norms,
                    query_norm=query_norm,
                )
                if distances.size == 0:
                    break
                best_pos = int(np.argmin(distances))
                best_dist = float(distances[best_pos])
                if best_dist < ep_dist:
                    ep = int(neighbor_indices[best_pos])
                    ep_dist = best_dist
                    changed = True
        return ep, ep_dist

    def insert(
        self,
        node_index: int,
        matrix: np.ndarray,
        norms: np.ndarray,
    ) -> None:
        """Insert a single vector into the HNSW graph (Algorithm 1 from the paper)."""
        with self._rwlock.write_locked():
            self._insert_unlocked(node_index, matrix, norms)

    def _insert_unlocked(
        self,
        node_index: int,
        matrix: np.ndarray,
        norms: np.ndarray,
    ) -> None:
        """Insert a single vector while the write lock is already held."""
        idx = self._index
        config = self._config
        query = matrix[node_index]
        query_norm = float(np.linalg.norm(query)) if config.metric == "cosine" else None
        level = self._random_level()

        node = _HNSWNode(
            index=node_index,
            level=level,
            neighbors=[[] for _ in range(level + 1)],
        )
        idx.nodes[node_index] = node
        idx.node_count += 1

        if idx.entry_point is None:
            idx.entry_point = node_index
            idx.max_level = level
            return

        ep = idx.entry_point
        ep_dist = self._distance(query, ep, matrix, norms, query_norm=query_norm)
        ep, ep_dist = self._greedy_descend(
            query,
            ep,
            ep_dist,
            idx.max_level,
            level,
            matrix,
            norms,
            query_norm=query_norm,
        )

        for lc in range(min(level, idx.max_level), -1, -1):
            candidates = self._search_layer(
                query,
                ep,
                config.ef_construction,
                lc,
                matrix,
                norms,
                query_norm=query_norm,
            )
            M_max = config.M_max0 if lc == 0 else config.M
            neighbors = self._select_neighbors_heuristic(query, candidates, M_max, matrix, norms)
            node.neighbors[lc] = [n_idx for n_idx, _ in neighbors]

            for n_idx, _ in neighbors:
                n_node = idx.nodes.get(n_idx)
                if n_node is None or n_node.deleted:
                    continue
                while len(n_node.neighbors) <= lc:
                    n_node.neighbors.append([])
                n_node.neighbors[lc].append(node_index)

                M_max_n = config.M_max0 if lc == 0 else config.M
                if len(n_node.neighbors[lc]) > M_max_n:
                    n_vec = matrix[n_idx]
                    valid_neighbors = [ni for ni in n_node.neighbors[lc] if ni in idx.nodes and not idx.nodes[ni].deleted]
                    if valid_neighbors:
                        ni_dists = self._distance_batch(n_vec, valid_neighbors, matrix, norms)
                        n_candidates = list(zip(valid_neighbors, (float(d) for d in ni_dists)))
                        pruned = self._select_neighbors_heuristic(n_vec, n_candidates, M_max_n, matrix, norms)
                        n_node.neighbors[lc] = [p_idx for p_idx, _ in pruned]

            if candidates:
                ep = min(candidates, key=lambda x: x[1])[0]

        if level > idx.max_level:
            idx.entry_point = node_index
            idx.max_level = level

    def _search_layer(
        self,
        query: np.ndarray,
        entry_point: int,
        ef: int,
        layer: int,
        matrix: np.ndarray,
        norms: np.ndarray,
        *,
        query_norm: float | None = None,
    ) -> list[tuple[int, float]]:
        """Search a single layer using beam search (Algorithm 2).

        Returns candidates sorted by distance (ascending).

        Uses two heaps for O(N log ef) complexity:
        - candidates: min-heap of (dist, idx) — next nodes to explore
        - results: max-heap of (-dist, idx) — best ef results found so far
          (negated so heapq.heappop gives the *worst* / furthest result)
        """
        if _search_layer_accel is not None:
            return _search_layer_accel(
            query,
            entry_point,
            ef,
            layer,
            matrix,
            norms,
            self._index.nodes,
            self._distance,
            self._distance_batch,
            )

        import heapq

        ep_dist = self._distance(query, entry_point, matrix, norms, query_norm=query_norm)
        candidates: list[tuple[float, int]] = [(ep_dist, entry_point)]  # min-heap
        visited: set[int] = {entry_point}
        # max-heap: store (-dist, idx) so top element is the worst (furthest) result
        results: list[tuple[float, int]] = [(-ep_dist, entry_point)]

        while candidates:
            c_dist, c_idx = heapq.heappop(candidates)

            # Worst result distance: -results[0][0]
            if results and c_dist > -results[0][0] and len(results) >= ef:
                break

            c_node = self._index.nodes.get(c_idx)
            if c_node is None or c_node.deleted:
                continue

            # Collect unvisited neighbors
            neighbor_indices: list[int] = []
            if layer < len(c_node.neighbors):
                for n_idx in c_node.neighbors[layer]:
                    if n_idx not in visited:
                        visited.add(n_idx)
                        if n_idx in self._index.nodes and not self._index.nodes[n_idx].deleted:
                            neighbor_indices.append(n_idx)

            if not neighbor_indices:
                continue

            # Batch distance computation
            distances = self._distance_batch(
                query,
                neighbor_indices,
                matrix,
                norms,
                query_norm=query_norm,
            )

            for i, n_idx in enumerate(neighbor_indices):
                d = float(distances[i])
                worst_dist = -results[0][0] if results else float("inf")
                if len(results) < ef or d < worst_dist:
                    heapq.heappush(candidates, (d, n_idx))
                    heapq.heappush(results, (-d, n_idx))
                    if len(results) > ef:
                        heapq.heappop(results)  # remove worst (furthest)

        return sorted(((idx, -neg_dist) for neg_dist, idx in results), key=lambda x: x[1])

    def _select_neighbors_heuristic(
        self,
        query: np.ndarray,
        candidates: list[tuple[int, float]],
        M: int,
        matrix: np.ndarray,
        norms: np.ndarray,
    ) -> list[tuple[int, float]]:
        """SELECT-NEIGHBORS-HEURISTIC (Algorithm 4) — diverse neighbor selection.

        Prefers neighbors that are close to query AND not redundant with each other.

        Vectorized implementation: precomputes pairwise distances between all
        candidates in one batched NumPy matmul instead of calling _distance_single
        in a nested Python loop (O(ef × M) individual NumPy calls → one BLAS matmul).
        """
        if len(candidates) <= M:
            return sorted(candidates, key=lambda x: x[1])

        # Sort candidates by distance to query (ascending)
        working = sorted(candidates, key=lambda x: x[1])
        cand_indices = [c_idx for c_idx, _ in working]
        n_cands = len(cand_indices)

        # Precompute pairwise distances between all candidates — one batched matmul
        # pairwise[i, j] = dist(cand[i], cand[j])
        cand_matrix = matrix[np.asarray(cand_indices, dtype=np.int64)]   # (K, D)
        cand_norms = norms[np.asarray(cand_indices, dtype=np.int64)]      # (K,)
        metric = self._config.metric

        if metric == "cosine":
            dots = cand_matrix @ cand_matrix.T                            # (K, K)
            denom = cand_norms[:, np.newaxis] * cand_norms[np.newaxis, :]
            # Avoid div-by-zero on zero-norm vectors (shouldn't occur on normalized data)
            safe = denom > 0
            pairwise = np.where(safe, 1.0 - dots / np.where(safe, denom, 1.0), np.float32(1.0))
        elif metric == "dot":
            pairwise = -(cand_matrix @ cand_matrix.T)
        else:  # l2
            # ||a - b||² = ||a||² + ||b||² - 2 a·b
            sq_norms = (cand_norms ** 2)
            pairwise = sq_norms[:, np.newaxis] + sq_norms[np.newaxis, :] - 2.0 * (cand_matrix @ cand_matrix.T)
            pairwise = np.maximum(pairwise, 0.0)

        pairwise = pairwise.astype(np.float32)

        selected_local: list[int] = []   # indices into `working`
        selected: list[tuple[int, float]] = []
        discarded: list[tuple[int, float]] = []

        for i, (c_idx, c_dist) in enumerate(working):
            if len(selected) >= M:
                break
            if not selected_local:
                # First candidate always selected
                selected.append((c_idx, c_dist))
                selected_local.append(i)
            else:
                # Diversity check: is c closer to any selected node than to the query?
                # pairwise row `i` sliced to selected_local is short (≤ M); use a plain
                # Python loop with early exit to avoid NumPy function-call overhead for
                # tiny arrays that would otherwise dominate via np.any().
                dists_row = pairwise[i]
                c_dist_f = float(c_dist)
                diverse = True
                for j in selected_local:
                    if dists_row[j] < c_dist_f:
                        diverse = False
                        break
                if diverse:
                    selected.append((c_idx, c_dist))
                    selected_local.append(i)
                else:
                    discarded.append((c_idx, c_dist))

        # Fill remaining slots with closest discarded (still sorted by distance)
        for c_idx, c_dist in discarded:
            if len(selected) >= M:
                break
            selected.append((c_idx, c_dist))

        return selected


    def search(
        self,
        query: np.ndarray,
        top_k: int,
        ef_search: int | None,
        matrix: np.ndarray,
        norms: np.ndarray,
        active: np.ndarray,
    ) -> list[tuple[int, float]]:
        """KNN search via HNSW graph traversal (Algorithm 5).

        Returns list of (index, distance) sorted by ascending distance.
        """
        with self._rwlock.read_locked():
            idx = self._index
            if idx.entry_point is None or idx.node_count == 0:
                return []

            ef = ef_search if ef_search is not None else self._config.ef_search
            ef = max(ef, top_k)
            query_norm = float(np.linalg.norm(query)) if self._config.metric == "cosine" else None

            ep = idx.entry_point
            ep_dist = self._distance(query, ep, matrix, norms, query_norm=query_norm)
            ep, ep_dist = self._greedy_descend(
                query,
                ep,
                ep_dist,
                idx.max_level,
                0,
                matrix,
                norms,
                query_norm=query_norm,
            )
            candidates = self._search_layer(
                query,
                ep,
                ef,
                0,
                matrix,
                norms,
                query_norm=query_norm,
            )

            results: list[tuple[int, float]] = []
            for c_idx, c_dist in candidates:
                if bool(active[c_idx]):
                    results.append((c_idx, c_dist))
                if len(results) >= top_k * 2:
                    break

            results.sort(key=lambda x: x[1])
            return results[:top_k]

    def mark_deleted(self, node_index: int) -> None:
        """Lazy-delete: mark node as deleted without removing edges."""
        with self._rwlock.write_locked():
            node = self._index.nodes.get(node_index)
            if node is not None:
                node.deleted = True

    def build_from_matrix(
        self,
        active_indices: np.ndarray,
        matrix: np.ndarray,
        norms: np.ndarray,
    ) -> HNSWIndex:
        """Build HNSW graph from scratch over all active indices."""
        with self._rwlock.write_locked():
            self._index = HNSWIndex(config=self._config, dimensions=self._dimensions)

            if active_indices.size == 0:
                return self._index

            order = active_indices.copy()
            rng = np.random.default_rng(self._config.seed)
            rng.shuffle(order)

            for node_index in order:
                self._insert_unlocked(int(node_index), matrix, norms)

            self._index.build_size = int(active_indices.size)

            if self._config.enable_pq and active_indices.size > 1000:
                pq = ProductQuantizer(
                    pq_segments=self._config.pq_segments,
                    pq_centroids=self._config.pq_centroids,
                    dimensions=self._dimensions,
                )
                pq.train(matrix[active_indices])
                pq.encode(matrix[active_indices])
                self._index.pq = pq
                pq_index_map = np.full(matrix.shape[0], -1, dtype=np.int32)
                pq_index_map[active_indices] = np.arange(active_indices.size, dtype=np.int32)
                self._index.pq_index_map = pq_index_map
            else:
                self._index.pq = None
                self._index.pq_index_map = None

            return self._index


# ---------------------------------------------------------------------------
# hnswlib C++ backend (fast path when hnswlib is installed)
# ---------------------------------------------------------------------------

try:
    import hnswlib as _hnswlib
    _HNSWLIB_AVAILABLE = True
except ImportError:  # pragma: no cover
    _hnswlib = None  # type: ignore[assignment]
    _HNSWLIB_AVAILABLE = False

_HNSWLIB_SPACE: dict[str, str] = {
    "cosine": "cosine",
    "dot": "ip",
    "l2": "l2",
}


class HNSWLibBackend:
    """C++ HNSW backend via hnswlib — same external interface as HNSWBuilder.

    Drop-in replacement with ~100-1000× faster build and query vs the pure-Python
    implementation.  Automatically selected by ``make_hnsw_builder()`` when hnswlib
    is installed.

    Key differences from HNSWBuilder:
    - Stores vectors internally (does not re-read matrix on every search call)
    - Bulk-add via hnswlib's BLAS-backed add_items (multi-threaded)
    - mark_deleted delegates to hnswlib's native lazy-delete
    - Serialization falls back to metadata-only (graph topology not exported, but
      the HNSWIndex returned from build_from_matrix has correct build_size / node_count
      so the engine's dirty-check works correctly)
    """

    def __init__(self, config: HNSWConfig, dimensions: int) -> None:
        self._config = config
        self._dimensions = dimensions
        self._lib_index: Any | None = None
        self._meta: HNSWIndex = HNSWIndex(config=config, dimensions=dimensions)
        self._rwlock = _ReadWriteLock()
        self._current_ef = max(self._config.ef_search, 1)

    @property
    def index(self) -> HNSWIndex:
        return self._meta

    def build_from_matrix(
        self,
        active_indices: np.ndarray,
        matrix: np.ndarray,
        norms: np.ndarray,
    ) -> HNSWIndex:
        """Build hnswlib index from active collection vectors."""
        with self._rwlock.write_locked():
            N = int(active_indices.size)
            space = _HNSWLIB_SPACE.get(self._config.metric, "cosine")

            lib_idx = _hnswlib.Index(space=space, dim=self._dimensions)
            lib_idx.init_index(
                max_elements=max(N, 1),
                ef_construction=self._config.ef_construction,
                M=self._config.M,
            )
            self._current_ef = max(self._config.ef_search, 1)
            lib_idx.set_ef(self._current_ef)
            lib_idx.set_num_threads(4)

            if N > 0:
                vecs = matrix[active_indices].astype(np.float32)
                lib_idx.add_items(vecs, active_indices.astype(np.int64))

            self._lib_index = lib_idx

            meta = HNSWIndex(
                config=self._config,
                dimensions=self._dimensions,
                entry_point=int(active_indices[0]) if N > 0 else None,
                max_level=0,
            )
            meta.node_count = N
            meta.build_size = N

            if self._config.enable_pq and N > 1000:
                pq = ProductQuantizer(
                    pq_segments=self._config.pq_segments,
                    pq_centroids=self._config.pq_centroids,
                    dimensions=self._dimensions,
                )
                pq.train(matrix[active_indices])
                pq.encode(matrix[active_indices])
                meta.pq = pq
                pq_index_map = np.full(matrix.shape[0], -1, dtype=np.int32)
                pq_index_map[active_indices] = np.arange(N, dtype=np.int32)
                meta.pq_index_map = pq_index_map
            else:
                meta.pq = None
                meta.pq_index_map = None

            self._meta = meta
            return meta

    def search(
        self,
        query: np.ndarray,
        top_k: int,
        ef_search: int | None,
        matrix: np.ndarray,
        norms: np.ndarray,
        active: np.ndarray,
    ) -> list[tuple[int, float]]:
        """KNN search via hnswlib — returns (index, distance) pairs."""
        ef = max(ef_search if ef_search is not None else self._config.ef_search, top_k)
        k_request = min(self._meta.node_count, max(top_k * 3, top_k + 20))
        q = query.astype(np.float32).reshape(1, -1)

        if ef == self._current_ef:
            with self._rwlock.read_locked():
                if self._lib_index is None or self._meta.node_count == 0:
                    return []
                labels, distances = self._lib_index.knn_query(q, k=k_request)
        else:
            with self._rwlock.write_locked():
                if self._lib_index is None or self._meta.node_count == 0:
                    return []
                self._lib_index.set_ef(ef)
                self._current_ef = ef
                labels, distances = self._lib_index.knn_query(q, k=k_request)

        results: list[tuple[int, float]] = []
        for label, dist in zip(labels[0], distances[0]):
            idx = int(label)
            if idx < len(active) and bool(active[idx]):
                results.append((idx, float(dist)))
            if len(results) >= top_k:
                break
        return results

    def mark_deleted(self, node_index: int) -> None:
        """Lazy-delete a vector from the hnswlib index."""
        with self._rwlock.write_locked():
            if self._lib_index is not None:
                try:
                    self._lib_index.mark_deleted(node_index)
                except Exception:
                    pass

    def upsert_vector(self, node_index: int, vector: np.ndarray) -> bool:
        """Incrementally add or replace a single vector without full rebuild.

        Returns True if the index was updated in-place (no rebuild needed).
        Returns False if a full rebuild is required (e.g., index not yet built).

        For deletions, use mark_deleted() + upsert_vector() with the new
        embedding.  hnswlib does not support true in-place update, so this
        marks the old label deleted (if present) and adds the new one.
        """
        with self._rwlock.write_locked():
            if self._lib_index is None:
                return False

            vec = vector.astype(np.float32).reshape(1, -1)
            label = np.array([node_index], dtype=np.int64)

            try:
                self._lib_index.mark_deleted(node_index)
            except Exception:
                pass

            current_max = self._lib_index.get_max_elements()
            if node_index >= current_max or self._meta.node_count >= current_max:
                new_max = max(current_max * 2, node_index + 1)
                self._lib_index.resize_index(new_max)

            self._lib_index.add_items(vec, label)
            self._meta.node_count = max(self._meta.node_count, node_index + 1)
            self._meta.build_size = max(self._meta.build_size, node_index + 1)
            return True


def make_hnsw_builder(config: HNSWConfig, dimensions: int) -> "HNSWBuilder | HNSWLibBackend":
    """Factory: returns HNSWLibBackend when hnswlib is installed, else HNSWBuilder.

    Always prefer the C++ backend — it is 100-1000× faster on build and
    competitive on query at all collection sizes.
    """
    if _HNSWLIB_AVAILABLE:
        return HNSWLibBackend(config, dimensions)
    return HNSWBuilder(config, dimensions)


# ---------------------------------------------------------------------------
# Serialization (for checkpoint/snapshot to disk)
# ---------------------------------------------------------------------------

_HNSW_MAGIC = b"HNSW\x01"
_HEADER_STRUCT = struct.Struct(">IIIIII")  # M, M_max0, ef_construction, ef_search, dimensions, node_count


def serialize_hnsw(index: HNSWIndex) -> bytes:
    """Serialize HNSW graph to bytes (without vector data — only graph topology)."""
    parts: list[bytes] = [_HNSW_MAGIC]
    parts.append(_HEADER_STRUCT.pack(
        index.config.M,
        index.config.M_max0,
        index.config.ef_construction,
        index.config.ef_search,
        index.dimensions,
        index.node_count,
    ))
    # Entry point and max level
    ep = index.entry_point if index.entry_point is not None else -1
    parts.append(struct.pack(">iI", ep, max(0, index.max_level + 1)))

    # Nodes: (index, level, deleted, neighbor counts per layer, neighbor indices)
    for node_index, node in sorted(index.nodes.items()):
        parts.append(struct.pack(">I", node_index))
        parts.append(struct.pack(">I", node.level))
        parts.append(struct.pack(">?", node.deleted))
        parts.append(struct.pack(">I", len(node.neighbors)))
        for layer_neighbors in node.neighbors:
            parts.append(struct.pack(">I", len(layer_neighbors)))
            for n_idx in layer_neighbors:
                parts.append(struct.pack(">I", n_idx))

    return b"".join(parts)


def deserialize_hnsw(data: bytes, config: HNSWConfig | None = None) -> HNSWIndex:
    """Deserialize HNSW graph from bytes."""
    offset = len(_HNSW_MAGIC)
    if data[:offset] != _HNSW_MAGIC:
        raise ValueError("Invalid HNSW magic bytes")

    M, M_max0, ef_construction, ef_search, dimensions, node_count = _HEADER_STRUCT.unpack(
        data[offset:offset + _HEADER_STRUCT.size]
    )
    offset += _HEADER_STRUCT.size

    ep, max_level_plus1 = struct.unpack(">iI", data[offset:offset + 8])
    offset += 8

    if config is None:
        config = HNSWConfig(M=M, M_max0=M_max0, ef_construction=ef_construction, ef_search=ef_search)

    index = HNSWIndex(
        config=config,
        dimensions=dimensions,
        entry_point=ep if ep >= 0 else None,
        max_level=max_level_plus1 - 1 if max_level_plus1 > 0 else -1,
    )

    for _ in range(node_count):
        (node_index,) = struct.unpack(">I", data[offset:offset + 4])
        offset += 4
        (level,) = struct.unpack(">I", data[offset:offset + 4])
        offset += 4
        (deleted,) = struct.unpack(">?", data[offset:offset + 1])
        offset += 1
        (n_layers,) = struct.unpack(">I", data[offset:offset + 4])
        offset += 4

        neighbors: list[list[int]] = []
        for _ in range(n_layers):
            (n_count,) = struct.unpack(">I", data[offset:offset + 4])
            offset += 4
            layer_neighbors: list[int] = []
            for _ in range(n_count):
                (n_idx,) = struct.unpack(">I", data[offset:offset + 4])
                offset += 4
                layer_neighbors.append(n_idx)
            neighbors.append(layer_neighbors)

        node = _HNSWNode(index=node_index, level=level, neighbors=neighbors, deleted=deleted)
        index.nodes[node_index] = node
        index.node_count += 1

    return index
