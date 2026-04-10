"""Separate vector-capable v2 runtime for Branch 33.

This package intentionally leaves the existing compressed state engine and
universal server unchanged. It adds exact-search and HNSW approximate
nearest-neighbor vector collection runtime for local AI memory and
semantic retrieval at billion-scale.
"""

from .engine import VectorCollectionStats, VectorStateEngineV2, VectorStateStats
from .hnsw import HNSWBuilder, HNSWConfig, HNSWIndex, ProductQuantizer
from .server import VectorStateServerV2

__all__ = [
    "HNSWBuilder",
    "HNSWConfig",
    "HNSWIndex",
    "ProductQuantizer",
    "VectorCollectionStats",
    "VectorStateEngineV2",
    "VectorStateServerV2",
    "VectorStateStats",
]