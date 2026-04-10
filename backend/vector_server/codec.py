# Copyright 2026 AhanaAI. All rights reserved.
#
# AhanaFlow Vector Engine Codec — Open-Core distribution layer.
#
# Tier priority (all compression runs LOCALLY — zero network latency):
#
#   1. Pro binary  (ahana_codec installed):
#      Trained 65 KB dictionary — ~88.7% footprint reduction on event payloads.
#      Install: pip install ahanaflow-pro  (licensed binary, no source)
#
#   2. Community  (ahana_codec absent, zstandard present):
#      Plain zstd level-1 — ~50-60% footprint reduction.
#      Install: pip install ahanaflow  (open-source, fully functional)
#
#   3. Last resort  (neither present):
#      gzip level-6 fallback — always available.

from __future__ import annotations

import gzip

_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

# --- Tier 1: Pro binary (trained dictionary) ---
try:
    from ahana_codec import compress as _pro_compress
    from ahana_codec import decompress as _pro_decompress
    from ahana_codec import TIER as _AHANA_TIER
    _PRO_AVAILABLE = (_AHANA_TIER == "pro")
except ImportError:
    _PRO_AVAILABLE = False

# --- Tier 2: Community (plain zstd, no dictionary) ---
if not _PRO_AVAILABLE:
    try:
        import zstandard as _zstd
        _COMMUNITY_ENC = _zstd.ZstdCompressor(level=1)
        _COMMUNITY_DEC = _zstd.ZstdDecompressor()
        _COMMUNITY_AVAILABLE = True
    except ModuleNotFoundError:  # pragma: no cover
        _COMMUNITY_AVAILABLE = False
else:
    _COMMUNITY_AVAILABLE = False


# --- Public API ---

def compress(raw: bytes) -> bytes:
    """Compress vector payload bytes.

    Uses the Pro trained-dictionary codec when available (88.7% reduction),
    falls back to community zstd (50-60%), then gzip.  All paths are local.
    """
    if _PRO_AVAILABLE:
        return _pro_compress(raw)
    if _COMMUNITY_AVAILABLE:
        return _COMMUNITY_ENC.compress(raw)
    return gzip.compress(raw, compresslevel=6)


def decompress(data: bytes) -> bytes:
    """Decompress vector payload bytes.  Auto-detects format (zstd vs gzip)."""
    if _PRO_AVAILABLE:
        return _pro_decompress(data)
    if _COMMUNITY_AVAILABLE and data[:4] == _ZSTD_MAGIC:
        return _COMMUNITY_DEC.decompress(data)
    return gzip.decompress(data)


def active_tier() -> str:
    """Return which compression tier is active: 'pro', 'community', or 'gzip'."""
    if _PRO_AVAILABLE:
        return "pro"
    if _COMMUNITY_AVAILABLE:
        return "community"
    return "gzip"

