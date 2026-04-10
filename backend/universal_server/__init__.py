"""Universal compressed state server for Branch 33.

This package wraps the local compressed state engine with a small network
protocol so it can be used as a standalone service similar to Redis for
control-plane workloads.
"""

from .server import UniversalStateServer

__all__ = ["UniversalStateServer"]
