"""
AhanaFlow Redis Compatibility Layer
====================================
RESP-protocol adapter that lets any existing Redis client speak directly to
UniversalStateServer with zero code changes.

Usage:
    python -m redis_compat.cli serve --port 6379

Environment variables:
    REDIS_COMPAT_HOST        Bind address (default: 0.0.0.0)
    REDIS_COMPAT_PORT        Bind port    (default: 6379)
    UNIVERSAL_STATE_HOST     USS host     (default: 127.0.0.1)
    UNIVERSAL_STATE_PORT     USS port     (default: 9633)
"""

from .server import run_server

__all__ = ["run_server"]
__version__ = "1.0.0"
