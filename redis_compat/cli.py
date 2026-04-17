"""
AhanaFlow Redis Compatibility Layer — CLI entry point.

    python -m redis_compat.cli serve [options]

Or via direct module invocation:

    python -m redis_compat

Environment variables (all overridable by CLI flags):
    REDIS_COMPAT_HOST        Bind address  (default: 0.0.0.0)
    REDIS_COMPAT_PORT        Bind port     (default: 6379)
    UNIVERSAL_STATE_HOST     USS hostname  (default: 127.0.0.1)
    UNIVERSAL_STATE_PORT     USS TCP port  (default: 9633)
    REDIS_COMPAT_LOG_LEVEL   Log verbosity (default: INFO)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from .server import run_server


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="redis_compat",
        description=(
            "AhanaFlow Redis Compatibility Layer\n"
            "Translates RESP wire protocol → UniversalStateServer JSON protocol.\n"
            "Drop-in replacement for Redis — connect any Redis client unchanged."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command")

    serve = sub.add_parser("serve", help="Start the RESP server")
    serve.add_argument(
        "--host",
        default=os.environ.get("REDIS_COMPAT_HOST", "0.0.0.0"),
        help="Bind address (default: 0.0.0.0)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("REDIS_COMPAT_PORT", "6379")),
        help="Bind port (default: 6379)",
    )
    serve.add_argument(
        "--uss-host",
        default=os.environ.get("UNIVERSAL_STATE_HOST", "127.0.0.1"),
        help="UniversalStateServer host (default: 127.0.0.1)",
    )
    serve.add_argument(
        "--uss-port",
        type=int,
        default=int(os.environ.get("UNIVERSAL_STATE_PORT", "9633")),
        help="UniversalStateServer port (default: 9633)",
    )
    serve.add_argument(
        "--log-level",
        default=os.environ.get("REDIS_COMPAT_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        # Default to serve if invoked as `python -m redis_compat`
        args.command = "serve"
        args.host = os.environ.get("REDIS_COMPAT_HOST", "0.0.0.0")
        args.port = int(os.environ.get("REDIS_COMPAT_PORT", "6379"))
        args.uss_host = os.environ.get("UNIVERSAL_STATE_HOST", "127.0.0.1")
        args.uss_port = int(os.environ.get("UNIVERSAL_STATE_PORT", "9633"))
        args.log_level = os.environ.get("REDIS_COMPAT_LOG_LEVEL", "INFO")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    log = logging.getLogger("redis_compat")
    log.info(
        "Starting AhanaFlow Redis Compat Layer on %s:%d → USS %s:%d",
        args.host, args.port, args.uss_host, args.uss_port,
    )
    log.info(
        "Connect with: redis-cli -h %s -p %d",
        "127.0.0.1" if args.host in ("0.0.0.0", "::") else args.host,
        args.port,
    )

    try:
        asyncio.run(run_server(args.host, args.port, args.uss_host, args.uss_port))
    except KeyboardInterrupt:
        log.info("Shutdown signal received — stopping")


if __name__ == "__main__":
    main()
