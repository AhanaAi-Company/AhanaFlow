#!/usr/bin/env python3
"""
CLI for Branch 33 Vector Server V2.

Usage:
    python -m business_ecosystem.33_event_streams.vector_server.cli serve --wal ./tmp.wal --port 9634
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

def _find_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "tools").exists():
            return parent
    return current.parents[2]


REPO_ROOT = _find_repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from tools.acp_logging import get_logger
except ModuleNotFoundError:  # pragma: no cover
    class _CompatLogger:
        def __init__(self, logger: logging.Logger) -> None:
            self._logger = logger

        def info(self, message: str, **fields: Any) -> None:
            if fields:
                self._logger.info("%s | %s", message, fields)
                return
            self._logger.info(message)

    def get_logger(name: str) -> _CompatLogger:
        logger = logging.getLogger(name)
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO)
        return _CompatLogger(logger)

from .server import VectorStateServerV2

log = get_logger("vector_server_cli")


def cmd_serve(args: argparse.Namespace) -> None:
    """Start the vector server."""
    log.info("starting_vector_server",
             wal=str(args.wal),
             host=args.host,
             port=args.port)
    
    server = VectorStateServerV2(
        wal_path=args.wal,
        host=args.host,
        port=args.port,
    )
    
    host, port = server.address
    print(f"Vector server listening on {host}:{port}")
    print(f"WAL: {args.wal}")
    print("Press Ctrl+C to stop")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
        log.info("server_stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Branch 33 Vector Server")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Serve command
    serve_parser = subparsers.add_parser("serve", help="Start the vector server")
    serve_parser.add_argument(
        "--wal",
        type=Path,
        default="vector_server.wal",
        help="Path to WAL file",
    )
    serve_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=9634,
        help="Port to listen on",
    )
    serve_parser.set_defaults(func=cmd_serve)
    
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
