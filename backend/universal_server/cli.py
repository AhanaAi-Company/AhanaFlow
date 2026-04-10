from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path

from .benchmark import run_benchmark
from .server import UniversalStateServer
from .security import SecurityConfig
from backend.vector_server import VectorStateServerV2


def _build_security_config(api_keys_file: str | None) -> SecurityConfig | None:
    resolved = (api_keys_file or os.environ.get("AHANAFLOW_API_KEYS_FILE") or os.environ.get("AHANAFLOW_API_KEY_REGISTRY_PATH") or "").strip()
    if not resolved:
        return None
    return SecurityConfig(api_keys_file=resolved, require_auth=True)


def main() -> int:
    parser = argparse.ArgumentParser(prog="ahana-universal", description="Ahana universal compressed state server")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Start the universal state server")
    serve.add_argument("--wal", default="./universal_server.wal", help="WAL path")
    serve.add_argument("--host", default="127.0.0.1", help="Bind host")
    serve.add_argument("--port", type=int, default=9633, help="Bind port")
    serve.add_argument(
        "--durability-mode",
        choices=["safe", "fast", "strict"],
        default="safe",
        help="Durability mode for the universal WAL",
    )
    serve.add_argument("--api-keys-file", default="", help="Structured registry or flat hash file for API key auth")

    serve_vector = sub.add_parser("serve-vector-v2", help="Start the separate vector-capable v2 server")
    serve_vector.add_argument("--wal", default="./vector_server_v2.wal", help="Vector WAL path")
    serve_vector.add_argument("--host", default="127.0.0.1", help="Bind host")
    serve_vector.add_argument("--port", type=int, default=9644, help="Bind port")
    serve_vector.add_argument("--api-keys-file", default="", help="Structured registry or flat hash file for API key auth")

    bench = sub.add_parser("benchmark", help="Run benchmark against local SQLite")
    bench.add_argument("--iterations", type=int, default=20_000, help="Benchmark iterations")

    args = parser.parse_args()

    if args.command == "serve":
        signal.signal(signal.SIGTERM, signal.default_int_handler)
        server = UniversalStateServer(
            Path(args.wal),
            host=args.host,
            port=args.port,
            durability_mode=args.durability_mode,
            security_config=_build_security_config(args.api_keys_file),
        )
        host, port = server.address
        print(f"Universal server listening on {host}:{port} WAL={args.wal}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.shutdown()
        print("Server stopped")
        return 0

    if args.command == "serve-vector-v2":
        signal.signal(signal.SIGTERM, signal.default_int_handler)
        server = VectorStateServerV2(Path(args.wal), host=args.host, port=args.port)
        server = VectorStateServerV2(
            Path(args.wal),
            host=args.host,
            port=args.port,
            security_config=_build_security_config(args.api_keys_file),
        )
        host, port = server.address
        print(f"Vector v2 server listening on {host}:{port} WAL={args.wal}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.shutdown()
        print("Vector server stopped")
        return 0

    if args.command == "benchmark":
        result = run_benchmark(iterations=args.iterations)
        print(json.dumps(result, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
