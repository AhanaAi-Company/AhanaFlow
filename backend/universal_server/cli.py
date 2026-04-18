from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path

from backend.common import read_secret

from .async_server import AsyncUniversalStateServer
from .benchmark import run_benchmark
from .server import UniversalStateServer
from .security import SecurityConfig
from backend.vector_server import VectorStateServerV2


def _build_security_config(api_keys_file: str | None) -> SecurityConfig | None:
    resolved = (
        api_keys_file
        or os.environ.get("AHANAFLOW_API_KEYS_FILE")
        or os.environ.get("AHANAFLOW_API_KEY_REGISTRY_PATH")
        or ""
    ).strip()
    sealed_policy_file = os.environ.get("AHANAFLOW_SEALED_POLICY_FILE", "").strip()
    sealed_policy_key = read_secret("AHANAFLOW_SEALED_POLICY_KEY")
    if not resolved and not sealed_policy_file:
        return None
    return SecurityConfig(
        api_keys_file=resolved or None,
        sealed_policy_file=sealed_policy_file or None,
        sealed_policy_key=sealed_policy_key or None,
        require_auth=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="ahana-universal", description="Ahana universal compressed state server")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Start the universal state server")
    serve.add_argument("--wal", default="./universal_server.wal", help="WAL path")
    serve.add_argument("--host", default="127.0.0.1", help="Bind host")
    serve.add_argument("--port", type=int, default=9633, help="Bind port")
    serve.add_argument("--runtime", choices=["threaded", "async"], default="threaded", help="Server runtime")
    serve.add_argument("--wire-protocol", choices=["json", "resp"], default="json", help="Server wire protocol")
    serve.add_argument(
        "--durability-mode",
        choices=["safe", "fast", "strict"],
        default="safe",
        help="Durability mode for the universal WAL",
    )
    serve.add_argument("--fast-batch-size", type=int, default=None, help="Experimental fast-mode batch size override")
    serve.add_argument(
        "--fast-flush-interval-ms",
        type=float,
        default=None,
        help="Experimental fast-mode flush interval override in milliseconds",
    )
    serve.add_argument(
        "--no-compress-threshold",
        type=int,
        default=None,
        help="Experimental raw-frame threshold override before WAL compression",
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
        server_class = AsyncUniversalStateServer if args.runtime == "async" else UniversalStateServer
        server_kwargs = {
            "host": args.host,
            "port": args.port,
            "durability_mode": args.durability_mode,
            "fast_batch_size": args.fast_batch_size,
            "fast_flush_interval_ms": args.fast_flush_interval_ms,
            "no_compress_threshold": args.no_compress_threshold,
            "security_config": _build_security_config(args.api_keys_file),
        }
        if args.runtime == "async":
            server_kwargs["wire_protocol"] = args.wire_protocol
        server = server_class(Path(args.wal), **server_kwargs)
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
