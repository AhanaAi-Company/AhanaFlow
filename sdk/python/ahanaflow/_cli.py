from __future__ import annotations

import argparse
import json
from typing import Any

from ahanaflow.client import AhanaFlowClient
from ahanaflow.exceptions import AhanaFlowError


def _parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _print_result(value: Any) -> None:
    if isinstance(value, str):
        print(value)
        return
    print(json.dumps(value, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ahanaflow",
        description="AhanaFlow Python SDK CLI for interacting with a running AhanaFlow server",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=9633, help="Server port")
    parser.add_argument("--timeout", type=float, default=5.0, help="Socket timeout in seconds")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="Health check the server")
    sub.add_parser("stats", help="Print server stats as JSON")

    get_cmd = sub.add_parser("get", help="Get a key")
    get_cmd.add_argument("key")

    set_cmd = sub.add_parser("set", help="Set a key")
    set_cmd.add_argument("key")
    set_cmd.add_argument("value", help="JSON value or raw string")
    set_cmd.add_argument("--ttl", type=int, default=None, help="Optional TTL in seconds")

    del_cmd = sub.add_parser("delete", help="Delete a key")
    del_cmd.add_argument("key")

    incr_cmd = sub.add_parser("incr", help="Increment an integer key")
    incr_cmd.add_argument("key")
    incr_cmd.add_argument("--amount", type=int, default=1)

    keys_cmd = sub.add_parser("keys", help="List keys")
    keys_cmd.add_argument("--prefix", default="")

    ttl_cmd = sub.add_parser("ttl", help="Read TTL for a key")
    ttl_cmd.add_argument("key")

    mget_cmd = sub.add_parser("mget", help="Get multiple keys")
    mget_cmd.add_argument("keys", nargs="+")

    enqueue_cmd = sub.add_parser("enqueue", help="Push to a queue")
    enqueue_cmd.add_argument("queue")
    enqueue_cmd.add_argument("payload", help="JSON payload or raw string")

    dequeue_cmd = sub.add_parser("dequeue", help="Pop from a queue")
    dequeue_cmd.add_argument("queue")

    qlen_cmd = sub.add_parser("qlen", help="Queue depth")
    qlen_cmd.add_argument("queue")

    xadd_cmd = sub.add_parser("xadd", help="Append an event to a stream")
    xadd_cmd.add_argument("stream")
    xadd_cmd.add_argument("event", help="JSON event or raw string")

    xrange_cmd = sub.add_parser("xrange", help="Read events from a stream")
    xrange_cmd.add_argument("stream")
    xrange_cmd.add_argument("--after", type=int, default=0)
    xrange_cmd.add_argument("--limit", type=int, default=100)

    config_get_cmd = sub.add_parser("config-get", help="Read a runtime config value")
    config_get_cmd.add_argument("key")

    config_set_cmd = sub.add_parser("config-set", help="Write a runtime config value")
    config_set_cmd.add_argument("key")
    config_set_cmd.add_argument("value", help="JSON value or raw string")

    mode_cmd = sub.add_parser("mode", help="Set the durability mode")
    mode_cmd.add_argument("mode", choices=("safe", "fast", "strict"))

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with AhanaFlowClient(args.host, args.port, timeout=args.timeout) as client:
            if args.command == "ping":
                _print_result(client.ping())
            elif args.command == "stats":
                _print_result(client.stats())
            elif args.command == "get":
                _print_result(client.get(args.key))
            elif args.command == "set":
                _print_result(client.set(args.key, _parse_value(args.value), ttl_seconds=args.ttl))
            elif args.command == "delete":
                _print_result(client.delete(args.key))
            elif args.command == "incr":
                _print_result(client.incr(args.key, args.amount))
            elif args.command == "keys":
                _print_result(client.keys(prefix=args.prefix))
            elif args.command == "ttl":
                _print_result(client.ttl(args.key))
            elif args.command == "mget":
                _print_result(client.mget(*args.keys))
            elif args.command == "enqueue":
                _print_result(client.enqueue(args.queue, _parse_value(args.payload)))
            elif args.command == "dequeue":
                _print_result(client.dequeue(args.queue))
            elif args.command == "qlen":
                _print_result(client.qlen(args.queue))
            elif args.command == "xadd":
                _print_result(client.xadd(args.stream, _parse_value(args.event)))
            elif args.command == "xrange":
                _print_result(client.xrange(args.stream, after=args.after, limit=args.limit))
            elif args.command == "config-get":
                _print_result(client.config_get(args.key))
            elif args.command == "config-set":
                _print_result(client.config_set(args.key, _parse_value(args.value)))
            elif args.command == "mode":
                _print_result(client.set_durability_mode(args.mode))
            else:
                parser.error(f"Unknown command: {args.command}")
    except AhanaFlowError as exc:
        print(f"Error: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())