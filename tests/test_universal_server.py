from __future__ import annotations

import threading
import time
from pathlib import Path

from backend.universal_server.async_server import AsyncUniversalStateServer
from backend.universal_server.benchmark import run_benchmark
from backend.universal_server.protocol import decode_command, encode_response
from backend.universal_server.server import UniversalStateServer
from benchmark_vs_competitors import UniversalStateClient


def test_protocol_round_trip() -> None:
    payload = decode_command(b'{"cmd":"ping"}')
    assert payload["cmd"] == "PING"

    compact_payload = decode_command(b'["get","hot:1"]')
    assert compact_payload["cmd"] == "GET"
    assert compact_payload["key"] == "hot:1"

    encoded = encode_response({"ok": True, "result": "PONG"})
    assert encoded.endswith(b"\n")
    assert b"PONG" in encoded

    compact_encoded = encode_response({"ok": True, "result": ["OK", 3]}, compact=True)
    assert compact_encoded == b'[1,["OK",3]]\n'


def test_server_dispatch_basic_operations(tmp_path: Path) -> None:
    server = UniversalStateServer(tmp_path / "server.wal", port=0)

    assert server.dispatch({"cmd": "PING"})["result"] == "PONG"
    assert server.dispatch({"cmd": "SET", "key": "a", "value": 1})["ok"] is True
    assert server.dispatch({"cmd": "GET", "key": "a"})["result"] == 1
    assert server.dispatch({"cmd": "INCR", "key": "ctr", "amount": 2})["result"] == 2
    assert server.dispatch({"cmd": "DEL", "key": "a"})["result"] == 1
    assert server.dispatch({"cmd": "GET", "key": "a"})["result"] is None

    server.shutdown()


def test_incr_accepts_integer_like_string_values(tmp_path: Path) -> None:
    server = UniversalStateServer(tmp_path / "server_string_counter.wal", port=0)

    assert server.dispatch({"cmd": "SET", "key": "ctr", "value": "0"})["ok"] is True
    assert server.dispatch({"cmd": "INCR", "key": "ctr", "amount": 2})["result"] == 2
    assert server.dispatch({"cmd": "GET", "key": "ctr"})["result"] == 2

    server.shutdown()


def test_server_accepts_experimental_fast_mode_overrides(tmp_path: Path) -> None:
    server = UniversalStateServer(
        tmp_path / "server_overrides.wal",
        port=0,
        durability_mode="fast",
        fast_batch_size=1024,
        fast_flush_interval_ms=25,
        no_compress_threshold=256,
    )

    assert server._engine._batch_size == 1024
    assert server._engine._flush_interval == 0.025
    assert server._engine._NO_COMPRESS_THRESHOLD == 256

    server.shutdown()


def test_server_dispatch_queue_and_stream(tmp_path: Path) -> None:
    server = UniversalStateServer(tmp_path / "server.wal", port=0)

    server.dispatch({"cmd": "ENQUEUE", "queue": "jobs", "payload": {"id": "j1"}})
    server.dispatch({"cmd": "ENQUEUE", "queue": "jobs", "payload": {"id": "j2"}})
    assert server.dispatch({"cmd": "QLEN", "queue": "jobs"})["result"] == 2
    assert server.dispatch({"cmd": "DEQUEUE", "queue": "jobs"})["result"] == {"id": "j1"}

    seq = server.dispatch({"cmd": "XADD", "stream": "access", "event": {"path": "/health"}})["result"]
    assert seq == 1
    events = server.dispatch({"cmd": "XRANGE", "stream": "access", "after_seq": 0, "limit": 10})["result"]
    assert events == [{"seq": 1, "event": {"path": "/health"}}]

    stats = server.dispatch({"cmd": "STATS"})["result"]
    assert "compression_ratio" in stats

    server.shutdown()


def test_server_dispatch_mset_and_pipeline(tmp_path: Path) -> None:
    server = UniversalStateServer(tmp_path / "server_batch.wal", port=0)

    assert server.dispatch({"cmd": "MSET", "values": {"a": 1, "b": 2}})["result"] == 2
    assert server.dispatch({"cmd": "MGET", "keys": ["a", "b", "c"]})["result"] == [1, 2, None]

    pipeline = server.dispatch(
        {
            "cmd": "PIPELINE",
            "commands": [
                {"cmd": "SET", "key": "ctr", "value": 3},
                {"cmd": "INCR", "key": "ctr", "amount": 2},
                {"cmd": "GET", "key": "ctr"},
            ],
        }
    )["result"]

    assert [entry["result"] for entry in pipeline] == ["OK", 5, 5]

    mincr = server.dispatch(
        {
            "cmd": "MINCR",
            "updates": [
                {"key": "ctr:a", "amount": 2},
                {"key": "ctr:b", "amount": 3},
                {"key": "ctr:a", "amount": 1},
            ],
        }
    )["result"]

    assert mincr == {"ctr:a": 3, "ctr:b": 3}

    server.shutdown()


def test_async_server_resp_round_trip(tmp_path: Path) -> None:
    server = AsyncUniversalStateServer(tmp_path / "server_async_resp.wal", port=0, durability_mode="fast", wire_protocol="resp")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while server.address[1] == 0 and time.time() < deadline:
        time.sleep(0.01)

    host, port = server.address
    assert port != 0

    client = UniversalStateClient(host, port, codec="resp")
    client.connect()
    try:
        assert client.send({"cmd": "SET", "key": "a", "value": "1"})["result"] == "OK"
        assert client.send({"cmd": "GET", "key": "a"})["result"] == "1"
        assert client.send({"cmd": "INCR", "key": "ctr", "amount": 2})["result"] == 2
        assert client.send({"cmd": "MGET", "keys": ["a", "missing"]})["result"] == ["1", None]
        pipeline = client.send_pipeline(
            [
                {"cmd": "SET", "key": "pipe", "value": "v"},
                {"cmd": "GET", "key": "pipe"},
            ]
        )["result"]
        assert pipeline == ["OK", "v"]
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)


def test_async_server_hybrid_redis_compat_client(tmp_path: Path) -> None:
    """Test the hybrid wire protocol with the RedisCompatClient."""
    from backend.universal_server.redis_client import RedisCompatClient

    server = AsyncUniversalStateServer(
        tmp_path / "server_hybrid.wal", port=0, durability_mode="fast",
        wire_protocol="hybrid",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while server.address[1] == 0 and time.time() < deadline:
        time.sleep(0.01)

    host, port = server.address
    assert port != 0

    client = RedisCompatClient(host, port)
    client.connect()
    try:
        # Basic operations
        assert client.ping() == "PONG"
        assert client.set("key1", "value1") == "OK"
        assert client.get("key1") == "value1"
        assert client.get("missing") is None
        assert client.incr("counter") == 1
        assert client.incr("counter") == 2
        assert client.incr("counter", amount=5) == 7

        # Pipeline
        with client.pipeline() as pipe:
            pipe.set("pk1", "pv1")
            pipe.set("pk2", "pv2")
            pipe.get("pk1")
            pipe.incr("pctr")
            results = pipe.execute()
        assert results[0] == "OK"
        assert results[1] == "OK"
        assert results[2] == "pv1"
        assert results[3] == 1

        # MSET/MGET
        assert client.mset({"m1": "a", "m2": "b"}) == "OK"
        assert client.mget("m1", "m2") == ["a", "b"]

        # DEL
        assert client.delete("m1") == 1
        assert client.get("m1") is None

        # FLUSHALL
        assert client.flushall() == "OK"
        assert client.get("key1") is None
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)


def test_async_server_hybrid_auto_detects_resp(tmp_path: Path) -> None:
    """Test that hybrid mode also handles RESP clients correctly."""
    server = AsyncUniversalStateServer(
        tmp_path / "server_hybrid_resp.wal", port=0, durability_mode="fast",
        wire_protocol="hybrid",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while server.address[1] == 0 and time.time() < deadline:
        time.sleep(0.01)

    host, port = server.address
    assert port != 0

    client = UniversalStateClient(host, port, codec="resp")
    client.connect()
    try:
        assert client.send({"cmd": "SET", "key": "rkey", "value": "rval"})["result"] == "OK"
        assert client.send({"cmd": "GET", "key": "rkey"})["result"] == "rval"
        assert client.send({"cmd": "INCR", "key": "rctr", "amount": 3})["result"] == 3
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)


def test_benchmark_smoke() -> None:
    result = run_benchmark(iterations=300)
    assert result["iterations"] == 300
    assert result["modes"]["safe"]["ops_per_sec"] > 0
    assert result["modes"]["fast"]["ops_per_sec"] > 0
    assert result["modes"]["strict"]["ops_per_sec"] > 0
    assert result["sqlite"]["ops_per_sec"] > 0
    assert result["comparisons"]["wal_reduction_fast_vs_safe"] > 0


def test_competitive_benchmark_includes_batched_paths() -> None:
    import benchmark_vs_competitors as bench

    class _UniversalClient:
        def connect(self):
            return None

        def close(self):
            return None

        def send(self, _cmd):
            return {"ok": True, "result": "OK"}

        def send_pipeline(self, commands):
            return {"ok": True, "result": [{"ok": True}] * len(commands)}

    class _RedisClient:
        def connect(self):
            return None

        def close(self):
            return None

        def send_raw(self, *_args):
            return "OK"

        def send_pipeline_raw(self, commands):
            return [1] * len(commands)

    mset = bench.benchmark_mset_mget_operations(lambda: _UniversalClient(), "UniversalStateServer", num_batches=10, batch_size=4)
    pipeline = bench.benchmark_pipeline_kv_operations(lambda: _UniversalClient(), "UniversalStateServer", num_batches=10, batch_size=4)
    batched_counter = bench.benchmark_batched_counter_operations(lambda: _RedisClient(), "Redis", num_batches=10, batch_size=4)

    assert mset.operation == "KV_MSET_MGET"
    assert pipeline.operation == "KV_PIPELINE_SET_GET"
    assert batched_counter.operation == "COUNTER_BATCH_INCR"
    assert mset.num_operations == 80
    assert pipeline.num_operations == 80
    assert batched_counter.num_operations == 40
