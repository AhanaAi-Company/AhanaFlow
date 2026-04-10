from __future__ import annotations

from pathlib import Path

from backend.universal_server.benchmark import run_benchmark
from backend.universal_server.protocol import decode_command, encode_response
from backend.universal_server.server import UniversalStateServer


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
