from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from backend.state_engine import CompressedStateEngine


def test_put_get_and_delete_round_trip(tmp_path: Path) -> None:
    engine = CompressedStateEngine(tmp_path / "engine.wal")
    engine.put("alpha", {"value": 123, "kind": "demo"})

    assert engine.get("alpha") == {"value": 123, "kind": "demo"}
    assert engine.delete("alpha") is True
    assert engine.get("alpha") is None


def test_incr_persists_across_restart(tmp_path: Path) -> None:
    wal = tmp_path / "engine.wal"
    engine = CompressedStateEngine(wal)
    assert engine.incr("counter") == 1
    assert engine.incr("counter", amount=4) == 5
    engine.flush()  # commit buffered WAL writes before replaying

    reloaded = CompressedStateEngine(wal)
    assert reloaded.get("counter") == 5
    assert reloaded.stats().records_replayed >= 2


def test_ttl_expiry_evicts_key_on_read(tmp_path: Path) -> None:
    engine = CompressedStateEngine(tmp_path / "engine.wal")
    engine.put("short_lived", "value", ttl_seconds=1)
    assert engine.get("short_lived") == "value"

    time.sleep(1.1)
    assert engine.get("short_lived") is None


def test_queue_enqueue_and_dequeue(tmp_path: Path) -> None:
    engine = CompressedStateEngine(tmp_path / "engine.wal")
    engine.enqueue("jobs", {"job_id": "a"})
    engine.enqueue("jobs", {"job_id": "b"})

    assert engine.queue_length("jobs") == 2
    assert engine.dequeue("jobs") == {"job_id": "a"}
    assert engine.queue_length("jobs") == 1
    assert engine.dequeue("jobs") == {"job_id": "b"}
    assert engine.dequeue("jobs") is None


def test_stream_append_and_range_read(tmp_path: Path) -> None:
    engine = CompressedStateEngine(tmp_path / "engine.wal")
    seq1 = engine.append_event("access", {"path": "/health", "status": 200})
    seq2 = engine.append_event("access", {"path": "/login", "status": 401})

    assert (seq1, seq2) == (1, 2)
    assert engine.read_events("access") == [
        {"seq": 1, "event": {"path": "/health", "status": 200}},
        {"seq": 2, "event": {"path": "/login", "status": 401}},
    ]
    assert engine.read_events("access", after_seq=1) == [
        {"seq": 2, "event": {"path": "/login", "status": 401}},
    ]


def test_stats_report_compression_and_structures(tmp_path: Path) -> None:
    engine = CompressedStateEngine(tmp_path / "engine.wal")
    for index in range(5):
        engine.put(f"key:{index}", {"tenant": "acme", "status": "ok", "n": index})
    engine.enqueue("jobs", {"job_id": "repeat-a", "tenant": "acme"})
    engine.append_event("access", {"path": "/health", "tenant": "acme", "status": 200})
    engine.flush()  # flush WAL buffer so wal_size_bytes is non-zero

    stats = engine.stats()
    assert stats.keys == 5
    assert stats.queues == 1
    assert stats.streams == 1
    assert stats.wal_size_bytes > 0
    assert stats.compressed_bytes_written > 0
    assert 0 < stats.compression_ratio < 1.5


def test_strict_mode_calls_fsync_per_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fsync_calls: list[int] = []

    def fake_fsync(fd: int) -> None:
        fsync_calls.append(fd)

    monkeypatch.setattr(os, "fsync", fake_fsync)

    engine = CompressedStateEngine(tmp_path / "strict.wal", durability_mode="strict")
    engine.put("alpha", {"value": 1})
    engine.incr("counter")

    assert len(fsync_calls) == 2


def test_switching_from_fast_to_strict_flushes_batch_and_syncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fsync_calls: list[int] = []

    def fake_fsync(fd: int) -> None:
        fsync_calls.append(fd)

    monkeypatch.setattr(os, "fsync", fake_fsync)

    wal = tmp_path / "mode-switch.wal"
    engine = CompressedStateEngine(wal, durability_mode="fast")
    engine.put("alpha", {"value": 1})
    engine.put("beta", {"value": 2})

    engine.set_durability_mode("strict")
    engine.put("gamma", {"value": 3})
    engine.close()

    reloaded = CompressedStateEngine(wal)
    assert reloaded.get("alpha") == {"value": 1}
    assert reloaded.get("beta") == {"value": 2}
    assert reloaded.get("gamma") == {"value": 3}
    assert len(fsync_calls) == 1
def test_tiny_records_can_skip_compression_and_replay(tmp_path: Path) -> None:
    wal = tmp_path / "tiny-raw.wal"
    engine = CompressedStateEngine(wal, durability_mode="safe")
    engine.put("a", 1)
    engine.put("b", True)
    engine.flush()
    engine.close()

    reloaded = CompressedStateEngine(wal)
    assert reloaded.get("a") == 1
    assert reloaded.get("b") is True
    assert reloaded.stats().records_replayed >= 2
