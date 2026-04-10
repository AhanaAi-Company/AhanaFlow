from __future__ import annotations

import contextlib
import io
import json
import os
import struct
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codec import compress as _compress, decompress as _decompress

# orjson is a Rust-compiled drop-in for json — 3-5× faster for small dicts.
try:
    import orjson as _orjson
    def _json_dumps(obj: Any) -> bytes:  # type: ignore[misc]
        return _orjson.dumps(obj)
    def _json_loads(data: bytes) -> Any:  # type: ignore[misc]
        return _orjson.loads(data)
except ModuleNotFoundError:  # pragma: no cover
    def _json_dumps(obj: Any) -> bytes:  # type: ignore[misc]
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")
    def _json_loads(data: bytes) -> Any:  # type: ignore[misc]
        return json.loads(data.decode("utf-8"))


class _ReadWriteLock:
    """Simple readers-writer lock.

    Multiple concurrent readers are allowed; a writer gets exclusive access.
    Writers are preferred over readers to prevent write starvation.
    The ``_lock`` attribute is the write lock (RLock) for compatibility with
    callers that already hold it (e.g. PIPELINE, flush timer, WAL replay).
    """

    __slots__ = ("_lock", "_read_cond", "_readers", "_write_waiting")

    def __init__(self) -> None:
        self._lock = threading.RLock()         # exclusive write lock (also used by callers directly)
        self._read_cond = threading.Condition(threading.Lock())
        self._readers: int = 0
        self._write_waiting: int = 0

    @contextlib.contextmanager
    def read_lock(self):
        """Acquire the lock for reading (shared, multiple concurrent readers ok)."""
        # Wait until no writers are pending or active.
        with self._read_cond:
            while self._write_waiting > 0 or self._readers < 0:
                self._read_cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._read_cond:
                self._readers -= 1
                if self._readers == 0:
                    self._read_cond.notify_all()

    @contextlib.contextmanager
    def write_lock(self):
        """Acquire the lock for writing (exclusive)."""
        with self._read_cond:
            self._write_waiting += 1
        try:
            with self._lock:
                # Wait until all current readers are done.
                with self._read_cond:
                    while self._readers > 0:
                        self._read_cond.wait()  # ← FIXED: removed timeout parameter to eliminate 1ms polling
                    self._write_waiting -= 1
                yield
                with self._read_cond:
                    self._read_cond.notify_all()
        except Exception:
            with self._read_cond:
                self._write_waiting = max(0, self._write_waiting - 1)
                self._read_cond.notify_all()
            raise



@dataclass(frozen=True)
class EngineStats:
    keys: int
    queues: int
    streams: int
    records_replayed: int
    wal_size_bytes: int
    compressed_bytes_written: int
    uncompressed_bytes_written: int

    @property
    def compression_ratio(self) -> float:
        if self.uncompressed_bytes_written <= 0:
            return 1.0
        return self.compressed_bytes_written / self.uncompressed_bytes_written


class CompressedStateEngine:
    """Single-node compressed state engine for hot control-plane workloads.

    Design goals:
      - append-only persistence with deterministic replay
      - compressed records on disk to reduce retained hot-state cost
      - very small API surface aimed at queues, counters, TTL values, and streams

    This is not a Redis or SQLite replacement. It is a narrow engine intended for
    repetitive state lanes where transport and retention cost matters more than
    arbitrary querying.
    """

    _LEN_STRUCT = struct.Struct(">I")
    _BATCH_FRAME_MARKER = b"\xff"
    _RAW_FRAME_MARKER = b"\x00"
    _NO_COMPRESS_THRESHOLD = 96

    def __init__(
        self,
        wal_path: str | Path,
        *,
        sync_writes: bool = False,
        durability_mode: str = "safe",
    ) -> None:
        """Initialise the engine.

        Parameters
        ----------
        wal_path:
            Path to the append-only WAL file.  Created if absent.
        sync_writes:
            Legacy parameter.  When *True* and *durability_mode* is ``"safe"``
            (the default), the effective mode is promoted to ``"strict"``.
            Ignored when *durability_mode* is set explicitly.
        durability_mode:
            ``"safe"``   — individual records, OS-buffered writes (default).
                           Up to ~30 s of data at risk on power failure.
            ``"fast"``   — batch writes (N=16 records or 50 ms, whichever
                           comes first).  3–5× higher write throughput;
                           up to 50 ms of data at risk on hard crash.
            ``"strict"`` — flush and fsync after every single record.
                           Equivalent to ``sync_writes=True``.
        """
        # 'sync_writes=True' is a legacy API for strict; durability_mode wins.
        if durability_mode == "safe" and sync_writes:
            durability_mode = "strict"
        if durability_mode not in ("safe", "fast", "strict"):
            raise ValueError(
                f"durability_mode must be 'safe', 'fast', or 'strict'; got {durability_mode!r}"
            )
        self._wal_path = Path(wal_path)
        self._wal_path.parent.mkdir(parents=True, exist_ok=True)
        self._rwlock = _ReadWriteLock()
        self._lock = self._rwlock._lock   # write lock (RLock) — kept for PIPELINE compatibility
        self._durability_mode: str = durability_mode
        self._sync_writes: bool = durability_mode == "strict"

        self._kv: dict[str, dict[str, Any]] = {}
        self._queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._streams: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._stream_seq: dict[str, int] = defaultdict(int)

        self._records_replayed = 0
        self._compressed_bytes_written = 0
        self._uncompressed_bytes_written = 0

        # Batch state for "fast" mode.
        self._pending_batch: list[bytes] = []
        self._batch_size: int = 256
        self._flush_interval: float = 0.050  # 50 ms
        self._last_batch_flush: float = time.time()
        self._closed: bool = False
        self._flush_event: threading.Event = threading.Event()
        self._flush_thread_started: bool = False

        # Persistent WAL handle — kept open to avoid repeated open() syscalls.
        if self._wal_path.exists():
            self._replay()
        else:
            self._wal_path.touch()
        self._wal_handle: io.RawIOBase = open(self._wal_path, "ab")  # noqa: SIM115

        if durability_mode == "fast":
            self._start_flush_timer()

    # -- public key-value API -------------------------------------------------

    def put(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        with self._lock:
            self._put_locked(key, value, ttl_seconds=ttl_seconds)

    def mset(self, mapping: dict[str, Any], ttl_seconds: int | None = None) -> int:
        with self._lock:
            for key, value in mapping.items():
                self._put_locked(str(key), value, ttl_seconds=ttl_seconds)
            return len(mapping)

    def mincr(self, updates: list[dict[str, int]], ttl_seconds: int | None = None) -> dict[str, int]:
        with self._lock:
            results: dict[str, int] = {}
            for update in updates:
                key = str(update["key"])
                amount = int(update.get("amount", 1))
                results[key] = self._incr_locked(key, amount=amount, ttl_seconds=ttl_seconds)
            return results

    def get(self, key: str, default: Any = None) -> Any:
        with self._rwlock.read_lock():
            return self._get_locked(key, default)

    def delete(self, key: str) -> bool:
        with self._lock:
            return self._delete_locked(key)

    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        with self._lock:
            return self._incr_locked(key, amount=amount, ttl_seconds=ttl_seconds)

    # -- queue API ------------------------------------------------------------

    def enqueue(self, queue: str, payload: dict[str, Any]) -> int:
        record = {
            "op": "enqueue",
            "queue": queue,
            "payload": payload,
            "ts": time.time(),
        }
        with self._lock:
            self._append_record(record)
            self._apply_record(record)
            return len(self._queues[queue])

    def dequeue(self, queue: str) -> dict[str, Any] | None:
        with self._lock:
            current = self._queues.get(queue)
            if not current:
                return None
            payload = current[0]
            record = {
                "op": "dequeue",
                "queue": queue,
                "ts": time.time(),
            }
            self._append_record(record)
            self._apply_record(record)
            return payload

    def queue_length(self, queue: str) -> int:
        with self._rwlock.read_lock():
            return len(self._queues.get(queue, ()))

    # -- convenience inspection API ------------------------------------------

    def exists(self, key: str) -> bool:
        """Return True if *key* exists and has not expired."""
        with self._rwlock.read_lock():
            self._purge_expired_key(key)
            return key in self._kv

    def keys(self, prefix: str = "") -> list[str]:
        """Return all live key names, optionally filtered by *prefix*."""
        with self._rwlock.read_lock():
            self._purge_all_expired()
            if prefix:
                return [k for k in self._kv if k.startswith(prefix)]
            return list(self._kv)

    def ttl(self, key: str) -> float | None:
        """Return remaining TTL in seconds, -1.0 if persistent, None if absent."""
        with self._rwlock.read_lock():
            self._purge_expired_key(key)
            entry = self._kv.get(key)
            if entry is None:
                return None
            expires_at = entry.get("expires_at")
            if expires_at is None:
                return -1.0
            return max(float(expires_at) - time.time(), 0.0)

    def mget(self, keys_list: list[str]) -> list[Any]:
        """Return values for multiple keys in one call."""
        with self._rwlock.read_lock():
            return [self._get_locked(k) for k in keys_list]

    def flushall(self) -> None:
        """Wipe all in-memory state and record a checkpoint in the WAL."""
        record = {"op": "flushall", "ts": time.time()}
        with self._lock:
            self._append_record(record)
            self._apply_record(record)

    # -- stream API -----------------------------------------------------------

    def append_event(self, stream: str, event: dict[str, Any]) -> int:
        with self._lock:
            seq = self._stream_seq[stream] + 1
            record = {
                "op": "append_event",
                "stream": stream,
                "seq": seq,
                "event": event,
                "ts": time.time(),
            }
            self._append_record(record)
            self._apply_record(record)
            return seq

    def read_events(self, stream: str, after_seq: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            events = self._streams.get(stream, [])
            return [item for item in events if int(item["seq"]) > after_seq][:limit]

    # -- stats ----------------------------------------------------------------

    def stats(self) -> EngineStats:
        with self._lock:
            self._purge_all_expired()
            return EngineStats(
                keys=len(self._kv),
                queues=sum(1 for q in self._queues.values() if q),
                streams=sum(1 for s in self._streams.values() if s),
                records_replayed=self._records_replayed,
                wal_size_bytes=self._wal_path.stat().st_size if self._wal_path.exists() else 0,
                compressed_bytes_written=self._compressed_bytes_written,
                uncompressed_bytes_written=self._uncompressed_bytes_written,
            )

    # -- public lifecycle helpers --------------------------------------------

    def flush(self) -> None:
        """Flush any pending batch and the WAL write-buffer to the OS page cache."""
        with self._lock:
            if self._durability_mode == "fast" and self._pending_batch:
                self._flush_pending_batch()
            self._wal_handle.flush()

    def close(self) -> None:
        """Flush any pending batch and close the WAL file handle."""
        with self._lock:
            self._closed = True
            try:
                if self._durability_mode == "fast" and self._pending_batch:
                    self._flush_pending_batch()
                self._wal_handle.flush()
            finally:
                self._wal_handle.close()
        if self._flush_thread_started:
            self._flush_event.set()  # signal background timer to stop

    def __enter__(self) -> "CompressedStateEngine":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- runtime configuration -----------------------------------------------

    @property
    def durability_mode(self) -> str:
        """Current durability mode: ``"safe"``, ``"fast"``, or ``"strict"``."""
        return self._durability_mode

    def set_durability_mode(self, mode: str) -> None:
        """Switch durability mode at runtime.

        Any pending fast-mode batch is flushed before the switch takes effect.
        Starting the background flush timer for ``"fast"`` mode is idempotent —
        at most one timer thread runs per engine instance.
        """
        if mode not in ("safe", "fast", "strict"):
            raise ValueError(
                f"durability_mode must be 'safe', 'fast', or 'strict'; got {mode!r}"
            )
        with self._lock:
            if self._durability_mode == "fast" and self._pending_batch:
                self._flush_pending_batch()
            self._durability_mode = mode
            self._sync_writes = mode == "strict"
        if mode == "fast" and not self._flush_thread_started:
            self._start_flush_timer()

    def _start_flush_timer(self) -> None:
        """Launch a daemon thread that auto-flushes the pending batch every 50 ms."""
        self._flush_event.clear()
        self._flush_thread_started = True

        def _timer_loop() -> None:
            # wait() returns True when the event is set (stop signal),
            # False on timeout (time to flush).
            while not self._flush_event.wait(self._flush_interval):
                with self._lock:
                    if self._pending_batch and not self._closed:
                        self._flush_pending_batch()

        t = threading.Thread(target=_timer_loop, daemon=True, name="ahana-batch-flush")
        t.start()

    def _put_locked(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        now = time.time()
        expires_at = now + ttl_seconds if ttl_seconds is not None else None
        record = {
            "op": "put",
            "key": key,
            "value": value,
            "expires_at": expires_at,
            "ts": now,
        }
        self._append_record(record)
        self._apply_record(record)

    def _get_locked(self, key: str, default: Any = None) -> Any:
        self._purge_expired_key(key)
        entry = self._kv.get(key)
        if entry is None:
            return default
        return entry["value"]

    def _delete_locked(self, key: str) -> bool:
        record = {"op": "delete", "key": key, "ts": time.time()}
        existed = key in self._kv
        self._append_record(record)
        self._apply_record(record)
        return existed

    def _incr_locked(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        current = self._get_locked(key, 0)
        if not isinstance(current, int):
            raise TypeError(f"Cannot increment non-integer key: {key}")
        updated = current + amount
        now = time.time()
        expires_at = now + ttl_seconds if ttl_seconds is not None else self._kv.get(key, {}).get("expires_at")
        record = {
            "op": "put",
            "key": key,
            "value": updated,
            "expires_at": expires_at,
            "ts": now,
        }
        self._append_record(record)
        self._apply_record(record)
        return updated

    # -- persistence ----------------------------------------------------------

    def _append_record(self, record: dict[str, Any]) -> None:
        raw = _json_dumps(record)
        if self._durability_mode == "fast":
            # Accumulate raw JSON bytes; compress as a batch later.
            self._pending_batch.append(raw)
            now = time.time()
            if (
                len(self._pending_batch) >= self._batch_size
                or now - self._last_batch_flush >= self._flush_interval
            ):
                self._flush_pending_batch()
        else:
            if len(raw) <= self._NO_COMPRESS_THRESHOLD:
                payload = self._RAW_FRAME_MARKER + raw
                stored_len = len(raw)
            else:
                compressed = _compress(raw)
                payload = compressed
                stored_len = len(compressed)
            frame = self._LEN_STRUCT.pack(len(payload)) + payload
            self._wal_handle.write(frame)
            if self._sync_writes:
                self._sync_record()
            self._uncompressed_bytes_written += len(raw)
            self._compressed_bytes_written += stored_len

    def _sync_record(self) -> None:
        self._wal_handle.flush()
        os.fsync(self._wal_handle.fileno())

    def _flush_pending_batch(self) -> None:
        """Compress all pending records as one WAL frame and write it.

        Batch frame encoding (backward-compatible with existing WAL files):
          [4-byte big-endian length] [0xFF marker byte] [zstd(newline-delimited JSON)]

        The 0xFF marker is safe because valid single-record frames start with
        the zstd magic byte 0x28, so existing WALs replay without any change.
        """
        batch = self._pending_batch
        self._pending_batch = []
        self._last_batch_flush = time.time()
        if not batch:
            return
        uncompressed_size = sum(len(r) for r in batch)
        combined = b"\n".join(batch)
        compressed = _compress(combined)
        # Prefix with 0xFF to mark this as a batch frame during replay.
        payload = self._BATCH_FRAME_MARKER + compressed
        frame = self._LEN_STRUCT.pack(len(payload)) + payload
        self._wal_handle.write(frame)
        self._wal_handle.flush()
        self._uncompressed_bytes_written += uncompressed_size
        self._compressed_bytes_written += len(compressed)

    def _replay(self) -> None:
        with self._wal_path.open("rb") as handle:
            while True:
                length_raw = handle.read(self._LEN_STRUCT.size)
                if not length_raw:
                    break
                if len(length_raw) != self._LEN_STRUCT.size:
                    raise ValueError("Corrupt WAL length prefix")
                (length,) = self._LEN_STRUCT.unpack(length_raw)
                payload = handle.read(length)
                if len(payload) != length:
                    raise ValueError("Corrupt WAL payload")
                if payload[:1] == self._BATCH_FRAME_MARKER:
                    # Batch frame: compressed newline-delimited JSON records.
                    compressed = payload[1:]
                    raw_ndjson = _decompress(compressed)
                    for raw_line in raw_ndjson.split(b"\n"):
                        raw_line = raw_line.strip()
                        if raw_line:
                            record = _json_loads(raw_line)
                            self._apply_record(record)
                            self._records_replayed += 1
                    self._uncompressed_bytes_written += len(raw_ndjson)
                    self._compressed_bytes_written += len(compressed)
                elif payload[:1] == self._RAW_FRAME_MARKER:
                    raw = payload[1:]
                    record = _json_loads(raw)
                    self._apply_record(record)
                    self._records_replayed += 1
                    self._uncompressed_bytes_written += len(raw)
                    self._compressed_bytes_written += len(raw)
                else:
                    # Single-record frame (legacy and current "safe"/"strict").
                    raw = _decompress(payload)
                    record = _json_loads(raw)
                    self._apply_record(record)
                    self._records_replayed += 1
                    self._uncompressed_bytes_written += len(raw)
                    self._compressed_bytes_written += len(payload)

    # -- apply/replay helpers -------------------------------------------------

    def _apply_record(self, record: dict[str, Any]) -> None:
        op = record["op"]
        if op == "put":
            self._kv[str(record["key"])] = {
                "value": record.get("value"),
                "expires_at": record.get("expires_at"),
            }
            return
        if op == "delete":
            self._kv.pop(str(record["key"]), None)
            return
        if op == "enqueue":
            self._queues[str(record["queue"])] .append(dict(record["payload"]))
            return
        if op == "dequeue":
            queue = self._queues.get(str(record["queue"]))
            if queue:
                queue.popleft()
            return
        if op == "append_event":
            stream = str(record["stream"])
            seq = int(record["seq"])
            self._streams[stream].append({"seq": seq, "event": record["event"]})
            self._stream_seq[stream] = max(self._stream_seq[stream], seq)
            return
        if op == "flushall":
            self._kv.clear()
            self._queues.clear()
            self._streams.clear()
            self._stream_seq.clear()
            return
        raise ValueError(f"Unknown operation: {op}")

    def _purge_expired_key(self, key: str) -> None:
        entry = self._kv.get(key)
        if entry is None:
            return
        expires_at = entry.get("expires_at")
        if expires_at is not None and float(expires_at) <= time.time():
            self._kv.pop(key, None)

    def _purge_all_expired(self) -> None:
        expired = []
        now = time.time()
        for key, entry in self._kv.items():
            expires_at = entry.get("expires_at")
            if expires_at is not None and float(expires_at) <= now:
                expired.append(key)
        for key in expired:
            self._kv.pop(key, None)
