"""
ahanaflow.client — Synchronous TCP client for AhanaFlow server

Protocol: newline-delimited JSON over TCP (port 9633 default).

Each request:  {"cmd": "SET", ...}  + newline
Each response: {"ok": true, "result": ...}  or  {"ok": false, "error": "..."}
"""
from __future__ import annotations

import json
import socket
import threading
from typing import Any

from ahanaflow.exceptions import (
    AhanaFlowError,
    CommandError,
    ConnectionError,
    ProtocolError,
    TimeoutError,
)

_TIMEOUT_DEFAULT = 5.0  # seconds
_BUFSIZE = 65536


class AhanaFlowClient:
    """
    Synchronous TCP client for a remote AhanaFlow server.

    Example::

        client = AhanaFlowClient("localhost", 9633)
        client.set("session:abc", {"user": "alice"}, ttl_seconds=3600)
        print(client.get("session:abc"))   # → {"user": "alice"}
        client.close()

    The client is also a context manager::

        with AhanaFlowClient("localhost", 9633) as client:
            client.set("x", 1)
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9633,
        *,
        timeout: float = _TIMEOUT_DEFAULT,
        auto_reconnect: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()
        self._buf = b""
        self._connect()

    # ── Connection ─────────────────────────────────────────────────────────

    def _connect(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self._timeout)
            sock.connect((self._host, self._port))
            self._sock = sock
            self._buf = b""
        except OSError as exc:
            raise ConnectionError(
                f"Cannot connect to AhanaFlow server at {self._host}:{self._port}: {exc}"
            ) from exc

    def _ensure_connected(self) -> None:
        if self._sock is None:
            if self._auto_reconnect:
                self._connect()
            else:
                raise ConnectionError("Not connected. Call connect() first.")

    def close(self) -> None:
        """Close the connection to the server."""
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass
                finally:
                    self._sock = None

    def __enter__(self) -> "AhanaFlowClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── Low-level send/recv ─────────────────────────────────────────────────

    def _send(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._ensure_connected()
            raw = (json.dumps(payload) + "\n").encode()
            try:
                self._sock.sendall(raw)          # type: ignore[union-attr]
                return self._read_line()
            except (OSError, BrokenPipeError) as exc:
                self._sock = None
                if self._auto_reconnect:
                    self._connect()
                    self._sock.sendall(raw)      # type: ignore[union-attr]
                    return self._read_line()
                raise ConnectionError(f"Connection lost: {exc}") from exc

    def _read_line(self) -> dict[str, Any]:
        assert self._sock is not None
        while b"\n" not in self._buf:
            try:
                chunk = self._sock.recv(_BUFSIZE)
            except socket.timeout as exc:
                raise TimeoutError("Timed out waiting for server response") from exc
            if not chunk:
                self._sock = None
                raise ConnectionError("Server closed the connection")
            self._buf += chunk

        line, self._buf = self._buf.split(b"\n", 1)
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"Invalid JSON from server: {line!r}") from exc

    def _cmd(self, payload: dict[str, Any]) -> Any:
        resp = self._send(payload)
        if not resp.get("ok"):
            raise CommandError(resp.get("error", "Unknown server error"))
        return resp.get("result")

    # ── Key-Value ──────────────────────────────────────────────────────────

    def set(self, key: str, value: Any, *, ttl_seconds: int | None = None) -> bool:
        """
        Store *value* under *key*. Optionally expires after *ttl_seconds*.

        Returns True on success.
        """
        payload: dict[str, Any] = {"cmd": "SET", "key": key, "value": value}
        if ttl_seconds is not None:
            payload["ttl"] = ttl_seconds
        result = self._cmd(payload)
        return bool(result)

    def get(self, key: str) -> Any:
        """
        Retrieve the value for *key*. Returns None if missing or expired.
        """
        return self._cmd({"cmd": "GET", "key": key})

    def delete(self, key: str) -> bool:
        """Delete *key*. Returns True if key existed."""
        return bool(self._cmd({"cmd": "DEL", "key": key}))

    def incr(self, key: str, amount: int = 1) -> int:
        """Atomically increment *key* by *amount* (default 1). Returns new value."""
        return int(self._cmd({"cmd": "INCR", "key": key, "amount": amount}))

    def exists(self, key: str) -> bool:
        """Return True if *key* exists and has not expired."""
        return bool(self._cmd({"cmd": "EXISTS", "key": key}))

    def keys(self, prefix: str = "") -> list[str]:
        """List all live keys, optionally filtered by *prefix*."""
        return list(self._cmd({"cmd": "KEYS", "prefix": prefix}) or [])

    def ttl(self, key: str) -> int:
        """
        Return remaining TTL in seconds for *key*.
        -1 = persistent (no expiry). -2 = key not found.
        """
        return int(self._cmd({"cmd": "TTL", "key": key}))

    def mget(self, *keys: str) -> list[Any]:
        """Fetch multiple keys in a single round-trip. Returns list in same order."""
        return list(self._cmd({"cmd": "MGET", "keys": list(keys)}) or [])

    # ── Queues ─────────────────────────────────────────────────────────────

    def enqueue(self, queue: str, payload: Any) -> bool:
        """Push *payload* onto the tail of FIFO queue *queue*."""
        return bool(self._cmd({"cmd": "ENQUEUE", "queue": queue, "payload": payload}))

    def dequeue(self, queue: str) -> Any:
        """
        Pop and return the head item from queue *queue*.
        Returns None if the queue is empty.
        """
        return self._cmd({"cmd": "DEQUEUE", "queue": queue})

    def qlen(self, queue: str) -> int:
        """Return the current depth of queue *queue*."""
        return int(self._cmd({"cmd": "QLEN", "queue": queue}))

    # ── Streams ────────────────────────────────────────────────────────────

    def xadd(self, stream: str, event: Any) -> int:
        """
        Append *event* to stream *stream*.
        Returns the assigned monotonic sequence ID.
        """
        return int(self._cmd({"cmd": "XADD", "stream": stream, "event": event}))

    def xrange(
        self,
        stream: str,
        after: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Read events from *stream* with seq > *after*, up to *limit* items.
        Each item is {"id": <int>, "event": <any>}.
        """
        return list(
            self._cmd({"cmd": "XRANGE", "stream": stream, "after": after, "limit": limit}) or []
        )

    # ── Control ────────────────────────────────────────────────────────────

    def ping(self) -> str:
        """Send PING. Returns 'PONG' on success."""
        return str(self._cmd({"cmd": "PING"}))

    def stats(self) -> dict[str, Any]:
        """Return live engine stats dict (keys, queues, WAL size, ratio, etc.)."""
        return dict(self._cmd({"cmd": "STATS"}) or {})

    def config_get(self, key: str) -> Any:
        """Read a runtime config setting (e.g. 'durability_mode')."""
        return self._cmd({"cmd": "CONFIG", "action": "GET", "key": key})

    def config_set(self, key: str, value: Any) -> bool:
        """
        Set a runtime config value.
        Example: client.config_set("durability_mode", "fast")
        """
        return bool(self._cmd({"cmd": "CONFIG", "action": "SET", "key": key, "value": value}))

    def set_durability_mode(self, mode: str) -> bool:
        """
        Switch the engine durability mode at runtime.

        Modes:
                    "safe"   — OS-buffered.         ~967K ops/s
                    "fast"   — 50ms batch flush.    ~1.57M ops/s
                    "strict" — flush + fsync.        ~770K ops/s

        (March 30, 2026 in-process benchmark)
        """
        if mode not in ("safe", "fast", "strict"):
            raise AhanaFlowError(f"Unknown durability mode: {mode!r}. Use 'safe', 'fast', or 'strict'.")
        return self.config_set("durability_mode", mode)

    def flushall(self) -> bool:
        """
        Wipe ALL state and checkpoint the WAL.
        This is irreversible. Use with caution.
        """
        return bool(self._cmd({"cmd": "FLUSHALL"}))
