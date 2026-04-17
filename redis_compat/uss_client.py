"""
Async client for UniversalStateServer (newline-delimited JSON over TCP).

Each ``USSClient`` instance owns one persistent TCP connection.  Commands
are serialized through an asyncio lock so a single client is safe for
concurrent callers from the same event-loop task.

Connection is established lazily on first use and re-established
automatically after a network error.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


class USSError(Exception):
    """Raised when USS returns ``{"ok": false, ...}``."""


class USSClient:
    """Async JSON-over-TCP client for UniversalStateServer."""

    __slots__ = ("_host", "_port", "_reader", "_writer", "_lock")

    def __init__(self, host: str = "127.0.0.1", port: int = 9633) -> None:
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        log.debug("USS connected to %s:%d", self._host, self._port)

    async def close(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            finally:
                self._reader = self._writer = None

    async def _ensure_connected(self) -> None:
        if self._writer is None or self._writer.is_closing():
            await self.connect()

    # ------------------------------------------------------------------
    # Low-level send
    # ------------------------------------------------------------------

    async def _send(self, cmd: dict[str, Any]) -> dict[str, Any]:
        """Send one JSON command and return the parsed response."""
        async with self._lock:
            for attempt in range(2):
                try:
                    await self._ensure_connected()
                    assert self._writer and self._reader
                    payload = json.dumps(cmd, separators=(",", ":")) + "\n"
                    self._writer.write(payload.encode())
                    await self._writer.drain()
                    line = await self._reader.readline()
                    if not line:
                        raise ConnectionResetError("USS closed connection")
                    return json.loads(line.decode().strip())
                except (ConnectionResetError, ConnectionError, OSError, EOFError) as exc:
                    if attempt == 1:
                        raise USSError(f"USS unreachable: {exc}") from exc
                    log.debug("USS connection lost, reconnecting: %s", exc)
                    await self.close()
            # unreachable
            raise USSError("USS unreachable after retries")

    async def send(self, cmd: dict[str, Any]) -> Any:
        """
        Send a command and return its ``result`` value.
        Raises ``USSError`` if ``ok`` is False.
        """
        resp = await self._send(cmd)
        if not resp.get("ok", False):
            raise USSError(resp.get("error", "unknown error"))
        return resp.get("result")

    # ------------------------------------------------------------------
    # KV commands
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[Any]:
        return await self.send({"cmd": "GET", "key": key})

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: Optional[int] = None,
    ) -> None:
        cmd: dict[str, Any] = {"cmd": "SET", "key": key, "value": value}
        if ttl_seconds is not None:
            cmd["ttl_seconds"] = ttl_seconds
        await self.send(cmd)

    async def delete(self, key: str) -> int:
        """Return 1 if the key existed and was deleted, 0 otherwise."""
        return int(await self.send({"cmd": "DEL", "key": key}))

    async def exists(self, key: str) -> int:
        """Return 1 if the key exists, 0 otherwise."""
        return int(await self.send({"cmd": "EXISTS", "key": key}))

    async def ttl(self, key: str) -> int:
        """Return TTL seconds, -1 if persistent, -2 if not found."""
        result = await self.send({"cmd": "TTL", "key": key})
        return int(result) if result is not None else -2

    async def keys(self, prefix: str = "") -> list[str]:
        """Return keys matching the given prefix."""
        result = await self.send({"cmd": "KEYS", "prefix": prefix})
        return list(result) if result else []

    async def flushall(self) -> None:
        await self.send({"cmd": "FLUSHALL"})

    # ------------------------------------------------------------------
    # Multi-key
    # ------------------------------------------------------------------

    async def mget(self, keys: list[str]) -> list[Optional[Any]]:
        result = await self.send({"cmd": "MGET", "keys": keys})
        return list(result) if result is not None else [None] * len(keys)

    async def mset(self, values: dict[str, Any], *, ttl_seconds: Optional[int] = None) -> int:
        """Atomically set multiple keys. Returns count set."""
        cmd: dict[str, Any] = {"cmd": "MSET", "values": values}
        if ttl_seconds is not None:
            cmd["ttl_seconds"] = ttl_seconds
        result = await self.send(cmd)
        return int(result) if result is not None else len(values)

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------

    async def incr(self, key: str, amount: int = 1, *, ttl_seconds: Optional[int] = None) -> int:
        cmd: dict[str, Any] = {"cmd": "INCR", "key": key, "amount": amount}
        if ttl_seconds is not None:
            cmd["ttl_seconds"] = ttl_seconds
        return int(await self.send(cmd))

    async def mincr(self, updates: list[dict[str, Any]]) -> list[int]:
        """Atomic multi-increment. updates: [{"key": k, "amount": n}, ...]"""
        result = await self.send({"cmd": "MINCR", "updates": updates})
        return list(result) if result else []

    # ------------------------------------------------------------------
    # Queue (FIFO)
    # ------------------------------------------------------------------

    async def enqueue(self, queue: str, payload: dict[str, Any]) -> int:
        """Enqueue a dict payload; return new queue size."""
        return int(await self.send({"cmd": "ENQUEUE", "queue": queue, "payload": payload}))

    async def dequeue(self, queue: str) -> Optional[dict[str, Any]]:
        """Pop oldest item from queue; return None if empty."""
        return await self.send({"cmd": "DEQUEUE", "queue": queue})

    async def qlen(self, queue: str) -> int:
        return int(await self.send({"cmd": "QLEN", "queue": queue}))

    # ------------------------------------------------------------------
    # Streams (append-only event log)
    # ------------------------------------------------------------------

    async def xadd(self, stream: str, event: dict[str, Any]) -> int:
        """Append event dict to stream; return sequence number."""
        return int(await self.send({"cmd": "XADD", "stream": stream, "event": event}))

    async def xrange(
        self, stream: str, *, after_seq: int = 0, limit: int = 100
    ) -> list[Any]:
        """Read events after after_seq (exclusive); returns list of event dicts."""
        result = await self.send(
            {"cmd": "XRANGE", "stream": stream, "after_seq": after_seq, "limit": limit}
        )
        return list(result) if result else []

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def stats(self) -> dict[str, Any]:
        result = await self.send({"cmd": "STATS"})
        return dict(result) if result else {}
