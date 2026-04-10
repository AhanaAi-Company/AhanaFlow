"""
ahanaflow.async_client — Asyncio TCP client for AhanaFlow server

Example::

    from ahanaflow import AsyncAhanaFlowClient

    async def main():
        client = AsyncAhanaFlowClient("localhost", 9633)
        await client.connect()

        await client.set("key", {"hello": "world"}, ttl_seconds=60)
        print(await client.get("key"))   # → {"hello": "world"}

        await client.close()

    asyncio.run(main())
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

from ahanaflow.exceptions import (
    AhanaFlowError,
    CommandError,
    ConnectionError,
    ProtocolError,
    TimeoutError,
)

_TIMEOUT_DEFAULT = 5.0


class AsyncAhanaFlowClient:
    """
    Asyncio TCP client for a remote AhanaFlow server.

    All public methods are coroutines. Use with ``async with`` or call
    ``await client.connect()`` / ``await client.close()`` manually.

    Example::

        async with AsyncAhanaFlowClient("localhost", 9633) as client:
            await client.set("x", 42)
            v = await client.get("x")
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
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the TCP connection to the server."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise ConnectionError(
                f"Cannot connect to AhanaFlow server at {self._host}:{self._port}: {exc}"
            ) from exc

    async def close(self) -> None:
        """Close the TCP connection."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            finally:
                self._writer = None
                self._reader = None

    async def __aenter__(self) -> "AsyncAhanaFlowClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ── Low-level send/recv ─────────────────────────────────────────────────

    async def _send(self, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            if self._writer is None:
                if self._auto_reconnect:
                    await self.connect()
                else:
                    raise ConnectionError("Not connected. Call connect() first.")
            raw = (json.dumps(payload) + "\n").encode()
            try:
                self._writer.write(raw)           # type: ignore[union-attr]
                await self._writer.drain()         # type: ignore[union-attr]
                return await self._read_line()
            except (OSError, BrokenPipeError) as exc:
                self._writer = None
                self._reader = None
                if self._auto_reconnect:
                    await self.connect()
                    self._writer.write(raw)        # type: ignore[union-attr]
                    await self._writer.drain()     # type: ignore[union-attr]
                    return await self._read_line()
                raise ConnectionError(f"Connection lost: {exc}") from exc

    async def _read_line(self) -> dict[str, Any]:
        assert self._reader is not None
        try:
            line = await asyncio.wait_for(
                self._reader.readline(),
                timeout=self._timeout,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for server response") from exc
        if not line:
            self._writer = None
            self._reader = None
            raise ConnectionError("Server closed the connection")
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"Invalid JSON from server: {line!r}") from exc

    async def _cmd(self, payload: dict[str, Any]) -> Any:
        resp = await self._send(payload)
        if not resp.get("ok"):
            raise CommandError(resp.get("error", "Unknown server error"))
        return resp.get("result")

    # ── Key-Value ──────────────────────────────────────────────────────────

    async def set(self, key: str, value: Any, *, ttl_seconds: int | None = None) -> bool:
        payload: dict[str, Any] = {"cmd": "SET", "key": key, "value": value}
        if ttl_seconds is not None:
            payload["ttl"] = ttl_seconds
        return bool(await self._cmd(payload))

    async def get(self, key: str) -> Any:
        return await self._cmd({"cmd": "GET", "key": key})

    async def delete(self, key: str) -> bool:
        return bool(await self._cmd({"cmd": "DEL", "key": key}))

    async def incr(self, key: str, amount: int = 1) -> int:
        return int(await self._cmd({"cmd": "INCR", "key": key, "amount": amount}))

    async def exists(self, key: str) -> bool:
        return bool(await self._cmd({"cmd": "EXISTS", "key": key}))

    async def keys(self, prefix: str = "") -> list[str]:
        return list(await self._cmd({"cmd": "KEYS", "prefix": prefix}) or [])

    async def ttl(self, key: str) -> int:
        return int(await self._cmd({"cmd": "TTL", "key": key}))

    async def mget(self, *keys: str) -> list[Any]:
        return list(await self._cmd({"cmd": "MGET", "keys": list(keys)}) or [])

    # ── Queues ─────────────────────────────────────────────────────────────

    async def enqueue(self, queue: str, payload: Any) -> bool:
        return bool(await self._cmd({"cmd": "ENQUEUE", "queue": queue, "payload": payload}))

    async def dequeue(self, queue: str) -> Any:
        return await self._cmd({"cmd": "DEQUEUE", "queue": queue})

    async def qlen(self, queue: str) -> int:
        return int(await self._cmd({"cmd": "QLEN", "queue": queue}))

    # ── Streams ────────────────────────────────────────────────────────────

    async def xadd(self, stream: str, event: Any) -> int:
        return int(await self._cmd({"cmd": "XADD", "stream": stream, "event": event}))

    async def xrange(self, stream: str, after: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        return list(
            await self._cmd({"cmd": "XRANGE", "stream": stream, "after": after, "limit": limit}) or []
        )

    # ── Control ────────────────────────────────────────────────────────────

    async def ping(self) -> str:
        return str(await self._cmd({"cmd": "PING"}))

    async def stats(self) -> dict[str, Any]:
        return dict(await self._cmd({"cmd": "STATS"}) or {})

    async def config_get(self, key: str) -> Any:
        return await self._cmd({"cmd": "CONFIG", "action": "GET", "key": key})

    async def config_set(self, key: str, value: Any) -> bool:
        return bool(await self._cmd({"cmd": "CONFIG", "action": "SET", "key": key, "value": value}))

    async def set_durability_mode(self, mode: str) -> bool:
        """
        Switch durability mode at runtime.
        Modes: "safe" (~967K ops/s), "fast" (~1.57M ops/s), "strict" (~770K ops/s)
        (March 30, 2026 in-process benchmark)
        """
        if mode not in ("safe", "fast", "strict"):
            raise AhanaFlowError(f"Unknown durability mode: {mode!r}")
        return await self.config_set("durability_mode", mode)

    async def flushall(self) -> bool:
        """Wipe all state. Irreversible."""
        return bool(await self._cmd({"cmd": "FLUSHALL"}))
