"""
Async RESP TCP server.

Each accepted TCP connection spawns an async task that:
  1. Creates a dedicated USSClient (one TCP connection to USS per Redis client)
  2. Creates a CommandHandler backed by that USSClient
  3. Reads raw bytes → parses RESP commands → dispatches → writes RESP replies

The server is a plain asyncio.start_server with no external dependencies.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .handlers import CommandHandler
from .resp import RespParser, RespProtocolError
from .uss_client import USSClient

log = logging.getLogger(__name__)

# Idle timeout: disconnect silent clients after this many seconds.
_IDLE_TIMEOUT_SECS = 300


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    uss_host: str,
    uss_port: int,
) -> None:
    peer = writer.get_extra_info("peername", ("?", 0))
    log.debug("Redis client connected: %s:%s", peer[0], peer[1])

    uss = USSClient(uss_host, uss_port)
    try:
        await uss.connect()
    except Exception as exc:
        log.error("Cannot reach USS at %s:%d — dropping client. Error: %s", uss_host, uss_port, exc)
        try:
            writer.close()
        except Exception:
            pass
        return

    handler = CommandHandler(uss)
    parser = RespParser()

    try:
        while True:
            try:
                data = await asyncio.wait_for(reader.read(65536), timeout=_IDLE_TIMEOUT_SECS)
            except asyncio.TimeoutError:
                log.debug("Client %s:%s idle timeout — closing", peer[0], peer[1])
                break
            if not data:
                break

            parser.feed(data)
            while True:
                try:
                    cmd = parser.get_command()
                except RespProtocolError as exc:
                    log.warning("RESP parse error from %s: %s", peer[0], exc)
                    from .resp import encode_error
                    writer.write(encode_error(str(exc)))
                    await writer.drain()
                    return  # Unrecoverable parse error — close connection
                if cmd is None:
                    break

                try:
                    response = await handler.handle(cmd)
                except Exception as exc:
                    log.exception("Unhandled error for command %s", cmd)
                    from .resp import encode_error
                    response = encode_error(str(exc))

                writer.write(response)

                # Flush after each command — keeps latency low for single-request clients.
                # For pipelined clients the OS buffer absorbs the extra drain well.
                if not writer.transport.is_closing():
                    await writer.drain()

                # QUIT command — server closes after sending OK
                if cmd and cmd[0].upper() == "QUIT":
                    return

    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as exc:
        log.warning("Client %s:%s error: %s", peer[0], peer[1], exc)
    finally:
        await uss.close()
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        log.debug("Redis client disconnected: %s:%s", peer[0], peer[1])


class RespServer:
    """
    Lifecycle-managed RESP server.

    Usage::

        server = RespServer("0.0.0.0", 6379, uss_host="127.0.0.1", uss_port=9633)
        await server.start()
        await server.serve_forever()   # blocks until stop() is called
        await server.stop()
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 6379,
        uss_host: str = "127.0.0.1",
        uss_port: int = 9633,
    ) -> None:
        self._host = host
        self._port = port
        self._uss_host = uss_host
        self._uss_port = uss_port
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            lambda r, w: _handle_client(r, w, self._uss_host, self._uss_port),
            self._host,
            self._port,
            reuse_address=True,
        )
        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        log.info(
            "AhanaFlow Redis Compat Layer listening on [%s] — routing to USS %s:%d",
            addrs,
            self._uss_host,
            self._uss_port,
        )

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            log.info("AhanaFlow Redis Compat Layer stopped")


async def run_server(
    host: str = "0.0.0.0",
    port: int = 6379,
    uss_host: str = "127.0.0.1",
    uss_port: int = 9633,
) -> None:
    """Convenience coroutine — start the server and block forever."""
    server = RespServer(host, port, uss_host, uss_port)
    await server.serve_forever()
