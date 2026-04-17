from __future__ import annotations

import asyncio
import hashlib
import socket
import time
from pathlib import Path
from typing import Any

from backend.state_engine import CompressedStateEngine

from .protocol import ProtocolError, decode_command, encode_response
from .protocol import _json_loads, _json_dumps
from redis_compat.resp import RESP_OK, RESP_PONG, RespParser, RespProtocolError, encode_array, encode_bulk_string, encode_error, encode_integer
from .security import SecurityConfig, SecurityError, SecurityMiddleware
from .server import UniversalStateServer

try:
    from ._resp_accel import FastRespParser as _FastRespParser
except ImportError:  # pragma: no cover
    _FastRespParser = None


# Pre-encoded compact responses for hot-path shortcuts
_COMPACT_OK = b'{"ok":true,"result":"OK"}\n'
_COMPACT_PONG = b'{"ok":true,"result":"PONG"}\n'


class AsyncUniversalStateServer(UniversalStateServer):
    def __init__(
        self,
        wal_path: str | Path,
        host: str = "127.0.0.1",
        port: int = 9633,
        *,
        durability_mode: str = "safe",
        fast_batch_size: int | None = None,
        fast_flush_interval_ms: int | float | None = None,
        no_compress_threshold: int | None = None,
        wire_protocol: str = "hybrid",
        security_config: SecurityConfig | None = None,
    ) -> None:
        self._engine = CompressedStateEngine(
            wal_path,
            durability_mode=durability_mode,
            fast_batch_size=fast_batch_size,
            fast_flush_interval_ms=fast_flush_interval_ms,
            no_compress_threshold=no_compress_threshold,
        )
        self._host = host
        self._port = port
        self._security = SecurityMiddleware(security_config) if security_config else None
        if wire_protocol not in {"json", "resp", "hybrid"}:
            raise ValueError("wire_protocol must be 'json', 'resp', or 'hybrid'")
        self._wire_protocol = wire_protocol
        self._server: asyncio.AbstractServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._serving = False
        self._closed = False
        self._bound_address: tuple[str, int] = (host, port)

    @property
    def address(self) -> tuple[str, int]:
        return self._bound_address

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Enable TCP_NODELAY for lower latency on small RESP replies
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client = writer.get_extra_info("peername", ("?", 0))
        client_ip = str(client[0])
        security = self._security

        if security:
            try:
                security.check_connection_limit(client_ip)
                security.register_connection(client_ip)
            except SecurityError as exc:
                writer.write(encode_response({"ok": False, "error": f"security: {exc}"}))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

        wire = self._wire_protocol

        if wire == "hybrid":
            await self._handle_client_hybrid(reader, writer, security, client_ip)
        elif wire == "resp":
            await self._handle_client_resp(reader, writer, security, client_ip)
        else:
            await self._handle_client_json(reader, writer, security, client_ip)

    async def _handle_client_hybrid(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> None:
        """Auto-detect RESP vs compact JSON per read — unified inlined dispatch."""
        # Pre-localize hot engine references for speed
        engine = self._engine
        _get = engine._get_locked
        _put = engine._put_locked
        _incr = engine._incr_locked
        _lock = engine._lock

        resp_parser = None
        json_buffer = bytearray()

        try:
            while True:
                data = await reader.read(262144)
                if not data:
                    break

                first_byte = data[0:1]

                if first_byte == b"*" or (resp_parser is not None and resp_parser._buf):
                    # ---- RESP path ----
                    if resp_parser is None:
                        resp_parser = _FastRespParser() if _FastRespParser is not None else RespParser()

                    resp_parser.feed(data)
                    commands: list[list[str]] = []
                    while True:
                        try:
                            cmd = resp_parser.get_command()
                        except RespProtocolError as exc:
                            writer.write(encode_error(str(exc)))
                            await writer.drain()
                            break
                        if cmd is None:
                            break
                        commands.append(cmd)

                    if commands:
                        writer.write(self._handle_resp_batch(commands, security, client_ip))
                        await writer.drain()
                else:
                    # ---- Compact JSON path (inlined for speed) ----
                    json_buffer.extend(data)
                    responses: list[bytes] = []

                    while True:
                        nl = json_buffer.find(b"\n")
                        if nl < 0:
                            break
                        line = bytes(json_buffer[:nl]).rstrip(b"\r")
                        del json_buffer[:nl + 1]
                        if not line:
                            continue

                        try:
                            if security:
                                security.validate_payload_size(line)
                            obj = _json_loads(line)
                        except SecurityError as exc:
                            responses.append(_json_dumps([0, f"security: {exc}"]) + b"\n")
                            continue
                        except Exception as exc:
                            responses.append(_json_dumps([0, f"parse error: {exc}"]) + b"\n")
                            continue

                        if isinstance(obj, list):
                            responses.append(
                                self._dispatch_compact_inlined(
                                    obj, _get, _put, _incr, _lock, security, client_ip,
                                )
                            )
                        elif isinstance(obj, dict):
                            # Dict-style command — use existing decode+dispatch path
                            # (less common, keeps full feature support)
                            responses.append(
                                self._dispatch_dict_command(obj, security, client_ip)
                            )
                        else:
                            responses.append(_json_dumps([0, "unsupported format"]) + b"\n")

                    if responses:
                        writer.writelines(responses)
                        await writer.drain()
        finally:
            if security:
                security.unregister_connection(client_ip)
            writer.close()
            await writer.wait_closed()

    async def _handle_client_resp(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> None:
        """RESP-only wire protocol handler."""
        resp_parser = _FastRespParser() if _FastRespParser is not None else RespParser()
        try:
            while True:
                data = await reader.read(262144)
                if not data:
                    break
                resp_parser.feed(data)
                resp_commands: list[list[str]] = []
                while True:
                    try:
                        resp_args = resp_parser.get_command()
                    except RespProtocolError as exc:
                        writer.write(encode_error(str(exc)))
                        await writer.drain()
                        break
                    if resp_args is None:
                        break
                    resp_commands.append(resp_args)
                if resp_commands:
                    writer.write(self._handle_resp_batch(resp_commands, security, client_ip))
                    await writer.drain()
        finally:
            if security:
                security.unregister_connection(client_ip)
            writer.close()
            await writer.wait_closed()

    async def _handle_client_json(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> None:
        """JSON/compact-only wire protocol handler (legacy path)."""
        buffer = bytearray()
        try:
            while True:
                data = await reader.read(262144)
                if not data:
                    break
                buffer.extend(data)
                responses: list[bytes] = []
                while True:
                    newline = buffer.find(b"\n")
                    if newline < 0:
                        break
                    line = bytes(buffer[:newline]).rstrip(b"\r")
                    del buffer[: newline + 1]
                    if not line:
                        continue

                    compact_response = False
                    try:
                        if security:
                            security.validate_payload_size(line)
                        command = decode_command(line)
                        compact_response = bool(command.pop("__compact__", False))
                        api_key = command.get("api_key")
                        if security:
                            if command["cmd"] == "AUTH":
                                security.authenticate(client_ip, api_key)
                                response = {"ok": True, "result": "OK"}
                            else:
                                security.authenticate(client_ip, api_key)
                                security.validate_command(command["cmd"])
                                security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)
                                if api_key:
                                    key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
                                    security.check_rate_limit(key_hash, security.config.rate_limit_per_key)
                                response = self.dispatch(command, security, compact_response=compact_response)
                        else:
                            response = self.dispatch(command, None, compact_response=compact_response)
                    except ProtocolError as exc:
                        response = {"ok": False, "error": str(exc)}
                        compact_response = False
                    except SecurityError as exc:
                        response = {"ok": False, "error": f"security: {exc}"}
                        compact_response = False
                    except Exception as exc:
                        response = {"ok": False, "error": f"internal error: {exc}"}
                        compact_response = False

                    responses.append(encode_response(response, compact=compact_response))

                if responses:
                    writer.writelines(responses)
                    await writer.drain()
        finally:
            if security:
                security.unregister_connection(client_ip)
            writer.close()
            await writer.wait_closed()

    # ------------------------------------------------------------------
    # Inlined compact dispatch (hybrid path — bypasses decode_command + dispatch)
    # ------------------------------------------------------------------

    def _dispatch_compact_inlined(
        self,
        obj: list[Any],
        _get: Any,
        _put: Any,
        _incr: Any,
        _lock: Any,
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> bytes:
        """Inlined compact array dispatch — skips dict conversion for speed."""
        if not obj:
            return _json_dumps([0, "empty command"]) + b"\n"

        cmd = str(obj[0]).upper()

        if cmd == "PIPELINE":
            if len(obj) != 2 or not isinstance(obj[1], list):
                return _json_dumps([0, "PIPELINE requires [cmd, commands]"]) + b"\n"
            sub_commands = obj[1]
            results: list[Any] = []
            with _lock:
                for sub in sub_commands:
                    try:
                        results.append(
                            self._dispatch_compact_single_locked(sub, _get, _put, _incr, security)
                        )
                    except SecurityError as exc:
                        results.append({"error": f"security: {exc}"})
                    except Exception as exc:
                        results.append({"error": str(exc)})
            # Flatten results for compact wire
            flat: list[Any] = []
            for r in results:
                if isinstance(r, dict) and "error" in r:
                    flat.append(r)
                else:
                    flat.append(r)
            return _json_dumps([1, flat]) + b"\n"

        # Single command — lock + dispatch
        if security:
            try:
                security.validate_command(cmd)
                security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)
            except SecurityError as exc:
                return _json_dumps([0, f"security: {exc}"]) + b"\n"

        with _lock:
            try:
                result = self._dispatch_compact_single_locked(obj, _get, _put, _incr, security)
            except SecurityError as exc:
                return _json_dumps([0, f"security: {exc}"]) + b"\n"
            except Exception as exc:
                return _json_dumps([0, f"internal error: {exc}"]) + b"\n"

        if result == "OK":
            return _COMPACT_OK
        if result == "PONG":
            return _COMPACT_PONG
        return _json_dumps([1, result]) + b"\n"

    def _dispatch_compact_single_locked(
        self,
        obj: list[Any],
        _get: Any,
        _put: Any,
        _incr: Any,
        security: SecurityMiddleware | None,
    ) -> Any:
        """Dispatch a single compact command under the engine lock. Returns raw result."""
        cmd = str(obj[0]).upper()

        if cmd == "SET":
            key = obj[1]
            value = obj[2]
            if security:
                security.validate_key(key)
                security.validate_value_size(value)
            _put(key, value)
            return "OK"
        if cmd == "GET":
            key = obj[1]
            if security:
                security.validate_key(key)
            return _get(key)
        if cmd == "INCR":
            key = obj[1]
            amount = int(obj[2]) if len(obj) > 2 else 1
            if security:
                security.validate_key(key)
            return _incr(key, amount=amount)
        if cmd == "DEL":
            key = obj[1]
            if security:
                security.validate_key(key)
            deleted = self._engine._delete_locked(key)
            return 1 if deleted else 0
        if cmd == "PING":
            return "PONG"
        if cmd == "MGET":
            keys = obj[1] if isinstance(obj[1], list) else list(obj[1:])
            if security:
                for k in keys:
                    security.validate_key(k)
            return [_get(k) for k in keys]
        if cmd == "MSET":
            values = obj[1] if isinstance(obj[1], dict) else {}
            if security:
                for k, v in values.items():
                    security.validate_key(k)
                    security.validate_value_size(v)
            for k, v in values.items():
                _put(k, v)
            return "OK"
        if cmd == "MINCR":
            updates = obj[1] if isinstance(obj[1], list) else []
            result_map: dict[str, int] = {}
            for upd in updates:
                k = upd["key"]
                amt = int(upd.get("amount", 1))
                if security:
                    security.validate_key(k)
                result_map[k] = _incr(k, amount=amt)
            return result_map
        if cmd == "FLUSHALL":
            record = {"op": "flushall", "ts": time.time()}
            self._engine._append_record(record)
            self._engine._apply_record(record)
            return "OK"

        raise ProtocolError(f"unsupported compact command: {cmd}")

    def _dispatch_dict_command(
        self,
        obj: dict[str, Any],
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> bytes:
        """Handle dict-style command via the standard decode+dispatch path."""
        compact_response = False
        try:
            command = obj.copy()
            cmd_name = command.get("cmd", "")
            if isinstance(cmd_name, str) and not cmd_name.isupper():
                command["cmd"] = cmd_name.upper()

            api_key = command.get("api_key")
            if security:
                if command.get("cmd") == "AUTH":
                    security.authenticate(client_ip, api_key)
                    response = {"ok": True, "result": "OK"}
                else:
                    security.authenticate(client_ip, api_key)
                    security.validate_command(command["cmd"])
                    security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)
                    if api_key:
                        key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
                        security.check_rate_limit(key_hash, security.config.rate_limit_per_key)
                    response = self.dispatch(command, security, compact_response=compact_response)
            else:
                response = self.dispatch(command, None, compact_response=compact_response)
        except ProtocolError as exc:
            response = {"ok": False, "error": str(exc)}
        except SecurityError as exc:
            response = {"ok": False, "error": f"security: {exc}"}
        except Exception as exc:
            response = {"ok": False, "error": f"internal error: {exc}"}
        return encode_response(response, compact=compact_response)

    def _handle_resp_command(
        self,
        args: list[str],
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> bytes:
        if not args:
            return encode_error("empty command")
        cmd = str(args[0]).upper()
        try:
            if security:
                if cmd != "AUTH":
                    security.validate_command(cmd)
                    security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)

            if cmd == "PING":
                return encode_bulk_string(args[1]) if len(args) > 1 else RESP_PONG
            if cmd == "AUTH":
                return RESP_OK

            return self._dispatch_resp_fast(cmd, args, security)
        except (ProtocolError, ValueError, TypeError) as exc:
            return encode_error(str(exc))
        except SecurityError as exc:
            return encode_error(f"security: {exc}")
        except Exception as exc:
            return encode_error(f"internal error: {exc}")

    def _handle_resp_batch(
        self,
        commands: list[list[str]],
        security: SecurityMiddleware | None,
        client_ip: str,
    ) -> bytes:
        if all(_supports_locked_resp_batch(command) for command in commands):
            responses = bytearray()
            engine = self._engine
            _get = engine._get_locked
            _put = engine._put_locked
            _incr = engine._incr_locked
            _enc_bulk = encode_bulk_string
            _enc_int = encode_integer
            _enc_err = encode_error
            _stringify = _stringify_resp_value
            with engine._lock:
                for args in commands:
                    try:
                        cmd = args[0]
                        if cmd == "SET":
                            _put(args[1], args[2])
                            responses.extend(RESP_OK)
                        elif cmd == "GET":
                            responses.extend(_enc_bulk(_stringify(_get(args[1]))))
                        elif cmd == "INCR":
                            responses.extend(_enc_int(_incr(args[1], amount=1)))
                        elif cmd == "INCRBY":
                            responses.extend(_enc_int(_incr(args[1], amount=int(args[2]))))
                        else:
                            # Fallback for less common commands (MGET, MSET, MINCR, FLUSHALL)
                            cmd_str = str(cmd).upper()
                            if security:
                                security.validate_command(cmd_str)
                            responses.extend(self._dispatch_resp_locked(cmd_str, args, security))
                    except (ProtocolError, ValueError, TypeError) as exc:
                        responses.extend(_enc_err(str(exc)))
                    except SecurityError as exc:
                        responses.extend(_enc_err(f"security: {exc}"))
                    except Exception as exc:
                        responses.extend(_enc_err(f"internal error: {exc}"))
            return bytes(responses)

        responses = bytearray()
        for args in commands:
            responses.extend(self._handle_resp_command(args, security, client_ip))
        return bytes(responses)

    def _dispatch_resp_fast(
        self,
        cmd: str,
        args: list[str],
        security: SecurityMiddleware | None,
    ) -> bytes:
        if _supports_locked_resp_batch(args):
            with self._engine._lock:
                return self._dispatch_resp_locked(cmd, args, security)

        command = self._resp_to_command(args)
        response = self.dispatch(command, security)
        if not response.get("ok", False):
            return encode_error(str(response.get("error", "unknown error")))
        return self._encode_resp_result(cmd, response.get("result"))

    def _dispatch_resp_locked(
        self,
        cmd: str,
        args: list[str],
        security: SecurityMiddleware | None,
    ) -> bytes:
        engine = self._engine

        if cmd == "GET":
            if len(args) != 2:
                raise ProtocolError("GET requires key")
            key = args[1]
            if security:
                security.validate_key(key)
            return encode_bulk_string(_stringify_resp_value(engine._get_locked(key)))

        if cmd == "SET":
            if len(args) < 3:
                raise ProtocolError("SET requires key and value")
            key = args[1]
            value = args[2]
            if security:
                security.validate_key(key)
                security.validate_value_size(value)
            engine._put_locked(key, value)
            return RESP_OK

        if cmd == "INCR":
            if len(args) != 2:
                raise ProtocolError("INCR requires key")
            key = args[1]
            if security:
                security.validate_key(key)
            return encode_integer(engine._incr_locked(key, amount=1))

        if cmd == "INCRBY":
            if len(args) != 3:
                raise ProtocolError("INCRBY requires key and amount")
            key = args[1]
            amount = int(args[2])
            if security:
                security.validate_key(key)
            return encode_integer(engine._incr_locked(key, amount=amount))

        if cmd == "MGET":
            if len(args) < 2:
                raise ProtocolError("MGET requires at least one key")
            keys = list(args[1:])
            if security:
                for key in keys:
                    security.validate_key(key)
            return encode_array([_stringify_resp_value(engine._get_locked(key)) for key in keys])

        if cmd == "MSET":
            if len(args) < 3 or len(args[1:]) % 2 != 0:
                raise ProtocolError("MSET requires key/value pairs")
            for index in range(1, len(args), 2):
                key = args[index]
                value = args[index + 1]
                if security:
                    security.validate_key(key)
                    security.validate_value_size(value)
                engine._put_locked(key, value)
            return RESP_OK

        if cmd == "MINCR":
            if len(args) < 3 or len(args[1:]) % 2 != 0:
                raise ProtocolError("MINCR requires key/amount pairs")
            result: dict[str, int] = {}
            for index in range(1, len(args), 2):
                key = args[index]
                amount = int(args[index + 1])
                if security:
                    security.validate_key(key)
                result[key] = engine._incr_locked(key, amount=amount)
            ordered = [result[key] for key in sorted(result)]
            return encode_array(ordered)

        if cmd == "FLUSHALL":
            record = {"op": "flushall", "ts": asyncio.get_running_loop().time()}
            engine._append_record(record)
            engine._apply_record(record)
            return RESP_OK

        raise ProtocolError(f"unsupported locked RESP command: {cmd}")

    def _resp_to_command(self, args: list[str]) -> dict[str, Any]:
        cmd = str(args[0]).upper()
        if cmd == "GET":
            if len(args) != 2:
                raise ProtocolError("GET requires key")
            return {"cmd": "GET", "key": args[1]}
        if cmd == "SET":
            if len(args) < 3:
                raise ProtocolError("SET requires key and value")
            return {"cmd": "SET", "key": args[1], "value": args[2]}
        if cmd == "INCR":
            if len(args) != 2:
                raise ProtocolError("INCR requires key")
            return {"cmd": "INCR", "key": args[1], "amount": 1}
        if cmd == "INCRBY":
            if len(args) != 3:
                raise ProtocolError("INCRBY requires key and amount")
            return {"cmd": "INCR", "key": args[1], "amount": int(args[2])}
        if cmd == "MGET":
            if len(args) < 2:
                raise ProtocolError("MGET requires at least one key")
            return {"cmd": "MGET", "keys": list(args[1:])}
        if cmd == "MSET":
            if len(args) < 3 or len(args[1:]) % 2 != 0:
                raise ProtocolError("MSET requires key/value pairs")
            values = {args[index]: args[index + 1] for index in range(1, len(args), 2)}
            return {"cmd": "MSET", "values": values}
        if cmd == "MINCR":
            if len(args) < 3 or len(args[1:]) % 2 != 0:
                raise ProtocolError("MINCR requires key/amount pairs")
            updates = [
                {"key": args[index], "amount": int(args[index + 1])}
                for index in range(1, len(args), 2)
            ]
            return {"cmd": "MINCR", "updates": updates}
        if cmd == "FLUSHALL":
            return {"cmd": "FLUSHALL"}
        raise ProtocolError(f"unsupported RESP command: {cmd}")

    def _encode_resp_result(self, cmd: str, result: Any) -> bytes:
        if cmd in {"SET", "MSET", "FLUSHALL", "AUTH"}:
            return RESP_OK
        if cmd in {"GET", "PING"}:
            return encode_bulk_string(_stringify_resp_value(result))
        if cmd in {"INCR", "INCRBY"}:
            return encode_integer(int(result))
        if cmd == "MGET":
            return encode_array([_stringify_resp_value(item) for item in (result or [])])
        if cmd == "MINCR":
            if isinstance(result, dict):
                ordered = [result[key] for key in sorted(result)]
                return encode_array([int(item) for item in ordered])
            return encode_integer(0)
        return encode_bulk_string(_stringify_resp_value(result))

    async def _serve_async(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self._host,
            self._port,
            reuse_address=True,
        )
        socket_info = self._server.sockets[0].getsockname() if self._server.sockets else (self._host, self._port)
        self._bound_address = (str(socket_info[0]), int(socket_info[1]))
        self._serving = True
        try:
            async with self._server:
                try:
                    await self._server.serve_forever()
                except asyncio.CancelledError:
                    pass
        finally:
            self._serving = False

    async def _shutdown_async(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def _finalize_shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._engine.close()
        if self._security:
            self._security.close()

    def serve_forever(self) -> None:
        self._loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._serve_async())
        finally:
            self._finalize_shutdown()
            self._loop.close()
            self._loop = None

    def shutdown(self) -> None:
        if self._closed:
            return
        if self._serving and self._loop is not None and self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._shutdown_async(), self._loop)
            future.result(timeout=5)
            return
        self._finalize_shutdown()


def _stringify_resp_value(value: Any) -> str | bytes | None:
    if value is None or isinstance(value, (str, bytes)):
        return value
    return str(value)


def _supports_locked_resp_batch(args: list[str]) -> bool:
    if not args:
        return False
    return str(args[0]).upper() in {"GET", "SET", "INCR", "INCRBY", "MGET", "MSET", "MINCR", "FLUSHALL"}