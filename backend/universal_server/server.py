from __future__ import annotations

import hashlib
import socketserver
from pathlib import Path
from typing import Any

from backend.state_engine import CompressedStateEngine

from .protocol import ProtocolError, decode_command, encode_response
from .security import SecurityConfig, SecurityError, SecurityMiddleware


class _UniversalHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        # Extract client IP
        client_ip = self.client_address[0]
        security: SecurityMiddleware | None = getattr(self.server, "_security", None)  # type: ignore[attr-defined]

        # Register connection
        if security:
            try:
                security.check_connection_limit(client_ip)
                security.register_connection(client_ip)
            except SecurityError as exc:
                response = {"ok": False, "error": f"security: {exc}"}
                self.wfile.write(encode_response(response))
                return

        try:
            decode = decode_command
            dispatch = self.server.dispatch  # type: ignore[attr-defined]
            write_response = self.wfile.write
            while True:
                line = self.rfile.readline()
                if not line:
                    break
                if line in {b"\n", b"\r\n"}:
                    continue
                line = line.rstrip(b"\r\n")
                if not line:
                    continue

                try:
                    # Security: Check payload size
                    if security:
                        security.validate_payload_size(line)

                    command = decode(line)
                    compact_response = bool(command.pop("__compact__", False))

                    # Security: Authenticate (if AUTH command or key provided)
                    api_key = command.get("api_key")
                    if security:
                        if command["cmd"] == "AUTH":
                            # AUTH command - validate and respond
                            security.authenticate(client_ip, api_key)
                            response = {"ok": True, "result": "OK"}
                        else:
                            # Regular command - check auth + rate limit + validation
                            security.authenticate(client_ip, api_key)
                            security.validate_command(command["cmd"])

                            # Rate limiting (per-IP and per-key)
                            security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)
                            if api_key:
                                key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
                                security.check_rate_limit(key_hash, security.config.rate_limit_per_key)

                            response = dispatch(command, security, compact_response=compact_response)
                    else:
                        response = dispatch(command, None, compact_response=compact_response)

                except ProtocolError as exc:
                    response = {"ok": False, "error": str(exc)}
                    compact_response = False
                except SecurityError as exc:
                    response = {"ok": False, "error": f"security: {exc}"}
                    compact_response = False
                except Exception as exc:
                    response = {"ok": False, "error": f"internal error: {exc}"}
                    compact_response = False

                write_response(encode_response(response, compact=compact_response))
        finally:
            # Unregister connection
            if security:
                security.unregister_connection(client_ip)


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class UniversalStateServer:
    """Threaded TCP server exposing the compressed state engine.

    Protocol: newline-delimited JSON (or compact array wire format)
    Example: {"cmd": "SET", "key": "k", "value": {"v": 1}}
    """

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
        self._srv = _ThreadedTCPServer((host, port), _UniversalHandler)
        self._srv.dispatch = self.dispatch  # type: ignore[attr-defined]
        self._srv._security = self._security  # type: ignore[attr-defined]
        self._serving = False
        self._closed = False

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._srv.server_address
        return str(host), int(port)

    def serve_forever(self) -> None:
        self._serving = True
        try:
            self._srv.serve_forever()
        finally:
            self._serving = False

    def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._serving:
            self._srv.shutdown()
        self._srv.server_close()
        self._engine.close()
        if self._security:
            self._security.close()

    def dispatch(
        self,
        command: dict[str, Any],
        security: SecurityMiddleware | None = None,
        *,
        compact_response: bool = False,
    ) -> dict[str, Any]:
        cmd = command["cmd"]

        if cmd == "PING":
            return {"ok": True, "result": "PONG"}

        if cmd == "AUTH":
            # AUTH is handled in the handler, but return OK if we get here
            return {"ok": True, "result": "OK"}

        if cmd == "PIPELINE":
            commands = command.get("commands")
            if not isinstance(commands, list) or not commands:
                raise ProtocolError("commands must be a non-empty array")
            # Acquire the engine lock ONCE for the whole pipeline so all
            # sub-commands execute atomically — no interleaving from other clients.
            with self._engine._lock:
                results = self._dispatch_pipeline_locked(commands, security, compact_response=compact_response)
            return {"ok": True, "result": results}

        return self._dispatch_single(command, security)

    def _dispatch_pipeline_locked(
        self,
        commands: list[dict[str, Any]],
        security: SecurityMiddleware | None = None,
        *,
        compact_response: bool = False,
    ) -> list[Any]:
        results: list[Any] = []
        engine = self._engine

        for subcommand in commands:
            if not isinstance(subcommand, dict):
                raise ProtocolError("pipeline commands must be objects")
            cmd = subcommand.get("cmd")
            if not isinstance(cmd, str) or not cmd:
                raise ProtocolError("pipeline command missing cmd")
            if not cmd.isupper():
                cmd = cmd.upper()
            if cmd == "PIPELINE":
                raise ProtocolError("nested PIPELINE commands are not supported")

            if cmd == "SET":
                key = _require_str(subcommand, "key")
                value = subcommand.get("value")
                ttl = subcommand.get("ttl_seconds")
                if ttl is not None:
                    ttl = int(ttl)
                if security:
                    security.validate_key(key)
                    security.validate_value_size(value)
                engine._put_locked(key, value, ttl_seconds=ttl)
                results.append("OK" if compact_response else {"ok": True, "result": "OK"})
                continue

            if cmd == "GET":
                key = _require_str(subcommand, "key")
                if security:
                    security.validate_key(key)
                value = engine._get_locked(key)
                results.append(value if compact_response else {"ok": True, "result": value})
                continue

            if cmd == "DEL":
                key = _require_str(subcommand, "key")
                if security:
                    security.validate_key(key)
                deleted = engine._delete_locked(key)
                result = 1 if deleted else 0
                results.append(result if compact_response else {"ok": True, "result": result})
                continue

            if cmd == "INCR":
                key = _require_str(subcommand, "key")
                amount = int(subcommand.get("amount", 1))
                ttl = subcommand.get("ttl_seconds")
                if ttl is not None:
                    ttl = int(ttl)
                if security:
                    security.validate_key(key)
                value = engine._incr_locked(key, amount=amount, ttl_seconds=ttl)
                results.append(value if compact_response else {"ok": True, "result": value})
                continue

            if cmd == "MSET":
                values = subcommand.get("values")
                if not isinstance(values, dict) or not values:
                    raise ProtocolError("values must be a non-empty object")
                ttl = subcommand.get("ttl_seconds")
                if ttl is not None:
                    ttl = int(ttl)
                if security:
                    for key, value in values.items():
                        if not isinstance(key, str) or not key:
                            raise ProtocolError("MSET keys must be non-empty strings")
                        security.validate_key(key)
                        security.validate_value_size(value)
                for key, value in values.items():
                    engine._put_locked(str(key), value, ttl_seconds=ttl)
                result = len(values)
                results.append(result if compact_response else {"ok": True, "result": result})
                continue

            if cmd == "MGET":
                keys_arg = subcommand.get("keys")
                if not isinstance(keys_arg, list):
                    raise ProtocolError("keys must be an array")
                if security:
                    for key in keys_arg:
                        if not isinstance(key, str):
                            raise ProtocolError("keys must be strings")
                        security.validate_key(key)
                values = [engine._get_locked(key) for key in keys_arg]
                results.append(values if compact_response else {"ok": True, "result": values})
                continue

            if cmd == "MINCR":
                updates = subcommand.get("updates")
                if not isinstance(updates, list) or not updates:
                    raise ProtocolError("updates must be a non-empty array")
                ttl = subcommand.get("ttl_seconds")
                if ttl is not None:
                    ttl = int(ttl)
                normalized_updates: list[dict[str, int]] = []
                for update in updates:
                    if not isinstance(update, dict):
                        raise ProtocolError("MINCR updates must be objects")
                    key = _require_str(update, "key")
                    amount = int(update.get("amount", 1))
                    if security:
                        security.validate_key(key)
                    normalized_updates.append({"key": key, "amount": amount})
                result = {
                    item["key"]: engine._incr_locked(item["key"], amount=item["amount"], ttl_seconds=ttl)
                    for item in normalized_updates
                }
                results.append(result if compact_response else {"ok": True, "result": result})
                continue

            nested = dict(subcommand)
            nested["cmd"] = cmd
            results.append(self._dispatch_single(nested, security))

        return results

    def _dispatch_single(
        self,
        command: dict[str, Any],
        security: SecurityMiddleware | None = None,
    ) -> dict[str, Any]:
        cmd = command["cmd"]

        if cmd == "SET":
            key = _require_str(command, "key")
            value = command.get("value")
            ttl = command.get("ttl_seconds")
            if ttl is not None:
                ttl = int(ttl)

            # Security: Validate key and value
            if security:
                security.validate_key(key)
                security.validate_value_size(value)

            self._engine.put(key, value, ttl_seconds=ttl)
            return {"ok": True, "result": "OK"}

        if cmd == "MSET":
            values = command.get("values")
            if not isinstance(values, dict) or not values:
                raise ProtocolError("values must be a non-empty object")
            ttl = command.get("ttl_seconds")
            if ttl is not None:
                ttl = int(ttl)
            if security:
                for key, value in values.items():
                    if not isinstance(key, str) or not key:
                        raise ProtocolError("MSET keys must be non-empty strings")
                    security.validate_key(key)
                    security.validate_value_size(value)
            count = self._engine.mset(values, ttl_seconds=ttl)
            return {"ok": True, "result": count}

        if cmd == "GET":
            key = _require_str(command, "key")
            if security:
                security.validate_key(key)
            return {"ok": True, "result": self._engine.get(key)}

        if cmd == "DEL":
            key = _require_str(command, "key")
            if security:
                security.validate_key(key)
            deleted = self._engine.delete(key)
            return {"ok": True, "result": 1 if deleted else 0}

        if cmd == "INCR":
            key = _require_str(command, "key")
            amount = int(command.get("amount", 1))
            ttl = command.get("ttl_seconds")
            if ttl is not None:
                ttl = int(ttl)
            if security:
                security.validate_key(key)
            value = self._engine.incr(key, amount=amount, ttl_seconds=ttl)
            return {"ok": True, "result": value}

        if cmd == "MINCR":
            updates = command.get("updates")
            if not isinstance(updates, list) or not updates:
                raise ProtocolError("updates must be a non-empty array")
            ttl = command.get("ttl_seconds")
            if ttl is not None:
                ttl = int(ttl)
            normalized_updates: list[dict[str, int]] = []
            for update in updates:
                if not isinstance(update, dict):
                    raise ProtocolError("MINCR updates must be objects")
                key = _require_str(update, "key")
                amount = int(update.get("amount", 1))
                if security:
                    security.validate_key(key)
                normalized_updates.append({"key": key, "amount": amount})
            return {"ok": True, "result": self._engine.mincr(normalized_updates, ttl_seconds=ttl)}

        if cmd == "ENQUEUE":
            queue = _require_str(command, "queue")
            payload = command.get("payload")
            if not isinstance(payload, dict):
                raise ProtocolError("payload must be an object")
            size = self._engine.enqueue(queue, payload)
            return {"ok": True, "result": size}

        if cmd == "DEQUEUE":
            queue = _require_str(command, "queue")
            return {"ok": True, "result": self._engine.dequeue(queue)}

        if cmd == "QLEN":
            queue = _require_str(command, "queue")
            return {"ok": True, "result": self._engine.queue_length(queue)}

        if cmd == "XADD":
            stream = _require_str(command, "stream")
            event = command.get("event")
            if not isinstance(event, dict):
                raise ProtocolError("event must be an object")
            seq = self._engine.append_event(stream, event)
            return {"ok": True, "result": seq}

        if cmd == "XRANGE":
            stream = _require_str(command, "stream")
            after_seq = int(command.get("after_seq", 0))
            limit = int(command.get("limit", 100))
            return {
                "ok": True,
                "result": self._engine.read_events(stream, after_seq=after_seq, limit=limit),
            }

        if cmd == "EXISTS":
            key = _require_str(command, "key")
            return {"ok": True, "result": 1 if self._engine.exists(key) else 0}

        if cmd == "KEYS":
            prefix = command.get("prefix", "")
            if not isinstance(prefix, str):
                raise ProtocolError("prefix must be a string")
            return {"ok": True, "result": self._engine.keys(prefix=prefix)}

        if cmd == "TTL":
            key = _require_str(command, "key")
            return {"ok": True, "result": self._engine.ttl(key)}

        if cmd == "MGET":
            keys_arg = command.get("keys")
            if not isinstance(keys_arg, list):
                raise ProtocolError("keys must be an array")
            return {"ok": True, "result": self._engine.mget(keys_arg)}

        if cmd == "FLUSHALL":
            self._engine.flushall()
            return {"ok": True, "result": "OK"}

        if cmd == "STATS":
            stats = self._engine.stats()
            return {
                "ok": True,
                "result": {
                    "keys": stats.keys,
                    "queues": stats.queues,
                    "streams": stats.streams,
                    "records_replayed": stats.records_replayed,
                    "wal_size_bytes": stats.wal_size_bytes,
                    "compressed_bytes_written": stats.compressed_bytes_written,
                    "uncompressed_bytes_written": stats.uncompressed_bytes_written,
                    "compression_ratio": stats.compression_ratio,
                },
            }

        if cmd == "CONFIG":
            action = str(command.get("action", "")).upper()
            key = str(command.get("key", ""))
            if action == "GET":
                if key == "durability_mode":
                    return {"ok": True, "result": self._engine.durability_mode}
                raise ProtocolError(f"unknown CONFIG key: {key!r}")
            if action == "SET":
                if key == "durability_mode":
                    value = command.get("value")
                    if not isinstance(value, str):
                        raise ProtocolError("CONFIG SET durability_mode: value must be a string")
                    self._engine.set_durability_mode(value)
                    return {"ok": True, "result": "OK"}
                raise ProtocolError(f"unknown CONFIG key: {key!r}")
            raise ProtocolError("CONFIG action must be 'GET' or 'SET'")

        raise ProtocolError(f"unknown command: {cmd}")


def _require_str(command: dict[str, Any], field: str) -> str:
    value = command.get(field)
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{field} must be a non-empty string")
    return value
