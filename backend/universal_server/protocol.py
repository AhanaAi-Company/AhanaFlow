from __future__ import annotations

import json
from typing import Any

try:
    import orjson as _orjson
    _JSON_ERRORS = (UnicodeDecodeError, json.JSONDecodeError, _orjson.JSONDecodeError)

    def _json_loads(data: bytes) -> Any:
        return _orjson.loads(data)

    def _json_dumps(obj: dict[str, Any]) -> bytes:
        return _orjson.dumps(obj)
except ModuleNotFoundError:  # pragma: no cover
    def _json_loads(data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    def _json_dumps(obj: dict[str, Any]) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    _JSON_ERRORS = (UnicodeDecodeError, json.JSONDecodeError)


class ProtocolError(ValueError):
    pass


_OK_RESPONSE = b'{"ok":true,"result":"OK"}\n'
_PONG_RESPONSE = b'{"ok":true,"result":"PONG"}\n'


def _decode_compact_command(obj: list[Any]) -> dict[str, Any]:
    if not obj:
        raise ProtocolError("empty compact command")
    cmd = obj[0]
    if not isinstance(cmd, str) or not cmd:
        raise ProtocolError("compact cmd must start with a non-empty string")
    if not cmd.isupper():
        cmd = cmd.upper()

    command: dict[str, Any] = {"cmd": cmd, "__compact__": True}
    if cmd in {"PING", "AUTH"}:
        if len(obj) > 1:
            command["api_key"] = obj[1]
        return command
    if cmd == "GET":
        if len(obj) != 2:
            raise ProtocolError("compact GET must be [cmd, key]")
        command["key"] = obj[1]
        return command
    if cmd == "SET":
        if len(obj) != 3:
            raise ProtocolError("compact SET must be [cmd, key, value]")
        command["key"] = obj[1]
        command["value"] = obj[2]
        return command
    if cmd == "INCR":
        if len(obj) not in {2, 3}:
            raise ProtocolError("compact INCR must be [cmd, key] or [cmd, key, amount]")
        command["key"] = obj[1]
        if len(obj) == 3:
            command["amount"] = obj[2]
        return command
    if cmd == "MGET":
        if len(obj) != 2:
            raise ProtocolError("compact MGET must be [cmd, keys]")
        command["keys"] = obj[1]
        return command
    if cmd == "MSET":
        if len(obj) != 2:
            raise ProtocolError("compact MSET must be [cmd, values]")
        command["values"] = obj[1]
        return command
    if cmd == "MINCR":
        if len(obj) != 2:
            raise ProtocolError("compact MINCR must be [cmd, updates]")
        command["updates"] = obj[1]
        return command
    if cmd == "PIPELINE":
        if len(obj) != 2 or not isinstance(obj[1], list):
            raise ProtocolError("compact PIPELINE must be [cmd, commands]")
        command["commands"] = [
            _decode_compact_command(item) if isinstance(item, list) else item
            for item in obj[1]
        ]
        return command
    raise ProtocolError(f"unsupported compact command: {cmd!r}")


def decode_command(line: bytes) -> dict[str, Any]:
    """Decode a newline-delimited JSON command payload."""
    if not line:
        raise ProtocolError("empty command")
    try:
        obj = _json_loads(line)
    except _JSON_ERRORS as exc:
        raise ProtocolError(f"invalid json: {exc}") from exc
    if isinstance(obj, list):
        return _decode_compact_command(obj)
    if not isinstance(obj, dict):
        raise ProtocolError("command must be a JSON object")
    if "cmd" not in obj:
        raise ProtocolError("missing cmd")
    cmd = obj["cmd"]
    if not isinstance(cmd, str) or not cmd:
        raise ProtocolError("cmd must be a non-empty string")
    if not cmd.isupper():
        cmd = cmd.upper()
    obj["cmd"] = cmd
    return obj


def encode_response(payload: dict[str, Any], *, compact: bool = False) -> bytes:
    """Encode a response payload as newline-delimited JSON."""
    if payload == {"ok": True, "result": "OK"}:
        return _OK_RESPONSE
    if payload == {"ok": True, "result": "PONG"}:
        return _PONG_RESPONSE
    if compact:
        if payload.get("ok"):
            result = payload.get("result")
            if isinstance(result, list) and result and all(isinstance(item, dict) and "ok" in item for item in result):
                result = [item.get("result") if item.get("ok") else {"error": item.get("error")} for item in result]
            return _json_dumps([1, result]) + b"\n"
        return _json_dumps([0, payload.get("error", "unknown error")]) + b"\n"
    return _json_dumps(payload) + b"\n"
