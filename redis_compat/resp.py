"""
RESP (Redis Serialization Protocol) parser and encoder.

Parser:  Incremental, position-based — safe for partial TCP reads.
Encoder: Produces correct RESP bytes for all reply types.

Supported reply types
---------------------
  Simple string  +OK\r\n
  Error          -ERR message\r\n
  Integer        :42\r\n
  Bulk string    $6\r\nfoobar\r\n   (null: $-1\r\n)
  Array          *2\r\n...          (null: *-1\r\n)
"""

from __future__ import annotations

from typing import Any, Optional


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class RespProtocolError(ValueError):
    """Raised for malformed RESP data (non-recoverable)."""


class RespIncomplete(Exception):
    """Raised when the buffer contains a partial command — retry after more data."""


# ---------------------------------------------------------------------------
# Encoder helpers
# ---------------------------------------------------------------------------

# Pre-cache small integer encodings to avoid repeated formatting
_INT_CACHE: dict[int, bytes] = {}
for _i in range(-2, 1025):
    _INT_CACHE[_i] = f":{_i}\r\n".encode()


def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_error(msg: str) -> bytes:
    # Emit as a typed error, NOT '-ERR -ERR ...'; if msg already starts with
    # a known type prefix (WRONGTYPE, MOVED, …) keep it; otherwise prefix ERR.
    if not any(msg.startswith(p) for p in ("ERR ", "WRONGTYPE ", "MOVED ", "NOSCRIPT ")):
        msg = "ERR " + msg
    return f"-{msg}\r\n".encode()


def encode_integer(n: int) -> bytes:
    cached = _INT_CACHE.get(n)
    if cached is not None:
        return cached
    return f":{n}\r\n".encode()


# Pre-cache small bulk-string length prefixes
_BULK_PREFIX_CACHE: dict[int, bytes] = {}
for _i in range(0, 256):
    _BULK_PREFIX_CACHE[_i] = f"${_i}\r\n".encode()


def encode_bulk_string(s: Optional[str | bytes]) -> bytes:
    if s is None:
        return b"$-1\r\n"
    if isinstance(s, str):
        s = s.encode("utf-8")
    prefix = _BULK_PREFIX_CACHE.get(len(s))
    if prefix is not None:
        return prefix + s + b"\r\n"
    return b"$" + str(len(s)).encode() + b"\r\n" + s + b"\r\n"


# Pre-cache small array length prefixes
_ARRAY_PREFIX_CACHE: dict[int, bytes] = {}
for _i in range(0, 64):
    _ARRAY_PREFIX_CACHE[_i] = f"*{_i}\r\n".encode()


def encode_array(items: Optional[list[Any]]) -> bytes:
    if items is None:
        return b"*-1\r\n"
    out = bytearray()
    prefix = _ARRAY_PREFIX_CACHE.get(len(items))
    if prefix is not None:
        out.extend(prefix)
    else:
        out.extend(b"*")
        out.extend(str(len(items)).encode())
        out.extend(b"\r\n")
    for item in items:
        out.extend(_encode_value(item))
    return bytes(out)


def _encode_value(v: Any) -> bytes:
    if v is None:
        return b"$-1\r\n"
    if isinstance(v, bool):
        # bool before int — Python bool is a subclass of int
        return encode_integer(1 if v else 0)
    if isinstance(v, int):
        return encode_integer(v)
    if isinstance(v, (str, bytes)):
        return encode_bulk_string(v)
    if isinstance(v, list):
        return encode_array(v)
    if isinstance(v, dict):
        # Encode dicts as flat key-value arrays (HGETALL style)
        out = bytearray()
        out.extend(b"*")
        out.extend(str(len(v) * 2).encode())
        out.extend(b"\r\n")
        for k, val in v.items():
            out.extend(encode_bulk_string(str(k)))
            out.extend(encode_bulk_string(str(val) if val is not None else None))
        return bytes(out)
    return encode_bulk_string(str(v))


# ---------------------------------------------------------------------------
# Pre-built common responses (avoid repeated allocations in hot path)
# ---------------------------------------------------------------------------

RESP_OK    = encode_simple_string("OK")
RESP_PONG  = encode_simple_string("PONG")
RESP_RESET = encode_simple_string("RESET")
RESP_QUEUED = encode_simple_string("QUEUED")
RESP_NULL_BULK  = b"$-1\r\n"
RESP_NULL_ARRAY = b"*-1\r\n"
RESP_ZERO  = encode_integer(0)
RESP_ONE   = encode_integer(1)
RESP_MINUS_ONE = encode_integer(-1)
RESP_MINUS_TWO = encode_integer(-2)

# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class RespParser:
    """
    Incremental RESP command parser.

    Feed raw bytes from the socket with ``feed()``.
    Call ``get_command()`` repeatedly until it returns ``None``; each
    call returns one complete command as a list of strings (or None on
    partial data).

    Handles both multi-bulk commands (*N\\r\\n...) and inline commands
    (PING\\r\\n, COMMAND\\r\\n).
    """

    __slots__ = ("_buf", "_pos")

    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0  # Logical read offset into _buf

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def feed(self, data: bytes) -> None:
        """Append newly received bytes to the internal buffer."""
        # Compact consumed prefix to keep memory bounded.
        if self._pos > 4096 and self._pos > len(self._buf) // 2:
            del self._buf[: self._pos]
            self._pos = 0
        self._buf.extend(data)

    def get_command(self) -> Optional[list[str]]:
        """
        Return the next fully-received command as a list of strings, or
        ``None`` if the buffer does not yet contain a complete command.

        Raises ``RespProtocolError`` on unrecoverable parse errors.
        """
        if self._pos >= len(self._buf):
            return None
        saved = self._pos
        try:
            if self._buf[self._pos : self._pos + 1] == b"*":
                result = self._read_multibulk()
            else:
                result = self._read_inline()
            return result
        except RespIncomplete:
            self._pos = saved
            return None

    # ------------------------------------------------------------------
    # Internal readers — raise RespIncomplete on partial data
    # ------------------------------------------------------------------

    def _read_line(self) -> str:
        """Read up to (and consume) the next CRLF; return the line without CRLF."""
        idx = self._buf.find(b"\r\n", self._pos)
        if idx == -1:
            raise RespIncomplete
        line = self._buf[self._pos : idx].decode("utf-8", errors="replace")
        self._pos = idx + 2
        return line

    def _read_inline(self) -> list[str]:
        """Parse an inline command (space-separated tokens on one line)."""
        line = self._read_line()
        parts = line.strip().split()
        return parts if parts else self.get_command()  # type: ignore[return-value]

    def _read_multibulk(self) -> list[str]:
        """Parse a *N multi-bulk command."""
        line = self._read_line()
        if not line.startswith("*"):
            raise RespProtocolError(f"Expected '*', got {line!r}")
        count = int(line[1:])
        if count <= 0:
            return []
        result: list[str] = []
        for _ in range(count):
            result.append(self._read_bulk_string())
        return result

    def _read_bulk_string(self) -> str:
        """Parse a $N bulk string; return as str (None for null bulk strings)."""
        line = self._read_line()
        if not line.startswith("$"):
            raise RespProtocolError(f"Expected '$', got {line!r}")
        length = int(line[1:])
        if length == -1:
            return None  # type: ignore[return-value]
        end = self._pos + length + 2  # +2 for trailing CRLF
        if end > len(self._buf):
            raise RespIncomplete
        value = self._buf[self._pos : self._pos + length].decode("utf-8", errors="replace")
        self._pos = end
        return value
