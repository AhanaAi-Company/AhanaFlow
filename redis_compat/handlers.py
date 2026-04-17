"""
Redis command handlers — dispatches incoming RESP commands to USS.

Coverage
--------
  Strings / KV   GET SET SETNX SETEX PSETEX GETSET GETDEL MGET MSET MSETNX
                 DEL UNLINK EXISTS EXPIRE PEXPIRE TTL PTTL PERSIST TYPE
                 RENAME KEYS SCAN APPEND STRLEN
  Numeric        INCR DECR INCRBY DECRBY INCRBYFLOAT
  List           LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX
  Hash           HSET HGET HGETALL HMSET HMGET HDEL HEXISTS HLEN
                 HKEYS HVALS HINCRBY HINCRBYFLOAT
  Stream         XADD XREAD XLEN XRANGE
  Admin          PING QUIT SELECT AUTH RESET INFO COMMAND CONFIG DBSIZE
                 FLUSHDB FLUSHALL OBJECT WAIT DEBUG OBJECT

Limitations
-----------
  • Lists and Hashes are stored as JSON blobs in the USS KV store.
    Multi-client LPUSH/RPUSH/HSET are NOT atomic — last-write-wins under
    true concurrency.  USS would need native list/hash support for
    true atomicity; this is acceptable for a compat layer.

  • KEYS pattern matching is limited: '*' = all keys with shared prefix
    (utilises USS KEYS prefix search).  Full glob not supported.

  • TTL is passed through to USS on SET/SETEX/EXPIRE.  PERSIST sets
    ttl=-1 (USS interprets as no-expiry re-set of the same value).

  • Binary-safe semantics require base64 for large binary values; this
    layer is UTF-8 transparent — suitable for text / JSON workloads.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

from .resp import (
    RESP_MINUS_ONE,
    RESP_MINUS_TWO,
    RESP_NULL_ARRAY,
    RESP_NULL_BULK,
    RESP_OK,
    RESP_ONE,
    RESP_PONG,
    RESP_RESET,
    RESP_ZERO,
    RespProtocolError,
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
)
from .uss_client import USSClient, USSError

log = logging.getLogger(__name__)

# Marker value stored inside list/hash blobs so _type() can identify them.
_LIST_TYPE = "list"
_HASH_TYPE = "hash"

_INFO_BLOB = (
    "# Server\r\n"
    "redis_version:7.0.0-ahanaflow-compat\r\n"
    "redis_mode:standalone\r\n"
    "os:Linux\r\n"
    "arch_bits:64\r\n"
    "tcp_port:6379\r\n"
    "\r\n"
    "# Clients\r\n"
    "connected_clients:1\r\n"
    "blocked_clients:0\r\n"
    "\r\n"
    "# Memory\r\n"
    "used_memory:1048576\r\n"
    "maxmemory:0\r\n"
    "maxmemory_policy:noeviction\r\n"
    "\r\n"
    "# Stats\r\n"
    "total_commands_processed:0\r\n"
    "\r\n"
    "# Keyspace\r\n"
    "db0:keys=0,expires=0,avg_ttl=0\r\n"
)


def _wrong_args(cmd: str) -> bytes:
    return encode_error(f"wrong number of arguments for '{cmd.lower()}' command")


def _unknown_cmd(name: str, args: list[str]) -> bytes:
    preview = " ".join(repr(a) for a in args[:3])
    return encode_error(f"unknown command '{name}', with args beginning with: {preview}")


class CommandHandler:
    """
    Stateless Redis command dispatcher, backed by a single USSClient.

    One CommandHandler is created per connected Redis client, sharing
    its own USS connection.
    """

    def __init__(self, uss: USSClient) -> None:
        self._uss = uss

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def handle(self, args: list[str]) -> bytes:
        if not args:
            return encode_error("empty command")
        cmd = args[0].upper()
        handler = _DISPATCH.get(cmd)
        if handler is None:
            return _unknown_cmd(args[0], args[1:])
        try:
            return await handler(self, args)
        except USSError as exc:
            log.error("USS error handling %s: %s", cmd, exc)
            return encode_error(f"backend unavailable: {exc}")
        except Exception as exc:
            log.exception("Unexpected error handling %s", cmd)
            return encode_error(str(exc))

    # ==================================================================
    # Connection / Admin
    # ==================================================================

    async def cmd_ping(self, args: list[str]) -> bytes:
        if len(args) > 1:
            return encode_bulk_string(args[1])
        return RESP_PONG

    async def cmd_quit(self, args: list[str]) -> bytes:
        return RESP_OK  # Caller closes connection after sending

    async def cmd_select(self, args: list[str]) -> bytes:
        # Single logical DB; ignore the index — always succeed
        return RESP_OK

    async def cmd_auth(self, args: list[str]) -> bytes:
        # Auth is enforced at USS level (API key); accept everything here
        return RESP_OK

    async def cmd_reset(self, args: list[str]) -> bytes:
        return RESP_RESET

    async def cmd_info(self, args: list[str]) -> bytes:
        return encode_bulk_string(_INFO_BLOB)

    async def cmd_command(self, args: list[str]) -> bytes:
        sub = args[1].upper() if len(args) > 1 else ""
        if sub == "COUNT":
            return encode_integer(len(_DISPATCH))
        # COMMAND DOCS / COMMAND INFO / COMMAND GETKEYS — return empty
        return encode_array([])

    async def cmd_config(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("CONFIG")
        sub = args[1].upper()
        if sub == "GET":
            param = (args[2] if len(args) > 2 else "*").lower()
            # Return commonly-checked params as empty strings
            if param in ("save", "appendonly", "hz", "maxmemory-policy"):
                return encode_array([param, ""])
            return encode_array([])
        if sub in ("SET", "RESETSTAT", "REWRITE"):
            return RESP_OK
        return RESP_OK

    async def cmd_dbsize(self, args: list[str]) -> bytes:
        try:
            keys = await self._uss.keys("")
            return encode_integer(len(keys))
        except USSError:
            return encode_integer(0)

    async def cmd_flushdb(self, args: list[str]) -> bytes:
        await self._uss.flushall()
        return RESP_OK

    async def cmd_flushall(self, args: list[str]) -> bytes:
        await self._uss.flushall()
        return RESP_OK

    async def cmd_object(self, args: list[str]) -> bytes:
        # OBJECT ENCODING / REFCOUNT / IDLETIME — return sane defaults
        if len(args) >= 2:
            sub = args[1].upper()
            if sub == "ENCODING":
                return encode_bulk_string("embstr")
            if sub in ("REFCOUNT", "IDLETIME", "FREQ"):
                return encode_integer(0)
            if sub == "HELP":
                return encode_array([
                    "OBJECT <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
                    "ENCODING <key>",
                    "REFCOUNT <key>",
                    "IDLETIME <key>",
                ])
        return RESP_NULL_BULK

    async def cmd_debug(self, args: list[str]) -> bytes:
        return RESP_OK

    async def cmd_wait(self, args: list[str]) -> bytes:
        return encode_integer(0)

    # ==================================================================
    # String / KV
    # ==================================================================

    async def cmd_get(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("GET")
        val = await self._uss.get(args[1])
        return encode_bulk_string(_to_str(val))

    async def cmd_set(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("SET")
        key, value = args[1], args[2]
        ttl: Optional[int] = None
        nx_flag = xx_flag = get_flag = False
        i = 3
        while i < len(args):
            opt = args[i].upper()
            if opt == "EX" and i + 1 < len(args):
                ttl = int(args[i + 1]); i += 2
            elif opt == "PX" and i + 1 < len(args):
                ttl = max(1, int(args[i + 1]) // 1000); i += 2
            elif opt == "EXAT" and i + 1 < len(args):
                ttl = max(1, int(args[i + 1]) - int(time.time())); i += 2
            elif opt == "PXAT" and i + 1 < len(args):
                ttl = max(1, int(args[i + 1]) // 1000 - int(time.time())); i += 2
            elif opt == "NX":
                nx_flag = True; i += 1
            elif opt == "XX":
                xx_flag = True; i += 1
            elif opt == "GET":
                get_flag = True; i += 1
            elif opt == "KEEPTTL":
                i += 1  # not supported by USS; ignore
            else:
                i += 1

        old_val: Optional[str] = None
        if nx_flag or xx_flag or get_flag:
            old_val = _to_str(await self._uss.get(key))

        if nx_flag and old_val is not None:
            return RESP_NULL_BULK if not get_flag else encode_bulk_string(old_val)
        if xx_flag and old_val is None:
            return RESP_NULL_BULK

        await self._uss.set(key, value, ttl_seconds=ttl)
        return encode_bulk_string(old_val) if get_flag else RESP_OK

    async def cmd_setnx(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("SETNX")
        if await self._uss.exists(args[1]):
            return RESP_ZERO
        await self._uss.set(args[1], args[2])
        return RESP_ONE

    async def cmd_setex(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("SETEX")
        await self._uss.set(args[1], args[3], ttl_seconds=int(args[2]))
        return RESP_OK

    async def cmd_psetex(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("PSETEX")
        await self._uss.set(args[1], args[3], ttl_seconds=max(1, int(args[2]) // 1000))
        return RESP_OK

    async def cmd_getset(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("GETSET")
        old = _to_str(await self._uss.get(args[1]))
        await self._uss.set(args[1], args[2])
        return encode_bulk_string(old)

    async def cmd_getdel(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("GETDEL")
        val = _to_str(await self._uss.get(args[1]))
        if val is not None:
            await self._uss.delete(args[1])
        return encode_bulk_string(val)

    async def cmd_del(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("DEL")
        count = 0
        for key in args[1:]:
            count += await self._uss.delete(key)
        return encode_integer(count)

    # UNLINK is DEL with async semantics — we handle synchronously
    cmd_unlink = cmd_del

    async def cmd_exists(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("EXISTS")
        # Redis allows EXISTS key [key ...] — count each occurrence
        count = 0
        for key in args[1:]:
            count += await self._uss.exists(key)
        return encode_integer(count)

    async def cmd_expire(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("EXPIRE")
        if not await self._uss.exists(args[1]):
            return RESP_ZERO
        val = await self._uss.get(args[1])
        await self._uss.set(args[1], val, ttl_seconds=int(args[2]))
        return RESP_ONE

    async def cmd_pexpire(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("PEXPIRE")
        if not await self._uss.exists(args[1]):
            return RESP_ZERO
        val = await self._uss.get(args[1])
        await self._uss.set(args[1], val, ttl_seconds=max(1, int(args[2]) // 1000))
        return RESP_ONE

    async def cmd_ttl(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("TTL")
        result = await self._uss.ttl(args[1])
        return encode_integer(result)

    async def cmd_pttl(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("PTTL")
        secs = await self._uss.ttl(args[1])
        return encode_integer(secs * 1000 if secs >= 0 else secs)

    async def cmd_persist(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("PERSIST")
        if not await self._uss.exists(args[1]):
            return RESP_ZERO
        val = await self._uss.get(args[1])
        # Re-set without TTL to remove expiry
        await self._uss.set(args[1], val)
        return RESP_ONE

    async def cmd_type(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("TYPE")
        val = await self._uss.get(args[1])
        if val is None:
            return encode_simple_string("none")
        if isinstance(val, dict):
            t = val.get("_ahana_type")
            if t == _LIST_TYPE:
                return encode_simple_string("list")
            if t == _HASH_TYPE:
                return encode_simple_string("hash")
            return encode_simple_string("string")
        return encode_simple_string("string")

    async def cmd_rename(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("RENAME")
        val = await self._uss.get(args[1])
        if val is None:
            return encode_error("ERR no such key")
        await self._uss.set(args[2], val)
        await self._uss.delete(args[1])
        return RESP_OK

    async def cmd_keys(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("KEYS")
        pattern = args[1]
        # Full glob matching via USS prefix search:
        #   '*'              → prefix=""  (all keys)
        #   'foo*'           → prefix="foo"
        #   'foo:*'          → prefix="foo:"
        #   exact / other    → prefix=pattern-literal-prefix
        # Deep glob patterns (? [] {}) are not yet supported — return subset.
        if pattern == "*":
            prefix = ""
        elif pattern.endswith("*") and "*" not in pattern[:-1] and "?" not in pattern:
            prefix = pattern[:-1]
        else:
            # Fallback: extract literal prefix up to first wildcard
            idx = min(
                (pattern.index(c) for c in ("*", "?", "[") if c in pattern),
                default=len(pattern),
            )
            prefix = pattern[:idx]
        keys = await self._uss.keys(prefix)
        return encode_array(keys)

    async def cmd_scan(self, args: list[str]) -> bytes:
        # SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
        # We always return cursor "0" (single-pass full scan)
        pattern = "*"
        i = 2
        while i < len(args):
            if args[i].upper() == "MATCH" and i + 1 < len(args):
                pattern = args[i + 1]; i += 2
            elif args[i].upper() in ("COUNT", "TYPE") and i + 1 < len(args):
                i += 2
            else:
                i += 1
        prefix = pattern.rstrip("*") if pattern.endswith("*") else ""
        keys = await self._uss.keys(prefix)
        return encode_array(["0", keys])

    async def cmd_append(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("APPEND")
        existing = _to_str(await self._uss.get(args[1])) or ""
        new_val = existing + args[2]
        await self._uss.set(args[1], new_val)
        return encode_integer(len(new_val.encode("utf-8")))

    async def cmd_strlen(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("STRLEN")
        val = _to_str(await self._uss.get(args[1])) or ""
        return encode_integer(len(val.encode("utf-8")))

    # ==================================================================
    # Multi-key
    # ==================================================================

    async def cmd_mget(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("MGET")
        results = await self._uss.mget(args[1:])
        return encode_array([_to_str(v) for v in results])

    async def cmd_mset(self, args: list[str]) -> bytes:
        if len(args) < 3 or len(args) % 2 == 0:
            return _wrong_args("MSET")
        values = {args[i]: args[i + 1] for i in range(1, len(args), 2)}
        await self._uss.mset(values)
        return RESP_OK

    async def cmd_msetnx(self, args: list[str]) -> bytes:
        if len(args) < 3 or len(args) % 2 == 0:
            return _wrong_args("MSETNX")
        keys = args[1::2]
        vals = args[2::2]
        existing = await self._uss.mget(keys)
        if any(v is not None for v in existing):
            return RESP_ZERO
        await self._uss.mset(dict(zip(keys, vals)))
        return RESP_ONE

    # ==================================================================
    # Numeric
    # ==================================================================

    async def _incr_key(self, key: str, amount: int) -> int:
        """
        Increment *key* by *amount*.  USS native INCR only works on keys
        it owns as integer type.  If the key was created by SET (string type)
        we fall back to a read-modify-write so the Redis contract
        ("treat the string value as integer") is honoured.
        """
        try:
            return await self._uss.incr(key, amount)
        except USSError:
            # Key may hold a string representation of an integer (SET "42" then INCR)
            raw = _to_str(await self._uss.get(key))
            if raw is None:
                raw = "0"
            try:
                new_val = int(raw) + amount
            except ValueError:
                raise RespProtocolError(
                    f"ERR value is not an integer or out of range"
                )
            await self._uss.set(key, str(new_val))
            return new_val

    async def cmd_incr(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("INCR")
        return encode_integer(await self._incr_key(args[1], 1))

    async def cmd_incrby(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("INCRBY")
        return encode_integer(await self._incr_key(args[1], int(args[2])))

    async def cmd_incrbyfloat(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("INCRBYFLOAT")
        raw = _to_str(await self._uss.get(args[1])) or "0"
        try:
            new_val = float(raw) + float(args[2])
        except ValueError:
            return encode_error("ERR value is not a valid float")
        formatted = f"{new_val:.17g}"
        await self._uss.set(args[1], formatted)
        return encode_bulk_string(formatted)

    async def cmd_decr(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("DECR")
        return encode_integer(await self._incr_key(args[1], -1))

    async def cmd_decrby(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("DECRBY")
        return encode_integer(await self._incr_key(args[1], -int(args[2])))

    # ==================================================================
    # List (stored as {"_ahana_type": "list", "items": [...]} in USS KV)
    # ==================================================================

    async def _list_get(self, key: str) -> list[str]:
        val = await self._uss.get(key)
        if val is None:
            return []
        if isinstance(val, dict) and val.get("_ahana_type") == _LIST_TYPE:
            return list(val.get("items", []))
        # Key exists but isn't a list
        raise RespProtocolError(
            f"WRONGTYPE Operation against a key holding the wrong kind of value"
        )

    async def _list_set(self, key: str, items: list[str]) -> None:
        await self._uss.set(key, {"_ahana_type": _LIST_TYPE, "items": items})

    async def cmd_lpush(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("LPUSH")
        items = await self._list_get(args[1])
        for val in args[2:]:
            items.insert(0, val)
        await self._list_set(args[1], items)
        return encode_integer(len(items))

    async def cmd_rpush(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("RPUSH")
        items = await self._list_get(args[1])
        items.extend(args[2:])
        await self._list_set(args[1], items)
        return encode_integer(len(items))

    async def cmd_lpushx(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("LPUSHX")
        if not await self._uss.exists(args[1]):
            return RESP_ZERO
        return await self.cmd_lpush(args)

    async def cmd_rpushx(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("RPUSHX")
        if not await self._uss.exists(args[1]):
            return RESP_ZERO
        return await self.cmd_rpush(args)

    async def cmd_lpop(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("LPOP")
        items = await self._list_get(args[1])
        count = int(args[2]) if len(args) > 2 else None
        if not items:
            return RESP_NULL_BULK if count is None else RESP_NULL_ARRAY
        if count is None:
            val = items.pop(0)
            await self._list_set(args[1], items)
            return encode_bulk_string(val)
        popped, remaining = items[:count], items[count:]
        await self._list_set(args[1], remaining)
        return encode_array(popped)

    async def cmd_rpop(self, args: list[str]) -> bytes:
        if len(args) < 2:
            return _wrong_args("RPOP")
        items = await self._list_get(args[1])
        count = int(args[2]) if len(args) > 2 else None
        if not items:
            return RESP_NULL_BULK if count is None else RESP_NULL_ARRAY
        if count is None:
            val = items.pop()
            await self._list_set(args[1], items)
            return encode_bulk_string(val)
        popped = list(reversed(items[-count:]))
        await self._list_set(args[1], items[:-count])
        return encode_array(popped)

    async def cmd_llen(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("LLEN")
        return encode_integer(len(await self._list_get(args[1])))

    async def cmd_lrange(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("LRANGE")
        items = await self._list_get(args[1])
        n = len(items)
        start, stop = int(args[2]), int(args[3])
        if start < 0:
            start = max(0, n + start)
        if stop < 0:
            stop = n + stop
        stop = min(stop, n - 1)
        if start > stop:
            return encode_array([])
        return encode_array(items[start : stop + 1])

    async def cmd_lindex(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("LINDEX")
        items = await self._list_get(args[1])
        idx = int(args[2])
        if idx < 0:
            idx = len(items) + idx
        if idx < 0 or idx >= len(items):
            return RESP_NULL_BULK
        return encode_bulk_string(items[idx])

    async def cmd_lset(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("LSET")
        items = await self._list_get(args[1])
        idx = int(args[2])
        if idx < 0:
            idx = len(items) + idx
        if idx < 0 or idx >= len(items):
            return encode_error("ERR index out of range")
        items[idx] = args[3]
        await self._list_set(args[1], items)
        return RESP_OK

    async def cmd_lrem(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("LREM")
        items = await self._list_get(args[1])
        count, element = int(args[2]), args[3]
        removed = 0
        if count == 0:
            new = [x for x in items if x != element]
            removed = len(items) - len(new)
        elif count > 0:
            new, removed = [], 0
            for x in items:
                if x == element and removed < count:
                    removed += 1
                else:
                    new.append(x)
        else:  # count < 0 — remove from tail
            new, removed = [], 0
            for x in reversed(items):
                if x == element and removed < -count:
                    removed += 1
                else:
                    new.insert(0, x)
        await self._list_set(args[1], new)
        return encode_integer(removed)

    # ==================================================================
    # Hash (stored as {"_ahana_type": "hash", "fields": {...}} in USS KV)
    # ==================================================================

    async def _hash_get(self, key: str) -> dict[str, str]:
        val = await self._uss.get(key)
        if val is None:
            return {}
        if isinstance(val, dict) and val.get("_ahana_type") == _HASH_TYPE:
            return dict(val.get("fields", {}))
        raise RespProtocolError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )

    async def _hash_set(self, key: str, fields: dict[str, str]) -> None:
        await self._uss.set(key, {"_ahana_type": _HASH_TYPE, "fields": fields})

    async def cmd_hset(self, args: list[str]) -> bytes:
        if len(args) < 4 or (len(args) - 2) % 2 != 0:
            return _wrong_args("HSET")
        h = await self._hash_get(args[1])
        added = 0
        for i in range(2, len(args), 2):
            if args[i] not in h:
                added += 1
            h[args[i]] = args[i + 1]
        await self._hash_set(args[1], h)
        return encode_integer(added)

    # HMSET is deprecated but still widely used
    async def cmd_hmset(self, args: list[str]) -> bytes:
        if len(args) < 4 or (len(args) - 2) % 2 != 0:
            return _wrong_args("HMSET")
        h = await self._hash_get(args[1])
        for i in range(2, len(args), 2):
            h[args[i]] = args[i + 1]
        await self._hash_set(args[1], h)
        return RESP_OK

    async def cmd_hget(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("HGET")
        h = await self._hash_get(args[1])
        return encode_bulk_string(h.get(args[2]))

    async def cmd_hgetall(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("HGETALL")
        h = await self._hash_get(args[1])
        flat: list[str] = []
        for k, v in h.items():
            flat.extend([k, v])
        return encode_array(flat)

    async def cmd_hmget(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("HMGET")
        h = await self._hash_get(args[1])
        return encode_array([h.get(f) for f in args[2:]])

    async def cmd_hdel(self, args: list[str]) -> bytes:
        if len(args) < 3:
            return _wrong_args("HDEL")
        h = await self._hash_get(args[1])
        count = sum(1 for f in args[2:] if h.pop(f, None) is not None)
        await self._hash_set(args[1], h)
        return encode_integer(count)

    async def cmd_hexists(self, args: list[str]) -> bytes:
        if len(args) != 3:
            return _wrong_args("HEXISTS")
        h = await self._hash_get(args[1])
        return RESP_ONE if args[2] in h else RESP_ZERO

    async def cmd_hlen(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("HLEN")
        return encode_integer(len(await self._hash_get(args[1])))

    async def cmd_hkeys(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("HKEYS")
        return encode_array(list((await self._hash_get(args[1])).keys()))

    async def cmd_hvals(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("HVALS")
        return encode_array(list((await self._hash_get(args[1])).values()))

    async def cmd_hincrby(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("HINCRBY")
        h = await self._hash_get(args[1])
        try:
            new_val = int(h.get(args[2], "0")) + int(args[3])
        except ValueError:
            return encode_error("ERR hash value is not an integer")
        h[args[2]] = str(new_val)
        await self._hash_set(args[1], h)
        return encode_integer(new_val)

    async def cmd_hincrbyfloat(self, args: list[str]) -> bytes:
        if len(args) != 4:
            return _wrong_args("HINCRBYFLOAT")
        h = await self._hash_get(args[1])
        try:
            new_val = float(h.get(args[2], "0")) + float(args[3])
        except ValueError:
            return encode_error("ERR hash value is not a float")
        formatted = f"{new_val:.17g}"
        h[args[2]] = formatted
        await self._hash_set(args[1], h)
        return encode_bulk_string(formatted)

    async def cmd_hscan(self, args: list[str]) -> bytes:
        # HSCAN key cursor [MATCH pattern] [COUNT count]
        if len(args) < 3:
            return _wrong_args("HSCAN")
        h = await self._hash_get(args[1])
        flat: list[str] = []
        for k, v in h.items():
            flat.extend([k, v])
        return encode_array(["0", flat])

    # ==================================================================
    # Streams (maps to USS XADD / XRANGE)
    # ==================================================================

    async def cmd_xadd(self, args: list[str]) -> bytes:
        """XADD key [NOMKSTREAM] [MAXLEN [~|=] threshold] *|id field value ..."""
        if len(args) < 5:
            return _wrong_args("XADD")
        key = args[1]
        i = 2
        # Skip optional NOMKSTREAM / MAXLEN / MINID flags
        while i < len(args) and args[i].upper() in ("NOMKSTREAM", "MAXLEN", "MINID", "~", "="):
            flag = args[i].upper()
            i += 1
            if flag in ("MAXLEN", "MINID"):
                # May be followed by ~|= then threshold, or just threshold
                while i < len(args) and args[i] in ("~", "="):
                    i += 1
                if i < len(args):
                    i += 1  # skip threshold value
        if i >= len(args):
            return _wrong_args("XADD")
        _msg_id = args[i]; i += 1  # * or explicit id — USS auto-assigns seq
        if (len(args) - i) % 2 != 0:
            return encode_error("ERR unbalanced list of fields and values")
        event: dict[str, str] = {}
        while i < len(args) - 1:
            event[args[i]] = args[i + 1]; i += 2
        ts_ms = int(time.time() * 1000)
        seq = await self._uss.xadd(key, {"_ts": ts_ms, **event})
        return encode_bulk_string(f"{ts_ms}-{seq}")

    async def cmd_xlen(self, args: list[str]) -> bytes:
        if len(args) != 2:
            return _wrong_args("XLEN")
        events = await self._uss.xrange(args[1], after_seq=0, limit=1_000_000)
        return encode_integer(len(events))

    async def cmd_xrange(self, args: list[str]) -> bytes:
        """XRANGE key start end [COUNT count]"""
        if len(args) < 4:
            return _wrong_args("XRANGE")
        key = args[1]
        count = 100
        for i, a in enumerate(args[4:], 4):
            if a.upper() == "COUNT" and i + 1 < len(args):
                count = int(args[i + 1])
        events = await self._uss.xrange(key, after_seq=0, limit=count)
        return encode_array(_format_stream_entries(events))

    async def cmd_xread(self, args: list[str]) -> bytes:
        """XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]"""
        i, count = 1, 100
        while i < len(args):
            if args[i].upper() == "COUNT" and i + 1 < len(args):
                count = int(args[i + 1]); i += 2
            elif args[i].upper() == "BLOCK" and i + 1 < len(args):
                i += 2  # non-blocking compat — ignore timeout
            elif args[i].upper() == "STREAMS":
                i += 1; break
            else:
                i += 1
        remaining = args[i:]
        mid = len(remaining) // 2
        keys_, ids = remaining[:mid], remaining[mid:]
        if not keys_:
            return _wrong_args("XREAD")
        result = []
        for key, start_id in zip(keys_, ids):
            after_seq = 0
            if start_id == "$":
                # Consumer wants only new events — return empty for now
                result.append([key, []])
                continue
            try:
                after_seq = int(start_id.split("-")[0])
            except ValueError:
                after_seq = 0
            events = await self._uss.xrange(key, after_seq=after_seq, limit=count)
            result.append([key, _format_stream_entries(events)])
        return encode_array(result)

    async def cmd_xrevrange(self, args: list[str]) -> bytes:
        if len(args) < 4:
            return _wrong_args("XREVRANGE")
        events = await self._uss.xrange(args[1], after_seq=0, limit=100)
        return encode_array(list(reversed(_format_stream_entries(events))))

    async def cmd_xinfo(self, args: list[str]) -> bytes:
        return encode_array([])  # Minimal compat


# ==================================================================
# Helpers
# ==================================================================

def _to_str(val: Any) -> Optional[str]:
    """Coerce a USS value to a Redis bulk-string-compatible str."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, (int, float)):
        return str(val)
    # Dicts/lists are stored as native types in USS; return JSON for clients
    return json.dumps(val, separators=(",", ":"))


def _format_stream_entries(events: list[Any]) -> list[list]:
    """Convert USS XRANGE result into Redis stream entry format [[id, [k,v,...]], ...]."""
    entries = []
    for idx, ev in enumerate(events):
        if isinstance(ev, dict):
            ts = ev.get("_ts", int(time.time() * 1000))
            fields: list[str] = []
            for k, v in ev.items():
                if k == "_ts":
                    continue
                fields.extend([str(k), str(v) if v is not None else ""])
            entries.append([f"{ts}-{idx}", fields])
        else:
            ts = int(time.time() * 1000)
            entries.append([f"{ts}-{idx}", ["value", str(ev)]])
    return entries


# ==================================================================
# Dispatch table (built once from method names)
# ==================================================================

def _build_dispatch() -> dict[str, Any]:
    table: dict[str, Any] = {}
    prefix = "cmd_"
    for name in dir(CommandHandler):
        if name.startswith(prefix):
            redis_cmd = name[len(prefix):].upper()
            table[redis_cmd] = getattr(CommandHandler, name)
    return table


_DISPATCH: dict[str, Any] = _build_dispatch()
