# ahanaflow — Python SDK

> Compressed State & Event Engine by AhanaAI  
> **In-process engine:** 1.57M ops/s in fast mode (March 30, 2026) · about 1.30x local SQLite WAL NORMAL · about 4.9x smaller WAL than safe/strict  
> **TCP server mode:** ~125-365K ops/sec depending on operation (network overhead) · near-parity with Redis after April 9 orjson optimization

## Install

```bash
pip install ahanaflow
```

Requires Python 3.11+. No external dependencies — pure stdlib.

---

## Quick Start

### Connect to a running AhanaFlow server

```python
from ahanaflow import AhanaFlowClient

client = AhanaFlowClient("localhost", 9633)

# KV with TTL
client.set("session:alice", {"role": "admin"}, ttl_seconds=3600)
print(client.get("session:alice"))   # → {"role": "admin"}

# Atomic counter
count = client.incr("page:views")

# Queue (FIFO)
client.enqueue("jobs", {"type": "email", "to": "bob@example.com"})
job = client.dequeue("jobs")

# Append-only stream
seq_id = client.xadd("events", {"action": "login", "user": "alice"})
events = client.xrange("events", after=0, limit=50)

# Multi-get
values = client.mget("key1", "key2", "key3")

# Switch durability mode at runtime — no server restart
# NOTE: These numbers are for in-process CompressedStateEngine.
# TCP server throughput is ~125-365K ops/sec due to network overhead.
client.set_durability_mode("fast")   # in-process: 1.57M ops/s (March 30, 2026)

client.close()
```

### Context manager

```python
with AhanaFlowClient("localhost", 9633) as client:
    client.set("x", 42)
    print(client.get("x"))
```

### Asyncio client

```python
import asyncio
from ahanaflow import AsyncAhanaFlowClient

async def main():
    async with AsyncAhanaFlowClient("localhost", 9633) as client:
        await client.set("key", "value")
        print(await client.get("key"))

asyncio.run(main())
```

---

## API Reference

### Key-Value

- `set(key, value, *, ttl_seconds=None)`: Store a value. Optional TTL in seconds.
- `get(key)`: Retrieve value. Returns `None` if missing or expired.
- `delete(key)`: Delete a key. Returns `True` if it existed.
- `incr(key, amount=1)`: Atomic integer increment. Returns new value.
- `exists(key)`: TTL-aware existence check.
- `keys(prefix="")`: List all live keys, optionally filtered by prefix.
- `ttl(key)`: Remaining TTL in seconds. `-1` means persistent and `-2` means missing.
- `mget(*keys)`: Fetch multiple keys in one round-trip.

### Queues

- `enqueue(queue, payload)`: Push to the FIFO queue tail.
- `dequeue(queue)`: Pop from the FIFO queue head. Returns `None` if empty.
- `qlen(queue)`: Return the current queue depth.

### Streams

- `xadd(stream, event)`: Append an event and return a monotonic sequence ID.
- `xrange(stream, after=0, limit=100)`: Read events after a sequence ID.

### Control

- `ping()`: Health check. Returns `"PONG"`.
- `stats()`: Live stats dict with keys, WAL size, compression ratio, and structure counts.
- `config_get(key)`: Read a runtime config setting.
- `config_set(key, value)`: Write a runtime config setting.
- `set_durability_mode(mode)`: Switch to `"safe"`, `"fast"`, or `"strict"`.
- `flushall()`: Wipe all state. This is irreversible.

---

## Durability Modes

- `safe`: about `967K ops/s`, about `2,862 KB` WAL per `20K` ops, OS-buffered single-record writes.
- `fast`: about `1.57M ops/s`, about `584 KB` WAL per `20K` ops, batched writes with about `50ms` flush cadence.
- `strict`: about `770K ops/s`, about `2,862 KB` WAL per `20K` ops, single-record writes with flush + `fsync`.

SQLite WAL NORMAL measured about `1.21M ops/s` on the same `20,000` iteration `SET + GET + INCR` harness.

Measured local competitors in the notebook now also include `dbm.ndbm` (~`1.21M`), `dbm.gnu` (~`1.09M`), SQLite FULL variants (~`680K`), a local `redis-server` configured without persistence (~`55K`), and DuckDB (~`2K`) on the same narrow loop.

Switch live:

```python
client.set_durability_mode("fast")   # no restart needed
```

---

## Performance Notes

**In-Process `CompressedStateEngine`:**
- **1.57M ops/s** (fast mode, March 30, 2026) - Direct Python API, no network overhead
- Use when: Embedding state engine in the same process as your application
- Ideal for: Single-process applications, libraries, embedded systems

**TCP Server Mode (`AhanaFlowClient` connecting to `universal_server`):**
- **~125K ops/sec** for single-key operations (KV_SET_GET: 0.95× Redis after April 9 orjson optimization)
- **~365K ops/sec** for batched operations (KV_MSET_MGET: 1.63× Redis - USS wins)
- Use when: Multi-process architecture, microservices, remote state access
- Ideal for: Distributed systems, language-agnostic access, shared state across services
- **April 9 evening:** Tail latency blocker fixed (removed 1ms polling timeout from readers-writer lock)

---

## Start the server (Python)

```bash
# Verified Branch 33 local CLI surface:
python -m universal_server.cli serve \
    --wal ./state.wal \
    --host 127.0.0.1 \
    --port 9633
```

## SDK CLI

The Python package now ships a thin CLI for interacting with a running server:

```bash
ahanaflow ping --host 127.0.0.1 --port 9633
ahanaflow stats --host 127.0.0.1 --port 9633
ahanaflow mode fast --host 127.0.0.1 --port 9633
python -m ahanaflow --help
```

---

## License

Apache 2.0 · Built on [Ahana Compression Protocol (ACP)](https://ahanaai.com) · © 2026 AhanaAI  
Contact: <jeremiah@ahanazip.com>
