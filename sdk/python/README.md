# ahanaflow — Python SDK

> Compressed State & Event Engine by AhanaAI  
> **Public benchmark boundary:** use the April 16 controlled-deployment packet in `docs/PRODUCTION_READINESS_REPORT.md`  
> **Historical in-process note:** the embedded engine reached 1.57M ops/s on a narrow March 30 harness, but that is not the primary public claim surface for the deploy repo

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
# NOTE: The primary public benchmark boundary for the deploy repo is the April 16
# controlled-deployment packet, not the historical in-process microbenchmark.
client.set_durability_mode("fast")

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

- `safe`: OS-buffered single-record writes.
- `fast`: batched writes with about `50ms` flush cadence.
- `strict`: single-record writes with flush + `fsync`.

See `docs/PRODUCTION_READINESS_REPORT.md` for the current public claim boundary and `README.md` for the approved vector lane wording.

Switch live:

```python
client.set_durability_mode("fast")   # no restart needed
```

---

## Performance Notes

**In-Process `CompressedStateEngine`:**
- Historical local microbenchmark only; not the primary public claim boundary for this repo
- Use when: Embedding state engine in the same process as your application
- Ideal for: Single-process applications, libraries, embedded systems

**TCP Server Mode (`AhanaFlowClient` connecting to `universal_server`):**
- Compact fast-mode and RESP public numbers are maintained in `docs/PRODUCTION_READINESS_REPORT.md`
- Use when: Multi-process architecture, microservices, remote state access
- Ideal for: Distributed systems, language-agnostic access, shared state across services
- Vector claim boundary is maintained in `docs/VECTOR_STATE_SERVER_V2_CLAIM_BOUNDARY.md`

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
