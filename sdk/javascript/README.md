# ahanaflow — Node.js / JavaScript SDK

> Compressed State & Event Engine by AhanaAI  
> **In-process engine:** 1.57M ops/s in fast mode (March 30, 2026) · about 1.30x local SQLite WAL NORMAL · about 4.9x smaller WAL than safe/strict  
> **TCP server mode:** ~125-365K ops/sec depending on operation (network overhead) · near-parity with Redis after April 9 orjson optimization

## Install

```bash
npm install ahanaflow
# or
yarn add ahanaflow
```

Requires Node.js 18+. Zero external dependencies — uses built-in `net` module only.

---

## Quick Start

```javascript
const { AhanaFlowClient } = require('ahanaflow');

const client = new AhanaFlowClient({ host: 'localhost', port: 9633 });

// KV with TTL
await client.set('session:alice', { role: 'admin' }, { ttl: 3600 });
console.log(await client.get('session:alice'));  // → { role: 'admin' }

// Atomic counter
const count = await client.incr('page:views');

// Queue (FIFO)
await client.enqueue('jobs', { type: 'email', to: 'bob@example.com' });
const job = await client.dequeue('jobs');

// Append-only stream
const seqId = await client.xadd('events', { action: 'login', user: 'alice' });
const events = await client.xrange('events', 0, 50);

// Multi-get
const values = await client.mget('key1', 'key2', 'key3');

// Switch durability mode at runtime — no server restart
// NOTE: These numbers are for in-process CompressedStateEngine.
// TCP server throughput is ~125-365K ops/sec due to network overhead.
await client.setDurabilityMode('fast');   // in-process: 1.57M ops/s (March 30, 2026)

await client.close();
```

### ESM (import)

```javascript
import { AhanaFlowClient } from 'ahanaflow';

const client = new AhanaFlowClient({ host: 'localhost', port: 9633 });
```

### TypeScript

```typescript
import { AhanaFlowClient, DurabilityMode } from 'ahanaflow';

const client = new AhanaFlowClient({ host: 'localhost', port: 9633 });
await client.setDurabilityMode('fast' as DurabilityMode);
```

---

## API Reference

### Constructor

```javascript
const client = new AhanaFlowClient({
  host: '127.0.0.1',   // default
  port: 9633,          // default
  timeout: 5000,       // ms, default
  autoReconnect: true, // default
});
```

### Key-Value

- `set(key, value, { ttl? })`: Store a value. Optional TTL in seconds.
- `get(key)`: Retrieve a value. Returns `null` if missing or expired.
- `delete(key)`: Delete a key. Returns `true` if it existed.
- `incr(key, amount=1)`: Atomic integer increment. Returns the new value.
- `exists(key)`: TTL-aware existence check.
- `keys(prefix='')`: List all live keys, optionally filtered by prefix.
- `ttl(key)`: Remaining TTL in seconds. `-1` means persistent and `-2` means missing.
- `mget(...keys)`: Fetch multiple keys in one round-trip.

### Queues

- `enqueue(queue, payload)`: Push to the FIFO queue tail.
- `dequeue(queue)`: Pop from the FIFO queue head. Returns `null` if empty.
- `qlen(queue)`: Return the current queue depth.

### Streams

- `xadd(stream, event)`: Append an event and return a monotonic sequence ID.
- `xrange(stream, after=0, limit=100)`: Read events after a sequence ID.

### Control

- `ping()`: Health check. Returns `'PONG'`.
- `stats()`: Live stats including keys, WAL size, compression ratio, and structure counts.
- `configGet(key)`: Read a runtime config setting.
- `configSet(key, value)`: Write a runtime config setting.
- `setDurabilityMode(mode)`: Switch to `'safe'`, `'fast'`, or `'strict'`.
- `flushAll()`: Wipe all state. This is irreversible.
- `close()`: Close the TCP connection.

---

## Durability Modes

- `'safe'`: about `967K ops/s`, about `2,862 KB` WAL per `20K` ops, OS-buffered single-record writes.
- `'fast'`: about `1.57M ops/s`, about `584 KB` WAL per `20K` ops, batched writes with about `50ms` flush cadence.
- `'strict'`: about `770K ops/s`, about `2,862 KB` WAL per `20K` ops, single-record writes with flush + `fsync`.

SQLite WAL NORMAL measured about `1.21M ops/s` on the same `20,000` iteration `SET + GET + INCR` harness.

Measured local competitors in the notebook now also include `dbm.ndbm` (~`1.21M`), `dbm.gnu` (~`1.09M`), SQLite FULL variants (~`680K`), a local `redis-server` configured without persistence (~`55K`), and DuckDB (~`2K`) on the same narrow loop.

```javascript
await client.setDurabilityMode('fast');  // no restart needed
```

---

## Performance Notes

**In-Process `CompressedStateEngine`:**
- **1.57M ops/s** (fast mode, March 30, 2026) - Direct API, no network overhead
- Use when: Embedding state engine in the same process as your Node.js application
- Ideal for: Single-process applications, libraries, embedded systems

**TCP Server Mode (`AhanaFlowClient` connecting to `universal_server`):**
- **~125K ops/sec** for single-key operations (KV_SET_GET: 0.95× Redis after April 9 orjson optimization)
- **~365K ops/sec** for batched operations (KV_MSET_MGET: 1.63× Redis - USS wins)
- Use when: Multi-process architecture, microservices, remote state access
- Ideal for: Distributed systems, polyglot access, shared state across services
- **April 9 evening:** Tail latency blocker fixed (removed 1ms polling timeout from readers-writer lock)

---

The Node package now ships a thin CLI for interacting with a running server:

```bash
ahanaflow ping --host 127.0.0.1 --port 9633
ahanaflow stats --host 127.0.0.1 --port 9633
ahanaflow mode fast --host 127.0.0.1 --port 9633
ahanaflow get session:alice --host 127.0.0.1 --port 9633
```

---

## Error Handling

```javascript
const {
  AhanaFlowClient,
  AhanaConnectionError,
  AhanaCommandError,
  AhanaTimeoutError,
} = require('ahanaflow');

try {
  await client.set('key', 'value');
} catch (err) {
  if (err instanceof AhanaConnectionError) {
    console.error('Server unreachable:', err.message);
  } else if (err instanceof AhanaCommandError) {
    console.error('Command rejected:', err.message);
  } else if (err instanceof AhanaTimeoutError) {
    console.error('Request timed out:', err.message);
  } else {
    throw err;
  }
}
```

---

## License

Apache 2.0 · Built on [Ahana Compression Protocol (ACP)](https://ahanaai.com) · © 2026 AhanaAI  
Contact: <jeremiah@ahanazip.com>
