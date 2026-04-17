# ahanaflow — Node.js / JavaScript SDK

> Compressed State & Event Engine by AhanaAI  
> **Public benchmark boundary:** use the April 16 controlled-deployment packet in `docs/PRODUCTION_READINESS_REPORT.md`  
> **Historical in-process note:** the embedded engine reached 1.57M ops/s on a narrow March 30 harness, but that is not the primary public claim surface for the deploy repo

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
// NOTE: The primary public benchmark boundary for the deploy repo is the April 16
// controlled-deployment packet, not the historical in-process microbenchmark.
await client.setDurabilityMode('fast');

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

- `'safe'`: OS-buffered single-record writes.
- `'fast'`: batched writes with about `50ms` flush cadence.
- `'strict'`: single-record writes with flush + `fsync`.

See `docs/PRODUCTION_READINESS_REPORT.md` for the current public claim boundary and `README.md` for the approved vector lane wording.

```javascript
await client.setDurabilityMode('fast');  // no restart needed
```

---

## Performance Notes

**In-Process `CompressedStateEngine`:**
- Historical local microbenchmark only; not the primary public claim boundary for this repo
- Use when: Embedding state engine in the same process as your Node.js application
- Ideal for: Single-process applications, libraries, embedded systems

**TCP Server Mode (`AhanaFlowClient` connecting to `universal_server`):**
- Compact fast-mode and RESP public numbers are maintained in `docs/PRODUCTION_READINESS_REPORT.md`
- Use when: Multi-process architecture, microservices, remote state access
- Ideal for: Distributed systems, polyglot access, shared state across services
- Vector claim boundary is maintained in `docs/VECTOR_STATE_SERVER_V2_CLAIM_BOUNDARY.md`

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
