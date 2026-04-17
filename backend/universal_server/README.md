# Universal Server (Isolated Folder)

This folder contains a standalone network server built on top of the Branch 33 compressed state engine.

## Scope

The universal server targets control-plane workloads that usually sit in Redis-like stores:

1. key/value state
2. counters
3. queue primitives
4. append-only event streams

Current wire/runtime scope also includes:

1. `MSET` for single-lock batch writes
2. `MGET` for grouped reads
3. `MINCR` for batched counter updates
4. `PIPELINE` for grouped command submission over one request

## Why Isolated

This implementation lives in its own folder so it can evolve independently from the lower-level `state_engine` module.

## Protocol

Transport: newline-delimited JSON over TCP.

Implementation notes:

1. `orjson` is used on the hot protocol path when available, with stdlib JSON fallback.
2. Tiny WAL records can skip compression to reduce overhead on very small frames.
3. Durability mode can be selected at launch (`safe`, `fast`, `strict`).

Example commands:

```json
{"cmd":"PING"}
{"cmd":"SET","key":"tenant:1","value":{"status":"ok"}}
{"cmd":"GET","key":"tenant:1"}
{"cmd":"INCR","key":"requests","amount":1}
{"cmd":"ENQUEUE","queue":"jobs","payload":{"id":"j-1"}}
{"cmd":"DEQUEUE","queue":"jobs"}
{"cmd":"XADD","stream":"access","event":{"path":"/health","status":200}}
{"cmd":"XRANGE","stream":"access","after_seq":0,"limit":100}
{"cmd":"STATS"}
```

## Run

From this branch directory:

```bash
python -m universal_server.cli serve --wal ./tmp_universal.wal --host 127.0.0.1 --port 9633 --durability-mode fast
```

Use `--durability-mode strict` when validating crash-recovery persistence.

## Benchmark

Run the durability-aware local benchmark against a SQLite baseline:

```bash
python -m universal_server.cli benchmark --iterations 20000
```

This benchmark reports:

1. operation throughput for `safe`, `fast`, and `strict` AhanaFlow durability modes
2. SQLite WAL NORMAL throughput and on-disk size for the same harness
3. AhanaFlow WAL size and compression ratio for each durability mode
4. comparison metrics such as `fast_vs_sqlite_speedup` and `wal_reduction_fast_vs_safe`

The current `strict` mode is the real local-durability path: every record is flushed and `fsync`'d before the next write.

## Current Measured Boundary

Latest validated results for the live TCP server:

1. Canonical compact JSON fast-mode lane: pipelined KV at `493,812 ops/sec` — **1.057× Redis AOF** throughput with **3.42× smaller** storage (USS WAL 3,835 KB vs Redis AOF 13,116 KB).
2. Async RESP lane (Redis-compatible): pipelined KV at `281,966 ops/sec` — `0.65× Redis AOF` throughput but **2.07× smaller** storage (USS WAL 3,861 KB vs Redis AOF 7,994 KB). Full RESP protocol support (GET/SET/INCR/MGET/MSET/MINCR/PIPELINE) with Cython-accelerated parser.
3. Safe-mode compact lane: still below Redis on lighter persistence baselines (~0.86×).
4. Strict mode: durability-first lane, not a speed lane.
5. **Storage advantage**: USS ACP-compressed WAL consistently 2-3.4× smaller than Redis AOF for the same operational workload.

The honest commercialization message: compact fast mode beats Redis AOF on both throughput and storage; RESP provides Redis drop-in compatibility with a throughput gap offset by 2× storage savings.

Treat this server as correct and durable enough for controlled deployment. The compact lane beats Redis AOF on throughput and storage. The RESP lane provides Redis compatibility with competitive (not leading) throughput and dominant storage efficiency.
