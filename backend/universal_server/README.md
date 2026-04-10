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

Latest validated results for the live TCP server (updated April 9, 2026):

1. Heavy mixed-client stress: `288,000` logical ops in `2.307 s` (`124,856.49 ops/sec`) with `0` errors.
2. Heavy mixed-client latency: `0.768 ms` p50, `9.907 ms` p95, `16.961 ms` p99.
3. Live Redis comparison after orjson optimization: **near-parity** on single-key ops, **UniversalStateServer wins** on batched operations.
4. Latest competitive results: `KV_SET_GET` at `0.95×` Redis, `KV_MSET_MGET` at **`1.63×` Redis (USS wins)**, `KV_PIPELINE_SET_GET` at `0.94×` Redis, `COUNTER_INCR` at `0.96×` Redis, `COUNTER_BATCH_INCR` at **`1.63×` Redis (USS wins)**.

Treat this server as correct, durable, and now with near-Redis throughput parity. The remaining primary blocker is tail latency under heavy mixed-client load (p95/p99).
