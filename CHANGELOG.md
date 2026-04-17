# AhanaFlow Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-04-16

### Added
- **Hybrid wire protocol** — auto-detects RESP (Redis) vs compact JSON per TCP connection
- **`RedisCompatClient`** — drop-in replacement for `redis-py` with pipeline support
- **`AsyncUniversalStateServer`** — production-grade asyncio server with hybrid routing
- **RESP protocol layer** (`redis_compat/`) — full RESP2 parser, encoder, pre-cached response pool
- **Cython RESP accelerator** (`_resp_accel.pyx`) — optional compiled fast path for RESP parsing
- **Pipeline dispatch** (`_dispatch_pipeline_locked`) — atomic batched command execution
- **Compact response mode** — reduced JSON wire overhead for high-throughput paths
- **`benchmark_vs_competitors.py`** — comprehensive Redis/USS competitive benchmark suite
- **Inlined engine dispatch** — bypasses Python method call overhead for hot paths (SET/GET/INCR)

### Performance
- **1.2-1.4× Redis throughput** on compact JSON wire (up from 0.94-0.96×)
- **1.63× Redis throughput** on batched operations (maintained)
- Near-parity with Redis on RESP wire (drop-in compatible)
- Inlined dispatch eliminates per-command Python overhead

### Fixed
- Protocol round-trip fidelity for all value types (None, bool, int, float, str, dict, list)
- Pipeline atomic execution under lock for consistency

---

## [1.0.0] - 2026-04-10

### Added
- Initial public release
- Universal State Server with KV store, queues, and streams
- Vector State Server V2 with exact and HNSW search
- Compressed WAL with 88.7% reduction (Pro tier)
- Three durability modes (fast/safe/strict)
- TTL support for keys
- Python SDK and TCP protocol
- Docker image and Kubernetes manifests
- Commercial licensing with API key system
- Documentation and examples

### Performance
- 1.57M ops/s in fast mode
- 967K ops/s in safe mode (production recommended)
- 770K ops/s in strict mode
- 5× smaller WAL files with Pro compression

### Documentation
- Complete deployment guide
- API key setup instructions
- Working examples (rate limiter, job queue, session store)
- Docker and Kubernetes deployment configs

## [Unreleased]

### Planned for v1.1 (Q3 2026)
- WAL compaction and snapshotting
- Multi-key atomic transactions (MULTI/EXEC)
- Prometheus metrics endpoint
- TLS encryption
- Authentication tokens
- Batch operations (MGET/MSET expanded)

### Planned for v1.2 (Q4 2026)
- Leader-follower replication
- Horizontal sharding
- Go and Rust client SDKs
- Kubernetes operator

### Planned for v2.0 (2027)
- Raft consensus for distributed deployments
- Geographic replication
- AhanaFlow Cloud (hosted SaaS)
- Advanced monitoring and alerting

---

For upgrade instructions, see [UPGRADING.md](./docs/UPGRADING.md)
