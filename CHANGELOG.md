# AhanaFlow Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
