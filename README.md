# AhanaFlow — Compressed Durable State And Controlled-Deployment Vector Runtime

<div align="center">

![AhanaFlow Logo](https://www.ahanaflow.com/assets/ahanaflow-logo.png)

**Compressed durable state, integrated vector operations, one runtime**

[![License: Dual](https://img.shields.io/badge/license-Dual%20(Non--Commercial%20%2F%20Commercial)-blue)](./LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Performance](https://img.shields.io/badge/performance-47.6k%20req%2Fs%20mixed%20load%20%7C%2046.20ms%20vector%20p99-green)](./docs/PRODUCTION_READINESS_REPORT.md)
[![Compression](https://img.shields.io/badge/compression-88.7%25-orange)](./docs/PRODUCTION_READINESS_REPORT.md)

[Website](https://www.ahanaflow.com) • [Documentation](./docs/) • [API Plans](https://www.ahanaflow.com/#pricing) • [Quick Start](#quick-start)

</div>

---

## What is AhanaFlow?

**AhanaFlow** packages two Branch 33 runtimes into one deployable system: UniversalStateServer for compressed durable control-plane state, and VectorStateServerV2 for exact plus HNSW-backed vector retrieval with an explicit public claim boundary. It provides:

- **Key-Value Store** with TTL, atomic counters, and grouped operations
- **FIFO Queues** for job processing and async workflows  
- **Event Streams** with append-only, sequence-indexed access
- **Vector Search** with exact and HNSW-backed retrieval for a bounded controlled-deployment lane
- **Compressed WAL** with 50-60% (community) or **88.7%** (pro) storage reduction
- **Mode-qualified April 16 performance** with `47,642.31 req/sec` mixed load in compact fast-mode and a frozen `46.20 ms` selective vector p99 lane
- **Zero external dependencies** — no Redis, no managed infra, just drop in as a library

### Why AhanaFlow?

Use AhanaFlow when you want one runtime for retained state plus integrated vector operations and the current controlled-deployment boundary fits your workload:

| What You Get | vs Redis | vs SQLite |
|--------------|----------|-----------|
| **Compact fast-mode** | 1.157× Redis on the official pipelined KV lane | N/A |
| **RESP-compatible lane** | Compatibility-first, slower than Redis today | N/A |
| **Compression** | 88.7% WAL reduction (Pro) | 4.9× smaller than uncompressed |
| **Features** | KV + TTL + Queues + Streams + Vectors | KV only, no TTL/queues |
| **Ops Complexity** | One runtime for state + vector packaging | Requires separate Redis server + AUTH/TLS |
| **Cost** | Self-host has no monthly fee. Planned hosted plans are $9.99 and $19.99/mo | $150–$400/mo for managed Redis |

### Current Claim Boundary

The approved public Branch 33 vector package is intentionally narrow:

1. Approved selective vector lane: `10K` corpus, `M=12`, `ef_construction=32`, `ef_search=24`, filtered 8-bucket query, pipeline depth `2`
2. Frozen selective result: `46.20 ms` p99 with fallback count `0`
3. Round 10 confirmed the same lane at `46.97 ms` p99 without widening the claim
4. Mixed append/query work produced about `33x+` maintenance relief on the bounded maintenance lane
5. April 9 proof remains public only when stated narrowly: `82K vectors/s` insert throughput and `32,036 docs/s` RAG ingest are not substitutes for concurrent network ANN latency claims

Canonical buyer-facing docs in this repo:

1. `docs/PRODUCTION_READINESS_REPORT.md`
2. `docs/VECTOR_STATE_SERVER_V2_CLAIM_BOUNDARY.md`

---

## Quick Start

This public tree has no `k8s/`, `reports/`, `v1_1/`, or `deploy/secrets/`. Start the in-repo single-node runtime and install the SDKs from this checkout (not published to PyPI or npm):

```bash
docker compose --profile single-node up -d

# Python SDK (not on PyPI)
pip install -e ./sdk

# JavaScript SDK (not on npm)
npm install ./sdk/javascript
```

Smoke-test state plus vector in one script:
```python
import json,socket; call=lambda p,c:(lambda s:(s.sendall((json.dumps(c)+"\n").encode()),json.loads(s.recv(16384).decode()))[1])(socket.create_connection(("127.0.0.1",p)))
call(9633,{"cmd":"SET","key":"tenant:acme:plan","value":"pilot"}); call(9644,{"cmd":"VECTOR_CREATE","collection":"docs","dimensions":3,"metric":"cosine"}); call(9644,{"cmd":"VECTOR_UPSERT","collection":"docs","id":"doc-1","vector":[1,0,0],"payload":{"text":"reset password"}}); print(call(9644,{"cmd":"VECTOR_QUERY","collection":"docs","vector":[1,0,0],"top_k":1})["result"]["hits"][0])
```

---

## Features

### Universal State Server (Port 9633)

**Key-Value Operations:**
- `SET`, `GET`, `DEL` — Basic storage
- `MGET`, `MSET` — Grouped operations
- `INCR` — Atomic counters with configurable increments
- `MINCR` — Batched counter increments
- `EXISTS`, `KEYS`, `TTL` — Introspection

**Queue Operations:**
- `ENQUEUE` — Add items to FIFO queue
- `DEQUEUE` — Remove items from queue
- `QLEN` — Get queue length

**Event Streams:**
- `APPEND_EVENT` — Append to sequence-indexed stream
- `READ_EVENTS` — Read event range by sequence ID

**Control:**
- `FLUSHALL` — Clear all data
- `PING` — Health check  
- `STATS` — Get engine statistics
- `CONFIG` — Runtime configuration

### Vector State Server (Port 9644)

**Exact Search:**
- Full matrix scan with cosine/dot-product similarity
- Metadata filtering
- Payload compression (up to 60% reduction on retrieval)

**HNSW Approximate Search:**
- Controlled-deployment HNSW lane with measured recall reporting
- Product Quantization for memory efficiency
- Configurable recall/speed tradeoff
- Measured recall@k reporting

**Supported Metrics:**
- Cosine similarity
- Euclidean distance (L2)
- Inner product (dot)

---

## Deployment

### Docker

```bash
# Pull the controlled-pilot release image
docker pull ghcr.io/ahanaai-company/ahanaflow:branch-33-controlled-deployment-v1.0

# Run the universal server
docker run -d \
  -p 9633:9633 \
  -v $(pwd)/data:/data \
  --name ahanaflow \
    ghcr.io/ahanaai-company/ahanaflow:branch-33-controlled-deployment-v1.0

# Test connection
echo '{"cmd":"PING"}' | nc localhost 9633
```

### Kubernetes

This tree has no `k8s/` directory. Copy the inline example from [docs/DEPLOYMENT_GUIDE.md](./docs/DEPLOYMENT_GUIDE.md), save it locally, then apply that file:

```bash
kubectl apply -f ahanaflow-deployment.yaml
kubectl port-forward svc/ahanaflow 9633:9633
```

See [docs/DEPLOYMENT_GUIDE.md](./docs/DEPLOYMENT_GUIDE.md) for complete deployment instructions.

---

## Commercial use and self-host

**Self-host means you run this repository on a machine you control.** It is not a monthly plan, and it is not the Enterprise product. The community license covers non-commercial use. Commercial use of a self-hosted copy still needs a commercial license.

Planned hosted prices. Checkout is not open. The same numbers are on [ahanaflow.com](https://www.ahanaflow.com).

| Plan | Price | Where it runs |
|------|-------|----------------|
| **Self-host** | No monthly fee | Your machine. This repository. |
| **Pro** | $9.99 / month | Planned hosted plan |
| **Team** | $19.99 / month | Planned hosted plan |
| **Enterprise** | Email [sales@ahanaai.com](mailto:sales@ahanaai.com) | Ask. Not a self-host fee. |

Community compression on the non-commercial license is 50-60%. A commercial license is what unlocks the Pro dictionary (88.7%).

### Get a commercial license

1. Read the prices on [www.ahanaflow.com](https://www.ahanaflow.com).
2. Email [sales@ahanaai.com](mailto:sales@ahanaai.com). Checkout is not open.
3. Authenticate the TCP server with your license key (`CompressedStateEngine` does not take `api_key=`):

```bash
export AHANAFLOW_LICENSE_KEY="your_license_key_here"
python -m backend.universal_server.cli serve
```

`AHANAFLOW_LICENSE_KEY` authenticates clients to the TCP server. It does not download or install a Pro codec.

For production, prefer `*_FILE` runtime secrets and a sealed policy file over plaintext env vars. The customer/admin surfaces now support `AHANAFLOW_ADMIN_API_KEY_FILE`, `AHANAFLOW_SERVICE_API_KEY_FILE`, `AHANAFLOW_SEALED_POLICY_KEY_FILE`, `STRIPE_SECRET_KEY_FILE`, `STRIPE_WEBHOOK_SECRET_FILE`, and `AHANAFLOW_SIGNING_KEY_FILE`.

Paid proprietary artifacts should be distributed through the backend manifest flow, not from the public repository. The backend can issue customer-specific fingerprints, short-lived download grants, and per-customer PUZZLE-AUTH unlock keys once entitlement checks pass.

**See [docs/API_KEY_SETUP.md](./docs/API_KEY_SETUP.md) for complete setup instructions.**

---

## Performance

**In-Process (Embedded) Usage:**

Measured on AMD Ryzen 9 9900X, 128GB RAM, NVMe SSD:

| Durability Mode | Ops/s | Disk per 20K ops | Latency (p99) |
|-----------------|-------|------------------|---------------|
| **compact fast-mode** | **47,642 req/s mixed load** | 3,835 KB WAL | 1.884 ms |
| **async RESP** | 25,645 req/s mixed load | 3,861 KB WAL | 6.044 ms |
| **approved vector lane** | sub-50 ms selective p99 | bounded proof packet | 46.20 ms |

*Use `docs/PRODUCTION_READINESS_REPORT.md` for the current public benchmark boundary. Earlier in-process numbers are not the buyer-safe claim surface for this repo.*

**TCP Server Mode (Hybrid Wire Protocol):**

AhanaFlow auto-detects whether each TCP connection speaks **RESP** (Redis wire protocol) or **compact JSON**, routing to the optimal inlined dispatch path per connection.

| Operation | Throughput | Boundary |
|-----------|-----------|----------|
| Compact fast-mode mixed load | 47,642 req/sec | buyer-safe KV lane |
| Compact fast-mode pipelined KV | 295,184 ops/sec | 1.157× Redis vs no-persistence lane |
| Async RESP pipelined KV | 155,096 ops/sec | compatibility-first lane |
| Approved vector selective lane | 46.20 ms p99 | bounded `10K / M=12 / ef_construction=32 / ef_search=24` proof |

*Based on the April 16, 2026 official hotspot packet. Compact fast-mode is the performance lead lane, RESP is the compatibility lane, and vector proof is intentionally bounded.*

**RedisCompatClient — Drop-in Redis Replacement:**

```python
from backend.universal_server import RedisCompatClient

# Drop-in replacement for redis-py
client = RedisCompatClient("localhost", 9633)
client.set("key", "value")
print(client.get("key"))  # b"value"

# Pipeline support
pipe = client.pipeline()
pipe.set("a", "1")
pipe.set("b", "2")
pipe.incr("counter")
results = pipe.execute()
```

See [docs/PRODUCTION_READINESS_REPORT.md](./docs/PRODUCTION_READINESS_REPORT.md) for the current public benchmark boundary.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Client (Python SDK / RedisCompatClient / redis-cli)│
└──────────────────────┬──────────────────────────────┘
                       │ auto-detect: RESP or compact JSON
┌──────────────────────▼──────────────────────────────┐
│  Async Universal State Server (port 9633)           │
│  ┌─────────────────────────────────────────────┐   │
│  │  Hybrid Wire Protocol (per-connection)      │   │
│  │  ├── RESP path → _dispatch_resp_fast()      │   │
│  │  └── Compact path → _dispatch_compact()     │   │
│  └──────────────────┬──────────────────────────┘   │
│                     │                               │
│  ┌──────────────────▼──────────────────────────┐   │
│  │  Command Router (SET/GET/INCR/ENQUEUE...)   │   │
│  └──────────────────┬──────────────────────────┘   │
│                     │                               │
│  ┌──────────────────▼──────────────────────────┐   │
│  │  Compressed State Engine                     │   │
│  │  - In-memory index (key → offset)           │   │
│  │  - WAL writer with batch compression        │   │
│  │  - TTL expiry tracking                       │   │
│  │  - Queue & stream sequencing                │   │
│  └──────────────────┬──────────────────────────┘   │
└───────────────────────┴──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Compressed WAL (app.wal)                           │
│  ┌─────────────────────────────────────────────┐   │
│  │  [Header] [Batch 1 (compressed)]            │   │
│  │  [Batch 2 (compressed)] [Batch 3...]        │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- Batch compression (N=16 records per frame in fast mode) for throughput
- fsync control via durability modes (safe/fast/strict)
- In-memory index for O(1) key lookups
- Forward-compatible WAL format (single + batch frames coexist)
- Zero-copy protocol for high-throughput clients

---

## Documentation

- [Deployment Guide](./docs/DEPLOYMENT_GUIDE.md) — Docker, Kubernetes, systemd
- [API Key Setup](./docs/API_KEY_SETUP.md) — How to configure commercial licenses
- [Production Readiness Report](./docs/PRODUCTION_READINESS_REPORT.md) — Public benchmark boundary
- [Vector State Server V2 Claim Boundary](./docs/VECTOR_STATE_SERVER_V2_CLAIM_BOUNDARY.md) — Approved vector claim surface
- [Secret Rotation Runbook](./docs/SECRET_ROTATION_RUNBOOK.md) — Runtime secret mounts and rotation

---

## Examples

See [examples/](./examples/) for complete working examples:

- [Rate Limiter](./examples/rate_limiter.py) — Token bucket using INCR
- [Job Queue](./examples/job_queue.py) — Background task processing with ENQUEUE/DEQUEUE
- [Session Store](./examples/session_store.py) — User session management with TTL

---

## Community & Support

- **Website:** [www.ahanaflow.com](https://www.ahanaflow.com)
- **Documentation:** [docs/](./docs/)
- **GitHub Issues:** [Report bugs or request features](https://github.com/AhanaAi-Company/AhanaFlow/issues)
- **Discord:** [Join our community](https://discord.gg/ahanaai) (coming soon)
- **Email Support:** support@ahanaai.com (paid plans only)

---

## License

AhanaFlow is released under a **dual license**:

- **Community Edition (Non-Commercial):** Free for personal, academic, and open-source use. See [LICENSE](./LICENSE) for details.
- **Commercial License:** Required for production deployments. Get your API key at [www.ahanaflow.com](https://www.ahanaflow.com).

For commercial inquiries: sales@ahanaai.com

---

## About AhanaAI

AhanaFlow is built by [AhanaAI Corporation](https://ahanaai.com), creators of the Ahana Compression Protocol (ACP). We specialize in neural compression systems that push closer to Shannon limits.

🌺 **AhanaAI — Compression Reimagined**

---

<div align="center">

Made with 🌺 in Honolulu, Hawaii

</div>
