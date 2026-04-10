# AhanaFlow DB — Compressed State & Event Engine

<div align="center">

![AhanaFlow Logo](https://www.ahanaflow.com/assets/ahanaflow-logo.png)

**State, Vector Search & Compression in One Runtime**

[![License: Dual](https://img.shields.io/badge/license-Dual%20(Non--Commercial%20%2F%20Commercial)-blue)](./LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Performance](https://img.shields.io/badge/performance-1.57M%20ops%2Fs%20(in--process)%20%7C%20~125K%20(TCP)-green)](./docs/BENCHMARKS.md)
[![Compression](https://img.shields.io/badge/compression-88.7%25-orange)](./docs/COMPRESSION.md)

[Website](https://www.ahanaflow.com) • [Documentation](./docs/) • [API Plans](https://www.ahanaflow.com/#pricing) • [Quick Start](#quick-start)

</div>

---

## What is AhanaFlow?

**AhanaFlow** is a Database suite to replace both Sqlite and Redis using a cutting edge **compressed, self-contained state and event streaming engine** built on AhanaAI's Ahana Compression Protocol (ACP). It provides:

- **Key-Value Store** with TTL, atomic counters, and grouped operations
- **FIFO Queues** for job processing and async workflows  
- **Event Streams** with append-only, sequence-indexed access
- **Vector Search** with exact and HNSW approximate nearest-neighbor (billion-scale)
- **Compressed WAL** with 50-60% (community) or **88.7%** (pro) storage reduction
- **1.57M ops/s** throughput with runtime-switchable durability modes
- **Zero external dependencies** — no Redis, no managed infra, just drop in as a library

### Why AhanaFlow?

Replace Redis for workloads that don't need Redis scale:

| What You Get | vs Redis | vs SQLite |
|--------------|----------|-----------|
| **Performance (in-process)** | 1.57M ops/s (embedded mode) | 1.3× faster than SQLite WAL |
| **Performance (TCP server)** | 0.94-0.96× Redis (single-key), 1.63× Redis (batched) | N/A |
| **Compression** | 88.7% WAL reduction (Pro) | 4.9× smaller than uncompressed |
| **Features** | KV + TTL + Queues + Streams + Vectors | KV only, no TTL/queues |
| **Ops Complexity** | Zero — single process | Requires separate Redis server + AUTH/TLS |
| **Cost** | $0–$499/mo for most use cases | $150–$400/mo for managed Redis |

---

## Quick Start

### Installation

```bash
# Community Edition (free for non-commercial use)
pip install ahanaflow

# Or install from source
git clone https://github.com/AhanaAI-Company/ahanaflow.git
cd ahanaflow
pip install -e .
```

### Basic Usage (In-Process)

```python
from backend.state_engine import CompressedStateEngine

# Create a compressed state engine
with CompressedStateEngine("app.wal", durability_mode="safe") as engine:
    # Key-value operations
    engine.put("user:123", {"name": "Alice", "plan": "pro"})
    user = engine.get("user:123")
    
    # Atomic counters
    engine.incr("page_views", 1)
    
    # TTL keys (expire after 1 hour)
    engine.put("session:abc", {"token": "xyz"}, ttl_seconds=3600)
```

### TCP Server Mode

```bash
# Start the universal server (KV + queues + streams)
python -m backend.universal_server.cli serve --port 9633 --host 0.0.0.0

# Start the vector server (vector search)
python -m backend.universal_server.cli serve-vector-v2 --port 9644 --host 0.0.0.0
```

### Python Client (TCP)

```python
import socket
import json

def send_command(sock, cmd):
    sock.sendall((json.dumps(cmd) + "\n").encode())
    return json.loads(sock.recv(16384).decode().strip())

# Connect
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 9633))

# Set a value
send_command(sock, {"cmd": "SET", "key": "msg", "value": "Hello, AhanaFlow!"})

# Get the value
result = send_command(sock, {"cmd": "GET", "key": "msg"})
print(result["result"])  # "Hello, AhanaFlow!"

# Increment counter
send_command(sock, {"cmd": "INCR", "key": "counter", "amount": 10})

# Enqueue job
send_command(sock, {"cmd": "ENQUEUE", "queue": "jobs", "item": {"type": "email", "to": "user@example.com"}})

# Dequeue job
job = send_command(sock, {"cmd": "DEQUEUE", "queue": "jobs"})
print(job["result"])  # {"type": "email", "to": "user@example.com"}
```

### Vector Search

```python
from backend.vector_server import VectorStateServerV2

# Create vector engine
engine = VectorStateServerV2("vectors.wal")

# Create collection (1536 dimensions for OpenAI embeddings)
engine.create_collection("documents", dimensions=1536, metric="cosine")

# Upsert vectors
engine.upsert("documents", "doc_1", embedding_vector_1536, 
              metadata={"title": "README.md"},
              payload={"text": "Full document text..."})

# Search similar vectors
results = engine.query("documents", query_vector_1536, top_k=10)
for match in results:
    print(f"{match['id']}: similarity={match['score']:.4f}")
    print(f"  Title: {match['metadata']['title']}")
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
- Billion-scale vector indexing
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
# Pull the image
docker pull ghcr.io/ahanaai-company/ahanaflow:latest

# Run the universal server
docker run -d \
  -p 9633:9633 \
  -v $(pwd)/data:/data \
  --name ahanaflow \
  ghcr.io/ahanaai-company/ahanaflow:latest

# Test connection
echo '{"cmd":"PING"}' | nc localhost 9633
```

### Kubernetes

```bash
# Apply the manifests
kubectl apply -f k8s/ahanaflow-deployment.yaml

# Port forward to test
kubectl port-forward svc/ahanaflow 9633:9633
```

See [docs/DEPLOYMENT_GUIDE.md](./docs/DEPLOYMENT_GUIDE.md) for complete deployment instructions.

---

## Commercial Use & API Plans

**AhanaFlow is free for non-commercial use.** For commercial deployment, you need an API key from [www.ahanaflow.com](https://www.ahanaflow.com).

### Pricing Plans

| Plan | Price | Requests/Month | Compression | Support |
|------|-------|----------------|-------------|---------|
| **Free** | $0 | ≤10K | 50-60% (community) | Community forums |
| **Starter** | $49/mo | 100K | 88.7% (Pro dictionary) | Email support |
| **Professional** | $149/mo | 1M | 88.7% (Pro dictionary) | Priority support + SLA |
| **Business** | $499/mo | 10M | 88.7% (Pro dictionary) | 24/7 support + dedicated CSM |
| **Enterprise** | Custom | Unlimited | Custom dictionaries | On-prem + source access |

### Why Upgrade?

✅ **88.7% compression** with trained dictionaries (vs 50-60% community)  
✅ 5× smaller WAL files = lower storage costs + faster backups  
✅ Production SLA with 99.9% uptime guarantee  
✅ Priority support with <4 hour response time  
✅ Legal indemnification for commercial deployment  

### Get Your API Key

1. Visit [www.ahanaflow.com](https://www.ahanaflow.com)
2. Click "Get API Key" and choose your plan
3. Add your API key to your deployment:

```python
# Configure your API key
engine = CompressedStateEngine(
    "app.wal",
    api_key="your_api_key_here",  # Unlocks Pro compression
    durability_mode="safe"
)
```

Or set via environment variable:

```bash
export AHANAFLOW_API_KEY="your_api_key_here"
python -m backend.universal_server.cli serve
```

**See [docs/API_KEY_SETUP.md](./docs/API_KEY_SETUP.md) for complete setup instructions.**

---

## Performance

**In-Process (Embedded) Usage:**

Measured on AMD Ryzen 9 9900X, 128GB RAM, NVMe SSD:

| Durability Mode | Ops/s | Disk per 20K ops | Latency (p99) |
|-----------------|-------|------------------|---------------|
| **fast** | **1.57M** | 584 KB | <1ms |
| **safe** | 967K | 2,862 KB | <2ms |
| **strict** | 770K | 2,862 KB | <5ms |

*Benchmark harness: 20,000 iterations of `SET + GET + INCR` with compression enabled, in-process `CompressedStateEngine` (no network overhead).*

**TCP Server Mode:**

With network protocol overhead (newline-delimited JSON over TCP):

| Operation | Throughput (NetworkServer) | vs Redis |
|-----------|---------------------------|----------|
| KV_SET_GET | ~125K ops/sec | 0.95× (near parity) |
| KV_MSET_MGET (batched) | ~365K ops/sec | **1.63× (USS wins)** |
| KV_PIPELINE_SET_GET | ~224K ops/sec | 0.94× (near parity) |
| COUNTER_INCR | ~96K ops/sec | 0.96× (near parity) |
| COUNTER_BATCH_INCR | ~236K ops/sec | **1.63× (USS wins)** |

*Based on April 9, 2026 competitive benchmark after orjson optimization. UniversalStateServer achieves near-parity with Redis on single-key operations and wins on batched operations.*

See [docs/BENCHMARKS.md](./docs/BENCHMARKS.md) for detailed performance analysis.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Client (Python SDK / TCP protocol)                 │
└──────────────────────┬──────────────────────────────┘
                       │ newline-delimited JSON
┌──────────────────────▼──────────────────────────────┐
│  Universal State Server (port 9633)                 │
│  ┌─────────────────────────────────────────────┐   │
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
- [API Reference](./docs/API_REFERENCE.md) — Complete command documentation  
- [Benchmarks](./docs/BENCHMARKS.md) — Performance analysis and comparison
- [Architecture](./docs/ARCHITECTURE.md) — Internal design and WAL format
- [Compression Guide](./docs/COMPRESSION.md) — How ACP compression works

---

## Examples

See [examples/](./examples/) for complete working examples:

- [Rate Limiter](./examples/rate_limiter.py) — Token bucket using INCR
- [Job Queue](./examples/job_queue.py) — Background task processing with ENQUEUE/DEQUEUE
- [Session Store](./examples/session_store.py) — User session management with TTL
- [Event Log](./examples/event_log.py) — Audit trail with APPEND_EVENT
- [RAG Memory](./examples/rag_memory.py) — Vector similarity search for LLM context

---

## Community & Support

- **Website:** [www.ahanaflow.com](https://www.ahanaflow.com)
- **Documentation:** [docs/](./docs/)
- **GitHub Issues:** [Report bugs or request features](https://github.com/AhanaAI-Company/ahanaflow/issues)
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
