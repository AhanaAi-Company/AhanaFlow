# AhanaFlow Deployment Guide

Complete guide for deploying AhanaFlow in various environments.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation Methods](#installation-methods)
3. [Configuration](#configuration)
4. [Deployment Options](#deployment-options)
5. [Production Best Practices](#production-best-practices)
6. [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## Prerequisites

### System Requirements

**Minimum:**
- **CPU:** 2 cores
- **RAM:** 2 GB
- **Disk:** 10 GB available (SSD recommended)
- **OS:** Linux, macOS, or Windows with Docker

**Recommended (Production):**
- **CPU:** 4+ cores
- **RAM:** 8+ GB
- **Disk:** 50+ GB SSD (NVMe for best performance)
- **OS:** Linux (Ubuntu 22.04 LTS or later)

### Software Dependencies

- **Python:** 3.10 or later
- **Docker:** 24.0+ (for containerized deployment)
- **Kubernetes:** 1.28+ (for cluster deployment)

---

## Installation Methods

### Method 1: pip Install (Simplest)

```bash
# Community Edition (free for non-commercial use)
pip install ahanaflow

# Verify installation
python -c "from backend.state_engine import CompressedStateEngine; print('✓ AhanaFlow installed')"
```

### Method 2: From Source

```bash
# Clone the repository
git clone https://github.com/AhanaAI-Company/ahanaflow.git
cd ahanaflow

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in editable mode
pip install -e .

# Run tests
python -m pytest tests/ -v
```

### Method 3: Docker (Production Ready)

```bash
# Pull the official image
docker pull ghcr.io/ahanaai-company/ahanaflow:latest

# Or build from source
docker build -t ahanaflow:local .
```

---

## Configuration

### Environment Variables

Create a `.env` file in your project root:

```bash
# API Key (for commercial deployments)
AHANAFLOW_API_KEY=your_api_key_here

# Server Configuration
AHANAFLOW_HOST=0.0.0.0
AHANAFLOW_PORT=9633
AHANAFLOW_VECTOR_PORT=9644

# Storage Paths
AHANAFLOW_DATA_DIR=/data
AHANAFLOW_WAL_PATH=/data/state.wal
AHANAFLOW_VECTOR_WAL_PATH=/data/vectors.wal

# Performance Tuning
AHANAFLOW_DURABILITY_MODE=safe  # Options: safe, fast, strict
AHANAFLOW_BATCH_SIZE=16         # Records per compression batch
AHANAFLOW_BATCH_TIMEOUT_MS=50   # Max wait time for batch

# Security (optional)
AHANAFLOW_AUTH_ENABLED=false
AHANAFLOW_AUTH_TOKEN=your_secure_token_here

# Logging
AHANAFLOW_LOG_LEVEL=INFO        # DEBUG, INFO, WARNING, ERROR
AHANAFLOW_LOG_FILE=/data/ahanaflow.log
```

### Configuration File (Optional)

Create `ahanaflow.yaml`:

```yaml
server:
  host: 0.0.0.0
  port: 9633
  vector_port: 9644

storage:
  data_dir: /data
  wal_path: /data/state.wal
  vector_wal_path: /data/vectors.wal

performance:
  durability_mode: safe
  batch_size: 16
  batch_timeout_ms: 50

security:
  auth_enabled: false
  auth_token: null

api:
  key: null  # Set via AHANAFLOW_API_KEY environment variable

logging:
  level: INFO
  file: /data/ahanaflow.log
```

---

## Deployment Options

### Option 1: Local Development

**In-Process (Embedded):**

```python
from backend.state_engine import CompressedStateEngine

# Create engine for your application
with CompressedStateEngine("app.wal", durability_mode="safe") as engine:
    engine.put("config:version", "1.0.0")
    version = engine.get("config:version")
```

**TCP Server (Multi-Process):**

```bash
# Terminal 1: Start universal server (KV + queues + streams)
python -m backend.universal_server.cli serve --port 9633

# Terminal 2: Start vector server (vector search)
python -m backend.universal_server.cli serve-vector-v2 --port 9644

# Terminal 3: Test connection
echo '{"cmd":"PING"}' | nc localhost 9633
```

### Option 2: Docker (Single Container)

**Run with Docker:**

```bash
# Create data directory
mkdir -p ~/ahanaflow-data

# Run the container
docker run -d \
  --name ahanaflow \
  -p 9633:9633 \
  -p 9644:9644 \
  -v ~/ahanaflow-data:/data \
  -e AHANAFLOW_API_KEY=your_api_key_here \
  -e AHANAFLOW_DURABILITY_MODE=safe \
  ghcr.io/ahanaai-company/ahanaflow:latest

# Check logs
docker logs -f ahanaflow

# Test connection
echo '{"cmd":"PING"}' | nc localhost 9633
```

**Docker Compose (Multi-Service):**

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  ahanaflow:
    image: ghcr.io/ahanaai-company/ahanaflow:latest
    container_name: ahanaflow
    ports:
      - "9633:9633"
      - "9644:9644"
    volumes:
      - ./data:/data
    environment:
      - AHANAFLOW_API_KEY=${AHANAFLOW_API_KEY}
      - AHANAFLOW_DURABILITY_MODE=safe
      - AHANAFLOW_LOG_LEVEL=INFO
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "9633"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Your application container
  app:
    build: .
    depends_on:
      - ahanaflow
    environment:
      - AHANAFLOW_HOST=ahanaflow
      - AHANAFLOW_PORT=9633
```

Start with:

```bash
docker-compose up -d
```

### Option 3: Kubernetes (Production Cluster)

**Apply Kubernetes Manifests:**

```bash
# Create namespace
kubectl create namespace ahanaflow

# Create secret for API key
kubectl create secret generic ahanaflow-api-key \
  --from-literal=api-key=your_api_key_here \
  -n ahanaflow

# Apply deployment
kubectl apply -f k8s/ahanaflow-deployment.yaml

# Verify deployment
kubectl get pods -n ahanaflow
kubectl logs -f -n ahanaflow -l app=ahanaflow
```

**Kubernetes Deployment YAML:**

Create `k8s/ahanaflow-deployment.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ahanaflow-config
  namespace: ahanaflow
data:
  AHANAFLOW_HOST: "0.0.0.0"
  AHANAFLOW_PORT: "9633"
  AHANAFLOW_VECTOR_PORT: "9644"
  AHANAFLOW_DURABILITY_MODE: "safe"
  AHANAFLOW_LOG_LEVEL: "INFO"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ahanaflow
  namespace: ahanaflow
spec:
  replicas: 1  # Single instance for WAL consistency
  selector:
    matchLabels:
      app: ahanaflow
  template:
    metadata:
      labels:
        app: ahanaflow
    spec:
      containers:
      - name: ahanaflow
        image: ghcr.io/ahanaai-company/ahanaflow:latest
        ports:
        - containerPort: 9633
          name: universal
        - containerPort: 9644
          name: vector
        env:
        - name: AHANAFLOW_API_KEY
          valueFrom:
            secretKeyRef:
              name: ahanaflow-api-key
              key: api-key
        envFrom:
        - configMapRef:
            name: ahanaflow-config
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
        livenessProbe:
          tcpSocket:
            port: 9633
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 9633
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: ahanaflow-data

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ahanaflow-data
  namespace: ahanaflow
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 50Gi
  storageClassName: fast-ssd  # Adjust to your cluster

---
apiVersion: v1
kind: Service
metadata:
  name: ahanaflow
  namespace: ahanaflow
spec:
  selector:
    app: ahanaflow
  ports:
  - name: universal
    port: 9633
    targetPort: 9633
  - name: vector
    port: 9644
    targetPort: 9644
  type: ClusterIP
```

### Option 4: systemd Service (Linux)

Create `/etc/systemd/system/ahanaflow.service`:

```ini
[Unit]
Description=AhanaFlow Compressed State Engine
After=network.target

[Service]
Type=simple
User=ahanaflow
Group=ahanaflow
WorkingDirectory=/opt/ahanaflow
Environment="AHANAFLOW_API_KEY=your_api_key_here"
Environment="AHANAFLOW_DURABILITY_MODE=safe"
ExecStart=/opt/ahanaflow/venv/bin/python -m backend.universal_server.cli serve --port 9633 --host 0.0.0.0
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
# Create user
sudo useradd -r -s /bin/false ahanaflow

# Create directories
sudo mkdir -p /opt/ahanaflow/data
sudo chown ahanaflow:ahanaflow /opt/ahanaflow/data

# Install AhanaFlow
sudo -u ahanaflow python -m venv /opt/ahanaflow/venv
sudo -u ahanaflow /opt/ahanaflow/venv/bin/pip install ahanaflow

# Enable service
sudo systemctl daemon-reload
sudo systemctl enable ahanaflow
sudo systemctl start ahanaflow

# Check status
sudo systemctl status ahanaflow
```

---

## Production Best Practices

### 1. Durability Mode Selection

| Mode | Ops/s | Durability | Use Case |
|------|-------|------------|----------|
| **fast** | 1.57M | Batch flush every 50ms or 16 records | Cache, counters, non-critical state |
| **safe** | 967K | OS-buffered fsync | General production (recommended) |
| **strict** | 770K | Per-record fsync | Financial, audit logs, critical data |

**Recommendation:** Use `safe` for most production deployments. Use `strict` only when data loss is unacceptable (e.g., payment processing).

### 2. Storage Configuration

- **Use SSD:** NVMe SSD provides 10× faster fsync than HDD
- **Separate volumes:** Keep WAL on dedicated disk from application logs
- **Monitor disk space:** Set alerts at 80% capacity
- **Backup regularly:** Copy WAL files to cold storage (they're already compressed)

### 3. High Availability

**Single-Node Reliability:**
- Use `systemd` or Kubernetes with restart policies
- Configure health checks (`PING` command)
- Monitor process uptime

**Future: Multi-Node (v1.2+):**
- Leader-follower replication via WAL streaming
- Automatic failover with Raft consensus
- Geographic redundancy

### 4. Security Hardening

**Enable Authentication:**

```bash
# Generate secure token
TOKEN=$(openssl rand -hex 32)

# Set in environment
export AHANAFLOW_AUTH_ENABLED=true
export AHANAFLOW_AUTH_TOKEN=$TOKEN

# Client must include token in commands
echo '{"cmd":"PING","auth_token":"'$TOKEN'"}' | nc localhost 9633
```

**TLS Encryption (Coming in v1.1):**

```bash
# Generate self-signed cert for testing
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Start with TLS
python -m backend.universal_server.cli serve --tls-cert cert.pem --tls-key key.pem
```

**Network Isolation:**
- Run on private network (10.x.x.x, 172.16.x.x, 192.168.x.x)
- Use firewall rules to restrict access
- Only expose via reverse proxy (nginx) with authentication

### 5. Resource Limits

**Memory:**
- Minimum: 2 GB
- Recommended: 8 GB (for in-memory index + vector HNSW)
- Monitor: `STATS` command shows memory usage

**Disk:**
- WAL grows ~ 584 KB per 20K operations (fast mode)
- Monitor and rotate logs regularly
- Plan for 10× peak daily volume

**CPU:**
- 2 cores minimum
- 4+ cores for high-throughput vector search
- CPU usage scales with request rate

### 6. Monitoring

**Health Check:**

```bash
# Simple ping
echo '{"cmd":"PING"}' | nc localhost 9633

# Get detailed stats
echo '{"cmd":"STATS"}' | nc localhost 9633
```

**Metrics to Track:**
- Request rate (ops/s)
- Request latency (p50, p95, p99)
- WAL file size growth
- Memory usage
- Error rate

**Prometheus Integration (Coming in v1.1):**

```yaml
scrape_configs:
  - job_name: 'ahanaflow'
    static_configs:
      - targets: ['ahanaflow:9635']  # Metrics endpoint
```

---

## Monitoring & Troubleshooting

### Common Issues

**1. "Connection refused" on port 9633**

- **Check if server is running:**
  ```bash
  ps aux | grep universal_server
  docker ps | grep ahanaflow
  kubectl get pods -n ahanaflow
  ```

- **Check if port is open:**
  ```bash
  netstat -tuln | grep 9633
  ```

- **Check firewall:**
  ```bash
  sudo ufw status
  sudo firewall-cmd --list-all
  ```

**2. "Module not found" errors**

- **Verify installation:**
  ```bash
  pip list | grep ahanaflow
  python -c "import backend.state_engine"
  ```

- **Check PYTHONPATH:**
  ```bash
  echo $PYTHONPATH
  export PYTHONPATH=/path/to/ahanaflow:$PYTHONPATH
  ```

**3. Slow performance**

- **Check durability mode:**
  ```bash
  # Should be "safe" or "fast" for production
  echo '{"cmd":"CONFIG","action":"get","key":"durability_mode"}' | nc localhost 9633
  ```

- **Check disk type:**
  ```bash
  df -T  # Should show SSD filesystem
  ```

- **Monitor CPU/memory:**
  ```bash
  top -p $(pgrep -f universal_server)
  ```

**4. WAL corruption after crash**

- **Check integrity:**
  ```bash
  python -m backend.state_engine.cli validate --wal /data/state.wal
  ```

- **Restore from backup:**
  ```bash
  cp /backup/state.wal.2026-04-09 /data/state.wal
  systemctl restart ahanaflow
  ```

### Logs & Debugging

**Enable debug logging:**

```bash
export AHANAFLOW_LOG_LEVEL=DEBUG
python -m backend.universal_server.cli serve --port 9633
```

**View logs:**

```bash
# Docker
docker logs -f ahanaflow

# Kubernetes
kubectl logs -f -n ahanaflow -l app=ahanaflow

# systemd
sudo journalctl -u ahanaflow -f

# File-based
tail -f /data/ahanaflow.log
```

### Getting Help

- **Documentation:** https://www.ahanaflow.com/docs
- **GitHub Issues:** https://github.com/AhanaAI-Company/ahanaflow/issues
- **Email Support:** support@ahanaai.com (paid plans only)
- **Community Discord:** https://discord.gg/ahanaai (coming soon)

---

## Next Steps

1. **[API Key Setup](./API_KEY_SETUP.md)** — Configure commercial license
2. **[API Reference](./API_REFERENCE.md)** — Complete command documentation
3. **[Examples](../examples/)** — Working code samples
4. **[Benchmarks](./BENCHMARKS.md)** — Performance tuning guide

---

🌺 **AhanaFlow — State, Vector Search & Compression in One Runtime**
