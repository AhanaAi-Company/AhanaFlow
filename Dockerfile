# AhanaAI Event Streams — Universal State & Vector Servers
# Multi-stage production container
# Build:   docker build -t ahana-event-streams .
# Run:     docker run -p 9633:9633 -p 9644:9644 ahana-event-streams

# ─── Stage 1: dependency builder ─────────────────────────────────────────────
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements if exists (servers use pure Python stdlib - no dependencies needed)
COPY requirements.txt* ./
RUN mkdir -p /install && \
    if [ -f requirements.txt ]; then \
        grep -v "^#" requirements.txt | grep -v "^$" > /tmp/filtered_requirements.txt || true; \
        if [ -s /tmp/filtered_requirements.txt ]; then \
            pip install --no-cache-dir --prefix=/install -r /tmp/filtered_requirements.txt; \
        fi \
    fi

# ─── Stage 2: runtime image ───────────────────────────────────────────────────
FROM python:3.13-slim

LABEL org.opencontainers.image.title="AhanaAI Event Streams" \
      org.opencontainers.image.description="UniversalStateServer (KV/cache/queue/stream) + VectorStateServerV2 (vector search)" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="AhanaAI" \
      org.opencontainers.image.url="https://ahanaeventstream.com" \
      org.opencontainers.image.source="https://github.com/AhanaAI-Company" \
      org.opencontainers.image.licenses="Proprietary"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Non-root user for security
RUN useradd -r -u 1000 -m -s /sbin/nologin appuser

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy backend application code with correct ownership
COPY --chown=appuser:appuser backend/           backend/

# Data directory for WAL files
RUN mkdir -p /data /data/universal /data/vector && \
    chown -R appuser:appuser /data

# Health check script
COPY --chown=appuser:appuser <<'EOF' /app/healthcheck.py
#!/usr/bin/env python3
import socket
import json
import sys

def check_server(port, name):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect(("127.0.0.1", port))
        
        # Send PING or GET command
        cmd = json.dumps({"cmd": "GET", "key": "_health"}) + "\n"
        sock.sendall(cmd.encode())
        
        response = sock.recv(4096).decode()
        sock.close()
        
        # Any response means server is alive
        if response:
            print(f"✓ {name} port {port} responding")
            return True
        return False
    except Exception as e:
        print(f"✗ {name} port {port} failed: {e}")
        return False

if __name__ == "__main__":
    universal_ok = check_server(9633, "UniversalStateServer")
    vector_ok = check_server(9644, "VectorStateServerV2")
    
    if universal_ok and vector_ok:
        sys.exit(0)
    else:
        sys.exit(1)
EOF

RUN chmod +x /app/healthcheck.py

# Switch to non-root user
USER appuser

# Expose both server ports + webhook server
EXPOSE 9633 9644 8090

# Supervisor script to run both servers
COPY --chown=appuser:appuser <<'EOF' /app/run_servers.sh
#!/bin/bash
set -e

echo "Starting AhanaAI Event Streams servers..."
echo "UniversalStateServer: ${UNIVERSAL_HOST}:${UNIVERSAL_PORT}"
echo "VectorStateServerV2:  ${VECTOR_HOST}:${VECTOR_PORT}"
echo "WAL paths: ${UNIVERSAL_WAL}, ${VECTOR_WAL}"

# Start UniversalStateServer in background
python -m backend.universal_server.cli serve \
    --wal "${UNIVERSAL_WAL}" \
    --host "${UNIVERSAL_HOST}" \
    --port "${UNIVERSAL_PORT}" &

UNIVERSAL_PID=$!
echo "UniversalStateServer started (PID $UNIVERSAL_PID)"

# Start VectorStateServerV2 in background
python -m backend.universal_server.cli serve-vector-v2 \
    --wal "${VECTOR_WAL}" \
    --host "${VECTOR_HOST}" \
    --port "${VECTOR_PORT}" &

VECTOR_PID=$!
echo "VectorStateServerV2 started (PID $VECTOR_PID)"

# Wait for both processes
wait $UNIVERSAL_PID $VECTOR_PID
EOF

RUN chmod +x /app/run_servers.sh

CMD ["/bin/bash", "/app/run_servers.sh"]
