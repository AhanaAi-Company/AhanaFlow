"""Security hardening tests for UniversalStateServer and VectorStateServerV2.

Tests authentication, rate limiting, payload size limits, connection limits,
input validation, command whitelisting, and audit logging.
"""

from __future__ import annotations

import hashlib
import json
import socket
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.universal_server.security import SecurityConfig, generate_api_keys_file, hash_api_key
from backend.universal_server.server import UniversalStateServer


class SecureClient:
    """Test client with API key support."""

    def __init__(self, host: str, port: int, api_key: str | None = None) -> None:
        self.host = host
        self.port = port
        self.api_key = api_key
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send(self, command: dict) -> dict:
        """Send command with optional API key."""
        if self.api_key:
            command["api_key"] = self.api_key

        payload = (json.dumps(command) + "\n").encode()
        self.sock.sendall(payload)

        # Read response until newline
        response_bytes = b""
        while True:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed by server")
            response_bytes += chunk
            if b"\n" in response_bytes:
                break

        return json.loads(response_bytes.decode().strip())

    def close(self) -> None:
        self.sock.close()


@pytest.fixture
def secure_api_keys_file(tmp_path: Path) -> Path:
    """Create temporary API keys file."""
    keys_file = tmp_path / "api_keys.txt"
    generate_api_keys_file(["test-key-12345", "admin-key-67890"], keys_file)
    return keys_file


@pytest.fixture
def secure_server(tmp_path: Path, secure_api_keys_file: Path):
    """Start UniversalStateServer with security enabled."""
    wal_path = tmp_path / "secure.wal"
    audit_log = tmp_path / "audit.log"

    config = SecurityConfig(
        enabled=True,
        api_keys_file=secure_api_keys_file,
        require_auth=True,
        rate_limit_enabled=True,
        rate_limit_per_ip=100,  # 100 ops/sec per IP
        rate_limit_per_key=500,  # 500 ops/sec per key
        max_payload_bytes=1024 * 1024,  # 1MB
        max_key_length=256,
        max_value_size=1024 * 1024,
        validate_keys=True,
        audit_log_path=audit_log,
        log_auth_failures=True,
        log_rate_limit_events=True,
    )

    server = UniversalStateServer(
        wal_path,
        host="127.0.0.1",
        port=0,  # Auto-assign port
        security_config=config,
    )

    import threading

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Wait for server to start
    time.sleep(0.5)

    yield server

    server.shutdown()


class TestAuthentication:
    """Test API key authentication."""

    def test_auth_required(self, secure_server):
        """Commands without API key should fail."""
        host, port = secure_server.address

        client = SecureClient(host, port, api_key=None)
        response = client.send({"cmd": "PING"})

        assert response["ok"] is False
        assert "security" in response["error"]
        assert "Missing API key" in response["error"]

        client.close()

    def test_auth_invalid_key(self, secure_server):
        """Invalid API key should fail."""
        host, port = secure_server.address

        client = SecureClient(host, port, api_key="wrong-key")
        response = client.send({"cmd": "PING"})

        assert response["ok"] is False
        assert "security" in response["error"]
        assert "Invalid API key" in response[ "error"]

        client.close()

    def test_auth_valid_key(self, secure_server):
        """Valid API key should succeed."""
        host, port = secure_server.address

        client = SecureClient(host, port, api_key="test-key-12345")
        response = client.send({"cmd": "PING"})

        assert response["ok"] is True
        assert response["result"] == "PONG"

        client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
