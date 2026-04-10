"""Security middleware for UniversalStateServer and VectorStateServerV2.

Provides:
- API key authentication (SHA-256 hashed)
- Rate limiting (sliding window, per-IP and per-key)
- Payload size limits
- Connection limits per IP
- Input validation (key length, character whitelist)
- Command whitelisting
- Security audit logging
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any


@dataclass
class SecurityConfig:
    """Security configuration for database servers."""

    # Authentication
    enabled: bool = True
    api_keys_file: str | Path | None = None  # Path to API keys file (one SHA-256 hash per line)
    require_auth: bool = True  # If False, auth is optional (dev mode)

    # Rate limiting (operations per second)
    rate_limit_enabled: bool = True
    rate_limit_per_ip: int = 1000  # Max ops/sec per IP
    rate_limit_per_key: int = 10000  # Max ops/sec per API key
    rate_limit_window_sec: float = 1.0  # Sliding window size

    # Payload limits
    max_payload_bytes: int = 10 * 1024 * 1024  # 10MB default
    max_key_length: int = 1024  # Max key name length
    max_value_size: int = 10 * 1024 * 1024  # 10MB max value

    # Connection limits
    max_connections_per_ip: int = 100
    max_connections_total: int = 10000

    # Input validation
    validate_keys: bool = True
    allowed_key_chars: str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-:."

    # Command whitelisting
    command_whitelist: set[str] | None = None  # None = allow all, otherwise only listed commands

    # Audit logging
    audit_log_path: str | Path | None = None
    log_all_commands: bool = False  # If True, log every command (verbose)
    log_auth_failures: bool = True
    log_rate_limit_events: bool = True


class SecurityError(Exception):
    """Base class for security-related errors."""

    pass


class AuthenticationError(SecurityError):
    """Authentication failed."""

    pass


class RateLimitError(SecurityError):
    """Rate limit exceeded."""

    pass


class ValidationError(SecurityError):
    """Input validation failed."""

    pass


class SecurityMiddleware:
    """Security enforcement layer for database servers."""

    def __init__(self, config: SecurityConfig | None = None) -> None:
        self.config = config or SecurityConfig()
        self._api_keys: set[str] = set()
        self._rate_limits: dict[str, list[float]] = defaultdict(list)  # key -> [timestamps]
        self._connections_per_ip: dict[str, int] = defaultdict(int)
        self._total_connections = 0
        self._lock = Lock()
        self._audit_file = None

        # Load API keys
        if self.config.api_keys_file:
            self._load_api_keys(self.config.api_keys_file)

        # Open audit log
        if self.config.audit_log_path:
            self._audit_file = open(self.config.audit_log_path, "a", buffering=1)

    def _load_api_keys(self, path: str | Path) -> None:
        """Load SHA-256 hashed API keys from file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"API keys file not found: {path}")

        raw = path.read_text(encoding="utf-8")
        stripped = raw.lstrip()
        if stripped.startswith("{"):
            payload = json.loads(raw)
            customers = payload.get("customers", {}) if isinstance(payload, dict) else {}
            for record in customers.values():
                for item in record.get("api_keys", []):
                    key_hash = str(item.get("key_hash", "")).lower()
                    if item.get("revoked_at"):
                        continue
                    if len(key_hash) == 64 and all(c in "0123456789abcdef" for c in key_hash):
                        self._api_keys.add(key_hash)
            return

        for line in raw.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                if len(line) == 64 and all(c in "0123456789abcdef" for c in line.lower()):
                    self._api_keys.add(line.lower())

    def _audit_log(self, event: str, **kwargs: Any) -> None:
        """Write security event to audit log."""
        if not self._audit_file:
            return

        entry = {
            "timestamp": time.time(),
            "event": event,
            **kwargs,
        }
        self._audit_file.write(json.dumps(entry) + "\n")

    def authenticate(self, client_ip: str, api_key: str | None) -> None:
        """Verify API key authentication.

        Args:
            client_ip: Client IP address
            api_key: Raw API key provided by client (will be hashed)

        Raises:
            AuthenticationError: If authentication fails
        """
        if not self.config.enabled or not self.config.require_auth:
            return  # Auth disabled or optional

        if not api_key:
            self._audit_log("auth_failure", reason="missing_key", ip=client_ip)
            raise AuthenticationError("Missing API key")

        # Hash the provided key
        key_hash = hashlib.sha256(api_key.encode()).hexdigest().lower()

        if key_hash not in self._api_keys:
            if self.config.log_auth_failures:
                self._audit_log("auth_failure", reason="invalid_key", ip=client_ip, key_hash=key_hash[:16])
            raise AuthenticationError("Invalid API key")

        # Success
        if self.config.log_all_commands:
            self._audit_log("auth_success", ip=client_ip, key_hash=key_hash[:16])

    def check_rate_limit(self, identifier: str, limit: int) -> None:
        """Check if rate limit exceeded for identifier.

        Args:
            identifier: IP address or API key hash
            limit: Max operations per second

        Raises:
            RateLimitError: If rate limit exceeded
        """
        if not self.config.rate_limit_enabled:
            return

        now = time.time()
        window_start = now - self.config.rate_limit_window_sec

        with self._lock:
            # Clean old timestamps
            timestamps = self._rate_limits[identifier]
            self._rate_limits[identifier] = [ts for ts in timestamps if ts > window_start]

            # Check limit
            if len(self._rate_limits[identifier]) >= limit:
                if self.config.log_rate_limit_events:
                    self._audit_log(
                        "rate_limit_exceeded",
                        identifier=identifier[:16],
                        limit=limit,
                        current=len(self._rate_limits[identifier]),
                    )
                raise RateLimitError(f"Rate limit exceeded: {limit} ops/sec")

            # Record this operation
            self._rate_limits[identifier].append(now)

    def check_connection_limit(self, client_ip: str) -> None:
        """Check if connection limits exceeded.

        Args:
            client_ip: Client IP address

        Raises:
            SecurityError: If connection limit exceeded
        """
        with self._lock:
            # Check per-IP limit
            if self._connections_per_ip[client_ip] >= self.config.max_connections_per_ip:
                raise SecurityError(f"Too many connections from IP: {client_ip}")

            # Check total limit
            if self._total_connections >= self.config.max_connections_total:
                raise SecurityError("Server connection limit reached")

    def register_connection(self, client_ip: str) -> None:
        """Register a new connection."""
        with self._lock:
            self._connections_per_ip[client_ip] += 1
            self._total_connections += 1

    def unregister_connection(self, client_ip: str) -> None:
        """Unregister a closed connection."""
        with self._lock:
            if self._connections_per_ip[client_ip] > 0:
                self._connections_per_ip[client_ip] -= 1
            if self._total_connections > 0:
                self._total_connections -= 1

    def validate_payload_size(self, payload: bytes) -> None:
        """Check if payload exceeds size limit.

        Args:
            payload: Raw payload bytes

        Raises:
            ValidationError: If payload too large
        """
        if len(payload) > self.config.max_payload_bytes:
            raise ValidationError(
                f"Payload too large: {len(payload)} bytes (limit: {self.config.max_payload_bytes})"
            )

    def validate_key(self, key: str) -> None:
        """Validate key name format.

        Args:
            key: Key name to validate

        Raises:
            ValidationError: If key invalid
        """
        if not self.config.validate_keys:
            return

        # Check length
        if len(key) > self.config.max_key_length:
            raise ValidationError(f"Key too long: {len(key)} (limit: {self.config.max_key_length})")

        if not key:
            raise ValidationError("Key cannot be empty")

        # Check characters
        if self.config.allowed_key_chars:
            invalid_chars = set(key) - set(self.config.allowed_key_chars)
            if invalid_chars:
                raise ValidationError(f"Invalid characters in key: {invalid_chars}")

    def validate_value_size(self, value: Any) -> None:
        """Validate value size.

        Args:
            value: Value to check

        Raises:
            ValidationError: If value too large
        """
        # Estimate size via JSON serialization
        value_json = json.dumps(value)
        if len(value_json) > self.config.max_value_size:
            raise ValidationError(
                f"Value too large: {len(value_json)} bytes (limit: {self.config.max_value_size})"
            )

    def validate_command(self, command: str) -> None:
        """Check if command is whitelisted.

        Args:
            command: Command name (uppercase)

        Raises:
            ValidationError: If command not allowed
        """
        if self.config.command_whitelist and command not in self.config.command_whitelist:
            raise ValidationError(f"Command not allowed: {command}")

    def close(self) -> None:
        """Close audit log file."""
        if self._audit_file:
            self._audit_file.close()
            self._audit_file = None


def hash_api_key(raw_key: str) -> str:
    """Generate SHA-256 hash of API key for storage.

    Example:
        >>> hash_api_key("my-secret-key-12345")
        'a1b2c3...'  # 64-char hex string
    """
    return hashlib.sha256(raw_key.encode()).hexdigest().lower()


def generate_api_keys_file(keys: list[str], output_path: str | Path) -> None:
    """Generate API keys file with SHA-256 hashes.

    Args:
        keys: List of raw API keys
        output_path: Path to write hashed keys

    Example:
        >>> generate_api_keys_file(
        ...     ["dev-key-12345", "prod-key-67890"],
        ...     "api_keys.txt"
        ... )
    """
    path = Path(output_path)
    with open(path, "w") as f:
        f.write("# API Keys (SHA-256 hashed)\n")
        f.write("# Generated: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
        for key in keys:
            key_hash = hash_api_key(key)
            f.write(f"{key_hash}\n")
