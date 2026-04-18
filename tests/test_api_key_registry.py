from __future__ import annotations

import base64
import json

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

from backend.stripe_webhook.api_key_registry import ApiKeyRegistry
from backend.stripe_webhook.license_keys import generate_license_key
from backend.universal_server.security import (
    AuthenticationError,
    SecurityConfig,
    SecurityMiddleware,
    hash_api_key,
    seal_security_policy,
)


def test_registry_enforces_api_key_limit(tmp_path):
    registry = ApiKeyRegistry(tmp_path / "registry.json")
    registry.upsert_entitlement(
        customer_id="cus_123",
        email="buyer@example.com",
        tier="pro",
        plan="team",
        max_api_keys=3,
        subscription_id="sub_123",
        price_id="price_team",
    )

    first = registry.create_api_key("cus_123", "primary")
    second = registry.create_api_key("cus_123", "secondary")
    third = registry.create_api_key("cus_123", "tertiary")

    assert first["raw_key"].startswith("afk_")
    assert len(registry.list_api_keys("cus_123")) == 3

    with pytest.raises(ValueError, match="API key limit reached"):
        registry.create_api_key("cus_123", "overflow")

    assert registry.revoke_api_key("cus_123", second["id"]) is True
    replacement = registry.create_api_key("cus_123", "replacement")
    assert replacement["raw_key"].startswith("afk_")


def test_security_middleware_loads_structured_registry(tmp_path):
    registry = ApiKeyRegistry(tmp_path / "registry.json")
    registry.upsert_entitlement(
        customer_id="cus_999",
        email="owner@example.com",
        tier="pro",
        plan="pro",
        max_api_keys=1,
    )
    issued = registry.create_api_key("cus_999", "default")

    security = SecurityMiddleware(SecurityConfig(api_keys_file=tmp_path / "registry.json"))
    security.authenticate("127.0.0.1", issued["raw_key"])

    registry.revoke_api_key("cus_999", issued["id"])
    security = SecurityMiddleware(SecurityConfig(api_keys_file=tmp_path / "registry.json"))
    with pytest.raises(AuthenticationError):
        security.authenticate("127.0.0.1", issued["raw_key"])


def test_generate_license_key_includes_extra_claims():
    private_key = Ed25519PrivateKey.generate()
    private_key_b64 = base64.b64encode(
        private_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    ).decode("utf-8")

    token = generate_license_key(
        private_key_b64,
        "cus_abc",
        tier="enterprise",
        days=30,
        extra_claims={"plan": "enterprise", "max_api_keys": 10},
    )

    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64))

    assert payload["tier"] == "enterprise"
    assert payload["plan"] == "enterprise"
    assert payload["max_api_keys"] == 10


def test_registry_access_code_round_trip(tmp_path):
    registry = ApiKeyRegistry(tmp_path / "registry.json")
    registry.upsert_entitlement(
        customer_id="cus_code",
        email="owner@example.com",
        tier="pro",
        plan="pro",
        max_api_keys=1,
    )
    registry.set_access_code("cus_code", "123456", expires_at=4_102_444_800)

    assert registry.verify_access_code("cus_code", "999999") is False
    assert registry.verify_access_code("cus_code", "123456") is True
    assert registry.verify_access_code("cus_code", "123456") is False


def test_security_middleware_loads_sealed_policy(tmp_path):
    encryption_key = "RnBvN1ZQNWpqY0xHRUh0Q2xtUnJ3RUpzclN3dm9BblA4Q0ZsSE1sQmxwUT0="
    issued_hash = hash_api_key("sealed-key-123")
    sealed_path = tmp_path / "security.policy"
    sealed_path.write_bytes(
        seal_security_policy(
            {
                "api_key_hashes": [issued_hash],
                "require_auth": True,
                "command_whitelist": ["PING", "GET"],
                "rate_limit_per_ip": 25,
                "rate_limit_per_key": 50,
            },
            encryption_key,
        )
    )

    security = SecurityMiddleware(
        SecurityConfig(
            sealed_policy_file=sealed_path,
            sealed_policy_key=encryption_key,
        )
    )

    security.authenticate("127.0.0.1", "sealed-key-123")
    assert security.config.command_whitelist == {"PING", "GET"}
    assert security.config.rate_limit_per_ip == 25
    assert security.config.rate_limit_per_key == 50