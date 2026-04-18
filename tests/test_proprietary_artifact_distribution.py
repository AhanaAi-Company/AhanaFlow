from __future__ import annotations

from fastapi.testclient import TestClient

from backend.stripe_webhook import server as webhook_server
from backend.stripe_webhook.api_key_registry import ApiKeyRegistry


def test_artifacts_manifest_requires_configuration(monkeypatch):
    client = TestClient(webhook_server.app)
    monkeypatch.setattr(webhook_server, "_require_portal_session", lambda token, email: {"email": email})
    monkeypatch.setattr(
        webhook_server,
        "_resolve_customer_and_subscription",
        lambda email: ({"id": "cus_123", "email": email}, {"id": "sub_123"}),
    )
    monkeypatch.setattr(
        webhook_server,
        "_sync_entitlement",
        lambda customer_id, email, subscription: {"tier": "pro", "plan": "pro", "max_api_keys": 1},
    )
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_ID", raising=False)
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_VERSION", raising=False)
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_URL", raising=False)
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_SHA256", raising=False)
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_SIGNING_KEY", raising=False)
    monkeypatch.delenv("AHANAFLOW_PRO_ARTIFACT_MASTER_KEY", raising=False)

    response = client.post("/artifacts/manifest", headers={"x-portal-token": "token"}, json={"email": "buyer@example.com"})

    assert response.status_code == 503
    assert response.json()["detail"] == "Proprietary artifact distribution is not configured"


def test_artifacts_manifest_returns_customer_specific_grant(monkeypatch, tmp_path):
    client = TestClient(webhook_server.app)
    monkeypatch.setattr(webhook_server, "API_KEY_REGISTRY", ApiKeyRegistry(tmp_path / "registry.json"))
    monkeypatch.setattr(webhook_server, "_require_portal_session", lambda token, email: {"email": email})
    monkeypatch.setattr(
        webhook_server,
        "_resolve_customer_and_subscription",
        lambda email: ({"id": "cus_123", "email": email}, {"id": "sub_123"}),
    )
    monkeypatch.setattr(
        webhook_server,
        "_sync_entitlement",
        lambda customer_id, email, subscription: {"tier": "pro", "plan": "pro", "max_api_keys": 1},
    )
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_ID", "ahanaflow-pro-codec")
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_VERSION", "2026.04")
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_URL", "https://private.example/artifact.aarm")
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_SHA256", "b" * 64)
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_SIGNING_KEY", "signing-secret")
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_MASTER_KEY", "master-secret")
    monkeypatch.setenv("AHANAFLOW_PRO_ARTIFACT_TTL_SECONDS", "600")

    response = client.post("/artifacts/manifest", headers={"x-portal-token": "token"}, json={"email": "buyer@example.com"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["artifact"]["artifact_id"] == "ahanaflow-pro-codec"
    assert payload["artifact"]["fingerprint"]
    assert payload["artifact"]["unlock_key"]
    assert payload["artifact_issues"]