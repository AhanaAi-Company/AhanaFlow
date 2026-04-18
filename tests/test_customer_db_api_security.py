from __future__ import annotations

from fastapi.testclient import TestClient

from backend.customer_db import api as customer_api


class _DummyCustomer:
    def __init__(self, customer_id: str) -> None:
        self.customer_id = customer_id

    def to_dict(self) -> dict[str, str]:
        return {"customer_id": self.customer_id, "email": "buyer@example.com"}


def test_customer_db_api_requires_admin_key(monkeypatch):
    client = TestClient(customer_api.app)
    monkeypatch.delenv("AHANAFLOW_ADMIN_API_KEY", raising=False)
    monkeypatch.delenv("AHANAFLOW_ADMIN_API_KEY_PREV", raising=False)
    monkeypatch.delenv("AHANAFLOW_ALLOW_INSECURE_ADMIN_API", raising=False)

    response = client.get("/customers/cus_123")

    assert response.status_code == 503
    assert response.json()["detail"] == "Admin API key is not configured"


def test_customer_db_api_rejects_invalid_admin_key(monkeypatch):
    client = TestClient(customer_api.app)
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY", "admin-secret")

    response = client.get("/customers/cus_123", headers={"x-admin-api-key": "wrong"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid admin API key"


def test_customer_db_api_accepts_valid_admin_key(monkeypatch):
    client = TestClient(customer_api.app)
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY", "admin-secret")
    monkeypatch.setattr(customer_api.db, "connect", lambda: None)
    monkeypatch.setattr(customer_api.db, "get_customer", lambda customer_id: _DummyCustomer(customer_id))

    response = client.get("/customers/cus_123", headers={"x-admin-api-key": "admin-secret"})

    assert response.status_code == 200
    assert response.json()["customer_id"] == "cus_123"


def test_customer_db_api_accepts_admin_key_file(monkeypatch, tmp_path):
    client = TestClient(customer_api.app)
    secret_file = tmp_path / "admin.key"
    secret_file.write_text("admin-secret\n", encoding="utf-8")
    monkeypatch.delenv("AHANAFLOW_ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY_FILE", str(secret_file))
    monkeypatch.setattr(customer_api.db, "connect", lambda: None)
    monkeypatch.setattr(customer_api.db, "get_customer", lambda customer_id: _DummyCustomer(customer_id))

    response = client.get("/customers/cus_123", headers={"x-admin-api-key": "admin-secret"})

    assert response.status_code == 200
    assert response.json()["customer_id"] == "cus_123"