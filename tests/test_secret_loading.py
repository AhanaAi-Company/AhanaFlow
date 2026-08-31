from __future__ import annotations

from pathlib import Path

from backend.common import read_license_key, read_secret


def test_read_secret_prefers_direct_env(monkeypatch, tmp_path):
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY", "from-env")
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY_FILE", str(secret_file))

    assert read_secret("AHANAFLOW_ADMIN_API_KEY") == "from-env"


def test_read_secret_supports_file_env(monkeypatch, tmp_path):
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("from-file\n", encoding="utf-8")
    monkeypatch.delenv("AHANAFLOW_ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("AHANAFLOW_ADMIN_API_KEY_FILE", str(secret_file))

    assert read_secret("AHANAFLOW_ADMIN_API_KEY") == "from-file"


def test_read_license_key_canonical_name(monkeypatch):
    monkeypatch.setenv("AHANAFLOW_LICENSE_KEY", "canonical")
    monkeypatch.delenv("LICENSE_KEY", raising=False)
    monkeypatch.delenv("AHANAFLOW_API_KEY", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    assert read_license_key() == "canonical"


def test_read_license_key_accepts_legacy_aliases(monkeypatch):
    monkeypatch.delenv("AHANAFLOW_LICENSE_KEY", raising=False)
    monkeypatch.delenv("LICENSE_KEY", raising=False)
    monkeypatch.setenv("AHANAFLOW_API_KEY", "legacy-api")
    monkeypatch.delenv("API_KEY", raising=False)
    assert read_license_key() == "legacy-api"


def test_compose_uses_backend_starter_not_missing_v1():
    root = Path(__file__).resolve().parents[1]
    compose = root.joinpath("docker-compose.yml").read_text(encoding="utf-8")
    dockerfile = root.joinpath("Dockerfile").read_text(encoding="utf-8")
    assert "v1_1.starter" not in compose
    assert "./deploy/secrets" not in compose
    assert "python -m backend.universal_server.cli" in dockerfile
