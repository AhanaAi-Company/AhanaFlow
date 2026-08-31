from __future__ import annotations

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


def test_read_license_key_canonical_env(monkeypatch):
    monkeypatch.setenv("AHANAFLOW_LICENSE_KEY", "license-jwt")
    monkeypatch.delenv("AHANAFLOW_API_KEY", raising=False)
    assert read_license_key() == "license-jwt"


def test_read_license_key_deprecated_alias(monkeypatch):
    monkeypatch.delenv("AHANAFLOW_LICENSE_KEY", raising=False)
    monkeypatch.setenv("AHANAFLOW_API_KEY", "legacy-key")
    assert read_license_key() == "legacy-key"


def test_read_license_key_prefers_canonical_over_alias(monkeypatch):
    monkeypatch.setenv("AHANAFLOW_LICENSE_KEY", "canonical")
    monkeypatch.setenv("AHANAFLOW_API_KEY", "legacy")
    assert read_license_key() == "canonical"


def test_read_license_key_file_variant(monkeypatch, tmp_path):
    secret_file = tmp_path / "license.txt"
    secret_file.write_text("file-jwt\n", encoding="utf-8")
    monkeypatch.delenv("AHANAFLOW_LICENSE_KEY", raising=False)
    monkeypatch.delenv("AHANAFLOW_API_KEY", raising=False)
    monkeypatch.setenv("AHANAFLOW_LICENSE_KEY_FILE", str(secret_file))
    assert read_license_key() == "file-jwt"
