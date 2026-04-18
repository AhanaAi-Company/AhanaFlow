from __future__ import annotations

from backend.common import read_secret


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