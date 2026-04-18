from __future__ import annotations

import os
from pathlib import Path


def read_secret(name: str, default: str = "") -> str:
    """Read a secret from NAME or NAME_FILE.

    Direct env vars are supported for compatibility. For production, prefer the
    corresponding ``*_FILE`` value so the secret can be mounted from Docker,
    Kubernetes, or a host-level secret manager without writing it into the repo.
    """
    direct = os.environ.get(name)
    if direct is not None and direct.strip():
        return direct.strip()

    file_path = os.environ.get(f"{name}_FILE", "").strip()
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()

    return default


def secret_is_configured(name: str) -> bool:
    return bool(read_secret(name, ""))