from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from backend.common import read_secret


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def _b64url_decode(value: str) -> bytes:
    value += "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value)


def _signing_digest(secret: str) -> bytes:
    return hashlib.sha256(secret.encode("utf-8")).digest()


@dataclass(frozen=True)
class ProprietaryArtifactConfig:
    artifact_id: str
    artifact_version: str
    artifact_url: str
    artifact_sha256: str
    signing_key: str
    master_key: str
    ttl_seconds: int = 900


def load_proprietary_artifact_config() -> ProprietaryArtifactConfig | None:
    artifact_id = os.environ.get("AHANAFLOW_PRO_ARTIFACT_ID", "").strip()
    artifact_version = os.environ.get("AHANAFLOW_PRO_ARTIFACT_VERSION", "").strip()
    artifact_url = os.environ.get("AHANAFLOW_PRO_ARTIFACT_URL", "").strip()
    artifact_sha256 = os.environ.get("AHANAFLOW_PRO_ARTIFACT_SHA256", "").strip().lower()
    signing_key = read_secret("AHANAFLOW_PRO_ARTIFACT_SIGNING_KEY")
    master_key = read_secret("AHANAFLOW_PRO_ARTIFACT_MASTER_KEY")
    ttl_seconds = int(os.environ.get("AHANAFLOW_PRO_ARTIFACT_TTL_SECONDS", "900") or 900)

    if not all((artifact_id, artifact_version, artifact_url, artifact_sha256, signing_key, master_key)):
        return None

    if len(artifact_sha256) != 64 or not all(ch in "0123456789abcdef" for ch in artifact_sha256):
        raise ValueError("AHANAFLOW_PRO_ARTIFACT_SHA256 must be a 64-character hex digest")

    return ProprietaryArtifactConfig(
        artifact_id=artifact_id,
        artifact_version=artifact_version,
        artifact_url=artifact_url,
        artifact_sha256=artifact_sha256,
        signing_key=signing_key,
        master_key=master_key,
        ttl_seconds=max(60, ttl_seconds),
    )


def derive_artifact_fingerprint(config: ProprietaryArtifactConfig, *, customer_id: str, email: str) -> str:
    message = f"fingerprint:{config.artifact_id}:{config.artifact_version}:{customer_id}:{email.strip().lower()}"
    raw = hmac.new(config.master_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()[:12]
    return _b64url_encode(raw)


def derive_artifact_unlock_key(config: ProprietaryArtifactConfig, *, customer_id: str, email: str) -> str:
    message = f"unlock:{config.artifact_id}:{config.artifact_version}:{customer_id}:{email.strip().lower()}"
    raw = hmac.new(config.master_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(raw)


def create_download_grant(
    config: ProprietaryArtifactConfig,
    *,
    customer_id: str,
    email: str,
    tier: str,
    plan: str,
    fingerprint: str,
) -> str:
    payload = {
        "artifact_id": config.artifact_id,
        "artifact_version": config.artifact_version,
        "artifact_sha256": config.artifact_sha256,
        "artifact_url": config.artifact_url,
        "customer_id": customer_id,
        "email": email.strip().lower(),
        "tier": tier,
        "plan": plan,
        "fingerprint": fingerprint,
        "iat": int(time.time()),
        "exp": int(time.time()) + config.ttl_seconds,
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_signing_digest(config.signing_key), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(sig)}"


def decode_download_grant(config: ProprietaryArtifactConfig, token: str) -> dict[str, Any]:
    try:
        payload_b64, sig_b64 = token.split(".", 1)
    except ValueError as exc:
        raise ValueError("Invalid download grant token") from exc

    expected = hmac.new(_signing_digest(config.signing_key), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    actual = _b64url_decode(sig_b64)
    if not hmac.compare_digest(expected, actual):
        raise ValueError("Invalid download grant signature")

    payload = json.loads(_b64url_decode(payload_b64))
    if int(payload.get("exp", 0) or 0) < int(time.time()):
        raise ValueError("Download grant expired")
    return payload


def build_artifact_manifest(
    config: ProprietaryArtifactConfig,
    *,
    customer_id: str,
    email: str,
    tier: str,
    plan: str,
) -> dict[str, Any]:
    fingerprint = derive_artifact_fingerprint(config, customer_id=customer_id, email=email)
    unlock_key = derive_artifact_unlock_key(config, customer_id=customer_id, email=email)
    grant = create_download_grant(
        config,
        customer_id=customer_id,
        email=email,
        tier=tier,
        plan=plan,
        fingerprint=fingerprint,
    )
    return {
        "artifact_id": config.artifact_id,
        "artifact_version": config.artifact_version,
        "artifact_url": config.artifact_url,
        "artifact_sha256": config.artifact_sha256,
        "distribution_mode": "backend-signed-manifest",
        "container_format": "puzzle_auth_aarm",
        "unlock_key": unlock_key,
        "fingerprint": fingerprint,
        "download_grant": grant,
        "download_grant_ttl_seconds": config.ttl_seconds,
    }