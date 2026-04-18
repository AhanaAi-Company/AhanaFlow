from __future__ import annotations

import hashlib
import json
import secrets
import time
from pathlib import Path
from threading import RLock
from typing import Any


class ApiKeyRegistry:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = RLock()

    def _default_payload(self) -> dict[str, Any]:
        return {"version": 1, "customers": {}}

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return self._default_payload()
        raw = self._path.read_text(encoding="utf-8").strip()
        if not raw:
            return self._default_payload()
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return self._default_payload()
        payload.setdefault("version", 1)
        payload.setdefault("customers", {})
        return payload

    def _save(self, payload: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self._path)

    def _customer_record(self, payload: dict[str, Any], customer_id: str) -> dict[str, Any]:
        customers = payload.setdefault("customers", {})
        record = customers.setdefault(
            customer_id,
            {
                "customer_id": customer_id,
                "email": "",
                "tier": "pro",
                "plan": "pro",
                "max_api_keys": 1,
                "subscription_id": "",
                "price_id": "",
                "updated_at": 0,
                "portal_access": {},
                "artifact_issues": [],
                "api_keys": [],
            },
        )
        record.setdefault("api_keys", [])
        record.setdefault("portal_access", {})
        record.setdefault("artifact_issues", [])
        return record

    def upsert_entitlement(
        self,
        *,
        customer_id: str,
        email: str,
        tier: str,
        plan: str,
        max_api_keys: int,
        subscription_id: str = "",
        price_id: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            record.update(
                {
                    "email": email.strip().lower(),
                    "tier": tier,
                    "plan": plan,
                    "max_api_keys": int(max_api_keys),
                    "subscription_id": subscription_id,
                    "price_id": price_id,
                    "updated_at": int(time.time()),
                }
            )
            self._save(payload)
            return record

    def get_customer_by_email(self, email: str) -> dict[str, Any] | None:
        normalized = email.strip().lower()
        with self._lock:
            payload = self._load()
            for record in payload.get("customers", {}).values():
                if str(record.get("email", "")).strip().lower() == normalized:
                    return record
        return None

    def get_customer(self, customer_id: str) -> dict[str, Any] | None:
        with self._lock:
            payload = self._load()
            return payload.get("customers", {}).get(customer_id)

    def list_api_keys(self, customer_id: str) -> list[dict[str, Any]]:
        record = self.get_customer(customer_id)
        if not record:
            return []
        keys = []
        for item in record.get("api_keys", []):
            if item.get("revoked_at"):
                continue
            keys.append(
                {
                    "id": item.get("id", ""),
                    "label": item.get("label", "API key"),
                    "preview": item.get("preview", ""),
                    "created_at": item.get("created_at", 0),
                }
            )
        return keys

    def active_api_key_count(self, customer_id: str) -> int:
        return len(self.list_api_keys(customer_id))

    def create_api_key(self, customer_id: str, label: str | None = None) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            active_count = sum(1 for item in record.get("api_keys", []) if not item.get("revoked_at"))
            max_api_keys = int(record.get("max_api_keys", 0) or 0)
            if active_count >= max_api_keys:
                raise ValueError(f"API key limit reached ({active_count}/{max_api_keys})")

            raw_key = "afk_" + secrets.token_urlsafe(24)
            key_id = "key_" + secrets.token_hex(8)
            preview = raw_key[-6:]
            entry = {
                "id": key_id,
                "label": (label or f"API key {active_count + 1}").strip()[:80] or f"API key {active_count + 1}",
                "key_hash": hashlib.sha256(raw_key.encode("utf-8")).hexdigest().lower(),
                "preview": preview,
                "created_at": int(time.time()),
                "revoked_at": 0,
            }
            record.setdefault("api_keys", []).append(entry)
            record["updated_at"] = int(time.time())
            self._save(payload)
            return {
                "id": key_id,
                "label": entry["label"],
                "preview": preview,
                "created_at": entry["created_at"],
                "raw_key": raw_key,
            }

    def revoke_api_key(self, customer_id: str, key_id: str) -> bool:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            for item in record.get("api_keys", []):
                if item.get("id") == key_id and not item.get("revoked_at"):
                    item["revoked_at"] = int(time.time())
                    record["updated_at"] = int(time.time())
                    self._save(payload)
                    return True
        return False

    def set_access_code(self, customer_id: str, code: str, expires_at: int) -> None:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            record["portal_access"] = {
                "code_hash": hashlib.sha256(code.encode("utf-8")).hexdigest().lower(),
                "expires_at": int(expires_at),
                "issued_at": int(time.time()),
            }
            record["updated_at"] = int(time.time())
            self._save(payload)

    def record_artifact_issue(
        self,
        customer_id: str,
        *,
        artifact_id: str,
        artifact_version: str,
        fingerprint: str,
        grant_token: str,
    ) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            entry = {
                "artifact_id": artifact_id,
                "artifact_version": artifact_version,
                "fingerprint": fingerprint,
                "grant_hash": hashlib.sha256(grant_token.encode("utf-8")).hexdigest().lower(),
                "issued_at": int(time.time()),
            }
            record.setdefault("artifact_issues", []).append(entry)
            record["updated_at"] = int(time.time())
            self._save(payload)
            return entry

    def list_artifact_issues(self, customer_id: str) -> list[dict[str, Any]]:
        record = self.get_customer(customer_id)
        if not record:
            return []
        return list(record.get("artifact_issues", []))

    def verify_access_code(self, customer_id: str, code: str) -> bool:
        with self._lock:
            payload = self._load()
            record = self._customer_record(payload, customer_id)
            portal_access = record.get("portal_access", {})
            if not portal_access:
                return False
            if int(portal_access.get("expires_at", 0) or 0) < int(time.time()):
                record["portal_access"] = {}
                self._save(payload)
                return False
            code_hash = hashlib.sha256(code.encode("utf-8")).hexdigest().lower()
            if code_hash != str(portal_access.get("code_hash", "")).lower():
                return False
            record["portal_access"] = {}
            record["updated_at"] = int(time.time())
            self._save(payload)
            return True
