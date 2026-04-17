# Copyright 2026 AhanaAI. All rights reserved.
"""
AhanaFlow Stripe Webhook Server
================================
Handles Stripe subscription events and issues Ed25519-signed JWT license keys.

Deploy this on api.ahanazip.com/webhooks (separate from the AhanaFlow servers).

Environment variables required (set in .env.production or systemd unit):
  STRIPE_SECRET_KEY          — sk_live_... (Stripe secret key)
  STRIPE_WEBHOOK_SECRET      — whsec_... (from Stripe dashboard → Webhooks)
  AHANAFLOW_SIGNING_KEY      — Base64-encoded Ed25519 private key (PKCS8 DER)
                               Generate with: python -m ahana_codec.keygen generate-keypair
  SMTP_HOST                  — SMTP server hostname (e.g. smtp.sendgrid.net)
  SMTP_PORT                  — SMTP port (587 for TLS, 465 for SSL)
  SMTP_USER                  — SMTP username
  SMTP_PASS                  — SMTP password / API key
  FROM_EMAIL                 — Sender address (e.g. licenses@ahanazip.com)
  LICENSE_PORTAL_BASE_URL    — Base URL for license retrieval (e.g. https://api.ahanazip.com)

Optional:
  LICENSE_DAYS_MONTHLY       — Days to issue for monthly plan (default: 32)
  LICENSE_DAYS_ANNUAL        — Days to issue for annual plan (default: 370)
  LOG_LEVEL                  — DEBUG / INFO / WARNING (default: INFO)

Stripe events handled:
  customer.subscription.created   → issue new license
  invoice.payment_succeeded       → renew license (reissue fresh JWT)
  customer.subscription.deleted   → let current JWT expire naturally

Run:
  uvicorn stripe_webhook.server:app --host 0.0.0.0 --port 8090
"""

from __future__ import annotations

import json
import base64
import hashlib
import hmac
import logging
import os
import secrets
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

import stripe
from fastapi import FastAPI, Request, Response, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.ahana_codec.keygen import generate_license_key
from backend.stripe_webhook.api_key_registry import ApiKeyRegistry
from backend.stripe_webhook.email_templates import license_issued, license_renewed, portal_access_code
from backend.customer_db import CustomerDatabaseEngine, Customer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("ahanaflow.webhook")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
AHANAFLOW_SIGNING_KEY = os.environ.get("AHANAFLOW_SIGNING_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "licenses@ahanazip.com")
LICENSE_PORTAL_BASE_URL = os.environ.get("LICENSE_PORTAL_BASE_URL", "https://api.ahanazip.com")
LICENSE_DAYS_MONTHLY = int(os.environ.get("LICENSE_DAYS_MONTHLY", 32))
LICENSE_DAYS_ANNUAL = int(os.environ.get("LICENSE_DAYS_ANNUAL", 370))
STRIPE_PRICE_PRO_MONTHLY = os.environ.get("STRIPE_PRICE_PRO_MONTHLY", "").strip()
STRIPE_PRICE_TEAM_MONTHLY = os.environ.get("STRIPE_PRICE_TEAM_MONTHLY", "").strip()
STRIPE_PRICE_ENTERPRISE_MONTHLY = os.environ.get("STRIPE_PRICE_ENTERPRISE_MONTHLY", "").strip()
AHANAFLOW_API_KEY_REGISTRY_PATH = os.environ.get(
    "AHANAFLOW_API_KEY_REGISTRY_PATH",
    "/data/ahanaflow/api_key_registry.json",
).strip()
DEFAULT_CHECKOUT_SUCCESS_URL = os.environ.get("STRIPE_CHECKOUT_SUCCESS_URL", "https://www.ahanaflow.com/#pricing").strip()
DEFAULT_CHECKOUT_CANCEL_URL = os.environ.get("STRIPE_CHECKOUT_CANCEL_URL", "https://www.ahanaflow.com/#pricing").strip()
PORTAL_CODE_TTL_SECONDS = int(os.environ.get("PORTAL_CODE_TTL_SECONDS", 600))
PORTAL_SESSION_TTL_SECONDS = int(os.environ.get("PORTAL_SESSION_TTL_SECONDS", 3600))

stripe.api_key = STRIPE_SECRET_KEY
API_KEY_REGISTRY = ApiKeyRegistry(AHANAFLOW_API_KEY_REGISTRY_PATH)

# ---------------------------------------------------------------------------
# Customer DB (UniversalStateServer — same instance on port 9633)
# ---------------------------------------------------------------------------
_AHANAFLOW_HOST = os.environ.get("AHANAFLOW_HOST", "127.0.0.1")
_AHANAFLOW_PORT = int(os.environ.get("AHANAFLOW_PORT", "9633"))


def _get_customer_db() -> Optional[CustomerDatabaseEngine]:
    """Open a fresh CustomerDatabaseEngine connection (short-lived per call)."""
    try:
        cdb = CustomerDatabaseEngine(host=_AHANAFLOW_HOST, port=_AHANAFLOW_PORT)
        cdb.connect()
        return cdb
    except Exception as exc:  # noqa: BLE001
        log.warning("CustomerDB unavailable (%s:%s): %s", _AHANAFLOW_HOST, _AHANAFLOW_PORT, exc)
        return None


# Module-level reference — used by _sync_entitlement and admin routes.
customer_db: Optional[CustomerDatabaseEngine] = _get_customer_db()

# ---------------------------------------------------------------------------
# Service API key — protects public-facing routes (e.g. /v1/leads)
# Stored in UniversalStateServer so rotation requires no container restart.
# Env var AHANAFLOW_SERVICE_API_KEY seeds the key on first boot.
# ---------------------------------------------------------------------------
_SERVICE_KEY_BOOT = os.environ.get("AHANAFLOW_SERVICE_API_KEY", "").strip()
_SERVICE_KEY_USS_CURRENT = "service_key:current"
_SERVICE_KEY_USS_PREV = "service_key:prev"


def _uss_get(key: str) -> Optional[str]:
    """Read a single key from UniversalStateServer (best-effort)."""
    try:
        cdb = CustomerDatabaseEngine(host=_AHANAFLOW_HOST, port=_AHANAFLOW_PORT)
        cdb.connect()
        resp = cdb._send({"cmd": "GET", "key": key})
        cdb.close()
        return resp.get("result")
    except Exception:
        return None


def _uss_set(key: str, value: str) -> None:
    """Write a single key to UniversalStateServer (best-effort)."""
    try:
        cdb = CustomerDatabaseEngine(host=_AHANAFLOW_HOST, port=_AHANAFLOW_PORT)
        cdb.connect()
        cdb._send({"cmd": "SET", "key": key, "value": value})
        cdb.close()
    except Exception:
        pass


def _bootstrap_service_key() -> None:
    """On startup, seed the service key from env if USS has none."""
    if not _SERVICE_KEY_BOOT:
        return
    existing = _uss_get(_SERVICE_KEY_USS_CURRENT)
    if not existing:
        _uss_set(_SERVICE_KEY_USS_CURRENT, _SERVICE_KEY_BOOT)
        log.info("Service API key seeded from env var into USS")


def _valid_service_keys() -> set[str]:
    """Return set of currently valid service keys (current + prev).

    USS keys (current + prev) are the authoritative source of truth.
    The boot env-var key is a last-resort fallback used only when USS
    is completely unreachable (e.g. first boot before seeding completes).
    """
    keys: set[str] = set()
    current = _uss_get(_SERVICE_KEY_USS_CURRENT)
    if current:
        keys.add(current)
    prev = _uss_get(_SERVICE_KEY_USS_PREV)
    if prev:
        keys.add(prev)
    # Fallback: use env-var key only when USS has NO keys at all
    if not keys and _SERVICE_KEY_BOOT:
        keys.add(_SERVICE_KEY_BOOT)
    return keys


def _require_service_key(x_service_key: str | None = Header(None)) -> None:
    """FastAPI dependency — rejects requests missing a valid service key."""
    valid = _valid_service_keys()
    if not valid:
        # No keys configured at all — open access (dev mode)
        return
    if not x_service_key or x_service_key not in valid:
        raise HTTPException(status_code=401, detail="Invalid or missing service API key")


PLAN_MAX_API_KEYS = {
    "pro": 1,
    "team": 3,
    "enterprise": 10,
}

PLAN_LICENSE_TIER = {
    "pro": "pro",
    "team": "pro",
    "enterprise": "enterprise",
}


def _cors_origins() -> list[str]:
    raw = os.environ.get(
        "CORS_ORIGIN",
        "https://ahanazip.com,https://ahanaflow.com,https://www.ahanaflow.com",
    )
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


class CheckoutRequest(BaseModel):
    priceId: str = Field(min_length=1)
    email: str = Field(min_length=3)
    successUrl: str | None = None
    cancelUrl: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckoutResponse(BaseModel):
    checkoutUrl: str
    sessionId: str
    status: str
    mode: str


class ApiKeyCreateRequest(BaseModel):
    email: str = Field(min_length=3)
    label: str | None = None


class ApiKeyRevokeRequest(BaseModel):
    email: str = Field(min_length=3)
    keyId: str = Field(min_length=3)


class PortalAccessCodeRequest(BaseModel):
    email: str = Field(min_length=3)


class PortalAccessCodeVerifyRequest(BaseModel):
    email: str = Field(min_length=3)
    code: str = Field(min_length=4, max_length=12)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AhanaFlow License Webhook",
    version="1.0.0",
    docs_url=None,   # no public Swagger
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _on_startup() -> None:
    """Seed the service key into USS after the server (and USS) are fully ready."""
    _bootstrap_service_key()



def _tier_for_price_id(price_id: str | None) -> str:
    return PLAN_LICENSE_TIER.get(_plan_for_price_id(price_id), "pro")


def _plan_for_price_id(price_id: str | None) -> str:
    normalized = (price_id or "").strip()
    if normalized == STRIPE_PRICE_TEAM_MONTHLY:
        return "team"
    if normalized == STRIPE_PRICE_ENTERPRISE_MONTHLY:
        return "enterprise"
    return "pro"


def _max_api_keys_for_plan(plan: str) -> int:
    return int(PLAN_MAX_API_KEYS.get(plan, 1))


def _subscription_price_id(subscription: Any) -> str:
    try:
        return str(subscription["items"]["data"][0]["price"]["id"])
    except (KeyError, IndexError, TypeError):
        return ""


def _tier_for_subscription(subscription: Any) -> str:
    return _tier_for_price_id(_subscription_price_id(subscription))


def _plan_for_subscription(subscription: Any) -> str:
    return _plan_for_price_id(_subscription_price_id(subscription))


def _allowed_checkout_prices() -> set[str]:
    return {
        price_id
        for price_id in (STRIPE_PRICE_PRO_MONTHLY, STRIPE_PRICE_TEAM_MONTHLY, STRIPE_PRICE_ENTERPRISE_MONTHLY)
        if price_id
    }


def _sync_entitlement(customer_id: str, customer_email: str, subscription: Any) -> dict[str, Any]:
    plan = _plan_for_subscription(subscription)
    tier = _tier_for_subscription(subscription)
    subscription_id = ""
    try:
        subscription_id = str(subscription.get("id", ""))
    except AttributeError:
        subscription_id = str(getattr(subscription, "id", "") or "")
    
    # Sync to API key registry
    record = API_KEY_REGISTRY.upsert_entitlement(
        customer_id=customer_id,
        email=customer_email,
        tier=tier,
        plan=plan,
        max_api_keys=_max_api_keys_for_plan(plan),
        subscription_id=subscription_id,
        price_id=_subscription_price_id(subscription),
    )
    
    # Sync to customer database (support & marketing)
    if customer_db:
        try:
            existing = customer_db.get_customer(customer_id)
            status = str(subscription.get("status", "") or "")
            if not existing:
                # Create new customer record
                customer = Customer(
                    customer_id=customer_id,
                    email=customer_email.strip().lower(),
                    created_at=int(time.time()),
                    updated_at=int(time.time()),
                    current_plan=plan,
                    subscription_id=subscription_id,
                    subscription_status=status,
                    price_id=_subscription_price_id(subscription),
                    license_tier=tier,
                    max_api_keys=_max_api_keys_for_plan(plan),
                    subscription_start=int(subscription.get("current_period_start", 0) or 0),
                    subscription_end=int(subscription.get("current_period_end", 0) or 0),
                )
                customer_db.create_customer(customer)
                log.info("Created customer DB record: %s", customer_id)
            else:
                # Update subscription
                mrr = 9.99 if plan == "pro" else (19.99 if plan == "team" else (49.99 if plan == "enterprise" else 0.0))
                customer_db.sync_subscription(
                    customer_id=customer_id,
                    plan=plan,
                    subscription_id=subscription_id,
                    status=status,
                    price_id=_subscription_price_id(subscription),
                    mrr=mrr,
                )
                log.info("Updated customer DB: %s -> %s", customer_id, plan)
        except Exception as e:
            log.error("Customer DB sync failed: %s", e)
    
    return record


def _license_claims_for_record(record: dict[str, Any]) -> dict[str, object]:
    return {
        "plan": str(record.get("plan", "pro")),
        "max_api_keys": int(record.get("max_api_keys", 1) or 1),
    }


def _portal_signing_key() -> bytes:
    return hashlib.sha256(AHANAFLOW_SIGNING_KEY.encode("utf-8")).digest()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def _b64url_decode(value: str) -> bytes:
    value += "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value)


def _create_portal_session_token(customer_id: str, email: str) -> str:
    payload = {
        "sub": customer_id,
        "email": email.strip().lower(),
        "iat": int(time.time()),
        "exp": int(time.time()) + PORTAL_SESSION_TTL_SECONDS,
    }
    payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    payload_b64 = _b64url_encode(payload_json)
    signature = hmac.new(_portal_signing_key(), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(signature)}"


def _decode_portal_session_token(token: str) -> dict[str, Any]:
    try:
        payload_b64, sig_b64 = token.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid portal token") from exc
    expected = hmac.new(_portal_signing_key(), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    actual = _b64url_decode(sig_b64)
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=401, detail="Invalid portal token")
    payload = json.loads(_b64url_decode(payload_b64))
    if int(payload.get("exp", 0) or 0) < int(time.time()):
        raise HTTPException(status_code=401, detail="Portal session expired")
    return payload


def _require_portal_session(portal_token: str | None, email: str) -> dict[str, Any]:
    if not portal_token:
        raise HTTPException(status_code=401, detail="Portal token required")
    payload = _decode_portal_session_token(portal_token)
    if str(payload.get("email", "")).strip().lower() != email.strip().lower():
        raise HTTPException(status_code=403, detail="Portal token does not match email")
    return payload


def _active_subscription_for_customer(customer_id: str) -> Any:
    subs = stripe.Subscription.list(customer=customer_id, status="active", limit=1)
    active_subs = list(getattr(subs, "data", []) or [])
    if not active_subs:
        raise HTTPException(status_code=403, detail="No active subscription found")
    return active_subs[0]


def _resolve_customer_and_subscription(email: str) -> tuple[dict[str, Any], Any]:
    normalized = email.strip().lower()
    if not normalized or "@" not in normalized:
        raise HTTPException(status_code=400, detail="Invalid email")

    try:
        customers = stripe.Customer.list(email=normalized, limit=5)
    except Exception as exc:
        log.error("Stripe customer list failed: %s", exc)
        raise HTTPException(status_code=503, detail="Stripe unavailable")

    customer_list = list(getattr(customers, "data", []) or [])
    if not customer_list:
        raise HTTPException(status_code=404, detail="Email not found")

    for customer in customer_list:
        try:
            sub = _active_subscription_for_customer(customer["id"])
            return customer, sub
        except HTTPException:
            continue
        except Exception:
            continue

    raise HTTPException(status_code=403, detail="No active subscription found")


def _portal_payload(customer: dict[str, Any], subscription: Any, *, license_key: str, days: int) -> dict[str, Any]:
    record = _sync_entitlement(customer["id"], str(customer.get("email", "") or ""), subscription)
    return {
        "license_key": license_key,
        "tier": str(record.get("tier", "pro")),
        "plan": str(record.get("plan", "pro")),
        "valid_days": days,
        "max_api_keys": int(record.get("max_api_keys", 1) or 1),
        "api_keys": API_KEY_REGISTRY.list_api_keys(customer["id"]),
    }


def _issue_portal_access_code(customer_id: str, email: str) -> None:
    code = f"{secrets.randbelow(1_000_000):06d}"
    API_KEY_REGISTRY.set_access_code(customer_id, code, int(time.time()) + PORTAL_CODE_TTL_SECONDS)
    mail = portal_access_code(email, code, minutes_valid=max(1, PORTAL_CODE_TTL_SECONDS // 60))
    _send_email(to=email, subject=mail["subject"], html=mail["html"], text=mail["text"])


def _sanitize_metadata(metadata: dict[str, Any]) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        cleaned[str(key)[:40]] = str(value)[:400]
    return cleaned


# ---------------------------------------------------------------------------
# Lead capture — no Stripe required
# ---------------------------------------------------------------------------
class LeadRequest(BaseModel):
    email: str = Field(min_length=3)
    company_name: str = ""
    source: str = ""


@app.post("/v1/leads", status_code=201, dependencies=[Depends(_require_service_key)])
async def capture_lead(payload: LeadRequest):
    """Capture a marketing lead directly into the customer DB."""
    normalized = payload.email.strip().lower()
    if "@" not in normalized:
        raise HTTPException(status_code=400, detail="Invalid email")

    cdb = _get_customer_db()
    customer_id = f"lead_{secrets.token_hex(8)}"
    already_exists = False
    if cdb:
        try:
            existing = cdb.get_customer_by_email(normalized)
            if existing:
                customer_id = existing.customer_id
                already_exists = True
            else:
                cdb.create_customer(Customer(
                    customer_id=customer_id,
                    email=normalized,
                    created_at=int(time.time()),
                    updated_at=int(time.time()),
                    company_name=payload.company_name or "",
                    source=payload.source or "organic",
                    tags=["lead"],
                    segment="lead",
                ))
            log.info("Lead captured email=%s id=%s new=%s", normalized, customer_id, not already_exists)
        except Exception as exc:  # noqa: BLE001
            log.error("Lead capture DB error: %s", exc)
            raise HTTPException(status_code=500, detail="Database error") from exc
        finally:
            cdb.close()
    else:
        log.warning("Lead capture: customer DB unavailable — lead not persisted")
        raise HTTPException(status_code=503, detail="Customer database unavailable")

    return {"ok": True, "customer_id": customer_id, "new": not already_exists}


@app.post("/billing/create-checkout-session", response_model=CheckoutResponse)
async def create_checkout_session(payload: CheckoutRequest) -> CheckoutResponse:
    allowed_prices = _allowed_checkout_prices()
    if not allowed_prices:
        raise HTTPException(status_code=503, detail="Stripe prices are not configured")

    if payload.priceId not in allowed_prices:
        raise HTTPException(status_code=400, detail="Unknown Stripe price")

    success_url = (payload.successUrl or DEFAULT_CHECKOUT_SUCCESS_URL).strip()
    cancel_url = (payload.cancelUrl or DEFAULT_CHECKOUT_CANCEL_URL).strip()

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": payload.priceId, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=payload.email.strip(),
            metadata=_sanitize_metadata(payload.metadata),
            allow_promotion_codes=True,
        )
    except Exception as exc:
        log.error("Stripe checkout session creation failed: %s", exc)
        raise HTTPException(status_code=503, detail="Stripe checkout unavailable")

    checkout_url = getattr(session, "url", None)
    session_id = getattr(session, "id", None)
    if not checkout_url or not session_id:
        raise HTTPException(status_code=502, detail="Stripe checkout response incomplete")

    log.info("Checkout session created email=%s price=%s session=%s", payload.email, payload.priceId, session_id)
    return CheckoutResponse(
        checkoutUrl=checkout_url,
        sessionId=session_id,
        status="open",
        mode="subscription",
    )


# ---------------------------------------------------------------------------
# Stripe signature verification (HMAC-SHA256, timing-safe)
# ---------------------------------------------------------------------------
def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> None:
    """Raise HTTPException 400 if the Stripe signature is invalid."""
    try:
        stripe.WebhookSignature.verify_header(payload, sig_header, secret, tolerance=300)
    except Exception as exc:
        log.warning("Stripe signature verification failed: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")


# ---------------------------------------------------------------------------
# License issuance
# ---------------------------------------------------------------------------
def _days_for_plan(subscription) -> int:
    """Return how many days to issue based on the subscription interval."""
    try:
        interval = subscription["items"]["data"][0]["price"]["recurring"]["interval"]
        return LICENSE_DAYS_ANNUAL if interval == "year" else LICENSE_DAYS_MONTHLY
    except (KeyError, IndexError, TypeError):
        return LICENSE_DAYS_MONTHLY


def _issue_license(
    customer_id: str,
    customer_email: str,
    days: int,
    tier: str = "pro",
    plan: str = "pro",
    max_api_keys: int = 1,
    renewal: bool = False,
) -> str:
    """Generate and email a license key.  Returns the JWT."""
    jwt = generate_license_key(
        private_key_b64=AHANAFLOW_SIGNING_KEY,
        customer_id=customer_id,
        tier=tier,
        days=days,
        extra_claims={"plan": plan, "max_api_keys": max_api_keys},
    )
    log.info(
        "License issued customer=%s tier=%s plan=%s max_api_keys=%d days=%d renewal=%s",
        customer_id,
        tier,
        plan,
        max_api_keys,
        days,
        renewal,
    )

    # Build email
    if renewal:
        mail = license_renewed(customer_email, jwt, days=days)
    else:
        mail = license_issued(customer_email, jwt, days=days)

    _send_email(to=customer_email, subject=mail["subject"], html=mail["html"], text=mail["text"])
    return jwt


# ---------------------------------------------------------------------------
# SMTP delivery
# ---------------------------------------------------------------------------
def _send_email(to: str, subject: str, html: str, text: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = to
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")

    if not smtp_host:
        log.warning("SMTP not configured — skipping email to %s (subject: %s)", to, subject)
        return

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(FROM_EMAIL, [to], msg.as_string())
        log.info("Email sent to=%s subject=%s", to, subject)
    except Exception as exc:
        log.error("Email delivery failed to=%s error=%s", to, exc)
        # Don't re-raise — the webhook must still return 200 to Stripe


# ---------------------------------------------------------------------------
# Customer email lookup
# ---------------------------------------------------------------------------
def _get_customer_email(customer_id: str) -> str:
    try:
        customer = stripe.Customer.retrieve(customer_id)
        return str(getattr(customer, "email", "") or "")
    except Exception as exc:
        log.error("Failed to fetch customer %s: %s", customer_id, exc)
        return ""


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------
@app.post("/webhooks/stripe")
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="stripe-signature"),
):
    payload = await request.body()
    _verify_stripe_signature(payload, stripe_signature or "", STRIPE_WEBHOOK_SECRET)

    event = json.loads(payload.decode("utf-8"))

    log.info("Stripe event received: %s id=%s", event.get("type"), event.get("id"))

    if event["type"] == "customer.subscription.created":
        sub = event["data"]["object"]
        customer_id = sub["customer"]
        email = _get_customer_email(customer_id)
        if email:
            record = _sync_entitlement(customer_id, email, sub)
            _issue_license(
                customer_id,
                email,
                days=_days_for_plan(sub),
                tier=str(record.get("tier", "pro")),
                plan=str(record.get("plan", "pro")),
                max_api_keys=int(record.get("max_api_keys", 1) or 1),
                renewal=False,
            )

    elif event["type"] == "invoice.payment_succeeded":
        invoice = event["data"]["object"]
        # Only re-issue for subscription renewals, not one-time charges
        if invoice.get("billing_reason") in ("subscription_cycle", "subscription_create"):
            customer_id = invoice["customer"]
            email = _get_customer_email(customer_id)
            if email:
                sub_id = invoice.get("subscription")
                days = LICENSE_DAYS_MONTHLY
                if sub_id:
                    try:
                        sub = stripe.Subscription.retrieve(sub_id)
                        days = _days_for_plan(sub)
                        record = _sync_entitlement(customer_id, email, sub)
                    except Exception:
                        record = {"tier": "pro", "plan": "pro", "max_api_keys": 1}
                else:
                    record = {"tier": "pro", "plan": "pro", "max_api_keys": 1}
                _issue_license(
                    customer_id,
                    email,
                    days=days,
                    tier=str(record.get("tier", "pro")),
                    plan=str(record.get("plan", "pro")),
                    max_api_keys=int(record.get("max_api_keys", 1) or 1),
                    renewal=True,
                )

    elif event["type"] == "customer.subscription.deleted":
        # JWT expires naturally at end of billing period — no action needed
        customer_id = event["data"]["object"]["customer"]
        log.info("Subscription cancelled for customer=%s — JWT will expire naturally", customer_id)

    else:
        log.debug("Unhandled event type: %s", event["type"])

    return Response(content='{"ok":true}', media_type="application/json")


# ---------------------------------------------------------------------------
# License portal — customers re-fetch their current key
# ---------------------------------------------------------------------------
@app.post("/license/reissue")
async def reissue_license(request: Request, x_portal_token: str | None = Header(None)):
    """
    Allow a logged-in customer to re-fetch their current license key.

    Expects JSON body: {"customer_id": "cus_abc", "stripe_customer_portal_session": "..."}

    In production, replace the stub auth check with a real Stripe customer
    portal session or your own auth layer (JWT user token etc.).
    """
    body = await request.json()
    customer_id = body.get("customer_id", "").strip()
    if not customer_id or not customer_id.startswith("cus_"):
        raise HTTPException(status_code=400, detail="Invalid customer_id")

    # Verify the customer has an active subscription
    try:
        sub = _active_subscription_for_customer(customer_id)
    except Exception as exc:
        log.error("Stripe lookup failed: %s", exc)
        if isinstance(exc, HTTPException):
            raise exc
        raise HTTPException(status_code=503, detail="Stripe unavailable")

    days = _days_for_plan(sub)
    email = _get_customer_email(customer_id)
    _require_portal_session(x_portal_token, email)
    record = _sync_entitlement(customer_id, email, sub)
    jwt = _issue_license(
        customer_id,
        email,
        days=days,
        tier=str(record.get("tier", "pro")),
        plan=str(record.get("plan", "pro")),
        max_api_keys=int(record.get("max_api_keys", 1) or 1),
        renewal=True,
    )
    log.info("License re-issued via portal customer=%s", customer_id)
    return {
        "license_key": jwt,
        "tier": str(record.get("tier", "pro")),
        "plan": str(record.get("plan", "pro")),
        "max_api_keys": int(record.get("max_api_keys", 1) or 1),
        "valid_days": days,
        "api_keys": API_KEY_REGISTRY.list_api_keys(customer_id),
    }


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def _health_payload() -> dict[str, int | str]:
    return {"status": "ok", "service": "ahanaflow-webhook", "ts": int(time.time())}


@app.get("/health")
async def health():
    return _health_payload()


@app.get("/webhooks/health")
async def webhook_health():
    return _health_payload()


# ---------------------------------------------------------------------------
# Portal auth — email access code + short-lived signed portal token
# ---------------------------------------------------------------------------
@app.post("/portal/request-access-code")
async def request_portal_access_code(request: PortalAccessCodeRequest):
    try:
        customer, sub = _resolve_customer_and_subscription(request.email)
    except HTTPException as exc:
        if exc.status_code in {403, 404}:
            return Response(content='{"ok":true}', media_type="application/json", status_code=202)
        raise
    email = str(customer.get("email", "") or "")
    _sync_entitlement(customer["id"], email, sub)
    _issue_portal_access_code(customer["id"], email)
    return Response(content='{"ok":true}', media_type="application/json", status_code=202)


@app.post("/portal/verify-access-code")
async def verify_portal_access_code(request: PortalAccessCodeVerifyRequest):
    customer, sub = _resolve_customer_and_subscription(request.email)
    if not API_KEY_REGISTRY.verify_access_code(customer["id"], request.code.strip()):
        raise HTTPException(status_code=401, detail="Invalid or expired access code")
    email = str(customer.get("email", "") or "")
    days = _days_for_plan(sub)
    record = _sync_entitlement(customer["id"], email, sub)
    jwt = _issue_license(
        customer["id"],
        email,
        days=days,
        tier=str(record.get("tier", "pro")),
        plan=str(record.get("plan", "pro")),
        max_api_keys=int(record.get("max_api_keys", 1) or 1),
        renewal=True,
    )
    portal_token = _create_portal_session_token(customer["id"], email)
    log.info("Portal access granted email=%s customer=%s", email, customer["id"])
    payload = _portal_payload(customer, sub, license_key=jwt, days=days)
    payload["portal_token"] = portal_token
    return payload


@app.post("/license/lookup")
async def lookup_license(request: Request, x_portal_token: str | None = Header(None)):
    body = await request.json()
    email = str(body.get("email", "") or "")
    _require_portal_session(x_portal_token, email)
    customer, sub = _resolve_customer_and_subscription(email)
    email = str(customer.get("email", "") or "")
    days = _days_for_plan(sub)
    record = _sync_entitlement(customer["id"], email, sub)
    jwt = _issue_license(
        customer["id"],
        email,
        days=days,
        tier=str(record.get("tier", "pro")),
        plan=str(record.get("plan", "pro")),
        max_api_keys=int(record.get("max_api_keys", 1) or 1),
        renewal=True,
    )
    log.info("License lookup issued via verified portal email=%s customer=%s", email, customer["id"])
    return _portal_payload(customer, sub, license_key=jwt, days=days)


@app.post("/api-keys/create")
async def create_api_key(request: ApiKeyCreateRequest, x_portal_token: str | None = Header(None)):
    _require_portal_session(x_portal_token, request.email)
    customer, sub = _resolve_customer_and_subscription(request.email)
    record = _sync_entitlement(customer["id"], str(customer.get("email", "") or ""), sub)
    try:
        issued = API_KEY_REGISTRY.create_api_key(customer["id"], label=request.label)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return {
        "ok": True,
        "plan": str(record.get("plan", "pro")),
        "tier": str(record.get("tier", "pro")),
        "max_api_keys": int(record.get("max_api_keys", 1) or 1),
        "api_key": issued["raw_key"],
        "issued_key": {k: v for k, v in issued.items() if k != "raw_key"},
        "api_keys": API_KEY_REGISTRY.list_api_keys(customer["id"]),
    }


@app.post("/api-keys/revoke")
async def revoke_api_key(request: ApiKeyRevokeRequest, x_portal_token: str | None = Header(None)):
    _require_portal_session(x_portal_token, request.email)
    customer, sub = _resolve_customer_and_subscription(request.email)
    record = _sync_entitlement(customer["id"], str(customer.get("email", "") or ""), sub)
    revoked = API_KEY_REGISTRY.revoke_api_key(customer["id"], request.keyId)
    if not revoked:
        raise HTTPException(status_code=404, detail="API key not found")

    return {
        "ok": True,
        "plan": str(record.get("plan", "pro")),
        "tier": str(record.get("tier", "pro")),
        "max_api_keys": int(record.get("max_api_keys", 1) or 1),
        "api_keys": API_KEY_REGISTRY.list_api_keys(customer["id"]),
    }


# ---------------------------------------------------------------------------
# Admin dashboard — password-protected operator UI
# ---------------------------------------------------------------------------
import asyncio
from pathlib import Path as _Path
from fastapi.responses import HTMLResponse, FileResponse

_ADMIN_PASSWORD_HASH: str = os.environ.get(
    "ADMIN_PASSWORD_HASH",
    # Default: SHA-256 of "AhanaAdmin2026!" — change via env var before production
    "9a1b9acb5b48d7b92ac85f5b149b38366661c220e7eba1b69bc05a0f1ee48685",
)
_ADMIN_TOKENS: dict[str, float] = {}  # token → expiry timestamp
_ADMIN_TOKEN_TTL = 8 * 3600  # 8 hours
_ADMIN_TOKEN_LOCK = __import__("threading").Lock()


class _AdminLoginRequest(BaseModel):
    password: str = Field(min_length=1)


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _issue_admin_token() -> str:
    token = secrets.token_urlsafe(32)
    with _ADMIN_TOKEN_LOCK:
        _ADMIN_TOKENS[token] = time.time() + _ADMIN_TOKEN_TTL
        # Prune expired tokens
        expired = [t for t, exp in _ADMIN_TOKENS.items() if exp < time.time()]
        for t in expired:
            _ADMIN_TOKENS.pop(t, None)
    return token


def _require_admin_token(x_admin_token: str | None) -> None:
    if not x_admin_token:
        raise HTTPException(status_code=401, detail="Admin token required")
    with _ADMIN_TOKEN_LOCK:
        expiry = _ADMIN_TOKENS.get(x_admin_token)
    if expiry is None or expiry < time.time():
        raise HTTPException(status_code=401, detail="Invalid or expired admin token")


@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard():
    """Serve the admin dashboard SPA."""
    html_path = _Path(__file__).parent.parent.parent / "website" / "admin.html"
    if not html_path.exists():
        raise HTTPException(status_code=503, detail="Admin dashboard not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/admin/login")
async def admin_login(request: _AdminLoginRequest):
    """Authenticate with admin password and receive a session token."""
    if _hash_password(request.password) != _ADMIN_PASSWORD_HASH.lower():
        raise HTTPException(status_code=401, detail="Invalid password")
    token = _issue_admin_token()
    return {"ok": True, "token": token, "ttl": _ADMIN_TOKEN_TTL}


@app.get("/admin/summary")
async def admin_summary(x_admin_token: str | None = Header(None)):
    """Return high-level dashboard summary."""
    _require_admin_token(x_admin_token)
    reg = API_KEY_REGISTRY._load()
    customers = list(reg.get("customers", {}).values())
    active = [c for c in customers if c.get("subscription_id")]
    plans = {"pro": 0, "team": 0, "enterprise": 0, "other": 0}
    for c in active:
        plan = str(c.get("plan", "other"))
        if plan in plans:
            plans[plan] += 1
        else:
            plans["other"] += 1
    total_api_keys = sum(len(c.get("api_keys", [])) for c in customers)

    # Count leads from customer DB
    db_leads = 0
    cdb = _get_customer_db()
    if cdb:
        try:
            db_leads = len(cdb.list_all_customers())
        except Exception:
            pass
        finally:
            cdb.close()

    return {
        "total_customers": len(customers),
        "active_subscriptions": len(active),
        "plan_breakdown": plans,
        "total_api_keys_issued": total_api_keys,
        "db_leads": db_leads,
    }


@app.get("/admin/customers")
async def admin_customers(x_admin_token: str | None = Header(None)):
    """Return all customer records (JSON registry + DB leads merged)."""
    _require_admin_token(x_admin_token)
    reg = API_KEY_REGISTRY._load()
    customers = list(reg.get("customers", {}).values())
    registry_emails = {str(c.get("email", "")).lower() for c in customers}

    # Merge leads from customer DB
    cdb = _get_customer_db()
    if cdb:
        try:
            db_customers = cdb.list_all_customers()
            for dbc in db_customers:
                if dbc.email.lower() not in registry_emails:
                    customers.append(dbc.to_dict())
        except Exception as exc:
            log.error("CustomerDB list failed in admin_customers: %s", exc)
        finally:
            cdb.close()

    # Sort newest first by updated_at
    customers.sort(key=lambda c: int(c.get("updated_at", 0)), reverse=True)
    # Redact raw API key values
    for c in customers:
        for key in c.get("api_keys", []):
            key.pop("raw_key", None)
    return {"customers": customers, "total": len(customers)}


@app.get("/admin/api-keys")
async def admin_api_keys(x_admin_token: str | None = Header(None)):
    """Return all issued API keys across all customers (raw keys redacted)."""
    _require_admin_token(x_admin_token)
    reg = API_KEY_REGISTRY._load()
    all_keys = []
    for customer_id, rec in reg.get("customers", {}).items():
        for key in rec.get("api_keys", []):
            entry = {k: v for k, v in key.items() if k != "raw_key"}
            entry["customer_id"] = customer_id
            entry["email"] = rec.get("email", "")
            entry["plan"] = rec.get("plan", "")
            all_keys.append(entry)
    all_keys.sort(key=lambda k: int(k.get("created_at", 0)), reverse=True)
    return {"api_keys": all_keys, "total": len(all_keys)}


@app.get("/admin/health")
async def admin_health(x_admin_token: str | None = Header(None)):
    """Return server health summary visible only to admins."""
    _require_admin_token(x_admin_token)
    import asyncio

    async def _tcp_ok(host: str, port: int, timeout: float = 2.0) -> bool:
        """Return True if a TCP connection to host:port succeeds."""
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False

    state_host = os.environ.get("UNIVERSAL_STATE_HOST", "127.0.0.1")
    state_port = int(os.environ.get("UNIVERSAL_STATE_PORT", "9633"))
    vector_host = os.environ.get("VECTOR_STATE_HOST", "127.0.0.1")
    vector_port = int(os.environ.get("VECTOR_STATE_PORT", "9644"))

    state_ok, vector_ok = await asyncio.gather(
        _tcp_ok(state_host, state_port),
        _tcp_ok(vector_host, vector_port),
    )

    return {
        "webhook_server": "ok",
        "universal_state_server": "ok" if state_ok else f"unreachable ({state_host}:{state_port})",
        "vector_state_server": "ok" if vector_ok else f"unreachable ({vector_host}:{vector_port})",
        "uptime_check": time.time(),
    }


@app.post("/admin/rotate-service-key")
async def rotate_service_key(x_admin_token: str | None = Header(None)):
    """Rotate the service API key used to authenticate /v1/leads.

    The old key becomes 'prev' and remains valid for ~2 weeks so callers
    can update their config without a hard cutover.  Returns the new key
    to the admin so they can update the caller's env var.
    """
    _require_admin_token(x_admin_token)
    new_key = "ahf_" + secrets.token_hex(32)
    old_current = _uss_get(_SERVICE_KEY_USS_CURRENT) or ""
    if old_current:
        _uss_set(_SERVICE_KEY_USS_PREV, old_current)
    _uss_set(_SERVICE_KEY_USS_CURRENT, new_key)
    log.info("Service API key rotated by admin")
    return {
        "ok": True,
        "new_key": new_key,
        "previous_key_still_valid": bool(old_current),
        "note": "Update AHANAFLOW_SERVICE_API_KEY in your caller's env and redeploy within 2 weeks.",
    }
