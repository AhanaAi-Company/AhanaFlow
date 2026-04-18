"""Customer Database Admin API — authenticated endpoints for customer queries."""

from __future__ import annotations

import hmac
import os

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from backend.common import read_secret
from backend.customer_db import CustomerDatabaseEngine, Customer, SupportNote


app = FastAPI(
    title="AhanaFlow Customer Database API",
    description="Customer data, subscriptions, support, and marketing",
    version="1.0.0",
)

# Connect to customer database (UniversalStateServer on port 9635)
CUSTOMER_DB_HOST = os.environ.get("CUSTOMER_DB_HOST", "127.0.0.1")
CUSTOMER_DB_PORT = int(os.environ.get("CUSTOMER_DB_PORT", "9635"))
db = CustomerDatabaseEngine(host=CUSTOMER_DB_HOST, port=CUSTOMER_DB_PORT)


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _valid_admin_keys() -> list[str]:
    keys = []
    for env_name in ("AHANAFLOW_ADMIN_API_KEY", "AHANAFLOW_ADMIN_API_KEY_PREV"):
        value = read_secret(env_name)
        if value:
            keys.append(value)
    return keys


def _require_admin_key(
    x_admin_api_key: str | None = Header(None),
    authorization: str | None = Header(None),
) -> None:
    configured_keys = _valid_admin_keys()
    if not configured_keys:
        if os.environ.get("AHANAFLOW_ALLOW_INSECURE_ADMIN_API", "").strip().lower() in {"1", "true", "yes"}:
            return
        raise HTTPException(status_code=503, detail="Admin API key is not configured")

    presented_key = x_admin_api_key or _extract_bearer_token(authorization)
    if not presented_key:
        raise HTTPException(status_code=401, detail="Missing admin API key")

    if not any(hmac.compare_digest(presented_key, candidate) for candidate in configured_keys):
        raise HTTPException(status_code=401, detail="Invalid admin API key")


# ── Request Models ──────────────────────────────────────────────────────────

class CreateSupportNoteRequest(BaseModel):
    customer_id: str = Field(min_length=3)
    category: str = Field(min_length=1)
    priority: str = "medium"
    subject: str = Field(min_length=1)
    content: str = Field(min_length=1)
    created_by: str = "api"


class AddTagRequest(BaseModel):
    customer_id: str = Field(min_length=3)
    tag: str = Field(min_length=1)


class SetSegmentRequest(BaseModel):
    customer_id: str = Field(min_length=3)
    segment: str = Field(min_length=1)


# ── Health Check ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        db.connect()
        return {"status": "healthy", "service": "customer-db-api"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Customer DB unhealthy: {e}")


# ── Customer Queries ────────────────────────────────────────────────────────

@app.get("/customers/{customer_id}")
async def get_customer(customer_id: str, _auth: None = Depends(_require_admin_key)):
    """Get customer by ID"""
    db.connect()
    customer = db.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer.to_dict()


@app.get("/customers/email/{email}")
async def get_customer_by_email(email: str, _auth: None = Depends(_require_admin_key)):
    """Get customer by email address"""
    db.connect()
    customer = db.get_customer_by_email(email)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer.to_dict()


@app.get("/customers/{customer_id}/usage")
async def get_customer_usage(customer_id: str, _auth: None = Depends(_require_admin_key)):
    """Get customer usage statistics"""
    db.connect()
    customer = db.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    usage = db.get_usage_stats(customer_id)
    return {
        "customer_id": customer_id,
        "email": customer.email,
        "plan": customer.current_plan,
        "api_calls": usage["api_calls"],
        "compressed_bytes": usage["compressed_bytes"],
    }


# ── Support Notes ───────────────────────────────────────────────────────────

@app.post("/support/notes")
async def create_support_note(request: CreateSupportNoteRequest, _auth: None = Depends(_require_admin_key)):
    """Add a support note to customer record"""
    db.connect()
    note = db.add_support_note(
        customer_id=request.customer_id,
        category=request.category,
        priority=request.priority,
        subject=request.subject,
        content=request.content,
        created_by=request.created_by,
    )
    return note.to_dict()


@app.get("/support/notes/{customer_id}/{note_id}")
async def get_support_note(customer_id: str, note_id: str, _auth: None = Depends(_require_admin_key)):
    """Get a support note"""
    db.connect()
    note = db.get_support_note(customer_id, note_id)
    if not note:
        raise HTTPException(status_code=404, detail="Support note not found")
    return note.to_dict()


@app.post("/support/notes/{customer_id}/{note_id}/resolve")
async def resolve_support_note(
    customer_id: str,
    note_id: str,
    resolution: str,
    resolved_by: str = "api",
    _auth: None = Depends(_require_admin_key),
):
    """Mark a support note as resolved"""
    db.connect()
    db.resolve_support_note(customer_id, note_id, resolution, resolved_by)
    return {"ok": True}


# ── Marketing ───────────────────────────────────────────────────────────────

@app.post("/marketing/tags")
async def add_customer_tag(request: AddTagRequest, _auth: None = Depends(_require_admin_key)):
    """Add a marketing tag to customer"""
    db.connect()
    db.add_tag(request.customer_id, request.tag)
    return {"ok": True}


@app.delete("/marketing/tags/{customer_id}/{tag}")
async def remove_customer_tag(customer_id: str, tag: str, _auth: None = Depends(_require_admin_key)):
    """Remove a marketing tag from customer"""
    db.connect()
    db.remove_tag(customer_id, tag)
    return {"ok": True}


@app.get("/marketing/tags/{tag}")
async def get_customers_by_tag(tag: str, _auth: None = Depends(_require_admin_key)):
    """Get all customers with a specific tag"""
    db.connect()
    customer_ids = db.get_customers_by_tag(tag)
    return {"tag": tag, "customer_count": len(customer_ids), "customer_ids": customer_ids}


@app.post("/marketing/segments")
async def set_customer_segment(request: SetSegmentRequest, _auth: None = Depends(_require_admin_key)):
    """Set customer segment"""
    db.connect()
    db.set_segment(request.customer_id, request.segment)
    return {"ok": True}


@app.get("/marketing/segments/{segment}")
async def get_customers_by_segment(segment: str, _auth: None = Depends(_require_admin_key)):
    """Get all customers in a specific segment"""
    db.connect()
    customer_ids = db.get_customers_by_segment(segment)
    return {
        "segment": segment,
        "customer_count": len(customer_ids),
        "customer_ids": customer_ids,
    }
