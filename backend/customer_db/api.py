"""
Customer Database Admin API — FastAPI endpoints for customer queries
"""

import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

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
async def get_customer(customer_id: str):
    """Get customer by ID"""
    db.connect()
    customer = db.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer.to_dict()


@app.get("/customers/email/{email}")
async def get_customer_by_email(email: str):
    """Get customer by email address"""
    db.connect()
    customer = db.get_customer_by_email(email)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer.to_dict()


@app.get("/customers/{customer_id}/usage")
async def get_customer_usage(customer_id: str):
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
async def create_support_note(request: CreateSupportNoteRequest):
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
async def get_support_note(customer_id: str, note_id: str):
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
):
    """Mark a support note as resolved"""
    db.connect()
    db.resolve_support_note(customer_id, note_id, resolution, resolved_by)
    return {"ok": True}


# ── Marketing ───────────────────────────────────────────────────────────────

@app.post("/marketing/tags")
async def add_customer_tag(request: AddTagRequest):
    """Add a marketing tag to customer"""
    db.connect()
    db.add_tag(request.customer_id, request.tag)
    return {"ok": True}


@app.delete("/marketing/tags/{customer_id}/{tag}")
async def remove_customer_tag(customer_id: str, tag: str):
    """Remove a marketing tag from customer"""
    db.connect()
    db.remove_tag(customer_id, tag)
    return {"ok": True}


@app.get("/marketing/tags/{tag}")
async def get_customers_by_tag(tag: str):
    """Get all customers with a specific tag"""
    db.connect()
    customer_ids = db.get_customers_by_tag(tag)
    return {"tag": tag, "customer_count": len(customer_ids), "customer_ids": customer_ids}


@app.post("/marketing/segments")
async def set_customer_segment(request: SetSegmentRequest):
    """Set customer segment"""
    db.connect()
    db.set_segment(request.customer_id, request.segment)
    return {"ok": True}


@app.get("/marketing/segments/{segment}")
async def get_customers_by_segment(segment: str):
    """Get all customers in a specific segment"""
    db.connect()
    customer_ids = db.get_customers_by_segment(segment)
    return {
        "segment": segment,
        "customer_count": len(customer_ids),
        "customer_ids": customer_ids,
    }
