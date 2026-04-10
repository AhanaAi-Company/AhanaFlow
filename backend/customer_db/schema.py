"""
Customer Database Schema for AhanaFlow
Uses UniversalStateServer for compressed durable storage
"""

from dataclasses import dataclass, asdict, field
from typing import Any
from datetime import datetime
import json


@dataclass
class Customer:
    """Customer record with subscription and support history"""
    
    customer_id: str  # Stripe customer ID (cus_...)
    email: str
    created_at: int  # Unix timestamp
    updated_at: int
    
    # Subscription data
    current_plan: str = "free"  # free, pro, team, enterprise
    subscription_id: str = ""  # Stripe subscription ID
    subscription_status: str = ""  # active, canceled, past_due, etc.
    price_id: str = ""
    subscription_start: int = 0
    subscription_end: int = 0
    
    # License data
    license_tier: str = "free"  # free, pro, enterprise
    max_api_keys: int = 0
    issued_api_keys: int = 0
    
    # Company/account info
    company_name: str = ""
    contact_name: str = ""
    phone: str = ""
    
    # Support data
    support_tier: str = "community"  # community, email, priority, dedicated
    support_notes: list[dict[str, Any]] = field(default_factory=list)
    last_support_contact: int = 0
    
    # Marketing data
    source: str = ""  # utm_source, referral, organic, etc.
    campaign: str = ""  # utm_campaign
    tags: list[str] = field(default_factory=list)
    segment: str = ""  # startup, enterprise, individual, etc.
    
    # Usage/engagement
    last_login: int = 0
    total_api_calls: int = 0
    total_data_compressed_bytes: int = 0
    
    # Billing
    lifetime_value: float = 0.0  # Total revenue in USD
    mrr: float = 0.0  # Monthly recurring revenue
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Customer":
        # Handle list fields that might be None
        data.setdefault("support_notes", [])
        data.setdefault("tags", [])
        return cls(**data)


@dataclass
class SupportNote:
    """Individual support interaction"""
    
    note_id: str  # UUID or timestamp-based
    customer_id: str
    created_at: int
    created_by: str  # Support agent or system
    category: str  # technical, billing, feature_request, bug, etc.
    priority: str  # low, medium, high, urgent
    status: str  # open, in_progress, resolved, closed
    subject: str
    content: str
    resolution: str = ""
    resolved_at: int = 0
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SupportNote":
        return cls(**data)


def customer_key(customer_id: str) -> str:
    """Generate UniversalStateServer key for customer record"""
    return f"customer:{customer_id}"


def subscription_key(subscription_id: str) -> str:
    """Generate UniversalStateServer key for subscription record"""
    return f"subscription:{subscription_id}"


def support_note_key(customer_id: str, note_id: str) -> str:
    """Generate UniversalStateServer key for support note"""
    return f"support:{customer_id}:{note_id}"


def email_index_key(email: str) -> str:
    """Generate UniversalStateServer key for email->customer_id lookup"""
    return f"email_idx:{email.lower()}"


def tag_index_key(tag: str) -> str:
    """Generate UniversalStateServer key for tag->customer_ids list"""
    return f"tag_idx:{tag.lower()}"


def segment_index_key(segment: str) -> str:
    """Generate UniversalStateServer key for segment->customer_ids list"""
    return f"segment_idx:{segment.lower()}"
