"""
Customer Database Engine — wraps UniversalStateServer
Provides customer-specific operations with compressed durable storage
"""

import json
import socket
import time
from typing import Any, Optional
from datetime import datetime
import uuid

from .schema import (
    Customer,
    SupportNote,
    customer_key,
    subscription_key,
    support_note_key,
    email_index_key,
    tag_index_key,
    segment_index_key,
)


class CustomerDatabaseEngine:
    """Customer database backed by UniversalStateServer"""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 9633):
        self.host = host
        self.port = port
        self._sock: Optional[socket.socket] = None
    
    def connect(self):
        """Connect to UniversalStateServer"""
        if self._sock:
            return
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
    
    def close(self):
        """Close connection to UniversalStateServer"""
        if self._sock:
            self._sock.close()
            self._sock = None
    
    def _send(self, cmd: dict[str, Any]) -> dict[str, Any]:
        """Send command to UniversalStateServer and return response"""
        if not self._sock:
            self.connect()
        payload = json.dumps(cmd) + "\n"
        self._sock.sendall(payload.encode("utf-8"))
        response = self._sock.recv(65536).decode("utf-8").strip()
        return json.loads(response)
    
    # ── Customer CRUD ──────────────────────────────────────────────────
    
    def create_customer(self, customer: Customer) -> None:
        """Create a new customer record"""
        customer.created_at = int(time.time())
        customer.updated_at = customer.created_at
        
        # Store customer record
        self._send({
            "cmd": "SET",
            "key": customer_key(customer.customer_id),
            "value": json.dumps(customer.to_dict()),
        })
        
        # Index by email
        self._send({
            "cmd": "SET",
            "key": email_index_key(customer.email),
            "value": customer.customer_id,
        })
        
        # Maintain all-customers index
        idx_resp = self._send({"cmd": "GET", "key": "customers:all:index"})
        all_ids = json.loads(idx_resp.get("result") or "[]")
        if customer.customer_id not in all_ids:
            all_ids.append(customer.customer_id)
            self._send({
                "cmd": "SET",
                "key": "customers:all:index",
                "value": json.dumps(all_ids),
            })
    
    def get_customer(self, customer_id: str) -> Optional[Customer]:
        """Get customer by ID"""
        resp = self._send({
            "cmd": "GET",
            "key": customer_key(customer_id),
        })
        if resp.get("result") is None:
            return None
        return Customer.from_dict(json.loads(resp["result"]))
    
    def get_customer_by_email(self, email: str) -> Optional[Customer]:
        """Get customer by email address"""
        resp = self._send({
            "cmd": "GET",
            "key": email_index_key(email),
        })
        customer_id = resp.get("result")
        if not customer_id:
            return None
        return self.get_customer(customer_id)
    
    def update_customer(self, customer: Customer) -> None:
        """Update existing customer record"""
        customer.updated_at = int(time.time())
        self._send({
            "cmd": "SET",
            "key": customer_key(customer.customer_id),
            "value": json.dumps(customer.to_dict()),
        })
    
    def delete_customer(self, customer_id: str) -> None:
        """Delete customer record (soft delete - mark as deleted)"""
        customer = self.get_customer(customer_id)
        if customer:
            customer.tags.append("deleted")
            customer.updated_at = int(time.time())
            self.update_customer(customer)
    
    # ── Subscription Integration ───────────────────────────────────────
    
    def sync_subscription(
        self,
        customer_id: str,
        plan: str,
        subscription_id: str,
        status: str,
        price_id: str,
        mrr: float,
    ) -> None:
        """Sync subscription data from Stripe webhook"""
        customer = self.get_customer(customer_id)
        if not customer:
            return
        
        customer.current_plan = plan
        customer.subscription_id = subscription_id
        customer.subscription_status = status
        customer.price_id = price_id
        customer.mrr = mrr
        customer.updated_at = int(time.time())
        
        # Update lifetime value if subscription active
        if status == "active":
            customer.lifetime_value += mrr
        
        self.update_customer(customer)
    
    # ── Support Notes ──────────────────────────────────────────────────
    
    def add_support_note(
        self,
        customer_id: str,
        category: str,
        priority: str,
        subject: str,
        content: str,
        created_by: str = "system",
    ) -> SupportNote:
        """Add a support note to customer record"""
        note_id = str(uuid.uuid4())
        note = SupportNote(
            note_id=note_id,
            customer_id=customer_id,
            created_at=int(time.time()),
            created_by=created_by,
            category=category,
            priority=priority,
            status="open",
            subject=subject,
            content=content,
        )
        
        # Store note
        self._send({
            "cmd": "SET",
            "key": support_note_key(customer_id, note_id),
            "value": json.dumps(note.to_dict()),
        })
        
        # Update customer last_support_contact
        customer = self.get_customer(customer_id)
        if customer:
            customer.last_support_contact = note.created_at
            customer.support_notes.append({
                "note_id": note_id,
                "created_at": note.created_at,
                "category": category,
                "subject": subject,
            })
            self.update_customer(customer)
        
        return note
    
    def get_support_note(self, customer_id: str, note_id: str) -> Optional[SupportNote]:
        """Get a support note by ID"""
        resp = self._send({
            "cmd": "GET",
            "key": support_note_key(customer_id, note_id),
        })
        if resp.get("result") is None:
            return None
        return SupportNote.from_dict(json.loads(resp["result"]))
    
    def resolve_support_note(
        self,
        customer_id: str,
        note_id: str,
        resolution: str,
        resolved_by: str = "system",
    ) -> None:
        """Mark a support note as resolved"""
        note = self.get_support_note(customer_id, note_id)
        if note:
            note.status = "resolved"
            note.resolution = resolution
            note.resolved_at = int(time.time())
            self._send({
                "cmd": "SET",
                "key": support_note_key(customer_id, note_id),
                "value": json.dumps(note.to_dict()),
            })
    
    # ── Marketing Segmentation ─────────────────────────────────────────
    
    def add_tag(self, customer_id: str, tag: str) -> None:
        """Add a marketing tag to customer"""
        customer = self.get_customer(customer_id)
        if customer and tag.lower() not in [t.lower() for t in customer.tags]:
            customer.tags.append(tag.lower())
            customer.updated_at = int(time.time())
            self.update_customer(customer)
            
            # Update tag index (append customer_id to tag's customer list)
            idx_key = tag_index_key(tag)
            resp = self._send({"cmd": "GET", "key": idx_key})
            customer_ids = json.loads(resp.get("result") or "[]")
            if customer_id not in customer_ids:
                customer_ids.append(customer_id)
                self._send({
                    "cmd": "SET",
                    "key": idx_key,
                    "value": json.dumps(customer_ids),
                })
    
    def remove_tag(self, customer_id: str, tag: str) -> None:
        """Remove a marketing tag from customer"""
        customer = self.get_customer(customer_id)
        if customer and tag.lower() in [t.lower() for t in customer.tags]:
            customer.tags = [t for t in customer.tags if t.lower() != tag.lower()]
            customer.updated_at = int(time.time())
            self.update_customer(customer)
            
            # Update tag index
            idx_key = tag_index_key(tag)
            resp = self._send({"cmd": "GET", "key": idx_key})
            customer_ids = json.loads(resp.get("result") or "[]")
            if customer_id in customer_ids:
                customer_ids.remove(customer_id)
                self._send({
                    "cmd": "SET",
                    "key": idx_key,
                    "value": json.dumps(customer_ids),
                })
    
    def get_customers_by_tag(self, tag: str) -> list[str]:
        """Get all customer IDs with a specific tag"""
        resp = self._send({"cmd": "GET", "key": tag_index_key(tag)})
        return json.loads(resp.get("result") or "[]")
    
    def set_segment(self, customer_id: str, segment: str) -> None:
        """Set customer segment (startup, enterprise, individual, etc.)"""
        customer = self.get_customer(customer_id)
        if customer:
            old_segment = customer.segment
            customer.segment = segment.lower()
            customer.updated_at = int(time.time())
            self.update_customer(customer)
            
            # Update segment indices
            if old_segment:
                old_idx_key = segment_index_key(old_segment)
                resp = self._send({"cmd": "GET", "key": old_idx_key})
                customer_ids = json.loads(resp.get("result") or "[]")
                if customer_id in customer_ids:
                    customer_ids.remove(customer_id)
                    self._send({
                        "cmd": "SET",
                        "key": old_idx_key,
                        "value": json.dumps(customer_ids),
                    })
            
            new_idx_key = segment_index_key(segment)
            resp = self._send({"cmd": "GET", "key": new_idx_key})
            customer_ids = json.loads(resp.get("result") or "[]")
            if customer_id not in customer_ids:
                customer_ids.append(customer_id)
                self._send({
                    "cmd": "SET",
                    "key": new_idx_key,
                    "value": json.dumps(customer_ids),
                })
    
    def get_customers_by_segment(self, segment: str) -> list[str]:
        """Get all customer IDs in a specific segment"""
        resp = self._send({"cmd": "GET", "key": segment_index_key(segment)})
        return json.loads(resp.get("result") or "[]")
    
    # ── Usage Tracking ─────────────────────────────────────────────────
    
    def increment_api_calls(self, customer_id: str, count: int = 1) -> None:
        """Increment customer's total API call count"""
        counter_key = f"usage:api_calls:{customer_id}"
        self._send({"cmd": "INCR", "key": counter_key, "amount": count})
    
    def increment_data_compressed(self, customer_id: str, bytes_compressed: int) -> None:
        """Increment customer's total compressed data bytes"""
        counter_key = f"usage:compressed_bytes:{customer_id}"
        self._send({"cmd": "INCR", "key": counter_key, "amount": bytes_compressed})
    
    def list_all_customers(self) -> list["Customer"]:
        """List all customer records from the all-customers index"""
        idx_resp = self._send({"cmd": "GET", "key": "customers:all:index"})
        all_ids = json.loads(idx_resp.get("result") or "[]")
        customers = []
        for cid in all_ids:
            c = self.get_customer(cid)
            if c:
                customers.append(c)
        return customers

    def get_usage_stats(self, customer_id: str) -> dict[str, int]:
        """Get customer usage statistics"""
        api_calls_resp = self._send({
            "cmd": "GET",
            "key": f"usage:api_calls:{customer_id}",
        })
        compressed_bytes_resp = self._send({
            "cmd": "GET",
            "key": f"usage:compressed_bytes:{customer_id}",
        })
        
        return {
            "api_calls": int(api_calls_resp.get("result") or "0"),
            "compressed_bytes": int(compressed_bytes_resp.get("result") or "0"),
        }
