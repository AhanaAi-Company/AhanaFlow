"""
AhanaFlow Session Store Example

User session management with TTL expiry.
Perfect for web applications, API authentication tokens, etc.
"""

import socket
import json
import secrets
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class Session:
    """Represents a user session."""
    session_id: str
    user_id: str
    created_at: datetime
    expires_at: datetime
    data: Dict[str, Any]


class SessionStore:
    """Session storage using AhanaFlow with TTL."""
    
    def __init__(self, host: str = "localhost", port: int = 9633):
        self.host = host
        self.port = port
    
    def send_command(self, cmd: dict) -> dict:
        """Send command to AhanaFlow server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            sock.sendall((json.dumps(cmd) + "\n").encode())
            response = sock.recv(16384).decode().strip()
            return json.loads(response)
        finally:
            sock.close()
    
    def create_session(
        self,
        user_id: str,
        ttl_seconds: int = 3600,
        data: Optional[Dict[str, Any]] = None
    ) -> Session:
        """Create new session with TTL."""
        session_id = secrets.token_urlsafe(32)
        now = datetime.now()
        expires_at = now + timedelta(seconds=ttl_seconds)
        
        session_data = {
            "session_id": session_id,
            "user_id": user_id,
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "data": data or {}
        }
        
        # Store with TTL
        self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": session_data,
            "ttl_seconds": ttl_seconds
        })
        
        return Session(
            session_id=session_id,
            user_id=user_id,
            created_at=now,
            expires_at=expires_at,
            data=data or {}
        )
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Retrieve session by ID."""
        result = self.send_command({
            "cmd": "GET",
            "key": f"session:{session_id}"
        })
        
        if result.get("status") == "ok" and result.get("result"):
            data = result["result"]
            return Session(
                session_id=data["session_id"],
                user_id=data["user_id"],
                created_at=datetime.fromisoformat(data["created_at"]),
                expires_at=datetime.fromisoformat(data["expires_at"]),
                data=data.get("data", {})
            )
        return None
    
    def update_session(
        self,
        session_id: str,
        data: Dict[str, Any],
        extend_ttl: bool = True
    ) -> bool:
        """Update session data."""
        session = self.get_session(session_id)
        if not session:
            return False
        
        # Merge new data
        session.data.update(data)
        
        # Calculate remaining TTL
        remaining_seconds = int((session.expires_at - datetime.now()).total_seconds())
        
        if extend_ttl:
            remaining_seconds = max(remaining_seconds, 3600)  # Reset to 1 hour
        
        if remaining_seconds <= 0:
            return False  # Session expired
        
        # Update with new TTL
        session_data = {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "created_at": session.created_at.isoformat(),
            "expires_at": (datetime.now() + timedelta(seconds=remaining_seconds)).isoformat(),
            "data": session.data
        }
        
        result = self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": session_data,
            "ttl_seconds": remaining_seconds
        })
        
        return result.get("status") == "ok"
    
    def delete_session(self, session_id: str) -> bool:
        """Delete session (logout)."""
        result = self.send_command({
            "cmd": "DEL",
            "key": f"session:{session_id}"
        })
        return result.get("status") == "ok"
    
    def refresh_session(self, session_id: str, ttl_seconds: int = 3600) -> bool:
        """Refresh session TTL (keep-alive)."""
        session = self.get_session(session_id)
        if not session:
            return False
        
        session_data = {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "created_at": session.created_at.isoformat(),
            "expires_at": (datetime.now() + timedelta(seconds=ttl_seconds)).isoformat(),
            "data": session.data
        }
        
        result = self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": session_data,
            "ttl_seconds": ttl_seconds
        })
        
        return result.get("status") == "ok"


# Example usage
if __name__ == "__main__":
    store = SessionStore()
    
    print("="*60)
    print("AhanaFlow Session Store Demo")
    print("="*60 + "\n")
    
    # Create session
    print("1. Creating session for user 'alice'...")
    session = store.create_session(
        user_id="alice",
        ttl_seconds=10,  # 10 seconds for demo
        data={"role": "admin", "theme": "dark"}
    )
    print(f"   ✓ Session ID: {session.session_id[:16]}...")
    print(f"   ✓ Expires: {session.expires_at.strftime('%H:%M:%S')}")
    print(f"   ✓ Data: {session.data}\n")
    
    # Retrieve session
    print("2. Retrieving session...")
    retrieved = store.get_session(session.session_id)
    if retrieved:
        print(f"   ✓ User: {retrieved.user_id}")
        print(f"   ✓ Data: {retrieved.data}\n")
    
    # Update session
    print("3. Updating session data...")
    store.update_session(
        session.session_id,
        data={"last_page": "/dashboard", "notifications": 3},
        extend_ttl=False
    )
    updated = store.get_session(session.session_id)
    print(f"   ✓ Updated data: {updated.data}\n")
    
    # Wait for expiry
    print("4. Waiting for session to expire (10 seconds)...")
    for i in range(10):
        time.sleep(1)
        remaining = store.get_session(session.session_id)
        if remaining:
            ttl_left = int((remaining.expires_at - datetime.now()).total_seconds())
            print(f"   ⏱️  TTL: {ttl_left}s remaining")
        else:
            print(f"   ✗ Session expired after {i+1}s")
            break
    
    # Try to retrieve expired session
    print("\n5. Attempting to retrieve expired session...")
    expired = store.get_session(session.session_id)
    if expired:
        print("   ⚠ Session still exists (unexpected)")
    else:
        print("   ✓ Session automatically expired (as expected)\n")
    
    print("="*60)
    print("Session store demo complete")
    print("="*60)
"""
AhanaFlow Session Store Example

User session management with automatic TTL expiry.
Perfect for web applications, API authentication, etc.
"""

import time
import socket
import json
import secrets
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta


@dataclass
class Session:
    """Represents a user session."""
    session_id: str
    user_id: str
    email: str
    created_at: str
    expires_at: str
    metadata: dict


class SessionStore:
    """Session store using AhanaFlow with TTL."""
    
    def __init__(self, host: str = "localhost", port: int = 9633):
        self.host = host
        self.port = port
        self.default_ttl = 3600  # 1 hour
    
    def send_command(self, cmd: dict) -> dict:
        """Send command to AhanaFlow server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            sock.sendall((json.dumps(cmd) + "\n").encode())
            response = sock.recv(16384).decode().strip()
            return json.loads(response)
        finally:
            sock.close()
    
    def create_session(
        self,
        user_id: str,
        email: str,
        ttl_seconds: Optional[int] = None
    ) -> Session:
        """Create new session with automatic expiry."""
        ttl = ttl_seconds or self.default_ttl
        session_id = secrets.token_urlsafe(32)
        
        now = datetime.now()
        expires_at = now + timedelta(seconds=ttl)
        
        session = Session(
            session_id=session_id,
            user_id=user_id,
            email=email,
            created_at=now.isoformat(),
            expires_at=expires_at.isoformat(),
            metadata={}
        )
        
        # Store with TTL
        self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": {
                "user_id": session.user_id,
                "email": session.email,
                "created_at": session.created_at,
                "expires_at": session.expires_at,
                "metadata": session.metadata
            },
            "ttl_seconds": ttl
        })
        
        # Track user's sessions
        self.send_command({
            "cmd": "SET",
            "key": f"user_sessions:{user_id}",
            "value": session_id,
            "ttl_seconds": ttl
        })
        
        return session
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Retrieve session if not expired."""
        result = self.send_command({
            "cmd": "GET",
            "key": f"session:{session_id}"
        })
        
        if result.get("status") == "ok" and result.get("result"):
            data = result["result"]
            return Session(
                session_id=session_id,
                user_id=data["user_id"],
                email=data["email"],
                created_at=data["created_at"],
                expires_at=data["expires_at"],
                metadata=data.get("metadata", {})
            )
        return None
    
    def update_session(self, session_id: str, metadata: dict) -> bool:
        """Update session metadata."""
        session = self.get_session(session_id)
        if not session:
            return False
        
        session.metadata.update(metadata)
        
        # Calculate remaining TTL
        expires_at = datetime.fromisoformat(session.expires_at)
        remaining_ttl = max(0, int((expires_at - datetime.now()).total_seconds()))
        
        result = self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": {
                "user_id": session.user_id,
                "email": session.email,
                "created_at": session.created_at,
                "expires_at": session.expires_at,
                "metadata": session.metadata
            },
            "ttl_seconds": remaining_ttl
        })
        
        return result.get("status") == "ok"
    
    def delete_session(self, session_id: str) -> bool:
        """Delete session (logout)."""
        result = self.send_command({
            "cmd": "DEL",
            "key": f"session:{session_id}"
        })
        return result.get("status") == "ok"
    
    def extend_session(self, session_id: str, additional_seconds: int) -> bool:
        """Extend session expiry."""
        session = self.get_session(session_id)
        if not session:
            return False
        
        # Calculate new expiry
        current_expires = datetime.fromisoformat(session.expires_at)
        new_expires = current_expires + timedelta(seconds=additional_seconds)
        session.expires_at = new_expires.isoformat()
        
        # Calculate total TTL from now
        total_ttl = int((new_expires - datetime.now()).total_seconds())
        
        result = self.send_command({
            "cmd": "SET",
            "key": f"session:{session_id}",
            "value": {
                "user_id": session.user_id,
                "email": session.email,
                "created_at": session.created_at,
                "expires_at": session.expires_at,
                "metadata": session.metadata
            },
            "ttl_seconds": total_ttl
        })
        
        return result.get("status") == "ok"


# Example usage
if __name__ == "__main__":
    print("="*60)
    print("AhanaFlow Session Store Demo")
    print("="*60 + "\n")
    
    store = SessionStore()
    
    # 1. Create session (30-second TTL for demo)
    print("1. Creating session...")
    session = store.create_session(
        user_id="user_12345",
        email="alice@example.com",
        ttl_seconds=30  # 30 seconds for demo
    )
    print(f"   ✓ Session ID: {session.session_id}")
    print(f"   ✓ User: {session.email}")
    print(f"   ✓ Expires: {session.expires_at}\n")
    
    # 2. Retrieve session
    print("2. Retrieving session...")
    retrieved = store.get_session(session.session_id)
    if retrieved:
        print(f"   ✓ Found session for {retrieved.email}\n")
    
    # 3. Update session metadata
    print("3. Updating session metadata...")
    store.update_session(session.session_id, {
        "last_page": "/dashboard",
        "theme": "dark",
        "language": "en"
    })
    print("   ✓ Metadata updated\n")
    
    # 4. Retrieve updated session
    print("4. Retrieving updated session...")
    updated = store.get_session(session.session_id)
    if updated:
        print(f"   ✓ Metadata: {updated.metadata}\n")
    
    # 5. Extend session
    print("5. Extending session by 60 seconds...")
    store.extend_session(session.session_id, 60)
    extended = store.get_session(session.session_id)
    if extended:
        print(f"   ✓ New expiry: {extended.expires_at}\n")
    
    # 6. Wait and check expiry
    print("6. Waiting 5 seconds...")
    time.sleep(5)
    still_valid = store.get_session(session.session_id)
    if still_valid:
        print("   ✓ Session still valid\n")
    
    # 7. Delete session (logout)
    print("7. Deleting session (logout)...")
    store.delete_session(session.session_id)
    deleted = store.get_session(session.session_id)
    if not deleted:
        print("   ✓ Session deleted successfully\n")
    
    print("="*60)
    print("Session store demo complete")
    print("="*60)
