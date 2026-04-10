"""
AhanaFlow Rate Limiter Example

Token bucket rate limiter using atomic INCR commands.
Prevents abuse by limiting requests per user/IP/API key.
"""

import time
import socket
import json
from typing import Optional


class RateLimiter:
    """Token bucket rate limiter using AhanaFlow atomic counters."""
    
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
    
    def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: int
    ) -> tuple[bool, int]:
        """
        Check if request is within rate limit.
        
        Args:
            key: Unique identifier (user_id, ip_address, api_key)
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds
            
        Returns:
            (allowed, remaining_requests)
        """
        # Use window-bucketed key
        now = int(time.time())
        window_start = (now // window_seconds) * window_seconds
        rate_key = f"ratelimit:{key}:{window_start}"
        
        # Atomic increment
        result = self.send_command({
            "cmd": "INCR",
            "key": rate_key,
            "amount": 1
        })
        
        current_count = result["result"]
        
        # Set TTL on first request in window
        if current_count == 1:
            self.send_command({
                "cmd": "SET",
                "key": rate_key,
                "value": current_count,
                "ttl_seconds": window_seconds * 2  # Extra buffer
            })
        
        # Check limit
        allowed = current_count <= max_requests
        remaining = max(0, max_requests - current_count)
        
        return allowed, remaining


# Example usage
if __name__ == "__main__":
    limiter = RateLimiter()
    
    # Rate limit: 10 requests per minute per user
    user_id = "user_12345"
    max_requests = 10
    window = 60  # seconds
    
    print(f"Rate Limit: {max_requests} requests per {window}s for {user_id}\n")
    
    # Simulate 15 requests
    for i in range(15):
        allowed, remaining = limiter.check_rate_limit(
            user_id,
            max_requests,
            window
        )
        
        status = "✓ ALLOWED" if allowed else "✗ BLOCKED"
        print(f"Request {i+1:2d}: {status} (remaining: {remaining})")
        
        if not allowed:
            print(f"\nRate limit exceeded! Wait {window}s before retry.\n")
            break
        
        time.sleep(0.1)  # Small delay between requests
    
    print("\n" + "="*60)
    print("Rate limiter demo complete")
    print("="*60)
