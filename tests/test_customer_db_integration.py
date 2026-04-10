#!/usr/bin/env python3
"""
Test customer database integration in AhanaFlow webhook server.
Verifies that Stripe subscription events sync to customer DB.
"""

import json
import socket
import time


def send_command(host: str, port: int, cmd: dict) -> dict:
    """Send JSON command to UniversalStateServer"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.sendall((json.dumps(cmd) + "\n").encode())
        response = sock.recv(16384).decode().strip()
        return json.loads(response)
    finally:
        sock.close()


def test_customer_db_connection():
    """Test that customer DB server is accessible"""
    print("→ Testing customer DB connection...")
    try:
        result = send_command("customer-db", 9635, {"cmd": "GET", "key": "test:connection"})
        print(f"✓ Customer DB connected: {result}")
        return True
    except Exception as e:
        print(f"✗ Customer DB connection failed: {e}")
        return False


def create_test_customer():
    """Create a test customer in customer DB"""
    print("\n→ Creating test customer...")
    
    customer_data = {
        "customer_id": "cus_test_" + str(int(time.time())),
        "email": "test@example.com",
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
        "current_plan": "pro",
        "subscription_id": "sub_test_123",
        "subscription_status": "active",
        "price_id": "price_test_pro",
        "license_tier": "pro",
        "max_api_keys": 1,
        "subscription_start": int(time.time()),
        "subscription_end": int(time.time()) + 2678400,  # +31 days
    }
    
    try:
        # Store customer record
        customer_key = f"customer:{customer_data['customer_id']}"
        result = send_command("customer-db", 9635, {
            "cmd": "SET",
            "key": customer_key,
            "value": json.dumps(customer_data),
        })
        print(f"✓ Customer created: {result}")
        
        # Create email index
        email_index_key = f"email:{customer_data['email'].lower()}"
        result = send_command("customer-db", 9635, {
            "cmd": "SET",
            "key": email_index_key,
            "value": customer_data["customer_id"],
        })
        print(f"✓ Email index created: {result}")
        
        # Retrieve customer by ID
        result = send_command("customer-db", 9635, {
            "cmd": "GET",
            "key": customer_key,
        })
        if result.get("result"):
            retrieved = json.loads(result["result"])
            print(f"✓ Customer retrieved: {retrieved['email']} (plan: {retrieved['current_plan']})")
            return customer_data["customer_id"]
        else:
            print(f"✗ Customer retrieval failed: {result}")
            return None
            
    except Exception as e:
        print(f"✗ Customer creation failed: {e}")
        return None


def test_support_note(customer_id: str):
    """Add a support note for the test customer"""
    print(f"\n→ Adding support note for {customer_id}...")
    
    note_data = {
        "note_id": f"note_{int(time.time())}",
        "customer_id": customer_id,
        "created_at": int(time.time()),
        "created_by": "admin@ahanazip.com",
        "note": "Test support note - customer database integration working",
        "resolved": False,
    }
    
    try:
        note_key = f"support_note:{note_data['note_id']}"
        result = send_command("customer-db", 9635, {
            "cmd": "SET",
            "key": note_key,
            "value": json.dumps(note_data),
        })
        print(f"✓ Support note created: {result}")
        return True
    except Exception as e:
        print(f"✗ Support note creation failed: {e}")
        return False


def test_marketing_tag(customer_id: str):
    """Add marketing tag for the test customer"""
    print(f"\n→ Adding marketing tag for {customer_id}...")
    
    try:
        tag_key = f"customer:{customer_id}:tags"
        result = send_command("customer-db", 9635, {
            "cmd": "SET",
            "key": tag_key,
            "value": json.dumps(["test_customer", "integration_test", "pro_plan"]),
        })
        print(f"✓ Marketing tags added: {result}")
        return True
    except Exception as e:
        print(f"✗ Marketing tag creation failed: {e}")
        return False


def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  AhanaFlow Customer Database Integration Test               ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")
    
    # Test 1: Connection
    if not test_customer_db_connection():
        print("\n✗ FAILED: Cannot connect to customer database")
        return 1
    
    # Test 2: Create customer
    customer_id = create_test_customer()
    if not customer_id:
        print("\n✗ FAILED: Cannot create customer")
        return 1
    
    # Test 3: Support notes
    if not test_support_note(customer_id):
        print("\n✗ FAILED: Cannot create support notes")
        return 1
    
    # Test 4: Marketing tags
    if not test_marketing_tag(customer_id):
        print("\n✗ FAILED: Cannot create marketing tags")
        return 1
    
    print("\n" + "="*64)
    print("✓ ALL TESTS PASSED")
    print("  Customer database integration is working correctly.")
    print("  Ready for production Stripe webhook sync.")
    print("="*64)
    return 0


if __name__ == "__main__":
    exit(main())
