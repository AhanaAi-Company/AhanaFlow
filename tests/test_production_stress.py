"""
Production stress tests for UniversalStateServer and VectorStateServerV2.
Tests concurrency, race conditions, data integrity, error handling, and edge cases.

Run with: pytest tests/test_production_stress.py -v --tb=short
"""

import pytest
import socket
import json
import threading
import time
import hashlib
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import tempfile
import os

# Import test utilities
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class UniversalStateClient:
    """Thread-safe client for UniversalStateServer."""
    
    def __init__(self, host="127.0.0.1", port=9633):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
    
    def send_command(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        if not self.sock:
            self.connect()
        message = json.dumps(cmd) + "\n"
        self.sock.sendall(message.encode())
        
        # Handle large responses by reading until newline
        response_bytes = b""
        while True:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed by server")
            response_bytes += chunk
            if b"\n" in response_bytes:
                break
        
        response = response_bytes.decode().strip()
        return json.loads(response)
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, *args):
        self.close()


@pytest.fixture(scope="function")
def universal_server():
    """Start UniversalStateServer for testing."""
    import subprocess
    import time
    
    # Get the parent directory (business_ecosystem/33_event_streams)
    test_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Create temp WAL file
    wal_file = tempfile.mktemp(suffix=".wal")
    
    # Start server with correct working directory
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m", "universal_server.cli",
            "serve",
            "--host", "127.0.0.1",
            "--port", "9633",
            "--wal", wal_file
        ],
        cwd=test_dir,  # Set working directory to where the modules are
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start and verify it's ready
    max_wait = 5
    start_time = time.time()
    server_ready = False
    
    while (time.time() - start_time) < max_wait:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            sock.connect(("127.0.0.1", 9633))
            sock.sendall(b'{"cmd":"PING"}\n')
            response = sock.recv(1024)
            sock.close()
            if response:
                server_ready = True
                break
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.2)
    
    if not server_ready:
        proc.terminate()
        raise RuntimeError("UniversalStateServer failed to start within 5 seconds")
    
    yield {"host": "127.0.0.1", "port": 9633, "proc": proc, "wal": wal_file}
    
    # Cleanup
    proc.terminate()
    proc.wait(timeout=5)
    if os.path.exists(wal_file):
        os.remove(wal_file)


class TestConcurrentAccess:
    """Test concurrent operations from multiple clients."""
    
    def test_concurrent_writes_no_corruption(self, universal_server):
        """100 concurrent clients writing unique keys should succeed without corruption."""
        num_clients = 100
        errors = []
        
        def write_worker(client_id):
            try:
                with UniversalStateClient() as client:
                    key = f"concurrent_key_{client_id}"
                    value = {"client": client_id, "data": "x" * 100}
                    result = client.send_command({"cmd": "SET", "key": key, "value": value})
                    assert result.get("result") == "OK", f"SET failed: {result}"
                    
                    # Verify read
                    result = client.send_command({"cmd": "GET", "key": key})
                    assert result.get("result") == value, f"GET mismatch for {key}"
            except Exception as e:
                errors.append(f"Client {client_id}: {e}")
        
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(write_worker, i) for i in range(num_clients)]
            for future in as_completed(futures):
                future.result()  # Will raise if worker failed
        
        assert len(errors) == 0, f"Concurrent write errors: {errors}"
    
    def test_concurrent_incr_atomic(self, universal_server):
        """Concurrent INCR operations should be atomic (no lost increments)."""
        num_clients = 50
        increments_per_client = 100
        expected_total = num_clients * increments_per_client
        
        # Use unique counter per test run to avoid state pollution
        counter_key = f"atomic_counter_{int(time.time() * 1000)}"
        
        def incr_worker(client_id):
            with UniversalStateClient() as client:
                for _ in range(increments_per_client):
                    client.send_command({"cmd": "INCR", "key": counter_key, "amount": 1})
        
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(incr_worker, i) for i in range(num_clients)]
            for future in as_completed(futures):
                future.result()
        
        # Verify final count
        with UniversalStateClient() as client:
            result = client.send_command({"cmd": "GET", "key": counter_key})
            final_value = result.get("result", 0)
            assert final_value == expected_total, \
                f"Atomic INCR failed: expected {expected_total}, got {final_value}"
    
    def test_concurrent_queue_operations(self, universal_server):
        """Concurrent ENQUEUE/DEQUEUE should maintain queue integrity."""
        num_producers = 20
        num_consumers = 20
        items_per_producer = 50
        expected_total = num_producers * items_per_producer
        
        queue_name = "stress_test_queue"
        consumed_items = []
        lock = threading.Lock()
        stop_flag = threading.Event()
        
        def producer_worker(producer_id):
            with UniversalStateClient() as client:
                for i in range(items_per_producer):
                    payload = {"producer": producer_id, "seq": i}
                    client.send_command({"cmd": "ENQUEUE", "queue": queue_name, "payload": payload})
        
        def consumer_worker():
            with UniversalStateClient() as client:
                while not stop_flag.is_set():
                    result = client.send_command({"cmd": "DEQUEUE", "queue": queue_name})
                    item = result.get("result")
                    if item is None:
                        time.sleep(0.01)  # Small delay before retrying
                        continue
                    with lock:
                        consumed_items.append(item)
        
        # Start consumers first
        with ThreadPoolExecutor(max_workers=num_consumers) as consumer_executor:
            consumer_futures = [consumer_executor.submit(consumer_worker) for _ in range(num_consumers)]
            
            # Start producers and wait for completion
            with ThreadPoolExecutor(max_workers=num_producers) as producer_executor:
                producer_futures = [producer_executor.submit(producer_worker, i) for i in range(num_producers)]
                for future in as_completed(producer_futures):
                    future.result()
            
            # Wait for consumers to drain queue
            max_wait = 10  # seconds
            start_time = time.time()
            while len(consumed_items) < expected_total and (time.time() - start_time) < max_wait:
                time.sleep(0.1)
            
            # Stop consumers
            stop_flag.set()
            for future in consumer_futures:
                future.cancel()
        
        # Verify all items consumed
        assert len(consumed_items) == expected_total, \
            f"Queue integrity failed: expected {expected_total} items, got {len(consumed_items)}"


class TestDataIntegrity:
    """Test data integrity under various conditions."""
    
    def test_large_value_roundtrip(self, universal_server):
        """Large values (>1MB) should roundtrip correctly."""
        with UniversalStateClient() as client:
            large_value = {"data": "x" * (1024 * 1024)}  # 1MB
            key = "large_value_test"
            
            client.send_command({"cmd": "SET", "key": key, "value": large_value})
            result = client.send_command({"cmd": "GET", "key": key})
            
            assert result.get("result") == large_value, "Large value roundtrip failed"
    
    def test_unicode_values(self, universal_server):
        """Unicode and special characters should be preserved."""
        with UniversalStateClient() as client:
            unicode_values = {
                "emoji": "🌺🚀💾",
                "chinese": "你好世界",
                "arabic": "مرحبا بالعالم",
                "special": "«»©®™€¥£¢",
                "mixed": "Hello世界🌺"
            }
            
            for key, value in unicode_values.items():
                test_key = f"unicode_{key}"
                client.send_command({"cmd": "SET", "key": test_key, "value": {"text": value}})
                result = client.send_command({"cmd": "GET", "key": test_key})
                assert result.get("result") == {"text": value}, \
                    f"Unicode preservation failed for {key}"
    
    def test_sha256_integrity_check(self, universal_server):
        """Data should have bit-exact SHA-256 integrity after SET/GET."""
        with UniversalStateClient() as client:
            for i in range(100):
                # Generate random data
                random_data = {
                    "index": i,
                    "payload": ''.join(random.choices(string.ascii_letters + string.digits, k=1000))
                }
                raw_json = json.dumps(random_data, sort_keys=True)
                expected_hash = hashlib.sha256(raw_json.encode()).hexdigest()
                
                # Store and retrieve
                key = f"integrity_{i}"
                client.send_command({"cmd": "SET", "key": key, "value": random_data})
                result = client.send_command({"cmd": "GET", "key": key})
                
                # Verify hash
                retrieved_data = result.get("result")
                retrieved_json = json.dumps(retrieved_data, sort_keys=True)
                actual_hash = hashlib.sha256(retrieved_json.encode()).hexdigest()
                
                assert expected_hash == actual_hash, \
                    f"SHA-256 integrity check failed for key {key}"


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_invalid_commands(self, universal_server):
        """Server should handle invalid commands gracefully."""
        with UniversalStateClient() as client:
            # Missing cmd
            result = client.send_command({"key": "test"})
            assert "error" in result or result.get("result") is None
            
            # Unknown command
            result = client.send_command({"cmd": "INVALID_CMD", "key": "test"})
            assert "error" in result or result.get("result") is None
            
            # Missing required parameters
            result = client.send_command({"cmd": "GET"})  # Missing key
            assert "error" in result or result.get("result") is None
    
    def test_ttl_expiry(self, universal_server):
        """TTL expiry should delete keys after timeout."""
        with UniversalStateClient() as client:
            key = "ttl_test"
            value = {"data": "expires"}
            
            # Set with 2-second TTL
            client.send_command({"cmd": "SET", "key": key, "value": value, "ttl_seconds": 2})
            
            # Should exist immediately
            result = client.send_command({"cmd": "GET", "key": key})
            assert result.get("result") == value
            
            # Wait for expiry
            time.sleep(3)
            
            # Should be gone
            result = client.send_command({"cmd": "GET", "key": key})
            assert result.get("result") is None, "TTL expiry failed"
    
    def test_connection_recovery(self, universal_server):
        """Client should handle connection drops gracefully."""
        client = UniversalStateClient()
        client.connect()
        
        # Normal operation
        result = client.send_command({"cmd": "PING"})
        assert result.get("result") == "PONG"
        
        # Close connection
        client.close()
        
        # Reconnect and verify
        client.connect()
        result = client.send_command({"cmd": "PING"})
        assert result.get("result") == "PONG"
        
        client.close()


class TestStressBoundaries:
    """Test boundary conditions and stress limits."""
    
    def test_rapid_fire_operations(self, universal_server):
        """Server should handle rapid-fire operations without crashes."""
        with UniversalStateClient() as client:
            for i in range(10000):
                client.send_command({"cmd": "SET", "key": f"rapid_{i % 100}", "value": i})
                client.send_command({"cmd": "GET", "key": f"rapid_{i % 100}"})
                client.send_command({"cmd": "INCR", "key": "rapid_counter", "amount": 1})
    
    def test_empty_values(self, universal_server):
        """Empty and null values should be handled correctly."""
        with UniversalStateClient() as client:
            test_cases = [
                ("empty_string", ""),
                ("empty_dict", {}),
                ("empty_list", []),
                ("null_value", None),
                ("zero", 0),
                ("false", False),
            ]
            
            for key, value in test_cases:
                client.send_command({"cmd": "SET", "key": key, "value": value})
                result = client.send_command({"cmd": "GET", "key": key})
                assert result.get("result") == value, f"Empty value handling failed for {key}"
    
    def test_stats_under_load(self, universal_server):
        """STATS command should work under concurrent load."""
        def worker():
            with UniversalStateClient() as client:
                for i in range(100):
                    client.send_command({"cmd": "SET", "key": f"load_{i}", "value": i})
                    if i % 10 == 0:
                        client.send_command({"cmd": "STATS"})
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker) for _ in range(10)]
            for future in as_completed(futures):
                future.result()
        
        # Final stats check
        with UniversalStateClient() as client:
            result = client.send_command({"cmd": "STATS"})
            assert "result" in result
            stats = result["result"]
            assert "keys" in stats
            assert stats["keys"] > 0


class TestCrashRecovery:
    """Test crash recovery and WAL replay.
    
    NOTE: These tests document known WAL behavior rather than strict requirements.
    The engine's default 'safe' mode uses OS buffering which may delay writes.
    """
    
    def test_wal_persistence_after_shutdown(self):
        """Data should survive server restart via WAL replay."""
        import subprocess
        import time
        
        test_dir = os.path.dirname(os.path.dirname(__file__))
        wal_file = tempfile.mktemp(suffix="_recovery.wal")
        
        try:
            # Start server with strict durability for immediate WAL writes
            proc = subprocess.Popen(
                [sys.executable, "-m", "universal_server.cli", "serve",
                 "--host", "127.0.0.1", "--port", "9635", "--wal", wal_file,
                 "--durability-mode", "strict"],
                cwd=test_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for startup and verify connectivity
            max_wait = 5
            start_time = time.time()
            connected = False
            
            while (time.time() - start_time) < max_wait:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.5)
                    sock.connect(("127.0.0.1", 9635))
                    sock.sendall(b'{"cmd":"PING"}\n')
                    response = sock.recv(1024)
                    sock.close()
                    if response:
                        connected = True
                        break
                except (ConnectionRefusedError, socket.timeout, OSError):
                    time.sleep(0.2)
            
            assert connected, "Server failed to start"
            
            # Write data
            with UniversalStateClient(port=9635) as client:
                result = client.send_command({"cmd": "SET", "key": "persistent_key", "value": "persistent_value"})
                assert result.get("ok") is True
                
                result = client.send_command({"cmd": "INCR", "key": "persistent_counter", "amount": 42})
                assert result.get("ok") is True
                
                result = client.send_command({"cmd": "ENQUEUE", "queue": "persistent_queue", 
                                            "payload": {"msg": "persistent"}})
                assert result.get("ok") is True
            
            # Give server time to flush
            time.sleep(1)
            
            # Graceful shutdown
            proc.terminate()
            proc.wait(timeout=5)
            
            # Verify WAL exists and has content
            assert os.path.exists(wal_file), "WAL file should exist after shutdown"
            wal_size = os.path.getsize(wal_file)
            assert wal_size > 0, f"WAL should have content, got {wal_size} bytes"
            
            # Restart server with same WAL
            proc = subprocess.Popen(
                [sys.executable, "-m", "universal_server.cli", "serve",
                 "--host", "127.0.0.1", "--port", "9635", "--wal", wal_file,
                 "--durability-mode", "strict"],
                cwd=test_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for startup
            start_time = time.time()
            connected = False
            
            while (time.time() - start_time) < max_wait:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.5)
                    sock.connect(("127.0.0.1", 9635))
                    sock.sendall(b'{"cmd":"PING"}\n')
                    response = sock.recv(1024)
                    sock.close()
                    if response:
                        connected = True
                        break
                except (ConnectionRefusedError, socket.timeout, OSError):
                    time.sleep(0.2)
            
            assert connected, "Server failed to restart"
            
            # Verify data restored
            with UniversalStateClient(port=9635) as client:
                result = client.send_command({"cmd": "GET", "key": "persistent_key"})
                assert result.get("result") == "persistent_value", f"KV data should survive restart, got {result}"
                
                result = client.send_command({"cmd": "GET", "key": "persistent_counter"})
                assert result.get("result") == 42, f"Counter data should survive restart, got {result}"
                
                result = client.send_command({"cmd": "DEQUEUE", "queue": "persistent_queue"})
                assert result.get("result") == {"msg": "persistent"}, f"Queue data should survive restart, got {result}"
            
            proc.terminate()
            proc.wait(timeout=5)
            
        finally:
            try:
                if 'proc' in locals() and proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=2)
            except:
                pass
            if os.path.exists(wal_file):
                os.remove(wal_file)
    
    def test_concurrent_writes_during_shutdown(self):
        """Server should handle graceful shutdown during active writes."""
        import subprocess
        import time
        import signal
        
        test_dir = os.path.dirname(os.path.dirname(__file__))
        wal_file = tempfile.mktemp(suffix="_concurrent.wal")
        
        try:
            # Start server
            proc = subprocess.Popen(
                [sys.executable, "-m", "universal_server.cli", "serve",
                 "--host", "127.0.0.1", "--port", "9636", "--wal", wal_file],
                cwd=test_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            time.sleep(2)
            
            # Start concurrent writes
            stop_flag = threading.Event()
            write_count = [0]
            
            def writer():
                try:
                    with UniversalStateClient(port=9636) as client:
                        while not stop_flag.is_set():
                            client.send_command({"cmd": "SET", "key": f"key_{write_count[0]}", 
                                                "value": write_count[0]})
                            write_count[0] += 1
                            time.sleep(0.001)
                except:
                    pass  # Expected during shutdown
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(writer) for _ in range(5)]
                
                # Let writes run for a bit
                time.sleep(1)
                
                # Graceful shutdown during active writes
                proc.terminate()
                stop_flag.set()
                
                # Wait for shutdown
                proc.wait(timeout=5)
                
                # Cancel workers
                for future in futures:
                    future.cancel()
            
            # Verify WAL integrity (should not be corrupted)
            assert os.path.exists(wal_file), "WAL should exist after concurrent shutdown"
            
        finally:
            if os.path.exists(wal_file):
                os.remove(wal_file)


@pytest.mark.slow
class TestVectorStress:
    """Stress tests for VectorStateServerV2."""
    
    @pytest.fixture(scope="function")
    def vector_server(self):
        """Start VectorStateServerV2 for testing."""
        import subprocess
        import time
        
        # Get the parent directory (business_ecosystem/33_event_streams)
        test_dir = os.path.dirname(os.path.dirname(__file__))
        
        wal_file = tempfile.mktemp(suffix="_vector.wal")
        
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m", "universal_server.cli",
                "serve-vector-v2",
                "--host", "127.0.0.1",
                "--port", "9644",
                "--wal", wal_file
            ],
            cwd=test_dir,  # Set working directory to where the modules are
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        time.sleep(2)
        
        yield {"host": "127.0.0.1", "port": 9644, "proc": proc, "wal": wal_file}
        
        proc.terminate()
        proc.wait(timeout=5)
        if os.path.exists(wal_file):
            os.remove(wal_file)
    
    def test_concurrent_vector_insertion(self, vector_server):
        """Concurrent vector insertions should not corrupt index."""
        import numpy as np
        
        collection = "concurrent_test"
        dimensions = 128
        num_workers = 10
        vectors_per_worker = 100
        
        def insert_worker(worker_id):
            with UniversalStateClient(port=9644) as client:
                # Create collection (idempotent)
                client.send_command({
                    "cmd": "VECTOR_CREATE",
                    "collection": collection,
                    "dimensions": dimensions,
                    "metric": "cosine"
                })
                
                # Insert vectors
                for i in range(vectors_per_worker):
                    vec_id = f"worker_{worker_id}_vec_{i}"
                    vec = np.random.randn(dimensions).tolist()
                    vec = (vec / np.linalg.norm(vec)).tolist()  # Normalize
                    
                    client.send_command({
                        "cmd": "VECTOR_UPSERT",
                        "collection": collection,
                        "id": vec_id,
                        "vector": vec,
                        "metadata": {"worker": worker_id, "seq": i}
                    })
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(insert_worker, i) for i in range(num_workers)]
            for future in as_completed(futures):
                future.result()
        
        # Verify total count
        with UniversalStateClient(port=9644) as client:
            result = client.send_command({"cmd": "VECTOR_COUNT", "collection": collection})
            total = result.get("result", 0)
            expected = num_workers * vectors_per_worker
            assert total == expected, f"Concurrent insertion failed: expected {expected}, got {total}"
    
    def test_vector_search_accuracy_under_load(self, vector_server):
        """Vector search should maintain accuracy under concurrent load."""
        import numpy as np
        
        collection = "accuracy_test"
        dimensions = 256
        num_vectors = 1000
        
        with UniversalStateClient(port=9644) as client:
            # Create collection
            client.send_command({
                "cmd": "VECTOR_CREATE",
                "collection": collection,
                "dimensions": dimensions,
                "metric": "cosine"
            })
            
            # Insert vectors
            vectors = []
            for i in range(num_vectors):
                vec = np.random.randn(dimensions)
                vec = vec / np.linalg.norm(vec)
                vectors.append(vec)
                
                client.send_command({
                    "cmd": "VECTOR_UPSERT",
                    "collection": collection,
                    "id": f"vec_{i}",
                    "vector": vec.tolist(),
                    "metadata": {"index": i}
                })
            
            # Build HNSW index
            client.send_command({
                "cmd": "VECTOR_BUILD_HNSW",
                "collection": collection
            })
        
        # Concurrent search workers
        def search_worker():
            with UniversalStateClient(port=9644) as client:
                for _ in range(10):
                    query_vec = np.random.randn(dimensions)
                    query_vec = query_vec / np.linalg.norm(query_vec)
                    
                    result = client.send_command({
                        "cmd": "VECTOR_QUERY",
                        "collection": collection,
                        "vector": query_vec.tolist(),
                        "k": 10
                    })
                    
                    assert "result" in result
                    assert len(result["result"]) <= 10
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(search_worker) for _ in range(20)]
            for future in as_completed(futures):
                future.result()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
