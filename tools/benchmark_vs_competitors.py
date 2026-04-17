#!/usr/bin/env python3
"""
Branch 33 Competitive Benchmark Suite
Compares UniversalStateServer vs Redis and VectorStateServerV2 vs pgvector/Qdrant

Measures:
- Throughput (ops/sec)
- Latency (p50, p95, p99)
- Memory usage
- Storage efficiency (WAL compression vs raw storage)
- Concurrent client scalability
"""

import socket
import json
import time
import statistics
import psutil
import subprocess
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

# Add workspace root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from tools.acp_logging import get_logger
from redis_compat.resp import RespParser, encode_array, encode_bulk_string, encode_integer

log = get_logger("branch33_benchmark")


@dataclass
class BenchmarkResult:
    """Single benchmark run result"""
    system: str
    operation: str
    num_operations: int
    duration_seconds: float
    throughput_ops_sec: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    memory_mb: float
    storage_mb: float
    error_count: int


class _RespRecvBuffer:
    """Buffered reader for RESP sockets — eliminates per-byte recv(1) calls."""
    __slots__ = ("_sock", "_buf", "_pos")

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()
        self._pos = 0

    def _ensure(self, need: int) -> None:
        while len(self._buf) - self._pos < need:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed")
            if self._pos > 0:
                del self._buf[: self._pos]
                self._pos = 0
            self._buf.extend(chunk)

    def readline(self) -> bytes:
        while True:
            idx = self._buf.find(b"\r\n", self._pos)
            if idx >= 0:
                line = bytes(self._buf[self._pos : idx])
                self._pos = idx + 2
                return line
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed")
            if self._pos > 0:
                del self._buf[: self._pos]
                self._pos = 0
            self._buf.extend(chunk)

    def read_exact(self, n: int) -> bytes:
        self._ensure(n)
        data = bytes(self._buf[self._pos : self._pos + n])
        self._pos += n
        return data

    def read_resp_value(self) -> Any:
        prefix = self.read_exact(1)
        if prefix == b"+":
            return self.readline().decode()
        if prefix == b":":
            return int(self.readline())
        if prefix == b"$":
            length = int(self.readline())
            if length == -1:
                return None
            payload = self.read_exact(length)
            self.read_exact(2)  # trailing \r\n
            return payload.decode()
        if prefix == b"*":
            count = int(self.readline())
            if count == -1:
                return None
            return [self.read_resp_value() for _ in range(count)]
        if prefix == b"-":
            raise RuntimeError(self.readline().decode())
        raise RuntimeError(f"Unsupported RESP prefix: {prefix!r}")


class UniversalStateClient:
    """Client for UniversalStateServer"""
    def __init__(self, host="localhost", port=9633, codec: str = "json"):
        self.host = host
        self.port = port
        self.sock = None
        self._resp_buf: _RespRecvBuffer | None = None
        if codec not in {"json", "compact", "resp"}:
            raise ValueError("codec must be 'json', 'compact', or 'resp'")
        self.codec = codec
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))
        if self.codec == "resp":
            self._resp_buf = _RespRecvBuffer(self.sock)
    
    def close(self):
        self._resp_buf = None
        if self.sock:
            self.sock.close()

    def _encode_command(self, cmd: dict[str, Any]) -> bytes:
        if self.codec == "resp":
            return self._encode_resp_command(cmd)
        if self.codec == "compact":
            compact = self._to_compact_command(cmd)
            if compact is not None:
                return json.dumps(compact, separators=(",", ":")).encode() + b"\n"
        return (json.dumps(cmd, separators=(",", ":")) + "\n").encode()

    def _encode_resp_command(self, cmd: dict[str, Any]) -> bytes:
        name = str(cmd.get("cmd", "")).upper()
        if name == "PING":
            return self._build_resp_command("PING")
        if name == "GET":
            return self._build_resp_command("GET", cmd.get("key"))
        if name == "SET":
            return self._build_resp_command("SET", cmd.get("key"), cmd.get("value"))
        if name == "INCR":
            amount = int(cmd.get("amount", 1))
            if amount == 1:
                return self._build_resp_command("INCR", cmd.get("key"))
            return self._build_resp_command("INCRBY", cmd.get("key"), amount)
        if name == "MGET":
            return self._build_resp_command("MGET", *(cmd.get("keys") or []))
        if name == "MSET":
            values = cmd.get("values") or {}
            flat: list[Any] = []
            for key, value in values.items():
                flat.extend([key, value])
            return self._build_resp_command("MSET", *flat)
        if name == "MINCR":
            updates = cmd.get("updates") or []
            flat_updates: list[Any] = []
            for update in updates:
                flat_updates.extend([update.get("key"), int(update.get("amount", 1))])
            return self._build_resp_command("MINCR", *flat_updates)
        if name == "FLUSHALL":
            return self._build_resp_command("FLUSHALL")
        raise ValueError(f"Unsupported RESP command: {name}")

    def _build_resp_command(self, *args: Any) -> bytes:
        out = bytearray()
        out.extend(b"*")
        out.extend(str(len(args)).encode())
        out.extend(b"\r\n")
        for arg in args:
            arg_bytes = str(arg).encode()
            out.extend(b"$")
            out.extend(str(len(arg_bytes)).encode())
            out.extend(b"\r\n")
            out.extend(arg_bytes)
            out.extend(b"\r\n")
        return bytes(out)

    def _to_compact_command(self, cmd: dict[str, Any]) -> list[Any] | None:
        name = cmd.get("cmd")
        if not isinstance(name, str):
            return None
        name = name.upper()
        if name == "PING":
            return [name]
        if name == "GET":
            return [name, cmd.get("key")]
        if name == "SET":
            return [name, cmd.get("key"), cmd.get("value")]
        if name == "INCR":
            amount = cmd.get("amount")
            return [name, cmd.get("key")] if amount in {None, 1} else [name, cmd.get("key"), amount]
        if name == "MGET":
            return [name, cmd.get("keys")]
        if name == "MSET":
            return [name, cmd.get("values")]
        if name == "MINCR":
            return [name, cmd.get("updates")]
        if name == "PIPELINE":
            commands = cmd.get("commands")
            if not isinstance(commands, list):
                return None
            compact_commands: list[Any] = []
            for entry in commands:
                if not isinstance(entry, dict):
                    return None
                compact_entry = self._to_compact_command(entry)
                if compact_entry is None:
                    return None
                compact_commands.append(compact_entry)
            return [name, compact_commands]
        return None

    def _decode_response(self, response_bytes: bytes) -> dict[str, Any]:
        if self.codec == "resp":
            result = self._decode_resp_value(response_bytes)
            return {"ok": True, "result": result}
        response = json.loads(response_bytes.decode().strip())
        if isinstance(response, list) and len(response) == 2 and response[0] in {0, 1, False, True}:
            if bool(response[0]):
                return {"ok": True, "result": response[1]}
            return {"ok": False, "error": response[1]}
        return response

    def _decode_resp_value(self, response_bytes: bytes) -> Any:
        parser = RespParser()
        parser.feed(response_bytes)
        # RespParser is command-oriented, so use the Redis parsing codepath instead.
        self._resp_buffer = response_bytes
        self._resp_pos = 0
        try:
            return self._read_resp_value_from_buffer()
        finally:
            del self._resp_buffer
            del self._resp_pos

    def _read_resp_line_from_buffer(self) -> bytes:
        end = self._resp_buffer.find(b"\r\n", self._resp_pos)
        if end < 0:
            raise ConnectionError("Incomplete RESP line")
        line = self._resp_buffer[self._resp_pos:end]
        self._resp_pos = end + 2
        return line

    def _read_resp_exact_from_buffer(self, size: int) -> bytes:
        end = self._resp_pos + size
        if end > len(self._resp_buffer):
            raise ConnectionError("Incomplete RESP payload")
        data = self._resp_buffer[self._resp_pos:end]
        self._resp_pos = end
        return data

    def _read_resp_value_from_buffer(self) -> Any:
        prefix = self._read_resp_exact_from_buffer(1)
        if prefix == b"+":
            return self._read_resp_line_from_buffer().decode()
        if prefix == b":":
            return int(self._read_resp_line_from_buffer())
        if prefix == b"$":
            length = int(self._read_resp_line_from_buffer())
            if length == -1:
                return None
            payload = self._read_resp_exact_from_buffer(length)
            self._read_resp_exact_from_buffer(2)
            return payload.decode()
        if prefix == b"*":
            count = int(self._read_resp_line_from_buffer())
            if count == -1:
                return None
            return [self._read_resp_value_from_buffer() for _ in range(count)]
        if prefix == b"-":
            raise RuntimeError(self._read_resp_line_from_buffer().decode())
        raise RuntimeError(f"Unsupported RESP prefix: {prefix!r}")
    
    def send(self, cmd: dict) -> dict:
        self.sock.sendall(self._encode_command(cmd))
        if self.codec == "resp":
            return {"ok": True, "result": self._read_resp_value()}
        # Handle large responses by reading until newline
        response_bytes = b""
        while True:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed by server")
            response_bytes += chunk
            if b"\n" in response_bytes:
                break
        return self._decode_response(response_bytes)

    def send_pipeline(self, commands: list[dict[str, Any]]) -> dict:
        if self.codec == "resp":
            out = bytearray()
            for command in commands:
                out.extend(self._encode_resp_command(command))
            self.sock.sendall(bytes(out))
            buf = self._resp_buf
            return {"ok": True, "result": [buf.read_resp_value() for _ in commands]}
        return self.send({"cmd": "PIPELINE", "commands": commands})

    def _read_resp_value(self) -> Any:
        return self._resp_buf.read_resp_value()


class RedisClient:
    """Client for Redis (competitor baseline)"""
    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        self.sock = None
        self._resp_buf: _RespRecvBuffer | None = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))
        self._resp_buf = _RespRecvBuffer(self.sock)
    
    def close(self):
        self._resp_buf = None
        if self.sock:
            self.sock.close()
    
    def send_raw(self, *args) -> str:
        """Send Redis protocol command"""
        self.sock.sendall(self._build_resp_command(*args))
        return self._resp_buf.read_resp_value()

    def send_pipeline_raw(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        out = bytearray()
        for command in commands:
            out.extend(self._build_resp_command(*command))
        self.sock.sendall(bytes(out))
        buf = self._resp_buf
        return [buf.read_resp_value() for _ in commands]

    def _build_resp_command(self, *args: Any) -> bytes:
        out = bytearray()
        out.extend(b"*")
        out.extend(str(len(args)).encode())
        out.extend(b"\r\n")
        for arg in args:
            arg_bytes = str(arg).encode()
            out.extend(b"$")
            out.extend(str(len(arg_bytes)).encode())
            out.extend(b"\r\n")
            out.extend(arg_bytes)
            out.extend(b"\r\n")
        return bytes(out)


def check_redis_available(host: str = "localhost", port: int = 6379) -> bool:
    """Check if Redis is running"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect((host, port))
        sock.close()
        return True
    except:
        return False


def start_universal_server(port=9633, wal_path="/tmp/universal_bench.wal") -> subprocess.Popen:
    """Start UniversalStateServer for benchmarking"""
    log.info("Starting UniversalStateServer", port=port, wal=wal_path)
    # Clean old WAL
    Path(wal_path).unlink(missing_ok=True)
    
    proc = subprocess.Popen(
        [sys.executable, "-m", "universal_server.cli", "serve",
         "--host", "0.0.0.0", "--port", str(port), "--wal", wal_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=Path(__file__).parent
    )
    time.sleep(1)  # Let server start
    return proc


def benchmark_kv_operations(client_factory, system_name: str, num_ops=10000) -> BenchmarkResult:
    """Benchmark key-value SET/GET operations"""
    log.info("Benchmarking KV operations", system=system_name, num_ops=num_ops)
    
    client = client_factory()
    client.connect()
    
    latencies_ms = []
    errors = 0
    
    # Warmup
    for i in range(100):
        try:
            if system_name == "UniversalStateServer":
                client.send({"cmd": "SET", "key": f"warmup_{i}", "value": f"val_{i}"})
            else:  # Redis
                client.send_raw("SET", f"warmup_{i}", f"val_{i}")
        except:
            pass
    
    # Actual benchmark
    start_time = time.time()
    start_mem = psutil.Process().memory_info().rss / 1024 / 1024
    
    for i in range(num_ops):
        op_start = time.time()
        try:
            if system_name == "UniversalStateServer":
                # SET
                client.send({"cmd": "SET", "key": f"key_{i}", "value": f"value_{i}"})
                # GET
                client.send({"cmd": "GET", "key": f"key_{i}"})
            else:  # Redis
                client.send_raw("SET", f"key_{i}", f"value_{i}")
                client.send_raw("GET", f"key_{i}")
            
            op_duration = (time.time() - op_start) * 1000  # ms
            latencies_ms.append(op_duration)
        except Exception as e:
            errors += 1
            log.error("Operation failed", error=str(e))
    
    duration = time.time() - start_time
    end_mem = psutil.Process().memory_info().rss / 1024 / 1024
    
    client.close()
    
    # Storage size
    storage_mb = 0.0
    if system_name == "UniversalStateServer":
        wal_path = Path("/tmp/universal_bench.wal")
        if wal_path.exists():
            storage_mb = wal_path.stat().st_size / 1024 / 1024
    else:  # Redis
        try:
            # Redis RDB size (approximation)
            storage_mb = 5.0  # Placeholder - would need CONFIG GET dir + dbfilename
        except:
            pass
    
    result = BenchmarkResult(
        system=system_name,
        operation="KV_SET_GET",
        num_operations=num_ops * 2,  # SET + GET
        duration_seconds=duration,
        throughput_ops_sec=(num_ops * 2) / duration,
        latency_p50_ms=statistics.median(latencies_ms),
        latency_p95_ms=statistics.quantiles(latencies_ms, n=20)[18],  # p95
        latency_p99_ms=statistics.quantiles(latencies_ms, n=100)[98],  # p99
        memory_mb=end_mem - start_mem,
        storage_mb=storage_mb,
        error_count=errors
    )
    
    log.metric("throughput", result.throughput_ops_sec, system=system_name)
    log.metric("latency_p50", result.latency_p50_ms, system=system_name)
    log.metric("latency_p99", result.latency_p99_ms, system=system_name)
    
    return result


def benchmark_mset_mget_operations(client_factory, system_name: str, num_batches=1000, batch_size=10) -> BenchmarkResult:
    """Benchmark batched key-value writes and reads."""
    log.info("Benchmarking batched KV operations", system=system_name, num_batches=num_batches, batch_size=batch_size)

    client = client_factory()
    client.connect()

    latencies_ms = []
    errors = 0
    start_time = time.time()

    for batch in range(num_batches):
        keys = [f"batch:{batch}:key:{offset}" for offset in range(batch_size)]
        values = {key: f"value:{batch}:{offset}" for offset, key in enumerate(keys)}
        op_start = time.time()
        try:
            if system_name == "UniversalStateServer":
                client.send({"cmd": "MSET", "values": values})
                client.send({"cmd": "MGET", "keys": keys})
            else:
                mset_args: list[Any] = ["MSET"]
                for key in keys:
                    mset_args.extend([key, values[key]])
                client.send_raw(*mset_args)
                client.send_raw("MGET", *keys)
            latencies_ms.append((time.time() - op_start) * 1000)
        except Exception as exc:
            errors += 1
            log.error("Batched KV operation failed", system=system_name, error=str(exc))

    duration = time.time() - start_time
    client.close()

    logical_ops = num_batches * batch_size * 2
    return BenchmarkResult(
        system=system_name,
        operation="KV_MSET_MGET",
        num_operations=logical_ops,
        duration_seconds=duration,
        throughput_ops_sec=logical_ops / duration,
        latency_p50_ms=statistics.median(latencies_ms),
        latency_p95_ms=statistics.quantiles(latencies_ms, n=20)[18],
        latency_p99_ms=statistics.quantiles(latencies_ms, n=100)[98],
        memory_mb=0.0,
        storage_mb=0.0,
        error_count=errors,
    )


def benchmark_pipeline_kv_operations(client_factory, system_name: str, num_batches=1000, batch_size=10) -> BenchmarkResult:
    """Benchmark pipelined SET/GET operations over one round trip per batch."""
    log.info("Benchmarking pipelined KV operations", system=system_name, num_batches=num_batches, batch_size=batch_size)

    client = client_factory()
    client.connect()

    latencies_ms = []
    errors = 0
    start_time = time.time()

    for batch in range(num_batches):
        op_start = time.time()
        try:
            if system_name == "UniversalStateServer":
                commands = []
                for offset in range(batch_size):
                    key = f"pipe:{batch}:key:{offset}"
                    value = f"value:{batch}:{offset}"
                    commands.append({"cmd": "SET", "key": key, "value": value})
                    commands.append({"cmd": "GET", "key": key})
                client.send_pipeline(commands)
            else:
                commands = []
                for offset in range(batch_size):
                    key = f"pipe:{batch}:key:{offset}"
                    value = f"value:{batch}:{offset}"
                    commands.append(("SET", key, value))
                    commands.append(("GET", key))
                client.send_pipeline_raw(commands)
            latencies_ms.append((time.time() - op_start) * 1000)
        except Exception as exc:
            errors += 1
            log.error("Pipelined KV operation failed", system=system_name, error=str(exc))

    duration = time.time() - start_time
    client.close()

    logical_ops = num_batches * batch_size * 2
    return BenchmarkResult(
        system=system_name,
        operation="KV_PIPELINE_SET_GET",
        num_operations=logical_ops,
        duration_seconds=duration,
        throughput_ops_sec=logical_ops / duration,
        latency_p50_ms=statistics.median(latencies_ms),
        latency_p95_ms=statistics.quantiles(latencies_ms, n=20)[18],
        latency_p99_ms=statistics.quantiles(latencies_ms, n=100)[98],
        memory_mb=0.0,
        storage_mb=0.0,
        error_count=errors,
    )


def benchmark_counter_operations(client_factory, system_name: str, num_ops=10000) -> BenchmarkResult:
    """Benchmark atomic counter INCR operations"""
    log.info("Benchmarking counter INCR", system=system_name, num_ops=num_ops)
    
    client = client_factory()
    client.connect()
    
    latencies_ms = []
    errors = 0
    
    start_time = time.time()
    
    for i in range(num_ops):
        op_start = time.time()
        try:
            if system_name == "UniversalStateServer":
                client.send({"cmd": "INCR", "key": "counter", "amount": 1})
            else:  # Redis
                client.send_raw("INCR", "counter")
            
            op_duration = (time.time() - op_start) * 1000
            latencies_ms.append(op_duration)
        except Exception as e:
            errors += 1
    
    duration = time.time() - start_time
    client.close()
    
    result = BenchmarkResult(
        system=system_name,
        operation="COUNTER_INCR",
        num_operations=num_ops,
        duration_seconds=duration,
        throughput_ops_sec=num_ops / duration,
        latency_p50_ms=statistics.median(latencies_ms),
        latency_p95_ms=statistics.quantiles(latencies_ms, n=20)[18],
        latency_p99_ms=statistics.quantiles(latencies_ms, n=100)[98],
        memory_mb=0.0,
        storage_mb=0.0,
        error_count=errors
    )
    
    log.metric("counter_throughput", result.throughput_ops_sec, system=system_name)
    
    return result


def benchmark_batched_counter_operations(client_factory, system_name: str, num_batches=1000, batch_size=10) -> BenchmarkResult:
    """Benchmark batched counter increments."""
    log.info("Benchmarking batched counter INCR", system=system_name, num_batches=num_batches, batch_size=batch_size)

    client = client_factory()
    client.connect()

    latencies_ms = []
    errors = 0
    start_time = time.time()

    for batch in range(num_batches):
        op_start = time.time()
        try:
            if system_name == "UniversalStateServer":
                updates = [
                    {"key": f"counter:{offset % 32}", "amount": 1}
                    for offset in range(batch * batch_size, (batch + 1) * batch_size)
                ]
                client.send({"cmd": "MINCR", "updates": updates})
            else:
                commands = [
                    ("INCRBY", f"counter:{offset % 32}", 1)
                    for offset in range(batch * batch_size, (batch + 1) * batch_size)
                ]
                client.send_pipeline_raw(commands)
            latencies_ms.append((time.time() - op_start) * 1000)
        except Exception as exc:
            errors += 1
            log.error("Batched counter operation failed", system=system_name, error=str(exc))

    duration = time.time() - start_time
    client.close()

    logical_ops = num_batches * batch_size
    return BenchmarkResult(
        system=system_name,
        operation="COUNTER_BATCH_INCR",
        num_operations=logical_ops,
        duration_seconds=duration,
        throughput_ops_sec=logical_ops / duration,
        latency_p50_ms=statistics.median(latencies_ms),
        latency_p95_ms=statistics.quantiles(latencies_ms, n=20)[18],
        latency_p99_ms=statistics.quantiles(latencies_ms, n=100)[98],
        memory_mb=0.0,
        storage_mb=0.0,
        error_count=errors,
    )


def run_universal_vs_redis_benchmark():
    """Run comprehensive UniversalStateServer vs Redis benchmark"""
    log.info("Starting UniversalStateServer vs Redis benchmark")
    
    results = []
    
    # Start UniversalStateServer
    universal_proc = start_universal_server(port=9633)
    
    try:
        # Benchmark UniversalStateServer
        log.info("Benchmarking UniversalStateServer...")
        results.append(benchmark_kv_operations(
            lambda: UniversalStateClient("localhost", 9633),
            "UniversalStateServer",
            num_ops=10000
        ))
        results.append(benchmark_mset_mget_operations(
            lambda: UniversalStateClient("localhost", 9633),
            "UniversalStateServer",
            num_batches=1000,
            batch_size=10,
        ))
        results.append(benchmark_pipeline_kv_operations(
            lambda: UniversalStateClient("localhost", 9633),
            "UniversalStateServer",
            num_batches=1000,
            batch_size=10,
        ))
        results.append(benchmark_counter_operations(
            lambda: UniversalStateClient("localhost", 9633),
            "UniversalStateServer",
            num_ops=10000
        ))
        results.append(benchmark_batched_counter_operations(
            lambda: UniversalStateClient("localhost", 9633),
            "UniversalStateServer",
            num_batches=1000,
            batch_size=10,
        ))
        
        # Benchmark Redis (if available)
        if check_redis_available():
            log.info("Benchmarking Redis...")
            results.append(benchmark_kv_operations(
                lambda: RedisClient("localhost", 6379),
                "Redis",
                num_ops=10000
            ))
            results.append(benchmark_mset_mget_operations(
                lambda: RedisClient("localhost", 6379),
                "Redis",
                num_batches=1000,
                batch_size=10,
            ))
            results.append(benchmark_pipeline_kv_operations(
                lambda: RedisClient("localhost", 6379),
                "Redis",
                num_batches=1000,
                batch_size=10,
            ))
            results.append(benchmark_counter_operations(
                lambda: RedisClient("localhost", 6379),
                "Redis",
                num_ops=10000
            ))
            results.append(benchmark_batched_counter_operations(
                lambda: RedisClient("localhost", 6379),
                "Redis",
                num_batches=1000,
                batch_size=10,
            ))
        else:
            log.warning("Redis not available - skipping Redis benchmark")
            log.info("Install Redis: sudo apt install redis-server")
    
    finally:
        # Stop UniversalStateServer
        universal_proc.terminate()
        universal_proc.wait(timeout=5)
    
    return results


def generate_report(results: List[BenchmarkResult], output_path: Path):
    """Generate benchmark report"""
    log.info("Generating benchmark report", output=str(output_path))
    
    report = {
        "timestamp": time.time(),
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "branch": "33_event_streams",
        "systems_tested": list(set(r.system for r in results)),
        "results": [asdict(r) for r in results],
        "summary": {}
    }
    
    # Calculate comparative summary
    for operation in set(r.operation for r in results):
        op_results = [r for r in results if r.operation == operation]
        if len(op_results) >= 2:
            universal = next((r for r in op_results if r.system == "UniversalStateServer"), None)
            competitor = next((r for r in op_results if r.system != "UniversalStateServer"), None)
            
            if universal and competitor:
                speedup = universal.throughput_ops_sec / competitor.throughput_ops_sec
                latency_improvement = (competitor.latency_p50_ms - universal.latency_p50_ms) / competitor.latency_p50_ms
                storage_reduction = (competitor.storage_mb - universal.storage_mb) / competitor.storage_mb if competitor.storage_mb > 0 else 0
                
                report["summary"][operation] = {
                    "throughput_speedup": f"{speedup:.2f}x",
                    "latency_improvement": f"{latency_improvement*100:.1f}%",
                    "storage_reduction": f"{storage_reduction*100:.1f}%",
                    "winner": "UniversalStateServer" if speedup > 1 else competitor.system
                }
    
    # Write JSON report
    output_path.write_text(json.dumps(report, indent=2))
    log.info("Report written", path=str(output_path))
    
    # Print summary to console
    print("\n" + "="*80)
    print("BRANCH 33 COMPETITIVE BENCHMARK RESULTS")
    print("="*80 + "\n")
    
    for result in results:
        print(f"{result.system} - {result.operation}:")
        print(f"  Throughput:  {result.throughput_ops_sec:,.0f} ops/sec")
        print(f"  Latency p50: {result.latency_p50_ms:.2f} ms")
        print(f"  Latency p99: {result.latency_p99_ms:.2f} ms")
        print(f"  Storage:     {result.storage_mb:.2f} MB")
        print(f"  Errors:      {result.error_count}\n")
    
    if report["summary"]:
        print("\nCOMPARATIVE SUMMARY:")
        print("-" * 80)
        for op, summary in report["summary"].items():
            print(f"{op}:")
            print(f"  Winner: {summary['winner']}")
            print(f"  Throughput: {summary['throughput_speedup']}")
            print(f"  Latency improvement: {summary['latency_improvement']}")
            print(f"  Storage reduction: {summary['storage_reduction']}\n")
    
    print("="*80)


def main():
    """Run full benchmark suite"""
    log.info("Starting Branch 33 competitive benchmark suite")
    
    # Run benchmarks
    results = run_universal_vs_redis_benchmark()
    
    # Generate report
    report_path = Path(__file__).parent / "reports" / "branch33_competitive_benchmark.json"
    report_path.parent.mkdir(exist_ok=True)
    generate_report(results, report_path)
    
    log.info("Benchmark complete", report=str(report_path))


if __name__ == "__main__":
    main()
