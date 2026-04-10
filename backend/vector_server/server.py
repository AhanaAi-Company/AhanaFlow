from __future__ import annotations

import socketserver
from pathlib import Path
from typing import Any

from backend.universal_server.protocol import ProtocolError, decode_command, encode_response
from backend.universal_server.security import SecurityConfig, SecurityError, SecurityMiddleware

from .engine import VectorStateEngineV2


class _VectorHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        # Extract client IP
        client_ip = self.client_address[0]
        security: SecurityMiddleware | None = getattr(self.server, "_security", None)  # type: ignore[attr-defined]

        # Register connection
        if security:
            try:
                security.check_connection_limit(client_ip)
                security.register_connection(client_ip)
            except SecurityError as exc:
                response = {"ok": False, "error": f"security: {exc}"}
                self.wfile.write(encode_response(response))
                return

        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue

                try:
                    # Security: Check payload size
                    if security:
                        security.validate_payload_size(line)

                    command = decode_command(line)

                    # Security: Authenticate and validate
                    api_key = command.get("api_key")
                    if security:
                        if command["cmd"] == "AUTH":
                            security.authenticate(client_ip, api_key)
                            response = {"ok": True, "result": "OK"}
                        else:
                            security.authenticate(client_ip, api_key)
                            security.validate_command(command["cmd"])

                            # Rate limiting
                            security.check_rate_limit(client_ip, security.config.rate_limit_per_ip)
                            if api_key:
                                import hashlib
                                key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
                                security.check_rate_limit(key_hash, security.config.rate_limit_per_key)

                            response = self.server.dispatch(command, security)  # type: ignore[attr-defined]
                    else:
                        response = self.server.dispatch(command, None)  # type: ignore[attr-defined]

                except ProtocolError as exc:
                    response = {"ok": False, "error": str(exc)}
                except SecurityError as exc:
                    response = {"ok": False, "error": f"security: {exc}"}
                except Exception as exc:
                    response = {"ok": False, "error": f"internal error: {exc}"}

                self.wfile.write(encode_response(response))
        finally:
            # Unregister connection
            if security:
                security.unregister_connection(client_ip)


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class VectorStateServerV2:
    """Separate vector-capable server surface for AhanaFlow v2."""

    def __init__(
        self,
        wal_path: str | Path,
        host: str = "127.0.0.1",
        port: int = 9644,
        *,
        security_config: SecurityConfig | None = None,
    ) -> None:
        self._engine = VectorStateEngineV2(wal_path)
        self._security = SecurityMiddleware(security_config) if security_config else None
        self._srv = _ThreadedTCPServer((host, port), _VectorHandler)
        self._srv.dispatch = self.dispatch  # type: ignore[attr-defined]
        self._srv._security = self._security  # type: ignore[attr-defined]
        self._serving = False

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._srv.server_address
        return str(host), int(port)

    def serve_forever(self) -> None:
        self._serving = True
        try:
            self._srv.serve_forever()
        finally:
            self._serving = False

    def shutdown(self) -> None:
        if self._serving:
            self._srv.shutdown()
        self._srv.server_close()
        self._engine.close()
        if self._security:
            self._security.close()

    def dispatch(self, command: dict[str, Any], security: SecurityMiddleware | None = None) -> dict[str, Any]:
        cmd = command["cmd"]

        if cmd == "PING":
            return {"ok": True, "result": "PONG"}

        if cmd == "AUTH":
            return {"ok": True, "result": "OK"}

        if cmd == "VECTOR_CREATE":
            collection = _require_str(command, "collection")
            dimensions = int(command.get("dimensions", 0))
            metric = str(command.get("metric", "cosine"))
            modality = str(command.get("modality", "vector"))
            self._engine.create_collection(collection, dimensions, metric=metric, modality=modality)
            return {"ok": True, "result": "OK"}

        if cmd == "VECTOR_LIST":
            return {"ok": True, "result": self._engine.list_collections()}

        if cmd == "VECTOR_UPSERT":
            collection = _require_str(command, "collection")
            item_id = _require_str(command, "id")
            vector = _require_float_list(command, "vector")
            metadata = command.get("metadata")
            if metadata is not None and not isinstance(metadata, dict):
                raise ProtocolError("metadata must be an object")
            ttl = command.get("ttl_seconds")
            self._engine.upsert(
                collection,
                item_id,
                vector,
                metadata=metadata,
                payload=command.get("payload"),
                ttl_seconds=None if ttl is None else int(ttl),
            )
            return {"ok": True, "result": "OK"}

        if cmd == "VECTOR_GET":
            collection = _require_str(command, "collection")
            item_id = _require_str(command, "id")
            include_vector = bool(command.get("include_vector", False))
            return {"ok": True, "result": self._engine.get(collection, item_id, include_vector=include_vector)}

        if cmd == "VECTOR_DELETE":
            collection = _require_str(command, "collection")
            item_id = _require_str(command, "id")
            return {"ok": True, "result": 1 if self._engine.delete(collection, item_id) else 0}

        if cmd == "VECTOR_COUNT":
            collection = _require_str(command, "collection")
            stats = self._engine.stats()
            count = next((s.vectors for s in stats.collection_stats if s.name == collection), 0)
            return {"ok": True, "result": count}

        if cmd == "VECTOR_BUILD_ANN":
            collection = _require_str(command, "collection")
            n_lists = _optional_int(command, "n_lists", minimum=1)
            return {"ok": True, "result": self._engine.build_ann_index(collection, n_lists=n_lists)}

        if cmd == "VECTOR_BUILD_HNSW":
            collection = _require_str(command, "collection")
            return {
                "ok": True,
                "result": self._engine.build_hnsw_index(
                    collection,
                    M=_optional_int(command, "M", minimum=2),
                    M_max0=_optional_int(command, "M_max0", minimum=2),
                    ef_construction=_optional_int(command, "ef_construction", minimum=1),
                    ef_search=_optional_int(command, "ef_search", minimum=1),
                    enable_pq=bool(command.get("enable_pq", False)),
                    pq_segments=_optional_int(command, "pq_segments", minimum=1),
                    pq_centroids=_optional_int(command, "pq_centroids", minimum=2),
                ),
            }

        if cmd == "VECTOR_QUERY":
            collection = _require_str(command, "collection")
            vector = _require_float_list(command, "vector")
            top_k = _int_or_default(command, "top_k", default=5, minimum=1)
            filters = command.get("filters")
            if filters is not None and not isinstance(filters, dict):
                raise ProtocolError("filters must be an object")
            include_vectors = bool(command.get("include_vectors", False))
            strategy = str(command.get("strategy", "exact"))
            candidate_multiplier = _int_or_default(command, "candidate_multiplier", default=8, minimum=1)
            ann_probe_count = _optional_int(command, "ann_probe_count", minimum=1)
            compress_results = bool(command.get("compress_results", False))
            include_diagnostics = bool(command.get("include_diagnostics", False))
            use_gpu = bool(command.get("use_gpu", False))
            query_text = command.get("query_text")
            if query_text is not None:
                query_text = str(query_text)
            ncd_weight = float(command.get("ncd_weight", 0.3))
            bpe_weight = float(command.get("bpe_weight", 0.3))
            return {
                "ok": True,
                "result": self._engine.query(
                    collection,
                    vector,
                    top_k=top_k,
                    filters=filters,
                    include_vectors=include_vectors,
                    strategy=strategy,
                    candidate_multiplier=candidate_multiplier,
                    ann_probe_count=ann_probe_count,
                    compress_results=compress_results,
                    include_diagnostics=include_diagnostics,
                    use_gpu=use_gpu,
                    query_text=query_text,
                    ncd_weight=ncd_weight,
                    bpe_weight=bpe_weight,
                ),
            }

        if cmd == "VECTOR_VERSION_HISTORY":
            collection = _require_str(command, "collection")
            item_id = _require_str(command, "id")
            limit = _int_or_default(command, "limit", default=100, minimum=1)
            return {"ok": True, "result": self._engine.get_version_history(collection, item_id, limit=limit)}

        if cmd == "VECTOR_QUERY_AS_OF":
            collection = _require_str(command, "collection")
            vector = _require_float_list(command, "vector")
            as_of = float(command.get("as_of", 0))
            if as_of <= 0:
                raise ProtocolError("as_of must be a positive Unix timestamp")
            top_k = _int_or_default(command, "top_k", default=5, minimum=1)
            filters = command.get("filters")
            if filters is not None and not isinstance(filters, dict):
                raise ProtocolError("filters must be an object")
            return {
                "ok": True,
                "result": self._engine.query_as_of(
                    collection, vector, as_of=as_of, top_k=top_k, filters=filters,
                ),
            }

        if cmd == "VECTOR_DRIFT":
            collection = _require_str(command, "collection")
            item_id = _require_str(command, "id")
            return {"ok": True, "result": self._engine.drift_detection(collection, item_id)}

        if cmd == "VECTOR_COMPACT":
            collection_raw = command.get("collection")
            if collection_raw is not None and (not isinstance(collection_raw, str) or not collection_raw):
                raise ProtocolError("collection must be a non-empty string when provided")
            collection_name = collection_raw if isinstance(collection_raw, str) else None
            return {"ok": True, "result": self._engine.compact(collection=collection_name)}

        if cmd == "VECTOR_STATS":
            stats = self._engine.stats()
            return {
                "ok": True,
                "result": {
                    "collections": stats.collections,
                    "vectors": stats.vectors,
                    "records_replayed": stats.records_replayed,
                    "wal_size_bytes": stats.wal_size_bytes,
                    "collection_stats": [
                        {
                            "name": item.name,
                            "dimensions": item.dimensions,
                            "metric": item.metric,
                            "vectors": item.vectors,
                        }
                        for item in stats.collection_stats
                    ],
                },
            }

        if cmd == "VECTOR_SCAN":
            collection = _require_str(command, "collection")
            limit = _int_or_default(command, "limit", default=1000, minimum=1)
            include_vectors = bool(command.get("include_vectors", False))
            return {
                "ok": True,
                "result": self._engine.scan(
                    collection,
                    limit=limit,
                    include_vectors=include_vectors,
                ),
            }

        raise ProtocolError(f"unknown command: {cmd}")


def _require_str(command: dict[str, Any], field: str) -> str:
    value = command.get(field)
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{field} must be a non-empty string")
    return value


def _require_float_list(command: dict[str, Any], field: str) -> list[float]:
    value = command.get(field)
    if not isinstance(value, list) or not value:
        raise ProtocolError(f"{field} must be a non-empty array")
    try:
        return [float(item) for item in value]
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"{field} must contain only numbers") from exc


def _int_or_default(command: dict[str, Any], field: str, *, default: int, minimum: int | None = None) -> int:
    raw = command.get(field, default)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"{field} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ProtocolError(f"{field} must be >= {minimum}")
    return value


def _optional_int(command: dict[str, Any], field: str, *, minimum: int | None = None) -> int | None:
    if field not in command or command.get(field) is None:
        return None
    return _int_or_default(command, field, default=0, minimum=minimum)