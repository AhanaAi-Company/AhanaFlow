#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.acp_logging import get_logger

log = get_logger("branch33_expanded_access_corpus")

DEFAULT_ENDPOINT_PREFIXES = (
    "/v1/compress",
    "/v1/decompress",
    "/health",
    "/v1/health",
)


def _iter_jsonl(path: Path) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            rows.append((stripped, json.loads(stripped)))
    return rows


def build_corpus(
    access_log_path: Path,
    usage_log_path: Path,
    output_path: Path,
    endpoint_prefixes: tuple[str, ...] = DEFAULT_ENDPOINT_PREFIXES,
) -> dict[str, Any]:
    access_rows = _iter_jsonl(access_log_path)
    usage_rows = _iter_jsonl(usage_log_path)

    selected_usage_lines: list[str] = []
    selected_usage_endpoints: dict[str, int] = {}

    for raw_line, payload in usage_rows:
        endpoint = str(payload.get("endpoint") or "")
        if not endpoint.startswith(endpoint_prefixes):
            continue
        selected_usage_lines.append(raw_line)
        selected_usage_endpoints[endpoint] = selected_usage_endpoints.get(endpoint, 0) + 1

    output_lines = [raw_line for raw_line, _payload in access_rows]
    output_lines.extend(selected_usage_lines)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")

    summary = {
        "status": "measured",
        "access_source": str(access_log_path),
        "usage_source": str(usage_log_path),
        "output_path": str(output_path),
        "endpoint_prefixes": list(endpoint_prefixes),
        "access_rows": len(access_rows),
        "usage_rows_scanned": len(usage_rows),
        "usage_rows_selected": len(selected_usage_lines),
        "output_rows": len(output_lines),
        "selected_usage_endpoints": selected_usage_endpoints,
    }
    log.info("built expanded access corpus", **summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a focused expanded access-event corpus for Branch 33.")
    parser.add_argument(
        "--access-log",
        type=Path,
        default=REPO_ROOT / "data/access.jsonl",
        help="Path to the primary access-event JSONL file.",
    )
    parser.add_argument(
        "--usage-log",
        type=Path,
        default=REPO_ROOT / "data/usage_events.jsonl",
        help="Path to the usage-event JSONL file used for access-like request expansion.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/access_events_expanded.jsonl",
        help="Path for the derived expanded access corpus.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_corpus(args.access_log, args.usage_log, args.output)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()