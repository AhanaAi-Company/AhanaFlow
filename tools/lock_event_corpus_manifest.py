#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.acp_logging import get_logger

log = get_logger("branch33_lock_event_manifest")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def lock_manifest(manifest_path: Path, output_path: Path) -> dict[str, Any]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    locked_sources: list[dict[str, Any]] = []

    for source in payload.get("candidate_sources", []):
        source_path = REPO_ROOT / source["path"]
        record = dict(source)
        record["exists"] = source_path.exists()
        record["resolved_path"] = str(source_path)

        if source_path.is_file():
            record["source_type"] = "file"
            record["bytes"] = source_path.stat().st_size
            record["sha256"] = sha256_file(source_path)
            if source_path.suffix == ".jsonl":
                record["line_count"] = count_lines(source_path)
        else:
            record["source_type"] = "missing"

        locked_sources.append(record)

    payload["status"] = "locked"
    payload["locked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload["locked_sources"] = locked_sources
    payload["source_summary"] = {
        "sources_total": len(locked_sources),
        "sources_present": sum(1 for item in locked_sources if item["exists"]),
        "jsonl_sources": sum(1 for item in locked_sources if item["source_type"] == "file"),
        "total_locked_bytes": sum(int(item.get("bytes", 0)) for item in locked_sources),
        "total_locked_lines": sum(int(item.get("line_count", 0)) for item in locked_sources),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    log.info("locked event corpus manifest", input=str(manifest_path), output=str(output_path))
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lock the Branch 33 event corpus manifest with current file metadata.")
    parser.add_argument(
        "--input",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.json",
        help="Path to the planned event corpus manifest.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.locked.json",
        help="Path for the locked manifest output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    locked = lock_manifest(args.input, args.output)
    print(json.dumps(locked["source_summary"], indent=2))


if __name__ == "__main__":
    main()