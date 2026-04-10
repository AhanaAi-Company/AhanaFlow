#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.acp_logging import get_logger

log = get_logger("branch33_event_baseline_report")


def _load_manifest_sources(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return payload.get("locked_sources") or payload.get("candidate_sources", [])


def _resolve_path(record: dict[str, Any]) -> Path:
    resolved_path = record.get("resolved_path")
    if resolved_path:
        return Path(resolved_path)
    return REPO_ROOT / record["path"]


def _source_selected(
    source: dict[str, Any],
    selected_paths: set[str] | None,
    selected_classes: set[str] | None,
) -> bool:
    if selected_paths:
        path_candidates = {str(source.get("path", "")), str(source.get("resolved_path", ""))}
        if not path_candidates.intersection(selected_paths):
            return False
    if selected_classes and source.get("class") not in selected_classes:
        return False
    return True


def _read_sources_bytes(
    payload: dict[str, Any],
    selected_paths: set[str] | None = None,
    selected_classes: set[str] | None = None,
) -> tuple[list[dict[str, Any]], bytes, int]:
    measured_sources: list[dict[str, Any]] = []
    combined = bytearray()
    total_lines = 0

    for source in _load_manifest_sources(payload):
        if not _source_selected(source, selected_paths, selected_classes):
            continue
        source_path = _resolve_path(source)
        if not source_path.exists() or not source_path.is_file():
            continue

        content = source_path.read_bytes()
        line_count = source.get("line_count")
        if line_count is None:
            line_count = content.count(b"\n")
        total_lines += int(line_count)
        combined.extend(content)

        measured_sources.append(
            {
                "path": str(source_path),
                "bytes": len(content),
                "line_count": int(line_count),
            }
        )

    return measured_sources, bytes(combined), total_lines


def build_report(
    manifest_path: Path,
    output_path: Path,
    selected_paths: list[str] | None = None,
    selected_classes: list[str] | None = None,
    selection_label: str | None = None,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    measured_sources, combined_bytes, total_lines = _read_sources_bytes(
        manifest,
        selected_paths=set(selected_paths) if selected_paths else None,
        selected_classes=set(selected_classes) if selected_classes else None,
    )
    total_input_bytes = len(combined_bytes)

    raw_variant = {
        "name": "raw_payload_transport",
        "status": "measured",
        "metrics": {
            "total_input_bytes": total_input_bytes,
            "total_output_bytes": total_input_bytes,
            "producer_ms": 0.0,
            "consumer_ms": 0.0,
            "total_lines": total_lines,
        },
    }

    producer_start = time.perf_counter()
    gzip_bundle = gzip.compress(combined_bytes, compresslevel=9)
    producer_ms = round((time.perf_counter() - producer_start) * 1000, 3)

    consumer_start = time.perf_counter()
    restored = gzip.decompress(gzip_bundle)
    consumer_ms = round((time.perf_counter() - consumer_start) * 1000, 3)

    bundle_variant = {
        "name": "gzip_or_zstd_payload_bundle",
        "status": "measured",
        "selected_backend": "gzip-9",
        "lossless_verified": restored == combined_bytes,
        "metrics": {
            "total_input_bytes": total_input_bytes,
            "total_output_bytes": len(gzip_bundle),
            "producer_ms": producer_ms,
            "consumer_ms": consumer_ms,
            "total_lines": total_lines,
        },
    }

    report = {
        "artifact_version": 2,
        "branch": 33,
        "status": "measured",
        "report_type": "baseline_stream_report",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "corpus_manifest": str(manifest_path),
        "manifest_status": manifest.get("status", "unknown"),
        "source_summary": {
            "present_sources": len(measured_sources),
            "total_input_bytes": total_input_bytes,
            "total_lines": total_lines,
        },
        "selection": {
            "label": selection_label,
            "selected_paths": selected_paths or [],
            "selected_classes": selected_classes or [],
        },
        "baseline_variants": [raw_variant, bundle_variant],
        "notes": [
            "The first event baseline builder uses gzip-9 as the deterministic bundle baseline.",
            "A later session can add broker-specific or zstd bundle comparisons if needed.",
        ],
        "measured_sources": measured_sources,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    log.info("built event baseline report", manifest=str(manifest_path), output=str(output_path))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Branch 33 baseline stream report from a locked event manifest.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.locked.json",
        help="Path to the locked event manifest.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/baseline_stream_report.json",
        help="Path for the generated baseline report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(args.manifest, args.output)
    print(json.dumps(report["source_summary"], indent=2))


if __name__ == "__main__":
    main()