#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
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

log = get_logger("branch33_event_acp_report")


def _get_compressor() -> Any:
    from ahana_tool.core.ahana_compression_protocol import AhanaCompressor

    return AhanaCompressor(entropy_level=22)


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


def _read_manifest_streams(
    payload: dict[str, Any],
    selected_paths: set[str] | None = None,
    selected_classes: set[str] | None = None,
) -> list[dict[str, Any]]:
    streams: list[dict[str, Any]] = []
    sources = payload.get("locked_sources") or payload.get("candidate_sources", [])
    for source in sources:
        if not _source_selected(source, selected_paths, selected_classes):
            continue
        resolved = source.get("resolved_path")
        source_path = Path(resolved) if resolved else REPO_ROOT / source["path"]
        if not source_path.is_file():
            continue
        raw = source_path.read_bytes()
        line_count = source.get("line_count")
        if line_count is None:
            line_count = raw.count(b"\n")
        streams.append(
            {
                "path": str(source_path),
                "raw": raw,
                "line_count": int(line_count),
            }
        )
    return streams


def build_reports(
    manifest_path: Path,
    report_output_path: Path,
    verification_output_path: Path,
    mode: str = "lossless",
    selected_paths: list[str] | None = None,
    selected_classes: list[str] | None = None,
    selection_label: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    streams = _read_manifest_streams(
        manifest,
        selected_paths=set(selected_paths) if selected_paths else None,
        selected_classes=set(selected_classes) if selected_classes else None,
    )
    compressor = _get_compressor()

    lane_results: list[dict[str, Any]] = []
    checked_streams: list[dict[str, Any]] = []
    total_input_bytes = 0
    total_output_bytes = 0
    total_producer_ms = 0.0
    total_consumer_ms = 0.0

    for stream in streams:
        raw = stream["raw"]
        total_input_bytes += len(raw)
        original_sha256 = hashlib.sha256(raw).hexdigest()

        producer_start = time.perf_counter()
        compression_result = compressor.compress_bytes(
            raw,
            mode=mode,
            metadata={"filename": Path(stream["path"]).name},
        )
        producer_ms = round((time.perf_counter() - producer_start) * 1000, 3)
        payload = compression_result._payload
        total_output_bytes += len(payload)
        total_producer_ms += producer_ms

        consumer_start = time.perf_counter()
        decompression_result = compressor.decompress_bytes(payload)
        consumer_ms = round((time.perf_counter() - consumer_start) * 1000, 3)
        total_consumer_ms += consumer_ms

        restored = decompression_result._raw
        restored_sha256 = hashlib.sha256(restored).hexdigest()
        replay_verified = restored == raw

        lane_results.append(
            {
                "path": stream["path"],
                "selected_mode": mode,
                "input_bytes": len(raw),
                "output_bytes": len(payload),
                "line_count": stream["line_count"],
                "savings_pct": round((1.0 - len(payload) / max(len(raw), 1)) * 100.0, 3),
                "producer_ms": producer_ms,
                "consumer_ms": consumer_ms,
                "replay_verified": replay_verified,
            }
        )
        checked_streams.append(
            {
                "path": stream["path"],
                "original_sha256": original_sha256,
                "restored_sha256": restored_sha256,
                "record_count": stream["line_count"],
                "replay_verified": replay_verified,
                "ordering_verified": replay_verified,
            }
        )

    savings_pct = round((1.0 - total_output_bytes / max(total_input_bytes, 1)) * 100.0, 3)
    report = {
        "artifact_version": 2,
        "branch": 33,
        "status": "measured",
        "report_type": "acp_event_report",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "corpus_manifest": str(manifest_path),
        "comparison_target": "reports/baseline_stream_report.json",
        "selected_mode": mode,
        "selection": {
            "label": selection_label,
            "selected_paths": selected_paths or [],
            "selected_classes": selected_classes or [],
        },
        "metrics": {
            "total_input_bytes": total_input_bytes,
            "total_output_bytes": total_output_bytes,
            "savings_pct": savings_pct,
            "producer_ms": round(total_producer_ms, 3),
            "consumer_ms": round(total_consumer_ms, 3),
            "streams_processed": len(streams),
        },
        "lane_results": lane_results,
        "notes": [
            "This first ACP event report uses a single lossless-safe mode for all streams.",
            "Replay integrity is recorded separately but generated from the same pass.",
        ],
    }

    verification = {
        "artifact_version": 2,
        "branch": 33,
        "status": "measured",
        "report_type": "replay_integrity_report",
        "verification_rule": "Event payloads, ordering, and record counts must match exactly.",
        "selection": {
            "label": selection_label,
            "selected_paths": selected_paths or [],
            "selected_classes": selected_classes or [],
        },
        "checked_streams": checked_streams,
        "summary": {
            "streams_checked": len(checked_streams),
            "replay_passes": sum(1 for item in checked_streams if item["replay_verified"]),
            "replay_failures": sum(1 for item in checked_streams if not item["replay_verified"]),
        },
    }

    report_output_path.parent.mkdir(parents=True, exist_ok=True)
    verification_output_path.parent.mkdir(parents=True, exist_ok=True)
    report_output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    verification_output_path.write_text(json.dumps(verification, indent=2) + "\n", encoding="utf-8")
    log.info(
        "built event ACP reports",
        manifest=str(manifest_path),
        report_output=str(report_output_path),
        verification_output=str(verification_output_path),
        streams_processed=len(streams),
    )
    return report, verification


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Branch 33 ACP comparison and replay verification reports.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.locked.json",
        help="Path to the locked event manifest.",
    )
    parser.add_argument(
        "--report-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/acp_event_report.json",
        help="Path for the generated ACP event report.",
    )
    parser.add_argument(
        "--verification-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/replay_integrity_report.json",
        help="Path for the generated replay integrity report.",
    )
    parser.add_argument("--mode", default="lossless", help="Compression mode for the first ACP pass.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report, _ = build_reports(args.manifest, args.report_output, args.verification_output, mode=args.mode)
    print(json.dumps(report["metrics"], indent=2))


if __name__ == "__main__":
    main()