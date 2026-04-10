#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.acp_logging import get_logger
from build_event_acp_report import build_reports as build_event_acp_reports
from build_event_baseline_report import build_report as build_event_baseline_report
from build_event_slice_reports import build_slice_reports
from lock_event_corpus_manifest import lock_manifest

log = get_logger("branch33_materialize_event_artifacts")


def _artifact_status(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists()}


def materialize_reports(
    manifest_path: Path,
    locked_output_path: Path,
    baseline_output_path: Path,
    acp_output_path: Path,
    verification_output_path: Path,
    adaptive_acp_output_path: Path,
    adaptive_verification_output_path: Path,
    slice_summary_output_path: Path,
    slices_dir: Path,
    status_output_path: Path,
) -> dict[str, Any]:
    locked_manifest = lock_manifest(manifest_path, locked_output_path)
    baseline_report = build_event_baseline_report(locked_output_path, baseline_output_path)
    acp_report, verification_report = build_event_acp_reports(
        locked_output_path,
        acp_output_path,
        verification_output_path,
        mode="lossless",
    )
    adaptive_report, adaptive_verification = build_event_acp_reports(
        locked_output_path,
        adaptive_acp_output_path,
        adaptive_verification_output_path,
        mode="adaptive",
    )
    slice_report = build_slice_reports(locked_output_path, slice_summary_output_path, slices_dir)

    status: dict[str, Any] = {
        "artifact_version": 1,
        "branch": 33,
        "status": "materialized_access_event_evidence",
        "report_type": "event_artifact_status",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "manifest": {
            "input": str(manifest_path),
            "locked": _artifact_status(locked_output_path),
            "sources_present": locked_manifest.get("source_summary", {}).get("sources_present", 0),
            "total_locked_bytes": locked_manifest.get("source_summary", {}).get("total_locked_bytes", 0),
            "total_locked_lines": locked_manifest.get("source_summary", {}).get("total_locked_lines", 0),
        },
        "baseline": {
            "artifact": _artifact_status(baseline_output_path),
            "status": baseline_report.get("status"),
            "bundle_backend": baseline_report.get("baseline_variants", [{}, {}])[1].get("selected_backend"),
        },
        "acp_lossless": {
            "artifact": _artifact_status(acp_output_path),
            "status": acp_report.get("status"),
            "metrics": acp_report.get("metrics"),
            "verification": verification_report.get("summary"),
        },
        "acp_adaptive": {
            "artifact": _artifact_status(adaptive_acp_output_path),
            "status": adaptive_report.get("status"),
            "metrics": adaptive_report.get("metrics"),
            "verification": adaptive_verification.get("summary"),
        },
        "slice_summary": {
            "artifact": _artifact_status(slice_summary_output_path),
            "status": slice_report.get("status"),
            "slices": len(slice_report.get("slices", [])),
        },
        "claim_boundary": "Branch 33 remains an archive-first retained access-event lane. Broader mixed-event claims stay blocked until they beat gzip on their own measured corpus.",
        "recommended_next_step": "Validate one more real retained access-log workflow before broadening the lane or reviving SDK-first positioning.",
    }

    status_output_path.parent.mkdir(parents=True, exist_ok=True)
    status_output_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    log.info("materialized branch33 event artifacts", status=str(status_output_path))
    return status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize the full Branch 33 event artifact packet.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.json",
        help="Path to the planned event corpus manifest.",
    )
    parser.add_argument(
        "--locked-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.locked.json",
        help="Path for the locked manifest output.",
    )
    parser.add_argument(
        "--baseline-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/baseline_stream_report.json",
        help="Path for the baseline stream report.",
    )
    parser.add_argument(
        "--acp-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/acp_event_report.json",
        help="Path for the lossless ACP report.",
    )
    parser.add_argument(
        "--verification-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/replay_integrity_report.json",
        help="Path for the lossless replay verification report.",
    )
    parser.add_argument(
        "--adaptive-acp-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/acp_event_report_adaptive.json",
        help="Path for the adaptive ACP report.",
    )
    parser.add_argument(
        "--adaptive-verification-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/replay_integrity_report_adaptive.json",
        help="Path for the adaptive replay verification report.",
    )
    parser.add_argument(
        "--slice-summary-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_slice_comparison_report.json",
        help="Path for the slice summary report.",
    )
    parser.add_argument(
        "--slices-dir",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/slices",
        help="Directory for per-slice artifacts.",
    )
    parser.add_argument(
        "--status-output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_artifact_status.json",
        help="Path for the event artifact status summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = materialize_reports(
        args.manifest,
        args.locked_output,
        args.baseline_output,
        args.acp_output,
        args.verification_output,
        args.adaptive_acp_output,
        args.adaptive_verification_output,
        args.slice_summary_output,
        args.slices_dir,
        args.status_output,
    )
    print(json.dumps({"status": summary["status"]}, indent=2))


if __name__ == "__main__":
    main()