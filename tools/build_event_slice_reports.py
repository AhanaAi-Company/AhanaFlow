#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.acp_logging import get_logger

log = get_logger("branch33_event_slice_reports")


def _load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import module at {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "slice"


def build_slice_reports(manifest_path: Path, output_path: Path, slices_dir: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sources = manifest.get("locked_sources") or manifest.get("candidate_sources", [])
    baseline_module = _load_module(
        "branch33_event_baseline_for_slices",
        REPO_ROOT / "business_ecosystem/33_event_streams/tools/build_event_baseline_report.py",
    )
    acp_module = _load_module(
        "branch33_event_acp_for_slices",
        REPO_ROOT / "business_ecosystem/33_event_streams/tools/build_event_acp_report.py",
    )

    slices: list[dict[str, Any]] = []
    slices_dir.mkdir(parents=True, exist_ok=True)

    for source in sources:
        selected_path = source.get("resolved_path") or source.get("path")
        source_class = source.get("class") or Path(str(selected_path)).stem
        label = _slugify(source_class)
        slice_dir = slices_dir / label
        slice_dir.mkdir(parents=True, exist_ok=True)

        baseline = baseline_module.build_report(
            manifest_path,
            slice_dir / "baseline_stream_report.json",
            selected_paths=[str(selected_path)],
            selection_label=label,
        )
        lossless_report, lossless_verify = acp_module.build_reports(
            manifest_path,
            slice_dir / "acp_event_report.json",
            slice_dir / "replay_integrity_report.json",
            mode="lossless",
            selected_paths=[str(selected_path)],
            selection_label=label,
        )
        adaptive_report, adaptive_verify = acp_module.build_reports(
            manifest_path,
            slice_dir / "acp_event_report_adaptive.json",
            slice_dir / "replay_integrity_report_adaptive.json",
            mode="adaptive",
            selected_paths=[str(selected_path)],
            selection_label=label,
        )

        gzip_bytes = baseline["baseline_variants"][1]["metrics"]["total_output_bytes"]
        lossless_bytes = lossless_report["metrics"]["total_output_bytes"]
        adaptive_bytes = adaptive_report["metrics"]["total_output_bytes"]
        best_mode = "lossless" if lossless_bytes <= adaptive_bytes else "adaptive"
        best_bytes = min(lossless_bytes, adaptive_bytes)

        slices.append(
            {
                "label": label,
                "selected_path": str(selected_path),
                "source_class": source_class,
                "baseline_report": str(slice_dir / "baseline_stream_report.json"),
                "lossless_report": str(slice_dir / "acp_event_report.json"),
                "adaptive_report": str(slice_dir / "acp_event_report_adaptive.json"),
                "best_acp_mode": best_mode,
                "gzip_output_bytes": gzip_bytes,
                "best_acp_output_bytes": best_bytes,
                "gzip_advantage_bytes": best_bytes - gzip_bytes,
                "lossless_replay_failures": lossless_verify["summary"]["replay_failures"],
                "adaptive_replay_failures": adaptive_verify["summary"]["replay_failures"],
            }
        )

    summary = {
        "artifact_version": 1,
        "branch": 33,
        "status": "measured",
        "report_type": "event_slice_comparison_report",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "corpus_manifest": str(manifest_path),
        "slices_dir": str(slices_dir),
        "slices": slices,
        "notes": [
            "Each slice reuses the same proof loop: gzip baseline, ACP lossless, ACP adaptive, and replay verification.",
            "Best ACP mode is selected only by compressed bytes on the slice; replay must still remain exact.",
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    log.info("built event slice reports", manifest=str(manifest_path), output=str(output_path), slices=len(slices))
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-slice Branch 33 event benchmark artifacts.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_corpus_manifest.locked.json",
        help="Path to the locked event manifest.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/event_slice_comparison_report.json",
        help="Path for the generated slice summary report.",
    )
    parser.add_argument(
        "--slices-dir",
        type=Path,
        default=REPO_ROOT / "business_ecosystem/33_event_streams/reports/slices",
        help="Directory for the generated per-slice artifacts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_slice_reports(args.manifest, args.output, args.slices_dir)
    print(json.dumps({"slices": len(report["slices"])}, indent=2))


if __name__ == "__main__":
    main()