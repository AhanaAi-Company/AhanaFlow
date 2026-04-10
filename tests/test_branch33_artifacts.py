from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any


class _FakeCompressor:
    def compress_bytes(self, data: bytes, mode: str = "lossless", metadata: dict[str, Any] | None = None) -> Any:
        return SimpleNamespace(_payload=b"ACP" + data, metadata=metadata or {}, mode=mode)

    def decompress_bytes(self, payload: bytes) -> Any:
        return SimpleNamespace(_raw=payload[3:])


def test_materialize_event_artifacts_writes_status_summary(tmp_path: Path, load_spec_module: Any) -> None:
    module = load_spec_module(
        "branch33_materialize_event_artifacts_test",
        "business_ecosystem/33_event_streams/tools/materialize_event_artifacts.py",
    )

    manifest_path = tmp_path / "event_manifest.json"
    manifest_path.write_text(json.dumps({"candidate_sources": []}), encoding="utf-8")

    module.lock_manifest = lambda manifest, output: {
        "status": "locked",
        "source_summary": {"sources_present": 3, "total_locked_bytes": 512, "total_locked_lines": 12},
    }
    module.build_event_baseline_report = lambda manifest, output: {
        "status": "measured",
        "baseline_variants": [{}, {"selected_backend": "gzip-9"}],
    }
    module.build_event_acp_reports = lambda manifest, report_output, verification_output, mode="lossless": (
        {"status": "measured", "metrics": {"total_output_bytes": 100 if mode == "lossless" else 120}},
        {"summary": {"streams_checked": 3, "replay_failures": 0}},
    )
    module.build_slice_reports = lambda manifest, output, slices_dir: {
        "status": "measured",
        "slices": [{"label": "access_events"}, {"label": "usage_events"}],
    }

    status_output = tmp_path / "event_artifact_status.json"
    summary = module.materialize_reports(
        manifest_path,
        tmp_path / "event_manifest.locked.json",
        tmp_path / "baseline_stream_report.json",
        tmp_path / "acp_event_report.json",
        tmp_path / "replay_integrity_report.json",
        tmp_path / "acp_event_report_adaptive.json",
        tmp_path / "replay_integrity_report_adaptive.json",
        tmp_path / "event_slice_comparison_report.json",
        tmp_path / "slices",
        status_output,
    )

    persisted = json.loads(status_output.read_text(encoding="utf-8"))
    assert summary["status"] == "materialized_access_event_evidence"
    assert persisted["baseline"]["bundle_backend"] == "gzip-9"
    assert persisted["slice_summary"]["slices"] == 2