from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class MetricTile:
    label: str
    value: int
    tone: str = "neutral"


@dataclass(frozen=True, slots=True)
class StatusSlice:
    label: str
    value: int
    color: str


@dataclass(frozen=True, slots=True)
class FolderResult:
    folder: str
    matched: int
    copied: int
    failed: int
    skipped: int
    total_size: int


@dataclass(frozen=True, slots=True)
class ResultsViewModel:
    run_id: str = ""
    mode: str = ""
    created_at: str = ""
    metrics: list[MetricTile] = field(default_factory=list)
    status_slices: list[StatusSlice] = field(default_factory=list)
    top_folders: list[FolderResult] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    verification: dict[str, Any] = field(default_factory=dict)
    output_files: list[Path] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.conflicts or self.failures or self.blocked)

    @property
    def success_message(self) -> str:
        failed = _metric_value(self.metrics, "Failed")
        copied = _metric_value(self.metrics, "Copied")
        matched = _metric_value(self.metrics, "Matched")
        if failed:
            return f"{failed} item(s) need attention. Originals were not deleted or moved."
        if copied:
            return f"{copied} file(s) were copied into the archive. Originals stayed in place."
        if matched:
            return "The archive plan is ready. No Dropbox changes were made in preview mode."
        return "No files matched the selected cutoff. Your Dropbox was scanned successfully."


def load_results_view_model(run_dir: Path) -> ResultsViewModel:
    summary_path = run_dir / "summary.json"
    verification_path = run_dir / "verification_report.json"
    summary = _read_json(summary_path)
    verification_payload = _read_json(verification_path)
    verification = summary.get("verification") or verification_payload.get("summary") or {}
    totals = summary.get("totals") or {}
    output_files = sorted([path for path in run_dir.glob("*") if path.is_file()], key=lambda path: path.name.casefold())

    metrics = [
        MetricTile("Scanned", int(totals.get("items_scanned", 0))),
        MetricTile("Matched", int(totals.get("files_matched", 0)), "accent"),
        MetricTile("Copied", int(totals.get("files_copied", 0)), "success"),
        MetricTile("Skipped", int(totals.get("files_skipped", 0)), "warning"),
        MetricTile("Failed", int(totals.get("files_failed", 0)), "danger"),
    ]
    if int(totals.get("namespaces_scanned", 0)):
        metrics.insert(1, MetricTile("Namespaces", int(totals.get("namespaces_scanned", 0))))
    if int(totals.get("members_covered", 0)):
        metrics.insert(2, MetricTile("Members", int(totals.get("members_covered", 0))))

    status_slices = [
        StatusSlice("Copied", int(totals.get("files_copied", 0)), "#2E7D5B"),
        StatusSlice("Skipped", int(totals.get("files_skipped", 0)), "#C07A2C"),
        StatusSlice("Failed", int(totals.get("files_failed", 0)), "#C84C4C"),
    ]

    top_folders = [
        FolderResult(
            folder=str(row.get("display_folder_path") or row.get("folder_path") or "Dropbox"),
            matched=int(row.get("matched_count", 0) or 0),
            copied=int(row.get("copied_count", 0) or 0),
            failed=int(row.get("failed_count", 0) or 0),
            skipped=int(row.get("skipped_count", 0) or 0),
            total_size=int(row.get("total_size", 0) or 0),
        )
        for row in summary.get("folder_breakdown", [])
    ]
    top_folders.sort(key=lambda row: (row.matched, row.copied, row.total_size), reverse=True)

    return ResultsViewModel(
        run_id=str(summary.get("run_id") or ""),
        mode=str(summary.get("mode") or ""),
        created_at=str(summary.get("created_at") or ""),
        metrics=metrics,
        status_slices=status_slices,
        top_folders=top_folders[:8],
        conflicts=list(summary.get("conflicts_preview") or []),
        failures=list(summary.get("failures_preview") or []),
        blocked=list(summary.get("blocked_preview") or []),
        verification=verification,
        output_files=output_files,
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _metric_value(metrics: list[MetricTile], label: str) -> int:
    for metric in metrics:
        if metric.label == label:
            return metric.value
    return 0
