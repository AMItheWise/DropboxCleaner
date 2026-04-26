from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.ui.results import ResultsViewModel, load_results_view_model


def discover_run_dirs(output_dir: Path) -> tuple[str | None, list[Path]]:
    output_dir = output_dir.expanduser()
    latest_run_dir: Path | None = None
    latest_pointer = output_dir / "latest_run.json"
    if latest_pointer.exists():
        try:
            payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
            latest_run_dir = Path(payload["run_dir"]).expanduser()
        except Exception:  # noqa: BLE001
            latest_run_dir = None

    run_dirs = []
    if output_dir.exists():
        run_dirs = [path for path in output_dir.iterdir() if path.is_dir() and (path / "summary.json").exists()]
    if latest_run_dir is not None and latest_run_dir.exists() and (latest_run_dir / "summary.json").exists():
        run_dirs.append(latest_run_dir)

    unique = {str(path.resolve()): path for path in run_dirs}
    sorted_dirs = sorted(unique.values(), key=lambda path: path.stat().st_mtime, reverse=True)
    latest_run_id = None
    if latest_run_dir is not None and (latest_run_dir / "summary.json").exists():
        latest_run_id = _safe_result(latest_run_dir).run_id or None
    return latest_run_id, sorted_dirs


def find_run_dir(output_dir: Path, run_id: str) -> Path | None:
    _latest_run_id, run_dirs = discover_run_dirs(output_dir)
    for run_dir in run_dirs:
        if run_dir.name == run_id:
            return run_dir
        result = _safe_result(run_dir)
        if result.run_id == run_id:
            return run_dir
    return None


def history_item(run_dir: Path, *, latest_run_id: str | None = None) -> dict[str, Any]:
    result = _safe_result(run_dir)
    return {
        "run_id": result.run_id or run_dir.name,
        "mode": result.mode,
        "created_at": result.created_at,
        "run_dir": str(run_dir),
        "latest": bool(latest_run_id and result.run_id == latest_run_id),
        "status_message": result.success_message,
        "metrics": [_metric_to_dict(metric) for metric in result.metrics],
        "has_issues": result.has_issues,
    }


def result_payload(run_dir: Path) -> dict[str, Any]:
    result = _safe_result(run_dir)
    return {
        "run_id": result.run_id,
        "mode": result.mode,
        "created_at": result.created_at,
        "success_message": result.success_message,
        "review_title": result.review_title,
        "has_issues": result.has_issues,
        "has_skipped_details": result.has_skipped_details,
        "metrics": [_metric_to_dict(metric) for metric in result.metrics],
        "status_slices": [asdict(slice_) for slice_ in result.status_slices],
        "top_folders": [asdict(folder) for folder in result.top_folders],
        "already_archived": result.already_archived,
        "conflicts": result.conflicts,
        "failures": result.failures,
        "blocked": result.blocked,
        "verification": result.verification,
        "output_files": [path.name for path in result.output_files],
    }


def safe_output_file(run_dir: Path, filename: str) -> Path | None:
    if "/" in filename or "\\" in filename or filename in ("", ".", ".."):
        return None
    candidate = (run_dir / filename).resolve()
    try:
        candidate.relative_to(run_dir.resolve())
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    allowed = {path.name for path in _safe_result(run_dir).output_files}
    return candidate if candidate.name in allowed else None


def _safe_result(run_dir: Path) -> ResultsViewModel:
    try:
        return load_results_view_model(run_dir)
    except Exception:  # noqa: BLE001
        return ResultsViewModel(run_id=run_dir.name)


def _metric_to_dict(metric: Any) -> dict[str, Any]:
    return {"label": metric.label, "value": metric.value, "tone": metric.tone}
