from __future__ import annotations

import csv
import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.models.config import AuthConfig, JobConfig, OutputPaths, RunContext
from app.models.records import SummaryReport, VerificationRecord
from app.persistence.repository import RunStateRepository
from app.utils.atomic import atomic_text_write


def _atomic_csv_write(path: Path, fieldnames: list[str], rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        Path(temp_name).replace(path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


class ReportWriter:
    def __init__(self, repository: RunStateRepository) -> None:
        self._repository = repository

    def write_inventory_csv(self, run_id: str, path: Path) -> None:
        _atomic_csv_write(
            path,
            [
                "item_type",
                "full_path",
                "filename",
                "parent_path",
                "dropbox_id",
                "size",
                "server_modified",
                "client_modified",
                "content_hash",
                "root_scope_used",
                "inventory_run_id",
                "inventory_timestamp",
                "account_mode",
                "namespace_id",
                "namespace_type",
                "namespace_name",
                "member_id",
                "member_email",
                "member_display_name",
                "canonical_source_path",
                "archive_bucket",
            ],
            (
                {
                    "item_type": row["item_type"],
                    "full_path": row["full_path"],
                    "filename": row["filename"],
                    "parent_path": row["parent_path"],
                    "dropbox_id": row["dropbox_id"],
                    "size": row["size"],
                    "server_modified": row["server_modified"],
                    "client_modified": row["client_modified"],
                    "content_hash": row["content_hash"],
                    "root_scope_used": row["root_scope_used"],
                    "inventory_run_id": row["run_id"],
                    "inventory_timestamp": row["inventory_timestamp"],
                    "account_mode": row["account_mode"],
                    "namespace_id": row["namespace_id"],
                    "namespace_type": row["namespace_type"],
                    "namespace_name": row["namespace_name"],
                    "member_id": row["member_id"],
                    "member_email": row["member_email"],
                    "member_display_name": row["member_display_name"],
                    "canonical_source_path": row["canonical_source_path"],
                    "archive_bucket": row["archive_bucket"],
                }
                for row in self._repository.iter_inventory_records(run_id)
            ),
        )

    def write_matched_csv(self, run_id: str, path: Path) -> None:
        _atomic_csv_write(
            path,
            [
                "original_path",
                "filename",
                "dropbox_id",
                "size",
                "server_modified",
                "client_modified",
                "content_hash",
                "planned_archive_path",
                "archive_canonical_path",
                "match_reason",
                "filter_run_id",
                "filter_timestamp",
                "account_mode",
                "namespace_id",
                "namespace_type",
                "namespace_name",
                "member_id",
                "member_email",
                "member_display_name",
                "canonical_source_path",
                "archive_bucket",
            ],
            (
                {
                    "original_path": row["original_path"],
                    "filename": row["filename"],
                    "dropbox_id": row["dropbox_id"],
                    "size": row["size"],
                    "server_modified": row["server_modified"],
                    "client_modified": row["client_modified"],
                    "content_hash": row["content_hash"],
                    "planned_archive_path": row["planned_archive_path"],
                    "archive_canonical_path": row["archive_canonical_path"],
                    "match_reason": row["match_reason"],
                    "filter_run_id": row["run_id"],
                    "filter_timestamp": row["filter_timestamp"],
                    "account_mode": row["account_mode"],
                    "namespace_id": row["namespace_id"],
                    "namespace_type": row["namespace_type"],
                    "namespace_name": row["namespace_name"],
                    "member_id": row["member_id"],
                    "member_email": row["member_email"],
                    "member_display_name": row["member_display_name"],
                    "canonical_source_path": row["canonical_source_path"],
                    "archive_bucket": row["archive_bucket"],
                }
                for row in self._repository.iter_matched_files(run_id)
            ),
        )

    def write_manifest_csv(self, run_id: str, path: Path) -> None:
        _atomic_csv_write(
            path,
            [
                "run_id",
                "mode",
                "original_path",
                "canonical_source_path",
                "archive_path",
                "archive_canonical_path",
                "dropbox_id",
                "size",
                "server_modified",
                "client_modified",
                "content_hash",
                "status",
                "status_detail",
                "attempt_count",
                "first_attempt_at",
                "last_attempt_at",
                "account_mode",
                "namespace_id",
                "namespace_type",
                "namespace_name",
                "member_id",
                "member_email",
                "member_display_name",
                "archive_bucket",
            ],
            (record.to_csv_row() for record in self._repository.manifest_rows(run_id)),
        )

    def write_verification_outputs(
        self,
        verification_rows: list[VerificationRecord],
        csv_path: Path,
        json_path: Path,
    ) -> dict[str, Any]:
        rows = [asdict(row) for row in verification_rows]
        _atomic_csv_write(
            csv_path,
            [
                "original_path",
                "archive_path",
                "verification_status",
                "detail",
                "source_size",
                "archive_size",
                "source_content_hash",
                "archive_content_hash",
                "account_mode",
                "namespace_id",
                "namespace_type",
                "namespace_name",
                "member_id",
                "member_email",
                "member_display_name",
                "canonical_source_path",
                "archive_canonical_path",
                "archive_bucket",
            ],
            rows,
        )
        summary = {
            "source_matched_file_count": len(verification_rows),
            "archive_staged_file_count": sum(1 for row in verification_rows if row.verification_status == "verified"),
            "source_matched_total_size": sum(row.source_size or 0 for row in verification_rows),
            "archive_staged_total_size": sum((row.archive_size or 0) for row in verification_rows if row.verification_status == "verified"),
            "missing_archive_targets": [row.archive_path for row in verification_rows if row.verification_status == "missing_archive_target"],
            "conflicts": [row.archive_path for row in verification_rows if row.verification_status == "conflict"],
            "successful_staged_copies": [row.archive_path for row in verification_rows if row.verification_status == "verified"],
        }
        atomic_text_write(json_path, json.dumps({"rows": rows, "summary": summary}, indent=2))
        return summary

    def write_summary_outputs(
        self,
        *,
        run_context: RunContext,
        output_paths: OutputPaths,
        verification_summary: dict[str, Any],
    ) -> SummaryReport:
        counters = self._repository.get_counters(run_context.run_id)
        folder_breakdown = self._repository.build_folder_summary(run_context.run_id)
        report = SummaryReport(
            run_id=run_context.run_id,
            mode=run_context.mode,
            phase="completed",
            created_at=run_context.created_at,
            totals=counters,
            folder_breakdown=folder_breakdown,
            conflicts_preview=self._repository.preview_copy_statuses(run_context.run_id, "skipped_existing_conflict"),
            failures_preview=self._repository.preview_copy_statuses(run_context.run_id, "failed"),
            blocked_preview=self._repository.preview_copy_statuses(run_context.run_id, "blocked_precondition"),
            verification=verification_summary,
        )
        atomic_text_write(output_paths.summary_json, json.dumps(asdict(report), indent=2))
        atomic_text_write(output_paths.summary_text, self._summary_markdown(report))
        return report

    def write_config_snapshot(
        self,
        *,
        path: Path,
        run_context: RunContext,
        job_config: JobConfig,
        auth_config: AuthConfig,
    ) -> None:
        snapshot = {
            "run_id": run_context.run_id,
            "created_at": run_context.created_at,
            "mode": run_context.mode,
            "job": {
                "source_roots": job_config.source_roots,
                "cutoff_date": job_config.cutoff_date,
                "archive_root": job_config.archive_root,
                "output_dir": str(job_config.output_dir),
                "batch_size": job_config.batch_size,
                "retry": asdict(job_config.retry),
                "conflict_policy": job_config.conflict_policy,
                "include_folders_in_inventory": job_config.include_folders_in_inventory,
                "exclude_archive_destination": job_config.exclude_archive_destination,
                "worker_count": job_config.worker_count,
                "verify_after_run": job_config.verify_after_run,
                "team_coverage_preset": job_config.team_coverage_preset,
            },
            "auth": {
                "method": auth_config.method,
                "account_mode": auth_config.account_mode,
                "app_key": auth_config.app_key,
                "scopes": list(auth_config.scopes),
                "admin_member_id": auth_config.admin_member_id,
            },
        }
        atomic_text_write(path, json.dumps(snapshot, indent=2))

    def write_latest_pointer(self, output_paths: OutputPaths, run_context: RunContext) -> None:
        payload = {
            "run_id": run_context.run_id,
            "run_dir": str(output_paths.run_dir),
            "state_db": str(output_paths.state_db),
            "summary_json": str(output_paths.summary_json),
            "verification_json": str(output_paths.verification_json),
        }
        atomic_text_write(output_paths.latest_pointer, json.dumps(payload, indent=2))

    def _summary_markdown(self, report: SummaryReport) -> str:
        lines = [
            "# Dropbox Cleaner Summary",
            "",
            f"- Run ID: `{report.run_id}`",
            f"- Mode: `{report.mode}`",
            f"- Created At: `{report.created_at}`",
            f"- Items Scanned: `{report.totals.get('items_scanned', 0)}`",
            f"- Namespaces Scanned: `{report.totals.get('namespaces_scanned', 0)}`",
            f"- Members Covered: `{report.totals.get('members_covered', 0)}`",
            f"- Files Matched: `{report.totals.get('files_matched', 0)}`",
            f"- Files Copied: `{report.totals.get('files_copied', 0)}`",
            f"- Files Skipped: `{report.totals.get('files_skipped', 0)}`",
            f"- Files Failed: `{report.totals.get('files_failed', 0)}`",
            "",
            "## Folder Breakdown",
            "",
            "| Folder | Files | Total Size | Matched | Copied | Failed | Skipped |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for folder in report.folder_breakdown:
            lines.append(
                f"| `{folder.folder_path}` | {folder.file_count} | {folder.total_size} | "
                f"{folder.matched_count} | {folder.copied_count} | {folder.failed_count} | {folder.skipped_count} |"
            )

        if report.conflicts_preview:
            lines.extend(["", "## Conflicts", ""])
            lines.extend(f"- {item}" for item in report.conflicts_preview)
        if report.failures_preview:
            lines.extend(["", "## Failures", ""])
            lines.extend(f"- {item}" for item in report.failures_preview)
        if report.blocked_preview:
            lines.extend(["", "## Blocked Preconditions", ""])
            lines.extend(f"- {item}" for item in report.blocked_preview)
        if report.verification:
            lines.extend(
                [
                    "",
                    "## Verification",
                    "",
                    f"- Source matched file count: `{report.verification.get('source_matched_file_count', 0)}`",
                    f"- Archive staged file count: `{report.verification.get('archive_staged_file_count', 0)}`",
                    f"- Source matched total size: `{report.verification.get('source_matched_total_size', 0)}`",
                    f"- Archive staged total size: `{report.verification.get('archive_staged_total_size', 0)}`",
                ]
            )
        return "\n".join(lines) + "\n"
