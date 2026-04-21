from __future__ import annotations

import logging
from datetime import datetime

from app.models.config import JobConfig, RunContext
from app.models.events import ProgressSnapshot
from app.models.records import MatchedFileRecord
from app.persistence.repository import RunStateRepository
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken, ProgressEmitter
from app.utils.time import isoformat_utc, parse_cutoff_date, parse_iso8601, utc_now


class FilterService:
    def __init__(self, repository: RunStateRepository, logger: logging.Logger) -> None:
        self._repository = repository
        self._logger = logger

    def run(
        self,
        *,
        run_context: RunContext,
        job_config: JobConfig,
        planner: ArchivePlanner,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
    ) -> int:
        self._repository.clear_matches(run_context.run_id)
        cutoff = parse_cutoff_date(job_config.cutoff_date)
        date_filter_field = job_config.date_filter_field
        matched = 0
        buffer: list[MatchedFileRecord] = []
        filter_timestamp = isoformat_utc(utc_now()) or ""

        for row in self._repository.iter_inventory_records(run_context.run_id, item_type="file"):
            cancellation_token.check()
            comparison_timestamp = self._comparison_timestamp(row, date_filter_field)
            if comparison_timestamp is None or comparison_timestamp >= cutoff:
                continue
            planned_archive_path = planner.map_to_archive_path(
                row["full_path"],
                archive_bucket=row.get("archive_bucket") or "personal",
                member_email=row.get("member_email"),
                member_id=row.get("member_id"),
                namespace_name=row.get("namespace_name"),
                namespace_id=row.get("namespace_id"),
            )
            archive_canonical_path = planner.build_archive_canonical_path(
                planned_archive_path,
                archive_bucket=row.get("archive_bucket") or "personal",
                namespace_id=row.get("namespace_id"),
            )
            buffer.append(
                MatchedFileRecord(
                    original_path=row["full_path"],
                    path_lower=row["path_lower"],
                    filename=row["filename"],
                    dropbox_id=row["dropbox_id"],
                    size=row["size"],
                    server_modified=row["server_modified"],
                    client_modified=row["client_modified"],
                    content_hash=row["content_hash"],
                    planned_archive_path=planned_archive_path,
                    archive_canonical_path=archive_canonical_path,
                    match_reason=f"{date_filter_field}_before_{cutoff.date().isoformat()}",
                    filter_run_id=run_context.run_id,
                    filter_timestamp=filter_timestamp,
                    parent_path=row["parent_path"],
                    account_mode=row.get("account_mode", "personal"),
                    namespace_id=row.get("namespace_id"),
                    namespace_type=row.get("namespace_type", "personal"),
                    namespace_name=row.get("namespace_name"),
                    member_id=row.get("member_id"),
                    member_email=row.get("member_email"),
                    member_display_name=row.get("member_display_name"),
                    canonical_source_path=row["canonical_source_path"],
                    canonical_parent_path=row["canonical_parent_path"],
                    archive_bucket=row.get("archive_bucket") or "personal",
                )
            )
            if len(buffer) >= 500:
                matched += self._repository.upsert_matched_records(buffer, run_context.mode)
                buffer.clear()
                if emit is not None:
                    emit(
                        ProgressSnapshot(
                            phase="filter",
                            message="Filtering files by cutoff date",
                            counters=self._repository.get_counters(run_context.run_id),
                        )
                    )

        if buffer:
            matched += self._repository.upsert_matched_records(buffer, run_context.mode)

        self._logger.info(
            "Filter phase found %s matching files older than %s using %s.",
            matched,
            cutoff.date().isoformat(),
            date_filter_field,
            extra={"phase": "filter"},
        )
        return matched

    @staticmethod
    def _comparison_timestamp(row: dict, date_filter_field: str) -> datetime | None:
        if date_filter_field == "client_modified":
            return parse_iso8601(row.get("client_modified"))
        if date_filter_field == "oldest_modified":
            timestamps = [
                timestamp
                for timestamp in (
                    parse_iso8601(row.get("server_modified")),
                    parse_iso8601(row.get("client_modified")),
                )
                if timestamp is not None
            ]
            return min(timestamps) if timestamps else None
        return parse_iso8601(row.get("server_modified"))
