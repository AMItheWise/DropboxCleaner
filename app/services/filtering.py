from __future__ import annotations

import logging

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
        matched = 0
        buffer: list[MatchedFileRecord] = []
        filter_timestamp = isoformat_utc(utc_now()) or ""

        for row in self._repository.iter_inventory_records(run_context.run_id, item_type="file"):
            cancellation_token.check()
            server_modified = parse_iso8601(row["server_modified"])
            if server_modified is None or server_modified >= cutoff:
                continue
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
                    planned_archive_path=planner.map_to_archive_path(row["full_path"]),
                    match_reason=f"server_modified_before_{cutoff.date().isoformat()}",
                    filter_run_id=run_context.run_id,
                    filter_timestamp=filter_timestamp,
                    parent_path=row["parent_path"],
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
            "Filter phase found %s matching files older than %s.",
            matched,
            cutoff.date().isoformat(),
            extra={"phase": "filter"},
        )
        return matched
