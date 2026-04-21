from __future__ import annotations

import logging

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.errors import TemporaryDropboxError
from app.models.config import JobConfig, RunContext
from app.models.events import ProgressSnapshot
from app.models.records import VerificationRecord
from app.persistence.repository import RunStateRepository
from app.services.runtime import CancellationToken, ProgressEmitter
from app.utils.retry import retry_call


class VerificationService:
    def __init__(self, repository: RunStateRepository, logger: logging.Logger) -> None:
        self._repository = repository
        self._logger = logger

    def run(
        self,
        *,
        adapter: DropboxAdapter,
        run_context: RunContext,
        job_config: JobConfig,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
    ) -> list[VerificationRecord]:
        rows: list[VerificationRecord] = []
        matched_files = list(self._repository.iter_matched_files(run_context.run_id))
        total = len(matched_files)
        for index, match in enumerate(matched_files, start=1):
            cancellation_token.check()
            archive_lookup_path = match["archive_canonical_path"] or match["planned_archive_path"]
            archive_entry = retry_call(
                operation_name=f"verify_get_metadata({archive_lookup_path})",
                func=lambda path=archive_lookup_path: adapter.get_metadata(path),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
            if archive_entry is None:
                rows.append(
                    VerificationRecord(
                        original_path=match["original_path"],
                        archive_path=match["planned_archive_path"],
                        verification_status="missing_archive_target",
                        detail="Archive target does not exist in Dropbox.",
                        source_size=match["size"],
                        archive_size=None,
                        source_content_hash=match["content_hash"],
                        archive_content_hash=None,
                        account_mode=match.get("account_mode", "personal"),
                        namespace_id=match.get("namespace_id"),
                        namespace_type=match.get("namespace_type", "personal"),
                        namespace_name=match.get("namespace_name"),
                        member_id=match.get("member_id"),
                        member_email=match.get("member_email"),
                        member_display_name=match.get("member_display_name"),
                        canonical_source_path=match.get("canonical_source_path"),
                        archive_canonical_path=match.get("archive_canonical_path"),
                        archive_bucket=match.get("archive_bucket", "personal"),
                    )
                )
            elif archive_entry.item_type != "file":
                rows.append(
                    VerificationRecord(
                        original_path=match["original_path"],
                        archive_path=match["planned_archive_path"],
                        verification_status="conflict",
                        detail="Archive target exists but is not a file.",
                        source_size=match["size"],
                        archive_size=archive_entry.size,
                        source_content_hash=match["content_hash"],
                        archive_content_hash=archive_entry.content_hash,
                        account_mode=match.get("account_mode", "personal"),
                        namespace_id=match.get("namespace_id"),
                        namespace_type=match.get("namespace_type", "personal"),
                        namespace_name=match.get("namespace_name"),
                        member_id=match.get("member_id"),
                        member_email=match.get("member_email"),
                        member_display_name=match.get("member_display_name"),
                        canonical_source_path=match.get("canonical_source_path"),
                        archive_canonical_path=match.get("archive_canonical_path"),
                        archive_bucket=match.get("archive_bucket", "personal"),
                    )
                )
            else:
                hashes_match = bool(
                    match["content_hash"]
                    and archive_entry.content_hash
                    and match["content_hash"] == archive_entry.content_hash
                )
                sizes_match = match["size"] == archive_entry.size
                if hashes_match or (match["content_hash"] is None and sizes_match):
                    rows.append(
                        VerificationRecord(
                            original_path=match["original_path"],
                            archive_path=match["planned_archive_path"],
                            verification_status="verified",
                            detail="Archive target exists and matches the planned source metadata.",
                            source_size=match["size"],
                            archive_size=archive_entry.size,
                            source_content_hash=match["content_hash"],
                            archive_content_hash=archive_entry.content_hash,
                            account_mode=match.get("account_mode", "personal"),
                            namespace_id=match.get("namespace_id"),
                            namespace_type=match.get("namespace_type", "personal"),
                            namespace_name=match.get("namespace_name"),
                            member_id=match.get("member_id"),
                            member_email=match.get("member_email"),
                            member_display_name=match.get("member_display_name"),
                            canonical_source_path=match.get("canonical_source_path"),
                            archive_canonical_path=match.get("archive_canonical_path"),
                            archive_bucket=match.get("archive_bucket", "personal"),
                        )
                    )
                else:
                    rows.append(
                        VerificationRecord(
                            original_path=match["original_path"],
                            archive_path=match["planned_archive_path"],
                            verification_status="conflict",
                            detail="Archive target exists but differs from the planned source metadata.",
                            source_size=match["size"],
                            archive_size=archive_entry.size,
                            source_content_hash=match["content_hash"],
                            archive_content_hash=archive_entry.content_hash,
                            account_mode=match.get("account_mode", "personal"),
                            namespace_id=match.get("namespace_id"),
                            namespace_type=match.get("namespace_type", "personal"),
                            namespace_name=match.get("namespace_name"),
                            member_id=match.get("member_id"),
                            member_email=match.get("member_email"),
                            member_display_name=match.get("member_display_name"),
                            canonical_source_path=match.get("canonical_source_path"),
                            archive_canonical_path=match.get("archive_canonical_path"),
                            archive_bucket=match.get("archive_bucket", "personal"),
                        )
                    )
            if emit is not None and index % 50 == 0:
                emit(
                    ProgressSnapshot(
                        phase="verify",
                        message=f"Verifying staged archive targets ({index}/{total})",
                        counters=self._repository.get_counters(run_context.run_id),
                    )
                )
        return rows
