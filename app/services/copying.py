from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import PurePosixPath

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.errors import (
    BlockedPreconditionError,
    ConflictPolicyAbortError,
    DestinationConflictError,
    PathNotFoundError,
    TemporaryDropboxError,
)
from app.models.config import AuthConfig, JobConfig, RunContext
from app.models.events import ProgressSnapshot
from app.models.records import RemoteEntry
from app.persistence.repository import RunStateRepository
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken, ProgressEmitter
from app.utils.paths import (
    join_dropbox_path,
    namespace_relative_parent,
    namespace_relative_path,
    normalize_dropbox_path,
    parent_path,
    split_namespace_relative_path,
)
from app.utils.retry import retry_call
from app.utils.time import isoformat_utc, utc_now

PENDING_COPY_STATUSES = ("planned", "resumed")


class ArchiveCopyService:
    def __init__(self, repository: RunStateRepository, logger: logging.Logger) -> None:
        self._repository = repository
        self._logger = logger
        self._ensured_folders: set[str] = set()
        self._folder_lock = threading.RLock()

    def run(
        self,
        *,
        adapter: DropboxAdapter,
        run_context: RunContext,
        job_config: JobConfig,
        planner: ArchivePlanner,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
        dry_run: bool,
        adapter_factory: Callable[[AuthConfig, logging.Logger], DropboxAdapter] | None = None,
        auth_config: AuthConfig | None = None,
    ) -> None:
        worker_count = max(1, int(job_config.worker_count))
        if worker_count > 1:
            self._logger.info(
                "Copy phase will use up to %s worker threads.",
                job_config.worker_count,
                extra={"phase": "copy"},
            )

        resumed_jobs = self._repository.promote_copy_jobs(
            run_context.run_id,
            from_statuses=("failed", "retried", "blocked_precondition"),
            to_status="resumed",
            detail="Resumed from a previous incomplete run.",
        )
        if resumed_jobs:
            self._logger.info(
                "Resuming %s previously incomplete copy jobs.",
                resumed_jobs,
                extra={"phase": "copy"},
            )

        if planner.account_mode == "team_admin" and planner.team_discovery is not None:
            if not planner.team_discovery.archive_namespace_id:
                detail = planner.team_discovery.archive_status_detail or "Central archive namespace is not ready."
                self._repository.promote_copy_jobs(
                    run_context.run_id,
                    from_statuses=PENDING_COPY_STATUSES,
                    to_status="blocked_precondition",
                    detail=detail,
                )
                return
            if not dry_run and not planner.team_discovery.archive_provisioned:
                detail = planner.team_discovery.archive_status_detail or "Central archive folder is not ready for writing."
                self._repository.promote_copy_jobs(
                    run_context.run_id,
                    from_statuses=PENDING_COPY_STATUSES,
                    to_status="blocked_precondition",
                    detail=detail,
                )
                return
            if not dry_run:
                self._ensure_folder_chain(
                    adapter,
                    namespace_relative_path(planner.team_discovery.archive_namespace_id, "/"),
                    job_config,
                )
        elif not dry_run:
            self._ensure_folder_chain(adapter, planner.archive_root, job_config)

        worker_adapters: list[DropboxAdapter] = []
        worker_adapters_lock = threading.Lock()
        worker_local = threading.local()

        def adapter_for_worker() -> DropboxAdapter:
            if worker_count <= 1 or adapter_factory is None or auth_config is None:
                return adapter
            local_adapter = getattr(worker_local, "adapter", None)
            if local_adapter is None:
                local_adapter = adapter_factory(auth_config, self._logger)
                setattr(worker_local, "adapter", local_adapter)
                with worker_adapters_lock:
                    worker_adapters.append(local_adapter)
            return local_adapter

        try:
            last_job_key: str | None = None
            while True:
                cancellation_token.check()
                pending_jobs = self._repository.fetch_copy_jobs(
                    run_context.run_id,
                    statuses=PENDING_COPY_STATUSES,
                    limit=max(1, job_config.batch_size),
                    after_job_key=last_job_key,
                )
                if not pending_jobs:
                    break
                if worker_count <= 1 or len(pending_jobs) == 1:
                    for job in pending_jobs:
                        cancellation_token.check()
                        self._process_job(
                            adapter=adapter,
                            run_id=run_context.run_id,
                            job=job,
                            job_config=job_config,
                            planner=planner,
                            dry_run=dry_run,
                        )
                        self._emit_copy_progress(emit, run_context.run_id, dry_run, job)
                else:
                    self._process_jobs_parallel(
                        adapter_for_worker=adapter_for_worker,
                        run_id=run_context.run_id,
                        jobs=pending_jobs,
                        job_config=job_config,
                        planner=planner,
                        dry_run=dry_run,
                        emit=emit,
                    )
                last_job_key = pending_jobs[-1]["canonical_source_path"]
        finally:
            for worker_adapter in worker_adapters:
                try:
                    worker_adapter.close()
                except Exception:  # noqa: BLE001
                    continue

    def _process_jobs_parallel(
        self,
        *,
        adapter_for_worker: Callable[[], DropboxAdapter],
        run_id: str,
        jobs: list[dict],
        job_config: JobConfig,
        planner: ArchivePlanner,
        dry_run: bool,
        emit: ProgressEmitter | None,
    ) -> None:
        max_workers = min(max(1, int(job_config.worker_count)), len(jobs))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dropbox-copy") as executor:
            futures = {
                executor.submit(
                    self._process_job_with_worker_adapter,
                    adapter_for_worker=adapter_for_worker,
                    run_id=run_id,
                    job=job,
                    job_config=job_config,
                    planner=planner,
                    dry_run=dry_run,
                ): job
                for job in jobs
            }
            for future in as_completed(futures):
                job = futures[future]
                future.result()
                self._emit_copy_progress(emit, run_id, dry_run, job)

    def _process_job_with_worker_adapter(
        self,
        *,
        adapter_for_worker: Callable[[], DropboxAdapter],
        run_id: str,
        job: dict,
        job_config: JobConfig,
        planner: ArchivePlanner,
        dry_run: bool,
    ) -> None:
        self._process_job(
            adapter=adapter_for_worker(),
            run_id=run_id,
            job=job,
            job_config=job_config,
            planner=planner,
            dry_run=dry_run,
        )

    def _emit_copy_progress(self, emit: ProgressEmitter | None, run_id: str, dry_run: bool, job: dict) -> None:
        if emit is None:
            return
        emit(
            ProgressSnapshot(
                phase="copy" if not dry_run else "dry_run",
                message=f"Processing {job['original_path']}",
                counters=self._repository.get_counters(run_id),
            )
        )

    def _process_job(
        self,
        *,
        adapter: DropboxAdapter,
        run_id: str,
        job: dict,
        job_config: JobConfig,
        planner: ArchivePlanner,
        dry_run: bool,
    ) -> None:
        original_path = job["original_path"]
        source_path = job["canonical_source_path"]
        archive_display_path = job["archive_path"]
        if self._job_is_user_excluded(job, planner):
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="excluded",
                status_detail="Skipped because the source path matches an excluded folder for this run.",
                attempt_count=job["attempt_count"],
                first_attempt_at=job["first_attempt_at"],
                last_attempt_at=job["last_attempt_at"],
                archive_canonical_path=job.get("archive_canonical_path"),
            )
            return
        planned_canonical_path = planner.build_archive_canonical_path(
            archive_display_path,
            archive_bucket=job.get("archive_bucket") or "personal",
            namespace_id=job.get("namespace_id"),
        )
        archive_canonical_path = (
            planned_canonical_path
            if planner.account_mode == "team_admin" and planned_canonical_path is not None
            else job["archive_canonical_path"] or planned_canonical_path
        )
        now_iso = isoformat_utc(utc_now())
        next_attempt_count = int(job["attempt_count"]) + (0 if dry_run else 1)
        first_attempt_at = job["first_attempt_at"] or now_iso

        if not archive_canonical_path:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="blocked_precondition",
                status_detail="Archive namespace could not be resolved for this run.",
                attempt_count=job["attempt_count"],
                first_attempt_at=job["first_attempt_at"],
                last_attempt_at=job["last_attempt_at"],
                archive_canonical_path=None,
            )
            return

        try:
            existing_destination = retry_call(
                operation_name=f"get_metadata({archive_canonical_path})",
                func=lambda: adapter.get_metadata(archive_canonical_path),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
        except BlockedPreconditionError as exc:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="blocked_precondition",
                status_detail=str(exc),
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            return

        if existing_destination is not None:
            if self._is_existing_copy_same(existing_destination, job):
                self._repository.update_copy_job_status(
                    run_id,
                    source_path,
                    status="skipped_existing_same",
                    status_detail="Destination already exists and matches the planned source metadata.",
                    attempt_count=next_attempt_count,
                    first_attempt_at=first_attempt_at,
                    last_attempt_at=now_iso,
                    archive_canonical_path=archive_canonical_path,
                )
                return
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="skipped_existing_conflict",
                status_detail="Destination already exists and could not be safely confirmed identical.",
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            if job_config.conflict_policy == "abort_run":
                raise ConflictPolicyAbortError(
                    f"Conflict detected at {archive_display_path} and the run is configured to abort on conflicts."
                )
            return

        if dry_run:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="planned",
                status_detail=f"DRY RUN: would copy to {archive_display_path}.",
                attempt_count=job["attempt_count"],
                first_attempt_at=job["first_attempt_at"],
                last_attempt_at=job["last_attempt_at"],
                archive_canonical_path=archive_canonical_path,
            )
            return

        try:
            self._ensure_folder_chain(adapter, namespace_relative_parent(archive_canonical_path), job_config)
            copied_entry = retry_call(
                operation_name=f"copy_file({source_path} -> {archive_canonical_path})",
                func=lambda: adapter.copy_file(
                    source_path,
                    archive_canonical_path,
                    member_id=job.get("member_id"),
                    source_display_path=original_path,
                    destination_display_path=archive_display_path,
                ),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
        except DestinationConflictError:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="skipped_existing_conflict",
                status_detail="Dropbox reported a destination or folder-chain conflict during server-side copy.",
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            if job_config.conflict_policy == "abort_run":
                raise ConflictPolicyAbortError(
                    f"Conflict detected at {archive_display_path} and the run is configured to abort on conflicts."
                )
            return
        except PathNotFoundError as exc:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="failed",
                status_detail=f"Source path was not found: {exc}",
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            return
        except BlockedPreconditionError as exc:
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="blocked_precondition",
                status_detail=str(exc),
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            return
        except Exception as exc:  # noqa: BLE001
            self._repository.update_copy_job_status(
                run_id,
                source_path,
                status="failed",
                status_detail=str(exc),
                attempt_count=next_attempt_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=now_iso,
                archive_canonical_path=archive_canonical_path,
            )
            return

        self._repository.update_copy_job_status(
            run_id,
            source_path,
            status="copied",
            status_detail="Server-side copy completed successfully.",
            attempt_count=next_attempt_count,
            first_attempt_at=first_attempt_at,
            last_attempt_at=now_iso,
            archive_path=archive_display_path,
            archive_canonical_path=archive_canonical_path,
        )

    def _ensure_folder_chain(self, adapter: DropboxAdapter, path: str, job_config: JobConfig) -> None:
        with self._folder_lock:
            namespace_id, relative_path = split_namespace_relative_path(path)
            normalized = normalize_dropbox_path(relative_path)
            if normalized == "/":
                return
            current_relative = ""
            for part in PurePosixPath(normalized).parts:
                if part == "/":
                    continue
                current_relative = f"{current_relative}/{part}" if current_relative else f"/{part}"
                current = namespace_relative_path(namespace_id, current_relative)
                if current in self._ensured_folders:
                    continue
                retry_call(
                    operation_name=f"create_folder_if_missing({current})",
                    func=lambda current_path=current: adapter.create_folder_if_missing(current_path),
                    logger=self._logger,
                    retry_settings=job_config.retry,
                    is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
                )
                self._ensured_folders.add(current)

    def _is_existing_copy_same(self, existing: RemoteEntry, job: dict) -> bool:
        if existing.item_type != "file":
            return False
        existing_hash = existing.content_hash
        job_hash = job["content_hash"]
        existing_size = existing.size
        job_size = job["size"]
        if existing_hash and job_hash:
            return existing_hash == job_hash and existing_size == job_size
        if existing_size is not None and job_size is not None and existing_size == job_size:
            return existing.server_modified == job["server_modified"]
        return False

    def _job_is_user_excluded(self, job: dict, planner: ArchivePlanner) -> bool:
        original_path = job["original_path"]
        if planner.is_user_excluded(original_path):
            return True
        if planner.account_mode != "team_admin" or job.get("archive_bucket") == "member_homes":
            return False
        namespace_id = job.get("namespace_id")
        root_namespace_id = planner.team_discovery.root_namespace_id if planner.team_discovery else None
        if namespace_id and root_namespace_id and namespace_id == root_namespace_id:
            return False
        namespace_name = job.get("namespace_name")
        if not namespace_name:
            return False
        return planner.is_user_excluded(join_dropbox_path("/", namespace_name, original_path))
