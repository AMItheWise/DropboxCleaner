from __future__ import annotations

import logging
from typing import Iterable

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.errors import CursorResetError, TemporaryDropboxError
from app.models.config import JobConfig, RunContext
from app.models.events import ProgressSnapshot
from app.models.records import InventoryRecord, RemoteEntry
from app.persistence.repository import RunStateRepository
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken, ProgressEmitter
from app.utils.paths import normalize_dropbox_path
from app.utils.retry import retry_call
from app.utils.time import isoformat_utc, utc_now


class DropboxInventoryService:
    def __init__(self, repository: RunStateRepository, logger: logging.Logger) -> None:
        self._repository = repository
        self._logger = logger

    def run(
        self,
        *,
        adapter: DropboxAdapter,
        run_context: RunContext,
        job_config: JobConfig,
        source_roots: list[str],
        planner: ArchivePlanner,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
    ) -> None:
        for root_path in source_roots:
            cancellation_token.check()
            normalized_root = normalize_dropbox_path(root_path)
            if planner.is_excluded_from_sources(normalized_root):
                self._logger.info(
                    "Skipped source root %s because it is the archive destination or inside it.",
                    normalized_root,
                    extra={"phase": "inventory"},
                )
                self._repository.record_event(
                    run_context.run_id,
                    "inventory",
                    "INFO",
                    "source_root_excluded",
                    f"Skipped source root {normalized_root} because it is inside the archive destination.",
                    {"root_path": normalized_root},
                )
                self._repository.save_inventory_checkpoint(
                    run_context.run_id,
                    normalized_root,
                    cursor=None,
                    completed=True,
                    page_count=0,
                    item_count=0,
                )
                continue

            restarted_after_cursor_reset = False
            while True:
                try:
                    self._run_root(
                        adapter=adapter,
                        run_context=run_context,
                        job_config=job_config,
                        root_path=normalized_root,
                        planner=planner,
                        emit=emit,
                        cancellation_token=cancellation_token,
                    )
                    break
                except CursorResetError:
                    if restarted_after_cursor_reset:
                        raise
                    restarted_after_cursor_reset = True
                    self._logger.warning(
                        "Cursor reset while inventorying %s. Restarting that root from scratch.",
                        normalized_root,
                        extra={"phase": "inventory"},
                    )
                    self._repository.record_event(
                        run_context.run_id,
                        "inventory",
                        "WARNING",
                        "cursor_reset",
                        f"Cursor reset while inventorying {normalized_root}; restarting that root.",
                        {"root_path": normalized_root},
                    )
                    self._repository.delete_inventory_items_for_root(run_context.run_id, normalized_root)

    def _run_root(
        self,
        *,
        adapter: DropboxAdapter,
        run_context: RunContext,
        job_config: JobConfig,
        root_path: str,
        planner: ArchivePlanner,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
    ) -> None:
        checkpoint = self._repository.get_inventory_checkpoint(run_context.run_id, root_path)
        if checkpoint and checkpoint["completed"]:
            self._logger.info("Inventory root %s already complete. Skipping.", root_path, extra={"phase": "inventory"})
            return

        page_count = int(checkpoint["page_count"]) if checkpoint else 0
        item_count = int(checkpoint["item_count"]) if checkpoint else 0

        if checkpoint and checkpoint["cursor"]:
            page = retry_call(
                operation_name=f"list_folder_continue({root_path})",
                func=lambda: adapter.list_folder_continue(checkpoint["cursor"]),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
            self._logger.info("Resuming inventory for %s from a saved cursor.", root_path, extra={"phase": "inventory"})
        else:
            page = retry_call(
                operation_name=f"list_folder({root_path})",
                func=lambda: adapter.list_folder(root_path, recursive=True, limit=job_config.batch_size),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
            self._logger.info("Starting inventory for %s.", root_path, extra={"phase": "inventory"})

        while True:
            cancellation_token.check()
            inventory_timestamp = isoformat_utc(utc_now())
            records = list(
                self._to_inventory_records(
                    run_id=run_context.run_id,
                    root_path=root_path,
                    inventory_timestamp=inventory_timestamp,
                    entries=page.entries,
                    include_folders=job_config.include_folders_in_inventory,
                    planner=planner,
                )
            )
            item_count += self._repository.upsert_inventory_records(records)
            page_count += 1
            self._repository.save_inventory_checkpoint(
                run_context.run_id,
                root_path,
                cursor=page.cursor,
                completed=not page.has_more,
                page_count=page_count,
                item_count=item_count,
            )

            if emit is not None:
                emit(
                    ProgressSnapshot(
                        phase="inventory",
                        message=f"Inventorying {root_path}",
                        counters=self._repository.get_counters(run_context.run_id),
                    )
                )

            if not page.has_more:
                break

            page = retry_call(
                operation_name=f"list_folder_continue({root_path})",
                func=lambda: adapter.list_folder_continue(page.cursor),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )

    def _to_inventory_records(
        self,
        *,
        run_id: str,
        root_path: str,
        inventory_timestamp: str | None,
        entries: Iterable[RemoteEntry],
        include_folders: bool,
        planner: ArchivePlanner,
    ) -> Iterable[InventoryRecord]:
        for entry in entries:
            if planner.is_excluded_from_sources(entry.full_path):
                self._logger.info(
                    "Excluded %s from inventory because it is inside the archive destination.",
                    entry.full_path,
                    extra={"phase": "inventory"},
                )
                continue
            if entry.item_type == "folder" and not include_folders:
                continue
            yield InventoryRecord(
                item_type=entry.item_type,
                full_path=entry.full_path,
                path_lower=entry.path_lower,
                filename=entry.filename,
                parent_path=entry.parent_path,
                dropbox_id=entry.dropbox_id,
                size=entry.size,
                server_modified=entry.server_modified,
                client_modified=entry.client_modified,
                content_hash=entry.content_hash,
                root_scope_used=root_path,
                inventory_run_id=run_id,
                inventory_timestamp=inventory_timestamp or "",
            )
