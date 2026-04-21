from __future__ import annotations

import logging
from collections.abc import Iterable

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.errors import CursorResetError, TemporaryDropboxError
from app.models.config import JobConfig, RunContext
from app.models.events import ProgressSnapshot
from app.models.records import InventoryRecord, RemoteEntry, TraversalRoot
from app.persistence.repository import RunStateRepository
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken, ProgressEmitter
from app.utils.paths import normalize_dropbox_path, parent_path
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
        traversal_roots: list[TraversalRoot] | None = None,
    ) -> None:
        targets = traversal_roots or [
            TraversalRoot(
                root_key=normalize_dropbox_path(root_path),
                root_path=normalize_dropbox_path(root_path),
                account_mode="personal",
                canonical_root=normalize_dropbox_path(root_path),
            )
            for root_path in source_roots
        ]
        for root in targets:
            cancellation_token.check()
            if root.account_mode == "personal" and planner.is_excluded_from_sources(root.root_path):
                self._logger.info(
                    "Skipped source root %s because it is the archive destination or inside it.",
                    root.root_path,
                    extra={"phase": "inventory"},
                )
                self._repository.record_event(
                    run_context.run_id,
                    "inventory",
                    "INFO",
                    "source_root_excluded",
                    f"Skipped source root {root.root_path} because it is inside the archive destination.",
                    {"root_path": root.root_path},
                )
                self._repository.save_inventory_checkpoint(
                    run_context.run_id,
                    root.root_key,
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
                        root=root,
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
                        root.root_key,
                        extra={"phase": "inventory"},
                    )
                    self._repository.record_event(
                        run_context.run_id,
                        "inventory",
                        "WARNING",
                        "cursor_reset",
                        f"Cursor reset while inventorying {root.root_key}; restarting that root.",
                        {"root_key": root.root_key},
                    )
                    self._repository.delete_inventory_items_for_root(run_context.run_id, root.root_key)

    def _run_root(
        self,
        *,
        adapter: DropboxAdapter,
        run_context: RunContext,
        job_config: JobConfig,
        root: TraversalRoot,
        planner: ArchivePlanner,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
    ) -> None:
        checkpoint = self._repository.get_inventory_checkpoint(run_context.run_id, root.root_key)
        if checkpoint and checkpoint["completed"]:
            self._logger.info("Inventory root %s already complete. Skipping.", root.root_key, extra={"phase": "inventory"})
            return

        page_count = int(checkpoint["page_count"]) if checkpoint else 0
        item_count = int(checkpoint["item_count"]) if checkpoint else 0

        if checkpoint and checkpoint["cursor"]:
            page = retry_call(
                operation_name=f"list_folder_continue({root.root_key})",
                func=lambda: adapter.list_folder_continue(checkpoint["cursor"]),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
            self._logger.info("Resuming inventory for %s from a saved cursor.", root.root_key, extra={"phase": "inventory"})
        else:
            page = retry_call(
                operation_name=f"list_folder({root.root_key})",
                func=lambda: adapter.list_folder(
                    root.root_path,
                    recursive=True,
                    limit=job_config.batch_size,
                    include_mounted_folders=root.include_mounted_folders,
                    namespace_id=root.namespace_id,
                ),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )
            self._logger.info("Starting inventory for %s.", root.root_key, extra={"phase": "inventory"})

        while True:
            cancellation_token.check()
            inventory_timestamp = isoformat_utc(utc_now())
            records = list(
                self._to_inventory_records(
                    run_id=run_context.run_id,
                    root=root,
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
                root.root_key,
                cursor=page.cursor,
                completed=not page.has_more,
                page_count=page_count,
                item_count=item_count,
            )

            if emit is not None:
                emit(
                    ProgressSnapshot(
                        phase="inventory",
                        message=f"Inventorying {root.namespace_name or root.root_path}",
                        counters=self._repository.get_counters(run_context.run_id),
                    )
                )

            if not page.has_more:
                break

            page = retry_call(
                operation_name=f"list_folder_continue({root.root_key})",
                func=lambda: adapter.list_folder_continue(page.cursor),
                logger=self._logger,
                retry_settings=job_config.retry,
                is_retryable=lambda exc: isinstance(exc, TemporaryDropboxError),
            )

    def _to_inventory_records(
        self,
        *,
        run_id: str,
        root: TraversalRoot,
        inventory_timestamp: str | None,
        entries: Iterable[RemoteEntry],
        include_folders: bool,
        planner: ArchivePlanner,
    ) -> Iterable[InventoryRecord]:
        for entry in entries:
            enriched = self._merge_entry_with_root(entry, root)
            if root.account_mode == "personal" and planner.is_excluded_from_sources(enriched.full_path):
                self._logger.info(
                    "Excluded %s from inventory because it is inside the archive destination.",
                    enriched.full_path,
                    extra={"phase": "inventory"},
                )
                continue
            if enriched.item_type == "folder" and not include_folders:
                continue
            yield InventoryRecord(
                item_type=enriched.item_type,
                full_path=enriched.full_path,
                path_lower=enriched.path_lower,
                filename=enriched.filename,
                parent_path=enriched.parent_path,
                dropbox_id=enriched.dropbox_id,
                size=enriched.size,
                server_modified=enriched.server_modified,
                client_modified=enriched.client_modified,
                content_hash=enriched.content_hash,
                root_scope_used=root.root_key,
                inventory_run_id=run_id,
                inventory_timestamp=inventory_timestamp or "",
                account_mode=enriched.account_mode,
                namespace_id=enriched.namespace_id,
                namespace_type=enriched.namespace_type,
                namespace_name=enriched.namespace_name,
                member_id=enriched.member_id,
                member_email=enriched.member_email,
                member_display_name=enriched.member_display_name,
                canonical_source_path=enriched.canonical_source_path or enriched.full_path,
                canonical_parent_path=enriched.canonical_parent_path or parent_path(enriched.full_path),
                archive_bucket=enriched.archive_bucket,
            )

    def _merge_entry_with_root(self, entry: RemoteEntry, root: TraversalRoot) -> RemoteEntry:
        return RemoteEntry(
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
            account_mode=root.account_mode,
            namespace_id=root.namespace_id or entry.namespace_id,
            namespace_type=root.namespace_type or entry.namespace_type,
            namespace_name=root.namespace_name or entry.namespace_name,
            member_id=root.member_id or entry.member_id,
            member_email=root.member_email or entry.member_email,
            member_display_name=root.member_display_name or entry.member_display_name,
            canonical_source_path=entry.canonical_source_path,
            canonical_parent_path=entry.canonical_parent_path,
            archive_bucket=root.archive_bucket,
        )
