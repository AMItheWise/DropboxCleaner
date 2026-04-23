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
from app.utils.paths import is_same_or_descendant, join_dropbox_path, normalize_dropbox_path, parent_path
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
        include_roots = self._normalized_include_roots(job_config.source_roots)
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
            if self._is_root_excluded(root, planner):
                self._logger.info(
                    "Skipped source root %s because it is excluded from this run.",
                    root.root_path,
                    extra={"phase": "inventory"},
                )
                self._repository.record_event(
                    run_context.run_id,
                    "inventory",
                    "INFO",
                    "source_root_excluded",
                    f"Skipped source root {root.root_path} because it is excluded from this run.",
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
                        include_roots=include_roots,
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
        include_roots: list[str],
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
                func=lambda: adapter.list_folder_continue(checkpoint["cursor"], namespace_id=root.namespace_id),
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
                    include_roots=include_roots,
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
                func=lambda: adapter.list_folder_continue(page.cursor, namespace_id=root.namespace_id),
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
        include_roots: list[str],
    ) -> Iterable[InventoryRecord]:
        for entry in entries:
            enriched = self._merge_entry_with_root(entry, root)
            if self._is_entry_excluded(enriched, root, planner):
                self._logger.info(
                    "Excluded %s from inventory because it matches an excluded folder.",
                    enriched.full_path,
                    extra={"phase": "inventory"},
                )
                continue
            if not self._is_entry_included(enriched, root, include_roots):
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

    def _is_root_excluded(self, root: TraversalRoot, planner: ArchivePlanner) -> bool:
        if planner.is_excluded_from_sources(root.root_path):
            return True
        display_root = self._team_display_path(root, "/")
        return bool(display_root and (planner.is_user_excluded(display_root) or planner.is_archive_destination_path(display_root)))

    def _is_entry_excluded(self, entry: RemoteEntry, root: TraversalRoot, planner: ArchivePlanner) -> bool:
        if planner.is_excluded_from_sources(entry.full_path):
            return True
        display_path = self._team_display_path(root, entry.full_path)
        return bool(display_path and (planner.is_user_excluded(display_path) or planner.is_archive_destination_path(display_path)))

    def _is_entry_included(self, entry: RemoteEntry, root: TraversalRoot, include_roots: list[str]) -> bool:
        if not include_roots:
            return True
        candidate_paths = [entry.full_path]
        display_path = self._team_display_path(root, entry.full_path)
        if display_path:
            candidate_paths.append(display_path)
        return any(
            is_same_or_descendant(candidate_path, include_root)
            for candidate_path in candidate_paths
            for include_root in include_roots
        )

    def _team_display_path(self, root: TraversalRoot, path: str) -> str | None:
        if root.account_mode != "team_admin" or not root.namespace_id:
            return None
        if root.archive_bucket == "member_homes":
            return None
        if root.namespace_type == "team_space":
            return normalize_dropbox_path(path)
        if not root.namespace_name:
            return None
        return join_dropbox_path("/", root.namespace_name, path)

    def _normalized_include_roots(self, source_roots: list[str]) -> list[str]:
        include_roots: list[str] = []
        for source_root in source_roots:
            if not source_root or not source_root.strip():
                continue
            normalized = normalize_dropbox_path(source_root)
            if normalized == "/":
                return []
            if normalized not in include_roots:
                include_roots.append(normalized)
        return include_roots
