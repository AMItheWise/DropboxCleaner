from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from pathlib import PurePosixPath

from app.dropbox_client.errors import BlockedPreconditionError, DestinationConflictError, PathNotFoundError, TemporaryDropboxError
from app.models.records import AccountInfo, ListingPage, RemoteEntry, TeamDiscoveryResult, TraversalRoot
from app.utils.paths import normalize_dropbox_path, parent_path, split_namespace_relative_path, namespace_relative_path


def make_file(
    path: str,
    *,
    dropbox_id: str,
    size: int = 1,
    server_modified: str = "2019-01-01T00:00:00Z",
    client_modified: str = "2019-01-01T00:00:00Z",
    content_hash: str | None = None,
    account_mode: str = "personal",
    namespace_id: str | None = None,
    namespace_type: str = "personal",
    namespace_name: str | None = None,
    member_id: str | None = None,
    member_email: str | None = None,
    member_display_name: str | None = None,
    archive_bucket: str = "personal",
) -> RemoteEntry:
    normalized = normalize_dropbox_path(path)
    canonical = namespace_relative_path(namespace_id, normalized)
    return RemoteEntry(
        item_type="file",
        full_path=normalized,
        path_lower=normalized.lower(),
        filename=PurePosixPath(normalized).name,
        parent_path=parent_path(normalized),
        dropbox_id=dropbox_id,
        size=size,
        server_modified=server_modified,
        client_modified=client_modified,
        content_hash=content_hash,
        account_mode=account_mode,  # type: ignore[arg-type]
        namespace_id=namespace_id,
        namespace_type=namespace_type,
        namespace_name=namespace_name,
        member_id=member_id,
        member_email=member_email,
        member_display_name=member_display_name,
        canonical_source_path=canonical,
        canonical_parent_path=namespace_relative_path(namespace_id, parent_path(normalized)),
        archive_bucket=archive_bucket,
    )


def make_folder(
    path: str,
    *,
    dropbox_id: str,
    account_mode: str = "personal",
    namespace_id: str | None = None,
    namespace_type: str = "personal",
    namespace_name: str | None = None,
    member_id: str | None = None,
    member_email: str | None = None,
    member_display_name: str | None = None,
    archive_bucket: str = "personal",
) -> RemoteEntry:
    normalized = normalize_dropbox_path(path)
    canonical = namespace_relative_path(namespace_id, normalized)
    return RemoteEntry(
        item_type="folder",
        full_path=normalized,
        path_lower=normalized.lower(),
        filename=PurePosixPath(normalized).name if normalized != "/" else "",
        parent_path=parent_path(normalized),
        dropbox_id=dropbox_id,
        size=None,
        server_modified=None,
        client_modified=None,
        content_hash=None,
        account_mode=account_mode,  # type: ignore[arg-type]
        namespace_id=namespace_id,
        namespace_type=namespace_type,
        namespace_name=namespace_name,
        member_id=member_id,
        member_email=member_email,
        member_display_name=member_display_name,
        canonical_source_path=canonical,
        canonical_parent_path=namespace_relative_path(namespace_id, parent_path(normalized)),
        archive_bucket=archive_bucket,
    )


class FakeDropboxBackend:
    def __init__(
        self,
        entries: list[RemoteEntry],
        page_size: int = 2,
        *,
        account: AccountInfo | None = None,
        team_discovery: TeamDiscoveryResult | None = None,
    ) -> None:
        self.entries: dict[str, RemoteEntry] = {
            entry.canonical_source_path or namespace_relative_path(entry.namespace_id, entry.full_path): entry for entry in entries
        }
        self.page_size = page_size
        self.account = account or AccountInfo("dbid:fake", "Fake User", "fake@example.com")
        self.team_discovery_result = team_discovery
        self._cursor_pages: dict[str, list[RemoteEntry]] = {}
        self._cursor_positions: dict[str, int] = {}
        self.list_continue_calls = 0
        self.copy_calls: list[tuple[str, str]] = []
        self.operation_failures: dict[tuple[str, str, str], list[Exception]] = defaultdict(list)

    def queue_failure(self, operation: str, key_a: str, key_b: str, exc: Exception) -> None:
        self.operation_failures[(operation, key_a, key_b)].append(exc)

    def account_info(self) -> AccountInfo:
        return self.account

    def team_discovery(self, create_archive: bool, archive_root: str) -> TeamDiscoveryResult:
        if self.team_discovery_result is None:
            raise BlockedPreconditionError("Team discovery was not configured for this fake backend.")
        discovery = self.team_discovery_result
        if discovery.team_model == "team_space":
            archive_namespace_id = discovery.root_namespace_id
            archive_display = normalize_dropbox_path(archive_root)
            if create_archive and archive_namespace_id:
                self.create_folder_if_missing(namespace_relative_path(archive_namespace_id, archive_display))
            return replace(
                discovery,
                archive_namespace_id=archive_namespace_id,
                archive_provisioned=bool(archive_namespace_id),
                archive_status_detail="Using fake team-space archive.",
            )
        if discovery.archive_namespace_id:
            return replace(discovery, archive_provisioned=True, archive_status_detail="Using fake legacy archive namespace.")
        if not create_archive:
            return replace(discovery, archive_provisioned=False, archive_status_detail="Legacy archive not provisioned.")
        archive_name = normalize_dropbox_path(archive_root).strip("/")
        archive_namespace_id = f"ns-archive-{archive_name}"
        self.team_discovery_result = replace(
            discovery,
            archive_namespace_id=archive_namespace_id,
            archive_provisioned=True,
            archive_status_detail="Provisioned fake legacy archive namespace.",
        )
        self.create_folder_if_missing(namespace_relative_path(archive_namespace_id, "/"))
        return self.team_discovery_result

    def list_page(self, root_path: str, limit: int, namespace_id: str | None = None) -> ListingPage:
        normalized_root = normalize_dropbox_path(root_path)
        eligible = [
            entry
            for entry in sorted(self.entries.values(), key=lambda item: (item.namespace_id or "", item.full_path))
            if entry.namespace_id == namespace_id
            and entry.full_path != normalized_root
            and (
                normalized_root == "/"
                or entry.full_path.startswith(normalized_root.rstrip("/") + "/")
            )
        ]
        if namespace_id is None and normalized_root == "/":
            eligible = [
                entry
                for entry in sorted(self.entries.values(), key=lambda item: item.full_path)
                if entry.namespace_id is None and entry.full_path != "/"
            ]
        return self._page_from_entries(eligible, limit)

    def list_continue(self, cursor: str) -> ListingPage:
        self.list_continue_calls += 1
        entries = self._cursor_pages[cursor]
        position = self._cursor_positions[cursor]
        page_size = self.page_size
        page_entries = entries[position : position + page_size]
        next_position = position + page_size
        self._cursor_positions[cursor] = next_position
        return ListingPage(entries=page_entries, cursor=cursor, has_more=next_position < len(entries))

    def get_metadata(self, path: str) -> RemoteEntry | None:
        namespace_id, relative_path = split_namespace_relative_path(path)
        key = namespace_relative_path(namespace_id, relative_path)
        return self.entries.get(key)

    def create_folder_if_missing(self, path: str) -> RemoteEntry:
        namespace_id, relative_path = split_namespace_relative_path(path)
        key = namespace_relative_path(namespace_id, relative_path)
        entry = self.entries.get(key)
        if entry is not None and entry.item_type == "folder":
            return entry
        if entry is not None and entry.item_type != "folder":
            raise DestinationConflictError(f"{key} already exists as a file")
        folder = make_folder(
            relative_path,
            dropbox_id=f"id:{key}",
            account_mode="team_admin" if namespace_id else "personal",
            namespace_id=namespace_id,
            namespace_type="team_space" if namespace_id else "personal",
        )
        self.entries[key] = folder
        return folder

    def copy_file(self, source_path: str, destination_path: str) -> RemoteEntry:
        source_namespace_id, source_relative = split_namespace_relative_path(source_path)
        destination_namespace_id, destination_relative = split_namespace_relative_path(destination_path)
        source_key = namespace_relative_path(source_namespace_id, source_relative)
        destination_key = namespace_relative_path(destination_namespace_id, destination_relative)
        failure_key = ("copy_file", source_key, destination_key)
        if self.operation_failures[failure_key]:
            exc = self.operation_failures[failure_key].pop(0)
            raise exc
        if destination_key in self.entries:
            raise DestinationConflictError(f"{destination_key} already exists")
        source_entry = self.entries.get(source_key)
        if source_entry is None:
            raise PathNotFoundError(f"{source_key} not found")
        destination_parent = parent_path(destination_relative)
        destination_parent_key = namespace_relative_path(destination_namespace_id, destination_parent)
        if destination_parent != "/" and destination_parent_key not in self.entries:
            raise PathNotFoundError(f"Parent folder {destination_parent_key} not found")
        copied = replace(
            source_entry,
            full_path=destination_relative,
            path_lower=destination_relative.lower(),
            filename=PurePosixPath(destination_relative).name,
            parent_path=destination_parent,
            dropbox_id=f"copied:{source_entry.dropbox_id}:{destination_key}",
            namespace_id=destination_namespace_id,
            canonical_source_path=destination_key,
            canonical_parent_path=namespace_relative_path(destination_namespace_id, destination_parent),
        )
        self.entries[destination_key] = copied
        self.copy_calls.append((source_key, destination_key))
        return copied

    def _page_from_entries(self, entries: list[RemoteEntry], limit: int) -> ListingPage:
        page_size = min(limit, self.page_size)
        cursor = f"cursor-{len(self._cursor_pages) + 1}"
        self._cursor_pages[cursor] = entries
        self._cursor_positions[cursor] = page_size
        page_entries = entries[:page_size]
        return ListingPage(entries=page_entries, cursor=cursor, has_more=page_size < len(entries))


class FakeDropboxAdapter:
    def __init__(self, auth_config, logger, backend: FakeDropboxBackend) -> None:
        self.auth_config = auth_config
        self.logger = logger
        self.backend = backend

    def close(self) -> None:
        return None

    def get_current_account(self) -> AccountInfo:
        return self.backend.account_info()

    def get_team_discovery(self, job_config=None):
        if self.backend.team_discovery_result is None:
            raise BlockedPreconditionError("Team discovery not configured.")
        return self.backend.team_discovery_result

    def prepare_archive_destination(self, discovery: TeamDiscoveryResult, archive_root: str, create: bool) -> TeamDiscoveryResult:
        result = self.backend.team_discovery(create_archive=create, archive_root=archive_root)
        self.backend.team_discovery_result = result
        return result

    def list_folder(self, path: str, recursive: bool, limit: int, *, include_mounted_folders: bool = True, namespace_id: str | None = None) -> ListingPage:
        key = ("list_folder", namespace_relative_path(namespace_id, normalize_dropbox_path(path)), "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.list_page(normalize_dropbox_path(path), limit, namespace_id=namespace_id)

    def list_folder_continue(self, cursor: str) -> ListingPage:
        return self.backend.list_continue(cursor)

    def validate_file_listing_access(self) -> None:
        if self.auth_config.account_mode == "team_admin":
            self.get_team_discovery()
            return
        self.list_folder("/", recursive=False, limit=1)

    def get_metadata(self, path: str) -> RemoteEntry | None:
        key = ("get_metadata", path, "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.get_metadata(path)

    def create_folder_if_missing(self, path: str) -> RemoteEntry:
        key = ("create_folder_if_missing", path, "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.create_folder_if_missing(path)

    def copy_file(self, source_path: str, destination_path: str, member_id: str | None = None) -> RemoteEntry:
        return self.backend.copy_file(source_path, destination_path)


def fake_adapter_factory(backend: FakeDropboxBackend):
    def _factory(auth_config, logger):
        return FakeDropboxAdapter(auth_config, logger, backend)

    return _factory
