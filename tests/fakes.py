from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from pathlib import PurePosixPath

from app.dropbox_client.errors import DestinationConflictError, PathNotFoundError, TemporaryDropboxError
from app.models.records import AccountInfo, ListingPage, RemoteEntry
from app.utils.paths import normalize_dropbox_path, parent_path


def make_file(
    path: str,
    *,
    dropbox_id: str,
    size: int = 1,
    server_modified: str = "2019-01-01T00:00:00Z",
    client_modified: str = "2019-01-01T00:00:00Z",
    content_hash: str | None = None,
) -> RemoteEntry:
    normalized = normalize_dropbox_path(path)
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
    )


def make_folder(path: str, *, dropbox_id: str) -> RemoteEntry:
    normalized = normalize_dropbox_path(path)
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
    )


class FakeDropboxBackend:
    def __init__(self, entries: list[RemoteEntry], page_size: int = 2) -> None:
        self.entries: dict[str, RemoteEntry] = {entry.full_path: entry for entry in entries}
        self.page_size = page_size
        self.account = AccountInfo("dbid:fake", "Fake User", "fake@example.com")
        self._cursor_pages: dict[str, list[RemoteEntry]] = {}
        self._cursor_positions: dict[str, int] = {}
        self.list_continue_calls = 0
        self.copy_calls: list[tuple[str, str]] = []
        self.operation_failures: dict[tuple[str, str, str], list[Exception]] = defaultdict(list)

    def queue_failure(self, operation: str, key_a: str, key_b: str, exc: Exception) -> None:
        self.operation_failures[(operation, key_a, key_b)].append(exc)

    def account_info(self) -> AccountInfo:
        return self.account

    def list_page(self, root_path: str, limit: int) -> ListingPage:
        eligible = [
            entry
            for entry in sorted(self.entries.values(), key=lambda item: item.full_path)
            if entry.full_path != root_path and entry.full_path.startswith(root_path.rstrip("/") + "/")
        ]
        if root_path == "/":
            eligible = [
                entry
                for entry in sorted(self.entries.values(), key=lambda item: item.full_path)
                if entry.full_path != "/"
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
        return self.entries.get(normalize_dropbox_path(path))

    def create_folder_if_missing(self, path: str) -> RemoteEntry:
        normalized = normalize_dropbox_path(path)
        entry = self.entries.get(normalized)
        if entry is not None and entry.item_type == "folder":
            return entry
        if entry is not None and entry.item_type != "folder":
            raise DestinationConflictError(f"{normalized} already exists as a file")
        folder = make_folder(normalized, dropbox_id=f"id:{normalized}")
        self.entries[normalized] = folder
        return folder

    def copy_file(self, source_path: str, destination_path: str) -> RemoteEntry:
        source = normalize_dropbox_path(source_path)
        destination = normalize_dropbox_path(destination_path)
        failure_key = ("copy_file", source, destination)
        if self.operation_failures[failure_key]:
            exc = self.operation_failures[failure_key].pop(0)
            raise exc
        if destination in self.entries:
            raise DestinationConflictError(f"{destination} already exists")
        source_entry = self.entries.get(source)
        if source_entry is None:
            raise PathNotFoundError(f"{source} not found")
        destination_parent = parent_path(destination)
        if destination_parent != "/" and destination_parent not in self.entries:
            raise PathNotFoundError(f"Parent folder {destination_parent} not found")
        copied = replace(
            source_entry,
            full_path=destination,
            path_lower=destination.lower(),
            filename=PurePosixPath(destination).name,
            parent_path=destination_parent,
            dropbox_id=f"copied:{source_entry.dropbox_id}:{destination}",
        )
        self.entries[destination] = copied
        self.copy_calls.append((source, destination))
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

    def list_folder(self, path: str, recursive: bool, limit: int) -> ListingPage:
        key = ("list_folder", normalize_dropbox_path(path), "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.list_page(normalize_dropbox_path(path), limit)

    def list_folder_continue(self, cursor: str) -> ListingPage:
        return self.backend.list_continue(cursor)

    def validate_file_listing_access(self) -> None:
        self.list_folder("/", recursive=False, limit=1)

    def get_metadata(self, path: str) -> RemoteEntry | None:
        key = ("get_metadata", normalize_dropbox_path(path), "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.get_metadata(path)

    def create_folder_if_missing(self, path: str) -> RemoteEntry:
        key = ("create_folder_if_missing", normalize_dropbox_path(path), "")
        if self.backend.operation_failures[key]:
            exc = self.backend.operation_failures[key].pop(0)
            raise exc
        return self.backend.create_folder_if_missing(path)

    def copy_file(self, source_path: str, destination_path: str) -> RemoteEntry:
        return self.backend.copy_file(source_path, destination_path)


def fake_adapter_factory(backend: FakeDropboxBackend):
    def _factory(auth_config, logger):
        return FakeDropboxAdapter(auth_config, logger, backend)

    return _factory
