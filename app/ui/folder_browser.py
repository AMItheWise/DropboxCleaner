from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.models.config import JobConfig
from app.models.records import ListingPage, TeamDiscoveryResult
from app.utils.paths import join_dropbox_path, normalize_dropbox_path


class FolderBrowsingAdapter(Protocol):
    def list_folder(
        self,
        path: str,
        recursive: bool,
        limit: int,
        *,
        include_mounted_folders: bool = True,
        namespace_id: str | None = None,
    ) -> ListingPage: ...

    def list_folder_continue(self, cursor: str, *, namespace_id: str | None = None) -> ListingPage: ...

    def get_team_discovery(self, job_config: JobConfig | None = None) -> TeamDiscoveryResult: ...


@dataclass(frozen=True, slots=True)
class BrowserLocation:
    display_path: str
    namespace_id: str | None = None
    namespace_path: str = "/"
    title: str = "Dropbox"


@dataclass(frozen=True, slots=True)
class BrowserFolder:
    name: str
    display_path: str
    namespace_id: str | None = None
    namespace_path: str = "/"
    namespace_type: str = "personal"
    subtitle: str = ""

    @property
    def location(self) -> BrowserLocation:
        return BrowserLocation(
            display_path=self.display_path,
            namespace_id=self.namespace_id,
            namespace_path=self.namespace_path,
            title=self.name,
        )


class DropboxFolderBrowserService:
    def __init__(
        self,
        adapter: FolderBrowsingAdapter,
        *,
        account_mode: str,
        job_config: JobConfig | None = None,
        page_size: int = 500,
    ) -> None:
        self._adapter = adapter
        self._account_mode = account_mode
        self._job_config = job_config
        self._page_size = page_size
        self._team_discovery: TeamDiscoveryResult | None = None

    def root_location(self) -> BrowserLocation:
        return BrowserLocation(display_path="/", title="Dropbox")

    def parent_location(self, location: BrowserLocation) -> BrowserLocation:
        if location.namespace_id is None or self._account_mode != "team_admin":
            parent = _parent_path(location.display_path)
            return BrowserLocation(display_path=parent, title="Dropbox" if parent == "/" else parent.rsplit("/", 1)[-1])
        if location.namespace_path == "/":
            return self.root_location()
        parent_namespace_path = _parent_path(location.namespace_path)
        display_parent = _parent_path(location.display_path)
        return BrowserLocation(
            display_path=display_parent,
            namespace_id=location.namespace_id,
            namespace_path=parent_namespace_path,
            title="Dropbox" if display_parent == "/" else display_parent.rsplit("/", 1)[-1],
        )

    def list_folders(self, location: BrowserLocation) -> list[BrowserFolder]:
        if self._account_mode == "team_admin" and location.namespace_id is None and location.display_path == "/":
            return self._team_roots()
        return self._list_folder_entries(location)

    def _team_roots(self) -> list[BrowserFolder]:
        discovery = self._get_team_discovery()
        folders: list[BrowserFolder] = []
        seen: set[tuple[str | None, str]] = set()
        for root in discovery.traversal_roots:
            if root.archive_bucket == "member_homes":
                continue
            if not root.namespace_id:
                continue
            name = root.namespace_name or root.namespace_id
            display_path = "/" if root.namespace_id == discovery.root_namespace_id else join_dropbox_path("/", name)
            key = (root.namespace_id, display_path.casefold())
            if key in seen:
                continue
            seen.add(key)
            folders.append(
                BrowserFolder(
                    name=name if display_path != "/" else f"{name} team space",
                    display_path=display_path,
                    namespace_id=root.namespace_id,
                    namespace_path="/",
                    namespace_type=root.namespace_type,
                    subtitle=_friendly_namespace_type(root.namespace_type),
                )
            )
        return sorted(folders, key=lambda folder: folder.name.casefold())

    def _list_folder_entries(self, location: BrowserLocation) -> list[BrowserFolder]:
        page = self._adapter.list_folder(
            location.namespace_path if location.namespace_id else location.display_path,
            recursive=False,
            limit=self._page_size,
            include_mounted_folders=True,
            namespace_id=location.namespace_id,
        )
        entries = list(page.entries)
        while page.has_more:
            page = self._adapter.list_folder_continue(page.cursor, namespace_id=location.namespace_id)
            entries.extend(page.entries)
        folders = []
        for entry in entries:
            if entry.item_type != "folder":
                continue
            expected_parent = location.namespace_path if location.namespace_id else location.display_path
            if normalize_dropbox_path(entry.parent_path) != normalize_dropbox_path(expected_parent):
                continue
            display_path = (
                join_dropbox_path(location.display_path, entry.filename)
                if location.namespace_id and location.display_path != "/"
                else entry.full_path
            )
            if location.namespace_id and location.display_path == "/" and entry.full_path != "/":
                display_path = entry.full_path
            folders.append(
                BrowserFolder(
                    name=entry.filename or entry.full_path,
                    display_path=normalize_dropbox_path(display_path),
                    namespace_id=location.namespace_id,
                    namespace_path=entry.full_path,
                    namespace_type=entry.namespace_type,
                    subtitle=_friendly_namespace_type(entry.namespace_type),
                )
            )
        return sorted(folders, key=lambda folder: folder.name.casefold())

    def _get_team_discovery(self) -> TeamDiscoveryResult:
        if self._team_discovery is None:
            self._team_discovery = self._adapter.get_team_discovery(self._job_config)
        return self._team_discovery


def _parent_path(path: str) -> str:
    normalized = normalize_dropbox_path(path)
    if normalized == "/":
        return "/"
    parent = normalized.rsplit("/", 1)[0]
    return parent or "/"


def _friendly_namespace_type(namespace_type: str) -> str:
    return {
        "personal": "Dropbox folder",
        "team_space": "Team space",
        "team_folder": "Team folder",
        "shared_folder": "Shared folder",
        "team_member_folder": "Member folder",
    }.get(namespace_type, "Dropbox folder")
