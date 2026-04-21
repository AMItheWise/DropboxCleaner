from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from app.models.config import AccountMode


ItemType = Literal["file", "folder"]
NamespaceTypeName = Literal["personal", "team_space", "team_folder", "shared_folder", "team_member_folder"]
ArchiveBucket = Literal["personal", "team_space", "member_homes", "shared_namespaces"]
CopyStatus = Literal[
    "planned",
    "copied",
    "skipped_existing_same",
    "skipped_existing_conflict",
    "failed",
    "excluded",
    "retried",
    "resumed",
    "blocked_precondition",
]


@dataclass(slots=True)
class AccountInfo:
    account_id: str
    display_name: str
    email: str | None = None
    account_mode: AccountMode = "personal"
    team_member_id: str | None = None
    team_id: str | None = None
    team_name: str | None = None
    team_model: str | None = None
    active_member_count: int = 0
    namespace_count: int = 0


@dataclass(slots=True)
class TraversalRoot:
    root_key: str
    root_path: str
    account_mode: AccountMode
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    archive_bucket: str = "personal"
    canonical_root: str = "/"
    include_mounted_folders: bool = True


@dataclass(slots=True)
class TeamDiscoveryResult:
    account_info: AccountInfo
    traversal_roots: list[TraversalRoot]
    team_model: str
    root_namespace_id: str | None
    archive_namespace_id: str | None = None
    archive_shared_folder_id: str | None = None
    archive_provisioned: bool = False
    archive_status_detail: str | None = None


@dataclass(slots=True)
class RemoteEntry:
    item_type: ItemType
    full_path: str
    path_lower: str
    filename: str
    parent_path: str
    dropbox_id: str
    size: int | None = None
    server_modified: str | None = None
    client_modified: str | None = None
    content_hash: str | None = None
    account_mode: AccountMode = "personal"
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    canonical_source_path: str | None = None
    canonical_parent_path: str | None = None
    archive_bucket: str = "personal"


@dataclass(slots=True)
class ListingPage:
    entries: list[RemoteEntry]
    cursor: str
    has_more: bool


@dataclass(slots=True)
class InventoryRecord:
    item_type: ItemType
    full_path: str
    path_lower: str
    filename: str
    parent_path: str
    dropbox_id: str
    size: int | None
    server_modified: str | None
    client_modified: str | None
    content_hash: str | None
    root_scope_used: str
    inventory_run_id: str
    inventory_timestamp: str
    account_mode: AccountMode = "personal"
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    canonical_source_path: str = "/"
    canonical_parent_path: str = "/"
    archive_bucket: str = "personal"

    def to_csv_row(self) -> dict[str, Any]:
        row = asdict(self)
        row.pop("path_lower", None)
        row.pop("canonical_parent_path", None)
        return row


@dataclass(slots=True)
class MatchedFileRecord:
    original_path: str
    path_lower: str
    filename: str
    dropbox_id: str
    size: int | None
    server_modified: str | None
    client_modified: str | None
    content_hash: str | None
    planned_archive_path: str
    archive_canonical_path: str | None
    match_reason: str
    filter_run_id: str
    filter_timestamp: str
    parent_path: str
    account_mode: AccountMode = "personal"
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    canonical_source_path: str = "/"
    canonical_parent_path: str = "/"
    archive_bucket: str = "personal"

    def to_csv_row(self) -> dict[str, Any]:
        row = asdict(self)
        row.pop("path_lower", None)
        row.pop("parent_path", None)
        row.pop("canonical_parent_path", None)
        return row


@dataclass(slots=True)
class CopyJobRecord:
    run_id: str
    mode: str
    original_path: str
    archive_path: str
    dropbox_id: str
    size: int | None
    server_modified: str | None
    client_modified: str | None
    content_hash: str | None
    status: CopyStatus
    status_detail: str
    attempt_count: int
    first_attempt_at: str | None
    last_attempt_at: str | None
    filename: str
    parent_path: str
    account_mode: AccountMode = "personal"
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    canonical_source_path: str = "/"
    archive_canonical_path: str | None = None
    canonical_parent_path: str = "/"
    archive_bucket: str = "personal"

    def to_csv_row(self) -> dict[str, Any]:
        row = asdict(self)
        row.pop("filename", None)
        row.pop("parent_path", None)
        row.pop("canonical_parent_path", None)
        return row


@dataclass(slots=True)
class VerificationRecord:
    original_path: str
    archive_path: str
    verification_status: str
    detail: str
    source_size: int | None
    archive_size: int | None
    source_content_hash: str | None
    archive_content_hash: str | None
    account_mode: AccountMode = "personal"
    namespace_id: str | None = None
    namespace_type: str = "personal"
    namespace_name: str | None = None
    member_id: str | None = None
    member_email: str | None = None
    member_display_name: str | None = None
    canonical_source_path: str | None = None
    archive_canonical_path: str | None = None
    archive_bucket: str = "personal"


@dataclass(slots=True)
class FolderSummary:
    folder_path: str
    file_count: int = 0
    total_size: int = 0
    matched_count: int = 0
    copied_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0


@dataclass(slots=True)
class SummaryReport:
    run_id: str
    mode: str
    phase: str
    created_at: str
    totals: dict[str, int]
    folder_breakdown: list[FolderSummary]
    conflicts_preview: list[str] = field(default_factory=list)
    failures_preview: list[str] = field(default_factory=list)
    blocked_preview: list[str] = field(default_factory=list)
    verification: dict[str, Any] = field(default_factory=dict)
