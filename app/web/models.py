from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from app.models.config import (
    AccountMode,
    ConflictPolicy,
    DateFilterField,
    RunMode,
    TeamArchiveLayout,
    TeamCoveragePreset,
)


class AuthStartRequest(BaseModel):
    account_mode: AccountMode = "personal"
    app_key: str | None = None


class AuthFinishRequest(BaseModel):
    auth_code: str
    admin_member_id: str | None = None


class AuthTestRequest(BaseModel):
    account_mode: AccountMode | None = None
    admin_member_id: str | None = None


class BrowserLocationPayload(BaseModel):
    display_path: str = "/"
    namespace_id: str | None = None
    namespace_path: str = "/"
    title: str = "Dropbox"
    view_mode: str = "default"


class FolderListRequest(BaseModel):
    account_mode: AccountMode = "personal"
    location: BrowserLocationPayload | None = None
    source_roots: list[str] = Field(default_factory=list)
    excluded_roots: list[str] = Field(default_factory=list)
    archive_root: str = "/Archive_PreMay2020"
    output_dir: str = "outputs"
    mode: RunMode = "dry_run"
    cutoff_date: str = "2020-05-01"
    date_filter_field: DateFilterField = "server_modified"
    team_coverage_preset: TeamCoveragePreset = "team_owned_only"
    team_archive_layout: TeamArchiveLayout = "segmented"


class RetryPayload(BaseModel):
    max_retries: int = 5
    initial_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0


class RunStartRequest(BaseModel):
    account_mode: AccountMode = "personal"
    source_roots: list[str] = Field(default_factory=list)
    excluded_roots: list[str] = Field(default_factory=list)
    cutoff_date: str = "2020-05-01"
    date_filter_field: DateFilterField = "server_modified"
    archive_root: str = "/Archive_PreMay2020"
    output_dir: str = "outputs"
    mode: RunMode = "dry_run"
    batch_size: int = 500
    retry: RetryPayload = Field(default_factory=RetryPayload)
    conflict_policy: ConflictPolicy = "safe_skip"
    include_folders_in_inventory: bool = True
    exclude_archive_destination: bool = True
    worker_count: int = 1
    verify_after_run: bool = True
    team_coverage_preset: TeamCoveragePreset = "team_owned_only"
    team_archive_layout: TeamArchiveLayout = "segmented"
    confirmed_copy_run: bool = False
    admin_member_id: str | None = None


class ResumeRunRequest(BaseModel):
    output_dir: str = "outputs"
    state_db_path: str | None = None
    account_mode: AccountMode | None = None
    admin_member_id: str | None = None


class AccountResponse(BaseModel):
    account_id: str
    display_name: str
    email: str | None = None
    account_mode: AccountMode
    team_member_id: str | None = None
    team_id: str | None = None
    team_name: str | None = None
    team_model: str | None = None
    active_member_count: int = 0
    namespace_count: int = 0


class AuthStatusResponse(BaseModel):
    saved_credentials_available: bool
    account_mode: AccountMode | None = None
    app_key: str | None = None
    admin_member_id: str | None = None
    packaged_app_key_available: bool = False


class FolderResponse(BaseModel):
    name: str
    display_path: str
    namespace_id: str | None = None
    namespace_path: str = "/"
    namespace_type: str = "personal"
    subtitle: str = ""


class RunStartResponse(BaseModel):
    run_id: str
    status: Literal["running"]


class EventResponse(BaseModel):
    seq: int
    type: str
    data: dict


class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    kind: str = "job"
    started_at: str | None = None
    completed_at: str | None = None
    mode: str | None = None
    actual_run_id: str | None = None
    run_dir: str | None = None
    summary_path: str | None = None
    verification_path: str | None = None
    error: str | None = None
    result: dict | None = None


class RunHistoryItem(BaseModel):
    run_id: str
    mode: str
    created_at: str
    run_dir: str
    latest: bool = False
    status_message: str
    metrics: list[dict]
    has_issues: bool


class RunHistoryResponse(BaseModel):
    output_dir: str
    latest_run_id: str | None = None
    runs: list[RunHistoryItem]


class OptionsResponse(BaseModel):
    accounts: list[dict]
    run_modes: list[dict]
    date_filters: list[dict]
    team_coverage: list[dict]
    team_archive_layouts: list[dict]
    defaults: dict
    packaged_app_key_available: bool


def expanded_path(value: str | Path) -> Path:
    return Path(value).expanduser()

