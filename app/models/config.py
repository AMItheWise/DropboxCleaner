from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


AccountMode = Literal["personal", "team_admin"]
AuthMethod = Literal["oauth_pkce", "refresh_token", "access_token"]
RunMode = Literal["inventory_only", "dry_run", "copy_run"]
ConflictPolicy = Literal["safe_skip", "abort_run"]
TeamCoveragePreset = Literal["all_team_content", "team_owned_only"]
DateFilterField = Literal["server_modified", "client_modified", "oldest_modified"]

DEFAULT_PERSONAL_SCOPES = (
    "account_info.read",
    "files.metadata.read",
    "files.content.read",
    "files.content.write",
)

DEFAULT_TEAM_SCOPES = (
    *DEFAULT_PERSONAL_SCOPES,
    "team_info.read",
    "members.read",
    "team_data.member",
    "sharing.read",
    "sharing.write",
    "files.team_metadata.read",
    "files.team_metadata.write",
    "team_data.team_space",
)


@dataclass(slots=True)
class RetrySettings:
    max_retries: int = 5
    initial_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0


@dataclass(slots=True)
class AuthConfig:
    method: AuthMethod
    account_mode: AccountMode = "personal"
    app_key: str | None = None
    app_secret: str | None = None
    refresh_token: str | None = None
    access_token: str | None = None
    scopes: tuple[str, ...] = DEFAULT_PERSONAL_SCOPES
    store_label: str = "default"
    admin_member_id: str | None = None


@dataclass(slots=True)
class JobConfig:
    source_roots: list[str]
    cutoff_date: str = "2020-05-01"
    date_filter_field: DateFilterField = "server_modified"
    archive_root: str = "/Archive_PreMay2020"
    output_dir: Path = Path("outputs")
    state_db_path: Path | None = None
    mode: RunMode = "dry_run"
    batch_size: int = 500
    retry: RetrySettings = field(default_factory=RetrySettings)
    conflict_policy: ConflictPolicy = "safe_skip"
    include_folders_in_inventory: bool = True
    exclude_archive_destination: bool = True
    worker_count: int = 1
    verify_after_run: bool = True
    start_fresh: bool = False
    team_coverage_preset: TeamCoveragePreset = "all_team_content"


@dataclass(slots=True)
class OutputPaths:
    base_output_dir: Path
    run_dir: Path
    state_db: Path
    inventory_csv: Path
    matched_csv: Path
    manifest_csv: Path
    summary_json: Path
    summary_text: Path
    verification_csv: Path
    verification_json: Path
    app_log: Path
    app_jsonl: Path
    config_snapshot_json: Path
    latest_pointer: Path

    @classmethod
    def create(cls, base_output_dir: Path, timestamp_slug: str, mode: RunMode) -> "OutputPaths":
        run_dir = base_output_dir / timestamp_slug
        manifest_name = "manifest_dry_run.csv" if mode == "dry_run" else "manifest_copy_run.csv"
        return cls(
            base_output_dir=base_output_dir,
            run_dir=run_dir,
            state_db=run_dir / "state.db",
            inventory_csv=run_dir / "inventory_full.csv",
            matched_csv=run_dir / "matched_pre_cutoff.csv",
            manifest_csv=run_dir / manifest_name,
            summary_json=run_dir / "summary.json",
            summary_text=run_dir / "summary.md",
            verification_csv=run_dir / "verification_report.csv",
            verification_json=run_dir / "verification_report.json",
            app_log=run_dir / "app.log",
            app_jsonl=run_dir / "app.jsonl",
            config_snapshot_json=run_dir / "config_snapshot.json",
            latest_pointer=base_output_dir / "latest_run.json",
        )


@dataclass(slots=True)
class RunContext:
    run_id: str
    created_at: str
    mode: RunMode
    output_paths: OutputPaths


@dataclass(slots=True)
class StoredCredentials:
    method: AuthMethod
    account_mode: AccountMode
    app_key: str | None
    refresh_token: str | None = None
    access_token: str | None = None
    scopes: tuple[str, ...] = DEFAULT_PERSONAL_SCOPES
    account_name: str | None = None
    account_email: str | None = None
    admin_member_id: str | None = None
