from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from app.dropbox_client.errors import TemporaryDropboxError
from app.models.config import AuthConfig, JobConfig
from app.models.records import AccountInfo, TeamDiscoveryResult, TraversalRoot
from app.ui.folder_browser import BrowserLocation, DropboxFolderBrowserService
from app.ui.options import (
    date_filter_label_to_value,
    run_label_to_value,
    team_coverage_label_to_value,
)
from app.ui.results import load_results_view_model
from tests.fakes import FakeDropboxAdapter, FakeDropboxBackend, make_file, make_folder


def make_logger() -> logging.Logger:
    logger = logging.getLogger("ui.helpers.test")
    logger.addHandler(logging.NullHandler())
    return logger


def test_friendly_choice_mappings() -> None:
    assert run_label_to_value("Inventory only") == "inventory_only"
    assert run_label_to_value("Preview archive") == "dry_run"
    assert run_label_to_value("Copy to archive") == "copy_run"
    assert date_filter_label_to_value("Original file date") == "client_modified"
    assert date_filter_label_to_value("Oldest available date") == "oldest_modified"
    assert team_coverage_label_to_value("Team-owned only") == "team_owned_only"


def test_results_view_model_parses_success_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "mode": "copy_run",
                "created_at": "2026-04-21T00:00:00Z",
                "totals": {
                    "items_scanned": 10,
                    "files_matched": 4,
                    "files_copied": 3,
                    "files_skipped": 1,
                    "files_failed": 0,
                },
                "folder_breakdown": [
                    {
                        "folder_path": "/Photos",
                        "matched_count": 4,
                        "copied_count": 3,
                        "failed_count": 0,
                        "skipped_count": 1,
                        "total_size": 100,
                    }
                ],
                "conflicts_preview": [],
                "failures_preview": [],
                "blocked_preview": [],
                "verification": {"source_matched_file_count": 4, "archive_staged_file_count": 3},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "manifest_copy_run.csv").write_text("run_id\n", encoding="utf-8")

    result = load_results_view_model(run_dir)

    assert result.run_id == "run-1"
    assert result.metrics[0].label == "Scanned"
    assert result.top_folders[0].folder == "/Photos"
    assert not result.has_issues
    assert "3 file(s) were copied" in result.success_message
    assert [path.name for path in result.output_files] == ["manifest_copy_run.csv", "summary.json"]


def test_results_view_model_parses_issues_and_empty_run(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "mode": "dry_run",
                "created_at": "2026-04-21T00:00:00Z",
                "totals": {"items_scanned": 0, "files_matched": 0, "files_copied": 0, "files_skipped": 0, "files_failed": 1},
                "folder_breakdown": [],
                "conflicts_preview": ["conflict"],
                "failures_preview": ["failed"],
                "blocked_preview": ["blocked"],
                "verification": {},
            }
        ),
        encoding="utf-8",
    )

    result = load_results_view_model(run_dir)

    assert result.has_issues
    assert "need attention" in result.success_message
    assert result.conflicts == ["conflict"]
    assert result.failures == ["failed"]
    assert result.blocked == ["blocked"]


def test_personal_folder_browser_lists_nested_folders() -> None:
    backend = FakeDropboxBackend(
        [
            make_folder("/Photos", dropbox_id="id:photos"),
            make_folder("/Photos/Trips", dropbox_id="id:trips"),
            make_file("/Photos/pic.jpg", dropbox_id="id:pic"),
        ],
        page_size=1,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger(), backend)
    service = DropboxFolderBrowserService(adapter, account_mode="personal")

    root_folders = service.list_folders(service.root_location())
    child_folders = service.list_folders(root_folders[0].location)

    assert [folder.display_path for folder in root_folders] == ["/Photos"]
    assert [folder.display_path for folder in child_folders] == ["/Photos/Trips"]
    assert backend.list_continue_calls >= 1


def test_team_folder_browser_lists_team_roots_and_children() -> None:
    discovery = TeamDiscoveryResult(
        account_info=AccountInfo("dbid:admin", "Admin", account_mode="team_admin"),
        traversal_roots=[
            TraversalRoot(
                root_key="namespace::ns-team-folder",
                root_path="/",
                account_mode="team_admin",
                namespace_id="ns-team-folder",
                namespace_type="team_folder",
                namespace_name="Team Folder",
                archive_bucket="team_space",
                canonical_root="ns:ns-team-folder",
            ),
            TraversalRoot(
                root_key="namespace::ns-member",
                root_path="/",
                account_mode="team_admin",
                namespace_id="ns-member",
                namespace_type="team_member_folder",
                namespace_name="Member Home",
                archive_bucket="member_homes",
                canonical_root="ns:ns-member",
            ),
        ],
        team_model="team_space",
        root_namespace_id="ns-root",
    )
    backend = FakeDropboxBackend(
        [
            make_folder(
                "/Archive",
                dropbox_id="id:archive",
                account_mode="team_admin",
                namespace_id="ns-team-folder",
                namespace_type="team_folder",
            )
        ],
        account=discovery.account_info,
        team_discovery=discovery,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", account_mode="team_admin", access_token="token"), make_logger(), backend)
    service = DropboxFolderBrowserService(adapter, account_mode="team_admin", job_config=JobConfig(source_roots=["/"]))

    roots = service.list_folders(service.root_location())
    children = service.list_folders(roots[0].location)

    assert [folder.display_path for folder in roots] == ["/Team Folder"]
    assert [folder.display_path for folder in children] == ["/Team Folder/Archive"]


def test_folder_browser_surfaces_api_failure() -> None:
    backend = FakeDropboxBackend([make_folder("/Photos", dropbox_id="id:photos")])
    backend.queue_failure("list_folder", "/", "", TemporaryDropboxError("temporary"))
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger(), backend)
    service = DropboxFolderBrowserService(adapter, account_mode="personal")

    with pytest.raises(TemporaryDropboxError):
        service.list_folders(BrowserLocation(display_path="/"))
