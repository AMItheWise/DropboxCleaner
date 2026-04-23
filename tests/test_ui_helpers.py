from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from app.dropbox_client.errors import TemporaryDropboxError
from app.models.config import AuthConfig, JobConfig
from app.models.records import AccountInfo, TeamDiscoveryResult, TraversalRoot
from app.ui.folder_browser import BrowserLocation, DropboxFolderBrowserService
from app.ui.options import (
    date_filter_label_to_value,
    run_label_to_value,
    team_archive_layout_label_to_value,
    team_coverage_label_to_value,
)
from app.ui.results import load_results_view_model
from tests.fakes import FakeDropboxAdapter, FakeDropboxBackend, make_file, make_folder


def make_logger() -> logging.Logger:
    logger = logging.getLogger("ui.helpers.test")
    logger.addHandler(logging.NullHandler())
    return logger


def test_qt_gui_entry_imports() -> None:
    from app.ui.main import main

    assert callable(main)


def test_qt_main_window_instantiates_with_guarded_continue() -> None:
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from app.ui.qt.main_window import DropboxCleanerMainWindow

    app = QApplication.instance() or QApplication([])
    window = DropboxCleanerMainWindow()

    assert app is not None
    assert not window.connection_screen.continue_button.isEnabled()
    window.settings_screen.set_account_mode("team_admin")
    assert not window.settings_screen.source_card.isHidden()

    window.close()


def test_friendly_choice_mappings() -> None:
    assert run_label_to_value("Inventory only") == "inventory_only"
    assert run_label_to_value("Preview archive") == "dry_run"
    assert run_label_to_value("Copy to archive") == "copy_run"
    assert date_filter_label_to_value("Original file date") == "client_modified"
    assert date_filter_label_to_value("Oldest available date") == "oldest_modified"
    assert team_coverage_label_to_value("Team-owned only") == "team_owned_only"
    assert team_archive_layout_label_to_value("Merge into one archive folder") == "merged"


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


def test_team_folder_browser_defaults_to_web_like_team_root() -> None:
    discovery = TeamDiscoveryResult(
        account_info=AccountInfo("dbid:admin", "Admin", account_mode="team_admin"),
        traversal_roots=[
            TraversalRoot(
                root_key="namespace::ns-root",
                root_path="/",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
                namespace_name="Example Team",
                archive_bucket="team_space",
                canonical_root="ns:ns-root",
            ),
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
                "/AMI Bad",
                dropbox_id="id:ami",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
            ),
            make_folder(
                "/Team Folder",
                dropbox_id="id:team-folder-mount",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
            ),
            make_folder(
                "/Team Folder/Archive",
                dropbox_id="id:web-archive",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
            ),
            make_folder(
                "/Archive",
                dropbox_id="id:raw-archive",
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
    children = service.list_folders(next(folder.location for folder in roots if folder.display_path == "/Team Folder"))
    advanced_roots = service.list_folders(service.advanced_team_root_location())

    assert [folder.display_path for folder in roots] == ["/AMI Bad", "/Team Folder"]
    assert [folder.display_path for folder in children] == ["/Team Folder/Archive"]
    assert [folder.display_path for folder in advanced_roots] == ["/", "/Team Folder"]


def test_folder_browser_surfaces_api_failure() -> None:
    backend = FakeDropboxBackend([make_folder("/Photos", dropbox_id="id:photos")])
    backend.queue_failure("list_folder", "/", "", TemporaryDropboxError("temporary"))
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger(), backend)
    service = DropboxFolderBrowserService(adapter, account_mode="personal")

    with pytest.raises(TemporaryDropboxError):
        service.list_folders(BrowserLocation(display_path="/"))
