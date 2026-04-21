from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from app.models.config import AuthConfig, JobConfig
from app.models.records import AccountInfo, TeamDiscoveryResult, TraversalRoot
from app.services.orchestrator import RunOrchestrator
from tests.fakes import FakeDropboxBackend, fake_adapter_factory, make_file, make_folder


def make_team_discovery() -> TeamDiscoveryResult:
    account = AccountInfo(
        account_id="dbid:admin",
        display_name="Admin User",
        email="admin@example.com",
        account_mode="team_admin",
        team_member_id="dbmid:admin",
        team_id="tid:1",
        team_name="Acme",
        team_model="team_space",
        active_member_count=1,
        namespace_count=2,
    )
    traversal_roots = [
        TraversalRoot(
            root_key="namespace::ns-root",
            root_path="/",
            account_mode="team_admin",
            namespace_id="ns-root",
            namespace_type="team_space",
            namespace_name="Acme",
            archive_bucket="team_space",
            canonical_root="ns:ns-root",
            include_mounted_folders=False,
        ),
        TraversalRoot(
            root_key="namespace::ns-home-alice",
            root_path="/",
            account_mode="team_admin",
            namespace_id="ns-home-alice",
            namespace_type="team_member_folder",
            namespace_name="Alice Home",
            member_id="dbmid:alice",
            member_email="alice@example.com",
            member_display_name="Alice",
            archive_bucket="member_homes",
            canonical_root="ns:ns-home-alice",
            include_mounted_folders=False,
        ),
    ]
    return TeamDiscoveryResult(
        account_info=account,
        traversal_roots=traversal_roots,
        team_model="team_space",
        root_namespace_id="ns-root",
    )


def test_integration_inventory_dry_run_and_resumed_copy(tmp_path: Path) -> None:
    backend = FakeDropboxBackend(
        [
            make_folder("/Team", dropbox_id="id:team"),
            make_folder("/Team/Artists", dropbox_id="id:artists"),
            make_file("/Team/Artists/old-a.pdf", dropbox_id="id:old-a", size=10, content_hash="hash-old-a"),
            make_file("/Team/Artists/old-b.pdf", dropbox_id="id:old-b", size=11, content_hash="hash-old-b"),
            make_file(
                "/Team/Artists/new.pdf",
                dropbox_id="id:new",
                size=12,
                server_modified="2022-01-01T00:00:00Z",
                client_modified="2022-01-01T00:00:00Z",
                content_hash="hash-new",
            ),
        ],
        page_size=2,
    )
    auth_config = AuthConfig(method="access_token", access_token="token")
    orchestrator = RunOrchestrator(adapter_factory=fake_adapter_factory(backend))

    dry_run_result = orchestrator.run(
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="dry_run"),  # type: ignore[arg-type]
        auth_config=auth_config,
    )
    dry_run_dir = Path(dry_run_result.run_dir)
    assert (dry_run_dir / "inventory_full.csv").exists()
    assert (dry_run_dir / "matched_pre_cutoff.csv").exists()
    assert (dry_run_dir / "manifest_dry_run.csv").exists()
    manifest_text = (dry_run_dir / "manifest_dry_run.csv").read_text(encoding="utf-8")
    assert "planned" in manifest_text

    backend.queue_failure(
        "copy_file",
        "/Team/Artists/old-b.pdf",
        "/Archive_PreMay2020/Team/Artists/old-b.pdf",
        RuntimeError("simulate interruption"),
    )
    copy_orchestrator = RunOrchestrator(adapter_factory=fake_adapter_factory(backend))
    copy_orchestrator.run(
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        auth_config=auth_config,
    )

    latest_pointer = json.loads((tmp_path / "latest_run.json").read_text(encoding="utf-8"))
    resumed = RunOrchestrator(adapter_factory=fake_adapter_factory(backend)).resume(
        state_db_path=Path(latest_pointer["state_db"]),
        auth_config=auth_config,
    )
    resumed_dir = Path(resumed.run_dir)
    verification_summary = json.loads((resumed_dir / "verification_report.json").read_text(encoding="utf-8"))["summary"]
    assert verification_summary["archive_staged_file_count"] == 2
    resumed_manifest = (resumed_dir / "manifest_copy_run.csv").read_text(encoding="utf-8")
    assert "copied" in resumed_manifest


def test_integration_team_admin_dry_run_and_resumed_copy(tmp_path: Path) -> None:
    team_discovery = make_team_discovery()
    backend = FakeDropboxBackend(
        [
            make_file(
                "/root-plan.docx",
                dropbox_id="id:root",
                size=4,
                content_hash="hash-root",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
                namespace_name="Acme",
                archive_bucket="team_space",
            ),
            make_file(
                "/Projects/old.psd",
                dropbox_id="id:alice-old",
                size=10,
                content_hash="hash-alice-old",
                account_mode="team_admin",
                namespace_id="ns-home-alice",
                namespace_type="team_member_folder",
                namespace_name="Alice Home",
                member_id="dbmid:alice",
                member_email="alice@example.com",
                member_display_name="Alice",
                archive_bucket="member_homes",
            ),
            make_file(
                "/Projects/new.psd",
                dropbox_id="id:alice-new",
                size=11,
                server_modified="2022-01-01T00:00:00Z",
                client_modified="2022-01-01T00:00:00Z",
                content_hash="hash-alice-new",
                account_mode="team_admin",
                namespace_id="ns-home-alice",
                namespace_type="team_member_folder",
                namespace_name="Alice Home",
                member_id="dbmid:alice",
                member_email="alice@example.com",
                member_display_name="Alice",
                archive_bucket="member_homes",
            ),
        ],
        page_size=2,
        account=team_discovery.account_info,
        team_discovery=team_discovery,
    )
    auth_config = AuthConfig(method="access_token", account_mode="team_admin", access_token="token")
    orchestrator = RunOrchestrator(adapter_factory=fake_adapter_factory(backend))

    dry_run_result = orchestrator.run(
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="dry_run"),  # type: ignore[arg-type]
        auth_config=auth_config,
    )
    dry_run_dir = Path(dry_run_result.run_dir)
    manifest_text = (dry_run_dir / "manifest_dry_run.csv").read_text(encoding="utf-8")
    assert "member_homes" in manifest_text
    assert "ns:ns-home-alice/Projects/old.psd" in manifest_text

    backend.queue_failure(
        "copy_file",
        "ns:ns-home-alice/Projects/old.psd",
        "ns:ns-root/Archive_PreMay2020/member_homes/alice-example.com/Projects/old.psd",
        RuntimeError("simulate interruption"),
    )
    copy_orchestrator = RunOrchestrator(adapter_factory=fake_adapter_factory(backend))
    copy_orchestrator.run(
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        auth_config=auth_config,
    )

    latest_pointer = json.loads((tmp_path / "latest_run.json").read_text(encoding="utf-8"))
    backend.team_discovery_result = replace(backend.team_discovery_result, archive_namespace_id="ns-root", archive_provisioned=True)
    resumed = RunOrchestrator(adapter_factory=fake_adapter_factory(backend)).resume(
        state_db_path=Path(latest_pointer["state_db"]),
        auth_config=auth_config,
    )
    resumed_dir = Path(resumed.run_dir)
    verification_summary = json.loads((resumed_dir / "verification_report.json").read_text(encoding="utf-8"))["summary"]
    assert verification_summary["archive_staged_file_count"] == 2
    resumed_manifest = (resumed_dir / "manifest_copy_run.csv").read_text(encoding="utf-8")
    assert "ns:ns-root/Archive_PreMay2020/member_homes/alice-example.com/Projects/old.psd" in resumed_manifest
