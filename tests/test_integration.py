from __future__ import annotations

import json
from pathlib import Path

from app.models.config import AuthConfig, JobConfig
from app.services.orchestrator import RunOrchestrator
from tests.fakes import FakeDropboxBackend, fake_adapter_factory, make_file, make_folder


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
