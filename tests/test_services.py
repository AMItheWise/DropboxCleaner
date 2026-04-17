from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from dropbox import exceptions as dbx_exceptions

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.auth import AuthManager
from app.dropbox_client.errors import ConflictPolicyAbortError, TemporaryDropboxError
from app.models.config import AuthConfig, JobConfig, OutputPaths, RetrySettings, RunContext
from app.persistence.repository import RunStateRepository
from app.reports.writers import ReportWriter
from app.services.copying import ArchiveCopyService
from app.services.filtering import FilterService
from app.services.inventory import DropboxInventoryService
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken
from app.services.verification import VerificationService
from app.utils.time import isoformat_utc, timestamp_slug, utc_now
from tests.fakes import FakeDropboxAdapter, FakeDropboxBackend, fake_adapter_factory, make_file, make_folder


def make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    return logger


def make_run_context(tmp_path: Path, mode: str = "dry_run") -> tuple[RunContext, RunStateRepository]:
    output_paths = OutputPaths.create(tmp_path, timestamp_slug(utc_now()), mode)  # type: ignore[arg-type]
    output_paths.run_dir.mkdir(parents=True, exist_ok=True)
    run_context = RunContext("run-1", isoformat_utc(utc_now()) or "", mode, output_paths)
    repository = RunStateRepository(output_paths.state_db)
    repository.create_run(
        run_context,
        JobConfig(source_roots=["/"], output_dir=tmp_path, mode=mode),  # type: ignore[arg-type]
        AuthConfig(method="access_token", access_token="token"),
    )
    return run_context, repository


def seed_inventory(repository: RunStateRepository, run_context: RunContext, rows: list[dict]) -> None:
    from app.models.records import InventoryRecord

    repository.upsert_inventory_records(
        [
            InventoryRecord(
                item_type=row["item_type"],
                full_path=row["full_path"],
                path_lower=row["full_path"].lower(),
                filename=Path(row["full_path"]).name,
                parent_path=str(Path(row["full_path"]).parent).replace("\\", "/") or "/",
                dropbox_id=row["dropbox_id"],
                size=row.get("size"),
                server_modified=row.get("server_modified"),
                client_modified=row.get("client_modified"),
                content_hash=row.get("content_hash"),
                root_scope_used="/",
                inventory_run_id=run_context.run_id,
                inventory_timestamp=run_context.created_at,
            )
            for row in rows
        ]
    )


def test_inventory_pagination(tmp_path: Path) -> None:
    backend = FakeDropboxBackend(
        [
            make_folder("/Docs", dropbox_id="id:/Docs"),
            make_file("/Docs/a.txt", dropbox_id="id:a"),
            make_file("/Docs/b.txt", dropbox_id="id:b"),
            make_file("/Docs/c.txt", dropbox_id="id:c"),
            make_file("/Docs/d.txt", dropbox_id="id:d"),
        ],
        page_size=2,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("pagination"), backend)
    run_context, repository = make_run_context(tmp_path, "inventory_only")
    service = DropboxInventoryService(repository, make_logger("inventory"))
    service.run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="inventory_only"),  # type: ignore[arg-type]
        source_roots=["/"],
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )

    rows = list(repository.iter_inventory_records(run_context.run_id))
    assert len(rows) == 5
    assert backend.list_continue_calls >= 2


def test_filter_by_cutoff_date(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "dry_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Old/report.pdf",
                "dropbox_id": "id:old",
                "size": 10,
                "server_modified": "2019-12-31T23:59:59Z",
                "client_modified": "2019-12-31T23:59:59Z",
                "content_hash": "hash-old",
            },
            {
                "item_type": "file",
                "full_path": "/New/report.pdf",
                "dropbox_id": "id:new",
                "size": 11,
                "server_modified": "2021-01-01T00:00:00Z",
                "client_modified": "2021-01-01T00:00:00Z",
                "content_hash": "hash-new",
            },
        ],
    )
    matched = FilterService(repository, make_logger("filter")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="dry_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    rows = list(repository.iter_matched_files(run_context.run_id))
    assert matched == 1
    assert rows[0]["planned_archive_path"] == "/Archive_PreMay2020/Old/report.pdf"


def test_archive_path_mapping() -> None:
    planner = ArchivePlanner("/Archive_PreMay2020")
    assert planner.map_to_archive_path("/Team/Artists/file.pdf") == "/Archive_PreMay2020/Team/Artists/file.pdf"


def test_archive_destination_exclusion(tmp_path: Path) -> None:
    backend = FakeDropboxBackend(
        [
            make_folder("/Archive_PreMay2020", dropbox_id="id:archive"),
            make_file("/Archive_PreMay2020/already/file.txt", dropbox_id="id:archived"),
            make_file("/Keep/file.txt", dropbox_id="id:keep"),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("exclude"), backend)
    run_context, repository = make_run_context(tmp_path, "inventory_only")
    DropboxInventoryService(repository, make_logger("inventory.exclude")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="inventory_only"),  # type: ignore[arg-type]
        source_roots=["/"],
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    rows = list(repository.iter_inventory_records(run_context.run_id))
    assert [row["full_path"] for row in rows] == ["/Keep/file.txt"]


def test_manifest_statuses_for_existing_same_and_conflict(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Src/same.txt",
                "dropbox_id": "id:same",
                "size": 10,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-same",
            },
            {
                "item_type": "file",
                "full_path": "/Src/conflict.txt",
                "dropbox_id": "id:conflict",
                "size": 20,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-conflict-source",
            },
        ],
    )
    FilterService(repository, make_logger("filter.manifest")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_folder("/Archive_PreMay2020", dropbox_id="id:archive"),
            make_folder("/Archive_PreMay2020/Src", dropbox_id="id:archive-src"),
            make_file(
                "/Archive_PreMay2020/Src/same.txt",
                dropbox_id="id:existing-same",
                size=10,
                server_modified="2019-01-01T00:00:00Z",
                client_modified="2019-01-01T00:00:00Z",
                content_hash="hash-same",
            ),
            make_file(
                "/Archive_PreMay2020/Src/conflict.txt",
                dropbox_id="id:existing-conflict",
                size=22,
                server_modified="2019-01-01T00:00:00Z",
                client_modified="2019-01-01T00:00:00Z",
                content_hash="hash-other",
            ),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("copy.manifest"), backend)
    ArchiveCopyService(repository, make_logger("copy.manifest")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )
    manifest = {row.original_path: row.status for row in repository.manifest_rows(run_context.run_id)}
    assert manifest["/Src/same.txt"] == "skipped_existing_same"
    assert manifest["/Src/conflict.txt"] == "skipped_existing_conflict"


def test_resume_logic_skips_successful_copy(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Src/a.txt",
                "dropbox_id": "id:a",
                "size": 1,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-a",
            },
            {
                "item_type": "file",
                "full_path": "/Src/b.txt",
                "dropbox_id": "id:b",
                "size": 1,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-b",
            },
        ],
    )
    FilterService(repository, make_logger("filter.resume")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_folder("/Archive_PreMay2020", dropbox_id="id:archive"),
            make_folder("/Archive_PreMay2020/Src", dropbox_id="id:archive-src"),
            make_file("/Src/a.txt", dropbox_id="id:a", content_hash="hash-a"),
            make_file("/Src/b.txt", dropbox_id="id:b", content_hash="hash-b"),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("copy.resume"), backend)
    service = ArchiveCopyService(repository, make_logger("copy.resume"))
    service.run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )
    first_copy_count = len(backend.copy_calls)
    service.run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )
    assert len(backend.copy_calls) == first_copy_count


def test_duplicate_conflict_abort_policy(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Src/conflict.txt",
                "dropbox_id": "id:conflict",
                "size": 20,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-conflict-source",
            }
        ],
    )
    FilterService(repository, make_logger("filter.abort")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_folder("/Archive_PreMay2020", dropbox_id="id:archive"),
            make_folder("/Archive_PreMay2020/Src", dropbox_id="id:archive-src"),
            make_file(
                "/Archive_PreMay2020/Src/conflict.txt",
                dropbox_id="id:existing-conflict",
                size=22,
                content_hash="hash-other",
            ),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("copy.abort"), backend)
    with pytest.raises(ConflictPolicyAbortError):
        ArchiveCopyService(repository, make_logger("copy.abort")).run(
            adapter=adapter,
            run_context=run_context,
            job_config=JobConfig(
                source_roots=["/"],
                output_dir=tmp_path,
                mode="copy_run",
                conflict_policy="abort_run",
            ),  # type: ignore[arg-type]
            planner=ArchivePlanner("/Archive_PreMay2020"),
            emit=None,
            cancellation_token=CancellationToken(),
            dry_run=False,
        )


def test_retry_logic(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Src/retry.txt",
                "dropbox_id": "id:retry",
                "size": 5,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-retry",
            }
        ],
    )
    FilterService(repository, make_logger("filter.retry")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_file("/Src/retry.txt", dropbox_id="id:retry", content_hash="hash-retry"),
        ],
        page_size=10,
    )
    backend.queue_failure(
        "copy_file",
        "/Src/retry.txt",
        "/Archive_PreMay2020/Src/retry.txt",
        TemporaryDropboxError("temporary"),
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("copy.retry"), backend)
    ArchiveCopyService(repository, make_logger("copy.retry")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(
            source_roots=["/"],
            output_dir=tmp_path,
            mode="copy_run",
            retry=RetrySettings(max_retries=2, initial_backoff_seconds=0, backoff_multiplier=1, max_backoff_seconds=0),
        ),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )
    manifest = list(repository.manifest_rows(run_context.run_id))
    assert manifest[0].status == "copied"
    assert backend.copy_calls == [("/Src/retry.txt", "/Archive_PreMay2020/Src/retry.txt")]


def test_verification_report(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Src/verified.txt",
                "dropbox_id": "id:verified",
                "size": 5,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-verified",
            },
            {
                "item_type": "file",
                "full_path": "/Src/missing.txt",
                "dropbox_id": "id:missing",
                "size": 5,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-missing",
            },
            {
                "item_type": "file",
                "full_path": "/Src/conflict.txt",
                "dropbox_id": "id:conflict",
                "size": 5,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-conflict",
            },
        ],
    )
    FilterService(repository, make_logger("filter.verify")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_file("/Archive_PreMay2020/Src/verified.txt", dropbox_id="id:a", content_hash="hash-verified", size=5),
            make_file("/Archive_PreMay2020/Src/conflict.txt", dropbox_id="id:b", content_hash="other", size=7),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("verify"), backend)
    rows = VerificationService(repository, make_logger("verify")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        emit=None,
        cancellation_token=CancellationToken(),
    )
    writer = ReportWriter(repository)
    summary = writer.write_verification_outputs(rows, run_context.output_paths.verification_csv, run_context.output_paths.verification_json)
    assert summary["archive_staged_file_count"] == 1
    assert len(summary["missing_archive_targets"]) == 1
    assert len(summary["conflicts"]) == 1


def test_bad_input_missing_scope_maps_to_missing_scope_error() -> None:
    adapter = DropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("adapter.map"))
    try:
        exc = dbx_exceptions.BadInputError(
            "req-1",
            'Error in call to API function "files/list_folder": Your app is not permitted to access this endpoint because it does not have the required scope \'files.metadata.read\'.',
        )
        mapped = adapter._map_exception(exc)
        assert mapped.__class__.__name__ == "MissingScopeError"
        assert getattr(mapped, "required_scope") == "files.metadata.read"
    finally:
        adapter.close()


def test_test_connection_validates_listing_scope() -> None:
    backend = FakeDropboxBackend([make_folder("/Docs", dropbox_id="id:/Docs")], page_size=10)
    backend.queue_failure(
        "list_folder",
        "/",
        "",
        dbx_exceptions.BadInputError(
            "req-2",
            'Error in call to API function "files/list_folder": Your app is not permitted to access this endpoint because it does not have the required scope \'files.metadata.read\'.',
        ),
    )
    auth_manager = AuthManager(adapter_factory=fake_adapter_factory(backend))
    with pytest.raises(Exception) as exc_info:
        auth_manager.test_connection(AuthConfig(method="access_token", access_token="token"), make_logger("auth.scope"))
    assert "files.metadata.read" in str(exc_info.value)
