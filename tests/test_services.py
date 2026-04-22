from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

import pytest
from dropbox import files
from dropbox import exceptions as dbx_exceptions

from app.dropbox_client.adapter import DropboxAdapter, path_root_for_namespace
from app.dropbox_client.auth import AuthManager
from app.dropbox_client.errors import BlockedPreconditionError, ConflictPolicyAbortError, TemporaryDropboxError
from app.models.config import AuthConfig, JobConfig, OutputPaths, RetrySettings, RunContext
from app.models.records import AccountInfo, InventoryRecord, TeamDiscoveryResult, TraversalRoot
from app.persistence.repository import RunStateRepository
from app.reports.writers import ReportWriter
from app.services.copying import ArchiveCopyService
from app.services.filtering import FilterService
from app.services.inventory import DropboxInventoryService
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationToken
from app.services.verification import VerificationService
from app.utils.paths import namespace_relative_path
from app.utils.time import isoformat_utc, timestamp_slug, utc_now
from tests.fakes import FakeDropboxAdapter, FakeDropboxBackend, fake_adapter_factory, make_file, make_folder


def make_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    return logger


def make_run_context(tmp_path: Path, mode: str = "dry_run", account_mode: str = "personal") -> tuple[RunContext, RunStateRepository]:
    output_paths = OutputPaths.create(tmp_path, timestamp_slug(utc_now()), mode)  # type: ignore[arg-type]
    output_paths.run_dir.mkdir(parents=True, exist_ok=True)
    run_context = RunContext("run-1", isoformat_utc(utc_now()) or "", mode, output_paths)
    repository = RunStateRepository(output_paths.state_db)
    repository.create_run(
        run_context,
        JobConfig(source_roots=["/"], output_dir=tmp_path, mode=mode),  # type: ignore[arg-type]
        AuthConfig(method="access_token", account_mode=account_mode, access_token="token"),
    )
    return run_context, repository


def seed_inventory(repository: RunStateRepository, run_context: RunContext, rows: list[dict]) -> None:
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
                root_scope_used=row.get("root_scope_used", "/"),
                inventory_run_id=run_context.run_id,
                inventory_timestamp=run_context.created_at,
                account_mode=row.get("account_mode", "personal"),
                namespace_id=row.get("namespace_id"),
                namespace_type=row.get("namespace_type", "personal"),
                namespace_name=row.get("namespace_name"),
                member_id=row.get("member_id"),
                member_email=row.get("member_email"),
                member_display_name=row.get("member_display_name"),
                canonical_source_path=row.get("canonical_source_path", row["full_path"]),
                canonical_parent_path=row.get("canonical_parent_path", str(Path(row["full_path"]).parent).replace("\\", "/") or "/"),
                archive_bucket=row.get("archive_bucket", "personal"),
            )
            for row in rows
        ]
    )


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
    assert rows[0]["match_reason"] == "server_modified_before_2020-05-01"


def test_filter_by_client_modified_date(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "dry_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Screenshots/Screenshot 2015.png",
                "dropbox_id": "id:screenshot",
                "size": 100,
                "server_modified": "2026-04-20T14:51:21Z",
                "client_modified": "2015-01-04T09:13:35Z",
                "content_hash": "hash-screenshot",
            },
        ],
    )

    matched = FilterService(repository, make_logger("filter.client_modified")).run(
        run_context=run_context,
        job_config=JobConfig(
            source_roots=["/"],
            output_dir=tmp_path,
            mode="dry_run",
            cutoff_date="2020-05-01",
            date_filter_field="client_modified",
        ),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )

    rows = list(repository.iter_matched_files(run_context.run_id))
    assert matched == 1
    assert rows[0]["original_path"] == "/Screenshots/Screenshot 2015.png"
    assert rows[0]["match_reason"] == "client_modified_before_2020-05-01"


def test_filter_by_oldest_modified_date(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "dry_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Imported/old-camera-file.png",
                "dropbox_id": "id:imported",
                "size": 100,
                "server_modified": "2026-04-20T14:51:21Z",
                "client_modified": "2015-01-04T09:13:35Z",
                "content_hash": "hash-imported",
            },
            {
                "item_type": "file",
                "full_path": "/Imported/new-file.png",
                "dropbox_id": "id:new",
                "size": 101,
                "server_modified": "2026-04-20T14:51:21Z",
                "client_modified": "2026-04-20T14:51:21Z",
                "content_hash": "hash-new",
            },
        ],
    )

    matched = FilterService(repository, make_logger("filter.oldest_modified")).run(
        run_context=run_context,
        job_config=JobConfig(
            source_roots=["/"],
            output_dir=tmp_path,
            mode="dry_run",
            cutoff_date="2020-05-01",
            date_filter_field="oldest_modified",
        ),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )

    rows = list(repository.iter_matched_files(run_context.run_id))
    assert matched == 1
    assert rows[0]["original_path"] == "/Imported/old-camera-file.png"
    assert rows[0]["match_reason"] == "oldest_modified_before_2020-05-01"


def test_archive_path_mapping() -> None:
    planner = ArchivePlanner("/Archive_PreMay2020")
    assert planner.map_to_archive_path("/Team/Artists/file.pdf") == "/Archive_PreMay2020/Team/Artists/file.pdf"


def test_team_archive_path_mapping() -> None:
    planner = ArchivePlanner("/Archive_PreMay2020", account_mode="team_admin").with_team_discovery(make_team_discovery())
    assert (
        planner.map_to_archive_path(
            "/Designs/file.pdf",
            archive_bucket="member_homes",
            member_email="alice@example.com",
            member_id="dbmid:alice",
        )
        == "/Archive_PreMay2020/member_homes/alice-example.com/Designs/file.pdf"
    )


def test_team_path_root_uses_arbitrary_namespace_for_non_root_namespaces() -> None:
    root_path_root = path_root_for_namespace("ns-root", "ns-root")
    child_path_root = path_root_for_namespace("ns-shared", "ns-root")

    assert root_path_root.is_root()
    assert root_path_root.get_root() == "ns-root"
    assert child_path_root.is_namespace_id()
    assert child_path_root.get_namespace_id() == "ns-shared"


def test_team_archive_location_resolves_mounted_namespace() -> None:
    discovery = replace(
        make_team_discovery(),
        traversal_roots=[
            *make_team_discovery().traversal_roots,
            TraversalRoot(
                root_key="namespace::ns-screenshots",
                root_path="/",
                account_mode="team_admin",
                namespace_id="ns-screenshots",
                namespace_type="shared_folder",
                namespace_name="Screenshots",
                archive_bucket="shared_namespaces",
                canonical_root="ns:ns-screenshots",
            ),
        ],
    )
    adapter = DropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("adapter.archive_location"),
    )
    try:
        namespace_id, relative_path, label = adapter._team_space_archive_location(discovery, "/Screenshots/Archive")
    finally:
        adapter.close()

    assert namespace_id == "ns-screenshots"
    assert relative_path == "/Archive"
    assert label == "mounted namespace Screenshots"


def test_team_archive_canonical_path_uses_archive_namespace_root_path() -> None:
    discovery = replace(
        make_team_discovery(),
        archive_namespace_id="ns-screenshots",
        archive_namespace_root_path="/Archive",
        archive_provisioned=True,
    )
    planner = ArchivePlanner("/Screenshots/Archive", account_mode="team_admin").with_team_discovery(discovery)

    assert (
        planner.build_archive_canonical_path(
            "/Screenshots/Archive/member_homes/amithewise-gmail.com/5.docx",
            archive_bucket="member_homes",
            namespace_id="ns-home",
        )
        == "ns:ns-screenshots/Archive/member_homes/amithewise-gmail.com/5.docx"
    )


def test_team_archive_sharing_switches_to_shared_folder_namespace() -> None:
    calls: dict[str, list] = {"share": [], "add": [], "mount": []}

    class ShareMetadata:
        shared_folder_id = "sf:archive"

    class ShareLaunch:
        def is_complete(self) -> bool:
            return True

        def is_async_job_id(self) -> bool:
            return False

        def get_complete(self) -> ShareMetadata:
            return ShareMetadata()

    class ShareClient:
        def __init__(self, label: str) -> None:
            self.label = label

        def sharing_share_folder(self, path: str, force_async: bool = False) -> ShareLaunch:
            calls["share"].append((path, force_async))
            return ShareLaunch()

        def sharing_add_folder_member(self, shared_folder_id: str, members: list, quiet: bool = False) -> None:
            calls["add"].append((shared_folder_id, members[0].member.get_email(), members[0].access_level.is_editor(), quiet))

        def sharing_mount_folder(self, shared_folder_id: str) -> None:
            calls["mount"].append((self.label, shared_folder_id))

    admin_client = ShareClient("admin")
    member_client = ShareClient("member")
    adapter = DropboxAdapter.__new__(DropboxAdapter)
    adapter._logger = make_logger("adapter.share_archive")
    adapter.get_metadata = lambda path: make_folder("/Archive", dropbox_id="id:archive", namespace_id="ns-team-folder")
    adapter._metadata_client_and_target = lambda path: (admin_client, "/Archive", "ns-team-folder")
    adapter._copy_client = lambda *, admin, member_id=None: admin_client if admin else member_client

    result = adapter._finalize_team_space_archive_destination(
        make_team_discovery(),
        archive_root="/Amithewise Team Folder/Archive",
        archive_path="ns:ns-team-folder/Archive",
        archive_namespace_id="ns-team-folder",
        archive_relative_path="/Archive",
        archive_location_label="mounted namespace Amithewise Team Folder",
        create=True,
        reused=True,
    )

    assert result.archive_namespace_id == "sf:archive"
    assert result.archive_namespace_root_path == "/"
    assert result.archive_shared_folder_id == "sf:archive"
    assert calls["share"] == [("/Archive", False)]
    assert calls["add"] == [("sf:archive", "alice@example.com", True, True)]
    assert calls["mount"] == [("member", "sf:archive")]


def test_metadata_client_uses_ns_path_for_namespace_root() -> None:
    adapter = DropboxAdapter.__new__(DropboxAdapter)
    adapter._auth_config = AuthConfig(method="access_token", account_mode="team_admin", access_token="token")
    class MetadataClient:
        def with_path_root(self, path_root):
            return self

    metadata_client = MetadataClient()
    adapter._metadata_client = lambda path: metadata_client
    adapter.get_team_discovery = lambda: make_team_discovery()

    client, target, namespace_id = adapter._metadata_client_and_target("ns:ns-root")

    assert client is metadata_client
    assert target == "ns:ns-root"
    assert namespace_id == "ns-root"


def test_no_write_permission_maps_to_blocked_precondition() -> None:
    adapter = DropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("adapter.no_write"))
    try:
        mapped = adapter._map_exception(RuntimeError("WriteError('no_write_permission', None)"))
        assert isinstance(mapped, BlockedPreconditionError)
        assert "editor access" in str(mapped)
    finally:
        adapter.close()


def test_team_copy_retries_member_context_after_admin_write_denied() -> None:
    calls: list[tuple[str, str, str, bool]] = []

    class CopyResult:
        def __init__(self) -> None:
            self.metadata = files.FileMetadata(
                name="5.docx",
                id="id:copied",
                client_modified=utc_now(),
                server_modified=utc_now(),
                rev="123456789",
                size=4,
                path_lower="/archive/member_homes/user/5.docx",
                path_display="/Archive/member_homes/user/5.docx",
            )

    class CopyClient:
        def __init__(self, label: str, exc: Exception | None = None) -> None:
            self.label = label
            self.exc = exc

        def files_copy_v2(self, source: str, destination: str, autorename: bool = False) -> CopyResult:
            calls.append((self.label, source, destination, autorename))
            if self.exc is not None:
                raise self.exc
            return CopyResult()

    adapter = DropboxAdapter.__new__(DropboxAdapter)
    adapter._auth_config = AuthConfig(method="access_token", account_mode="team_admin", access_token="token")
    adapter._logger = make_logger("adapter.copy_fallback")
    admin_client = CopyClient("admin", RuntimeError("RelocationError('to', WriteError('no_write_permission', None))"))
    member_client = CopyClient("member")
    adapter._copy_client = lambda *, admin, member_id=None: admin_client if admin else member_client

    entry = adapter.copy_file(
        "ns:member-home/5.docx",
        "ns:team-archive/Archive/5.docx",
        member_id="dbmid:user",
        source_display_path="/5.docx",
        destination_display_path="/Archive/5.docx",
    )

    assert entry.dropbox_id == "id:copied"
    assert entry.namespace_id == "team-archive"
    assert calls == [
        ("admin", "ns:member-home/5.docx", "ns:team-archive/Archive/5.docx", False),
        ("member", "/5.docx", "/Archive/5.docx", False),
    ]


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


def test_user_excluded_folder_is_not_inventoried(tmp_path: Path) -> None:
    backend = FakeDropboxBackend(
        [
            make_folder("/SkipMe", dropbox_id="id:skip-folder"),
            make_file("/SkipMe/old.txt", dropbox_id="id:skip-old"),
            make_file("/Keep/old.txt", dropbox_id="id:keep-old"),
        ],
        page_size=10,
    )
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("exclude.user"), backend)
    run_context, repository = make_run_context(tmp_path, "inventory_only")
    job_config = JobConfig(
        source_roots=["/"],
        excluded_roots=["/SkipMe"],
        output_dir=tmp_path,
        mode="inventory_only",
    )
    DropboxInventoryService(repository, make_logger("inventory.exclude.user")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=job_config,
        source_roots=["/"],
        planner=ArchivePlanner("/Archive_PreMay2020", excluded_roots=job_config.excluded_roots),
        emit=None,
        cancellation_token=CancellationToken(),
    )

    rows = list(repository.iter_inventory_records(run_context.run_id))
    assert [row["full_path"] for row in rows] == ["/Keep/old.txt"]


def test_user_excluded_copy_job_is_skipped(tmp_path: Path) -> None:
    run_context, repository = make_run_context(tmp_path, "copy_run")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/SkipMe/old.txt",
                "dropbox_id": "id:skip-old",
                "size": 5,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-skip-old",
            }
        ],
    )
    FilterService(repository, make_logger("filter.exclude.copy")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020"),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend([make_file("/SkipMe/old.txt", dropbox_id="id:skip-old", content_hash="hash-skip-old")], page_size=10)
    adapter = FakeDropboxAdapter(AuthConfig(method="access_token", access_token="token"), make_logger("copy.exclude.user"), backend)
    ArchiveCopyService(repository, make_logger("copy.exclude.user")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], excluded_roots=["/SkipMe"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020", excluded_roots=["/SkipMe"]),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )

    manifest = list(repository.manifest_rows(run_context.run_id))
    assert manifest[0].status == "excluded"
    assert backend.copy_calls == []
    assert repository.get_counters(run_context.run_id)["files_skipped"] == 1


def test_team_inventory_uses_namespace_context(tmp_path: Path) -> None:
    team_discovery = make_team_discovery()
    backend = FakeDropboxBackend(
        [
            make_file(
                "/root-plan.docx",
                dropbox_id="id:root-doc",
                account_mode="team_admin",
                namespace_id="ns-root",
                namespace_type="team_space",
                namespace_name="Acme",
                archive_bucket="team_space",
            ),
            make_file(
                "/Projects/old.psd",
                dropbox_id="id:alice-old",
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
        page_size=10,
        account=team_discovery.account_info,
        team_discovery=team_discovery,
    )
    adapter = FakeDropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("team.inventory"),
        backend,
    )
    run_context, repository = make_run_context(tmp_path, "inventory_only", "team_admin")
    DropboxInventoryService(repository, make_logger("team.inventory")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="inventory_only"),  # type: ignore[arg-type]
        source_roots=["/"],
        planner=ArchivePlanner("/Archive_PreMay2020", account_mode="team_admin").with_team_discovery(team_discovery),
        emit=None,
        cancellation_token=CancellationToken(),
        traversal_roots=team_discovery.traversal_roots,
    )
    rows = list(repository.iter_inventory_records(run_context.run_id))
    assert {row["canonical_source_path"] for row in rows} == {"ns:ns-root/root-plan.docx", "ns:ns-home-alice/Projects/old.psd"}
    assert {row["archive_bucket"] for row in rows} == {"team_space", "member_homes"}


def test_team_namespace_display_folder_can_be_excluded(tmp_path: Path) -> None:
    team_discovery = replace(
        make_team_discovery(),
        traversal_roots=[
            *make_team_discovery().traversal_roots,
            TraversalRoot(
                root_key="namespace::ns-screenshots",
                root_path="/",
                account_mode="team_admin",
                namespace_id="ns-screenshots",
                namespace_type="shared_folder",
                namespace_name="Screenshots",
                archive_bucket="shared_namespaces",
                canonical_root="ns:ns-screenshots",
                include_mounted_folders=True,
            ),
        ],
    )
    backend = FakeDropboxBackend(
        [
            make_file(
                "/old.png",
                dropbox_id="id:old-screenshot",
                account_mode="team_admin",
                namespace_id="ns-screenshots",
                namespace_type="shared_folder",
                namespace_name="Screenshots",
                archive_bucket="shared_namespaces",
            ),
            make_file(
                "/Projects/old.psd",
                dropbox_id="id:alice-old",
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
        page_size=10,
        account=team_discovery.account_info,
        team_discovery=team_discovery,
    )
    adapter = FakeDropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("team.inventory.exclude"),
        backend,
    )
    run_context, repository = make_run_context(tmp_path, "inventory_only", "team_admin")
    job_config = JobConfig(
        source_roots=["/"],
        excluded_roots=["/Screenshots"],
        output_dir=tmp_path,
        mode="inventory_only",
    )
    DropboxInventoryService(repository, make_logger("team.inventory.exclude")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=job_config,
        source_roots=["/"],
        planner=ArchivePlanner(
            "/Archive_PreMay2020",
            account_mode="team_admin",
            excluded_roots=job_config.excluded_roots,
        ).with_team_discovery(team_discovery),
        emit=None,
        cancellation_token=CancellationToken(),
        traversal_roots=team_discovery.traversal_roots,
    )

    rows = list(repository.iter_inventory_records(run_context.run_id))
    assert {row["canonical_source_path"] for row in rows} == {"ns:ns-home-alice/Projects/old.psd"}


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
            make_file("/Archive_PreMay2020/Src/conflict.txt", dropbox_id="id:existing-conflict", size=22, content_hash="hash-other"),
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
    backend = FakeDropboxBackend([make_file("/Src/retry.txt", dropbox_id="id:retry", content_hash="hash-retry")], page_size=10)
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


def test_team_copy_uses_canonical_archive_paths(tmp_path: Path) -> None:
    team_discovery = make_team_discovery()
    team_discovery = replace(
        team_discovery,
        archive_namespace_id="ns-root",
        archive_namespace_root_path="/Archive_PreMay2020",
        archive_provisioned=True,
    )
    run_context, repository = make_run_context(tmp_path, "copy_run", "team_admin")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/Projects/old.psd",
                "dropbox_id": "id:alice-old",
                "size": 10,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-alice",
                "account_mode": "team_admin",
                "namespace_id": "ns-home-alice",
                "namespace_type": "team_member_folder",
                "namespace_name": "Alice Home",
                "member_id": "dbmid:alice",
                "member_email": "alice@example.com",
                "member_display_name": "Alice",
                "canonical_source_path": "ns:ns-home-alice/Projects/old.psd",
                "canonical_parent_path": "ns:ns-home-alice/Projects",
                "archive_bucket": "member_homes",
                "root_scope_used": "namespace::ns-home-alice",
            }
        ],
    )
    planner = ArchivePlanner("/Archive_PreMay2020", account_mode="team_admin").with_team_discovery(team_discovery)
    FilterService(repository, make_logger("filter.team.copy")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=planner,
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend(
        [
            make_file(
                "/Projects/old.psd",
                dropbox_id="id:alice-old",
                content_hash="hash-alice",
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
        page_size=10,
        account=team_discovery.account_info,
        team_discovery=team_discovery,
    )
    adapter = FakeDropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("copy.team"),
        backend,
    )
    ArchiveCopyService(repository, make_logger("copy.team")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=planner,
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )
    manifest = list(repository.manifest_rows(run_context.run_id))
    assert manifest[0].status == "copied"
    assert manifest[0].archive_canonical_path == "ns:ns-root/Archive_PreMay2020/member_homes/alice-example.com/Projects/old.psd"


def test_team_copy_blocks_cleanly_when_archive_not_provisioned(tmp_path: Path) -> None:
    team_discovery = replace(
        make_team_discovery(),
        archive_namespace_id="ns-root",
        archive_namespace_root_path="/Archive_PreMay2020",
        archive_provisioned=False,
        archive_status_detail="Dropbox denied write permission while creating /Archive_PreMay2020.",
    )
    run_context, repository = make_run_context(tmp_path, "copy_run", "team_admin")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/root-plan.docx",
                "dropbox_id": "id:root",
                "size": 4,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-root",
                "account_mode": "team_admin",
                "namespace_id": "ns-root",
                "namespace_type": "team_space",
                "namespace_name": "Acme",
                "canonical_source_path": "ns:ns-root/root-plan.docx",
                "canonical_parent_path": "ns:ns-root",
                "archive_bucket": "team_space",
            }
        ],
    )
    planner = ArchivePlanner("/Archive_PreMay2020", account_mode="team_admin").with_team_discovery(team_discovery)
    FilterService(repository, make_logger("filter.team.blocked")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=planner,
        emit=None,
        cancellation_token=CancellationToken(),
    )
    backend = FakeDropboxBackend([], page_size=10, account=team_discovery.account_info, team_discovery=team_discovery)
    adapter = FakeDropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("copy.team.blocked"),
        backend,
    )

    ArchiveCopyService(repository, make_logger("copy.team.blocked")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=planner,
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )

    manifest = list(repository.manifest_rows(run_context.run_id))
    assert manifest[0].status == "blocked_precondition"
    assert "denied write permission" in manifest[0].status_detail
    assert backend.copy_calls == []

    backend.queue_failure(
        "get_metadata",
        "ns:ns-root/Archive_PreMay2020/team_space/root-plan.docx",
        "",
        TemporaryDropboxError("verification should not call Dropbox for blocked jobs"),
    )
    rows = VerificationService(repository, make_logger("verify.team.blocked")).run(
        adapter=adapter,
        run_context=run_context,
        job_config=JobConfig(
            source_roots=["/"],
            output_dir=tmp_path,
            mode="copy_run",
            retry=RetrySettings(max_retries=0, initial_backoff_seconds=0, backoff_multiplier=1, max_backoff_seconds=0),
        ),  # type: ignore[arg-type]
        emit=None,
        cancellation_token=CancellationToken(),
    )
    assert rows[0].verification_status == "blocked_precondition"
    assert "denied write permission" in rows[0].detail

    resumed_discovery = replace(team_discovery, archive_provisioned=True)
    resume_backend = FakeDropboxBackend(
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
            )
        ],
        page_size=10,
        account=resumed_discovery.account_info,
        team_discovery=resumed_discovery,
    )
    resume_adapter = FakeDropboxAdapter(
        AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
        make_logger("copy.team.resumed_blocked"),
        resume_backend,
    )
    ArchiveCopyService(repository, make_logger("copy.team.resumed_blocked")).run(
        adapter=resume_adapter,
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Archive_PreMay2020", account_mode="team_admin").with_team_discovery(resumed_discovery),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )

    resumed_manifest = list(repository.manifest_rows(run_context.run_id))
    assert resumed_manifest[0].status == "copied"
    assert resume_backend.copy_calls == [("ns:ns-root/root-plan.docx", "ns:ns-root/Archive_PreMay2020/team_space/root-plan.docx")]


def test_team_resume_rebuilds_archive_canonical_path_for_mounted_archive(tmp_path: Path) -> None:
    old_discovery = replace(
        make_team_discovery(),
        archive_namespace_id="ns-root",
        archive_namespace_root_path="/Screenshots/Archive",
        archive_provisioned=True,
    )
    run_context, repository = make_run_context(tmp_path, "copy_run", "team_admin")
    seed_inventory(
        repository,
        run_context,
        [
            {
                "item_type": "file",
                "full_path": "/root-plan.docx",
                "dropbox_id": "id:root",
                "size": 4,
                "server_modified": "2019-01-01T00:00:00Z",
                "client_modified": "2019-01-01T00:00:00Z",
                "content_hash": "hash-root",
                "account_mode": "team_admin",
                "namespace_id": "ns-root",
                "namespace_type": "team_space",
                "namespace_name": "Acme",
                "canonical_source_path": "ns:ns-root/root-plan.docx",
                "canonical_parent_path": "ns:ns-root",
                "archive_bucket": "team_space",
            }
        ],
    )
    FilterService(repository, make_logger("filter.team.rebuild")).run(
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Screenshots/Archive", account_mode="team_admin").with_team_discovery(old_discovery),
        emit=None,
        cancellation_token=CancellationToken(),
    )
    old_manifest = list(repository.manifest_rows(run_context.run_id))
    assert old_manifest[0].archive_canonical_path == "ns:ns-root/Screenshots/Archive/team_space/root-plan.docx"
    repository.promote_copy_jobs(
        run_context.run_id,
        from_statuses=("planned",),
        to_status="blocked_precondition",
        detail="Old run blocked before mounted archive namespace was resolved.",
    )

    new_discovery = replace(
        make_team_discovery(),
        archive_namespace_id="ns-screenshots",
        archive_namespace_root_path="/Archive",
        archive_provisioned=True,
    )
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
            )
        ],
        page_size=10,
        account=new_discovery.account_info,
        team_discovery=new_discovery,
    )
    ArchiveCopyService(repository, make_logger("copy.team.rebuild")).run(
        adapter=FakeDropboxAdapter(
            AuthConfig(method="access_token", account_mode="team_admin", access_token="token"),
            make_logger("copy.team.rebuild.adapter"),
            backend,
        ),
        run_context=run_context,
        job_config=JobConfig(source_roots=["/"], output_dir=tmp_path, mode="copy_run"),  # type: ignore[arg-type]
        planner=ArchivePlanner("/Screenshots/Archive", account_mode="team_admin").with_team_discovery(new_discovery),
        emit=None,
        cancellation_token=CancellationToken(),
        dry_run=False,
    )

    resumed_manifest = list(repository.manifest_rows(run_context.run_id))
    assert resumed_manifest[0].status == "copied"
    assert resumed_manifest[0].archive_canonical_path == "ns:ns-screenshots/Archive/team_space/root-plan.docx"
    assert backend.copy_calls == [("ns:ns-root/root-plan.docx", "ns:ns-screenshots/Archive/team_space/root-plan.docx")]
