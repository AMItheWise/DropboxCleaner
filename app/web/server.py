from __future__ import annotations

import json
import logging
import os
import sys
import threading
import webbrowser
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.auth import AuthManager, CredentialStore, default_scopes_for_mode
from app.dropbox_client.errors import MissingScopeError
from app.models.config import AuthConfig, JobConfig, RetrySettings
from app.services.orchestrator import RunOrchestrator
from app.ui.folder_browser import BrowserLocation, DropboxFolderBrowserService
from app.ui.options import (
    ACCOUNT_CHOICES,
    DATE_FILTER_CHOICES,
    RUN_MODE_CHOICES,
    TEAM_ARCHIVE_LAYOUT_CHOICES,
    TEAM_COVERAGE_CHOICES,
)
from app.utils.paths import normalize_dropbox_path
from app.web.history import discover_run_dirs, find_run_dir, history_item, result_payload, safe_output_file
from app.web.jobs import JobManager
from app.web.models import (
    AuthFinishRequest,
    AuthStartRequest,
    AuthStatusResponse,
    AuthTestRequest,
    BrowserLocationPayload,
    FolderListRequest,
    ResumeRunRequest,
    RunStartRequest,
    expanded_path,
)


AdapterFactory = Callable[[AuthConfig, logging.Logger], Any]


def create_app(
    *,
    adapter_factory: AdapterFactory = DropboxAdapter,
    credential_store: CredentialStore | None = None,
    static_dir: Path | None = None,
    browser_url: str | None = None,
) -> FastAPI:
    auth_manager = AuthManager(credential_store=credential_store, adapter_factory=adapter_factory)
    job_manager = JobManager(orchestrator_factory=lambda: RunOrchestrator(adapter_factory=adapter_factory))

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN202
        if browser_url:
            threading.Timer(0.5, lambda: webbrowser.open(browser_url)).start()
        yield

    app = FastAPI(title="Dropbox Cleaner", version="1.0.0", lifespan=lifespan)
    app.state.auth_manager = auth_manager
    app.state.job_manager = job_manager
    app.state.adapter_factory = adapter_factory

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/options")
    def options() -> dict[str, Any]:
        return {
            "accounts": [_choice(choice) for choice in ACCOUNT_CHOICES],
            "run_modes": [_choice(choice) for choice in RUN_MODE_CHOICES],
            "date_filters": [_choice(choice) for choice in DATE_FILTER_CHOICES],
            "team_coverage": [_choice(choice) for choice in TEAM_COVERAGE_CHOICES],
            "team_archive_layouts": [_choice(choice) for choice in TEAM_ARCHIVE_LAYOUT_CHOICES],
            "defaults": {
                "account_mode": "personal",
                "mode": "dry_run",
                "cutoff_date": "2020-05-01",
                "date_filter_field": "server_modified",
                "archive_root": "/Archive_PreMay2020",
                "output_dir": str(Path("outputs").resolve()),
                "batch_size": 500,
                "conflict_policy": "safe_skip",
                "include_folders_in_inventory": True,
                "exclude_archive_destination": True,
                "worker_count": 1,
                "verify_after_run": True,
                "team_coverage_preset": "team_owned_only",
                "team_archive_layout": "segmented",
            },
            "packaged_app_key_available": bool(resolve_packaged_app_key()),
        }

    @app.get("/api/auth/status")
    def auth_status() -> dict[str, Any]:
        saved = auth_manager.load_credentials("default")
        packaged_key = resolve_packaged_app_key()
        return AuthStatusResponse(
            saved_credentials_available=saved is not None,
            account_mode=saved.account_mode if saved else None,
            app_key=packaged_key or (saved.app_key if saved else None),
            admin_member_id=saved.admin_member_id if saved else None,
            packaged_app_key_available=bool(packaged_key),
        ).model_dump()

    @app.post("/api/auth/start")
    def auth_start(payload: AuthStartRequest) -> dict[str, str]:
        app_key = resolve_packaged_app_key() or (payload.app_key or "").strip()
        if not app_key:
            raise HTTPException(status_code=400, detail="Enter a Dropbox app key first.")
        authorize_url = auth_manager.start_pkce_flow(
            app_key,
            default_scopes_for_mode(payload.account_mode),
            account_mode=payload.account_mode,
            label="default",
        )
        return {"authorize_url": authorize_url}

    @app.post("/api/auth/finish")
    def auth_finish(payload: AuthFinishRequest) -> dict[str, Any]:
        try:
            credentials = auth_manager.finish_pkce_flow(payload.auth_code, label="default")
            if payload.admin_member_id:
                credentials.admin_member_id = payload.admin_member_id
            auth_manager.save_credentials("default", credentials)
            account = auth_manager.test_connection(auth_manager.credentials_to_auth_config(credentials), _logger("web.auth"))
            return {"account": asdict(account)}
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=_format_exception_for_user(exc, "personal")) from exc

    @app.post("/api/auth/test")
    def auth_test(payload: AuthTestRequest) -> dict[str, Any]:
        auth_config = _saved_auth_config(auth_manager, payload.account_mode, payload.admin_member_id)
        try:
            account = auth_manager.test_connection(auth_config, _logger("web.auth"))
            return {"account": asdict(account)}
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=_format_exception_for_user(exc, auth_config.account_mode)) from exc

    @app.delete("/api/auth")
    def auth_clear() -> dict[str, str]:
        auth_manager.clear_credentials("default")
        return {"status": "cleared"}

    @app.post("/api/folders/list")
    def folder_list(payload: FolderListRequest) -> dict[str, Any]:
        auth_config = _saved_auth_config(auth_manager, payload.account_mode, None)
        job_config = _job_config_from_folder_request(payload)
        adapter = adapter_factory(auth_config, _logger("web.folder_browser"))
        try:
            service = DropboxFolderBrowserService(adapter, account_mode=auth_config.account_mode, job_config=job_config)
            location = _browser_location(payload.location) if payload.location else service.root_location()
            folders = service.list_folders(location)
            parent = service.parent_location(location)
            return {
                "location": asdict(location),
                "parent": asdict(parent),
                "folders": [asdict(folder) for folder in folders],
                "advanced_team_locations_available": service.has_advanced_team_locations(),
            }
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            adapter.close()

    @app.post("/api/runs")
    def start_run(payload: RunStartRequest) -> dict[str, str]:
        if payload.mode == "copy_run" and not payload.confirmed_copy_run:
            raise HTTPException(status_code=400, detail="Copy runs require explicit confirmation.")
        auth_config = _saved_auth_config(auth_manager, payload.account_mode, payload.admin_member_id)
        job_config = _job_config_from_run_request(payload)
        try:
            state = job_manager.start_run(auth_config=auth_config, job_config=job_config)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"run_id": state.job_id, "status": "running"}

    @app.post("/api/runs/resume")
    def resume_run(payload: ResumeRunRequest) -> dict[str, str]:
        auth_config = _saved_auth_config(auth_manager, payload.account_mode, payload.admin_member_id)
        state_db = _resolve_state_db(payload)
        try:
            state = job_manager.resume_run(auth_config=auth_config, state_db_path=state_db)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"run_id": state.job_id, "status": "running"}

    @app.post("/api/runs/{run_id}/cancel")
    def cancel_run(run_id: str) -> dict[str, str]:
        if not job_manager.cancel(run_id):
            raise HTTPException(status_code=404, detail="No active run was found for that ID.")
        return {"status": "cancelling"}

    @app.get("/api/runs")
    def list_runs(output_dir: str = Query("outputs")) -> dict[str, Any]:
        base_dir = expanded_path(output_dir)
        latest_run_id, run_dirs = discover_run_dirs(base_dir)
        return {
            "output_dir": str(base_dir),
            "latest_run_id": latest_run_id,
            "runs": [history_item(run_dir, latest_run_id=latest_run_id) for run_dir in run_dirs],
        }

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str, output_dir: str = Query("outputs")) -> dict[str, Any]:
        job_status = job_manager.status(run_id)
        if job_status is not None:
            if job_status.get("run_dir"):
                try:
                    job_status["result"] = result_payload(Path(str(job_status["run_dir"])))
                except Exception:  # noqa: BLE001
                    job_status["result"] = None
            return job_status
        run_dir = find_run_dir(expanded_path(output_dir), run_id)
        if run_dir is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        return {
            "run_id": run_id,
            "status": "completed",
            "kind": "history",
            "run_dir": str(run_dir),
            "result": result_payload(run_dir),
        }

    @app.get("/api/runs/{run_id}/events")
    def run_events(
        run_id: str,
        after: int = Query(0),
        accept: str | None = Header(default=None),
    ):
        if accept and "text/event-stream" in accept:
            return StreamingResponse(job_manager.sse_events(run_id, after), media_type="text/event-stream")
        return {"events": job_manager.events_after(run_id, after)}

    @app.get("/api/runs/{run_id}/files/{name}")
    def run_file(run_id: str, name: str, output_dir: str = Query("outputs")):
        run_dir = None
        job_status = job_manager.status(run_id)
        if job_status and job_status.get("run_dir"):
            run_dir = Path(str(job_status["run_dir"]))
        if run_dir is None:
            run_dir = find_run_dir(expanded_path(output_dir), run_id)
        if run_dir is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        file_path = safe_output_file(run_dir, name)
        if file_path is None:
            raise HTTPException(status_code=404, detail="File not found.")
        return FileResponse(file_path, filename=file_path.name)

    _mount_static(app, static_dir or default_static_dir())
    return app


def resolve_packaged_app_key() -> str | None:
    env_value = os.environ.get("DROPBOX_CLEANER_APP_KEY") or os.environ.get("DROPBOX_APP_KEY")
    if env_value:
        return env_value
    candidates: list[Path] = []
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).with_name("dropbox_app_key.txt"))
        candidates.append(Path(sys.executable).parent.parent / "Resources" / "dropbox_app_key.txt")
    candidates.append(Path.cwd() / "dropbox_app_key.txt")
    for path in candidates:
        if path.exists():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
    return None


def default_static_dir() -> Path:
    return Path(__file__).resolve().parent / "static"


def _mount_static(app: FastAPI, static_dir: Path) -> None:
    index = static_dir / "index.html"
    if index.exists():
        app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa_fallback(path: str = ""):  # noqa: ARG001
            return FileResponse(index)

    else:
        @app.get("/", include_in_schema=False)
        def missing_static() -> HTMLResponse:
            return HTMLResponse(
                "<!doctype html><title>Dropbox Cleaner</title>"
                "<main style='font-family:system-ui;padding:32px;max-width:720px'>"
                "<h1>Dropbox Cleaner web UI is not built yet.</h1>"
                "<p>Run the Vite build from the web folder, then restart this server.</p>"
                "</main>"
            )


def _saved_auth_config(auth_manager: AuthManager, account_mode: str | None, admin_member_id: str | None) -> AuthConfig:
    saved = auth_manager.load_credentials("default")
    if saved is None:
        raise HTTPException(status_code=400, detail="Connect Dropbox first.")
    auth_config = auth_manager.credentials_to_auth_config(saved)
    if account_mode:
        auth_config.account_mode = account_mode  # type: ignore[assignment]
    if admin_member_id:
        auth_config.admin_member_id = admin_member_id
    return auth_config


def _job_config_from_run_request(payload: RunStartRequest) -> JobConfig:
    source_roots = payload.source_roots or ["/"]
    return JobConfig(
        source_roots=source_roots,
        excluded_roots=payload.excluded_roots,
        cutoff_date=payload.cutoff_date,
        date_filter_field=payload.date_filter_field,
        archive_root=normalize_dropbox_path(payload.archive_root),
        output_dir=expanded_path(payload.output_dir),
        mode=payload.mode,
        batch_size=payload.batch_size,
        retry=RetrySettings(**payload.retry.model_dump()),
        conflict_policy=payload.conflict_policy,
        include_folders_in_inventory=payload.include_folders_in_inventory,
        exclude_archive_destination=payload.exclude_archive_destination,
        worker_count=payload.worker_count,
        verify_after_run=payload.verify_after_run,
        team_coverage_preset=payload.team_coverage_preset,
        team_archive_layout=payload.team_archive_layout,
    )


def _job_config_from_folder_request(payload: FolderListRequest) -> JobConfig:
    return JobConfig(
        source_roots=payload.source_roots or ["/"],
        excluded_roots=payload.excluded_roots,
        cutoff_date=payload.cutoff_date,
        date_filter_field=payload.date_filter_field,
        archive_root=normalize_dropbox_path(payload.archive_root),
        output_dir=expanded_path(payload.output_dir),
        mode=payload.mode,
        team_coverage_preset=payload.team_coverage_preset,
        team_archive_layout=payload.team_archive_layout,
    )


def _browser_location(payload: BrowserLocationPayload) -> BrowserLocation:
    return BrowserLocation(
        display_path=payload.display_path,
        namespace_id=payload.namespace_id,
        namespace_path=payload.namespace_path,
        title=payload.title,
        view_mode=payload.view_mode,
    )


def _resolve_state_db(payload: ResumeRunRequest) -> Path:
    if payload.state_db_path:
        return expanded_path(payload.state_db_path)
    latest_pointer = expanded_path(payload.output_dir) / "latest_run.json"
    if not latest_pointer.exists():
        raise HTTPException(status_code=400, detail=f"Could not find {latest_pointer}.")
    try:
        data = json.loads(latest_pointer.read_text(encoding="utf-8"))
        return Path(data["state_db"]).expanduser()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not read {latest_pointer}.") from exc


def _choice(choice: Any) -> dict[str, str]:
    return {"label": choice.label, "value": choice.value, "description": choice.description}


def _logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"dropbox_cleaner.{name}")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def _format_exception_for_user(message: str | Exception, account_mode: str) -> str:
    text = str(message)
    if isinstance(message, MissingScopeError) or "required scope" in text.casefold():
        scope_block = (
            "account_info.read, files.metadata.read, files.content.read, files.content.write, "
            "team_info.read, members.read, team_data.member, sharing.read, sharing.write, "
            "files.team_metadata.read, files.team_metadata.write, team_data.team_space."
            if account_mode == "team_admin"
            else "account_info.read, files.metadata.read, files.content.read, files.content.write."
        )
        return (
            "Dropbox permissions need one more step. Open the Dropbox App Console, enable the required scopes, "
            f"save, then reconnect this app. Required scopes: {scope_block}"
        )
    return text
