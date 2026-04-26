from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from app.models.config import StoredCredentials
from app.web.server import create_app
from tests.fakes import FakeDropboxBackend, fake_adapter_factory, make_file, make_folder


class MemoryCredentialStore:
    def __init__(self) -> None:
        self.saved: dict[str, StoredCredentials] = {}

    def save(self, label: str, credentials: StoredCredentials) -> None:
        self.saved[label] = credentials

    def load(self, label: str) -> StoredCredentials | None:
        return self.saved.get(label)

    def clear(self, label: str) -> None:
        self.saved.pop(label, None)


def make_client(backend: FakeDropboxBackend, store: MemoryCredentialStore | None = None) -> TestClient:
    app = create_app(adapter_factory=fake_adapter_factory(backend), credential_store=store or MemoryCredentialStore())
    return TestClient(app)


def save_personal_credentials(store: MemoryCredentialStore) -> None:
    store.save(
        "default",
        StoredCredentials(
            method="access_token",
            account_mode="personal",
            app_key=None,
            access_token="token",
        ),
    )


def test_web_health_options_and_auth_status() -> None:
    store = MemoryCredentialStore()
    backend = FakeDropboxBackend([])
    client = make_client(backend, store)

    assert client.get("/api/health").json() == {"status": "ok"}
    options = client.get("/api/options").json()
    assert options["defaults"]["mode"] == "dry_run"
    assert {choice["value"] for choice in options["run_modes"]} == {"inventory_only", "dry_run", "copy_run"}
    assert client.get("/api/auth/status").json()["saved_credentials_available"] is False

    save_personal_credentials(store)
    status = client.get("/api/auth/status").json()
    assert status["saved_credentials_available"] is True
    assert status["account_mode"] == "personal"


def test_web_auth_test_and_folder_listing() -> None:
    store = MemoryCredentialStore()
    save_personal_credentials(store)
    backend = FakeDropboxBackend(
        [
            make_folder("/Photos", dropbox_id="id:photos"),
            make_folder("/Photos/Trips", dropbox_id="id:trips"),
            make_file("/Photos/Trips/a.jpg", dropbox_id="id:a"),
        ],
        page_size=1,
    )
    client = make_client(backend, store)

    account = client.post("/api/auth/test", json={"account_mode": "personal"}).json()["account"]
    assert account["display_name"] == "Fake User"

    root = client.post("/api/folders/list", json={"account_mode": "personal"}).json()
    assert [folder["display_path"] for folder in root["folders"]] == ["/Photos"]

    child = client.post(
        "/api/folders/list",
        json={
            "account_mode": "personal",
            "location": root["folders"][0] | {"title": "Photos", "view_mode": "default"},
        },
    ).json()
    assert [folder["display_path"] for folder in child["folders"]] == ["/Photos/Trips"]


def test_web_copy_run_requires_confirmation(tmp_path: Path) -> None:
    store = MemoryCredentialStore()
    save_personal_credentials(store)
    client = make_client(FakeDropboxBackend([]), store)

    response = client.post(
        "/api/runs",
        json={
            "account_mode": "personal",
            "mode": "copy_run",
            "source_roots": ["/"],
            "output_dir": str(tmp_path),
            "archive_root": "/Archive",
            "confirmed_copy_run": False,
        },
    )

    assert response.status_code == 400
    assert "confirmation" in response.json()["detail"]


def test_web_run_lifecycle_and_history(tmp_path: Path) -> None:
    store = MemoryCredentialStore()
    save_personal_credentials(store)
    backend = FakeDropboxBackend(
        [
            make_folder("/Docs", dropbox_id="id:docs"),
            make_file("/Docs/old.txt", dropbox_id="id:old", content_hash="hash-old"),
            make_file(
                "/Docs/new.txt",
                dropbox_id="id:new",
                server_modified="2025-01-01T00:00:00Z",
                client_modified="2025-01-01T00:00:00Z",
                content_hash="hash-new",
            ),
        ],
        page_size=2,
    )
    client = make_client(backend, store)

    started = client.post(
        "/api/runs",
        json={
            "account_mode": "personal",
            "mode": "dry_run",
            "source_roots": ["/"],
            "output_dir": str(tmp_path),
            "archive_root": "/Archive",
        },
    ).json()

    status = _wait_for_completion(client, started["run_id"], tmp_path)
    assert status["status"] == "completed"
    assert status["actual_run_id"]
    metric_labels = {metric["label"] for metric in status["result"]["metrics"]}
    assert "Matched" in metric_labels

    events = client.get(f"/api/runs/{started['run_id']}/events").json()["events"]
    assert any(event["type"] == "progress" for event in events)

    history = client.get(f"/api/runs?output_dir={tmp_path}").json()
    assert history["latest_run_id"] == status["actual_run_id"]
    assert history["runs"][0]["run_id"] == status["actual_run_id"]

    summary = client.get(
        f"/api/runs/{status['actual_run_id']}/files/summary.json",
        params={"output_dir": str(tmp_path)},
    )
    assert summary.status_code == 200


def _wait_for_completion(client: TestClient, run_id: str, output_dir: Path) -> dict:
    deadline = time.time() + 10
    while time.time() < deadline:
        status = client.get(f"/api/runs/{run_id}", params={"output_dir": str(output_dir)}).json()
        if status["status"] != "running":
            return status
        time.sleep(0.05)
    raise AssertionError("Run did not finish in time.")
