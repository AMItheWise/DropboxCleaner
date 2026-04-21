from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import keyring
from keyring.errors import KeyringError
from platformdirs import user_config_dir
from dropbox import DropboxOAuth2FlowNoRedirect

from app.dropbox_client.adapter import DropboxAdapter
from app.models.config import AuthConfig, DEFAULT_PERSONAL_SCOPES, DEFAULT_TEAM_SCOPES, StoredCredentials
from app.models.records import AccountInfo
from app.utils.atomic import atomic_text_write


def default_scopes_for_mode(account_mode: str) -> tuple[str, ...]:
    return DEFAULT_TEAM_SCOPES if account_mode == "team_admin" else DEFAULT_PERSONAL_SCOPES


class CredentialStore:
    SERVICE_NAME = "DropboxCleaner"

    def __init__(self) -> None:
        config_dir = Path(user_config_dir("DropboxCleaner", "OpenAI"))
        self._fallback_path = config_dir / "credentials.json"

    def save(self, label: str, credentials: StoredCredentials) -> None:
        payload = json.dumps(asdict(credentials))
        try:
            keyring.set_password(self.SERVICE_NAME, label, payload)
            return
        except KeyringError:
            self._fallback_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_text_write(self._fallback_path, payload)

    def load(self, label: str) -> StoredCredentials | None:
        try:
            payload = keyring.get_password(self.SERVICE_NAME, label)
        except KeyringError:
            payload = None
        if payload is None and self._fallback_path.exists():
            payload = self._fallback_path.read_text(encoding="utf-8")
        if not payload:
            return None
        raw = json.loads(payload)
        raw["account_mode"] = raw.get("account_mode") or "personal"
        raw["scopes"] = tuple(raw.get("scopes") or default_scopes_for_mode(raw["account_mode"]))
        return StoredCredentials(**raw)

    def clear(self, label: str) -> None:
        try:
            keyring.delete_password(self.SERVICE_NAME, label)
        except Exception:  # noqa: BLE001
            pass
        if self._fallback_path.exists():
            self._fallback_path.unlink()


class AuthManager:
    def __init__(self, credential_store: CredentialStore | None = None, adapter_factory=DropboxAdapter) -> None:
        self._credential_store = credential_store or CredentialStore()
        self._adapter_factory = adapter_factory
        self._flows: dict[str, DropboxOAuth2FlowNoRedirect] = {}

    def start_pkce_flow(
        self,
        app_key: str,
        scopes: tuple[str, ...],
        *,
        account_mode: str = "personal",
        label: str = "default",
    ) -> str:
        flow = DropboxOAuth2FlowNoRedirect(
            app_key,
            token_access_type="offline",
            scope=list(scopes),
            use_pkce=True,
        )
        setattr(flow, "_dropbox_cleaner_account_mode", account_mode)
        self._flows[label] = flow
        return flow.start()

    def finish_pkce_flow(self, auth_code: str, label: str = "default") -> StoredCredentials:
        flow = self._flows.get(label)
        if flow is None:
            raise ValueError("No in-progress OAuth PKCE flow found. Start authorization first.")
        result = flow.finish(auth_code.strip())
        credentials = StoredCredentials(
            method="oauth_pkce",
            account_mode=getattr(flow, "_dropbox_cleaner_account_mode", "personal"),
            app_key=flow.consumer_key,
            refresh_token=result.refresh_token,
            scopes=tuple(flow.scope or default_scopes_for_mode(getattr(flow, "_dropbox_cleaner_account_mode", "personal"))),
        )
        self._flows.pop(label, None)
        return credentials

    def save_credentials(self, label: str, credentials: StoredCredentials) -> None:
        self._credential_store.save(label, credentials)

    def load_credentials(self, label: str = "default") -> StoredCredentials | None:
        return self._credential_store.load(label)

    def clear_credentials(self, label: str = "default") -> None:
        self._credential_store.clear(label)

    def credentials_to_auth_config(self, credentials: StoredCredentials) -> AuthConfig:
        return AuthConfig(
            method=credentials.method,
            account_mode=credentials.account_mode,
            app_key=credentials.app_key,
            refresh_token=credentials.refresh_token,
            access_token=credentials.access_token,
            scopes=credentials.scopes,
            store_label="default",
            admin_member_id=credentials.admin_member_id,
        )

    def test_connection(self, auth_config: AuthConfig, logger) -> AccountInfo:
        adapter = self._adapter_factory(auth_config, logger)
        try:
            account = adapter.get_current_account()
            adapter.validate_file_listing_access()
            return account
        finally:
            adapter.close()

    def save_manual_token(
        self,
        *,
        method: str,
        account_mode: str,
        app_key: str | None,
        refresh_token: str | None,
        access_token: str | None,
        admin_member_id: str | None = None,
        label: str = "default",
    ) -> StoredCredentials:
        credentials = StoredCredentials(
            method=method,  # type: ignore[arg-type]
            account_mode=account_mode,  # type: ignore[arg-type]
            app_key=app_key,
            refresh_token=refresh_token,
            access_token=access_token,
            scopes=default_scopes_for_mode(account_mode),
            admin_member_id=admin_member_id,
        )
        self.save_credentials(label, credentials)
        return credentials
