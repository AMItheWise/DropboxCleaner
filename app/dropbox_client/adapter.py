from __future__ import annotations

import logging
import re
from typing import Any

import dropbox
from dropbox import files
from dropbox import exceptions as dbx_exceptions

from app.dropbox_client.errors import (
    AuthenticationFailureError,
    CursorResetError,
    DestinationConflictError,
    MissingScopeError,
    PathNotFoundError,
    PermanentDropboxError,
    TemporaryDropboxError,
)
from app.models.config import AuthConfig
from app.models.records import AccountInfo, ListingPage, RemoteEntry
from app.utils.paths import normalize_dropbox_path, parent_path, sdk_path
from app.utils.time import isoformat_utc


class DropboxAdapter:
    def __init__(self, auth_config: AuthConfig, logger: logging.Logger, timeout: int = 100) -> None:
        self._auth_config = auth_config
        self._logger = logger
        self._client = self._build_client(auth_config, timeout)

    def _build_client(self, auth_config: AuthConfig, timeout: int) -> dropbox.Dropbox:
        common_args: dict[str, Any] = {
            "timeout": timeout,
            "max_retries_on_error": 0,
            "max_retries_on_rate_limit": 0,
        }
        if auth_config.method == "refresh_token":
            if not auth_config.refresh_token or not auth_config.app_key:
                raise AuthenticationFailureError("Refresh-token auth requires both an app key and a refresh token.")
            return dropbox.Dropbox(
                oauth2_refresh_token=auth_config.refresh_token,
                app_key=auth_config.app_key,
                scope=list(auth_config.scopes),
                **common_args,
            )
        if auth_config.method == "oauth_pkce":
            if not auth_config.refresh_token or not auth_config.app_key:
                raise AuthenticationFailureError("PKCE auth requires a saved refresh token and app key.")
            return dropbox.Dropbox(
                oauth2_refresh_token=auth_config.refresh_token,
                app_key=auth_config.app_key,
                scope=list(auth_config.scopes),
                **common_args,
            )
        if auth_config.method == "access_token":
            if not auth_config.access_token:
                raise AuthenticationFailureError("Access-token auth requires an access token.")
            return dropbox.Dropbox(oauth2_access_token=auth_config.access_token, **common_args)
        raise AuthenticationFailureError(f"Unsupported auth method: {auth_config.method}")

    def close(self) -> None:
        self._client.close()

    def get_current_account(self) -> AccountInfo:
        try:
            account = self._client.users_get_current_account()
            return AccountInfo(
                account_id=account.account_id,
                display_name=account.name.display_name,
                email=getattr(account, "email", None),
            )
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def validate_file_listing_access(self) -> None:
        self.list_folder("/", recursive=False, limit=1)

    def list_folder(self, path: str, recursive: bool, limit: int) -> ListingPage:
        try:
            result = self._client.files_list_folder(
                sdk_path(path),
                recursive=recursive,
                limit=limit,
                include_deleted=False,
                include_non_downloadable_files=True,
            )
            return self._map_listing_page(result)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def list_folder_continue(self, cursor: str) -> ListingPage:
        try:
            result = self._client.files_list_folder_continue(cursor)
            return self._map_listing_page(result)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def get_metadata(self, path: str) -> RemoteEntry | None:
        try:
            metadata = self._client.files_get_metadata(sdk_path(path))
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, PathNotFoundError):
                return None
            raise mapped from exc
        return self._map_entry(metadata)

    def create_folder_if_missing(self, path: str) -> RemoteEntry | None:
        try:
            result = self._client.files_create_folder_v2(sdk_path(path), autorename=False)
            return self._map_entry(result.metadata)
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, DestinationConflictError):
                existing = self.get_metadata(path)
                if existing and existing.item_type == "folder":
                    return existing
            raise mapped from exc

    def copy_file(self, source_path: str, destination_path: str) -> RemoteEntry:
        try:
            result = self._client.files_copy_v2(
                sdk_path(source_path),
                sdk_path(destination_path),
                autorename=False,
            )
            return self._map_entry(result.metadata)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def _map_listing_page(self, result: Any) -> ListingPage:
        return ListingPage(
            entries=[self._map_entry(entry) for entry in result.entries if self._map_entry(entry) is not None],
            cursor=result.cursor,
            has_more=result.has_more,
        )

    def _map_entry(self, entry: Any) -> RemoteEntry | None:
        if isinstance(entry, files.DeletedMetadata):
            return None
        if isinstance(entry, files.FileMetadata):
            path_display = normalize_dropbox_path(entry.path_display)
            return RemoteEntry(
                item_type="file",
                full_path=path_display,
                path_lower=normalize_dropbox_path(entry.path_lower),
                filename=entry.name,
                parent_path=parent_path(path_display),
                dropbox_id=entry.id,
                size=entry.size,
                server_modified=isoformat_utc(entry.server_modified),
                client_modified=isoformat_utc(entry.client_modified),
                content_hash=getattr(entry, "content_hash", None),
            )
        if isinstance(entry, files.FolderMetadata):
            path_display = normalize_dropbox_path(entry.path_display)
            return RemoteEntry(
                item_type="folder",
                full_path=path_display,
                path_lower=normalize_dropbox_path(entry.path_lower),
                filename=entry.name,
                parent_path=parent_path(path_display),
                dropbox_id=entry.id,
                size=None,
                server_modified=None,
                client_modified=None,
                content_hash=None,
            )
        return None

    def _raise_mapped(self, exc: Exception) -> None:
        raise self._map_exception(exc) from exc

    def _map_exception(self, exc: Exception) -> Exception:
        message = getattr(exc, "message", str(exc))
        lowered = message.casefold()
        missing_scope = self._extract_required_scope(message)
        if isinstance(exc, dbx_exceptions.BadInputError):
            if missing_scope is not None or "missing_scope" in lowered:
                return MissingScopeError(message, required_scope=missing_scope)
            return PermanentDropboxError(message)
        if isinstance(exc, dbx_exceptions.RateLimitError):
            return TemporaryDropboxError(message, retry_after=getattr(exc, "backoff", None))
        if isinstance(exc, dbx_exceptions.InternalServerError):
            return TemporaryDropboxError(message)
        if isinstance(exc, dbx_exceptions.HttpError):
            return TemporaryDropboxError(message)
        if isinstance(exc, dbx_exceptions.AuthError):
            return AuthenticationFailureError(message)
        if isinstance(exc, dbx_exceptions.ApiError):
            if missing_scope is not None or "missing_scope" in lowered:
                return MissingScopeError(message, required_scope=missing_scope)
            if "reset" in lowered and "cursor" in lowered:
                return CursorResetError(message)
            if "not_found" in lowered:
                return PathNotFoundError(message)
            if "conflict" in lowered or "already exists" in lowered:
                return DestinationConflictError(message)
            if any(token in lowered for token in ("rate_limit", "too_many_requests", "temporarily_unavailable")):
                return TemporaryDropboxError(message)
        return PermanentDropboxError(message)

    def _extract_required_scope(self, message: str) -> str | None:
        match = re.search(r"required scope ['\"]([^'\"]+)['\"]", message, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None
