from __future__ import annotations

import logging
import re
from dataclasses import replace
from typing import Any

import dropbox
from dropbox import common, files, team
from dropbox import exceptions as dbx_exceptions

from app.dropbox_client.errors import (
    AuthenticationFailureError,
    BlockedPreconditionError,
    CursorResetError,
    DestinationConflictError,
    MissingScopeError,
    PathNotFoundError,
    PermanentDropboxError,
    TemporaryDropboxError,
)
from app.models.config import AuthConfig, JobConfig
from app.models.records import AccountInfo, ListingPage, RemoteEntry, TeamDiscoveryResult, TraversalRoot
from app.utils.paths import (
    namespace_relative_parent,
    namespace_relative_path,
    normalize_dropbox_path,
    parent_path,
    sdk_path,
)
from app.utils.time import isoformat_utc


class DropboxAdapter:
    def __init__(self, auth_config: AuthConfig, logger: logging.Logger, timeout: int = 100) -> None:
        self._auth_config = auth_config
        self._logger = logger
        self._timeout = timeout
        self._client: dropbox.Dropbox | None = None
        self._team_client: dropbox.DropboxTeam | None = None
        self._cursor_clients: dict[str, tuple[dropbox.Dropbox, str | None]] = {}
        self._team_discovery_cache: TeamDiscoveryResult | None = None
        if auth_config.account_mode == "team_admin":
            self._team_client = self._build_team_client(auth_config, timeout)
        else:
            self._client = self._build_user_client(auth_config, timeout)

    def _build_common_args(self, timeout: int) -> dict[str, Any]:
        return {
            "timeout": timeout,
            "max_retries_on_error": 0,
            "max_retries_on_rate_limit": 0,
        }

    def _build_user_client(self, auth_config: AuthConfig, timeout: int) -> dropbox.Dropbox:
        common_args = self._build_common_args(timeout)
        if auth_config.method in ("refresh_token", "oauth_pkce"):
            if not auth_config.refresh_token or not auth_config.app_key:
                raise AuthenticationFailureError("Refresh-token auth requires both an app key and a refresh token.")
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

    def _build_team_client(self, auth_config: AuthConfig, timeout: int) -> dropbox.DropboxTeam:
        common_args = self._build_common_args(timeout)
        if auth_config.method in ("refresh_token", "oauth_pkce"):
            if not auth_config.refresh_token or not auth_config.app_key:
                raise AuthenticationFailureError("Team admin PKCE auth requires a refresh token and app key.")
            return dropbox.DropboxTeam(
                oauth2_refresh_token=auth_config.refresh_token,
                app_key=auth_config.app_key,
                scope=list(auth_config.scopes),
                **common_args,
            )
        if auth_config.method == "access_token":
            if not auth_config.access_token:
                raise AuthenticationFailureError("Access-token auth requires an access token.")
            return dropbox.DropboxTeam(oauth2_access_token=auth_config.access_token, **common_args)
        raise AuthenticationFailureError(f"Unsupported auth method: {auth_config.method}")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
        if self._team_client is not None:
            self._team_client.close()
        for client, _namespace_id in self._cursor_clients.values():
            try:
                client.close()
            except Exception:  # noqa: BLE001
                continue

    def get_current_account(self) -> AccountInfo:
        if self._auth_config.account_mode == "team_admin":
            return self.get_team_discovery().account_info
        try:
            assert self._client is not None
            account = self._client.users_get_current_account()
            return AccountInfo(
                account_id=account.account_id,
                display_name=account.name.display_name,
                email=getattr(account, "email", None),
                account_mode="personal",
            )
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def validate_file_listing_access(self) -> None:
        if self._auth_config.account_mode == "team_admin":
            self.get_team_discovery()
            return
        self.list_folder("/", recursive=False, limit=1)

    def get_team_discovery(self, job_config: JobConfig | None = None) -> TeamDiscoveryResult:
        if self._auth_config.account_mode != "team_admin":
            raise AuthenticationFailureError("Team discovery is only available in team-admin mode.")
        if self._team_discovery_cache is not None:
            return self._team_discovery_cache
        try:
            assert self._team_client is not None
            admin_result = self._team_client.team_token_get_authenticated_admin()
            admin_profile = admin_result.admin_profile
            admin_member_id = self._auth_config.admin_member_id or admin_profile.team_member_id
            team_info = self._team_client.team_get_info()
            admin_client = self._team_client.as_admin(admin_member_id)
            admin_account = admin_client.users_get_current_account()
            feature_values = self._team_client.team_features_get_values([team.Feature.has_team_shared_dropbox])
            has_team_shared_dropbox = False
            if feature_values.values:
                feature_value = feature_values.values[0]
                if feature_value.is_has_team_shared_dropbox():
                    has_team_shared_dropbox = feature_value.get_has_team_shared_dropbox()
            team_model = "team_space" if has_team_shared_dropbox else "team_folders"

            active_members = self._list_active_team_members()
            member_map = {member["member_id"]: member for member in active_members if member["member_id"]}

            root_namespace_id = getattr(admin_account.root_info, "root_namespace_id", None)
            traversal_roots: list[TraversalRoot] = []
            seen_namespaces: set[str] = set()
            if has_team_shared_dropbox and root_namespace_id:
                traversal_roots.append(
                    TraversalRoot(
                        root_key=f"namespace::{root_namespace_id}",
                        root_path="/",
                        account_mode="team_admin",
                        namespace_id=root_namespace_id,
                        namespace_type="team_space",
                        namespace_name=team_info.name,
                        archive_bucket="team_space",
                        canonical_root=namespace_relative_path(root_namespace_id, "/"),
                        include_mounted_folders=False,
                    )
                )
                seen_namespaces.add(root_namespace_id)

            for namespace in self._list_team_namespaces():
                namespace_id = namespace.namespace_id
                if namespace_id in seen_namespaces:
                    continue
                namespace_type = self._namespace_type_name(namespace.namespace_type)
                if namespace_type == "app_folder":
                    continue
                member_details = member_map.get(getattr(namespace, "team_member_id", None))
                if namespace_type == "team_member_folder":
                    if member_details is None:
                        continue
                    archive_bucket = "member_homes"
                    include_mounted = False
                elif namespace_type in ("team_space", "team_folder"):
                    archive_bucket = "team_space"
                    include_mounted = True
                else:
                    archive_bucket = "shared_namespaces"
                    include_mounted = True
                if job_config is not None and job_config.team_coverage_preset == "team_owned_only":
                    if archive_bucket == "member_homes":
                        continue
                traversal_roots.append(
                    TraversalRoot(
                        root_key=f"namespace::{namespace_id}",
                        root_path="/",
                        account_mode="team_admin",
                        namespace_id=namespace_id,
                        namespace_type="team_space" if namespace_type == "team_space" else namespace_type,
                        namespace_name=namespace.name,
                        member_id=member_details["member_id"] if member_details else None,
                        member_email=member_details["member_email"] if member_details else None,
                        member_display_name=member_details["member_display_name"] if member_details else None,
                        archive_bucket=archive_bucket,
                        canonical_root=namespace_relative_path(namespace_id, "/"),
                        include_mounted_folders=include_mounted,
                    )
                )
                seen_namespaces.add(namespace_id)

            account_info = AccountInfo(
                account_id=admin_account.account_id,
                display_name=admin_account.name.display_name,
                email=getattr(admin_account, "email", None),
                account_mode="team_admin",
                team_member_id=admin_member_id,
                team_id=team_info.team_id,
                team_name=team_info.name,
                team_model=team_model,
                active_member_count=len(active_members),
                namespace_count=len(traversal_roots),
            )
            self._team_discovery_cache = TeamDiscoveryResult(
                account_info=account_info,
                traversal_roots=traversal_roots,
                team_model=team_model,
                root_namespace_id=root_namespace_id,
            )
            return self._team_discovery_cache
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def prepare_archive_destination(self, discovery: TeamDiscoveryResult, archive_root: str, create: bool) -> TeamDiscoveryResult:
        if self._auth_config.account_mode != "team_admin":
            return discovery
        archive_name = normalize_dropbox_path(archive_root).strip("/")
        if not archive_name:
            raise BlockedPreconditionError("Team-admin mode requires a dedicated non-root archive folder.")
        if discovery.team_model == "team_space":
            if not discovery.root_namespace_id:
                raise BlockedPreconditionError("Team space root namespace was not returned by Dropbox.")
            archive_path = namespace_relative_path(discovery.root_namespace_id, archive_root)
            if create:
                self.create_folder_if_missing(archive_path)
            else:
                metadata = self.get_metadata(archive_path)
                if metadata is None:
                    return replace(
                        discovery,
                        archive_namespace_id=discovery.root_namespace_id,
                        archive_provisioned=False,
                        archive_status_detail=f"Archive root {archive_root} does not exist yet.",
                    )
            return replace(
                discovery,
                archive_namespace_id=discovery.root_namespace_id,
                archive_provisioned=True,
                archive_status_detail=f"Using central archive at {archive_root} in the team space.",
            )

        archive_namespace = self._find_legacy_archive_namespace(archive_name)
        if archive_namespace is None and create:
            try:
                assert self._team_client is not None
                self._team_client.team_team_folder_create(archive_name)
            except Exception as exc:  # noqa: BLE001
                self._raise_mapped(exc)
            archive_namespace = self._find_legacy_archive_namespace(archive_name)
        if archive_namespace is None:
            return replace(
                discovery,
                archive_namespace_id=None,
                archive_provisioned=False,
                archive_status_detail=(
                    f"Legacy team archive folder {archive_root} was not found. "
                    "Run a real copy job to provision it, or create it as a team folder first."
                ),
            )
        return replace(
            discovery,
            archive_namespace_id=archive_namespace["namespace_id"],
            archive_provisioned=True,
            archive_status_detail=f"Using legacy team archive namespace {archive_namespace['namespace_name']}.",
        )

    def list_folder(
        self,
        path: str,
        recursive: bool,
        limit: int,
        *,
        include_mounted_folders: bool = True,
        namespace_id: str | None = None,
    ) -> ListingPage:
        try:
            client = self._listing_client(namespace_id)
            result = client.files_list_folder(
                sdk_path(path),
                recursive=recursive,
                limit=limit,
                include_deleted=False,
                include_non_downloadable_files=True,
                include_mounted_folders=include_mounted_folders,
            )
            self._cursor_clients[result.cursor] = (client, namespace_id)
            return self._map_listing_page(result, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def list_folder_continue(self, cursor: str) -> ListingPage:
        try:
            client_info = self._cursor_clients.get(cursor)
            if client_info is None:
                client = self._default_listing_continue_client()
                namespace_id = None
            else:
                client, namespace_id = client_info
            result = client.files_list_folder_continue(cursor)
            self._cursor_clients[result.cursor] = (client, namespace_id)
            return self._map_listing_page(result, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def get_metadata(self, path: str) -> RemoteEntry | None:
        try:
            client = self._metadata_client(path)
            metadata = client.files_get_metadata(path if path.startswith("id:") or path.startswith("rev:") or path.startswith("ns:") else sdk_path(path))
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, PathNotFoundError):
                return None
            raise mapped from exc
        namespace_id = self._namespace_id_from_path(path)
        return self._map_entry(metadata, namespace_id=namespace_id)

    def create_folder_if_missing(self, path: str) -> RemoteEntry | None:
        try:
            client = self._metadata_client(path)
            target = path if path.startswith("ns:") else sdk_path(path)
            result = client.files_create_folder_v2(target, autorename=False)
            namespace_id = self._namespace_id_from_path(path)
            return self._map_entry(result.metadata, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, DestinationConflictError):
                existing = self.get_metadata(path)
                if existing and existing.item_type == "folder":
                    return existing
            raise mapped from exc

    def copy_file(self, source_path: str, destination_path: str, member_id: str | None = None) -> RemoteEntry:
        try:
            client = self._copy_client(admin=True)
            result = client.files_copy_v2(
                source_path if source_path.startswith("ns:") else sdk_path(source_path),
                destination_path if destination_path.startswith("ns:") else sdk_path(destination_path),
                autorename=False,
            )
            namespace_id = self._namespace_id_from_path(destination_path)
            return self._map_entry(result.metadata, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if member_id and isinstance(mapped, (AuthenticationFailureError, PathNotFoundError)):
                try:
                    client = self._copy_client(admin=False, member_id=member_id)
                    result = client.files_copy_v2(
                        source_path if source_path.startswith("ns:") else sdk_path(source_path),
                        destination_path if destination_path.startswith("ns:") else sdk_path(destination_path),
                        autorename=False,
                    )
                    namespace_id = self._namespace_id_from_path(destination_path)
                    return self._map_entry(result.metadata, namespace_id=namespace_id)
                except Exception as user_exc:  # noqa: BLE001
                    self._raise_mapped(user_exc)
            raise mapped from exc

    def _list_active_team_members(self) -> list[dict[str, str | None]]:
        assert self._team_client is not None
        result = self._team_client.team_members_list_v2(limit=1000, include_removed=False)
        members = self._map_members(result.members)
        while result.has_more:
            result = self._team_client.team_members_list_continue_v2(result.cursor)
            members.extend(self._map_members(result.members))
        return [member for member in members if member["status"] == "active"]

    def _map_members(self, members: list[Any]) -> list[dict[str, str | None]]:
        mapped: list[dict[str, str | None]] = []
        for member_info in members:
            profile = member_info.profile
            status = getattr(profile, "status", None)
            status_name = "active"
            if status is not None:
                if hasattr(status, "is_active") and status.is_active():
                    status_name = "active"
                elif hasattr(status, "is_suspended") and status.is_suspended():
                    status_name = "suspended"
                elif hasattr(status, "is_removed") and status.is_removed():
                    status_name = "removed"
                else:
                    status_name = "invited"
            mapped.append(
                {
                    "member_id": getattr(profile, "team_member_id", None),
                    "member_email": getattr(profile, "email", None),
                    "member_display_name": getattr(profile.name, "display_name", None) if getattr(profile, "name", None) else None,
                    "member_folder_id": getattr(profile, "member_folder_id", None),
                    "status": status_name,
                }
            )
        return mapped

    def _list_team_namespaces(self) -> list[Any]:
        assert self._team_client is not None
        result = self._team_client.team_namespaces_list(limit=1000)
        namespaces = list(result.namespaces)
        while result.has_more:
            result = self._team_client.team_namespaces_list_continue(result.cursor)
            namespaces.extend(result.namespaces)
        return namespaces

    def _find_legacy_archive_namespace(self, archive_name: str) -> dict[str, str] | None:
        for namespace in self._list_team_namespaces():
            namespace_type = self._namespace_type_name(namespace.namespace_type)
            if namespace_type not in ("team_folder", "shared_folder"):
                continue
            if namespace.name == archive_name:
                return {"namespace_id": namespace.namespace_id, "namespace_name": namespace.name}
        return None

    def _namespace_type_name(self, namespace_type: Any) -> str:
        if hasattr(namespace_type, "is_team_member_folder") and namespace_type.is_team_member_folder():
            return "team_member_folder"
        if hasattr(namespace_type, "is_team_folder") and namespace_type.is_team_folder():
            return "team_folder"
        if hasattr(namespace_type, "is_shared_folder") and namespace_type.is_shared_folder():
            return "shared_folder"
        if hasattr(namespace_type, "is_app_folder") and namespace_type.is_app_folder():
            return "app_folder"
        return "team_space"

    def _listing_client(self, namespace_id: str | None) -> dropbox.Dropbox:
        if self._auth_config.account_mode != "team_admin":
            assert self._client is not None
            return self._client
        discovery = self.get_team_discovery()
        admin_member_id = discovery.account_info.team_member_id
        if admin_member_id is None:
            raise BlockedPreconditionError("Dropbox did not return an authenticated admin member ID.")
        assert self._team_client is not None
        client = self._team_client.as_admin(admin_member_id)
        if namespace_id:
            client = client.with_path_root(common.PathRoot.root(namespace_id))
        return client

    def _default_listing_continue_client(self) -> dropbox.Dropbox:
        return self._listing_client(None)

    def _metadata_client(self, path: str) -> dropbox.Dropbox:
        if self._auth_config.account_mode != "team_admin":
            assert self._client is not None
            return self._client
        return self._copy_client(admin=True)

    def _copy_client(self, *, admin: bool, member_id: str | None = None) -> dropbox.Dropbox:
        if self._auth_config.account_mode != "team_admin":
            assert self._client is not None
            return self._client
        discovery = self.get_team_discovery()
        assert self._team_client is not None
        if admin:
            admin_member_id = self._auth_config.admin_member_id or discovery.account_info.team_member_id
            if not admin_member_id:
                raise BlockedPreconditionError("Dropbox did not return an authenticated admin member ID.")
            return self._team_client.as_admin(admin_member_id)
        if not member_id:
            raise BlockedPreconditionError("A team member ID is required when acting as a selected user.")
        return self._team_client.as_user(member_id)

    def _map_listing_page(self, result: Any, *, namespace_id: str | None) -> ListingPage:
        return ListingPage(
            entries=[entry for entry in (self._map_entry(item, namespace_id=namespace_id) for item in result.entries) if entry is not None],
            cursor=result.cursor,
            has_more=result.has_more,
        )

    def _map_entry(self, entry: Any, *, namespace_id: str | None) -> RemoteEntry | None:
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
                account_mode=self._auth_config.account_mode,
                namespace_id=namespace_id,
                canonical_source_path=namespace_relative_path(namespace_id, path_display),
                canonical_parent_path=namespace_relative_parent(namespace_relative_path(namespace_id, path_display)),
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
                account_mode=self._auth_config.account_mode,
                namespace_id=namespace_id,
                canonical_source_path=namespace_relative_path(namespace_id, path_display),
                canonical_parent_path=namespace_relative_parent(namespace_relative_path(namespace_id, path_display)),
            )
        return None

    def _namespace_id_from_path(self, path: str) -> str | None:
        if path.startswith("ns:"):
            payload = path[3:]
            return payload.split("/", 1)[0]
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
