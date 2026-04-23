from __future__ import annotations

import logging
import re
import time
from dataclasses import replace
from typing import Any

import dropbox
from dropbox import common, files, sharing, team
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
    split_namespace_relative_path,
)
from app.utils.time import isoformat_utc


def path_root_for_namespace(namespace_id: str, root_namespace_id: str | None) -> common.PathRoot:
    if namespace_id == root_namespace_id:
        return common.PathRoot.root(namespace_id)
    return common.PathRoot.namespace_id(namespace_id)


def filter_team_discovery_for_job(discovery: TeamDiscoveryResult, job_config: JobConfig | None) -> TeamDiscoveryResult:
    if job_config is None or job_config.team_coverage_preset != "team_owned_only":
        return discovery
    traversal_roots = [
        root
        for root in discovery.traversal_roots
        if root.archive_bucket != "member_homes" and root.namespace_type != "team_member_folder"
    ]
    return replace(
        discovery,
        traversal_roots=traversal_roots,
        account_info=replace(discovery.account_info, namespace_count=len(traversal_roots)),
    )


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
            return filter_team_discovery_for_job(self._team_discovery_cache, job_config)
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
            return filter_team_discovery_for_job(self._team_discovery_cache, job_config)
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
            archive_namespace_id, archive_relative_path, archive_location_label = self._team_space_archive_location(
                discovery,
                archive_root,
            )
            archive_path = namespace_relative_path(archive_namespace_id, archive_relative_path)
            metadata = self.get_metadata(archive_path)
            if metadata is not None:
                if metadata.item_type != "folder":
                    return replace(
                        discovery,
                        archive_namespace_id=None,
                        archive_provisioned=False,
                        archive_status_detail=f"Archive path {archive_root} already exists but is not a folder.",
                    )
                return self._finalize_team_space_archive_destination(
                    discovery,
                    archive_root=archive_root,
                    archive_path=archive_path,
                    archive_namespace_id=archive_namespace_id,
                    archive_relative_path=archive_relative_path,
                    archive_location_label=archive_location_label,
                    create=create,
                    reused=True,
                )
            if not create:
                return replace(
                    discovery,
                    archive_namespace_id=archive_namespace_id,
                    archive_namespace_root_path=archive_relative_path,
                    archive_provisioned=False,
                    archive_status_detail=f"Archive root {archive_root} does not exist yet.",
                )
            try:
                self.create_folder_if_missing(archive_path)
            except BlockedPreconditionError as exc:
                detail = self._archive_write_blocked_detail(archive_root, exc)
                self._logger.warning(detail, extra={"phase": "team_discovery"})
                return replace(
                    discovery,
                    archive_namespace_id=archive_namespace_id,
                    archive_namespace_root_path=archive_relative_path,
                    archive_provisioned=False,
                    archive_status_detail=detail,
                )
            return self._finalize_team_space_archive_destination(
                discovery,
                archive_root=archive_root,
                archive_path=archive_path,
                archive_namespace_id=archive_namespace_id,
                archive_relative_path=archive_relative_path,
                archive_location_label=archive_location_label,
                create=create,
                reused=False,
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

    def list_folder_continue(self, cursor: str, *, namespace_id: str | None = None) -> ListingPage:
        try:
            client_info = self._cursor_clients.get(cursor)
            if client_info is None:
                client = self._listing_client(namespace_id)
            else:
                client, namespace_id = client_info
            result = client.files_list_folder_continue(cursor)
            self._cursor_clients[result.cursor] = (client, namespace_id)
            return self._map_listing_page(result, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            self._raise_mapped(exc)

    def get_metadata(self, path: str) -> RemoteEntry | None:
        try:
            client, target, namespace_id = self._metadata_client_and_target(path)
            metadata = client.files_get_metadata(target)
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, PathNotFoundError):
                return None
            raise mapped from exc
        return self._map_entry(metadata, namespace_id=namespace_id)

    def create_folder_if_missing(self, path: str) -> RemoteEntry | None:
        try:
            client, target, namespace_id = self._metadata_client_and_target(path)
            result = client.files_create_folder_v2(target, autorename=False)
            return self._map_entry(result.metadata, namespace_id=namespace_id)
        except Exception as exc:  # noqa: BLE001
            mapped = self._map_exception(exc)
            if isinstance(mapped, DestinationConflictError):
                existing = self.get_metadata(path)
                if existing and existing.item_type == "folder":
                    return existing
            raise mapped from exc

    def copy_file(
        self,
        source_path: str,
        destination_path: str,
        member_id: str | None = None,
        *,
        source_display_path: str | None = None,
        destination_display_path: str | None = None,
    ) -> RemoteEntry:
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
            if member_id and self._should_retry_copy_as_member(mapped):
                self._logger.info(
                    "Admin-context copy was denied for %s -> %s. Retrying as team member %s.",
                    source_path,
                    destination_path,
                    member_id,
                    extra={"phase": "copy"},
                )
                try:
                    client = self._copy_client(admin=False, member_id=member_id)
                    result = client.files_copy_v2(
                        source_display_path if source_display_path is not None else source_path if source_path.startswith("ns:") else sdk_path(source_path),
                        destination_display_path
                        if destination_display_path is not None
                        else destination_path
                        if destination_path.startswith("ns:")
                        else sdk_path(destination_path),
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

    def _finalize_team_space_archive_destination(
        self,
        discovery: TeamDiscoveryResult,
        *,
        archive_root: str,
        archive_path: str,
        archive_namespace_id: str,
        archive_relative_path: str,
        archive_location_label: str,
        create: bool,
        reused: bool,
    ) -> TeamDiscoveryResult:
        action = "existing central archive" if reused else "central archive"
        detail = f"Using {action} at {archive_root} in {archive_location_label}."
        if not create:
            return replace(
                discovery,
                archive_namespace_id=archive_namespace_id,
                archive_namespace_root_path=archive_relative_path,
                archive_provisioned=True,
                archive_status_detail=detail,
            )
        try:
            shared_folder_id, member_count = self._share_archive_with_member_home_sources(archive_path, discovery)
        except MissingScopeError:
            raise
        except Exception as exc:  # noqa: BLE001
            blocked_detail = (
                f"{detail} Dropbox refused to grant source members editor access to the archive folder. "
                "Member-home server-side copies require the source member to be able to write to the archive folder. "
                "Grant the active source members editor access to the archive folder in Dropbox, then rerun or resume. "
                f"{exc}"
            )
            self._logger.warning(blocked_detail, extra={"phase": "team_discovery"})
            return replace(
                discovery,
                archive_namespace_id=archive_namespace_id,
                archive_namespace_root_path=archive_relative_path,
                archive_provisioned=False,
                archive_status_detail=blocked_detail,
            )
        if shared_folder_id:
            return replace(
                discovery,
                archive_namespace_id=shared_folder_id,
                archive_namespace_root_path="/",
                archive_shared_folder_id=shared_folder_id,
                archive_provisioned=True,
                archive_status_detail=f"{detail} Shared archive write access with {member_count} active member-home source member(s).",
            )
        return replace(
            discovery,
            archive_namespace_id=archive_namespace_id,
            archive_namespace_root_path=archive_relative_path,
            archive_provisioned=True,
            archive_status_detail=detail,
        )

    def _share_archive_with_member_home_sources(self, archive_path: str, discovery: TeamDiscoveryResult) -> tuple[str | None, int]:
        members = sorted(
            {
                (root.member_id, root.member_email)
                for root in discovery.traversal_roots
                if root.archive_bucket == "member_homes" and root.member_email
            }
        )
        if not members:
            return None, 0
        shared_folder_id = self._ensure_shared_folder_id(archive_path)
        self._add_archive_folder_editors(shared_folder_id, members)
        return shared_folder_id, len(members)

    def _ensure_shared_folder_id(self, archive_path: str) -> str:
        metadata = self.get_metadata(archive_path)
        if metadata and metadata.shared_folder_id:
            return metadata.shared_folder_id
        client, target, _namespace_id = self._metadata_client_and_target(archive_path)
        try:
            launch = client.sharing_share_folder(target, force_async=False)
        except Exception as exc:  # noqa: BLE001
            if "already" in str(exc).casefold():
                metadata = self.get_metadata(archive_path)
                if metadata and metadata.shared_folder_id:
                    return metadata.shared_folder_id
            mapped = self._map_exception(exc)
            if isinstance(mapped, MissingScopeError):
                raise mapped from exc
            raise BlockedPreconditionError(f"Could not share archive folder {archive_path}: {mapped}") from exc
        return self._shared_folder_id_from_launch(client, launch)

    def _shared_folder_id_from_launch(self, client: dropbox.Dropbox, launch: Any) -> str:
        if launch.is_complete():
            return launch.get_complete().shared_folder_id
        if launch.is_async_job_id():
            async_job_id = launch.get_async_job_id()
            for _attempt in range(30):
                status = client.sharing_check_share_job_status(async_job_id)
                if status.is_complete():
                    return status.get_complete().shared_folder_id
                if status.is_failed():
                    raise BlockedPreconditionError(f"Dropbox failed to share the archive folder: {status.get_failed()}")
                time.sleep(1)
        raise BlockedPreconditionError("Dropbox did not finish sharing the archive folder in time.")

    def _add_archive_folder_editors(self, shared_folder_id: str, members: list[tuple[str | None, str | None]]) -> None:
        client = self._copy_client(admin=True)
        for member_id, email in members:
            if not email:
                continue
            try:
                client.sharing_add_folder_member(
                    shared_folder_id,
                    [
                        sharing.AddMember(
                            member=sharing.MemberSelector.email(email),
                            access_level=sharing.AccessLevel.editor,
                        )
                    ],
                    quiet=True,
                )
            except Exception as exc:  # noqa: BLE001
                lowered = str(exc).casefold()
                if "already" in lowered:
                    continue
                mapped = self._map_exception(exc)
                if isinstance(mapped, MissingScopeError):
                    raise mapped from exc
                raise BlockedPreconditionError(
                    f"Could not grant {email} editor access to archive folder {shared_folder_id}: {mapped}"
                ) from exc
            if member_id:
                self._mount_archive_folder_for_member(shared_folder_id, member_id, email)
            else:
                self._logger.warning(
                    "Could not mount archive folder %s for %s because Dropbox did not return a team member ID.",
                    shared_folder_id,
                    email,
                    extra={"phase": "team_discovery"},
                )

    def _mount_archive_folder_for_member(self, shared_folder_id: str, member_id: str, email: str) -> None:
        client = self._copy_client(admin=False, member_id=member_id)
        try:
            client.sharing_mount_folder(shared_folder_id)
            self._logger.info(
                "Mounted archive folder %s for %s.",
                shared_folder_id,
                email,
                extra={"phase": "team_discovery"},
            )
        except Exception as exc:  # noqa: BLE001
            lowered = str(exc).casefold()
            if "already_mounted" in lowered or "already mounted" in lowered:
                return
            mapped = self._map_exception(exc)
            if isinstance(mapped, MissingScopeError):
                raise mapped from exc
            raise BlockedPreconditionError(
                f"Could not mount archive folder {shared_folder_id} for {email}: {mapped}"
            ) from exc

    def _team_space_archive_location(self, discovery: TeamDiscoveryResult, archive_root: str) -> tuple[str, str, str]:
        if not discovery.root_namespace_id:
            raise BlockedPreconditionError("Team space root namespace was not returned by Dropbox.")
        normalized = normalize_dropbox_path(archive_root)
        parts = normalized.strip("/").split("/", 1)
        top_level_name = parts[0] if parts and parts[0] else ""
        if top_level_name:
            matches = [
                root
                for root in discovery.traversal_roots
                if root.namespace_id
                and root.namespace_id != discovery.root_namespace_id
                and root.namespace_name
                and root.namespace_name.casefold() == top_level_name.casefold()
            ]
            if len(matches) == 1:
                relative_path = normalize_dropbox_path(parts[1] if len(parts) > 1 else "/")
                return matches[0].namespace_id or discovery.root_namespace_id, relative_path, f"mounted namespace {matches[0].namespace_name}"
            if len(matches) > 1:
                raise BlockedPreconditionError(
                    f"Archive root {normalized} starts with {top_level_name}, but multiple team namespaces have that name. "
                    "Choose a unique archive folder path or rename the duplicate team folders."
                )
        return discovery.root_namespace_id, normalized, "the team space"

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
            client = client.with_path_root(path_root_for_namespace(namespace_id, discovery.root_namespace_id))
        return client

    def _metadata_client(self, path: str) -> dropbox.Dropbox:
        if self._auth_config.account_mode != "team_admin":
            assert self._client is not None
            return self._client
        return self._copy_client(admin=True)

    def _metadata_client_and_target(self, path: str) -> tuple[dropbox.Dropbox, str, str | None]:
        if path.startswith("id:") or path.startswith("rev:"):
            return self._metadata_client(path), path, None
        namespace_id, relative_path = split_namespace_relative_path(path)
        client = self._metadata_client(path)
        if self._auth_config.account_mode == "team_admin" and namespace_id:
            discovery = self.get_team_discovery()
            client = client.with_path_root(path_root_for_namespace(namespace_id, discovery.root_namespace_id))
        target = namespace_relative_path(namespace_id, relative_path) if namespace_id and relative_path == "/" else sdk_path(relative_path)
        return client, target, namespace_id

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
                shared_folder_id=getattr(entry, "shared_folder_id", None),
            )
        return None

    def _namespace_id_from_path(self, path: str) -> str | None:
        if path.startswith("ns:"):
            payload = path[3:]
            return payload.split("/", 1)[0]
        return None

    def _should_retry_copy_as_member(self, mapped_error: Exception) -> bool:
        return isinstance(mapped_error, (AuthenticationFailureError, PathNotFoundError, BlockedPreconditionError))

    def _raise_mapped(self, exc: Exception) -> None:
        raise self._map_exception(exc) from exc

    def _map_exception(self, exc: Exception) -> Exception:
        message = getattr(exc, "message", str(exc))
        lowered = message.casefold()
        missing_scope = self._extract_required_scope(message)
        if "no_write_permission" in lowered:
            return BlockedPreconditionError(
                "Dropbox denied write permission for the requested archive path. "
                "Create or choose an archive folder where the authenticated admin/app has editor access, then resume. "
                f"Dropbox error: {message}"
            )
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

    def _archive_write_blocked_detail(self, archive_root: str, exc: Exception) -> str:
        return (
            f"Dropbox denied write permission while creating {normalize_dropbox_path(archive_root)} in the team space. "
            "This is usually a Dropbox team-space policy or folder-permission setting. "
            "Create that archive folder manually in Dropbox, or choose an existing team-space folder where the authenticated admin/app has editor access, then rerun or resume. "
            "Original files were not changed. "
            f"{exc}"
        )
