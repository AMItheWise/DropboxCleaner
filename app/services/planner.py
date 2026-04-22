from __future__ import annotations

from dataclasses import dataclass, field

from app.models.config import AccountMode
from app.models.records import TeamDiscoveryResult
from app.utils.paths import (
    is_same_or_descendant,
    join_dropbox_path,
    namespace_relative_path,
    normalize_dropbox_path,
    slugify_path_component,
)


@dataclass(slots=True)
class ArchivePlanner:
    archive_root: str
    exclude_archive_destination: bool = True
    account_mode: AccountMode = "personal"
    excluded_roots: list[str] = field(default_factory=list)
    team_discovery: TeamDiscoveryResult | None = None

    def __post_init__(self) -> None:
        self.archive_root = normalize_dropbox_path(self.archive_root)
        self.excluded_roots = [normalize_dropbox_path(path) for path in self.excluded_roots if path and path.strip()]

    def with_team_discovery(self, team_discovery: TeamDiscoveryResult) -> "ArchivePlanner":
        self.team_discovery = team_discovery
        self.account_mode = "team_admin"
        return self

    def is_excluded_from_sources(self, path: str) -> bool:
        if self.is_user_excluded(path):
            return True
        if not self.exclude_archive_destination:
            return False
        if self.account_mode == "team_admin":
            # Team-admin inventory works namespace-by-namespace. Archive exclusion is handled
            # by not traversing the dedicated archive namespace root as a source namespace.
            return False
        return is_same_or_descendant(path, self.archive_root)

    def is_user_excluded(self, path: str) -> bool:
        if not self.excluded_roots:
            return False
        return any(is_same_or_descendant(path, excluded_root) for excluded_root in self.excluded_roots)

    def map_to_archive_path(
        self,
        original_path: str,
        *,
        archive_bucket: str = "personal",
        member_email: str | None = None,
        member_id: str | None = None,
        namespace_name: str | None = None,
        namespace_id: str | None = None,
    ) -> str:
        original_path = normalize_dropbox_path(original_path)
        if self.account_mode != "team_admin":
            if self.archive_root == "/":
                raise ValueError("Archive root cannot be /. Use a dedicated top-level folder.")
            return join_dropbox_path(self.archive_root, original_path)

        if archive_bucket == "team_space":
            return join_dropbox_path(self.archive_root, "team_space", original_path)
        if archive_bucket == "member_homes":
            member_slug = slugify_path_component(member_email, member_id or "member")
            return join_dropbox_path(self.archive_root, "member_homes", member_slug, original_path)
        namespace_slug = slugify_path_component(namespace_name, namespace_id or "namespace")
        return join_dropbox_path(self.archive_root, "shared_namespaces", namespace_slug, original_path)

    def build_archive_canonical_path(
        self,
        display_archive_path: str,
        *,
        archive_bucket: str,
        namespace_id: str | None,
    ) -> str | None:
        display_archive_path = normalize_dropbox_path(display_archive_path)
        if self.account_mode != "team_admin":
            return display_archive_path
        if self.team_discovery is None or self.team_discovery.archive_namespace_id is None:
            return None
        if self.team_discovery.team_model == "team_space":
            relative_inside_archive = display_archive_path.removeprefix(self.archive_root)
            relative_inside_archive = normalize_dropbox_path(relative_inside_archive or "/")
            archive_base = normalize_dropbox_path(self.team_discovery.archive_namespace_root_path)
            target_relative_path = (
                relative_inside_archive
                if archive_base == "/"
                else join_dropbox_path(archive_base, relative_inside_archive)
            )
            return namespace_relative_path(self.team_discovery.archive_namespace_id, target_relative_path)
        relative_inside_archive = display_archive_path.removeprefix(self.archive_root)
        relative_inside_archive = normalize_dropbox_path(relative_inside_archive or "/")
        return namespace_relative_path(self.team_discovery.archive_namespace_id, relative_inside_archive)
