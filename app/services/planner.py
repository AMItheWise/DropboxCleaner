from __future__ import annotations

from dataclasses import dataclass

from app.utils.paths import is_same_or_descendant, normalize_dropbox_path, planned_archive_path


@dataclass(slots=True)
class ArchivePlanner:
    archive_root: str
    exclude_archive_destination: bool = True

    def __post_init__(self) -> None:
        self.archive_root = normalize_dropbox_path(self.archive_root)

    def is_excluded_from_sources(self, path: str) -> bool:
        if not self.exclude_archive_destination:
            return False
        return is_same_or_descendant(path, self.archive_root)

    def map_to_archive_path(self, original_path: str) -> str:
        return planned_archive_path(self.archive_root, original_path)
