from __future__ import annotations

from pathlib import PurePosixPath


def normalize_dropbox_path(path: str | None) -> str:
    if path is None:
        return "/"
    value = path.strip().replace("\\", "/")
    if not value or value == "/":
        return "/"
    if not value.startswith("/"):
        value = f"/{value}"
    while "//" in value:
        value = value.replace("//", "/")
    if len(value) > 1 and value.endswith("/"):
        value = value.rstrip("/")
    return value or "/"


def sdk_path(path: str) -> str:
    normalized = normalize_dropbox_path(path)
    return "" if normalized == "/" else normalized


def path_key(path: str) -> str:
    return normalize_dropbox_path(path).casefold()


def join_dropbox_path(*parts: str) -> str:
    normalized_parts = [normalize_dropbox_path(part).strip("/") for part in parts if part is not None]
    joined = "/".join([part for part in normalized_parts if part])
    return f"/{joined}" if joined else "/"


def parent_path(path: str) -> str:
    normalized = normalize_dropbox_path(path)
    if normalized == "/":
        return "/"
    parent = str(PurePosixPath(normalized).parent)
    return normalize_dropbox_path(parent)


def is_same_or_descendant(path: str, ancestor: str) -> bool:
    target = normalize_dropbox_path(path)
    root = normalize_dropbox_path(ancestor)
    if root == "/":
        return True
    if target.casefold() == root.casefold():
        return True
    return target.casefold().startswith(f"{root.casefold()}/")


def dedupe_source_roots(paths: list[str]) -> tuple[list[str], list[str]]:
    normalized = [normalize_dropbox_path(path) for path in paths if path and path.strip()]
    if not normalized:
        return [], []
    deduped: list[str] = []
    ignored: list[str] = []
    for root in normalized:
        if root == "/":
            ignored.extend([path for path in normalized if path != "/"])
            return ["/"], ignored
    for root in normalized:
        if any(is_same_or_descendant(root, existing) for existing in deduped):
            ignored.append(root)
            continue
        deduped = [existing for existing in deduped if not is_same_or_descendant(existing, root)]
        deduped.append(root)
    return deduped, ignored


def planned_archive_path(archive_root: str, original_path: str) -> str:
    archive_root = normalize_dropbox_path(archive_root)
    original_path = normalize_dropbox_path(original_path)
    if archive_root == "/":
        raise ValueError("Archive root cannot be /. Use a dedicated top-level folder.")
    if original_path == "/":
        return archive_root
    return join_dropbox_path(archive_root, original_path)
