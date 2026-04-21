from __future__ import annotations

import re
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


def namespace_relative_path(namespace_id: str | None, path: str) -> str:
    normalized = normalize_dropbox_path(path)
    if not namespace_id:
        return normalized
    if normalized == "/":
        return f"ns:{namespace_id}"
    return f"ns:{namespace_id}{normalized}"


def is_namespace_relative_path(path: str) -> bool:
    return path.startswith("ns:")


def namespace_relative_parent(path: str) -> str:
    if not is_namespace_relative_path(path):
        return parent_path(path)
    namespace_id, relative_path = split_namespace_relative_path(path)
    return namespace_relative_path(namespace_id, parent_path(relative_path))


def split_namespace_relative_path(path: str) -> tuple[str | None, str]:
    if not is_namespace_relative_path(path):
        return None, normalize_dropbox_path(path)
    payload = path[3:]
    if "/" not in payload:
        return payload, "/"
    namespace_id, remainder = payload.split("/", 1)
    return namespace_id, normalize_dropbox_path(f"/{remainder}")


def slugify_path_component(value: str | None, fallback: str) -> str:
    if value:
        normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().lower()).strip("-")
        if normalized:
            return normalized
    return re.sub(r"[^A-Za-z0-9._-]+", "-", fallback.strip().lower()).strip("-") or "unknown"
