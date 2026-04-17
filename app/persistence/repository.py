from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from app.models.config import AuthConfig, JobConfig, RunContext
from app.models.records import CopyJobRecord, FolderSummary, InventoryRecord, MatchedFileRecord
from app.utils.time import isoformat_utc, utc_now


class RunStateRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _initialize_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    status TEXT NOT NULL,
                    base_output_dir TEXT NOT NULL,
                    run_dir TEXT NOT NULL,
                    state_db_path TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    auth_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS inventory_checkpoints (
                    run_id TEXT NOT NULL,
                    root_path TEXT NOT NULL,
                    cursor TEXT,
                    completed INTEGER NOT NULL DEFAULT 0,
                    page_count INTEGER NOT NULL DEFAULT 0,
                    item_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, root_path),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS inventory_items (
                    run_id TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    full_path TEXT NOT NULL,
                    path_lower TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    parent_path TEXT NOT NULL,
                    dropbox_id TEXT NOT NULL,
                    size INTEGER,
                    server_modified TEXT,
                    client_modified TEXT,
                    content_hash TEXT,
                    root_scope_used TEXT NOT NULL,
                    inventory_timestamp TEXT NOT NULL,
                    PRIMARY KEY (run_id, item_type, path_lower),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_inventory_items_run_item_type
                ON inventory_items(run_id, item_type);

                CREATE INDEX IF NOT EXISTS idx_inventory_items_run_parent
                ON inventory_items(run_id, parent_path);

                CREATE TABLE IF NOT EXISTS matched_files (
                    run_id TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    path_lower TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    dropbox_id TEXT NOT NULL,
                    size INTEGER,
                    server_modified TEXT,
                    client_modified TEXT,
                    content_hash TEXT,
                    planned_archive_path TEXT NOT NULL,
                    match_reason TEXT NOT NULL,
                    filter_timestamp TEXT NOT NULL,
                    parent_path TEXT NOT NULL,
                    PRIMARY KEY (run_id, original_path),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_matched_files_run_parent
                ON matched_files(run_id, parent_path);

                CREATE TABLE IF NOT EXISTS copy_jobs (
                    run_id TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    archive_path TEXT NOT NULL,
                    dropbox_id TEXT NOT NULL,
                    size INTEGER,
                    server_modified TEXT,
                    client_modified TEXT,
                    content_hash TEXT,
                    status TEXT NOT NULL,
                    status_detail TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    first_attempt_at TEXT,
                    last_attempt_at TEXT,
                    filename TEXT NOT NULL,
                    parent_path TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    PRIMARY KEY (run_id, original_path),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_copy_jobs_run_status
                ON copy_jobs(run_id, status);

                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                """
            )
            self._conn.commit()

    def create_run(self, run_context: RunContext, job_config: JobConfig, auth_config: AuthConfig) -> None:
        payload = {
            "source_roots": job_config.source_roots,
            "cutoff_date": job_config.cutoff_date,
            "archive_root": job_config.archive_root,
            "batch_size": job_config.batch_size,
            "conflict_policy": job_config.conflict_policy,
            "include_folders_in_inventory": job_config.include_folders_in_inventory,
            "exclude_archive_destination": job_config.exclude_archive_destination,
            "worker_count": job_config.worker_count,
            "verify_after_run": job_config.verify_after_run,
        }
        auth_payload = {
            "method": auth_config.method,
            "app_key": auth_config.app_key,
            "scopes": list(auth_config.scopes),
        }
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO runs (
                    run_id, created_at, updated_at, mode, phase, status,
                    base_output_dir, run_dir, state_db_path, config_json, auth_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_context.run_id,
                    run_context.created_at,
                    run_context.created_at,
                    run_context.mode,
                    "created",
                    "running",
                    str(run_context.output_paths.base_output_dir),
                    str(run_context.output_paths.run_dir),
                    str(run_context.output_paths.state_db),
                    json.dumps(payload),
                    json.dumps(auth_payload),
                ),
            )
            self._conn.commit()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    def get_latest_run(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def update_run_phase(self, run_id: str, phase: str, status: str | None = None) -> None:
        with self._lock:
            if status is None:
                self._conn.execute(
                    "UPDATE runs SET phase = ?, updated_at = ? WHERE run_id = ?",
                    (phase, isoformat_utc(utc_now()), run_id),
                )
            else:
                self._conn.execute(
                    "UPDATE runs SET phase = ?, status = ?, updated_at = ? WHERE run_id = ?",
                    (phase, status, isoformat_utc(utc_now()), run_id),
                )
            self._conn.commit()

    def finish_run(self, run_id: str, status: str) -> None:
        self.update_run_phase(run_id, "completed", status)

    def record_event(
        self,
        run_id: str,
        phase: str,
        level: str,
        event_type: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO job_events (run_id, event_time, phase, level, event_type, message, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    isoformat_utc(utc_now()),
                    phase,
                    level,
                    event_type,
                    message,
                    json.dumps(payload or {}),
                ),
            )
            self._conn.commit()

    def save_inventory_checkpoint(
        self,
        run_id: str,
        root_path: str,
        *,
        cursor: str | None,
        completed: bool,
        page_count: int,
        item_count: int,
        last_error: str | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO inventory_checkpoints (
                    run_id, root_path, cursor, completed, page_count, item_count, last_error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, root_path) DO UPDATE SET
                    cursor = excluded.cursor,
                    completed = excluded.completed,
                    page_count = excluded.page_count,
                    item_count = excluded.item_count,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    run_id,
                    root_path,
                    cursor,
                    int(completed),
                    page_count,
                    item_count,
                    last_error,
                    isoformat_utc(utc_now()),
                ),
            )
            self._conn.commit()

    def get_inventory_checkpoint(self, run_id: str, root_path: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM inventory_checkpoints WHERE run_id = ? AND root_path = ?",
                (run_id, root_path),
            ).fetchone()
        return dict(row) if row else None

    def upsert_inventory_records(self, records: Iterable[InventoryRecord]) -> int:
        rows = [
            (
                record.inventory_run_id,
                record.item_type,
                record.full_path,
                record.path_lower,
                record.filename,
                record.parent_path,
                record.dropbox_id,
                record.size,
                record.server_modified,
                record.client_modified,
                record.content_hash,
                record.root_scope_used,
                record.inventory_timestamp,
            )
            for record in records
        ]
        if not rows:
            return 0
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO inventory_items (
                    run_id, item_type, full_path, path_lower, filename, parent_path, dropbox_id,
                    size, server_modified, client_modified, content_hash, root_scope_used, inventory_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, item_type, path_lower) DO UPDATE SET
                    full_path = excluded.full_path,
                    filename = excluded.filename,
                    parent_path = excluded.parent_path,
                    dropbox_id = excluded.dropbox_id,
                    size = excluded.size,
                    server_modified = excluded.server_modified,
                    client_modified = excluded.client_modified,
                    content_hash = excluded.content_hash,
                    root_scope_used = excluded.root_scope_used,
                    inventory_timestamp = excluded.inventory_timestamp
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    def delete_inventory_items_for_root(self, run_id: str, root_path: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM inventory_items WHERE run_id = ? AND root_scope_used = ?",
                (run_id, root_path),
            )
            self._conn.execute(
                "DELETE FROM inventory_checkpoints WHERE run_id = ? AND root_path = ?",
                (run_id, root_path),
            )
            self._conn.commit()

    def iter_inventory_records(self, run_id: str, item_type: str | None = None) -> Iterable[dict[str, Any]]:
        query = "SELECT * FROM inventory_items WHERE run_id = ?"
        params: list[Any] = [run_id]
        if item_type is not None:
            query += " AND item_type = ?"
            params.append(item_type)
        query += " ORDER BY full_path"
        with self._lock:
            cursor = self._conn.execute(query, tuple(params))
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)

    def clear_matches(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM matched_files WHERE run_id = ?", (run_id,))
            self._conn.execute("DELETE FROM copy_jobs WHERE run_id = ?", (run_id,))
            self._conn.commit()

    def upsert_matched_records(self, records: Iterable[MatchedFileRecord], mode: str) -> int:
        rows = list(records)
        if not rows:
            return 0
        matched_rows = [
            (
                record.filter_run_id,
                record.original_path,
                record.path_lower,
                record.filename,
                record.dropbox_id,
                record.size,
                record.server_modified,
                record.client_modified,
                record.content_hash,
                record.planned_archive_path,
                record.match_reason,
                record.filter_timestamp,
                record.parent_path,
            )
            for record in rows
        ]
        job_rows = [
            (
                record.filter_run_id,
                record.original_path,
                record.planned_archive_path,
                record.dropbox_id,
                record.size,
                record.server_modified,
                record.client_modified,
                record.content_hash,
                "planned",
                "Planned from filter phase.",
                0,
                None,
                None,
                record.filename,
                record.parent_path,
                mode,
            )
            for record in rows
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO matched_files (
                    run_id, original_path, path_lower, filename, dropbox_id, size,
                    server_modified, client_modified, content_hash, planned_archive_path,
                    match_reason, filter_timestamp, parent_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, original_path) DO UPDATE SET
                    path_lower = excluded.path_lower,
                    filename = excluded.filename,
                    dropbox_id = excluded.dropbox_id,
                    size = excluded.size,
                    server_modified = excluded.server_modified,
                    client_modified = excluded.client_modified,
                    content_hash = excluded.content_hash,
                    planned_archive_path = excluded.planned_archive_path,
                    match_reason = excluded.match_reason,
                    filter_timestamp = excluded.filter_timestamp,
                    parent_path = excluded.parent_path
                """,
                matched_rows,
            )
            self._conn.executemany(
                """
                INSERT INTO copy_jobs (
                    run_id, original_path, archive_path, dropbox_id, size, server_modified,
                    client_modified, content_hash, status, status_detail, attempt_count,
                    first_attempt_at, last_attempt_at, filename, parent_path, mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, original_path) DO NOTHING
                """,
                job_rows,
            )
            self._conn.commit()
        return len(rows)

    def iter_matched_files(self, run_id: str) -> Iterable[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM matched_files WHERE run_id = ? ORDER BY original_path",
                (run_id,),
            )
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)

    def fetch_copy_jobs(self, run_id: str, statuses: tuple[str, ...], limit: int, after_original_path: str | None = None) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [run_id, *statuses]
        after_clause = ""
        if after_original_path is not None:
            after_clause = " AND original_path > ?"
            params.append(after_original_path)
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM copy_jobs
                WHERE run_id = ? AND status IN ({placeholders}) {after_clause}
                ORDER BY original_path
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def iter_all_copy_jobs(self, run_id: str) -> Iterable[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM copy_jobs WHERE run_id = ? ORDER BY original_path",
                (run_id,),
            )
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)

    def promote_copy_jobs(self, run_id: str, from_statuses: tuple[str, ...], to_status: str, detail: str) -> int:
        placeholders = ",".join("?" for _ in from_statuses)
        with self._lock:
            cursor = self._conn.execute(
                f"""
                UPDATE copy_jobs
                SET status = ?, status_detail = ?
                WHERE run_id = ? AND status IN ({placeholders})
                """,
                (to_status, detail, run_id, *from_statuses),
            )
            self._conn.commit()
        return int(cursor.rowcount or 0)

    def update_copy_job_status(
        self,
        run_id: str,
        original_path: str,
        *,
        status: str,
        status_detail: str,
        attempt_count: int | None = None,
        first_attempt_at: str | None = None,
        last_attempt_at: str | None = None,
        archive_path: str | None = None,
    ) -> None:
        with self._lock:
            current = self._conn.execute(
                "SELECT attempt_count, first_attempt_at, archive_path FROM copy_jobs WHERE run_id = ? AND original_path = ?",
                (run_id, original_path),
            ).fetchone()
            if current is None:
                return
            next_attempt_count = attempt_count if attempt_count is not None else current["attempt_count"]
            first_attempt_value = first_attempt_at if first_attempt_at is not None else current["first_attempt_at"]
            archive_value = archive_path if archive_path is not None else current["archive_path"]
            self._conn.execute(
                """
                UPDATE copy_jobs
                SET status = ?, status_detail = ?, attempt_count = ?, first_attempt_at = ?,
                    last_attempt_at = ?, archive_path = ?
                WHERE run_id = ? AND original_path = ?
                """,
                (
                    status,
                    status_detail,
                    next_attempt_count,
                    first_attempt_value,
                    last_attempt_at,
                    archive_value,
                    run_id,
                    original_path,
                ),
            )
            self._conn.commit()

    def get_counters(self, run_id: str) -> dict[str, int]:
        with self._lock:
            inventory_total = self._conn.execute(
                "SELECT COUNT(*) AS count FROM inventory_items WHERE run_id = ?",
                (run_id,),
            ).fetchone()["count"]
            matched_total = self._conn.execute(
                "SELECT COUNT(*) AS count FROM matched_files WHERE run_id = ?",
                (run_id,),
            ).fetchone()["count"]
            status_rows = self._conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM copy_jobs
                WHERE run_id = ?
                GROUP BY status
                """,
                (run_id,),
            ).fetchall()
        counters = {
            "items_scanned": int(inventory_total),
            "files_matched": int(matched_total),
            "files_copied": 0,
            "files_skipped": 0,
            "files_failed": 0,
        }
        for row in status_rows:
            status = row["status"]
            count = int(row["count"])
            if status == "copied":
                counters["files_copied"] += count
            elif status.startswith("skipped"):
                counters["files_skipped"] += count
            elif status == "failed":
                counters["files_failed"] += count
        return counters

    def build_folder_summary(self, run_id: str) -> list[FolderSummary]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    parent_path AS folder_path,
                    COUNT(*) AS file_count,
                    COALESCE(SUM(COALESCE(size, 0)), 0) AS total_size,
                    COUNT(*) AS matched_count,
                    SUM(CASE WHEN status = 'copied' THEN 1 ELSE 0 END) AS copied_count,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN status LIKE 'skipped%' THEN 1 ELSE 0 END) AS skipped_count
                FROM copy_jobs
                WHERE run_id = ?
                GROUP BY parent_path
                ORDER BY parent_path
                """,
                (run_id,),
            ).fetchall()
        return [
            FolderSummary(
                folder_path=row["folder_path"],
                file_count=int(row["file_count"]),
                total_size=int(row["total_size"]),
                matched_count=int(row["matched_count"] or 0),
                copied_count=int(row["copied_count"] or 0),
                failed_count=int(row["failed_count"] or 0),
                skipped_count=int(row["skipped_count"] or 0),
            )
            for row in rows
        ]

    def preview_copy_statuses(self, run_id: str, status_prefix: str, limit: int = 20) -> list[str]:
        if status_prefix.endswith("%"):
            comparator = "LIKE"
            prefix = status_prefix
        else:
            comparator = "="
            prefix = status_prefix
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT original_path, archive_path, status_detail
                FROM copy_jobs
                WHERE run_id = ? AND status {comparator} ?
                ORDER BY original_path
                LIMIT ?
                """,
                (run_id, prefix, limit),
            ).fetchall()
        return [f"{row['original_path']} -> {row['archive_path']}: {row['status_detail']}" for row in rows]

    def manifest_rows(self, run_id: str) -> Iterable[CopyJobRecord]:
        for row in self.iter_all_copy_jobs(run_id):
            yield CopyJobRecord(
                run_id=row["run_id"],
                mode=row["mode"],
                original_path=row["original_path"],
                archive_path=row["archive_path"],
                dropbox_id=row["dropbox_id"],
                size=row["size"],
                server_modified=row["server_modified"],
                client_modified=row["client_modified"],
                content_hash=row["content_hash"],
                status=row["status"],
                status_detail=row["status_detail"],
                attempt_count=row["attempt_count"],
                first_attempt_at=row["first_attempt_at"],
                last_attempt_at=row["last_attempt_at"],
                filename=row["filename"],
                parent_path=row["parent_path"],
            )
