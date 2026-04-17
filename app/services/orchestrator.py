from __future__ import annotations

import json
import logging
from pathlib import Path
from queue import Queue

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.errors import ConflictPolicyAbortError
from app.models.config import AuthConfig, JobConfig, OutputPaths, RunContext
from app.models.events import ProgressSnapshot
from app.persistence.repository import RunStateRepository
from app.reports.writers import ReportWriter
from app.services.copying import ArchiveCopyService
from app.services.filtering import FilterService
from app.services.inventory import DropboxInventoryService
from app.services.planner import ArchivePlanner
from app.services.runtime import CancellationRequested, CancellationToken, ProgressEmitter, RunResult
from app.services.verification import VerificationService
from app.utils.ids import new_run_id
from app.utils.logging import build_run_logger
from app.utils.paths import dedupe_source_roots, normalize_dropbox_path
from app.utils.time import isoformat_utc, timestamp_slug, utc_now


class RunOrchestrator:
    def __init__(self, adapter_factory=DropboxAdapter) -> None:
        self._adapter_factory = adapter_factory

    def run(
        self,
        *,
        job_config: JobConfig,
        auth_config: AuthConfig,
        emit: ProgressEmitter | None = None,
        cancellation_token: CancellationToken | None = None,
        ui_log_queue: Queue[str] | None = None,
    ) -> RunResult:
        cancellation_token = cancellation_token or CancellationToken()
        source_roots, ignored_roots = dedupe_source_roots(job_config.source_roots)
        if not source_roots:
            raise ValueError("At least one Dropbox source root is required.")

        job_config.source_roots = source_roots
        job_config.archive_root = normalize_dropbox_path(job_config.archive_root)

        run_id = new_run_id()
        created_at = isoformat_utc(utc_now()) or ""
        output_paths = OutputPaths.create(
            job_config.output_dir,
            f"{timestamp_slug(utc_now())}_{run_id.split('-')[0]}",
            job_config.mode,
        )
        output_paths.run_dir.mkdir(parents=True, exist_ok=True)
        logger = build_run_logger(run_id, output_paths.app_log, output_paths.app_jsonl, ui_queue=ui_log_queue)
        repository = RunStateRepository(output_paths.state_db)
        report_writer = ReportWriter(repository)
        run_context = RunContext(run_id=run_id, created_at=created_at, mode=job_config.mode, output_paths=output_paths)
        repository.create_run(run_context, job_config, auth_config)

        if ignored_roots:
            repository.record_event(
                run_id,
                "validation",
                "INFO",
                "ignored_source_roots",
                "Ignored overlapping or redundant source roots.",
                {"ignored_roots": ignored_roots},
            )

        report_writer.write_config_snapshot(
            path=output_paths.config_snapshot_json,
            run_context=run_context,
            job_config=job_config,
            auth_config=auth_config,
        )
        return self._execute_workflow(
            repository=repository,
            report_writer=report_writer,
            logger=logger,
            run_context=run_context,
            job_config=job_config,
            auth_config=auth_config,
            emit=emit,
            cancellation_token=cancellation_token,
            resume_phase=None,
        )

    def resume(
        self,
        *,
        state_db_path: Path,
        auth_config: AuthConfig,
        emit: ProgressEmitter | None = None,
        cancellation_token: CancellationToken | None = None,
        ui_log_queue: Queue[str] | None = None,
    ) -> RunResult:
        cancellation_token = cancellation_token or CancellationToken()
        repository = RunStateRepository(state_db_path)
        run_row = repository.get_latest_run()
        if run_row is None:
            raise ValueError(f"No run metadata found in {state_db_path}.")

        output_paths = self._output_paths_from_state_db(state_db_path, run_row["mode"])
        run_context = RunContext(
            run_id=run_row["run_id"],
            created_at=run_row["created_at"],
            mode=run_row["mode"],
            output_paths=output_paths,
        )
        logger = build_run_logger(run_context.run_id, output_paths.app_log, output_paths.app_jsonl, ui_queue=ui_log_queue)
        report_writer = ReportWriter(repository)
        config_payload = json.loads(run_row["config_json"])
        job_config = JobConfig(
            source_roots=config_payload["source_roots"],
            cutoff_date=config_payload["cutoff_date"],
            archive_root=config_payload["archive_root"],
            output_dir=Path(run_row["base_output_dir"]),
            state_db_path=state_db_path,
            mode=run_row["mode"],
            batch_size=config_payload["batch_size"],
            conflict_policy=config_payload["conflict_policy"],
            include_folders_in_inventory=config_payload["include_folders_in_inventory"],
            exclude_archive_destination=config_payload["exclude_archive_destination"],
            worker_count=config_payload["worker_count"],
            verify_after_run=config_payload["verify_after_run"],
        )
        return self._execute_workflow(
            repository=repository,
            report_writer=report_writer,
            logger=logger,
            run_context=run_context,
            job_config=job_config,
            auth_config=auth_config,
            emit=emit,
            cancellation_token=cancellation_token,
            resume_phase=run_row["phase"],
        )

    def verify_only(
        self,
        *,
        state_db_path: Path,
        auth_config: AuthConfig,
        emit: ProgressEmitter | None = None,
        cancellation_token: CancellationToken | None = None,
        ui_log_queue: Queue[str] | None = None,
    ) -> RunResult:
        cancellation_token = cancellation_token or CancellationToken()
        repository = RunStateRepository(state_db_path)
        run_row = repository.get_latest_run()
        if run_row is None:
            raise ValueError(f"No run metadata found in {state_db_path}.")
        output_paths = self._output_paths_from_state_db(state_db_path, run_row["mode"])
        run_context = RunContext(
            run_id=run_row["run_id"],
            created_at=run_row["created_at"],
            mode=run_row["mode"],
            output_paths=output_paths,
        )
        logger = build_run_logger(run_context.run_id, output_paths.app_log, output_paths.app_jsonl, ui_queue=ui_log_queue)
        report_writer = ReportWriter(repository)
        config_payload = json.loads(run_row["config_json"])
        job_config = JobConfig(
            source_roots=config_payload["source_roots"],
            cutoff_date=config_payload["cutoff_date"],
            archive_root=config_payload["archive_root"],
            output_dir=Path(run_row["base_output_dir"]),
            state_db_path=state_db_path,
            mode=run_row["mode"],
            batch_size=config_payload["batch_size"],
            conflict_policy=config_payload["conflict_policy"],
            include_folders_in_inventory=config_payload["include_folders_in_inventory"],
            exclude_archive_destination=config_payload["exclude_archive_destination"],
            worker_count=config_payload["worker_count"],
            verify_after_run=True,
        )
        adapter = self._adapter_factory(auth_config, logger)
        verification_rows = []
        try:
            verification_service = VerificationService(repository, logger)
            verification_rows = verification_service.run(
                adapter=adapter,
                run_context=run_context,
                job_config=job_config,
                emit=emit,
                cancellation_token=cancellation_token,
            )
            verification_summary = report_writer.write_verification_outputs(
                verification_rows,
                output_paths.verification_csv,
                output_paths.verification_json,
            )
            report_writer.write_summary_outputs(
                run_context=run_context,
                output_paths=output_paths,
                verification_summary=verification_summary,
            )
            report_writer.write_latest_pointer(output_paths, run_context)
            return RunResult(
                run_id=run_context.run_id,
                run_dir=str(output_paths.run_dir),
                summary_path=str(output_paths.summary_json),
                verification_path=str(output_paths.verification_json),
            )
        finally:
            adapter.close()

    def _execute_workflow(
        self,
        *,
        repository: RunStateRepository,
        report_writer: ReportWriter,
        logger: logging.Logger,
        run_context: RunContext,
        job_config: JobConfig,
        auth_config: AuthConfig,
        emit: ProgressEmitter | None,
        cancellation_token: CancellationToken,
        resume_phase: str | None,
    ) -> RunResult:
        planner = ArchivePlanner(job_config.archive_root, job_config.exclude_archive_destination)
        verification_summary: dict = {}
        adapter = self._adapter_factory(auth_config, logger)
        try:
            if emit is not None:
                emit(ProgressSnapshot(phase="connecting", message="Connecting to Dropbox"))
            adapter.get_current_account()

            pending_copy_jobs = repository.fetch_copy_jobs(
                run_context.run_id,
                statuses=("planned", "failed", "retried", "resumed"),
                limit=1,
            )
            has_pending_copy_jobs = bool(pending_copy_jobs)
            counters = repository.get_counters(run_context.run_id)
            has_matches = counters.get("files_matched", 0) > 0

            should_run_inventory = resume_phase in (None, "created", "inventory")
            should_run_filter = run_context.mode != "inventory_only" and (
                resume_phase in (None, "created", "inventory", "filter") or not has_matches
            )
            should_run_copy = run_context.mode in ("dry_run", "copy_run") and (
                resume_phase in (None, "created", "inventory", "filter", "copy") or has_pending_copy_jobs
            )
            should_run_verify = run_context.mode in ("dry_run", "copy_run") and job_config.verify_after_run

            if should_run_inventory:
                repository.update_run_phase(run_context.run_id, "inventory")
                DropboxInventoryService(repository, logger).run(
                    adapter=adapter,
                    run_context=run_context,
                    job_config=job_config,
                    source_roots=job_config.source_roots,
                    planner=planner,
                    emit=emit,
                    cancellation_token=cancellation_token,
                )

            if should_run_filter:
                repository.update_run_phase(run_context.run_id, "filter")
                FilterService(repository, logger).run(
                    run_context=run_context,
                    job_config=job_config,
                    planner=planner,
                    emit=emit,
                    cancellation_token=cancellation_token,
                )

            if should_run_copy:
                repository.update_run_phase(run_context.run_id, "copy")
                ArchiveCopyService(repository, logger).run(
                    adapter=adapter,
                    run_context=run_context,
                    job_config=job_config,
                    planner=planner,
                    emit=emit,
                    cancellation_token=cancellation_token,
                    dry_run=run_context.mode == "dry_run",
                )

            if should_run_verify:
                repository.update_run_phase(run_context.run_id, "verify")
                verification_rows = VerificationService(repository, logger).run(
                    adapter=adapter,
                    run_context=run_context,
                    job_config=job_config,
                    emit=emit,
                    cancellation_token=cancellation_token,
                )
                verification_summary = report_writer.write_verification_outputs(
                    verification_rows,
                    run_context.output_paths.verification_csv,
                    run_context.output_paths.verification_json,
                )

            repository.update_run_phase(run_context.run_id, "outputs")
            report_writer.write_inventory_csv(run_context.run_id, run_context.output_paths.inventory_csv)
            if run_context.mode in ("dry_run", "copy_run"):
                report_writer.write_matched_csv(run_context.run_id, run_context.output_paths.matched_csv)
                report_writer.write_manifest_csv(run_context.run_id, run_context.output_paths.manifest_csv)
            report_writer.write_summary_outputs(
                run_context=run_context,
                output_paths=run_context.output_paths,
                verification_summary=verification_summary,
            )
            report_writer.write_latest_pointer(run_context.output_paths, run_context)
            repository.finish_run(run_context.run_id, "completed")
            if emit is not None:
                emit(
                    ProgressSnapshot(
                        phase="completed",
                        message="Run completed",
                        counters=repository.get_counters(run_context.run_id),
                        outputs={
                            "run_dir": str(run_context.output_paths.run_dir),
                            "summary_json": str(run_context.output_paths.summary_json),
                        },
                    )
                )
            return RunResult(
                run_id=run_context.run_id,
                run_dir=str(run_context.output_paths.run_dir),
                summary_path=str(run_context.output_paths.summary_json),
                verification_path=str(run_context.output_paths.verification_json)
                if run_context.mode in ("dry_run", "copy_run")
                else None,
            )
        except CancellationRequested:
            repository.finish_run(run_context.run_id, "cancelled")
            self._write_best_effort_outputs(report_writer, repository, run_context, verification_summary)
            raise
        except ConflictPolicyAbortError:
            repository.finish_run(run_context.run_id, "failed")
            self._write_best_effort_outputs(report_writer, repository, run_context, verification_summary)
            raise
        except Exception:
            repository.finish_run(run_context.run_id, "failed")
            self._write_best_effort_outputs(report_writer, repository, run_context, verification_summary)
            raise
        finally:
            adapter.close()

    def _write_best_effort_outputs(
        self,
        report_writer: ReportWriter,
        repository: RunStateRepository,
        run_context: RunContext,
        verification_summary: dict,
    ) -> None:
        try:
            report_writer.write_inventory_csv(run_context.run_id, run_context.output_paths.inventory_csv)
            if run_context.mode in ("dry_run", "copy_run"):
                report_writer.write_matched_csv(run_context.run_id, run_context.output_paths.matched_csv)
                report_writer.write_manifest_csv(run_context.run_id, run_context.output_paths.manifest_csv)
            report_writer.write_summary_outputs(
                run_context=run_context,
                output_paths=run_context.output_paths,
                verification_summary=verification_summary,
            )
            report_writer.write_latest_pointer(run_context.output_paths, run_context)
        except Exception:
            return

    def _output_paths_from_state_db(self, state_db_path: Path, mode: str) -> OutputPaths:
        run_dir = state_db_path.parent
        base_output_dir = run_dir.parent
        output_paths = OutputPaths.create(base_output_dir, run_dir.name, mode)  # type: ignore[arg-type]
        output_paths.run_dir = run_dir
        output_paths.state_db = state_db_path
        output_paths.inventory_csv = run_dir / "inventory_full.csv"
        output_paths.matched_csv = run_dir / "matched_pre_cutoff.csv"
        output_paths.manifest_csv = run_dir / (
            "manifest_dry_run.csv" if mode == "dry_run" else "manifest_copy_run.csv"
        )
        output_paths.summary_json = run_dir / "summary.json"
        output_paths.summary_text = run_dir / "summary.md"
        output_paths.verification_csv = run_dir / "verification_report.csv"
        output_paths.verification_json = run_dir / "verification_report.json"
        output_paths.app_log = run_dir / "app.log"
        output_paths.app_jsonl = run_dir / "app.jsonl"
        output_paths.config_snapshot_json = run_dir / "config_snapshot.json"
        output_paths.latest_pointer = base_output_dir / "latest_run.json"
        return output_paths
