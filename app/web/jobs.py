from __future__ import annotations

import json
import threading
import traceback
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from typing import Any
from uuid import uuid4

from app.models.config import AuthConfig, JobConfig
from app.models.events import ProgressSnapshot
from app.services.orchestrator import RunOrchestrator
from app.services.runtime import CancellationRequested, CancellationToken, RunResult


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class WebLogSink(Queue[str]):
    def __init__(self, manager: "JobManager", job_id: str) -> None:
        super().__init__()
        self._manager = manager
        self._job_id = job_id

    def put(self, item: str, block: bool = True, timeout: float | None = None) -> None:  # noqa: ARG002
        self._manager.add_event(self._job_id, "log", {"message": str(item)})


@dataclass
class JobState:
    job_id: str
    status: str
    mode: str | None = None
    started_at: str = field(default_factory=utc_timestamp)
    completed_at: str | None = None
    actual_run_id: str | None = None
    run_dir: str | None = None
    summary_path: str | None = None
    verification_path: str | None = None
    error: str | None = None
    details: str | None = None
    cancellation_token: CancellationToken = field(default_factory=CancellationToken)
    thread: threading.Thread | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    next_seq: int = 1

    def to_response(self) -> dict[str, Any]:
        return {
            "run_id": self.job_id,
            "status": self.status,
            "kind": "job",
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "mode": self.mode,
            "actual_run_id": self.actual_run_id,
            "run_dir": self.run_dir,
            "summary_path": self.summary_path,
            "verification_path": self.verification_path,
            "error": self.error,
        }


class JobManager:
    def __init__(self, orchestrator_factory: Callable[[], RunOrchestrator] | None = None) -> None:
        self._orchestrator_factory = orchestrator_factory or RunOrchestrator
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs: dict[str, JobState] = {}
        self._active_job_id: str | None = None

    def start_run(self, *, auth_config: AuthConfig, job_config: JobConfig) -> JobState:
        state = self._start_job(mode=job_config.mode)

        def _target() -> None:
            try:
                result = self._orchestrator_factory().run(
                    job_config=job_config,
                    auth_config=auth_config,
                    emit=lambda snapshot: self.add_progress(state.job_id, snapshot),
                    cancellation_token=state.cancellation_token,
                    ui_log_queue=WebLogSink(self, state.job_id),
                )
                self._complete_job(state.job_id, result)
            except CancellationRequested as exc:
                self._fail_job(state.job_id, "cancelled", str(exc), traceback.format_exc())
            except Exception as exc:  # noqa: BLE001
                self._fail_job(state.job_id, "failed", str(exc), traceback.format_exc())

        self._launch(state, _target)
        return state

    def resume_run(self, *, auth_config: AuthConfig, state_db_path: Path) -> JobState:
        state = self._start_job(mode="resume")

        def _target() -> None:
            try:
                result = self._orchestrator_factory().resume(
                    state_db_path=state_db_path,
                    auth_config=auth_config,
                    emit=lambda snapshot: self.add_progress(state.job_id, snapshot),
                    cancellation_token=state.cancellation_token,
                    ui_log_queue=WebLogSink(self, state.job_id),
                )
                self._complete_job(state.job_id, result)
            except CancellationRequested as exc:
                self._fail_job(state.job_id, "cancelled", str(exc), traceback.format_exc())
            except Exception as exc:  # noqa: BLE001
                self._fail_job(state.job_id, "failed", str(exc), traceback.format_exc())

        self._launch(state, _target)
        return state

    def cancel(self, job_id: str) -> bool:
        with self._condition:
            state = self._jobs.get(job_id) or self._job_by_actual_run_id(job_id)
            if state is None or state.status != "running":
                return False
            state.cancellation_token.cancel()
            self._add_event_locked(state, "status", {"message": "Cancellation requested."})
            self._condition.notify_all()
            return True

    def status(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            state = self._jobs.get(job_id) or self._job_by_actual_run_id(job_id)
            return state.to_response() if state else None

    def add_progress(self, job_id: str, snapshot: ProgressSnapshot) -> None:
        self.add_event(job_id, "progress", asdict(snapshot))

    def add_event(self, job_id: str, event_type: str, data: dict[str, Any]) -> None:
        with self._condition:
            state = self._jobs.get(job_id)
            if state is None:
                return
            self._add_event_locked(state, event_type, data)
            self._condition.notify_all()

    def events_after(self, job_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            state = self._jobs.get(job_id) or self._job_by_actual_run_id(job_id)
            if state is None:
                return []
            return [event for event in state.events if event["seq"] > after]

    def sse_events(self, job_id: str, after: int = 0) -> Iterator[str]:
        last_seq = after
        idle_count = 0
        while True:
            with self._condition:
                state = self._jobs.get(job_id) or self._job_by_actual_run_id(job_id)
                if state is None:
                    yield _sse("error", {"message": "Run not found."}, last_seq + 1)
                    return
                events = [event for event in state.events if event["seq"] > last_seq]
                if not events and state.status in ("completed", "failed", "cancelled"):
                    return
                if not events:
                    self._condition.wait(timeout=10)
                    idle_count += 1
                    if idle_count >= 1:
                        yield ": keep-alive\n\n"
                    continue
            idle_count = 0
            for event in events:
                last_seq = int(event["seq"])
                yield _sse(event["type"], event["data"], last_seq)

    def _start_job(self, *, mode: str) -> JobState:
        with self._condition:
            if self._active_job_id is not None:
                active = self._jobs.get(self._active_job_id)
                if active and active.status == "running":
                    raise RuntimeError("A run is already in progress.")
            state = JobState(job_id=f"job-{uuid4().hex[:12]}", status="running", mode=mode)
            self._jobs[state.job_id] = state
            self._active_job_id = state.job_id
            self._add_event_locked(state, "status", {"message": "Run queued."})
            self._condition.notify_all()
            return state

    def _launch(self, state: JobState, target: Callable[[], None]) -> None:
        thread = threading.Thread(target=target, name=f"dropbox-cleaner-{state.job_id}", daemon=True)
        with self._condition:
            state.thread = thread
        thread.start()

    def _complete_job(self, job_id: str, result: RunResult) -> None:
        with self._condition:
            state = self._jobs[job_id]
            state.status = "completed"
            state.completed_at = utc_timestamp()
            state.actual_run_id = result.run_id
            state.run_dir = result.run_dir
            state.summary_path = result.summary_path
            state.verification_path = result.verification_path
            self._active_job_id = None
            self._add_event_locked(
                state,
                "result",
                {
                    "run_id": result.run_id,
                    "run_dir": result.run_dir,
                    "summary_path": result.summary_path,
                    "verification_path": result.verification_path,
                },
            )
            self._condition.notify_all()

    def _fail_job(self, job_id: str, status: str, message: str, details: str) -> None:
        with self._condition:
            state = self._jobs[job_id]
            state.status = status
            state.completed_at = utc_timestamp()
            state.error = message
            state.details = details
            self._active_job_id = None
            self._add_event_locked(state, "error", {"message": message, "details": details})
            self._condition.notify_all()

    def _add_event_locked(self, state: JobState, event_type: str, data: dict[str, Any]) -> None:
        event = {"seq": state.next_seq, "type": event_type, "data": data}
        state.next_seq += 1
        state.events.append(event)
        if len(state.events) > 1000:
            state.events = state.events[-1000:]

    def _job_by_actual_run_id(self, run_id: str) -> JobState | None:
        for state in self._jobs.values():
            if state.actual_run_id == run_id:
                return state
        return None


def _sse(event_type: str, data: dict[str, Any], event_id: int) -> str:
    payload = json.dumps(data, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event_type}\ndata: {payload}\n\n"

