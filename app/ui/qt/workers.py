from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

from PySide6.QtCore import QObject, Signal, Slot

from app.models.config import AuthConfig, JobConfig
from app.services.orchestrator import RunOrchestrator
from app.services.runtime import CancellationToken
from app.ui.folder_browser import BrowserLocation, DropboxFolderBrowserService


class WorkerSignals(QObject):
    progress = Signal(object)
    log = Signal(str)
    result = Signal(object)
    error = Signal(str, str)
    folders = Signal(object, object)
    finished = Signal()


class QtLogSink:
    def __init__(self, signal: Signal) -> None:
        self._signal = signal

    def put(self, line: str) -> None:
        self._signal.emit(str(line))


class RunWorker(QObject):
    def __init__(
        self,
        *,
        orchestrator: RunOrchestrator,
        auth_config: AuthConfig,
        job_config: JobConfig | None = None,
        state_db_path: Path | None = None,
        cancellation_token: CancellationToken | None = None,
        resume: bool = False,
    ) -> None:
        super().__init__()
        self.signals = WorkerSignals()
        self._orchestrator = orchestrator
        self._auth_config = auth_config
        self._job_config = job_config
        self._state_db_path = state_db_path
        self._cancellation_token = cancellation_token or CancellationToken()
        self._resume = resume

    @Slot()
    def run(self) -> None:
        try:
            if self._resume:
                if self._state_db_path is None:
                    raise ValueError("No previous run state was found.")
                result = self._orchestrator.resume(
                    state_db_path=self._state_db_path,
                    auth_config=self._auth_config,
                    emit=self.signals.progress.emit,
                    cancellation_token=self._cancellation_token,
                    ui_log_queue=QtLogSink(self.signals.log),  # type: ignore[arg-type]
                )
            else:
                if self._job_config is None:
                    raise ValueError("No job configuration was provided.")
                result = self._orchestrator.run(
                    job_config=self._job_config,
                    auth_config=self._auth_config,
                    emit=self.signals.progress.emit,
                    cancellation_token=self._cancellation_token,
                    ui_log_queue=QtLogSink(self.signals.log),  # type: ignore[arg-type]
                )
            self.signals.result.emit(result)
        except Exception as exc:  # noqa: BLE001
            self.signals.error.emit(str(exc), traceback.format_exc())
        finally:
            self.signals.finished.emit()


class ConnectionTestWorker(QObject):
    def __init__(self, *, auth_manager: Any, auth_config: AuthConfig, logger: Any) -> None:
        super().__init__()
        self.signals = WorkerSignals()
        self._auth_manager = auth_manager
        self._auth_config = auth_config
        self._logger = logger

    @Slot()
    def run(self) -> None:
        try:
            account = self._auth_manager.test_connection(self._auth_config, self._logger)
            self.signals.result.emit(account)
        except Exception as exc:  # noqa: BLE001
            self.signals.error.emit(str(exc), traceback.format_exc())
        finally:
            self.signals.finished.emit()


class FolderLoadWorker(QObject):
    def __init__(self, *, service: DropboxFolderBrowserService, location: BrowserLocation) -> None:
        super().__init__()
        self.signals = WorkerSignals()
        self._service = service
        self._location = location

    @Slot()
    def run(self) -> None:
        try:
            folders = self._service.list_folders(self._location)
            self.signals.folders.emit(self._location, folders)
        except Exception as exc:  # noqa: BLE001
            self.signals.error.emit(str(exc), traceback.format_exc())
        finally:
            self.signals.finished.emit()

