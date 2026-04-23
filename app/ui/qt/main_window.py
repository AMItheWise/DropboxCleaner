from __future__ import annotations

import json
import logging
import os
import sys
import webbrowser
from pathlib import Path

from PySide6.QtCore import QThread, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QApplication, QMainWindow, QMessageBox, QStackedWidget

from app.dropbox_client.auth import AuthManager, default_scopes_for_mode
from app.dropbox_client.errors import MissingScopeError
from app.models.config import AuthConfig, JobConfig, RetrySettings
from app.models.records import AccountInfo
from app.services.orchestrator import RunOrchestrator
from app.services.runtime import CancellationToken, RunResult
from app.ui.options import (
    date_filter_label_to_value,
    run_label_to_value,
    team_archive_layout_label_to_value,
    team_coverage_label_to_value,
)
from app.ui.qt import theme
from app.ui.qt.dialogs import DropboxFolderPickerDialog, ErrorDetailsDialog, choose_local_output_dir
from app.ui.qt.screens import AccountScreen, ConnectionScreen, ResultsScreen, RunScreen, SettingsScreen
from app.ui.qt.workers import ConnectionTestWorker, RunWorker
from app.ui.results import load_results_view_model


class DropboxCleanerMainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(theme.APP_TITLE)
        self.resize(1280, 860)
        self.setMinimumSize(1040, 720)

        self.auth_manager = AuthManager()
        self.orchestrator = RunOrchestrator()
        self.account_mode = "personal"
        self.latest_run_dir: Path | None = None
        self.current_run_result: RunResult | None = None
        self.cancellation_token: CancellationToken | None = None
        self._threads: list[QThread] = []
        self._workers: list[object] = []
        self._connection_verified = False
        self._connected_account_summary = "Not connected yet."
        self._packaged_key = self._packaged_app_key()

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)
        self.account_screen = AccountScreen()
        self.connection_screen = ConnectionScreen()
        self.settings_screen = SettingsScreen()
        self.run_screen = RunScreen()
        self.results_screen = ResultsScreen()
        for screen in (
            self.account_screen,
            self.connection_screen,
            self.settings_screen,
            self.run_screen,
            self.results_screen,
        ):
            self.stack.addWidget(screen)

        self._wire_signals()
        self._load_saved_credentials_hint()
        self._load_latest_run_hint()
        self.connection_screen.set_packaged_app_key(self._packaged_key)
        self.account_screen.set_resume_available(self.latest_run_dir is not None)
        self.settings_screen.set_resume_available(self.latest_run_dir is not None)
        self.stack.setCurrentWidget(self.account_screen)

    def _wire_signals(self) -> None:
        self.account_screen.account_selected.connect(self._select_account_mode)
        self.account_screen.resume_requested.connect(self.resume_last_run)

        self.connection_screen.start_oauth_requested.connect(self.start_oauth)
        self.connection_screen.finish_oauth_requested.connect(self.finish_oauth)
        self.connection_screen.test_connection_requested.connect(self.test_saved_connection)
        self.connection_screen.disconnect_requested.connect(self.clear_saved_credentials)
        self.connection_screen.back_requested.connect(lambda: self.stack.setCurrentWidget(self.account_screen))
        self.connection_screen.continue_requested.connect(self._continue_to_settings)
        self.connection_screen.save_token_requested.connect(self.save_manual_token)

        self.settings_screen.back_requested.connect(lambda: self.stack.setCurrentWidget(self.connection_screen))
        self.settings_screen.browse_archive_requested.connect(lambda: self.open_folder_picker("archive"))
        self.settings_screen.browse_source_requested.connect(lambda: self.open_folder_picker("source"))
        self.settings_screen.browse_exclusion_requested.connect(lambda: self.open_folder_picker("exclude"))
        self.settings_screen.browse_output_requested.connect(self.choose_output_dir)
        self.settings_screen.start_run_requested.connect(self.start_run_from_settings)
        self.settings_screen.resume_requested.connect(self.resume_last_run)

        self.run_screen.cancel_requested.connect(self.stop_run)
        self.run_screen.view_results_requested.connect(self.show_results_screen)

        self.results_screen.open_output_requested.connect(self.open_output_folder)
        self.results_screen.open_summary_requested.connect(lambda: self._open_named_output("summary.md"))
        self.results_screen.open_manifest_requested.connect(self._open_manifest)
        self.results_screen.resume_requested.connect(self.resume_last_run)
        self.results_screen.start_another_requested.connect(lambda: self.stack.setCurrentWidget(self.settings_screen))

    def _select_account_mode(self, value: str) -> None:
        mode_changed = value != self.account_mode
        self.account_mode = value
        self.connection_screen.set_account_mode(value)
        if mode_changed:
            self._connection_verified = False
            self.connection_screen.set_connected(False)
            self.connection_screen.set_saved_credentials_available(False)
        self.connection_screen.set_busy(False)
        self.settings_screen.set_account_mode(value)
        self.stack.setCurrentWidget(self.connection_screen)

    def start_oauth(self) -> None:
        app_key = self._effective_app_key()
        if not app_key:
            self._show_simple_error("Missing app key", "Enter your Dropbox app key first.")
            return
        try:
            self._connection_verified = False
            self.connection_screen.set_connected(False)
            authorize_url = self.auth_manager.start_pkce_flow(
                app_key,
                default_scopes_for_mode(self.account_mode),
                account_mode=self.account_mode,
                label="default",
            )
            webbrowser.open(authorize_url)
            self.connection_screen.set_status(
                "Dropbox opened in your browser. Paste the authorization code here after approving access."
            )
        except Exception as exc:  # noqa: BLE001
            self._show_error("Could not start Dropbox authorization", exc)

    def finish_oauth(self) -> None:
        try:
            credentials = self.auth_manager.finish_pkce_flow(self.connection_screen.auth_code_edit.text().strip(), label="default")
            admin_member_id = self.connection_screen.admin_member_id_edit.text().strip()
            if admin_member_id:
                credentials.admin_member_id = admin_member_id
            self.auth_manager.save_credentials("default", credentials)
            self.account_mode = credentials.account_mode
            self.connection_screen.token_edit.clear()
            self._test_connection_from_config(self.auth_manager.credentials_to_auth_config(credentials))
        except Exception as exc:  # noqa: BLE001
            self._show_error("Connection failed", exc)

    def save_manual_token(self) -> None:
        token = self.connection_screen.token_edit.text().strip()
        if not token:
            self._show_simple_error("Missing token", "Enter a refresh token first.")
            return
        app_key = self._effective_app_key()
        if not app_key:
            self._show_simple_error("Missing app key", "A Dropbox app key is required for refresh-token auth.")
            return
        try:
            credentials = self.auth_manager.save_manual_token(
                method="refresh_token",
                account_mode=self.account_mode,
                app_key=app_key,
                refresh_token=token,
                admin_member_id=self.connection_screen.admin_member_id_edit.text().strip() or None,
            )
            self._test_connection_from_config(self.auth_manager.credentials_to_auth_config(credentials))
        except Exception as exc:  # noqa: BLE001
            self._show_error("Connection failed", exc)

    def test_saved_connection(self) -> None:
        try:
            self._test_connection_from_config(self._build_auth_config())
        except Exception as exc:  # noqa: BLE001
            self._show_error("Connection failed", exc)

    def _test_connection_from_config(self, auth_config: AuthConfig) -> None:
        self._connection_verified = False
        self.connection_screen.set_connected(False)
        self.connection_screen.set_busy(True)
        worker = ConnectionTestWorker(auth_manager=self.auth_manager, auth_config=auth_config, logger=self._temporary_logger())
        thread = QThread(self)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.signals.result.connect(self._connection_test_succeeded)
        worker.signals.error.connect(self._connection_test_failed)
        worker.signals.finished.connect(thread.quit)
        worker.signals.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda thread=thread: self._threads.remove(thread) if thread in self._threads else None)
        thread.finished.connect(lambda worker=worker: self._workers.remove(worker) if worker in self._workers else None)
        self._threads.append(thread)
        self._workers.append(worker)
        self.connection_screen.set_status("Testing Dropbox connection...")
        thread.start()

    def _connection_test_succeeded(self, account: AccountInfo) -> None:
        self.connection_screen.set_busy(False)
        self._connection_verified = True
        self.account_mode = account.account_mode
        self.connection_screen.set_account_mode(self.account_mode)
        self.settings_screen.set_account_mode(self.account_mode)
        if account.account_mode == "team_admin":
            summary = (
                f"Connected as {account.display_name}\n"
                f"Team: {account.team_name or 'Unknown'}\n"
                f"Model: {account.team_model or 'unknown'}\n"
                f"Active members: {account.active_member_count}\n"
                f"Namespaces: {account.namespace_count}"
            )
        else:
            summary = f"Connected as {account.display_name} ({account.email or 'no email returned'})"
        self._connected_account_summary = summary
        self.connection_screen.set_account_status(
            display_name=account.display_name,
            email=account.email,
            account_mode=account.account_mode,
            team_name=account.team_name,
            team_model=account.team_model,
            active_member_count=account.active_member_count,
            namespace_count=account.namespace_count,
        )
        self.connection_screen.set_connected(True)

    def _connection_test_failed(self, message: str, details: str) -> None:
        self.connection_screen.set_busy(False)
        self._connection_verified = False
        self.connection_screen.set_connected(False)
        self.connection_screen.set_status("Connection test failed. Review the details and try again.")
        ErrorDetailsDialog("Connection failed", self._format_exception_for_user(message), details, self).exec()

    def clear_saved_credentials(self) -> None:
        self.auth_manager.clear_credentials("default")
        self.connection_screen.auth_code_edit.clear()
        self.connection_screen.token_edit.clear()
        self._connection_verified = False
        self._connected_account_summary = "Saved connection removed. Connect Dropbox again to continue."
        self.connection_screen.set_saved_credentials_available(False)
        self.connection_screen.set_status(self._connected_account_summary)
        self.connection_screen.set_connected(False)

    def _continue_to_settings(self) -> None:
        if not self._connection_verified:
            QMessageBox.information(
                self,
                "Connect Dropbox first",
                "Finish Dropbox authorization or press Use saved connection before continuing.",
            )
            return
        self.stack.setCurrentWidget(self.settings_screen)

    def choose_output_dir(self) -> None:
        selected = choose_local_output_dir(self, self.settings_screen.output_dir)
        if selected:
            self.settings_screen.output_edit.setText(selected)
            self._load_latest_run_hint()
            self.account_screen.set_resume_available(self.latest_run_dir is not None)
            self.settings_screen.set_resume_available(self.latest_run_dir is not None)

    def open_folder_picker(self, purpose: str) -> None:
        try:
            auth_config = self._build_auth_config()
            job_config = self._build_job_config(self.settings_screen.selected_run_mode)
        except Exception as exc:  # noqa: BLE001
            self._show_error("Connect Dropbox first", exc)
            return
        dialog = DropboxFolderPickerDialog(
            auth_config=auth_config,
            job_config=job_config,
            purpose=purpose,
            parent=self,
        )
        if dialog.exec() != dialog.DialogCode.Accepted or not dialog.selected_path:
            return
        if purpose == "archive":
            self.settings_screen.archive_edit.setText(dialog.selected_path)
        elif purpose == "exclude":
            self.settings_screen.add_excluded_root(dialog.selected_path)
        else:
            self.settings_screen.add_source_root(dialog.selected_path)

    def start_run_from_settings(self) -> None:
        self.start_run(resume=False)

    def start_run(self, *, resume: bool) -> None:
        if self._is_worker_running():
            QMessageBox.information(self, "Run in progress", "A run is already in progress.")
            return
        mode = self.settings_screen.selected_run_mode
        if not resume and mode == "copy_run":
            confirmed = QMessageBox.question(
                self,
                "Confirm copy run",
                "This will create Dropbox archive folders and server-side copied files.\n\n"
                "Originals will not be deleted or moved.\n\nContinue?",
            )
            if confirmed != QMessageBox.StandardButton.Yes:
                return
        try:
            auth_config = self._build_auth_config()
            job_config = None if resume else self._build_job_config(mode)
            state_db_path = self._resolve_latest_state_db() if resume else None
        except Exception as exc:  # noqa: BLE001
            self._show_error("Run cannot start", exc)
            return

        self.cancellation_token = CancellationToken()
        self.run_screen.reset(dry_run=(mode == "dry_run" and not resume))
        self.stack.setCurrentWidget(self.run_screen)

        worker = RunWorker(
            orchestrator=self.orchestrator,
            auth_config=auth_config,
            job_config=job_config,
            state_db_path=state_db_path,
            cancellation_token=self.cancellation_token,
            resume=resume,
        )
        thread = QThread(self)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.signals.progress.connect(self.run_screen.apply_progress)
        worker.signals.progress.connect(self._capture_run_dir_from_progress)
        worker.signals.log.connect(self.run_screen.append_log)
        worker.signals.result.connect(self._run_succeeded)
        worker.signals.error.connect(self._run_failed)
        worker.signals.finished.connect(thread.quit)
        worker.signals.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda thread=thread: self._threads.remove(thread) if thread in self._threads else None)
        thread.finished.connect(lambda worker=worker: self._workers.remove(worker) if worker in self._workers else None)
        self._threads.append(thread)
        self._workers.append(worker)
        thread.start()

    def resume_last_run(self) -> None:
        self.start_run(resume=True)

    def stop_run(self) -> None:
        if self.cancellation_token is not None:
            self.cancellation_token.cancel()
            self.run_screen.phase_label.setText("Stopping safely")
            self.run_screen.message_label.setText("The current item will finish, then the run can be resumed later.")

    def _run_succeeded(self, result: RunResult) -> None:
        self.current_run_result = result
        self.latest_run_dir = Path(result.run_dir)
        self.account_screen.set_resume_available(True)
        self.settings_screen.set_resume_available(True)
        self.run_screen.mark_completed()

    def _run_failed(self, message: str, details: str) -> None:
        self._load_latest_run_hint()
        self.run_screen.append_log(details)
        self.run_screen.mark_failed("The run stopped before completion. Originals were not deleted or moved.")
        ErrorDetailsDialog("Run needs attention", self._format_exception_for_user(message), details, self).exec()

    def _capture_run_dir_from_progress(self, snapshot) -> None:
        run_dir = snapshot.outputs.get("run_dir") if hasattr(snapshot, "outputs") else None
        if run_dir:
            self.latest_run_dir = Path(run_dir)

    def show_results_screen(self) -> None:
        if self.latest_run_dir is None:
            self.results_screen.set_empty()
        else:
            result = load_results_view_model(self.latest_run_dir)
            self.results_screen.set_result(result, self.latest_run_dir)
        self.stack.setCurrentWidget(self.results_screen)

    def open_output_folder(self) -> None:
        if self.latest_run_dir is None:
            QMessageBox.information(self, "No output folder yet", "Run the app first.")
            return
        self._open_path(self.latest_run_dir)

    def _open_named_output(self, filename: str) -> None:
        if self.latest_run_dir is None:
            return
        path = self.latest_run_dir / filename
        if path.exists():
            self._open_path(path)
        else:
            QMessageBox.information(self, "File not found", f"{filename} was not generated for this run.")

    def _open_manifest(self) -> None:
        if self.latest_run_dir is None:
            return
        for name in ("manifest_copy_run.csv", "manifest_dry_run.csv"):
            path = self.latest_run_dir / name
            if path.exists():
                self._open_path(path)
                return
        QMessageBox.information(self, "File not found", "No manifest was generated for this run.")

    def _build_auth_config(self) -> AuthConfig:
        saved = self.auth_manager.load_credentials("default")
        if saved is None:
            raise ValueError("Connect Dropbox first.")
        auth_config = self.auth_manager.credentials_to_auth_config(saved)
        auth_config.account_mode = self.account_mode  # type: ignore[assignment]
        admin_member_id = self.connection_screen.admin_member_id_edit.text().strip()
        if admin_member_id:
            auth_config.admin_member_id = admin_member_id
        return auth_config

    def _build_job_config(self, mode: str) -> JobConfig:
        return JobConfig(
            source_roots=self.settings_screen.source_roots(),
            excluded_roots=self.settings_screen.excluded_roots(),
            cutoff_date=self.settings_screen.cutoff_date,
            date_filter_field=date_filter_label_to_value(self.settings_screen.date_filter_label),
            archive_root=self.settings_screen.archive_root,
            output_dir=Path(self.settings_screen.output_dir).expanduser(),
            mode=run_label_to_value(mode) if mode not in ("inventory_only", "dry_run", "copy_run") else mode,  # type: ignore[arg-type]
            batch_size=int(self.settings_screen.batch_size.value()),
            retry=RetrySettings(
                max_retries=int(self.settings_screen.retry_count.value()),
                initial_backoff_seconds=float(self.settings_screen.initial_backoff.value()),
                backoff_multiplier=float(self.settings_screen.backoff_multiplier.value()),
                max_backoff_seconds=float(self.settings_screen.max_backoff.value()),
            ),
            conflict_policy=self.settings_screen.conflict_policy.currentText(),  # type: ignore[arg-type]
            include_folders_in_inventory=self.settings_screen.include_folders.isChecked(),
            exclude_archive_destination=self.settings_screen.exclude_archive.isChecked(),
            worker_count=int(self.settings_screen.worker_count.value()),
            verify_after_run=True,
            team_coverage_preset=team_coverage_label_to_value(self.settings_screen.team_coverage_label),
            team_archive_layout=team_archive_layout_label_to_value(self.settings_screen.team_archive_layout_label),
        )

    def _resolve_latest_state_db(self) -> Path:
        latest_pointer = Path(self.settings_screen.output_dir) / "latest_run.json"
        if not latest_pointer.exists():
            raise ValueError("Could not find latest_run.json in the selected output folder.")
        payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
        return Path(payload["state_db"])

    def _load_saved_credentials_hint(self) -> None:
        saved = self.auth_manager.load_credentials("default")
        if saved is None:
            return
        self.account_mode = saved.account_mode
        self.connection_screen.set_account_mode(saved.account_mode)
        self.settings_screen.set_account_mode(saved.account_mode)
        if saved.app_key and not self.connection_screen.app_key_edit.text():
            self.connection_screen.app_key_edit.setText(saved.app_key)
        if saved.admin_member_id:
            self.connection_screen.admin_member_id_edit.setText(saved.admin_member_id)
        self._connected_account_summary = "Saved Dropbox connection found. Press Use saved connection to continue."
        self.connection_screen.set_saved_credentials_available(
            True,
            "Press Use saved connection to keep using the Dropbox authorization saved on this computer, or connect a different account if needed.",
        )
        self.connection_screen.set_status(self._connected_account_summary)
        self.connection_screen.set_connected(False)

    def _load_latest_run_hint(self) -> None:
        latest_pointer = Path(self.settings_screen.output_dir) / "latest_run.json"
        if latest_pointer.exists():
            try:
                payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
                self.latest_run_dir = Path(payload["run_dir"])
            except Exception:  # noqa: BLE001
                self.latest_run_dir = None

    def _temporary_logger(self) -> logging.Logger:
        logger = logging.getLogger("dropbox_cleaner.ui.connection_test")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            logger.addHandler(logging.NullHandler())
        return logger

    def _format_exception_for_user(self, message: str | Exception) -> str:
        text = str(message)
        if isinstance(message, MissingScopeError) or "required scope" in text.casefold():
            scope_block = (
                "account_info.read, files.metadata.read, files.content.read, files.content.write, "
                "team_info.read, members.read, team_data.member, sharing.read, sharing.write, "
                "files.team_metadata.read, files.team_metadata.write, team_data.team_space."
                if self.account_mode == "team_admin"
                else "account_info.read, files.metadata.read, files.content.read, files.content.write."
            )
            return (
                "Dropbox permissions need one more step.\n\n"
                "Open the Dropbox App Console, enable the required scopes, save, then reconnect this app.\n\n"
                f"Required scopes: {scope_block}"
            )
        return text

    def _packaged_app_key(self) -> str | None:
        env_value = os.environ.get("DROPBOX_CLEANER_APP_KEY") or os.environ.get("DROPBOX_APP_KEY")
        if env_value:
            return env_value
        candidates: list[Path] = []
        if getattr(sys, "frozen", False):
            candidates.append(Path(sys.executable).with_name("dropbox_app_key.txt"))
            candidates.append(Path(sys.executable).parent.parent / "Resources" / "dropbox_app_key.txt")
        candidates.append(Path.cwd() / "dropbox_app_key.txt")
        for path in candidates:
            if path.exists():
                value = path.read_text(encoding="utf-8").strip()
                if value:
                    return value
        return None

    def _effective_app_key(self) -> str | None:
        return self._packaged_key or self.connection_screen.app_key_edit.text().strip() or None

    def _open_path(self, path: Path) -> None:
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(path.resolve())))

    def _is_worker_running(self) -> bool:
        return any(thread.isRunning() for thread in self._threads)

    def _show_simple_error(self, title: str, message: str) -> None:
        QMessageBox.warning(self, title, message)

    def _show_error(self, title: str, exc: Exception) -> None:
        ErrorDetailsDialog(title, self._format_exception_for_user(exc), repr(exc), self).exec()


def run_app() -> int:
    app = QApplication.instance()
    owns_app = app is None
    if app is None:
        app = QApplication(sys.argv)
    app.setApplicationName(theme.APP_TITLE)
    app.setStyleSheet(theme.app_stylesheet())
    window = DropboxCleanerMainWindow()
    window.show()
    if owns_app:
        return int(app.exec())
    return 0
