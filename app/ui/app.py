from __future__ import annotations

import json
import os
import platform
import subprocess
import threading
import traceback
import webbrowser
from pathlib import Path
from queue import Empty, Queue
from tkinter import END, BOTH, LEFT, BooleanVar, Listbox, StringVar, Text, Tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from app.dropbox_client.auth import AuthManager, default_scopes_for_mode
from app.dropbox_client.errors import MissingScopeError
from app.models.config import AuthConfig, JobConfig, RetrySettings
from app.models.events import ProgressSnapshot
from app.services.orchestrator import RunOrchestrator
from app.services.runtime import CancellationToken


AUTH_LABELS = {
    "oauth_pkce": "OAuth PKCE (recommended)",
    "refresh_token": "Refresh token",
    "access_token": "Access token",
}

ACCOUNT_MODE_LABELS = {
    "personal": "Personal",
    "team_admin": "Team Admin",
}

MODE_LABELS = {
    "inventory_only": "Inventory only",
    "dry_run": "Dry run",
    "copy_run": "Copy run",
    "resume_previous_run": "Resume previous run",
}

TEAM_COVERAGE_LABELS = {
    "all_team_content": "All team content",
    "team_owned_only": "Team-owned only",
}


class DropboxCleanerApp:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Dropbox Cleaner")
        self.root.geometry("1260x900")

        self.auth_manager = AuthManager()
        self.orchestrator = RunOrchestrator()
        self.progress_queue: Queue[tuple[str, object]] = Queue()
        self.log_queue: Queue[str] = Queue()
        self.cancellation_token: CancellationToken | None = None
        self.worker_thread: threading.Thread | None = None
        self.latest_run_dir: Path | None = None

        self.account_mode_var = StringVar(value="personal")
        self.auth_method_var = StringVar(value="oauth_pkce")
        self.app_key_var = StringVar()
        self.auth_code_var = StringVar()
        self.token_var = StringVar()
        self.admin_member_id_var = StringVar()
        self.account_info_var = StringVar(value="Not connected.")
        self.connection_help_var = StringVar()
        self.job_setup_hint_var = StringVar()

        self.source_root_var = StringVar()
        self.cutoff_date_var = StringVar(value="2020-05-01")
        self.date_filter_field_var = StringVar(value="server_modified")
        self.archive_root_var = StringVar(value="/Archive_PreMay2020")
        self.output_dir_var = StringVar(value=str(Path("outputs").resolve()))
        self.mode_var = StringVar(value="dry_run")
        self.team_coverage_var = StringVar(value="all_team_content")
        self.batch_size_var = StringVar(value="500")
        self.retry_count_var = StringVar(value="5")
        self.initial_backoff_var = StringVar(value="1.0")
        self.backoff_multiplier_var = StringVar(value="2.0")
        self.max_backoff_var = StringVar(value="30.0")
        self.conflict_policy_var = StringVar(value="safe_skip")
        self.include_folders_var = BooleanVar(value=True)
        self.exclude_archive_var = BooleanVar(value=True)
        self.worker_count_var = StringVar(value="1")

        self.phase_var = StringVar(value="Idle")
        self.items_scanned_var = StringVar(value="0")
        self.namespaces_scanned_var = StringVar(value="0")
        self.members_covered_var = StringVar(value="0")
        self.files_matched_var = StringVar(value="0")
        self.files_copied_var = StringVar(value="0")
        self.files_skipped_var = StringVar(value="0")
        self.files_failed_var = StringVar(value="0")
        self.last_output_var = StringVar(value="")
        self.dry_run_banner_var = StringVar(value="DRY RUN: no Dropbox changes will be made.")

        self._build_ui()
        self._apply_account_mode_ui()
        self._load_saved_credentials_hint()
        self._load_latest_run_hint()
        self.root.after(200, self._poll_queues)

    def _build_ui(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=BOTH, expand=True, padx=10, pady=10)

        self.connection_tab = ttk.Frame(notebook)
        self.setup_tab = ttk.Frame(notebook)
        self.run_tab = ttk.Frame(notebook)
        self.results_tab = ttk.Frame(notebook)
        notebook.add(self.connection_tab, text="Connection")
        notebook.add(self.setup_tab, text="Job Setup")
        notebook.add(self.run_tab, text="Run / Progress")
        notebook.add(self.results_tab, text="Results")

        self._build_connection_tab()
        self._build_setup_tab()
        self._build_run_tab()
        self._build_results_tab()

    def _build_connection_tab(self) -> None:
        frame = ttk.Frame(self.connection_tab, padding=12)
        frame.pack(fill=BOTH, expand=True)

        ttk.Label(frame, text="Account Mode").grid(row=0, column=0, sticky="w")
        mode_box = ttk.Combobox(
            frame,
            textvariable=self.account_mode_var,
            values=list(ACCOUNT_MODE_LABELS.keys()),
            state="readonly",
        )
        mode_box.grid(row=0, column=1, sticky="ew", padx=8, pady=4)
        mode_box.bind("<<ComboboxSelected>>", lambda _event: self._apply_account_mode_ui())

        ttk.Label(frame, text="Authentication Method").grid(row=1, column=0, sticky="w")
        method_box = ttk.Combobox(
            frame,
            textvariable=self.auth_method_var,
            values=list(AUTH_LABELS.keys()),
            state="readonly",
        )
        method_box.grid(row=1, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(frame, text="Dropbox App Key").grid(row=2, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.app_key_var, width=48).grid(row=2, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(frame, text="Authorization Code").grid(row=3, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.auth_code_var, width=48).grid(row=3, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(frame, text="Refresh or Access Token").grid(row=4, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.token_var, width=48, show="*").grid(row=4, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(frame, text="Admin Member ID").grid(row=5, column=0, sticky="w")
        self.admin_member_id_entry = ttk.Entry(frame, textvariable=self.admin_member_id_var, width=48)
        self.admin_member_id_entry.grid(row=5, column=1, sticky="ew", padx=8, pady=4)

        button_row = ttk.Frame(frame)
        button_row.grid(row=6, column=0, columnspan=2, sticky="w", pady=10)
        ttk.Button(button_row, text="Start OAuth", command=self.start_oauth).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Finish OAuth && Save", command=self.finish_oauth).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Save Token", command=self.save_manual_token).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Test Connection", command=self.test_connection).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Disconnect / Clear", command=self.clear_saved_credentials).pack(side=LEFT)

        ttk.Label(frame, text="Connected Account").grid(row=7, column=0, sticky="nw")
        ttk.Label(frame, textvariable=self.account_info_var, wraplength=880).grid(row=7, column=1, sticky="w", padx=8, pady=6)

        ttk.Label(frame, textvariable=self.connection_help_var, wraplength=980, justify=LEFT).grid(
            row=8,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(18, 0),
        )

        frame.columnconfigure(1, weight=1)

    def _build_setup_tab(self) -> None:
        frame = ttk.Frame(self.setup_tab, padding=12)
        frame.pack(fill=BOTH, expand=True)

        source_frame = ttk.LabelFrame(frame, text="Source Roots", padding=8)
        source_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))
        self.source_root_entry = ttk.Entry(source_frame, textvariable=self.source_root_var, width=36)
        self.source_root_entry.pack(side=LEFT, padx=(0, 8))
        self.add_source_button = ttk.Button(source_frame, text="Add", command=self.add_source_root)
        self.add_source_button.pack(side=LEFT, padx=(0, 8))
        self.remove_source_button = ttk.Button(source_frame, text="Remove Selected", command=self.remove_source_root)
        self.remove_source_button.pack(side=LEFT)
        self.source_roots_listbox = Listbox(source_frame, height=6)
        self.source_roots_listbox.pack(fill=BOTH, expand=True, pady=(10, 0))
        self.source_roots_listbox.insert(END, "/")

        settings_frame = ttk.LabelFrame(frame, text="Job Settings", padding=8)
        settings_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 8))

        ttk.Label(settings_frame, text="Cutoff Date").grid(row=0, column=0, sticky="w")
        ttk.Entry(settings_frame, textvariable=self.cutoff_date_var).grid(row=0, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(settings_frame, text="Date Filter Field").grid(row=1, column=0, sticky="w")
        ttk.Combobox(
            settings_frame,
            textvariable=self.date_filter_field_var,
            values=("server_modified", "client_modified", "oldest_modified"),
            state="readonly",
        ).grid(row=1, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(settings_frame, text="Archive Folder").grid(row=2, column=0, sticky="w")
        ttk.Entry(settings_frame, textvariable=self.archive_root_var).grid(row=2, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(settings_frame, text="Output Directory").grid(row=3, column=0, sticky="w")
        output_row = ttk.Frame(settings_frame)
        output_row.grid(row=3, column=1, sticky="ew", padx=8, pady=4)
        ttk.Entry(output_row, textvariable=self.output_dir_var).pack(side=LEFT, fill=BOTH, expand=True)
        ttk.Button(output_row, text="Browse", command=self.choose_output_dir).pack(side=LEFT, padx=(8, 0))

        ttk.Label(settings_frame, text="Mode").grid(row=4, column=0, sticky="w")
        ttk.Combobox(
            settings_frame,
            textvariable=self.mode_var,
            values=list(MODE_LABELS.keys()),
            state="readonly",
        ).grid(row=4, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(settings_frame, text="Team Coverage").grid(row=5, column=0, sticky="w")
        self.team_coverage_box = ttk.Combobox(
            settings_frame,
            textvariable=self.team_coverage_var,
            values=list(TEAM_COVERAGE_LABELS.keys()),
            state="readonly",
        )
        self.team_coverage_box.grid(row=5, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(settings_frame, textvariable=self.job_setup_hint_var, foreground="#1d3557", wraplength=640, justify=LEFT).grid(
            row=6,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(8, 0),
        )

        advanced_frame = ttk.LabelFrame(frame, text="Advanced", padding=8)
        advanced_frame.grid(row=0, column=1, rowspan=2, sticky="nsew")

        entries = [
            ("Batch Size", self.batch_size_var),
            ("Retry Count", self.retry_count_var),
            ("Initial Backoff", self.initial_backoff_var),
            ("Backoff Multiplier", self.backoff_multiplier_var),
            ("Max Backoff", self.max_backoff_var),
            ("Worker Count", self.worker_count_var),
        ]
        for idx, (label, variable) in enumerate(entries):
            ttk.Label(advanced_frame, text=label).grid(row=idx, column=0, sticky="w")
            ttk.Entry(advanced_frame, textvariable=variable).grid(row=idx, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(advanced_frame, text="Conflict Policy").grid(row=6, column=0, sticky="w")
        ttk.Combobox(
            advanced_frame,
            textvariable=self.conflict_policy_var,
            values=("safe_skip", "abort_run"),
            state="readonly",
        ).grid(row=6, column=1, sticky="ew", padx=8, pady=4)

        ttk.Checkbutton(
            advanced_frame,
            text="Include folders in inventory export",
            variable=self.include_folders_var,
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(8, 4))
        ttk.Checkbutton(
            advanced_frame,
            text="Exclude archive subtree from traversal",
            variable=self.exclude_archive_var,
        ).grid(row=8, column=0, columnspan=2, sticky="w", pady=(0, 8))

        ttk.Label(
            frame,
            text="Nothing will be deleted. Originals stay in place.",
            foreground="#155724",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(12, 0))

        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        settings_frame.columnconfigure(1, weight=1)
        advanced_frame.columnconfigure(1, weight=1)

    def _build_run_tab(self) -> None:
        frame = ttk.Frame(self.run_tab, padding=12)
        frame.pack(fill=BOTH, expand=True)

        button_row = ttk.Frame(frame)
        button_row.pack(fill="x", pady=(0, 10))
        ttk.Button(button_row, text="Start", command=self.start_run).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Stop / Cancel Gracefully", command=self.stop_run).pack(side=LEFT)

        ttk.Label(frame, textvariable=self.dry_run_banner_var, foreground="#b36b00").pack(anchor="w", pady=(0, 8))
        self.progress = ttk.Progressbar(frame, mode="indeterminate")
        self.progress.pack(fill="x", pady=(0, 8))

        ttk.Label(frame, textvariable=self.phase_var).pack(anchor="w", pady=(0, 8))

        counters = ttk.Frame(frame)
        counters.pack(fill="x", pady=(0, 10))
        counter_rows = (
            ("Items Scanned", self.items_scanned_var),
            ("Namespaces", self.namespaces_scanned_var),
            ("Members", self.members_covered_var),
            ("Files Matched", self.files_matched_var),
            ("Files Copied", self.files_copied_var),
            ("Files Skipped", self.files_skipped_var),
            ("Files Failed", self.files_failed_var),
        )
        for idx, (label, variable) in enumerate(counter_rows):
            ttk.Label(counters, text=f"{label}:").grid(row=0, column=idx * 2, sticky="w")
            ttk.Label(counters, textvariable=variable).grid(row=0, column=idx * 2 + 1, sticky="w", padx=(4, 14))

        ttk.Label(frame, text="Live Log").pack(anchor="w")
        self.log_text = ScrolledText(frame, height=22, state="disabled")
        self.log_text.pack(fill=BOTH, expand=True, pady=(4, 8))

        ttk.Label(frame, text="Last Output Path").pack(anchor="w")
        ttk.Label(frame, textvariable=self.last_output_var, wraplength=1120).pack(anchor="w")

    def _build_results_tab(self) -> None:
        frame = ttk.Frame(self.results_tab, padding=12)
        frame.pack(fill=BOTH, expand=True)

        button_row = ttk.Frame(frame)
        button_row.pack(fill="x", pady=(0, 10))
        ttk.Button(button_row, text="Open Output Folder", command=self.open_output_folder).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Refresh Results", command=self.refresh_results).pack(side=LEFT, padx=(0, 8))
        ttk.Button(button_row, text="Resume Last Run", command=self.resume_last_run).pack(side=LEFT)

        ttk.Label(frame, text="Generated Files").pack(anchor="w")
        self.generated_files = Listbox(frame, height=8)
        self.generated_files.pack(fill="x", pady=(4, 10))

        ttk.Label(frame, text="Summary Preview").pack(anchor="w")
        self.summary_text = ScrolledText(frame, height=14, state="disabled")
        self.summary_text.pack(fill=BOTH, expand=True, pady=(4, 10))

        ttk.Label(frame, text="Conflicts / Failures Preview").pack(anchor="w")
        self.issues_text = ScrolledText(frame, height=10, state="disabled")
        self.issues_text.pack(fill=BOTH, expand=True, pady=(4, 0))

    def _apply_account_mode_ui(self) -> None:
        is_team_admin = self.account_mode_var.get() == "team_admin"
        source_state = "disabled" if is_team_admin else "normal"
        listbox_state = "disabled" if is_team_admin else "normal"
        self.source_root_entry.configure(state=source_state)
        self.add_source_button.configure(state=source_state)
        self.remove_source_button.configure(state=source_state)
        self.source_roots_listbox.configure(state=listbox_state)
        self.team_coverage_box.configure(state="readonly" if is_team_admin else "disabled")
        self.admin_member_id_entry.configure(state="normal" if is_team_admin else "disabled")
        if is_team_admin:
            self.job_setup_hint_var.set(
                "Team Admin mode inventories the whole Dropbox team from a single admin-authorized app. "
                "Source roots are not used in this mode; coverage is controlled by the Team Coverage preset. "
                "Use Date Filter Field = client_modified or oldest_modified when Dropbox shows old file dates but server_modified is recent."
            )
            self.connection_help_var.set(
                "Connection Help\n"
                "Use Team Admin mode with a team-linked Dropbox app and OAuth PKCE. Recommended scopes: "
                "account_info.read, files.metadata.read, files.content.read, files.content.write, "
                "team_info.read, members.read, team_data.member, sharing.read, sharing.write, "
                "files.team_metadata.read, files.team_metadata.write, team_data.team_space.\n"
                "Nothing is deleted by this app. The initial workflow inventories team content and stages server-side copies."
            )
            if self.source_roots_listbox.size() == 0:
                self.source_roots_listbox.insert(END, "/")
        else:
            self.job_setup_hint_var.set(
                "Personal mode inventories the selected Dropbox roots and stages server-side copies into a dedicated archive folder. "
                "Use Date Filter Field = client_modified or oldest_modified when Dropbox shows old file dates but server_modified is recent."
            )
            self.connection_help_var.set(
                "Connection Help\n"
                "Use OAuth PKCE for a secure local desktop flow. Required Dropbox scopes: "
                "account_info.read, files.metadata.read, files.content.read, files.content.write.\n"
                "Nothing is deleted by this app. The initial workflow only inventories files and stages server-side copies."
            )

    def add_source_root(self) -> None:
        value = self.source_root_var.get().strip()
        if not value:
            return
        self.source_roots_listbox.insert(END, value)
        self.source_root_var.set("")

    def remove_source_root(self) -> None:
        selection = list(self.source_roots_listbox.curselection())
        for index in reversed(selection):
            self.source_roots_listbox.delete(index)

    def choose_output_dir(self) -> None:
        chosen = filedialog.askdirectory(initialdir=self.output_dir_var.get() or ".")
        if chosen:
            self.output_dir_var.set(chosen)

    def start_oauth(self) -> None:
        app_key = self.app_key_var.get().strip()
        if not app_key:
            messagebox.showerror("Missing App Key", "Enter your Dropbox app key first.")
            return
        account_mode = self.account_mode_var.get()
        authorize_url = self.auth_manager.start_pkce_flow(
            app_key,
            default_scopes_for_mode(account_mode),
            account_mode=account_mode,
            label="default",
        )
        webbrowser.open(authorize_url)
        self.account_info_var.set("Authorization started. Approve the app in your browser, then paste the code here.")

    def finish_oauth(self) -> None:
        try:
            credentials = self.auth_manager.finish_pkce_flow(self.auth_code_var.get().strip(), label="default")
            if self.admin_member_id_var.get().strip():
                credentials.admin_member_id = self.admin_member_id_var.get().strip()
            self.auth_manager.save_credentials("default", credentials)
            self.account_mode_var.set(credentials.account_mode)
            self.token_var.set("")
            self._apply_account_mode_ui()
            self.account_info_var.set("OAuth credentials saved. Use Test Connection to confirm account or team details.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("OAuth Failed", str(exc))

    def save_manual_token(self) -> None:
        token = self.token_var.get().strip()
        if not token:
            messagebox.showerror("Missing Token", "Enter a refresh token or access token first.")
            return
        method = self.auth_method_var.get()
        app_key = self.app_key_var.get().strip() or None
        account_mode = self.account_mode_var.get()
        if method in ("refresh_token", "oauth_pkce") and not app_key:
            messagebox.showerror("Missing App Key", "A Dropbox app key is required for refresh-token auth.")
            return
        credentials = self.auth_manager.save_manual_token(
            method="refresh_token" if method in ("refresh_token", "oauth_pkce") else "access_token",
            account_mode=account_mode,
            app_key=app_key,
            refresh_token=token if method in ("refresh_token", "oauth_pkce") else None,
            access_token=token if method == "access_token" else None,
            admin_member_id=self.admin_member_id_var.get().strip() or None,
        )
        self.account_info_var.set(f"Saved {ACCOUNT_MODE_LABELS[credentials.account_mode]} {AUTH_LABELS[credentials.method]} credentials locally.")

    def clear_saved_credentials(self) -> None:
        self.auth_manager.clear_credentials("default")
        self.account_info_var.set("Saved credentials cleared.")
        self.token_var.set("")
        self.auth_code_var.set("")
        self.admin_member_id_var.set("")

    def test_connection(self) -> None:
        try:
            auth_config = self._build_auth_config()
            account = self.auth_manager.test_connection(auth_config, self._temporary_logger())
            if account.account_mode == "team_admin":
                self.account_info_var.set(
                    f"{account.display_name} ({account.email or 'no email returned'})\n"
                    f"Team: {account.team_name or 'Unknown'}\n"
                    f"Model: {account.team_model or 'Unknown'}\n"
                    f"Active Members: {account.active_member_count}\n"
                    f"Namespaces: {account.namespace_count}"
                )
            else:
                self.account_info_var.set(f"{account.display_name} ({account.email or 'no email returned'})")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Connection Failed", self._format_exception_for_user(exc))

    def start_run(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("Run In Progress", "A run is already in progress.")
            return

        mode = self.mode_var.get()
        if mode == "copy_run":
            confirmed = messagebox.askyesno(
                "Confirm Copy Run",
                "This will create Dropbox archive folders and server-side copied files.\n\nOriginals will not be deleted or moved.\n\nContinue?",
            )
            if not confirmed:
                return

        try:
            auth_config = self._build_auth_config()
            job_config = self._build_job_config(mode if mode != "resume_previous_run" else "copy_run")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Invalid Settings", str(exc))
            return

        self._clear_text(self.log_text)
        self.phase_var.set("Starting")
        self.progress.start(8)
        self.dry_run_banner_var.set(
            "DRY RUN: no Dropbox changes will be made."
            if mode == "dry_run"
            else "Nothing will be deleted. Originals remain in place."
        )
        self.cancellation_token = CancellationToken()

        def worker() -> None:
            try:
                if mode == "resume_previous_run":
                    state_db = self._resolve_latest_state_db()
                    result = self.orchestrator.resume(
                        state_db_path=state_db,
                        auth_config=auth_config,
                        emit=self._emit_progress,
                        cancellation_token=self.cancellation_token,
                        ui_log_queue=self.log_queue,
                    )
                else:
                    result = self.orchestrator.run(
                        job_config=job_config,
                        auth_config=auth_config,
                        emit=self._emit_progress,
                        cancellation_token=self.cancellation_token,
                        ui_log_queue=self.log_queue,
                    )
                self.progress_queue.put(("result", result))
            except Exception as exc:  # noqa: BLE001
                self.progress_queue.put(("error", {"message": self._format_exception_for_user(exc), "traceback": traceback.format_exc()}))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def stop_run(self) -> None:
        if self.cancellation_token is not None:
            self.cancellation_token.cancel()
            self.phase_var.set("Cancellation requested")

    def resume_last_run(self) -> None:
        self.mode_var.set("resume_previous_run")
        self.start_run()

    def open_output_folder(self) -> None:
        if self.latest_run_dir is None:
            messagebox.showinfo("No Run Yet", "Run the app first, or refresh results from an existing output directory.")
            return
        self._open_path(self.latest_run_dir)

    def refresh_results(self) -> None:
        if self.latest_run_dir is None:
            self._load_latest_run_hint()
        if self.latest_run_dir is None:
            return
        self.generated_files.delete(0, END)
        for file_path in sorted(self.latest_run_dir.glob("*")):
            self.generated_files.insert(END, file_path.name)
        summary_path = self.latest_run_dir / "summary.md"
        summary_json = self.latest_run_dir / "summary.json"
        issues_payload = ""
        if summary_path.exists():
            self._set_text(self.summary_text, summary_path.read_text(encoding="utf-8"))
        elif summary_json.exists():
            self._set_text(self.summary_text, summary_json.read_text(encoding="utf-8"))
        else:
            self._set_text(self.summary_text, "No summary file found yet.")
        if summary_json.exists():
            payload = json.loads(summary_json.read_text(encoding="utf-8"))
            issues_payload = "\n".join(
                payload.get("conflicts_preview", []) + payload.get("failures_preview", []) + payload.get("blocked_preview", [])
            )
        self._set_text(self.issues_text, issues_payload or "No conflicts or failures recorded.")

    def _build_auth_config(self) -> AuthConfig:
        method = self.auth_method_var.get()
        app_key = self.app_key_var.get().strip() or None
        token = self.token_var.get().strip()
        account_mode = self.account_mode_var.get()
        admin_member_id = self.admin_member_id_var.get().strip() or None
        if method == "access_token" and token:
            return AuthConfig(method="access_token", account_mode=account_mode, access_token=token, admin_member_id=admin_member_id)
        if method in ("refresh_token", "oauth_pkce") and token:
            return AuthConfig(
                method="refresh_token",
                account_mode=account_mode,
                app_key=app_key,
                refresh_token=token,
                scopes=default_scopes_for_mode(account_mode),
                admin_member_id=admin_member_id,
            )
        saved = self.auth_manager.load_credentials("default")
        if saved is None:
            raise ValueError("No saved Dropbox credentials were found. Use OAuth or save a token first.")
        auth_config = self.auth_manager.credentials_to_auth_config(saved)
        auth_config.account_mode = account_mode
        if admin_member_id:
            auth_config.admin_member_id = admin_member_id
        return auth_config

    def _build_job_config(self, mode: str) -> JobConfig:
        source_roots = list(self.source_roots_listbox.get(0, END))
        if self.account_mode_var.get() == "personal" and mode != "resume_previous_run" and not source_roots:
            raise ValueError("Add at least one source root.")
        return JobConfig(
            source_roots=source_roots or ["/"],
            cutoff_date=self.cutoff_date_var.get().strip(),
            date_filter_field=self.date_filter_field_var.get(),  # type: ignore[arg-type]
            archive_root=self.archive_root_var.get().strip(),
            output_dir=Path(self.output_dir_var.get()).expanduser(),
            mode=mode,  # type: ignore[arg-type]
            batch_size=int(self.batch_size_var.get()),
            retry=RetrySettings(
                max_retries=int(self.retry_count_var.get()),
                initial_backoff_seconds=float(self.initial_backoff_var.get()),
                backoff_multiplier=float(self.backoff_multiplier_var.get()),
                max_backoff_seconds=float(self.max_backoff_var.get()),
            ),
            conflict_policy=self.conflict_policy_var.get(),  # type: ignore[arg-type]
            include_folders_in_inventory=self.include_folders_var.get(),
            exclude_archive_destination=self.exclude_archive_var.get(),
            worker_count=int(self.worker_count_var.get()),
            verify_after_run=True,
            team_coverage_preset=self.team_coverage_var.get(),  # type: ignore[arg-type]
        )

    def _emit_progress(self, snapshot: ProgressSnapshot) -> None:
        self.progress_queue.put(("progress", snapshot))

    def _poll_queues(self) -> None:
        while True:
            try:
                kind, payload = self.progress_queue.get_nowait()
            except Empty:
                break
            if kind == "progress":
                self._apply_progress(payload)  # type: ignore[arg-type]
            elif kind == "result":
                self.progress.stop()
                self.phase_var.set("Completed")
                self.last_output_var.set(payload.run_dir)  # type: ignore[attr-defined]
                self.latest_run_dir = Path(payload.run_dir)  # type: ignore[attr-defined]
                self.refresh_results()
            elif kind == "error":
                self.progress.stop()
                self.phase_var.set("Failed")
                if isinstance(payload, dict):
                    self._append_log(payload.get("traceback", ""))
                    messagebox.showerror("Run Failed", payload.get("message", "Unknown error"))
                else:
                    messagebox.showerror("Run Failed", str(payload))
        while True:
            try:
                line = self.log_queue.get_nowait()
            except Empty:
                break
            self._append_log(line)
        self.root.after(200, self._poll_queues)

    def _apply_progress(self, snapshot: ProgressSnapshot) -> None:
        self.phase_var.set(f"{snapshot.phase}: {snapshot.message}")
        counters = snapshot.counters
        self.items_scanned_var.set(str(counters.get("items_scanned", 0)))
        self.namespaces_scanned_var.set(str(counters.get("namespaces_scanned", 0)))
        self.members_covered_var.set(str(counters.get("members_covered", 0)))
        self.files_matched_var.set(str(counters.get("files_matched", 0)))
        self.files_copied_var.set(str(counters.get("files_copied", 0)))
        self.files_skipped_var.set(str(counters.get("files_skipped", 0)))
        self.files_failed_var.set(str(counters.get("files_failed", 0)))
        if snapshot.outputs.get("run_dir"):
            self.last_output_var.set(snapshot.outputs["run_dir"])

    def _append_log(self, line: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert(END, line + "\n")
        self.log_text.see(END)
        self.log_text.configure(state="disabled")

    def _clear_text(self, widget: Text) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", END)
        widget.configure(state="disabled")

    def _set_text(self, widget: Text, content: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", END)
        widget.insert("1.0", content)
        widget.configure(state="disabled")

    def _open_path(self, path: Path) -> None:
        system = platform.system()
        if system == "Windows":
            os.startfile(path)  # type: ignore[attr-defined]
        elif system == "Darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])

    def _resolve_latest_state_db(self) -> Path:
        latest_pointer = Path(self.output_dir_var.get()) / "latest_run.json"
        if not latest_pointer.exists():
            raise ValueError("Could not find latest_run.json in the selected output directory.")
        payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
        return Path(payload["state_db"])

    def _load_saved_credentials_hint(self) -> None:
        saved = self.auth_manager.load_credentials("default")
        if saved is not None:
            self.auth_method_var.set(saved.method)
            self.account_mode_var.set(saved.account_mode)
            if saved.app_key:
                self.app_key_var.set(saved.app_key)
            if saved.admin_member_id:
                self.admin_member_id_var.set(saved.admin_member_id)
            self._apply_account_mode_ui()
            self.account_info_var.set("Saved Dropbox credentials found. Use Test Connection to confirm account or team details.")

    def _load_latest_run_hint(self) -> None:
        latest_pointer = Path(self.output_dir_var.get()) / "latest_run.json"
        if latest_pointer.exists():
            payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
            self.latest_run_dir = Path(payload["run_dir"])
            self.last_output_var.set(str(self.latest_run_dir))
            self.refresh_results()

    def _temporary_logger(self):
        import logging

        logger = logging.getLogger("dropbox_cleaner.ui.connection_test")
        if logger.handlers:
            return logger
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.NullHandler())
        return logger

    def _format_exception_for_user(self, exc: Exception) -> str:
        if isinstance(exc, MissingScopeError) or "required scope" in str(exc).casefold():
            required_scope = getattr(exc, "required_scope", None) or "unknown"
            scope_block = (
                "account_info.read, files.metadata.read, files.content.read, files.content.write, "
                "team_info.read, members.read, team_data.member, sharing.read, sharing.write, "
                "files.team_metadata.read, files.team_metadata.write, team_data.team_space."
                if self.account_mode_var.get() == "team_admin"
                else "account_info.read, files.metadata.read, files.content.read, files.content.write."
            )
            return (
                "Dropbox app permissions are incomplete.\n\n"
                f"Missing required scope: {required_scope}\n\n"
                "Fix this in the Dropbox App Console:\n"
                "1. Open your Dropbox app.\n"
                "2. Go to the Permissions tab.\n"
                "3. Enable the missing scope, plus the other required scopes:\n"
                f"   {scope_block}\n"
                "4. Save the app settings.\n"
                "5. In Dropbox Cleaner, click Disconnect / Clear.\n"
                "6. Run Start OAuth and Finish OAuth & Save again.\n"
                "7. Click Test Connection again.\n\n"
                "Changing scopes on the Dropbox app is not enough by itself. You must reconnect so Dropbox issues a token with the updated scopes."
            )
        return str(exc)


def run_app() -> int:
    root = Tk()
    style = ttk.Style(root)
    if "vista" in style.theme_names():
        style.theme_use("vista")
    app = DropboxCleanerApp(root)
    root.mainloop()
    return 0
