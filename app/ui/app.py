from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import threading
import traceback
import webbrowser
from pathlib import Path
from queue import Empty, Queue
from tkinter import BooleanVar, Canvas, StringVar, filedialog, messagebox

import customtkinter as ctk
from tkcalendar import DateEntry

from app.dropbox_client.adapter import DropboxAdapter
from app.dropbox_client.auth import AuthManager, default_scopes_for_mode
from app.dropbox_client.errors import MissingScopeError
from app.models.config import AuthConfig, JobConfig, RetrySettings
from app.models.events import ProgressSnapshot
from app.services.orchestrator import RunOrchestrator
from app.services.runtime import CancellationToken
from app.ui.folder_browser import BrowserLocation, DropboxFolderBrowserService
from app.ui.options import (
    ACCOUNT_CHOICES,
    DATE_FILTER_CHOICES,
    RUN_MODE_CHOICES,
    TEAM_COVERAGE_CHOICES,
    date_filter_label_to_value,
    date_filter_value_to_label,
    run_label_to_value,
    run_value_to_label,
    team_coverage_label_to_value,
    team_coverage_value_to_label,
)
from app.ui.results import ResultsViewModel, load_results_view_model


APP_TITLE = "Dropbox Cleaner"
ACCENT = "#1D7A8C"
INK = "#183039"
MUTED = "#65777D"
SOFT = "#EEF7F6"
SUCCESS = "#2E7D5B"
WARNING = "#C07A2C"
DANGER = "#C84C4C"


class DropboxCleanerApp:
    def __init__(self, root: ctk.CTk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1280x860")
        self.root.minsize(1080, 720)

        self.auth_manager = AuthManager()
        self.orchestrator = RunOrchestrator()
        self.progress_queue: Queue[tuple[str, object]] = Queue()
        self.log_queue: Queue[str] = Queue()
        self.cancellation_token: CancellationToken | None = None
        self.worker_thread: threading.Thread | None = None
        self.latest_run_dir: Path | None = None
        self.connected_account_summary: str = "Not connected yet."
        self.technical_log_lines: list[str] = []

        self.account_mode_var = StringVar(value="personal")
        self.auth_code_var = StringVar()
        self.token_var = StringVar()
        self.app_key_var = StringVar(value=self._packaged_app_key() or "")
        self.admin_member_id_var = StringVar()
        self.cutoff_date_var = StringVar(value="2020-05-01")
        self.date_filter_label_var = StringVar(value=date_filter_value_to_label("server_modified"))
        self.archive_root_var = StringVar(value="/Archive_PreMay2020")
        self.output_dir_var = StringVar(value=str(Path("outputs").resolve()))
        self.run_mode_label_var = StringVar(value=run_value_to_label("dry_run"))
        self.team_coverage_label_var = StringVar(value=team_coverage_value_to_label("all_team_content"))
        self.batch_size_var = StringVar(value="500")
        self.retry_count_var = StringVar(value="5")
        self.initial_backoff_var = StringVar(value="1.0")
        self.backoff_multiplier_var = StringVar(value="2.0")
        self.max_backoff_var = StringVar(value="30.0")
        self.conflict_policy_var = StringVar(value="safe_skip")
        self.include_folders_var = BooleanVar(value=True)
        self.exclude_archive_var = BooleanVar(value=True)
        self.worker_count_var = StringVar(value="1")

        self.phase_var = StringVar(value="Ready")
        self.phase_detail_var = StringVar(value="Choose an account type to begin.")
        self.dry_run_banner_var = StringVar(value="Preview mode makes no Dropbox changes.")
        self.last_output_var = StringVar(value="")
        self.current_run_result = None
        self.source_roots: list[str] = ["/"]

        self._build_shell()
        self._load_saved_credentials_hint()
        self._load_latest_run_hint()
        self.show_account_screen()
        self.root.after(200, self._poll_queues)

    def _build_shell(self) -> None:
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

        self.shell = ctk.CTkFrame(self.root, fg_color="#F7FAF8", corner_radius=0)
        self.shell.grid(row=0, column=0, sticky="nsew")
        self.shell.grid_columnconfigure(0, weight=1)
        self.shell.grid_rowconfigure(1, weight=1)

        self.header = ctk.CTkFrame(self.shell, fg_color="#F7FAF8", corner_radius=0)
        self.header.grid(row=0, column=0, sticky="ew", padx=28, pady=(22, 10))
        self.header.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(
            self.header,
            text=APP_TITLE,
            text_color=INK,
            font=ctk.CTkFont(size=24, weight="bold"),
        ).grid(row=0, column=0, sticky="w")
        self.step_label = ctk.CTkLabel(
            self.header,
            text="Start",
            text_color=MUTED,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        self.step_label.grid(row=0, column=1, sticky="e")

        self.content = ctk.CTkFrame(self.shell, fg_color="#F7FAF8", corner_radius=0)
        self.content.grid(row=1, column=0, sticky="nsew", padx=28, pady=(0, 28))
        self.content.grid_columnconfigure(0, weight=1)
        self.content.grid_rowconfigure(0, weight=1)

    def _clear_content(self) -> None:
        for widget in self.content.winfo_children():
            widget.destroy()

    def show_account_screen(self) -> None:
        self._clear_content()
        self.step_label.configure(text="Step 1 of 5")
        frame = self._screen_frame()
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)

        hero = ctk.CTkFrame(frame, fg_color="#DDF1ED", corner_radius=30)
        hero.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=4, pady=(4, 24))
        hero.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            hero,
            text="Archive Dropbox files without deleting anything.",
            text_color=INK,
            font=ctk.CTkFont(size=38, weight="bold"),
            wraplength=780,
        ).grid(row=0, column=0, sticky="w", padx=44, pady=(42, 8))
        ctk.CTkLabel(
            hero,
            text="Choose the account type, connect securely, preview the archive, then copy only when you are ready.",
            text_color=MUTED,
            font=ctk.CTkFont(size=17),
            wraplength=760,
        ).grid(row=1, column=0, sticky="w", padx=44, pady=(0, 34))
        self._draw_cloud_hero(hero).grid(row=0, column=1, rowspan=2, padx=34, pady=28)

        for index, choice in enumerate(ACCOUNT_CHOICES):
            self._choice_panel(
                frame,
                row=1,
                column=index,
                title=choice.label,
                body=choice.description,
                button_text="Use this account type",
                command=lambda value=choice.value: self._select_account_mode(value),
            )

        if self.latest_run_dir is not None:
            ctk.CTkButton(
                frame,
                text="Resume last run",
                fg_color="#FFFFFF",
                text_color=ACCENT,
                border_width=1,
                border_color="#BCD8D5",
                hover_color="#E8F4F2",
                command=self.resume_last_run,
            ).grid(row=2, column=0, sticky="w", padx=8, pady=(28, 0))

    def _select_account_mode(self, value: str) -> None:
        self.account_mode_var.set(value)
        self.show_connection_screen()

    def show_connection_screen(self) -> None:
        self._clear_content()
        self.step_label.configure(text="Step 2 of 5")
        frame = self._screen_frame()
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)

        left = ctk.CTkFrame(frame, fg_color="#FFFFFF", corner_radius=28)
        left.grid(row=0, column=0, sticky="nsew", padx=(4, 14), pady=4)
        left.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            left,
            text="Connect Dropbox",
            text_color=INK,
            font=ctk.CTkFont(size=32, weight="bold"),
        ).pack(anchor="w", padx=34, pady=(34, 8))
        mode_copy = "team admin app" if self.account_mode_var.get() == "team_admin" else "personal Dropbox app"
        ctk.CTkLabel(
            left,
            text=f"We open Dropbox in your browser, you approve this {mode_copy}, then paste the code here.",
            text_color=MUTED,
            font=ctk.CTkFont(size=16),
            wraplength=520,
            justify="left",
        ).pack(anchor="w", padx=34, pady=(0, 24))

        if not self._packaged_app_key():
            ctk.CTkLabel(left, text="Dropbox app key", text_color=INK, font=ctk.CTkFont(size=14, weight="bold")).pack(
                anchor="w", padx=34
            )
            ctk.CTkEntry(left, textvariable=self.app_key_var, placeholder_text="Paste your Dropbox app key").pack(
                fill="x", padx=34, pady=(6, 18)
            )

        ctk.CTkButton(
            left,
            text="Connect Dropbox",
            height=48,
            fg_color=ACCENT,
            hover_color="#155F6E",
            font=ctk.CTkFont(size=16, weight="bold"),
            command=self.start_oauth,
        ).pack(fill="x", padx=34, pady=(0, 18))

        ctk.CTkLabel(left, text="Authorization code", text_color=INK, font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=34
        )
        ctk.CTkEntry(left, textvariable=self.auth_code_var, placeholder_text="Paste the code Dropbox gives you").pack(
            fill="x", padx=34, pady=(6, 12)
        )
        ctk.CTkButton(
            left,
            text="Finish connection",
            height=42,
            fg_color="#12343B",
            hover_color="#0F2A30",
            command=self.finish_oauth,
        ).pack(fill="x", padx=34, pady=(0, 18))

        self.connection_status_label = ctk.CTkLabel(
            left,
            text=self.connected_account_summary,
            text_color=SUCCESS if self.connected_account_summary != "Not connected yet." else MUTED,
            font=ctk.CTkFont(size=14),
            wraplength=520,
            justify="left",
        )
        self.connection_status_label.pack(anchor="w", padx=34, pady=(8, 24))

        saved_actions = ctk.CTkFrame(left, fg_color="transparent")
        saved_actions.pack(fill="x", padx=34, pady=(0, 16))
        ctk.CTkButton(
            saved_actions,
            text="Test saved connection",
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=self.test_saved_connection,
        ).pack(side="left", fill="x", expand=True, padx=(0, 8))
        ctk.CTkButton(
            saved_actions,
            text="Disconnect",
            fg_color="#FFFFFF",
            text_color=DANGER,
            border_width=1,
            border_color="#E3C5C5",
            hover_color="#F8ECEC",
            command=self.clear_saved_credentials,
        ).pack(side="left", fill="x", expand=True, padx=(8, 0))

        ctk.CTkButton(
            left,
            text="Continue to settings",
            height=46,
            fg_color=SUCCESS,
            hover_color="#246A4D",
            command=self.show_settings_screen,
        ).pack(fill="x", padx=34, pady=(0, 34))

        right = ctk.CTkFrame(frame, fg_color=SOFT, corner_radius=28)
        right.grid(row=0, column=1, sticky="nsew", padx=(14, 4), pady=4)
        self._draw_connection_graphic(right).pack(pady=(42, 18))
        ctk.CTkLabel(
            right,
            text="Safe by design",
            text_color=INK,
            font=ctk.CTkFont(size=24, weight="bold"),
        ).pack(anchor="w", padx=34, pady=(0, 8))
        bullets = [
            "No Dropbox password is requested.",
            "Nothing is deleted or moved.",
            "Preview mode makes no Dropbox changes.",
            "Credentials are saved locally when possible.",
        ]
        for bullet in bullets:
            ctk.CTkLabel(right, text=f"✓ {bullet}", text_color=INK, font=ctk.CTkFont(size=15)).pack(
                anchor="w", padx=34, pady=4
            )

        self.advanced_connection = ctk.CTkFrame(right, fg_color="#FFFFFF", corner_radius=18)
        ctk.CTkButton(
            right,
            text="Advanced connection options",
            fg_color="transparent",
            text_color=ACCENT,
            hover_color="#D9EFEB",
            command=self._toggle_advanced_connection,
        ).pack(anchor="w", padx=26, pady=(28, 6))

    def _toggle_advanced_connection(self) -> None:
        if self.advanced_connection.winfo_ismapped():
            self.advanced_connection.pack_forget()
            return
        self.advanced_connection.pack(fill="x", padx=26, pady=(0, 24))
        for widget in self.advanced_connection.winfo_children():
            widget.destroy()
        ctk.CTkLabel(
            self.advanced_connection,
            text="Manual token connection",
            text_color=INK,
            font=ctk.CTkFont(size=16, weight="bold"),
        ).pack(anchor="w", padx=18, pady=(16, 6))
        ctk.CTkEntry(self.advanced_connection, textvariable=self.token_var, show="*", placeholder_text="Refresh or access token").pack(
            fill="x", padx=18, pady=6
        )
        if self.account_mode_var.get() == "team_admin":
            ctk.CTkEntry(
                self.advanced_connection,
                textvariable=self.admin_member_id_var,
                placeholder_text="Optional admin member ID override",
            ).pack(fill="x", padx=18, pady=6)
        ctk.CTkButton(
            self.advanced_connection,
            text="Save token and test",
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_color="#B9D8D5",
            border_width=1,
            hover_color="#E8F4F2",
            command=self.save_manual_token,
        ).pack(fill="x", padx=18, pady=(8, 16))

    def show_settings_screen(self) -> None:
        self._clear_content()
        self.step_label.configure(text="Step 3 of 5")
        frame = self._screen_frame()
        frame.grid_columnconfigure(0, weight=3)
        frame.grid_columnconfigure(1, weight=2)

        main = ctk.CTkScrollableFrame(frame, fg_color="#FFFFFF", corner_radius=28)
        main.grid(row=0, column=0, sticky="nsew", padx=(4, 14), pady=4)
        main.grid_columnconfigure(0, weight=1)
        side = ctk.CTkFrame(frame, fg_color=SOFT, corner_radius=28)
        side.grid(row=0, column=1, sticky="nsew", padx=(14, 4), pady=4)
        side.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(main, text="Run settings", text_color=INK, font=ctk.CTkFont(size=32, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=30, pady=(28, 4)
        )
        ctk.CTkLabel(
            main,
            text="Pick what to archive. Originals always stay where they are.",
            text_color=MUTED,
            font=ctk.CTkFont(size=15),
        ).grid(row=1, column=0, sticky="w", padx=30, pady=(0, 22))

        self._build_date_section(main, row=2)
        self._build_archive_section(main, row=3)
        if self.account_mode_var.get() == "personal":
            self._build_source_section(main, row=4)
        else:
            self._build_team_section(main, row=4)
        self._build_output_section(main, row=5)
        self._build_advanced_settings(main, row=6)

        ctk.CTkLabel(side, text="Choose the run", text_color=INK, font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=26, pady=(28, 8)
        )
        ctk.CTkLabel(
            side,
            text="Start with Preview archive if you want to confirm everything before copying.",
            text_color=MUTED,
            font=ctk.CTkFont(size=14),
            wraplength=360,
            justify="left",
        ).grid(row=1, column=0, sticky="w", padx=26, pady=(0, 18))
        self.run_choice_buttons: dict[str, ctk.CTkButton] = {}
        for idx, choice in enumerate(RUN_MODE_CHOICES):
            self._run_choice(side, choice.label, choice.description, idx + 2)
        self._refresh_run_choice_buttons()

        ctk.CTkButton(
            side,
            text="Start run",
            height=52,
            fg_color=ACCENT,
            hover_color="#155F6E",
            font=ctk.CTkFont(size=17, weight="bold"),
            command=self.start_run_from_settings,
        ).grid(row=6, column=0, sticky="ew", padx=26, pady=(24, 12))
        ctk.CTkLabel(
            side,
            text="Nothing will be deleted. Copy mode only creates archive copies.",
            text_color=SUCCESS,
            font=ctk.CTkFont(size=13, weight="bold"),
            wraplength=360,
        ).grid(row=7, column=0, sticky="w", padx=26, pady=(0, 20))

        if self.latest_run_dir is not None:
            ctk.CTkButton(
                side,
                text="Resume last run",
                fg_color="#FFFFFF",
                text_color=ACCENT,
                border_width=1,
                border_color="#BCD8D5",
                hover_color="#E8F4F2",
                command=self.resume_last_run,
            ).grid(row=8, column=0, sticky="ew", padx=26, pady=(0, 22))

    def _build_date_section(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Cutoff date", "Files older than this date will be included.")
        date_row = ctk.CTkFrame(section, fg_color="transparent")
        date_row.pack(fill="x", padx=20, pady=(0, 14))
        DateEntry(
            date_row,
            textvariable=self.cutoff_date_var,
            date_pattern="yyyy-mm-dd",
            width=16,
            background=ACCENT,
            foreground="white",
            borderwidth=0,
        ).pack(side="left", padx=(0, 12), ipady=6)
        date_filter = ctk.CTkOptionMenu(
            date_row,
            values=[choice.label for choice in DATE_FILTER_CHOICES],
            variable=self.date_filter_label_var,
            fg_color="#F4F8F7",
            button_color=ACCENT,
            button_hover_color="#155F6E",
            text_color=INK,
        )
        date_filter.pack(side="left", fill="x", expand=True)

    def _build_archive_section(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Archive folder", "Copied files are staged here with the same folder structure.")
        inner = ctk.CTkFrame(section, fg_color="transparent")
        inner.pack(fill="x", padx=20, pady=(0, 14))
        ctk.CTkEntry(inner, textvariable=self.archive_root_var).pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkButton(
            inner,
            text="Browse Dropbox",
            width=150,
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=lambda: self.open_folder_picker("archive"),
        ).pack(side="left")

    def _build_source_section(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Source folders", "Choose all Dropbox folders to scan.")
        self.sources_container = ctk.CTkFrame(section, fg_color="transparent")
        self.sources_container.pack(fill="x", padx=20, pady=(0, 8))
        self._render_source_roots()
        ctk.CTkButton(
            section,
            text="Add source folder from Dropbox",
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=lambda: self.open_folder_picker("source"),
        ).pack(anchor="w", padx=20, pady=(0, 16))

    def _build_team_section(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Team coverage", "Team mode scans coverage presets instead of manual source paths.")
        ctk.CTkOptionMenu(
            section,
            values=[choice.label for choice in TEAM_COVERAGE_CHOICES],
            variable=self.team_coverage_label_var,
            fg_color="#F4F8F7",
            button_color=ACCENT,
            button_hover_color="#155F6E",
            text_color=INK,
        ).pack(fill="x", padx=20, pady=(0, 16))

    def _build_output_section(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Local output folder", "Reports, logs, manifests, and resume state are saved here.")
        inner = ctk.CTkFrame(section, fg_color="transparent")
        inner.pack(fill="x", padx=20, pady=(0, 14))
        ctk.CTkEntry(inner, textvariable=self.output_dir_var).pack(side="left", fill="x", expand=True, padx=(0, 10))
        ctk.CTkButton(
            inner,
            text="Choose",
            width=120,
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=self.choose_output_dir,
        ).pack(side="left")

    def _build_advanced_settings(self, parent: ctk.CTkFrame, row: int) -> None:
        section = self._settings_section(parent, row, "Advanced", "Safe defaults are already selected.")
        grid = ctk.CTkFrame(section, fg_color="transparent")
        grid.pack(fill="x", padx=20, pady=(0, 16))
        advanced = [
            ("Batch size", self.batch_size_var),
            ("Retry count", self.retry_count_var),
            ("Initial backoff", self.initial_backoff_var),
            ("Backoff multiplier", self.backoff_multiplier_var),
            ("Max backoff", self.max_backoff_var),
            ("Worker count", self.worker_count_var),
        ]
        for index, (label, var) in enumerate(advanced):
            ctk.CTkLabel(grid, text=label, text_color=MUTED).grid(row=index // 2 * 2, column=index % 2, sticky="w", padx=(0, 16))
            ctk.CTkEntry(grid, textvariable=var, width=150).grid(
                row=index // 2 * 2 + 1, column=index % 2, sticky="w", padx=(0, 16), pady=(2, 10)
            )
        ctk.CTkCheckBox(
            section,
            text="Include folders in inventory export",
            variable=self.include_folders_var,
            fg_color=ACCENT,
            text_color=INK,
        ).pack(anchor="w", padx=20, pady=(0, 8))
        ctk.CTkCheckBox(
            section,
            text="Exclude archive folder from scanning",
            variable=self.exclude_archive_var,
            fg_color=ACCENT,
            text_color=INK,
        ).pack(anchor="w", padx=20, pady=(0, 16))

    def _settings_section(self, parent: ctk.CTkFrame, row: int, title: str, body: str) -> ctk.CTkFrame:
        section = ctk.CTkFrame(parent, fg_color="#F7FAF8", corner_radius=18)
        section.grid(row=row, column=0, sticky="ew", padx=24, pady=10)
        section.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(section, text=title, text_color=INK, font=ctk.CTkFont(size=18, weight="bold")).pack(
            anchor="w", padx=20, pady=(16, 3)
        )
        ctk.CTkLabel(section, text=body, text_color=MUTED, font=ctk.CTkFont(size=13), wraplength=620).pack(
            anchor="w", padx=20, pady=(0, 12)
        )
        return section

    def _run_choice(self, parent: ctk.CTkFrame, label: str, description: str, row: int) -> None:
        button = ctk.CTkButton(
            parent,
            text=f"{label}\n{description}",
            height=76,
            anchor="w",
            font=ctk.CTkFont(size=14, weight="bold"),
            command=lambda value=label: self._select_run_mode(value),
        )
        button.grid(row=row, column=0, sticky="ew", padx=26, pady=6)
        self.run_choice_buttons[label] = button

    def _select_run_mode(self, label: str) -> None:
        self.run_mode_label_var.set(label)
        self._refresh_run_choice_buttons()

    def _refresh_run_choice_buttons(self) -> None:
        selected = self.run_mode_label_var.get()
        for label, button in self.run_choice_buttons.items():
            is_selected = label == selected
            button.configure(
                fg_color=ACCENT if is_selected else "#FFFFFF",
                text_color="#FFFFFF" if is_selected else INK,
                hover_color="#155F6E" if is_selected else "#E8F4F2",
                border_width=0 if is_selected else 1,
                border_color="#BCD8D5",
            )

    def show_run_screen(self) -> None:
        self._clear_content()
        self.step_label.configure(text="Step 4 of 5")
        frame = self._screen_frame()
        frame.grid_columnconfigure(0, weight=2)
        frame.grid_columnconfigure(1, weight=1)

        left = ctk.CTkFrame(frame, fg_color="#FFFFFF", corner_radius=28)
        left.grid(row=0, column=0, sticky="nsew", padx=(4, 14), pady=4)
        left.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(left, text="Run in progress", text_color=INK, font=ctk.CTkFont(size=32, weight="bold")).pack(
            anchor="w", padx=32, pady=(30, 4)
        )
        ctk.CTkLabel(left, textvariable=self.dry_run_banner_var, text_color=SUCCESS, font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=32, pady=(0, 18)
        )
        self.run_progress = ctk.CTkProgressBar(left, mode="indeterminate", progress_color=ACCENT)
        self.run_progress.pack(fill="x", padx=32, pady=(0, 22))
        self.run_progress.start()
        ctk.CTkLabel(left, textvariable=self.phase_var, text_color=INK, font=ctk.CTkFont(size=22, weight="bold")).pack(
            anchor="w", padx=32
        )
        ctk.CTkLabel(left, textvariable=self.phase_detail_var, text_color=MUTED, font=ctk.CTkFont(size=14), wraplength=720).pack(
            anchor="w", padx=32, pady=(4, 24)
        )

        self.counter_grid = ctk.CTkFrame(left, fg_color="#F7FAF8", corner_radius=20)
        self.counter_grid.pack(fill="x", padx=32, pady=(0, 22))
        self.counter_labels: dict[str, ctk.CTkLabel] = {}
        for index, label in enumerate(["Scanned", "Matched", "Copied", "Skipped", "Failed", "Namespaces", "Members"]):
            tile = ctk.CTkFrame(self.counter_grid, fg_color="#FFFFFF", corner_radius=16)
            tile.grid(row=index // 4, column=index % 4, sticky="ew", padx=8, pady=8)
            self.counter_grid.grid_columnconfigure(index % 4, weight=1)
            value_label = ctk.CTkLabel(tile, text="0", text_color=INK, font=ctk.CTkFont(size=24, weight="bold"))
            value_label.pack(anchor="w", padx=16, pady=(12, 0))
            ctk.CTkLabel(tile, text=label, text_color=MUTED, font=ctk.CTkFont(size=12)).pack(anchor="w", padx=16, pady=(0, 12))
            self.counter_labels[label] = value_label

        self.view_results_button = ctk.CTkButton(
            left,
            text="View results",
            height=46,
            fg_color=SUCCESS,
            hover_color="#246A4D",
            command=self.show_results_screen,
        )
        self.cancel_button = ctk.CTkButton(
            left,
            text="Stop safely",
            height=42,
            fg_color="#FFFFFF",
            text_color=DANGER,
            border_width=1,
            border_color="#E3C5C5",
            hover_color="#F8ECEC",
            command=self.stop_run,
        )
        self.cancel_button.pack(anchor="w", padx=32, pady=(0, 28))

        right = ctk.CTkFrame(frame, fg_color=SOFT, corner_radius=28)
        right.grid(row=0, column=1, sticky="nsew", padx=(14, 4), pady=4)
        ctk.CTkLabel(right, text="Details for support", text_color=INK, font=ctk.CTkFont(size=22, weight="bold")).pack(
            anchor="w", padx=24, pady=(28, 8)
        )
        ctk.CTkLabel(
            right,
            text="You can ignore this during a normal run. It is useful if Dropbox reports a permission issue.",
            text_color=MUTED,
            wraplength=330,
            justify="left",
        ).pack(anchor="w", padx=24, pady=(0, 12))
        self.log_text = ctk.CTkTextbox(right, height=560, fg_color="#FFFFFF", text_color="#1D3036", wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=24, pady=(0, 24))
        self.log_text.configure(state="disabled")

    def show_results_screen(self) -> None:
        self._clear_content()
        self.step_label.configure(text="Step 5 of 5")
        frame = self._screen_frame()
        frame.grid_columnconfigure(0, weight=3)
        frame.grid_columnconfigure(1, weight=2)
        run_dir = self.latest_run_dir
        if run_dir is None:
            self._empty_results(frame)
            return
        result = load_results_view_model(run_dir)

        left = ctk.CTkScrollableFrame(frame, fg_color="#FFFFFF", corner_radius=28)
        left.grid(row=0, column=0, sticky="nsew", padx=(4, 14), pady=4)
        left.grid_columnconfigure(0, weight=1)
        right = ctk.CTkFrame(frame, fg_color=SOFT, corner_radius=28)
        right.grid(row=0, column=1, sticky="nsew", padx=(14, 4), pady=4)
        right.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(left, text="Run complete", text_color=INK, font=ctk.CTkFont(size=34, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=30, pady=(30, 4)
        )
        ctk.CTkLabel(left, text=result.success_message, text_color=MUTED, font=ctk.CTkFont(size=16), wraplength=720).grid(
            row=1, column=0, sticky="w", padx=30, pady=(0, 20)
        )
        self._render_metrics(left, result, row=2)
        self._render_folder_table(left, result, row=3)
        self._render_issue_table(left, result, row=4)

        ctk.CTkLabel(right, text="Archive status", text_color=INK, font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=26, pady=(30, 10)
        )
        chart = Canvas(right, width=340, height=220, bg=SOFT, highlightthickness=0)
        chart.grid(row=1, column=0, sticky="ew", padx=26, pady=(0, 18))
        self._draw_status_chart(chart, result)
        verify_chart = Canvas(right, width=340, height=130, bg=SOFT, highlightthickness=0)
        verify_chart.grid(row=2, column=0, sticky="ew", padx=26, pady=(0, 20))
        self._draw_verification_chart(verify_chart, result)

        ctk.CTkButton(right, text="Open output folder", height=44, fg_color=ACCENT, command=self.open_output_folder).grid(
            row=3, column=0, sticky="ew", padx=26, pady=(0, 10)
        )
        ctk.CTkButton(
            right,
            text="Open summary",
            height=40,
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=lambda: self._open_named_output("summary.md"),
        ).grid(row=4, column=0, sticky="ew", padx=26, pady=(0, 10))
        ctk.CTkButton(
            right,
            text="Open manifest",
            height=40,
            fg_color="#FFFFFF",
            text_color=ACCENT,
            border_width=1,
            border_color="#BCD8D5",
            hover_color="#E8F4F2",
            command=self._open_manifest,
        ).grid(row=5, column=0, sticky="ew", padx=26, pady=(0, 10))
        ctk.CTkButton(
            right,
            text="Resume or retry last run",
            height=40,
            fg_color="#FFFFFF",
            text_color=WARNING,
            border_width=1,
            border_color="#E5D2B9",
            hover_color="#F6EFE6",
            command=self.resume_last_run,
        ).grid(row=6, column=0, sticky="ew", padx=26, pady=(8, 10))
        ctk.CTkButton(
            right,
            text="Start another run",
            height=40,
            fg_color="transparent",
            text_color=ACCENT,
            hover_color="#D9EFEB",
            command=self.show_settings_screen,
        ).grid(row=7, column=0, sticky="ew", padx=26, pady=(0, 24))

    def _empty_results(self, parent: ctk.CTkFrame) -> None:
        panel = ctk.CTkFrame(parent, fg_color="#FFFFFF", corner_radius=28)
        panel.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        ctk.CTkLabel(panel, text="No results yet", text_color=INK, font=ctk.CTkFont(size=32, weight="bold")).pack(
            anchor="w", padx=34, pady=(34, 8)
        )
        ctk.CTkLabel(panel, text="Run a preview or copy job first.", text_color=MUTED).pack(anchor="w", padx=34)

    def _render_metrics(self, parent: ctk.CTkFrame, result: ResultsViewModel, row: int) -> None:
        panel = ctk.CTkFrame(parent, fg_color="#F7FAF8", corner_radius=20)
        panel.grid(row=row, column=0, sticky="ew", padx=28, pady=(0, 18))
        for index, metric in enumerate(result.metrics):
            tile = ctk.CTkFrame(panel, fg_color="#FFFFFF", corner_radius=16)
            tile.grid(row=index // 4, column=index % 4, sticky="ew", padx=8, pady=8)
            panel.grid_columnconfigure(index % 4, weight=1)
            color = {"success": SUCCESS, "warning": WARNING, "danger": DANGER, "accent": ACCENT}.get(metric.tone, INK)
            ctk.CTkLabel(tile, text=str(metric.value), text_color=color, font=ctk.CTkFont(size=25, weight="bold")).pack(
                anchor="w", padx=16, pady=(14, 0)
            )
            ctk.CTkLabel(tile, text=metric.label, text_color=MUTED, font=ctk.CTkFont(size=12)).pack(
                anchor="w", padx=16, pady=(0, 14)
            )

    def _render_folder_table(self, parent: ctk.CTkFrame, result: ResultsViewModel, row: int) -> None:
        panel = ctk.CTkFrame(parent, fg_color="#F7FAF8", corner_radius=20)
        panel.grid(row=row, column=0, sticky="ew", padx=28, pady=(0, 18))
        ctk.CTkLabel(panel, text="Top folders", text_color=INK, font=ctk.CTkFont(size=18, weight="bold")).pack(
            anchor="w", padx=18, pady=(16, 8)
        )
        if not result.top_folders:
            ctk.CTkLabel(panel, text="No folder-level copy results yet.", text_color=MUTED).pack(anchor="w", padx=18, pady=(0, 16))
            return
        for folder in result.top_folders:
            line = f"{folder.folder}  ·  matched {folder.matched}  copied {folder.copied}  skipped {folder.skipped}  failed {folder.failed}"
            ctk.CTkLabel(panel, text=line, text_color=INK, anchor="w", wraplength=760, justify="left").pack(
                fill="x", padx=18, pady=4
            )

    def _render_issue_table(self, parent: ctk.CTkFrame, result: ResultsViewModel, row: int) -> None:
        panel = ctk.CTkFrame(parent, fg_color="#F7FAF8", corner_radius=20)
        panel.grid(row=row, column=0, sticky="ew", padx=28, pady=(0, 28))
        ctk.CTkLabel(panel, text="Needs attention", text_color=INK, font=ctk.CTkFont(size=18, weight="bold")).pack(
            anchor="w", padx=18, pady=(16, 8)
        )
        issues = result.blocked + result.failures + result.conflicts
        if not issues:
            ctk.CTkLabel(panel, text="No conflicts, failures, or blocked items recorded.", text_color=SUCCESS).pack(
                anchor="w", padx=18, pady=(0, 16)
            )
            return
        for issue in issues[:12]:
            ctk.CTkLabel(panel, text=f"• {issue}", text_color=DANGER, anchor="w", wraplength=760, justify="left").pack(
                fill="x", padx=18, pady=3
            )

    def start_oauth(self) -> None:
        app_key = self._effective_app_key()
        if not app_key:
            messagebox.showerror("Missing app key", "Enter your Dropbox app key first.")
            return
        account_mode = self.account_mode_var.get()
        authorize_url = self.auth_manager.start_pkce_flow(
            app_key,
            default_scopes_for_mode(account_mode),
            account_mode=account_mode,
            label="default",
        )
        webbrowser.open(authorize_url)
        self._set_connection_status("Dropbox opened in your browser. Paste the authorization code here when you approve access.", MUTED)

    def finish_oauth(self) -> None:
        try:
            credentials = self.auth_manager.finish_pkce_flow(self.auth_code_var.get().strip(), label="default")
            if self.admin_member_id_var.get().strip():
                credentials.admin_member_id = self.admin_member_id_var.get().strip()
            self.auth_manager.save_credentials("default", credentials)
            self.account_mode_var.set(credentials.account_mode)
            self.token_var.set("")
            self._test_connection_from_config(self.auth_manager.credentials_to_auth_config(credentials))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Connection failed", self._format_exception_for_user(exc))

    def save_manual_token(self) -> None:
        token = self.token_var.get().strip()
        if not token:
            messagebox.showerror("Missing token", "Enter a refresh token or access token first.")
            return
        app_key = self._effective_app_key()
        if not app_key:
            messagebox.showerror("Missing app key", "A Dropbox app key is required for refresh-token auth.")
            return
        credentials = self.auth_manager.save_manual_token(
            method="refresh_token",
            account_mode=self.account_mode_var.get(),
            app_key=app_key,
            refresh_token=token,
            admin_member_id=self.admin_member_id_var.get().strip() or None,
        )
        self._test_connection_from_config(self.auth_manager.credentials_to_auth_config(credentials))

    def test_saved_connection(self) -> None:
        try:
            self._test_connection_from_config(self._build_auth_config())
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Connection failed", self._format_exception_for_user(exc))

    def clear_saved_credentials(self) -> None:
        self.auth_manager.clear_credentials("default")
        self.auth_code_var.set("")
        self.token_var.set("")
        self.connected_account_summary = "Saved connection removed. Connect Dropbox again to continue."
        self._set_connection_status(self.connected_account_summary, MUTED)

    def _test_connection_from_config(self, auth_config: AuthConfig) -> None:
        account = self.auth_manager.test_connection(auth_config, self._temporary_logger())
        if account.account_mode == "team_admin":
            self.connected_account_summary = (
                f"Connected as {account.display_name}\n"
                f"Team: {account.team_name or 'Unknown'} · {account.active_member_count} active member(s) · "
                f"{account.namespace_count} namespace(s)"
            )
        else:
            self.connected_account_summary = f"Connected as {account.display_name} ({account.email or 'no email returned'})"
        self._set_connection_status(self.connected_account_summary, SUCCESS)

    def _set_connection_status(self, text: str, color: str) -> None:
        self.connected_account_summary = text
        if hasattr(self, "connection_status_label"):
            self.connection_status_label.configure(text=text, text_color=color)

    def choose_output_dir(self) -> None:
        chosen = filedialog.askdirectory(initialdir=self.output_dir_var.get() or ".")
        if chosen:
            self.output_dir_var.set(chosen)

    def open_folder_picker(self, purpose: str) -> None:
        try:
            auth_config = self._build_auth_config()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Connect Dropbox first", str(exc))
            return
        dialog = DropboxFolderPickerDialog(
            self,
            auth_config=auth_config,
            job_config=self._build_job_config(run_label_to_value(self.run_mode_label_var.get())),
            purpose=purpose,
        )
        self.root.wait_window(dialog.window)
        if dialog.selected_path is None:
            return
        if purpose == "archive":
            self.archive_root_var.set(dialog.selected_path)
        elif dialog.selected_path not in self.source_roots:
            self.source_roots.append(dialog.selected_path)
            self._render_source_roots()

    def _render_source_roots(self) -> None:
        if not hasattr(self, "sources_container"):
            return
        for widget in self.sources_container.winfo_children():
            widget.destroy()
        for root_path in self.source_roots:
            row = ctk.CTkFrame(self.sources_container, fg_color="#FFFFFF", corner_radius=14)
            row.pack(fill="x", pady=4)
            ctk.CTkLabel(row, text=root_path, text_color=INK, anchor="w").pack(side="left", fill="x", expand=True, padx=14, pady=10)
            ctk.CTkButton(
                row,
                text="Remove",
                width=90,
                fg_color="transparent",
                text_color=DANGER,
                hover_color="#F8ECEC",
                command=lambda value=root_path: self._remove_source_root(value),
            ).pack(side="right", padx=8)

    def _remove_source_root(self, root_path: str) -> None:
        self.source_roots = [item for item in self.source_roots if item != root_path]
        if not self.source_roots:
            self.source_roots = ["/"]
        self._render_source_roots()

    def start_run_from_settings(self) -> None:
        self.show_run_screen()
        self.start_run(resume=False)

    def start_run(self, *, resume: bool) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("Run in progress", "A run is already in progress.")
            return
        mode = run_label_to_value(self.run_mode_label_var.get())
        if not resume and mode == "copy_run":
            confirmed = messagebox.askyesno(
                "Confirm copy run",
                "This will create Dropbox archive folders and server-side copied files.\n\nOriginals will not be deleted or moved.\n\nContinue?",
            )
            if not confirmed:
                self.show_settings_screen()
                return
        try:
            auth_config = self._build_auth_config()
            job_config = self._build_job_config(mode)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Check settings", str(exc))
            self.show_settings_screen()
            return

        self.technical_log_lines.clear()
        self.phase_var.set("Starting")
        self.phase_detail_var.set("Preparing a safe local run.")
        self.dry_run_banner_var.set("Preview mode makes no Dropbox changes." if mode == "dry_run" else "Originals remain in place.")
        self.cancellation_token = CancellationToken()

        def worker() -> None:
            try:
                if resume:
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
            self.phase_var.set("Stopping safely")
            self.phase_detail_var.set("The current item will finish, then the run can be resumed later.")

    def resume_last_run(self) -> None:
        self.show_run_screen()
        self.start_run(resume=True)

    def open_output_folder(self) -> None:
        if self.latest_run_dir is None:
            messagebox.showinfo("No output folder yet", "Run the app first.")
            return
        self._open_path(self.latest_run_dir)

    def _open_named_output(self, name: str) -> None:
        if self.latest_run_dir is None:
            return
        path = self.latest_run_dir / name
        if path.exists():
            self._open_path(path)
        else:
            messagebox.showinfo("File not found", f"{name} was not generated for this run.")

    def _open_manifest(self) -> None:
        if self.latest_run_dir is None:
            return
        for name in ("manifest_copy_run.csv", "manifest_dry_run.csv"):
            path = self.latest_run_dir / name
            if path.exists():
                self._open_path(path)
                return
        messagebox.showinfo("File not found", "No manifest was generated for this run.")

    def _build_auth_config(self) -> AuthConfig:
        saved = self.auth_manager.load_credentials("default")
        if saved is None:
            raise ValueError("Connect Dropbox first.")
        auth_config = self.auth_manager.credentials_to_auth_config(saved)
        auth_config.account_mode = self.account_mode_var.get()  # type: ignore[assignment]
        admin_member_id = self.admin_member_id_var.get().strip()
        if admin_member_id:
            auth_config.admin_member_id = admin_member_id
        return auth_config

    def _build_job_config(self, mode: str) -> JobConfig:
        source_roots = self.source_roots or ["/"]
        return JobConfig(
            source_roots=source_roots,
            cutoff_date=self.cutoff_date_var.get().strip(),
            date_filter_field=date_filter_label_to_value(self.date_filter_label_var.get()),
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
            team_coverage_preset=team_coverage_label_to_value(self.team_coverage_label_var.get()),
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
                self.current_run_result = payload
                self.latest_run_dir = Path(payload.run_dir)  # type: ignore[attr-defined]
                self.last_output_var.set(str(self.latest_run_dir))
                if hasattr(self, "run_progress"):
                    self.run_progress.stop()
                self.phase_var.set("Completed")
                self.phase_detail_var.set("The reports are ready. Review the visual summary next.")
                if hasattr(self, "cancel_button"):
                    self.cancel_button.pack_forget()
                if hasattr(self, "view_results_button"):
                    self.view_results_button.pack(anchor="w", padx=32, pady=(0, 28))
            elif kind == "error":
                if hasattr(self, "run_progress"):
                    self.run_progress.stop()
                self.phase_var.set("Needs attention")
                self.phase_detail_var.set("The run stopped before completion. Originals were not deleted or moved.")
                if isinstance(payload, dict):
                    self._append_log(payload.get("traceback", ""))
                    messagebox.showerror("Run stopped", payload.get("message", "Unknown error"))
                else:
                    messagebox.showerror("Run stopped", str(payload))
                if hasattr(self, "view_results_button") and self.latest_run_dir is not None:
                    self.view_results_button.pack(anchor="w", padx=32, pady=(0, 28))

        while True:
            try:
                line = self.log_queue.get_nowait()
            except Empty:
                break
            self._append_log(line)
        self.root.after(200, self._poll_queues)

    def _apply_progress(self, snapshot: ProgressSnapshot) -> None:
        phase = _friendly_phase(snapshot.phase)
        self.phase_var.set(phase)
        self.phase_detail_var.set(snapshot.message)
        counters = snapshot.counters
        mapping = {
            "Scanned": "items_scanned",
            "Matched": "files_matched",
            "Copied": "files_copied",
            "Skipped": "files_skipped",
            "Failed": "files_failed",
            "Namespaces": "namespaces_scanned",
            "Members": "members_covered",
        }
        for label, key in mapping.items():
            if hasattr(self, "counter_labels") and label in self.counter_labels:
                self.counter_labels[label].configure(text=str(counters.get(key, 0)))
        if snapshot.outputs.get("run_dir"):
            self.latest_run_dir = Path(snapshot.outputs["run_dir"])
            self.last_output_var.set(snapshot.outputs["run_dir"])

    def _append_log(self, line: str) -> None:
        self.technical_log_lines.append(line)
        if not hasattr(self, "log_text"):
            return
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _screen_frame(self) -> ctk.CTkFrame:
        frame = ctk.CTkFrame(self.content, fg_color="transparent", corner_radius=0)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.grid_rowconfigure(0, weight=1)
        return frame

    def _choice_panel(
        self,
        parent: ctk.CTkFrame,
        *,
        row: int,
        column: int,
        title: str,
        body: str,
        button_text: str,
        command,
    ) -> None:
        panel = ctk.CTkFrame(parent, fg_color="#FFFFFF", corner_radius=26)
        panel.grid(row=row, column=column, sticky="nsew", padx=8, pady=8)
        ctk.CTkLabel(panel, text=title, text_color=INK, font=ctk.CTkFont(size=24, weight="bold")).pack(
            anchor="w", padx=28, pady=(28, 8)
        )
        ctk.CTkLabel(panel, text=body, text_color=MUTED, font=ctk.CTkFont(size=15), wraplength=420, justify="left").pack(
            anchor="w", padx=28, pady=(0, 24)
        )
        ctk.CTkButton(panel, text=button_text, height=44, fg_color=ACCENT, hover_color="#155F6E", command=command).pack(
            fill="x", padx=28, pady=(0, 28)
        )

    def _draw_cloud_hero(self, parent) -> Canvas:
        canvas = Canvas(parent, width=300, height=190, bg="#DDF1ED", highlightthickness=0)
        canvas.create_oval(60, 70, 155, 150, fill="#FFFFFF", outline="")
        canvas.create_oval(120, 45, 230, 150, fill="#FFFFFF", outline="")
        canvas.create_oval(175, 82, 255, 150, fill="#FFFFFF", outline="")
        canvas.create_rectangle(95, 105, 230, 150, fill="#FFFFFF", outline="")
        canvas.create_line(150, 88, 150, 132, fill=ACCENT, width=5)
        canvas.create_line(132, 110, 150, 88, fill=ACCENT, width=5)
        canvas.create_line(168, 110, 150, 88, fill=ACCENT, width=5)
        canvas.create_text(150, 172, text="Copy-first archive", fill=INK, font=("Arial", 14, "bold"))
        return canvas

    def _draw_connection_graphic(self, parent) -> Canvas:
        canvas = Canvas(parent, width=320, height=180, bg=SOFT, highlightthickness=0)
        canvas.create_oval(24, 40, 114, 130, fill="#FFFFFF", outline="#BCD8D5", width=2)
        canvas.create_text(69, 85, text="You", fill=INK, font=("Arial", 15, "bold"))
        canvas.create_oval(206, 40, 296, 130, fill="#FFFFFF", outline="#BCD8D5", width=2)
        canvas.create_text(251, 85, text="Dropbox", fill=INK, font=("Arial", 13, "bold"))
        canvas.create_line(116, 86, 202, 86, fill=ACCENT, width=4, arrow="last")
        canvas.create_text(160, 118, text="OAuth", fill=ACCENT, font=("Arial", 12, "bold"))
        return canvas

    def _draw_status_chart(self, canvas: Canvas, result: ResultsViewModel) -> None:
        canvas.delete("all")
        total = sum(slice_.value for slice_ in result.status_slices)
        if total <= 0:
            canvas.create_text(170, 104, text="No copy results yet", fill=MUTED, font=("Arial", 14, "bold"))
            return
        start = 90
        for slice_ in result.status_slices:
            extent = 360 * (slice_.value / total)
            canvas.create_arc(40, 20, 190, 170, start=start, extent=extent, fill=slice_.color, outline=SOFT)
            start += extent
        canvas.create_oval(82, 62, 148, 128, fill=SOFT, outline=SOFT)
        y = 38
        for slice_ in result.status_slices:
            canvas.create_rectangle(220, y, 234, y + 14, fill=slice_.color, outline="")
            canvas.create_text(242, y + 7, text=f"{slice_.label}: {slice_.value}", fill=INK, anchor="w", font=("Arial", 11, "bold"))
            y += 28

    def _draw_verification_chart(self, canvas: Canvas, result: ResultsViewModel) -> None:
        canvas.delete("all")
        verification = result.verification or {}
        source = int(verification.get("source_matched_file_count", 0) or 0)
        staged = int(verification.get("archive_staged_file_count", 0) or 0)
        canvas.create_text(6, 12, text="Verification", fill=INK, anchor="w", font=("Arial", 13, "bold"))
        max_value = max(source, staged, 1)
        canvas.create_rectangle(6, 38, 6 + 300 * source / max_value, 62, fill="#BFDCD8", outline="")
        canvas.create_text(12, 50, text=f"Source matched: {source}", fill=INK, anchor="w", font=("Arial", 10, "bold"))
        canvas.create_rectangle(6, 78, 6 + 300 * staged / max_value, 102, fill=SUCCESS, outline="")
        canvas.create_text(12, 90, text=f"Archive staged: {staged}", fill=INK, anchor="w", font=("Arial", 10, "bold"))

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
        if saved is None:
            return
        self.account_mode_var.set(saved.account_mode)
        if saved.app_key and not self.app_key_var.get():
            self.app_key_var.set(saved.app_key)
        if saved.admin_member_id:
            self.admin_member_id_var.set(saved.admin_member_id)
        self.connected_account_summary = "Saved Dropbox connection found. Continue or reconnect if needed."

    def _load_latest_run_hint(self) -> None:
        latest_pointer = Path(self.output_dir_var.get()) / "latest_run.json"
        if latest_pointer.exists():
            payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
            self.latest_run_dir = Path(payload["run_dir"])
            self.last_output_var.set(str(self.latest_run_dir))

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
                "Dropbox permissions need one more step.\n\n"
                f"Missing scope: {required_scope}\n\n"
                "Open the Dropbox App Console, enable the required scopes, save, then reconnect this app.\n\n"
                f"Required scopes: {scope_block}"
            )
        return str(exc)

    def _packaged_app_key(self) -> str | None:
        env_value = os.environ.get("DROPBOX_CLEANER_APP_KEY") or os.environ.get("DROPBOX_APP_KEY")
        if env_value:
            return env_value
        candidates = []
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
        return self._packaged_app_key() or self.app_key_var.get().strip() or None


class DropboxFolderPickerDialog:
    def __init__(self, app: DropboxCleanerApp, *, auth_config: AuthConfig, job_config: JobConfig, purpose: str) -> None:
        self.app = app
        self.purpose = purpose
        self.selected_path: str | None = None
        self.adapter = DropboxAdapter(auth_config, app._temporary_logger())
        self.service = DropboxFolderBrowserService(
            self.adapter,
            account_mode=auth_config.account_mode,
            job_config=job_config,
        )
        self.current_location = self.service.root_location()
        self.window = ctk.CTkToplevel(app.root)
        self.window.title("Choose Dropbox folder")
        self.window.geometry("760x620")
        self.window.transient(app.root)
        self.window.protocol("WM_DELETE_WINDOW", self._close)
        self.window.grid_columnconfigure(0, weight=1)
        self.window.grid_rowconfigure(2, weight=1)

        title = "Choose archive folder" if purpose == "archive" else "Choose source folder"
        ctk.CTkLabel(self.window, text=title, text_color=INK, font=ctk.CTkFont(size=24, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=24, pady=(24, 4)
        )
        self.path_label = ctk.CTkLabel(self.window, text="/", text_color=MUTED, font=ctk.CTkFont(size=14))
        self.path_label.grid(row=1, column=0, sticky="w", padx=24, pady=(0, 12))

        self.list_frame = ctk.CTkScrollableFrame(self.window, fg_color="#F7FAF8", corner_radius=18)
        self.list_frame.grid(row=2, column=0, sticky="nsew", padx=24, pady=(0, 16))
        actions = ctk.CTkFrame(self.window, fg_color="transparent")
        actions.grid(row=3, column=0, sticky="ew", padx=24, pady=(0, 24))
        actions.grid_columnconfigure(2, weight=1)
        ctk.CTkButton(actions, text="Up", width=90, fg_color="#FFFFFF", text_color=ACCENT, command=self._go_up).grid(
            row=0, column=0, padx=(0, 8)
        )
        ctk.CTkButton(actions, text="Refresh", width=100, fg_color="#FFFFFF", text_color=ACCENT, command=self._load).grid(
            row=0, column=1, padx=(0, 8)
        )
        ctk.CTkButton(actions, text="Choose this folder", width=170, fg_color=ACCENT, command=self._choose_current).grid(
            row=0, column=3, padx=(8, 0)
        )
        self._load()

    def _load(self) -> None:
        for widget in self.list_frame.winfo_children():
            widget.destroy()
        self.path_label.configure(text=self.current_location.display_path)
        try:
            folders = self.service.list_folders(self.current_location)
        except Exception as exc:  # noqa: BLE001
            ctk.CTkLabel(self.list_frame, text=f"Could not load folders: {exc}", text_color=DANGER, wraplength=660).pack(
                anchor="w", padx=16, pady=16
            )
            return
        if not folders:
            ctk.CTkLabel(self.list_frame, text="No folders here.", text_color=MUTED).pack(anchor="w", padx=16, pady=16)
            return
        for folder in folders:
            row = ctk.CTkButton(
                self.list_frame,
                text=f"📁  {folder.name}\n{folder.display_path}  {folder.subtitle}",
                height=58,
                anchor="w",
                fg_color="#FFFFFF",
                text_color=INK,
                hover_color="#E8F4F2",
                command=lambda value=folder.location: self._open_location(value),
            )
            row.pack(fill="x", padx=10, pady=5)

    def _open_location(self, location: BrowserLocation) -> None:
        self.current_location = location
        self._load()

    def _go_up(self) -> None:
        self.current_location = self.service.parent_location(self.current_location)
        self._load()

    def _choose_current(self) -> None:
        if self.purpose == "archive" and self.current_location.display_path == "/":
            messagebox.showinfo("Choose a folder", "Choose or create a dedicated archive folder, not the Dropbox root.")
            return
        self.selected_path = self.current_location.display_path
        self._close()

    def _close(self) -> None:
        try:
            self.adapter.close()
        finally:
            self.window.destroy()


def _friendly_phase(phase: str) -> str:
    return {
        "connecting": "Connecting to Dropbox",
        "team_discovery": "Reading team folders",
        "inventory": "Scanning files",
        "filter": "Finding older files",
        "copy": "Staging archive copies",
        "verify": "Checking the archive",
        "outputs": "Writing reports",
        "completed": "Completed",
    }.get(phase, phase.replace("_", " ").title())


def run_app() -> int:
    ctk.set_appearance_mode("light")
    ctk.set_default_color_theme("blue")
    root = ctk.CTk()
    app = DropboxCleanerApp(root)
    root.mainloop()
    return 0
