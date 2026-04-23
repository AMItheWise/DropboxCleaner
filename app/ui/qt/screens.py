from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QDate, Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDoubleSpinBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from app.models.events import ProgressSnapshot
from app.ui.options import (
    ACCOUNT_CHOICES,
    DATE_FILTER_CHOICES,
    RUN_MODE_CHOICES,
    TEAM_ARCHIVE_LAYOUT_CHOICES,
    TEAM_COVERAGE_CHOICES,
    date_filter_value_to_label,
    team_archive_layout_value_to_label,
    team_coverage_value_to_label,
)
from app.ui.results import ResultsViewModel
from app.ui.qt import theme
from app.ui.qt.widgets import (
    ChoiceCard,
    DonutChartWidget,
    FolderTable,
    IssueTable,
    MetricTileWidget,
    PhaseTimeline,
    SafetyPanel,
    TitleBlock,
    VerificationBarWidget,
    card_frame,
    clear_layout,
    metrics_grid,
)
from app.utils.paths import normalize_dropbox_path


class AccountScreen(QWidget):
    account_selected = Signal(str)
    resume_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        layout = QGridLayout(self)
        layout.setContentsMargins(34, 28, 34, 34)
        layout.setSpacing(20)
        layout.setColumnStretch(0, 2)
        layout.setColumnStretch(1, 1)

        hero = card_frame("softCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(34, 34, 34, 34)
        hero_layout.setSpacing(18)
        hero_layout.addWidget(
            TitleBlock(
                "Start",
                "Archive Dropbox files without deleting anything.",
                "Choose the account type, connect securely, preview the archive, then copy only when you are ready.",
            )
        )
        hero_layout.addStretch(1)
        layout.addWidget(hero, 0, 0, 2, 1)

        choices = QWidget()
        choice_layout = QVBoxLayout(choices)
        choice_layout.setContentsMargins(0, 0, 0, 0)
        choice_layout.setSpacing(14)
        for choice in ACCOUNT_CHOICES:
            button = QPushButton(f"{choice.label}\n{choice.description}")
            button.setMinimumHeight(104)
            theme.set_role(button, "card")
            button.clicked.connect(lambda _checked=False, value=choice.value: self.account_selected.emit(value))
            choice_layout.addWidget(button)
        self.resume_button = QPushButton("Resume last run")
        theme.set_role(self.resume_button, "ghost")
        self.resume_button.clicked.connect(self.resume_requested.emit)
        self.resume_button.hide()
        choice_layout.addWidget(self.resume_button)
        choice_layout.addStretch(1)
        layout.addWidget(choices, 0, 1, 2, 1)

    def set_resume_available(self, available: bool) -> None:
        self.resume_button.setVisible(available)


class ConnectionScreen(QWidget):
    back_requested = Signal()
    start_oauth_requested = Signal()
    finish_oauth_requested = Signal()
    test_connection_requested = Signal()
    disconnect_requested = Signal()
    continue_requested = Signal()
    save_token_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self._account_mode = "personal"
        self._connected = False
        self._saved_credentials_available = False
        self._packaged_app_key_available = False
        self._spinner_index = 0
        self._spinner_frames = ("|", "/", "-", "\\")
        self._spinner_timer = QTimer(self)
        self._spinner_timer.setInterval(120)
        self._spinner_timer.timeout.connect(self._advance_spinner)
        layout = QGridLayout(self)
        layout.setContentsMargins(34, 28, 34, 34)
        layout.setSpacing(20)
        layout.setColumnStretch(0, 3)
        layout.setColumnStretch(1, 2)

        main = card_frame()
        main_layout = QVBoxLayout(main)
        main_layout.setContentsMargins(28, 28, 28, 28)
        main_layout.setSpacing(12)
        self.title_block = TitleBlock(
            "Step 2 of 5",
            "Connect Dropbox",
            "We open Dropbox in your browser. Approve access, then paste the authorization code here.",
        )
        main_layout.addWidget(self.title_block)

        back_row = QHBoxLayout()
        self.back_button = QPushButton("Back")
        theme.set_role(self.back_button, "ghost")
        self.back_button.clicked.connect(self.back_requested.emit)
        back_row.addWidget(self.back_button)
        back_row.addStretch(1)
        main_layout.addLayout(back_row)

        self.saved_panel = card_frame("softCard")
        saved_layout = QVBoxLayout(self.saved_panel)
        saved_layout.setContentsMargins(18, 18, 18, 18)
        saved_layout.setSpacing(10)
        saved_title = QLabel("Saved Dropbox connection")
        saved_title.setObjectName("sectionTitle")
        self.saved_summary_label = QLabel(
            "Press Use saved connection to keep using the Dropbox authorization saved on this computer, or connect a different account if needed."
        )
        self.saved_summary_label.setObjectName("body")
        self.saved_summary_label.setWordWrap(True)
        saved_actions = QHBoxLayout()
        self.use_saved_button = QPushButton("Use saved connection")
        self.use_saved_button.setToolTip("Test the locally saved Dropbox authorization and continue if it is still valid.")
        theme.set_role(self.use_saved_button, "primary")
        self.use_saved_button.clicked.connect(self.test_connection_requested.emit)
        self.reconnect_button = QPushButton("Connect a different account")
        theme.set_role(self.reconnect_button, "ghost")
        self.reconnect_button.setToolTip("Keep the saved connection for now, but show the Dropbox authorization form again.")
        self.reconnect_button.clicked.connect(self._show_reconnect_form)
        saved_actions.addWidget(self.use_saved_button)
        saved_actions.addWidget(self.reconnect_button)
        saved_layout.addWidget(saved_title)
        saved_layout.addWidget(self.saved_summary_label)
        saved_layout.addLayout(saved_actions)
        self.saved_panel.hide()
        main_layout.addWidget(self.saved_panel)

        self.app_key_label = QLabel("Dropbox app key")
        main_layout.addWidget(self.app_key_label)
        self.app_key_edit = QLineEdit()
        self.app_key_edit.setPlaceholderText("Paste your Dropbox app key")
        main_layout.addWidget(self.app_key_edit)

        self.connect_button = QPushButton("Connect Dropbox")
        theme.set_role(self.connect_button, "primary")
        self.connect_button.clicked.connect(self.start_oauth_requested.emit)
        main_layout.addWidget(self.connect_button)

        self.auth_code_label = QLabel("Authorization code")
        main_layout.addWidget(self.auth_code_label)
        self.auth_code_edit = QLineEdit()
        self.auth_code_edit.setPlaceholderText("Paste the code Dropbox gives you")
        main_layout.addWidget(self.auth_code_edit)

        self.finish_button = QPushButton("Finish connection")
        theme.set_role(self.finish_button, "success")
        self.finish_button.clicked.connect(self.finish_oauth_requested.emit)
        main_layout.addWidget(self.finish_button)
        self._oauth_widgets = [
            self.app_key_label,
            self.app_key_edit,
            self.connect_button,
            self.auth_code_label,
            self.auth_code_edit,
            self.finish_button,
        ]

        busy_row = QHBoxLayout()
        self.spinner_label = QLabel("|")
        self.spinner_label.setObjectName("spinner")
        self.spinner_label.setFixedWidth(24)
        self.busy_label = QLabel("Testing Dropbox connection...")
        self.busy_label.setObjectName("body")
        self.connection_progress = QProgressBar()
        self.connection_progress.setRange(0, 0)
        self.connection_progress.setTextVisible(False)
        busy_row.addWidget(self.spinner_label)
        busy_row.addWidget(self.busy_label)
        busy_row.addWidget(self.connection_progress, 1)
        main_layout.addLayout(busy_row)
        self._busy_widgets = [self.spinner_label, self.busy_label, self.connection_progress]
        self.set_busy(False)

        self.status_label = QLabel("Not connected yet.")
        self.status_label.setObjectName("body")
        self.status_label.setWordWrap(True)
        main_layout.addWidget(self.status_label)

        self.account_status_card = card_frame("successCard")
        account_status_layout = QVBoxLayout(self.account_status_card)
        account_status_layout.setContentsMargins(18, 18, 18, 18)
        account_status_layout.setSpacing(12)
        self.account_status_title = QLabel("Connected to Dropbox")
        self.account_status_title.setObjectName("sectionTitle")
        self.account_status_body = QLabel("Connection verified. You can continue to settings.")
        self.account_status_body.setObjectName("body")
        self.account_status_body.setWordWrap(True)
        self.account_details_layout = QGridLayout()
        self.account_details_layout.setHorizontalSpacing(18)
        self.account_details_layout.setVerticalSpacing(8)
        account_status_layout.addWidget(self.account_status_title)
        account_status_layout.addWidget(self.account_status_body)
        account_status_layout.addLayout(self.account_details_layout)
        self.account_status_card.hide()
        main_layout.addWidget(self.account_status_card)

        actions = QHBoxLayout()
        self.test_button = QPushButton("Test saved connection")
        self.test_button.clicked.connect(self.test_connection_requested.emit)
        self.disconnect_button = QPushButton("Disconnect")
        theme.set_role(self.disconnect_button, "danger")
        self.disconnect_button.clicked.connect(self.disconnect_requested.emit)
        actions.addWidget(self.test_button)
        actions.addWidget(self.disconnect_button)
        main_layout.addLayout(actions)

        self.advanced = QGroupBox("Advanced connection options")
        self.advanced.setCheckable(True)
        self.advanced.setChecked(False)
        advanced_layout = QVBoxLayout(self.advanced)
        self.token_edit = QLineEdit()
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.token_edit.setPlaceholderText("Refresh token")
        advanced_layout.addWidget(self.token_edit)
        self.admin_member_id_edit = QLineEdit()
        self.admin_member_id_edit.setPlaceholderText("Optional admin member ID override")
        advanced_layout.addWidget(self.admin_member_id_edit)
        self.save_token_button = QPushButton("Save token and test")
        self.save_token_button.clicked.connect(self.save_token_requested.emit)
        advanced_layout.addWidget(self.save_token_button)
        self._advanced_widgets = [self.token_edit, self.admin_member_id_edit, self.save_token_button]
        self.advanced.toggled.connect(self._set_advanced_visible)
        main_layout.addWidget(self.advanced)
        self._set_advanced_visible(False)

        self.continue_button = QPushButton("Continue")
        theme.set_role(self.continue_button, "primary")
        self.continue_button.clicked.connect(self.continue_requested.emit)
        self.continue_button.setEnabled(False)
        main_layout.addWidget(self.continue_button)
        layout.addWidget(main, 0, 0)

        side = SafetyPanel("Connection privacy")
        layout.addWidget(side, 0, 1)

    def set_account_mode(self, value: str) -> None:
        self._account_mode = value
        mode_copy = "team admin app" if value == "team_admin" else "personal Dropbox app"
        self.title_block.body_label.setText(
            f"We open Dropbox in your browser. Approve this {mode_copy}, then paste the authorization code here."
        )
        self.admin_member_id_edit.setVisible(self.advanced.isChecked() and value == "team_admin")

    def set_packaged_app_key(self, app_key: str | None) -> None:
        has_key = bool(app_key)
        self._packaged_app_key_available = has_key
        if has_key:
            self.app_key_edit.setText(app_key or "")
        self._apply_connection_form_visibility()

    def set_saved_credentials_available(self, available: bool, summary: str = "") -> None:
        self._saved_credentials_available = available
        self.saved_panel.setVisible(available)
        if summary:
            self.saved_summary_label.setText(summary)
        self._apply_connection_form_visibility()
        if available:
            self.advanced.setChecked(False)
            self.advanced.setVisible(False)
            self.set_connected(False)
        else:
            self.advanced.setVisible(True)
        self.test_button.setVisible(not available)
        self.disconnect_button.setText("Forget saved connection" if available else "Disconnect")

    def _show_reconnect_form(self) -> None:
        self.set_saved_credentials_available(False)
        self.set_connected(False)
        self.set_status("Connect with Dropbox again, then paste the new authorization code.")

    def _apply_connection_form_visibility(self) -> None:
        show_oauth = not self._saved_credentials_available
        for widget in getattr(self, "_oauth_widgets", []):
            widget.setVisible(show_oauth)
        if show_oauth and self._packaged_app_key_available:
            self.app_key_label.setVisible(False)
            self.app_key_edit.setVisible(False)

    def set_status(self, text: str, success: bool = False) -> None:
        self.status_label.setText(text)
        self.status_label.setStyleSheet(f"color: {theme.SUCCESS if success else theme.MUTED}; background: transparent;")
        self.status_label.setVisible(bool(text) and not success)
        if not success:
            self.account_status_card.hide()

    def set_account_status(
        self,
        *,
        display_name: str,
        email: str | None = None,
        account_mode: str = "personal",
        team_name: str | None = None,
        team_model: str | None = None,
        active_member_count: int | None = None,
        namespace_count: int | None = None,
    ) -> None:
        clear_layout(self.account_details_layout)
        mode_label = "Team admin" if account_mode == "team_admin" else "Personal"
        self.account_status_body.setText(
            "Team-admin access is verified. Dropbox Cleaner can now scan the selected team coverage."
            if account_mode == "team_admin"
            else "Personal Dropbox access is verified. Dropbox Cleaner can now scan selected folders."
        )
        fields: list[tuple[str, str]] = [
            ("Account", display_name or "Unknown"),
            ("Mode", mode_label),
        ]
        if email:
            fields.insert(1, ("Email", email))
        if account_mode == "team_admin":
            fields.extend(
                [
                    ("Team", team_name or "Unknown"),
                    ("Team model", team_model or "Unknown"),
                    ("Active members", str(active_member_count if active_member_count is not None else "Unknown")),
                    ("Namespaces", str(namespace_count if namespace_count is not None else "Unknown")),
                ]
            )
        for index, (label, value) in enumerate(fields):
            label_widget = QLabel(label)
            label_widget.setObjectName("statusLabel")
            value_widget = QLabel(value)
            value_widget.setObjectName("statusValue")
            value_widget.setWordWrap(True)
            row = index // 2
            column = (index % 2) * 2
            self.account_details_layout.addWidget(label_widget, row, column)
            self.account_details_layout.addWidget(value_widget, row, column + 1)
        self.status_label.hide()
        self.saved_panel.hide()
        self.account_status_card.show()

    def set_connected(self, connected: bool) -> None:
        self._connected = connected
        self.continue_button.setEnabled(connected)
        if not connected:
            self.account_status_card.hide()

    def set_busy(self, busy: bool) -> None:
        for widget in getattr(self, "_busy_widgets", []):
            widget.setVisible(busy)
        continue_button = getattr(self, "continue_button", None)
        for widget in (
            getattr(self, "back_button", None),
            getattr(self, "connect_button", None),
            getattr(self, "finish_button", None),
            getattr(self, "test_button", None),
            getattr(self, "disconnect_button", None),
            getattr(self, "use_saved_button", None),
            getattr(self, "reconnect_button", None),
            getattr(self, "save_token_button", None),
            getattr(self, "continue_button", None),
        ):
            if widget is not None:
                widget.setEnabled(not busy and (widget is not continue_button or self._connected))
        if busy:
            self._spinner_index = 0
            self.spinner_label.setText(self._spinner_frames[self._spinner_index])
            self._spinner_timer.start()
        else:
            self._spinner_timer.stop()

    def _advance_spinner(self) -> None:
        self._spinner_index = (self._spinner_index + 1) % len(self._spinner_frames)
        self.spinner_label.setText(self._spinner_frames[self._spinner_index])

    def _set_advanced_visible(self, visible: bool) -> None:
        for widget in self._advanced_widgets:
            widget.setVisible(visible)
        self.admin_member_id_edit.setVisible(visible and self._account_mode == "team_admin")


class SettingsScreen(QWidget):
    back_requested = Signal()
    browse_archive_requested = Signal()
    browse_source_requested = Signal()
    browse_exclusion_requested = Signal()
    browse_output_requested = Signal()
    start_run_requested = Signal()
    resume_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self._account_mode = "personal"
        self._source_roots: list[str] = []
        self._excluded_roots: list[str] = []
        self._selected_run_mode = "dry_run"

        root_layout = QGridLayout(self)
        root_layout.setContentsMargins(34, 28, 34, 34)
        root_layout.setSpacing(20)
        root_layout.setColumnStretch(0, 3)
        root_layout.setColumnStretch(1, 2)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        content = QWidget()
        self.content_layout = QVBoxLayout(content)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(14)
        scroll.setWidget(content)
        root_layout.addWidget(scroll, 0, 0)

        self.content_layout.addWidget(
            TitleBlock(
                "Step 3 of 5",
                "Run settings",
                "Pick the cutoff date, archive folder, reports folder, and run type.",
            )
        )
        back_row = QHBoxLayout()
        self.back_button = QPushButton("Back")
        theme.set_role(self.back_button, "ghost")
        self.back_button.clicked.connect(self.back_requested.emit)
        back_row.addWidget(self.back_button)
        back_row.addStretch(1)
        self.content_layout.addLayout(back_row)
        self._build_date_card()
        self._build_archive_card()
        self._build_source_card()
        self._build_exclusions_card()
        self._build_output_card()
        self._build_advanced_card()
        self.content_layout.addStretch(1)

        side = card_frame("softCard")
        side_layout = QVBoxLayout(side)
        side_layout.setContentsMargins(24, 24, 24, 24)
        side_layout.setSpacing(14)
        heading = QLabel("Choose the run")
        heading.setObjectName("sectionTitle")
        side_layout.addWidget(heading)
        body = QLabel("Start with Preview archive if you want to confirm everything before copying.")
        body.setObjectName("body")
        body.setWordWrap(True)
        side_layout.addWidget(body)
        self.run_cards: dict[str, ChoiceCard] = {}
        for choice in RUN_MODE_CHOICES:
            card = ChoiceCard(choice.label, choice.description, choice.value)
            card.selected.connect(self._select_run_mode)
            side_layout.addWidget(card)
            self.run_cards[choice.value] = card
        self._refresh_run_cards()
        side_layout.addStretch(1)

        action_panel = card_frame()
        action_layout = QVBoxLayout(action_panel)
        action_layout.setContentsMargins(18, 18, 18, 18)
        action_layout.setSpacing(10)
        start_button = QPushButton("Start run")
        theme.set_role(start_button, "primary")
        start_button.clicked.connect(self.start_run_requested.emit)
        action_layout.addWidget(start_button)
        self.resume_button = QPushButton("Resume last run")
        theme.set_role(self.resume_button, "ghost")
        self.resume_button.clicked.connect(self.resume_requested.emit)
        self.resume_button.hide()
        action_layout.addWidget(self.resume_button)
        safe = QLabel("Copy mode asks for confirmation before it starts.")
        safe.setObjectName("safe")
        safe.setWordWrap(True)
        action_layout.addWidget(safe)
        side_layout.addWidget(action_panel)
        root_layout.addWidget(side, 0, 1)

    def _build_date_card(self) -> None:
        card = _settings_card("Cutoff date", "Files older than this date will be included.")
        layout = card.layout()
        row = QHBoxLayout()
        self.cutoff_edit = QDateEdit(QDate(2020, 5, 1))
        self.cutoff_edit.setDisplayFormat("yyyy-MM-dd")
        self.cutoff_edit.setCalendarPopup(True)
        row.addWidget(self.cutoff_edit, 1)
        self.date_filter_combo = QComboBox()
        self.date_filter_combo.addItems([choice.label for choice in DATE_FILTER_CHOICES])
        self.date_filter_combo.setCurrentText(date_filter_value_to_label("server_modified"))
        row.addWidget(self.date_filter_combo, 2)
        layout.addLayout(row)
        self.content_layout.addWidget(card)

    def _build_archive_card(self) -> None:
        card = _settings_card("Archive folder", "Copied files are staged here with the same folder structure.")
        layout = card.layout()
        row = QHBoxLayout()
        self.archive_edit = QLineEdit("/Archive_PreMay2020")
        row.addWidget(self.archive_edit, 1)
        browse = QPushButton("Browse Dropbox")
        browse.clicked.connect(self.browse_archive_requested.emit)
        row.addWidget(browse)
        layout.addLayout(row)
        self.content_layout.addWidget(card)

    def _build_source_card(self) -> None:
        self.source_card = _settings_card(
            "Folders to include",
            "Optional: add folders to limit the scan. Leave this list empty to scan the whole Dropbox.",
        )
        layout = self.source_card.layout()
        self.team_card = _settings_card("Team coverage", "Team admin mode scans team content through a coverage preset.")
        team_layout = self.team_card.layout()
        self.team_coverage_combo = QComboBox()
        self.team_coverage_combo.addItems([choice.label for choice in TEAM_COVERAGE_CHOICES])
        self.team_coverage_combo.setCurrentText(team_coverage_value_to_label("team_owned_only"))
        team_layout.addWidget(self.team_coverage_combo)
        team_archive_layout_label = QLabel("Archive layout")
        team_archive_layout_label.setObjectName("body")
        self.team_archive_layout_combo = QComboBox()
        self.team_archive_layout_combo.addItems([choice.label for choice in TEAM_ARCHIVE_LAYOUT_CHOICES])
        self.team_archive_layout_combo.setCurrentText(team_archive_layout_value_to_label("segmented"))
        team_layout.addWidget(team_archive_layout_label)
        team_layout.addWidget(self.team_archive_layout_combo)

        self.source_list = QListWidget()
        self.source_list.setMinimumHeight(90)
        layout.addWidget(self.source_list)

        typed_row = QHBoxLayout()
        self.source_path_edit = QLineEdit()
        self.source_path_edit.setPlaceholderText("Type a Dropbox path, for example /Screenshots")
        typed_row.addWidget(self.source_path_edit, 1)
        add_typed = QPushButton("Add typed path")
        add_typed.clicked.connect(self._add_typed_source)
        typed_row.addWidget(add_typed)
        layout.addLayout(typed_row)

        button_row = QHBoxLayout()
        add = QPushButton("Add Dropbox folder")
        add.clicked.connect(self.browse_source_requested.emit)
        remove = QPushButton("Remove selected")
        remove.clicked.connect(self._remove_selected_source)
        button_row.addWidget(add)
        button_row.addWidget(remove)
        button_row.addStretch(1)
        layout.addLayout(button_row)
        self._render_source_roots()
        self.content_layout.addWidget(self.source_card)
        self.content_layout.addWidget(self.team_card)

    def _build_exclusions_card(self) -> None:
        self.exclusions_card = _settings_card(
            "Folders to skip",
            "Optional: choose folders that should not be inventoried or copied into the archive.",
        )
        layout = self.exclusions_card.layout()
        self.exclusions_list = QListWidget()
        self.exclusions_list.setMinimumHeight(90)
        layout.addWidget(self.exclusions_list)

        typed_row = QHBoxLayout()
        self.exclude_path_edit = QLineEdit()
        self.exclude_path_edit.setPlaceholderText("Type a Dropbox path, for example /Screenshots")
        typed_row.addWidget(self.exclude_path_edit, 1)
        add_typed = QPushButton("Add typed path")
        add_typed.clicked.connect(self._add_typed_exclusion)
        typed_row.addWidget(add_typed)
        layout.addLayout(typed_row)

        button_row = QHBoxLayout()
        browse = QPushButton("Browse Dropbox")
        browse.clicked.connect(self.browse_exclusion_requested.emit)
        remove = QPushButton("Remove selected")
        remove.clicked.connect(self._remove_selected_exclusion)
        button_row.addWidget(browse)
        button_row.addWidget(remove)
        button_row.addStretch(1)
        layout.addLayout(button_row)
        self.content_layout.addWidget(self.exclusions_card)

    def _build_output_card(self) -> None:
        card = _settings_card("Local reports folder", "CSV files, logs, summaries, and state are written locally.")
        layout = card.layout()
        row = QHBoxLayout()
        self.output_edit = QLineEdit(str(Path("outputs").resolve()))
        row.addWidget(self.output_edit, 1)
        browse = QPushButton("Browse")
        browse.clicked.connect(self.browse_output_requested.emit)
        row.addWidget(browse)
        layout.addLayout(row)
        self.content_layout.addWidget(card)

    def _build_advanced_card(self) -> None:
        advanced = QGroupBox("Advanced settings")
        advanced.setCheckable(True)
        advanced.setChecked(False)
        layout = QGridLayout(advanced)
        self.batch_size = QSpinBox()
        self.batch_size.setRange(1, 10000)
        self.batch_size.setValue(500)
        self.retry_count = QSpinBox()
        self.retry_count.setRange(0, 20)
        self.retry_count.setValue(5)
        self.initial_backoff = QDoubleSpinBox()
        self.initial_backoff.setRange(0.1, 120.0)
        self.initial_backoff.setValue(1.0)
        self.backoff_multiplier = QDoubleSpinBox()
        self.backoff_multiplier.setRange(1.0, 10.0)
        self.backoff_multiplier.setValue(2.0)
        self.max_backoff = QDoubleSpinBox()
        self.max_backoff.setRange(1.0, 600.0)
        self.max_backoff.setValue(30.0)
        self.worker_count = QSpinBox()
        self.worker_count.setRange(1, 8)
        self.worker_count.setValue(1)
        self.conflict_policy = QComboBox()
        self.conflict_policy.addItems(["safe_skip", "abort_run"])
        self.include_folders = QCheckBox("Include folders in inventory export")
        self.include_folders.setChecked(True)
        self.exclude_archive = QCheckBox("Exclude archive folder from traversal")
        self.exclude_archive.setChecked(True)

        fields = [
            ("Batch size", self.batch_size),
            ("Retry count", self.retry_count),
            ("Initial backoff", self.initial_backoff),
            ("Backoff multiplier", self.backoff_multiplier),
            ("Max backoff", self.max_backoff),
            ("Worker count", self.worker_count),
            ("Conflict policy", self.conflict_policy),
        ]
        for row, (label, widget) in enumerate(fields):
            layout.addWidget(QLabel(label), row, 0)
            layout.addWidget(widget, row, 1)
        layout.addWidget(self.include_folders, len(fields), 0, 1, 2)
        layout.addWidget(self.exclude_archive, len(fields) + 1, 0, 1, 2)
        self.content_layout.addWidget(advanced)

    def set_account_mode(self, value: str) -> None:
        self._account_mode = value
        self.source_card.setVisible(True)
        self.team_card.setVisible(value == "team_admin")

    def set_resume_available(self, available: bool) -> None:
        self.resume_button.setVisible(available)

    def source_roots(self) -> list[str]:
        return list(self._source_roots)

    def excluded_roots(self) -> list[str]:
        return list(self._excluded_roots)

    def add_source_root(self, root: str) -> None:
        normalized = normalize_dropbox_path(root)
        if normalized == "/":
            self._source_roots.clear()
        elif normalized not in self._source_roots:
            self._source_roots.append(normalized)
        self._render_source_roots()

    def add_excluded_root(self, root: str) -> None:
        normalized = normalize_dropbox_path(root)
        if normalized != "/" and normalized not in self._excluded_roots:
            self._excluded_roots.append(normalized)
        self._render_excluded_roots()

    def _remove_selected_source(self) -> None:
        row = self.source_list.currentRow()
        if row >= 0 and self._source_roots:
            self._source_roots.pop(row)
        self._render_source_roots()

    def _remove_selected_exclusion(self) -> None:
        row = self.exclusions_list.currentRow()
        if row >= 0:
            self._excluded_roots.pop(row)
        self._render_excluded_roots()

    def _add_typed_exclusion(self) -> None:
        self.add_excluded_root(self.exclude_path_edit.text())
        self.exclude_path_edit.clear()

    def _add_typed_source(self) -> None:
        self.add_source_root(self.source_path_edit.text())
        self.source_path_edit.clear()

    def _render_source_roots(self) -> None:
        self.source_list.clear()
        if not self._source_roots:
            item = QListWidgetItem("Whole Dropbox (no include folders selected)")
            item.setFlags(Qt.ItemFlag.NoItemFlags)
            self.source_list.addItem(item)
            return
        for root in self._source_roots:
            self.source_list.addItem(QListWidgetItem(root))

    def _render_excluded_roots(self) -> None:
        self.exclusions_list.clear()
        for root in self._excluded_roots:
            self.exclusions_list.addItem(QListWidgetItem(root))

    def _select_run_mode(self, value: str) -> None:
        self._selected_run_mode = value
        self._refresh_run_cards()

    def _refresh_run_cards(self) -> None:
        for value, card in self.run_cards.items():
            card.set_selected(value == self._selected_run_mode)

    @property
    def selected_run_mode(self) -> str:
        return self._selected_run_mode

    @property
    def cutoff_date(self) -> str:
        return self.cutoff_edit.date().toString("yyyy-MM-dd")

    @property
    def date_filter_label(self) -> str:
        return self.date_filter_combo.currentText()

    @property
    def archive_root(self) -> str:
        return self.archive_edit.text().strip()

    @property
    def output_dir(self) -> str:
        return self.output_edit.text().strip()

    @property
    def team_coverage_label(self) -> str:
        return self.team_coverage_combo.currentText()

    @property
    def team_archive_layout_label(self) -> str:
        return self.team_archive_layout_combo.currentText()


class RunScreen(QWidget):
    cancel_requested = Signal()
    view_results_requested = Signal()

    COUNTERS = {
        "Scanned": "items_scanned",
        "Namespaces": "namespaces_scanned",
        "Members": "members_covered",
        "Matched": "files_matched",
        "Copied": "files_copied",
        "Skipped": "files_skipped",
        "Failed": "files_failed",
    }

    def __init__(self) -> None:
        super().__init__()
        layout = QGridLayout(self)
        layout.setContentsMargins(34, 28, 34, 34)
        layout.setSpacing(20)
        layout.setColumnStretch(0, 3)
        layout.setColumnStretch(1, 2)

        main = card_frame()
        main_layout = QVBoxLayout(main)
        main_layout.setContentsMargins(28, 28, 28, 28)
        main_layout.setSpacing(14)
        main_layout.addWidget(TitleBlock("Step 4 of 5", "Run in progress", "Dropbox Cleaner is scanning and writing reports."))
        self.safety_label = QLabel("Nothing will be deleted. Originals remain in place.")
        self.safety_label.setObjectName("safe")
        main_layout.addWidget(self.safety_label)
        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        main_layout.addWidget(self.progress)
        self.timeline = PhaseTimeline()
        main_layout.addWidget(self.timeline)
        self.phase_label = QLabel("Starting")
        self.phase_label.setObjectName("sectionTitle")
        main_layout.addWidget(self.phase_label)
        self.message_label = QLabel("Preparing the run.")
        self.message_label.setObjectName("body")
        self.message_label.setWordWrap(True)
        main_layout.addWidget(self.message_label)

        counter_grid = QGridLayout()
        self.counter_tiles: dict[str, MetricTileWidget] = {}
        for index, label in enumerate(self.COUNTERS):
            tile = MetricTileWidget(label, 0, "neutral")
            self.counter_tiles[label] = tile
            counter_grid.addWidget(tile, index // 4, index % 4)
        main_layout.addLayout(counter_grid)

        actions = QHBoxLayout()
        self.cancel_button = QPushButton("Stop safely")
        theme.set_role(self.cancel_button, "danger")
        self.cancel_button.clicked.connect(self.cancel_requested.emit)
        self.results_button = QPushButton("View results")
        theme.set_role(self.results_button, "primary")
        self.results_button.clicked.connect(self.view_results_requested.emit)
        self.results_button.hide()
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.results_button)
        actions.addStretch(1)
        main_layout.addLayout(actions)
        layout.addWidget(main, 0, 0)

        logs_card = card_frame("softCard")
        logs_layout = QVBoxLayout(logs_card)
        logs_layout.setContentsMargins(24, 24, 24, 24)
        logs_layout.addWidget(QLabel("Details for support"))
        self.logs = QPlainTextEdit()
        self.logs.setReadOnly(True)
        logs_layout.addWidget(self.logs, 1)
        layout.addWidget(logs_card, 0, 1)

    def reset(self, dry_run: bool) -> None:
        self.progress.setRange(0, 0)
        self.results_button.hide()
        self.cancel_button.show()
        self.logs.clear()
        self.phase_label.setText("Starting")
        self.message_label.setText("Preparing the run.")
        self.safety_label.setText(
            "Preview mode makes no Dropbox changes." if dry_run else "Nothing will be deleted. Originals remain in place."
        )
        for tile in self.counter_tiles.values():
            tile.set_value(0)

    def apply_progress(self, snapshot: ProgressSnapshot) -> None:
        self.timeline.set_phase(snapshot.phase)
        self.phase_label.setText(_friendly_phase(snapshot.phase))
        self.message_label.setText(snapshot.message)
        for label, key in self.COUNTERS.items():
            self.counter_tiles[label].set_value(snapshot.counters.get(key, 0))

    def append_log(self, line: str) -> None:
        self.logs.appendPlainText(line)

    def mark_completed(self) -> None:
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self.cancel_button.hide()
        self.results_button.show()
        self.phase_label.setText("Completed")
        self.message_label.setText("Reports are ready. Review the visual summary next.")

    def mark_failed(self, message: str) -> None:
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self.cancel_button.hide()
        self.results_button.show()
        self.phase_label.setText("Needs attention")
        self.message_label.setText(message)


class ResultsScreen(QWidget):
    open_output_requested = Signal()
    open_summary_requested = Signal()
    open_manifest_requested = Signal()
    resume_requested = Signal()
    start_another_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self._current_result: ResultsViewModel | None = None
        self._current_run_dir: Path | None = None
        self._last_compact: bool | None = None
        self._layout = QGridLayout(self)
        self._layout.setContentsMargins(34, 28, 34, 34)
        self._layout.setSpacing(20)

    def set_empty(self) -> None:
        self._current_result = None
        self._current_run_dir = None
        self._last_compact = None
        clear_layout(self._layout)
        card = card_frame()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(28, 28, 28, 28)
        layout.addWidget(TitleBlock("Step 5 of 5", "No results yet", "Run a preview or copy job first."))
        self._layout.addWidget(card, 0, 0)

    def set_result(self, result: ResultsViewModel, run_dir: Path) -> None:
        self._current_result = result
        self._current_run_dir = run_dir
        self._last_compact = self._is_compact()
        self._render_result()

    def resizeEvent(self, event) -> None:  # noqa: N802
        super().resizeEvent(event)
        if self._current_result is None:
            return
        compact = self._is_compact()
        if compact != self._last_compact:
            self._last_compact = compact
            self._render_result()

    def _render_result(self) -> None:
        if self._current_result is None or self._current_run_dir is None:
            return
        result = self._current_result
        run_dir = self._current_run_dir
        compact = self._is_compact()
        clear_layout(self._layout)
        self._layout.setColumnStretch(0, 1 if compact else 5)
        self._layout.setColumnStretch(1, 0 if compact else 2)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(self._build_results_content(result, run_dir, compact=compact))
        self._layout.addWidget(scroll, 0, 0)
        if not compact:
            self._layout.addWidget(self._build_side_panel(result, run_dir), 0, 1)

    def _build_results_content(self, result: ResultsViewModel, run_dir: Path, *, compact: bool) -> QWidget:
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(16)
        content_layout.addWidget(TitleBlock("Step 5 of 5", "Run complete", result.success_message))
        content_layout.addWidget(metrics_grid(result.metrics, columns=2 if compact else 4))
        if compact:
            content_layout.addWidget(self._build_side_panel(result, run_dir))
        folders = card_frame()
        folders_layout = QVBoxLayout(folders)
        folders_layout.setContentsMargins(18, 18, 18, 18)
        folder_title = QLabel("Top folders")
        folder_title.setObjectName("sectionTitle")
        folders_layout.addWidget(folder_title)
        table = FolderTable()
        table.setMinimumHeight(180)
        table.set_folders(result.top_folders)
        folders_layout.addWidget(table)
        content_layout.addWidget(folders)

        issues = card_frame()
        issues_layout = QVBoxLayout(issues)
        issues_layout.setContentsMargins(18, 18, 18, 18)
        issue_title = QLabel(result.review_title)
        issue_title.setObjectName("sectionTitle")
        issues_layout.addWidget(issue_title)
        issue_table = IssueTable()
        issue_table.setMinimumHeight(200)
        issue_table.set_result(result)
        issues_layout.addWidget(issue_table)
        content_layout.addWidget(issues)
        content_layout.addStretch(1)
        return content

    def _build_side_panel(self, result: ResultsViewModel, run_dir: Path) -> QFrame:
        side = card_frame("softCard")
        if not self._is_compact():
            side.setMinimumWidth(360)
            side.setMaximumWidth(520)
        side_layout = QVBoxLayout(side)
        side_layout.setContentsMargins(24, 24, 24, 24)
        side_layout.setSpacing(14)
        run_id_label = QLabel(f"Run ID: {result.run_id or 'unknown'}")
        run_id_label.setWordWrap(True)
        output_label = QLabel(f"Output folder: {run_dir}")
        output_label.setWordWrap(True)
        side_layout.addWidget(run_id_label)
        side_layout.addWidget(output_label)
        donut = DonutChartWidget()
        donut.set_slices(result.status_slices)
        side_layout.addWidget(donut)
        verification = result.verification or {}
        verify = VerificationBarWidget()
        verify.set_values(
            int(verification.get("source_matched_file_count", 0) or 0),
            int(verification.get("archive_staged_file_count", 0) or 0),
        )
        side_layout.addWidget(verify)
        output_button = QPushButton("Open output folder")
        theme.set_role(output_button, "primary")
        output_button.clicked.connect(self.open_output_requested.emit)
        side_layout.addWidget(output_button)
        summary_button = QPushButton("Open summary")
        summary_button.clicked.connect(self.open_summary_requested.emit)
        side_layout.addWidget(summary_button)
        manifest_button = QPushButton("Open manifest")
        manifest_button.clicked.connect(self.open_manifest_requested.emit)
        side_layout.addWidget(manifest_button)
        retry_button = QPushButton("Resume or retry last run")
        theme.set_role(retry_button, "ghost")
        retry_button.clicked.connect(self.resume_requested.emit)
        side_layout.addWidget(retry_button)
        another_button = QPushButton("Start another run")
        another_button.clicked.connect(self.start_another_requested.emit)
        side_layout.addWidget(another_button)
        side_layout.addStretch(1)
        return side

    def _is_compact(self) -> bool:
        return self.width() < 1180


def _settings_card(title: str, body: str) -> QFrame:
    card = card_frame()
    layout = QVBoxLayout(card)
    layout.setContentsMargins(20, 18, 20, 18)
    layout.setSpacing(10)
    heading = QLabel(title)
    heading.setObjectName("sectionTitle")
    description = QLabel(body)
    description.setObjectName("body")
    description.setWordWrap(True)
    layout.addWidget(heading)
    layout.addWidget(description)
    return card


def _friendly_phase(phase: str) -> str:
    return {
        "connecting": "Connecting",
        "team_discovery": "Discovering team content",
        "inventory": "Scanning Dropbox",
        "filter": "Finding older files",
        "copy": "Copying archive files",
        "verify": "Verifying archive",
        "outputs": "Writing reports",
        "completed": "Completed",
    }.get(phase, phase.replace("_", " ").title())
