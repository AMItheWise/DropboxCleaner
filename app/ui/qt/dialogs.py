from __future__ import annotations

import logging
from pathlib import Path

from PySide6.QtCore import Qt, QThread
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from app.dropbox_client.adapter import DropboxAdapter
from app.models.config import AuthConfig, JobConfig
from app.ui.folder_browser import BrowserFolder, BrowserLocation, DropboxFolderBrowserService
from app.ui.qt import theme
from app.ui.qt.workers import FolderLoadWorker
from app.utils.paths import normalize_dropbox_path


class ErrorDetailsDialog(QDialog):
    def __init__(self, title: str, message: str, details: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(760, 520)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(14)

        heading = QLabel(title)
        heading.setObjectName("sectionTitle")
        layout.addWidget(heading)

        label = QLabel(message)
        label.setObjectName("body")
        label.setWordWrap(True)
        layout.addWidget(label)

        details_box = QTextEdit()
        details_box.setPlainText(details)
        details_box.setReadOnly(True)
        layout.addWidget(details_box, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)


class DropboxFolderPickerDialog(QDialog):
    def __init__(
        self,
        *,
        auth_config: AuthConfig,
        job_config: JobConfig,
        purpose: str,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Choose Dropbox folder")
        self.resize(720, 580)
        self.selected_path: str | None = None
        self._purpose = purpose
        self._threads: list[QThread] = []
        self._workers: list[FolderLoadWorker] = []
        self._logger = logging.getLogger("dropbox_cleaner.ui.folder_picker")
        self._logger.addHandler(logging.NullHandler())
        self._adapter = DropboxAdapter(auth_config, self._logger)
        self._service = DropboxFolderBrowserService(
            self._adapter,
            account_mode=auth_config.account_mode,
            job_config=job_config,
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(22, 22, 22, 22)
        layout.setSpacing(12)

        title = QLabel("Choose where Dropbox Cleaner should work")
        title.setObjectName("sectionTitle")
        layout.addWidget(title)

        if purpose == "archive":
            help_text = "Pick a folder for archive copies. The Dropbox root cannot be used as the archive destination."
        elif purpose == "exclude":
            help_text = "Pick a folder to skip. Files inside it will not be inventoried or copied."
        else:
            help_text = "Pick a Dropbox folder to scan. You can add more folders after choosing this one."
        body = QLabel(help_text)
        body.setObjectName("body")
        body.setWordWrap(True)
        layout.addWidget(body)

        self.path_label = QLabel("Dropbox")
        self.path_label.setObjectName("safe")
        layout.addWidget(self.path_label)
        self.loading_label = QLabel("Loading Dropbox folders...")
        self.loading_label.setObjectName("body")
        self.loading_label.setWordWrap(True)
        self.loading_label.hide()
        layout.addWidget(self.loading_label)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Folder", "Location"])
        self.tree.setColumnWidth(0, 260)
        self.tree.itemExpanded.connect(self._on_item_expanded)
        self.tree.currentItemChanged.connect(lambda *_: self._refresh_selection_label())
        self.tree.itemDoubleClicked.connect(lambda item, _col: self.tree.expandItem(item))
        layout.addWidget(self.tree, 1)

        button_row = QHBoxLayout()
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self._reload_root)
        button_row.addWidget(self.refresh_button)
        button_row.addStretch(1)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        choose_button = QPushButton("Choose this folder")
        theme.set_role(choose_button, "primary")
        choose_button.clicked.connect(self._choose_current)
        button_row.addWidget(cancel_button)
        button_row.addWidget(choose_button)
        layout.addLayout(button_row)

        self._load_root()

    def closeEvent(self, event) -> None:  # noqa: N802
        try:
            self._adapter.close()
        finally:
            super().closeEvent(event)

    def reject(self) -> None:
        self._adapter.close()
        super().reject()

    def accept(self) -> None:
        self._adapter.close()
        super().accept()

    def _load_root(self) -> None:
        self.loading_label.hide()
        self.tree.clear()
        root = QTreeWidgetItem(["Dropbox", "/"])
        root.setData(0, Qt.ItemDataRole.UserRole, self._service.root_location())
        root.setData(0, Qt.ItemDataRole.UserRole + 1, False)
        root.addChild(_loading_item("Loading Dropbox folders..."))
        self.tree.addTopLevelItem(root)
        if self._service.has_advanced_team_locations():
            advanced = QTreeWidgetItem(["Advanced team locations", "Team namespaces"])
            advanced.setToolTip(
                0,
                "Shows raw Dropbox team namespaces. Most users should choose folders from the Dropbox node above.",
            )
            advanced.setData(0, Qt.ItemDataRole.UserRole, self._service.advanced_team_root_location())
            advanced.setData(0, Qt.ItemDataRole.UserRole + 1, False)
            advanced.addChild(_loading_item("Loading team locations..."))
            self.tree.addTopLevelItem(advanced)
        self.tree.setCurrentItem(root)
        self.tree.expandItem(root)

    def _reload_root(self) -> None:
        self._load_root()

    def _on_item_expanded(self, item: QTreeWidgetItem) -> None:
        if item.data(0, Qt.ItemDataRole.UserRole + 1):
            return
        location = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(location, BrowserLocation):
            return
        item.setData(0, Qt.ItemDataRole.UserRole + 1, True)
        item.takeChildren()
        item.addChild(_loading_item("Loading folders..."))
        self._load_children(item, location)

    def _load_children(self, item: QTreeWidgetItem, location: BrowserLocation) -> None:
        self.loading_label.setText(f"Loading folders in {location.display_path}...")
        self.loading_label.show()
        worker = FolderLoadWorker(service=self._service, location=location)
        thread = QThread(self)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.signals.folders.connect(lambda _location, folders, item=item: self._apply_children(item, folders))
        worker.signals.error.connect(lambda message, details: self._show_error(message, details))
        worker.signals.finished.connect(thread.quit)
        worker.signals.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda thread=thread: self._threads.remove(thread) if thread in self._threads else None)
        thread.finished.connect(lambda worker=worker: self._workers.remove(worker) if worker in self._workers else None)
        self._threads.append(thread)
        self._workers.append(worker)
        thread.start()

    def _apply_children(self, item: QTreeWidgetItem, folders: list[BrowserFolder]) -> None:
        self.loading_label.hide()
        item.takeChildren()
        if not folders:
            item.addChild(_loading_item("No folders here"))
            return
        for folder in folders:
            child = QTreeWidgetItem([folder.name, folder.display_path])
            child.setToolTip(0, folder.subtitle)
            child.setData(0, Qt.ItemDataRole.UserRole, folder.location)
            child.setData(0, Qt.ItemDataRole.UserRole + 1, False)
            child.addChild(_loading_item("Expand to load folders"))
            item.addChild(child)
        self.tree.resizeColumnToContents(0)
        self.tree.resizeColumnToContents(1)

    def _refresh_selection_label(self) -> None:
        item = self.tree.currentItem()
        location = item.data(0, Qt.ItemDataRole.UserRole) if item else None
        if isinstance(location, BrowserLocation):
            self.path_label.setText(f"Selected: {location.display_path}")

    def _choose_current(self) -> None:
        item = self.tree.currentItem()
        location = item.data(0, Qt.ItemDataRole.UserRole) if item else None
        if not isinstance(location, BrowserLocation):
            return
        selected = normalize_dropbox_path(location.display_path)
        if self._purpose == "archive" and selected == "/":
            QMessageBox.information(
                self,
                "Choose a folder inside Dropbox",
                "The Dropbox root cannot be used as the archive folder. Choose or create a dedicated folder instead.",
            )
            return
        if self._purpose == "exclude" and selected == "/":
            QMessageBox.information(
                self,
                "Choose a folder inside Dropbox",
                "The Dropbox root cannot be skipped because that would exclude the whole run.",
            )
            return
        self.selected_path = selected
        self.accept()

    def _show_error(self, message: str, details: str) -> None:
        self.loading_label.hide()
        ErrorDetailsDialog("Dropbox folder browser error", message, details, self).exec()


def choose_local_output_dir(parent: QWidget | None, current: str) -> str | None:
    from PySide6.QtWidgets import QFileDialog

    selected = QFileDialog.getExistingDirectory(parent, "Choose output folder", str(Path(current).expanduser()))
    return selected or None


def _loading_item(message: str) -> QTreeWidgetItem:
    item = QTreeWidgetItem([f"  {message}", ""])
    item.setFirstColumnSpanned(True)
    item.setToolTip(0, message)
    item.setFlags(Qt.ItemFlag.NoItemFlags)
    return item
