from __future__ import annotations

from collections.abc import Iterable

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from app.ui.results import FolderResult, ResultsViewModel, StatusSlice
from app.ui.qt import theme


def clear_layout(layout) -> None:
    while layout.count():
        item = layout.takeAt(0)
        child = item.widget()
        if child is not None:
            child.deleteLater()
        child_layout = item.layout()
        if child_layout is not None:
            clear_layout(child_layout)


def card_frame(object_name: str = "card") -> QFrame:
    frame = QFrame()
    frame.setObjectName(object_name)
    return frame


class TitleBlock(QWidget):
    def __init__(self, eyebrow: str, title: str, body: str) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        self.eyebrow_label = QLabel(eyebrow)
        self.eyebrow_label.setObjectName("eyebrow")
        self.title_label = QLabel(title)
        self.title_label.setObjectName("title")
        self.title_label.setWordWrap(True)
        self.body_label = QLabel(body)
        self.body_label.setObjectName("body")
        self.body_label.setWordWrap(True)
        layout.addWidget(self.eyebrow_label)
        layout.addWidget(self.title_label)
        layout.addWidget(self.body_label)


class ChoiceCard(QPushButton):
    selected = Signal(str)

    def __init__(self, label: str, description: str, value: str) -> None:
        super().__init__(f"{label}\n{description}")
        self.value = value
        self.setMinimumHeight(92)
        self.setCheckable(True)
        self.clicked.connect(lambda: self.selected.emit(self.value))
        theme.set_role(self, "card")

    def set_selected(self, selected: bool) -> None:
        self.setChecked(selected)
        theme.set_role(self, "selectedCard" if selected else "card")


class MetricTileWidget(QFrame):
    def __init__(self, label: str, value: int | str, tone: str = "neutral") -> None:
        super().__init__()
        self.setObjectName("card")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(4)
        self.value_label = QLabel(str(value))
        self.value_label.setFont(QFont("", 24, QFont.Weight.Bold))
        self.value_label.setStyleSheet(f"color: {_tone_color(tone)}; background: transparent;")
        label_widget = QLabel(label)
        label_widget.setStyleSheet(f"color: {theme.MUTED}; background: transparent;")
        layout.addWidget(self.value_label)
        layout.addWidget(label_widget)

    def set_value(self, value: int | str) -> None:
        self.value_label.setText(str(value))


class SafetyPanel(QFrame):
    def __init__(self, title: str = "Safe by design") -> None:
        super().__init__()
        self.setObjectName("softCard")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(10)
        heading = QLabel(title)
        heading.setObjectName("sectionTitle")
        layout.addWidget(heading)
        for line in (
            "Dropbox opens in your browser for approval.",
            "No Dropbox password is requested.",
            "Tokens are stored locally when possible.",
            "No files are changed while connecting.",
        ):
            label = QLabel(f"- {line}")
            label.setObjectName("body")
            label.setWordWrap(True)
            layout.addWidget(label)
        layout.addStretch(1)


class PhaseTimeline(QWidget):
    PHASES = (
        ("connecting", "Connect"),
        ("team_discovery", "Team"),
        ("inventory", "Scan"),
        ("filter", "Match"),
        ("copy", "Archive"),
        ("verify", "Verify"),
        ("outputs", "Reports"),
        ("completed", "Done"),
    )

    def __init__(self) -> None:
        super().__init__()
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        self.labels: dict[str, QLabel] = {}
        for phase, text in self.PHASES:
            label = QLabel(text)
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setStyleSheet(_phase_style(active=False))
            layout.addWidget(label)
            self.labels[phase] = label

    def set_phase(self, phase: str) -> None:
        seen = True
        for key, label in reversed(list(self.labels.items())):
            active = key == phase or (phase == "completed" and key == "completed")
            if key == phase:
                seen = False
            label.setStyleSheet(_phase_style(active=active, completed=not seen))


class DonutChartWidget(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._slices: list[StatusSlice] = []
        self.setMinimumSize(260, 210)

    def set_slices(self, slices: Iterable[StatusSlice]) -> None:
        self._slices = list(slices)
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        del event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setFont(_ui_font(self, 10))
        rect = self.rect().adjusted(18, 18, -110, -18)
        size = min(rect.width(), rect.height())
        rect.setWidth(size)
        rect.setHeight(size)
        total = sum(item.value for item in self._slices)
        if total <= 0:
            painter.setPen(QColor(theme.MUTED))
            painter.setFont(_ui_font(self, 10, QFont.Weight.Medium))
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "No copy results yet")
            return
        start = 90 * 16
        for item in self._slices:
            span = int(-360 * 16 * item.value / total)
            painter.setBrush(QColor(item.color))
            painter.setPen(QPen(QColor(theme.SOFT), 2))
            painter.drawPie(rect, start, span)
            start += span
        inner = rect.adjusted(size // 4, size // 4, -size // 4, -size // 4)
        painter.setBrush(QColor(theme.SOFT))
        painter.setPen(QColor(theme.SOFT))
        painter.drawEllipse(inner)
        y = 42
        painter.setFont(_ui_font(self, 9, QFont.Weight.DemiBold))
        for item in self._slices:
            painter.setBrush(QColor(item.color))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawRoundedRect(self.width() - 96, y, 14, 14, 3, 3)
            painter.setPen(QColor(theme.INK))
            painter.drawText(self.width() - 76, y + 12, f"{item.label}: {item.value}")
            y += 30


class VerificationBarWidget(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._source = 0
        self._staged = 0
        self.setMinimumHeight(130)

    def set_values(self, source: int, staged: int) -> None:
        self._source = source
        self._staged = staged
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        del event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(QColor(theme.INK))
        painter.setFont(_ui_font(self, 11, QFont.Weight.Bold))
        painter.drawText(4, 18, "Source vs staged archive")
        max_value = max(self._source, self._staged, 1)
        self._bar(painter, 4, 42, self._source, max_value, "#BBDCD7", f"Matched source: {self._source}")
        self._bar(painter, 4, 82, self._staged, max_value, theme.SUCCESS, f"Staged archive: {self._staged}")

    def _bar(self, painter: QPainter, x: int, y: int, value: int, max_value: int, color: str, label: str) -> None:
        width = max(8, int((self.width() - 16) * value / max_value))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#DDE9E6"))
        painter.drawRoundedRect(x, y, self.width() - 16, 24, 8, 8)
        painter.setBrush(QColor(color))
        painter.drawRoundedRect(x, y, width, 24, 8, 8)
        painter.setPen(QColor(theme.INK))
        painter.setFont(_ui_font(self, 10, QFont.Weight.DemiBold))
        painter.drawText(x + 10, y + 17, label)


class FolderTable(QTableWidget):
    def __init__(self) -> None:
        super().__init__(0, 5)
        self.setHorizontalHeaderLabels(["Folder", "Matched", "Copied", "Skipped", "Failed"])
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for column in range(1, 5):
            self.horizontalHeader().setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)

    def set_folders(self, folders: list[FolderResult]) -> None:
        self.setRowCount(len(folders))
        for row, folder in enumerate(folders):
            values = [folder.folder, folder.matched, folder.copied, folder.skipped, folder.failed]
            for col, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if col:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.setItem(row, col, item)
        self.resizeColumnsToContents()


class IssueTable(QTableWidget):
    def __init__(self) -> None:
        super().__init__(0, 2)
        self.setHorizontalHeaderLabels(["Type", "Details"])
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)

    def set_result(self, result: ResultsViewModel) -> None:
        rows = [("Blocked", item) for item in result.blocked]
        rows += [("Failed", item) for item in result.failures]
        rows += [("Conflict", item) for item in result.conflicts]
        rows += [("Already archived", item) for item in result.already_archived]
        if not rows:
            rows = [("Clear", "No conflicts, failures, blocked items, or skipped archive hits recorded.")]
        self.setRowCount(min(len(rows), 20))
        for row, (kind, detail) in enumerate(rows[:20]):
            self.setItem(row, 0, QTableWidgetItem(kind))
            self.setItem(row, 1, QTableWidgetItem(detail))
        self.resizeColumnsToContents()


def metrics_grid(metrics, columns: int = 4) -> QWidget:
    container = QWidget()
    layout = QGridLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(12)
    for index, metric in enumerate(metrics):
        layout.addWidget(MetricTileWidget(metric.label, metric.value, metric.tone), index // columns, index % columns)
    for col in range(columns):
        layout.setColumnStretch(col, 1)
    return container


def labeled_value(label: str, value: str) -> QWidget:
    container = QWidget()
    layout = QVBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(4)
    title = QLabel(label)
    title.setObjectName("eyebrow")
    text = QLabel(value)
    text.setObjectName("body")
    text.setWordWrap(True)
    layout.addWidget(title)
    layout.addWidget(text)
    return container


def _tone_color(tone: str) -> str:
    return {
        "accent": theme.ACCENT,
        "success": theme.SUCCESS,
        "warning": theme.WARNING,
        "danger": theme.DANGER,
    }.get(tone, theme.INK)


def _ui_font(widget: QWidget, size: int, weight: QFont.Weight = QFont.Weight.Normal) -> QFont:
    font = QFont(widget.font())
    font.setPointSize(size)
    font.setWeight(weight)
    return font


def _phase_style(*, active: bool, completed: bool = False) -> str:
    if active:
        return f"background: {theme.ACCENT}; color: white; border-radius: 12px; padding: 8px; font-weight: 800;"
    if completed:
        return f"background: #DDEFEA; color: {theme.SUCCESS}; border-radius: 12px; padding: 8px; font-weight: 800;"
    return f"background: {theme.SOFT_ALT}; color: {theme.MUTED}; border-radius: 12px; padding: 8px; font-weight: 700;"
