from __future__ import annotations

from PySide6.QtWidgets import QWidget


APP_TITLE = "Dropbox Cleaner"
BACKGROUND = "#F6F8F5"
SURFACE = "#FFFFFF"
SOFT = "#EAF4F1"
SOFT_ALT = "#F1F6F4"
INK = "#172D34"
MUTED = "#63767B"
ACCENT = "#167A8B"
ACCENT_DARK = "#105D6B"
SUCCESS = "#2E7D5B"
WARNING = "#B96E25"
DANGER = "#C84C4C"
BORDER = "#D9E5E2"


def app_stylesheet() -> str:
    return f"""
    QWidget {{
        background: {BACKGROUND};
        color: {INK};
        font-family: "Segoe UI", "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
        font-size: 14px;
    }}

    QLabel {{
        background: transparent;
    }}

    QFrame#card, QWidget#card {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 18px;
    }}

    QFrame#softCard, QWidget#softCard {{
        background: {SOFT};
        border: 1px solid #D1E5E1;
        border-radius: 18px;
    }}

    QFrame#successCard, QWidget#successCard {{
        background: #E9F6F1;
        border: 1px solid #BFDCD0;
        border-radius: 18px;
    }}

    QLabel#eyebrow {{
        color: {MUTED};
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }}

    QLabel#title {{
        color: {INK};
        font-size: 34px;
        font-weight: 800;
    }}

    QLabel#sectionTitle {{
        color: {INK};
        font-size: 20px;
        font-weight: 800;
    }}

    QLabel#body {{
        color: {MUTED};
        font-size: 15px;
        line-height: 1.35;
    }}

    QLabel#safe {{
        color: {SUCCESS};
        font-size: 14px;
        font-weight: 700;
    }}

    QLabel#statusLabel {{
        color: {MUTED};
        font-size: 12px;
        font-weight: 800;
        letter-spacing: 0.03em;
        text-transform: uppercase;
    }}

    QLabel#statusValue {{
        color: {INK};
        font-size: 15px;
        font-weight: 700;
    }}

    QLabel#spinner {{
        color: {ACCENT};
        font-size: 18px;
        font-weight: 900;
    }}

    QPushButton {{
        background: {SURFACE};
        color: {INK};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 10px 16px;
        font-weight: 700;
    }}

    QPushButton:hover {{
        background: #EEF7F5;
        border-color: #B9D7D2;
    }}

    QPushButton:pressed {{
        background: #DDEBE8;
        border-color: {ACCENT};
        padding-top: 12px;
        padding-bottom: 8px;
    }}

    QPushButton:checked {{
        background: {ACCENT};
        color: white;
        border-color: {ACCENT};
    }}

    QPushButton:disabled {{
        color: #9AA9AD;
        background: #EDF1EF;
        border-color: #E0E7E4;
    }}

    QPushButton[role="primary"] {{
        background: {ACCENT};
        color: white;
        border-color: {ACCENT};
    }}

    QPushButton[role="primary"]:hover {{
        background: {ACCENT_DARK};
        border-color: {ACCENT_DARK};
    }}

    QPushButton[role="primary"]:pressed {{
        background: #0B4752;
        border-color: #0B4752;
    }}

    QPushButton[role="success"] {{
        background: {SUCCESS};
        color: white;
        border-color: {SUCCESS};
    }}

    QPushButton[role="success"]:hover {{
        background: #246A4D;
        border-color: #246A4D;
    }}

    QPushButton[role="success"]:pressed {{
        background: #1D573F;
        border-color: #1D573F;
    }}

    QPushButton[role="danger"] {{
        color: {DANGER};
        border-color: #E5C9C9;
        background: #FFF8F8;
    }}

    QPushButton[role="danger"]:hover {{
        background: #FCECEC;
        border-color: #DBA9A9;
    }}

    QPushButton[role="danger"]:pressed {{
        background: #F4DADA;
        border-color: {DANGER};
    }}

    QPushButton[role="ghost"] {{
        color: {ACCENT};
        border-color: transparent;
        background: transparent;
    }}

    QPushButton[role="ghost"]:hover {{
        color: {ACCENT_DARK};
        background: #E6F2EF;
        border-color: #D1E5E1;
    }}

    QPushButton[role="ghost"]:pressed {{
        background: #D5E9E5;
    }}

    QPushButton[role="selectedCard"] {{
        background: {ACCENT};
        color: white;
        border-color: {ACCENT};
        text-align: left;
    }}

    QPushButton[role="selectedCard"]:hover {{
        background: {ACCENT_DARK};
        border-color: {ACCENT_DARK};
    }}

    QPushButton[role="selectedCard"]:pressed {{
        background: #0B4752;
        border-color: #0B4752;
    }}

    QPushButton[role="card"] {{
        background: {SURFACE};
        color: {INK};
        border: 1px solid {BORDER};
        text-align: left;
        padding: 18px;
        border-radius: 16px;
    }}

    QPushButton[role="card"]:hover {{
        background: #EEF8F5;
        border-color: {ACCENT};
    }}

    QPushButton[role="card"]:pressed {{
        background: #DDEFEA;
        border-color: {ACCENT_DARK};
    }}

    QLineEdit, QComboBox, QDateEdit, QSpinBox, QDoubleSpinBox {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 10px;
        padding: 8px 10px;
        min-height: 24px;
    }}

    QLineEdit:focus, QComboBox:focus, QDateEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus {{
        border: 1px solid {ACCENT};
    }}

    QCheckBox {{
        spacing: 8px;
    }}

    QTextEdit, QPlainTextEdit {{
        background: #10272E;
        color: #E9F4F2;
        border: 1px solid #24434A;
        border-radius: 14px;
        padding: 10px;
        font-family: Consolas, "SF Mono", monospace;
        font-size: 12px;
    }}

    QProgressBar {{
        background: #DDE9E6;
        border: 0;
        border-radius: 8px;
        min-height: 14px;
        text-align: center;
    }}

    QProgressBar::chunk {{
        background: {ACCENT};
        border-radius: 8px;
    }}

    QTableWidget, QTreeWidget {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
        gridline-color: #E8EFEC;
        selection-background-color: #DDF0EC;
        selection-color: {INK};
    }}

    QHeaderView::section {{
        background: #F1F6F4;
        color: {MUTED};
        border: 0;
        padding: 8px;
        font-weight: 700;
    }}

    QScrollArea {{
        border: 0;
    }}
    """


def set_role(widget: QWidget, role: str) -> None:
    widget.setProperty("role", role)
    widget.style().unpolish(widget)
    widget.style().polish(widget)
