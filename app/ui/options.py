from __future__ import annotations

from dataclasses import dataclass

from app.models.config import AccountMode, DateFilterField, RunMode, TeamArchiveLayout, TeamCoveragePreset


@dataclass(frozen=True, slots=True)
class Choice:
    label: str
    value: str
    description: str


ACCOUNT_CHOICES: tuple[Choice, ...] = (
    Choice("Personal Dropbox", "personal", "Use one Dropbox account.",),
    Choice("Team Dropbox", "team_admin", "Scan team content with an admin-authorized app.",),
)

RUN_MODE_CHOICES: tuple[Choice, ...] = (
    Choice("Inventory only", "inventory_only", "Scan Dropbox and create a file list."),
    Choice("Preview archive", "dry_run", "Show what would be copied. No Dropbox changes."),
    Choice("Copy to archive", "copy_run", "Create archive copies. Originals stay in place."),
)

DATE_FILTER_CHOICES: tuple[Choice, ...] = (
    Choice("Dropbox modified date", "server_modified", "Best audit default. Uses Dropbox's server-side modified date."),
    Choice("Original file date", "client_modified", "Useful when imported files keep older original dates."),
    Choice("Oldest available date", "oldest_modified", "Matches if either Dropbox or original file date is old."),
)

TEAM_COVERAGE_CHOICES: tuple[Choice, ...] = (
    Choice("Team-owned only", "team_owned_only", "Only team-owned and shared team namespaces."),
    Choice("All team content", "all_team_content", "Team-owned folders plus active member home namespaces."),
)

TEAM_ARCHIVE_LAYOUT_CHOICES: tuple[Choice, ...] = (
    Choice("Separate team/member folders", "segmented", "Safest default. Keeps team space, member homes, and shared namespaces separate."),
    Choice("Merge into one archive folder", "merged", "Copies into one archive tree using visible folder paths where possible."),
)


def account_label_to_value(label: str) -> AccountMode:
    return _label_to_value(label, ACCOUNT_CHOICES, "personal")  # type: ignore[return-value]


def run_label_to_value(label: str) -> RunMode:
    return _label_to_value(label, RUN_MODE_CHOICES, "dry_run")  # type: ignore[return-value]


def run_value_to_label(value: str) -> str:
    return _value_to_label(value, RUN_MODE_CHOICES, "Preview archive")


def date_filter_label_to_value(label: str) -> DateFilterField:
    return _label_to_value(label, DATE_FILTER_CHOICES, "server_modified")  # type: ignore[return-value]


def date_filter_value_to_label(value: str) -> str:
    return _value_to_label(value, DATE_FILTER_CHOICES, "Dropbox modified date")


def team_coverage_label_to_value(label: str) -> TeamCoveragePreset:
    return _label_to_value(label, TEAM_COVERAGE_CHOICES, "team_owned_only")  # type: ignore[return-value]


def team_coverage_value_to_label(value: str) -> str:
    return _value_to_label(value, TEAM_COVERAGE_CHOICES, "Team-owned only")


def team_archive_layout_label_to_value(label: str) -> TeamArchiveLayout:
    return _label_to_value(label, TEAM_ARCHIVE_LAYOUT_CHOICES, "segmented")  # type: ignore[return-value]


def team_archive_layout_value_to_label(value: str) -> str:
    return _value_to_label(value, TEAM_ARCHIVE_LAYOUT_CHOICES, "Separate team/member folders")


def _label_to_value(label: str, choices: tuple[Choice, ...], default: str) -> str:
    for choice in choices:
        if choice.label == label:
            return choice.value
    return default


def _value_to_label(value: str, choices: tuple[Choice, ...], default: str) -> str:
    for choice in choices:
        if choice.value == value:
            return choice.label
    return default
