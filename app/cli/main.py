from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from app.dropbox_client.auth import AuthManager, default_scopes_for_mode
from app.models.config import AuthConfig, JobConfig, RetrySettings
from app.services.orchestrator import RunOrchestrator
from app.utils.config import load_yaml_file


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 1
    return args.handler(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inventory Dropbox files and stage archival copies safely.")
    subparsers = parser.add_subparsers(dest="command")

    oauth_parser = subparsers.add_parser("oauth-link", help="Run the Dropbox PKCE auth flow and save credentials.")
    oauth_parser.add_argument("--app-key", required=True, help="Dropbox app key.")
    oauth_parser.add_argument(
        "--account-mode",
        choices=("personal", "team_admin"),
        default="personal",
        help="Authentication mode for Dropbox access.",
    )
    oauth_parser.add_argument(
        "--scopes",
        nargs="*",
        help="Dropbox OAuth scopes to request. Defaults to the recommended set for the selected account mode.",
    )
    oauth_parser.set_defaults(handler=handle_oauth_link)

    connect_parser = subparsers.add_parser("connect-test", help="Test Dropbox authentication.")
    add_auth_args(connect_parser)
    connect_parser.set_defaults(handler=handle_connect_test)

    for command_name, mode in (("inventory", "inventory_only"), ("dry-run", "dry_run"), ("copy", "copy_run")):
        command_parser = subparsers.add_parser(command_name, help=f"Run a {command_name} workflow.")
        add_auth_args(command_parser)
        add_job_args(command_parser)
        command_parser.set_defaults(handler=handle_run_command, workflow_mode=mode)

    resume_parser = subparsers.add_parser("resume", help="Resume the latest interrupted run from a state DB.")
    add_auth_args(resume_parser)
    resume_parser.add_argument("--job-state", type=Path, help="Path to an existing state.db file.")
    resume_parser.add_argument("--output-dir", type=Path, default=Path("outputs"), help="Base output directory.")
    resume_parser.set_defaults(handler=handle_resume_command)

    verify_parser = subparsers.add_parser("verify", help="Run verification against an existing run.")
    add_auth_args(verify_parser)
    verify_parser.add_argument("--job-state", type=Path, help="Path to an existing state.db file.")
    verify_parser.add_argument("--output-dir", type=Path, default=Path("outputs"), help="Base output directory.")
    verify_parser.set_defaults(handler=handle_verify_command)

    return parser


def add_auth_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, help="Optional YAML config file.")
    parser.add_argument("--use-saved-auth", action="store_true", help="Use previously saved credentials.")
    parser.add_argument("--store-label", default="default", help="Saved credential label.")
    parser.add_argument("--account-mode", choices=("personal", "team_admin"), help="Dropbox account mode.")
    parser.add_argument("--app-key", help="Dropbox app key.")
    parser.add_argument("--refresh-token", help="Dropbox refresh token.")
    parser.add_argument("--access-token", help="Dropbox access token.")
    parser.add_argument("--admin-member-id", help="Optional Dropbox team admin member ID override for team-admin mode.")


def add_job_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-root", action="append", dest="source_roots", help="Dropbox root path to include in personal mode.")
    parser.add_argument(
        "--exclude-root",
        action="append",
        dest="excluded_roots",
        help="Dropbox folder path to exclude from inventory and copy planning. Can be supplied more than once.",
    )
    parser.add_argument("--cutoff-date", default=None, help="Cutoff date in YYYY-MM-DD format.")
    parser.add_argument(
        "--date-filter-field",
        choices=("server_modified", "client_modified", "oldest_modified"),
        default=None,
        help="Timestamp field used for cutoff filtering. Default: server_modified.",
    )
    parser.add_argument("--archive-root", default=None, help="Archive root folder in Dropbox.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Base output directory.")
    parser.add_argument("--job-state", type=Path, help="Reserved for resuming an existing run.")
    parser.add_argument("--batch-size", type=int, default=None, help="Dropbox page and work batch size.")
    parser.add_argument("--retry-count", type=int, default=None, help="Maximum retry attempts.")
    parser.add_argument("--initial-backoff", type=float, default=None, help="Initial retry backoff in seconds.")
    parser.add_argument("--backoff-multiplier", type=float, default=None, help="Retry backoff multiplier.")
    parser.add_argument("--max-backoff", type=float, default=None, help="Maximum backoff in seconds.")
    parser.add_argument(
        "--conflict-policy",
        choices=("safe_skip", "abort_run"),
        default=None,
        help="Conflict policy when the destination already exists.",
    )
    parser.add_argument("--exclude-folders-from-inventory", action="store_true", help="Do not include folders in inventory CSV.")
    parser.add_argument(
        "--include-archive-destination",
        action="store_true",
        help="Do not exclude the archive folder subtree from source traversal in personal mode.",
    )
    parser.add_argument("--worker-count", type=int, default=None, help="Requested worker count for copy phase.")
    parser.add_argument("--skip-verify", action="store_true", help="Skip the verification phase.")
    parser.add_argument(
        "--team-coverage-preset",
        choices=("all_team_content", "team_owned_only"),
        default=None,
        help="Coverage preset for team-admin mode.",
    )
    parser.add_argument(
        "--team-archive-layout",
        choices=("segmented", "merged"),
        default=None,
        help="Team archive folder layout. segmented keeps team/member buckets separate; merged uses one archive tree.",
    )


def handle_oauth_link(args: argparse.Namespace) -> int:
    auth_manager = AuthManager()
    scopes = tuple(args.scopes) if args.scopes else default_scopes_for_mode(args.account_mode)
    authorize_url = auth_manager.start_pkce_flow(
        args.app_key,
        scopes,
        account_mode=args.account_mode,
        label="default",
    )
    print("Open this URL in your browser and approve the app:\n")
    print(authorize_url)
    print()
    auth_code = input("Enter the Dropbox authorization code: ").strip()
    credentials = auth_manager.finish_pkce_flow(auth_code)
    auth_manager.save_credentials("default", credentials)
    print("Saved credentials securely where available.")
    return 0


def handle_connect_test(args: argparse.Namespace) -> int:
    config_data = load_config(args.config)
    logger = get_cli_logger()
    auth_config = resolve_auth_config(args, config_data)
    account = AuthManager().test_connection(auth_config, logger)
    if account.account_mode == "team_admin":
        print(
            f"Connected as team admin {account.display_name} ({account.email or 'no email returned'}) "
            f"for team {account.team_name or 'unknown team'} [{account.team_model or 'unknown model'}]."
        )
    else:
        print(f"Connected as {account.display_name} ({account.email or 'no email returned'}).")
    return 0


def handle_run_command(args: argparse.Namespace) -> int:
    config_data = load_config(args.config)
    auth_config = resolve_auth_config(args, config_data)
    job_config = resolve_job_config(args, config_data, args.workflow_mode)
    result = RunOrchestrator().run(job_config=job_config, auth_config=auth_config)
    print_run_result(result)
    return 0


def handle_resume_command(args: argparse.Namespace) -> int:
    config_data = load_config(args.config)
    auth_config = resolve_auth_config(args, config_data)
    state_db = resolve_state_db_path(args.job_state, args.output_dir)
    result = RunOrchestrator().resume(state_db_path=state_db, auth_config=auth_config)
    print_run_result(result)
    return 0


def handle_verify_command(args: argparse.Namespace) -> int:
    config_data = load_config(args.config)
    auth_config = resolve_auth_config(args, config_data)
    state_db = resolve_state_db_path(args.job_state, args.output_dir)
    result = RunOrchestrator().verify_only(state_db_path=state_db, auth_config=auth_config)
    print_run_result(result)
    return 0


def resolve_auth_config(args: argparse.Namespace, config_data: dict[str, Any]) -> AuthConfig:
    auth_manager = AuthManager()
    auth_section = config_data.get("auth", {})
    store_label = getattr(args, "store_label", "default")
    account_mode = args.account_mode or auth_section.get("account_mode") or "personal"

    refresh_token = args.refresh_token or auth_section.get("refresh_token")
    access_token = args.access_token or auth_section.get("access_token")
    app_key = args.app_key or auth_section.get("app_key")
    admin_member_id = args.admin_member_id or auth_section.get("admin_member_id")

    if refresh_token:
        return AuthConfig(
            method="refresh_token",
            account_mode=account_mode,
            app_key=app_key,
            refresh_token=refresh_token,
            scopes=tuple(auth_section.get("scopes") or default_scopes_for_mode(account_mode)),
            store_label=store_label,
            admin_member_id=admin_member_id,
        )
    if access_token:
        return AuthConfig(
            method="access_token",
            account_mode=account_mode,
            access_token=access_token,
            store_label=store_label,
            admin_member_id=admin_member_id,
        )

    if args.use_saved_auth or not any((refresh_token, access_token)):
        saved = auth_manager.load_credentials(store_label)
        if saved is None:
            raise ValueError("No saved Dropbox credentials were found. Use oauth-link or supply a token.")
        auth_config = auth_manager.credentials_to_auth_config(saved)
        if args.account_mode:
            auth_config.account_mode = args.account_mode
        if admin_member_id:
            auth_config.admin_member_id = admin_member_id
        return auth_config

    raise ValueError("Unable to resolve Dropbox authentication settings.")


def resolve_job_config(args: argparse.Namespace, config_data: dict[str, Any], mode: str) -> JobConfig:
    job_section = config_data.get("job", {})
    source_roots = args.source_roots or job_section.get("source_roots") or ["/"]
    excluded_roots = args.excluded_roots or job_section.get("excluded_roots") or []
    include_folders_in_inventory = bool(job_section.get("include_folders_in_inventory", True))
    if getattr(args, "exclude_folders_from_inventory", False):
        include_folders_in_inventory = False
    exclude_archive_destination = bool(job_section.get("exclude_archive_destination", True))
    if getattr(args, "include_archive_destination", False):
        exclude_archive_destination = False
    retry = RetrySettings(
        max_retries=args.retry_count if getattr(args, "retry_count", None) is not None else job_section.get("retry_count", 5),
        initial_backoff_seconds=(
            args.initial_backoff if getattr(args, "initial_backoff", None) is not None else job_section.get("initial_backoff", 1.0)
        ),
        backoff_multiplier=(
            args.backoff_multiplier
            if getattr(args, "backoff_multiplier", None) is not None
            else job_section.get("backoff_multiplier", 2.0)
        ),
        max_backoff_seconds=args.max_backoff if getattr(args, "max_backoff", None) is not None else job_section.get("max_backoff", 30.0),
    )
    return JobConfig(
        source_roots=list(source_roots),
        excluded_roots=list(excluded_roots),
        cutoff_date=getattr(args, "cutoff_date", None) or job_section.get("cutoff_date", "2020-05-01"),
        date_filter_field=getattr(args, "date_filter_field", None) or job_section.get("date_filter_field", "server_modified"),
        archive_root=getattr(args, "archive_root", None) or job_section.get("archive_root", "/Archive_PreMay2020"),
        output_dir=getattr(args, "output_dir", None) or Path(job_section.get("output_dir", "outputs")),
        mode=mode,  # type: ignore[arg-type]
        batch_size=getattr(args, "batch_size", None) or job_section.get("batch_size", 500),
        retry=retry,
        conflict_policy=getattr(args, "conflict_policy", None) or job_section.get("conflict_policy", "safe_skip"),
        include_folders_in_inventory=include_folders_in_inventory,
        exclude_archive_destination=exclude_archive_destination,
        worker_count=getattr(args, "worker_count", None) or job_section.get("worker_count", 1),
        verify_after_run=not getattr(args, "skip_verify", False),
        team_coverage_preset=getattr(args, "team_coverage_preset", None) or job_section.get("team_coverage_preset", "team_owned_only"),
        team_archive_layout=getattr(args, "team_archive_layout", None) or job_section.get("team_archive_layout", "segmented"),
    )


def resolve_state_db_path(explicit_path: Path | None, output_dir: Path) -> Path:
    if explicit_path is not None:
        return explicit_path
    latest_pointer = output_dir / "latest_run.json"
    if not latest_pointer.exists():
        raise ValueError(f"Could not find {latest_pointer}. Supply --job-state explicitly.")
    payload = json.loads(latest_pointer.read_text(encoding="utf-8"))
    return Path(payload["state_db"])


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return load_yaml_file(path)


def get_cli_logger() -> logging.Logger:
    logger = logging.getLogger("dropbox_cleaner.cli")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


def print_run_result(result) -> None:
    print(f"Run ID: {result.run_id}")
    print(f"Run directory: {result.run_dir}")
    print(f"Summary: {result.summary_path}")
    if result.verification_path:
        print(f"Verification: {result.verification_path}")


if __name__ == "__main__":
    raise SystemExit(main())
