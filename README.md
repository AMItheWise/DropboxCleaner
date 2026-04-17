# Dropbox Cleaner

Local-first desktop utility for inventorying a personal Dropbox account, identifying files older than a cutoff date, and staging archive copies into a dedicated Dropbox archive folder without touching the originals.

Dropbox Cleaner is built for cautious archival work. It inventories first, plans first, writes clear manifests, and only performs server-side copy operations when you explicitly choose a real copy run.

## Highlights

- Local desktop app with a simple Tkinter interface
- Shared Python backend used by both GUI and CLI
- Full Dropbox metadata traversal with pagination support
- Cutoff-based matching using `server_modified`
- Dry-run mode with planned manifest output
- Safe copy-first archive staging inside Dropbox
- SQLite-backed resumability and checkpoints
- Verification and audit artifacts after each run

## Preview

- Presentation deck: [docs/slides/DropboxCleaner_Open_Source_Overview.pptx](docs/slides/DropboxCleaner_Open_Source_Overview.pptx)
- Rendered slide previews: [docs/slides/rendered](docs/slides/rendered)

![Dropbox Cleaner slide preview](docs/slides/rendered/montage.png)

## What It Does

- Connects to Dropbox through the official Dropbox Python SDK
- Enumerates files and folders under the full account root or selected source roots
- Exports a full inventory CSV
- Identifies files older than a user-selected cutoff date
- Exports a matched-file CSV with planned archive destinations
- Creates or reuses a dedicated archive folder such as `/Archive_PreMay2020`
- Preserves original folder structure under the archive root
- Writes logs, manifests, summaries, and verification reports
- Supports safe resume after interruption

## What It Does Not Do

- It does not delete originals
- It does not move originals
- It does not ask for your Dropbox password
- It does not silently overwrite archive files
- It does not rely on Dropbox search

## Quick Start

### Requirements

- Python 3.11+
- A personal Dropbox account
- A Dropbox API app with these scopes:
  - `account_info.read`
  - `files.metadata.read`
  - `files.content.read`
  - `files.content.write`

### Install

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -3.11 -m pip install -r requirements.txt
py -3.11 -m pip install -r requirements-dev.txt
```

### Run The GUI

```powershell
py -3.11 -m app
```

### Run The CLI

```powershell
py -3.11 -m app.cli.main --help
```

## Authentication

Recommended flow:

1. Create a scoped Dropbox app in the Dropbox App Console.
2. Use OAuth PKCE in the GUI or `dropbox-cleaner oauth-link`.
3. Save the resulting refresh token locally through the app.
4. Re-authorize if you later add or change scopes.

The app never asks for your Dropbox password and does not log tokens.

## Common Workflows

### Inventory Only

```powershell
dropbox-cleaner inventory ^
  --use-saved-auth ^
  --source-root / ^
  --output-dir ./outputs
```

### Dry Run

```powershell
dropbox-cleaner dry-run ^
  --use-saved-auth ^
  --source-root /Team ^
  --cutoff-date 2020-05-01 ^
  --archive-root /Archive_PreMay2020 ^
  --output-dir ./outputs
```

### Copy Run

```powershell
dropbox-cleaner copy ^
  --use-saved-auth ^
  --source-root /Team ^
  --cutoff-date 2020-05-01 ^
  --archive-root /Archive_PreMay2020 ^
  --output-dir ./outputs
```

### Resume

```powershell
dropbox-cleaner resume ^
  --use-saved-auth ^
  --job-state ./outputs/your-run-folder/state.db
```

### Verify

```powershell
dropbox-cleaner verify ^
  --use-saved-auth ^
  --job-state ./outputs/your-run-folder/state.db
```

## Outputs

Every run creates a timestamped output folder with:

- `inventory_full.csv`
- `matched_pre_cutoff.csv`
- `manifest_dry_run.csv` or `manifest_copy_run.csv`
- `summary.json`
- `summary.md`
- `verification_report.csv`
- `verification_report.json`
- `app.log`
- `app.jsonl`
- `state.db`

## Project Structure

```text
app/
  cli/
  dropbox_client/
  models/
  persistence/
  reports/
  services/
  ui/
  utils/
tests/
docs/slides/
```

## Development

Run the quality checks used by CI:

```powershell
py -3.11 -m pytest -q
py -3.11 -m compileall app tests
py -3.11 -m build
```

## Community

- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security policy: [SECURITY.md](SECURITY.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)

## License

[MIT](LICENSE)
