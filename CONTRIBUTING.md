# Contributing

Thanks for your interest in improving Dropbox Cleaner.

## Ground Rules

- Keep the project local-first and safety-first.
- Do not add delete or move workflows for Dropbox originals in this repository's mainline without a separate design review.
- Avoid logging secrets, tokens, or personal Dropbox data.
- Preserve resumability and auditability when changing the workflow.

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

On Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -3.11 -m pip install -r requirements.txt
py -3.11 -m pip install -r requirements-dev.txt
```

## Run The App

GUI:

```bash
python -m app
```

CLI:

```bash
dropbox-cleaner --help
```

## Tests

```bash
pytest -q
python -m compileall app tests
python -m build
```

## Pull Requests

- Keep changes focused and reviewable.
- Add or update tests for behavior changes.
- Update the README or docs when user-visible behavior changes.
- Use clear commit messages and include a concise PR summary.

## Areas That Usually Need Extra Care

- Dropbox API scope handling
- Retry behavior and resumability
- Manifest and verification outputs
- Conflict policy behavior
- Cross-platform Tkinter behavior
