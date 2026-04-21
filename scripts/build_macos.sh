#!/usr/bin/env bash
set -euo pipefail

DROPBOX_APP_KEY="${1:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

python3.11 -m pip install --upgrade pip
python3.11 -m pip install -r requirements.txt
python3.11 -m pip install -r requirements-dev.txt

python3.11 -m PyInstaller packaging/DropboxCleaner-macos.spec --noconfirm --clean

if [[ -n "$DROPBOX_APP_KEY" ]]; then
  KEY_PATH="dist/Dropbox Cleaner.app/Contents/Resources/dropbox_app_key.txt"
  printf "%s" "$DROPBOX_APP_KEY" > "$KEY_PATH"
  echo "Wrote Dropbox app key to $KEY_PATH"
fi

if [[ ! -d "dist/Dropbox Cleaner.app" ]]; then
  echo "Expected macOS app bundle was not created at dist/Dropbox Cleaner.app" >&2
  exit 1
fi

echo "macOS app built at dist/Dropbox Cleaner.app"
