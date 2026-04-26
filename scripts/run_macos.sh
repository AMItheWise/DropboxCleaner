#!/usr/bin/env bash
set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$REPO_ROOT/launcher_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP="$(date +"%Y%m%d-%H%M%S")"
LOG_FILE="$LOG_DIR/dropbox-cleaner-launch-$TIMESTAMP.log"

COLOR_RESET=""
COLOR_RED=""
COLOR_GREEN=""
COLOR_YELLOW=""
COLOR_CYAN=""
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  COLOR_RESET=$'\033[0m'
  COLOR_RED=$'\033[31m'
  COLOR_GREEN=$'\033[32m'
  COLOR_YELLOW=$'\033[33m'
  COLOR_CYAN=$'\033[36m'
fi

PORT=""
NO_BROWSER=0
SETUP_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      if [[ $# -lt 2 || "$2" == -* ]]; then
        echo "Missing value for --port"
        echo "Supported options: --port PORT, --no-browser, --setup-only"
        exit 2
      fi
      PORT="${2:-}"
      shift 2
      ;;
    --no-browser)
      NO_BROWSER=1
      shift
      ;;
    --setup-only)
      SETUP_ONLY=1
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Supported options: --port PORT, --no-browser, --setup-only"
      exit 2
      ;;
  esac
done

log() {
  local level="${2:-INFO}"
  local color_name="${3:-}"
  local line
  local color=""
  line="[$(date +"%Y-%m-%d %H:%M:%S")] [$level] $1"
  case "$color_name" in
    green) color="$COLOR_GREEN" ;;
    yellow) color="$COLOR_YELLOW" ;;
    cyan) color="$COLOR_CYAN" ;;
    red) color="$COLOR_RED" ;;
  esac
  if [[ -z "$color" ]]; then
    case "$level" in
      ERROR) color="$COLOR_RED" ;;
      WARN) color="$COLOR_YELLOW" ;;
    esac
  fi
  if [[ -n "$color" ]]; then
    printf '%s%s%s\n' "$color" "$line" "$COLOR_RESET"
  else
    printf '%s\n' "$line"
  fi
  printf '%s\n' "$line" >> "$LOG_FILE"
}

fail() {
  log "$1" "ERROR"
  log "Full log: $LOG_FILE" "ERROR"
  exit 1
}

run_logged() {
  local failure_message="$1"
  shift
  log "> $*"
  "$@" 2>&1 | while IFS= read -r line; do
    log "$line"
  done
  local status=${PIPESTATUS[0]}
  if [[ $status -ne 0 ]]; then
    fail "$failure_message (exit code $status)"
  fi
}

capture() {
  "$@" 2>/dev/null
}

python_version() {
  local python="$1"
  capture "$python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")'
}

python_ok() {
  local python="$1"
  local version="$2"
  capture "$python" - "$version" <<'PY'
import sys
major, minor, *_ = [int(part) for part in sys.argv[1].split(".")]
raise SystemExit(0 if (major, minor) >= (3, 11) else 1)
PY
}

find_base_python() {
  local candidates=("python3.11" "python3" "python")
  local candidate version
  BASE_PYTHON=""
  for candidate in "${candidates[@]}"; do
    if ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi
    version="$(python_version "$candidate" || true)"
    if [[ -n "$version" ]] && python_ok "$candidate" "$version"; then
      log "Found $candidate: Python $version"
      BASE_PYTHON="$candidate"
      return 0
    fi
    if [[ -n "$version" ]]; then
      log "Ignoring $candidate: Python $version is too old. Python 3.11+ is required." "WARN"
    fi
  done
  return 1
}

requirements_hash() {
  "$1" - "$REQUIREMENTS_PATH" <<'PY'
import hashlib
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
}

imports_ok() {
  "$1" - <<'PY' >/dev/null 2>&1
import dropbox, fastapi, keyring, platformdirs, pydantic, yaml, uvicorn
PY
}

log "Dropbox Cleaner launcher started." "INFO" "cyan"
log "Scanning system"
log "Project folder: $REPO_ROOT"
log "Log file: $LOG_FILE"
log "Machine: $(uname -s) $(uname -m)"
if [[ "$(uname -m)" == "arm64" ]]; then
  log "Detected Apple Silicon Mac."
elif [[ "$(uname -s)" == "Darwin" ]]; then
  log "Detected Intel Mac."
fi

REQUIREMENTS_PATH="$REPO_ROOT/requirements.txt"
if [[ ! -f "$REQUIREMENTS_PATH" ]]; then
  fail "Could not find requirements.txt. Run this from a complete Dropbox Cleaner folder."
fi

log "Checking Python"
VENV_DIR="$REPO_ROOT/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"
VENV_READY=0
if [[ -x "$VENV_PYTHON" ]]; then
  VENV_VERSION="$(python_version "$VENV_PYTHON" || true)"
  if [[ -n "$VENV_VERSION" ]] && python_ok "$VENV_PYTHON" "$VENV_VERSION"; then
    log "Found local virtual environment: Python $VENV_VERSION"
    VENV_READY=1
  else
    log "Local virtual environment is missing or too old. It will be repaired." "WARN"
  fi
fi

if [[ "$VENV_READY" -ne 1 ]]; then
  if ! find_base_python || [[ -z "$BASE_PYTHON" ]]; then
    log "Python 3.11 or newer was not found." "ERROR"
    if [[ "$(uname -s)" == "Darwin" ]] && command -v open >/dev/null 2>&1; then
      log "Opening the official Python for macOS download page."
      open "https://www.python.org/downloads/macos/" >/dev/null 2>&1 || true
    fi
    fail "Install Python 3.11 or newer from https://www.python.org/downloads/macos/ and run this launcher again."
  fi
  log "Creating local virtual environment" "INFO" "yellow"
  run_logged "Could not create the local virtual environment." "$BASE_PYTHON" -m venv "$VENV_DIR"
  VENV_VERSION="$(python_version "$VENV_PYTHON" || true)"
  if [[ -z "$VENV_VERSION" ]] || ! python_ok "$VENV_PYTHON" "$VENV_VERSION"; then
    fail "The local virtual environment was created, but its Python version is invalid."
  fi
  log "Local virtual environment ready: Python $VENV_VERSION"
fi

MARKER_PATH="$VENV_DIR/.dropbox-cleaner-requirements.sha256"
CURRENT_HASH="$(requirements_hash "$VENV_PYTHON")"
PREVIOUS_HASH=""
if [[ -f "$MARKER_PATH" ]]; then
  PREVIOUS_HASH="$(tr -d '[:space:]' < "$MARKER_PATH")"
fi

if [[ "$PREVIOUS_HASH" == "$CURRENT_HASH" ]] && imports_ok "$VENV_PYTHON"; then
  log "Requirements unchanged. Skipping dependency install."
else
  log "Installing Dropbox Cleaner requirements" "INFO" "yellow"
  run_logged "Could not upgrade pip tooling." "$VENV_PYTHON" -m pip install --upgrade pip setuptools wheel
  run_logged "Could not install Dropbox Cleaner requirements. Check your internet connection and try again." "$VENV_PYTHON" -m pip install -r "$REQUIREMENTS_PATH"
  printf '%s\n' "$CURRENT_HASH" > "$MARKER_PATH"
fi

log "Checking browser UI files"
STATIC_DIR="$REPO_ROOT/app/web/static"
if [[ ! -f "$STATIC_DIR/index.html" ]] || ! compgen -G "$STATIC_DIR/assets/*.js" >/dev/null || ! compgen -G "$STATIC_DIR/assets/*.css" >/dev/null; then
  fail "The browser UI files are missing. This release is incomplete because app/web/static does not contain index.html plus JS/CSS assets."
fi
log "Browser UI files are present."

if [[ "$SETUP_ONLY" -eq 1 ]]; then
  log "Setup check completed. SetupOnly was specified, so the app was not started." "INFO" "green"
  exit 0
fi

log "Starting Dropbox Cleaner" "INFO" "green"
log "To stop: press Ctrl+C in this window." "INFO" "yellow"

LAUNCH_ARGS=(-u -m app.web.main)
if [[ -n "$PORT" ]]; then
  LAUNCH_ARGS+=(--port "$PORT")
fi
if [[ "$NO_BROWSER" -eq 1 ]]; then
  LAUNCH_ARGS+=(--no-browser)
fi

cd "$REPO_ROOT" || fail "Could not enter project folder."
"$VENV_PYTHON" "${LAUNCH_ARGS[@]}" 2>&1 | while IFS= read -r line; do
  if [[ "$line" =~ Dropbox\ Cleaner\ web\ UI:\ (http://[^[:space:]]+) ]]; then
    log "Browser UI URL: ${BASH_REMATCH[1]}" "INFO" "green"
  else
    log "$line"
  fi
done
status=${PIPESTATUS[0]}
if [[ $status -ne 0 ]]; then
  if [[ $status -eq 130 || $status -eq 143 ]]; then
    log "Dropbox Cleaner stopped." "INFO" "green"
    exit 0
  fi
  fail "Dropbox Cleaner stopped unexpectedly with exit code $status."
fi

log "Dropbox Cleaner stopped." "INFO" "green"
