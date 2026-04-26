#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1

bash "./scripts/run_macos.sh" "$@"
status=$?

if [[ $status -ne 0 ]]; then
  echo
  echo "Dropbox Cleaner stopped with an error. Press Return to close this window."
  read -r _
fi

exit "$status"
