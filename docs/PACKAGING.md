# Packaging Dropbox Cleaner

Dropbox Cleaner's supported v1 browser migration is launched as a local web server that opens the user's browser.

Recommended launch commands:

Windows:

```powershell
.\scripts\start_web.ps1
```

macOS/Linux:

```bash
bash ./scripts/start_web.sh
```

The server binds to `127.0.0.1` by default and serves the built React bundle from `app/web/static`.

## Legacy PySide6 Builds

The older PySide6 desktop app can still be packaged with PyInstaller for transition testing, but it is no longer the preferred macOS distribution path. Public macOS distribution of a native `.app` still requires Developer ID signing and notarization.

## Windows

Build on Windows:

```powershell
.\scripts\build_windows.ps1
```

To bundle a Dropbox app key without committing it to the repo:

```powershell
.\scripts\build_windows.ps1 -DropboxAppKey "your-app-key"
```

Output:

```text
dist\DropboxCleaner\DropboxCleaner.exe
```

## macOS

Build on macOS:

```bash
./scripts/build_macos.sh
```

To bundle a Dropbox app key without committing it to the repo:

```bash
./scripts/build_macos.sh "your-app-key"
```

Output:

```text
dist/Dropbox Cleaner.app
```

## Notes

- Build macOS apps on macOS and Windows apps on Windows.
- The build scripts install runtime and packaging dependencies from `requirements.txt` and `requirements-dev.txt`.
- If no bundled app key is provided, the GUI asks for a Dropbox app key on the connection screen.
- GitHub Actions builds unsigned zipped artifacts for pull requests, pushes to `main`/`master`, and manual workflow runs: `DropboxCleaner-Windows.zip` and `DropboxCleaner-macOS.zip`.
- For public distribution on macOS, sign and notarize the `.app` after building.
- For public distribution on Windows, sign the executable or installer after building.
