@echo off
setlocal
cd /d "%~dp0"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run_windows.ps1" %*
set "EXITCODE=%ERRORLEVEL%"

if not "%EXITCODE%"=="0" (
    echo.
    echo Dropbox Cleaner stopped with an error. Press any key to close this window.
    pause >nul
)

exit /b %EXITCODE%
