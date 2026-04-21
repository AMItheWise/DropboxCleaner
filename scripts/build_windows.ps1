param(
    [string]$DropboxAppKey = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

py -3.11 -m pip install --upgrade pip
py -3.11 -m pip install -r requirements.txt
py -3.11 -m pip install -r requirements-dev.txt

py -3.11 -m PyInstaller packaging/DropboxCleaner-windows.spec --noconfirm --clean

if ($DropboxAppKey) {
    $keyPath = Join-Path $repoRoot "dist\DropboxCleaner\dropbox_app_key.txt"
    Set-Content -Path $keyPath -Value $DropboxAppKey -Encoding UTF8
    Write-Host "Wrote Dropbox app key to $keyPath"
}

$exePath = Join-Path $repoRoot "dist\DropboxCleaner\DropboxCleaner.exe"
if (-not (Test-Path $exePath)) {
    throw "Expected Windows executable was not created at $exePath"
}

Write-Host "Windows app built at $exePath"
