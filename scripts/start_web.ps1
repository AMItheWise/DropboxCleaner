param(
    [int]$Port = 0,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = "py"
    $Args = @("-3.11", "-m", "app.web.main")
} else {
    $Args = @("-m", "app.web.main")
}

if ($Port -gt 0) {
    $Args += @("--port", "$Port")
}
if ($NoBrowser) {
    $Args += "--no-browser"
}

Push-Location $RepoRoot
try {
    & $Python @Args
} finally {
    Pop-Location
}
