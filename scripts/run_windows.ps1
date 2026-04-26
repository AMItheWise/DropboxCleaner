param(
    [int]$Port = 0,
    [switch]$NoBrowser,
    [switch]$SetupOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir = Join-Path $RepoRoot "launcher_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$LogFile = Join-Path $LogDir "dropbox-cleaner-launch-$Timestamp.log"

function Write-Log {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Level = "INFO",
        [string]$Color = ""
    )
    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    if ($Color) {
        Write-Host $line -ForegroundColor $Color
    } elseif ($Level -eq "ERROR") {
        Write-Host $line -ForegroundColor Red
    } elseif ($Level -eq "WARN") {
        Write-Host $line -ForegroundColor Yellow
    } else {
        Write-Host $line
    }
    Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8
}

function Stop-WithMessage {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Log $Message "ERROR"
    Write-Log "Full log: $LogFile" "ERROR"
    exit 1
}

function Invoke-Capture {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$Arguments = @()
    )
    try {
        $output = & $FilePath @Arguments 2>$null
        if ($LASTEXITCODE -ne 0) {
            return $null
        }
        return (($output | Out-String).Trim())
    } catch {
        return $null
    }
}

function Invoke-Logged {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$Arguments = @(),
        [Parameter(Mandatory = $true)][string]$FailureMessage
    )
    Write-Log ("> {0} {1}" -f $FilePath, ($Arguments -join " "))
    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $FilePath @Arguments 2>&1 | ForEach-Object {
            Write-Log ($_.ToString())
        }
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    if ($exitCode -ne 0) {
        throw "$FailureMessage (exit code $exitCode)"
    }
}

function Get-PythonVersion {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$PrefixArguments = @()
    )
    $code = "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
    $args = @()
    $args += $PrefixArguments
    $args += @("-c", $code)
    $version = Invoke-Capture -FilePath $FilePath -Arguments $args
    if (-not $version) {
        return $null
    }
    if ($version -notmatch "^\d+\.\d+\.\d+") {
        return $null
    }
    return $version.Trim()
}

function Test-MinimumPython {
    param([Parameter(Mandatory = $true)][string]$Version)
    $parts = $Version.Split(".")
    $major = [int]$parts[0]
    $minor = [int]$parts[1]
    return ($major -gt 3 -or ($major -eq 3 -and $minor -ge 11))
}

function Find-BasePython {
    $candidates = @(
        @{ Label = "Python Launcher 3.11"; File = "py"; Args = @("-3.11") },
        @{ Label = "python"; File = "python"; Args = @() },
        @{ Label = "python3"; File = "python3"; Args = @() }
    )
    foreach ($candidate in $candidates) {
        $version = Get-PythonVersion -FilePath $candidate.File -PrefixArguments $candidate.Args
        if ($version -and (Test-MinimumPython -Version $version)) {
            Write-Log ("Found {0}: Python {1}" -f $candidate.Label, $version)
            return @{ File = $candidate.File; Args = $candidate.Args; Version = $version; Label = $candidate.Label }
        }
        if ($version) {
            Write-Log ("Ignoring {0}: Python {1} is too old. Python 3.11+ is required." -f $candidate.Label, $version) "WARN"
        }
    }
    return $null
}

function Test-Imports {
    param([Parameter(Mandatory = $true)][string]$Python)
    $code = "import dropbox, fastapi, keyring, platformdirs, pydantic, yaml, uvicorn"
    $result = Invoke-Capture -FilePath $Python -Arguments @("-c", $code)
    return ($null -ne $result)
}

function Get-RequirementsHash {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

Write-Log "Dropbox Cleaner launcher started." -Color Cyan
Write-Log "Scanning system"
Write-Log "Project folder: $RepoRoot"
Write-Log "Log file: $LogFile"

$RequirementsPath = Join-Path $RepoRoot "requirements.txt"
if (-not (Test-Path -LiteralPath $RequirementsPath)) {
    Stop-WithMessage "Could not find requirements.txt. Run this from a complete Dropbox Cleaner folder."
}

Write-Log "Checking Python"
$VenvDir = Join-Path $RepoRoot ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvReady = $false
if (Test-Path -LiteralPath $VenvPython) {
    $venvVersion = Get-PythonVersion -FilePath $VenvPython
    if ($venvVersion -and (Test-MinimumPython -Version $venvVersion)) {
        Write-Log "Found local virtual environment: Python $venvVersion"
        $VenvReady = $true
    } else {
        Write-Log "Local virtual environment is missing or too old. It will be repaired." "WARN"
    }
}

if (-not $VenvReady) {
    $BasePython = Find-BasePython
    if (-not $BasePython) {
        Stop-WithMessage "Python 3.11 or newer was not found. Install Python from https://www.python.org/downloads/windows/ and run this launcher again."
    }
    Write-Log "Creating local virtual environment" -Color Yellow
    $venvArgs = @()
    $venvArgs += $BasePython.Args
    $venvArgs += @("-m", "venv", $VenvDir)
    try {
        Invoke-Logged -FilePath $BasePython.File -Arguments $venvArgs -FailureMessage "Could not create the local virtual environment."
    } catch {
        Stop-WithMessage $_.Exception.Message
    }
    $venvVersion = Get-PythonVersion -FilePath $VenvPython
    if (-not $venvVersion -or -not (Test-MinimumPython -Version $venvVersion)) {
        Stop-WithMessage "The local virtual environment was created, but its Python version is invalid."
    }
    Write-Log "Local virtual environment ready: Python $venvVersion"
}

$MarkerPath = Join-Path $VenvDir ".dropbox-cleaner-requirements.sha256"
$CurrentHash = Get-RequirementsHash -Path $RequirementsPath
$PreviousHash = if (Test-Path -LiteralPath $MarkerPath) { (Get-Content -LiteralPath $MarkerPath -Raw).Trim() } else { "" }
$ImportsOk = Test-Imports -Python $VenvPython

if ($PreviousHash -eq $CurrentHash -and $ImportsOk) {
    Write-Log "Requirements unchanged. Skipping dependency install."
} else {
    Write-Log "Installing Dropbox Cleaner requirements" -Color Yellow
    try {
        Invoke-Logged -FilePath $VenvPython -Arguments @("-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel") -FailureMessage "Could not upgrade pip tooling."
        Invoke-Logged -FilePath $VenvPython -Arguments @("-m", "pip", "install", "-r", $RequirementsPath) -FailureMessage "Could not install Dropbox Cleaner requirements."
        Set-Content -LiteralPath $MarkerPath -Value $CurrentHash -Encoding ASCII
    } catch {
        Stop-WithMessage ("Dependency installation failed. Check your internet connection and try again. {0}" -f $_.Exception.Message)
    }
}

Write-Log "Checking browser UI files"
$StaticDir = Join-Path $RepoRoot "app\web\static"
$IndexPath = Join-Path $StaticDir "index.html"
$AssetsDir = Join-Path $StaticDir "assets"
$JsAssets = @(Get-ChildItem -LiteralPath $AssetsDir -Filter "*.js" -ErrorAction SilentlyContinue)
$CssAssets = @(Get-ChildItem -LiteralPath $AssetsDir -Filter "*.css" -ErrorAction SilentlyContinue)
if (-not (Test-Path -LiteralPath $IndexPath) -or $JsAssets.Count -lt 1 -or $CssAssets.Count -lt 1) {
    Stop-WithMessage "The browser UI files are missing. This release is incomplete because app\web\static does not contain index.html plus JS/CSS assets."
}
Write-Log "Browser UI files are present."

if ($SetupOnly) {
    Write-Log "Setup check completed. SetupOnly was specified, so the app was not started." -Color Green
    exit 0
}

Write-Log "Starting Dropbox Cleaner" -Color Green
Write-Log "To stop: press Ctrl+C in this window." -Color Yellow
$LaunchArgs = @("-u", "-m", "app.web.main")
if ($Port -gt 0) {
    $LaunchArgs += @("--port", "$Port")
}
if ($NoBrowser) {
    $LaunchArgs += "--no-browser"
}

Push-Location $RepoRoot
try {
    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $VenvPython @LaunchArgs 2>&1 | ForEach-Object {
            $line = $_.ToString()
            if ($line -match "Dropbox Cleaner web UI:\s+(http://\S+)") {
                Write-Log ("Browser UI URL: {0}" -f $Matches[1]) -Color Green
            } else {
                Write-Log $line
            }
        }
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    if ($exitCode -ne 0) {
        if ($exitCode -eq 130 -or $exitCode -eq -1073741510) {
            Write-Log "Dropbox Cleaner stopped." -Color Green
            exit 0
        }
        Stop-WithMessage "Dropbox Cleaner stopped unexpectedly with exit code $exitCode."
    }
} finally {
    Pop-Location
}

Write-Log "Dropbox Cleaner stopped." -Color Green
