# scripts/install/install.ps1 — standalone installer for devstudio-proxy on Windows
# Downloads the latest release binary from GitHub and installs it to
# %LOCALAPPDATA%\devstudio-proxy\ (or a user-specified directory).
#
# Usage (PowerShell):
#   irm https://raw.githubusercontent.com/devstudio-live/devstudio-proxy/main/scripts/install/install.ps1 | iex
#
#   # Or with options:
#   & .\install.ps1 -InstallDir "C:\tools\devproxy" -Version "v0.70.0"
#
# Options:
#   -InstallDir   Install directory (default: $env:LOCALAPPDATA\devstudio-proxy)
#   -Version      Install a specific version (default: latest)
#   -NoVerify     Skip SHA256 checksum verification
#   -AddToPath    Add install directory to user PATH (default: true)

[CmdletBinding()]
param(
    [string]$InstallDir = "",
    [string]$Version = "",
    [switch]$NoVerify,
    [bool]$AddToPath = $true
)

$ErrorActionPreference = "Stop"

$Repo = "devstudio-live/devstudio-proxy"
$BinaryName = "devproxy.exe"

if (-not $InstallDir) {
    $InstallDir = Join-Path $env:LOCALAPPDATA "devstudio-proxy"
}

# ── Helpers ──────────────────────────────────────────────────────────────

function Write-Info  { param([string]$Msg) Write-Host "==> " -NoNewline -ForegroundColor Blue;  Write-Host $Msg }
function Write-Ok    { param([string]$Msg) Write-Host "==> " -NoNewline -ForegroundColor Green; Write-Host $Msg }
function Write-Warn  { param([string]$Msg) Write-Host "warning: " -NoNewline -ForegroundColor Yellow; Write-Host $Msg }
function Exit-Error  { param([string]$Msg) Write-Host "error: " -NoNewline -ForegroundColor Red; Write-Host $Msg; exit 1 }

# ── Detect architecture ─────────────────────────────────────────────────

function Get-Arch {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    switch ($arch) {
        "X64"   { return "amd64" }
        "Arm64" { return "arm64" }
        default { Exit-Error "Unsupported architecture: $arch" }
    }
}

$Arch = Get-Arch
$AssetName = "devproxy-windows-${Arch}.exe"

Write-Info "Detected platform: windows/$Arch"

# ── Resolve version ─────────────────────────────────────────────────────

if (-not $Version) {
    Write-Info "Fetching latest release version..."
    try {
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -UseBasicParsing
        $Version = $release.tag_name
    } catch {
        Exit-Error "Failed to fetch latest release: $_"
    }
    if (-not $Version) { Exit-Error "Could not determine latest version" }
}

Write-Info "Installing devstudio-proxy $Version"

# ── Download ─────────────────────────────────────────────────────────────

$DownloadUrl = "https://github.com/$Repo/releases/download/$Version/$AssetName"
$ChecksumsUrl = "https://github.com/$Repo/releases/download/$Version/checksums.txt"

$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "devstudio-proxy-install-$(Get-Random)"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

try {
    $TmpBinary = Join-Path $TmpDir $AssetName

    Write-Info "Downloading $AssetName..."
    try {
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $TmpBinary -UseBasicParsing
    } catch {
        Exit-Error "Download failed. Check that $Version exists and includes $AssetName"
    }

    # ── Verify checksum ──────────────────────────────────────────────────

    if (-not $NoVerify) {
        Write-Info "Verifying SHA256 checksum..."
        try {
            $checksums = (Invoke-WebRequest -Uri $ChecksumsUrl -UseBasicParsing).Content
        } catch {
            Exit-Error "Failed to download checksums.txt"
        }

        $expectedLine = ($checksums -split "`n") | Where-Object { $_ -match "\s+${AssetName}$" } | Select-Object -First 1
        if (-not $expectedLine) { Exit-Error "Asset $AssetName not found in checksums.txt" }
        $expected = ($expectedLine.Trim() -split '\s+')[0]

        $actual = (Get-FileHash -Path $TmpBinary -Algorithm SHA256).Hash.ToLower()

        if ($actual -ne $expected) {
            Exit-Error "Checksum mismatch!`n  expected: $expected`n  actual:   $actual"
        }
        Write-Ok "Checksum verified"
    } else {
        Write-Warn "Skipping checksum verification (-NoVerify)"
    }

    # ── Install binary ───────────────────────────────────────────────────

    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

    $DestBinary = Join-Path $InstallDir $BinaryName

    # Stop running instance if present (best-effort) so we can overwrite
    Get-Process -Name "devproxy" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 500

    Copy-Item -Path $TmpBinary -Destination $DestBinary -Force
    Write-Ok "Installed $BinaryName to $DestBinary"

    # ── Write default config ────────────────────────────────────────────

    $ConfigDir = Join-Path $env:LOCALAPPDATA "devstudio-proxy"
    $ConfigFile = Join-Path $ConfigDir "config.conf"

    if (-not (Test-Path $ConfigFile)) {
        New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
        @"
# devstudio-proxy configuration
# CLI flags override these settings. See: devproxy -help
#
# Priority: compiled defaults < this file < DEVPROXY_* env vars < CLI flags

# Port to listen on (default: 7700)
PORT=7700

# Enable per-request logging to stderr (default: false)
LOG=false

# Also log all request headers (default: false)
VERBOSE=false

# MCP script refresh interval, e.g. 30m, 1h, 0 to disable (default: 30m)
MCP_REFRESH=30m

# Enable remote MCP forward fallback (default: false)
MCP_FALLBACK=false
"@ | Set-Content -Path $ConfigFile -Encoding UTF8
        Write-Info "Default config written to $ConfigFile"
    }

    # ── Add to PATH ──────────────────────────────────────────────────────

    if ($AddToPath) {
        $userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
        if ($userPath -notlike "*$InstallDir*") {
            [Environment]::SetEnvironmentVariable("PATH", "$InstallDir;$userPath", "User")
            $env:PATH = "$InstallDir;$env:PATH"
            Write-Ok "Added $InstallDir to user PATH"
            Write-Info "Restart your terminal for PATH changes to take effect in new sessions"
        }
    }

    # ── Done ─────────────────────────────────────────────────────────────

    Write-Ok "devstudio-proxy $Version installed successfully!"
    Write-Info "Run:  devproxy -version"
    Write-Info "Docs: https://github.com/$Repo#readme"

} finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}
