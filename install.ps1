# Verglas one-line installer (Windows, PowerShell).
#
# Fetches prebuilt binaries for all four Verglas tools from the latest GitHub
# Release and installs them (no Rust toolchain required). Each tool has its own
# cargo-dist installer that verifies a SHA-256 checksum; this wrapper runs all
# four so `irm ... install.ps1 | iex` installs the whole suite in one command.
#
# cargo-dist emits one installer per crate (the four binaries live in four
# workspace packages), so there is no single native "install everything"
# script -- this wrapper is that entry point. To install one tool only, run its
# own installer, e.g.:
#   irm https://github.com/verglas-org/verglas/releases/latest/download/verglas-installer.ps1 | iex
#
# NOTE: installing verglasd as a Windows service is not supported yet; run the
# daemon in the foreground (`verglasd --config <path>`).
$ErrorActionPreference = 'Stop'

$base = if ($env:VERGLAS_RELEASE_BASE) { $env:VERGLAS_RELEASE_BASE } else { 'https://github.com/verglas-org/verglas/releases/latest/download' }
$apps = @('verglas', 'verglasd', 'verglas-mcp', 'verglas-consolidate')

foreach ($app in $apps) {
    Write-Host "==> installing $app"
    & ([scriptblock]::Create((irm "$base/$app-installer.ps1")))
}

Write-Host ''
Write-Host 'Verglas installed (verglas, verglasd, verglas-mcp, verglas-consolidate).'
Write-Host 'Next: verglas init --bucket <bucket> --region <region>'
