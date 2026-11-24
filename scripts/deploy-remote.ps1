<#
.SYNOPSIS
Pulls the latest code from GitHub on a remote Linux host, refreshes the editable install, and runs pytest.
Configure via parameters or environment variables: AI_MEMORY_SSH_HOST and AI_MEMORY_REMOTE_DIR.
#>
param(
    [string]$RemoteHost = $env:AI_MEMORY_SSH_HOST,
    [string]$RemoteDir = $env:AI_MEMORY_REMOTE_DIR,
    [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

if (-not $RemoteHost) {
    throw "Specify -RemoteHost or set AI_MEMORY_SSH_HOST."
}
if (-not $RemoteDir) {
    throw "Specify -RemoteDir or set AI_MEMORY_REMOTE_DIR."
}

Write-Host "Pulling origin/$Branch on remote host ..."
$cmd = "cd $RemoteDir && git fetch origin && git pull --ff-only origin $Branch && .venv/bin/pip install -q --no-build-isolation --no-deps -e . && .venv/bin/pytest -q"
& ssh -o BatchMode=yes $RemoteHost $cmd
if ($LASTEXITCODE -ne 0) {
    throw "Remote deploy failed with exit code $LASTEXITCODE"
}
Write-Host "Remote host updated and verified."
