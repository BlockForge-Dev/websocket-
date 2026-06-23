$ErrorActionPreference = "Stop"

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    go mod download
    go version
    Write-Host "Repository setup complete."
} finally {
    Pop-Location
}
