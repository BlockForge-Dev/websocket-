$ErrorActionPreference = "Stop"

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $unformatted = gofmt -l ./cmd ./internal ./tests
    if ($unformatted) {
        Write-Error "Go files require formatting:`n$($unformatted -join "`n")"
    }

    go vet ./...
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    go test ./...
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}
