@echo off
setlocal
cd /d "%~dp0\.."

for /f "delims=" %%F in ('gofmt -l ./cmd ./internal ./tests') do (
  echo Go file requires formatting: %%F
  exit /b 1
)

go vet ./...
if errorlevel 1 exit /b %errorlevel%

go test ./...
if errorlevel 1 exit /b %errorlevel%
