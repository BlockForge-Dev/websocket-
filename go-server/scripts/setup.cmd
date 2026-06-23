@echo off
setlocal
cd /d "%~dp0\.."
go mod download
if errorlevel 1 exit /b %errorlevel%
go version
if errorlevel 1 exit /b %errorlevel%
echo Repository setup complete.
