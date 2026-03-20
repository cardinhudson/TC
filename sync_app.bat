@echo off
setlocal

cd /d "%~dp0"

if /I "%~1"=="watch" (
    powershell -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -Watch
) else (
    powershell -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1"
)

endlocal