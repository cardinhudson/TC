@echo off
setlocal

cd /d "%~dp0"

set "DBX_PROFILE=%SCI_DATABRICKS_PROFILE%"
if not "%~2"=="" set "DBX_PROFILE=%~2"
if not "%~1"=="" if /I not "%~1"=="watch" set "DBX_PROFILE=%~1"

set "PROFILE_ARGS="
if not "%DBX_PROFILE%"=="" set "PROFILE_ARGS=-Profile "%DBX_PROFILE%""

if /I "%~1"=="watch" (
    powershell -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -Watch %PROFILE_ARGS%
) else (
    powershell -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" %PROFILE_ARGS%
)

endlocal