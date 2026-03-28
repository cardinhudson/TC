@echo off
setlocal

cd /d "%~dp0\.."
powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" %*

set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
