@echo off
setlocal

cd /d "%~dp0"

set "MODE=sync"
set "DBX_PROFILE=%SCI_DATABRICKS_PROFILE%"

if /I "%~1"=="watch" (
    set "MODE=watch"
    if not "%~2"=="" set "DBX_PROFILE=%~2"
) else if /I "%~1"=="deploy" (
    set "MODE=deploy"
    if not "%~2"=="" set "DBX_PROFILE=%~2"
) else if /I "%~1"=="sync" (
    set "MODE=sync"
    if not "%~2"=="" set "DBX_PROFILE=%~2"
) else if /I "%~1"=="nodeploy" (
    set "MODE=nodeploy"
    if not "%~2"=="" set "DBX_PROFILE=%~2"
) else if not "%~1"=="" (
    set "DBX_PROFILE=%~1"
)

if /I "%MODE%"=="watch" (
    if "%DBX_PROFILE%"=="" (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -Watch
    ) else (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -Watch -Profile "%DBX_PROFILE%"
    )
) else if /I "%MODE%"=="deploy" (
    if "%DBX_PROFILE%"=="" (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -DeployOnly
    ) else (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -DeployOnly -Profile "%DBX_PROFILE%"
    )
) else if /I "%MODE%"=="nodeploy" (
    if "%DBX_PROFILE%"=="" (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -SkipDeploy
    ) else (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -SkipDeploy -Profile "%DBX_PROFILE%"
    )
) else (
    if "%DBX_PROFILE%"=="" (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1"
    ) else (
        powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" -Profile "%DBX_PROFILE%"
    )
)

endlocal