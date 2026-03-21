@echo off
setlocal

cd /d "%~dp0"

echo ========================================
echo   TC - Sincronizar e Executar App
echo ========================================
echo.

set "DBX_PROFILE=%SCI_DATABRICKS_PROFILE%"
if not "%~1"=="" set "DBX_PROFILE=%~1"

set "PROFILE_ARGS="
if not "%DBX_PROFILE%"=="" set "PROFILE_ARGS=-Profile "%DBX_PROFILE%""

REM ---- 1. Ativar ambiente virtual ----
set "VENV_DIR=.venv"
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    if exist "venv\Scripts\activate.bat" (
        set "VENV_DIR=venv"
    )
)

if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo [ERRO] Ambiente virtual nao encontrado!
    echo Crie com: python -m venv .venv
    pause
    exit /b 1
)

echo [1/3] Ativando ambiente virtual...
call "%VENV_DIR%\Scripts\activate.bat"
echo       OK
echo.

REM ---- 2. Sincronizar app (Databricks + espelhos) ----
echo [2/3] Sincronizando app com Databricks...
powershell -ExecutionPolicy Bypass -File ".\scripts\sync_databricks_app.ps1" %PROFILE_ARGS%
if errorlevel 1 (
    echo [AVISO] Sincronizacao retornou com avisos. Verifique acima.
    echo         Continuando para executar o app...
    echo.
)
echo       Sincronizacao concluida.
echo.

REM ---- 3. Executar o app ----
echo [3/3] Iniciando Streamlit...
echo ========================================
echo   App rodando em: http://localhost:8501
echo   Pressione Ctrl+C para parar.
echo ========================================
echo.
streamlit run app.py

endlocal
