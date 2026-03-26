@echo off
echo ========================================
echo   Ativando Ambiente Virtual Python
echo ========================================
echo.

REM Verificar se o ambiente virtual existe
set "VENV_DIR=.venv"
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    if exist "venv\Scripts\activate.bat" (
        set "VENV_DIR=venv"
    )
)

if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo [ERRO] Ambiente virtual nao encontrado!
    echo.
    echo Criando ambiente virtual...
    set "VENV_DIR=.venv"
    python -m venv %VENV_DIR%
    echo.
    echo Instalando dependencias...
    call %VENV_DIR%\Scripts\activate.bat
    pip install -r requirements.txt
    set "SCI_USE_OPTIMIZED_PARQUETS=true"
    echo   SCI_USE_OPTIMIZED_PARQUETS = true
    echo.
    echo Ambiente virtual criado e dependencias instaladas!
    echo.
) else (
    echo Ativando ambiente virtual...
    call %VENV_DIR%\Scripts\activate.bat
    set "SCI_USE_OPTIMIZED_PARQUETS=true"
    echo.
    echo Ambiente virtual ativado!
    echo   SCI_USE_OPTIMIZED_PARQUETS = true
    echo.
    echo Para executar a aplicacao, use:
    echo   streamlit run app.py
    echo.
)

REM Manter o prompt aberto
cmd /k


