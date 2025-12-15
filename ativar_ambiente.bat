@echo off
echo ========================================
echo   Ativando Ambiente Virtual Python
echo ========================================
echo.

REM Verificar se o ambiente virtual existe
if not exist "venv\Scripts\activate.bat" (
    echo [ERRO] Ambiente virtual nao encontrado!
    echo.
    echo Criando ambiente virtual...
    python -m venv venv
    echo.
    echo Instalando dependencias...
    call venv\Scripts\activate.bat
    pip install -r requirements.txt
    echo.
    echo Ambiente virtual criado e dependencias instaladas!
    echo.
) else (
    echo Ativando ambiente virtual...
    call venv\Scripts\activate.bat
    echo.
    echo Ambiente virtual ativado!
    echo.
    echo Para executar a aplicacao, use:
    echo   streamlit run app.py
    echo.
)

REM Manter o prompt aberto
cmd /k


