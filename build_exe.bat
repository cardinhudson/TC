@echo off
chcp 65001 >nul
:: =============================================================================
:: build_exe.bat — Stellantis Cost Intelligence (SCI)
:: =============================================================================
:: Usa streamlit-desktop-app (mesmo metodo do DashAPPwin11)
::
:: USO: Duplo-clique ou execute no terminal
:: =============================================================================

echo.
echo ===========================================================================
echo    Stellantis Cost Intelligence - Build EXE
echo ===========================================================================
echo.

cd /d "%~dp0"
echo [INFO] Diretorio: %CD%
echo.

:: Verificar arquivos essenciais
if not exist "app.py" (
    echo [ERRO] app.py NAO encontrado
    pause
    exit /b 1
)
if not exist ".venv\Scripts\python.exe" (
    echo [ERRO] .venv NAO encontrado
    pause
    exit /b 1
)

:: Ativar ambiente virtual
call .venv\Scripts\activate.bat

:: Verificar streamlit-desktop-app
echo [1/4] Verificando streamlit-desktop-app...
pip show streamlit-desktop-app >nul 2>&1
if %errorlevel% neq 0 (
    echo       Instalando streamlit-desktop-app...
    pip install streamlit-desktop-app --quiet
)
echo       OK

:: Limpar builds anteriores
echo.
echo [2/4] Limpando builds anteriores...
taskkill /F /IM "Stellantis-Cost-Intelligence.exe" >nul 2>&1
if exist "dist\Stellantis-Cost-Intelligence" rmdir /s /q "dist\Stellantis-Cost-Intelligence"
if exist "build\Stellantis-Cost-Intelligence" rmdir /s /q "build\Stellantis-Cost-Intelligence"
echo       OK

:: Build com streamlit-desktop-app
echo.
echo [3/4] Executando build (pode demorar 2-5 minutos)...
echo.
streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence
if %errorlevel% neq 0 (
    echo [ERRO] Build falhou!
    pause
    exit /b 1
)

:: Copiar dados para _internal
echo.
echo [4/4] Copiando recursos para _internal...
set DEST=dist\Stellantis-Cost-Intelligence\_internal

if exist "dados" xcopy "dados" "%DEST%\dados\" /E /I /Y /Q >nul
if exist "pages" xcopy "pages" "%DEST%\pages\" /E /I /Y /Q >nul
if exist "tc_core" xcopy "tc_core" "%DEST%\tc_core\" /E /I /Y /Q >nul
if exist "tc_principal" xcopy "tc_principal" "%DEST%\tc_principal\" /E /I /Y /Q >nul
if exist "tc_ext" xcopy "tc_ext" "%DEST%\tc_ext\" /E /I /Y /Q >nul
if exist ".streamlit" xcopy ".streamlit" "%DEST%\.streamlit\" /E /I /Y /Q >nul

:: Arquivos Python essenciais para extração
copy "processamento_dados.py" "%DEST%\" >nul 2>&1
copy "processamento_dados_BUD.py" "%DEST%\" >nul 2>&1
copy "processamento_dados_veiculos.py" "%DEST%\" >nul 2>&1
copy "processamento_dados_veiculos_BUD.py" "%DEST%\" >nul 2>&1
copy "versionamento.py" "%DEST%\" >nul 2>&1
copy "sincronizar_notebooks.py" "%DEST%\" >nul 2>&1
copy "tc_exports.py" "%DEST%\" >nul 2>&1
copy "chatbot_documentacao.py" "%DEST%\" >nul 2>&1

:: Configurações JSON
copy "versao.json" "%DEST%\" >nul 2>&1
copy "dados_equipe.json" "%DEST%\" >nul 2>&1
copy "rateios_manuais.json" "%DEST%\" >nul 2>&1
copy "controle_paginas.json" "%DEST%\" >nul 2>&1

:: Imagens
copy "SCI_faixa.png" "%DEST%\" >nul 2>&1
copy "Designer.png" "%DEST%\" >nul 2>&1

:: Documentação
copy "DOCUMENTACAO_SISTEMA_TC.md" "%DEST%\" >nul 2>&1
copy "DOCUMENTACAO_TC_PRINCIPAL.md" "%DEST%\" >nul 2>&1
copy "GUIA_EXECUTAVEL.md" "%DEST%\" >nul 2>&1

:: AgGrid (streamlit-aggrid) — páginas do Streamlit são carregadas em runtime,
:: então o PyInstaller pode não incluir o pacote automaticamente.
:: Solução robusta: copiar o pacote do .venv para dentro do _internal.
if exist ".venv\Lib\site-packages\st_aggrid" xcopy ".venv\Lib\site-packages\st_aggrid" "%DEST%\st_aggrid\" /E /I /Y /Q >nul
for /d %%D in (".venv\Lib\site-packages\streamlit_aggrid-*.dist-info") do xcopy "%%D" "%DEST%\%%~nxD\" /E /I /Y /Q >nul

echo       OK

:: Resultado
echo.
echo ===========================================================================
if exist "dist\Stellantis-Cost-Intelligence\Stellantis-Cost-Intelligence.exe" (
    echo [SUCESSO] Build completo!
    echo.
    echo Para executar:
    echo    dist\Stellantis-Cost-Intelligence\Stellantis-Cost-Intelligence.exe
) else (
    echo [ERRO] Executavel NAO foi gerado
)
echo ===========================================================================
echo.
pause
