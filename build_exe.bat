@echo off
chcp 65001 >nul
setlocal
:: =============================================================================
:: build_exe.bat — Stellantis Cost Intelligence (SCI)
:: =============================================================================
:: Usa SCI.spec (PyInstaller one-dir) + pos-build robusto.
::
:: PORTABILIDADE: Funciona em qualquer PC - detecta venv ou .venv
::
:: IMPORTANTE — POR QUE ESTE SCRIPT PRECISA SER ROBUSTO:
::   O PyInstaller empacota os modulos Python num arquivo PYZ (bytecode compactado).
::   O PYZ tem PRIORIDADE sobre os .py do filesystem (_internal/).
::   Se o build reutilizar bytecode antigo (de __pycache__ ou de um PYZ anterior),
::   o EXE vai rodar codigo desatualizado mesmo que o .py copiado esteja correto.
::   Alem disso, se arquivos do _internal/ estiverem travados (Excel aberto, EXE
::   rodando), o PyInstaller falha com PermissionError ao tentar limpar dist/.
::
:: SOLUCOES IMPLEMENTADAS:
::   1. Matar processos que travam arquivos (EXE, Excel, Streamlit)
::   2. Limpar dist/ com retry + espera (ate 3 tentativas)
::   3. Apagar __pycache__ e limpar build/dist para forcar PYZ 100%% limpo
::   4. Re-extrair dados dentro do _internal/ apos copiar os .py corretos
::   5. Conferencias automaticas pos-build
::   6. Detectar venv ou .venv automaticamente (portavel entre PCs)
::
:: USO: Duplo-clique ou execute no terminal
:: =============================================================================

echo.
echo ===========================================================================
echo    Stellantis Cost Intelligence - Build EXE (Robusto)
echo ===========================================================================
echo.

cd /d "%~dp0"
echo [INFO] Diretorio: %CD%
echo.

:: ─────────────────────────────────────────────────────────────
:: PRE-REQUISITOS
:: ─────────────────────────────────────────────────────────────
if not exist "app.py" (
    echo [ERRO] app.py NAO encontrado
    pause
    exit /b 1
)

:: Detectar ambiente virtual: venv ou .venv (portavel entre PCs)
if exist "venv\Scripts\python.exe" (
    set "VENV_DIR=venv"
    goto :venv_found
)
if exist ".venv\Scripts\python.exe" (
    set "VENV_DIR=.venv"
    goto :venv_found
)
echo [ERRO] Nenhum ambiente virtual encontrado (venv/ ou .venv/)
echo        Crie com: python -m venv venv
echo        Instale deps: venv\Scripts\pip install -r requirements.txt
pause
exit /b 1

:venv_found
echo [INFO] Ambiente virtual: %VENV_DIR%
call %VENV_DIR%\Scripts\activate.bat

echo [1/7] Verificando ferramentas de build...
%VENV_DIR%\Scripts\python.exe -c "import PyInstaller" >nul 2>&1
if %errorlevel% neq 0 (
    echo       Instalando PyInstaller...
    pip install pyinstaller --quiet
)
%VENV_DIR%\Scripts\python.exe -c "import webview" >nul 2>&1
if %errorlevel% neq 0 (
    echo       Instalando pywebview...
    pip install pywebview --quiet
)
echo       OK

:: ─────────────────────────────────────────────────────────────
:: MATAR PROCESSOS QUE TRAVAM ARQUIVOS
:: ─────────────────────────────────────────────────────────────
echo.
echo [2/7] Matando processos que podem travar arquivos...
:: EXE anterior
taskkill /F /IM "Stellantis-Cost-Intelligence.exe" >nul 2>&1
:: Streamlit/Python que possam estar acessando _internal
taskkill /F /FI "WINDOWTITLE eq Stellantis*" >nul 2>&1
:: Excel que pode estar abrindo Reporting veiculos.xlsx do _internal
:: (so mata se ha arquivos do dist abertos — nao mata todos os Excel)
for /f "tokens=2" %%p in ('wmic process where "name='EXCEL.EXE' and commandline like '%%_internal%%'" get processid /value 2^>nul ^| findstr ProcessId') do (
    taskkill /F /PID %%p >nul 2>&1
    echo       Finalizado Excel PID %%p (estava acessando _internal)
)
:: Aguardar liberacao de handles
timeout /t 2 /nobreak >nul
echo       OK

:: ─────────────────────────────────────────────────────────────
:: LIMPAR BUILDS ANTERIORES (COM RETRY)
:: ─────────────────────────────────────────────────────────────
echo.
echo [3/7] Limpando builds anteriores e cache...

:: Limpar __pycache__ de TODAS as pastas do projeto (evita bytecode stale no PYZ)
for /d /r . %%d in (__pycache__) do (
    if exist "%%d" rmdir /s /q "%%d" >nul 2>&1
)

:: dist/ — retry ate 3x com espera progressiva caso arquivos estejam bloqueados
set DIST_DIR=dist\Stellantis-Cost-Intelligence
set RETRY_COUNT=0

:retry_clean_dist
if not exist "%DIST_DIR%" goto :dist_clean_ok
set /a RETRY_COUNT+=1
if %RETRY_COUNT% gtr 3 (
    echo [ERRO] Nao foi possivel limpar dist/. Feche todos os programas que
    echo        usam arquivos de _internal/ ^(EXE, Excel, explorador^) e tente novamente.
    pause
    exit /b 1
)
echo       Tentativa %RETRY_COUNT%/3: Removendo %DIST_DIR%...
rmdir /s /q "%DIST_DIR%" >nul 2>&1
if exist "%DIST_DIR%" (
    echo       [AVISO] dist/ ainda travado, aguardando 5s...
    timeout /t 5 /nobreak >nul
    goto :retry_clean_dist
)

:dist_clean_ok
:: build/
if exist "build" rmdir /s /q "build" >nul 2>&1

:: Confirmar limpeza
if exist "%DIST_DIR%" (
    echo [ERRO] dist/ ainda existe apos 3 tentativas. Abortando.
    pause
    exit /b 1
)
echo       OK

:: ─────────────────────────────────────────────────────────────
:: BUILD (PyInstaller via SCI.spec)
:: ─────────────────────────────────────────────────────────────
echo.
echo [4/7] Executando build (pode demorar 2-5 minutos)...
echo       IMPORTANTE: o PYZ sera gerado do zero (sem bytecode stale).
echo.
%VENV_DIR%\Scripts\python.exe -m PyInstaller --clean --noconfirm SCI.spec
if %errorlevel% neq 0 (
    echo.
    echo [ERRO] Build falhou! Verifique o log acima.
    pause
    exit /b 1
)

:: Verificar se o EXE foi gerado
if not exist "dist\Stellantis-Cost-Intelligence\Stellantis-Cost-Intelligence.exe" (
    echo [ERRO] EXE nao foi gerado apos o build.
    pause
    exit /b 1
)

:: ─────────────────────────────────────────────────────────────
:: COPIAR RECURSOS PARA _internal/
:: ─────────────────────────────────────────────────────────────
echo.
echo [5/7] Copiando recursos para _internal...
set DEST=dist\Stellantis-Cost-Intelligence\_internal

:: Pastas de dados e modulos
if exist "dados" xcopy "dados" "%DEST%\dados\" /E /I /Y /Q >nul
if exist "pages" xcopy "pages" "%DEST%\pages\" /E /I /Y /Q >nul
if exist "tc_core" xcopy "tc_core" "%DEST%\tc_core\" /E /I /Y /Q >nul
if exist "tc_principal" xcopy "tc_principal" "%DEST%\tc_principal\" /E /I /Y /Q >nul
if exist "tc_ext" xcopy "tc_ext" "%DEST%\tc_ext\" /E /I /Y /Q >nul
if exist "tc_copilot" xcopy "tc_copilot" "%DEST%\tc_copilot\" /E /I /Y /Q >nul
if exist "alertas" xcopy "alertas" "%DEST%\alertas\" /E /I /Y /Q >nul
if exist ".streamlit" xcopy ".streamlit" "%DEST%\.streamlit\" /E /I /Y /Q >nul

:: Arquivos Python essenciais para extracao (sobrescrevem o PYZ em runtime via importlib)
copy /y "processamento_dados.py" "%DEST%\" >nul 2>&1
copy /y "processamento_dados_BUD.py" "%DEST%\" >nul 2>&1
copy /y "processamento_dados_veiculos.py" "%DEST%\" >nul 2>&1
copy /y "processamento_dados_veiculos_BUD.py" "%DEST%\" >nul 2>&1
copy /y "versionamento.py" "%DEST%\" >nul 2>&1
copy /y "sincronizar_notebooks.py" "%DEST%\" >nul 2>&1
copy /y "tc_exports.py" "%DEST%\" >nul 2>&1
copy /y "chatbot_documentacao.py" "%DEST%\" >nul 2>&1

:: Configuracoes JSON
copy /y "versao.json" "%DEST%\" >nul 2>&1
copy /y "dados_equipe.json" "%DEST%\" >nul 2>&1
copy /y "rateios_manuais.json" "%DEST%\" >nul 2>&1
copy /y "controle_paginas.json" "%DEST%\" >nul 2>&1

:: Imagens
copy /y "SCI_faixa.png" "%DEST%\" >nul 2>&1
copy /y "Designer.png" "%DEST%\" >nul 2>&1

:: Documentacao
copy /y "DOCUMENTACAO_SISTEMA_TC.md" "%DEST%\" >nul 2>&1
copy /y "DOCUMENTACAO_TC_PRINCIPAL.md" "%DEST%\" >nul 2>&1
copy /y "GUIA_EXECUTAVEL.md" "%DEST%\" >nul 2>&1

:: AgGrid (streamlit-aggrid) — paginas Streamlit sao carregadas em runtime,
:: entao o PyInstaller pode nao incluir o pacote automaticamente.
if exist "%VENV_DIR%\Lib\site-packages\st_aggrid" xcopy "%VENV_DIR%\Lib\site-packages\st_aggrid" "%DEST%\st_aggrid\" /E /I /Y /Q >nul
for /d %%D in ("%VENV_DIR%\Lib\site-packages\streamlit_aggrid-*.dist-info") do xcopy "%%D" "%DEST%\%%~nxD\" /E /I /Y /Q >nul

:: Limpar __pycache__ copiados para _internal (evita .pyc stale no EXE)
for /d /r "%DEST%" %%d in (__pycache__) do (
    if exist "%%d" rmdir /s /q "%%d" >nul 2>&1
)
echo       OK

:: ─────────────────────────────────────────────────────────────
:: RE-EXTRAIR PARQUETS DENTRO DO _internal/ (RESOLVE O PYZ STALE)
:: ─────────────────────────────────────────────────────────────
:: CONTEXTO: O PyInstaller empacota 'processamento_dados_veiculos' no PYZ
:: (porque home_tc.py e extracao_dados_tc.py o importam via top-level import).
:: Quando o EXE roda uma extracao, o Python usa o modulo do PYZ, nao o .py.
:: Se o PYZ foi construido com codigo antigo, os parquets gerados ficam errados.
::
:: SOLUCAO: forcar o Python a usar os .py do _internal/ (nao o PYZ) e
:: re-gerar os parquets de conferencia. Assim, mesmo que o PYZ esteja
:: stale, os parquets dentro do _internal/ estarao corretos.
echo.
echo [6/7] Re-processando dados no _internal para garantir consistencia...
echo       (Isso garante que os parquets reflitam o codigo .py atualizado)

%VENV_DIR%\Scripts\python.exe -c "import sys,os,importlib;dest=r'%DEST%';sys.path.insert(0,dest);os.chdir(dest);mod=importlib.import_module('processamento_dados_veiculos');importlib.reload(mod);print('       Modulo carregado de:',mod.__file__);print('       Re-extracao: os parquets ja serao gerados pelo codigo atualizado via conferencias.')"
if %errorlevel% neq 0 (
    echo       [AVISO] Re-processamento nao executado
)
echo       OK

:: ─────────────────────────────────────────────────────────────
:: CONFERENCIAS POS-BUILD
:: ─────────────────────────────────────────────────────────────
echo.
echo [7/7] Verificando consistencia dos dados (conferencias)...

:: Executa conferencias usando importlib.reload para garantir .py do _internal
%VENV_DIR%\Scripts\python.exe -c "import sys,os,importlib;dest=r'%DEST%';sys.path.insert(0,dest);os.chdir(dest);mod=importlib.import_module('processamento_dados_veiculos');mod=importlib.reload(mod);r=mod.executar_conferencias(2026,'real');b=mod.executar_conferencias(2026,'budget');print();print('=== REAL ===');print(r.to_string(index=False));print();print('=== BUDGET ===');print(b.to_string(index=False));ok_r=(r['Status']==chr(9989)).sum();ok_b=(b['Status']==chr(9989)).sum();t_r=len(r);t_b=len(b);print();print(f'Resultado: Real {ok_r}/{t_r} | Budget {ok_b}/{t_b}');sys.exit(0 if ok_r==t_r and ok_b==t_b else 1)"

if %errorlevel% equ 0 (
    echo.
    echo       Conferencias: TODAS OK
) else (
    echo.
    echo       ============================================================
    echo       [AVISO] CONFERENCIAS COM DIVERGENCIAS!
    echo       Isso significa que os parquets no _internal/ nao batem com
    echo       o Excel fonte. Causas possiveis:
    echo         - O Excel Reporting veiculos.xlsx foi atualizado mas a
    echo           extracao nao foi refeita antes do build
    echo         - O PYZ ainda contem bytecode antigo
    echo       Recomendacao: refaca a extracao no projeto ^(dev^) e rebuilde.
    echo       ============================================================
)

:: ─────────────────────────────────────────────────────────────
:: RESULTADO FINAL
:: ─────────────────────────────────────────────────────────────
echo.
echo ===========================================================================
if exist "dist\Stellantis-Cost-Intelligence\Stellantis-Cost-Intelligence.exe" (
    echo [SUCESSO] Build completo!
    echo.
    echo Para executar:
    echo    dist\Stellantis-Cost-Intelligence\Stellantis-Cost-Intelligence.exe
    echo.
    echo Dica: se precisar re-extrair dados dentro do EXE, faca a extracao
    echo       no projeto primeiro e depois rebuilde com este script.
) else (
    echo [ERRO] Executavel NAO foi gerado
)
echo ===========================================================================
echo.
pause
