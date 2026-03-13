# -*- mode: python ; coding: utf-8 -*-
# =============================================================================
# SCI.spec — PyInstaller spec para Stellantis Cost Intelligence
# =============================================================================
# Gerar o EXE:  pyinstaller --clean --noconfirm SCI.spec
#
# Estrutura gerada em dist/Stellantis-Cost-Intelligence/:
#   ├── Stellantis-Cost-Intelligence.exe   ← executável principal
#   └── _internal/                          ← TUDO bundled aqui (self-contained)
#       ├── app.py
#       ├── SCI_faixa.png
#       ├── .streamlit/
#       ├── dados/
#       ├── versao.json
#       ├── dados_equipe.json
#       ├── rateios_manuais.json
#       ├── controle_paginas.json
#       └── ...
#
# PORTABILIDADE:
#   - Nenhum caminho absoluto — tudo relativo ao diretório do spec
#   - O EXE gerado roda em qualquer PC Windows sem dependências externas
#   - console=True para diagnóstico (erros ficam visíveis na primeira execução)
#
# CORREÇÕES (Mar/2026):
#   - Todos os caminhos são RELATIVOS (sem hardcode de C:\Users\...)
#   - collect_all('streamlit') + copy_metadata('streamlit') — FIX CRÍTICO
#   - Inclui processamento_dados*.py e todos os scripts da raiz como data
#   - hookspath=['.'] para usar hook-streamlit.py local
#   - Inclui st_aggrid/streamlit-aggrid para tabelas interativas
#   - Inclui alertas, TC Copilot e dependências dinâmicas recentes (Graph/PPT/PDF)
#   - Removido pywebview/pythonnet (pesado) — app abre direto no navegador
#   - console=False para UX limpa (sem janela de console)
# =============================================================================

from PyInstaller.utils.hooks import (
    collect_all,
    collect_data_files,
    collect_dynamic_libs,
    collect_submodules,
    copy_metadata,
)
import sys
import os
import glob

block_cipher = None

# Diretório base = onde este .spec está (raiz do projeto)
SPEC_DIR = os.path.dirname(os.path.abspath(SPECPATH)) if 'SPECPATH' in dir() else os.path.abspath(".")

# ---------------------------------------------------------------------------
# Coleta COMPLETA do Streamlit: dados + binários + submodules + METADADOS
# ---------------------------------------------------------------------------
st_datas, st_binaries, st_hiddenimports = collect_all("streamlit")

# Coleta st_aggrid para tabelas interativas
ag_datas, ag_binaries, ag_hiddenimports = [], [], []
try:
    ag_datas, ag_binaries, ag_hiddenimports = collect_all("st_aggrid")
except Exception:
    pass

# Coleta bibliotecas com imports dinâmicos usados em relatórios, alertas e apresentação
pptx_datas, pptx_binaries, pptx_hiddenimports = [], [], []
try:
    pptx_datas, pptx_binaries, pptx_hiddenimports = collect_all("pptx")
except Exception:
    pass

reportlab_datas, reportlab_binaries, reportlab_hiddenimports = [], [], []
try:
    reportlab_datas, reportlab_binaries, reportlab_hiddenimports = collect_all("reportlab")
except Exception:
    pass

msal_datas, msal_binaries, msal_hiddenimports = [], [], []
try:
    msal_datas, msal_binaries, msal_hiddenimports = collect_all("msal")
except Exception:
    pass

openai_datas, openai_binaries, openai_hiddenimports = [], [], []
try:
    openai_datas, openai_binaries, openai_hiddenimports = collect_all("openai")
except Exception:
    pass

# Metadados de pacotes que usam importlib.metadata em runtime
extra_metadata = []
for pkg in ["streamlit", "altair", "pandas", "pyarrow", "packaging",
            "watchdog", "click", "tornado", "openpyxl",
            "plotly", "numpy", "streamlit-aggrid", "python-pptx",
            "reportlab", "msal", "openai", "PyPDF2", "python-dotenv",
            "certifi", "truststore", "jsonschema", "jsonschema-specifications",
            "referencing"]:
    try:
        extra_metadata += copy_metadata(pkg)
    except Exception:
        pass

# Altair: dados estáticos (schemas JSON)
altair_datas = collect_data_files("altair")
altair_schema_datas = []
try:
    altair_schema_datas = collect_data_files("altair.vegalite.v5.schema")
except Exception:
    pass

jsonschema_spec_datas = []
try:
    jsonschema_spec_datas = collect_data_files("jsonschema_specifications")
except Exception:
    pass

# Bibliotecas nativas e recursos críticos de pacotes numéricos
numpy_datas, numpy_binaries = [], []
try:
    numpy_datas = collect_data_files("numpy")
    numpy_binaries = collect_dynamic_libs("numpy")
except Exception:
    pass

pandas_datas, pandas_binaries = [], []
try:
    pandas_datas = collect_data_files("pandas")
    pandas_binaries = collect_dynamic_libs("pandas")
except Exception:
    pass

pyarrow_datas, pyarrow_binaries = [], []
pyarrow_hiddenimports = []
try:
    pyarrow_datas = collect_data_files("pyarrow")
    pyarrow_binaries = collect_dynamic_libs("pyarrow")
    pyarrow_hiddenimports = collect_submodules("pyarrow")
except Exception:
    pass

scipy_binaries = []
try:
    scipy_binaries = collect_dynamic_libs("scipy")
except Exception:
    pass

# Plotly: templates e dados estáticos
plotly_datas = []
try:
    plotly_datas = collect_data_files("plotly")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Hidden imports — módulos carregados dinamicamente
# ---------------------------------------------------------------------------
hidden = (
    st_hiddenimports
    + ag_hiddenimports
    + pptx_hiddenimports
    + reportlab_hiddenimports
    + msal_hiddenimports
    + openai_hiddenimports
    + [
    # Streamlit internos extras
    "streamlit.web.cli",
    "streamlit.runtime",
    "streamlit.runtime.scriptrunner",
    "streamlit.runtime.scriptrunner.script_runner",
    "streamlit.components.v1",
    "streamlit.runtime.state",
    "streamlit.elements",
    "streamlit.logger",
    # Pacotes de dados
    "altair",
    "altair.vegalite.v5",
    "altair.vegalite.v5.schema",
    "jsonschema",
    "jsonschema.protocols",
    "jsonschema.validators",
    "jsonschema_specifications",
    "referencing",
    "openpyxl",
    "openpyxl.styles.stylesheet",
    "pyarrow",
    "pyarrow.pandas_compat",
    "pyarrow.parquet",
    "pyarrow._parquet",
    "pyarrow.lib",
    "pyarrow._arrow_lib",
    "pandas",
    "numpy",
    "plotly",
    "plotly.graph_objects",
    "plotly.express",
    "pydeck",
    "reportlab",
    "reportlab.lib",
    "reportlab.platypus",
    "pptx",
    "msal",
    "requests",
    "certifi",
    "truststore",
    "dotenv",
    "openai",
    "PyPDF2",
    "matplotlib",
    "matplotlib.pyplot",
    # AgGrid
    "st_aggrid",
    # Dependências do Streamlit
    "watchdog",
    "tornado",
    "click",
    "packaging",
    "PIL",
    # Módulos internos do projeto (pacotes)
    "tc_core",
    "tc_core.utils",
    "tc_core.utils.portabilidade",
    "tc_core.constants",
    "tc_core.data",
    "tc_core.data.paths",
    "tc_core.data.periodos",
    "tc_core.data.schema",
    "tc_core.finance",
    "tc_core.finance.currency",
    "tc_core.finance.currency_db",
    "tc_core.ui",
    "tc_core.ui.header",
    "tc_principal",
    "tc_principal.shared",
    "tc_principal.ui_components",
    "tc_principal.pages",
    "tc_principal.pages.home_tc",
    "tc_principal.pages.waterfall_tc",
    "tc_principal.pages.best_estimate_simulador_tc",
    "tc_principal.pages.extracao_dados_tc",
    "tc_principal.pages.debug_calculos_tc",
    "tc_ext",
    "tc_ext.normalizacao",
    "tc_ext.metricas_tc_ext",
    "tc_ext.pages",
    "tc_ext.pages.home_ext",
    "tc_ext.pages.be_analise_ext",
    "tc_copilot",
    "tc_copilot.pages",
    "tc_copilot.pages.home_copilot",
    "tc_core.presentation_docs",
    "alertas",
    "alertas.alert_ui",
    "alertas.alert_config_ui",
    "alertas.alert_engine",
    "alertas.notifications_email",
    "alertas.notifications_teams",
    "alertas.email_graph",
    "alertas.scheduler",
    # Scripts da raiz (importados por nome sem pacote)
    "processamento_dados",
    "processamento_dados_BUD",
    "processamento_dados_veiculos",
    "processamento_dados_veiculos_BUD",
    "versionamento",
    "tc_exports",
    "sincronizar_notebooks",
    "chatbot_documentacao",
    # Python stdlib que pode ser lazy-loaded
    "base64",
    "pathlib",
    "json",
    "datetime",
    "traceback",
    "socket",
    "threading",
    "webbrowser",
    "ctypes",
])

hidden += pyarrow_hiddenimports

# ---------------------------------------------------------------------------
# Dados bundled — tudo em _internal/ (self-contained)
# TODOS os caminhos são RELATIVOS (portável para qualquer PC)
# ---------------------------------------------------------------------------
datas = [
    # Imagens e branding
    ("SCI_faixa.png",   "."),
    ("Designer.png",    "."),
    # Configuração do Streamlit
    (".streamlit",      ".streamlit"),
    # Arquivo principal do app
    ("app.py",          "."),
    # Páginas Streamlit (multipage)
    ("pages",           "pages"),
    # Pacotes do projeto (código-fonte completo)
    ("tc_core",         "tc_core"),
    ("tc_principal",    "tc_principal"),
    ("tc_ext",          "tc_ext"),
    ("tc_copilot",      "tc_copilot"),
    ("alertas",         "alertas"),
    # Scripts Python da raiz (importados diretamente pelo nome)
    ("processamento_dados.py",              "."),
    ("processamento_dados_BUD.py",          "."),
    ("processamento_dados_veiculos.py",     "."),
    ("processamento_dados_veiculos_BUD.py", "."),
    ("versionamento.py",                    "."),
    ("tc_exports.py",                       "."),
    ("sincronizar_notebooks.py",            "."),
    ("chatbot_documentacao.py",             "."),
    # Documentação bundled (Markdown exibido na interface)
    ("DOCUMENTACAO_SISTEMA_TC.md",          "."),
    ("DOCUMENTACAO_TC_PRINCIPAL.md",        "."),
    ("DOCUMENTACAO_FLEX_BUD_ANO_COMPLETO.md", "."),
    ("GUIA_EXECUTAVEL.md",                  "."),
    # Dados bundled — ficam em _internal/dados/ (leitura + escrita ok em one-dir)
    ("dados",                               "dados"),
    # JSONs de configuração / estado (mutáveis pelo app em runtime)
    ("versao.json",                         "."),
    ("dados_equipe.json",                   "."),
    ("rateios_manuais.json",                "."),
    ("controle_paginas.json",               "."),
]

# Juntar com dados coletados automaticamente
datas += st_datas
datas += altair_datas
datas += altair_schema_datas
datas += jsonschema_spec_datas
datas += plotly_datas
datas += numpy_datas
datas += pandas_datas
datas += pyarrow_datas
datas += extra_metadata
datas += ag_datas
datas += pptx_datas
datas += reportlab_datas
datas += msal_datas
datas += openai_datas

# Binaries
all_binaries = (
    st_binaries
    + ag_binaries
    + numpy_binaries
    + pandas_binaries
    + pyarrow_binaries
    + scipy_binaries
    + pptx_binaries
    + reportlab_binaries
    + msal_binaries
    + openai_binaries
)

# ---------------------------------------------------------------------------
# Análise — TODOS os caminhos relativos
# ---------------------------------------------------------------------------
a = Analysis(
    ["launcher.py"],
    pathex=["."],
    binaries=all_binaries,
    datas=datas,
    hiddenimports=hidden,
    hookspath=["."],       # usa hook-streamlit.py local
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Excluir pacotes de dev/teste para reduzir tamanho
        "pytest",
        "IPython",
        "jupyter",
        "notebook",
        "black",
        "flake8",
        "mypy",
        "pip",
        "setuptools",
        # pywebview e dependencias .NET (pesado, app abre no navegador)
        "webview",
        "clr_loader",
        "pythonnet",
        "bottle",
        "proxy_tools",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Stellantis-Cost-Intelligence",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,           # Sem janela de console — app abre direto no navegador
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon="assets/SCI.ico",  # Descomente e ajuste se tiver ícone .ico
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="Stellantis-Cost-Intelligence",
)
