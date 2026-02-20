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
#       ├── dados/                            ← dados bundled (leitura + escrita ok)
#       ├── versao.json
#       ├── dados_equipe.json
#       ├── rateios_manuais.json
#       ├── controle_paginas.json
#       └── ...
# NOTA: _internal/ é uma pasta normal e gravável — JSONs podem ser
#       editados pelo app em tempo de execução sem problemas.
#
# CORREÇÕES (Jul/2026):
#   - collect_all('streamlit') + copy_metadata('streamlit')  ← FIX CRÍTICO
#     Streamlit 1.x usa importlib.metadata na inicialização. Sem os metadados
#     bundled lança PackageNotFoundError → sys.exit(1) → janela fecha silencioso.
#   - hookspath=['.'] para usar hook-streamlit.py local
#   - console=True para diagnóstico (erros ficam visíveis)
# =============================================================================

from PyInstaller.utils.hooks import (
    collect_all,
    collect_data_files,
    collect_submodules,
    copy_metadata,
)
import sys
import os

block_cipher = None

# ---------------------------------------------------------------------------
# Coleta COMPLETA do Streamlit: dados + binários + submodules + METADADOS
# Ref: https://pyinstaller.org/en/stable/hooks.html#copy-metadata
# copy_metadata é OBRIGATÓRIO — Streamlit chama importlib.metadata.version()
# na inicialização; sem os dist-info bundled → crash silencioso.
# ---------------------------------------------------------------------------
st_datas, st_binaries, st_hiddenimports = collect_all("streamlit")

# Coleta pywebview para app desktop
try:
    wv_datas, wv_binaries, wv_hiddenimports = collect_all("webview")
except Exception:
    wv_datas, wv_binaries, wv_hiddenimports = [], [], []

# Metadados de pacotes que também usam importlib.metadata em runtime
extra_metadata = []
for pkg in ["streamlit", "altair", "pandas", "pyarrow", "packaging",
            "validators", "watchdog", "click", "tornado", "openpyxl",
            "plotly", "numpy", "scipy", "scikit-learn", "pywebview"]:
    try:
        extra_metadata += copy_metadata(pkg)
    except Exception:
        pass  # pacote não instalado — ignorar

# Altair: dados estáticos (schemas JSON)
altair_datas = collect_data_files("altair")

# ---------------------------------------------------------------------------
# Coleta automática de sub-módulos dinâmicos do projeto e dependências
# ---------------------------------------------------------------------------
hidden = st_hiddenimports + [
    # Streamlit internos extras
    "streamlit.web.cli",
    "streamlit.runtime.scriptrunner",
    "streamlit.runtime.scriptrunner.script_runner",
    "streamlit.components.v1",
    "streamlit.runtime.state",
    "streamlit.runtime.legacy_caching",
    "streamlit.elements",
    "streamlit.logger",
    # pywebview para app desktop
    "webview",
    "webview.platforms",
    "webview.platforms.winforms",
    "webview.platforms.edgechromium",
    "clr_loader",
    "pythonnet",
    "bottle",
    "proxy_tools",
    # Pacotes de dados
    "altair",
    "altair.vegalite.v5",
    "openpyxl",
    "openpyxl.styles.stylesheet",
    "pyarrow",
    "pyarrow.pandas_compat",
    "pandas",
    "numpy",
    "plotly",
    "plotly.graph_objects",
    "sklearn",
    "scipy",
    "sqlalchemy",
    "pydeck",
    # Dependências do Streamlit
    "validators",
    "watchdog",
    "tornado",
    "click",
    "packaging",
    "PIL",
    # Módulos internos do projeto
    "tc_core",
    "tc_core.utils",
    "tc_core.utils.portabilidade",
    "tc_core.data",
    "tc_core.finance",
    "tc_core.ui",
    "tc_principal",
    "tc_principal.pages",
    "tc_ext",
    "tc_ext.pages",
    "versionamento",
]

# ---------------------------------------------------------------------------
# Todos os assets e dados bundled em _internal/ (self-contained)
# ---------------------------------------------------------------------------
datas = [
    # Imagens e branding
    ("SCI_faixa.png",   "."),
    ("Designer.png",    "."),
    # Configuração do Streamlit
    (".streamlit",      ".streamlit"),
    # Arquivos do app e módulos
    ("app.py",          "."),
    ("pages",           "pages"),
    ("tc_core",         "tc_core"),
    ("tc_principal",    "tc_principal"),
    ("tc_ext",          "tc_ext"),
    # Documentação bundled (Markdown exibido na interface)
    ("DOCUMENTACAO_SISTEMA_TC.md",      "."),
    ("DOCUMENTACAO_TC_PRINCIPAL.md",    "."),
    # Dados bundled — ficam em _internal/dados/ (leit. e escrita ok em one-dir)
    ("dados",                           "dados"),
    # JSONs de configuração / estado (mutáveis pelo app em runtime)
    ("versao.json",                     "."),
    ("dados_equipe.json",               "."),
    ("rateios_manuais.json",            "."),
    ("controle_paginas.json",           "."),
]

# Juntar com dados coletados automaticamente
datas += st_datas
datas += altair_datas
datas += extra_metadata
datas += wv_datas  # pywebview data files

# Binaries do pywebview
all_binaries = st_binaries + wv_binaries

# ---------------------------------------------------------------------------
# Análise
# ---------------------------------------------------------------------------
a = Analysis(
    ["launcher.py"],
    pathex=["."],
    binaries=all_binaries,
    datas=datas,
    hiddenimports=hidden + wv_hiddenimports,
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
    console=True,            # console=True para diagnóstico — erros são visíveis
                             # Trocar para False após confirmar que funciona
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
