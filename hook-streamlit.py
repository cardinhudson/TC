"""
hook-streamlit.py — Hook PyInstaller para Streamlit
====================================================
Copiado/adaptado do DashAPPwin11 (que funciona) e alinhado com
a documentação oficial do PyInstaller:
  https://pyinstaller.org/en/stable/hooks.html#copy-metadata

Resolve o erro silencioso:
   PackageNotFoundError: No package metadata was found for streamlit

Causa: Streamlit 1.x chama importlib.metadata.version('streamlit') e
pkg_resources na inicialização. Sem os metadados bundled, o processo
termina com sys.exit(1) e a janela fecha sem mostrar nada.

Solução: copy_metadata + collect_submodules + hiddenimports completos.
"""
from PyInstaller.utils.hooks import (
    copy_metadata,
    collect_data_files,
    collect_submodules,
)

# ESSENCIAL: copiar os dist-info do streamlit para que importlib.metadata
# possa encontrar a versão e os entry-points do pacote em tempo de execução.
datas = copy_metadata("streamlit")

# Assets estáticos do Streamlit (HTML, CSS, JS, ícones, templates)
datas += collect_data_files("streamlit")

# Todos os submódulos do Streamlit (evita ImportError em components, utils, etc.)
hiddenimports = collect_submodules("streamlit")

# Módulos adicionais que o Streamlit usa por string dinâmica
hiddenimports += [
    "streamlit.web.cli",
    "streamlit.runtime",
    "streamlit.runtime.scriptrunner",
    "streamlit.runtime.scriptrunner.script_runner",
    "streamlit.runtime.state",
    "streamlit.runtime.legacy_caching",
    "streamlit.elements",
    "streamlit.logger",
    "altair",
    "validators",
    "watchdog",
    "tornado",
    "click",
    "packaging",
    "PIL",
    "pyarrow",
    "pyarrow.pandas_compat",
]
