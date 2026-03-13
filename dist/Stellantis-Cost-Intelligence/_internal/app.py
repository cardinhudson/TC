import base64
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import streamlit as st

from tc_core.utils.portabilidade import get_base_path, get_data_root
from tc_principal.pages.home_tc import render as render_home_tc

from tc_principal.pages.extracao_dados_tc import (
    render as render_extracao_dados_tc,
)
from tc_principal.pages.debug_calculos_tc import (
    render as render_debug_calculos_tc,
)
from tc_copilot.pages.home_copilot import render as render_copilot


def _iter_startup_files(data_root: Path, assets_root: Path) -> list[Path]:
    roots = [data_root, assets_root / ".streamlit"]
    allowed_exts = {".parquet", ".json", ".toml", ".png"}
    priority_names = {
        "df_principal_BUD.parquet",
        "df_principal.parquet",
        "df_vol_veiculos_BUD.parquet",
        "df_vol_veiculos.parquet",
        "df_final_historico.parquet",
        "df_vol_historico.parquet",
        "df_final_historico_BUD.parquet",
        "df_vol_historico_BUD.parquet",
        "SCI_faixa.png",
        "config.toml",
    }
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in allowed_exts:
                files.append(path)
    files.sort(key=lambda item: (item.name not in priority_names, len(str(item))))
    return files


def _warm_file(path: Path) -> None:
    size = path.stat().st_size
    bytes_to_read = size if size <= 5_000_000 else min(size, 1_000_000)
    with path.open("rb") as handle:
        handle.read(bytes_to_read)


@st.cache_resource(show_spinner=False)
def start_background_warmup(data_root_str: str, assets_root_str: str):
    status = {
        "done": False,
        "files": 0,
        "errors": 0,
        "started_at": time.time(),
        "finished_at": None,
    }

    def worker() -> None:
        data_root = Path(data_root_str)
        assets_root = Path(assets_root_str)
        files = _iter_startup_files(data_root, assets_root)
        status["files"] = len(files)
        max_workers = min(8, max(2, os.cpu_count() or 4))
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for path in files:
                    executor.submit(_warm_file, path)
        except Exception:
            status["errors"] += 1
        finally:
            status["done"] = True
            status["finished_at"] = time.time()

    threading.Thread(target=worker, name="sci-startup-warmup", daemon=True).start()
    return status


st.set_page_config(
    page_title="SCI | Stellantis Cost Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

base_url = st.get_option("server.baseUrlPath") or ""
if base_url:
    base_url = "/" + base_url.strip("/")
target_url = f"{base_url}/" if base_url else "/"
host = st.get_option("server.address") or "localhost"
port = st.get_option("server.port") or 8501
browser_url = f"http://{host}:{port}{target_url}"
warmup_status = start_background_warmup(str(get_data_root()), str(get_base_path()))

# Faixa no sidebar (proporção original, sem cortes)
faixa_path = get_base_path() / "SCI_faixa.png"
if faixa_path.exists():
    faixa_b64 = base64.b64encode(faixa_path.read_bytes()).decode("ascii")
    st.sidebar.markdown(
        f"""
        <style>
            .sci-faixa-btn {{
                display: block;
                border-radius: 6px;
                transition: transform 0.15s ease, box-shadow 0.15s ease, filter 0.15s ease;
                outline: 1px solid transparent;
            }}
            .sci-faixa-btn:hover {{
                transform: translateY(-3px) scale(1.02);
                box-shadow: 0 14px 30px rgba(0, 0, 0, 0.5);
                filter: saturate(1.28) brightness(1.12) contrast(1.08);
                outline: 3px solid rgba(255, 255, 255, 0.45);
            }}
        </style>
        <a href="{target_url}" class="sci-faixa-btn" style="text-decoration: none;">
            <div style="width: 100%; margin: 0 0 0.5rem 0;">
                <img
                    src="data:image/png;base64,{faixa_b64}"
                    alt="SCI"
                    style="width: 100%; height: auto; display: block; border-radius: 6px;"
                />
            </div>
        </a>
        """,
        unsafe_allow_html=True,
    )

st.sidebar.markdown(
    f"""
    <a href="{browser_url}" target="_blank" style="text-decoration: none;">
        <div style="width: 100%; padding: 0.15rem 0; margin: 0 0 0.6rem 0; text-align: center;">
            <span style="text-decoration: underline; font-weight: 700; color: #0f3460;">
                Abrir no navegador
            </span>
        </div>
    </a>
    """,
    unsafe_allow_html=True,
)

if not warmup_status.get("done"):
    st.sidebar.caption("Aquecimento inicial de arquivos em andamento para acelerar a primeira navegação.")

# Título principal (restaurado)
st.markdown(
    """
    <div style="display:flex; align-items:center; gap:12px; margin: 0 0 0.35rem 0;">
        <div style="width:46px; height:46px; display:flex; align-items:center; justify-content:center;">
            <svg width="44" height="44" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="AI">
                <rect x="20" y="20" width="24" height="24" rx="5" stroke="currentColor" stroke-width="3" opacity="0.85"/>
                <path d="M26 16v4M32 16v4M38 16v4" stroke="currentColor" stroke-width="3" stroke-linecap="round" opacity="0.75"/>
                <path d="M26 44v4M32 44v4M38 44v4" stroke="currentColor" stroke-width="3" stroke-linecap="round" opacity="0.75"/>
                <path d="M16 26h4M16 32h4M16 38h4" stroke="currentColor" stroke-width="3" stroke-linecap="round" opacity="0.75"/>
                <path d="M44 26h4M44 32h4M44 38h4" stroke="currentColor" stroke-width="3" stroke-linecap="round" opacity="0.75"/>
                <circle cx="32" cy="32" r="4" fill="currentColor" opacity="0.85"/>
                <circle cx="26" cy="26" r="2.5" fill="currentColor" opacity="0.75"/>
                <circle cx="38" cy="26" r="2.5" fill="currentColor" opacity="0.75"/>
                <circle cx="26" cy="38" r="2.5" fill="currentColor" opacity="0.75"/>
                <circle cx="38" cy="38" r="2.5" fill="currentColor" opacity="0.75"/>
                <path d="M28.5 28.5L30 30M35.5 28.5L34 30M28.5 35.5L30 34M35.5 35.5L34 34" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" opacity="0.8"/>
            </svg>
        </div>
        <div style="line-height: 1.05;">
            <div style="font-size: 2.1rem; font-weight: 800; margin: 0;">Stellantis Cost Intelligence (SCI)</div>
            <div style="font-size: 1.05rem; opacity: 0.85; margin-top: 0.15rem;">— A Evolução da Controladoria Industrial —</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.caption("Selecione o módulo no menu lateral.")

PAGES = {
    "TC Veículos": [
        st.Page(render_home_tc, title="Home (TC Veículos)", url_path="tc"),
        st.Page(
            "tc_principal/pages/waterfall_tc.py",
            title="Waterfall",
            url_path="tc-waterfall",
        ),
        st.Page(
            "tc_principal/pages/best_estimate_simulador_tc.py",
            title="Best Estimate (Simulador)",
            url_path="tc-best-estimate-simulador",
        ),
        st.Page(
            render_extracao_dados_tc,
            title="Extração de Dados",
            url_path="tc-extracao",
        ),
        st.Page(
            render_debug_calculos_tc,
            title="Debug de Cálculos",
            url_path="tc-debug",
        ),
    ],
    "TC Ext (Linhas Secundárias)": [
        st.Page(
            "tc_ext/pages/home_ext.py",
            title="Home (TC Ext)",
            url_path="tc-ext",
        ),
        st.Page(
            "pages/1 - Waterfall.py",
            title="Waterfall",
            url_path="tc-ext-waterfall",
        ),
        st.Page(
            "pages/2 - Best Estimate - Simulador.py",
            title="Best Estimate (Simulador)",
            url_path="tc-ext-best-estimate-simulador",
        ),
        st.Page(
            "pages/5 - Extração de Dados.py",
            title="Extração de Dados",
            url_path="tc-ext-extracao",
        ),
    ],
    "Documentação": [
        st.Page(
            "pages/6 - Documentacao.py",
            title="Documentação (Projeto)",
            url_path="documentacao",
        ),
    ],
    "TC Copilot": [
        st.Page(
            render_copilot,
            title="TC Copilot",
            url_path="tc-copilot",
        ),
    ],
    "Central de Alertas": [
        st.Page(
            "alertas/alert_ui.py",
            title="Monitoramento",
            url_path="alertas",
        ),
        st.Page(
            "alertas/alert_config_ui.py",
            title="Configuração de Alertas",
            url_path="alertas-config",
        ),
    ],
}

pg = st.navigation(PAGES, expanded=False)
pg.run()
