import base64
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import streamlit as st

from tc_core.utils.portabilidade import get_base_path, get_data_root


DEFAULT_WORKSPACE_REPO_ROOT = Path(
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
)

_CLOUD_DATA_CACHE = Path("/tmp/sci_data_cache")


def _configure_cloud_data_root() -> None:
    """Resolve a pasta compartilhada de dados antes dos imports das páginas.

    Ordem de tentativa quando SCI_SHARED_DATA_ROOT está definida:
      1. Acesso direto ao filesystem (cluster, Workspace FUSE mount)
      2. Volumes UC  /  DBFS  (resolve_data_root)
      3. Mirror via Databricks REST API → cache local em /tmp/sci_data_cache/
    """
    env_root = os.environ.get("SCI_SHARED_DATA_ROOT")

    if env_root:
        # ── 1. Acesso direto ao path configurado ──────────────────────
        if os.path.isdir(env_root):
            return  # Tudo OK — nada a fazer

        # O path configurado NÃO é acessível via filesystem.
        # Isso acontece em Databricks Apps onde o container só monta
        # seu próprio diretório de código-fonte.
        from tc_core.utils.portabilidade import (
            cloud_path_exists,
            mirror_workspace_tree,
            resolve_data_root,
        )

        _log = lambda msg: print(msg, file=sys.stderr)  # noqa: E731

        _log(f"[SCI] _configure_cloud_data_root: env_root={env_root}")
        _log(f"[SCI]   os.path.isdir={os.path.isdir(env_root)}")
        _log(f"[SCI]   DATABRICKS_HOST={os.environ.get('DATABRICKS_HOST', '(vazio)')}")
        _log(f"[SCI]   DATABRICKS_TOKEN presente={bool(os.environ.get('DATABRICKS_TOKEN'))}")
        _log(f"[SCI]   DATABRICKS_CLIENT_ID={os.environ.get('DATABRICKS_CLIENT_ID', '(vazio)')}")

        # ── 2. Tentar Volumes / DBFS ──────────────────────────────────
        try:
            resolved = resolve_data_root(log=_log)
            os.environ["SCI_SHARED_DATA_ROOT"] = str(resolved)
            _log(f"[SCI] DATA_ROOT resolvido via Volumes/DBFS: {resolved}")
            return
        except FileNotFoundError:
            _log("[SCI]   Volumes/DBFS não encontrados — tentando API/SDK")

        # ── 3. Mirror via Workspace API (SDK ou REST) ─────────────────
        _log(f"[SCI]   Verificando cloud_path_exists({env_root})...")
        exists = cloud_path_exists(env_root)
        _log(f"[SCI]   cloud_path_exists={exists}")
        if exists:
            _log(f"[SCI] Workspace path existe via API — iniciando mirror: {env_root}")
            ok = mirror_workspace_tree(env_root, _CLOUD_DATA_CACHE, log=_log)
            if ok:
                if not os.environ.get("SCI_WORKSPACE_DATA_ROOT"):
                    os.environ["SCI_WORKSPACE_DATA_ROOT"] = env_root
                os.environ["SCI_SHARED_DATA_ROOT"] = str(_CLOUD_DATA_CACHE)
                _log(f"[SCI] DATA_ROOT redirecionado para cache local: {_CLOUD_DATA_CACHE}")
                return
            else:
                _log("[SCI]   mirror_workspace_tree retornou False")
        elif _CLOUD_DATA_CACHE.exists() and any(_CLOUD_DATA_CACHE.iterdir()):
            # Cache local sobreviveu de execução anterior
            if not os.environ.get("SCI_WORKSPACE_DATA_ROOT"):
                os.environ["SCI_WORKSPACE_DATA_ROOT"] = env_root
            os.environ["SCI_SHARED_DATA_ROOT"] = str(_CLOUD_DATA_CACHE)
            _log(f"[SCI] Usando cache local existente: {_CLOUD_DATA_CACHE}")
            return

        # Mantém o valor original como último recurso
        _log(f"[SCI] ⚠ Nenhuma alternativa encontrada — mantendo {env_root}")
        return

    # ── Auto-discovery (sem env var definida) ─────────────────────────
    base_path = get_base_path()
    base_path_str = str(base_path)
    in_workspace = base_path_str.startswith("/Workspace/Users/")
    if not in_workspace and not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return

    candidates = []
    if base_path.name == "sci_app":
        candidates.append(base_path.parent / "sci" / "dados")
    candidates.append(DEFAULT_WORKSPACE_REPO_ROOT / "dados")
    candidates.append(base_path / "dados")

    selected = candidates[0]
    for candidate in candidates:
        try:
            if candidate.exists():
                selected = candidate
                break
        except OSError:
            continue

    os.environ["SCI_SHARED_DATA_ROOT"] = str(selected)
    os.environ.setdefault("SCI_CLOUD", "1")


_configure_cloud_data_root()


def render_home_tc():
    from tc_principal.pages.home_tc import render
    return render()


def render_extracao_dados_tc():
    from tc_principal.pages.extracao_dados_tc import render
    return render()


def render_debug_calculos_tc():
    from tc_principal.pages.debug_calculos_tc import render
    return render()


def render_copilot():
    from tc_copilot.pages.home_copilot import render
    return render()


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

# Botão para limpar cache (substitui o menu nativo do Streamlit Cloud)
if st.sidebar.button("🗑️ Limpar Cache", use_container_width=True, help="Limpa todos os dados em cache e recarrega a página"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.sidebar.success("✅ Cache limpo!")
    st.rerun()

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
            "pages/1_Waterfall.py",
            title="Waterfall",
            url_path="tc-ext-waterfall",
        ),
        st.Page(
            "pages/2_Best_Estimate.py",
            title="Best Estimate (Simulador)",
            url_path="tc-ext-best-estimate-simulador",
        ),
        st.Page(
            "pages/5_Extracao_Dados.py",
            title="Extração de Dados",
            url_path="tc-ext-extracao",
        ),
    ],
    "Documentação": [
        st.Page(
            "pages/6_Documentacao.py",
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
