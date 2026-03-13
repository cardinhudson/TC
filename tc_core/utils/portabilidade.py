"""
tc_core/utils/portabilidade.py
==============================
Utilitários de portabilidade para execução dual (Dev ↔ EXE).

Regras:
  - Dev  (python app.py):  caminhos relativos à raiz do repositório.
  - EXE  (PyInstaller):    TUDO dentro de _internal/ (sys._MEIPASS).
                            Tanto leitura (dados/) quanto escrita (JSONs)
                            operam sobre sys._MEIPASS, que é uma pasta
                            normal e gravável em one-dir.

Uso:
    from tc_core.utils.portabilidade import get_base_path, get_assets_path, IS_FROZEN
"""
from __future__ import annotations

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Flag: estamos rodando dentro de um bundle PyInstaller?
# ---------------------------------------------------------------------------
IS_FROZEN: bool = getattr(sys, "frozen", False)


def _get_env_data_root() -> Path | None:
    """Retorna o diretório de dados compartilhados definido por ambiente, se houver."""
    raw = (sys.environ.get("SCI_SHARED_DATA_ROOT") if hasattr(sys, "environ") else None)
    if not raw:
        import os
        raw = os.environ.get("SCI_SHARED_DATA_ROOT")
    if not raw:
        return None
    return Path(raw).expanduser()


def get_base_path() -> Path:
    """
    Retorna o diretório base para dados, configs e assets.

    - Dev : raiz do repositório (pasta que contém app.py)
    - EXE : sys._MEIPASS (_internal/) — onde o PyInstaller extrai
            TODOS os datas, incluindo dados/, páginas e JSONs.
            A pasta _internal/ é gravável em one-dir, então
            versao.json, dados_equipe.json etc. também funcionam.
    """
    if IS_FROZEN:
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    # __file__ = .../tc_core/utils/portabilidade.py → dois levels acima = raiz
    return Path(__file__).resolve().parents[2]


def get_data_root() -> Path:
    """
    Retorna a pasta base de dados (`dados/`).

    - Padrão: `<base_path>/dados`
    - Override: variável de ambiente `SCI_SHARED_DATA_ROOT`
    """
    env_root = _get_env_data_root()
    if env_root is not None:
        return env_root
    return get_base_path() / "dados"


def is_shared_data_override_active() -> bool:
    """Indica se o app está usando uma raiz de dados compartilhada externa."""
    return _get_env_data_root() is not None


def get_assets_path() -> Path:
    """
    Retorna o diretório de assets estáticos bundled (imagens, .streamlit/).
    Idêntico a get_base_path() — ambos apontam para o mesmo lugar.
    """
    return get_base_path()


def get_output_path(subdir: str = "") -> Path:
    """
    Retorna o diretório para escrita de artefatos (logs, exports).

    - Dev : raiz do repositório
    - EXE : sys._MEIPASS (_internal/) — gravável em one-dir

    Exemplo:
        get_output_path("logs")  →  _internal/logs/
    """
    base = get_base_path()
    if subdir:
        path = base / subdir
        path.mkdir(parents=True, exist_ok=True)
        return path
    return base


def resolve_path(relativo: str) -> Path:
    """
    Converte um caminho relativo (ex: 'dados/TC_Principal') para absoluto,
    usando get_base_path() como âncora.  Funciona igual nos dois modos.

    Uso:
        caminho = resolve_path("dados/TC_Principal/2025/BUD")
    """
    return get_base_path() / relativo


def resolve_data_path(*parts: str) -> Path:
    """Resolve caminhos relativos à pasta de dados efetiva."""
    return get_data_root().joinpath(*parts)
