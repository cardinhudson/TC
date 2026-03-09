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
