"""Utilitários de caminho para localização do Excel de entrada."""

from __future__ import annotations

import os
import unicodedata
from pathlib import Path

_TMP_CACHE_ROOT = "/tmp/sci_data_cache"

# DBFS acessível nos notebooks Databricks como /dbfs/... (upload via dbfs/create API)
_DBFS_DATA_ROOT = "/dbfs/sci_data"

# Nomes aceitos para o Excel de entrada (ordem de prioridade).
# REGRA: o nome canônico (com acento e espaço) DEVE ser o primeiro.
# A versão slugified é apenas fallback para compatibilidade retroativa.
EXCEL_CANDIDATES = (
    "Reporting veículos.xlsx",       # canônico (com acento e espaço)
    "Reporting veiculos.xlsx",       # sem acento, com espaço
    "Reporting_veiculos.xlsx",       # slugified (fallback legado)
)


def slugify_filename(name: str) -> str:
    """Remove acentos e substitui espaços por underscore.

    >>> slugify_filename("Reporting veículos.xlsx")
    'Reporting_veiculos.xlsx'
    """
    # Decompor acentos → remover combining marks → recompor
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
    return ascii_name.replace(" ", "_")


def resolve_excel_path(
    ano: int | str,
    data_root: str | None = None,
    dataset_key: str = "TC_Principal",
) -> str:
    """Localiza o Excel de entrada buscando em Workspace e /tmp.

    Ordem de busca:
      1. {data_root}/{dataset_key}/{ano}/{candidato}   (cada nome)
      2. /tmp/sci_data_cache/{dataset_key}/{ano}/{candidato}

    Retorna o primeiro caminho existente (``os.path.exists``).
    Se nenhum existir, levanta ``FileNotFoundError`` com mensagem guiada.
    """
    ano_str = str(ano)

    if data_root is None:
        data_root = os.environ.get(
            "SCI_SHARED_DATA_ROOT",
            str(Path.cwd() / "dados"),
        )

    # Ordem de busca: Workspace → DBFS → /tmp cache
    candidate_dirs = [
        os.path.join(data_root, dataset_key, ano_str),          # Workspace/dados
        os.path.join(_DBFS_DATA_ROOT, dataset_key, ano_str),    # DBFS (upload novo)
        os.path.join(_TMP_CACHE_ROOT, dataset_key, ano_str),    # /tmp cache
    ]

    tested: list[str] = []

    for base in candidate_dirs:
        for candidate in EXCEL_CANDIDATES:
            path = os.path.join(base, candidate)
            tested.append(path)
            if os.path.exists(path):
                if "/tmp" in base:
                    tag = "[FALLBACK /tmp]"
                elif base.startswith(_DBFS_DATA_ROOT):
                    tag = "[DBFS]"
                else:
                    tag = "[Workspace]"
                print(f"[INFO] Excel encontrado {tag}: {path}")
                return path

    raise FileNotFoundError(
        "Excel de entrada não encontrado. Caminhos testados:\n"
        + "\n".join(f"  - {p}" for p in tested)
        + "\n\nUse o uploader do app para enviar o arquivo, "
        + "ou copie manualmente para uma das pastas acima."
    )
