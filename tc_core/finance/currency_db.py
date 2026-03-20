from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict

from tc_core.utils.portabilidade import get_data_root, is_cloud

logger = logging.getLogger(__name__)

_DEFAULTS: Dict[str, float] = {"USD": 5.00, "EUR": 6.4855}


# ---------------------------------------------------------------------------
#  JSON backend (cloud)
# ---------------------------------------------------------------------------

def _json_path() -> str:
    return str(get_data_root() / "taxas_cambio.json")


def _carregar_taxas_json() -> Dict[str, float]:
    path = _json_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            taxas = {k: float(v) for k, v in data.get("taxas", {}).items()}
        except Exception:
            logger.warning("Falha ao ler %s — usando defaults.", path)
            taxas = {}
    else:
        taxas = {}
    for k, v in _DEFAULTS.items():
        taxas.setdefault(k, v)
    return taxas


def _salvar_taxas_json(taxas: Dict[str, float]) -> str:
    path = _json_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload = {
        "taxas": {k: float(v) for k, v in taxas.items()},
        "data_atualizacao": datetime.now().isoformat(),
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return path


# ---------------------------------------------------------------------------
#  SQLite backend (local / EXE)
# ---------------------------------------------------------------------------

def inicializar_banco_taxas(caminho_db: str | None = None) -> str:
    if caminho_db is None:
        caminho_db = os.path.join(os.getcwd(), "taxas_cambio.db")

    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS taxas_cambio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            moeda TEXT NOT NULL,
            taxa_para_brl REAL NOT NULL,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(moeda)
        )
        """
    )
    conn.commit()
    conn.close()
    return caminho_db


# ---------------------------------------------------------------------------
#  API pública (dual mode)
# ---------------------------------------------------------------------------

def carregar_taxas_banco(caminho_db: str | None = None) -> Dict[str, float]:
    if is_cloud():
        return _carregar_taxas_json()

    caminho_db = inicializar_banco_taxas(caminho_db)
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT moeda, taxa_para_brl FROM taxas_cambio ORDER BY data_atualizacao DESC"
    )
    resultados = cursor.fetchall()
    conn.close()

    taxas: Dict[str, float] = {moeda: float(taxa) for moeda, taxa in resultados}

    for k, v in _DEFAULTS.items():
        taxas.setdefault(k, v)
    return taxas


def salvar_taxas_banco(taxas: Dict[str, float], caminho_db: str | None = None) -> str:
    if is_cloud():
        return _salvar_taxas_json(taxas)

    caminho_db = inicializar_banco_taxas(caminho_db)
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()

    agora = datetime.now()
    for moeda, taxa in taxas.items():
        cursor.execute(
            """
            INSERT OR REPLACE INTO taxas_cambio (moeda, taxa_para_brl, data_atualizacao)
            VALUES (?, ?, ?)
            """,
            (moeda, float(taxa), agora),
        )

    conn.commit()
    conn.close()
    return caminho_db
