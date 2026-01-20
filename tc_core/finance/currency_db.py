from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Dict


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


def carregar_taxas_banco(caminho_db: str | None = None) -> Dict[str, float]:
    caminho_db = inicializar_banco_taxas(caminho_db)
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT moeda, taxa_para_brl FROM taxas_cambio ORDER BY data_atualizacao DESC"
    )
    resultados = cursor.fetchall()
    conn.close()

    taxas: Dict[str, float] = {moeda: float(taxa) for moeda, taxa in resultados}

    # Defaults
    taxas.setdefault("USD", 5.00)
    taxas.setdefault("EUR", 5.50)
    return taxas


def salvar_taxas_banco(taxas: Dict[str, float], caminho_db: str | None = None) -> str:
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
