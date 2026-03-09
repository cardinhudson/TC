"""
TC Copilot — Banco de dados SQLite para metadados de PDFs gerados.

Registra PDFs mensais e anuais para download rápido na UI.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any


_DEFAULT_DB = os.path.join(os.getcwd(), "relatorios.db")


def _caminho_db(caminho_db: str | None = None) -> str:
    return caminho_db or _DEFAULT_DB


def inicializar_banco_relatorios(caminho_db: str | None = None) -> str:
    """Cria a tabela se não existir. Retorna o caminho do banco."""
    db = _caminho_db(caminho_db)
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS relatorios_pdf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            modo TEXT NOT NULL,
            moeda TEXT NOT NULL DEFAULT 'EUR',
            caminho TEXT NOT NULL,
            tamanho_bytes INTEGER NOT NULL DEFAULT 0,
            gerado_em TEXT NOT NULL
        )
    """)
    # Índice único: um PDF por (ano, mes, modo)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_relatorio_unico
        ON relatorios_pdf (ano, mes, modo)
    """)
    conn.commit()
    conn.close()
    return db


def registrar_pdf(
    ano: int,
    mes: int,
    modo: str,
    moeda: str,
    caminho: str,
    caminho_db: str | None = None,
) -> None:
    """Registra ou atualiza metadados de um PDF gerado.

    Args:
        ano: Ano do relatório.
        mes: Mês (1-12) ou 0 para o PDF anual consolidado.
        modo: 'local' ou 'ia'.
        moeda: 'BRL', 'USD' ou 'EUR'.
        caminho: Caminho absoluto do PDF no disco.
    """
    db = _caminho_db(caminho_db)
    inicializar_banco_relatorios(db)

    tamanho = 0
    if os.path.exists(caminho):
        tamanho = os.path.getsize(caminho)

    agora = datetime.now().isoformat()

    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO relatorios_pdf
            (ano, mes, modo, moeda, caminho, tamanho_bytes, gerado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ano, mes, modo, moeda, caminho, tamanho, agora))
    conn.commit()
    conn.close()


def listar_pdfs(
    ano: int,
    modo: str,
    caminho_db: str | None = None,
) -> list[dict[str, Any]]:
    """Lista todos os PDFs de um ano/modo, ordenados por mês.

    Retorna lista de dicts com chaves:
        ano, mes, modo, moeda, caminho, tamanho_bytes, gerado_em
    """
    db = _caminho_db(caminho_db)
    inicializar_banco_relatorios(db)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ano, mes, modo, moeda, caminho, tamanho_bytes, gerado_em
        FROM relatorios_pdf
        WHERE ano = ? AND modo = ?
        ORDER BY mes
    """, (ano, modo))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def obter_pdf(
    ano: int,
    mes: int,
    modo: str,
    caminho_db: str | None = None,
) -> dict[str, Any] | None:
    """Retorna metadados de um PDF específico, ou None se não existir."""
    db = _caminho_db(caminho_db)
    inicializar_banco_relatorios(db)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ano, mes, modo, moeda, caminho, tamanho_bytes, gerado_em
        FROM relatorios_pdf
        WHERE ano = ? AND mes = ? AND modo = ?
    """, (ano, mes, modo))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None
