"""
tc_core/manifest.py
===================
Geração de manifest JSON para auditoria de parquets derivados.

Cada execução do pipeline de processamento gera um manifest contendo:
  - Timestamp, commit hash, arquivos de entrada
  - Para cada parquet: nome, linhas, colunas, checksum de somatório
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _git_commit_hash() -> str:
    """Retorna o short commit hash atual ou 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _checksum_series(series) -> str:
    """MD5 do somatório de uma série numérica (para auditoria rápida)."""
    total = float(series.sum()) if len(series) > 0 else 0.0
    return hashlib.md5(f"{total:.6f}".encode()).hexdigest()


def registrar_parquet(nome: str, df, col_auditoria: str = "Custo FP") -> dict[str, Any]:
    """Cria registro de manifest para um DataFrame."""
    info: dict[str, Any] = {
        "nome": nome,
        "linhas": len(df),
        "colunas": list(df.columns),
        "num_colunas": len(df.columns),
    }
    if col_auditoria and col_auditoria in df.columns:
        info["soma_auditoria"] = {
            "coluna": col_auditoria,
            "valor": round(float(df[col_auditoria].sum()), 2),
            "checksum": _checksum_series(df[col_auditoria]),
        }
    return info


def gerar_manifest(
    pasta: str,
    parquets_info: list[dict[str, Any]],
    source_files: list[str] | None = None,
) -> str:
    """Gera e salva manifest.json na pasta indicada.

    Parameters
    ----------
    pasta : str
        Diretório onde salvar o manifest.
    parquets_info : list[dict]
        Lista de registros (saída de ``registrar_parquet``).
    source_files : list[str] | None
        Caminhos dos arquivos de entrada (Excel, etc.).

    Returns
    -------
    str
        Caminho completo do manifest salvo.
    """
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commit_hash": _git_commit_hash(),
        "source_files": source_files or [],
        "parquets": parquets_info,
    }

    caminho = os.path.join(pasta, "manifest.json")
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    return caminho
