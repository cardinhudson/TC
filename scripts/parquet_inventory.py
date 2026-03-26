"""
scripts/parquet_inventory.py
============================
Inventário automático de todos os parquets do SCI.

Percorre ``dados/`` recursivamente, lista cada .parquet com:
  - nome, path relativo, tamanho, schema (colunas)
  - referência cruzada com código-fonte (.py)
  - classificação: USED / NOT_FOUND_IN_CODE / LEGACY

Uso:
    python scripts/parquet_inventory.py
    python scripts/parquet_inventory.py --json inventory.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Projeto na raiz
ROOT = Path(__file__).resolve().parent.parent
DADOS_DIR = ROOT / "dados"


def _tamanho_humano(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def listar_parquets(pasta: Path) -> list[dict]:
    """Lista todos os .parquet dentro de ``pasta``."""
    resultados = []
    if not pasta.exists():
        return resultados
    for p in sorted(pasta.rglob("*.parquet")):
        try:
            import pandas as pd
            cols = list(pd.read_parquet(p, columns=[]).columns)
        except Exception:
            cols = []
        resultados.append({
            "nome": p.name,
            "nome_sem_ext": p.stem,
            "path_relativo": str(p.relative_to(ROOT)),
            "tamanho_bytes": p.stat().st_size,
            "tamanho_humano": _tamanho_humano(p.stat().st_size),
            "colunas": cols,
            "num_colunas": len(cols),
        })
    return resultados


def buscar_referencias_codigo(nome_sem_ext: str) -> list[str]:
    """Busca referências a um parquet nos .py do projeto."""
    refs = []
    # Escapar caracteres especiais para regex
    pattern = re.compile(re.escape(nome_sem_ext), re.IGNORECASE)
    for py_file in sorted(ROOT.rglob("*.py")):
        # Pular __pycache__ e .venv
        rel = str(py_file.relative_to(ROOT))
        if "__pycache__" in rel or ".venv" in rel or "build" in rel:
            continue
        try:
            text = py_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if pattern.search(text):
            refs.append(rel)
    return refs


def classificar(refs: list[str], nome: str) -> str:
    """Classifica status do parquet."""
    # Parquets thin/agg são derivados, não órfãos
    if any(tag in nome for tag in ("_thin", "_agg_home", "_agg")):
        return "DERIVED"
    if not refs:
        return "NOT_FOUND_IN_CODE"
    # Se só referenciado em processamento (escrita) mas não em pages/shared
    pages_or_shared = [r for r in refs if "pages/" in r or "shared.py" in r or "alert" in r]
    if not pages_or_shared:
        return "PIPELINE_ONLY"
    return "USED"


def run(output_json: str | None = None):
    parquets = listar_parquets(DADOS_DIR)
    print(f"\n{'='*80}")
    print(f"  INVENTÁRIO DE PARQUETS — SCI")
    print(f"  Pasta: {DADOS_DIR}")
    print(f"  Total: {len(parquets)} arquivos")
    print(f"{'='*80}\n")

    resultados = []
    for pq in parquets:
        refs = buscar_referencias_codigo(pq["nome_sem_ext"])
        status = classificar(refs, pq["nome"])
        pq["referencias"] = refs
        pq["status"] = status
        resultados.append(pq)

    # Tabela markdown
    print(f"| {'Status':<20} | {'Nome':<45} | {'Tamanho':>10} | {'Cols':>4} | {'Refs':>4} |")
    print(f"|{'-'*22}|{'-'*47}|{'-'*12}|{'-'*6}|{'-'*6}|")
    for r in sorted(resultados, key=lambda x: x["status"]):
        print(f"| {r['status']:<20} | {r['nome']:<45} | {r['tamanho_humano']:>10} | {r['num_colunas']:>4} | {len(r['referencias']):>4} |")

    # Resumo
    from collections import Counter
    contagem = Counter(r["status"] for r in resultados)
    print(f"\n--- Resumo ---")
    for status, count in sorted(contagem.items()):
        print(f"  {status}: {count}")

    # Órfãos
    orfaos = [r for r in resultados if r["status"] in ("NOT_FOUND_IN_CODE", "PIPELINE_ONLY")]
    if orfaos:
        print(f"\n--- Candidatos a Órfãos ({len(orfaos)}) ---")
        for r in orfaos:
            print(f"  {r['path_relativo']} ({r['tamanho_humano']}) [{r['status']}]")
            if r["referencias"]:
                for ref in r["referencias"]:
                    print(f"    └─ {ref}")

    if output_json:
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        print(f"\n✅ JSON salvo: {output_json}")

    return resultados


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inventário de parquets SCI")
    parser.add_argument("--json", type=str, help="Caminho para salvar JSON")
    args = parser.parse_args()
    run(args.json)
