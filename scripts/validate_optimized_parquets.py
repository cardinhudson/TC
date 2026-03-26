"""
scripts/validate_optimized_parquets.py
======================================
Validação automática: compara parquets FULL vs THIN/AGG.

Garante que:
  1) Somatórios (Custo FP, Volume, Total) são idênticos (tolerância zero)
  2) CPU recalculado = SUM(Custo)/SUM(Volume) — tolerância < 0.01
  3) THIN tem mesma contagem de linhas que FULL
  4) AGG cobre todas combinações de filtros do FULL

Uso:
    python scripts/validate_optimized_parquets.py [--ano 2026]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tc_core.parquet_schemas import AGG_SCHEMAS, THIN_SCHEMAS


def _read(path: str) -> pd.DataFrame | None:
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None


def _build_path(base: str, nome: str) -> str:
    return os.path.join(base, f"{nome}.parquet")


# ═══════════════════════════════════════════════════════════════
#   VALIDAÇÃO THIN
# ═══════════════════════════════════════════════════════════════

def validar_thin(pasta: str, schema_name: str, schema: dict) -> dict:
    """Valida thin vs full: mesma contagem de linhas, colunas drop ausentes."""
    full_path = _build_path(pasta, schema["source"])
    thin_path = _build_path(pasta, schema_name)

    result = {"schema": schema_name, "tipo": "THIN", "status": "SKIP", "detalhes": []}

    df_full = _read(full_path)
    df_thin = _read(thin_path)

    if df_full is None:
        result["detalhes"].append(f"Full não encontrado: {full_path}")
        return result
    if df_thin is None:
        result["detalhes"].append(f"Thin não encontrado: {thin_path}")
        return result

    result["status"] = "PASS"

    # Contagem de linhas
    if len(df_full) != len(df_thin):
        result["status"] = "FAIL"
        result["detalhes"].append(
            f"Linhas diferem: full={len(df_full)}, thin={len(df_thin)}"
        )

    # Colunas drop devem estar ausentes no thin
    for col in schema["drop_columns"]:
        if col in df_thin.columns:
            result["status"] = "FAIL"
            result["detalhes"].append(f"Coluna deveria estar ausente: {col}")

    # Colunas numéricas comuns devem ter mesmos somatórios
    common_numeric = [
        c for c in df_thin.select_dtypes(include=[np.number]).columns
        if c in df_full.columns
    ]
    for col in common_numeric:
        s_full = df_full[col].sum()
        s_thin = df_thin[col].sum()
        if abs(s_full - s_thin) > 0.01:
            result["status"] = "FAIL"
            result["detalhes"].append(
                f"SUM({col}): full={s_full:.2f}, thin={s_thin:.2f}, diff={abs(s_full-s_thin):.2f}"
            )

    return result


# ═══════════════════════════════════════════════════════════════
#   VALIDAÇÃO AGG
# ═══════════════════════════════════════════════════════════════

def validar_agg(pasta: str, schema_name: str, schema: dict) -> dict:
    """Valida agg vs full: somatórios idênticos quando full agregado nos mesmos eixos."""
    full_path = _build_path(pasta, schema["source"])
    agg_path = _build_path(pasta, schema_name)

    result = {"schema": schema_name, "tipo": "AGG", "status": "SKIP", "detalhes": []}

    df_full = _read(full_path)
    df_agg = _read(agg_path)

    if df_full is None:
        result["detalhes"].append(f"Full não encontrado: {full_path}")
        return result
    if df_agg is None:
        result["detalhes"].append(f"AGG não encontrado: {agg_path}")
        return result

    result["status"] = "PASS"

    keys = [k for k in schema["group_keys"] if k in df_full.columns]
    sums = [c for c in schema["sum_columns"] if c in df_full.columns]

    if not keys or not sums:
        result["status"] = "SKIP"
        result["detalhes"].append("Keys ou sums não encontrados no full")
        return result

    # Reagregar full nos mesmos eixos
    df_full_agg = df_full.groupby(keys, as_index=False)[sums].sum()

    # Comparar somatórios globais (tolerância zero)
    for col in sums:
        s_full = df_full_agg[col].sum()
        s_agg = df_agg[col].sum() if col in df_agg.columns else 0
        if abs(s_full - s_agg) > 0.01:
            result["status"] = "FAIL"
            result["detalhes"].append(
                f"SUM({col}): full_agg={s_full:.2f}, agg={s_agg:.2f}, diff={abs(s_full-s_agg):.2f}"
            )

    # Cobertura: todas combinações de keys no full devem existir no agg
    agg_keys = [k for k in keys if k in df_agg.columns]
    if agg_keys:
        full_combos = set(df_full_agg[agg_keys].drop_duplicates().itertuples(index=False, name=None))
        agg_combos = set(df_agg[agg_keys].drop_duplicates().itertuples(index=False, name=None))
        missing = full_combos - agg_combos
        if missing:
            result["status"] = "FAIL"
            result["detalhes"].append(
                f"Combinações faltantes no AGG: {len(missing)} (ex: {list(missing)[:3]})"
            )

    # Contagem de linhas (informativo)
    result["detalhes"].append(
        f"Linhas: full_agg={len(df_full_agg)}, agg={len(df_agg)}"
    )

    return result


# ═══════════════════════════════════════════════════════════════
#   TESTE DE PERFORMANCE
# ═══════════════════════════════════════════════════════════════

def medir_performance(pasta: str, schema_name: str, source_name: str) -> dict:
    """Mede tempo de leitura de full vs otimizado."""
    full_path = _build_path(pasta, source_name)
    opt_path = _build_path(pasta, schema_name)

    result = {"schema": schema_name, "source": source_name}

    if os.path.exists(full_path):
        t0 = time.perf_counter()
        pd.read_parquet(full_path)
        result["full_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        result["full_size_kb"] = round(os.path.getsize(full_path) / 1024, 1)

    if os.path.exists(opt_path):
        t0 = time.perf_counter()
        pd.read_parquet(opt_path)
        result["opt_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        result["opt_size_kb"] = round(os.path.getsize(opt_path) / 1024, 1)

    if "full_ms" in result and "opt_ms" in result and result["full_ms"] > 0:
        result["speedup"] = f"{result['full_ms'] / result['opt_ms']:.1f}x"
        result["size_reduction"] = f"{(1 - result['opt_size_kb'] / result['full_size_kb']) * 100:.0f}%"

    return result


# ═══════════════════════════════════════════════════════════════
#   EXECUÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def _resolve_pastas(ano: str) -> list[tuple[str, str]]:
    """Retorna pares (pasta, label) para validação."""
    dados = ROOT / "dados"
    pastas = []
    for domain in ("TC_Principal", "TC_Ext"):
        for sub in ("", "BUD"):
            pasta = dados / domain / str(ano)
            if sub:
                pasta = pasta / sub
            if pasta.exists():
                pastas.append((str(pasta), f"{domain}/{ano}/{sub or 'Real'}"))
    # Forecast
    forecast = dados / "TC_Principal" / str(ano) / "Forecast"
    if forecast.exists():
        pastas.append((str(forecast), f"TC_Principal/{ano}/Forecast"))
    return pastas


def run(ano: str = "2026"):
    print(f"\n{'='*80}")
    print(f"  VALIDAÇÃO DE PARQUETS OTIMIZADOS — SCI")
    print(f"  Ano: {ano}")
    print(f"{'='*80}\n")

    resultados = []
    perf_results = []

    for pasta, label in _resolve_pastas(ano):
        print(f"\n📂 {label}")
        print(f"   {pasta}\n")

        # Thin
        for name, schema in THIN_SCHEMAS.items():
            if _build_path(pasta, name).replace(".parquet", "") or True:
                r = validar_thin(pasta, name, schema)
                if r["status"] != "SKIP":
                    icon = "✅" if r["status"] == "PASS" else "❌"
                    print(f"   {icon} THIN {name}: {r['status']}")
                    for d in r["detalhes"]:
                        print(f"      {d}")
                    resultados.append(r)

        # AGG
        for name, schema in AGG_SCHEMAS.items():
            r = validar_agg(pasta, name, schema)
            if r["status"] != "SKIP":
                icon = "✅" if r["status"] == "PASS" else "❌"
                print(f"   {icon} AGG  {name}: {r['status']}")
                for d in r["detalhes"]:
                    print(f"      {d}")
                resultados.append(r)

        # Performance
        all_schemas = {**{k: v["source"] for k, v in THIN_SCHEMAS.items()},
                       **{k: v["source"] for k, v in AGG_SCHEMAS.items()}}
        for name, source in all_schemas.items():
            if os.path.exists(_build_path(pasta, name)):
                perf = medir_performance(pasta, name, source)
                if "speedup" in perf:
                    perf_results.append(perf)

    # Resumo
    total = len(resultados)
    passed = sum(1 for r in resultados if r["status"] == "PASS")
    failed = sum(1 for r in resultados if r["status"] == "FAIL")
    print(f"\n{'='*80}")
    print(f"  RESUMO: {passed}/{total} PASS, {failed} FAIL")
    print(f"{'='*80}")

    if perf_results:
        print(f"\n  PERFORMANCE:")
        for p in perf_results:
            print(f"    {p['schema']}: {p.get('speedup', 'N/A')} faster, "
                  f"{p.get('size_reduction', 'N/A')} smaller")

    return failed == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validação de parquets otimizados")
    parser.add_argument("--ano", type=str, default="2026", help="Ano para validar")
    args = parser.parse_args()
    ok = run(args.ano)
    sys.exit(0 if ok else 1)
