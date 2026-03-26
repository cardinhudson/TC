"""
tc_core/data_router.py
======================
DataSource Router — escolhe automaticamente entre parquets otimizados
(thin / agg) e full, com fallback transparente.

Controlado pela feature-flag ``SCI_USE_OPTIMIZED_PARQUETS``.
Quando desligada (default), todas as leituras vão para o parquet original.

Uso
---
>>> from tc_core.data_router import read_optimized
>>> df = read_optimized("TC_Principal", "2026", "BUD",
...                     "df_principal_BUD", prefer="agg")
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Sequence

import pandas as pd

from tc_core.feature_flags import get_flag
from tc_core.telemetry import log_data_source, perf_timer
from tc_core.utils.portabilidade import get_data_root

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Mapeamento: parquet original → variante otimizada
# ---------------------------------------------------------------------------

_AGG_MAP: dict[str, str] = {
    "df_principal_BUD":          "df_principal_agg_home_BUD",
    "df_principal":              "df_principal_agg_home",
    "df_veiculos_custo_fp_BUD":  "df_veiculos_agg_home_BUD",
    "df_veiculos_custo_fp":      "df_veiculos_agg_home",
    "df_final_BUD":              "df_final_agg_BUD",
    "df_final":                  "df_final_agg",
    "forecast_completo":         "forecast_agg",
}

_THIN_MAP: dict[str, str] = {
    "df_principal_BUD":          "df_principal_thin_BUD",
    "df_principal":              "df_principal_thin",
    "df_tc_sapiens":             "df_tc_sapiens_thin",
    "df_final_BUD":              "df_final_thin_BUD",
    "df_final":                  "df_final_thin",
}


def _use_optimized() -> bool:
    """True quando parquets otimizados estão habilitados."""
    return get_flag("SCI_USE_OPTIMIZED_PARQUETS", default="false") == "true"


def _build_path(domain: str, ano: str, subfolder: str, table: str) -> str:
    """Monta caminho para um parquet."""
    parts = [str(get_data_root()), domain, str(ano)]
    if subfolder:
        parts.append(subfolder)
    return os.path.join(*parts, f"{table}.parquet")


def read_optimized(
    domain: str,
    ano: str,
    subfolder: str,
    table: str,
    prefer: str = "agg",
    columns: Optional[Sequence[str]] = None,
    consumer: str = "unknown",
) -> Optional[pd.DataFrame]:
    """Lê parquet otimizado com fallback para full.

    Parameters
    ----------
    domain : str
        "TC_Principal" ou "TC_Ext".
    ano : str
        Ano (ex: "2026").
    subfolder : str
        Subfolder ("BUD", "Forecast", ou "").
    table : str
        Nome-base do parquet original (sem .parquet).
    prefer : str
        "agg" ou "thin".
    columns : list[str] | None
        Colunas a ler (projection pushdown).
    consumer : str
        Identificador do chamador para telemetria.

    Returns
    -------
    pd.DataFrame | None
        DataFrame lido da melhor fonte disponível.
    """
    if _use_optimized():
        variant_map = _AGG_MAP if prefer == "agg" else _THIN_MAP
        variant_name = variant_map.get(table)

        if variant_name:
            path = _build_path(domain, ano, subfolder, variant_name)
            if os.path.exists(path):
                mode = "AGG" if prefer == "agg" else "THIN"
                logger.info("DataRouter: usando %s (otimizado)", variant_name)
                with perf_timer() as t:
                    df = pd.read_parquet(path, columns=columns)
                log_data_source(
                    consumer=consumer,
                    logical_dataset=table,
                    physical_path=path,
                    mode=mode,
                    nrows=len(df),
                    ncols=len(df.columns),
                    load_ms=t.elapsed_ms,
                )
                return df
            logger.info(
                "DataRouter: %s não encontrado, fallback para %s",
                variant_name, table,
            )

    # Fallback: parquet original
    path = _build_path(domain, ano, subfolder, table)
    if not os.path.exists(path):
        return None
    with perf_timer() as t:
        df = pd.read_parquet(path, columns=columns)
    log_data_source(
        consumer=consumer,
        logical_dataset=table,
        physical_path=path,
        mode="FULL",
        nrows=len(df),
        ncols=len(df.columns),
        load_ms=t.elapsed_ms,
    )
    return df
