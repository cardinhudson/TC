from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from tc_ext.normalizacao import padronizar_colunas


def cpu_por_chaves(
    df_custo: pd.DataFrame | None,
    df_volume: pd.DataFrame | None,
    chaves_preferidas: Iterable[str] = ("Ano", "Período", "Oficina"),
    coluna_custo: str = "Total",
    coluna_volume: str = "Volume",
) -> pd.DataFrame:
    """Calcula CPU = sum(custo) / sum(volume) no mesmo grão do volume.

    Motivação: em várias bases o volume NÃO possui dimensão "Veículo".
    Se o custo estiver no grão (Oficina, Veículo, Período) e o volume no grão
    (Oficina, Período), fazer merge antes de agregar duplica o volume e derruba
    o CPU.

    Esta função agrega custo e volume no mesmo conjunto de chaves e só então
    calcula CPU.
    """
    if df_custo is None or not hasattr(df_custo, "columns"):
        return pd.DataFrame()

    df_custo_n = padronizar_colunas(df_custo)
    df_volume_n = (
        padronizar_colunas(df_volume) if df_volume is not None else None
    )

    if coluna_custo not in df_custo_n.columns:
        return pd.DataFrame()

    chaves = [
        c
        for c in chaves_preferidas
        if c in df_custo_n.columns
        and (df_volume_n is None or c in df_volume_n.columns)
    ]
    if not chaves:
        # Sem chaves comuns, não dá para garantir grão correto.
        return pd.DataFrame()

    custo_num = pd.to_numeric(
        df_custo_n[coluna_custo], errors="coerce"
    ).fillna(0)
    custo_agr = (
        df_custo_n.assign(**{coluna_custo: custo_num})
        .groupby(chaves, dropna=False)
        .agg({coluna_custo: "sum"})
        .reset_index()
    )
    custo_agr = custo_agr.rename(columns={coluna_custo: "Total"})

    if df_volume_n is None or coluna_volume not in df_volume_n.columns:
        out = custo_agr.copy()
        out["Volume"] = 0.0
        out["CPU"] = 0.0
        return out

    vol_num = pd.to_numeric(
        df_volume_n[coluna_volume], errors="coerce"
    ).fillna(0)
    vol_agr = (
        df_volume_n.assign(**{coluna_volume: vol_num})
        .groupby(chaves, dropna=False)
        .agg({coluna_volume: "sum"})
        .reset_index()
    )
    vol_agr = vol_agr.rename(columns={coluna_volume: "Volume"})

    # Importante: usar OUTER para não perder volume quando não há custo
    # no recorte.
    # Ex.: existe Volume em um mês/veículo, mas não houve lançamento de custo.
    # A regra correta é custo=0 nesse caso, e o volume deve entrar
    # no denominador.
    out = custo_agr.merge(vol_agr, on=chaves, how="outer")
    out["Total"] = pd.to_numeric(
        out.get("Total", 0), errors="coerce"
    ).fillna(0)
    out["Volume"] = pd.to_numeric(
        out.get("Volume", 0), errors="coerce"
    ).fillna(0)
    out["CPU"] = np.where(
        out["Volume"] != 0,
        out["Total"] / out["Volume"],
        0.0,
    )
    return out
