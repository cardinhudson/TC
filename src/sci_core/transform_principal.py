from __future__ import annotations

import json
from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _pick_value_column(frame: pd.DataFrame) -> str | None:
    for column in frame.columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.notna().sum() > 0:
            return column
    return None


def _canonicalize_rows(
    spark: SparkSession,
    frame: pd.DataFrame,
    *,
    ano: int,
    origem: str,
    aba: str,
) -> DataFrame:
    base = frame.copy()
    base = base.dropna(axis=0, how="all").reset_index(drop=True)
    value_column = _pick_value_column(base)

    key_one = (
        base.iloc[:, 0].astype("string")
        if base.shape[1] >= 1
        else pd.Series(dtype="string")
    )
    key_two = (
        base.iloc[:, 1].astype("string")
        if base.shape[1] >= 2
        else pd.Series(dtype="string")
    )
    value_series = (
        pd.to_numeric(base[value_column], errors="coerce")
        if value_column is not None
        else pd.Series([None] * len(base), dtype="float64")
    )
    payload_series = base.fillna("").astype(str).to_dict(orient="records")
    chave_um = key_one if not key_one.empty else pd.Series([None] * len(base))
    chave_dois = (
        key_two if not key_two.empty else pd.Series([None] * len(base))
    )
    payload_json = [
        json.dumps(item, ensure_ascii=False) for item in payload_series
    ]

    canonical = pd.DataFrame(
        {
            "Ano": int(ano),
            "Origem": origem,
            "Aba": aba,
            "Linha": range(1, len(base) + 1),
            "Chave1": chave_um,
            "Chave2": chave_dois,
            "Valor": value_series,
            "PayloadJson": payload_json,
        }
    )
    canonical = canonical.fillna({"Chave1": "", "Chave2": ""})
    spark_df = spark.createDataFrame(canonical)
    return spark_df.withColumn("IngestionTs", F.current_timestamp())


def _combine_frames(
    spark: SparkSession,
    frames: dict[str, pd.DataFrame],
    *,
    ano: int,
    origem: str,
    preferred_sheets: Iterable[str],
) -> DataFrame:
    selected = [sheet for sheet in preferred_sheets if sheet in frames]
    if not selected:
        selected = list(frames.keys())[:1]

    result: DataFrame | None = None
    for sheet in selected:
        current = _canonicalize_rows(
            spark,
            frames[sheet],
            ano=ano,
            origem=origem,
            aba=sheet,
        )
        result = current if result is None else result.unionByName(current)

    if result is None:
        raise ValueError(f"Nenhuma aba disponível para origem {origem}")
    return result


def processar_budget(
    spark: SparkSession,
    ano: int,
    frames: dict[str, pd.DataFrame],
) -> dict[str, DataFrame]:
    """Stub operacional do Budget com saída Spark padronizada."""
    budget_df = _combine_frames(
        spark,
        frames,
        ano=ano,
        origem="BUDGET",
        preferred_sheets=("massa primária - BDG", "massa - REDIS"),
    )
    return {"tc_principal_bud": budget_df}


def processar_real(
    spark: SparkSession,
    ano: int,
    frames: dict[str, pd.DataFrame],
) -> dict[str, DataFrame]:
    """Stub operacional do Real com saída Spark padronizada."""
    real_df = _combine_frames(
        spark,
        frames,
        ano=ano,
        origem="REAL",
        preferred_sheets=("Sapiens", "massa - REDIS", "Volume Actual"),
    )
    return {"tc_principal_real": real_df}


def build_comparativo(
    budget_df: DataFrame | None,
    real_df: DataFrame | None,
) -> DataFrame | None:
    """Gera um comparativo enxuto por origem e ano para validação inicial."""
    frames = [frame for frame in (budget_df, real_df) if frame is not None]
    if not frames:
        return None

    union_df: DataFrame | None = None
    for frame in frames:
        union_df = frame if union_df is None else union_df.unionByName(frame)

    if union_df is None:
        return None

    return (
        union_df.groupBy("Ano", "Origem")
        .agg(
            F.count("*").alias("QtdLinhas"),
            F.round(F.sum(F.coalesce(F.col("Valor"), F.lit(0.0))), 2).alias(
                "ValorTotal"
            ),
        )
        .withColumn("IngestionTs", F.current_timestamp())
    )
