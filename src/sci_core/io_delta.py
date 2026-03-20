from __future__ import annotations

import shutil
from pathlib import Path
from typing import Callable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def _emit(log: Callable[[str], None] | None, level: str, message: str) -> None:
    writer = log or print
    writer(f"[{level}] {message}")


def _normalize_pandas(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "Tipo" not in frame.columns:
        frame["Tipo"] = ""

    for column in frame.columns:
        if frame[column].dtype == "object":
            frame[column] = frame[column].where(frame[column].notna(), "")
        elif pd.api.types.is_integer_dtype(frame[column]):
            numeric_series = pd.to_numeric(frame[column], errors="coerce")
            frame[column] = numeric_series.astype("Int64")
        elif pd.api.types.is_numeric_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        else:
            frame[column] = frame[column].astype(str)
    return frame


def _build_schema(frame: pd.DataFrame) -> StructType:
    fields: list[StructField] = []
    for column in frame.columns:
        series = frame[column]
        if pd.api.types.is_integer_dtype(series):
            spark_type = LongType()
        elif pd.api.types.is_numeric_dtype(series):
            spark_type = DoubleType()
        else:
            spark_type = StringType()
        fields.append(StructField(str(column), spark_type, True))
    return StructType(fields)


def to_spark_safe(
    spark: SparkSession,
    df: pd.DataFrame,
    *,
    log: Callable[[str], None] | None = None,
) -> DataFrame:
    """Converte pandas -> Spark de forma robusta, com Arrow desligado."""
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    frame = _normalize_pandas(df)
    schema = _build_schema(frame)
    records = frame.to_dict(orient="records")
    _emit(
        log,
        "INFO",
        f"Conversão pandas->Spark segura: {len(records)} linhas",
    )
    return spark.createDataFrame(records, schema=schema)


def _write_parquet_fallback(
    df_spark: DataFrame | pd.DataFrame,
    path: str | Path,
    *,
    mode: str,
    partition_by: list[str] | None,
) -> None:
    dataset_path = Path(path)
    if mode == "overwrite" and dataset_path.exists():
        shutil.rmtree(dataset_path)
    dataset_path.mkdir(parents=True, exist_ok=True)

    frame = df_spark.toPandas() if hasattr(df_spark, "toPandas") else df_spark
    partition_cols = [
        col for col in (partition_by or []) if col in frame.columns
    ]
    frame.to_parquet(
        dataset_path,
        index=False,
        engine="pyarrow",
        partition_cols=partition_cols or None,
    )


def write_delta(
    df_spark: DataFrame | pd.DataFrame,
    path: str | Path,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    log: Callable[[str], None] | None = None,
) -> str:
    """Tenta Delta; se falhar, grava Parquet e retorna o modo usado."""
    target_path = str(path)
    try:
        writer = df_spark.write.format("delta").mode(mode)
        partitions = [
            col for col in (partition_by or []) if col in df_spark.columns
        ]
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(target_path)
        _emit(log, "INFO", f"Delta gravado com sucesso em {target_path}")
        return "delta"
    except Exception as exc:  # noqa: BLE001
        _emit(log, "WARN", f"Falha ao gravar Delta em {target_path}: {exc}")
        _emit(log, "WARN", "Fallback para Parquet em Workspace Files")
        _write_parquet_fallback(
            df_spark,
            target_path,
            mode=mode,
            partition_by=partition_by,
        )
        return "parquet"


def read_dataset(
    spark: SparkSession,
    path: str | Path,
    *,
    log: Callable[[str], None] | None = None,
) -> DataFrame:
    target_path = str(path)
    try:
        _emit(log, "INFO", f"Lendo Delta de {target_path}")
        return spark.read.format("delta").load(target_path)
    except Exception as exc:  # noqa: BLE001
        _emit(log, "WARN", f"Delta indisponível em {target_path}: {exc}")
        _emit(log, "INFO", f"Lendo Parquet de {target_path}")
        return spark.read.parquet(target_path)
