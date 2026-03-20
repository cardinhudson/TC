from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame


def _to_pandas(df: DataFrame | pd.DataFrame) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df
    return df.toPandas()


def write_parquet_dataset(
    df: DataFrame | pd.DataFrame,
    path: str | Path,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    dataset_path = Path(path)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)

    frame = _to_pandas(df)
    if mode == "overwrite" and dataset_path.exists():
        shutil.rmtree(dataset_path)

    if mode == "append" and dataset_path.exists():
        existing = pd.read_parquet(dataset_path)
        frame = pd.concat([existing, frame], ignore_index=True)
        shutil.rmtree(dataset_path)

    if frame.empty:
        frame = pd.DataFrame(columns=frame.columns)

    partition_cols = [
        col for col in (partition_by or []) if col in frame.columns
    ]
    frame.to_parquet(
        dataset_path,
        index=False,
        engine="pyarrow",
        partition_cols=partition_cols or None,
    )


def read_parquet_dataset(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(Path(path), engine="pyarrow")
