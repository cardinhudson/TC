from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_PRINCIPAL_SHEETS = (
    "massa primária - BDG",
    "massa - REDIS",
    "Sapiens",
    "Volume e EST PdR - BDG",
    "Volume e EST PdR - Actual",
    "Volume BDG",
    "Volume Actual",
    "EST veículos - BDG",
    "EST veículos - Actual",
)

HEADER_CANDIDATES = {
    "Sapiens": (1, 0),
    "massa primária - BDG": (0,),
    "massa - REDIS": (0,),
    "Volume e EST PdR - BDG": (0,),
    "Volume e EST PdR - Actual": (0,),
    "Volume BDG": (50, 0),
    "Volume Actual": (1, 0),
    "EST veículos - BDG": (1, 0),
    "EST veículos - Actual": (1, 0),
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(col).strip() for col in frame.columns]
    frame = frame.dropna(axis=0, how="all")
    frame = frame.dropna(axis=1, how="all")
    frame = frame.reset_index(drop=True)
    return frame


def _read_sheet(path_excel: Path, sheet_name: str) -> pd.DataFrame:
    last_error: Exception | None = None
    for header in HEADER_CANDIDATES.get(sheet_name, (0,)):
        try:
            frame = pd.read_excel(
                path_excel,
                sheet_name=sheet_name,
                header=header,
                engine="openpyxl",
            )
            frame = _normalize_columns(frame)
            if not frame.empty:
                return frame
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def read_principal_excel(
    path_excel: str | Path,
    ano: int,
    sheets: Iterable[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Lê o Excel principal com pandas/openpyxl e adiciona Ano."""
    excel_path = Path(path_excel)
    workbook = pd.ExcelFile(excel_path, engine="openpyxl")
    selected = list(sheets or DEFAULT_PRINCIPAL_SHEETS)
    available = [sheet for sheet in selected if sheet in workbook.sheet_names]
    if not available:
        available = workbook.sheet_names[:1]

    frames: dict[str, pd.DataFrame] = {}
    for sheet_name in available:
        frame = _read_sheet(excel_path, sheet_name)
        if "Ano" not in frame.columns:
            frame["Ano"] = int(ano)
        frame["OrigemAba"] = sheet_name
        frames[sheet_name] = frame
    return frames
