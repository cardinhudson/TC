from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class SchemaCheck:
    ok: bool
    missing: list[str]
    extra: list[str]


def check_columns(df: pd.DataFrame, required: Iterable[str]) -> SchemaCheck:
    required_set = list(required)
    cols = set(df.columns)
    missing = [c for c in required_set if c not in cols]
    # extra is informational only
    extra = sorted([c for c in cols if c not in set(required_set)])
    return SchemaCheck(ok=len(missing) == 0, missing=missing, extra=extra)


REQUIRED_DF_FINAL = [
    "Ano",
    "Período",
    "Total",
]

REQUIRED_DF_VOL = [
    "Ano",
    "Período",
    "Volume",
]


def normalize_common_column_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza alguns nomes de coluna comuns quando há problema de encoding.

    Ex.: 'Per�odo' -> 'Período', 'Ve�culo' -> 'Veículo'.
    Não tenta ser genérico; só cobre os casos vistos no projeto.
    """
    replacements = {
        "Per�odo": "Período",
        "Ve�culo": "Veículo",
        "Veiculo": "Veículo",
        "Periodo": "Período",
    }
    cols = list(df.columns)
    new_cols = [replacements.get(c, c) for c in cols]
    if new_cols == cols:
        return df
    df = df.copy()
    df.columns = new_cols
    return df
