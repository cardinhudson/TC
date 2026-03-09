from __future__ import annotations

from typing import Any

import pandas as pd

from tc_core.constants import MAPEAMENTO_MESES_LOWER


def normalizar_periodo(valor: Any) -> Any:
    if pd.isna(valor):
        return valor
    texto = str(valor).strip()
    if not texto:
        return texto
    texto_lower = texto.lower()
    return MAPEAMENTO_MESES_LOWER.get(texto_lower, texto)


def normalizar_coluna_periodo(df: pd.DataFrame, coluna: str = "Período") -> pd.DataFrame:
    if coluna not in df.columns:
        return df
    df = df.copy()
    df[coluna] = df[coluna].apply(normalizar_periodo)
    return df
