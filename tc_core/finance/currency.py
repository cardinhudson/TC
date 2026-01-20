from __future__ import annotations

from typing import Dict

import pandas as pd


def converter_moeda(valor, moeda_destino: str, taxas: Dict[str, float]):
    """Converte valor em BRL para a moeda destino usando taxa multiplicativa (1 BRL -> X moeda)."""
    if valor is None or pd.isna(valor):
        return valor
    if moeda_destino == "BRL":
        return valor
    taxa = float(taxas.get(moeda_destino, 1.0))
    return valor * taxa


def converter_coluna_moeda(df: pd.DataFrame, coluna: str, moeda_destino: str, taxas: Dict[str, float]) -> pd.DataFrame:
    if coluna not in df.columns:
        return df
    if moeda_destino == "BRL":
        return df
    df = df.copy()
    df[coluna] = df[coluna].apply(lambda x: converter_moeda(x, moeda_destino, taxas))
    return df


def obter_simbolo_moeda(moeda_codigo: str) -> str:
    return {"BRL": "R$", "USD": "$", "EUR": "€"}.get(moeda_codigo, "R$")
