from __future__ import annotations

from typing import Dict
import unicodedata

import pandas as pd


_COL_RENAMES: Dict[str, str] = {
    # Column-name mojibake that shows up in parquets
    "Per�odo": "Período",
    "Ve�culo": "Veículo",
    "Usu�rio": "Usuário",
    "N�conta": "Nºconta",
    "Fornec_": "Fornec.",
    "Fornec": "Fornec.",
    "Fornec.": "Fornec.",
    "Centrocst": "Centrocst",
}

# Aliases comuns (sem acento, caixa diferente, etc.).
# Observação: a UI/terminal às vezes exibe caracteres "quebrados"; esta camada tenta
# reduzir qualquer divergência de encoding/normalização para evitar merges falhos.
_COL_ALIASES: Dict[str, str] = {
    "periodo": "Período",
    "período": "Período",
    "veiculo": "Veículo",
    "veículo": "Veículo",
    "usuario": "Usuário",
    "usuário": "Usuário",
    "nconta": "Nºconta",
    "nºconta": "Nºconta",
    "fornec": "Fornec.",
    "fornec.": "Fornec.",
    "fornec_": "Fornec.",
}

_VALUE_RENAMES: Dict[str, Dict[str, str]] = {
    # Values (not column names)
    "Período": {
        "Mar�o": "Março",
        "mar�o": "Março",
    },
    "Custo": {
        "Vari�vel": "Variável",
        "vari�vel": "Variável",
        "N�o-Fixo": "Não-Fixo",
        "N3o-Fixo": "Não-Fixo",
    },
    "Tipo_Custo": {
        "Vari�vel": "Variável",
        "vari�vel": "Variável",
    },
}


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas/valores para reduzir regressões.

    - Corrige mojibake comum (Per�odo/Ve�culo etc.).
    - Normaliza alguns valores críticos (ex.: Mar�o -> Março).

    Não altera semântica numérica; apenas limpeza/compatibilidade.
    """
    if df is None:
        return df
    if not hasattr(df, "columns"):
        return df

    def _norm_text(value: object) -> str:
        # NFKC ajuda a colapsar formas compatíveis; NFC estabiliza acentos.
        # Também remove NBSP e espaços extras para estabilizar chaves de merge.
        s = "" if value is None else str(value)
        s = s.replace("\u00a0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = unicodedata.normalize("NFC", s)
        return s.strip()

    out = df.copy()

    # 1) Renomear colunas conhecidas + aliases comuns
    rename_map: Dict[object, str] = {}
    for col in out.columns:
        col_str_raw = "" if col is None else str(col)

        # Primeiro: correções diretas (mojibake conhecido)
        if col_str_raw in _COL_RENAMES:
            rename_map[col] = _COL_RENAMES[col_str_raw]
            continue

        # Segundo: normalização unicode + aliases (sem acento, caixa, etc.)
        col_norm = _norm_text(col_str_raw)
        alias_key = col_norm.casefold()
        if alias_key in _COL_ALIASES:
            rename_map[col] = _COL_ALIASES[alias_key]
            continue

        # Terceiro: padronizar apenas whitespace/normalização, sem mudar nome
        if col_norm != col_str_raw:
            rename_map[col] = col_norm

    if rename_map:
        out = out.rename(columns=rename_map)

    # 2) Ajustes de valores em colunas específicas
    for col, mapping in _VALUE_RENAMES.items():
        if col in out.columns:
            try:
                series = out[col]
                # Trabalhar em string sem quebrar categoricals
                series_str = series.astype(str).map(_norm_text)
                out[col] = series_str.replace(mapping)
            except Exception:
                # Se der erro por dtype/categoria, não quebrar o app
                pass

    # 3) Strip básico em dimensões mais usadas
    for col in ["Oficina", "Veículo", "Período"]:
        if col in out.columns:
            try:
                out[col] = out[col].astype(str).map(_norm_text)
            except Exception:
                pass

    return out
