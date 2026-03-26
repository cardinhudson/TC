"""
tc_core/parquet_schemas.py
==========================
Contratos de dados (schemas) para parquets derivados THIN e AGG.

Cada schema define explicitamente as colunas esperadas, as colunas a
remover, e as chaves/métricas de agregação.  Usado por:
  - Pipelines de processamento (gerar derivados)
  - Validação automática (scripts/validate_optimized_parquets.py)
  - Data Router (tc_core/data_router.py) — verificação de integridade

Multi-planta (futuro)
---------------------
Quando multi-planta for implementado, ``plant_id`` será adicionado como
**primeira chave** em todos os ``AGG_SCHEMAS["group_keys"]`` e como coluna
preservada em todos os ``THIN_SCHEMAS``.  Os parquets serão segmentados
por diretório: ``dados/<domain>/<plant_id>/<ano>/...``.
"""

from __future__ import annotations

# ═══════════════════════════════════════════════════════════════
#   COLUNAS REMOVIDAS NO THIN (6 colunas pesadas não usadas nos filtros)
# ═══════════════════════════════════════════════════════════════
# NOTA: Centrocst, Nºconta, Tipo, Fornecedor, Fornec. e Usuário
#       são usados nos filtros da sidebar e DEVEM permanecer no THIN.

COLUNAS_DROP_THIN: list[str] = [
    "Texto breve",
    "Material",
    "Doc.compra",
    "Nºdoc.ref.",
    "Dt.lçto.",
    "QTD",
]


# ═══════════════════════════════════════════════════════════════
#   SCHEMAS THIN  (mesmas linhas, menos colunas)
# ═══════════════════════════════════════════════════════════════

THIN_SCHEMAS: dict[str, dict] = {
    "df_principal_thin": {
        "source": "df_principal",
        "drop_columns": COLUNAS_DROP_THIN,
        "description": "df_principal sem colunas pesadas de texto (Texto breve, Material, Doc.compra, etc.)",
    },
    "df_principal_thin_BUD": {
        "source": "df_principal_BUD",
        "drop_columns": COLUNAS_DROP_THIN,
        "description": "df_principal_BUD sem colunas pesadas de texto",
    },
    "df_tc_sapiens_thin": {
        "source": "df_tc_sapiens",
        "drop_columns": COLUNAS_DROP_THIN,
        "description": "df_tc_sapiens sem colunas pesadas de texto",
    },
    "df_final_thin": {
        "source": "df_final",
        "drop_columns": ["Nºconta", "QTD"],
        "description": "TC_Ext df_final sem Nºconta e QTD",
    },
    "df_final_thin_BUD": {
        "source": "df_final_BUD",
        "drop_columns": ["Nºconta", "QTD"],
        "description": "TC_Ext df_final_BUD sem Nºconta e QTD",
    },
}


# ═══════════════════════════════════════════════════════════════
#   SCHEMAS AGG  (menos linhas, pré-agregados)
# ═══════════════════════════════════════════════════════════════

AGG_SCHEMAS: dict[str, dict] = {
    # --- TC_Principal (Budget/Real) ---
    "df_principal_agg_home": {
        "source": "df_principal",
        "group_keys": ["Ano", "Período", "Oficina", "Type 05", "Type 06", "Account", "Custo"],
        "sum_columns": ["Despesa Primaria", "Custo FA", "Custo FP", "D&A dedicado", "FP sem Dedicada"],
        "description": "Agregado Home/Waterfall — TC_Principal Real",
    },
    "df_principal_agg_home_BUD": {
        "source": "df_principal_BUD",
        "group_keys": ["Ano", "Período", "Oficina", "Type 05", "Type 06", "Account", "Custo"],
        "sum_columns": ["Despesa Primaria", "Custo FA", "Custo FP", "D&A dedicado", "FP sem Dedicada"],
        "description": "Agregado Home/Waterfall — TC_Principal Budget",
    },
    # --- Veículos ---
    "df_veiculos_agg_home": {
        "source": "df_veiculos_custo_fp",
        "group_keys": ["Ano", "Período", "Oficina", "Veículo"],
        "sum_columns": ["Custo FP Veiculo", "Custo Rateado", "D&A dedicado"],
        "description": "Agregado Home veículos — Real",
    },
    "df_veiculos_agg_home_BUD": {
        "source": "df_veiculos_custo_fp_BUD",
        "group_keys": ["Ano", "Período", "Oficina", "Veículo"],
        "sum_columns": ["Custo FP Veiculo", "Custo Rateado", "D&A dedicado"],
        "description": "Agregado Home veículos — Budget",
    },
    # --- TC_Ext ---
    "df_final_agg": {
        "source": "df_final",
        "group_keys": ["Ano", "Período", "Oficina", "Veículo"],
        "sum_columns": ["Total", "Valor"],
        "description": "Agregado TC_Ext — Real",
    },
    "df_final_agg_BUD": {
        "source": "df_final_BUD",
        "group_keys": ["Ano", "Período", "Oficina", "Veículo"],
        "sum_columns": ["Total", "Valor"],
        "description": "Agregado TC_Ext — Budget",
    },
    # --- Forecast ---
    "forecast_agg": {
        "source": "forecast_completo",
        "group_keys": ["Ano", "Período", "Oficina", "Tipo"],
        "sum_columns": ["Custo FP", "FP sem Dedicada", "D&A dedicado"],
        "description": "Agregado Forecast (Histórico/BE)",
    },
}


# ═══════════════════════════════════════════════════════════════
#   HELPERS DE GERAÇÃO
# ═══════════════════════════════════════════════════════════════

def gerar_thin(df, schema_name: str):
    """Retorna DataFrame thin removendo colunas DETAIL-only.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame original (full).
    schema_name : str
        Nome do schema em THIN_SCHEMAS.

    Returns
    -------
    pd.DataFrame
        Cópia sem as colunas especificadas.
    """
    schema = THIN_SCHEMAS[schema_name]
    drop = [c for c in schema["drop_columns"] if c in df.columns]
    return df.drop(columns=drop)


def gerar_agg(df, schema_name: str):
    """Retorna DataFrame pré-agregado conforme schema AGG.

    REGRA: CPU nunca é somado — deve ser recalculado pós-agregação pelo consumidor.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame original (full ou thin).
    schema_name : str
        Nome do schema em AGG_SCHEMAS.

    Returns
    -------
    pd.DataFrame
        Agregado (groupby + sum).
    """
    schema = AGG_SCHEMAS[schema_name]
    keys = [k for k in schema["group_keys"] if k in df.columns]
    sums = [c for c in schema["sum_columns"] if c in df.columns]
    if not keys or not sums:
        return df
    return df.groupby(keys, as_index=False)[sums].sum()
