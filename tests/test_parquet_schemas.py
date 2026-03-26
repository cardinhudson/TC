"""Testes — tc_core.parquet_schemas (THIN + AGG)"""

from __future__ import annotations

import pandas as pd
import pytest

from tc_core.parquet_schemas import (
    AGG_SCHEMAS,
    COLUNAS_DROP_THIN,
    THIN_SCHEMAS,
    gerar_agg,
    gerar_thin,
)


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def df_principal_sample():
    """DataFrame mínimo imitando df_principal com colunas THIN + DETAIL."""
    return pd.DataFrame({
        "Ano": [2026, 2026, 2026, 2026],
        "Período": ["Janeiro", "Janeiro", "Fevereiro", "Fevereiro"],
        "Oficina": ["OF1", "OF1", "OF2", "OF2"],
        "Type 05": ["A", "B", "A", "B"],
        "Type 06": ["X", "X", "Y", "Y"],
        "Account": ["C1", "C2", "C1", "C2"],
        "Custo": ["Direto", "Indireto", "Direto", "Indireto"],
        "USI": [1, 2, 3, 4],
        "Despesa Primaria": [100.0, 200.0, 300.0, 400.0],
        "Custo FA": [10.0, 20.0, 30.0, 40.0],
        "Custo FP": [50.0, 60.0, 70.0, 80.0],
        "D&A dedicado": [5.0, 6.0, 7.0, 8.0],
        "FP sem Dedicada": [45.0, 54.0, 63.0, 72.0],
        # Colunas DETAIL-only (devem ser removidas no THIN)
        "Texto breve": ["abc", "def", "ghi", "jkl"],
        "Fornecedor": ["F1", "F2", "F3", "F4"],
        "Fornec.": ["f1", "f2", "f3", "f4"],
        "Material": ["M1", "M2", "M3", "M4"],
        "Doc.compra": ["D1", "D2", "D3", "D4"],
        "Nºdoc.ref.": ["N1", "N2", "N3", "N4"],
        "Dt.lçto.": ["2026-01-01", "2026-01-02", "2026-02-01", "2026-02-02"],
        "Usuário": ["U1", "U2", "U3", "U4"],
        "Centrocst": ["CC1", "CC2", "CC3", "CC4"],
        "Nºconta": ["501", "502", "503", "504"],
        "Tipo": ["Histórico", "Histórico", "BE", "BE"],
        "QTD": [1, 2, 3, 4],
    })


@pytest.fixture
def df_final_sample():
    """DataFrame mínimo imitando df_final (TC_Ext)."""
    return pd.DataFrame({
        "Ano": [2026, 2026, 2026],
        "Período": ["Janeiro", "Janeiro", "Fevereiro"],
        "Oficina": ["OF1", "OF1", "OF2"],
        "Veículo": ["J516 biton", "CC21 biton", "J516 biton"],
        "Total": [1000.0, 2000.0, 3000.0],
        "Valor": [500.0, 600.0, 700.0],
        "Nºconta": ["501", "502", "503"],
        "QTD": [10, 20, 30],
    })


# ── Testes THIN ───────────────────────────────────────────────

class TestGeraThin:
    def test_remove_all_detail_columns(self, df_principal_sample):
        df_thin = gerar_thin(df_principal_sample, "df_principal_thin")
        for col in COLUNAS_DROP_THIN:
            assert col not in df_thin.columns, f"Coluna {col} deveria ter sido removida"

    def test_keeps_essential_columns(self, df_principal_sample):
        df_thin = gerar_thin(df_principal_sample, "df_principal_thin")
        for col in ["Ano", "Período", "Oficina", "Custo FP", "Despesa Primaria"]:
            assert col in df_thin.columns, f"Coluna {col} deveria ser mantida"

    def test_row_count_unchanged(self, df_principal_sample):
        df_thin = gerar_thin(df_principal_sample, "df_principal_thin")
        assert len(df_thin) == len(df_principal_sample)

    def test_values_unchanged(self, df_principal_sample):
        df_thin = gerar_thin(df_principal_sample, "df_principal_thin")
        pd.testing.assert_series_equal(
            df_thin["Custo FP"].reset_index(drop=True),
            df_principal_sample["Custo FP"].reset_index(drop=True),
        )

    def test_df_final_thin_drops_only_nconta_qtd(self, df_final_sample):
        df_thin = gerar_thin(df_final_sample, "df_final_thin")
        assert "Nºconta" not in df_thin.columns
        assert "QTD" not in df_thin.columns
        assert "Total" in df_thin.columns
        assert "Valor" in df_thin.columns

    def test_missing_columns_tolerated(self):
        """THIN tolera quando colunas a remover não existem no df."""
        df = pd.DataFrame({"Ano": [2026], "Custo FP": [100.0]})
        df_thin = gerar_thin(df, "df_principal_thin")
        assert "Ano" in df_thin.columns
        assert len(df_thin) == 1

    def test_all_thin_schemas_registered(self):
        expected = {"df_principal_thin", "df_principal_thin_BUD",
                    "df_tc_sapiens_thin", "df_final_thin", "df_final_thin_BUD"}
        assert set(THIN_SCHEMAS.keys()) == expected


# ── Testes AGG ────────────────────────────────────────────────

class TestGeraAgg:
    def test_aggregation_reduces_rows(self, df_principal_sample):
        df_agg = gerar_agg(df_principal_sample, "df_principal_agg_home")
        assert len(df_agg) <= len(df_principal_sample)

    def test_sum_total_matches(self, df_principal_sample):
        df_agg = gerar_agg(df_principal_sample, "df_principal_agg_home")
        assert abs(df_agg["Custo FP"].sum() - df_principal_sample["Custo FP"].sum()) < 0.01

    def test_despesa_primaria_sum_matches(self, df_principal_sample):
        df_agg = gerar_agg(df_principal_sample, "df_principal_agg_home")
        assert abs(df_agg["Despesa Primaria"].sum() - df_principal_sample["Despesa Primaria"].sum()) < 0.01

    def test_agg_keys_present(self, df_principal_sample):
        df_agg = gerar_agg(df_principal_sample, "df_principal_agg_home")
        schema = AGG_SCHEMAS["df_principal_agg_home"]
        for key in schema["group_keys"]:
            assert key in df_agg.columns

    def test_df_final_agg_sum(self, df_final_sample):
        df_agg = gerar_agg(df_final_sample, "df_final_agg")
        assert abs(df_agg["Total"].sum() - df_final_sample["Total"].sum()) < 0.01
        assert abs(df_agg["Valor"].sum() - df_final_sample["Valor"].sum()) < 0.01

    def test_all_agg_schemas_registered(self):
        expected = {
            "df_principal_agg_home", "df_principal_agg_home_BUD",
            "df_veiculos_agg_home", "df_veiculos_agg_home_BUD",
            "df_final_agg", "df_final_agg_BUD",
            "forecast_agg",
        }
        assert set(AGG_SCHEMAS.keys()) == expected

    def test_agg_no_detail_columns(self, df_principal_sample):
        df_agg = gerar_agg(df_principal_sample, "df_principal_agg_home")
        for col in COLUNAS_DROP_THIN:
            assert col not in df_agg.columns
