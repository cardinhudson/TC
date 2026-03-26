"""Testes — load_forecast_agg (dual-loader Home)"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from tc_core.data_router import read_optimized


# ── Fixtures ──────────────────────────────────────────────────

_AGG_COLS = ["Ano", "Período", "Oficina", "Tipo", "Custo FP",
             "FP sem Dedicada", "D&A dedicado"]

_FULL_EXTRA_COLS = ["Despesa Primaria", "Custo FA", "Type 05",
                    "Type 06", "Custo", "Veículo"]


@pytest.fixture
def forecast_data(monkeypatch, tmp_path):
    """Cria forecast_completo.parquet (FULL) e forecast_agg.parquet."""
    fc_dir = tmp_path / "TC_Principal" / "" / "Forecast"
    fc_dir.mkdir(parents=True)

    # FULL (25K simulado com 10 rows para teste)
    df_full = pd.DataFrame({
        "Ano": [2026] * 10,
        "Período": ["Janeiro"] * 5 + ["Fevereiro"] * 5,
        "Oficina": ["OF1", "OF2"] * 5,
        "Tipo": ["BE"] * 10,
        "Custo FP": [100.0] * 10,
        "FP sem Dedicada": [80.0] * 10,
        "D&A dedicado": [20.0] * 10,
        "Despesa Primaria": [50.0] * 10,
        "Custo FA": [30.0] * 10,
        "Type 05": ["T05A"] * 10,
        "Type 06": ["T06X"] * 10,
        "Custo": ["Fixo"] * 10,
        "Veículo": ["VeicA"] * 10,
    })
    df_full.to_parquet(str(fc_dir / "forecast_completo.parquet"))

    # AGG (agregado: 93 rows simulado com 4 rows)
    df_agg = pd.DataFrame({
        "Ano": [2026, 2026, 2026, 2026],
        "Período": ["Janeiro", "Janeiro", "Fevereiro", "Fevereiro"],
        "Oficina": ["OF1", "OF2", "OF1", "OF2"],
        "Tipo": ["BE", "BE", "BE", "BE"],
        "Custo FP": [250.0, 250.0, 250.0, 250.0],
        "FP sem Dedicada": [200.0, 200.0, 200.0, 200.0],
        "D&A dedicado": [50.0, 50.0, 50.0, 50.0],
    })
    df_agg.to_parquet(str(fc_dir / "forecast_agg.parquet"))

    monkeypatch.setattr("tc_core.data_router.get_data_root", lambda: str(tmp_path))
    return tmp_path


# ── Testes ────────────────────────────────────────────────────

class TestForecastAggRouting:
    def test_agg_returns_small_schema(self, forecast_data, monkeypatch):
        """Flag on + prefer=agg → retorna AGG (sem colunas extras)."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        df = read_optimized("TC_Principal", "", "Forecast",
                            "forecast_completo", prefer="agg",
                            consumer="test_agg")
        assert df is not None
        assert len(df) == 4
        for col in _AGG_COLS:
            assert col in df.columns
        for col in _FULL_EXTRA_COLS:
            assert col not in df.columns

    def test_full_fallback_when_flag_off(self, forecast_data, monkeypatch):
        """Flag off → retorna FULL (com colunas extras)."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "false")
        df = read_optimized("TC_Principal", "", "Forecast",
                            "forecast_completo", prefer="agg",
                            consumer="test_full_fallback")
        assert df is not None
        assert len(df) == 10
        assert "Veículo" in df.columns
        assert "Type 06" in df.columns

    def test_thin_doesnt_match_forecast(self, forecast_data, monkeypatch):
        """prefer=thin para forecast_completo → fallback para FULL
        (forecast não está no _THIN_MAP)."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        df = read_optimized("TC_Principal", "", "Forecast",
                            "forecast_completo", prefer="thin",
                            consumer="test_thin_no_match")
        assert df is not None
        assert len(df) == 10  # FULL

    def test_agg_sums_match_full(self, forecast_data, monkeypatch):
        """Totais do AGG devem igualar FULL (para colunas compartilhadas)."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "false")
        df_full = read_optimized("TC_Principal", "", "Forecast",
                                 "forecast_completo", prefer="agg",
                                 consumer="test_sum_full")
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        df_agg = read_optimized("TC_Principal", "", "Forecast",
                                "forecast_completo", prefer="agg",
                                consumer="test_sum_agg")
        for col in ["Custo FP", "FP sem Dedicada", "D&A dedicado"]:
            assert abs(df_full[col].sum() - df_agg[col].sum()) < 0.01, \
                f"Mismatch in {col}: FULL={df_full[col].sum()}, AGG={df_agg[col].sum()}"

    def test_agg_fallback_when_file_missing(self, forecast_data, monkeypatch):
        """AGG file missing → graceful fallback to FULL."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        agg_path = os.path.join(
            str(forecast_data), "TC_Principal", "", "Forecast",
            "forecast_agg.parquet",
        )
        os.remove(agg_path)
        df = read_optimized("TC_Principal", "", "Forecast",
                            "forecast_completo", prefer="agg",
                            consumer="test_missing_agg")
        assert df is not None
        assert len(df) == 10  # fell back to FULL


class TestFeatureFlag:
    def test_perf_trace_flag_exists(self):
        from tc_core.feature_flags import _DEFAULTS
        assert "SCI_DEBUG_PERF_TRACE" in _DEFAULTS

    def test_perf_trace_default_false(self):
        from tc_core.feature_flags import get_flag
        assert get_flag("SCI_DEBUG_PERF_TRACE") == "false"
