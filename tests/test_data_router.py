"""Testes — tc_core.data_router"""

from __future__ import annotations

import os
import tempfile

import pandas as pd
import pytest

from tc_core.data_router import _AGG_MAP, _THIN_MAP, _build_path, read_optimized


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def temp_data(monkeypatch, tmp_path):
    """Cria estrutura de diretórios temporária com parquets full + agg."""
    # TC_Principal/2026/BUD/
    bud_dir = tmp_path / "TC_Principal" / "2026" / "BUD"
    bud_dir.mkdir(parents=True)

    # Parquet original
    df_full = pd.DataFrame({
        "Ano": [2026], "Período": ["Janeiro"], "Oficina": ["OF1"],
        "Custo FP": [100.0], "Texto breve": ["detalhe"],
    })
    df_full.to_parquet(str(bud_dir / "df_principal_BUD.parquet"))

    # Parquet AGG
    df_agg = pd.DataFrame({
        "Ano": [2026], "Período": ["Janeiro"], "Oficina": ["OF1"],
        "Custo FP": [100.0],
    })
    df_agg.to_parquet(str(bud_dir / "df_principal_agg_home_BUD.parquet"))

    # Parquet THIN
    df_thin = pd.DataFrame({
        "Ano": [2026], "Período": ["Janeiro"], "Oficina": ["OF1"],
        "Custo FP": [100.0],
    })
    df_thin.to_parquet(str(bud_dir / "df_principal_thin_BUD.parquet"))

    # Monkeypatch get_data_root para apontar para tmp_path
    monkeypatch.setattr("tc_core.data_router.get_data_root", lambda: str(tmp_path))
    return tmp_path


# ── Testes ────────────────────────────────────────────────────

class TestBuildPath:
    def test_path_with_subfolder(self, monkeypatch, tmp_path):
        monkeypatch.setattr("tc_core.data_router.get_data_root", lambda: str(tmp_path))
        path = _build_path("TC_Principal", "2026", "BUD", "df_principal_BUD")
        assert path.endswith("df_principal_BUD.parquet")
        assert "TC_Principal" in path
        assert "2026" in path
        assert "BUD" in path

    def test_path_without_subfolder(self, monkeypatch, tmp_path):
        monkeypatch.setattr("tc_core.data_router.get_data_root", lambda: str(tmp_path))
        path = _build_path("TC_Principal", "2026", "", "df_principal")
        assert "BUD" not in path


class TestReadOptimized:
    def test_fallback_when_flag_off(self, temp_data, monkeypatch):
        """Flag desligada → lê direto do original."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "false")
        df = read_optimized("TC_Principal", "2026", "BUD", "df_principal_BUD", prefer="agg")
        assert df is not None
        assert "Texto breve" in df.columns  # Full tem a coluna detail

    def test_agg_when_flag_on(self, temp_data, monkeypatch):
        """Flag ligada → lê do AGG (sem coluna Texto breve)."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        df = read_optimized("TC_Principal", "2026", "BUD", "df_principal_BUD", prefer="agg")
        assert df is not None
        assert "Texto breve" not in df.columns

    def test_thin_when_flag_on(self, temp_data, monkeypatch):
        """Flag ligada + prefer=thin → lê do THIN."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        df = read_optimized("TC_Principal", "2026", "BUD", "df_principal_BUD", prefer="thin")
        assert df is not None

    def test_fallback_when_optimized_missing(self, temp_data, monkeypatch):
        """Flag ligada mas parquet otimizado não existe → fallback para original."""
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "true")
        # Remover o AGG
        agg_path = os.path.join(str(temp_data), "TC_Principal", "2026", "BUD",
                                "df_principal_agg_home_BUD.parquet")
        os.remove(agg_path)
        df = read_optimized("TC_Principal", "2026", "BUD", "df_principal_BUD", prefer="agg")
        assert df is not None
        assert "Texto breve" in df.columns  # Leu do original

    def test_returns_none_when_nothing_exists(self, temp_data, monkeypatch):
        monkeypatch.setenv("SCI_USE_OPTIMIZED_PARQUETS", "false")
        df = read_optimized("TC_Principal", "2099", "BUD", "nao_existe", prefer="agg")
        assert df is None


class TestMaps:
    def test_agg_map_covers_all_schemas(self):
        assert "df_principal_BUD" in _AGG_MAP
        assert "df_principal" in _AGG_MAP
        assert "df_final_BUD" in _AGG_MAP
        assert "df_final" in _AGG_MAP
        assert "forecast_completo" in _AGG_MAP

    def test_thin_map_covers_all_schemas(self):
        assert "df_principal_BUD" in _THIN_MAP
        assert "df_principal" in _THIN_MAP
        assert "df_tc_sapiens" in _THIN_MAP
        assert "df_final_BUD" in _THIN_MAP
        assert "df_final" in _THIN_MAP
