"""Testes — tc_core.data_source (backend local apenas)"""

from __future__ import annotations

import pandas as pd
import pytest

from tc_core.data_source import _parquet_path, read_table, list_available_years
from tc_core.feature_flags import set_flag


@pytest.fixture(autouse=True)
def _force_local_backend():
    """Garante que os testes rodam com backend local."""
    set_flag("SCI_DATA_BACKEND", "local")
    yield
    set_flag("SCI_DATA_BACKEND", "local")


class TestParquetPath:
    def test_com_subfolder(self):
        p = _parquet_path("TC_Principal", "2025", "BUD", "df_principal_BUD")
        assert p.name == "df_principal_BUD.parquet"
        assert "TC_Principal" in str(p)
        assert "2025" in str(p)
        assert "BUD" in str(p)

    def test_sem_subfolder(self):
        p = _parquet_path("TC_Principal", "2025", "", "df_tc_sapiens")
        assert p.name == "df_tc_sapiens.parquet"
        parts = p.parts
        assert "BUD" not in parts


class TestReadTable:
    def test_arquivo_inexistente_retorna_none(self):
        df = read_table("DOMINIO_FAKE", "9999", "BUD", "tabela_fake")
        assert df is None

    def test_leitura_real_se_existir(self, tmp_path, monkeypatch):
        """Cria um parquet temporário e valida leitura."""
        # Montar estrutura: tmp/TC_Test/2025/BUD/df_test.parquet
        bud = tmp_path / "TC_Test" / "2025" / "BUD"
        bud.mkdir(parents=True)
        df_orig = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        df_orig.to_parquet(bud / "df_test.parquet")

        # Redirecionar data_root
        monkeypatch.setattr("tc_core.data_source.get_data_root", lambda: tmp_path)

        df = read_table("TC_Test", "2025", "BUD", "df_test")
        assert df is not None
        assert list(df.columns) == ["A", "B"]
        assert len(df) == 2

    def test_columns_filter(self, tmp_path, monkeypatch):
        bud = tmp_path / "TC_Test" / "2025" / "BUD"
        bud.mkdir(parents=True)
        pd.DataFrame({"A": [1], "B": [2], "C": [3]}).to_parquet(bud / "df_test.parquet")

        monkeypatch.setattr("tc_core.data_source.get_data_root", lambda: tmp_path)

        df = read_table("TC_Test", "2025", "BUD", "df_test", columns=["A", "C"])
        assert list(df.columns) == ["A", "C"]


class TestListAvailableYears:
    def test_sem_pasta(self, tmp_path, monkeypatch):
        monkeypatch.setattr("tc_core.data_source.get_data_root", lambda: tmp_path)
        assert list_available_years("TC_Fake") == []

    def test_com_anos(self, tmp_path, monkeypatch):
        for ano in [2024, 2025, 2026]:
            bud = tmp_path / "TC_Test" / str(ano) / "BUD"
            bud.mkdir(parents=True)
            pd.DataFrame({"x": [1]}).to_parquet(bud / "dummy.parquet")

        monkeypatch.setattr("tc_core.data_source.get_data_root", lambda: tmp_path)

        anos = list_available_years("TC_Test", require_subfolder="BUD")
        assert anos == [2026, 2025, 2024]
