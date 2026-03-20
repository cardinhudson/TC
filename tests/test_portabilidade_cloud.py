"""Testes — tc_core.utils.portabilidade (extensões cloud)"""

from __future__ import annotations

import os

from tc_core.utils.portabilidade import IS_FROZEN, is_cloud, get_data_root, get_base_path


class TestIsCloud:
    def test_local_nao_cloud(self):
        os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        os.environ.pop("SCI_CLOUD", None)
        assert is_cloud() is False

    def test_databricks_runtime(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
        assert is_cloud() is True

    def test_sci_cloud_flag(self, monkeypatch):
        monkeypatch.setenv("SCI_CLOUD", "1")
        assert is_cloud() is True


class TestPortabilidadeBasica:
    def test_is_frozen_false_em_testes(self):
        assert IS_FROZEN is False

    def test_get_base_path_retorna_path(self):
        p = get_base_path()
        assert p.exists()

    def test_get_data_root_retorna_path(self):
        p = get_data_root()
        assert "dados" in str(p).lower() or os.environ.get("SCI_SHARED_DATA_ROOT")
