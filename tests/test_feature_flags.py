"""Testes — tc_core.feature_flags"""

from __future__ import annotations

import os

from tc_core.feature_flags import all_flags, get_flag, set_flag


class TestGetFlag:
    def test_default_local(self):
        # Sem env var definida, retorna default compilado
        os.environ.pop("SCI_DATA_BACKEND", None)
        assert get_flag("SCI_DATA_BACKEND") == "local"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("SCI_DATA_BACKEND", "snowflake")
        assert get_flag("SCI_DATA_BACKEND") == "snowflake"

    def test_env_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("SCI_DATA_BACKEND", "SNOWFLAKE")
        assert get_flag("SCI_DATA_BACKEND") == "snowflake"

    def test_custom_default(self):
        os.environ.pop("MINHA_FLAG_CUSTOM", None)
        assert get_flag("MINHA_FLAG_CUSTOM", default="abc") == "abc"

    def test_unknown_flag_empty(self):
        os.environ.pop("TOTAL_DESCONHECIDA", None)
        assert get_flag("TOTAL_DESCONHECIDA") == ""


class TestSetFlag:
    def test_set_and_get(self):
        set_flag("SCI_DATA_BACKEND", "snowflake")
        assert get_flag("SCI_DATA_BACKEND") == "snowflake"
        # Limpar
        set_flag("SCI_DATA_BACKEND", "local")

    def test_set_persists_in_env(self):
        set_flag("SCI_TEST_ONLY", "valor123")
        assert os.environ["SCI_TEST_ONLY"] == "valor123"
        os.environ.pop("SCI_TEST_ONLY", None)


class TestAllFlags:
    def test_returns_all_defaults(self):
        flags = all_flags()
        assert "SCI_DATA_BACKEND" in flags
        assert "SCI_EMAIL_BACKEND" in flags
        assert "SCI_LLM_PROVIDER" in flags
        assert "SCI_SCHEDULER" in flags
