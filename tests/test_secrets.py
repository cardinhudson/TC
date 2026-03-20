"""Testes — tc_core.secrets"""

from __future__ import annotations

import os

from tc_core.secrets import get_secret, is_databricks


class TestGetSecret:
    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET_KEY", "s3cret")
        assert get_secret("MY_SECRET_KEY") == "s3cret"

    def test_missing_returns_none(self):
        os.environ.pop("NONEXISTENT_SECRET_XYZ", None)
        assert get_secret("NONEXISTENT_SECRET_XYZ") is None

    def test_missing_returns_default(self):
        os.environ.pop("NONEXISTENT_SECRET_XYZ", None)
        assert get_secret("NONEXISTENT_SECRET_XYZ", default="fallback") == "fallback"

    def test_env_takes_precedence_over_default(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET_KEY", "env_val")
        assert get_secret("MY_SECRET_KEY", default="fallback") == "env_val"


class TestIsDatabricks:
    def test_not_databricks_locally(self):
        # Em ambiente de teste local, nunca é Databricks
        assert is_databricks() is False
