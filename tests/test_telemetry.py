"""Testes — tc_core.telemetry"""

from __future__ import annotations

import logging

import pytest

from tc_core.telemetry import log_data_source, perf_timer, get_trace_log, clear_trace_log


class TestLogDataSource:
    """Testa log_data_source com logging padrão."""

    def test_logs_to_logger(self, caplog):
        with caplog.at_level(logging.INFO, logger="sci.telemetry"):
            log_data_source(
                consumer="test_consumer",
                logical_dataset="df_principal_BUD",
                physical_path="/fake/path.parquet",
                mode="THIN",
                nrows=100,
                ncols=10,
                load_ms=42.5,
            )
        assert "test_consumer" in caplog.text
        assert "df_principal_BUD" in caplog.text
        assert "THIN" in caplog.text
        assert "42.5" in caplog.text

    def test_log_format_contains_all_fields(self, caplog):
        with caplog.at_level(logging.INFO, logger="sci.telemetry"):
            log_data_source(
                consumer="home_kpi",
                logical_dataset="forecast_completo",
                physical_path="/data/forecast.parquet",
                mode="AGG",
                nrows=500,
                ncols=8,
                load_ms=15.3,
            )
        assert "consumer=home_kpi" in caplog.text
        assert "dataset=forecast_completo" in caplog.text
        assert "mode=AGG" in caplog.text
        assert "rows=500" in caplog.text
        assert "cols=8" in caplog.text


class TestPerfTimer:
    """Testa context manager de medição de tempo."""

    def test_measures_positive_time(self):
        with perf_timer() as t:
            _ = sum(range(1000))
        assert t.elapsed_ms >= 0

    def test_elapsed_ms_is_float(self):
        with perf_timer() as t:
            pass
        assert isinstance(t.elapsed_ms, float)


class TestTraceAccumulation:
    """Testa acumulação em session_state (sem Streamlit real)."""

    def test_get_trace_log_returns_list_without_streamlit(self):
        # Sem Streamlit ativo, deve retornar lista vazia
        result = get_trace_log()
        assert isinstance(result, list)

    def test_clear_trace_log_no_error_without_streamlit(self):
        # Não deve levantar exceção sem Streamlit
        clear_trace_log()
