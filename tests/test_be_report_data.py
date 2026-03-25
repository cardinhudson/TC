"""Testes — Fluxo de dados Best Estimate nos relatórios.

Valida que meses após ultimo_periodo_dados em config_forecast.json
(Mar-Dez 2026) usam forecast_completo.parquet como fonte, e que meses
anteriores (Jan-Fev 2026) continuam usando os parquets normais.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tc_copilot.data_collector import (
    _carregar_forecast_completo,
    _extrair_be_do_forecast,
    _mes_eh_best_estimate,
    coletar_dados_mes,
    calcular_variacoes,
)


# ───────────────────────────────────────────────────────────────
#  Fixture: forecast carregado uma vez
# ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def forecast():
    df = _carregar_forecast_completo()
    if df is None:
        pytest.skip("forecast_completo.parquet não encontrado")
    return df


# ───────────────────────────────────────────────────────────────
#  1. _mes_eh_best_estimate() — detecção via config_forecast.json
# ───────────────────────────────────────────────────────────────

class TestDeteccaoBE:
    def test_marco_2026_eh_be(self, forecast):
        """Março é BE: config diz ultimo_periodo='Fevereiro 2026', num_meses=10."""
        assert _mes_eh_best_estimate(forecast, "Março")

    def test_fevereiro_2026_eh_real(self, forecast):
        assert not _mes_eh_best_estimate(forecast, "Fevereiro")

    def test_janeiro_2026_eh_real(self, forecast):
        assert not _mes_eh_best_estimate(forecast, "Janeiro")

    def test_abril_2026_eh_be(self, forecast):
        assert _mes_eh_best_estimate(forecast, "Abril")

    def test_maio_2026_eh_be(self, forecast):
        assert _mes_eh_best_estimate(forecast, "Maio")

    def test_dezembro_2026_eh_be(self, forecast):
        assert _mes_eh_best_estimate(forecast, "Dezembro")

    def test_none_retorna_false(self):
        assert not _mes_eh_best_estimate(None, "Abril")

    def test_vazio_retorna_false(self):
        assert not _mes_eh_best_estimate(pd.DataFrame(), "Abril")


# ───────────────────────────────────────────────────────────────
#  2. coletar_dados_mes() — mês BE (Abril 2026)
# ───────────────────────────────────────────────────────────────

class TestColetaDadosBE:
    @pytest.fixture(scope="class")
    def dados_abril(self):
        return coletar_dados_mes(2026, 4)

    def test_fonte_dados_be(self, dados_abril):
        assert dados_abril["fonte_dados"] == "Best Estimate"

    def test_custo_real_nao_vazio(self, dados_abril):
        df = dados_abril["custo_real"]
        assert df is not None and not df.empty

    def test_volume_real_nao_vazio(self, dados_abril):
        df = dados_abril["volume_real"]
        assert df is not None and not df.empty

    def test_volume_real_numerico(self, dados_abril):
        df = dados_abril["volume_real"]
        assert df is not None
        assert "Volume" in df.columns
        assert pd.api.types.is_numeric_dtype(df["Volume"])

    def test_cpu_real_nao_vazio(self, dados_abril):
        df = dados_abril["cpu_real"]
        assert df is not None and not df.empty

    def test_cpu_real_tem_colunas(self, dados_abril):
        df = dados_abril["cpu_real"]
        assert df is not None
        assert "Veículo" in df.columns
        assert "CPU" in df.columns

    def test_cpu_real_valores_positivos(self, dados_abril):
        """CPU BE deve ser positivo (custos reais por veículo)."""
        df = dados_abril["cpu_real"]
        assert df is not None
        cpus = pd.to_numeric(df["CPU"], errors="coerce").dropna()
        assert len(cpus) > 0
        assert cpus.mean() > 0, "CPU médio BE deve ser positivo"

    def test_custo_fp_real_nao_vazio(self, dados_abril):
        df = dados_abril["custo_fp_real"]
        assert df is not None and not df.empty

    def test_custo_fp_real_tem_coluna(self, dados_abril):
        df = dados_abril["custo_fp_real"]
        assert df is not None
        # forecast deve ter "Custo FP" ou "Total"
        assert "Custo FP" in df.columns or "Total" in df.columns

    def test_custo_real_ant_nao_vazio(self, dados_abril):
        """Mês anterior de Abril = Março (Real)."""
        df = dados_abril["custo_real_ant"]
        assert df is not None and not df.empty

    def test_volume_real_ant_nao_vazio(self, dados_abril):
        df = dados_abril["volume_real_ant"]
        assert df is not None and not df.empty


# ───────────────────────────────────────────────────────────────
#  3. coletar_dados_mes() — mês BE (Março 2026, via config)
# ───────────────────────────────────────────────────────────────

class TestColetaDadosMarco:
    """Março é BE conforme config_forecast.json (último período = Fevereiro)."""

    @pytest.fixture(scope="class")
    def dados_marco(self):
        return coletar_dados_mes(2026, 3)

    def test_fonte_dados_be(self, dados_marco):
        assert dados_marco["fonte_dados"] == "Best Estimate"

    def test_custo_real_nao_vazio(self, dados_marco):
        df = dados_marco["custo_real"]
        assert df is not None and not df.empty

    def test_custo_fp_real_nao_vazio(self, dados_marco):
        df = dados_marco["custo_fp_real"]
        assert df is not None and not df.empty


# ───────────────────────────────────────────────────────────────
#  4. calcular_variacoes() — dados BE coerentes
# ───────────────────────────────────────────────────────────────

class TestVariacoesBE:
    @pytest.fixture(scope="class")
    def variacoes_abril(self):
        dados = coletar_dados_mes(2026, 4)
        return calcular_variacoes(dados)

    def test_volume_real_positivo(self, variacoes_abril):
        assert variacoes_abril["volume"]["real"] > 0

    def test_custo_fp_real_positivo(self, variacoes_abril):
        assert variacoes_abril["custo_fp"]["real"] > 0

    def test_cpu_modelos_real_nao_vazio(self, variacoes_abril):
        cpu = variacoes_abril["cpu_modelos"]["real"]
        assert isinstance(cpu, dict)
        assert len(cpu) > 0, "cpu_modelos['real'] não deve ser vazio para meses BE"

    def test_variacao_modelos_nao_vazio(self, variacoes_abril):
        vm = variacoes_abril["variacao_modelos"]
        assert len(vm) > 0


# ───────────────────────────────────────────────────────────────
#  5. Cadeia de mês anterior — BE → Real, BE → BE
# ───────────────────────────────────────────────────────────────

class TestMesAnteriorCadeia:
    def test_abril_anterior_eh_marco_be(self):
        """Mês anterior de Abril (BE) é Março (também BE via config)."""
        dados = coletar_dados_mes(2026, 4)
        df_ant = dados["custo_real_ant"]
        assert df_ant is not None and not df_ant.empty

    def test_maio_anterior_eh_abril_be(self):
        """Mês anterior de Maio (BE) deve ser Abril (BE) com dados do forecast."""
        dados = coletar_dados_mes(2026, 5)
        df_ant = dados["custo_real_ant"]
        assert df_ant is not None and not df_ant.empty
