"""Testes — motor de alertas v3 + ranking consolidado + notificações."""

from __future__ import annotations

from datetime import date

import pandas as pd

from alertas.alert_engine import (
    MODOS_COMPARACAO,
    _filtrar,
    _get_mes_anterior,
    _safe_div,
    calcular_ranking_consolidado,
    calcular_ranking_por_oficina,
    classify_severity,
    evaluate_all_rules,
    evaluate_rule,
    fmt_cpu,
    fmt_delta_cpu,
    fmt_delta_k,
    fmt_k,
    fmt_linha_account,
    fmt_linha_type06,
    gerar_tabela_validacao,
    load_alert_log,
    load_alert_rules,
    normalizar_filtros_dependentes,
    normalizar_regra_alerta,
    save_alert_log,
    save_alert_rules,
)
from alertas.notifications_teams import build_teams_card_consolidated
from alertas.notifications_email import (
    build_email_html_consolidated,
    _tabela_html,
)


# =========================================================================
#  Fixtures
# =========================================================================

_COL = "Custo FP"


def _make_real_df() -> pd.DataFrame:
    """Real com 2 oficinas, 2 Type 06 cada, 2 períodos."""
    rows = [
        # Junho — Oficina OF1
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 7000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart"},
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scraps",
         "Oficina": "OF1", "Custo": "Variável", _COL: 3000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "rebut"},
        {"Type 05": "Burden", "Type 06": "Energy", "Account": "Electricity",
         "Oficina": "OF1", "Custo": "Fixo", _COL: 2000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "elec"},
        # Junho — Oficina OF2
        {"Type 05": "Labor", "Type 06": "Direct Labor", "Account": "Wages",
         "Oficina": "OF2", "Custo": "Fixo", _COL: 5000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "sal"},
        # Maio (mês anterior)
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 4000, "Período": "Maio", "Ano": 2025,
         "Texto breve": "ecart"},
        {"Type 05": "Burden", "Type 06": "Energy", "Account": "Electricity",
         "Oficina": "OF1", "Custo": "Fixo", _COL: 1500, "Período": "Maio", "Ano": 2025,
         "Texto breve": "elec"},
        {"Type 05": "Labor", "Type 06": "Direct Labor", "Account": "Wages",
         "Oficina": "OF2", "Custo": "Fixo", _COL: 3000, "Período": "Maio", "Ano": 2025,
         "Texto breve": "sal"},
    ]
    return pd.DataFrame(rows)


def _make_flex_df() -> pd.DataFrame:
    """Flex BUD detalhado (simula saída de calcular_flex_budget_detalhado)."""
    rows = [
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scrap Sales", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 3000},
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scraps", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 1000},
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Energy",
         "Account": "Electricity", "Custo": "Fixo", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 1800},
        {"Oficina": "OF2", "Type 05": "Labor", "Type 06": "Direct Labor",
         "Account": "Wages", "Custo": "Fixo", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 4000},
    ]
    return pd.DataFrame(rows)


def _make_vol_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"Volume": 5000, "Período": "Junho", "Ano": 2025},
        {"Volume": 5000, "Período": "Maio", "Ano": 2025},
    ])


def _make_data() -> dict:
    return {
        "real": _make_real_df(),
        "budget": None,
        "real_vol": _make_vol_df(),
        "budget_vol": None,
        "flex_detalhado": _make_flex_df(),
        "taxas": {"EUR": 0.18, "USD": 0.20},
    }


# =========================================================================
#  classify_severity
# =========================================================================

class TestClassifySeverity:
    def test_critico(self):
        assert classify_severity(20.0) == "critico"
        assert classify_severity(-16.0) == "critico"

    def test_moderado(self):
        assert classify_severity(10.0) == "moderado"

    def test_informativo(self):
        assert classify_severity(4.9) == "informativo"
        assert classify_severity(0.0) == "informativo"


# =========================================================================
#  _filtrar
# =========================================================================

class TestFiltrar:
    def test_sem_filtros_retorna_tudo(self):
        df = _make_real_df()
        assert len(_filtrar(df)) == len(df)

    def test_oficina(self):
        df = _make_real_df()
        r = _filtrar(df, oficinas=["OF1"])
        assert all(r["Oficina"] == "OF1")

    def test_periodo(self):
        df = _make_real_df()
        r = _filtrar(df, periodo="Junho")
        assert all(r["Período"] == "Junho")

    def test_type_05(self):
        df = _make_real_df()
        r = _filtrar(df, filtro_type_05=["Burden"])
        assert all(r["Type 05"] == "Burden")

    def test_type_06(self):
        df = _make_real_df()
        r = _filtrar(df, filtro_type_06=["Energy"])
        assert all(r["Type 06"] == "Energy")

    def test_account(self):
        df = _make_real_df()
        r = _filtrar(df, filtro_account=["Scrap Sales"])
        assert all(r["Account"] == "Scrap Sales")

    def test_none_df(self):
        assert _filtrar(None).empty

    def test_combined(self):
        df = _make_real_df()
        r = _filtrar(df, oficinas=["OF1"], periodo="Junho", filtro_type_06=["Material Losses"])
        assert len(r) == 2  # Scrap Sales + Scraps


class TestHelpers:
    def test_get_mes_anterior(self):
        assert _get_mes_anterior("Junho") == "Maio"
        assert _get_mes_anterior("Janeiro") is None
        assert _get_mes_anterior("invalido") is None


# =========================================================================
#  Formatação
# =========================================================================

class TestFormatacao:
    def test_fmt_k(self):
        assert fmt_k(26200, "EUR") == "26,2 kEUR"
        assert fmt_k(0, "EUR") == "0,0 kEUR"

    def test_fmt_cpu(self):
        assert fmt_cpu(6.8, "€") == "6,8 €/veíc"

    def test_fmt_delta_k(self):
        r = fmt_delta_k(75500, "EUR")
        assert "∆" in r
        assert "+" in r
        assert "75,5" in r
        assert "kEUR" in r

    def test_fmt_delta_k_negativo(self):
        r = fmt_delta_k(-10000, "EUR")
        assert "-10,0" in r

    def test_fmt_delta_cpu(self):
        r = fmt_delta_cpu(19.6, "€")
        assert "∆" in r
        assert "+19,6" in r
        assert "€/veíc" in r

    def test_fmt_linha_type06(self):
        item = {
            "type_06": "Material Losses",
            "real": 26200,
            "cpu_real": 6.8,
            "desvio": 75500,
            "delta_cpu": 19.6,
        }
        line = fmt_linha_type06(item, "EUR", "€")
        assert "Material Losses" in line
        assert "kEUR" in line
        assert "€/veíc" in line
        assert "∆" in line

    def test_fmt_linha_account(self):
        acc = {"account": "Scrap Sales", "desvio": 71200, "delta_cpu": 18.5}
        line = fmt_linha_account(acc, "EUR", "€")
        assert "Scrap Sales" in line
        assert "kEUR" in line
        assert line.startswith("  ")  # indentado


# =========================================================================
#  calcular_ranking_por_oficina — modo flex_bud_x_real
# =========================================================================

class TestRankingFlexBud:
    def test_retorna_por_oficina(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        # Deve ter 1 card por oficina que tenha desvio
        oficinas = {r["oficina"] for r in result}
        assert len(oficinas) >= 1

    def test_estrutura_resultado(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=["OF1"], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        assert len(result) == 1
        card = result[0]
        assert card["oficina"] == "OF1"
        assert "itens" in card
        assert "volume_total" in card
        assert "severidade" in card

        # Cada item tem hierarquia Type 06 → Accounts
        for it in card["itens"]:
            assert "type_06" in it
            assert "type_05" in it
            assert "real" in it
            assert "esperado" in it
            assert "desvio" in it
            assert "cpu_real" in it
            assert "delta_cpu" in it
            assert "accounts" in it
            for acc in it["accounts"]:
                assert "account" in acc
                assert "desvio" in acc
                assert "delta_cpu" in acc

    def test_top_n_limita(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=["OF1"], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=1,
            moeda="BRL",
        )
        if result:
            assert len(result[0]["itens"]) <= 1

    def test_oficina_filtra(self):
        """OF2 card não deve conter Material Losses (que é de OF1)."""
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=["OF2"], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        for card in result:
            for it in card["itens"]:
                assert it["type_06"] != "Material Losses"

    def test_periodo_sem_dados(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Dezembro",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        assert result == []

    def test_conversao_moeda(self):
        """Com moeda EUR, valores devem ser convertidos."""
        data = _make_data()
        result_brl = calcular_ranking_por_oficina(
            data, oficinas=["OF1"], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        result_eur = calcular_ranking_por_oficina(
            data, oficinas=["OF1"], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            moeda="EUR",
        )
        if result_brl and result_eur:
            # EUR values should be different from BRL (multiplied by 0.18)
            brl_real = result_brl[0]["itens"][0]["real"]
            eur_real = result_eur[0]["itens"][0]["real"]
            assert abs(eur_real - brl_real * 0.18) < 1.0

    def test_filtro_type05(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            filtro_type_05=["Labor"], moeda="BRL",
        )
        for card in result:
            for it in card["itens"]:
                assert it["type_05"] == "Labor"

    def test_filtro_type06(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Junho",
            modo="flex_bud_x_real", proporcao=0.5, top_n=10,
            filtro_type_06=["Energy"], moeda="BRL",
        )
        for card in result:
            for it in card["itens"]:
                assert it["type_06"] == "Energy"


# =========================================================================
#  calcular_ranking_por_oficina — modo mes_x_mes_anterior
# =========================================================================

class TestRankingMesAnterior:
    def test_ranking_basico(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=["OF1"], periodo="Junho",
            modo="mes_x_mes_anterior", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        assert len(result) >= 1

    def test_janeiro_sem_anterior(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Janeiro",
            modo="mes_x_mes_anterior", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        assert result == []

    def test_modo_desconhecido(self):
        data = _make_data()
        result = calcular_ranking_por_oficina(
            data, oficinas=[], periodo="Junho",
            modo="inventado", proporcao=0.5, top_n=10,
            moeda="BRL",
        )
        assert result == []


# =========================================================================
#  evaluate_rule
# =========================================================================

class TestEvaluateRule:
    def test_retorna_alertas_por_oficina(self):
        data = _make_data()
        rule = {
            "id": "r1",
            "nome": "Teste",
            "ativo": True,
            "oficinas": [],
            "modo_comparacao": "flex_bud_x_real",
            "top_n": 5,
            "ano": 2025,
            "moeda": "BRL",
        }
        alertas = evaluate_rule(rule, data, proporcao=0.5, periodo="Junho")
        assert len(alertas) >= 1
        for al in alertas:
            assert "ranking" in al
            assert "mensagem" in al
            # Mensagem deve estar no formato relatório
            assert "∆" in al["mensagem"] or al["mensagem"] == ""

    def test_com_filtros(self):
        data = _make_data()
        rule = {
            "id": "r2",
            "oficinas": ["OF1"],
            "modo_comparacao": "flex_bud_x_real",
            "top_n": 5,
            "filtro_type_05": ["Burden"],
            "moeda": "BRL",
        }
        alertas = evaluate_rule(rule, data, proporcao=0.5, periodo="Junho")
        for al in alertas:
            meta = al.get("metadata", {})
            assert meta.get("oficina") == "OF1"


# =========================================================================
#  evaluate_all_rules
# =========================================================================

class TestEvaluateAllRules:
    def test_filtra_inativos(self):
        data = _make_data()
        rules = [
            {"id": "r1", "ativo": True, "oficinas": ["OF1"],
             "modo_comparacao": "flex_bud_x_real", "top_n": 3,
             "ano": 2025, "moeda": "BRL"},
            {"id": "r2", "ativo": False, "oficinas": ["OF1"],
             "modo_comparacao": "flex_bud_x_real", "top_n": 3,
             "ano": 2025, "moeda": "BRL"},
        ]
        alertas = evaluate_all_rules(data, rules, "Junho", date(2025, 6, 15))
        rule_ids = {a["rule_id"] for a in alertas}
        assert "r1" in rule_ids
        assert "r2" not in rule_ids


# =========================================================================
#  Persistência
# =========================================================================

class TestPersistencia:
    def test_load_rules_inexistente(self, tmp_path):
        path = str(tmp_path / "rules.json")
        data = load_alert_rules(path)
        assert "rules" in data
        assert "config" in data

    def test_save_and_load_rules(self, tmp_path):
        path = str(tmp_path / "rules.json")
        payload = {"config": {"test": True}, "rules": [{"id": "r1"}]}
        save_alert_rules(payload, path)
        loaded = load_alert_rules(path)
        assert loaded["rules"][0]["id"] == "r1"

    def test_load_rules_normaliza_schedule_legado(self, tmp_path):
        path = str(tmp_path / "rules.json")
        payload = {
            "config": {"schedule": {"enabled": True, "hour": 9}},
            "rules": [{"id": "r1", "ano": 2025}],
        }
        save_alert_rules(payload, path)
        loaded = load_alert_rules(path)
        schedule = loaded["rules"][0]["schedule"]
        assert schedule["enabled"] is True
        assert schedule["hour"] == 9
        assert schedule["frequency"] == "daily"

    def test_save_and_load_log(self, tmp_path):
        path = str(tmp_path / "log.json")
        save_alert_log([{"id": "a1"}], path)
        assert load_alert_log(path)[0]["id"] == "a1"

    def test_load_log_inexistente(self, tmp_path):
        assert load_alert_log(str(tmp_path / "log.json")) == []


# =========================================================================
#  Constantes
# =========================================================================

class TestConstantes:
    def test_modos_definidos(self):
        assert "flex_bud_x_real" in MODOS_COMPARACAO
        assert "mes_x_mes_anterior" in MODOS_COMPARACAO


class TestNormalizacaoRegras:
    def test_normaliza_filtros_dependentes(self, monkeypatch):
        monkeypatch.setattr(
            "alertas.alert_engine.type06_disponiveis",
            lambda ano, type05_list=None: ["T6A"] if type05_list == ["T5A"] else [],
        )
        monkeypatch.setattr(
            "alertas.alert_engine.accounts_disponiveis",
            lambda ano, type06_list=None: ["ACC1"] if type06_list == ["T6A"] else [],
        )

        filtros = normalizar_filtros_dependentes(
            2025,
            ["T5A"],
            ["T6A", "T6B"],
            ["ACC1", "ACC9"],
        )
        assert filtros == {
            "filtro_type_05": ["T5A"],
            "filtro_type_06": ["T6A"],
            "filtro_account": ["ACC1"],
        }

    def test_normaliza_regra_com_schedule_mensal(self, monkeypatch):
        monkeypatch.setattr(
            "alertas.alert_engine.type06_disponiveis",
            lambda ano, type05_list=None: ["T6A"],
        )
        monkeypatch.setattr(
            "alertas.alert_engine.accounts_disponiveis",
            lambda ano, type06_list=None: ["ACC1"],
        )

        regra = normalizar_regra_alerta({
            "id": "r1",
            "ano": 2025,
            "filtro_type_05": ["T5A"],
            "filtro_type_06": ["T6A"],
            "filtro_account": ["ACC1"],
            "schedule": {
                "enabled": True,
                "frequency": "monthly",
                "hour": 7,
                "minute": 30,
                "days_of_month": [15, 3, 15],
            },
        }, {"schedule": {"enabled": False, "hour": 8}})

        assert regra["schedule"] == {
            "enabled": True,
            "frequency": "monthly",
            "hour": 7,
            "minute": 30,
            "start_day_of_month": 1,
            "days_of_week": [],
            "days_of_month": [3, 15],
        }


# =========================================================================
#  calcular_ranking_consolidado
# =========================================================================

def _make_rich_real_df() -> pd.DataFrame:
    """Real com múltiplas oficinas, accounts e Texto breve (4+ por oficina)."""
    rows = [
        # OF1 — Material Losses / Scrap Sales
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 3000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart A"},
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 2500, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart B"},
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 2000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart C"},
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF1", "Custo": "Variável", _COL: 1500, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart D"},  # 4th — must NOT appear in top 3

        # OF1 — Material Losses / Scraps
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scraps",
         "Oficina": "OF1", "Custo": "Variável", _COL: 4000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "rebut"},

        # OF2 — Material Losses / Scrap Sales (perde)
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF2", "Custo": "Variável", _COL: 5000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart X"},

        # OF3 — Material Losses / Scrap Sales (perde menos)
        {"Type 05": "Burden", "Type 06": "Material Losses", "Account": "Scrap Sales",
         "Oficina": "OF3", "Custo": "Variável", _COL: 1800, "Período": "Junho", "Ano": 2025,
         "Texto breve": "ecart Y"},

        # OF1 — Energy / Electricity
        {"Type 05": "Burden", "Type 06": "Energy", "Account": "Electricity",
         "Oficina": "OF1", "Custo": "Fixo", _COL: 2000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "elec"},

        # OF2 — Direct Labor / Wages
        {"Type 05": "Labor", "Type 06": "Direct Labor", "Account": "Wages",
         "Oficina": "OF2", "Custo": "Fixo", _COL: 6000, "Período": "Junho", "Ano": 2025,
         "Texto breve": "sal"},
    ]
    return pd.DataFrame(rows)


def _make_rich_flex_df() -> pd.DataFrame:
    """Flex BUD que gera desvio positivo em Material Losses e Wages."""
    rows = [
        # Material Losses — Esperado < Real → desvio positivo
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scrap Sales", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 4000},
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scraps", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 2000},
        {"Oficina": "OF2", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scrap Sales", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 2000},
        {"Oficina": "OF3", "Type 05": "Burden", "Type 06": "Material Losses",
         "Account": "Scrap Sales", "Custo": "Variável", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 1000},

        # Energy — Esperado próximo de Real (pouco desvio)
        {"Oficina": "OF1", "Type 05": "Burden", "Type 06": "Energy",
         "Account": "Electricity", "Custo": "Fixo", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 1900},

        # Wages — desvio positivo
        {"Oficina": "OF2", "Type 05": "Labor", "Type 06": "Direct Labor",
         "Account": "Wages", "Custo": "Fixo", "Ano": 2025,
         "Período": "Junho", "Flex_Bud": 4000},
    ]
    return pd.DataFrame(rows)


def _make_rich_data() -> dict:
    return {
        "real": _make_rich_real_df(),
        "budget": None,
        "real_vol": _make_vol_df(),
        "budget_vol": None,
        "flex_detalhado": _make_rich_flex_df(),
        "taxas": {"EUR": 0.18, "USD": 0.20},
    }


class TestRankingConsolidado:
    """Testes para calcular_ranking_consolidado (card único hierárquico)."""

    def test_retorna_dict_unico(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        assert isinstance(result, dict)
        # NÃO é lista (diferente de calcular_ranking_por_oficina)
        assert not isinstance(result, list)

    def test_estrutura_basica(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        # Campos obrigatórios do resultado
        for key in ("periodo", "volume_total", "moeda", "simbolo",
                     "severidade", "total_desvio", "itens"):
            assert key in result, f"Campo '{key}' ausente"

        assert result["periodo"] == "Junho"
        assert result["moeda"] == "BRL"
        assert result["severidade"] in ("critico", "moderado", "informativo")
        assert len(result["itens"]) >= 1

    def test_estrutura_item(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        item = result["itens"][0]
        for key in ("type_06", "type_05", "real", "esperado",
                     "desvio", "desvio_pct", "cpu_real", "delta_cpu",
                     "severidade", "accounts"):
            assert key in item, f"Campo '{key}' ausente no item"

    def test_accounts_tem_oficinas(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        # Pelo menos uma account deve ter oficinas
        found_oficinas = False
        for it in result["itens"]:
            for acc in it["accounts"]:
                assert "oficinas" in acc
                if acc["oficinas"]:
                    found_oficinas = True
                    ofi = acc["oficinas"][0]
                    for key in ("oficina", "desvio", "delta_cpu", "textos"):
                        assert key in ofi, f"Campo '{key}' ausente na oficina"
        assert found_oficinas, "Nenhuma oficina encontrada nos accounts"

    def test_todas_oficinas_perdendo_incluidas(self):
        """Não deve limitar o número de oficinas — todas com desvio > 0."""
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        # Material Losses / Scrap Sales tem 3 oficinas com Real > Esperado
        for it in result["itens"]:
            if it["type_06"] == "Material Losses":
                for acc in it["accounts"]:
                    if acc["account"] == "Scrap Sales":
                        ofi_names = {o["oficina"] for o in acc["oficinas"]}
                        # OF1, OF2, OF3 — todas com desvio positivo
                        assert len(ofi_names) >= 2  # pelo menos OF1, OF2 (pode variar com proporcao)
                        # Todas devem ter desvio > 0
                        for o in acc["oficinas"]:
                            assert o["desvio"] > 0

    def test_texto_breve_max_3_por_oficina(self):
        """Texto breve deve ter no máximo 3 por oficina."""
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        for it in result["itens"]:
            for acc in it["accounts"]:
                for ofi in acc["oficinas"]:
                    assert len(ofi["textos"]) <= 3, (
                        f"Oficina {ofi['oficina']} tem {len(ofi['textos'])} textos (max 3)"
                    )

    def test_texto_breve_of1_exclui_quarto(self):
        """OF1/Scrap Sales tem 4 textos no real, deve retornar só top 3."""
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert result is not None
        for it in result["itens"]:
            if it["type_06"] == "Material Losses":
                for acc in it["accounts"]:
                    if acc["account"] == "Scrap Sales":
                        for ofi in acc["oficinas"]:
                            if ofi["oficina"] == "OF1":
                                nomes = {t["texto"] for t in ofi["textos"]}
                                # ecart d (1500 — menor) não deve estar no top 3
                                assert "ecart d" not in nomes
                                # ecart a (3000), b (2500), c (2000) são top 3
                                assert len(nomes) == 3
                                assert nomes == {"ecart a", "ecart b", "ecart c"}

    def test_top_n_limita_itens(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=1, moeda="BRL",
        )
        assert result is not None
        assert len(result["itens"]) <= 1

    def test_conversao_moeda_eur(self):
        data = _make_rich_data()
        brl = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        eur = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="EUR",
        )
        assert brl is not None and eur is not None
        # EUR real = BRL real * 0.18
        brl_d = brl["itens"][0]["desvio"]
        eur_d = eur["itens"][0]["desvio"]
        assert abs(eur_d - brl_d * 0.18) < 1.0
        assert eur["simbolo"] == "€"

    def test_periodo_sem_dados(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Dezembro", modo="flex_bud_x_real",
            proporcao=0.5, moeda="BRL",
        )
        assert result is None

    def test_modo_desconhecido(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="inventado",
            proporcao=0.5, moeda="BRL",
        )
        assert result is None

    def test_mes_x_mes_anterior(self):
        """Modo mês anterior com dados da fixture original (tem Maio)."""
        data = _make_data()  # fixture original com dados de Maio
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="mes_x_mes_anterior",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        # Pode retornar None se nenhum desvio positivo — ok
        if result is not None:
            assert isinstance(result, dict)
            assert result["periodo"] == "Junho"

    def test_filtro_type05(self):
        data = _make_rich_data()
        result = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, filtro_type_05=["Labor"], moeda="BRL",
        )
        if result is not None:
            for it in result["itens"]:
                assert it["type_05"] == "Labor"


# =========================================================================
#  build_teams_card_consolidated
# =========================================================================

class TestTeamsCardConsolidado:
    def test_estrutura_card(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        card = build_teams_card_consolidated(ranking, "flex_bud_x_real")
        assert card["@type"] == "MessageCard"
        assert "sections" in card
        section = card["sections"][0]
        # Agora usa text (árvore hierárquica) em vez de facts
        assert "text" in section
        assert len(section["text"]) > 0

    def test_card_summary(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        card = build_teams_card_consolidated(ranking, "flex_bud_x_real")
        assert "Junho" in card["summary"]

    def test_card_com_tabela(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        tabela_df = gerar_tabela_validacao(
            data, oficina=None, periodo="Junho",
            proporcao=0.5, moeda="BRL",
        )
        card = build_teams_card_consolidated(
            ranking, "flex_bud_x_real", tabela_df,
        )
        # Deve ter 2 sections: ranking + tabela
        assert len(card["sections"]) == 2
        tab_section = card["sections"][1]
        assert "Tabela de Validação" in tab_section["activityTitle"]
        assert "text" in tab_section
        assert len(tab_section["text"]) > 0

    def test_card_texto_hierarquico(self):
        """Texto do card deve conter caracteres de árvore e ícones."""
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        card = build_teams_card_consolidated(ranking, "flex_bud_x_real")
        text = card["sections"][0]["text"]
        # Deve conter caracteres de árvore
        assert any(c in text for c in ("├─", "└─"))
        # Deve conter ícones de severidade
        assert any(icon in text for icon in ("🔴", "🟠", "🟡", "🟢"))
        # Deve conter legenda (espaços viram &nbsp; no HTML Teams)
        assert "Desvio" in text and "Total" in text
        assert "maior" in text and "desvio" in text
        assert "█" in text


# =========================================================================
#  Tabela de validação — sort
# =========================================================================

class TestTabelaValidacaoSort:
    def test_sort_por_desvio_desc(self):
        """Tabela deve estar ordenada por 'Real - Flex BUD P' decrescente."""
        data = _make_rich_data()
        df = gerar_tabela_validacao(
            data, oficina=None, periodo="Junho",
            proporcao=0.5, moeda="BRL",
        )
        assert not df.empty
        vals = df["Real - Flex BUD P"].tolist()
        # Verificar que está em ordem decrescente
        for i in range(len(vals) - 1):
            assert vals[i] >= vals[i + 1], (
                f"Linha {i}: {vals[i]} < {vals[i+1]} — não está decrescente"
            )

    def test_sort_primeiro_valor_maior(self):
        data = _make_rich_data()
        df = gerar_tabela_validacao(
            data, oficina=None, periodo="Junho",
            proporcao=0.5, moeda="BRL",
        )
        if len(df) > 1:
            assert df.iloc[0]["Real - Flex BUD P"] >= df.iloc[-1]["Real - Flex BUD P"]


# =========================================================================
#  Email consolidado
# =========================================================================

class TestEmailConsolidado:
    def test_html_contem_ranking(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        html = build_email_html_consolidated(
            ranking, "flex_bud_x_real", 0.5,
        )
        assert "Ranking de Desvios" in html
        assert "Junho" in html
        assert "Material Losses" in html

    def test_html_com_tabela(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        tabela_df = gerar_tabela_validacao(
            data, oficina=None, periodo="Junho",
            proporcao=0.5, moeda="BRL",
        )
        html = build_email_html_consolidated(
            ranking, "flex_bud_x_real", 0.5, tabela_df,
        )
        assert "Tabela de Validação" in html
        assert "<table" in html

    def test_html_sem_tabela(self):
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        assert ranking is not None
        html = build_email_html_consolidated(
            ranking, "flex_bud_x_real", 0.5, None,
        )
        assert "Tabela de Validação" not in html

    def test_tabela_html_func(self):
        data = _make_rich_data()
        df = gerar_tabela_validacao(
            data, oficina=None, periodo="Junho",
            proporcao=0.5, moeda="BRL",
        )
        html = _tabela_html(df)
        assert "<table" in html
        assert "<thead>" in html
        assert "Type 06" in html

    def test_tabela_html_vazia(self):
        assert _tabela_html(pd.DataFrame()) == ""
        assert _tabela_html(None) == ""


class TestEmailParseRecipients:
    def test_parse_bare_email(self):
        from alertas.email_graph import parse_email
        assert parse_email("user@example.com") == "user@example.com"

    def test_parse_display_name_format(self):
        from alertas.email_graph import parse_email
        r = '"FREDERICO DE JESUS" <frederico.dejesus@stellantis.com>'
        assert parse_email(r) == "frederico.dejesus@stellantis.com"

    def test_parse_angle_brackets_only(self):
        from alertas.email_graph import parse_email
        assert parse_email("<user@test.com>") == "user@test.com"


class TestBuildRankingText:
    def test_contem_arvore(self):
        from alertas.notifications_teams import _build_ranking_text
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        text = _build_ranking_text(ranking)
        assert any(c in text for c in ("├─", "└─"))
        assert "📍" in text

    def test_contem_oficinas(self):
        from alertas.notifications_teams import _build_ranking_text
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        text = _build_ranking_text(ranking)
        # Ranking deve listar oficinas que perdem
        assert "📍" in text

    def test_texto_breve_lowercase_e_barra(self):
        from alertas.notifications_teams import _build_ranking_text
        data = _make_rich_data()
        ranking = calcular_ranking_consolidado(
            data, periodo="Junho", modo="flex_bud_x_real",
            proporcao=0.5, top_n=10, moeda="BRL",
        )
        text = _build_ranking_text(ranking)
        assert "sal" in text
        assert "maior" in text and "desvio" in text


# =========================================================================
#  Graph API — email_graph.py
# =========================================================================

class TestGraphPayload:
    def test_build_payload_estrutura(self):
        from alertas.email_graph import build_send_mail_payload
        payload = build_send_mail_payload(
            subject="Teste",
            html_body="<h1>Oi</h1>",
            recipients=["user@test.com"],
        )
        assert "message" in payload
        msg = payload["message"]
        assert msg["subject"] == "Teste"
        assert msg["body"]["contentType"] == "HTML"
        assert msg["body"]["content"] == "<h1>Oi</h1>"
        assert len(msg["toRecipients"]) == 1
        assert msg["toRecipients"][0]["emailAddress"]["address"] == "user@test.com"
        assert payload["saveToSentItems"] is True

    def test_build_payload_multiple_recipients(self):
        from alertas.email_graph import build_send_mail_payload
        payload = build_send_mail_payload(
            subject="Multi",
            html_body="<p>test</p>",
            recipients=["a@test.com", '"Nome" <b@test.com>'],
        )
        addrs = [
            r["emailAddress"]["address"]
            for r in payload["message"]["toRecipients"]
        ]
        assert addrs == ["a@test.com", "b@test.com"]

    def test_build_payload_display_name_stripped(self):
        from alertas.email_graph import build_send_mail_payload
        payload = build_send_mail_payload(
            subject="S",
            html_body="B",
            recipients=['"LAURO PAIVA" <lauro@stellantis.com>'],
        )
        addr = payload["message"]["toRecipients"][0]["emailAddress"]["address"]
        assert addr == "lauro@stellantis.com"


class TestGraphClient:
    """Testes do GraphEmailClient com MSAL mockado."""

    def _make_client(self, tmp_path, monkeypatch):
        """Helper: cria GraphEmailClient com MSAL mockado."""
        import alertas.email_graph as eg

        class FakeApp:
            def __init__(self, *a, **kw):
                pass
            def get_accounts(self):
                return []
            def initiate_device_flow(self, scopes=None):
                return {"user_code": "ABC123", "verification_uri": "https://microsoft.com/devicelogin"}
            def acquire_token_by_device_flow(self, flow):
                return {"access_token": "tok123"}
            def acquire_token_silent(self, scopes=None, account=None):
                return None

        monkeypatch.setattr(eg.msal, "PublicClientApplication", FakeApp)
        cache_path = tmp_path / ".token_cache.json"
        return eg.GraphEmailClient(
            client_id="fake-id",
            tenant_id="fake-tenant",
            token_cache_path=str(cache_path),
        )

    def test_client_init_sem_token(self, tmp_path, monkeypatch):
        client = self._make_client(tmp_path, monkeypatch)
        assert not client.is_authenticated()
        assert client.get_account_info() is None

    def test_client_acquire_silent_sem_account(self, tmp_path, monkeypatch):
        client = self._make_client(tmp_path, monkeypatch)
        assert client.acquire_token_silent() is None

    def test_client_get_token_raises_sem_auth(self, tmp_path, monkeypatch):
        import pytest
        from alertas.email_graph import GraphAuthError
        client = self._make_client(tmp_path, monkeypatch)
        with pytest.raises(GraphAuthError, match="Token expirado"):
            client._get_token()

    def test_send_email_401_raises_auth_error(self, tmp_path, monkeypatch):
        import pytest
        from alertas.email_graph import GraphAuthError
        client = self._make_client(tmp_path, monkeypatch)
        monkeypatch.setattr(client, "_get_token", lambda: "fake-token")

        class FakeResp:
            status_code = 401
            text = "Unauthorized"
        monkeypatch.setattr("alertas.email_graph.requests.post", lambda *a, **kw: FakeResp())

        with pytest.raises(GraphAuthError, match="401"):
            client.send_email("Teste", "<p>body</p>", ["x@test.com"])

    def test_send_email_403_raises_permission_error(self, tmp_path, monkeypatch):
        import pytest
        from alertas.email_graph import GraphPermissionError
        client = self._make_client(tmp_path, monkeypatch)
        monkeypatch.setattr(client, "_get_token", lambda: "fake-token")

        class FakeResp:
            status_code = 403
            text = "Forbidden"
        monkeypatch.setattr("alertas.email_graph.requests.post", lambda *a, **kw: FakeResp())

        with pytest.raises(GraphPermissionError, match="403"):
            client.send_email("Teste", "<p>b</p>", ["x@test.com"])

    def test_send_email_202_success(self, tmp_path, monkeypatch):
        client = self._make_client(tmp_path, monkeypatch)
        monkeypatch.setattr(client, "_get_token", lambda: "fake-token")

        class FakeResp:
            status_code = 202
            text = ""
        monkeypatch.setattr("alertas.email_graph.requests.post", lambda *a, **kw: FakeResp())

        # Não deve levantar exceção
        client.send_email("Teste", "<p>ok</p>", ["x@test.com"])

    def test_logout_remove_cache(self, tmp_path, monkeypatch):
        cache_path = tmp_path / ".token_cache.json"
        cache_path.write_text("{}", encoding="utf-8")
        assert cache_path.exists()

        client = self._make_client(tmp_path, monkeypatch)
        client.logout()
        assert not cache_path.exists()


class TestSendGraphIntegration:
    def test_send_graph_missing_config(self):
        import pytest
        from alertas.notifications_email import _send_graph
        with pytest.raises(ValueError, match="client_id"):
            _send_graph("Test", "<p>x</p>", {"recipients": ["a@b.com"]})

    def test_send_graph_missing_recipients(self):
        import pytest
        from alertas.notifications_email import _send_graph
        with pytest.raises(ValueError, match="destinatário"):
            _send_graph("Test", "<p>x</p>", {
                "client_id": "x", "tenant_id": "y", "recipients": [],
            })


# =========================================================================
#  Scheduler
# =========================================================================

class TestScheduler:
    def test_init_and_stop(self):
        from alertas.scheduler import init_scheduler, stop_scheduler, is_running
        # Não deve falhar mesmo sem regras
        init_scheduler(hour=23, minute=59)
        assert is_running()
        stop_scheduler()
        assert not is_running()

    def test_restart_sem_config(self):
        from alertas.scheduler import restart_scheduler, is_running, stop_scheduler
        # Se não tiver schedule.enabled, deve parar
        restart_scheduler()
        # Pode estar parado se config não tem schedule.enabled
        stop_scheduler()  # cleanup

    def test_build_rule_trigger_kwargs(self):
        from alertas.scheduler import _build_rule_trigger_kwargs

        semanal = _build_rule_trigger_kwargs({
            "enabled": True,
            "frequency": "weekly",
            "hour": 10,
            "minute": 15,
            "days_of_week": ["Segunda", "Sexta"],
        })
        assert semanal == {
            "hour": 10,
            "minute": 15,
            "day_of_week": "mon,fri",
        }

        mensal = _build_rule_trigger_kwargs({
            "enabled": True,
            "frequency": "monthly",
            "hour": 8,
            "minute": 0,
            "days_of_month": [5, 20],
        })
        assert mensal == {
            "hour": 8,
            "minute": 0,
            "day": "5,20",
        }

    def test_schedule_matches_date_daily_start_day(self):
        from alertas.scheduler import _schedule_matches_date

        schedule = {
            "enabled": True,
            "frequency": "daily",
            "start_day_of_month": 10,
        }
        assert not _schedule_matches_date(schedule, date(2025, 6, 9))
        assert _schedule_matches_date(schedule, date(2025, 6, 10))

    def test_iter_rule_job_specs_filtra_regras(self):
        from alertas.scheduler import _iter_rule_job_specs

        jobs = _iter_rule_job_specs({
            "config": {"schedule": {"enabled": True}},
            "rules": [
                {
                    "id": "r1",
                    "ativo": True,
                    "schedule": {
                        "enabled": True,
                        "frequency": "weekly",
                        "hour": 9,
                        "minute": 30,
                        "days_of_week": ["Quarta"],
                    },
                },
                {
                    "id": "r2",
                    "ativo": False,
                    "schedule": {"enabled": True, "frequency": "daily"},
                },
                {
                    "id": "r3",
                    "ativo": True,
                    "schedule": {"enabled": False, "frequency": "daily"},
                },
            ],
        })

        assert jobs == [{
            "id": "rule_alert_check_r1",
            "rule_id": "r1",
            "trigger_kwargs": {
                "hour": 9,
                "minute": 30,
                "day_of_week": "wed",
            },
        }]
