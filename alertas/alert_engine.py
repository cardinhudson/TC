"""Motor de alertas — ranking de perdas por Type 06 → Account.

Fonte de dados: **TC_Principal** (TC Veículos).
Usa ``calcular_flex_budget_detalhado`` de ``tc_principal.shared``
para obter Flex BUD com dimensões (Oficina/Type 05/Type 06/Account).

Dois modos de comparação:
  1. Budget Flex × Real  (principal)
  2. Mês × Mês Anterior  (secundário)

Resultado: **um card por oficina** com Top N piores Type 06,
sub-itens Account, valores em kMOEDA + CPU (moeda/veíc) + ∆.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date
from pathlib import Path

import pandas as pd

from tc_core.constants import ORDEM_MESES
from tc_core.data.paths import PASTA_TC_PRINCIPAL
from tc_core.data.periodos import normalizar_coluna_periodo
from tc_core.data.schema import normalize_common_column_mojibake
from tc_core.finance.currency import converter_moeda, obter_simbolo_moeda
from tc_core.finance.currency_db import carregar_taxas_banco
from tc_principal.shared import calcular_flex_budget_detalhado

from alertas.utils_dates import (
    mes_atual_nome,
    proporcao_mes,
    timestamp_agora_iso,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caminhos — TC_Principal (TC Veículos)
# ---------------------------------------------------------------------------
_HIST = os.path.join(PASTA_TC_PRINCIPAL, "historico_consolidado")
_PATH_REAL = os.path.join(_HIST, "df_principal_historico.parquet")
_PATH_REAL_VOL = os.path.join(_HIST, "df_vol_historico.parquet")
_PATH_BUD = os.path.join(_HIST, "BUD", "df_principal_historico_BUD.parquet")
_PATH_BUD_VOL = os.path.join(_HIST, "BUD", "df_vol_historico_BUD.parquet")

# Persistência
_DIR = Path(__file__).resolve().parent
_DEFAULT_RULES = str(_DIR / "alert_rules.json")
_DEFAULT_LOG = str(_DIR / "alert_log.json")

# Modos de comparação
MODOS_COMPARACAO = {
    "flex_bud_x_real": "Budget Flex × Real",
    "mes_x_mes_anterior": "Mês × Mês Anterior",
}

# Severidade (desvio % absoluto)
_SEV_CRITICO = 15.0
_SEV_MODERADO = 5.0

# Coluna de custo usada em toda a base (mesmo que shared.py)
_COL_CUSTO = "Custo FP"


# =========================================================================
#  Persistência
# =========================================================================

def _default_config() -> dict:
    return {
        "email": {
            "smtp_host": "smtp.office365.com",
            "smtp_port": 587,
            "sender": "",
            "recipients": [],
            "use_tls": True,
        },
        "teams_webhook_url": "",
        "notifications_enabled": {
            "internal": True,
            "email": False,
            "teams": False,
        },
        "ultima_execucao": None,
    }


def load_alert_rules(path: str = _DEFAULT_RULES) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"config": _default_config(), "rules": []}


def save_alert_rules(data: dict, path: str = _DEFAULT_RULES) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def load_alert_log(path: str = _DEFAULT_LOG) -> list[dict]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return []


def save_alert_log(log: list[dict], path: str = _DEFAULT_LOG) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(log, fh, ensure_ascii=False, indent=2)


def append_to_alert_log(alert: dict, path: str = _DEFAULT_LOG) -> None:
    log = load_alert_log(path)
    log.append(alert)
    save_alert_log(log, path)


# =========================================================================
#  Leitura de Parquets
# =========================================================================

def _read_parquet_safe(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        logger.warning("Parquet não encontrado: %s", path)
        return None
    df = pd.read_parquet(path)
    df = normalize_common_column_mojibake(df)
    df = normalizar_coluna_periodo(df, "Período")
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    return df


def load_all_data(ano: int) -> dict:
    """Carrega todos os DataFrames necessários para o ano.

    Retorna dict com:
      real, budget, real_vol, budget_vol, flex_detalhado, taxas
    """
    resultado: dict = {}
    for chave, caminho in [
        ("real", _PATH_REAL),
        ("real_vol", _PATH_REAL_VOL),
        ("budget", _PATH_BUD),
        ("budget_vol", _PATH_BUD_VOL),
    ]:
        df = _read_parquet_safe(caminho)
        if df is not None and "Ano" in df.columns:
            df = df[df["Ano"] == ano].copy()
        resultado[chave] = df

    # Flex BUD detalhado — reutiliza cálculo existente de shared.py
    df_bud = resultado.get("budget")
    df_bud_vol = resultado.get("budget_vol")
    df_real_vol = resultado.get("real_vol")
    if df_bud is not None and df_bud_vol is not None and df_real_vol is not None:
        resultado["flex_detalhado"] = calcular_flex_budget_detalhado(
            df_bud, df_bud_vol, df_real_vol,
            col_custo=_COL_CUSTO, tem_ano=True,
        )
    else:
        resultado["flex_detalhado"] = None

    # Taxas de câmbio
    resultado["taxas"] = carregar_taxas_banco()

    return resultado


# =========================================================================
#  Helpers de consulta (periodos, oficinas, dimensões)
# =========================================================================

def periodos_disponiveis(ano: int) -> list[str]:
    """Períodos com dados Real para *ano*, em ordem cronológica."""
    df = _read_parquet_safe(_PATH_REAL)
    if df is None or "Período" not in df.columns:
        return []
    if "Ano" in df.columns:
        df = df[df["Ano"] == ano]
    periodos = set(df["Período"].dropna().unique())
    return [m for m in ORDEM_MESES if m in periodos]


def oficinas_disponiveis(ano: int) -> list[str]:
    df = _read_parquet_safe(_PATH_REAL)
    if df is None or "Oficina" not in df.columns:
        return []
    if "Ano" in df.columns:
        df = df[df["Ano"] == ano]
    return sorted(df["Oficina"].dropna().unique().tolist())


def _dimensoes_disponiveis(
    ano: int, coluna: str,
    filtro_col: str | None = None,
    filtro_vals: list[str] | None = None,
) -> list[str]:
    """Valores únicos de *coluna*, opcionalmente filtrados por outra coluna."""
    df = _read_parquet_safe(_PATH_REAL)
    if df is None or coluna not in df.columns:
        return []
    if "Ano" in df.columns:
        df = df[df["Ano"] == ano]
    if filtro_col and filtro_vals and filtro_col in df.columns:
        df = df[df[filtro_col].isin(filtro_vals)]
    return sorted(df[coluna].dropna().unique().tolist())


def type05_disponiveis(ano: int) -> list[str]:
    return _dimensoes_disponiveis(ano, "Type 05")


def type06_disponiveis(
    ano: int, type05_list: list[str] | None = None,
) -> list[str]:
    if type05_list:
        return _dimensoes_disponiveis(ano, "Type 06", "Type 05", type05_list)
    return _dimensoes_disponiveis(ano, "Type 06")


def accounts_disponiveis(
    ano: int, type06_list: list[str] | None = None,
) -> list[str]:
    if type06_list:
        return _dimensoes_disponiveis(ano, "Account", "Type 06", type06_list)
    return _dimensoes_disponiveis(ano, "Account")


# =========================================================================
#  Helpers de filtragem
# =========================================================================

def _filtrar(
    df: pd.DataFrame | None,
    oficinas: list[str] | None = None,
    periodo: str | None = None,
    filtro_type_05: list[str] | None = None,
    filtro_type_06: list[str] | None = None,
    filtro_account: list[str] | None = None,
) -> pd.DataFrame:
    """Aplica todos os filtros relevantes a um DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    if oficinas and "Oficina" in d.columns:
        d = d[d["Oficina"].isin(oficinas)]
    if periodo and "Período" in d.columns:
        d = d[d["Período"] == periodo]
    if filtro_type_05 and "Type 05" in d.columns:
        d = d[d["Type 05"].isin(filtro_type_05)]
    if filtro_type_06 and "Type 06" in d.columns:
        d = d[d["Type 06"].isin(filtro_type_06)]
    if filtro_account and "Account" in d.columns:
        d = d[d["Account"].isin(filtro_account)]
    return d


def _get_mes_anterior(periodo: str) -> str | None:
    if periodo not in ORDEM_MESES:
        return None
    idx = ORDEM_MESES.index(periodo)
    return ORDEM_MESES[idx - 1] if idx > 0 else None


# =========================================================================
#  Severidade
# =========================================================================

def classify_severity(desvio_pct: float) -> str:
    pct = abs(desvio_pct)
    if pct >= _SEV_CRITICO:
        return "critico"
    if pct >= _SEV_MODERADO:
        return "moderado"
    return "informativo"


# =========================================================================
#  Formatação — estilo relatório (kMOEDA + moeda/veíc + ∆)
# =========================================================================

def _fmt_num(valor: float, decimais: int = 1) -> str:
    """Formata número pt-BR (ponto milhar, vírgula decimal)."""
    try:
        s = f"{float(valor):,.{decimais}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(valor)


def fmt_k(valor: float, moeda: str = "EUR", decimais: int = 1) -> str:
    """Valor em k{moeda}. Ex: 26200 → '26,2 kEUR'."""
    v = float(valor) / 1000 if valor else 0.0
    return f"{_fmt_num(v, decimais)} k{moeda}"


def fmt_cpu(valor: float, simbolo: str = "€") -> str:
    """CPU em {simbolo}/veíc. Ex: 6.8 → '6,8 €/veíc'."""
    return f"{_fmt_num(float(valor) if valor else 0.0)} {simbolo}/veíc"


def fmt_delta_k(valor: float, moeda: str = "EUR") -> str:
    """∆ com sinal em kMOEDA. Ex: 75500 → '∆ +75,5 kEUR'."""
    v = float(valor) / 1000 if valor else 0.0
    sinal = "+" if v >= 0 else ""
    return f"∆ {sinal}{_fmt_num(v)} k{moeda}"


def fmt_delta_cpu(valor: float, simbolo: str = "€") -> str:
    """∆ CPU com sinal. Ex: 19.6 → '∆ +19,6 €/veíc'."""
    v = float(valor) if valor else 0.0
    sinal = "+" if v >= 0 else ""
    return f"∆ {sinal}{_fmt_num(v)} {simbolo}/veíc"


def fmt_linha_type06(item: dict, moeda: str = "EUR", simbolo: str = "€") -> str:
    """Linha completa de um Type 06 no formato relatório.

    ``Material Losses: 26,2 kEUR (6,8 €/veíc) | ∆ +75,5 kEUR (∆ +19,6 €/veíc)``
    """
    nome = item.get("type_06", "")
    real = item.get("real", 0)
    cpu_real = item.get("cpu_real", 0)
    desvio = item.get("desvio", 0)
    delta_cpu = item.get("delta_cpu", 0)
    return (
        f"{nome}: {fmt_k(real, moeda)} ({fmt_cpu(cpu_real, simbolo)}) "
        f"| {fmt_delta_k(desvio, moeda)} ({fmt_delta_cpu(delta_cpu, simbolo)})"
    )


def fmt_linha_account(acc: dict, moeda: str = "EUR", simbolo: str = "€") -> str:
    """Sub-linha de Account (indentada, apenas delta).

    ``  Scrap Sales: +71,2 kEUR (∆ +18,5 €/veíc)``
    """
    nome = acc.get("account", "")
    desvio = acc.get("desvio", 0)
    delta_cpu = acc.get("delta_cpu", 0)
    v_k = float(desvio) / 1000 if desvio else 0.0
    sinal = "+" if v_k >= 0 else ""
    return (
        f"  {nome}: {sinal}{_fmt_num(v_k)} k{moeda} "
        f"({fmt_delta_cpu(delta_cpu, simbolo)})"
    )


# =========================================================================
#  Ranking por Oficina (função central)
# =========================================================================

def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def calcular_ranking_por_oficina(
    data: dict,
    oficinas: list[str],
    periodo: str,
    modo: str,
    proporcao: float,
    top_n: int = 10,
    filtro_type_05: list[str] | None = None,
    filtro_type_06: list[str] | None = None,
    filtro_account: list[str] | None = None,
    moeda: str = "BRL",
) -> list[dict]:
    """Calcula ranking para **cada oficina** separadamente.

    Retorna lista de dicts (1 por oficina):
    ``{oficina, periodo, volume_total, moeda, simbolo, itens: [...]}``
    """
    df_real = data.get("real")
    df_flex = data.get("flex_detalhado")
    taxas = data.get("taxas", {})
    simbolo = obter_simbolo_moeda(moeda)

    if df_real is None or df_real.empty:
        return []

    # Volume TOTAL do TC (não por oficina) para CPU
    df_vol = data.get("real_vol")
    vol_total_periodo = 0.0
    if df_vol is not None and not df_vol.empty:
        mask = df_vol["Período"] == periodo if "Período" in df_vol.columns else pd.Series(True, index=df_vol.index)
        vol_total_periodo = float(df_vol.loc[mask, "Volume"].sum())

    # Determinar oficinas a processar
    if not oficinas:
        if "Oficina" in df_real.columns:
            oficinas_iter = sorted(df_real["Oficina"].dropna().unique().tolist())
        else:
            oficinas_iter = ["Todas"]
    else:
        oficinas_iter = list(oficinas)

    resultados: list[dict] = []

    for ofi in oficinas_iter:
        ofi_list = [ofi] if ofi != "Todas" else []

        # --- Real filtrado: oficina + período + filtros dimensionais ---
        r = _filtrar(df_real, ofi_list, periodo, filtro_type_05, filtro_type_06, filtro_account)
        if r.empty or _COL_CUSTO not in r.columns:
            continue

        # --- Esperado: conforme modo ---
        if modo == "flex_bud_x_real":
            if df_flex is None or df_flex.empty:
                continue
            f = _filtrar(df_flex, ofi_list, periodo, filtro_type_05, filtro_type_06, filtro_account)
            if f.empty:
                continue
            # Agregar Flex por Type 06
            esp_t6 = (
                f.groupby("Type 06", as_index=False)["Flex_Bud"]
                .sum()
                .rename(columns={"Flex_Bud": "Esperado_Full"})
            )
            esp_t6["Esperado"] = esp_t6["Esperado_Full"] * proporcao

            # Agregar Flex por (Type 06, Account)
            esp_acc = (
                f.groupby(["Type 06", "Account"], as_index=False)["Flex_Bud"]
                .sum()
                .rename(columns={"Flex_Bud": "Esperado_Full_Acc"})
            )
            esp_acc["Esperado_Acc"] = esp_acc["Esperado_Full_Acc"] * proporcao

        elif modo == "mes_x_mes_anterior":
            mes_ant = _get_mes_anterior(periodo)
            if mes_ant is None:
                continue
            r_ant = _filtrar(df_real, ofi_list, mes_ant, filtro_type_05, filtro_type_06, filtro_account)
            if r_ant.empty or _COL_CUSTO not in r_ant.columns:
                continue
            esp_t6 = (
                r_ant.groupby("Type 06", as_index=False)[_COL_CUSTO]
                .sum()
                .rename(columns={_COL_CUSTO: "Anterior"})
            )
            esp_t6["Esperado"] = esp_t6["Anterior"] * proporcao

            esp_acc = (
                r_ant.groupby(["Type 06", "Account"], as_index=False)[_COL_CUSTO]
                .sum()
                .rename(columns={_COL_CUSTO: "Anterior_Acc"})
            )
            esp_acc["Esperado_Acc"] = esp_acc["Anterior_Acc"] * proporcao
        else:
            continue

        # --- Real agregado por Type 06 ---
        real_t6 = (
            r.groupby("Type 06", as_index=False)[_COL_CUSTO]
            .sum()
            .rename(columns={_COL_CUSTO: "Real"})
        )
        # Mapa Type 06 → Type 05 (moda)
        t05_map = {}
        if "Type 05" in r.columns:
            for t6, grp in r.groupby("Type 06"):
                vals = grp["Type 05"].dropna()
                t05_map[t6] = vals.mode().iloc[0] if not vals.empty else ""

        # Merge Real × Esperado por Type 06
        merged = real_t6.merge(esp_t6[["Type 06", "Esperado"]], on="Type 06", how="outer")
        merged["Real"] = merged["Real"].fillna(0.0)
        merged["Esperado"] = merged["Esperado"].fillna(0.0)
        merged["Desvio"] = merged["Real"] - merged["Esperado"]

        # Apenas desvios positivos (real acima do esperado = perda)
        perdas = merged[merged["Desvio"] > 0].copy()
        perdas = perdas.sort_values("Desvio", ascending=False).head(top_n)

        if perdas.empty:
            continue

        # --- Real agregado por (Type 06, Account) ---
        real_acc = (
            r.groupby(["Type 06", "Account"], as_index=False)[_COL_CUSTO]
            .sum()
            .rename(columns={_COL_CUSTO: "Real_Acc"})
        )

        # --- Montar itens ---
        itens: list[dict] = []
        for _, row in perdas.iterrows():
            t6 = str(row["Type 06"])
            real_val = float(row["Real"])
            esp_val = float(row["Esperado"])
            desvio_val = float(row["Desvio"])

            # Converter moeda
            if moeda != "BRL":
                real_val = converter_moeda(real_val, moeda, taxas)
                esp_val = converter_moeda(esp_val, moeda, taxas)
                desvio_val = converter_moeda(desvio_val, moeda, taxas)

            desvio_pct = _safe_div(desvio_val, abs(esp_val)) * 100 if esp_val else 0.0
            cpu_real = _safe_div(real_val, vol_total_periodo)
            cpu_esp = _safe_div(esp_val, vol_total_periodo)
            delta_cpu = cpu_real - cpu_esp

            # Sub-itens: Accounts deste Type 06
            acc_real_t6 = real_acc[real_acc["Type 06"] == t6]
            acc_esp_t6 = esp_acc[esp_acc["Type 06"] == t6]
            acc_merged = acc_real_t6.merge(
                acc_esp_t6[["Type 06", "Account", "Esperado_Acc"]],
                on=["Type 06", "Account"], how="outer",
            )
            acc_merged["Real_Acc"] = acc_merged["Real_Acc"].fillna(0)
            acc_merged["Esperado_Acc"] = acc_merged["Esperado_Acc"].fillna(0)
            acc_merged["Desvio_Acc"] = acc_merged["Real_Acc"] - acc_merged["Esperado_Acc"]
            acc_merged = acc_merged.sort_values("Desvio_Acc", ascending=False)

            accounts: list[dict] = []
            for _, ar in acc_merged.iterrows():
                acc_desvio = float(ar["Desvio_Acc"])
                if moeda != "BRL":
                    acc_desvio = converter_moeda(acc_desvio, moeda, taxas)
                if abs(acc_desvio) < 0.01:
                    continue  # Ignorar accounts com desvio zero
                acc_delta_cpu = _safe_div(acc_desvio, vol_total_periodo)
                accounts.append({
                    "account": str(ar["Account"]),
                    "desvio": round(acc_desvio, 2),
                    "delta_cpu": round(acc_delta_cpu, 4),
                })

            itens.append({
                "type_06": t6,
                "type_05": t05_map.get(t6, ""),
                "real": round(real_val, 2),
                "esperado": round(esp_val, 2),
                "desvio": round(desvio_val, 2),
                "desvio_pct": round(desvio_pct, 2),
                "cpu_real": round(cpu_real, 4),
                "cpu_esperado": round(cpu_esp, 4),
                "delta_cpu": round(delta_cpu, 4),
                "severidade": classify_severity(desvio_pct),
                "accounts": accounts,
            })

        sev_list = [it["severidade"] for it in itens]
        sev_geral = (
            "critico" if "critico" in sev_list
            else "moderado" if "moderado" in sev_list
            else "informativo"
        )

        resultados.append({
            "oficina": ofi,
            "periodo": periodo,
            "volume_total": vol_total_periodo,
            "moeda": moeda,
            "simbolo": simbolo,
            "severidade": sev_geral,
            "itens": itens,
        })

    return resultados


# =========================================================================
#  Ranking Consolidado (card único com drill-down)
# =========================================================================

def calcular_ranking_consolidado(
    data: dict,
    periodo: str,
    modo: str,
    proporcao: float,
    top_n: int = 10,
    filtro_type_05: list[str] | None = None,
    filtro_type_06: list[str] | None = None,
    filtro_account: list[str] | None = None,
    moeda: str = "BRL",
) -> dict | None:
    """Ranking **global** com drill-down hierárquico.

    Hierarquia: Type 05 → Type 06 → Account → Oficinas (todas perdendo)
                → Texto breve (top 3 por oficina).

    Retorna dict único (não lista) ou ``None`` se sem dados.
    """
    df_real = data.get("real")
    df_flex = data.get("flex_detalhado")
    taxas = data.get("taxas", {})
    simbolo = obter_simbolo_moeda(moeda)

    if df_real is None or df_real.empty:
        return None

    # Volume TOTAL para CPU
    df_vol = data.get("real_vol")
    vol_total_periodo = 0.0
    if df_vol is not None and not df_vol.empty:
        mask = (
            df_vol["Período"] == periodo
            if "Período" in df_vol.columns
            else pd.Series(True, index=df_vol.index)
        )
        vol_total_periodo = float(df_vol.loc[mask, "Volume"].sum())

    # --- Real e Esperado: visão GLOBAL (sem filtro de oficina) ---
    r = _filtrar(df_real, None, periodo, filtro_type_05, filtro_type_06, filtro_account)
    if r.empty or _COL_CUSTO not in r.columns:
        return None

    if modo == "flex_bud_x_real":
        if df_flex is None or df_flex.empty:
            return None
        f = _filtrar(df_flex, None, periodo, filtro_type_05, filtro_type_06, filtro_account)
        if f.empty:
            return None
        esp_t6 = (
            f.groupby("Type 06", as_index=False)["Flex_Bud"]
            .sum()
            .rename(columns={"Flex_Bud": "Esperado_Full"})
        )
        esp_t6["Esperado"] = esp_t6["Esperado_Full"] * proporcao

        esp_acc = (
            f.groupby(["Type 06", "Account"], as_index=False)["Flex_Bud"]
            .sum()
            .rename(columns={"Flex_Bud": "Esperado_Full_Acc"})
        )
        esp_acc["Esperado_Acc"] = esp_acc["Esperado_Full_Acc"] * proporcao

    elif modo == "mes_x_mes_anterior":
        mes_ant = _get_mes_anterior(periodo)
        if mes_ant is None:
            return None
        r_ant = _filtrar(df_real, None, mes_ant, filtro_type_05, filtro_type_06, filtro_account)
        if r_ant.empty or _COL_CUSTO not in r_ant.columns:
            return None
        esp_t6 = (
            r_ant.groupby("Type 06", as_index=False)[_COL_CUSTO]
            .sum()
            .rename(columns={_COL_CUSTO: "Anterior"})
        )
        esp_t6["Esperado"] = esp_t6["Anterior"] * proporcao

        esp_acc = (
            r_ant.groupby(["Type 06", "Account"], as_index=False)[_COL_CUSTO]
            .sum()
            .rename(columns={_COL_CUSTO: "Anterior_Acc"})
        )
        esp_acc["Esperado_Acc"] = esp_acc["Anterior_Acc"] * proporcao
    else:
        return None

    # --- Agregar Real global por Type 06 ---
    real_t6 = (
        r.groupby("Type 06", as_index=False)[_COL_CUSTO]
        .sum()
        .rename(columns={_COL_CUSTO: "Real"})
    )
    t05_map: dict[str, str] = {}
    if "Type 05" in r.columns:
        for t6, grp in r.groupby("Type 06"):
            vals = grp["Type 05"].dropna()
            t05_map[t6] = vals.mode().iloc[0] if not vals.empty else ""

    merged = real_t6.merge(esp_t6[["Type 06", "Esperado"]], on="Type 06", how="outer")
    merged["Real"] = merged["Real"].fillna(0.0)
    merged["Esperado"] = merged["Esperado"].fillna(0.0)
    merged["Desvio"] = merged["Real"] - merged["Esperado"]

    perdas = merged[merged["Desvio"] > 0].copy()
    perdas = perdas.sort_values("Desvio", ascending=False).head(top_n)

    if perdas.empty:
        return None

    # --- Real global por (Type 06, Account) ---
    real_acc = (
        r.groupby(["Type 06", "Account"], as_index=False)[_COL_CUSTO]
        .sum()
        .rename(columns={_COL_CUSTO: "Real_Acc"})
    )

    # --- Preparar dados POR OFICINA para drill-down ---
    _has_oficina = "Oficina" in r.columns
    _has_texto = "Texto breve" in r.columns

    if _has_oficina:
        real_ofi_acc = (
            r.groupby(["Type 06", "Account", "Oficina"], as_index=False)[_COL_CUSTO]
            .sum()
            .rename(columns={_COL_CUSTO: "Real_Ofi"})
        )
        # Esperado por oficina (Flex detalhado já tem Oficina)
        if modo == "flex_bud_x_real" and df_flex is not None:
            f_full = _filtrar(df_flex, None, periodo, filtro_type_05, filtro_type_06, filtro_account)
            if "Oficina" in f_full.columns:
                esp_ofi_acc = (
                    f_full.groupby(["Type 06", "Account", "Oficina"], as_index=False)["Flex_Bud"]
                    .sum()
                    .rename(columns={"Flex_Bud": "Esp_Ofi_Full"})
                )
                esp_ofi_acc["Esp_Ofi"] = esp_ofi_acc["Esp_Ofi_Full"] * proporcao
            else:
                esp_ofi_acc = pd.DataFrame(
                    columns=["Type 06", "Account", "Oficina", "Esp_Ofi"],
                )
        elif modo == "mes_x_mes_anterior":
            r_ant_full = _filtrar(df_real, None, mes_ant, filtro_type_05, filtro_type_06, filtro_account)
            if "Oficina" in r_ant_full.columns:
                esp_ofi_acc = (
                    r_ant_full.groupby(["Type 06", "Account", "Oficina"], as_index=False)[_COL_CUSTO]
                    .sum()
                    .rename(columns={_COL_CUSTO: "Ant_Ofi"})
                )
                esp_ofi_acc["Esp_Ofi"] = esp_ofi_acc["Ant_Ofi"] * proporcao
            else:
                esp_ofi_acc = pd.DataFrame(
                    columns=["Type 06", "Account", "Oficina", "Esp_Ofi"],
                )
        else:
            esp_ofi_acc = pd.DataFrame(
                columns=["Type 06", "Account", "Oficina", "Esp_Ofi"],
            )

    # --- Montar itens ---
    itens: list[dict] = []
    for _, row in perdas.iterrows():
        t6 = str(row["Type 06"])
        real_val = float(row["Real"])
        esp_val = float(row["Esperado"])
        desvio_val = float(row["Desvio"])

        if moeda != "BRL":
            real_val = converter_moeda(real_val, moeda, taxas)
            esp_val = converter_moeda(esp_val, moeda, taxas)
            desvio_val = converter_moeda(desvio_val, moeda, taxas)

        desvio_pct = _safe_div(desvio_val, abs(esp_val)) * 100 if esp_val else 0.0
        cpu_real = _safe_div(real_val, vol_total_periodo)
        cpu_esp = _safe_div(esp_val, vol_total_periodo)
        delta_cpu = cpu_real - cpu_esp

        # --- Sub-itens: Accounts deste Type 06 ---
        acc_real_t6 = real_acc[real_acc["Type 06"] == t6]
        acc_esp_t6 = esp_acc[esp_acc["Type 06"] == t6]
        acc_merged = acc_real_t6.merge(
            acc_esp_t6[["Type 06", "Account", "Esperado_Acc"]],
            on=["Type 06", "Account"], how="outer",
        )
        acc_merged["Real_Acc"] = acc_merged["Real_Acc"].fillna(0)
        acc_merged["Esperado_Acc"] = acc_merged["Esperado_Acc"].fillna(0)
        acc_merged["Desvio_Acc"] = acc_merged["Real_Acc"] - acc_merged["Esperado_Acc"]
        acc_merged = acc_merged.sort_values("Desvio_Acc", ascending=False)

        accounts: list[dict] = []
        for _, ar in acc_merged.iterrows():
            acc_desvio = float(ar["Desvio_Acc"])
            acc_esperado = float(ar["Esperado_Acc"])
            if moeda != "BRL":
                acc_desvio = converter_moeda(acc_desvio, moeda, taxas)
                acc_esperado = converter_moeda(acc_esperado, moeda, taxas)
            if abs(acc_desvio) < 0.01:
                continue
            acc_delta_cpu = _safe_div(acc_desvio, vol_total_periodo)
            acc_name = str(ar["Account"])

            # --- Drill-down: oficinas com desvio > 0 ---
            oficinas_list: list[dict] = []
            if _has_oficina:
                _ro = real_ofi_acc[
                    (real_ofi_acc["Type 06"] == t6) & (real_ofi_acc["Account"] == acc_name)
                ]
                _eo = esp_ofi_acc[
                    (esp_ofi_acc["Type 06"] == t6) & (esp_ofi_acc["Account"] == acc_name)
                ] if not esp_ofi_acc.empty else pd.DataFrame()

                ofi_merged = _ro.merge(
                    _eo[["Type 06", "Account", "Oficina", "Esp_Ofi"]],
                    on=["Type 06", "Account", "Oficina"], how="outer",
                ) if not _eo.empty else _ro.copy()

                if "Esp_Ofi" not in ofi_merged.columns:
                    ofi_merged["Esp_Ofi"] = 0.0
                ofi_merged["Real_Ofi"] = ofi_merged.get("Real_Ofi", pd.Series(0.0)).fillna(0.0)
                ofi_merged["Esp_Ofi"] = ofi_merged["Esp_Ofi"].fillna(0.0)
                ofi_merged["Dev_Ofi"] = ofi_merged["Real_Ofi"] - ofi_merged["Esp_Ofi"]

                # Filtrar TODAS as oficinas com desvio > 0 (perdendo)
                ofi_perdas = ofi_merged[ofi_merged["Dev_Ofi"] > 0].sort_values(
                    "Dev_Ofi", ascending=False,
                )

                for _, orow in ofi_perdas.iterrows():
                    ofi_name = str(orow["Oficina"])
                    ofi_dev = float(orow["Dev_Ofi"])
                    if moeda != "BRL":
                        ofi_dev = converter_moeda(ofi_dev, moeda, taxas)
                    ofi_dcpu = _safe_div(ofi_dev, vol_total_periodo)

                    # --- Texto breve: top 3 por oficina ---
                    textos: list[dict] = []
                    if _has_texto:
                        mask_txt = (
                            (r["Type 06"] == t6)
                            & (r["Account"] == acc_name)
                            & (r["Oficina"] == ofi_name)
                        )
                        df_txt = r.loc[mask_txt].copy()
                        if not df_txt.empty and "Texto breve" in df_txt.columns:
                            txt_agg = (
                                df_txt.groupby("Texto breve", as_index=False)[_COL_CUSTO]
                                .sum()
                                .rename(columns={_COL_CUSTO: "Valor"})
                                .sort_values("Valor", ascending=False)
                                .head(3)
                            )
                            for _, trow in txt_agg.iterrows():
                                tv = float(trow["Valor"])
                                if moeda != "BRL":
                                    tv = converter_moeda(tv, moeda, taxas)
                                texto_str = str(trow["Texto breve"]).strip()
                                if texto_str and abs(tv) > 0.01:
                                    textos.append({
                                        "texto": texto_str,
                                        "valor": round(tv, 2),
                                    })

                    oficinas_list.append({
                        "oficina": ofi_name,
                        "desvio": round(ofi_dev, 2),
                        "delta_cpu": round(ofi_dcpu, 4),
                        "textos": textos,
                    })

            accounts.append({
                "account": acc_name,
                "desvio": round(acc_desvio, 2),
                "delta_cpu": round(acc_delta_cpu, 4),
                "esperado": round(acc_esperado, 2),
                "oficinas": oficinas_list,
            })

        itens.append({
            "type_06": t6,
            "type_05": t05_map.get(t6, ""),
            "real": round(real_val, 2),
            "esperado": round(esp_val, 2),
            "desvio": round(desvio_val, 2),
            "desvio_pct": round(desvio_pct, 2),
            "cpu_real": round(cpu_real, 4),
            "cpu_esperado": round(cpu_esp, 4),
            "delta_cpu": round(delta_cpu, 4),
            "severidade": classify_severity(desvio_pct),
            "accounts": accounts,
        })

    sev_list = [it["severidade"] for it in itens]
    sev_geral = (
        "critico" if "critico" in sev_list
        else "moderado" if "moderado" in sev_list
        else "informativo"
    )
    total_desvio = sum(it["desvio"] for it in itens)

    return {
        "periodo": periodo,
        "volume_total": vol_total_periodo,
        "moeda": moeda,
        "simbolo": simbolo,
        "severidade": sev_geral,
        "total_desvio": round(total_desvio, 2),
        "itens": itens,
    }


# =========================================================================
#  Tabela de validação (debug)
# =========================================================================

def gerar_tabela_validacao(
    data: dict,
    oficina: str | None,
    periodo: str,
    proporcao: float,
    moeda: str = "BRL",
    filtro_type_05: list[str] | None = None,
    filtro_type_06: list[str] | None = None,
    filtro_account: list[str] | None = None,
) -> pd.DataFrame:
    """Gera DataFrame com todas as linhas para conferência visual.

    Colunas: Type 05, Type 06, Account, Flex BUD, Flex BUD P, Real,
             Real - Flex BUD P, % Delta
    """
    df_real = data.get("real")
    df_flex = data.get("flex_detalhado")
    taxas = data.get("taxas", {})

    if df_real is None or df_real.empty or df_flex is None or df_flex.empty:
        return pd.DataFrame()

    ofi_list = [oficina] if oficina else []

    # --- Filtrar Real ---
    r = _filtrar(df_real, ofi_list, periodo, filtro_type_05, filtro_type_06, filtro_account)
    if r.empty or _COL_CUSTO not in r.columns:
        return pd.DataFrame()

    # --- Filtrar Flex ---
    f = _filtrar(df_flex, ofi_list, periodo, filtro_type_05, filtro_type_06, filtro_account)
    if f.empty:
        return pd.DataFrame()

    # Agregar Real por (Type 05, Type 06, Account)
    if "Type 05" not in r.columns:
        return pd.DataFrame()

    real_agg = (
        r.groupby(["Type 05", "Type 06", "Account"], as_index=False)[_COL_CUSTO]
        .sum()
        .rename(columns={_COL_CUSTO: "Real"})
    )

    # Agregar Flex por (Type 05, Type 06, Account) — Flex_Bud já existe no detalhado
    # Precisamos do Type 05: vem do df_real (mapa)
    t05_map = {}
    if "Type 05" in r.columns:
        for (t6, acc), grp in r.groupby(["Type 06", "Account"]):
            vals = grp["Type 05"].dropna()
            t05_map[(t6, acc)] = vals.mode().iloc[0] if not vals.empty else ""

    flex_agg = (
        f.groupby(["Type 06", "Account"], as_index=False)["Flex_Bud"]
        .sum()
        .rename(columns={"Flex_Bud": "Flex BUD"})
    )
    flex_agg["Type 05"] = flex_agg.apply(
        lambda row: t05_map.get((row["Type 06"], row["Account"]), ""), axis=1,
    )

    # Merge
    merged = real_agg.merge(
        flex_agg[["Type 05", "Type 06", "Account", "Flex BUD"]],
        on=["Type 05", "Type 06", "Account"],
        how="outer",
    )
    merged["Real"] = merged["Real"].fillna(0.0)
    merged["Flex BUD"] = merged["Flex BUD"].fillna(0.0)

    # Flex BUD P = Flex BUD × proporção
    merged["Flex BUD P"] = merged["Flex BUD"] * proporcao

    # Conversão de moeda
    if moeda != "BRL":
        for col in ["Flex BUD", "Flex BUD P", "Real"]:
            merged[col] = merged[col].apply(
                lambda v: converter_moeda(v, moeda, taxas),
            )

    # Colunas calculadas
    merged["Real - Flex BUD P"] = merged["Real"] - merged["Flex BUD P"]
    merged["% Delta"] = merged.apply(
        lambda row: _safe_div(row["Real - Flex BUD P"], abs(row["Flex BUD P"])) * 100,
        axis=1,
    )

    # Ordenar e formatar
    result = merged[
        ["Type 05", "Type 06", "Account",
         "Flex BUD", "Flex BUD P", "Real",
         "Real - Flex BUD P", "% Delta"]
    ].sort_values("Real - Flex BUD P", ascending=False).reset_index(drop=True)

    # Arredondamento
    for col in ["Flex BUD", "Flex BUD P", "Real", "Real - Flex BUD P"]:
        result[col] = result[col].round(2)
    result["% Delta"] = result["% Delta"].round(2)

    return result


# =========================================================================
#  Avaliação de regras
# =========================================================================

def evaluate_rule(
    rule: dict,
    data: dict,
    proporcao: float,
    periodo: str,
) -> list[dict]:
    """Avalia uma regra e retorna **lista de alertas** (1 por oficina)."""
    oficinas = rule.get("oficinas", [])
    modo = rule.get("modo_comparacao", "flex_bud_x_real")
    top_n = rule.get("top_n", 10)
    moeda = rule.get("moeda", "BRL")

    ranking_list = calcular_ranking_por_oficina(
        data, oficinas, periodo, modo, proporcao, top_n,
        filtro_type_05=rule.get("filtro_type_05"),
        filtro_type_06=rule.get("filtro_type_06"),
        filtro_account=rule.get("filtro_account"),
        moeda=moeda,
    )
    if not ranking_list:
        return []

    modo_label = MODOS_COMPARACAO.get(modo, modo)
    alertas: list[dict] = []
    for rank_oficina in ranking_list:
        ofi = rank_oficina["oficina"]
        itens = rank_oficina["itens"]
        total_desvio = sum(it["desvio"] for it in itens)
        sev = rank_oficina["severidade"]

        # Montar mensagem no formato relatório
        linhas: list[str] = []
        for it in itens:
            linhas.append(fmt_linha_type06(it, moeda, rank_oficina["simbolo"]))
            for acc in it.get("accounts", []):
                linhas.append(fmt_linha_account(acc, moeda, rank_oficina["simbolo"]))
        mensagem_relatorio = "\n".join(linhas)

        alertas.append({
            "id": str(uuid.uuid4()),
            "rule_id": rule.get("id"),
            "timestamp": timestamp_agora_iso(),
            "titulo": f"{ofi} — {periodo}",
            "mensagem": mensagem_relatorio,
            "severidade": sev,
            "tipo": "automatico",
            "lido": False,
            "metadata": {
                "oficina": ofi,
                "periodo": periodo,
                "ano": rule.get("ano"),
                "modo": modo,
                "modo_label": modo_label,
                "moeda": moeda,
                "proporcao_mes": round(proporcao, 4),
                "total_desvio": round(total_desvio, 2),
                "volume_total": rank_oficina["volume_total"],
            },
            "ranking": rank_oficina,
            "notificacoes_enviadas": {
                "email": False,
                "teams": False,
            },
        })

    return alertas


def evaluate_all_rules(
    data: dict,
    rules: list[dict],
    periodo: str,
    data_ref: date | None = None,
) -> list[dict]:
    if data_ref is None:
        data_ref = date.today()
    prop = proporcao_mes(data_ref)

    alertas: list[dict] = []
    for rule in rules:
        if not rule.get("ativo", True):
            continue
        alertas.extend(evaluate_rule(rule, data, prop, periodo))
    return alertas


# =========================================================================
#  Orquestrador
# =========================================================================

def run_daily_check(
    periodo: str | None = None,
    data_ref: date | None = None,
) -> list[dict]:
    """Carrega dados → calcula ranking consolidado → salva log → notifica.

    Usa ``calcular_ranking_consolidado`` + ``gerar_tabela_validacao`` e envia
    via email/Teams no formato consolidado (com tabela de validação).
    """
    if data_ref is None:
        data_ref = date.today()
    if periodo is None:
        periodo = mes_atual_nome(data_ref)

    rules_data = load_alert_rules()
    rules = rules_data.get("rules", [])
    config = rules_data.get("config", _default_config())

    if not rules:
        logger.info("Nenhuma regra configurada.")
        return []

    prop = proporcao_mes(data_ref)
    all_alerts: list[dict] = []

    anos = {r.get("ano", data_ref.year) for r in rules if r.get("ativo", True)}

    for ano in anos:
        data = load_all_data(ano)
        regras_ano = [
            r for r in rules
            if r.get("ano", data_ref.year) == ano and r.get("ativo", True)
        ]
        if not regras_ano:
            continue

        regra = regras_ano[0]
        modo = regra.get("modo_comparacao", "flex_bud_x_real")
        top_n = regra.get("top_n", 10)
        moeda = regra.get("moeda", "BRL")

        ranking = calcular_ranking_consolidado(
            data=data,
            periodo=periodo,
            modo=modo,
            proporcao=prop,
            top_n=top_n,
            filtro_type_05=regra.get("filtro_type_05") or None,
            filtro_type_06=regra.get("filtro_type_06") or None,
            filtro_account=regra.get("filtro_account") or None,
            moeda=moeda,
        )

        if not ranking:
            logger.info("Sem desvios para ano=%s periodo=%s", ano, periodo)
            continue

        # Gerar tabela de validação
        tabela_df = gerar_tabela_validacao(
            data=data,
            oficina=None,
            periodo=periodo,
            proporcao=prop,
            moeda=moeda,
            filtro_type_05=regra.get("filtro_type_05") or None,
            filtro_type_06=regra.get("filtro_type_06") or None,
            filtro_account=regra.get("filtro_account") or None,
        )

        # Registrar no log
        alerta_log = {
            "id": f"daily_{ano}_{periodo}_{timestamp_agora_iso()}",
            "rule_id": regra.get("id", ""),
            "timestamp": timestamp_agora_iso(),
            "periodo": periodo,
            "ano": ano,
            "severidade": ranking.get("severidade", "informativo"),
            "total_desvio": ranking.get("total_desvio", 0),
            "notificacoes_enviadas": {"email": False, "teams": False},
        }
        all_alerts.append(alerta_log)

        # Notificações
        notif = config.get("notifications_enabled", {})

        if notif.get("email"):
            from alertas.notifications_email import send_alert_email_consolidated
            try:
                send_alert_email_consolidated(
                    ranking, config.get("email", {}), modo, prop, tabela_df,
                )
                alerta_log["notificacoes_enviadas"]["email"] = True
            except Exception:
                logger.exception("Falha e-mail consolidado ano=%s", ano)

        if notif.get("teams"):
            webhook = config.get("teams_webhook_url", "")
            if webhook:
                from alertas.notifications_teams import send_alert_teams_consolidated
                try:
                    send_alert_teams_consolidated(
                        ranking, webhook, modo, tabela_df,
                    )
                    alerta_log["notificacoes_enviadas"]["teams"] = True
                except Exception:
                    logger.exception("Falha Teams consolidado ano=%s", ano)

    if all_alerts:
        log = load_alert_log()
        log.extend(all_alerts)
        save_alert_log(log)

    rules_data["config"]["ultima_execucao"] = timestamp_agora_iso()
    save_alert_rules(rules_data)

    return all_alerts
