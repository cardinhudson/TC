"""
TC Copilot — Coleta e agregação de dados dos parquets.

Reutiliza os data loaders do tc_principal/shared.py e lê parquets do tc_ext.
Agrega dados por mês, calcula variações e formata para envio à LLM.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import numpy as np
import pandas as pd

from tc_core.constants import ORDEM_MESES, MESES_NUMERO
from tc_principal.shared import calcular_flex_budget, calcular_flex_budget_detalhado

logger = logging.getLogger(__name__)

# Raiz do projeto (compatível com EXE)
if hasattr(sys, "_MEIPASS"):
    _ROOT = sys._MEIPASS
else:
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _fmt(valor: float, decimais: int = 2) -> str:
    """Formata número no padrão pt-BR: 1.234.567,89"""
    if pd.isna(valor) or valor is None:
        return "0"
    try:
        s = f"{float(valor):,.{decimais}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(valor)


def _pct(atual: float, anterior: float) -> str:
    """Calcula variação percentual formatada."""
    if anterior == 0:
        return "sem ref." if atual == 0 else "sem base (ref.=0)"
    var = (atual - anterior) / abs(anterior) * 100
    sinal = "+" if var > 0 else ""
    return f"{sinal}{var:.1f}%"


def _var_abs(atual: float, anterior: float) -> str:
    """Calcula variação absoluta formatada com Δ."""
    diff = atual - anterior
    sinal = "+" if diff >= 0 else ""
    return f"Δ {sinal}{_fmt(diff)}"


def _fmt_k(valor: float, decimais: int = 1) -> str:
    """Formata valor em kBRL (÷1000). Ex: 448700.47 → '448,7 kBRL'."""
    if pd.isna(valor) or valor is None:
        return "0 kBRL"
    try:
        v = float(valor) / 1000
        s = f"{v:,.{decimais}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".") + " kBRL"
    except (ValueError, TypeError):
        return str(valor)


def _var_k(atual: float, anterior: float) -> str:
    """Variação absoluta em kBRL com Δ."""
    diff = atual - anterior
    sinal = "+" if diff >= 0 else ""
    return f"Δ {sinal}{_fmt_k(diff)}"


def _cpu(custo: float, volume: float) -> float:
    """Calcula CPU = custo total / volume. Retorna 0 se volume <= 0."""
    if not volume or volume <= 0:
        return 0.0
    return custo / volume


def _fmt_cpu(valor: float) -> str:
    """Formata valor como R$/veíc (sem divisão por 1000)."""
    if pd.isna(valor) or valor is None:
        return "0 R$/veíc"
    try:
        v = float(valor)
        s = f"{v:,.1f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".") + " R$/veíc"
    except (ValueError, TypeError):
        return str(valor)


def _safe_sum(df: pd.DataFrame | None, col: str, filtro_periodo: str | None = None) -> float:
    """Soma segura de uma coluna, opcionalmente filtrada por período."""
    if df is None or df.empty or col not in df.columns:
        return 0.0
    if filtro_periodo and "Período" in df.columns:
        mask = df["Período"] == filtro_periodo
        return float(df.loc[mask, col].sum())
    return float(df[col].sum())


def _filtrar_mes(df: pd.DataFrame | None, mes: str) -> pd.DataFrame | None:
    """Filtra DataFrame por período/mês."""
    if df is None or df.empty:
        return None
    if "Período" in df.columns:
        return df[df["Período"] == mes].copy()
    return None


def _nome_mes(numero: int) -> str:
    """Número do mês → nome em português."""
    return MESES_NUMERO.get(numero, str(numero))


def _mes_anterior(mes_numero: int) -> tuple[int, str]:
    """Retorna (numero, nome) do mês anterior."""
    ant = mes_numero - 1 if mes_numero > 1 else 12
    return ant, _nome_mes(ant)


# ═══════════════════════════════════════════════════════════════
#  CARREGAMENTO DE DADOS (sem Streamlit cache — leitura direta)
# ═══════════════════════════════════════════════════════════════

def _ler_parquet(caminho: str) -> pd.DataFrame | None:
    """Lê parquet com tratamento de erro."""
    if os.path.exists(caminho):
        try:
            return pd.read_parquet(caminho)
        except Exception as e:
            logger.warning("Erro ao ler %s: %s", caminho, e)
    return None


def _pasta_real(ano: int) -> str:
    return os.path.join(_ROOT, "dados", "TC_Principal", str(ano))


def _pasta_bud(ano: int) -> str:
    return os.path.join(_ROOT, "dados", "TC_Principal", str(ano), "BUD")


def _pasta_historico() -> str:
    return os.path.join(_ROOT, "dados", "TC_Principal", "historico_consolidado")


def _pasta_tc_ext(ano: int) -> str:
    return os.path.join(_ROOT, "dados", "TC_Ext", str(ano))


def _pasta_tc_ext_bud(ano: int) -> str:
    return os.path.join(_ROOT, "dados", "TC_Ext", str(ano), "BUD")


# ── TC Veículos — Real ──

def carregar_principal_real(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_real(ano), "df_principal.parquet"))


def carregar_volume_real(ano: int) -> pd.DataFrame | None:
    df = _ler_parquet(os.path.join(_pasta_real(ano), "df_vol_veiculos.parquet"))
    if df is not None and "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    return df


def carregar_cpu_real(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_real(ano), "df_veiculos_cpu.parquet"))


def carregar_custo_fp_real(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_real(ano), "df_veiculos_custo_fp.parquet"))


# ── TC Veículos — Budget ──

def carregar_principal_bud(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_bud(ano), "df_principal_BUD.parquet"))


def carregar_volume_bud(ano: int) -> pd.DataFrame | None:
    df = _ler_parquet(os.path.join(_pasta_bud(ano), "df_vol_veiculos_BUD.parquet"))
    if df is not None and "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    return df


def carregar_volume_actual(ano: int) -> pd.DataFrame | None:
    df = _ler_parquet(os.path.join(_pasta_bud(ano), "df_vol_veiculos_actual.parquet"))
    if df is not None and "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    return df


def carregar_cpu_bud(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_bud(ano), "df_veiculos_cpu_BUD.parquet"))


def carregar_custo_fp_bud(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_bud(ano), "df_veiculos_custo_fp_BUD.parquet"))


# ── Histórico (multi-ano) ──

def carregar_historico_principal() -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_historico(), "df_principal_historico.parquet"))


def carregar_historico_volume() -> pd.DataFrame | None:
    df = _ler_parquet(os.path.join(_pasta_historico(), "df_vol_historico.parquet"))
    if df is not None and "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
    return df


# ── TC Ext ──

def carregar_tc_ext_real(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_tc_ext(ano), "df_final.parquet"))


def carregar_tc_ext_bud(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_tc_ext_bud(ano), "df_final_BUD.parquet"))


def carregar_tc_ext_vol(ano: int) -> pd.DataFrame | None:
    return _ler_parquet(os.path.join(_pasta_tc_ext(ano), "df_vol.parquet"))


# ═══════════════════════════════════════════════════════════════
#  COLETAR DADOS DE UM MÊS
# ═══════════════════════════════════════════════════════════════

def coletar_dados_mes(ano: int, mes_numero: int) -> dict[str, Any]:
    """
    Coleta todos os dados necessários para gerar o relatório de um mês.

    Returns:
        Dicionário com chaves: volume_real, volume_bud, volume_actual,
        custo_real, custo_bud, cpu_real, cpu_bud, custo_fp_real, custo_fp_bud,
        tc_ext_real, tc_ext_bud, historico_vol, historico_custo,
        mes_nome, mes_numero, ano
    """
    mes_nome = _nome_mes(mes_numero)
    mes_ant_num, mes_ant_nome = _mes_anterior(mes_numero)

    # ── Carregar dados completos ──
    vol_real_full = carregar_volume_real(ano)
    vol_bud_full = carregar_volume_bud(ano)
    vol_actual_full = carregar_volume_actual(ano)
    custo_real_full = carregar_principal_real(ano)
    custo_bud_full = carregar_principal_bud(ano)
    cpu_real_full = carregar_cpu_real(ano)
    cpu_bud_full = carregar_cpu_bud(ano)
    custo_fp_real_full = carregar_custo_fp_real(ano)
    custo_fp_bud_full = carregar_custo_fp_bud(ano)

    # Mês anterior: se Janeiro, carregar Dezembro do ano anterior
    if mes_numero == 1:
        vol_real_ant_full = carregar_volume_real(ano - 1)
        custo_real_ant_full = carregar_principal_real(ano - 1)
        cpu_real_ant_full = carregar_cpu_real(ano - 1)
    else:
        vol_real_ant_full = vol_real_full
        custo_real_ant_full = custo_real_full
        cpu_real_ant_full = cpu_real_full

    # Histórico (ano anterior)
    hist_vol = carregar_historico_volume()
    hist_custo = carregar_historico_principal()

    return {
        # Dados completos (para calcular flex, etc.)
        "_vol_real_full": vol_real_full,
        "_vol_bud_full": vol_bud_full,
        "_vol_actual_full": vol_actual_full,
        "_custo_real_full": custo_real_full,
        "_custo_bud_full": custo_bud_full,
        # Dados filtrados do mês
        "volume_real": _filtrar_mes(vol_real_full, mes_nome),
        "volume_bud": _filtrar_mes(vol_bud_full, mes_nome),
        "volume_actual": _filtrar_mes(vol_actual_full, mes_nome),
        "custo_real": _filtrar_mes(custo_real_full, mes_nome),
        "custo_bud": _filtrar_mes(custo_bud_full, mes_nome),
        "cpu_real": _filtrar_mes(cpu_real_full, mes_nome),
        "cpu_bud": _filtrar_mes(cpu_bud_full, mes_nome),
        "custo_fp_real": _filtrar_mes(custo_fp_real_full, mes_nome),
        "custo_fp_bud": _filtrar_mes(custo_fp_bud_full, mes_nome),
        # Mês anterior (usa ano-1 se Janeiro)
        "volume_real_ant": _filtrar_mes(vol_real_ant_full, mes_ant_nome),
        "custo_real_ant": _filtrar_mes(custo_real_ant_full, mes_ant_nome),
        "cpu_real_ant": _filtrar_mes(cpu_real_ant_full, mes_ant_nome),
        # Histórico (ano anterior, mesmo mês)
        "historico_vol": hist_vol,
        "historico_custo": hist_custo,
        # Metadata
        "mes_nome": mes_nome,
        "mes_nome_anterior": mes_ant_nome,
        "mes_numero": mes_numero,
        "ano": ano,
        "ano_anterior": ano - 1,
    }


# ═══════════════════════════════════════════════════════════════
#  CALCULAR VARIAÇÕES
# ═══════════════════════════════════════════════════════════════

def _volume_total(df: pd.DataFrame | None) -> float:
    if df is None or df.empty or "Volume" not in df.columns:
        return 0.0
    return float(df["Volume"].sum())


def _volume_por_modelo(df: pd.DataFrame | None) -> dict[str, float]:
    if df is None or df.empty:
        return {}
    if "Veículo" in df.columns and "Volume" in df.columns:
        return df.groupby("Veículo")["Volume"].sum().to_dict()
    return {}


def _despesa_total(df: pd.DataFrame | None) -> float:
    if df is None or df.empty:
        return 0.0
    if "Despesa Primaria" in df.columns:
        return float(df["Despesa Primaria"].sum())
    if "Total" in df.columns:
        return float(df["Total"].sum())
    return 0.0


def _custo_fp_total(df: pd.DataFrame | None) -> float:
    if df is None or df.empty:
        return 0.0
    for col in ["Custo FP", "Custo FP Veiculo"]:
        if col in df.columns:
            return float(df[col].sum())
    return 0.0


def _cpu_por_modelo(df: pd.DataFrame | None) -> dict[str, float]:
    if df is None or df.empty:
        return {}
    if "Veículo" in df.columns and "CPU" in df.columns:
        return df.groupby("Veículo")["CPU"].sum().to_dict()
    return {}


def _volume_ano_anterior(hist_vol: pd.DataFrame | None, ano_ant: int, mes: str) -> float:
    """Extrai volume do ano anterior para o mesmo mês."""
    if hist_vol is None or hist_vol.empty:
        return 0.0
    mask = True
    if "Ano" in hist_vol.columns:
        mask = mask & (hist_vol["Ano"] == ano_ant)
    if "Período" in hist_vol.columns:
        mask = mask & (hist_vol["Período"] == mes)
    if "Volume" in hist_vol.columns:
        return float(hist_vol.loc[mask, "Volume"].sum())
    return 0.0


def _volume_por_modelo_ano_anterior(
    hist_vol: pd.DataFrame | None, ano_ant: int, mes: str,
) -> dict[str, float]:
    """Extrai volume por modelo do ano anterior para o mesmo mês."""
    if hist_vol is None or hist_vol.empty:
        return {}
    mask = pd.Series(True, index=hist_vol.index)
    if "Ano" in hist_vol.columns:
        mask = mask & (hist_vol["Ano"] == ano_ant)
    if "Período" in hist_vol.columns:
        mask = mask & (hist_vol["Período"] == mes)
    df = hist_vol.loc[mask]
    if df.empty or "Veículo" not in df.columns or "Volume" not in df.columns:
        return {}
    return df.groupby("Veículo")["Volume"].sum().to_dict()


def _despesa_ano_anterior(hist_custo: pd.DataFrame | None, ano_ant: int, mes: str) -> float:
    """Extrai despesa do ano anterior para o mesmo mês."""
    if hist_custo is None or hist_custo.empty:
        return 0.0
    mask = True
    if "Ano" in hist_custo.columns:
        mask = mask & (hist_custo["Ano"] == ano_ant)
    if "Período" in hist_custo.columns:
        mask = mask & (hist_custo["Período"] == mes)
    if "Despesa Primaria" in hist_custo.columns:
        return float(hist_custo.loc[mask, "Despesa Primaria"].sum())
    return 0.0


def calcular_flex_budget_mes(dados: dict, mes: str) -> float:
    """
    Calcula Flex Budget total para um mês.
    Usa calcular_flex_budget do shared.py (mesma lógica do waterfall TC Veículos).
    """
    custo_bud_full = dados.get("_custo_bud_full")
    vol_bud_full = dados.get("_vol_bud_full")
    vol_actual_full = dados.get("_vol_actual_full")

    if custo_bud_full is None or vol_bud_full is None or vol_actual_full is None:
        return 0.0

    # ── Preferir a função canônica do shared.py (alinhada com waterfall) ──
    try:
        df_flex = calcular_flex_budget(
            custo_bud_full, vol_bud_full, vol_actual_full,
            col_custo="Custo FP",
        )
        if df_flex is not None and not df_flex.empty and "Período" in df_flex.columns:
            row = df_flex[df_flex["Período"] == mes]
            if not row.empty:
                return float(row["Flex_Bud"].sum())
    except Exception:
        pass

    # ── Fallback inline (caso calcular_flex_budget falhe) ──
    try:
        from tc_principal.shared import mask_custo_fixo
    except ImportError:
        import unicodedata

        def mask_custo_fixo(serie):
            def _norm(v):
                if pd.isna(v):
                    return ""
                return unicodedata.normalize("NFKD", str(v)).encode("ascii", "ignore").decode("ascii").strip().lower()
            return serie.astype(str).map(_norm).str.startswith("fix")

    df_bud = custo_bud_full[custo_bud_full["Período"] == mes] if "Período" in custo_bud_full.columns else custo_bud_full

    if df_bud.empty or "Custo FP" not in df_bud.columns or "Custo" not in df_bud.columns:
        return 0.0

    fixo_mask = mask_custo_fixo(df_bud["Custo"])
    custo_fixo = float(df_bud.loc[fixo_mask, "Custo FP"].sum())
    custo_total = float(df_bud["Custo FP"].sum())
    custo_nao_fixo = custo_total - custo_fixo

    vol_bud = _safe_sum(vol_bud_full, "Volume", mes)
    vol_act = _safe_sum(vol_actual_full, "Volume", mes)

    proporcao = vol_act / vol_bud if vol_bud != 0 else 1.0
    flex = custo_fixo + custo_nao_fixo * proporcao
    return flex


def calcular_variacoes(dados: dict) -> dict[str, Any]:
    """
    Calcula todas as variações necessárias para o relatório mensal.

    Returns:
        Dict com variações de volume, custo, CPU para cada comparativo.
    """
    mes = dados["mes_nome"]
    ano = dados["ano"]
    ano_ant = dados["ano_anterior"]

    # ── Volume ──
    vol_real = _volume_total(dados["volume_real"])
    vol_bud = _volume_total(dados["volume_bud"])
    vol_actual = _volume_total(dados["volume_actual"])
    vol_real_ant = _volume_total(dados["volume_real_ant"])
    vol_ano_ant = _volume_ano_anterior(dados["historico_vol"], ano_ant, mes)

    # Volume por modelo
    vol_modelos_real = _volume_por_modelo(dados["volume_real"])
    vol_modelos_ant = _volume_por_modelo(dados["volume_real_ant"])
    vol_modelos_bud = _volume_por_modelo(dados["volume_bud"])
    vol_modelos_actual = _volume_por_modelo(dados["volume_actual"])
    vol_modelos_ano_ant = _volume_por_modelo_ano_anterior(
        dados["historico_vol"], ano_ant, mes,
    )
    mes_numero = dados["mes_numero"]

    # ── Custo (Despesa Primária) ──
    desp_real = _despesa_total(dados["custo_real"])
    desp_bud = _despesa_total(dados["custo_bud"])
    desp_real_ant = _despesa_total(dados["custo_real_ant"])
    desp_ano_ant = _despesa_ano_anterior(dados["historico_custo"], ano_ant, mes)

    # ── Custo FP (usar df_principal — fonte correta, sem duplicação por veículo) ──
    fp_real = _safe_sum(dados["custo_real"], "Custo FP")
    fp_bud = _safe_sum(dados["custo_bud"], "Custo FP")
    fp_real_ant = _safe_sum(dados["custo_real_ant"], "Custo FP")

    # ── Flex Budget ──
    flex_bud = calcular_flex_budget_mes(dados, mes)

    # ── CPU ──
    cpu_modelos_real = _cpu_por_modelo(dados["cpu_real"])
    cpu_modelos_bud = _cpu_por_modelo(dados["cpu_bud"])
    cpu_modelos_ant = _cpu_por_modelo(dados["cpu_real_ant"])

    # ── Variações por modelo (volume) ──
    variacao_modelos = {}
    todos_modelos = set(
        list(vol_modelos_real.keys()) + list(vol_modelos_ant.keys())
        + list(vol_modelos_bud.keys()) + list(vol_modelos_ano_ant.keys())
    )
    for modelo in sorted(todos_modelos):
        v_real = vol_modelos_real.get(modelo, 0)
        v_ant = vol_modelos_ant.get(modelo, 0)
        v_bud = vol_modelos_bud.get(modelo, 0)
        v_actual = vol_modelos_actual.get(modelo, 0)
        v_ano_ant = vol_modelos_ano_ant.get(modelo, 0)
        variacao_modelos[modelo] = {
            "vol_real": v_real,
            "vol_mes_ant": v_ant,
            "vol_budget": v_bud,
            "vol_actual": v_actual,
            "vol_ano_ant": v_ano_ant,
            "var_mes_ant": v_real - v_ant,
            "var_budget": v_real - v_bud,
            "var_ano_ant": v_real - v_ano_ant,
            "pct_mes_ant": ((v_real - v_ant) / abs(v_ant) * 100) if v_ant != 0 else None,
            "pct_budget": ((v_real - v_bud) / abs(v_bud) * 100) if v_bud != 0 else None,
            "pct_ano_ant": ((v_real - v_ano_ant) / abs(v_ano_ant) * 100) if v_ano_ant != 0 else None,
        }

    # ── Flags de disponibilidade de referência ──
    sem_budget = (vol_bud == 0 and fp_bud == 0)
    sem_mes_anterior = (vol_real_ant == 0 and fp_real_ant == 0 and mes_numero == 1)
    sem_ano_anterior = (vol_ano_ant == 0 and desp_ano_ant == 0)

    return {
        "volume": {
            "real": vol_real,
            "budget": vol_bud,
            "actual": vol_actual,
            "mes_anterior": vol_real_ant,
            "ano_anterior": vol_ano_ant,
        },
        "despesa": {
            "real": desp_real,
            "budget": desp_bud,
            "flex": flex_bud,
            "mes_anterior": desp_real_ant,
            "ano_anterior": desp_ano_ant,
        },
        "custo_fp": {
            "real": fp_real,
            "budget": fp_bud,
            "flex": flex_bud,
            "mes_anterior": fp_real_ant,
        },
        "variacao_modelos": variacao_modelos,
        "cpu_modelos": {
            "real": cpu_modelos_real,
            "budget": cpu_modelos_bud,
            "mes_anterior": cpu_modelos_ant,
        },
        # Flags de referência
        "sem_budget": sem_budget,
        "sem_mes_anterior": sem_mes_anterior,
        "sem_ano_anterior": sem_ano_anterior,
    }


# ═══════════════════════════════════════════════════════════════
#  FORMATAR DADOS PARA A LLM
# ═══════════════════════════════════════════════════════════════

def _fmt_pct_modelo(pct_val) -> str:
    """Formata percentual de variação por modelo (None → N/A)."""
    if pct_val is None:
        return "N/A (sem ref.)"
    sinal = "+" if pct_val > 0 else ""
    return f"{sinal}{pct_val:.1f}%"


def formatar_dados_volume(dados: dict, variacoes: dict) -> str:
    """Formata dados de volume em 4 sub-tópicos para o prompt da LLM."""
    v = variacoes["volume"]
    modelos = variacoes["variacao_modelos"]

    lines = [
        f"=== 1. ANÁLISE DE VOLUME — {dados['mes_nome']}/{dados['ano']} ===",
        "",
        "--- 1.1 Volume Total ---",
        f"Volume Real: {_fmt(v['real'], 0)} un.",
        f"Volume Actual (projeção): {_fmt(v['actual'], 0)} un.",
        f"Volume Budget: {_fmt(v['budget'], 0)} un.",
        "",
    ]

    # --- 1.2 Real vs Budget ---
    lines.append("--- 1.2 Real vs Budget ---")
    lines.append(f"Delta total: {_var_abs(v['real'], v['budget'])} un. ({_pct(v['real'], v['budget'])})")
    lines.append("Top 10 modelos por impacto (Real vs Budget):")
    sorted_bud = sorted(modelos.items(), key=lambda x: abs(x[1]["var_budget"]), reverse=True)[:10]
    for modelo, info in sorted_bud:
        diff = info["var_budget"]
        sinal = "+" if diff > 0 else ""
        qual = "acima" if diff > 0 else "abaixo"
        cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
        lines.append(
            f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
            f"({qual} do budget em {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info['pct_budget'])})"
        )
    lines.append("")

    # --- 1.3 Real vs Mês Anterior ---
    lines.append("--- 1.3 Real vs Mês Anterior ---")
    lines.append(f"Delta total: {_var_abs(v['real'], v['mes_anterior'])} un. ({_pct(v['real'], v['mes_anterior'])})")
    lines.append("Top 10 modelos por impacto (Real vs Mês Anterior):")
    sorted_ant = sorted(modelos.items(), key=lambda x: abs(x[1]["var_mes_ant"]), reverse=True)[:10]
    for modelo, info in sorted_ant:
        diff = info["var_mes_ant"]
        sinal = "+" if diff > 0 else ""
        qual = "aumento" if diff > 0 else "redução"
        cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
        lines.append(
            f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
            f"({qual} de {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info['pct_mes_ant'])})"
        )
    lines.append("")

    # --- 1.4 Real vs Ano Anterior ---
    lines.append(f"--- 1.4 Real vs Mesmo Mês de {dados['ano_anterior']} ---")
    lines.append(f"Delta total: {_var_abs(v['real'], v['ano_anterior'])} un. ({_pct(v['real'], v['ano_anterior'])})")
    sorted_yoy = sorted(modelos.items(), key=lambda x: abs(x[1].get("var_ano_ant", 0)), reverse=True)[:10]
    has_yoy = any(info.get("vol_ano_ant", 0) != 0 or info.get("var_ano_ant", 0) != 0 for _, info in sorted_yoy)
    if has_yoy:
        lines.append("Top 10 modelos por impacto (Real vs Ano Anterior):")
        for modelo, info in sorted_yoy:
            diff = info.get("var_ano_ant", 0)
            sinal = "+" if diff > 0 else ""
            qual = "aumento" if diff > 0 else "redução"
            cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
            lines.append(
                f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
                f"({qual} de {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info.get('pct_ano_ant'))})"
            )
    else:
        lines.append("⚠️ Sem dados de volume do ano anterior para comparação por modelo.")

    return "\n".join(lines)


def formatar_dados_variacoes_modelo(dados: dict, variacoes: dict) -> str:
    """Formata maiores variações de volume por modelo de veículo para a LLM."""
    modelos = variacoes.get("variacao_modelos", {})
    if not modelos:
        return "Sem dados de variação por modelo."

    lines = [
        f"Maiores variações de volume por modelo em {dados['mes_nome']}/{dados['ano']}:",
        "",
    ]

    # Top 10 por variação absoluta vs mês anterior
    sorted_ant = sorted(
        modelos.items(),
        key=lambda x: abs(x[1]["var_mes_ant"]),
        reverse=True,
    )[:10]

    lines.append("Variações vs Mês Anterior (top 10 por impacto):")
    for modelo, info in sorted_ant:
        diff = info["var_mes_ant"]
        sinal = "+" if diff > 0 else ""
        qual = "aumento" if diff > 0 else "redução"
        cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
        lines.append(
            f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
            f"({qual} de {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info['pct_mes_ant'])})"
        )

    lines.append("")

    # Top 10 por variação absoluta vs budget
    sorted_bud = sorted(
        modelos.items(),
        key=lambda x: abs(x[1]["var_budget"]),
        reverse=True,
    )[:10]

    lines.append("Variações vs Budget (top 10 por impacto):")
    for modelo, info in sorted_bud:
        diff = info["var_budget"]
        sinal = "+" if diff > 0 else ""
        qual = "acima" if diff > 0 else "abaixo"
        cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
        lines.append(
            f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
            f"({qual} do budget em {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info['pct_budget'])})"
        )

    lines.append("")

    # Top 10 por variação absoluta vs ano anterior
    sorted_yoy = sorted(
        modelos.items(),
        key=lambda x: abs(x[1].get("var_ano_ant", 0)),
        reverse=True,
    )[:10]
    has_yoy = any(info.get("vol_ano_ant", 0) != 0 for _, info in sorted_yoy)
    if has_yoy:
        lines.append("Variações vs Ano Anterior (top 10 por impacto):")
        for modelo, info in sorted_yoy:
            diff = info.get("var_ano_ant", 0)
            sinal = "+" if diff > 0 else ""
            qual = "aumento" if diff > 0 else "redução"
            cor_vol = "🟢" if diff > 0 else ("🔴" if diff < 0 else "⚪")
            lines.append(
                f"- {cor_vol} {modelo}: {_fmt(info['vol_real'], 0)} un. "
                f"({qual} de {sinal}{_fmt(diff, 0)} un., {_fmt_pct_modelo(info.get('pct_ano_ant'))})"
            )
    else:
        lines.append("⚠️ Sem dados de volume do ano anterior para comparação por modelo.")

    return "\n".join(lines)


def _flex_adjust_df(
    df_bud: pd.DataFrame | None,
    vol_actual: float,
    vol_budget: float,
) -> pd.DataFrame | None:
    """
    Cria cópia do DataFrame de Budget com Custo FP ajustado pelo Flex.
    Fixo permanece igual; Variável é multiplicado por (vol_actual / vol_budget).
    """
    if df_bud is None or df_bud.empty:
        return df_bud
    if vol_budget == 0 or "Custo" not in df_bud.columns or "Custo FP" not in df_bud.columns:
        return df_bud

    try:
        from tc_principal.shared import mask_custo_fixo
    except ImportError:
        import unicodedata as _ud

        def mask_custo_fixo(serie):
            def _norm(v):
                if pd.isna(v):
                    return ""
                return _ud.normalize("NFKD", str(v)).encode("ascii", "ignore").decode("ascii").strip().lower()
            return serie.astype(str).map(_norm).str.startswith("fix")

    df = df_bud.copy()
    ratio = vol_actual / vol_budget
    fixo = mask_custo_fixo(df["Custo"])
    df.loc[~fixo, "Custo FP"] = df.loc[~fixo, "Custo FP"] * ratio
    return df


def _calc_diffs(real_series: pd.Series, ref_series: pd.Series) -> list[tuple[str, float, float, float]]:
    """Retorna lista de (nome, real, ref, diff) ordenada por |diff| desc."""
    all_keys = set(real_series.index) | set(ref_series.index)
    items = []
    for k in all_keys:
        r = float(real_series.get(k, 0))
        b = float(ref_series.get(k, 0))
        items.append((k, r, b, r - b))
    return sorted(items, key=lambda x: abs(x[3]), reverse=True)


def _drill_down_completo(
    df_real: pd.DataFrame | None,
    df_ref: pd.DataFrame | None,
    label_ref: str,
    col_ref: str = "Custo FP",
    tipo: str = "mes_anterior",
    top_type06: int = 3,
    top_accounts: int = 2,
    volume: float = 0,
) -> str:
    """
    Drill-down 3 níveis: Type 05 → Type 06 → Account.
    Todos os valores em kBRL + R$/veíc (se volume > 0). Nomes originais preservados.

    col_ref: coluna do df_ref a somar (ex: 'Custo FP' ou 'Flex_Bud').
    tipo: tipo de comparação — define linguagem (ganho/perda vs redução/aumento).
    volume: volume real para cálculo de R$/veíc (0 = não exibir).
    """
    if df_real is None or df_real.empty:
        return ""
    required_real = {"Type 05", "Type 06", "Account", "Custo FP"}
    if not required_real.issubset(df_real.columns):
        return ""

    # Se referência ausente ou vazia — avisar explicitamente
    if df_ref is None or df_ref.empty:
        total_real = float(df_real["Custo FP"].sum())
        return (
            f"\n⚠️ Referência ({label_ref}) não disponível para este período.\n"
            f"Custo FP Real total: {_fmt_k(total_real)}. Delta não calculável.\n"
        )

    # Linguagem contextual
    is_budget = tipo in ("flex", "budget")
    lbl_neg = "ganho" if is_budget else "redução"
    lbl_pos = "perda" if is_budget else "aumento"

    # Séries de referência (vazias se não houver dados)
    def _ref_grouped(cols):
        if df_ref is not None and not df_ref.empty and col_ref in df_ref.columns:
            req_dims = set(cols)
            if req_dims.issubset(df_ref.columns):
                return df_ref.groupby(list(cols))[col_ref].sum()
        return pd.Series(dtype=float)

    real_t05 = df_real.groupby("Type 05")["Custo FP"].sum().sort_values(ascending=False)
    ref_t05 = _ref_grouped(["Type 05"])

    lines = []

    for t05 in real_t05.index:
        val_r = float(real_t05.get(t05, 0))
        val_b = float(ref_t05.get(t05, 0)) if t05 in ref_t05.index else 0.0
        delta = val_r - val_b
        sinal = "+" if delta > 0 else ""
        qual = lbl_neg if delta < 0 else lbl_pos

        lines.append("")
        cor = "🔴" if delta > 0 else ("🟢" if delta < 0 else "⚪")
        # R$/veíc para Type 05 (se volume disponível)
        if volume and volume > 0:
            cpu_r = val_r / volume
            cpu_d = delta / volume
            s_cpu = "+" if cpu_d >= 0 else ""
            lines.append(
                f"**{cor} {t05}**: {_fmt_k(val_r)} ({_fmt_cpu(cpu_r)}) "
                f"| Δ {sinal}{_fmt_k(delta)} (Δ {s_cpu}{_fmt_cpu(cpu_d)}), {_pct(val_r, val_b)} | {qual}"
            )
        else:
            lines.append(
                f"**{cor} {t05}**: {_fmt_k(val_r)} | Δ {sinal}{_fmt_k(delta)}, {_pct(val_r, val_b)} | {qual}"
            )

        # ── Nível Type 06 dentro deste Type 05 ──
        df_r_t05 = df_real[df_real["Type 05"] == t05]
        real_t06 = df_r_t05.groupby("Type 06")["Custo FP"].sum()

        ref_t06 = pd.Series(dtype=float)
        if df_ref is not None and not df_ref.empty and "Type 05" in df_ref.columns and "Type 06" in df_ref.columns:
            df_b_t05 = df_ref[df_ref["Type 05"] == t05]
            if not df_b_t05.empty and col_ref in df_b_t05.columns:
                ref_t06 = df_b_t05.groupby("Type 06")[col_ref].sum()

        diffs_t06 = _calc_diffs(real_t06, ref_t06)

        for t06_name, r06, b06, d06 in diffs_t06[:top_type06]:
            s06 = "+" if d06 > 0 else ""
            cor06 = "🔴" if d06 > 0 else ("🟢" if d06 < 0 else "⚪")
            if volume and volume > 0:
                cpu_06 = r06 / volume
                cpu_d06 = d06 / volume
                s_c06 = "+" if cpu_d06 >= 0 else ""
                lines.append(
                    f"- {cor06} {t06_name}: {_fmt_k(r06)} ({_fmt_cpu(cpu_06)}) "
                    f"| Δ {s06}{_fmt_k(d06)} (Δ {s_c06}{_fmt_cpu(cpu_d06)})"
                )
            else:
                lines.append(
                    f"- {cor06} {t06_name}: {_fmt_k(r06)} | Δ {s06}{_fmt_k(d06)}"
                )

            # ── Nível Account dentro deste Type 06 ──
            df_r_t06 = df_r_t05[df_r_t05["Type 06"] == t06_name]
            real_acc = df_r_t06.groupby("Account")["Custo FP"].sum()

            ref_acc = pd.Series(dtype=float)
            if df_ref is not None and not df_ref.empty:
                mask_ref = (df_ref.get("Type 05", pd.Series()) == t05) & (df_ref.get("Type 06", pd.Series()) == t06_name)
                df_b_t06 = df_ref.loc[mask_ref] if mask_ref.any() else pd.DataFrame()
                if not df_b_t06.empty and "Account" in df_b_t06.columns and col_ref in df_b_t06.columns:
                    ref_acc = df_b_t06.groupby("Account")[col_ref].sum()

            diffs_acc = _calc_diffs(real_acc, ref_acc)
            for acc_name, r_a, b_a, d_a in diffs_acc[:top_accounts]:
                s_a = "+" if d_a > 0 else ""
                cor_a = "🔴" if d_a > 0 else ("🟢" if d_a < 0 else "⚪")
                if volume and volume > 0:
                    cpu_da = d_a / volume
                    s_ca = "+" if cpu_da >= 0 else ""
                    lines.append(
                        f"  - {cor_a} {acc_name}: {s_a}{_fmt_k(d_a)} (Δ {s_ca}{_fmt_cpu(cpu_da)})"
                    )
                else:
                    lines.append(
                        f"  - {cor_a} {acc_name}: {s_a}{_fmt_k(d_a)}"
                    )

    lines.append("")
    lines.append("🟢 = economia/ganho | 🔴 = perda/aumento de despesa")
    return "\n".join(lines)


def formatar_dados_comparativo(
    dados: dict,
    variacoes: dict,
    tipo: str,
) -> str:
    """
    Formata dados de um comparativo específico.

    Args:
        tipo: 'mes_anterior', 'flex', 'budget', 'ano_anterior'
    """
    v = variacoes["volume"]
    d = variacoes["despesa"]

    if tipo == "mes_anterior":
        ref_vol = v["mes_anterior"]
        label = "Mês Anterior"
    elif tipo == "flex":
        ref_vol = v["actual"]  # Flex usa volume actual como referência
        label = "Flex Budget"
    elif tipo == "budget":
        ref_vol = v["budget"]
        label = "Budget"
    elif tipo == "ano_anterior":
        ref_vol = v["ano_anterior"]
        label = f"Mesmo Mês {dados['ano_anterior']}"
    else:
        return f"[Tipo comparativo '{tipo}' não reconhecido]"

    # ── Custo FP (do df_principal — fonte correta) ──
    fp = variacoes["custo_fp"]
    fp_real = fp["real"]
    if tipo == "mes_anterior":
        fp_ref = fp.get("mes_anterior", 0)
        vol_ref = v["mes_anterior"]
    elif tipo == "flex":
        fp_ref = fp.get("flex", 0)
        vol_ref = v["actual"]
    elif tipo == "budget":
        fp_ref = fp.get("budget", 0)
        vol_ref = v["budget"]
    else:
        fp_ref = 0
        vol_ref = v.get("ano_anterior", 0)

    # CPU total (R$/veíc)
    vol_real = v["real"]
    cpu_real_t = _cpu(fp_real, vol_real)
    cpu_ref_t = _cpu(fp_ref, vol_ref)
    cpu_delta = cpu_real_t - cpu_ref_t
    s_cpu = "+" if cpu_delta >= 0 else ""

    lines = [
        f"Comparativo Real vs {label}:",
        f"",
        f"Custo FP Total: {_fmt_k(fp_real)} ({_fmt_cpu(cpu_real_t)}) "
        f"| Δ {_var_k(fp_real, fp_ref)} ({s_cpu}{_fmt_cpu(cpu_delta)}), "
        f"{_pct(fp_real, fp_ref)} vs {label} de {_fmt_k(fp_ref)} ({_fmt_cpu(cpu_ref_t)})",
    ]

    # ── Drill-down 3 níveis: Type 05 → Type 06 → Account ──
    df_real = dados.get("custo_real")
    if tipo == "mes_anterior":
        df_ref = dados.get("custo_real_ant")
        col_ref = "Custo FP"
    elif tipo == "budget":
        df_ref = dados.get("custo_bud")
        col_ref = "Custo FP"
    elif tipo == "flex":
        # Reutilizar calcular_flex_budget_detalhado do shared.py (mesma lógica do waterfall)
        try:
            df_flex_det = calcular_flex_budget_detalhado(
                dados["_custo_bud_full"],
                dados["_vol_bud_full"],
                dados["_vol_actual_full"],
                col_custo="Custo FP",
            )
        except Exception:
            df_flex_det = None
        if df_flex_det is not None and not df_flex_det.empty:
            col_per = "Período" if "Período" in df_flex_det.columns else "Periodo"
            mes = dados["mes_nome"]
            df_ref = df_flex_det[df_flex_det[col_per] == mes].copy()
        else:
            # Fallback: ajuste inline
            df_ref = _flex_adjust_df(dados.get("custo_bud"), v["actual"], v["budget"])
        col_ref = "Flex_Bud" if (df_ref is not None and "Flex_Bud" in df_ref.columns) else "Custo FP"
    else:
        # ano_anterior: usar historico_custo filtrado pelo mesmo mês e ano anterior
        hist = dados.get("historico_custo")
        ano_ant = dados.get("ano_anterior", dados["ano"] - 1)
        mes = dados["mes_nome"]
        if hist is not None and not hist.empty:
            mask = pd.Series(True, index=hist.index)
            if "Ano" in hist.columns:
                mask = mask & (hist["Ano"] == ano_ant)
            if "Período" in hist.columns:
                mask = mask & (hist["Período"] == mes)
            df_ref = hist.loc[mask].copy() if mask.any() else None
        else:
            df_ref = None
        col_ref = "Custo FP"

    lines.append(_drill_down_completo(df_real, df_ref, label, col_ref=col_ref, tipo=tipo, volume=v["real"]))

    # CPU por modelo (R$/veíc)
    cpu_real = variacoes["cpu_modelos"].get("real", {})
    cpu_ref_key = "budget" if tipo in ("flex", "budget") else "mes_anterior"
    cpu_ref = variacoes["cpu_modelos"].get(cpu_ref_key, {})
    if cpu_real:
        lines.append("")
        lines.append("**CPU por modelo (R$/veíc):**")
        for modelo in sorted(cpu_real.keys()):
            r = cpu_real.get(modelo, 0)
            b = cpu_ref.get(modelo, 0)
            diff = r - b
            s = "+" if diff > 0 else ""
            cor_cpu = "🔴" if diff > 0 else ("🟢" if diff < 0 else "⚪")
            lines.append(
                f"- {cor_cpu} {modelo}: {_fmt_cpu(r)} | Δ {s}{_fmt_cpu(diff)}, {_pct(r, b)}"
            )

    return "\n".join(lines)


def formatar_dados_anomalias(dados: dict, variacoes: dict) -> str:
    """Formata dados consolidados para detecção de anomalias pela LLM."""
    v = variacoes["volume"]
    d = variacoes["despesa"]
    fp = variacoes["custo_fp"]

    lines = [
        f"Resumo consolidado do mês de {dados['mes_nome']}/{dados['ano']}:",
        f"",
        f"Volume total Real: {_fmt(v['real'], 0)} un.",
        f"Despesa Primária Real: R$ {_fmt(d['real'])}",
        f"Custo FP Real: R$ {_fmt(fp['real'])}",
        f"Flex Budget: R$ {_fmt(d['flex'])}",
        f"",
        f"Variações significativas:",
        f"  Real vs Mês Anterior (Desp.): {_pct(d['real'], d['mes_anterior'])}",
        f"  Real vs Flex: {_pct(d['real'], d['flex'])}",
        f"  Real vs Budget: {_pct(d['real'], d['budget'])}",
        f"  Real vs Ano Anterior: {_pct(d['real'], d['ano_anterior'])}",
        f"",
        f"Modelos com maiores variações (volume vs mês anterior):",
    ]

    # Top 5 variações
    modelos_sorted = sorted(
        variacoes["variacao_modelos"].items(),
        key=lambda x: abs(x[1]["var_mes_ant"]),
        reverse=True,
    )[:5]
    for modelo, info in modelos_sorted:
        lines.append(
            f"  {modelo}: {_var_abs(info['vol_real'], info['vol_mes_ant'])} un. "
            f"({_pct(info['vol_real'], info['vol_mes_ant'])})"
        )

    return "\n".join(lines)


def formatar_comparativo_budget_flex_unificado(
    dados: dict, variacoes: dict,
) -> str:
    """
    Seção unificada Budget + Flex Volume + Operacional.

    Narrativa do waterfall:
      Budget → +Efeito Volume (barra amarela) → Flex → Real
      Efeito Volume = Flex - Budget
      Efeito Operacional = Real - Flex

    Drill-down detalhado usa Flex como referência (igual ao antigo Real vs Flex).
    """
    fp = variacoes["custo_fp"]
    v = variacoes["volume"]
    fp_real = fp["real"]
    fp_bud = fp.get("budget", 0)
    fp_flex = fp.get("flex", 0)
    vol_real = v["real"]
    vol_bud = v["budget"]

    efeito_volume = fp_flex - fp_bud      # barra amarela waterfall
    efeito_operacional = fp_real - fp_flex  # restante
    delta_total = fp_real - fp_bud         # soma dos dois

    # CPU (R$/veíc)
    cpu_bud = _cpu(fp_bud, vol_bud)
    cpu_flex = _cpu(fp_flex, vol_real)   # Flex já ajusta para vol real
    cpu_real_t = _cpu(fp_real, vol_real)
    cpu_ev = cpu_flex - cpu_bud
    cpu_eo = cpu_real_t - cpu_flex
    cpu_dt = cpu_real_t - cpu_bud

    # Sinal e cor para cada efeito (convenção despesa: Δ+ = 🔴, Δ- = 🟢)
    def _sinal_cor(val):
        s = "+" if val >= 0 else ""
        c = "🔴" if val > 0 else ("🟢" if val < 0 else "⚪")
        return s, c

    s_vol, c_vol = _sinal_cor(efeito_volume)
    s_op, c_op = _sinal_cor(efeito_operacional)
    s_tot, c_tot = _sinal_cor(delta_total)
    s_cpu_ev = "+" if cpu_ev >= 0 else ""
    s_cpu_eo = "+" if cpu_eo >= 0 else ""
    s_cpu_dt = "+" if cpu_dt >= 0 else ""

    lines = [
        "Comparativo Real vs Budget (com Efeito Flex Volume):",
        "",
        f"- **Budget:** {_fmt_k(fp_bud)} ({_fmt_cpu(cpu_bud)})",
        f"- **{c_vol} Efeito Flex Volume:** {s_vol}{_fmt_k(efeito_volume)} ({s_cpu_ev}{_fmt_cpu(cpu_ev)})",
        f"- **{c_op} Efeito Operacional:** {s_op}{_fmt_k(efeito_operacional)} ({s_cpu_eo}{_fmt_cpu(cpu_eo)}) | {_pct(fp_real, fp_flex)} vs Flex",
        f"- **{c_tot} Delta Total:** {s_tot}{_fmt_k(delta_total)} ({s_cpu_dt}{_fmt_cpu(cpu_dt)}) | {_pct(fp_real, fp_bud)} vs Budget",
        f"- **Real:** {_fmt_k(fp_real)} ({_fmt_cpu(cpu_real_t)})",
        "",
    ]

    # Parágrafo explicativo do waterfall
    _imp_vol = "negativamente" if efeito_volume > 0 else "positivamente"
    _res_vol = "aumento" if efeito_volume > 0 else "redução"
    _rel_vol = "superou" if vol_real > vol_bud else "ficou abaixo do"
    _imp_op = "gerou uma economia" if efeito_operacional < 0 else "gerou um aumento"
    _perf_op = "melhor" if efeito_operacional < 0 else "pior"
    lines.append(
        f"O Efeito Flex Volume impactou {_imp_vol} o custo, resultando em "
        f"{'um ' + _res_vol if efeito_volume != 0 else 'variação nula'} de {_fmt_k(abs(efeito_volume))} "
        f"devido ao volume real de {_fmt(vol_real, 0)} un., que {_rel_vol} Budget de "
        f"{_fmt(vol_bud, 0)} un. em {_pct(vol_real, vol_bud)}. "
        f"Após o ajuste pelo volume, o Flex Budget ficou em {_fmt_k(fp_flex)} ({_fmt_cpu(cpu_flex)}). "
        f"O Efeito Operacional, que mede a eficiência de preço e mix, "
        f"{_imp_op} de {_fmt_k(abs(efeito_operacional))} (Δ {s_cpu_eo}{_fmt_cpu(cpu_eo)}), "
        f"indicando uma performance {_perf_op} do que o esperado."
    )
    lines.append("")

    # Drill-down com Flex como referência (mesma lógica do antigo tipo "flex")
    df_real = dados.get("custo_real")
    try:
        df_flex_det = calcular_flex_budget_detalhado(
            dados["_custo_bud_full"],
            dados["_vol_bud_full"],
            dados["_vol_actual_full"],
            col_custo="Custo FP",
        )
    except Exception:
        df_flex_det = None

    if df_flex_det is not None and not df_flex_det.empty:
        col_per = "Período" if "Período" in df_flex_det.columns else "Periodo"
        mes = dados["mes_nome"]
        df_ref = df_flex_det[df_flex_det[col_per] == mes].copy()
    else:
        v_data = variacoes["volume"]
        df_ref = _flex_adjust_df(dados.get("custo_bud"), v_data["actual"], v_data["budget"])

    col_ref = "Flex_Bud" if (df_ref is not None and "Flex_Bud" in df_ref.columns) else "Custo FP"

    lines.append(_drill_down_completo(df_real, df_ref, "Flex Budget", col_ref=col_ref, tipo="flex", volume=v["real"]))

    # CPU por modelo (R$/veíc)
    cpu_real = variacoes["cpu_modelos"].get("real", {})
    cpu_ref = variacoes["cpu_modelos"].get("budget", {})
    if cpu_real:
        lines.append("")
        lines.append("**CPU por modelo (R$/veíc):**")
        for modelo in sorted(cpu_real.keys()):
            r = cpu_real.get(modelo, 0)
            b = cpu_ref.get(modelo, 0)
            diff = r - b
            s = "+" if diff > 0 else ""
            cor_cpu = "🔴" if diff > 0 else ("🟢" if diff < 0 else "⚪")
            lines.append(
                f"- {cor_cpu} {modelo}: {_fmt_cpu(r)} | Δ {s}{_fmt_cpu(diff)}, {_pct(r, b)}"
            )

    return "\n".join(lines)


def formatar_dados_comparativos_agrupado(
    dados: dict, variacoes: dict,
) -> dict[str, str]:
    """
    Formata os 3 comparativos unificados.

    Retorna dict com chaves "budget_flex", "mes_anterior", "ano_anterior".
    Cada valor é o drill-down formatado (sem header ### ).
    Chaves ausentes → dados indisponíveis (flags sem_mes_anterior / sem_ano_anterior).
    """
    resultado: dict[str, str] = {
        "budget_flex": formatar_comparativo_budget_flex_unificado(dados, variacoes),
    }

    sem_mes_ant = variacoes.get("sem_mes_anterior", False)
    if not sem_mes_ant:
        resultado["mes_anterior"] = formatar_dados_comparativo(
            dados, variacoes, "mes_anterior",
        )

    sem_ano_ant = variacoes.get("sem_ano_anterior", False)
    if not sem_ano_ant:
        resultado["ano_anterior"] = formatar_dados_comparativo(
            dados, variacoes, "ano_anterior",
        )

    return resultado


# ── FUNÇÕES CONSOLIDADAS (v2) ──────────────────────────────────

def formatar_dados_volume_completo(dados: dict, variacoes: dict) -> str:
    """
    Consolida volume + variações por modelo num único bloco de dados para a LLM.
    Fusão das seções 1 (volume) + 2 (variações modelo).
    """
    bloco_volume = formatar_dados_volume(dados, variacoes)
    bloco_modelos = formatar_dados_variacoes_modelo(dados, variacoes)

    # Flags de referência
    avisos = []
    if variacoes.get("sem_budget"):
        avisos.append("⚠️ Budget não disponível para este período — comparações vs Budget são nulas.")
    if variacoes.get("sem_mes_anterior"):
        avisos.append("⚠️ Mês anterior não disponível (primeiro mês do ano) — comparações vs mês anterior limitadas.")
    if variacoes.get("sem_ano_anterior"):
        avisos.append("⚠️ Ano anterior sem dados para este mês — comparação YoY não disponível.")

    parts = [bloco_volume, "", "---", "", bloco_modelos]
    if avisos:
        parts.extend(["", "---", ""] + avisos)
    return "\n".join(parts)


def formatar_dados_conclusoes(dados: dict, variacoes: dict) -> str:
    """
    Consolida anomalias + observações finais num único bloco para a LLM.
    Fusão das seções 4 (anomalias) + 5 (observações finais).
    """
    bloco_anomalias = formatar_dados_anomalias(dados, variacoes)
    bloco_resumo = formatar_resumo_mes(dados, variacoes)

    # Flags de referência
    avisos = []
    if variacoes.get("sem_budget"):
        avisos.append("⚠️ Budget não disponível — parte das comparações não são possíveis.")
    if variacoes.get("sem_mes_anterior"):
        avisos.append("⚠️ Mês anterior sem dados — variações vs mês anterior indisponíveis.")

    parts = [bloco_anomalias, "", "---", "", bloco_resumo]
    if avisos:
        parts.extend(["", "---", ""] + avisos)
    return "\n".join(parts)


def descobrir_oficinas(dados: dict) -> list[str]:
    """Retorna lista de oficinas únicas presentes nos dados reais do mês."""
    df = dados.get("custo_real")
    if df is None or df.empty or "Oficina" not in df.columns:
        return []
    oficinas = sorted(df["Oficina"].dropna().unique().tolist())
    return [o for o in oficinas if str(o).strip()]


def _filtrar_por_oficina(dados: dict, oficina: str) -> dict:
    """
    Cria cópia dos dados filtrados por Oficina.
    Retorna novo dict compatível com formatar_dados_comparativo.
    """
    dados_ofc = {}
    for k, v in dados.items():
        if isinstance(v, pd.DataFrame) and "Oficina" in v.columns:
            dados_ofc[k] = v[v["Oficina"] == oficina].copy()
        else:
            dados_ofc[k] = v
    return dados_ofc


def _calcular_variacoes_oficina(dados_ofc: dict, dados_full: dict) -> dict:
    """Calcula variações para uma oficina, reaproveitando a lógica geral."""
    mes = dados_ofc["mes_nome"]
    ano = dados_ofc["ano"]
    ano_ant = dados_ofc.get("ano_anterior", ano - 1)

    # Custo FP
    fp_real = _safe_sum(dados_ofc.get("custo_real"), "Custo FP")
    fp_bud = _safe_sum(dados_ofc.get("custo_bud"), "Custo FP")
    fp_real_ant = _safe_sum(dados_ofc.get("custo_real_ant"), "Custo FP")

    # Flex Budget para a oficina -- cálculo simplificado
    # Usa proporção de volume global (a oficina não tem volume próprio)
    variacoes_full = calcular_variacoes(dados_full)
    vol_ratio = (
        variacoes_full["volume"]["actual"] / variacoes_full["volume"]["budget"]
        if variacoes_full["volume"]["budget"] != 0
        else 1.0
    )
    df_bud_ofc = dados_ofc.get("custo_bud")
    if df_bud_ofc is not None and not df_bud_ofc.empty and "Custo" in df_bud_ofc.columns and "Custo FP" in df_bud_ofc.columns:
        try:
            from tc_principal.shared import mask_custo_fixo
        except ImportError:
            import unicodedata as _ud
            def mask_custo_fixo(serie):
                def _norm(v):
                    if pd.isna(v): return ""
                    return _ud.normalize("NFKD", str(v)).encode("ascii", "ignore").decode("ascii").strip().lower()
                return serie.astype(str).map(_norm).str.startswith("fix")
        fixo = mask_custo_fixo(df_bud_ofc["Custo"])
        fp_fixo = float(df_bud_ofc.loc[fixo, "Custo FP"].sum())
        fp_var = float(df_bud_ofc.loc[~fixo, "Custo FP"].sum())
        flex_ofc = fp_fixo + fp_var * vol_ratio
    else:
        flex_ofc = fp_bud

    # Volume (totais globais — oficinas não têm volume)
    v = variacoes_full["volume"]

    return {
        "volume": v,
        "despesa": {
            "real": _despesa_total(dados_ofc.get("custo_real")),
            "budget": _despesa_total(dados_ofc.get("custo_bud")),
            "flex": flex_ofc,
            "mes_anterior": _despesa_total(dados_ofc.get("custo_real_ant")),
            "ano_anterior": 0,
        },
        "custo_fp": {
            "real": fp_real,
            "budget": fp_bud,
            "flex": flex_ofc,
            "mes_anterior": fp_real_ant,
        },
        "variacao_modelos": variacoes_full.get("variacao_modelos", {}),
        "cpu_modelos": variacoes_full.get("cpu_modelos", {}),
    }


def formatar_dados_oficina(
    dados: dict, variacoes: dict, oficina: str,
) -> dict[str, str]:
    """
    Formata análise resumida de uma oficina.
    Retorna dicionário com sub-seções separadas para renderização
    por tópicos na UI, além de ``texto_completo`` para LLM/PDF.

    Chaves retornadas:
        resumo, flex, mes_anterior, budget, ano_anterior, texto_completo
    """
    dados_ofc = _filtrar_por_oficina(dados, oficina)
    var_ofc = _calcular_variacoes_oficina(dados_ofc, dados)

    # ── Resumo (Custo FP + flags) ──
    fp = var_ofc["custo_fp"]
    resumo_lines = [
        f"📊 Custo FP Total: {_fmt_k(fp['real'])} "
        f"(Δ vs Flex: {_var_k(fp['real'], fp['flex'])}, {_pct(fp['real'], fp['flex'])})",
        f"   vs Budget: {_var_k(fp['real'], fp['budget'])}, {_pct(fp['real'], fp['budget'])}",
        f"   vs Mês Anterior: {_var_k(fp['real'], fp['mes_anterior'])}, {_pct(fp['real'], fp['mes_anterior'])}",
    ]
    if fp['flex'] == 0 and fp['budget'] == 0:
        resumo_lines.append("⚠️ Budget/Flex não disponível para esta oficina — deltas podem ser imprecisos.")
    if fp['mes_anterior'] == 0:
        resumo_lines.append("⚠️ Mês anterior sem dados para esta oficina.")
    resumo_text = "\n".join(resumo_lines)

    # ── Sub-seções de comparativo (drill-down por Type 05 → 06 → Account) ──
    # Seção unificada Budget+Flex
    budget_flex_text = formatar_comparativo_budget_flex_unificado(dados_ofc, var_ofc)

    comparativos_map = [
        ("budget_flex", "Real vs Budget (Efeito Flex Volume)"),
        ("mes_anterior", "Real vs Mês Anterior"),
        ("ano_anterior", f"Real vs Mesmo Mês de {dados.get('ano_anterior', dados['ano'] - 1)}"),
    ]
    sub_secoes: dict[str, str] = {
        "budget_flex": budget_flex_text,
        "mes_anterior": formatar_dados_comparativo(dados_ofc, var_ofc, "mes_anterior"),
        "ano_anterior": formatar_dados_comparativo(dados_ofc, var_ofc, "ano_anterior"),
    }

    # ── Texto completo (para LLM e PDF — retrocompatível) ──
    all_lines = [
        f"🏭 Análise da Oficina {oficina} — {dados['mes_nome']}/{dados['ano']}",
        "",
        resumo_text,
        "",
    ]
    for tipo, titulo in comparativos_map:
        all_lines.append(f"--- {titulo} ---")
        all_lines.append(sub_secoes[tipo])
        all_lines.append("")
    texto_completo = "\n".join(all_lines)

    return {
        "resumo": resumo_text,
        "budget_flex": sub_secoes["budget_flex"],
        "mes_anterior": sub_secoes["mes_anterior"],
        "ano_anterior": sub_secoes["ano_anterior"],
        "texto_completo": texto_completo,
    }


def formatar_contexto_parquet(
    ano: int, mes_numero: int,
) -> str:
    """
    Gera contexto textual completo a partir dos parquets para uso no chatbot.
    Coleta dados do mês, calcula variações e formata um resumo rico.
    """
    dados = coletar_dados_mes(ano, mes_numero)
    variacoes = calcular_variacoes(dados)

    blocos = [
        f"=== DADOS DE {dados['mes_nome'].upper()}/{ano} ===",
        "",
        "--- 📊 VOLUME E VARIAÇÕES POR MODELO ---",
        formatar_dados_volume_completo(dados, variacoes),
        "",
        "--- 📈 COMPARATIVOS ---",
        "\n\n".join(formatar_dados_comparativos_agrupado(dados, variacoes).values()),
        "",
        "--- 📋 CONCLUSÕES E ALERTAS ---",
        formatar_dados_conclusoes(dados, variacoes),
    ]

    # Oficinas
    oficinas = descobrir_oficinas(dados)
    if oficinas:
        blocos.append("")
        blocos.append("--- 🏭 ANÁLISE POR OFICINA ---")
        for ofc in oficinas:
            blocos.append("")
            ofc_result = formatar_dados_oficina(dados, variacoes, ofc)
            blocos.append(
                ofc_result["texto_completo"]
                if isinstance(ofc_result, dict)
                else str(ofc_result)
            )

    return "\n".join(blocos)


def formatar_dados_resumo_executivo(dados: dict, variacoes: dict) -> str:
    """
    Compila dados-chave para o Resumo Executivo (seção 0).
    A LLM transformará isso em 6-8 parágrafos analíticos.
    Inclui R$/veíc em todos os valores relevantes.
    """
    v = variacoes["volume"]
    fp = variacoes["custo_fp"]
    mes = dados["mes_nome"]
    ano = dados["ano"]

    fp_real = fp["real"]
    fp_bud = fp.get("budget", 0)
    fp_flex = fp.get("flex", 0)
    fp_ant = fp.get("mes_anterior", 0)
    vol_real = v["real"]
    vol_bud = v["budget"]
    vol_ant = v["mes_anterior"]
    vol_ano_ant = v.get("ano_anterior", 0)

    efeito_vol = fp_flex - fp_bud
    efeito_op = fp_real - fp_flex
    delta_bud = fp_real - fp_bud
    delta_ant = fp_real - fp_ant

    # CPU total (R$/veíc)
    cpu_real_t = _cpu(fp_real, vol_real)
    cpu_bud_t = _cpu(fp_bud, vol_bud)
    cpu_flex_t = _cpu(fp_flex, vol_real)
    cpu_ant_t = _cpu(fp_ant, vol_ant)
    cpu_ev = cpu_flex_t - cpu_bud_t
    cpu_eo = cpu_real_t - cpu_flex_t
    cpu_dt = cpu_real_t - cpu_bud_t
    cpu_da = cpu_real_t - cpu_ant_t

    s_ev = "+" if efeito_vol >= 0 else ""
    s_eo = "+" if efeito_op >= 0 else ""
    s_dt = "+" if delta_bud >= 0 else ""
    s_cev = "+" if cpu_ev >= 0 else ""
    s_ceo = "+" if cpu_eo >= 0 else ""
    s_cdt = "+" if cpu_dt >= 0 else ""
    s_cda = "+" if cpu_da >= 0 else ""

    lines = [
        f"=== DADOS PARA RESUMO EXECUTIVO — {mes}/{ano} ===",
        "",
        "** Volume de Produção **",
        f"Volume Real: {_fmt(vol_real, 0)} un.",
        f"Volume Budget: {_fmt(vol_bud, 0)} un. (Δ {_fmt(vol_real - vol_bud, 0)} un., {_pct(vol_real, vol_bud)})",
        f"Volume Mês Anterior: {_fmt(vol_ant, 0)} un. (Δ {_fmt(vol_real - vol_ant, 0)} un., {_pct(vol_real, vol_ant)})",
        f"Volume Ano Anterior: {_fmt(vol_ano_ant, 0)} un. (Δ {_fmt(vol_real - vol_ano_ant, 0)} un., {_pct(vol_real, vol_ano_ant)})" if vol_ano_ant else "",
        "",
        "** Custo FP — Visão Waterfall (kBRL + R$/veíc) **",
        f"Budget: {_fmt_k(fp_bud)} ({_fmt_cpu(cpu_bud_t)})",
        f"Efeito Flex Volume (barra amarela): {s_ev}{_fmt_k(efeito_vol)} ({s_cev}{_fmt_cpu(cpu_ev)})",
        f"Flex Budget: {_fmt_k(fp_flex)} ({_fmt_cpu(cpu_flex_t)})",
        f"Efeito Operacional (Real vs Flex): {s_eo}{_fmt_k(efeito_op)} ({s_ceo}{_fmt_cpu(cpu_eo)}), {_pct(fp_real, fp_flex)}",
        f"Real: {_fmt_k(fp_real)} ({_fmt_cpu(cpu_real_t)})",
        "",
        f"Delta Total (Real vs Budget): {s_dt}{_fmt_k(delta_bud)} ({s_cdt}{_fmt_cpu(cpu_dt)}), {_pct(fp_real, fp_bud)}",
        f"Delta vs Mês Anterior: {_var_k(fp_real, fp_ant)} ({s_cda}{_fmt_cpu(cpu_da)}), {_pct(fp_real, fp_ant)}",
        "",
    ]

    # Top modelos por impacto de volume
    vm = variacoes["variacao_modelos"]
    if vm:
        modelos_bud = sorted(vm.items(), key=lambda x: abs(x[1]["var_budget"]), reverse=True)[:5]
        lines.append("** Top 5 Modelos — Variação de Volume vs Budget **")
        for modelo, info in modelos_bud:
            delta = info["var_budget"]
            s = "+" if delta >= 0 else ""
            cor = "🟢" if delta > 0 else ("🔴" if delta < 0 else "⚪")
            lines.append(
                f"- {cor} {modelo}: {_fmt(info['vol_real'], 0)} un. "
                f"(Δ {s}{_fmt(delta, 0)} un., {_fmt_pct_modelo(info['pct_budget'])})"
            )
        lines.append("")

    # CPU por modelo (top modelos)
    cpu_modelos_real = variacoes["cpu_modelos"].get("real", {})
    cpu_modelos_bud = variacoes["cpu_modelos"].get("budget", {})
    if cpu_modelos_real:
        cpu_diffs = []
        for modelo in cpu_modelos_real:
            r = cpu_modelos_real[modelo]
            b = cpu_modelos_bud.get(modelo, 0)
            cpu_diffs.append((modelo, r, b, r - b))
        cpu_diffs.sort(key=lambda x: abs(x[3]), reverse=True)
        lines.append("** Top 5 Modelos — CPU vs Budget (R$/veíc) **")
        for modelo, r, b, d in cpu_diffs[:5]:
            s = "+" if d >= 0 else ""
            cor = "🔴" if d > 0 else ("🟢" if d < 0 else "⚪")
            lines.append(
                f"- {cor} {modelo}: {_fmt_cpu(r)} | Δ {s}{_fmt_cpu(d)}, {_pct(r, b)}"
            )
        lines.append("")

    # Top impactos por Type 05 (Real vs Flex)
    df_real = dados.get("custo_real")
    df_bud = dados.get("custo_bud")
    if df_real is not None and not df_real.empty:
        col_t05 = "Type 05" if "Type 05" in df_real.columns else None
        if col_t05:
            try:
                df_flex_det = calcular_flex_budget_detalhado(
                    dados["_custo_bud_full"],
                    dados["_vol_bud_full"],
                    dados["_vol_actual_full"],
                    col_custo="Custo FP",
                )
                mes_nome = dados["mes_nome"]
                col_per = "Período" if "Período" in df_flex_det.columns else "Periodo"
                df_f = df_flex_det[df_flex_det[col_per] == mes_nome]
                col_ref = "Flex_Bud"
            except Exception:
                df_f = df_bud
                col_ref = "Custo FP"

            if df_f is not None and not df_f.empty:
                r_grp = df_real.groupby(col_t05)["Custo FP"].sum()
                f_grp = df_f.groupby(col_t05)[col_ref].sum() if df_f is not None else pd.Series(dtype=float)
                delta_grp = (r_grp - f_grp.reindex(r_grp.index, fill_value=0)).sort_values()
                # CPU por Type 05 (R$/veíc)
                cpu_r_grp = r_grp / vol_real if vol_real > 0 else r_grp * 0
                cpu_f_grp = f_grp / vol_real if vol_real > 0 else f_grp * 0
                cpu_delta_grp = cpu_r_grp - cpu_f_grp.reindex(cpu_r_grp.index, fill_value=0)

                lines.append("** Impacto por Type 05 (Real vs Flex) **")
                for t05 in delta_grp.index:
                    val = delta_grp[t05]
                    cpu_val = cpu_delta_grp.get(t05, 0)
                    s = "+" if val >= 0 else ""
                    s_c = "+" if cpu_val >= 0 else ""
                    cor = "🔴" if val > 0 else "🟢"
                    real_t05 = r_grp.get(t05, 0)
                    cpu_real_t05 = cpu_r_grp.get(t05, 0)
                    lines.append(
                        f"- {cor} {t05}: {_fmt_k(real_t05)} ({_fmt_cpu(cpu_real_t05)}) "
                        f"| Δ {s}{_fmt_k(val)} ({s_c}{_fmt_cpu(cpu_val)})"
                    )
                lines.append("")

    # Top oficinas com maior delta + top Type 06 driver
    oficinas = descobrir_oficinas(dados)
    if oficinas:
        ofc_deltas = []
        for ofc in oficinas:
            dados_ofc = _filtrar_por_oficina(dados, ofc)
            df_r_ofc = dados_ofc.get("custo_real")
            df_b_ofc = dados_ofc.get("custo_bud")
            fp_ofc = _safe_sum(df_r_ofc, "Custo FP")
            fp_bud_ofc = _safe_sum(df_b_ofc, "Custo FP")
            # Descobrir top Type 06 driver
            top_t06_info = ""
            try:
                if df_r_ofc is not None and "Type 06" in df_r_ofc.columns:
                    # Calcular flex para oficina
                    try:
                        df_flex_ofc = calcular_flex_budget_detalhado(
                            dados["_custo_bud_full"],
                            dados["_vol_bud_full"],
                            dados["_vol_actual_full"],
                            col_custo="Custo FP",
                        )
                        mes_n = dados["mes_nome"]
                        col_p = "Período" if "Período" in df_flex_ofc.columns else "Periodo"
                        df_flex_ofc = df_flex_ofc[df_flex_ofc[col_p] == mes_n]
                        if ofc in df_flex_ofc.get("Oficina", pd.Series()).values:
                            df_flex_ofc = df_flex_ofc[df_flex_ofc["Oficina"] == ofc]
                        col_fb = "Flex_Bud"
                    except Exception:
                        df_flex_ofc = df_b_ofc
                        col_fb = "Custo FP"

                    if df_flex_ofc is not None and not df_flex_ofc.empty and "Type 06" in df_flex_ofc.columns:
                        r06 = df_r_ofc.groupby("Type 06")["Custo FP"].sum()
                        f06 = df_flex_ofc.groupby("Type 06")[col_fb].sum()
                        d06 = (r06 - f06.reindex(r06.index, fill_value=0)).sort_values(key=abs, ascending=False)
                        if not d06.empty:
                            top_name = d06.index[0]
                            top_val = d06.iloc[0]
                            s_t = "+" if top_val >= 0 else ""
                            cpu_t06 = _cpu(top_val, vol_real)
                            s_ct = "+" if cpu_t06 >= 0 else ""
                            top_t06_info = f" (top driver: {top_name} {s_t}{_fmt_k(top_val)}, {s_ct}{_fmt_cpu(cpu_t06)})"
            except Exception:
                pass
            ofc_deltas.append((ofc, fp_ofc - fp_bud_ofc, fp_ofc, top_t06_info))
        ofc_deltas.sort(key=lambda x: abs(x[1]), reverse=True)
        lines.append("** Oficinas — maiores desvios vs Budget **")
        for ofc, delta, total, t06_info in ofc_deltas[:5]:
            s = "+" if delta >= 0 else ""
            cor = "🔴" if delta > 0 else "🟢"
            cpu_ofc = _cpu(total, vol_real)
            cpu_d_ofc = _cpu(delta, vol_real)
            s_c = "+" if cpu_d_ofc >= 0 else ""
            lines.append(
                f"- {cor} {ofc}: {_fmt_k(total)} ({_fmt_cpu(cpu_ofc)}) "
                f"| Δ {s}{_fmt_k(delta)} ({s_c}{_fmt_cpu(cpu_d_ofc)}){t06_info}"
            )
        lines.append("")

    # Flags de referência
    if variacoes.get("sem_budget"):
        lines.append("⚠️ Budget não disponível — comparações vs Budget são nulas.")
    if variacoes.get("sem_mes_anterior"):
        lines.append("⚠️ Mês anterior não disponível (primeiro mês do ano).")
    if variacoes.get("sem_ano_anterior"):
        lines.append("⚠️ Ano anterior sem dados para este mês.")

    return "\n".join(lines)


def formatar_resumo_mes(dados: dict, variacoes: dict) -> str:
    """Formata resumo geral do mês para observações finais."""
    v = variacoes["volume"]
    fp = variacoes["custo_fp"]

    return (
        f"Mês: {dados['mes_nome']}/{dados['ano']}\n"
        f"Volume Real: {_fmt(v['real'], 0)} un. "
        f"(vs mês anterior: {_pct(v['real'], v['mes_anterior'])}, "
        f"vs budget: {_pct(v['real'], v['budget'])})\n"
        f"Custo FP Real: {_fmt_k(fp['real'])} "
        f"(vs flex: {_pct(fp['real'], fp.get('flex', 0))}, "
        f"vs mês anterior: {_pct(fp['real'], fp.get('mes_anterior', 0))})\n"
    )


# ═══════════════════════════════════════════════════════════════
#  DOCUMENTAÇÃO DO SISTEMA (para chatbot)
# ═══════════════════════════════════════════════════════════════

def carregar_documentacao_sistema() -> str:
    """
    Lê os arquivos DOCUMENTACAO*.md e retorna texto concatenado.
    Usado SOMENTE no chatbot — nunca no relatório.
    """
    arquivos = [
        "DOCUMENTACAO_TC_PRINCIPAL.md",
        "DOCUMENTACAO_SISTEMA_TC.md",
        "DOCUMENTACAO_FLEX_BUD_ANO_COMPLETO.md",
    ]
    partes = []
    for arq in arquivos:
        caminho = os.path.join(_ROOT, arq)
        if os.path.exists(caminho):
            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    conteudo = f.read()
                partes.append(f"--- {arq} ---\n{conteudo}")
            except Exception as e:
                logger.warning("Erro ao ler %s: %s", arq, e)

    # Carregar dados da equipe (JSON)
    equipe_path = os.path.join(_ROOT, "dados_equipe.json")
    if os.path.exists(equipe_path):
        try:
            import json
            with open(equipe_path, "r", encoding="utf-8") as f:
                equipe = json.load(f)
            linhas_equipe = ["--- EQUIPE DO PROJETO ---"]
            for nome, info in equipe.items():
                cargo = info.get("cargo", "")
                empresa = info.get("empresa", "")
                exp = info.get("experiencia", "")
                linkedin = info.get("linkedin", "")
                linhas_equipe.append(
                    f"\u2022 {nome}: {cargo} | {empresa} | {exp} | LinkedIn: {linkedin}"
                )
            partes.append("\n".join(linhas_equipe))
        except Exception as e:
            logger.warning("Erro ao ler dados_equipe.json: %s", e)

    return "\n\n".join(partes) if partes else ""


# ═══════════════════════════════════════════════════════════════
#  DESCOBRIR MESES DISPONÍVEIS
# ═══════════════════════════════════════════════════════════════

def descobrir_meses_disponiveis(ano: int) -> list[int]:
    """
    Retorna lista de números de meses que possuem dados Real
    processados para o ano especificado.
    """
    vol = carregar_volume_real(ano)
    if vol is None or vol.empty or "Período" not in vol.columns:
        return []

    meses_com_dados = []
    for num, nome in MESES_NUMERO.items():
        if nome in vol["Período"].values:
            total = vol[vol["Período"] == nome]["Volume"].sum()
            if total > 0:
                meses_com_dados.append(num)

    return sorted(meses_com_dados)


def descobrir_anos_disponiveis() -> list[int]:
    """Retorna lista de anos que possuem dados processados."""
    pasta_base = os.path.join(_ROOT, "dados", "TC_Principal")
    if not os.path.isdir(pasta_base):
        return []
    anos = []
    for d in sorted(os.listdir(pasta_base), reverse=True):
        pasta = os.path.join(pasta_base, d)
        if os.path.isdir(pasta) and d.isdigit():
            # Verificar se tem parquet Real
            if any(f.endswith(".parquet") for f in os.listdir(pasta)):
                anos.append(int(d))
    return anos
