from __future__ import annotations

"""Stable public API for Streamlit pages.

Goal: pages should import from here, not from app.py.
That avoids executing the whole dashboard (and its CSS/UI side-effects) just to reuse helpers.

This file is a compatibility layer: keep function names/signatures stable.
"""

import os
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

from tc_core.data.periodos import normalizar_coluna_periodo
from tc_core.data.schema import normalize_common_column_mojibake
from tc_core.data.paths import encontrar_arquivo_parquet, listar_anos_disponiveis
from tc_core.finance.currency import converter_coluna_moeda, converter_moeda, obter_simbolo_moeda
from tc_core.finance.currency_db import carregar_taxas_banco, inicializar_banco_taxas, salvar_taxas_banco


def _otimizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    # Converter colunas numéricas conhecidas para numérico ANTES da otimização
    colunas_numericas = ["Valor", "Total", "Volume", "CPU"]
    for col in colunas_numericas:
        if col in df.columns and df[col].dtype == "object":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Otimizar tipos de dados
    for col in df.columns:
        if df[col].dtype == "object":
            unique_ratio = df[col].nunique() / max(len(df), 1)
            if unique_ratio < 0.5:
                df[col] = df[col].astype("category")

    # Converter floats para tipos menores
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    # Converter ints para tipos menores
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    return df


@st.cache_data(ttl=3600, max_entries=10, show_spinner=True)
def load_data(ano_selecionado_param):
    """Carrega df_final do histórico consolidado e filtra por ano quando aplicável."""
    caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
    caminho_absoluto = os.path.abspath(caminho_historico)

    if not os.path.exists(caminho_historico):
        st.error(f"❌ Arquivo de histórico consolidado não encontrado: {caminho_absoluto}")
        st.info("💡 Execute tc_ext/notebooks/dados.ipynb para gerar o histórico consolidado")
        st.stop()

    df = pd.read_parquet(caminho_historico)
    df = normalize_common_column_mojibake(df)
    df = normalizar_coluna_periodo(df, "Período")

    if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
        try:
            df = df[df["Ano"] == int(ano_selecionado_param)].copy()
        except (ValueError, TypeError):
            pass

    df = _otimizar_tipos(df)
    return df


@st.cache_data(ttl=60, max_entries=10, show_spinner=True)
def load_volume_data(ano_selecionado_param):
    """Carrega df_vol do histórico consolidado e filtra por ano quando aplicável."""
    caminho_historico = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")

    if not os.path.exists(caminho_historico):
        return None

    df = pd.read_parquet(caminho_historico)
    df = normalize_common_column_mojibake(df)

    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("float64")

    if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
        try:
            df = df[df["Ano"] == int(ano_selecionado_param)].copy()
        except (ValueError, TypeError):
            pass

    df = normalizar_coluna_periodo(df, "Período")
    df = _otimizar_tipos(df)
    return df


@st.cache_data(ttl=3600, max_entries=10, show_spinner=True)
def load_budget_data(ano_selecionado_param):
    """Carrega df_final do histórico consolidado BUD e filtra por ano quando aplicável."""
    caminho_budget = os.path.join(
        "dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"
    )
    if not os.path.exists(caminho_budget):
        return None

    df = pd.read_parquet(caminho_budget)
    df = normalize_common_column_mojibake(df)

    if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
        try:
            df = df[df["Ano"] == int(ano_selecionado_param)].copy()
        except (ValueError, TypeError):
            pass

    df = normalizar_coluna_periodo(df, "Período")
    df = _otimizar_tipos(df)
    return df


@st.cache_data(ttl=3600, max_entries=10, show_spinner=True)
def load_budget_volume_data(ano_selecionado_param):
    """Carrega df_vol do histórico consolidado BUD e filtra por ano quando aplicável."""
    caminho_budget_vol = os.path.join(
        "dados", "historico_consolidado", "BUD", "df_vol_historico_BUD.parquet"
    )
    if not os.path.exists(caminho_budget_vol):
        return None

    df = pd.read_parquet(caminho_budget_vol)
    df = normalize_common_column_mojibake(df)

    # Governança: Volume BUD *precisa* ter Veículo; ausência indica erro na extração.
    if "Veículo" not in df.columns:
        st.error(
            "❌ ERRO NA EXTRAÇÃO: o arquivo de volume do Budget não contém a coluna 'Veículo'."
        )
        st.info(
            "💡 Refaça a extração do BUDGET (página 'Extração de Dados') e corrija a aba 'Volume BDG' "
            "no Excel para incluir 'Veículo'."
        )
        st.stop()

    if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
        try:
            df = df[df["Ano"] == int(ano_selecionado_param)].copy()
        except (ValueError, TypeError):
            pass

    df = normalizar_coluna_periodo(df, "Período")

    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    df = _otimizar_tipos(df)
    return df


def formatar_ratio_com_barra(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com barra de progresso em HTML."""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        percentual = valor * 100

    largura_barra = 100 if percentual >= 100 else percentual

    if percentual <= 90:
        r, g, b = 0, 170, 0
    elif percentual >= 100:
        r, g, b = 255, 0, 0
    else:
        progresso = (percentual - 90) / 10
        r = int(255 * progresso)
        g = int(170 * (1 - progresso))
        b = 0

    cor = f"rgb({r}, {g}, {b})"

    try:
        theme_base = st.get_option("theme.base") or "light"
        texto_cor = "#FAFAFA" if theme_base == "dark" else "#31333F"
    except Exception:
        texto_cor = "var(--text-color, #31333F)"

    return f"""
    <div style=\"display: flex; align-items: center; gap: 5px; width: 100%; justify-content: flex-start; margin: 0; padding: 0; vertical-align: middle;\">
        <div style=\"width: 64px; background-color: #333; border-radius: 3px; height: 11px; position: relative; overflow: hidden; flex-shrink: 0; margin: 0;\">
            <div style=\"width: {largura_barra}%; height: 100%; background-color: {cor}; transition: width 0.3s;\"></div>
        </div>
        <span style=\"width: 65px; text-align: left; font-weight: normal; color: {texto_cor}; font-size: 0.75rem; flex-shrink: 0; line-height: 1.2; margin: 0;\">{percentual:.0f}%</span>
    </div>
    """


def criar_tabela_html_com_barra(df_display, linha_resumo=None, linha_volumes=None):
    """Cria uma tabela HTML customizada no padrão Streamlit para renderizar HTML nas células."""
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            header_bg = "rgba(38, 39, 48, 0.15)"
            resumo_bg = "rgba(38, 39, 48, 0.15)"
            row_bg = "transparent"
            border_color = "rgba(250, 250, 250, 0.1)"
        else:
            header_bg = "rgba(240, 242, 246, 0.15)"
            resumo_bg = "rgba(240, 242, 246, 0.15)"
            row_bg = "transparent"
            border_color = "rgba(49, 51, 63, 0.1)"
    except Exception:
        header_bg = "rgba(38, 39, 48, 0.15)"
        resumo_bg = "rgba(38, 39, 48, 0.15)"
        row_bg = "transparent"
        border_color = "rgba(250, 250, 250, 0.1)"

    html_table = """
    <div class='stDataFrame' style='overflow-x: auto; margin: 1rem 0;'>
        <style>
            .flex-bud-table {
                width: 100%;
                border-collapse: collapse;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            }
            .flex-bud-table thead tr {
                background-color: """ + header_bg + """;
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table th {
                padding: 0.75rem 1rem;
                text-align: left;
                font-weight: 600;
                font-size: 0.75rem;
                color: inherit;
            }
            .flex-bud-table tbody tr {
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table tbody tr:last-child {
                border-bottom: none;
            }
            .flex-bud-table .resumo-row {
                border-top: 2px solid """ + border_color + """;
            }
            .flex-bud-table td {
                padding: 0.75rem 1rem;
                font-size: 0.75rem;
                vertical-align: middle;
                font-weight: normal;
            }
            .flex-bud-table .resumo-row {
                background-color: """ + resumo_bg + """;
                font-weight: 600;
            }
            .flex-bud-table .number-cell {
                text-align: right;
                font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
                font-variant-numeric: tabular-nums;
                font-size: 0.7rem;
                font-weight: normal;
            }
            .flex-bud-table .total-flex-bud-col {
                max-width: 140px;
                width: 140px;
                white-space: nowrap;
            }
        </style>
        <table class='flex-bud-table'>
    """

    html_table += "<thead><tr>"
    for col in df_display.columns:
        if col == "Total / Flex Bud":
            html_table += f"<th class='total-flex-bud-col'>{col}</th>"
        else:
            html_table += f"<th>{col}</th>"
    html_table += "</tr></thead><tbody>"

    for _, row in df_display.iterrows():
        html_table += f"<tr style='background-color: {row_bg};'>"
        for col in df_display.columns:
            if col == "Total / Flex Bud":
                valor_celula = row[col]
                if isinstance(valor_celula, str) and "<div" in valor_celula:
                    html_table += f"<td class='total-flex-bud-col'>{valor_celula}</td>"
                else:
                    valor_num = (
                        float(valor_celula)
                        if pd.notna(valor_celula) and isinstance(valor_celula, (int, float))
                        else 0
                    )
                    html_formatado = formatar_ratio_com_barra(valor_num)
                    html_table += f"<td class='total-flex-bud-col'>{html_formatado}</td>"
            else:
                valor_celula = str(row[col])
                if any(
                    char.isdigit() or char in ["$", "€", "R$", ",", ".", "K", "M"]
                    for char in valor_celula
                ):
                    html_table += f"<td class='number-cell'>{valor_celula}</td>"
                else:
                    html_table += f"<td>{valor_celula}</td>"
        html_table += "</tr>"

    if linha_volumes:
        html_table += f"<tr class='resumo-row' style='background-color: {resumo_bg}; border-top: 2px solid {border_color};'>"
        for col in df_display.columns:
            valor_volume = linha_volumes.get(col, "-")
            html_table += f"<td class='number-cell' style='font-weight: 600;'>{valor_volume}</td>"
        html_table += "</tr>"

    html_table += "</tbody></table></div>"
    return html_table


def formatar_periodo_abreviado(periodo_str, ano=None, usar_ano_completo=False):
    meses_abrev = {
        "janeiro": "Jan",
        "fevereiro": "Fev",
        "março": "Mar",
        "abril": "Abr",
        "maio": "Mai",
        "junho": "Jun",
        "julho": "Jul",
        "agosto": "Ago",
        "setembro": "Set",
        "outubro": "Out",
        "novembro": "Nov",
        "dezembro": "Dez",
    }

    periodo_str = str(periodo_str).strip()
    mes_abrev = None
    ano_extraido = None

    if periodo_str.startswith("Total "):
        partes = periodo_str.split(" ", 1)
        if len(partes) > 1:
            ano_str = partes[1].strip()
            if ano_str.isdigit():
                return f"Total/{ano_str}"
        return "Total"
    elif " S" in periodo_str:
        partes = periodo_str.split(" S")
        if len(partes) == 2:
            ano_str = partes[0].strip()
            semestre = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{semestre}"
        return periodo_str
    elif " Q" in periodo_str:
        partes = periodo_str.split(" Q")
        if len(partes) == 2:
            ano_str = partes[0].strip()
            quarter = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{quarter}"
        return periodo_str
    else:
        if " " in periodo_str:
            partes = periodo_str.split(" ", 1)
            mes_nome = partes[0].lower().strip()
            if len(partes) > 1:
                ano_str = partes[1].strip()
                if ano_str.isdigit():
                    ano_extraido = int(ano_str)
                elif any(c.isdigit() for c in ano_str):
                    numero_str = "".join([c for c in ano_str if c.isdigit()])[:4]
                    if numero_str:
                        ano_extraido = int(numero_str)

            mes_abrev = meses_abrev.get(
                mes_nome,
                mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize(),
            )
        else:
            mes_nome = periodo_str.lower().strip()
            mes_abrev = meses_abrev.get(
                mes_nome,
                mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize(),
            )

    if ano is not None:
        ano_final = ano
    elif ano_extraido is not None:
        ano_final = ano_extraido
    else:
        ano_final = None

    if mes_abrev:
        if ano_final:
            ano_abrev = str(ano_final)[-2:]
            return f"{mes_abrev}/{ano_abrev}"
        return mes_abrev

    return periodo_str


def reordenar_colunas_padrao(colunas_numericas):
    ordem_colunas = [
        "BUD",
        "Flex Bud - BUD",
        "Flex BUD",
        "Total - Flex Bud",
        "Total",
        "Total / Flex Bud",
    ]
    colunas_ordenadas = [c for c in ordem_colunas if c in colunas_numericas]
    for col in colunas_numericas:
        if col not in colunas_ordenadas:
            colunas_ordenadas.append(col)
    return colunas_ordenadas


def calcular_resumo_tabela_flex(df_original, tipo_visualizacao, moeda_simbolo, fator_conversao=None):
    """Mesma lógica do app atual: calcula totalizadores para a tabela Flex Bud."""
    linha_resumo: Dict[str, Any] = {}
    linha_resumo_formatado: Dict[str, Any] = {}

    primeira_col = df_original.columns[0]
    linha_resumo[primeira_col] = "**TOTAL**"
    linha_resumo_formatado[primeira_col] = "**TOTAL**"

    if tipo_visualizacao == "CPU (Custo por Unidade)":
        if (
            "_Flex_Bud_Total" in df_original.columns
            and "_Total_Custo_Total" in df_original.columns
            and "_Volume_Real" in df_original.columns
        ):
            flex_bud_total_custo = df_original["_Flex_Bud_Total"].sum()
            total_custo_total = df_original["_Total_Custo_Total"].sum()

            volumes_reais = df_original["_Volume_Real"].dropna()
            volume_total_real = float(volumes_reais.iloc[0]) if len(volumes_reais) > 0 else 0.0

            flex_bud_cpu = (
                flex_bud_total_custo / volume_total_real
                if volume_total_real != 0 and pd.notnull(volume_total_real)
                else 0
            )
            total_cpu = (
                total_custo_total / volume_total_real
                if volume_total_real != 0 and pd.notnull(volume_total_real)
                else 0
            )

            volume_total_budget = 0
            if "_Budget_Total" in df_original.columns and "_Volume_Budget" in df_original.columns:
                budget_total_custo = df_original["_Budget_Total"].sum()
                volumes_budget = df_original["_Volume_Budget"].dropna()
                volume_total_budget = (
                    float(volumes_budget.iloc[0]) if len(volumes_budget) > 0 else 0.0
                )
                bud_cpu = (
                    budget_total_custo / volume_total_budget
                    if volume_total_budget != 0 and pd.notnull(volume_total_budget)
                    else 0
                )
            else:
                bud_cpu = df_original["BUD"].sum() if "BUD" in df_original.columns else 0

            linha_resumo["Flex BUD"] = flex_bud_cpu
            linha_resumo["Total"] = total_cpu
            linha_resumo["BUD"] = bud_cpu
            linha_resumo["Flex Bud - BUD"] = flex_bud_cpu - bud_cpu
            linha_resumo["Total - Flex Bud"] = total_cpu - flex_bud_cpu

            linha_resumo["_Volume_Real_Calculo"] = volume_total_real
            linha_resumo["_Volume_Budget_Calculo"] = volume_total_budget

            linha_resumo_formatado["Flex BUD"] = f"{flex_bud_cpu:,.2f}"
            linha_resumo_formatado["Total"] = f"{total_cpu:,.2f}"
            linha_resumo_formatado["BUD"] = f"{bud_cpu:,.2f}"
            linha_resumo_formatado["Flex Bud - BUD"] = f"{flex_bud_cpu - bud_cpu:,.2f}"
            linha_resumo_formatado["Total - Flex Bud"] = f"{total_cpu - flex_bud_cpu:,.2f}"
            linha_resumo_formatado["_Volume_Real_Calculo"] = f"{volume_total_real:,.0f}"
            linha_resumo_formatado["_Volume_Budget_Calculo"] = f"{volume_total_budget:,.0f}"
        else:
            for col in ["BUD", "Flex Bud - BUD", "Flex BUD", "Total - Flex Bud", "Total"]:
                if col in df_original.columns:
                    soma = df_original[col].sum()
                    linha_resumo[col] = soma
                    linha_resumo_formatado[col] = f"{soma:,.2f}"

            if "_Volume_Real" in df_original.columns:
                volumes_reais = df_original["_Volume_Real"].dropna()
                volume_total_real = float(volumes_reais.iloc[0]) if len(volumes_reais) > 0 else 0.0
            else:
                volume_total_real = 0.0

            if "_Volume_Budget" in df_original.columns:
                volumes_budget = df_original["_Volume_Budget"].dropna()
                volume_total_budget = (
                    float(volumes_budget.iloc[0]) if len(volumes_budget) > 0 else 0.0
                )
            else:
                volume_total_budget = 0.0

            linha_resumo["_Volume_Real_Calculo"] = volume_total_real
            linha_resumo["_Volume_Budget_Calculo"] = volume_total_budget
            linha_resumo_formatado["_Volume_Real_Calculo"] = f"{volume_total_real:,.0f}"
            linha_resumo_formatado["_Volume_Budget_Calculo"] = f"{volume_total_budget:,.0f}"
    else:
        for col in ["BUD", "Flex Bud - BUD", "Flex BUD", "Total - Flex Bud", "Total"]:
            if col in df_original.columns:
                soma = df_original[col].sum()
                linha_resumo[col] = soma
                sufixo = ""
                if fator_conversao == "K (milhares)":
                    sufixo = " K"
                elif fator_conversao == "M (Milhões)":
                    sufixo = " M"
                linha_resumo_formatado[col] = f"{soma:,.2f}{sufixo}"

    if "Total" in linha_resumo and "Flex BUD" in linha_resumo:
        total_soma = linha_resumo["Total"]
        flex_bud_soma = linha_resumo["Flex BUD"]
        ratio_resumo = (
            total_soma / flex_bud_soma
            if flex_bud_soma != 0 and pd.notnull(flex_bud_soma)
            else 0
        )
        linha_resumo["Total / Flex Bud"] = ratio_resumo
        linha_resumo_formatado["Total / Flex Bud"] = ratio_resumo

    return linha_resumo, linha_resumo_formatado


def exibir_caixas_resumo_dinamico(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    colunas_auxiliares = ["_Volume_Real_Calculo", "_Volume_Budget_Calculo"]
    colunas_numericas = [col for col in linha_resumo.keys() if col not in colunas_auxiliares]

    primeiro_periodo = None
    segundo_periodo_maiuscula = None
    segundo_periodo_minuscula = None
    flex_primeiro_menos_primeiro = None
    flex_primeiro = None
    percentual = None

    for col in colunas_numericas:
        if (
            not col.startswith("%")
            and not col.startswith("Flex")
            and "-" not in col
            and len(col) > 0
            and col[0].isupper()
        ):
            primeiro_periodo = col
        elif col.startswith("Flex") and "-" in col:
            flex_primeiro_menos_primeiro = col
        elif col.startswith("Flex") and "-" not in col:
            flex_primeiro = col
        elif (
            "-" in col
            and not col.startswith("%")
            and not col.startswith("Flex")
            and len(col) > 0
            and col[0].isupper()
        ):
            segundo_periodo_maiuscula = col
        elif (
            not col.startswith("%")
            and not col.startswith("Flex")
            and "-" not in col
            and len(col) > 0
            and col[0].islower()
        ):
            segundo_periodo_minuscula = col
        elif col.startswith("%"):
            percentual = col

    ordem_explicita = []
    if primeiro_periodo:
        ordem_explicita.append(primeiro_periodo)
    if flex_primeiro_menos_primeiro:
        ordem_explicita.append(flex_primeiro_menos_primeiro)
    if flex_primeiro:
        ordem_explicita.append(flex_primeiro)
    if segundo_periodo_maiuscula:
        ordem_explicita.append(segundo_periodo_maiuscula)
    if segundo_periodo_minuscula:
        ordem_explicita.append(segundo_periodo_minuscula)
    if percentual:
        ordem_explicita.append(percentual)

    colunas_restantes = [col for col in colunas_numericas if col not in ordem_explicita]
    colunas_ordenadas = ordem_explicita + colunas_restantes

    num_colunas = min(len(colunas_ordenadas), 6)
    if num_colunas > 0:
        cols = st.columns(num_colunas, gap="small")
        for idx, col_nome in enumerate(colunas_ordenadas[:num_colunas]):
            with cols[idx]:
                valor_formatado = linha_resumo_formatado.get(col_nome, "-")
                st.markdown(
                    f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>",
                    unsafe_allow_html=True,
                )

    if mostrar_volumes:
        volume_real_display = linha_resumo_formatado.get("_Volume_Real_Calculo", "-")
        volume_budget_display = linha_resumo_formatado.get("_Volume_Budget_Calculo", "-")

        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>",
                unsafe_allow_html=True,
            )
        with col_vol2:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>",
                unsafe_allow_html=True,
            )


def exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    if mostrar_volumes:
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")

        with col1:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col4:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col5:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Total', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col6:
            ratio_valor = linha_resumo.get("Total / Flex Bud", 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(
                    f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>",
                    unsafe_allow_html=True,
                )

        st.markdown("<br>", unsafe_allow_html=True)

        volume_real_display = linha_resumo_formatado.get("_Volume_Real_Calculo", None)
        volume_budget_display = linha_resumo_formatado.get("_Volume_Budget_Calculo", None)

        if volume_real_display is None or volume_real_display == "-":
            if "_Volume_Real_Calculo" in linha_resumo:
                v = linha_resumo["_Volume_Real_Calculo"]
                volume_real_display = f"{v:,.0f}" if isinstance(v, (int, float)) and not pd.isna(v) and v != 0 else "-"
            else:
                volume_real_display = "-"

        if volume_budget_display is None or volume_budget_display == "-":
            if "_Volume_Budget_Calculo" in linha_resumo:
                v = linha_resumo["_Volume_Budget_Calculo"]
                volume_budget_display = f"{v:,.0f}" if isinstance(v, (int, float)) and not pd.isna(v) and v != 0 else "-"
            else:
                volume_budget_display = "-"

        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>",
                unsafe_allow_html=True,
            )
        with col_vol2:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>",
                unsafe_allow_html=True,
            )
    else:
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")

        with col1:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col4:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col5:
            st.markdown(
                f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Total', '-')}</div>",
                unsafe_allow_html=True,
            )
        with col6:
            ratio_valor = linha_resumo.get("Total / Flex Bud", 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(
                    f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>",
                    unsafe_allow_html=True,
                )
