"""
TC Principal — Componentes de UI
Header, sidebar global, CSS, seletores e tabelas HTML padronizados.
Replica o padrão visual do TC Ext.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import os
import json
import toml
from datetime import datetime

from tc_core.constants import ORDEM_MESES
from tc_core.finance.currency import converter_moeda, obter_simbolo_moeda
from tc_core.finance.currency_db import (
    carregar_taxas_banco,
    inicializar_banco_taxas,
    salvar_taxas_banco,
)
from tc_principal.shared import descobrir_anos_tc_principal, obter_timestamp_parquets


# ═══════════════════════════════════════════════════════════════
#  CSS GLOBAL  (replica home_ext.py L264-L615)
# ═══════════════════════════════════════════════════════════════

def injetar_css_global():
    """Injeta CSS global padronizado (idêntico ao TC Ext)."""
    st.markdown("""
    <style>
    /* ── Títulos reduzidos 20% ── */
    h1 { font-size: 2.4rem !important; }
    h2 { font-size: 1.6rem !important; }
    h3 { font-size: 1.28rem !important; }

    /* ── Botões compactos ── */
    .stButton > button {
        font-size: 0.85rem !important;
        padding: 0.3rem 0.8rem !important;
    }

    /* ── Radio buttons compactos ── */
    div[data-testid="stRadio"] label {
        font-size: 0.8rem !important;
        white-space: nowrap !important;
    }
    div[data-testid="stRadio"] > div {
        gap: 0.25rem !important;
    }
    div[data-testid="stRadio"] > div[role="radiogroup"] {
        gap: 0.25rem !important;
    }

    /* ── Colunas com padding reduzido ── */
    div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
        padding: 0.2rem !important;
    }

    /* ── Tabelas nativas ── */
    table td, table th {
        vertical-align: middle !important;
    }

    /* ── Inputs menores na sidebar ── */
    .sidebar .stNumberInput label,
    section[data-testid="stSidebar"] .stNumberInput label {
        font-size: 0.7rem !important;
        white-space: nowrap !important;
    }
    .sidebar .stNumberInput input,
    section[data-testid="stSidebar"] .stNumberInput input {
        font-size: 0.75rem !important;
    }

    /* ── Selectbox/multiselect menores na sidebar ── */
    section[data-testid="stSidebar"] .stMultiSelect label,
    section[data-testid="stSidebar"] .stSelectbox label {
        font-size: 0.8rem !important;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab"] {
        font-size: 0.85rem !important;
        padding: 0.5rem 1rem !important;
    }

    /* ── Métricas compactas ── */
    div[data-testid="stMetric"] {
        border: 1px solid rgba(250, 250, 250, 0.1);
        border-radius: 8px;
        padding: 12px 16px;
    }
    div[data-testid="stMetric"] label {
        font-size: 0.75rem !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.3rem !important;
    }

    /* ── KPI Cards (padrão TC Ext) ── */
    .tc-kpi-card {
        padding: 0.6rem 0.8rem;
        border: 1px solid rgba(49, 51, 63, 0.15);
        border-radius: 8px;
        background: rgba(0, 0, 0, 0.02);
    }
    .tc-kpi-label {
        opacity: 0.75;
        font-size: 0.75rem;
    }
    .tc-kpi-value {
        font-size: 1.1em;
        font-weight: 600;
    }
    .tc-kpi-spacer {
        display: block;
        height: 1.75rem;
    }
    </style>
    """, unsafe_allow_html=True)


def render_kpi(label: str, value: str) -> None:
    """
    Renderiza um KPI card no padrão TC Ext.

    Args:
        label: Título do KPI (ex: 'BUD', 'Flex Bud')
        value: Valor formatado (ex: 'R$ 1.234,56')
    """
    st.markdown(
        f"""
        <div class="tc-kpi-card">
            <div class="tc-kpi-label">{label}</div>
            <div class="tc-kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_spacer() -> None:
    """Renderiza espaçador após linha de KPIs."""
    st.markdown("<div class='tc-kpi-spacer'></div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
#  HEADER / BANNER
# ═══════════════════════════════════════════════════════════════

def render_header():
    """Renderiza banner superior no padrão TC Ext (gradiente roxo)."""
    # Versão
    versao_str = "—"
    try:
        with open('versao.json', 'r', encoding='utf-8') as f:
            v = json.load(f)
            versao_str = v.get('versao', '—')
    except Exception:
        pass

    # Mês/ano atual
    agora = datetime.now()
    meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
    }
    mes_ano = f"{meses_pt.get(agora.month, '')} {agora.year}"

    # Data atualização parquets
    data_atualizacao = "—"
    anos = descobrir_anos_tc_principal()
    if anos:
        ts = obter_timestamp_parquets(anos[0])
        if ts:
            dt = datetime.fromtimestamp(ts)
            data_atualizacao = dt.strftime('%d/%m/%Y %H:%M')

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 8px 16px; border-radius: 6px; margin-bottom: 14px;
                border-bottom: 1px solid #5a4fcf;
                display: flex; justify-content: space-between; align-items: center;">
        <div style="color: white; font-size: 0.85rem;">
            <strong>TC Planta Principal</strong> • v{versao_str} • {mes_ano}
        </div>
        <div style="color: rgba(255,255,255,0.8); font-size: 0.75rem;">
            📅 Dados: {data_atualizacao}
        </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
#  TEMA (Dark / Light)
# ═══════════════════════════════════════════════════════════════

def _get_config_path():
    return os.path.join('.streamlit', 'config.toml')


def get_current_theme():
    """Lê o tema atual do config.toml."""
    path = _get_config_path()
    if os.path.exists(path):
        try:
            cfg = toml.load(path)
            bg = cfg.get('theme', {}).get('backgroundColor', '#0E1117')
            return 'light' if bg.upper() in ('#FFFFFF', '#FFF') else 'dark'
        except Exception:
            pass
    return 'dark'


def save_theme_to_config(theme_name):
    """Salva tema no config.toml e recarrega."""
    path = _get_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        cfg = toml.load(path) if os.path.exists(path) else {}
    except Exception:
        cfg = {}

    if theme_name == 'dark':
        cfg['theme'] = {
            'primaryColor': '#FF4B4B',
            'backgroundColor': '#0E1117',
            'secondaryBackgroundColor': '#262730',
            'textColor': '#FAFAFA',
        }
    else:
        cfg['theme'] = {
            'primaryColor': '#FF4B4B',
            'backgroundColor': '#FFFFFF',
            'secondaryBackgroundColor': '#F0F2F6',
            'textColor': '#262730',
        }

    with open(path, 'w') as f:
        toml.dump(cfg, f)


def is_dark_theme():
    """Retorna True se o tema atual é dark."""
    return get_current_theme() == 'dark'


# ═══════════════════════════════════════════════════════════════
#  SIDEBAR GLOBAL
# ═══════════════════════════════════════════════════════════════

def render_sidebar_global(page_key):
    """
    Renderiza elementos globais da sidebar:
    Ano, Moeda (com bandeiras), Taxas, Tipo, Fator, Tema.

    Retorna dict: {ano, moeda, simbolo, taxas, tipo, fator}
    """
    with st.sidebar:
        st.header("⚙️ Configurações")

        # ── Ano ──
        anos = descobrir_anos_tc_principal()
        if not anos:
            st.error("❌ Nenhum dado processado em dados/*/TC_Principal/BUD/")
            st.info("💡 Execute o processamento na página Extração de Dados.")
            st.stop()

        ano = st.selectbox("📅 Ano", anos, index=0, key=f'{page_key}_ano')

        st.divider()

        # ── Moeda com bandeiras ──
        st.markdown("**💱 Moeda**")

        # Bandeiras visuais
        moedas_info = [
            ('BRL', '🇧🇷', 'R$', 'https://flagcdn.com/w40/br.png'),
            ('USD', '🇺🇸', '$', 'https://flagcdn.com/w40/us.png'),
            ('EUR', '🇪🇺', '€', 'https://flagcdn.com/w40/eu.png'),
        ]

        if f'{page_key}_moeda' not in st.session_state:
            st.session_state[f'{page_key}_moeda'] = 'BRL'

        moeda_atual = st.session_state[f'{page_key}_moeda']

        # Bandeiras como botões visuais
        flag_cols = st.columns(3)
        for i, (cod, emoji, simb, url) in enumerate(moedas_info):
            with flag_cols[i]:
                is_selected = moeda_atual == cod
                border = '2px solid #ff4b4b' if is_selected else '2px solid transparent'
                shadow = 'box-shadow: 0 0 6px rgba(255,75,75,0.6);' if is_selected else ''
                st.markdown(f"""
                <div style="text-align: center; cursor: pointer;">
                    <img src="{url}" width="40" height="28"
                         style="border: {border}; border-radius: 4px; {shadow}">
                    <div style="font-size: 0.7rem; margin-top: 2px;">{simb}</div>
                </div>
                """, unsafe_allow_html=True)

        moeda = st.radio(
            "Moeda", ['BRL', 'USD', 'EUR'],
            index=['BRL', 'USD', 'EUR'].index(moeda_atual),
            horizontal=True, key=f'{page_key}_moeda_radio',
            label_visibility='collapsed',
        )
        st.session_state[f'{page_key}_moeda'] = moeda

        # ── Taxas ──
        inicializar_banco_taxas()
        taxas = carregar_taxas_banco()

        if moeda != 'BRL':
            col_t1, col_t2 = st.columns([1.1, 1.1], gap="small")
            with col_t1:
                taxas['USD'] = st.number_input(
                    "USD→BRL", value=taxas.get('USD', 5.0),
                    min_value=0.01, step=0.01, format="%.2f",
                    key=f'{page_key}_taxa_usd',
                )
            with col_t2:
                taxas['EUR'] = st.number_input(
                    "EUR→BRL", value=taxas.get('EUR', 5.5),
                    min_value=0.01, step=0.01, format="%.2f",
                    key=f'{page_key}_taxa_eur',
                )
            salvar_taxas_banco(taxas)

        st.divider()

        # ── Tipo de visualização ──
        tipo = st.radio(
            "📊 Tipo", ["Custo Total", "CPU (Custo por Unidade)"],
            horizontal=True, key=f'{page_key}_tipo',
        )

        # ── Fator ──
        if tipo == "Custo Total":
            fator = st.radio(
                "🔢 Fator", ["Nenhum", "K (milhares)", "M (milhões)"],
                horizontal=True, key=f'{page_key}_fator',
            )
        else:
            fator = "Nenhum"

        st.divider()

        # ── Tema ──
        st.markdown("**🎨 Tema**")
        col_dark, col_light = st.columns(2)
        tema_atual = get_current_theme()

        with col_dark:
            if st.button("🌙 Dark", use_container_width=True, key=f'{page_key}_dark',
                         disabled=(tema_atual == 'dark')):
                save_theme_to_config('dark')
                st.session_state['_theme_changed'] = True

        with col_light:
            if st.button("☀️ Light", use_container_width=True, key=f'{page_key}_light',
                         disabled=(tema_atual == 'light')):
                save_theme_to_config('light')
                st.session_state['_theme_changed'] = True

        # Force reload se tema mudou
        if st.session_state.get('_theme_changed'):
            st.session_state['_theme_changed'] = False
            components.html(
                "<script>window.top.location.reload();</script>",
                height=0,
            )

        st.divider()

        # ── Limpar Cache ──
        if st.button("🔄 Limpar Cache", use_container_width=True,
                     key=f'{page_key}_clear_cache'):
            st.cache_data.clear()
            st.rerun()

    simbolo = obter_simbolo_moeda(moeda)
    return {
        'ano': ano,
        'moeda': moeda,
        'simbolo': simbolo,
        'taxas': taxas,
        'tipo': tipo,
        'fator': fator,
    }


def render_sidebar_filters(df, page_key, filtros=None):
    """
    Renderiza filtros de dados na sidebar.
    filtros: lista de nomes de filtro a exibir.
             Default: ['oficina', 'custo', 'periodo']

    Retorna dict com seleções.
    """
    if filtros is None:
        filtros = ['oficina', 'custo', 'periodo']

    result = {}

    with st.sidebar:
        st.subheader("🔍 Filtros")

        if 'oficina' in filtros and 'Oficina' in df.columns:
            oficinas = sorted(df['Oficina'].dropna().unique())
            result['oficinas'] = st.multiselect(
                "Oficina", oficinas, default=oficinas,
                key=f'{page_key}_ofi',
            )

        if 'custo' in filtros and 'Custo' in df.columns:
            custos = sorted(df['Custo'].dropna().unique())
            result['custos'] = st.multiselect(
                "Tipo Custo", custos, default=custos,
                key=f'{page_key}_custo',
            )

        if 'periodo' in filtros:
            periodos_disp = [m for m in ORDEM_MESES if m in df['Período'].unique()]
            result['periodos'] = st.multiselect(
                "Período", periodos_disp, default=periodos_disp,
                key=f'{page_key}_per',
            )

        if 'veiculo' in filtros and 'Veículo' in df.columns:
            veiculos = sorted(df['Veículo'].dropna().unique())
            result['veiculos'] = st.multiselect(
                "Veículo", veiculos, default=veiculos,
                key=f'{page_key}_veic',
            )

        if 'account' in filtros and 'Account' in df.columns:
            accounts = sorted(df['Account'].dropna().unique())
            result['accounts'] = st.multiselect(
                "Account", accounts, default=[],
                key=f'{page_key}_account',
            )

    return result


def aplicar_filtros(df, filtros_sel):
    """Aplica filtros da sidebar ao DataFrame."""
    df = df.copy()
    if 'oficinas' in filtros_sel and filtros_sel['oficinas']:
        df = df[df['Oficina'].isin(filtros_sel['oficinas'])]
    if 'custos' in filtros_sel and filtros_sel['custos']:
        df = df[df['Custo'].isin(filtros_sel['custos'])]
    if 'periodos' in filtros_sel and filtros_sel['periodos']:
        df = df[df['Período'].isin(filtros_sel['periodos'])]
    if 'veiculos' in filtros_sel and filtros_sel['veiculos']:
        df = df[df['Veículo'].isin(filtros_sel['veiculos'])]
    if 'accounts' in filtros_sel and filtros_sel['accounts']:
        df = df[df['Account'].isin(filtros_sel['accounts'])]
    return df


# ═══════════════════════════════════════════════════════════════
#  TABELA HTML CUSTOMIZADA
# ═══════════════════════════════════════════════════════════════

def _formatar_barra_progresso(valor, referencia):
    """Gera HTML da barra de progresso valor/referência."""
    if referencia == 0 or pd.isna(referencia) or pd.isna(valor):
        return "—"
    pct = abs(valor / referencia) * 100
    pct_display = min(pct, 100)

    # Cores: verde ≤80%, amarelo 80-100%, vermelho >100%
    if pct <= 80:
        cor = '#27ae60'
    elif pct <= 100:
        cor = '#f39c12'
    else:
        cor = '#e74c3c'

    return f"""
    <div style="display: flex; align-items: center; gap: 4px;">
        <div style="flex: 1; height: 12px; background: rgba(255,255,255,0.08);
                    border-radius: 3px; overflow: hidden; min-width: 60px;">
            <div style="width: {pct_display}%; height: 100%; background: {cor};
                        border-radius: 3px; transition: width 0.3s;"></div>
        </div>
        <span style="font-size: 0.65rem; min-width: 36px; text-align: right;">
            {pct:.0f}%
        </span>
    </div>
    """


def criar_tabela_html(df, tema=None, col_barra=None, col_ref=None,
                      linha_total=True, simbolo='R$'):
    """
    Cria tabela HTML estilizada no padrão TC Ext.

    Args:
        df: DataFrame a exibir
        tema: 'dark' ou 'light' (auto-detectado se None)
        col_barra: coluna para barra de progresso
        col_ref: coluna de referência para a barra
        linha_total: se True, adiciona linha com totais
        simbolo: símbolo monetário para formatação
    """
    if tema is None:
        tema = get_current_theme()

    is_dark = tema == 'dark'

    # Cores tema
    bg_header = 'rgba(255,255,255,0.05)' if is_dark else 'rgba(0,0,0,0.03)'
    border_color = 'rgba(250,250,250,0.1)' if is_dark else 'rgba(49,51,63,0.1)'
    text_color = '#FAFAFA' if is_dark else '#262730'
    bg_total = 'rgba(255,255,255,0.08)' if is_dark else 'rgba(0,0,0,0.05)'

    html = f"""
    <div style="overflow-x: auto;">
    <table style="width: 100%; border-collapse: collapse;
                  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                  font-size: 0.75rem; color: {text_color};">
    <thead>
    <tr style="border-bottom: 2px solid {border_color}; background: {bg_header};">
    """

    # Header
    for col in df.columns:
        align = 'right' if df[col].dtype in ['float64', 'int64', 'float32', 'int32'] else 'left'
        html += f"""<th style="padding: 8px 10px; font-weight: 600; font-size: 0.75rem;
                              text-transform: uppercase; letter-spacing: 0.5px;
                              text-align: {align}; white-space: nowrap;">{col}</th>"""
    html += "</tr></thead><tbody>"

    # Rows
    for _, row in df.iterrows():
        html += f'<tr style="border-bottom: 1px solid {border_color};">'
        for col in df.columns:
            val = row[col]
            is_numeric = isinstance(val, (int, float, np.integer, np.floating))

            if col == col_barra and col_ref and col_ref in row.index:
                cell_html = _formatar_barra_progresso(val, row[col_ref])
                html += f'<td style="padding: 6px 10px;">{cell_html}</td>'
            elif is_numeric:
                formatted = f"{val:,.2f}" if isinstance(val, float) else f"{val:,}"
                html += f"""<td style="padding: 6px 10px; text-align: right;
                                      font-family: 'SF Mono', 'Fira Code', monospace;
                                      font-variant-numeric: tabular-nums;
                                      font-size: 0.7rem;">{formatted}</td>"""
            else:
                html += f'<td style="padding: 6px 10px;">{val}</td>'
        html += '</tr>'

    # Linha total
    if linha_total and len(df) > 1:
        html += f'<tr style="border-top: 2px solid {border_color}; background: {bg_total}; font-weight: 600;">'
        for i, col in enumerate(df.columns):
            if i == 0:
                html += f'<td style="padding: 8px 10px;">Total</td>'
            elif df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
                total = df[col].sum()
                formatted = f"{total:,.2f}" if df[col].dtype == 'float64' else f"{total:,}"
                html += f"""<td style="padding: 8px 10px; text-align: right;
                                      font-family: monospace; font-size: 0.7rem;">
                              {formatted}</td>"""
            else:
                html += '<td style="padding: 8px 10px;">—</td>'
        html += '</tr>'

    html += "</tbody></table></div>"
    return html
