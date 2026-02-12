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
    versao_str = "1.91"
    try:
        with open('versao.json', 'r', encoding='utf-8') as f:
            v = json.load(f)
            versao_str = v.get('versao', '1.91')
    except Exception:
        pass

    # Mês/ano atual
    agora = datetime.now()
    meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
    }
    mes_atual = meses_pt.get(agora.month, '')
    ano_atual = agora.year

    # Data atualização parquets (formato igual TC Ext)
    data_atualizacao = None
    try:
        pasta_dados = os.path.join("dados", "TC_Principal")
        if os.path.exists(pasta_dados):
            anos = [d for d in os.listdir(pasta_dados)
                    if os.path.isdir(os.path.join(pasta_dados, d)) and d.isdigit()]
            if anos:
                ano_mais_recente = max(anos, key=int)
                arquivos = [
                    os.path.join(pasta_dados, ano_mais_recente, "df_principal.parquet"),
                    os.path.join(pasta_dados, ano_mais_recente, "df_volume.parquet"),
                    os.path.join(pasta_dados, "historico_consolidado", "df_principal_historico.parquet"),
                ]
                ts_max = None
                for arq in arquivos:
                    if os.path.exists(arq):
                        ts = os.path.getmtime(arq)
                        if ts_max is None or ts > ts_max:
                            ts_max = ts
                if ts_max:
                    dt = datetime.fromtimestamp(ts_max)
                    data_atualizacao = f"{dt.day:02d} de {meses_pt[dt.month]} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
    except Exception:
        pass

    # Montar textos do cabeçalho (igual TC Ext)
    texto_esquerda = f"📚 Documentação Completa do Sistema TC | Versão {versao_str} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin e Lauro Paiva"
    texto_direita = f"📅 Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

    st.markdown(f"""
    <div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
        <div style='flex: 1;'>{texto_esquerda}</div>
        <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
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
            ('BRL', '🇧🇷 R$', 'R$', 'https://flagcdn.com/w40/br.png'),
            ('USD', '🇺🇸 $', '$', 'https://flagcdn.com/w40/us.png'),
            ('EUR', '🇪🇺 €', '€', 'https://flagcdn.com/w40/eu.png'),
        ]

        if f'{page_key}_moeda' not in st.session_state:
            st.session_state[f'{page_key}_moeda'] = 'BRL'

        # Função callback para sincronização imediata (evita 2 cliques)
        def atualizar_moeda():
            if f'{page_key}_moeda_radio' in st.session_state:
                val = st.session_state[f'{page_key}_moeda_radio']
                # Extrair código da moeda do label com emoji
                for cod, label, _, _ in moedas_info:
                    if val == label:
                        st.session_state[f'{page_key}_moeda'] = cod
                        break

        moeda_atual = st.session_state[f'{page_key}_moeda']

        # Radio com emojis de bandeira (alinhamento perfeito)
        opcoes_radio = [label for _, label, _, _ in moedas_info]
        label_atual = next(label for cod, label, _, _ in moedas_info if cod == moeda_atual)
        idx_atual = opcoes_radio.index(label_atual) if label_atual in opcoes_radio else 0

        moeda_label = st.radio(
            "Moeda", opcoes_radio,
            index=idx_atual,
            horizontal=True, key=f'{page_key}_moeda_radio',
            label_visibility='collapsed',
            on_change=atualizar_moeda,
        )
        # Backup de sincronização
        for cod, label, _, _ in moedas_info:
            if moeda_label == label:
                moeda = cod
                break
        else:
            moeda = 'BRL'
        if st.session_state[f'{page_key}_moeda'] != moeda:
            st.session_state[f'{page_key}_moeda'] = moeda

        # ── Taxas ──
        inicializar_banco_taxas()
        taxas_entrada = carregar_taxas_banco()  # taxas no formato "1 USD = X BRL"

        if moeda != 'BRL':
            col_t1, col_t2 = st.columns([1.1, 1.1], gap="small")
            with col_t1:
                st.markdown(
                    '<p style="font-size:0.7rem;margin-bottom:0.2rem;">🇺🇸 1 $ (USD) = R$</p>',
                    unsafe_allow_html=True
                )
                taxas_entrada['USD'] = st.number_input(
                    "Taxa USD para BRL",
                    value=taxas_entrada.get('USD', 5.0),
                    min_value=0.01, step=0.01, format="%.2f",
                    key=f'{page_key}_taxa_usd',
                    label_visibility='collapsed',
                )
            with col_t2:
                st.markdown(
                    '<p style="font-size:0.7rem;margin-bottom:0.2rem;">🇪🇺 1 € (EUR) = R$</p>',
                    unsafe_allow_html=True
                )
                taxas_entrada['EUR'] = st.number_input(
                    "Taxa EUR para BRL",
                    value=taxas_entrada.get('EUR', 5.5),
                    min_value=0.01, step=0.01, format="%.2f",
                    key=f'{page_key}_taxa_eur',
                    label_visibility='collapsed',
                )
            salvar_taxas_banco(taxas_entrada)

        # Calcular taxas INVERSAS para conversão (1 BRL = X USD/EUR)
        # Ex: Se 1 USD = 5 BRL, então 1 BRL = 0.20 USD
        # Assim: 100 BRL * 0.20 = 20 USD (correto!)
        taxa_usd = taxas_entrada.get('USD', 5.0)
        taxa_eur = taxas_entrada.get('EUR', 5.5)
        taxas = {
            'BRL': 1.0,
            'USD': 1.0 / taxa_usd if taxa_usd > 0 else 0.20,
            'EUR': 1.0 / taxa_eur if taxa_eur > 0 else 0.18,
        }

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
                index=1,  # Padrão: K (milhares)
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


# ═══════════════════════════════════════════════════════════════
#  FORMATAÇÃO RATIO COM BARRINHA (padrão TC Ext)
# ═══════════════════════════════════════════════════════════════

def formatar_ratio_com_barra(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com barra de progresso em HTML"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular largura da barra: 100% = barra cheia, acima de 100% também fica cheia
    if percentual >= 100:
        largura_barra = 100  # Barra cheia para 100% ou mais
    else:
        largura_barra = max(0, percentual)  # Proporcional até 100%
    
    # Calcular cor: verde até 90%, depois gradiente até vermelho em 100%
    if percentual <= 0:
        r, g, b = 0, 170, 0  # Verde (#00AA00)
    elif percentual <= 90:
        r, g, b = 0, 170, 0  # Verde puro até 90%
    elif percentual >= 100:
        r, g, b = 255, 0, 0  # Vermelho (#FF0000) quando 100% ou mais
    else:
        # Gradiente de verde para vermelho entre 90% e 100%
        progresso = (percentual - 90) / 10
        r = int(255 * progresso)
        g = int(170 * (1 - progresso))
        b = 0
    
    cor = f"rgb({r}, {g}, {b})"
    
    # Detectar tema para adaptar cor do texto
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            texto_cor = "#FAFAFA"
        else:
            texto_cor = "#31333F"
    except:
        texto_cor = "#31333F"
    
    html = f"""
    <div style="display: flex; align-items: center; gap: 5px; width: 100%; justify-content: flex-start; margin: 0; padding: 0; vertical-align: middle;">
        <div style="width: 64px; background-color: #333; border-radius: 3px; height: 11px; position: relative; overflow: hidden; flex-shrink: 0; margin: 0;">
            <div style="width: {largura_barra}%; height: 100%; background-color: {cor}; transition: width 0.3s;"></div>
        </div>
        <span style="width: 65px; text-align: left; font-weight: normal; color: {texto_cor}; font-size: 0.75rem; flex-shrink: 0; line-height: 1.2; margin: 0;">{percentual:.0f}%</span>
    </div>
    """
    return html


def criar_tabela_html_flex(df_display, simbolo='R$', sufixo=''):
    """
    Cria tabela HTML para Análise Flex por Categoria com barrinha no Total / Flex Bud.
    
    Args:
        df_display: DataFrame com colunas Account, BUD, Flex Bud - BUD, Flex BUD, 
                   Total - Flex Bud, Total, Total / Flex Bud
        simbolo: Símbolo da moeda
        sufixo: Sufixo do valor (ex: ' K', ' M')
    """
    # Detectar tema
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            header_bg = "rgba(38, 39, 48, 0.15)"
            border_color = "rgba(250, 250, 250, 0.1)"
            text_color = "#FAFAFA"
        else:
            header_bg = "rgba(240, 242, 246, 0.15)"
            border_color = "rgba(49, 51, 63, 0.1)"
            text_color = "#31333F"
    except:
        header_bg = "rgba(38, 39, 48, 0.15)"
        border_color = "rgba(250, 250, 250, 0.1)"
        text_color = "#FAFAFA"
    
    html = f"""
    <div style='overflow-x: auto; margin: 0.5rem 0;'>
        <style>
            .flex-table {{
                width: 100%;
                border-collapse: collapse;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                font-size: 0.75rem;
            }}
            .flex-table th {{
                background: {header_bg};
                padding: 0.5rem 0.75rem;
                text-align: left;
                font-weight: 600;
                border-bottom: 1px solid {border_color};
                color: {text_color};
            }}
            .flex-table td {{
                padding: 0.5rem 0.75rem;
                border-bottom: 1px solid {border_color};
                vertical-align: middle;
                color: {text_color};
            }}
            .flex-table .num {{
                text-align: right;
                font-family: 'SF Mono', Consolas, monospace;
                font-variant-numeric: tabular-nums;
            }}
            .flex-table .ratio-col {{
                min-width: 130px;
            }}
        </style>
        <table class='flex-table'>
            <thead>
                <tr>
    """
    
    # Colunas
    colunas = df_display.columns.tolist()
    for col in colunas:
        classe = "ratio-col" if col == "Total / Flex Bud" else ""
        html += f"<th class='{classe}'>{col}</th>"
    html += "</tr></thead><tbody>"
    
    # Linhas de dados
    for _, row in df_display.iterrows():
        html += "<tr>"
        for col in colunas:
            val = row[col]
            if col == "Total / Flex Bud":
                # Renderizar com barrinha
                if isinstance(val, (int, float)) and not pd.isna(val):
                    barra_html = formatar_ratio_com_barra(val)
                    html += f"<td class='ratio-col'>{barra_html}</td>"
                else:
                    html += f"<td class='ratio-col'>—</td>"
            elif col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                # Formatar valor monetário
                if isinstance(val, (int, float)) and not pd.isna(val):
                    html += f"<td class='num'>{simbolo} {val:,.2f}{sufixo}</td>"
                else:
                    html += f"<td class='num'>—</td>"
            else:
                # Texto (Account, Type 05, etc.)
                html += f"<td>{val}</td>"
        html += "</tr>"
    
    html += "</tbody></table></div>"
    return html

