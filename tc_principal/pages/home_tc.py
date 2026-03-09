"""
TC Veículos — Home
Dashboard com visão geral do custo de produção de veículos.
Padrão visual TC Ext: Altair, CSS global, seletores universais.
"""

import sys as _sys
import os as _os
if hasattr(_sys, '_MEIPASS'):
    _ROOT = _sys._MEIPASS
else:
    _ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import json
import unicodedata
import hashlib
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    load_principal, load_principal_real,
    load_volume_bud, load_volume_actual,
    load_tempo_veiculos, load_tempo_veiculos_real,
    load_dea_dedicado, load_dea_dedicado_real, load_volume_fa, load_volume_fa_real,
    load_custo_fp_veiculo, load_custo_fp_veiculo_real,
    load_custo_fp_veiculo_forecast_fresh,
    load_forecast_completo,
    load_percentual_rateio_veiculos_real, ratear_be_por_veiculo,
    load_tc_sapiens,
    normalizar_periodo, ordenar_por_mes,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    aplicar_fator_df,
    converter_moeda_df, obter_sufixo_fator, calcular_cpu,
    extrair_redis,
    _pivotar_detalhado, _pivotar_flex, render_secao_tabela_detalhe,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, render_sidebar_filters, aplicar_filtros,
    criar_tabela_html, render_kpi, render_kpi_spacer,
    formatar_ratio_com_barra, criar_tabela_html_flex,
)
from processamento_dados_veiculos import executar_conferencias

# Desabilitar limite de linhas do Altair (nível de módulo, uma única vez)
alt.data_transformers.disable_max_rows()

# Dicionário de meses em português
meses_pt = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}


def _carregar_rateios_manuais():
    """Carrega rateios manuais (QY/GS/SM) do arquivo raiz do projeto."""
    caminho = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'rateios_manuais.json'
    )
    padrao = {'QY': 0.087526, 'GS': 0.086982, 'SM': 0.075452}
    try:
        if os.path.exists(caminho):
            with open(caminho, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {
                'QY': float(data.get('QY', padrao['QY'])),
                'GS': float(data.get('GS', padrao['GS'])),
                'SM': float(data.get('SM', padrao['SM'])),
            }
    except Exception:
        pass
    return padrao


_MAP_PER = {
    'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
    'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
    'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
    'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro',
}


@st.cache_data(ttl=3600, show_spinner=True)
def _load_forecast(ano=None):
    """Carrega forecast_completo.parquet (Histórico + BE)."""
    caminho = os.path.join(
        "dados", "TC_Principal", "Forecast", "forecast_completo.parquet"
    )
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    df = normalizar_periodo(df)
    if 'Período' in df.columns:
        df['Período'] = (
            df['Período'].astype(str).str.strip().str.lower()
            .map(_MAP_PER).fillna(df['Período'])
        )
    for c in COLUNAS_MONETARIAS + ['Total', 'Volume', 'CPU']:
        if c in df.columns and df[c].dtype == 'object':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    if ano and ano != "Todos" and 'Ano' in df.columns:
        try:
            df = df[df['Ano'] == int(ano)].copy()
        except (ValueError, TypeError):
            pass
    # Normalizar coluna Tipo: Histórico / BE
    if 'Tipo' in df.columns:
        def _norm_tipo(v):
            if pd.isna(v):
                return 'BE'
            txt = str(v).replace('\ufffd', '').strip().lower()
            txt = (
                unicodedata.normalize('NFKD', txt)
                .encode('ascii', 'ignore')
                .decode('ascii')
            )
            if 'hist' in txt:
                return 'Histórico'
            return 'BE'
        df['Tipo'] = df['Tipo'].apply(_norm_tipo)
    return df


def create_periodo_chart(df_periodo, df_flex, tipo, label_valor,
                         simbolo, sufixo, ordem_per, tem_ano=False,
                         col_tipo=None, modo_be=False):
    """
    Cria gráfico de Custo FP por Período usando Plotly (resolução definitiva
    do bug de renderização do Altair).

    Modo Real  → barras com degradê roxo contínuo, Flex Bud laranja pontilhada.
    Modo BE    → barras Histórico roxo escuro + BE roxo claro, Flex Bud laranja.
    Delta      → mini-barras verde (negativo = bom) / vermelho (positivo = ruim).
    """
    try:
        # Garantir ordem_per único (remover duplicados mantendo ordem)
        ordem_per = list(dict.fromkeys(ordem_per)) if ordem_per else []
        
        coluna = 'Custo FP'
        titulo_y = f'{label_valor} ({simbolo}{sufixo})'

        # ── Preparar dados ──
        df_p = df_periodo.copy()
        df_p[coluna] = pd.to_numeric(df_p[coluna], errors='coerce').fillna(0)
        df_p = df_p.replace([np.inf, -np.inf], 0)

        # Coluna do período para eixo X
        if tem_ano and 'Ano' in df_p.columns:
            df_p['_x_label'] = df_p['Período'].astype(str) + ' ' + df_p['Ano'].astype(str)
        else:
            df_p['_x_label'] = df_p['Período'].astype(str)

        x_col = '_x_label'
        _usar_tipo = bool(col_tipo and col_tipo in df_p.columns)

        # ── Decidir número de subplots ──
        tem_flex = False
        df_flex_p = _preparar_flex(df_flex, tem_ano, tipo, ordem_per)
        if df_flex_p is not None and len(df_flex_p) > 0:
            tem_flex = True

        n_rows = 2 if tem_flex else 1
        row_heights = [0.162, 0.838] if tem_flex else [1.0]

        fig = make_subplots(
            rows=n_rows, cols=1, shared_xaxes=True,
            vertical_spacing=0.17,
            row_heights=row_heights,
        )

        # ════════════════════════════════════════
        # BARRAS PRINCIPAIS (último subplot = embaixo)
        # ════════════════════════════════════════
        bar_row = n_rows  # última linha

        if _usar_tipo and modo_be:
            # ── Modo BE: empilhar Histórico (escuro) + BE (claro) por período ──
            _cores_tipo = {'Histórico': '#4C1D95', 'BE': '#C4B5FD'}
            for tipo_val in ['Histórico', 'BE']:
                mask = df_p[col_tipo] == tipo_val
                sub = df_p[mask].copy()
                if sub.empty:
                    continue
                # Agregar por x_label (um valor por período por tipo)
                sub_agg = sub.groupby(x_col, as_index=False)[coluna].sum()
                # Ordenar
                sub_agg[x_col] = pd.Categorical(sub_agg[x_col], categories=ordem_per, ordered=True)
                sub_agg = sub_agg.sort_values(x_col)

                fig.add_trace(go.Bar(
                    x=sub_agg[x_col].astype(str),
                    y=sub_agg[coluna],
                    name=tipo_val,
                    marker_color=_cores_tipo.get(tipo_val, '#C4B5FD'),
                    text=sub_agg[coluna],
                    texttemplate='%{y:,.2f}',
                    textposition='outside',
                    cliponaxis=False,
                    textfont=dict(size=9, color=_cores_tipo.get(tipo_val, '#C4B5FD')),
                    hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>' + tipo_val + '</extra>',
                ), row=bar_row, col=1)
        else:
            # ── Modo Real: degradê roxo contínuo ──
            # Agregar por período (sem Tipo)
            df_agg = df_p.groupby(x_col, as_index=False)[coluna].sum()
            df_agg[x_col] = pd.Categorical(df_agg[x_col], categories=ordem_per, ordered=True)
            df_agg = df_agg.sort_values(x_col)

            vals = df_agg[coluna].values
            v_min = vals.min() if len(vals) > 0 else 0
            v_max = vals.max() if len(vals) > 0 else 1
            if v_max == v_min:
                v_max = v_min + 1

            # Gerar cores roxo degradê (mais claro → mais escuro proporcional ao valor)
            bar_colors = []
            for v in vals:
                t = (v - v_min) / (v_max - v_min) if v_max > v_min else 0.5
                # Interpolar de roxo claro (#D8B4FE) a roxo escuro (#4C1D95)
                r = int(216 + t * (76 - 216))
                g = int(180 + t * (29 - 180))
                b = int(254 + t * (149 - 254))
                bar_colors.append(f'rgb({r},{g},{b})')

            fig.add_trace(go.Bar(
                x=df_agg[x_col].astype(str),
                y=df_agg[coluna],
                name='Real',
                marker_color=bar_colors,
                text=df_agg[coluna],
                texttemplate='%{y:,.2f}',
                textposition='outside',
                cliponaxis=False,
                textfont=dict(size=9, color=bar_colors),
                hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>Real</extra>',
                showlegend=False,
            ), row=bar_row, col=1)

        # ════════════════════════════════════════
        # LINHA FLEX BUD (laranja pontilhada)
        # ════════════════════════════════════════
        if tem_flex and df_flex_p is not None:
            # Garantir ordem
            if x_col == '_x_label':
                if tem_ano and 'Ano' in df_flex_p.columns:
                    df_flex_p['_x_label'] = df_flex_p['Período'].astype(str) + ' ' + df_flex_p['Ano'].astype(str)
                else:
                    df_flex_p['_x_label'] = df_flex_p['Período'].astype(str)
            df_flex_p[x_col] = pd.Categorical(df_flex_p[x_col], categories=ordem_per, ordered=True)
            df_flex_p = df_flex_p.sort_values(x_col)

            fig.add_trace(go.Scatter(
                x=df_flex_p[x_col].astype(str),
                y=df_flex_p['Flex_Bud'],
                name='Flex Bud',
                mode='lines+markers+text',
                line=dict(color='#FF6B35', width=2, dash='dot'),
                marker=dict(color='#FF6B35', size=7),
                text=df_flex_p['Flex_Bud'],
                texttemplate='%{y:,.2f}',
                textposition='top center',
                cliponaxis=False,
                textfont=dict(size=9, color='#FF6B35'),
                hovertemplate='%{x}<br>Flex Bud: %{y:,.2f}<extra>Flex Bud</extra>',
            ), row=bar_row, col=1)

            # ════════════════════════════════════════
            # DELTA (mini-barras no topo)
            # ════════════════════════════════════════
            delta_label = 'BE' if modo_be else 'Real'
            delta_titulo = f'Delta ({delta_label} - Flex Bud)'

            # Agregar período sem Tipo para comparar com Flex
            delta_real = df_p.groupby(x_col, as_index=False)[coluna].sum()
            delta_flex_agg = df_flex_p.groupby(x_col, as_index=False)['Flex_Bud'].sum()
            delta_data = delta_real.merge(delta_flex_agg, on=x_col, how='left')
            delta_data['Flex_Bud'] = delta_data['Flex_Bud'].fillna(0)
            delta_data['Delta'] = delta_data[coluna] - delta_data['Flex_Bud']
            delta_data[x_col] = pd.Categorical(delta_data[x_col], categories=ordem_per, ordered=True)
            delta_data = delta_data.sort_values(x_col)

            delta_colors = ['#00AA00' if d < 0 else '#FF0000' for d in delta_data['Delta']]

            fig.add_trace(go.Bar(
                x=delta_data[x_col].astype(str),
                y=delta_data['Delta'],
                name=delta_titulo,
                marker_color=delta_colors,
                width=0.315,
                text=delta_data['Delta'],
                texttemplate='%{y:,.2f}',
                textposition='outside',
                cliponaxis=False,
                textfont=dict(size=8, color=delta_colors),
                hovertemplate='%{x}<br>Delta: %{y:,.2f}<extra>' + delta_titulo + '</extra>',
                showlegend=False,
            ), row=1, col=1)

            fig.update_yaxes(title_text=delta_titulo, row=1, col=1,
                             showgrid=False, zeroline=True,
                             zerolinecolor='rgba(160,160,160,0.35)', zerolinewidth=0.5,
                             tickfont=dict(size=8))
            fig.update_xaxes(
                row=1, col=1,
                showline=False,
                showgrid=False,
                linecolor='rgba(0,0,0,0)',
                linewidth=0,
                ticks='',
            )

        # ════════════════════════════════════════
        # LAYOUT FINAL
        # ════════════════════════════════════════
        n_periodos = len(ordem_per) if ordem_per else 1
        altura = min(620, max(350, 22 * n_periodos + 180))

        fig.update_yaxes(title_text=titulo_y, row=bar_row, col=1,
                 showgrid=False, automargin=True)
        fig.update_xaxes(title_text='Período', row=bar_row, col=1,
                         categoryorder='array', categoryarray=ordem_per,
                         automargin=True, title_standoff=20)
        if tem_flex:
            fig.update_xaxes(showticklabels=False, row=1, col=1,
                             categoryorder='array', categoryarray=ordem_per)
            fig.update_xaxes(showline=False, row=1, col=1)
            fig.update_xaxes(showline=False, row=bar_row, col=1)
        else:
            fig.update_xaxes(showline=False, row=bar_row, col=1)

        _altura_base = altura + (100 if tem_flex else 0)
        _altura_final = int(_altura_base * 1.24) if tem_flex else _altura_base

        fig.update_layout(
            height=_altura_final,
            barmode='stack' if (_usar_tipo and modo_be) else 'group',
            legend=dict(
                orientation='h', yanchor='top', y=-0.24,
                xanchor='center', x=0.5, font=dict(size=10),
            ),
            margin=dict(l=60, r=30, t=130, b=130),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )

        return fig

    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None


def _preparar_flex(df_flex, tem_ano, tipo, ordem_per):
    """Prepara dados do Flex Budget para o gráfico."""
    if df_flex is None or len(df_flex) == 0:
        return None

    colunas_flex = ['Período', 'Flex_Bud']
    if tem_ano and 'Ano' in df_flex.columns:
        colunas_flex.insert(0, 'Ano')

    df_flex_p = df_flex[colunas_flex].copy()
    df_flex_p['Período'] = df_flex_p['Período'].astype(str)

    if tem_ano and 'Ano' in df_flex_p.columns:
        df_flex_p['Ano'] = df_flex_p['Ano'].astype(str)

    df_flex_p = ordenar_por_mes(df_flex_p)

    if tipo == 'CPU (Custo por Unidade)' and 'Vol_Actual' in df_flex.columns:
        colunas_vol_merge = ['Período', 'Vol_Actual']
        if tem_ano and 'Ano' in df_flex.columns:
            colunas_vol_merge.insert(0, 'Ano')
            merge_on = ['Ano', 'Período']
        else:
            merge_on = 'Período'
        df_flex_vol = df_flex[colunas_vol_merge].copy()
        df_flex_vol['Período'] = df_flex_vol['Período'].astype(str)
        if tem_ano and 'Ano' in df_flex_vol.columns:
            df_flex_vol['Ano'] = df_flex_vol['Ano'].astype(str)
        df_flex_p = df_flex_p.merge(df_flex_vol, on=merge_on, how='left')
        df_flex_p['Vol_Actual'] = df_flex_p['Vol_Actual'].fillna(0)
        df_flex_p['Flex_Bud'] = calcular_cpu(df_flex_p['Flex_Bud'], df_flex_p['Vol_Actual'])

    df_flex_p = df_flex_p.replace([np.inf, -np.inf], 0)
    df_flex_p['Flex_Bud'] = df_flex_p['Flex_Bud'].fillna(0)
    df_flex_p = df_flex_p.reset_index(drop=True)

    if df_flex_p['Flex_Bud'].abs().sum() == 0:
        return None

    return df_flex_p


def render():
    """Renderiza a página Home do TC Veículos."""

    injetar_css_global()
    render_header()

    st.title("🏭 Dashboard TC Veículos")
    st.subheader("Custo de Produção de Veículos • Real")

    # ── Sidebar Global ──
    cfg = render_sidebar_global('home')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, tipo, fator = cfg['taxas'], cfg['tipo'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    # ── Carregar dados ──
    df_principal = load_principal(ano)
    df_real_raw = load_principal_real(ano)
    df_be_raw = _load_forecast(ano)
    df_vol_bud = load_volume_bud(ano)
    df_vol_actual = load_volume_actual(ano)
    df_tempo_veic = load_tempo_veiculos(ano)

    # ── Carregar dados rateados por veículo ──
    df_veic_bud_raw = load_custo_fp_veiculo(ano)
    df_veic_real_raw = load_custo_fp_veiculo_real(ano)
    df_veic_be_raw = load_custo_fp_veiculo_forecast_fresh()  # Forecast com veículo (cache invalidado por mtime)

    if df_principal is None:
        st.error(f"❌ Dados do TC Veículos não encontrados para {ano}")
        st.info("💡 Execute o processamento na página **Extração de Dados**.")
        st.stop()

    df_principal = normalizar_periodo(df_principal)

    # ── Cópias raw para filtros locais da Tab 1 ──
    _raw_df_principal = df_principal.copy()
    _raw_df_real = normalizar_periodo(df_real_raw.copy()) if df_real_raw is not None else None
    _raw_df_be = normalizar_periodo(df_be_raw.copy()) if df_be_raw is not None else None
    _raw_df_vol_bud = normalizar_periodo(df_vol_bud.copy()) if df_vol_bud is not None else None
    _raw_df_vol_actual = normalizar_periodo(df_vol_actual.copy()) if df_vol_actual is not None else None

    # ── Filtros (inclui Veículo como selectbox na sidebar) ──
    filtros_sel = render_sidebar_filters(
        df_principal, 'home', ['oficina', 'custo', 'veiculo', 'periodo']
    )

    # ── Determinar se usa dados rateados por veículo ──
    usar_rateado = not filtros_sel.get('veiculo_todos', True)

    if usar_rateado and df_veic_bud_raw is not None:
        # Dados rateados por veículo — BUD
        _df_base_bud = normalizar_periodo(df_veic_bud_raw.copy())
        if 'Custo FP Veiculo' in _df_base_bud.columns:
            _df_base_bud['Custo FP'] = _df_base_bud['Custo FP Veiculo']
        df = aplicar_filtros(_df_base_bud, filtros_sel)

        # Dados rateados por veículo — Real
        df_real = None
        if df_veic_real_raw is not None:
            _df_base_real = normalizar_periodo(df_veic_real_raw.copy())
            if 'Custo FP Veiculo' in _df_base_real.columns:
                _df_base_real['Custo FP'] = _df_base_real['Custo FP Veiculo']
            _df_real_filt = aplicar_filtros(_df_base_real, filtros_sel)
            if not _df_real_filt.empty:
                df_real = _df_real_filt

        # Volumes filtrados pelo veículo selecionado
        veiculos_sel = filtros_sel.get('veiculos', [])
        if df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
            df_vol_bud = normalizar_periodo(df_vol_bud.copy())
            df_vol_bud = df_vol_bud[df_vol_bud['Veículo'].isin(veiculos_sel)]
        if df_vol_actual is not None and 'Veículo' in df_vol_actual.columns:
            df_vol_actual = normalizar_periodo(df_vol_actual.copy())
            df_vol_actual = df_vol_actual[df_vol_actual['Veículo'].isin(veiculos_sel)]
    else:
        # Dados consolidados (principal)
        df = aplicar_filtros(df_principal, filtros_sel)
        df_real = None
        if df_real_raw is not None:
            df_real_temp = normalizar_periodo(df_real_raw.copy())
            df_real_temp = aplicar_filtros(df_real_temp, filtros_sel)
            if not df_real_temp.empty:
                df_real = df_real_temp

    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        st.stop()

    # ── Aplicar fator e moeda ──
    cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
    df = aplicar_fator_df(df, cols_val, fator)
    df = converter_moeda_df(df, cols_val, moeda, taxas)

    if df_real is not None:
        cols_val_real = [c for c in COLUNAS_MONETARIAS if c in df_real.columns]
        df_real = aplicar_fator_df(df_real, cols_val_real, fator)
        df_real = converter_moeda_df(df_real, cols_val_real, moeda, taxas)

    # ── Budget Flex (calculado com dados filtrados) ──
    tem_ano_df = 'Ano' in df.columns
    df_flex = calcular_flex_budget(df, df_vol_bud, df_vol_actual, tem_ano=tem_ano_df)
    # IMPORTANTE: NÃO aplicar fator/moeda aqui - df já tem fator/moeda aplicados,
    # então Flex_Bud calculado a partir dele já está na escala correta

    # ── Flex detalhado (com dimensões Oficina/Type05/06/Account/Custo) ──
    df_flex_det = calcular_flex_budget_detalhado(
        df, df_vol_bud, df_vol_actual,
        col_custo='Custo FP', tem_ano=tem_ano_df,
    )
    # NOTA: df_flex_det já herda a escala (fator/moeda) do df de entrada,
    # pois Flex_Bud = (Custo / Vol_Bud) * Vol_Actual usa valores já convertidos.
    # NÃO reaplicar fator/moeda aqui para evitar dupla conversão.

    # ── df_bud = Budget, df = Real (ou Budget se sem Real) ──
    df_bud = df.copy()
    tem_real = df_real is not None
    if tem_real:
        df = df_real  # A partir daqui, df = Real para todas exibições

    # ════════════════════════════════════════
    #  MÉTRICAS RESUMO
    # ════════════════════════════════════════
    label_valor = 'CPU' if tipo == 'CPU (Custo por Unidade)' else 'Custo'
    vol_total = df_vol_bud['Volume'].sum() if df_vol_bud is not None else 0

    if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
        soma = {c: df[c].sum() / vol_total for c in cols_val if c in df.columns}
    else:
        soma = {c: df[c].sum() for c in cols_val if c in df.columns}

    # Redis vem de linhas originadas da aba massa-REDIS (marcadas com _fonte_redis), não de coluna separada
    redis_total = extrair_redis(df)
    if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
        redis_val = redis_total / vol_total
    else:
        redis_val = redis_total

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(f"📦 {label_valor} Desp. Primária", f"{simbolo} {soma.get('Despesa Primaria', 0):,.2f}{sufixo}")
    c2.metric(f"🏭 {label_valor} FA", f"{simbolo} {soma.get('Custo FA', 0):,.2f}{sufixo}")
    c3.metric("💰 Redis", f"{simbolo} {redis_val:,.2f}{sufixo}")
    c4.metric(f"🚗 {label_valor} FP", f"{simbolo} {soma.get('Custo FP', 0):,.2f}{sufixo}")
    c5.metric("📉 D&A Dedicada", f"{simbolo} {soma.get('D&A dedicado', 0):,.2f}{sufixo}")
    c6.metric("✅ FP sem Dedicada", f"{simbolo} {soma.get('FP sem Dedicada', 0):,.2f}{sufixo}")

    # ════════════════════════════════════════
    #  TABS
    # ════════════════════════════════════════
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🚗 TC Veículos", "📈 Volume",
        "🏭 Custos por Oficina", "📉 Análise Flex",
        "🚗 Tempo de Produção", "📋 Dados Detalhados",
    ])

    # ── TAB 1: TC Veículos ──
    # Salvar estado global para restaurar após tab1
    _save_df_bud = df_bud.copy()
    _save_df = df.copy()
    _save_df_vol_bud = df_vol_bud.copy() if df_vol_bud is not None else None
    _save_df_vol_actual = df_vol_actual.copy() if df_vol_actual is not None else None
    _save_df_flex = df_flex.copy() if df_flex is not None else None
    _save_cols_val = cols_val[:]
    _save_vol_total = vol_total
    _save_tem_real = tem_real

    with tab1:
        st.markdown("---")

        _fonte_dados_t1 = st.radio(
            "📊 Fonte de Dados",
            ["Real", "BE (Simulado)"],
            index=0,
            horizontal=True,
            key="t1_fonte_dados",
        )
        _usar_be_t1 = _fonte_dados_t1 == "BE (Simulado)"
        if _usar_be_t1 and (_raw_df_be is None or _raw_df_be.empty):
            st.warning(
                "⚠️ Forecast (Best Estimate) não encontrado. "
                "Exibindo Real como fallback."
            )

        # ════════════════════════════════════════
        # 🔍 Filtros da Aba (Oficina + Veículo)
        # ════════════════════════════════════════
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            _oficinas_all = sorted(
                _raw_df_principal['Oficina'].dropna().unique()
            ) if 'Oficina' in _raw_df_principal.columns else []
            _sel_ofi_t1 = st.multiselect(
                "🏭 Oficina", ["Todos"] + _oficinas_all,
                default=["Todos"], key="t1_oficina"
            )
            _ofi_t1 = (
                _oficinas_all if "Todos" in _sel_ofi_t1
                else [x for x in _sel_ofi_t1 if x != "Todos"]
            )
        with col_f2:
            # Veículos do arquivo rateado (df_veic_bud_raw), pois df_principal não tem coluna Veículo
            _df_veic_src = None
            if df_veic_bud_raw is not None:
                _df_veic_src = normalizar_periodo(df_veic_bud_raw.copy())
                # Filtrar por oficinas selecionadas (cascata)
                if _ofi_t1 and 'Oficina' in _df_veic_src.columns:
                    _df_veic_src = _df_veic_src[_df_veic_src['Oficina'].isin(_ofi_t1)]
            _veiculos_all = sorted(
                _df_veic_src['Veículo'].dropna().unique()
            ) if _df_veic_src is not None and 'Veículo' in _df_veic_src.columns else []
            _sel_veic_t1 = st.selectbox(
                "🚗 Veículo", ["Todos"] + _veiculos_all,
                index=0, key="t1_veiculo"
            )

        # Períodos: usar todos disponíveis (filtro de período fica na seção Análise Flex)
        _periodos_all = [
            m for m in ORDEM_MESES
            if m in _raw_df_principal['Período'].unique()
        ]
        _per_t1 = _periodos_all

        # ── Reconstruir dados locais com filtros da aba ──
        _filtros_t1 = {
            'oficinas': _ofi_t1,
            'periodos': _per_t1,
        }
        # Só incluir veiculos no filtro quando um veículo específico for selecionado
        # (df_principal não tem coluna Veículo; apenas os dados rateados têm)
        if _sel_veic_t1 != "Todos":
            _filtros_t1['veiculos'] = [_sel_veic_t1]
        _usar_rateado_t1 = _sel_veic_t1 != "Todos"
        _be_t1 = None

        if _usar_rateado_t1 and df_veic_bud_raw is not None:
            _bud_t1 = normalizar_periodo(df_veic_bud_raw.copy())
            if 'Custo FP Veiculo' in _bud_t1.columns:
                _bud_t1['Custo FP'] = _bud_t1['Custo FP Veiculo']
            df_bud = aplicar_filtros(_bud_t1, _filtros_t1)

            _real_t1 = None
            if df_veic_real_raw is not None:
                _r_t1 = normalizar_periodo(df_veic_real_raw.copy())
                if 'Custo FP Veiculo' in _r_t1.columns:
                    _r_t1['Custo FP'] = _r_t1['Custo FP Veiculo']
                _rt = aplicar_filtros(_r_t1, _filtros_t1)
                if not _rt.empty:
                    _real_t1 = _rt

            if _raw_df_be is not None:
                _filtros_be_t1 = {'oficinas': _ofi_t1, 'periodos': _per_t1}
                
                # PRIORIDADE: Usar arquivo pré-gerado (df_veic_be_raw)
                if df_veic_be_raw is not None and not df_veic_be_raw.empty:
                    _be_veic_raw_t1 = normalizar_periodo(df_veic_be_raw.copy())
                    if 'Custo FP Veiculo' in _be_veic_raw_t1.columns:
                        _be_veic_raw_t1['Custo FP'] = _be_veic_raw_t1['Custo FP Veiculo']
                    _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                    _rt_be = aplicar_filtros(_be_veic_raw_t1, _filtros_be_t1)
                    if not _rt_be.empty:
                        _be_t1 = _rt_be
                else:
                    # Fallback: verificar se dados originais têm veículo
                    _be_tem_veiculo = (
                        'Veículo' in _raw_df_be.columns
                        and _raw_df_be['Veículo'].notna().any()
                        and _sel_veic_t1 in _raw_df_be['Veículo'].values
                    )
                    if _be_tem_veiculo and _sel_veic_t1 != 'Todos':
                        _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                        _rt_be = aplicar_filtros(_raw_df_be, _filtros_be_t1)
                    else:
                        # Último fallback: ratear em runtime (mesma lógica do Real)
                        _pct_be_t1 = load_percentual_rateio_veiculos_real(ano)
                        _dea_be_t1 = load_dea_dedicado_real(ano)
                        _be_rateado_t1 = ratear_be_por_veiculo(
                            _raw_df_be, _pct_be_t1, df_dea=_dea_be_t1
                        )
                        if (
                            _be_rateado_t1 is not None
                            and 'Veículo' in _be_rateado_t1.columns
                        ):
                            if 'Custo FP Veiculo' in _be_rateado_t1.columns:
                                _be_rateado_t1['Custo FP'] = (
                                    _be_rateado_t1['Custo FP Veiculo']
                                )
                            _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                            _rt_be = aplicar_filtros(
                                _be_rateado_t1, _filtros_be_t1
                            )
                        else:
                            _rt_be = aplicar_filtros(_raw_df_be, _filtros_be_t1)
                    if not _rt_be.empty:
                        _be_t1 = _rt_be

            df_vol_bud = _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
            if df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
                df_vol_bud = df_vol_bud[df_vol_bud['Veículo'] == _sel_veic_t1]
            df_vol_actual = _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None
            if df_vol_actual is not None and 'Veículo' in df_vol_actual.columns:
                df_vol_actual = df_vol_actual[df_vol_actual['Veículo'] == _sel_veic_t1]
        else:
            df_bud = aplicar_filtros(_raw_df_principal, _filtros_t1)
            _real_t1 = None
            if _raw_df_real is not None:
                _rt = aplicar_filtros(_raw_df_real, _filtros_t1)
                if not _rt.empty:
                    _real_t1 = _rt
            if _raw_df_be is not None:
                _rt_be = aplicar_filtros(_raw_df_be, _filtros_t1)
                if not _rt_be.empty:
                    _be_t1 = _rt_be
            df_vol_bud = _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
            df_vol_actual = _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None

        if df_bud.empty:
            st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
            df = df_bud.copy()
            df_flex = None
            vol_total = 0
            cols_val = []
            tem_real = False
        else:
            # Aplicar fator e moeda aos dados locais
            cols_val = [c for c in COLUNAS_MONETARIAS if c in df_bud.columns]
            df_bud = aplicar_fator_df(df_bud, cols_val, fator)
            df_bud = converter_moeda_df(df_bud, cols_val, moeda, taxas)

            if _real_t1 is not None:
                _cv_t1 = [c for c in COLUNAS_MONETARIAS if c in _real_t1.columns]
                _real_t1 = aplicar_fator_df(_real_t1, _cv_t1, fator)
                _real_t1 = converter_moeda_df(_real_t1, _cv_t1, moeda, taxas)

            if _be_t1 is not None:
                _cv_be_t1 = [c for c in COLUNAS_MONETARIAS if c in _be_t1.columns]
                _be_t1 = aplicar_fator_df(_be_t1, _cv_be_t1, fator)
                _be_t1 = converter_moeda_df(_be_t1, _cv_be_t1, moeda, taxas)

            tem_real = _real_t1 is not None
            tem_be_t1 = _be_t1 is not None
            if _usar_be_t1:
                if tem_be_t1:
                    df = _be_t1
                elif tem_real:
                    df = _real_t1.copy()
                else:
                    df = df_bud.copy()
            else:
                df = _real_t1 if tem_real else df_bud.copy()

            _tem_ano_t1 = 'Ano' in df.columns
            df_flex = calcular_flex_budget(
                df_bud, df_vol_bud, df_vol_actual, tem_ano=_tem_ano_t1
            )
            # IMPORTANTE: NÃO aplicar fator/moeda - já aplicado em df_bud
            
            vol_total = (
                df_vol_bud['Volume'].sum()
                if df_vol_bud is not None and 'Volume' in df_vol_bud.columns
                else 0
            )
            cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]

        st.markdown("---")

        # ════════════════════════════════════════
        # 📊 Resumo TC Veículos (KPIs dentro da tab)
        # ════════════════════════════════════════
        st.subheader(
            "📊 Resumo Best Estimate"
            if _usar_be_t1 else
            "📊 Resumo TC Veículos"
        )

        # Calcular BUD e Flex BUD usando dados do Budget (já filtrados pela sidebar)
        df_resumo_bud = df_bud.copy()
        df_resumo_bud['Custo_str'] = df_resumo_bud['Custo'].astype(str).str.lower()
        df_resumo_bud['Categoria'] = df_resumo_bud['Custo_str'].apply(
            lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
        )
        
        # Calcular totais por categoria (Budget)
        bud_fixo = df_resumo_bud[df_resumo_bud['Categoria'] == 'Fixo']['Custo FP'].sum()
        bud_variavel = df_resumo_bud[df_resumo_bud['Categoria'] == 'Variável']['Custo FP'].sum()
        bud_total = bud_fixo + bud_variavel
        
        # Calcular proporção global de volume
        if df_vol_bud is not None and df_vol_actual is not None:
            vol_budget_total = df_vol_bud['Volume'].sum()
            vol_actual_total = df_vol_actual['Volume'].sum()
            proporcao_global_tc = (vol_actual_total / vol_budget_total) if vol_budget_total > 0 else 1
        else:
            vol_budget_total = 0
            vol_actual_total = 0
            proporcao_global_tc = 1
        
        # Calcular Flex BUD: Fixo + (Variável × Proporção Global)
        flex_bud_total = bud_fixo + (bud_variavel * proporcao_global_tc)
        
        # Total Real
        total_custo = df['Custo FP'].sum() if 'Custo FP' in df.columns else 0

        # Aplicar CPU se necessário
        if tipo == 'CPU (Custo por Unidade)' and vol_actual_total > 0:
            bud_exibir = bud_total / vol_actual_total
            flex_exibir = flex_bud_total / vol_actual_total
            total_exibir = total_custo / vol_actual_total
        else:
            bud_exibir = bud_total
            flex_exibir = flex_bud_total
            total_exibir = total_custo

        flex_menos_bud = flex_exibir - bud_exibir
        total_menos_flex = total_exibir - flex_exibir
        total_div_flex = (
            (total_exibir / flex_exibir) if flex_exibir != 0 else 0
        )

        def _fmt_val(v):
            return f"{simbolo} {v:,.2f}{sufixo}"

        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            render_kpi("BUD", _fmt_val(bud_exibir))
        with k2:
            render_kpi("Flex Bud - BUD", _fmt_val(flex_menos_bud))
        with k3:
            render_kpi("Flex BUD", _fmt_val(flex_exibir))
        with k4:
            render_kpi(
                "BE - Flex Bud" if _usar_be_t1 else "Real - Flex Bud",
                _fmt_val(total_menos_flex)
            )
        with k5:
            render_kpi(
                "Best Estimate" if _usar_be_t1 else "Real",
                _fmt_val(total_exibir)
            )
        with k6:
            render_kpi(
                "BE / Flex Bud" if _usar_be_t1 else "Real / Flex Bud",
                f"{total_div_flex:.0%}"
            )

        render_kpi_spacer()

        # Alerta sobre volumes iguais
        volumes_iguais = abs(vol_budget_total - vol_actual_total) < 1
        if volumes_iguais:
            st.warning(
                f"⚠️ **Volume Budget ({vol_budget_total:,.0f}) = "
                f"Volume Realizado ({vol_actual_total:,.0f})**  \n"
                f"Proporção = {proporcao_global_tc:.2%} → Flex BUD = BUD.  \n"
                "Verifique os dados de volume na aba **📈 Volume**."
            )

        st.divider()

        # ════════════════════════════════════════
        # Gráfico: Custo FP por Período + Série selecionada + Linha Flex BUD
        # ════════════════════════════════════════
        st.markdown(
            "### Custo FP por Período — Best Estimate"
            if _usar_be_t1 else
            "### Custo FP por Período — Real"
        )

        # Detectar se há coluna Ano (padrão TC Ext)
        tem_ano = 'Ano' in df.columns

        # ── Barras = série selecionada (Real ou BE) ──
        df_periodo = None
        _col_tipo_graf = None
        if 'Custo FP' in df.columns:
            df_graf = df.copy()
            cols_val_graf = [c for c in COLUNAS_MONETARIAS if c in df_graf.columns]
            _grp_cols_per = ['Período']
            if tem_ano and 'Ano' in df_graf.columns:
                _grp_cols_per = ['Ano', 'Período']
            # No modo BE, incluir Tipo no agrupamento para cores Histórico/BE
            if _usar_be_t1 and 'Tipo' in df_graf.columns:
                _grp_cols_per = _grp_cols_per + ['Tipo']
                _col_tipo_graf = 'Tipo'
            df_periodo = df_graf.groupby(_grp_cols_per, as_index=False).agg({
                c: 'sum' for c in cols_val_graf
            })
            df_periodo = ordenar_por_mes(df_periodo)
            df_periodo['Período'] = df_periodo['Período'].astype(str)
            if tem_ano and 'Ano' in df_periodo.columns:
                df_periodo['Ano'] = df_periodo['Ano'].astype(str)

            # Aplicar CPU à série selecionada se necessário
            if tipo == 'CPU (Custo por Unidade)' and df_vol_actual is not None:
                vol_act_norm = df_vol_actual.copy()
                cols_agrup_vol = ['Ano', 'Período'] if tem_ano and 'Ano' in vol_act_norm.columns else ['Período']
                vol_per = vol_act_norm.groupby(cols_agrup_vol, as_index=False)['Volume'].sum()
                vol_per['Período'] = vol_per['Período'].astype(str)
                if tem_ano and 'Ano' in vol_per.columns:
                    vol_per['Ano'] = vol_per['Ano'].astype(str)
                df_periodo = df_periodo.merge(vol_per, on=cols_agrup_vol, how='left')
                df_periodo['Volume'] = df_periodo['Volume'].fillna(0)
                if df_periodo['Volume'].sum() > 0:
                    for c in cols_val_graf:
                        if c in df_periodo.columns:
                            df_periodo[c] = calcular_cpu(
                                df_periodo[c], df_periodo['Volume']
                            )

        # Ordenação cronológica usando lista filtrada de ORDEM_MESES
        if df_periodo is None or len(df_periodo) == 0 or 'Custo FP' not in df_periodo.columns:
            st.info(
                "ℹ️ Nenhum dado de Best Estimate disponível para exibir no gráfico."
                if _usar_be_t1 else
                "ℹ️ Nenhum dado de Realizado disponível para exibir no gráfico."
            )
        else:
            # Criar lista de ordem de períodos
            if tem_ano and 'Período_Completo' not in df_periodo.columns:
                # Criar Período_Completo temporariamente só para ordenação
                df_periodo['Período_Completo'] = df_periodo['Período'] + ' ' + df_periodo['Ano']
            
            periodos_presentes = df_periodo['Período'].unique().tolist()
            ordem_per = [m for m in ORDEM_MESES if m in periodos_presentes]
            
            # Se tem ano, precisamos criar ordem com Período_Completo
            if tem_ano:
                ordem_per = df_periodo['Período_Completo'].tolist()
            
            # Criar gráfico usando função separada (padrão TC Ext)
            grafico_final = create_periodo_chart(
                df_periodo, df_flex, tipo, label_valor,
                simbolo, sufixo, ordem_per, tem_ano,
                col_tipo=_col_tipo_graf,
                modo_be=_usar_be_t1,
            )

            # Renderizar gráfico diretamente (sem placeholder)
            try:
                if grafico_final is not None:
                    st.plotly_chart(grafico_final, use_container_width=True)
                else:
                    st.warning("⚠️ O gráfico não pôde ser criado.")
            except Exception as e:
                import traceback
                st.error(f"❌ Erro ao renderizar gráfico: {str(e)}")
                st.code(traceback.format_exc())

        st.divider()

        # ════════════════════════════════════════
        # 📊 Análise Flex por Categoria (padrão TC Ext)
        # ════════════════════════════════════════
        st.subheader("📊 Análise Flex por Categoria")

        # Períodos disponíveis no Budget
        _periodos_flex_all = sorted(
            df_bud['Período'].dropna().unique().tolist(),
            key=lambda x: ORDEM_MESES.index(x) if x in ORDEM_MESES else 99
        )

        if df_flex is not None and 'Custo' in df.columns:
            # Controles de visualização, período e download
            col_viz1, col_viz2, col_viz3 = st.columns([1.2, 1.5, 0.8])
            with col_viz1:
                modo_visualizacao = st.radio(
                    "📊 **Visualização:**",
                    ["Fixo/Variável", "Total"],
                    index=0,
                    horizontal=True,
                    key="flex_modo_visualizacao"
                )
            with col_viz2:
                _sel_per_flex = st.multiselect(
                    "📅 **Período(s):**",
                    ["Todos"] + _periodos_flex_all,
                    default=["Todos"],
                    key="flex_periodo"
                )
                periodos_flex = (
                    _periodos_flex_all if "Todos" in _sel_per_flex
                    else [x for x in _sel_per_flex if x != "Todos"]
                )
                if not periodos_flex:
                    periodos_flex = _periodos_flex_all
            with col_viz3:
                btn_excel = st.button(
                    "📥 Baixar Excel",
                    key="flex_download_excel",
                    use_container_width=True
                )
            st.markdown("---")
            # ── BUD: agrupar do Budget (tem todos os meses) ──
            df_bud_cat = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
            df_bud_cat['Custo_str'] = df_bud_cat['Custo'].astype(str).str.lower()
            df_bud_cat['Categoria'] = df_bud_cat['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )
            df_bud_cat_agg = df_bud_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})
            df_bud_cat_agg['Período'] = df_bud_cat_agg['Período'].astype(str)
            df_bud_cat_agg = ordenar_por_mes(df_bud_cat_agg)

            # ── Real: agrupar do Real (pode ter menos meses) ──
            df_real_cat = df[df['Período'].isin(periodos_flex)].copy()
            if not df_real_cat.empty and 'Custo' in df_real_cat.columns:
                df_real_cat['Custo_str'] = df_real_cat['Custo'].astype(str).str.lower()
                df_real_cat['Categoria'] = df_real_cat['Custo_str'].apply(
                    lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
                )
                df_real_cat_agg = df_real_cat.groupby(
                    ['Categoria', 'Período'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
                df_real_cat_agg['Período'] = df_real_cat_agg['Período'].astype(str)
            else:
                df_real_cat_agg = pd.DataFrame(columns=['Categoria', 'Período', 'Total'])

            # Preparar dados de volume para cálculo de Flex
            if df_vol_bud is not None and df_vol_actual is not None:
                df_vol_bud_norm = normalizar_periodo(df_vol_bud.copy())
                df_vol_bud_norm = df_vol_bud_norm[
                    df_vol_bud_norm['Período'].isin(periodos_flex)
                ].copy()
                df_vol_act_norm = normalizar_periodo(df_vol_actual.copy())
                df_vol_act_norm = df_vol_act_norm[
                    df_vol_act_norm['Período'].isin(periodos_flex)
                ].copy()
                vol_total_budget = df_vol_bud_norm['Volume'].sum()
                vol_total_actual = df_vol_act_norm['Volume'].sum()
                proporcao_global = (vol_total_actual / vol_total_budget) if vol_total_budget > 0 else 1
            else:
                proporcao_global = 1

            # Merge: BUD como base (ano completo), Real onde disponível
            df_cat_agg = df_bud_cat_agg.merge(
                df_real_cat_agg, on=['Categoria', 'Período'], how='left'
            )
            df_cat_agg['Total'] = df_cat_agg['Total'].fillna(0)

            # Calcular Flex BUD usando proporção GLOBAL
            df_cat_agg['Flex BUD'] = df_cat_agg.apply(
                lambda r: r['BUD'] if r['Categoria'] == 'Fixo'
                else r['BUD'] * proporcao_global,
                axis=1
            )

            # Aplicar CPU se necessário
            if tipo == 'CPU (Custo por Unidade)':
                if 'vol_total_budget' in locals() and 'vol_total_actual' in locals():
                    df_cat_agg['Total'] = calcular_cpu(
                        df_cat_agg['Total'], vol_total_actual
                    )
                    df_cat_agg['BUD'] = calcular_cpu(
                        df_cat_agg['BUD'], vol_total_budget
                    )
                    df_cat_agg['Flex BUD'] = calcular_cpu(
                        df_cat_agg['Flex BUD'], vol_total_actual
                    )

            # Calcular diferenças
            df_cat_agg['Flex Bud - BUD'] = (
                df_cat_agg['Flex BUD'] - df_cat_agg['BUD']
            )
            df_cat_agg['Total - Flex Bud'] = (
                df_cat_agg['Total'] - df_cat_agg['Flex BUD']
            )
            df_cat_agg['Total / Flex Bud'] = df_cat_agg.apply(
                lambda r: (r['Total'] / r['Flex BUD'])
                if r['Flex BUD'] != 0 else 0,
                axis=1
            )

            # ═══════════════════════════════════════
            # 📊 Resumo Geral
            # ═══════════════════════════════════════
            st.markdown("### 📊 Resumo Geral")

            # Calcular totais
            total_bud = df_cat_agg['BUD'].sum()
            total_flex_bud = df_cat_agg['Flex BUD'].sum()
            total_real = df_cat_agg['Total'].sum()
            total_flex_diff = total_flex_bud - total_bud
            total_real_diff = total_real - total_flex_bud
            total_ratio = (total_real / total_flex_bud) if total_flex_bud != 0 else 0

            # KPIs de Resumo - 6 em linha única
            kr1, kr2, kr3, kr4, kr5, kr6 = st.columns(6)
            with kr1:
                render_kpi("BUD", f"{simbolo} {total_bud:,.2f}{sufixo}")
            with kr2:
                render_kpi("Flex - BUD", f"{simbolo} {total_flex_diff:+,.2f}{sufixo}")
            with kr3:
                render_kpi("Flex BUD", f"{simbolo} {total_flex_bud:,.2f}{sufixo}")
            with kr4:
                render_kpi("Total - Flex", f"{simbolo} {total_real_diff:+,.2f}{sufixo}")
            with kr5:
                render_kpi(
                    "Best Estimate" if _usar_be_t1 else "Total Real",
                    f"{simbolo} {total_real:,.2f}{sufixo}"
                )
            with kr6:
                render_kpi("Total / Flex", f"{total_ratio:.0%}")

            render_kpi_spacer()
            st.markdown("---")

            # ═══════════════════════════════════════
            # 📥 Exportar para Excel (se botão clicado)
            # ═══════════════════════════════════════
            if btn_excel:
                try:
                    # Preparar DataFrame para download
                    df_download = df_cat_agg[['Categoria', 'Período', 'BUD',
                                              'Flex Bud - BUD', 'Flex BUD',
                                              'Total - Flex Bud', 'Total',
                                              'Total / Flex Bud']].copy()
                    # Formatar ratio como percentual
                    df_download['Total / Flex Bud'] = df_download['Total / Flex Bud'].apply(
                        lambda x: f"{x:.2%}"
                    )

                    # Salvar na pasta Downloads
                    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                    tipo_nome = "CPU" if tipo == "CPU (Custo por Unidade)" else "Custo_Total"
                    modo_nome = "Fixo_Variavel" if modo_visualizacao == "Fixo/Variável" else "Total"
                    file_name = f"TC_Principal_Flex_{modo_nome}_{tipo_nome}_{ano}.xlsx"
                    file_path = os.path.join(downloads_path, file_name)

                    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                        df_download.to_excel(writer, index=False, sheet_name='Flex_Bud')

                    st.success(f"✅ Arquivo salvo em: {file_path}")
                except Exception as e:
                    st.error(f"❌ Erro ao exportar: {e}")

            def _preparar_tabela_flex_hierarquia(
                df_base: pd.DataFrame,
                coluna_id: str,
            ) -> pd.DataFrame:
                colunas_tabela = [
                    col for col in [
                        coluna_id,
                        'BUD',
                        'Flex Bud - BUD',
                        'Flex BUD',
                        'Total - Flex Bud',
                        'Total',
                        'Total / Flex Bud',
                    ]
                    if col in df_base.columns
                ]
                if not colunas_tabela:
                    return pd.DataFrame()

                df_tabela = df_base[colunas_tabela].copy()
                for col in ['BUD', 'Flex BUD', 'Total']:
                    if col not in df_tabela.columns:
                        df_tabela[col] = 0.0

                df_tabela = df_tabela[
                    (df_tabela['Total'].abs() > 0.01)
                    | (df_tabela['BUD'].abs() > 0.01)
                    | (df_tabela['Flex BUD'].abs() > 0.01)
                ].copy()
                return df_tabela

            # ═══════════════════════════════════════
            # Expanders 💰 Fixo e 💰 Variável com hierarquia Type 05 → Type 06 → Account
            # ═══════════════════════════════════════
            expand_state_key = 'home_tc_flex_expand_all'
            if expand_state_key not in st.session_state:
                st.session_state[expand_state_key] = False

            ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 3])
            with ctrl_col1:
                if st.button('Expandir tudo', key='home_tc_expandir_flex'):
                    st.session_state[expand_state_key] = True
            with ctrl_col2:
                if st.button('Recolher tudo', key='home_tc_recolher_flex'):
                    st.session_state[expand_state_key] = False
            with ctrl_col3:
                st.caption('Controle aplicado aos expanders desta tabela Flex.')

            expandir_flex = st.session_state[expand_state_key]

            # Mostrar expanders apenas se visualização for Fixo/Variável
            if modo_visualizacao == "Fixo/Variável":
                # ── BUD: hierarquia do Budget (todos os accounts) ──
                df_bud_hier_base = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
                df_bud_hier_base['Custo_str'] = df_bud_hier_base['Custo'].astype(str).str.lower()
                df_bud_hier_base['Categoria'] = df_bud_hier_base['Custo_str'].apply(
                    lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
                )
                if 'Type 05' not in df_bud_hier_base.columns:
                    df_bud_hier_base['Type 05'] = 'N/A'
                if 'Type 06' not in df_bud_hier_base.columns:
                    df_bud_hier_base['Type 06'] = 'N/A'
                if 'Account' not in df_bud_hier_base.columns:
                    df_bud_hier_base['Account'] = 'N/A'
                df_bud_hier = df_bud_hier_base.groupby(
                    ['Categoria', 'Type 05', 'Type 06', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

                # ── Real: hierarquia do Real (pode ter menos accounts) ──
                df_real_hier = df[df['Período'].isin(periodos_flex)].copy()
                if not df_real_hier.empty and 'Custo' in df_real_hier.columns:
                    df_real_hier['Custo_str'] = df_real_hier['Custo'].astype(str).str.lower()
                    df_real_hier['Categoria'] = df_real_hier['Custo_str'].apply(
                        lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
                    )
                    if 'Type 05' not in df_real_hier.columns:
                        df_real_hier['Type 05'] = 'N/A'
                    if 'Type 06' not in df_real_hier.columns:
                        df_real_hier['Type 06'] = 'N/A'
                    if 'Account' not in df_real_hier.columns:
                        df_real_hier['Account'] = 'N/A'
                    df_real_hier_agg = df_real_hier.groupby(
                        ['Categoria', 'Type 05', 'Type 06', 'Account'], as_index=False
                    )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
                else:
                    df_real_hier_agg = pd.DataFrame(
                        columns=['Categoria', 'Type 05', 'Type 06', 'Account', 'Total']
                    )

                # Merge com volumes para cálculo de Flex (filtrados por período)
                if df_vol_bud is not None:
                    df_vol_bud_filt = normalizar_periodo(df_vol_bud.copy())
                    df_vol_bud_filt = df_vol_bud_filt[
                        df_vol_bud_filt['Período'].isin(periodos_flex)
                    ].copy()
                    vol_total_bud = df_vol_bud_filt['Volume'].sum()
                else:
                    vol_total_bud = 1
                    
                if df_vol_actual is not None:
                    df_vol_act_filt = normalizar_periodo(df_vol_actual.copy())
                    df_vol_act_filt = df_vol_act_filt[
                        df_vol_act_filt['Período'].isin(periodos_flex)
                    ].copy()
                    vol_total_act = df_vol_act_filt['Volume'].sum()
                else:
                    vol_total_act = vol_total_bud
                proporcao_global = vol_total_act / vol_total_bud if vol_total_bud > 0 else 1

                # Merge: BUD como base, Real onde disponível
                df_hier_agg = df_bud_hier.merge(
                    df_real_hier_agg,
                    on=['Categoria', 'Type 05', 'Type 06', 'Account'],
                    how='left'
                )
                df_hier_agg['Total'] = df_hier_agg['Total'].fillna(0)

                # Calcular Flex BUD (Fixo = BUD, Variável = BUD * Proporção)
                df_hier_agg['Flex BUD'] = df_hier_agg.apply(
                    lambda r: r['BUD'] if r['Categoria'] == 'Fixo'
                    else r['BUD'] * proporcao_global,
                    axis=1
                )

                # Aplicar CPU se necessário
                if tipo == 'CPU (Custo por Unidade)':
                    df_hier_agg['Total'] = calcular_cpu(
                        df_hier_agg['Total'], vol_total_act
                    )
                    df_hier_agg['BUD'] = calcular_cpu(
                        df_hier_agg['BUD'], vol_total_bud
                    )
                    df_hier_agg['Flex BUD'] = calcular_cpu(
                        df_hier_agg['Flex BUD'], vol_total_act
                    )

                # Calcular diferenças e ratio
                df_hier_agg['Flex Bud - BUD'] = df_hier_agg['Flex BUD'] - df_hier_agg['BUD']
                df_hier_agg['Total - Flex Bud'] = df_hier_agg['Total'] - df_hier_agg['Flex BUD']
                df_hier_agg['Total / Flex Bud'] = df_hier_agg.apply(
                    lambda r: r['Total'] / r['Flex BUD'] if r['Flex BUD'] != 0 else 0,
                    axis=1
                )

                for categoria in ['Fixo', 'Variável']:
                    df_cat_hier = df_hier_agg[
                        df_hier_agg['Categoria'] == categoria
                    ].copy()

                    if len(df_cat_hier) == 0:
                        continue

                    # Totais da categoria
                    cat_bud = df_cat_hier['BUD'].sum()
                    cat_flex = df_cat_hier['Flex BUD'].sum()
                    cat_total = df_cat_hier['Total'].sum()
                    cat_flex_diff = cat_flex - cat_bud
                    cat_real_diff = cat_total - cat_flex
                    cat_ratio = cat_total / cat_flex if cat_flex != 0 else 0
                    total_cat_fmt = f"{simbolo} {cat_total:,.2f}{sufixo}"

                    with st.expander(
                        f"💰 {categoria} - Total: {total_cat_fmt}",
                        expanded=expandir_flex
                    ):
                        # KPIs da categoria - 6 em linha única
                        ck1, ck2, ck3, ck4, ck5, ck6 = st.columns(6)
                        with ck1:
                            render_kpi("BUD", f"{simbolo} {cat_bud:,.2f}{sufixo}")
                        with ck2:
                            render_kpi("Flex - BUD", f"{simbolo} {cat_flex_diff:+,.2f}{sufixo}")
                        with ck3:
                            render_kpi("Flex BUD", f"{simbolo} {cat_flex:,.2f}{sufixo}")
                        with ck4:
                            render_kpi("Total - Flex", f"{simbolo} {cat_real_diff:+,.2f}{sufixo}")
                        with ck5:
                            render_kpi("Total", f"{simbolo} {cat_total:,.2f}{sufixo}")
                        with ck6:
                            render_kpi("Total / Flex", f"{cat_ratio:.0%}")

                        render_kpi_spacer()

                        # Sub-expanders por Type 05
                        type05_list = df_cat_hier['Type 05'].unique()
                        for type05 in type05_list:
                            df_type05 = df_cat_hier[
                                df_cat_hier['Type 05'] == type05
                            ].copy()

                            # Totais do Type 05
                            t05_bud = df_type05['BUD'].sum()
                            t05_flex = df_type05['Flex BUD'].sum()
                            t05_total = df_type05['Total'].sum()
                            t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                            with st.expander(
                                f"📊 Type 05: {type05} - Total: {t05_fmt}",
                                expanded=expandir_flex
                            ):
                                type06_list = df_type05['Type 06'].unique()
                                exibiu_type06 = False
                                for type06 in type06_list:
                                    df_type06 = df_type05[
                                        df_type05['Type 06'] == type06
                                    ].copy()
                                    df_tabela = _preparar_tabela_flex_hierarquia(
                                        df_type06,
                                        'Account',
                                    )

                                    if len(df_tabela) == 0:
                                        continue

                                    exibiu_type06 = True
                                    t06_total = df_type06['Total'].sum()
                                    t06_fmt = f"{simbolo} {t06_total:,.2f}{sufixo}"

                                    with st.expander(
                                        f"📑 Type 06: {type06} - Total: {t06_fmt}",
                                        expanded=expandir_flex,
                                    ):
                                        st.caption(f"Total do Type 06: {t06_fmt}")
                                        html_tabela = criar_tabela_html_flex(
                                            df_tabela, simbolo, sufixo
                                        )
                                        st.markdown(html_tabela, unsafe_allow_html=True)

                                if not exibiu_type06:
                                    st.info("Sem dados para exibir.")
            else:
                # Modo Total: expanders direto por Type 05 → Type 06 → Account (sem Fixo/Variável)
                # ── BUD: agrupar do Budget (todos os accounts) ──
                df_bud_total_base = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
                if 'Type 05' not in df_bud_total_base.columns:
                    df_bud_total_base['Type 05'] = 'N/A'
                if 'Type 06' not in df_bud_total_base.columns:
                    df_bud_total_base['Type 06'] = 'N/A'
                if 'Account' not in df_bud_total_base.columns:
                    df_bud_total_base['Account'] = 'N/A'
                df_bud_total = df_bud_total_base.groupby(
                    ['Type 05', 'Type 06', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

                # ── Real: agrupar do Real (pode ter menos accounts) ──
                df_real_total = df[df['Período'].isin(periodos_flex)].copy()
                if 'Type 05' not in df_real_total.columns:
                    df_real_total['Type 05'] = 'N/A'
                if 'Type 06' not in df_real_total.columns:
                    df_real_total['Type 06'] = 'N/A'
                if 'Account' not in df_real_total.columns:
                    df_real_total['Account'] = 'N/A'
                df_real_total_agg = df_real_total.groupby(
                    ['Type 05', 'Type 06', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})

                # Merge com volumes para cálculo de Flex (filtrados por período)
                if df_vol_bud is not None:
                    df_vol_bud_filt = normalizar_periodo(df_vol_bud.copy())
                    df_vol_bud_filt = df_vol_bud_filt[
                        df_vol_bud_filt['Período'].isin(periodos_flex)
                    ].copy()
                    vol_total_bud = df_vol_bud_filt['Volume'].sum()
                else:
                    vol_total_bud = 1
                    
                if df_vol_actual is not None:
                    df_vol_act_filt = normalizar_periodo(df_vol_actual.copy())
                    df_vol_act_filt = df_vol_act_filt[
                        df_vol_act_filt['Período'].isin(periodos_flex)
                    ].copy()
                    vol_total_act = df_vol_act_filt['Volume'].sum()
                else:
                    vol_total_act = vol_total_bud
                proporcao_global = (vol_total_act / vol_total_bud
                                   if vol_total_bud > 0 else 1)

                # Merge: BUD como base, Real onde disponível
                df_total_agg = df_bud_total.merge(
                    df_real_total_agg,
                    on=['Type 05', 'Type 06', 'Account'],
                    how='left'
                )
                df_total_agg['Total'] = df_total_agg['Total'].fillna(0)

                # Flex BUD (média de Fixo e Variável = BUD * proporcao parcial)
                df_total_agg['Flex BUD'] = df_total_agg['BUD'] * proporcao_global

                # Aplicar CPU se necessário
                if tipo == 'CPU (Custo por Unidade)':
                    df_total_agg['Total'] = calcular_cpu(
                        df_total_agg['Total'], vol_total_act
                    )
                    df_total_agg['BUD'] = calcular_cpu(
                        df_total_agg['BUD'], vol_total_bud
                    )
                    df_total_agg['Flex BUD'] = calcular_cpu(
                        df_total_agg['Flex BUD'], vol_total_act
                    )

                # Calcular diferenças e ratio
                df_total_agg['Flex Bud - BUD'] = (
                    df_total_agg['Flex BUD'] - df_total_agg['BUD']
                )
                df_total_agg['Total - Flex Bud'] = (
                    df_total_agg['Total'] - df_total_agg['Flex BUD']
                )
                df_total_agg['Total / Flex Bud'] = df_total_agg.apply(
                    lambda r: r['Total'] / r['Flex BUD']
                    if r['Flex BUD'] != 0 else 0,
                    axis=1
                )

                # Expanders por Type 05 (diretamente, sem Fixo/Variável)
                type05_list = df_total_agg['Type 05'].unique()
                for type05 in type05_list:
                    df_type05 = df_total_agg[
                        df_total_agg['Type 05'] == type05
                    ].copy()

                    # Filtrar linhas zeradas/nulas
                    df_type05 = df_type05[
                        (df_type05['Total'].abs() > 0.01) |
                        (df_type05['BUD'].abs() > 0.01)
                    ].copy()

                    if len(df_type05) == 0:
                        continue

                    # Totais do Type 05
                    t05_total = df_type05['Total'].sum()
                    t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                    with st.expander(
                        f"📊 Type 05: {type05} - Total: {t05_fmt}",
                        expanded=expandir_flex
                    ):
                        # KPIs do Type 05
                        t05_bud = df_type05['BUD'].sum()
                        t05_flex = df_type05['Flex BUD'].sum()
                        t05_flex_diff = t05_flex - t05_bud
                        t05_real_diff = t05_total - t05_flex
                        t05_ratio = t05_total / t05_flex if t05_flex != 0 else 0

                        tk1, tk2, tk3, tk4, tk5, tk6 = st.columns(6)
                        with tk1:
                            render_kpi("BUD", f"{simbolo} {t05_bud:,.2f}{sufixo}")
                        with tk2:
                            render_kpi("Flex-BUD", f"{simbolo} {t05_flex_diff:+,.2f}{sufixo}")
                        with tk3:
                            render_kpi("Flex BUD", f"{simbolo} {t05_flex:,.2f}{sufixo}")
                        with tk4:
                            render_kpi("Total-Flex", f"{simbolo} {t05_real_diff:+,.2f}{sufixo}")
                        with tk5:
                            render_kpi("Total", f"{simbolo} {t05_total:,.2f}{sufixo}")
                        with tk6:
                            render_kpi("Total/Flex", f"{t05_ratio:.0%}")

                        render_kpi_spacer()

                        type06_list = df_type05['Type 06'].unique()
                        exibiu_type06 = False
                        for type06 in type06_list:
                            df_type06 = df_type05[
                                df_type05['Type 06'] == type06
                            ].copy()
                            df_tabela = _preparar_tabela_flex_hierarquia(
                                df_type06,
                                'Account',
                            )

                            if len(df_tabela) == 0:
                                continue

                            exibiu_type06 = True
                            t06_total = df_type06['Total'].sum()
                            t06_fmt = f"{simbolo} {t06_total:,.2f}{sufixo}"

                            with st.expander(
                                f"📑 Type 06: {type06} - Total: {t06_fmt}",
                                expanded=expandir_flex,
                            ):
                                st.caption(f"Total do Type 06: {t06_fmt}")
                                html_tabela = criar_tabela_html_flex(
                                    df_tabela, simbolo, sufixo
                                )
                                st.markdown(html_tabela, unsafe_allow_html=True)

                        if not exibiu_type06:
                            st.info("Sem dados para exibir.")

        else:
            st.info(
                "ℹ️ Dados de categoria (Custo) não disponíveis para "
                "análise Flex."
            )

        # ════════════════════════════════════════
        # 🔍 VALIDAÇÃO DE CONSISTÊNCIA COMPLETA
        # ════════════════════════════════════════
        st.markdown("---")
        with st.expander("🔍 Validação de Consistência de Dados (Todos os Datasets)", expanded=False):
            st.markdown("""
            **Objetivo:** Garantir a confiabilidade do projeto comparando **TODOS** os datasets:
            - 📋 **Dados Detalhados** (Tab 6 - valores diretos)
            - 📊 **Agregação por Período** (como aparecem nos gráficos)
            
            **Validando:** Budget, Flex Budget, Real, Best Estimate (Total e Por Veículo)
            """)
            
            try:
                # ══════════════════════════════════════════════
                # FUNÇÃO AUXILIAR: Validar consistência dataset
                # ══════════════════════════════════════════════
                def _validar_dataset(df_raw, col_valor, nome_dataset, icone):
                    """Valida se total direto = total agregado por período"""
                    if df_raw is None or df_raw.empty or col_valor not in df_raw.columns:
                        return None
                    
                    # Total direto (como no Tab 6) - arredondado para 2 casas
                    total_direto = round(float(df_raw[col_valor].sum()), 2)
                    
                    # Agregar por período (como no gráfico)
                    tem_ano = 'Ano' in df_raw.columns
                    grp_cols = ['Ano', 'Período'] if tem_ano else ['Período']
                    
                    df_periodo_agg = df_raw.groupby(grp_cols, as_index=False)[col_valor].sum()
                    total_periodo = round(float(df_periodo_agg[col_valor].sum()), 2)
                    
                    # Diferença em valor absoluto (arredondado para 2 casas)
                    diff_valor = round(total_periodo - total_direto, 2)
                    
                    # Calcular diferença percentual
                    if total_direto > 0:
                        diff_perc = round(((total_periodo - total_direto) / total_direto) * 100, 2)
                    else:
                        diff_perc = 0.0
                    
                    # Status baseado na diferença em valor (mais preciso após arredondamento)
                    if abs(diff_valor) < 0.01:
                        status = "✅"
                        status_text = "OK"
                    elif abs(diff_perc) < 1.0:
                        status = "⚠️"
                        status_text = "Pequena dif."
                    else:
                        status = "❌"
                        status_text = "INCONSISTENTE"
                    
                    return {
                        'icone': icone,
                        'nome': nome_dataset,
                        'total_direto': total_direto,
                        'total_periodo': total_periodo,
                        'diff_valor': diff_valor,
                        'diff_perc': diff_perc,
                        'status': status,
                        'status_text': status_text,
                    }
                
                # ══════════════════════════════════════════════
                # PREPARAR DADOS (sem filtros, com fator/moeda)
                # ══════════════════════════════════════════════
                _cols_mon = [c for c in COLUNAS_MONETARIAS if c in df_principal.columns]
                
                # 1. Budget Total
                _df_bud_val = normalizar_periodo(_raw_df_principal.copy())
                _df_bud_val = aplicar_fator_df(_df_bud_val, _cols_mon, fator)
                _df_bud_val = converter_moeda_df(_df_bud_val, _cols_mon, moeda, taxas)
                
                # 2. Flex Budget Total
                # NOTA: df_flex já vem com fator/moeda aplicados (calculado a partir de df convertido)
                # NÃO reaplicar fator/moeda para evitar dupla conversão
                _df_flex_val = None
                if df_flex is not None and not df_flex.empty:
                    _df_flex_val = normalizar_periodo(df_flex.copy())
                    # Flex_Bud já está na escala correta
                
                # 3. Real Total
                _df_real_val = None
                if _raw_df_real is not None and not _raw_df_real.empty:
                    _df_real_val = normalizar_periodo(_raw_df_real.copy())
                    _cols_r = [c for c in COLUNAS_MONETARIAS if c in _df_real_val.columns]
                    _df_real_val = aplicar_fator_df(_df_real_val, _cols_r, fator)
                    _df_real_val = converter_moeda_df(_df_real_val, _cols_r, moeda, taxas)
                
                # 4. Best Estimate Total
                _df_be_val = None
                if _raw_df_be is not None and not _raw_df_be.empty:
                    _df_be_val = normalizar_periodo(_raw_df_be.copy())
                    _cols_be = [c for c in COLUNAS_MONETARIAS if c in _df_be_val.columns]
                    _df_be_val = aplicar_fator_df(_df_be_val, _cols_be, fator)
                    _df_be_val = converter_moeda_df(_df_be_val, _cols_be, moeda, taxas)
                
                # 5. Budget Por Veículo
                _df_vbud_val = None
                if df_veic_bud_raw is not None and not df_veic_bud_raw.empty:
                    _df_vbud_val = normalizar_periodo(df_veic_bud_raw.copy())
                    if 'Custo FP Veiculo' in _df_vbud_val.columns:
                        _df_vbud_val['Custo FP'] = _df_vbud_val['Custo FP Veiculo']
                    _cols_vb = [c for c in COLUNAS_MONETARIAS if c in _df_vbud_val.columns]
                    _df_vbud_val = aplicar_fator_df(_df_vbud_val, _cols_vb, fator)
                    _df_vbud_val = converter_moeda_df(_df_vbud_val, _cols_vb, moeda, taxas)
                
                # 6. Flex Budget Por Veículo (calcular a partir de Budget Por Veículo)
                _df_vflex_val = None
                if _df_vbud_val is not None and df_vol_bud is not None:
                    try:
                        _df_vflex_val = calcular_flex_budget_detalhado(
                            _df_vbud_val, df_vol_bud.copy(), df_vol_actual.copy() if df_vol_actual is not None else None
                        )
                        if _df_vflex_val is not None and 'Flex_Bud' in _df_vflex_val.columns:
                            # Já está com fator/moeda aplicados
                            pass
                    except Exception:
                        _df_vflex_val = None
                
                # 7. Real Por Veículo
                _df_vreal_val = None
                if df_veic_real_raw is not None and not df_veic_real_raw.empty:
                    _df_vreal_val = normalizar_periodo(df_veic_real_raw.copy())
                    if 'Custo FP Veiculo' in _df_vreal_val.columns:
                        _df_vreal_val['Custo FP'] = _df_vreal_val['Custo FP Veiculo']
                    _cols_vr = [c for c in COLUNAS_MONETARIAS if c in _df_vreal_val.columns]
                    _df_vreal_val = aplicar_fator_df(_df_vreal_val, _cols_vr, fator)
                    _df_vreal_val = converter_moeda_df(_df_vreal_val, _cols_vr, moeda, taxas)
                
                # 8. Best Estimate Por Veículo (prioridade: arquivo pré-gerado)
                _df_vbe_val = None
                
                # Prioridade: usar arquivo pré-gerado (igual Budget/Real)
                if df_veic_be_raw is not None and not df_veic_be_raw.empty:
                    try:
                        _df_vbe_val = normalizar_periodo(df_veic_be_raw.copy())
                        # Validacao cruzada precisa comparar o mesmo universo do
                        # forecast total. Nao aplicar filtros de tela aqui, senao
                        # o total anual passa a ser comparado com um recorte local.
                        if 'Ano' in _df_vbe_val.columns:
                            _df_vbe_val = _df_vbe_val[
                                _df_vbe_val['Ano'] == int(ano)
                            ].copy()
                        if _df_vbe_val is not None and 'Custo FP Veiculo' in _df_vbe_val.columns:
                            _df_vbe_val['Custo FP'] = _df_vbe_val['Custo FP Veiculo']
                        _cols_vbe = [c for c in COLUNAS_MONETARIAS if c in _df_vbe_val.columns]
                        _df_vbe_val = aplicar_fator_df(_df_vbe_val, _cols_vbe, fator)
                        _df_vbe_val = converter_moeda_df(_df_vbe_val, _cols_vbe, moeda, taxas)
                    except Exception:
                        _df_vbe_val = None
                
                # Fallback: ratear em runtime se arquivo não existe (mesma lógica do Real)
                if _df_vbe_val is None and _df_be_val is not None:
                    try:
                        _pct_rateio = load_percentual_rateio_veiculos_real(ano)
                        _dea_rateio = load_dea_dedicado_real(ano)
                        if _pct_rateio is not None:
                            _df_vbe_val = ratear_be_por_veiculo(_df_be_val, _pct_rateio, df_dea=_dea_rateio)
                            if _df_vbe_val is not None and 'Custo FP Veiculo' in _df_vbe_val.columns:
                                _df_vbe_val['Custo FP'] = _df_vbe_val['Custo FP Veiculo']
                    except Exception:
                        _df_vbe_val = None
                
                # ══════════════════════════════════════════════
                # FUNÇÃO AUXILIAR: Validar Total vs Por Veículo
                # ══════════════════════════════════════════════
                def _validar_total_vs_veiculo(df_total, df_veic, col_valor, nome, icone):
                    """Valida se Total == Soma(Por Veículo) para detectar erros de escala"""
                    if df_total is None or df_total.empty or col_valor not in df_total.columns:
                        return None
                    if df_veic is None or df_veic.empty or col_valor not in df_veic.columns:
                        return None
                    
                    total_val = round(float(df_total[col_valor].sum()), 2)
                    veic_val = round(float(df_veic[col_valor].sum()), 2)
                    
                    diff_valor = round(veic_val - total_val, 2)
                    
                    if total_val > 0:
                        diff_perc = round(((veic_val - total_val) / total_val) * 100, 2)
                    else:
                        diff_perc = 0.0
                    
                    # Detectar erro de escala (ex: K vs M = 1000x diferença)
                    if total_val > 0 and veic_val > 0:
                        ratio = veic_val / total_val
                        if ratio > 500:  # Por Veículo é 500x+ maior que Total
                            return {
                                'icone': icone, 'nome': f'{nome}: ESCALA ERRADA',
                                'total_direto': total_val, 'total_periodo': veic_val,
                                'diff_valor': diff_valor, 'diff_perc': diff_perc,
                                'status': "❌", 'status_text': "ESCALA!",
                            }
                        elif ratio < 0.002:  # Por Veículo é 500x+ menor que Total
                            return {
                                'icone': icone, 'nome': f'{nome}: ESCALA ERRADA',
                                'total_direto': total_val, 'total_periodo': veic_val,
                                'diff_valor': diff_valor, 'diff_perc': diff_perc,
                                'status': "❌", 'status_text': "ESCALA!",
                            }
                    
                    if abs(diff_valor) < 0.01:
                        status, status_text = "✅", "OK"
                    elif abs(diff_perc) < 1.0:
                        status, status_text = "⚠️", "Pequena dif."
                    else:
                        status, status_text = "❌", "INCONSISTENTE"
                    
                    return {
                        'icone': icone, 'nome': nome,
                        'total_direto': total_val, 'total_periodo': veic_val,
                        'diff_valor': diff_valor, 'diff_perc': diff_perc,
                        'status': status, 'status_text': status_text,
                    }
                
                # ══════════════════════════════════════════════
                # EXECUTAR VALIDAÇÕES
                # ══════════════════════════════════════════════
                resultados = []
                resultados_cruzados = []
                
                # Validações internas (Total Direto vs Agregado)
                for df_val, col, nome, icone in [
                    (_df_bud_val, 'Custo FP', 'Budget', '💰'),
                    (_df_flex_val, 'Flex_Bud', 'Flex Budget', '📐'),
                    (_df_real_val, 'Custo FP', 'Real', '✅'),
                    (_df_be_val, 'Custo FP', 'Best Estimate', '🔮'),
                ]:
                    r = _validar_dataset(df_val, col, nome, icone)
                    if r: resultados.append(r)
                
                # Validações cruzadas (Total vs Por Veículo)
                for df_t, df_v, col, nome, icone in [
                    (_df_bud_val, _df_vbud_val, 'Custo FP', 'Budget: Total vs Veículo', '🚗💰'),
                    (_df_flex_val, _df_vflex_val, 'Flex_Bud', 'Flex Budget: Total vs Veículo', '🚗📐'),
                    (_df_real_val, _df_vreal_val, 'Custo FP', 'Real: Total vs Veículo', '🚗✅'),
                    (_df_be_val, _df_vbe_val, 'Custo FP', 'Best Estimate: Total vs Veículo', '🚗🔮'),
                ]:
                    r = _validar_total_vs_veiculo(df_t, df_v, col, nome, icone)
                    if r: resultados_cruzados.append(r)
                
                # ══════════════════════════════════════════════
                # RENDERIZAR TABELA HTML
                # ══════════════════════════════════════════════
                # TABELA 1: Validações INTERNAS (Direto vs Agregado)
                # ══════════════════════════════════════════════
                if len(resultados) > 0:
                    st.markdown("#### 📊 Validação Interna (Direto vs Agregado)")
                    html = f"""
                    <style>
                    .validacao-table {{
                        width: 100%;
                        border-collapse: collapse;
                        font-family: 'Segoe UI', sans-serif;
                        font-size: 12px;
                        margin: 10px 0;
                    }}
                    .validacao-table th {{
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 10px 6px;
                        text-align: center;
                        font-weight: 600;
                        border-bottom: 2px solid #764ba2;
                        font-size: 11px;
                    }}
                    .validacao-table td {{
                        padding: 8px 6px;
                        border-bottom: 1px solid #e5e7eb;
                    }}
                    .validacao-table tr:hover {{
                        background-color: #f9fafb;
                    }}
                    .valor-num {{
                        text-align: right;
                        font-family: 'Consolas', monospace;
                        font-weight: 500;
                    }}
                    .status-ok {{ color: #10b981; font-weight: bold; }}
                    .status-warn {{ color: #f59e0b; font-weight: bold; }}
                    .status-erro {{ color: #ef4444; font-weight: bold; }}
                    .diff-positivo {{ color: #ef4444; }}
                    .diff-negativo {{ color: #10b981; }}
                    .diff-zero {{ color: #6b7280; }}
                    </style>
                    <table class="validacao-table">
                        <thead>
                            <tr>
                                <th style="width: 50px;">Status</th>
                                <th style="text-align: left;">Dataset</th>
                                <th>Total Direto</th>
                                <th>Total Agregado</th>
                                <th>Diferença ({simbolo})</th>
                                <th>Erro %</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    
                    for res in resultados:
                        status_class = "status-ok" if res['status'] == "✅" else ("status-warn" if res['status'] == "⚠️" else "status-erro")
                        
                        # Classe para diferença em valor
                        if abs(res['diff_valor']) < 0.01:
                            diff_class = "diff-zero"
                        elif res['diff_valor'] > 0:
                            diff_class = "diff-positivo"
                        else:
                            diff_class = "diff-negativo"
                        
                        html += f"""
                        <tr>
                            <td style="text-align: center; font-size: 16px;">{res['status']}</td>
                            <td style="text-align: left;"><strong>{res['icone']} {res['nome']}</strong></td>
                            <td class="valor-num">{simbolo}{res['total_direto']:,.2f}{sufixo}</td>
                            <td class="valor-num">{simbolo}{res['total_periodo']:,.2f}{sufixo}</td>
                            <td class="valor-num {diff_class}">{res['diff_valor']:+,.2f}{sufixo}</td>
                            <td class="valor-num {status_class}">{res['diff_perc']:+.2f}%</td>
                        </tr>
                        """
                    
                    html += """
                        </tbody>
                    </table>
                    """
                    
                    st.markdown(html, unsafe_allow_html=True)
                
                # ══════════════════════════════════════════════
                # TABELA 2: Validações CRUZADAS (Total vs Por Veículo)
                # ══════════════════════════════════════════════
                if len(resultados_cruzados) > 0:
                    st.markdown(
                        "#### 🔗 Validação Cruzada: Total Anual Consolidado vs Σ Veículos"
                    )
                    st.caption(
                        "Compara o universo anual consolidado, sem filtros de tela, "
                        "com a soma agregada por veículo."
                    )
                    
                    html2 = f"""
                    <table class="validacao-table">
                        <thead>
                            <tr>
                                <th style="width: 50px;">Status</th>
                                <th style="text-align: left;">Comparação</th>
                                <th>Total</th>
                                <th>Σ Veículos</th>
                                <th>Diferença ({simbolo})</th>
                                <th>Erro %</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    
                    for res in resultados_cruzados:
                        status_class = "status-ok" if res['status'] == "✅" else ("status-warn" if res['status'] == "⚠️" else "status-erro")
                        if abs(res['diff_valor']) < 0.01:
                            diff_class = "diff-zero"
                        elif res['diff_valor'] > 0:
                            diff_class = "diff-positivo"
                        else:
                            diff_class = "diff-negativo"
                        
                        html2 += f"""
                        <tr>
                            <td style="text-align: center; font-size: 16px;">{res['status']}</td>
                            <td style="text-align: left;"><strong>{res['icone']} {res['nome']}</strong></td>
                            <td class="valor-num">{simbolo}{res['total_direto']:,.2f}{sufixo}</td>
                            <td class="valor-num">{simbolo}{res['total_periodo']:,.2f}{sufixo}</td>
                            <td class="valor-num {diff_class}">{res['diff_valor']:+,.2f}{sufixo}</td>
                            <td class="valor-num {status_class}">{res['diff_perc']:+.2f}%</td>
                        </tr>
                        """
                    
                    html2 += """
                        </tbody>
                    </table>
                    """
                    
                    st.markdown(html2, unsafe_allow_html=True)
                    
                    # Resumo combinado
                    st.markdown("---")
                    all_results = resultados + resultados_cruzados
                    ok_count = sum(1 for r in all_results if r['status'] == "✅")
                    warn_count = sum(1 for r in all_results if r['status'] == "⚠️")
                    erro_count = sum(1 for r in all_results if r['status'] == "❌")
                    
                    col_s1, col_s2, col_s3 = st.columns(3)
                    with col_s1:
                        st.metric("✅ Consistentes", f"{ok_count}/{len(all_results)}")
                    with col_s2:
                        st.metric("⚠️ Pequenas Dif.", f"{warn_count}/{len(all_results)}")
                    with col_s3:
                        st.metric("❌ Inconsistentes", f"{erro_count}/{len(all_results)}")
                    
                    if erro_count == 0 and warn_count == 0:
                        st.success("🎉 **Todos os datasets estão consistentes!** A integridade dos dados está garantida.")
                    elif erro_count == 0:
                        st.info("ℹ️ Pequenas diferenças detectadas (< 1%) - provavelmente arredondamento.")
                    else:
                        st.error(f"⚠️ {erro_count} validação(ões) com erro - investigar processamento.")
                
                elif len(resultados) > 0:
                    # Só validações internas, sem cruzadas
                    st.markdown("---")
                    ok_count = sum(1 for r in resultados if r['status'] == "✅")
                    warn_count = sum(1 for r in resultados if r['status'] == "⚠️")
                    erro_count = sum(1 for r in resultados if r['status'] == "❌")
                    
                    col_s1, col_s2, col_s3 = st.columns(3)
                    with col_s1:
                        st.metric("✅ Consistentes", f"{ok_count}/{len(resultados)}")
                    with col_s2:
                        st.metric("⚠️ Pequenas Dif.", f"{warn_count}/{len(resultados)}")
                    with col_s3:
                        st.metric("❌ Inconsistentes", f"{erro_count}/{len(resultados)}")
                    
                    if erro_count == 0 and warn_count == 0:
                        st.success("🎉 **Todos os datasets estão consistentes!** A integridade dos dados está garantida.")
                    elif erro_count == 0:
                        st.info("ℹ️ Pequenas diferenças detectadas (< 1%) - provavelmente arredondamento.")
                    else:
                        st.error(f"⚠️ {erro_count} validação(ões) com erro - investigar processamento.")
                
                else:
                    st.warning("⚠️ Nenhum dataset disponível para validação.")
            
            except Exception as e:
                st.error(f"❌ Erro ao calcular validação: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

        # ════════════════════════════════════════
        # 🔍 VALIDAÇÃO EXCEL × SCI (Fontes vs Calculados)
        # ════════════════════════════════════════
        with st.expander("🔍 Validação de Consistência Excel × SCI (Fontes vs Calculados)", expanded=False):
            st.markdown("""
            <div style="padding: 0.8rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 8px; margin-bottom: 1rem; color: white;">
            <h4 style="color: white; margin: 0; font-size: 1rem;">🔍 Validação Automática: Excel (fonte) × Parquets (SCI)</h4>
            <p style="color: #f0f0f0; margin: 0.3rem 0 0 0; font-size: 0.85rem;">
                Confere se os dados calculados pelo SCI estão consistentes com os dados brutos do Excel
            </p>
            </div>
            """, unsafe_allow_html=True)

            # Caminho do Excel
            _excel_path_val = os.path.join(_ROOT, 'dados', 'TC_Principal', str(ano), 'Reporting veículos.xlsx')

            if not os.path.exists(_excel_path_val):
                st.warning(f"⚠️ Arquivo Excel não encontrado: `{_excel_path_val}`")
                st.info("Para executar esta validação, o arquivo 'Reporting veículos.xlsx' deve estar presente na pasta do ano.")
            else:
                st.success(f"✅ Excel encontrado: `Reporting veículos.xlsx` (Ano {ano})")

                if st.button("🔄 Executar Validação Excel × SCI", key="btn_val_excel_sci_home", use_container_width=True):
                    with st.spinner("Executando conferências..."):
                        # ── Budget ──
                        st.markdown("#### 📊 Budget")
                        df_conf_bud = executar_conferencias(ano, 'budget')
                        _ok_b = (df_conf_bud['Status'] == '✅').sum()
                        _total_b = len(df_conf_bud)
                        st.dataframe(df_conf_bud, use_container_width=True, hide_index=True)
                        if _ok_b == _total_b:
                            st.success(f"🎉 Budget: {_ok_b}/{_total_b} conferências OK")
                        else:
                            st.warning(f"⚠️ Budget: {_ok_b}/{_total_b} conferências OK")

                        st.markdown("---")

                        # ── Real ──
                        st.markdown("#### 📊 Real")
                        df_conf_real = executar_conferencias(ano, 'real')
                        _ok_r = (df_conf_real['Status'] == '✅').sum()
                        _total_r = len(df_conf_real)
                        st.dataframe(df_conf_real, use_container_width=True, hide_index=True)
                        if _ok_r == _total_r:
                            st.success(f"🎉 Real: {_ok_r}/{_total_r} conferências OK")
                        else:
                            st.warning(f"⚠️ Real: {_ok_r}/{_total_r} conferências OK")

            st.markdown("""
            ---
            **Legenda:** ✅ OK (< 0,01%) | ⚠️ Atenção (0,01% - 1%) | ❌ Divergência (> 1%)
            
            **Conferências:** Despesa Primária (fonte real), Redis, Volume FA, Custo FA/FP (BDG), Prova cruzada DP=FA+FP.
            """)

    # ── Restaurar estado global após tab1 ──
    df_bud = _save_df_bud
    df = _save_df
    df_vol_bud = _save_df_vol_bud
    df_vol_actual = _save_df_vol_actual
    df_flex = _save_df_flex
    cols_val = _save_cols_val
    vol_total = _save_vol_total
    tem_real = _save_tem_real

    # ── TAB 2: Volume ──
    with tab2:
        st.subheader("Volume de Produção")

        if df_vol_bud is not None:
            # Preparar dados Budget
            df_vb = normalizar_periodo(df_vol_bud.copy())
            df_vb = ordenar_por_mes(df_vb)
            df_vb['Período'] = df_vb['Período'].astype(str)

            # Preparar dados Actual (se existir)
            df_va = None
            if df_vol_actual is not None:
                df_va = normalizar_periodo(df_vol_actual.copy())
                df_va = ordenar_por_mes(df_va)
                df_va['Período'] = df_va['Período'].astype(str)

            # ─── Filtro de Veículo ───
            _veiculos_disponiveis = sorted(df_vb['Veículo'].dropna().unique().tolist())
            _filtro_veiculos = st.multiselect(
                "🚗 Filtrar por Veículo:",
                options=_veiculos_disponiveis,
                default=[],
                placeholder="Todos os veículos",
                key="filtro_vol_veiculo",
            )
            if _filtro_veiculos:
                df_vb = df_vb[df_vb['Veículo'].isin(_filtro_veiculos)]
                if df_va is not None:
                    df_va = df_va[df_va['Veículo'].isin(_filtro_veiculos)]

            vol_bud_total_tab2 = df_vb['Volume'].sum()
            vol_act_total_tab2 = vol_bud_total_tab2  # default
            if df_va is not None:
                vol_act_total_tab2 = df_va['Volume'].sum()

            proporcao_vol = (vol_act_total_tab2 / vol_bud_total_tab2) if vol_bud_total_tab2 > 0 else 1.0

            # ═══════════════════════════════════════
            # KPIs de Volume
            # ═══════════════════════════════════════
            kv1, kv2, kv3, kv4 = st.columns(4)
            with kv1:
                render_kpi("Vol Budget", f"{vol_bud_total_tab2:,.0f}")
            with kv2:
                render_kpi("Vol Actual", f"{vol_act_total_tab2:,.0f}")
            with kv3:
                render_kpi("Diferença", f"{vol_act_total_tab2 - vol_bud_total_tab2:+,.0f}")
            with kv4:
                render_kpi("Proporção", f"{proporcao_vol:.2%}")

            render_kpi_spacer()

            # ═══════════════════════════════════════
            # Gráfico 1: Volume Total por Período
            # ═══════════════════════════════════════
            st.markdown("### 📊 Volume Total por Período")

            # Agregar Budget por período
            df_vb_per = df_vb.groupby('Período', as_index=False)['Volume'].sum()
            df_vb_per['Tipo'] = 'Budget'
            ordem_per = [m for m in ORDEM_MESES if m in df_vb_per['Período'].values]

            # Barras de Budget com degradê verde (padrão TC Ext)
            bar_bud = alt.Chart(df_vb_per).mark_bar().encode(
                x=alt.X('Período:N', sort=ordem_per, title='Período',
                        axis=alt.Axis(grid=False, domain=True, ticks=True)),
                y=alt.Y('Volume:Q', title='Volume',
                        axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    'Volume:Q',
                    title='Volume',
                    scale=alt.Scale(scheme='greens'),
                    legend=alt.Legend(orient='right', titleFontSize=10, labelFontSize=9)
                ),
                tooltip=[
                    alt.Tooltip('Período:N', title='Período'),
                    alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
                ],
            )

            # Rótulos nas barras
            rotulos_bud = bar_bud.mark_text(
                align='center', dy=-10, fontSize=9, color='black'
            ).encode(text=alt.Text('Volume:Q', format=','))

            layers_vol = [bar_bud, rotulos_bud]

            # Linha Actual (se existir e for diferente de Budget)
            if df_va is not None:
                df_va_per = df_va.groupby('Período', as_index=False)['Volume'].sum()
                df_va_per['Tipo'] = 'Realizado'

                # Verificar se são diferentes
                vol_bud_total = df_vb_per['Volume'].sum()
                vol_act_total = df_va_per['Volume'].sum()
                sao_diferentes = abs(vol_bud_total - vol_act_total) > 1

                if sao_diferentes:
                    # Linha tracejada laranja para Realizado
                    line_act = alt.Chart(df_va_per).mark_line(
                        color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                    ).encode(
                        x=alt.X('Período:N', sort=ordem_per),
                        y='Volume:Q',
                        tooltip=[
                            alt.Tooltip('Período:N', title='Período'),
                            alt.Tooltip('Volume:Q', title='Volume Realizado', format=',')
                        ],
                    )
                    # Pontos na linha
                    pontos_act = alt.Chart(df_va_per).mark_circle(
                        color='#FF6B35', size=60
                    ).encode(
                        x=alt.X('Período:N', sort=ordem_per),
                        y='Volume:Q',
                    )
                    # Rótulos na linha
                    rotulos_act = alt.Chart(df_va_per).mark_text(
                        align='center', dy=-15, fontSize=9, color='#FF6B35',
                        fontWeight='bold'
                    ).encode(
                        x=alt.X('Período:N', sort=ordem_per),
                        y='Volume:Q',
                        text=alt.Text('Volume:Q', format=',')
                    )
                    layers_vol.extend([line_act, pontos_act, rotulos_act])

                    # Legenda manual
                    st.caption(
                        "📊 Barras com degradê verde = Volume Budget | "
                        "🟠 Linha tracejada = Volume Realizado"
                    )
                else:
                    st.info(
                        "ℹ️ Volume Budget e Realizado são idênticos. "
                        "Flex Budget = Budget neste cenário."
                    )

            chart_vol_per = (
                alt.layer(*layers_vol)
                .properties(height=400)
                .configure_axis(labelFontSize=11, titleFontSize=13)
            )
            st.altair_chart(chart_vol_per, use_container_width=True)

            # ═══════════════════════════════════════
            # Gráfico 2: Volume por Veículo (padrão TC Ext - degradê verde)
            # ═══════════════════════════════════════
            st.markdown("### 📊 Volume por Veículo")

            # Agregar por veículo (soma total) e ordenar por volume
            df_vb_total_veic = df_vb.groupby(
                'Veículo', as_index=False
            )['Volume'].sum().sort_values('Volume', ascending=False)
            ordem_veiculos = df_vb_total_veic['Veículo'].tolist()

            # Gráfico de barras com degradê verde (igual TC Ext)
            bar_veic = alt.Chart(df_vb_total_veic).mark_bar().encode(
                x=alt.X('Veículo:N', sort=ordem_veiculos, title='Veículo',
                        axis=alt.Axis(grid=False, domain=True, ticks=True)),
                y=alt.Y('Volume:Q', title='Volume (Unidades)',
                        axis=alt.Axis(grid=False)),
                color=alt.Color(
                    'Volume:Q',
                    title='Volume Budget',
                    scale=alt.Scale(scheme='greens'),
                    legend=alt.Legend(orient='right', titleFontSize=10, labelFontSize=9)
                ),
                tooltip=[
                    alt.Tooltip('Veículo:N', title='Veículo'),
                    alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
                ],
            ).properties(height=360)

            # Rótulos nas barras
            rotulos_veic = bar_veic.mark_text(
                align='center', dy=-10, fontSize=9, color='black'
            ).encode(text=alt.Text('Volume:Q', format=','))

            layers_veic = [bar_veic, rotulos_veic]

            # Adicionar linha BUD (se Volume Actual existir e for diferente)
            if df_va is not None:
                df_va_total_veic = df_va.groupby(
                    'Veículo', as_index=False
                )['Volume'].sum()
                # Verificar se são diferentes
                vol_veic_bud = df_vb_total_veic['Volume'].sum()
                vol_veic_act = df_va_total_veic['Volume'].sum()
                sao_diferentes_veic = abs(vol_veic_bud - vol_veic_act) > 1

                if sao_diferentes_veic:
                    # Linha tracejada laranja para Volume Actual
                    line_veic_act = alt.Chart(df_va_total_veic).mark_line(
                        color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                    ).encode(
                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                        y='Volume:Q',
                        tooltip=[
                            alt.Tooltip('Veículo:N', title='Veículo'),
                            alt.Tooltip('Volume:Q', title='Volume Realizado', format=',')
                        ],
                    )
                    # Pontos na linha
                    pontos_veic_act = alt.Chart(df_va_total_veic).mark_circle(
                        color='#FF6B35', size=60
                    ).encode(
                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                        y='Volume:Q',
                    )
                    layers_veic.extend([line_veic_act, pontos_veic_act])

                    # Legenda
                    st.caption(
                        "🟢 Barras com degradê verde = Volume Budget | "
                        "🟠 Linha tracejada = Volume Realizado"
                    )

            chart_veic = alt.layer(*layers_veic)
            st.altair_chart(chart_veic, use_container_width=True)

            # ═══════════════════════════════════════
            # Tabela Comparativa Budget vs Actual
            # ═══════════════════════════════════════
            with st.expander("📋 Tabela Budget vs Realizado por Período", expanded=False):
                # Montar tabela comparativa
                df_comp = df_vb_per[['Período', 'Volume']].rename(
                    columns={'Volume': 'Vol_Budget'}
                )
                if df_va is not None:
                    df_va_per_comp = df_va.groupby(
                        'Período', as_index=False
                    )['Volume'].sum().rename(columns={'Volume': 'Vol_Actual'})
                    df_comp = df_comp.merge(df_va_per_comp, on='Período', how='outer')
                    df_comp['Vol_Actual'] = df_comp['Vol_Actual'].fillna(0)
                    df_comp['Diferença'] = df_comp['Vol_Actual'] - df_comp['Vol_Budget']
                    df_comp['Proporção'] = (
                        df_comp['Vol_Actual'] / df_comp['Vol_Budget']
                    ).replace([float('inf'), float('-inf')], 0).fillna(1)
                else:
                    df_comp['Vol_Actual'] = df_comp['Vol_Budget']
                    df_comp['Diferença'] = 0
                    df_comp['Proporção'] = 1.0

                # Ordenar por mês
                df_comp['_ordem'] = df_comp['Período'].apply(
                    lambda x: ORDEM_MESES.index(x) if x in ORDEM_MESES else 99
                )
                df_comp = df_comp.sort_values('_ordem').drop(columns='_ordem')

                # Adicionar linha Total
                total_row = pd.DataFrame([{
                    'Período': 'Total',
                    'Vol_Budget': df_comp['Vol_Budget'].sum(),
                    'Vol_Actual': df_comp['Vol_Actual'].sum(),
                    'Diferença': df_comp['Diferença'].sum(),
                    'Proporção': (
                        df_comp['Vol_Actual'].sum() / df_comp['Vol_Budget'].sum()
                        if df_comp['Vol_Budget'].sum() > 0 else 1.0
                    ),
                }])
                df_comp = pd.concat([df_comp, total_row], ignore_index=True)

                # Formatar
                df_comp_fmt = df_comp.copy()
                df_comp_fmt['Vol_Budget'] = df_comp_fmt['Vol_Budget'].apply(
                    lambda x: f"{x:,.0f}"
                )
                df_comp_fmt['Vol_Actual'] = df_comp_fmt['Vol_Actual'].apply(
                    lambda x: f"{x:,.0f}"
                )
                df_comp_fmt['Diferença'] = df_comp_fmt['Diferença'].apply(
                    lambda x: f"{x:+,.0f}"
                )
                df_comp_fmt['Proporção'] = df_comp['Proporção'].apply(
                    lambda x: f"{x:.2%}"
                )

                st.dataframe(df_comp_fmt, use_container_width=True, hide_index=True)

                # Destacar proporção
                prop_total = df_comp[df_comp['Período'] == 'Total']['Proporção'].iloc[0]
                if abs(prop_total - 1.0) < 0.001:
                    st.warning(
                        "⚠️ **Proporção = 100%**: Volume Budget = Volume Realizado. "
                        "O Flex Budget será igual ao Budget."
                    )
                elif prop_total > 1.0:
                    st.success(
                        f"📈 **Proporção = {prop_total:.1%}**: "
                        "Volume Realizado maior que Budget."
                    )
                else:
                    st.info(
                        f"📉 **Proporção = {prop_total:.1%}**: "
                        "Volume Realizado menor que Budget."
                    )

            # ═══════════════════════════════════════
            # Tabelas Pivot: Volume Budget e Volume Actual
            # ═══════════════════════════════════════
            def _build_pivot_volume(df_pivot: pd.DataFrame, label_col: str = 'Volume') -> pd.DataFrame:
                """Constrói tabela pivot: Veículo × Mês, com linha Total."""
                # Garantir que Período esteja normalizado
                df_pivot = df_pivot.copy()
                df_pivot['Período'] = df_pivot['Período'].astype(str).str.strip().str.capitalize()

                # Ordenar meses conforme ORDEM_MESES
                meses_presentes = [m for m in ORDEM_MESES if m in df_pivot['Período'].unique()]

                pivot = df_pivot.pivot_table(
                    index='Veículo',
                    columns='Período',
                    values=label_col,
                    aggfunc='sum',
                    fill_value=0,
                )
                # Reordenar colunas por mês
                pivot = pivot.reindex(columns=[m for m in meses_presentes if m in pivot.columns], fill_value=0)

                # Adicionar coluna Total
                pivot['Total'] = pivot.sum(axis=1)

                # Resetar índice para que Veículo seja uma coluna normal
                pivot = pivot.reset_index()

                # Adicionar linha Total
                total_vals = {'Veículo': 'Total'}
                for c in pivot.columns:
                    if c != 'Veículo':
                        total_vals[c] = pivot[c].sum()
                pivot = pd.concat([pivot, pd.DataFrame([total_vals])], ignore_index=True)

                return pivot

            def _formatar_pivot(pivot: pd.DataFrame) -> pd.DataFrame:
                """Formata valores do pivot como inteiros com separador de milhar."""
                fmt = pivot.copy()
                for c in fmt.columns:
                    if c != 'Veículo':
                        fmt[c] = fmt[c].apply(lambda v: f"{int(v):,}" if pd.notna(v) else '-')
                return fmt

            # ─── Tabela 1: Volume Budget ───
            with st.expander("📋 Volume Budget — Tabela por Veículo × Mês", expanded=False):
                _pivot_bud = _build_pivot_volume(df_vb, 'Volume')
                _pivot_bud_fmt = _formatar_pivot(_pivot_bud)

                # Destacar linha Total com CSS via markdown
                st.dataframe(
                    _pivot_bud_fmt,
                    use_container_width=True,
                    hide_index=True,
                )

            # ─── Tabela 2: Volume Actual ───
            if df_va is not None:
                with st.expander("📋 Volume Actual (Real) — Tabela por Veículo × Mês", expanded=False):
                    _pivot_act = _build_pivot_volume(df_va, 'Volume')
                    _pivot_act_fmt = _formatar_pivot(_pivot_act)
                    st.dataframe(
                        _pivot_act_fmt,
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                with st.expander("📋 Volume Actual (Real) — Tabela por Veículo × Mês", expanded=False):
                    st.info("Dados de Volume Actual não disponíveis para este ano.")

        else:
            st.warning("Dados de volume não encontrados.")

    # ── TAB 3: Custos por Oficina ──
    with tab3:
        st.subheader("Custos por Oficina")

        df_oficina = df.groupby('Oficina', as_index=False).agg({
            c: 'sum' for c in cols_val + ['Rateio FA'] if c in df.columns
        })
        if 'Rateio FA' in df_oficina.columns:
            df_oficina['Rateio FA'] = df.groupby('Oficina')['Rateio FA'].mean().values
        df_oficina = df_oficina.sort_values('Custo FP', ascending=False)

        if tipo == 'CPU (Custo por Unidade)' and df_vol_bud is not None and 'Oficina' in df_vol_bud.columns:
            vol_ofi = df_vol_bud.groupby('Oficina', as_index=False)['Volume'].sum()
            df_oficina = df_oficina.merge(vol_ofi, on='Oficina', how='left')
            df_oficina['Volume'] = df_oficina['Volume'].fillna(0)
            for c in cols_val:
                if c in df_oficina.columns:
                    df_oficina[c] = calcular_cpu(df_oficina[c], df_oficina['Volume'])

        # KPIs de resumo: Top 3 Oficinas
        top3 = df_oficina.nlargest(3, 'Custo FP')
        ko1, ko2, ko3, ko4 = st.columns(4)
        with ko1:
            render_kpi("Total Custo FP", f"{simbolo} {df_oficina['Custo FP'].sum():,.2f}{sufixo}")
        if len(top3) >= 1:
            with ko2:
                render_kpi(f"#{1} {top3.iloc[0]['Oficina']}", f"{simbolo} {top3.iloc[0]['Custo FP']:,.2f}{sufixo}")
        if len(top3) >= 2:
            with ko3:
                render_kpi(f"#{2} {top3.iloc[1]['Oficina']}", f"{simbolo} {top3.iloc[1]['Custo FP']:,.2f}{sufixo}")
        if len(top3) >= 3:
            with ko4:
                render_kpi(f"#{3} {top3.iloc[2]['Oficina']}", f"{simbolo} {top3.iloc[2]['Custo FP']:,.2f}{sufixo}")

        render_kpi_spacer()

        col_a, col_b = st.columns(2)

        with col_a:
            # Gráfico de barras simples de Custo FP por Oficina
            bar_ofi = (alt.Chart(df_oficina).mark_bar(
                color='#4A90E2', cornerRadiusTopLeft=3, cornerRadiusTopRight=3
            ).encode(
                x=alt.X('Oficina:N', sort='-y', title='Oficina'),
                y=alt.Y('Custo FP:Q', title=f'{label_valor} ({simbolo}{sufixo})'),
                tooltip=['Oficina:N', alt.Tooltip('Custo FP:Q', format=',.2f', title='Custo FP')],
            ).properties(height=450, title='Custo FP por Oficina'))
            st.altair_chart(bar_ofi, use_container_width=True)

        with col_b:
            if 'Rateio FA' in df_oficina.columns:
                df_rat = df_oficina[['Oficina', 'Rateio FA']].copy()
                df_rat['Rateio %'] = df_rat['Rateio FA'] * 100
                bar_rat = (alt.Chart(df_rat).mark_bar(
                    cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
                ).encode(
                    x=alt.X('Oficina:N', sort='-y'),
                    y=alt.Y('Rateio %:Q', title='Rateio FA (%)'),
                    color=alt.condition(
                        alt.datum['Rateio %'] > 0, alt.value('#27ae60'), alt.value('#f44336'),
                    ),
                    tooltip=['Oficina:N', alt.Tooltip('Rateio %:Q', format='.2f')],
                ).properties(height=450, title='Rateio FA por Oficina'))
                st.altair_chart(bar_rat, use_container_width=True)

        # Tabela com Custo FP
        show = df_oficina[['Oficina', 'Custo FP']].copy()
        if 'Rateio FA' in df_oficina.columns:
            show['Rateio FA %'] = df_oficina['Rateio FA'].apply(
                lambda x: f"{x*100:.2f}%"
            )
        st.markdown(
            criar_tabela_html(show, linha_total=False, simbolo=simbolo),
            unsafe_allow_html=True,
        )

        # ── Tabela Pivotada Oficina × Período ──
        with st.expander("📊 Resumo BUD vs Flex por Oficina × Período"):
            if df_flex is not None and 'Oficina' in df.columns:
                # Agrupar por Oficina e Período para BUD (usar df_bud) e Flex
                df_pivot_base = df_bud.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Custo FP'].sum()

                # Preparar pivot BUD
                piv_bud = df_pivot_base.pivot_table(
                    index='Oficina',
                    columns='Período',
                    values='Custo FP',
                    aggfunc='sum',
                )
                # Ordenar colunas por ORDEM_MESES
                cols_ord = [m for m in ORDEM_MESES if m in piv_bud.columns]
                piv_bud = piv_bud[cols_ord]
                piv_bud['Ano'] = piv_bud.sum(axis=1)

                # Linha Total
                piv_bud.loc['Total'] = piv_bud.sum()
                piv_bud.index.name = 'Oficina'

                # Formatar valores
                fmt_bud = piv_bud.copy()
                for col in fmt_bud.columns:
                    fmt_bud[col] = fmt_bud[col].apply(
                        lambda x: f"{simbolo} {x:,.0f}" if pd.notna(x) else "—"
                    )

                st.markdown("**📦 Budget (BUD)**")
                st.dataframe(fmt_bud, use_container_width=True)

                # Flex Budget por Oficina × Período (se disponível)
                if 'Custo' in df_bud.columns:
                    # Calcular Flex por Oficina × Período (usando Budget)
                    df_flex_base = df_bud.copy()
                    df_flex_base['Custo_str'] = df_flex_base['Custo'].astype(
                        str
                    ).str.lower()
                    df_flex_base['is_fixo'] = df_flex_base['Custo_str'].str.startswith(
                        'fix'
                    )
                    df_flex_base['Custo_Fixo'] = df_flex_base.apply(
                        lambda r: r['Custo FP'] if r['is_fixo'] else 0, axis=1
                    )
                    df_flex_base['Custo_NaoFixo'] = df_flex_base.apply(
                        lambda r: r['Custo FP'] if not r['is_fixo'] else 0, axis=1
                    )

                    df_fixo = df_flex_base.groupby(
                        ['Oficina', 'Período'], as_index=False
                    )['Custo_Fixo'].sum()
                    df_nfixo = df_flex_base.groupby(
                        ['Oficina', 'Período'], as_index=False
                    )['Custo_NaoFixo'].sum()

                    # Merge com proporção de volume (df_flex)
                    df_flex_merged = df_fixo.merge(
                        df_nfixo, on=['Oficina', 'Período'], how='outer'
                    ).fillna(0)
                    df_flex_merged = df_flex_merged.merge(
                        df_flex[['Período', 'Proporcao']], on='Período', how='left'
                    )
                    df_flex_merged['Proporcao'] = df_flex_merged['Proporcao'].fillna(1)
                    df_flex_merged['Flex_Bud'] = (
                        df_flex_merged['Custo_Fixo']
                        + df_flex_merged['Custo_NaoFixo'] * df_flex_merged['Proporcao']
                    )

                    piv_flex = df_flex_merged.pivot_table(
                        index='Oficina',
                        columns='Período',
                        values='Flex_Bud',
                        aggfunc='sum',
                    )
                    piv_flex = piv_flex[[m for m in ORDEM_MESES if m in piv_flex.columns]]
                    piv_flex['Ano'] = piv_flex.sum(axis=1)
                    piv_flex.loc['Total'] = piv_flex.sum()
                    piv_flex.index.name = 'Oficina'

                    fmt_flex = piv_flex.copy()
                    for col in fmt_flex.columns:
                        fmt_flex[col] = fmt_flex[col].apply(
                            lambda x: f"{simbolo} {x:,.0f}" if pd.notna(x) else "—"
                        )

                    st.markdown("**📈 Flex Budget**")
                    st.dataframe(fmt_flex, use_container_width=True)
            else:
                st.info("Dados de Flex Budget não disponíveis.")

    # ── TAB 4: Análise Flex ──
    with tab4:
        st.subheader("Análise Flex — Fixo vs Variável")

        if df_flex is not None and 'Custo' in df.columns:
            # KPIs de Flex no topo
            bud_total_t4 = df_flex['Custo_Total_Bud'].sum()
            flex_total_t4 = df_flex['Flex_Bud'].sum()
            fixo_total_t4 = df_flex['Custo_Fixo'].sum()
            nfixo_total_t4 = df_flex['Custo_NaoFixo'].sum()

            kf1, kf2, kf3, kf4 = st.columns(4)
            with kf1:
                render_kpi("Custo Fixo", f"{simbolo} {fixo_total_t4:,.2f}{sufixo}")
            with kf2:
                render_kpi("Custo Variável", f"{simbolo} {nfixo_total_t4:,.2f}{sufixo}")
            with kf3:
                render_kpi("BUD Total", f"{simbolo} {bud_total_t4:,.2f}{sufixo}")
            with kf4:
                render_kpi("Flex BUD Total", f"{simbolo} {flex_total_t4:,.2f}{sufixo}")

            render_kpi_spacer()
            # Decompor custos por categoria (Fixo / Não-Fixo)
            df_cat = df.copy()
            df_cat['Custo_str'] = df_cat['Custo'].astype(str).str.lower()
            df_cat['Categoria'] = df_cat['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )

            # Agrupar por Período e Categoria
            df_cat_agg = df_cat.groupby(
                ['Período', 'Categoria'], as_index=False
            )['Custo FP'].sum()
            df_cat_agg = ordenar_por_mes(df_cat_agg)

            ordem_per = [m for m in ORDEM_MESES if m in df_cat_agg['Período'].unique()]

            col_a, col_b = st.columns(2)

            with col_a:
                # Gráfico de barras empilhadas
                bar_cat = (alt.Chart(df_cat_agg).mark_bar().encode(
                    x=alt.X('Período:N', sort=ordem_per, title='Período'),
                    y=alt.Y('Custo FP:Q', title=f'{label_valor} ({simbolo}{sufixo})',
                            stack=True),
                    color=alt.Color('Categoria:N',
                                    scale=alt.Scale(
                                        domain=['Fixo', 'Variável'],
                                        range=['#3498db', '#e74c3c']
                                    ),
                                    legend=alt.Legend(orient='top')),
                    tooltip=['Período:N', 'Categoria:N',
                             alt.Tooltip('Custo FP:Q', format=',.0f')],
                ).properties(height=400, title='Custo FP por Categoria'))
                st.altair_chart(bar_cat, use_container_width=True)

            with col_b:
                # Gráfico pizza
                df_cat_total = df_cat.groupby('Categoria', as_index=False)['Custo FP'].sum()
                pie_cat = (alt.Chart(df_cat_total).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta('Custo FP:Q'),
                    color=alt.Color('Categoria:N',
                                    scale=alt.Scale(
                                        domain=['Fixo', 'Variável'],
                                        range=['#3498db', '#e74c3c']
                                    )),
                    tooltip=['Categoria:N', alt.Tooltip('Custo FP:Q', format=',')],
                ).properties(height=400, title='Participação por Categoria'))
                st.altair_chart(pie_cat, use_container_width=True)

            # Tabela resumo
            st.markdown("**📊 Resumo por Categoria**")
            df_cat_pivot = df_cat_agg.pivot_table(
                index='Categoria',
                columns='Período',
                values='Custo FP',
                aggfunc='sum',
            )
            df_cat_pivot = df_cat_pivot[[m for m in ORDEM_MESES if m in df_cat_pivot.columns]]
            df_cat_pivot['Total'] = df_cat_pivot.sum(axis=1)
            df_cat_pivot.loc['Total'] = df_cat_pivot.sum()

            # Formatar valores
            fmt_cat = df_cat_pivot.copy()
            for col in fmt_cat.columns:
                fmt_cat[col] = fmt_cat[col].apply(
                    lambda x: f"{simbolo} {x:,.0f}" if pd.notna(x) else "—"
                )
            st.dataframe(fmt_cat, use_container_width=True)

            # Comparação BUD vs Flex por categoria
            st.markdown("**📈 BUD vs Flex Budget por Categoria**")
            bud_total = df_flex['Custo_Total_Bud'].sum()
            flex_total = df_flex['Flex_Bud'].sum()
            fixo_total = df_flex['Custo_Fixo'].sum()
            nfixo_total = df_flex['Custo_NaoFixo'].sum()

            comp_data = pd.DataFrame({
                'Métrica': ['Custo Fixo', 'Custo Variável', 'Total'],
                'BUD': [fixo_total, nfixo_total, bud_total],
                'Flex BUD': [fixo_total, nfixo_total * df_flex['Proporcao'].mean(),
                             fixo_total + nfixo_total * df_flex['Proporcao'].mean()],
            })
            comp_data['Diferença'] = comp_data['Flex BUD'] - comp_data['BUD']

            # Formatar
            for col in ['BUD', 'Flex BUD', 'Diferença']:
                comp_data[col] = comp_data[col].apply(lambda x: f"{simbolo} {x:,.0f}")

            st.dataframe(comp_data, use_container_width=True, hide_index=True)
        else:
            st.info("Dados de categoria (Custo) não disponíveis para análise Flex.")

    # ── TAB 5: Tempo de Produção / Custo FP por Veículo ──
    with tab5:
        # ── Filtros no topo da aba ──
        _col_filtro1, _col_filtro2, _ = st.columns([1.5, 1.5, 3])
        with _col_filtro1:
            unidade_tempo = st.radio(
                "🕒 Unidade de tempo",
                ["Minutos", "Horas"],
                horizontal=True,
                key="home_unidade_tempo_tab5"
            )
        with _col_filtro2:
            fonte_dados_tempo = st.radio(
                "📊 Fonte dos dados",
                ["Budget", "Real"],
                horizontal=True,
                key="home_fonte_dados_tempo"
            )
        fator_tempo = 1.0 if unidade_tempo == "Minutos" else 1.0 / 60.0
        label_tempo = "min" if unidade_tempo == "Minutos" else "h"

        st.subheader("Custo FP por Veículo")

        # Custo FP por Veículo (análogo ao TC Ext por Veíc)
        if 'Veículo' in df.columns:
            df_veic = df.groupby('Veículo', as_index=False).agg({
                c: 'sum' for c in cols_val if c in df.columns
            })
            df_veic = df_veic.sort_values('Custo FP', ascending=False)

            if tipo == 'CPU (Custo por Unidade)' and df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
                vol_veic = df_vol_bud.groupby('Veículo', as_index=False)['Volume'].sum()
                df_veic = df_veic.merge(vol_veic, on='Veículo', how='left')
                df_veic['Volume'] = df_veic['Volume'].fillna(0)
                for c in cols_val:
                    if c in df_veic.columns:
                        df_veic[c] = calcular_cpu(df_veic[c], df_veic['Volume'])

            # KPIs: Total e Top 3
            custo_fp_total_veic = df_veic['Custo FP'].sum()
            custo_fp_media = df_veic['Custo FP'].mean()
            top3_veic = df_veic.nlargest(3, 'Custo FP')

            ktv1, ktv2, ktv3, ktv4 = st.columns(4)
            with ktv1:
                render_kpi("Total Custo FP", f"{simbolo} {custo_fp_total_veic:,.2f}{sufixo}")
            with ktv2:
                render_kpi("Média/Veículo", f"{simbolo} {custo_fp_media:,.2f}{sufixo}")
            if len(top3_veic) >= 1:
                with ktv3:
                    render_kpi(f"#{1} {top3_veic.iloc[0]['Veículo']}", f"{simbolo} {top3_veic.iloc[0]['Custo FP']:,.2f}{sufixo}")
            if len(top3_veic) >= 2:
                with ktv4:
                    render_kpi(f"#{2} {top3_veic.iloc[1]['Veículo']}", f"{simbolo} {top3_veic.iloc[1]['Custo FP']:,.2f}{sufixo}")

            render_kpi_spacer()

            col_a, col_b = st.columns(2)

            with col_a:
                # Gráfico de barras Custo FP por Veículo
                bar_veic = (alt.Chart(df_veic).mark_bar(
                    cornerRadiusTopLeft=3, cornerRadiusTopRight=3
                ).encode(
                    x=alt.X('Veículo:N', sort='-y', title='Veículo'),
                    y=alt.Y('Custo FP:Q', title=f'{label_valor} ({simbolo}{sufixo})'),
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        ),
                        legend=None
                    ),
                    tooltip=['Veículo:N', alt.Tooltip('Custo FP:Q', format=',.2f', title='Custo FP')],
                ).properties(height=400, title='Custo FP por Veículo'))
                st.altair_chart(bar_veic, use_container_width=True)

            with col_b:
                # Gráfico pizza de participação
                pie_veic = (alt.Chart(df_veic).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta('Custo FP:Q'),
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        )
                    ),
                    tooltip=['Veículo:N', alt.Tooltip('Custo FP:Q', format=',.2f', title='Custo FP')],
                ).properties(height=400, title='Participação por Veículo'))
                st.altair_chart(pie_veic, use_container_width=True)

            # Tabela com Custo FP por Veículo
            st.markdown("**Custo FP por Veículo**")
            st.markdown(
                criar_tabela_html(df_veic[['Veículo', 'Custo FP']], linha_total=True, simbolo=simbolo),
                unsafe_allow_html=True,
            )

        # === Seção adicional: Tempo de Produção (se houver dados) ===
        st.divider()
        st.markdown("### Tempo de Produção — Veículos vs Fluxo Anexo")
        st.caption(f"📊 Fonte: **{fonte_dados_tempo}** | ⏱️ Unidade: **{unidade_tempo.lower()}**")

        # Carregar dados conforme fonte selecionada (Budget ou Real)
        if fonte_dados_tempo == "Real":
            _df_tv_src = load_tempo_veiculos_real(ano)
            _df_fa_src = load_volume_fa_real(ano)
        else:
            _df_tv_src = load_tempo_veiculos(ano)
            _df_fa_src = load_volume_fa(ano)

        if _df_tv_src is not None:
            df_tv = normalizar_periodo(_df_tv_src.copy())

            # ── GRÁFICO DE EVOLUÇÃO: Tempo Veículo vs Tempo FA por Período ──
            if _df_fa_src is not None:
                df_fa_evo = normalizar_periodo(_df_fa_src.copy())
                st.subheader("📈 Evolução Tempo Veículo vs Tempo FA por Período")

                # Agregar por período
                df_tv_per = df_tv.groupby('Período', as_index=False)['Tempo Veic'].sum()
                df_fa_per = df_fa_evo.groupby('Período', as_index=False)['Tempo FA'].sum()
                df_evo = pd.merge(df_tv_per, df_fa_per, on='Período', how='outer').fillna(0)
                df_evo['Tempo Veic'] = df_evo['Tempo Veic'] * fator_tempo
                df_evo['Tempo FA'] = df_evo['Tempo FA'] * fator_tempo
                df_evo['Total'] = df_evo['Tempo Veic'] + df_evo['Tempo FA']

                # Ordenar por mês
                ordem_meses_cat = pd.CategoricalDtype(categories=ORDEM_MESES, ordered=True)
                df_evo['Período'] = df_evo['Período'].astype(str).str.strip().str.capitalize()
                df_evo['Período'] = df_evo['Período'].astype(ordem_meses_cat)
                df_evo = df_evo.sort_values('Período').dropna(subset=['Período'])
                ordem_per_evo = df_evo['Período'].astype(str).tolist()
                df_evo['Período'] = df_evo['Período'].astype(str)

                col_evo_e, col_evo_d = st.columns(2)

                with col_evo_e:
                    # Barras empilhadas (valores)
                    df_evo_long = df_evo.melt(
                        id_vars='Período', value_vars=['Tempo Veic', 'Tempo FA'],
                        var_name='Tipo', value_name='Valor'
                    )
                    bar_evo = alt.Chart(df_evo_long).mark_bar().encode(
                        x=alt.X('Período:N', sort=ordem_per_evo, title='Período'),
                        y=alt.Y('Valor:Q', stack=True, title=f'Tempo ({label_tempo})'),
                        color=alt.Color('Tipo:N',
                            scale=alt.Scale(domain=['Tempo Veic', 'Tempo FA'],
                                            range=['#7C3AED', '#C4B5FD'])),
                        tooltip=['Período:N', 'Tipo:N', alt.Tooltip('Valor:Q', format=',.1f')],
                    ).properties(height=400, title=f'Evolução Tempo (empilhado) ({label_tempo})')

                    # Rótulo de total no topo
                    labels_evo = alt.Chart(df_evo).mark_text(
                        align='center', dy=-10, fontSize=9, color='#7C3AED', fontWeight='bold'
                    ).encode(
                        x=alt.X('Período:N', sort=ordem_per_evo),
                        y=alt.Y('Total:Q'),
                        text=alt.Text('Total:Q', format=',.1f'),
                    )
                    st.altair_chart(bar_evo + labels_evo, use_container_width=True)

                with col_evo_d:
                    # ── Rateio FA real (mesma fórmula do processamento) ──
                    # Automáticas (BS, PS, PL): Rateio FA = TFA / (TFA + TVeic)
                    # Manuais (QY, GS, SM): Rateio FA = fator × taxa_pdr
                    #   taxa_pdr = ∑TFA_global / ∑TVeic_global (excluindo GS/SM do denom)
                    OFICINAS_AUTO = {'BS', 'PS', 'PL'}
                    OFICINAS_MANUAL = {'QY', 'GS', 'SM'}
                    OFICINAS_EXCLUIR_DENOM = {'GS', 'SM'}

                    rateios_manuais = _carregar_rateios_manuais()

                    # Agregar Tempo FA por (Oficina, Período)
                    df_tfa_agg = df_fa_evo.groupby(['Oficina', 'Período'], as_index=False)['Tempo FA'].sum()
                    df_tfa_agg.rename(columns={'Tempo FA': 'TFA'}, inplace=True)
                    df_tfa_agg['Oficina_norm'] = df_tfa_agg['Oficina'].astype(str).str.strip().str.upper()
                    df_tfa_agg['Período'] = df_tfa_agg['Período'].astype(str).str.strip().str.capitalize()
                    df_tfa_agg = df_tfa_agg[df_tfa_agg['Período'].isin(ordem_per_evo)]

                    # Agregar Tempo Veic por (Oficina, Período)
                    df_tvc_agg = df_tv.groupby(['Oficina', 'Período'], as_index=False)['Tempo Veic'].sum()
                    df_tvc_agg.rename(columns={'Tempo Veic': 'TVeic'}, inplace=True)
                    df_tvc_agg['Oficina_norm'] = df_tvc_agg['Oficina'].astype(str).str.strip().str.upper()
                    df_tvc_agg['Período'] = df_tvc_agg['Período'].astype(str).str.strip().str.capitalize()

                    # Merge TFA + TVeic
                    df_rateio = pd.merge(df_tfa_agg, df_tvc_agg[['Oficina_norm', 'Período', 'TVeic']],
                                         on=['Oficina_norm', 'Período'], how='outer')
                    df_rateio['TFA'] = df_rateio['TFA'].fillna(0)
                    df_rateio['TVeic'] = df_rateio['TVeic'].fillna(0)
                    # Preencher Oficina se veio do merge outer
                    if df_rateio['Oficina'].isna().any():
                        df_rateio['Oficina'] = df_rateio['Oficina'].fillna(df_rateio['Oficina_norm'])

                    # Calcular taxa_pdr global por período (excluindo GS/SM do denominador TVeic)
                    tfa_global = df_tfa_agg.groupby('Período', as_index=False)['TFA'].sum()
                    tfa_global.rename(columns={'TFA': 'TFA_global'}, inplace=True)
                    df_tvc_filt = df_tvc_agg[~df_tvc_agg['Oficina_norm'].isin(OFICINAS_EXCLUIR_DENOM)]
                    tvc_global = df_tvc_filt.groupby('Período', as_index=False)['TVeic'].sum()
                    tvc_global.rename(columns={'TVeic': 'TVC_global'}, inplace=True)
                    taxa_prod = pd.merge(tfa_global, tvc_global, on='Período', how='outer').fillna(0)
                    taxa_prod['taxa_pdr'] = np.where(
                        taxa_prod['TVC_global'] > 0,
                        taxa_prod['TFA_global'] / taxa_prod['TVC_global'],
                        0.0
                    )

                    # Calcular Rateio FA
                    df_rateio = pd.merge(df_rateio, taxa_prod[['Período', 'taxa_pdr']], on='Período', how='left')
                    df_rateio['taxa_pdr'] = df_rateio['taxa_pdr'].fillna(0)
                    df_rateio['Rateio FA'] = 0.0

                    # Automáticas: TFA / (TFA + TVeic)
                    mask_auto = df_rateio['Oficina_norm'].isin(OFICINAS_AUTO)
                    denom_auto = df_rateio.loc[mask_auto, 'TFA'] + df_rateio.loc[mask_auto, 'TVeic']
                    df_rateio.loc[mask_auto, 'Rateio FA'] = np.where(
                        denom_auto > 0,
                        df_rateio.loc[mask_auto, 'TFA'] / denom_auto,
                        np.nan
                    )

                    # Manuais: fator × taxa_pdr
                    for ofi in OFICINAS_MANUAL:
                        mask_ofi = df_rateio['Oficina_norm'] == ofi
                        fator_man = float(rateios_manuais.get(ofi, 0.0))
                        df_rateio.loc[mask_ofi, 'Rateio FA'] = fator_man * df_rateio.loc[mask_ofi, 'taxa_pdr']

                    # Garantir que oficinas manuais apareçam mesmo sem dados
                    periodos_existentes = df_rateio['Período'].unique()
                    for ofi in OFICINAS_MANUAL:
                        if ofi not in df_rateio['Oficina_norm'].values:
                            fator_man = float(rateios_manuais.get(ofi, 0.0))
                            novas = pd.DataFrame({
                                'Oficina': ofi,
                                'Oficina_norm': ofi,
                                'Período': periodos_existentes,
                                'TFA': 0.0,
                                'TVeic': 0.0,
                                'taxa_pdr': taxa_prod.set_index('Período')['taxa_pdr'].reindex(periodos_existentes).fillna(0).values,
                                'Rateio FA': 0.0,
                            })
                            novas['Rateio FA'] = fator_man * novas['taxa_pdr']
                            df_rateio = pd.concat([df_rateio, novas], ignore_index=True)

                    # Períodos sem dados reais (taxa_pdr=0) → NaN para não plotar zeros
                    mask_manual_all = df_rateio['Oficina_norm'].isin(OFICINAS_MANUAL)
                    df_rateio.loc[mask_manual_all & (df_rateio['taxa_pdr'] == 0), 'Rateio FA'] = np.nan

                    # Filtrar só períodos válidos e remover NaN para não plotar zeros
                    df_rateio = df_rateio[df_rateio['Período'].isin(ordem_per_evo)]
                    df_rateio = df_rateio.dropna(subset=['Rateio FA'])

                    line_pct = alt.Chart(df_rateio).mark_line(point=True, strokeWidth=2).encode(
                        x=alt.X('Período:N', sort=ordem_per_evo, title='Período'),
                        y=alt.Y('Rateio FA:Q', title='Rateio FA (%)', axis=alt.Axis(format='.1%')),
                        color=alt.Color('Oficina:N', title='Oficina'),
                        tooltip=[
                            'Oficina:N', 'Período:N',
                            alt.Tooltip('Rateio FA:Q', format='.2%', title='Rateio FA'),
                            alt.Tooltip('TFA:Q', format=',.1f', title='Tempo FA'),
                            alt.Tooltip('TVeic:Q', format=',.1f', title='Tempo Veic'),
                        ],
                    ).properties(height=400, title=f'Rateio FA por Oficina ({fonte_dados_tempo})')
                    st.altair_chart(line_pct, use_container_width=True)

                st.divider()

            col_c, col_d = st.columns(2)

            with col_c:
                df_tv_of = df_tv.groupby('Oficina', as_index=False)['Tempo Veic'].sum()
                df_tv_of['Tempo Veic'] = df_tv_of['Tempo Veic'] * fator_tempo
                bar_tv = (alt.Chart(df_tv_of).mark_bar(
                    color='#7C3AED', cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
                ).encode(
                    x=alt.X('Oficina:N', sort='-y'),
                    y=alt.Y('Tempo Veic:Q', title=f'Tempo Veic ({label_tempo})'),
                    tooltip=['Oficina:N', alt.Tooltip('Tempo Veic:Q', format=',.1f')],
                ).properties(height=400, title=f'Tempo Veículo por Oficina ({label_tempo})'))
                labels_tv = bar_tv.mark_text(
                    align='center', dy=-10, fontSize=9, color='#7C3AED'
                ).encode(text=alt.Text('Tempo Veic:Q', format=',.1f'))
                st.altair_chart(bar_tv + labels_tv, use_container_width=True)

            with col_d:
                if _df_fa_src is not None:
                    df_fa_tempo = normalizar_periodo(_df_fa_src.copy())
                    df_fa_agg = df_fa_tempo.groupby('Oficina', as_index=False)['Tempo FA'].sum()
                    df_tv_agg = df_tv.groupby('Oficina', as_index=False)['Tempo Veic'].sum()
                    df_comp_tempo = pd.merge(df_tv_agg, df_fa_agg, on='Oficina', how='outer').fillna(0)
                    df_comp_tempo['Tempo Veic'] = df_comp_tempo['Tempo Veic'] * fator_tempo
                    df_comp_tempo['Tempo FA'] = df_comp_tempo['Tempo FA'] * fator_tempo
                    df_comp_long = df_comp_tempo.melt(
                        id_vars='Oficina', value_vars=['Tempo Veic', 'Tempo FA'],
                        var_name='Tipo', value_name='Tempo',
                    )
                    bar_comp = (alt.Chart(df_comp_long).mark_bar().encode(
                        x=alt.X('Oficina:N', sort='-y'),
                        y=alt.Y('Tempo:Q', title=f'Tempo ({label_tempo})'),
                        color=alt.Color('Tipo:N',
                                        scale=alt.Scale(domain=['Tempo Veic', 'Tempo FA'],
                                                        range=['#7C3AED', '#C4B5FD'])),
                        xOffset='Tipo:N',
                        tooltip=['Oficina:N', 'Tipo:N', alt.Tooltip('Tempo:Q', format=',.1f')],
                    ).properties(height=400, title=f'Tempo Veículo vs Tempo FA ({label_tempo})'))
                    labels_comp = bar_comp.mark_text(
                        align='center', dy=-10, fontSize=9, color='black'
                    ).encode(text=alt.Text('Tempo:Q', format=',.1f'))
                    st.altair_chart(bar_comp + labels_comp, use_container_width=True)

            st.markdown(f"**EST e Tempo por Veículo e Oficina ({label_tempo})**")
            df_tv_tab = df_tv.groupby(['Oficina', 'Veículo'], as_index=False).agg({
                'EST': 'first', 'Volume': 'sum', 'Tempo Veic': 'sum',
            }).sort_values(['Oficina', 'Tempo Veic'], ascending=[True, False])
            df_tv_tab['Tempo Veic'] = df_tv_tab['Tempo Veic'] * fator_tempo
            df_tv_tab = df_tv_tab.rename(columns={'Tempo Veic': f'Tempo Veic ({label_tempo})'})
            st.dataframe(df_tv_tab, use_container_width=True, hide_index=True)

            # ── Tabela de Percentuais de Rateio (conferência com Excel) ──
            st.divider()
            st.markdown("### 📊 Percentuais de Rateio por Veículo")
            st.caption(
                "Conferência: valores idênticos à coluna R da aba "
                "\"EST veículos - Actual\" do Excel"
            )
            _df_pct_home = load_percentual_rateio_veiculos_real(ano)
            if _df_pct_home is not None and not _df_pct_home.empty:
                _pct = _df_pct_home.copy()
                _pct['Período'] = _pct['Período'].astype(str).str.strip().str.capitalize()
                # Criar label Oficina × Veículo
                _pct['Oficina_Veículo'] = _pct['Oficina'].astype(str) + ' — ' + _pct['Veículo'].astype(str)
                # Pivotar: meses como colunas
                _ordem_m = [m.capitalize() for m in ORDEM_MESES]
                _piv = _pct.pivot_table(
                    index='Oficina_Veículo', columns='Período',
                    values='Percentual', aggfunc='first',
                )
                _cols_ord = [m for m in _ordem_m if m in _piv.columns]
                _piv = _piv[_cols_ord]
                _piv = _piv.reset_index()
                # Formatar como percentual
                _fmt = {c: '{:.2%}'.format for c in _cols_ord}
                st.dataframe(
                    _piv.style.format(_fmt, na_rep='—'),
                    use_container_width=True, hide_index=True, height=500,
                )
                # Verificar soma = 100%
                _soma_pct = _pct.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Percentual'].sum()
                _min_s, _max_s = _soma_pct['Percentual'].min(), _soma_pct['Percentual'].max()
                if abs(_min_s - 1.0) < 0.001 and abs(_max_s - 1.0) < 0.001:
                    st.success("✅ Soma dos percentuais = 100% em todas as Oficinas/Períodos")
                else:
                    st.warning(
                        f"⚠️ Soma dos percentuais: min={_min_s:.4f}, max={_max_s:.4f} "
                        "(esperado: 1.0000)"
                    )
            else:
                st.info("Dados de percentual de rateio não disponíveis.")
        else:
            st.info("Dados de tempo de produção não disponíveis.")

    # ── TAB 6: Dados Detalhados ──
    with tab6:
        st.subheader("📋 Dados Detalhados")

        # Seletor de visualização: Total ou Fixo/Variável
        _col_viz, _ = st.columns([1.3, 3])
        with _col_viz:
            modo_tab6 = st.radio(
                "📊 **Visualização:**",
                ["Total", "Fixo/Variável"],
                index=0, horizontal=True,
                key="home_tab6_viz",
            )

        col_valor_tab6 = 'Custo FP'

        # ═══════════════════════════════════════════════════════
        # 📊 TABELAS TC TOTAL
        # ═══════════════════════════════════════════════════════
        st.markdown("## 📊 Tabelas TC Total")

        # Tabela — Budget Total
        piv_bud, ofc_bud = _pivotar_detalhado(df_bud, col_valor_tab6)
        render_secao_tabela_detalhe(
            piv_bud, ofc_bud, "Budget Total", "💰",
            "home_bud", ano, simbolo, sufixo,
            expanded=True, modo=modo_tab6,
        )

        # Tabela — Flex Budget Total
        if df_flex_det is not None and not df_flex_det.empty:
            piv_flex_d, ofc_flex_d = _pivotar_detalhado(
                df_flex_det, 'Flex_Bud',
            )
            render_secao_tabela_detalhe(
                piv_flex_d, ofc_flex_d, "Flex Budget", "📐",
                "home_flex", ano, simbolo, sufixo,
                expanded=False, modo=modo_tab6,
            )
        else:
            piv_flex, ofc_flex = _pivotar_flex(df_flex)
            render_secao_tabela_detalhe(
                piv_flex, ofc_flex, "Flex Budget", "📐",
                "home_flex", ano, simbolo, sufixo,
                expanded=False, modo=modo_tab6,
            )

        # Tabela — Real Total
        piv_real, ofc_real = _pivotar_detalhado(df, col_valor_tab6)
        render_secao_tabela_detalhe(
            piv_real, ofc_real, "Real Total", "✅",
            "home_real", ano, simbolo, sufixo,
            expanded=False, modo=modo_tab6,
        )

        # Tabela — Best Estimate Total
        # IMPORTANTE: Usar mesmos filtros da Tab 1 (sidebar) para consistência
        # EXCETO filtro de veículo (para permitir rateio correto posteriormente)
        _df_be_tab6 = None
        _df_be_tab6_com_filtro_veiculo = None
        try:
            if _raw_df_be is not None and not _raw_df_be.empty:
                # Criar filtros SEM veículo para BE Por Veículo poder ratear
                filtros_sem_veiculo = {k: v for k, v in filtros_sel.items() if k != 'Veículo'}
                
                _fc = _raw_df_be.copy()
                _fc = aplicar_filtros(_fc, filtros_sem_veiculo)
                
                if not _fc.empty:
                    _cv = [c for c in COLUNAS_MONETARIAS if c in _fc.columns]
                    _fc = aplicar_fator_df(_fc, _cv, fator)
                    _fc = converter_moeda_df(_fc, _cv, moeda, taxas)
                    _df_be_tab6 = _fc
                    
                    # Para tabela BE Total, aplicar também filtro de veículo
                    if 'Veículo' in filtros_sel and filtros_sel['Veículo']:
                        _df_be_tab6_com_filtro_veiculo = aplicar_filtros(_fc.copy(), {'Veículo': filtros_sel['Veículo']})
                    else:
                        _df_be_tab6_com_filtro_veiculo = _fc.copy()
        except Exception:
            pass

        _df_be_para_tabela = _df_be_tab6_com_filtro_veiculo if _df_be_tab6_com_filtro_veiculo is not None else _df_be_tab6
        if _df_be_para_tabela is not None:
            piv_be, ofc_be = _pivotar_detalhado(
                _df_be_para_tabela, col_valor_tab6,
            )
            render_secao_tabela_detalhe(
                piv_be, ofc_be, "Best Estimate", "🔮",
                "home_be", ano, simbolo, sufixo,
                expanded=False, modo=modo_tab6,
            )

        # ═══════════════════════════════════════════════════════
        # 🚗 TABELAS TC POR VEÍCULOS
        # ═══════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("## 🚗 Tabelas TC Por Veículos")

        # ── Budget por Veículo ──
        veiculos_bud = []
        _df_veic_bud_tab6 = None
        if df_veic_bud_raw is not None:
            _vb = normalizar_periodo(df_veic_bud_raw.copy())
            if 'Custo FP Veiculo' in _vb.columns:
                _vb['Custo FP'] = _vb['Custo FP Veiculo']
            _cv_vb = [c for c in COLUNAS_MONETARIAS if c in _vb.columns]
            _vb = aplicar_fator_df(_vb, _cv_vb, fator)
            _vb = converter_moeda_df(_vb, _cv_vb, moeda, taxas)
            if not _vb.empty:
                _df_veic_bud_tab6 = _vb

        with st.expander("💰 Budget Por Veículo", expanded=False):
            if _df_veic_bud_tab6 is not None and 'Veículo' in _df_veic_bud_tab6.columns:
                veiculos_bud = sorted(
                    _df_veic_bud_tab6['Veículo'].dropna().unique()
                )
                for veic in veiculos_bud:
                    _dv = _df_veic_bud_tab6[
                        _df_veic_bud_tab6['Veículo'] == veic
                    ].copy()
                    if _dv.empty:
                        continue
                    piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                    render_secao_tabela_detalhe(
                        piv_v, ofc_v,
                        f"Budget — {veic}", "🚗",
                        f"home_vbud_{veic}", ano, simbolo, sufixo,
                        expanded=False, modo=modo_tab6,
                    )
            else:
                st.info("ℹ️ Dados de Budget por veículo não disponíveis.")

        # ── Flex Budget por Veículo ──
        with st.expander("📐 Flex Budget Por Veículo", expanded=False):
            if _df_veic_bud_tab6 is not None and 'Veículo' in _df_veic_bud_tab6.columns:
                for veic in veiculos_bud:
                    _dv_fb = _df_veic_bud_tab6[
                        _df_veic_bud_tab6['Veículo'] == veic
                    ].copy()
                    if _dv_fb.empty:
                        continue
                    # Calcular flex budget por veículo (detalhado)
                    _vol_bud_v = None
                    _vol_act_v = None
                    if _raw_df_vol_bud is not None and 'Veículo' in _raw_df_vol_bud.columns:
                        _vol_bud_v = _raw_df_vol_bud[
                            _raw_df_vol_bud['Veículo'] == veic
                        ].copy()
                    if _raw_df_vol_actual is not None and 'Veículo' in _raw_df_vol_actual.columns:
                        _vol_act_v = _raw_df_vol_actual[
                            _raw_df_vol_actual['Veículo'] == veic
                        ].copy()
                    # Tentar versão detalhada (preserva dimensões)
                    _fx_v_det = calcular_flex_budget_detalhado(
                        _dv_fb, _vol_bud_v, _vol_act_v,
                        col_custo='Custo FP',
                        tem_ano='Ano' in _dv_fb.columns,
                    )
                    if _fx_v_det is not None and not _fx_v_det.empty:
                        piv_fv, ofc_fv = _pivotar_detalhado(
                            _fx_v_det, 'Flex_Bud',
                        )
                    else:
                        # Fallback: versão agregada
                        _fx_v = calcular_flex_budget(
                            _dv_fb, _vol_bud_v, _vol_act_v,
                            tem_ano='Ano' in _dv_fb.columns,
                        )
                        if _fx_v is not None and not _fx_v.empty:
                            piv_fv, ofc_fv = _pivotar_flex(_fx_v)
                        else:
                            continue
                    render_secao_tabela_detalhe(
                        piv_fv, ofc_fv,
                        f"Flex Budget — {veic}", "📐",
                        f"home_vflex_{veic}", ano, simbolo, sufixo,
                        expanded=False, modo=modo_tab6,
                    )
            else:
                st.info("ℹ️ Dados de Flex Budget por veículo não disponíveis.")

        # ── Real por Veículo ──
        _df_veic_real_tab6 = None
        if df_veic_real_raw is not None:
            _vr = normalizar_periodo(df_veic_real_raw.copy())
            if 'Custo FP Veiculo' in _vr.columns:
                _vr['Custo FP'] = _vr['Custo FP Veiculo']
            _cv_vr = [c for c in COLUNAS_MONETARIAS if c in _vr.columns]
            _vr = aplicar_fator_df(_vr, _cv_vr, fator)
            _vr = converter_moeda_df(_vr, _cv_vr, moeda, taxas)
            if not _vr.empty:
                _df_veic_real_tab6 = _vr

        with st.expander("✅ Real Por Veículo", expanded=False):
            if _df_veic_real_tab6 is not None and 'Veículo' in _df_veic_real_tab6.columns:
                veiculos_real = sorted(
                    _df_veic_real_tab6['Veículo'].dropna().unique()
                )
                for veic in veiculos_real:
                    _dv = _df_veic_real_tab6[
                        _df_veic_real_tab6['Veículo'] == veic
                    ].copy()
                    if _dv.empty:
                        continue
                    piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                    render_secao_tabela_detalhe(
                        piv_v, ofc_v,
                        f"Real — {veic}", "🚗",
                        f"home_vreal_{veic}", ano, simbolo, sufixo,
                        expanded=False, modo=modo_tab6,
                    )
            else:
                st.info("ℹ️ Dados Real por veículo não disponíveis.")

        # ── Best Estimate por Veículo ──
        # PRIORIDADE: Usar arquivo pré-gerado (forecast_veiculos_custo_fp.parquet)
        # FALLBACK: Ratear em runtime usando percentuais Real
        with st.expander("🔮 Best Estimate Por Veículo", expanded=False):
            _df_be_veic_tab6 = None
            
            # Tentar usar arquivo pré-gerado (igual Budget/Real)
            if df_veic_be_raw is not None and not df_veic_be_raw.empty:
                _vbe = normalizar_periodo(df_veic_be_raw.copy())
                
                # Aplicar filtros (exceto Veículo para mostrar todos)
                filtros_sem_veiculo = {k: v for k, v in filtros_sel.items() if k != 'Veículo'}
                _vbe = aplicar_filtros(_vbe, filtros_sem_veiculo)
                
                if not _vbe.empty:
                    # Usar 'Custo FP Veiculo' como 'Custo FP'
                    if 'Custo FP Veiculo' in _vbe.columns:
                        _vbe['Custo FP'] = _vbe['Custo FP Veiculo']
                    
                    # Aplicar fator/moeda
                    _cv_vbe = [c for c in COLUNAS_MONETARIAS if c in _vbe.columns]
                    _vbe = aplicar_fator_df(_vbe, _cv_vbe, fator)
                    _vbe = converter_moeda_df(_vbe, _cv_vbe, moeda, taxas)
                    _df_be_veic_tab6 = _vbe
            
            # Usar arquivo pré-gerado se disponível
            if _df_be_veic_tab6 is not None and 'Veículo' in _df_be_veic_tab6.columns:
                veiculos_be = sorted(_df_be_veic_tab6['Veículo'].dropna().unique())
                
                if len(veiculos_be) > 0:
                    for veic in veiculos_be:
                        _dv = _df_be_veic_tab6[_df_be_veic_tab6['Veículo'] == veic].copy()
                        if _dv.empty:
                            continue
                        piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                        render_secao_tabela_detalhe(
                            piv_v, ofc_v,
                            f"Best Estimate — {veic}", "🔮",
                            f"home_vbe_{veic}", ano, simbolo, sufixo,
                            expanded=False, modo=modo_tab6,
                        )
                else:
                    st.info("ℹ️ Nenhum veículo encontrado nos dados de BE.")
            
            # Fallback: ratear em runtime se arquivo não existe
            elif _df_be_tab6 is not None and not _df_be_tab6.empty:
                st.info("ℹ️ Arquivo com veículo não encontrado. Aplicando rateio...")
                
                _df_be_para_ratear = _df_be_tab6.drop(columns=['Veículo'], errors='ignore')
                _pct_real = load_percentual_rateio_veiculos_real(ano)
                _dea_real = load_dea_dedicado_real(ano)
                
                if _pct_real is None or _pct_real.empty:
                    st.warning(
                        "⚠️ Percentuais de rateio Real não encontrados. "
                        "Execute o processamento Real ou regenere o Forecast."
                    )
                else:
                    _df_be_veic = ratear_be_por_veiculo(_df_be_para_ratear, _pct_real, df_dea=_dea_real)
                    
                    if _df_be_veic is not None and 'Veículo' in _df_be_veic.columns:
                        if 'Custo FP Veiculo' in _df_be_veic.columns:
                            _df_be_veic['Custo FP'] = _df_be_veic['Custo FP Veiculo']
                        
                        veiculos_be = sorted(_df_be_veic['Veículo'].dropna().unique())
                        
                        for veic in veiculos_be:
                            _dv = _df_be_veic[_df_be_veic['Veículo'] == veic].copy()
                            if _dv.empty:
                                continue
                            piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                            render_secao_tabela_detalhe(
                                piv_v, ofc_v,
                                f"Best Estimate — {veic}", "🔮",
                                f"home_vbe_{veic}", ano, simbolo, sufixo,
                                expanded=False, modo=modo_tab6,
                            )
                    else:
                        st.error(
                            "❌ Erro ao ratear BE por veículo. "
                            "Regenere o Forecast no BE Simulador."
                        )
            else:
                st.info("ℹ️ Dados de BE por veículo não disponíveis. Gere um Forecast primeiro.")

        # ═══════════════════════════════════════════════════════
        # 📑 TABELA TC SAPIENS (dados detalhados com todas as colunas)
        # ═══════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("## 📑 Dados Sapiens Detalhados")

        df_sapiens = load_tc_sapiens(ano)
        if df_sapiens is not None and not df_sapiens.empty:
            with st.expander("📑 Tabela Sapiens — Todas as Colunas", expanded=False):
                # ── Filtros locais ──
                _flt_c1, _flt_c2, _flt_c3 = st.columns(3)
                with _flt_c1:
                    _ofc_opts = sorted(df_sapiens['Oficina'].dropna().unique()) if 'Oficina' in df_sapiens.columns else []
                    _ofc_sel = st.multiselect(
                        "🏭 Oficina", _ofc_opts, default=[], key="sap_oficina",
                    )
                with _flt_c2:
                    _per_opts = sorted(df_sapiens['Período'].dropna().unique()) if 'Período' in df_sapiens.columns else []
                    _per_sel = st.multiselect(
                        "📅 Período", _per_opts, default=[], key="sap_periodo",
                    )
                with _flt_c3:
                    _t05_opts = sorted(df_sapiens['Type 05'].dropna().unique()) if 'Type 05' in df_sapiens.columns else []
                    _t05_sel = st.multiselect(
                        "📂 Type 05", _t05_opts, default=[], key="sap_type05",
                    )

                _df_sap_filt = df_sapiens.copy()
                if _ofc_sel:
                    _df_sap_filt = _df_sap_filt[_df_sap_filt['Oficina'].isin(_ofc_sel)]
                if _per_sel:
                    _df_sap_filt = _df_sap_filt[_df_sap_filt['Período'].isin(_per_sel)]
                if _t05_sel:
                    _df_sap_filt = _df_sap_filt[_df_sap_filt['Type 05'].isin(_t05_sel)]

                st.caption(f"📊 {len(_df_sap_filt):,} linhas × {len(_df_sap_filt.columns)} colunas")
                st.dataframe(_df_sap_filt, use_container_width=True, height=500)

                # ── Download Excel ──
                if st.button(
                    "📥 Baixar Sapiens Detalhado (Excel)",
                    key="dl_sapiens_det",
                    use_container_width=True,
                ):
                    with st.spinner("Gerando arquivo…"):
                        try:
                            downloads = os.path.join(
                                os.path.expanduser("~"), "Downloads",
                            )
                            os.makedirs(downloads, exist_ok=True)
                            fname = f"TC_Sapiens_Detalhado_{ano}.xlsx"
                            fpath = os.path.join(downloads, fname)
                            with pd.ExcelWriter(fpath, engine='openpyxl') as w:
                                _df_sap_filt.to_excel(
                                    w, index=False, sheet_name='Sapiens',
                                )
                            st.success(f"✅ Arquivo salvo em: {fpath}")
                            st.info(f"📁 Verifique sua pasta Downloads: {downloads}")
                        except Exception as e:
                            st.error(f"❌ Erro ao gerar Excel: {e}")
        else:
            st.info(
                "ℹ️ Dados Sapiens detalhados não disponíveis. "
                "Execute o processamento Real na página **Extração de Dados** para gerar."
            )

    st.divider()

    # Rodapé padrão TC Ext
    mes_rodape = meses_pt.get(datetime.now().month, '')
    ano_rodape = datetime.now().year
    versao_rodape = '1.91'
    try:
        with open('versao.json', 'r', encoding='utf-8') as f:
            versao_rodape = json.load(f).get('versao', '1.91')
    except Exception:
        pass
    st.markdown(f"""
    <div style='text-align: center; color: #666; padding: 20px;'>
        📚 Stellantis Cost Intelligence (SCI) | Versão {versao_rodape} | {mes_rodape} {ano_rodape}
        <br>
        <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    render()
