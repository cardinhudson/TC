"""
Best Estimate — Análise (TC Veículos)
Cópia fiel da Home TC, substituindo dados Reais por dados do Best Estimate
(forecast_completo.parquet). Todas as 6 abas mantidas.
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import json
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    load_principal,
    load_volume_bud, load_volume_actual,
    load_tempo_veiculos, load_dea_dedicado, load_volume_fa,
    load_custo_fp_veiculo,
    normalizar_periodo, ordenar_por_mes,
    calcular_flex_budget, aplicar_fator_df,
    converter_moeda_df, obter_sufixo_fator, calcular_cpu,
    extrair_redis,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, render_sidebar_filters, aplicar_filtros,
    criar_tabela_html, render_kpi, render_kpi_spacer,
    formatar_ratio_com_barra, criar_tabela_html_flex,
)

# Desabilitar limite de linhas do Altair (nível de módulo, uma única vez)
alt.data_transformers.disable_max_rows()

# Dicionário de meses em português
meses_pt = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}

# ═══════════════════════════════════════════════════════════
#  LOADER — FORECAST
# ═══════════════════════════════════════════════════════════

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
    return df


# ═══════════════════════════════════════════════════════════
#  GRÁFICO: Custo FP por Período (cópia da Home, label "BE")
# ═══════════════════════════════════════════════════════════

def create_periodo_chart(df_periodo, df_flex, tipo, label_valor,
                         simbolo, sufixo, ordem_per, tem_ano=False):
    """
    Cria gráfico de Custo FP por Período — cópia da Home TC.
    Usa scheme='purples' para degradê nas barras.
    """
    try:
        coluna = 'Custo FP'
        titulo_y = f'{label_valor} ({simbolo}{sufixo})'

        # Limpar dados
        df_periodo = df_periodo.replace([np.inf, -np.inf], 0)
        df_periodo[coluna] = df_periodo[coluna].fillna(0)
        df_periodo = df_periodo.copy().reset_index(drop=True)

        # Determinar coluna do período para eixo X
        if tem_ano and 'Ano' in df_periodo.columns:
            df_periodo['Período_Completo'] = (
                df_periodo['Período'].astype(str) + ' '
                + df_periodo['Ano'].astype(str)
            )
            coluna_periodo = 'Período_Completo'
        else:
            coluna_periodo = 'Período'

        # Garantir dados numéricos
        df_periodo[coluna] = pd.to_numeric(
            df_periodo[coluna], errors='coerce'
        ).fillna(0)

        n_periodos = len(ordem_per) if ordem_per else 0
        altura_grafico = (
            min(520, max(260, 18 * n_periodos + 120))
            if n_periodos else 260
        )

        # ── Barras com degradê roxo (purples) ──
        grafico_barras = alt.Chart(df_periodo).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo}:N', title='Período', sort=ordem_per,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(
                f'{coluna}:Q', title=titulo_y,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            color=alt.Color(
                f'{coluna}:Q', title=coluna,
                scale=alt.Scale(scheme='purples'),
                legend=alt.Legend(
                    title=coluna, orient='right',
                    titleFontSize=10, labelFontSize=9
                )
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo}:N', title='Período'),
                alt.Tooltip(f'{coluna}:Q', title=coluna, format=',.2f')
            ]
        ).properties(height=altura_grafico, width=900)

        # ── Rótulos nas barras ──
        rotulos = grafico_barras.mark_text(
            align='center', baseline='middle', dy=-10,
            color='black', fontSize=9
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=',.2f')
        ).transform_filter(
            (alt.datum[coluna] != None) & (alt.datum[coluna] != 0)  # noqa
        )

        # ── Linha Flex Bud (pontilhada laranja) ──
        linha_flex = None
        df_flex_p = None
        if df_flex is not None and len(df_flex) > 0:
            colunas_flex = ['Período', 'Flex_Bud']
            if tem_ano and 'Ano' in df_flex.columns:
                colunas_flex.insert(0, 'Ano')

            df_flex_p = df_flex[colunas_flex].copy()
            df_flex_p['Período'] = df_flex_p['Período'].astype(str)

            if tem_ano and 'Ano' in df_flex_p.columns:
                df_flex_p['Ano'] = df_flex_p['Ano'].astype(str)
                df_flex_p['Período_Completo'] = (
                    df_flex_p['Período'] + ' ' + df_flex_p['Ano']
                )

            df_flex_p = ordenar_por_mes(df_flex_p)

            if (tipo == 'CPU (Custo por Unidade)'
                    and 'Vol_Actual' in df_flex.columns):
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
                df_flex_p = df_flex_p.merge(
                    df_flex_vol, on=merge_on, how='left'
                )
                df_flex_p['Vol_Actual'] = df_flex_p['Vol_Actual'].fillna(0)
                df_flex_p['Flex_Bud'] = calcular_cpu(
                    df_flex_p['Flex_Bud'], df_flex_p['Vol_Actual']
                )

            df_flex_p = df_flex_p.replace([np.inf, -np.inf], 0)
            df_flex_p['Flex_Bud'] = df_flex_p['Flex_Bud'].fillna(0)
            df_flex_p = df_flex_p.copy().reset_index(drop=True)

            if df_flex_p['Flex_Bud'].abs().sum() == 0:
                df_flex_p = None

        if df_flex_p is not None and len(df_flex_p) > 0:
            df_flex_p['Tipo'] = 'Flex Bud'

            line_flex = alt.Chart(df_flex_p).mark_line(
                strokeDash=[10, 5], strokeWidth=1.5, opacity=0.8
            ).encode(
                x=alt.X(
                    f'{coluna_periodo}:N', title='Período', sort=ordem_per,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(
                    'Flex_Bud:Q', title=titulo_y,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                color=alt.Color(
                    'Tipo:N', title='Legenda',
                    scale=alt.Scale(
                        domain=['BE', 'Flex Bud'],
                        range=['#8B5CF6', '#FF6B35']
                    ),
                    legend=alt.Legend(
                        title='Legenda', orient='bottom',
                        titleFontSize=10, labelFontSize=9,
                        titleAnchor='middle', direction='horizontal',
                        symbolType='square'
                    )
                ),
                strokeDash=alt.StrokeDash(
                    'Tipo:N',
                    scale=alt.Scale(
                        domain=['BE', 'Flex Bud'],
                        range=[[0], [10, 5]]
                    ),
                    legend=None
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo}:N', title='Período'),
                    alt.Tooltip('Flex_Bud:Q', format=',.2f', title='Flex Bud')
                ]
            )

            pontos_flex = alt.Chart(df_flex_p).mark_circle(
                size=80, opacity=0.9
            ).encode(
                x=alt.X(f'{coluna_periodo}:N', sort=ordem_per),
                y='Flex_Bud:Q',
                color=alt.Color(
                    'Tipo:N',
                    scale=alt.Scale(
                        domain=['BE', 'Flex Bud'],
                        range=['#8B5CF6', '#FF6B35']
                    ),
                    legend=None
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo}:N', title='Período'),
                    alt.Tooltip('Flex_Bud:Q', format=',.2f', title='Flex Bud')
                ]
            )

            rotulos_flex = alt.Chart(df_flex_p).mark_text(
                align='center', baseline='bottom', dy=-15,
                color='#FF6B35', fontSize=9, fontWeight='bold'
            ).encode(
                x=alt.X(f'{coluna_periodo}:N', sort=ordem_per),
                y='Flex_Bud:Q',
                text=alt.Text('Flex_Bud:Q', format=',.2f')
            )

            linha_flex = line_flex + pontos_flex + rotulos_flex

        # ── Gráfico Delta (BE - Flex Bud) ──
        grafico_delta = None
        if df_flex_p is not None and len(df_flex_p) > 0:
            try:
                delta_data = df_periodo[[coluna_periodo, coluna]].copy()
                delta_data = delta_data.merge(
                    df_flex_p[[coluna_periodo, 'Flex_Bud']],
                    on=coluna_periodo, how='left'
                )
                delta_data['Flex_Bud'] = delta_data['Flex_Bud'].fillna(0)
                delta_data[coluna] = delta_data[coluna].fillna(0)
                delta_data['Delta'] = (
                    delta_data[coluna] - delta_data['Flex_Bud']
                )

                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1

                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20
                ).encode(
                    x=alt.X(
                        f'{coluna_periodo}:N', title='', sort=ordem_per,
                        axis=alt.Axis(
                            grid=False, domain=False,
                            ticks=False, labels=False
                        )
                    ),
                    y=alt.Y(
                        'Delta:Q', title='Delta (BE − Flex Bud)',
                        axis=alt.Axis(
                            grid=False, domain=True,
                            ticks=True, labels=True
                        )
                    ),
                    color=alt.Color(
                        'Delta:Q', title='Delta',
                        scale=alt.Scale(
                            domain=[delta_min, 0, delta_max],
                            range=['#00AA00', '#FFFFFF', '#FF0000'],
                            type='linear', nice=False
                        ),
                        legend=None
                    ),
                    tooltip=[
                        alt.Tooltip(f'{coluna_periodo}:N', title='Período'),
                        alt.Tooltip(
                            'Delta:Q',
                            title='Delta (BE − Flex Bud)', format=',.2f'
                        ),
                        alt.Tooltip(
                            f'{coluna}:Q', title='BE', format=',.2f'
                        ),
                        alt.Tooltip(
                            'Flex_Bud:Q', title='Flex Bud', format=',.2f'
                        )
                    ]
                ).properties(height=38)

                rotulos_delta_pos = alt.Chart(
                    delta_data[delta_data['Delta'] >= 0]
                ).mark_text(
                    align='center', baseline='bottom', dy=-12,
                    fontSize=9, fontWeight='bold'
                ).encode(
                    x=alt.X(f'{coluna_periodo}:N', sort=ordem_per),
                    y='Delta:Q',
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],
                            type='linear', nice=False
                        ), legend=None
                    )
                )

                rotulos_delta_neg = alt.Chart(
                    delta_data[delta_data['Delta'] < 0]
                ).mark_text(
                    align='center', baseline='top', dy=12,
                    fontSize=9, fontWeight='bold'
                ).encode(
                    x=alt.X(f'{coluna_periodo}:N', sort=ordem_per),
                    y='Delta:Q',
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],
                            type='linear', nice=False
                        ), legend=None
                    )
                )

                grafico_delta = (
                    grafico_delta + rotulos_delta_pos + rotulos_delta_neg
                )
            except Exception:
                grafico_delta = None

        # ── Combinar tudo ──
        if linha_flex is not None:
            grafico_principal = alt.layer(
                grafico_barras, rotulos, linha_flex
            ).resolve_scale(x='shared', y='shared')
        else:
            grafico_principal = grafico_barras + rotulos

        if grafico_delta is not None:
            grafico_final = alt.vconcat(
                grafico_delta, grafico_principal
            ).resolve_scale(x='shared')
        else:
            grafico_final = grafico_principal

        return grafico_final

    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None


# ═══════════════════════════════════════════════════════════
#  PÁGINA PRINCIPAL
# ═══════════════════════════════════════════════════════════

injetar_css_global()
render_header()

st.title("🔮 Best Estimate — Análise")
st.subheader("Custo de Produção de Veículos • Best Estimate")

# ── Sidebar Global ──
cfg = render_sidebar_global('be_ana')
ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
taxas, tipo, fator = cfg['taxas'], cfg['tipo'], cfg['fator']
sufixo = obter_sufixo_fator(fator)

# ── Carregar dados ──
df_principal = load_principal(ano)
df_be_raw = _load_forecast(ano)          # ← substitui load_principal_real
df_vol_bud = load_volume_bud(ano)
df_vol_actual = load_volume_actual(ano)
df_tempo_veic = load_tempo_veiculos(ano)

# ── Carregar dados rateados por veículo (apenas BUD — forecast não tem) ──
df_veic_bud_raw = load_custo_fp_veiculo(ano)
df_veic_real_raw = None  # ← forecast não tem rateio por veículo

if df_principal is None:
    st.error(f"❌ Dados do TC Veículos não encontrados para {ano}")
    st.info("💡 Execute o processamento na página **Extração de Dados**.")
    st.stop()

caminho_forecast = os.path.join(
    "dados", "TC_Principal", "Forecast", "forecast_completo.parquet"
)
if not os.path.exists(caminho_forecast) or df_be_raw is None or df_be_raw.empty:
    st.warning("⚠️ Dados de Best Estimate não encontrados.")
    st.info("💡 Gere os dados na página **Best Estimate (Simulador)**.")
    st.stop()

df_principal = normalizar_periodo(df_principal)

# ── Cópias raw para filtros locais da Tab 1 ──
_raw_df_principal = df_principal.copy()
_raw_df_be = normalizar_periodo(df_be_raw.copy()) if df_be_raw is not None else None
_raw_df_vol_bud = (
    normalizar_periodo(df_vol_bud.copy()) if df_vol_bud is not None else None
)
_raw_df_vol_actual = (
    normalizar_periodo(df_vol_actual.copy()) if df_vol_actual is not None else None
)

# ── Filtros (inclui Veículo como selectbox na sidebar) ──
filtros_sel = render_sidebar_filters(
    df_principal, 'be_ana', ['oficina', 'custo', 'veiculo', 'periodo']
)

# ── Determinar se usa dados rateados por veículo ──
usar_rateado = not filtros_sel.get('veiculo_todos', True)

if usar_rateado and df_veic_bud_raw is not None:
    # Dados rateados por veículo — BUD
    _df_base_bud = normalizar_periodo(df_veic_bud_raw.copy())
    if 'Custo FP Veiculo' in _df_base_bud.columns:
        _df_base_bud['Custo FP'] = _df_base_bud['Custo FP Veiculo']
    df = aplicar_filtros(_df_base_bud, filtros_sel)

    # BE: forecast não tem rateio — usar consolidado filtrado por oficina/custo/periodo
    df_be = None
    if df_be_raw is not None:
        _df_be_temp = normalizar_periodo(df_be_raw.copy())
        _filtros_be = {
            k: v for k, v in filtros_sel.items()
            if k not in ('veiculos', 'veiculo_todos')
        }
        _df_be_filt = aplicar_filtros(_df_be_temp, _filtros_be)
        if not _df_be_filt.empty:
            df_be = _df_be_filt

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
    df_be = None
    if df_be_raw is not None:
        df_be_temp = normalizar_periodo(df_be_raw.copy())
        df_be_temp = aplicar_filtros(df_be_temp, filtros_sel)
        if not df_be_temp.empty:
            df_be = df_be_temp

if df.empty:
    st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
    st.stop()

# ── Aplicar fator e moeda ──
cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
df = aplicar_fator_df(df, cols_val, fator)
df = converter_moeda_df(df, cols_val, moeda, taxas)

if df_be is not None:
    cols_val_be = [c for c in COLUNAS_MONETARIAS if c in df_be.columns]
    df_be = aplicar_fator_df(df_be, cols_val_be, fator)
    df_be = converter_moeda_df(df_be, cols_val_be, moeda, taxas)

# ── Budget Flex (calculado com dados filtrados) ──
tem_ano_df = 'Ano' in df.columns
df_flex = calcular_flex_budget(
    df, df_vol_bud, df_vol_actual, tem_ano=tem_ano_df
)

# ── df_bud = Budget, df = BE (ou Budget se sem BE) ──
df_bud = df.copy()
tem_be = df_be is not None
if tem_be:
    df = df_be  # A partir daqui, df = BE para todas exibições

# ════════════════════════════════════════
#  MÉTRICAS RESUMO
# ════════════════════════════════════════
label_valor = 'CPU' if tipo == 'CPU (Custo por Unidade)' else 'Custo'
vol_total = df_vol_bud['Volume'].sum() if df_vol_bud is not None else 0

if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
    soma = {c: df[c].sum() / vol_total for c in cols_val if c in df.columns}
else:
    soma = {c: df[c].sum() for c in cols_val if c in df.columns}

# Redis vem das linhas com Account='Redis', não de coluna separada
redis_total = extrair_redis(df)
if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
    redis_val = redis_total / vol_total
else:
    redis_val = redis_total

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric(
    f"📦 {label_valor} Desp. Primária",
    f"{simbolo} {soma.get('Despesa Primaria', 0):,.2f}{sufixo}"
)
c2.metric(
    f"🏭 {label_valor} FA",
    f"{simbolo} {soma.get('Custo FA', 0):,.2f}{sufixo}"
)
c3.metric("💰 Redis", f"{simbolo} {redis_val:,.2f}{sufixo}")
c4.metric(
    f"🚗 {label_valor} FP",
    f"{simbolo} {soma.get('Custo FP', 0):,.2f}{sufixo}"
)
c5.metric(
    "📉 D&A Dedicada",
    f"{simbolo} {soma.get('D&A dedicado', 0):,.2f}{sufixo}"
)
c6.metric(
    "✅ FP sem Dedicada",
    f"{simbolo} {soma.get('FP sem Dedicada', 0):,.2f}{sufixo}"
)

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
_save_df_vol_actual = (
    df_vol_actual.copy() if df_vol_actual is not None else None
)
_save_df_flex = df_flex.copy() if df_flex is not None else None
_save_cols_val = cols_val[:]
_save_vol_total = vol_total
_save_tem_be = tem_be

with tab1:
    st.markdown("---")

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
            default=["Todos"], key="be_t1_oficina"
        )
        _ofi_t1 = (
            _oficinas_all if "Todos" in _sel_ofi_t1
            else [x for x in _sel_ofi_t1 if x != "Todos"]
        )
    with col_f2:
        # Veículos do arquivo rateado (df_veic_bud_raw)
        _df_veic_src = None
        if df_veic_bud_raw is not None:
            _df_veic_src = normalizar_periodo(df_veic_bud_raw.copy())
            # Filtrar por oficinas selecionadas (cascata)
            if _ofi_t1 and 'Oficina' in _df_veic_src.columns:
                _df_veic_src = _df_veic_src[
                    _df_veic_src['Oficina'].isin(_ofi_t1)
                ]
        _veiculos_all = sorted(
            _df_veic_src['Veículo'].dropna().unique()
        ) if (_df_veic_src is not None
              and 'Veículo' in _df_veic_src.columns) else []
        _sel_veic_t1 = st.selectbox(
            "🚗 Veículo", ["Todos"] + _veiculos_all,
            index=0, key="be_t1_veiculo"
        )

    # Períodos: usar todos disponíveis
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
    if _sel_veic_t1 != "Todos":
        _filtros_t1['veiculos'] = [_sel_veic_t1]
    _usar_rateado_t1 = _sel_veic_t1 != "Todos"

    if _usar_rateado_t1 and df_veic_bud_raw is not None:
        _bud_t1 = normalizar_periodo(df_veic_bud_raw.copy())
        if 'Custo FP Veiculo' in _bud_t1.columns:
            _bud_t1['Custo FP'] = _bud_t1['Custo FP Veiculo']
        df_bud = aplicar_filtros(_bud_t1, _filtros_t1)

        # BE: sem rateio — usar consolidado filtrado por oficina
        _be_t1 = None
        if _raw_df_be is not None:
            _filtros_be_t1 = {'oficinas': _ofi_t1, 'periodos': _per_t1}
            _rt = aplicar_filtros(_raw_df_be, _filtros_be_t1)
            if not _rt.empty:
                _be_t1 = _rt

        df_vol_bud = _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
        if df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
            df_vol_bud = df_vol_bud[df_vol_bud['Veículo'] == _sel_veic_t1]
        df_vol_actual = (
            _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None
        )
        if df_vol_actual is not None and 'Veículo' in df_vol_actual.columns:
            df_vol_actual = df_vol_actual[
                df_vol_actual['Veículo'] == _sel_veic_t1
            ]
    else:
        df_bud = aplicar_filtros(_raw_df_principal, _filtros_t1)
        _be_t1 = None
        if _raw_df_be is not None:
            _rt = aplicar_filtros(_raw_df_be, _filtros_t1)
            if not _rt.empty:
                _be_t1 = _rt
        df_vol_bud = (
            _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
        )
        df_vol_actual = (
            _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None
        )

    if df_bud.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        df = df_bud.copy()
        df_flex = None
        vol_total = 0
        cols_val = []
        tem_be = False
    else:
        # Aplicar fator e moeda aos dados locais
        cols_val = [c for c in COLUNAS_MONETARIAS if c in df_bud.columns]
        df_bud = aplicar_fator_df(df_bud, cols_val, fator)
        df_bud = converter_moeda_df(df_bud, cols_val, moeda, taxas)

        if _be_t1 is not None:
            _cv_t1 = [c for c in COLUNAS_MONETARIAS if c in _be_t1.columns]
            _be_t1 = aplicar_fator_df(_be_t1, _cv_t1, fator)
            _be_t1 = converter_moeda_df(_be_t1, _cv_t1, moeda, taxas)

        tem_be = _be_t1 is not None
        df = _be_t1 if tem_be else df_bud.copy()

        _tem_ano_t1 = 'Ano' in df.columns
        df_flex = calcular_flex_budget(
            df_bud, df_vol_bud, df_vol_actual, tem_ano=_tem_ano_t1
        )
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
    st.subheader("📊 Resumo Best Estimate")

    # Calcular BUD e Flex BUD
    df_resumo_bud = df_bud.copy()
    if not df_resumo_bud.empty and 'Custo' in df_resumo_bud.columns:
        df_resumo_bud['Custo_str'] = (
            df_resumo_bud['Custo'].astype(str).str.lower()
        )
        df_resumo_bud['Categoria'] = df_resumo_bud['Custo_str'].apply(
            lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
        )

        bud_fixo = df_resumo_bud[
            df_resumo_bud['Categoria'] == 'Fixo'
        ]['Custo FP'].sum()
        bud_variavel = df_resumo_bud[
            df_resumo_bud['Categoria'] == 'Variável'
        ]['Custo FP'].sum()
        bud_total = bud_fixo + bud_variavel
    else:
        bud_fixo = bud_variavel = bud_total = 0

    # Calcular proporção global de volume
    if df_vol_bud is not None and df_vol_actual is not None:
        vol_budget_total = df_vol_bud['Volume'].sum()
        vol_actual_total = df_vol_actual['Volume'].sum()
        proporcao_global_tc = (
            (vol_actual_total / vol_budget_total)
            if vol_budget_total > 0 else 1
        )
    else:
        vol_budget_total = 0
        vol_actual_total = 0
        proporcao_global_tc = 1

    # Calcular Flex BUD: Fixo + (Variável × Proporção Global)
    flex_bud_total = bud_fixo + (bud_variavel * proporcao_global_tc)

    # Total BE
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
        render_kpi("BE - Flex Bud", _fmt_val(total_menos_flex))
    with k5:
        render_kpi("Best Estimate", _fmt_val(total_exibir))
    with k6:
        render_kpi("BE / Flex Bud", f"{total_div_flex:.0%}")

    render_kpi_spacer()

    # Alerta sobre volumes iguais
    volumes_iguais = abs(vol_budget_total - vol_actual_total) < 1
    if volumes_iguais and vol_budget_total > 0:
        st.warning(
            f"⚠️ **Volume Budget ({vol_budget_total:,.0f}) = "
            f"Volume Realizado ({vol_actual_total:,.0f})**  \n"
            f"Proporção = {proporcao_global_tc:.2%} → Flex BUD = BUD.  \n"
            "Verifique os dados de volume na aba **📈 Volume**."
        )

    st.divider()

    # ════════════════════════════════════════
    # Gráfico: Custo FP por Período — Best Estimate
    # ════════════════════════════════════════
    st.markdown("### Custo FP por Período — Best Estimate")

    tem_ano = 'Ano' in df.columns

    # ── Barras = BE ──
    df_periodo = None
    if 'Custo FP' in df.columns:
        df_be_graf = df.copy()
        cols_val_be = [c for c in COLUNAS_MONETARIAS if c in df_be_graf.columns]
        if tem_ano and 'Ano' in df_be_graf.columns:
            df_periodo = df_be_graf.groupby(
                ['Ano', 'Período'], as_index=False
            ).agg({c: 'sum' for c in cols_val_be})
        else:
            df_periodo = df_be_graf.groupby(
                'Período', as_index=False
            ).agg({c: 'sum' for c in cols_val_be})
        df_periodo = ordenar_por_mes(df_periodo)
        df_periodo['Período'] = df_periodo['Período'].astype(str)
        if tem_ano and 'Ano' in df_periodo.columns:
            df_periodo['Ano'] = df_periodo['Ano'].astype(str)

        # Aplicar CPU ao BE se necessário
        if tipo == 'CPU (Custo por Unidade)' and df_vol_actual is not None:
            vol_act_norm = df_vol_actual.copy()
            cols_agrup_vol = (
                ['Ano', 'Período']
                if tem_ano and 'Ano' in vol_act_norm.columns
                else ['Período']
            )
            vol_per = vol_act_norm.groupby(
                cols_agrup_vol, as_index=False
            )['Volume'].sum()
            vol_per['Período'] = vol_per['Período'].astype(str)
            if tem_ano and 'Ano' in vol_per.columns:
                vol_per['Ano'] = vol_per['Ano'].astype(str)
            df_periodo = df_periodo.merge(
                vol_per, on=cols_agrup_vol, how='left'
            )
            df_periodo['Volume'] = df_periodo['Volume'].fillna(0)
            if df_periodo['Volume'].sum() > 0:
                for c in cols_val_be:
                    if c in df_periodo.columns:
                        df_periodo[c] = calcular_cpu(
                            df_periodo[c], df_periodo['Volume']
                        )

    if (df_periodo is None or len(df_periodo) == 0
            or 'Custo FP' not in df_periodo.columns):
        st.info(
            "ℹ️ Nenhum dado de Best Estimate disponível "
            "para exibir no gráfico."
        )
    else:
        if tem_ano and 'Período_Completo' not in df_periodo.columns:
            df_periodo['Período_Completo'] = (
                df_periodo['Período'] + ' ' + df_periodo['Ano']
            )

        periodos_presentes = df_periodo['Período'].unique().tolist()
        ordem_per = [
            m for m in ORDEM_MESES if m in periodos_presentes
        ]
        if tem_ano:
            ordem_per = df_periodo['Período_Completo'].tolist()

        chart_placeholder = st.empty()
        grafico_final = create_periodo_chart(
            df_periodo, df_flex, tipo, label_valor,
            simbolo, sufixo, ordem_per, tem_ano
        )

        try:
            if grafico_final is not None:
                chart_placeholder.altair_chart(
                    grafico_final, use_container_width=True
                )
            else:
                chart_placeholder.warning(
                    "⚠️ O gráfico não pôde ser criado."
                )
        except Exception as e:
            import traceback
            chart_placeholder.error(
                f"❌ Erro ao renderizar gráfico: {str(e)}"
            )
            chart_placeholder.code(traceback.format_exc())

    st.divider()

    # ════════════════════════════════════════
    # 📊 Análise Flex por Categoria
    # ════════════════════════════════════════
    st.subheader("📊 Análise Flex por Categoria")

    _periodos_flex_all = sorted(
        df_bud['Período'].dropna().unique().tolist(),
        key=lambda x: ORDEM_MESES.index(x) if x in ORDEM_MESES else 99
    ) if not df_bud.empty and 'Período' in df_bud.columns else []

    if df_flex is not None and 'Custo' in df.columns:
        col_viz1, col_viz2, col_viz3 = st.columns([1.2, 1.5, 0.8])
        with col_viz1:
            modo_visualizacao = st.radio(
                "📊 **Visualização:**",
                ["Fixo/Variável", "Total"],
                index=0, horizontal=True,
                key="be_flex_modo_visualizacao"
            )
        with col_viz2:
            _sel_per_flex = st.multiselect(
                "📅 **Período(s):**",
                ["Todos"] + _periodos_flex_all,
                default=["Todos"],
                key="be_flex_periodo"
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
                key="be_flex_download_excel",
                use_container_width=True
            )
        st.markdown("---")

        # ── BUD: agrupar do Budget ──
        df_bud_cat = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
        df_bud_cat['Custo_str'] = (
            df_bud_cat['Custo'].astype(str).str.lower()
        )
        df_bud_cat['Categoria'] = df_bud_cat['Custo_str'].apply(
            lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
        )
        df_bud_cat_agg = df_bud_cat.groupby(
            ['Categoria', 'Período'], as_index=False
        )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})
        df_bud_cat_agg['Período'] = df_bud_cat_agg['Período'].astype(str)
        df_bud_cat_agg = ordenar_por_mes(df_bud_cat_agg)

        # ── BE: agrupar ──
        df_be_cat = df[df['Período'].isin(periodos_flex)].copy()
        if not df_be_cat.empty and 'Custo' in df_be_cat.columns:
            df_be_cat['Custo_str'] = (
                df_be_cat['Custo'].astype(str).str.lower()
            )
            df_be_cat['Categoria'] = df_be_cat['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )
            df_be_cat_agg = df_be_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
            df_be_cat_agg['Período'] = df_be_cat_agg['Período'].astype(str)
        else:
            df_be_cat_agg = pd.DataFrame(
                columns=['Categoria', 'Período', 'Total']
            )

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
            proporcao_global = (
                (vol_total_actual / vol_total_budget)
                if vol_total_budget > 0 else 1
            )
        else:
            proporcao_global = 1

        df_cat_agg = df_bud_cat_agg.merge(
            df_be_cat_agg, on=['Categoria', 'Período'], how='left'
        )
        df_cat_agg['Total'] = df_cat_agg['Total'].fillna(0)

        df_cat_agg['Flex BUD'] = df_cat_agg.apply(
            lambda r: r['BUD'] if r['Categoria'] == 'Fixo'
            else r['BUD'] * proporcao_global,
            axis=1
        )

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

        total_bud = df_cat_agg['BUD'].sum()
        total_flex_bud = df_cat_agg['Flex BUD'].sum()
        total_be = df_cat_agg['Total'].sum()
        total_flex_diff = total_flex_bud - total_bud
        total_be_diff = total_be - total_flex_bud
        total_ratio = (
            (total_be / total_flex_bud) if total_flex_bud != 0 else 0
        )

        kr1, kr2, kr3, kr4, kr5, kr6 = st.columns(6)
        with kr1:
            render_kpi("BUD", f"{simbolo} {total_bud:,.2f}{sufixo}")
        with kr2:
            render_kpi(
                "Flex - BUD",
                f"{simbolo} {total_flex_diff:+,.2f}{sufixo}"
            )
        with kr3:
            render_kpi(
                "Flex BUD", f"{simbolo} {total_flex_bud:,.2f}{sufixo}"
            )
        with kr4:
            render_kpi(
                "BE - Flex",
                f"{simbolo} {total_be_diff:+,.2f}{sufixo}"
            )
        with kr5:
            render_kpi(
                "Best Estimate", f"{simbolo} {total_be:,.2f}{sufixo}"
            )
        with kr6:
            render_kpi("BE / Flex", f"{total_ratio:.0%}")

        render_kpi_spacer()
        st.markdown("---")

        # ═══════════════════════════════════════
        # 📥 Exportar para Excel
        # ═══════════════════════════════════════
        if btn_excel:
            try:
                df_download = df_cat_agg[[
                    'Categoria', 'Período', 'BUD',
                    'Flex Bud - BUD', 'Flex BUD',
                    'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                ]].copy()
                df_download['Total / Flex Bud'] = (
                    df_download['Total / Flex Bud'].apply(
                        lambda x: f"{x:.2%}"
                    )
                )

                downloads_path = os.path.join(
                    os.path.expanduser("~"), "Downloads"
                )
                tipo_nome = (
                    "CPU" if tipo == "CPU (Custo por Unidade)"
                    else "Custo_Total"
                )
                modo_nome = (
                    "Fixo_Variavel"
                    if modo_visualizacao == "Fixo/Variável"
                    else "Total"
                )
                file_name = (
                    f"BE_Analise_Flex_{modo_nome}_{tipo_nome}_{ano}.xlsx"
                )
                file_path = os.path.join(downloads_path, file_name)

                with pd.ExcelWriter(
                    file_path, engine='openpyxl'
                ) as writer:
                    df_download.to_excel(
                        writer, index=False, sheet_name='Flex_Bud'
                    )

                st.success(f"✅ Arquivo salvo em: {file_path}")
            except Exception as e:
                st.error(f"❌ Erro ao exportar: {e}")

        # ═══════════════════════════════════════
        # Expanders Fixo/Variável → Type 05 → Account
        # ═══════════════════════════════════════
        if modo_visualizacao == "Fixo/Variável":
            df_bud_hier_base = df_bud[
                df_bud['Período'].isin(periodos_flex)
            ].copy()
            df_bud_hier_base['Custo_str'] = (
                df_bud_hier_base['Custo'].astype(str).str.lower()
            )
            df_bud_hier_base['Categoria'] = (
                df_bud_hier_base['Custo_str'].apply(
                    lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
                )
            )
            if 'Type 05' not in df_bud_hier_base.columns:
                df_bud_hier_base['Type 05'] = 'N/A'
            if 'Account' not in df_bud_hier_base.columns:
                df_bud_hier_base['Account'] = 'N/A'
            df_bud_hier = df_bud_hier_base.groupby(
                ['Categoria', 'Type 05', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

            df_be_hier = df[df['Período'].isin(periodos_flex)].copy()
            if not df_be_hier.empty and 'Custo' in df_be_hier.columns:
                df_be_hier['Custo_str'] = (
                    df_be_hier['Custo'].astype(str).str.lower()
                )
                df_be_hier['Categoria'] = df_be_hier['Custo_str'].apply(
                    lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
                )
                if 'Type 05' not in df_be_hier.columns:
                    df_be_hier['Type 05'] = 'N/A'
                if 'Account' not in df_be_hier.columns:
                    df_be_hier['Account'] = 'N/A'
                df_be_hier_agg = df_be_hier.groupby(
                    ['Categoria', 'Type 05', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
            else:
                df_be_hier_agg = pd.DataFrame(
                    columns=['Categoria', 'Type 05', 'Account', 'Total']
                )

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
            proporcao_global = (
                vol_total_act / vol_total_bud
                if vol_total_bud > 0 else 1
            )

            df_hier_agg = df_bud_hier.merge(
                df_be_hier_agg,
                on=['Categoria', 'Type 05', 'Account'], how='left'
            )
            df_hier_agg['Total'] = df_hier_agg['Total'].fillna(0)

            df_hier_agg['Flex BUD'] = df_hier_agg.apply(
                lambda r: r['BUD'] if r['Categoria'] == 'Fixo'
                else r['BUD'] * proporcao_global,
                axis=1
            )

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

            df_hier_agg['Flex Bud - BUD'] = (
                df_hier_agg['Flex BUD'] - df_hier_agg['BUD']
            )
            df_hier_agg['Total - Flex Bud'] = (
                df_hier_agg['Total'] - df_hier_agg['Flex BUD']
            )
            df_hier_agg['Total / Flex Bud'] = df_hier_agg.apply(
                lambda r: r['Total'] / r['Flex BUD']
                if r['Flex BUD'] != 0 else 0,
                axis=1
            )

            for categoria in ['Fixo', 'Variável']:
                df_cat_hier = df_hier_agg[
                    df_hier_agg['Categoria'] == categoria
                ].copy()

                if len(df_cat_hier) == 0:
                    continue

                cat_bud = df_cat_hier['BUD'].sum()
                cat_flex = df_cat_hier['Flex BUD'].sum()
                cat_total = df_cat_hier['Total'].sum()
                cat_flex_diff = cat_flex - cat_bud
                cat_be_diff = cat_total - cat_flex
                cat_ratio = (
                    cat_total / cat_flex if cat_flex != 0 else 0
                )
                total_cat_fmt = f"{simbolo} {cat_total:,.2f}{sufixo}"

                with st.expander(
                    f"💰 {categoria} - Total: {total_cat_fmt}",
                    expanded=False
                ):
                    ck1, ck2, ck3, ck4, ck5, ck6 = st.columns(6)
                    with ck1:
                        render_kpi(
                            "BUD",
                            f"{simbolo} {cat_bud:,.2f}{sufixo}"
                        )
                    with ck2:
                        render_kpi(
                            "Flex - BUD",
                            f"{simbolo} {cat_flex_diff:+,.2f}{sufixo}"
                        )
                    with ck3:
                        render_kpi(
                            "Flex BUD",
                            f"{simbolo} {cat_flex:,.2f}{sufixo}"
                        )
                    with ck4:
                        render_kpi(
                            "BE - Flex",
                            f"{simbolo} {cat_be_diff:+,.2f}{sufixo}"
                        )
                    with ck5:
                        render_kpi(
                            "Best Estimate",
                            f"{simbolo} {cat_total:,.2f}{sufixo}"
                        )
                    with ck6:
                        render_kpi(
                            "BE / Flex", f"{cat_ratio:.0%}"
                        )

                    render_kpi_spacer()

                    type05_list = df_cat_hier['Type 05'].unique()
                    for type05 in type05_list:
                        df_type05 = df_cat_hier[
                            df_cat_hier['Type 05'] == type05
                        ].copy()

                        t05_bud = df_type05['BUD'].sum()
                        t05_flex = df_type05['Flex BUD'].sum()
                        t05_total = df_type05['Total'].sum()
                        t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                        with st.expander(
                            f"📊 Type 05: {type05} - Total: {t05_fmt}",
                            expanded=False
                        ):
                            df_tabela = df_type05[[
                                'Account', 'BUD', 'Flex Bud - BUD',
                                'Flex BUD', 'Total - Flex Bud',
                                'Total', 'Total / Flex Bud'
                            ]].copy()

                            df_tabela = df_tabela[
                                (df_tabela['Total'].abs() > 0.01)
                                | (df_tabela['BUD'].abs() > 0.01)
                            ].copy()

                            if len(df_tabela) > 0:
                                html_tabela = criar_tabela_html_flex(
                                    df_tabela, simbolo, sufixo
                                )
                                st.markdown(
                                    html_tabela, unsafe_allow_html=True
                                )
                            else:
                                st.info("Sem dados para exibir.")
        else:
            # Modo Total
            df_bud_total_base = df_bud[
                df_bud['Período'].isin(periodos_flex)
            ].copy()
            if 'Type 05' not in df_bud_total_base.columns:
                df_bud_total_base['Type 05'] = 'N/A'
            if 'Account' not in df_bud_total_base.columns:
                df_bud_total_base['Account'] = 'N/A'
            df_bud_total = df_bud_total_base.groupby(
                ['Type 05', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

            df_be_total = df[
                df['Período'].isin(periodos_flex)
            ].copy()
            if 'Type 05' not in df_be_total.columns:
                df_be_total['Type 05'] = 'N/A'
            if 'Account' not in df_be_total.columns:
                df_be_total['Account'] = 'N/A'
            df_be_total_agg = df_be_total.groupby(
                ['Type 05', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})

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
            proporcao_global = (
                vol_total_act / vol_total_bud
                if vol_total_bud > 0 else 1
            )

            df_total_agg = df_bud_total.merge(
                df_be_total_agg, on=['Type 05', 'Account'], how='left'
            )
            df_total_agg['Total'] = df_total_agg['Total'].fillna(0)
            df_total_agg['Flex BUD'] = (
                df_total_agg['BUD'] * proporcao_global
            )

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

            type05_list = df_total_agg['Type 05'].unique()
            for type05 in type05_list:
                df_type05 = df_total_agg[
                    df_total_agg['Type 05'] == type05
                ].copy()

                df_type05 = df_type05[
                    (df_type05['Total'].abs() > 0.01)
                    | (df_type05['BUD'].abs() > 0.01)
                ].copy()

                if len(df_type05) == 0:
                    continue

                t05_total = df_type05['Total'].sum()
                t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                with st.expander(
                    f"📊 Type 05: {type05} - Total: {t05_fmt}",
                    expanded=False
                ):
                    t05_bud = df_type05['BUD'].sum()
                    t05_flex = df_type05['Flex BUD'].sum()
                    t05_flex_diff = t05_flex - t05_bud
                    t05_be_diff = t05_total - t05_flex
                    t05_ratio = (
                        t05_total / t05_flex if t05_flex != 0 else 0
                    )

                    tk1, tk2, tk3, tk4, tk5, tk6 = st.columns(6)
                    with tk1:
                        render_kpi(
                            "BUD",
                            f"{simbolo} {t05_bud:,.2f}{sufixo}"
                        )
                    with tk2:
                        render_kpi(
                            "Flex-BUD",
                            f"{simbolo} {t05_flex_diff:+,.2f}{sufixo}"
                        )
                    with tk3:
                        render_kpi(
                            "Flex BUD",
                            f"{simbolo} {t05_flex:,.2f}{sufixo}"
                        )
                    with tk4:
                        render_kpi(
                            "BE-Flex",
                            f"{simbolo} {t05_be_diff:+,.2f}{sufixo}"
                        )
                    with tk5:
                        render_kpi(
                            "Best Estimate",
                            f"{simbolo} {t05_total:,.2f}{sufixo}"
                        )
                    with tk6:
                        render_kpi(
                            "BE/Flex", f"{t05_ratio:.0%}"
                        )

                    render_kpi_spacer()

                    df_tabela = df_type05[[
                        'Account', 'BUD', 'Flex Bud - BUD', 'Flex BUD',
                        'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                    ]].copy()

                    html_tabela = criar_tabela_html_flex(
                        df_tabela, simbolo, sufixo
                    )
                    st.markdown(html_tabela, unsafe_allow_html=True)

    else:
        st.info(
            "ℹ️ Dados de categoria (Custo) não disponíveis para "
            "análise Flex."
        )

# ── Restaurar estado global após tab1 ──
df_bud = _save_df_bud
df = _save_df
df_vol_bud = _save_df_vol_bud
df_vol_actual = _save_df_vol_actual
df_flex = _save_df_flex
cols_val = _save_cols_val
vol_total = _save_vol_total
tem_be = _save_tem_be

# ── TAB 2: Volume ──
with tab2:
    st.subheader("Volume de Produção")

    if df_vol_bud is not None:
        df_vb = normalizar_periodo(df_vol_bud.copy())
        df_vb = ordenar_por_mes(df_vb)
        df_vb['Período'] = df_vb['Período'].astype(str)

        df_va = None
        vol_bud_total_tab2 = df_vb['Volume'].sum()
        vol_act_total_tab2 = vol_bud_total_tab2
        if df_vol_actual is not None:
            df_va = normalizar_periodo(df_vol_actual.copy())
            df_va = ordenar_por_mes(df_va)
            df_va['Período'] = df_va['Período'].astype(str)
            vol_act_total_tab2 = df_va['Volume'].sum()

        proporcao_vol = (
            (vol_act_total_tab2 / vol_bud_total_tab2)
            if vol_bud_total_tab2 > 0 else 1.0
        )

        kv1, kv2, kv3, kv4 = st.columns(4)
        with kv1:
            render_kpi("Vol Budget", f"{vol_bud_total_tab2:,.0f}")
        with kv2:
            render_kpi("Vol Actual", f"{vol_act_total_tab2:,.0f}")
        with kv3:
            render_kpi(
                "Diferença",
                f"{vol_act_total_tab2 - vol_bud_total_tab2:+,.0f}"
            )
        with kv4:
            render_kpi("Proporção", f"{proporcao_vol:.2%}")

        render_kpi_spacer()

        st.markdown("### 📊 Volume Total por Período")

        df_vb_per = df_vb.groupby(
            'Período', as_index=False
        )['Volume'].sum()
        df_vb_per['Tipo'] = 'Budget'
        ordem_per = [
            m for m in ORDEM_MESES if m in df_vb_per['Período'].values
        ]

        bar_bud = alt.Chart(df_vb_per).mark_bar().encode(
            x=alt.X(
                'Período:N', sort=ordem_per, title='Período',
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(
                'Volume:Q', title='Volume',
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            color=alt.Color(
                'Volume:Q', title='Volume',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(
                    orient='right', titleFontSize=10, labelFontSize=9
                )
            ),
            tooltip=[
                alt.Tooltip('Período:N', title='Período'),
                alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
            ],
        )

        rotulos_bud = bar_bud.mark_text(
            align='center', dy=-10, fontSize=9, color='black'
        ).encode(text=alt.Text('Volume:Q', format=','))

        layers_vol = [bar_bud, rotulos_bud]

        if df_va is not None:
            df_va_per = df_va.groupby(
                'Período', as_index=False
            )['Volume'].sum()
            df_va_per['Tipo'] = 'Realizado'

            vol_bud_total = df_vb_per['Volume'].sum()
            vol_act_total = df_va_per['Volume'].sum()
            sao_diferentes = abs(vol_bud_total - vol_act_total) > 1

            if sao_diferentes:
                line_act = alt.Chart(df_va_per).mark_line(
                    color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                    tooltip=[
                        alt.Tooltip('Período:N', title='Período'),
                        alt.Tooltip(
                            'Volume:Q', title='Volume Realizado',
                            format=','
                        )
                    ],
                )
                pontos_act = alt.Chart(df_va_per).mark_circle(
                    color='#FF6B35', size=60
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                )
                rotulos_act = alt.Chart(df_va_per).mark_text(
                    align='center', dy=-15, fontSize=9,
                    color='#FF6B35', fontWeight='bold'
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                    text=alt.Text('Volume:Q', format=',')
                )
                layers_vol.extend([line_act, pontos_act, rotulos_act])

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

        # Volume por Veículo
        st.markdown("### 📊 Volume por Veículo")

        df_vb_total_veic = df_vb.groupby(
            'Veículo', as_index=False
        )['Volume'].sum().sort_values('Volume', ascending=False)
        ordem_veiculos = df_vb_total_veic['Veículo'].tolist()

        bar_veic = alt.Chart(df_vb_total_veic).mark_bar().encode(
            x=alt.X(
                'Veículo:N', sort=ordem_veiculos, title='Veículo',
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(
                'Volume:Q', title='Volume (Unidades)',
                axis=alt.Axis(grid=False)
            ),
            color=alt.Color(
                'Volume:Q', title='Volume Budget',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(
                    orient='right', titleFontSize=10, labelFontSize=9
                )
            ),
            tooltip=[
                alt.Tooltip('Veículo:N', title='Veículo'),
                alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
            ],
        ).properties(height=360)

        rotulos_veic = bar_veic.mark_text(
            align='center', dy=-10, fontSize=9, color='black'
        ).encode(text=alt.Text('Volume:Q', format=','))

        layers_veic = [bar_veic, rotulos_veic]

        if df_va is not None:
            df_va_total_veic = df_va.groupby(
                'Veículo', as_index=False
            )['Volume'].sum()
            vol_veic_bud = df_vb_total_veic['Volume'].sum()
            vol_veic_act = df_va_total_veic['Volume'].sum()
            sao_diferentes_veic = abs(vol_veic_bud - vol_veic_act) > 1

            if sao_diferentes_veic:
                line_veic_act = alt.Chart(df_va_total_veic).mark_line(
                    color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos),
                    y='Volume:Q',
                    tooltip=[
                        alt.Tooltip('Veículo:N', title='Veículo'),
                        alt.Tooltip(
                            'Volume:Q', title='Volume Realizado',
                            format=','
                        )
                    ],
                )
                pontos_veic_act = alt.Chart(df_va_total_veic).mark_circle(
                    color='#FF6B35', size=60
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos),
                    y='Volume:Q',
                )
                layers_veic.extend([line_veic_act, pontos_veic_act])

                st.caption(
                    "🟢 Barras com degradê verde = Volume Budget | "
                    "🟠 Linha tracejada = Volume Realizado"
                )

        chart_veic = alt.layer(*layers_veic)
        st.altair_chart(chart_veic, use_container_width=True)

        # Tabela Comparativa
        st.markdown("### 📋 Tabela Budget vs Realizado por Período")

        df_comp = df_vb_per[['Período', 'Volume']].rename(
            columns={'Volume': 'Vol_Budget'}
        )
        if df_va is not None:
            df_va_per_comp = df_va.groupby(
                'Período', as_index=False
            )['Volume'].sum().rename(columns={'Volume': 'Vol_Actual'})
            df_comp = df_comp.merge(
                df_va_per_comp, on='Período', how='outer'
            )
            df_comp['Vol_Actual'] = df_comp['Vol_Actual'].fillna(0)
            df_comp['Diferença'] = (
                df_comp['Vol_Actual'] - df_comp['Vol_Budget']
            )
            df_comp['Proporção'] = (
                df_comp['Vol_Actual'] / df_comp['Vol_Budget']
            ).replace([float('inf'), float('-inf')], 0).fillna(1)
        else:
            df_comp['Vol_Actual'] = df_comp['Vol_Budget']
            df_comp['Diferença'] = 0
            df_comp['Proporção'] = 1.0

        df_comp['_ordem'] = df_comp['Período'].apply(
            lambda x: ORDEM_MESES.index(x) if x in ORDEM_MESES else 99
        )
        df_comp = df_comp.sort_values('_ordem').drop(columns='_ordem')

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

        st.dataframe(
            df_comp_fmt, use_container_width=True, hide_index=True
        )

        prop_total = df_comp[
            df_comp['Período'] == 'Total'
        ]['Proporção'].iloc[0]
        if abs(prop_total - 1.0) < 0.001:
            st.warning(
                "⚠️ **Proporção = 100%**: Volume Budget = Volume "
                "Realizado. O Flex Budget será igual ao Budget."
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
    else:
        st.warning("Dados de volume não encontrados.")

# ── TAB 3: Custos por Oficina ──
with tab3:
    st.subheader("Custos por Oficina")

    df_oficina = df.groupby('Oficina', as_index=False).agg({
        c: 'sum' for c in cols_val + ['Rateio FA'] if c in df.columns
    })
    if 'Rateio FA' in df_oficina.columns:
        df_oficina['Rateio FA'] = (
            df.groupby('Oficina')['Rateio FA'].mean().values
        )
    df_oficina = df_oficina.sort_values('Custo FP', ascending=False)

    if (tipo == 'CPU (Custo por Unidade)'
            and df_vol_bud is not None
            and 'Oficina' in df_vol_bud.columns):
        vol_ofi = df_vol_bud.groupby(
            'Oficina', as_index=False
        )['Volume'].sum()
        df_oficina = df_oficina.merge(vol_ofi, on='Oficina', how='left')
        df_oficina['Volume'] = df_oficina['Volume'].fillna(0)
        for c in cols_val:
            if c in df_oficina.columns:
                df_oficina[c] = calcular_cpu(
                    df_oficina[c], df_oficina['Volume']
                )

    top3 = df_oficina.nlargest(3, 'Custo FP')
    ko1, ko2, ko3, ko4 = st.columns(4)
    with ko1:
        render_kpi(
            "Total Custo FP",
            f"{simbolo} {df_oficina['Custo FP'].sum():,.2f}{sufixo}"
        )
    if len(top3) >= 1:
        with ko2:
            render_kpi(
                f"#{1} {top3.iloc[0]['Oficina']}",
                f"{simbolo} {top3.iloc[0]['Custo FP']:,.2f}{sufixo}"
            )
    if len(top3) >= 2:
        with ko3:
            render_kpi(
                f"#{2} {top3.iloc[1]['Oficina']}",
                f"{simbolo} {top3.iloc[1]['Custo FP']:,.2f}{sufixo}"
            )
    if len(top3) >= 3:
        with ko4:
            render_kpi(
                f"#{3} {top3.iloc[2]['Oficina']}",
                f"{simbolo} {top3.iloc[2]['Custo FP']:,.2f}{sufixo}"
            )

    render_kpi_spacer()

    col_a, col_b = st.columns(2)

    with col_a:
        bar_ofi = (
            alt.Chart(df_oficina).mark_bar(
                color='#4A90E2',
                cornerRadiusTopLeft=3, cornerRadiusTopRight=3
            ).encode(
                x=alt.X('Oficina:N', sort='-y', title='Oficina'),
                y=alt.Y(
                    'Custo FP:Q',
                    title=f'{label_valor} ({simbolo}{sufixo})'
                ),
                tooltip=[
                    'Oficina:N',
                    alt.Tooltip(
                        'Custo FP:Q', format=',.2f', title='Custo FP'
                    )
                ],
            ).properties(height=450, title='Custo FP por Oficina')
        )
        st.altair_chart(bar_ofi, use_container_width=True)

    with col_b:
        if 'Rateio FA' in df_oficina.columns:
            df_rat = df_oficina[['Oficina', 'Rateio FA']].copy()
            df_rat['Rateio %'] = df_rat['Rateio FA'] * 100
            bar_rat = (
                alt.Chart(df_rat).mark_bar(
                    cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
                ).encode(
                    x=alt.X('Oficina:N', sort='-y'),
                    y=alt.Y('Rateio %:Q', title='Rateio FA (%)'),
                    color=alt.condition(
                        alt.datum['Rateio %'] > 0,
                        alt.value('#27ae60'), alt.value('#f44336'),
                    ),
                    tooltip=[
                        'Oficina:N',
                        alt.Tooltip('Rateio %:Q', format='.2f')
                    ],
                ).properties(height=450, title='Rateio FA por Oficina')
            )
            st.altair_chart(bar_rat, use_container_width=True)

    show = df_oficina[['Oficina', 'Custo FP']].copy()
    if 'Rateio FA' in df_oficina.columns:
        show['Rateio FA %'] = df_oficina['Rateio FA'].apply(
            lambda x: f"{x*100:.2f}%"
        )
    st.markdown(
        criar_tabela_html(show, linha_total=False, simbolo=simbolo),
        unsafe_allow_html=True,
    )

    # Tabela Pivotada Oficina × Período
    with st.expander("📊 Resumo BUD vs Flex por Oficina × Período"):
        if df_flex is not None and 'Oficina' in df.columns:
            df_pivot_base = df_bud.groupby(
                ['Oficina', 'Período'], as_index=False
            )['Custo FP'].sum()

            piv_bud = df_pivot_base.pivot_table(
                index='Oficina', columns='Período',
                values='Custo FP', aggfunc='sum',
            )
            cols_ord = [
                m for m in ORDEM_MESES if m in piv_bud.columns
            ]
            piv_bud = piv_bud[cols_ord]
            piv_bud['Ano'] = piv_bud.sum(axis=1)
            piv_bud.loc['Total'] = piv_bud.sum()
            piv_bud.index.name = 'Oficina'

            fmt_bud = piv_bud.copy()
            for col in fmt_bud.columns:
                fmt_bud[col] = fmt_bud[col].apply(
                    lambda x: f"{simbolo} {x:,.0f}"
                    if pd.notna(x) else "—"
                )

            st.markdown("**📦 Budget (BUD)**")
            st.dataframe(fmt_bud, use_container_width=True)

            if 'Custo' in df_bud.columns:
                df_flex_base = df_bud.copy()
                df_flex_base['Custo_str'] = (
                    df_flex_base['Custo'].astype(str).str.lower()
                )
                df_flex_base['is_fixo'] = (
                    df_flex_base['Custo_str'].str.startswith('fix')
                )
                df_flex_base['Custo_Fixo'] = df_flex_base.apply(
                    lambda r: r['Custo FP'] if r['is_fixo'] else 0,
                    axis=1
                )
                df_flex_base['Custo_NaoFixo'] = df_flex_base.apply(
                    lambda r: r['Custo FP'] if not r['is_fixo'] else 0,
                    axis=1
                )

                df_fixo = df_flex_base.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Custo_Fixo'].sum()
                df_nfixo = df_flex_base.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Custo_NaoFixo'].sum()

                df_flex_merged = df_fixo.merge(
                    df_nfixo, on=['Oficina', 'Período'], how='outer'
                ).fillna(0)
                df_flex_merged = df_flex_merged.merge(
                    df_flex[['Período', 'Proporcao']],
                    on='Período', how='left'
                )
                df_flex_merged['Proporcao'] = (
                    df_flex_merged['Proporcao'].fillna(1)
                )
                df_flex_merged['Flex_Bud'] = (
                    df_flex_merged['Custo_Fixo']
                    + df_flex_merged['Custo_NaoFixo']
                    * df_flex_merged['Proporcao']
                )

                piv_flex = df_flex_merged.pivot_table(
                    index='Oficina', columns='Período',
                    values='Flex_Bud', aggfunc='sum',
                )
                piv_flex = piv_flex[
                    [m for m in ORDEM_MESES if m in piv_flex.columns]
                ]
                piv_flex['Ano'] = piv_flex.sum(axis=1)
                piv_flex.loc['Total'] = piv_flex.sum()
                piv_flex.index.name = 'Oficina'

                fmt_flex = piv_flex.copy()
                for col in fmt_flex.columns:
                    fmt_flex[col] = fmt_flex[col].apply(
                        lambda x: f"{simbolo} {x:,.0f}"
                        if pd.notna(x) else "—"
                    )

                st.markdown("**📈 Flex Budget**")
                st.dataframe(fmt_flex, use_container_width=True)
        else:
            st.info("Dados de Flex Budget não disponíveis.")

# ── TAB 4: Análise Flex ──
with tab4:
    st.subheader("Análise Flex — Fixo vs Variável")

    if df_flex is not None and 'Custo' in df.columns:
        bud_total_t4 = df_flex['Custo_Total_Bud'].sum()
        flex_total_t4 = df_flex['Flex_Bud'].sum()
        fixo_total_t4 = df_flex['Custo_Fixo'].sum()
        nfixo_total_t4 = df_flex['Custo_NaoFixo'].sum()

        kf1, kf2, kf3, kf4 = st.columns(4)
        with kf1:
            render_kpi(
                "Custo Fixo",
                f"{simbolo} {fixo_total_t4:,.2f}{sufixo}"
            )
        with kf2:
            render_kpi(
                "Custo Variável",
                f"{simbolo} {nfixo_total_t4:,.2f}{sufixo}"
            )
        with kf3:
            render_kpi(
                "BUD Total",
                f"{simbolo} {bud_total_t4:,.2f}{sufixo}"
            )
        with kf4:
            render_kpi(
                "Flex BUD Total",
                f"{simbolo} {flex_total_t4:,.2f}{sufixo}"
            )

        render_kpi_spacer()

        df_cat = df.copy()
        df_cat['Custo_str'] = df_cat['Custo'].astype(str).str.lower()
        df_cat['Categoria'] = df_cat['Custo_str'].apply(
            lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
        )

        df_cat_agg = df_cat.groupby(
            ['Período', 'Categoria'], as_index=False
        )['Custo FP'].sum()
        df_cat_agg = ordenar_por_mes(df_cat_agg)

        ordem_per = [
            m for m in ORDEM_MESES
            if m in df_cat_agg['Período'].unique()
        ]

        col_a, col_b = st.columns(2)

        with col_a:
            bar_cat = (
                alt.Chart(df_cat_agg).mark_bar().encode(
                    x=alt.X(
                        'Período:N', sort=ordem_per, title='Período'
                    ),
                    y=alt.Y(
                        'Custo FP:Q',
                        title=f'{label_valor} ({simbolo}{sufixo})',
                        stack=True
                    ),
                    color=alt.Color(
                        'Categoria:N',
                        scale=alt.Scale(
                            domain=['Fixo', 'Variável'],
                            range=['#3498db', '#e74c3c']
                        ),
                        legend=alt.Legend(orient='top')
                    ),
                    tooltip=[
                        'Período:N', 'Categoria:N',
                        alt.Tooltip('Custo FP:Q', format=',.0f')
                    ],
                ).properties(
                    height=400, title='Custo FP por Categoria'
                )
            )
            st.altair_chart(bar_cat, use_container_width=True)

        with col_b:
            df_cat_total = df_cat.groupby(
                'Categoria', as_index=False
            )['Custo FP'].sum()
            pie_cat = (
                alt.Chart(df_cat_total).mark_arc(
                    innerRadius=50
                ).encode(
                    theta=alt.Theta('Custo FP:Q'),
                    color=alt.Color(
                        'Categoria:N',
                        scale=alt.Scale(
                            domain=['Fixo', 'Variável'],
                            range=['#3498db', '#e74c3c']
                        )
                    ),
                    tooltip=[
                        'Categoria:N',
                        alt.Tooltip('Custo FP:Q', format=',')
                    ],
                ).properties(
                    height=400, title='Participação por Categoria'
                )
            )
            st.altair_chart(pie_cat, use_container_width=True)

        st.markdown("**📊 Resumo por Categoria**")
        df_cat_pivot = df_cat_agg.pivot_table(
            index='Categoria', columns='Período',
            values='Custo FP', aggfunc='sum',
        )
        df_cat_pivot = df_cat_pivot[
            [m for m in ORDEM_MESES if m in df_cat_pivot.columns]
        ]
        df_cat_pivot['Total'] = df_cat_pivot.sum(axis=1)
        df_cat_pivot.loc['Total'] = df_cat_pivot.sum()

        fmt_cat = df_cat_pivot.copy()
        for col in fmt_cat.columns:
            fmt_cat[col] = fmt_cat[col].apply(
                lambda x: f"{simbolo} {x:,.0f}" if pd.notna(x) else "—"
            )
        st.dataframe(fmt_cat, use_container_width=True)

        st.markdown("**📈 BUD vs Flex Budget por Categoria**")
        bud_total = df_flex['Custo_Total_Bud'].sum()
        flex_total = df_flex['Flex_Bud'].sum()
        fixo_total = df_flex['Custo_Fixo'].sum()
        nfixo_total = df_flex['Custo_NaoFixo'].sum()

        comp_data = pd.DataFrame({
            'Métrica': ['Custo Fixo', 'Custo Variável', 'Total'],
            'BUD': [fixo_total, nfixo_total, bud_total],
            'Flex BUD': [
                fixo_total,
                nfixo_total * df_flex['Proporcao'].mean(),
                fixo_total
                + nfixo_total * df_flex['Proporcao'].mean(),
            ],
        })
        comp_data['Diferença'] = comp_data['Flex BUD'] - comp_data['BUD']

        for col in ['BUD', 'Flex BUD', 'Diferença']:
            comp_data[col] = comp_data[col].apply(
                lambda x: f"{simbolo} {x:,.0f}"
            )

        st.dataframe(
            comp_data, use_container_width=True, hide_index=True
        )
    else:
        st.info(
            "Dados de categoria (Custo) não disponíveis "
            "para análise Flex."
        )

# ── TAB 5: Tempo de Produção / Custo FP por Veículo ──
with tab5:
    st.subheader("Custo FP por Veículo")

    if 'Veículo' in df.columns:
        df_veic = df.groupby('Veículo', as_index=False).agg({
            c: 'sum' for c in cols_val if c in df.columns
        })
        df_veic = df_veic.sort_values('Custo FP', ascending=False)

        if (tipo == 'CPU (Custo por Unidade)'
                and df_vol_bud is not None
                and 'Veículo' in df_vol_bud.columns):
            vol_veic = df_vol_bud.groupby(
                'Veículo', as_index=False
            )['Volume'].sum()
            df_veic = df_veic.merge(vol_veic, on='Veículo', how='left')
            df_veic['Volume'] = df_veic['Volume'].fillna(0)
            for c in cols_val:
                if c in df_veic.columns:
                    df_veic[c] = calcular_cpu(
                        df_veic[c], df_veic['Volume']
                    )

        custo_fp_total_veic = df_veic['Custo FP'].sum()
        custo_fp_media = df_veic['Custo FP'].mean()
        top3_veic = df_veic.nlargest(3, 'Custo FP')

        ktv1, ktv2, ktv3, ktv4 = st.columns(4)
        with ktv1:
            render_kpi(
                "Total Custo FP",
                f"{simbolo} {custo_fp_total_veic:,.2f}{sufixo}"
            )
        with ktv2:
            render_kpi(
                "Média/Veículo",
                f"{simbolo} {custo_fp_media:,.2f}{sufixo}"
            )
        if len(top3_veic) >= 1:
            with ktv3:
                render_kpi(
                    f"#{1} {top3_veic.iloc[0]['Veículo']}",
                    f"{simbolo} "
                    f"{top3_veic.iloc[0]['Custo FP']:,.2f}{sufixo}"
                )
        if len(top3_veic) >= 2:
            with ktv4:
                render_kpi(
                    f"#{2} {top3_veic.iloc[1]['Veículo']}",
                    f"{simbolo} "
                    f"{top3_veic.iloc[1]['Custo FP']:,.2f}{sufixo}"
                )

        render_kpi_spacer()

        col_a, col_b = st.columns(2)

        with col_a:
            bar_veic = (
                alt.Chart(df_veic).mark_bar(
                    cornerRadiusTopLeft=3, cornerRadiusTopRight=3
                ).encode(
                    x=alt.X('Veículo:N', sort='-y', title='Veículo'),
                    y=alt.Y(
                        'Custo FP:Q',
                        title=f'{label_valor} ({simbolo}{sufixo})'
                    ),
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        ),
                        legend=None
                    ),
                    tooltip=[
                        'Veículo:N',
                        alt.Tooltip(
                            'Custo FP:Q', format=',.2f',
                            title='Custo FP'
                        )
                    ],
                ).properties(
                    height=400, title='Custo FP por Veículo'
                )
            )
            st.altair_chart(bar_veic, use_container_width=True)

        with col_b:
            pie_veic = (
                alt.Chart(df_veic).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta('Custo FP:Q'),
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        )
                    ),
                    tooltip=[
                        'Veículo:N',
                        alt.Tooltip(
                            'Custo FP:Q', format=',.2f',
                            title='Custo FP'
                        )
                    ],
                ).properties(
                    height=400, title='Participação por Veículo'
                )
            )
            st.altair_chart(pie_veic, use_container_width=True)

        st.markdown("**Custo FP por Veículo**")
        st.markdown(
            criar_tabela_html(
                df_veic[['Veículo', 'Custo FP']],
                linha_total=True, simbolo=simbolo
            ),
            unsafe_allow_html=True,
        )

    # Seção adicional: Tempo de Produção
    st.divider()
    st.markdown("### Tempo de Produção — Veículos vs Fluxo Anexo")

    if df_tempo_veic is not None:
        df_tv = normalizar_periodo(df_tempo_veic.copy())
        col_c, col_d = st.columns(2)

        with col_c:
            df_tv_of = df_tv.groupby(
                'Oficina', as_index=False
            )['Tempo Veic'].sum()
            bar_tv = (
                alt.Chart(df_tv_of).mark_bar(
                    color='#4A90E2',
                    cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
                ).encode(
                    x=alt.X('Oficina:N', sort='-y'),
                    y=alt.Y('Tempo Veic:Q'),
                    tooltip=[
                        'Oficina:N',
                        alt.Tooltip('Tempo Veic:Q', format=',')
                    ],
                ).properties(
                    height=400, title='Tempo Veículo por Oficina'
                )
            )
            st.altair_chart(bar_tv, use_container_width=True)

        with col_d:
            df_fa_tempo = load_volume_fa(ano)
            if df_fa_tempo is not None:
                df_fa_tempo = normalizar_periodo(df_fa_tempo)
                df_fa_agg = df_fa_tempo.groupby(
                    'Oficina', as_index=False
                )['Tempo FA'].sum()
                df_tv_agg = df_tv.groupby(
                    'Oficina', as_index=False
                )['Tempo Veic'].sum()
                df_comp_tempo = pd.merge(
                    df_tv_agg, df_fa_agg, on='Oficina', how='outer'
                ).fillna(0)
                df_comp_long = df_comp_tempo.melt(
                    id_vars='Oficina',
                    value_vars=['Tempo Veic', 'Tempo FA'],
                    var_name='Tipo', value_name='Tempo',
                )
                bar_comp = (
                    alt.Chart(df_comp_long).mark_bar().encode(
                        x=alt.X('Oficina:N', sort='-y'),
                        y='Tempo:Q',
                        color=alt.Color(
                            'Tipo:N',
                            scale=alt.Scale(
                                domain=['Tempo Veic', 'Tempo FA'],
                                range=['#4A90E2', '#27ae60']
                            )
                        ),
                        xOffset='Tipo:N',
                        tooltip=[
                            'Oficina:N', 'Tipo:N',
                            alt.Tooltip('Tempo:Q', format=',')
                        ],
                    ).properties(
                        height=400,
                        title='Tempo Veículo vs Tempo FA'
                    )
                )
                st.altair_chart(bar_comp, use_container_width=True)

        st.markdown("**EST e Tempo por Veículo e Oficina**")
        df_tv_tab = df_tv.groupby(
            ['Oficina', 'Veículo'], as_index=False
        ).agg({
            'EST': 'first', 'Volume': 'sum', 'Tempo Veic': 'sum',
        }).sort_values(
            ['Oficina', 'Tempo Veic'], ascending=[True, False]
        )
        st.dataframe(
            df_tv_tab, use_container_width=True, hide_index=True
        )
    else:
        st.info("Dados de tempo de produção não disponíveis.")

# ── TAB 6: Dados Detalhados ──
with tab6:
    st.subheader("Dados Detalhados")

    # Seção 1: Dados BE
    with st.expander("📋 Dados Best Estimate (Custo FP)", expanded=True):
        if 'Account' in df.columns:
            accounts = sorted(df['Account'].dropna().unique())
            account_sel = st.multiselect(
                "Filtrar por Account", accounts, default=[],
                key='be_account_detail'
            )
            df_det = (
                df[df['Account'].isin(account_sel)].copy()
                if account_sel else df.copy()
            )
        else:
            df_det = df.copy()

        st.dataframe(
            df_det, use_container_width=True, hide_index=True
        )
        st.caption(
            f"Total de linhas: {len(df_det):,} | "
            f"Moeda: {moeda} | {tipo}"
        )

        csv = df_det.to_csv(index=False, sep=';', decimal=',')
        st.download_button(
            "📥 Baixar Dados BE (CSV)", data=csv,
            file_name=f"tc_principal_be_{ano}.csv", mime="text/csv"
        )

    # Seção 2: Dados Budget
    with st.expander(
        "🧾 Dados Budget (BUD e Flex BUD)", expanded=False
    ):
        if df_flex is not None and not df_flex.empty:
            st.dataframe(
                df_flex, use_container_width=True, hide_index=True
            )
            st.caption(
                f"Total de linhas: {len(df_flex):,} | Moeda: {moeda}"
            )

            csv_bud = df_flex.to_csv(
                index=False, sep=';', decimal=','
            )
            st.download_button(
                "📥 Baixar Dados Budget (CSV)", data=csv_bud,
                file_name=f"tc_principal_bud_flex_{ano}.csv",
                mime="text/csv", key='be_download_bud_flex'
            )
        else:
            st.info("ℹ️ Dados de Budget/Flex não disponíveis.")

st.divider()

# Rodapé
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
    📚 Documentação Completa do Sistema TC | Versão {versao_rodape} \
| {mes_rodape} {ano_rodape}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)
