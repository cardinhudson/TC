"""
TC Principal — Best Estimate (Análise)
Análise de KPIs, tendências e CPU de custo.
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    load_principal, load_volume_bud, load_tempo_veiculos,
    normalizar_periodo, ordenar_por_mes,
    aplicar_fator_df, converter_moeda_df,
    obter_sufixo_fator, calcular_cpu,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, render_sidebar_filters, aplicar_filtros,
    criar_tabela_html,
)


def render():
    injetar_css_global()
    render_header()

    st.title("📈 Best Estimate (Análise)")
    st.subheader("KPIs, Tendências e CPU • TC Planta Principal")

    # ── Sidebar ──
    cfg = render_sidebar_global('be_an')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, fator = cfg['taxas'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    df = load_principal(ano)
    df_vol = load_volume_bud(ano)
    df_tempo = load_tempo_veiculos(ano)

    if df is None:
        st.error("Dados não encontrados.")
        st.stop()

    df = normalizar_periodo(df)

    # Filtros
    filtros_sel = render_sidebar_filters(df, 'be_an', filtros=['oficina', 'periodo'])
    df = aplicar_filtros(df, filtros_sel)

    if df.empty:
        st.warning("Nenhum dado.")
        st.stop()

    # Aplicar fator e moeda
    cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
    df = aplicar_fator_df(df, cols_val, fator)
    df = converter_moeda_df(df, cols_val, moeda, taxas)

    # ════════════════════════════════════════
    #  TABS
    # ════════════════════════════════════════
    tab1, tab2, tab3 = st.tabs([
        "📊 KPIs e CPU", "📈 Tendências Mensais", "🏭 Análise por Oficina",
    ])

    # ── TAB 1: KPIs e CPU ──
    with tab1:
        st.subheader("KPIs de Custo por Unidade (CPU)")

        custo_fp = df['Custo FP'].sum()
        fp_sem = df['FP sem Dedicada'].sum()
        dea_total = df['D&A dedicado'].sum()
        custo_fa = df['Custo FA'].sum()

        vol_total = df_vol['Volume'].sum() if df_vol is not None else 0

        if vol_total > 0:
            cpu_fp = custo_fp / vol_total
            cpu_fp_sem = fp_sem / vol_total
            cpu_dea = dea_total / vol_total
            cpu_fa = custo_fa / vol_total
        else:
            cpu_fp = cpu_fp_sem = cpu_dea = cpu_fa = 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("CPU — Custo FP", f"{simbolo} {cpu_fp:,.2f}{sufixo}")
        c2.metric("CPU — FP sem Ded.", f"{simbolo} {cpu_fp_sem:,.2f}{sufixo}")
        c3.metric("CPU — D&A Ded.", f"{simbolo} {cpu_dea:,.2f}{sufixo}")
        c4.metric("CPU — Custo FA", f"{simbolo} {cpu_fa:,.2f}{sufixo}")
        st.caption(f"Volume total: {vol_total:,.0f} veículos")

        st.divider()

        # Composição do Custo FP (Altair donut)
        st.markdown("**Composição do Custo FP**")
        df_comp = pd.DataFrame({
            'Componente': ['FP sem Dedicada', 'D&A Dedicada'],
            'Valor': [abs(fp_sem), abs(dea_total)],
        })
        donut = (alt.Chart(df_comp).mark_arc(innerRadius=50).encode(
            theta='Valor:Q',
            color=alt.Color('Componente:N',
                            scale=alt.Scale(domain=['FP sem Dedicada', 'D&A Dedicada'],
                                            range=['#4A90E2', '#FF9800'])),
            tooltip=['Componente:N', alt.Tooltip('Valor:Q', format=',.0f')],
        ).properties(height=400, title='Composição do Custo FP'))
        st.altair_chart(donut, use_container_width=True)

    # ── TAB 2: Tendências Mensais ──
    with tab2:
        st.subheader("Tendências Mensais")

        df_mensal = df.groupby('Período', as_index=False).agg({c: 'sum' for c in cols_val})
        df_mensal = ordenar_por_mes(df_mensal)
        df_mensal['Período'] = df_mensal['Período'].astype(str)
        ordem_per = [m for m in ORDEM_MESES if m in df_mensal['Período'].values]

        # CPU mensal + Volume
        if df_vol is not None:
            df_vol_m = normalizar_periodo(df_vol.copy())
            df_vol_mensal = df_vol_m.groupby('Período', as_index=False)['Volume'].sum()
            df_vol_mensal['Período'] = df_vol_mensal['Período'].astype(str)
            df_mensal = df_mensal.merge(df_vol_mensal, on='Período', how='left')
            df_mensal['Volume'] = df_mensal['Volume'].fillna(0)
            df_mensal['CPU FP'] = calcular_cpu(df_mensal['Custo FP'], df_mensal['Volume'])

            # Dual axis: Volume bars + CPU line
            base = alt.Chart(df_mensal).encode(
                x=alt.X('Período:N', sort=ordem_per, title='Período'),
            )

            bar_vol = base.mark_bar(opacity=0.4, color='#27ae60').encode(
                y=alt.Y('Volume:Q', title='Volume', axis=alt.Axis(titleColor='#27ae60')),
                tooltip=['Período:N', alt.Tooltip('Volume:Q', format=',')],
            )

            line_cpu = base.mark_line(color='#f44336', strokeWidth=3, point=True).encode(
                y=alt.Y('CPU FP:Q', title=f'CPU FP ({simbolo}{sufixo})',
                         axis=alt.Axis(titleColor='#f44336')),
                tooltip=['Período:N', alt.Tooltip('CPU FP:Q', format=',.2f')],
            )

            chart_cpu = (alt.layer(bar_vol, line_cpu)
                         .resolve_scale(y='independent')
                         .properties(height=450, title='CPU FP vs Volume Mensal')
                         .configure_view(strokeWidth=0))
            st.altair_chart(chart_cpu, use_container_width=True)

        # Evolução custos
        metricas_trend = ['Custo FP', 'FP sem Dedicada', 'D&A dedicado', 'Custo FA']
        metricas_disp = [m for m in metricas_trend if m in df_mensal.columns]
        df_trend = df_mensal.melt(
            id_vars='Período', value_vars=metricas_disp,
            var_name='Métrica', value_name='Valor',
        )
        cores_trend = {'Custo FP': '#4A90E2', 'FP sem Dedicada': '#27ae60',
                       'D&A dedicado': '#FF9800', 'Custo FA': '#9C27B0'}

        line_trend = (alt.Chart(df_trend).mark_line(point=True, strokeWidth=2).encode(
            x=alt.X('Período:N', sort=ordem_per),
            y=alt.Y('Valor:Q', title=f'Valor ({simbolo}{sufixo})'),
            color=alt.Color('Métrica:N',
                            scale=alt.Scale(domain=list(cores_trend.keys()),
                                            range=list(cores_trend.values())),
                            legend=alt.Legend(orient='top')),
            strokeDash=alt.condition(
                alt.datum.Métrica == 'Custo FA',
                alt.value([5, 3]),
                alt.value([0]),
            ),
            tooltip=['Período:N', 'Métrica:N', alt.Tooltip('Valor:Q', format=',.0f')],
        ).properties(height=450, title='Evolução Mensal dos Custos')
         .configure_view(strokeWidth=0))
        st.altair_chart(line_trend, use_container_width=True)

    # ── TAB 3: Análise por Oficina ──
    with tab3:
        st.subheader("Análise por Oficina")

        df_ofi = df.groupby('Oficina', as_index=False).agg({
            c: 'sum' for c in cols_val
        })
        if 'Rateio FA' in df.columns:
            rateio_mean = df.groupby('Oficina')['Rateio FA'].mean()
            df_ofi = df_ofi.merge(
                rateio_mean.reset_index(), on='Oficina', how='left',
            )
        df_ofi = df_ofi.sort_values('Custo FP', ascending=False)

        # Heatmap (Altair)
        metricas_heat = ['FP sem Dedicada', 'D&A dedicado', 'Custo FA']
        metricas_heat = [m for m in metricas_heat if m in df_ofi.columns]
        df_heat = df_ofi.melt(
            id_vars='Oficina', value_vars=metricas_heat,
            var_name='Componente', value_name='Valor',
        )

        heatmap = (alt.Chart(df_heat).mark_rect(cornerRadius=3).encode(
            x=alt.X('Oficina:N', title='Oficina'),
            y=alt.Y('Componente:N', title=''),
            color=alt.Color('Valor:Q', scale=alt.Scale(scheme='blues'),
                            legend=alt.Legend(title=f'{simbolo}{sufixo}')),
            tooltip=['Oficina:N', 'Componente:N', alt.Tooltip('Valor:Q', format=',.0f')],
        ).properties(height=250, title='Mapa de Calor — Custos por Oficina')
         .configure_view(strokeWidth=0))
        st.altair_chart(heatmap, use_container_width=True)

        # Ranking tabela
        st.markdown("**Ranking de Oficinas por Custo FP**")
        df_rank = df_ofi.copy()
        total_fp = df_rank['Custo FP'].sum()
        df_rank['% do Total'] = (df_rank['Custo FP'] / total_fp * 100).round(1) if total_fp != 0 else 0
        if 'Rateio FA' in df_rank.columns:
            df_rank['Rateio FA'] = df_rank['Rateio FA'].apply(lambda x: f"{x*100:.2f}%")
        show_cols = ['Oficina', 'Custo FP', 'FP sem Dedicada', 'D&A dedicado',
                     'Custo FA', '% do Total']
        show_cols = [c for c in show_cols if c in df_rank.columns]
        if 'Rateio FA' in df_rank.columns:
            show_cols.append('Rateio FA')
        st.markdown(criar_tabela_html(df_rank[show_cols], linha_total=False, simbolo=simbolo),
                    unsafe_allow_html=True)

        # Tempo x Custo scatter
        if df_tempo is not None:
            st.divider()
            st.markdown("**Tempo de Produção vs Custo por Oficina**")
            df_t = normalizar_periodo(df_tempo.copy()).groupby('Oficina', as_index=False)['Tempo Veic'].sum()
            df_scatter = pd.merge(
                df_ofi[['Oficina', 'Custo FP']], df_t, on='Oficina', how='left',
            ).fillna(0)

            # Circles + labels
            circles = (alt.Chart(df_scatter).mark_circle(opacity=0.7, color='#4A90E2').encode(
                x='Tempo Veic:Q',
                y='Custo FP:Q',
                size=alt.Size('Custo FP:Q', legend=None),
                tooltip=['Oficina:N', alt.Tooltip('Tempo Veic:Q', format=','),
                         alt.Tooltip('Custo FP:Q', format=',.0f')],
            ))
            labels = (alt.Chart(df_scatter).mark_text(align='left', dx=8, fontSize=11).encode(
                x='Tempo Veic:Q',
                y='Custo FP:Q',
                text='Oficina:N',
            ))
            scatter_chart = ((circles + labels)
                             .properties(height=400, title='Tempo Veículo vs Custo FP')
                             .configure_view(strokeWidth=0))
            st.altair_chart(scatter_chart, use_container_width=True)

    st.divider()
    st.caption(f"TC — Planta Principal | Análise | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
