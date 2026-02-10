"""
TC Principal — Waterfall
Gráfico Waterfall mostrando decomposição de custo por fase.
Waterfall permanece em Plotly (sem equivalente nativo Altair).
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.graph_objects as go
from datetime import datetime

from tc_core.finance.currency import converter_moeda, obter_simbolo_moeda
from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    load_principal, normalizar_periodo, ordenar_por_mes,
    aplicar_fator, aplicar_fator_df, converter_moeda_df,
    obter_sufixo_fator, calcular_cpu,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, render_sidebar_filters, aplicar_filtros,
)


def render():
    injetar_css_global()
    render_header()

    st.title("🌊 Waterfall — TC Planta Principal")
    st.subheader("Decomposição de Custos • Budget")

    # ── Sidebar ──
    cfg = render_sidebar_global('wf')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, tipo, fator = cfg['taxas'], cfg['tipo'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    df = load_principal(ano)
    if df is None:
        st.error("Dados não encontrados.")
        st.stop()
    df = normalizar_periodo(df)

    # Filtros
    filtros_sel = render_sidebar_filters(df, 'wf')
    df = aplicar_filtros(df, filtros_sel)

    if df.empty:
        st.warning("Nenhum dado com os filtros selecionados.")
        st.stop()

    # Aplicar fator e moeda
    cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
    df = aplicar_fator_df(df, cols_val, fator)
    df = converter_moeda_df(df, cols_val, moeda, taxas)

    # ── Waterfall global (Plotly) ──
    st.subheader("Waterfall — Decomposição do Custo Total")

    despesa = df['Despesa Primaria'].sum()
    redis = df['Redis'].sum()
    custo_fa = df['Custo FA'].sum()
    dea = df['D&A dedicado'].sum()
    custo_fp = df['Custo FP'].sum()

    fig = go.Figure(go.Waterfall(
        name="Custo",
        orientation="v",
        measure=["absolute", "relative", "relative", "relative", "total"],
        x=["Desp. Primária", "Redis", "Custo FA", "D&A Dedicada", "Custo FP"],
        y=[despesa, redis, custo_fa, dea, custo_fp],
        text=[f"{simbolo} {v:,.0f}{sufixo}" for v in [despesa, redis, custo_fa, dea, custo_fp]],
        textposition="outside",
        connector=dict(line=dict(color="#888", width=1)),
        increasing=dict(marker=dict(color='#f44336')),
        decreasing=dict(marker=dict(color='#4CAF50')),
        totals=dict(marker=dict(color='#4A90E2')),
    ))
    fig.update_layout(
        height=550, template='plotly_dark',
        yaxis_title=f'Valor ({simbolo}{sufixo})',
        showlegend=False, margin=dict(t=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Waterfall por oficina (Plotly) ──
    st.subheader("Waterfall por Oficina")

    df_of = df.groupby('Oficina', as_index=False).agg({
        c: 'sum' for c in cols_val
    }).sort_values('Custo FP', ascending=False)

    n_cols = min(3, len(df_of))
    if n_cols > 0:
        cols = st.columns(n_cols)
        for idx, (_, row) in enumerate(df_of.iterrows()):
            with cols[idx % n_cols]:
                vals = {
                    'Desp. Prim.': row['Despesa Primaria'],
                    'Redis': row['Redis'],
                    'Custo FA': row['Custo FA'],
                    'D&A Ded.': row['D&A dedicado'],
                    'Custo FP': row['Custo FP'],
                }
                fig_of = go.Figure(go.Waterfall(
                    orientation="v",
                    measure=["absolute", "relative", "relative", "relative", "total"],
                    x=list(vals.keys()),
                    y=list(vals.values()),
                    text=[f"{v:,.0f}" for v in vals.values()],
                    textposition="outside", textfont=dict(size=9),
                    connector=dict(line=dict(color="#666", width=0.5)),
                    increasing=dict(marker=dict(color='#f44336')),
                    decreasing=dict(marker=dict(color='#4CAF50')),
                    totals=dict(marker=dict(color='#4A90E2')),
                ))
                fig_of.update_layout(
                    height=350, template='plotly_dark',
                    title=dict(text=f"Oficina {row['Oficina']}", font=dict(size=14)),
                    margin=dict(t=50, b=20, l=30, r=10),
                    yaxis=dict(title=''), showlegend=False,
                )
                st.plotly_chart(fig_of, use_container_width=True)

    # ── Custo FP Mensal (Altair) ──
    st.subheader("Custo FP Mensal")

    df_mes = df.groupby('Período', as_index=False)['Custo FP'].sum()
    df_mes = ordenar_por_mes(df_mes)
    df_mes['Período'] = df_mes['Período'].astype(str)
    ordem_per = [m for m in ORDEM_MESES if m in df_mes['Período'].values]

    bar_mensal = (alt.Chart(df_mes).mark_bar(
        cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
    ).encode(
        x=alt.X('Período:N', sort=ordem_per, title='Período'),
        y=alt.Y('Custo FP:Q', title=f'Custo FP ({simbolo}{sufixo})'),
        color=alt.condition(
            alt.datum['Custo FP'] >= 0, alt.value('#4A90E2'), alt.value('#f44336'),
        ),
        tooltip=['Período:N', alt.Tooltip('Custo FP:Q', format=',.0f')],
    ).properties(height=400)
     .configure_view(strokeWidth=0)
     .configure_axis(labelFontSize=11, titleFontSize=13))
    st.altair_chart(bar_mensal, use_container_width=True)

    st.divider()
    st.caption(f"TC — Planta Principal | Waterfall | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
