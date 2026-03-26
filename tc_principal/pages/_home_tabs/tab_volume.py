"""Tab 2: Volume — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

from tc_principal.shared import (
    ORDEM_MESES, ordenar_por_mes,
)
from tc_principal.ui_components import render_kpi, render_kpi_spacer

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None


def render(ctx):
    """Renderiza a aba Volume."""
    # ── Desempacotar contexto ──
    ano = ctx.ano
    simbolo = ctx.simbolo
    sufixo = ctx.sufixo
    df_vol_bud = ctx.df_vol_bud
    df_vol_actual = ctx.df_vol_actual

    st.subheader("Volume de Produção")

    if df_vol_bud is not None:
        # Preparar dados Budget
        df_vb = df_vol_bud.copy()
        df_vb = ordenar_por_mes(df_vb)
        df_vb['Período'] = df_vb['Período'].astype(str)

        # Preparar dados Actual (se existir)
        df_va = None
        if df_vol_actual is not None:
            df_va = df_vol_actual.copy()
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

        # Agregar Realizado por período (barras verdes)
        df_va_per = None
        ordem_per = [m for m in ORDEM_MESES if m in df_vb.groupby('Período', as_index=False)['Volume'].sum()['Período'].values]

        if df_va is not None:
            df_va_per = df_va.groupby('Período', as_index=False)['Volume'].sum()
            df_va_per['Tipo'] = 'Realizado'
            ordem_per = [m for m in ORDEM_MESES if m in df_va_per['Período'].values]

        # Agregar Budget por período (linha tracejada)
        df_vb_per = df_vb.groupby('Período', as_index=False)['Volume'].sum()
        df_vb_per['Tipo'] = 'Budget'
        if not ordem_per:
            ordem_per = [m for m in ORDEM_MESES if m in df_vb_per['Período'].values]

        # Determinar qual df vai nas barras
        df_bar_per = df_va_per if df_va_per is not None else df_vb_per

        # Barras de Realizado com degradê verde
        bar_real = alt.Chart(df_bar_per).mark_bar().encode(
            x=alt.X('Período:N', sort=ordem_per, title='Período',
                    axis=alt.Axis(grid=False, domain=True, ticks=True)),
            y=alt.Y('Volume:Q', title='Volume',
                    axis=alt.Axis(grid=False, domain=True, ticks=True)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(orient='right', titleFontSize=10, labelFontSize=12)
            ),
            tooltip=[
                alt.Tooltip('Período:N', title='Período'),
                alt.Tooltip('Volume:Q', title='Volume Realizado', format=',')
            ],
        )

        # Rótulos nas barras
        rotulos_real = bar_real.mark_text(
            align='center', dy=-10, fontSize=12, color='black'
        ).encode(text=alt.Text('Volume:Q', format=','))

        layers_vol = [bar_real, rotulos_real]

        # Linha Budget (se existir e for diferente de Realizado)
        if df_va_per is not None:
            # Verificar se são diferentes
            vol_act_total = df_va_per['Volume'].sum()
            vol_bud_total = df_vb_per['Volume'].sum()
            sao_diferentes = abs(vol_bud_total - vol_act_total) > 1

            if sao_diferentes:
                # Linha tracejada laranja para Budget
                line_bud = alt.Chart(df_vb_per).mark_line(
                    color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                    tooltip=[
                        alt.Tooltip('Período:N', title='Período'),
                        alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
                    ],
                )
                # Pontos na linha
                pontos_bud = alt.Chart(df_vb_per).mark_circle(
                    color='#FF6B35', size=60
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                )
                # Rótulos na linha
                rotulos_bud = alt.Chart(df_vb_per).mark_text(
                    align='center', dy=-15, fontSize=12, color='#FF6B35',
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Período:N', sort=ordem_per),
                    y='Volume:Q',
                    text=alt.Text('Volume:Q', format=',')
                )
                layers_vol.extend([line_bud, pontos_bud, rotulos_bud])

                # Legenda manual
                st.caption(
                    "📊 Barras com degradê verde = Volume Realizado | "
                    "🟠 Linha tracejada = Volume Budget"
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

        # Agregar Realizado por veículo (barras) — ou Budget se Actual não existir
        if df_va is not None:
            df_va_total_veic = df_va.groupby(
                'Veículo', as_index=False
            )['Volume'].sum().sort_values('Volume', ascending=False)
            ordem_veiculos = df_va_total_veic['Veículo'].tolist()
            df_bar_veic = df_va_total_veic
        else:
            df_bar_veic = df_vb.groupby(
                'Veículo', as_index=False
            )['Volume'].sum().sort_values('Volume', ascending=False)
            ordem_veiculos = df_bar_veic['Veículo'].tolist()

        # Agregar Budget por veículo (linha)
        df_vb_total_veic = df_vb.groupby(
            'Veículo', as_index=False
        )['Volume'].sum()

        # Gráfico de barras com degradê verde = Realizado
        bar_veic = alt.Chart(df_bar_veic).mark_bar().encode(
            x=alt.X('Veículo:N', sort=ordem_veiculos, title='Veículo',
                    axis=alt.Axis(grid=False, domain=True, ticks=True)),
            y=alt.Y('Volume:Q', title='Volume (Unidades)',
                    axis=alt.Axis(grid=False)),
            color=alt.Color(
                'Volume:Q',
                title='Volume Realizado',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(orient='right', titleFontSize=10, labelFontSize=12)
            ),
            tooltip=[
                alt.Tooltip('Veículo:N', title='Veículo'),
                alt.Tooltip('Volume:Q', title='Volume Realizado', format=',')
            ],
        ).properties(height=360)

        # Rótulos nas barras
        rotulos_veic = bar_veic.mark_text(
            align='center', dy=-10, fontSize=12, color='black'
        ).encode(text=alt.Text('Volume:Q', format=','))

        layers_veic = [bar_veic, rotulos_veic]

        # Adicionar linha Budget (se Actual existir e for diferente)
        if df_va is not None:
            # Verificar se são diferentes
            vol_veic_act = df_va_total_veic['Volume'].sum()
            vol_veic_bud = df_vb_total_veic['Volume'].sum()
            sao_diferentes_veic = abs(vol_veic_bud - vol_veic_act) > 1

            if sao_diferentes_veic:
                # Linha tracejada laranja para Volume Budget
                line_veic_bud = alt.Chart(df_vb_total_veic).mark_line(
                    color='#FF6B35', strokeDash=[5, 3], strokeWidth=2
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos),
                    y='Volume:Q',
                    tooltip=[
                        alt.Tooltip('Veículo:N', title='Veículo'),
                        alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
                    ],
                )
                # Pontos na linha
                pontos_veic_bud = alt.Chart(df_vb_total_veic).mark_circle(
                    color='#FF6B35', size=60
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos),
                    y='Volume:Q',
                )
                layers_veic.extend([line_veic_bud, pontos_veic_bud])

                # Legenda
                st.caption(
                    "🟢 Barras com degradê verde = Volume Realizado | "
                    "🟠 Linha tracejada = Volume Budget"
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

            st.dataframe(df_comp_fmt, width="stretch", hide_index=True)

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
                width="stretch",
                hide_index=True,
            )

        # ─── Tabela 2: Volume Actual ───
        if df_va is not None:
            with st.expander("📋 Volume Actual (Real) — Tabela por Veículo × Mês", expanded=False):
                _pivot_act = _build_pivot_volume(df_va, 'Volume')
                _pivot_act_fmt = _formatar_pivot(_pivot_act)
                st.dataframe(
                    _pivot_act_fmt,
                    width="stretch",
                    hide_index=True,
                )
        else:
            with st.expander("📋 Volume Actual (Real) — Tabela por Veículo × Mês", expanded=False):
                st.info("Dados de Volume Actual não disponíveis para este ano.")

    else:
        st.warning("Dados de volume não encontrados.")

