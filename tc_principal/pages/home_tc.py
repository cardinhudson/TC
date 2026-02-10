"""
TC Principal — Home (Planta Principal)
Dashboard com visão geral do custo de produção de veículos.
Padrão visual TC Ext: Altair, CSS global, seletores universais.
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    load_principal, load_volume_bud, load_volume_actual,
    load_tempo_veiculos, load_dea_dedicado, load_volume_fa,
    normalizar_periodo, ordenar_por_mes,
    calcular_flex_budget, aplicar_fator_df,
    converter_moeda_df, obter_sufixo_fator, calcular_cpu,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, render_sidebar_filters, aplicar_filtros,
    criar_tabela_html, render_kpi, render_kpi_spacer,
)


def render():
    """Renderiza a página Home do TC (Planta Principal)."""

    injetar_css_global()
    render_header()

    st.title("🏭 Dashboard TC Planta Principal")
    st.subheader("Custo de Produção de Veículos • Budget")

    # ── Sidebar Global ──
    cfg = render_sidebar_global('home')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, tipo, fator = cfg['taxas'], cfg['tipo'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    # ── Carregar dados ──
    df_principal = load_principal(ano)
    df_vol_bud = load_volume_bud(ano)
    df_vol_actual = load_volume_actual(ano)
    df_tempo_veic = load_tempo_veiculos(ano)

    if df_principal is None:
        st.error(f"❌ Dados do TC Principal não encontrados para {ano}")
        st.info("💡 Execute o processamento na página **Extração de Dados**.")
        st.stop()

    df_principal = normalizar_periodo(df_principal)

    # ── Filtros ──
    filtros_sel = render_sidebar_filters(df_principal, 'home')
    df = aplicar_filtros(df_principal, filtros_sel)

    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        st.stop()

    # ── Aplicar fator e moeda ──
    cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
    df = aplicar_fator_df(df, cols_val, fator)
    df = converter_moeda_df(df, cols_val, moeda, taxas)

    # ── Budget Flex ──
    df_flex = calcular_flex_budget(df_principal, df_vol_bud, df_vol_actual)
    if df_flex is not None:
        df_flex = aplicar_fator_df(
            df_flex, ['Custo_Fixo', 'Custo_NaoFixo', 'Custo_Total_Bud', 'Flex_Bud'], fator,
        )
        df_flex = converter_moeda_df(
            df_flex, ['Custo_Fixo', 'Custo_NaoFixo', 'Custo_Total_Bud', 'Flex_Bud'], moeda, taxas,
        )

    # ════════════════════════════════════════
    #  MÉTRICAS RESUMO
    # ════════════════════════════════════════
    label_valor = 'CPU' if tipo == 'CPU (Custo por Unidade)' else 'Custo'
    vol_total = df_vol_bud['Volume'].sum() if df_vol_bud is not None else 0

    if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
        soma = {c: df[c].sum() / vol_total for c in cols_val}
    else:
        soma = {c: df[c].sum() for c in cols_val}

    c1, c2, c3 = st.columns(3)
    c1.metric(f"📦 {label_valor} Desp. Primária", f"{simbolo} {soma.get('Despesa Primaria', 0):,.0f}{sufixo}")
    c2.metric(f"🏭 {label_valor} FA", f"{simbolo} {soma.get('Custo FA', 0):,.0f}{sufixo}")
    c3.metric("💰 Redis", f"{simbolo} {soma.get('Redis', 0):,.0f}{sufixo}")

    c4, c5, c6 = st.columns(3)
    c4.metric(f"🚗 {label_valor} FP", f"{simbolo} {soma.get('Custo FP', 0):,.0f}{sufixo}")
    c5.metric("📉 D&A Dedicada", f"{simbolo} {soma.get('D&A dedicado', 0):,.0f}{sufixo}")
    c6.metric("✅ FP sem Dedicada", f"{simbolo} {soma.get('FP sem Dedicada', 0):,.0f}{sufixo}")

    # ════════════════════════════════════════
    #  KPIs TC Ext (6 cards)
    # ════════════════════════════════════════
    st.subheader("📊 Resumo TC Principal")

    if df_flex is not None and not df_flex.empty:
        bud_total = df_flex['Custo_Total_Bud'].sum()
        flex_bud_total = df_flex['Flex_Bud'].sum()
        vol_budget_total = df_flex['Vol_Budget'].sum() if 'Vol_Budget' in df_flex.columns else 0
        vol_actual_total = df_flex['Vol_Actual'].sum() if 'Vol_Actual' in df_flex.columns else vol_total
        proporcao_media = df_flex['Proporcao'].mean() if 'Proporcao' in df_flex.columns else 1.0

        # Verificar se volumes são iguais
        volumes_iguais = abs(vol_budget_total - vol_actual_total) < 1

        # Usar Custo FP como proxy para "Total" (dado que não temos dados Actual separados)
        total_custo = soma.get('Custo FP', 0)

        # Aplicar CPU se necessário
        if tipo == 'CPU (Custo por Unidade)' and vol_actual_total > 0:
            bud_exibir = bud_total / vol_actual_total
            flex_exibir = flex_bud_total / vol_actual_total
            total_exibir = total_custo
        else:
            bud_exibir = bud_total
            flex_exibir = flex_bud_total
            total_exibir = total_custo

        flex_menos_bud = flex_exibir - bud_exibir
        total_menos_flex = total_exibir - flex_exibir
        total_div_flex = (total_exibir / flex_exibir) if flex_exibir != 0 else 0

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
            render_kpi("Total - Flex Bud", _fmt_val(total_menos_flex))
        with k5:
            render_kpi("Total", _fmt_val(total_exibir))
        with k6:
            render_kpi("Total / Flex Bud", f"{total_div_flex:.0%}")

        render_kpi_spacer()

        # Alerta sobre volumes iguais
        if volumes_iguais:
            st.warning(
                f"⚠️ **Volume Budget ({vol_budget_total:,.0f}) = "
                f"Volume Realizado ({vol_actual_total:,.0f})**  \n"
                f"Proporção = {proporcao_media:.2%} → Flex BUD = BUD.  \n"
                "Verifique os dados de volume na aba **📈 Volume**."
            )
    else:
        st.info("ℹ️ Dados de volume não disponíveis para cálculo de Flex Budget.")

    st.divider()

    # ════════════════════════════════════════
    #  TABS
    # ════════════════════════════════════════
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Custos por Período", "📈 Volume",
        "🏭 Custos por Oficina", "📉 Análise Flex",
        "🚗 Tempo de Produção", "📋 Dados Detalhados",
    ])

    # ── TAB 1: Custos por Período ──
    with tab1:
        st.subheader("Decomposição de Custos por Período")

        df_periodo = df.groupby('Período', as_index=False).agg({
            c: 'sum' for c in cols_val
        })
        df_periodo = ordenar_por_mes(df_periodo)
        df_periodo['Período'] = df_periodo['Período'].astype(str)

        if tipo == 'CPU (Custo por Unidade)' and df_vol_bud is not None:
            vol_per = normalizar_periodo(df_vol_bud.copy()).groupby('Período', as_index=False)['Volume'].sum()
            vol_per['Período'] = vol_per['Período'].astype(str)
            df_periodo = df_periodo.merge(vol_per, on='Período', how='left')
            df_periodo['Volume'] = df_periodo['Volume'].fillna(0)
            for c in cols_val:
                if c in df_periodo.columns:
                    df_periodo[c] = calcular_cpu(df_periodo[c], df_periodo['Volume'])

        metricas = ['FP sem Dedicada', 'D&A dedicado', 'Custo FA']
        df_long = df_periodo.melt(
            id_vars='Período', value_vars=metricas,
            var_name='Componente', value_name='Valor',
        )
        ordem_per = [m for m in ORDEM_MESES if m in df_periodo['Período'].values]
        cores_comp = {'FP sem Dedicada': '#4A90E2', 'D&A dedicado': '#FF9800', 'Custo FA': '#27ae60'}

        bar = alt.Chart(df_long).mark_bar().encode(
            x=alt.X('Período:N', sort=ordem_per, title='Período'),
            y=alt.Y('Valor:Q', title=f'{label_valor} ({simbolo}{sufixo})', stack=True),
            color=alt.Color('Componente:N',
                            scale=alt.Scale(domain=list(cores_comp.keys()),
                                            range=list(cores_comp.values())),
                            legend=alt.Legend(orient='top')),
            tooltip=['Período:N', 'Componente:N', alt.Tooltip('Valor:Q', format=',.0f')],
        ).properties(height=450)

        # Linha Redis (abs)
        df_redis_line = df_periodo[['Período', 'Redis']].copy()
        df_redis_line['Redis'] = df_redis_line['Redis'].abs()
        line_redis = alt.Chart(df_redis_line).mark_line(
            color='#f44336', strokeDash=[5, 3], strokeWidth=2,
        ).encode(
            x=alt.X('Período:N', sort=ordem_per),
            y='Redis:Q',
            tooltip=['Período:N', alt.Tooltip('Redis:Q', format=',.0f')],
        )

        layers = [bar, line_redis]

        # Linha Budget Total (tracejada laranja)
        if df_flex is not None:
            df_bud_p = df_flex[['Período', 'Custo_Total_Bud']].copy()
            df_bud_p['Período'] = df_bud_p['Período'].astype(str)
            if tipo == 'CPU (Custo por Unidade)':
                df_bud_vol = df_flex[['Período', 'Vol_Budget']].copy()
                df_bud_vol['Período'] = df_bud_vol['Período'].astype(str)
                df_bud_p = df_bud_p.merge(df_bud_vol, on='Período', how='left')
                df_bud_p['Custo_Total_Bud'] = calcular_cpu(
                    df_bud_p['Custo_Total_Bud'], df_bud_p['Vol_Budget']
                )

            line_bud = alt.Chart(df_bud_p).mark_line(
                color='#FF6B35', strokeDash=[5, 3], strokeWidth=2,
            ).encode(
                x=alt.X('Período:N', sort=ordem_per),
                y='Custo_Total_Bud:Q',
                tooltip=['Período:N', alt.Tooltip(
                    'Custo_Total_Bud:Q', format=',.0f', title='Budget'
                )],
            )
            layers.append(line_bud)

        # Linha Flex Bud (sólida verde)
        if df_flex is not None:
            df_flex_p = df_flex[['Período', 'Flex_Bud']].copy()
            df_flex_p['Período'] = df_flex_p['Período'].astype(str)
            if tipo == 'CPU (Custo por Unidade)':
                df_flex_vol = df_flex[['Período', 'Vol_Actual']].copy()
                df_flex_vol['Período'] = df_flex_vol['Período'].astype(str)
                df_flex_p = df_flex_p.merge(df_flex_vol, on='Período', how='left')
                df_flex_p['Flex_Bud'] = calcular_cpu(
                    df_flex_p['Flex_Bud'], df_flex_p['Vol_Actual']
                )

            line_flex = alt.Chart(df_flex_p).mark_line(
                color='#27ae60', strokeWidth=2.5,
            ).encode(
                x=alt.X('Período:N', sort=ordem_per),
                y='Flex_Bud:Q',
                tooltip=['Período:N', alt.Tooltip(
                    'Flex_Bud:Q', format=',.0f', title='Flex Bud'
                )],
            )
            layers.append(line_flex)

        chart = (alt.layer(*layers)
                 .configure_view(strokeWidth=0)
                 .configure_axis(labelFontSize=11, titleFontSize=13))
        st.altair_chart(chart, use_container_width=True)

        # Tabela
        show_cols = ['Período'] + metricas + ['Redis', 'Despesa Primaria']
        show_cols = [c for c in show_cols if c in df_periodo.columns]
        st.markdown(criar_tabela_html(df_periodo[show_cols], simbolo=simbolo),
                    unsafe_allow_html=True)

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

            # ═══════════════════════════════════════
            # Gráfico 1: Volume Total por Período
            # ═══════════════════════════════════════
            st.markdown("### 📊 Volume Total por Período")

            # Agregar Budget por período
            df_vb_per = df_vb.groupby('Período', as_index=False)['Volume'].sum()
            df_vb_per['Tipo'] = 'Budget'
            ordem_per = [m for m in ORDEM_MESES if m in df_vb_per['Período'].values]

            # Barras de Budget
            bar_bud = alt.Chart(df_vb_per).mark_bar(
                color='#3498db', opacity=0.8
            ).encode(
                x=alt.X('Período:N', sort=ordem_per, title='Período'),
                y=alt.Y('Volume:Q', title='Volume'),
                tooltip=[
                    alt.Tooltip('Período:N', title='Período'),
                    alt.Tooltip('Volume:Q', title='Volume Budget', format=',')
                ],
            )

            # Rótulos nas barras
            rotulos_bud = bar_bud.mark_text(
                align='center', dy=-10, fontSize=9, color='#3498db'
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
                        "🟦 Barras = Volume Budget | "
                        "🟠 Linha = Volume Realizado"
                    )
                else:
                    st.info(
                        "ℹ️ Volume Budget e Realizado são idênticos. "
                        "Flex Budget = Budget neste cenário."
                    )

            chart_vol_per = (
                alt.layer(*layers_vol)
                .properties(height=400)
                .configure_view(strokeWidth=0)
                .configure_axis(labelFontSize=11, titleFontSize=13)
            )
            st.altair_chart(chart_vol_per, use_container_width=True)

            # ═══════════════════════════════════════
            # Gráfico 2: Volume por Veículo
            # ═══════════════════════════════════════
            st.markdown("### 📊 Volume por Veículo")

            col_g1, col_g2 = st.columns(2)

            with col_g1:
                # Barras empilhadas por Veículo
                df_vb_veic = df_vb.groupby(
                    ['Período', 'Veículo'], as_index=False
                )['Volume'].sum()

                bar_veic = (alt.Chart(df_vb_veic).mark_bar().encode(
                    x=alt.X('Período:N', sort=ordem_per, title='Período'),
                    y=alt.Y('Volume:Q', stack=True, title='Volume'),
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        ),
                        legend=alt.Legend(orient='top', columns=5)
                    ),
                    tooltip=[
                        'Período:N', 'Veículo:N',
                        alt.Tooltip('Volume:Q', format=',')
                    ],
                ).properties(height=400, title='Volume Budget por Veículo')
                 .configure_view(strokeWidth=0))
                st.altair_chart(bar_veic, use_container_width=True)

            with col_g2:
                # Pizza por Veículo
                df_vb_total_veic = df_vb.groupby(
                    'Veículo', as_index=False
                )['Volume'].sum()
                pie_veic = (alt.Chart(df_vb_total_veic).mark_arc(innerRadius=50).encode(
                    theta='Volume:Q',
                    color=alt.Color(
                        'Veículo:N',
                        scale=alt.Scale(
                            domain=list(CORES_VEICULOS.keys()),
                            range=list(CORES_VEICULOS.values())
                        )
                    ),
                    tooltip=['Veículo:N', alt.Tooltip('Volume:Q', format=',')],
                ).properties(height=400, title='Participação por Modelo'))
                st.altair_chart(pie_veic, use_container_width=True)

            # ═══════════════════════════════════════
            # Tabela Comparativa Budget vs Actual
            # ═══════════════════════════════════════
            st.markdown("### 📋 Tabela Budget vs Realizado por Período")

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

        col_a, col_b = st.columns(2)

        with col_a:
            metricas_ofi = ['FP sem Dedicada', 'D&A dedicado', 'Custo FA']
            metricas_disp = [m for m in metricas_ofi if m in df_oficina.columns]
            df_ofi_long = df_oficina.melt(
                id_vars='Oficina', value_vars=metricas_disp,
                var_name='Componente', value_name='Valor',
            )
            bar_ofi = (alt.Chart(df_ofi_long).mark_bar().encode(
                x=alt.X('Oficina:N', sort='-y'),
                y=alt.Y('Valor:Q', title=f'{label_valor} ({simbolo}{sufixo})', stack=True),
                color=alt.Color('Componente:N',
                                scale=alt.Scale(domain=metricas_disp,
                                                range=['#4A90E2', '#FF9800', '#27ae60'][:len(metricas_disp)])),
                tooltip=['Oficina:N', 'Componente:N', alt.Tooltip('Valor:Q', format=',.0f')],
            ).properties(height=450).configure_view(strokeWidth=0))
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
                ).properties(height=450, title='Rateio FA por Oficina')
                 .configure_view(strokeWidth=0))
                st.altair_chart(bar_rat, use_container_width=True)

        # Tabela
        show = df_oficina[['Oficina'] + metricas_disp].copy()
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
                # Agrupar por Oficina e Período para BUD e Flex
                df_pivot_base = df.groupby(
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
                if 'Custo' in df.columns:
                    # Calcular Flex por Oficina × Período
                    df_flex_base = df.copy()
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
                ).properties(height=400, title='Custo FP por Categoria')
                 .configure_view(strokeWidth=0))
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

    # ── TAB 5: Tempo de Produção ──
    with tab5:
        st.subheader("Tempo de Produção — Veículos vs Fluxo Anexo")

        if df_tempo_veic is not None:
            df_tv = normalizar_periodo(df_tempo_veic.copy())
            col_a, col_b = st.columns(2)

            with col_a:
                df_tv_of = df_tv.groupby('Oficina', as_index=False)['Tempo Veic'].sum()
                bar_tv = (alt.Chart(df_tv_of).mark_bar(
                    color='#4A90E2', cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
                ).encode(
                    x=alt.X('Oficina:N', sort='-y'),
                    y=alt.Y('Tempo Veic:Q'),
                    tooltip=['Oficina:N', alt.Tooltip('Tempo Veic:Q', format=',')],
                ).properties(height=400, title='Tempo Veículo por Oficina')
                 .configure_view(strokeWidth=0))
                st.altair_chart(bar_tv, use_container_width=True)

            with col_b:
                df_fa_tempo = load_volume_fa(ano)
                if df_fa_tempo is not None:
                    df_fa_tempo = normalizar_periodo(df_fa_tempo)
                    df_fa_agg = df_fa_tempo.groupby('Oficina', as_index=False)['Tempo FA'].sum()
                    df_tv_agg = df_tv.groupby('Oficina', as_index=False)['Tempo Veic'].sum()
                    df_comp = pd.merge(df_tv_agg, df_fa_agg, on='Oficina', how='outer').fillna(0)
                    df_comp_long = df_comp.melt(
                        id_vars='Oficina', value_vars=['Tempo Veic', 'Tempo FA'],
                        var_name='Tipo', value_name='Tempo',
                    )
                    bar_comp = (alt.Chart(df_comp_long).mark_bar().encode(
                        x=alt.X('Oficina:N', sort='-y'),
                        y='Tempo:Q',
                        color=alt.Color('Tipo:N',
                                        scale=alt.Scale(domain=['Tempo Veic', 'Tempo FA'],
                                                        range=['#4A90E2', '#27ae60'])),
                        xOffset='Tipo:N',
                        tooltip=['Oficina:N', 'Tipo:N', alt.Tooltip('Tempo:Q', format=',')],
                    ).properties(height=400, title='Tempo Veículo vs Tempo FA')
                     .configure_view(strokeWidth=0))
                    st.altair_chart(bar_comp, use_container_width=True)

            st.markdown("**EST e Tempo por Veículo e Oficina**")
            df_tv_tab = df_tv.groupby(['Oficina', 'Veículo'], as_index=False).agg({
                'EST': 'first', 'Volume': 'sum', 'Tempo Veic': 'sum',
            }).sort_values(['Oficina', 'Tempo Veic'], ascending=[True, False])
            st.dataframe(df_tv_tab, use_container_width=True, hide_index=True)
        else:
            st.warning("Dados de tempo de produção não encontrados.")

    # ── TAB 6: Dados Detalhados ──
    with tab6:
        st.subheader("Dados Detalhados — Tabela Principal")

        if 'Account' in df.columns:
            accounts = sorted(df['Account'].dropna().unique())
            account_sel = st.multiselect("Filtrar por Account", accounts, default=[],
                                         key='home_account_detail')
            df_det = df[df['Account'].isin(account_sel)].copy() if account_sel else df.copy()
        else:
            df_det = df.copy()

        st.dataframe(df_det, use_container_width=True, hide_index=True)
        st.caption(f"Total de linhas: {len(df_det):,} | Moeda: {moeda} | {tipo}")

        csv = df_det.to_csv(index=False, sep=';', decimal=',')
        st.download_button("📥 Baixar CSV", data=csv,
                           file_name=f"tc_principal_bud_{ano}.csv", mime="text/csv")

    st.divider()
    st.caption(f"TC — Planta Principal | Budget {ano} | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
