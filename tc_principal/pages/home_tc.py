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
    formatar_ratio_com_barra, criar_tabela_html_flex,
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
    #  TABS
    # ════════════════════════════════════════
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "� TC Veículos", "📈 Volume",
        "🏭 Custos por Oficina", "📉 Análise Flex",
        "🚗 Tempo de Produção", "📋 Dados Detalhados",
    ])

    # ── TAB 1: TC Veículos ──
    with tab1:
        # ════════════════════════════════════════
        # 📊 Resumo TC Principal (KPIs dentro da tab)
        # ════════════════════════════════════════
        st.subheader("📊 Resumo TC Principal")

        if df_flex is not None and not df_flex.empty:
            bud_total = df_flex['Custo_Total_Bud'].sum()
            flex_bud_total = df_flex['Flex_Bud'].sum()
            vol_budget_total = (
                df_flex['Vol_Budget'].sum()
                if 'Vol_Budget' in df_flex.columns else 0
            )
            vol_actual_total = (
                df_flex['Vol_Actual'].sum()
                if 'Vol_Actual' in df_flex.columns else vol_total
            )
            proporcao_media = (
                df_flex['Proporcao'].mean()
                if 'Proporcao' in df_flex.columns else 1.0
            )

            # Verificar se volumes são iguais
            volumes_iguais = abs(vol_budget_total - vol_actual_total) < 1

            # Usar Custo FP como proxy para "Total"
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
            st.info(
                "ℹ️ Dados de volume não disponíveis para cálculo de Flex Budget."
            )

        st.divider()

        # ════════════════════════════════════════
        # Gráfico: Custo FP por Período (degradê azul) + Linha Flex BUD pontilhada
        # ════════════════════════════════════════
        st.markdown("### Custo FP por Período")

        df_periodo = df.groupby('Período', as_index=False).agg({
            c: 'sum' for c in cols_val
        })
        df_periodo = ordenar_por_mes(df_periodo)
        df_periodo['Período'] = df_periodo['Período'].astype(str)

        if tipo == 'CPU (Custo por Unidade)' and df_vol_bud is not None:
            vol_per = normalizar_periodo(df_vol_bud.copy()).groupby(
                'Período', as_index=False
            )['Volume'].sum()
            vol_per['Período'] = vol_per['Período'].astype(str)
            df_periodo = df_periodo.merge(vol_per, on='Período', how='left')
            df_periodo['Volume'] = df_periodo['Volume'].fillna(0)
            for c in cols_val:
                if c in df_periodo.columns:
                    df_periodo[c] = calcular_cpu(
                        df_periodo[c], df_periodo['Volume']
                    )

        # Ordenação cronológica usando lista filtrada de ORDEM_MESES
        periodos_presentes = df_periodo['Período'].unique().tolist()
        ordem_per = [m for m in ORDEM_MESES if m in periodos_presentes]

        # Gráfico de barras com degradê azul (como TC Ext)
        bar = alt.Chart(df_periodo).mark_bar().encode(
            x=alt.X(
                'Período:N',
                sort=ordem_per,
                title='Período',
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(
                'Custo FP:Q',
                title=f'{label_valor} ({simbolo}{sufixo})',
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            color=alt.Color(
                'Custo FP:Q',
                title='Custo FP',
                scale=alt.Scale(scheme='blues'),
                legend=alt.Legend(
                    title='Custo FP', orient='right',
                    titleFontSize=10, labelFontSize=9
                )
            ),
            tooltip=[
                alt.Tooltip('Período:N', title='Período'),
                alt.Tooltip('Custo FP:Q', format=',.2f', title='Custo FP')
            ],
        ).properties(height=450)

        # Rótulos de valores nas barras
        rotulos = bar.mark_text(
            align='center', baseline='middle', dy=-10,
            color='black', fontSize=9
        ).encode(
            text=alt.Text('Custo FP:Q', format=',.2f')
        ).transform_filter(
            (alt.datum['Custo FP'] != None) & (alt.datum['Custo FP'] != 0)
        )

        layers = [bar, rotulos]

        # Linha Flex Bud (pontilhada laranja como TC Ext)
        df_flex_p = None
        if df_flex is not None:
            df_flex_p = df_flex[['Período', 'Flex_Bud']].copy()
            df_flex_p['Período'] = df_flex_p['Período'].astype(str)
            # IMPORTANTE: ordenar dados cronologicamente ANTES de desenhar linha
            df_flex_p = ordenar_por_mes(df_flex_p)
            if tipo == 'CPU (Custo por Unidade)':
                df_flex_vol = df_flex[['Período', 'Vol_Actual']].copy()
                df_flex_vol['Período'] = df_flex_vol['Período'].astype(str)
                df_flex_p = df_flex_p.merge(
                    df_flex_vol, on='Período', how='left'
                )
                df_flex_p['Flex_Bud'] = calcular_cpu(
                    df_flex_p['Flex_Bud'], df_flex_p['Vol_Actual']
                )

            df_flex_p['Tipo'] = 'Flex Bud'

            # Importante: com eixo X nominal, forçar ordem de ligação da linha
            line_flex = alt.Chart(df_flex_p).mark_line(
                strokeDash=[10, 5],
                strokeWidth=1.5,
                opacity=0.8
            ).encode(
                x=alt.X(
                    'Período:N',
                    sort=ordem_per,
                ),
                y='Flex_Bud:Q',
                color=alt.Color(
                    'Tipo:N',
                    scale=alt.Scale(
                        domain=['Real', 'Flex Bud'],
                        range=['#4A90E2', '#FF6B35']
                    ),
                    legend=alt.Legend(
                        title='Legenda', orient='bottom',
                        titleFontSize=10, labelFontSize=9,
                        direction='horizontal', symbolType='square'
                    )
                ),
                tooltip=[
                    'Período:N',
                    alt.Tooltip('Flex_Bud:Q', format=',.2f', title='Flex Bud')
                ],
            )

            # Pontos na linha Flex Bud
            pontos_flex = alt.Chart(df_flex_p).mark_circle(
                size=80, opacity=0.9
            ).encode(
                x=alt.X(
                    'Período:N',
                    sort=ordem_per,
                ),
                y='Flex_Bud:Q',
                color=alt.value('#FF6B35'),
                tooltip=[
                    'Período:N',
                    alt.Tooltip('Flex_Bud:Q', format=',.2f', title='Flex Bud')
                ],
            )

            layers.extend([line_flex, pontos_flex])

        # Combinar gráfico principal
        grafico_principal = alt.layer(*layers).resolve_scale(
            x='shared', y='shared'
        )

        # ════════════════════════════════════════
        # Gráfico Delta (Real - Flex Bud) como TC Ext
        # ════════════════════════════════════════
        grafico_delta = None
        if df_flex_p is not None and len(df_flex_p) > 0:
            try:
                delta_data = df_periodo[['Período', 'Custo FP']].copy()
                delta_data = delta_data.merge(
                    df_flex_p[['Período', 'Flex_Bud']],
                    on='Período', how='left'
                )
                delta_data['Flex_Bud'] = delta_data['Flex_Bud'].fillna(0)
                delta_data['Delta'] = (
                    delta_data['Custo FP'].fillna(0)
                    - delta_data['Flex_Bud'].fillna(0)
                )

                # Calcular escala simétrica para cores
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1

                # Gráfico de barras delta
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20
                ).encode(
                    x=alt.X(
                        'Período:N',
                        title='',
                        sort=ordem_per,
                        axis=alt.Axis(
                            grid=False, domain=False,
                            ticks=False, labels=False
                        )
                    ),
                    y=alt.Y(
                        'Delta:Q', title='Delta (Real - Flex Bud)',
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
                        alt.Tooltip('Período:N', title='Período'),
                        alt.Tooltip(
                            'Delta:Q', title='Delta (Real - Flex Bud)',
                            format=',.2f'
                        ),
                        alt.Tooltip(
                            'Custo FP:Q', title='Real', format=',.2f'
                        ),
                        alt.Tooltip(
                            'Flex_Bud:Q', title='Flex Bud', format=',.2f'
                        )
                    ]
                ).properties(height=38)

                # Rótulos delta positivos (acima)
                rotulos_delta_pos = alt.Chart(
                    delta_data[delta_data['Delta'] >= 0]
                ).mark_text(
                    align='center', baseline='bottom', dy=-12,
                    fontSize=9, fontWeight='bold'
                ).encode(
                    x=alt.X(
                        'Período:N',
                        sort=ordem_per,
                    ),
                    y='Delta:Q',
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],
                            type='linear', nice=False
                        ),
                        legend=None
                    )
                )

                # Rótulos delta negativos (abaixo)
                rotulos_delta_neg = alt.Chart(
                    delta_data[delta_data['Delta'] < 0]
                ).mark_text(
                    align='center', baseline='top', dy=12,
                    fontSize=9, fontWeight='bold'
                ).encode(
                    x=alt.X(
                        'Período:N',
                        sort=ordem_per,
                    ),
                    y='Delta:Q',
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],
                            type='linear', nice=False
                        ),
                        legend=None
                    )
                )

                grafico_delta = grafico_delta + rotulos_delta_pos + rotulos_delta_neg
            except Exception:
                grafico_delta = None

        # Combinar gráficos verticalmente (delta em cima)
        if grafico_delta is not None:
            grafico_final = alt.vconcat(
                grafico_delta, grafico_principal
            ).resolve_scale(x='shared')
        else:
            grafico_final = grafico_principal

        grafico_final = grafico_final.configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=11, titleFontSize=13
        )

        st.altair_chart(grafico_final, use_container_width=True)

        # Legenda do gráfico
        st.caption(
            "🟦 Barras = Custo FP Real (degradê azul) | "
            "🟠 Linha pontilhada = Flex Budget"
        )
        if grafico_delta is not None:
            st.caption(
                "🟢 Delta negativo = Abaixo do Flex (favorável) | "
                "🔴 Delta positivo = Acima do Flex (desfavorável)"
            )

        st.divider()

        # ════════════════════════════════════════
        # 📊 Análise Flex por Categoria (padrão TC Ext)
        # ════════════════════════════════════════
        st.subheader("📊 Análise Flex por Categoria")

        if df_flex is not None and 'Custo' in df.columns:
            # Preparar dados para tabela Flex por Categoria
            df_cat = df.copy()
            df_cat['Custo_str'] = df_cat['Custo'].astype(str).str.lower()
            df_cat['Categoria'] = df_cat['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )

            # Agrupar por Categoria e Período
            df_cat_agg = df_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum()
            df_cat_agg = ordenar_por_mes(df_cat_agg)

            # Preparar dados de volume para cálculo de Flex
            if df_vol_bud is not None and df_vol_actual is not None:
                df_vol_bud_agg = normalizar_periodo(df_vol_bud.copy()).groupby(
                    'Período', as_index=False
                )['Volume'].sum().rename(columns={'Volume': 'Vol_Budget'})
                df_vol_act_agg = normalizar_periodo(df_vol_actual.copy()).groupby(
                    'Período', as_index=False
                )['Volume'].sum().rename(columns={'Volume': 'Vol_Actual'})
            else:
                df_vol_bud_agg = pd.DataFrame({'Período': [], 'Vol_Budget': []})
                df_vol_act_agg = pd.DataFrame({'Período': [], 'Vol_Actual': []})

            # Merge com volumes
            df_cat_agg['Período'] = df_cat_agg['Período'].astype(str)
            df_vol_bud_agg['Período'] = df_vol_bud_agg['Período'].astype(str)
            df_vol_act_agg['Período'] = df_vol_act_agg['Período'].astype(str)

            df_cat_agg = df_cat_agg.merge(
                df_vol_bud_agg, on='Período', how='left'
            ).merge(
                df_vol_act_agg, on='Período', how='left'
            )
            df_cat_agg['Vol_Budget'] = df_cat_agg['Vol_Budget'].fillna(1)
            df_cat_agg['Vol_Actual'] = df_cat_agg['Vol_Actual'].fillna(1)

            # Calcular proporção de volume
            df_cat_agg['Proporcao'] = (
                df_cat_agg['Vol_Actual'] / df_cat_agg['Vol_Budget']
            ).fillna(1)

            # Merge com BUD do df_flex
            df_bud_cat = df.copy()
            df_bud_cat['Custo_str'] = df_bud_cat['Custo'].astype(str).str.lower()
            df_bud_cat['Categoria'] = df_bud_cat['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )
            df_bud_cat_agg = df_bud_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})
            df_bud_cat_agg['Período'] = df_bud_cat_agg['Período'].astype(str)

            df_cat_agg = df_cat_agg.merge(
                df_bud_cat_agg, on=['Categoria', 'Período'], how='left'
            )
            df_cat_agg['BUD'] = df_cat_agg['BUD'].fillna(0)

            # Calcular Flex BUD:
            # Fixo: Flex = BUD (não flexibiliza)
            # Variável: Flex = BUD * Proporção
            df_cat_agg['Flex BUD'] = df_cat_agg.apply(
                lambda r: r['BUD'] if r['Categoria'] == 'Fixo'
                else r['BUD'] * r['Proporcao'],
                axis=1
            )

            # Aplicar CPU se necessário
            if tipo == 'CPU (Custo por Unidade)':
                df_cat_agg['Custo FP'] = calcular_cpu(
                    df_cat_agg['Custo FP'], df_cat_agg['Vol_Actual']
                )
                df_cat_agg['BUD'] = calcular_cpu(
                    df_cat_agg['BUD'], df_cat_agg['Vol_Budget']
                )
                df_cat_agg['Flex BUD'] = calcular_cpu(
                    df_cat_agg['Flex BUD'], df_cat_agg['Vol_Actual']
                )

            # Renomear para padrão TC Ext
            df_cat_agg = df_cat_agg.rename(columns={'Custo FP': 'Total'})

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
                render_kpi("Total Real", f"{simbolo} {total_real:,.2f}{sufixo}")
            with kr6:
                render_kpi("Total / Flex", f"{total_ratio:.0%}")

            render_kpi_spacer()
            st.markdown("---")

            # ═══════════════════════════════════════
            # Expanders 💰 Fixo e 💰 Variável com hierarquia Type 05 → Account
            # ═══════════════════════════════════════
            # Preparar dados com Type 05 e Account para hierarquia
            df_hier = df.copy()
            df_hier['Custo_str'] = df_hier['Custo'].astype(str).str.lower()
            df_hier['Categoria'] = df_hier['Custo_str'].apply(
                lambda x: 'Fixo' if x.startswith('fix') else 'Variável'
            )

            # Garantir que Type 05 e Account existem
            if 'Type 05' not in df_hier.columns:
                df_hier['Type 05'] = 'N/A'
            if 'Account' not in df_hier.columns:
                df_hier['Account'] = 'N/A'

            # Agrupar por Categoria, Type 05, Account
            df_hier_agg = df_hier.groupby(
                ['Categoria', 'Type 05', 'Account'], as_index=False
            )['Custo FP'].sum()

            # Merge com volumes para cálculo de Flex
            vol_total_bud = (df_vol_bud['Volume'].sum()
                            if df_vol_bud is not None else 1)
            vol_total_act = (df_vol_actual['Volume'].sum()
                            if df_vol_actual is not None else vol_total_bud)
            proporcao_global = vol_total_act / vol_total_bud if vol_total_bud > 0 else 1

            # Duplicar para ter BUD (original já tem Total)
            df_hier_agg = df_hier_agg.rename(columns={'Custo FP': 'Total'})

            # Calcular BUD com base nos dados originais
            df_bud_hier = df_hier.groupby(
                ['Categoria', 'Type 05', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

            df_hier_agg = df_hier_agg.merge(
                df_bud_hier, on=['Categoria', 'Type 05', 'Account'], how='left'
            )
            df_hier_agg['BUD'] = df_hier_agg['BUD'].fillna(0)

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
                    expanded=False
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
                            expanded=False
                        ):
                            # Preparar tabela por Account
                            df_tabela = df_type05[[
                                'Account', 'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                            ]].copy()

                            # Usar tabela HTML com barrinha
                            html_tabela = criar_tabela_html_flex(
                                df_tabela, simbolo, sufixo
                            )
                            st.markdown(html_tabela, unsafe_allow_html=True)

        else:
            st.info(
                "ℹ️ Dados de categoria (Custo) não disponíveis para "
                "análise Flex."
            )

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
            vol_bud_total_tab2 = df_vb['Volume'].sum()
            vol_act_total_tab2 = vol_bud_total_tab2  # default
            if df_vol_actual is not None:
                df_va = normalizar_periodo(df_vol_actual.copy())
                df_va = ordenar_por_mes(df_va)
                df_va['Período'] = df_va['Período'].astype(str)
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
                    title='Volume',
                    scale=alt.Scale(scheme='greens'),
                    legend=alt.Legend(orient='right', titleFontSize=10, labelFontSize=9)
                ),
                tooltip=[
                    alt.Tooltip('Veículo:N', title='Veículo'),
                    alt.Tooltip('Volume:Q', title='Volume', format=',')
                ],
            ).properties(height=360)

            # Rótulos nas barras
            rotulos_veic = bar_veic.mark_text(
                align='center', dy=-10, fontSize=9, color='black'
            ).encode(text=alt.Text('Volume:Q', format=','))

            chart_veic = (bar_veic + rotulos_veic).configure_view(strokeWidth=0)
            st.altair_chart(chart_veic, use_container_width=True)

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
            ).properties(height=450, title='Custo FP por Oficina')
             .configure_view(strokeWidth=0))
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

    # ── TAB 5: Tempo de Produção / Custo FP por Veículo ──
    with tab5:
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
                ).properties(height=400, title='Custo FP por Veículo')
                 .configure_view(strokeWidth=0))
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

        if df_tempo_veic is not None:
            df_tv = normalizar_periodo(df_tempo_veic.copy())
            col_c, col_d = st.columns(2)

            with col_c:
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

            with col_d:
                df_fa_tempo = load_volume_fa(ano)
                if df_fa_tempo is not None:
                    df_fa_tempo = normalizar_periodo(df_fa_tempo)
                    df_fa_agg = df_fa_tempo.groupby('Oficina', as_index=False)['Tempo FA'].sum()
                    df_tv_agg = df_tv.groupby('Oficina', as_index=False)['Tempo Veic'].sum()
                    df_comp_tempo = pd.merge(df_tv_agg, df_fa_agg, on='Oficina', how='outer').fillna(0)
                    df_comp_long = df_comp_tempo.melt(
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
            st.info("Dados de tempo de produção não disponíveis.")

    # ── TAB 6: Dados Detalhados ──
    with tab6:
        st.subheader("Dados Detalhados")

        # Seção 1: Dados Reais (Custo FP)
        with st.expander("📋 Dados Reais (Custo FP)", expanded=True):
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
            st.download_button("📥 Baixar Dados Reais (CSV)", data=csv,
                               file_name=f"tc_principal_real_{ano}.csv", mime="text/csv")

        # Seção 2: Dados Budget (BUD e Flex)
        with st.expander("🧾 Dados Budget (BUD e Flex BUD)", expanded=False):
            if df_flex is not None and not df_flex.empty:
                st.dataframe(df_flex, use_container_width=True, hide_index=True)
                st.caption(f"Total de linhas: {len(df_flex):,} | Moeda: {moeda}")

                csv_bud = df_flex.to_csv(index=False, sep=';', decimal=',')
                st.download_button("📥 Baixar Dados Budget (CSV)", data=csv_bud,
                                   file_name=f"tc_principal_bud_flex_{ano}.csv", mime="text/csv",
                                   key='download_bud_flex')
            else:
                st.info("ℹ️ Dados de Budget/Flex não disponíveis.")

    st.divider()
    st.caption(f"TC — Planta Principal | Budget {ano} | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
