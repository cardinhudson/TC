"""Tab 3: Análise Flex — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    ordenar_por_mes,
    aplicar_fator_df, converter_moeda_df,
    calcular_flex_budget, calcular_cpu,
)
from tc_principal.ui_components import render_kpi, render_kpi_spacer

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None


def render(ctx):
    """Renderiza a aba Análise Flex."""
    # ── Desempacotar contexto ──
    ano = ctx.ano
    moeda = ctx.moeda
    simbolo = ctx.simbolo
    taxas = ctx.taxas
    tipo = ctx.tipo
    fator = ctx.fator
    sufixo = ctx.sufixo
    label_valor = ctx.label_valor
    df = ctx.df
    df_bud = ctx.df_bud
    df_flex = ctx.df_flex
    df_vol_bud = ctx.df_vol_bud
    df_vol_actual = ctx.df_vol_actual
    _raw_df_be = ctx._raw_df_be
    _get_be_full = ctx._get_be_full
    tem_ano_df = ctx.tem_ano_df

    st.subheader("Análise Flex — Fixo vs Variável")

    if df_flex is not None and 'Custo' in df.columns:
        # ── Fonte dos dados (Budget / Real / Best Estimate) ──
        _fx_ctrl1, _fx_ctrl2 = st.columns([2.5, 3.5])
        with _fx_ctrl1:
            _opcoes_fonte_flex = ["Budget", "Real"]
            if _raw_df_be is not None and not _raw_df_be.empty:
                _opcoes_fonte_flex.append("Best Estimate")
            _fonte_flex = st.radio(
                "📊 Fonte dos dados",
                _opcoes_fonte_flex,
                horizontal=True,
                key="home_fonte_dados_flex"
            )

        # Selecionar base de dados conforme fonte
        if _fonte_flex == "Budget":
            _df_base_flex = df_bud.copy()
        elif _fonte_flex == "Best Estimate":
            _be_flex_tmp = _get_be_full()
            _be_flex_src = _be_flex_tmp if _be_flex_tmp is not None else _raw_df_be
            _df_be_flex = _be_flex_src.copy()
            _cols_be = [c for c in COLUNAS_MONETARIAS if c in _df_be_flex.columns]
            _df_be_flex = aplicar_fator_df(_df_be_flex, _cols_be, fator)
            _df_be_flex = converter_moeda_df(_df_be_flex, _cols_be, moeda, taxas)
            _df_base_flex = _df_be_flex
        else:
            _df_base_flex = df.copy()

        # ── Filtros locais ──
        _fx_c1, _fx_c2 = st.columns(2)
        with _fx_c1:
            _fx_ofc_opts = sorted(_df_base_flex['Oficina'].dropna().unique()) if 'Oficina' in _df_base_flex.columns else []
            _fx_ofc_sel = st.multiselect("🏭 Oficina", _fx_ofc_opts, default=[], key="flex_oficina")
        with _fx_c2:
            _fx_veic_opts = sorted(_df_base_flex['Type 06'].dropna().unique()) if 'Type 06' in _df_base_flex.columns else []
            _fx_veic_sel = st.multiselect("🚗 Veículo", _fx_veic_opts, default=[], key="flex_veiculo")
        df_flex_local = _df_base_flex.copy()
        if _fx_ofc_sel:
            df_flex_local = df_flex_local[df_flex_local['Oficina'].isin(_fx_ofc_sel)]
        if _fx_veic_sel:
            df_flex_local = df_flex_local[df_flex_local['Type 06'].isin(_fx_veic_sel)]

        # ── Recalcular Flex a partir da base selecionada ──
        _df_flex_t3 = calcular_flex_budget(
            _df_base_flex, df_vol_bud, df_vol_actual, tem_ano=tem_ano_df
        )

        # ── Volume para CPU (sempre usar df_vol_actual, como no gráfico principal) ──
        _df_vol_cpu = df_vol_actual if df_vol_actual is not None else df_vol_bud
        _vol_flex_total = _df_vol_cpu['Volume'].sum() if _df_vol_cpu is not None else 0
        _cpu_flex = tipo == 'CPU (Custo por Unidade)' and _vol_flex_total > 0
        _dec_flex = 2 if _cpu_flex else 0

        # KPIs de Flex no topo
        if _df_flex_t3 is not None and not _df_flex_t3.empty:
            bud_total_t4 = _df_flex_t3['Custo_Total_Bud'].sum()
            flex_total_t4 = _df_flex_t3['Flex_Bud'].sum()
            fixo_total_t4 = _df_flex_t3['Custo_Fixo'].sum()
            nfixo_total_t4 = _df_flex_t3['Custo_NaoFixo'].sum()

            if _cpu_flex:
                bud_total_t4 /= _vol_flex_total
                flex_total_t4 /= _vol_flex_total
                fixo_total_t4 /= _vol_flex_total
                nfixo_total_t4 /= _vol_flex_total

            kf1, kf2, kf3, kf4 = st.columns(4)
            with kf1:
                render_kpi("Custo Fixo", f"{simbolo} {fixo_total_t4:,.2f}{sufixo}")
            with kf2:
                render_kpi("Custo Variável", f"{simbolo} {nfixo_total_t4:,.2f}{sufixo}")
            with kf3:
                render_kpi("BUD Total", f"{simbolo} {bud_total_t4:,.2f}{sufixo}")
            with kf4:
                render_kpi("Flex BUD Total", f"{simbolo} {flex_total_t4:,.2f}{sufixo}")
        else:
            st.info("ℹ️ Dados de Flex BUD não disponíveis.")

        render_kpi_spacer()
        # Decompor custos por categoria (Fixo / Não-Fixo)
        df_cat = df_flex_local.copy()
        if 'Custo' in df_cat.columns:
            df_cat['Custo_str'] = df_cat['Custo'].astype(str).str.lower()
        else:
            df_cat['Custo_str'] = 'variável'
        df_cat['Categoria'] = np.where(
            df_cat['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
        )

        # Agrupar por Período e Categoria
        _cat_col = 'Custo FP' if 'Custo FP' in df_cat.columns else None
        if _cat_col:
            df_cat_agg = df_cat.groupby(
                ['Período', 'Categoria'], as_index=False
            )[_cat_col].sum()
        else:
            df_cat_agg = pd.DataFrame(columns=['Período', 'Categoria', 'Custo FP'])
        df_cat_agg = ordenar_por_mes(df_cat_agg)

        # Guardar cópia RAW (antes de CPU) para tabela pivot
        _df_cat_raw = df_cat_agg.copy()

        # Aplicar CPU se necessário (mesmo cálculo do gráfico principal)
        _vol_per = None
        if _cpu_flex and _df_vol_cpu is not None:
            _vol_per = _df_vol_cpu.groupby('Período', as_index=False)['Volume'].sum()
            _vol_per['Período'] = _vol_per['Período'].astype(str)
            df_cat_agg['Período'] = df_cat_agg['Período'].astype(str)
            df_cat_agg = df_cat_agg.merge(_vol_per, on='Período', how='left')
            df_cat_agg['Volume'] = df_cat_agg['Volume'].fillna(0)
            df_cat_agg['Custo FP'] = calcular_cpu(
                df_cat_agg['Custo FP'], df_cat_agg['Volume']
            )
            df_cat_agg = df_cat_agg[['Período', 'Categoria', 'Custo FP']]

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
                         alt.Tooltip('Custo FP:Q', format=f',.{_dec_flex}f')],
            ).properties(height=400, title='Custo FP por Categoria'))
            text_cat = (alt.Chart(df_cat_agg).mark_text(
                align='center', dy=-8, fontSize=12, color='black'
            ).encode(
                x=alt.X('Período:N', sort=ordem_per),
                y=alt.Y('Custo FP:Q', stack=True),
                text=alt.Text('Custo FP:Q', format=f',.{_dec_flex}f'),
                order=alt.Order('Categoria:N'),
            ))
            st.altair_chart(bar_cat + text_cat, use_container_width=True)

        with col_b:
            # Gráfico pizza
            df_cat_total = df_cat.groupby('Categoria', as_index=False)['Custo FP'].sum()
            # Aplicar CPU ao pizza também
            if _cpu_flex:
                df_cat_total['Custo FP'] = df_cat_total['Custo FP'] / _vol_flex_total
            df_cat_total['pct'] = (df_cat_total['Custo FP'] / df_cat_total['Custo FP'].sum() * 100).round(1)
            df_cat_total['label'] = df_cat_total['pct'].apply(lambda x: f"{x:.1f}%")
            pie_cat = (alt.Chart(df_cat_total).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Custo FP:Q'),
                color=alt.Color('Categoria:N',
                                scale=alt.Scale(
                                    domain=['Fixo', 'Variável'],
                                    range=['#3498db', '#e74c3c']
                                )),
                tooltip=['Categoria:N', alt.Tooltip('Custo FP:Q', format=',')],
            ).properties(height=400, title='Participação por Categoria'))
            pie_text = (alt.Chart(df_cat_total).mark_text(
                radius=90, fontSize=12, fontWeight='bold'
            ).encode(
                theta=alt.Theta('Custo FP:Q', stack=True),
                text='label:N',
            ))
            st.altair_chart(pie_cat + pie_text, use_container_width=True)

        # Tabela resumo
        st.markdown("**📊 Resumo por Categoria**")
        # Construir pivot a partir dos dados RAW (pré-CPU)
        _df_cat_raw['Período'] = _df_cat_raw['Período'].astype(str)
        df_cat_pivot = _df_cat_raw.pivot_table(
            index='Categoria',
            columns='Período',
            values='Custo FP',
            aggfunc='sum',
        )
        df_cat_pivot = df_cat_pivot[[m for m in ORDEM_MESES if m in df_cat_pivot.columns]]

        # Aplicar CPU por coluna (mesmo cálculo do gráfico)
        if _cpu_flex and _vol_per is not None:
            _vol_map = dict(zip(_vol_per['Período'].astype(str), _vol_per['Volume']))
            for _m in df_cat_pivot.columns:
                _v = _vol_map.get(str(_m), 0)
                if _v > 0:
                    df_cat_pivot[_m] = df_cat_pivot[_m] / _v
                else:
                    df_cat_pivot[_m] = 0
            # Total ponderado: soma custos raw / soma volumes
            _raw_totals = _df_cat_raw.groupby('Categoria')['Custo FP'].sum()
            df_cat_pivot['Total'] = _raw_totals / _vol_flex_total
            df_cat_pivot.loc['Total'] = df_cat_pivot.sum()
        else:
            df_cat_pivot['Total'] = df_cat_pivot.sum(axis=1)
            df_cat_pivot.loc['Total'] = df_cat_pivot.sum()

        # Formatar valores
        fmt_cat = df_cat_pivot.copy()
        for col in fmt_cat.columns:
            fmt_cat[col] = fmt_cat[col].apply(
                lambda x: f"{simbolo} {x:,.{_dec_flex}f}" if pd.notna(x) else "—"
            )
        st.dataframe(fmt_cat, width="stretch")

        # Comparação BUD vs Flex por categoria
        if _df_flex_t3 is not None and not _df_flex_t3.empty:
            st.markdown("**📈 BUD vs Flex Budget por Categoria**")
            _comp_bud = _df_flex_t3['Custo_Total_Bud'].sum()
            _comp_flex = _df_flex_t3['Flex_Bud'].sum()
            _comp_fixo = _df_flex_t3['Custo_Fixo'].sum()
            _comp_nfixo = _df_flex_t3['Custo_NaoFixo'].sum()
            _comp_prop = _df_flex_t3['Proporcao'].mean()

            if _cpu_flex:
                _comp_bud /= _vol_flex_total
                _comp_flex /= _vol_flex_total
                _comp_fixo /= _vol_flex_total
                _comp_nfixo /= _vol_flex_total

            comp_data = pd.DataFrame({
                'Métrica': ['Custo Fixo', 'Custo Variável', 'Total'],
                'BUD': [_comp_fixo, _comp_nfixo, _comp_bud],
                'Flex BUD': [_comp_fixo, _comp_nfixo * _comp_prop,
                             _comp_fixo + _comp_nfixo * _comp_prop],
            })
            comp_data['Diferença'] = comp_data['Flex BUD'] - comp_data['BUD']

            # Formatar
            for col in ['BUD', 'Flex BUD', 'Diferença']:
                comp_data[col] = comp_data[col].apply(lambda x: f"{simbolo} {x:,.{_dec_flex}f}")

            st.dataframe(comp_data, width="stretch", hide_index=True)
    else:
        st.info("Dados de categoria (Custo) não disponíveis para análise Flex.")

