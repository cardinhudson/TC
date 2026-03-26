"""Tab 4: Tempo de Produção — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    normalizar_periodo, ordenar_por_mes,
    calcular_cpu,
    load_tempo_veiculos, load_tempo_veiculos_real,
    load_volume_fa, load_volume_fa_real,
    load_percentual_rateio_veiculos_real,
)
from tc_principal.ui_components import (
    criar_tabela_html, render_kpi, render_kpi_spacer,
)
from tc_principal.pages._home_tabs.data_helpers import _carregar_rateios_manuais

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None


def render(ctx):
    """Renderiza a aba Tempo de Produção."""
    # ── Desempacotar contexto ──
    ano = ctx.ano
    tipo = ctx.tipo
    label_valor = ctx.label_valor
    simbolo = ctx.simbolo
    sufixo = ctx.sufixo
    df = ctx.df
    cols_val = ctx.cols_val
    df_vol_bud = ctx.df_vol_bud

    # ── Filtros no topo da aba ──
    _col_filtro1, _col_filtro2, _col_filtro3, _col_filtro4 = st.columns([1.5, 1.5, 1.5, 1.5])
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
    with _col_filtro3:
        _tp_ofc_opts = sorted(df['Oficina'].dropna().unique()) if 'Oficina' in df.columns else []
        _tp_ofc_sel = st.multiselect("🏭 Oficina", _tp_ofc_opts, default=[], key="tempo_prod_oficina")
    with _col_filtro4:
        _tp_veic_opts = sorted(df['Type 06'].dropna().unique()) if 'Type 06' in df.columns else []
        _tp_veic_sel = st.multiselect("🚗 Veículo", _tp_veic_opts, default=[], key="tempo_prod_veiculo")
    fator_tempo = 1.0 if unidade_tempo == "Minutos" else 1.0 / 60.0
    label_tempo = "min" if unidade_tempo == "Minutos" else "h"

    # Aplicar filtros locais
    _df_tp = df.copy()
    if _tp_ofc_sel:
        _df_tp = _df_tp[_df_tp['Oficina'].isin(_tp_ofc_sel)]
    if _tp_veic_sel and 'Type 06' in _df_tp.columns:
        _df_tp = _df_tp[_df_tp['Type 06'].isin(_tp_veic_sel)]

    st.subheader("💰 Custo FP por Veículo")

    # Custo FP por Veículo (análogo ao TC Ext por Veíc)
    if 'Veículo' in _df_tp.columns:
        df_veic = _df_tp.groupby('Veículo', as_index=False).agg({
            c: 'sum' for c in cols_val if c in _df_tp.columns
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
                    align='center', dy=-10, fontSize=12, color='#7C3AED', fontWeight='bold'
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
                align='center', dy=-10, fontSize=12, color='#7C3AED'
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
                    align='center', dy=-10, fontSize=12, color='black'
                ).encode(text=alt.Text('Tempo:Q', format=',.1f'))
                st.altair_chart(bar_comp + labels_comp, use_container_width=True)

        st.markdown(f"**EST e Tempo por Veículo e Oficina ({label_tempo})**")
        df_tv_tab = df_tv.groupby(['Oficina', 'Veículo'], as_index=False).agg({
            'EST': 'first', 'Volume': 'sum', 'Tempo Veic': 'sum',
        }).sort_values(['Oficina', 'Tempo Veic'], ascending=[True, False])
        df_tv_tab['Tempo Veic'] = df_tv_tab['Tempo Veic'] * fator_tempo
        df_tv_tab = df_tv_tab.rename(columns={'Tempo Veic': f'Tempo Veic ({label_tempo})'})
        st.dataframe(df_tv_tab, width="stretch", hide_index=True)

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
                width="stretch", hide_index=True, height=500,
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

