"""
TC Principal — Best Estimate (Simulador)
Permite simular cenários alterando volumes e rateios FA.
"""

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import json
from datetime import datetime

from tc_core.finance.currency import converter_moeda, obter_simbolo_moeda
from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    load_principal, load_volume_bud, load_tempo_veiculos,
    normalizar_periodo, aplicar_fator, aplicar_fator_df,
    converter_moeda_df, obter_sufixo_fator,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, criar_tabela_html,
)


def render():
    injetar_css_global()
    render_header()

    st.title("🔮 Best Estimate (Simulador)")
    st.subheader("Simulação de cenários de Volume e Custo • TC Planta Principal")

    # ── Sidebar ──
    cfg = render_sidebar_global('be_sim')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, fator = cfg['taxas'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    df_principal = load_principal(ano)
    df_vol = load_volume_bud(ano)
    df_tempo = load_tempo_veiculos(ano)

    if df_principal is None:
        st.error("Dados não encontrados.")
        st.stop()

    # ════════════════════════════════════════
    #  SIMULADOR DE VOLUME
    # ════════════════════════════════════════
    st.subheader("📊 Simulador de Volume")
    st.info("Ajuste os volumes e veja o impacto estimado no custo.")

    if df_vol is not None:
        veiculos = sorted(df_vol['Veículo'].unique())
        vol_anual_bud = df_vol.groupby('Veículo')['Volume'].sum()

        n_cols = min(5, len(veiculos))
        cols = st.columns(n_cols)
        vol_simulado = {}
        for i, v in enumerate(veiculos):
            with cols[i % n_cols]:
                base = int(vol_anual_bud.get(v, 0))
                vol_simulado[v] = st.number_input(
                    v, value=base, min_value=0, step=100, key=f'vol_sim_{v}',
                )

        vol_total_bud = vol_anual_bud.sum()
        vol_total_sim = sum(vol_simulado.values())
        var_pct = ((vol_total_sim / vol_total_bud) - 1) * 100 if vol_total_bud != 0 else 0

        # Custo proporcional
        custo_fp_total = df_principal['Custo FP'].sum()
        custo_fp_sim = custo_fp_total * (vol_total_sim / vol_total_bud) if vol_total_bud != 0 else 0

        # Aplicar fator e moeda
        custo_fp_total_display = aplicar_fator(custo_fp_total, fator)
        custo_fp_sim_display = aplicar_fator(custo_fp_sim, fator)
        if moeda != 'BRL':
            custo_fp_total_display = converter_moeda(custo_fp_total_display, moeda, taxas)
            custo_fp_sim_display = converter_moeda(custo_fp_sim_display, moeda, taxas)

        var_custo = ((custo_fp_sim / custo_fp_total) - 1) * 100 if custo_fp_total != 0 else 0

        c1, c2, c3 = st.columns(3)
        c1.metric("Volume Budget", f"{vol_total_bud:,.0f}")
        c2.metric("Volume Simulado", f"{vol_total_sim:,.0f}", delta=f"{var_pct:+.1f}%")
        c3.metric("Custo FP Estimado", f"{simbolo} {custo_fp_sim_display:,.0f}{sufixo}",
                  delta=f"{var_custo:+.1f}%")

        st.divider()

        # Gráfico comparativo (Altair)
        df_comp = pd.DataFrame({
            'Veículo': veiculos * 2,
            'Volume': [int(vol_anual_bud.get(v, 0)) for v in veiculos]
                      + [vol_simulado[v] for v in veiculos],
            'Cenário': ['Budget'] * len(veiculos) + ['Simulado'] * len(veiculos),
        })

        bar_comp = (alt.Chart(df_comp).mark_bar().encode(
            x=alt.X('Veículo:N'),
            y=alt.Y('Volume:Q'),
            color=alt.Color('Cenário:N',
                            scale=alt.Scale(domain=['Budget', 'Simulado'],
                                            range=['#4A90E2', '#FF6B35'])),
            xOffset='Cenário:N',
            tooltip=['Veículo:N', 'Cenário:N', alt.Tooltip('Volume:Q', format=',')],
        ).properties(height=400, title='Budget vs Simulado — Volume Anual')
         .configure_view(strokeWidth=0))
        st.altair_chart(bar_comp, use_container_width=True)
    else:
        st.warning("Dados de volume não encontrados.")

    # ════════════════════════════════════════
    #  SIMULADOR DE RATEIO FA
    # ════════════════════════════════════════
    st.subheader("⚖️ Simulador de Rateio FA")
    st.info("💡 **Oficinas QY, GS e SM** usam fatores multiplicativos. "
            "O rateio FA = `fator × taxa_PdR_global` por período. "
            "**BS, PS, PL** são calculados automaticamente.")

    rateio_path = 'rateios_manuais.json'
    if os.path.exists(rateio_path):
        with open(rateio_path, 'r') as f:
            rateios_base = json.load(f)
    else:
        rateios_base = {'QY': 0.087526, 'GS': 0.086982, 'SM': 0.075452}

    # Taxa PdR média
    taxa_pdr_media = None
    if df_tempo is not None:
        try:
            tempo_agg = normalizar_periodo(df_tempo.copy()).groupby(
                'Período', as_index=False
            ).agg({'Tempo FA': 'sum', 'Tempo Veíc': 'sum'})
            if 'Tempo Veíc' not in tempo_agg.columns and 'Tempo Veic' in df_tempo.columns:
                tempo_agg = normalizar_periodo(df_tempo.copy()).groupby(
                    'Período', as_index=False
                ).agg({'Tempo FA': 'sum', 'Tempo Veic': 'sum'})
                tempo_agg = tempo_agg.rename(columns={'Tempo Veic': 'Tempo Veíc'})
            tempo_agg['Taxa PdR'] = tempo_agg['Tempo FA'] / (
                tempo_agg['Tempo FA'] + tempo_agg['Tempo Veíc']
            )
            taxa_pdr_media = tempo_agg['Taxa PdR'].mean()
        except Exception:
            pass

    oficinas_manuais = sorted(rateios_base.keys())
    cols_r = st.columns(len(oficinas_manuais))
    rateios_sim = {}
    for i, ofi in enumerate(oficinas_manuais):
        with cols_r[i]:
            rateios_sim[ofi] = st.number_input(
                f"Fator {ofi}", value=float(rateios_base.get(ofi, 0)),
                min_value=0.0, max_value=5.0, step=0.001,
                format="%.6f", key=f'rat_sim_{ofi}',
            )
            if taxa_pdr_media is not None:
                rateio_efetivo = rateios_sim[ofi] * taxa_pdr_media
                st.caption(f"Rateio efetivo ≈ {rateio_efetivo:.4f} ({rateio_efetivo*100:.2f}%)")

    # Impacto do rateio
    df_ofi = df_principal.groupby('Oficina', as_index=False).agg({
        'Despesa Primaria': 'sum', 'Custo FA': 'sum', 'Custo FP': 'sum',
    })

    resultados = []
    for _, row in df_ofi.iterrows():
        ofi = row['Oficina']
        if ofi in rateios_sim and taxa_pdr_media is not None and taxa_pdr_media > 0:
            novo_fa = row['Despesa Primaria'] * rateios_sim[ofi] * taxa_pdr_media
        else:
            novo_fa = row['Custo FA']
        diff = novo_fa - row['Custo FA']

        # Aplicar fator e moeda
        fa_bud = aplicar_fator(row['Custo FA'], fator)
        fa_sim = aplicar_fator(novo_fa, fator)
        diff_display = aplicar_fator(diff, fator)
        if moeda != 'BRL':
            fa_bud = converter_moeda(fa_bud, moeda, taxas)
            fa_sim = converter_moeda(fa_sim, moeda, taxas)
            diff_display = converter_moeda(diff_display, moeda, taxas)

        resultados.append({
            'Oficina': ofi,
            f'Custo FA Budget ({simbolo}{sufixo})': f"{fa_bud:,.2f}",
            f'Custo FA Simulado ({simbolo}{sufixo})': f"{fa_sim:,.2f}",
            f'Diferença ({simbolo}{sufixo})': f"{diff_display:,.2f}",
        })

    st.markdown("**Impacto estimado da alteração do fator de rateio FA:**")
    st.dataframe(pd.DataFrame(resultados), use_container_width=True, hide_index=True)

    st.divider()
    st.caption(f"TC — Planta Principal | Simulador | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
