"""
TC Principal — Debug de Cálculos
8 abas de auditoria: integridade, tabela principal, rateio FA, custo FA,
                     custo FP, D&A, volume/tempo, comparar Excel.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    _pasta_tc_principal, load_principal,
    load_volume_bud, load_volume_actual,
    load_tempo_veiculos, load_dea_dedicado, load_volume_fa,
    normalizar_periodo, ordenar_por_mes,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header, criar_tabela_html,
)


# ════════════════════════════════════════════
# Helpers internos
# ════════════════════════════════════════════

def _fmt(v, dec=2):
    if pd.isna(v):
        return '—'
    return f"{v:,.{dec}f}"


def _resumo_df(df, nome: str):
    """Mostra resumo rápido de um dataframe."""
    if df is None:
        st.warning(f"⚠️ `{nome}` — Dados não encontrados.")
        return
    st.markdown(f"**{nome}** — {len(df):,} linhas × {len(df.columns)} colunas")
    st.caption(f"Colunas: {', '.join(df.columns.tolist())}")


# ════════════════════════════════════════════
# Render
# ════════════════════════════════════════════

def render():
    injetar_css_global()
    render_header()

    st.title("🔍 Debug de Cálculos")
    st.subheader("Auditoria da Pipeline de Dados • TC Planta Principal")

    # ── Sidebar ──
    anos_disp = []
    pasta_dados = 'dados'
    if os.path.exists(pasta_dados):
        for d in sorted(os.listdir(pasta_dados), reverse=True):
            pasta_tc = os.path.join(pasta_dados, d, 'TC_Principal', 'BUD')
            if os.path.isdir(pasta_tc):
                try:
                    anos_disp.append(int(d))
                except ValueError:
                    pass
    if not anos_disp:
        anos_disp = [datetime.now().year]

    ano = st.sidebar.selectbox("📅 Ano", anos_disp, key='debug_ano')

    # Carregar todos os datasets
    df = load_principal(ano)
    df_vol_bud = load_volume_bud(ano)
    df_vol_act = load_volume_actual(ano)
    df_tempo = load_tempo_veiculos(ano)
    df_dea = load_dea_dedicado(ano)
    df_vol_fa = load_volume_fa(ano)

    if df is None:
        st.error("❌ `df_principal_BUD.parquet` não encontrado. Execute o processamento primeiro.")
        st.stop()

    df = normalizar_periodo(df)

    # ══════════════════════════════════════
    #  TABS
    # ══════════════════════════════════════
    tabs = st.tabs([
        "1️⃣ Integridade",
        "2️⃣ Tabela Principal",
        "3️⃣ Rateio FA",
        "4️⃣ Custo FA",
        "5️⃣ Custo FP",
        "6️⃣ D&A",
        "7️⃣ Volume/Tempo",
        "8️⃣ Comparar Excel",
    ])

    # ── 1. INTEGRIDADE ──
    with tabs[0]:
        st.subheader("Integridade dos Parquets")

        pasta = _pasta_tc_principal(ano)
        parquets = [
            ('df_principal_BUD.parquet', df),
            ('df_vol_veiculos_BUD.parquet', df_vol_bud),
            ('df_vol_veiculos_actual.parquet', df_vol_act),
            ('df_tempo_veiculos_BUD.parquet', df_tempo),
            ('df_dea_dedicado_BUD.parquet', df_dea),
            ('df_vol_fa_veiculos_BUD.parquet', df_vol_fa),
        ]
        for nome_arq, dados in parquets:
            caminho = os.path.join(pasta, nome_arq)
            if dados is not None:
                tam = os.path.getsize(caminho) / 1024 if os.path.exists(caminho) else 0
                st.success(f"✅ `{nome_arq}` — {len(dados):,} linhas × {len(dados.columns)} cols | {tam:.0f} KB")
            else:
                st.warning(f"⚠️ `{nome_arq}` — não encontrado")

        st.divider()
        st.markdown("**Verificação de NaN na Tabela Principal:**")
        nan_cols = df.isnull().sum()
        nan_cols = nan_cols[nan_cols > 0]
        if nan_cols.empty:
            st.success("✅ Nenhum NaN detectado.")
        else:
            st.warning("⚠️ Colunas com NaN:")
            st.dataframe(nan_cols.to_frame('NaN count'), use_container_width=True)

        st.divider()
        st.markdown("**Períodos únicos:**")
        periodos = sorted(df['Período'].unique().tolist()) if 'Período' in df.columns else []
        st.write(periodos)

        st.markdown("**Oficinas únicas:**")
        oficinas = sorted(df['Oficina'].unique().tolist()) if 'Oficina' in df.columns else []
        st.write(oficinas)

        if 'Custo' in df.columns:
            st.markdown("**Valores únicos coluna 'Custo' (Fixo/Variável):**")
            st.write(sorted(df['Custo'].dropna().unique().tolist()))

    # ── 2. TABELA PRINCIPAL ──
    with tabs[1]:
        st.subheader("Tabela Principal Completa")

        # Filtros locais
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            ofi_sel = st.multiselect("Oficina", sorted(df['Oficina'].unique()),
                                      key='dbg_ofi')
        with col_f2:
            per_sel = st.multiselect("Período", [m for m in ORDEM_MESES if m in df['Período'].values],
                                      key='dbg_per')

        df_filt = df.copy()
        if ofi_sel:
            df_filt = df_filt[df_filt['Oficina'].isin(ofi_sel)]
        if per_sel:
            df_filt = df_filt[df_filt['Período'].isin(per_sel)]

        st.caption(f"{len(df_filt):,} linhas")
        st.dataframe(df_filt, use_container_width=True, height=500)

        st.divider()
        st.markdown("**Totais por coluna monetária:**")
        totais = {}
        for c in COLUNAS_MONETARIAS:
            if c in df_filt.columns:
                totais[c] = df_filt[c].sum()
        st.dataframe(pd.DataFrame(totais, index=['Total']).T.rename(columns={0: 'Valor'}),
                     use_container_width=True)

    # ── 3. RATEIO FA ──
    with tabs[2]:
        st.subheader("Rateio FA — Detalhes")

        if 'Rateio FA' in df.columns:
            st.markdown("**Rateios por Oficina (média):**")
            rateios_ofi = df.groupby('Oficina')['Rateio FA'].mean().sort_values(ascending=False)
            st.dataframe(rateios_ofi.to_frame('Rateio FA (média)').style.format("{:.6f}"),
                         use_container_width=True)

            st.divider()
            st.markdown("**Rateios por Oficina × Período:**")
            pivot_rateio = df.pivot_table(values='Rateio FA', index='Oficina',
                                          columns='Período', aggfunc='mean')
            # Reordenar colunas
            cols_ord = [m for m in ORDEM_MESES if m in pivot_rateio.columns]
            if cols_ord:
                pivot_rateio = pivot_rateio[cols_ord]
            st.dataframe(pivot_rateio.style.format("{:.6f}"), use_container_width=True)
        else:
            st.warning("⚠️ Coluna 'Rateio FA' não encontrada no dataframe.")

        # Volume FA
        if df_vol_fa is not None:
            st.divider()
            st.markdown("**Volume FA (Tempo FA):**")
            _resumo_df(df_vol_fa, 'df_vol_fa')
            st.dataframe(df_vol_fa.head(50), use_container_width=True, height=300)

    # ── 4. CUSTO FA ──
    with tabs[3]:
        st.subheader("Custo FA — Auditoria")
        st.caption("Custo FA = Rateio FA × Despesa Primária")

        if 'Custo FA' in df.columns and 'Rateio FA' in df.columns and 'Despesa Primaria' in df.columns:
            df_fa = df[['Oficina', 'Account', 'Período', 'Despesa Primaria', 'Rateio FA', 'Custo FA']].copy()
            df_fa['Recalculado'] = df_fa['Rateio FA'] * df_fa['Despesa Primaria']
            df_fa['Diff'] = (df_fa['Custo FA'] - df_fa['Recalculado']).round(4)
            df_fa['OK'] = df_fa['Diff'].abs() < 0.01

            erros = df_fa[~df_fa['OK']]
            if erros.empty:
                st.success(f"✅ Todas as {len(df_fa):,} linhas batem (diff < 0,01).")
            else:
                st.error(f"❌ {len(erros):,} linhas com diferença ≥ 0,01")
                st.dataframe(erros, use_container_width=True)

            st.divider()
            st.markdown("**Total Custo FA:**")
            total_fa = df['Custo FA'].sum()
            st.metric("Custo FA Total", f"R$ {total_fa:,.2f}")

            # Por oficina
            fa_ofi = df.groupby('Oficina')['Custo FA'].sum().sort_values(ascending=False)
            st.markdown("**Custo FA por Oficina:**")
            st.dataframe(fa_ofi.to_frame().style.format("R$ {:,.2f}"), use_container_width=True)
        else:
            st.warning("⚠️ Colunas necessárias para auditoria de Custo FA não encontradas.")

    # ── 5. CUSTO FP ──
    with tabs[4]:
        st.subheader("Custo FP — Auditoria")
        st.caption("Custo FP = Despesa Primária − Custo FA + Redistribuição")

        cols_fp = ['Oficina', 'Account', 'Período', 'Despesa Primaria', 'Custo FA', 'Redis', 'Custo FP']
        cols_fp = [c for c in cols_fp if c in df.columns]
        df_fp = df[cols_fp].copy()

        if all(c in df.columns for c in ['Despesa Primaria', 'Custo FA', 'Redis', 'Custo FP']):
            df_fp['Recalculado'] = df_fp['Despesa Primaria'] - df_fp['Custo FA'] + df_fp['Redis']
            df_fp['Diff'] = (df_fp['Custo FP'] - df_fp['Recalculado']).round(4)
            df_fp['OK'] = df_fp['Diff'].abs() < 0.01

            erros = df_fp[~df_fp['OK']]
            if erros.empty:
                st.success(f"✅ Todas as {len(df_fp):,} linhas batem (diff < 0,01).")
            else:
                st.error(f"❌ {len(erros):,} linhas com diferença ≥ 0,01")
                st.dataframe(erros, use_container_width=True)

            st.divider()
            st.markdown("**Totais:**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Despesa Primária", f"R$ {df['Despesa Primaria'].sum():,.2f}")
            c2.metric("Custo FA", f"R$ {df['Custo FA'].sum():,.2f}")
            c3.metric("Redis", f"R$ {df['Redis'].sum():,.2f}")
            c4.metric("Custo FP", f"R$ {df['Custo FP'].sum():,.2f}")

            # Tipo Custo (Fixo/Variável) breakdown
            if 'Custo' in df.columns:
                st.divider()
                st.markdown("**Custo FP por Tipo (Fixo/Variável):**")
                fp_tipo = df.groupby('Custo')['Custo FP'].sum().sort_values(ascending=False)
                st.dataframe(fp_tipo.to_frame().style.format("R$ {:,.2f}"), use_container_width=True)
        else:
            st.warning("⚠️ Colunas necessárias para auditoria de Custo FP não encontradas.")

    # ── 6. D&A DEDICADO ──
    with tabs[5]:
        st.subheader("D&A Dedicado — Auditoria")

        if 'D&A dedicado' in df.columns and 'FP sem Dedicada' in df.columns:
            st.caption("FP sem Dedicada = Custo FP − D&A dedicado")

            df_dea_check = df[['Oficina', 'Período', 'Custo FP', 'D&A dedicado', 'FP sem Dedicada']].copy()
            df_dea_check['Recalculado'] = df_dea_check['Custo FP'] - df_dea_check['D&A dedicado']
            df_dea_check['Diff'] = (df_dea_check['FP sem Dedicada'] - df_dea_check['Recalculado']).round(4)
            df_dea_check['OK'] = df_dea_check['Diff'].abs() < 0.01

            erros = df_dea_check[~df_dea_check['OK']]
            if erros.empty:
                st.success(f"✅ Todas as {len(df_dea_check):,} linhas batem.")
            else:
                st.error(f"❌ {len(erros):,} linhas com diferença")
                st.dataframe(erros, use_container_width=True)

            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric("Custo FP", f"R$ {df['Custo FP'].sum():,.2f}")
            c2.metric("D&A Dedicado", f"R$ {df['D&A dedicado'].sum():,.2f}")
            c3.metric("FP sem Dedicada", f"R$ {df['FP sem Dedicada'].sum():,.2f}")
        else:
            st.warning("⚠️ Colunas 'D&A dedicado' / 'FP sem Dedicada' não encontradas.")

        # Dataset separado
        if df_dea is not None:
            st.divider()
            st.markdown("**Parquet `df_dea_dedicado_BUD.parquet`:**")
            _resumo_df(df_dea, 'df_dea_dedicado')
            dea_ofi = df_dea.groupby('Oficina')['D&A dedicado'].sum().sort_values(ascending=False)
            st.dataframe(dea_ofi.to_frame().style.format("R$ {:,.2f}"), use_container_width=True)

    # ── 7. VOLUME / TEMPO ──
    with tabs[6]:
        st.subheader("Volume e Tempo — Auditoria")

        col_v1, col_v2 = st.columns(2)

        with col_v1:
            st.markdown("**Volume Budget**")
            if df_vol_bud is not None:
                _resumo_df(df_vol_bud, 'df_vol_veiculos_BUD')
                vol_total = df_vol_bud['Volume'].sum() if 'Volume' in df_vol_bud.columns else 0
                st.metric("Volume Budget Total", f"{vol_total:,.0f}")
                st.dataframe(df_vol_bud.head(30), use_container_width=True, height=300)
            else:
                st.warning("⚠️ Parquet não encontrado.")

        with col_v2:
            st.markdown("**Volume Actual**")
            if df_vol_act is not None:
                _resumo_df(df_vol_act, 'df_vol_veiculos_actual')
                vol_total = df_vol_act['Volume'].sum() if 'Volume' in df_vol_act.columns else 0
                st.metric("Volume Actual Total", f"{vol_total:,.0f}")
                st.dataframe(df_vol_act.head(30), use_container_width=True, height=300)
            else:
                st.warning("⚠️ Parquet não encontrado.")

        st.divider()
        st.markdown("**Tempo Veículos**")
        if df_tempo is not None:
            _resumo_df(df_tempo, 'df_tempo_veiculos_BUD')
            if 'Tempo Veic' in df_tempo.columns:
                tempo_ofi = df_tempo.groupby('Oficina')['Tempo Veic'].sum().sort_values(ascending=False)
                st.dataframe(tempo_ofi.to_frame().style.format("{:,.2f}"), use_container_width=True)
            st.dataframe(df_tempo.head(30), use_container_width=True, height=300)
        else:
            st.warning("⚠️ Parquet não encontrado.")

    # ── 8. COMPARAR EXCEL ──
    with tabs[7]:
        st.subheader("Comparar com Excel Fonte")
        st.caption("Faça upload do Excel para comparar totais com os parquets processados.")

        uploaded = st.file_uploader("📄 Upload: Reporting fluxo anexo.xlsx", type=['xlsx'],
                                     key='dbg_upload_excel')
        if uploaded:
            try:
                xl = pd.ExcelFile(uploaded)
                st.success(f"✅ Abas disponíveis: {xl.sheet_names}")

                aba_sel = st.selectbox("Selecionar aba:", xl.sheet_names, key='dbg_aba')
                df_excel = pd.read_excel(uploaded, sheet_name=aba_sel)
                st.markdown(f"**{aba_sel}** — {len(df_excel):,} linhas × {len(df_excel.columns)} cols")
                st.dataframe(df_excel.head(50), use_container_width=True, height=400)

                # Comparação automática de totais
                st.divider()
                st.markdown("**Comparação de totais numéricos:**")
                num_cols_excel = df_excel.select_dtypes(include=[np.number]).columns
                if len(num_cols_excel) > 0:
                    totais_excel = df_excel[num_cols_excel].sum()
                    st.dataframe(totais_excel.to_frame('Total Excel').style.format("{:,.2f}"),
                                 use_container_width=True)

            except Exception as e:
                st.error(f"❌ Erro ao ler Excel: {e}")
        else:
            st.info("Faça upload do arquivo Excel para comparar.")

            # Tentar carregar automaticamente se existir
            caminho_auto = os.path.join('dados', str(ano), 'Reporting fluxo anexo.xlsx')
            if os.path.exists(caminho_auto):
                st.caption(f"💡 Arquivo encontrado em `{caminho_auto}`. Use o upload acima ou:")
                if st.button("📂 Carregar arquivo local", key='dbg_load_local'):
                    try:
                        xl = pd.ExcelFile(caminho_auto)
                        st.success(f"Abas: {xl.sheet_names}")
                    except Exception as e:
                        st.error(f"Erro: {e}")

    st.divider()
    st.caption(f"TC — Planta Principal | Debug | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
