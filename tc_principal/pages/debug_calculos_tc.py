"""
TC Principal — Debug de Cálculos
15 abas de auditoria: integridade, tabela principal, rateio FA, custo FA,
                     custo FP, D&A, volume/tempo, comparar Excel,
                     + 7 novas abas de cálculo por veículo.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime

from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS, ACCOUNT_REDIS,
    _pasta_tc_principal, load_principal,
    load_volume_bud, load_volume_actual,
    load_tempo_veiculos, load_dea_dedicado, load_volume_fa,
    load_fp_sem_da_veiculos, load_percentual_rateio_veiculos,
    load_custo_rateado_veiculos, load_custo_fp_veiculo, load_cpu_veiculo,
    normalizar_periodo, ordenar_por_mes, extrair_redis,
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

    # Novos datasets de veículos
    df_fp_sem_da = load_fp_sem_da_veiculos(ano)
    df_pct_rateio = load_percentual_rateio_veiculos(ano)
    df_custo_rateado = load_custo_rateado_veiculos(ano)
    df_custo_fp_veic = load_custo_fp_veiculo(ano)
    df_cpu_veic = load_cpu_veiculo(ano)

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
        "9️⃣ % Rateio Veíc",
        "🔟 FP sem D&A",
        "1️⃣1️⃣ Custo Rateado",
        "1️⃣2️⃣ FP Veículo",
        "1️⃣3️⃣ Antes×Depois",
        "1️⃣4️⃣ CPU Veículo",
        "1️⃣5️⃣ Inconsistências",
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
            ('df_veiculos_fp_sem_da_BUD.parquet', df_fp_sem_da),
            ('df_veiculos_percentual_rateio_BUD.parquet', df_pct_rateio),
            ('df_veiculos_custo_rateado_BUD.parquet', df_custo_rateado),
            ('df_veiculos_custo_fp_BUD.parquet', df_custo_fp_veic),
            ('df_veiculos_cpu_BUD.parquet', df_cpu_veic),
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
        st.caption("Custo FP = Despesa Primária − Custo FA (Redis como linhas com Rateio FA = 0)")

        cols_fp = ['Oficina', 'Account', 'Período', 'Despesa Primaria', 'Custo FA', 'Custo FP']
        cols_fp = [c for c in cols_fp if c in df.columns]
        df_fp = df[cols_fp].copy()

        if all(c in df.columns for c in ['Despesa Primaria', 'Custo FA', 'Custo FP']):
            df_fp['Recalculado'] = df_fp['Despesa Primaria'] - df_fp['Custo FA']
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
            redis_total = extrair_redis(df)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Despesa Primária", f"R$ {df['Despesa Primaria'].sum():,.2f}")
            c2.metric("Custo FA", f"R$ {df['Custo FA'].sum():,.2f}")
            c3.metric("Redis (linhas)", f"R$ {redis_total:,.2f}")
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

    # ══════════════════════════════════════
    #  9. PERCENTUAIS DE RATEIO POR VEÍCULO
    # ══════════════════════════════════════
    with tabs[8]:
        st.subheader("Percentuais de Rateio por Veículo")
        st.caption("Percentual = (EST × Volume) / Σ(EST × Volume por oficina)")

        if df_pct_rateio is not None:
            _resumo_df(df_pct_rateio, 'df_veiculos_percentual_rateio_BUD')

            # Filtros locais
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                ofi_sel9 = st.multiselect("Oficina", sorted(df_pct_rateio['Oficina'].unique()),
                                           key='dbg_pct_ofi')
            with col_f2:
                per_sel9 = st.multiselect("Período",
                    [m for m in ORDEM_MESES if m in df_pct_rateio['Período'].values],
                    key='dbg_pct_per')

            df_pct_f = df_pct_rateio.copy()
            if ofi_sel9:
                df_pct_f = df_pct_f[df_pct_f['Oficina'].isin(ofi_sel9)]
            if per_sel9:
                df_pct_f = df_pct_f[df_pct_f['Período'].isin(per_sel9)]

            st.dataframe(df_pct_f, use_container_width=True, height=500)

            # Validação: soma por oficina/período = 100%
            st.divider()
            st.markdown("**Validação: Σ Percentual por (Oficina, Período) = 100%**")
            soma_pct = df_pct_rateio.groupby(['Oficina', 'Período'])['Percentual'].sum().reset_index()
            soma_pct['OK'] = (soma_pct['Percentual'] - 1.0).abs() < 0.001
            erros_pct = soma_pct[~soma_pct['OK']]
            if erros_pct.empty:
                st.success(f"✅ Todos os {len(soma_pct)} grupos somam 100%.")
            else:
                st.error(f"❌ {len(erros_pct)} grupos com soma ≠ 100%:")
                st.dataframe(erros_pct, use_container_width=True)

            # Pivot Oficina × Veículo (média dos percentuais)
            st.divider()
            st.markdown("**Percentuais médios: Oficina × Veículo**")
            pivot_pct = df_pct_rateio.pivot_table(
                values='Percentual', index='Oficina', columns='Veículo', aggfunc='mean'
            )
            st.dataframe(pivot_pct.style.format("{:.4%}"), use_container_width=True)
        else:
            st.warning("⚠️ Parquet `df_veiculos_percentual_rateio_BUD.parquet` não encontrado. Execute o processamento.")

    # ══════════════════════════════════════
    #  10. FP SEM D&A DEDICADO
    # ══════════════════════════════════════
    with tabs[9]:
        st.subheader("Custo FP sem D&A Dedicado")
        st.caption("Base de rateio = Custo FP − D&A dedicado = FP sem Dedicada")

        if df_fp_sem_da is not None:
            _resumo_df(df_fp_sem_da, 'df_veiculos_fp_sem_da_BUD')

            col_f1, col_f2 = st.columns(2)
            with col_f1:
                ofi_sel10 = st.multiselect("Oficina", sorted(df_fp_sem_da['Oficina'].unique()),
                                            key='dbg_fpsda_ofi')
            with col_f2:
                per_sel10 = st.multiselect("Período",
                    [m for m in ORDEM_MESES if m in df_fp_sem_da['Período'].values],
                    key='dbg_fpsda_per')

            df_fpsda_f = df_fp_sem_da.copy()
            if ofi_sel10:
                df_fpsda_f = df_fpsda_f[df_fpsda_f['Oficina'].isin(ofi_sel10)]
            if per_sel10:
                df_fpsda_f = df_fpsda_f[df_fpsda_f['Período'].isin(per_sel10)]

            st.dataframe(df_fpsda_f, use_container_width=True, height=500)

            st.divider()
            st.markdown("**Totais:**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Custo FP", f"R$ {df_fp_sem_da['Custo FP'].sum():,.2f}")
            c2.metric("D&A Dedicado", f"R$ {df_fp_sem_da['D&A dedicado'].sum():,.2f}")
            c3.metric("FP sem Dedicada", f"R$ {df_fp_sem_da['FP sem Dedicada'].sum():,.2f}")
        else:
            st.warning("⚠️ Parquet `df_veiculos_fp_sem_da_BUD.parquet` não encontrado. Execute o processamento.")

    # ══════════════════════════════════════
    #  11. CUSTO RATEADO POR VEÍCULO
    # ══════════════════════════════════════
    with tabs[10]:
        st.subheader("Custo Rateado por Veículo")
        st.caption("Custo Rateado = FP sem Dedicada × Percentual do veículo")

        if df_custo_rateado is not None:
            _resumo_df(df_custo_rateado, 'df_veiculos_custo_rateado_BUD')

            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                ofi_sel11 = st.multiselect("Oficina", sorted(df_custo_rateado['Oficina'].unique()),
                                            key='dbg_rat_ofi')
            with col_f2:
                veic_sel11 = st.multiselect("Veículo",
                    sorted(df_custo_rateado['Veículo'].unique()) if 'Veículo' in df_custo_rateado.columns else [],
                    key='dbg_rat_veic')
            with col_f3:
                per_sel11 = st.multiselect("Período",
                    [m for m in ORDEM_MESES if m in df_custo_rateado['Período'].values],
                    key='dbg_rat_per')

            df_rat_f = df_custo_rateado.copy()
            if ofi_sel11:
                df_rat_f = df_rat_f[df_rat_f['Oficina'].isin(ofi_sel11)]
            if veic_sel11:
                df_rat_f = df_rat_f[df_rat_f['Veículo'].isin(veic_sel11)]
            if per_sel11:
                df_rat_f = df_rat_f[df_rat_f['Período'].isin(per_sel11)]

            st.dataframe(df_rat_f, use_container_width=True, height=500)

            # Validação: soma rateado = soma FP sem Ded
            st.divider()
            st.markdown("**Validação: Σ Custo Rateado ≈ Σ FP sem Dedicada (original)**")
            soma_rateado = df_custo_rateado['Custo Rateado'].sum()
            soma_fp_sem = df['FP sem Dedicada'].sum() if 'FP sem Dedicada' in df.columns else 0
            diff_rat = abs(soma_rateado - soma_fp_sem)
            c1, c2, c3 = st.columns(3)
            c1.metric("Σ FP sem Ded (original)", f"R$ {soma_fp_sem:,.2f}")
            c2.metric("Σ Custo Rateado", f"R$ {soma_rateado:,.2f}")
            c3.metric("Diferença", f"R$ {diff_rat:,.2f}",
                       delta=f"{'✅ OK' if diff_rat < 0.01 else '❌ DIVERGENTE'}",
                       delta_color="off" if diff_rat < 0.01 else "inverse")
        else:
            st.warning("⚠️ Parquet `df_veiculos_custo_rateado_BUD.parquet` não encontrado.")

    # ══════════════════════════════════════
    #  12. CUSTO FP VEÍCULO
    # ══════════════════════════════════════
    with tabs[11]:
        st.subheader("Custo FP por Veículo")
        st.caption("Custo FP Veículo = Custo Rateado + D&A dedicado")

        if df_custo_fp_veic is not None:
            _resumo_df(df_custo_fp_veic, 'df_veiculos_custo_fp_BUD')

            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                ofi_sel12 = st.multiselect("Oficina", sorted(df_custo_fp_veic['Oficina'].unique()),
                                            key='dbg_fpv_ofi')
            with col_f2:
                veic_sel12 = st.multiselect("Veículo",
                    sorted(df_custo_fp_veic['Veículo'].unique()) if 'Veículo' in df_custo_fp_veic.columns else [],
                    key='dbg_fpv_veic')
            with col_f3:
                per_sel12 = st.multiselect("Período",
                    [m for m in ORDEM_MESES if m in df_custo_fp_veic['Período'].values],
                    key='dbg_fpv_per')

            df_fpv_f = df_custo_fp_veic.copy()
            if ofi_sel12:
                df_fpv_f = df_fpv_f[df_fpv_f['Oficina'].isin(ofi_sel12)]
            if veic_sel12:
                df_fpv_f = df_fpv_f[df_fpv_f['Veículo'].isin(veic_sel12)]
            if per_sel12:
                df_fpv_f = df_fpv_f[df_fpv_f['Período'].isin(per_sel12)]

            st.dataframe(df_fpv_f, use_container_width=True, height=500)

            # Totais por veículo
            st.divider()
            st.markdown("**Custo FP Veículo por modelo:**")
            if 'Custo FP Veiculo' in df_custo_fp_veic.columns:
                fp_veic = df_custo_fp_veic.groupby('Veículo')['Custo FP Veiculo'].sum().sort_values(ascending=False)
                st.dataframe(fp_veic.to_frame().style.format("R$ {:,.2f}"), use_container_width=True)
        else:
            st.warning("⚠️ Parquet `df_veiculos_custo_fp_BUD.parquet` não encontrado.")

    # ══════════════════════════════════════
    #  13. ANTES × DEPOIS (FECHAMENTO GERAL)
    # ══════════════════════════════════════
    with tabs[12]:
        st.subheader("Comparação Antes × Depois")
        st.caption("Validação: Σ Custo FP (original) deve ser igual a Σ Custo FP Veículo")

        # Dados originais
        soma_fp_original = df['Custo FP'].sum() if 'Custo FP' in df.columns else 0
        soma_da_original = df['D&A dedicado'].sum() if 'D&A dedicado' in df.columns else 0
        soma_fp_sem_ded = df['FP sem Dedicada'].sum() if 'FP sem Dedicada' in df.columns else 0

        # Dados calculados
        soma_fp_veiculo = df_custo_fp_veic['Custo FP Veiculo'].sum() if df_custo_fp_veic is not None and 'Custo FP Veiculo' in df_custo_fp_veic.columns else 0
        soma_rateado = df_custo_rateado['Custo Rateado'].sum() if df_custo_rateado is not None and 'Custo Rateado' in df_custo_rateado.columns else 0
        soma_da_veiculo = df_custo_fp_veic['D&A dedicado'].sum() if df_custo_fp_veic is not None and 'D&A dedicado' in df_custo_fp_veic.columns else 0

        # Tabela comparativa
        comparacao = pd.DataFrame({
            'Indicador': [
                'Σ Custo FP (original)',
                'Σ FP sem Dedicada (original)',
                'Σ D&A Dedicado (original)',
                'Σ Custo Rateado (veículos)',
                'Σ D&A Dedicado (veículos)',
                'Σ Custo FP Veículo',
            ],
            'Valor (R$)': [
                soma_fp_original,
                soma_fp_sem_ded,
                soma_da_original,
                soma_rateado,
                soma_da_veiculo,
                soma_fp_veiculo,
            ]
        })
        st.dataframe(comparacao.style.format({'Valor (R$)': "R$ {:,.2f}"}), use_container_width=True)

        # Alerta principal
        st.divider()
        diff_principal = abs(soma_fp_original - soma_fp_veiculo)
        c1, c2, c3 = st.columns(3)
        c1.metric("Σ Custo FP Original", f"R$ {soma_fp_original:,.2f}")
        c2.metric("Σ Custo FP Veículo", f"R$ {soma_fp_veiculo:,.2f}")
        c3.metric("Diferença", f"R$ {diff_principal:,.2f}")

        if diff_principal < 0.01:
            st.success("✅ **FECHAMENTO OK** — Os valores batem. A soma do Custo FP Veículo é igual ao Custo FP original.")
        elif soma_fp_veiculo == 0:
            st.warning("⚠️ Dados de Custo FP Veículo não disponíveis. Execute o processamento primeiro.")
        else:
            st.error(f"❌ **DIVERGÊNCIA DETECTADA** — Diferença de R$ {diff_principal:,.2f}. "
                     "Verifique o processamento das Fases 13–16.")

    # ══════════════════════════════════════
    #  14. CPU POR VEÍCULO
    # ══════════════════════════════════════
    with tabs[13]:
        st.subheader("CPU (Custo Por Unidade) por Veículo")
        st.caption("CPU = Custo FP Veículo / Volume do Veículo")

        if df_cpu_veic is not None:
            _resumo_df(df_cpu_veic, 'df_veiculos_cpu_BUD')

            st.dataframe(df_cpu_veic, use_container_width=True, height=400)

            # Pivot Veículo × Período
            st.divider()
            st.markdown("**CPU por Veículo × Período:**")
            if 'CPU' in df_cpu_veic.columns:
                pivot_cpu = df_cpu_veic.pivot_table(
                    values='CPU', index='Veículo', columns='Período', aggfunc='sum'
                )
                cols_ord = [m for m in ORDEM_MESES if m in pivot_cpu.columns]
                if cols_ord:
                    pivot_cpu = pivot_cpu[cols_ord]
                st.dataframe(pivot_cpu.style.format("R$ {:,.2f}"), use_container_width=True)

            # Volume usado
            st.divider()
            st.markdown("**Volume usado no cálculo:**")
            if 'Volume' in df_cpu_veic.columns:
                pivot_vol = df_cpu_veic.pivot_table(
                    values='Volume', index='Veículo', columns='Período', aggfunc='sum'
                )
                cols_ord = [m for m in ORDEM_MESES if m in pivot_vol.columns]
                if cols_ord:
                    pivot_vol = pivot_vol[cols_ord]
                st.dataframe(pivot_vol.style.format("{:,.0f}"), use_container_width=True)

            # Alertas volumes zero
            if 'Volume' in df_cpu_veic.columns:
                zeros = df_cpu_veic[df_cpu_veic['Volume'] == 0]
                if len(zeros) > 0:
                    st.warning(f"⚠️ {len(zeros)} linhas com Volume = 0 (CPU será 0):")
                    st.dataframe(zeros[['Veículo', 'Período', 'Volume']], use_container_width=True)
        else:
            st.warning("⚠️ Parquet `df_veiculos_cpu_BUD.parquet` não encontrado.")

    # ══════════════════════════════════════
    #  15. INDICADORES DE INCONSISTÊNCIA
    # ══════════════════════════════════════
    with tabs[14]:
        st.subheader("Dashboard de Inconsistências")
        st.caption("Consolidação de todas as validações dos cálculos por veículo")

        alertas = []

        # 1. Fechamento geral FP
        diff_fp = abs(soma_fp_original - soma_fp_veiculo)
        if soma_fp_veiculo == 0:
            alertas.append(('⚠️', 'Custo FP Veículo não processado', 'Execute a extração para gerar os dados'))
        elif diff_fp > 0.01:
            alertas.append(('❌', f'Fechamento Custo FP: diff = R$ {diff_fp:,.2f}',
                           'Σ Custo FP ≠ Σ Custo FP Veículo'))
        else:
            alertas.append(('✅', 'Fechamento Custo FP OK', f'Diff = R$ {diff_fp:,.6f}'))

        # 2. Percentuais de rateio
        if df_pct_rateio is not None:
            soma_pct_check = df_pct_rateio.groupby(['Oficina', 'Período'])['Percentual'].sum()
            erros_pct_check = soma_pct_check[(soma_pct_check - 1.0).abs() > 0.001]
            if len(erros_pct_check) > 0:
                alertas.append(('❌', f'{len(erros_pct_check)} grupos com Σ%≠100%',
                               'Verificar tempos de veículos'))
            else:
                alertas.append(('✅', f'Percentuais de rateio OK ({len(soma_pct_check)} grupos)',
                               'Todos somam 100%'))
        else:
            alertas.append(('⚠️', 'Percentuais de rateio não disponíveis', 'Execute o processamento'))

        # 3. Volumes zerados
        if df_cpu_veic is not None and 'Volume' in df_cpu_veic.columns:
            n_vol_zero = (df_cpu_veic['Volume'] == 0).sum()
            if n_vol_zero > 0:
                alertas.append(('⚠️', f'{n_vol_zero} linhas com volume = 0 no CPU',
                               'CPU será 0 nestas linhas'))
            else:
                alertas.append(('✅', 'Todos os volumes > 0 no CPU', ''))
        else:
            alertas.append(('⚠️', 'CPU não disponível para análise de volume', ''))

        # 4. NaN nos novos parquets
        for nome, data in [
            ('Percentual Rateio', df_pct_rateio),
            ('Custo Rateado', df_custo_rateado),
            ('Custo FP Veículo', df_custo_fp_veic),
            ('CPU Veículo', df_cpu_veic),
        ]:
            if data is not None:
                n_nan = data.isnull().sum().sum()
                if n_nan > 0:
                    alertas.append(('⚠️', f'{nome}: {n_nan} valores NaN', 'Verificar merge e dados fonte'))
                else:
                    alertas.append(('✅', f'{nome}: sem NaN', ''))

        # 5. Rateio: FP sem Ded = Rateado
        if df_custo_rateado is not None and 'Custo Rateado' in df_custo_rateado.columns:
            diff_rat_check = abs(soma_fp_sem_ded - df_custo_rateado['Custo Rateado'].sum())
            if diff_rat_check > 0.01:
                alertas.append(('❌', f'Rateio: diff FP sem Ded vs Rateado = R$ {diff_rat_check:,.2f}', ''))
            else:
                alertas.append(('✅', 'Rateio: FP sem Ded ≈ Custo Rateado', f'Diff = R$ {diff_rat_check:,.6f}'))

        # Exibir alertas
        for icone, titulo, detalhe in alertas:
            if icone == '✅':
                st.success(f"{icone} **{titulo}** — {detalhe}")
            elif icone == '⚠️':
                st.warning(f"{icone} **{titulo}** — {detalhe}")
            else:
                st.error(f"{icone} **{titulo}** — {detalhe}")

    st.divider()
    st.caption(f"TC — Planta Principal | Debug | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
