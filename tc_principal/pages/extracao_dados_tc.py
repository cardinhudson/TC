"""
TC Principal — Extração e Processamento de Dados
Validação de arquivos Excel, upload, processamento e status dos parquets.
"""

import streamlit as st
import pandas as pd
import os
import sys
import re
import json
import unicodedata
from datetime import datetime

from tc_principal.ui_components import injetar_css_global, render_header
# from tc_principal.shared import obter_timestamp_parquets  # futuro

# ── Processamento ──
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from processamento_dados_veiculos_BUD import processar_veiculos_budget
except ImportError:
    processar_veiculos_budget = None


# ════════════════════════════════════════════
# Funções de validação
# ════════════════════════════════════════════

def _encontrar_arquivo(ano: int, nome_arquivo: str, incluir_bud: bool = False):
    candidatos = [
        os.path.join('dados', str(ano), nome_arquivo),
        os.path.join('.', nome_arquivo),
    ]
    if incluir_bud:
        candidatos.insert(1, os.path.join('dados', str(ano), 'BUD', nome_arquivo))
    for c in candidatos:
        if os.path.exists(c):
            return c
    return None


def _validar_abas_excel(caminho: str, abas_obrigatorias: list, contexto: str):
    msgs = []
    try:
        xl = pd.ExcelFile(caminho)
        abas = xl.sheet_names
    except Exception as e:
        return False, [f"❌ Não foi possível abrir o Excel ({contexto}): {e}"]

    faltando = [a for a in abas_obrigatorias if a not in abas]
    if faltando:
        msgs.append(f"❌ Abas faltando em {contexto}: {faltando}")
        msgs.append(f"   Abas disponíveis: {abas}")
        return False, msgs

    msgs.append(f"✅ Abas OK em {contexto}: {abas_obrigatorias}")
    return True, msgs


def _normalizar_col(v) -> str:
    s = str(v).lower().strip()
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    return re.sub(r'[^a-z0-9]', '', s)


def _extrair_colunas_rateio(caminho: str, sheet_name: str):
    df_raw = pd.read_excel(caminho, sheet_name=sheet_name, header=None)
    df = df_raw.iloc[1:].reset_index(drop=True)
    if df.empty:
        return [], []
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(axis=1, how='all')
    colunas = [str(c) for c in df.columns if pd.notna(c)]
    pref_meses = {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}
    colunas_meses = [c for c in colunas if _normalizar_col(c)[:3] in pref_meses]
    return colunas, colunas_meses


def _ler_volume_para_validacao(caminho: str, sheet_name: str):
    for h in [50, 0, 1, 2]:
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=h, nrows=5)
            return df, f"header={h}"
        except Exception:
            continue
    return None, None


def _validar_pre_extracao_budget(ano: int):
    msgs = []
    ok = True

    caminho_rateio = _encontrar_arquivo(ano, 'Reporting veículos.xlsx')
    if not caminho_rateio:
        return False, ["❌ 'Reporting veículos.xlsx' não encontrado."]

    # Abas esperadas para Budget Veículos (MP)
    abas = ['massa primária - BDG', 'Rateio BDG', 'Volume BDG']
    ok_abas, m = _validar_abas_excel(caminho_rateio, abas, 'Reporting veículos.xlsx')
    msgs.extend(m)
    ok &= ok_abas

    if ok_abas:
        # massa primária - BDG
        try:
            df = pd.read_excel(caminho_rateio, sheet_name='massa primária - BDG', nrows=5)
            cols = {str(c) for c in df.columns}
            obrig = {'Oficina', 'Account'}
            faltando = obrig - cols
            if faltando:
                ok = False
                msgs.append(f"❌ Aba 'massa primária - BDG': colunas faltando: {sorted(faltando)}")
            else:
                msgs.append("✅ Aba 'massa primária - BDG': colunas mínimas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'massa primária - BDG': {e}")

        # Rateio BDG
        try:
            colunas, colunas_meses = _extrair_colunas_rateio(caminho_rateio, 'Rateio BDG')
            norm = {re.sub(r'\s+', '', c.lower()): c for c in colunas}
            if 'oficina' not in norm:
                ok = False
                msgs.append("❌ Aba 'Rateio BDG': coluna 'Oficina' não encontrada")
            if not colunas_meses:
                ok = False
                msgs.append("❌ Aba 'Rateio BDG': nenhuma coluna de mês detectada")
            else:
                msgs.append(f"✅ Aba 'Rateio BDG': {len(colunas_meses)} meses detectados")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Rateio BDG': {e}")

        # Volume BDG
        try:
            dfv, info = _ler_volume_para_validacao(caminho_rateio, 'Volume BDG')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume BDG'")
            else:
                cn = [_normalizar_col(c) for c in dfv.columns]
                pref = [c[:3] for c in cn if c]
                meses = [p for p in pref if p in {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}]
                if 'oficina' not in cn:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': coluna 'Oficina' não encontrada ({info})")
                if not meses:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': nenhum mês detectado ({info})")
                else:
                    msgs.append(f"✅ Aba 'Volume BDG': {len(set(meses))} meses detectados ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume BDG': {e}")

    return ok, msgs


# ════════════════════════════════════════════
# Rateios manuais (QY / GS / SM)
# ════════════════════════════════════════════

RATEIOS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'rateios_manuais.json')


def _carregar_rateios():
    if os.path.exists(RATEIOS_PATH):
        with open(RATEIOS_PATH, 'r') as f:
            return json.load(f)
    return {"QY": 0.087526, "GS": 0.086982, "SM": 0.075452}


def _salvar_rateios(rateios: dict):
    with open(RATEIOS_PATH, 'w') as f:
        json.dump(rateios, f, indent=2)


# ════════════════════════════════════════════
# Render principal
# ════════════════════════════════════════════

def render():
    injetar_css_global()
    render_header()

    st.title("📥 Extração e Processamento de Dados")
    st.subheader("TC Planta Principal (Veículos Budget)")

    ano_padrao = datetime.now().year
    ano_selecionado = st.sidebar.number_input("📅 Ano", min_value=2020, max_value=2100,
                                               value=ano_padrao, step=1, key='ext_ano')

    st.sidebar.info("""
    **📋 Instruções:**
    1. Verifique os arquivos necessários
    2. Ajuste rateios manuais se necessário
    3. Execute o processamento
    4. Confira o status dos parquets
    """)

    # ── Tabs ──
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Validação", "⚙️ Processamento",
        "📐 Rateios Manuais", "📊 Status Parquets",
    ])

    # ── TAB 1: Validação ──
    with tab1:
        st.subheader("📋 Validação de Arquivos")

        col_u1, col_u2 = st.columns(2)
        with col_u1:
            st.markdown("**Upload: Reporting veículos.xlsx**")
            uploaded = st.file_uploader("Selecionar arquivo", type=["xlsx"],
                                        key='upload_reporting_tc')
            if uploaded:
                pasta = f"dados/{ano_selecionado}"
                os.makedirs(pasta, exist_ok=True)
                dest = os.path.join(pasta, "Reporting veículos.xlsx")
                with open(dest, 'wb') as f:
                    f.write(uploaded.getbuffer())
                st.success(f"✅ Salvo em `{dest}`")
                st.rerun()

        with col_u2:
            st.markdown("**Arquivo esperado em:**")
            caminho = f"dados/{ano_selecionado}/Reporting veículos.xlsx"
            if os.path.exists(caminho):
                tam = os.path.getsize(caminho) / (1024 * 1024)
                dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                st.success(f"✅ `{caminho}` ({tam:.1f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
            else:
                st.warning(f"⚠️ `{caminho}` não encontrado")

        st.divider()

        if st.button("🔎 Executar Pré-validação", type="secondary", use_container_width=True):
            ok, msgs = _validar_pre_extracao_budget(int(ano_selecionado))
            with st.expander("📋 Relatório de Pré-validação", expanded=True):
                st.code("\n".join(msgs), language="text")
            if ok:
                st.success("✅ Pré-validação OK — pode executar o processamento.")
            else:
                st.error("❌ Corrija os itens acima antes de executar.")

    # ── TAB 2: Processamento ──
    with tab2:
        st.subheader("⚙️ Executar Processamento")
        st.info("O processamento lê o Excel, aplica rateios e gera os 11 parquets para o TC Principal (6 básicos + 5 de cálculo por veículo).")

        if processar_veiculos_budget is None:
            st.error("❌ Módulo `processamento_dados_veiculos_BUD` não encontrado.")
        else:
            if st.button("🚀 Processar Veículos Budget", type="primary", use_container_width=True):
                with st.spinner("🔄 Processando..."):
                    try:
                        resultado = processar_veiculos_budget(ano=int(ano_selecionado))
                        st.success("✅ Processamento concluído!")
                        st.json(resultado)
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")
                        st.exception(e)

    # ── TAB 3: Rateios Manuais ──
    with tab3:
        st.subheader("📐 Rateios Manuais (Oficinas QY / GS / SM)")
        st.caption("Esses fatores são usados no cálculo do Custo FA para oficinas sem taxa PDR direta.")

        rateios = _carregar_rateios()

        col_r1, col_r2, col_r3 = st.columns(3)
        with col_r1:
            r_qy = st.number_input("QY", value=rateios.get('QY', 0.087526),
                                    format="%.6f", step=0.000001, key='rat_qy')
        with col_r2:
            r_gs = st.number_input("GS", value=rateios.get('GS', 0.086982),
                                    format="%.6f", step=0.000001, key='rat_gs')
        with col_r3:
            r_sm = st.number_input("SM", value=rateios.get('SM', 0.075452),
                                    format="%.6f", step=0.000001, key='rat_sm')

        if st.button("💾 Salvar Rateios", type="primary"):
            novos = {"QY": r_qy, "GS": r_gs, "SM": r_sm}
            _salvar_rateios(novos)
            st.success(f"✅ Rateios salvos em `rateios_manuais.json`")

        st.divider()
        st.markdown("**Valores atuais no arquivo:**")
        st.json(rateios)

    # ── TAB 4: Status Parquets ──
    with tab4:
        st.subheader("📊 Status dos Parquets Gerados")

        pasta_base = os.path.join('dados', 'TC_Principal', str(ano_selecionado), 'BUD')

        parquets_esperados = [
            'df_principal_BUD.parquet',
            'df_vol_veiculos_BUD.parquet',
            'df_vol_veiculos_actual.parquet',
            'df_tempo_veiculos_BUD.parquet',
            'df_dea_dedicado_BUD.parquet',
            'df_volume_fa_BUD.parquet',
            'df_veiculos_fp_sem_da_BUD.parquet',
            'df_veiculos_percentual_rateio_BUD.parquet',
            'df_veiculos_custo_rateado_BUD.parquet',
            'df_veiculos_custo_fp_BUD.parquet',
            'df_veiculos_cpu_BUD.parquet',
        ]

        for arq in parquets_esperados:
            caminho = os.path.join(pasta_base, arq)
            if os.path.exists(caminho):
                tam = os.path.getsize(caminho) / (1024 * 1024)
                dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                try:
                    df_tmp = pd.read_parquet(caminho)
                    linhas = len(df_tmp)
                    colunas = len(df_tmp.columns)
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {linhas:,} linhas × {colunas} cols | {dt_mod:%d/%m/%Y %H:%M}")
                except Exception:
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {dt_mod:%d/%m/%Y %H:%M}")
            else:
                st.warning(f"⚠️ `{arq}` não encontrado")

        st.divider()
        st.markdown("**Pasta do ano:**")
        pasta_ano = os.path.join('dados', str(ano_selecionado))
        if os.path.exists(pasta_ano):
            arquivos = []
            for root, dirs, files in os.walk(pasta_ano):
                for f in files:
                    fp = os.path.join(root, f)
                    rel = os.path.relpath(fp, pasta_ano)
                    tam = os.path.getsize(fp) / (1024 * 1024)
                    arquivos.append(f"  📄 {rel} ({tam:.2f} MB)")
            if arquivos:
                st.code("\n".join(sorted(arquivos)), language="text")
            else:
                st.info("Pasta vazia.")
        else:
            st.warning(f"Pasta `{pasta_ano}` não existe.")

    st.divider()
    st.caption(f"TC — Planta Principal | Extração | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
