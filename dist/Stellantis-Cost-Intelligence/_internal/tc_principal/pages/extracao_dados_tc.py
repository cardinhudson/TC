"""
TC Veículos — Extração e Processamento de Dados
Replica o layout e funcionalidades do TC Ext (3 tabs + radio).
Upload com proteção contra sobrescrita, pré-validação, barra de progresso,
log ao vivo, consolidação histórica multi-ano e status de parquets.
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

# ── Caminho raiz do projeto ──
if hasattr(sys, '_MEIPASS'):
    _ROOT = sys._MEIPASS
else:
    _ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    from processamento_dados_veiculos_BUD import processar_veiculos_budget
except ImportError:
    processar_veiculos_budget = None

try:
    from processamento_dados_veiculos import processar_veiculos_real, executar_conferencias
except ImportError:
    processar_veiculos_real = None
    executar_conferencias = None

# ════════════════════════════════════════════
# CONSTANTES
# ════════════════════════════════════════════

PASTA_TC = os.path.join(_ROOT, 'dados', 'TC_Principal')
RATEIOS_PATH = os.path.join(_ROOT, 'rateios_manuais.json')

PARQUETS_BUDGET = [
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

PARQUETS_REAL = [
    'df_principal.parquet',
    'df_volume_fa.parquet',
    'df_tempo_veiculos.parquet',
    'df_vol_veiculos.parquet',
    'df_dea_dedicado.parquet',
    'df_veiculos_fp_sem_da.parquet',
    'df_veiculos_percentual_rateio.parquet',
    'df_veiculos_custo_rateado.parquet',
    'df_veiculos_custo_fp.parquet',
    'df_veiculos_cpu.parquet',
    'df_comparativo_real_budget.parquet',
]


# ════════════════════════════════════════════
# FUNÇÕES AUXILIARES
# ════════════════════════════════════════════

def _encontrar_arquivo(ano: int, nome_arquivo: str, incluir_bud: bool = False):
    candidatos = [
        os.path.join(_ROOT, 'dados', 'TC_Principal', str(ano), nome_arquivo),
        os.path.join('.', nome_arquivo),
    ]
    if incluir_bud:
        candidatos.insert(1, os.path.join(_ROOT, 'dados', 'TC_Principal', str(ano), 'BUD', nome_arquivo))
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
    """Tenta múltiplos valores de header, validando se as colunas fazem sentido.
    
    Retorna o primeiro DataFrame cujas colunas contenham meses ou 'Veículo'/'Oficina'
    (indicando que o header correto foi encontrado).
    """
    pref_meses = {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}

    def _avaliar_colunas(colunas):
        cn = [_normalizar_col(c) for c in colunas]
        pref = [c[:3] for c in cn if c]
        qtd_meses = sum(1 for p in pref if p in pref_meses)
        tem_dim = 'oficina' in cn or 'veiculo' in cn or 'veculo' in cn
        return tem_dim, qtd_meses

    primeiro_ok = None  # fallback: primeiro que leu sem erro

    # 1) Heurística: encontrar a melhor linha de header na própria planilha
    try:
        amostra = pd.read_excel(caminho, sheet_name=sheet_name, header=None, nrows=80)
        melhor_h = None
        melhor_score = (-1, -1)  # (tem_dim, qtd_meses)

        for i in range(len(amostra.index)):
            linha = amostra.iloc[i].tolist()
            tem_dim, qtd_meses = _avaliar_colunas(linha)
            score = (1 if tem_dim else 0, qtd_meses)
            if score > melhor_score:
                melhor_score = score
                melhor_h = i

        if melhor_h is not None and melhor_score > (0, 0):
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=melhor_h, nrows=5)
            return df, f"header={melhor_h}"
    except Exception:
        pass

    # 2) Fallback: tentativas conhecidas
    for h in [50, 1, 2, 0]:
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=h, nrows=5)
            tem_dim, qtd_meses = _avaliar_colunas(df.columns)
            if tem_dim or qtd_meses > 0:
                return df, f"header={h}"
            if primeiro_ok is None:
                primeiro_ok = (df, f"header={h}")
        except Exception:
            continue

    if primeiro_ok is not None:
        return primeiro_ok
    return None, None


# ════════════════════════════════════════════
# PRÉ-VALIDAÇÃO
# ════════════════════════════════════════════

def _validar_pre_extracao_budget(ano: int):
    """Pré-validação para Budget (abas do Reporting veículos.xlsx)."""
    msgs = []
    ok = True

    caminho = _encontrar_arquivo(ano, 'Reporting veículos.xlsx')
    if not caminho:
        return False, ["❌ 'Reporting veículos.xlsx' não encontrado."]

    abas = [
        'massa primária - BDG', 'massa - REDIS',
        'Volume e EST PdR - BDG', 'Volume BDG', 'Volume Actual',
        'EST veículos - BDG', 'massa - D&A dedicado',
    ]
    ok_abas, m = _validar_abas_excel(caminho, abas, 'Reporting veículos.xlsx')
    msgs.extend(m)
    ok &= ok_abas

    if ok_abas:
        # massa primária - BDG
        try:
            df = pd.read_excel(caminho, sheet_name='massa primária - BDG', nrows=5)
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

        # massa - REDIS
        try:
            df = pd.read_excel(caminho, sheet_name='massa - REDIS', nrows=5)
            cols = {str(c) for c in df.columns}
            if 'Oficina' not in cols:
                ok = False
                msgs.append("❌ Aba 'massa - REDIS': coluna 'Oficina' não encontrada")
            else:
                msgs.append("✅ Aba 'massa - REDIS': colunas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'massa - REDIS': {e}")

        # Volume BDG
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume BDG')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume BDG'")
            else:
                cn = [_normalizar_col(c) for c in dfv.columns]
                pref = [c[:3] for c in cn if c]
                meses = [p for p in pref if p in {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}]
                # Volume BDG em Reporting veículos.xlsx pode não ter 'Oficina'
                # (só tem 'Veículo'). Aceitar qualquer uma das duas.
                tem_oficina = 'oficina' in cn
                tem_veiculo = 'veiculo' in cn or 'veculo' in cn
                # Em alguns layouts, a dimensão vem na 1ª coluna sem nome
                # (ex.: "Unnamed: 0"), e no processamento essa coluna é tratada
                # como 'Veículo'. Aceitar esse cenário na pré-validação.
                dim_generica = [
                    c for c in cn
                    if c and c[:3] not in {'jan', 'fev', 'mar', 'abr', 'mai', 'jun',
                                           'jul', 'ago', 'set', 'out', 'nov', 'dez'}
                    and c not in {'ano', 'total'}
                ]
                tem_dim_implicita = bool(dim_generica)
                if not tem_oficina and not tem_veiculo and not tem_dim_implicita:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': coluna 'Oficina' ou 'Veículo' não encontrada ({info})")
                if not meses:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': nenhum mês detectado ({info})")
                else:
                    dim_label = 'Oficina' if tem_oficina else (
                        'Veículo' if tem_veiculo else 'Dimensão (coluna sem nome)'
                    )
                    msgs.append(f"✅ Aba 'Volume BDG': {len(set(meses))} meses detectados, dimensão '{dim_label}' OK ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume BDG': {e}")

        # Volume Actual
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume Actual')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume Actual'")
            else:
                msgs.append(f"✅ Aba 'Volume Actual': legível ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume Actual': {e}")

    return ok, msgs


def _validar_pre_extracao_real(ano: int):
    """Pré-validação para Real (Sapiens)."""
    msgs = []
    ok = True

    caminho = _encontrar_arquivo(ano, 'Reporting veículos.xlsx')
    if not caminho:
        return False, ["❌ 'Reporting veículos.xlsx' não encontrado."]

    abas = ['Sapiens', 'Volume e EST PdR - Actual', 'Volume Actual', 'EST veículos - Actual']
    ok_abas, m = _validar_abas_excel(caminho, abas, 'Reporting veículos.xlsx')
    msgs.extend(m)
    ok &= ok_abas

    if ok_abas:
        # Sapiens
        try:
            df = pd.read_excel(caminho, sheet_name='Sapiens', header=1, nrows=5)
            cols = {str(c) for c in df.columns}
            obrig = {'Oficina', 'Account'}
            faltando = obrig - cols
            if faltando:
                ok = False
                msgs.append(f"❌ Aba 'Sapiens': colunas faltando: {sorted(faltando)}")
            else:
                msgs.append(f"✅ Aba 'Sapiens': colunas mínimas OK ({len(df.columns)} colunas)")
            if 'Valor' not in cols:
                ok = False
                msgs.append("❌ Aba 'Sapiens': coluna 'Valor' não encontrada")
            else:
                msgs.append("✅ Aba 'Sapiens': coluna 'Valor' presente")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'Sapiens': {e}")

        # Volume e EST PdR - Actual
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume e EST PdR - Actual')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume e EST PdR - Actual'")
            else:
                msgs.append(f"✅ Aba 'Volume e EST PdR - Actual': legível ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume e EST PdR - Actual': {e}")

        # EST veículos - Actual
        try:
            dfe = pd.read_excel(caminho, sheet_name='EST veículos - Actual', header=1, nrows=5)
            msgs.append(f"✅ Aba 'EST veículos - Actual': legível ({len(dfe.columns)} colunas)")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'EST veículos - Actual': {e}")

    # D&A Budget (pré-requisito)
    caminho_dea = os.path.join(PASTA_TC, str(ano), 'BUD', 'df_dea_dedicado_BUD.parquet')
    if os.path.exists(caminho_dea):
        msgs.append("✅ D&A Dedicado Budget encontrado")
    else:
        msgs.append("⚠️ D&A Dedicado Budget não encontrado — precisa processar Budget antes")

    return ok, msgs


# ════════════════════════════════════════════
# CONSOLIDAÇÃO HISTÓRICA
# ════════════════════════════════════════════

def _consolidar_historico_tc_principal():
    """Consolida parquets de todos os anos em histórico multi-ano.

    - Real: df_principal, df_vol_veiculos, df_veiculos_cpu
    - Budget: df_principal_BUD, df_vol_veiculos_BUD, df_veiculos_cpu_BUD
    """
    resultados = []

    # Descobrir anos disponíveis
    anos = []
    if os.path.exists(PASTA_TC):
        for item in os.listdir(PASTA_TC):
            if os.path.isdir(os.path.join(PASTA_TC, item)) and item.isdigit():
                anos.append(int(item))
    anos = sorted(anos)

    if not anos:
        return ["⚠️ Nenhum ano encontrado em dados/TC_Principal/"]

    pasta_hist = os.path.join(PASTA_TC, 'historico_consolidado')
    pasta_hist_bud = os.path.join(pasta_hist, 'BUD')
    os.makedirs(pasta_hist, exist_ok=True)
    os.makedirs(pasta_hist_bud, exist_ok=True)

    def _consolidar(mapa_arquivos: dict, pasta_destino: str, sufixo: str = ''):
        """Consolida uma lista de parquets de vários anos.

        mapa_arquivos: {nome_historico: (nome_fonte, subpasta)}
        """
        for nome_hist, (nome_fonte, subpasta) in mapa_arquivos.items():
            dfs = []
            for a in anos:
                if subpasta:
                    caminho = os.path.join(PASTA_TC, str(a), subpasta, nome_fonte)
                else:
                    caminho = os.path.join(PASTA_TC, str(a), nome_fonte)
                if os.path.exists(caminho):
                    try:
                        df = pd.read_parquet(caminho)
                        if 'Ano' not in df.columns:
                            df['Ano'] = a
                        dfs.append(df)
                    except Exception as e:
                        resultados.append(f"⚠️ Erro ao ler {caminho}: {e}")

            if dfs:
                df_final = pd.concat(dfs, ignore_index=True)
                destino = os.path.join(pasta_destino, nome_hist)
                df_final.to_parquet(destino)
                resultados.append(f"✅ {nome_hist}: {len(dfs)} ano(s) → {len(df_final):,} linhas")
            else:
                resultados.append(f"⚠️ {nome_hist}: nenhum dado encontrado")

    # Real
    _consolidar({
        'df_principal_historico.parquet': ('df_principal.parquet', ''),
        'df_vol_historico.parquet': ('df_vol_veiculos.parquet', ''),
        'df_cpu_historico.parquet': ('df_veiculos_cpu.parquet', ''),
        'df_veiculos_custo_fp_historico.parquet': ('df_veiculos_custo_fp.parquet', ''),
    }, pasta_hist)

    # Budget
    _consolidar({
        'df_principal_historico_BUD.parquet': ('df_principal_BUD.parquet', 'BUD'),
        'df_vol_historico_BUD.parquet': ('df_vol_veiculos_BUD.parquet', 'BUD'),
        'df_cpu_historico_BUD.parquet': ('df_veiculos_cpu_BUD.parquet', 'BUD'),
        'df_veiculos_custo_fp_historico_BUD.parquet': ('df_veiculos_custo_fp_BUD.parquet', 'BUD'),
    }, pasta_hist_bud)

    return resultados


# ════════════════════════════════════════════
# RATEIOS MANUAIS
# ════════════════════════════════════════════

def _carregar_rateios():
    if os.path.exists(RATEIOS_PATH):
        with open(RATEIOS_PATH, 'r') as f:
            return json.load(f)
    return {"QY": 0.087526, "GS": 0.086982, "SM": 0.075452}


def _salvar_rateios(rateios: dict):
    with open(RATEIOS_PATH, 'w') as f:
        json.dump(rateios, f, indent=2)


# ════════════════════════════════════════════
# RENDER PRINCIPAL
# ════════════════════════════════════════════

def render():
    injetar_css_global()
    render_header()

    st.title("📥 Extração e Processamento de Dados")
    st.caption("TC Veículos (Budget + Real)")
    st.markdown("---")

    # ── Controles na página principal ──
    col_cfg1, col_cfg2 = st.columns(2)

    with col_cfg1:
        tipo_extracao = st.radio(
            "📊 Selecione o tipo de extração:",
            ["📊 Dados REAIS", "💰 Dados BUDGET", "🔄 Ambos"],
            horizontal=True,
            key='ext_tipo',
        )

    with col_cfg2:
        ano_padrao = datetime.now().year
        ano_selecionado = st.number_input(
            "📅 Ano para processar:",
            min_value=2020,
            max_value=2100,
            value=ano_padrao,
            step=1,
            key='ext_ano',
        )

    st.markdown("---")

    # ── Sidebar: Instruções + Rateios ──
    st.sidebar.header("ℹ️ Informações")
    st.sidebar.info("""
**📋 Instruções:**
1. Selecione o tipo de extração (Real / Budget / Ambos)
2. Informe o ano
3. Faça upload do Reporting veículos.xlsx se necessário
4. Execute a pré-validação
5. Inicie o processamento
""")

    # Rateios manuais na sidebar (sempre acessível)
    st.sidebar.markdown("---")
    st.sidebar.subheader("📐 Rateios Manuais")
    st.sidebar.caption("QY / GS / SM — usados no cálculo da taxa PDR.")
    rateios = _carregar_rateios()

    r_qy = st.sidebar.number_input("QY", value=rateios.get('QY', 0.087526),
                                    format="%.6f", step=0.000001, key='rat_qy')
    r_gs = st.sidebar.number_input("GS", value=rateios.get('GS', 0.086982),
                                    format="%.6f", step=0.000001, key='rat_gs')
    r_sm = st.sidebar.number_input("SM", value=rateios.get('SM', 0.075452),
                                    format="%.6f", step=0.000001, key='rat_sm')

    if st.sidebar.button("💾 Salvar Rateios", type="primary", use_container_width=True):
        _salvar_rateios({"QY": r_qy, "GS": r_gs, "SM": r_sm})
        st.sidebar.success("✅ Rateios salvos!")

    # ═══════════════════════════════════════
    #  TABS
    # ═══════════════════════════════════════

    tab1, tab2, tab3 = st.tabs([
        "📋 Validação de Arquivos",
        "⚙️ Executar Processamento",
        "📊 Status e Logs",
    ])

    # ─────────────────────────────────────
    #  TAB 1: Validação de Arquivos
    # ─────────────────────────────────────
    with tab1:
        st.header("📋 Validação de Arquivos Necessários")

        # ── Upload unificado ──
        st.markdown("### 📤 Upload de Arquivo")
        st.info(
            f"**💡 Dica:** O arquivo `Reporting veículos.xlsx` deve estar em "
            f"`dados/TC_Principal/{ano_selecionado}/`. Se necessário, faça upload abaixo."
        )

        pasta_ano = os.path.join(_ROOT, 'dados', 'TC_Principal', str(ano_selecionado))
        destino = os.path.join(pasta_ano, "Reporting veículos.xlsx")

        arquivo_upload = st.file_uploader(
            "📄 Upload: Reporting veículos.xlsx",
            type=["xlsx"],
            key="upload_reporting_tc",
            help="Arquivo principal contendo abas Budget e Real.",
        )

        # Se já existe, mostra info
        if os.path.exists(destino):
            tam = os.path.getsize(destino) / (1024 * 1024)
            dt_mod = datetime.fromtimestamp(os.path.getmtime(destino))
            st.warning(f"⚠️ Já existe: `{destino}` ({tam:.1f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
        else:
            st.caption(f"📁 Destino: `{destino}`")

        if arquivo_upload is not None:
            precisa_confirmar = os.path.exists(destino)
            confirmar = True
            if precisa_confirmar:
                confirmar = st.checkbox(
                    "Confirmar sobrescrita do arquivo existente",
                    value=False,
                    key="upload_confirm_overwrite",
                )

            if st.button(
                "💾 Salvar Reporting veículos.xlsx",
                key="btn_salvar_upload",
                use_container_width=False,
                type="primary",
                disabled=precisa_confirmar and not confirmar,
            ):
                os.makedirs(pasta_ano, exist_ok=True)
                with open(destino, "wb") as f:
                    f.write(arquivo_upload.getbuffer())
                st.success(f"✅ Arquivo salvo em: `{destino}`")
                st.rerun()

        st.divider()

        # ── Pré-validação ──
        st.markdown("### 🔎 Pré-validação (recomendado)")
        col_v1, col_v2 = st.columns([1, 3])
        with col_v1:
            btn_prevalidar = st.button(
                "🔎 Pré-validar estrutura do Excel",
                use_container_width=True,
                type="secondary",
            )
        with col_v2:
            st.caption(
                "Checa abas e colunas esperadas antes de executar. "
                "Não grava parquets."
            )

        if btn_prevalidar:
            relatorio = []
            ok_total = True

            if tipo_extracao in ["📊 Dados REAIS", "🔄 Ambos"]:
                ok_r, msgs = _validar_pre_extracao_real(int(ano_selecionado))
                ok_total &= ok_r
                relatorio.append("─── 📊 REAIS ───")
                relatorio.extend(msgs)

            if tipo_extracao in ["💰 Dados BUDGET", "🔄 Ambos"]:
                ok_b, msgs = _validar_pre_extracao_budget(int(ano_selecionado))
                ok_total &= ok_b
                relatorio.append("─── 💰 BUDGET ───")
                relatorio.extend(msgs)

            with st.expander("📋 Relatório de Pré-validação", expanded=True):
                st.code("\n".join(relatorio), language="text")

            if ok_total:
                st.success("✅ Pré-validação OK — pode executar a extração.")
            else:
                st.error("❌ Corrija os itens acima antes de executar.")

    # ─────────────────────────────────────
    #  TAB 2: Executar Processamento
    # ─────────────────────────────────────
    with tab2:
        st.header("⚙️ Executar Processamento")
        st.info("""
**⚠️ Importante:**
- Certifique-se de que todos os arquivos necessários estão presentes
- O processamento pode levar alguns minutos
- Não feche a página durante a execução
        """)

        # Botões de execução
        col_b1, col_b2, col_b3 = st.columns(3)

        executar_reais = False
        executar_budget = False
        executar_ambos = False

        with col_b1:
            if tipo_extracao in ["📊 Dados REAIS", "🔄 Ambos"]:
                executar_reais = st.button(
                    "🚀 Processar Real (Sapiens)",
                    type="primary",
                    use_container_width=True,
                )

        with col_b2:
            if tipo_extracao in ["💰 Dados BUDGET", "🔄 Ambos"]:
                executar_budget = st.button(
                    "🚀 Processar Budget",
                    type="primary",
                    use_container_width=True,
                )

        with col_b3:
            if tipo_extracao == "🔄 Ambos":
                executar_ambos = st.button(
                    "🚀 Executar Ambos",
                    type="primary",
                    use_container_width=True,
                )

        # Container de logs
        log_container = st.container()

        # ── Processamento REAIS ──
        if executar_reais or (executar_ambos and tipo_extracao == "🔄 Ambos"):
            with log_container:
                st.subheader("📊 Processando Dados REAIS...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                log_messages = st.empty()

                mensagens_log = []

                def callback_reais(mensagem):
                    mensagens_log.append(mensagem)
                    log_messages.text("\n".join(mensagens_log[-10:]))

                if processar_veiculos_real is None:
                    st.error("❌ Módulo `processamento_dados_veiculos` não encontrado.")
                else:
                    try:
                        with st.spinner("🔄 Processando dados REAIS..."):
                            resultado = processar_veiculos_real(
                                ano=int(ano_selecionado),
                                progress_callback=callback_reais,
                            )

                            progress_bar.progress(100)
                            status_text.success("✅ Processamento Real concluído!")

                            # Consolidar histórico
                            status_text_hist = st.empty()
                            status_text_hist.info("🔄 Consolidando histórico...")
                            hist_msgs = _consolidar_historico_tc_principal()
                            status_text_hist.success("✅ Histórico consolidado!")

                            with st.expander("📁 Arquivos gerados", expanded=False):
                                if 'arquivos' in resultado:
                                    for nome, caminho_arq in resultado['arquivos'].items():
                                        st.write(f"  ✅ {nome}")
                                st.markdown("**Consolidação:**")
                                for msg in hist_msgs:
                                    st.write(msg)

                            # ══ Conferência Automática Real ══
                            if executar_conferencias is not None:
                                with st.expander("📋 Conferência Automática (Real × Excel)", expanded=True):
                                    try:
                                        df_conf = executar_conferencias(int(ano_selecionado), tipo='real')
                                        # Colorir status
                                        def _color_status(val):
                                            if '✅' in str(val):
                                                return 'background-color: #d4edda'
                                            elif '❌' in str(val):
                                                return 'background-color: #f8d7da'
                                            elif '⚠️' in str(val):
                                                return 'background-color: #fff3cd'
                                            return ''
                                        st.dataframe(
                                            df_conf.style.applymap(_color_status, subset=['Status']),
                                            use_container_width=True,
                                            hide_index=True,
                                        )
                                        n_ok = df_conf['Status'].str.contains('✅').sum()
                                        n_err = df_conf['Status'].str.contains('❌').sum()
                                        n_warn = df_conf['Status'].str.contains('⚠️').sum()
                                        st.caption(f"✅ {n_ok} OK | ⚠️ {n_warn} Atenção | ❌ {n_err} Divergências")
                                    except Exception as e_conf:
                                        st.warning(f"⚠️ Conferência não disponível: {e_conf}")

                    except Exception as e:
                        progress_bar.progress(0)
                        status_text.error(f"❌ Erro: {str(e)}")
                        st.exception(e)

        # ── Processamento BUDGET ──
        if executar_budget or (executar_ambos and tipo_extracao == "🔄 Ambos"):
            with log_container:
                st.subheader("💰 Processando Dados BUDGET...")
                progress_bar_b = st.progress(0)
                status_text_b = st.empty()
                log_messages_b = st.empty()

                mensagens_log_b = []

                def callback_budget(mensagem):
                    mensagens_log_b.append(mensagem)
                    log_messages_b.text("\n".join(mensagens_log_b[-10:]))

                if processar_veiculos_budget is None:
                    st.error("❌ Módulo `processamento_dados_veiculos_BUD` não encontrado.")
                else:
                    try:
                        with st.spinner("🔄 Processando dados BUDGET..."):
                            resultado = processar_veiculos_budget(
                                ano=int(ano_selecionado),
                                progress_callback=callback_budget,
                            )

                            progress_bar_b.progress(100)
                            status_text_b.success("✅ Processamento Budget concluído!")

                            # Consolidar histórico
                            status_text_hist_b = st.empty()
                            status_text_hist_b.info("🔄 Consolidando histórico...")
                            hist_msgs = _consolidar_historico_tc_principal()
                            status_text_hist_b.success("✅ Histórico consolidado!")

                            with st.expander("📁 Arquivos gerados", expanded=False):
                                if 'arquivos' in resultado:
                                    for nome, caminho_arq in resultado['arquivos'].items():
                                        st.write(f"  ✅ {nome}")
                                st.markdown("**Consolidação:**")
                                for msg in hist_msgs:
                                    st.write(msg)

                            # ══ Conferência Automática Budget ══
                            if executar_conferencias is not None:
                                with st.expander("📋 Conferência Automática (Budget × Excel)", expanded=True):
                                    try:
                                        df_conf_b = executar_conferencias(int(ano_selecionado), tipo='budget')
                                        def _color_status_b(val):
                                            if '✅' in str(val):
                                                return 'background-color: #d4edda'
                                            elif '❌' in str(val):
                                                return 'background-color: #f8d7da'
                                            elif '⚠️' in str(val):
                                                return 'background-color: #fff3cd'
                                            return ''
                                        st.dataframe(
                                            df_conf_b.style.applymap(_color_status_b, subset=['Status']),
                                            use_container_width=True,
                                            hide_index=True,
                                        )
                                        n_ok = df_conf_b['Status'].str.contains('✅').sum()
                                        n_err = df_conf_b['Status'].str.contains('❌').sum()
                                        n_warn = df_conf_b['Status'].str.contains('⚠️').sum()
                                        st.caption(f"✅ {n_ok} OK | ⚠️ {n_warn} Atenção | ❌ {n_err} Divergências")
                                    except Exception as e_conf:
                                        st.warning(f"⚠️ Conferência não disponível: {e_conf}")

                    except Exception as e:
                        progress_bar_b.progress(0)
                        status_text_b.error(f"❌ Erro: {str(e)}")
                        st.exception(e)

    # ─────────────────────────────────────
    #  TAB 3: Status e Logs
    # ─────────────────────────────────────
    with tab3:
        st.header("📊 Status e Logs")

        # ── Budget Parquets ──
        st.markdown("### 💰 Parquets Budget")
        pasta_bud = os.path.join(PASTA_TC, str(ano_selecionado), 'BUD')

        for arq in PARQUETS_BUDGET:
            caminho = os.path.join(pasta_bud, arq)
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

        # ── Real Parquets ──
        st.markdown("### 📊 Parquets Real (Sapiens)")
        pasta_real = os.path.join(PASTA_TC, str(ano_selecionado))

        for arq in PARQUETS_REAL:
            caminho = os.path.join(pasta_real, arq)
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

        # ── Histórico Consolidado ──
        st.markdown("### 📚 Histórico Consolidado")

        pasta_hist = os.path.join(PASTA_TC, 'historico_consolidado')
        pasta_hist_bud = os.path.join(pasta_hist, 'BUD')

        hist_real = [
            'df_principal_historico.parquet',
            'df_vol_historico.parquet',
            'df_cpu_historico.parquet',
        ]
        hist_bud = [
            'df_principal_historico_BUD.parquet',
            'df_vol_historico_BUD.parquet',
            'df_cpu_historico_BUD.parquet',
        ]

        if os.path.exists(pasta_hist):
            st.markdown("**Real:**")
            for arq in hist_real:
                caminho = os.path.join(pasta_hist, arq)
                if os.path.exists(caminho):
                    tam = os.path.getsize(caminho) / (1024 * 1024)
                    dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                    st.success(f"  ✅ {arq} ({tam:.2f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
                else:
                    st.warning(f"  ⚠️ {arq} não encontrado")

            if os.path.exists(pasta_hist_bud):
                st.markdown("**Budget:**")
                for arq in hist_bud:
                    caminho = os.path.join(pasta_hist_bud, arq)
                    if os.path.exists(caminho):
                        tam = os.path.getsize(caminho) / (1024 * 1024)
                        dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                        st.success(f"  ✅ {arq} ({tam:.2f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
                    else:
                        st.warning(f"  ⚠️ {arq} não encontrado")
            else:
                st.warning("⚠️ Pasta histórico Budget não existe ainda")
        else:
            st.warning("⚠️ Pasta `dados/TC_Principal/historico_consolidado/` não existe ainda")

        # Botão para forçar re-consolidação
        if st.button("🔄 Re-consolidar Histórico", type="secondary"):
            with st.spinner("Consolidando..."):
                msgs = _consolidar_historico_tc_principal()
            for m in msgs:
                st.write(m)
            st.success("✅ Consolidação concluída!")
            st.rerun()

        st.divider()

        # ── Árvore de pastas ──
        st.markdown("### 📁 Estrutura de Pastas")
        pasta_raiz_ano = os.path.join(_ROOT, 'dados', str(ano_selecionado))
        pasta_tc_ano = os.path.join(PASTA_TC, str(ano_selecionado))

        for label, pasta in [
            (f"dados/{ano_selecionado}/", pasta_raiz_ano),
            (f"dados/TC_Principal/{ano_selecionado}/", pasta_tc_ano),
        ]:
            if os.path.exists(pasta):
                arquivos = []
                for root, dirs, files in os.walk(pasta):
                    for f in files:
                        fp = os.path.join(root, f)
                        rel = os.path.relpath(fp, pasta)
                        tam = os.path.getsize(fp) / (1024 * 1024)
                        arquivos.append(f"  📄 {rel} ({tam:.2f} MB)")
                if arquivos:
                    st.markdown(f"**`{label}`**")
                    st.code("\n".join(sorted(arquivos)), language="text")
                else:
                    st.info(f"`{label}` — pasta vazia.")
            else:
                st.caption(f"`{label}` não existe.")

    # ── Rodapé ──
    st.divider()
    st.caption(f"TC — Veículos | Extração | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()
