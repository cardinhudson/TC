import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import altair as alt
import os
import numpy as np
import unicodedata
import json
import sqlite3
from datetime import datetime
import plotly.graph_objects as go
from versionamento import obter_versao_atual, verificar_mudancas_paginas

# Camada core (refatoraÃ§Ã£o incremental): helpers compartilhados, sem depender de app.py
from tc_core.data.paths import (
    encontrar_arquivo_parquet as _core_encontrar_arquivo_parquet,
    listar_anos_disponiveis as _core_listar_anos_disponiveis,
)
from tc_core.finance.currency import (
    converter_coluna_moeda as _core_converter_coluna_moeda,
    converter_moeda as _core_converter_moeda,
    obter_simbolo_moeda as _core_obter_simbolo_moeda,
)
from tc_core.finance.currency_db import (
    carregar_taxas_banco as _core_carregar_taxas_banco,
    inicializar_banco_taxas as _core_inicializar_banco_taxas,
    salvar_taxas_banco as _core_salvar_taxas_banco,
)

from tc_ext.normalizacao import padronizar_colunas
from tc_ext.metricas_tc_ext import cpu_por_chaves


def _normalizar_texto_sem_acento(valor) -> str:
    if pd.isna(valor):
        return ""
    return (
        unicodedata.normalize('NFKD', str(valor))
        .encode('ascii', 'ignore')
        .decode('ascii')
        .strip()
        .lower()
    )


def _normalizar_rotulo_custo(valor):
    texto = _normalizar_texto_sem_acento(valor)
    if not texto:
        return valor
    if texto.startswith('fix'):
        return 'Fixo'
    if texto.startswith('var'):
        return 'VariÃ¡vel'
    return str(valor).strip()


def _mask_custo_fixo(serie: pd.Series) -> pd.Series:
    return serie.astype(str).map(_normalizar_texto_sem_acento).str.startswith('fix')


def _remover_linhas_sem_valores_para_exibicao(
    df: pd.DataFrame,
    colunas_ignorar: list[str] | None = None,
    eps: float = 0.0001,
) -> pd.DataFrame:
    """Remove linhas 100% nulas/zeradas APENAS para melhorar a visibilidade.

    GovernanÃ§a: sempre esconder linhas sem impacto (nÃ£o muda somatÃ³rios),
    e nunca usar esse filtro para cÃ¡lculos de resumo/totais.
    """
    if df is None or df.empty:
        return df

    colunas_ignorar = colunas_ignorar or []
    colunas_numericas = [
        col
        for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col]) and col not in colunas_ignorar
    ]
    if not colunas_numericas:
        return df

    df_tmp = df[colunas_numericas].fillna(0)
    mask_mantem = df_tmp.abs().sum(axis=1) > eps
    return df.loc[mask_mantem].copy()

# Verificar mudanÃ§as nas pÃ¡ginas e incrementar versÃ£o se necessÃ¡rio
verificar_mudancas_paginas()

# ConfiguraÃ§Ã£o da pÃ¡gina fica no app.py (roteador) para evitar chamadas duplicadas

# FunÃ§Ã£o para obter mÃªs atual em portuguÃªs
def obter_mes_atual():
    """Retorna o mÃªs atual em portuguÃªs"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "MarÃ§o", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# FunÃ§Ã£o para obter data e hora de atualizaÃ§Ã£o dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da Ãºltima atualizaÃ§Ã£o dos arquivos de dados"""
    try:
        # Tentar mÃºltiplos caminhos possÃ­veis (para compatibilidade com diferentes ambientes)
        arquivos_dados = [
            # Caminhos do histÃ³rico consolidado
            os.path.join("dados", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join("dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
            # Caminhos alternativos (pode existir em diferentes estruturas)
            os.path.join("./dados", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("./dados", "historico_consolidado", "df_vol_historico.parquet"),
        ]
        
        # TambÃ©m tentar buscar em pastas de anos recentes
        pasta_dados = "dados"
        if os.path.exists(pasta_dados):
            try:
                anos = [d for d in os.listdir(pasta_dados) if os.path.isdir(os.path.join(pasta_dados, d)) and d.isdigit()]
                if anos:
                    ano_mais_recente = max(anos, key=int)
                    arquivos_dados.extend([
                        os.path.join(pasta_dados, ano_mais_recente, "df_final.parquet"),
                        os.path.join(pasta_dados, ano_mais_recente, "df_vol.parquet"),
                    ])
            except (OSError, ValueError):
                pass
        
        data_atualizacao = None
        for arquivo in arquivos_dados:
            if os.path.exists(arquivo):
                try:
                    data_modificacao = os.path.getmtime(arquivo)
                    if data_modificacao and data_modificacao > 0:
                        if data_atualizacao is None or data_modificacao > data_atualizacao:
                            data_atualizacao = data_modificacao
                except (OSError, ValueError):
                    continue
        
        if data_atualizacao and data_atualizacao > 0:
            try:
                dt = datetime.fromtimestamp(data_atualizacao)
                meses = {
                    1: "Janeiro", 2: "Fevereiro", 3: "MarÃ§o", 4: "Abril",
                    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} Ã s {dt.hour:02d}:{dt.minute:02d}"
            except (ValueError, OSError):
                return None
        return None
    except Exception as e:
        # NÃ£o mascarar erro de governanÃ§a (Volume BUD sem VeÃ­culo)
        if isinstance(e, ValueError) and "ERRO NA EXTRAÃ‡ÃƒO" in str(e):
            raise
        return None


def _get_project_root_tc_ext():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
def get_budget_oficinas_opcoes(ano_selecionado_param):
    """Retorna lista de oficinas existentes no Budget (custos) (histÃ³rico consolidado BUD)."""
    try:
        project_root = _get_project_root_tc_ext()
        caminho_budget = os.path.join(
            project_root,
            "dados",
            "historico_consolidado",
            "BUD",
            "df_final_historico_BUD.parquet",
        )
        if not os.path.exists(caminho_budget):
            return []

        try:
            df = pd.read_parquet(caminho_budget, columns=["Oficina", "Ano"])
        except Exception:
            df = pd.read_parquet(caminho_budget)

        df = padronizar_colunas(df)

        if df is None or df.empty or 'Oficina' not in df.columns:
            return []

        if (
            ano_selecionado_param
            and ano_selecionado_param != "Todos"
            and 'Ano' in df.columns
        ):
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                pass

        return sorted(set(df['Oficina'].dropna().astype(str).unique().tolist()))
    except Exception:
        return []


@st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
def get_budget_volume_oficinas_opcoes(ano_selecionado_param):
    """Retorna lista de oficinas existentes no Budget de Volume (histÃ³rico consolidado BUD)."""
    try:
        project_root = _get_project_root_tc_ext()
        caminho_budget_vol = os.path.join(
            project_root,
            "dados",
            "historico_consolidado",
            "BUD",
            "df_vol_historico_BUD.parquet",
        )
        if not os.path.exists(caminho_budget_vol):
            return []

        try:
            df = pd.read_parquet(caminho_budget_vol, columns=["Oficina", "Ano"])
        except Exception:
            df = pd.read_parquet(caminho_budget_vol)

        df = padronizar_colunas(df)

        if df is None or df.empty or 'Oficina' not in df.columns:
            return []

        if (
            ano_selecionado_param
            and ano_selecionado_param != "Todos"
            and 'Ano' in df.columns
        ):
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                pass

        return sorted(set(df['Oficina'].dropna().astype(str).unique().tolist()))
    except Exception:
        return []

# CabeÃ§alho compacto com data de atualizaÃ§Ã£o
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
data_atualizacao = obter_data_atualizacao_dados()

# Montar textos do cabeÃ§alho
texto_esquerda = f"ðŸ“š DocumentaÃ§Ã£o Completa do Sistema TC | VersÃ£o {versao_atual} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin e Lauro Paiva"
texto_direita = f"ðŸ“… Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

st.markdown(f"""
<div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
    <div style='flex: 1;'>{texto_esquerda}</div>
    <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
</div>
""", unsafe_allow_html=True)


# CSS para reduzir tÃ­tulos em 20% e evitar quebra de linha
st.markdown("""
    <style>
        h1 {
            /* Reduzido de 3rem para 2.4rem (20%) */
            font-size: 2.4rem !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        h2 {
            /* Reduzido de 2rem para 1.6rem (20%) */
            font-size: 1.6rem !important;
        }
        h3 {
            /* Reduzido de 1.6rem para 1.28rem (20%) */
            font-size: 1.28rem !important;
        }
        /* Alinhamento vertical para cÃ©lulas de tabela */
        .stDataFrame table td {
            vertical-align: middle !important;
        }
        .stDataFrame table th {
            vertical-align: middle !important;
        }
        /* Estilos para botÃµes: reduzir fonte e aproximar */
        .stButton > button {
            font-size: 0.85rem !important;
            padding: 0.4rem 1rem !important;
            margin-bottom: 0.3rem !important;
        }
        /* Ajustar tamanho da fonte dos radio buttons no topo (exceto moeda) */
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) label {
            font-size: 0.8rem !important;
            line-height: 1.1 !important;
        }
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) label p {
            font-size: 0.8rem !important;
            margin-bottom: 0 !important;
            line-height: 1.1 !important;
            padding-bottom: 0 !important;
        }
        /* Reduzir espaÃ§amento dos radio buttons horizontais (exceto moeda) */
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) > div {
            gap: 0.25rem !important;
        }
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) > div > label {
            padding: 0.15rem 0.35rem !important;
            margin-bottom: 0 !important;
        }
        /* Reduzir espaÃ§amento entre colunas */
        .stColumn {
            padding-left: 0.2rem !important;
            padding-right: 0.2rem !important;
        }
        /* Eliminar espaÃ§amento nas colunas de moeda - SEM interferir nos cliques */
        div[data-testid="column"]:has(div[data-testid="stRadio"][key="moeda_selecionada_radio"]) {
            padding-left: 0 !important;
            padding-right: 0.05rem !important;
            margin: 0 !important;
            pointer-events: auto !important;
        }
        div[data-testid="column"]:has(#flag-brl):not(:has(#flag-usd)):not(:has(#flag-eur)) {
            padding-left: 0.05rem !important;
            padding-right: 0 !important;
            margin: 0 !important;
            pointer-events: auto !important;
        }
        /* Garantir que os radio buttons fiquem compactos */
        div[data-testid="stRadio"] {
            margin-bottom: 0 !important;
            padding-bottom: 0 !important;
        }
        /* Reduzir margem do label do radio */
        div[data-testid="stRadio"] > label {
            margin-bottom: 0 !important;
            padding-bottom: 0 !important;
        }
        /* Reduzir altura total do container do radio */
        div[data-testid="stRadio"] > div {
            margin-top: 0 !important;
            margin-bottom: 0 !important;
        }
        /* ForÃ§ar altura mÃ­nima do container do radio */
        div[data-testid="stRadio"] {
            min-height: auto !important;
            height: auto !important;
        }
        /* Reduzir espaÃ§amento do tÃ­tulo do radio */
        div[data-testid="stRadio"] > label > div {
            margin-bottom: 0.15rem !important;
            padding-bottom: 0 !important;
        }
        /* Compactar ainda mais os elementos das colunas */
        [data-testid="stColumn"] {
            /* NÃ£o force layout flex nas colunas (isso pode encolher grÃ¡ficos) */
            align-items: stretch !important;
        }
        [data-testid="stColumn"] > div {
            width: 100% !important;
        }
        /* Garantir que os radio buttons nÃ£o quebrem linha */
        div[data-testid="stRadio"] > div[role="radiogroup"] {
            display: flex !important;
            flex-wrap: nowrap !important;
        }
        /* Evitar que palavras sejam cortadas */
        div[data-testid="stRadio"] > div > label {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            word-break: keep-all !important;
        }
        div[data-testid="stRadio"] label p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            word-break: keep-all !important;
        }
        /* Garantir que o container nÃ£o corte o conteÃºdo */
        div[data-testid="stRadio"] {
            overflow: visible !important;
        }
        div[data-testid="stColumn"] {
            overflow: visible !important;
        }
        /* REMOVER qualquer interferÃªncia nos radio buttons de moeda */
        div[data-testid="stRadio"][key="moeda_selecionada_radio"] {
            overflow: visible !important;
            pointer-events: auto !important;
        }
        div[data-testid="stRadio"][key="moeda_selecionada_radio"] * {
            pointer-events: auto !important;
        }
        div[data-testid="stRadio"][key="moeda_selecionada_radio"] input[type="radio"] {
            pointer-events: auto !important;
            cursor: pointer !important;
            z-index: 999 !important;
            position: relative !important;
        }
        div[data-testid="stRadio"][key="moeda_selecionada_radio"] label {
            pointer-events: auto !important;
            cursor: pointer !important;
        }
        /* Ocultar botÃµes de moeda ocultos */
        button[key="btn_brl_hidden"],
        button[key="btn_usd_hidden"],
        button[key="btn_eur_hidden"] {
            display: none !important;
            visibility: hidden !important;
            position: absolute !important;
            left: -9999px !important;
            width: 0 !important;
            height: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            opacity: 0 !important;
            pointer-events: none !important;
        }
        /* Ocultar containers dos botÃµes de moeda */
        div:has(button[key="btn_brl_hidden"]),
        div:has(button[key="btn_usd_hidden"]),
        div:has(button[key="btn_eur_hidden"]) {
            display: none !important;
            visibility: hidden !important;
            height: 0 !important;
            width: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            overflow: hidden !important;
        }
        div[data-testid="stButton"]:has(button[key="btn_brl_hidden"]),
        div[data-testid="stButton"]:has(button[key="btn_usd_hidden"]),
        div[data-testid="stButton"]:has(button[key="btn_eur_hidden"]) {
            display: none !important;
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
        }
        /* Container fixo para as bandeiras no topo direito */
        #flags-container-top {{
            position: fixed !important;
            top: 1rem !important;
            right: 1rem !important;
            z-index: 9999 !important;
            display: flex !important;
            flex-direction: row !important;
            gap: 0.5rem !important;
            align-items: center !important;
            justify-content: center !important;
            background-color: rgba(14, 17, 23, 0.95) !important;
            padding: 0.5rem !important;
            border-radius: 8px !important;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3) !important;
            pointer-events: auto !important;
        }}
        /* Garantir que as bandeiras fiquem em linha horizontal */
        #flags-container-top > div {{
            display: inline-block !important;
            flex-shrink: 0 !important;
        }}
        /* Ocultar qualquer texto que possa aparecer */
        #flags-container-top p,
        #flags-container-top span,
        #flags-container-top::before,
        #flags-container-top::after {{
            display: none !important;
        }}
        /* Ocultar texto do Streamlit ao redor do container */
        div:has(#flags-container-top) p,
        div:has(#flags-container-top) > *:not(#flags-container-top) {{
            display: none !important;
        }}
        /* Garantir que o container pai nÃ£o adicione texto */
        [data-testid="stMarkdownContainer"]:has(#flags-container-top) > *:not(#flags-container-top) {{
            display: none !important;
        }}
        /* Ocultar qualquer cÃ³digo JavaScript que apareÃ§a como texto */
        div:has(#flags-container-top) + *,
        div:has(#flags-container-top) ~ * {{
            display: none !important;
        }}
        /* Ocultar texto JavaScript especÃ­fico */
        *:not(script):not(style) {{
            font-size: inherit !important;
        }}
        /* Ocultar elementos com texto JavaScript - REGRAS MAIS AGRESSIVAS */
        p:contains('}}'),
        span:contains('}}'),
        div:contains('}}'),
        *:not(script):not(style):not(#flags-container-top) {{
            font-size: inherit !important;
        }}
        /* Ocultar qualquer elemento que contenha apenas texto JavaScript */
        body *:not(script):not(style):not(#flags-container-top):not([data-testid]):not(input):not(button):not(select):not(textarea):not(img):not(svg) {{
            font-size: inherit !important;
        }}
        /* Ocultar texto especÃ­fico "})();" - mas NÃƒO interferir nos radio buttons */
        body *:not(script):not(style):not(#flags-container-top):not([data-testid="stRadio"]):not([data-testid="stRadio"] *) {{
            position: relative !important;
        }}
        body *:not(script):not(style):not(#flags-container-top)::before,
        body *:not(script):not(style):not(#flags-container-top)::after {{
            content: none !important;
        }}
        /* REMOVER qualquer interferÃªncia - deixar Streamlit gerenciar normalmente */
        div[data-testid="stRadio"][key="moeda_selecionada_radio"] {
            margin-top: 0 !important;
            margin-bottom: 0 !important;
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }
        /* Padronizar campos de entrada de taxas - reduzir tamanho e evitar quebra */
        div[data-testid="stNumberInput"] label {
            font-size: 0.7rem !important;
            white-space: nowrap !important;
            word-break: keep-all !important;
            overflow: visible !important;
            max-width: none !important;
            width: auto !important;
        }
        div[data-testid="stNumberInput"] label p {
            font-size: 0.7rem !important;
            white-space: nowrap !important;
            word-break: keep-all !important;
            margin: 0 !important;
            padding: 0 !important;
            line-height: 1.2 !important;
            max-width: none !important;
            width: auto !important;
            overflow: visible !important;
            display: inline-block !important;
        }
        /* Garantir que R$ nÃ£o seja quebrado - forÃ§ar renderizaÃ§Ã£o completa */
        div[data-testid="stNumberInput"] label p {
            letter-spacing: 0 !important;
            word-spacing: normal !important;
        }
        /* CSS especÃ­fico para garantir que o $ nÃ£o seja cortado */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] label,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] label {
            font-size: 0.7rem !important;
            white-space: nowrap !important;
            word-break: keep-all !important;
            overflow: visible !important;
            width: auto !important;
            max-width: none !important;
        }
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] label p,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] label p {
            font-size: 0.7rem !important;
            white-space: nowrap !important;
            word-break: keep-all !important;
            overflow: visible !important;
            text-overflow: clip !important;
            width: auto !important;
            min-width: fit-content !important;
            max-width: none !important;
        }
        /* Garantir que o container nÃ£o corte o texto */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"],
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] {
            overflow: visible !important;
        }
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] > div,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] > div {
            overflow: visible !important;
        }
        /* ForÃ§ar que o label completo seja visÃ­vel */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] > div > label,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] > div > label {
            overflow: visible !important;
            width: auto !important;
            max-width: none !important;
        }
        /* Garantir que nenhum elemento corte o texto R$ */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] *,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] * {
            overflow: visible !important;
        }
        /* EspecÃ­fico para garantir que o parÃ¡grafo com R$ seja completo */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] label p:contains("R$"),
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] label p:contains("R$") {
            white-space: nowrap !important;
            word-break: keep-all !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }
        /* Padronizar tamanho de texto dos radio buttons de Tipo e Fator */
        div[data-testid="stRadio"][key="tipo_visualizacao_top"] label,
        div[data-testid="stRadio"][key="fator_conversao_top"] label {
            font-size: 0.7rem !important;
        }
        div[data-testid="stRadio"][key="tipo_visualizacao_top"] label p,
        div[data-testid="stRadio"][key="fator_conversao_top"] label p {
            font-size: 0.7rem !important;
            line-height: 1.2 !important;
        }
""", unsafe_allow_html=True)

# Verificar se estamos na pÃ¡gina principal (app.py) e nÃ£o em uma pÃ¡gina separada
# IMPORTANTE: Verificar ANTES de renderizar qualquer conteÃºdo do dashboard
is_main_page = True
try:
    import os
    # Verificar pelo nome do arquivo diretamente (mais confiÃ¡vel)
    current_file_name = os.path.basename(__file__)
    if current_file_name in ('app.py', 'home_ext.py'):
        is_main_page = True
    else:
        # Verificar pelo caminho completo
        current_file = os.path.abspath(__file__)
        # Verificar se o arquivo atual estÃ¡ na pasta pages
        if current_file and ('pages' in current_file.replace('\\', '/') or 'pages/' in current_file.replace('\\', '/')):
            is_main_page = False
        # Verificar se hÃ¡ flag no session_state indicando pÃ¡gina separada (ex: Waterfall)
        if 'is_waterfall_page' in st.session_state and st.session_state.is_waterfall_page:
            is_main_page = False
except Exception as e:
    # Em caso de erro, assumir que estamos na pÃ¡gina principal
    is_main_page = True

# VerificaÃ§Ã£o adicional: garantir que no app.py sempre seja True
try:
    import os
    current_file_name = os.path.basename(__file__)
    if current_file_name in ('app.py', 'home_ext.py'):
        is_main_page = True
    # Se nÃ£o estamos em pages, forÃ§ar is_main_page = True
    elif not is_main_page:
        current_file_check = os.path.abspath(__file__)
        if current_file_check and 'pages' not in current_file_check.replace('\\', '/'):
            is_main_page = True
except:
    # Em caso de erro, assumir pÃ¡gina principal
    is_main_page = True

if is_main_page:
    # TÃ­tulo - Movido para o topo da pÃ¡gina
    st.title("ðŸ­ Dashboard TC Estendido Porto Real")
    st.subheader("AnÃ¡lise de dados agrupados por Oficina e PerÃ­odo")

    st.markdown("---")

    # Inicializar estado se nÃ£o existir
    if 'moeda_selecionada' not in st.session_state:
        st.session_state.moeda_selecionada = "ï¿½ðŸ‡º â‚¬"
    # Inicializar moeda_selecionada_radio tambÃ©m para evitar erro no callback
    if 'moeda_selecionada_radio' not in st.session_state:
        st.session_state.moeda_selecionada_radio = "ðŸ‡ªðŸ‡º â‚¬"

    # URLs das bandeiras
    bandeira_brasil_url = "https://flagcdn.com/br.svg"
    bandeira_eua_url = "https://flagcdn.com/us.svg"
    bandeira_europa_url = "https://flagcdn.com/eu.svg"

    # SeleÃ§Ã£o de moeda com bandeiras ao lado (sem botÃµes, apenas visual)
    col_moeda1, col_moeda2 = st.columns([3, 1])

    with col_moeda1:
        st.markdown("ðŸ’± **Moeda:**", unsafe_allow_html=True)
        opcoes_moeda = ["ðŸ‡§ðŸ‡· R$", "ðŸ‡ºðŸ‡¸ $", "ðŸ‡ªðŸ‡º â‚¬"]
        
        # SEMPRE usar o valor mais atual do session_state para calcular o Ã­ndice
        moeda_atual_para_index = st.session_state.get('moeda_selecionada', 'ï¿½ðŸ‡º â‚¬')
        index_moeda = opcoes_moeda.index(moeda_atual_para_index) if moeda_atual_para_index in opcoes_moeda else 0
        
        # FunÃ§Ã£o callback para garantir sincronizaÃ§Ã£o imediata
        def atualizar_moeda():
            # O valor jÃ¡ estÃ¡ em st.session_state.moeda_selecionada_radio apÃ³s o clique
            # Verificar se a chave existe antes de acessar
            if 'moeda_selecionada_radio' in st.session_state:
                st.session_state.moeda_selecionada = st.session_state.moeda_selecionada_radio
        
        moeda_selecionada = st.radio(
            "Moeda",
            opcoes_moeda,
            index=index_moeda,
            horizontal=True,
            help="Selecione a moeda para exibiÃ§Ã£o nos grÃ¡ficos",
            key="moeda_selecionada_radio",
            label_visibility="collapsed",
            on_change=atualizar_moeda
        )
        
        # Garantir que o estado esteja sincronizado (backup caso on_change nÃ£o funcione)
        if st.session_state.moeda_selecionada != moeda_selecionada:
            st.session_state.moeda_selecionada = moeda_selecionada

    # Obter moeda atual do session_state (sempre atualizado)
    moeda_atual = st.session_state.get('moeda_selecionada', 'ï¿½ðŸ‡º â‚¬')
    flag_selecionada_brl = moeda_atual == 'ðŸ‡§ðŸ‡· R$'
    flag_selecionada_usd = moeda_atual == 'ðŸ‡ºðŸ‡¸ $'
    flag_selecionada_eur = moeda_atual == 'ðŸ‡ªðŸ‡º â‚¬'

    with col_moeda2:
        st.markdown("<br>", unsafe_allow_html=True)  # EspaÃ§amento vertical
        st.markdown(f"""
        <div style="display: flex; flex-direction: row; gap: 0.5rem; align-items: center; margin-top: 0.5rem; justify-content: center;">
            <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_brl else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_brl else 'transparent'};">
                <img src="{bandeira_brasil_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_brl else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_brl else 'none'};">
            </div>
            <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_usd else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_usd else 'transparent'};">
                <img src="{bandeira_eua_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_usd else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_usd else 'none'};">
            </div>
            <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_eur else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_eur else 'transparent'};">
                <img src="{bandeira_europa_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_eur else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_eur else 'none'};">
            </div>
        </div>
        """, unsafe_allow_html=True)

# FunÃ§Ãµes de banco de dados SQLite (definir ANTES de usar - disponÃ­veis para todas as pÃ¡ginas)
def inicializar_banco_taxas():
    """Cria o banco de dados e tabela para taxas de cÃ¢mbio se nÃ£o existir."""
    _core_inicializar_banco_taxas()

def carregar_taxas_banco():
    """Carrega as taxas de cÃ¢mbio do banco de dados SQLite."""
    return _core_carregar_taxas_banco()

def salvar_taxas_banco(taxas):
    """Salva as taxas de cÃ¢mbio no banco de dados SQLite."""
    _core_salvar_taxas_banco(taxas)

# FunÃ§Ã£o auxiliar para listar anos disponÃ­veis (definir ANTES de usar)
def listar_anos_disponiveis():
    """Lista todos os anos disponÃ­veis nas pastas de dados."""
    return _core_listar_anos_disponiveis()

# BotÃµes para alternar tema (no topo da sidebar)
st.sidebar.markdown("---")
st.sidebar.markdown("**ðŸŽ¨ Tema**")

# FunÃ§Ã£o para salvar tema no config.toml
def save_theme_to_config(theme_name):
    """Salva o tema no config.toml"""
    import os
    import toml
    
    config_path = os.path.join(".streamlit", "config.toml")
    try:
        # Ler configuraÃ§Ã£o atual
        with open(config_path, 'r') as f:
            config = toml.load(f)
        
        # Atualizar tema
        if 'theme' not in config:
            config['theme'] = {}
        config['theme']['base'] = theme_name
        
        # Cores especÃ­ficas para cada tema
        if theme_name == 'dark':
            config['theme']['primaryColor'] = "#FF4B4B"
            config['theme']['backgroundColor'] = "#0E1117"
            config['theme']['secondaryBackgroundColor'] = "#262730"
            config['theme']['textColor'] = "#FAFAFA"
            config['theme']['font'] = "sans serif"
        else:  # light
            config['theme']['primaryColor'] = "#FF4B4B"
            config['theme']['backgroundColor'] = "#FFFFFF"
            config['theme']['secondaryBackgroundColor'] = "#F0F2F6"
            config['theme']['textColor'] = "#262730"
            config['theme']['font'] = "sans serif"
        
        # Salvar configuraÃ§Ã£o
        with open(config_path, 'w') as f:
            toml.dump(config, f)
        
        return True
    except Exception as e:
        st.sidebar.error(f"âŒ Erro: {str(e)}")
        return False

# FunÃ§Ã£o para ler tema atual
def get_current_theme():
    import os
    import toml
    config_path = os.path.join(".streamlit", "config.toml")
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        return config.get('theme', {}).get('base', 'light')
    except:
        return 'light'

# Ler tema atual (uma vez por sessÃ£o)
if 'current_saved_theme' not in st.session_state:
    st.session_state.current_saved_theme = get_current_theme()

# Mostrar tema atual
st.sidebar.caption(f"Tema ativo: **{st.session_state.current_saved_theme.upper()}**")

# Criar duas colunas para os botÃµes
col_dark, col_light = st.sidebar.columns(2)

# Inicializar flag de mensagem
if 'show_reload_message' not in st.session_state:
    st.session_state.show_reload_message = False
if 'theme_needs_reload' not in st.session_state:
    st.session_state.theme_needs_reload = False

# BotÃ£o Dark Mode (Lua)
with col_dark:
    if st.button("ðŸŒ™ Dark", key="btn_dark", help="Ativar Dark Mode", width="stretch"):
        if save_theme_to_config('dark'):
            st.session_state.current_saved_theme = 'dark'
            st.session_state.show_reload_message = True
            st.session_state.theme_needs_reload = True

# BotÃ£o Light Mode (Sol)
with col_light:
    if st.button("â˜€ï¸ Light", key="btn_light", help="Ativar Light Mode", width="stretch"):
        if save_theme_to_config('light'):
            st.session_state.current_saved_theme = 'light'
            st.session_state.show_reload_message = True
            st.session_state.theme_needs_reload = True

# Mostrar mensagem se tema foi alterado
if st.session_state.show_reload_message:
    st.sidebar.success(f"âœ… Tema **{st.session_state.current_saved_theme.upper()}** salvo!")
    st.sidebar.info("ðŸ”„ Aplicando tema...")
    if st.session_state.theme_needs_reload:
        st.session_state.theme_needs_reload = False
        st.session_state.show_reload_message = False
        components.html(
            """
            <script>
            setTimeout(function () {
                const target = window.top || window.parent || window;
                target.location.reload();
            }, 100);
            </script>
            """,
            height=0,
            width=0,
        )


st.sidebar.markdown("---")
st.sidebar.markdown("**ðŸ“… SeleÃ§Ã£o de Ano**")

# Listar anos disponÃ­veis
anos_disponiveis = listar_anos_disponiveis()
opcoes_ano = ["Todos"] + [str(ano) for ano in anos_disponiveis]

# Determinar Ã­ndice padrÃ£o: ano atual se disponÃ­vel, senÃ£o "Todos" (Ã­ndice 0)
from datetime import datetime
ano_atual = datetime.now().year
ano_atual_str = str(ano_atual)
if ano_atual_str in opcoes_ano:
    index_padrao = opcoes_ano.index(ano_atual_str)
else:
    index_padrao = 0  # "Todos" se ano atual nÃ£o estiver disponÃ­vel

# Inicializar session_state para manter valores dos filtros
if 'filtro_ano_tc_ext' not in st.session_state:
    st.session_state.filtro_ano_tc_ext = opcoes_ano[index_padrao] if index_padrao < len(opcoes_ano) else "Todos"

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=opcoes_ano.index(st.session_state.filtro_ano_tc_ext) if st.session_state.filtro_ano_tc_ext in opcoes_ano else index_padrao,
    help="Selecione 'Todos' para ver dados consolidados ou um ano especÃ­fico",
    key="filtro_ano_tc_ext_selectbox"
)
# Atualizar session_state
st.session_state.filtro_ano_tc_ext = ano_selecionado

# FunÃ§Ã£o para carregar dados com cache (disponÃ­vel para todas as pÃ¡ginas - deve estar antes do uso)
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet - SEMPRE do histÃ³rico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histÃ³rico consolidado para garantir consistÃªncia
        # Apenas aplicar filtro de ano quando necessÃ¡rio
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
        caminho_absoluto = os.path.abspath(caminho_historico)
        
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
            df = padronizar_colunas(df)
        else:
            st.error(f"âŒ Arquivo de histÃ³rico consolidado nÃ£o encontrado: {caminho_absoluto}")
            st.info("ðŸ’¡ Execute o dados.ipynb para gerar o histÃ³rico consolidado")
            st.stop()
            return None

        # Se um ano especÃ­fico foi selecionado, filtrar apÃ³s carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histÃ³rico consolidado)
        # e apenas filtramos pelo ano, mantendo consistÃªncia
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se nÃ£o conseguir converter para int, nÃ£o filtrar por ano
                pass

        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos para formato capitalizado (primeira letra maiÃºscula)
        # Isso garante consistÃªncia com o resto do cÃ³digo que espera perÃ­odos capitalizados
        if 'PerÃ­odo' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza perÃ­odo para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
            
            df['PerÃ­odo'] = df['PerÃ­odo'].apply(normalizar_periodo)

        # Converter colunas numÃ©ricas conhecidas para numÃ©rico ANTES da otimizaÃ§Ã£o
        # Isso evita que sejam convertidas para categorical
        colunas_numericas = ['Valor', 'Total', 'Volume', 'CPU']
        for col in colunas_numericas:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')

        # Converter floats para tipos menores
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Converter ints para tipos menores
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    except Exception as e:
        st.error(f"âŒ Erro ao carregar dados: {str(e)}")
        st.stop()

# FunÃ§Ã£o auxiliar para obter opÃ§Ãµes de filtro (disponÃ­vel para todas as pÃ¡ginas - deve estar antes do uso)
@st.cache_data(ttl=1800, max_entries=5)
def get_filter_options(df, column_name):
    """ObtÃ©m opÃ§Ãµes de filtro com cache"""
    if column_name in df.columns:
        opcoes = sorted(
            df[column_name].dropna().astype(str).unique().tolist()
        )
        return ["Todos"] + opcoes
    return ["Todos"]

# Continuar apenas se estivermos na pÃ¡gina principal
if is_main_page:
    # Carregar taxas do banco de dados para usar na pÃ¡gina principal
    try:
        taxas_cambio_banco = carregar_taxas_banco()
    except Exception as e:
        taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

    # Taxas de conversÃ£o: entrada em "1 $ = R$ X" e "1 â‚¬ = R$ X"
    taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
    taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

    # SeÃ§Ã£o de Taxas de CÃ¢mbio (seguindo o mesmo padrÃ£o dos outros blocos)
    st.markdown("ðŸ“ **Entrada de Taxas:**", unsafe_allow_html=True)

    # Criar colunas para as taxas
    # Criar colunas para as taxas (ajustar proporÃ§Ã£o para evitar corte de texto)
    col_taxa1, col_taxa2 = st.columns([1.1, 1.1], gap="small")

    with col_taxa1:
        # Usar markdown para o label e campo sem label para evitar corte
        st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">ðŸ‡ºðŸ‡¸ 1 $ (USD) = R$</p>', unsafe_allow_html=True)
        taxa_usd_para_brl = st.number_input(
            "Taxa USD para BRL",
            min_value=0.001,
            max_value=100.0,
            value=float(taxa_usd_para_brl_padrao),
            step=0.001,
            format="%.3f",
            help="Digite quanto vale 1 DÃ³lar Americano em Reais Brasileiros. Exemplo: se 1 USD = 5.00 BRL, digite 5.00",
            key="taxa_usd_para_brl_input",
            label_visibility="collapsed"
        )

    with col_taxa2:
        # Usar markdown para o label e campo sem label para evitar corte
        st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">ðŸ‡ªðŸ‡º 1 â‚¬ (EUR) = R$</p>', unsafe_allow_html=True)
        taxa_eur_para_brl = st.number_input(
            "Taxa EUR para BRL",
            min_value=0.001,
            max_value=100.0,
            value=float(taxa_eur_para_brl_padrao),
            step=0.001,
            format="%.3f",
            help="Digite quanto vale 1 Euro em Reais Brasileiros. Exemplo: se 1 EUR = 5.50 BRL, digite 5.50",
            key="taxa_eur_para_brl_input",
            label_visibility="collapsed"
        )

    # Calcular taxas inversas para conversÃ£o (1 R$ = X USD/EUR)
    taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
    taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

    # Salvar taxas quando alteradas
    # Usar session_state para evitar salvar mÃºltiplas vezes na mesma execuÃ§Ã£o
    taxa_usd_atual_key = "taxa_usd_atual_salva"
    taxa_eur_atual_key = "taxa_eur_atual_salva"

    # Verificar se as taxas mudaram desde a Ãºltima vez que foram salvas
    taxa_usd_mudou = (taxa_usd_atual_key not in st.session_state or 
                      st.session_state.get(taxa_usd_atual_key) != taxa_usd_para_brl)
    taxa_eur_mudou = (taxa_eur_atual_key not in st.session_state or 
                      st.session_state.get(taxa_eur_atual_key) != taxa_eur_para_brl)

    if taxa_usd_mudou or taxa_eur_mudou:
        novas_taxas = {
            "USD": float(taxa_usd_para_brl),
            "EUR": float(taxa_eur_para_brl)
        }
        try:
            salvar_taxas_banco(novas_taxas)
            st.session_state[taxa_usd_atual_key] = taxa_usd_para_brl
            st.session_state[taxa_eur_atual_key] = taxa_eur_para_brl
        except Exception as e:
            st.error(f"âŒ Erro ao salvar taxas: {e}")

    # Armazenar taxas em dicionÃ¡rio (para conversÃ£o: 1 R$ = X USD/EUR)
    # IMPORTANTE: Estas taxas sÃ£o para MULTIPLICAR valores em BRL
    # Exemplo: Se taxa_brl_para_usd = 0.20, entÃ£o 100 BRL * 0.20 = 20 USD
    # Isso Ã© equivalente a: 100 BRL / 5 = 20 USD (onde 5 Ã© taxa_usd_para_brl)
    taxas_cambio = {
        "BRL": 1.0,  # Real Ã© a moeda base
        "USD": taxa_brl_para_usd,  # Ex: 0.20 (se 1 USD = 5 BRL, entÃ£o 1 BRL = 0.20 USD)
        "EUR": taxa_brl_para_eur   # Ex: 0.18 (se 1 EUR = 5.50 BRL, entÃ£o 1 BRL = 0.18 EUR)
    }

    # Seletores no topo da pÃ¡gina (layout horizontal compacto - mesma linha)
    col_tipo, col_fator = st.columns([1.3, 1.2], gap="small")

    with col_tipo:
        tipo_visualizacao = st.radio(
            "ðŸ“Š **Tipo:**",
            ["Custo Total", "CPU (Custo por Unidade)"],
            index=1,  # PadrÃ£o: CPU (Custo por Unidade)
            horizontal=True,
            key="tipo_visualizacao_top"
        )

    with col_fator:
        if tipo_visualizacao == "Custo Total":
            fator_conversao = st.radio(
                "ðŸ”¢ **Fator:**",
                ["Nenhum", "K (milhares)", "M (MilhÃµes)"],
                index=1,
                horizontal=True,
                help="Aplica divisÃ£o aos valores para simplificar visualizaÃ§Ã£o. NÃ£o afeta cÃ¡lculos.",
                key="fator_conversao_top"
            )
        else:
            fator_conversao = None

    # Obter a moeda selecionada do session state (jÃ¡ estÃ¡ atualizado acima)
    moeda_selecionada = st.session_state.get('moeda_selecionada', 'ï¿½ðŸ‡º â‚¬')

    # Extrair cÃ³digo e sÃ­mbolo da moeda
    if moeda_selecionada == "ðŸ‡§ðŸ‡· R$":
        moeda_codigo = "BRL"
        moeda_simbolo = "R$"
    elif moeda_selecionada == "ðŸ‡ºðŸ‡¸ $":
        moeda_codigo = "USD"
        moeda_simbolo = "$"
    elif moeda_selecionada == "ðŸ‡ªðŸ‡º â‚¬":
        moeda_codigo = "EUR"
        moeda_simbolo = "â‚¬"
    else:
        # Fallback
        moeda_codigo = "BRL"
        moeda_simbolo = "R$"

    st.markdown("---")

    # Teste de validaÃ§Ã£o da conversÃ£o (mostrar exemplo)
    if moeda_codigo != "BRL":
        valor_teste = 100.0
        valor_convertido = _core_converter_moeda(valor_teste, moeda_codigo, taxas_cambio)
        if moeda_codigo == "USD":
            taxa_esperada = taxa_usd_para_brl
            valor_esperado_divisao = valor_teste / taxa_esperada
            st.sidebar.info(f"ðŸ’¡ Teste conversÃ£o: R$ {valor_teste:,.2f} = {moeda_simbolo} {valor_convertido:,.2f} (taxa: 1 {moeda_simbolo} = R$ {taxa_esperada:.2f})")
            st.sidebar.caption(f"âœ… ValidaÃ§Ã£o: {valor_teste:,.2f} / {taxa_esperada:.2f} = {valor_esperado_divisao:,.2f} (deve ser igual a {valor_convertido:,.2f})")
        else:  # EUR
            taxa_esperada = taxa_eur_para_brl
            valor_esperado_divisao = valor_teste / taxa_esperada
            st.sidebar.info(f"ðŸ’¡ Teste conversÃ£o: R$ {valor_teste:,.2f} = {moeda_simbolo} {valor_convertido:,.2f} (taxa: 1 {moeda_simbolo} = R$ {taxa_esperada:.2f})")
            st.sidebar.caption(f"âœ… ValidaÃ§Ã£o: {valor_teste:,.2f} / {taxa_esperada:.2f} = {valor_esperado_divisao:,.2f} (deve ser igual a {valor_convertido:,.2f})")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**ðŸ” Filtros**")

    # Carregar dados com o ano selecionado
    try:
        df_total = load_data(ano_selecionado)
        # Evitar mutaÃ§Ãµes no cache
        if df_total is not None:
            df_total = df_total.copy()
        
        # Verificar se df_total foi carregado corretamente
        if df_total is None:
            st.error("âŒ Erro: Nenhum dado foi carregado (df_total Ã© None)")
            st.stop()
        
        if df_total.empty:
            st.error("âŒ Erro: DataFrame carregado estÃ¡ vazio")
            st.stop()
    except Exception as e:
        st.error(f"âŒ Erro: {str(e)}")
        import traceback
        st.error(f"Detalhes: {traceback.format_exc()}")
        st.stop()

    # Aplicar fator de conversÃ£o nas colunas Total e BUD (antes de qualquer processamento)
    # Isso simplifica os cÃ¡lculos pois o fator Ã© aplicado uma Ãºnica vez na origem
    # MantÃ©m os dados na mesma unidade para comparaÃ§Ãµes consistentes
    # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: NÃƒO aplicar fator de conversÃ£o quando estÃ¡ em modo CPU
    # No modo CPU, o fator nÃ£o deve ser aplicado pois CPU jÃ¡ Ã© uma razÃ£o (Total/Volume)
    if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
        if fator_conversao == "K (milhares)":
            if 'Total' in df_total.columns:
                df_total['Total'] = df_total['Total'] / 1000
        elif fator_conversao == "M (MilhÃµes)":
            if 'Total' in df_total.columns:
                df_total['Total'] = df_total['Total'] / 1000000

    # Aplicar conversÃ£o de moeda DEPOIS do fator de conversÃ£o (mesma lÃ³gica do fator)
    # Isso garante que todos os dados derivados jÃ¡ terÃ£o a conversÃ£o aplicada
    # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
    # IMPORTANTE: Aplicar conversÃ£o em AMBOS os modos (Custo Total e CPU)
    # No modo CPU, o Total convertido serÃ¡ usado para calcular CPU = Total convertido / Volume
    if moeda_codigo != "BRL" and 'Total' in df_total.columns:
        df_total = _core_converter_coluna_moeda(df_total, 'Total', moeda_codigo, taxas_cambio)

    def _sync_oficina_from_sidebar():
        selecionadas = st.session_state.get('filtro_oficina_tc_ext_multiselect', ["Todos"]) or ["Todos"]
        st.session_state.filtro_oficina_tc_ext = selecionadas
        st.session_state['filtro_oficina_grafico_periodo'] = selecionadas

    def _sync_veiculo_from_sidebar():
        selecionadas = st.session_state.get('filtro_veiculo_tc_ext_multiselect', ["Todos"]) or ["Todos"]
        st.session_state.filtro_veiculo_tc_ext = selecionadas
        st.session_state['filtro_veiculo_grafico_periodo'] = selecionadas

    # Inicializar session_state para filtros
    if 'filtro_oficina_tc_ext' not in st.session_state:
        st.session_state.filtro_oficina_tc_ext = ["Todos"]

    # Filtro 1: Oficina (com cache otimizado)
    if 'Oficina' in df_total.columns:
        # ðŸ”§ Ajuste: incluir oficinas disponÃ­veis no Budget (custos) e no Budget de Volume
        oficinas_set = set(df_total['Oficina'].dropna().astype(str).unique().tolist())
        oficinas_set.update(get_budget_oficinas_opcoes(ano_selecionado))
        oficinas_set.update(get_budget_volume_oficinas_opcoes(ano_selecionado))
        oficina_opcoes = ["Todos"] + sorted(oficinas_set)
        st.session_state['_oficina_opcoes_tc_ext'] = oficina_opcoes
        if 'filtro_oficina_tc_ext_multiselect' not in st.session_state:
            st.session_state['filtro_oficina_tc_ext_multiselect'] = st.session_state.filtro_oficina_tc_ext
        # Validar valores salvos
        default_oficina = st.session_state.filtro_oficina_tc_ext if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_tc_ext) else ["Todos"]
        oficina_selecionadas = st.sidebar.multiselect(
            "Selecione a Oficina:",
            oficina_opcoes,
            default=default_oficina,
            key="filtro_oficina_tc_ext_multiselect",
            on_change=_sync_oficina_from_sidebar,
        )
        # Atualizar session_state
        st.session_state.filtro_oficina_tc_ext = oficina_selecionadas if oficina_selecionadas else ["Todos"]

        # Filtrar o DataFrame com base na Oficina
        if "Todos" in oficina_selecionadas or not oficina_selecionadas:
            df_filtrado = df_total.copy()
        else:
            df_filtrado = df_total[
                df_total['Oficina'].astype(str).isin(oficina_selecionadas)
            ].copy()
    else:
        df_filtrado = df_total.copy()

    # Filtro 2: VeÃ­culo
    if 'filtro_veiculo_tc_ext' not in st.session_state:
        st.session_state.filtro_veiculo_tc_ext = ["Todos"]
    
    if 'VeÃ­culo' in df_filtrado.columns:
        veiculo_opcoes = get_filter_options(df_filtrado, 'VeÃ­culo')
        st.session_state['_veiculo_opcoes_tc_ext'] = veiculo_opcoes
        if 'filtro_veiculo_tc_ext_multiselect' not in st.session_state:
            st.session_state['filtro_veiculo_tc_ext_multiselect'] = st.session_state.filtro_veiculo_tc_ext
        default_veiculo = st.session_state.filtro_veiculo_tc_ext if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_tc_ext) else ["Todos"]
        veiculo_selecionados = st.sidebar.multiselect(
            "Selecione o VeÃ­culo:",
            veiculo_opcoes,
            default=default_veiculo,
            key="filtro_veiculo_tc_ext_multiselect",
            on_change=_sync_veiculo_from_sidebar,
        )
        st.session_state.filtro_veiculo_tc_ext = veiculo_selecionados if veiculo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no VeÃ­culo
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados)
            ].copy()

    # Filtro 3: USI
    if 'filtro_usi_tc_ext' not in st.session_state:
        st.session_state.filtro_usi_tc_ext = ["Todos"]
    
    if 'USI' in df_filtrado.columns:
        usi_opcoes = get_filter_options(df_filtrado, 'USI')
        default_usi = st.session_state.filtro_usi_tc_ext if all(x in usi_opcoes for x in st.session_state.filtro_usi_tc_ext) else ["Todos"]
        usi_selecionadas = st.sidebar.multiselect(
            "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_tc_ext_multiselect"
        )
        st.session_state.filtro_usi_tc_ext = usi_selecionadas if usi_selecionadas else ["Todos"]
        
        # Filtrar o DataFrame com base na USI
        if usi_selecionadas and "Todos" not in usi_selecionadas:
            df_filtrado = df_filtrado[
                df_filtrado['USI'].astype(str).isin(usi_selecionadas)
            ].copy()

    # Filtro 4: PerÃ­odo
    if 'filtro_periodo_tc_ext' not in st.session_state:
        # PadrÃ£o: mÃªs atual
        from datetime import datetime as _dt_ext
        _meses_ext = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'MarÃ§o', 4: 'Abril',
            5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
            9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
        }
        _mes_atual_ext = _meses_ext.get(_dt_ext.now().month, '')
        st.session_state.filtro_periodo_tc_ext = [_mes_atual_ext] if _mes_atual_ext else ["Todos"]
    
    if 'PerÃ­odo' in df_filtrado.columns:
        # ðŸ”§ CORREÃ‡ÃƒO: nÃ£o limitar a meses do realizado.
        # Sempre oferecer todos os meses (e tambÃ©m o que existir em Budget/Volume Budget).
        ordem_meses = ['Janeiro', 'Fevereiro', 'MarÃ§o', 'Abril', 'Maio', 'Junho',
                      'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

        periodos_set = set(ordem_meses)
        try:
            periodo_opcoes_real = get_filter_options(df_total, 'PerÃ­odo') if 'PerÃ­odo' in df_total.columns else []
            periodos_set.update([p for p in periodo_opcoes_real if p and p != 'Todos'])
        except Exception:
            pass

        # Trazer perÃ­odos do Budget (custos) e do Budget (volume), quando disponÃ­veis
        try:
            df_budget_opcoes = load_budget_data(ano_selecionado)
            if df_budget_opcoes is not None and 'PerÃ­odo' in df_budget_opcoes.columns:
                periodos_set.update(df_budget_opcoes['PerÃ­odo'].dropna().astype(str).unique().tolist())
        except Exception:
            pass

        try:
            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
            if df_budget_vol_opcoes is not None and 'PerÃ­odo' in df_budget_vol_opcoes.columns:
                periodos_set.update(df_budget_vol_opcoes['PerÃ­odo'].dropna().astype(str).unique().tolist())
        except Exception:
            pass

        # Montar lista ordenada: meses (sempre) + outros perÃ­odos
        outros_periodos = sorted([p for p in periodos_set if p not in ordem_meses and p not in (None, '', 'Todos')])
        periodo_opcoes_ordenados = ["Todos"] + ordem_meses + outros_periodos
        
        default_periodo = st.session_state.filtro_periodo_tc_ext if all(x in periodo_opcoes_ordenados for x in st.session_state.filtro_periodo_tc_ext) else ["Todos"]
        periodo_selecionados = st.sidebar.multiselect(
            "Selecione o PerÃ­odo:", periodo_opcoes_ordenados, default=default_periodo, key="filtro_periodo_tc_ext_multiselect"
        )
        st.session_state.filtro_periodo_tc_ext = periodo_selecionados if periodo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no PerÃ­odo
        if periodo_selecionados and "Todos" not in periodo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['PerÃ­odo'].astype(str).isin(periodo_selecionados)
            ].copy()

    # Filtros principais adicionais
    filtros_principais = [
        ("Type 05", "Type 05", "multiselect"),
        ("Type 06", "Type 06", "multiselect"),
        ("Account", "Account", "multiselect"),
        ("Fornecedor", "Fornecedor", "multiselect"),
        ("Fornec.", "Fornec.", "multiselect"),
        ("Tipo", "Tipo", "multiselect")
    ]

    for col_name, label, widget_type in filtros_principais:
        if col_name in df_filtrado.columns:
            # Inicializar session_state para cada filtro principal
            filtro_key = f'filtro_{col_name}_tc_ext'
            if filtro_key not in st.session_state:
                st.session_state[filtro_key] = ["Todos"]
            
            opcoes = get_filter_options(df_filtrado, col_name)
            if widget_type == "multiselect":
                # Validar valores salvos
                default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
                selecionadas = st.sidebar.multiselect(
                    f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect"
                )
                # Atualizar session_state
                st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
                if selecionadas and "Todos" not in selecionadas:
                    df_filtrado = df_filtrado[
                        df_filtrado[col_name].astype(str).isin(selecionadas)
                    ].copy()

    # Filtros avanÃ§ados (expansÃ­vel)
    with st.sidebar.expander("ðŸ” Filtros AvanÃ§ados"):
        filtros_avancados = [
            ("Custo", "Custo"),
            ("Type 07", "Type 07"),
            ("Texto breve", "Texto breve"),
            ("Material", "Material"),
            ("Pedido", "Pedido"),
            ("Ordem", "Ordem"),
            ("CtAtvFixo", "CtAtvFixo")
        ]
        
        for col_name, label in filtros_avancados:
            if col_name in df_filtrado.columns:
                # Inicializar session_state para cada filtro avanÃ§ado
                filtro_key = f'filtro_{col_name}_tc_ext_av'
                if filtro_key not in st.session_state:
                    st.session_state[filtro_key] = ["Todos"]
                
                opcoes = get_filter_options(df_filtrado, col_name)
                # Validar valores salvos
                default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
                selecionadas = st.sidebar.multiselect(
                    f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect"
                )
                # Atualizar session_state
                st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
                if selecionadas and "Todos" not in selecionadas:
                    df_filtrado = df_filtrado[
                        df_filtrado[col_name].astype(str).isin(selecionadas)
                    ].copy()

# FunÃ§Ã£o auxiliar para encontrar arquivo parquet na ordem de prioridade (disponÃ­vel para todas as pÃ¡ginas)
def encontrar_arquivo_parquet(nome_arquivo, ano_selecionado=None):
    """
    Busca arquivo parquet na seguinte ordem de prioridade:
    1. Se ano_selecionado for None ou "Todos": HistÃ³rico consolidado (dados/TC_Ext/historico_consolidado/)
    2. Se ano_selecionado for especificado: Pasta do ano (dados/TC_Ext/{ANO}/)
    3. Pasta do ano mais recente (dados/TC_Ext/{ANO}/)
    4. Raiz do projeto (compatibilidade)
    """
    return _core_encontrar_arquivo_parquet(nome_arquivo, ano_selecionado)


# FunÃ§Ã£o para converter valor de R$ para outra moeda
def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino."""
    return _core_converter_moeda(valor, moeda_destino, taxas)

# FunÃ§Ã£o para converter coluna inteira de DataFrame
def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda."""
    return _core_converter_coluna_moeda(df, coluna, moeda_destino, taxas)

# FunÃ§Ã£o para obter sÃ­mbolo da moeda (disponÃ­vel para todas as pÃ¡ginas)
def obter_simbolo_moeda(moeda_codigo):
    """Retorna o sÃ­mbolo da moeda."""
    return _core_obter_simbolo_moeda(moeda_codigo)

# FunÃ§Ã£o para carregar dados com cache (disponÃ­vel para todas as pÃ¡ginas)
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet - SEMPRE do histÃ³rico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histÃ³rico consolidado para garantir consistÃªncia
        # Apenas aplicar filtro de ano quando necessÃ¡rio
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
        caminho_absoluto = os.path.abspath(caminho_historico)
        
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
        else:
            st.error(f"âŒ Arquivo de histÃ³rico consolidado nÃ£o encontrado: {caminho_absoluto}")
            st.info("ðŸ’¡ Execute o dados.ipynb para gerar o histÃ³rico consolidado")
            st.stop()
            return None

        # Se um ano especÃ­fico foi selecionado, filtrar apÃ³s carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histÃ³rico consolidado)
        # e apenas filtramos pelo ano, mantendo consistÃªncia
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se nÃ£o conseguir converter para int, nÃ£o filtrar por ano
                pass

        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos para formato capitalizado (primeira letra maiÃºscula)
        # Isso garante consistÃªncia com o resto do cÃ³digo que espera perÃ­odos capitalizados
        if 'PerÃ­odo' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza perÃ­odo para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
            
            df['PerÃ­odo'] = df['PerÃ­odo'].apply(normalizar_periodo)

        # Converter colunas numÃ©ricas conhecidas para numÃ©rico ANTES da otimizaÃ§Ã£o
        # Isso evita que sejam convertidas para categorical
        colunas_numericas = ['Valor', 'Total', 'Volume', 'CPU']
        for col in colunas_numericas:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')

        # Converter floats para tipos menores
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Converter ints para tipos menores
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    except Exception as e:
        st.error(f"âŒ Erro ao carregar dados: {str(e)}")
        st.stop()


# FunÃ§Ã£o para carregar dados de volume com cache
@st.cache_data(
    ttl=60,  # ðŸ”§ REDUZIDO para 60 segundos para forÃ§ar atualizaÃ§Ã£o mais frequente
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_volume_data(ano_selecionado_param):
    """Carrega os dados de volume do arquivo parquet - SEMPRE do histÃ³rico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histÃ³rico consolidado para garantir consistÃªncia
        # Apenas aplicar filtro de ano quando necessÃ¡rio
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
        
        if os.path.exists(caminho_historico):
            # ðŸ”§ CORREÃ‡ÃƒO: Garantir que Volume seja sempre numÃ©rico ao carregar
            df = pd.read_parquet(caminho_historico)
            df = padronizar_colunas(df)
            if 'Volume' in df.columns:
                df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
        else:
            return None

        # Se um ano especÃ­fico foi selecionado, filtrar apÃ³s carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histÃ³rico consolidado)
        # e apenas filtramos pelo ano, mantendo consistÃªncia
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se nÃ£o conseguir converter para int, nÃ£o filtrar por ano
                pass

        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos para formato capitalizado (primeira letra maiÃºscula)
        # Isso garante consistÃªncia com o resto do cÃ³digo que espera perÃ­odos capitalizados
        if 'PerÃ­odo' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza perÃ­odo para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
            
            df['PerÃ­odo'] = df['PerÃ­odo'].apply(normalizar_periodo)

        # Converter colunas numÃ©ricas conhecidas para numÃ©rico ANTES da otimizaÃ§Ã£o
        # Isso evita que sejam convertidas para categorical
        colunas_numericas = ['Valor', 'Total', 'Volume', 'CPU']
        for col in colunas_numericas:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')

        # Converter floats para tipos menores
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Converter ints para tipos menores
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    except Exception:
        return None


def filtrar_volume_com_sidebar(df_vol, df_total_base, ignorar_veiculo: bool = False):
    """Aplica os filtros da sidebar aos dados de volume.

    `ignorar_veiculo=True` Ã© Ãºtil quando o volume serÃ¡ usado como base de participaÃ§Ã£o
    (rateio) e nÃ£o deve ser renormalizado ao filtrar um subconjunto de veÃ­culos.
    """
    if df_vol is None:
        return None

    df_vol_filtrado = df_vol.copy()

    # Filtro 1: Oficina
    if 'Oficina' in df_vol_filtrado.columns and df_total_base is not None:
        # ðŸ”§ Ajuste: incluir tambÃ©m oficinas disponÃ­veis no Budget (custos e volume)
        oficinas_set = set(get_filter_options(df_total_base, 'Oficina'))
        oficinas_set.discard("Todos")
        try:
            df_budget_opcoes = load_budget_data(ano_selecionado)
            if df_budget_opcoes is not None and 'Oficina' in df_budget_opcoes.columns:
                oficinas_set.update(df_budget_opcoes['Oficina'].dropna().astype(str).unique().tolist())
        except Exception:
            pass
        try:
            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
            if df_budget_vol_opcoes is not None and 'Oficina' in df_budget_vol_opcoes.columns:
                oficinas_set.update(df_budget_vol_opcoes['Oficina'].dropna().astype(str).unique().tolist())
        except Exception:
            pass
        oficina_opcoes_disponiveis = sorted(oficinas_set)
        oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])

        if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
            ].copy()
        else:
            oficinas_validas = [o for o in oficina_selecionadas_sidebar if o in oficina_opcoes_disponiveis]
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Oficina'].astype(str).isin(oficinas_validas)
            ].copy()

    # Filtro 2: VeÃ­culo
    if (not ignorar_veiculo) and 'VeÃ­culo' in df_vol_filtrado.columns:
        veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])
        if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_sidebar)
            ].copy()

    # Filtro 3: USI
    if 'USI' in df_vol_filtrado.columns:
        usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_ext', ["Todos"])
        if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['USI'].astype(str).isin(usi_selecionada_sidebar)
            ].copy()

    # Filtro 4: PerÃ­odo (mÃªs)
    if 'PerÃ­odo' in df_vol_filtrado.columns:
        periodo_selecionado_sidebar = st.session_state.get('filtro_periodo_tc_ext', ["Todos"])
        if isinstance(periodo_selecionado_sidebar, tuple):
            periodo_selecionado_sidebar = list(periodo_selecionado_sidebar)
        if periodo_selecionado_sidebar and "Todos" not in periodo_selecionado_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['PerÃ­odo'].astype(str).isin([str(x) for x in periodo_selecionado_sidebar])
            ].copy()

    # Filtro 5: Centro cst
    if 'Centrocst' in df_vol_filtrado.columns:
        centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_ext', "Todos")
        if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
            ].copy()

    # Filtro 6: Conta contÃ¡bil
    if 'NÂºconta' in df_vol_filtrado.columns:
        conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_ext', [])
        if conta_contabil_selecionadas_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['NÂºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
            ].copy()

    # Filtros principais
    filtros_principais_nomes = ["Type 05", "Type 06", "Fornecedor", "Fornec.", "Tipo"]
    for col_name in filtros_principais_nomes:
        if col_name in df_vol_filtrado.columns:
            filtro_key = f'filtro_{col_name}_tc_ext'
            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                df_vol_filtrado = df_vol_filtrado[
                    df_vol_filtrado[col_name].astype(str).isin(selecionadas_sidebar)
                ].copy()

    # Filtros avanÃ§ados
    filtros_avancados_nomes = ["UsuÃ¡rio", "Material", "Dt.lÃ§to.", "Texto breve", "Account"]
    for col_name in filtros_avancados_nomes:
        if col_name in df_vol_filtrado.columns:
            filtro_key = f'filtro_avancado_{col_name}_tc_ext'
            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                df_vol_filtrado = df_vol_filtrado[
                    df_vol_filtrado[col_name].astype(str).isin(selecionadas_sidebar)
                ].copy()

    return df_vol_filtrado


def _merge_volume_com_fallback(df_base, df_volume):
    """Garante coluna Volume em df_base usando chaves disponÃ­veis; fallback por Oficina."""
    if df_base is None or df_volume is None:
        return df_base
    if 'Volume' not in df_volume.columns:
        return df_base

    df_out = df_base.copy()

    # Se Volume jÃ¡ existe e parece vÃ¡lido (tem valores nÃ£o-nulos e nÃ£o-zerados), nÃ£o mexer.
    if 'Volume' in df_out.columns:
        try:
            vol_existing = pd.to_numeric(df_out['Volume'], errors='coerce')
            if vol_existing.notna().any() and float(vol_existing.fillna(0).abs().sum()) > 0.0:
                return df_out
        except Exception:
            # Se der problema de conversÃ£o, tentar recalcular por merge.
            pass

        # Caso esteja todo NaN/0, vamos substituir via merge.
        try:
            df_out = df_out.drop(columns=['Volume'])
        except Exception:
            pass

    # Normalizar dimensÃµes para reduzir mismatch (spaces/categorias)
    df_base_tmp = df_out.copy()
    df_vol_tmp = df_volume.copy()
    for col in ['Oficina', 'VeÃ­culo', 'PerÃ­odo', 'Ano']:
        if col in df_base_tmp.columns and col in df_vol_tmp.columns:
            df_base_tmp[col] = df_base_tmp[col].astype(str).str.strip()
            df_vol_tmp[col] = df_vol_tmp[col].astype(str).str.strip()

    df_vol_tmp['Volume'] = pd.to_numeric(df_vol_tmp['Volume'], errors='coerce')

    # Tentar merges do mais granular para o mais agregador (para evitar conflito de filtros)
    candidatos = []
    chaves_full = [
        col for col in ['Oficina', 'VeÃ­culo', 'PerÃ­odo', 'Ano']
        if col in df_base_tmp.columns and col in df_vol_tmp.columns
    ]
    if chaves_full:
        candidatos.append(chaves_full)

    for ks in [
        ['Oficina', 'PerÃ­odo', 'Ano'],
        ['Oficina', 'PerÃ­odo'],
        ['Oficina', 'Ano'],
        ['Oficina'],
    ]:
        ks_ok = [c for c in ks if c in df_base_tmp.columns and c in df_vol_tmp.columns]
        if ks_ok and ks_ok not in candidatos:
            candidatos.append(ks_ok)

    for chaves in candidatos:
        try:
            vol_agr = df_vol_tmp.groupby(chaves)['Volume'].sum().reset_index()
            merged = df_base_tmp.merge(vol_agr, on=chaves, how='left')
            vol_m = pd.to_numeric(merged['Volume'], errors='coerce')
            if vol_m.notna().any() and float(vol_m.fillna(0).abs().sum()) > 0.0:
                return merged
        except Exception:
            continue

    # Ãšltimo fallback: adiciona Volume=0 para evitar falhas posteriores
    df_base_tmp['Volume'] = 0
    return df_base_tmp


# FunÃ§Ã£o para carregar dados de budget (Total) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_data(ano_selecionado_param):
    """Carrega os dados de budget do arquivo parquet - SEMPRE do histÃ³rico consolidado BUD"""
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        caminho_budget = os.path.join(
            project_root,
            "dados",
            "historico_consolidado",
            "BUD",
            "df_final_historico_BUD.parquet",
        )
        
        if os.path.exists(caminho_budget):
            df = pd.read_parquet(caminho_budget)
            df = padronizar_colunas(df)
        else:
            return None

        # Se um ano especÃ­fico foi selecionado, filtrar apÃ³s carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se nÃ£o conseguir converter para int, nÃ£o filtrar por ano
                pass

        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos para formato capitalizado (primeira letra maiÃºscula)
        # Isso garante consistÃªncia com o resto do cÃ³digo que espera perÃ­odos capitalizados
        if 'PerÃ­odo' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza perÃ­odo para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
            
            df['PerÃ­odo'] = df['PerÃ­odo'].apply(normalizar_periodo)

        # Converter colunas numÃ©ricas conhecidas para numÃ©rico ANTES da otimizaÃ§Ã£o
        colunas_numericas = ['Valor', 'Total', 'Volume', 'CPU']
        for col in colunas_numericas:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')

        # Converter floats para tipos menores
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Converter ints para tipos menores
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    except Exception:
        return None


# FunÃ§Ã£o para carregar dados de budget (Volume) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_volume_data(ano_selecionado_param):
    """Carrega os dados de volume de budget do arquivo parquet - SEMPRE do histÃ³rico consolidado BUD"""
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        caminho_budget_vol = os.path.join(
            project_root,
            "dados",
            "historico_consolidado",
            "BUD",
            "df_vol_historico_BUD.parquet",
        )
        
        if os.path.exists(caminho_budget_vol):
            df = pd.read_parquet(caminho_budget_vol)
            df = padronizar_colunas(df)
        else:
            return None

        # GovernanÃ§a: Volume BUD precisa ter 'VeÃ­culo'. Se nÃ£o tiver, Ã© erro de extraÃ§Ã£o.
        if 'VeÃ­culo' not in df.columns:
            raise ValueError(
                "âŒ ERRO NA EXTRAÃ‡ÃƒO: o arquivo 'df_vol_historico_BUD.parquet' nÃ£o contÃ©m a coluna 'VeÃ­culo'. "
                "RefaÃ§a a extraÃ§Ã£o do BUDGET e verifique a aba 'Volume BDG'."
            )

        # Se um ano especÃ­fico foi selecionado, filtrar apÃ³s carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se nÃ£o conseguir converter para int, nÃ£o filtrar por ano
                pass

        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos para formato capitalizado (primeira letra maiÃºscula)
        # Isso garante consistÃªncia com o resto do cÃ³digo que espera perÃ­odos capitalizados
        if 'PerÃ­odo' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza perÃ­odo para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
            
            df['PerÃ­odo'] = df['PerÃ­odo'].apply(normalizar_periodo)

        # Converter colunas numÃ©ricas conhecidas para numÃ©rico ANTES da otimizaÃ§Ã£o
        colunas_numericas = ['Volume']
        for col in colunas_numericas:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    df[col] = df[col].astype('category')

        # Converter floats para tipos menores
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Converter ints para tipos menores
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')

        return df
    except Exception:
        return None

# Ordem dos meses para ordenaÃ§Ã£o cronolÃ³gica (disponÃ­vel para todas as pÃ¡ginas)
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'marÃ§o', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]


def _formatar_num_ptbr(valor, casas=2):
    """Formata nÃºmero no padrÃ£o pt-BR (1.234,56)."""
    try:
        if pd.isna(valor):
            return "-"
        v = float(valor)
        s = f"{v:,.{casas}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "-"


def _normalizar_mes_lower(periodo):
    if pd.isna(periodo):
        return periodo
    p = str(periodo).strip()
    if not p:
        return p
    pl = p.lower()
    mapeamento = {
        'janeiro': 'janeiro',
        'fevereiro': 'fevereiro',
        'marÃ§o': 'marÃ§o',
        'marco': 'marÃ§o',
        'abril': 'abril',
        'maio': 'maio',
        'junho': 'junho',
        'julho': 'julho',
        'agosto': 'agosto',
        'setembro': 'setembro',
        'outubro': 'outubro',
        'novembro': 'novembro',
        'dezembro': 'dezembro',
    }
    return mapeamento.get(pl, pl)


def _montar_tabela_resumo_oficinas(
    df_valores,
    tipo_visualizacao,
    index_name,
    coluna_valor_preferida=None,
    df_volume=None,
):
    """Gera tabela (Oficina x mÃªs + Ano) alinhada aos filtros e ao modo CPU."""
    try:
        if df_valores is None or getattr(df_valores, "empty", True):
            return None

        if 'Oficina' not in df_valores.columns or 'PerÃ­odo' not in df_valores.columns:
            return None

        base = df_valores.copy()
        base['Oficina'] = base['Oficina'].astype(str).str.strip()
        base['PerÃ­odo'] = base['PerÃ­odo'].apply(_normalizar_mes_lower)

        # Para CPU precisamos de Total (custo) e Volume no MESMO nÃ­vel da linha.
        # Regra: CPU sempre Ã© calculado como soma(Total)/soma(Volume) no nÃ­vel desejado.
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if 'Total' not in base.columns:
                return None

            if df_volume is None or getattr(df_volume, "empty", True) or 'Volume' not in df_volume.columns:
                return None

            # 1) Numerador: custo agregado por Oficina+PerÃ­odo
            custo = base[['Oficina', 'PerÃ­odo', 'Total']].copy()
            custo['Total'] = pd.to_numeric(custo['Total'], errors='coerce').fillna(0)
            custo_mes = custo.groupby(['Oficina', 'PerÃ­odo'], as_index=False)['Total'].sum()
            custo_ano = custo.groupby(['Oficina'], as_index=False)['Total'].sum().rename(columns={'Total': 'Total_Ano'})

            # 2) Denominador: volume agregado por Oficina+PerÃ­odo (NÃƒO mergear volume na granularidade de custo)
            vol = df_volume.copy()
            if 'Oficina' not in vol.columns or 'PerÃ­odo' not in vol.columns:
                return None
            vol['Oficina'] = vol['Oficina'].astype(str).str.strip()
            vol['PerÃ­odo'] = vol['PerÃ­odo'].apply(_normalizar_mes_lower)
            vol['Volume'] = pd.to_numeric(vol['Volume'], errors='coerce').fillna(0)
            vol_mes = vol.groupby(['Oficina', 'PerÃ­odo'], as_index=False)['Volume'].sum()
            vol_ano = vol.groupby(['Oficina'], as_index=False)['Volume'].sum().rename(columns={'Volume': 'Volume_Ano'})

            # 3) Juntar no nÃ­vel correto e calcular CPU por cÃ©lula
            agr_mes = custo_mes.merge(vol_mes, on=['Oficina', 'PerÃ­odo'], how='outer')
            agr_mes['Total'] = pd.to_numeric(agr_mes.get('Total'), errors='coerce').fillna(0)
            agr_mes['Volume'] = pd.to_numeric(agr_mes.get('Volume'), errors='coerce').fillna(0)
            agr_mes['Metrica'] = np.where(agr_mes['Volume'] != 0, agr_mes['Total'] / agr_mes['Volume'], np.nan)

            agr_ano = custo_ano.merge(vol_ano, on=['Oficina'], how='outer')
            agr_ano['Total_Ano'] = pd.to_numeric(agr_ano.get('Total_Ano'), errors='coerce').fillna(0)
            agr_ano['Volume_Ano'] = pd.to_numeric(agr_ano.get('Volume_Ano'), errors='coerce').fillna(0)
            agr_ano['Ano'] = np.where(agr_ano['Volume_Ano'] != 0, agr_ano['Total_Ano'] / agr_ano['Volume_Ano'], np.nan)

            piv = agr_mes.pivot_table(index='Oficina', columns='PerÃ­odo', values='Metrica', aggfunc='sum')
            piv['Ano'] = piv.index.to_series().map(dict(zip(agr_ano['Oficina'], agr_ano['Ano'])))

            # Linha Total (CPU total = soma(Total)/soma(Volume) por PerÃ­odo e no Ano)
            tot_mes = agr_mes.groupby('PerÃ­odo', as_index=False).agg({'Total': 'sum', 'Volume': 'sum'})
            tot_mes['CPU'] = np.where(tot_mes['Volume'] != 0, tot_mes['Total'] / tot_mes['Volume'], np.nan)
            total_row = {str(p): np.nan for p in ORDEM_MESES}
            for _, r in tot_mes.iterrows():
                p = r.get('PerÃ­odo')
                if pd.notna(p):
                    total_row[str(p)] = r.get('CPU')
            total_total = float(pd.to_numeric(agr_mes['Total'], errors='coerce').fillna(0).sum())
            total_volume = float(pd.to_numeric(agr_mes['Volume'], errors='coerce').fillna(0).sum())
            total_row['Ano'] = (total_total / total_volume) if total_volume != 0 else np.nan
            piv.loc['Total'] = pd.Series(total_row)
        else:
            col_valor = coluna_valor_preferida if coluna_valor_preferida in base.columns else None
            if col_valor is None:
                col_valor = 'Total' if 'Total' in base.columns else None
            if col_valor is None:
                return None

            base[col_valor] = pd.to_numeric(base[col_valor], errors='coerce')
            agr_mes = base.groupby(['Oficina', 'PerÃ­odo'], as_index=False)[col_valor].sum()
            agr_ano = base.groupby(['Oficina'], as_index=False)[col_valor].sum().rename(columns={col_valor: 'Ano'})
            piv = agr_mes.pivot_table(index='Oficina', columns='PerÃ­odo', values=col_valor, aggfunc='sum')
            piv['Ano'] = piv.index.to_series().map(dict(zip(agr_ano['Oficina'], agr_ano['Ano'])))

            # Linha Total
            tot_mes = agr_mes.groupby('PerÃ­odo', as_index=False)[col_valor].sum()
            total_row = {str(p): np.nan for p in ORDEM_MESES}
            for _, r in tot_mes.iterrows():
                p = r.get('PerÃ­odo')
                if pd.notna(p):
                    total_row[str(p)] = r.get(col_valor)
            total_row['Ano'] = float(pd.to_numeric(agr_ano['Ano'], errors='coerce').fillna(0).sum()) if 'Ano' in agr_ano.columns else np.nan
            piv.loc['Total'] = pd.Series(total_row)

        # Ordenar colunas (sempre exibir os 12 meses + Ano)
        cols = [c for c in piv.columns if isinstance(c, str)]
        outros = [c for c in cols if c not in ORDEM_MESES and c != 'Ano']
        ordem_final = list(ORDEM_MESES) + outros
        if 'Ano' in piv.columns:
            ordem_final += ['Ano']
        piv = piv.reindex(columns=ordem_final)

        # Ordenar oficinas alfabeticamente e manter "Total" no final
        if 'Total' in piv.index:
            idx = [i for i in piv.index.tolist() if i != 'Total']
            idx_sorted = sorted(idx)
            piv = piv.reindex(idx_sorted + ['Total'])
        else:
            piv = piv.sort_index()
        piv.index.name = index_name
        return piv
    except Exception:
        return None

# (CÃ³digo de filtros movido para dentro do bloco if is_main_page:)


def formatar_ratio_com_barra(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com barra de progresso em HTML"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular largura da barra: 100% = barra cheia, acima de 100% tambÃ©m fica cheia
    if percentual >= 100:
        largura_barra = 100  # Barra cheia para 100% ou mais
    else:
        largura_barra = percentual  # Proporcional atÃ© 100%
    
    # Calcular cor: verde atÃ© 90%, depois gradiente atÃ© vermelho em 100%
    if percentual <= 0:
        r, g, b = 0, 170, 0  # Verde (#00AA00)
    elif percentual <= 90:
        r, g, b = 0, 170, 0  # Verde puro atÃ© 90%
    elif percentual >= 100:
        r, g, b = 255, 0, 0  # Vermelho (#FF0000) quando 100% ou mais
    else:
        # Gradiente de verde para vermelho entre 90% e 100%
        # progresso vai de 0 (em 90%) a 1 (em 100%)
        progresso = (percentual - 90) / 10
        r = int(255 * progresso)  # 0 em 90%, 255 em 100%
        g = int(170 * (1 - progresso))  # 170 em 90%, 0 em 100%
        b = 0
    
    cor = f"rgb({r}, {g}, {b})"
    
    # Detectar tema para adaptar cor do texto (igual Ã s outras colunas)
    try:
        theme_base = st.get_option("theme.base") or "light"
        # Usar a mesma cor que o Streamlit usa para texto em tabelas
        # Dark mode: rgb(250, 250, 250) ou #FAFAFA
        # Light mode: rgb(49, 51, 63) ou #31333F (cor padrÃ£o do Streamlit para texto)
        if theme_base == "dark":
            texto_cor = "#FAFAFA"  # Branco claro para dark mode
        else:
            texto_cor = "#31333F"  # Cinza escuro para light mode (cor padrÃ£o do Streamlit)
    except:
        # Fallback: tentar detectar via CSS do Streamlit
        texto_cor = "var(--text-color, #31333F)"  # Usar variÃ¡vel CSS se disponÃ­vel, senÃ£o usar cor padrÃ£o
    
    html = f"""
    <div style="display: flex; align-items: center; gap: 5px; width: 100%; justify-content: flex-start; margin: 0; padding: 0; vertical-align: middle;">
        <div style="width: 64px; background-color: #333; border-radius: 3px; height: 11px; position: relative; overflow: hidden; flex-shrink: 0; margin: 0;">
            <div style="width: {largura_barra}%; height: 100%; background-color: {cor}; transition: width 0.3s;"></div>
        </div>
        <span style="width: 65px; text-align: left; font-weight: normal; color: {texto_cor}; font-size: 0.75rem; flex-shrink: 0; line-height: 1.2; margin: 0;">{percentual:.0f}%</span>
    </div>
    """
    return html

def criar_tabela_html_com_barra(df_display, linha_resumo=None, linha_volumes=None):
    """Cria uma tabela HTML customizada no padrÃ£o Streamlit para renderizar HTML nas cÃ©lulas
    
    Args:
        df_display: DataFrame com os dados a serem exibidos
        linha_resumo: DicionÃ¡rio opcional com valores de resumo formatados para adicionar como primeira linha
        linha_volumes: DicionÃ¡rio opcional com volumes para adicionar como Ãºltima linha (ex: {'Volume Real': '1,000', 'Volume Budget': '1,200'})
    """
    # Usar o padrÃ£o de estilos do Streamlit para st.dataframe
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            # Cores transparentes no padrÃ£o Streamlit dark mode
            header_bg = "rgba(38, 39, 48, 0.15)"  # CabeÃ§alho mais transparente
            resumo_bg = "rgba(38, 39, 48, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(250, 250, 250, 0.1)"
        else:
            # Cores transparentes no padrÃ£o Streamlit light mode
            header_bg = "rgba(240, 242, 246, 0.15)"  # CabeÃ§alho mais transparente
            resumo_bg = "rgba(240, 242, 246, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(49, 51, 63, 0.1)"
    except:
        header_bg = "rgba(38, 39, 48, 0.15)"
        resumo_bg = "rgba(38, 39, 48, 0.15)"
        row_bg = "transparent"
        border_color = "rgba(250, 250, 250, 0.1)"
    
    # Criar tabela no padrÃ£o Streamlit
    html_table = """
    <div class='stDataFrame' style='overflow-x: auto; margin: 1rem 0;'>
        <style>
            .flex-bud-table {
                width: 100%;
                border-collapse: collapse;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            }
            .flex-bud-table thead tr {
                background-color: """ + header_bg + """;
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table th {
                padding: 0.75rem 1rem;
                text-align: left;
                font-weight: 600;
                font-size: 0.75rem;
                color: inherit;
            }
            .flex-bud-table tbody tr {
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table tbody tr:last-child {
                border-bottom: none;
            }
            .flex-bud-table .resumo-row {
                border-top: 2px solid """ + border_color + """;
            }
            .flex-bud-table td {
                padding: 0.75rem 1rem;
                font-size: 0.75rem;
                vertical-align: middle;
                font-weight: normal;
            }
            .flex-bud-table .resumo-row {
                background-color: """ + resumo_bg + """;
                font-weight: 600;
            }
            .flex-bud-table .number-cell {
                text-align: right;
                font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
                font-variant-numeric: tabular-nums;
                font-size: 0.7rem;
                font-weight: normal;
            }
            .flex-bud-table .total-flex-bud-col {
                max-width: 140px;
                width: 140px;
                white-space: nowrap;
            }
        </style>
        <table class='flex-bud-table'>
    """

    # CabeÃ§alho
    html_table += "<thead><tr>"
    for col in df_display.columns:
        if col == 'Total / Flex Bud':
            html_table += f"<th class='total-flex-bud-col'>{col}</th>"
        else:
            html_table += f"<th>{col}</th>"
    html_table += "</tr></thead><tbody>"
    
    # Linhas de dados - todas transparentes
    for idx, row in df_display.iterrows():
        html_table += f"<tr style='background-color: {row_bg};'>"
        for col in df_display.columns:
            if col == 'Total / Flex Bud':
                # O valor jÃ¡ deve estar formatado como HTML (com barrinha e percentual)
                # Se nÃ£o estiver formatado, formatar agora
                valor_celula = row[col]
                if isinstance(valor_celula, str) and '<div' in valor_celula:
                    # JÃ¡ estÃ¡ formatado como HTML
                    html_table += f"<td class='total-flex-bud-col'>{valor_celula}</td>"
                else:
                    # Formatar agora se ainda nÃ£o estiver formatado
                    valor_num = float(valor_celula) if pd.notna(valor_celula) and isinstance(valor_celula, (int, float)) else 0
                    html_formatado = formatar_ratio_com_barra(valor_num)
                    html_table += f"<td class='total-flex-bud-col'>{html_formatado}</td>"
            else:
                valor_celula = str(row[col])
                if any(char.isdigit() or char in ['$', 'â‚¬', 'R$', ',', '.', 'K', 'M'] for char in valor_celula):
                    html_table += f"<td class='number-cell'>{valor_celula}</td>"
                else:
                    html_table += f"<td>{valor_celula}</td>"
        html_table += "</tr>"
    
    # Linha de resumo removida - os resumos agora sÃ£o exibidos separadamente com caixas de texto
    
    # Adicionar linha de volumes se fornecida
    if linha_volumes:
        html_table += f"<tr class='resumo-row' style='background-color: {resumo_bg}; border-top: 2px solid {border_color};'>"
        for col in df_display.columns:
            valor_volume = linha_volumes.get(col, '-')
            html_table += f"<td class='number-cell' style='font-weight: 600;'>{valor_volume}</td>"
        html_table += "</tr>"
    
    html_table += "</tbody></table></div>"
    return html_table

def formatar_periodo_abreviado(periodo_str, ano=None, usar_ano_completo=False):
    """Formata perÃ­odo para formato abreviado (ex: Setembro 2024 -> Set/24 ou Set/2024 se usar_ano_completo=True)
    
    Args:
        periodo_str: String do perÃ­odo (ex: "Setembro 2024", "Total 2024", "2024 S1", "2024 Q1")
        ano: Ano opcional (se None, serÃ¡ extraÃ­do de periodo_str)
        usar_ano_completo: Se True, usa ano com 4 dÃ­gitos (para Ano a Ano, Semestre, Quarter)
    """
    # Mapeamento de meses para abreviaÃ§Ãµes
    meses_abrev = {
        'janeiro': 'Jan', 'fevereiro': 'Fev', 'marÃ§o': 'Mar', 'abril': 'Abr',
        'maio': 'Mai', 'junho': 'Jun', 'julho': 'Jul', 'agosto': 'Ago',
        'setembro': 'Set', 'outubro': 'Out', 'novembro': 'Nov', 'dezembro': 'Dez'
    }
    
    periodo_str = str(periodo_str).strip()
    mes_abrev = None
    ano_extraido = None
    
    # Verificar se Ã© formato especial (Ano a Ano, Semestre, Quarter)
    # Exemplos: "Total 2024", "2024 S1", "2024 Q1"
    if periodo_str.startswith('Total '):
        # Formato: "Total 2024" â†’ retornar "Total/2024"
        partes = periodo_str.split(' ', 1)
        if len(partes) > 1:
            ano_str = partes[1].strip()
            if ano_str.isdigit():
                return f"Total/{ano_str}"
        return "Total"
    elif ' S' in periodo_str:
        # Formato: "2024 S1" ou "2024 S2" â†’ retornar "2024/1" ou "2024/2"
        partes = periodo_str.split(' S')
        if len(partes) == 2:
            ano_str = partes[0].strip()
            semestre = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{semestre}"
        return periodo_str
    elif ' Q' in periodo_str:
        # Formato: "2024 Q1", "2024 Q2", etc. â†’ retornar "2024/1", "2024/2", etc.
        partes = periodo_str.split(' Q')
        if len(partes) == 2:
            ano_str = partes[0].strip()
            quarter = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{quarter}"
        return periodo_str
    else:
        # Formato normal: "Setembro 2024" ou "setembro 2024"
        if ' ' in periodo_str:
            partes = periodo_str.split(' ', 1)
            mes_nome = partes[0].lower().strip()
            if len(partes) > 1:
                ano_str = partes[1].strip()
                # Tentar extrair ano (pode ser apenas nÃºmero)
                if ano_str.isdigit():
                    ano_extraido = int(ano_str)
                # Se nÃ£o for apenas nÃºmero, tentar extrair primeiro nÃºmero encontrado
                elif any(c.isdigit() for c in ano_str):
                    # Extrair primeiro sequÃªncia de dÃ­gitos
                    numero_str = ''.join([c for c in ano_str if c.isdigit()])[:4]  # Limitar a 4 dÃ­gitos
                    if numero_str:
                        ano_extraido = int(numero_str)
            
            # Obter abreviaÃ§Ã£o do mÃªs
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
        else:
            mes_nome = periodo_str.lower().strip()
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
    
    # Usar ano fornecido como parÃ¢metro ou o extraÃ­do
    if ano is not None:
        ano_final = ano
    elif ano_extraido is not None:
        ano_final = ano_extraido
    else:
        ano_final = None
    
    # Formatar resultado
    if mes_abrev:
        if ano_final:
            # Usar Ãºltimos 2 dÃ­gitos para meses normais
            ano_abrev = str(ano_final)[-2:]
            return f"{mes_abrev}/{ano_abrev}"
        else:
            return mes_abrev
    else:
        return periodo_str

def reordenar_colunas_padrao(colunas_numericas):
    """Reordena colunas numÃ©ricas na ordem padrÃ£o: BUD, Flex Bud - BUD, Flex BUD, Total - Flex Bud, Total, Total / Flex Bud"""
    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
    colunas_ordenadas = []
    for col_ordem in ordem_colunas:
        if col_ordem in colunas_numericas:
            colunas_ordenadas.append(col_ordem)
    # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
    for col in colunas_numericas:
        if col not in colunas_ordenadas:
            colunas_ordenadas.append(col)
    return colunas_ordenadas

def reorganizar_colunas_por_periodo(df_tabela_flex, periodos_selecionados, tipo_visualizacao):
    """Reorganiza a tabela para mostrar colunas por perÃ­odo na ordem de seleÃ§Ã£o"""
    if len(periodos_selecionados) <= 1 or 'PerÃ­odo' not in df_tabela_flex.columns:
        # Se hÃ¡ apenas 1 perÃ­odo ou nÃ£o hÃ¡ coluna PerÃ­odo, retornar tabela original
        return df_tabela_flex
    
    # Manter a ordem de seleÃ§Ã£o dos perÃ­odos
    periodos_ordenados = periodos_selecionados.copy()
    
    # Criar lista de colunas na ordem especificada
    colunas_finais = []
    
    # Colunas de identificaÃ§Ã£o (Type 05, Type 06, Account, etc.)
    colunas_id = []
    for col in ['Type 05', 'Type 06', 'Account', 'Custo']:
        if col in df_tabela_flex.columns:
            colunas_id.append(col)
    
    colunas_finais.extend(colunas_id)
    
    # Para cada perÃ­odo na ordem de seleÃ§Ã£o
    primeiro_periodo = periodos_ordenados[0]
    primeiro_periodo_abrev = formatar_periodo_abreviado(primeiro_periodo)
    
    # Primeiro perÃ­odo: Total, Flex (removendo coluna redundante "Flex - Total")
    colunas_finais.append(f"{primeiro_periodo_abrev}")
    colunas_finais.append(f"Flex {primeiro_periodo_abrev.lower()}")
    
    # Demais perÃ­odos: PerÃ­odo - Flex primeiro, PerÃ­odo, % PerÃ­odo/Flex primeiro
    for periodo in periodos_ordenados[1:]:
        periodo_abrev = formatar_periodo_abreviado(periodo)
        colunas_finais.append(f"{periodo_abrev} - Flex {primeiro_periodo_abrev.lower()}")
        colunas_finais.append(f"{periodo_abrev.lower()}")
        colunas_finais.append(f"% {periodo_abrev.lower()}/flex {primeiro_periodo_abrev.lower()}")
    
    # Criar DataFrame pivot por perÃ­odo
    # Primeiro, precisamos ter os dados separados por perÃ­odo
    # Vou criar uma estrutura que agrupa por categoria e perÃ­odo
    colunas_agrupamento = [col for col in ['Type 05', 'Type 06', 'Account', 'Custo'] if col in df_tabela_flex.columns]
    
    # Se nÃ£o houver dados separados por perÃ­odo, retornar tabela original
    if 'PerÃ­odo' not in df_tabela_flex.columns or df_tabela_flex['PerÃ­odo'].nunique() <= 1:
        return df_tabela_flex
    
    # Criar pivot table com perÃ­odos como colunas
    df_pivot = df_tabela_flex.pivot_table(
        index=colunas_agrupamento if colunas_agrupamento else ['Type 06'],
        columns='PerÃ­odo',
        values=['Total', 'Flex BUD', 'BUD'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Flatten column names
    df_pivot.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else str(col) for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Reorganizar colunas conforme especificado
    # Por enquanto, retornar a estrutura pivot bÃ¡sica
    # A reorganizaÃ§Ã£o completa serÃ¡ feita na exibiÃ§Ã£o
    return df_pivot

def calcular_resumo_tabela_flex(df_original, tipo_visualizacao, moeda_simbolo, fator_conversao=None):
    """Calcula linha de resumo (totais) para tabela Flex Bud
    
    Args:
        df_original: DataFrame com valores numÃ©ricos originais (antes da formataÃ§Ã£o)
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        moeda_simbolo: SÃ­mbolo da moeda (R$, $, â‚¬)
        fator_conversao: Fator de conversÃ£o opcional (K, M)
    
    Returns:
        DicionÃ¡rio com valores de resumo formatados (valores numÃ©ricos e formatados)
    """
    linha_resumo = {}
    linha_resumo_formatado = {}
    
    # Primeira coluna: "TOTAL"
    primeira_col = df_original.columns[0]
    linha_resumo[primeira_col] = "**TOTAL**"
    linha_resumo_formatado[primeira_col] = "**TOTAL**"
    
    # ðŸ”§ CORREÃ‡ÃƒO: Para CPU, recalcular usando valores em Custo Total se disponÃ­veis
    # (mesma lÃ³gica do grÃ¡fico - nÃ£o somar valores em CPU diretamente)
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        # Verificar se temos colunas auxiliares para recalcular corretamente
        if '_Flex_Bud_Total' in df_original.columns and '_Total_Custo_Total' in df_original.columns and '_Volume_Real' in df_original.columns:
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: O grÃ¡fico calcula Flex Bud por perÃ­odo (sem categoria)
            # Quando hÃ¡ mÃºltiplos perÃ­odos, o grÃ¡fico mostra cada perÃ­odo separadamente
            # Mas o valor total que o usuÃ¡rio vÃª Ã© a soma de Flex Bud Total de TODOS os perÃ­odos e categorias
            # dividido pela soma dos volumes de todos os perÃ­odos
            
            # Somar TODAS as categorias e perÃ­odos (mesma lÃ³gica do grÃ¡fico)
            flex_bud_total_custo = df_original['_Flex_Bud_Total'].sum()  # Soma de TODAS as categorias e perÃ­odos
            total_custo_total = df_original['_Total_Custo_Total'].sum()  # Soma de TODAS as categorias e perÃ­odos
            
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: _Volume_Real contÃ©m o volume total por perÃ­odo (nÃ£o por categoria)
            # Quando hÃ¡ mÃºltiplos perÃ­odos agregados, todos os volumes sÃ£o iguais (volume total de todos os perÃ­odos)
            # IMPORTANTE: Este valor jÃ¡ Ã© a SOMA dos volumes de todos os perÃ­odos (calculado na linha 4668)
            # EntÃ£o devemos usar o primeiro valor (todos sÃ£o iguais)
            # ðŸ”§ CORREÃ‡ÃƒO: Obter volume real corretamente
            volumes_reais = df_original['_Volume_Real'].dropna()
            if len(volumes_reais) > 0:
                # Quando hÃ¡ mÃºltiplos perÃ­odos agregados, todos os volumes sÃ£o iguais (volume total de todos os perÃ­odos)
                # Usar o primeiro valor (todos sÃ£o iguais)
                volume_total_real = float(volumes_reais.iloc[0]) if len(volumes_reais) > 0 else 0.0
            else:
                volume_total_real = 0.0
            
            # Recalcular CPU a partir dos totais (mesma lÃ³gica do grÃ¡fico)
            # Flex BUD CPU Total = (Soma de Flex Bud Total de todas as categorias) / (Volume Total)
            flex_bud_cpu = flex_bud_total_custo / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            total_cpu = total_custo_total / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            
            # Calcular BUD tambÃ©m
            volume_total_budget = 0  # Inicializar
            if '_Budget_Total' in df_original.columns and '_Volume_Budget' in df_original.columns:
                budget_total_custo = df_original['_Budget_Total'].sum()  # Soma de TODAS as categorias
                # Mesma lÃ³gica para volume de budget
                # ðŸ”§ CORREÃ‡ÃƒO: Obter volume budget corretamente
                volumes_budget = df_original['_Volume_Budget'].dropna()
                if len(volumes_budget) > 0:
                    # Quando hÃ¡ mÃºltiplos perÃ­odos agregados, todos os volumes sÃ£o iguais (volume total de todos os perÃ­odos)
                    # Usar o primeiro valor (todos sÃ£o iguais)
                    volume_total_budget = float(volumes_budget.iloc[0]) if len(volumes_budget) > 0 else 0.0
                else:
                    volume_total_budget = 0.0
                bud_cpu = budget_total_custo / volume_total_budget if volume_total_budget != 0 and pd.notnull(volume_total_budget) else 0
            else:
                # Se nÃ£o tiver colunas auxiliares, usar soma direta
                bud_cpu = df_original['BUD'].sum() if 'BUD' in df_original.columns else 0
                volume_total_budget = 0  # NÃ£o temos volume de budget disponÃ­vel
            
            linha_resumo['Flex BUD'] = flex_bud_cpu
            linha_resumo['Total'] = total_cpu
            linha_resumo['BUD'] = bud_cpu
            linha_resumo['Flex Bud - BUD'] = flex_bud_cpu - bud_cpu
            linha_resumo['Total - Flex Bud'] = total_cpu - flex_bud_cpu
            
            # ðŸ”§ ADICIONAR: Incluir volumes usados nos cÃ¡lculos (apenas para resumo geral)
            linha_resumo['_Volume_Real_Calculo'] = volume_total_real
            linha_resumo['_Volume_Budget_Calculo'] = volume_total_budget
            
            # FormataÃ§Ã£o
            linha_resumo_formatado['Flex BUD'] = f"{flex_bud_cpu:,.2f}"
            linha_resumo_formatado['Total'] = f"{total_cpu:,.2f}"
            linha_resumo_formatado['BUD'] = f"{bud_cpu:,.2f}"
            linha_resumo_formatado['Flex Bud - BUD'] = f"{flex_bud_cpu - bud_cpu:,.2f}"
            linha_resumo_formatado['Total - Flex Bud'] = f"{total_cpu - flex_bud_cpu:,.2f}"
            # ðŸ”§ ADICIONAR: Formatar volumes usados nos cÃ¡lculos (sem casas decimais)
            linha_resumo_formatado['_Volume_Real_Calculo'] = f"{volume_total_real:,.0f}"
            linha_resumo_formatado['_Volume_Budget_Calculo'] = f"{volume_total_budget:,.0f}"
        else:
            # Se nÃ£o tiver colunas auxiliares, somar diretamente (comportamento antigo)
            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                if col in df_original.columns:
                    soma = df_original[col].sum()
                    linha_resumo[col] = soma
                    linha_resumo_formatado[col] = f"{soma:,.2f}"
            
            # ðŸ”§ ADICIONAR: Tentar obter volumes mesmo sem colunas auxiliares (se disponÃ­veis)
            if '_Volume_Real' in df_original.columns:
                volumes_reais = df_original['_Volume_Real'].dropna()
                if len(volumes_reais) > 0:
                    volume_total_real = float(volumes_reais.iloc[0])
                else:
                    volume_total_real = 0.0
                linha_resumo['_Volume_Real_Calculo'] = volume_total_real
                linha_resumo_formatado['_Volume_Real_Calculo'] = f"{volume_total_real:,.0f}"
            else:
                linha_resumo['_Volume_Real_Calculo'] = 0
                linha_resumo_formatado['_Volume_Real_Calculo'] = "0"
            
            if '_Volume_Budget' in df_original.columns:
                volumes_budget = df_original['_Volume_Budget'].dropna()
                if len(volumes_budget) > 0:
                    volume_total_budget = float(volumes_budget.iloc[0])
                else:
                    volume_total_budget = 0.0
                linha_resumo['_Volume_Budget_Calculo'] = volume_total_budget
                linha_resumo_formatado['_Volume_Budget_Calculo'] = f"{volume_total_budget:,.0f}"
            else:
                linha_resumo['_Volume_Budget_Calculo'] = 0
                linha_resumo_formatado['_Volume_Budget_Calculo'] = "0"
    else:
        # Para Custo Total: apenas somar
        for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
            if col in df_original.columns:
                soma = df_original[col].sum()
                linha_resumo[col] = soma
                sufixo = ""
                if fator_conversao:
                    if fator_conversao == "K (milhares)":
                        sufixo = " K"
                    elif fator_conversao == "M (MilhÃµes)":
                        sufixo = " M"
                linha_resumo_formatado[col] = f"{soma:,.2f}{sufixo}"
    
    # Recalcular Total / Flex Bud
    if 'Total' in linha_resumo and 'Flex BUD' in linha_resumo:
        total_soma = linha_resumo['Total']
        flex_bud_soma = linha_resumo['Flex BUD']
        ratio_resumo = total_soma / flex_bud_soma if flex_bud_soma != 0 and pd.notnull(flex_bud_soma) else 0
        linha_resumo['Total / Flex Bud'] = ratio_resumo
        linha_resumo_formatado['Total / Flex Bud'] = ratio_resumo
    
    return linha_resumo, linha_resumo_formatado

def exibir_caixas_resumo_dinamico(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    """Exibe caixas de texto com valores de resumo usando nomes de colunas dinÃ¢micas (ex: Set/24, Flex set/24, etc.)
    
    Args:
        linha_resumo: DicionÃ¡rio com valores numÃ©ricos (usando nomes de colunas dinÃ¢micas)
        linha_resumo_formatado: DicionÃ¡rio com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget
    """
    # Obter colunas numÃ©ricas (excluindo volumes e colunas auxiliares)
    colunas_auxiliares = ['_Volume_Real_Calculo', '_Volume_Budget_Calculo']
    colunas_numericas = [col for col in linha_resumo.keys() if col not in colunas_auxiliares]
    
    # Ordenar colunas na ordem exata: Jul/25, Flex jul/25 - jul/25, Flex jul/25, Nov/25 - Flex jul/25, nov/25, % nov/25/flex jul/25
    # Detectar primeiro e segundo perÃ­odos
    primeiro_periodo = None
    segundo_periodo_maiuscula = None
    segundo_periodo_minuscula = None
    flex_primeiro_menos_primeiro = None
    flex_primeiro = None
    percentual = None
    
    # Primeiro, identificar todas as colunas
    for col in colunas_numericas:
        # Primeiro perÃ­odo: nÃ£o comeÃ§a com 'Flex' ou '%', nÃ£o tem '-', comeÃ§a com maiÃºscula
        if not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].isupper():
            primeiro_periodo = col
        # Flex primeiro - primeiro: comeÃ§a com 'Flex' e tem '-'
        elif col.startswith('Flex') and '-' in col:
            flex_primeiro_menos_primeiro = col
        # Flex primeiro: comeÃ§a com 'Flex' e nÃ£o tem '-'
        elif col.startswith('Flex') and '-' not in col:
            flex_primeiro = col
        # Segundo perÃ­odo maiÃºscula: nÃ£o comeÃ§a com 'Flex' ou '%', tem '-', comeÃ§a com maiÃºscula (ex: Nov/25 - Flex jul/25)
        elif '-' in col and not col.startswith('%') and not col.startswith('Flex') and len(col) > 0 and col[0].isupper():
            segundo_periodo_maiuscula = col
        # Segundo perÃ­odo minÃºscula: nÃ£o comeÃ§a com 'Flex' ou '%', nÃ£o tem '-', comeÃ§a com minÃºscula (ex: nov/25)
        elif not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].islower():
            segundo_periodo_minuscula = col
        # Percentual: comeÃ§a com '%'
        elif col.startswith('%'):
            percentual = col
    
    # Criar ordem explÃ­cita na ordem correta
    ordem_explicita = []
    
    # 1. Primeiro perÃ­odo (ex: Jul/25)
    if primeiro_periodo:
        ordem_explicita.append(primeiro_periodo)
    
    # 2. Flex primeiro - primeiro (ex: Flex jul/25 - jul/25)
    if flex_primeiro_menos_primeiro:
        ordem_explicita.append(flex_primeiro_menos_primeiro)
    
    # 3. Flex primeiro (ex: Flex jul/25)
    if flex_primeiro:
        ordem_explicita.append(flex_primeiro)
    
    # 4. Segundo perÃ­odo - Flex primeiro (ex: Nov/25 - Flex jul/25)
    if segundo_periodo_maiuscula:
        ordem_explicita.append(segundo_periodo_maiuscula)
    
    # 5. Segundo perÃ­odo minÃºscula (ex: nov/25)
    if segundo_periodo_minuscula:
        ordem_explicita.append(segundo_periodo_minuscula)
    
    # 6. Percentual (ex: % nov/25/flex jul/25)
    if percentual:
        ordem_explicita.append(percentual)
    
    # Se a ordem explÃ­cita nÃ£o capturou todas as colunas, adicionar as restantes no final
    colunas_restantes = [col for col in colunas_numericas if col not in ordem_explicita]
    ordem_explicita.extend(colunas_restantes)
    
    colunas_ordenadas = ordem_explicita
    
    # Exibir caixas (mÃ¡ximo 6 colunas principais)
    num_colunas = min(len(colunas_ordenadas), 6)
    if num_colunas > 0:
        cols = st.columns(num_colunas, gap="small")
        for idx, col_nome in enumerate(colunas_ordenadas[:num_colunas]):
            with cols[idx]:
                valor_formatado = linha_resumo_formatado.get(col_nome, '-')
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
    
    # Exibir volumes se solicitado
    if mostrar_volumes:
        volume_real_display = linha_resumo_formatado.get('_Volume_Real_Calculo', '-')
        volume_budget_display = linha_resumo_formatado.get('_Volume_Budget_Calculo', '-')
        
        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>", unsafe_allow_html=True)
        with col_vol2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>", unsafe_allow_html=True)

def exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    """Exibe caixas de texto com os valores de resumo (BUD, Flex BUD, Total, etc.) com fonte menor
    
    Args:
        linha_resumo: DicionÃ¡rio com valores numÃ©ricos
        linha_resumo_formatado: DicionÃ¡rio com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget usados nos cÃ¡lculos (apenas para resumo geral)
    """
    if mostrar_volumes:
        # Exibir com volumes (resumo geral) - valores principais
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")
        
        with col1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>", unsafe_allow_html=True)
        with col4:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>", unsafe_allow_html=True)
        with col5:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Total', '-')}</div>", unsafe_allow_html=True)
        with col6:
            ratio_valor = linha_resumo.get('Total / Flex Bud', 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                # Usar a funÃ§Ã£o formatar_ratio_com_barra para exibir a barra de percentual
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>", unsafe_allow_html=True)
        
        # EspaÃ§amento entre as caixas de texto e os volumes
        st.markdown("<br>", unsafe_allow_html=True)
        
        # ðŸ”§ ADICIONAR: Exibir volumes abaixo da linha de valores
        # Tentar obter volumes do dicionÃ¡rio formatado primeiro
        volume_real_display = linha_resumo_formatado.get('_Volume_Real_Calculo', None)
        volume_budget_display = linha_resumo_formatado.get('_Volume_Budget_Calculo', None)
        
        # Se os volumes nÃ£o estiverem formatados, tentar obter do dicionÃ¡rio numÃ©rico e formatar
        if volume_real_display is None or volume_real_display == '-':
            if '_Volume_Real_Calculo' in linha_resumo:
                volume_real_valor = linha_resumo['_Volume_Real_Calculo']
                if isinstance(volume_real_valor, (int, float)) and not pd.isna(volume_real_valor) and volume_real_valor != 0:
                    volume_real_display = f"{volume_real_valor:,.0f}"
                else:
                    volume_real_display = '-'
            else:
                volume_real_display = '-'
        
        if volume_budget_display is None or volume_budget_display == '-':
            if '_Volume_Budget_Calculo' in linha_resumo:
                volume_budget_valor = linha_resumo['_Volume_Budget_Calculo']
                if isinstance(volume_budget_valor, (int, float)) and not pd.isna(volume_budget_valor) and volume_budget_valor != 0:
                    volume_budget_display = f"{volume_budget_valor:,.0f}"
                else:
                    volume_budget_display = '-'
            else:
                volume_budget_display = '-'
        
        # Exibir volumes sempre que mostrar_volumes=True (mesmo padrÃ£o das caixas acima, com valor na frente na mesma linha)
        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>", unsafe_allow_html=True)
        with col_vol2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>", unsafe_allow_html=True)
    else:
        # Exibir sem volumes (resumos de categorias)
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")
        
        with col1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>", unsafe_allow_html=True)
        with col4:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>", unsafe_allow_html=True)
        with col5:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Total', '-')}</div>", unsafe_allow_html=True)
        with col6:
            ratio_valor = linha_resumo.get('Total / Flex Bud', 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                # Usar a funÃ§Ã£o formatar_ratio_com_barra para exibir a barra de percentual
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>", unsafe_allow_html=True)

def formatar_ratio_para_tabela(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com indicador visual para tabelas"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular nÃºmero de barras: 100% = barra cheia (10 barras)
    if percentual >= 100:
        num_barras = 10  # Barra cheia para 100% ou mais
    else:
        num_barras = int(percentual / 10)  # Proporcional atÃ© 100%
    
    # Criar barra visual com gradiente verde->vermelho usando emojis coloridos
    # Usar caracteres Unicode para criar efeito de gradiente
    barras_preenchidas = num_barras
    barras_vazias = 10 - num_barras
    
    # Para valores acima de 100%, mostrar barra cheia
    if percentual >= 100:
        barra = "â–ˆ" * 10
    else:
        barra = "â–ˆ" * barras_preenchidas + "â–‘" * barras_vazias
    
    return f"{percentual:.1f}% {barra}"

def ordenar_por_mes(df, coluna_periodo='PerÃ­odo'):
    """Ordena DataFrame por ordem cronolÃ³gica dos meses, considerando ano se disponÃ­vel"""
    df_copy = df.copy()
    
    # Se houver coluna "Ano", sempre ordenar por ano e mÃªs (mesmo que haja apenas um ano)
    # Isso garante que quando "Todos" estÃ¡ selecionado, todos os perÃ­odos sejam mostrados ordenados
    if 'Ano' in df_copy.columns:
        # Criar coluna de ordenaÃ§Ã£o: ano primeiro, depois mÃªs
        df_copy['_ordem_ano'] = df_copy['Ano']
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
        df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
    else:
        # OrdenaÃ§Ã£o simples por mÃªs (comportamento original quando nÃ£o hÃ¡ coluna Ano)
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values('_ordem_mes')
        df_copy = df_copy.drop(columns=['_ordem_mes'])
    
    return df_copy


# FunÃ§Ã£o para calcular FLEX de volume comparando dados reais vs budget
def calcular_flex_budget(df_real, df_real_vol, df_budget, df_budget_vol, tipo_viz, tem_ano):
    """
    Calcula FLEX de volume comparando dados reais vs budget.
    
    Regra:
    - Custo Fixo: sensibilidade = 0 (nÃ£o varia)
    - Custo VariÃ¡vel: sensibilidade = 1 (varia 100% do volume)
    
    FÃ³rmula:
    - ProporÃ§Ã£o_Volume = Volume_Budget / Volume_Real
    - VariaÃ§Ã£o_% = ProporÃ§Ã£o_Volume - 1.0
    - FLEX_Fixo = Custo_Fixo_Real Ã— VariaÃ§Ã£o_% Ã— 0 = 0
    - FLEX_VariÃ¡vel = Custo_VariÃ¡vel_Real Ã— VariaÃ§Ã£o_% Ã— 1
    - FLEX_Total = FLEX_Fixo + FLEX_VariÃ¡vel
    
    Para CPU:
    - FLEX_CPU = FLEX_Total / Volume_Real
    
    Retorna DataFrame com colunas: Ano (se tem_ano), PerÃ­odo, FLEX, Budget_Total (valores originais do budget)
    """
    try:
        if df_budget is None or df_real is None:
            return None
        
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Normalizar perÃ­odos em todos os DataFrames ANTES de agrupar
        # Mapear meses para formato capitalizado (primeira letra maiÃºscula) - MESMA LÃ“GICA DO NOTEBOOK
        mapeamento_meses = {
            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
        }
        
        def normalizar_periodo(periodo):
            """Normaliza perÃ­odo para formato capitalizado"""
            if pd.isna(periodo):
                return periodo
            periodo_str = str(periodo).strip()
            for mes_min, mes_cap in mapeamento_meses.items():
                if periodo_str.lower() == mes_min.lower():
                    return mes_cap
            return periodo_str  # Retornar original se nÃ£o for um mÃªs conhecido
        
        # Normalizar perÃ­odos em todos os DataFrames
        if 'PerÃ­odo' in df_real.columns:
            df_real = df_real.copy()
            df_real['PerÃ­odo'] = df_real['PerÃ­odo'].apply(normalizar_periodo)
        if 'Custo' in df_real.columns:
            df_real = df_real.copy()
            df_real['Custo'] = df_real['Custo'].apply(_normalizar_rotulo_custo)
        if df_real_vol is not None and 'PerÃ­odo' in df_real_vol.columns:
            df_real_vol = df_real_vol.copy()
            df_real_vol['PerÃ­odo'] = df_real_vol['PerÃ­odo'].apply(normalizar_periodo)
        if 'PerÃ­odo' in df_budget.columns:
            df_budget = df_budget.copy()
            df_budget['PerÃ­odo'] = df_budget['PerÃ­odo'].apply(normalizar_periodo)
        if 'Custo' in df_budget.columns:
            df_budget = df_budget.copy()
            df_budget['Custo'] = df_budget['Custo'].apply(_normalizar_rotulo_custo)
        if df_budget_vol is not None and 'PerÃ­odo' in df_budget_vol.columns:
            df_budget_vol = df_budget_vol.copy()
            df_budget_vol['PerÃ­odo'] = df_budget_vol['PerÃ­odo'].apply(normalizar_periodo)
        
        # Agrupar dados reais por perÃ­odo
        if tem_ano:
            # Agrupar por Ano e PerÃ­odo
            if 'Custo' in df_real.columns and 'Total' in df_real.columns:
                real_agrupado = df_real.groupby(['Ano', 'PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
            else:
                return None
            
            # Agrupar budget por perÃ­odo
            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['Ano', 'PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
            else:
                return None
            
            # ðŸ”§ CORREÃ‡ÃƒO: Normalizar perÃ­odos antes do merge para garantir correspondÃªncia
            # Normalizar perÃ­odos para string e remover espaÃ§os extras (jÃ¡ normalizados acima, mas garantir)
            real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
            budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
            real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
            budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os perÃ­odos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on=['Ano', 'PerÃ­odo'],
                how='outer',  # ðŸ”§ MUDANÃ‡A: usar outer para ver todos os perÃ­odos
                suffixes=('_real', '_budget')
            )
            
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: NÃƒO filtrar perÃ­odos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando nÃ£o houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para perÃ­odos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas perÃ­odos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada perÃ­odo
            flex_data = []
            for _, vol_row in volumes.iterrows():
                ano = vol_row['Ano']
                periodo = vol_row['PerÃ­odo']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporÃ§Ã£o de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este perÃ­odo
                custos_real = real_agrupado[
                    (real_agrupado['Ano'] == ano) & 
                    (real_agrupado['PerÃ­odo'] == periodo)
                ]
                
                # Obter valores originais do budget para este perÃ­odo
                custos_budget = budget_agrupado[
                    (budget_agrupado['Ano'] == ano) & 
                    (budget_agrupado['PerÃ­odo'] == periodo)
                ]
                
                # ðŸ”§ CORREÃ‡ÃƒO: Se nÃ£o encontrar budget para este perÃ­odo, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                else:
                    budget_total = custos_budget['Total'].sum()
                    mask_fixo = _mask_custo_fixo(custos_budget['Custo']) if 'Custo' in custos_budget.columns else pd.Series(False, index=custos_budget.index)
                    custo_fixo_budget = custos_budget.loc[mask_fixo, 'Total'].sum()

                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA (Flex): NÃ£o ignorar categorias fora de 'VariÃ¡vel'.
                # Regra: tudo que NÃƒO Ã© Fixo Ã© flexÃ­vel (escala com Volume Real/Budget).
                custo_nao_fixo_budget = budget_total - custo_fixo_budget
                
                # ðŸ”§ NOVO: Se nÃ£o houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Total'].sum()
                
                # NOTA: A conversÃ£o de moeda jÃ¡ foi aplicada no df_budget (linha ~2563)
                # Portanto, budget_total e custo_fixo_budget (e o nÃ£o-fixo derivado) jÃ¡ estÃ£o convertidos
                
                # ProporÃ§Ã£o para flexionar o componente nÃ£o-fixo
                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)

                if tipo_viz == "CPU (Custo por Unidade)":
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Flex Bud Total (Custo Total)
                    flex_valor = flex_bud_total_custo_total
                    budget_valor = budget_total
                
                flex_data.append({
                    'Ano': ano,
                    'PerÃ­odo': periodo,
                    'FLEX': flex_valor,  # Agora contÃ©m Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
        else:
            # Sem coluna Ano: agrupar apenas por PerÃ­odo
            # (PerÃ­odos jÃ¡ foram normalizados acima)
            if 'Custo' in df_real.columns and 'Total' in df_real.columns:
                real_agrupado = df_real.groupby(['PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
            else:
                return None
            
            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
            else:
                return None
            
            # ðŸ”§ CORREÃ‡ÃƒO: Normalizar perÃ­odos antes do merge para garantir correspondÃªncia
            # Normalizar perÃ­odos para string e remover espaÃ§os extras (jÃ¡ normalizados acima, mas garantir)
            real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
            budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
            real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
            budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os perÃ­odos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on='PerÃ­odo',
                how='outer',  # ðŸ”§ MUDANÃ‡A: usar outer para ver todos os perÃ­odos
                suffixes=('_real', '_budget')
            )
            
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: NÃƒO filtrar perÃ­odos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando nÃ£o houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para perÃ­odos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas perÃ­odos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada perÃ­odo
            flex_data = []
            for _, vol_row in volumes.iterrows():
                periodo = vol_row['PerÃ­odo']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporÃ§Ã£o de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este perÃ­odo
                custos_real = real_agrupado[real_agrupado['PerÃ­odo'] == periodo]
                
                # Obter valores originais do budget para este perÃ­odo
                custos_budget = budget_agrupado[budget_agrupado['PerÃ­odo'] == periodo]
                
                # ðŸ”§ CORREÃ‡ÃƒO: Se nÃ£o encontrar budget para este perÃ­odo, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                else:
                    budget_total = custos_budget['Total'].sum()
                    mask_fixo = _mask_custo_fixo(custos_budget['Custo']) if 'Custo' in custos_budget.columns else pd.Series(False, index=custos_budget.index)
                    custo_fixo_budget = custos_budget.loc[mask_fixo, 'Total'].sum()

                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA (Flex): NÃ£o ignorar categorias fora de 'VariÃ¡vel'.
                # Regra: tudo que NÃƒO Ã© Fixo Ã© flexÃ­vel (escala com Volume Real/Budget).
                custo_nao_fixo_budget = budget_total - custo_fixo_budget
                
                # ðŸ”§ NOVO: Se nÃ£o houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Total'].sum()
                
                # NOTA: A conversÃ£o de moeda jÃ¡ foi aplicada no df_budget (linha ~2550)
                # Portanto, budget_total e custo_fixo_budget (e o nÃ£o-fixo derivado) jÃ¡ estÃ£o convertidos
                
                # ProporÃ§Ã£o para flexionar o componente nÃ£o-fixo
                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)

                if tipo_viz == "CPU (Custo por Unidade)":
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Flex Bud Total (Custo Total)
                    flex_valor = flex_bud_total_custo_total
                    budget_valor = budget_total
                
                flex_data.append({
                    'PerÃ­odo': periodo,
                    'FLEX': flex_valor,  # Agora contÃ©m Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
    except Exception as e:
        st.sidebar.warning(f"âš ï¸ Erro ao calcular FLEX: {e}")
        return None


# GrÃ¡fico 1: Soma do Valor por PerÃ­odo
# Cache removido: DataFrames grandes podem causar problemas de hash
def create_period_chart(df_data, coluna, tipo_viz, df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, moeda_simbolo="R$", debug=False, debug_context=""):
    """Cria grÃ¡fico de barras por PerÃ­odo com linha pontilhada de FLEX (budget) opcional"""
    try:
        # Detectar tema para adaptar cores (dark/light mode)
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        
        # ValidaÃ§Ãµes iniciais
        if df_data is None or df_data.empty:
            st.warning("âš ï¸ Dados vazios ou None passados para o grÃ¡fico")
            return None
        
        if 'PerÃ­odo' not in df_data.columns:
            st.warning(f"âš ï¸ Coluna 'PerÃ­odo' nÃ£o encontrada. Colunas disponÃ­veis: {list(df_data.columns)[:10]}")
            return None

        # ==============================
        # DEBUG: auditoria de Flex/Volume
        # ==============================
        if debug and df_budget is not None and df_budget_vol is not None and df_real_vol is not None:
            try:
                with st.expander(f"ðŸ› ï¸ Debug Flex/Volume {debug_context}".strip(), expanded=False):
                    st.caption("Valida: (1) Flex BUD vs BUD, (2) Volume Real vs Budget, (3) por Oficina. Usa o mesmo recorte do grÃ¡fico.")

                    # ðŸ”§ IMPORTANTE: nÃ£o filtrar volume por "recorte do custo".
                    # Isso pode remover veÃ­culos/oficinas que nÃ£o aparecem no realizado (custo),
                    # mas existem no volume, gerando volume total errado.
                    df_real_vol_dbg = df_real_vol.copy()
                    df_budget_vol_dbg = df_budget_vol.copy()
                    df_budget_dbg = df_budget.copy()

                    # Normalizar PerÃ­odo para evitar mismatch bobo de merge
                    for _df in [df_budget_dbg, df_real_vol_dbg, df_budget_vol_dbg]:
                        if _df is not None and len(_df) > 0 and 'PerÃ­odo' in _df.columns:
                            _df['PerÃ­odo'] = _df['PerÃ­odo'].astype(str).str.strip()

                    tem_ano = 'Ano' in df_budget_dbg.columns and 'Ano' in df_real_vol_dbg.columns and 'Ano' in df_budget_vol_dbg.columns
                    chaves = ['PerÃ­odo']
                    if tem_ano:
                        chaves = ['Ano', 'PerÃ­odo']

                    # Custos Budget por perÃ­odo
                    if 'Total' not in df_budget_dbg.columns:
                        st.warning("Debug: df_budget nÃ£o tem coluna 'Total'.")
                    else:
                        bud_total = df_budget_dbg.groupby(chaves)['Total'].sum().reset_index().rename(columns={'Total': 'BUD_Total'})
                        bud_fixo = df_budget_dbg[df_budget_dbg.get('Custo', '').astype(str) == 'Fixo'].groupby(chaves)['Total'].sum().reset_index().rename(columns={'Total': 'BUD_Fixo'})
                        df_dbg = bud_total.merge(bud_fixo, on=chaves, how='left')
                        df_dbg['BUD_Fixo'] = df_dbg['BUD_Fixo'].fillna(0.0)
                        df_dbg['BUD_NaoFixo'] = df_dbg['BUD_Total'] - df_dbg['BUD_Fixo']

                        # Volumes por perÃ­odo
                        vol_real = df_real_vol_dbg.groupby(chaves)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Real'})
                        vol_bud = df_budget_vol_dbg.groupby(chaves)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Budget'})
                        df_dbg = df_dbg.merge(vol_real, on=chaves, how='left').merge(vol_bud, on=chaves, how='left')
                        df_dbg['Volume_Real'] = df_dbg['Volume_Real'].fillna(0.0)
                        df_dbg['Volume_Budget'] = df_dbg['Volume_Budget'].fillna(0.0)

                        df_dbg['Proporcao_Real_Bud'] = (df_dbg['Volume_Real'] / df_dbg['Volume_Budget'].replace(0, 1)).fillna(1.0)
                        df_dbg['Flex_BUD_CustoTotal'] = df_dbg['BUD_Fixo'] + (df_dbg['BUD_NaoFixo'] * df_dbg['Proporcao_Real_Bud'])
                        df_dbg['Flex_minus_BUD'] = df_dbg['Flex_BUD_CustoTotal'] - df_dbg['BUD_Total']

                        # Totais
                        st.markdown("**Totais do recorte (somatÃ³rio por perÃ­odo)**")
                        total_real_recorte = None
                        cpu_real_recorte = None
                        try:
                            if df_data is not None and len(df_data) > 0 and 'Total' in df_data.columns:
                                total_real_recorte = float(pd.to_numeric(df_data['Total'], errors='coerce').fillna(0).sum())
                                vol_real_recorte = float(pd.to_numeric(df_dbg['Volume_Real'], errors='coerce').fillna(0).sum())
                                cpu_real_recorte = (total_real_recorte / vol_real_recorte) if vol_real_recorte not in (0, None) else 0.0
                        except Exception:
                            total_real_recorte = None
                            cpu_real_recorte = None
                        st.write({
                            'BUD_Total': float(df_dbg['BUD_Total'].sum()),
                            'Flex_BUD_CustoTotal': float(df_dbg['Flex_BUD_CustoTotal'].sum()),
                            'Dif_Flex_minus_BUD': float(df_dbg['Flex_minus_BUD'].sum()),
                            'Volume_Real': float(df_dbg['Volume_Real'].sum()),
                            'Volume_Budget': float(df_dbg['Volume_Budget'].sum()),
                            'Real_Total': total_real_recorte,
                            'Real_CPU_(Total/VolReal)': cpu_real_recorte,
                        })

                        # Mostrar por perÃ­odo (evidencia distribuiÃ§Ã£o diferente)
                        st.markdown("**Por perÃ­odo (BUD vs Flex e volumes)**")
                        cols_show = chaves + ['BUD_Total', 'BUD_Fixo', 'BUD_NaoFixo', 'Volume_Real', 'Volume_Budget', 'Proporcao_Real_Bud', 'Flex_BUD_CustoTotal', 'Flex_minus_BUD']
                        st.dataframe(df_dbg[cols_show].sort_values(chaves), width="stretch")

                        # Por oficina (se existir nas bases)
                        if 'Oficina' in df_budget_dbg.columns and 'Oficina' in df_real_vol_dbg.columns and 'Oficina' in df_budget_vol_dbg.columns:
                            chaves_of = chaves + ['Oficina']
                            bud_total_of = df_budget_dbg.groupby(chaves_of)['Total'].sum().reset_index().rename(columns={'Total': 'BUD_Total'})
                            bud_fixo_of = df_budget_dbg[df_budget_dbg.get('Custo', '').astype(str) == 'Fixo'].groupby(chaves_of)['Total'].sum().reset_index().rename(columns={'Total': 'BUD_Fixo'})
                            dbg_of = bud_total_of.merge(bud_fixo_of, on=chaves_of, how='left')
                            dbg_of['BUD_Fixo'] = dbg_of['BUD_Fixo'].fillna(0.0)
                            dbg_of['BUD_NaoFixo'] = dbg_of['BUD_Total'] - dbg_of['BUD_Fixo']
                            vol_real_of = df_real_vol_dbg.groupby(chaves_of)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Real'})
                            vol_bud_of = df_budget_vol_dbg.groupby(chaves_of)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Budget'})
                            dbg_of = dbg_of.merge(vol_real_of, on=chaves_of, how='left').merge(vol_bud_of, on=chaves_of, how='left')
                            dbg_of['Volume_Real'] = dbg_of['Volume_Real'].fillna(0.0)
                            dbg_of['Volume_Budget'] = dbg_of['Volume_Budget'].fillna(0.0)
                            dbg_of['Proporcao_Real_Bud'] = (dbg_of['Volume_Real'] / dbg_of['Volume_Budget'].replace(0, 1)).fillna(1.0)
                            dbg_of['Flex_BUD_CustoTotal'] = dbg_of['BUD_Fixo'] + (dbg_of['BUD_NaoFixo'] * dbg_of['Proporcao_Real_Bud'])
                            dbg_of['Flex_minus_BUD'] = dbg_of['Flex_BUD_CustoTotal'] - dbg_of['BUD_Total']

                            # Agregar por oficina (somando perÃ­odos) e ordenar pelos maiores gaps
                            agg_cols = ['BUD_Total', 'Flex_BUD_CustoTotal', 'Flex_minus_BUD', 'Volume_Real', 'Volume_Budget']
                            dbg_of_tot = dbg_of.groupby('Oficina')[agg_cols].sum().reset_index()
                            dbg_of_tot = dbg_of_tot.sort_values('Flex_minus_BUD', key=lambda s: s.abs(), ascending=False)
                            st.markdown("**Por oficina (maiores diferenÃ§as)**")
                            st.dataframe(dbg_of_tot.head(50), width="stretch")

                        # DiagnÃ³stico: quais categorias estÃ£o vindo no Budget
                        if 'Custo' in df_budget_dbg.columns:
                            st.markdown("**Budget por categoria Custo (para ver 'Outros')**")
                            df_cat = df_budget_dbg.groupby('Custo')['Total'].sum().reset_index().sort_values('Total', ascending=False)
                            st.dataframe(df_cat, width="stretch")
            except Exception as _e:
                st.warning(f"Debug Flex falhou: {_e}")
        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns):
                st.warning(f"âš ï¸ Coluna necessÃ¡ria nÃ£o encontrada: {coluna}")
                st.warning(f"âš ï¸ Colunas disponÃ­veis: {list(df_data.columns)[:10]}")
                return None

        # Verificar se hÃ¡ coluna Ano - sempre mostrar ano junto com perÃ­odo quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e PerÃ­odo (sempre que houver coluna Ano)
            # IMPORTANTE: Sempre agrupar por Ano e PerÃ­odo para garantir consistÃªncia
            # independentemente de "Todos" estar selecionado ou um ano especÃ­fico
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                # Verificar se temos Total e Volume, ou se precisamos usar CPU existente
                if 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÃ“GICA DA TABELA: Agrupar por Ano e PerÃ­odo, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessÃ¡rias para o agrupamento
                    colunas_agrupamento = ['Ano', 'PerÃ­odo']
                    chart_data = df_data[colunas_agrupamento + ['Total', 'Volume']].groupby(colunas_agrupamento).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lÃ³gica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU jÃ¡ existe mas temos Total e Volume, recalcular
                    colunas_agrupamento = ['Ano', 'PerÃ­odo']
                    chart_data = df_data[colunas_agrupamento + ['Total', 'Volume']].groupby(colunas_agrupamento).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                else:
                    # Fallback: agrupar apenas por Ano e PerÃ­odo
                    chart_data = df_data.groupby(['Ano', 'PerÃ­odo'])[coluna].sum().reset_index()
            else:
                # Para Custo Total, tambÃ©m agrupar por Ano e PerÃ­odo para garantir consistÃªncia
                # Otimizar: usar apenas as colunas necessÃ¡rias
                chart_data = df_data[['Ano', 'PerÃ­odo', coluna]].groupby(['Ano', 'PerÃ­odo'])[coluna].sum().reset_index()
            
            # Criar coluna combinada para o rÃ³tulo do grÃ¡fico
            chart_data['PerÃ­odo_Completo'] = chart_data['PerÃ­odo'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mÃªs
            chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
            ordem_periodos = chart_data['PerÃ­odo_Completo'].tolist()
            
            # Usar PerÃ­odo_Completo no grÃ¡fico
            coluna_periodo_grafico = 'PerÃ­odo_Completo'
        else:
            # Comportamento original: agrupar apenas por PerÃ­odo (quando nÃ£o hÃ¡ coluna Ano)
            # Para CPU, usar EXATAMENTE a mesma lÃ³gica da tabela (que estÃ¡ correta)
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                if 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÃ“GICA DA TABELA: Agrupar por PerÃ­odo, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessÃ¡rias
                    chart_data = df_data[['PerÃ­odo', 'Total', 'Volume']].groupby('PerÃ­odo').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lÃ³gica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU jÃ¡ existe mas temos Total e Volume, recalcular
                    # Otimizar: usar apenas as colunas necessÃ¡rias
                    chart_data = df_data[['PerÃ­odo', 'Total', 'Volume']].groupby('PerÃ­odo').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                else:
                    # Fallback: agrupar apenas por PerÃ­odo
                    chart_data = df_data[['PerÃ­odo', coluna]].groupby('PerÃ­odo')[coluna].sum().reset_index()
            else:
                # Otimizar: usar apenas as colunas necessÃ¡rias
                chart_data = df_data[['PerÃ­odo', coluna]].groupby('PerÃ­odo')[coluna].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
            ordem_periodos = chart_data['PerÃ­odo'].tolist()
            coluna_periodo_grafico = 'PerÃ­odo'

        # Definir tÃ­tulo do eixo Y baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por PerÃ­odo"
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por PerÃ­odo"

        # Garantir que todos os perÃ­odos do Budget apareÃ§am (com realizado = 0)
        if df_budget is not None and not df_budget.empty and 'PerÃ­odo' in df_budget.columns:
            # Guardar perÃ­odos reais antes do reindex
            if tem_ano:
                periodos_reais_set = set(chart_data[['Ano', 'PerÃ­odo']].apply(tuple, axis=1))
            else:
                periodos_reais_set = set(chart_data['PerÃ­odo'].tolist())

            if tem_ano and 'Ano' in df_budget.columns:
                periodos_budget = df_budget[['Ano', 'PerÃ­odo']].dropna().drop_duplicates()
                index_full = pd.MultiIndex.from_frame(periodos_budget)
                chart_data = chart_data.set_index(['Ano', 'PerÃ­odo']).reindex(index_full).reset_index()
                # Zerar realizado quando nÃ£o hÃ¡ dado real
                mask_real = chart_data[['Ano', 'PerÃ­odo']].apply(tuple, axis=1).isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan
            else:
                periodos_budget = df_budget['PerÃ­odo'].dropna().drop_duplicates().tolist()
                chart_data = chart_data.set_index('PerÃ­odo').reindex(periodos_budget).reset_index()
                mask_real = chart_data['PerÃ­odo'].isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan

            # Preencher colunas numÃ©ricas com zero (sem usar budget)
            colunas_zero = [col for col in chart_data.columns if pd.api.types.is_numeric_dtype(chart_data[col]) and col != coluna]
            for col in colunas_zero:
                chart_data[col] = chart_data[col].fillna(0)

            # Reordenar apÃ³s completar perÃ­odos
            chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
            if tem_ano:
                chart_data['PerÃ­odo_Completo'] = chart_data['PerÃ­odo'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                ordem_periodos = chart_data['PerÃ­odo_Completo'].tolist()
                coluna_periodo_grafico = 'PerÃ­odo_Completo'
            else:
                ordem_periodos = chart_data['PerÃ­odo'].tolist()
                coluna_periodo_grafico = 'PerÃ­odo'

        # Validar se chart_data tem dados apÃ³s agrupamento e filtros
        if chart_data is None or chart_data.empty:
            st.warning("âš ï¸ Nenhum dado apÃ³s agrupamento. Verifique os filtros aplicados.")
            return None
            
        # Verificar se a coluna tem valores vÃ¡lidos
        if coluna not in chart_data.columns:
            st.warning(f"âš ï¸ Coluna '{coluna}' nÃ£o encontrada apÃ³s agrupamento. Colunas disponÃ­veis: {list(chart_data.columns)}")
            return None
            
        # Se houver volume real, zerar/ocultar realizado em perÃ­odos sem volume
        if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'PerÃ­odo' in df_real_vol.columns:
            if tem_ano and 'Ano' in df_real_vol.columns and 'Ano' in chart_data.columns:
                vol_agr = df_real_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on=['Ano', 'PerÃ­odo'], how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])
            else:
                vol_agr = df_real_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on='PerÃ­odo', how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])

        # NÃ£o cortar meses futuros aqui.
        # O grÃ¡fico deve refletir os dados disponÃ­veis (ex.: Budget/Forecast pode ter o ano completo).

        # Garantir que os valores sejam numÃ©ricos (preservar NaN para nÃ£o desenhar barras)
        chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce')
            
        # Verificar se hÃ¡ valores nÃ£o-nulos (apenas para Custo Total, CPU jÃ¡ filtra zeros)
        if tipo_viz != "CPU (Custo por Unidade)":
            valores_validos = chart_data[coluna].notna() & (chart_data[coluna] != 0)
            if not valores_validos.any():
                # NÃ£o bloquear, apenas avisar - pode haver valores muito pequenos apÃ³s conversÃ£o
                st.info(f"â„¹ï¸ Todos os valores na coluna '{coluna}' sÃ£o zero apÃ³s agrupamento. Mostrando grÃ¡fico mesmo assim.")
        
        # Verificar se chart_data estÃ¡ vazio
        if chart_data is None or chart_data.empty or len(chart_data) == 0:
            st.warning("âš ï¸ Nenhum dado disponÃ­vel apÃ³s agrupamento e filtros.")
            return None

        # Usar gradiente baseado no valor da coluna (como na figura 1)
        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='PerÃ­odo',
                sort=ordem_periodos,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
            color=alt.Color(
                f'{coluna}:Q',
                title=coluna,
                scale=alt.Scale(scheme='blues'),
                legend=alt.Legend(title=coluna, orient='right', titleFontSize=10, labelFontSize=9)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='PerÃ­odo'),
                alt.Tooltip(
                    f'{coluna}:Q',
                    title=coluna,
                    format=',.2f' if tipo_viz == "CPU (Custo por Unidade)"
                    else ',.2f'
                )
            ]
        ).properties(
            height=altura_grafico,
            width=900
        )

        # Adicionar rÃ³tulos com valores nas barras
        formato_rotulo = (
            ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
        )
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        ).transform_filter(
            (alt.datum[coluna] != None) & (alt.datum[coluna] != 0)
        )

        # Processar dados de budget e calcular FLEX se fornecidos
        linha_budget = None
        budget_data = None  # Inicializar para uso no grÃ¡fico de delta
        # IMPORTANTE: No modo CPU, df_data pode nÃ£o ter a coluna 'Custo' necessÃ¡ria para calcular FLEX
        # Usar df_real_original se disponÃ­vel, caso contrÃ¡rio usar df_data
        df_real_para_flex = df_real_original if df_real_original is not None else df_data
        
        # ðŸ”§ CORREÃ‡ÃƒO: Verificar se os dados necessÃ¡rios estÃ£o disponÃ­veis
        # Verificar se df_budget existe e tem a coluna PerÃ­odo
        tem_budget = df_budget is not None and not df_budget.empty and 'PerÃ­odo' in df_budget.columns
        # Verificar se df_real_vol existe e tem a coluna Volume
        tem_real_vol = df_real_vol is not None and not df_real_vol.empty and 'Volume' in df_real_vol.columns
        # Verificar se df_budget_vol existe (pode ser None, mas se existir deve ter Volume)
        tem_budget_vol = df_budget_vol is not None and not df_budget_vol.empty and 'Volume' in df_budget_vol.columns
        
        dados_budget_disponiveis = tem_budget and tem_real_vol
        
        if dados_budget_disponiveis:
            # Verificar se temos dados com coluna 'Custo' para calcular FLEX
            if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                try:
                    def _normalizar_custo_label(valor):
                        if pd.isna(valor):
                            return valor
                        txt = str(valor).strip()
                        txt_sem_acento = unicodedata.normalize('NFKD', txt).encode('ascii', 'ignore').decode('ascii')
                        txt_norm = txt_sem_acento.strip().lower()
                        if txt_norm == 'fixo':
                            return 'Fixo'
                        if txt_norm == 'variavel':
                            return 'VariÃ¡vel'
                        return txt

                    # ðŸ”§ CORREÃ‡ÃƒO: Usar a MESMA lÃ³gica do grÃ¡fico de Oficina (que funciona!)
                    # Calcular Flex Bud diretamente em vez de usar calcular_flex_budget
                    # Normalizar perÃ­odos ANTES de agrupar
                    mapeamento_meses = {
                        'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                        'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                        'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                        'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                    }
                    
                    def normalizar_periodo(periodo):
                        """Normaliza perÃ­odo para formato capitalizado"""
                        if pd.isna(periodo):
                            return periodo
                        periodo_str = str(periodo).strip()
                        for mes_min, mes_cap in mapeamento_meses.items():
                            if periodo_str.lower() == mes_min.lower():
                                return mes_cap
                        return periodo_str
                    
                    # Normalizar perÃ­odos em todos os DataFrames
                    if 'PerÃ­odo' in df_real_para_flex.columns:
                        df_real_para_flex = df_real_para_flex.copy()
                        df_real_para_flex['PerÃ­odo'] = df_real_para_flex['PerÃ­odo'].apply(normalizar_periodo)
                    if 'Custo' in df_real_para_flex.columns:
                        df_real_para_flex['Custo'] = df_real_para_flex['Custo'].apply(_normalizar_custo_label)
                    if df_real_vol is not None and 'PerÃ­odo' in df_real_vol.columns:
                        df_real_vol = df_real_vol.copy()
                        df_real_vol['PerÃ­odo'] = df_real_vol['PerÃ­odo'].apply(normalizar_periodo)
                    if 'PerÃ­odo' in df_budget.columns:
                        df_budget = df_budget.copy()
                        df_budget['PerÃ­odo'] = df_budget['PerÃ­odo'].apply(normalizar_periodo)
                    if 'Custo' in df_budget.columns:
                        df_budget['Custo'] = df_budget['Custo'].apply(_normalizar_custo_label)
                    if df_budget_vol is not None and 'PerÃ­odo' in df_budget_vol.columns:
                        df_budget_vol = df_budget_vol.copy()
                        df_budget_vol['PerÃ­odo'] = df_budget_vol['PerÃ­odo'].apply(normalizar_periodo)

                    # ðŸ”§ IMPORTANTE: nÃ£o filtrar volume por "recorte do custo".
                    # O volume pode conter veÃ­culos/oficinas sem custo realizado; cortar aqui
                    # gera volumes totais incorretos e distorce o Flex.
                    
                    # Agrupar dados reais por PerÃ­odo (mesma lÃ³gica do grÃ¡fico de Oficina)
                    if tem_ano:
                        if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['Ano', 'PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['Ano', 'PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    else:
                        if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['PerÃ­odo', 'Custo'])['Total'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    
                    # Verificar se temos todos os dados necessÃ¡rios
                    if (real_agrupado is None or real_vol_agrupado is None or 
                        budget_agrupado is None or budget_vol_agrupado is None):
                        flex_data = None
                    else:
                        # Normalizar perÃ­odos nos DataFrames agrupados antes do merge
                        if tem_ano:
                            real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                            budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                            real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
                            budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
                        else:
                            real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                            budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                            real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
                            budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
                        
                        # Fazer merge de volumes por PerÃ­odo
                        if tem_ano:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on=['Ano', 'PerÃ­odo'],
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        else:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on='PerÃ­odo',
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        
                        # Calcular FLEX para cada PerÃ­odo (mesma lÃ³gica do grÃ¡fico de Oficina)
                        flex_data = []
                        for _, vol_row in volumes.iterrows():
                            if tem_ano:
                                ano = vol_row['Ano']
                                periodo = vol_row['PerÃ­odo']
                            else:
                                periodo = vol_row['PerÃ­odo']
                            
                            volume_real = vol_row['Volume_real']
                            volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                            
                            if volume_real == 0 or pd.isna(volume_real):
                                continue
                            
                            # Obter custos reais para este PerÃ­odo
                            if tem_ano:
                                custos_real = real_agrupado[
                                    (real_agrupado['Ano'] == ano) & 
                                    (real_agrupado['PerÃ­odo'] == periodo)
                                ]
                                custos_budget = budget_agrupado[
                                    (budget_agrupado['Ano'] == ano) & 
                                    (budget_agrupado['PerÃ­odo'] == periodo)
                                ]
                            else:
                                custos_real = real_agrupado[real_agrupado['PerÃ­odo'] == periodo]
                                custos_budget = budget_agrupado[budget_agrupado['PerÃ­odo'] == periodo]
                            
                            # Se nÃ£o houver dados de budget para este perÃ­odo, usar zeros
                            if len(custos_budget) == 0:
                                budget_total = 0
                                custo_fixo_budget = 0
                            else:
                                budget_total = custos_budget['Total'].sum()
                                custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()

                            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA (Flex): tudo que NÃƒO Ã© Fixo Ã© flexÃ­vel
                            custo_nao_fixo_budget = budget_total - custo_fixo_budget
                            
                            # Calcular Flex Bud (mesma lÃ³gica do grÃ¡fico de Oficina)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)
                                # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                                flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                            else:
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_valor = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)
                            
                            # Adicionar ao flex_data
                            if tem_ano:
                                flex_data.append({
                                    'Ano': ano,
                                    'PerÃ­odo': periodo,
                                    'FLEX': flex_valor
                                })
                            else:
                                flex_data.append({
                                    'PerÃ­odo': periodo,
                                    'FLEX': flex_valor
                                })
                        
                        if len(flex_data) == 0:
                            flex_data = None
                        else:
                            flex_data = pd.DataFrame(flex_data)
                    
                    if flex_data is None:
                        budget_data = None
                    
                    if flex_data is not None and len(flex_data) > 0:
                        # Renomear coluna FLEX para o nome da coluna do grÃ¡fico
                        flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                        
                        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Fazer merge com chart_data para garantir correspondÃªncia de perÃ­odos
                        # Isso garante que budget_data tenha os mesmos perÃ­odos que chart_data
                        if tem_ano:
                            # Criar coluna combinada para o rÃ³tulo do grÃ¡fico no flex_data
                            flex_data['PerÃ­odo_Completo'] = flex_data['PerÃ­odo'].astype(str) + ' ' + flex_data['Ano'].astype(str)
                            # Ordenar por ano e mÃªs
                            flex_data = ordenar_por_mes(flex_data, 'PerÃ­odo')
                            
                            # Fazer merge com chart_data para garantir correspondÃªncia
                            # Usar left join para manter todos os perÃ­odos do chart_data
                            budget_data = chart_data[['PerÃ­odo_Completo']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['PerÃ­odo_Completo', coluna]],
                                on='PerÃ­odo_Completo',
                                how='left'
                            )
                        else:
                            # Ordenar por mÃªs
                            flex_data = ordenar_por_mes(flex_data, 'PerÃ­odo')
                            
                            # Fazer merge com chart_data para garantir correspondÃªncia
                            # Usar left join para manter todos os perÃ­odos do chart_data
                            budget_data = chart_data[['PerÃ­odo']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['PerÃ­odo', coluna]],
                                on='PerÃ­odo',
                                how='left'
                            )
                        
                        # Preencher valores NaN com 0 (perÃ­odos sem Flex Bud)
                        budget_data[coluna] = budget_data[coluna].fillna(0)
                        
                        # Criar linha pontilhada se budget_data tiver dados
                        # IMPORTANTE: Criar mesmo que alguns valores sejam zero, desde que tenha dados
                        if len(budget_data) > 0:
                            # Determinar campo do eixo X baseado em tem_ano
                            campo_x = 'PerÃ­odo_Completo' if tem_ano else 'PerÃ­odo'
                            
                            # Criar linha tracejada de Flex Bud usando EXATAMENTE o mesmo eixo X das barras
                            # Usar o mesmo campo e sort garante que compartilhem o mesmo eixo X
                            # Adicionar coluna de legenda para identificar a linha
                            budget_data_legenda = budget_data.copy()
                            budget_data_legenda['Tipo'] = 'Flex Bud'
                            
                            linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                strokeDash=[10, 5],
                                strokeWidth=1.5,
                                opacity=0.8
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='PerÃ­odo',
                                    sort=ordem_periodos,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                color=alt.Color(
                                    'Tipo:N',
                                    title='Legenda',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                    legend=alt.Legend(
                                        title='Legenda', 
                                        orient='bottom', 
                                        titleFontSize=10, 
                                        labelFontSize=9,
                                        titleAnchor='middle',
                                        direction='horizontal',
                                        symbolType='square'
                                    )
                                ),
                                strokeDash=alt.StrokeDash(
                                    'Tipo:N',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=[[0], [10, 5]]),
                                    legend=None
                                ),
                                tooltip=[
                                    alt.Tooltip(f'{campo_x}:N', title='PerÃ­odo'),
                                    alt.Tooltip('Tipo:N', title='Tipo'),
                                    alt.Tooltip(
                                        f'{coluna}:Q',
                                        title='Flex Bud',
                                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                    )
                                ]
                            )
                            
                            # Adicionar bolinhas nos pontos da linha
                            pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                size=80,
                                opacity=0.9
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='PerÃ­odo',
                                    sort=ordem_periodos,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                color=alt.Color(
                                    'Tipo:N',
                                    title='Legenda',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                    legend=None
                                ),
                                tooltip=[
                                    alt.Tooltip(f'{campo_x}:N', title='PerÃ­odo'),
                                    alt.Tooltip('Tipo:N', title='Tipo'),
                                    alt.Tooltip(
                                        f'{coluna}:Q',
                                        title='Flex Bud',
                                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                    )
                                ]
                            )
                            
                            # Adicionar rÃ³tulos de texto na linha pontilhada
                            formato_rotulo_budget = ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                            rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                align='center',
                                baseline='bottom',
                                dy=-15,
                                color='#FF6B35',
                                fontSize=9,
                                fontWeight='bold'
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='PerÃ­odo',
                                    sort=ordem_periodos
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y
                                ),
                                text=alt.Text(f'{coluna}:Q', format=formato_rotulo_budget)
                            )
                            
                            # Combinar linha, pontos e rÃ³tulos
                            linha_budget = linha_budget + pontos_budget + rotulos_budget
                        else:
                            # Se budget_data nÃ£o tem valores nÃ£o-zero, nÃ£o criar linha
                            linha_budget = None
                            budget_data = None
                    else:
                        # Se budget_data foi criado mas estÃ¡ vazio, definir como None
                        budget_data = None
                except Exception as e:
                    budget_data = None
                    linha_budget = None
            else:
                budget_data = None

        # Criar grÃ¡fico de delta (Real - Flex Bud) se budget_data estiver disponÃ­vel
        # IMPORTANTE: No modo CPU, garantir que budget_data seja usado mesmo se estiver vazio
        grafico_delta = None
        if budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Flex Bud - Real
                # Fazer merge dos dados de Real e Flex Bud para calcular delta
                delta_data = chart_data.copy()
                
                # Determinar campo do eixo X baseado em tem_ano
                campo_x_delta = 'PerÃ­odo_Completo' if tem_ano else 'PerÃ­odo'
                
                # Fazer merge com budget_data para obter valores de Flex Bud
                # Renomear coluna antes do merge para evitar conflito
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                
                if tem_ano:
                    # Garantir que budget_data_merge tenha a coluna PerÃ­odo_Completo
                    # budget_data jÃ¡ foi criado com PerÃ­odo_Completo no merge anterior
                    if campo_x_delta not in budget_data_merge.columns:
                        # Se nÃ£o tiver, criar a partir de PerÃ­odo e Ano
                        if 'PerÃ­odo' in budget_data_merge.columns and 'Ano' in budget_data_merge.columns:
                            budget_data_merge[campo_x_delta] = budget_data_merge['PerÃ­odo'].astype(str) + ' ' + budget_data_merge['Ano'].astype(str)
                    
                    # Garantir que delta_data tambÃ©m tenha PerÃ­odo_Completo
                    if campo_x_delta not in delta_data.columns:
                        delta_data[campo_x_delta] = delta_data['PerÃ­odo'].astype(str) + ' ' + delta_data['Ano'].astype(str)
                    
                    delta_data = delta_data.merge(
                        budget_data_merge[[campo_x_delta, f'{coluna}_FlexBud']],
                        on=campo_x_delta,
                        how='left'
                    )
                else:
                    delta_data = delta_data.merge(
                        budget_data_merge[['PerÃ­odo', f'{coluna}_FlexBud']],
                        on='PerÃ­odo',
                        how='left'
                    )
                
                # Calcular delta: Real - Flex Bud
                coluna_real = coluna  # A coluna original jÃ¡ Ã© o Real
                coluna_flex = f'{coluna}_FlexBud'
                # Preencher valores NaN com 0 antes de calcular delta
                delta_data[coluna_flex] = delta_data[coluna_flex].fillna(0)
                delta_data['Delta'] = delta_data[coluna_real].fillna(0) - delta_data[coluna_flex].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simÃ©tricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domÃ­nio simÃ©trico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Criar grÃ¡fico de barras para delta (mais baixo)
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20  # Barras mais finas
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos,
                        axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)  # Sem linha, ticks ou labels no eixo X
                    ),
                    y=alt.Y(
                        'Delta:Q',
                        title='Delta (Real - Budget)',
                        axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                    ),
                    color=alt.Color(
                        'Delta:Q',
                        title='Delta',
                        scale=alt.Scale(
                            domain=[delta_min, 0, delta_max],
                            range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None  # Sem legenda para evitar duplicaÃ§Ã£o - o grÃ¡fico principal jÃ¡ tem sua legenda
                    ),
                    tooltip=[
                        alt.Tooltip(f'{campo_x_delta}:N', title='PerÃ­odo'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                        alt.Tooltip(f'{coluna_real}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna_flex}:Q', title='Flex Bud', format=',.2f')
                    ]
                ).properties(
                    height=38  # GrÃ¡fico mais baixo/fino
                )
                
                # Adicionar rÃ³tulos de dados no grÃ¡fico de delta
                # Posicionar acima para valores positivos e abaixo para negativos
                # Usar a mesma cor das barras (verde para negativo, vermelho para positivo)
                rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Delta:Q', title=''),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                    align='center',
                    baseline='top',
                    dy=12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Delta:Q', title=''),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                # Combinar grÃ¡fico de delta com rÃ³tulos
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                grafico_delta = None
        
        # Combinar grÃ¡fico de barras com linha de budget se disponÃ­vel
        if linha_budget is not None:
            # Criar grÃ¡fico principal com barras, rÃ³tulos e linha
            grafico_principal = alt.layer(
                grafico_barras,
                rotulos,
                linha_budget
            ).resolve_scale(
                x='shared',
                y='shared'
            )
            
            # Se temos grÃ¡fico de delta, combinar verticalmente (delta em cima)
            if grafico_delta is not None:
                # Combinar grÃ¡ficos verticalmente compartilhando eixo X
                # Delta fica em cima (primeiro), grÃ¡fico principal embaixo
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_principal
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os grÃ¡ficos
                )
            else:
                grafico_final = grafico_principal
        else:
            # Se nÃ£o hÃ¡ linha de budget, mas temos grÃ¡fico de delta, combinar com grÃ¡fico de barras
            if grafico_delta is not None:
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_barras + rotulos
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os grÃ¡ficos
                )
            else:
                grafico_final = grafico_barras + rotulos
        
        return grafico_final
    except Exception as e:
        st.error(f"Erro ao criar grÃ¡fico: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


# GrÃ¡fico 2: Volume por PerÃ­odo
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data, df_budget_vol=None):
    """Cria grÃ¡fico de barras de Volume por PerÃ­odo com linha pontilhada de volume do Budget opcional"""
    try:
        altura_grafico = 260
        if 'Volume' not in df_data.columns or 'PerÃ­odo' not in df_data.columns:
            return None

        # Verificar se hÃ¡ coluna Ano - sempre mostrar ano junto com perÃ­odo quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e PerÃ­odo (sempre que houver coluna Ano)
            chart_data = df_data.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
            
            # Criar coluna combinada para o rÃ³tulo do grÃ¡fico
            chart_data['PerÃ­odo_Completo'] = chart_data['PerÃ­odo'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mÃªs
            chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
            ordem_periodos = chart_data['PerÃ­odo_Completo'].tolist()
            
            # Usar PerÃ­odo_Completo no grÃ¡fico
            coluna_periodo_grafico = 'PerÃ­odo_Completo'
        else:
            # Comportamento original: agrupar apenas por PerÃ­odo (quando nÃ£o hÃ¡ coluna Ano)
            chart_data = df_data.groupby('PerÃ­odo')['Volume'].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
            ordem_periodos = chart_data['PerÃ­odo'].tolist()
            coluna_periodo_grafico = 'PerÃ­odo'

        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        # Usar gradiente verde baseado no valor do Volume (como no grÃ¡fico Volume por VeÃ­culo)
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='PerÃ­odo',
                sort=ordem_periodos,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume Total', axis=alt.Axis(grid=False, domain=True, ticks=True)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(title='Volume', orient='right', titleFontSize=10, labelFontSize=9)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='PerÃ­odo'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=altura_grafico,
            width='container'
            # TÃ­tulo removido para evitar duplicaÃ§Ã£o com st.subheader
        )

        # Adicionar rÃ³tulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )

        # Processar dados de volume do budget se fornecidos
        linha_budget_vol = None
        if df_budget_vol is not None and 'PerÃ­odo' in df_budget_vol.columns:
            try:
                # Processar volume do budget seguindo a mesma lÃ³gica dos dados principais
                tem_ano_budget_vol = 'Ano' in df_budget_vol.columns
                
                if tem_ano_budget_vol:
                    # Agrupar por Ano e PerÃ­odo (mesma lÃ³gica dos dados principais)
                    budget_vol_data = df_budget_vol.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                    
                    # Criar coluna combinada para o rÃ³tulo do grÃ¡fico
                    budget_vol_data['PerÃ­odo_Completo'] = budget_vol_data['PerÃ­odo'].astype(str) + ' ' + budget_vol_data['Ano'].astype(str)
                    # Ordenar por ano e mÃªs
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'PerÃ­odo')
                    # Filtrar apenas perÃ­odos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['PerÃ­odo_Completo'].isin(ordem_periodos)].copy()
                else:
                    # Sem coluna Ano: agrupar apenas por PerÃ­odo
                    budget_vol_data = df_budget_vol.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                    # Ordenar por mÃªs
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'PerÃ­odo')
                    # Filtrar apenas perÃ­odos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['PerÃ­odo'].isin(ordem_periodos)].copy()
                
                if len(budget_vol_data) > 0:
                    # Determinar campo do eixo X baseado em tem_ano
                    campo_x = 'PerÃ­odo_Completo' if tem_ano else 'PerÃ­odo'
                    
                    # Adicionar coluna de legenda
                    budget_vol_data_legenda = budget_vol_data.copy()
                    budget_vol_data_legenda['Tipo'] = 'Volume Budget'
                    
                    # Criar linha tracejada de volume do budget
                    linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                        strokeDash=[10, 5],
                        strokeWidth=1.5,
                        color='#FF6B35',
                        opacity=0.8
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='PerÃ­odo',
                            sort=ordem_periodos,
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total',
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        color=alt.Color(
                            'Tipo:N',
                            title='Legenda',
                            scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                            legend=alt.Legend(title='Legenda', orient='right', titleFontSize=10, labelFontSize=9)
                        ),
                        strokeDash=alt.StrokeDash(
                            'Tipo:N',
                            scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip(f'{campo_x}:N', title='PerÃ­odo'),
                            alt.Tooltip('Tipo:N', title='Tipo'),
                            alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                        ]
                    )
                    
                    # Adicionar bolinhas nos pontos da linha
                    pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                        size=80,
                        color='#FF6B35',
                        opacity=0.9
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='PerÃ­odo',
                            sort=ordem_periodos,
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total',
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        color=alt.Color(
                            'Tipo:N',
                            title='Legenda',
                            scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip(f'{campo_x}:N', title='PerÃ­odo'),
                            alt.Tooltip('Tipo:N', title='Tipo'),
                            alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                        ]
                    )
                    
                    # Adicionar rÃ³tulos de texto na linha pontilhada
                    rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                        align='center',
                        baseline='bottom',
                        dy=-15,
                        color='#FF6B35',
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='PerÃ­odo',
                            sort=ordem_periodos
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total'
                        ),
                        text=alt.Text('Volume:Q', format=',.0f')
                    )
                    
                    # Combinar linha, pontos e rÃ³tulos
                    linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception as e:
                st.sidebar.warning(f"âš ï¸ Erro ao processar dados de volume do budget: {e}")

        # Combinar grÃ¡fico de barras com linha de budget se disponÃ­vel
        if linha_budget_vol is not None:
            return alt.layer(
                grafico_barras,
                rotulos,
                linha_budget_vol
            ).resolve_scale(
                x='shared',
                y='shared'
            )
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar grÃ¡fico: {e}")
        return None


# GrÃ¡fico 4.5: Volume por VeÃ­culo
@st.cache_data(ttl=900, max_entries=2)
def create_volume_veiculo_chart(df_data, df_budget_vol=None, df_despesas=None):
    """Cria grÃ¡fico de barras de Volume por VeÃ­culo com linha pontilhada de volume do Budget opcional
    df_despesas: parÃ¢metro legado (nÃ£o usado)."""
    try:
        if 'Volume' not in df_data.columns or 'VeÃ­culo' not in df_data.columns:
            return None
        
        # Filtrar linhas com Volume e VeÃ­culo nÃ£o nulos
        df_data = df_data[df_data['Volume'].notna() & df_data['VeÃ­culo'].notna()].copy()
        
        if len(df_data) == 0:
            return None
        
        # Agrupar por VeÃ­culo e somar Volume
        # Se houver mÃºltiplos anos, agrupar por VeÃ­culo, PerÃ­odo e Ano primeiro
        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        
        if tem_multiplos_anos and 'PerÃ­odo' in df_data.columns:
            # Agrupar por VeÃ­culo, PerÃ­odo e Ano, somar Volume
            df_agrupado_periodo = df_data.groupby(['VeÃ­culo', 'PerÃ­odo', 'Ano']).agg({
                'Volume': 'sum'
            }).reset_index()
            # Agora agrupar por VeÃ­culo, somar Volume de todos os perÃ­odos
            chart_data = df_agrupado_periodo.groupby('VeÃ­culo').agg({
                'Volume': 'sum'
            }).reset_index()
        elif 'PerÃ­odo' in df_data.columns:
            # Agrupar por VeÃ­culo e PerÃ­odo, somar Volume
            df_agrupado_periodo = df_data.groupby(['VeÃ­culo', 'PerÃ­odo']).agg({
                'Volume': 'sum'
            }).reset_index()
            # Agora agrupar por VeÃ­culo, somar Volume de todos os perÃ­odos
            chart_data = df_agrupado_periodo.groupby('VeÃ­culo').agg({
                'Volume': 'sum'
            }).reset_index()
        else:
            # Se nÃ£o tiver PerÃ­odo, agrupar apenas por VeÃ­culo
            chart_data = df_data.groupby('VeÃ­culo').agg({
                'Volume': 'sum'
            }).reset_index()
        
        # Verificar se hÃ¡ dados
        if len(chart_data) == 0:
            return None
        
        # Filtrar valores nulos
        chart_data = chart_data[chart_data['Volume'].notna()].copy()
        
        if len(chart_data) == 0:
            return None
        
        chart_data = chart_data.sort_values('Volume', ascending=False)
        
        # Determinar ordem dos veÃ­culos (usar a mesma ordem para barras e linha)
        ordem_veiculos = chart_data['VeÃ­culo'].tolist()
        
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'VeÃ­culo:N',
                title='VeÃ­culo',
                sort=ordem_veiculos,
                scale=alt.Scale(domain=ordem_veiculos),
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume (Unidades)', axis=alt.Axis(grid=False)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens')
            ),
            tooltip=[
                alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=360,
            width='container'
            # TÃ­tulo removido para evitar duplicaÃ§Ã£o com st.subheader
        )
        
        # Adicionar rÃ³tulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )
        
        # Processar dados de volume do budget se fornecidos
        # ObservaÃ§Ã£o: nÃ£o aplicar filtros por "meses com despesa" aqui. Volume Ã© independente de custo.
        linha_budget_vol = None
        if df_budget_vol is not None and 'VeÃ­culo' in df_budget_vol.columns:
            try:
                # Filtrar linhas com Volume e VeÃ­culo nÃ£o nulos
                df_budget_vol_filtrado = df_budget_vol[df_budget_vol['Volume'].notna() & df_budget_vol['VeÃ­culo'].notna()].copy()
                
                if len(df_budget_vol_filtrado) > 0:
                    # Agrupar por VeÃ­culo seguindo a mesma lÃ³gica dos dados principais
                    tem_multiplos_anos_budget = 'Ano' in df_budget_vol_filtrado.columns and df_budget_vol_filtrado['Ano'].nunique() > 1
                    
                    if tem_multiplos_anos_budget and 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                        # Agrupar por VeÃ­culo, PerÃ­odo e Ano, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['VeÃ­culo', 'PerÃ­odo', 'Ano']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por VeÃ­culo, somar Volume de todos os perÃ­odos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('VeÃ­culo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    elif 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                        # Agrupar por VeÃ­culo e PerÃ­odo, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['VeÃ­culo', 'PerÃ­odo']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por VeÃ­culo, somar Volume de todos os perÃ­odos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('VeÃ­culo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        # Se nÃ£o tiver PerÃ­odo, agrupar apenas por VeÃ­culo
                        budget_vol_data = df_budget_vol_filtrado.groupby('VeÃ­culo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    
                    # IMPORTANTE: Garantir que todos os veÃ­culos do realizado estejam no budget
                    # Criar DataFrame completo com todos os veÃ­culos do realizado
                    budget_vol_data_completo = pd.DataFrame({'VeÃ­culo': ordem_veiculos})
                    
                    # Fazer merge com os dados de budget (left join para manter todos os veÃ­culos do realizado)
                    budget_vol_data = budget_vol_data_completo.merge(
                        budget_vol_data,
                        on='VeÃ­culo',
                        how='left'
                    )
                    
                    # Preencher valores faltantes com 0
                    budget_vol_data['Volume'] = budget_vol_data['Volume'].fillna(0)
                    
                    if len(budget_vol_data) > 0:
                        # Adicionar coluna de legenda
                        budget_vol_data_legenda = budget_vol_data.copy()
                        budget_vol_data_legenda['Tipo'] = 'Volume Budget'
                        
                        # Garantir que estÃ¡ na ordem correta (jÃ¡ estÃ¡ na ordem correta por causa do merge)
                        # Mas vamos garantir explicitamente
                        ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos)}
                        budget_vol_data_legenda['_ordem'] = budget_vol_data_legenda['VeÃ­culo'].map(ordem_dict)
                        budget_vol_data_legenda = budget_vol_data_legenda.sort_values('_ordem')
                        budget_vol_data_legenda = budget_vol_data_legenda.drop(columns=['_ordem'])
                        
                        # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras (ordem_veiculos)
                        # Isso garante que a linha do budget apareÃ§a na mesma ordem do realizado
                        ordem_veiculos_budget = ordem_veiculos
                        
                        # Criar linha tracejada de volume do budget
                        linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                            x=alt.X(
                                'VeÃ­culo:N',
                                title='VeÃ­culo',
                                sort=ordem_veiculos_budget,
                                scale=alt.Scale(domain=ordem_veiculos_budget),
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            y=alt.Y(
                                'Volume:Q',
                                title='Volume (Unidades)',
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            color=alt.Color(
                                'Tipo:N',
                                title='Legenda',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=alt.Legend(
                                    title='Legenda',
                                    orient='right',
                                    titleFontSize=10,
                                    labelFontSize=9
                                )
                            ),
                            strokeDash=alt.StrokeDash(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar bolinhas nos pontos da linha
                        pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('VeÃ­culo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='VeÃ­culo'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar rÃ³tulos nos pontos
                        rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('VeÃ­culo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='VeÃ­culo'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            text=alt.Text('Volume:Q', format=',.0f'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            )
                        )
                        
                        linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception as e:
                # Silenciar erro, apenas nÃ£o mostrar linha do budget
                pass
        
        # Combinar grÃ¡fico de barras com linha do budget se existir
        if linha_budget_vol is not None:
            return grafico_barras + rotulos + linha_budget_vol
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar grÃ¡fico de volume: {e}")
        return None


# GrÃ¡fico 4.6: Volume por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_volume_oficina_chart(df_data, df_budget_vol=None, df_despesas=None):
    """Cria grÃ¡fico de barras de Volume por Oficina com linha pontilhada de volume do Budget opcional
    df_despesas: parÃ¢metro legado (nÃ£o usado)."""
    try:
        if 'Volume' not in df_data.columns or 'Oficina' not in df_data.columns:
            return None

        df_data = df_data[df_data['Volume'].notna() & df_data['Oficina'].notna()].copy()
        if len(df_data) == 0:
            return None

        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        if tem_multiplos_anos and 'PerÃ­odo' in df_data.columns:
            df_agrupado_periodo = df_data.groupby(['Oficina', 'PerÃ­odo', 'Ano']).agg({'Volume': 'sum'}).reset_index()
            chart_data = df_agrupado_periodo.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
        elif 'PerÃ­odo' in df_data.columns:
            df_agrupado_periodo = df_data.groupby(['Oficina', 'PerÃ­odo']).agg({'Volume': 'sum'}).reset_index()
            chart_data = df_agrupado_periodo.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
        else:
            chart_data = df_data.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()

        if len(chart_data) == 0:
            return None

        chart_data = chart_data[chart_data['Volume'].notna()].copy()
        if len(chart_data) == 0:
            return None

        chart_data = chart_data.sort_values('Volume', ascending=False)
        ordem_oficinas = chart_data['Oficina'].tolist()

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Oficina:N',
                title='Oficina',
                sort=ordem_oficinas,
                scale=alt.Scale(domain=ordem_oficinas),
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume (Unidades)', axis=alt.Axis(grid=False)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens')
            ),
            tooltip=[
                alt.Tooltip('Oficina:N', title='Oficina'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=360,
            width='container'
        )

        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )

        linha_budget_vol = None
        if df_budget_vol is not None and 'Oficina' in df_budget_vol.columns:
            try:
                df_budget_vol_filtrado = df_budget_vol[df_budget_vol['Volume'].notna() & df_budget_vol['Oficina'].notna()].copy()
                if len(df_budget_vol_filtrado) > 0:
                    tem_multiplos_anos_budget = 'Ano' in df_budget_vol_filtrado.columns and df_budget_vol_filtrado['Ano'].nunique() > 1

                    if tem_multiplos_anos_budget and 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Oficina', 'PerÃ­odo', 'Ano']).agg({'Volume': 'sum'}).reset_index()
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
                    elif 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Oficina', 'PerÃ­odo']).agg({'Volume': 'sum'}).reset_index()
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
                    else:
                        budget_vol_data = df_budget_vol_filtrado.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()

                    budget_vol_data_completo = pd.DataFrame({'Oficina': ordem_oficinas})
                    budget_vol_data = budget_vol_data_completo.merge(
                        budget_vol_data,
                        on='Oficina',
                        how='left'
                    )
                    budget_vol_data['Volume'] = budget_vol_data['Volume'].fillna(0)

                    if len(budget_vol_data) > 0:
                        budget_vol_data_legenda = budget_vol_data.copy()
                        budget_vol_data_legenda['Tipo'] = 'Volume Budget'

                        ordem_dict = {oficina: idx for idx, oficina in enumerate(ordem_oficinas)}
                        budget_vol_data_legenda['_ordem'] = budget_vol_data_legenda['Oficina'].map(ordem_dict)
                        budget_vol_data_legenda = budget_vol_data_legenda.sort_values('_ordem')
                        budget_vol_data_legenda = budget_vol_data_legenda.drop(columns=['_ordem'])

                        ordem_oficinas_budget = ordem_oficinas

                        linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                            x=alt.X(
                                'Oficina:N',
                                title='Oficina',
                                sort=ordem_oficinas_budget,
                                scale=alt.Scale(domain=ordem_oficinas_budget),
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            y=alt.Y(
                                'Volume:Q',
                                title='Volume (Unidades)',
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            color=alt.Color(
                                'Tipo:N',
                                title='Legenda',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=alt.Legend(
                                    title='Legenda',
                                    orient='right',
                                    titleFontSize=10,
                                    labelFontSize=9
                                )
                            ),
                            strokeDash=alt.StrokeDash(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Oficina:N', title='Oficina'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )

                        pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('Oficina:N', sort=ordem_oficinas_budget, scale=alt.Scale(domain=ordem_oficinas_budget), title='Oficina'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Oficina:N', title='Oficina'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )

                        rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('Oficina:N', sort=ordem_oficinas_budget, scale=alt.Scale(domain=ordem_oficinas_budget), title='Oficina'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            text=alt.Text('Volume:Q', format=',.0f'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            )
                        )

                        linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception:
                pass

        if linha_budget_vol is not None:
            return grafico_barras + rotulos + linha_budget_vol
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar grÃ¡fico de volume por oficina: {e}")
        return None


# Inicializar session_state para manter a tab selecionada
# Usar uma chave mais especÃ­fica para evitar conflitos
if 'tab_selecionada_tc_ext_persistente' not in st.session_state:
    st.session_state.tab_selecionada_tc_ext_persistente = 0

# Verificar se hÃ¡ parÃ¢metro de tab na URL e atualizar session_state
# Isso garante que a tab seja mantida mesmo apÃ³s recarregamento por filtros
tab_from_url = st.query_params.get('tab', None)
if tab_from_url is not None:
    try:
        tab_index = int(tab_from_url)
        if 0 <= tab_index <= 3:  # Validar Ã­ndice (0-3 para 4 tabs)
            st.session_state.tab_selecionada_tc_ext_persistente = tab_index
    except ValueError:
        pass
# Se nÃ£o houver parÃ¢metro na URL, manter o valor atual do session_state
# Isso evita que a tab seja resetada quando hÃ¡ mudanÃ§as de filtros
# O valor jÃ¡ foi inicializado acima se nÃ£o existir

# Manter compatibilidade com a chave antiga
st.session_state.tab_selecionada_tc_ext = st.session_state.tab_selecionada_tc_ext_persistente

# SÃ³ criar tabs e JavaScript se estivermos na pÃ¡gina principal
if is_main_page:
    # JavaScript ANTES das tabs para interceptar a criaÃ§Ã£o
    # Este script serÃ¡ executado antes que o Streamlit defina a primeira tab como padrÃ£o
    st.markdown(f"""
<script>
(function() {{
    // ForÃ§ar re-render de grÃ¡ficos Altair/Vega quando a tab Ã© exibida.
    // Em alguns navegadores, apenas dispatchEvent('resize') nÃ£o Ã© suficiente.
    function forcarResizeVega() {{
        try {{
            const embeds = document.querySelectorAll('.vega-embed');
            embeds.forEach((el) => {{
                try {{
                    const view = el.__vega_view__ || (el.querySelector('div') ? el.querySelector('div').__vega_view__ : null);
                    if (view && typeof view.resize === 'function') {{
                        const resized = view.resize();
                        if (resized && typeof resized.runAsync === 'function') {{
                            resized.runAsync();
                        }} else if (resized && typeof resized.run === 'function') {{
                            resized.run();
                        }} else if (typeof view.runAsync === 'function') {{
                            view.runAsync();
                        }} else if (typeof view.run === 'function') {{
                            view.run();
                        }}
                    }}
                }} catch (e) {{}}
            }});
        }} catch (e) {{}}
    }}

    function forcarReflowGraficos() {{
        try {{ window.dispatchEvent(new Event('resize')); }} catch (e) {{}}
        // Rodar em momentos diferentes para pegar quando o container fica visÃ­vel
        setTimeout(forcarResizeVega, 0);
        setTimeout(forcarResizeVega, 150);
        setTimeout(forcarResizeVega, 500);
    }}

    // Obter Ã­ndice da tab da URL
    function obterTabIndex() {{
        const urlParams = new URLSearchParams(window.location.search);
        const tabIndexUrl = urlParams.get('tab');
        if (tabIndexUrl !== null) {{
            const index = parseInt(tabIndexUrl);
            if (index >= 0 && index <= 4) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_ext_persistente};
    }}
    
    const tabIndexDesejado = obterTabIndex();
    
    // Interceptar a criaÃ§Ã£o das tabs ANTES que sejam renderizadas
    // Usar MutationObserver para detectar quando as tabs sÃ£o criadas
    const observerPrecoce = new MutationObserver(function(mutations) {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length >= 5) {{
            // Tabs foram criadas, verificar se a primeira estÃ¡ selecionada
            const primeiraTab = tabs[0];
            if (primeiraTab && primeiraTab.getAttribute('aria-selected') === 'true' && tabIndexDesejado !== 0) {{
                // Primeira tab estÃ¡ selecionada mas nÃ£o deveria estar
                // Clicar na tab correta IMEDIATAMENTE
                if (tabs[tabIndexDesejado]) {{
                    // Usar requestAnimationFrame para garantir execuÃ§Ã£o no prÃ³ximo frame
                    requestAnimationFrame(function() {{
                        tabs[tabIndexDesejado].click();
                    }});
                }}
            }}
        }}
    }});
    
    // ComeÃ§ar a observar imediatamente
    observerPrecoce.observe(document.body, {{
        childList: true,
        subtree: true
    }});
    
    // TambÃ©m tentar executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            const tabs = document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs.length >= 5 && tabIndexDesejado !== 0) {{
                const primeiraTab = tabs[0];
                if (primeiraTab && primeiraTab.getAttribute('aria-selected') === 'true') {{
                    requestAnimationFrame(function() {{
                        if (tabs[tabIndexDesejado]) {{
                            tabs[tabIndexDesejado].click();
                        }}
                    }});
                }}
            }}
        }});
    }}
}})();
</script>
""", unsafe_allow_html=True)

    # Criar estrutura de tabs para organizaÃ§Ã£o
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "ðŸ“Š TC Ext",
        "ðŸ“ˆ Volume",
        "ðŸš— TC Ext por VeÃ­c",
        "ðŸ“‹ Detalhe Real",
        "ðŸ§¾ Detalhe Budget",
    ])
else:
    # Se nÃ£o estamos na pÃ¡gina principal, criar tabs vazias para evitar erros
    # Mas nÃ£o renderizar conteÃºdo
    tab1 = tab2 = tab3 = tab4 = tab5 = None

# JavaScript DEPOIS das tabs para manter a seleÃ§Ã£o
st.markdown(f"""
<script>
(function() {{
    // Obter Ã­ndice da tab da URL
    function obterTabIndex() {{
        const urlParams = new URLSearchParams(window.location.search);
        const tabIndexUrl = urlParams.get('tab');
        if (tabIndexUrl !== null) {{
            const index = parseInt(tabIndexUrl);
            if (index >= 0 && index <= 4) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_ext_persistente};
    }}
    
    let tabIndexSalvo = obterTabIndex();
    let restauracaoEmAndamento = false;
    
    // FunÃ§Ã£o para forÃ§ar a seleÃ§Ã£o da tab correta
    function forcarSelecaoTab(index) {{
        if (restauracaoEmAndamento) return;
        
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0 || index < 0 || index >= tabs.length) return;
        
        const tabAlvo = tabs[index];
        if (!tabAlvo) return;
        
        // Verificar se jÃ¡ estÃ¡ selecionada
        if (tabAlvo.getAttribute('aria-selected') === 'true') {{
            return; // JÃ¡ estÃ¡ selecionada
        }}
        
        restauracaoEmAndamento = true;
        
        // MÃºltiplas tentativas de clicar
        function tentarClicar() {{
            tabAlvo.click();

            // ForÃ§ar recalculo de layout para grÃ¡ficos dentro de tabs (Altair/Plotly)
            // Quando o grÃ¡fico Ã© renderizado com a tab escondida, pode ficar com width=0.
            forcarReflowGraficos();
            
            // Verificar se funcionou
            setTimeout(function() {{
                if (tabAlvo.getAttribute('aria-selected') === 'true') {{
                    restauracaoEmAndamento = false;
                }} else {{
                    // Tentar novamente
                    requestAnimationFrame(tentarClicar);
                }}
            }}, 50);
        }}
        
        requestAnimationFrame(tentarClicar);
    }}
    
    // FunÃ§Ã£o para verificar e restaurar
    function verificarERestaurar() {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0) return;
        
        // Atualizar da URL
        tabIndexSalvo = obterTabIndex();
        
        // Verificar qual tab estÃ¡ selecionada
        let tabAtual = -1;
        tabs.forEach((tab, index) => {{
            if (tab.getAttribute('aria-selected') === 'true') {{
                tabAtual = index;
            }}
        }});
        
        // Se nÃ£o estÃ¡ na tab correta, restaurar
        if (tabAtual !== tabIndexSalvo) {{
            forcarSelecaoTab(tabIndexSalvo);
        }}
    }}
    
    // Configurar listeners para salvar na URL quando clicar
    function configurarListeners() {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        tabs.forEach((tab, index) => {{
            // Remover listeners antigos se existirem
            const novoTab = tab.cloneNode(true);
            if (tab.parentNode) {{
                tab.parentNode.replaceChild(novoTab, tab);
            }}
            
            // Adicionar novo listener com captura (true) para interceptar antes do Streamlit
            novoTab.addEventListener('click', function(e) {{
                tabIndexSalvo = index;
                const url = new URL(window.location);
                url.searchParams.set('tab', index);
                window.history.replaceState({{}}, '', url);
                
                // TambÃ©m salvar no sessionStorage para persistÃªncia entre recarregamentos
                sessionStorage.setItem('tab_selecionada_tc_ext', index);

                // ForÃ§ar recalculo de layout dos grÃ¡ficos ao trocar de aba
                forcarReflowGraficos();
                
                // Atualizar session_state no Streamlit via query params
                // Isso garante que o Streamlit saiba qual tab estÃ¡ selecionada
                if (window.parent && window.parent.postMessage) {{
                    window.parent.postMessage({{
                        type: 'streamlit:setFrameHeight',
                        height: document.body.scrollHeight
                    }}, '*');
                }}
            }}, true);
        }});
    }}
    
    // Tentar restaurar do sessionStorage se nÃ£o houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_ext');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // SÃ³ usar sessionStorage se nÃ£o houver parÃ¢metro na URL
                    tabIndexSalvo = index;
                    const url = new URL(window.location);
                    url.searchParams.set('tab', index);
                    window.history.replaceState({{}}, '', url);
                }}
            }}
        }}
    }}
    
    // Tentar restaurar do sessionStorage se nÃ£o houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_ext');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // SÃ³ usar sessionStorage se nÃ£o houver parÃ¢metro na URL
                    tabIndexSalvo = index;
                    const url = new URL(window.location);
                    url.searchParams.set('tab', index);
                    window.history.replaceState({{}}, '', url);
                }}
            }}
        }}
    }}
    
    // Restaurar do sessionStorage primeiro
    restaurarDeSessionStorage();
    
    // Executar imediatamente usando requestAnimationFrame
    requestAnimationFrame(function() {{
        verificarERestaurar();
        configurarListeners();
    }});
    
    // Executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            restaurarDeSessionStorage();
            verificarERestaurar();
            configurarListeners();
        }});
    }} else {{
        // DOM jÃ¡ estÃ¡ pronto
        restaurarDeSessionStorage();
        verificarERestaurar();
        configurarListeners();
    }}
    
    // Executar periodicamente (mais frequente para garantir)
    // IMPORTANTE: Reduzir frequÃªncia para evitar conflitos com recarregamentos do Streamlit
    setInterval(function() {{
        verificarERestaurar();
    }}, 200);
    
    // Observar mudanÃ§as no DOM
    const observer = new MutationObserver(function() {{
        verificarERestaurar();
        configurarListeners();

        // MudanÃ§as de aria-selected podem esconder/mostrar charts; forÃ§ar reflow ajuda a re-renderizar.
        forcarReflowGraficos();
    }});
    
    // Observar o container principal
    setTimeout(function() {{
        const mainContainer = document.querySelector('main') || document.body;
        if (mainContainer) {{
            observer.observe(mainContainer, {{
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['aria-selected']
            }});
        }}
        
        const tabsContainer = document.querySelector('[data-baseweb="tabs"]');
        if (tabsContainer) {{
            observer.observe(tabsContainer, {{
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['aria-selected']
            }});
        }}
    }}, 50);
    
    // Executar em mÃºltiplos momentos para garantir
    [100, 200, 300, 500, 1000].forEach(function(delay) {{
        setTimeout(function() {{
            verificarERestaurar();
            configurarListeners();
        }}, delay);
    }});
}})();
</script>
""", unsafe_allow_html=True)

# ==========================================
# TAB 1: TC Ext
# ==========================================
# SÃ³ exibir conteÃºdo das tabs se estivermos na pÃ¡gina principal
if is_main_page:
    # Preparar volume filtrado pela sidebar para uso em CPU (antes das tabs)
    df_vol_filtrado_sidebar = None
    df_vol_filtrado_sidebar_share = None
    try:
        df_vol_base = load_volume_data(ano_selecionado)
        if df_vol_base is not None and 'Volume' in df_vol_base.columns:
            df_vol_filtrado_sidebar = filtrar_volume_com_sidebar(df_vol_base, df_total)
            # Base para rateio: respeita filtros gerais, mas ignora filtro de VeÃ­culo
            df_vol_filtrado_sidebar_share = filtrar_volume_com_sidebar(
                df_vol_base,
                df_total,
                ignorar_veiculo=True,
            )
    except Exception:
        df_vol_filtrado_sidebar = None
        df_vol_filtrado_sidebar_share = None

    # Criar df_visualizacao a partir de df_filtrado antes de usar nas tabs
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_visualizacao = df_filtrado.copy()
        # Definir coluna_visualizacao baseado no tipo_visualizacao
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            coluna_visualizacao = 'CPU'
            if 'Volume' not in df_visualizacao.columns:
                if df_vol_filtrado_sidebar is None:
                    try:
                        df_vol_base = load_volume_data(ano_selecionado)
                        if df_vol_base is not None and 'Volume' in df_vol_base.columns:
                            df_vol_filtrado_sidebar = filtrar_volume_com_sidebar(df_vol_base, df_total)
                    except Exception:
                        df_vol_filtrado_sidebar = None
                if df_vol_filtrado_sidebar is not None and 'Volume' in df_vol_filtrado_sidebar.columns:
                    chaves_merge = [
                        col for col in ['Oficina', 'VeÃ­culo', 'PerÃ­odo', 'Ano']
                        if col in df_visualizacao.columns and col in df_vol_filtrado_sidebar.columns
                    ]
                    if chaves_merge:
                        df_visualizacao = df_visualizacao.merge(
                            df_vol_filtrado_sidebar[chaves_merge + ['Volume']],
                            on=chaves_merge,
                            how='left'
                        )
            if 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                df_visualizacao['CPU'] = np.where(
                    (df_visualizacao['Volume'].notna()) & (df_visualizacao['Volume'] != 0),
                    df_visualizacao['Total'] / df_visualizacao['Volume'],
                    0
                )
        else:
            coluna_visualizacao = 'Total' if 'Total' in df_visualizacao.columns else 'Valor'
    else:
        # Se df_filtrado nÃ£o estiver disponÃ­vel, criar DataFrame vazio
        df_visualizacao = pd.DataFrame()
        coluna_visualizacao = 'Total'
    
    # Criar df_para_grafico_periodo a partir de df_filtrado (antes do filtro de perÃ­odo)
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_para_grafico_periodo = df_filtrado.copy()
    else:
        df_para_grafico_periodo = pd.DataFrame()
    
    with tab1:
        # Exibir grÃ¡fico por PerÃ­odo
        # No modo CPU, a coluna 'CPU' pode nÃ£o existir ainda em df_visualizacao,
        # mas serÃ¡ criada dentro do bloco. Verificar apenas se 'PerÃ­odo' existe.
        if 'PerÃ­odo' in df_visualizacao.columns:
            # IMPORTANTE: Criar df_visualizacao_para_grafico usando df_para_grafico_periodo
            # (dados ANTES do filtro de perÃ­odo) para mostrar TODOS os perÃ­odos no grÃ¡fico
            # Aplicar a mesma lÃ³gica de preparaÃ§Ã£o de dados, mas usando df_para_grafico_periodo
            
            # Carregar dados de volume reais (necessÃ¡rio para cÃ¡lculo de FLEX)
            df_vol_calc_grafico = load_volume_data(ano_selecionado)
        
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
                if ('Oficina' in df_para_grafico_periodo.columns and
                        'PerÃ­odo' in df_para_grafico_periodo.columns):
                    tem_veiculo = 'VeÃ­culo' in df_para_grafico_periodo.columns
                    tem_ano = 'Ano' in df_para_grafico_periodo.columns
                    
                    # Aplicar mesmos filtros de VeÃ­culo e Oficina ao volume
                    # Preferir volume jÃ¡ filtrado com a sidebar (mesma base das tabelas)
                    if (
                        'df_vol_filtrado_sidebar' in locals()
                        and df_vol_filtrado_sidebar is not None
                        and hasattr(df_vol_filtrado_sidebar, 'columns')
                        and 'Volume' in df_vol_filtrado_sidebar.columns
                    ):
                        df_vol_calc_filtrado_grafico = df_vol_filtrado_sidebar.copy()
                    else:
                        df_vol_calc_filtrado_grafico = df_vol_calc_grafico.copy()
                # ðŸ”§ IMPORTANTE: NÃƒO recortar o volume usando "quais veÃ­culos/oficinas aparecem no custo".
                # VeÃ­culos/oficinas podem ter volume mesmo sem despesa; nesse caso custo=0 e o volume deve entrar no denominador.
                
                colunas_agrupamento_grafico = ['Oficina', 'PerÃ­odo']
                if tem_ano:
                    colunas_agrupamento_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_grafico.append('VeÃ­culo')
                
                if 'Total' in df_para_grafico_periodo.columns:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Total'].sum()
                else:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Valor'].sum()
                    df_total_agrupado_grafico.rename(columns={'Valor': 'Total'}, inplace=True)
                
                colunas_agrupamento_vol_grafico = ['Oficina', 'PerÃ­odo']
                if tem_ano:
                    colunas_agrupamento_vol_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_vol_grafico.append('VeÃ­culo')
                
                df_vol_agrupado_grafico = df_vol_calc_filtrado_grafico.groupby(
                    colunas_agrupamento_vol_grafico, as_index=False
                )['Volume'].sum()
                
                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Volume nÃ£o pode ser recortado pela existÃªncia de custo.
                # Se existir volume em um VeÃ­culo/PerÃ­odo sem despesa, o custo Ã© 0 e o volume deve entrar no denominador.
                df_cpu_grafico = pd.merge(
                    df_total_agrupado_grafico,
                    df_vol_agrupado_grafico,
                    on=colunas_agrupamento_grafico,
                    how='outer'
                )
                df_cpu_grafico['Volume'] = pd.to_numeric(df_cpu_grafico.get('Volume'), errors='coerce').fillna(0)
                df_cpu_grafico['Total'] = pd.to_numeric(df_cpu_grafico.get('Total'), errors='coerce').fillna(0)
                
                # Calcular CPU - VETORIZADO
                df_cpu_grafico['CPU'] = np.where(
                    (df_cpu_grafico['Volume'].notna()) & (df_cpu_grafico['Volume'] != 0),
                    df_cpu_grafico['Total'] / df_cpu_grafico['Volume'],
                    0
                )
                
                # IMPORTANTE: Manter colunas Total e Volume para que o grÃ¡fico possa recalcular CPU corretamente
                # O grÃ¡fico agrupa por Ano e PerÃ­odo e recalcula CPU a partir de Total e Volume agregados
                df_visualizacao_para_grafico = df_cpu_grafico.copy()
                coluna_visualizacao_grafico = 'CPU'
            else:
                df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
                coluna_visualizacao_grafico = 'Total' if 'Total' in df_para_grafico_periodo.columns else 'Valor'
        else:
            df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
            coluna_visualizacao_grafico = 'Total' if 'Total' in df_para_grafico_periodo.columns else 'Valor'
        
        # Filtros especÃ­ficos para este grÃ¡fico (multiselect)
        df_grafico_periodo = df_visualizacao_para_grafico.copy()
        
        # Inicializar variÃ¡veis de filtro
        oficina_selecionadas_grafico = ["Todos"]
        veiculo_selecionados_grafico = ["Todos"]
        
        # Criar colunas para os filtros
        col1, col2 = st.columns(2)
        
        # Filtro de Oficina
        with col1:
            if 'Oficina' in df_grafico_periodo.columns:
                oficina_opcoes_grafico = st.session_state.get('_oficina_opcoes_tc_ext')
                if not isinstance(oficina_opcoes_grafico, list) or not oficina_opcoes_grafico:
                    oficinas_set = set(df_grafico_periodo['Oficina'].dropna().astype(str).unique().tolist())
                    oficinas_set.update(get_budget_oficinas_opcoes(ano_selecionado))
                    oficinas_set.update(get_budget_volume_oficinas_opcoes(ano_selecionado))
                    oficina_opcoes_grafico = ["Todos"] + sorted(oficinas_set)

                if 'filtro_oficina_grafico_periodo' not in st.session_state:
                    st.session_state['filtro_oficina_grafico_periodo'] = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])

                default_grafico = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])
                if not all(x in oficina_opcoes_grafico for x in default_grafico):
                    default_grafico = ["Todos"]
                oficina_selecionadas_grafico = st.multiselect(
                    "ðŸ­ Filtrar por Oficina:",
                    oficina_opcoes_grafico,
                    default=default_grafico,
                    key="filtro_oficina_grafico_periodo",
                )
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
        
        # Filtro de VeÃ­culo
        with col2:
            if 'VeÃ­culo' in df_grafico_periodo.columns:
                veiculo_opcoes_grafico = st.session_state.get('_veiculo_opcoes_tc_ext')
                if not isinstance(veiculo_opcoes_grafico, list) or not veiculo_opcoes_grafico:
                    veiculo_opcoes_grafico = get_filter_options(df_grafico_periodo, 'VeÃ­culo')

                if 'filtro_veiculo_grafico_periodo' not in st.session_state:
                    st.session_state['filtro_veiculo_grafico_periodo'] = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])

                default_veiculo_grafico = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])
                if not all(x in veiculo_opcoes_grafico for x in default_veiculo_grafico):
                    default_veiculo_grafico = ["Todos"]
                veiculo_selecionados_grafico = st.multiselect(
                    "ðŸš— Filtrar por VeÃ­culo:",
                    veiculo_opcoes_grafico,
                    default=default_veiculo_grafico,
                    key="filtro_veiculo_grafico_periodo",
                )
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        # IMPORTANTE: Quando "Todos" estÃ¡ selecionado, garantir que todos os perÃ­odos de todos os anos sejam mostrados
        # O create_period_chart jÃ¡ faz o agrupamento correto por Ano e PerÃ­odo quando hÃ¡ coluna Ano
        
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar filtros do grÃ¡fico aos dados de volume e budget DEPOIS que os filtros sÃ£o definidos
        # Os filtros de Oficina e VeÃ­culo do grÃ¡fico devem ser aplicados a TODOS os dados (volumes, budget, etc.)
        
        # Carregar dados de budget e aplicar mesmos filtros
        df_budget_filtrado = None
        df_budget_vol_filtrado = None
        
        try:
            # Carregar dados de budget
            df_budget = load_budget_data(ano_selecionado)
            df_budget_vol = load_budget_volume_data(ano_selecionado)

            def _aplicar_filtro_selecionado(df_in, coluna_filtro, chave_state):
                if df_in is None or coluna_filtro not in df_in.columns:
                    return df_in
                selecionadas = st.session_state.get(chave_state, ["Todos"])
                if isinstance(selecionadas, tuple):
                    selecionadas = list(selecionadas)
                if selecionadas and "Todos" not in selecionadas:
                    df_in = df_in[df_in[coluna_filtro].astype(str).isin([str(x) for x in selecionadas])].copy()
                return df_in
            
            if df_budget is not None:
                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar fator de conversÃ£o na coluna Total do budget (mesma unidade que Total real)
                # Isso mantÃ©m os dados na mesma unidade para comparaÃ§Ãµes consistentes
                # IMPORTANTE: NÃƒO aplicar fator quando estÃ¡ em modo CPU (CPU jÃ¡ Ã© uma razÃ£o)
                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Total' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Total'] = df_budget['Total'] / 1000
                    elif fator_conversao == "M (MilhÃµes)":
                        df_budget['Total'] = df_budget['Total'] / 1000000
                
                # Aplicar conversÃ£o de moeda DEPOIS do fator de conversÃ£o (mesma lÃ³gica do fator)
                # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
                if moeda_codigo != "BRL" and 'Total' in df_budget.columns:
                    df_budget = converter_coluna_moeda(df_budget, 'Total', moeda_codigo, taxas_cambio)
                
                # âœ… Aplicar apenas filtros efetivamente selecionados (sidebar), sem interseÃ§Ã£o com o Real
                df_budget_filtrado = df_budget.copy()

                # Filtros principais
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'Oficina', 'filtro_oficina_tc_ext')
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'VeÃ­culo', 'filtro_veiculo_tc_ext')
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'USI', 'filtro_usi_tc_ext')
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'PerÃ­odo', 'filtro_periodo_tc_ext')

                # Filtros adicionais (mesmos nomes usados na sidebar)
                for col_filtro in ['Centrocst', 'NÂºconta', 'Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                    df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, col_filtro, f'filtro_{col_filtro}_tc_ext')

                for col_filtro in ['UsuÃ¡rio', 'Material', 'Dt.lÃ§to.', 'Texto breve']:
                    df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, col_filtro, f'filtro_avancado_{col_filtro}_tc_ext')

                # Filtros do grÃ¡fico (Oficina/VeÃ­culo) - normalmente sincronizados, mas mantidos por seguranÃ§a
                if 'Oficina' in df_budget_filtrado.columns and oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
                if 'VeÃ­culo' in df_budget_filtrado.columns and veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
            
            if df_budget_vol is not None:
                # âœ… Aplicar apenas filtros efetivamente selecionados (sidebar), sem interseÃ§Ã£o com o Real
                df_budget_vol_filtrado = df_budget_vol.copy()

                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'Oficina', 'filtro_oficina_tc_ext')
                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'VeÃ­culo', 'filtro_veiculo_tc_ext')
                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'USI', 'filtro_usi_tc_ext')
                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'PerÃ­odo', 'filtro_periodo_tc_ext')

                for col_filtro in ['Centrocst', 'NÂºconta', 'Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                    df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, col_filtro, f'filtro_{col_filtro}_tc_ext')

                for col_filtro in ['UsuÃ¡rio', 'Material', 'Dt.lÃ§to.', 'Texto breve']:
                    df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, col_filtro, f'filtro_avancado_{col_filtro}_tc_ext')

                if 'Oficina' in df_budget_vol_filtrado.columns and oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
                if 'VeÃ­culo' in df_budget_vol_filtrado.columns and veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        except Exception as e:
            st.sidebar.warning(f"âš ï¸ Erro ao carregar dados de budget: {e}")
        
        # Criar grÃ¡fico com dados filtrados (usar coluna_visualizacao_grafico que foi criada acima)
        # O create_period_chart jÃ¡ faz o agrupamento correto por Ano e PerÃ­odo quando hÃ¡ coluna Ano
        # Preparar dados de volume reais para cÃ¡lculo de FLEX
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar TODOS os filtros da sidebar ao volume (mesmos de df_para_grafico_periodo)
        # O volume precisa ter os mesmos filtros que os dados reais para garantir consistÃªncia
        df_volume_real_filtrado = None
        if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
            # âœ… Usar a mesma lÃ³gica da aba Volume: filtros da sidebar, sem interseÃ§Ã£o com o Real (custo)
            # Isso evita cortar volume para apenas veÃ­culos/oficinas que aparecem no realizado.
            df_volume_real_filtrado = filtrar_volume_com_sidebar(df_vol_calc_grafico, df_total)
            
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar filtros do grÃ¡fico (Oficina e VeÃ­culo) ao volume DEPOIS que os filtros sÃ£o definidos
            # Isso garante que o volume responda aos filtros do grÃ¡fico
            if df_volume_real_filtrado is not None:
                # Aplicar filtro de Oficina do grÃ¡fico
                if 'Oficina' in df_volume_real_filtrado.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Aplicar filtro de VeÃ­culo do grÃ¡fico
                if 'VeÃ­culo' in df_volume_real_filtrado.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar filtros do grÃ¡fico (Oficina e VeÃ­culo) aos dados de budget DEPOIS que os filtros sÃ£o definidos
        # Isso garante que os dados de budget respondam aos filtros do grÃ¡fico
        if df_budget_filtrado is not None:
            # Aplicar filtro de Oficina do grÃ¡fico
            if 'Oficina' in df_budget_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de VeÃ­culo do grÃ¡fico
            if 'VeÃ­culo' in df_budget_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        if df_budget_vol_filtrado is not None:
            # Aplicar filtro de Oficina do grÃ¡fico
            if 'Oficina' in df_budget_vol_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de VeÃ­culo do grÃ¡fico
            if 'VeÃ­culo' in df_budget_vol_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()

        # ðŸ“Š Resumo TC Ext (acima do grÃ¡fico) - mesmos indicadores da tabela Flex
        try:
            if (
                df_budget_filtrado is not None
                and df_budget_vol_filtrado is not None
                and df_volume_real_filtrado is not None
                and 'Total' in df_budget_filtrado.columns
                and 'Total' in df_filtrado.columns
            ):
                # Totais (jÃ¡ estÃ£o na mesma moeda/fator aplicados nos DataFrames)
                total_real_custo = pd.to_numeric(df_filtrado['Total'], errors='coerce').fillna(0).sum()

                # Aplicar tambÃ©m os filtros do grÃ¡fico (Oficina e VeÃ­culo) ao total real do resumo
                df_real_para_resumo = df_filtrado.copy()
                if 'Oficina' in df_real_para_resumo.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'VeÃ­culo' in df_real_para_resumo.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                total_real_custo = pd.to_numeric(df_real_para_resumo['Total'], errors='coerce').fillna(0).sum()

                bud_total_custo = pd.to_numeric(df_budget_filtrado['Total'], errors='coerce').fillna(0).sum()

                volume_real_total = 0.0
                if 'Volume' in df_volume_real_filtrado.columns:
                    df_vol_real_para_resumo = df_volume_real_filtrado
                    # ðŸ”§ CORREÃ‡ÃƒO: aplicar mesmos filtros do grÃ¡fico tambÃ©m nos volumes
                    if 'Oficina' in df_vol_real_para_resumo.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_vol_real_para_resumo = df_vol_real_para_resumo[
                                df_vol_real_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'VeÃ­culo' in df_vol_real_para_resumo.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_vol_real_para_resumo = df_vol_real_para_resumo[
                                df_vol_real_para_resumo['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                    volume_real_total = float(pd.to_numeric(df_vol_real_para_resumo['Volume'], errors='coerce').fillna(0).sum())

                volume_budget_total = 0.0
                if 'Volume' in df_budget_vol_filtrado.columns:
                    df_vol_bud_para_resumo = df_budget_vol_filtrado
                    # ðŸ”§ CORREÃ‡ÃƒO: aplicar mesmos filtros do grÃ¡fico tambÃ©m nos volumes
                    if 'Oficina' in df_vol_bud_para_resumo.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_vol_bud_para_resumo = df_vol_bud_para_resumo[
                                df_vol_bud_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'VeÃ­culo' in df_vol_bud_para_resumo.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_vol_bud_para_resumo = df_vol_bud_para_resumo[
                                df_vol_bud_para_resumo['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                    volume_budget_total = float(pd.to_numeric(df_vol_bud_para_resumo['Volume'], errors='coerce').fillna(0).sum())

                # Split budget por custo (Fixo / NÃ£o-Fixo) para cÃ¡lculo do Flex BUD
                # Regra: tudo que NÃƒO Ã© Fixo Ã© flexÃ­vel
                bud_fixo = 0.0
                if 'Custo' in df_budget_filtrado.columns:
                    custo_str = df_budget_filtrado['Custo'].astype(str).str.lower()
                    mask_fixo = custo_str.str.startswith('fix')
                    bud_fixo = pd.to_numeric(df_budget_filtrado.loc[mask_fixo, 'Total'], errors='coerce').fillna(0).sum()
                bud_nao_fixo = float(bud_total_custo - bud_fixo)

                proporcao_volume = (volume_real_total / volume_budget_total) if volume_budget_total not in (0, None) else 1.0
                flex_bud_total_custo = float(bud_fixo + (bud_nao_fixo * proporcao_volume))

                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    total_exibir = (total_real_custo / volume_real_total) if volume_real_total not in (0, None) else 0.0
                    flex_exibir = (flex_bud_total_custo / volume_real_total) if volume_real_total not in (0, None) else 0.0
                    bud_exibir = (bud_total_custo / volume_budget_total) if volume_budget_total not in (0, None) else 0.0
                    sufixo = ""
                else:
                    total_exibir = float(total_real_custo)
                    flex_exibir = float(flex_bud_total_custo)
                    bud_exibir = float(bud_total_custo)
                    sufixo = ""
                    if fator_conversao:
                        if fator_conversao == "K (milhares)":
                            sufixo = " K"
                        elif fator_conversao == "M (MilhÃµes)":
                            sufixo = " M"

                flex_menos_bud = flex_exibir - bud_exibir
                total_menos_flex = total_exibir - flex_exibir
                total_div_flex = (total_exibir / flex_exibir) if flex_exibir not in (0, None) else 0.0

                def _fmt_val(v):
                    return f"{v:,.2f}{sufixo}"

                st.subheader("ðŸ“Š Resumo TC Ext")
                st.markdown(
                    """
                    <style>
                    .tc-kpi-card {padding: 0.6rem 0.8rem; border: 1px solid rgba(49, 51, 63, 0.15); border-radius: 8px; background: rgba(0, 0, 0, 0.02);}
                    .tc-kpi-label {opacity: 0.75;}
                    .tc-kpi-value {font-size: 1.1em; font-weight: 600;}
                    .tc-kpi-spacer {display: block; height: 1.75rem;}
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                def _render_tc_kpi(label, value):
                    st.markdown(
                        f"""
                        <div class=\"tc-kpi-card\">
                            <div class=\"tc-kpi-label\">{label}</div>
                            <div class=\"tc-kpi-value\">{value}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                c1, c2, c3, c4, c5, c6 = st.columns(6)
                with c1:
                    _render_tc_kpi("BUD", _fmt_val(bud_exibir))
                with c2:
                    _render_tc_kpi("Flex Bud - BUD", _fmt_val(flex_menos_bud))
                with c3:
                    _render_tc_kpi("Flex BUD", _fmt_val(flex_exibir))
                with c4:
                    _render_tc_kpi("Total - Flex Bud", _fmt_val(total_menos_flex))
                with c5:
                    _render_tc_kpi("Total", _fmt_val(total_exibir))
                with c6:
                    _render_tc_kpi("Total / Flex Bud", f"{total_div_flex:.0%}")

                st.markdown("<div class='tc-kpi-spacer'></div>", unsafe_allow_html=True)
        except Exception:
            # Se algo der errado no resumo, nÃ£o quebrar a tela.
            pass
        
        # No modo CPU, precisamos passar os dados originais (com 'Custo') para calcular FLEX
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Usar df_total diretamente (que tem 'Custo') em vez de df_para_grafico_periodo
        # porque df_para_grafico_periodo pode nÃ£o ter 'Custo' se foi processado
        df_real_original_grafico = None
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # IMPORTANTE: No modo CPU, usar df_filtrado diretamente que jÃ¡ tem TODOS os filtros aplicados
            # e tem a coluna 'Custo' necessÃ¡ria para calcular Flex Bud
            # df_filtrado jÃ¡ tem a conversÃ£o de moeda aplicada e todos os filtros da sidebar
            if 'Custo' in df_filtrado.columns and 'Total' in df_filtrado.columns:
                df_real_original_grafico = df_filtrado.copy()
                
                # Aplicar apenas os filtros do grÃ¡fico (Oficina e VeÃ­culo) se diferentes dos da sidebar
                # Filtro de Oficina do grÃ¡fico
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Filtro de VeÃ­culo do grÃ¡fico
                if 'VeÃ­culo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # NOTA: A conversÃ£o de moeda jÃ¡ foi aplicada no df_total (linha 1104) e df_filtrado herda isso
                # Portanto, df_real_original_grafico['Total'] jÃ¡ estÃ¡ convertido
                
                # ðŸ”§ VERIFICAÃ‡ÃƒO: Garantir que df_real_original_grafico tem dados vÃ¡lidos apÃ³s aplicar filtros
                if len(df_real_original_grafico) == 0:
                    st.warning("âš ï¸ Aviso: df_real_original_grafico estÃ¡ vazio apÃ³s aplicar filtros. Verifique os filtros selecionados.")
                elif 'Total' in df_real_original_grafico.columns and abs(df_real_original_grafico['Total'].sum()) < 0.0001:
                    st.warning("âš ï¸ Aviso: df_real_original_grafico tem Total muito prÃ³ximo de zero. Verifique os dados e filtros.")
            else:
                # Fallback: tentar usar df_para_grafico_periodo se df_total nÃ£o tiver 'Custo'
                df_real_original_grafico = df_para_grafico_periodo.copy()
                # Aplicar mesmos filtros de Oficina e VeÃ­culo
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'VeÃ­culo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # ObservaÃ§Ã£o: meses faltantes sÃ£o tratados no create_period_chart
        # (perÃ­odos do budget entram apenas no eixo; realizado fica vazio/zero)

        # =============================
        # Resumo (tabelas) Budget x Real por Oficina
        # =============================
        with st.expander("ðŸ“‹ Resumo Budget e Real Oficinas", expanded=False):
            # Base Real (usar df_filtrado + filtros do grÃ¡fico para consistÃªncia)
            df_real_resumo_tab1 = None
            try:
                if 'df_filtrado' in locals() and df_filtrado is not None:
                    df_real_resumo_tab1 = df_filtrado.copy()
                    if 'Oficina' in df_real_resumo_tab1.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_resumo_tab1 = df_real_resumo_tab1[
                                df_real_resumo_tab1['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'VeÃ­culo' in df_real_resumo_tab1.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_resumo_tab1 = df_real_resumo_tab1[
                                df_real_resumo_tab1['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
            except Exception:
                df_real_resumo_tab1 = None

            # Budget (BDG)
            df_tab_budget = _montar_tabela_resumo_oficinas(
                df_budget_filtrado,
                tipo_visualizacao,
                index_name="BDG",
                coluna_valor_preferida="Total",
                df_volume=df_budget_vol_filtrado,
            )
            if df_tab_budget is None or df_tab_budget.empty:
                st.info("â„¹ï¸ Sem dados de Budget para exibir no resumo.")
            else:
                st.markdown("**Budget (BDG)**")
                st.dataframe(
                    df_tab_budget.style.format(lambda x: _formatar_num_ptbr(x, 2)),
                    use_container_width=True,
                )

            # Real
            df_tab_real = _montar_tabela_resumo_oficinas(
                df_real_resumo_tab1,
                tipo_visualizacao,
                index_name="REAL",
                coluna_valor_preferida="Total",
                df_volume=df_volume_real_filtrado,
            )
            if df_tab_real is None or df_tab_real.empty:
                st.info("â„¹ï¸ Sem dados de Real para exibir no resumo.")
            else:
                st.markdown("**Real (Realizado)**")
                st.dataframe(
                    df_tab_real.style.format(lambda x: _formatar_num_ptbr(x, 2)),
                    use_container_width=True,
                )
        
        # Exibir tÃ­tulo do grÃ¡fico apÃ³s os filtros para evitar sobreposiÃ§Ã£o
        st.markdown("<br>", unsafe_allow_html=True)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("ðŸ“Š CPU por PerÃ­odo")
        else:
            st.subheader("ðŸ“Š Soma do Valor por PerÃ­odo")

        # Validar dados antes de criar grÃ¡fico
        if df_grafico_periodo is None or df_grafico_periodo.empty:
            st.warning("âš ï¸ Dados do grÃ¡fico estÃ£o vazios. Verifique os filtros aplicados.")
        elif coluna_visualizacao_grafico not in df_grafico_periodo.columns:
            st.warning(f"âš ï¸ Coluna '{coluna_visualizacao_grafico}' nÃ£o encontrada nos dados do grÃ¡fico.")
            st.warning(f"âš ï¸ Colunas disponÃ­veis: {list(df_grafico_periodo.columns)[:10]}")
        else:
            # Criar placeholder para o grÃ¡fico (forÃ§a renderizaÃ§Ã£o imediata)
            chart_placeholder = st.empty()
            
            # Criar grÃ¡fico (sem spinner para evitar bloqueio de renderizaÃ§Ã£o)
            try:
                if 'PerÃ­odo' not in df_grafico_periodo.columns:
                    chart_placeholder.error("âŒ Coluna 'PerÃ­odo' nÃ£o encontrada nos dados do grÃ¡fico.")
                elif df_grafico_periodo[coluna_visualizacao_grafico].isna().all():
                    chart_placeholder.warning("âš ï¸ Todos os valores na coluna sÃ£o NaN. Verifique os dados.")
                else:
                    grafico_periodo = create_period_chart(
                        df_grafico_periodo, coluna_visualizacao_grafico, tipo_visualizacao,
                        df_budget_filtrado, df_budget_vol_filtrado, df_volume_real_filtrado,
                        df_real_original_grafico,  # Dados originais com 'Custo' para calcular FLEX
                        moeda_simbolo,  # Passar sÃ­mbolo da moeda para o grÃ¡fico
                        debug=False,
                        debug_context=""
                    )
                    if grafico_periodo is not None:
                        # Exibir grÃ¡fico no placeholder (renderizaÃ§Ã£o imediata)
                        chart_placeholder.altair_chart(grafico_periodo, use_container_width=True)
                    else:
                        chart_placeholder.warning("âš ï¸ O grÃ¡fico nÃ£o pÃ´de ser criado. Verifique os dados e filtros aplicados.")
            except Exception as e:
                import traceback
                chart_placeholder.error(f"âŒ Erro ao criar grÃ¡fico: {str(e)}")
                chart_placeholder.code(traceback.format_exc())
        
        # Tabela: AnÃ¡lise Flex Bud por Categoria
        if df_budget_filtrado is not None and df_budget_vol_filtrado is not None and df_volume_real_filtrado is not None:
            st.markdown("---")
            # Adicionar elemento com ID para scroll
            st.markdown('<div id="analise-flex-bud-por-categoria"></div>', unsafe_allow_html=True)
            st.subheader("ðŸ“Š AnÃ¡lise Flex por Categoria")
            
            # Verificar se temos coluna 'Custo' nos dados
            tem_custo_real = False
            if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns:
                tem_custo_real = True
            elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns:
                tem_custo_real = True
            
            if 'Custo' in df_budget_filtrado.columns and tem_custo_real:
                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Preparar dados reais para a tabela
                # IMPORTANTE: No modo CPU, precisamos de dados com Total em Custo Total (nÃ£o em CPU)
                # Priorizar df_real_original_grafico que vem diretamente de df_total (sem processamento de CPU)
                if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns and 'Total' in df_real_original_grafico.columns:
                    df_real_tabela = df_real_original_grafico.copy()
                elif 'df_filtrado' in locals() and df_filtrado is not None and 'Custo' in df_filtrado.columns and 'Total' in df_filtrado.columns:
                    # Usar df_filtrado que tem Total em Custo Total (sem processamento de CPU)
                    df_real_tabela = df_filtrado.copy()
                elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns and 'Total' in df_grafico_periodo.columns:
                    # Fallback: usar df_grafico_periodo se tiver Total (pode estar em CPU, mas vamos verificar)
                    # Se estiver em modo CPU e df_grafico_periodo tem CPU mas nÃ£o Total, nÃ£o usar
                    df_real_tabela = df_grafico_periodo.copy()
                else:
                    df_real_tabela = None
                
                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Aplicar filtros do grÃ¡fico (Oficina e VeÃ­culo) aos dados reais da tabela
                # Isso garante que a tabela responda aos filtros do grÃ¡fico
                if df_real_tabela is not None:
                    # Aplicar filtro de Oficina do grÃ¡fico
                    if 'Oficina' in df_real_tabela.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de VeÃ­culo do grÃ¡fico
                    if 'VeÃ­culo' in df_real_tabela.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                
                if df_real_tabela is None or len(df_real_tabela) == 0:
                    st.info("â„¹ï¸ NÃ£o hÃ¡ dados reais disponÃ­veis para criar a tabela Flex Bud.")
                elif 'Custo' not in df_real_tabela.columns:
                    st.error("âŒ Erro: df_real_tabela nÃ£o tem coluna 'Custo'. Verifique a origem dos dados.")
                elif 'Total' not in df_real_tabela.columns:
                    st.error("âŒ Erro: df_real_tabela nÃ£o tem coluna 'Total'. Verifique a origem dos dados.")
                else:
                    # Agrupar dados reais por Custo, Type 05, Type 06, Account (se existir)
                    colunas_agrupamento = ['Custo']
                    if 'Type 05' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 05')
                    if 'Type 06' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 06')
                    if 'Account' in df_real_tabela.columns:
                        colunas_agrupamento.append('Account')
                    
                    # ðŸ”§ CORREÃ‡ÃƒO: Calcular Flex Bud POR PERÃODO primeiro (mesma lÃ³gica do grÃ¡fico)
                    # Incluir PerÃ­odo no agrupamento para calcular por perÃ­odo
                    colunas_agrupamento_com_periodo = colunas_agrupamento.copy()
                    if 'PerÃ­odo' in df_real_tabela.columns:
                        colunas_agrupamento_com_periodo.append('PerÃ­odo')
                    if 'Ano' in df_real_tabela.columns:
                        colunas_agrupamento_com_periodo.append('Ano')
                    
                    # ðŸ”§ VERIFICAÃ‡ÃƒO: Garantir que df_real_tabela tem Total em Custo Total (nÃ£o em CPU)
                    # Se df_real_tabela tem coluna 'CPU' mas nÃ£o 'Total', hÃ¡ problema
                    if 'Total' not in df_real_tabela.columns:
                        st.error("âŒ Erro: df_real_tabela nÃ£o tem coluna 'Total'. Verifique a origem dos dados.")
                        df_real_agrupado = pd.DataFrame()
                    else:
                        # Normalizar o rÃ³tulo de Custo para garantir governanÃ§a (Fixo Ã© fixo; VariÃ¡vel 100% variÃ¡vel)
                        df_real_tabela = df_real_tabela.copy()
                        df_real_tabela['Custo'] = df_real_tabela['Custo'].apply(_normalizar_rotulo_custo)
                        df_budget_filtrado = df_budget_filtrado.copy()
                        if 'Custo' in df_budget_filtrado.columns:
                            df_budget_filtrado['Custo'] = df_budget_filtrado['Custo'].apply(_normalizar_rotulo_custo)
                        # Normalizar PerÃ­odo para evitar mismatch (ex: 'janeiro' vs 'Janeiro')
                        mapeamento_meses = {
                            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                        }

                        def _normalizar_periodo_local(periodo):
                            if pd.isna(periodo):
                                return periodo
                            periodo_str = str(periodo).strip()
                            for mes_min, mes_cap in mapeamento_meses.items():
                                if periodo_str.lower() == mes_min.lower():
                                    return mes_cap
                            return periodo_str

                        if 'PerÃ­odo' in df_real_tabela.columns:
                            df_real_tabela['PerÃ­odo'] = df_real_tabela['PerÃ­odo'].apply(_normalizar_periodo_local)
                        if 'PerÃ­odo' in df_budget_filtrado.columns:
                            df_budget_filtrado['PerÃ­odo'] = df_budget_filtrado['PerÃ­odo'].apply(_normalizar_periodo_local)
                        if df_volume_real_filtrado is not None and 'PerÃ­odo' in df_volume_real_filtrado.columns:
                            df_volume_real_filtrado = df_volume_real_filtrado.copy()
                            df_volume_real_filtrado['PerÃ­odo'] = df_volume_real_filtrado['PerÃ­odo'].apply(_normalizar_periodo_local)
                        if df_budget_vol_filtrado is not None and 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                            df_budget_vol_filtrado = df_budget_vol_filtrado.copy()
                            df_budget_vol_filtrado['PerÃ­odo'] = df_budget_vol_filtrado['PerÃ­odo'].apply(_normalizar_periodo_local)

                        # Agrupar dados reais por categoria E perÃ­odo
                        # IMPORTANTE: NÃ£o verificar se Total estÃ¡ zerado antes de agrupar, pois pode haver
                        # valores positivos e negativos que se cancelam no total, mas sÃ£o vÃ¡lidos por categoria
                        df_real_agrupado = df_real_tabela.groupby(colunas_agrupamento_com_periodo)['Total'].sum().reset_index()
                    
                    # Agrupar dados de budget por categoria E perÃ­odo
                    colunas_budget_periodo = [col for col in colunas_agrupamento_com_periodo if col in df_budget_filtrado.columns]
                    df_budget_agrupado = df_budget_filtrado.groupby(colunas_budget_periodo)['Total'].sum().reset_index()
                    
                    # ðŸ”§ CORREÃ‡ÃƒO: Tab TC Ext usa dados de BUDGET reais (df_budget_agrupado)
                    # Aplicar mesmos filtros de categoria nos volumes (real e budget)
                    df_vol_real_agrupado = pd.DataFrame()
                    if 'Volume' in df_volume_real_filtrado.columns:
                        df_vol_real_para_agrupar = df_volume_real_filtrado.copy()
                        for col_filtro in ['Type 05', 'Type 06', 'Account']:
                            if col_filtro in df_vol_real_para_agrupar.columns and col_filtro in df_real_tabela.columns:
                                valores_presentes = df_real_tabela[col_filtro].dropna().unique()
                                if len(valores_presentes) > 0:
                                    df_vol_real_para_agrupar = df_vol_real_para_agrupar[
                                        df_vol_real_para_agrupar[col_filtro].isin(valores_presentes)
                                    ]
                        if 'Ano' in df_vol_real_para_agrupar.columns and 'Ano' in df_real_tabela.columns and 'PerÃ­odo' in df_vol_real_para_agrupar.columns:
                            df_vol_real_agrupado = df_vol_real_para_agrupar.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                        elif 'PerÃ­odo' in df_vol_real_para_agrupar.columns:
                            df_vol_real_agrupado = df_vol_real_para_agrupar.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                        else:
                            volume_total = df_vol_real_para_agrupar['Volume'].sum()
                            df_vol_real_agrupado = pd.DataFrame({'Volume': [volume_total]})
                    
                    df_vol_budget_agrupado = pd.DataFrame()
                    if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                        df_vol_budget_para_agrupar = df_budget_vol_filtrado.copy()
                        for col_filtro in ['Type 05', 'Type 06', 'Account']:
                            if col_filtro in df_vol_budget_para_agrupar.columns and col_filtro in df_budget_filtrado.columns:
                                valores_presentes = df_budget_filtrado[col_filtro].dropna().unique()
                                if len(valores_presentes) > 0:
                                    df_vol_budget_para_agrupar = df_vol_budget_para_agrupar[
                                        df_vol_budget_para_agrupar[col_filtro].isin(valores_presentes)
                                    ]
                        if 'Ano' in df_vol_budget_para_agrupar.columns and 'Ano' in df_budget_filtrado.columns and 'PerÃ­odo' in df_vol_budget_para_agrupar.columns:
                            df_vol_budget_agrupado = df_vol_budget_para_agrupar.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                        elif 'PerÃ­odo' in df_vol_budget_para_agrupar.columns:
                            df_vol_budget_agrupado = df_vol_budget_para_agrupar.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                        else:
                            volume_total = df_vol_budget_para_agrupar['Volume'].sum()
                            df_vol_budget_agrupado = pd.DataFrame({'Volume': [volume_total]})
                    
                    # Merge Real + Budget custos
                    df_tabela_flex = df_real_agrupado.merge(
                        df_budget_agrupado,
                        on=colunas_agrupamento_com_periodo,
                        how='outer',
                        suffixes=('', '_Budget')
                    )
                    df_tabela_flex['Total'] = df_tabela_flex['Total'].fillna(0)
                    df_tabela_flex['Total_Budget'] = df_tabela_flex['Total_Budget'].fillna(0)
                    df_tabela_flex['Budget_Total_Custo'] = df_tabela_flex['Total_Budget']
                    df_tabela_flex['Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                    
                    # Merge com volumes (real e budget) por PerÃ­odo (+ Ano quando existir)
                    chaves_merge_vol = ['PerÃ­odo']
                    if 'Ano' in df_tabela_flex.columns and len(df_vol_real_agrupado) > 0 and 'Ano' in df_vol_real_agrupado.columns:
                        chaves_merge_vol = ['Ano', 'PerÃ­odo']

                    if len(df_vol_real_agrupado) > 0 and all(c in df_vol_real_agrupado.columns for c in chaves_merge_vol):
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_real_agrupado[chaves_merge_vol + ['Volume']].rename(columns={'Volume': 'Volume_Real'}),
                            on=chaves_merge_vol,
                            how='left'
                        )
                        df_tabela_flex['Volume_Real'] = df_tabela_flex['Volume_Real'].fillna(0)
                    elif len(df_vol_real_agrupado) > 0:
                        volume_total_real = df_vol_real_agrupado['Volume'].sum() if 'Volume' in df_vol_real_agrupado.columns else 0
                        df_tabela_flex['Volume_Real'] = volume_total_real
                    else:
                        df_tabela_flex['Volume_Real'] = 0
                    
                    if len(df_vol_budget_agrupado) > 0 and all(c in df_vol_budget_agrupado.columns for c in chaves_merge_vol):
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_budget_agrupado[chaves_merge_vol + ['Volume']].rename(columns={'Volume': 'Volume_Budget'}),
                            on=chaves_merge_vol,
                            how='left'
                        )
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex['Volume_Budget'].fillna(0)
                    elif len(df_vol_budget_agrupado) > 0:
                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum() if 'Volume' in df_vol_budget_agrupado.columns else 0
                        df_tabela_flex['Volume_Budget'] = volume_total_budget
                    else:
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex.get('Volume_Real', 0)
                    
                    # Calcular Flex Bud usando operaÃ§Ãµes vetorizadas (muito mais rÃ¡pido)
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        # Calcular Flex Bud em Custo Total primeiro
                        # Fixo: Flex Bud = Budget
                        # NÃ£o-Fixo (VariÃ¡vel + Outros): Flex Bud = Budget * (Volume Real / Volume Budget)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operaÃ§Ãµes vetorizadas ao invÃ©s de apply (muito mais rÃ¡pido)
                        # ðŸ”’ GovernanÃ§a: Fixo nÃ£o flexibiliza (independente do texto vir como 'Fixo ', 'FIXO', etc.)
                        mask_fixo = _mask_custo_fixo(df_tabela_flex['Custo']) if 'Custo' in df_tabela_flex.columns else pd.Series(False, index=df_tabela_flex.index)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(mask_fixo, 0)
                        df_tabela_flex['_Flex_Bud_NaoFixo'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~mask_fixo, 0)
                        
                        df_tabela_flex['_Flex_Bud_Total_Custo'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_NaoFixo']
                        
                        # Converter para CPU
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Total_Custo'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['Flex BUD'].fillna(0)
                        
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['BUD'] = df_tabela_flex['BUD'].fillna(0)
                        
                        df_tabela_flex['Total'] = df_tabela_flex['Total'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Total'] = df_tabela_flex['Total'].fillna(0)
                        
                        # Guardar valores para agregaÃ§Ã£o
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['_Flex_Bud_Total_Custo']
                        # ðŸ”§ CORREÃ‡ÃƒO: _Total_Custo_Total deve ser o Total em Custo Total (antes da conversÃ£o para CPU)
                        # Total jÃ¡ estÃ¡ em CPU, entÃ£o precisamos reverter multiplicando por Volume_Real
                        # Mas Volume_Real Ã© o mesmo para todas as categorias do mesmo perÃ­odo (volume total do perÃ­odo)
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Total'] * df_tabela_flex['Volume_Real']  # Reverter para Custo Total
                    else:
                        # Custo Total
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operaÃ§Ãµes vetorizadas ao invÃ©s de apply (muito mais rÃ¡pido)
                        # ðŸ”’ GovernanÃ§a: Fixo nÃ£o flexibiliza
                        mask_fixo = _mask_custo_fixo(df_tabela_flex['Custo']) if 'Custo' in df_tabela_flex.columns else pd.Series(False, index=df_tabela_flex.index)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(mask_fixo, 0)
                        df_tabela_flex['_Flex_Bud_NaoFixo'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~mask_fixo, 0)
                        
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_NaoFixo']
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo']
                        
                        # Guardar valores para agregaÃ§Ã£o
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['Flex BUD']
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Total']
                    
                    # Guardar valores auxiliares
                    df_tabela_flex['_Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                    
                    # Calcular diferenÃ§as
                    df_tabela_flex['Flex Bud - BUD'] = df_tabela_flex['Flex BUD'] - df_tabela_flex['BUD']
                    df_tabela_flex['Total - Flex Bud'] = df_tabela_flex['Total'] - df_tabela_flex['Flex BUD']
                    # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, serÃ¡ convertido para % na formataÃ§Ã£o)
                    df_tabela_flex['Total / Flex Bud'] = df_tabela_flex.apply(
                        lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                        axis=1
                    )
                    
                    # Remover colunas auxiliares temporÃ¡rias
                    colunas_remover_temp = ['Budget_Total', 'Budget_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_NaoFixo']
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        colunas_remover_temp.append('_Flex_Bud_Total_Custo')
                    df_tabela_flex = df_tabela_flex.drop(columns=[col for col in colunas_remover_temp if col in df_tabela_flex.columns])

                    # ðŸ”§ FILTRAR: remover linhas totalmente zeradas/nulas (limpa o Resumo Geral)
                    if len(df_tabela_flex) > 0:
                        colunas_numericas_gerais = [
                            col for col in df_tabela_flex.columns
                            if pd.api.types.is_numeric_dtype(df_tabela_flex[col])
                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']
                        ]
                        if colunas_numericas_gerais:
                            df_tmp = df_tabela_flex[colunas_numericas_gerais].fillna(0)
                            df_tabela_flex = df_tabela_flex[df_tmp.abs().sum(axis=1) > 0.0001].copy()
                    
                    if len(df_tabela_flex) > 0:
                        
                        # Seletor de perÃ­odo (linha superior)
                        if 'PerÃ­odo' in df_real_tabela.columns:
                            # ðŸ”§ CORREÃ‡ÃƒO: nÃ£o limitar a meses do realizado.
                            # Usar uniÃ£o Real + Budget + Volume Budget para listar todos os meses.
                            periodos_set = set(df_real_tabela['PerÃ­odo'].dropna().astype(str).unique().tolist())
                            if 'df_budget_filtrado' in locals() and df_budget_filtrado is not None and 'PerÃ­odo' in df_budget_filtrado.columns:
                                periodos_set.update(df_budget_filtrado['PerÃ­odo'].dropna().astype(str).unique().tolist())
                            if 'df_budget_vol_filtrado' in locals() and df_budget_vol_filtrado is not None and 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                                periodos_set.update(df_budget_vol_filtrado['PerÃ­odo'].dropna().astype(str).unique().tolist())
                            # Garantir todos os meses sempre
                            periodos_set.update(ORDEM_MESES)
                            periodos_disponiveis = sorted([p for p in periodos_set if p and p != 'Todos'])
                            # Ordenar meses cronologicamente
                            meses_ordenados = []
                            outros_periodos = []
                            for periodo in periodos_disponiveis:
                                periodo_lower = str(periodo).lower()
                                if periodo_lower in ORDEM_MESES:
                                    meses_ordenados.append(periodo)
                                else:
                                    outros_periodos.append(periodo)
                            
                            meses_ordenados.sort(
                                key=lambda x: ORDEM_MESES.index(str(x).lower())
                                if str(x).lower() in ORDEM_MESES else 999
                            )
                            periodos_ordenados = meses_ordenados + outros_periodos
                            
                            # Novo filtro de perÃ­odos - versÃ£o simplificada
                            periodo_tabela_key = "filtro_periodo_tabela_flex"
                            
                            # Adicionar opÃ§Ã£o "Todos" no inÃ­cio da lista
                            opcoes_com_todos = ["Todos"] + periodos_ordenados
                            
                            # Inicializar session_state se necessÃ¡rio
                            if periodo_tabela_key not in st.session_state:
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Validar valores salvos
                            periodos_salvos = st.session_state[periodo_tabela_key]
                            periodos_validos = [p for p in periodos_salvos if p in opcoes_com_todos]
                            
                            # Se nÃ£o houver perÃ­odos vÃ¡lidos, resetar para "Todos"
                            if not periodos_validos:
                                periodos_validos = ["Todos"]
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Adicionar CSS simples para prevenir scroll automÃ¡tico
                            st.markdown("""
                            <style>
                                /* Prevenir scroll automÃ¡tico do Streamlit */
                                html {
                                    scroll-behavior: auto !important;
                                }
                                /* Prevenir foco automÃ¡tico que causa scroll */
                                [data-testid="stMultiSelect"] input:focus {
                                    scroll-margin: 0 !important;
                                }
                                /* Prevenir scroll quando o multiselect recebe foco */
                                [data-testid="stMultiSelect"] {
                                    scroll-margin: 0 !important;
                                }
                            </style>
                            """, unsafe_allow_html=True)
                            
                            # Criar o multiselect DEPOIS do JavaScript
                            # ðŸ”§ CORREÃ‡ÃƒO: Remover 'default' e usar apenas 'key' para evitar conflito
                            # O Streamlit automaticamente sincroniza o valor do widget com session_state[key]
                            periodos_tabela_raw = st.multiselect(
                                "ðŸ“… **PerÃ­odo(s):**",
                                opcoes_com_todos,
                                key=periodo_tabela_key
                            )
                            
                            # Atualizar session_state com o valor selecionado (caso tenha mudado)
                            if periodos_tabela_raw != st.session_state[periodo_tabela_key]:
                                st.session_state[periodo_tabela_key] = periodos_tabela_raw
                            
                            # Processar seleÃ§Ã£o
                            selecionou_todos_tabela = "Todos" in periodos_tabela_raw
                            if selecionou_todos_tabela:
                                # Se "Todos" estÃ¡ selecionado, selecionar todos os perÃ­odos para filtro
                                periodos_tabela = periodos_ordenados.copy()
                            else:
                                # Se "Todos" nÃ£o estÃ¡ selecionado, usar apenas os perÃ­odos selecionados
                                periodos_tabela = [p for p in periodos_tabela_raw if p != "Todos"]
                            
                            # Se nenhum perÃ­odo foi selecionado, usar todos (mas mostrar apenas "Todos")
                            if not periodos_tabela:
                                periodos_tabela = periodos_ordenados.copy()
                        else:
                            periodos_tabela = []
                        
                        # Filtrar df_tabela_flex por perÃ­odos selecionados
                        # Inicializar variÃ¡veis
                        usar_colunas_por_periodo = False
                        periodos_ordenados_selecao = []
                        
                        if len(periodos_tabela) > 0 and 'PerÃ­odo' in df_tabela_flex.columns and len(df_tabela_flex) > 0:
                            # ðŸ”§ IMPORTANTE: Salvar dados originais ANTES de filtrar (para usar em colunas por perÃ­odo)
                            df_tabela_flex_original = df_tabela_flex.copy()
                            
                            df_tabela_flex = df_tabela_flex[df_tabela_flex['PerÃ­odo'].isin(periodos_tabela)].copy()
                            
                            # ðŸ”§ CRÃTICO: Salvar df_tabela_flex DEPOIS do filtro de perÃ­odo, mas ANTES de qualquer transformaÃ§Ã£o
                            # Esta versÃ£o tem as colunas BUD, Flex BUD, Total originais e jÃ¡ estÃ¡ filtrada pelos perÃ­odos selecionados
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()
                            
                            # ðŸ”§ NOVA LÃ“GICA: Se hÃ¡ mÃºltiplos perÃ­odos, criar colunas separadas por perÃ­odo
                            # Manter a ordem de seleÃ§Ã£o dos perÃ­odos (periodos_tabela_raw mantÃ©m a ordem)
                            # Regra de UX: quando o usuÃ¡rio escolhe "Todos", manter estrutura padrÃ£o e mostrar total anual
                            if len(periodos_tabela) > 1:
                                # Manter a ordem de seleÃ§Ã£o (usar periodos_tabela_raw se disponÃ­vel, senÃ£o usar periodos_tabela)
                                if 'periodos_tabela_raw' in locals() and len(periodos_tabela_raw) > 0:
                                    periodos_ordenados_selecao = [p for p in periodos_tabela_raw if p != "Todos" and p in periodos_tabela]
                                else:
                                    periodos_ordenados_selecao = periodos_tabela.copy()
                                
                                # Se ainda nÃ£o temos a ordem correta, usar periodos_tabela
                                if not periodos_ordenados_selecao:
                                    periodos_ordenados_selecao = periodos_tabela.copy()

                                # Criar flag para indicar que vamos usar colunas por perÃ­odo
                                # IMPORTANTE: se selecionou "Todos", NÃƒO usar colunas por perÃ­odo (apenas somatÃ³rio anual)
                                usar_colunas_por_periodo = not bool(locals().get('selecionou_todos_tabela', False))
                            else:
                                periodos_ordenados_selecao = periodos_tabela.copy()
                                usar_colunas_por_periodo = False
                            
                            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Agregar corretamente quando hÃ¡ 1 ou mÃºltiplos perÃ­odos
                            # (mesma lÃ³gica do grÃ¡fico - calcular Flex Bud por perÃ­odo primeiro, depois agregar)
                            # O grÃ¡fico sempre soma todas as categorias primeiro e depois calcula Flex Bud Total
                            # A tabela deve fazer o mesmo: somar _Flex_Bud_Total de todas as categorias e dividir pelo volume total
                            if len(periodos_tabela) >= 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "CPU (Custo por Unidade)":
                                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: O grÃ¡fico calcula Flex Bud por perÃ­odo, entÃ£o devemos fazer o mesmo
                                # 1. Calcular Flex Bud por perÃ­odo e categoria (jÃ¡ feito acima)
                                # 2. Agregar por categoria somando Flex Bud Total e Volume Total
                                
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Usar volume TOTAL de todos os perÃ­odos selecionados (nÃ£o por categoria)
                                # O grÃ¡fico calcula por perÃ­odo usando volume total do perÃ­odo, entÃ£o devemos usar o mesmo aqui
                                # IMPORTANTE: O grÃ¡fico agrupa volumes por PerÃ­odo ANTES de calcular Flex BUD
                                # A tabela jÃ¡ tem df_vol_real_agrupado e df_vol_budget_agrupado que foram agrupados por PerÃ­odo
                                # EntÃ£o devemos usar esses DataFrames agrupados para garantir consistÃªncia
                                if len(df_vol_real_agrupado) > 0 and 'PerÃ­odo' in df_vol_real_agrupado.columns:
                                    # Usar o DataFrame jÃ¡ agrupado por PerÃ­odo (igual ao grÃ¡fico)
                                    volume_total_real = df_vol_real_agrupado[df_vol_real_agrupado['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                elif 'PerÃ­odo' in df_volume_real_filtrado.columns:
                                    # Fallback: agrupar por PerÃ­odo primeiro (igual ao grÃ¡fico), depois filtrar e somar
                                    df_vol_real_por_periodo = df_volume_real_filtrado.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                                    volume_total_real = df_vol_real_por_periodo[df_vol_real_por_periodo['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                else:
                                    volume_total_real = df_volume_real_filtrado['Volume'].sum()
                                
                                # Volume Budget: usar os dados de volume de BUDGET (nÃ£o o real)
                                if len(df_vol_budget_agrupado) > 0 and 'Volume' in df_vol_budget_agrupado.columns:
                                    if 'PerÃ­odo' in df_vol_budget_agrupado.columns and len(periodos_tabela) > 0:
                                        if 'Ano' in df_vol_budget_agrupado.columns and 'Ano' in df_tabela_flex.columns:
                                            # Se houver Ano, filtrar tambÃ©m pelo Ano presente em df_tabela_flex
                                            anos_sel = df_tabela_flex['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex.columns else []
                                            df_tmp = df_vol_budget_agrupado.copy()
                                            if anos_sel:
                                                df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                            volume_total_budget = df_tmp[df_tmp['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                        else:
                                            volume_total_budget = df_vol_budget_agrupado[df_vol_budget_agrupado['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                    else:
                                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum()
                                elif df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                                    if 'PerÃ­odo' in df_budget_vol_filtrado.columns and len(periodos_tabela) > 0:
                                        if 'Ano' in df_budget_vol_filtrado.columns and 'Ano' in df_tabela_flex.columns:
                                            anos_sel = df_tabela_flex['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex.columns else []
                                            df_tmp = df_budget_vol_filtrado.copy()
                                            if anos_sel:
                                                df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                            volume_total_budget = df_tmp[df_tmp['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                        else:
                                            volume_total_budget = df_budget_vol_filtrado[df_budget_vol_filtrado['PerÃ­odo'].isin(periodos_tabela)]['Volume'].sum()
                                    else:
                                        volume_total_budget = df_budget_vol_filtrado['Volume'].sum()
                                else:
                                    volume_total_budget = volume_total_real
                                
                                # ðŸ”§ CORREÃ‡ÃƒO: Agrupar por categoria (sem perÃ­odo) - somar valores em Custo Total
                                # IMPORTANTE: Somar _Flex_Bud_Total que jÃ¡ estÃ¡ em Custo Total (calculado por perÃ­odo)
                                # df_tabela_flex_original jÃ¡ foi criado antes do filtro (linha acima)
                                
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    '_Flex_Bud_Total': 'sum',  # Flex Bud Total em Custo Total (soma de todos os perÃ­odos)
                                    '_Total_Custo_Total': 'sum',  # Total em Custo Total (soma de todos os perÃ­odos)
                                    '_Budget_Total': 'sum'  # Budget em Custo Total (soma de todos os perÃ­odos)
                                }).reset_index()
                                
                                # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Usar volume TOTAL de todos os perÃ­odos (nÃ£o somar por categoria)
                                # O grÃ¡fico usa volume total por perÃ­odo, entÃ£o quando agregamos mÃºltiplos perÃ­odos,
                                # devemos usar a SOMA dos volumes de todos os perÃ­odos selecionados
                                df_agregado['_Volume_Real'] = volume_total_real
                                df_agregado['_Volume_Budget'] = volume_total_budget
                                
                                # Recalcular CPU usando operaÃ§Ãµes vetorizadas (muito mais rÃ¡pido)
                                # Flex BUD CPU = (Soma de Flex Bud Total de todos os perÃ­odos) / (Soma de Volume Real de todos os perÃ­odos)
                                df_agregado['Flex BUD'] = (df_agregado['_Flex_Bud_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['Total'] = (df_agregado['_Total_Custo_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['BUD'] = (df_agregado['_Budget_Total'] / df_agregado['_Volume_Budget'].replace(0, 1)).fillna(0)
                                
                                # Recalcular diferenÃ§as
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Total'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, serÃ¡ convertido para % na formataÃ§Ã£o)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # ðŸ”§ CORREÃ‡ÃƒO: Manter colunas auxiliares para o resumo geral recalcular corretamente
                                # NÃ£o remover ainda - serÃ£o removidas apÃ³s o cÃ¡lculo do resumo
                                # colunas_remover = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget']
                                # df_agregado = df_agregado.drop(columns=[col for col in colunas_remover if col in df_agregado.columns])
                                
                                # Se hÃ¡ mÃºltiplos perÃ­odos e devemos usar colunas por perÃ­odo, criar estrutura pivot
                                if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1 and 'PerÃ­odo' in df_tabela_flex_original.columns:
                                    # Usar dados originais antes da agregaÃ§Ã£o (ainda tem PerÃ­odo)
                                    df_tabela_flex_por_periodo = df_tabela_flex_original.copy()
                                    
                                    # Criar pivot table com perÃ­odos como colunas
                                    colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex_por_periodo.columns]
                                    
                                    # Criar pivot para cada mÃ©trica
                                    df_pivot_total = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='PerÃ­odo',
                                        values='Total',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_flex = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='PerÃ­odo',
                                        values='Flex BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_bud = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='PerÃ­odo',
                                        values='BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    # Reorganizar colunas na ordem de seleÃ§Ã£o
                                    periodos_ordenados_selecao_clean = [p for p in periodos_ordenados_selecao if p in df_pivot_total.columns]
                                    
                                    if periodos_ordenados_selecao_clean:
                                        df_pivot_total = df_pivot_total[periodos_ordenados_selecao_clean]
                                        df_pivot_flex = df_pivot_flex[periodos_ordenados_selecao_clean]
                                        df_pivot_bud = df_pivot_bud[periodos_ordenados_selecao_clean]
                                    
                                    # Criar DataFrame final com colunas reorganizadas
                                    df_final = df_pivot_total.reset_index()
                                    
                                    # Adicionar colunas na ordem especificada
                                    primeiro_periodo = periodos_ordenados_selecao_clean[0] if periodos_ordenados_selecao_clean else None
                                    primeiro_periodo_abrev = formatar_periodo_abreviado(primeiro_periodo) if primeiro_periodo else ""
                                    
                                    # Reorganizar colunas na ordem exata especificada
                                    # Ordem: Set/24, Flex set/24 - set/24, Flex set/24, Out/24 - Flex set/24, out/24, % out/24/flex set/24
                                    
                                    # Remover colunas de perÃ­odo do pivot (vamos criar novas colunas)
                                    for col in df_pivot_total.columns:
                                        if col in df_final.columns:
                                            df_final = df_final.drop(columns=[col])
                                    
                                    # Primeiro perÃ­odo: Set/24, Flex set/24 (removendo coluna redundante)
                                    if primeiro_periodo and primeiro_periodo in df_pivot_total.columns:
                                        df_final[f"{primeiro_periodo_abrev}"] = df_pivot_total[primeiro_periodo].values
                                        df_final[f"Flex {primeiro_periodo_abrev.lower()}"] = df_pivot_flex[primeiro_periodo].values
                                    
                                    # Demais perÃ­odos: Out/24 - Flex set/24, out/24, % out/24/flex set/24
                                    for periodo in periodos_ordenados_selecao_clean[1:]:
                                        periodo_abrev = formatar_periodo_abreviado(periodo)
                                        if periodo in df_pivot_total.columns and primeiro_periodo and primeiro_periodo in df_pivot_flex.columns:
                                            df_final[f"{periodo_abrev} - Flex {primeiro_periodo_abrev.lower()}"] = (df_pivot_total[periodo] - df_pivot_flex[primeiro_periodo]).values
                                            df_final[f"{periodo_abrev.lower()}"] = df_pivot_total[periodo].values
                                            # Calcular percentual
                                            df_final[f"% {periodo_abrev.lower()}/flex {primeiro_periodo_abrev.lower()}"] = (
                                                (df_pivot_total[periodo] / df_pivot_flex[primeiro_periodo].replace(0, 1)) * 100
                                            ).fillna(0).values
                                    
                                    df_tabela_flex = df_final
                                else:
                                    # Adicionar PerÃ­odo: se houver apenas 1 perÃ­odo, manter o nome; se mÃºltiplos, mostrar lista
                                    if len(periodos_tabela) == 1:
                                        df_agregado['PerÃ­odo'] = periodos_tabela[0]
                                    else:
                                        df_agregado['PerÃ­odo'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} perÃ­odos"
                                    
                                    df_tabela_flex = df_agregado
                            elif len(periodos_tabela) > 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "Custo Total":
                                # Para Custo Total: apenas somar por categoria (sem perÃ­odo)
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # Agrupar por categoria (sem perÃ­odo) e somar
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    'BUD': 'sum',
                                    'Flex BUD': 'sum',
                                    'Total': 'sum'
                                }).reset_index()
                                
                                # Recalcular diferenÃ§as
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Total'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, serÃ¡ convertido para % na formataÃ§Ã£o)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # Adicionar PerÃ­odo como "Todos" ou lista de perÃ­odos
                                df_agregado['PerÃ­odo'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} perÃ­odos"
                                
                                df_tabela_flex = df_agregado
                        else:
                            # Se nÃ£o houver filtro de perÃ­odo, usar df_tabela_flex diretamente
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()

                        # ðŸ”Ž GOVERNANÃ‡A (VISIBILIDADE): remover linhas totalmente zeradas/nulas apenas na EXIBIÃ‡ÃƒO.
                        # Importante: nÃ£o altera os totais, pois o resumo usa df_tabela_flex_para_resumo.
                        df_tabela_flex = _remover_linhas_sem_valores_para_exibicao(
                            df_tabela_flex,
                            colunas_ignorar=['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo', 'Ano'],
                        )
                        
                        # ðŸ”§ CORREÃ‡ÃƒO: Remover colunas auxiliares da tabela principal (para exibiÃ§Ã£o)
                        # df_tabela_flex_para_resumo jÃ¡ foi salvo DEPOIS do filtro de perÃ­odo, mas ANTES das transformaÃ§Ãµes
                        colunas_auxiliares = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget', '_Flex_Bud_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_NaoFixo', 'Volume_Real', 'Volume_Budget', 'Total_Budget']
                        colunas_para_remover = [col for col in colunas_auxiliares if col in df_tabela_flex.columns]
                        
                        # Remover colunas auxiliares da tabela principal (para exibiÃ§Ã£o)
                        if colunas_para_remover:
                            df_tabela_flex = df_tabela_flex.drop(columns=colunas_para_remover)
                        
                        # Selecionador de visualizaÃ§Ã£o (linha inferior)
                        modo_tabela_flex = st.radio(
                            "ðŸ“Š **VisualizaÃ§Ã£o:**",
                            ["Fixo/VariÃ¡vel", "Total"],
                            index=0,
                            horizontal=True,
                            key="modo_tabela_flex_bud"
                        )
                        
                        # Resumo geral (fora dos expanders)
                        # ðŸ”§ CORREÃ‡ÃƒO: Usar DataFrame com colunas auxiliares para recalcular corretamente
                        if len(df_tabela_flex) > 0:
                            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Obter volumes EXATAMENTE como o grÃ¡fico (mesmos DataFrames)
                            # O grÃ¡fico usa df_vol_real_agrupado e df_vol_budget_agrupado agrupados por PerÃ­odo
                            # IMPORTANTE: Usar os mesmos DataFrames e a mesma lÃ³gica do grÃ¡fico
                            volume_real_para_resumo = 0.0
                            volume_budget_para_resumo = 0.0
                            
                            # Obter perÃ­odos selecionados (mesma lÃ³gica usada acima)
                            periodos_para_volume = periodos_tabela if 'periodos_tabela' in locals() else []
                            if not periodos_para_volume:
                                # Se nÃ£o houver perÃ­odos selecionados, usar todos os perÃ­odos disponÃ­veis
                                if len(df_vol_real_agrupado) > 0 and 'PerÃ­odo' in df_vol_real_agrupado.columns:
                                    periodos_para_volume = df_vol_real_agrupado['PerÃ­odo'].unique().tolist()
                            
                            # Obter volumes dos mesmos DataFrames que o grÃ¡fico usa
                            if len(df_vol_real_agrupado) > 0 and 'PerÃ­odo' in df_vol_real_agrupado.columns:
                                # Usar o DataFrame jÃ¡ agrupado por PerÃ­odo (igual ao grÃ¡fico)
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_agrupado[df_vol_real_agrupado['PerÃ­odo'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_agrupado['Volume'].sum()
                            elif 'PerÃ­odo' in df_volume_real_filtrado.columns:
                                # Fallback: agrupar por PerÃ­odo e somar (igual ao grÃ¡fico)
                                df_vol_real_temp = df_volume_real_filtrado.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_temp[df_vol_real_temp['PerÃ­odo'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_temp['Volume'].sum()
                            else:
                                volume_real_para_resumo = df_volume_real_filtrado['Volume'].sum() if 'Volume' in df_volume_real_filtrado.columns else 0.0
                            
                            # Volume Budget: usar df_budget_vol_filtrado (volume do budget, nÃ£o do real)
                            if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                                # Agrupar volume de budget por PerÃ­odo
                                if 'Ano' in df_budget_vol_filtrado.columns and 'Ano' in df_tabela_flex_para_resumo.columns and 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                                    anos_sel = df_tabela_flex_para_resumo['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex_para_resumo.columns else []
                                    df_tmp = df_budget_vol_filtrado.copy()
                                    if anos_sel:
                                        df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                    df_vol_budget_temp = df_tmp.groupby(['Ano', 'PerÃ­odo'])['Volume'].sum().reset_index()
                                    if len(periodos_para_volume) > 0:
                                        volume_budget_para_resumo = df_vol_budget_temp[df_vol_budget_temp['PerÃ­odo'].isin(periodos_para_volume)]['Volume'].sum()
                                    else:
                                        volume_budget_para_resumo = df_vol_budget_temp['Volume'].sum()
                                elif 'PerÃ­odo' in df_budget_vol_filtrado.columns:
                                    df_vol_budget_temp = df_budget_vol_filtrado.groupby('PerÃ­odo')['Volume'].sum().reset_index()
                                    if len(periodos_para_volume) > 0:
                                        volume_budget_para_resumo = df_vol_budget_temp[df_vol_budget_temp['PerÃ­odo'].isin(periodos_para_volume)]['Volume'].sum()
                                    else:
                                        volume_budget_para_resumo = df_vol_budget_temp['Volume'].sum()
                                else:
                                    volume_budget_para_resumo = df_budget_vol_filtrado['Volume'].sum()
                            else:
                                # Fallback: se nÃ£o houver volume de budget, usar volume real (comportamento antigo)
                                volume_budget_para_resumo = volume_real_para_resumo
                            
                            # ðŸ”§ CORREÃ‡ÃƒO: Adaptar resumo para usar nomes das colunas dinÃ¢micas (se usar_colunas_por_periodo)
                            if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1:
                                # Obter nomes das colunas dinÃ¢micas do DataFrame
                                primeiro_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[0]) if len(periodos_ordenados_selecao) > 0 else ""
                                segundo_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[1]) if len(periodos_ordenados_selecao) > 1 else ""
                                
                                # Criar resumo com nomes dinÃ¢micos
                                linha_resumo_geral = {}
                                linha_resumo_geral_formatado = {}
                                
                                # Obter colunas numÃ©ricas do DataFrame
                                colunas_numericas = [col for col in df_tabela_flex_para_resumo.columns 
                                                    if pd.api.types.is_numeric_dtype(df_tabela_flex_para_resumo[col]) 
                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                
                                for col in colunas_numericas:
                                    valor = df_tabela_flex_para_resumo[col].sum()
                                    linha_resumo_geral[col] = valor
                                    
                                    # Formatar valor
                                    if col.startswith('%'):
                                        # Usar formatar_ratio_com_barra para percentuais (dividir por 100 pois estÃ¡ em %)
                                        if pd.notna(valor) and isinstance(valor, (int, float)):
                                            linha_resumo_geral_formatado[col] = formatar_ratio_com_barra(valor / 100)
                                        else:
                                            linha_resumo_geral_formatado[col] = "-"
                                    elif tipo_visualizacao == "CPU (Custo por Unidade)":
                                        linha_resumo_geral_formatado[col] = f"{valor:,.2f}"
                                    else:
                                        sufixo = ""
                                        if fator_conversao:
                                            if fator_conversao == "K (milhares)":
                                                sufixo = " K"
                                            elif fator_conversao == "M (MilhÃµes)":
                                                sufixo = " M"
                                        linha_resumo_geral_formatado[col] = f"{valor:,.2f}{sufixo}"
                                
                                # Adicionar volumes
                                if pd.isna(volume_real_para_resumo) or volume_real_para_resumo is None:
                                    volume_real_para_resumo = 0.0
                                if pd.isna(volume_budget_para_resumo) or volume_budget_para_resumo is None:
                                    volume_budget_para_resumo = 0.0
                                
                                linha_resumo_geral['_Volume_Real_Calculo'] = float(volume_real_para_resumo)
                                linha_resumo_geral['_Volume_Budget_Calculo'] = float(volume_budget_para_resumo)
                                linha_resumo_geral_formatado['_Volume_Real_Calculo'] = f"{float(volume_real_para_resumo):,.0f}"
                                linha_resumo_geral_formatado['_Volume_Budget_Calculo'] = f"{float(volume_budget_para_resumo):,.0f}"
                            else:
                                # Usar funÃ§Ã£o padrÃ£o para colunas fixas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(df_tabela_flex_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                
                                # ðŸ”§ CORREÃ‡ÃƒO: Usar volumes do DataFrame (que jÃ¡ tÃªm os filtros corretos aplicados)
                                # Os volumes em df_tabela_flex_para_resumo jÃ¡ foram calculados com todos os filtros
                                if 'Volume_Real' in df_tabela_flex_para_resumo.columns:
                                    # Volume Real: somar todos os volumes Ãºnicos (mesmo perÃ­odo tem mesmo volume)
                                    if 'PerÃ­odo' in df_tabela_flex_para_resumo.columns:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo.groupby('PerÃ­odo')['Volume_Real'].first().sum()
                                    else:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo['Volume_Real'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_real_para_resumo = linha_resumo_geral.get('_Volume_Real_Calculo', 0.0)
                                
                                if 'Volume_Budget' in df_tabela_flex_para_resumo.columns:
                                    # Volume Budget: somar volumes Ãºnicos por perÃ­odo
                                    if 'PerÃ­odo' in df_tabela_flex_para_resumo.columns:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo.groupby('PerÃ­odo')['Volume_Budget'].first().sum()
                                    else:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo['Volume_Budget'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_budget_para_resumo = linha_resumo_geral.get('_Volume_Budget_Calculo', 0.0)
                                
                                # Garantir que os volumes sejam sempre nÃºmeros (nÃ£o NaN ou None)
                                if pd.isna(volume_real_para_resumo) or volume_real_para_resumo is None:
                                    volume_real_para_resumo = 0.0
                                if pd.isna(volume_budget_para_resumo) or volume_budget_para_resumo is None:
                                    volume_budget_para_resumo = 0.0
                                
                                linha_resumo_geral['_Volume_Real_Calculo'] = float(volume_real_para_resumo)
                                linha_resumo_geral['_Volume_Budget_Calculo'] = float(volume_budget_para_resumo)
                                linha_resumo_geral_formatado['_Volume_Real_Calculo'] = f"{float(volume_real_para_resumo):,.0f}"
                                linha_resumo_geral_formatado['_Volume_Budget_Calculo'] = f"{float(volume_budget_para_resumo):,.0f}"
                            
                            st.markdown("### ðŸ“Š Resumo Geral")
                            # Exibir caixas de resumo com volumes
                            exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=True)
                            st.markdown("<br>", unsafe_allow_html=True)  # Pequeno espaÃ§o antes das tabelas
                        # Criar estrutura hierÃ¡rquica com expanders
                        if modo_tabela_flex == "Fixo/VariÃ¡vel":
                            # ðŸ”§ CORREÃ‡ÃƒO: Usar df_tabela_flex_para_resumo para cÃ¡lculos de resumo (tem colunas originais)
                            # df_tabela_flex pode ter colunas por perÃ­odo (Jul, Ago, etc.) que nÃ£o servem para resumo
                            df_para_resumo_fixo_variavel = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # NÃ­vel 1: Custo (Fixo/VariÃ¡vel) - separado
                            for custo in ['Fixo', 'VariÃ¡vel']:
                                df_custo = df_tabela_flex[df_tabela_flex['Custo'] == custo].copy()
                                # ðŸ”§ CORREÃ‡ÃƒO: Criar versÃ£o para resumo com colunas originais
                                df_custo_para_resumo = df_para_resumo_fixo_variavel[df_para_resumo_fixo_variavel['Custo'] == custo].copy() if 'Custo' in df_para_resumo_fixo_variavel.columns else df_custo.copy()
                                
                                if len(df_custo) > 0:
                                    # ðŸ”§ FILTRAR: Verificar se Custo tem valores nÃ£o zerados
                                    colunas_numericas_custo_check = [col for col in df_custo.columns 
                                                                     if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                    if colunas_numericas_custo_check:
                                        df_custo_check = df_custo[colunas_numericas_custo_check].fillna(0)
                                        tem_valores_nao_zerados = (df_custo_check.abs().sum(axis=1) > 0.0001).any()
                                        if not tem_valores_nao_zerados:
                                            continue  # Pular Custo completamente zerado
                                    else:
                                        if 'Total' in df_custo.columns:
                                            if df_custo['Total'].fillna(0).abs().sum() <= 0.0001:
                                                continue  # Pular Custo completamente zerado
                                    
                                    # Verificar se a coluna 'Total' existe antes de acessÃ¡-la
                                    # ðŸ”§ CORREÃ‡ÃƒO: Tentar usar df_custo_para_resumo primeiro (tem colunas originais)
                                    if 'Total' in df_custo_para_resumo.columns:
                                        total_custo = df_custo_para_resumo['Total'].sum()
                                    elif 'Total' in df_custo.columns:
                                        total_custo = df_custo['Total'].sum()
                                    else:
                                        # Se nÃ£o houver coluna 'Total', usar 0 ou calcular a partir de outras colunas
                                        total_custo = 0.0
                                    total_custo_formatado = f"{total_custo:,.2f}"
                                    
                                    with st.expander(f"ðŸ’° {custo} - Total: {total_custo_formatado}", expanded=False):
                                        # Resumo do Custo (Fixo ou VariÃ¡vel)
                                        # ðŸ”§ CORREÃ‡ÃƒO: Usar df_custo_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                        linha_resumo_custo, linha_resumo_custo_formatado = calcular_resumo_tabela_flex(df_custo_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                        exibir_caixas_resumo(linha_resumo_custo, linha_resumo_custo_formatado, tipo_visualizacao)
                                        st.markdown("---")
                                        
                                        # NÃ­vel 2: Type 05 (se existir)
                                        if 'Type 05' in df_custo.columns:
                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                # ðŸ”§ CORREÃ‡ÃƒO: Criar versÃ£o para resumo com colunas originais
                                                df_type05_para_resumo = df_custo_para_resumo[df_custo_para_resumo['Type 05'] == type05].copy() if 'Type 05' in df_custo_para_resumo.columns else df_type05.copy()
                                                
                                                if len(df_type05) > 0:
                                                    # ðŸ”§ FILTRAR: Verificar se Type 05 tem valores nÃ£o zerados
                                                    colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                                     if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                    if colunas_numericas_type05_check:
                                                        df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                                        tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                                        if not tem_valores_nao_zerados:
                                                            continue  # Pular Type 05 completamente zerado
                                                    else:
                                                        # Preferir a versÃ£o para resumo (colunas originais), pois df_type05
                                                        # pode estar com colunas dinÃ¢micas por perÃ­odo e sem 'Total'
                                                        if 'Total' in df_type05_para_resumo.columns:
                                                            if df_type05_para_resumo['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                continue  # Pular Type 05 completamente zerado
                                                        elif 'Total' in df_type05.columns:
                                                            if df_type05['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                continue  # Pular Type 05 completamente zerado
                                                    
                                                    # Verificar se a coluna 'Total' existe antes de acessÃ¡-la
                                                    if 'Total' in df_type05_para_resumo.columns:
                                                        total_type05 = df_type05_para_resumo['Total'].sum()
                                                    elif 'Total' in df_type05.columns:
                                                        total_type05 = df_type05['Total'].sum()
                                                    else:
                                                        total_type05 = 0.0
                                                    total_type05_formatado = f"{total_type05:,.2f}"
                                                    
                                                    with st.expander(f"ðŸ“Š Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                        # NÃ­vel 3: Type 06 (se existir)
                                                        if 'Type 06' in df_type05.columns:
                                                            for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                # ðŸ”§ CORREÃ‡ÃƒO: Criar versÃ£o para resumo com colunas originais
                                                                df_type06_para_resumo = df_type05_para_resumo[df_type05_para_resumo['Type 06'] == type06].copy() if 'Type 06' in df_type05_para_resumo.columns else df_type06.copy()
                                                                
                                                                if len(df_type06) > 0:
                                                                    # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores nÃ£o zerados
                                                                    colunas_numericas_check = [col for col in df_type06.columns 
                                                                                              if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                              and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                                    if colunas_numericas_check:
                                                                        # Verificar se hÃ¡ pelo menos uma linha com valores nÃ£o zerados
                                                                        df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                        tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                        if not tem_valores_nao_zerados:
                                                                            continue  # Pular Type 06 completamente zerado
                                                                    else:
                                                                        # Se nÃ£o hÃ¡ colunas numÃ©ricas, verificar se Total existe e Ã© zero
                                                                        if 'Total' in df_type06_para_resumo.columns:
                                                                            if df_type06_para_resumo['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                                continue  # Pular Type 06 completamente zerado
                                                                        elif 'Total' in df_type06.columns:
                                                                            if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                                continue  # Pular Type 06 completamente zerado

                                                                    # Verificar se a coluna 'Total' existe antes de acessÃ¡-la
                                                                    if 'Total' in df_type06_para_resumo.columns:
                                                                        total_type06 = df_type06_para_resumo['Total'].sum()
                                                                    elif 'Total' in df_type06.columns:
                                                                        total_type06 = df_type06['Total'].sum()
                                                                    else:
                                                                        total_type06 = 0.0
                                                                    total_type06_formatado = f"{total_type06:,.2f}"
                                                                    
                                                                    # NÃ­vel 4: Account (se existir)
                                                                    if 'Account' in df_type06.columns:
                                                                        # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                                        # Usar todas as colunas numÃ©ricas (incluindo colunas dinÃ¢micas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numÃ©ricas Ã© zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
                                                                            df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                            df_type06_filtrado = df_type06[
                                                                                df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                            ].copy()
                                                                        else:
                                                                            df_type06_filtrado = df_type06.copy()
                                                                        
                                                                        # SÃ³ exibir se houver dados apÃ³s filtrar
                                                                        if len(df_type06_filtrado) > 0:
                                                                            # ðŸ”§ CORREÃ‡ÃƒO: Usar container em vez de expander para evitar problema de 3 nÃ­veis aninhados
                                                                            # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar uma Ãºnica tabela com todas as Accounts
                                                                                # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                                                colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'PerÃ­odo']]
                                                                                
                                                                                # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                                                for col in colunas_numericas:
                                                                                    if col not in colunas_ordenadas:
                                                                                        colunas_ordenadas.append(col)
                                                                                
                                                                                colunas_display = colunas_id + colunas_ordenadas
                                                                                df_display = df_type06_filtrado[colunas_display].copy()

                                                                                # ExibiÃ§Ã£o: a coluna 'Ano' aqui Ã© uma coluna dinÃ¢mica e pode conter o ano (ex.: 2025),
                                                                                # o que impede o filtro de linhas zeradas e ainda fica formatada como moeda.
                                                                                # NÃ£o impacta totais: removemos apenas do dataframe de exibiÃ§Ã£o.
                                                                                if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                                    df_display = df_display.drop(columns=['Ano'])

                                                                                # Visibilidade: remover linhas 100% zeradas/nulas somente na exibiÃ§Ã£o
                                                                                df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                                    df_display,
                                                                                    colunas_ignorar=colunas_id,
                                                                                )
                                                                                
                                                                                # Formatar valores (formatar todas as colunas numÃ©ricas dinamicamente)
                                                                                for col in df_display.columns:
                                                                                    if col not in colunas_id:
                                                                                        # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, nÃ£o percentual)
                                                                                        if col == 'Total / Flex Bud':
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                                        # Formatar percentuais de forma especial com barrinha
                                                                                        elif col.startswith('%'):
                                                                                            # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                        # Formatar outras colunas numÃ©ricas
                                                                                        elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                            else:
                                                                                                sufixo = ""
                                                                                                if fator_conversao:
                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                        sufixo = " K"
                                                                                                    elif fator_conversao == "M (MilhÃµes)":
                                                                                                        sufixo = " M"
                                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # ðŸ”§ CORREÃ‡ÃƒO: Usar versÃ£o para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                if 'Account' in df_type06_para_resumo.columns:
                                                                                    # Aplicar mesmo filtro de linhas zeradas na versÃ£o para resumo
                                                                                    colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                                if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                                                    if colunas_numericas_resumo:
                                                                                        df_type06_para_resumo_temp = df_type06_para_resumo[colunas_numericas_resumo].fillna(0)
                                                                                        df_type06_para_resumo_filtrado = df_type06_para_resumo[
                                                                                            df_type06_para_resumo_temp.abs().sum(axis=1) > 0.0001
                                                                                        ].copy()
                                                                                    else:
                                                                                        df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                else:
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                
                                                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_para_resumo_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                                
                                                                                # Exibir caixas de resumo
                                                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                                
                                                                                # Exibir tabela com resumo (todas as Accounts em uma Ãºnica tabela)
                                                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                                st.markdown(html_table, unsafe_allow_html=True)
                                                                    else:
                                                                        # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                                        # Usar todas as colunas numÃ©ricas (incluindo colunas dinÃ¢micas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numÃ©ricas Ã© zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
                                                                            df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                            df_type06_filtrado = df_type06[
                                                                                df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                            ].copy()
                                                                        else:
                                                                            df_type06_filtrado = df_type06.copy()
                                                                        
                                                                        # SÃ³ exibir se houver dados apÃ³s filtrar
                                                                        if len(df_type06_filtrado) > 0:
                                                                            # ðŸ”§ CORREÃ‡ÃƒO: Usar container em vez de expander para evitar problema de 3 nÃ­veis aninhados
                                                                            # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar tabela para este Type 06
                                                                                # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                                                colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'PerÃ­odo']]
                                                                                
                                                                                # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                                                for col in colunas_numericas:
                                                                                    if col not in colunas_ordenadas:
                                                                                        colunas_ordenadas.append(col)
                                                                                
                                                                                colunas_display = colunas_id + colunas_ordenadas
                                                                                df_display = df_type06_filtrado[colunas_display].copy()

                                                                                # ExibiÃ§Ã£o: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                                                                if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                                    df_display = df_display.drop(columns=['Ano'])

                                                                                # Visibilidade: remover linhas 100% zeradas/nulas somente na exibiÃ§Ã£o
                                                                                df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                                    df_display,
                                                                                    colunas_ignorar=colunas_id,
                                                                                )
                                                                                
                                                                                # Formatar valores
                                                                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                                                    if col in df_display.columns:
                                                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                        else:
                                                                                            sufixo = ""
                                                                                            if fator_conversao:
                                                                                                if fator_conversao == "K (milhares)":
                                                                                                    sufixo = " K"
                                                                                                elif fator_conversao == "M (MilhÃµes)":
                                                                                                    sufixo = " M"
                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                
                                                                                # Formatar Total / Flex Bud com barra HTML
                                                                                if 'Total / Flex Bud' in df_display.columns:
                                                                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # ðŸ”§ CORREÃ‡ÃƒO: Usar versÃ£o para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                            if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                                                if colunas_numericas_resumo:
                                                                                    df_type06_para_resumo_temp = df_type06_para_resumo[colunas_numericas_resumo].fillna(0)
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo[
                                                                                        df_type06_para_resumo_temp.abs().sum(axis=1) > 0.0001
                                                                                    ].copy()
                                                                                else:
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                
                                                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_para_resumo_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                                
                                                                                # Exibir caixas de resumo
                                                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                                
                                                                                # Exibir tabela com resumo
                                                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                                st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            # Sem Type 06: exibir diretamente Type 05
                                                            # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                            colunas_numericas_type05 = [col for col in df_type05.columns 
                                                                                        if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                        and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                            if colunas_numericas_type05:
                                                                df_type05_temp = df_type05[colunas_numericas_type05].fillna(0)
                                                                df_type05 = df_type05[
                                                                    df_type05_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            
                                                            # Criar tabela para este Type 05
                                                            # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                            colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                            colunas_numericas = [col for col in df_type05.columns 
                                                                                if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                            
                                                            # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                            ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                            colunas_ordenadas = []
                                                            for col_ordem in ordem_colunas:
                                                                if col_ordem in colunas_numericas:
                                                                    colunas_ordenadas.append(col_ordem)
                                                            # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                            for col in colunas_numericas:
                                                                if col not in colunas_ordenadas:
                                                                    colunas_ordenadas.append(col)
                                                            
                                                            colunas_display = colunas_id + colunas_ordenadas
                                                            df_display = df_type05[colunas_display].copy()

                                                            # ExibiÃ§Ã£o: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                                            if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                df_display = df_display.drop(columns=['Ano'])

                                                            # Visibilidade: remover linhas 100% zeradas/nulas somente na exibiÃ§Ã£o
                                                            df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                df_display,
                                                                colunas_ignorar=colunas_id,
                                                            )
                                                            
                                                            # Formatar valores
                                                            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                                if col in df_display.columns:
                                                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                    else:
                                                                        sufixo = ""
                                                                        if fator_conversao:
                                                                            if fator_conversao == "K (milhares)":
                                                                                sufixo = " K"
                                                                            elif fator_conversao == "M (MilhÃµes)":
                                                                                sufixo = " M"
                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                            
                                                            # Formatar Total / Flex Bud com barra HTML
                                                            if 'Total / Flex Bud' in df_display.columns:
                                                                df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                            
                                                            # Calcular linha de resumo
                                                            linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type05, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                            
                                                            # Exibir caixas de resumo
                                                            exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                            
                                                            # Exibir tabela com resumo
                                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                        else:
                                            # Sem Type 05: exibir diretamente Custo
                                            # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                            colunas_numericas_custo = [col for col in df_custo.columns 
                                                                       if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                       and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                            if colunas_numericas_custo:
                                                df_custo_temp = df_custo[colunas_numericas_custo].fillna(0)
                                                df_custo = df_custo[
                                                    df_custo_temp.abs().sum(axis=1) > 0.0001
                                                ].copy()
                                            
                                            # Criar tabela para este Custo
                                            # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                            colunas_id = ['Custo'] if 'Custo' in df_custo.columns else []
                                            colunas_numericas = [col for col in df_custo.columns 
                                                                if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Account', 'PerÃ­odo']]
                                            colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                            colunas_display = colunas_id + colunas_ordenadas
                                            df_display = df_custo[colunas_display].copy()

                                            # ExibiÃ§Ã£o: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                            if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                df_display = df_display.drop(columns=['Ano'])

                                            # Visibilidade: remover linhas 100% zeradas/nulas somente na exibiÃ§Ã£o
                                            df_display = _remover_linhas_sem_valores_para_exibicao(
                                                df_display,
                                                colunas_ignorar=colunas_id,
                                            )
                                            
                                            # Formatar valores (formatar todas as colunas numÃ©ricas dinamicamente)
                                            for col in df_display.columns:
                                                if col not in colunas_id:
                                                    # Formatar percentuais de forma especial com barrinha
                                                    if col.startswith('%'):
                                                        # Usar formatar_ratio_com_barra para colunas de percentual (dividir por 100 pois estÃ¡ em %)
                                                        df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                    # Formatar outras colunas numÃ©ricas
                                                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                        else:
                                                            sufixo = ""
                                                            if fator_conversao:
                                                                if fator_conversao == "K (milhares)":
                                                                    sufixo = " K"
                                                                elif fator_conversao == "M (MilhÃµes)":
                                                                    sufixo = " M"
                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                            
                                            # Calcular linha de resumo
                                            linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_custo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                            
                                            # Exibir caixas de resumo
                                            exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                            
                                            # Exibir tabela com resumo
                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                            st.markdown(html_table, unsafe_allow_html=True)
                        else:
                            # Modo "Total": nÃ£o separar por Fixo/VariÃ¡vel
                            # Agrupar todos os dados sem separar por Custo
                            # Remover coluna Custo do agrupamento para exibiÃ§Ã£o
                            df_tabela_total = df_tabela_flex.copy()
                            
                            # Verificar se df_tabela_total tem dados
                            if len(df_tabela_total) == 0:
                                st.warning("âš ï¸ Nenhum dado disponÃ­vel para exibiÃ§Ã£o no modo Total.")
                                df_tabela_total_agrupado = pd.DataFrame()
                            else:
                                # Agrupar por Type 05, Type 06, Account (se existirem) somando valores
                                colunas_agrupamento_total = []
                                if 'Type 05' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Type 05')
                                if 'Type 06' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Type 06')
                                if 'Account' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Account')
                                
                                if len(colunas_agrupamento_total) > 0:
                                    # Verificar quais colunas existem antes de agrupar
                                    colunas_para_agregar = []
                                    colunas_esperadas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']
                                    for col in colunas_esperadas:
                                        if col in df_tabela_total.columns:
                                            colunas_para_agregar.append(col)
                                    
                                    if len(colunas_para_agregar) > 0:
                                        # Agrupar somando os valores
                                        df_tabela_total_agrupado = df_tabela_total.groupby(colunas_agrupamento_total).agg({
                                            col: 'sum' for col in colunas_para_agregar
                                        }).reset_index()
                                    else:
                                        # Se nÃ£o hÃ¡ colunas para agregar, usar todas as colunas numÃ©ricas disponÃ­veis
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in colunas_agrupamento_total 
                                                           and col not in ['Custo', 'PerÃ­odo']]
                                        if len(colunas_numericas) > 0:
                                            df_tabela_total_agrupado = df_tabela_total.groupby(colunas_agrupamento_total).agg({
                                                col: 'sum' for col in colunas_numericas
                                            }).reset_index()
                                        else:
                                            st.warning("âš ï¸ Nenhuma coluna numÃ©rica encontrada em df_tabela_total. Colunas disponÃ­veis: " + ", ".join(df_tabela_total.columns.tolist()))
                                            df_tabela_total_agrupado = pd.DataFrame(columns=colunas_agrupamento_total)
                                
                                # Recalcular Total / Flex Bud apÃ³s agrupamento (se as colunas necessÃ¡rias existirem)
                                if len(df_tabela_total_agrupado) > 0 and 'Total' in df_tabela_total_agrupado.columns and 'Flex BUD' in df_tabela_total_agrupado.columns:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = df_tabela_total_agrupado.apply(
                                        lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                        axis=1
                                    )
                                elif len(df_tabela_total_agrupado) > 0:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = 0
                                
                                # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS apÃ³s agrupamento
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                                  and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                    if colunas_numericas_agrupado:
                                        df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0)
                                        df_tabela_total_agrupado = df_tabela_total_agrupado[
                                            df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                        ].copy()
                                else:
                                    # Se nÃ£o houver colunas de agrupamento, somar tudo
                                    # Verificar quais colunas existem antes de somar
                                    valores_soma = {}
                                    colunas_esperadas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']
                                    for col in colunas_esperadas:
                                        if col in df_tabela_total.columns:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Se nÃ£o encontrou as colunas esperadas, tentar usar todas as colunas numÃ©ricas
                                    if len(valores_soma) == 0:
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in ['Custo', 'PerÃ­odo']]
                                        for col in colunas_numericas:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Calcular Total / Flex Bud se as colunas necessÃ¡rias existirem
                                    if 'Total' in valores_soma and 'Flex BUD' in valores_soma:
                                        if valores_soma['Flex BUD'] != 0 and pd.notnull(valores_soma['Flex BUD']):
                                            valores_soma['Total / Flex Bud'] = valores_soma['Total'] / valores_soma['Flex BUD']
                                        else:
                                            valores_soma['Total / Flex Bud'] = 0
                                    
                                    if len(valores_soma) > 0:
                                        df_tabela_total_agrupado = pd.DataFrame([valores_soma])
                                    else:
                                        # Se nÃ£o hÃ¡ colunas para somar, criar DataFrame vazio
                                        st.warning("âš ï¸ Nenhuma coluna numÃ©rica encontrada em df_tabela_total. Colunas disponÃ­veis: " + ", ".join(df_tabela_total.columns.tolist()))
                                        df_tabela_total_agrupado = pd.DataFrame()
                                
                                # ðŸ”§ FILTRAR: Se a linha Ãºnica tiver todos os valores zerados, nÃ£o exibir
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col])]
                                    if colunas_numericas_agrupado:
                                        soma_absoluta = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0).abs().sum(axis=1).iloc[0]
                                        if soma_absoluta <= 0.0001:
                                            df_tabela_total_agrupado = pd.DataFrame()  # DataFrame vazio para nÃ£o exibir
                            
                            # ðŸ”§ ADICIONAR: Exibir Resumo Geral no modo Total
                            if len(df_tabela_total_agrupado) > 0:
                                st.markdown("### ðŸ“Š Resumo Geral")
                                
                                # Usar df_tabela_flex_para_resumo (salvo ANTES da transformaÃ§Ã£o em colunas por perÃ­odo)
                                # Se nÃ£o existir (caso de perÃ­odo Ãºnico), usar df_tabela_flex
                                df_para_resumo = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                                # Calcular resumo geral usando df_para_resumo
                                # Isso garante que todos os dados sejam incluÃ­dos no resumo com as colunas corretas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(
                                    df_para_resumo, 
                                    tipo_visualizacao, 
                                    moeda_simbolo, 
                                    fator_conversao
                                )
                                
                                # Exibir caixas de resumo
                                exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=False)
                                st.markdown("---")
                            
                            # Criar estrutura hierÃ¡rquica sem separaÃ§Ã£o por Custo
                            # ðŸ”§ CORREÃ‡ÃƒO: Usar df_tabela_flex_para_resumo para cÃ¡lculos de resumo (tem colunas originais)
                            df_para_resumo_total = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # NÃ­vel 1: Type 05 (se existir)
                            if 'Type 05' in df_tabela_total_agrupado.columns:
                                for type05 in sorted(df_tabela_total_agrupado['Type 05'].dropna().unique()):
                                    df_type05 = df_tabela_total_agrupado[df_tabela_total_agrupado['Type 05'] == type05].copy()
                                    # ðŸ”§ CORREÃ‡ÃƒO: Criar versÃ£o para resumo com colunas originais
                                    df_type05_para_resumo = df_para_resumo_total[df_para_resumo_total['Type 05'] == type05].copy() if 'Type 05' in df_para_resumo_total.columns else df_type05.copy()
                                    
                                    if len(df_type05) > 0:
                                        # ðŸ”§ FILTRAR: Verificar se Type 05 tem valores nÃ£o zerados
                                        colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                         if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                         and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                        if colunas_numericas_type05_check:
                                            df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                            tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                            if not tem_valores_nao_zerados:
                                                continue  # Pular Type 05 completamente zerado
                                        else:
                                            if 'Total' in df_type05.columns:
                                                if df_type05['Total'].fillna(0).abs().sum() <= 0.0001:
                                                    continue  # Pular Type 05 completamente zerado
                                        
                                        # Verificar se a coluna 'Total' existe antes de acessÃ¡-la
                                        if 'Total' in df_type05.columns:
                                            total_type05 = df_type05['Total'].sum()
                                        else:
                                            total_type05 = 0.0
                                        total_type05_formatado = f"{total_type05:,.2f}"
                                        
                                        with st.expander(f"ðŸ“Š Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                            # Resumo do Type 05
                                            # ðŸ”§ CORREÃ‡ÃƒO: Usar df_type05_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                            linha_resumo_type05, linha_resumo_type05_formatado = calcular_resumo_tabela_flex(df_type05_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                            exibir_caixas_resumo(linha_resumo_type05, linha_resumo_type05_formatado, tipo_visualizacao)
                                            st.markdown("---")
                                            
                                            # NÃ­vel 2: Type 06 (se existir)
                                            if 'Type 06' in df_type05.columns:
                                                for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                    df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                    
                                                    if len(df_type06) > 0:
                                                        # ðŸ”§ FILTRAR: Verificar se Type 06 tem valores nÃ£o zerados
                                                        colunas_numericas_type06_check = [col for col in df_type06.columns 
                                                                                          if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                          and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                        if colunas_numericas_type06_check:
                                                            df_type06_check = df_type06[colunas_numericas_type06_check].fillna(0)
                                                            tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                            if not tem_valores_nao_zerados:
                                                                continue  # Pular Type 06 completamente zerado
                                                        else:
                                                            if 'Total' in df_type06.columns:
                                                                if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                    continue  # Pular Type 06 completamente zerado
                                                        
                                                        # Verificar se a coluna 'Total' existe antes de acessÃ¡-la
                                                        if 'Total' in df_type06.columns:
                                                            total_type06 = df_type06['Total'].sum()
                                                        else:
                                                            total_type06 = 0.0
                                                        total_type06_formatado = f"{total_type06:,.2f}"
                                                        
                                                        # NÃ­vel 3: Account (se existir)
                                                        if 'Account' in df_type06.columns:
                                                            # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                            # Usar todas as colunas numÃ©ricas (incluindo colunas dinÃ¢micas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numÃ©ricas Ã© zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                df_type06_filtrado = df_type06[
                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            else:
                                                                df_type06_filtrado = df_type06.copy()
                                                            
                                                            # SÃ³ exibir se houver dados apÃ³s filtrar
                                                            if len(df_type06_filtrado) > 0:
                                                                # ðŸ”§ CORREÃ‡ÃƒO: Usar container em vez de expander para evitar problema de 3 nÃ­veis aninhados
                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar uma Ãºnica tabela com todas as Accounts
                                                                    # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                                    colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'PerÃ­odo']]
                                                                    
                                                                    # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                                    for col in colunas_numericas:
                                                                        if col not in colunas_ordenadas:
                                                                            colunas_ordenadas.append(col)
                                                                    
                                                                    colunas_display = colunas_id + colunas_ordenadas
                                                                    df_display = df_type06_filtrado[colunas_display].copy()
                                                                    
                                                                    # Formatar valores (formatar todas as colunas numÃ©ricas dinamicamente)
                                                                    for col in df_display.columns:
                                                                        if col not in colunas_id:
                                                                            # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, nÃ£o percentual)
                                                                            if col == 'Total / Flex Bud':
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                            # Formatar percentuais de forma especial com barrinha
                                                                            elif col.startswith('%'):
                                                                                # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                            # Formatar outras colunas numÃ©ricas
                                                                            elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                else:
                                                                                    sufixo = ""
                                                                                    if fator_conversao:
                                                                                        if fator_conversao == "K (milhares)":
                                                                                            sufixo = " K"
                                                                                        elif fator_conversao == "M (MilhÃµes)":
                                                                                            sufixo = " M"
                                                                                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                    
                                                                    # Calcular linha de resumo (usar dados filtrados)
                                                                    linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                    
                                                                    # Exibir caixas de resumo
                                                                    exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                    
                                                                    # Exibir tabela com resumo (todas as Accounts em uma Ãºnica tabela)
                                                                    html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                    st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                            # Usar todas as colunas numÃ©ricas (incluindo colunas dinÃ¢micas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numÃ©ricas Ã© zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                df_type06_filtrado = df_type06[
                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            else:
                                                                df_type06_filtrado = df_type06.copy()
                                                            
                                                            # SÃ³ exibir se houver dados apÃ³s filtrar
                                                            if len(df_type06_filtrado) > 0:
                                                                # ðŸ”§ CORREÃ‡ÃƒO: Usar container em vez de expander para evitar problema de 3 nÃ­veis aninhados
                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar tabela para este Type 06
                                                                    # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                                    colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'PerÃ­odo']]
                                                                    
                                                                    # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                                    for col in colunas_numericas:
                                                                        if col not in colunas_ordenadas:
                                                                            colunas_ordenadas.append(col)
                                                                    
                                                                    colunas_display = colunas_id + colunas_ordenadas
                                                                    df_display = df_type06_filtrado[colunas_display].copy()
                                                                    
                                                                    # Formatar valores
                                                                    for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                                        if col in df_display.columns:
                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                            else:
                                                                                sufixo = ""
                                                                                if fator_conversao:
                                                                                    if fator_conversao == "K (milhares)":
                                                                                        sufixo = " K"
                                                                                    elif fator_conversao == "M (MilhÃµes)":
                                                                                        sufixo = " M"
                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                    
                                                                    # Formatar Total / Flex Bud com barra HTML
                                                                    if 'Total / Flex Bud' in df_display.columns:
                                                                        df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                                    
                                                                    # Calcular linha de resumo (usar dados filtrados)
                                                                    linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                    
                                                                    # Exibir caixas de resumo
                                                                    exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                    
                                                                    # Exibir tabela com resumo
                                                                    html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                    st.markdown(html_table, unsafe_allow_html=True)
                                            else:
                                                # Sem Type 06: exibir diretamente Type 05
                                                # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                                colunas_numericas_type05_total = [col for col in df_type05.columns 
                                                                                   if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                   and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                if colunas_numericas_type05_total:
                                                    df_type05_temp_total = df_type05[colunas_numericas_type05_total].fillna(0)
                                                    df_type05 = df_type05[
                                                        df_type05_temp_total.abs().sum(axis=1) > 0.0001
                                                    ].copy()
                                                
                                                # Criar tabela para este Type 05
                                                # Usar colunas dinÃ¢micas (pode ter colunas por perÃ­odo ou colunas padrÃ£o)
                                                colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                colunas_numericas = [col for col in df_type05.columns 
                                                                    if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                                
                                                # ðŸ”§ CORREÃ‡ÃƒO: Reordenar colunas na ordem correta
                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                colunas_ordenadas = []
                                                for col_ordem in ordem_colunas:
                                                    if col_ordem in colunas_numericas:
                                                        colunas_ordenadas.append(col_ordem)
                                                # Adicionar outras colunas numÃ©ricas que nÃ£o estÃ£o na ordem padrÃ£o (colunas dinÃ¢micas)
                                                for col in colunas_numericas:
                                                    if col not in colunas_ordenadas:
                                                        colunas_ordenadas.append(col)
                                                
                                                colunas_display = colunas_id + colunas_ordenadas
                                                df_display = df_type05[colunas_display].copy()
                                                
                                                # Formatar valores
                                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                    if col in df_display.columns:
                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                        else:
                                                            sufixo = ""
                                                            if fator_conversao:
                                                                if fator_conversao == "K (milhares)":
                                                                    sufixo = " K"
                                                                elif fator_conversao == "M (MilhÃµes)":
                                                                    sufixo = " M"
                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                
                                                # Formatar Total / Flex Bud com barra HTML
                                                if 'Total / Flex Bud' in df_display.columns:
                                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                
                                                # Calcular linha de resumo
                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type05, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                
                                                # Exibir caixas de resumo
                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                
                                                # Exibir tabela com resumo
                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                st.markdown(html_table, unsafe_allow_html=True)
                            else:
                                # Sem Type 05: exibir tabela total diretamente
                                # Criar tabela Ãºnica com todos os dados agregados
                                
                                # ðŸ”§ FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numÃ©ricos sÃ£o zero ou nulos
                                colunas_numericas = [col for col in df_tabela_total_agrupado.columns 
                                                    if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'PerÃ­odo']]
                                if colunas_numericas:
                                    # Filtrar linhas onde a soma absoluta de todas as colunas numÃ©ricas Ã© zero ou nula
                                    df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas].fillna(0)
                                    df_tabela_total_agrupado = df_tabela_total_agrupado[
                                        df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                    ].copy()
                                
                                total_geral = df_tabela_total_agrupado['Total'].sum() if len(df_tabela_total_agrupado) > 0 else 0
                                total_geral_formatado = f"{total_geral:,.2f}"
                                
                                st.markdown(f"**Total Geral: {total_geral_formatado}**")
                                
                                # Criar tabela (usar colunas dinÃ¢micas se disponÃ­veis)
                                colunas_padrao = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                colunas_existentes = [col for col in colunas_padrao if col in df_tabela_total_agrupado.columns]
                                # Se nÃ£o tiver colunas padrÃ£o, usar todas as colunas numÃ©ricas
                                if not colunas_existentes:
                                    colunas_existentes = colunas_numericas if colunas_numericas else df_tabela_total_agrupado.columns.tolist()
                                
                                df_display = df_tabela_total_agrupado[colunas_existentes].copy()

                                # Visibilidade: remover linhas 100% zeradas/nulas somente na exibiÃ§Ã£o
                                df_display = _remover_linhas_sem_valores_para_exibicao(df_display)
                                
                                # Formatar valores
                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                    if col in df_display.columns:
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                        else:
                                            sufixo = ""
                                            if fator_conversao:
                                                if fator_conversao == "K (milhares)":
                                                    sufixo = " K"
                                                elif fator_conversao == "M (MilhÃµes)":
                                                    sufixo = " M"
                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                
                                # Formatar Total / Flex Bud com barra HTML (garantir que estÃ¡ em formato de ratio, nÃ£o percentual)
                                if 'Total / Flex Bud' in df_display.columns:
                                    # Garantir que os valores estÃ£o em formato de ratio (0.95 = 95%), nÃ£o em percentual
                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].apply(
                                        lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0)
                                    )
                                
                                # Calcular linha de resumo
                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_tabela_total_agrupado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                
                                # Exibir caixas de resumo
                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                
                                # Exibir tabela com resumo
                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                st.markdown(html_table, unsafe_allow_html=True)
                        
                        # BotÃ£o de download da tabela Flex Bud
                        if st.button(
                            "ðŸ“¥ Baixar Tabela Flex Bud (Excel)",
                            width="stretch",
                            key="download_tabela_flex_bud"
                        ):
                            with st.spinner("Gerando arquivo da tabela..."):
                                try:
                                    # Preparar DataFrame para download (usar dados originais antes da formataÃ§Ã£o)
                                    df_download = df_tabela_flex.copy()
                                    
                                    # Remover coluna 'PerÃ­odo' se existir (jÃ¡ foi filtrada)
                                    if 'PerÃ­odo' in df_download.columns:
                                        df_download = df_download.drop(columns=['PerÃ­odo'])
                                    
                                    # Formatar valores numÃ©ricos para o Excel (manter valores originais)
                                    # As colunas numÃ©ricas jÃ¡ estÃ£o com valores corretos, apenas garantir formato
                                    for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                        if col in df_download.columns:
                                            # Garantir que sÃ£o numÃ©ricos
                                            df_download[col] = pd.to_numeric(df_download[col], errors='coerce')
                                    
                                    # Formatar 'Total / Flex Bud' como percentual (0.95 = 95%)
                                    if 'Total / Flex Bud' in df_download.columns:
                                        df_download['Total / Flex Bud'] = pd.to_numeric(df_download['Total / Flex Bud'], errors='coerce')
                                        # Converter para percentual se necessÃ¡rio (se estiver entre 0 e 1)
                                        df_download['Total / Flex Bud'] = df_download['Total / Flex Bud'].apply(
                                            lambda x: x * 100 if pd.notnull(x) and x <= 1 else x
                                        )

                                    # ============================
                                    # Aba 2: Realizado (no mesmo arquivo)
                                    # ============================
                                    df_real_download = None
                                    try:
                                        # Preferir a base real usada no cÃ¡lculo da tabela Flex
                                        if 'df_real_tabela' in locals() and df_real_tabela is not None and len(df_real_tabela) > 0:
                                            df_real_download = df_real_tabela.copy()
                                        elif 'df_real_original_grafico' in locals() and df_real_original_grafico is not None and len(df_real_original_grafico) > 0:
                                            df_real_download = df_real_original_grafico.copy()
                                        elif 'df_filtrado' in locals() and df_filtrado is not None and len(df_filtrado) > 0:
                                            df_real_download = df_filtrado.copy()

                                        # Aplicar filtro de perÃ­odo (se existir na tela)
                                        if df_real_download is not None and 'PerÃ­odo' in df_real_download.columns and 'periodos_tabela' in locals() and periodos_tabela:
                                            df_real_download = df_real_download.copy()
                                            df_real_download['PerÃ­odo'] = df_real_download['PerÃ­odo'].apply(_normalizar_mes_lower)
                                            periodos_norm = [
                                                _normalizar_mes_lower(p)
                                                for p in periodos_tabela
                                                if p is not None and str(p).strip() != ''
                                            ]
                                            if periodos_norm:
                                                df_real_download = df_real_download[
                                                    df_real_download['PerÃ­odo'].isin(periodos_norm)
                                                ].copy()

                                        # Agregar Realizado no mesmo nÃ­vel da visualizaÃ§Ã£o (Fixo/VariÃ¡vel vs Total)
                                        if df_real_download is not None and 'Total' in df_real_download.columns:
                                            df_real_download['Total'] = pd.to_numeric(df_real_download['Total'], errors='coerce').fillna(0)
                                            group_cols = []
                                            if 'modo_tabela_flex' in locals() and modo_tabela_flex == "Fixo/VariÃ¡vel" and 'Custo' in df_real_download.columns:
                                                group_cols.append('Custo')
                                            for col in ['Type 05', 'Type 06', 'Account']:
                                                if col in df_real_download.columns:
                                                    group_cols.append(col)

                                            if group_cols:
                                                df_real_aggr = df_real_download.groupby(group_cols, as_index=False)['Total'].sum()
                                            else:
                                                df_real_aggr = pd.DataFrame({'Total': [df_real_download['Total'].sum()]})

                                            # No modo CPU, exportar tambÃ©m CPU do Realizado (Total/Volume total do recorte)
                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                volume_real_total = 0.0
                                                if 'df_volume_real_filtrado' in locals() and df_volume_real_filtrado is not None and 'Volume' in df_volume_real_filtrado.columns:
                                                    vol_tmp = df_volume_real_filtrado.copy()
                                                    if 'PerÃ­odo' in vol_tmp.columns:
                                                        vol_tmp['PerÃ­odo'] = vol_tmp['PerÃ­odo'].apply(_normalizar_mes_lower)
                                                        if 'periodos_tabela' in locals() and periodos_tabela:
                                                            vol_tmp = vol_tmp[vol_tmp['PerÃ­odo'].isin(periodos_norm)].copy() if 'periodos_norm' in locals() else vol_tmp
                                                    volume_real_total = float(pd.to_numeric(vol_tmp['Volume'], errors='coerce').fillna(0).sum())

                                                df_real_aggr = df_real_aggr.rename(columns={'Total': 'Total_Custo'})
                                                df_real_aggr['Volume_Real_Total'] = volume_real_total
                                                df_real_aggr['CPU'] = (
                                                    df_real_aggr['Total_Custo'] / volume_real_total
                                                    if volume_real_total not in (0, None) else 0.0
                                                )

                                            df_real_download = df_real_aggr
                                    except Exception:
                                        df_real_download = None
                                    
                                    # Obter pasta Downloads do usuÃ¡rio
                                    downloads_path = os.path.join(
                                        os.path.expanduser("~"), "Downloads"
                                    )
                                    tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                                    modo_nome = "Fixo_Variavel" if modo_tabela_flex == "Fixo/VariÃ¡vel" else "Total"
                                    file_name = f"TC_Flex_Bud_{modo_nome}_{tipo_nome}.xlsx"
                                    file_path = os.path.join(downloads_path, file_name)
                                    
                                    # Salvar arquivo diretamente na pasta Downloads
                                    with pd.ExcelWriter(
                                        file_path, engine='openpyxl'
                                    ) as writer:
                                        df_download.to_excel(
                                            writer, index=False, sheet_name='Flex_Bud'
                                        )

                                        # Adicionar segunda aba com Realizado (se disponÃ­vel)
                                        if df_real_download is not None and hasattr(df_real_download, 'empty') and not df_real_download.empty:
                                            df_real_download.to_excel(
                                                writer, index=False, sheet_name='Realizado'
                                            )
                                    
                                    st.success(
                                        f"âœ… Arquivo salvo com sucesso em: {file_path}"
                                    )
                                    st.info(
                                        f"ðŸ“ Verifique sua pasta Downloads: {downloads_path}"
                                    )
                                except Exception as e:
                                    st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")
            else:
                st.info("â„¹ï¸ Tabela Flex Bud disponÃ­vel apenas quando hÃ¡ dados de budget e coluna 'Custo' nos dados.")

# ==========================================
# TAB 2: Volume
# ==========================================
if is_main_page:
    with tab2:
        # IMPORTANTE: Usar a mesma lÃ³gica de filtragem em ambos os modos
        # para garantir que os volumes sejam consistentes
        df_vol = load_volume_data(ano_selecionado)
        
        # Carregar dados de volume do budget para o grÃ¡fico de volume
        df_budget_vol_grafico = load_budget_volume_data(ano_selecionado)
        
        if df_vol is not None:
            # Verificar se tem as colunas necessÃ¡rias
            if 'PerÃ­odo' in df_vol.columns and 'Volume' in df_vol.columns:
                # Aplicar filtros da sidebar de forma centralizada
                df_vol_filtrado = filtrar_volume_com_sidebar(df_vol, df_total)
                if df_vol_filtrado is None:
                    df_vol_filtrado = df_vol.copy()
                
                # Aplicar tambÃ©m os filtros especÃ­ficos do grÃ¡fico (Oficina e VeÃ­culo) se foram selecionados
                # Isso permite que o grÃ¡fico de volume responda aos filtros do grÃ¡fico tambÃ©m
                if 'Oficina' in df_vol_filtrado.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_vol_filtrado = df_vol_filtrado[
                            df_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                if 'VeÃ­culo' in df_vol_filtrado.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_vol_filtrado = df_vol_filtrado[
                            df_vol_filtrado['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # Criar grÃ¡fico com dados filtrados (sempre mostrando todos os perÃ­odos)
                # Aplicar mesmos filtros ao volume do budget
                df_budget_vol_filtrado_grafico = None
                if df_budget_vol_grafico is not None:
                    df_budget_vol_filtrado_grafico = df_budget_vol_grafico.copy()
                    
                    # Aplicar TODOS os filtros da sidebar diretamente ao df_budget_vol (mesma lÃ³gica do volume real)
                    # Filtro 1: Oficina
                    # CORREÃ‡ÃƒO: Garantir que apenas oficinas presentes nas opÃ§Ãµes do filtro sejam consideradas
                    if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                        # Obter as opÃ§Ãµes de oficina disponÃ­veis (uniÃ£o Real + Budget - mesmas do filtro principal)
                        oficinas_set = set(get_filter_options(df_total, 'Oficina'))
                        oficinas_set.discard("Todos")
                        try:
                            df_budget_opcoes = load_budget_data(ano_selecionado)
                            if df_budget_opcoes is not None and 'Oficina' in df_budget_opcoes.columns:
                                oficinas_set.update(df_budget_opcoes['Oficina'].dropna().astype(str).unique().tolist())
                        except Exception:
                            pass
                        try:
                            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
                            if df_budget_vol_opcoes is not None and 'Oficina' in df_budget_vol_opcoes.columns:
                                oficinas_set.update(df_budget_vol_opcoes['Oficina'].dropna().astype(str).unique().tolist())
                        except Exception:
                            pass
                        oficina_opcoes_disponiveis = sorted(oficinas_set)
                        
                        # Obter oficinas selecionadas no filtro
                        oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])
                        
                        # Se "Todos" estiver selecionado, usar todas as opÃ§Ãµes disponÃ­veis no filtro
                        if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
                            # Filtrar apenas pelas oficinas que estÃ£o nas opÃ§Ãµes do filtro (nÃ£o incluir oficinas que nÃ£o estÃ£o no filtro)
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
                            ].copy()
                        else:
                            # Filtrar apenas pelas oficinas selecionadas (que jÃ¡ estÃ£o nas opÃ§Ãµes do filtro)
                            # Garantir que apenas oficinas que estÃ£o nas opÃ§Ãµes sejam consideradas
                            oficinas_validas = [o for o in oficina_selecionadas_sidebar if o in oficina_opcoes_disponiveis]
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficinas_validas)
                            ].copy()
                    
                    # Filtro 2: VeÃ­culo
                    if 'VeÃ­culo' in df_budget_vol_filtrado_grafico.columns:
                        veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])
                        if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['VeÃ­culo'].astype(str).isin(veiculo_selecionados_sidebar)
                            ].copy()
                    
                    # Filtro 3: USI
                    if 'USI' in df_budget_vol_filtrado_grafico.columns:
                        usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_ext', ["Todos"])
                        if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['USI'].astype(str).isin(usi_selecionada_sidebar)
                            ].copy()
                    
                    # Filtro 4: PerÃ­odo - NÃƒO aplicar aqui, mostrar todos os perÃ­odos no grÃ¡fico
                    
                    # Filtro 5: Centro cst
                    if 'Centrocst' in df_budget_vol_filtrado_grafico.columns:
                        centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_ext', "Todos")
                        if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
                            ].copy()
                    
                    # Filtro 6: Conta contÃ¡bil
                    if 'NÂºconta' in df_budget_vol_filtrado_grafico.columns:
                        conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_ext', [])
                        if conta_contabil_selecionadas_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['NÂºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
                            ].copy()
                    
                    # Filtros principais
                    filtros_principais_nomes = ["Type 05", "Type 06", "Fornecedor", "Fornec.", "Tipo"]
                    for col_name in filtros_principais_nomes:
                        if col_name in df_budget_vol_filtrado_grafico.columns:
                            filtro_key = f'filtro_{col_name}_tc_ext'
                            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
                            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                                df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                    df_budget_vol_filtrado_grafico[col_name].astype(str).isin(selecionadas_sidebar)
                                ].copy()
                    
                    # Filtros avanÃ§ados
                    filtros_avancados_nomes = ["UsuÃ¡rio", "Material", "Dt.lÃ§to.", "Texto breve", "Account"]
                    for col_name in filtros_avancados_nomes:
                        if col_name in df_budget_vol_filtrado_grafico.columns:
                            filtro_key = f'filtro_avancado_{col_name}_tc_ext'
                            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
                            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                                df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                    df_budget_vol_filtrado_grafico[col_name].astype(str).isin(selecionadas_sidebar)
                                ].copy()
                    
                    # Aplicar filtro de Oficina do grÃ¡fico
                    if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de VeÃ­culo do grÃ¡fico
                    if 'VeÃ­culo' in df_budget_vol_filtrado_grafico.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['VeÃ­culo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                
                # ðŸ“Š Resumo Volume (acima do grÃ¡fico)
                volume_real_total = pd.to_numeric(
                    df_vol_filtrado['Volume'], errors='coerce'
                ).fillna(0).sum()
                volume_budget_total = None
                if (
                    df_budget_vol_filtrado_grafico is not None
                    and 'Volume' in df_budget_vol_filtrado_grafico.columns
                ):
                    volume_budget_total = pd.to_numeric(
                        df_budget_vol_filtrado_grafico['Volume'], errors='coerce'
                    ).fillna(0).sum()

                if volume_budget_total is not None:
                    diferenca_real_bud = volume_real_total - volume_budget_total
                    percentual_real_bud = (
                        volume_real_total / volume_budget_total
                        if volume_budget_total != 0
                        else None
                    )
                else:
                    diferenca_real_bud = None
                    percentual_real_bud = None

                st.subheader("ðŸ“Š Resumo Volume")
                st.markdown(
                    """
                    <style>
                    .volume-summary-card {padding: 0.6rem 0.8rem; border: 1px solid rgba(49, 51, 63, 0.15); border-radius: 8px; background: rgba(0, 0, 0, 0.02);}
                    .volume-summary-label {opacity: 0.75;}
                    .volume-summary-value {font-size: 1.1em; font-weight: 600;}
                    .volume-summary-spacer {display: block; height: 1.75rem;}
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                def _render_volume_card(label, value):
                    st.markdown(
                        f"""
                        <div class="volume-summary-card">
                            <div class="volume-summary-label">{label}</div>
                            <div class="volume-summary-value">{value}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                col_res_1, col_res_2, col_res_3, col_res_4 = st.columns(4)
                with col_res_1:
                    _render_volume_card(
                        "Volume Budget",
                        f"{volume_budget_total:,.0f}" if volume_budget_total is not None else "-",
                    )
                with col_res_2:
                    _render_volume_card("Volume Real", f"{volume_real_total:,.0f}")
                with col_res_3:
                    _render_volume_card(
                        "DiferenÃ§a Real - Bud",
                        f"{diferenca_real_bud:,.0f}" if diferenca_real_bud is not None else "-",
                    )
                with col_res_4:
                    _render_volume_card(
                        "Percentual Real/Bud",
                        f"{percentual_real_bud:,.1%}" if percentual_real_bud is not None else "-",
                    )

                st.markdown("<div class='volume-summary-spacer'></div>", unsafe_allow_html=True)

                # Exibir grÃ¡fico de Volume logo abaixo, usando os mesmos filtros
                st.subheader("ðŸ“Š Volume Total por PerÃ­odo")
                if 'J516' in df_vol_filtrado['VeÃ­culo'].values:
                    j516_vol_total = df_vol_filtrado[df_vol_filtrado['VeÃ­culo'] == 'J516']['Volume'].sum()
                grafico_volume = create_volume_chart(df_vol_filtrado, df_budget_vol_filtrado_grafico)
                if grafico_volume:
                    st.altair_chart(grafico_volume, use_container_width=True)
        
        # GrÃ¡fico de Volume por VeÃ­culo (dentro da aba Volume)
        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: Usar df_vol_filtrado (mesmo DataFrame usado no grÃ¡fico "Volume Total")
        # para garantir que os mesmos filtros de Oficina sejam aplicados
        if df_vol is not None and 'PerÃ­odo' in df_vol.columns and 'Volume' in df_vol.columns:
            if df_vol_filtrado is not None and 'Volume' in df_vol_filtrado.columns and 'Oficina' in df_vol_filtrado.columns:
                st.subheader("ðŸ“Š Volume por Oficina")
                df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
                grafico_volume_oficina = create_volume_oficina_chart(df_vol_filtrado, df_budget_vol_para_grafico)
                if grafico_volume_oficina is not None:
                    st.altair_chart(grafico_volume_oficina, use_container_width=True)

            if df_vol_filtrado is not None and 'Volume' in df_vol_filtrado.columns and 'VeÃ­culo' in df_vol_filtrado.columns:
                st.subheader("ðŸ“Š Volume por VeÃ­culo")
                
                if 'J516' in df_vol_filtrado['VeÃ­culo'].values:
                    j516_vol_filtrado = df_vol_filtrado[df_vol_filtrado['VeÃ­culo'] == 'J516']['Volume'].sum()
                # Usar df_budget_vol_filtrado_grafico se disponÃ­vel (mesma variÃ¡vel usada no grÃ¡fico de volume por perÃ­odo)
                df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
                # Volume nÃ£o deve ser recortado por existÃªncia de despesa.
                grafico_volume_veiculo = create_volume_veiculo_chart(df_vol_filtrado, df_budget_vol_para_grafico)
                if grafico_volume_veiculo is not None:
                    st.altair_chart(grafico_volume_veiculo, use_container_width=True)

# GrÃ¡fico 2: Soma do Valor por Oficina
# Cache removido temporariamente para forÃ§ar atualizaÃ§Ã£o
def create_oficina_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None):
    """Cria grÃ¡fico de barras por Oficina com linha de Flex Bud opcional"""
    try:
        # Robustez: garantir nomes/valores canÃ´nicos (ex.: Per\uFFFDodo -> PerÃ­odo)
        try:
            df_data = padronizar_colunas(df_data)
            df_budget = padronizar_colunas(df_budget) if df_budget is not None else None
            df_budget_vol = padronizar_colunas(df_budget_vol) if df_budget_vol is not None else None
            df_real_vol = padronizar_colunas(df_real_vol) if df_real_vol is not None else None
            df_real_original = padronizar_colunas(df_real_original) if df_real_original is not None else None
        except Exception:
            pass

        if 'Oficina' not in df_data.columns:
            return None
        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns):
                return None

        # ðŸ”§ CORREÃ‡ÃƒO: No modo CPU, sempre agrupar apenas por Oficina (sem VeÃ­culo) para padronizar com Custo Total
        # Removido o bloco que agrupava por VeÃ­culo - agora sempre usa a lÃ³gica do bloco "else" abaixo
        if (tipo_viz == "CPU (Custo por Unidade)" and
                'VeÃ­culo' in df_data.columns and
                'Total' not in df_data.columns):
            chart_data = df_data.groupby(
                ['Oficina', 'VeÃ­culo'], as_index=False
            )[coluna].sum()

            # Ordenar por Oficina e depois por CPU decrescente
            chart_data = chart_data.sort_values(
                ['Oficina', coluna], ascending=[True, False]
            )

            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Oficina e VeÃ­culo"

            # Criar grÃ¡fico de barras agrupadas
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y', axis=alt.Axis(grid=False)),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    'VeÃ­culo:N',
                    title='VeÃ­culo',
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=',.2f'
                    )
                ]
            ).properties(
                height=300,
                width='container'
                # TÃ­tulo removido para evitar duplicaÃ§Ã£o com st.subheader
            )

            # Adicionar rÃ³tulos com valores nas barras
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                text=alt.Text(f'{coluna}:Q', format=',.2f')
            )

            return grafico_barras + rotulos
        else:
            # GrÃ¡fico normal sem separaÃ§Ã£o por veÃ­culo
            # Para CPU, calcular SEM depender de Volume jÃ¡ mergeado em df_data.
            # Regra: CPU = soma(Total) / soma(Volume) no mesmo recorte/filtros.
            # Isso evita subestimar CPU quando o volume Ã© repetido por divergÃªncia de grÃ£o.
            if tipo_viz == "CPU (Custo por Unidade)" and df_real_vol is not None:
                try:
                    df_custo_base = df_real_original if df_real_original is not None else df_data

                    # Recorte: garantir que custo/volume respeitam o mesmo slice visÃ­vel no grÃ¡fico.
                    try:
                        oficinas_recorte = set(df_data['Oficina'].astype(str).str.strip().dropna().unique().tolist()) if 'Oficina' in df_data.columns else set()
                    except Exception:
                        oficinas_recorte = set()
                    try:
                        periodos_recorte = set(df_data['PerÃ­odo'].astype(str).str.strip().dropna().unique().tolist()) if 'PerÃ­odo' in df_data.columns else None
                    except Exception:
                        periodos_recorte = None
                    try:
                        anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist()) if 'Ano' in df_data.columns else None
                    except Exception:
                        anos_recorte = None
                    try:
                        veiculos_recorte = set(df_data['VeÃ­culo'].astype(str).str.strip().dropna().unique().tolist()) if 'VeÃ­culo' in df_data.columns else None
                    except Exception:
                        veiculos_recorte = None

                    def _recortar(df_in: pd.DataFrame | None) -> pd.DataFrame | None:
                        if df_in is None:
                            return None
                        df_out = df_in
                        if oficinas_recorte and 'Oficina' in df_out.columns:
                            df_out = df_out[df_out['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()
                        if periodos_recorte is not None and 'PerÃ­odo' in df_out.columns:
                            df_out = df_out[df_out['PerÃ­odo'].astype(str).str.strip().isin(periodos_recorte)].copy()
                        if anos_recorte is not None and 'Ano' in df_out.columns:
                            ano_num = pd.to_numeric(df_out['Ano'], errors='coerce')
                            df_out = df_out[ano_num.isin(anos_recorte)].copy()
                        # SÃ³ recortar por VeÃ­culo se a base tiver a coluna; volume do Budget pode nÃ£o ter.
                        if veiculos_recorte is not None and 'VeÃ­culo' in df_out.columns:
                            df_out = df_out[df_out['VeÃ­culo'].astype(str).str.strip().isin(veiculos_recorte)].copy()
                        return df_out

                    df_custo_base = _recortar(df_custo_base)
                    df_real_vol_cpu = _recortar(df_real_vol)

                    df_cpu = cpu_por_chaves(
                        df_custo_base,
                        df_real_vol_cpu,
                        chaves_preferidas=("Ano", "PerÃ­odo", "Oficina"),
                        coluna_custo="Total",
                        coluna_volume="Volume",
                    )
                    if df_cpu is not None and not df_cpu.empty:
                        chart_data = (
                            df_cpu.groupby('Oficina', as_index=False)
                            .agg({'Total': 'sum', 'Volume': 'sum'})
                        )
                        vol = pd.to_numeric(chart_data['Volume'], errors='coerce').fillna(0)
                        tot = pd.to_numeric(chart_data['Total'], errors='coerce').fillna(0)
                        chart_data[coluna] = np.where(vol != 0, tot / vol, 0)
                        chart_data = chart_data[['Oficina', coluna]]
                    else:
                        chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
                except Exception:
                    chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
            else:
                # Caminho quando nÃ£o tem Total/Volume ou nÃ£o tem VeÃ­culo
                chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()

            # SanitizaÃ§Ã£o mÃ­nima para evitar grÃ¡ficos vazios/legendas NaN
            chart_data = chart_data.copy()
            chart_data['Oficina'] = chart_data['Oficina'].astype(str).str.strip()
            chart_data = chart_data[
                chart_data['Oficina'].notna()
                & (chart_data['Oficina'] != '')
                & (~chart_data['Oficina'].str.lower().isin(['nan', 'none']))
            ].copy()
            chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
            chart_data['Tipo'] = 'Real'

            if chart_data.empty:
                return None

            # Se, apÃ³s filtros/limpeza, todos os valores ficaram zerados,
            # o grÃ¡fico de barras pode parecer "em branco". Retornar um aviso visual.
            try:
                if float(chart_data[coluna].abs().sum()) == 0.0:
                    aviso_df = pd.DataFrame({"msg": ["Sem valores para exibir apÃ³s filtros"]})
                    return (
                        alt.Chart(aviso_df)
                        .mark_text(size=14, color="#666", align="center")
                        .encode(text="msg:N")
                        .properties(height=80, width=900)
                    )
            except Exception:
                pass
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores vÃ¡lidos
            if coluna not in chart_data.columns:
                return None
            
            chart_data = chart_data.sort_values(coluna, ascending=False)
            
            # Determinar ordem das oficinas (usar a mesma ordem para barras e linha)
            ordem_oficinas_barras = chart_data['Oficina'].tolist()

            # Definir tÃ­tulo do eixo Y baseado no tipo e moeda
            if tipo_viz == "CPU (Custo por Unidade)":
                titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
                titulo_grafico = "CPU por Oficina"
            else:
                titulo_y = f"Soma do Valor ({moeda_simbolo})"
                titulo_grafico = "Soma do Valor por Oficina"

            # Processar dados de budget e calcular FLEX se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'Oficina' in df_budget.columns and df_real_vol is not None:
                # A funÃ§Ã£o precisa da coluna 'Custo'
                # Se nÃ£o tiver, tentar usar df_real_original ou df_total que deve ter
                if 'Custo' not in df_real_para_flex.columns:
                    # Tentar usar df_total global que deve ter a coluna 'Custo'
                    if 'df_total' in globals() and 'Custo' in globals()['df_total'].columns:
                        df_real_para_flex = globals()['df_total'].copy()
                    else:
                        df_real_para_flex = None
                
                if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                    try:
                        # ðŸ”§ CORREÃ‡ÃƒO: Normalizar perÃ­odos ANTES de agrupar (mesma lÃ³gica do calcular_flex_budget)
                        mapeamento_meses = {
                            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'marÃ§o': 'MarÃ§o',
                            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                        }
                        
                        def normalizar_periodo(periodo):
                            """Normaliza perÃ­odo para formato capitalizado"""
                            if pd.isna(periodo):
                                return periodo
                            periodo_str = str(periodo).strip()
                            for mes_min, mes_cap in mapeamento_meses.items():
                                if periodo_str.lower() == mes_min.lower():
                                    return mes_cap
                            return periodo_str
                        
                        # Normalizar perÃ­odos em todos os DataFrames
                        if 'PerÃ­odo' in df_real_para_flex.columns:
                            df_real_para_flex = df_real_para_flex.copy()
                            df_real_para_flex['PerÃ­odo'] = df_real_para_flex['PerÃ­odo'].apply(normalizar_periodo)
                        if df_real_vol is not None and 'PerÃ­odo' in df_real_vol.columns:
                            df_real_vol = df_real_vol.copy()
                            df_real_vol['PerÃ­odo'] = df_real_vol['PerÃ­odo'].apply(normalizar_periodo)
                        if 'PerÃ­odo' in df_budget.columns:
                            df_budget = df_budget.copy()
                            df_budget['PerÃ­odo'] = df_budget['PerÃ­odo'].apply(normalizar_periodo)
                        if df_budget_vol is not None and 'PerÃ­odo' in df_budget_vol.columns:
                            df_budget_vol = df_budget_vol.copy()
                            df_budget_vol['PerÃ­odo'] = df_budget_vol['PerÃ­odo'].apply(normalizar_periodo)

                        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: o Flex Bud/Delta DEVEM respeitar o mesmo recorte do grÃ¡fico (Ano/PerÃ­odo/Oficina).
                        # Sem isso, com filtro em um mÃªs (ex.: Agosto/2026), a linha/delta pode acabar somando o ano inteiro.
                        try:
                            oficinas_recorte = set(chart_data['Oficina'].astype(str).str.strip().unique().tolist())
                        except Exception:
                            oficinas_recorte = set()

                        anos_recorte = None
                        if 'Ano' in df_data.columns:
                            try:
                                anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist())
                            except Exception:
                                anos_recorte = None

                        periodos_recorte = None
                        if 'PerÃ­odo' in df_data.columns:
                            try:
                                periodos_recorte = set(df_data['PerÃ­odo'].astype(str).str.strip().unique().tolist())
                            except Exception:
                                periodos_recorte = None

                        def _aplicar_recorte(df_in: pd.DataFrame | None) -> pd.DataFrame | None:
                            if df_in is None:
                                return None
                            df_out = df_in
                            if oficinas_recorte and 'Oficina' in df_out.columns:
                                df_out = df_out[df_out['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()
                            if periodos_recorte is not None and 'PerÃ­odo' in df_out.columns:
                                df_out = df_out[df_out['PerÃ­odo'].astype(str).str.strip().isin(periodos_recorte)].copy()
                            if anos_recorte is not None and 'Ano' in df_out.columns:
                                ano_num = pd.to_numeric(df_out['Ano'], errors='coerce')
                                df_out = df_out[ano_num.isin(anos_recorte)].copy()
                            return df_out

                        df_real_para_flex = _aplicar_recorte(df_real_para_flex)
                        df_real_vol = _aplicar_recorte(df_real_vol)
                        df_budget = _aplicar_recorte(df_budget)
                        df_budget_vol = _aplicar_recorte(df_budget_vol)
                        
                        # Calcular FLEX agrupado por Oficina seguindo a mesma lÃ³gica do grÃ¡fico por PerÃ­odo
                        # Primeiro calcular FLEX por PerÃ­odo e Oficina, depois agrupar por Oficina
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        
                        # Calcular Flex Bud por PerÃ­odo e Oficina (mesma lÃ³gica do grÃ¡fico por PerÃ­odo)
                        # Agrupar dados reais por PerÃ­odo e Oficina
                        if tem_ano:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Ano', 'PerÃ­odo', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Ano', 'PerÃ­odo', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            # Agrupar budget por PerÃ­odo e Oficina
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Ano', 'PerÃ­odo', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'PerÃ­odo', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        else:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['PerÃ­odo', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['PerÃ­odo', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['PerÃ­odo', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['PerÃ­odo', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        
                        # Verificar se temos todos os dados necessÃ¡rios
                        if (real_agrupado is None or real_vol_agrupado is None or 
                            budget_agrupado is None or budget_vol_agrupado is None):
                            flex_data = None
                        else:
                            # ðŸ”§ CORREÃ‡ÃƒO: Normalizar perÃ­odos nos DataFrames agrupados antes do merge
                            if tem_ano:
                                real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                                budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                                real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
                                budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
                            else:
                                real_vol_agrupado['PerÃ­odo'] = real_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                                budget_vol_agrupado['PerÃ­odo'] = budget_vol_agrupado['PerÃ­odo'].astype(str).str.strip()
                                real_agrupado['PerÃ­odo'] = real_agrupado['PerÃ­odo'].astype(str).str.strip()
                                budget_agrupado['PerÃ­odo'] = budget_agrupado['PerÃ­odo'].astype(str).str.strip()
                            
                            # Fazer merge de volumes por PerÃ­odo e Oficina
                            # Usar 'left' para incluir todas as oficinas dos dados reais, mesmo sem budget
                            if tem_ano:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Ano', 'PerÃ­odo', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se nÃ£o houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            else:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['PerÃ­odo', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se nÃ£o houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            
                            # ðŸ”§ CORREÃ‡ÃƒO: Para CPU, nÃ£o podemos somar os CPUs de cada perÃ­odo
                            # Devemos calcular o Flex Bud Total (Custo Total) por perÃ­odo e oficina,
                            # depois agregar por oficina e recalcular o CPU final
                            
                            # Calcular Flex Bud Total (Custo Total) por PerÃ­odo e Oficina (vetorizado)
                            try:
                                chaves_base = ['PerÃ­odo', 'Oficina']
                                if tem_ano:
                                    chaves_base = ['Ano'] + chaves_base

                                # Budget: pivot por Custo para obter Fixo e Total
                                budget_agrupado = budget_agrupado.copy()
                                if 'Custo' in budget_agrupado.columns:
                                    budget_agrupado['Custo'] = budget_agrupado['Custo'].apply(_normalizar_rotulo_custo)
                                budget_piv = budget_agrupado.pivot_table(
                                    index=chaves_base,
                                    columns='Custo',
                                    values='Total',
                                    aggfunc='sum',
                                    fill_value=0
                                )
                                custo_fixo_budget = budget_piv['Fixo'] if 'Fixo' in budget_piv.columns else 0
                                budget_total = budget_piv.sum(axis=1)
                                budget_sum = pd.DataFrame({
                                    'Budget_Total': budget_total,
                                    'Custo_Fixo_Budget': custo_fixo_budget,
                                }).reset_index()

                                flex_df = volumes.merge(budget_sum, on=chaves_base, how='left')
                                flex_df['Budget_Total'] = pd.to_numeric(flex_df['Budget_Total'], errors='coerce').fillna(0)
                                flex_df['Custo_Fixo_Budget'] = pd.to_numeric(flex_df['Custo_Fixo_Budget'], errors='coerce').fillna(0)

                                flex_df['Volume_real'] = pd.to_numeric(flex_df['Volume_real'], errors='coerce').fillna(0)
                                flex_df['Volume_budget'] = pd.to_numeric(flex_df['Volume_budget'], errors='coerce').fillna(0)
                                custo_nao_fixo = flex_df['Budget_Total'] - flex_df['Custo_Fixo_Budget']
                                proporcao = np.where(flex_df['Volume_budget'] != 0, flex_df['Volume_real'] / flex_df['Volume_budget'], 1.0)
                                flex_df['Flex_Bud_Total'] = flex_df['Custo_Fixo_Budget'] + (custo_nao_fixo * proporcao)
                                flex_df['Volume_Real'] = flex_df['Volume_real']

                                flex_data = flex_df[chaves_base + ['Flex_Bud_Total', 'Volume_Real']].copy()
                            except Exception:
                                flex_data = None
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud Total e Volume Real por Oficina (somar todos os perÃ­odos)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Para CPU: somar Flex Bud Total e Volume Real, depois recalcular CPU
                                budget_data = flex_data.groupby('Oficina').agg({
                                    'Flex_Bud_Total': 'sum',
                                    'Volume_Real': 'sum'
                                }).reset_index()
                                
                                # Recalcular CPU: Flex Bud Total agregado / Volume Real agregado (vetorizado)
                                volr = pd.to_numeric(budget_data['Volume_Real'], errors='coerce').fillna(0)
                                fbt = pd.to_numeric(budget_data['Flex_Bud_Total'], errors='coerce').fillna(0)
                                budget_data[coluna] = np.where(volr != 0, fbt / volr, 0)
                                
                                # Manter apenas colunas necessÃ¡rias
                                budget_data = budget_data[['Oficina', coluna]]
                            else:
                                # Para Custo Total: apenas somar Flex Bud Total
                                budget_data = flex_data.groupby('Oficina')['Flex_Bud_Total'].sum().reset_index()
                                budget_data.rename(columns={'Flex_Bud_Total': coluna}, inplace=True)
                            
                            # Filtrar apenas oficinas que existem no chart_data
                            budget_data['Oficina'] = budget_data['Oficina'].astype(str).str.strip()
                            budget_data = budget_data[budget_data['Oficina'].isin(chart_data['Oficina'])].copy()
                            
                            if len(budget_data) > 0:
                                # Criar linha tracejada de Flex Bud
                                budget_data_legenda = budget_data.copy()
                                budget_data_legenda['Tipo'] = 'Flex Bud'
                                
                                # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras
                                # Reordenar budget_data_legenda para seguir a ordem de ordem_oficinas_barras
                                # Criar um dicionÃ¡rio de mapeamento de ordem
                                ordem_dict = {oficina: idx for idx, oficina in enumerate(ordem_oficinas_barras)}
                                # Adicionar coluna de ordem para ordenar
                                budget_data_legenda['_ordem'] = budget_data_legenda['Oficina'].map(ordem_dict)
                                # Filtrar apenas oficinas que existem na ordem e ordenar
                                budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                                budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                                budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                                
                                # Usar a mesma ordem das barras (filtrando apenas oficinas que existem no budget)
                                ordem_oficinas = [o for o in ordem_oficinas_barras if o in budget_data_legenda['Oficina'].tolist()]
                                
                                # Criar linha tracejada de Flex Bud (igual ao grÃ¡fico por PerÃ­odo)
                                linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                    strokeDash=[10, 5],
                                    strokeWidth=1.5,
                                    opacity=0.8
                                ).encode(
                                    x=alt.X(
                                        'Oficina:N',
                                        title='Oficina',
                                        sort=ordem_oficinas,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    y=alt.Y(
                                        f'{coluna}:Q',
                                        title=titulo_y,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    color=alt.Color(
                                        'Tipo:N',
                                        title='Legenda',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                        legend=None
                                    ),
                                    strokeDash=alt.StrokeDash(
                                        'Tipo:N',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=[[0], [10, 5]]),
                                        legend=None
                                    ),
                                    tooltip=[
                                        alt.Tooltip('Oficina:N', title='Oficina'),
                                        alt.Tooltip('Tipo:N', title='Tipo'),
                                        alt.Tooltip(
                                            f'{coluna}:Q',
                                            title='Flex Bud',
                                            format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                        )
                                    ]
                                )
                                
                                # Adicionar bolinhas nos pontos da linha (usar mesma ordem)
                                pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                    size=80,
                                    opacity=0.9
                                ).encode(
                                    x=alt.X(
                                        'Oficina:N',
                                        title='Oficina',
                                        sort=ordem_oficinas,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    y=alt.Y(
                                        f'{coluna}:Q',
                                        title=titulo_y,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    color=alt.Color(
                                        'Tipo:N',
                                        title='Legenda',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                        legend=None
                                    ),
                                    tooltip=[
                                        alt.Tooltip('Oficina:N', title='Oficina'),
                                        alt.Tooltip('Tipo:N', title='Tipo'),
                                        alt.Tooltip(
                                            f'{coluna}:Q',
                                            title='Flex Bud',
                                            format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                        )
                                    ]
                                )
                                
                                # Adicionar rÃ³tulos
                                formato_rotulo_budget = ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                    align='center',
                                    baseline='bottom',
                                    dy=-20,
                                    color='#FF6B35',
                                    fontSize=9,
                                    fontWeight='bold'
                                ).encode(
                                    x=alt.X('Oficina:N', sort=ordem_oficinas),
                                    y=alt.Y(f'{coluna}:Q'),
                                    text=alt.Text(f'{coluna}:Q', format=formato_rotulo_budget)
                                )
                                
                                # Combinar linha, pontos e rÃ³tulos
                                linha_budget = linha_budget + pontos_budget + rotulos_budget
                    except Exception as e:
                        # Silenciar erro, apenas nÃ£o mostrar linha de budget
                        pass

            # Usar a ordem explÃ­cita para garantir sincronizaÃ§Ã£o com a linha pontilhada
            max_abs_cpu = None
            if tipo_viz == "CPU (Custo por Unidade)":
                try:
                    max_abs_cpu = float(pd.to_numeric(chart_data[coluna], errors='coerce').abs().max())
                except Exception:
                    max_abs_cpu = None

            if tipo_viz == "CPU (Custo por Unidade)":
                y_enc = alt.Y(
                    f'{coluna}:Q',
                    title=titulo_y,
                    axis=alt.Axis(
                        grid=False,
                        domain=True,
                        ticks=True,
                        format=',.4f' if max_abs_cpu is not None and max_abs_cpu < 1 else ',.2f'
                    ),
                    scale=alt.Scale(zero=False)
                )
                formato_rotulo = ',.4f' if max_abs_cpu is not None and max_abs_cpu < 1 else ',.2f'
            else:
                y_enc = alt.Y(
                    f'{coluna}:Q',
                    title=titulo_y,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                )
                formato_rotulo = ',.2f'

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort=ordem_oficinas_barras, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                y=y_enc,
                color=alt.Color(
                    'Tipo:N',
                    title='Legenda',
                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                    legend=alt.Legend(
                        title='Legenda',
                        orient='bottom',
                        titleFontSize=10,
                        labelFontSize=9,
                        titleAnchor='middle',
                        direction='horizontal',
                        symbolType='square'
                    )
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('Tipo:N', title='Tipo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato_rotulo
                    )
                ]
            ).properties(
                height=300,
                width=900
                # TÃ­tulo removido para evitar duplicaÃ§Ã£o com st.subheader
            )

            # Adicionar rÃ³tulos com valores nas barras
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                x=alt.X('Oficina:N', sort=ordem_oficinas_barras, title='Oficina'),
                text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
            )

            # Pontos para garantir visibilidade quando os valores sÃ£o muito pequenos
            pontos_real = alt.Chart(chart_data).mark_circle(size=70, opacity=0.9).encode(
                x=alt.X('Oficina:N', sort=ordem_oficinas_barras, title='Oficina'),
                y=y_enc,
                color=alt.Color(
                    'Tipo:N',
                    title='Legenda',
                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                    legend=None
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('Tipo:N', title='Tipo'),
                    alt.Tooltip(f'{coluna}:Q', title=coluna, format=formato_rotulo)
                ]
            )

            # Criar grÃ¡fico de delta (Real - Flex Bud) se linha_budget estiver disponÃ­vel
            grafico_delta = None
            if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
                try:
                    # Calcular delta: Real - Flex Bud
                    delta_data = chart_data.copy()
                    budget_data_merge = budget_data.copy()
                    budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                    
                    delta_data = delta_data.merge(
                        budget_data_merge[['Oficina', f'{coluna}_FlexBud']],
                        on='Oficina',
                        how='left'
                    )
                    
                    # Calcular delta: Real - Flex Bud
                    delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data[f'{coluna}_FlexBud'].fillna(0)
                    
                    # Calcular min e max do delta para a escala de cores
                    # Usar valores absolutos simÃ©tricos para garantir que zero sempre seja o centro
                    delta_min_abs = abs(delta_data['Delta'].min())
                    delta_max_abs = abs(delta_data['Delta'].max())
                    delta_abs_max = max(delta_min_abs, delta_max_abs)
                    
                    # Criar domÃ­nio simÃ©trico baseado no maior valor absoluto
                    # Isso garante que zero sempre fique no centro, independente dos filtros
                    delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                    delta_max = delta_abs_max if delta_abs_max > 0 else 1
                    
                    # Ordenar por valor para manter ordem consistente
                    delta_data = delta_data.sort_values(coluna, ascending=False)
                    ordem_oficinas_delta = delta_data['Oficina'].tolist()
                    
                    # Criar grÃ¡fico de barras para delta
                    grafico_delta = alt.Chart(delta_data).mark_bar(
                        size=20
                    ).encode(
                        x=alt.X(
                            'Oficina:N',
                            title='',
                            sort=ordem_oficinas_delta,
                            axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)
                        ),
                        y=alt.Y(
                            'Delta:Q',
                            title='Delta (Real - Flex Bud)',
                            axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                        ),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[delta_min, 0, delta_max],
                                range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip('Oficina:N', title='Oficina'),
                            alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                            alt.Tooltip(f'{coluna}:Q', title='Real', format=',.2f'),
                            alt.Tooltip(f'{coluna}_FlexBud:Q', title='Flex Bud', format=',.2f')
                        ]
                    ).properties(
                        height=38
                    )
                    
                    # Adicionar rÃ³tulos de dados no grÃ¡fico de delta
                    # Usar a mesma cor das barras (escala baseada no valor do Delta)
                    rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                        align='center',
                        baseline='bottom',
                        dy=-12,
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X('Oficina:N', sort=ordem_oficinas_delta),
                        y=alt.Y('Delta:Q'),
                        text=alt.Text('Delta:Q', format=',.2f'),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[0, delta_max],
                                range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        )
                    )
                    
                    rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                        align='center',
                        baseline='top',
                        dy=12,
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X('Oficina:N', sort=ordem_oficinas_delta),
                        y=alt.Y('Delta:Q'),
                        text=alt.Text('Delta:Q', format=',.2f'),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[delta_min, 0],
                                range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        )
                    )
                    
                    grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
                except Exception as e:
                    pass  # Silenciar erro, apenas nÃ£o mostrar delta
            
            # Combinar grÃ¡fico de barras com linha de budget se disponÃ­vel
            if linha_budget is not None:
                grafico_principal = alt.layer(
                    grafico_barras,
                    rotulos,
                    pontos_real,
                    linha_budget
                ).resolve_scale(
                    x='shared',
                    y='shared',
                    color='shared'
                ).resolve_legend(
                    color='shared'
                )
                
                # Se temos grÃ¡fico de delta, combinar verticalmente (delta em cima)
                if grafico_delta is not None:
                    grafico_final = alt.vconcat(
                        grafico_delta,
                        grafico_principal
                    ).resolve_scale(
                        x='shared'
                    )
                else:
                    grafico_final = grafico_principal
            else:
                grafico_final = alt.layer(grafico_barras, rotulos, pontos_real)
            
            return grafico_final
    except Exception as e:
        st.error(f"Erro ao criar grÃ¡fico: {e}")
        return None


# GrÃ¡fico 4: Total/CPU por VeÃ­culo
# Cache removido temporariamente para forÃ§ar atualizaÃ§Ã£o
def create_total_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, df_visualizacao_volume=None, df_total_completo=None, df_despesas=None, df_total_filtrado=None, df_volume_filtrado_grafico=None, df_real_vol_share_base=None):
    """Cria grÃ¡fico de barras de Total/CPU por VeÃ­culo com linha de Flex Bud opcional"""
    try:
        # Robustez: garantir nomes/valores canÃ´nicos (ex.: Per\uFFFDodo -> PerÃ­odo)
        try:
            df_data = padronizar_colunas(df_data)
            df_budget = padronizar_colunas(df_budget) if df_budget is not None else None
            df_budget_vol = padronizar_colunas(df_budget_vol) if df_budget_vol is not None else None
            df_real_vol = padronizar_colunas(df_real_vol) if df_real_vol is not None else None
            df_real_original = padronizar_colunas(df_real_original) if df_real_original is not None else None
            df_total_filtrado = padronizar_colunas(df_total_filtrado) if df_total_filtrado is not None else None
            df_volume_filtrado_grafico = (
                padronizar_colunas(df_volume_filtrado_grafico)
                if df_volume_filtrado_grafico is not None
                else None
            )
        except Exception:
            pass

        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns):
                return None

        # Definir tÃ­tulo e formato baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            formato = ',.2f'
            if 'VeÃ­culo' in df_data.columns:
                titulo_grafico = "CPU por VeÃ­culo"
            else:
                titulo_grafico = "CPU por PerÃ­odo"
        else:
            titulo_y = f"Total ({moeda_simbolo})"
            formato = ',.2f'
            if 'VeÃ­culo' in df_data.columns:
                titulo_grafico = "Total por VeÃ­culo"
            else:
                titulo_grafico = "Total por PerÃ­odo"

        # Verificar se tem coluna VeÃ­culo
        if 'VeÃ­culo' in df_data.columns:
            # Para CPU, recalcular a partir de Total e Volume agregados
            # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: No modo CPU, df_data (df_visualizacao) tem Total e Volume jÃ¡ agregados por Oficina+PerÃ­odo+VeÃ­culo
            # Quando agrupamos apenas por VeÃ­culo, estamos somando valores que podem estar duplicados
            # Precisamos usar o Total DIRETO de df_real_original (dados originais) e Volume DIRETO de df_real_vol (arquivo original)
            if tipo_viz == "CPU (Custo por Unidade)":
                # Regra (igual Ã s tabelas): CPU = soma(Total) / soma(Volume) no grÃ£o do veÃ­culo.
                # Evita erro quando Volume foi mergeado/duplicado em df_data.
                base_custo = df_total_filtrado if df_total_filtrado is not None else (df_real_original if df_real_original is not None else df_data)
                base_vol = df_volume_filtrado_grafico if df_volume_filtrado_grafico is not None else (df_real_vol if df_real_vol is not None else df_visualizacao_volume)

                if base_custo is None or base_vol is None:
                    return None
                if 'Total' not in base_custo.columns or 'VeÃ­culo' not in base_custo.columns:
                    return None
                if 'Volume' not in base_vol.columns or 'VeÃ­culo' not in base_vol.columns:
                    return None

                # Regra da tabela (e do esperado do usuÃ¡rio):
                # CPU = sum(Total) / sum(Volume) por VeÃ­culo, incluindo volume mesmo quando nÃ£o hÃ¡ custo.
                # Portanto: agregamos custo e volume separadamente e fazemos merge OUTER.
                custo_por_veic = (
                    base_custo
                    .assign(Total=pd.to_numeric(base_custo['Total'], errors='coerce').fillna(0))
                    .groupby('VeÃ­culo', dropna=False)
                    .agg({'Total': 'sum'})
                    .reset_index()
                )
                vol_por_veic = (
                    base_vol
                    .assign(Volume=pd.to_numeric(base_vol['Volume'], errors='coerce').fillna(0))
                    .groupby('VeÃ­culo', dropna=False)
                    .agg({'Volume': 'sum'})
                    .reset_index()
                )
                chart_data = pd.merge(custo_por_veic, vol_por_veic, on='VeÃ­culo', how='outer')
                chart_data['Total'] = pd.to_numeric(chart_data.get('Total', 0), errors='coerce').fillna(0)
                chart_data['Volume'] = pd.to_numeric(chart_data.get('Volume', 0), errors='coerce').fillna(0)
                chart_data[coluna] = np.where(chart_data['Volume'] != 0, chart_data['Total'] / chart_data['Volume'], 0)
                chart_data = chart_data[['VeÃ­culo', coluna]]

                # Sinalizar que jÃ¡ calculamos corretamente e nÃ£o precisamos do fallback antigo
                usar_logica_antiga = False
            else:
                chart_data = (
                    df_data.groupby('VeÃ­culo')[coluna].sum().reset_index()
                )
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores vÃ¡lidos
            if coluna not in chart_data.columns:
                return None
            
            chart_data = chart_data.sort_values(coluna, ascending=False)

            # Determinar ordem dos veÃ­culos (usar a mesma ordem para barras e linha)
            ordem_veiculos_barras = chart_data['VeÃ­culo'].tolist()

            # Processar dados de budget (CPU por veÃ­culo) se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'VeÃ­culo' in df_budget.columns and df_real_vol is not None:
                try:
                    # CPU Budget por veÃ­culo: sum(Total_veÃ­culo) / sum(Volume_veÃ­culo)
                    # GovernanÃ§a: Volume do Budget deve ter 'VeÃ­culo' (sem rateio/fallback no app).

                    base_budget_custo = df_budget.copy()
                    if 'Total' not in base_budget_custo.columns and 'Valor' in base_budget_custo.columns:
                        base_budget_custo['Total'] = base_budget_custo['Valor']
                    base_budget_custo['Total'] = pd.to_numeric(base_budget_custo.get('Total', 0), errors='coerce').fillna(0)

                    base_budget_vol = df_budget_vol.copy() if df_budget_vol is not None else None
                    base_real_vol_share = df_real_vol_share_base if df_real_vol_share_base is not None else df_real_vol

                    # Recorte de Ano/PerÃ­odo/Oficina conforme o grÃ¡fico
                    try:
                        periodos_recorte = (
                            set(df_data['PerÃ­odo'].astype(str).str.strip().dropna().unique().tolist())
                            if 'PerÃ­odo' in df_data.columns
                            else None
                        )
                    except Exception:
                        periodos_recorte = None
                    try:
                        anos_recorte = (
                            set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist())
                            if 'Ano' in df_data.columns
                            else None
                        )
                    except Exception:
                        anos_recorte = None
                    try:
                        oficinas_recorte = (
                            set(df_data['Oficina'].astype(str).str.strip().dropna().unique().tolist())
                            if 'Oficina' in df_data.columns
                            else None
                        )
                    except Exception:
                        oficinas_recorte = None

                    for df_tmp_name in ['base_budget_custo', 'base_budget_vol', 'base_real_vol_share']:
                        pass

                    if periodos_recorte is not None and 'PerÃ­odo' in base_budget_custo.columns:
                        base_budget_custo = base_budget_custo[base_budget_custo['PerÃ­odo'].astype(str).str.strip().isin(periodos_recorte)].copy()
                    if anos_recorte is not None and 'Ano' in base_budget_custo.columns:
                        ano_num = pd.to_numeric(base_budget_custo['Ano'], errors='coerce')
                        base_budget_custo = base_budget_custo[ano_num.isin(anos_recorte)].copy()
                    if oficinas_recorte is not None and 'Oficina' in base_budget_custo.columns:
                        base_budget_custo = base_budget_custo[base_budget_custo['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()

                    if base_budget_vol is not None:
                        if periodos_recorte is not None and 'PerÃ­odo' in base_budget_vol.columns:
                            base_budget_vol = base_budget_vol[base_budget_vol['PerÃ­odo'].astype(str).str.strip().isin(periodos_recorte)].copy()
                        if anos_recorte is not None and 'Ano' in base_budget_vol.columns:
                            ano_num = pd.to_numeric(base_budget_vol['Ano'], errors='coerce')
                            base_budget_vol = base_budget_vol[ano_num.isin(anos_recorte)].copy()
                        if oficinas_recorte is not None and 'Oficina' in base_budget_vol.columns:
                            base_budget_vol = base_budget_vol[base_budget_vol['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()

                    if base_real_vol_share is not None:
                        if periodos_recorte is not None and 'PerÃ­odo' in base_real_vol_share.columns:
                            base_real_vol_share = base_real_vol_share[base_real_vol_share['PerÃ­odo'].astype(str).str.strip().isin(periodos_recorte)].copy()
                        if anos_recorte is not None and 'Ano' in base_real_vol_share.columns:
                            ano_num = pd.to_numeric(base_real_vol_share['Ano'], errors='coerce')
                            base_real_vol_share = base_real_vol_share[ano_num.isin(anos_recorte)].copy()
                        if oficinas_recorte is not None and 'Oficina' in base_real_vol_share.columns:
                            base_real_vol_share = base_real_vol_share[base_real_vol_share['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()

                    tem_ano_budget = 'Ano' in base_budget_custo.columns
                    keys_full = ['Oficina', 'PerÃ­odo', 'VeÃ­culo'] + (['Ano'] if tem_ano_budget else [])
                    # custo agregado
                    custo_agr = (
                        base_budget_custo
                        .groupby([k for k in keys_full if k in base_budget_custo.columns], dropna=False)
                        .agg({'Total': 'sum'})
                        .reset_index()
                    )

                    # volume
                    if base_budget_vol is not None and 'Volume' in getattr(base_budget_vol, 'columns', []) and 'VeÃ­culo' in getattr(base_budget_vol, 'columns', []):
                        vol_keys = [k for k in keys_full if k in base_budget_vol.columns]
                        vol_agr = (
                            base_budget_vol
                            .groupby(vol_keys, dropna=False)
                            .agg({'Volume': 'sum'})
                            .reset_index()
                        )
                        base = pd.merge(custo_agr, vol_agr, on=vol_keys, how='outer')
                    else:
                        st.error(
                            "âŒ ERRO NA EXTRAÃ‡ÃƒO: o Volume do Budget precisa conter a coluna 'VeÃ­culo'. "
                            "NÃ£o Ã© mais permitido rateio/fallback no app."
                        )
                        st.info(
                            "ðŸ’¡ RefaÃ§a a extraÃ§Ã£o do BUDGET (pÃ¡gina 'ExtraÃ§Ã£o de Dados') e corrija a aba 'Volume BDG' no Excel."
                        )
                        st.stop()

                    base['Total'] = pd.to_numeric(base.get('Total', 0), errors='coerce').fillna(0)
                    base['Volume'] = pd.to_numeric(base.get('Volume', 0), errors='coerce').fillna(0)

                    # agregado final por veÃ­culo
                    budget_data = (
                        base.groupby('VeÃ­culo', dropna=False)
                        .agg({'Total': 'sum', 'Volume': 'sum'})
                        .reset_index()
                    )
                    vol = pd.to_numeric(budget_data['Volume'], errors='coerce').fillna(0)
                    tot = pd.to_numeric(budget_data['Total'], errors='coerce').fillna(0)
                    budget_data[coluna] = np.where(vol != 0, tot / vol, 0)
                    budget_data = budget_data[['VeÃ­culo', coluna]]
                    budget_data = budget_data[budget_data['VeÃ­culo'].isin(chart_data['VeÃ­culo'])].copy()

                    if len(budget_data) > 0:
                        # Criar linha tracejada de Budget
                        budget_data_legenda = budget_data.copy()
                        budget_data_legenda['Tipo'] = 'Budget'
                        
                        # IMPORTANTE: Usar a mesma ordem das barras (ordem_veiculos_barras)
                        # Reordenar budget_data_legenda para seguir a ordem de ordem_veiculos_barras
                        ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos_barras)}
                        budget_data_legenda['_ordem'] = budget_data_legenda['VeÃ­culo'].map(ordem_dict)
                        budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                        budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                        budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                        
                        # Usar a mesma ordem das barras (filtrando apenas veÃ­culos que existem no budget)
                        ordem_veiculos = [v for v in ordem_veiculos_barras if v in budget_data_legenda['VeÃ­culo'].tolist()]
                        
                        linha_budget = alt.Chart(budget_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                                        x=alt.X(
                                            'VeÃ­culo:N',
                                            title='VeÃ­culo',
                                            sort=ordem_veiculos,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        y=alt.Y(
                                            f'{coluna}:Q',
                                            title=titulo_y,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        color=alt.Color(
                                            'Tipo:N',
                                            title='Legenda',
                                            scale=alt.Scale(domain=['Real', 'Budget'], range=['#4A90E2', '#FF6B35']),
                                            legend=alt.Legend(
                                                title='Legenda',
                                                orient='bottom',
                                                titleFontSize=10,
                                                labelFontSize=9,
                                                titleAnchor='middle',
                                                direction='horizontal',
                                                symbolType='square'
                                            )
                                        ),
                                        strokeDash=alt.StrokeDash(
                                            'Tipo:N',
                                            scale=alt.Scale(domain=['Real', 'Budget'], range=[[0], [10, 5]]),
                                            legend=None
                                        ),
                                        tooltip=[
                                            alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                                            alt.Tooltip('Tipo:N', title='Tipo'),
                                            alt.Tooltip(
                                                f'{coluna}:Q',
                                                title='Budget',
                                                format=formato
                                            )
                                        ]
                                    )

                        # Adicionar pontos na linha
                        pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('VeÃ­culo:N', sort=ordem_veiculos),
                            y=alt.Y(f'{coluna}:Q'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Real', 'Budget'], range=['#4A90E2', '#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip(
                                    f'{coluna}:Q',
                                    title='Budget',
                                    format=formato
                                )
                            ]
                        )

                        # Adicionar rÃ³tulos
                        rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            color='#FF6B35',
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('VeÃ­culo:N', sort=ordem_veiculos),
                            y=alt.Y(f'{coluna}:Q'),
                            text=alt.Text(f'{coluna}:Q', format=formato)
                        )

                        linha_budget = linha_budget + pontos_budget + rotulos_budget
                except Exception:
                    pass  # Silenciar erro, apenas nÃ£o mostrar Budget

            # Usar a ordem explÃ­cita para garantir sincronizaÃ§Ã£o com a linha pontilhada
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    'VeÃ­culo:N',
                    title='VeÃ­culo',
                    sort=ordem_veiculos_barras,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    f'{coluna}:Q',
                    title=coluna,
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato
                    )
                ]
            ).properties(
                height=300,
                width=900
            )
        else:
            # Se nÃ£o tiver VeÃ­culo, usar PerÃ­odo como fallback
            if 'PerÃ­odo' not in df_data.columns:
                return None
            
            # Verificar se hÃ¡ mÃºltiplos anos
            tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
            
            if tem_multiplos_anos:
                # Agrupar por Ano e PerÃ­odo
                # Para CPU, usar EXATAMENTE a mesma lÃ³gica da tabela (que estÃ¡ correta)
                # IMPORTANTE: A tabela funciona porque agrupa df_visualizacao por Ano e PerÃ­odo, soma Total e Volume, e calcula CPU
                # O grÃ¡fico deve fazer EXATAMENTE o mesmo
                if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÃ“GICA DA TABELA (linha 1577-1589): Agrupar por Ano e PerÃ­odo, somar Total e Volume, calcular CPU
                    # Isso garante que valores sejam calculados corretamente, nÃ£o somando CPUs jÃ¡ calculados
                    chart_data = df_data.groupby(['Ano', 'PerÃ­odo']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (EXATAMENTE como a tabela linha 1582-1588)
                    chart_data[coluna] = chart_data.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    chart_data = chart_data[['Ano', 'PerÃ­odo', coluna]]
                else:
                    chart_data = df_data.groupby(['Ano', 'PerÃ­odo'])[coluna].sum().reset_index()
                
                # Criar coluna combinada para o rÃ³tulo do grÃ¡fico
                chart_data['PerÃ­odo_Completo'] = chart_data['PerÃ­odo'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                
                # Ordenar por ano e mÃªs
                chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
                ordem_periodos = chart_data['PerÃ­odo_Completo'].tolist()
                
                # Usar PerÃ­odo_Completo no grÃ¡fico
                coluna_periodo_grafico = 'PerÃ­odo_Completo'
            else:
                # Comportamento original: agrupar apenas por PerÃ­odo
                # Para CPU, recalcular a partir de Total e Volume agregados
                if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    chart_data = df_data.groupby('PerÃ­odo').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU
                    chart_data[coluna] = chart_data.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    chart_data = chart_data[['PerÃ­odo', coluna]]
                else:
                    chart_data = df_data.groupby('PerÃ­odo')[coluna].sum().reset_index()
                chart_data = ordenar_por_mes(chart_data, 'PerÃ­odo')
                ordem_periodos = chart_data['PerÃ­odo'].tolist()
                coluna_periodo_grafico = 'PerÃ­odo'

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    f'{coluna_periodo_grafico}:N',
                    title='PerÃ­odo',
                    sort=ordem_periodos,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    f'{coluna}:Q',
                    title=coluna,
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo_grafico}:N', title='PerÃ­odo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato
                    )
                ]
            ).properties(
                height=300,
                width=900
            )

        # Adicionar rÃ³tulos
        if 'VeÃ­culo' in df_data.columns:
            # Usar a ordem explÃ­cita para garantir sincronizaÃ§Ã£o
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                x=alt.X('VeÃ­culo:N', sort=ordem_veiculos_barras, title='VeÃ­culo'),
                text=alt.Text(f'{coluna}:Q', format=formato)
            )
        else:
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                text=alt.Text(f'{coluna}:Q', format=formato)
            )

        # Criar grÃ¡fico de delta (Real - Flex Bud) se linha_budget estiver disponÃ­vel
        grafico_delta = None
        if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Real - Budget
                delta_data = chart_data.copy()
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_Budget'})
                
                delta_data = delta_data.merge(
                    budget_data_merge[['VeÃ­culo', f'{coluna}_Budget']],
                    on='VeÃ­culo',
                    how='left'
                )
                
                # Calcular delta: Real - Budget
                delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data[f'{coluna}_Budget'].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simÃ©tricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domÃ­nio simÃ©trico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Usar a mesma ordem das barras para manter consistÃªncia
                ordem_veiculos_delta = ordem_veiculos_barras
                
                # Criar grÃ¡fico de barras para delta
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20
                ).encode(
                    x=alt.X(
                        'VeÃ­culo:N',
                        title='',
                        sort=ordem_veiculos_delta,
                        axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)
                    ),
                    y=alt.Y(
                        'Delta:Q',
                        title='Delta (Real - Flex Bud)',
                        axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                    ),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0, delta_max],
                            range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    ),
                    tooltip=[
                        alt.Tooltip('VeÃ­culo:N', title='VeÃ­culo'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Budget)', format=',.2f'),
                        alt.Tooltip(f'{coluna}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna}_Budget:Q', title='Budget', format=',.2f')
                    ]
                ).properties(
                    height=38,
                    width=900
                )
                
                # Adicionar rÃ³tulos de dados no grÃ¡fico de delta
                # Usar a mesma cor das barras (escala baseada no valor do Delta)
                rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('VeÃ­culo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                    align='center',
                    baseline='top',
                    dy=12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('VeÃ­culo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                pass  # Silenciar erro, apenas nÃ£o mostrar delta
        
        # Combinar grÃ¡fico de barras com linha de budget se disponÃ­vel
        if linha_budget is not None:
            grafico_principal = alt.layer(
                grafico_barras,
                rotulos,
                linha_budget
            ).resolve_scale(
                x='shared',
                y='shared'
            )
            
            # Se temos grÃ¡fico de delta, combinar verticalmente (delta em cima)
            if grafico_delta is not None:
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_principal
                ).resolve_scale(
                    x='shared'
                )
            else:
                grafico_final = grafico_principal
        else:
            grafico_final = grafico_barras + rotulos

        return grafico_final
    except Exception as e:
        import traceback
        st.error(f"Erro ao criar grÃ¡fico Total por VeÃ­culo: {e}")
        st.error(traceback.format_exc())
        return None


# ==========================================
# TAB 3: TC Ext por VeÃ­c
# ==========================================
if is_main_page:
    with tab3:
        st.subheader("ðŸš— TC Ext por VeÃ­c")

        # Filtros locais (nÃ£o impactam outras abas)
        filtros_col1, filtros_col2 = st.columns([1, 2])
        with filtros_col1:
            st.markdown("**ðŸ“… Ano**")
            tab3_ano_opcoes = ["Todos"]
            try:
                if 'df_total' in globals() and df_total is not None and 'Ano' in df_total.columns:
                    tab3_ano_opcoes += sorted(pd.to_numeric(df_total['Ano'], errors='coerce').dropna().unique().tolist())
            except Exception:
                pass
            tab3_ano_selecionado = st.selectbox(
                "Ano (Tab 3)",
                options=tab3_ano_opcoes,
                index=0,
                key="tab3_filtro_ano"
                ,
                label_visibility="collapsed"
            )
        with filtros_col2:
            st.markdown("**ðŸ—“ï¸ PerÃ­odo**")
            tab3_periodo_opcoes = ["Todos"]
            try:
                if 'df_total' in globals() and df_total is not None and 'PerÃ­odo' in df_total.columns:
                    periodos = df_total['PerÃ­odo'].dropna().astype(str).unique().tolist()
                    tab3_periodo_opcoes += periodos
            except Exception:
                pass
            tab3_periodos_selecionados = st.multiselect(
                "PerÃ­odo (Tab 3)",
                options=tab3_periodo_opcoes,
                default=["Todos"],
                key="tab3_filtro_periodo"
                ,
                label_visibility="collapsed"
            )

        def _base_real_tab3():
            try:
                if df_filtrado is not None:
                    return df_filtrado
            except NameError:
                pass
            try:
                if df_total is not None:
                    return df_total
            except NameError:
                pass
            return None

        def _base_volume_tab3():
            try:
                if df_vol_filtrado_sidebar is not None:
                    return df_vol_filtrado_sidebar
            except NameError:
                pass
            return None

        def _aplicar_filtros_locais(df_in):
            if df_in is None or df_in.empty:
                return df_in
            df_out = df_in.copy()
            if tab3_ano_selecionado != "Todos" and 'Ano' in df_out.columns:
                df_out = df_out[pd.to_numeric(df_out['Ano'], errors='coerce') == tab3_ano_selecionado].copy()
            if tab3_periodos_selecionados and "Todos" not in tab3_periodos_selecionados and 'PerÃ­odo' in df_out.columns:
                df_out = df_out[df_out['PerÃ­odo'].astype(str).isin([str(x) for x in tab3_periodos_selecionados])].copy()
            return df_out

        def _carregar_budget_filtrado():
            try:
                df_budget = load_budget_data(ano_selecionado)
                df_budget_vol = load_budget_volume_data(ano_selecionado)
            except Exception:
                return None, None

            if df_budget is not None:
                df_budget = df_budget.copy()
                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Total' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Total'] = df_budget['Total'] / 1000
                    elif fator_conversao == "M (MilhÃµes)":
                        df_budget['Total'] = df_budget['Total'] / 1000000
                if moeda_codigo != "BRL" and 'Total' in df_budget.columns:
                    df_budget = _core_converter_coluna_moeda(df_budget, 'Total', moeda_codigo, taxas_cambio)

            if df_budget_vol is not None:
                df_budget_vol = df_budget_vol.copy()

            df_budget = _aplicar_filtros_locais(df_budget)
            df_budget_vol = _aplicar_filtros_locais(df_budget_vol)
            return df_budget, df_budget_vol

        def _flex_por_categoria(df_budget, df_budget_vol, df_real_vol, categoria, tipo_viz):
            if df_budget is None or df_budget_vol is None or df_real_vol is None:
                return None
            if categoria not in df_budget.columns or categoria not in df_budget_vol.columns or categoria not in df_real_vol.columns:
                return None
            if 'Total' not in df_budget.columns or 'Volume' not in df_budget_vol.columns or 'Volume' not in df_real_vol.columns:
                return None
            if 'Custo' not in df_budget.columns:
                return None

            bud = df_budget.copy()
            bud_vol = df_budget_vol.copy()
            real_vol = df_real_vol.copy()

            bud['Total'] = pd.to_numeric(bud['Total'], errors='coerce').fillna(0)
            bud_vol['Volume'] = pd.to_numeric(bud_vol['Volume'], errors='coerce').fillna(0)
            real_vol['Volume'] = pd.to_numeric(real_vol['Volume'], errors='coerce').fillna(0)

            bud[categoria] = bud[categoria].astype(str)
            bud_vol[categoria] = bud_vol[categoria].astype(str)
            real_vol[categoria] = real_vol[categoria].astype(str)

            bud['Custo'] = bud['Custo'].apply(_normalizar_rotulo_custo)
            mask_fixo = _mask_custo_fixo(bud['Custo'])

            bud_total = bud.groupby(categoria, dropna=False)['Total'].sum().reset_index()
            bud_fixo = bud.loc[mask_fixo].groupby(categoria, dropna=False)['Total'].sum().reset_index()
            bud_fixo = bud_fixo.rename(columns={'Total': 'Fixo'})

            bud_base = bud_total.merge(bud_fixo, on=categoria, how='left')
            bud_base['Fixo'] = bud_base['Fixo'].fillna(0)
            bud_base['NaoFixo'] = bud_base['Total'] - bud_base['Fixo']

            vol_budget = bud_vol.groupby(categoria, dropna=False)['Volume'].sum().reset_index().rename(columns={'Volume': 'VolBudget'})
            vol_real = real_vol.groupby(categoria, dropna=False)['Volume'].sum().reset_index().rename(columns={'Volume': 'VolReal'})

            base = bud_base.merge(vol_budget, on=categoria, how='left').merge(vol_real, on=categoria, how='left')
            base['VolBudget'] = base['VolBudget'].fillna(0)
            base['VolReal'] = base['VolReal'].fillna(0)

            def _calc_flex(row):
                if row['VolBudget'] and row['VolBudget'] != 0:
                    return row['Fixo'] + (row['NaoFixo'] * (row['VolReal'] / row['VolBudget']))
                return row['Fixo'] + row['NaoFixo']

            base['Flex'] = base.apply(_calc_flex, axis=1)

            if tipo_viz == "CPU (Custo por Unidade)":
                base['CPU'] = np.where(base['VolReal'] != 0, base['Flex'] / base['VolReal'], 0)
                return base[[categoria, 'CPU']].rename(columns={categoria: categoria})

            return base[[categoria, 'Flex']].rename(columns={'Flex': 'Valor'})

        def _resumo_tab3(base_real, base_vol, df_budget, df_budget_vol, tipo_viz, moeda):
            try:
                if base_real is None or base_real.empty or base_vol is None or base_vol.empty:
                    return None, None
                if df_budget is None or df_budget.empty or df_budget_vol is None or df_budget_vol.empty:
                    return None, None

                real_total = pd.to_numeric(base_real.get('Total', 0), errors='coerce').fillna(0).sum()
                vol_real = pd.to_numeric(base_vol.get('Volume', 0), errors='coerce').fillna(0).sum()

                bud = df_budget.copy()
                bud['Total'] = pd.to_numeric(bud.get('Total', 0), errors='coerce').fillna(0)
                bud['Custo'] = bud.get('Custo', '').apply(_normalizar_rotulo_custo)
                mask_fixo = _mask_custo_fixo(bud['Custo']) if 'Custo' in bud.columns else pd.Series(False, index=bud.index)
                bud_total = bud['Total'].sum()
                bud_fixo = bud.loc[mask_fixo, 'Total'].sum() if 'Custo' in bud.columns else 0
                bud_nao_fixo = bud_total - bud_fixo

                bud_vol_total = pd.to_numeric(df_budget_vol.get('Volume', 0), errors='coerce').fillna(0).sum()

                if bud_vol_total and bud_vol_total != 0:
                    flex_total = bud_fixo + (bud_nao_fixo * (vol_real / bud_vol_total))
                else:
                    flex_total = bud_total

                if tipo_viz == "CPU (Custo por Unidade)":
                    total_val = (real_total / vol_real) if vol_real else 0
                    flex_val = (flex_total / vol_real) if vol_real else 0
                    bud_val = (bud_total / bud_vol_total) if bud_vol_total else 0
                else:
                    total_val = real_total
                    flex_val = flex_total
                    bud_val = bud_total

                linha_resumo = {
                    'BUD': bud_val,
                    'Flex BUD': flex_val,
                    'Total': total_val,
                    'Flex Bud - BUD': flex_val - bud_val,
                    'Total - Flex Bud': total_val - flex_val,
                    'Total / Flex Bud': (total_val / flex_val) if flex_val else 0,
                    '_Volume_Real_Calculo': vol_real,
                    '_Volume_Budget_Calculo': bud_vol_total,
                }

                linha_resumo_formatado = {
                    'BUD': f"{moeda} {_formatar_num_ptbr(bud_val, 2)}",
                    'Flex BUD': f"{moeda} {_formatar_num_ptbr(flex_val, 2)}",
                    'Total': f"{moeda} {_formatar_num_ptbr(total_val, 2)}",
                    'Flex Bud - BUD': f"{moeda} {_formatar_num_ptbr(flex_val - bud_val, 2)}",
                    'Total - Flex Bud': f"{moeda} {_formatar_num_ptbr(total_val - flex_val, 2)}",
                    '_Volume_Real_Calculo': _formatar_num_ptbr(vol_real, 0),
                    '_Volume_Budget_Calculo': _formatar_num_ptbr(bud_vol_total, 0),
                }

                return linha_resumo, linha_resumo_formatado
            except Exception:
                return None, None

        def _formatar_volume_por_categoria(df_in, categoria):
            if df_in is None or df_in.empty:
                return "-"
            if categoria not in df_in.columns or 'Volume' not in df_in.columns:
                return "-"
            df_tmp = df_in[[categoria, 'Volume']].copy()
            df_tmp = df_tmp[df_tmp[categoria].notna()]
            if df_tmp.empty:
                return "-"
            df_tmp['Volume'] = pd.to_numeric(df_tmp['Volume'], errors='coerce').fillna(0)
            agg = (
                df_tmp.groupby(categoria, dropna=False)['Volume']
                .sum()
                .reset_index()
                .sort_values('Volume', ascending=False)
            )
            partes = [f"{row[categoria]}: {_formatar_num_ptbr(row['Volume'], 0)}" for _, row in agg.iterrows()]
            return " | ".join(partes) if partes else "-"

        def _agregar_total(base_df, group_cols, coluna_valor):
            if base_df is None or base_df.empty or coluna_valor not in base_df.columns:
                return pd.DataFrame()
            df_tmp = base_df.copy()
            df_tmp = padronizar_colunas(df_tmp)
            df_tmp[coluna_valor] = pd.to_numeric(df_tmp[coluna_valor], errors='coerce').fillna(0)
            for col in group_cols:
                if col in df_tmp.columns:
                    df_tmp[col] = df_tmp[col].astype(str)
            return (
                df_tmp.groupby(group_cols, dropna=False)[coluna_valor]
                .sum()
                .reset_index()
                .rename(columns={coluna_valor: 'Valor'})
            )

        def _agregar_cpu(base_df, vol_df, group_cols):
            if base_df is None or base_df.empty or 'Total' not in base_df.columns:
                return pd.DataFrame()
            if vol_df is None or vol_df.empty or 'Volume' not in vol_df.columns:
                return pd.DataFrame()

            custo = base_df.copy()
            vol = vol_df.copy()
            custo = padronizar_colunas(custo)
            vol = padronizar_colunas(vol)

            custo['Total'] = pd.to_numeric(custo['Total'], errors='coerce').fillna(0)
            vol['Volume'] = pd.to_numeric(vol['Volume'], errors='coerce').fillna(0)

            for col in group_cols:
                if col in custo.columns:
                    custo[col] = custo[col].astype(str)
                if col in vol.columns:
                    vol[col] = vol[col].astype(str)

            custo_agr = custo.groupby(group_cols, dropna=False)['Total'].sum().reset_index()
            vol_agr = vol.groupby(group_cols, dropna=False)['Volume'].sum().reset_index()

            df_cpu = custo_agr.merge(vol_agr, on=group_cols, how='left')
            df_cpu['CPU'] = np.where(
                (df_cpu['Volume'].notna()) & (df_cpu['Volume'] != 0),
                df_cpu['Total'] / df_cpu['Volume'],
                0
            )
            return df_cpu

        def _plot_rank(df_rank, coluna_valor, titulo, moeda, df_flex_line=None):
            if df_rank is None or df_rank.empty or coluna_valor not in df_rank.columns:
                st.info("â„¹ï¸ Sem dados para o grÃ¡fico com os filtros atuais.")
                return

            df_plot = df_rank.copy()
            df_plot[coluna_valor] = pd.to_numeric(df_plot[coluna_valor], errors='coerce').fillna(0)
            df_plot = df_plot.sort_values(by=coluna_valor, ascending=False)

            eixo_x = df_plot.columns[0]
            valores = df_plot[coluna_valor].tolist()
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=df_plot[eixo_x],
                        y=valores,
                        text=[f"{v:,.2f}" for v in valores],
                        textposition='outside',
                        marker=dict(
                            color=valores,
                            colorscale='Blues',
                            showscale=False
                        ),
                        hovertemplate=(
                            f"%{{x}}<br>{coluna_valor}: %{{y:,.2f}}<extra></extra>"
                        ),
                    )
                ]
            )

            if df_flex_line is not None and not df_flex_line.empty and coluna_valor in df_flex_line.columns:
                df_line = df_flex_line.copy()
                df_line = df_line[df_line[eixo_x].isin(df_plot[eixo_x])]
                df_line = df_line.set_index(eixo_x).reindex(df_plot[eixo_x]).reset_index()
                fig.add_trace(
                    go.Scatter(
                        x=df_line[eixo_x],
                        y=df_line[coluna_valor],
                        mode='lines+markers+text',
                        name='Flex Bud',
                        line=dict(color='#FF6B35', width=2, dash='dash'),
                        marker=dict(size=6),
                        text=[f"{v:,.2f}" for v in df_line[coluna_valor].fillna(0).tolist()],
                        textposition='top center',
                        hovertemplate=(
                            f"%{{x}}<br>Flex Bud: %{{y:,.2f}}<extra></extra>"
                        )
                    )
                )

            fig.update_layout(
                title=titulo,
                xaxis_title=eixo_x,
                yaxis_title=f"{coluna_valor} ({moeda})" if coluna_valor != 'CPU' else coluna_valor,
                margin=dict(l=20, r=20, t=60, b=40),
                height=460,
            )
            fig.update_xaxes(showgrid=False, zeroline=False)
            fig.update_yaxes(showgrid=False, zeroline=False)
            st.plotly_chart(fig, use_container_width=True)

        base_real = _aplicar_filtros_locais(_base_real_tab3())
        base_vol = _aplicar_filtros_locais(_base_volume_tab3())
        df_budget_tab3, df_budget_vol_tab3 = _carregar_budget_filtrado()

        if base_real is None or base_real.empty:
            st.warning("âš ï¸ Sem dados Real para o Tab 3 com os filtros atuais.")
        else:
            moeda_label = moeda_simbolo if 'moeda_simbolo' in locals() else "R$"

            # Resumo (estilo Tab 1)
            linha_resumo_tab3, linha_resumo_tab3_formatado = _resumo_tab3(
                base_real,
                base_vol,
                df_budget_tab3,
                df_budget_vol_tab3,
                tipo_visualizacao,
                moeda_label
            )
            if linha_resumo_tab3 and linha_resumo_tab3_formatado:
                exibir_caixas_resumo(linha_resumo_tab3, linha_resumo_tab3_formatado, tipo_visualizacao, mostrar_volumes=True)
                st.markdown("---")

            # -----------------------------
            # GrÃ¡fico por Oficina
            # -----------------------------
            if 'Oficina' in base_real.columns:
                st.markdown(
                    f"**ðŸ“¦ Volume Real (Oficinas):** {_formatar_volume_por_categoria(base_vol, 'Oficina')}")
                st.markdown(
                    f"**ðŸ“¦ Volume Budget (Oficinas):** {_formatar_volume_por_categoria(df_budget_vol_tab3, 'Oficina')}")
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    df_cpu_of = _agregar_cpu(base_real, base_vol, ['Oficina'])
                    df_flex_of = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_of = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Oficina',
                                "CPU (Custo por Unidade)"
                            )
                        except Exception:
                            df_flex_of = None
                    _plot_rank(df_cpu_of, 'CPU', "ðŸ“Š CPU por Oficina", moeda_label, df_flex_of)
                else:
                    coluna_valor = 'Total' if 'Total' in base_real.columns else 'Valor'
                    df_tot_of = _agregar_total(base_real, ['Oficina'], coluna_valor)
                    df_flex_of = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_of = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Oficina',
                                "Custo Total"
                            )
                        except Exception:
                            df_flex_of = None
                    _plot_rank(df_tot_of, 'Valor', "ðŸ“Š Custo Total por Oficina", moeda_label, df_flex_of)
            else:
                st.info("â„¹ï¸ Coluna 'Oficina' nÃ£o encontrada para o grÃ¡fico por Oficina.")

            # -----------------------------
            # GrÃ¡fico por VeÃ­culo
            # -----------------------------
            if 'VeÃ­culo' in base_real.columns:
                st.markdown(
                    f"**ðŸ“¦ Volume Real (VeÃ­culos):** {_formatar_volume_por_categoria(base_vol, 'VeÃ­culo')}")
                st.markdown(
                    f"**ðŸ“¦ Volume Budget (VeÃ­culos):** {_formatar_volume_por_categoria(df_budget_vol_tab3, 'VeÃ­culo')}")
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    df_cpu_veic = _agregar_cpu(base_real, base_vol, ['VeÃ­culo'])
                    df_flex_veic = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_veic = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'VeÃ­culo',
                                "CPU (Custo por Unidade)"
                            )
                        except Exception:
                            df_flex_veic = None
                    _plot_rank(df_cpu_veic, 'CPU', "ðŸ“Š CPU por VeÃ­culo", moeda_label, df_flex_veic)
                else:
                    coluna_valor = 'Total' if 'Total' in base_real.columns else 'Valor'
                    df_tot_veic = _agregar_total(base_real, ['VeÃ­culo'], coluna_valor)
                    df_flex_veic = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_veic = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'VeÃ­culo',
                                "Custo Total"
                            )
                        except Exception:
                            df_flex_veic = None
                    _plot_rank(df_tot_veic, 'Valor', "ðŸ“Š Custo Total por VeÃ­culo", moeda_label, df_flex_veic)
            else:
                st.info("â„¹ï¸ Coluna 'VeÃ­culo' nÃ£o encontrada para o grÃ¡fico por VeÃ­culo.")

    # VariÃ¡veis necessÃ¡rias para o tab4 (definidas dentro do bloco is_main_page)
    tem_veiculo = 'VeÃ­culo' in df_visualizacao.columns
    tem_oficina = 'Oficina' in df_visualizacao.columns
    tem_periodo = 'PerÃ­odo' in df_visualizacao.columns

    # ==========================================
    # TAB 4: Detalhe Real
    # ==========================================
    with tab4:
        # Expander para mostrar/ocultar todo o bloco de tabelas
        with st.expander("ðŸ“Š **Tabelas Detalhadas**", expanded=False):
            # Tabela: VeÃ­culo, Oficina e PerÃ­odos (seguindo filtros da sidebar)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                st.subheader("ðŸ“‹ Tabela - CPU por VeÃ­culo, Oficina e PerÃ­odo")
            else:
                st.subheader("ðŸ“‹ Tabela - Custo Total por VeÃ­culo, Oficina e PerÃ­odo")
                
            if tem_veiculo and tem_oficina and tem_periodo:
                # ðŸ”§ Garantir variÃ¡veis sempre definidas (evita NameError em caminhos alternativos)
                colunas_periodos = []
                coluna_periodo_pivot = 'PerÃ­odo'
                df_visualizacao_pivot = df_visualizacao.copy()
                df_real_agr_cpu = None

                # Base de volume real jÃ¡ filtrada com a sidebar (nÃ£o deve ser mergeada linha-a-linha no custo)
                df_volume_real_base = None
                if 'df_vol_filtrado_sidebar' in locals() and df_vol_filtrado_sidebar is not None and hasattr(df_vol_filtrado_sidebar, 'columns'):
                    df_volume_real_base = df_vol_filtrado_sidebar.copy()
                    if 'Volume' in df_volume_real_base.columns:
                        df_volume_real_base['Volume'] = pd.to_numeric(df_volume_real_base['Volume'], errors='coerce')

                # Em CPU, NÃƒO fazer merge de Volume linha-a-linha (isso multiplica o volume por linha de custo).
                # O CPU deve ser calculado apÃ³s agregaÃ§Ã£o: sum(Total)/sum(Volume).

                # Usar coluna_visualizacao que jÃ¡ estÃ¡ definida
                if coluna_visualizacao in df_visualizacao.columns:
                    # As variÃ¡veis colunas_periodos, coluna_periodo_pivot e colunas_adicionais
                    # jÃ¡ foram definidas no bloco anterior (tabela de total). Se nÃ£o foram, criar agora.
                    # Definir tem_multiplos_anos antes do try para garantir que estÃ¡ disponÃ­vel
                    tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                    
                    try:
                        # Tentar usar as variÃ¡veis jÃ¡ definidas
                        _ = colunas_periodos
                        _ = coluna_periodo_pivot
                        _ = df_visualizacao_pivot
                        _ = colunas_adicionais
                    except NameError:
                        # Se nÃ£o existirem, criar agora (mesma lÃ³gica)
                        pass
                        
                    if tem_multiplos_anos:
                        df_visualizacao_pivot = df_visualizacao.copy()
                        df_visualizacao_pivot['PerÃ­odo_Ano'] = (
                            df_visualizacao_pivot['PerÃ­odo'].astype(str) + ' ' + 
                            df_visualizacao_pivot['Ano'].astype(str)
                        )
                        coluna_periodo_pivot = 'PerÃ­odo_Ano'
                    else:
                        df_visualizacao_pivot = df_visualizacao.copy()
                        coluna_periodo_pivot = 'PerÃ­odo'
                        
                    if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao_pivot.columns:
                        # Agregar custo e volume separadamente no grÃ£o correto
                        chaves_agr = ['Oficina', 'VeÃ­culo', 'PerÃ­odo']
                        if tem_multiplos_anos and 'Ano' in df_visualizacao_pivot.columns:
                            chaves_agr.append('Ano')

                        df_custo_agr = (
                            df_visualizacao_pivot.groupby(chaves_agr, dropna=False)
                            .agg({'Total': 'sum'})
                            .reset_index()
                        )

                        if (
                            df_volume_real_base is not None
                            and 'Volume' in df_volume_real_base.columns
                            and all(k in df_volume_real_base.columns for k in chaves_agr)
                        ):
                            df_vol_agr = (
                                df_volume_real_base.groupby(chaves_agr, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                        else:
                            df_vol_agr = df_custo_agr[chaves_agr].copy()
                            df_vol_agr['Volume'] = 0

                        # Outer para incluir chaves com volume mesmo sem custo (Total=0)
                        df_real_agr = pd.merge(df_custo_agr, df_vol_agr, on=chaves_agr, how='outer')
                        df_real_agr['Volume'] = pd.to_numeric(df_real_agr['Volume'], errors='coerce').fillna(0)
                        df_real_agr['Total'] = pd.to_numeric(df_real_agr['Total'], errors='coerce').fillna(0)
                        df_real_agr['CPU'] = np.where(
                            (df_real_agr['Volume'].notna()) & (df_real_agr['Volume'] != 0),
                            df_real_agr['Total'] / df_real_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos:
                            df_real_agr['PerÃ­odo_Ano'] = (
                                df_real_agr['PerÃ­odo'].astype(str) + ' ' + df_real_agr['Ano'].astype(str)
                            )
                            col_pivot_cpu = 'PerÃ­odo_Ano'
                        else:
                            col_pivot_cpu = 'PerÃ­odo'

                        df_real_agr_cpu = df_real_agr.copy()

                        df_tabela_ref = df_real_agr.pivot_table(
                            index=['Oficina', 'VeÃ­culo'],
                            columns=col_pivot_cpu,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    elif coluna_visualizacao in df_visualizacao_pivot.columns:
                        df_tabela_ref = df_visualizacao_pivot.pivot_table(
                            index=['Oficina', 'VeÃ­culo'],
                            columns=coluna_periodo_pivot,
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )
                    else:
                        st.warning("âš ï¸ NÃ£o foi possÃ­vel montar a tabela em CPU (colunas Total/Volume ausentes).")
                        df_tabela_ref = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=['Oficina', 'VeÃ­culo']))
                        
                    if tem_multiplos_anos:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
                            
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                coluna_combinada = f"{mes.capitalize()} {ano}"
                                if coluna_combinada in df_tabela_ref.columns:
                                    colunas_ordenadas.append(coluna_combinada)
                            
                        colunas_restantes = [
                            col for col in df_tabela_ref.columns 
                            if col not in colunas_ordenadas
                        ]
                        colunas_periodos = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [col for col in meses_ordem if col in df_tabela_ref.columns]
                        colunas_restantes = [col for col in df_tabela_ref.columns if col not in meses_ordem]
                        colunas_periodos = colunas_existentes + colunas_restantes
                    
                # Definir colunas_adicionais tambÃ©m
                colunas_excluidas = {
                    'Ano', 'PerÃ­odo', 'PerÃ­odo_Ano', 'VeÃ­culo', 'Oficina', 
                    'Total', 'Valor', 'CPU', 'Volume', coluna_visualizacao,
                    'Dt.lÃ§to.', 'Data LanÃ§amento', 'Data de LanÃ§amento',
                    'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
                }
                # Manter a ordem original das colunas do DataFrame
                colunas_adicionais = [
                    col for col in df_visualizacao.columns 
                    if col not in colunas_excluidas
                ]
                
            # Usar as mesmas colunas de perÃ­odos jÃ¡ determinadas
            # Para CPU: calcular SEMPRE a partir de custo agregado + volume agregado (evita volume duplicado)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                if df_real_agr_cpu is None:
                    # Se ainda nÃ£o foi calculado acima (por algum caminho alternativo), calcular aqui
                    chaves_agr = ['Oficina', 'VeÃ­culo', 'PerÃ­odo']
                    tem_multiplos_anos = 'Ano' in df_visualizacao_pivot.columns and df_visualizacao_pivot['Ano'].nunique() > 1
                    if tem_multiplos_anos and 'Ano' in df_visualizacao_pivot.columns:
                        chaves_agr.append('Ano')

                    if 'Total' not in df_visualizacao_pivot.columns:
                        if 'Valor' in df_visualizacao_pivot.columns:
                            df_visualizacao_pivot['Total'] = df_visualizacao_pivot['Valor']
                        elif coluna_visualizacao in df_visualizacao_pivot.columns:
                            df_visualizacao_pivot['Total'] = df_visualizacao_pivot[coluna_visualizacao]

                    df_custo_agr = (
                        df_visualizacao_pivot.groupby(chaves_agr, dropna=False)
                        .agg({'Total': 'sum'})
                        .reset_index()
                    )

                    if (
                        df_volume_real_base is not None
                        and 'Volume' in df_volume_real_base.columns
                        and all(k in df_volume_real_base.columns for k in chaves_agr)
                    ):
                        df_vol_agr = (
                            df_volume_real_base.groupby(chaves_agr, dropna=False)
                            .agg({'Volume': 'sum'})
                            .reset_index()
                        )
                    else:
                        df_vol_agr = df_custo_agr[chaves_agr].copy()
                        df_vol_agr['Volume'] = 0

                    # Outer para incluir chaves com volume mesmo sem custo (Total=0)
                    df_real_agr_cpu = pd.merge(df_custo_agr, df_vol_agr, on=chaves_agr, how='outer')
                    df_real_agr_cpu['Volume'] = pd.to_numeric(df_real_agr_cpu['Volume'], errors='coerce').fillna(0)
                    df_real_agr_cpu['Total'] = pd.to_numeric(df_real_agr_cpu['Total'], errors='coerce').fillna(0)
                    df_real_agr_cpu['CPU'] = np.where(
                        (df_real_agr_cpu['Volume'].notna()) & (df_real_agr_cpu['Volume'] != 0),
                        df_real_agr_cpu['Total'] / df_real_agr_cpu['Volume'],
                        0
                    )

                    if tem_multiplos_anos:
                        df_real_agr_cpu['PerÃ­odo_Ano'] = (
                            df_real_agr_cpu['PerÃ­odo'].astype(str) + ' ' + df_real_agr_cpu['Ano'].astype(str)
                        )
                        coluna_periodo_pivot = 'PerÃ­odo_Ano'
                    else:
                        coluna_periodo_pivot = 'PerÃ­odo'

                df_tabela = df_real_agr_cpu.pivot_table(
                    index=['Oficina', 'VeÃ­culo'],
                    columns=coluna_periodo_pivot,
                    values='CPU',
                    aggfunc='first',
                    fill_value=0
                )
            else:
                # Para Custo Total, usar soma normalmente
                df_tabela = df_visualizacao_pivot.pivot_table(
                    index=['Oficina', 'VeÃ­culo'],
                    columns=coluna_periodo_pivot,
                    values=coluna_visualizacao,
                    aggfunc='sum',
                    fill_value=0
                )

            # Se nÃ£o foi possÃ­vel determinar colunas_periodos acima, usar as colunas da prÃ³pria tabela
            if not colunas_periodos:
                colunas_periodos = list(df_tabela.columns)
            
            # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
            for col in colunas_periodos:
                if col not in df_tabela.columns:
                    df_tabela[col] = 0
                
            # Reordenar para usar exatamente as mesmas colunas
            df_tabela = df_tabela[colunas_periodos]
                
            # Calcular total por linha
            # Em CPU, o TOTAL deve ser ponderado: sum(Total) / sum(Volume) (NUNCA somar CPUs mensais)
            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                df_total_oficina_veiculo = df_real_agr_cpu.groupby(['Oficina', 'VeÃ­culo'], dropna=False).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Calcular CPU - VETORIZADO
                df_total_oficina_veiculo['CPU'] = np.where(
                    (df_total_oficina_veiculo['Volume'].notna()) & (df_total_oficina_veiculo['Volume'] != 0),
                    df_total_oficina_veiculo['Total'] / df_total_oficina_veiculo['Volume'],
                    0
                )
                # Fazer merge com df_tabela para adicionar coluna Total
                df_tabela = df_tabela.reset_index()
                df_tabela = pd.merge(
                    df_tabela,
                    df_total_oficina_veiculo[['Oficina', 'VeÃ­culo', 'CPU']],
                    on=['Oficina', 'VeÃ­culo'],
                    how='left'
                )
                df_tabela.rename(columns={'CPU': 'Total'}, inplace=True)
                df_tabela = df_tabela.set_index(['Oficina', 'VeÃ­culo'])
            elif tipo_visualizacao != "CPU (Custo por Unidade)":
                df_tabela['Total'] = df_tabela.sum(axis=1)
            else:
                # Sem base agregada, nÃ£o dÃ¡ para calcular TOTAL corretamente em CPU.
                # Evitar soma de CPUs mensais (que distorce o valor).
                df_tabela['Total'] = 0
            try:
                df_tabela = df_tabela.sort_values(['Oficina', 'VeÃ­culo'])
            except KeyError:
                if isinstance(df_tabela.index, pd.MultiIndex):
                    df_tabela = df_tabela.sort_index()
                else:
                    if 'Oficina' not in df_tabela.columns:
                        df_tabela['Oficina'] = pd.NA
                    if 'VeÃ­culo' not in df_tabela.columns:
                        df_tabela['VeÃ­culo'] = pd.NA
                    df_tabela = df_tabela.sort_values(['Oficina', 'VeÃ­culo'])
                
            # Resetar Ã­ndice para ter Oficina e VeÃ­culo como colunas (Oficina primeiro)
            df_tabela = df_tabela.reset_index()
            
            # Adicionar colunas adicionais fazendo merge com o primeiro valor nÃ£o nulo por Oficina e VeÃ­culo
            if colunas_adicionais:
                # Filtrar apenas colunas que realmente existem no DataFrame
                colunas_adicionais_validas = [
                    col for col in colunas_adicionais 
                    if col in df_visualizacao.columns
                ]
                    
                if colunas_adicionais_validas:
                    # Agrupar por Oficina e VeÃ­culo e pegar o primeiro valor nÃ£o nulo de cada coluna adicional
                    # Usar df_visualizacao original para ter todas as colunas
                    df_colunas_adicionais = df_visualizacao.groupby(['Oficina', 'VeÃ­culo'])[colunas_adicionais_validas].first().reset_index()
                    # Fazer merge com a tabela
                    df_tabela = pd.merge(
                        df_tabela,
                        df_colunas_adicionais,
                        on=['Oficina', 'VeÃ­culo'],
                        how='left'
                    )
                    # Reordenar colunas: Oficina, VeÃ­culo, colunas adicionais (na ordem original), perÃ­odos, Total
                    # Manter a ordem original das colunas adicionais
                    colunas_adicionais_ordenadas = [
                        col for col in colunas_adicionais 
                        if col in colunas_adicionais_validas
                    ]
                    colunas_finais = ['Oficina', 'VeÃ­culo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Total']
                    # Manter apenas colunas que existem
                    colunas_finais = [col for col in colunas_finais if col in df_tabela.columns]
                    df_tabela = df_tabela[colunas_finais]
            else:
                # Reordenar colunas para garantir que Oficina venha antes de VeÃ­culo
                colunas_ordenadas = ['Oficina', 'VeÃ­culo'] + [col for col in df_tabela.columns 
                                                              if col not in ['Oficina', 'VeÃ­culo']]
                df_tabela = df_tabela[colunas_ordenadas]
                
            # Formatar valores baseado no tipo de visualizaÃ§Ã£o - OTIMIZADO
            # Aplicar formataÃ§Ã£o apenas nas colunas numÃ©ricas (exceto VeÃ­culo, Oficina e colunas adicionais)
            df_tabela_formatado = df_tabela.copy()
            # Obter colunas adicionais que foram realmente adicionadas Ã  tabela
            colunas_adicionais_na_tabela = [
                col for col in df_tabela_formatado.columns 
                if col not in ['Oficina', 'VeÃ­culo'] + colunas_periodos + ['Total']
            ]
            colunas_formatar = [
                col for col in df_tabela_formatado.columns 
                if col not in ['VeÃ­culo', 'Oficina'] + colunas_adicionais_na_tabela and
                df_tabela_formatado[col].dtype in ['float64', 'float32', 'int64', 'int32']
            ]
            # FormataÃ§Ã£o vetorizada
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                for col in colunas_formatar:
                    df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
            else:
                # Adicionar sufixo baseado no fator de conversÃ£o
                sufixo = ""
                if fator_conversao:
                    if fator_conversao == "K (milhares)":
                        sufixo = " K"
                    elif fator_conversao == "M (MilhÃµes)":
                        sufixo = " M"
                for col in colunas_formatar:
                    df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}{sufixo}" if isinstance(x, (int, float)) else x)
            
            # FunÃ§Ã£o para formatar valores (definida antes de ser usada)
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    # NOTA: Os dados jÃ¡ estÃ£o convertidos na base, entÃ£o apenas formatamos
                    simbolo = obter_simbolo_moeda(moeda_codigo)
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        # Adicionar sufixo baseado no fator de conversÃ£o (apenas para Custo Total)
                        sufixo = ""
                        if tipo_visualizacao == "Custo Total" and fator_conversao:
                            if fator_conversao == "K (milhares)":
                                sufixo = " K"
                            elif fator_conversao == "M (MilhÃµes)":
                                sufixo = " M"
                        return f"{simbolo} {val:,.2f}{sufixo}"
                return val
            
            # Agrupar por Oficina e renderizar blocos
            oficinas = df_tabela_formatado['Oficina'].unique()

            if len(oficinas) == 0:
                st.info("Nenhum dado encontrado para exibir por Oficina.")
            else:
                for oficina in sorted(oficinas):
                    # Filtrar dados da oficina
                    df_oficina = df_tabela_formatado[df_tabela_formatado['Oficina'] == oficina].copy()

                    # Calcular total da oficina
                    if 'Total' in df_oficina.columns:
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                            df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                            total_custo_of = float(pd.to_numeric(df_base_of['Total'], errors='coerce').fillna(0).sum())
                            vol_of = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                            total_oficina = (total_custo_of / vol_of) if vol_of not in (0, None) else 0.0
                        else:
                            df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                            total_oficina = float(pd.to_numeric(df_oficina_numerico['Total'], errors='coerce').fillna(0).sum())
                        total_formatado = formatar_valor(total_oficina, tipo_visualizacao)
                    else:
                        total_formatado = "N/A"

                    # Criar container para cada oficina (substituindo expander para evitar aninhamento)
                    with st.container():
                        st.markdown(
                            f"### ðŸ­ **{oficina}** - Total: {total_formatado} ("
                            f"{len(df_oficina)} veÃ­culo{'s' if len(df_oficina) > 1 else ''})"
                        )

                        # Em CPU, o TOTAL por mÃªs Ã© ponderado por Volume (Total/Volume).
                        # Se o volume muda entre meses, o TOTAL pode mudar mesmo com CPUs por veÃ­culo parecidas.
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                            with st.expander("ðŸ“¦ Volume por perÃ­odo (explica variaÃ§Ãµes do TOTAL em CPU)", expanded=False):
                                df_tmp_vol = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                col_per = (
                                    'PerÃ­odo_Ano'
                                    if coluna_periodo_pivot == 'PerÃ­odo_Ano' and 'PerÃ­odo_Ano' in df_tmp_vol.columns
                                    else 'PerÃ­odo'
                                )
                                vol_por_periodo = (
                                    df_tmp_vol.groupby(col_per, dropna=False)['Volume']
                                    .sum()
                                    .astype(float)
                                )

                                # Ordenar colunas no mesmo padrÃ£o exibido na tabela
                                ordem_cols = [c for c in colunas_periodos if c in vol_por_periodo.index]
                                extras = [c for c in vol_por_periodo.index if c not in set(ordem_cols)]
                                vol_por_periodo = vol_por_periodo.reindex(ordem_cols + extras, fill_value=0.0)

                                st.dataframe(pd.DataFrame([vol_por_periodo]), width="stretch")
                                st.caption(
                                    "No modo CPU, o TOTAL do mÃªs Ã© calculado como sum(Total)/sum(Volume). "
                                    "Se o volume muda entre meses, o TOTAL pode mudar mesmo com CPUs por veÃ­culo iguais apÃ³s arredondamento."
                                )

                        # Remover coluna Oficina da tabela (jÃ¡ estÃ¡ no tÃ­tulo)
                        df_oficina_display = df_oficina.drop(columns=['Oficina'])

                        # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
                        colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                        for col in colunas_para_remover:
                            if col in df_oficina_display.columns:
                                df_oficina_display = df_oficina_display.drop(columns=[col])

                        # Calcular totais por coluna (meses) usando dados numÃ©ricos
                        df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                        df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])

                        # Criar linha de total
                        linha_total = {'VeÃ­culo': '**TOTAL**'}

                        # Obter colunas adicionais que foram realmente adicionadas Ã  tabela
                        colunas_adicionais_na_tabela = [
                            col for col in df_oficina_numerico.columns
                            if col not in ['VeÃ­culo'] + colunas_periodos + ['Total']
                        ]

                        # Adicionar valores vazios para colunas adicionais na linha de total
                        for col in colunas_adicionais_na_tabela:
                            if col in df_oficina_numerico.columns:
                                linha_total[col] = pd.NA

                        # Adicionar totais por coluna (meses e Total)
                        for col in df_oficina_numerico.columns:
                            if col not in ['VeÃ­culo'] + colunas_adicionais_na_tabela:
                                if col in colunas_periodos:
                                    # Para colunas de perÃ­odo, se for CPU, calcular Total/Volume do perÃ­odo
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                        df_tmp = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                        if coluna_periodo_pivot == 'PerÃ­odo_Ano' and 'PerÃ­odo_Ano' in df_tmp.columns:
                                            df_tmp_p = df_tmp[df_tmp['PerÃ­odo_Ano'] == col]
                                        else:
                                            df_tmp_p = df_tmp[df_tmp['PerÃ­odo'] == col]

                                        total_periodo = float(pd.to_numeric(df_tmp_p['Total'], errors='coerce').fillna(0).sum())
                                        volume_periodo = float(pd.to_numeric(df_tmp_p['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_periodo = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0
                                        linha_total[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        # Para Custo Total, somar normalmente
                                        if df_oficina_numerico[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                                            total_col = df_oficina_numerico[col].sum()
                                            linha_total[col] = formatar_valor(total_col, tipo_visualizacao)
                                elif col == 'Total':
                                    # Para a coluna Total, se for CPU, calcular Total/Volume geral da oficina
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                        df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                        total_geral = float(pd.to_numeric(df_base_of['Total'], errors='coerce').fillna(0).sum())
                                        volume_geral = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
                                        linha_total[col] = formatar_valor(cpu_geral, tipo_visualizacao)
                                    else:
                                        # Para Custo Total, somar normalmente
                                        if df_oficina_numerico[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                                            total_col = df_oficina_numerico[col].sum()
                                            linha_total[col] = formatar_valor(total_col, tipo_visualizacao)

                        # Adicionar linha de total ao DataFrame
                        df_oficina_display = pd.concat([
                            df_oficina_display,
                            pd.DataFrame([linha_total])
                        ], ignore_index=True)

                        st.dataframe(df_oficina_display, width="stretch")
            
            # BotÃ£o de download da tabela (dentro do expander, fora do loop)
            if st.button(
                "ðŸ“¥ Baixar Tabela por VeÃ­culo e Oficina (Excel)",
                width="stretch",
                key="download_tabela_veiculo_oficina"
            ):
                with st.spinner("Gerando arquivo da tabela..."):
                    try:
                        # Criar DataFrame completo para download (com todas as oficinas e totais)
                        df_download_list = []
                            
                        for oficina in sorted(oficinas):
                            # Dados da oficina (sem formataÃ§Ã£o para manter valores numÃ©ricos)
                            df_oficina_download = df_tabela[df_tabela['Oficina'] == oficina].copy()
                                
                            # Adicionar linha de total da oficina
                            linha_total_download = {'Oficina': oficina, 'VeÃ­culo': 'TOTAL'}
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                for col in colunas_periodos:
                                    if col in df_oficina_download.columns:
                                        if coluna_periodo_pivot == 'PerÃ­odo_Ano' and 'PerÃ­odo_Ano' in df_base_of.columns:
                                            df_p = df_base_of[df_base_of['PerÃ­odo_Ano'] == col]
                                        else:
                                            df_p = df_base_of[df_base_of['PerÃ­odo'] == col]

                                        total_p = float(pd.to_numeric(df_p['Total'], errors='coerce').fillna(0).sum())
                                        vol_p = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                                        linha_total_download[col] = (total_p / vol_p) if vol_p not in (0, None) else 0.0

                                if 'Total' in df_oficina_download.columns:
                                    total_geral = float(pd.to_numeric(df_base_of['Total'], errors='coerce').fillna(0).sum())
                                    vol_geral = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                                    linha_total_download['Total'] = (total_geral / vol_geral) if vol_geral not in (0, None) else 0.0
                            else:
                                df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                                df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])
                                for col in df_oficina_numerico.columns:
                                    if col != 'VeÃ­culo':
                                        total_col = df_oficina_numerico[col].sum()
                                        linha_total_download[col] = total_col
                            
                            # Adicionar dados da oficina
                            df_download_list.append(df_oficina_download)
                            # Adicionar linha de total
                            df_download_list.append(pd.DataFrame([linha_total_download]))
                            
                        # Concatenar todos os DataFrames
                        df_download = pd.concat(df_download_list, ignore_index=True)
                            
                        # Obter pasta Downloads do usuÃ¡rio
                        downloads_path = os.path.join(
                            os.path.expanduser("~"), "Downloads"
                        )
                        tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                        file_name = f"TC_Ext_tabela_veiculo_oficina_{tipo_nome}.xlsx"
                        file_path = os.path.join(downloads_path, file_name)
                            
                        # Salvar arquivo diretamente na pasta Downloads
                        with pd.ExcelWriter(
                            file_path, engine='openpyxl'
                        ) as writer:
                            df_download.to_excel(
                                writer, index=False, sheet_name='Veiculo_Oficina'
                            )
                            
                        st.success(
                            f"âœ… Arquivo salvo com sucesso em: {file_path}"
                        )
                        st.info(
                            f"ðŸ“ Verifique sua pasta Downloads: {downloads_path}"
                        )
                    except Exception as e:
                        st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")
            else:
                colunas_faltando = []
                if not tem_veiculo:
                    colunas_faltando.append("VeÃ­culo")
                if not tem_oficina:
                    colunas_faltando.append("Oficina")
                if not tem_periodo:
                    colunas_faltando.append("PerÃ­odo")
                st.info(f"â„¹ï¸ Colunas necessÃ¡rias nÃ£o encontradas para criar a tabela: {', '.join(colunas_faltando)}")
            
            # Tabela dinÃ¢mica: Valor por Oficina e PerÃ­odo
            if ('Oficina' in df_visualizacao.columns and
                    'PerÃ­odo' in df_visualizacao.columns):
                # Determinar tÃ­tulo
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.subheader("ðŸ“‹ Tabela DinÃ¢mica - CPU por Oficina e PerÃ­odo")
                else:
                    st.subheader("ðŸ“‹ Tabela DinÃ¢mica - Valor por Oficina e PerÃ­odo")
                
                if coluna_visualizacao in df_visualizacao.columns:
                    # Verificar se hÃ¡ mÃºltiplos anos e criar coluna combinada se necessÃ¡rio
                    tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                    
                    if tem_multiplos_anos:
                        # Criar coluna combinada PerÃ­odo + Ano para separar meses por ano
                        df_visualizacao_pivot = df_visualizacao.copy()
                        df_visualizacao_pivot['PerÃ­odo_Ano'] = (
                            df_visualizacao_pivot['PerÃ­odo'].astype(str) + ' ' + 
                            df_visualizacao_pivot['Ano'].astype(str)
                        )
                        
                        # Criar tabela pivot
                        df_pivot = df_visualizacao_pivot.pivot_table(
                            index='Oficina',
                            columns='PerÃ­odo_Ano',
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )
                        
                        # Ordenar colunas por ano e mÃªs
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
                        
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                coluna_combinada = f"{mes.capitalize()} {ano}"
                                if coluna_combinada in df_pivot.columns:
                                    colunas_ordenadas.append(coluna_combinada)
                        
                        # Adicionar colunas que nÃ£o sÃ£o meses (ex: Total, outros perÃ­odos)
                        colunas_restantes = [
                            col for col in df_pivot.columns 
                            if col not in colunas_ordenadas
                        ]
                        df_pivot = df_pivot[colunas_ordenadas + colunas_restantes]
                    else:
                        # Criar tabela pivot
                        df_pivot = df_visualizacao.pivot_table(
                            index='Oficina',
                            columns='PerÃ­odo',
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )

                        # Ordenar colunas por ordem cronolÃ³gica dos meses
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [col for col in meses_ordem if col in df_pivot.columns]
                        colunas_restantes = [col for col in df_pivot.columns if col not in meses_ordem]
                        df_pivot = df_pivot[colunas_existentes + colunas_restantes]

                    # Calcular total por linha
                    # Regra crÃ­tica (documentaÃ§Ã£o): em CPU, o total deve ser ponderado por volume.
                    # Importante: NÃƒO usar Volume mergeado no df_visualizacao (duplica volume por linha de custo).
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_visualizacao_cpu = df_visualizacao.copy()
                        if 'Total' not in df_visualizacao_cpu.columns:
                            if 'Valor' in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Total'] = df_visualizacao_cpu['Valor']
                            elif 'Custo' in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Total'] = df_visualizacao_cpu['Custo']
                            elif coluna_visualizacao in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Total'] = df_visualizacao_cpu[coluna_visualizacao]

                        chaves_cpu = ['Oficina', 'PerÃ­odo']
                        if tem_multiplos_anos and 'Ano' in df_visualizacao_cpu.columns:
                            chaves_cpu.append('Ano')

                        df_custo_agr = (
                            df_visualizacao_cpu.groupby(chaves_cpu, dropna=False)
                            .agg({'Total': 'sum'})
                            .reset_index()
                        )

                        df_volume_base = df_vol_filtrado_sidebar.copy() if 'df_vol_filtrado_sidebar' in locals() else pd.DataFrame()
                        if (not df_volume_base.empty) and 'Volume' in df_volume_base.columns and all(k in df_volume_base.columns for k in chaves_cpu):
                            df_vol_agr = (
                                df_volume_base.groupby(chaves_cpu, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                        else:
                            df_vol_agr = df_custo_agr[chaves_cpu].copy()
                            df_vol_agr['Volume'] = 0

                        # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: incluir chaves com volume mesmo sem custo (Total=0)
                        df_cpu_agr = pd.merge(df_custo_agr, df_vol_agr, on=chaves_cpu, how='outer')
                        df_cpu_agr['Volume'] = pd.to_numeric(df_cpu_agr['Volume'], errors='coerce').fillna(0)
                        df_cpu_agr['Total'] = pd.to_numeric(df_cpu_agr['Total'], errors='coerce').fillna(0)
                        df_cpu_agr['CPU'] = np.where(
                            (df_cpu_agr['Volume'].notna()) & (df_cpu_agr['Volume'] != 0),
                            df_cpu_agr['Total'] / df_cpu_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos and 'Ano' in df_cpu_agr.columns:
                            df_cpu_agr['PerÃ­odo_Ano'] = df_cpu_agr['PerÃ­odo'].astype(str) + ' ' + df_cpu_agr['Ano'].astype(str)
                            col_periodo_cpu = 'PerÃ­odo_Ano'
                        else:
                            col_periodo_cpu = 'PerÃ­odo'

                        df_pivot = df_cpu_agr.pivot_table(
                            index='Oficina',
                            columns=col_periodo_cpu,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )

                        # OrdenaÃ§Ã£o de colunas (cronolÃ³gica)
                        if tem_multiplos_anos and 'Ano' in df_cpu_agr.columns:
                            colunas_ordenadas = []
                            anos_unicos = sorted(df_cpu_agr['Ano'].unique())
                            for ano in anos_unicos:
                                for mes in ORDEM_MESES:
                                    coluna_combinada = f"{mes.capitalize()} {ano}"
                                    if coluna_combinada in df_pivot.columns:
                                        colunas_ordenadas.append(coluna_combinada)
                            colunas_restantes = [col for col in df_pivot.columns if col not in colunas_ordenadas]
                            df_pivot = df_pivot[colunas_ordenadas + colunas_restantes]
                        else:
                            meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                            colunas_existentes = [col for col in meses_ordem if col in df_pivot.columns]
                            colunas_restantes = [col for col in df_pivot.columns if col not in meses_ordem]
                            df_pivot = df_pivot[colunas_existentes + colunas_restantes]

                        # Evitar colisÃ£o quando existe um PerÃ­odo chamado "Total".
                        # Se a pivot tiver uma coluna "Total" vinda do PerÃ­odo, ela conflita com a coluna de total geral.
                        if 'Total' in df_pivot.columns:
                            df_pivot = df_pivot.rename(columns={'Total': 'Total (PerÃ­odo)'})

                        # Total ponderado por volume por Oficina (nÃ£o somar CPUs)
                        df_total_oficina = df_cpu_agr.groupby('Oficina', dropna=False).agg({'Total': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_oficina['CPU_Total'] = np.where(
                            (df_total_oficina['Volume'].notna()) & (df_total_oficina['Volume'] != 0),
                            df_total_oficina['Total'] / df_total_oficina['Volume'],
                            0
                        )
                        df_pivot['Total'] = df_pivot.index.to_series().map(
                            df_total_oficina.set_index('Oficina')['CPU_Total']
                        ).fillna(0)
                    else:
                        # Evitar colisÃ£o quando existe um PerÃ­odo chamado "Total".
                        if 'Total' in df_pivot.columns:
                            df_pivot = df_pivot.rename(columns={'Total': 'Total (PerÃ­odo)'})

                        df_pivot['Total'] = df_pivot.sum(axis=1)

                    df_pivot = df_pivot.sort_values('Total', ascending=False)

                    # Formatar valores baseado no tipo de visualizaÃ§Ã£o
                    def formatar_valor(val, tipo):
                        if isinstance(val, (int, float)):
                            if tipo == "CPU (Custo por Unidade)":
                                return f"{val:,.2f}"
                            else:
                                return f"R$ {val:,.2f}"
                        return val

                    # Aplicar formataÃ§Ã£o
                    df_pivot_formatado = df_pivot.copy()
                    for col in df_pivot_formatado.columns:
                        df_pivot_formatado[col] = df_pivot_formatado[col].apply(
                            lambda x: formatar_valor(x, tipo_visualizacao)
                        )

                    # Adicionar linha de somatÃ³rio (TOTAL)
                    try:
                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                            # Total ponderado por Volume: CPU = sum(Total)/sum(Volume)
                            if 'df_cpu_agr' in locals() and df_cpu_agr is not None and not df_cpu_agr.empty and 'Total' in df_cpu_agr.columns and 'Volume' in df_cpu_agr.columns:
                                df_tot_periodo = df_cpu_agr.groupby(col_periodo_cpu, dropna=False).agg({'Total': 'sum', 'Volume': 'sum'}).reset_index()
                                df_tot_periodo['CPU_TOTAL'] = np.where(
                                    (df_tot_periodo['Volume'].notna()) & (df_tot_periodo['Volume'] != 0),
                                    df_tot_periodo['Total'] / df_tot_periodo['Volume'],
                                    0
                                )
                                mapa_tot = df_tot_periodo.set_index(col_periodo_cpu)['CPU_TOTAL'].to_dict()

                                total_geral = float(pd.to_numeric(df_cpu_agr['Total'], errors='coerce').fillna(0).sum())
                                volume_geral = float(pd.to_numeric(df_cpu_agr['Volume'], errors='coerce').fillna(0).sum())
                                cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0

                                linha_total_fmt = {c: '' for c in df_pivot_formatado.columns}
                                for c in df_pivot_formatado.columns:
                                    if c == 'Total':
                                        linha_total_fmt[c] = formatar_valor(cpu_geral, tipo_visualizacao)
                                    elif c in mapa_tot:
                                        linha_total_fmt[c] = formatar_valor(float(mapa_tot.get(c, 0) or 0), tipo_visualizacao)
                                df_pivot_formatado = pd.concat(
                                    [df_pivot_formatado, pd.DataFrame([linha_total_fmt], index=['**TOTAL**'])]
                                )
                        else:
                            # Custo Total: somar colunas
                            linha_total = df_pivot.sum(axis=0, numeric_only=True)
                            linha_total_fmt = {}
                            for c in df_pivot_formatado.columns:
                                if c in linha_total.index:
                                    linha_total_fmt[c] = formatar_valor(float(linha_total[c]), tipo_visualizacao)
                                else:
                                    linha_total_fmt[c] = ''
                            df_pivot_formatado = pd.concat(
                                [df_pivot_formatado, pd.DataFrame([linha_total_fmt], index=['**TOTAL**'])]
                            )
                    except Exception:
                        # Se falhar, apenas nÃ£o exibe o total (nÃ£o quebrar a tela)
                        pass
                    
                    # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
                    colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                    for col in colunas_para_remover:
                        if col in df_pivot_formatado.columns:
                            df_pivot_formatado = df_pivot_formatado.drop(columns=[col])

                    st.dataframe(df_pivot_formatado, width="stretch")

                    # BotÃ£o de download da Tabela DinÃ¢mica
                    if st.button(
                        "ðŸ“¥ Baixar Tabela DinÃ¢mica (Excel)",
                        width="stretch",
                        key="download_pivot"
                    ):
                        with st.spinner("Gerando arquivo da tabela dinÃ¢mica..."):
                            try:
                                # Obter pasta Downloads do usuÃ¡rio
                                downloads_path = os.path.join(
                                    os.path.expanduser("~"), "Downloads"
                                )
                                file_name = "TC_Ext_tabela_dinamica.xlsx"
                                file_path = os.path.join(downloads_path, file_name)

                                # Salvar arquivo diretamente na pasta Downloads
                                with pd.ExcelWriter(
                                    file_path, engine='openpyxl'
                                ) as writer:
                                    df_pivot.to_excel(
                                        writer, index=True, sheet_name='Tabela_Dinamica'
                                    )

                                st.success(
                                    f"âœ… Arquivo salvo com sucesso em: {file_path}"
                                )
                                st.info(
                                    f"ðŸ“ Verifique sua pasta Downloads: {downloads_path}"
                                )
                            except Exception as e:
                                st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")
        
        # Tabela: Total por VeÃ­culo e PerÃ­odos (sem Oficina) - no final do bloco
        # Determinar tÃ­tulo do expander
        # ATUALIZADO: Usando mesma lÃ³gica do grÃ¡fico para linha de total geral
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            titulo_expander_total = "ðŸ“‹ **Tabela - CPU Total por VeÃ­culo e PerÃ­odo**"
        else:
            titulo_expander_total = "ðŸ“‹ **Tabela - Custo Total por VeÃ­culo e PerÃ­odo**"
            
        # Usar expander no mesmo formato do expander de "Tabelas Detalhadas"
        with st.expander(titulo_expander_total, expanded=False):
            if tem_veiculo and tem_periodo:
                # ðŸ”§ Para CPU: garantir insumos no dataframe-base (df_visualizacao)
                # A tabela total usa df_visualizacao para agrupar/calcular CPU; se Volume/Total nÃ£o estiverem aqui,
                # ela cai em fallbacks e pode renderizar tudo como None.
                df_visualizacao_total = df_visualizacao.copy()
                df_agrupado_periodo = None
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    if 'Total' not in df_visualizacao_total.columns:
                        if 'Valor' in df_visualizacao_total.columns:
                            df_visualizacao_total['Total'] = df_visualizacao_total['Valor']
                        elif 'Custo' in df_visualizacao_total.columns:
                            df_visualizacao_total['Total'] = df_visualizacao_total['Custo']
                        elif coluna_visualizacao in df_visualizacao_total.columns:
                            df_visualizacao_total['Total'] = df_visualizacao_total[coluna_visualizacao]

                    if 'Total' in df_visualizacao_total.columns:
                        df_visualizacao_total['Total'] = pd.to_numeric(
                            df_visualizacao_total['Total'], errors='coerce'
                        )

                # Em CPU, NUNCA usar Volume mergeado em df_visualizacao_total (duplica volume por linha de custo).

                # Em CPU, evitamos merge de volume linha-a-linha para nÃ£o inflar o denominador.
                # Inicializar variÃ¡veis para CPU
                df_tabela_total_valores = None
                df_tabela_total_volumes = None
                
                # Para CPU, usar a mesma lÃ³gica do grÃ¡fico: agrupar diretamente por VeÃ­culo e PerÃ­odo+Ano
                # Isso garante que apenas perÃ­odos com dados sejam considerados (evita problemas com volumes sem custos)
                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao_total.columns:
                    # Verificar se hÃ¡ mÃºltiplos anos
                    tem_multiplos_anos = 'Ano' in df_visualizacao_total.columns and df_visualizacao_total['Ano'].nunique() > 1

                    # CPU deve ser calculado por (Total agregado) / (Volume agregado).
                    # Volume real frequentemente vem no grÃ£o Oficina+VeÃ­culo+PerÃ­odo; para a tabela TOTAL por veÃ­culo,
                    # somamos volume sobre oficinas antes do merge.
                    chaves_merge = ['VeÃ­culo', 'PerÃ­odo']
                    if tem_multiplos_anos and 'Ano' in df_visualizacao_total.columns:
                        chaves_merge.append('Ano')

                    df_custo_agr = (
                        df_visualizacao_total.groupby(chaves_merge, dropna=False)
                        .agg({'Total': 'sum'})
                        .reset_index()
                    )

                    df_volume_base = df_vol_filtrado_sidebar.copy() if 'df_vol_filtrado_sidebar' in locals() else pd.DataFrame()
                    if not df_volume_base.empty and 'Volume' in df_volume_base.columns and all(k in df_volume_base.columns for k in chaves_merge):
                        df_volume_agr = (
                            df_volume_base.groupby(chaves_merge, dropna=False)
                            .agg({'Volume': 'sum'})
                            .reset_index()
                        )
                    else:
                        # Sem volume vÃ¡lido no grÃ£o necessÃ¡rio -> cria volume 0 para evitar tabela nula/None
                        df_volume_agr = df_custo_agr[chaves_merge].copy()
                        df_volume_agr['Volume'] = 0

                    # ðŸ”§ CORREÃ‡ÃƒO CRÃTICA: incluir veÃ­culos/perÃ­odos com volume mesmo sem custo (custo=0)
                    df_agrupado_periodo = pd.merge(
                        df_custo_agr,
                        df_volume_agr,
                        on=chaves_merge,
                        how='outer'
                    )
                    df_agrupado_periodo['Volume'] = pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0)
                    df_agrupado_periodo['Total'] = pd.to_numeric(df_agrupado_periodo['Total'], errors='coerce').fillna(0)
                        
                    # Agrupar por VeÃ­culo e PerÃ­odo+Ano, somar Total e Volume, calcular CPU
                    # Usar a mesma coluna_periodo_pivot que foi determinada anteriormente
                    if tem_multiplos_anos:
                        # Criar coluna PerÃ­odo_Ano para fazer o pivot (usar o mesmo formato)
                        df_agrupado_periodo[coluna_periodo_pivot] = (
                            df_agrupado_periodo['PerÃ­odo'].astype(str) + ' ' + 
                            df_agrupado_periodo['Ano'].astype(str)
                        )
                    else:
                        # Sem mÃºltiplos anos: coluna_periodo_pivot permanece 'PerÃ­odo'
                        pass
                        
                    # Calcular CPU por perÃ­odo (mesma lÃ³gica do grÃ¡fico) - vetorizado
                    df_agrupado_periodo['CPU'] = np.where(
                        (df_agrupado_periodo['Volume'].notna()) & (df_agrupado_periodo['Volume'] != 0),
                        df_agrupado_periodo['Total'] / df_agrupado_periodo['Volume'],
                        0
                    )
                        
                    # Criar tabelas pivot de Total e Volume apenas com dados existentes
                    # Usar coluna_periodo_pivot que jÃ¡ foi determinada
                    df_tabela_total_valores = df_agrupado_periodo.pivot_table(
                        index='VeÃ­culo',
                        columns=coluna_periodo_pivot,
                        values='Total',
                        aggfunc='sum',
                        fill_value=0
                    )
                        
                    df_tabela_total_volumes = df_agrupado_periodo.pivot_table(
                        index='VeÃ­culo',
                        columns=coluna_periodo_pivot,
                        values='Volume',
                        aggfunc='sum',
                        fill_value=0
                    )
                        
                    # Dividir Total / Volume para obter CPU
                    df_tabela_total = df_tabela_total_valores / df_tabela_total_volumes.replace(0, np.nan)
                    df_tabela_total = df_tabela_total.fillna(0)

                    # Garantir nome do Ã­ndice para reset_index consistente
                    df_tabela_total.index.name = 'VeÃ­culo'
                        
                    # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                    for col in colunas_periodos:
                        if col not in df_tabela_total.columns:
                            df_tabela_total[col] = 0
                        
                    # Reordenar para usar exatamente as mesmas colunas
                    df_tabela_total = df_tabela_total[colunas_periodos]
                        
                    # Calcular total por linha: usar EXATAMENTE a mesma lÃ³gica do grÃ¡fico "CPU por VeÃ­culo"
                    # Primeiro agrupar por VeÃ­culo e PerÃ­odo+Ano, depois por VeÃ­culo
                    if tem_multiplos_anos:
                        df_total_veiculo = df_agrupado_periodo.groupby('VeÃ­culo', dropna=False).agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        df_total_veiculo = df_agrupado_periodo.groupby('VeÃ­culo', dropna=False).agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        
                    # Recalcular CPU (mesma lÃ³gica do grÃ¡fico linha 2080)
                    df_total_veiculo['CPU'] = df_total_veiculo.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Fazer merge com df_tabela_total para adicionar coluna Total
                    df_tabela_total = df_tabela_total.reset_index()
                    df_tabela_total = pd.merge(
                        df_tabela_total,
                        df_total_veiculo[['VeÃ­culo', 'CPU']],
                        on='VeÃ­culo',
                        how='left'
                    )
                    df_tabela_total.rename(columns={'CPU': 'Total'}, inplace=True)
                elif tipo_visualizacao == "CPU (Custo por Unidade)" and coluna_visualizacao in df_visualizacao_pivot.columns:
                    try:
                        df_tabela_total = df_visualizacao_pivot.pivot_table(
                            index='VeÃ­culo',
                            columns=coluna_periodo_pivot,
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )
                    except KeyError:
                        st.warning(f"âš ï¸ Coluna '{coluna_visualizacao}' nÃ£o encontrada para montar a tabela por veÃ­culo.")
                        df_tabela_total = pd.DataFrame(index=pd.Index([], name='VeÃ­culo'))
                elif tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.warning("âš ï¸ NÃ£o foi possÃ­vel montar a tabela em CPU (faltam colunas Total/Volume).")
                    df_tabela_total = pd.DataFrame(index=pd.Index([], name='VeÃ­culo'))
                else:
                    # Para Custo Total, usar soma normalmente
                    if coluna_visualizacao not in df_visualizacao_pivot.columns:
                        st.warning(f"âš ï¸ Coluna '{coluna_visualizacao}' nÃ£o encontrada para montar a tabela por veÃ­culo.")
                        df_tabela_total = pd.DataFrame(index=pd.Index([], name='VeÃ­culo'))
                    else:
                        try:
                            df_tabela_total = df_visualizacao_pivot.pivot_table(
                                index='VeÃ­culo',
                                columns=coluna_periodo_pivot,
                                values=coluna_visualizacao,
                                aggfunc='sum',
                                fill_value=0
                            )
                        except KeyError:
                            st.warning(f"âš ï¸ Coluna '{coluna_visualizacao}' nÃ£o encontrada para montar a tabela por veÃ­culo.")
                            df_tabela_total = pd.DataFrame(index=pd.Index([], name='VeÃ­culo'))
                    
                # Se "VeÃ­culo" ficou como Ã­ndice em algum caminho, trazer para coluna antes de reordenar
                if df_tabela_total.index.name == 'VeÃ­culo' and 'VeÃ­culo' not in df_tabela_total.columns:
                    df_tabela_total = df_tabela_total.reset_index()

                # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                for col in colunas_periodos:
                    if col not in df_tabela_total.columns:
                        df_tabela_total[col] = 0

                # Reordenar mantendo a coluna "VeÃ­culo" (nÃ£o pode sumir, senÃ£o vira RangeIndex numÃ©rico)
                # e preservando (quando existir) a coluna "Total" ponderada (CPU)
                colunas_base = ['VeÃ­culo'] if 'VeÃ­culo' in df_tabela_total.columns else []
                colunas_reordenadas = colunas_base + colunas_periodos
                if 'Total' in df_tabela_total.columns:
                    colunas_reordenadas = colunas_reordenadas + ['Total']
                df_tabela_total = df_tabela_total[colunas_reordenadas]

                # Calcular total por linha
                # Em CPU, manter o Total ponderado por Volume (nÃ£o somar CPUs)
                if tipo_visualizacao != "CPU (Custo por Unidade)":
                    df_tabela_total['Total'] = df_tabela_total[colunas_periodos].sum(axis=1)
                else:
                    if 'Total' not in df_tabela_total.columns:
                        df_tabela_total['Total'] = 0
                
            # Resetar Ã­ndice apenas se necessÃ¡rio e seguro
            if df_tabela_total.index.name == 'VeÃ­culo' and 'VeÃ­culo' not in df_tabela_total.columns:
                df_tabela_total = df_tabela_total.reset_index()
            
            # Ordenar com seguranÃ§a (evita KeyError quando a coluna nÃ£o existe)
            if 'VeÃ­culo' in df_tabela_total.columns:
                df_tabela_total = df_tabela_total.sort_values('VeÃ­culo')
            else:
                df_tabela_total['VeÃ­culo'] = pd.NA
                
            # Adicionar colunas adicionais fazendo merge com o primeiro valor nÃ£o nulo por VeÃ­culo
            if colunas_adicionais:
                # Filtrar apenas colunas que realmente existem no DataFrame
                colunas_adicionais_validas = [
                    col for col in colunas_adicionais 
                    if col in df_visualizacao.columns
                ]
                    
                if colunas_adicionais_validas:
                    # Agrupar por VeÃ­culo e pegar o primeiro valor nÃ£o nulo de cada coluna adicional
                    # Usar df_visualizacao original para ter todas as colunas
                    df_colunas_adicionais = df_visualizacao_total.groupby('VeÃ­culo')[colunas_adicionais_validas].first().reset_index()
                    # Fazer merge com a tabela total
                    df_tabela_total = pd.merge(
                        df_tabela_total,
                        df_colunas_adicionais,
                        on='VeÃ­culo',
                        how='left'
                    )
                    # Reordenar colunas: VeÃ­culo, colunas adicionais (na ordem original), perÃ­odos, Total
                    # Manter a ordem original das colunas adicionais
                    colunas_adicionais_ordenadas = [
                        col for col in colunas_adicionais 
                        if col in colunas_adicionais_validas
                    ]
                    colunas_finais = ['VeÃ­culo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Total']
                    # Manter apenas colunas que existem
                    colunas_finais = [col for col in colunas_finais if col in df_tabela_total.columns]
                    df_tabela_total = df_tabela_total[colunas_finais]
                
            # Formatar valores baseado no tipo de visualizaÃ§Ã£o
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        return f"R$ {val:,.2f}"
                return val
                
            # Aplicar formataÃ§Ã£o apenas nas colunas numÃ©ricas (exceto VeÃ­culo e colunas adicionais)
            df_tabela_total_formatado = df_tabela_total.copy()
            # Obter colunas adicionais que foram realmente adicionadas Ã  tabela
            colunas_adicionais_na_tabela = [
                col for col in df_tabela_total_formatado.columns 
                if col not in ['VeÃ­culo'] + colunas_periodos + ['Total']
            ]
            colunas_formatar_total = [
                col for col in df_tabela_total_formatado.columns 
                if col not in ['VeÃ­culo'] + colunas_adicionais_na_tabela and 
                df_tabela_total_formatado[col].dtype in ['float64', 'float32', 'int64', 'int32']
            ]
            for col in colunas_formatar_total:
                df_tabela_total_formatado[col] = df_tabela_total_formatado[col].apply(
                    lambda x: formatar_valor(x, tipo_visualizacao)
                )
                
            # Calcular totais por coluna (meses) usando dados numÃ©ricos
            linha_total_geral = {'VeÃ­culo': '**TOTAL**'}
                
            # Adicionar valores vazios para colunas adicionais na linha de total
            for col in colunas_adicionais_na_tabela:
                if col in df_tabela_total.columns:
                    linha_total_geral[col] = pd.NA
                
            # Adicionar totais por coluna (meses e Total)
            # LÃ“GICA CORRIGIDA: Quando filtra por um veÃ­culo, o total deve ser o valor desse veÃ­culo
            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_agrupado_periodo is not None:
                # Total por perÃ­odo e Total geral: sempre baseado na base agregada (custo+volume)
                for col in colunas_periodos:
                    if col in df_tabela_total.columns:
                        df_p = df_agrupado_periodo[df_agrupado_periodo[coluna_periodo_pivot] == col]
                        total_periodo = float(pd.to_numeric(df_p['Total'], errors='coerce').fillna(0).sum())
                        volume_periodo = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                        cpu_periodo = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0
                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)

                if 'Total' in df_tabela_total.columns:
                    total_geral = float(pd.to_numeric(df_agrupado_periodo['Total'], errors='coerce').fillna(0).sum())
                    volume_geral = float(pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0).sum())
                    cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
                    linha_total_geral['Total'] = formatar_valor(cpu_geral, tipo_visualizacao)
                # NÃƒO processar outras colunas numÃ©ricas aqui - apenas colunas de perÃ­odo jÃ¡ foram processadas acima
                # elif df_tabela_total[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                #     total_col = df_tabela_total[col].sum()
                #     linha_total_geral[col] = formatar_valor(total_col, tipo_visualizacao)
            else:
                # Para Custo Total, somar normalmente
                for col in df_tabela_total.columns:
                    if col not in ['VeÃ­culo'] + colunas_adicionais_na_tabela:
                        if df_tabela_total[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                            total_col = df_tabela_total[col].sum()
                            linha_total_geral[col] = formatar_valor(total_col, tipo_visualizacao)
                
            # Adicionar linha de total ao DataFrame
            df_tabela_total_display = pd.concat([
                df_tabela_total_formatado,
                pd.DataFrame([linha_total_geral])
            ], ignore_index=True)
                
            # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
            colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
            for col in colunas_para_remover:
                if col in df_tabela_total_display.columns:
                    df_tabela_total_display = df_tabela_total_display.drop(columns=[col])
                
            st.dataframe(df_tabela_total_display, width="stretch")
                
            # BotÃ£o de download da tabela total
            if st.button(
                "ðŸ“¥ Baixar Tabela Total por VeÃ­culo (Excel)",
                width="stretch",
                key="download_tabela_total_veiculo"
            ):
                with st.spinner("Gerando arquivo da tabela total..."):
                    try:
                        # Criar DataFrame completo para download (com linha de total)
                        df_total_download = df_tabela_total.copy()
                            
                        # Adicionar linha de total
                        linha_total_download = {'VeÃ­culo': 'TOTAL'}
                        # Em CPU, a linha TOTAL deve ser ponderada por volume (sem usar Volume mergeado em linhas de custo)
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_agrupado_periodo is not None:
                            for col in colunas_periodos:
                                if col in df_tabela_total.columns:
                                    df_p = df_agrupado_periodo[df_agrupado_periodo[coluna_periodo_pivot] == col]
                                    total_periodo = float(pd.to_numeric(df_p['Total'], errors='coerce').fillna(0).sum())
                                    volume_periodo = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                                    linha_total_download[col] = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0

                            if 'Total' in df_tabela_total.columns:
                                total_geral = float(pd.to_numeric(df_agrupado_periodo['Total'], errors='coerce').fillna(0).sum())
                                volume_geral = float(pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0).sum())
                                linha_total_download['Total'] = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
                        else:
                            # Para Custo Total, somar normalmente
                            for col in df_tabela_total.columns:
                                if col != 'VeÃ­culo':
                                    total_col = df_tabela_total[col].sum()
                                    linha_total_download[col] = total_col
                            
                        df_total_download = pd.concat([
                            df_total_download,
                            pd.DataFrame([linha_total_download])
                        ], ignore_index=True)
                            
                        # Obter pasta Downloads do usuÃ¡rio
                        downloads_path = os.path.join(
                            os.path.expanduser("~"), "Downloads"
                        )
                        tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                        file_name = f"TC_Ext_tabela_total_veiculo_{tipo_nome}.xlsx"
                        file_path = os.path.join(downloads_path, file_name)
                            
                        # Salvar arquivo diretamente na pasta Downloads
                        with pd.ExcelWriter(
                            file_path, engine='openpyxl'
                        ) as writer:
                            df_total_download.to_excel(
                                writer, index=False, sheet_name='Total_Veiculo'
                            )
                            
                        st.success(
                            f"âœ… Arquivo salvo com sucesso em: {file_path}"
                        )
                        st.info(
                            f"ðŸ“ Verifique sua pasta Downloads: {downloads_path}"
                        )
                    except Exception as e:
                        st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")
            else:
                if not tem_veiculo or not tem_periodo:
                    colunas_faltando_total = []
                    if not tem_veiculo:
                        colunas_faltando_total.append("VeÃ­culo")
                    if not tem_periodo:
                        colunas_faltando_total.append("PerÃ­odo")
                    st.info(f"â„¹ï¸ Colunas necessÃ¡rias nÃ£o encontradas para criar a tabela total: {', '.join(colunas_faltando_total)}")

        # Exibir tabela filtrada (TODAS as linhas)
        # Determinar tÃ­tulo do expander
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            titulo_expander_filtrada = "ðŸ“‹ **Tabela Filtrada - CPU (Todas as Linhas)**"
        else:
            titulo_expander_filtrada = "ðŸ“‹ **Tabela Filtrada (Todas as Linhas)**"

        with st.expander(titulo_expander_filtrada, expanded=False):
            # Usar TODAS as linhas (sem limite)
            df_display = df_visualizacao.copy()

            # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
            colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
            for col in colunas_para_remover:
                if col in df_display.columns:
                    df_display = df_display.drop(columns=[col])

            st.info(f"ðŸ“Š Exibindo todas as {len(df_display):,} linhas e {len(df_display.columns)} colunas")
            st.dataframe(df_display, width="stretch")

            # BotÃ£o de download da Tabela Filtrada
            if st.button(
                "ðŸ“¥ Baixar Tabela Filtrada (Excel)",
                width="stretch",
                key="download_filtered"
            ):
                with st.spinner("Gerando arquivo da tabela filtrada..."):
                    try:
                        # Obter pasta Downloads do usuÃ¡rio
                        downloads_path = os.path.join(
                            os.path.expanduser("~"), "Downloads"
                        )
                        file_name = "TC_Ext_tabela_filtrada.xlsx"
                        file_path = os.path.join(downloads_path, file_name)

                        # Salvar arquivo diretamente na pasta Downloads
                        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                            df_visualizacao.to_excel(
                                writer, index=False, sheet_name='Dados_Filtrados'
                            )

                        st.success(f"âœ… Arquivo salvo com sucesso em: {file_path}")
                        st.info(f"ðŸ“ Verifique sua pasta Downloads: {downloads_path}")
                    except Exception as e:
                        st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")

    # ==========================================
    # TAB 5: Detalhe Budget
    # ==========================================
    with tab5:
        # Bases do Budget (custos e volume) jÃ¡ sÃ£o filtradas pelos mesmos filtros da sidebar no TAB 1
        df_budget_base = df_budget_filtrado if 'df_budget_filtrado' in locals() else None
        df_budget_vol_base = df_budget_vol_filtrado if 'df_budget_vol_filtrado' in locals() else None

        if df_budget_base is None or df_budget_base is False or (hasattr(df_budget_base, 'empty') and df_budget_base.empty):
            st.info("â„¹ï¸ Sem dados de Budget para exibir com os filtros atuais.")
        else:
            df_visualizacao_budget = df_budget_base.copy()

            # Definir coluna de visualizaÃ§Ã£o para o Budget
            # OBS: no modo CPU, a coluna 'CPU' nÃ£o deve ser calculada linha-a-linha;
            # serÃ¡ calculada apÃ³s agregaÃ§Ãµes usando custos + volume agregados (regra da documentaÃ§Ã£o).
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                coluna_visualizacao_budget = 'Total'
            else:
                coluna_visualizacao_budget = 'Total' if 'Total' in df_visualizacao_budget.columns else 'Valor'

            # Garantir Volume no Budget quando necessÃ¡rio (CPU serÃ¡ calculado apÃ³s agregaÃ§Ãµes)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                if df_budget_vol_base is None or (hasattr(df_budget_vol_base, 'empty') and df_budget_vol_base.empty) or 'Volume' not in getattr(df_budget_vol_base, 'columns', []):
                    try:
                        df_budget_vol_base = load_budget_volume_data(ano_selecionado)
                    except Exception:
                        df_budget_vol_base = None

                if 'Total' not in df_visualizacao_budget.columns:
                    if 'Valor' in df_visualizacao_budget.columns:
                        df_visualizacao_budget['Total'] = df_visualizacao_budget['Valor']
                    elif 'Custo' in df_visualizacao_budget.columns:
                        df_visualizacao_budget['Total'] = df_visualizacao_budget['Custo']

                if 'Total' in df_visualizacao_budget.columns:
                    df_visualizacao_budget['Total'] = pd.to_numeric(df_visualizacao_budget['Total'], errors='coerce')
                if df_budget_vol_base is not None and 'Volume' in getattr(df_budget_vol_base, 'columns', []):
                    df_budget_vol_base = df_budget_vol_base.copy()
                    df_budget_vol_base['Volume'] = pd.to_numeric(df_budget_vol_base['Volume'], errors='coerce')

            tem_veiculo_budget = 'VeÃ­culo' in df_visualizacao_budget.columns
            tem_oficina_budget = 'Oficina' in df_visualizacao_budget.columns
            tem_periodo_budget = 'PerÃ­odo' in df_visualizacao_budget.columns

            # ------------------------------
            # Tabelas detalhadas (Budget)
            # ------------------------------
            with st.expander("ðŸ“Š **Tabelas Detalhadas (Budget)**", expanded=False):
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    # Budget tem 2 leituras (e elas NÃƒO sÃ£o iguais):
                    # - CPU Budget: Total Budget / Volume Budget
                    # - CPU Flex Bud: Flex Bud (ajuste do nÃ£o-fixo por Volume Real/Volume Budget) / Volume Real
                    metodo_cpu_budget = st.radio(
                        "Modo de CPU (Budget):",
                        [
                            "CPU Budget (Total Budget / Volume Budget)",
                            "CPU Flex Bud (ajuste por Volume Real/Volume Budget)",
                        ],
                        index=0,
                        horizontal=True,
                        key="cpu_budget_modo_tab5",
                    )
                    if metodo_cpu_budget.startswith("CPU Flex Bud"):
                        st.subheader("ðŸ“‹ Tabela - CPU (Flex Bud) por VeÃ­culo, Oficina e PerÃ­odo")
                    else:
                        st.subheader("ðŸ“‹ Tabela - CPU (Budget) por VeÃ­culo, Oficina e PerÃ­odo")
                else:
                    st.subheader("ðŸ“‹ Tabela - Custo Total (Budget) por VeÃ­culo, Oficina e PerÃ­odo")

                if tem_veiculo_budget and tem_oficina_budget and tem_periodo_budget and coluna_visualizacao_budget in df_visualizacao_budget.columns:
                    df_budget_pivot = df_visualizacao_budget.copy()

                    df_budget_agr_cpu = None
                    df_total_oficina_periodo_cpu = None

                    tem_multiplos_anos_budget = 'Ano' in df_budget_pivot.columns and df_budget_pivot['Ano'].nunique() > 1

                    # Chaves reais (nÃ£o usar PerÃ­odo_Ano para merge/agrupamento)
                    chaves_agr_budget = ['Oficina', 'VeÃ­culo', 'PerÃ­odo']
                    if tem_multiplos_anos_budget and 'Ano' in df_budget_pivot.columns:
                        chaves_agr_budget.append('Ano')

                    # Para CPU: agregar custo e volume separadamente (evita multiplicar Volume por linha de custo)
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        usar_flex_bud = metodo_cpu_budget.startswith("CPU Flex Bud")
                        df_custo_agr = (
                            df_budget_pivot
                            .groupby(chaves_agr_budget, dropna=False)
                            .agg({'Total': 'sum'})
                            .reset_index()
                        )

                        if usar_flex_bud:
                            st.caption(
                                "â„¹ï¸ CPU Flex Bud: separa Budget em Fixo e NÃ£o-Fixo; "
                                "o NÃ£o-Fixo Ã© ajustado por (Volume Real / Volume Budget) por perÃ­odo; "
                                "divide pelo Volume Real (mesmos filtros da sidebar)."
                            )

                            # Volume Real no mesmo grÃ£o do custo (Oficina/VeÃ­culo/PerÃ­odo[/Ano])
                            try:
                                df_vol_real_base = load_volume_data(ano_selecionado)
                            except Exception:
                                df_vol_real_base = None

                            df_vol_real_filtrado = None
                            if df_vol_real_base is not None:
                                try:
                                    df_total_base_para_filtro = df_total if 'df_total' in locals() else None
                                except Exception:
                                    df_total_base_para_filtro = None
                                df_vol_real_filtrado = filtrar_volume_com_sidebar(
                                    df_vol_real_base,
                                    df_total_base_para_filtro,
                                    ignorar_veiculo=False,
                                )

                            # Volume Budget total por Oficina/PerÃ­odo[/Ano] (nÃ£o tem VeÃ­culo)
                            chaves_bud_tot = ['Oficina', 'PerÃ­odo']
                            if tem_multiplos_anos_budget and 'Ano' in df_budget_pivot.columns:
                                chaves_bud_tot.append('Ano')

                            vol_bud_tot = None
                            if df_budget_vol_base is not None and 'Volume' in getattr(df_budget_vol_base, 'columns', []):
                                vol_bud_tot = (
                                    df_budget_vol_base
                                    .groupby([k for k in chaves_bud_tot if k in df_budget_vol_base.columns], dropna=False)
                                    .agg({'Volume': 'sum'})
                                    .reset_index()
                                    .rename(columns={'Volume': 'Volume_Budget_Total'})
                                )
                                vol_bud_tot['Volume_Budget_Total'] = pd.to_numeric(
                                    vol_bud_tot['Volume_Budget_Total'], errors='coerce'
                                ).fillna(0)

                            # Volume Real (veÃ­culo) e total (oficina/perÃ­odo)
                            df_vol_real_v = None
                            if (
                                df_vol_real_filtrado is not None
                                and 'Volume' in getattr(df_vol_real_filtrado, 'columns', [])
                                and all(k in df_vol_real_filtrado.columns for k in ['Oficina', 'PerÃ­odo', 'VeÃ­culo'])
                            ):
                                chaves_real_v = ['Oficina', 'PerÃ­odo', 'VeÃ­culo']
                                if tem_multiplos_anos_budget and 'Ano' in df_vol_real_filtrado.columns:
                                    chaves_real_v.append('Ano')
                                df_vol_real_v = (
                                    df_vol_real_filtrado
                                    .groupby(chaves_real_v, dropna=False)
                                    .agg({'Volume': 'sum'})
                                    .reset_index()
                                )
                                df_vol_real_v['Volume'] = pd.to_numeric(df_vol_real_v['Volume'], errors='coerce').fillna(0)
                                chaves_real_tot = [k for k in chaves_real_v if k != 'VeÃ­culo']
                                df_vol_real_tot = (
                                    df_vol_real_v
                                    .groupby(chaves_real_tot, dropna=False)
                                    .agg({'Volume': 'sum'})
                                    .reset_index()
                                    .rename(columns={'Volume': 'Volume_Real_Total'})
                                )
                                df_vol_real_tot['Volume_Real_Total'] = pd.to_numeric(
                                    df_vol_real_tot['Volume_Real_Total'], errors='coerce'
                                ).fillna(0)
                            else:
                                df_vol_real_tot = None

                            # Budget fixo por veÃ­culo/perÃ­odo (precisa da coluna Custo)
                            if 'Custo' in df_budget_pivot.columns:
                                df_fix = df_budget_pivot[df_budget_pivot['Custo'].astype(str).str.strip() == 'Fixo'].copy()
                                df_fix_agr = (
                                    df_fix
                                    .groupby(chaves_agr_budget, dropna=False)
                                    .agg({'Total': 'sum'})
                                    .reset_index()
                                    .rename(columns={'Total': 'Total_Fixo'})
                                )
                            else:
                                df_fix_agr = None

                            df_tot_agr = df_custo_agr.rename(columns={'Total': 'Total_Budget'})
                            if df_fix_agr is not None:
                                df_tot_agr = pd.merge(df_tot_agr, df_fix_agr, on=chaves_agr_budget, how='left')
                            df_tot_agr['Total_Fixo'] = pd.to_numeric(df_tot_agr.get('Total_Fixo', 0), errors='coerce').fillna(0)
                            df_tot_agr['Total_Budget'] = pd.to_numeric(df_tot_agr.get('Total_Budget', 0), errors='coerce').fillna(0)
                            df_tot_agr['Total_Nao_Fixo'] = df_tot_agr['Total_Budget'] - df_tot_agr['Total_Fixo']

                            # RazÃ£o por Oficina/PerÃ­odo[/Ano]: Volume Real Total / Volume Budget Total
                            if df_vol_real_tot is not None and vol_bud_tot is not None:
                                ratio = pd.merge(
                                    df_vol_real_tot,
                                    vol_bud_tot,
                                    on=[k for k in chaves_bud_tot if k in df_vol_real_tot.columns],
                                    how='left'
                                )
                                ratio['Volume_Budget_Total'] = pd.to_numeric(
                                    ratio.get('Volume_Budget_Total', 0), errors='coerce'
                                ).fillna(0)
                                ratio['Fator'] = np.where(
                                    ratio['Volume_Budget_Total'] != 0,
                                    ratio['Volume_Real_Total'] / ratio['Volume_Budget_Total'],
                                    1.0,
                                )
                                ratio_cols = [k for k in chaves_bud_tot if k in ratio.columns] + ['Fator']
                                ratio = ratio[ratio_cols]
                            else:
                                ratio = None

                            # Aplicar fator no nÃ£o-fixo e dividir pelo volume real do veÃ­culo
                            chaves_merge_ratio = [k for k in chaves_bud_tot if k in df_tot_agr.columns]
                            if ratio is not None and chaves_merge_ratio:
                                df_flex = pd.merge(df_tot_agr, ratio, on=chaves_merge_ratio, how='left')
                            else:
                                df_flex = df_tot_agr.copy()
                                df_flex['Fator'] = 1.0
                            df_flex['Fator'] = pd.to_numeric(df_flex.get('Fator', 1.0), errors='coerce').fillna(1.0)
                            df_flex['Total'] = df_flex['Total_Fixo'] + (df_flex['Total_Nao_Fixo'] * df_flex['Fator'])

                            # Merge com Volume Real do veÃ­culo
                            if df_vol_real_v is not None:
                                vol_keys = [k for k in chaves_agr_budget if k in df_vol_real_v.columns]
                                df_budget_agr = pd.merge(
                                    df_flex[chaves_agr_budget + ['Total']],
                                    df_vol_real_v[vol_keys + ['Volume']],
                                    on=vol_keys,
                                    how='outer'
                                )
                            else:
                                df_budget_agr = df_flex[chaves_agr_budget + ['Total']].copy()
                                df_budget_agr['Volume'] = 0
                        else:
                            # Volume Budget pode nÃ£o ter VeÃ­culo.
                            # Se NÃƒO tiver VeÃ­culo, ratear o Volume do Budget por veÃ­culo usando a participaÃ§Ã£o do volume Real
                            # (mesmos filtros). Isso evita dividir custo de um veÃ­culo pelo volume total da oficina.
                            bud_vol_tem_veiculo = (
                                df_budget_vol_base is not None
                                and hasattr(df_budget_vol_base, 'columns')
                                and 'VeÃ­culo' in df_budget_vol_base.columns
                            )

                            if (not bud_vol_tem_veiculo) and df_budget_vol_base is not None and 'Volume' in getattr(df_budget_vol_base, 'columns', []):
                                st.error(
                                    "âŒ ERRO NA EXTRAÃ‡ÃƒO: o Volume do Budget precisa conter a coluna 'VeÃ­culo'. "
                                    "NÃ£o Ã© mais permitido rateio/fallback no app."
                                )
                                st.info(
                                    "ðŸ’¡ RefaÃ§a a extraÃ§Ã£o do BUDGET (pÃ¡gina 'ExtraÃ§Ã£o de Dados') e corrija a aba 'Volume BDG' no Excel."
                                )
                                st.stop()
                            else:
                                # Volume Budget tem VeÃ­culo (ou nÃ£o existe volume): usar merge direto no mesmo grÃ£o
                                vol_keys = []
                                if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                                    vol_keys = [k for k in chaves_agr_budget if k in df_budget_vol_base.columns]

                                if (
                                    df_budget_vol_base is not None
                                    and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                                    and vol_keys
                                ):
                                    df_vol_agr = (
                                        df_budget_vol_base
                                        .groupby(vol_keys, dropna=False)
                                        .agg({'Volume': 'sum'})
                                        .reset_index()
                                    )
                                    df_budget_agr = pd.merge(df_custo_agr, df_vol_agr, on=vol_keys, how='left')
                                else:
                                    df_budget_agr = df_custo_agr.copy()
                                    df_budget_agr['Volume'] = 0

                        df_budget_agr['Volume'] = pd.to_numeric(df_budget_agr.get('Volume', 0), errors='coerce').fillna(0)
                        df_budget_agr['Total'] = pd.to_numeric(df_budget_agr.get('Total', 0), errors='coerce').fillna(0)
                        df_budget_agr['CPU'] = np.where(
                            (df_budget_agr['Volume'].notna()) & (df_budget_agr['Volume'] != 0),
                            df_budget_agr['Total'] / df_budget_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos_budget:
                            df_budget_agr['PerÃ­odo_Ano'] = (
                                df_budget_agr['PerÃ­odo'].astype(str) + ' ' + df_budget_agr['Ano'].astype(str)
                            )
                            coluna_periodo_pivot_budget = 'PerÃ­odo_Ano'
                        else:
                            coluna_periodo_pivot_budget = 'PerÃ­odo'

                        # Guardar base agregada (Total/Volume) para cÃ¡lculos de TOTAL por oficina/perÃ­odo
                        df_budget_agr_cpu = df_budget_agr.copy()

                        # Base TOTAL por Oficina+PerÃ­odo (+Ano) deve seguir EXATAMENTE o mesmo denominador
                        # usado na tabela (Volume Budget rateado ou Volume Real/Flex), e respeitar filtros.
                        df_total_oficina_periodo_cpu = None
                        try:
                            chaves_of_p = ['Oficina', 'PerÃ­odo']
                            if tem_multiplos_anos_budget and 'Ano' in df_budget_agr_cpu.columns:
                                chaves_of_p.append('Ano')

                            df_total_oficina_periodo_cpu = (
                                df_budget_agr_cpu
                                .groupby(chaves_of_p, dropna=False)
                                .agg({'Total': 'sum', 'Volume': 'sum'})
                                .reset_index()
                            )
                            df_total_oficina_periodo_cpu['Total'] = pd.to_numeric(
                                df_total_oficina_periodo_cpu.get('Total', 0),
                                errors='coerce'
                            ).fillna(0)
                            df_total_oficina_periodo_cpu['Volume'] = pd.to_numeric(
                                df_total_oficina_periodo_cpu.get('Volume', 0),
                                errors='coerce'
                            ).fillna(0)
                            df_total_oficina_periodo_cpu['CPU'] = np.where(
                                (df_total_oficina_periodo_cpu['Volume'].notna()) & (df_total_oficina_periodo_cpu['Volume'] != 0),
                                df_total_oficina_periodo_cpu['Total'] / df_total_oficina_periodo_cpu['Volume'],
                                0
                            )
                            if tem_multiplos_anos_budget:
                                df_total_oficina_periodo_cpu['PerÃ­odo_Ano'] = (
                                    df_total_oficina_periodo_cpu['PerÃ­odo'].astype(str)
                                    + ' '
                                    + df_total_oficina_periodo_cpu['Ano'].astype(str)
                                )
                        except Exception:
                            df_total_oficina_periodo_cpu = None

                        df_tabela_budget = df_budget_agr.pivot_table(
                            index=['Oficina', 'VeÃ­culo'],
                            columns=coluna_periodo_pivot_budget,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        # Custo Total: soma normal
                        coluna_periodo_pivot_budget = 'PerÃ­odo'
                        if tem_multiplos_anos_budget:
                            df_budget_pivot['PerÃ­odo_Ano'] = (
                                df_budget_pivot['PerÃ­odo'].astype(str) + ' ' + df_budget_pivot['Ano'].astype(str)
                            )
                            coluna_periodo_pivot_budget = 'PerÃ­odo_Ano'

                        df_tabela_budget = df_budget_pivot.pivot_table(
                            index=['Oficina', 'VeÃ­culo'],
                            columns=coluna_periodo_pivot_budget,
                            values=coluna_visualizacao_budget,
                            aggfunc='sum',
                            fill_value=0
                        )

                    # Ordenar colunas de perÃ­odos
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_budget_pivot['Ano'].unique())
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in df_tabela_budget.columns:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in df_tabela_budget.columns if c not in colunas_ordenadas]
                        colunas_periodos_budget = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [c for c in meses_ordem if c in df_tabela_budget.columns]
                        colunas_restantes = [c for c in df_tabela_budget.columns if c not in meses_ordem]
                        colunas_periodos_budget = colunas_existentes + colunas_restantes

                    for col in colunas_periodos_budget:
                        if col not in df_tabela_budget.columns:
                            df_tabela_budget[col] = 0
                    df_tabela_budget = df_tabela_budget[colunas_periodos_budget]

                    # Total por linha
                    # Em CPU, o TOTAL deve ser ponderado: sum(Total) / sum(Volume) (NUNCA somar CPUs mensais)
                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_budget_agr_cpu is not None:
                        df_total_oficina_veiculo = df_budget_agr_cpu.groupby(['Oficina', 'VeÃ­culo'], dropna=False).agg({'Total': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_oficina_veiculo['CPU'] = np.where(
                            (df_total_oficina_veiculo['Volume'].notna()) & (df_total_oficina_veiculo['Volume'] != 0),
                            df_total_oficina_veiculo['Total'] / df_total_oficina_veiculo['Volume'],
                            0
                        )
                        df_tabela_budget = df_tabela_budget.reset_index()
                        df_tabela_budget = pd.merge(
                            df_tabela_budget,
                            df_total_oficina_veiculo[['Oficina', 'VeÃ­culo', 'CPU']],
                            on=['Oficina', 'VeÃ­culo'],
                            how='left'
                        )
                        df_tabela_budget.rename(columns={'CPU': 'Total'}, inplace=True)
                    elif tipo_visualizacao != "CPU (Custo por Unidade)":
                        df_tabela_budget = df_tabela_budget.reset_index()
                        df_tabela_budget['Total'] = df_tabela_budget[colunas_periodos_budget].sum(axis=1)
                    else:
                        df_tabela_budget = df_tabela_budget.reset_index()
                        df_tabela_budget['Total'] = 0

                    # FormataÃ§Ã£o simples
                    df_tabela_budget_fmt = df_tabela_budget.copy()
                    cols_num = [c for c in df_tabela_budget_fmt.columns if c not in ['Oficina', 'VeÃ­culo']]
                    for c in cols_num:
                        if c in df_tabela_budget_fmt.columns:
                            df_tabela_budget_fmt[c] = df_tabela_budget_fmt[c].map(
                                lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x
                            )

                    # Renderizar por Oficina (igual ao Detalhe Real): subtotal no tÃ­tulo e tabela sem coluna Oficina
                    oficinas_budget = df_tabela_budget_fmt['Oficina'].dropna().unique().tolist()
                    if len(oficinas_budget) == 0:
                        st.info("Nenhum dado encontrado para exibir por Oficina (Budget).")
                    else:
                        for oficina in sorted(oficinas_budget):
                            df_oficina_fmt = df_tabela_budget_fmt[df_tabela_budget_fmt['Oficina'] == oficina].copy()
                            df_oficina_num = df_tabela_budget[df_tabela_budget['Oficina'] == oficina].copy()

                            # Subtotal da oficina
                            if (
                                tipo_visualizacao == "CPU (Custo por Unidade)"
                                and df_total_oficina_periodo_cpu is not None
                                and 'Total' in df_total_oficina_periodo_cpu.columns
                                and 'Volume' in df_total_oficina_periodo_cpu.columns
                            ):
                                df_base_oficina = df_total_oficina_periodo_cpu[df_total_oficina_periodo_cpu['Oficina'] == oficina].copy()
                                total_of = float(pd.to_numeric(df_base_oficina['Total'], errors='coerce').fillna(0).sum())
                                vol_of = float(pd.to_numeric(df_base_oficina['Volume'], errors='coerce').fillna(0).sum())
                                subtotal_of = (total_of / vol_of) if vol_of not in (0, None) else 0.0
                            else:
                                subtotal_of = float(pd.to_numeric(df_oficina_num.get('Total', 0), errors='coerce').fillna(0).sum())

                            subtotal_of_fmt = f"{subtotal_of:,.2f}"

                            with st.container():
                                st.markdown(
                                    f"### ðŸ­ **{oficina}** - Total: {subtotal_of_fmt} ("
                                    f"{len(df_oficina_fmt)} veÃ­culo{'s' if len(df_oficina_fmt) > 1 else ''})"
                                )

                                # Remover coluna Oficina (jÃ¡ estÃ¡ no tÃ­tulo)
                                df_oficina_display = df_oficina_fmt.drop(columns=['Oficina'])

                                # Linha TOTAL por oficina
                                linha_total = {'VeÃ­culo': '**TOTAL**'}
                                for col in colunas_periodos_budget:
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_total_oficina_periodo_cpu is not None:
                                        df_tmp = df_total_oficina_periodo_cpu[df_total_oficina_periodo_cpu['Oficina'] == oficina]
                                        if coluna_periodo_pivot_budget in df_tmp.columns:
                                            df_tmp_p = df_tmp[df_tmp[coluna_periodo_pivot_budget] == col]
                                        else:
                                            df_tmp_p = df_tmp
                                        total_p = float(pd.to_numeric(df_tmp_p['Total'], errors='coerce').fillna(0).sum())
                                        vol_p = float(pd.to_numeric(df_tmp_p['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_p = (total_p / vol_p) if vol_p not in (0, None) else 0.0
                                        linha_total[col] = f"{cpu_p:,.2f}"
                                    else:
                                        soma_p = float(pd.to_numeric(df_oficina_num.get(col, 0), errors='coerce').fillna(0).sum())
                                        linha_total[col] = f"{soma_p:,.2f}"

                                # Coluna Total
                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                    linha_total['Total'] = f"{subtotal_of:,.2f}"
                                else:
                                    soma_total = float(pd.to_numeric(df_oficina_num.get('Total', 0), errors='coerce').fillna(0).sum())
                                    linha_total['Total'] = f"{soma_total:,.2f}"

                                df_oficina_display = pd.concat(
                                    [df_oficina_display, pd.DataFrame([linha_total])],
                                    ignore_index=True
                                )

                                st.dataframe(df_oficina_display, width="stretch")
                else:
                    st.info("â„¹ï¸ Colunas necessÃ¡rias nÃ£o encontradas para montar a tabela detalhada de Budget.")

                # Tabela dinÃ¢mica: Budget por Oficina e PerÃ­odo
                if (
                    'Oficina' in df_visualizacao_budget.columns
                    and 'PerÃ­odo' in df_visualizacao_budget.columns
                    and coluna_visualizacao_budget in df_visualizacao_budget.columns
                ):
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        st.subheader("ðŸ“‹ Tabela DinÃ¢mica - CPU (Budget) por Oficina e PerÃ­odo")
                    else:
                        st.subheader("ðŸ“‹ Tabela DinÃ¢mica - Valor (Budget) por Oficina e PerÃ­odo")

                    df_pivot_budget = df_visualizacao_budget.copy()
                    tem_multiplos_anos_budget = 'Ano' in df_pivot_budget.columns and df_pivot_budget['Ano'].nunique() > 1

                    # Chaves reais para agregaÃ§Ã£o (sem PerÃ­odo_Ano)
                    chaves_of = ['Oficina', 'PerÃ­odo']
                    if tem_multiplos_anos_budget and 'Ano' in df_pivot_budget.columns:
                        chaves_of.append('Ano')

                    if tem_multiplos_anos_budget:
                        col_pivot_budget = 'PerÃ­odo_Ano'
                    else:
                        col_pivot_budget = 'PerÃ­odo'

                    # Regra crÃ­tica (documentaÃ§Ã£o): em CPU, calcular a partir de custo agregado e volume agregado
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_custo_of = (
                            df_pivot_budget
                            .groupby(chaves_of, dropna=False)
                            .agg({'Total': 'sum'})
                            .reset_index()
                        )

                        vol_keys_of = []
                        if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                            vol_keys_of = [k for k in chaves_of if k in df_budget_vol_base.columns]

                        if (
                            df_budget_vol_base is not None
                            and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                            and vol_keys_of
                        ):
                            df_vol_of = (
                                df_budget_vol_base
                                .groupby(vol_keys_of, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                            df_cpu_agr = pd.merge(df_custo_of, df_vol_of, on=vol_keys_of, how='left')
                        else:
                            df_cpu_agr = df_custo_of.copy()
                            df_cpu_agr['Volume'] = 0

                        df_cpu_agr['Total'] = pd.to_numeric(df_cpu_agr.get('Total', 0), errors='coerce').fillna(0)
                        df_cpu_agr['Volume'] = pd.to_numeric(df_cpu_agr.get('Volume', 0), errors='coerce').fillna(0)
                        df_cpu_agr['CPU'] = np.where(
                            (df_cpu_agr['Volume'].notna()) & (df_cpu_agr['Volume'] != 0),
                            df_cpu_agr['Total'] / df_cpu_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos_budget:
                            df_cpu_agr['PerÃ­odo_Ano'] = (
                                df_cpu_agr['PerÃ­odo'].astype(str) + ' ' + df_cpu_agr['Ano'].astype(str)
                            )

                        df_pivot = df_cpu_agr.pivot_table(
                            index='Oficina',
                            columns=col_pivot_budget,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )

                        # Total ponderado por Oficina
                        df_total_of = df_cpu_agr.groupby('Oficina', dropna=False).agg({'Total': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_of['Total_CPU'] = np.where(
                            (df_total_of['Volume'].notna()) & (df_total_of['Volume'] != 0),
                            df_total_of['Total'] / df_total_of['Volume'],
                            0
                        )
                        df_pivot['Total'] = df_pivot.index.to_series().map(
                            df_total_of.set_index('Oficina')['Total_CPU']
                        ).fillna(0)
                    else:
                        # Custo Total: soma normal
                        if tem_multiplos_anos_budget:
                            df_pivot_budget['PerÃ­odo_Ano'] = (
                                df_pivot_budget['PerÃ­odo'].astype(str) + ' ' + df_pivot_budget['Ano'].astype(str)
                            )
                        df_pivot = df_pivot_budget.pivot_table(
                            index='Oficina',
                            columns=col_pivot_budget,
                            values=coluna_visualizacao_budget,
                            aggfunc='sum',
                            fill_value=0
                        )
                        df_pivot['Total'] = df_pivot.sum(axis=1)

                    # Ordenar colunas de perÃ­odos (sem mexer na coluna Total)
                    colunas_periodos_pivot = [c for c in df_pivot.columns if c != 'Total']
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_pivot_budget['Ano'].unique())
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in colunas_periodos_pivot:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in colunas_periodos_pivot if c not in colunas_ordenadas]
                        df_pivot = df_pivot[colunas_ordenadas + colunas_restantes + ['Total']]
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [c for c in meses_ordem if c in colunas_periodos_pivot]
                        colunas_restantes = [c for c in colunas_periodos_pivot if c not in meses_ordem]
                        df_pivot = df_pivot[colunas_existentes + colunas_restantes + ['Total']]

                    df_pivot = df_pivot.sort_values('Total', ascending=False)
                    df_pivot_fmt = df_pivot.applymap(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
                    st.dataframe(df_pivot_fmt, width="stretch")

            # ------------------------------
            # Tabela total por VeÃ­culo e PerÃ­odo (Budget)
            # ------------------------------
            titulo_total_budget = (
                "ðŸ“‹ **Tabela - CPU Total (Budget) por VeÃ­culo e PerÃ­odo**"
                if tipo_visualizacao == "CPU (Custo por Unidade)"
                else "ðŸ“‹ **Tabela - Custo Total (Budget) por VeÃ­culo e PerÃ­odo**"
            )
            with st.expander(titulo_total_budget, expanded=False):
                if tem_veiculo_budget and tem_periodo_budget:
                    df_budget_total = df_visualizacao_budget.copy()

                    tem_multiplos_anos_budget = 'Ano' in df_budget_total.columns and df_budget_total['Ano'].nunique() > 1
                    chaves = ['VeÃ­culo', 'PerÃ­odo'] + (['Ano'] if tem_multiplos_anos_budget and 'Ano' in df_budget_total.columns else [])

                    # Custo agregado
                    if 'Total' not in df_budget_total.columns and 'Valor' in df_budget_total.columns:
                        df_budget_total['Total'] = df_budget_total['Valor']
                    df_budget_total['Total'] = pd.to_numeric(df_budget_total.get('Total', 0), errors='coerce').fillna(0)

                    df_custo_agr = df_budget_total.groupby(chaves, dropna=False).agg({'Total': 'sum'}).reset_index()

                    # Volume agregado
                    bud_vol_tem_veiculo = (
                        df_budget_vol_base is not None
                        and hasattr(df_budget_vol_base, 'columns')
                        and 'VeÃ­culo' in df_budget_vol_base.columns
                    )

                    if tipo_visualizacao == "CPU (Custo por Unidade)" and (not bud_vol_tem_veiculo) and df_budget_vol_base is not None and 'Volume' in getattr(df_budget_vol_base, 'columns', []):
                        st.error(
                            "âŒ ERRO NA EXTRAÃ‡ÃƒO: o Volume do Budget precisa conter a coluna 'VeÃ­culo'. "
                            "NÃ£o Ã© mais permitido rateio/fallback no app."
                        )
                        st.info(
                            "ðŸ’¡ RefaÃ§a a extraÃ§Ã£o do BUDGET (pÃ¡gina 'ExtraÃ§Ã£o de Dados') e corrija a aba 'Volume BDG' no Excel."
                        )
                        st.stop()
                    else:
                        # Volume agregado direto no grÃ£o disponÃ­vel
                        vol_keys_tot = []
                        if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                            vol_keys_tot = [k for k in chaves if k in df_budget_vol_base.columns]

                        if (
                            df_budget_vol_base is not None
                            and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                            and vol_keys_tot
                        ):
                            df_volume_agr = (
                                df_budget_vol_base
                                .groupby(vol_keys_tot, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                            df_total_agr = pd.merge(df_custo_agr, df_volume_agr, on=vol_keys_tot, how='left')
                        else:
                            df_total_agr = df_custo_agr.copy()
                            df_total_agr['Volume'] = 0

                    df_total_agr['Volume'] = pd.to_numeric(df_total_agr.get('Volume', 0), errors='coerce').fillna(0)
                    df_total_agr['Total'] = pd.to_numeric(df_total_agr.get('Total', 0), errors='coerce').fillna(0)

                    # Coluna de pivot
                    if tem_multiplos_anos_budget:
                        df_total_agr['PerÃ­odo_Ano'] = (
                            df_total_agr['PerÃ­odo'].astype(str) + ' ' + df_total_agr['Ano'].astype(str)
                        )
                        col_pivot = 'PerÃ­odo_Ano'
                    else:
                        col_pivot = 'PerÃ­odo'

                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_total_agr['CPU'] = np.where(
                            (df_total_agr['Volume'].notna()) & (df_total_agr['Volume'] != 0),
                            df_total_agr['Total'] / df_total_agr['Volume'],
                            0
                        )
                        df_tabela_total = df_total_agr.pivot_table(
                            index='VeÃ­culo',
                            columns=col_pivot,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        df_tabela_total = df_total_agr.pivot_table(
                            index='VeÃ­culo',
                            columns=col_pivot,
                            values='Total',
                            aggfunc='sum',
                            fill_value=0
                        )

                    df_tabela_total.index.name = 'VeÃ­culo'
                    df_tabela_total = df_tabela_total.reset_index()

                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_total_veiculo = df_total_agr.groupby('VeÃ­culo', dropna=False).agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        df_total_veiculo['CPU_Total'] = np.where(
                            (df_total_veiculo['Volume'].notna()) & (df_total_veiculo['Volume'] != 0),
                            df_total_veiculo['Total'] / df_total_veiculo['Volume'],
                            0
                        )
                        df_tabela_total = pd.merge(
                            df_tabela_total,
                            df_total_veiculo[['VeÃ­culo', 'CPU_Total']],
                            on='VeÃ­culo',
                            how='left'
                        )
                        df_tabela_total.rename(columns={'CPU_Total': 'Total'}, inplace=True)
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['VeÃ­culo', 'Total']]
                    else:
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['VeÃ­culo']]
                        df_tabela_total['Total'] = df_tabela_total[colunas_periodos].sum(axis=1)
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['VeÃ­culo', 'Total']]

                    # Ordenar colunas de meses cronologicamente
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        try:
                            anos_unicos = sorted(df_total_agr['Ano'].dropna().unique().tolist())
                        except Exception:
                            anos_unicos = []
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in colunas_periodos:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in colunas_periodos if c not in colunas_ordenadas]
                        colunas_periodos = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_ordenadas = [c for c in meses_ordem if c in colunas_periodos]
                        colunas_restantes = [c for c in colunas_periodos if c not in meses_ordem]
                        colunas_periodos = colunas_ordenadas + colunas_restantes

                    # Reordenar DataFrame final
                    colunas_finais = ['VeÃ­culo'] + colunas_periodos + ['Total']
                    colunas_finais = [c for c in colunas_finais if c in df_tabela_total.columns]
                    df_tabela_total = df_tabela_total[colunas_finais]

                    df_tabela_total_fmt = df_tabela_total.copy()
                    for c in colunas_periodos + ['Total']:
                        if c in df_tabela_total_fmt.columns:
                            df_tabela_total_fmt[c] = df_tabela_total_fmt[c].map(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
                    st.dataframe(df_tabela_total_fmt, width="stretch")

                    if st.button(
                        "ðŸ“¥ Baixar Tabela Total por VeÃ­culo (Budget) (Excel)",
                        width="stretch",
                        key="download_tabela_total_veiculo_budget"
                    ):
                        with st.spinner("Gerando arquivo da tabela total (Budget)..."):
                            try:
                                downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                                tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                                file_name = f"TC_Ext_tabela_total_veiculo_Budget_{tipo_nome}.xlsx"
                                file_path = os.path.join(downloads_path, file_name)
                                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                                    df_tabela_total.to_excel(writer, index=False, sheet_name='Total_Veiculo_Budget')
                                st.success(f"âœ… Arquivo salvo com sucesso em: {file_path}")
                            except Exception as e:
                                st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")
                else:
                    st.info("â„¹ï¸ Colunas necessÃ¡rias nÃ£o encontradas para criar a tabela total de Budget.")

            # ------------------------------
            # Tabela filtrada (Budget)
            # ------------------------------
            titulo_filtrada_budget = (
                "ðŸ“‹ **Tabela Filtrada - CPU (Budget) (Todas as Linhas)**"
                if tipo_visualizacao == "CPU (Custo por Unidade)"
                else "ðŸ“‹ **Tabela Filtrada (Budget) (Todas as Linhas)**"
            )
            with st.expander(titulo_filtrada_budget, expanded=False):
                df_display_budget = df_visualizacao_budget.copy()
                colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                for col in colunas_para_remover:
                    if col in df_display_budget.columns:
                        df_display_budget = df_display_budget.drop(columns=[col])
                st.info(f"ðŸ“Š Exibindo todas as {len(df_display_budget):,} linhas e {len(df_display_budget.columns)} colunas")
                st.dataframe(df_display_budget, width="stretch")

                if st.button(
                    "ðŸ“¥ Baixar Tabela Filtrada (Budget) (Excel)",
                    width="stretch",
                    key="download_filtered_budget"
                ):
                    with st.spinner("Gerando arquivo da tabela filtrada (Budget)..."):
                        try:
                            downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                            file_name = "TC_Ext_tabela_filtrada_Budget.xlsx"
                            file_path = os.path.join(downloads_path, file_name)
                            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                                df_visualizacao_budget.to_excel(writer, index=False, sheet_name='Dados_Budget')
                            st.success(f"âœ… Arquivo salvo com sucesso em: {file_path}")
                        except Exception as e:
                            st.error(f"âŒ Erro ao salvar arquivo: {str(e)}")

# ==========================================
# TAB 6: Waterfall - MOVED TO pages/4 - Waterfall.py
# ==========================================
# O cÃ³digo do Waterfall foi movido para uma pÃ¡gina separada (pages/4 - Waterfall.py)
# O cÃ³digo completo do tab5 (linhas 9659-11008) foi extraÃ­do para a nova pÃ¡gina
# Removido: todo o cÃ³digo do tab5 foi movido para pages/4 - Waterfall.py

# Fechar bloco condicional do dashboard principal
# (O rodapÃ© abaixo serÃ¡ exibido em todas as pÃ¡ginas)

# FunÃ§Ã£o para obter mÃªs atual em portuguÃªs
def obter_mes_atual():
    """Retorna o mÃªs atual em portuguÃªs"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "MarÃ§o", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# RodapÃ©
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    ðŸ“š DocumentaÃ§Ã£o Completa do Sistema TC | VersÃ£o {versao_atual} | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)

