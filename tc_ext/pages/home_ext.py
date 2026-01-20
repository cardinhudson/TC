import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import altair as alt
import os
import numpy as np
import json
import sqlite3
from datetime import datetime
import plotly.graph_objects as go
from versionamento import obter_versao_atual, verificar_mudancas_paginas

# Camada core (refatoração incremental): helpers compartilhados, sem depender de app.py
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

# Verificar mudanças nas páginas e incrementar versão se necessário
verificar_mudancas_paginas()

# Configuração da página fica no app.py (roteador) para evitar chamadas duplicadas

# Função para obter mês atual em português
def obter_mes_atual():
    """Retorna o mês atual em português"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# Função para obter data e hora de atualização dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da última atualização dos arquivos de dados"""
    try:
        # Tentar múltiplos caminhos possíveis (para compatibilidade com diferentes ambientes)
        arquivos_dados = [
            # Caminhos do histórico consolidado
            os.path.join("dados", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join("dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
            # Caminhos alternativos (pode existir em diferentes estruturas)
            os.path.join("./dados", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("./dados", "historico_consolidado", "df_vol_historico.parquet"),
        ]
        
        # Também tentar buscar em pastas de anos recentes
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
                    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
                    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
            except (ValueError, OSError):
                return None
        return None
    except Exception:
        return None

# Cabeçalho compacto com data de atualização
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
data_atualizacao = obter_data_atualizacao_dados()

# Montar textos do cabeçalho
texto_esquerda = f"📚 Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin e Lauro Paiva"
texto_direita = f"📅 Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

st.markdown(f"""
<div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
    <div style='flex: 1;'>{texto_esquerda}</div>
    <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
</div>
""", unsafe_allow_html=True)


# CSS para reduzir títulos em 20% e evitar quebra de linha
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
        /* Alinhamento vertical para células de tabela */
        .stDataFrame table td {
            vertical-align: middle !important;
        }
        .stDataFrame table th {
            vertical-align: middle !important;
        }
        /* Estilos para botões: reduzir fonte e aproximar */
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
        /* Reduzir espaçamento dos radio buttons horizontais (exceto moeda) */
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) > div {
            gap: 0.25rem !important;
        }
        div[data-testid="stRadio"]:not([key*="moeda_selecionada"]) > div > label {
            padding: 0.15rem 0.35rem !important;
            margin-bottom: 0 !important;
        }
        /* Reduzir espaçamento entre colunas */
        .stColumn {
            padding-left: 0.2rem !important;
            padding-right: 0.2rem !important;
        }
        /* Eliminar espaçamento nas colunas de moeda - SEM interferir nos cliques */
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
        /* Forçar altura mínima do container do radio */
        div[data-testid="stRadio"] {
            min-height: auto !important;
            height: auto !important;
        }
        /* Reduzir espaçamento do título do radio */
        div[data-testid="stRadio"] > label > div {
            margin-bottom: 0.15rem !important;
            padding-bottom: 0 !important;
        }
        /* Compactar ainda mais os elementos das colunas */
        [data-testid="stColumn"] {
            /* Não force layout flex nas colunas (isso pode encolher gráficos) */
            align-items: stretch !important;
        }
        [data-testid="stColumn"] > div {
            width: 100% !important;
        }
        /* Garantir que os radio buttons não quebrem linha */
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
        /* Garantir que o container não corte o conteúdo */
        div[data-testid="stRadio"] {
            overflow: visible !important;
        }
        div[data-testid="stColumn"] {
            overflow: visible !important;
        }
        /* REMOVER qualquer interferência nos radio buttons de moeda */
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
        /* Ocultar botões de moeda ocultos */
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
        /* Ocultar containers dos botões de moeda */
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
        /* Garantir que o container pai não adicione texto */
        [data-testid="stMarkdownContainer"]:has(#flags-container-top) > *:not(#flags-container-top) {{
            display: none !important;
        }}
        /* Ocultar qualquer código JavaScript que apareça como texto */
        div:has(#flags-container-top) + *,
        div:has(#flags-container-top) ~ * {{
            display: none !important;
        }}
        /* Ocultar texto JavaScript específico */
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
        /* Ocultar texto específico "})();" - mas NÃO interferir nos radio buttons */
        body *:not(script):not(style):not(#flags-container-top):not([data-testid="stRadio"]):not([data-testid="stRadio"] *) {{
            position: relative !important;
        }}
        body *:not(script):not(style):not(#flags-container-top)::before,
        body *:not(script):not(style):not(#flags-container-top)::after {{
            content: none !important;
        }}
        /* REMOVER qualquer interferência - deixar Streamlit gerenciar normalmente */
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
        /* Garantir que R$ não seja quebrado - forçar renderização completa */
        div[data-testid="stNumberInput"] label p {
            letter-spacing: 0 !important;
            word-spacing: normal !important;
        }
        /* CSS específico para garantir que o $ não seja cortado */
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
        /* Garantir que o container não corte o texto */
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"],
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] {
            overflow: visible !important;
        }
        div[data-testid="stNumberInput"][key="taxa_usd_para_brl_input"] > div,
        div[data-testid="stNumberInput"][key="taxa_eur_para_brl_input"] > div {
            overflow: visible !important;
        }
        /* Forçar que o label completo seja visível */
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
        /* Específico para garantir que o parágrafo com R$ seja completo */
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

# Verificar se estamos na página principal (app.py) e não em uma página separada
# IMPORTANTE: Verificar ANTES de renderizar qualquer conteúdo do dashboard
is_main_page = True
try:
    import os
    # Verificar pelo nome do arquivo diretamente (mais confiável)
    current_file_name = os.path.basename(__file__)
    if current_file_name in ('app.py', 'home_ext.py'):
        is_main_page = True
    else:
        # Verificar pelo caminho completo
        current_file = os.path.abspath(__file__)
        # Verificar se o arquivo atual está na pasta pages
        if current_file and ('pages' in current_file.replace('\\', '/') or 'pages/' in current_file.replace('\\', '/')):
            is_main_page = False
        # Verificar se há flag no session_state indicando página separada (ex: Waterfall)
        if 'is_waterfall_page' in st.session_state and st.session_state.is_waterfall_page:
            is_main_page = False
except Exception as e:
    # Em caso de erro, assumir que estamos na página principal
    is_main_page = True

# Verificação adicional: garantir que no app.py sempre seja True
try:
    import os
    current_file_name = os.path.basename(__file__)
    if current_file_name in ('app.py', 'home_ext.py'):
        is_main_page = True
    # Se não estamos em pages, forçar is_main_page = True
    elif not is_main_page:
        current_file_check = os.path.abspath(__file__)
        if current_file_check and 'pages' not in current_file_check.replace('\\', '/'):
            is_main_page = True
except:
    # Em caso de erro, assumir página principal
    is_main_page = True

if is_main_page:
    # Título - Movido para o topo da página
    st.title("🏭 Dashboard TC Extendido Porto Real")
    st.subheader("Análise de dados agrupados por Oficina e Período")

    st.markdown("---")

    # Inicializar estado se não existir
    if 'moeda_selecionada' not in st.session_state:
        st.session_state.moeda_selecionada = "🇧🇷 R$"
    # Inicializar moeda_selecionada_radio também para evitar erro no callback
    if 'moeda_selecionada_radio' not in st.session_state:
        st.session_state.moeda_selecionada_radio = "🇧🇷 R$"

    # URLs das bandeiras
    bandeira_brasil_url = "https://flagcdn.com/br.svg"
    bandeira_eua_url = "https://flagcdn.com/us.svg"
    bandeira_europa_url = "https://flagcdn.com/eu.svg"

    # Seleção de moeda com bandeiras ao lado (sem botões, apenas visual)
    col_moeda1, col_moeda2 = st.columns([3, 1])

    with col_moeda1:
        st.markdown("💱 **Moeda:**", unsafe_allow_html=True)
        opcoes_moeda = ["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"]
        
        # SEMPRE usar o valor mais atual do session_state para calcular o índice
        moeda_atual_para_index = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
        index_moeda = opcoes_moeda.index(moeda_atual_para_index) if moeda_atual_para_index in opcoes_moeda else 0
        
        # Função callback para garantir sincronização imediata
        def atualizar_moeda():
            # O valor já está em st.session_state.moeda_selecionada_radio após o clique
            # Verificar se a chave existe antes de acessar
            if 'moeda_selecionada_radio' in st.session_state:
                st.session_state.moeda_selecionada = st.session_state.moeda_selecionada_radio
        
        moeda_selecionada = st.radio(
            "Moeda",
            opcoes_moeda,
            index=index_moeda,
            horizontal=True,
            help="Selecione a moeda para exibição nos gráficos",
            key="moeda_selecionada_radio",
            label_visibility="collapsed",
            on_change=atualizar_moeda
        )
        
        # Garantir que o estado esteja sincronizado (backup caso on_change não funcione)
        if st.session_state.moeda_selecionada != moeda_selecionada:
            st.session_state.moeda_selecionada = moeda_selecionada

    # Obter moeda atual do session_state (sempre atualizado)
    moeda_atual = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
    flag_selecionada_brl = moeda_atual == '🇧🇷 R$'
    flag_selecionada_usd = moeda_atual == '🇺🇸 $'
    flag_selecionada_eur = moeda_atual == '🇪🇺 €'

    with col_moeda2:
        st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento vertical
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

# Funções de banco de dados SQLite (definir ANTES de usar - disponíveis para todas as páginas)
def inicializar_banco_taxas():
    """Cria o banco de dados e tabela para taxas de câmbio se não existir."""
    _core_inicializar_banco_taxas()

def carregar_taxas_banco():
    """Carrega as taxas de câmbio do banco de dados SQLite."""
    return _core_carregar_taxas_banco()

def salvar_taxas_banco(taxas):
    """Salva as taxas de câmbio no banco de dados SQLite."""
    _core_salvar_taxas_banco(taxas)

# Função auxiliar para listar anos disponíveis (definir ANTES de usar)
def listar_anos_disponiveis():
    """Lista todos os anos disponíveis nas pastas de dados."""
    return _core_listar_anos_disponiveis()

# Botões para alternar tema (no topo da sidebar)
st.sidebar.markdown("---")
st.sidebar.markdown("**🎨 Tema**")

# Função para salvar tema no config.toml
def save_theme_to_config(theme_name):
    """Salva o tema no config.toml"""
    import os
    import toml
    
    config_path = os.path.join(".streamlit", "config.toml")
    try:
        # Ler configuração atual
        with open(config_path, 'r') as f:
            config = toml.load(f)
        
        # Atualizar tema
        if 'theme' not in config:
            config['theme'] = {}
        config['theme']['base'] = theme_name
        
        # Cores específicas para cada tema
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
        
        # Salvar configuração
        with open(config_path, 'w') as f:
            toml.dump(config, f)
        
        return True
    except Exception as e:
        st.sidebar.error(f"❌ Erro: {str(e)}")
        return False

# Função para ler tema atual
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

# Ler tema atual (uma vez por sessão)
if 'current_saved_theme' not in st.session_state:
    st.session_state.current_saved_theme = get_current_theme()

# Mostrar tema atual
st.sidebar.caption(f"Tema ativo: **{st.session_state.current_saved_theme.upper()}**")

# Criar duas colunas para os botões
col_dark, col_light = st.sidebar.columns(2)

# Inicializar flag de mensagem
if 'show_reload_message' not in st.session_state:
    st.session_state.show_reload_message = False
if 'theme_needs_reload' not in st.session_state:
    st.session_state.theme_needs_reload = False

# Botão Dark Mode (Lua)
with col_dark:
    if st.button("🌙 Dark", key="btn_dark", help="Ativar Dark Mode", width="stretch"):
        if save_theme_to_config('dark'):
            st.session_state.current_saved_theme = 'dark'
            st.session_state.show_reload_message = True
            st.session_state.theme_needs_reload = True

# Botão Light Mode (Sol)
with col_light:
    if st.button("☀️ Light", key="btn_light", help="Ativar Light Mode", width="stretch"):
        if save_theme_to_config('light'):
            st.session_state.current_saved_theme = 'light'
            st.session_state.show_reload_message = True
            st.session_state.theme_needs_reload = True

# Mostrar mensagem se tema foi alterado
if st.session_state.show_reload_message:
    st.sidebar.success(f"✅ Tema **{st.session_state.current_saved_theme.upper()}** salvo!")
    st.sidebar.info("🔄 Aplicando tema...")
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
st.sidebar.markdown("**📅 Seleção de Ano**")

# Listar anos disponíveis
anos_disponiveis = listar_anos_disponiveis()
opcoes_ano = ["Todos"] + [str(ano) for ano in anos_disponiveis]

# Determinar índice padrão: ano atual se disponível, senão "Todos" (índice 0)
from datetime import datetime
ano_atual = datetime.now().year
ano_atual_str = str(ano_atual)
if ano_atual_str in opcoes_ano:
    index_padrao = opcoes_ano.index(ano_atual_str)
else:
    index_padrao = 0  # "Todos" se ano atual não estiver disponível

# Inicializar session_state para manter valores dos filtros
if 'filtro_ano_tc_ext' not in st.session_state:
    st.session_state.filtro_ano_tc_ext = opcoes_ano[index_padrao] if index_padrao < len(opcoes_ano) else "Todos"

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=opcoes_ano.index(st.session_state.filtro_ano_tc_ext) if st.session_state.filtro_ano_tc_ext in opcoes_ano else index_padrao,
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico",
    key="filtro_ano_tc_ext_selectbox"
)
# Atualizar session_state
st.session_state.filtro_ano_tc_ext = ano_selecionado

# Função para carregar dados com cache (disponível para todas as páginas - deve estar antes do uso)
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet - SEMPRE do histórico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histórico consolidado para garantir consistência
        # Apenas aplicar filtro de ano quando necessário
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
        caminho_absoluto = os.path.abspath(caminho_historico)
        
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
        else:
            st.error(f"❌ Arquivo de histórico consolidado não encontrado: {caminho_absoluto}")
            st.info("💡 Execute o dados.ipynb para gerar o histórico consolidado")
            st.stop()
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
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
        st.error(f"❌ Erro ao carregar dados: {str(e)}")
        st.stop()

# Função auxiliar para obter opções de filtro (disponível para todas as páginas - deve estar antes do uso)
@st.cache_data(ttl=1800, max_entries=5)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache"""
    if column_name in df.columns:
        opcoes = sorted(
            df[column_name].dropna().astype(str).unique().tolist()
        )
        return ["Todos"] + opcoes
    return ["Todos"]

# Continuar apenas se estivermos na página principal
if is_main_page:
    # Carregar taxas do banco de dados para usar na página principal
    try:
        taxas_cambio_banco = carregar_taxas_banco()
    except Exception as e:
        taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

    # Taxas de conversão: entrada em "1 $ = R$ X" e "1 € = R$ X"
    taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
    taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

    # Seção de Taxas de Câmbio (seguindo o mesmo padrão dos outros blocos)
    st.markdown("📝 **Entrada de Taxas:**", unsafe_allow_html=True)

    # Criar colunas para as taxas
    # Criar colunas para as taxas (ajustar proporção para evitar corte de texto)
    col_taxa1, col_taxa2 = st.columns([1.1, 1.1], gap="small")

    with col_taxa1:
        # Usar markdown para o label e campo sem label para evitar corte
        st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇺🇸 1 $ (USD) = R$</p>', unsafe_allow_html=True)
        taxa_usd_para_brl = st.number_input(
            "Taxa USD para BRL",
            min_value=0.01,
            max_value=100.0,
            value=float(taxa_usd_para_brl_padrao),
            step=0.01,
            format="%.2f",
            help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros. Exemplo: se 1 USD = 5.00 BRL, digite 5.00",
            key="taxa_usd_para_brl_input",
            label_visibility="collapsed"
        )

    with col_taxa2:
        # Usar markdown para o label e campo sem label para evitar corte
        st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇪🇺 1 € (EUR) = R$</p>', unsafe_allow_html=True)
        taxa_eur_para_brl = st.number_input(
            "Taxa EUR para BRL",
            min_value=0.01,
            max_value=100.0,
            value=float(taxa_eur_para_brl_padrao),
            step=0.01,
            format="%.2f",
            help="Digite quanto vale 1 Euro em Reais Brasileiros. Exemplo: se 1 EUR = 5.50 BRL, digite 5.50",
            key="taxa_eur_para_brl_input",
            label_visibility="collapsed"
        )

    # Calcular taxas inversas para conversão (1 R$ = X USD/EUR)
    taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
    taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

    # Salvar taxas quando alteradas
    # Usar session_state para evitar salvar múltiplas vezes na mesma execução
    taxa_usd_atual_key = "taxa_usd_atual_salva"
    taxa_eur_atual_key = "taxa_eur_atual_salva"

    # Verificar se as taxas mudaram desde a última vez que foram salvas
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
            st.error(f"❌ Erro ao salvar taxas: {e}")

    # Armazenar taxas em dicionário (para conversão: 1 R$ = X USD/EUR)
    # IMPORTANTE: Estas taxas são para MULTIPLICAR valores em BRL
    # Exemplo: Se taxa_brl_para_usd = 0.20, então 100 BRL * 0.20 = 20 USD
    # Isso é equivalente a: 100 BRL / 5 = 20 USD (onde 5 é taxa_usd_para_brl)
    taxas_cambio = {
        "BRL": 1.0,  # Real é a moeda base
        "USD": taxa_brl_para_usd,  # Ex: 0.20 (se 1 USD = 5 BRL, então 1 BRL = 0.20 USD)
        "EUR": taxa_brl_para_eur   # Ex: 0.18 (se 1 EUR = 5.50 BRL, então 1 BRL = 0.18 EUR)
    }

    # Seletores no topo da página (layout horizontal compacto - mesma linha)
    col_tipo, col_fator = st.columns([1.3, 1.2], gap="small")

    with col_tipo:
        tipo_visualizacao = st.radio(
            "📊 **Tipo:**",
            ["Custo Total", "CPU (Custo por Unidade)"],
            index=0,
            horizontal=True,
            key="tipo_visualizacao_top"
        )

    with col_fator:
        if tipo_visualizacao == "Custo Total":
            fator_conversao = st.radio(
                "🔢 **Fator:**",
                ["Nenhum", "K (milhares)", "M (Milhões)"],
                index=1,
                horizontal=True,
                help="Aplica divisão aos valores para simplificar visualização. Não afeta cálculos.",
                key="fator_conversao_top"
            )
        else:
            fator_conversao = None

    # Obter a moeda selecionada do session state (já está atualizado acima)
    moeda_selecionada = st.session_state.get('moeda_selecionada', '🇧🇷 R$')

    # Extrair código e símbolo da moeda
    if moeda_selecionada == "🇧🇷 R$":
        moeda_codigo = "BRL"
        moeda_simbolo = "R$"
    elif moeda_selecionada == "🇺🇸 $":
        moeda_codigo = "USD"
        moeda_simbolo = "$"
    elif moeda_selecionada == "🇪🇺 €":
        moeda_codigo = "EUR"
        moeda_simbolo = "€"
    else:
        # Fallback
        moeda_codigo = "BRL"
        moeda_simbolo = "R$"

    st.markdown("---")

    # Teste de validação da conversão (mostrar exemplo)
    if moeda_codigo != "BRL":
        valor_teste = 100.0
        valor_convertido = _core_converter_moeda(valor_teste, moeda_codigo, taxas_cambio)
        if moeda_codigo == "USD":
            taxa_esperada = taxa_usd_para_brl
            valor_esperado_divisao = valor_teste / taxa_esperada
            st.sidebar.info(f"💡 Teste conversão: R$ {valor_teste:,.2f} = {moeda_simbolo} {valor_convertido:,.2f} (taxa: 1 {moeda_simbolo} = R$ {taxa_esperada:.2f})")
            st.sidebar.caption(f"✅ Validação: {valor_teste:,.2f} / {taxa_esperada:.2f} = {valor_esperado_divisao:,.2f} (deve ser igual a {valor_convertido:,.2f})")
        else:  # EUR
            taxa_esperada = taxa_eur_para_brl
            valor_esperado_divisao = valor_teste / taxa_esperada
            st.sidebar.info(f"💡 Teste conversão: R$ {valor_teste:,.2f} = {moeda_simbolo} {valor_convertido:,.2f} (taxa: 1 {moeda_simbolo} = R$ {taxa_esperada:.2f})")
            st.sidebar.caption(f"✅ Validação: {valor_teste:,.2f} / {taxa_esperada:.2f} = {valor_esperado_divisao:,.2f} (deve ser igual a {valor_convertido:,.2f})")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔍 Filtros**")

    # Carregar dados com o ano selecionado
    try:
        df_total = load_data(ano_selecionado)
        # Evitar mutações no cache
        if df_total is not None:
            df_total = df_total.copy()
        
        # Verificar se df_total foi carregado corretamente
        if df_total is None:
            st.error("❌ Erro: Nenhum dado foi carregado (df_total é None)")
            st.stop()
        
        if df_total.empty:
            st.error("❌ Erro: DataFrame carregado está vazio")
            st.stop()
    except Exception as e:
        st.error(f"❌ Erro: {str(e)}")
        import traceback
        st.error(f"Detalhes: {traceback.format_exc()}")
        st.stop()

    # Aplicar fator de conversão nas colunas Total e BUD (antes de qualquer processamento)
    # Isso simplifica os cálculos pois o fator é aplicado uma única vez na origem
    # Mantém os dados na mesma unidade para comparações consistentes
    # 🔧 CORREÇÃO CRÍTICA: NÃO aplicar fator de conversão quando está em modo CPU
    # No modo CPU, o fator não deve ser aplicado pois CPU já é uma razão (Total/Volume)
    if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
        if fator_conversao == "K (milhares)":
            if 'Total' in df_total.columns:
                df_total['Total'] = df_total['Total'] / 1000
        elif fator_conversao == "M (Milhões)":
            if 'Total' in df_total.columns:
                df_total['Total'] = df_total['Total'] / 1000000

    # Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
    # Isso garante que todos os dados derivados já terão a conversão aplicada
    # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
    # IMPORTANTE: Aplicar conversão em AMBOS os modos (Custo Total e CPU)
    # No modo CPU, o Total convertido será usado para calcular CPU = Total convertido / Volume
    if moeda_codigo != "BRL" and 'Total' in df_total.columns:
        df_total = _core_converter_coluna_moeda(df_total, 'Total', moeda_codigo, taxas_cambio)

    # Inicializar session_state para filtros
    if 'filtro_oficina_tc_ext' not in st.session_state:
        st.session_state.filtro_oficina_tc_ext = ["Todos"]

    # Filtro 1: Oficina (com cache otimizado)
    if 'Oficina' in df_total.columns:
        oficina_opcoes = get_filter_options(df_total, 'Oficina')
        # Validar valores salvos
        default_oficina = st.session_state.filtro_oficina_tc_ext if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_tc_ext) else ["Todos"]
        oficina_selecionadas = st.sidebar.multiselect(
            "Selecione a Oficina:", oficina_opcoes, default=default_oficina, key="filtro_oficina_tc_ext_multiselect"
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

    # Filtro 2: Veículo
    if 'filtro_veiculo_tc_ext' not in st.session_state:
        st.session_state.filtro_veiculo_tc_ext = ["Todos"]
    
    if 'Veículo' in df_filtrado.columns:
        veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
        default_veiculo = st.session_state.filtro_veiculo_tc_ext if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_tc_ext) else ["Todos"]
        veiculo_selecionados = st.sidebar.multiselect(
            "Selecione o Veículo:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_tc_ext_multiselect"
        )
        st.session_state.filtro_veiculo_tc_ext = veiculo_selecionados if veiculo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no Veículo
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
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

    # Filtro 4: Período
    if 'filtro_periodo_tc_ext' not in st.session_state:
        st.session_state.filtro_periodo_tc_ext = ["Todos"]
    
    if 'Período' in df_filtrado.columns:
        periodo_opcoes = get_filter_options(df_filtrado, 'Período')
        # Ordenar períodos cronologicamente
        ordem_meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 
                      'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        periodo_opcoes_ordenados = ["Todos"]
        for mes in ordem_meses:
            if mes in periodo_opcoes:
                periodo_opcoes_ordenados.append(mes)
        # Adicionar outros períodos que não são meses
        for periodo in periodo_opcoes:
            if periodo != "Todos" and periodo not in periodo_opcoes_ordenados:
                periodo_opcoes_ordenados.append(periodo)
        
        default_periodo = st.session_state.filtro_periodo_tc_ext if all(x in periodo_opcoes_ordenados for x in st.session_state.filtro_periodo_tc_ext) else ["Todos"]
        periodo_selecionados = st.sidebar.multiselect(
            "Selecione o Período:", periodo_opcoes_ordenados, default=default_periodo, key="filtro_periodo_tc_ext_multiselect"
        )
        st.session_state.filtro_periodo_tc_ext = periodo_selecionados if periodo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no Período
        if periodo_selecionados and "Todos" not in periodo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['Período'].astype(str).isin(periodo_selecionados)
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

    # Filtros avançados (expansível)
    with st.sidebar.expander("🔍 Filtros Avançados"):
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
                # Inicializar session_state para cada filtro avançado
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

# Função auxiliar para encontrar arquivo parquet na ordem de prioridade (disponível para todas as páginas)
def encontrar_arquivo_parquet(nome_arquivo, ano_selecionado=None):
    """
    Busca arquivo parquet na seguinte ordem de prioridade:
    1. Se ano_selecionado for None ou "Todos": Histórico consolidado (dados/historico_consolidado/)
    2. Se ano_selecionado for especificado: Pasta do ano (dados/{ANO}/)
    3. Pasta do ano mais recente (dados/{ANO}/)
    4. Raiz do projeto (compatibilidade)
    """
    return _core_encontrar_arquivo_parquet(nome_arquivo, ano_selecionado)


# Função para converter valor de R$ para outra moeda
def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino."""
    return _core_converter_moeda(valor, moeda_destino, taxas)

# Função para converter coluna inteira de DataFrame
def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda."""
    return _core_converter_coluna_moeda(df, coluna, moeda_destino, taxas)

# Função para obter símbolo da moeda (disponível para todas as páginas)
def obter_simbolo_moeda(moeda_codigo):
    """Retorna o símbolo da moeda."""
    return _core_obter_simbolo_moeda(moeda_codigo)

# Função para carregar dados com cache (disponível para todas as páginas)
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet - SEMPRE do histórico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histórico consolidado para garantir consistência
        # Apenas aplicar filtro de ano quando necessário
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
        caminho_absoluto = os.path.abspath(caminho_historico)
        
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
        else:
            st.error(f"❌ Arquivo de histórico consolidado não encontrado: {caminho_absoluto}")
            st.info("💡 Execute o dados.ipynb para gerar o histórico consolidado")
            st.stop()
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
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
        st.error(f"❌ Erro ao carregar dados: {str(e)}")
        st.stop()


# Função para carregar dados de volume com cache
@st.cache_data(
    ttl=60,  # 🔧 REDUZIDO para 60 segundos para forçar atualização mais frequente
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_volume_data(ano_selecionado_param):
    """Carrega os dados de volume do arquivo parquet - SEMPRE do histórico consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do histórico consolidado para garantir consistência
        # Apenas aplicar filtro de ano quando necessário
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
        
        if os.path.exists(caminho_historico):
            # 🔧 CORREÇÃO: Garantir que Volume seja sempre numérico ao carregar
            df = pd.read_parquet(caminho_historico)
            if 'Volume' in df.columns:
                df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
        else:
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
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


def filtrar_volume_com_sidebar(df_vol, df_total_base):
    """Aplica os filtros da sidebar aos dados de volume."""
    if df_vol is None:
        return None

    df_vol_filtrado = df_vol.copy()

    # Filtro 1: Oficina
    if 'Oficina' in df_vol_filtrado.columns and df_total_base is not None:
        oficina_opcoes_disponiveis = get_filter_options(df_total_base, 'Oficina')
        oficina_opcoes_disponiveis = [o for o in oficina_opcoes_disponiveis if o != "Todos"]
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

    # Filtro 2: Veículo
    if 'Veículo' in df_vol_filtrado.columns:
        veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])
        if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_sidebar)
            ].copy()

    # Filtro 3: USI
    if 'USI' in df_vol_filtrado.columns:
        usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_ext', ["Todos"])
        if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['USI'].astype(str).isin(usi_selecionada_sidebar)
            ].copy()

    # Filtro 5: Centro cst
    if 'Centrocst' in df_vol_filtrado.columns:
        centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_ext', "Todos")
        if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
            ].copy()

    # Filtro 6: Conta contábil
    if 'Nºconta' in df_vol_filtrado.columns:
        conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_ext', [])
        if conta_contabil_selecionadas_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Nºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
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

    # Filtros avançados
    filtros_avancados_nomes = ["Usuário", "Material", "Dt.lçto.", "Texto breve", "Account"]
    for col_name in filtros_avancados_nomes:
        if col_name in df_vol_filtrado.columns:
            filtro_key = f'filtro_avancado_{col_name}_tc_ext'
            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                df_vol_filtrado = df_vol_filtrado[
                    df_vol_filtrado[col_name].astype(str).isin(selecionadas_sidebar)
                ].copy()

    return df_vol_filtrado


# Função para carregar dados de budget (Total) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_data(ano_selecionado_param):
    """Carrega os dados de budget do arquivo parquet - SEMPRE do histórico consolidado BUD"""
    try:
        caminho_budget = os.path.join("dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet")
        
        if os.path.exists(caminho_budget):
            df = pd.read_parquet(caminho_budget)
        else:
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
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


# Função para carregar dados de budget (Volume) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_volume_data(ano_selecionado_param):
    """Carrega os dados de volume de budget do arquivo parquet - SEMPRE do histórico consolidado BUD"""
    try:
        caminho_budget_vol = os.path.join("dados", "historico_consolidado", "BUD", "df_vol_historico_BUD.parquet")
        
        if os.path.exists(caminho_budget_vol):
            df = pd.read_parquet(caminho_budget_vol)
        else:
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
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

# Ordem dos meses para ordenação cronológica (disponível para todas as páginas)
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]

# (Código de filtros movido para dentro do bloco if is_main_page:)


def formatar_ratio_com_barra(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com barra de progresso em HTML"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular largura da barra: 100% = barra cheia, acima de 100% também fica cheia
    if percentual >= 100:
        largura_barra = 100  # Barra cheia para 100% ou mais
    else:
        largura_barra = percentual  # Proporcional até 100%
    
    # Calcular cor: verde até 90%, depois gradiente até vermelho em 100%
    if percentual <= 0:
        r, g, b = 0, 170, 0  # Verde (#00AA00)
    elif percentual <= 90:
        r, g, b = 0, 170, 0  # Verde puro até 90%
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
    
    # Detectar tema para adaptar cor do texto (igual às outras colunas)
    try:
        theme_base = st.get_option("theme.base") or "light"
        # Usar a mesma cor que o Streamlit usa para texto em tabelas
        # Dark mode: rgb(250, 250, 250) ou #FAFAFA
        # Light mode: rgb(49, 51, 63) ou #31333F (cor padrão do Streamlit para texto)
        if theme_base == "dark":
            texto_cor = "#FAFAFA"  # Branco claro para dark mode
        else:
            texto_cor = "#31333F"  # Cinza escuro para light mode (cor padrão do Streamlit)
    except:
        # Fallback: tentar detectar via CSS do Streamlit
        texto_cor = "var(--text-color, #31333F)"  # Usar variável CSS se disponível, senão usar cor padrão
    
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
    """Cria uma tabela HTML customizada no padrão Streamlit para renderizar HTML nas células
    
    Args:
        df_display: DataFrame com os dados a serem exibidos
        linha_resumo: Dicionário opcional com valores de resumo formatados para adicionar como primeira linha
        linha_volumes: Dicionário opcional com volumes para adicionar como última linha (ex: {'Volume Real': '1,000', 'Volume Budget': '1,200'})
    """
    # Usar o padrão de estilos do Streamlit para st.dataframe
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            # Cores transparentes no padrão Streamlit dark mode
            header_bg = "rgba(38, 39, 48, 0.15)"  # Cabeçalho mais transparente
            resumo_bg = "rgba(38, 39, 48, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(250, 250, 250, 0.1)"
        else:
            # Cores transparentes no padrão Streamlit light mode
            header_bg = "rgba(240, 242, 246, 0.15)"  # Cabeçalho mais transparente
            resumo_bg = "rgba(240, 242, 246, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(49, 51, 63, 0.1)"
    except:
        header_bg = "rgba(38, 39, 48, 0.15)"
        resumo_bg = "rgba(38, 39, 48, 0.15)"
        row_bg = "transparent"
        border_color = "rgba(250, 250, 250, 0.1)"
    
    # Criar tabela no padrão Streamlit
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

    # Cabeçalho
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
                # O valor já deve estar formatado como HTML (com barrinha e percentual)
                # Se não estiver formatado, formatar agora
                valor_celula = row[col]
                if isinstance(valor_celula, str) and '<div' in valor_celula:
                    # Já está formatado como HTML
                    html_table += f"<td class='total-flex-bud-col'>{valor_celula}</td>"
                else:
                    # Formatar agora se ainda não estiver formatado
                    valor_num = float(valor_celula) if pd.notna(valor_celula) and isinstance(valor_celula, (int, float)) else 0
                    html_formatado = formatar_ratio_com_barra(valor_num)
                    html_table += f"<td class='total-flex-bud-col'>{html_formatado}</td>"
            else:
                valor_celula = str(row[col])
                if any(char.isdigit() or char in ['$', '€', 'R$', ',', '.', 'K', 'M'] for char in valor_celula):
                    html_table += f"<td class='number-cell'>{valor_celula}</td>"
                else:
                    html_table += f"<td>{valor_celula}</td>"
        html_table += "</tr>"
    
    # Linha de resumo removida - os resumos agora são exibidos separadamente com caixas de texto
    
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
    """Formata período para formato abreviado (ex: Setembro 2024 -> Set/24 ou Set/2024 se usar_ano_completo=True)
    
    Args:
        periodo_str: String do período (ex: "Setembro 2024", "Total 2024", "2024 S1", "2024 Q1")
        ano: Ano opcional (se None, será extraído de periodo_str)
        usar_ano_completo: Se True, usa ano com 4 dígitos (para Ano a Ano, Semestre, Quarter)
    """
    # Mapeamento de meses para abreviações
    meses_abrev = {
        'janeiro': 'Jan', 'fevereiro': 'Fev', 'março': 'Mar', 'abril': 'Abr',
        'maio': 'Mai', 'junho': 'Jun', 'julho': 'Jul', 'agosto': 'Ago',
        'setembro': 'Set', 'outubro': 'Out', 'novembro': 'Nov', 'dezembro': 'Dez'
    }
    
    periodo_str = str(periodo_str).strip()
    mes_abrev = None
    ano_extraido = None
    
    # Verificar se é formato especial (Ano a Ano, Semestre, Quarter)
    # Exemplos: "Total 2024", "2024 S1", "2024 Q1"
    if periodo_str.startswith('Total '):
        # Formato: "Total 2024" → retornar "Total/2024"
        partes = periodo_str.split(' ', 1)
        if len(partes) > 1:
            ano_str = partes[1].strip()
            if ano_str.isdigit():
                return f"Total/{ano_str}"
        return "Total"
    elif ' S' in periodo_str:
        # Formato: "2024 S1" ou "2024 S2" → retornar "2024/1" ou "2024/2"
        partes = periodo_str.split(' S')
        if len(partes) == 2:
            ano_str = partes[0].strip()
            semestre = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{semestre}"
        return periodo_str
    elif ' Q' in periodo_str:
        # Formato: "2024 Q1", "2024 Q2", etc. → retornar "2024/1", "2024/2", etc.
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
                # Tentar extrair ano (pode ser apenas número)
                if ano_str.isdigit():
                    ano_extraido = int(ano_str)
                # Se não for apenas número, tentar extrair primeiro número encontrado
                elif any(c.isdigit() for c in ano_str):
                    # Extrair primeiro sequência de dígitos
                    numero_str = ''.join([c for c in ano_str if c.isdigit()])[:4]  # Limitar a 4 dígitos
                    if numero_str:
                        ano_extraido = int(numero_str)
            
            # Obter abreviação do mês
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
        else:
            mes_nome = periodo_str.lower().strip()
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
    
    # Usar ano fornecido como parâmetro ou o extraído
    if ano is not None:
        ano_final = ano
    elif ano_extraido is not None:
        ano_final = ano_extraido
    else:
        ano_final = None
    
    # Formatar resultado
    if mes_abrev:
        if ano_final:
            # Usar últimos 2 dígitos para meses normais
            ano_abrev = str(ano_final)[-2:]
            return f"{mes_abrev}/{ano_abrev}"
        else:
            return mes_abrev
    else:
        return periodo_str

def reordenar_colunas_padrao(colunas_numericas):
    """Reordena colunas numéricas na ordem padrão: BUD, Flex Bud - BUD, Flex BUD, Total - Flex Bud, Total, Total / Flex Bud"""
    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
    colunas_ordenadas = []
    for col_ordem in ordem_colunas:
        if col_ordem in colunas_numericas:
            colunas_ordenadas.append(col_ordem)
    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
    for col in colunas_numericas:
        if col not in colunas_ordenadas:
            colunas_ordenadas.append(col)
    return colunas_ordenadas

def reorganizar_colunas_por_periodo(df_tabela_flex, periodos_selecionados, tipo_visualizacao):
    """Reorganiza a tabela para mostrar colunas por período na ordem de seleção"""
    if len(periodos_selecionados) <= 1 or 'Período' not in df_tabela_flex.columns:
        # Se há apenas 1 período ou não há coluna Período, retornar tabela original
        return df_tabela_flex
    
    # Manter a ordem de seleção dos períodos
    periodos_ordenados = periodos_selecionados.copy()
    
    # Criar lista de colunas na ordem especificada
    colunas_finais = []
    
    # Colunas de identificação (Type 05, Type 06, Account, etc.)
    colunas_id = []
    for col in ['Type 05', 'Type 06', 'Account', 'Custo']:
        if col in df_tabela_flex.columns:
            colunas_id.append(col)
    
    colunas_finais.extend(colunas_id)
    
    # Para cada período na ordem de seleção
    primeiro_periodo = periodos_ordenados[0]
    primeiro_periodo_abrev = formatar_periodo_abreviado(primeiro_periodo)
    
    # Primeiro período: Total, Flex (removendo coluna redundante "Flex - Total")
    colunas_finais.append(f"{primeiro_periodo_abrev}")
    colunas_finais.append(f"Flex {primeiro_periodo_abrev.lower()}")
    
    # Demais períodos: Período - Flex primeiro, Período, % Período/Flex primeiro
    for periodo in periodos_ordenados[1:]:
        periodo_abrev = formatar_periodo_abreviado(periodo)
        colunas_finais.append(f"{periodo_abrev} - Flex {primeiro_periodo_abrev.lower()}")
        colunas_finais.append(f"{periodo_abrev.lower()}")
        colunas_finais.append(f"% {periodo_abrev.lower()}/flex {primeiro_periodo_abrev.lower()}")
    
    # Criar DataFrame pivot por período
    # Primeiro, precisamos ter os dados separados por período
    # Vou criar uma estrutura que agrupa por categoria e período
    colunas_agrupamento = [col for col in ['Type 05', 'Type 06', 'Account', 'Custo'] if col in df_tabela_flex.columns]
    
    # Se não houver dados separados por período, retornar tabela original
    if 'Período' not in df_tabela_flex.columns or df_tabela_flex['Período'].nunique() <= 1:
        return df_tabela_flex
    
    # Criar pivot table com períodos como colunas
    df_pivot = df_tabela_flex.pivot_table(
        index=colunas_agrupamento if colunas_agrupamento else ['Type 06'],
        columns='Período',
        values=['Total', 'Flex BUD', 'BUD'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Flatten column names
    df_pivot.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else str(col) for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Reorganizar colunas conforme especificado
    # Por enquanto, retornar a estrutura pivot básica
    # A reorganização completa será feita na exibição
    return df_pivot

def calcular_resumo_tabela_flex(df_original, tipo_visualizacao, moeda_simbolo, fator_conversao=None):
    """Calcula linha de resumo (totais) para tabela Flex Bud
    
    Args:
        df_original: DataFrame com valores numéricos originais (antes da formatação)
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        moeda_simbolo: Símbolo da moeda (R$, $, €)
        fator_conversao: Fator de conversão opcional (K, M)
    
    Returns:
        Dicionário com valores de resumo formatados (valores numéricos e formatados)
    """
    linha_resumo = {}
    linha_resumo_formatado = {}
    
    # Primeira coluna: "TOTAL"
    primeira_col = df_original.columns[0]
    linha_resumo[primeira_col] = "**TOTAL**"
    linha_resumo_formatado[primeira_col] = "**TOTAL**"
    
    # 🔧 CORREÇÃO: Para CPU, recalcular usando valores em Custo Total se disponíveis
    # (mesma lógica do gráfico - não somar valores em CPU diretamente)
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        # Verificar se temos colunas auxiliares para recalcular corretamente
        if '_Flex_Bud_Total' in df_original.columns and '_Total_Custo_Total' in df_original.columns and '_Volume_Real' in df_original.columns:
            # 🔧 CORREÇÃO CRÍTICA: O gráfico calcula Flex Bud por período (sem categoria)
            # Quando há múltiplos períodos, o gráfico mostra cada período separadamente
            # Mas o valor total que o usuário vê é a soma de Flex Bud Total de TODOS os períodos e categorias
            # dividido pela soma dos volumes de todos os períodos
            
            # Somar TODAS as categorias e períodos (mesma lógica do gráfico)
            flex_bud_total_custo = df_original['_Flex_Bud_Total'].sum()  # Soma de TODAS as categorias e períodos
            total_custo_total = df_original['_Total_Custo_Total'].sum()  # Soma de TODAS as categorias e períodos
            
            # 🔧 CORREÇÃO CRÍTICA: _Volume_Real contém o volume total por período (não por categoria)
            # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
            # IMPORTANTE: Este valor já é a SOMA dos volumes de todos os períodos (calculado na linha 4668)
            # Então devemos usar o primeiro valor (todos são iguais)
            # 🔧 CORREÇÃO: Obter volume real corretamente
            volumes_reais = df_original['_Volume_Real'].dropna()
            if len(volumes_reais) > 0:
                # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
                # Usar o primeiro valor (todos são iguais)
                volume_total_real = float(volumes_reais.iloc[0]) if len(volumes_reais) > 0 else 0.0
            else:
                volume_total_real = 0.0
            
            # Recalcular CPU a partir dos totais (mesma lógica do gráfico)
            # Flex BUD CPU Total = (Soma de Flex Bud Total de todas as categorias) / (Volume Total)
            flex_bud_cpu = flex_bud_total_custo / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            total_cpu = total_custo_total / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            
            # Calcular BUD também
            volume_total_budget = 0  # Inicializar
            if '_Budget_Total' in df_original.columns and '_Volume_Budget' in df_original.columns:
                budget_total_custo = df_original['_Budget_Total'].sum()  # Soma de TODAS as categorias
                # Mesma lógica para volume de budget
                # 🔧 CORREÇÃO: Obter volume budget corretamente
                volumes_budget = df_original['_Volume_Budget'].dropna()
                if len(volumes_budget) > 0:
                    # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
                    # Usar o primeiro valor (todos são iguais)
                    volume_total_budget = float(volumes_budget.iloc[0]) if len(volumes_budget) > 0 else 0.0
                else:
                    volume_total_budget = 0.0
                bud_cpu = budget_total_custo / volume_total_budget if volume_total_budget != 0 and pd.notnull(volume_total_budget) else 0
            else:
                # Se não tiver colunas auxiliares, usar soma direta
                bud_cpu = df_original['BUD'].sum() if 'BUD' in df_original.columns else 0
                volume_total_budget = 0  # Não temos volume de budget disponível
            
            linha_resumo['Flex BUD'] = flex_bud_cpu
            linha_resumo['Total'] = total_cpu
            linha_resumo['BUD'] = bud_cpu
            linha_resumo['Flex Bud - BUD'] = flex_bud_cpu - bud_cpu
            linha_resumo['Total - Flex Bud'] = total_cpu - flex_bud_cpu
            
            # 🔧 ADICIONAR: Incluir volumes usados nos cálculos (apenas para resumo geral)
            linha_resumo['_Volume_Real_Calculo'] = volume_total_real
            linha_resumo['_Volume_Budget_Calculo'] = volume_total_budget
            
            # Formatação
            linha_resumo_formatado['Flex BUD'] = f"{flex_bud_cpu:,.2f}"
            linha_resumo_formatado['Total'] = f"{total_cpu:,.2f}"
            linha_resumo_formatado['BUD'] = f"{bud_cpu:,.2f}"
            linha_resumo_formatado['Flex Bud - BUD'] = f"{flex_bud_cpu - bud_cpu:,.2f}"
            linha_resumo_formatado['Total - Flex Bud'] = f"{total_cpu - flex_bud_cpu:,.2f}"
            # 🔧 ADICIONAR: Formatar volumes usados nos cálculos (sem casas decimais)
            linha_resumo_formatado['_Volume_Real_Calculo'] = f"{volume_total_real:,.0f}"
            linha_resumo_formatado['_Volume_Budget_Calculo'] = f"{volume_total_budget:,.0f}"
        else:
            # Se não tiver colunas auxiliares, somar diretamente (comportamento antigo)
            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                if col in df_original.columns:
                    soma = df_original[col].sum()
                    linha_resumo[col] = soma
                    linha_resumo_formatado[col] = f"{soma:,.2f}"
            
            # 🔧 ADICIONAR: Tentar obter volumes mesmo sem colunas auxiliares (se disponíveis)
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
                    elif fator_conversao == "M (Milhões)":
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
    """Exibe caixas de texto com valores de resumo usando nomes de colunas dinâmicas (ex: Set/24, Flex set/24, etc.)
    
    Args:
        linha_resumo: Dicionário com valores numéricos (usando nomes de colunas dinâmicas)
        linha_resumo_formatado: Dicionário com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget
    """
    # Obter colunas numéricas (excluindo volumes e colunas auxiliares)
    colunas_auxiliares = ['_Volume_Real_Calculo', '_Volume_Budget_Calculo']
    colunas_numericas = [col for col in linha_resumo.keys() if col not in colunas_auxiliares]
    
    # Ordenar colunas na ordem exata: Jul/25, Flex jul/25 - jul/25, Flex jul/25, Nov/25 - Flex jul/25, nov/25, % nov/25/flex jul/25
    # Detectar primeiro e segundo períodos
    primeiro_periodo = None
    segundo_periodo_maiuscula = None
    segundo_periodo_minuscula = None
    flex_primeiro_menos_primeiro = None
    flex_primeiro = None
    percentual = None
    
    # Primeiro, identificar todas as colunas
    for col in colunas_numericas:
        # Primeiro período: não começa com 'Flex' ou '%', não tem '-', começa com maiúscula
        if not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].isupper():
            primeiro_periodo = col
        # Flex primeiro - primeiro: começa com 'Flex' e tem '-'
        elif col.startswith('Flex') and '-' in col:
            flex_primeiro_menos_primeiro = col
        # Flex primeiro: começa com 'Flex' e não tem '-'
        elif col.startswith('Flex') and '-' not in col:
            flex_primeiro = col
        # Segundo período maiúscula: não começa com 'Flex' ou '%', tem '-', começa com maiúscula (ex: Nov/25 - Flex jul/25)
        elif '-' in col and not col.startswith('%') and not col.startswith('Flex') and len(col) > 0 and col[0].isupper():
            segundo_periodo_maiuscula = col
        # Segundo período minúscula: não começa com 'Flex' ou '%', não tem '-', começa com minúscula (ex: nov/25)
        elif not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].islower():
            segundo_periodo_minuscula = col
        # Percentual: começa com '%'
        elif col.startswith('%'):
            percentual = col
    
    # Criar ordem explícita na ordem correta
    ordem_explicita = []
    
    # 1. Primeiro período (ex: Jul/25)
    if primeiro_periodo:
        ordem_explicita.append(primeiro_periodo)
    
    # 2. Flex primeiro - primeiro (ex: Flex jul/25 - jul/25)
    if flex_primeiro_menos_primeiro:
        ordem_explicita.append(flex_primeiro_menos_primeiro)
    
    # 3. Flex primeiro (ex: Flex jul/25)
    if flex_primeiro:
        ordem_explicita.append(flex_primeiro)
    
    # 4. Segundo período - Flex primeiro (ex: Nov/25 - Flex jul/25)
    if segundo_periodo_maiuscula:
        ordem_explicita.append(segundo_periodo_maiuscula)
    
    # 5. Segundo período minúscula (ex: nov/25)
    if segundo_periodo_minuscula:
        ordem_explicita.append(segundo_periodo_minuscula)
    
    # 6. Percentual (ex: % nov/25/flex jul/25)
    if percentual:
        ordem_explicita.append(percentual)
    
    # Se a ordem explícita não capturou todas as colunas, adicionar as restantes no final
    colunas_restantes = [col for col in colunas_numericas if col not in ordem_explicita]
    ordem_explicita.extend(colunas_restantes)
    
    colunas_ordenadas = ordem_explicita
    
    # Exibir caixas (máximo 6 colunas principais)
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
        linha_resumo: Dicionário com valores numéricos
        linha_resumo_formatado: Dicionário com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget usados nos cálculos (apenas para resumo geral)
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
                # Usar a função formatar_ratio_com_barra para exibir a barra de percentual
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>", unsafe_allow_html=True)
        
        # Espaçamento entre as caixas de texto e os volumes
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 🔧 ADICIONAR: Exibir volumes abaixo da linha de valores
        # Tentar obter volumes do dicionário formatado primeiro
        volume_real_display = linha_resumo_formatado.get('_Volume_Real_Calculo', None)
        volume_budget_display = linha_resumo_formatado.get('_Volume_Budget_Calculo', None)
        
        # Se os volumes não estiverem formatados, tentar obter do dicionário numérico e formatar
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
        
        # Exibir volumes sempre que mostrar_volumes=True (mesmo padrão das caixas acima, com valor na frente na mesma linha)
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
                # Usar a função formatar_ratio_com_barra para exibir a barra de percentual
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
    
    # Calcular número de barras: 100% = barra cheia (10 barras)
    if percentual >= 100:
        num_barras = 10  # Barra cheia para 100% ou mais
    else:
        num_barras = int(percentual / 10)  # Proporcional até 100%
    
    # Criar barra visual com gradiente verde->vermelho usando emojis coloridos
    # Usar caracteres Unicode para criar efeito de gradiente
    barras_preenchidas = num_barras
    barras_vazias = 10 - num_barras
    
    # Para valores acima de 100%, mostrar barra cheia
    if percentual >= 100:
        barra = "█" * 10
    else:
        barra = "█" * barras_preenchidas + "░" * barras_vazias
    
    return f"{percentual:.1f}% {barra}"

def ordenar_por_mes(df, coluna_periodo='Período'):
    """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
    df_copy = df.copy()
    
    # Se houver coluna "Ano", sempre ordenar por ano e mês (mesmo que haja apenas um ano)
    # Isso garante que quando "Todos" está selecionado, todos os períodos sejam mostrados ordenados
    if 'Ano' in df_copy.columns:
        # Criar coluna de ordenação: ano primeiro, depois mês
        df_copy['_ordem_ano'] = df_copy['Ano']
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
        df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
    else:
        # Ordenação simples por mês (comportamento original quando não há coluna Ano)
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values('_ordem_mes')
        df_copy = df_copy.drop(columns=['_ordem_mes'])
    
    return df_copy


# Função para calcular FLEX de volume comparando dados reais vs budget
def calcular_flex_budget(df_real, df_real_vol, df_budget, df_budget_vol, tipo_viz, tem_ano):
    """
    Calcula FLEX de volume comparando dados reais vs budget.
    
    Regra:
    - Custo Fixo: sensibilidade = 0 (não varia)
    - Custo Variável: sensibilidade = 1 (varia 100% do volume)
    
    Fórmula:
    - Proporção_Volume = Volume_Budget / Volume_Real
    - Variação_% = Proporção_Volume - 1.0
    - FLEX_Fixo = Custo_Fixo_Real × Variação_% × 0 = 0
    - FLEX_Variável = Custo_Variável_Real × Variação_% × 1
    - FLEX_Total = FLEX_Fixo + FLEX_Variável
    
    Para CPU:
    - FLEX_CPU = FLEX_Total / Volume_Real
    
    Retorna DataFrame com colunas: Ano (se tem_ano), Período, FLEX, Budget_Total (valores originais do budget)
    """
    try:
        if df_budget is None or df_real is None:
            return None
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos em todos os DataFrames ANTES de agrupar
        # Mapear meses para formato capitalizado (primeira letra maiúscula) - MESMA LÓGICA DO NOTEBOOK
        mapeamento_meses = {
            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
        }
        
        def normalizar_periodo(periodo):
            """Normaliza período para formato capitalizado"""
            if pd.isna(periodo):
                return periodo
            periodo_str = str(periodo).strip()
            for mes_min, mes_cap in mapeamento_meses.items():
                if periodo_str.lower() == mes_min.lower():
                    return mes_cap
            return periodo_str  # Retornar original se não for um mês conhecido
        
        # Normalizar períodos em todos os DataFrames
        if 'Período' in df_real.columns:
            df_real = df_real.copy()
            df_real['Período'] = df_real['Período'].apply(normalizar_periodo)
        if df_real_vol is not None and 'Período' in df_real_vol.columns:
            df_real_vol = df_real_vol.copy()
            df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
        if 'Período' in df_budget.columns:
            df_budget = df_budget.copy()
            df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
            df_budget_vol = df_budget_vol.copy()
            df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)
        
        # Agrupar dados reais por período
        if tem_ano:
            # Agrupar por Ano e Período
            if 'Custo' in df_real.columns and 'Total' in df_real.columns:
                real_agrupado = df_real.groupby(['Ano', 'Período', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            else:
                return None
            
            # Agrupar budget por período
            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            else:
                return None
            
            # 🔧 CORREÇÃO: Normalizar períodos antes do merge para garantir correspondência
            # Normalizar períodos para string e remover espaços extras (já normalizados acima, mas garantir)
            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os períodos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on=['Ano', 'Período'],
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # 🔧 CORREÇÃO CRÍTICA: NÃO filtrar períodos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando não houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas períodos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada período
            flex_data = []
            for _, vol_row in volumes.iterrows():
                ano = vol_row['Ano']
                periodo = vol_row['Período']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporção de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este período
                custos_real = real_agrupado[
                    (real_agrupado['Ano'] == ano) & 
                    (real_agrupado['Período'] == periodo)
                ]
                
                # Obter valores originais do budget para este período
                custos_budget = budget_agrupado[
                    (budget_agrupado['Ano'] == ano) & 
                    (budget_agrupado['Período'] == periodo)
                ]
                
                # 🔧 CORREÇÃO: Se não encontrar budget para este período, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                    custo_variavel_budget = 0
                else:
                    budget_total = custos_budget['Total'].sum()
                    custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                    custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                
                # 🔧 NOVO: Se não houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Total'].sum()
                
                # NOTA: A conversão de moeda já foi aplicada no df_budget (linha ~2563)
                # Portanto, budget_total, custo_fixo_budget e custo_variavel_budget já estão convertidos
                
                if tipo_viz == "CPU (Custo por Unidade)":
                    # IMPORTANTE: MESMA LÓGICA DA TABELA
                    # 1) Calcular Flex Bud em "Custo Total" primeiro
                    # Flex Bud Fixo = BUD Fixo (sempre igual ao budget, não varia com volume)
                    flex_bud_fixo = custo_fixo_budget
                    # Flex Bud Variável = BUD Variável × (Volume Real / Volume Budget)
                    proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                    flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                    # Flex Bud Total (em Custo Total) = Flex Bud Fixo + Flex Bud Variável
                    flex_bud_total_custo_total = flex_bud_fixo + flex_bud_variavel
                    
                    # 2) Converter para CPU dividindo pelos volumes corretos
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Nova lógica
                    # Flex Bud Fixo = BUD (sempre igual ao budget, não varia com volume)
                    flex_bud_fixo = custo_fixo_budget
                    # Flex Bud Variável = BUD × (Volume Real / Volume Budget)
                    # Se Volume Real < Volume Budget: Flex Bud Variável < BUD (diminui)
                    # Se Volume Real > Volume Budget: Flex Bud Variável > BUD (aumenta)
                    proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                    flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                    # Flex Bud Total = Flex Bud Fixo + Flex Bud Variável
                    flex_valor = flex_bud_fixo + flex_bud_variavel
                    budget_valor = budget_total
                
                flex_data.append({
                    'Ano': ano,
                    'Período': periodo,
                    'FLEX': flex_valor,  # Agora contém Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
        else:
            # Sem coluna Ano: agrupar apenas por Período
            # (Períodos já foram normalizados acima)
            if 'Custo' in df_real.columns and 'Total' in df_real.columns:
                real_agrupado = df_real.groupby(['Período', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
            else:
                return None
            
            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['Período', 'Custo'])['Total'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
            else:
                return None
            
            # 🔧 CORREÇÃO: Normalizar períodos antes do merge para garantir correspondência
            # Normalizar períodos para string e remover espaços extras (já normalizados acima, mas garantir)
            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os períodos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on='Período',
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # 🔧 CORREÇÃO CRÍTICA: NÃO filtrar períodos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando não houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas períodos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada período
            flex_data = []
            for _, vol_row in volumes.iterrows():
                periodo = vol_row['Período']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporção de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este período
                custos_real = real_agrupado[real_agrupado['Período'] == periodo]
                
                # Obter valores originais do budget para este período
                custos_budget = budget_agrupado[budget_agrupado['Período'] == periodo]
                
                # 🔧 CORREÇÃO: Se não encontrar budget para este período, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                    custo_variavel_budget = 0
                else:
                    budget_total = custos_budget['Total'].sum()
                    custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                    custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                
                # 🔧 NOVO: Se não houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Total'].sum()
                
                # NOTA: A conversão de moeda já foi aplicada no df_budget (linha ~2550)
                # Portanto, budget_total, custo_fixo_budget e custo_variavel_budget já estão convertidos
                
                if tipo_viz == "CPU (Custo por Unidade)":
                    # IMPORTANTE: MESMA LÓGICA DA TABELA
                    # 1) Calcular Flex Bud em "Custo Total" primeiro
                    # Flex Bud Fixo = BUD Fixo (sempre igual ao budget, não varia com volume)
                    flex_bud_fixo = custo_fixo_budget
                    # Flex Bud Variável = BUD Variável × (Volume Real / Volume Budget)
                    proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                    flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                    # Flex Bud Total (em Custo Total) = Flex Bud Fixo + Flex Bud Variável
                    flex_bud_total_custo_total = flex_bud_fixo + flex_bud_variavel
                    
                    # 2) Converter para CPU dividindo pelos volumes corretos
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Nova lógica
                    # Flex Bud Fixo = BUD (sempre igual ao budget, não varia com volume)
                    flex_bud_fixo = custo_fixo_budget
                    # Flex Bud Variável = BUD × (Volume Real / Volume Budget)
                    # Se Volume Real < Volume Budget: Flex Bud Variável < BUD (diminui)
                    # Se Volume Real > Volume Budget: Flex Bud Variável > BUD (aumenta)
                    proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                    flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                    # Flex Bud Total = Flex Bud Fixo + Flex Bud Variável
                    flex_valor = flex_bud_fixo + flex_bud_variavel
                    budget_valor = budget_total
                
                flex_data.append({
                    'Período': periodo,
                    'FLEX': flex_valor,  # Agora contém Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
    except Exception as e:
        st.sidebar.warning(f"⚠️ Erro ao calcular FLEX: {e}")
        return None


# Gráfico 1: Soma do Valor por Período
# Cache removido: DataFrames grandes podem causar problemas de hash
def create_period_chart(df_data, coluna, tipo_viz, df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, moeda_simbolo="R$"):
    """Cria gráfico de barras por Período com linha pontilhada de FLEX (budget) opcional"""
    try:
        # Detectar tema para adaptar cores (dark/light mode)
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        
        # Validações iniciais
        if df_data is None or df_data.empty:
            st.warning("⚠️ Dados vazios ou None passados para o gráfico")
            return None
        
        if 'Período' not in df_data.columns:
            st.warning(f"⚠️ Coluna 'Período' não encontrada. Colunas disponíveis: {list(df_data.columns)[:10]}")
            return None
        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns):
                st.warning(f"⚠️ Coluna necessária não encontrada: {coluna}")
                st.warning(f"⚠️ Colunas disponíveis: {list(df_data.columns)[:10]}")
                return None

        # Verificar se há coluna Ano - sempre mostrar ano junto com período quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e Período (sempre que houver coluna Ano)
            # IMPORTANTE: Sempre agrupar por Ano e Período para garantir consistência
            # independentemente de "Todos" estar selecionado ou um ano específico
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                # Verificar se temos Total e Volume, ou se precisamos usar CPU existente
                if 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA: Agrupar por Ano e Período, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessárias para o agrupamento
                    colunas_agrupamento = ['Ano', 'Período']
                    chart_data = df_data[colunas_agrupamento + ['Total', 'Volume']].groupby(colunas_agrupamento).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lógica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU já existe mas temos Total e Volume, recalcular
                    colunas_agrupamento = ['Ano', 'Período']
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
                    # Fallback: agrupar apenas por Ano e Período
                    chart_data = df_data.groupby(['Ano', 'Período'])[coluna].sum().reset_index()
            else:
                # Para Custo Total, também agrupar por Ano e Período para garantir consistência
                # Otimizar: usar apenas as colunas necessárias
                chart_data = df_data[['Ano', 'Período', coluna]].groupby(['Ano', 'Período'])[coluna].sum().reset_index()
            
            # Criar coluna combinada para o rótulo do gráfico
            chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mês
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período_Completo'].tolist()
            
            # Usar Período_Completo no gráfico
            coluna_periodo_grafico = 'Período_Completo'
        else:
            # Comportamento original: agrupar apenas por Período (quando não há coluna Ano)
            # Para CPU, usar EXATAMENTE a mesma lógica da tabela (que está correta)
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                if 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA: Agrupar por Período, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessárias
                    chart_data = df_data[['Período', 'Total', 'Volume']].groupby('Período').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lógica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU já existe mas temos Total e Volume, recalcular
                    # Otimizar: usar apenas as colunas necessárias
                    chart_data = df_data[['Período', 'Total', 'Volume']].groupby('Período').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Total'] / chart_data['Volume'],
                        0
                    )
                else:
                    # Fallback: agrupar apenas por Período
                    chart_data = df_data[['Período', coluna]].groupby('Período')[coluna].sum().reset_index()
            else:
                # Otimizar: usar apenas as colunas necessárias
                chart_data = df_data[['Período', coluna]].groupby('Período')[coluna].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        # Definir título do eixo Y baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Período"
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por Período"

        # Garantir que todos os períodos do Budget apareçam (com realizado = 0)
        if df_budget is not None and not df_budget.empty and 'Período' in df_budget.columns:
            # Guardar períodos reais antes do reindex
            if tem_ano:
                periodos_reais_set = set(chart_data[['Ano', 'Período']].apply(tuple, axis=1))
            else:
                periodos_reais_set = set(chart_data['Período'].tolist())

            if tem_ano and 'Ano' in df_budget.columns:
                periodos_budget = df_budget[['Ano', 'Período']].dropna().drop_duplicates()
                index_full = pd.MultiIndex.from_frame(periodos_budget)
                chart_data = chart_data.set_index(['Ano', 'Período']).reindex(index_full).reset_index()
                # Zerar realizado quando não há dado real
                mask_real = chart_data[['Ano', 'Período']].apply(tuple, axis=1).isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan
            else:
                periodos_budget = df_budget['Período'].dropna().drop_duplicates().tolist()
                chart_data = chart_data.set_index('Período').reindex(periodos_budget).reset_index()
                mask_real = chart_data['Período'].isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan

            # Preencher colunas numéricas com zero (sem usar budget)
            colunas_zero = [col for col in chart_data.columns if pd.api.types.is_numeric_dtype(chart_data[col]) and col != coluna]
            for col in colunas_zero:
                chart_data[col] = chart_data[col].fillna(0)

            # Reordenar após completar períodos
            chart_data = ordenar_por_mes(chart_data, 'Período')
            if tem_ano:
                chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                ordem_periodos = chart_data['Período_Completo'].tolist()
                coluna_periodo_grafico = 'Período_Completo'
            else:
                ordem_periodos = chart_data['Período'].tolist()
                coluna_periodo_grafico = 'Período'

        # Validar se chart_data tem dados após agrupamento e filtros
        if chart_data is None or chart_data.empty:
            st.warning("⚠️ Nenhum dado após agrupamento. Verifique os filtros aplicados.")
            return None
            
        # Verificar se a coluna tem valores válidos
        if coluna not in chart_data.columns:
            st.warning(f"⚠️ Coluna '{coluna}' não encontrada após agrupamento. Colunas disponíveis: {list(chart_data.columns)}")
            return None
            
        # Se houver volume real, zerar/ocultar realizado em períodos sem volume
        if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Período' in df_real_vol.columns:
            if tem_ano and 'Ano' in df_real_vol.columns and 'Ano' in chart_data.columns:
                vol_agr = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on=['Ano', 'Período'], how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])
            else:
                vol_agr = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on='Período', how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])

        # Para o ano atual, zerar períodos futuros sem realizado
        if tem_ano and 'Ano' in chart_data.columns and 'Período' in chart_data.columns:
            ano_atual_local = datetime.now().year
            idx_mes_atual = datetime.now().month - 1
            if 0 <= idx_mes_atual < len(ORDEM_MESES):
                meses_ate_atual = set(ORDEM_MESES[:idx_mes_atual + 1])
                periodos_lower = chart_data['Período'].astype(str).str.lower()
                anos_num = pd.to_numeric(chart_data['Ano'].astype(str), errors='coerce')
                mask_ano_atual = anos_num == ano_atual_local
                mask_futuro = ~periodos_lower.isin(meses_ate_atual)
                chart_data.loc[mask_ano_atual & mask_futuro, coluna] = 0

        # Garantir que os valores sejam numéricos (preservar NaN para não desenhar barras)
        chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce')
            
        # Verificar se há valores não-nulos (apenas para Custo Total, CPU já filtra zeros)
        if tipo_viz != "CPU (Custo por Unidade)":
            valores_validos = chart_data[coluna].notna() & (chart_data[coluna] != 0)
            if not valores_validos.any():
                # Não bloquear, apenas avisar - pode haver valores muito pequenos após conversão
                st.info(f"ℹ️ Todos os valores na coluna '{coluna}' são zero após agrupamento. Mostrando gráfico mesmo assim.")
        
        # Verificar se chart_data está vazio
        if chart_data is None or chart_data.empty or len(chart_data) == 0:
            st.warning("⚠️ Nenhum dado disponível após agrupamento e filtros.")
            return None

        # Usar gradiente baseado no valor da coluna (como na figura 1)
        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
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
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip(
                    f'{coluna}:Q',
                    title=coluna,
                    format=',.2f' if tipo_viz == "CPU (Custo por Unidade)"
                    else ',.2f'
                )
            ]
        ).properties(
            height=altura_grafico,
            width='container'
        )

        # Adicionar rótulos com valores nas barras
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
        budget_data = None  # Inicializar para uso no gráfico de delta
        # IMPORTANTE: No modo CPU, df_data pode não ter a coluna 'Custo' necessária para calcular FLEX
        # Usar df_real_original se disponível, caso contrário usar df_data
        df_real_para_flex = df_real_original if df_real_original is not None else df_data
        
        # 🔧 CORREÇÃO: Verificar se os dados necessários estão disponíveis
        # Verificar se df_budget existe e tem a coluna Período
        tem_budget = df_budget is not None and not df_budget.empty and 'Período' in df_budget.columns
        # Verificar se df_real_vol existe e tem a coluna Volume
        tem_real_vol = df_real_vol is not None and not df_real_vol.empty and 'Volume' in df_real_vol.columns
        # Verificar se df_budget_vol existe (pode ser None, mas se existir deve ter Volume)
        tem_budget_vol = df_budget_vol is not None and not df_budget_vol.empty and 'Volume' in df_budget_vol.columns
        
        dados_budget_disponiveis = tem_budget and tem_real_vol
        
        if dados_budget_disponiveis:
            # Verificar se temos dados com coluna 'Custo' para calcular FLEX
            if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                try:
                    # 🔧 CORREÇÃO: Usar a MESMA lógica do gráfico de Oficina (que funciona!)
                    # Calcular Flex Bud diretamente em vez de usar calcular_flex_budget
                    # Normalizar períodos ANTES de agrupar
                    mapeamento_meses = {
                        'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                        'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                        'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                        'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                    }
                    
                    def normalizar_periodo(periodo):
                        """Normaliza período para formato capitalizado"""
                        if pd.isna(periodo):
                            return periodo
                        periodo_str = str(periodo).strip()
                        for mes_min, mes_cap in mapeamento_meses.items():
                            if periodo_str.lower() == mes_min.lower():
                                return mes_cap
                        return periodo_str
                    
                    # Normalizar períodos em todos os DataFrames
                    if 'Período' in df_real_para_flex.columns:
                        df_real_para_flex = df_real_para_flex.copy()
                        df_real_para_flex['Período'] = df_real_para_flex['Período'].apply(normalizar_periodo)
                    if df_real_vol is not None and 'Período' in df_real_vol.columns:
                        df_real_vol = df_real_vol.copy()
                        df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
                    if 'Período' in df_budget.columns:
                        df_budget = df_budget.copy()
                        df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
                    if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
                        df_budget_vol = df_budget_vol.copy()
                        df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)
                    
                    # Agrupar dados reais por Período (mesma lógica do gráfico de Oficina)
                    if tem_ano:
                        if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Custo'])['Total'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Custo'])['Total'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    else:
                        if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['Período', 'Custo'])['Total'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Total' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['Período', 'Custo'])['Total'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    
                    # Verificar se temos todos os dados necessários
                    if (real_agrupado is None or real_vol_agrupado is None or 
                        budget_agrupado is None or budget_vol_agrupado is None):
                        flex_data = None
                    else:
                        # Normalizar períodos nos DataFrames agrupados antes do merge
                        if tem_ano:
                            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                        else:
                            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                        
                        # Fazer merge de volumes por Período
                        if tem_ano:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on=['Ano', 'Período'],
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        else:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on='Período',
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        
                        # Calcular FLEX para cada Período (mesma lógica do gráfico de Oficina)
                        flex_data = []
                        for _, vol_row in volumes.iterrows():
                            if tem_ano:
                                ano = vol_row['Ano']
                                periodo = vol_row['Período']
                            else:
                                periodo = vol_row['Período']
                            
                            volume_real = vol_row['Volume_real']
                            volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                            
                            if volume_real == 0 or pd.isna(volume_real):
                                continue
                            
                            # Obter custos reais para este Período
                            if tem_ano:
                                custos_real = real_agrupado[
                                    (real_agrupado['Ano'] == ano) & 
                                    (real_agrupado['Período'] == periodo)
                                ]
                                custos_budget = budget_agrupado[
                                    (budget_agrupado['Ano'] == ano) & 
                                    (budget_agrupado['Período'] == periodo)
                                ]
                            else:
                                custos_real = real_agrupado[real_agrupado['Período'] == periodo]
                                custos_budget = budget_agrupado[budget_agrupado['Período'] == periodo]
                            
                            # Se não houver dados de budget para este período, usar zeros
                            if len(custos_budget) == 0:
                                budget_total = 0
                                custo_fixo_budget = 0
                                custo_variavel_budget = 0
                            else:
                                budget_total = custos_budget['Total'].sum()
                                custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                                custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                            
                            # Calcular Flex Bud (mesma lógica do gráfico de Oficina)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Flex Bud Fixo = BUD Fixo
                                flex_bud_fixo = custo_fixo_budget
                                # Flex Bud Variável = BUD Variável × (Volume Real / Volume Budget)
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                                # Flex Bud Total (em Custo Total) = Flex Bud Fixo + Flex Bud Variável
                                flex_bud_total_custo_total = flex_bud_fixo + flex_bud_variavel
                                # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                                flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                            else:
                                # Para Custo Total: Flex Bud Fixo + Flex Bud Variável
                                flex_bud_fixo = custo_fixo_budget
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                                flex_valor = flex_bud_fixo + flex_bud_variavel
                            
                            # Adicionar ao flex_data
                            if tem_ano:
                                flex_data.append({
                                    'Ano': ano,
                                    'Período': periodo,
                                    'FLEX': flex_valor
                                })
                            else:
                                flex_data.append({
                                    'Período': periodo,
                                    'FLEX': flex_valor
                                })
                        
                        if len(flex_data) == 0:
                            flex_data = None
                        else:
                            flex_data = pd.DataFrame(flex_data)
                    
                    if flex_data is None:
                        budget_data = None
                    
                    if flex_data is not None and len(flex_data) > 0:
                        # Renomear coluna FLEX para o nome da coluna do gráfico
                        flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                        
                        # 🔧 CORREÇÃO CRÍTICA: Fazer merge com chart_data para garantir correspondência de períodos
                        # Isso garante que budget_data tenha os mesmos períodos que chart_data
                        if tem_ano:
                            # Criar coluna combinada para o rótulo do gráfico no flex_data
                            flex_data['Período_Completo'] = flex_data['Período'].astype(str) + ' ' + flex_data['Ano'].astype(str)
                            # Ordenar por ano e mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            
                            # Fazer merge com chart_data para garantir correspondência
                            # Usar left join para manter todos os períodos do chart_data
                            budget_data = chart_data[['Período_Completo']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['Período_Completo', coluna]],
                                on='Período_Completo',
                                how='left'
                            )
                        else:
                            # Ordenar por mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            
                            # Fazer merge com chart_data para garantir correspondência
                            # Usar left join para manter todos os períodos do chart_data
                            budget_data = chart_data[['Período']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['Período', coluna]],
                                on='Período',
                                how='left'
                            )
                        
                        # Preencher valores NaN com 0 (períodos sem Flex Bud)
                        budget_data[coluna] = budget_data[coluna].fillna(0)
                        
                        # Criar linha pontilhada se budget_data tiver dados
                        # IMPORTANTE: Criar mesmo que alguns valores sejam zero, desde que tenha dados
                        if len(budget_data) > 0:
                            # Determinar campo do eixo X baseado em tem_ano
                            campo_x = 'Período_Completo' if tem_ano else 'Período'
                            
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
                                    title='Período',
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
                                    alt.Tooltip(f'{campo_x}:N', title='Período'),
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
                                    title='Período',
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
                                    alt.Tooltip(f'{campo_x}:N', title='Período'),
                                    alt.Tooltip('Tipo:N', title='Tipo'),
                                    alt.Tooltip(
                                        f'{coluna}:Q',
                                        title='Flex Bud',
                                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                    )
                                ]
                            )
                            
                            # Adicionar rótulos de texto na linha pontilhada
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
                                    title='Período',
                                    sort=ordem_periodos
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y
                                ),
                                text=alt.Text(f'{coluna}:Q', format=formato_rotulo_budget)
                            )
                            
                            # Combinar linha, pontos e rótulos
                            linha_budget = linha_budget + pontos_budget + rotulos_budget
                        else:
                            # Se budget_data não tem valores não-zero, não criar linha
                            linha_budget = None
                            budget_data = None
                    else:
                        # Se budget_data foi criado mas está vazio, definir como None
                        budget_data = None
                except Exception as e:
                    budget_data = None
                    linha_budget = None
            else:
                budget_data = None

        # Criar gráfico de delta (Real - Flex Bud) se budget_data estiver disponível
        # IMPORTANTE: No modo CPU, garantir que budget_data seja usado mesmo se estiver vazio
        grafico_delta = None
        if budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Flex Bud - Real
                # Fazer merge dos dados de Real e Flex Bud para calcular delta
                delta_data = chart_data.copy()
                
                # Determinar campo do eixo X baseado em tem_ano
                campo_x_delta = 'Período_Completo' if tem_ano else 'Período'
                
                # Fazer merge com budget_data para obter valores de Flex Bud
                # Renomear coluna antes do merge para evitar conflito
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                
                if tem_ano:
                    # Garantir que budget_data_merge tenha a coluna Período_Completo
                    # budget_data já foi criado com Período_Completo no merge anterior
                    if campo_x_delta not in budget_data_merge.columns:
                        # Se não tiver, criar a partir de Período e Ano
                        if 'Período' in budget_data_merge.columns and 'Ano' in budget_data_merge.columns:
                            budget_data_merge[campo_x_delta] = budget_data_merge['Período'].astype(str) + ' ' + budget_data_merge['Ano'].astype(str)
                    
                    # Garantir que delta_data também tenha Período_Completo
                    if campo_x_delta not in delta_data.columns:
                        delta_data[campo_x_delta] = delta_data['Período'].astype(str) + ' ' + delta_data['Ano'].astype(str)
                    
                    delta_data = delta_data.merge(
                        budget_data_merge[[campo_x_delta, f'{coluna}_FlexBud']],
                        on=campo_x_delta,
                        how='left'
                    )
                else:
                    delta_data = delta_data.merge(
                        budget_data_merge[['Período', f'{coluna}_FlexBud']],
                        on='Período',
                        how='left'
                    )
                
                # Calcular delta: Real - Flex Bud
                coluna_real = coluna  # A coluna original já é o Real
                coluna_flex = f'{coluna}_FlexBud'
                # Preencher valores NaN com 0 antes de calcular delta
                delta_data[coluna_flex] = delta_data[coluna_flex].fillna(0)
                delta_data['Delta'] = delta_data[coluna_real].fillna(0) - delta_data[coluna_flex].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domínio simétrico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Criar gráfico de barras para delta (mais baixo)
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
                        title='Delta (Real - Flex Bud)',
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
                        legend=None  # Sem legenda para evitar duplicação - o gráfico principal já tem sua legenda
                    ),
                    tooltip=[
                        alt.Tooltip(f'{campo_x_delta}:N', title='Período'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                        alt.Tooltip(f'{coluna_real}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna_flex}:Q', title='Flex Bud', format=',.2f')
                    ]
                ).properties(
                    height=38  # Gráfico mais baixo/fino
                )
                
                # Adicionar rótulos de dados no gráfico de delta
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
                
                # Combinar gráfico de delta com rótulos
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                grafico_delta = None
        
        # Combinar gráfico de barras com linha de budget se disponível
        if linha_budget is not None:
            # Criar gráfico principal com barras, rótulos e linha
            grafico_principal = alt.layer(
                grafico_barras,
                rotulos,
                linha_budget
            ).resolve_scale(
                x='shared',
                y='shared'
            )
            
            # Se temos gráfico de delta, combinar verticalmente (delta em cima)
            if grafico_delta is not None:
                # Combinar gráficos verticalmente compartilhando eixo X
                # Delta fica em cima (primeiro), gráfico principal embaixo
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_principal
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os gráficos
                )
            else:
                grafico_final = grafico_principal
        else:
            # Se não há linha de budget, mas temos gráfico de delta, combinar com gráfico de barras
            if grafico_delta is not None:
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_barras + rotulos
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os gráficos
                )
            else:
                grafico_final = grafico_barras + rotulos
        
        return grafico_final
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


# Gráfico 2: Volume por Período
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data, df_budget_vol=None):
    """Cria gráfico de barras de Volume por Período com linha pontilhada de volume do Budget opcional"""
    try:
        altura_grafico = 260
        if 'Volume' not in df_data.columns or 'Período' not in df_data.columns:
            return None

        # Verificar se há coluna Ano - sempre mostrar ano junto com período quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e Período (sempre que houver coluna Ano)
            chart_data = df_data.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            
            # Criar coluna combinada para o rótulo do gráfico
            chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mês
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período_Completo'].tolist()
            
            # Usar Período_Completo no gráfico
            coluna_periodo_grafico = 'Período_Completo'
        else:
            # Comportamento original: agrupar apenas por Período (quando não há coluna Ano)
            chart_data = df_data.groupby('Período')['Volume'].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        # Usar gradiente verde baseado no valor do Volume (como no gráfico Volume por Veículo)
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
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
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=altura_grafico,
            width='container'
            # Título removido para evitar duplicação com st.subheader
        )

        # Adicionar rótulos
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
        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
            try:
                # Processar volume do budget seguindo a mesma lógica dos dados principais
                tem_ano_budget_vol = 'Ano' in df_budget_vol.columns
                
                if tem_ano_budget_vol:
                    # Agrupar por Ano e Período (mesma lógica dos dados principais)
                    budget_vol_data = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                    
                    # Criar coluna combinada para o rótulo do gráfico
                    budget_vol_data['Período_Completo'] = budget_vol_data['Período'].astype(str) + ' ' + budget_vol_data['Ano'].astype(str)
                    # Ordenar por ano e mês
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'Período')
                    # Filtrar apenas períodos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['Período_Completo'].isin(ordem_periodos)].copy()
                else:
                    # Sem coluna Ano: agrupar apenas por Período
                    budget_vol_data = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
                    # Ordenar por mês
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'Período')
                    # Filtrar apenas períodos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['Período'].isin(ordem_periodos)].copy()
                
                if len(budget_vol_data) > 0:
                    # Determinar campo do eixo X baseado em tem_ano
                    campo_x = 'Período_Completo' if tem_ano else 'Período'
                    
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
                            title='Período',
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
                            alt.Tooltip(f'{campo_x}:N', title='Período'),
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
                            title='Período',
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
                            alt.Tooltip(f'{campo_x}:N', title='Período'),
                            alt.Tooltip('Tipo:N', title='Tipo'),
                            alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                        ]
                    )
                    
                    # Adicionar rótulos de texto na linha pontilhada
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
                            title='Período',
                            sort=ordem_periodos
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total'
                        ),
                        text=alt.Text('Volume:Q', format=',.0f')
                    )
                    
                    # Combinar linha, pontos e rótulos
                    linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception as e:
                st.sidebar.warning(f"⚠️ Erro ao processar dados de volume do budget: {e}")

        # Combinar gráfico de barras com linha de budget se disponível
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
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Gráfico 4.5: Volume por Veículo
@st.cache_data(ttl=900, max_entries=2)
def create_volume_veiculo_chart(df_data, df_budget_vol=None, df_despesas=None):
    """Cria gráfico de barras de Volume por Veículo com linha pontilhada de volume do Budget opcional
    df_despesas: DataFrame com dados de despesas para filtrar apenas períodos com despesas"""
    try:
        if 'Volume' not in df_data.columns or 'Veículo' not in df_data.columns:
            return None
        
        # 🔧 CORREÇÃO CRÍTICA: Incluir TODOS os meses do ano, não apenas a partir do primeiro com despesa
        # Isso permite ver o Flex Bud completo do ano, mesmo para meses sem dados realizados
        if df_despesas is not None and 'Veículo' in df_despesas.columns:
            # Obter combinações de Veículo + Período (e Ano se houver) que têm despesas
            if 'Ano' in df_despesas.columns and 'Período' in df_despesas.columns:
                # Agrupar por Veículo e Ano
                periodos_com_despesas = df_despesas[['Veículo', 'Ano']].drop_duplicates()
                
                # Para cada combinação de Veículo e Ano, incluir TODOS os meses
                periodos_filtrados_list = []
                for veiculo in periodos_com_despesas['Veículo'].unique():
                    for ano in periodos_com_despesas['Ano'].unique():
                        # 🔧 MUDANÇA CRÍTICA: Incluir TODOS os 12 meses do ano
                        for periodo in ORDEM_MESES:
                            periodos_filtrados_list.append({
                                'Veículo': veiculo,
                                'Período': periodo.capitalize(),  # Capitalizar para corresponder ao formato
                                'Ano': ano
                            })
                
                if periodos_filtrados_list:
                    periodos_filtrados = pd.DataFrame(periodos_filtrados_list)
                    
                    
                    # IMPORTANTE: Normalizar períodos antes do merge para garantir correspondência
                    df_data_merge = df_data.copy()
                    periodos_filtrados_merge = periodos_filtrados.copy()
                    
                    # Normalizar períodos para minúsculas para o merge
                    df_data_merge['Período_normalizado'] = df_data_merge['Período'].astype(str).str.lower().str.strip()
                    periodos_filtrados_merge['Período_normalizado'] = periodos_filtrados_merge['Período'].astype(str).str.lower().str.strip()
                    
                    # Fazer merge usando períodos normalizados
                    df_data_merged = pd.merge(
                        df_data_merge,
                        periodos_filtrados_merge[['Veículo', 'Período_normalizado', 'Ano']],
                        on=['Veículo', 'Período_normalizado', 'Ano'],
                        how='inner'
                    )
                    
                    # Remover coluna temporária e manter coluna original de Período
                    df_data_merged = df_data_merged.drop(columns=['Período_normalizado'])
                    df_data = df_data_merged
                    
            elif 'Período' in df_despesas.columns:
                # Agrupar por Veículo para obter o primeiro e último período com despesas
                periodos_com_despesas = df_despesas[['Veículo', 'Período']].drop_duplicates()
                
                # Criar mapeamento de ordem dos meses
                ordem_meses_dict = {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
                
                # Para cada Veículo, encontrar o primeiro e último período
                periodos_filtrados_list = []
                for veiculo in periodos_com_despesas['Veículo'].unique():
                    periodos_veiculo = periodos_com_despesas[
                        periodos_com_despesas['Veículo'] == veiculo
                    ]['Período'].unique()
                    
                    if len(periodos_veiculo) > 0:
                        # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                        periodos_ordenados = sorted(
                            periodos_veiculo,
                            key=lambda x: ordem_meses_dict.get(str(x).lower(), 999)
                        )
                        primeiro_periodo = periodos_ordenados[0]
                        ultimo_periodo = periodos_ordenados[-1]
                        
                        # Obter índice do primeiro período na ordem
                        idx_primeiro = ordem_meses_dict.get(primeiro_periodo, 0)
                        
                        # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                        meses_para_incluir = ORDEM_MESES[idx_primeiro:]
                        
                        # Criar DataFrame com todos os períodos a partir do primeiro
                        for periodo in meses_para_incluir:
                            periodos_filtrados_list.append({
                                'Veículo': veiculo,
                                'Período': periodo
                            })
                
                if periodos_filtrados_list:
                    periodos_filtrados = pd.DataFrame(periodos_filtrados_list)
                    # Fazer merge com df_data para filtrar apenas períodos a partir do primeiro mês com despesa
                    df_data = pd.merge(
                        df_data,
                        periodos_filtrados,
                        on=['Veículo', 'Período'],
                        how='inner'
                    )
        
        # Filtrar linhas com Volume e Veículo não nulos
        df_data = df_data[df_data['Volume'].notna() & df_data['Veículo'].notna()].copy()
        
        if len(df_data) == 0:
            return None
        
        # Agrupar por Veículo e somar Volume
        # Se houver múltiplos anos, agrupar por Veículo, Período e Ano primeiro
        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        
        if tem_multiplos_anos and 'Período' in df_data.columns:
            # Agrupar por Veículo, Período e Ano, somar Volume
            df_agrupado_periodo = df_data.groupby(['Veículo', 'Período', 'Ano']).agg({
                'Volume': 'sum'
            }).reset_index()
            # Agora agrupar por Veículo, somar Volume de todos os períodos
            chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                'Volume': 'sum'
            }).reset_index()
        elif 'Período' in df_data.columns:
            # Agrupar por Veículo e Período, somar Volume
            df_agrupado_periodo = df_data.groupby(['Veículo', 'Período']).agg({
                'Volume': 'sum'
            }).reset_index()
            # Agora agrupar por Veículo, somar Volume de todos os períodos
            chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                'Volume': 'sum'
            }).reset_index()
        else:
            # Se não tiver Período, agrupar apenas por Veículo
            chart_data = df_data.groupby('Veículo').agg({
                'Volume': 'sum'
            }).reset_index()
        
        # Verificar se há dados
        if len(chart_data) == 0:
            return None
        
        # Filtrar valores nulos
        chart_data = chart_data[chart_data['Volume'].notna()].copy()
        
        if len(chart_data) == 0:
            return None
        
        chart_data = chart_data.sort_values('Volume', ascending=False)
        
        # Determinar ordem dos veículos (usar a mesma ordem para barras e linha)
        ordem_veiculos = chart_data['Veículo'].tolist()
        
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Veículo:N',
                title='Veículo',
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
                alt.Tooltip('Veículo:N', title='Veículo'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=360,
            width='container'
            # Título removido para evitar duplicação com st.subheader
        )
        
        # Adicionar rótulos
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
        # 🔧 CORREÇÃO: Aplicar a mesma lógica de filtro (a partir do primeiro mês com despesa) ao budget
        linha_budget_vol = None
        if df_budget_vol is not None and 'Veículo' in df_budget_vol.columns:
            try:
                # Filtrar linhas com Volume e Veículo não nulos
                df_budget_vol_filtrado = df_budget_vol[df_budget_vol['Volume'].notna() & df_budget_vol['Veículo'].notna()].copy()
                
                # 🔧 IMPORTANTE: Aplicar o mesmo filtro de períodos que foi aplicado aos dados principais
                # 🔧 CORREÇÃO CRÍTICA: Incluir TODOS os meses do ano no Budget
                if df_despesas is not None and 'Veículo' in df_despesas.columns and len(df_budget_vol_filtrado) > 0:
                    # Obter os mesmos períodos que foram usados para os dados principais (todos os 12 meses)
                    if 'Ano' in df_despesas.columns and 'Período' in df_despesas.columns and 'Ano' in df_budget_vol_filtrado.columns:
                        # Obter períodos com despesas
                        periodos_com_despesas_budget = df_despesas[['Veículo', 'Ano']].drop_duplicates()
                        
                        # Para cada combinação de Veículo e Ano, incluir TODOS os 12 meses
                        periodos_filtrados_list_budget = []
                        for veiculo in periodos_com_despesas_budget['Veículo'].unique():
                            for ano in periodos_com_despesas_budget['Ano'].unique():
                                # 🔧 MUDANÇA CRÍTICA: Incluir TODOS os 12 meses do ano
                                for periodo in ORDEM_MESES:
                                    periodos_filtrados_list_budget.append({
                                        'Veículo': veiculo,
                                        'Período': periodo.capitalize(),
                                        'Ano': ano
                                    })
                        
                        if periodos_filtrados_list_budget:
                            periodos_filtrados_budget = pd.DataFrame(periodos_filtrados_list_budget)
                            
                            # Normalizar períodos antes do merge
                            df_budget_vol_merge = df_budget_vol_filtrado.copy()
                            periodos_filtrados_budget_merge = periodos_filtrados_budget.copy()
                            
                            df_budget_vol_merge['Período_normalizado'] = df_budget_vol_merge['Período'].astype(str).str.lower().str.strip()
                            periodos_filtrados_budget_merge['Período_normalizado'] = periodos_filtrados_budget_merge['Período'].astype(str).str.lower().str.strip()
                            
                            # Fazer merge usando períodos normalizados
                            df_budget_vol_filtrado = pd.merge(
                                df_budget_vol_merge,
                                periodos_filtrados_budget_merge[['Veículo', 'Período_normalizado', 'Ano']],
                                on=['Veículo', 'Período_normalizado', 'Ano'],
                                how='inner'
                            )
                            
                            # Remover coluna temporária
                            df_budget_vol_filtrado = df_budget_vol_filtrado.drop(columns=['Período_normalizado'])
                    
                    elif 'Período' in df_despesas.columns and 'Período' in df_budget_vol_filtrado.columns:
                        # Obter períodos com despesas
                        periodos_com_despesas_budget = df_despesas[['Veículo']].drop_duplicates()
                        
                        # Para cada Veículo, incluir TODOS os 12 meses
                        periodos_filtrados_list_budget = []
                        for veiculo in periodos_com_despesas_budget['Veículo'].unique():
                            # 🔧 MUDANÇA CRÍTICA: Incluir TODOS os 12 meses
                            for periodo in ORDEM_MESES:
                                periodos_filtrados_list_budget.append({
                                    'Veículo': veiculo,
                                    'Período': periodo.capitalize()
                                })
                        
                        if periodos_filtrados_list_budget:
                            periodos_filtrados_budget = pd.DataFrame(periodos_filtrados_list_budget)
                            
                            # Normalizar períodos antes do merge
                            df_budget_vol_merge = df_budget_vol_filtrado.copy()
                            periodos_filtrados_budget_merge = periodos_filtrados_budget.copy()
                            
                            df_budget_vol_merge['Período_normalizado'] = df_budget_vol_merge['Período'].astype(str).str.lower().str.strip()
                            periodos_filtrados_budget_merge['Período_normalizado'] = periodos_filtrados_budget_merge['Período'].astype(str).str.lower().str.strip()
                            
                            # Fazer merge usando períodos normalizados
                            df_budget_vol_filtrado = pd.merge(
                                df_budget_vol_merge,
                                periodos_filtrados_budget_merge[['Veículo', 'Período_normalizado']],
                                on=['Veículo', 'Período_normalizado'],
                                how='inner'
                            )
                            
                            # Remover coluna temporária
                            df_budget_vol_filtrado = df_budget_vol_filtrado.drop(columns=['Período_normalizado'])
                
                if len(df_budget_vol_filtrado) > 0:
                    # Agrupar por Veículo seguindo a mesma lógica dos dados principais
                    tem_multiplos_anos_budget = 'Ano' in df_budget_vol_filtrado.columns and df_budget_vol_filtrado['Ano'].nunique() > 1
                    
                    if tem_multiplos_anos_budget and 'Período' in df_budget_vol_filtrado.columns:
                        # Agrupar por Veículo, Período e Ano, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Veículo', 'Período', 'Ano']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por Veículo, somar Volume de todos os períodos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    elif 'Período' in df_budget_vol_filtrado.columns:
                        # Agrupar por Veículo e Período, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Veículo', 'Período']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por Veículo, somar Volume de todos os períodos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        # Se não tiver Período, agrupar apenas por Veículo
                        budget_vol_data = df_budget_vol_filtrado.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    
                    # IMPORTANTE: Garantir que todos os veículos do realizado estejam no budget
                    # Criar DataFrame completo com todos os veículos do realizado
                    budget_vol_data_completo = pd.DataFrame({'Veículo': ordem_veiculos})
                    
                    # Fazer merge com os dados de budget (left join para manter todos os veículos do realizado)
                    budget_vol_data = budget_vol_data_completo.merge(
                        budget_vol_data,
                        on='Veículo',
                        how='left'
                    )
                    
                    # Preencher valores faltantes com 0
                    budget_vol_data['Volume'] = budget_vol_data['Volume'].fillna(0)
                    
                    if len(budget_vol_data) > 0:
                        # Adicionar coluna de legenda
                        budget_vol_data_legenda = budget_vol_data.copy()
                        budget_vol_data_legenda['Tipo'] = 'Volume Budget'
                        
                        # Garantir que está na ordem correta (já está na ordem correta por causa do merge)
                        # Mas vamos garantir explicitamente
                        ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos)}
                        budget_vol_data_legenda['_ordem'] = budget_vol_data_legenda['Veículo'].map(ordem_dict)
                        budget_vol_data_legenda = budget_vol_data_legenda.sort_values('_ordem')
                        budget_vol_data_legenda = budget_vol_data_legenda.drop(columns=['_ordem'])
                        
                        # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras (ordem_veiculos)
                        # Isso garante que a linha do budget apareça na mesma ordem do realizado
                        ordem_veiculos_budget = ordem_veiculos
                        
                        # Criar linha tracejada de volume do budget
                        linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                            x=alt.X(
                                'Veículo:N',
                                title='Veículo',
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
                                alt.Tooltip('Veículo:N', title='Veículo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar bolinhas nos pontos da linha
                        pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('Veículo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='Veículo'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Veículo:N', title='Veículo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar rótulos nos pontos
                        rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('Veículo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='Veículo'),
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
                # Silenciar erro, apenas não mostrar linha do budget
                pass
        
        # Combinar gráfico de barras com linha do budget se existir
        if linha_budget_vol is not None:
            return grafico_barras + rotulos + linha_budget_vol
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico de volume: {e}")
        return None


# Inicializar session_state para manter a tab selecionada
# Usar uma chave mais específica para evitar conflitos
if 'tab_selecionada_tc_ext_persistente' not in st.session_state:
    st.session_state.tab_selecionada_tc_ext_persistente = 0

# Verificar se há parâmetro de tab na URL e atualizar session_state
# Isso garante que a tab seja mantida mesmo após recarregamento por filtros
tab_from_url = st.query_params.get('tab', None)
if tab_from_url is not None:
    try:
        tab_index = int(tab_from_url)
        if 0 <= tab_index <= 3:  # Validar índice (0-3 para 4 tabs)
            st.session_state.tab_selecionada_tc_ext_persistente = tab_index
    except ValueError:
        pass
# Se não houver parâmetro na URL, manter o valor atual do session_state
# Isso evita que a tab seja resetada quando há mudanças de filtros
# O valor já foi inicializado acima se não existir

# Manter compatibilidade com a chave antiga
st.session_state.tab_selecionada_tc_ext = st.session_state.tab_selecionada_tc_ext_persistente

# Só criar tabs e JavaScript se estivermos na página principal
if is_main_page:
    # JavaScript ANTES das tabs para interceptar a criação
    # Este script será executado antes que o Streamlit defina a primeira tab como padrão
    st.markdown(f"""
<script>
(function() {{
    // Obter índice da tab da URL
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
    
    // Interceptar a criação das tabs ANTES que sejam renderizadas
    // Usar MutationObserver para detectar quando as tabs são criadas
    const observerPrecoce = new MutationObserver(function(mutations) {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length >= 5) {{
            // Tabs foram criadas, verificar se a primeira está selecionada
            const primeiraTab = tabs[0];
            if (primeiraTab && primeiraTab.getAttribute('aria-selected') === 'true' && tabIndexDesejado !== 0) {{
                // Primeira tab está selecionada mas não deveria estar
                // Clicar na tab correta IMEDIATAMENTE
                if (tabs[tabIndexDesejado]) {{
                    // Usar requestAnimationFrame para garantir execução no próximo frame
                    requestAnimationFrame(function() {{
                        tabs[tabIndexDesejado].click();
                    }});
                }}
            }}
        }}
    }});
    
    // Começar a observar imediatamente
    observerPrecoce.observe(document.body, {{
        childList: true,
        subtree: true
    }});
    
    // Também tentar executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            const tabs = document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs.length >= 4 && tabIndexDesejado !== 0) {{
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

    # Criar estrutura de tabs para organização
    tab1, tab2, tab3, tab4 = st.tabs(["📊 TC Ext", "📈 Volume", "🚗 TC Ext por Veíc", "📋 Detalhe Real"])
else:
    # Se não estamos na página principal, criar tabs vazias para evitar erros
    # Mas não renderizar conteúdo
    tab1 = tab2 = tab3 = tab4 = None

# JavaScript DEPOIS das tabs para manter a seleção
st.markdown(f"""
<script>
(function() {{
    // Obter índice da tab da URL
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
    
    // Função para forçar a seleção da tab correta
    function forcarSelecaoTab(index) {{
        if (restauracaoEmAndamento) return;
        
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0 || index < 0 || index >= tabs.length) return;
        
        const tabAlvo = tabs[index];
        if (!tabAlvo) return;
        
        // Verificar se já está selecionada
        if (tabAlvo.getAttribute('aria-selected') === 'true') {{
            return; // Já está selecionada
        }}
        
        restauracaoEmAndamento = true;
        
        // Múltiplas tentativas de clicar
        function tentarClicar() {{
            tabAlvo.click();
            
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
    
    // Função para verificar e restaurar
    function verificarERestaurar() {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0) return;
        
        // Atualizar da URL
        tabIndexSalvo = obterTabIndex();
        
        // Verificar qual tab está selecionada
        let tabAtual = -1;
        tabs.forEach((tab, index) => {{
            if (tab.getAttribute('aria-selected') === 'true') {{
                tabAtual = index;
            }}
        }});
        
        // Se não está na tab correta, restaurar
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
                
                // Também salvar no sessionStorage para persistência entre recarregamentos
                sessionStorage.setItem('tab_selecionada_tc_ext', index);
                
                // Atualizar session_state no Streamlit via query params
                // Isso garante que o Streamlit saiba qual tab está selecionada
                if (window.parent && window.parent.postMessage) {{
                    window.parent.postMessage({{
                        type: 'streamlit:setFrameHeight',
                        height: document.body.scrollHeight
                    }}, '*');
                }}
            }}, true);
        }});
    }}
    
    // Tentar restaurar do sessionStorage se não houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_ext');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // Só usar sessionStorage se não houver parâmetro na URL
                    tabIndexSalvo = index;
                    const url = new URL(window.location);
                    url.searchParams.set('tab', index);
                    window.history.replaceState({{}}, '', url);
                }}
            }}
        }}
    }}
    
    // Tentar restaurar do sessionStorage se não houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_ext');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // Só usar sessionStorage se não houver parâmetro na URL
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
        // DOM já está pronto
        restaurarDeSessionStorage();
        verificarERestaurar();
        configurarListeners();
    }}
    
    // Executar periodicamente (mais frequente para garantir)
    // IMPORTANTE: Reduzir frequência para evitar conflitos com recarregamentos do Streamlit
    setInterval(function() {{
        verificarERestaurar();
    }}, 200);
    
    // Observar mudanças no DOM
    const observer = new MutationObserver(function() {{
        verificarERestaurar();
        configurarListeners();
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
    
    // Executar em múltiplos momentos para garantir
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
# Só exibir conteúdo das tabs se estivermos na página principal
if is_main_page:
    # Preparar volume filtrado pela sidebar para uso em CPU (antes das tabs)
    df_vol_filtrado_sidebar = None
    try:
        df_vol_base = load_volume_data(ano_selecionado)
        if df_vol_base is not None and 'Volume' in df_vol_base.columns:
            df_vol_filtrado_sidebar = filtrar_volume_com_sidebar(df_vol_base, df_total)
    except Exception:
        df_vol_filtrado_sidebar = None

    # Criar df_visualizacao a partir de df_filtrado antes de usar nas tabs
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_visualizacao = df_filtrado.copy()
        # Definir coluna_visualizacao baseado no tipo_visualizacao
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            coluna_visualizacao = 'CPU'
            if 'Volume' not in df_visualizacao.columns and df_vol_filtrado_sidebar is not None:
                if 'Volume' in df_vol_filtrado_sidebar.columns:
                    chaves_merge = [
                        col for col in ['Oficina', 'Veículo', 'Período', 'Ano']
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
        # Se df_filtrado não estiver disponível, criar DataFrame vazio
        df_visualizacao = pd.DataFrame()
        coluna_visualizacao = 'Total'
    
    # Criar df_para_grafico_periodo a partir de df_filtrado (antes do filtro de período)
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_para_grafico_periodo = df_filtrado.copy()
    else:
        df_para_grafico_periodo = pd.DataFrame()
    
    with tab1:
        # Exibir gráfico por Período
        # No modo CPU, a coluna 'CPU' pode não existir ainda em df_visualizacao,
        # mas será criada dentro do bloco. Verificar apenas se 'Período' existe.
        if 'Período' in df_visualizacao.columns:
            # IMPORTANTE: Criar df_visualizacao_para_grafico usando df_para_grafico_periodo
            # (dados ANTES do filtro de período) para mostrar TODOS os períodos no gráfico
            # Aplicar a mesma lógica de preparação de dados, mas usando df_para_grafico_periodo
            
            # Carregar dados de volume reais (necessário para cálculo de FLEX)
            df_vol_calc_grafico = load_volume_data(ano_selecionado)
        
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
                if ('Oficina' in df_para_grafico_periodo.columns and
                        'Período' in df_para_grafico_periodo.columns):
                    tem_veiculo = 'Veículo' in df_para_grafico_periodo.columns
                    tem_ano = 'Ano' in df_para_grafico_periodo.columns
                    
                    # Aplicar mesmos filtros de Veículo e Oficina ao volume
                    df_vol_calc_filtrado_grafico = df_vol_calc_grafico.copy()
                if tem_veiculo and 'Veículo' in df_vol_calc_filtrado_grafico.columns:
                    veiculos_filtrados = df_para_grafico_periodo['Veículo'].dropna().unique()
                    if len(veiculos_filtrados) > 0:
                        df_vol_calc_filtrado_grafico = df_vol_calc_filtrado_grafico[
                            df_vol_calc_filtrado_grafico['Veículo'].isin(veiculos_filtrados)
                        ].copy()
                if 'Oficina' in df_para_grafico_periodo.columns and 'Oficina' in df_vol_calc_filtrado_grafico.columns:
                    oficinas_filtradas = df_para_grafico_periodo['Oficina'].dropna().unique()
                    if len(oficinas_filtradas) > 0:
                        df_vol_calc_filtrado_grafico = df_vol_calc_filtrado_grafico[
                            df_vol_calc_filtrado_grafico['Oficina'].isin(oficinas_filtradas)
                        ].copy()
                
                colunas_agrupamento_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_grafico.append('Veículo')
                
                if 'Total' in df_para_grafico_periodo.columns:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Total'].sum()
                else:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Valor'].sum()
                    df_total_agrupado_grafico.rename(columns={'Valor': 'Total'}, inplace=True)
                
                colunas_agrupamento_vol_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_vol_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_vol_grafico.append('Veículo')
                
                df_vol_agrupado_grafico = df_vol_calc_filtrado_grafico.groupby(
                    colunas_agrupamento_vol_grafico, as_index=False
                )['Volume'].sum()
                
                # Fazer merge usando sempre 'left' para manter consistência
                # O df_total_agrupado_grafico já contém todas as combinações válidas
                # Usar 'left' garante que apenas combinações que existem nos dados sejam mantidas
                df_cpu_grafico = pd.merge(
                    df_total_agrupado_grafico,
                    df_vol_agrupado_grafico,
                    on=colunas_agrupamento_grafico,
                    how='left'
                )
                # Preencher valores faltantes de Volume com 0 (mas não criar novas linhas)
                df_cpu_grafico['Volume'] = df_cpu_grafico['Volume'].fillna(0)
                
                # Calcular CPU - VETORIZADO
                df_cpu_grafico['CPU'] = np.where(
                    (df_cpu_grafico['Volume'].notna()) & (df_cpu_grafico['Volume'] != 0),
                    df_cpu_grafico['Total'] / df_cpu_grafico['Volume'],
                    0
                )
                
                # IMPORTANTE: Manter colunas Total e Volume para que o gráfico possa recalcular CPU corretamente
                # O gráfico agrupa por Ano e Período e recalcula CPU a partir de Total e Volume agregados
                df_visualizacao_para_grafico = df_cpu_grafico.copy()
                coluna_visualizacao_grafico = 'CPU'
            else:
                df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
                coluna_visualizacao_grafico = 'Total' if 'Total' in df_para_grafico_periodo.columns else 'Valor'
        else:
            df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
            coluna_visualizacao_grafico = 'Total' if 'Total' in df_para_grafico_periodo.columns else 'Valor'
        
        # Filtros específicos para este gráfico (multiselect)
        df_grafico_periodo = df_visualizacao_para_grafico.copy()
        
        # Inicializar variáveis de filtro
        oficina_selecionadas_grafico = ["Todos"]
        veiculo_selecionados_grafico = ["Todos"]
        
        # Criar colunas para os filtros
        col1, col2 = st.columns(2)
        
        # Filtro de Oficina
        with col1:
            if 'Oficina' in df_grafico_periodo.columns:
                oficina_opcoes_grafico = get_filter_options(df_grafico_periodo, 'Oficina')
                oficina_selecionadas_grafico = st.multiselect(
                    "🏭 Filtrar por Oficina:",
                    oficina_opcoes_grafico,
                    default=["Todos"],
                    key="filtro_oficina_grafico_periodo"
                )
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
        
        # Filtro de Veículo
        with col2:
            if 'Veículo' in df_grafico_periodo.columns:
                veiculo_opcoes_grafico = get_filter_options(df_grafico_periodo, 'Veículo')
                veiculo_selecionados_grafico = st.multiselect(
                    "🚗 Filtrar por Veículo:",
                    veiculo_opcoes_grafico,
                    default=["Todos"],
                    key="filtro_veiculo_grafico_periodo"
                )
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        # IMPORTANTE: Quando "Todos" está selecionado, garantir que todos os períodos de todos os anos sejam mostrados
        # O create_period_chart já faz o agrupamento correto por Ano e Período quando há coluna Ano
        
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico aos dados de volume e budget DEPOIS que os filtros são definidos
        # Os filtros de Oficina e Veículo do gráfico devem ser aplicados a TODOS os dados (volumes, budget, etc.)
        
        # Carregar dados de budget e aplicar mesmos filtros
        df_budget_filtrado = None
        df_budget_vol_filtrado = None
        
        try:
            # Carregar dados de budget
            df_budget = load_budget_data(ano_selecionado)
            df_budget_vol = load_budget_volume_data(ano_selecionado)
            
            if df_budget is not None:
                # 🔧 CORREÇÃO CRÍTICA: Aplicar fator de conversão na coluna Total do budget (mesma unidade que Total real)
                # Isso mantém os dados na mesma unidade para comparações consistentes
                # IMPORTANTE: NÃO aplicar fator quando está em modo CPU (CPU já é uma razão)
                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Total' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Total'] = df_budget['Total'] / 1000
                    elif fator_conversao == "M (Milhões)":
                        df_budget['Total'] = df_budget['Total'] / 1000000
                
                # Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
                # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
                if moeda_codigo != "BRL" and 'Total' in df_budget.columns:
                    df_budget = converter_coluna_moeda(df_budget, 'Total', moeda_codigo, taxas_cambio)
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros da sidebar aos dados de budget (mesmos de df_para_grafico_periodo)
                df_budget_filtrado = df_budget.copy()
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros que existem em df_para_grafico_periodo
                # (Oficina, Veículo, USI, e outros filtros da sidebar)
                if 'df_para_grafico_periodo' in locals() and df_para_grafico_periodo is not None and len(df_para_grafico_periodo) > 0:
                    # Aplicar filtro de Veículo
                    if 'Veículo' in df_para_grafico_periodo.columns and 'Veículo' in df_budget_filtrado.columns:
                        veiculos_filtrados = df_para_grafico_periodo['Veículo'].dropna().unique()
                        if len(veiculos_filtrados) > 0:
                            df_budget_filtrado = df_budget_filtrado[
                                df_budget_filtrado['Veículo'].isin(veiculos_filtrados)
                            ].copy()
                    
                    # Aplicar filtro de Oficina
                    if 'Oficina' in df_para_grafico_periodo.columns and 'Oficina' in df_budget_filtrado.columns:
                        oficinas_filtradas = df_para_grafico_periodo['Oficina'].dropna().unique()
                        if len(oficinas_filtradas) > 0:
                            df_budget_filtrado = df_budget_filtrado[
                                df_budget_filtrado['Oficina'].isin(oficinas_filtradas)
                            ].copy()
                    
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de USI (importante para TC Ext)
                    if 'USI' in df_para_grafico_periodo.columns and 'USI' in df_budget_filtrado.columns:
                        usi_filtradas = df_para_grafico_periodo['USI'].dropna().unique()
                        if len(usi_filtradas) > 0:
                            df_budget_filtrado = df_budget_filtrado[
                                df_budget_filtrado['USI'].isin(usi_filtradas)
                            ].copy()
                    
                    # Aplicar outros filtros comuns (se existirem)
                    colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                    for col_filtro in colunas_filtro_comuns:
                        if col_filtro in df_para_grafico_periodo.columns and col_filtro in df_budget_filtrado.columns:
                            valores_filtrados = df_para_grafico_periodo[col_filtro].dropna().unique()
                            if len(valores_filtrados) > 0:
                                df_budget_filtrado = df_budget_filtrado[
                                    df_budget_filtrado[col_filtro].isin(valores_filtrados)
                                ].copy()
                else:
                    # Fallback: usar filtros do gráfico (comportamento antigo)
                    # Aplicar filtro de Oficina
                    if 'Oficina' in df_budget_filtrado.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_budget_filtrado = df_budget_filtrado[
                                df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de Veículo
                    if 'Veículo' in df_budget_filtrado.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_budget_filtrado = df_budget_filtrado[
                                df_budget_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
            
            if df_budget_vol is not None:
                # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros da sidebar ao volume de budget (mesmos de df_para_grafico_periodo)
                # O volume de budget precisa ter os mesmos filtros que o volume real para garantir consistência
                df_budget_vol_filtrado = df_budget_vol.copy()
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros que existem em df_para_grafico_periodo
                # (Oficina, Veículo, USI, e outros filtros da sidebar)
                if 'df_para_grafico_periodo' in locals() and df_para_grafico_periodo is not None and len(df_para_grafico_periodo) > 0:
                    # Aplicar filtro de Veículo
                    if 'Veículo' in df_para_grafico_periodo.columns and 'Veículo' in df_budget_vol_filtrado.columns:
                        veiculos_filtrados = df_para_grafico_periodo['Veículo'].dropna().unique()
                        if len(veiculos_filtrados) > 0:
                            df_budget_vol_filtrado = df_budget_vol_filtrado[
                                df_budget_vol_filtrado['Veículo'].isin(veiculos_filtrados)
                            ].copy()
                    
                    # Aplicar filtro de Oficina
                    if 'Oficina' in df_para_grafico_periodo.columns and 'Oficina' in df_budget_vol_filtrado.columns:
                        oficinas_filtradas = df_para_grafico_periodo['Oficina'].dropna().unique()
                        if len(oficinas_filtradas) > 0:
                            df_budget_vol_filtrado = df_budget_vol_filtrado[
                                df_budget_vol_filtrado['Oficina'].isin(oficinas_filtradas)
                            ].copy()
                    
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de USI (importante para TC Ext)
                    if 'USI' in df_para_grafico_periodo.columns and 'USI' in df_budget_vol_filtrado.columns:
                        usi_filtradas = df_para_grafico_periodo['USI'].dropna().unique()
                        if len(usi_filtradas) > 0:
                            df_budget_vol_filtrado = df_budget_vol_filtrado[
                                df_budget_vol_filtrado['USI'].isin(usi_filtradas)
                            ].copy()
                    
                    # Aplicar outros filtros comuns (se existirem)
                    colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                    for col_filtro in colunas_filtro_comuns:
                        if col_filtro in df_para_grafico_periodo.columns and col_filtro in df_budget_vol_filtrado.columns:
                            valores_filtrados = df_para_grafico_periodo[col_filtro].dropna().unique()
                            if len(valores_filtrados) > 0:
                                df_budget_vol_filtrado = df_budget_vol_filtrado[
                                    df_budget_vol_filtrado[col_filtro].isin(valores_filtrados)
                                ].copy()
                else:
                    # Fallback: usar filtros do gráfico (comportamento antigo)
                    # Aplicar filtro de Oficina
                    if 'Oficina' in df_budget_vol_filtrado.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_budget_vol_filtrado = df_budget_vol_filtrado[
                                df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de Veículo
                    if 'Veículo' in df_budget_vol_filtrado.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_budget_vol_filtrado = df_budget_vol_filtrado[
                                df_budget_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
        except Exception as e:
            st.sidebar.warning(f"⚠️ Erro ao carregar dados de budget: {e}")
        
        # Criar gráfico com dados filtrados (usar coluna_visualizacao_grafico que foi criada acima)
        # O create_period_chart já faz o agrupamento correto por Ano e Período quando há coluna Ano
        # Preparar dados de volume reais para cálculo de FLEX
        # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros da sidebar ao volume (mesmos de df_para_grafico_periodo)
        # O volume precisa ter os mesmos filtros que os dados reais para garantir consistência
        df_volume_real_filtrado = None
        if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
            df_volume_real_filtrado = df_vol_calc_grafico.copy()
            
            # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros que existem em df_para_grafico_periodo
            # (Oficina, Veículo, USI, e outros filtros da sidebar)
            if 'df_para_grafico_periodo' in locals() and df_para_grafico_periodo is not None and len(df_para_grafico_periodo) > 0:
                # Aplicar filtro de Veículo
                if 'Veículo' in df_para_grafico_periodo.columns and 'Veículo' in df_volume_real_filtrado.columns:
                    veiculos_filtrados = df_para_grafico_periodo['Veículo'].dropna().unique()
                    if len(veiculos_filtrados) > 0:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Veículo'].isin(veiculos_filtrados)
                        ].copy()
                
                # Aplicar filtro de Oficina
                if 'Oficina' in df_para_grafico_periodo.columns and 'Oficina' in df_volume_real_filtrado.columns:
                    oficinas_filtradas = df_para_grafico_periodo['Oficina'].dropna().unique()
                    if len(oficinas_filtradas) > 0:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Oficina'].isin(oficinas_filtradas)
                        ].copy()
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de USI (importante para TC Ext)
                if 'USI' in df_para_grafico_periodo.columns and 'USI' in df_volume_real_filtrado.columns:
                    usi_filtradas = df_para_grafico_periodo['USI'].dropna().unique()
                    if len(usi_filtradas) > 0:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['USI'].isin(usi_filtradas)
                        ].copy()
                
                # Aplicar outros filtros comuns (se existirem)
                colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                for col_filtro in colunas_filtro_comuns:
                    if col_filtro in df_para_grafico_periodo.columns and col_filtro in df_volume_real_filtrado.columns:
                        valores_filtrados = df_para_grafico_periodo[col_filtro].dropna().unique()
                        if len(valores_filtrados) > 0:
                            df_volume_real_filtrado = df_volume_real_filtrado[
                                df_volume_real_filtrado[col_filtro].isin(valores_filtrados)
                            ].copy()
            else:
                # Fallback: usar filtros do gráfico (comportamento antigo)
                # Aplicar filtro de Oficina
                if 'Oficina' in df_volume_real_filtrado.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Aplicar filtro de Veículo
                if 'Veículo' in df_volume_real_filtrado.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
            
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) ao volume DEPOIS que os filtros são definidos
            # Isso garante que o volume responda aos filtros do gráfico
            if df_volume_real_filtrado is not None:
                # Aplicar filtro de Oficina do gráfico
                if 'Oficina' in df_volume_real_filtrado.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Aplicar filtro de Veículo do gráfico
                if 'Veículo' in df_volume_real_filtrado.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) aos dados de budget DEPOIS que os filtros são definidos
        # Isso garante que os dados de budget respondam aos filtros do gráfico
        if df_budget_filtrado is not None:
            # Aplicar filtro de Oficina do gráfico
            if 'Oficina' in df_budget_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de Veículo do gráfico
            if 'Veículo' in df_budget_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        if df_budget_vol_filtrado is not None:
            # Aplicar filtro de Oficina do gráfico
            if 'Oficina' in df_budget_vol_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de Veículo do gráfico
            if 'Veículo' in df_budget_vol_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()

        # 📊 Resumo TC Ext (acima do gráfico) - mesmos indicadores da tabela Flex
        try:
            if (
                df_budget_filtrado is not None
                and df_budget_vol_filtrado is not None
                and df_volume_real_filtrado is not None
                and 'Total' in df_budget_filtrado.columns
                and 'Total' in df_filtrado.columns
            ):
                # Totais (já estão na mesma moeda/fator aplicados nos DataFrames)
                total_real_custo = pd.to_numeric(df_filtrado['Total'], errors='coerce').fillna(0).sum()

                # Aplicar também os filtros do gráfico (Oficina e Veículo) ao total real do resumo
                df_real_para_resumo = df_filtrado.copy()
                if 'Oficina' in df_real_para_resumo.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'Veículo' in df_real_para_resumo.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                total_real_custo = pd.to_numeric(df_real_para_resumo['Total'], errors='coerce').fillna(0).sum()

                bud_total_custo = pd.to_numeric(df_budget_filtrado['Total'], errors='coerce').fillna(0).sum()

                volume_real_total = 0.0
                if 'Volume' in df_volume_real_filtrado.columns:
                    volume_real_total = float(pd.to_numeric(df_volume_real_filtrado['Volume'], errors='coerce').fillna(0).sum())

                volume_budget_total = 0.0
                if 'Volume' in df_budget_vol_filtrado.columns:
                    volume_budget_total = float(pd.to_numeric(df_budget_vol_filtrado['Volume'], errors='coerce').fillna(0).sum())

                # Split budget por custo (Fixo/Variável) para cálculo do Flex BUD
                bud_fixo = 0.0
                bud_variavel = 0.0
                if 'Custo' in df_budget_filtrado.columns:
                    custo_str = df_budget_filtrado['Custo'].astype(str).str.lower()
                    mask_fixo = custo_str.str.startswith('fix')
                    mask_variavel = custo_str.str.startswith('vari')
                    bud_fixo = pd.to_numeric(df_budget_filtrado.loc[mask_fixo, 'Total'], errors='coerce').fillna(0).sum()
                    bud_variavel = pd.to_numeric(df_budget_filtrado.loc[mask_variavel, 'Total'], errors='coerce').fillna(0).sum()
                else:
                    bud_variavel = float(bud_total_custo)

                proporcao_volume = (volume_real_total / volume_budget_total) if volume_budget_total not in (0, None) else 1.0
                flex_bud_total_custo = float(bud_fixo + (bud_variavel * proporcao_volume))

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
                        elif fator_conversao == "M (Milhões)":
                            sufixo = " M"

                flex_menos_bud = flex_exibir - bud_exibir
                total_menos_flex = total_exibir - flex_exibir
                total_div_flex = (total_exibir / flex_exibir) if flex_exibir not in (0, None) else 0.0

                def _fmt_val(v):
                    return f"{v:,.2f}{sufixo}"

                st.subheader("📊 Resumo TC Ext")
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
            # Se algo der errado no resumo, não quebrar a tela.
            pass
        
        # No modo CPU, precisamos passar os dados originais (com 'Custo') para calcular FLEX
        # 🔧 CORREÇÃO CRÍTICA: Usar df_total diretamente (que tem 'Custo') em vez de df_para_grafico_periodo
        # porque df_para_grafico_periodo pode não ter 'Custo' se foi processado
        df_real_original_grafico = None
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # IMPORTANTE: No modo CPU, usar df_filtrado diretamente que já tem TODOS os filtros aplicados
            # e tem a coluna 'Custo' necessária para calcular Flex Bud
            # df_filtrado já tem a conversão de moeda aplicada e todos os filtros da sidebar
            if 'Custo' in df_filtrado.columns and 'Total' in df_filtrado.columns:
                df_real_original_grafico = df_filtrado.copy()
                
                # Aplicar apenas os filtros do gráfico (Oficina e Veículo) se diferentes dos da sidebar
                # Filtro de Oficina do gráfico
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Filtro de Veículo do gráfico
                if 'Veículo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # NOTA: A conversão de moeda já foi aplicada no df_total (linha 1104) e df_filtrado herda isso
                # Portanto, df_real_original_grafico['Total'] já está convertido
                
                # 🔧 VERIFICAÇÃO: Garantir que df_real_original_grafico tem dados válidos após aplicar filtros
                if len(df_real_original_grafico) == 0:
                    st.warning("⚠️ Aviso: df_real_original_grafico está vazio após aplicar filtros. Verifique os filtros selecionados.")
                elif 'Total' in df_real_original_grafico.columns and abs(df_real_original_grafico['Total'].sum()) < 0.0001:
                    st.warning("⚠️ Aviso: df_real_original_grafico tem Total muito próximo de zero. Verifique os dados e filtros.")
            else:
                # Fallback: tentar usar df_para_grafico_periodo se df_total não tiver 'Custo'
                df_real_original_grafico = df_para_grafico_periodo.copy()
                # Aplicar mesmos filtros de Oficina e Veículo
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'Veículo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # Observação: meses faltantes são tratados no create_period_chart
        # (períodos do budget entram apenas no eixo; realizado fica vazio/zero)
        
        # Exibir título do gráfico após os filtros para evitar sobreposição
        st.markdown("<br>", unsafe_allow_html=True)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("📊 CPU por Período")
        else:
            st.subheader("📊 Soma do Valor por Período")
        
        # Validar dados antes de criar gráfico
        if df_grafico_periodo is None or df_grafico_periodo.empty:
            st.warning("⚠️ Dados do gráfico estão vazios. Verifique os filtros aplicados.")
        elif coluna_visualizacao_grafico not in df_grafico_periodo.columns:
            st.warning(f"⚠️ Coluna '{coluna_visualizacao_grafico}' não encontrada nos dados do gráfico.")
            st.warning(f"⚠️ Colunas disponíveis: {list(df_grafico_periodo.columns)[:10]}")
        else:
            # Criar placeholder para o gráfico (força renderização imediata)
            chart_placeholder = st.empty()
            
            # Criar gráfico (sem spinner para evitar bloqueio de renderização)
            try:
                if 'Período' not in df_grafico_periodo.columns:
                    chart_placeholder.error("❌ Coluna 'Período' não encontrada nos dados do gráfico.")
                elif df_grafico_periodo[coluna_visualizacao_grafico].isna().all():
                    chart_placeholder.warning("⚠️ Todos os valores na coluna são NaN. Verifique os dados.")
                else:
                    grafico_periodo = create_period_chart(
                        df_grafico_periodo, coluna_visualizacao_grafico, tipo_visualizacao,
                        df_budget_filtrado, df_budget_vol_filtrado, df_volume_real_filtrado,
                        df_real_original_grafico,  # Dados originais com 'Custo' para calcular FLEX
                        moeda_simbolo  # Passar símbolo da moeda para o gráfico
                    )
                    if grafico_periodo is not None:
                        # Exibir gráfico no placeholder (renderização imediata)
                        chart_placeholder.altair_chart(grafico_periodo, use_container_width=True)
                    else:
                        chart_placeholder.warning("⚠️ O gráfico não pôde ser criado. Verifique os dados e filtros aplicados.")
            except Exception as e:
                import traceback
                chart_placeholder.error(f"❌ Erro ao criar gráfico: {str(e)}")
                chart_placeholder.code(traceback.format_exc())
        
        # Tabela: Análise Flex Bud por Categoria
        if df_budget_filtrado is not None and df_budget_vol_filtrado is not None and df_volume_real_filtrado is not None:
            st.markdown("---")
            # Adicionar elemento com ID para scroll
            st.markdown('<div id="analise-flex-bud-por-categoria"></div>', unsafe_allow_html=True)
            st.subheader("📊 Análise Flex por Categoria")
            
            # Verificar se temos coluna 'Custo' nos dados
            tem_custo_real = False
            if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns:
                tem_custo_real = True
            elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns:
                tem_custo_real = True
            
            if 'Custo' in df_budget_filtrado.columns and tem_custo_real:
                # 🔧 CORREÇÃO CRÍTICA: Preparar dados reais para a tabela
                # IMPORTANTE: No modo CPU, precisamos de dados com Total em Custo Total (não em CPU)
                # Priorizar df_real_original_grafico que vem diretamente de df_total (sem processamento de CPU)
                if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns and 'Total' in df_real_original_grafico.columns:
                    df_real_tabela = df_real_original_grafico.copy()
                elif 'df_filtrado' in locals() and df_filtrado is not None and 'Custo' in df_filtrado.columns and 'Total' in df_filtrado.columns:
                    # Usar df_filtrado que tem Total em Custo Total (sem processamento de CPU)
                    df_real_tabela = df_filtrado.copy()
                elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns and 'Total' in df_grafico_periodo.columns:
                    # Fallback: usar df_grafico_periodo se tiver Total (pode estar em CPU, mas vamos verificar)
                    # Se estiver em modo CPU e df_grafico_periodo tem CPU mas não Total, não usar
                    df_real_tabela = df_grafico_periodo.copy()
                else:
                    df_real_tabela = None
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) aos dados reais da tabela
                # Isso garante que a tabela responda aos filtros do gráfico
                if df_real_tabela is not None:
                    # Aplicar filtro de Oficina do gráfico
                    if 'Oficina' in df_real_tabela.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de Veículo do gráfico
                    if 'Veículo' in df_real_tabela.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                
                if df_real_tabela is None or len(df_real_tabela) == 0:
                    st.info("ℹ️ Não há dados reais disponíveis para criar a tabela Flex Bud.")
                elif 'Custo' not in df_real_tabela.columns:
                    st.error("❌ Erro: df_real_tabela não tem coluna 'Custo'. Verifique a origem dos dados.")
                elif 'Total' not in df_real_tabela.columns:
                    st.error("❌ Erro: df_real_tabela não tem coluna 'Total'. Verifique a origem dos dados.")
                else:
                    # Agrupar dados reais por Custo, Type 05, Type 06, Account (se existir)
                    colunas_agrupamento = ['Custo']
                    if 'Type 05' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 05')
                    if 'Type 06' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 06')
                    if 'Account' in df_real_tabela.columns:
                        colunas_agrupamento.append('Account')
                    
                    # 🔧 CORREÇÃO: Calcular Flex Bud POR PERÍODO primeiro (mesma lógica do gráfico)
                    # Incluir Período no agrupamento para calcular por período
                    colunas_agrupamento_com_periodo = colunas_agrupamento.copy()
                    if 'Período' in df_real_tabela.columns:
                        colunas_agrupamento_com_periodo.append('Período')
                    
                    # 🔧 VERIFICAÇÃO: Garantir que df_real_tabela tem Total em Custo Total (não em CPU)
                    # Se df_real_tabela tem coluna 'CPU' mas não 'Total', há problema
                    if 'Total' not in df_real_tabela.columns:
                        st.error("❌ Erro: df_real_tabela não tem coluna 'Total'. Verifique a origem dos dados.")
                        df_real_agrupado = pd.DataFrame()
                    else:
                        # Agrupar dados reais por categoria E período
                        # IMPORTANTE: Não verificar se Total está zerado antes de agrupar, pois pode haver
                        # valores positivos e negativos que se cancelam no total, mas são válidos por categoria
                        df_real_agrupado = df_real_tabela.groupby(colunas_agrupamento_com_periodo)['Total'].sum().reset_index()
                    
                    # Agrupar dados de budget por categoria E período
                    colunas_budget_periodo = [col for col in colunas_agrupamento_com_periodo if col in df_budget_filtrado.columns]
                    df_budget_agrupado = df_budget_filtrado.groupby(colunas_budget_periodo)['Total'].sum().reset_index()
                    
                    # 🔧 CORREÇÃO: Tab TC Ext usa dados de BUDGET reais (df_budget_agrupado)
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
                        if 'Período' in df_vol_real_para_agrupar.columns:
                            df_vol_real_agrupado = df_vol_real_para_agrupar.groupby('Período')['Volume'].sum().reset_index()
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
                        if 'Período' in df_vol_budget_para_agrupar.columns:
                            df_vol_budget_agrupado = df_vol_budget_para_agrupar.groupby('Período')['Volume'].sum().reset_index()
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
                    
                    # Merge com volumes (real e budget) apenas por Período
                    if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_real_agrupado[['Período', 'Volume']].rename(columns={'Volume': 'Volume_Real'}),
                            on='Período',
                            how='left'
                        )
                        df_tabela_flex['Volume_Real'] = df_tabela_flex['Volume_Real'].fillna(0)
                    elif len(df_vol_real_agrupado) > 0:
                        volume_total_real = df_vol_real_agrupado['Volume'].sum() if 'Volume' in df_vol_real_agrupado.columns else 0
                        df_tabela_flex['Volume_Real'] = volume_total_real
                    else:
                        df_tabela_flex['Volume_Real'] = 0
                    
                    if len(df_vol_budget_agrupado) > 0 and 'Período' in df_vol_budget_agrupado.columns:
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_budget_agrupado[['Período', 'Volume']].rename(columns={'Volume': 'Volume_Budget'}),
                            on='Período',
                            how='left'
                        )
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex['Volume_Budget'].fillna(0)
                    elif len(df_vol_budget_agrupado) > 0:
                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum() if 'Volume' in df_vol_budget_agrupado.columns else 0
                        df_tabela_flex['Volume_Budget'] = volume_total_budget
                    else:
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex.get('Volume_Real', 0)
                    
                    # Calcular Flex Bud usando operações vetorizadas (muito mais rápido)
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        # Calcular Flex Bud em Custo Total primeiro
                        # Fixo: Flex Bud = Budget
                        # Variável: Flex Bud = Budget * (Volume Real / Volume Budget)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operações vetorizadas ao invés de apply (muito mais rápido)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(df_tabela_flex['Custo'] == 'Fixo', 0)
                        df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(df_tabela_flex['Custo'] == 'Variável', 0)
                        
                        df_tabela_flex['_Flex_Bud_Total_Custo'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                        
                        # Converter para CPU
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Total_Custo'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['Flex BUD'].fillna(0)
                        
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['BUD'] = df_tabela_flex['BUD'].fillna(0)
                        
                        df_tabela_flex['Total'] = df_tabela_flex['Total'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Total'] = df_tabela_flex['Total'].fillna(0)
                        
                        # Guardar valores para agregação
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['_Flex_Bud_Total_Custo']
                        # 🔧 CORREÇÃO: _Total_Custo_Total deve ser o Total em Custo Total (antes da conversão para CPU)
                        # Total já está em CPU, então precisamos reverter multiplicando por Volume_Real
                        # Mas Volume_Real é o mesmo para todas as categorias do mesmo período (volume total do período)
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Total'] * df_tabela_flex['Volume_Real']  # Reverter para Custo Total
                    else:
                        # Custo Total
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operações vetorizadas ao invés de apply (muito mais rápido)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(df_tabela_flex['Custo'] == 'Fixo', 0)
                        df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(df_tabela_flex['Custo'] == 'Variável', 0)
                        
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo']
                        
                        # Guardar valores para agregação
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['Flex BUD']
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Total']
                    
                    # Guardar valores auxiliares
                    df_tabela_flex['_Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                    
                    # Calcular diferenças
                    df_tabela_flex['Flex Bud - BUD'] = df_tabela_flex['Flex BUD'] - df_tabela_flex['BUD']
                    df_tabela_flex['Total - Flex Bud'] = df_tabela_flex['Total'] - df_tabela_flex['Flex BUD']
                    # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                    df_tabela_flex['Total / Flex Bud'] = df_tabela_flex.apply(
                        lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                        axis=1
                    )
                    
                    # Remover colunas auxiliares temporárias
                    colunas_remover_temp = ['Budget_Total', 'Budget_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel']
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        colunas_remover_temp.append('_Flex_Bud_Total_Custo')
                    df_tabela_flex = df_tabela_flex.drop(columns=[col for col in colunas_remover_temp if col in df_tabela_flex.columns])
                    
                    if len(df_tabela_flex) > 0:
                        
                        # Seletor de período (linha superior)
                        if 'Período' in df_real_tabela.columns:
                            periodos_disponiveis = sorted(df_real_tabela['Período'].dropna().unique().tolist())
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
                            
                            # Novo filtro de períodos - versão simplificada
                            periodo_tabela_key = "filtro_periodo_tabela_flex"
                            
                            # Adicionar opção "Todos" no início da lista
                            opcoes_com_todos = ["Todos"] + periodos_ordenados
                            
                            # Inicializar session_state se necessário
                            if periodo_tabela_key not in st.session_state:
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Validar valores salvos
                            periodos_salvos = st.session_state[periodo_tabela_key]
                            periodos_validos = [p for p in periodos_salvos if p in opcoes_com_todos]
                            
                            # Se não houver períodos válidos, resetar para "Todos"
                            if not periodos_validos:
                                periodos_validos = ["Todos"]
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Adicionar CSS simples para prevenir scroll automático
                            st.markdown("""
                            <style>
                                /* Prevenir scroll automático do Streamlit */
                                html {
                                    scroll-behavior: auto !important;
                                }
                                /* Prevenir foco automático que causa scroll */
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
                            # 🔧 CORREÇÃO: Remover 'default' e usar apenas 'key' para evitar conflito
                            # O Streamlit automaticamente sincroniza o valor do widget com session_state[key]
                            periodos_tabela_raw = st.multiselect(
                                "📅 **Período(s):**",
                                opcoes_com_todos,
                                key=periodo_tabela_key
                            )
                            
                            # Atualizar session_state com o valor selecionado (caso tenha mudado)
                            if periodos_tabela_raw != st.session_state[periodo_tabela_key]:
                                st.session_state[periodo_tabela_key] = periodos_tabela_raw
                            
                            # Processar seleção
                            if "Todos" in periodos_tabela_raw:
                                # Se "Todos" está selecionado, selecionar todos os períodos para filtro
                                periodos_tabela = periodos_ordenados.copy()
                            else:
                                # Se "Todos" não está selecionado, usar apenas os períodos selecionados
                                periodos_tabela = [p for p in periodos_tabela_raw if p != "Todos"]
                            
                            # Se nenhum período foi selecionado, usar todos (mas mostrar apenas "Todos")
                            if not periodos_tabela:
                                periodos_tabela = periodos_ordenados.copy()
                        else:
                            periodos_tabela = []
                        
                        # Filtrar df_tabela_flex por períodos selecionados
                        # Inicializar variáveis
                        usar_colunas_por_periodo = False
                        periodos_ordenados_selecao = []
                        
                        if len(periodos_tabela) > 0 and 'Período' in df_tabela_flex.columns and len(df_tabela_flex) > 0:
                            # 🔧 IMPORTANTE: Salvar dados originais ANTES de filtrar (para usar em colunas por período)
                            df_tabela_flex_original = df_tabela_flex.copy()
                            
                            df_tabela_flex = df_tabela_flex[df_tabela_flex['Período'].isin(periodos_tabela)].copy()
                            
                            # 🔧 CRÍTICO: Salvar df_tabela_flex DEPOIS do filtro de período, mas ANTES de qualquer transformação
                            # Esta versão tem as colunas BUD, Flex BUD, Total originais e já está filtrada pelos períodos selecionados
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()
                            
                            # 🔧 NOVA LÓGICA: Se há múltiplos períodos, criar colunas separadas por período
                            # Manter a ordem de seleção dos períodos (periodos_tabela_raw mantém a ordem)
                            if len(periodos_tabela) > 1:
                                # Manter a ordem de seleção (usar periodos_tabela_raw se disponível, senão usar periodos_tabela)
                                if 'periodos_tabela_raw' in locals() and len(periodos_tabela_raw) > 0:
                                    periodos_ordenados_selecao = [p for p in periodos_tabela_raw if p != "Todos" and p in periodos_tabela]
                                else:
                                    periodos_ordenados_selecao = periodos_tabela.copy()
                                
                                # Se ainda não temos a ordem correta, usar periodos_tabela
                                if not periodos_ordenados_selecao:
                                    periodos_ordenados_selecao = periodos_tabela.copy()
                                
                                # Criar flag para indicar que vamos usar colunas por período
                                usar_colunas_por_periodo = True
                            else:
                                periodos_ordenados_selecao = periodos_tabela.copy()
                                usar_colunas_por_periodo = False
                            
                            # 🔧 CORREÇÃO CRÍTICA: Agregar corretamente quando há 1 ou múltiplos períodos
                            # (mesma lógica do gráfico - calcular Flex Bud por período primeiro, depois agregar)
                            # O gráfico sempre soma todas as categorias primeiro e depois calcula Flex Bud Total
                            # A tabela deve fazer o mesmo: somar _Flex_Bud_Total de todas as categorias e dividir pelo volume total
                            if len(periodos_tabela) >= 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "CPU (Custo por Unidade)":
                                # 🔧 CORREÇÃO CRÍTICA: O gráfico calcula Flex Bud por período, então devemos fazer o mesmo
                                # 1. Calcular Flex Bud por período e categoria (já feito acima)
                                # 2. Agregar por categoria somando Flex Bud Total e Volume Total
                                
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # 🔧 CORREÇÃO CRÍTICA: Usar volume TOTAL de todos os períodos selecionados (não por categoria)
                                # O gráfico calcula por período usando volume total do período, então devemos usar o mesmo aqui
                                # IMPORTANTE: O gráfico agrupa volumes por Período ANTES de calcular Flex BUD
                                # A tabela já tem df_vol_real_agrupado e df_vol_budget_agrupado que foram agrupados por Período
                                # Então devemos usar esses DataFrames agrupados para garantir consistência
                                if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                    # Usar o DataFrame já agrupado por Período (igual ao gráfico)
                                    volume_total_real = df_vol_real_agrupado[df_vol_real_agrupado['Período'].isin(periodos_tabela)]['Volume'].sum()
                                elif 'Período' in df_volume_real_filtrado.columns:
                                    # Fallback: agrupar por Período primeiro (igual ao gráfico), depois filtrar e somar
                                    df_vol_real_por_periodo = df_volume_real_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                    volume_total_real = df_vol_real_por_periodo[df_vol_real_por_periodo['Período'].isin(periodos_tabela)]['Volume'].sum()
                                else:
                                    volume_total_real = df_volume_real_filtrado['Volume'].sum()
                                
                                # 🔧 CORREÇÃO: No modo Real, Volume "Budget" = Volume Real do primeiro período
                                if len(df_vol_real_agrupado) > 0:
                                    # Obter períodos disponíveis do DataFrame
                                    periodos_disponiveis = sorted(df_tabela_flex['Período'].dropna().unique().tolist()) if 'Período' in df_tabela_flex.columns else []
                                    if len(periodos_disponiveis) > 0:
                                        primeiro_periodo = periodos_disponiveis[0]
                                        volume_total_budget = df_vol_real_agrupado[df_vol_real_agrupado['Período'] == primeiro_periodo]['Volume'].sum()
                                        if volume_total_budget == 0:
                                            # Se não encontrou, usar o volume total real como fallback
                                            volume_total_budget = volume_total_real
                                    else:
                                        volume_total_budget = volume_total_real
                                else:
                                    volume_total_budget = volume_total_real
                                
                                # 🔧 CORREÇÃO: Agrupar por categoria (sem período) - somar valores em Custo Total
                                # IMPORTANTE: Somar _Flex_Bud_Total que já está em Custo Total (calculado por período)
                                # df_tabela_flex_original já foi criado antes do filtro (linha acima)
                                
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    '_Flex_Bud_Total': 'sum',  # Flex Bud Total em Custo Total (soma de todos os períodos)
                                    '_Total_Custo_Total': 'sum',  # Total em Custo Total (soma de todos os períodos)
                                    '_Budget_Total': 'sum'  # Budget em Custo Total (soma de todos os períodos)
                                }).reset_index()
                                
                                # 🔧 CORREÇÃO CRÍTICA: Usar volume TOTAL de todos os períodos (não somar por categoria)
                                # O gráfico usa volume total por período, então quando agregamos múltiplos períodos,
                                # devemos usar a SOMA dos volumes de todos os períodos selecionados
                                df_agregado['_Volume_Real'] = volume_total_real
                                df_agregado['_Volume_Budget'] = volume_total_budget
                                
                                # Recalcular CPU usando operações vetorizadas (muito mais rápido)
                                # Flex BUD CPU = (Soma de Flex Bud Total de todos os períodos) / (Soma de Volume Real de todos os períodos)
                                df_agregado['Flex BUD'] = (df_agregado['_Flex_Bud_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['Total'] = (df_agregado['_Total_Custo_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['BUD'] = (df_agregado['_Budget_Total'] / df_agregado['_Volume_Budget'].replace(0, 1)).fillna(0)
                                
                                # Recalcular diferenças
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Total'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # 🔧 CORREÇÃO: Manter colunas auxiliares para o resumo geral recalcular corretamente
                                # Não remover ainda - serão removidas após o cálculo do resumo
                                # colunas_remover = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget']
                                # df_agregado = df_agregado.drop(columns=[col for col in colunas_remover if col in df_agregado.columns])
                                
                                # Se há múltiplos períodos e devemos usar colunas por período, criar estrutura pivot
                                if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1 and 'Período' in df_tabela_flex_original.columns:
                                    # Usar dados originais antes da agregação (ainda tem Período)
                                    df_tabela_flex_por_periodo = df_tabela_flex_original.copy()
                                    
                                    # Criar pivot table com períodos como colunas
                                    colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex_por_periodo.columns]
                                    
                                    # Criar pivot para cada métrica
                                    df_pivot_total = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='Total',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_flex = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='Flex BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_bud = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    # Reorganizar colunas na ordem de seleção
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
                                    
                                    # Remover colunas de período do pivot (vamos criar novas colunas)
                                    for col in df_pivot_total.columns:
                                        if col in df_final.columns:
                                            df_final = df_final.drop(columns=[col])
                                    
                                    # Primeiro período: Set/24, Flex set/24 (removendo coluna redundante)
                                    if primeiro_periodo and primeiro_periodo in df_pivot_total.columns:
                                        df_final[f"{primeiro_periodo_abrev}"] = df_pivot_total[primeiro_periodo].values
                                        df_final[f"Flex {primeiro_periodo_abrev.lower()}"] = df_pivot_flex[primeiro_periodo].values
                                    
                                    # Demais períodos: Out/24 - Flex set/24, out/24, % out/24/flex set/24
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
                                    # Adicionar Período: se houver apenas 1 período, manter o nome; se múltiplos, mostrar lista
                                    if len(periodos_tabela) == 1:
                                        df_agregado['Período'] = periodos_tabela[0]
                                    else:
                                        df_agregado['Período'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} períodos"
                                    
                                    df_tabela_flex = df_agregado
                            elif len(periodos_tabela) > 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "Custo Total":
                                # Para Custo Total: apenas somar por categoria (sem período)
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # Agrupar por categoria (sem período) e somar
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    'BUD': 'sum',
                                    'Flex BUD': 'sum',
                                    'Total': 'sum'
                                }).reset_index()
                                
                                # Recalcular diferenças
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Total'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # Adicionar Período como "Todos" ou lista de períodos
                                df_agregado['Período'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} períodos"
                                
                                df_tabela_flex = df_agregado
                        else:
                            # Se não houver filtro de período, usar df_tabela_flex diretamente
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()
                        
                        # 🔧 CORREÇÃO: Remover colunas auxiliares da tabela principal (para exibição)
                        # df_tabela_flex_para_resumo já foi salvo DEPOIS do filtro de período, mas ANTES das transformações
                        colunas_auxiliares = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget', '_Flex_Bud_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel', 'Volume_Real', 'Volume_Budget', 'Total_Budget']
                        colunas_para_remover = [col for col in colunas_auxiliares if col in df_tabela_flex.columns]
                        
                        # Remover colunas auxiliares da tabela principal (para exibição)
                        if colunas_para_remover:
                            df_tabela_flex = df_tabela_flex.drop(columns=colunas_para_remover)
                        
                        # Selecionador de visualização (linha inferior)
                        modo_tabela_flex = st.radio(
                            "📊 **Visualização:**",
                            ["Fixo/Variável", "Total"],
                            index=0,
                            horizontal=True,
                            key="modo_tabela_flex_bud"
                        )
                        
                        # Resumo geral (fora dos expanders)
                        # 🔧 CORREÇÃO: Usar DataFrame com colunas auxiliares para recalcular corretamente
                        if len(df_tabela_flex) > 0:
                            # 🔧 CORREÇÃO CRÍTICA: Obter volumes EXATAMENTE como o gráfico (mesmos DataFrames)
                            # O gráfico usa df_vol_real_agrupado e df_vol_budget_agrupado agrupados por Período
                            # IMPORTANTE: Usar os mesmos DataFrames e a mesma lógica do gráfico
                            volume_real_para_resumo = 0.0
                            volume_budget_para_resumo = 0.0
                            
                            # Obter períodos selecionados (mesma lógica usada acima)
                            periodos_para_volume = periodos_tabela if 'periodos_tabela' in locals() else []
                            if not periodos_para_volume:
                                # Se não houver períodos selecionados, usar todos os períodos disponíveis
                                if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                    periodos_para_volume = df_vol_real_agrupado['Período'].unique().tolist()
                            
                            # Obter volumes dos mesmos DataFrames que o gráfico usa
                            if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                # Usar o DataFrame já agrupado por Período (igual ao gráfico)
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_agrupado[df_vol_real_agrupado['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_agrupado['Volume'].sum()
                            elif 'Período' in df_volume_real_filtrado.columns:
                                # Fallback: agrupar por Período e somar (igual ao gráfico)
                                df_vol_real_temp = df_volume_real_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_temp[df_vol_real_temp['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_temp['Volume'].sum()
                            else:
                                volume_real_para_resumo = df_volume_real_filtrado['Volume'].sum() if 'Volume' in df_volume_real_filtrado.columns else 0.0
                            
                            # Volume Budget: usar df_budget_vol_filtrado (volume do budget, não do real)
                            if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                                # Agrupar volume de budget por Período
                                if 'Período' in df_budget_vol_filtrado.columns:
                                    df_vol_budget_temp = df_budget_vol_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                    if len(periodos_para_volume) > 0:
                                        volume_budget_para_resumo = df_vol_budget_temp[df_vol_budget_temp['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                    else:
                                        volume_budget_para_resumo = df_vol_budget_temp['Volume'].sum()
                                else:
                                    volume_budget_para_resumo = df_budget_vol_filtrado['Volume'].sum()
                            else:
                                # Fallback: se não houver volume de budget, usar volume real (comportamento antigo)
                                volume_budget_para_resumo = volume_real_para_resumo
                            
                            # 🔧 CORREÇÃO: Adaptar resumo para usar nomes das colunas dinâmicas (se usar_colunas_por_periodo)
                            if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1:
                                # Obter nomes das colunas dinâmicas do DataFrame
                                primeiro_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[0]) if len(periodos_ordenados_selecao) > 0 else ""
                                segundo_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[1]) if len(periodos_ordenados_selecao) > 1 else ""
                                
                                # Criar resumo com nomes dinâmicos
                                linha_resumo_geral = {}
                                linha_resumo_geral_formatado = {}
                                
                                # Obter colunas numéricas do DataFrame
                                colunas_numericas = [col for col in df_tabela_flex_para_resumo.columns 
                                                    if pd.api.types.is_numeric_dtype(df_tabela_flex_para_resumo[col]) 
                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                
                                for col in colunas_numericas:
                                    valor = df_tabela_flex_para_resumo[col].sum()
                                    linha_resumo_geral[col] = valor
                                    
                                    # Formatar valor
                                    if col.startswith('%'):
                                        # Usar formatar_ratio_com_barra para percentuais (dividir por 100 pois está em %)
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
                                            elif fator_conversao == "M (Milhões)":
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
                                # Usar função padrão para colunas fixas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(df_tabela_flex_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                
                                # 🔧 CORREÇÃO: Usar volumes do DataFrame (que já têm os filtros corretos aplicados)
                                # Os volumes em df_tabela_flex_para_resumo já foram calculados com todos os filtros
                                if 'Volume_Real' in df_tabela_flex_para_resumo.columns:
                                    # Volume Real: somar todos os volumes únicos (mesmo período tem mesmo volume)
                                    if 'Período' in df_tabela_flex_para_resumo.columns:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo.groupby('Período')['Volume_Real'].first().sum()
                                    else:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo['Volume_Real'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_real_para_resumo = linha_resumo_geral.get('_Volume_Real_Calculo', 0.0)
                                
                                if 'Volume_Budget' in df_tabela_flex_para_resumo.columns:
                                    # Volume Budget: somar volumes únicos por período
                                    if 'Período' in df_tabela_flex_para_resumo.columns:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo.groupby('Período')['Volume_Budget'].first().sum()
                                    else:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo['Volume_Budget'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_budget_para_resumo = linha_resumo_geral.get('_Volume_Budget_Calculo', 0.0)
                                
                                # Garantir que os volumes sejam sempre números (não NaN ou None)
                                if pd.isna(volume_real_para_resumo) or volume_real_para_resumo is None:
                                    volume_real_para_resumo = 0.0
                                if pd.isna(volume_budget_para_resumo) or volume_budget_para_resumo is None:
                                    volume_budget_para_resumo = 0.0
                                
                                linha_resumo_geral['_Volume_Real_Calculo'] = float(volume_real_para_resumo)
                                linha_resumo_geral['_Volume_Budget_Calculo'] = float(volume_budget_para_resumo)
                                linha_resumo_geral_formatado['_Volume_Real_Calculo'] = f"{float(volume_real_para_resumo):,.0f}"
                                linha_resumo_geral_formatado['_Volume_Budget_Calculo'] = f"{float(volume_budget_para_resumo):,.0f}"
                            
                            st.markdown("---")
                            st.markdown("### 📊 Resumo Geral")
                            # Exibir caixas de resumo com volumes
                            exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=True)
                            st.markdown("<br>", unsafe_allow_html=True)  # Pequeno espaço antes das tabelas
                        # Criar estrutura hierárquica com expanders
                        if modo_tabela_flex == "Fixo/Variável":
                            # 🔧 CORREÇÃO: Usar df_tabela_flex_para_resumo para cálculos de resumo (tem colunas originais)
                            # df_tabela_flex pode ter colunas por período (Jul, Ago, etc.) que não servem para resumo
                            df_para_resumo_fixo_variavel = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # Nível 1: Custo (Fixo/Variável) - separado
                            for custo in ['Fixo', 'Variável']:
                                df_custo = df_tabela_flex[df_tabela_flex['Custo'] == custo].copy()
                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                df_custo_para_resumo = df_para_resumo_fixo_variavel[df_para_resumo_fixo_variavel['Custo'] == custo].copy() if 'Custo' in df_para_resumo_fixo_variavel.columns else df_custo.copy()
                                
                                if len(df_custo) > 0:
                                    # 🔧 FILTRAR: Verificar se Custo tem valores não zerados
                                    colunas_numericas_custo_check = [col for col in df_custo.columns 
                                                                     if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                    if colunas_numericas_custo_check:
                                        df_custo_check = df_custo[colunas_numericas_custo_check].fillna(0)
                                        tem_valores_nao_zerados = (df_custo_check.abs().sum(axis=1) > 0.0001).any()
                                        if not tem_valores_nao_zerados:
                                            continue  # Pular Custo completamente zerado
                                    else:
                                        if 'Total' in df_custo.columns:
                                            if df_custo['Total'].fillna(0).abs().sum() <= 0.0001:
                                                continue  # Pular Custo completamente zerado
                                    
                                    # Verificar se a coluna 'Total' existe antes de acessá-la
                                    # 🔧 CORREÇÃO: Tentar usar df_custo_para_resumo primeiro (tem colunas originais)
                                    if 'Total' in df_custo_para_resumo.columns:
                                        total_custo = df_custo_para_resumo['Total'].sum()
                                    elif 'Total' in df_custo.columns:
                                        total_custo = df_custo['Total'].sum()
                                    else:
                                        # Se não houver coluna 'Total', usar 0 ou calcular a partir de outras colunas
                                        total_custo = 0.0
                                    total_custo_formatado = f"{total_custo:,.2f}"
                                    
                                    with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=False):
                                        # Resumo do Custo (Fixo ou Variável)
                                        # 🔧 CORREÇÃO: Usar df_custo_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                        linha_resumo_custo, linha_resumo_custo_formatado = calcular_resumo_tabela_flex(df_custo_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                        exibir_caixas_resumo(linha_resumo_custo, linha_resumo_custo_formatado, tipo_visualizacao)
                                        st.markdown("---")
                                        
                                        # Nível 2: Type 05 (se existir)
                                        if 'Type 05' in df_custo.columns:
                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                                df_type05_para_resumo = df_custo_para_resumo[df_custo_para_resumo['Type 05'] == type05].copy() if 'Type 05' in df_custo_para_resumo.columns else df_type05.copy()
                                                
                                                if len(df_type05) > 0:
                                                    # 🔧 FILTRAR: Verificar se Type 05 tem valores não zerados
                                                    colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                                     if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                    if colunas_numericas_type05_check:
                                                        df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                                        tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                                        if not tem_valores_nao_zerados:
                                                            continue  # Pular Type 05 completamente zerado
                                                    else:
                                                        if 'Total' in df_type05.columns:
                                                            if df_type05['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                continue  # Pular Type 05 completamente zerado
                                                    
                                                    # Verificar se a coluna 'Total' existe antes de acessá-la
                                                    if 'Total' in df_type05.columns:
                                                        total_type05 = df_type05['Total'].sum()
                                                    else:
                                                        total_type05 = 0.0
                                                    total_type05_formatado = f"{total_type05:,.2f}"
                                                    
                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                        # Nível 3: Type 06 (se existir)
                                                        if 'Type 06' in df_type05.columns:
                                                            for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                                                df_type06_para_resumo = df_type05_para_resumo[df_type05_para_resumo['Type 06'] == type06].copy() if 'Type 06' in df_type05_para_resumo.columns else df_type06.copy()
                                                                
                                                                if len(df_type06) > 0:
                                                                    # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores não zerados
                                                                    colunas_numericas_check = [col for col in df_type06.columns 
                                                                                              if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                              and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                    if colunas_numericas_check:
                                                                        # Verificar se há pelo menos uma linha com valores não zerados
                                                                        df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                        tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                        if not tem_valores_nao_zerados:
                                                                            continue  # Pular Type 06 completamente zerado
                                                                    else:
                                                                        # Se não há colunas numéricas, verificar se Total existe e é zero
                                                                        if 'Total' in df_type06.columns:
                                                                            if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                                continue  # Pular Type 06 completamente zerado
                                                                    
                                                                    # Verificar se a coluna 'Total' existe antes de acessá-la
                                                                    if 'Total' in df_type06.columns:
                                                                        total_type06 = df_type06['Total'].sum()
                                                                    else:
                                                                        total_type06 = 0.0
                                                                    total_type06_formatado = f"{total_type06:,.2f}"
                                                                    
                                                                    # Nível 4: Account (se existir)
                                                                    if 'Account' in df_type06.columns:
                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                        # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
                                                                            df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                            df_type06_filtrado = df_type06[
                                                                                df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                            ].copy()
                                                                        else:
                                                                            df_type06_filtrado = df_type06.copy()
                                                                        
                                                                        # Só exibir se houver dados após filtrar
                                                                        if len(df_type06_filtrado) > 0:
                                                                            # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                            # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar uma única tabela com todas as Accounts
                                                                                # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                                colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                                
                                                                                # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                                for col in colunas_numericas:
                                                                                    if col not in colunas_ordenadas:
                                                                                        colunas_ordenadas.append(col)
                                                                                
                                                                                colunas_display = colunas_id + colunas_ordenadas
                                                                                df_display = df_type06_filtrado[colunas_display].copy()
                                                                                
                                                                                # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                                                                for col in df_display.columns:
                                                                                    if col not in colunas_id:
                                                                                        # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, não percentual)
                                                                                        if col == 'Total / Flex Bud':
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                                        # Formatar percentuais de forma especial com barrinha
                                                                                        elif col.startswith('%'):
                                                                                            # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                        # Formatar outras colunas numéricas
                                                                                        elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                            else:
                                                                                                sufixo = ""
                                                                                                if fator_conversao:
                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                        sufixo = " K"
                                                                                                    elif fator_conversao == "M (Milhões)":
                                                                                                        sufixo = " M"
                                                                                                df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # 🔧 CORREÇÃO: Usar versão para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                if 'Account' in df_type06_para_resumo.columns:
                                                                                    # Aplicar mesmo filtro de linhas zeradas na versão para resumo
                                                                                    colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                                if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
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
                                                                                
                                                                                # Exibir tabela com resumo (todas as Accounts em uma única tabela)
                                                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                                st.markdown(html_table, unsafe_allow_html=True)
                                                                    else:
                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                        # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
                                                                            df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                            df_type06_filtrado = df_type06[
                                                                                df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                            ].copy()
                                                                        else:
                                                                            df_type06_filtrado = df_type06.copy()
                                                                        
                                                                        # Só exibir se houver dados após filtrar
                                                                        if len(df_type06_filtrado) > 0:
                                                                            # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                            # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar tabela para este Type 06
                                                                                # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                                colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'Período']]
                                                                                
                                                                                # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
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
                                                                                                elif fator_conversao == "M (Milhões)":
                                                                                                    sufixo = " M"
                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                
                                                                                # Formatar Total / Flex Bud com barra HTML
                                                                                if 'Total / Flex Bud' in df_display.columns:
                                                                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # 🔧 CORREÇÃO: Usar versão para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                            if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
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
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            colunas_numericas_type05 = [col for col in df_type05.columns 
                                                                                        if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                        and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas_type05:
                                                                df_type05_temp = df_type05[colunas_numericas_type05].fillna(0)
                                                                df_type05 = df_type05[
                                                                    df_type05_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            
                                                            # Criar tabela para este Type 05
                                                            # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                            colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                            colunas_numericas = [col for col in df_type05.columns 
                                                                                if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'Período']]
                                                            
                                                            # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                            ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                            colunas_ordenadas = []
                                                            for col_ordem in ordem_colunas:
                                                                if col_ordem in colunas_numericas:
                                                                    colunas_ordenadas.append(col_ordem)
                                                            # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
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
                                                                            elif fator_conversao == "M (Milhões)":
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
                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                            colunas_numericas_custo = [col for col in df_custo.columns 
                                                                       if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                       and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                            if colunas_numericas_custo:
                                                df_custo_temp = df_custo[colunas_numericas_custo].fillna(0)
                                                df_custo = df_custo[
                                                    df_custo_temp.abs().sum(axis=1) > 0.0001
                                                ].copy()
                                            
                                            # Criar tabela para este Custo
                                            # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                            colunas_id = ['Custo'] if 'Custo' in df_custo.columns else []
                                            colunas_numericas = [col for col in df_custo.columns 
                                                                if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Account', 'Período']]
                                            colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                            colunas_display = colunas_id + colunas_ordenadas
                                            df_display = df_custo[colunas_display].copy()
                                            
                                            # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                            for col in df_display.columns:
                                                if col not in colunas_id:
                                                    # Formatar percentuais de forma especial com barrinha
                                                    if col.startswith('%'):
                                                        # Usar formatar_ratio_com_barra para colunas de percentual (dividir por 100 pois está em %)
                                                        df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                    # Formatar outras colunas numéricas
                                                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                        else:
                                                            sufixo = ""
                                                            if fator_conversao:
                                                                if fator_conversao == "K (milhares)":
                                                                    sufixo = " K"
                                                                elif fator_conversao == "M (Milhões)":
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
                            # Modo "Total": não separar por Fixo/Variável
                            # Agrupar todos os dados sem separar por Custo
                            # Remover coluna Custo do agrupamento para exibição
                            df_tabela_total = df_tabela_flex.copy()
                            
                            # Verificar se df_tabela_total tem dados
                            if len(df_tabela_total) == 0:
                                st.warning("⚠️ Nenhum dado disponível para exibição no modo Total.")
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
                                        # Se não há colunas para agregar, usar todas as colunas numéricas disponíveis
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in colunas_agrupamento_total 
                                                           and col not in ['Custo', 'Período']]
                                        if len(colunas_numericas) > 0:
                                            df_tabela_total_agrupado = df_tabela_total.groupby(colunas_agrupamento_total).agg({
                                                col: 'sum' for col in colunas_numericas
                                            }).reset_index()
                                        else:
                                            st.warning("⚠️ Nenhuma coluna numérica encontrada em df_tabela_total. Colunas disponíveis: " + ", ".join(df_tabela_total.columns.tolist()))
                                            df_tabela_total_agrupado = pd.DataFrame(columns=colunas_agrupamento_total)
                                
                                # Recalcular Total / Flex Bud após agrupamento (se as colunas necessárias existirem)
                                if len(df_tabela_total_agrupado) > 0 and 'Total' in df_tabela_total_agrupado.columns and 'Flex BUD' in df_tabela_total_agrupado.columns:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = df_tabela_total_agrupado.apply(
                                        lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                        axis=1
                                    )
                                elif len(df_tabela_total_agrupado) > 0:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = 0
                                
                                # 🔧 FILTRAR LINHAS ZERADAS E NULAS após agrupamento
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                                  and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                    if colunas_numericas_agrupado:
                                        df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0)
                                        df_tabela_total_agrupado = df_tabela_total_agrupado[
                                            df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                        ].copy()
                                else:
                                    # Se não houver colunas de agrupamento, somar tudo
                                    # Verificar quais colunas existem antes de somar
                                    valores_soma = {}
                                    colunas_esperadas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']
                                    for col in colunas_esperadas:
                                        if col in df_tabela_total.columns:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Se não encontrou as colunas esperadas, tentar usar todas as colunas numéricas
                                    if len(valores_soma) == 0:
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in ['Custo', 'Período']]
                                        for col in colunas_numericas:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Calcular Total / Flex Bud se as colunas necessárias existirem
                                    if 'Total' in valores_soma and 'Flex BUD' in valores_soma:
                                        if valores_soma['Flex BUD'] != 0 and pd.notnull(valores_soma['Flex BUD']):
                                            valores_soma['Total / Flex Bud'] = valores_soma['Total'] / valores_soma['Flex BUD']
                                        else:
                                            valores_soma['Total / Flex Bud'] = 0
                                    
                                    if len(valores_soma) > 0:
                                        df_tabela_total_agrupado = pd.DataFrame([valores_soma])
                                    else:
                                        # Se não há colunas para somar, criar DataFrame vazio
                                        st.warning("⚠️ Nenhuma coluna numérica encontrada em df_tabela_total. Colunas disponíveis: " + ", ".join(df_tabela_total.columns.tolist()))
                                        df_tabela_total_agrupado = pd.DataFrame()
                                
                                # 🔧 FILTRAR: Se a linha única tiver todos os valores zerados, não exibir
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col])]
                                    if colunas_numericas_agrupado:
                                        soma_absoluta = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0).abs().sum(axis=1).iloc[0]
                                        if soma_absoluta <= 0.0001:
                                            df_tabela_total_agrupado = pd.DataFrame()  # DataFrame vazio para não exibir
                            
                            # 🔧 ADICIONAR: Exibir Resumo Geral no modo Total
                            if len(df_tabela_total_agrupado) > 0:
                                st.markdown("---")
                                st.markdown("### 📊 Resumo Geral")
                                
                                # Usar df_tabela_flex_para_resumo (salvo ANTES da transformação em colunas por período)
                                # Se não existir (caso de período único), usar df_tabela_flex
                                df_para_resumo = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                                # Calcular resumo geral usando df_para_resumo
                                # Isso garante que todos os dados sejam incluídos no resumo com as colunas corretas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(
                                    df_para_resumo, 
                                    tipo_visualizacao, 
                                    moeda_simbolo, 
                                    fator_conversao
                                )
                                
                                # Exibir caixas de resumo
                                exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=False)
                                st.markdown("---")
                            
                            # Criar estrutura hierárquica sem separação por Custo
                            # 🔧 CORREÇÃO: Usar df_tabela_flex_para_resumo para cálculos de resumo (tem colunas originais)
                            df_para_resumo_total = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # Nível 1: Type 05 (se existir)
                            if 'Type 05' in df_tabela_total_agrupado.columns:
                                for type05 in sorted(df_tabela_total_agrupado['Type 05'].dropna().unique()):
                                    df_type05 = df_tabela_total_agrupado[df_tabela_total_agrupado['Type 05'] == type05].copy()
                                    # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                    df_type05_para_resumo = df_para_resumo_total[df_para_resumo_total['Type 05'] == type05].copy() if 'Type 05' in df_para_resumo_total.columns else df_type05.copy()
                                    
                                    if len(df_type05) > 0:
                                        # 🔧 FILTRAR: Verificar se Type 05 tem valores não zerados
                                        colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                         if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                         and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                        if colunas_numericas_type05_check:
                                            df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                            tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                            if not tem_valores_nao_zerados:
                                                continue  # Pular Type 05 completamente zerado
                                        else:
                                            if 'Total' in df_type05.columns:
                                                if df_type05['Total'].fillna(0).abs().sum() <= 0.0001:
                                                    continue  # Pular Type 05 completamente zerado
                                        
                                        # Verificar se a coluna 'Total' existe antes de acessá-la
                                        if 'Total' in df_type05.columns:
                                            total_type05 = df_type05['Total'].sum()
                                        else:
                                            total_type05 = 0.0
                                        total_type05_formatado = f"{total_type05:,.2f}"
                                        
                                        with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                            # Resumo do Type 05
                                            # 🔧 CORREÇÃO: Usar df_type05_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                            linha_resumo_type05, linha_resumo_type05_formatado = calcular_resumo_tabela_flex(df_type05_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                            exibir_caixas_resumo(linha_resumo_type05, linha_resumo_type05_formatado, tipo_visualizacao)
                                            st.markdown("---")
                                            
                                            # Nível 2: Type 06 (se existir)
                                            if 'Type 06' in df_type05.columns:
                                                for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                    df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                    
                                                    if len(df_type06) > 0:
                                                        # 🔧 FILTRAR: Verificar se Type 06 tem valores não zerados
                                                        colunas_numericas_type06_check = [col for col in df_type06.columns 
                                                                                          if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                          and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                        if colunas_numericas_type06_check:
                                                            df_type06_check = df_type06[colunas_numericas_type06_check].fillna(0)
                                                            tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                            if not tem_valores_nao_zerados:
                                                                continue  # Pular Type 06 completamente zerado
                                                        else:
                                                            if 'Total' in df_type06.columns:
                                                                if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                    continue  # Pular Type 06 completamente zerado
                                                        
                                                        # Verificar se a coluna 'Total' existe antes de acessá-la
                                                        if 'Total' in df_type06.columns:
                                                            total_type06 = df_type06['Total'].sum()
                                                        else:
                                                            total_type06 = 0.0
                                                        total_type06_formatado = f"{total_type06:,.2f}"
                                                        
                                                        # Nível 3: Account (se existir)
                                                        if 'Account' in df_type06.columns:
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                df_type06_filtrado = df_type06[
                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            else:
                                                                df_type06_filtrado = df_type06.copy()
                                                            
                                                            # Só exibir se houver dados após filtrar
                                                            if len(df_type06_filtrado) > 0:
                                                                # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar uma única tabela com todas as Accounts
                                                                    # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                    colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                    
                                                                    # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                    for col in colunas_numericas:
                                                                        if col not in colunas_ordenadas:
                                                                            colunas_ordenadas.append(col)
                                                                    
                                                                    colunas_display = colunas_id + colunas_ordenadas
                                                                    df_display = df_type06_filtrado[colunas_display].copy()
                                                                    
                                                                    # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                                                    for col in df_display.columns:
                                                                        if col not in colunas_id:
                                                                            # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, não percentual)
                                                                            if col == 'Total / Flex Bud':
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                            # Formatar percentuais de forma especial com barrinha
                                                                            elif col.startswith('%'):
                                                                                # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                            # Formatar outras colunas numéricas
                                                                            elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                else:
                                                                                    sufixo = ""
                                                                                    if fator_conversao:
                                                                                        if fator_conversao == "K (milhares)":
                                                                                            sufixo = " K"
                                                                                        elif fator_conversao == "M (Milhões)":
                                                                                            sufixo = " M"
                                                                                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                    
                                                                    # Calcular linha de resumo (usar dados filtrados)
                                                                    linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                    
                                                                    # Exibir caixas de resumo
                                                                    exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                    
                                                                    # Exibir tabela com resumo (todas as Accounts em uma única tabela)
                                                                    html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                    st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                df_type06_filtrado = df_type06[
                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            else:
                                                                df_type06_filtrado = df_type06.copy()
                                                            
                                                            # Só exibir se houver dados após filtrar
                                                            if len(df_type06_filtrado) > 0:
                                                                # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar tabela para este Type 06
                                                                    # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                    colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'Período']]
                                                                    
                                                                    # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
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
                                                                                 