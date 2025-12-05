import streamlit as st
import pandas as pd
import altair as alt
import os
import numpy as np
import json
import sqlite3
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Dashboard TC Ext - df_final",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
            display: flex !important;
            flex-direction: column !important;
            align-items: flex-start !important;
        }
        [data-testid="stColumn"] > div {
            flex: 0 0 auto !important;
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
""", unsafe_allow_html=True)

# Inicializar estado se não existir
if 'moeda_selecionada' not in st.session_state:
    st.session_state.moeda_selecionada = "🇧🇷 R$"

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
        st.session_state.moeda_selecionada = st.session_state.moeda_selecionada_radio
    
    moeda_selecionada = st.radio(
        "",
        opcoes_moeda,
        index=index_moeda,
        horizontal=True,
        help="Selecione a moeda para exibição nos gráficos",
        key="moeda_selecionada_radio",
        label_visibility="visible",
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
            index=0,
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

# Título
st.title("📊 Dashboard TC Ext - df_final")
st.subheader("Análise de dados agrupados por Oficina e Período")

st.markdown("---")

# Função auxiliar para listar anos disponíveis
def listar_anos_disponiveis():
    """Lista todos os anos disponíveis nas pastas de dados"""
    pasta_dados = "dados"
    anos_disponiveis = []
    
    if os.path.exists(pasta_dados):
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos_disponiveis.append(int(item))
    
    return sorted(anos_disponiveis, reverse=True)  # Mais recente primeiro

# Função auxiliar para encontrar arquivo parquet na ordem de prioridade
def encontrar_arquivo_parquet(nome_arquivo, ano_selecionado=None):
    """
    Busca arquivo parquet na seguinte ordem de prioridade:
    1. Se ano_selecionado for None ou "Todos": Histórico consolidado (dados/historico_consolidado/)
    2. Se ano_selecionado for especificado: Pasta do ano (dados/{ANO}/)
    3. Pasta do ano mais recente (dados/{ANO}/)
    4. Raiz do projeto (compatibilidade)
    """
    # Se ano específico foi selecionado, buscar na pasta do ano
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join("dados", str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano
    
    # 1. Tentar histórico consolidado (para "Todos" ou quando não especificado)
    caminho_historico = os.path.join("dados", "historico_consolidado", nome_arquivo.replace(".parquet", "_historico.parquet"))
    if os.path.exists(caminho_historico):
        return caminho_historico
    
    # 2. Tentar pasta do ano mais recente
    pasta_dados = "dados"
    if os.path.exists(pasta_dados):
        anos_disponiveis = []
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos_disponiveis.append(int(item))
        
        if anos_disponiveis:
            ano_mais_recente = max(anos_disponiveis)
            caminho_ano = os.path.join(pasta_dados, str(ano_mais_recente), nome_arquivo)
            if os.path.exists(caminho_ano):
                return caminho_ano
    
    # 3. Tentar raiz (compatibilidade)
    if os.path.exists(nome_arquivo):
        return nome_arquivo
    
    return None

# Filtros na sidebar - ANTES de carregar dados
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

# Função para inicializar banco de dados SQLite
def inicializar_banco_taxas():
    """Cria o banco de dados e tabela para taxas de câmbio se não existir"""
    conn = sqlite3.connect('taxas_cambio.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS taxas_cambio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            moeda TEXT NOT NULL,
            taxa_para_brl REAL NOT NULL,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(moeda)
        )
    ''')
    conn.commit()
    conn.close()

# Função para carregar taxas do banco de dados
def carregar_taxas_banco():
    """Carrega as taxas de câmbio do banco de dados SQLite"""
    inicializar_banco_taxas()
    conn = sqlite3.connect('taxas_cambio.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT moeda, taxa_para_brl FROM taxas_cambio ORDER BY data_atualizacao DESC')
    resultados = cursor.fetchall()
    conn.close()
    
    taxas = {}
    for moeda, taxa in resultados:
        taxas[moeda] = taxa
    
    # Valores padrão se não houver dados
    if 'USD' not in taxas:
        taxas['USD'] = 5.00
    if 'EUR' not in taxas:
        taxas['EUR'] = 5.50
    
    return taxas

# Função para salvar taxas no banco de dados
def salvar_taxas_banco(taxas):
    """Salva as taxas de câmbio no banco de dados SQLite"""
    inicializar_banco_taxas()
    conn = sqlite3.connect('taxas_cambio.db')
    cursor = conn.cursor()
    
    for moeda, taxa in taxas.items():
        cursor.execute('''
            INSERT OR REPLACE INTO taxas_cambio (moeda, taxa_para_brl, data_atualizacao)
            VALUES (?, ?, ?)
        ''', (moeda, float(taxa), datetime.now()))
    
    conn.commit()
    conn.close()

st.sidebar.markdown("---")
st.sidebar.markdown("**💱 Taxas de Câmbio**")
st.sidebar.markdown("*Configure as taxas de conversão*")
st.sidebar.info("💾 Taxas salvas em: `taxas_cambio.db` (SQLite)")

# Carregar taxas do banco de dados
try:
    taxas_cambio_banco = carregar_taxas_banco()
except Exception as e:
    st.sidebar.warning(f"⚠️ Erro ao carregar taxas: {e}")
    taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

# Taxas de conversão: entrada em "1 $ = R$ X" e "1 € = R$ X"
taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

st.sidebar.markdown("**📝 Entrada de Taxas:**")

taxa_usd_para_brl = st.sidebar.number_input(
    "🇺🇸 1 $ (USD) = R$",
    min_value=0.01,
    max_value=100.0,
    value=float(taxa_usd_para_brl_padrao),
    step=0.01,
    format="%.2f",
    help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros. Exemplo: se 1 USD = 5.00 BRL, digite 5.00",
    key="taxa_usd_para_brl_input"
)

taxa_eur_para_brl = st.sidebar.number_input(
    "🇪🇺 1 € (EUR) = R$",
    min_value=0.01,
    max_value=100.0,
    value=float(taxa_eur_para_brl_padrao),
    step=0.01,
    format="%.2f",
    help="Digite quanto vale 1 Euro em Reais Brasileiros. Exemplo: se 1 EUR = 5.50 BRL, digite 5.50",
    key="taxa_eur_para_brl_input"
)

# Calcular taxas inversas para conversão (1 R$ = X USD/EUR)
taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

# Salvar taxas quando alteradas
if (taxa_usd_para_brl != taxa_usd_para_brl_padrao or 
    taxa_eur_para_brl != taxa_eur_para_brl_padrao):
    novas_taxas = {
        "USD": float(taxa_usd_para_brl),
        "EUR": float(taxa_eur_para_brl)
    }
    salvar_taxas_banco(novas_taxas)
    st.sidebar.success("✅ Taxas salvas no banco de dados!")

# Armazenar taxas em dicionário (para conversão: 1 R$ = X USD/EUR)
# IMPORTANTE: Estas taxas são para MULTIPLICAR valores em BRL
# Exemplo: Se taxa_brl_para_usd = 0.20, então 100 BRL * 0.20 = 20 USD
# Isso é equivalente a: 100 BRL / 5 = 20 USD (onde 5 é taxa_usd_para_brl)
taxas_cambio = {
    "BRL": 1.0,  # Real é a moeda base
    "USD": taxa_brl_para_usd,  # Ex: 0.20 (se 1 USD = 5 BRL, então 1 BRL = 0.20 USD)
    "EUR": taxa_brl_para_eur   # Ex: 0.18 (se 1 EUR = 5.50 BRL, então 1 BRL = 0.18 EUR)
}

# Função para converter valor de R$ para outra moeda
def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino"""
    if valor is None or pd.isna(valor):
        return valor
    if moeda_destino == "BRL":
        return valor
    taxa = taxas.get(moeda_destino, 1.0)
    return valor * taxa

# Função para converter coluna inteira de DataFrame
def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda"""
    if coluna not in df.columns:
        return df
    if moeda_destino == "BRL":
        return df
    df = df.copy()
    df[coluna] = df[coluna].apply(lambda x: converter_moeda(x, moeda_destino, taxas))
    return df

# Teste de validação da conversão (mostrar exemplo)
if moeda_codigo != "BRL":
    valor_teste = 100.0
    valor_convertido = converter_moeda(valor_teste, moeda_codigo, taxas_cambio)
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

# Mostrar valores de referência
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Valores de Referência**")
st.sidebar.markdown(f"**1 $ = R$ {taxa_usd_para_brl:.2f}**")
st.sidebar.markdown(f"**1 € = R$ {taxa_eur_para_brl:.2f}**")
st.sidebar.markdown(f"**1 R$ = ${taxa_brl_para_usd:.4f} USD**")
st.sidebar.markdown(f"**1 R$ = €{taxa_brl_para_eur:.4f} EUR**")

# Função para obter símbolo da moeda
def obter_simbolo_moeda(moeda_codigo):
    """Retorna o símbolo da moeda"""
    simbolos = {
        "BRL": "R$",
        "USD": "$",
        "EUR": "€"
    }
    return simbolos.get(moeda_codigo, "R$")

st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Filtros**")

# Função para carregar dados com cache
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
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
    ttl=3600,
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
            df = pd.read_parquet(caminho_historico)
        else:
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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


# Carregar dados com o ano selecionado
try:
    df_total = load_data(ano_selecionado)
    
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

# Função auxiliar para obter opções de filtro


@st.cache_data(ttl=1800, max_entries=5)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache"""
    if column_name in df.columns:
        opcoes = sorted(
            df[column_name].dropna().astype(str).unique().tolist()
        )
        return ["Todos"] + opcoes
    return ["Todos"]


# Ordem dos meses para ordenação cronológica
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]


# Aplicar fator de conversão nas colunas Total e BUD (antes de qualquer processamento)
# Isso simplifica os cálculos pois o fator é aplicado uma única vez na origem
# Mantém os dados na mesma unidade para comparações consistentes
if fator_conversao and fator_conversao != "Nenhum":
    if fator_conversao == "K (milhares)":
        if 'Total' in df_total.columns:
            df_total['Total'] = df_total['Total'] / 1000
    elif fator_conversao == "M (Milhões)":
        if 'Total' in df_total.columns:
            df_total['Total'] = df_total['Total'] / 1000000

# Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
# Isso garante que todos os dados derivados já terão a conversão aplicada
# IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
if moeda_codigo != "BRL" and 'Total' in df_total.columns:
    df_total = converter_coluna_moeda(df_total, 'Total', moeda_codigo, taxas_cambio)

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

# Inicializar session_state para Veículo
if 'filtro_veiculo_tc_ext' not in st.session_state:
    st.session_state.filtro_veiculo_tc_ext = ["Todos"]

# Filtro 2: Veículo (com cache otimizado)
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
    # Validar valores salvos
    default_veiculo = st.session_state.filtro_veiculo_tc_ext if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_tc_ext) else ["Todos"]
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_tc_ext_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_veiculo_tc_ext = veiculo_selecionados if veiculo_selecionados else ["Todos"]
    if veiculo_selecionados and "Todos" not in veiculo_selecionados:
        df_filtrado = df_filtrado[
            df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
        ].copy()

# Inicializar session_state para USI
if 'filtro_usi_tc_ext' not in st.session_state:
    if 'USI' in df_total.columns:
        usi_opcoes_temp = get_filter_options(df_total, 'USI')
        st.session_state.filtro_usi_tc_ext = ["TC Ext"] if "TC Ext" in usi_opcoes_temp else ["Todos"]
    else:
        st.session_state.filtro_usi_tc_ext = ["Todos"]

# Filtro 3: USI (com cache otimizado)
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    # Validar valores salvos
    default_usi = st.session_state.filtro_usi_tc_ext if all(x in usi_opcoes for x in st.session_state.filtro_usi_tc_ext) else (["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"])
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_tc_ext_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_usi_tc_ext = usi_selecionada if usi_selecionada else ["Todos"]

    # Filtrar o DataFrame com base na USI
    if "Todos" in usi_selecionada or not usi_selecionada:
        pass  # Manter df_filtrado como está
    else:
        df_filtrado = df_filtrado[
            df_filtrado['USI'].astype(str).isin(usi_selecionada)
        ].copy()

# Filtro 4: Período (com cache otimizado)
# IMPORTANTE: Criar cópia ANTES do filtro de período para usar no gráfico
df_para_grafico_periodo = df_filtrado.copy()

if 'Período' in df_filtrado.columns:
    periodo_opcoes_raw = get_filter_options(df_filtrado, 'Período')

    # Ordenar meses cronologicamente
    periodo_opcoes = ["Todos"]
    meses_ordenados = []
    outros_periodos = []

    for periodo in periodo_opcoes_raw[1:]:  # Pular "Todos"
        periodo_lower = str(periodo).lower()
        if periodo_lower in ORDEM_MESES:
            meses_ordenados.append(periodo)
        else:
            outros_periodos.append(periodo)

    # Ordenar meses pela ordem cronológica
    meses_ordenados.sort(
        key=lambda x: ORDEM_MESES.index(str(x).lower())
        if str(x).lower() in ORDEM_MESES else 999
    )

    # Combinar: Todos + meses ordenados + outros períodos
    periodo_opcoes = periodo_opcoes + meses_ordenados + outros_periodos

    # Inicializar session_state para Período
    if 'filtro_periodo_tc_ext' not in st.session_state:
        st.session_state.filtro_periodo_tc_ext = "Todos"
    
    # Validar valor salvo
    periodo_default = st.session_state.filtro_periodo_tc_ext if st.session_state.filtro_periodo_tc_ext in periodo_opcoes else "Todos"
    periodo_index = periodo_opcoes.index(periodo_default) if periodo_default in periodo_opcoes else 0
    
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes, index=periodo_index, key="filtro_periodo_tc_ext_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_periodo_tc_ext = periodo_selecionado
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Inicializar session_state para Centro cst
if 'filtro_centro_cst_tc_ext' not in st.session_state:
    st.session_state.filtro_centro_cst_tc_ext = "Todos"

# Filtro 5: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    # Validar valor salvo
    centro_cst_default = st.session_state.filtro_centro_cst_tc_ext if st.session_state.filtro_centro_cst_tc_ext in centro_cst_opcoes else "Todos"
    centro_cst_index = centro_cst_opcoes.index(centro_cst_default) if centro_cst_default in centro_cst_opcoes else 0
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes, index=centro_cst_index, key="filtro_centro_cst_tc_ext_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_centro_cst_tc_ext = centro_cst_selecionado
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Inicializar session_state para Conta contábil
if 'filtro_conta_contabil_tc_ext' not in st.session_state:
    st.session_state.filtro_conta_contabil_tc_ext = []

# Filtro 6: Conta contábil (com cache otimizado)
if 'Nºconta' in df_filtrado.columns:
    conta_contabil_opcoes = get_filter_options(df_filtrado, 'Nºconta')[1:]
    # Validar valores salvos
    default_conta = [x for x in st.session_state.filtro_conta_contabil_tc_ext if x in conta_contabil_opcoes] if st.session_state.filtro_conta_contabil_tc_ext else []
    conta_contabil_selecionadas = st.sidebar.multiselect(
        "Selecione a Conta contábil:", conta_contabil_opcoes, default=default_conta, key="filtro_conta_contabil_tc_ext_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_conta_contabil_tc_ext = conta_contabil_selecionadas
    if conta_contabil_selecionadas:
        df_filtrado = df_filtrado[
            df_filtrado['Nºconta'].astype(str).isin(
                conta_contabil_selecionadas
            )
        ].copy()

# Filtros principais (com cache otimizado)
filtros_principais = [
    ("Type 05", "Type 05", "multiselect"),
    ("Type 06", "Type 06", "multiselect"),
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
        ("Usuário", "Usuário", "multiselect"),
        ("Material", "Material", "multiselect"),
        ("Dt.lçto.", "Data Lançamento", "multiselect"),
        ("Texto breve", "Texto breve", "multiselect"),
        ("Account", "Account", "multiselect")
    ]

    for col_name, label, widget_type in filtros_avancados:
        if col_name in df_filtrado.columns:
            opcoes = get_filter_options(df_filtrado, col_name)
            # Limitar opções para melhor performance
            if len(opcoes) > 101:  # 100 + "Todos"
                opcoes = opcoes[:101]
                st.caption(
                    f"⚠️ {label}: Limitado a 100 opções para performance"
                )

            if widget_type == "multiselect":
                # Inicializar session_state para cada filtro avançado
                filtro_key = f'filtro_avancado_{col_name}_tc_ext'
                if filtro_key not in st.session_state:
                    st.session_state[filtro_key] = ["Todos"]
                
                # Validar valores salvos
                default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
                selecionadas = st.multiselect(
                    f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect"
                )
                # Atualizar session_state
                st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
                if selecionadas and "Todos" not in selecionadas:
                    df_filtrado = df_filtrado[
                        df_filtrado[col_name].astype(str).isin(selecionadas)
                    ].copy()

# Preparar dados para visualização
if tipo_visualizacao == "CPU (Custo por Unidade)":
    # Carregar dados de volume
    df_vol_calc = load_volume_data(ano_selecionado)

    if df_vol_calc is not None and 'Volume' in df_vol_calc.columns:
        # Agrupar df_filtrado por Oficina e Período para calcular Total
        if ('Oficina' in df_filtrado.columns and
                'Período' in df_filtrado.columns):
            # Agrupar Total por Oficina e Período
            if 'Total' in df_filtrado.columns:
                df_total_agrupado = df_filtrado.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Total'].sum()
            elif 'Valor' in df_filtrado.columns:
                df_total_agrupado = df_filtrado.groupby(
                    ['Oficina', 'Período'], as_index=False
                )['Valor'].sum()
                df_total_agrupado.rename(
                    columns={'Valor': 'Total'}, inplace=True
                )
            else:
                st.warning(
                    "⚠️ Colunas 'Total' ou 'Valor' necessárias para "
                    "calcular CPU"
                )
                df_visualizacao = df_filtrado.copy()
                coluna_visualizacao = (
                    'Total' if 'Total' in df_filtrado.columns else 'Valor'
                )
                tipo_visualizacao = "Custo Total"
                df_vol_calc = None

            if df_vol_calc is not None:
                # Verificar se df_filtrado tem Veículo e Ano
                tem_veiculo = 'Veículo' in df_filtrado.columns
                tem_ano = 'Ano' in df_filtrado.columns
                
                # IMPORTANTE: Filtrar df_vol_calc pelos mesmos filtros aplicados em df_filtrado
                # Isso garante que quando filtra por um veículo, o volume também seja filtrado
                df_vol_calc_filtrado = df_vol_calc.copy()
                
                # Aplicar filtros de Veículo se existir
                if tem_veiculo and 'Veículo' in df_vol_calc_filtrado.columns:
                    # Obter veículos únicos de df_filtrado (já filtrado pela sidebar)
                    veiculos_filtrados = df_filtrado['Veículo'].dropna().unique()
                    if len(veiculos_filtrados) > 0:
                        df_vol_calc_filtrado = df_vol_calc_filtrado[
                            df_vol_calc_filtrado['Veículo'].isin(veiculos_filtrados)
                        ].copy()
                
                # Aplicar filtros de Oficina se existir
                if 'Oficina' in df_filtrado.columns and 'Oficina' in df_vol_calc_filtrado.columns:
                    oficinas_filtradas = df_filtrado['Oficina'].dropna().unique()
                    if len(oficinas_filtradas) > 0:
                        df_vol_calc_filtrado = df_vol_calc_filtrado[
                            df_vol_calc_filtrado['Oficina'].isin(oficinas_filtradas)
                        ].copy()
                
                # Usar df_vol_calc_filtrado em vez de df_vol_calc
                df_vol_calc = df_vol_calc_filtrado

                # 🔧 CORREÇÃO: Incluir 'Ano' no groupby se existir
                colunas_agrupamento = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento.append('Veículo')

                # Agrupar Volume por Oficina, Período, Ano (se existir) e Veículo (se existir)
                if tem_veiculo and 'Veículo' in df_vol_calc.columns:
                    # Agrupar Total incluindo Veículo e Ano
                    if 'Total' in df_filtrado.columns:
                        df_total_agrupado = df_filtrado.groupby(
                            colunas_agrupamento,
                            as_index=False
                        )['Total'].sum()
                    else:
                        df_total_agrupado = df_filtrado.groupby(
                            colunas_agrupamento,
                            as_index=False
                        )['Valor'].sum()
                        df_total_agrupado.rename(
                            columns={'Valor': 'Total'}, inplace=True
                        )

                    # Agrupar Volume incluindo Veículo e Ano
                    colunas_agrupamento_vol = ['Oficina', 'Período']
                    if tem_ano and 'Ano' in df_vol_calc.columns:
                        colunas_agrupamento_vol.append('Ano')
                    if 'Veículo' in df_vol_calc.columns:
                        colunas_agrupamento_vol.append('Veículo')
                    
                    df_vol_agrupado = df_vol_calc.groupby(
                        colunas_agrupamento_vol, as_index=False
                    )['Volume'].sum()

                    # Fazer merge incluindo Veículo e Ano
                    df_cpu = pd.merge(
                        df_total_agrupado,
                        df_vol_agrupado,
                        on=colunas_agrupamento,
                        how='left'
                    )
                else:
                    # Agrupar Total por Oficina, Período e Ano (se existir)
                    if 'Total' in df_filtrado.columns:
                        df_total_agrupado = df_filtrado.groupby(
                            colunas_agrupamento,
                            as_index=False
                        )['Total'].sum()
                    else:
                        df_total_agrupado = df_filtrado.groupby(
                            colunas_agrupamento,
                            as_index=False
                        )['Valor'].sum()
                        df_total_agrupado.rename(
                            columns={'Valor': 'Total'}, inplace=True
                        )
                    
                    # Agrupar Volume por Oficina, Período e Ano (se existir)
                    colunas_agrupamento_vol = ['Oficina', 'Período']
                    if tem_ano and 'Ano' in df_vol_calc.columns:
                        colunas_agrupamento_vol.append('Ano')
                    
                    df_vol_agrupado = df_vol_calc.groupby(
                        colunas_agrupamento_vol, as_index=False
                    )['Volume'].sum()

                    # Fazer merge
                    df_cpu = pd.merge(
                        df_total_agrupado,
                        df_vol_agrupado,
                        on=colunas_agrupamento,
                        how='left'
                    )

                    # Se df_filtrado tem Veículo mas df_vol não, expandir
                    if tem_veiculo:
                        # Fazer merge com df_filtrado para obter Veículo e Ano
                        colunas_merge_veiculo = ['Oficina', 'Período', 'Veículo']
                        if tem_ano:
                            colunas_merge_veiculo.append('Ano')
                        
                        df_filtrado_veiculo = (
                            df_filtrado[colunas_merge_veiculo]
                            .drop_duplicates()
                        )
                        df_cpu_expandido = pd.merge(
                            df_filtrado_veiculo,
                            df_cpu,
                            on=colunas_agrupamento,
                            how='right'
                        )
                        # Usar o mesmo Volume para todos os veículos
                        df_cpu = df_cpu_expandido.copy()

                # NOTA: A conversão de moeda já foi aplicada no df_total (linha ~707)
                # Portanto, df_cpu['Total'] já está convertido, e o CPU será calculado automaticamente na moeda correta
                
                # Calcular CPU (evitando divisão por zero) - VETORIZADO
                # CPU já será calculado na moeda convertida automaticamente (pois Total já está convertido)
                df_cpu['CPU'] = np.where(
                    (df_cpu['Volume'].notna()) & (df_cpu['Volume'] != 0),
                    df_cpu['Total'] / df_cpu['Volume'],
                    0
                )

                # Criar DataFrame para visualização com CPU
                df_visualizacao = df_cpu.copy()
                coluna_visualizacao = 'CPU'
        else:
            st.warning(
                "⚠️ Colunas 'Oficina' e 'Período' necessárias para "
                "calcular CPU"
            )
            df_visualizacao = df_filtrado.copy()
            coluna_visualizacao = (
                'Total' if 'Total' in df_filtrado.columns else 'Valor'
            )
            tipo_visualizacao = "Custo Total"
    else:
        st.warning(
            "⚠️ Dados de volume não disponíveis. "
            "Mostrando Custo Total."
        )
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = (
            'Total' if 'Total' in df_filtrado.columns else 'Valor'
        )
        tipo_visualizacao = "Custo Total"
else:
    # Usar Total ou Valor diretamente
    # IMPORTANTE: Adicionar Volume ao df_visualizacao para que o gráfico funcione igual ao modo CPU
    if 'Total' in df_filtrado.columns:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Total'
    elif 'Valor' in df_filtrado.columns:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Valor'
    else:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Total'
    
    # Adicionar Volume ao df_visualizacao usando a mesma lógica do modo CPU
    # PROBLEMA IDENTIFICADO: df_visualizacao = df_filtrado.copy() pode ter múltiplas linhas
    # para a mesma combinação de Oficina+Período+Veículo, causando duplicação no merge
    # SOLUÇÃO: Agrupar df_visualizacao ANTES do merge, igual ao modo CPU faz com df_total_agrupado
    if 'Veículo' in df_visualizacao.columns and 'Oficina' in df_visualizacao.columns and 'Período' in df_visualizacao.columns:
        df_vol_calc = load_volume_data(ano_selecionado)
        if df_vol_calc is not None and 'Volume' in df_vol_calc.columns:
            tem_veiculo = 'Veículo' in df_visualizacao.columns
            tem_ano = 'Ano' in df_visualizacao.columns
            
            # Filtrar df_vol_calc pelos mesmos filtros (mesma lógica do modo CPU)
            df_vol_calc_filtrado = df_vol_calc.copy()
            
            if tem_veiculo and 'Veículo' in df_vol_calc_filtrado.columns:
                veiculos_filtrados = df_visualizacao['Veículo'].dropna().unique()
                if len(veiculos_filtrados) > 0:
                    df_vol_calc_filtrado = df_vol_calc_filtrado[
                        df_vol_calc_filtrado['Veículo'].isin(veiculos_filtrados)
                    ].copy()
            
            if 'Oficina' in df_visualizacao.columns and 'Oficina' in df_vol_calc_filtrado.columns:
                oficinas_filtradas = df_visualizacao['Oficina'].dropna().unique()
                if len(oficinas_filtradas) > 0:
                    df_vol_calc_filtrado = df_vol_calc_filtrado[
                        df_vol_calc_filtrado['Oficina'].isin(oficinas_filtradas)
                    ].copy()
            
            df_vol_calc = df_vol_calc_filtrado
            
            # Agrupar Volume exatamente como no modo CPU
            if tem_veiculo and 'Veículo' in df_vol_calc.columns:
                colunas_agrupamento_vol = ['Oficina', 'Período']
                if tem_ano and 'Ano' in df_vol_calc.columns:
                    colunas_agrupamento_vol.append('Ano')
                if 'Veículo' in df_vol_calc.columns:
                    colunas_agrupamento_vol.append('Veículo')
                
                df_vol_agrupado = df_vol_calc.groupby(
                    colunas_agrupamento_vol, as_index=False
                )['Volume'].sum()
                
                # IMPORTANTE: Usar EXATAMENTE as mesmas colunas de agrupamento para o merge
                # Garantir que colunas_agrupamento seja idêntica a colunas_agrupamento_vol
                colunas_agrupamento = colunas_agrupamento_vol.copy()
                
                # Agrupar df_visualizacao mantendo apenas as colunas necessárias
                if coluna_visualizacao in df_visualizacao.columns:
                    # Se tiver coluna de visualização, somar ela também
                    df_visualizacao_agrupado = df_visualizacao.groupby(
                        colunas_agrupamento, as_index=False
                    )[coluna_visualizacao].sum()
                else:
                    # Se não tiver, apenas agrupar para ter estrutura única
                    df_visualizacao_agrupado = df_visualizacao[colunas_agrupamento].drop_duplicates()
                
                # Fazer merge com df_vol_agrupado usando as MESMAS colunas
                # Isso garante que não há duplicação
                df_visualizacao = pd.merge(
                    df_visualizacao_agrupado,
                    df_vol_agrupado[colunas_agrupamento_vol + ['Volume']],
                    on=colunas_agrupamento_vol,
                    how='left'
                )

# Resumo na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Resumo**")
st.sidebar.write(f"**Linhas:** {df_filtrado.shape[0]:,}")

# Calcular totais se as colunas existirem
if 'Valor' in df_filtrado.columns:
    valor_total = df_filtrado['Valor'].sum()
    st.sidebar.write(f"**Total Valor:** R$ {valor_total:,.2f}")
if 'Total' in df_filtrado.columns:
    total_sum = df_filtrado['Total'].sum()
    st.sidebar.write(f"**Total:** R$ {total_sum:,.2f}")
    
    # DIAGNÓSTICO: Comparar com o valor esperado
    total_esperado = 5849755.04
    diferenca = total_sum - total_esperado
    percentual_diff = (diferenca / total_esperado) * 100 if total_esperado != 0 else 0
    
    if abs(diferenca) > 0.01:  # Se a diferença for maior que 1 centavo
        st.sidebar.markdown("---")
        st.sidebar.markdown("**⚠️ Diagnóstico**")
        st.sidebar.write(f"**Esperado:** R$ {total_esperado:,.2f}")
        st.sidebar.write(f"**Diferença:** R$ {diferenca:,.2f} ({percentual_diff:+.2f}%)")
        
        # Verificar filtros aplicados
        if 'Account' in df_filtrado.columns:
            account_nan = df_filtrado['Account'].isna().sum()
            account_zero = (df_filtrado['Account'] == 0).sum()
            account_tc_ext = (df_filtrado['Account'] == 'TC Ext').sum()
            if account_nan > 0 or account_zero > 0 or account_tc_ext > 0:
                st.sidebar.write(f"**Account inválidos:** {account_nan + account_zero + account_tc_ext} linhas")
if 'Volume' in df_filtrado.columns:
    volume_total = df_filtrado['Volume'].sum()
    st.sidebar.write(f"**Total Volume:** {volume_total:,.0f}")
if 'CPU' in df_filtrado.columns:
    df_cpu_positivo = df_filtrado[df_filtrado['CPU'] > 0]
    cpu_medio = (
        df_cpu_positivo['CPU'].mean()
        if len(df_cpu_positivo) > 0 else 0
    )
    st.sidebar.write(f"**CPU Médio:** R$ {cpu_medio:,.2f}")

# Mostrar tipo de visualização selecionado
st.sidebar.info(f"📈 **Visualizando:** {tipo_visualizacao}")


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
    
    # Calcular cor com gradiente verde->vermelho
    # 0% = verde puro, 100% = vermelho puro
    # Quando chegar em 100% ou mais, fica vermelho
    if percentual <= 0:
        r, g, b = 0, 170, 0  # Verde (#00AA00)
    elif percentual >= 100:
        r, g, b = 255, 0, 0  # Vermelho (#FF0000) quando 100% ou mais
    else:
        # Interpolação linear: verde (0, 170, 0) -> vermelho (255, 0, 0)
        # Quanto mais próximo de 100%, mais vermelho
        r = int(255 * (percentual / 100))
        g = int(170 * (1 - percentual / 100))
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
    <div style="display: flex; align-items: center; gap: 6px; width: 100%; justify-content: flex-start; margin: 0; padding: 0; vertical-align: middle;">
        <div style="width: 80px; background-color: #333; border-radius: 3px; height: 14px; position: relative; overflow: hidden; flex-shrink: 0; margin: 0;">
            <div style="width: {largura_barra}%; height: 100%; background-color: {cor}; transition: width 0.3s;"></div>
        </div>
        <span style="width: 65px; text-align: left; font-weight: bold; color: {texto_cor}; font-size: 0.75rem; flex-shrink: 0; line-height: 1.2; margin: 0;">{percentual:.1f}%</span>
    </div>
    """
    return html

def criar_tabela_html_com_barra(df_display):
    """Cria uma tabela HTML customizada para renderizar HTML nas células"""
    # Usar EXATAMENTE a mesma estrutura e classes CSS que o Streamlit usa para st.dataframe
    # O Streamlit usa a classe 'stDataFrame' e aplica estilos automaticamente baseados no tema
    # Usar a mesma cor do título do expander (que é a cor padrão do texto do Streamlit)
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            # Cor do texto do expander no dark mode: rgb(250, 250, 250) com fundo transparente/cinza
            # Usar um cinza escuro transparente que combine com o fundo do expander
            header_bg = "rgba(38, 39, 48, 0.1)"  # 90% de transparência (10% opaco)
        else:
            # Cor do texto do expander no light mode: rgb(49, 51, 63) com fundo claro
            header_bg = "rgba(240, 242, 246, 0.1)"  # 90% de transparência (10% opaco)
    except:
        header_bg = "rgba(38, 39, 48, 0.1)"  # Padrão dark - 90% de transparência
    
    html_table = "<div class='stDataFrame' style='overflow-x: auto;'>"
    html_table += "<table style='width: 100%; border-collapse: collapse;'>"
    # Cabeçalho - com fundo cinza transparente
    html_table += f"<thead><tr style='background-color: {header_bg};'>"
    for col in df_display.columns:
        html_table += f"<th style='padding: 0.5rem; text-align: left; font-weight: 600; font-size: 0.875rem; vertical-align: middle;'>{col}</th>"
    html_table += "</tr></thead><tbody>"
    # Linhas - Streamlit aplica estilos automaticamente via classe stDataFrame
    for idx, row in df_display.iterrows():
        html_table += "<tr>"
        for col in df_display.columns:
            if col == 'Total / Flex Bud':
                # Renderizar HTML diretamente com alinhamento centralizado
                html_table += f"<td style='padding: 0.5rem; vertical-align: middle; font-size: 0.875rem; text-align: left;'>{row[col]}</td>"
            else:
                html_table += f"<td style='padding: 0.5rem; vertical-align: middle; font-size: 0.875rem; text-align: left;'>{row[col]}</td>"
        html_table += "</tr>"
    html_table += "</tbody></table></div>"
    return html_table

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
            
            # Fazer merge de volumes
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on=['Ano', 'Período'],
                how='inner',
                suffixes=('_real', '_budget')
            )
            
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
                budget_total = custos_budget['Total'].sum()
                custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                
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
            
            # Fazer merge de volumes
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on='Período',
                how='inner',
                suffixes=('_real', '_budget')
            )
            
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
                budget_total = custos_budget['Total'].sum()
                custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                
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
        
        if coluna not in df_data.columns or 'Período' not in df_data.columns:
            st.warning(f"⚠️ Colunas necessárias não encontradas. Coluna: {coluna}, Período: {'Período' in df_data.columns}")
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
            # Remover valores nulos e zero do gráfico no modo CPU
            chart_data = chart_data[
                (chart_data[coluna].notna()) & 
                (chart_data[coluna] != 0) & 
                (chart_data[coluna] != 0.0)
            ].copy()
            # Reordenar após filtrar
            if tem_ano:
                chart_data = ordenar_por_mes(chart_data, 'Período')
                ordem_periodos = chart_data['Período_Completo'].tolist()
            else:
                chart_data = ordenar_por_mes(chart_data, 'Período')
                ordem_periodos = chart_data['Período'].tolist()
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por Período"

        # Validar se chart_data tem dados após agrupamento e filtros
        if chart_data is None or chart_data.empty:
            st.warning("⚠️ Nenhum dado após agrupamento. Verifique os filtros aplicados.")
            return None
            
        # Verificar se a coluna tem valores válidos
        if coluna not in chart_data.columns:
            st.warning(f"⚠️ Coluna '{coluna}' não encontrada após agrupamento. Colunas disponíveis: {list(chart_data.columns)}")
            return None
            
        # Garantir que os valores sejam numéricos
        chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce').fillna(0)
            
        # Verificar se há valores não-nulos (apenas para Custo Total, CPU já filtra zeros)
        if tipo_viz != "CPU (Custo por Unidade)":
            valores_validos = chart_data[coluna].notna() & (chart_data[coluna] != 0)
            if not valores_validos.any():
                # Não bloquear, apenas avisar - pode haver valores muito pequenos após conversão
                st.info(f"ℹ️ Todos os valores na coluna '{coluna}' são zero após agrupamento. Mostrando gráfico mesmo assim.")
        
        # Verificar se chart_data está vazio
        if len(chart_data) == 0:
            return None

        # Usar gradiente baseado no valor da coluna (como na figura 1)
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
            height=250  # Reduzido ainda mais para ocupar menos espaço
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
        )

        # Processar dados de budget e calcular FLEX se fornecidos
        linha_budget = None
        budget_data = None  # Inicializar para uso no gráfico de delta
        # IMPORTANTE: No modo CPU, df_data pode não ter a coluna 'Custo' necessária para calcular FLEX
        # Usar df_real_original se disponível, caso contrário usar df_data
        df_real_para_flex = df_real_original if df_real_original is not None else df_data
        if df_budget is not None and 'Período' in df_budget.columns and df_real_vol is not None:
            # Verificar se temos dados com coluna 'Custo' para calcular FLEX
            if 'Custo' in df_real_para_flex.columns:
                try:
                    # Calcular FLEX comparando dados reais vs budget
                    # FLEX = efeito da variação de volume aplicando sensibilidade fixa (Fixo=0, Variável=1)
                    flex_data = calcular_flex_budget(
                        df_real_para_flex,  # Dados reais (com coluna 'Custo')
                        df_real_vol,  # Volume real
                        df_budget,  # Dados de budget
                        df_budget_vol,  # Volume de budget
                        tipo_viz,
                        tem_ano
                    )
                    
                    if flex_data is not None and len(flex_data) > 0:
                        # Renomear coluna FLEX para o nome da coluna do gráfico
                        flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                        
                        if tem_ano:
                            # Criar coluna combinada para o rótulo do gráfico
                            flex_data['Período_Completo'] = flex_data['Período'].astype(str) + ' ' + flex_data['Ano'].astype(str)
                            # Ordenar por ano e mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            # Filtrar apenas períodos que existem no chart_data
                            flex_data = flex_data[flex_data['Período_Completo'].isin(ordem_periodos)].copy()
                        else:
                            # Ordenar por mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            # Filtrar apenas períodos que existem no chart_data
                            flex_data = flex_data[flex_data['Período'].isin(ordem_periodos)].copy()
                        
                        if len(flex_data) > 0:
                            budget_data = flex_data.copy()
                            
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
                except Exception as e:
                    st.sidebar.warning(f"⚠️ Erro ao processar dados de budget: {e}")

        # Criar gráfico de delta (Flex Bud - Real) se linha_budget estiver disponível
        grafico_delta = None
        if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
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
                
                # Calcular delta: Real - Flex Bud (invertido)
                coluna_real = coluna  # A coluna original já é o Real
                coluna_flex = f'{coluna}_FlexBud'
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
                    height=60  # Gráfico mais baixo/fino
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
                st.sidebar.warning(f"⚠️ Erro ao criar gráfico de delta: {e}")
        
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
            title='Volume Total por Período',
            height=400
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
def create_volume_veiculo_chart(df_data):
    """Cria gráfico de barras de Volume por Veículo"""
    try:
        if 'Volume' not in df_data.columns or 'Veículo' not in df_data.columns:
            return None
        
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
        
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Veículo:N',
                title='Veículo',
                sort='-y',
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
            title="Volume por Veículo",
            height=400
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
        
        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico de volume: {e}")
        return None


# Inicializar session_state para manter a tab selecionada
if 'tab_selecionada_tc_ext' not in st.session_state:
    st.session_state.tab_selecionada_tc_ext = 0

# Verificar se há parâmetro de tab na URL e atualizar session_state
# Isso garante que a tab seja mantida mesmo após recarregamento por filtros
tab_from_url = st.query_params.get('tab', None)
if tab_from_url is not None:
    try:
        tab_index = int(tab_from_url)
        if 0 <= tab_index <= 3:  # Validar índice (0-3 para 4 tabs)
            st.session_state.tab_selecionada_tc_ext = tab_index
    except ValueError:
        pass

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
            if (index >= 0 && index <= 3) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_ext};
    }}
    
    const tabIndexDesejado = obterTabIndex();
    
    // Interceptar a criação das tabs ANTES que sejam renderizadas
    // Usar MutationObserver para detectar quando as tabs são criadas
    const observerPrecoce = new MutationObserver(function(mutations) {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length >= 4) {{
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
            if (index >= 0 && index <= 3) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_ext};
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
            // Remover listeners antigos
            const novoTab = tab.cloneNode(true);
            tab.parentNode.replaceChild(novoTab, tab);
            
            // Adicionar novo listener
            novoTab.addEventListener('click', function() {{
                tabIndexSalvo = index;
                const url = new URL(window.location);
                url.searchParams.set('tab', index);
                window.history.replaceState({{}}, '', url);
            }}, true);
        }});
    }}
    
    // Executar imediatamente usando requestAnimationFrame
    requestAnimationFrame(function() {{
        verificarERestaurar();
        configurarListeners();
    }});
    
    // Executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            verificarERestaurar();
            configurarListeners();
        }});
    }}
    
    // Executar periodicamente (muito frequente)
    setInterval(function() {{
        verificarERestaurar();
    }}, 100);
    
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
        
        # Carregar dados de budget e aplicar mesmos filtros
        df_budget_filtrado = None
        df_budget_vol_filtrado = None
        
        try:
            # Carregar dados de budget
            df_budget = load_budget_data(ano_selecionado)
            df_budget_vol = load_budget_volume_data(ano_selecionado)
            
            if df_budget is not None:
                # Aplicar fator de conversão na coluna Total do budget (mesma unidade que Total real)
                # Isso mantém os dados na mesma unidade para comparações consistentes
                if fator_conversao and fator_conversao != "Nenhum" and 'Total' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Total'] = df_budget['Total'] / 1000
                    elif fator_conversao == "M (Milhões)":
                        df_budget['Total'] = df_budget['Total'] / 1000000
                
                # Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
                # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
                if moeda_codigo != "BRL" and 'Total' in df_budget.columns:
                    df_budget = converter_coluna_moeda(df_budget, 'Total', moeda_codigo, taxas_cambio)
                
                # Aplicar mesmos filtros de Oficina e Veículo aos dados de budget
                df_budget_filtrado = df_budget.copy()
                
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
                # Aplicar mesmos filtros de Oficina e Veículo aos dados de volume de budget
                df_budget_vol_filtrado = df_budget_vol.copy()
                
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
        df_volume_real_filtrado = None
        if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
            # Aplicar mesmos filtros de Oficina e Veículo aos dados de volume reais
            df_volume_real_filtrado = df_vol_calc_grafico.copy()
            
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
        
        # No modo CPU, precisamos passar os dados originais (com 'Custo') para calcular FLEX
        df_real_original_grafico = None
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # Usar df_para_grafico_periodo que contém a coluna 'Custo'
            df_real_original_grafico = df_para_grafico_periodo.copy()
            # NOTA: A conversão de moeda já foi aplicada no df_total (linha ~702)
            # Portanto, df_real_original_grafico['Total'] já está convertido
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
                grafico_periodo = create_period_chart(
                    df_grafico_periodo, coluna_visualizacao_grafico, tipo_visualizacao,
                    df_budget_filtrado, df_budget_vol_filtrado, df_volume_real_filtrado,
                    df_real_original_grafico,  # Dados originais com 'Custo' para calcular FLEX
                    moeda_simbolo  # Passar símbolo da moeda para o gráfico
                )
                if grafico_periodo:
                    # Exibir gráfico no placeholder (renderização imediata)
                    chart_placeholder.altair_chart(grafico_periodo, use_container_width=True)
                else:
                    chart_placeholder.warning("⚠️ O gráfico não pôde ser criado. Verifique os dados e filtros aplicados.")
            except Exception as e:
                chart_placeholder.error(f"❌ Erro ao criar gráfico: {str(e)}")
        
        # Tabela de análise Flex Bud (para modos "Custo Total" e "CPU")
        if tipo_visualizacao in ["Custo Total", "CPU (Custo por Unidade)"]:
            st.markdown("---")
            st.subheader("📊 Análise Flex Bud por Categoria")
        
        # Filtro de período para a tabela (independente dos outros filtros)
        if 'Período' in df_grafico_periodo.columns:
            periodo_opcoes_tabela_raw = get_filter_options(df_grafico_periodo, 'Período')
            
            # Ordenar meses cronologicamente
            periodo_opcoes_tabela = ["Todos"]
            meses_ordenados_tabela = []
            outros_periodos_tabela = []
            
            for periodo in periodo_opcoes_tabela_raw[1:]:  # Pular "Todos"
                periodo_lower = str(periodo).lower()
                if periodo_lower in ORDEM_MESES:
                    meses_ordenados_tabela.append(periodo)
                else:
                    outros_periodos_tabela.append(periodo)
            
            # Ordenar meses pela ordem cronológica
            meses_ordenados_tabela.sort(
                key=lambda x: ORDEM_MESES.index(str(x).lower())
                if str(x).lower() in ORDEM_MESES else 999
            )
            
            # Combinar: Todos + meses ordenados + outros períodos
            periodo_opcoes_tabela = periodo_opcoes_tabela + meses_ordenados_tabela + outros_periodos_tabela
            
            periodo_selecionado_tabela = st.multiselect(
                "📅 Filtrar por Período (para a tabela):",
                periodo_opcoes_tabela,
                default=["Todos"]
            )
        else:
            periodo_selecionado_tabela = ["Todos"]
        
        # Calcular e exibir percentual de variação do volume dos períodos selecionados
        # IMPORTANTE: Usar os mesmos dados que o gráfico de volume usa para garantir consistência
        # Carregar dados de volume da mesma forma que o gráfico
        df_vol_tabela = load_volume_data(ano_selecionado)
        df_budget_vol_tabela = load_budget_volume_data(ano_selecionado)
        
        if df_vol_tabela is not None and df_budget_vol_tabela is not None:
            # Aplicar MESMOS filtros que o gráfico de volume usa
            if 'Período' in df_vol_tabela.columns and 'Volume' in df_vol_tabela.columns:
                # Identificar colunas comuns entre df_filtrado e df_vol
                colunas_comuns = set(df_filtrado.columns) & set(df_vol_tabela.columns)
                colunas_filtro = [
                    col for col in colunas_comuns
                    if col not in ['Volume', 'Total', 'Valor', 'CPU', 'Período']
                ]
                
                # Aplicar filtros do df_filtrado ao df_vol usando colunas comuns
                df_vol_real_tabela = df_vol_tabela.copy()
                df_vol_budget_tabela = df_budget_vol_tabela.copy()
                
                for col in colunas_filtro:
                    if col in df_filtrado.columns:
                        valores_filtrados = df_filtrado[col].dropna().unique()
                        if len(valores_filtrados) > 0:
                            df_vol_real_tabela = df_vol_real_tabela[
                                df_vol_real_tabela[col].isin(valores_filtrados)
                            ].copy()
                            df_vol_budget_tabela = df_vol_budget_tabela[
                                df_vol_budget_tabela[col].isin(valores_filtrados)
                            ].copy()
                
                # Aplicar filtros de Oficina e Veículo (mesmos do gráfico)
                if 'Oficina' in df_vol_real_tabela.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_vol_real_tabela = df_vol_real_tabela[
                            df_vol_real_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                        df_vol_budget_tabela = df_vol_budget_tabela[
                            df_vol_budget_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                if 'Veículo' in df_vol_real_tabela.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_vol_real_tabela = df_vol_real_tabela[
                            df_vol_real_tabela['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                        df_vol_budget_tabela = df_vol_budget_tabela[
                            df_vol_budget_tabela['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # Filtrar pelos períodos selecionados na tabela
                if periodo_selecionado_tabela and "Todos" not in periodo_selecionado_tabela and 'Período' in df_vol_real_tabela.columns:
                    df_vol_real_tabela = df_vol_real_tabela[
                        df_vol_real_tabela['Período'].astype(str).isin([str(p) for p in periodo_selecionado_tabela])
                    ].copy()
                    df_vol_budget_tabela = df_vol_budget_tabela[
                        df_vol_budget_tabela['Período'].astype(str).isin([str(p) for p in periodo_selecionado_tabela])
                    ].copy()
                
                # Calcular volumes totais - MESMA LÓGICA DO GRÁFICO
                # Agrupar por Ano e Período primeiro (se tiver coluna Ano), depois somar
                # Isso garante que o volume na tabela seja igual ao do gráfico
                if 'Ano' in df_vol_real_tabela.columns:
                    # Agrupar por Ano e Período, somar Volume (mesma lógica do gráfico)
                    df_vol_real_agrupado = df_vol_real_tabela.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                    volume_real_total_tabela = df_vol_real_agrupado['Volume'].sum() if len(df_vol_real_agrupado) > 0 else 0
                else:
                    # Sem coluna Ano: agrupar apenas por Período
                    df_vol_real_agrupado = df_vol_real_tabela.groupby('Período')['Volume'].sum().reset_index()
                    volume_real_total_tabela = df_vol_real_agrupado['Volume'].sum() if len(df_vol_real_agrupado) > 0 else 0
                
                if 'Ano' in df_vol_budget_tabela.columns:
                    # Agrupar por Ano e Período, somar Volume (mesma lógica do gráfico)
                    df_vol_budget_agrupado = df_vol_budget_tabela.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                    volume_budget_total_tabela = df_vol_budget_agrupado['Volume'].sum() if len(df_vol_budget_agrupado) > 0 else 0
                else:
                    # Sem coluna Ano: agrupar apenas por Período
                    df_vol_budget_agrupado = df_vol_budget_tabela.groupby('Período')['Volume'].sum().reset_index()
                    volume_budget_total_tabela = df_vol_budget_agrupado['Volume'].sum() if len(df_vol_budget_agrupado) > 0 else 0
            else:
                volume_real_total_tabela = 0
                volume_budget_total_tabela = 0
        else:
            volume_real_total_tabela = 0
            volume_budget_total_tabela = 0
        
        # Exibir métricas apenas se houver dados
        if volume_real_total_tabela > 0 or volume_budget_total_tabela > 0:
            
            # Calcular variação percentual
            if volume_real_total_tabela != 0 and not pd.isna(volume_real_total_tabela):
                proporcao_volume_tabela = volume_budget_total_tabela / volume_real_total_tabela
                variacao_percentual_tabela = (proporcao_volume_tabela - 1.0) * 100  # Converter para percentual
                
                # CSS para aumentar tamanho das métricas em 30%
                st.markdown("""
                    <style>
                    div[data-testid="stMetricValue"] {
                        font-size: 1.3em !important;
                    }
                    div[data-testid="stMetricLabel"] {
                        font-size: 1.3em !important;
                    }
                    </style>
                """, unsafe_allow_html=True)
                
                # Exibir métrica
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(
                        "📊 Variação do Volume",
                        f"{variacao_percentual_tabela:+.2f}%",
                        help="Percentual de variação do volume Budget em relação ao volume Real para os períodos selecionados"
                    )
                with col2:
                    st.metric(
                        "📈 Volume Real",
                        f"{volume_real_total_tabela:,.0f}",
                        help="Volume total real dos períodos selecionados"
                    )
                with col3:
                    st.metric(
                        "📉 Volume Budget",
                        f"{volume_budget_total_tabela:,.0f}",
                        help="Volume total budget dos períodos selecionados"
                    )
        
        # Inicializar estado se não existir
        if 'tipo_analise_selecionado' not in st.session_state:
            st.session_state.tipo_analise_selecionado = "Fixo / Variável"
        
        tipo_visualizacao_tabela = st.session_state.tipo_analise_selecionado
        
        # Estilizar radio buttons como na imagem (círculo vermelho quando selecionado, cinza quando não)
        st.markdown(f"""
            <style>
            /* Estilizar o container do radio */
            div[data-testid="stRadio"] > div {{
                flex-direction: row !important;
                gap: 20px !important;
            }}
            
            /* Estilizar cada opção do radio - deixar Streamlit gerenciar cores */
            div[data-testid="stRadio"] > div > label {{
                display: flex !important;
                align-items: center !important;
                gap: 8px !important;
                padding: 8px 12px !important;
                cursor: pointer !important;
                font-size: 14px !important;
            }}
            
            /* Círculo do radio button - não selecionado (cinza) */
            div[data-testid="stRadio"] > div > label > div:first-child {{
                width: 20px !important;
                height: 20px !important;
                min-width: 20px !important;
                min-height: 20px !important;
                border: 2px solid #666 !important;
                border-radius: 50% !important;
                background: transparent !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                position: relative !important;
            }}
            
            /* Círculo do radio button - selecionado (vermelho preenchido) */
            div[data-testid="stRadio"] > div > label:has(input[type="radio"]:checked) > div:first-child {{
                width: 20px !important;
                height: 20px !important;
                min-width: 20px !important;
                min-height: 20px !important;
                border: 2px solid #ff4b4b !important;
                border-radius: 50% !important;
                background: #ff4b4b !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                position: relative !important;
            }}
            
            /* Ponto branco no centro quando selecionado - usando pseudo-elemento */
            div[data-testid="stRadio"] > div > label:has(input[type="radio"]:checked) > div:first-child::after {{
                content: '' !important;
                width: 6px !important;
                height: 6px !important;
                border-radius: 50% !important;
                background: white !important;
                display: block !important;
                position: absolute !important;
                top: 50% !important;
                left: 50% !important;
                transform: translate(-50%, -50%) !important;
            }}
            
            /* Esconder o input nativo mas manter funcionalidade */
            div[data-testid="stRadio"] input[type="radio"] {{
                opacity: 0 !important;
                position: absolute !important;
                width: 0 !important;
                height: 0 !important;
            }}
            
            /* Estilizar o texto - deixar Streamlit gerenciar cores automaticamente */
            div[data-testid="stRadio"] > div > label > div:last-child {{
                font-weight: 500 !important;
            }}
            </style>
        """, unsafe_allow_html=True)
        
        # Criar seletor usando radio buttons estilizados com ícones
        opcoes = ["📊 Fixo / Variável", "📈 Total"]
        opcoes_valores = ["Fixo / Variável", "Total"]
        
        # Determinar índice atual
        valor_atual = st.session_state.tipo_analise_selecionado
        index_atual = 0 if valor_atual == "Fixo / Variável" else 1
        
        tipo_selecionado = st.radio(
            "Tipo de Análise:",
            options=opcoes,
            index=index_atual,
            key="radio_tipo_analise",
            horizontal=True,
            label_visibility="visible"
        )
        
        # Converter de volta para valor sem ícone
        tipo_visualizacao_tabela = opcoes_valores[opcoes.index(tipo_selecionado)]
        
        # Atualizar estado se mudou
        if tipo_visualizacao_tabela != st.session_state.tipo_analise_selecionado:
            st.session_state.tipo_analise_selecionado = tipo_visualizacao_tabela
            st.rerun()
        
        tipo_visualizacao_tabela = st.session_state.tipo_analise_selecionado
        
        # Criar função para calcular valores da tabela usando os mesmos dados do gráfico
        @st.cache_data(ttl=900, max_entries=5)
        def calcular_tabela_flex_bud(df_real_original, df_budget_original, df_real_vol, df_budget_vol, tem_ano, periodos_filtro=None, tipo_viz="Custo Total"):
            """
            Calcula valores para a tabela de análise Flex Bud usando os mesmos dados do gráfico.
            Agrupa por Custo, Type 05, Type 06, Account (sem Período) e calcula: BUD, Flex Bud, Total e diferenças.
            """
            try:
                # Verificar se coluna 'Custo' existe
                if 'Custo' not in df_real_original.columns or 'Custo' not in df_budget_original.columns:
                    return pd.DataFrame()
                
                # Filtrar por período se especificado - OTIMIZADO (evitar cópias desnecessárias)
                if periodos_filtro and "Todos" not in periodos_filtro and 'Período' in df_real_original.columns:
                    periodos_str = [str(p) for p in periodos_filtro]
                    df_real_filtrado = df_real_original[
                        df_real_original['Período'].astype(str).isin(periodos_str)
                    ]
                    df_budget_filtrado = df_budget_original[
                        df_budget_original['Período'].astype(str).isin(periodos_str)
                    ]
                    if df_real_vol is not None and 'Período' in df_real_vol.columns:
                        df_real_vol_filtrado = df_real_vol[
                            df_real_vol['Período'].astype(str).isin(periodos_str)
                        ]
                    else:
                        df_real_vol_filtrado = df_real_vol
                    if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
                        df_budget_vol_filtrado = df_budget_vol[
                            df_budget_vol['Período'].astype(str).isin(periodos_str)
                        ]
                    else:
                        df_budget_vol_filtrado = df_budget_vol
                else:
                    df_real_filtrado = df_real_original
                    df_budget_filtrado = df_budget_original
                    df_real_vol_filtrado = df_real_vol
                    df_budget_vol_filtrado = df_budget_vol
                
                # Agrupar dados reais por Custo, Type 05, Type 06, Account (SEM Período e Ano)
                colunas_agrupamento = ['Custo']
                if 'Type 05' in df_real_filtrado.columns:
                    colunas_agrupamento.append('Type 05')
                if 'Type 06' in df_real_filtrado.columns:
                    colunas_agrupamento.append('Type 06')
                if 'Account' in df_real_filtrado.columns:
                    colunas_agrupamento.append('Account')
                
                # Preparar dados reais: preencher NaN em colunas categóricas antes de agrupar - OTIMIZADO
                df_real_filtrado_clean = df_real_filtrado.copy()
                for col in colunas_agrupamento:
                    if col in df_real_filtrado_clean.columns:
                        if df_real_filtrado_clean[col].dtype.name == 'category':
                            df_real_filtrado_clean[col] = df_real_filtrado_clean[col].astype(str)
                        # Usar fillna apenas se houver NaN
                        if df_real_filtrado_clean[col].isna().any():
                            df_real_filtrado_clean[col] = df_real_filtrado_clean[col].fillna('Não Informado')
                
                # IMPORTANTE: Seguir EXATAMENTE a mesma lógica do gráfico
                # No gráfico: agrupa por Período, calcula FLEX por período, depois agrega
                # Na tabela: vamos calcular por período primeiro, depois agregar por categoria
                
                # Calcular volumes por período (mesma lógica do gráfico)
                if tem_ano and 'Ano' in df_real_vol_filtrado.columns:
                    colunas_vol_agrupamento = ['Ano', 'Período']
                else:
                    colunas_vol_agrupamento = ['Período']
                
                # Agrupar volumes reais por período
                if df_real_vol_filtrado is not None and 'Volume' in df_real_vol_filtrado.columns:
                    df_real_vol_agrupado = df_real_vol_filtrado.groupby(colunas_vol_agrupamento)['Volume'].sum().reset_index()
                    df_real_vol_agrupado.rename(columns={'Volume': 'Volume_Real'}, inplace=True)
                else:
                    return pd.DataFrame()
                
                # Agrupar volumes de budget por período
                if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                    df_budget_vol_agrupado = df_budget_vol_filtrado.groupby(colunas_vol_agrupamento)['Volume'].sum().reset_index()
                    df_budget_vol_agrupado.rename(columns={'Volume': 'Volume_Budget'}, inplace=True)
                else:
                    return pd.DataFrame()
                
                # Fazer merge de volumes por período
                df_volumes = pd.merge(
                    df_real_vol_agrupado,
                    df_budget_vol_agrupado,
                    on=colunas_vol_agrupamento,
                    how='inner'
                )
                
                if len(df_volumes) == 0:
                    return pd.DataFrame()
                
                # Preparar dados de budget: preencher NaN em colunas categóricas antes de agrupar - OTIMIZADO
                df_budget_filtrado_clean = df_budget_filtrado.copy()
                colunas_agrupamento_bud = ['Custo']
                if 'Type 05' in df_budget_filtrado.columns:
                    colunas_agrupamento_bud.append('Type 05')
                if 'Type 06' in df_budget_filtrado.columns:
                    colunas_agrupamento_bud.append('Type 06')
                if 'Account' in df_budget_filtrado.columns:
                    colunas_agrupamento_bud.append('Account')
                
                for col in colunas_agrupamento_bud:
                    if col in df_budget_filtrado_clean.columns:
                        if df_budget_filtrado_clean[col].dtype.name == 'category':
                            df_budget_filtrado_clean[col] = df_budget_filtrado_clean[col].astype(str)
                        # Usar fillna apenas se houver NaN
                        if df_budget_filtrado_clean[col].isna().any():
                            df_budget_filtrado_clean[col] = df_budget_filtrado_clean[col].fillna('Não Informado')
                
                # Agrupar dados reais por período e categoria (mesma lógica do gráfico)
                colunas_agrupamento_periodo = colunas_vol_agrupamento + ['Custo'] + colunas_agrupamento[1:]  # Adicionar Type 05, Type 06, Account
                colunas_agrupamento_periodo = [c for c in colunas_agrupamento_periodo if c in df_real_filtrado_clean.columns]
                
                # NOTA: A conversão de moeda já foi aplicada no df_total (linha ~702)
                # Portanto, df_real_filtrado_clean['Total'] já está convertido
                
                # SEMPRE usar 'Total' para calcular FLEX (não CPU)
                df_real_por_periodo = df_real_filtrado_clean.groupby(colunas_agrupamento_periodo)['Total'].sum().reset_index()
                
                # Agrupar budget por período e categoria
                colunas_agrupamento_bud_periodo = colunas_vol_agrupamento + ['Custo'] + [c for c in colunas_agrupamento[1:] if c in df_budget_filtrado_clean.columns]
                df_budget_por_periodo = df_budget_filtrado_clean.groupby(colunas_agrupamento_bud_periodo)['Total'].sum().reset_index()
                
                # OTIMIZAÇÃO: Substituir iterrows() por operações vetorizadas usando merge
                # Fazer merge de volumes com dados reais e budget por período
                
                # Preparar dados para merge: converter colunas categóricas para string
                df_real_por_periodo_merge = df_real_por_periodo.copy()
                df_budget_por_periodo_merge = df_budget_por_periodo.copy()
                
                for col in colunas_vol_agrupamento:
                    if col in df_real_por_periodo_merge.columns:
                        if df_real_por_periodo_merge[col].dtype.name == 'category':
                            df_real_por_periodo_merge[col] = df_real_por_periodo_merge[col].astype(str)
                        df_real_por_periodo_merge[col] = df_real_por_periodo_merge[col].fillna('Não Informado')
                    if col in df_budget_por_periodo_merge.columns:
                        if df_budget_por_periodo_merge[col].dtype.name == 'category':
                            df_budget_por_periodo_merge[col] = df_budget_por_periodo_merge[col].astype(str)
                        df_budget_por_periodo_merge[col] = df_budget_por_periodo_merge[col].fillna('Não Informado')
                
                # Fazer merge de volumes com dados reais
                df_real_com_volumes = pd.merge(
                    df_real_por_periodo_merge,
                    df_volumes,
                    on=colunas_vol_agrupamento,
                    how='inner'
                )
                
                # Filtrar volumes válidos
                df_real_com_volumes = df_real_com_volumes[
                    (df_real_com_volumes['Volume_Real'] != 0) & 
                    (df_real_com_volumes['Volume_Real'].notna())
                ].copy()
                
                # IMPORTANTE: O Total (Real) já foi convertido antes de agrupar (linha ~3016)
                # Não converter novamente aqui!
                
                if len(df_real_com_volumes) == 0:
                    return pd.DataFrame()
                
                # Calcular proporções de volume (vetorizado)
                df_real_com_volumes['proporcao_volume'] = np.where(
                    df_real_com_volumes['Volume_Real'] != 0,
                    df_real_com_volumes['Volume_Budget'] / df_real_com_volumes['Volume_Real'],
                    1.0
                )
                df_real_com_volumes['variacao_percentual'] = df_real_com_volumes['proporcao_volume'] - 1.0
                df_real_com_volumes['proporcao_volume_real_bud'] = np.where(
                    (df_real_com_volumes['Volume_Budget'] != 0) & df_real_com_volumes['Volume_Budget'].notna(),
                    df_real_com_volumes['Volume_Real'] / df_real_com_volumes['Volume_Budget'],
                    1.0
                )
                
                # Agrupar budget por período para calcular totais do período
                # NOTA: A conversão de moeda já foi aplicada no df_budget (linha ~2563)
                # Portanto, df_budget_por_periodo_merge['Total'] já está convertido
                
                budget_por_periodo_agg = df_budget_por_periodo_merge.groupby(colunas_vol_agrupamento).agg({
                    'Total': 'sum'
                }).reset_index()
                budget_por_periodo_agg.rename(columns={'Total': 'budget_total_periodo'}, inplace=True)
                
                # Separar budget fixo e variável por período
                budget_fixo_periodo = df_budget_por_periodo_merge[df_budget_por_periodo_merge['Custo'] == 'Fixo'].groupby(colunas_vol_agrupamento)['Total'].sum().reset_index()
                budget_fixo_periodo.rename(columns={'Total': 'custo_fixo_budget_periodo'}, inplace=True)
                
                budget_variavel_periodo = df_budget_por_periodo_merge[df_budget_por_periodo_merge['Custo'] == 'Variável'].groupby(colunas_vol_agrupamento)['Total'].sum().reset_index()
                budget_variavel_periodo.rename(columns={'Total': 'custo_variavel_budget_periodo'}, inplace=True)
                
                # Fazer merge com volumes
                df_volumes_com_budget = pd.merge(
                    df_volumes,
                    budget_por_periodo_agg,
                    on=colunas_vol_agrupamento,
                    how='left'
                )
                df_volumes_com_budget = pd.merge(
                    df_volumes_com_budget,
                    budget_fixo_periodo,
                    on=colunas_vol_agrupamento,
                    how='left'
                )
                df_volumes_com_budget = pd.merge(
                    df_volumes_com_budget,
                    budget_variavel_periodo,
                    on=colunas_vol_agrupamento,
                    how='left'
                )
                df_volumes_com_budget['custo_fixo_budget_periodo'] = df_volumes_com_budget['custo_fixo_budget_periodo'].fillna(0.0)
                df_volumes_com_budget['custo_variavel_budget_periodo'] = df_volumes_com_budget['custo_variavel_budget_periodo'].fillna(0.0)
                df_volumes_com_budget['budget_total_periodo'] = df_volumes_com_budget['budget_total_periodo'].fillna(0.0)
                
                # Calcular Flex Bud total por período (vetorizado)
                # IMPORTANTE: Os valores de budget já foram convertidos ANTES de agrupar (linha 3079)
                # Portanto, flex_bud_total_periodo já estará na moeda correta
                if tipo_viz == "Custo Total":
                    df_volumes_com_budget['flex_bud_fixo_periodo'] = df_volumes_com_budget['custo_fixo_budget_periodo']
                    df_volumes_com_budget['proporcao_volume_real_bud'] = np.where(
                        (df_volumes_com_budget['Volume_Budget'] != 0) & df_volumes_com_budget['Volume_Budget'].notna(),
                        df_volumes_com_budget['Volume_Real'] / df_volumes_com_budget['Volume_Budget'],
                        1.0
                    )
                    df_volumes_com_budget['flex_bud_variavel_periodo'] = df_volumes_com_budget['custo_variavel_budget_periodo'] * df_volumes_com_budget['proporcao_volume_real_bud']
                    df_volumes_com_budget['flex_bud_total_periodo'] = df_volumes_com_budget['flex_bud_fixo_periodo'] + df_volumes_com_budget['flex_bud_variavel_periodo']
                else:
                    # Para CPU: calcular real por período
                    real_fixo_periodo = df_real_com_volumes[df_real_com_volumes['Custo'] == 'Fixo'].groupby(colunas_vol_agrupamento)['Total'].sum().reset_index()
                    real_fixo_periodo.rename(columns={'Total': 'custo_fixo_real_periodo'}, inplace=True)
                    real_variavel_periodo = df_real_com_volumes[df_real_com_volumes['Custo'] == 'Variável'].groupby(colunas_vol_agrupamento)['Total'].sum().reset_index()
                    real_variavel_periodo.rename(columns={'Total': 'custo_variavel_real_periodo'}, inplace=True)
                    
                    df_volumes_com_budget = pd.merge(
                        df_volumes_com_budget,
                        real_fixo_periodo,
                        on=colunas_vol_agrupamento,
                        how='left'
                    )
                    df_volumes_com_budget = pd.merge(
                        df_volumes_com_budget,
                        real_variavel_periodo,
                        on=colunas_vol_agrupamento,
                        how='left'
                    )
                    df_volumes_com_budget['custo_fixo_real_periodo'] = df_volumes_com_budget['custo_fixo_real_periodo'].fillna(0.0)
                    df_volumes_com_budget['custo_variavel_real_periodo'] = df_volumes_com_budget['custo_variavel_real_periodo'].fillna(0.0)
                    
                    df_volumes_com_budget['variacao_percentual'] = np.where(
                        df_volumes_com_budget['Volume_Real'] != 0,
                        (df_volumes_com_budget['Volume_Budget'] / df_volumes_com_budget['Volume_Real']) - 1.0,
                        0.0
                    )
                    df_volumes_com_budget['flex_variavel_periodo'] = df_volumes_com_budget['custo_variavel_real_periodo'] * df_volumes_com_budget['variacao_percentual']
                    df_volumes_com_budget['flex_total_periodo'] = df_volumes_com_budget['flex_variavel_periodo']
                    df_volumes_com_budget['flex_bud_total_periodo'] = df_volumes_com_budget['budget_total_periodo'] + df_volumes_com_budget['flex_total_periodo']
                
                # Calcular total real por período
                total_real_por_periodo = df_real_com_volumes.groupby(colunas_vol_agrupamento)['Total'].sum().reset_index()
                total_real_por_periodo.rename(columns={'Total': 'total_real_periodo'}, inplace=True)
                
                # Fazer merge com Flex Bud total por período
                df_real_com_volumes = pd.merge(
                    df_real_com_volumes,
                    df_volumes_com_budget[colunas_vol_agrupamento + ['flex_bud_total_periodo']],
                    on=colunas_vol_agrupamento,
                    how='left'
                )
                df_real_com_volumes = pd.merge(
                    df_real_com_volumes,
                    total_real_por_periodo,
                    on=colunas_vol_agrupamento,
                    how='left'
                )
                
                # Fazer merge com budget por categoria e período
                df_real_com_budget = pd.merge(
                    df_real_com_volumes,
                    df_budget_por_periodo_merge,
                    on=colunas_vol_agrupamento + ['Custo'] + [c for c in colunas_agrupamento[1:] if c in df_budget_por_periodo_merge.columns],
                    how='left',
                    suffixes=('', '_budget')
                )
                df_real_com_budget['Total_budget'] = df_real_com_budget['Total_budget'].fillna(0.0)
                
                # NOTA: A conversão de moeda já foi aplicada:
                # - df_real_filtrado_clean['Total'] foi convertido ANTES de agrupar (linha ~3016)
                # - df_budget_por_periodo_merge['Total'] foi convertido ANTES de agrupar (linha 3072)
                # - Portanto, Total_budget e flex_bud_total_periodo já estão na moeda correta
                # NÃO converter novamente aqui para evitar conversão dupla!
                
                # Calcular valores finais (vetorizado)
                if tipo_viz == "Custo Total":
                    # Calcular proporção desta categoria no total do período
                    df_real_com_budget['proporcao_categoria'] = np.where(
                        df_real_com_budget['total_real_periodo'] != 0,
                        df_real_com_budget['Total'] / df_real_com_budget['total_real_periodo'],
                        0.0
                    )
                    # Distribuir Flex Bud total proporcionalmente
                    df_real_com_budget['Flex_BUD'] = df_real_com_budget['flex_bud_total_periodo'] * df_real_com_budget['proporcao_categoria']
                    df_real_com_budget['BUD'] = df_real_com_budget['Total_budget']
                    df_real_com_budget['Total'] = df_real_com_budget['Total']
                else:
                    # Para CPU: manter lógica anterior
                    df_real_com_budget['variacao_percentual'] = np.where(
                        df_real_com_budget['Volume_Real'] != 0,
                        (df_real_com_budget['Volume_Budget'] / df_real_com_budget['Volume_Real']) - 1.0,
                        0.0
                    )
                    df_real_com_budget['flex_ajuste'] = np.where(
                        df_real_com_budget['Custo'] == 'Variável',
                        df_real_com_budget['Total'] * df_real_com_budget['variacao_percentual'],
                        0.0
                    )
                    # NOTA: Total e Total_budget já estão convertidos (aplicados no início)
                    # Portanto, flex_ajuste e Flex_BUD já estarão na moeda correta
                    df_real_com_budget['Flex_BUD'] = df_real_com_budget['Total_budget'] + df_real_com_budget['flex_ajuste']
                    df_real_com_budget['BUD'] = df_real_com_budget['Total_budget']
                    df_real_com_budget['Total'] = df_real_com_budget['Total']
                
                # Selecionar colunas finais
                colunas_finais = colunas_agrupamento + ['Flex_BUD', 'BUD', 'Total']
                df_flex_df = df_real_com_budget[colunas_finais].copy()
                
                # Agrupar por categoria (sem período)
                colunas_agrupamento_final = ['Custo']
                if 'Type 05' in df_flex_df.columns:
                    colunas_agrupamento_final.append('Type 05')
                if 'Type 06' in df_flex_df.columns:
                    colunas_agrupamento_final.append('Type 06')
                if 'Account' in df_flex_df.columns:
                    colunas_agrupamento_final.append('Account')
                
                df_flex_agrupado = df_flex_df.groupby(colunas_agrupamento_final).agg({
                    'Flex_BUD': 'sum',
                    'BUD': 'sum',
                    'Total': 'sum'
                }).reset_index()
                
                # Calcular diferenças
                df_flex_agrupado['Flex_Bud_BUD'] = df_flex_agrupado['Flex_BUD'] - df_flex_agrupado['BUD']
                df_flex_agrupado['Total_Flex_Bud'] = df_flex_agrupado['Total'] - df_flex_agrupado['Flex_BUD']
                df_flex_agrupado['Total_Flex_Bud_Ratio'] = df_flex_agrupado['Total'] / df_flex_agrupado['Flex_BUD'].replace(0, np.nan)
                df_flex_agrupado['Total_Flex_Bud_Ratio'] = df_flex_agrupado['Total_Flex_Bud_Ratio'].fillna(0)
                
                # Remover linhas onde todos os valores importantes são zero (ou muito próximos de zero)
                # Considerar uma linha zerada se BUD, Flex_BUD e Total são todos zero
                df_flex_agrupado = df_flex_agrupado[
                    (df_flex_agrupado['BUD'].abs() > 0.01) | 
                    (df_flex_agrupado['Flex_BUD'].abs() > 0.01) | 
                    (df_flex_agrupado['Total'].abs() > 0.01)
                ].copy()
                
                return df_flex_agrupado
                
            except Exception as e:
                st.error(f"Erro ao calcular tabela Flex Bud: {e}")
                import traceback
                st.error(traceback.format_exc())
                return pd.DataFrame()
        
        # Calcular valores da tabela usando os mesmos dados do gráfico
        # IMPORTANTE: No modo CPU, df_grafico_periodo pode não ter a coluna 'Custo'
        # Usar df_real_original_grafico (que tem 'Custo') ou df_para_grafico_periodo
        df_tabela_flex = pd.DataFrame()  # Inicializar como DataFrame vazio
        
        if df_budget_filtrado is not None and df_volume_real_filtrado is not None:
            # IMPORTANTE: No modo CPU, usar dados originais com coluna 'Custo'
            df_real_para_tabela = None
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                # No modo CPU, usar dados originais com coluna 'Custo'
                if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns:
                    df_real_para_tabela = df_real_original_grafico.copy()
                elif 'Custo' in df_para_grafico_periodo.columns:
                    df_real_para_tabela = df_para_grafico_periodo.copy()
                    # Aplicar mesmos filtros de Oficina e Veículo
                    if 'Oficina' in df_real_para_tabela.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_para_tabela = df_real_para_tabela[
                                df_real_para_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'Veículo' in df_real_para_tabela.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_para_tabela = df_real_para_tabela[
                                df_real_para_tabela['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                else:
                    st.warning("⚠️ Coluna 'Custo' não encontrada nos dados. A tabela Flex Bud requer esta coluna.")
                    df_tabela_flex = pd.DataFrame()
            else:
                # No modo Custo Total, usar df_grafico_periodo
                df_real_para_tabela = df_grafico_periodo.copy()
            
            if df_real_para_tabela is not None and 'Custo' in df_real_para_tabela.columns:
                # Verificar se tem Ano
                tem_ano_tabela = 'Ano' in df_real_para_tabela.columns
                
                # IMPORTANTE: Sempre calcular em "Custo Total" primeiro, depois converter para CPU se necessário
                df_tabela_flex = calcular_tabela_flex_bud(
                    df_real_para_tabela,  # Dados reais com coluna 'Custo'
                    df_budget_filtrado,  # Dados de budget (mesma base do gráfico)
                    df_volume_real_filtrado,  # Volume real (mesma base do gráfico)
                    df_budget_vol_filtrado,  # Volume budget (mesma base do gráfico)
                    tem_ano_tabela,
                    periodo_selecionado_tabela,  # Períodos selecionados no filtro
                    "Custo Total"  # SEMPRE calcular em Custo Total primeiro
                )
                
                # Se for modo CPU, converter os valores dividindo pelos volumes
                if tipo_visualizacao == "CPU (Custo por Unidade)" and not df_tabela_flex.empty:
                    # Calcular volumes totais para os períodos selecionados
                    df_vol_real_tabela = df_volume_real_filtrado.copy()
                    df_vol_budget_tabela = df_budget_vol_filtrado.copy()
                    
                    if periodo_selecionado_tabela and "Todos" not in periodo_selecionado_tabela and 'Período' in df_vol_real_tabela.columns:
                        df_vol_real_tabela = df_vol_real_tabela[
                            df_vol_real_tabela['Período'].astype(str).isin([str(p) for p in periodo_selecionado_tabela])
                        ].copy()
                        df_vol_budget_tabela = df_vol_budget_tabela[
                            df_vol_budget_tabela['Período'].astype(str).isin([str(p) for p in periodo_selecionado_tabela])
                        ].copy()
                    
                    # Calcular volumes totais - MESMA LÓGICA DO GRÁFICO
                    # Agrupar por Ano e Período primeiro (se tiver coluna Ano), depois somar
                    # Isso garante que o volume usado nos cálculos de CPU seja igual ao do gráfico
                    if 'Ano' in df_vol_real_tabela.columns:
                        # Agrupar por Ano e Período, somar Volume (mesma lógica do gráfico)
                        df_vol_real_agrupado_cpu = df_vol_real_tabela.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        volume_real_total = df_vol_real_agrupado_cpu['Volume'].sum() if len(df_vol_real_agrupado_cpu) > 0 else 0
                    else:
                        # Sem coluna Ano: agrupar apenas por Período
                        df_vol_real_agrupado_cpu = df_vol_real_tabela.groupby('Período')['Volume'].sum().reset_index()
                        volume_real_total = df_vol_real_agrupado_cpu['Volume'].sum() if len(df_vol_real_agrupado_cpu) > 0 else 0
                    
                    if 'Ano' in df_vol_budget_tabela.columns:
                        # Agrupar por Ano e Período, somar Volume (mesma lógica do gráfico)
                        df_vol_budget_agrupado_cpu = df_vol_budget_tabela.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        volume_budget_total = df_vol_budget_agrupado_cpu['Volume'].sum() if len(df_vol_budget_agrupado_cpu) > 0 else 0
                    else:
                        # Sem coluna Ano: agrupar apenas por Período
                        df_vol_budget_agrupado_cpu = df_vol_budget_tabela.groupby('Período')['Volume'].sum().reset_index()
                        volume_budget_total = df_vol_budget_agrupado_cpu['Volume'].sum() if len(df_vol_budget_agrupado_cpu) > 0 else 0
                    
                    # Converter valores de Custo Total para CPU
                    # 1) BUD: resultado do modo Custo Total do BUD / volume BUD
                    if volume_budget_total != 0 and not pd.isna(volume_budget_total):
                        df_tabela_flex['BUD'] = df_tabela_flex['BUD'] / volume_budget_total
                    
                    # 2) Flex BUD: resultado Flex Bud (do modo Custo Total) / volume Real
                    if volume_real_total != 0 and not pd.isna(volume_real_total):
                        df_tabela_flex['Flex_BUD'] = df_tabela_flex['Flex_BUD'] / volume_real_total
                    
                    # 3) Total: fazer o mesmo cálculo que já faz (Total / volume Real)
                    if volume_real_total != 0 and not pd.isna(volume_real_total):
                        df_tabela_flex['Total'] = df_tabela_flex['Total'] / volume_real_total
                    
                    # Recalcular diferenças após conversão para CPU
                    # Flex Bud - BUD: subtração
                    df_tabela_flex['Flex_Bud_BUD'] = df_tabela_flex['Flex_BUD'] - df_tabela_flex['BUD']
                    # Total - Flex Bud: subtração
                    df_tabela_flex['Total_Flex_Bud'] = df_tabela_flex['Total'] - df_tabela_flex['Flex_BUD']
                    # Total / Flex Bud: divisão
                    df_tabela_flex['Total_Flex_Bud_Ratio'] = df_tabela_flex['Total'] / df_tabela_flex['Flex_BUD'].replace(0, np.nan)
                    df_tabela_flex['Total_Flex_Bud_Ratio'] = df_tabela_flex['Total_Flex_Bud_Ratio'].fillna(0)
            else:
                st.warning("⚠️ Coluna 'Custo' não encontrada nos dados. A tabela Flex Bud requer esta coluna.")
                df_tabela_flex = pd.DataFrame()
            
            if not df_tabela_flex.empty:
                # CSS para reduzir tamanho das métricas (aplicar uma vez no início)
                st.markdown("""
                    <style>
                    .stMetric {
                        font-size: 0.85rem;
                    }
                    .stMetric [data-testid="stMetricValue"] {
                        font-size: 0.9rem !important;
                    }
                    .stMetric [data-testid="stMetricLabel"] {
                        font-size: 0.75rem !important;
                    }
                    </style>
                """, unsafe_allow_html=True)
                
                # Mostrar tabela baseado na seleção
                if tipo_visualizacao_tabela == "Fixo / Variável":
                    # Preparar dados para exibição com agrupamento expansível
                    # Criar estrutura hierárquica: Custo > Type 05 > Type 06
                    
                    # Ordenar por Custo, Type 05, Type 06, Account
                    colunas_ordenacao = ['Custo']
                    if 'Type 05' in df_tabela_flex.columns:
                        colunas_ordenacao.append('Type 05')
                    if 'Type 06' in df_tabela_flex.columns:
                        colunas_ordenacao.append('Type 06')
                    if 'Account' in df_tabela_flex.columns:
                        colunas_ordenacao.append('Account')
                    
                    df_tabela_flex_ordenado = df_tabela_flex.sort_values(colunas_ordenacao)
                    
                    # Criar tabela com agrupamento expansível usando st.dataframe com groupby
                    # Agrupar por Custo
                    custos = df_tabela_flex_ordenado['Custo'].unique() if 'Custo' in df_tabela_flex_ordenado.columns else []
                    
                    for custo in sorted(custos):
                        df_custo = df_tabela_flex_ordenado[df_tabela_flex_ordenado['Custo'] == custo].copy()
                        
                        # Calcular totais do grupo
                        total_bud = df_custo['BUD'].sum()
                        total_flex_bud_bud = df_custo['Flex_Bud_BUD'].sum()
                        total_flex_bud = df_custo['Flex_BUD'].sum()
                        total_total_flex_bud = df_custo['Total_Flex_Bud'].sum()
                        total_total = df_custo['Total'].sum()
                        total_ratio = total_total / total_flex_bud if total_flex_bud != 0 else 0
                        
                        # Criar expander para cada Custo
                        with st.expander(f"💰 {custo} - Total: R$ {total_total:,.2f}", expanded=False):
                            # Tabela de resumo do grupo
                            st.markdown(f"**Resumo - {custo}**")
                            col1, col2, col3, col4, col5, col6 = st.columns(6)
                            with col1:
                                st.metric("BUD", f"R$ {total_bud:,.2f}")
                            with col2:
                                st.metric("Flex Bud - BUD", f"R$ {total_flex_bud_bud:,.2f}")
                            with col3:
                                st.metric("Flex BUD", f"R$ {total_flex_bud:,.2f}")
                            with col4:
                                st.metric("Total - Flex Bud", f"R$ {total_total_flex_bud:,.2f}")
                            with col5:
                                st.metric("Total", f"R$ {total_total:,.2f}")
                            with col6:
                                # Formatar como percentual com indicador visual
                                percentual_ratio = total_ratio * 100
                                st.markdown(f"**Total / Flex Bud**")
                                st.markdown(formatar_ratio_com_barra(total_ratio), unsafe_allow_html=True)
                            
                            # Agrupar por Type 05 dentro de cada Custo
                            if 'Type 05' in df_custo.columns:
                                type05s = df_custo['Type 05'].dropna().unique()
                                
                                for type05 in sorted(type05s):
                                    df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                    
                                    # Calcular totais do Type 05
                                    total_bud_t05 = df_type05['BUD'].sum()
                                    total_flex_bud_bud_t05 = df_type05['Flex_Bud_BUD'].sum()
                                    total_flex_bud_t05 = df_type05['Flex_BUD'].sum()
                                    total_total_flex_bud_t05 = df_type05['Total_Flex_Bud'].sum()
                                    total_total_t05 = df_type05['Total'].sum()
                                    total_ratio_t05 = total_total_t05 / total_flex_bud_t05 if total_flex_bud_t05 != 0 else 0
                                    
                                    # Criar expander para Type 05
                                    with st.expander(f"📋 Type 05: {type05} - Total: R$ {total_total_t05:,.2f}", expanded=False):
                                        # Tabela de resumo do Type 05
                                        st.markdown(f"**Resumo - {custo} > {type05}**")
                                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                                        with col1:
                                            st.metric("BUD", f"R$ {total_bud_t05:,.2f}")
                                        with col2:
                                            st.metric("Flex Bud - BUD", f"R$ {total_flex_bud_bud_t05:,.2f}")
                                        with col3:
                                            st.metric("Flex BUD", f"R$ {total_flex_bud_t05:,.2f}")
                                        with col4:
                                            st.metric("Total - Flex Bud", f"R$ {total_total_flex_bud_t05:,.2f}")
                                        with col5:
                                            st.metric("Total", f"R$ {total_total_t05:,.2f}")
                                        with col6:
                                            # Formatar como percentual com indicador visual
                                            st.markdown(f"**Total / Flex Bud**")
                                            st.markdown(formatar_ratio_com_barra(total_ratio_t05), unsafe_allow_html=True)
                                        
                                        # Agrupar por Type 06 dentro de cada Type 05
                                        if 'Type 06' in df_type05.columns:
                                            type06s = df_type05['Type 06'].dropna().unique()
                                            
                                            for type06 in sorted(type06s):
                                                df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                
                                                # Calcular totais do Type 06
                                                total_bud_t06 = df_type06['BUD'].sum()
                                                total_flex_bud_bud_t06 = df_type06['Flex_Bud_BUD'].sum()
                                                total_flex_bud_t06 = df_type06['Flex_BUD'].sum()
                                                total_total_flex_bud_t06 = df_type06['Total_Flex_Bud'].sum()
                                                total_total_t06 = df_type06['Total'].sum()
                                                total_ratio_t06 = total_total_t06 / total_flex_bud_t06 if total_flex_bud_t06 != 0 else 0
                                                
                                                # Criar expander para Type 06
                                                with st.expander(f"📊 Type 06: {type06} - Total: R$ {total_total_t06:,.2f}", expanded=False):
                                                    # Tabela de resumo do Type 06
                                                    st.markdown(f"**Resumo - {custo} > {type05} > {type06}**")
                                                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                                                    with col1:
                                                        st.metric("BUD", f"R$ {total_bud_t06:,.2f}")
                                                    with col2:
                                                        st.metric("Flex Bud - BUD", f"R$ {total_flex_bud_bud_t06:,.2f}")
                                                    with col3:
                                                        st.metric("Flex BUD", f"R$ {total_flex_bud_t06:,.2f}")
                                                    with col4:
                                                        st.metric("Total - Flex Bud", f"R$ {total_total_flex_bud_t06:,.2f}")
                                                    with col5:
                                                        st.metric("Total", f"R$ {total_total_t06:,.2f}")
                                                    with col6:
                                                        # Formatar como percentual com indicador visual
                                                        st.markdown(f"**Total / Flex Bud**")
                                                        st.markdown(formatar_ratio_com_barra(total_ratio_t06), unsafe_allow_html=True)
                                                    
                                                    # Agrupar por Account (Type 07) dentro de cada Type 06
                                                    # Exibir todos os Accounts como linhas em uma única tabela
                                                    if 'Account' in df_type06.columns:
                                                        # Preparar DataFrame com todas as linhas de Account
                                                        df_display = df_type06[[
                                                            'Account', 'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                            'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                                        ]].copy()
                                                        
                                                        # Ordenar por Account
                                                        df_display = df_display.sort_values('Account')
                                                        
                                                        # Renomear colunas
                                                        df_display.columns = [
                                                            'Account', 'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                            'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                                        ]
                                                        
                                                        # Formatar valores
                                                        for col in df_display.columns:
                                                            if col not in ['Account', 'Total / Flex Bud']:
                                                                df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}")
                                                        
                                                        # Configurar coluna "Total / Flex Bud" com HTML
                                                        if 'Total / Flex Bud' in df_display.columns:
                                                            # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                            # Mapear Account para valores originais
                                                            df_type06_account = df_type06[df_type06['Account'].isin(df_display['Account'].values)].copy()
                                                            df_type06_account = df_type06_account.sort_values('Account')
                                                            valores_originais = df_type06_account['Total_Flex_Bud_Ratio'].values
                                                            
                                                            # Aplicar formatação HTML
                                                            df_display['Total / Flex Bud'] = [
                                                                formatar_ratio_com_barra(val) for val in valores_originais
                                                            ]
                                                            
                                                            # Criar tabela HTML customizada para renderizar HTML
                                                            html_table = criar_tabela_html_com_barra(df_display)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            st.dataframe(df_display, use_container_width=True, hide_index=True)
                                                    else:
                                                        # Se não tem Account, mostrar Type 06 diretamente (linhas são por Type 06)
                                                        df_display = df_type06[[
                                                            'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                            'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                                        ]].copy()
                                                        
                                                        df_display.columns = [
                                                            'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                            'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                                        ]
                                                        
                                                        for col in df_display.columns:
                                                            if col != 'Total / Flex Bud':
                                                                df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}")
                                                        
                                                        # Configurar coluna "Total / Flex Bud" com HTML
                                                        if 'Total / Flex Bud' in df_display.columns:
                                                            # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                            valores_originais = df_type06['Total_Flex_Bud_Ratio'].values
                                                            
                                                            # Aplicar formatação HTML
                                                            df_display['Total / Flex Bud'] = [
                                                                formatar_ratio_com_barra(val) for val in valores_originais
                                                            ]
                                                            
                                                            # Criar tabela HTML customizada para renderizar HTML
                                                            html_table = criar_tabela_html_com_barra(df_display)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            st.dataframe(df_display, use_container_width=True, hide_index=True)
                                        else:
                                            # Se não tem Type 06, mostrar Type 05 diretamente (linhas são por Type 05)
                                            df_display = df_type05[[
                                                'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                            ]].copy()
                                            
                                            df_display.columns = [
                                                'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                            ]
                                            
                                            for col in df_display.columns:
                                                if col != 'Total / Flex Bud':
                                                    df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}")
                                            
                                            # Configurar coluna "Total / Flex Bud" com HTML
                                            if 'Total / Flex Bud' in df_display.columns:
                                                # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                valores_originais = df_type05['Total_Flex_Bud_Ratio'].values
                                                
                                                # Aplicar formatação HTML
                                                df_display['Total / Flex Bud'] = [
                                                    formatar_ratio_com_barra(val) for val in valores_originais
                                                ]
                                                
                                                # Criar tabela HTML customizada para renderizar HTML
                                                html_table = criar_tabela_html_com_barra(df_display)
                                                st.markdown(html_table, unsafe_allow_html=True)
                                            else:
                                                st.dataframe(df_display, use_container_width=True, hide_index=True)
                            else:
                                # Se não tem Type 05, mostrar Custo diretamente (linhas são por Custo)
                                df_display = df_custo[[
                                    'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                    'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                ]].copy()
                                
                                df_display.columns = [
                                    'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                    'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                ]
                                
                                for col in df_display.columns:
                                    if col != 'Total / Flex Bud':
                                        df_display[col] = df_display[col].apply(lambda x: f"{x:,.2f}")
                                
                                # Configurar coluna "Total / Flex Bud" com HTML
                                if 'Total / Flex Bud' in df_display.columns:
                                    # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                    valores_originais = df_custo['Total_Flex_Bud_Ratio'].values
                                    
                                    # Aplicar formatação HTML
                                    df_display['Total / Flex Bud'] = [
                                        formatar_ratio_com_barra(val) for val in valores_originais
                                    ]
                                    
                                    # Criar tabela HTML customizada para renderizar HTML
                                    html_table = criar_tabela_html_com_barra(df_display)
                                    st.markdown(html_table, unsafe_allow_html=True)
                                else:
                                    st.dataframe(df_display, use_container_width=True, hide_index=True)
                
                elif tipo_visualizacao_tabela == "Total":
                    # Criar tabela com Total agregado (sem separar Fixo e Variável)
                    # Agrupar tudo sem separar por Custo
                    colunas_agrupamento_total = []
                    if 'Type 05' in df_tabela_flex.columns:
                        colunas_agrupamento_total.append('Type 05')
                    if 'Type 06' in df_tabela_flex.columns:
                        colunas_agrupamento_total.append('Type 06')
                    if 'Account' in df_tabela_flex.columns:
                        colunas_agrupamento_total.append('Account')
                    
                    if len(colunas_agrupamento_total) > 0:
                        # Agrupar por Type 05, Type 06, Account (sem Custo)
                        df_tabela_total = df_tabela_flex.groupby(colunas_agrupamento_total).agg({
                            'BUD': 'sum',
                            'Flex_Bud_BUD': 'sum',
                            'Flex_BUD': 'sum',
                            'Total_Flex_Bud': 'sum',
                            'Total': 'sum',
                            'Total_Flex_Bud_Ratio': lambda x: x.iloc[0] if len(x) > 0 else 0  # Manter ratio do primeiro (será recalculado)
                        }).reset_index()
                        
                        # Recalcular ratio
                        df_tabela_total['Total_Flex_Bud_Ratio'] = df_tabela_total['Total'] / df_tabela_total['Flex_BUD'].replace(0, np.nan)
                        df_tabela_total['Total_Flex_Bud_Ratio'] = df_tabela_total['Total_Flex_Bud_Ratio'].fillna(0)
                        
                        # Filtrar linhas zeradas
                        df_tabela_total = df_tabela_total[
                            (df_tabela_total['BUD'].abs() > 0.01) | 
                            (df_tabela_total['Flex_BUD'].abs() > 0.01) | 
                            (df_tabela_total['Total'].abs() > 0.01)
                        ].copy()
                        
                        # Ordenar
                        df_tabela_total = df_tabela_total.sort_values(colunas_agrupamento_total)
                    else:
                        df_tabela_total = pd.DataFrame()
                    
                    # Calcular total geral
                    if not df_tabela_total.empty:
                        total_geral_bud = df_tabela_total['BUD'].sum()
                        total_geral_flex_bud_bud = df_tabela_total['Flex_Bud_BUD'].sum()
                        total_geral_flex_bud = df_tabela_total['Flex_BUD'].sum()
                        total_geral_total_flex_bud = df_tabela_total['Total_Flex_Bud'].sum()
                        total_geral_total = df_tabela_total['Total'].sum()
                        total_geral_ratio = total_geral_total / total_geral_flex_bud if total_geral_flex_bud != 0 else 0
                        
                        # Criar expander para Total
                        with st.expander(f"💰 Total - Total: R$ {total_geral_total:,.2f}", expanded=False):
                            # Tabela de resumo do Total
                            st.markdown(f"**Resumo - Total**")
                            col1, col2, col3, col4, col5, col6 = st.columns(6)
                            with col1:
                                st.metric("BUD", f"R$ {total_geral_bud:,.2f}")
                            with col2:
                                st.metric("Flex Bud - BUD", f"R$ {total_geral_flex_bud_bud:,.2f}")
                            with col3:
                                st.metric("Flex BUD", f"R$ {total_geral_flex_bud:,.2f}")
                            with col4:
                                st.metric("Total - Flex Bud", f"R$ {total_geral_total_flex_bud:,.2f}")
                            with col5:
                                st.metric("Total", f"R$ {total_geral_total:,.2f}")
                            with col6:
                                # Formatar como percentual com indicador visual
                                st.markdown(f"**Total / Flex Bud**")
                                st.markdown(formatar_ratio_com_barra(total_geral_ratio), unsafe_allow_html=True)
                            
                            # Agrupar por Type 05
                            if 'Type 05' in df_tabela_total.columns:
                                type05s = df_tabela_total['Type 05'].dropna().unique()
                                
                                for type05 in sorted(type05s):
                                    df_type05_total = df_tabela_total[df_tabela_total['Type 05'] == type05].copy()
                                    
                                    # Calcular totais do Type 05
                                    total_bud_t05_total = df_type05_total['BUD'].sum()
                                    total_flex_bud_bud_t05_total = df_type05_total['Flex_Bud_BUD'].sum()
                                    total_flex_bud_t05_total = df_type05_total['Flex_BUD'].sum()
                                    total_total_flex_bud_t05_total = df_type05_total['Total_Flex_Bud'].sum()
                                    total_total_t05_total = df_type05_total['Total'].sum()
                                    total_ratio_t05_total = total_total_t05_total / total_flex_bud_t05_total if total_flex_bud_t05_total != 0 else 0
                                    
                                    # Criar expander para Type 05
                                    with st.expander(f"📋 Type 05: {type05} - Total: R$ {total_total_t05_total:,.2f}", expanded=False):
                                        # Tabela de resumo do Type 05
                                        st.markdown(f"**Resumo - Total > {type05}**")
                                        col1, col2, col3, col4, col5, col6 = st.columns(6)
                                        with col1:
                                            st.metric("BUD", f"R$ {total_bud_t05_total:,.2f}")
                                        with col2:
                                            st.metric("Flex Bud - BUD", f"R$ {total_flex_bud_bud_t05_total:,.2f}")
                                        with col3:
                                            st.metric("Flex BUD", f"R$ {total_flex_bud_t05_total:,.2f}")
                                        with col4:
                                            st.metric("Total - Flex Bud", f"R$ {total_total_flex_bud_t05_total:,.2f}")
                                        with col5:
                                            st.metric("Total", f"R$ {total_total_t05_total:,.2f}")
                                        with col6:
                                            # Formatar como percentual com indicador visual
                                            st.markdown(f"**Total / Flex Bud**")
                                            st.markdown(formatar_ratio_com_barra(total_ratio_t05_total), unsafe_allow_html=True)
                                        
                                        # Agrupar por Type 06
                                        if 'Type 06' in df_type05_total.columns:
                                            type06s = df_type05_total['Type 06'].dropna().unique()
                                            
                                            for type06 in sorted(type06s):
                                                df_type06_total = df_type05_total[df_type05_total['Type 06'] == type06].copy()
                                                
                                                # Calcular totais do Type 06
                                                total_bud_t06_total = df_type06_total['BUD'].sum()
                                                total_flex_bud_bud_t06_total = df_type06_total['Flex_Bud_BUD'].sum()
                                                total_flex_bud_t06_total = df_type06_total['Flex_BUD'].sum()
                                                total_total_flex_bud_t06_total = df_type06_total['Total_Flex_Bud'].sum()
                                                total_total_t06_total = df_type06_total['Total'].sum()
                                                total_ratio_t06_total = total_total_t06_total / total_flex_bud_t06_total if total_flex_bud_t06_total != 0 else 0
                                                
                                                # Criar expander para Type 06
                                                with st.expander(f"📊 Type 06: {type06} - Total: R$ {total_total_t06_total:,.2f}", expanded=False):
                                                    # Tabela de resumo do Type 06
                                                    st.markdown(f"**Resumo - Total > {type05} > {type06}**")
                                                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                                                    with col1:
                                                        st.metric("BUD", f"R$ {total_bud_t06_total:,.2f}")
                                                    with col2:
                                                        st.metric("Flex Bud - BUD", f"R$ {total_flex_bud_bud_t06_total:,.2f}")
                                                    with col3:
                                                        st.metric("Flex BUD", f"R$ {total_flex_bud_t06_total:,.2f}")
                                                    with col4:
                                                        st.metric("Total - Flex Bud", f"R$ {total_total_flex_bud_t06_total:,.2f}")
                                                    with col5:
                                                        st.metric("Total", f"R$ {total_total_t06_total:,.2f}")
                                                    with col6:
                                                        # Formatar como percentual com indicador visual
                                                        st.markdown(f"**Total / Flex Bud**")
                                                        st.markdown(formatar_ratio_com_barra(total_ratio_t06_total), unsafe_allow_html=True)
                                                    
                                                    # Exibir Accounts diretamente como linhas
                                                    if 'Account' in df_type06_total.columns:
                                                        df_display_total = df_type06_total[[
                                                            'Account', 'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                            'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                                        ]].copy()
                                                        
                                                        df_display_total = df_display_total.sort_values('Account')
                                                        
                                                        df_display_total.columns = [
                                                            'Account', 'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                            'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                                        ]
                                                        
                                                        for col in df_display_total.columns:
                                                            if col not in ['Account', 'Total / Flex Bud']:
                                                                # Adicionar sufixo baseado no fator de conversão
                                                                sufixo = ""
                                                                if fator_conversao:
                                                                    if fator_conversao == "K (milhares)":
                                                                        sufixo = " K"
                                                                    elif fator_conversao == "M (Milhões)":
                                                                        sufixo = " M"
                                                                df_display_total[col] = df_display_total[col].apply(lambda x: f"{x:,.2f}{sufixo}")
                                                        
                                                        # Configurar coluna "Total / Flex Bud" com HTML
                                                        column_config = {}
                                                        if 'Total / Flex Bud' in df_display_total.columns:
                                                            # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                            # Mapear Account para valores originais
                                                            df_type06_total_account = df_type06_total[df_type06_total['Account'].isin(df_display_total['Account'].values)].copy()
                                                            df_type06_total_account = df_type06_total_account.sort_values('Account')
                                                            valores_originais = df_type06_total_account['Total_Flex_Bud_Ratio'].values
                                                            
                                                            # Aplicar formatação HTML
                                                            df_display_total['Total / Flex Bud'] = [
                                                                formatar_ratio_com_barra(val) for val in valores_originais
                                                            ]
                                                            
                                                            # Criar tabela HTML customizada para renderizar HTML
                                                            html_table = criar_tabela_html_com_barra(df_display_total)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            st.dataframe(df_display_total, use_container_width=True, hide_index=True)
                                                    else:
                                                        # Se não tem Account, mostrar Type 06 diretamente
                                                        df_display_total = df_type06_total[[
                                                            'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                            'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                                        ]].copy()
                                                        
                                                        df_display_total.columns = [
                                                            'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                            'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                                        ]
                                                        
                                                        for col in df_display_total.columns:
                                                            if col != 'Total / Flex Bud':
                                                                # Adicionar sufixo baseado no fator de conversão
                                                                sufixo = ""
                                                                if fator_conversao:
                                                                    if fator_conversao == "K (milhares)":
                                                                        sufixo = " K"
                                                                    elif fator_conversao == "M (Milhões)":
                                                                        sufixo = " M"
                                                                df_display_total[col] = df_display_total[col].apply(lambda x: f"{x:,.2f}{sufixo}")
                                                        
                                                        # Configurar coluna "Total / Flex Bud" com HTML
                                                        column_config = {}
                                                        if 'Total / Flex Bud' in df_display_total.columns:
                                                            # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                            valores_originais = df_type06_total['Total_Flex_Bud_Ratio'].values
                                                            
                                                            # Aplicar formatação HTML
                                                            df_display_total['Total / Flex Bud'] = [
                                                                formatar_ratio_com_barra(val) for val in valores_originais
                                                            ]
                                                            
                                                            # Criar tabela HTML customizada para renderizar HTML
                                                            html_table = criar_tabela_html_com_barra(df_display_total)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            st.dataframe(df_display_total, use_container_width=True, hide_index=True)
                                        else:
                                            # Se não tem Type 06, mostrar Type 05 diretamente
                                            df_display_total = df_type05_total[[
                                                'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                                'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                            ]].copy()
                                            
                                            df_display_total.columns = [
                                                'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                                'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                            ]
                                            
                                            for col in df_display_total.columns:
                                                if col != 'Total / Flex Bud':
                                                    df_display_total[col] = df_display_total[col].apply(lambda x: f"R$ {x:,.2f}")
                                            
                                            # Configurar coluna "Total / Flex Bud" com HTML
                                            column_config = {}
                                            if 'Total / Flex Bud' in df_display_total.columns:
                                                # Obter valores originais da coluna Total_Flex_Bud_Ratio
                                                valores_originais = df_type05_total['Total_Flex_Bud_Ratio'].values
                                                
                                                # Aplicar formatação HTML
                                                df_display_total['Total / Flex Bud'] = [
                                                    formatar_ratio_com_barra(val) for val in valores_originais
                                                ]
                                                
                                                # Criar tabela HTML customizada para renderizar HTML
                                                html_table = criar_tabela_html_com_barra(df_display_total)
                                                st.markdown(html_table, unsafe_allow_html=True)
                                            else:
                                                st.dataframe(df_display_total, use_container_width=True, hide_index=True)
                            else:
                                # Se não tem Type 05, mostrar total diretamente
                                df_display_total = df_tabela_total[[
                                    'BUD', 'Flex_Bud_BUD', 'Flex_BUD', 
                                    'Total_Flex_Bud', 'Total', 'Total_Flex_Bud_Ratio'
                                ]].copy()
                                
                                df_display_total.columns = [
                                    'BUD', 'Flex Bud - BUD', 'Flex BUD',
                                    'Total - Flex Bud', 'Total', 'Total / Flex Bud'
                                ]
                                
                                for col in df_display_total.columns:
                                    if col != 'Total / Flex Bud':
                                        df_display_total[col] = df_display_total[col].apply(lambda x: f"R$ {x:,.2f}")
                                    else:
                                        df_display_total[col] = df_display_total[col].apply(lambda x: f"{x:,.2f}")
                                
                                st.dataframe(df_display_total, use_container_width=True, hide_index=True)
                    else:
                        st.info("ℹ️ Não há dados disponíveis para exibir a tabela de análise Flex Bud (Total).")
            else:
                st.info("ℹ️ Não há dados disponíveis para exibir a tabela de análise Flex Bud.")
        else:
            st.info("ℹ️ Carregue os dados de budget e volume para visualizar a tabela de análise Flex Bud.")

# ==========================================
# TAB 2: Volume
# ==========================================
with tab2:
    # Exibir gráfico de Volume logo abaixo, usando os mesmos filtros
    st.subheader("📊 Volume Total por Período")
    
    # IMPORTANTE: Usar a mesma lógica de filtragem em ambos os modos
    # para garantir que os volumes sejam consistentes
    df_vol = load_volume_data(ano_selecionado)
    
    # Carregar dados de volume do budget para o gráfico de volume
    df_budget_vol_grafico = load_budget_volume_data(ano_selecionado)
    
    if df_vol is not None:
        # Verificar se tem as colunas necessárias
        if 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
            # Aplicar TODOS os filtros da sidebar diretamente ao df_vol
            # Usar as mesmas variáveis de session_state usadas nos filtros da sidebar
            df_vol_filtrado = df_vol.copy()
            
            # Filtro 1: Oficina
            # CORREÇÃO: Garantir que apenas oficinas presentes nas opções do filtro sejam consideradas
            if 'Oficina' in df_vol_filtrado.columns:
                # Obter as opções de oficina disponíveis no df_total (mesmas opções do filtro principal)
                # Usar get_filter_options que já foi usado para criar o filtro na sidebar
                oficina_opcoes_disponiveis = get_filter_options(df_total, 'Oficina')
                # Remover "Todos" da lista de opções
                oficina_opcoes_disponiveis = [o for o in oficina_opcoes_disponiveis if o != "Todos"]
                
                # Obter oficinas selecionadas no filtro
                oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])
                
                # Se "Todos" estiver selecionado, usar todas as opções disponíveis no filtro
                if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
                    # Filtrar apenas pelas oficinas que estão nas opções do filtro (não incluir oficinas que não estão no filtro)
                        df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
                    ].copy()
                else:
                    # Filtrar apenas pelas oficinas selecionadas (que já estão nas opções do filtro)
                    # Garantir que apenas oficinas que estão nas opções sejam consideradas
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
            
            # Filtro 4: Período - NÃO aplicar aqui, mostrar todos os períodos no gráfico
            
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
            
            # Aplicar também os filtros específicos do gráfico (Oficina e Veículo) se foram selecionados
            # Isso permite que o gráfico de volume responda aos filtros do gráfico também
            if 'Oficina' in df_vol_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            if 'Veículo' in df_vol_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
            
            # Criar gráfico com dados filtrados (sempre mostrando todos os períodos)
            # Aplicar mesmos filtros ao volume do budget
            df_budget_vol_filtrado_grafico = None
            if df_budget_vol_grafico is not None:
                df_budget_vol_filtrado_grafico = df_budget_vol_grafico.copy()
                
                # Aplicar TODOS os filtros da sidebar diretamente ao df_budget_vol (mesma lógica do volume real)
                # Filtro 1: Oficina
                # CORREÇÃO: Garantir que apenas oficinas presentes nas opções do filtro sejam consideradas
                if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                    # Obter as opções de oficina disponíveis no df_total (mesmas opções do filtro principal)
                    oficina_opcoes_disponiveis = get_filter_options(df_total, 'Oficina')
                    # Remover "Todos" da lista de opções
                    oficina_opcoes_disponiveis = [o for o in oficina_opcoes_disponiveis if o != "Todos"]
                    
                    # Obter oficinas selecionadas no filtro
                    oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_ext', ["Todos"])
                    
                    # Se "Todos" estiver selecionado, usar todas as opções disponíveis no filtro
                    if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
                        # Filtrar apenas pelas oficinas que estão nas opções do filtro (não incluir oficinas que não estão no filtro)
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
                        ].copy()
                    else:
                        # Filtrar apenas pelas oficinas selecionadas (que já estão nas opções do filtro)
                        # Garantir que apenas oficinas que estão nas opções sejam consideradas
                        oficinas_validas = [o for o in oficina_selecionadas_sidebar if o in oficina_opcoes_disponiveis]
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficinas_validas)
                            ].copy()
                
                # Filtro 2: Veículo
                if 'Veículo' in df_budget_vol_filtrado_grafico.columns:
                    veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_ext', ["Todos"])
                    if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Veículo'].astype(str).isin(veiculo_selecionados_sidebar)
                        ].copy()
                
                # Filtro 3: USI
                if 'USI' in df_budget_vol_filtrado_grafico.columns:
                    usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_ext', ["Todos"])
                    if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['USI'].astype(str).isin(usi_selecionada_sidebar)
                        ].copy()
                
                # Filtro 4: Período - NÃO aplicar aqui, mostrar todos os períodos no gráfico
                
                # Filtro 5: Centro cst
                if 'Centrocst' in df_budget_vol_filtrado_grafico.columns:
                    centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_ext', "Todos")
                    if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
                        ].copy()
                
                # Filtro 6: Conta contábil
                if 'Nºconta' in df_budget_vol_filtrado_grafico.columns:
                    conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_ext', [])
                    if conta_contabil_selecionadas_sidebar:
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Nºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
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
                
                # Filtros avançados
                filtros_avancados_nomes = ["Usuário", "Material", "Dt.lçto.", "Texto breve", "Account"]
                for col_name in filtros_avancados_nomes:
                    if col_name in df_budget_vol_filtrado_grafico.columns:
                        filtro_key = f'filtro_avancado_{col_name}_tc_ext'
                        selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
                        if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico[col_name].astype(str).isin(selecionadas_sidebar)
                            ].copy()
                
                # Aplicar filtro de Oficina do gráfico
                if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Aplicar filtro de Veículo do gráfico
                if 'Veículo' in df_budget_vol_filtrado_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                            df_budget_vol_filtrado_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
            
            grafico_volume = create_volume_chart(df_vol_filtrado, df_budget_vol_filtrado_grafico)
            if grafico_volume:
                st.altair_chart(grafico_volume, use_container_width=True)
            else:
                st.info("Não foi possível criar o gráfico de volume.")
        else:
            st.warning(
                "⚠️ O arquivo df_vol.parquet não contém as colunas "
                "'Período' e 'Volume' necessárias."
            )
    else:
        st.info(
            "ℹ️ Carregue o arquivo df_vol.parquet para visualizar "
            "o gráfico de volume."
        )
    
    # Gráfico de Volume por Veículo (dentro da aba Volume)
    if 'Volume' in df_visualizacao.columns and 'Veículo' in df_visualizacao.columns:
        st.subheader("📊 Volume por Veículo")
        grafico_volume_veiculo = create_volume_veiculo_chart(df_visualizacao)
        if grafico_volume_veiculo is not None:
            st.altair_chart(grafico_volume_veiculo, use_container_width=True)

# Gráfico 2: Soma do Valor por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_oficina_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None):
    """Cria gráfico de barras por Oficina com linha de Flex Bud opcional"""
    try:
        if (coluna not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

        # Se for CPU e tiver coluna Veículo, agrupar por Oficina e Veículo
        # IMPORTANTE: Sempre agrupar por Período+Ano primeiro, depois por Oficina+Veículo
        if (tipo_viz == "CPU (Custo por Unidade)" and
                'Veículo' in df_data.columns and
                'Total' in df_data.columns and 'Volume' in df_data.columns):
            # Verificar se há múltiplos anos
            tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
            
            if tem_multiplos_anos and 'Período' in df_data.columns:
                # Agrupar por Oficina, Veículo, Período E Ano, somar Total e Volume, calcular CPU
                df_agrupado_periodo = df_data.groupby(['Oficina', 'Veículo', 'Período', 'Ano']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Recalcular CPU por Período+Ano
                df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                # Agora agrupar por Oficina e Veículo, somar Total e Volume de todos os períodos
                chart_data = df_agrupado_periodo.groupby(['Oficina', 'Veículo']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
            elif 'Período' in df_data.columns:
                # Agrupar por Oficina, Veículo e Período, somar Total e Volume, calcular CPU
                df_agrupado_periodo = df_data.groupby(['Oficina', 'Veículo', 'Período']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Recalcular CPU por Período
                df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                # Agora agrupar por Oficina e Veículo, somar Total e Volume de todos os períodos
                chart_data = df_agrupado_periodo.groupby(['Oficina', 'Veículo']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
            else:
                # Se não tiver Período, agrupar apenas por Oficina e Veículo
                chart_data = df_data.groupby(['Oficina', 'Veículo']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
            
            # Recalcular CPU final (Total agregado / Volume agregado)
            chart_data[coluna] = chart_data.apply(
                lambda row: (
                    row['Total'] / row['Volume']
                    if pd.notnull(row['Volume']) and row['Volume'] != 0
                    else 0
                ),
                axis=1
            )
            chart_data = chart_data[['Oficina', 'Veículo', coluna]]
        elif (tipo_viz == "CPU (Custo por Unidade)" and
                'Veículo' in df_data.columns):
            chart_data = df_data.groupby(
                ['Oficina', 'Veículo'], as_index=False
            )[coluna].sum()

            # Ordenar por Oficina e depois por CPU decrescente
            chart_data = chart_data.sort_values(
                ['Oficina', coluna], ascending=[True, False]
            )

            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Oficina e Veículo"

            # Criar gráfico de barras agrupadas
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y', axis=alt.Axis(grid=False)),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    'Veículo:N',
                    title='Veículo',
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('Veículo:N', title='Veículo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=',.2f'
                    )
                ]
            ).properties(
                title=titulo_grafico,
                height=400
            )

            # Adicionar rótulos com valores nas barras
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
            # Gráfico normal sem separação por veículo
            # Para CPU, recalcular a partir de Total e Volume agregados
            if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                # Verificar se há múltiplos anos
                tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
                
                if tem_multiplos_anos and 'Período' in df_data.columns:
                    # Agrupar por Oficina, Período E Ano, somar Total e Volume, calcular CPU
                    df_agrupado_periodo = df_data.groupby(['Oficina', 'Período', 'Ano']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU por Período+Ano
                    df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Agora agrupar por Oficina, somar Total e Volume de todos os períodos
                    chart_data = df_agrupado_periodo.groupby('Oficina').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                elif 'Período' in df_data.columns:
                    # Agrupar por Oficina e Período, somar Total e Volume, calcular CPU
                    df_agrupado_periodo = df_data.groupby(['Oficina', 'Período']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU por Período
                    df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Agora agrupar por Oficina, somar Total e Volume de todos os períodos
                    chart_data = df_agrupado_periodo.groupby('Oficina').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                else:
                    # Se não tiver Período, agrupar apenas por Oficina
                    chart_data = df_data.groupby('Oficina').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                
                # Recalcular CPU final (Total agregado / Volume agregado)
                chart_data[coluna] = chart_data.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                chart_data = chart_data[['Oficina', coluna]]
            else:
                chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
            chart_data = chart_data.sort_values(coluna, ascending=False)
            
            # Determinar ordem das oficinas (usar a mesma ordem para barras e linha)
            ordem_oficinas_barras = chart_data['Oficina'].tolist()

            # Definir título do eixo Y baseado no tipo e moeda
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
            
            # Debug: verificar condições
            st.sidebar.info(f"🔍 Debug Oficina: df_budget is None: {df_budget is None}")
            if df_budget is not None:
                st.sidebar.info(f"🔍 Debug Oficina: 'Oficina' em df_budget: {'Oficina' in df_budget.columns}")
                st.sidebar.info(f"🔍 Debug Oficina: df_real_vol is None: {df_real_vol is None}")
                if df_real_para_flex is not None:
                    st.sidebar.info(f"🔍 Debug Oficina: 'Custo' em df_real_para_flex: {'Custo' in df_real_para_flex.columns}")
                    st.sidebar.info(f"🔍 Debug Oficina: Colunas df_real_para_flex: {list(df_real_para_flex.columns)[:15]}")
                else:
                    st.sidebar.warning("⚠️ df_real_para_flex é None")
            
            if df_budget is not None and 'Oficina' in df_budget.columns and df_real_vol is not None:
                # A função calcular_flex_budget precisa da coluna 'Custo'
                # Se não tiver, tentar usar df_real_original ou df_total que deve ter
                if 'Custo' not in df_real_para_flex.columns:
                    st.sidebar.warning("⚠️ Coluna 'Custo' não encontrada em df_real_para_flex. Tentando usar df_total...")
                    # Tentar usar df_total global que deve ter a coluna 'Custo'
                    if 'df_total' in globals() and 'Custo' in globals()['df_total'].columns:
                        df_real_para_flex = globals()['df_total'].copy()
                        st.sidebar.success("✅ Usando df_total com coluna 'Custo'")
                    else:
                        st.sidebar.error("❌ Não foi possível encontrar dados com coluna 'Custo'")
                        df_real_para_flex = None
                
                if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                    try:
                        # Calcular FLEX agrupado por Oficina (não por período)
                        # Primeiro calcular FLEX por período, depois agrupar por Oficina
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        flex_data = calcular_flex_budget(
                            df_real_para_flex,
                            df_real_vol,
                            df_budget,
                            df_budget_vol,
                            tipo_viz,
                            tem_ano
                        )
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud por Oficina (somar todos os períodos)
                            if 'Oficina' in flex_data.columns:
                                flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                                budget_data = flex_data.groupby('Oficina')[coluna].sum().reset_index()
                                
                                # Filtrar apenas oficinas que existem no chart_data
                                budget_data = budget_data[budget_data['Oficina'].isin(chart_data['Oficina'])].copy()
                                
                                if len(budget_data) > 0:
                                    # Criar linha tracejada de Flex Bud
                                    budget_data_legenda = budget_data.copy()
                                    budget_data_legenda['Tipo'] = 'Flex Bud'
                                    
                                    # Ordenar por valor para manter ordem consistente (usar mesma ordem das barras)
                                    budget_data_legenda = budget_data_legenda.sort_values(coluna, ascending=False)
                                    # Garantir que a ordem seja a mesma das barras
                                    ordem_oficinas = [o for o in ordem_oficinas_barras if o in budget_data_legenda['Oficina'].tolist()]
                                    
                                    # Criar linha tracejada de Flex Bud (igual ao gráfico por Período)
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
                                            alt.Tooltip('Oficina:N', title='Oficina'),
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
                                    
                                    # Adicionar rótulos
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
                                    
                                    # Combinar linha, pontos e rótulos
                                    linha_budget = linha_budget + pontos_budget + rotulos_budget
                    except Exception as e:
                        # Log do erro para debug
                        import traceback
                        st.sidebar.warning(f"⚠️ Erro ao calcular Flex Bud para gráfico de Oficina: {str(e)}")
                        st.sidebar.code(traceback.format_exc())
                else:
                    # Debug: verificar por que não tem coluna Custo
                    if df_real_para_flex is not None:
                        st.sidebar.info(f"ℹ️ Colunas disponíveis em df_real_para_flex: {list(df_real_para_flex.columns)[:10]}")
                        st.sidebar.info(f"ℹ️ 'Custo' presente: {'Custo' in df_real_para_flex.columns}")
                    else:
                        st.sidebar.warning("⚠️ df_real_para_flex é None")
            else:
                # Debug: verificar por que não tem dados de budget
                st.sidebar.info(f"ℹ️ df_budget é None: {df_budget is None}")
                st.sidebar.info(f"ℹ️ 'Oficina' em df_budget: {df_budget is not None and 'Oficina' in df_budget.columns if df_budget is not None else False}")
                st.sidebar.info(f"ℹ️ df_real_vol é None: {df_real_vol is None}")

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y', axis=alt.Axis(grid=False)),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    f'{coluna}:Q',
                    title=coluna,
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)"
                        else ',.2f'
                    )
                ]
            ).properties(
                title=titulo_grafico,
                height=400
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
            )

            # Criar gráfico de delta (Real - Flex Bud) se linha_budget estiver disponível
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
                    # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                    delta_min_abs = abs(delta_data['Delta'].min())
                    delta_max_abs = abs(delta_data['Delta'].max())
                    delta_abs_max = max(delta_min_abs, delta_max_abs)
                    
                    # Criar domínio simétrico baseado no maior valor absoluto
                    # Isso garante que zero sempre fique no centro, independente dos filtros
                    delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                    delta_max = delta_abs_max if delta_abs_max > 0 else 1
                    
                    # Ordenar por valor para manter ordem consistente
                    delta_data = delta_data.sort_values(coluna, ascending=False)
                    ordem_oficinas_delta = delta_data['Oficina'].tolist()
                    
                    # Criar gráfico de barras para delta
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
                        height=60
                    )
                    
                    # Adicionar rótulos de dados no gráfico de delta
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
                        color=alt.value('#FF0000')
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
                        color=alt.value('#00AA00')
                    )
                    
                    grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
                except Exception as e:
                    pass  # Silenciar erro, apenas não mostrar delta
            
            # Combinar gráfico de barras com linha de budget se disponível
            if linha_budget is not None:
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
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Gráfico 4: Total/CPU por Veículo
@st.cache_data(ttl=900, max_entries=2)
def create_total_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None):
    """Cria gráfico de barras de Total/CPU por Veículo com linha de Flex Bud opcional"""
    try:
        if coluna not in df_data.columns:
            return None

        # Definir título e formato baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            formato = ',.2f'
            if 'Veículo' in df_data.columns:
                titulo_grafico = "CPU por Veículo"
            else:
                titulo_grafico = "CPU por Período"
        else:
            titulo_y = f"Total ({moeda_simbolo})"
            formato = ',.2f'
            if 'Veículo' in df_data.columns:
                titulo_grafico = "Total por Veículo"
            else:
                titulo_grafico = "Total por Período"

        # Verificar se tem coluna Veículo
        if 'Veículo' in df_data.columns:
            # Para CPU, recalcular a partir de Total e Volume agregados
            # IMPORTANTE: Sempre agrupar por Período+Ano primeiro, depois por Veículo
            if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                # Verificar se há múltiplos anos e agrupar corretamente
                tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
                
                if tem_multiplos_anos:
                    # Agrupar por Veículo, Período E Ano, somar Total e Volume, calcular CPU
                    # Depois agrupar por Veículo, somar Total e Volume, e recalcular CPU final
                    df_agrupado_periodo = df_data.groupby(['Veículo', 'Período', 'Ano']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU por Período+Ano
                    df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                    chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                else:
                    # Agrupar por Veículo e Período, somar Total e Volume, calcular CPU
                    # Depois agrupar por Veículo, somar Total e Volume, e recalcular CPU final
                    if 'Período' in df_data.columns:
                        df_agrupado_periodo = df_data.groupby(['Veículo', 'Período']).agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        # Recalcular CPU por Período
                        df_agrupado_periodo['CPU_temp'] = df_agrupado_periodo.apply(
                            lambda row: (
                                row['Total'] / row['Volume']
                                if pd.notnull(row['Volume']) and row['Volume'] != 0
                                else 0
                            ),
                            axis=1
                        )
                        # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                        chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        # Se não tiver Período, agrupar apenas por Veículo
                        chart_data = df_data.groupby('Veículo').agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                
                # Recalcular CPU final (Total agregado / Volume agregado)
                chart_data[coluna] = chart_data.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                chart_data = chart_data[['Veículo', coluna]]
            else:
                chart_data = (
                    df_data.groupby('Veículo')[coluna].sum().reset_index()
                )
            chart_data = chart_data.sort_values(coluna, ascending=False)

            # Processar dados de budget e calcular FLEX se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'Veículo' in df_budget.columns and df_real_vol is not None:
                if 'Custo' in df_real_para_flex.columns:
                    try:
                        # Calcular FLEX agrupado por Veículo (não por período)
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        flex_data = calcular_flex_budget(
                            df_real_para_flex,
                            df_real_vol,
                            df_budget,
                            df_budget_vol,
                            tipo_viz,
                            tem_ano
                        )
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud por Veículo (somar todos os períodos)
                            if 'Veículo' in flex_data.columns:
                                flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                                budget_data = flex_data.groupby('Veículo')[coluna].sum().reset_index()
                                
                                # Filtrar apenas veículos que existem no chart_data
                                budget_data = budget_data[budget_data['Veículo'].isin(chart_data['Veículo'])].copy()
                                
                                if len(budget_data) > 0:
                                    # Criar linha tracejada de Flex Bud
                                    budget_data_legenda = budget_data.copy()
                                    budget_data_legenda['Tipo'] = 'Flex Bud'
                                    
                                    # Ordenar por valor para manter ordem consistente
                                    budget_data_legenda = budget_data_legenda.sort_values(coluna, ascending=False)
                                    ordem_veiculos = budget_data_legenda['Veículo'].tolist()
                                    
                                    linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                        strokeDash=[10, 5],
                                        strokeWidth=1.5,
                                        opacity=0.8,
                                        color='#FF6B35'
                                    ).encode(
                                        x=alt.X(
                                            'Veículo:N',
                                            title='Veículo',
                                            sort=ordem_veiculos,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        y=alt.Y(
                                            f'{coluna}:Q',
                                            title=titulo_y,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        tooltip=[
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip(
                                                f'{coluna}:Q',
                                                title='Flex Bud',
                                                format=formato
                                            )
                                        ]
                                    )
                                    
                                    # Adicionar pontos na linha
                                    pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                        size=80,
                                        opacity=0.9,
                                        color='#FF6B35'
                                    ).encode(
                                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                                        y=alt.Y(f'{coluna}:Q'),
                                        tooltip=[
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip(
                                                f'{coluna}:Q',
                                                title='Flex Bud',
                                                format=formato
                                            )
                                        ]
                                    )
                                    
                                    # Adicionar rótulos
                                    rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                        align='center',
                                        baseline='bottom',
                                        dy=-15,
                                        color='#FF6B35',
                                        fontSize=9,
                                        fontWeight='bold'
                                    ).encode(
                                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                                        y=alt.Y(f'{coluna}:Q'),
                                        text=alt.Text(f'{coluna}:Q', format=formato)
                                    )
                                    
                                    linha_budget = linha_budget + pontos_budget + rotulos_budget
                    except Exception as e:
                        pass  # Silenciar erro, apenas não mostrar Flex Bud

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    'Veículo:N',
                    title='Veículo',
                    sort='-y',
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                color=alt.Color(
                    f'{coluna}:Q',
                    title=coluna,
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip('Veículo:N', title='Veículo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato
                    )
                ]
            ).properties(
                title=titulo_grafico,
                height=400
            )
        else:
            # Se não tiver Veículo, usar Período como fallback
            if 'Período' not in df_data.columns:
                return None
            
            # Verificar se há múltiplos anos
            tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
            
            if tem_multiplos_anos:
                # Agrupar por Ano e Período
                # Para CPU, usar EXATAMENTE a mesma lógica da tabela (que está correta)
                # IMPORTANTE: A tabela funciona porque agrupa df_visualizacao por Ano e Período, soma Total e Volume, e calcula CPU
                # O gráfico deve fazer EXATAMENTE o mesmo
                if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA (linha 1577-1589): Agrupar por Ano e Período, somar Total e Volume, calcular CPU
                    # Isso garante que valores sejam calculados corretamente, não somando CPUs já calculados
                    chart_data = df_data.groupby(['Ano', 'Período']).agg({
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
                    chart_data = chart_data[['Ano', 'Período', coluna]]
                else:
                    chart_data = df_data.groupby(['Ano', 'Período'])[coluna].sum().reset_index()
                
                # Criar coluna combinada para o rótulo do gráfico
                chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                
                # Ordenar por ano e mês
                chart_data = ordenar_por_mes(chart_data, 'Período')
                ordem_periodos = chart_data['Período_Completo'].tolist()
                
                # Usar Período_Completo no gráfico
                coluna_periodo_grafico = 'Período_Completo'
            else:
                # Comportamento original: agrupar apenas por Período
                # Para CPU, recalcular a partir de Total e Volume agregados
                if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                    chart_data = df_data.groupby('Período').agg({
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
                    chart_data = chart_data[['Período', coluna]]
                else:
                    chart_data = df_data.groupby('Período')[coluna].sum().reset_index()
                chart_data = ordenar_por_mes(chart_data, 'Período')
                ordem_periodos = chart_data['Período'].tolist()
                coluna_periodo_grafico = 'Período'

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
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato
                    )
                ]
            ).properties(
                title=titulo_grafico,
                height=400
            )

        # Adicionar rótulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato)
        )

        # Criar gráfico de delta (Real - Flex Bud) se linha_budget estiver disponível
        grafico_delta = None
        if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Real - Flex Bud
                delta_data = chart_data.copy()
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                
                delta_data = delta_data.merge(
                    budget_data_merge[['Veículo', f'{coluna}_FlexBud']],
                    on='Veículo',
                    how='left'
                )
                
                # Calcular delta: Real - Flex Bud
                delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data[f'{coluna}_FlexBud'].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domínio simétrico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Ordenar por valor para manter ordem consistente
                delta_data = delta_data.sort_values(coluna, ascending=False)
                ordem_veiculos_delta = delta_data['Veículo'].tolist()
                
                # Criar gráfico de barras para delta
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20
                ).encode(
                    x=alt.X(
                        'Veículo:N',
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
                        alt.Tooltip('Veículo:N', title='Veículo'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                        alt.Tooltip(f'{coluna}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna}_FlexBud:Q', title='Flex Bud', format=',.2f')
                    ]
                ).properties(
                    height=60
                )
                
                # Adicionar rótulos de dados no gráfico de delta
                rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.value('#FF0000')
                )
                
                rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                    align='center',
                    baseline='top',
                    dy=12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.value('#00AA00')
                )
                
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                pass  # Silenciar erro, apenas não mostrar delta
        
        # Combinar gráfico de barras com linha de budget se disponível
        if linha_budget is not None:
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
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# ==========================================
# TAB 3: TC Ext por Veíc
# ==========================================
with tab3:
    # Carregar dados de budget e volume para esta aba também
    df_budget_filtrado_tab3 = None
    df_budget_vol_filtrado_tab3 = None
    df_volume_real_filtrado_tab3 = None
    df_real_original_grafico_tab3 = None
    
    try:
        # Carregar dados de budget
        df_budget_tab3 = load_budget_data(ano_selecionado)
        df_budget_vol_tab3 = load_budget_volume_data(ano_selecionado)
        
        if df_budget_tab3 is not None:
            # Aplicar fator de conversão na coluna Total do budget
            if fator_conversao and fator_conversao != "Nenhum" and 'Total' in df_budget_tab3.columns:
                if fator_conversao == "K (milhares)":
                    df_budget_tab3['Total'] = df_budget_tab3['Total'] / 1000
                elif fator_conversao == "M (Milhões)":
                    df_budget_tab3['Total'] = df_budget_tab3['Total'] / 1000000
            
            # Aplicar conversão de moeda
            if moeda_codigo != "BRL" and 'Total' in df_budget_tab3.columns:
                df_budget_tab3 = converter_coluna_moeda(df_budget_tab3, 'Total', moeda_codigo, taxas_cambio)
            
            df_budget_filtrado_tab3 = df_budget_tab3.copy()
        
        if df_budget_vol_tab3 is not None:
            df_budget_vol_filtrado_tab3 = df_budget_vol_tab3.copy()
    except Exception as e:
        pass  # Silenciar erro
    
    # Carregar dados de volume reais
    try:
        df_vol_calc_grafico_tab3 = load_volume_data(ano_selecionado)
        if df_vol_calc_grafico_tab3 is not None and 'Volume' in df_vol_calc_grafico_tab3.columns:
            df_volume_real_filtrado_tab3 = df_vol_calc_grafico_tab3.copy()
    except Exception as e:
        pass  # Silenciar erro
    
    # Preparar dados originais para cálculo de FLEX (modo CPU)
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        df_real_original_grafico_tab3 = df_total.copy()
        # Aplicar mesmos filtros básicos se necessário
    else:
        df_real_original_grafico_tab3 = df_total.copy()
    
    # Usar df_visualizacao (já tem os dados calculados com filtros da sidebar)
    # Verificar se tem as colunas necessárias
    tem_veiculo = 'Veículo' in df_visualizacao.columns
    tem_oficina = 'Oficina' in df_visualizacao.columns
    tem_periodo = 'Período' in df_visualizacao.columns
    
    # Preparar dados e determinar colunas de períodos (usar mesma lógica para ambas tabelas)
    if tem_veiculo and tem_periodo and coluna_visualizacao in df_visualizacao.columns:
        # Verificar se há múltiplos anos e criar coluna combinada se necessário
        tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
        
        # Preparar DataFrame para pivot
        if tem_multiplos_anos:
            df_visualizacao_pivot = df_visualizacao.copy()
            df_visualizacao_pivot['Período_Ano'] = (
                df_visualizacao_pivot['Período'].astype(str) + ' ' + 
                df_visualizacao_pivot['Ano'].astype(str)
            )
            coluna_periodo_pivot = 'Período_Ano'
        else:
            df_visualizacao_pivot = df_visualizacao.copy()
            coluna_periodo_pivot = 'Período'
        
        # Criar tabela pivot temporária para determinar as colunas de períodos
        # Usar a tabela por oficina como referência para garantir mesmas colunas
        if tem_oficina:
            df_tabela_ref = df_visualizacao_pivot.pivot_table(
                index=['Oficina', 'Veículo'],
                columns=coluna_periodo_pivot,
                values=coluna_visualizacao,
                aggfunc='sum',
                fill_value=0
            )
        else:
            df_tabela_ref = df_visualizacao_pivot.pivot_table(
                index='Veículo',
                columns=coluna_periodo_pivot,
                values=coluna_visualizacao,
                aggfunc='sum',
                fill_value=0
            )
        
        # Ordenar colunas de períodos (mesma lógica para ambas tabelas)
        if tem_multiplos_anos:
            colunas_ordenadas = []
            anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
            
            for ano in anos_unicos:
                for mes in ORDEM_MESES:
                    coluna_combinada = f"{mes} {ano}"
                    if coluna_combinada in df_tabela_ref.columns:
                        colunas_ordenadas.append(coluna_combinada)
            
            colunas_restantes = [
                col for col in df_tabela_ref.columns 
                if col not in colunas_ordenadas
            ]
            colunas_periodos = colunas_ordenadas + colunas_restantes
        else:
            colunas_existentes = [
                col for col in ORDEM_MESES if col in df_tabela_ref.columns
            ]
            colunas_restantes = [
                col for col in df_tabela_ref.columns if col not in ORDEM_MESES
            ]
            colunas_periodos = colunas_existentes + colunas_restantes
        
        # Reordenar colunas na tabela de referência
        df_tabela_ref = df_tabela_ref[colunas_periodos]
        
        # Identificar colunas adicionais para incluir (todas exceto Ano, Período e colunas já usadas)
        # Usar df_visualizacao original para ter todas as colunas disponíveis
        colunas_excluidas = {
            'Ano', 'Período', 'Período_Ano', 'Veículo', 'Oficina', 
            'Total', 'Valor', 'CPU', 'Volume', coluna_visualizacao,
            'Dt.lçto.', 'Data Lançamento', 'Data de Lançamento',
            'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
        }
        # Pegar colunas do DataFrame original (df_visualizacao) que não estão excluídas
        # Manter a ordem original das colunas do DataFrame
        colunas_adicionais = [
            col for col in df_visualizacao.columns 
            if col not in colunas_excluidas
        ]
        
        # Debug: mostrar colunas adicionais encontradas (comentado para produção)
        # st.write(f"Colunas adicionais encontradas: {colunas_adicionais}")
    
    # Exibir gráfico por Oficina
    if ('Oficina' in df_visualizacao.columns and
            coluna_visualizacao in df_visualizacao.columns):
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("📊 CPU por Oficina")
        else:
            st.subheader("📊 Soma do Valor por Oficina")
        grafico_oficina = create_oficina_chart(
            df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo,
            df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
        )
        if grafico_oficina:
            st.altair_chart(grafico_oficina, use_container_width=True)
    
    # Exibir gráfico de Total/CPU por Veículo
    if 'Veículo' in df_visualizacao.columns:
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if coluna_visualizacao in df_visualizacao.columns:
                st.subheader("📊 CPU por Veículo")
                grafico_total = create_total_chart(
                    df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo,
                    df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
                )
                if grafico_total:
                    st.altair_chart(grafico_total, use_container_width=True)
        elif tipo_visualizacao == "Custo Total":
            if 'Total' in df_filtrado.columns:
                st.subheader("📊 Total por Veículo")
                grafico_total = create_total_chart(
                    df_filtrado, 'Total', tipo_visualizacao, moeda_simbolo,
                    df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
                )
                if grafico_total:
                    st.altair_chart(grafico_total, use_container_width=True)
    elif 'Período' in df_visualizacao.columns:
        # Fallback para Período se não tiver Veículo
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if coluna_visualizacao in df_visualizacao.columns:
                st.subheader("📊 CPU por Período")
                grafico_total = create_total_chart(
                    df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo,
                    df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
                )
                if grafico_total:
                    st.altair_chart(grafico_total, use_container_width=True)
        elif tipo_visualizacao == "Custo Total":
            if 'Total' in df_filtrado.columns:
                st.subheader("📊 Total por Período")
                grafico_total = create_total_chart(
                    df_filtrado, 'Total', tipo_visualizacao, moeda_simbolo,
                    df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
                )
                if grafico_total:
                    st.altair_chart(grafico_total, use_container_width=True)

# ==========================================
# TAB 4: Detalhe Real
# ==========================================
with tab4:
    # Bloco de Tabelas: Veículo, Oficina e Períodos + Total por Veículo
    st.markdown("---")

    # Expander para mostrar/ocultar todo o bloco de tabelas
    with st.expander("📊 **Tabelas Detalhadas**", expanded=False):
        # Tabela: Veículo, Oficina e Períodos (seguindo filtros da sidebar)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("📋 Tabela - CPU por Veículo, Oficina e Período")
        else:
            st.subheader("📋 Tabela - Custo Total por Veículo, Oficina e Período")
            
        if tem_veiculo and tem_oficina and tem_periodo:
            # Usar coluna_visualizacao que já está definida
            if coluna_visualizacao in df_visualizacao.columns:
                # As variáveis colunas_periodos, coluna_periodo_pivot e colunas_adicionais
                # já foram definidas no bloco anterior (tabela de total). Se não foram, criar agora.
                try:
                    # Tentar usar as variáveis já definidas
                    _ = colunas_periodos
                    _ = coluna_periodo_pivot
                    _ = df_visualizacao_pivot
                    _ = colunas_adicionais
                except NameError:
                    # Se não existirem, criar agora (mesma lógica)
                    tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                    
                if tem_multiplos_anos:
                    df_visualizacao_pivot = df_visualizacao.copy()
                    df_visualizacao_pivot['Período_Ano'] = (
                        df_visualizacao_pivot['Período'].astype(str) + ' ' + 
                        df_visualizacao_pivot['Ano'].astype(str)
                    )
                    coluna_periodo_pivot = 'Período_Ano'
                else:
                    df_visualizacao_pivot = df_visualizacao.copy()
                    coluna_periodo_pivot = 'Período'
                    
                df_tabela_ref = df_visualizacao_pivot.pivot_table(
                    index=['Oficina', 'Veículo'],
                    columns=coluna_periodo_pivot,
                    values=coluna_visualizacao,
                    aggfunc='sum',
                    fill_value=0
                )
                    
                if tem_multiplos_anos:
                    colunas_ordenadas = []
                    anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
                        
                    for ano in anos_unicos:
                        for mes in ORDEM_MESES:
                            coluna_combinada = f"{mes} {ano}"
                            if coluna_combinada in df_tabela_ref.columns:
                                colunas_ordenadas.append(coluna_combinada)
                        
                    colunas_restantes = [
                        col for col in df_tabela_ref.columns 
                        if col not in colunas_ordenadas
                    ]
                    colunas_periodos = colunas_ordenadas + colunas_restantes
                else:
                    colunas_existentes = [
                        col for col in ORDEM_MESES if col in df_tabela_ref.columns
                    ]
                    colunas_restantes = [
                        col for col in df_tabela_ref.columns if col not in ORDEM_MESES
                    ]
                    colunas_periodos = colunas_existentes + colunas_restantes
                    
                # Definir colunas_adicionais também
                colunas_excluidas = {
                    'Ano', 'Período', 'Período_Ano', 'Veículo', 'Oficina', 
                    'Total', 'Valor', 'CPU', 'Volume', coluna_visualizacao,
                    'Dt.lçto.', 'Data Lançamento', 'Data de Lançamento',
                    'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
                }
                # Manter a ordem original das colunas do DataFrame
                colunas_adicionais = [
                    col for col in df_visualizacao.columns 
                    if col not in colunas_excluidas
                ]
                
            # Usar as mesmas colunas de períodos já determinadas
            # Para CPU, recalcular a partir de Total e Volume agregados
            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao_pivot.columns and 'Volume' in df_visualizacao_pivot.columns:
                # Agrupar por Oficina, Veículo e Período, somar Total e Volume
                df_agrupado = df_visualizacao_pivot.groupby(['Oficina', 'Veículo', coluna_periodo_pivot]).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                    
                # Recalcular CPU - VETORIZADO
                df_agrupado['CPU'] = np.where(
                    (df_agrupado['Volume'].notna()) & (df_agrupado['Volume'] != 0),
                    df_agrupado['Total'] / df_agrupado['Volume'],
                    0
                )
                    
                # Criar tabela pivot com CPU recalculado
                df_tabela = df_agrupado.pivot_table(
                    index=['Oficina', 'Veículo'],
                    columns=coluna_periodo_pivot,
                    values='CPU',
                    aggfunc='first',
                    fill_value=0
                )
            else:
                # Para Custo Total, usar soma normalmente
                df_tabela = df_visualizacao_pivot.pivot_table(
                    index=['Oficina', 'Veículo'],
                    columns=coluna_periodo_pivot,
                    values=coluna_visualizacao,
                    aggfunc='sum',
                    fill_value=0
                )
            
            # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
            for col in colunas_periodos:
                if col not in df_tabela.columns:
                    df_tabela[col] = 0
                
            # Reordenar para usar exatamente as mesmas colunas
            df_tabela = df_tabela[colunas_periodos]
                
            # Calcular total por linha
            # Para CPU, recalcular a partir de Total e Volume agregados por Oficina e Veículo
            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao_pivot.columns and 'Volume' in df_visualizacao_pivot.columns:
                # Agrupar por Oficina e Veículo, somar Total e Volume, e recalcular CPU
                df_total_oficina_veiculo = df_visualizacao_pivot.groupby(['Oficina', 'Veículo']).agg({
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
                    df_total_oficina_veiculo[['Oficina', 'Veículo', 'CPU']],
                    on=['Oficina', 'Veículo'],
                    how='left'
                )
                df_tabela.rename(columns={'CPU': 'Total'}, inplace=True)
                df_tabela = df_tabela.set_index(['Oficina', 'Veículo'])
            else:
                df_tabela['Total'] = df_tabela.sum(axis=1)
            df_tabela = df_tabela.sort_values(['Oficina', 'Veículo'])
                
            # Resetar índice para ter Oficina e Veículo como colunas (Oficina primeiro)
            df_tabela = df_tabela.reset_index()
            
        # Adicionar colunas adicionais fazendo merge com o primeiro valor não nulo por Oficina e Veículo
        if colunas_adicionais:
            # Filtrar apenas colunas que realmente existem no DataFrame
            colunas_adicionais_validas = [
                col for col in colunas_adicionais 
                if col in df_visualizacao.columns
            ]
                
            if colunas_adicionais_validas:
                # Agrupar por Oficina e Veículo e pegar o primeiro valor não nulo de cada coluna adicional
                # Usar df_visualizacao original para ter todas as colunas
                df_colunas_adicionais = df_visualizacao.groupby(['Oficina', 'Veículo'])[colunas_adicionais_validas].first().reset_index()
                # Fazer merge com a tabela
                df_tabela = pd.merge(
                    df_tabela,
                    df_colunas_adicionais,
                    on=['Oficina', 'Veículo'],
                    how='left'
                )
                # Reordenar colunas: Oficina, Veículo, colunas adicionais (na ordem original), períodos, Total
                # Manter a ordem original das colunas adicionais
                colunas_adicionais_ordenadas = [
                    col for col in colunas_adicionais 
                    if col in colunas_adicionais_validas
                ]
                colunas_finais = ['Oficina', 'Veículo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Total']
                # Manter apenas colunas que existem
                colunas_finais = [col for col in colunas_finais if col in df_tabela.columns]
                df_tabela = df_tabela[colunas_finais]
        else:
            # Reordenar colunas para garantir que Oficina venha antes de Veículo
            colunas_ordenadas = ['Oficina', 'Veículo'] + [col for col in df_tabela.columns 
                                                          if col not in ['Oficina', 'Veículo']]
            df_tabela = df_tabela[colunas_ordenadas]
            
        # Formatar valores baseado no tipo de visualização - OTIMIZADO
        # Aplicar formatação apenas nas colunas numéricas (exceto Veículo, Oficina e colunas adicionais)
        df_tabela_formatado = df_tabela.copy()
        # Obter colunas adicionais que foram realmente adicionadas à tabela
        colunas_adicionais_na_tabela = [
            col for col in df_tabela_formatado.columns 
            if col not in ['Oficina', 'Veículo'] + colunas_periodos + ['Total']
        ]
        colunas_formatar = [
            col for col in df_tabela_formatado.columns 
            if col not in ['Veículo', 'Oficina'] + colunas_adicionais_na_tabela and
            df_tabela_formatado[col].dtype in ['float64', 'float32', 'int64', 'int32']
        ]
        # Formatação vetorizada
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            for col in colunas_formatar:
                df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
        else:
            # Adicionar sufixo baseado no fator de conversão
            sufixo = ""
            if fator_conversao:
                if fator_conversao == "K (milhares)":
                    sufixo = " K"
                elif fator_conversao == "M (Milhões)":
                    sufixo = " M"
            for col in colunas_formatar:
                df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}{sufixo}" if isinstance(x, (int, float)) else x)
            
        # Função para formatar valores (definida antes de ser usada)
        def formatar_valor(val, tipo):
            if isinstance(val, (int, float)):
                # NOTA: Os dados já estão convertidos na base, então apenas formatamos
                simbolo = obter_simbolo_moeda(moeda_codigo)
                if tipo == "CPU (Custo por Unidade)":
                    return f"{val:,.2f}"
                else:
                    # Adicionar sufixo baseado no fator de conversão (apenas para Custo Total)
                    sufixo = ""
                    if tipo_visualizacao == "Custo Total" and fator_conversao:
                        if fator_conversao == "K (milhares)":
                            sufixo = " K"
                        elif fator_conversao == "M (Milhões)":
                            sufixo = " M"
                    return f"{simbolo} {val:,.2f}{sufixo}"
            return val
        
        # Agrupar por Oficina e criar expanders (abertos por padrão)
        oficinas = df_tabela_formatado['Oficina'].unique()
            
        for oficina in sorted(oficinas):
            # Filtrar dados da oficina
            df_oficina = df_tabela_formatado[df_tabela_formatado['Oficina'] == oficina].copy()
                
            # Calcular total da oficina
            if 'Total' in df_oficina.columns:
                # Converter Total de string formatada para número para calcular
                df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                total_oficina = df_oficina_numerico['Total'].sum()
                total_formatado = formatar_valor(total_oficina, tipo_visualizacao)
            else:
                total_formatado = "N/A"
                
            # Criar container para cada oficina (substituindo expander para evitar aninhamento)
            st.markdown("---")
            with st.container():
                st.markdown(f"### 🏭 **{oficina}** - Total: {total_formatado} ({len(df_oficina)} veículo{'s' if len(df_oficina) > 1 else ''})")
                # Remover coluna Oficina da tabela (já está no título)
                df_oficina_display = df_oficina.drop(columns=['Oficina'])
                    
                # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
                colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                for col in colunas_para_remover:
                    if col in df_oficina_display.columns:
                        df_oficina_display = df_oficina_display.drop(columns=[col])
                    
                # Calcular totais por coluna (meses) usando dados numéricos
                df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])
                    
                # Criar linha de total
                linha_total = {'Veículo': '**TOTAL**'}
                    
                # Obter colunas adicionais que foram realmente adicionadas à tabela
                colunas_adicionais_na_tabela = [
                    col for col in df_oficina_numerico.columns 
                    if col not in ['Veículo'] + colunas_periodos + ['Total']
                ]
                    
                # Adicionar valores vazios para colunas adicionais na linha de total
                for col in colunas_adicionais_na_tabela:
                    if col in df_oficina_numerico.columns:
                        linha_total[col] = ''
                    
                # Adicionar totais por coluna (meses e Total)
                for col in df_oficina_numerico.columns:
                    if col not in ['Veículo'] + colunas_adicionais_na_tabela:
                        if col in colunas_periodos:
                            # Para colunas de período, se for CPU, calcular Total/Volume do período
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                                # Filtrar dados da oficina e do período específico
                                df_oficina_filtrado = df_visualizacao[df_visualizacao['Oficina'] == oficina].copy()
                                    
                                # Verificar se há múltiplos anos
                                tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                                    
                                if tem_multiplos_anos:
                                    # Filtrar pelo período específico (formato: "mês ano")
                                    df_temp = df_oficina_filtrado.copy()
                                    df_temp['Período_Ano_temp'] = df_temp['Período'].astype(str) + ' ' + df_temp['Ano'].astype(str)
                                    df_periodo_filtrado = df_temp[df_temp['Período_Ano_temp'] == col].copy()
                                else:
                                    # Filtrar apenas por Período
                                    df_periodo_filtrado = df_oficina_filtrado[df_oficina_filtrado['Período'] == col].copy()
                                    
                                if len(df_periodo_filtrado) > 0:
                                    # Agrupar e calcular Total e Volume do período
                                    total_periodo = df_periodo_filtrado['Total'].sum()
                                    volume_periodo = df_periodo_filtrado['Volume'].sum()
                                    if pd.notnull(volume_periodo) and volume_periodo != 0:
                                        cpu_periodo = total_periodo / volume_periodo
                                    else:
                                        cpu_periodo = 0
                                    linha_total[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                else:
                                    linha_total[col] = formatar_valor(0, tipo_visualizacao)
                            else:
                                # Para Custo Total, somar normalmente
                                if df_oficina_numerico[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                                    total_col = df_oficina_numerico[col].sum()
                                    linha_total[col] = formatar_valor(total_col, tipo_visualizacao)
                        elif col == 'Total':
                            # Para a coluna Total, se for CPU, calcular Total/Volume geral da oficina
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                                # Filtrar dados da oficina
                                df_oficina_filtrado = df_visualizacao[df_visualizacao['Oficina'] == oficina].copy()
                                total_geral = df_oficina_filtrado['Total'].sum()
                                volume_geral = df_oficina_filtrado['Volume'].sum()
                                if pd.notnull(volume_geral) and volume_geral != 0:
                                    cpu_geral = total_geral / volume_geral
                                else:
                                    cpu_geral = 0
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
                    
                st.dataframe(df_oficina_display, use_container_width=True)
        
        # Botão de download da tabela (fora do loop de oficinas)
        if st.button(
            "📥 Baixar Tabela por Veículo e Oficina (Excel)",
            use_container_width=True,
            key="download_tabela_veiculo_oficina"
        ):
            with st.spinner("Gerando arquivo da tabela..."):
                try:
                    # Criar DataFrame completo para download (com todas as oficinas e totais)
                    df_download_list = []
                        
                    for oficina in sorted(oficinas):
                        # Dados da oficina (sem formatação para manter valores numéricos)
                        df_oficina_download = df_tabela[df_tabela['Oficina'] == oficina].copy()
                            
                        # Adicionar linha de total da oficina
                        linha_total_download = {'Oficina': oficina, 'Veículo': 'TOTAL'}
                        df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                        df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])
                            
                        for col in df_oficina_numerico.columns:
                            if col != 'Veículo':
                                total_col = df_oficina_numerico[col].sum()
                                linha_total_download[col] = total_col
                            
                        # Adicionar dados da oficina
                        df_download_list.append(df_oficina_download)
                        # Adicionar linha de total
                        df_download_list.append(pd.DataFrame([linha_total_download]))
                        
                    # Concatenar todos os DataFrames
                    df_download = pd.concat(df_download_list, ignore_index=True)
                        
                    # Obter pasta Downloads do usuário
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
                        f"✅ Arquivo salvo com sucesso em: {file_path}"
                    )
                    st.info(
                        f"📁 Verifique sua pasta Downloads: {downloads_path}"
                    )
                except Exception as e:
                    st.error(f"❌ Erro ao salvar arquivo: {str(e)}")
        else:
            colunas_faltando = []
            if not tem_veiculo:
                colunas_faltando.append("Veículo")
            if not tem_oficina:
                colunas_faltando.append("Oficina")
            if not tem_periodo:
                colunas_faltando.append("Período")
            st.info(f"ℹ️ Colunas necessárias não encontradas para criar a tabela: {', '.join(colunas_faltando)}")
    
    # Tabela: Total por Veículo e Períodos (sem Oficina) - no final do bloco
    st.markdown("---")
        
    # Determinar título do expander
    # ATUALIZADO: Usando mesma lógica do gráfico para linha de total geral
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        titulo_expander_total = "📋 **Tabela - CPU Total por Veículo e Período**"
    else:
        titulo_expander_total = "📋 **Tabela - Custo Total por Veículo e Período**"
        
    # Usar expander no mesmo formato do expander de "Tabelas Detalhadas"
    with st.expander(titulo_expander_total, expanded=False):
        if tem_veiculo and tem_periodo:
            # Inicializar variáveis para CPU
            df_tabela_total_valores = None
            df_tabela_total_volumes = None
                
            # Para CPU, usar a mesma lógica do gráfico: agrupar diretamente por Veículo e Período+Ano
            # Isso garante que apenas períodos com dados sejam considerados (evita problemas com volumes sem custos)
            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                # Verificar se há múltiplos anos
                tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                    
                # Agrupar por Veículo e Período+Ano, somar Total e Volume, calcular CPU
                # Usar a mesma coluna_periodo_pivot que foi determinada anteriormente
                if tem_multiplos_anos:
                    # Agrupar por Veículo, Período e Ano
                    df_agrupado_periodo = df_visualizacao.groupby(['Veículo', 'Período', 'Ano']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Criar coluna Período_Ano para fazer o pivot (usar o mesmo formato)
                    df_agrupado_periodo[coluna_periodo_pivot] = (
                        df_agrupado_periodo['Período'].astype(str) + ' ' + 
                        df_agrupado_periodo['Ano'].astype(str)
                    )
                else:
                    # Agrupar por Veículo e Período
                    df_agrupado_periodo = df_visualizacao.groupby(['Veículo', 'Período']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    
                # Calcular CPU por período (mesma lógica do gráfico)
                df_agrupado_periodo['CPU'] = df_agrupado_periodo.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                    
                # Criar tabelas pivot de Total e Volume apenas com dados existentes
                # Usar coluna_periodo_pivot que já foi determinada
                df_tabela_total_valores = df_agrupado_periodo.pivot_table(
                    index='Veículo',
                    columns=coluna_periodo_pivot,
                    values='Total',
                    aggfunc='sum',
                    fill_value=0
                )
                    
                df_tabela_total_volumes = df_agrupado_periodo.pivot_table(
                    index='Veículo',
                    columns=coluna_periodo_pivot,
                    values='Volume',
                    aggfunc='sum',
                    fill_value=0
                )
                    
                # Dividir Total / Volume para obter CPU
                df_tabela_total = df_tabela_total_valores / df_tabela_total_volumes.replace(0, np.nan)
                df_tabela_total = df_tabela_total.fillna(0)
                    
                # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                for col in colunas_periodos:
                    if col not in df_tabela_total.columns:
                        df_tabela_total[col] = 0
                    
                # Reordenar para usar exatamente as mesmas colunas
                df_tabela_total = df_tabela_total[colunas_periodos]
                    
                # Calcular total por linha: usar EXATAMENTE a mesma lógica do gráfico "CPU por Veículo"
                # Primeiro agrupar por Veículo e Período+Ano, depois por Veículo
                if tem_multiplos_anos:
                    # Agrupar por Veículo, Período e Ano primeiro (mesma lógica do gráfico linha 2030)
                    df_agrupado_periodo_total = df_visualizacao.groupby(['Veículo', 'Período', 'Ano']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                    df_total_veiculo = df_agrupado_periodo_total.groupby('Veículo').agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                else:
                    # Agrupar por Veículo e Período primeiro (mesma lógica do gráfico linha 2054)
                    if 'Período' in df_visualizacao.columns:
                        df_agrupado_periodo_total = df_visualizacao.groupby(['Veículo', 'Período']).agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                        df_total_veiculo = df_agrupado_periodo_total.groupby('Veículo').agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        # Se não tiver Período, agrupar apenas por Veículo
                        df_total_veiculo = df_visualizacao.groupby('Veículo').agg({
                            'Total': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                    
                # Recalcular CPU (mesma lógica do gráfico linha 2080)
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
                    df_total_veiculo[['Veículo', 'CPU']],
                    on='Veículo',
                    how='left'
                )
                df_tabela_total.rename(columns={'CPU': 'Total'}, inplace=True)
            else:
                # Para Custo Total, usar soma normalmente
                df_tabela_total = df_visualizacao_pivot.pivot_table(
                    index='Veículo',
                    columns=coluna_periodo_pivot,
                    values=coluna_visualizacao,
                    aggfunc='sum',
                    fill_value=0
                )
                    
                # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                for col in colunas_periodos:
                    if col not in df_tabela_total.columns:
                        df_tabela_total[col] = 0
                    
                # Reordenar para usar exatamente as mesmas colunas
                df_tabela_total = df_tabela_total[colunas_periodos]
                    
                # Calcular total por linha
                df_tabela_total['Total'] = df_tabela_total.sum(axis=1)
                
            # Resetar índice se ainda estiver como índice
            if df_tabela_total.index.name == 'Veículo' or 'Veículo' not in df_tabela_total.columns:
                df_tabela_total = df_tabela_total.reset_index()
                
            df_tabela_total = df_tabela_total.sort_values('Veículo')
                
            # Adicionar colunas adicionais fazendo merge com o primeiro valor não nulo por Veículo
            if colunas_adicionais:
                # Filtrar apenas colunas que realmente existem no DataFrame
                colunas_adicionais_validas = [
                    col for col in colunas_adicionais 
                    if col in df_visualizacao.columns
                ]
                    
                if colunas_adicionais_validas:
                    # Agrupar por Veículo e pegar o primeiro valor não nulo de cada coluna adicional
                    # Usar df_visualizacao original para ter todas as colunas
                    df_colunas_adicionais = df_visualizacao.groupby('Veículo')[colunas_adicionais_validas].first().reset_index()
                    # Fazer merge com a tabela total
                    df_tabela_total = pd.merge(
                        df_tabela_total,
                        df_colunas_adicionais,
                        on='Veículo',
                        how='left'
                    )
                    # Reordenar colunas: Veículo, colunas adicionais (na ordem original), períodos, Total
                    # Manter a ordem original das colunas adicionais
                    colunas_adicionais_ordenadas = [
                        col for col in colunas_adicionais 
                        if col in colunas_adicionais_validas
                    ]
                    colunas_finais = ['Veículo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Total']
                    # Manter apenas colunas que existem
                    colunas_finais = [col for col in colunas_finais if col in df_tabela_total.columns]
                    df_tabela_total = df_tabela_total[colunas_finais]
                
            # Formatar valores baseado no tipo de visualização
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        return f"R$ {val:,.2f}"
                return val
                
            # Aplicar formatação apenas nas colunas numéricas (exceto Veículo e colunas adicionais)
            df_tabela_total_formatado = df_tabela_total.copy()
            # Obter colunas adicionais que foram realmente adicionadas à tabela
            colunas_adicionais_na_tabela = [
                col for col in df_tabela_total_formatado.columns 
                if col not in ['Veículo'] + colunas_periodos + ['Total']
            ]
            colunas_formatar_total = [
                col for col in df_tabela_total_formatado.columns 
                if col not in ['Veículo'] + colunas_adicionais_na_tabela and 
                df_tabela_total_formatado[col].dtype in ['float64', 'float32', 'int64', 'int32']
            ]
            for col in colunas_formatar_total:
                df_tabela_total_formatado[col] = df_tabela_total_formatado[col].apply(
                    lambda x: formatar_valor(x, tipo_visualizacao)
                )
                
            # Calcular totais por coluna (meses) usando dados numéricos
            linha_total_geral = {'Veículo': '**TOTAL**'}
                
            # Adicionar valores vazios para colunas adicionais na linha de total
            for col in colunas_adicionais_na_tabela:
                if col in df_tabela_total.columns:
                    linha_total_geral[col] = ''
                
            # Adicionar totais por coluna (meses e Total)
            # LÓGICA CORRIGIDA: Quando filtra por um veículo, o total deve ser o valor desse veículo
            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                # Verificar quantos veículos há na tabela
                num_veiculos_tabela = len(df_tabela_total)
                    
                for col in df_tabela_total.columns:
                    if col not in ['Veículo'] + colunas_adicionais_na_tabela:
                        if col in colunas_periodos:
                            # Se houver apenas um veículo na tabela, recalcular a partir de df_visualizacao filtrado
                            # Isso garante que o valor seja calculado corretamente mesmo quando há apenas 1 veículo
                            if num_veiculos_tabela == 1:
                                # Quando filtra por um veículo, recalcular a partir dos dados filtrados
                                tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                                    
                                if tem_multiplos_anos:
                                    # Filtrar df_visualizacao pelo período específico
                                    df_temp = df_visualizacao.copy()
                                    df_temp['Período_Ano_temp'] = df_temp['Período'].astype(str) + ' ' + df_temp['Ano'].astype(str)
                                    df_periodo_filtrado = df_temp[df_temp['Período_Ano_temp'] == col].copy()
                                        
                                    if len(df_periodo_filtrado) > 0:
                                        # Agrupar por Ano e Período e calcular CPU
                                        df_agrupado = df_periodo_filtrado.groupby(['Ano', 'Período']).agg({
                                            'Total': 'sum',
                                            'Volume': 'sum'
                                        }).reset_index()
                                        total_periodo = df_agrupado['Total'].sum()
                                        volume_periodo = df_agrupado['Volume'].sum()
                                        if pd.notnull(volume_periodo) and volume_periodo != 0:
                                            cpu_periodo = total_periodo / volume_periodo
                                        else:
                                            cpu_periodo = 0
                                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        linha_total_geral[col] = formatar_valor(0, tipo_visualizacao)
                                else:
                                    # Sem múltiplos anos, filtrar apenas por Período
                                    df_periodo_filtrado = df_visualizacao[df_visualizacao['Período'] == col].copy()
                                        
                                    if len(df_periodo_filtrado) > 0:
                                        # Agrupar por Período e calcular CPU
                                        df_agrupado = df_periodo_filtrado.groupby('Período').agg({
                                            'Total': 'sum',
                                            'Volume': 'sum'
                                        }).reset_index()
                                        total_periodo = df_agrupado['Total'].sum()
                                        volume_periodo = df_agrupado['Volume'].sum()
                                        if pd.notnull(volume_periodo) and volume_periodo != 0:
                                            cpu_periodo = total_periodo / volume_periodo
                                        else:
                                            cpu_periodo = 0
                                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        linha_total_geral[col] = formatar_valor(0, tipo_visualizacao)
                            else:
                                # Se houver múltiplos veículos, calcular a partir dos dados filtrados
                                # Agrupar por período usando df_visualizacao filtrado, somar Total e Volume, calcular CPU
                                tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                                    
                                if tem_multiplos_anos:
                                    # Extrair mês e ano da coluna (formato: "mês ano")
                                    # Filtrar df_visualizacao pelo período específico
                                    df_temp = df_visualizacao.copy()
                                    df_temp['Período_Ano_temp'] = df_temp['Período'].astype(str) + ' ' + df_temp['Ano'].astype(str)
                                    df_periodo_filtrado = df_temp[df_temp['Período_Ano_temp'] == col].copy()
                                        
                                    if len(df_periodo_filtrado) > 0:
                                        # Agrupar por Ano e Período (mesma lógica da tabela)
                                        df_agrupado = df_periodo_filtrado.groupby(['Ano', 'Período']).agg({
                                            'Total': 'sum',
                                            'Volume': 'sum'
                                        }).reset_index()
                                        total_periodo = df_agrupado['Total'].sum()
                                        volume_periodo = df_agrupado['Volume'].sum()
                                        if pd.notnull(volume_periodo) and volume_periodo != 0:
                                            cpu_periodo = total_periodo / volume_periodo
                                        else:
                                            cpu_periodo = 0
                                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        linha_total_geral[col] = formatar_valor(0, tipo_visualizacao)
                                else:
                                    # Sem múltiplos anos, filtrar apenas por Período
                                    df_periodo_filtrado = df_visualizacao[df_visualizacao['Período'] == col].copy()
                                        
                                    if len(df_periodo_filtrado) > 0:
                                        # Agrupar por Período
                                        df_agrupado = df_periodo_filtrado.groupby('Período').agg({
                                            'Total': 'sum',
                                            'Volume': 'sum'
                                        }).reset_index()
                                        total_periodo = df_agrupado['Total'].sum()
                                        volume_periodo = df_agrupado['Volume'].sum()
                                        if pd.notnull(volume_periodo) and volume_periodo != 0:
                                            cpu_periodo = total_periodo / volume_periodo
                                        else:
                                            cpu_periodo = 0
                                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        linha_total_geral[col] = formatar_valor(0, tipo_visualizacao)
                        elif col == 'Total':
                            # Para a coluna Total, agregar Total e Volume de todos os veículos e períodos
                            total_geral = df_visualizacao['Total'].sum()
                            volume_geral = df_visualizacao['Volume'].sum()
                            if pd.notnull(volume_geral) and volume_geral != 0:
                                cpu_geral = total_geral / volume_geral
                            else:
                                cpu_geral = 0
                            linha_total_geral[col] = formatar_valor(cpu_geral, tipo_visualizacao)
                # NÃO processar outras colunas numéricas aqui - apenas colunas de período já foram processadas acima
                # elif df_tabela_total[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                #     total_col = df_tabela_total[col].sum()
                #     linha_total_geral[col] = formatar_valor(total_col, tipo_visualizacao)
            else:
                # Para Custo Total, somar normalmente
                for col in df_tabela_total.columns:
                    if col not in ['Veículo'] + colunas_adicionais_na_tabela:
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
                
            st.dataframe(df_tabela_total_display, use_container_width=True)
                
            # Botão de download da tabela total
            if st.button(
                "📥 Baixar Tabela Total por Veículo (Excel)",
                use_container_width=True,
                key="download_tabela_total_veiculo"
            ):
                with st.spinner("Gerando arquivo da tabela total..."):
                    try:
                        # Criar DataFrame completo para download (com linha de total)
                        df_total_download = df_tabela_total.copy()
                            
                        # Adicionar linha de total
                        linha_total_download = {'Veículo': 'TOTAL'}
                        # Para CPU, usar df_visualizacao diretamente para garantir agrupamento correto por Período+Ano
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                            # Verificar se há múltiplos anos
                            tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                                
                            for col in df_tabela_total.columns:
                                if col != 'Veículo':
                                    if col in colunas_periodos:
                                        # Usar EXATAMENTE a mesma lógica do gráfico "CPU por Período" (linha 2157)
                                        # Agrupar diretamente por Ano e Período de df_visualizacao, sem filtrar primeiro
                                        if tem_multiplos_anos:
                                            # Agrupar por Ano e Período de TODOS os dados, depois filtrar pelo período específico
                                            df_agrupado_todos = df_visualizacao.groupby(['Ano', 'Período']).agg({
                                                'Total': 'sum',
                                                'Volume': 'sum'
                                            }).reset_index()
                                            # Criar coluna Período_Ano para fazer match
                                            df_agrupado_todos['Período_Ano_temp'] = (
                                                df_agrupado_todos['Período'].astype(str) + ' ' + 
                                                df_agrupado_todos['Ano'].astype(str)
                                            )
                                            # Filtrar pelo período específico
                                            df_periodo_especifico = df_agrupado_todos[df_agrupado_todos['Período_Ano_temp'] == col]
                                                
                                            if len(df_periodo_especifico) > 0:
                                                total_periodo = df_periodo_especifico['Total'].iloc[0]
                                                volume_periodo = df_periodo_especifico['Volume'].iloc[0]
                                                if pd.notnull(volume_periodo) and volume_periodo != 0:
                                                    cpu_periodo = total_periodo / volume_periodo
                                                else:
                                                    cpu_periodo = 0
                                                linha_total_download[col] = cpu_periodo
                                            else:
                                                linha_total_download[col] = 0
                                        else:
                                            # Sem múltiplos anos, agrupar apenas por Período
                                            df_agrupado_todos = df_visualizacao.groupby('Período').agg({
                                                'Total': 'sum',
                                                'Volume': 'sum'
                                            }).reset_index()
                                            # Filtrar pelo período específico
                                            df_periodo_especifico = df_agrupado_todos[df_agrupado_todos['Período'] == col]
                                                
                                            if len(df_periodo_especifico) > 0:
                                                total_periodo = df_periodo_especifico['Total'].iloc[0]
                                                volume_periodo = df_periodo_especifico['Volume'].iloc[0]
                                                if pd.notnull(volume_periodo) and volume_periodo != 0:
                                                    cpu_periodo = total_periodo / volume_periodo
                                                else:
                                                    cpu_periodo = 0
                                                linha_total_download[col] = cpu_periodo
                                            else:
                                                linha_total_download[col] = 0
                                    elif col == 'Total':
                                        # Para a coluna Total, agregar Total e Volume de todos os veículos e períodos
                                        total_geral = df_visualizacao['Total'].sum()
                                        volume_geral = df_visualizacao['Volume'].sum()
                                        if pd.notnull(volume_geral) and volume_geral != 0:
                                            cpu_geral = total_geral / volume_geral
                                        else:
                                            cpu_geral = 0
                                        linha_total_download[col] = cpu_geral
                                    else:
                                        total_col = df_tabela_total[col].sum()
                                        linha_total_download[col] = total_col
                        else:
                            # Para Custo Total, somar normalmente
                            for col in df_tabela_total.columns:
                                if col != 'Veículo':
                                    total_col = df_tabela_total[col].sum()
                                    linha_total_download[col] = total_col
                            
                        df_total_download = pd.concat([
                            df_total_download,
                            pd.DataFrame([linha_total_download])
                        ], ignore_index=True)
                            
                        # Obter pasta Downloads do usuário
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
                            f"✅ Arquivo salvo com sucesso em: {file_path}"
                        )
                        st.info(
                            f"📁 Verifique sua pasta Downloads: {downloads_path}"
                        )
                    except Exception as e:
                        st.error(f"❌ Erro ao salvar arquivo: {str(e)}")
        else:
            if not tem_veiculo or not tem_periodo:
                colunas_faltando_total = []
                if not tem_veiculo:
                    colunas_faltando_total.append("Veículo")
                if not tem_periodo:
                    colunas_faltando_total.append("Período")
                st.info(f"ℹ️ Colunas necessárias não encontradas para criar a tabela total: {', '.join(colunas_faltando_total)}")

    # Tabela dinâmica: Valor por Oficina e Período
    if ('Oficina' in df_visualizacao.columns and
            'Período' in df_visualizacao.columns):
        st.markdown("---")
        
        # Determinar título do expander
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            titulo_expander_dinamica = "📋 **Tabela Dinâmica - CPU por Oficina e Período**"
        else:
            titulo_expander_dinamica = "📋 **Tabela Dinâmica - Valor por Oficina e Período**"
        
        with st.expander(titulo_expander_dinamica, expanded=False):
            if coluna_visualizacao in df_visualizacao.columns:
                # Verificar se há múltiplos anos e criar coluna combinada se necessário
                tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                
                if tem_multiplos_anos:
                    # Criar coluna combinada Período + Ano para separar meses por ano
                    df_visualizacao_pivot = df_visualizacao.copy()
                    df_visualizacao_pivot['Período_Ano'] = (
                        df_visualizacao_pivot['Período'].astype(str) + ' ' + 
                        df_visualizacao_pivot['Ano'].astype(str)
                    )
                    
                    # Criar tabela pivot
                    df_pivot = df_visualizacao_pivot.pivot_table(
                        index='Oficina',
                        columns='Período_Ano',
                        values=coluna_visualizacao,
                        aggfunc='sum',
                        fill_value=0
                    )
                    
                    # Ordenar colunas por ano e mês
                    colunas_ordenadas = []
                    anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
                    
                    for ano in anos_unicos:
                        for mes in ORDEM_MESES:
                            coluna_combinada = f"{mes} {ano}"
                            if coluna_combinada in df_pivot.columns:
                                colunas_ordenadas.append(coluna_combinada)
                    
                    # Adicionar colunas que não são meses (ex: Total, outros períodos)
                    colunas_restantes = [
                        col for col in df_pivot.columns 
                        if col not in colunas_ordenadas
                    ]
                    df_pivot = df_pivot[colunas_ordenadas + colunas_restantes]
                else:
                    # Criar tabela pivot
                    df_pivot = df_visualizacao.pivot_table(
                        index='Oficina',
                        columns='Período',
                        values=coluna_visualizacao,
                        aggfunc='sum',
                        fill_value=0
                    )

                    # Ordenar colunas por ordem cronológica dos meses
                    colunas_existentes = [
                        col for col in ORDEM_MESES if col in df_pivot.columns
                    ]
                    colunas_restantes = [
                        col for col in df_pivot.columns if col not in ORDEM_MESES
                    ]
                df_pivot = df_pivot[colunas_existentes + colunas_restantes]

            # Calcular total por linha
            df_pivot['Total'] = df_pivot.sum(axis=1)
            df_pivot = df_pivot.sort_values('Total', ascending=False)

            # Formatar valores baseado no tipo de visualização
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        return f"R$ {val:,.2f}"
                return val

            # Aplicar formatação
            df_pivot_formatado = df_pivot.copy()
            for col in df_pivot_formatado.columns:
                df_pivot_formatado[col] = df_pivot_formatado[col].apply(
                    lambda x: formatar_valor(x, tipo_visualizacao)
                )
            
            # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
            colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
            for col in colunas_para_remover:
                if col in df_pivot_formatado.columns:
                    df_pivot_formatado = df_pivot_formatado.drop(columns=[col])

            st.dataframe(df_pivot_formatado, use_container_width=True)

            # Botão de download da Tabela Dinâmica
            if st.button(
                "📥 Baixar Tabela Dinâmica (Excel)",
                use_container_width=True,
                key="download_pivot"
            ):
                with st.spinner("Gerando arquivo da tabela dinâmica..."):
                    try:
                        # Obter pasta Downloads do usuário
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
                            f"✅ Arquivo salvo com sucesso em: {file_path}"
                        )
                        st.info(
                            f"📁 Verifique sua pasta Downloads: {downloads_path}"
                        )
                    except Exception as e:
                        st.error(f"❌ Erro ao salvar arquivo: {str(e)}")

    # Exibir tabela filtrada (TODAS as linhas)
    st.markdown("---")

    # Determinar título do expander
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        titulo_expander_filtrada = "📋 **Tabela Filtrada - CPU (Todas as Linhas)**"
    else:
        titulo_expander_filtrada = "📋 **Tabela Filtrada (Todas as Linhas)**"

    with st.expander(titulo_expander_filtrada, expanded=False):
        # Usar TODAS as linhas (sem limite)
        df_display = df_visualizacao.copy()

        # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
        colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
        for col in colunas_para_remover:
            if col in df_display.columns:
                df_display = df_display.drop(columns=[col])

        st.info(f"📊 Exibindo todas as {len(df_display):,} linhas e {len(df_display.columns)} colunas")
        st.dataframe(df_display, use_container_width=True)

        # Botão de download da Tabela Filtrada
        if st.button(
            "📥 Baixar Tabela Filtrada (Excel)",
            use_container_width=True,
            key="download_filtered"
        ):
            with st.spinner("Gerando arquivo da tabela filtrada..."):
                try:
                    # Obter pasta Downloads do usuário
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

                    st.success(f"✅ Arquivo salvo com sucesso em: {file_path}")
                    st.info(f"📁 Verifique sua pasta Downloads: {downloads_path}")
                except Exception as e:
                    st.error(f"❌ Erro ao salvar arquivo: {str(e)}")

# Footer
st.markdown("---")
st.info("💡 Dashboard TC Ext - df_final com visualizações interativas")
