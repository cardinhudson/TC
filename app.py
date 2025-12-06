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

# Funções de banco de dados SQLite (definir ANTES de usar)
def inicializar_banco_taxas():
    """Cria o banco de dados e tabela para taxas de câmbio se não existir"""
    caminho_db = os.path.join(os.getcwd(), 'taxas_cambio.db')
    conn = sqlite3.connect(caminho_db)
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

def carregar_taxas_banco():
    """Carrega as taxas de câmbio do banco de dados SQLite"""
    inicializar_banco_taxas()
    caminho_db = os.path.join(os.getcwd(), 'taxas_cambio.db')
    conn = sqlite3.connect(caminho_db)
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

def salvar_taxas_banco(taxas):
    """Salva as taxas de câmbio no banco de dados SQLite"""
    inicializar_banco_taxas()
    caminho_db = os.path.join(os.getcwd(), 'taxas_cambio.db')
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    for moeda, taxa in taxas.items():
        cursor.execute('''
            INSERT OR REPLACE INTO taxas_cambio (moeda, taxa_para_brl, data_atualizacao)
            VALUES (?, ?, ?)
        ''', (moeda, float(taxa), datetime.now()))
    
    conn.commit()
    conn.close()

# Função auxiliar para listar anos disponíveis (definir ANTES de usar)
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
col_taxa1, col_taxa2 = st.columns(2, gap="small")

with col_taxa1:
    taxa_usd_para_brl = st.number_input(
        "🇺🇸 1 $ (USD) = R$",
        min_value=0.01,
        max_value=100.0,
        value=float(taxa_usd_para_brl_padrao),
        step=0.01,
        format="%.2f",
        help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros. Exemplo: se 1 USD = 5.00 BRL, digite 5.00",
        key="taxa_usd_para_brl_input"
    )

with col_taxa2:
    taxa_eur_para_brl = st.number_input(
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
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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

# Mostrar valores de referência (no final do sidebar)
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Valores de Referência**")
st.sidebar.markdown(f"**1 $ = R$ {taxa_usd_para_brl:.2f}**")
st.sidebar.markdown(f"**1 € = R$ {taxa_eur_para_brl:.2f}**")
st.sidebar.markdown(f"**1 R$ = ${taxa_brl_para_usd:.4f} USD**")
st.sidebar.markdown(f"**1 R$ = €{taxa_brl_para_eur:.4f} EUR**")


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
            
            # Fazer merge de volumes (usar outer para ver todos os períodos, depois filtrar)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on=['Ano', 'Período'],
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # Filtrar apenas períodos que têm volume real (necessário para calcular Flex Bud)
            volumes = volumes[volumes['Volume_real'].notna() & (volumes['Volume_real'] > 0)].copy()
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
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
            
            # Fazer merge de volumes (usar outer para ver todos os períodos, depois filtrar)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on='Período',
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # Filtrar apenas períodos que têm volume real (necessário para calcular Flex Bud)
            volumes = volumes[volumes['Volume_real'].notna() & (volumes['Volume_real'] > 0)].copy()
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
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
        if chart_data is None or chart_data.empty or len(chart_data) == 0:
            st.warning("⚠️ Nenhum dado disponível após agrupamento e filtros.")
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
            height=170  # Reduzido para ocupar menos espaço
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
            height=300  # Aumentado em 25% para melhor visualização
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
        
        # 🔧 CORREÇÃO: Filtrar volume a partir do primeiro mês com despesa no ano
        # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
        if df_despesas is not None and 'Veículo' in df_despesas.columns:
            # Obter combinações de Veículo + Período (e Ano se houver) que têm despesas
            if 'Ano' in df_despesas.columns and 'Período' in df_despesas.columns:
                # Agrupar por Veículo e Ano para obter o primeiro e último período com despesas
                periodos_com_despesas = df_despesas[['Veículo', 'Período', 'Ano']].drop_duplicates()
                
                # Criar mapeamento de ordem dos meses
                ordem_meses_dict = {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
                
                # Para cada combinação de Veículo e Ano, encontrar o primeiro e último período
                periodos_filtrados_list = []
                for veiculo in periodos_com_despesas['Veículo'].unique():
                    for ano in periodos_com_despesas['Ano'].unique():
                        periodos_veiculo_ano = periodos_com_despesas[
                            (periodos_com_despesas['Veículo'] == veiculo) & 
                            (periodos_com_despesas['Ano'] == ano)
                        ]['Período'].unique()
                        
                        if len(periodos_veiculo_ano) > 0:
                            # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_lower = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            periodos_ordenados = sorted(
                                periodos_veiculo_ano,
                                key=lambda x: ordem_meses_dict_lower.get(str(x).lower(), 999)
                            )
                            primeiro_periodo = periodos_ordenados[0]
                            
                            # Obter índice do primeiro período na ordem (normalizar para minúsculas)
                            idx_primeiro = ordem_meses_dict_lower.get(str(primeiro_periodo).lower(), 0)
                            
                            # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                            meses_para_incluir = ORDEM_MESES[idx_primeiro:]
                            
                            # Criar DataFrame com todos os períodos a partir do primeiro
                            for periodo in meses_para_incluir:
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
            height=300  # Aumentado em 25% para melhor visualização
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
                # Usar os mesmos períodos filtrados (a partir do primeiro mês com despesa)
                if df_despesas is not None and 'Veículo' in df_despesas.columns and len(df_budget_vol_filtrado) > 0:
                    # Obter os mesmos períodos filtrados que foram usados para os dados principais
                    if 'Ano' in df_despesas.columns and 'Período' in df_despesas.columns and 'Ano' in df_budget_vol_filtrado.columns:
                        # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                        ordem_meses_dict_budget = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                        
                        # Obter períodos com despesas
                        periodos_com_despesas_budget = df_despesas[['Veículo', 'Período', 'Ano']].drop_duplicates()
                        
                        # Para cada combinação de Veículo e Ano, encontrar o primeiro período
                        periodos_filtrados_list_budget = []
                        for veiculo in periodos_com_despesas_budget['Veículo'].unique():
                            for ano in periodos_com_despesas_budget['Ano'].unique():
                                periodos_veiculo_ano_budget = periodos_com_despesas_budget[
                                    (periodos_com_despesas_budget['Veículo'] == veiculo) & 
                                    (periodos_com_despesas_budget['Ano'] == ano)
                                ]['Período'].unique()
                                
                                if len(periodos_veiculo_ano_budget) > 0:
                                    # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                    periodos_ordenados_budget = sorted(
                                        periodos_veiculo_ano_budget,
                                        key=lambda x: ordem_meses_dict_budget.get(str(x).lower(), 999)
                                    )
                                    primeiro_periodo_budget = periodos_ordenados_budget[0]
                                    
                                    # Obter índice do primeiro período na ordem
                                    idx_primeiro_budget = ordem_meses_dict_budget.get(str(primeiro_periodo_budget).lower(), 0)
                                    
                                    # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                    meses_para_incluir_budget = ORDEM_MESES[idx_primeiro_budget:]
                                    
                                    # Criar DataFrame com todos os períodos a partir do primeiro
                                    for periodo in meses_para_incluir_budget:
                                        periodo_formatado = periodo.capitalize() if str(primeiro_periodo_budget)[0].isupper() else periodo
                                        periodos_filtrados_list_budget.append({
                                            'Veículo': veiculo,
                                            'Período': periodo_formatado,
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
                        # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                        ordem_meses_dict_budget = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                        
                        # Obter períodos com despesas
                        periodos_com_despesas_budget = df_despesas[['Veículo', 'Período']].drop_duplicates()
                        
                        # Para cada Veículo, encontrar o primeiro período
                        periodos_filtrados_list_budget = []
                        for veiculo in periodos_com_despesas_budget['Veículo'].unique():
                            periodos_veiculo_budget = periodos_com_despesas_budget[
                                periodos_com_despesas_budget['Veículo'] == veiculo
                            ]['Período'].unique()
                            
                            if len(periodos_veiculo_budget) > 0:
                                # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                periodos_ordenados_budget = sorted(
                                    periodos_veiculo_budget,
                                    key=lambda x: ordem_meses_dict_budget.get(str(x).lower(), 999)
                                )
                                primeiro_periodo_budget = periodos_ordenados_budget[0]
                                
                                # Obter índice do primeiro período na ordem
                                idx_primeiro_budget = ordem_meses_dict_budget.get(str(primeiro_periodo_budget).lower(), 0)
                                
                                # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                meses_para_incluir_budget = ORDEM_MESES[idx_primeiro_budget:]
                                
                                # Criar DataFrame com todos os períodos a partir do primeiro
                                for periodo in meses_para_incluir_budget:
                                    periodo_formatado = periodo.capitalize() if str(primeiro_periodo_budget)[0].isupper() else periodo
                                    periodos_filtrados_list_budget.append({
                                        'Veículo': veiculo,
                                        'Período': periodo_formatado
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
        return {st.session_state.tab_selecionada_tc_ext_persistente};
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
        # 🔧 CORREÇÃO CRÍTICA: Usar df_total diretamente (que tem 'Custo') em vez de df_para_grafico_periodo
        # porque df_para_grafico_periodo pode não ter 'Custo' se foi processado
        df_real_original_grafico = None
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # IMPORTANTE: Usar df_total diretamente (que tem 'Custo') e aplicar TODOS os filtros
            # Isso garante que temos a coluna 'Custo' necessária para calcular Flex Bud
            if 'Custo' in df_total.columns:
                df_real_original_grafico = df_total.copy()
                
                # Aplicar TODOS os filtros da sidebar (mesmos que foram aplicados a df_filtrado)
                # Filtro de Ano
                if ano_selecionado != "Todos" and 'Ano' in df_real_original_grafico.columns:
                    df_real_original_grafico = df_real_original_grafico[
                        df_real_original_grafico['Ano'] == int(ano_selecionado)
                    ].copy()
                
                # Filtro de Oficina
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Filtro de Veículo
                if 'Veículo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # NOTA: A conversão de moeda já foi aplicada no df_total (linha ~999)
                # Portanto, df_real_original_grafico['Total'] já está convertido
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
        
        # Tabela de análise Flex Bud removida conforme solicitado

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
            
            if 'J516' in df_vol_filtrado['Veículo'].values:
                j516_vol_total = df_vol_filtrado[df_vol_filtrado['Veículo'] == 'J516']['Volume'].sum()
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
    # 🔧 CORREÇÃO CRÍTICA: Usar df_vol_filtrado (mesmo DataFrame usado no gráfico "Volume Total")
    # para garantir que os mesmos filtros de Oficina sejam aplicados
    if df_vol_filtrado is not None and 'Volume' in df_vol_filtrado.columns and 'Veículo' in df_vol_filtrado.columns:
        st.subheader("📊 Volume por Veículo")
        
        if 'J516' in df_vol_filtrado['Veículo'].values:
            j516_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Veículo'] == 'J516']['Volume'].sum()
        # Usar df_budget_vol_filtrado_grafico se disponível (mesma variável usada no gráfico de volume por período)
        df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
        # 🔧 IMPORTANTE: Passar df_visualizacao como df_despesas para filtrar apenas períodos com despesas
        df_despesas_para_filtro = df_visualizacao if 'df_visualizacao' in locals() and 'Veículo' in df_visualizacao.columns else None
        # 🔧 IMPORTANTE: Usar df_vol_filtrado em vez de df_visualizacao para garantir consistência
        grafico_volume_veiculo = create_volume_veiculo_chart(df_vol_filtrado, df_budget_vol_para_grafico, df_despesas_para_filtro)
        if grafico_volume_veiculo is not None:
            st.altair_chart(grafico_volume_veiculo, use_container_width=True)
    elif 'Volume' in df_visualizacao.columns and 'Veículo' in df_visualizacao.columns:
        # Fallback: usar df_visualizacao se df_vol_filtrado não estiver disponível
        st.subheader("📊 Volume por Veículo")
        df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
        # 🔧 IMPORTANTE: Passar df_visualizacao como df_despesas para filtrar apenas períodos com despesas
        df_despesas_para_filtro = df_visualizacao.copy() if 'Veículo' in df_visualizacao.columns else None
        grafico_volume_veiculo = create_volume_veiculo_chart(df_visualizacao, df_budget_vol_para_grafico, df_despesas_para_filtro)
        if grafico_volume_veiculo is not None:
            st.altair_chart(grafico_volume_veiculo, use_container_width=True)

# Gráfico 2: Soma do Valor por Oficina
# Cache removido temporariamente para forçar atualização
def create_oficina_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None):
    """Cria gráfico de barras por Oficina com linha de Flex Bud opcional"""
    try:
        if (coluna not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

        # 🔧 CORREÇÃO: No modo CPU, sempre agrupar apenas por Oficina (sem Veículo) para padronizar com Custo Total
        # Removido o bloco que agrupava por Veículo - agora sempre usa a lógica do bloco "else" abaixo
        if (tipo_viz == "CPU (Custo por Unidade)" and
                'Veículo' in df_data.columns and
                'Total' not in df_data.columns):
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
                height=320
                # Título removido para evitar duplicação com st.subheader
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
                # Caminho quando não tem Total/Volume ou não tem Veículo
                chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores válidos
            if coluna not in chart_data.columns:
                return None
            
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
            
            if df_budget is not None and 'Oficina' in df_budget.columns and df_real_vol is not None:
                # A função precisa da coluna 'Custo'
                # Se não tiver, tentar usar df_real_original ou df_total que deve ter
                if 'Custo' not in df_real_para_flex.columns:
                    # Tentar usar df_total global que deve ter a coluna 'Custo'
                    if 'df_total' in globals() and 'Custo' in globals()['df_total'].columns:
                        df_real_para_flex = globals()['df_total'].copy()
                    else:
                        df_real_para_flex = None
                
                if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                    try:
                        # 🔧 CORREÇÃO: Normalizar períodos ANTES de agrupar (mesma lógica do calcular_flex_budget)
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
                        
                        # Calcular FLEX agrupado por Oficina seguindo a mesma lógica do gráfico por Período
                        # Primeiro calcular FLEX por Período e Oficina, depois agrupar por Oficina
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        
                        # Calcular Flex Bud por Período e Oficina (mesma lógica do gráfico por Período)
                        # Agrupar dados reais por Período e Oficina
                        if tem_ano:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            # Agrupar budget por Período e Oficina
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        else:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Período', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Período', 'Oficina', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        
                        # Verificar se temos todos os dados necessários
                        if (real_agrupado is None or real_vol_agrupado is None or 
                            budget_agrupado is None or budget_vol_agrupado is None):
                            flex_data = None
                        else:
                            # 🔧 CORREÇÃO: Normalizar períodos nos DataFrames agrupados antes do merge
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
                            
                            # Fazer merge de volumes por Período e Oficina
                            # Usar 'left' para incluir todas as oficinas dos dados reais, mesmo sem budget
                            if tem_ano:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Ano', 'Período', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se não houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            else:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Período', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se não houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            
                            # 🔧 CORREÇÃO: Para CPU, não podemos somar os CPUs de cada período
                            # Devemos calcular o Flex Bud Total (Custo Total) por período e oficina,
                            # depois agregar por oficina e recalcular o CPU final
                            
                            # Calcular Flex Bud Total (Custo Total) para cada Período e Oficina
                            flex_data = []
                            for _, vol_row in volumes.iterrows():
                                if tem_ano:
                                    ano = vol_row['Ano']
                                    periodo = vol_row['Período']
                                    oficina = vol_row['Oficina']
                                else:
                                    periodo = vol_row['Período']
                                    oficina = vol_row['Oficina']
                                
                                volume_real = vol_row['Volume_real']
                                volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                                
                                if volume_real == 0 or pd.isna(volume_real):
                                    continue
                                
                                # Obter custos reais para este Período e Oficina
                                if tem_ano:
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Ano'] == ano) & 
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Oficina'] == oficina)
                                    ]
                                else:
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Oficina'] == oficina)
                                    ]
                                
                                # Se não houver dados de budget para esta oficina, usar zeros
                                if len(custos_budget) == 0:
                                    custo_fixo_budget = 0
                                    custo_variavel_budget = 0
                                else:
                                    custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                                    custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                                
                                # Calcular Flex Bud Total (Custo Total) para este período e oficina
                                # Flex Bud Fixo = BUD Fixo (não varia com volume)
                                flex_bud_fixo = custo_fixo_budget
                                # Flex Bud Variável = BUD Variável × (Volume Real / Volume Budget)
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                                # Flex Bud Total (em Custo Total) = Flex Bud Fixo + Flex Bud Variável
                                flex_bud_total_custo_total = flex_bud_fixo + flex_bud_variavel
                                
                                # Adicionar ao flex_data com Oficina (armazenar Custo Total, não CPU)
                                if tem_ano:
                                    flex_data.append({
                                        'Ano': ano,
                                        'Período': periodo,
                                        'Oficina': oficina,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                                else:
                                    flex_data.append({
                                        'Período': periodo,
                                        'Oficina': oficina,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                            
                            if len(flex_data) == 0:
                                flex_data = None
                            else:
                                flex_data = pd.DataFrame(flex_data)
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud Total e Volume Real por Oficina (somar todos os períodos)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Para CPU: somar Flex Bud Total e Volume Real, depois recalcular CPU
                                budget_data = flex_data.groupby('Oficina').agg({
                                    'Flex_Bud_Total': 'sum',
                                    'Volume_Real': 'sum'
                                }).reset_index()
                                
                                # Recalcular CPU: Flex Bud Total agregado / Volume Real agregado
                                budget_data[coluna] = budget_data.apply(
                                    lambda row: (
                                        row['Flex_Bud_Total'] / row['Volume_Real']
                                        if pd.notnull(row['Volume_Real']) and row['Volume_Real'] != 0
                                        else 0
                                    ),
                                    axis=1
                                )
                                
                                # Manter apenas colunas necessárias
                                budget_data = budget_data[['Oficina', coluna]]
                            else:
                                # Para Custo Total: apenas somar Flex Bud Total
                                budget_data = flex_data.groupby('Oficina')['Flex_Bud_Total'].sum().reset_index()
                                budget_data.rename(columns={'Flex_Bud_Total': coluna}, inplace=True)
                            
                            # Filtrar apenas oficinas que existem no chart_data
                            budget_data = budget_data[budget_data['Oficina'].isin(chart_data['Oficina'])].copy()
                            
                            if len(budget_data) > 0:
                                # Criar linha tracejada de Flex Bud
                                budget_data_legenda = budget_data.copy()
                                budget_data_legenda['Tipo'] = 'Flex Bud'
                                
                                # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras
                                # Reordenar budget_data_legenda para seguir a ordem de ordem_oficinas_barras
                                # Criar um dicionário de mapeamento de ordem
                                ordem_dict = {oficina: idx for idx, oficina in enumerate(ordem_oficinas_barras)}
                                # Adicionar coluna de ordem para ordenar
                                budget_data_legenda['_ordem'] = budget_data_legenda['Oficina'].map(ordem_dict)
                                # Filtrar apenas oficinas que existem na ordem e ordenar
                                budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                                budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                                budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                                
                                # Usar a mesma ordem das barras (filtrando apenas oficinas que existem no budget)
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
                        # Silenciar erro, apenas não mostrar linha de budget
                        pass

            # Usar a ordem explícita para garantir sincronização com a linha pontilhada
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort=ordem_oficinas_barras, axis=alt.Axis(grid=False, domain=True, ticks=True)),
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
                height=320
                # Título removido para evitar duplicação com st.subheader
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
                x=alt.X('Oficina:N', sort=ordem_oficinas_barras, title='Oficina'),
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
                        height=38
                    )
                    
                    # Adicionar rótulos de dados no gráfico de delta
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
# Cache removido temporariamente para forçar atualização
def create_total_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, df_visualizacao_volume=None, df_total_completo=None, df_despesas=None, df_total_filtrado=None, df_volume_filtrado_grafico=None):
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
            # 🔧 CORREÇÃO CRÍTICA: No modo CPU, df_data (df_visualizacao) tem Total e Volume já agregados por Oficina+Período+Veículo
            # Quando agrupamos apenas por Veículo, estamos somando valores que podem estar duplicados
            # Precisamos usar o Total DIRETO de df_real_original (dados originais) e Volume DIRETO de df_real_vol (arquivo original)
            if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns:
                # 🔧 CORREÇÃO: Usar EXATAMENTE os mesmos DataFrames dos gráficos
                # Total: usar df_total_filtrado (mesmo usado no gráfico "Total por Veículo" modo Custo Total)
                # Volume: usar df_volume_filtrado_grafico (mesmo usado no gráfico "Volume por Veículo")
                
                # PRIORIDADE 1: Usar df_total_filtrado e df_volume_filtrado_grafico se disponíveis
                if df_total_filtrado is not None and 'Total' in df_total_filtrado.columns and 'Veículo' in df_total_filtrado.columns:
                    # Calcular Total EXATAMENTE como no gráfico "Total por Veículo" (modo Custo Total)
                    df_total_agrupado_final = df_total_filtrado.groupby('Veículo')['Total'].sum().reset_index()
                else:
                    df_total_agrupado_final = None
                
                # PRIORIDADE 1: Usar df_volume_filtrado_grafico se disponível (mesmo usado no gráfico "Volume por Veículo")
                if df_volume_filtrado_grafico is not None and 'Volume' in df_volume_filtrado_grafico.columns and 'Veículo' in df_volume_filtrado_grafico.columns:
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar o MESMO filtro de períodos que é aplicado em create_volume_veiculo_chart
                    # O df_volume_filtrado_grafico ainda não foi filtrado por períodos com despesas
                    # Precisamos aplicar o mesmo filtro aqui
                    df_volume_processado = df_volume_filtrado_grafico.copy()
                    
                    # Aplicar filtro de períodos com despesas (MESMA LÓGICA de create_volume_veiculo_chart)
                    if df_despesas is not None and 'Veículo' in df_despesas.columns and 'Período' in df_volume_processado.columns:
                        # Obter combinações de Veículo + Período (e Ano se houver) que têm despesas
                        if 'Ano' in df_despesas.columns and 'Ano' in df_volume_processado.columns:
                            # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_vol = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            
                            # Agrupar por Veículo e Ano para obter o primeiro período com despesas
                            periodos_com_despesas_vol = df_despesas[['Veículo', 'Período', 'Ano']].drop_duplicates()
                            
                            # Para cada combinação de Veículo e Ano, encontrar o primeiro período
                            periodos_filtrados_list_vol = []
                            for veiculo in periodos_com_despesas_vol['Veículo'].unique():
                                for ano in periodos_com_despesas_vol['Ano'].unique():
                                    periodos_veiculo_ano_vol = periodos_com_despesas_vol[
                                        (periodos_com_despesas_vol['Veículo'] == veiculo) & 
                                        (periodos_com_despesas_vol['Ano'] == ano)
                                    ]['Período'].unique()
                                    
                                    if len(periodos_veiculo_ano_vol) > 0:
                                        # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                        periodos_ordenados_vol = sorted(
                                            periodos_veiculo_ano_vol,
                                            key=lambda x: ordem_meses_dict_vol.get(str(x).lower(), 999)
                                        )
                                        primeiro_periodo_vol = periodos_ordenados_vol[0]
                                        
                                        # Obter índice do primeiro período na ordem
                                        idx_primeiro_vol = ordem_meses_dict_vol.get(str(primeiro_periodo_vol).lower(), 0)
                                        
                                        # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                        meses_para_incluir_vol = ORDEM_MESES[idx_primeiro_vol:]
                                        
                                        # Criar DataFrame com todos os períodos a partir do primeiro
                                        for periodo in meses_para_incluir_vol:
                                            periodo_formatado = periodo.capitalize() if str(primeiro_periodo_vol)[0].isupper() else periodo
                                            periodos_filtrados_list_vol.append({
                                                'Veículo': veiculo,
                                                'Período': periodo_formatado,
                                                'Ano': ano
                                            })
                            
                            if periodos_filtrados_list_vol:
                                periodos_filtrados_vol = pd.DataFrame(periodos_filtrados_list_vol)
                                
                                # Normalizar períodos antes do merge
                                df_volume_merge = df_volume_processado.copy()
                                periodos_filtrados_vol_merge = periodos_filtrados_vol.copy()
                                
                                df_volume_merge['Período_normalizado'] = df_volume_merge['Período'].astype(str).str.lower().str.strip()
                                periodos_filtrados_vol_merge['Período_normalizado'] = periodos_filtrados_vol_merge['Período'].astype(str).str.lower().str.strip()
                                
                                # Fazer merge usando períodos normalizados
                                df_volume_processado = pd.merge(
                                    df_volume_merge,
                                    periodos_filtrados_vol_merge[['Veículo', 'Período_normalizado', 'Ano']],
                                    on=['Veículo', 'Período_normalizado', 'Ano'],
                                    how='inner'
                                )
                                
                                # Remover coluna temporária
                                df_volume_processado = df_volume_processado.drop(columns=['Período_normalizado'])
                        
                        elif 'Período' in df_despesas.columns:
                            # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_vol = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            
                            # Agrupar por Veículo para obter o primeiro período com despesas
                            periodos_com_despesas_vol = df_despesas[['Veículo', 'Período']].drop_duplicates()
                            
                            # Para cada Veículo, encontrar o primeiro período
                            periodos_filtrados_list_vol = []
                            for veiculo in periodos_com_despesas_vol['Veículo'].unique():
                                periodos_veiculo_vol = periodos_com_despesas_vol[
                                    periodos_com_despesas_vol['Veículo'] == veiculo
                                ]['Período'].unique()
                                
                                if len(periodos_veiculo_vol) > 0:
                                    # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                    periodos_ordenados_vol = sorted(
                                        periodos_veiculo_vol,
                                        key=lambda x: ordem_meses_dict_vol.get(str(x).lower(), 999)
                                    )
                                    primeiro_periodo_vol = periodos_ordenados_vol[0]
                                    
                                    # Obter índice do primeiro período na ordem
                                    idx_primeiro_vol = ordem_meses_dict_vol.get(str(primeiro_periodo_vol).lower(), 0)
                                    
                                    # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                    meses_para_incluir_vol = ORDEM_MESES[idx_primeiro_vol:]
                                    
                                    # Criar DataFrame com todos os períodos a partir do primeiro
                                    for periodo in meses_para_incluir_vol:
                                        periodo_formatado = periodo.capitalize() if str(primeiro_periodo_vol)[0].isupper() else periodo
                                        periodos_filtrados_list_vol.append({
                                            'Veículo': veiculo,
                                            'Período': periodo_formatado
                                        })
                            
                            if periodos_filtrados_list_vol:
                                periodos_filtrados_vol = pd.DataFrame(periodos_filtrados_list_vol)
                                
                                # Normalizar períodos antes do merge
                                df_volume_merge = df_volume_processado.copy()
                                periodos_filtrados_vol_merge = periodos_filtrados_vol.copy()
                                
                                df_volume_merge['Período_normalizado'] = df_volume_merge['Período'].astype(str).str.lower().str.strip()
                                periodos_filtrados_vol_merge['Período_normalizado'] = periodos_filtrados_vol_merge['Período'].astype(str).str.lower().str.strip()
                                
                                # Fazer merge usando períodos normalizados
                                df_volume_processado = pd.merge(
                                    df_volume_merge,
                                    periodos_filtrados_vol_merge[['Veículo', 'Período_normalizado']],
                                    on=['Veículo', 'Período_normalizado'],
                                    how='inner'
                                )
                                
                                # Remover coluna temporária
                                df_volume_processado = df_volume_processado.drop(columns=['Período_normalizado'])
                    
                    # Agora processar o volume filtrado EXATAMENTE como no gráfico "Volume por Veículo"
                    tem_multiplos_anos_vol = 'Ano' in df_volume_processado.columns and df_volume_processado['Ano'].nunique() > 1
                    
                    if tem_multiplos_anos_vol and 'Período' in df_volume_processado.columns:
                        df_agrupado_periodo_vol = df_volume_processado.groupby(['Veículo', 'Período', 'Ano']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        df_volume_agrupado = df_agrupado_periodo_vol.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    elif 'Período' in df_volume_processado.columns:
                        df_agrupado_periodo_vol = df_volume_processado.groupby(['Veículo', 'Período']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        df_volume_agrupado = df_agrupado_periodo_vol.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        df_volume_agrupado = df_volume_processado.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    
                else:
                    df_volume_agrupado = None
                
                # Se temos ambos, fazer o merge e calcular CPU
                if df_total_agrupado_final is not None and df_volume_agrupado is not None:
                    chart_data = pd.merge(
                        df_total_agrupado_final,
                        df_volume_agrupado,
                        on='Veículo',
                        how='left'
                    )
                    chart_data['Volume'] = chart_data['Volume'].fillna(0)
                    
                    # Calcular CPU
                    chart_data[coluna] = chart_data.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    
                    chart_data = chart_data[['Veículo', coluna]]
                    # ✅ SUCESSO: Usamos os mesmos valores dos gráficos, não precisamos continuar com a lógica antiga
                    # Pular toda a lógica antiga e ir direto para criar o gráfico
                    # Definir uma flag para pular a lógica antiga
                    usar_logica_antiga = False
                else:
                    # Fallback: usar lógica antiga
                    usar_logica_antiga = True
                    # Continuar com a lógica antiga (código abaixo)
                    df_total_para_calculo = None
                    df_volume_para_calculo = None
                
                # Só executar a lógica antiga se não tivermos calculado com os DataFrames corretos
                if usar_logica_antiga:
                    if df_total_completo is not None and 'Total' in df_total_completo.columns and 'Veículo' in df_total_completo.columns:
                        df_total_para_calculo = df_total_completo.copy()
                        # Aplicar apenas filtros de Ano e Oficina (se houver), mas NÃO filtro de Veículo
                        # Os filtros de Ano e Oficina já devem estar aplicados em df_real_original
                        if df_real_original is not None:
                            # Aplicar filtro de Ano se houver
                            if 'Ano' in df_real_original.columns and 'Ano' in df_total_para_calculo.columns:
                                anos_em_real = df_real_original['Ano'].unique()
                                if len(anos_em_real) > 0:
                                    df_total_para_calculo = df_total_para_calculo[
                                        df_total_para_calculo['Ano'].isin(anos_em_real)
                                    ].copy()
                            # Aplicar filtro de Oficina se houver
                            if 'Oficina' in df_real_original.columns and 'Oficina' in df_total_para_calculo.columns:
                                oficinas_em_real = df_real_original['Oficina'].unique()
                                if len(oficinas_em_real) > 0:
                                    df_total_para_calculo = df_total_para_calculo[
                                        df_total_para_calculo['Oficina'].isin(oficinas_em_real)
                                    ].copy()
                    
                    # Fallback: usar df_real_original se df_total_completo não estiver disponível
                    if df_total_para_calculo is None:
                        if df_real_original is not None and 'Total' in df_real_original.columns and 'Veículo' in df_real_original.columns:
                            df_total_para_calculo = df_real_original
                        else:
                            # Último fallback: usar df_data
                            df_total_para_calculo = df_data
                    
                    # Para Volume: usar df_real_vol (arquivo original)
                    if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Veículo' in df_real_vol.columns:
                        df_volume_para_calculo = df_real_vol
                    elif df_visualizacao_volume is not None and 'Volume' in df_visualizacao_volume.columns and 'Veículo' in df_visualizacao_volume.columns:
                        df_volume_para_calculo = df_visualizacao_volume
                    
                    if df_total_para_calculo is not None and df_volume_para_calculo is not None:
                        # 🔧 CORREÇÃO CRÍTICA: Para calcular o Total por Veículo, precisamos de TODOS os dados de cada veículo
                        # NÃO devemos aplicar o filtro de Veículo aqui, porque queremos somar TODOS os dados de cada veículo
                        # Apenas aplicamos filtros de Ano e Oficina (se houver), mas NÃO o filtro de Veículo
                        # O filtro de Veículo será aplicado DEPOIS, quando formos exibir apenas os veículos selecionados
                        
                        # Filtrar linhas com Volume e Veículo não nulos (mesma lógica do gráfico de volume)
                        df_volume_filtrado = df_volume_para_calculo[df_volume_para_calculo['Volume'].notna() & df_volume_para_calculo['Veículo'].notna()].copy()
                        
                        # 🔧 CORREÇÃO: Aplicar o mesmo filtro de períodos (a partir do primeiro mês com despesa) ao volume
                        # Isso garante que o volume usado para calcular CPU seja o mesmo do gráfico "Volume por Veículo"
                        if df_despesas is not None and 'Veículo' in df_despesas.columns and 'Período' in df_volume_filtrado.columns:
                            # Obter combinações de Veículo + Período (e Ano se houver) que têm despesas
                            if 'Ano' in df_despesas.columns and 'Ano' in df_volume_filtrado.columns:
                                # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                                ordem_meses_dict_vol = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                                
                                # Agrupar por Veículo e Ano para obter o primeiro período com despesas
                                periodos_com_despesas_vol = df_despesas[['Veículo', 'Período', 'Ano']].drop_duplicates()
                                
                                # Para cada combinação de Veículo e Ano, encontrar o primeiro período
                                periodos_filtrados_list_vol = []
                                for veiculo in periodos_com_despesas_vol['Veículo'].unique():
                                    for ano in periodos_com_despesas_vol['Ano'].unique():
                                        periodos_veiculo_ano_vol = periodos_com_despesas_vol[
                                            (periodos_com_despesas_vol['Veículo'] == veiculo) & 
                                            (periodos_com_despesas_vol['Ano'] == ano)
                                        ]['Período'].unique()
                                        
                                        if len(periodos_veiculo_ano_vol) > 0:
                                            # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                            periodos_ordenados_vol = sorted(
                                                periodos_veiculo_ano_vol,
                                                key=lambda x: ordem_meses_dict_vol.get(str(x).lower(), 999)
                                            )
                                            primeiro_periodo_vol = periodos_ordenados_vol[0]
                                            
                                            # Obter índice do primeiro período na ordem
                                            idx_primeiro_vol = ordem_meses_dict_vol.get(str(primeiro_periodo_vol).lower(), 0)
                                            
                                            # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                            meses_para_incluir_vol = ORDEM_MESES[idx_primeiro_vol:]
                                            
                                            # Criar DataFrame com todos os períodos a partir do primeiro
                                            for periodo in meses_para_incluir_vol:
                                                periodo_formatado = periodo.capitalize() if str(primeiro_periodo_vol)[0].isupper() else periodo
                                                periodos_filtrados_list_vol.append({
                                                    'Veículo': veiculo,
                                                    'Período': periodo_formatado,
                                                    'Ano': ano
                                                })
                                
                                if periodos_filtrados_list_vol:
                                    periodos_filtrados_vol = pd.DataFrame(periodos_filtrados_list_vol)
                                    
                                    # Normalizar períodos antes do merge
                                    df_volume_merge = df_volume_filtrado.copy()
                                    periodos_filtrados_vol_merge = periodos_filtrados_vol.copy()
                                    
                                    df_volume_merge['Período_normalizado'] = df_volume_merge['Período'].astype(str).str.lower().str.strip()
                                    periodos_filtrados_vol_merge['Período_normalizado'] = periodos_filtrados_vol_merge['Período'].astype(str).str.lower().str.strip()
                                    
                                    # Fazer merge usando períodos normalizados
                                    df_volume_filtrado = pd.merge(
                                        df_volume_merge,
                                        periodos_filtrados_vol_merge[['Veículo', 'Período_normalizado', 'Ano']],
                                        on=['Veículo', 'Período_normalizado', 'Ano'],
                                        how='inner'
                                    )
                                    
                                    # Remover coluna temporária
                                    df_volume_filtrado = df_volume_filtrado.drop(columns=['Período_normalizado'])
                        
                        elif 'Período' in df_despesas.columns:
                            # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_vol = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            
                            # Agrupar por Veículo para obter o primeiro período com despesas
                            periodos_com_despesas_vol = df_despesas[['Veículo', 'Período']].drop_duplicates()
                            
                            # Para cada Veículo, encontrar o primeiro período
                            periodos_filtrados_list_vol = []
                            for veiculo in periodos_com_despesas_vol['Veículo'].unique():
                                periodos_veiculo_vol = periodos_com_despesas_vol[
                                    periodos_com_despesas_vol['Veículo'] == veiculo
                                ]['Período'].unique()
                                
                                if len(periodos_veiculo_vol) > 0:
                                    # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                    periodos_ordenados_vol = sorted(
                                        periodos_veiculo_vol,
                                        key=lambda x: ordem_meses_dict_vol.get(str(x).lower(), 999)
                                    )
                                    primeiro_periodo_vol = periodos_ordenados_vol[0]
                                    
                                    # Obter índice do primeiro período na ordem
                                    idx_primeiro_vol = ordem_meses_dict_vol.get(str(primeiro_periodo_vol).lower(), 0)
                                    
                                    # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                    meses_para_incluir_vol = ORDEM_MESES[idx_primeiro_vol:]
                                    
                                    # Criar DataFrame com todos os períodos a partir do primeiro
                                    for periodo in meses_para_incluir_vol:
                                        periodo_formatado = periodo.capitalize() if str(primeiro_periodo_vol)[0].isupper() else periodo
                                        periodos_filtrados_list_vol.append({
                                            'Veículo': veiculo,
                                            'Período': periodo_formatado
                                        })
                            
                            if periodos_filtrados_list_vol:
                                periodos_filtrados_vol = pd.DataFrame(periodos_filtrados_list_vol)
                                
                                # Normalizar períodos antes do merge
                                df_volume_merge = df_volume_filtrado.copy()
                                periodos_filtrados_vol_merge = periodos_filtrados_vol.copy()
                                
                                df_volume_merge['Período_normalizado'] = df_volume_merge['Período'].astype(str).str.lower().str.strip()
                                periodos_filtrados_vol_merge['Período_normalizado'] = periodos_filtrados_vol_merge['Período'].astype(str).str.lower().str.strip()
                                
                                # Fazer merge usando períodos normalizados
                                df_volume_filtrado = pd.merge(
                                    df_volume_merge,
                                    periodos_filtrados_vol_merge[['Veículo', 'Período_normalizado']],
                                    on=['Veículo', 'Período_normalizado'],
                                    how='inner'
                                )
                                
                                # Remover coluna temporária
                                df_volume_filtrado = df_volume_filtrado.drop(columns=['Período_normalizado'])
                    
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar o MESMO filtro de períodos ao Total
                    # O Total deve incluir apenas os mesmos períodos que o Volume (a partir do primeiro mês com despesa)
                    if df_despesas is not None and 'Veículo' in df_despesas.columns and 'Período' in df_total_para_calculo.columns:
                        # Obter combinações de Veículo + Período (e Ano se houver) que têm despesas
                        if 'Ano' in df_despesas.columns and 'Ano' in df_total_para_calculo.columns:
                            # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_total = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            
                            # Agrupar por Veículo e Ano para obter o primeiro período com despesas
                            periodos_com_despesas_total = df_despesas[['Veículo', 'Período', 'Ano']].drop_duplicates()
                            
                            # Para cada combinação de Veículo e Ano, encontrar o primeiro período
                            periodos_filtrados_list_total = []
                            for veiculo in periodos_com_despesas_total['Veículo'].unique():
                                for ano in periodos_com_despesas_total['Ano'].unique():
                                    periodos_veiculo_ano_total = periodos_com_despesas_total[
                                        (periodos_com_despesas_total['Veículo'] == veiculo) & 
                                        (periodos_com_despesas_total['Ano'] == ano)
                                    ]['Período'].unique()
                                    
                                    if len(periodos_veiculo_ano_total) > 0:
                                        # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                        periodos_ordenados_total = sorted(
                                            periodos_veiculo_ano_total,
                                            key=lambda x: ordem_meses_dict_total.get(str(x).lower(), 999)
                                        )
                                        primeiro_periodo_total = periodos_ordenados_total[0]
                                        
                                        # Obter índice do primeiro período na ordem
                                        idx_primeiro_total = ordem_meses_dict_total.get(str(primeiro_periodo_total).lower(), 0)
                                        
                                        # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                        meses_para_incluir_total = ORDEM_MESES[idx_primeiro_total:]
                                        
                                        # Criar DataFrame com todos os períodos a partir do primeiro
                                        for periodo in meses_para_incluir_total:
                                            periodo_formatado = periodo.capitalize() if str(primeiro_periodo_total)[0].isupper() else periodo
                                            periodos_filtrados_list_total.append({
                                                'Veículo': veiculo,
                                                'Período': periodo_formatado,
                                                'Ano': ano
                                            })
                            
                            if periodos_filtrados_list_total:
                                periodos_filtrados_total = pd.DataFrame(periodos_filtrados_list_total)
                                
                                # Normalizar períodos antes do merge
                                df_total_merge = df_total_para_calculo.copy()
                                periodos_filtrados_total_merge = periodos_filtrados_total.copy()
                                
                                df_total_merge['Período_normalizado'] = df_total_merge['Período'].astype(str).str.lower().str.strip()
                                periodos_filtrados_total_merge['Período_normalizado'] = periodos_filtrados_total_merge['Período'].astype(str).str.lower().str.strip()
                                
                                # Fazer merge usando períodos normalizados
                                df_total_para_calculo = pd.merge(
                                    df_total_merge,
                                    periodos_filtrados_total_merge[['Veículo', 'Período_normalizado', 'Ano']],
                                    on=['Veículo', 'Período_normalizado', 'Ano'],
                                    how='inner'
                                )
                                
                                # Remover coluna temporária
                                df_total_para_calculo = df_total_para_calculo.drop(columns=['Período_normalizado'])
                                
                        
                        elif 'Período' in df_despesas.columns:
                            # Criar mapeamento de ordem dos meses (normalizar para minúsculas)
                            ordem_meses_dict_total = {mes.lower(): idx for idx, mes in enumerate(ORDEM_MESES)}
                            
                            # Agrupar por Veículo para obter o primeiro período com despesas
                            periodos_com_despesas_total = df_despesas[['Veículo', 'Período']].drop_duplicates()
                            
                            # Para cada Veículo, encontrar o primeiro período
                            periodos_filtrados_list_total = []
                            for veiculo in periodos_com_despesas_total['Veículo'].unique():
                                periodos_veiculo_total = periodos_com_despesas_total[
                                    periodos_com_despesas_total['Veículo'] == veiculo
                                ]['Período'].unique()
                                
                                if len(periodos_veiculo_total) > 0:
                                    # Ordenar períodos pela ordem dos meses (normalizar para minúsculas)
                                    periodos_ordenados_total = sorted(
                                        periodos_veiculo_total,
                                        key=lambda x: ordem_meses_dict_total.get(str(x).lower(), 999)
                                    )
                                    primeiro_periodo_total = periodos_ordenados_total[0]
                                    
                                    # Obter índice do primeiro período na ordem
                                    idx_primeiro_total = ordem_meses_dict_total.get(str(primeiro_periodo_total).lower(), 0)
                                    
                                    # Incluir todos os meses desde o primeiro mês com despesa até o final do ano
                                    meses_para_incluir_total = ORDEM_MESES[idx_primeiro_total:]
                                    
                                    # Criar DataFrame com todos os períodos a partir do primeiro
                                    for periodo in meses_para_incluir_total:
                                        periodo_formatado = periodo.capitalize() if str(primeiro_periodo_total)[0].isupper() else periodo
                                        periodos_filtrados_list_total.append({
                                            'Veículo': veiculo,
                                            'Período': periodo_formatado
                                        })
                            
                            if periodos_filtrados_list_total:
                                periodos_filtrados_total = pd.DataFrame(periodos_filtrados_list_total)
                                
                                # Normalizar períodos antes do merge
                                df_total_merge = df_total_para_calculo.copy()
                                periodos_filtrados_total_merge = periodos_filtrados_total.copy()
                                
                                df_total_merge['Período_normalizado'] = df_total_merge['Período'].astype(str).str.lower().str.strip()
                                periodos_filtrados_total_merge['Período_normalizado'] = periodos_filtrados_total_merge['Período'].astype(str).str.lower().str.strip()
                                
                                # Fazer merge usando períodos normalizados
                                df_total_para_calculo = pd.merge(
                                    df_total_merge,
                                    periodos_filtrados_total_merge[['Veículo', 'Período_normalizado']],
                                    on=['Veículo', 'Período_normalizado'],
                                    how='inner'
                                )
                                
                                # Remover coluna temporária
                                df_total_para_calculo = df_total_para_calculo.drop(columns=['Período_normalizado'])
                                
                        
                        # 🔧 IMPORTANTE: NÃO aplicar filtro de Veículo aqui - queremos TODOS os dados de cada veículo
                        # Apenas aplicar filtros de Ano e Oficina se necessário (mas esses já devem estar aplicados em df_real_original)
                        # O filtro de Veículo será aplicado DEPOIS, quando formos filtrar o resultado final
                        
                        # Verificar se há múltiplos anos (mesma lógica do gráfico de volume)
                        tem_multiplos_anos_vol = 'Ano' in df_volume_filtrado.columns and df_volume_filtrado['Ano'].nunique() > 1
                        tem_multiplos_anos_total = 'Ano' in df_total_para_calculo.columns and df_total_para_calculo['Ano'].nunique() > 1
                        
                        if tem_multiplos_anos_vol and 'Período' in df_volume_filtrado.columns and tem_multiplos_anos_total and 'Período' in df_total_para_calculo.columns:
                            # Agrupar Total por Veículo, Período e Ano (usando dados originais)
                            df_total_agrupado = df_total_para_calculo.groupby(['Veículo', 'Período', 'Ano'])['Total'].sum().reset_index()
                            # Agrupar Volume por Veículo, Período e Ano (MESMA LÓGICA do gráfico de volume)
                            df_agrupado_periodo_vol = df_volume_filtrado.groupby(['Veículo', 'Período', 'Ano']).agg({
                                'Volume': 'sum'
                            }).reset_index()
                            # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                            df_total_agrupado_final = df_total_agrupado.groupby('Veículo')['Total'].sum().reset_index()
                            df_volume_agrupado = df_agrupado_periodo_vol.groupby('Veículo').agg({
                                'Volume': 'sum'
                            }).reset_index()
                        elif 'Período' in df_volume_filtrado.columns and 'Período' in df_total_para_calculo.columns:
                            # Agrupar Total por Veículo e Período (usando dados originais)
                            df_total_agrupado = df_total_para_calculo.groupby(['Veículo', 'Período'])['Total'].sum().reset_index()
                            # Agrupar Volume por Veículo e Período (MESMA LÓGICA do gráfico de volume)
                            df_agrupado_periodo_vol = df_volume_filtrado.groupby(['Veículo', 'Período']).agg({
                                'Volume': 'sum'
                            }).reset_index()
                            # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                            df_total_agrupado_final = df_total_agrupado.groupby('Veículo')['Total'].sum().reset_index()
                            df_volume_agrupado = df_agrupado_periodo_vol.groupby('Veículo').agg({
                                'Volume': 'sum'
                            }).reset_index()
                        else:
                            # Agrupar Total por Veículo (usando dados originais)
                            df_total_agrupado_final = df_total_para_calculo.groupby('Veículo')['Total'].sum().reset_index()
                            # Agrupar Volume por Veículo (MESMA LÓGICA do gráfico de volume)
                            df_volume_agrupado = df_volume_filtrado.groupby('Veículo').agg({
                                'Volume': 'sum'
                            }).reset_index()
                        
                        # Fazer merge
                        chart_data = pd.merge(
                            df_total_agrupado_final,
                            df_volume_agrupado,
                            on='Veículo',
                            how='left'
                        )
                        
                        # Preencher Volume faltante com 0
                        chart_data['Volume'] = chart_data['Volume'].fillna(0)
                    elif 'Volume' in df_data.columns:
                        # Fallback: usar Volume de df_data se df_real_vol não estiver disponível
                        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
                        
                        if tem_multiplos_anos:
                            df_agrupado_periodo = df_data.groupby(['Veículo', 'Período', 'Ano']).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            }).reset_index()
                            chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            }).reset_index()
                        elif 'Período' in df_data.columns:
                            df_agrupado_periodo = df_data.groupby(['Veículo', 'Período']).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            }).reset_index()
                            chart_data = df_agrupado_periodo.groupby('Veículo').agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            }).reset_index()
                        else:
                            chart_data = df_data.groupby('Veículo').agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            }).reset_index()
                    else:
                        # Se não tiver Volume, não pode calcular CPU
                        return None
                    
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
                # Fim do bloco if usar_logica_antiga
            else:
                chart_data = (
                    df_data.groupby('Veículo')[coluna].sum().reset_index()
                )
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores válidos
            if coluna not in chart_data.columns:
                return None
            
            chart_data = chart_data.sort_values(coluna, ascending=False)

            # Determinar ordem dos veículos (usar a mesma ordem para barras e linha)
            ordem_veiculos_barras = chart_data['Veículo'].tolist()

            # Processar dados de budget e calcular FLEX se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'Veículo' in df_budget.columns and df_real_vol is not None:
                if 'Custo' in df_real_para_flex.columns:
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
                        
                        # Calcular Flex Bud por Período e Veículo (mesma lógica do gráfico de Oficina)
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        
                        # Agrupar dados reais por Período e Veículo
                        if tem_ano:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Veículo', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Veículo' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Veículo' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Veículo', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Veículo' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        else:
                            if 'Custo' in df_real_para_flex.columns and 'Total' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Período', 'Veículo', 'Custo'])['Total'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Veículo' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Total' in df_budget.columns and 'Veículo' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Período', 'Veículo', 'Custo'])['Total'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Veículo' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Período', 'Veículo'])['Volume'].sum().reset_index()
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
                            
                            # Fazer merge de volumes por Período e Veículo
                            if tem_ano:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Ano', 'Período', 'Veículo'],
                                    how='left',
                                    suffixes=('_real', '_budget')
                                )
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            else:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Período', 'Veículo'],
                                    how='left',
                                    suffixes=('_real', '_budget')
                                )
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            
                            # Calcular FLEX para cada Período e Veículo (mesma lógica do gráfico de Oficina)
                            flex_data = []
                            for _, vol_row in volumes.iterrows():
                                if tem_ano:
                                    ano = vol_row['Ano']
                                    periodo = vol_row['Período']
                                    veiculo = vol_row['Veículo']
                                else:
                                    periodo = vol_row['Período']
                                    veiculo = vol_row['Veículo']
                                
                                volume_real = vol_row['Volume_real']
                                volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                                
                                if volume_real == 0 or pd.isna(volume_real):
                                    continue
                                
                                # Obter custos reais para este Período e Veículo
                                if tem_ano:
                                    custos_real = real_agrupado[
                                        (real_agrupado['Ano'] == ano) & 
                                        (real_agrupado['Período'] == periodo) &
                                        (real_agrupado['Veículo'] == veiculo)
                                    ]
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Ano'] == ano) & 
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Veículo'] == veiculo)
                                    ]
                                else:
                                    custos_real = real_agrupado[
                                        (real_agrupado['Período'] == periodo) &
                                        (real_agrupado['Veículo'] == veiculo)
                                    ]
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Veículo'] == veiculo)
                                    ]
                                
                                # Se não houver dados de budget para este veículo, usar zeros
                                if len(custos_budget) == 0:
                                    budget_total = 0
                                    custo_fixo_budget = 0
                                    custo_variavel_budget = 0
                                else:
                                    budget_total = custos_budget['Total'].sum()
                                    custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Total'].sum()
                                    custo_variavel_budget = custos_budget[custos_budget['Custo'] == 'Variável']['Total'].sum()
                                
                                # 🔧 CORREÇÃO: Calcular Flex Bud Total (Custo Total) para este período e veículo
                                # Flex Bud Fixo = BUD Fixo (não varia com volume)
                                flex_bud_fixo = custo_fixo_budget
                                # Flex Bud Variável = BUD Variável × (Volume Real / Volume Budget)
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_variavel = custo_variavel_budget * proporcao_volume_real_bud
                                # Flex Bud Total (em Custo Total) = Flex Bud Fixo + Flex Bud Variável
                                flex_bud_total_custo_total = flex_bud_fixo + flex_bud_variavel
                                
                                # Adicionar ao flex_data com Veículo (armazenar Custo Total, não CPU)
                                if tem_ano:
                                    flex_data.append({
                                        'Ano': ano,
                                        'Período': periodo,
                                        'Veículo': veiculo,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                                else:
                                    flex_data.append({
                                        'Período': periodo,
                                        'Veículo': veiculo,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                            
                            if len(flex_data) == 0:
                                flex_data = None
                            else:
                                flex_data = pd.DataFrame(flex_data)
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud Total e Volume Real por Veículo (somar todos os períodos)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Para CPU: somar Flex Bud Total e Volume Real, depois recalcular CPU
                                budget_data = flex_data.groupby('Veículo').agg({
                                    'Flex_Bud_Total': 'sum',
                                    'Volume_Real': 'sum'
                                }).reset_index()
                                
                                # Recalcular CPU: Flex Bud Total agregado / Volume Real agregado
                                budget_data[coluna] = budget_data.apply(
                                    lambda row: (
                                        row['Flex_Bud_Total'] / row['Volume_Real']
                                        if pd.notnull(row['Volume_Real']) and row['Volume_Real'] != 0
                                        else 0
                                    ),
                                    axis=1
                                )
                                
                                # Manter apenas colunas necessárias
                                budget_data = budget_data[['Veículo', coluna]]
                            else:
                                # Para Custo Total: apenas somar Flex Bud Total
                                budget_data = flex_data.groupby('Veículo')['Flex_Bud_Total'].sum().reset_index()
                                budget_data.rename(columns={'Flex_Bud_Total': coluna}, inplace=True)
                            
                            # Filtrar apenas veículos que existem no chart_data
                            budget_data = budget_data[budget_data['Veículo'].isin(chart_data['Veículo'])].copy()
                            
                            if len(budget_data) > 0:
                                    # Criar linha tracejada de Flex Bud
                                    budget_data_legenda = budget_data.copy()
                                    budget_data_legenda['Tipo'] = 'Flex Bud'
                                    
                                    # IMPORTANTE: Usar a mesma ordem das barras (ordem_veiculos_barras)
                                    # Reordenar budget_data_legenda para seguir a ordem de ordem_veiculos_barras
                                    ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos_barras)}
                                    budget_data_legenda['_ordem'] = budget_data_legenda['Veículo'].map(ordem_dict)
                                    budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                                    budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                                    budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                                    
                                    # Usar a mesma ordem das barras (filtrando apenas veículos que existem no budget)
                                    ordem_veiculos = [v for v in ordem_veiculos_barras if v in budget_data_legenda['Veículo'].tolist()]
                                    
                                    linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                        strokeDash=[10, 5],
                                        strokeWidth=1.5,
                                        opacity=0.8
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
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip('Tipo:N', title='Tipo'),
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
                                        opacity=0.9
                                    ).encode(
                                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                                        y=alt.Y(f'{coluna}:Q'),
                                        color=alt.Color(
                                            'Tipo:N',
                                            scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                            legend=None
                                        ),
                                        tooltip=[
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip('Tipo:N', title='Tipo'),
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

            # Usar a ordem explícita para garantir sincronização com a linha pontilhada
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    'Veículo:N',
                    title='Veículo',
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
                    alt.Tooltip('Veículo:N', title='Veículo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato
                    )
                ]
            ).properties(
                height=192,  # Reduzido em 20% para achatamento (Total por Veículo)
                width='container'  # Usar largura do container para achatamento
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
                height=192,  # Reduzido em 20% para achatamento (Total por Veículo)
                width='container'  # Usar largura do container para achatamento
            )

        # Adicionar rótulos
        if 'Veículo' in df_data.columns:
            # Usar a ordem explícita para garantir sincronização
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                x=alt.X('Veículo:N', sort=ordem_veiculos_barras, title='Veículo'),
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
                
                # Usar a mesma ordem das barras para manter consistência
                ordem_veiculos_delta = ordem_veiculos_barras
                
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
                    height=38
                )
                
                # Adicionar rótulos de dados no gráfico de delta
                # Usar a mesma cor das barras (escala baseada no valor do Delta)
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
                    x=alt.X('Veículo:N', sort=ordem_veiculos_delta),
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
        import traceback
        st.error(f"Erro ao criar gráfico Total por Veículo: {e}")
        st.error(traceback.format_exc())
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
    
    # Verificar se há dados para exibir
    if df_visualizacao is None or len(df_visualizacao) == 0:
        st.info("ℹ️ Nenhum dado disponível com os filtros selecionados. Tente ajustar os filtros na barra lateral.")
    else:
        # Exibir gráfico por Oficina
        if ('Oficina' in df_visualizacao.columns and
                coluna_visualizacao in df_visualizacao.columns):
            # Título para ambos os modos
            if tipo_visualizacao == "Custo Total":
                st.subheader("📊 Soma do Valor por Oficina")
            elif tipo_visualizacao == "CPU (Custo por Unidade)":
                st.subheader("📊 CPU por Oficina")
            try:
                grafico_oficina = create_oficina_chart(
                    df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo,
                    df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3
                )
                if grafico_oficina:
                    st.altair_chart(grafico_oficina, use_container_width=True)
                else:
                    st.warning(f"⚠️ O gráfico de Oficina não pôde ser criado. Verifique se há dados de Oficina disponíveis e se a coluna '{coluna_visualizacao}' contém valores válidos.")
            except Exception as e:
                import traceback
                st.error(f"❌ Erro ao criar gráfico de Oficina: {e}")
                st.error(traceback.format_exc())
    
        # Exibir gráfico de Total/CPU por Veículo
        if 'Veículo' in df_visualizacao.columns:
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                if coluna_visualizacao in df_visualizacao.columns:
                    st.subheader("📊 Total por Veículo")
                    # 🔧 CORREÇÃO: Passar df_filtrado e df_vol_filtrado para usar EXATAMENTE os mesmos valores dos gráficos
                    grafico_total = create_total_chart(
                        df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo,
                        df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3, df_visualizacao, df_total, df_visualizacao,
                        df_filtrado if 'df_filtrado' in locals() else None,
                        df_vol_filtrado if 'df_vol_filtrado' in locals() else None
                    )
                    if grafico_total:
                        st.altair_chart(grafico_total, use_container_width=True)
                    else:
                        st.warning(f"⚠️ O gráfico não pôde ser criado. Verifique se a coluna '{coluna_visualizacao}' contém dados válidos e se há dados de Veículo disponíveis.")
            elif tipo_visualizacao == "Custo Total":
                if 'df_filtrado' in locals() and df_filtrado is not None and 'Total' in df_filtrado.columns:
                    st.subheader("📊 Total por Veículo")
                    grafico_total = create_total_chart(
                        df_filtrado, 'Total', tipo_visualizacao, moeda_simbolo,
                        df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3, None, df_total, df_visualizacao
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
                        df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3, df_visualizacao, df_total, df_visualizacao
                    )
                    if grafico_total:
                        st.altair_chart(grafico_total, use_container_width=True)
            elif tipo_visualizacao == "Custo Total":
                if 'df_filtrado' in locals() and df_filtrado is not None and 'Total' in df_filtrado.columns:
                    st.subheader("📊 Total por Período")
                    grafico_total = create_total_chart(
                        df_filtrado, 'Total', tipo_visualizacao, moeda_simbolo,
                        df_budget_filtrado_tab3, df_budget_vol_filtrado_tab3, df_volume_real_filtrado_tab3, df_real_original_grafico_tab3, None, df_total, df_visualizacao
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
