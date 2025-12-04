import streamlit as st
import pandas as pd
import altair as alt
import os
import sqlite3
from datetime import datetime
import sqlite3
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Dashboard TC - KE5Z Group",
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
        /* Estilos para botões: reduzir fonte e aproximar */
        .stButton > button {
            font-size: 0.85rem !important;
            padding: 0.4rem 1rem !important;
            margin-bottom: 0.3rem !important;
        }
    </style>
""", unsafe_allow_html=True)

# Título
st.title("📊 Dashboard TC - KE5Z Group")
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
if 'filtro_ano' not in st.session_state:
    st.session_state.filtro_ano = opcoes_ano[index_padrao] if index_padrao < len(opcoes_ano) else "Todos"

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=opcoes_ano.index(st.session_state.filtro_ano) if st.session_state.filtro_ano in opcoes_ano else index_padrao,
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico",
    key="filtro_ano_selectbox"
)
# Atualizar session_state
st.session_state.filtro_ano = ano_selecionado

st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Filtros**")

# Função para carregar dados com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet"""
    try:
        # Converter "Todos" para None
        ano_para_busca = None if ano_selecionado_param == "Todos" else ano_selecionado_param
        
        # Buscar arquivo na ordem de prioridade
        arquivo_parquet = encontrar_arquivo_parquet("df_ke5z_group.parquet", ano_para_busca)

        if arquivo_parquet is None:
            st.error(f"❌ Arquivo não encontrado: df_ke5z_group.parquet")
            st.info("💡 Verifique se o arquivo existe em:")
            st.info("   - dados/historico_consolidado/df_ke5z_historico.parquet")
            st.info("   - dados/{ANO}/df_ke5z_group.parquet")
            st.info("   - df_ke5z_group.parquet (raiz)")
            st.stop()

        # Carregar dados
        df = pd.read_parquet(arquivo_parquet)

        # Se carregou do histórico consolidado e um ano específico foi selecionado, filtrar
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
    """Carrega os dados de volume do arquivo parquet"""
    try:
        # Converter "Todos" para None
        ano_para_busca = None if ano_selecionado_param == "Todos" else ano_selecionado_param
        
        # Buscar arquivo na ordem de prioridade
        arquivo_parquet = encontrar_arquivo_parquet("df_vol.parquet", ano_para_busca)

        if arquivo_parquet is None:
            return None

        df = pd.read_parquet(arquivo_parquet)

        # Se carregou do histórico consolidado e um ano específico foi selecionado, filtrar
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

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
    st.sidebar.success("✅ Dados carregados com sucesso")
    if ano_selecionado == "Todos":
        st.sidebar.info(f"📊 {len(df_total):,} registros (Todos os anos)")
    else:
        st.sidebar.info(f"📊 {len(df_total):,} registros (Ano {ano_selecionado})")
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
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


# Inicializar session_state para tipo de visualização e moeda
if 'filtro_tipo_visualizacao' not in st.session_state:
    st.session_state.filtro_tipo_visualizacao = "Custo Total"
if 'filtro_moeda' not in st.session_state:
    st.session_state.filtro_moeda = "🇧🇷 R$"

# Seletor de tipo de visualização
st.sidebar.markdown("**📊 Tipo de Visualização**")
tipo_visualizacao = st.sidebar.radio(
    "Selecione o tipo:",
    ["Custo Total", "CPU (Custo por Unidade)"],
    index=0 if st.session_state.filtro_tipo_visualizacao == "Custo Total" else 1,
    key="filtro_tipo_visualizacao_radio"
)
# Atualizar session_state
st.session_state.filtro_tipo_visualizacao = tipo_visualizacao

# Seletor de moeda
st.sidebar.markdown("**💱 Moeda**")
moeda_selecionada = st.sidebar.radio(
    "Selecione a moeda:",
    ["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"],
    index=["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"].index(st.session_state.filtro_moeda) if st.session_state.filtro_moeda in ["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"] else 0,
    help="Selecione a moeda para exibição nos gráficos",
    key="filtro_moeda_radio"
)
# Atualizar session_state
st.session_state.filtro_moeda = moeda_selecionada

# Extrair código e símbolo da moeda
if moeda_selecionada == "🇧🇷 R$":
    moeda_codigo = "BRL"
    moeda_simbolo = "R$"
elif moeda_selecionada == "🇺🇸 $":
    moeda_codigo = "USD"
    moeda_simbolo = "$"
else:  # Euro
    moeda_codigo = "EUR"
    moeda_simbolo = "€"

# ====================================================================
# FUNÇÕES DE CONVERSÃO DE MOEDA
# ====================================================================
def inicializar_banco_taxas():
    """Inicializa o banco de dados de taxas de câmbio"""
    conn = sqlite3.connect('taxas_cambio.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS taxas_cambio (
            moeda TEXT PRIMARY KEY,
            taxa_para_brl REAL,
            data_atualizacao TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

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

def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino
    
    Exemplo: Se 1 USD = 5 BRL, então:
    - taxa_usd_para_brl = 5.0
    - taxa_brl_para_usd = 1/5 = 0.20
    - Para converter 100 BRL para USD: 100 * 0.20 = 20 USD
    - Ou seja: 100 / 5 = 20 USD (divisão)
    """
    if valor is None or pd.isna(valor):
        return valor
    if moeda_destino == "BRL":
        return valor
    taxa = taxas.get(moeda_destino, 1.0)
    # A taxa já está calculada como 1 BRL = X moeda_destino
    # Então multiplicamos: valor_brl * taxa = valor_moeda_destino
    resultado = valor * taxa
    return resultado

def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda"""
    if coluna not in df.columns:
        return df
    if moeda_destino == "BRL":
        return df
    df = df.copy()
    df[coluna] = df[coluna].apply(lambda x: converter_moeda(x, moeda_destino, taxas))
    return df

# ====================================================================
# CONFIGURAÇÃO DE TAXAS DE CÂMBIO
# ====================================================================
st.sidebar.markdown("---")
st.sidebar.markdown("**💱 Taxas de Câmbio**")
st.sidebar.markdown("*Configure as taxas de conversão*")

# Carregar taxas do banco de dados
try:
    taxas_cambio_banco = carregar_taxas_banco()
except Exception as e:
    st.sidebar.warning(f"⚠️ Erro ao carregar taxas: {e}")
    taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

# Taxas de conversão: entrada em "1 $ = R$ X" e "1 € = R$ X"
taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

taxa_usd_para_brl = st.sidebar.number_input(
    "🇺🇸 1 $ (USD) = R$",
    min_value=0.01,
    max_value=100.0,
    value=float(taxa_usd_para_brl_padrao),
    step=0.01,
    format="%.2f",
    help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros",
    key="taxa_usd_para_brl_input"
)

taxa_eur_para_brl = st.sidebar.number_input(
    "🇪🇺 1 € (EUR) = R$",
    min_value=0.01,
    max_value=100.0,
    value=float(taxa_eur_para_brl_padrao),
    step=0.01,
    format="%.2f",
    help="Digite quanto vale 1 Euro em Reais Brasileiros",
    key="taxa_eur_para_brl_input"
)

# Calcular taxas inversas para conversão (1 R$ = X USD/EUR)
# Exemplo: Se 1 USD = 5 BRL, então 1 BRL = 1/5 = 0.20 USD
# Para converter 100 BRL para USD: 100 * 0.20 = 20 USD (ou seja, 100 / 5 = 20)
taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

# Debug: mostrar exemplo de conversão
st.sidebar.caption(f"💡 Exemplo: R$ 100 = {100 * taxa_brl_para_usd:.2f} USD (taxa: {taxa_usd_para_brl:.2f})")

# Salvar taxas quando alteradas
if (taxa_usd_para_brl != taxa_usd_para_brl_padrao or 
    taxa_eur_para_brl != taxa_eur_para_brl_padrao):
    novas_taxas = {
        "USD": float(taxa_usd_para_brl),
        "EUR": float(taxa_eur_para_brl)
    }
    salvar_taxas_banco(novas_taxas)

# Armazenar taxas em dicionário (para conversão: 1 R$ = X USD/EUR)
# IMPORTANTE: Estas taxas são para MULTIPLICAR valores em BRL
# Exemplo: Se taxa_brl_para_usd = 0.20, então 100 BRL * 0.20 = 20 USD
# Isso é equivalente a: 100 BRL / 5 = 20 USD (onde 5 é taxa_usd_para_brl)
taxas_cambio = {
    "BRL": 1.0,  # Real é a moeda base
    "USD": taxa_brl_para_usd,  # Ex: 0.20 (se 1 USD = 5 BRL, então 1 BRL = 0.20 USD)
    "EUR": taxa_brl_para_eur   # Ex: 0.18 (se 1 EUR = 5.50 BRL, então 1 BRL = 0.18 EUR)
}

# Teste de validação da conversão
if moeda_codigo != "BRL":
    valor_teste = 100.0
    valor_convertido = converter_moeda(valor_teste, moeda_codigo, taxas_cambio)
    if moeda_codigo == "USD":
        taxa_esperada = taxa_usd_para_brl
        valor_esperado = valor_teste / taxa_esperada
    else:  # EUR
        taxa_esperada = taxa_eur_para_brl
        valor_esperado = valor_teste / taxa_esperada
    
    # Mostrar teste de validação
    st.sidebar.caption(f"✅ Teste: R$ {valor_teste:,.2f} = {moeda_simbolo} {valor_convertido:,.2f} (taxa: {taxa_esperada:.2f})")

# Mostrar indicador da moeda selecionada
st.sidebar.markdown(f"**💱 Moeda atual:** {moeda_selecionada} ({moeda_simbolo})")
st.sidebar.markdown("---")

# Inicializar session_state para filtros
if 'filtro_oficina' not in st.session_state:
    st.session_state.filtro_oficina = ["Todos"]

# Filtro 1: Oficina (com cache otimizado)
if 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    # Validar valores salvos
    default_oficina = st.session_state.filtro_oficina if all(x in oficina_opcoes for x in st.session_state.filtro_oficina) else ["Todos"]
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=default_oficina, key="filtro_oficina_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_oficina = oficina_selecionadas if oficina_selecionadas else ["Todos"]

    # Filtrar o DataFrame com base na Oficina
    if "Todos" in oficina_selecionadas or not oficina_selecionadas:
        df_filtrado = df_total.copy()
    else:
        df_filtrado = df_total[
            df_total['Oficina'].astype(str).isin(oficina_selecionadas)
        ].copy()
else:
    df_filtrado = df_total.copy()

# Inicializar session_state para USI
if 'filtro_usi' not in st.session_state:
    if 'USI' in df_total.columns:
        usi_opcoes_temp = get_filter_options(df_total, 'USI')
        st.session_state.filtro_usi = ["TC Ext"] if "TC Ext" in usi_opcoes_temp else ["Todos"]
    else:
        st.session_state.filtro_usi = ["Todos"]

# Filtro 2: USI (com cache otimizado)
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    # Validar valores salvos
    default_usi = st.session_state.filtro_usi if all(x in usi_opcoes for x in st.session_state.filtro_usi) else (["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"])
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_usi = usi_selecionada if usi_selecionada else ["Todos"]

    # Filtrar o DataFrame com base na USI
    if "Todos" in usi_selecionada or not usi_selecionada:
        pass  # Manter df_filtrado como está
    else:
        df_filtrado = df_filtrado[
            df_filtrado['USI'].astype(str).isin(usi_selecionada)
        ].copy()

# Filtro 3: Período (com cache otimizado)
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
    if 'filtro_periodo' not in st.session_state:
        st.session_state.filtro_periodo = "Todos"
    
    # Validar valor salvo
    periodo_default = st.session_state.filtro_periodo if st.session_state.filtro_periodo in periodo_opcoes else "Todos"
    periodo_index = periodo_opcoes.index(periodo_default) if periodo_default in periodo_opcoes else 0
    
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes, index=periodo_index, key="filtro_periodo_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_periodo = periodo_selecionado
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Inicializar session_state para Centro cst
if 'filtro_centro_cst' not in st.session_state:
    st.session_state.filtro_centro_cst = "Todos"

# Filtro 4: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    # Validar valor salvo
    centro_cst_default = st.session_state.filtro_centro_cst if st.session_state.filtro_centro_cst in centro_cst_opcoes else "Todos"
    centro_cst_index = centro_cst_opcoes.index(centro_cst_default) if centro_cst_default in centro_cst_opcoes else 0
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes, index=centro_cst_index, key="filtro_centro_cst_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_centro_cst = centro_cst_selecionado
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Inicializar session_state para Conta contábil
if 'filtro_conta_contabil' not in st.session_state:
    st.session_state.filtro_conta_contabil = []

# Filtro 5: Conta contábil (com cache otimizado)
if 'Nºconta' in df_filtrado.columns:
    conta_contabil_opcoes = get_filter_options(df_filtrado, 'Nºconta')[1:]
    # Validar valores salvos
    default_conta = [x for x in st.session_state.filtro_conta_contabil if x in conta_contabil_opcoes] if st.session_state.filtro_conta_contabil else []
    conta_contabil_selecionadas = st.sidebar.multiselect(
        "Selecione a Conta contábil:", conta_contabil_opcoes, default=default_conta, key="filtro_conta_contabil_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_conta_contabil = conta_contabil_selecionadas
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
        filtro_key = f'filtro_{col_name}'
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
                filtro_key = f'filtro_avancado_{col_name}'
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
        # Agrupar df_filtrado por Oficina e Período para calcular Valor total
        if ('Oficina' in df_filtrado.columns and
                'Período' in df_filtrado.columns):
            # Aplicar conversão de moeda ANTES de agrupar (se necessário)
            df_filtrado_para_cpu = df_filtrado.copy()
            if moeda_codigo != "BRL" and 'Valor' in df_filtrado_para_cpu.columns:
                df_filtrado_para_cpu = converter_coluna_moeda(df_filtrado_para_cpu, 'Valor', moeda_codigo, taxas_cambio)
            
            # Agrupar Valor por Oficina e Período
            df_valor_agrupado = df_filtrado_para_cpu.groupby(
                ['Oficina', 'Período'], as_index=False
            )['Valor'].sum()

            # Agrupar Volume por Oficina e Período do df_vol
            df_vol_agrupado = df_vol_calc.groupby(
                ['Oficina', 'Período'], as_index=False
            )['Volume'].sum()

            # Fazer merge
            df_cpu = pd.merge(
                df_valor_agrupado,
                df_vol_agrupado,
                on=['Oficina', 'Período'],
                how='left'
            )

            # Calcular CPU (evitando divisão por zero)
            df_cpu['CPU'] = df_cpu.apply(
                lambda row: (
                    row['Valor'] / row['Volume']
                    if pd.notnull(row['Volume']) and row['Volume'] != 0
                    else 0
                ),
                axis=1
            )

            # Criar DataFrame para visualização com CPU
            # Nota: A conversão já foi aplicada no Valor antes do cálculo do CPU
            df_visualizacao = df_cpu.copy()
            df_visualizacao['Valor'] = df_visualizacao['CPU']
            coluna_visualizacao = 'CPU'
        else:
            st.warning(
                "⚠️ Colunas 'Oficina' e 'Período' necessárias para "
                "calcular CPU"
            )
            df_visualizacao = df_filtrado.copy()
            coluna_visualizacao = 'Valor'
            tipo_visualizacao = "Custo Total"
    else:
        st.warning(
            "⚠️ Dados de volume não disponíveis. "
            "Mostrando Custo Total."
        )
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Valor'
        tipo_visualizacao = "Custo Total"
else:
    # Usar Valor ou Total diretamente
    df_visualizacao = df_filtrado.copy()
    # Verificar qual coluna existe e tem dados
    if 'Total' in df_filtrado.columns and df_filtrado['Total'].notna().any():
        coluna_visualizacao = 'Total'
    elif 'Valor' in df_filtrado.columns and df_filtrado['Valor'].notna().any():
        coluna_visualizacao = 'Valor'
    elif 'Total' in df_filtrado.columns:
        coluna_visualizacao = 'Total'
    else:
        coluna_visualizacao = 'Valor'
    
    # Aplicar conversão de moeda se necessário
    if moeda_codigo != "BRL":
        # Debug: mostrar valores antes da conversão
        if coluna_visualizacao in df_visualizacao.columns:
            valor_antes = df_visualizacao[coluna_visualizacao].sum()
            df_visualizacao = converter_coluna_moeda(df_visualizacao, coluna_visualizacao, moeda_codigo, taxas_cambio)
            valor_depois = df_visualizacao[coluna_visualizacao].sum()
            # Mostrar exemplo de conversão na sidebar
            if len(df_visualizacao) > 0:
                exemplo_valor = df_visualizacao[coluna_visualizacao].iloc[0] if not df_visualizacao[coluna_visualizacao].isna().all() else 0
                if exemplo_valor != 0:
                    st.sidebar.caption(f"📊 Exemplo: {coluna_visualizacao} convertido de R$ para {moeda_simbolo}")
        # Também converter outras colunas numéricas se existirem
        if 'Total' in df_visualizacao.columns and coluna_visualizacao != 'Total':
            df_visualizacao = converter_coluna_moeda(df_visualizacao, 'Total', moeda_codigo, taxas_cambio)
        if 'Valor' in df_visualizacao.columns and coluna_visualizacao != 'Valor':
            df_visualizacao = converter_coluna_moeda(df_visualizacao, 'Valor', moeda_codigo, taxas_cambio)

# Resumo na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Resumo**")
st.sidebar.write(f"**Linhas:** {df_filtrado.shape[0]:,}")

# Calcular totais se as colunas existirem (usar df_visualizacao que já tem conversão aplicada)
if coluna_visualizacao in df_visualizacao.columns:
    valor_total = df_visualizacao[coluna_visualizacao].sum()
    st.sidebar.write(f"**Total {coluna_visualizacao}:** {moeda_simbolo} {valor_total:,.2f}")
elif 'Valor' in df_filtrado.columns:
    valor_total = df_filtrado['Valor'].sum()
    # Aplicar conversão se necessário
    if moeda_codigo != "BRL":
        valor_total = converter_moeda(valor_total, moeda_codigo, taxas_cambio)
    st.sidebar.write(f"**Total Valor:** {moeda_simbolo} {valor_total:,.2f}")
if 'Total' in df_filtrado.columns:
    total_sum = df_filtrado['Total'].sum()
    # Aplicar conversão se necessário
    if moeda_codigo != "BRL":
        total_sum = converter_moeda(total_sum, moeda_codigo, taxas_cambio)
    st.sidebar.write(f"**Total:** {moeda_simbolo} {total_sum:,.2f}")
if 'Volume' in df_filtrado.columns:
    volume_total = df_filtrado['Volume'].sum()
    st.sidebar.write(f"**Total Volume:** {volume_total:,.2f}")

# Mostrar tipo de visualização selecionado
st.sidebar.info(f"📈 **Visualizando:** {tipo_visualizacao}")


def ordenar_por_mes(df, coluna_periodo='Período'):
    """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
    df_copy = df.copy()
    
    # Se houver coluna "Ano" e múltiplos anos, ordenar por ano e mês
    if 'Ano' in df_copy.columns and df_copy['Ano'].nunique() > 1:
        # Criar coluna de ordenação: ano primeiro, depois mês
        df_copy['_ordem_ano'] = df_copy['Ano']
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
        df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
    else:
        # Ordenação simples por mês (comportamento original)
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).fillna(999)
        df_copy = df_copy.sort_values('_ordem_mes')
        df_copy = df_copy.drop(columns=['_ordem_mes'])
    
    return df_copy


# Gráfico 1: Soma do Valor por Período
def create_period_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$"):
    """Cria gráfico de barras por Período"""
    try:
        if df_data is None or df_data.empty:
            st.error(f"❌ Debug: df_data está vazio ou None. Moeda: {moeda_simbolo}")
            return None
            
        if coluna not in df_data.columns:
            st.error(f"❌ Debug: Coluna '{coluna}' não encontrada. Colunas disponíveis: {list(df_data.columns)[:10]}. Moeda: {moeda_simbolo}")
            return None

        # Detectar tema do Streamlit
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        axis_color = "#FAFAFA" if theme_base == "dark" else "#000000"

        # Verificar se a coluna Período existe
        if 'Período' not in df_data.columns:
            return None
        
        # Verificar se há múltiplos anos
        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        
        if tem_multiplos_anos:
            # Agrupar por Ano e Período
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
            chart_data = df_data.groupby('Período')[coluna].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'
        
        # Verificar se há dados após agrupamento
        if chart_data.empty:
            return None
        
        # Garantir que os valores sejam numéricos
        chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce').fillna(0)
        
        # Verificar se há pelo menos um período com dados
        if len(chart_data) == 0:
            return None
        
        # Remover períodos com valores NaN ou infinitos
        chart_data = chart_data[chart_data[coluna].notna() & chart_data[coluna].ne(float('inf')) & chart_data[coluna].ne(float('-inf'))]
        
        if chart_data.empty:
            return None

        # Definir título do eixo Y baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Período"
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por Período"
        
        # Debug: verificar dados antes de criar gráfico
        if len(chart_data) == 0:
            st.error(f"❌ Debug: chart_data está vazio após agrupamento. Moeda: {moeda_simbolo}, Coluna: {coluna}")
            return None

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos,
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            y=alt.Y(
                f'{coluna}:Q',
                title=titulo_y,
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color),
                scale=alt.Scale(zero=True)
            ),
            color=alt.Color(
                f'{coluna}:Q',
                title=coluna,
                scale=alt.Scale(scheme='redyellowgreen', reverse=True)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip(
                    f'{coluna}:Q',
                    title=coluna,
                    format=',.4f' if tipo_viz == "CPU (Custo por Unidade)"
                    else ',.2f'
                )
            ]
        ).properties(
            title=titulo_grafico,
            height=400
        )

        # Adicionar rótulos com valores nas barras
        formato_rotulo = (
            ',.4f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
        )
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color=text_color,
            fontSize=12
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico por Período
# Verificar se df_visualizacao existe e tem dados
if df_visualizacao is not None and not df_visualizacao.empty and coluna_visualizacao in df_visualizacao.columns:
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📊 CPU por Período")
        
        # Filtros específicos para este gráfico
        df_grafico_periodo = df_visualizacao.copy()
        
        # Criar colunas para os filtros
        col1, col2 = st.columns(2)
        
        # Filtro de Veículo
        with col1:
            if 'Veículo' in df_grafico_periodo.columns:
                veiculo_opcoes_grafico = sorted(
                    df_grafico_periodo['Veículo'].dropna().astype(str).unique().tolist()
                )
                veiculo_selecionado_grafico = st.selectbox(
                    "🚗 Filtrar por Veículo:",
                    ["Todos"] + veiculo_opcoes_grafico,
                    key="filtro_veiculo_grafico_periodo"
                )
                if veiculo_selecionado_grafico != "Todos":
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Veículo'].astype(str) == str(veiculo_selecionado_grafico)
                    ].copy()
        
        # Filtro de Oficina
        with col2:
            if 'Oficina' in df_grafico_periodo.columns:
                oficina_opcoes_grafico = sorted(
                    df_grafico_periodo['Oficina'].dropna().astype(str).unique().tolist()
                )
                oficina_selecionada_grafico = st.selectbox(
                    "🏭 Filtrar por Oficina:",
                    ["Todos"] + oficina_opcoes_grafico,
                    key="filtro_oficina_grafico_periodo"
                )
                if oficina_selecionada_grafico != "Todos":
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Oficina'].astype(str) == str(oficina_selecionada_grafico)
                    ].copy()
        
        # Verificar se há dados após filtros
        if df_grafico_periodo.empty:
            st.warning("⚠️ Nenhum dado encontrado após aplicar os filtros de Veículo e Oficina.")
            grafico_periodo = None
        else:
            # Criar gráfico com dados filtrados
            grafico_periodo = create_period_chart(
                df_grafico_periodo, coluna_visualizacao, tipo_visualizacao, moeda_simbolo
            )
    else:
        st.subheader("📊 Soma do Valor por Período")
        # Verificar se há dados
        if df_visualizacao.empty:
            st.warning("⚠️ Nenhum dado disponível para exibir.")
            grafico_periodo = None
        else:
            grafico_periodo = create_period_chart(
                df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo
            )
    
    # Sempre mostrar debug quando o gráfico não aparecer
    if grafico_periodo is not None:
        try:
            st.altair_chart(grafico_periodo, use_container_width=True)
        except Exception as e:
            st.error(f"❌ Erro ao exibir gráfico: {e}")
            grafico_periodo = None
    
    # Mostrar debug se o gráfico não foi criado ou se não há dados suficientes
    if grafico_periodo is None:
        # Debug: mostrar informações sobre os dados
        st.markdown("---")
        st.markdown("### 🔍 Debug - Informações dos dados")
        st.write(f"**Moeda selecionada:** {moeda_selecionada} ({moeda_simbolo})")
        st.write(f"**Tipo de visualização:** {tipo_visualizacao}")
        st.write(f"**Coluna de visualização:** {coluna_visualizacao}")
        
        if df_visualizacao is not None and not df_visualizacao.empty:
            st.write(f"**Colunas disponíveis:** {', '.join(df_visualizacao.columns.tolist()[:20])}")
            st.write(f"**Linhas no DataFrame:** {len(df_visualizacao):,}")
            
            if 'Período' in df_visualizacao.columns:
                periodos_unicos = df_visualizacao['Período'].dropna().unique()
                st.write(f"**Períodos únicos:** {len(periodos_unicos)}")
                if len(periodos_unicos) > 0:
                    st.write(f"**Períodos:** {', '.join(periodos_unicos.astype(str).tolist()[:15])}")
            
            if coluna_visualizacao in df_visualizacao.columns:
                valores_nao_nulos = df_visualizacao[coluna_visualizacao].notna().sum()
                soma_valores = df_visualizacao[coluna_visualizacao].sum()
                st.write(f"**Valores não nulos na coluna '{coluna_visualizacao}':** {valores_nao_nulos:,}")
                st.write(f"**Soma da coluna '{coluna_visualizacao}':** {moeda_simbolo} {soma_valores:,.2f}")
                
                # Mostrar amostra dos dados
                if len(df_visualizacao) > 0:
                    st.write("**Amostra dos dados (primeiras 5 linhas):**")
                    colunas_mostrar = ['Período', coluna_visualizacao]
                    if 'Ano' in df_visualizacao.columns:
                        colunas_mostrar.insert(1, 'Ano')
                    st.dataframe(df_visualizacao[colunas_mostrar].head(), use_container_width=True)
            else:
                st.error(f"❌ A coluna '{coluna_visualizacao}' não existe no DataFrame!")
        else:
            st.error("❌ O DataFrame está vazio ou não foi criado!")
        
        st.warning("⚠️ Não há dados para exibir no gráfico. Verifique os filtros selecionados.")
else:
    st.warning(f"⚠️ A coluna '{coluna_visualizacao}' não está disponível nos dados filtrados.")
    if df_visualizacao is not None and not df_visualizacao.empty:
        st.write(f"**Colunas disponíveis:** {', '.join(df_visualizacao.columns.tolist())}")


# Gráfico 2: Soma do Valor por Oficina
def create_oficina_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$"):
    """Cria gráfico de barras por Oficina"""
    try:
        if (coluna not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

        # Detectar tema do Streamlit
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        axis_color = "#FAFAFA" if theme_base == "dark" else "#000000"

        chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
        chart_data = chart_data.sort_values(coluna, ascending=False)

        # Definir título do eixo Y baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Oficina"
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por Oficina"

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Oficina:N',
                title='Oficina',
                sort='-y',
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            y=alt.Y(
                f'{coluna}:Q',
                title=titulo_y,
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            color=alt.Color(
                f'{coluna}:Q',
                title=coluna,
                scale=alt.Scale(scheme='redyellowgreen', reverse=True)
            ),
            tooltip=[
                alt.Tooltip('Oficina:N', title='Oficina'),
                alt.Tooltip(
                    f'{coluna}:Q',
                    title=coluna,
                    format=',.4f' if tipo_viz == "CPU (Custo por Unidade)"
                    else ',.2f'
                )
            ]
        ).properties(
            title=titulo_grafico,
            height=400
        )

        # Adicionar rótulos com valores nas barras
        formato_rotulo = (
            ',.4f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
        )
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color=text_color,
            fontSize=12
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico por Oficina
if ('Oficina' in df_visualizacao.columns and
        coluna_visualizacao in df_visualizacao.columns):
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📊 CPU por Oficina")
    else:
        st.subheader("📊 Soma do Valor por Oficina")
    grafico_oficina = create_oficina_chart(
        df_visualizacao, coluna_visualizacao, tipo_visualizacao, moeda_simbolo
    )
    if grafico_oficina:
        st.altair_chart(grafico_oficina, use_container_width=True)


# Gráfico 3: Volume por Período (se coluna Volume existir)
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data):
    """Cria gráfico de barras de Volume por Período"""
    try:
        if 'Volume' not in df_data.columns or 'Período' not in df_data.columns:
            return None

        # Detectar tema do Streamlit
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        axis_color = "#FAFAFA" if theme_base == "dark" else "#000000"

        # Verificar se há múltiplos anos
        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        
        if tem_multiplos_anos:
            # Agrupar por Ano e Período
            chart_data = df_data.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            
            # Criar coluna combinada para o rótulo do gráfico
            chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mês
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período_Completo'].tolist()
            
            # Usar Período_Completo no gráfico
            coluna_periodo_grafico = 'Período_Completo'
        else:
            # Comportamento original: agrupar apenas por Período
            chart_data = df_data.groupby('Período')['Volume'].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos,
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            y=alt.Y(
                'Volume:Q',
                title='Volume Total',
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='blues')
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.2f')
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
            color=text_color,
            fontSize=12
        ).encode(
            text=alt.Text('Volume:Q', format=',.2f')
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico de Volume
st.subheader("📊 Volume Total por Período")

# Carregar dados de volume do arquivo df_vol.parquet
# Este gráfico não é afetado pelos filtros de Período
df_vol = load_volume_data(ano_selecionado)

if df_vol is not None:
    # Verificar se tem as colunas necessárias
    if 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
        # Aplicar filtros apenas para colunas que não são Período
        # Identificar colunas comuns entre df_filtrado e df_vol
        colunas_comuns = set(df_filtrado.columns) & set(df_vol.columns)
        # Remover colunas que não devem ser usadas para filtro
        # Excluir Período para não filtrar por mês
        colunas_filtro = [
            col for col in colunas_comuns
            if col not in ['Volume', 'Total', 'Valor', 'CPU', 'Período']
        ]

        # Aplicar filtros do df_filtrado ao df_vol usando colunas comuns
        df_vol_filtrado = df_vol.copy()

        for col in colunas_filtro:
            if col in df_filtrado.columns:
                # Obter valores únicos da coluna no df_filtrado
                valores_filtrados = df_filtrado[col].dropna().unique()
                if len(valores_filtrados) > 0:
                    # Filtrar df_vol com os mesmos valores
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado[col].isin(valores_filtrados)
                    ]

        # Criar gráfico (sempre mostrando todos os períodos)
        grafico_volume = create_volume_chart(df_vol_filtrado)
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


# Gráfico 4: Total por Período (se coluna Total existir)
def create_total_chart(df_data, moeda_simbolo="R$"):
    """Cria gráfico de barras de Total por Período"""
    try:
        if 'Total' not in df_data.columns:
            return None

        # Detectar tema do Streamlit
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        axis_color = "#FAFAFA" if theme_base == "dark" else "#000000"

        # Verificar se há múltiplos anos
        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        
        if tem_multiplos_anos:
            # Agrupar por Ano e Período
            chart_data = df_data.groupby(['Ano', 'Período'])['Total'].sum().reset_index()
            
            # Criar coluna combinada para o rótulo do gráfico
            chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
            
            # Ordenar por ano e mês
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período_Completo'].tolist()
            
            # Usar Período_Completo no gráfico
            coluna_periodo_grafico = 'Período_Completo'
        else:
            # Comportamento original: agrupar apenas por Período
            chart_data = df_data.groupby('Período')['Total'].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos,
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            y=alt.Y(
                'Total:Q',
                title=f'Total ({moeda_simbolo})',
                axis=alt.Axis(labelColor=axis_color, titleColor=axis_color)
            ),
            color=alt.Color(
                'Total:Q',
                title='Total',
                scale=alt.Scale(scheme='redyellowgreen', reverse=True)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip('Total:Q', title='Total', format=',.2f')
            ]
        ).properties(
            title='Total por Período',
            height=400
        )

        # Adicionar rótulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color=text_color,
            fontSize=12
        ).encode(
            text=alt.Text('Total:Q', format=',.2f')
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico de Total (apenas para Custo Total)
if tipo_visualizacao == "Custo Total" and 'Total' in df_filtrado.columns:
    st.subheader("📊 Total por Período")
    grafico_total = create_total_chart(df_filtrado, moeda_simbolo)
    if grafico_total:
        st.altair_chart(grafico_total, use_container_width=True)

# Tabela dinâmica: Valor por Oficina e Período
if ('Oficina' in df_visualizacao.columns and
        'Período' in df_visualizacao.columns):
    st.markdown("---")
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📋 Tabela Dinâmica - CPU por Oficina e Período")
    else:
        st.subheader("📋 Tabela Dinâmica - Valor por Oficina e Período")

    if coluna_visualizacao in df_visualizacao.columns:
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
        def formatar_valor(val, tipo, simbolo_moeda="R$"):
            if isinstance(val, (int, float)):
                if tipo == "CPU (Custo por Unidade)":
                    return f"{val:,.4f}"
                else:
                    return f"{simbolo_moeda} {val:,.2f}"
            return val

        # Aplicar formatação
        df_pivot_formatado = df_pivot.copy()
        for col in df_pivot_formatado.columns:
            df_pivot_formatado[col] = df_pivot_formatado[col].apply(
                lambda x: formatar_valor(x, tipo_visualizacao, moeda_simbolo)
            )

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
                    file_name = "TC_tabela_dinamica.xlsx"
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

# Exibir tabela filtrada
st.markdown("---")
if tipo_visualizacao == "CPU (Custo por Unidade)":
    st.subheader("📋 Tabela Filtrada - CPU")
else:
    st.subheader("📋 Tabela Filtrada")
display_limit = 1000
if len(df_visualizacao) > display_limit:
    st.info(
        f"📊 Mostrando {display_limit:,} de "
        f"{len(df_visualizacao):,} registros"
    )
    df_display = df_visualizacao.head(display_limit)
else:
    df_display = df_visualizacao

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
            file_name = "TC_tabela_filtrada.xlsx"
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
st.info("💡 Dashboard TC - KE5Z Group com visualizações interativas")
