import streamlit as st
import pandas as pd
import altair as alt
import os
import numpy as np
import re
import shutil
from datetime import datetime, timedelta
from versionamento import obter_versao_atual
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# Configuração da página
st.set_page_config(
    page_title="Best Estimate - Simulador",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
        arquivos_dados = [
            os.path.join("dados", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join("dados", "Forecast", "forecast_completo.parquet"),
            os.path.join("dados", "Forecast", "forecast_historico.parquet"),
        ]
        
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
                return "Não disponível"
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


# CSS para customização
st.markdown("""
    <style>
        /* Reduzir títulos em 20% e evitar quebra de linha */
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
st.title("🔮 Best Estimate - Simulador")
st.subheader("Análise preditiva e previsões de custos e volumes")

st.markdown("---")

# ========== CABEÇALHO PADRONIZADO (Moeda, Bandeiras, Taxas, Tipo, Fator) ==========
import sqlite3
from datetime import datetime

# Inicializar estado se não existir
if 'moeda_selecionada' not in st.session_state:
    st.session_state.moeda_selecionada = "🇧🇷 R$"
if 'moeda_selecionada_radio' not in st.session_state:
    st.session_state.moeda_selecionada_radio = "🇧🇷 R$"

# URLs das bandeiras
bandeira_brasil_url = "https://flagcdn.com/br.svg"
bandeira_eua_url = "https://flagcdn.com/us.svg"
bandeira_europa_url = "https://flagcdn.com/eu.svg"

# Funções de banco de dados SQLite
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

# Seleção de moeda com bandeiras ao lado
col_moeda1, col_moeda2 = st.columns([3, 1])

with col_moeda1:
    st.markdown("💱 **Moeda:**", unsafe_allow_html=True)
    opcoes_moeda = ["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"]
    
    moeda_atual_para_index = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
    index_moeda = opcoes_moeda.index(moeda_atual_para_index) if moeda_atual_para_index in opcoes_moeda else 0
    
    def atualizar_moeda():
        if 'moeda_selecionada_radio' in st.session_state:
            st.session_state.moeda_selecionada = st.session_state.moeda_selecionada_radio
    
    moeda_selecionada = st.radio(
        "",
        opcoes_moeda,
        index=index_moeda,
        horizontal=True,
        help="Selecione a moeda para exibição nos gráficos",
        key="moeda_selecionada_radio_forecast",
        label_visibility="visible",
        on_change=atualizar_moeda
    )
    
    if st.session_state.moeda_selecionada != moeda_selecionada:
        st.session_state.moeda_selecionada = moeda_selecionada

# Obter moeda atual do session_state
moeda_atual = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
flag_selecionada_brl = moeda_atual == '🇧🇷 R$'
flag_selecionada_usd = moeda_atual == '🇺🇸 $'
flag_selecionada_eur = moeda_atual == '🇪🇺 €'

with col_moeda2:
    st.markdown("<br>", unsafe_allow_html=True)
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

# Carregar taxas do banco de dados
try:
    taxas_cambio_banco = carregar_taxas_banco()
except Exception as e:
    taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

# Taxas de conversão
taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

# Seção de Taxas de Câmbio
st.markdown("📝 **Entrada de Taxas:**", unsafe_allow_html=True)

col_taxa1, col_taxa2 = st.columns([1.1, 1.1], gap="small")

with col_taxa1:
    st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇺🇸 1 $ (USD) = R$</p>', unsafe_allow_html=True)
    taxa_usd_para_brl = st.number_input(
        "",
        min_value=0.01,
        max_value=100.0,
        value=float(taxa_usd_para_brl_padrao),
        step=0.01,
        format="%.2f",
        help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros",
        key="taxa_usd_para_brl_input_forecast",
        label_visibility="collapsed"
    )

with col_taxa2:
    st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇪🇺 1 € (EUR) = R$</p>', unsafe_allow_html=True)
    taxa_eur_para_brl = st.number_input(
        "",
        min_value=0.01,
        max_value=100.0,
        value=float(taxa_eur_para_brl_padrao),
        step=0.01,
        format="%.2f",
        help="Digite quanto vale 1 Euro em Reais Brasileiros",
        key="taxa_eur_para_brl_input_forecast",
        label_visibility="collapsed"
    )

# Calcular taxas inversas
taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

# Salvar taxas quando alteradas
taxa_usd_atual_key = "taxa_usd_atual_salva_forecast"
taxa_eur_atual_key = "taxa_eur_atual_salva_forecast"

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

# Armazenar taxas em dicionário
taxas_cambio = {
    "BRL": 1.0,
    "USD": taxa_brl_para_usd,
    "EUR": taxa_brl_para_eur
}

# Seletores Tipo e Fator
col_tipo, col_fator = st.columns([1.3, 1.2], gap="small")

with col_tipo:
    tipo_visualizacao = st.radio(
        "📊 **Tipo:**",
        ["Custo Total", "CPU (Custo por Unidade)"],
        index=0,
        horizontal=True,
        key="tipo_visualizacao_top_forecast"
    )

with col_fator:
    if tipo_visualizacao == "Custo Total":
        fator_conversao = st.radio(
            "🔢 **Fator:**",
            ["Nenhum", "K (milhares)", "M (Milhões)"],
            index=1,
            horizontal=True,
            help="Aplica divisão aos valores para simplificar visualização. Não afeta cálculos.",
            key="fator_conversao_top_forecast"
        )
    else:
        fator_conversao = None

# Obter código e símbolo da moeda
moeda_selecionada = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
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
    moeda_codigo = "BRL"
    moeda_simbolo = "R$"

# Funções de conversão de moeda
def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino"""
    if valor is None or pd.isna(valor):
        return valor
    if moeda_destino == "BRL":
        return valor
    taxa = taxas.get(moeda_destino, 1.0)
    return valor * taxa

def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda"""
    if coluna not in df.columns:
        return df
    if moeda_destino == "BRL":
        return df
    df = df.copy()
    df[coluna] = df[coluna].apply(lambda x: converter_moeda(x, moeda_destino, taxas))
    return df

def obter_simbolo_moeda(moeda_codigo):
    """Retorna o símbolo da moeda"""
    simbolos = {
        "BRL": "R$",
        "USD": "$",
        "EUR": "€"
    }
    return simbolos.get(moeda_codigo, "R$")

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
    1. Pasta Forecast (dados/Forecast/) - PRIORIDADE MÁXIMA
    2. Se ano_selecionado for especificado: Pasta do ano (dados/{ANO}/)
    3. Histórico consolidado (dados/historico_consolidado/)
    4. Pasta do ano mais recente (dados/{ANO}/)
    5. Raiz do projeto (compatibilidade)
    """
    # 1. PRIORIDADE: Tentar pasta Forecast primeiro
    if nome_arquivo == "df_final.parquet":
        caminho_forecast = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_forecast):
            return caminho_forecast
    
    if nome_arquivo == "df_vol.parquet":
        caminho_forecast_vol = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        if os.path.exists(caminho_forecast_vol):
            return caminho_forecast_vol
    
    # Se ano específico foi selecionado, buscar na pasta do ano
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join("dados", str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano
    
    # 2. Tentar histórico consolidado (fallback)
    caminho_historico = os.path.join("dados", "historico_consolidado", nome_arquivo.replace(".parquet", "_historico.parquet"))
    if os.path.exists(caminho_historico):
        return caminho_historico
    
    # 3. Tentar pasta do ano mais recente
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
    
    # 4. Tentar raiz (compatibilidade)
    if os.path.exists(nome_arquivo):
        return nome_arquivo
    
    return None

# Filtros na sidebar - ANTES de carregar dados
st.sidebar.markdown("---")
st.sidebar.markdown("**📅 Seleção de Ano**")

# Listar anos disponíveis
anos_disponiveis = listar_anos_disponiveis()
opcoes_ano = ["Todos"] + [str(ano) for ano in anos_disponiveis]

# Inicializar session_state para manter valores dos filtros
if 'filtro_ano_simulador' not in st.session_state:
    st.session_state.filtro_ano_simulador = "Todos"

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=opcoes_ano.index(st.session_state.filtro_ano_simulador) if st.session_state.filtro_ano_simulador in opcoes_ano else 0,
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico",
    key="filtro_ano_simulador_selectbox"
)
# Atualizar session_state
st.session_state.filtro_ano_simulador = ano_selecionado

st.sidebar.markdown("---")

# Seletor de tipo de visualização (mesma lógica do TC_Ext)
st.sidebar.markdown("**📊 Tipo de Visualização**")
tipo_visualizacao = st.sidebar.radio(
    "Selecione o tipo:",
    ["Custo Total", "CPU (Custo por Unidade)"],
    index=0
)
st.sidebar.markdown("---")

# Botão para limpar cache (útil após mudanças no código)
if st.sidebar.button("🗑️ Limpar Cache", help="Limpa o cache do Streamlit para forçar recálculo"):
    st.cache_data.clear()
    st.sidebar.success("✅ Cache limpo! Recarregue a página.")
st.sidebar.markdown("---")

# 🔧 OTIMIZAÇÃO: Sincronização Excel/Parquet removida do início
# A sincronização só acontece quando o botão "Aplicar Configurações" é clicado,
# pois ambos os arquivos são gerados juntos nesse momento

# 🔧 OTIMIZAÇÃO: Função cacheada para otimização de tipos de dados
@st.cache_data(ttl=3600, show_spinner=False)
def otimizar_tipos_dados(df):
    """Otimiza tipos de dados do DataFrame (executa apenas uma vez)"""
    if df is None or df.empty:
        return df
    
    df_opt = df.copy()
    
    # Otimizar tipos de dados
    for col in df_opt.columns:
        if df_opt[col].dtype == 'object':
            try:
                unique_ratio = df_opt[col].nunique() / len(df_opt)
                if unique_ratio < 0.5:
                    df_opt[col] = df_opt[col].astype('category')
            except:
                pass

    # Converter floats para tipos menores
    for col in df_opt.select_dtypes(include=['float64']).columns:
        try:
            df_opt[col] = pd.to_numeric(df_opt[col], downcast='float')
        except:
            pass

    # Converter ints para tipos menores
    for col in df_opt.select_dtypes(include=['int64']).columns:
        try:
            df_opt[col] = pd.to_numeric(df_opt[col], downcast='integer')
        except:
            pass

    return df_opt

# Função para carregar dados com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param):
    """Carrega os dados do arquivo parquet - PRIORIZA PASTA FORECAST"""
    try:
        # 🔧 CORREÇÃO CRÍTICA: NÃO carregar diretamente do forecast_completo.parquet
        # O forecast_completo.parquet pode estar desatualizado com sensibilidade/inflação antigas
        # O forecast deve ser sempre recalculado em tempo real com as configurações atuais
        # Usar apenas dados históricos como base
        caminho_forecast = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        # REMOVIDO: Não carregar diretamente do forecast_completo.parquet
        # O forecast será calculado em tempo real com as configurações atuais
        
        # FALLBACK: Usar função encontrar_arquivo_parquet (que também prioriza Forecast)
        ano_para_busca = None if ano_selecionado_param == "Todos" else ano_selecionado_param
        arquivo_parquet = encontrar_arquivo_parquet("df_final.parquet", ano_para_busca)

        if arquivo_parquet is None:
            st.error(f"❌ Arquivo não encontrado: df_final.parquet")
            st.info("💡 Verifique se o arquivo existe em:")
            st.info("   - dados/Forecast/forecast_completo.parquet (PRIORIDADE)")
            st.info("   - dados/historico_consolidado/df_final_historico.parquet")
            st.info("   - dados/{ANO}/df_final.parquet")
            st.stop()

        # Carregar dados
        df = pd.read_parquet(arquivo_parquet)

        # Se carregou do histórico consolidado e um ano específico foi selecionado, filtrar
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

        # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
        df = otimizar_tipos_dados(df)

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
    """Carrega os dados de volume do arquivo parquet - PRIORIZA PASTA FORECAST"""
    try:
        # PRIORIDADE 1: Tentar carregar de df_vol_historico.parquet na pasta Forecast
        caminho_forecast_vol = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        
        if os.path.exists(caminho_forecast_vol):
            df = pd.read_parquet(caminho_forecast_vol)
            
            # Se um ano específico foi selecionado, filtrar
            if ano_selecionado_param != "Todos" and "Ano" in df.columns:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            
            # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
            df = otimizar_tipos_dados(df)
            
            return df
        
        # FALLBACK: Usar função encontrar_arquivo_parquet
        ano_para_busca = None if ano_selecionado_param == "Todos" else ano_selecionado_param
        arquivo_parquet = encontrar_arquivo_parquet("df_vol.parquet", ano_para_busca)

        if arquivo_parquet is None:
            return None

        df = pd.read_parquet(arquivo_parquet)

        # Se carregou do histórico consolidado e um ano específico foi selecionado, filtrar
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()

        # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
        df = otimizar_tipos_dados(df)

        return df
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def load_volume_historico_data():
    """Carrega os dados de volume histórico da pasta Forecast"""
    try:
        # PRIORIDADE: Buscar arquivo na pasta Forecast
        caminho_forecast = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        
        if os.path.exists(caminho_forecast):
            df = pd.read_parquet(caminho_forecast)
            # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
            df = otimizar_tipos_dados(df)
            return df
        
        # FALLBACK: Tentar histórico consolidado
        caminho_historico = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
            # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
            df = otimizar_tipos_dados(df)
            return df
        
        return None
    except Exception:
        return None


# 🔧 OTIMIZAÇÃO: Lazy loading - carregar dados apenas quando necessário
# Remover mensagens de sidebar para reduzir overhead de renderização
df_total = None
try:
    df_total = load_data(ano_selecionado)
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
    st.stop()

# Filtros na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Filtros**")

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

# Botão de atualizar dados na sidebar (após definir todas as funções com cache)
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Atualizar Dados", use_container_width=True):
    # Limpar cache de todas as funções (verificar se existem)
    try:
        load_data.clear()
        load_volume_data.clear()
        get_filter_options.clear()
        aplicar_filtros.clear()
        otimizar_tipos_dados.clear()
    except:
        pass
    
    # Limpar cache de calcular_medias_forecast se existir
    try:
        calcular_medias_forecast.clear()
    except:
        pass
    
    # Limpar session_state de debug
    if 'debug_calcular_medias' in st.session_state:
        del st.session_state['debug_calcular_medias']
    
    st.rerun()


# Ordem dos meses para ordenação cronológica
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]

# Lista de meses do ano (usado em cálculos de forecast)
meses_ano = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']


# Função para aplicar filtros com cache
@st.cache_data(ttl=3600, max_entries=50, show_spinner=False)
def aplicar_filtros(df_total_cache, oficina_selecionadas_cache, veiculo_selecionados_cache, 
                     usi_selecionada_cache, periodo_selecionado_cache):
    """Aplica filtros ao DataFrame com cache"""
    df_filtrado = df_total_cache.copy()
    
    # 🔧 CORREÇÃO: Converter para lista se for tuple (para compatibilidade)
    if isinstance(oficina_selecionadas_cache, tuple):
        oficina_selecionadas_cache = list(oficina_selecionadas_cache)
    if isinstance(veiculo_selecionados_cache, tuple):
        veiculo_selecionados_cache = list(veiculo_selecionados_cache)
    if isinstance(usi_selecionada_cache, tuple):
        usi_selecionada_cache = list(usi_selecionada_cache)
    if isinstance(periodo_selecionado_cache, tuple):
        periodo_selecionado_cache = list(periodo_selecionado_cache)
    
    # Filtro 1: Oficina
    if 'Oficina' in df_filtrado.columns:
        if oficina_selecionadas_cache and len(oficina_selecionadas_cache) > 0 and "Todos" not in oficina_selecionadas_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_cache)
            ].copy()
    
    # Filtro 2: Veículo
    if 'Veículo' in df_filtrado.columns:
        if veiculo_selecionados_cache and len(veiculo_selecionados_cache) > 0 and "Todos" not in veiculo_selecionados_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_cache)
            ].copy()
    
    # Filtro 3: USI
    if 'USI' in df_filtrado.columns:
        if usi_selecionada_cache and len(usi_selecionada_cache) > 0 and "Todos" not in usi_selecionada_cache:
            df_filtrado = df_filtrado[
                df_filtrado['USI'].astype(str).isin(usi_selecionada_cache)
            ].copy()
    
    # Filtro 4: Período
    if 'Período' in df_filtrado.columns:
        # 🔧 CORREÇÃO: Tratar string "Todos" como não aplicar filtro
        if periodo_selecionado_cache and periodo_selecionado_cache != "Todos" and len(periodo_selecionado_cache) > 0 and "Todos" not in periodo_selecionado_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Período'].astype(str).isin(periodo_selecionado_cache)
            ].copy()
    
    return df_filtrado

# Inicializar session_state para filtros
if 'filtro_oficina_simulador' not in st.session_state:
    st.session_state.filtro_oficina_simulador = ["Todos"]
if 'filtro_veiculo_simulador' not in st.session_state:
    st.session_state.filtro_veiculo_simulador = ["Todos"]
if 'filtro_usi_simulador' not in st.session_state:
    st.session_state.filtro_usi_simulador = ["TC Ext"]
if 'filtro_periodo_simulador' not in st.session_state:
    st.session_state.filtro_periodo_simulador = ["Todos"]

# 🔧 OTIMIZAÇÃO: Carregar opções de filtro apenas quando necessário
# Filtro 1: Oficina
oficina_selecionadas = ["Todos"]
if df_total is not None and 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    default_oficina = st.session_state.filtro_oficina_simulador if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_simulador) else ["Todos"]
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=default_oficina, key="filtro_oficina_simulador_multiselect"
    )
    st.session_state.filtro_oficina_simulador = oficina_selecionadas if oficina_selecionadas else ["Todos"]

# Filtro 2: Veículo
veiculo_selecionados = ["Todos"]
if df_total is not None and 'Veículo' in df_total.columns:
    # Usar df_total para opções, mas depois filtrar
    veiculo_opcoes = get_filter_options(df_total, 'Veículo')
    default_veiculo = st.session_state.filtro_veiculo_simulador if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_simulador) else ["Todos"]
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_simulador_multiselect"
    )
    st.session_state.filtro_veiculo_simulador = veiculo_selecionados if veiculo_selecionados else ["Todos"]

# Filtro 3: USI
usi_selecionada = ["TC Ext"]
if df_total is not None and 'USI' in df_total.columns:
    usi_opcoes = get_filter_options(df_total, 'USI')
    default_usi = st.session_state.filtro_usi_simulador if all(x in usi_opcoes for x in st.session_state.filtro_usi_simulador) else (["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"])
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_simulador_multiselect"
    )
    st.session_state.filtro_usi_simulador = usi_selecionada if usi_selecionada else ["TC Ext"]

# Filtro 4: Período (multiselect - igual aos outros filtros)
periodo_selecionado = ["Todos"]
if df_total is not None and 'Período' in df_total.columns:
    periodo_opcoes_raw = get_filter_options(df_total, 'Período')
    
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
    
    default_periodo = st.session_state.filtro_periodo_simulador if all(x in periodo_opcoes for x in st.session_state.filtro_periodo_simulador) else ["Todos"]
    periodo_selecionado = st.sidebar.multiselect(
        "Selecione o Período:", periodo_opcoes, default=default_periodo, key="filtro_periodo_simulador_multiselect"
    )
    st.session_state.filtro_periodo_simulador = periodo_selecionado if periodo_selecionado else ["Todos"]

# 🔧 OTIMIZAÇÃO: Adiar criação de DataFrames filtrados até serem realmente necessários
# Criar apenas se df_total estiver disponível
df_para_grafico_periodo = None
df_filtrado = None

if df_total is not None:
    # 🔧 CORREÇÃO CRÍTICA: Criar df_para_grafico_periodo ANTES de aplicar o filtro de Período (mesma lógica do TC_Ext linha 350)
    # Isso garante que o gráfico use os mesmos dados que o modo CPU, mas sem o filtro de Período
    df_para_grafico_periodo = aplicar_filtros(
        df_total,
        tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
        tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
        tuple(usi_selecionada) if usi_selecionada else tuple(),
        tuple()  # NÃO aplicar filtro de Período - queremos mostrar todos os períodos no gráfico
    )

    # Aplicar todos os filtros com cache (incluindo Período) para df_filtrado
    df_filtrado = aplicar_filtros(
        df_total,
        tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
        tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
        tuple(usi_selecionada) if usi_selecionada else tuple(),
        tuple(periodo_selecionado) if periodo_selecionado else tuple()
    )
    
    # Aplicar filtros adicionais em df_para_grafico_periodo (já foi criado acima)
    if df_para_grafico_periodo is not None:
        if oficina_selecionadas and "Todos" not in oficina_selecionadas:
            if 'Oficina' in df_para_grafico_periodo.columns:
                df_para_grafico_periodo = df_para_grafico_periodo[
                    df_para_grafico_periodo['Oficina'].astype(str).isin(oficina_selecionadas)
                ].copy()
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            if 'Veículo' in df_para_grafico_periodo.columns:
                df_para_grafico_periodo = df_para_grafico_periodo[
                    df_para_grafico_periodo['Veículo'].astype(str).isin(veiculo_selecionados)
                ].copy()
        if usi_selecionada and "Todos" not in usi_selecionada:
            if 'USI' in df_para_grafico_periodo.columns:
                df_para_grafico_periodo = df_para_grafico_periodo[
                    df_para_grafico_periodo['USI'].astype(str).isin(usi_selecionada)
                ].copy()

# ====================================================================
# 🔧 MODO CPU: Preparar dados para visualização (mesma lógica do TC_Ext)
# ====================================================================
# IMPORTANTE: Usar dados da pasta Forecast
# - Valores: dados\Forecast\forecast_completo.parquet
# - Volume: dados\Forecast\df_vol_historico.parquet
# 🔧 OTIMIZAÇÃO: Verificar se df_filtrado está disponível antes de processar
if df_filtrado is not None and tipo_visualizacao == "CPU (Custo por Unidade)":
    # 🔧 OTIMIZAÇÃO: Usar função com cache em vez de carregar diretamente
    df_vol_calc = load_volume_historico_data()
    
    if df_vol_calc is not None and 'Volume' in df_vol_calc.columns:
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
        # Isso garante que apenas o ano selecionado seja considerado
        if ano_selecionado != "Todos" and 'Ano' in df_vol_calc.columns:
            df_vol_calc = df_vol_calc[
                df_vol_calc['Ano'] == int(ano_selecionado)
            ].copy()
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar Período no volume para corresponder ao formato do forecast
        # No forecast: Período = "Novembro", Ano = 2025 (separados)
        # No volume: pode estar como "Novembro 2025" ou "Novembro" com Ano separado
        if 'Período' in df_vol_calc.columns:
            df_vol_calc = df_vol_calc.copy()
            # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
            df_vol_calc['Período'] = df_vol_calc['Período'].astype(str)
            # Extrair apenas o nome do mês do Período (remover ano se estiver incluído)
            def normalizar_periodo_volume_cpu(periodo_str):
                periodo_str = str(periodo_str).strip()
                # Se contém espaço, pegar apenas a primeira parte (nome do mês)
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    mes_nome = partes[0].strip().capitalize()
                    # Se a segunda parte é um número (ano), retornar tupla
                    if len(partes) > 1 and partes[1].strip().isdigit():
                        ano_val = int(partes[1].strip())
                        return (mes_nome, ano_val)
                    return (mes_nome, None)
                return (periodo_str.capitalize(), None)
            
            # Aplicar normalização e separar período e ano
            periodos_normalizados = df_vol_calc['Período'].apply(normalizar_periodo_volume_cpu)
            df_vol_calc['Período'] = periodos_normalizados.apply(lambda x: x[0])
            
            # Extrair anos e atualizar coluna Ano
            anos_extraidos = periodos_normalizados.apply(lambda x: x[1] if x[1] is not None else None)
            if anos_extraidos.notna().any():
                if 'Ano' not in df_vol_calc.columns:
                    df_vol_calc['Ano'] = anos_extraidos
                else:
                    # Atualizar apenas onde o ano foi extraído do Período
                    mask_ano_extraido = anos_extraidos.notna()
                    df_vol_calc.loc[mask_ano_extraido, 'Ano'] = anos_extraidos[mask_ano_extraido].astype(int)
        
        # 🔧 CORREÇÃO: Normalizar Período no df_filtrado também (garantir formato consistente)
        if 'Período' in df_filtrado.columns:
            df_filtrado = df_filtrado.copy()
            # Converter para string para evitar problemas com CategoricalIndex
            df_filtrado['Período'] = df_filtrado['Período'].astype(str)
            def normalizar_periodo_forecast_cpu(periodo_str):
                periodo_str = str(periodo_str).strip()
                # Se contém espaço, pegar apenas a primeira parte (nome do mês)
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    mes_nome = partes[0].strip().capitalize()
                    return mes_nome
                return periodo_str.capitalize()
            
            df_filtrado['Período'] = df_filtrado['Período'].apply(normalizar_periodo_forecast_cpu)
        
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
                df_vol_calc_filtrado = df_vol_calc.copy()
                
                # Aplicar filtros de Veículo se existir
                if tem_veiculo and 'Veículo' in df_vol_calc_filtrado.columns:
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

                # 🔧 CORREÇÃO: Incluir 'Ano' no groupby se existir (mesma lógica do TC_Ext linha 544-547)
                # O filtro de ano já foi aplicado no load_data, então quando um ano específico está selecionado,
                # o df_filtrado já está filtrado por aquele ano. Incluir 'Ano' no agrupamento garante consistência.
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
                    # 🔧 CORREÇÃO: Incluir 'Ano' no groupby se existir (mesma lógica do TC_Ext)
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
                    # 🔧 CORREÇÃO: Incluir 'Ano' no groupby se existir (mesma lógica do TC_Ext)
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

                # Calcular CPU (evitando divisão por zero)
                df_cpu['CPU'] = df_cpu.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
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
    if 'Total' in df_filtrado.columns:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Total'
    elif 'Valor' in df_filtrado.columns:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Valor'
    else:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Total'

# 🔧 CORREÇÃO CRÍTICA: Adicionar Volume ao df_visualizacao mesmo quando não está no modo CPU
# Isso garante que o gráfico de Volume por Veículo use os mesmos dados do TC_Ext
# PROBLEMA IDENTIFICADO: df_visualizacao pode ter múltiplas linhas
# para a mesma combinação de Oficina+Período+Veículo, causando duplicação no merge
# SOLUÇÃO: Agrupar df_visualizacao ANTES do merge, igual ao modo CPU faz com df_total_agrupado
if 'Veículo' in df_visualizacao.columns and 'Oficina' in df_visualizacao.columns and 'Período' in df_visualizacao.columns:
    # 🔧 CORREÇÃO CRÍTICA: Carregar volume da MESMA fonte e com a MESMA lógica usada no forecast
    # O volume deve ser exatamente o mesmo usado para calcular o forecast linha a linha
    # Fonte: C:\GIT\TC\dados\Forecast\df_vol_historico.parquet (mesmo arquivo usado no forecast)
    # IMPORTANTE: Este é o mesmo arquivo usado na função calcular_forecast_completo (linha 5274)
    # que calcula o forecast linha a linha. Garantir que o volume seja o mesmo garante que
    # quando sensibilidade = 1 e inflação = 0, o CPU seja constante (igual à média)
    # 🔧 OTIMIZAÇÃO: Usar função com cache em vez de carregar diretamente
    df_vol_calc = load_volume_historico_data()
    
    if df_vol_calc is not None and 'Volume' in df_vol_calc.columns:
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de processar (mesma lógica do modo CPU e do forecast)
        if ano_selecionado != "Todos" and 'Ano' in df_vol_calc.columns:
            df_vol_calc = df_vol_calc[
                df_vol_calc['Ano'] == int(ano_selecionado)
            ].copy()
        
        tem_veiculo = 'Veículo' in df_visualizacao.columns
        tem_ano = 'Ano' in df_visualizacao.columns
        
        # 🔧 CORREÇÃO: NÃO normalizar o Período no volume aqui - manter formato original
        # A normalização será feita apenas se necessário para corresponder ao df_visualizacao
        # Isso garante que o volume seja o mesmo usado no forecast
        
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
        
        # 🔧 CORREÇÃO CRÍTICA: Agrupar Volume EXATAMENTE como no forecast
        # No forecast, o volume_por_mes é calculado diretamente a partir do volume histórico
        # e é agrupado por ['Oficina', 'Veículo', 'Período', 'Ano'] (se Ano existir) - linha 5244-5248
        # IMPORTANTE: Usar a MESMA lógica de agrupamento garante que o volume seja o mesmo
        # usado para calcular o forecast linha a linha, garantindo que quando sensibilidade = 1
        # e inflação = 0, o CPU seja constante (igual à média)
        if tem_veiculo and 'Veículo' in df_vol_calc.columns:
            # 🔧 CORREÇÃO: Usar a MESMA lógica do volume_por_mes do forecast (linha 5244-5248)
            # Agrupar por ['Oficina', 'Veículo', 'Período'] e incluir 'Ano' se existir
            # Este é o mesmo agrupamento usado no forecast, garantindo consistência
            colunas_agrupamento_vol = ['Oficina', 'Veículo', 'Período']
            if tem_ano and 'Ano' in df_vol_calc.columns:
                colunas_agrupamento_vol.append('Ano')
            
            df_vol_agrupado = df_vol_calc.groupby(
                colunas_agrupamento_vol, as_index=False
            )['Volume'].sum()
            
            # IMPORTANTE: Usar EXATAMENTE as mesmas colunas de agrupamento para o merge
            # Garantir que colunas_agrupamento seja idêntica a colunas_agrupamento_vol
            # 🔧 CORREÇÃO: Seguir a mesma lógica do TC_Ext - não normalizar Período antes do merge
            # O formato do Período deve ser mantido como está no volume e no df_visualizacao
            colunas_agrupamento = colunas_agrupamento_vol.copy()
            
            # 🔧 CORREÇÃO: Garantir que o Período no volume e no df_visualizacao estejam no mesmo formato
            # Se o df_visualizacao tem Período normalizado (sem ano) mas o volume tem Ano separado,
            # precisamos normalizar o Período no volume para corresponder
            if 'Período' in df_visualizacao.columns and 'Período' in df_vol_agrupado.columns and len(df_visualizacao) > 0 and len(df_vol_agrupado) > 0:
                # Verificar se o df_visualizacao tem Período normalizado (sem ano) mas tem coluna Ano separada
                periodo_exemplo_vis = str(df_visualizacao['Período'].iloc[0]).strip()
                periodo_tem_ano_vis = ' ' in periodo_exemplo_vis and any(char.isdigit() for char in periodo_exemplo_vis.split(' ', 1)[-1] if len(periodo_exemplo_vis.split(' ', 1)) > 1)
                
                # Se o df_visualizacao tem Período sem ano mas tem coluna Ano, normalizar o volume
                if not periodo_tem_ano_vis and 'Ano' in df_visualizacao.columns and 'Ano' in df_vol_agrupado.columns:
                    # Normalizar Período no volume para remover ano (se houver)
                    def normalizar_periodo_vol_simples(periodo_str):
                        periodo_str = str(periodo_str).strip()
                        if ' ' in periodo_str:
                            # Remover ano do Período
                            return periodo_str.split(' ', 1)[0].strip().capitalize()
                        return periodo_str.capitalize()
                    
                    df_vol_agrupado = df_vol_agrupado.copy()
                    # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
                    df_vol_agrupado['Período'] = df_vol_agrupado['Período'].astype(str)
                    df_vol_agrupado['Período'] = df_vol_agrupado['Período'].apply(normalizar_periodo_vol_simples)
            
            # Agrupar df_visualizacao mantendo apenas as colunas necessárias
            # 🔧 CORREÇÃO CRÍTICA: Sempre somar 'Total' se existir, não apenas coluna_visualizacao
            # Isso garante que quando há múltiplas linhas com mesma combinação, o Total seja somado corretamente
            colunas_para_somar = []
            if 'Total' in df_visualizacao.columns:
                colunas_para_somar.append('Total')
            if coluna_visualizacao in df_visualizacao.columns and coluna_visualizacao != 'Total':
                colunas_para_somar.append(coluna_visualizacao)
            
            # 🔧 CORREÇÃO: Verificar se as colunas de agrupamento existem no DataFrame
            colunas_agrupamento_existentes = [col for col in colunas_agrupamento if col in df_visualizacao.columns]
            if len(colunas_agrupamento_existentes) != len(colunas_agrupamento):
                # Se alguma coluna de agrupamento não existe, usar apenas as existentes
                colunas_agrupamento = colunas_agrupamento_existentes
                # Atualizar também colunas_agrupamento_vol para corresponder
                colunas_agrupamento_vol = [col for col in colunas_agrupamento_vol if col in colunas_agrupamento]
            
            if colunas_para_somar and len(colunas_agrupamento) > 0:
                # 🔧 CORREÇÃO: Verificar se as colunas existem antes de agrupar
                # Filtrar apenas colunas que realmente existem no DataFrame
                colunas_para_somar_existentes = [col for col in colunas_para_somar if col in df_visualizacao.columns]
                
                if colunas_para_somar_existentes:
                    # Agrupar e somar todas as colunas numéricas necessárias
                    # 🔧 CORREÇÃO: Usar reset_index() em vez de as_index=False para evitar problemas com índices
                    agg_dict = {col: 'sum' for col in colunas_para_somar_existentes}
                    df_visualizacao_agrupado = df_visualizacao.groupby(
                        colunas_agrupamento
                    ).agg(agg_dict).reset_index()
                else:
                    # Se não tiver colunas para somar, apenas agrupar para ter estrutura única
                    if len(colunas_agrupamento) > 0:
                        df_visualizacao_agrupado = df_visualizacao[colunas_agrupamento].drop_duplicates()
                    else:
                        df_visualizacao_agrupado = df_visualizacao.copy()
            else:
                # Se não tiver colunas para somar, apenas agrupar para ter estrutura única
                if len(colunas_agrupamento) > 0:
                    df_visualizacao_agrupado = df_visualizacao[colunas_agrupamento].drop_duplicates()
                else:
                    df_visualizacao_agrupado = df_visualizacao.copy()
            
            # Fazer merge com df_vol_agrupado usando as MESMAS colunas
            # Isso garante que não há duplicação
            df_visualizacao = pd.merge(
                df_visualizacao_agrupado,
                df_vol_agrupado[colunas_agrupamento_vol + ['Volume']],
                on=colunas_agrupamento_vol,
                how='left'
            )
            df_visualizacao['Volume'] = df_visualizacao['Volume'].fillna(0.0)

# Resumo na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Resumo**")
st.sidebar.write(f"**Linhas:** {df_visualizacao.shape[0]:,}")

# Calcular totais se as colunas existirem
if tipo_visualizacao == "CPU (Custo por Unidade)":
    if 'CPU' in df_visualizacao.columns:
        # Para CPU, mostrar média ponderada ou total
        cpu_medio = df_visualizacao['CPU'].mean()
        cpu_medio_convertido = converter_moeda(cpu_medio, moeda_codigo, taxas_cambio)
        st.sidebar.write(f"**CPU Médio:** {moeda_simbolo} {cpu_medio_convertido:,.2f}")
    if 'Total' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
        total_sum = df_visualizacao['Total'].sum()
        volume_sum = df_visualizacao['Volume'].sum()
        if volume_sum > 0:
            cpu_geral = total_sum / volume_sum
            cpu_geral_convertido = converter_moeda(cpu_geral, moeda_codigo, taxas_cambio)
            st.sidebar.write(f"**CPU Geral:** {moeda_simbolo} {cpu_geral_convertido:,.2f}")
else:
    if 'Valor' in df_visualizacao.columns:
        # Converter para numérico caso seja categórico
        valor_series = pd.to_numeric(df_visualizacao['Valor'], errors='coerce')
        valor_total = valor_series.sum()
        valor_total_convertido = converter_moeda(valor_total, moeda_codigo, taxas_cambio)
        st.sidebar.write(f"**Total Valor:** {moeda_simbolo} {valor_total_convertido:,.2f}")
    if 'Total' in df_visualizacao.columns:
        # Converter para numérico caso seja categórico
        total_series = pd.to_numeric(df_visualizacao['Total'], errors='coerce')
        total_sum = total_series.sum()
        total_sum_convertido = converter_moeda(total_sum, moeda_codigo, taxas_cambio)
        st.sidebar.write(f"**Total:** {moeda_simbolo} {total_sum_convertido:,.2f}")

# Área principal - Forecast
st.markdown("## 📈 Best Estimate - Previsão de Custo Total")

# ====================================================================
# 🔮 CONFIGURAÇÃO DO FORECAST - PRIMEIRO (antes dos sliders)
# ====================================================================
st.markdown("### 🔮 Configuração do Forecast")

# Lista de meses do ano (necessária para a configuração)
meses_ano = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

# Verificar se temos dados com múltiplos anos (usar df_total para ver TODOS os anos disponíveis)
tem_anos = 'Ano' in df_total.columns and df_total['Ano'].nunique() > 1

# Determinar o ano dos dados (usar df_total para obter TODOS os anos, não apenas os filtrados)
if tem_anos and 'Ano' in df_total.columns:
    anos_disponiveis = sorted(df_total['Ano'].dropna().unique())
    ano_maximo = int(df_total['Ano'].max())
    ano_minimo = int(df_total['Ano'].min())
else:
    from datetime import datetime
    anos_disponiveis = [datetime.now().year]
    ano_maximo = datetime.now().year
    ano_minimo = datetime.now().year

# Função auxiliar para ordenar períodos (definir antes de usar)
def ordenar_periodo_para_select(periodo_str):
    """Ordena períodos para o selectbox"""
    periodo_str = str(periodo_str).strip()
    # Se tiver ano (ex: "Novembro 2025")
    if ' ' in periodo_str:
        partes = periodo_str.split(' ', 1)
        mes_nome = partes[0]
        ano = int(partes[1]) if partes[1].isdigit() else 0
        # Normalizar nome do mês (capitalizar)
        mes_nome_capitalizado = mes_nome.capitalize()
        mes_idx = meses_ano.index(mes_nome_capitalizado) if mes_nome_capitalizado in meses_ano else 0
        return (ano, mes_idx)
    else:
        # Apenas mês - normalizar (capitalizar)
        mes_nome_capitalizado = periodo_str.capitalize()
        mes_idx = meses_ano.index(mes_nome_capitalizado) if mes_nome_capitalizado in meses_ano else 0
        return (0, mes_idx)

# Criar lista de períodos disponíveis com ano (baseado nos dados do historico_consolidado)
# IMPORTANTE: Carregar períodos diretamente do arquivo historico_consolidado para obter TODOS os períodos disponíveis
periodos_disponiveis = []
caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")

if os.path.exists(caminho_historico):
    try:
        # Carregar dados do histórico consolidado para obter todos os períodos disponíveis
        df_historico_periodos = pd.read_parquet(caminho_historico)
        
        if 'Período' in df_historico_periodos.columns:
            # Pegar períodos únicos dos dados históricos consolidados
            periodos_unicos = df_historico_periodos['Período'].dropna().unique()
            
            # Verificar se os períodos já têm ano ou não
            periodos_com_ano = any(' ' in str(p) and str(p).split(' ', 1)[1].isdigit() for p in periodos_unicos)
            
            # Verificar se há múltiplos anos no histórico
            tem_anos_historico = 'Ano' in df_historico_periodos.columns and df_historico_periodos['Ano'].nunique() > 1
            
            if tem_anos_historico:
                anos_historico = sorted(df_historico_periodos['Ano'].dropna().unique())
            else:
                anos_historico = anos_disponiveis
            
            # Se não tiver ano nos períodos mas temos múltiplos anos, adicionar ano para cada combinação
            if not periodos_com_ano and tem_anos_historico:
                # Criar períodos com ano para TODOS os anos disponíveis no histórico
                periodos_com_ano_lista = []
                for periodo in periodos_unicos:
                    periodo_str = str(periodo).strip()
                    # Capitalizar primeira letra
                    periodo_capitalizado = periodo_str.capitalize() if periodo_str else periodo_str
                    # Adicionar ano para cada ano disponível no histórico
                    for ano in anos_historico:
                        periodo_com_ano = f"{periodo_capitalizado} {ano}"
                        # Verificar se esse período realmente existe nos dados históricos
                        if len(df_historico_periodos[(df_historico_periodos['Período'].astype(str).str.strip().str.capitalize() == periodo_capitalizado) & 
                                   (df_historico_periodos['Ano'] == ano)]) > 0:
                            periodos_com_ano_lista.append(periodo_com_ano)
                periodos_disponiveis = sorted(periodos_com_ano_lista, key=lambda x: ordenar_periodo_para_select(x))
            elif periodos_com_ano and tem_anos_historico:
                # Se já tem ano, usar como está
                periodos_disponiveis = sorted(periodos_unicos, key=lambda x: ordenar_periodo_para_select(x))
            else:
                # Se não tem múltiplos anos, usar como está
                periodos_disponiveis = sorted(periodos_unicos, key=lambda x: ordenar_periodo_para_select(x))
        else:
            # Fallback: criar períodos baseado nos meses e anos disponíveis no histórico
            if tem_anos_historico and 'Ano' in df_historico_periodos.columns:
                anos_historico = sorted(df_historico_periodos['Ano'].dropna().unique())
            else:
                anos_historico = anos_disponiveis
            
            for ano in anos_historico:
                for mes in meses_ano:
                    if tem_anos_historico:
                        periodo = f"{mes} {ano}"
                    else:
                        periodo = mes
                    periodos_disponiveis.append(periodo)
    except Exception as e:
        st.warning(f"⚠️ Erro ao carregar períodos do histórico consolidado: {str(e)}")
        # Fallback: usar df_total se houver erro
        if 'Período' in df_total.columns:
            periodos_unicos = df_total['Período'].dropna().unique()
            periodos_disponiveis = sorted(periodos_unicos, key=lambda x: ordenar_periodo_para_select(x))
        else:
            periodos_disponiveis = []
else:
    # Se o arquivo histórico não existir, usar df_total como fallback
    st.warning(f"⚠️ Arquivo histórico consolidado não encontrado: {caminho_historico}")
    st.info("ℹ️ Usando períodos dos dados carregados como fallback")
    
    if 'Período' in df_total.columns:
        periodos_unicos = df_total['Período'].dropna().unique()
        periodos_com_ano = any(' ' in str(p) and str(p).split(' ', 1)[1].isdigit() for p in periodos_unicos)
        
        if not periodos_com_ano and tem_anos:
            periodos_com_ano_lista = []
            for periodo in periodos_unicos:
                periodo_str = str(periodo).strip()
                periodo_capitalizado = periodo_str.capitalize() if periodo_str else periodo_str
                for ano in anos_disponiveis:
                    periodo_com_ano = f"{periodo_capitalizado} {ano}"
                    if len(df_total[(df_total['Período'].astype(str).str.strip().str.capitalize() == periodo_capitalizado) & 
                                   (df_total['Ano'] == ano)]) > 0:
                        periodos_com_ano_lista.append(periodo_com_ano)
            periodos_disponiveis = sorted(periodos_com_ano_lista, key=lambda x: ordenar_periodo_para_select(x))
        else:
            periodos_disponiveis = sorted(periodos_unicos, key=lambda x: ordenar_periodo_para_select(x))
    else:
        # Fallback final: criar períodos baseado nos meses e anos disponíveis
        for ano in anos_disponiveis:
            for mes in meses_ano:
                if tem_anos:
                    periodo = f"{mes} {ano}"
                else:
                    periodo = mes
                periodos_disponiveis.append(periodo)

# Layout em 2 colunas para os controles principais
col_config1, col_config2 = st.columns(2)

with col_config1:
    # 1. Selecionar último período com dados reais (com ano)
    from datetime import datetime
    mes_atual_sistema = datetime.now().month
    mes_atual_nome = meses_ano[mes_atual_sistema - 1] if mes_atual_sistema <= 12 else meses_ano[11]
    
    # Determinar período padrão
    if tem_anos:
        periodo_padrao = f"{mes_atual_nome} {ano_maximo}"
        # Se o período padrão não estiver na lista, usar o último disponível
        if periodo_padrao not in periodos_disponiveis and periodos_disponiveis:
            periodo_padrao = periodos_disponiveis[-1]
    else:
        periodo_padrao = mes_atual_nome
        if periodo_padrao not in periodos_disponiveis and periodos_disponiveis:
            periodo_padrao = periodos_disponiveis[-1]
    
    # Encontrar índice do período padrão
    try:
        indice_padrao = periodos_disponiveis.index(periodo_padrao)
    except ValueError:
        indice_padrao = len(periodos_disponiveis) - 1 if periodos_disponiveis else 0
    
    ultimo_periodo_dados = st.selectbox(
        "📅 Último período com dados reais:",
        options=periodos_disponiveis,
        index=indice_padrao if indice_padrao < len(periodos_disponiveis) else 0,
        help="Selecione o último período (mês e ano) que possui dados históricos reais"
    )
    
    # Extrair mês do período selecionado
    if ' ' in str(ultimo_periodo_dados):
        ultimo_mes_dados = str(ultimo_periodo_dados).split(' ', 1)[0]
    else:
        ultimo_mes_dados = str(ultimo_periodo_dados)
    
    # Normalizar para capitalizar (ex: "setembro" -> "Setembro")
    ultimo_mes_dados = ultimo_mes_dados.capitalize()
    
    # Encontrar índice do mês na lista meses_ano
    if ultimo_mes_dados in meses_ano:
        indice_ultimo_mes = meses_ano.index(ultimo_mes_dados)
    else:
        indice_ultimo_mes = 0
    
    # 2. Quantos meses prever
    meses_disponiveis_para_prever = len(meses_ano) - (indice_ultimo_mes + 1)
    if meses_disponiveis_para_prever <= 0:
        meses_disponiveis_para_prever = 12  # Se já passou dezembro, permitir prever o próximo ano
    
    num_meses_prever = st.number_input(
        "🔮 Quantos meses prever:",
        min_value=1,
        max_value=12,
        value=min(meses_disponiveis_para_prever, 6),
        step=1,
        help="Número de meses futuros para prever"
    )

with col_config2:
    # 3. Quantos meses usar para calcular a média
    # LÓGICA: Contar apenas os períodos que estão disponíveis no filtro até o período selecionado
    
    # Encontrar o índice do período selecionado na lista de períodos disponíveis
    if ultimo_periodo_dados in periodos_disponiveis:
        indice_periodo_selecionado = periodos_disponiveis.index(ultimo_periodo_dados)
    else:
        indice_periodo_selecionado = len(periodos_disponiveis) - 1 if periodos_disponiveis else 0
    
    # Filtrar apenas os períodos disponíveis até o período selecionado (inclusive)
    periodos_disponiveis_ate_selecionado = periodos_disponiveis[:indice_periodo_selecionado + 1]
    
    # 🔧 CORREÇÃO: Filtrar apenas períodos do mesmo ano do período selecionado
    # Extrair ano do período selecionado
    ano_periodo_selecionado = None
    if ' ' in str(ultimo_periodo_dados):
        partes = str(ultimo_periodo_dados).split(' ', 1)
        if len(partes) > 1 and partes[1].isdigit():
            ano_periodo_selecionado = int(partes[1])
    
    # Filtrar apenas períodos do mesmo ano
    if ano_periodo_selecionado is not None:
        periodos_disponiveis_ate_selecionado = [
            p for p in periodos_disponiveis_ate_selecionado
            if ' ' in str(p) and str(p).split(' ', 1)[1].isdigit() and int(str(p).split(' ', 1)[1]) == ano_periodo_selecionado
        ]
    
    # Contar quantos períodos existem até o período selecionado (inclusive)
    # Isso considera apenas os períodos disponíveis no filtro do mesmo ano
    max_meses_media = len(periodos_disponiveis_ate_selecionado)
    
    # Lista de meses até o último mês selecionado (inclusive) - usado depois
    # Extrair mês do período selecionado para criar a lista de meses
    if ' ' in str(ultimo_periodo_dados):
        ultimo_mes_dados = str(ultimo_periodo_dados).split(' ', 1)[0].capitalize()
    else:
        ultimo_mes_dados = str(ultimo_periodo_dados).capitalize()
    
    if ultimo_mes_dados in meses_ano:
        indice_ultimo_mes = meses_ano.index(ultimo_mes_dados)
    else:
        indice_ultimo_mes = 0
    
    meses_historicos_disponiveis = meses_ano[:indice_ultimo_mes + 1]
    
    # 🔧 CORREÇÃO: Ajustar valor inicial baseado no session_state ou no max disponível
    # Se houver valor salvo, usar ele, mas limitar ao novo max_meses_media
    valor_inicial_media = min(max_meses_media, 6)  # Valor padrão
    if 'config_forecast_aplicada' in st.session_state and st.session_state.config_forecast_aplicada.get('num_meses_media') is not None:
        valor_salvo = st.session_state.config_forecast_aplicada['num_meses_media']
        # Ajustar valor salvo se ele exceder o novo máximo (quando último período mudar)
        valor_inicial_media = min(valor_salvo, max_meses_media)
    
    # 🔧 CORREÇÃO CRÍTICA: Usar key baseada no ultimo_periodo_dados para forçar atualização quando mudar
    # Isso garante que o widget seja recriado com os novos max_value e value quando o último período mudar
    key_num_meses_media = f"num_meses_media_{ultimo_periodo_dados}"
    
    # Verificar se o último período mudou e ajustar o valor no session_state
    if key_num_meses_media not in st.session_state:
        st.session_state[key_num_meses_media] = valor_inicial_media
    else:
        # Se o último período mudou (key diferente), ajustar o valor ao novo máximo
        valor_atual = st.session_state.get(key_num_meses_media, valor_inicial_media)
        if valor_atual > max_meses_media:
            st.session_state[key_num_meses_media] = max_meses_media
        else:
            st.session_state[key_num_meses_media] = valor_atual
    
    num_meses_media = st.number_input(
        "📈 Quantos meses usar para a média:",
        min_value=1,
        max_value=max_meses_media,
        value=st.session_state[key_num_meses_media],
        step=1,
        key=key_num_meses_media,
        help=f"Número de meses históricos para calcular a média (máximo: {max_meses_media} meses com valores até {ultimo_periodo_dados})"
    )
    
    # 4. Selecionar quais meses excluir do cálculo da média
    #    (exibir com ano para evitar confusão em cenários multi-ano)
    if meses_historicos_disponiveis:
        # Determinar ano de referência a partir do último período selecionado
        ano_referencia = None
        if ' ' in str(ultimo_periodo_dados):
            partes_periodo = str(ultimo_periodo_dados).split(' ', 1)
            if len(partes_periodo) > 1 and partes_periodo[1].isdigit():
                ano_referencia = partes_periodo[1]
        if ano_referencia is None:
            # Fallback: usar ano_maximo (ano dos dados carregados)
            ano_referencia = str(ano_maximo) if 'ano_maximo' in locals() else str(datetime.now().year)

        # Criar opções com ano para o multiselect
        opcoes_excluir = [f"{mes} {ano_referencia}" for mes in meses_historicos_disponiveis]

        selecao_excluir = st.multiselect(
            "🚫 Excluir meses do cálculo da média:",
            options=opcoes_excluir,
            default=[],
            help="Selecione meses (com ano) que foram fora da curva e devem ser excluídos do cálculo da média"
        )

        # Converter seleção de "Mês Ano" de volta apenas para o nome do mês
        meses_excluir_media = []
        for opcao in selecao_excluir:
            opcao_str = str(opcao).strip()
            mes_nome = opcao_str.split(' ', 1)[0] if ' ' in opcao_str else opcao_str
            meses_excluir_media.append(mes_nome)
    else:
        meses_excluir_media = []

# Extrair ano do último período selecionado
if ' ' in str(ultimo_periodo_dados):
    ultimo_ano_dados = int(str(ultimo_periodo_dados).split(' ', 1)[1])
else:
    # Se não tiver ano no período, usar o ano máximo dos dados
    if tem_anos and 'Ano' in df_filtrado.columns:
        ultimo_ano_dados = int(df_filtrado['Ano'].max())
    else:
        ultimo_ano_dados = datetime.now().year

# Calcular quais períodos serão previstos (com ano)
periodos_restantes = []
meses_restantes = []

for i in range(num_meses_prever):
    indice_mes = indice_ultimo_mes + 1 + i
    ano_futuro = ultimo_ano_dados
    
    # Se passar de dezembro, avançar para o próximo ano
    if indice_mes >= 12:
        ano_futuro += (indice_mes // 12)
        indice_mes = indice_mes % 12
    
    mes_nome = meses_ano[indice_mes]
    meses_restantes.append(mes_nome)
    
    # Criar período com ano se necessário
    if tem_anos:
        periodo_futuro = f"{mes_nome} {ano_futuro}"
    else:
        periodo_futuro = mes_nome
    
    periodos_restantes.append(periodo_futuro)

# Calcular quais períodos serão usados para a média (com ano)
# IMPORTANTE: Usar apenas os períodos disponíveis no filtro até o período selecionado
periodos_para_media = []
meses_para_media = []

# Usar os períodos disponíveis até o período selecionado (já calculado acima)
if 'periodos_disponiveis_ate_selecionado' in locals() and periodos_disponiveis_ate_selecionado:
    # Filtrar períodos excluídos
    periodos_considerados = [p for p in periodos_disponiveis_ate_selecionado if p not in meses_excluir_media]
    
    # Pegar os últimos N períodos (após excluir)
    if periodos_considerados:
        periodos_para_media = periodos_considerados[-num_meses_media:] if len(periodos_considerados) >= num_meses_media else periodos_considerados
        
        # Extrair nomes dos meses para meses_para_media (usado em outros lugares)
        meses_para_media = []
        for periodo in periodos_para_media:
            if ' ' in str(periodo):
                mes_nome = str(periodo).split(' ', 1)[0]
                meses_para_media.append(mes_nome)
            else:
                meses_para_media.append(str(periodo))
    else:
        meses_para_media = []
        periodos_para_media = []
else:
    # Fallback: usar meses_historicos_disponiveis (comportamento antigo)
    if meses_historicos_disponiveis:
        meses_considerados = meses_historicos_disponiveis.copy()
        
        # Remover meses excluídos
        for mes_excluir in meses_excluir_media:
            mes_excluir_nome = str(mes_excluir).split(' ', 1)[0] if ' ' in str(mes_excluir) else str(mes_excluir)
            if mes_excluir_nome in meses_considerados:
                meses_considerados.remove(mes_excluir_nome)
        
        if meses_considerados:
            meses_para_media = meses_considerados[-num_meses_media:] if len(meses_considerados) >= num_meses_media else meses_considerados
            
            ano_para_periodos = ultimo_ano_dados
            if ' ' in str(ultimo_periodo_dados):
                partes = str(ultimo_periodo_dados).split(' ', 1)
                if len(partes) > 1 and partes[1].isdigit():
                    ano_para_periodos = int(partes[1])
            
            for mes in meses_para_media:
                periodo_com_ano = f"{mes} {ano_para_periodos}"
                periodos_para_media.append(periodo_com_ano)
        else:
            meses_para_media = []
            periodos_para_media = []
    else:
        meses_para_media = []
        periodos_para_media = []

# 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance
# st.sidebar.info(f"🔍 Debug periodos_para_media:\n- Último ano dados: {ultimo_ano_dados}\n- Meses históricos disponíveis: {meses_historicos_disponiveis}\n- Meses para média: {meses_para_media}\n- Períodos para média: {periodos_para_media}\n- Número de meses para média: {num_meses_media}")

# Mostrar resumo da configuração
col_resumo1, col_resumo2 = st.columns(2)
with col_resumo1:
    if periodos_restantes:
        st.success(f"📊 **Períodos a prever:** {', '.join(periodos_restantes)}")
    else:
        st.warning("⚠️ Nenhum período selecionado para prever")

with col_resumo2:
    if periodos_para_media:
        st.success(f"✅ **Períodos para média:** {', '.join(periodos_para_media)} ({len(periodos_para_media)} períodos)")
    else:
        st.error("❌ Nenhum período disponível para calcular a média!")

if meses_excluir_media:
    st.info(f"ℹ️ **Meses excluídos da média:** {', '.join(meses_excluir_media)}")

st.markdown("---")

# Inicializar variável para armazenar configurações temporárias de sensibilidade e inflação
# (será preenchida nos blocos condicionais abaixo)
config_sensibilidade_temp = {
    'sensibilidade_fixo': None,
    'sensibilidade_variavel': None,
    'inflacao_global': None,
    'sensibilidades_type06': None,
    'inflacao_type06': None
}

# Sliders de sensibilidade
st.markdown("### 🎚️ Sensibilidade à Variação de Volume")

# Verificar se Type 06 existe nos dados
if 'Type 06' in df_filtrado.columns:
    # Obter valores únicos de Type 06
    type06_valores = sorted(df_filtrado['Type 06'].dropna().unique().tolist())
    
    if len(type06_valores) > 0:
        st.markdown("""
        Ajuste a sensibilidade para cada categoria de **Type 06**:
        - **0.0**: Nenhuma variação (custo fixo independente do volume)
        - **1.0**: Variação total (custo varia 100% com o volume)
        - **0.5**: Variação parcial (custo varia 50% com o volume)
        """)
        
        # Opção de configuração: Global ou Detalhada
        modo_config = st.radio(
            "Modo de Configuração:",
            ["🌐 Global (Fixo/Variável)", "🎯 Detalhado (por Type 06)"],
            horizontal=True
        )
        
        if modo_config == "🌐 Global (Fixo/Variável)":
            # Modo global (original)
            
            # Inicializar session_state para modo global
            if 'sensibilidade_fixo_aplicada' not in st.session_state:
                st.session_state.sensibilidade_fixo_aplicada = 0.0
            if 'sensibilidade_variavel_aplicada' not in st.session_state:
                st.session_state.sensibilidade_variavel_aplicada = 1.0
            if 'inflacao_global_aplicada' not in st.session_state:
                st.session_state.inflacao_global_aplicada = 0.0
            
            # Layout em 3 colunas: Fixo, Variável, Inflação
            col_sens1, col_sens2, col_infl = st.columns(3)
            
            with col_sens1:
                sensibilidade_fixo_temp = st.slider(
                    "🔵 Sensibilidade - Custo Fixo",
                    min_value=0.0,
                    max_value=1.0,
                    value=st.session_state.sensibilidade_fixo_aplicada,
                    step=0.05,
                    help="Define quanto o custo fixo varia com o volume"
                )
                st.info(f"Custo Fixo variará **{sensibilidade_fixo_temp*100:.0f}%** da variação do volume")
            
            with col_sens2:
                sensibilidade_variavel_temp = st.slider(
                    "🟠 Sensibilidade - Custo Variável",
                    min_value=0.0,
                    max_value=1.0,
                    value=st.session_state.sensibilidade_variavel_aplicada,
                    step=0.05,
                    help="Define quanto o custo variável varia com o volume"
                )
                st.info(f"Custo Variável variará **{sensibilidade_variavel_temp*100:.0f}%** da variação do volume")
            
            with col_infl:
                # Usar slider para inflação também, para manter alinhamento
                inflacao_global_temp = st.slider(
                    "📈 Inflação Global",
                    min_value=0.0,
                    max_value=20.0,
                    value=st.session_state.inflacao_global_aplicada,
                    step=0.5,
                    format="%.2f",
                    help="Inflação aplicada uma única vez no primeiro mês da previsão"
                )
                st.info(f"📊 Inflação: **{inflacao_global_temp:.2f}%**")
                st.caption("💡 Aplicada uma vez no 1º mês e mantida nos demais")
            
            # Armazenar valores temporários para aplicar depois no botão unificado
            config_sensibilidade_temp['sensibilidade_fixo'] = sensibilidade_fixo_temp
            config_sensibilidade_temp['sensibilidade_variavel'] = sensibilidade_variavel_temp
            config_sensibilidade_temp['inflacao_global'] = inflacao_global_temp
            
            # Usar valores aplicados (se existirem) ou temporários
            if 'sensibilidade_fixo_aplicada' in st.session_state:
                sensibilidade_fixo = st.session_state.sensibilidade_fixo_aplicada
            else:
                sensibilidade_fixo = sensibilidade_fixo_temp
            
            if 'sensibilidade_variavel_aplicada' in st.session_state:
                sensibilidade_variavel = st.session_state.sensibilidade_variavel_aplicada
            else:
                sensibilidade_variavel = sensibilidade_variavel_temp
            
            if 'inflacao_global_aplicada' in st.session_state:
                inflacao_global = st.session_state.inflacao_global_aplicada
            else:
                inflacao_global = inflacao_global_temp
            
            # Criar dicionário de sensibilidades (None = usar global)
            sensibilidades_type06 = None
            
            # 🔧 CORREÇÃO: Criar dicionário de inflação global sempre que inflacao_global estiver definida
            # A inflação deve ser aplicada mesmo sem coluna 'Type 06'
            if inflacao_global is not None:
                if 'Type 06' in df_filtrado.columns:
                    # Se há coluna Type 06, criar dicionário com todos os valores Type 06
                    # E também adicionar chave 'GLOBAL' para fallback
                    type06_valores_global = df_filtrado['Type 06'].dropna().unique().tolist()
                    if type06_valores_global:
                        inflacao_type06 = {type06: inflacao_global for type06 in type06_valores_global}
                        # Adicionar chave 'GLOBAL' para garantir que sempre funcione
                        inflacao_type06['GLOBAL'] = inflacao_global
                    else:
                        # Se não há valores Type 06, criar dicionário com chave genérica
                        inflacao_type06 = {'GLOBAL': inflacao_global}
                else:
                    # Se não há coluna Type 06, criar dicionário com chave genérica
                    inflacao_type06 = {'GLOBAL': inflacao_global}
            else:
                inflacao_type06 = None
            
            st.info(f"ℹ️ Usando: Fixo={sensibilidade_fixo*100:.0f}%, Variável={sensibilidade_variavel*100:.0f}%, Inflação={inflacao_global:.2f}%")
            
        else:
            # Modo detalhado por Type 06
            st.markdown("#### 📊 Configuração por Type 06")
            st.info(f"Configure a sensibilidade individualmente para cada um dos **{len(type06_valores)}** valores de Type 06.")
            
            # Inicializar session_state para valores temporários dos sliders
            if 'valores_temp_sens' not in st.session_state:
                st.session_state.valores_temp_sens = {}
            if 'valores_temp_infl' not in st.session_state:
                st.session_state.valores_temp_infl = {}
            if 'widget_key_counter' not in st.session_state:
                st.session_state.widget_key_counter = 0
            
            # Criar dicionários para armazenar sensibilidades e inflação
            sensibilidades_type06 = {}
            inflacao_type06 = {}
            
            # Botões de ação rápida NO TOPO
            st.markdown("##### ⚡ Ações Rápidas - Sensibilidade")
            col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
            
            with col_btn1:
                if st.button("🔵 Todos Fixos (0.0)", use_container_width=True, key="btn_fixos"):
                    for type06 in type06_valores:
                        st.session_state.valores_temp_sens[type06] = 0.0
                    st.rerun()
            
            with col_btn2:
                if st.button("🟠 Todos Variáveis (1.0)", use_container_width=True, key="btn_variaveis"):
                    for type06 in type06_valores:
                        st.session_state.valores_temp_sens[type06] = 1.0
                    st.rerun()
            
            with col_btn3:
                if st.button("⚖️ Todos Médios (0.5)", use_container_width=True, key="btn_medios"):
                    for type06 in type06_valores:
                        st.session_state.valores_temp_sens[type06] = 0.5
                    st.rerun()
            
            with col_btn4:
                if st.button("🧹 Limpar Configurações", use_container_width=True, key="btn_limpar"):
                    st.session_state.sensibilidades_aplicadas = None
                    st.session_state.inflacao_aplicada = None
                    st.session_state.valores_temp_sens = {}
                    st.session_state.valores_temp_infl = {}
                    st.success("Configurações limpas!")
                    st.rerun()
            
            # Botões de ação rápida para INFLAÇÃO
            st.markdown("##### 📈 Ações Rápidas - Inflação")
            st.markdown("Digite o valor de inflação e clique no botão para aplicar a todas as linhas:")
            
            col_infl_input, col_infl_btn1, col_infl_btn2 = st.columns([2, 1, 1])
            
            with col_infl_input:
                inflacao_rapida = st.number_input(
                    "Inflação para todas as linhas (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.5,
                    format="%.2f",
                    key="inflacao_rapida_input",
                    help="Digite o valor e clique em 'Aplicar a Todas'"
                )
            
            with col_infl_btn1:
                st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento
                if st.button("📈 Aplicar a Todas", use_container_width=True, key="btn_aplicar_inflacao"):
                    # Aplicar o novo valor a todas as linhas
                    for type06 in type06_valores:
                        st.session_state.valores_temp_infl[type06] = inflacao_rapida
                    # Incrementar contador para forçar recriação dos widgets
                    st.session_state.widget_key_counter += 1
                    st.rerun()
            
            with col_infl_btn2:
                st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento
                if st.button("🔄 Zerar Inflação", use_container_width=True, key="btn_zerar_inflacao"):
                    # Aplicar zero a todas as linhas
                    for type06 in type06_valores:
                        st.session_state.valores_temp_infl[type06] = 0.0
                    # Incrementar contador para forçar recriação dos widgets
                    st.session_state.widget_key_counter += 1
                    st.rerun()
            
            st.markdown("---")
            
            # Criar tabela interativa com sliders
            st.markdown("##### Tabela de Sensibilidades e Inflação")
            
            st.info("""
            💡 **Inflação**: Digite o percentual de inflação que será aplicado **uma única vez** no primeiro mês da previsão.
            Exemplo: 5% significa que o custo aumentará 5% a partir do primeiro mês e manterá esse valor ajustado nos meses seguintes.
            """)
            
            # Cabeçalho da tabela
            col_header1, col_header2, col_header3, col_header4, col_header5, col_header6 = st.columns([2, 2.5, 1.5, 2.5, 1, 1.5])
            with col_header1:
                st.markdown("**Type 05**")
            with col_header2:
                st.markdown("**Type 06**")
            with col_header3:
                st.markdown("**Tipo**")
            with col_header4:
                st.markdown("**Sensibilidade**")
            with col_header5:
                st.markdown("**%**")
            with col_header6:
                st.markdown("**Inflação %**")
            
            st.markdown("---")
            
            # Criar sliders para cada Type 06
            for type06 in type06_valores:
                # Verificar tipo predominante (Fixo ou Variável)
                df_type06 = df_filtrado[df_filtrado['Type 06'] == type06]
                if 'Custo' in df_type06.columns:
                    tipo_counts = df_type06['Custo'].value_counts()
                    tipo_predominante = tipo_counts.index[0] if len(tipo_counts) > 0 else 'Variável'
                else:
                    tipo_predominante = 'Variável'
                
                # Obter Type 05 correspondente (pegar o mais comum)
                type05_valor = ""
                if 'Type 05' in df_type06.columns:
                    type05_counts = df_type06['Type 05'].value_counts()
                    type05_valor = type05_counts.index[0] if len(type05_counts) > 0 else ""
                
                # Definir valor padrão baseado no tipo ou usar valor temporário
                valor_padrao_sens = 0.0 if tipo_predominante == 'Fixo' else 1.0
                if type06 in st.session_state.valores_temp_sens:
                    valor_padrao_sens = st.session_state.valores_temp_sens[type06]
                
                valor_padrao_infl = 0.0
                if type06 in st.session_state.valores_temp_infl:
                    valor_padrao_infl = st.session_state.valores_temp_infl[type06]
                
                # Criar linha da tabela (mais compacta)
                col1, col2, col3, col4, col5, col6 = st.columns([2, 2.5, 1.5, 2.5, 1, 1.5])
                
                with col1:
                    st.markdown(f"<small>{type05_valor}</small>", unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"<small><b>{type06}</b></small>", unsafe_allow_html=True)
                
                with col3:
                    emoji = "🔵" if tipo_predominante == 'Fixo' else "🟠"
                    tipo_abrev = "F" if tipo_predominante == 'Fixo' else "V"
                    st.markdown(f"<small>{emoji} {tipo_abrev}</small>", unsafe_allow_html=True)
                
                with col4:
                    sens = st.slider(
                        f"Sensibilidade",
                        min_value=0.0,
                        max_value=1.0,
                        value=valor_padrao_sens,
                        step=0.05,
                        key=f"sens_{type06}",
                        label_visibility="collapsed"
                    )
                    sensibilidades_type06[type06] = sens
                    # Atualizar valor temporário
                    st.session_state.valores_temp_sens[type06] = sens
                
                with col5:
                    st.markdown(f"<small><b>{sens*100:.0f}%</b></small>", unsafe_allow_html=True)
                
                with col6:
                    # Usar contador para forçar recriação do widget
                    widget_key = f"infl_{type06}_{st.session_state.widget_key_counter}"
                    inflacao = st.number_input(
                        "Inflação %",
                        min_value=0.0,
                        max_value=100.0,
                        value=valor_padrao_infl,
                        step=0.5,
                        format="%.2f",
                        key=widget_key,
                        label_visibility="collapsed"
                    )
                    inflacao_type06[type06] = inflacao
                    # Atualizar valor temporário
                    st.session_state.valores_temp_infl[type06] = inflacao
            
            # Botão para aplicar configurações
            st.markdown("---")
            
            # Armazenar configurações temporárias em session_state
            if 'sensibilidades_aplicadas' not in st.session_state:
                st.session_state.sensibilidades_aplicadas = None
            if 'inflacao_aplicada' not in st.session_state:
                st.session_state.inflacao_aplicada = None
            
            # Armazenar valores temporários para aplicar depois no botão unificado
            config_sensibilidade_temp['sensibilidades_type06'] = sensibilidades_type06.copy() if sensibilidades_type06 else None
            config_sensibilidade_temp['inflacao_type06'] = inflacao_type06.copy() if inflacao_type06 else None
            
            # Usar configurações aplicadas (se existirem) ou temporárias
            if st.session_state.sensibilidades_aplicadas is not None:
                sensibilidades_type06 = st.session_state.sensibilidades_aplicadas
                inflacao_type06 = st.session_state.inflacao_aplicada
            else:
                # Usar valores temporários (ainda não aplicados)
                # sensibilidades_type06 e inflacao_type06 já contêm os valores temporários
                pass
            
            # Valores globais para compatibilidade (não serão usados)
            sensibilidade_fixo = 0.0
            sensibilidade_variavel = 1.0
    else:
        st.warning("⚠️ Nenhum valor encontrado na coluna Type 06.")
        sensibilidade_fixo = 0.0
        sensibilidade_variavel = 1.0
        sensibilidades_type06 = None
else:
    st.warning("⚠️ Coluna 'Type 06' não encontrada nos dados.")
    # Fallback para modo global simples
    col_sens1, col_sens2 = st.columns(2)
    with col_sens1:
        sensibilidade_fixo = st.slider(
            "🔵 Sensibilidade - Custo Fixo",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05
        )
    with col_sens2:
        sensibilidade_variavel = st.slider(
            "🟠 Sensibilidade - Custo Variável",
            min_value=0.0,
            max_value=1.0,
            value=1.0,
            step=0.05
        )
    sensibilidades_type06 = None

st.markdown("---")

# ====================================================================
# 💰 CUSTOS ESPECÍFICOS / MANUAIS
# ====================================================================
st.markdown("### 💰 Custos Específicos / Manuais")

st.markdown("""
Adicione custos específicos que serão aplicados apenas no forecast (não afetam dados históricos).
Estes custos seguirão as mesmas regras de rateio por veículo que os dados normais.
""")

# Funções auxiliares para gerenciar custos específicos
def carregar_custos_especificos():
    """Carrega custos específicos do arquivo parquet"""
    caminho_custos = os.path.join("dados", "Forecast", "custos_especificos.parquet")
    if os.path.exists(caminho_custos):
        try:
            df = pd.read_parquet(caminho_custos)
            return df
        except Exception as e:
            st.warning(f"⚠️ Erro ao carregar custos específicos: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

def salvar_custos_especificos(df):
    """Salva custos específicos no arquivo parquet"""
    pasta_forecast = os.path.join("dados", "Forecast")
    os.makedirs(pasta_forecast, exist_ok=True)
    caminho_custos = os.path.join(pasta_forecast, "custos_especificos.parquet")
    try:
        df.to_parquet(caminho_custos, index=False)
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar custos específicos: {str(e)}")
        return False

def carregar_rateio_arquivo(ano_selecionado=None):
    """Carrega o arquivo de rateio diretamente do Reporting fluxo anexo.xlsx"""
    try:
        # Determinar caminho do arquivo
        if ano_selecionado and ano_selecionado != "Todos":
            caminho_rateio = os.path.join("dados", str(ano_selecionado), "Reporting fluxo anexo.xlsx")
        else:
            # Tentar encontrar em qualquer pasta de ano
            caminho_rateio = None
            for ano in [2024, 2025]:
                caminho_teste = os.path.join("dados", str(ano), "Reporting fluxo anexo.xlsx")
                if os.path.exists(caminho_teste):
                    caminho_rateio = caminho_teste
                    break
        
        # Se não encontrou, tentar na raiz
        if caminho_rateio is None or not os.path.exists(caminho_rateio):
            caminho_rateio = "Reporting fluxo anexo.xlsx"
        
        if not os.path.exists(caminho_rateio):
            return None
        
        # Ler a guia "Rateio" do arquivo Excel
        df_raw = pd.read_excel(caminho_rateio, sheet_name='Rateio', header=None)
        
        # Excluir a primeira linha (linha de referência)
        df = df_raw.iloc[1:].reset_index(drop=True)
        
        # Usar a primeira linha (que agora é a linha dos nomes/meses) como cabeçalho real
        df.columns = df.iloc[0]
        
        # Excluir a linha usada como cabeçalho
        df = df.iloc[1:].reset_index(drop=True)
        
        # Remover colunas totalmente NaN
        df = df.loc[:, df.notna().any(axis=0)]
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, df.columns.notna()]
        
        # Identificar as colunas que são meses (janeiro a dezembro)
        meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        colunas_meses = [col for col in df.columns if any(mes.lower() in str(col).lower() for mes in meses)]
        
        # Identificar as colunas que NÃO são meses (para usar como id_vars)
        colunas_id = [col for col in df.columns if col not in colunas_meses and pd.notna(col)]
        
        # Transformar as colunas de meses em linhas usando melt
        df = df.melt(id_vars=colunas_id, value_vars=colunas_meses, var_name='Mês', value_name='Rateio')
        df['Rateio'] = pd.to_numeric(df['Rateio'], errors='coerce').fillna(0)
        df = df.rename(columns={'Mês': 'Período'})
        
        # Normalizar Período para capitalizado
        df['Período'] = df['Período'].astype(str).str.strip().str.capitalize()
        
        # Filtrar: Remove 'Veículos' e linhas com Oficina NaN
        df = df[df['Oficina'] != 'Veículos']
        df = df[df['Oficina'].notna()]
        
        return df
    except Exception as e:
        return None

def buscar_rateios_arquivo(oficina, periodo, ano_selecionado=None):
    """Busca rateios por veículo diretamente do arquivo Reporting fluxo anexo.xlsx"""
    df_rateio = carregar_rateio_arquivo(ano_selecionado)
    
    if df_rateio is None or df_rateio.empty:
        return {}
    
    # Normalizar período para comparação
    periodo_normalizado = str(periodo).strip().capitalize()
    if ' ' in periodo_normalizado:
        periodo_normalizado = periodo_normalizado.split(' ', 1)[0]
    
    # Filtrar por Oficina e Período
    mask_oficina = df_rateio['Oficina'].astype(str) == str(oficina)
    periodos_df = df_rateio['Período'].astype(str).str.strip().str.capitalize()
    # Extrair primeira palavra do período (normalizar) - usar apply para extrair primeira palavra
    periodos_df_normalizados = periodos_df.apply(lambda x: x.split(' ', 1)[0] if ' ' in str(x) else str(x))
    mask_periodo = periodos_df_normalizados == periodo_normalizado
    
    df_filtrado = df_rateio[mask_oficina & mask_periodo]
    
    if df_filtrado.empty:
        return {}
    
    # Criar dicionário com rateios por veículo
    rateios = {}
    veiculos = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
    
    for veiculo in veiculos:
        mask_veiculo = df_filtrado['Veículo'].astype(str) == str(veiculo)
        df_veiculo = df_filtrado[mask_veiculo]
        
        if not df_veiculo.empty:
            # Calcular média dos rateios (pode haver múltiplas linhas)
            valores = pd.to_numeric(df_veiculo['Rateio'], errors='coerce').fillna(0.0)
            media = valores.mean()
            if media > 0:
                rateios[f"{veiculo}%"] = float(media)
    
    return rateios

def buscar_info_por_account(account, df_base=None):
    """Busca Type 06, Type 05, Custo e USI a partir do Account (Type 07)"""
    if df_base is None or df_base.empty or not account or pd.isna(account):
        return {}
    
    if 'Account' not in df_base.columns:
        return {}
    
    # Filtrar por Account
    mask = df_base['Account'].astype(str) == str(account)
    df_filtrado = df_base[mask]
    
    if df_filtrado.empty:
        return {}
    
    # Buscar informações (pegar o primeiro valor encontrado, já que Account deve ser único)
    info = {}
    
    if 'Type 06' in df_filtrado.columns:
        type06_valores = df_filtrado['Type 06'].dropna().unique()
        if len(type06_valores) > 0:
            info['Type 06'] = str(type06_valores[0])
    
    if 'Type 05' in df_filtrado.columns:
        type05_valores = df_filtrado['Type 05'].dropna().unique()
        if len(type05_valores) > 0:
            info['Type 05'] = str(type05_valores[0])
    
    if 'Custo' in df_filtrado.columns:
        custo_valores = df_filtrado['Custo'].dropna().unique()
        if len(custo_valores) > 0:
            info['Custo'] = str(custo_valores[0])
            # Custo já está preenchido, não precisa criar Tipo_Custo (redundante)
    
    if 'USI' in df_filtrado.columns:
        usi_valores = df_filtrado['USI'].dropna().unique()
        if len(usi_valores) > 0:
            info['USI'] = str(usi_valores[0])
    
    # Se não encontrou Custo nos dados, tentar buscar do arquivo Base conso
    if 'Custo' not in info:
        try:
            caminho_sapiens = os.path.join("dados", "Dados SAPIENS.xlsx")
            if os.path.exists(caminho_sapiens):
                df_base_conso = pd.read_excel(caminho_sapiens, sheet_name='Base conso')
                if 'Type 04' in df_base_conso.columns:
                    df_base_conso = df_base_conso.rename(columns={'Type 04': 'Custo'})
                if 'Custo' in df_base_conso.columns and 'Type 07' in df_base_conso.columns:
                    df_base_conso = df_base_conso[['Custo', 'Type 07']].rename(columns={'Type 07': 'Account'})
                    df_base_conso = df_base_conso.drop_duplicates(subset=['Account'], keep='first')
                    
                    mask_conso = df_base_conso['Account'].astype(str) == str(account)
                    df_conso_filtrado = df_base_conso[mask_conso]
                    
                    if not df_conso_filtrado.empty:
                        custo_valor = df_conso_filtrado['Custo'].iloc[0]
                        if pd.notna(custo_valor):
                            info['Custo'] = str(custo_valor)
                            custo_str = str(custo_valor).strip().upper()
                            if 'FIXO' in custo_str or 'FIX' in custo_str:
                                info['Tipo_Custo'] = 'Fixo'
                            else:
                                info['Tipo_Custo'] = 'Variável'
        except Exception as e:
            pass  # Se não conseguir ler, continua sem o Custo
    
    return info

# Lista de veículos padrão
VEICULOS_PADRAO = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']

# Carregar custos existentes
df_custos_especificos = carregar_custos_especificos()

# Definir colunas da tabela (mesmas do forecast)
# NOTA: Usamos apenas 'Custo' (original), não 'Tipo_Custo' (redundante)
colunas_tabela_custos = [
    'Oficina', 'Veículo', 'Ano', 'Período', 'Custo',
    'Total', 'Valor', 'Centocst', 'Fornec.', 'Fornecedor', 'USI',
    'Type 05', 'Type 06', 'Account', 'CC21%', 'CC22%', 'CC24%', 
    'CC24 5L%', 'CC24 7L%', 'J516%', 'Tipo_Aplicacao', 
    'Mes_Inicial', 'Meses_Especificos', 'Descricao'
]

# Inicializar DataFrame se vazio
if df_custos_especificos.empty:
    df_custos_especificos = pd.DataFrame(columns=colunas_tabela_custos)
else:
    # Garantir que todas as colunas existam
    for col in colunas_tabela_custos:
        if col not in df_custos_especificos.columns:
            df_custos_especificos[col] = None
    # Reordenar colunas
    colunas_existentes = [col for col in colunas_tabela_custos if col in df_custos_especificos.columns]
    colunas_restantes = [col for col in df_custos_especificos.columns if col not in colunas_existentes]
    df_custos_especificos = df_custos_especificos[colunas_existentes + colunas_restantes]

# Interface para gerenciar custos
tab_visualizar, tab_adicionar = st.tabs(["📋 Visualizar Custos", "➕ Adicionar Custo"])

with tab_visualizar:
    if not df_custos_especificos.empty:
        # Os custos já vêm com uma linha por veículo, então apenas formatar para exibição
        df_custos_formatado = df_custos_especificos.copy()
        
        # Filtrar apenas linhas válidas (com Oficina, Veículo, Período e Total)
        mask_valido = (
            df_custos_formatado['Oficina'].notna() &
            df_custos_formatado['Veículo'].notna() &
            df_custos_formatado['Período'].notna() &
            (df_custos_formatado['Total'].notna()) &
            (pd.to_numeric(df_custos_formatado['Total'], errors='coerce') > 0)
        )
        df_custos_formatado = df_custos_formatado[mask_valido].copy()
        
        if not df_custos_formatado.empty:
            # Renomear 'Total' para 'Valor' para compatibilidade com forecast_completo
            if 'Valor' not in df_custos_formatado.columns:
                df_custos_formatado['Valor'] = pd.to_numeric(df_custos_formatado['Total'], errors='coerce').fillna(0.0)
            else:
                # Se já existe 'Valor', usar o maior entre 'Total' e 'Valor'
                df_custos_formatado['Valor'] = pd.to_numeric(df_custos_formatado['Total'], errors='coerce').fillna(0.0)
            
            # Garantir que todas as colunas necessárias existam (sem Tipo_Custo, apenas Custo)
            colunas_necessarias = [
                'Oficina', 'Veículo', 'Ano', 'Período', 'Custo',
                'Valor', 'Total', 'Centocst', 'Fornec.', 'Fornecedor', 'USI',
                'Type 05', 'Type 06', 'Account', 'Tipo'
            ]
            
            for col in colunas_necessarias:
                if col not in df_custos_formatado.columns:
                    df_custos_formatado[col] = None
            
            # Preencher 'Tipo' com 'BE Manual' se estiver vazio (para custos específicos)
            if 'Tipo' in df_custos_formatado.columns:
                df_custos_formatado['Tipo'] = df_custos_formatado['Tipo'].fillna('BE Manual')
            else:
                df_custos_formatado['Tipo'] = 'BE Manual'
            
            # Remover coluna Tipo_Custo se existir (redundante, usamos apenas Custo)
            if 'Tipo_Custo' in df_custos_formatado.columns:
                df_custos_formatado = df_custos_formatado.drop(columns=['Tipo_Custo'])
            
            # Aplicar padronização de colunas usando a mesma função do forecast
            def padronizar_colunas_custos(df, nome_tipo="Custos Específicos"):
                """Padroniza colunas do DataFrame de custos para garantir mesma ordem do df_final_historico_forecast.xlsx
                Mantém APENAS as colunas que existem no arquivo Excel de referência"""
                if df is None or df.empty:
                    return df
                
                df_padronizado = df.copy()
                
                # Ordem EXATA das colunas do arquivo df_final_historico_forecast.xlsx
                # NOTA: Removemos Tipo_Custo (redundante, usamos apenas Custo)
                ordem_colunas_referencia = [
                    'Account', 'Ano', 'Centrocst', 'Custo', 'Fornec.', 'Fornecedor', 
                    'Mes', 'Oficina', 'Período', 'Soma_Percentuais', 'Tipo', 
                    'Total', 'Type 05', 'Type 06', 'USI', 'Valor', 'Veículo'
                ]
                
                # Coletar todas as colunas do DataFrame
                colunas_existentes = list(df_padronizado.columns)
                
                # Remover Tipo_Custo se existir (redundante)
                if 'Tipo_Custo' in df_padronizado.columns:
                    df_padronizado = df_padronizado.drop(columns=['Tipo_Custo'])
                    colunas_existentes = list(df_padronizado.columns)
                
                # Adicionar colunas faltantes com valores None (apenas as do arquivo de referência)
                for col in ordem_colunas_referencia:
                    if col not in colunas_existentes:
                        df_padronizado[col] = None
                
                # Reordenar DataFrame seguindo EXATAMENTE a ordem de referência
                # Manter apenas colunas que existem no DataFrame ou que estão na referência
                colunas_finais = [col for col in ordem_colunas_referencia if col in df_padronizado.columns]
                df_padronizado = df_padronizado.reindex(columns=colunas_finais)
                
                return df_padronizado
            
            # Aplicar padronização
            df_custos_formatado = padronizar_colunas_custos(df_custos_formatado)
            
            # Criar tabela usando st.dataframe com scroll horizontal e botões de deletar
            st.markdown("#### 📋 Custos Específicos Cadastrados")
            
            # Ordem EXATA das colunas do arquivo df_final_historico_forecast.xlsx
            # NOTA: Removemos Tipo_Custo (redundante, usamos apenas Custo)
            ordem_colunas_referencia = [
                'Account', 'Ano', 'Centrocst', 'Custo', 'Fornec.', 'Fornecedor', 
                'Mes', 'Oficina', 'Período', 'Soma_Percentuais', 'Tipo', 
                'Total', 'Type 05', 'Type 06', 'USI', 'Valor', 'Veículo'
            ]
            
            # Usar APENAS as colunas do arquivo de referência (na mesma ordem)
            colunas_para_exibir = [col for col in ordem_colunas_referencia if col in df_custos_formatado.columns]
            
            # Remover coluna 'Índice' se existir (para evitar duplicação)
            if 'Índice' in df_custos_formatado.columns:
                df_custos_formatado = df_custos_formatado.drop(columns=['Índice'])
            if 'Índice_Original' in df_custos_formatado.columns:
                df_custos_formatado = df_custos_formatado.drop(columns=['Índice_Original'])
            
            # Criar DataFrame para exibição com todas as colunas na ordem correta
            df_display = df_custos_formatado[colunas_para_exibir].copy()
            
            # Resetar índice e adicionar como coluna para referência
            df_display = df_display.reset_index(drop=True)
            df_display.insert(0, 'Índice', df_display.index)
            
            # Configurar AgGrid com seleção múltipla (checkboxes)
            gb = GridOptionsBuilder.from_dataframe(df_display)
            
            # Configurar larguras mínimas e auto-size para todas as colunas
            larguras_colunas = {
                'Índice': 80,
                'Account': 150,
                'Ano': 60,
                'Centrocst': 100,
                'Custo': 120,
                'Fornec.': 80,
                'Fornecedor': 150,
                'Mes': 80,
                'Oficina': 120,
                'Período': 120,
                'Soma_Percentuais': 120,
                'Tipo': 100,
                'Total': 120,
                'Type 05': 100,
                'Type 06': 100,
                'USI': 80,
                'Valor': 120,
                'Veículo': 100
            }
            
            # Colunas numéricas que devem ser formatadas com 2 casas decimais
            colunas_numericas = ['Total', 'Valor', 'Soma_Percentuais']
            
            # Configurar todas as colunas como não editáveis com auto-size
            for col in df_display.columns:
                largura = larguras_colunas.get(col, 120)
                
                # Configuração especial para colunas numéricas
                if col in colunas_numericas:
                    gb.configure_column(
                        col, 
                        editable=False, 
                        sortable=True, 
                        filter=True,
                        minWidth=largura,
                        width=largura,
                        autoSizeColumns=True,
                        wrapText=True,
                        autoHeight=True,
                        type=["numericColumn"],
                        valueFormatter="value != null ? value.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2, useGrouping: true}) : ''"
                    )
                else:
                    gb.configure_column(
                        col, 
                        editable=False, 
                        sortable=True, 
                        filter=True,
                        minWidth=largura,
                        width=largura,
                        autoSizeColumns=True,
                        wrapText=True,
                        autoHeight=True
                    )
            
            # Configurar seleção múltipla com checkboxes
            gb.configure_selection('multiple', use_checkbox=True, header_checkbox=True)
            gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
            gb.configure_side_bar()
            gb.configure_default_column(groupable=False, value=True, enableRowGroup=True, aggFunc='sum', editable=False)
            
            # Fixar coluna de índice à esquerda
            gb.configure_column('Índice', pinned='left', width=80, minWidth=80)
            
            grid_options = gb.build()
            
            # Exibir tabela AgGrid
            grid_response = AgGrid(
                df_display,
                gridOptions=grid_options,
                height=480,  # Aumentado em 20% (400 * 1.2 = 480)
                width='100%',
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=False,
                enable_enterprise_modules=False,
                theme='streamlit',
                key='tabela_custos_aggrid',
                reload_data=False
            )
            
            # Processar exclusões
            indices_para_deletar = []
            
            # Botão para deletar linhas selecionadas
            # CSS para ajustar tamanho do botão
            st.markdown("""
                <style>
                    div[data-testid="column"]:first-child button {
                        min-width: 200px;
                        height: 45px;
                        font-size: 16px;
                        font-weight: 600;
                    }
                </style>
            """, unsafe_allow_html=True)
            
            col_btn1, col_btn2 = st.columns([2, 3])
            with col_btn1:
                if st.button("🗑️ Deletar Selecionadas", type="primary", use_container_width=True):
                    selected_rows = grid_response.get('selected_rows')
                    if selected_rows is not None:
                        # Converter para lista se for DataFrame
                        if isinstance(selected_rows, pd.DataFrame):
                            selected_rows = selected_rows.to_dict('records')
                        elif not isinstance(selected_rows, list):
                            selected_rows = []
                        
                        if len(selected_rows) > 0:
                            # Extrair os índices da coluna 'Índice' das linhas selecionadas
                            indices_selecionados = []
                            for row in selected_rows:
                                if 'Índice' in row and pd.notna(row.get('Índice')):
                                    idx_valor = row.get('Índice')
                                    # O índice pode ser o valor da coluna 'Índice' que corresponde ao índice original
                                    indices_selecionados.append(idx_valor)
                            
                            if indices_selecionados:
                                # Usar os dados das linhas selecionadas diretamente para buscar no original
                                indices_originais_para_deletar = []
                                
                                for row in selected_rows:
                                    # Buscar no DataFrame original usando os campos únicos
                                    mask = pd.Series([True] * len(df_custos_especificos))
                                    
                                    if 'Oficina' in row and pd.notna(row.get('Oficina')):
                                        mask = mask & (df_custos_especificos['Oficina'].astype(str) == str(row['Oficina']))
                                    if 'Veículo' in row and pd.notna(row.get('Veículo')):
                                        mask = mask & (df_custos_especificos['Veículo'].astype(str) == str(row['Veículo']))
                                    if 'Período' in row and pd.notna(row.get('Período')):
                                        mask = mask & (df_custos_especificos['Período'].astype(str) == str(row['Período']))
                                    if 'Total' in row and pd.notna(row.get('Total')):
                                        valor_total = pd.to_numeric(row.get('Total'), errors='coerce')
                                        if pd.notna(valor_total):
                                            mask = mask & (pd.to_numeric(df_custos_especificos['Total'], errors='coerce') == valor_total)
                                    
                                    indices_encontrados = df_custos_especificos[mask].index.tolist()
                                    indices_originais_para_deletar.extend(indices_encontrados)
                                
                                # Remover duplicatas
                                indices_originais_para_deletar = list(set(indices_originais_para_deletar))
                                
                                if indices_originais_para_deletar:
                                    df_custos_especificos = df_custos_especificos.drop(indices_originais_para_deletar).reset_index(drop=True)
                                    if salvar_custos_especificos(df_custos_especificos):
                                        st.success(f"✅ {len(indices_originais_para_deletar)} custo(s) excluído(s) com sucesso!")
                                        st.rerun()
                                else:
                                    st.warning("⚠️ Não foi possível encontrar as linhas correspondentes no arquivo original.")
                            else:
                                st.warning("⚠️ Nenhum índice válido encontrado nas linhas selecionadas.")
                        else:
                            st.warning("⚠️ Selecione pelo menos uma linha na tabela para deletar.")
                    else:
                        st.warning("⚠️ Selecione pelo menos uma linha na tabela para deletar.")
            
            with col_btn2:
                selected_rows = grid_response.get('selected_rows')
                if selected_rows is not None:
                    # Converter para lista se for DataFrame
                    if isinstance(selected_rows, pd.DataFrame):
                        selected_count = len(selected_rows)
                    elif isinstance(selected_rows, list):
                        selected_count = len(selected_rows)
                    else:
                        selected_count = 0
                else:
                    selected_count = 0
                
                if selected_count > 0:
                    st.info(f"📊 {selected_count} linha(s) selecionada(s)")
            
            st.info(f"📊 Total de {len(df_custos_formatado)} linha(s) de custos específicos.")
        else:
            st.info("ℹ️ Nenhum custo específico válido encontrado.")
    else:
        st.info("ℹ️ Nenhum custo específico cadastrado ainda.")

with tab_adicionar:
    st.markdown("#### ➕ Adicionar Novo Custo Específico")
    
    # Obter opções de Oficina e Veículo dos dados
    oficinas_disponiveis = sorted(df_filtrado['Oficina'].dropna().unique().tolist()) if df_filtrado is not None and 'Oficina' in df_filtrado.columns else []
    veiculos_disponiveis = ["Todos"] + sorted(df_filtrado['Veículo'].dropna().unique().tolist()) if df_filtrado is not None and 'Veículo' in df_filtrado.columns else ["Todos"]
    
    col1, col2 = st.columns(2)
    with col1:
        oficina_selecionada = st.selectbox("Oficina:", oficinas_disponiveis)
        
        # Campo Account (Type 07)
        accounts_disponiveis = ["Nenhum"] + sorted(df_filtrado['Account'].dropna().unique().tolist()) if df_filtrado is not None and 'Account' in df_filtrado.columns else ["Nenhum"]
        
        # Inicializar session_state para armazenar informações do Account (usar chaves diferentes dos widgets)
        if 'account_info_cache' not in st.session_state:
            st.session_state.account_info_cache = {}
        
        # Inicializar session_state para os valores dos campos
        if 'type06_valor' not in st.session_state:
            st.session_state.type06_valor = ""
        if 'type05_valor' not in st.session_state:
            st.session_state.type05_valor = ""
        if 'custo_valor' not in st.session_state:
            st.session_state.custo_valor = ""
        if 'tipo_custo_auto' not in st.session_state:
            st.session_state.tipo_custo_auto = "Variável"
        if 'usi_valor' not in st.session_state:
            st.session_state.usi_valor = ""
        
        # Função callback para atualizar campos quando Account mudar
        def atualizar_campos_account():
            account_atual = st.session_state.account_selectbox
            if account_atual and account_atual != "Nenhum":
                # Verificar se já temos as informações em cache
                if account_atual in st.session_state.account_info_cache:
                    cache_info = st.session_state.account_info_cache[account_atual]
                    st.session_state.type06_valor = cache_info.get('Type 06', '')
                    st.session_state.type05_valor = cache_info.get('Type 05', '')
                    st.session_state.custo_valor = cache_info.get('Custo', '')
                    st.session_state.usi_valor = cache_info.get('USI', '')
                    st.session_state.tipo_custo_auto = cache_info.get('Tipo_Custo', 'Variável')
                    
                    # Atualizar também os valores dos widgets diretamente
                    st.session_state.type06_display = st.session_state.type06_valor
                    st.session_state.type05_display = st.session_state.type05_valor
                    st.session_state.custo_display = st.session_state.custo_valor
                else:
                    # Buscar informações do Account
                    df_para_buscar = df_total if df_total is not None and not df_total.empty else df_filtrado
                    
                    if df_para_buscar is not None and not df_para_buscar.empty:
                        info_account = buscar_info_por_account(account_atual, df_para_buscar)
                        
                        if info_account:
                            # Preencher valores
                            st.session_state.type06_valor = info_account.get('Type 06', '')
                            st.session_state.type05_valor = info_account.get('Type 05', '')
                            st.session_state.custo_valor = info_account.get('Custo', '')
                            st.session_state.usi_valor = info_account.get('USI', '')
                            
                            # Atualizar também os valores dos widgets diretamente
                            st.session_state.type06_display = st.session_state.type06_valor
                            st.session_state.type05_display = st.session_state.type05_valor
                            st.session_state.custo_display = st.session_state.custo_valor
                            
                            # Atualizar Tipo_Custo se encontrado
                            if 'Tipo_Custo' in info_account:
                                st.session_state.tipo_custo_auto = info_account['Tipo_Custo']
                            else:
                                st.session_state.tipo_custo_auto = "Variável"
                            
                            # Salvar no cache
                            st.session_state.account_info_cache[account_atual] = {
                                'Type 06': st.session_state.type06_valor,
                                'Type 05': st.session_state.type05_valor,
                                'Custo': st.session_state.custo_valor,
                                'USI': st.session_state.usi_valor,
                                'Tipo_Custo': st.session_state.tipo_custo_auto
                            }
                        else:
                            # Limpar campos se não encontrou
                            st.session_state.type06_valor = ""
                            st.session_state.type05_valor = ""
                            st.session_state.custo_valor = ""
                            st.session_state.tipo_custo_auto = "Variável"
                    else:
                        # Limpar campos se não tem dados
                        st.session_state.type06_valor = ""
                        st.session_state.type05_valor = ""
                        st.session_state.custo_valor = ""
                        st.session_state.tipo_custo_auto = "Variável"
            else:
                # Limpar campos se nenhum Account selecionado
                st.session_state.type06_valor = ""
                st.session_state.type05_valor = ""
                st.session_state.custo_valor = ""
                st.session_state.tipo_custo_auto = "Variável"
        
        account_selecionado = st.selectbox(
            "Account (Type 07):", 
            accounts_disponiveis, 
            help="Selecione o Account para buscar automaticamente Type 06, Type 05 e Custo",
            key="account_selectbox"
        )
        
        # Sempre verificar e atualizar após o selectbox (mesmo que não tenha mudado)
        if account_selecionado and account_selecionado != "Nenhum":
            # Verificar se o Account mudou ou se precisa buscar
            account_anterior = st.session_state.get('account_anterior', None)
            account_mudou = account_anterior != account_selecionado
            
            # Verificar se precisa buscar (Account mudou, não está no cache ou valores estão vazios)
            precisa_buscar = (
                account_mudou or
                account_selecionado not in st.session_state.account_info_cache or
                not st.session_state.type06_valor
            )
            
            if precisa_buscar:
                atualizar_campos_account()
                st.session_state.account_anterior = account_selecionado
                # Forçar atualização da interface
                st.rerun()
        else:
            # Limpar campos se nenhum Account selecionado
            if st.session_state.type06_valor or st.session_state.type05_valor or st.session_state.custo_valor:
                st.session_state.type06_valor = ""
                st.session_state.type05_valor = ""
                st.session_state.custo_valor = ""
                st.session_state.tipo_custo_auto = "Variável"
                st.session_state.account_anterior = None
        
        tipo_aplicacao = st.radio(
            "Tipo de Aplicação:",
            ["Pontual (meses específicos)", "Constante (a partir de um mês)"],
            help="Pontual: aplica apenas nos meses selecionados. Constante: aplica a partir do mês inicial até o final do forecast."
        )
    
    with col2:
        veiculo_selecionado = st.selectbox("Veículo:", veiculos_disponiveis, help="Selecione 'Todos' para aplicar a todos os veículos")
        valor_total = st.number_input("Valor Total (R$):", min_value=0.0, value=0.0, step=1000.0, format="%.2f")
        descricao = st.text_input("Descrição do Custo:", placeholder="Ex: Manutenção preventiva")
    
    # Campos para Type 06, Type 05 e Custo (preenchidos automaticamente)
    # Usar session_state diretamente nos widgets para garantir atualização
    col_type1, col_type2, col_type3 = st.columns(3)
    with col_type1:
        # Usar key diferente e atualizar via session_state
        if 'type06_display' not in st.session_state:
            st.session_state.type06_display = st.session_state.get('type06_valor', '')
        else:
            # Atualizar o valor do widget se o valor mudou
            if st.session_state.get('type06_valor', '') != st.session_state.type06_display:
                st.session_state.type06_display = st.session_state.get('type06_valor', '')
        type06_display = st.text_input("Type 06:", value=st.session_state.type06_display, key="type06_display", disabled=True, help="Preenchido automaticamente ao selecionar Account")
    with col_type2:
        if 'type05_display' not in st.session_state:
            st.session_state.type05_display = st.session_state.get('type05_valor', '')
        else:
            if st.session_state.get('type05_valor', '') != st.session_state.type05_display:
                st.session_state.type05_display = st.session_state.get('type05_valor', '')
        type05_display = st.text_input("Type 05:", value=st.session_state.type05_display, key="type05_display", disabled=True, help="Preenchido automaticamente ao selecionar Account")
    with col_type3:
        if 'custo_display' not in st.session_state:
            st.session_state.custo_display = st.session_state.get('custo_valor', '')
        else:
            if st.session_state.get('custo_valor', '') != st.session_state.custo_display:
                st.session_state.custo_display = st.session_state.get('custo_valor', '')
        custo_display = st.text_input("Custo:", value=st.session_state.custo_display, key="custo_display", disabled=True, help="Preenchido automaticamente ao selecionar Account (determina Tipo_Custo: Fixo ou Variável)")
    
    # Mostrar mensagem se informações foram encontradas
    if account_selecionado and account_selecionado != "Nenhum" and st.session_state.type06_valor:
        st.success(f"✅ Informações encontradas para Account '{account_selecionado}' - Tipo_Custo: {st.session_state.tipo_custo_auto}")
    
    # Configuração de meses baseado no tipo de aplicação
    if tipo_aplicacao == "Pontual (meses específicos)":
        meses_selecionados = st.multiselect(
            "Selecione os meses específicos:",
            options=periodos_restantes if periodos_restantes else meses_ano,
            help="Selecione os meses onde este custo será aplicado"
        )
        mes_inicial = None
    else:  # Constante
        mes_inicial = st.selectbox(
            "Mês inicial:",
            options=periodos_restantes if periodos_restantes else meses_ano,
            help="A partir deste mês, o custo será aplicado em todos os meses seguintes"
        )
        meses_selecionados = None
    
    # Rateio por veículo - busca automática do arquivo original
    st.markdown("#### 📊 Rateio por Veículo")
    st.info("ℹ️ Os rateios serão buscados automaticamente do arquivo 'Reporting fluxo anexo.xlsx' baseado em Oficina e Período. O sistema criará uma linha para cada veículo com o valor rateado.")
    
    # Buscar rateios automaticamente quando Oficina e Período estiverem disponíveis
    rateios_preview = {}
    if oficina_selecionada and periodos_restantes:
        # Usar o primeiro período disponível para buscar rateios (preview)
        periodo_para_buscar = periodos_restantes[0]
        rateios_preview = buscar_rateios_arquivo(oficina_selecionada, periodo_para_buscar, ano_selecionado)
        
        if rateios_preview:
            st.success(f"✅ Rateios encontrados para Oficina '{oficina_selecionada}' e Período '{periodo_para_buscar}':")
            # Mostrar preview dos rateios
            for veiculo_pct, percentual in rateios_preview.items():
                veiculo_nome = veiculo_pct.replace('%', '')
                st.text(f"   • {veiculo_nome}: {percentual*100:.2f}%")
        else:
            st.warning(f"⚠️ Nenhum rateio encontrado para Oficina '{oficina_selecionada}' e Período '{periodo_para_buscar}'. Os rateios serão buscados ao salvar.")
    
    # Botão para adicionar
    if st.button("➕ Adicionar Custo", type="primary"):
        # Validações
        if valor_total <= 0:
            st.error("❌ O valor total deve ser maior que zero.")
        elif tipo_aplicacao == "Pontual (meses específicos)" and not meses_selecionados:
            st.error("❌ Selecione pelo menos um mês para aplicação pontual.")
        elif tipo_aplicacao == "Constante (a partir de um mês)" and not mes_inicial:
            st.error("❌ Selecione o mês inicial para aplicação constante.")
        else:
            # Determinar períodos que serão aplicados
            periodos_aplicar = []
            if tipo_aplicacao == "Pontual (meses específicos)" and meses_selecionados:
                periodos_aplicar = meses_selecionados
            elif tipo_aplicacao == "Constante (a partir de um mês)" and mes_inicial:
                # Todos os períodos a partir do mês inicial
                if periodos_restantes:
                    idx_inicial = periodos_restantes.index(mes_inicial) if mes_inicial in periodos_restantes else 0
                    periodos_aplicar = periodos_restantes[idx_inicial:]
            
            if not periodos_aplicar:
                st.error("❌ Nenhum período selecionado para aplicação.")
            else:
                # Determinar quais veículos serão incluídos
                veiculos = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
                linhas_novas = []
                
                # Se um veículo específico foi selecionado (não "Todos"), aplicar 100% para ele
                veiculo_especifico = None
                if veiculo_selecionado and veiculo_selecionado != "Todos":
                    veiculo_especifico = veiculo_selecionado
                    # Validar se o veículo selecionado está na lista
                    if veiculo_especifico not in veiculos:
                        st.error(f"❌ Veículo '{veiculo_especifico}' não é válido.")
                    else:
                        st.info(f"ℹ️ Veículo específico selecionado: '{veiculo_especifico}'. Rateio será 100% para este veículo.")
                
                for periodo in periodos_aplicar:
                    # Se um veículo específico foi selecionado, aplicar 100% para ele
                    if veiculo_especifico:
                        # Criar rateio manual: 100% para o veículo selecionado, 0% para os outros
                        rateios_periodo = {}
                        for veiculo in veiculos:
                            veiculo_pct = f"{veiculo}%"
                            if veiculo == veiculo_especifico:
                                rateios_periodo[veiculo_pct] = 1.0  # 100%
                            else:
                                rateios_periodo[veiculo_pct] = 0.0  # 0%
                    else:
                        # Buscar rateios do arquivo para este período específico
                        rateios_periodo = buscar_rateios_arquivo(oficina_selecionada, periodo, ano_selecionado)
                        
                        # Se não encontrou rateios, usar distribuição igual
                        if not rateios_periodo or all(v == 0.0 for v in rateios_periodo.values()):
                            st.warning(f"⚠️ Rateios não encontrados para {periodo}. Será usado rateio igual para todos os veículos.")
                            veiculos_pct = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']
                            rateio_igual = 1.0 / len(veiculos_pct)
                            for veiculo_pct in veiculos_pct:
                                rateios_periodo[veiculo_pct] = rateio_igual
                    
                    # Criar uma linha para cada veículo (ou apenas para o veículo selecionado)
                    veiculos_para_criar = [veiculo_especifico] if veiculo_especifico else veiculos
                    
                    for veiculo in veiculos_para_criar:
                        veiculo_pct = f"{veiculo}%"
                        rateio_veiculo = rateios_periodo.get(veiculo_pct, 0.0)
                        
                        # Se rateio é 0, pular este veículo (a menos que seja o veículo específico)
                        if rateio_veiculo == 0.0 and not veiculo_especifico:
                            continue
                        
                        # Calcular valor rateado para este veículo
                        valor_rateado = valor_total * rateio_veiculo
                        
                        # Determinar o ano para preencher automaticamente
                        ano_para_custo = None
                        if ano_selecionado and ano_selecionado != "Todos":
                            # Se ano selecionado é um número, usar diretamente
                            try:
                                ano_para_custo = int(ano_selecionado)
                            except (ValueError, TypeError):
                                ano_para_custo = None
                        
                        # Se não conseguiu determinar o ano, tentar extrair do período
                        if ano_para_custo is None and periodo:
                            periodo_str = str(periodo)
                            # Tentar extrair ano do período (formato: "Janeiro 2024" ou "2024 Janeiro")
                            anos_encontrados = re.findall(r'\b(20\d{2})\b', periodo_str)
                            if anos_encontrados:
                                try:
                                    ano_para_custo = int(anos_encontrados[0])
                                except (ValueError, TypeError):
                                    pass
                        
                        # Se ainda não tem ano, usar o ano atual
                        if ano_para_custo is None:
                            ano_para_custo = datetime.now().year
                        
                        # Criar registro para este veículo e período
                        # Usar 'Custo' (padrão do projeto) em vez de 'Tipo_Custo' (redundante)
                        novo_custo = {
                            'Oficina': oficina_selecionada,
                            'Veículo': veiculo,
                            'Período': periodo,
                            'Custo': st.session_state.custo_valor if st.session_state.custo_valor else st.session_state.tipo_custo_auto,
                            'Total': valor_rateado,  # Valor já rateado para este veículo
                            'Tipo_Aplicacao': tipo_aplicacao,
                            'Mes_Inicial': mes_inicial if tipo_aplicacao == "Constante (a partir de um mês)" else None,
                            'Meses_Especificos': ','.join(meses_selecionados) if meses_selecionados else None,
                            'Descricao': descricao if descricao else "Sem descrição",
                            'Ano': ano_para_custo,
                            'Tipo': 'BE Manual'  # Marcar como BE Manual (custo específico)
                        }
                        
                        # Adicionar Account, Type 06, Type 05, Custo e USI se preenchidos
                        if account_selecionado and account_selecionado != "Nenhum":
                            novo_custo['Account'] = account_selecionado
                        if st.session_state.type06_valor:
                            novo_custo['Type 06'] = st.session_state.type06_valor
                        if st.session_state.type05_valor:
                            novo_custo['Type 05'] = st.session_state.type05_valor
                        if st.session_state.custo_valor:
                            novo_custo['Custo'] = st.session_state.custo_valor
                        if st.session_state.usi_valor:
                            novo_custo['USI'] = st.session_state.usi_valor
                        
                        # Adicionar rateio usado (para referência)
                        novo_custo[veiculo_pct] = rateio_veiculo
                        
                        linhas_novas.append(novo_custo)
                
                # Adicionar todas as linhas ao DataFrame
                if linhas_novas:
                    df_custos_especificos = pd.concat([df_custos_especificos, pd.DataFrame(linhas_novas)], ignore_index=True)
                    
                    # Salvar
                    if salvar_custos_especificos(df_custos_especificos):
                        st.success(f"✅ {len(linhas_novas)} linha(s) de custo específico adicionada(s) com sucesso!")
                        # Limpar cache do Account para forçar nova busca na próxima vez
                        if account_selecionado and account_selecionado != "Nenhum":
                            if account_selecionado in st.session_state.account_info_cache:
                                del st.session_state.account_info_cache[account_selecionado]
                        st.rerun()
                else:
                    st.error("❌ Nenhuma linha foi criada. Verifique os rateios disponíveis.")

st.markdown("---")

# ====================================================================
# 🎯 BOTÃO UNIFICADO PARA APLICAR TODAS AS CONFIGURAÇÕES
# ====================================================================
# Inicializar session_state para configurações do forecast
if 'config_forecast_aplicada' not in st.session_state:
    st.session_state.config_forecast_aplicada = {
        'ultimo_periodo_dados': None,
        'num_meses_prever': None,
        'num_meses_media': None,
        'meses_excluir_media': None,
        'periodos_restantes': None,
        'periodos_para_media': None,
        'ultimo_ano_dados': None
    }

# Armazenar configurações temporárias do forecast
config_forecast_temp = {
    'ultimo_periodo_dados': ultimo_periodo_dados,
    'num_meses_prever': num_meses_prever,
    'num_meses_media': num_meses_media,
    'meses_excluir_media': meses_excluir_media,
    'periodos_restantes': periodos_restantes,
    'periodos_para_media': periodos_para_media,
    'ultimo_ano_dados': ultimo_ano_dados
}

# Botão unificado para aplicar todas as configurações
col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
with col_btn2:
    aplicar_config_forecast = st.button(
        "✅ Aplicar Configurações do Forecast",
        use_container_width=True,
        type="primary",
        help="Clique para aplicar todas as configurações (períodos, sensibilidade e inflação) e atualizar o forecast"
    )

# 🔧 CORREÇÃO: Exibir mensagens de sucesso e log de processamento se forecast foi gerado (após rerun)
if st.session_state.get('forecast_mensagem_sucesso', None):
    st.markdown("---")
    st.success(st.session_state.forecast_mensagem_sucesso)
    st.info(st.session_state.forecast_mensagem_info)
    
    # 🔧 CORREÇÃO: Exibir também o log de processamento se existir
    if 'mensagens_debug' in st.session_state and st.session_state.mensagens_debug:
        mensagens_debug = st.session_state.mensagens_debug.copy()  # Fazer cópia para não modificar a original
        # Contar mensagens por tipo para o título
        total_mensagens = len(mensagens_debug)
        tipos_contagem = {}
        for tipo, _ in mensagens_debug:
            tipos_contagem[tipo] = tipos_contagem.get(tipo, 0) + 1
        
        # Criar título resumido
        titulo_expander = f"📊 Log de Processamento ({total_mensagens} mensagens)"
        if tipos_contagem.get('error', 0) > 0:
            titulo_expander += f" ⚠️ {tipos_contagem['error']} erro(s)"
        elif tipos_contagem.get('warning', 0) > 0:
            titulo_expander += f" ⚠️ {tipos_contagem['warning']} aviso(s)"
        
        with st.expander(titulo_expander, expanded=True):  # 🔧 CORREÇÃO: expanded=True para mostrar por padrão
            for tipo, mensagem in mensagens_debug:
                # Usar texto menor e sem caixas coloridas
                if tipo == "error":
                    st.markdown(f"<small style='color: #ff4b4b;'>❌ {mensagem}</small>", unsafe_allow_html=True)
                elif tipo == "warning":
                    st.markdown(f"<small style='color: #ffa500;'>⚠️ {mensagem}</small>", unsafe_allow_html=True)
                elif tipo == "success":
                    st.markdown(f"<small style='color: #00cc00;'>✅ {mensagem}</small>", unsafe_allow_html=True)
                else:  # info
                    st.markdown(f"<small>{mensagem}</small>", unsafe_allow_html=True)
    
    # Limpar mensagens após exibir
    st.session_state.forecast_mensagem_sucesso = None
    st.session_state.forecast_mensagem_info = None
    # 🔧 CORREÇÃO: NÃO limpar mensagens_debug aqui - deixar para limpar depois de exibir o status completo

# Se clicar em aplicar, salvar todas as configurações
if aplicar_config_forecast:
    # Salvar configurações do forecast
    st.session_state.config_forecast_aplicada = config_forecast_temp.copy()
    
    # Salvar configurações de sensibilidade e inflação
    # Modo Global
    if config_sensibilidade_temp['sensibilidade_fixo'] is not None:
        st.session_state.sensibilidade_fixo_aplicada = config_sensibilidade_temp['sensibilidade_fixo']
    if config_sensibilidade_temp['sensibilidade_variavel'] is not None:
        st.session_state.sensibilidade_variavel_aplicada = config_sensibilidade_temp['sensibilidade_variavel']
    if config_sensibilidade_temp['inflacao_global'] is not None:
        st.session_state.inflacao_global_aplicada = config_sensibilidade_temp['inflacao_global']
    
    # Modo Detalhado
    if config_sensibilidade_temp['sensibilidades_type06'] is not None:
        st.session_state.sensibilidades_aplicadas = config_sensibilidade_temp['sensibilidades_type06']
    if config_sensibilidade_temp['inflacao_type06'] is not None:
        st.session_state.inflacao_aplicada = config_sensibilidade_temp['inflacao_type06']
    
    # 🔧 OTIMIZAÇÃO: Sincronizar Excel/Parquet apenas quando aplicar configurações
    # Ambos os arquivos são gerados juntos neste momento
    try:
        caminho_forecast_vol = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        caminho_forecast_vol_excel = os.path.join("dados", "Forecast", "df_vol_historico.xlsx")
        
        # Se o Excel existe e é mais recente que o Parquet, sincronizar
        if os.path.exists(caminho_forecast_vol_excel):
            if not os.path.exists(caminho_forecast_vol) or os.path.getmtime(caminho_forecast_vol_excel) > os.path.getmtime(caminho_forecast_vol):
                try:
                    df_vol_excel = pd.read_excel(caminho_forecast_vol_excel, engine='openpyxl')
                    df_vol_excel.to_parquet(caminho_forecast_vol, index=False, engine='pyarrow')
                    # Limpar cache do volume histórico para forçar recarregar
                    load_volume_historico_data.clear()
                except Exception:
                    pass
    except Exception:
        pass
    
    # 🔧 CORREÇÃO: Limpar cache ANTES de gerar arquivos (mesma lógica do Forecast copy)
    # Isso garante que os dados sejam recalculados com as novas configurações
    try:
        calcular_medias_forecast.clear()
        load_data.clear()  # 🔧 CORREÇÃO: Limpar cache de load_data para forçar recarregar
        otimizar_tipos_dados.clear()  # 🔧 OTIMIZAÇÃO: Limpar cache de otimização
        # Limpar também cache de volume
        try:
            load_volume_data.clear()
            load_volume_historico_data.clear()
        except:
            pass
    except:
        pass
    
    # 🔧 OTIMIZAÇÃO: Limpar session_state de debug para evitar mensagens antigas
    if 'debug_calcular_medias' in st.session_state:
        del st.session_state['debug_calcular_medias']
    
    # 🔧 CORREÇÃO: Adicionar função calcular_medias_forecast (mesma do Forecast copy)
    # Função para calcular médias com cache (mesma lógica do Forecast copy)
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_medias_forecast(df_filtrado_cache, colunas_adicionais_cache, periodos_para_media_cache, ultimo_periodo_dados_cache=None):
        """Calcula médias mensais históricas com cache, usando apenas os períodos selecionados"""
        # 🔧 CORREÇÃO CRÍTICA: Extrair ano de referência ANTES de qualquer filtro
        # Isso garante que o mesmo ano seja usado em todos os filtros
        ano_referencia_filtro = None
        if periodos_para_media_cache:
            # Extrair ano dos períodos procurados
            anos_nos_periodos = []
            for periodo_procurado in periodos_para_media_cache:
                periodo_str = str(periodo_procurado).strip()
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    if len(partes) > 1 and partes[1].isdigit():
                        anos_nos_periodos.append(int(partes[1]))
            if anos_nos_periodos:
                ano_referencia_filtro = max(anos_nos_periodos)
        if ano_referencia_filtro is None and ultimo_periodo_dados_cache:
            ultimo_periodo_str = str(ultimo_periodo_dados_cache).strip()
            if ' ' in ultimo_periodo_str:
                ano_str = ultimo_periodo_str.split(' ', 1)[1]
                if ano_str.isdigit():
                    ano_referencia_filtro = int(ano_str)
        
        # Filtrar apenas os períodos que serão usados para calcular a média
        if periodos_para_media_cache and 'Período' in df_filtrado_cache.columns:
            # Normalizar períodos procurados (manter mês + ano se disponível)
            periodos_procurados_normalizados = []
            for periodo_procurado in periodos_para_media_cache:
                periodo_str = str(periodo_procurado).strip()
                # Normalizar para minúsculas para comparação
                periodos_procurados_normalizados.append(periodo_str.lower())
            
            # Extrair último mês e ano para validação
            ultimo_mes_limite = None
            ultimo_ano_limite = None
            if ultimo_periodo_dados_cache:
                ultimo_periodo_str = str(ultimo_periodo_dados_cache).strip().lower()
                if ' ' in ultimo_periodo_str:
                    ultimo_mes_limite = ultimo_periodo_str.split(' ', 1)[0]
                    ultimo_ano_limite = int(ultimo_periodo_str.split(' ', 1)[1]) if ultimo_periodo_str.split(' ', 1)[1].isdigit() else None
                else:
                    ultimo_mes_limite = ultimo_periodo_str
                    ultimo_ano_limite = None
            
            # Verificar períodos no DataFrame
            periodos_no_df = df_filtrado_cache['Período'].astype(str).str.strip().str.lower()
            
            # 🔧 CORREÇÃO: Usar ano_referencia_filtro já definido no início da função
            # Se não foi definido, usar ultimo_ano_limite como fallback
            if ano_referencia_filtro is None:
                ano_referencia_filtro = ultimo_ano_limite
            
            # Criar máscara: comparar período completo (mês + ano) quando disponível
            # 🔧 CORREÇÃO CRÍTICA: Garantir que apenas períodos do ano de referência sejam incluídos
            def periodo_corresponde(periodo_df):
                periodo_df_lower = str(periodo_df).strip().lower()
                periodo_df_tem_ano = ' ' in periodo_df_lower and len(periodo_df_lower.split(' ', 1)) > 1
                periodo_df_ano = None
                periodo_df_mes = None
                
                if periodo_df_tem_ano:
                    partes = periodo_df_lower.split(' ', 1)
                    periodo_df_mes = partes[0]
                    if len(partes) > 1 and partes[1].isdigit():
                        periodo_df_ano = int(partes[1])
                else:
                    periodo_df_mes = periodo_df_lower
                
                # 🔧 CORREÇÃO CRÍTICA: Se há ano de referência definido, filtrar APENAS esse ano
                if ano_referencia_filtro:
                    if periodo_df_ano is not None:
                        # Se o período tem ano diferente do ano de referência, NÃO incluir
                        if periodo_df_ano != ano_referencia_filtro:
                            return False
                    else:
                        # Se o período não tem ano mas há ano de referência, NÃO incluir
                        # (evita incluir períodos sem ano quando há períodos com ano)
                        return False
                
                # Verificar se o período está antes ou no último mês selecionado
                if ultimo_mes_limite and ultimo_ano_limite:
                    if periodo_df_ano is not None:
                        # Verificar se está antes do último mês
                        if periodo_df_ano > ultimo_ano_limite:
                            return False
                        if periodo_df_ano == ultimo_ano_limite:
                            # Comparar meses usando índice
                            meses_ano_lower = [m.lower() for m in meses_ano]
                            if periodo_df_mes in meses_ano_lower and ultimo_mes_limite in meses_ano_lower:
                                idx_periodo = meses_ano_lower.index(periodo_df_mes)
                                idx_limite = meses_ano_lower.index(ultimo_mes_limite)
                                if idx_periodo > idx_limite:
                                    return False
                    else:
                        # Se o período do DataFrame não tem ano, verificar apenas pelo mês
                        meses_ano_lower = [m.lower() for m in meses_ano]
                        if periodo_df_mes in meses_ano_lower and ultimo_mes_limite in meses_ano_lower:
                            idx_periodo = meses_ano_lower.index(periodo_df_mes)
                            idx_limite = meses_ano_lower.index(ultimo_mes_limite)
                            if idx_periodo > idx_limite:
                                return False
                
                # Comparação exata primeiro (período completo)
                if periodo_df_lower in periodos_procurados_normalizados:
                    return True
                
                # Se não houver correspondência exata, verificar se o mês corresponde aos períodos procurados
                for periodo_procurado in periodos_procurados_normalizados:
                    periodo_procurado_tem_ano = ' ' in periodo_procurado and len(periodo_procurado.split(' ', 1)) > 1
                    
                    if periodo_procurado_tem_ano:
                        partes_procurado = periodo_procurado.split(' ', 1)
                        mes_procurado = partes_procurado[0]
                        ano_procurado = int(partes_procurado[1]) if len(partes_procurado) > 1 and partes_procurado[1].isdigit() else None
                        
                        # Se ambos têm ano, comparar mês e ano
                        if periodo_df_ano is not None and ano_procurado is not None:
                            if periodo_df_mes == mes_procurado and periodo_df_ano == ano_procurado:
                                return True
                    else:
                        # Se o período procurado não tem ano, comparar apenas o mês
                        mes_procurado = periodo_procurado
                        if periodo_df_mes == mes_procurado:
                            # Se o período do DF tem ano mas o procurado não tem, não incluir
                            if periodo_df_ano is not None:
                                continue
                            return True
                
                return False
            
            df_filtrado_media = df_filtrado_cache[
                periodos_no_df.apply(periodo_corresponde)
            ].copy()
            
            # Armazenar debug em session_state para exibir depois (função cached não pode exibir diretamente)
            if not df_filtrado_media.empty:
                periodos_encontrados_debug = df_filtrado_media['Período'].dropna().unique().tolist()
                st.session_state['debug_calcular_medias'] = {
                    'periodos_procurados': periodos_para_media_cache,
                    'ano_referencia': ano_referencia_filtro,
                    'periodos_encontrados': periodos_encontrados_debug,
                    'total_registros': len(df_filtrado_media)
                }
        else:
            # Se não houver períodos selecionados, usar todos os dados (comportamento original)
            df_filtrado_media = df_filtrado_cache.copy()
        
        if df_filtrado_media.empty:
            # Retornar DataFrames vazios se não houver dados
            colunas_base = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais_cache
            df_medias = pd.DataFrame(columns=colunas_base + ['Total'])
            colunas_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
            df_media_mensal = pd.DataFrame(columns=colunas_media + ['Total'])
            return df_medias, df_media_mensal
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar Período para SEMPRE incluir o ano antes do groupby
        # Isso evita somar meses de anos diferentes (ex: "Novembro 2024" + "Novembro 2025")
        # 🔧 CORREÇÃO: Usar o mesmo ano_referencia_filtro que foi usado no filtro inicial
        ano_referencia = ano_referencia_filtro
        if ano_referencia is None:
            if ultimo_periodo_dados_cache:
                ultimo_periodo_str = str(ultimo_periodo_dados_cache).strip()
                if ' ' in ultimo_periodo_str:
                    ano_str = ultimo_periodo_str.split(' ', 1)[1]
                    if ano_str.isdigit():
                        ano_referencia = int(ano_str)
            elif periodos_para_media_cache:
                # Tentar extrair ano dos períodos selecionados
                for p in periodos_para_media_cache:
                    p_str = str(p).strip()
                    if ' ' in p_str:
                        ano_str = p_str.split(' ', 1)[1]
                        if ano_str.isdigit():
                            ano_referencia = int(ano_str)
                            break
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar Período usando coluna Ano ORIGINAL (não ano_referencia)
        # Estratégia: Se Período não tem ano, usar coluna Ano original dos dados
        # Isso garante que Período e Ano sejam sempre consistentes
        if 'Período' in df_filtrado_media.columns:
            df_filtrado_media = df_filtrado_media.copy()
            # 🔧 CORREÇÃO: Converter Período para string ANTES de qualquer operação (pode ser Categorical)
            df_filtrado_media['Período'] = df_filtrado_media['Período'].astype(str).str.lower().str.strip()
            
            def extrair_ano_do_periodo(periodo_str):
                periodo_str = str(periodo_str).strip()
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    if len(partes) > 1 and partes[1].isdigit():
                        return int(partes[1])
                return None
            
            # Verificar quais períodos não têm ano
            df_filtrado_media['Ano_Do_Periodo'] = df_filtrado_media['Período'].apply(extrair_ano_do_periodo)
            mask_sem_ano_periodo = df_filtrado_media['Ano_Do_Periodo'].isna()
            
            # 🔧 CORREÇÃO: Usar coluna Ano ORIGINAL dos dados para normalizar Período
            if 'Ano' in df_filtrado_media.columns:
                # Converter Ano para int (remover .0 se for float)
                df_filtrado_media['Ano'] = pd.to_numeric(df_filtrado_media['Ano'], errors='coerce')
                
                # Se Período não tem ano, adicionar ano da coluna Ano ORIGINAL
                mask_ano_valido = df_filtrado_media.loc[mask_sem_ano_periodo, 'Ano'].notna()
                # 🔧 CORREÇÃO: Converter Período para string antes de concatenar (pode ser Categorical)
                df_filtrado_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'] = (
                    df_filtrado_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'].astype(str) + ' ' +
                    df_filtrado_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Ano'].astype(int).astype(str)
                )
                # Re-extrair ano após adicionar
                df_filtrado_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Ano_Do_Periodo'] = (
                    df_filtrado_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'].apply(extrair_ano_do_periodo)
                )
                
                # Se Período já tem ano, sincronizar coluna Ano com o ano do Período
                # Mas manter a coluna Ano original se não houver conflito
                mask_ano_periodo_valido = df_filtrado_media['Ano_Do_Periodo'].notna()
                # Sincronizar: usar ano do Período na coluna Ano (já está normalizado)
                df_filtrado_media.loc[mask_ano_periodo_valido, 'Ano'] = df_filtrado_media.loc[mask_ano_periodo_valido, 'Ano_Do_Periodo']
            
            df_filtrado_media = df_filtrado_media.drop(columns=['Ano_Do_Periodo'], errors='ignore')
        
        # 🔧 CORREÇÃO CRÍTICA: Filtrar por ano ANTES do groupby para evitar incluir dados de ambos os anos
        # Isso garante que apenas períodos do ano de referência sejam agrupados
        if ano_referencia:
            if 'Ano' in df_filtrado_media.columns:
                # Filtrar diretamente pela coluna Ano ANTES do groupby
                df_filtrado_media = df_filtrado_media[df_filtrado_media['Ano'] == ano_referencia].copy()
            elif 'Período' in df_filtrado_media.columns:
                # Filtrar pelo ano no Período se não houver coluna Ano
                def periodo_tem_ano_correto_pre_groupby(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        ano_val = periodo_str.split(' ', 1)[1]
                        if ano_val.isdigit():
                            return int(ano_val) == ano_referencia
                    return False
                df_filtrado_media = df_filtrado_media[
                    df_filtrado_media['Período'].apply(periodo_tem_ano_correto_pre_groupby)
                ].copy()
        
        # Agrupar por Oficina, Veículo, Período (com ano) e Tipo_Custo para obter totais
        # 🔧 CORREÇÃO: Se houver coluna Ano, incluí-la no groupby (mesma lógica da TC_Ext)
        # Isso garante que "Julho 2024" e "Julho 2025" sejam tratados separadamente
        # MAS agora df_filtrado_media já contém APENAS o ano de referência
        colunas_groupby = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais_cache
        # Se houver coluna Ano, incluí-la no groupby para evitar somar meses de anos diferentes
        if 'Ano' in df_filtrado_media.columns:
            colunas_groupby = ['Ano'] + colunas_groupby
        colunas_groupby = [col for col in colunas_groupby if col in df_filtrado_media.columns]
        
        # 🔧 CORREÇÃO: Sempre usar coluna 'Total' (nunca 'Valor')
        if 'Total' not in df_filtrado_media.columns:
            raise ValueError("❌ Coluna 'Total' não encontrada nos dados! A origem dos dados deve ter a coluna 'Total'.")
        agg_dict = {'Total': 'sum'}  # Sempre usar 'Total' para ter valores totais reais
        df_medias = df_filtrado_media.groupby(colunas_groupby).agg(agg_dict).reset_index()
        
        # 🔧 CORREÇÃO: df_medias já contém apenas o ano de referência (foi filtrado antes do groupby)
        # Mas vamos garantir novamente para segurança
        df_medias_ano_recente = df_medias.copy()
        if ano_referencia:
            if 'Ano' in df_medias_ano_recente.columns:
                # Filtrar diretamente pela coluna Ano (mais eficiente e correto)
                df_medias_ano_recente = df_medias_ano_recente[df_medias_ano_recente['Ano'] == ano_referencia].copy()
            elif 'Período' in df_medias.columns:
                # Fallback: filtrar pelo ano no Período
                def periodo_tem_ano_correto(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        ano_val = periodo_str.split(' ', 1)[1]
                        if ano_val.isdigit():
                            return int(ano_val) == ano_referencia
                    # Se não tem ano após normalização, excluir
                    return False
                df_medias_ano_recente = df_medias[
                    df_medias['Período'].apply(periodo_tem_ano_correto)
                ].copy()
            else:
                # Se não temos coluna Ano nem Período, usar todos (compatibilidade)
                df_medias_ano_recente = df_medias.copy()
        else:
            # Se não temos ano de referência, usar todos (compatibilidade)
            df_medias_ano_recente = df_medias.copy()
        
        # Calcular média geral mensal por linha (média das médias dos meses selecionados)
        # 🔧 CORREÇÃO CRÍTICA: Garantir que df_medias_ano_recente contém APENAS o ano de referência
        # Se ainda houver dados de outros anos após o filtro, filtrar novamente
        if ano_referencia and 'Ano' in df_medias_ano_recente.columns:
            anos_ainda_presentes = df_medias_ano_recente['Ano'].dropna().unique()
            if len(anos_ainda_presentes) > 1 or (len(anos_ainda_presentes) == 1 and anos_ainda_presentes[0] != ano_referencia):
                # Forçar filtro novamente
                df_medias_ano_recente = df_medias_ano_recente[df_medias_ano_recente['Ano'] == ano_referencia].copy()
        elif ano_referencia and 'Período' in df_medias_ano_recente.columns:
            # Filtrar pelo ano no Período se não houver coluna Ano
            def periodo_tem_ano_correto_final(periodo_val):
                periodo_str = str(periodo_val).strip()
                if ' ' in periodo_str:
                    ano_val = periodo_str.split(' ', 1)[1]
                    if ano_val.isdigit():
                        return int(ano_val) == ano_referencia
                return False
            df_medias_ano_recente = df_medias_ano_recente[
                df_medias_ano_recente['Período'].apply(periodo_tem_ano_correto_final)
            ].copy()
        
        # Calcular média geral mensal por linha (média das médias dos meses selecionados)
        # 🔧 CORREÇÃO: Incluir 'Ano' no groupby se existir (preservar ano para forecast)
        # IMPORTANTE: df_medias_ano_recente já deve conter APENAS o ano de referência
        colunas_groupby_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
        if 'Ano' in df_medias_ano_recente.columns:
            colunas_groupby_media.insert(2, 'Ano')  # Inserir Ano após Veículo
        colunas_groupby_media = [col for col in colunas_groupby_media if col in df_medias_ano_recente.columns]
        
        # 🔧 CORREÇÃO: Sempre usar coluna 'Total' (nunca 'Valor')
        if 'Total' not in df_medias_ano_recente.columns:
            raise ValueError("❌ Coluna 'Total' não encontrada nos dados! A origem dos dados deve ter a coluna 'Total'.")
        agg_dict_media = {'Total': 'mean'}  # Sempre usar 'Total'
        df_media_mensal = df_medias_ano_recente.groupby(colunas_groupby_media).agg(agg_dict_media).reset_index()
        
        # 🔧 VERIFICAÇÃO FINAL: Garantir que não há duplicatas após o agrupamento
        # Se ainda houver duplicatas, significa que o agrupamento não está funcionando corretamente
        if len(colunas_groupby_media) > 0:
            duplicatas_final = df_media_mensal.duplicated(subset=colunas_groupby_media, keep=False)
            if duplicatas_final.any():
                # Se ainda houver duplicatas, forçar agrupamento novamente
                df_media_mensal = df_media_mensal.groupby(
                    colunas_groupby_media, as_index=False
                ).agg(agg_dict_media)
        
        return df_medias, df_media_mensal
    
    # 🔧 CORREÇÃO CRÍTICA: Obter valores APLICADOS do session_state (não usar variáveis temporárias)
    # Isso garante que estamos usando os valores que acabamos de salvar
    sensibilidade_fixo_aplicada = st.session_state.get('sensibilidade_fixo_aplicada', None)
    sensibilidade_variavel_aplicada = st.session_state.get('sensibilidade_variavel_aplicada', None)
    inflacao_global_aplicada = st.session_state.get('inflacao_global_aplicada', None)
    sensibilidades_aplicadas = st.session_state.get('sensibilidades_aplicadas', None)
    inflacao_aplicada = st.session_state.get('inflacao_aplicada', None)
    
    # Obter configurações do forecast aplicadas
    config_forecast_aplicada = st.session_state.get('config_forecast_aplicada', {})
    periodos_restantes = config_forecast_aplicada.get('periodos_restantes', [])
    periodos_para_media = config_forecast_aplicada.get('periodos_para_media', [])
    meses_excluir_media = config_forecast_aplicada.get('meses_excluir_media', [])
    
    # 🆕 NOVA FUNCIONALIDADE: Gerar arquivos diretamente nesta página
    # Os arquivos serão criados aqui, não na página de visualização
    
    # Coletar todas as mensagens de debug/info para exibir em um único expander
    # 🔧 CORREÇÃO: Usar session_state para garantir que as mensagens sejam preservadas e exibidas
    # 🔧 CORREÇÃO CRÍTICA: Limpar mensagens anteriores para não acumular
    st.session_state.mensagens_debug = []
    
    mensagens_debug = st.session_state.mensagens_debug
    
    def adicionar_mensagem(tipo, mensagem):
        """Adiciona mensagem ao log de debug"""
        if 'mensagens_debug' not in st.session_state:
            st.session_state.mensagens_debug = []
        st.session_state.mensagens_debug.append((tipo, mensagem))
    
    # 🔧 DEBUG: Verificar configurações aplicadas (DEPOIS de definir adicionar_mensagem)
    adicionar_mensagem("info", f"📊 Configurações aplicadas:")
    adicionar_mensagem("info", f"   - Último período: {config_forecast_aplicada.get('ultimo_periodo_dados', 'N/A')}")
    adicionar_mensagem("info", f"   - Períodos para média: {periodos_para_media}")
    adicionar_mensagem("info", f"   - Períodos a prever: {periodos_restantes}")
    
    # 🔧 DEBUG: Verificar se as configurações foram obtidas corretamente
    adicionar_mensagem("info", "🔍 **Iniciando geração de arquivos de forecast...**")
    adicionar_mensagem("info", f"📊 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not periodos_restantes:
        adicionar_mensagem("error", "❌ **ERRO:** Nenhum período restante encontrado nas configurações aplicadas!")
        adicionar_mensagem("info", f"📊 Configurações disponíveis: {list(config_forecast_aplicada.keys())}")
        adicionar_mensagem("info", f"📊 Conteúdo de config_forecast_aplicada: {config_forecast_aplicada}")
        st.error("❌ **ERRO:** Nenhum período restante encontrado nas configurações aplicadas!")
        st.stop()
    
    if not periodos_para_media:
        adicionar_mensagem("error", "❌ **ERRO:** Nenhum período para média encontrado nas configurações aplicadas!")
        adicionar_mensagem("info", f"📊 Configurações disponíveis: {list(config_forecast_aplicada.keys())}")
        st.error("❌ **ERRO:** Nenhum período para média encontrado nas configurações aplicadas!")
        st.stop()
    
    adicionar_mensagem("success", "✅ **Configurações validadas:**")
    adicionar_mensagem("info", f"   - Períodos restantes: {len(periodos_restantes)} períodos")
    adicionar_mensagem("info", f"   - Períodos para média: {len(periodos_para_media)} períodos")
    adicionar_mensagem("info", f"   - Meses a excluir da média: {len(meses_excluir_media)} meses")
    adicionar_mensagem("info", f"   - Sensibilidade Fixo: {sensibilidade_fixo_aplicada*100:.0f}%")
    adicionar_mensagem("info", f"   - Sensibilidade Variável: {sensibilidade_variavel_aplicada*100:.0f}%")
    adicionar_mensagem("info", f"   - Inflação Global: {inflacao_global_aplicada:.2f}%")
    
    with st.spinner("🔄 Gerando arquivos de forecast... Isso pode levar alguns minutos."):
        try:
            # CRIAR PASTA FORECAST PRIMEIRO (antes de qualquer processamento)
            pasta_dados = "dados"
            pasta_forecast = os.path.join(pasta_dados, "Forecast")
            try:
                # Criar pasta dados se não existir
                if not os.path.exists(pasta_dados):
                    os.makedirs(pasta_dados, exist_ok=True)
                
                # Criar pasta Forecast dentro de dados
                if not os.path.exists(pasta_forecast):
                    os.makedirs(pasta_forecast, exist_ok=True)
            except Exception as e_pasta_inicial:
                adicionar_mensagem("error", f"❌ Erro ao criar pasta Forecast: {str(e_pasta_inicial)}")
                st.error(f"❌ Erro ao criar pasta Forecast: {str(e_pasta_inicial)}")
                st.stop()
            
            # Carregar dados completos da base original
            caminho_base_original = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
            df_base_completo = None
            
            if os.path.exists(caminho_base_original):
                df_base_completo = pd.read_parquet(caminho_base_original)
            else:
                # Se não existir, tentar carregar do arquivo forecast
                caminho_forecast_original = os.path.join("dados", "historico_consolidado", "df_final_historico_forecast.parquet")
                if os.path.exists(caminho_forecast_original):
                    df_base_completo = pd.read_parquet(caminho_forecast_original)
                    # Converter valores antigos 'Forecast' para 'BE' (compatibilidade com arquivos antigos)
                    if 'Tipo' in df_base_completo.columns:
                        if 'Forecast' in df_base_completo['Tipo'].values:
                            df_base_completo.loc[df_base_completo['Tipo'] == 'Forecast', 'Tipo'] = 'BE'
                else:
                    st.error(f"❌ Arquivo base não encontrado: {caminho_base_original}")
                    st.error("ℹ️ Por favor, verifique se o arquivo existe na pasta 'dados/historico_consolidado/'")
                    st.stop()
            
            # Verificar se df_base_completo foi carregado corretamente
            if df_base_completo is None or df_base_completo.empty:
                st.error("❌ Erro: Não foi possível carregar dados históricos.")
                st.stop()
            
            # 🔧 CORREÇÃO: Limpar cache de aplicar_filtros antes de usar para garantir que os filtros sejam aplicados corretamente
            try:
                aplicar_filtros.clear()
            except:
                pass
            
            # Aplicar filtros (Oficina, Veículo, USI) mas NÃO filtrar por Período
            # para incluir TODOS os períodos históricos no arquivo forecast_completo.parquet
            adicionar_mensagem("info", f"🔍 Aplicando filtros:")
            adicionar_mensagem("info", f"   - Oficinas: {oficina_selecionadas if oficina_selecionadas else 'Todos'}")
            adicionar_mensagem("info", f"   - Veículos: {veiculo_selecionados if veiculo_selecionados else 'Todos'}")
            adicionar_mensagem("info", f"   - USI: {usi_selecionada if usi_selecionada else 'Todos'}")
            adicionar_mensagem("info", f"   - Período: Todos (incluindo todos os períodos históricos)")
            
            df_base_filtrado = aplicar_filtros(
                df_base_completo,
                tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
                tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
                tuple(usi_selecionada) if usi_selecionada else tuple(),
                "Todos"  # NÃO filtrar por período - incluir todos os períodos históricos
            )
            
            adicionar_mensagem("success", f"✅ Filtros aplicados: {len(df_base_completo):,} → {len(df_base_filtrado):,} linhas")
            
            # Verificar se as colunas Type 05, Type 06 e Account existem
            colunas_adicionais = []
            if 'Type 05' in df_base_filtrado.columns:
                colunas_adicionais.append('Type 05')
            if 'Type 06' in df_base_filtrado.columns:
                colunas_adicionais.append('Type 06')
            if 'Account' in df_base_filtrado.columns:
                colunas_adicionais.append('Account')
            
            # 🔧 CORREÇÃO: Criar coluna 'Custo' se não existir
            if 'Custo' not in df_base_filtrado.columns:
                if 'Tipo_Custo' in df_base_filtrado.columns:
                    df_base_filtrado['Custo'] = df_base_filtrado['Tipo_Custo'].apply(
                        lambda x: 'Fixo' if str(x).upper() in ['FIXO', 'FIX', 'FIXED'] else 'Variável'
                    )
                else:
                    df_base_filtrado['Custo'] = 'Variável'
            
            # 🔧 CORREÇÃO: Criar coluna Tipo_Custo se não existir
            if 'Tipo_Custo' not in df_base_filtrado.columns:
                def is_custo_fixo(valor_custo):
                    if pd.isna(valor_custo):
                        return False
                    valor_str = str(valor_custo).strip().upper()
                    palavras_fixo = ['FIXO', 'FIX', 'FIXED']
                    return any(palavra in valor_str for palavra in palavras_fixo)
                
                df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Custo'].apply(is_custo_fixo)
                df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Tipo_Custo'].map({True: 'Fixo', False: 'Variável'})
            
            # 🔧 CORREÇÃO CRÍTICA: Usar a MESMA lógica do Forecast copy (linha 6195-6217)
            # NÃO usar calcular_medias_forecast aqui - calcular diretamente como no Forecast copy
            
            # Calcular médias históricas linha a linha (MESMA LÓGICA DO FORECAST COPY)
            colunas_chave_forecast = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
            if 'Ano' in df_base_filtrado.columns:
                colunas_chave_forecast.insert(2, 'Ano')
            colunas_chave_forecast_existentes = [col for col in colunas_chave_forecast if col in df_base_filtrado.columns]
            
            adicionar_mensagem("info", f"📊 Calculando médias históricas usando {len(colunas_chave_forecast_existentes)} colunas chave...")
            
            # 🔧 CORREÇÃO CRÍTICA: Extrair ano de referência ANTES de qualquer filtro (MESMA LÓGICA DO FORECAST COPY linha 4676-4695)
            # Isso garante que o mesmo ano seja usado em todos os filtros
            ano_referencia_media = None
            if periodos_para_media:
                # Extrair ano dos períodos selecionados
                anos_nos_periodos = []
                for periodo_selecionado in periodos_para_media:
                    periodo_str = str(periodo_selecionado).strip()
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        if len(partes) > 1 and partes[1].isdigit():
                            anos_nos_periodos.append(int(partes[1]))
                if anos_nos_periodos:
                    ano_referencia_media = max(anos_nos_periodos)
            
            # Se não encontrou ano nos períodos, tentar extrair do último período de dados
            if ano_referencia_media is None and config_forecast_aplicada.get('ultimo_periodo_dados'):
                ultimo_periodo_str = str(config_forecast_aplicada['ultimo_periodo_dados']).strip()
                if ' ' in ultimo_periodo_str:
                    ano_str = ultimo_periodo_str.split(' ', 1)[1]
                    if ano_str.isdigit():
                        ano_referencia_media = int(ano_str)
            
            # Filtrar apenas períodos selecionados para média (MESMA LÓGICA DO FORECAST COPY linha 6199-6210)
            if periodos_para_media and 'Período' in df_base_filtrado.columns:
                # 🔧 DEBUG: Verificar períodos disponíveis nos dados
                periodos_disponiveis_df = df_base_filtrado['Período'].unique()
                adicionar_mensagem("info", f"📊 Períodos disponíveis nos dados: {len(periodos_disponiveis_df)} períodos únicos")
                adicionar_mensagem("info", f"📊 Primeiros 10 períodos nos dados: {list(periodos_disponiveis_df[:10])}")
                adicionar_mensagem("info", f"📊 Períodos selecionados para média: {periodos_para_media}")
                adicionar_mensagem("info", f"📊 Ano de referência extraído: {ano_referencia_media}")
                
                # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos dos dados para incluir ano ANTES da correspondência (MESMA LÓGICA DO FORECAST COPY linha 5106-5127)
                # Isso garante que períodos sem ano nos dados sejam normalizados com o ano de referência
                df_base_filtrado_ano = df_base_filtrado.copy()
                if ano_referencia_media and 'Ano' in df_base_filtrado_ano.columns:
                    # Filtrar por ano primeiro
                    df_base_filtrado_ano = df_base_filtrado_ano[df_base_filtrado_ano['Ano'] == ano_referencia_media].copy()
                    
                    # Normalizar Período para incluir ano se não tiver (MESMA LÓGICA DO FORECAST COPY)
                    if 'Período' in df_base_filtrado_ano.columns:
                        def normalizar_periodo_com_ano(periodo_val):
                            periodo_str = str(periodo_val).strip()
                            if ' ' in periodo_str:
                                partes = periodo_str.split(' ', 1)
                                if len(partes) > 1 and partes[1].isdigit():
                                    return periodo_str.lower()  # Já tem ano, apenas normalizar
                            # Se não tem ano, adicionar ano de referência
                            return f"{periodo_str.lower()} {ano_referencia_media}"
                        
                        df_base_filtrado_ano['Período_Norm'] = df_base_filtrado_ano['Período'].astype(str).apply(normalizar_periodo_com_ano)
                    else:
                        df_base_filtrado_ano['Período_Norm'] = df_base_filtrado_ano['Período'].astype(str).str.strip().str.lower()
                else:
                    df_base_filtrado_ano['Período_Norm'] = df_base_filtrado_ano['Período'].astype(str).str.strip().str.lower()
                
                # Normalizar períodos selecionados
                periodos_normalizados = [str(p).strip().lower() for p in periodos_para_media]
                
                # 🔧 CORREÇÃO: Fazer correspondência EXATA apenas (MESMA LÓGICA DO FORECAST COPY linha 6203)
                # Agora que os períodos dos dados foram normalizados com ano, a correspondência exata deve funcionar
                periodos_norm_disponiveis = df_base_filtrado_ano['Período_Norm'].unique()
                adicionar_mensagem("info", f"📊 Períodos normalizados disponíveis após filtro por ano {ano_referencia_media}: {list(periodos_norm_disponiveis[:10])}")
                
                # Correspondência exata (MESMA LÓGICA DO FORECAST COPY)
                df_base_para_media = df_base_filtrado_ano[df_base_filtrado_ano['Período_Norm'].isin(periodos_normalizados)].copy()
                
                if len(df_base_para_media) > 0:
                    adicionar_mensagem("success", f"✅ Períodos encontrados: {len(periodos_normalizados)} períodos, {len(df_base_para_media):,} linhas")
                else:
                    adicionar_mensagem("warning", f"⚠️ Nenhum período encontrado após normalização! Verifique se os períodos nos dados correspondem aos selecionados.")
                    # Fallback: usar todos os períodos do ano
                    df_base_para_media = df_base_filtrado_ano.copy()
                
                # Excluir meses marcados
                if meses_excluir_media:
                    meses_excluir_normalizados = [str(m).strip().lower() for m in meses_excluir_media]
                    linhas_antes_exclusao = len(df_base_para_media)
                    df_base_para_media = df_base_para_media[~df_base_para_media['Período_Norm'].isin(meses_excluir_normalizados)].copy()
                    linhas_removidas = linhas_antes_exclusao - len(df_base_para_media)
                    if linhas_removidas > 0:
                        adicionar_mensagem("info", f"📊 Removidas {linhas_removidas:,} linhas dos meses excluídos")
            else:
                df_base_para_media = df_base_filtrado.copy()
            
            adicionar_mensagem("info", f"📊 Linhas após filtro de períodos: {len(df_base_para_media):,}")
            
            # 🔧 CORREÇÃO CRÍTICA: Filtrar por ano ANTES do groupby (MESMA LÓGICA DO FORECAST COPY linha 4949-4966)
            # Isso garante que apenas períodos do ano de referência sejam agrupados
            if ano_referencia_media:
                if 'Ano' in df_base_para_media.columns:
                    # Filtrar diretamente pela coluna Ano ANTES do groupby
                    linhas_antes_filtro_ano = len(df_base_para_media)
                    df_base_para_media = df_base_para_media[df_base_para_media['Ano'] == ano_referencia_media].copy()
                    linhas_removidas_ano = linhas_antes_filtro_ano - len(df_base_para_media)
                    adicionar_mensagem("info", f"📊 Filtro por ano {ano_referencia_media} ANTES do groupby: {linhas_antes_filtro_ano:,} → {len(df_base_para_media):,} linhas ({linhas_removidas_ano:,} removidas)")
                elif 'Período' in df_base_para_media.columns:
                    # Filtrar pelo ano no Período se não houver coluna Ano
                    def periodo_tem_ano_correto_pre_groupby(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if ' ' in periodo_str:
                            ano_val = periodo_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                return int(ano_val) == ano_referencia_media
                        return False
                    linhas_antes_filtro_ano = len(df_base_para_media)
                    df_base_para_media = df_base_para_media[
                        df_base_para_media['Período'].apply(periodo_tem_ano_correto_pre_groupby)
                    ].copy()
                    linhas_removidas_ano = linhas_antes_filtro_ano - len(df_base_para_media)
                    adicionar_mensagem("info", f"📊 Filtro por ano {ano_referencia_media} ANTES do groupby (via Período): {linhas_antes_filtro_ano:,} → {len(df_base_para_media):,} linhas ({linhas_removidas_ano:,} removidas)")
            
            # Aplicar filtro por ano ANTES do groupby (ano_referencia_media já foi definido acima)
            if ano_referencia_media:
                linhas_antes_filtro_ano = len(df_base_para_media)
                
                # 🔧 DEBUG: Verificar anos presentes antes do filtro
                if 'Ano' in df_base_para_media.columns:
                    anos_presentes_antes = sorted(df_base_para_media['Ano'].dropna().unique())
                    adicionar_mensagem("info", f"📊 Anos presentes nos dados antes do filtro: {anos_presentes_antes}")
                
                if 'Ano' in df_base_para_media.columns:
                    # Filtrar diretamente pela coluna Ano ANTES do groupby
                    df_base_para_media = df_base_para_media[df_base_para_media['Ano'] == ano_referencia_media].copy()
                    linhas_removidas = linhas_antes_filtro_ano - len(df_base_para_media)
                    adicionar_mensagem("success", f"✅ Filtro por ano aplicado: {ano_referencia_media} - {linhas_antes_filtro_ano:,} → {len(df_base_para_media):,} linhas ({linhas_removidas:,} removidas)")
                    
                    # 🔧 DEBUG: Verificar anos presentes depois do filtro
                    anos_presentes_depois = sorted(df_base_para_media['Ano'].dropna().unique())
                    adicionar_mensagem("info", f"📊 Anos presentes nos dados depois do filtro: {anos_presentes_depois}")
                elif 'Período' in df_base_para_media.columns:
                    # Filtrar pelo ano no Período se não houver coluna Ano
                    def periodo_tem_ano_correto_pre_groupby(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if ' ' in periodo_str:
                            ano_val = periodo_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                return int(ano_val) == ano_referencia_media
                        return False
                    df_base_para_media = df_base_para_media[
                        df_base_para_media['Período'].apply(periodo_tem_ano_correto_pre_groupby)
                    ].copy()
                    linhas_removidas = linhas_antes_filtro_ano - len(df_base_para_media)
                    adicionar_mensagem("success", f"✅ Filtro por ano aplicado (via Período): {ano_referencia_media} - {linhas_antes_filtro_ano:,} → {len(df_base_para_media):,} linhas ({linhas_removidas:,} removidas)")
            else:
                adicionar_mensagem("warning", f"⚠️ Ano de referência não encontrado! Usando todos os períodos sem filtro por ano.")
            
            # Calcular média histórica por chave única (MESMA LÓGICA DO FORECAST COPY linha 6212-6217)
            # 🔧 CORREÇÃO: Sempre usar coluna 'Total' (nunca 'Valor')
            if 'Total' not in df_base_para_media.columns:
                adicionar_mensagem("error", "❌ Coluna 'Total' não encontrada nos dados!")
                st.error("❌ Coluna 'Total' não encontrada nos dados!")
                st.stop()
            
            # 🔧 DEBUG: Verificar dados antes do cálculo da média
            total_antes_media = df_base_para_media['Total'].sum()
            linhas_com_total_positivo = (df_base_para_media['Total'] > 0).sum()
            linhas_com_total_zero = (df_base_para_media['Total'] == 0).sum()
            adicionar_mensagem("info", f"📊 DEBUG antes do cálculo da média:")
            adicionar_mensagem("info", f"   - Total de linhas: {len(df_base_para_media):,}")
            adicionar_mensagem("info", f"   - Linhas com Total > 0: {linhas_com_total_positivo:,}")
            adicionar_mensagem("info", f"   - Linhas com Total = 0: {linhas_com_total_zero:,}")
            adicionar_mensagem("info", f"   - Soma total de 'Total': R$ {total_antes_media:,.2f}")
            
            # Verificar períodos únicos
            if 'Período' in df_base_para_media.columns:
                periodos_unicos_media = sorted(df_base_para_media['Período'].dropna().unique())
                adicionar_mensagem("info", f"   - Períodos únicos: {len(periodos_unicos_media)} períodos")
                adicionar_mensagem("info", f"   - Primeiros 10 períodos: {periodos_unicos_media[:10]}")
            
            # 🔧 CORREÇÃO CRÍTICA: Calcular média como (Soma dos Totais) / (Soma dos Volumes) para os períodos selecionados
            # Primeiro, precisamos ter o volume para os mesmos períodos
            # Carregar volume histórico para calcular a soma dos volumes
            df_vol_historico_para_media = load_volume_historico_data()
            df_vol_para_calculo_media = None
            if df_vol_historico_para_media is not None and not df_vol_historico_para_media.empty:
                if 'Oficina' in df_vol_historico_para_media.columns and 'Veículo' in df_vol_historico_para_media.columns and 'Volume' in df_vol_historico_para_media.columns:
                    # Filtrar volume pelos mesmos períodos e ano usados para custo
                    df_vol_para_calculo_media = df_vol_historico_para_media.copy()
                    
                    # Filtrar por ano primeiro
                    if ano_referencia_media and 'Ano' in df_vol_para_calculo_media.columns:
                        df_vol_para_calculo_media = df_vol_para_calculo_media[df_vol_para_calculo_media['Ano'] == ano_referencia_media].copy()
                    
                    # Filtrar pelos períodos selecionados (mesma lógica do volume_base)
                    if periodos_para_media and 'Período' in df_vol_para_calculo_media.columns:
                        # Normalizar períodos do volume
                        periodos_normalizados_vol_media = [str(p).strip().lower() for p in periodos_para_media]
                        df_vol_para_calculo_media['Período_Norm'] = df_vol_para_calculo_media['Período'].astype(str).str.strip().str.lower()
                        
                        # Buscar correspondências
                        periodos_encontrados_vol_media = []
                        for periodo_norm in periodos_normalizados_vol_media:
                            if periodo_norm in df_vol_para_calculo_media['Período_Norm'].values:
                                periodos_encontrados_vol_media.append(periodo_norm)
                            else:
                                mes_periodo = periodo_norm.split()[0] if ' ' in periodo_norm else periodo_norm
                                periodos_parciais = df_vol_para_calculo_media[df_vol_para_calculo_media['Período_Norm'].str.startswith(mes_periodo)]['Período_Norm'].unique()
                                if len(periodos_parciais) > 0:
                                    periodos_encontrados_vol_media.extend(periodos_parciais.tolist())
                        
                        if periodos_encontrados_vol_media:
                            df_vol_para_calculo_media = df_vol_para_calculo_media[df_vol_para_calculo_media['Período_Norm'].isin(periodos_encontrados_vol_media)].copy()
                        
                        # Excluir meses marcados
                        if meses_excluir_media and not df_vol_para_calculo_media.empty:
                            meses_excluir_normalizados = [str(m).strip().lower() for m in meses_excluir_media]
                            df_vol_para_calculo_media = df_vol_para_calculo_media[~df_vol_para_calculo_media['Período_Norm'].isin(meses_excluir_normalizados)].copy()
            
            # 🔧 CORREÇÃO CRÍTICA: Calcular média EXATAMENTE como o Forecast copy (MESMA LÓGICA linha 4968-5044)
            # O Forecast copy NÃO divide por volume, apenas calcula média aritmética dos totais
            # ETAPA 1: Agrupar por chave + período para obter total por período (SUM)
            colunas_groupby_por_periodo = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais
            # Se houver coluna Ano, incluí-la no groupby (MESMA LÓGICA DO FORECAST COPY linha 4974-4975)
            if 'Ano' in df_base_para_media.columns:
                colunas_groupby_por_periodo = ['Ano'] + colunas_groupby_por_periodo
            colunas_groupby_por_periodo = [col for col in colunas_groupby_por_periodo if col in df_base_para_media.columns]
            
            # 🔧 DEBUG: Verificar quantos períodos únicos serão usados para calcular a média
            if 'Período' in df_base_para_media.columns:
                periodos_unicos_antes_groupby = df_base_para_media['Período'].unique()
                adicionar_mensagem("info", f"📊 Períodos únicos ANTES do groupby: {len(periodos_unicos_antes_groupby)} períodos")
                adicionar_mensagem("info", f"📊 Períodos: {list(periodos_unicos_antes_groupby[:10])}")
                if periodos_para_media:
                    adicionar_mensagem("info", f"📊 Períodos SELECIONADOS para média: {len(periodos_para_media)} períodos")
                    adicionar_mensagem("info", f"📊 Períodos selecionados: {periodos_para_media}")
            
            # Agrupar por período e somar totais (MESMA LÓGICA DO FORECAST COPY linha 4981-4982)
            agg_dict = {'Total': 'sum'}  # Sempre usar 'Total' para ter valores totais reais
            df_medias = df_base_para_media.groupby(colunas_groupby_por_periodo).agg(agg_dict).reset_index()
            
            # 🔧 DEBUG: Verificar quantos períodos únicos foram agrupados
            if 'Período' in df_medias.columns:
                periodos_unicos_apos_groupby = df_medias['Período'].unique()
                adicionar_mensagem("info", f"📊 Períodos únicos APÓS o groupby (soma por período): {len(periodos_unicos_apos_groupby)} períodos")
                adicionar_mensagem("info", f"📊 Períodos: {list(periodos_unicos_apos_groupby[:10])}")
            
            # ETAPA 2: Filtrar por ano de referência (MESMA LÓGICA DO FORECAST COPY linha 4986-5009)
            df_medias_ano_recente = df_medias.copy()
            if ano_referencia_media:
                if 'Ano' in df_medias_ano_recente.columns:
                    # Filtrar diretamente pela coluna Ano (mais eficiente e correto)
                    df_medias_ano_recente = df_medias_ano_recente[df_medias_ano_recente['Ano'] == ano_referencia_media].copy()
                elif 'Período' in df_medias.columns:
                    # Fallback: filtrar pelo ano no Período
                    def periodo_tem_ano_correto(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if ' ' in periodo_str:
                            ano_val = periodo_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                return int(ano_val) == ano_referencia_media
                        return False
                    df_medias_ano_recente = df_medias[
                        df_medias['Período'].apply(periodo_tem_ano_correto)
                    ].copy()
            
            # ETAPA 3: Calcular média geral mensal por linha (média dos períodos) (MESMA LÓGICA DO FORECAST COPY linha 5032-5044)
            colunas_groupby_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
            if 'Ano' in df_medias_ano_recente.columns:
                colunas_groupby_media.insert(2, 'Ano')  # Inserir Ano após Veículo
            colunas_groupby_media = [col for col in colunas_groupby_media if col in df_medias_ano_recente.columns]
            
            # 🔧 DEBUG: Verificar quantos períodos únicos serão usados para calcular a média (após filtro por ano)
            if 'Período' in df_medias_ano_recente.columns:
                periodos_unicos_para_media = df_medias_ano_recente['Período'].unique()
                adicionar_mensagem("info", f"📊 Períodos únicos que serão usados para calcular a média: {len(periodos_unicos_para_media)} períodos")
                adicionar_mensagem("info", f"📊 Períodos: {list(periodos_unicos_para_media)}")
                # Verificar se há duplicatas de períodos por chave
                if len(colunas_groupby_media) > 0:
                    # Contar quantos períodos únicos existem por chave
                    periodos_por_chave = df_medias_ano_recente.groupby(colunas_groupby_media)['Período'].nunique()
                    adicionar_mensagem("info", f"📊 Média de períodos únicos por chave: {periodos_por_chave.mean():.2f}")
                    adicionar_mensagem("info", f"📊 Min períodos por chave: {periodos_por_chave.min()}, Max: {periodos_por_chave.max()}")
            
            # 🔧 CORREÇÃO CRÍTICA: Garantir que todas as chaves sejam divididas pelos 4 períodos selecionados
            # O problema é que algumas chaves não têm dados para todos os 4 períodos, então o .mean() divide
            # pela quantidade de períodos que cada chave tem, não pelos 4 períodos selecionados.
            # Solução: Calcular a média manualmente dividindo a soma pelo número de períodos selecionados
            if periodos_para_media and len(periodos_para_media) > 0:
                num_periodos_selecionados = len(periodos_para_media)
                adicionar_mensagem("info", f"📊 Calculando média dividindo pela quantidade de períodos selecionados: {num_periodos_selecionados} períodos")
                
                # Calcular soma dos totais por chave
                df_soma_totais = df_medias_ano_recente.groupby(colunas_groupby_media, as_index=False)['Total'].sum()
                df_soma_totais.rename(columns={'Total': 'Soma_Total'}, inplace=True)
                
                # Dividir pela quantidade de períodos selecionados (não pela quantidade que cada chave tem)
                df_soma_totais['Média_Mensal_Histórica'] = df_soma_totais['Soma_Total'] / num_periodos_selecionados
                df_soma_totais = df_soma_totais.drop(columns=['Soma_Total'], errors='ignore')
                
                df_medias_linha = df_soma_totais
                adicionar_mensagem("info", f"📊 Média calculada como Soma / {num_periodos_selecionados} períodos (garantindo divisão correta)")
            else:
                # Fallback: usar média aritmética normal se não houver períodos selecionados
                agg_dict_media = {'Total': 'mean'}  # Sempre usar 'Total'
                df_medias_linha = df_medias_ano_recente.groupby(colunas_groupby_media).agg(agg_dict_media).reset_index()
                df_medias_linha.rename(columns={'Total': 'Média_Mensal_Histórica'}, inplace=True)
            
            # 🔧 VERIFICAÇÃO FINAL: Garantir que não há duplicatas (MESMA LÓGICA DO FORECAST COPY linha 5046-5054)
            if len(colunas_groupby_media) > 0:
                duplicatas_final = df_medias_linha.duplicated(subset=colunas_groupby_media, keep=False)
                if duplicatas_final.any():
                    # Se ainda houver duplicatas, forçar agrupamento novamente
                    df_medias_linha = df_medias_linha.groupby(
                        colunas_groupby_media, as_index=False
                    ).agg(agg_dict_media)
            
            adicionar_mensagem("info", f"📊 Média calculada como média aritmética dos totais (MESMA LÓGICA DO FORECAST COPY)")
            
            adicionar_mensagem("success", f"✅ Médias calculadas: {len(df_medias_linha):,} linhas")
            
            # Verificar se há valores não-zero
            if 'Média_Mensal_Histórica' in df_medias_linha.columns:
                linhas_com_media = (df_medias_linha['Média_Mensal_Histórica'] > 0).sum()
                total_linhas = len(df_medias_linha)
                soma_total = df_medias_linha['Média_Mensal_Histórica'].sum()
                adicionar_mensagem("info", f"📊 Linhas com Média_Mensal_Histórica > 0: {linhas_com_media:,} de {total_linhas:,} ({linhas_com_media/total_linhas*100:.1f}%)")
                adicionar_mensagem("info", f"📊 Soma total de Média_Mensal_Histórica: R$ {soma_total:,.2f}")
                if linhas_com_media == 0:
                    adicionar_mensagem("warning", "⚠️ AVISO: Nenhuma linha com Média_Mensal_Histórica > 0 encontrada!")
            
            # Remover colunas de normalização se existirem (MESMA LÓGICA DO FORECAST COPY linha 6219-6223)
            colunas_normalizacao_remover = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
            colunas_normalizacao_existentes = [col for col in colunas_normalizacao_remover if col in df_medias_linha.columns]
            if colunas_normalizacao_existentes:
                df_medias_linha = df_medias_linha.drop(columns=colunas_normalizacao_existentes)
            
            # Carregar volume histórico (MESMA LÓGICA DO FORECAST COPY linha 6225-6235)
            df_vol_historico = load_volume_historico_data()
            volume_base = None
            if df_vol_historico is not None and not df_vol_historico.empty:
                # Calcular volume médio histórico (MESMA LÓGICA DO FORECAST COPY)
                # O volume_base será calculado a partir dos períodos selecionados para média
                if 'Oficina' in df_vol_historico.columns and 'Veículo' in df_vol_historico.columns and 'Volume' in df_vol_historico.columns:
                    # Filtrar volumes pelos mesmos períodos usados para calcular a média de custo
                    if periodos_para_media and 'Período' in df_vol_historico.columns:
                        # 🔧 CORREÇÃO CRÍTICA: Filtrar por ano PRIMEIRO, antes de filtrar por períodos
                        # Isso garante que apenas dados do ano correto sejam considerados
                        df_vol_historico_filtrado = df_vol_historico.copy()
                        if ano_referencia_media:
                            if 'Ano' in df_vol_historico_filtrado.columns:
                                df_vol_historico_filtrado = df_vol_historico_filtrado[df_vol_historico_filtrado['Ano'] == ano_referencia_media].copy()
                                adicionar_mensagem("info", f"📊 Volume: Pré-filtro por ano {ano_referencia_media} - {len(df_vol_historico):,} → {len(df_vol_historico_filtrado):,} linhas")
                        
                        # Agora filtrar pelos períodos específicos (já dentro do ano correto)
                        periodos_normalizados_vol = [str(p).strip().lower() for p in periodos_para_media]
                        df_vol_historico_filtrado['Período_Norm'] = df_vol_historico_filtrado['Período'].astype(str).str.strip().str.lower()
                        
                        # 🔧 DEBUG: Verificar períodos disponíveis no volume após filtro por ano
                        periodos_disponiveis_vol = df_vol_historico_filtrado['Período_Norm'].unique()
                        adicionar_mensagem("info", f"📊 Volume: Períodos disponíveis após filtro por ano: {list(periodos_disponiveis_vol[:10])}")
                        
                        # Tentar correspondência exata primeiro
                        periodos_encontrados_vol = []
                        for periodo_norm in periodos_normalizados_vol:
                            if periodo_norm in df_vol_historico_filtrado['Período_Norm'].values:
                                periodos_encontrados_vol.append(periodo_norm)
                                adicionar_mensagem("info", f"📊 Volume: Período '{periodo_norm}' encontrado exatamente")
                            else:
                                # Tentar correspondência parcial (apenas o mês) - mas só se estiver no ano correto
                                # Como já filtramos por ano, todos os períodos aqui são do ano correto
                                mes_periodo = periodo_norm.split()[0] if ' ' in periodo_norm else periodo_norm
                                periodos_parciais = [p for p in periodos_disponiveis_vol if p.startswith(mes_periodo)]
                                if periodos_parciais:
                                    periodos_encontrados_vol.extend(periodos_parciais)
                                    adicionar_mensagem("warning", f"⚠️ Volume: Período '{periodo_norm}' não encontrado exatamente, mas encontrados períodos parciais: {periodos_parciais}")
                        
                        if periodos_encontrados_vol:
                            df_vol_para_media = df_vol_historico_filtrado[df_vol_historico_filtrado['Período_Norm'].isin(periodos_encontrados_vol)].copy()
                            adicionar_mensagem("success", f"✅ Volume: {len(periodos_encontrados_vol)} períodos encontrados, {len(df_vol_para_media):,} linhas")
                        else:
                            df_vol_para_media = pd.DataFrame()
                            adicionar_mensagem("warning", f"⚠️ Volume: Nenhum período encontrado!")
                        
                        # Excluir meses marcados
                        if meses_excluir_media and not df_vol_para_media.empty:
                            meses_excluir_normalizados = [str(m).strip().lower() for m in meses_excluir_media]
                            df_vol_para_media = df_vol_para_media[~df_vol_para_media['Período_Norm'].isin(meses_excluir_normalizados)].copy()
                        
                        # Calcular volume médio histórico (média dos meses selecionados)
                        if not df_vol_para_media.empty:
                            # 🔧 CORREÇÃO: Garantir que ainda está no ano correto (dupla verificação)
                            if ano_referencia_media:
                                linhas_antes_filtro_ano_vol = len(df_vol_para_media)
                                if 'Ano' in df_vol_para_media.columns:
                                    df_vol_para_media = df_vol_para_media[df_vol_para_media['Ano'] == ano_referencia_media].copy()
                                    linhas_removidas_vol = linhas_antes_filtro_ano_vol - len(df_vol_para_media)
                                    adicionar_mensagem("info", f"📊 Volume: Filtro por ano {ano_referencia_media} aplicado - {linhas_antes_filtro_ano_vol:,} → {len(df_vol_para_media):,} linhas ({linhas_removidas_vol:,} removidas)")
                                elif 'Período' in df_vol_para_media.columns:
                                    # Filtrar pelo ano no Período se não houver coluna Ano
                                    def periodo_tem_ano_correto_vol(periodo_val):
                                        periodo_str = str(periodo_val).strip()
                                        if ' ' in periodo_str:
                                            ano_val = periodo_str.split(' ', 1)[1]
                                            if ano_val.isdigit():
                                                return int(ano_val) == ano_referencia_media
                                        return False
                                    df_vol_para_media = df_vol_para_media[
                                        df_vol_para_media['Período'].apply(periodo_tem_ano_correto_vol)
                                    ].copy()
                                    linhas_removidas_vol = linhas_antes_filtro_ano_vol - len(df_vol_para_media)
                                    adicionar_mensagem("info", f"📊 Volume: Filtro por ano {ano_referencia_media} aplicado (via Período) - {linhas_antes_filtro_ano_vol:,} → {len(df_vol_para_media):,} linhas ({linhas_removidas_vol:,} removidas)")
                            
                            # 🔧 CORREÇÃO CRÍTICA: Calcular volume médio em DUAS ETAPAS (MESMA LÓGICA DO FORECAST COPY linha 5582-5590)
                            # 1. Primeiro agrupar por período para obter média por período
                            # 2. Depois agrupar novamente para obter média dos períodos
                            # Isso garante que períodos com múltiplas linhas sejam tratados corretamente
                            if not df_vol_para_media.empty:
                                # Normalizar Período usando coluna Ano ORIGINAL dos dados (MESMA LÓGICA DO FORECAST COPY linha 5545-5580)
                                if 'Período' in df_vol_para_media.columns:
                                    df_vol_para_media = df_vol_para_media.copy()
                                    # Converter Período para string ANTES de qualquer operação
                                    df_vol_para_media['Período'] = df_vol_para_media['Período'].astype(str).str.lower().str.strip()
                                    
                                    def extrair_ano_do_periodo(periodo_str):
                                        periodo_str = str(periodo_str).strip()
                                        if ' ' in periodo_str:
                                            partes = periodo_str.split(' ', 1)
                                            if len(partes) > 1 and partes[1].isdigit():
                                                return int(partes[1])
                                        return None
                                    
                                    df_vol_para_media['Ano_Do_Periodo'] = df_vol_para_media['Período'].apply(extrair_ano_do_periodo)
                                    mask_sem_ano_periodo = df_vol_para_media['Ano_Do_Periodo'].isna()
                                    
                                    # Se Período não tem ano, adicionar ano da coluna Ano ORIGINAL
                                    if 'Ano' in df_vol_para_media.columns:
                                        df_vol_para_media['Ano'] = pd.to_numeric(df_vol_para_media['Ano'], errors='coerce')
                                        mask_ano_valido = df_vol_para_media.loc[mask_sem_ano_periodo, 'Ano'].notna()
                                        # Converter Período para string antes de concatenar
                                        df_vol_para_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'] = (
                                            df_vol_para_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'].astype(str) + ' ' +
                                            df_vol_para_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Ano'].astype(int).astype(str)
                                        )
                                        # Re-extrair ano após adicionar
                                        df_vol_para_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Ano_Do_Periodo'] = (
                                            df_vol_para_media.loc[mask_sem_ano_periodo & mask_ano_valido, 'Período'].apply(extrair_ano_do_periodo)
                                        )
                                        # Sincronizar: se Período tem ano, usar na coluna Ano
                                        mask_ano_periodo_valido = df_vol_para_media['Ano_Do_Periodo'].notna()
                                        df_vol_para_media.loc[mask_ano_periodo_valido, 'Ano'] = df_vol_para_media.loc[mask_ano_periodo_valido, 'Ano_Do_Periodo']
                                    
                                    df_vol_para_media = df_vol_para_media.drop(columns=['Ano_Do_Periodo'], errors='ignore')
                                
                                # ETAPA 1: Agrupar incluindo Ano e Período (MESMA LÓGICA DO FORECAST COPY linha 5582-5586)
                                colunas_groupby_vol_medio = ['Oficina', 'Veículo', 'Período']
                                if 'Ano' in df_vol_para_media.columns:
                                    colunas_groupby_vol_medio.append('Ano')
                                df_vol_medio = df_vol_para_media.groupby(colunas_groupby_vol_medio, as_index=False)['Volume'].mean()
                                
                                # ETAPA 2: Calcular volume médio mensal (média dos meses selecionados do ano correto) (MESMA LÓGICA DO FORECAST COPY linha 5588-5590)
                                volume_base = df_vol_medio.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
                                volume_base.rename(columns={'Volume': 'Volume_Medio_Historico'}, inplace=True)
                                adicionar_mensagem("success", f"✅ Volume médio histórico calculado: {len(volume_base):,} combinações Oficina/Veículo")
                            else:
                                adicionar_mensagem("warning", f"⚠️ Volume: Nenhum dado após filtro por ano {ano_referencia_media}")
                                volume_base = None
                    else:
                        # Se não há períodos selecionados, calcular média de todos os volumes
                        volume_base = df_vol_historico.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
                        volume_base.rename(columns={'Volume': 'Volume_Medio_Historico'}, inplace=True)
            
            # Fazer merge com volume_base para obter Volume_Medio_Historico (MESMA LÓGICA DO FORECAST COPY linha 6226-6235)
            if volume_base is not None and not volume_base.empty:
                colunas_merge_vol = ['Oficina', 'Veículo']
                df_medias_linha = df_medias_linha.merge(
                    volume_base[colunas_merge_vol + ['Volume_Medio_Historico']],
                    on=colunas_merge_vol,
                    how='left'
                )
                df_medias_linha['Volume_Medio_Historico'] = df_medias_linha['Volume_Medio_Historico'].fillna(0.0)
            else:
                df_medias_linha['Volume_Medio_Historico'] = 0.0
            
            # Preparar dados para cálculo de forecast linha a linha
            df_forecast_completo = df_base_filtrado.copy()
            
            # Remover colunas de normalização de período
            colunas_normalizacao_remover = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
            colunas_normalizacao_existentes = [col for col in colunas_normalizacao_remover if col in df_forecast_completo.columns]
            if colunas_normalizacao_existentes:
                df_forecast_completo = df_forecast_completo.drop(columns=colunas_normalizacao_existentes)
            
            # Adicionar Média_Mensal_Histórica e Volume_Medio_Historico via merge
            # 🔧 CORREÇÃO: Definir colunas_chave_forecast_existentes antes de usar
            colunas_chave_forecast = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
            if 'Ano' in df_forecast_completo.columns:
                colunas_chave_forecast.insert(2, 'Ano')
            colunas_chave_forecast_existentes = [col for col in colunas_chave_forecast if col in df_forecast_completo.columns]
            
            colunas_merge_medias = [col for col in colunas_chave_forecast_existentes if col in df_medias_linha.columns]
            colunas_para_merge = colunas_merge_medias.copy()
            if 'Média_Mensal_Histórica' in df_medias_linha.columns:
                colunas_para_merge.append('Média_Mensal_Histórica')
            if 'Volume_Medio_Historico' in df_medias_linha.columns:
                colunas_para_merge.append('Volume_Medio_Historico')
            
            df_forecast_completo = df_forecast_completo.merge(
                df_medias_linha[colunas_para_merge],
                on=colunas_merge_medias,
                how='left'
            )
            
            # Garantir que as colunas existam após o merge
            if 'Média_Mensal_Histórica' not in df_forecast_completo.columns:
                df_forecast_completo['Média_Mensal_Histórica'] = 0.0
            else:
                df_forecast_completo['Média_Mensal_Histórica'] = df_forecast_completo['Média_Mensal_Histórica'].fillna(0.0)
            
            if 'Volume_Medio_Historico' not in df_forecast_completo.columns:
                df_forecast_completo['Volume_Medio_Historico'] = 0.0
            else:
                df_forecast_completo['Volume_Medio_Historico'] = df_forecast_completo['Volume_Medio_Historico'].fillna(0.0)
            
            # 🔧 CORREÇÃO CRÍTICA: Usar valores APLICADOS do session_state (não variáveis temporárias)
            # Converter sensibilidades e inflação para dict se necessário
            sensibilidades_type06_dict = None
            if sensibilidades_aplicadas is not None:
                if isinstance(sensibilidades_aplicadas, dict):
                    sensibilidades_type06_dict = sensibilidades_aplicadas
                elif isinstance(sensibilidades_aplicadas, tuple):
                    sensibilidades_type06_dict = dict(sensibilidades_aplicadas)
            
            inflacao_type06_dict = None
            if inflacao_aplicada is not None:
                if isinstance(inflacao_aplicada, dict):
                    inflacao_type06_dict = inflacao_aplicada
                elif isinstance(inflacao_aplicada, tuple):
                    inflacao_type06_dict = dict(inflacao_aplicada)
            elif inflacao_global_aplicada is not None:
                # Se não há inflação detalhada, usar inflação global
                if 'Type 06' in df_base_filtrado.columns:
                    type06_valores_global = df_base_filtrado['Type 06'].dropna().unique().tolist()
                    if type06_valores_global:
                        inflacao_type06_dict = {type06: inflacao_global_aplicada for type06 in type06_valores_global}
                        inflacao_type06_dict['GLOBAL'] = inflacao_global_aplicada
                    else:
                        inflacao_type06_dict = {'GLOBAL': inflacao_global_aplicada}
                else:
                    inflacao_type06_dict = {'GLOBAL': inflacao_global_aplicada}
            
            # Usar sensibilidades aplicadas (global ou detalhada)
            sensibilidade_fixo = sensibilidade_fixo_aplicada if sensibilidade_fixo_aplicada is not None else 0.0
            sensibilidade_variavel = sensibilidade_variavel_aplicada if sensibilidade_variavel_aplicada is not None else 1.0
            
            # Calcular forecast para cada período linha a linha
            # Carregar volume por mês (MESMA LÓGICA DO FORECAST COPY linha 5596-5615)
            df_vol_por_mes = None
            if df_vol_historico is not None and not df_vol_historico.empty:
                if 'Período' in df_vol_historico.columns and 'Volume' in df_vol_historico.columns:
                    # Volume por mês (incluindo meses futuros) - MESMA LÓGICA DO FORECAST COPY
                    df_vol_para_por_mes = df_vol_historico.copy()
                    
                    # Se há coluna 'Ano', filtrar apenas o ano mais recente (MESMA LÓGICA DO FORECAST COPY linha 5601-5609)
                    if 'Ano' in df_vol_para_por_mes.columns:
                        anos_unicos = df_vol_para_por_mes['Ano'].dropna().unique()
                        if len(anos_unicos) > 1:
                            # Pegar o ano mais recente
                            ano_mais_recente = df_vol_para_por_mes['Ano'].max()
                            df_vol_para_por_mes = df_vol_para_por_mes[df_vol_para_por_mes['Ano'] == ano_mais_recente].copy()
                            adicionar_mensagem("info", f"📊 Volume: Filtrado para ano mais recente: {ano_mais_recente}")
                    
                    # Incluir 'Ano' no groupby (MESMA LÓGICA DO FORECAST COPY linha 5611-5615)
                    colunas_groupby_vol_por_mes = ['Oficina', 'Veículo', 'Período']
                    if 'Ano' in df_vol_para_por_mes.columns:
                        colunas_groupby_vol_por_mes.append('Ano')
                    df_vol_por_mes = df_vol_para_por_mes.groupby(colunas_groupby_vol_por_mes, as_index=False)['Volume'].sum()
                    
                    # 🔧 DEBUG: Mostrar períodos disponíveis no volume
                    if 'Período' in df_vol_por_mes.columns:
                        periodos_vol_disponiveis = sorted(df_vol_por_mes['Período'].astype(str).unique())
                        adicionar_mensagem("info", f"📊 Períodos disponíveis no volume: {len(periodos_vol_disponiveis)} períodos")
                        adicionar_mensagem("info", f"📊 Primeiros 15 períodos: {periodos_vol_disponiveis[:15]}")
                        if 'Ano' in df_vol_por_mes.columns:
                            anos_vol_disponiveis = sorted(df_vol_por_mes['Ano'].dropna().unique())
                            adicionar_mensagem("info", f"📊 Anos disponíveis no volume: {anos_vol_disponiveis}")
            
            # 🔧 DEBUG: Verificar se há períodos para calcular
            adicionar_mensagem("info", "🔍 **Verificando dados para cálculo do forecast...**")
            
            if not periodos_restantes:
                adicionar_mensagem("error", "❌ **ERRO:** Nenhum período restante para calcular forecast!")
                adicionar_mensagem("info", f"📊 Configurações: {config_forecast_aplicada}")
                st.stop()
            
            # 🔧 DEBUG: Verificar se há dados para calcular
            if df_forecast_completo is None or df_forecast_completo.empty:
                adicionar_mensagem("error", "❌ **ERRO:** DataFrame vazio! Não é possível calcular forecast.")
                st.stop()
            
            # 🔧 DEBUG: Verificar se há médias históricas
            if 'Média_Mensal_Histórica' not in df_forecast_completo.columns:
                adicionar_mensagem("error", "❌ **ERRO:** Coluna 'Média_Mensal_Histórica' não encontrada!")
                adicionar_mensagem("info", f"📊 Colunas disponíveis: {list(df_forecast_completo.columns)[:10]}...")
                st.stop()
            
            # Verificar se há pelo menos uma linha com média > 0
            linhas_com_media = (df_forecast_completo['Média_Mensal_Histórica'] > 0).sum()
            if linhas_com_media == 0:
                adicionar_mensagem("warning", "⚠️ **AVISO:** Nenhuma linha com Média_Mensal_Histórica > 0 encontrada!")
                adicionar_mensagem("info", "ℹ️ O forecast será calculado, mas todos os valores podem ser zero.")
            else:
                adicionar_mensagem("success", f"✅ Encontradas {linhas_com_media:,} linhas com Média_Mensal_Histórica > 0")
            
            adicionar_mensagem("info", f"📊 **Iniciando cálculo do forecast:**")
            adicionar_mensagem("info", f"   - Períodos: {len(periodos_restantes)} períodos ({', '.join(periodos_restantes)})")
            adicionar_mensagem("info", f"   - Total de linhas no DataFrame: {len(df_forecast_completo):,}")
            
            for periodo in periodos_restantes:
                # Buscar volume para este período
                volume_mes_serie = None
                if df_vol_por_mes is not None and not df_vol_por_mes.empty:
                    # 🔧 CORREÇÃO CRÍTICA: Extrair mês e ano do período para buscar corretamente (MESMA LÓGICA DO FORECAST COPY)
                    periodo_str = str(periodo).strip()
                    periodo_mes = None
                    periodo_ano = None
                    
                    # Extrair mês e ano do período
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        periodo_mes = partes[0].strip()
                        if len(partes) > 1 and partes[1].strip().isdigit():
                            periodo_ano = int(partes[1].strip())
                    else:
                        periodo_mes = periodo_str
                    
                    # Buscar volume pelo mês (e ano se disponível)
                    if periodo_ano and 'Ano' in df_vol_por_mes.columns:
                        # Buscar por mês E ano
                        vol_mes_df = df_vol_por_mes[
                            (df_vol_por_mes['Período'].astype(str).str.strip().str.capitalize() == periodo_mes.capitalize()) &
                            (df_vol_por_mes['Ano'] == periodo_ano)
                        ].copy()
                    else:
                        # Buscar apenas por mês (fallback)
                        vol_mes_df = df_vol_por_mes[
                            df_vol_por_mes['Período'].astype(str).str.strip().str.capitalize() == periodo_mes.capitalize()
                        ].copy()
                    
                    if not vol_mes_df.empty:
                        vol_mes_df = vol_mes_df.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].sum()
                        adicionar_mensagem("info", f"📊 Volume encontrado para '{periodo}': {len(vol_mes_df):,} combinações Oficina/Veículo")
                        
                        volume_dict = {}
                        for _, row in vol_mes_df.iterrows():
                            chave = (str(row['Oficina']), str(row['Veículo']))
                            volume_dict[chave] = float(row['Volume'])
                        
                        volume_valores = []
                        volume_encontrado_count = 0
                        volume_medio_count = 0
                        for idx in df_forecast_completo.index:
                            chave = (str(df_forecast_completo.loc[idx, 'Oficina']), str(df_forecast_completo.loc[idx, 'Veículo']))
                            if chave in volume_dict:
                                volume_valores.append(volume_dict[chave])
                                volume_encontrado_count += 1
                            elif 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                volume_valores.append(float(df_forecast_completo.loc[idx, 'Volume_Medio_Historico']))
                                volume_medio_count += 1
                            else:
                                volume_valores.append(0.0)
                        
                        volume_mes_serie = pd.Series(volume_valores, index=df_forecast_completo.index)
                        adicionar_mensagem("info", f"📊 Volume para '{periodo}': {volume_encontrado_count:,} encontrados, {volume_medio_count:,} usando médio histórico")
                    else:
                        adicionar_mensagem("warning", f"⚠️ Volume não encontrado para '{periodo}'. Usando volume médio histórico.")
                        if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                            volume_mes_serie = df_forecast_completo['Volume_Medio_Historico'].copy()
                        else:
                            volume_mes_serie = pd.Series(0.0, index=df_forecast_completo.index)
                else:
                    if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                        volume_mes_serie = df_forecast_completo['Volume_Medio_Historico'].copy()
                    else:
                        volume_mes_serie = pd.Series(0.0, index=df_forecast_completo.index)
                
                # Calcular forecast linha a linha (MESMA LÓGICA DO FORECAST COPY linha 6379-6452)
                df_forecast_completo[periodo] = 0.0
                
                for idx in df_forecast_completo.index:
                    try:
                        # 🔧 CORREÇÃO: Verificar se as colunas existem antes de acessá-las (MESMA LÓGICA DO FORECAST COPY linha 6384-6388)
                        if 'Média_Mensal_Histórica' not in df_forecast_completo.columns:
                            df_forecast_completo['Média_Mensal_Histórica'] = 0.0
                        if 'Volume_Medio_Historico' not in df_forecast_completo.columns:
                            df_forecast_completo['Volume_Medio_Historico'] = 0.0
                        
                        media_historica = float(df_forecast_completo.loc[idx, 'Média_Mensal_Histórica'])
                        volume_medio_historico = float(df_forecast_completo.loc[idx, 'Volume_Medio_Historico'])
                        if isinstance(volume_mes_serie, pd.Series):
                            volume_mes = float(volume_mes_serie.loc[idx]) if idx in volume_mes_serie.index else float(volume_medio_historico)
                        else:
                            volume_mes = float(volume_mes_serie) if isinstance(volume_mes_serie, (int, float)) else float(volume_medio_historico)
                        
                        # 🔧 CORREÇÃO: Usar 'Custo' (padrão do projeto) em vez de 'Tipo_Custo' (redundante)
                        # Verificar se Custo existe, senão verificar Tipo_Custo (para compatibilidade)
                        if 'Custo' in df_forecast_completo.columns:
                            tipo_custo = df_forecast_completo.loc[idx, 'Custo']
                        elif 'Tipo_Custo' in df_forecast_completo.columns:
                            tipo_custo = df_forecast_completo.loc[idx, 'Tipo_Custo']
                        else:
                            tipo_custo = 'Variável'
                        
                        # Garantir que tipo_custo seja string válida
                        if pd.isna(tipo_custo) or str(tipo_custo).strip() not in ['Fixo', 'Variável']:
                            tipo_custo = 'Variável'
                    except Exception as e:
                        # 🔧 CORREÇÃO: Em caso de erro, usar valores padrão e continuar (MESMA LÓGICA DO FORECAST COPY linha 6406-6409)
                        adicionar_mensagem("warning", f"⚠️ Erro ao processar linha {idx}: {str(e)}")
                        continue
                    
                    # Calcular proporção de volume
                    if volume_medio_historico > 0:
                        proporcao_volume = volume_mes / volume_medio_historico
                    else:
                        proporcao_volume = 1.0
                    
                    # 🔧 DEBUG: Verificar se está usando volume médio histórico (proporção = 1.0)
                    if abs(proporcao_volume - 1.0) < 0.0001 and volume_medio_historico > 0:
                        # Está usando volume médio histórico, o que significa que não encontrou volume específico
                        # Isso é esperado para períodos futuros, mas pode indicar problema se houver volume disponível
                        pass
                    
                    # Calcular variação percentual
                    variacao_percentual = proporcao_volume - 1.0
                    
                    # 🔧 CORREÇÃO: Obter sensibilidade usando valores APLICADOS
                    if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_completo.columns:
                        type06_valor = df_forecast_completo.loc[idx, 'Type 06']
                        if pd.notna(type06_valor) and type06_valor in sensibilidades_type06_dict:
                            sensibilidade = sensibilidades_type06_dict[type06_valor]
                        else:
                            # Fallback para sensibilidade global baseada no tipo de custo
                            sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                    else:
                        # Modo global: usar sensibilidade baseada no tipo de custo
                        sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                    
                    # Aplicar sensibilidade
                    variacao_ajustada = variacao_percentual * sensibilidade
                    
                    # 🔧 DEBUG: Verificar valores de cálculo (apenas para primeiras linhas para não poluir)
                    if idx < 5 and media_historica > 0:
                        adicionar_mensagem("info", f"🔍 DEBUG linha {idx}: media={media_historica:.2f}, vol_mes={volume_mes:.2f}, vol_medio={volume_medio_historico:.2f}, prop={proporcao_volume:.4f}, var={variacao_percentual:.4f}, sens={sensibilidade:.2f}, var_ajust={variacao_ajustada:.4f}")
                    
                    # Obter inflação (MESMA LÓGICA DO FORECAST COPY linha 6434-6445)
                    if inflacao_type06_dict is not None and 'Type 06' in df_forecast_completo.columns:
                        type06_valor = df_forecast_completo.loc[idx, 'Type 06']
                        if pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                            inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0
                        else:
                            inflacao_percentual = 0.0
                    else:
                        if inflacao_type06_dict is not None:
                            primeiro_valor = next(iter(inflacao_type06_dict.values()), 0.0)
                            inflacao_percentual = primeiro_valor / 100.0
                        else:
                            inflacao_percentual = 0.0
                    
                    # Calcular forecast
                    fator_variacao = 1.0 + variacao_ajustada
                    fator_inflacao = 1.0 + inflacao_percentual
                    forecast = media_historica * fator_variacao * fator_inflacao
                    
                    df_forecast_completo.loc[idx, periodo] = forecast
                
                # 🔧 DEBUG: Verificar se o forecast foi calculado para este período
                valores_forecast = df_forecast_completo[periodo].sum()
                linhas_com_forecast = (df_forecast_completo[periodo] > 0).sum()
                if valores_forecast == 0:
                    adicionar_mensagem("warning", f"⚠️ **AVISO:** Forecast para '{periodo}' está zerado. Verifique se há médias históricas > 0.")
                else:
                    adicionar_mensagem("success", f"✅ **Forecast calculado para '{periodo}':** Total = R$ {valores_forecast:,.2f} ({linhas_com_forecast:,} linhas com valor > 0)")
                
                # 💰 NOTA: Custos específicos são adicionados como linhas separadas mais abaixo (linha ~4777)
                # Não adicionar aqui para evitar duplicação - os custos específicos já são incluídos
                # como linhas separadas no df_final_historico_forecast
            
            # 🔧 DEBUG: Verificar se todos os períodos foram calculados
            adicionar_mensagem("success", f"✅ **Forecast calculado para todos os {len(periodos_restantes)} períodos!**")
            
            # Transformar colunas de forecast em linhas na coluna "Período"
            linhas_finais = []
            
            # 1. Adicionar linhas históricas
            df_historico_linhas = df_base_filtrado.copy()
            
            # Remover colunas de forecast se existirem
            for periodo in periodos_restantes:
                if periodo in df_historico_linhas.columns:
                    df_historico_linhas = df_historico_linhas.drop(columns=[periodo])
            
            # Normalizar Período no histórico
            if 'Período' in df_historico_linhas.columns:
                def normalizar_periodo_historico(periodo_val, ano_val=None):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        mes_nome = periodo_str.split(' ', 1)[0].strip().capitalize()
                        return mes_nome
                    return periodo_str.strip().capitalize()
                
                if 'Ano' in df_historico_linhas.columns:
                    df_historico_linhas['Período'] = df_historico_linhas.apply(
                        lambda row: normalizar_periodo_historico(row['Período'], row.get('Ano')), axis=1
                    )
                else:
                    df_historico_linhas['Período'] = df_historico_linhas['Período'].apply(
                        lambda p: normalizar_periodo_historico(p)
                    )
            
            # 🔧 CORREÇÃO CRÍTICA: Filtrar períodos históricos para remover os que serão previstos (MESMA LÓGICA DO FORECAST COPY linha 6502-6554)
            # Isso garante que não haverá duplicação quando juntar histórico + forecast
            # O arquivo forecast_historico.parquet NÃO deve conter os meses que estão sendo previstos
            linhas_antes_filtro_historico = len(df_historico_linhas)
            
            # Extrair apenas o nome do mês dos períodos de forecast
            meses_forecast = []
            anos_forecast = []
            periodos_forecast_formatados = []
            for periodo in periodos_restantes:
                periodo_str = str(periodo).strip()
                if ' ' in periodo_str:
                    mes_nome = periodo_str.split(' ', 1)[0].strip().capitalize()
                    partes = periodo_str.split(' ', 1)
                    if len(partes) > 1 and partes[1].isdigit():
                        ano_val = int(partes[1])
                        anos_forecast.append(ano_val)
                        periodos_forecast_formatados.append(f"{mes_nome} {ano_val}")
                else:
                    mes_nome = periodo_str.strip().capitalize()
                    periodos_forecast_formatados.append(mes_nome)
                meses_forecast.append(mes_nome)
            
            # Filtrar linhas históricas: remover períodos que correspondem aos meses de forecast
            if 'Período' in df_historico_linhas.columns:
                # Se há coluna Ano e anos de forecast, filtrar por mês E ano (mais preciso)
                if 'Ano' in df_historico_linhas.columns and anos_forecast:
                    # 🔧 CORREÇÃO: Filtrar períodos que correspondem EXATAMENTE aos meses e anos de forecast
                    mask_remover = (df_historico_linhas['Período'].isin(meses_forecast)) & \
                                  (df_historico_linhas['Ano'].isin(anos_forecast))
                    df_historico_linhas = df_historico_linhas[~mask_remover].copy()
                    
                    # Confirmação: mostrar quantas linhas foram removidas
                    linhas_removidas = linhas_antes_filtro_historico - len(df_historico_linhas)
                    if linhas_removidas > 0:
                        adicionar_mensagem("success", f"✅ CONFIRMADO: Removidas {linhas_removidas:,} linhas históricas dos períodos que serão previstos: {', '.join(periodos_forecast_formatados)}")
                        adicionar_mensagem("info", f"📊 O arquivo forecast_historico.parquet NÃO conterá estes períodos para evitar duplicação ao juntar com forecast_previsao.parquet")
                    else:
                        adicionar_mensagem("info", f"ℹ️ Nenhuma linha histórica removida (períodos de forecast não encontrados no histórico)")
                else:
                    # Se não há coluna Ano ou não há anos de forecast, filtrar apenas por mês
                    mask_remover = df_historico_linhas['Período'].isin(meses_forecast)
                    df_historico_linhas = df_historico_linhas[~mask_remover].copy()
                    
                    # Confirmação: mostrar quantas linhas foram removidas
                    linhas_removidas = linhas_antes_filtro_historico - len(df_historico_linhas)
                    if linhas_removidas > 0:
                        adicionar_mensagem("success", f"✅ CONFIRMADO: Removidas {linhas_removidas:,} linhas históricas dos meses que serão previstos: {', '.join(meses_forecast)}")
                        adicionar_mensagem("info", f"📊 O arquivo forecast_historico.parquet NÃO conterá estes meses para evitar duplicação ao juntar com forecast_previsao.parquet")
                    else:
                        adicionar_mensagem("info", f"ℹ️ Nenhuma linha histórica removida (meses de forecast não encontrados no histórico)")
            else:
                adicionar_mensagem("warning", f"⚠️ Coluna 'Período' não encontrada no histórico. Não é possível filtrar períodos de forecast.")
            
            df_historico_linhas['Tipo'] = 'Histórico'
            linhas_finais.append(df_historico_linhas)
            
            # 2. Criar linhas de forecast para cada período (MESMA LÓGICA DO FORECAST COPY linha 6560-6577)
            # IMPORTANTE: Usar df_forecast_completo com valores calculados
            df_fonte_forecast = df_forecast_completo.copy()
            adicionar_mensagem("info", "ℹ️ Usando valores calculados do df_forecast_completo para gerar linhas de forecast.")
            
            # 🔧 CORREÇÃO: Fazer merge com histórico para garantir que colunas importantes estejam presentes (MESMA LÓGICA DO FORECAST COPY linha 6579-6603)
            colunas_importantes_historico = ['Custo', 'Centocst', 'Fornec.', 'Fornecedor', 'USI']
            if df_fonte_forecast is not None and df_historico_linhas is not None and not df_historico_linhas.empty:
                colunas_faltantes = [col for col in colunas_importantes_historico 
                                   if col in df_historico_linhas.columns and col not in df_fonte_forecast.columns]
                
                if colunas_faltantes:
                    colunas_merge = ['Oficina', 'Veículo']
                    if 'Ano' in df_historico_linhas.columns and 'Ano' in df_fonte_forecast.columns:
                        colunas_merge.append('Ano')
                    if 'Tipo_Custo' in df_historico_linhas.columns and 'Tipo_Custo' in df_fonte_forecast.columns:
                        colunas_merge.append('Tipo_Custo')
                    if 'Fornec.' in df_historico_linhas.columns and 'Fornec.' in df_fonte_forecast.columns:
                        colunas_merge.append('Fornec.')
                    
                    colunas_merge_existentes = [col for col in colunas_merge if col in df_historico_linhas.columns and col in df_fonte_forecast.columns]
                    
                    if colunas_merge_existentes:
                        df_historico_agrupado = df_historico_linhas.groupby(
                            colunas_merge_existentes,
                            as_index=False
                        ).first()[colunas_merge_existentes + colunas_faltantes]
                        
                        df_fonte_forecast = df_fonte_forecast.merge(
                            df_historico_agrupado,
                            on=colunas_merge_existentes,
                            how='left'
                        )
                        
                        colunas_ainda_faltantes = [col for col in colunas_faltantes 
                                                  if col in df_fonte_forecast.columns and df_fonte_forecast[col].isna().any()]
                        if colunas_ainda_faltantes:
                            colunas_merge_simples = ['Oficina', 'Veículo']
                            if 'Ano' in df_historico_linhas.columns and 'Ano' in df_fonte_forecast.columns:
                                colunas_merge_simples.append('Ano')
                            colunas_merge_simples_existentes = [col for col in colunas_merge_simples 
                                                               if col in df_historico_linhas.columns and col in df_fonte_forecast.columns]
                            
                            if colunas_merge_simples_existentes and len(colunas_merge_simples_existentes) < len(colunas_merge_existentes):
                                df_historico_fallback = df_historico_linhas.groupby(
                                    colunas_merge_simples_existentes,
                                    as_index=False
                                ).first()[colunas_merge_simples_existentes + colunas_ainda_faltantes]
                                
                                for col in colunas_ainda_faltantes:
                                    if col in df_historico_fallback.columns:
                                        serie_historico = df_historico_fallback.set_index(colunas_merge_simples_existentes)[col]
                                        mask_nulos = df_fonte_forecast[col].isna()
                                        if mask_nulos.any():
                                            indices_para_preencher = df_fonte_forecast.loc[mask_nulos, colunas_merge_simples_existentes].apply(
                                                lambda row: tuple(row), axis=1
                                            )
                                            valores_para_preencher = indices_para_preencher.map(serie_historico)
                                            df_fonte_forecast.loc[mask_nulos, col] = valores_para_preencher.values
                        adicionar_mensagem("info", f"✅ Colunas adicionadas do histórico via merge: {', '.join(colunas_faltantes)}")
                        for col in colunas_faltantes:
                            if col in df_fonte_forecast.columns:
                                valores_preenchidos = df_fonte_forecast[col].notna().sum()
                                total_linhas = len(df_fonte_forecast)
                                adicionar_mensagem("info", f"📊 {col}: {valores_preenchidos:,} de {total_linhas:,} linhas preenchidas ({valores_preenchidos/total_linhas*100:.1f}%)")
            
            # 🔧 DEBUG: Verificar se há períodos para criar forecast (MESMA LÓGICA DO FORECAST COPY linha 6656-6664)
            adicionar_mensagem("info", f"📊 Períodos restantes para criar forecast: {periodos_restantes}")
            if df_fonte_forecast is not None and not df_fonte_forecast.empty:
                adicionar_mensagem("info", f"📊 Total de linhas em df_fonte_forecast: {len(df_fonte_forecast)}")
                adicionar_mensagem("info", f"📊 Colunas disponíveis em df_fonte_forecast: {list(df_fonte_forecast.columns)[:15]}...")
                colunas_periodos = [col for col in df_fonte_forecast.columns if col in periodos_restantes]
                adicionar_mensagem("info", f"📊 Colunas de períodos encontradas: {colunas_periodos}")
                if not colunas_periodos:
                    adicionar_mensagem("warning", f"⚠️ Nenhuma coluna de período encontrada! Períodos esperados: {periodos_restantes}")
            
            linhas_forecast_dicts = []
            linhas_forecast_criadas = 0
            
            adicionar_mensagem("info", f"📊 Iniciando criação de linhas de forecast para {len(periodos_restantes)} períodos")
            if df_fonte_forecast is None or df_fonte_forecast.empty:
                adicionar_mensagem("error", f"❌ df_fonte_forecast está None ou vazio! Não é possível criar forecast.")
            else:
                adicionar_mensagem("info", f"📊 df_fonte_forecast tem {len(df_fonte_forecast):,} linhas e {len(df_fonte_forecast.columns)} colunas")
            
            for periodo in periodos_restantes:
                adicionar_mensagem("info", f"📊 Processando período: {periodo}")
                if df_fonte_forecast is not None and periodo in df_fonte_forecast.columns:
                    adicionar_mensagem("info", f"✅ Período '{periodo}' encontrado em df_fonte_forecast")
                    # Para cada linha única, criar uma nova linha com Período = periodo (MESMA LÓGICA DO FORECAST COPY linha 6682-6739)
                    colunas_chave_linha = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
                    if 'Ano' in df_fonte_forecast.columns:
                        colunas_chave_linha.insert(2, 'Ano')
                    colunas_chave_linha = [col for col in colunas_chave_linha if col in df_fonte_forecast.columns]
                    
                    # 🔧 CORREÇÃO: Incluir colunas importantes que devem ser preservadas (MESMA LÓGICA DO FORECAST COPY linha 6689-6690)
                    colunas_importantes = ['Custo', 'Centocst', 'Fornec.', 'Fornecedor', 'USI']
                    colunas_para_preservar = [col for col in colunas_importantes if col in df_fonte_forecast.columns]
                    
                    # Obter linhas únicas com valores de forecast (incluindo colunas importantes) (MESMA LÓGICA DO FORECAST COPY linha 6693-6698)
                    colunas_para_linha = colunas_chave_linha + colunas_para_preservar + [periodo]
                    colunas_para_linha = [col for col in colunas_para_linha if col in df_fonte_forecast.columns]
                    
                    df_linhas_unicas = df_fonte_forecast[colunas_para_linha].drop_duplicates(
                        subset=colunas_chave_linha
                    )
                    
                    # OTIMIZAÇÃO: Usar to_dict('records') em vez de iterrows() (mais rápido) (MESMA LÓGICA DO FORECAST COPY linha 6700-6701)
                    for linha_original in df_linhas_unicas.to_dict('records'):
                        nova_linha = linha_original.copy()
                        
                        # 🔧 CORREÇÃO: Definir Período apenas com o nome do mês (sem ano) (MESMA LÓGICA DO FORECAST COPY linha 6704-6721)
                        # Seguir o mesmo padrão do arquivo histórico
                        periodo_str = str(periodo).strip()
                        if ' ' in periodo_str:
                            # Extrair apenas o nome do mês
                            mes_nome = periodo_str.split(' ', 1)[0].strip().capitalize()
                            nova_linha['Período'] = mes_nome
                            
                            # Extrair ano e colocar na coluna Ano separadamente
                            partes = periodo_str.split(' ', 1)
                            if len(partes) == 2 and partes[1].isdigit():
                                nova_linha['Ano'] = int(partes[1])
                        else:
                            # Se não tem ano, usar apenas o mês
                            nova_linha['Período'] = periodo_str.strip().capitalize()
                            # Se não tem ano no período mas há coluna Ano na linha original, manter
                            if 'Ano' not in nova_linha and 'Ano' in linha_original:
                                nova_linha['Ano'] = linha_original.get('Ano', None)
                        
                        # 🔧 CORREÇÃO: Sempre usar coluna 'Total' para o valor de forecast (MESMA LÓGICA DO FORECAST COPY linha 6723-6725)
                        valor_forecast = float(nova_linha.get(periodo, 0.0))
                        nova_linha['Total'] = valor_forecast
                        
                        # 🔧 CORREÇÃO: Garantir que colunas importantes sejam preenchidas (MESMA LÓGICA DO FORECAST COPY linha 6730-6738)
                        # OTIMIZAÇÃO: As colunas já foram adicionadas via merge anterior
                        # Não é necessário buscar novamente no histórico para cada linha individual
                        for col_imp in colunas_importantes:
                            if col_imp not in nova_linha:
                                nova_linha[col_imp] = None
                        
                        if periodo in nova_linha:
                            del nova_linha[periodo]
                        
                        for p in periodos_restantes:
                            if p in nova_linha and p != periodo:
                                del nova_linha[p]
                        
                        if 'Média_Mensal_Histórica' in nova_linha:
                            del nova_linha['Média_Mensal_Histórica']
                        if 'Volume_Medio_Historico' in nova_linha:
                            del nova_linha['Volume_Medio_Historico']
                        
                        nova_linha['Tipo'] = 'BE'
                        linhas_forecast_dicts.append(nova_linha)
                        linhas_forecast_criadas += 1
                else:
                    adicionar_mensagem("warning", f"⚠️ Período '{periodo}' não encontrado nas colunas de df_fonte_forecast")
            
            adicionar_mensagem("info", f"📊 Total de dicionários de forecast coletados: {len(linhas_forecast_dicts)}")
            if linhas_forecast_dicts:
                df_forecast_final_temp = pd.DataFrame(linhas_forecast_dicts)
                adicionar_mensagem("info", f"📊 DataFrame de forecast criado com {len(df_forecast_final_temp):,} linhas")
                adicionar_mensagem("info", f"📊 Colunas no DataFrame de forecast: {list(df_forecast_final_temp.columns)[:10]}...")
                if 'Tipo' in df_forecast_final_temp.columns:
                    tipos_unicos = df_forecast_final_temp['Tipo'].unique()
                    adicionar_mensagem("info", f"📊 Valores únicos na coluna 'Tipo': {tipos_unicos}")
                linhas_finais.append(df_forecast_final_temp)
            else:
                adicionar_mensagem("warning", f"⚠️ Nenhuma linha de forecast foi criada! Verifique se há períodos restantes e se df_fonte_forecast contém dados.")
                adicionar_mensagem("info", f"📊 Períodos restantes: {periodos_restantes}")
                if df_fonte_forecast is not None and not df_fonte_forecast.empty:
                    adicionar_mensagem("info", f"📊 df_fonte_forecast tem {len(df_fonte_forecast):,} linhas")
                    adicionar_mensagem("info", f"📊 Colunas em df_fonte_forecast: {list(df_fonte_forecast.columns)[:15]}...")
                else:
                    adicionar_mensagem("warning", f"⚠️ df_fonte_forecast está vazio ou None!")
            
            adicionar_mensagem("info", f"📊 Linhas de forecast criadas: {linhas_forecast_criadas}")
            adicionar_mensagem("info", f"📊 Total de DataFrames em linhas_finais: {len(linhas_finais)}")
            
            # 💰 ADICIONAR CUSTOS ESPECÍFICOS COMO LINHAS SEPARADAS NO FORECAST
            df_custos_especificos_para_forecast = carregar_custos_especificos()
            if not df_custos_especificos_para_forecast.empty:
                adicionar_mensagem("info", f"💰 Carregando {len(df_custos_especificos_para_forecast):,} linha(s) de custos específicos para incluir no forecast")
                
                # Filtrar apenas custos que se aplicam aos períodos de forecast
                linhas_custos_especificos = []
                
                for idx, custo_row in df_custos_especificos_para_forecast.iterrows():
                    # Verificar se este custo se aplica a algum período de forecast
                    tipo_aplicacao = custo_row.get('Tipo_Aplicacao', None)
                    periodo_custo = custo_row.get('Período', None)
                    
                    if pd.isna(periodo_custo):
                        continue
                    
                    # Normalizar período do custo
                    periodo_custo_str = str(periodo_custo).strip()
                    if ' ' in periodo_custo_str:
                        mes_custo = periodo_custo_str.split(' ', 1)[0].strip().capitalize()
                    else:
                        mes_custo = periodo_custo_str.capitalize()
                    
                    # Verificar se o período do custo está nos períodos de forecast
                    periodos_aplicaveis = []
                    for periodo_forecast in periodos_restantes:
                        periodo_forecast_str = str(periodo_forecast).strip()
                        if ' ' in periodo_forecast_str:
                            mes_forecast = periodo_forecast_str.split(' ', 1)[0].strip().capitalize()
                        else:
                            mes_forecast = periodo_forecast_str.capitalize()
                        
                        if mes_custo == mes_forecast:
                            periodos_aplicaveis.append(periodo_forecast)
                    
                    # Se o custo se aplica a algum período de forecast, criar linha
                    if periodos_aplicaveis:
                        # Criar linha para cada período aplicável
                        for periodo_aplicavel in periodos_aplicaveis:
                            linha_custo = {}
                            
                            # Copiar todas as colunas do custo
                            for col in df_custos_especificos_para_forecast.columns:
                                if col not in ['Tipo_Aplicacao', 'Mes_Inicial', 'Meses_Especificos']:
                                    linha_custo[col] = custo_row.get(col, None)
                            
                            # Ajustar Período para o formato do forecast
                            periodo_str = str(periodo_aplicavel).strip()
                            if ' ' in periodo_str:
                                mes_nome = periodo_str.split(' ', 1)[0].strip().capitalize()
                                linha_custo['Período'] = mes_nome
                                
                                # Extrair ano
                                partes = periodo_str.split(' ', 1)
                                if len(partes) == 2 and partes[1].isdigit():
                                    linha_custo['Ano'] = int(partes[1])
                            else:
                                linha_custo['Período'] = periodo_str.strip().capitalize()
                            
                            # Garantir que Total e Valor estejam preenchidos
                            if 'Total' not in linha_custo or pd.isna(linha_custo.get('Total')):
                                if 'Valor' in linha_custo and pd.notna(linha_custo.get('Valor')):
                                    linha_custo['Total'] = linha_custo['Valor']
                                else:
                                    linha_custo['Total'] = 0.0
                            
                            if 'Valor' not in linha_custo or pd.isna(linha_custo.get('Valor')):
                                if 'Total' in linha_custo and pd.notna(linha_custo.get('Total')):
                                    linha_custo['Valor'] = linha_custo['Total']
                                else:
                                    linha_custo['Valor'] = 0.0
                            
                            # Marcar como BE Manual (custo específico/manual)
                            linha_custo['Tipo'] = 'BE Manual'
                            
                            # Adicionar descrição se não existir
                            if 'Descricao' not in linha_custo or pd.isna(linha_custo.get('Descricao')):
                                linha_custo['Descricao'] = 'Custo Específico'
                            
                            linhas_custos_especificos.append(linha_custo)
                
                if linhas_custos_especificos:
                    df_custos_especificos_forecast = pd.DataFrame(linhas_custos_especificos)
                    adicionar_mensagem("success", f"✅ {len(df_custos_especificos_forecast):,} linha(s) de custos específicos adicionada(s) ao forecast")
                    linhas_finais.append(df_custos_especificos_forecast)
                else:
                    adicionar_mensagem("info", f"ℹ️ Nenhum custo específico se aplica aos períodos de forecast selecionados")
            
            # 3. Separar histórico e forecast (MESMA LÓGICA DO FORECAST COPY linha 6788-6847)
            df_historico_final = None
            df_forecast_final = None
            df_consolidado_final = None
            
            if linhas_finais:
                df_todos = pd.concat(linhas_finais, ignore_index=True)
                
                todas_colunas = sorted(set([col for df in linhas_finais for col in df.columns]))
                df_todos = df_todos.reindex(columns=todas_colunas)
                
                # 🔧 DEBUG: Verificar separação (MESMA LÓGICA DO FORECAST COPY)
                adicionar_mensagem("info", f"🔍 DEBUG: Total de linhas em df_todos: {len(df_todos):,}")
                if 'Tipo' in df_todos.columns:
                    tipos_unicos = df_todos['Tipo'].unique()
                    adicionar_mensagem("info", f"🔍 DEBUG: Valores únicos na coluna 'Tipo': {tipos_unicos}")
                    for tipo in tipos_unicos:
                        count = len(df_todos[df_todos['Tipo'] == tipo])
                        adicionar_mensagem("info", f"🔍 DEBUG: Linhas com Tipo='{tipo}': {count:,}")
                    
                    df_historico_final = df_todos[df_todos['Tipo'] == 'Histórico'].copy()
                    # Incluir tanto 'BE' quanto 'BE Manual' no forecast final (e 'Forecast' para compatibilidade com arquivos antigos)
                    df_forecast_final = df_todos[df_todos['Tipo'].isin(['BE', 'BE Manual', 'Forecast'])].copy()
                    df_consolidado_final = df_todos.copy()
                    
                    # Converter valores antigos 'Forecast' para 'BE' para padronização
                    if 'Forecast' in df_forecast_final['Tipo'].values:
                        df_forecast_final.loc[df_forecast_final['Tipo'] == 'Forecast', 'Tipo'] = 'BE'
                    if 'Forecast' in df_consolidado_final['Tipo'].values:
                        df_consolidado_final.loc[df_consolidado_final['Tipo'] == 'Forecast', 'Tipo'] = 'BE'
                    
                    adicionar_mensagem("info", f"🔍 DEBUG: df_historico_final: {len(df_historico_final):,} linhas")
                    adicionar_mensagem("info", f"🔍 DEBUG: df_forecast_final: {len(df_forecast_final):,} linhas")
                else:
                    # Se não tem coluna Tipo, assumir que tudo é histórico
                    adicionar_mensagem("warning", f"⚠️ Coluna 'Tipo' não encontrada em df_todos! Assumindo que tudo é histórico.")
                    adicionar_mensagem("info", f"🔍 DEBUG: Colunas disponíveis: {list(df_todos.columns)[:15]}...")
                    df_historico_final = df_todos.copy()
                    df_forecast_final = pd.DataFrame()
                    df_consolidado_final = df_todos.copy()
                
                df_forecast_completo = df_consolidado_final
                
                adicionar_mensagem("info", f"✅ Tabela criada com {len(df_consolidado_final):,} linhas (histórico + forecast)")
                adicionar_mensagem("info", f"📊 Histórico: {len(df_historico_final):,} linhas")
                adicionar_mensagem("info", f"📊 Forecast: {len(df_forecast_final):,} linhas")
                adicionar_mensagem("info", f"📊 Períodos de forecast incluídos: {', '.join(periodos_restantes)}")
            else:
                adicionar_mensagem("warning", "⚠️ Nenhuma linha foi criada!")
                # Se não criou linhas, usar df_base_filtrado como base
                df_historico_final = df_base_filtrado.copy()
                df_historico_final['Tipo'] = 'Histórico'
                df_forecast_final = pd.DataFrame()
                df_forecast_completo = df_historico_final.copy()
                adicionar_mensagem("info", "ℹ️ Usando apenas dados históricos (sem forecast)")
            
            # Limpar DataFrames
            def limpar_dataframe(df):
                if df is None or df.empty:
                    return df
                
                df_limpo = df.copy()
                
                colunas_para_remover = ['Nºconta', 'Nºdoc.ref.', 'Dt.lçto.', 'QTD', 'Nºdoc.ref', 'Doc.compra', 'Texto breve', 'Material', 'Usuário']
                colunas_normalizacao = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
                colunas_para_remover.extend(colunas_normalizacao)
                colunas_para_remover = [col for col in colunas_para_remover if col != 'Custo']
                colunas_para_remover_existentes = [col for col in colunas_para_remover if col in df_limpo.columns]
                if colunas_para_remover_existentes:
                    df_limpo = df_limpo.drop(columns=colunas_para_remover_existentes)
                
                if 'Total' in df_limpo.columns:
                    mask_valor_valido = df_limpo['Total'].notna() & (df_limpo['Total'] != 0)
                    df_limpo = df_limpo[mask_valor_valido].copy()
                
                colunas_criticas = ['Oficina', 'Veículo', 'Período']
                colunas_criticas_existentes = [col for col in colunas_criticas if col in df_limpo.columns]
                if colunas_criticas_existentes:
                    mask_linhas_validas = df_limpo[colunas_criticas_existentes].notna().any(axis=1)
                    df_limpo = df_limpo[mask_linhas_validas].copy()
                
                colunas_todas_nulas = df_limpo.columns[df_limpo.isna().all()].tolist()
                if colunas_todas_nulas:
                    df_limpo = df_limpo.drop(columns=colunas_todas_nulas)
                
                return df_limpo
            
            # Aplicar limpeza nos DataFrames separados (MESMA LÓGICA DO FORECAST COPY)
            if df_historico_final is not None and not df_historico_final.empty:
                linhas_antes_hist = len(df_historico_final)
                df_historico_final = limpar_dataframe(df_historico_final)
                linhas_depois_hist = len(df_historico_final)
                if linhas_antes_hist != linhas_depois_hist:
                    adicionar_mensagem("info", f"🧹 Histórico: {linhas_antes_hist:,} → {linhas_depois_hist:,} linhas após limpeza")
            
            if df_forecast_final is not None and not df_forecast_final.empty:
                linhas_antes_for = len(df_forecast_final)
                adicionar_mensagem("info", f"🔍 DEBUG: df_forecast_final antes da limpeza: {linhas_antes_for:,} linhas")
                if 'Total' in df_forecast_final.columns:
                    valores_nao_zero = (df_forecast_final['Total'].notna() & (df_forecast_final['Total'] != 0)).sum()
                    adicionar_mensagem("info", f"🔍 DEBUG: Linhas com Total não-zero: {valores_nao_zero:,} de {linhas_antes_for:,}")
                
                df_forecast_final = limpar_dataframe(df_forecast_final)
                linhas_depois_for = len(df_forecast_final)
                if linhas_antes_for != linhas_depois_for:
                    adicionar_mensagem("warning", f"⚠️ Forecast: {linhas_antes_for:,} → {linhas_depois_for:,} linhas após limpeza ({linhas_antes_for - linhas_depois_for:,} removidas)")
                else:
                    adicionar_mensagem("info", f"✅ Forecast: {linhas_depois_for:,} linhas (nenhuma removida na limpeza)")
            else:
                if df_forecast_final is None:
                    adicionar_mensagem("warning", f"⚠️ df_forecast_final é None!")
                elif df_forecast_final.empty:
                    adicionar_mensagem("warning", f"⚠️ df_forecast_final está vazio!")
            
            linhas_antes = len(df_forecast_completo)
            df_forecast_completo = limpar_dataframe(df_forecast_completo)
            linhas_depois = len(df_forecast_completo)
            
            # 🔧 CORREÇÃO: Padronizar colunas para garantir mesma ordem e nomes consistentes (MESMA LÓGICA DO FORECAST COPY)
            def padronizar_colunas(df, nome_tipo="DataFrame"):
                """Padroniza colunas do DataFrame para garantir ordem e nomes consistentes"""
                if df is None or df.empty:
                    return df
                
                df_padronizado = df.copy()
                
                # Definir ordem padrão das colunas (colunas principais primeiro)
                # NOTA: Usamos apenas 'Custo' (padrão do projeto), 'Tipo_Custo' é redundante
                ordem_colunas_principal = [
                    'Oficina', 'Veículo', 'Ano', 'Período', 'Custo',
                    'Total', 'Valor',  # Total sempre antes de Valor
                    'Centocst', 'Fornec.', 'Fornecedor', 'USI',
                    'Type 05', 'Type 06', 'Account',
                    'Volume', 'CPU', 'Tipo'  # Tipo no final para identificação
                ]
                
                # Coletar todas as colunas do DataFrame
                colunas_existentes = list(df_padronizado.columns)
                
                # Separar colunas em: principais (na ordem), outras (alfabética), Tipo (sempre no final)
                colunas_principais_ordenadas = []
                colunas_outras = []
                coluna_tipo = None
                
                for col in ordem_colunas_principal:
                    if col in colunas_existentes:
                        colunas_principais_ordenadas.append(col)
                
                for col in colunas_existentes:
                    if col not in ordem_colunas_principal:
                        if col == 'Tipo':
                            coluna_tipo = col
                        else:
                            colunas_outras.append(col)
                
                # Ordenar colunas outras alfabeticamente
                colunas_outras = sorted(colunas_outras)
                
                # Montar ordem final: principais + outras + Tipo (se existir)
                ordem_final = colunas_principais_ordenadas + colunas_outras
                if coluna_tipo:
                    ordem_final.append(coluna_tipo)
                
                # Reordenar DataFrame
                df_padronizado = df_padronizado.reindex(columns=ordem_final)
                
                return df_padronizado
            
            # Padronizar colunas de histórico e forecast ANTES de combinar (MESMA LÓGICA DO FORECAST COPY)
            if df_historico_final is not None and not df_historico_final.empty:
                df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
            
            if df_forecast_final is not None and not df_forecast_final.empty:
                df_forecast_final = padronizar_colunas(df_forecast_final, "BE")
            
            # Atualizar consolidado após limpeza e padronização (recombinar histórico e forecast limpos) (MESMA LÓGICA DO FORECAST COPY)
            if df_historico_final is not None and df_forecast_final is not None:
                if not df_historico_final.empty and not df_forecast_final.empty:
                    # Garantir que ambos tenham exatamente as mesmas colunas na mesma ordem
                    todas_colunas_limpas = sorted(set(list(df_historico_final.columns) + list(df_forecast_final.columns)))
                    
                    # Reindexar ambos para ter as mesmas colunas na mesma ordem
                    df_historico_final = df_historico_final.reindex(columns=todas_colunas_limpas)
                    df_forecast_final = df_forecast_final.reindex(columns=todas_colunas_limpas)
                    
                    # Padronizar novamente após reindex para garantir ordem correta
                    df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
                    df_forecast_final = padronizar_colunas(df_forecast_final, "BE")
                    
                    # Garantir que ambos tenham exatamente as mesmas colunas na mesma ordem
                    colunas_finais = list(df_historico_final.columns)
                    df_forecast_final = df_forecast_final.reindex(columns=colunas_finais)
                    
                    # Combinar
                    df_forecast_completo = pd.concat([df_historico_final, df_forecast_final], ignore_index=True)
                    
                    # Padronizar consolidado também
                    df_forecast_completo = padronizar_colunas(df_forecast_completo, "Consolidado")
                    
                    # Debug: verificar se as colunas estão alinhadas
                    adicionar_mensagem("info", f"✅ Colunas padronizadas: {len(colunas_finais)} colunas na mesma ordem")
                    adicionar_mensagem("info", f"📊 Primeiras colunas: {', '.join(colunas_finais[:10])}...")
                elif not df_historico_final.empty:
                    df_forecast_completo = padronizar_colunas(df_historico_final.copy(), "Consolidado")
                elif not df_forecast_final.empty:
                    df_forecast_completo = padronizar_colunas(df_forecast_final.copy(), "Consolidado")
            
            # Verificar se histórico e forecast têm as mesmas colunas na mesma ordem (MESMA LÓGICA DO FORECAST COPY)
            if (df_historico_final is not None and not df_historico_final.empty and 
                df_forecast_final is not None and not df_forecast_final.empty):
                colunas_hist = list(df_historico_final.columns)
                colunas_for = list(df_forecast_final.columns)
                if colunas_hist == colunas_for:
                    adicionar_mensagem("success", f"✅ CONFIRMADO: Histórico e Forecast têm {len(colunas_hist)} colunas na mesma ordem")
                else:
                    adicionar_mensagem("warning", f"⚠️ Colunas diferentes! Histórico: {len(colunas_hist)}, Forecast: {len(colunas_for)}")
                    # Forçar alinhamento
                    todas_colunas = sorted(set(colunas_hist + colunas_for))
                    df_historico_final = df_historico_final.reindex(columns=todas_colunas)
                    df_forecast_final = df_forecast_final.reindex(columns=todas_colunas)
                    # Padronizar novamente
                    df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
                    df_forecast_final = padronizar_colunas(df_forecast_final, "BE")
                    adicionar_mensagem("info", f"✅ Colunas alinhadas e padronizadas: {len(todas_colunas)} colunas")
            
            # ============================================================
            # PASSO 1: Copiar arquivo completo de volume histórico (MESMA LÓGICA DO FORECAST COPY)
            # ============================================================
            try:
                # Carregar arquivo completo de volume histórico (antes dos filtros)
                caminho_vol_historico_original = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
                
                if os.path.exists(caminho_vol_historico_original):
                    # Carregar arquivo completo de volume histórico
                    df_vol_historico_completo = pd.read_parquet(caminho_vol_historico_original)
                    
                    # Salvar Parquet na pasta Forecast
                    caminho_vol_historico_destino = os.path.join(pasta_forecast, "df_vol_historico.parquet")
                    df_vol_historico_completo.to_parquet(caminho_vol_historico_destino, index=False, engine='pyarrow')
                    
                    # Salvar Excel na pasta Forecast (mesmo padrão dos outros arquivos)
                    caminho_vol_historico_excel = os.path.join(pasta_forecast, "df_vol_historico.xlsx")
                    df_vol_historico_completo.to_excel(caminho_vol_historico_excel, index=False, engine='openpyxl')
                    
                    adicionar_mensagem("success", f"✅ Arquivo de volume histórico salvo: {os.path.abspath(caminho_vol_historico_destino)}")
                    adicionar_mensagem("info", f"   📊 Total de linhas: {len(df_vol_historico_completo):,}")
                    adicionar_mensagem("info", f"   ✅ Parquet e Excel criados/atualizados")
                else:
                    adicionar_mensagem("warning", f"⚠️ Arquivo de volume histórico não encontrado: {caminho_vol_historico_original}")
            except Exception as e_volume:
                adicionar_mensagem("warning", f"⚠️ Erro ao copiar arquivo de volume histórico: {str(e_volume)}")
                import traceback
                adicionar_mensagem("error", f"Detalhes: {traceback.format_exc()}")
            
            # Função auxiliar para salvar arquivo
            def salvar_arquivo(df, nome_base, descricao):
                if df is None or df.empty:
                    return {
                        'sucesso': False,
                        'mensagem': f"⚠️ {descricao} está vazio. Arquivo não será criado.",
                        'parquet': None,
                        'excel': None
                    }
                
                sucesso_parquet = False
                sucesso_excel = False
                info_parquet = None
                info_excel = None
                
                caminho_parquet = os.path.join(pasta_forecast, f"{nome_base}.parquet")
                try:
                    df.to_parquet(caminho_parquet, index=False, engine='pyarrow')
                    if os.path.exists(caminho_parquet):
                        tamanho = os.path.getsize(caminho_parquet) / (1024 * 1024)
                        info_parquet = f"✅ {descricao} Parquet: {tamanho:.2f} MB, {len(df):,} linhas"
                        sucesso_parquet = True
                except Exception as e:
                    info_parquet = f"❌ Erro ao salvar {descricao} Parquet: {str(e)}"
                
                caminho_excel = os.path.join(pasta_forecast, f"{nome_base}.xlsx")
                try:
                    if os.path.exists(caminho_excel):
                        try:
                            os.remove(caminho_excel)
                        except:
                            pass
                    
                    with pd.ExcelWriter(caminho_excel, engine='openpyxl', mode='w') as writer:
                        df.to_excel(writer, index=False, sheet_name='Dados')
                    
                    if os.path.exists(caminho_excel):
                        tamanho = os.path.getsize(caminho_excel) / (1024 * 1024)
                        info_excel = f"✅ {descricao} Excel: {tamanho:.2f} MB, {len(df):,} linhas"
                        sucesso_excel = True
                except Exception as e:
                    info_excel = f"⚠️ Erro ao salvar {descricao} Excel: {str(e)}"
                
                return {
                    'sucesso': sucesso_parquet or sucesso_excel,
                    'parquet': info_parquet,
                    'excel': info_excel,
                    'linhas': len(df)
                }
            
            # ============================================================
            # PASSO 2: Salvar arquivos separados (histórico, forecast, consolidado) (MESMA LÓGICA DO FORECAST COPY)
            # ============================================================
            
            # Criar pasta Forecast em dados/Forecast (ANTES de tentar salvar) (MESMA LÓGICA DO FORECAST COPY)
            pasta_dados = "dados"
            pasta_forecast = os.path.join(pasta_dados, "Forecast")
            
            adicionar_mensagem("info", f"📁 Preparando para salvar em: {os.path.abspath(pasta_forecast)}")
            try:
                # Criar pasta dados se não existir
                if not os.path.exists(pasta_dados):
                    os.makedirs(pasta_dados, exist_ok=True)
                    adicionar_mensagem("info", f"📁 Pasta 'dados' criada: {os.path.abspath(pasta_dados)}")
                
                # Criar pasta Forecast dentro de dados
                if not os.path.exists(pasta_forecast):
                    os.makedirs(pasta_forecast, exist_ok=True)
                    adicionar_mensagem("success", f"✅ Pasta Forecast criada: {os.path.abspath(pasta_forecast)}")
                else:
                    adicionar_mensagem("info", f"📁 Pasta Forecast já existe: {os.path.abspath(pasta_forecast)}")
            except Exception as e_pasta:
                adicionar_mensagem("error", f"❌ Erro ao criar pasta Forecast: {str(e_pasta)}")
                import traceback
                adicionar_mensagem("error", f"Detalhes: {traceback.format_exc()}")
                # Fallback: tentar criar na raiz
                pasta_forecast = "Forecast"
                try:
                    os.makedirs(pasta_forecast, exist_ok=True)
                    adicionar_mensagem("warning", f"⚠️ Usando pasta Forecast na raiz: {os.path.abspath(pasta_forecast)}")
                except:
                    pasta_forecast = "."  # Último fallback: diretório atual
                    adicionar_mensagem("error", f"❌ Usando diretório atual como fallback: {os.path.abspath(pasta_forecast)}")
            
            # Usar nome fixo para substituir arquivo existente (não usar timestamp) (MESMA LÓGICA DO FORECAST COPY)
            nome_arquivo_base = "forecast_completo"
            
            # 🔧 DEBUG: Verificar estado dos DataFrames antes de salvar
            adicionar_mensagem("info", f"🔍 DEBUG: Antes de salvar arquivos:")
            adicionar_mensagem("info", f"   - df_historico_final: {'None' if df_historico_final is None else ('vazio' if df_historico_final.empty else f'{len(df_historico_final):,} linhas')}")
            adicionar_mensagem("info", f"   - df_forecast_final: {'None' if df_forecast_final is None else ('vazio' if df_forecast_final.empty else f'{len(df_forecast_final):,} linhas')}")
            adicionar_mensagem("info", f"   - df_forecast_completo: {'None' if df_forecast_completo is None else ('vazio' if df_forecast_completo.empty else f'{len(df_forecast_completo):,} linhas')}")
            
            # Verificar se df_forecast_final está vazio e adicionar mensagem de aviso
            if df_forecast_final is None or df_forecast_final.empty:
                adicionar_mensagem("warning", f"⚠️ ATENÇÃO: df_forecast_final está {'None' if df_forecast_final is None else 'vazio'}! O arquivo forecast_previsao não será criado.")
                if df_forecast_final is None:
                    adicionar_mensagem("info", f"   - df_forecast_final é None - verifique se as linhas de forecast foram criadas corretamente")
                elif df_forecast_final.empty:
                    adicionar_mensagem("info", f"   - df_forecast_final está vazio - verifique se a limpeza não removeu todas as linhas")
                    adicionar_mensagem("info", f"   - Verifique se há períodos restantes e se os valores de forecast foram calculados corretamente")
            
            # Salvar arquivos e coletar informações
            info_historico = salvar_arquivo(df_historico_final, "forecast_historico", "Histórico")
            info_forecast = salvar_arquivo(df_forecast_final, "forecast_previsao", "BE")
            info_consolidado = salvar_arquivo(df_forecast_completo, nome_arquivo_base, "Consolidado")
            
            # 🔧 DEBUG: Verificar resultado do salvamento
            adicionar_mensagem("info", f"🔍 DEBUG: Resultado do salvamento:")
            adicionar_mensagem("info", f"   - Histórico: {'✅' if info_historico['sucesso'] else '❌'} - {info_historico.get('mensagem', info_historico.get('parquet', 'N/A'))}")
            adicionar_mensagem("info", f"   - Forecast: {'✅' if info_forecast['sucesso'] else '❌'} - {info_forecast.get('mensagem', info_forecast.get('parquet', 'N/A'))}")
            adicionar_mensagem("info", f"   - Consolidado: {'✅' if info_consolidado['sucesso'] else '❌'} - {info_consolidado.get('mensagem', info_consolidado.get('parquet', 'N/A'))}")
            
            # ====================================================================
            # 🆕 OTIMIZAÇÃO: Usar arquivos já salvos para criar consolidado (muito mais rápido) (MESMA LÓGICA DO FORECAST COPY)
            # ====================================================================
            adicionar_mensagem("info", "📝 Gerando arquivo consolidado com histórico + forecast")
            
            try:
                # OTIMIZAÇÃO: Em vez de reprocessar tudo, apenas carregar os arquivos já salvos e juntar
                # Removido st.spinner para evitar mensagens separadas - tudo vai para o expander
                caminho_historico_salvo = os.path.join(pasta_forecast, "forecast_historico.parquet")
                caminho_forecast_salvo = os.path.join(pasta_forecast, "forecast_previsao.parquet")
                
                # Carregar arquivos salvos (muito mais rápido que reprocessar)
                if os.path.exists(caminho_historico_salvo) and os.path.exists(caminho_forecast_salvo):
                    adicionar_mensagem("info", "✅ Carregando arquivos já salvos (otimizado)")
                    df_historico_carregado = pd.read_parquet(caminho_historico_salvo)
                    df_forecast_carregado = pd.read_parquet(caminho_forecast_salvo)
                    
                    # Converter valores antigos 'Forecast' para 'BE' (compatibilidade com arquivos antigos)
                    if 'Tipo' in df_forecast_carregado.columns:
                        if 'Forecast' in df_forecast_carregado['Tipo'].values:
                            df_forecast_carregado.loc[df_forecast_carregado['Tipo'] == 'Forecast', 'Tipo'] = 'BE'
                    
                    adicionar_mensagem("info", f"📊 Histórico carregado: {len(df_historico_carregado):,} linhas")
                    adicionar_mensagem("info", f"📊 Forecast carregado: {len(df_forecast_carregado):,} linhas")
                    
                    # OTIMIZAÇÃO: Juntar diretamente sem reprocessar (muito mais rápido)
                    # Garantir que todas as colunas estejam presentes
                    todas_colunas_consolidado = sorted(set(list(df_historico_carregado.columns) + list(df_forecast_carregado.columns)))
                    df_historico_carregado = df_historico_carregado.reindex(columns=todas_colunas_consolidado)
                    df_forecast_carregado = df_forecast_carregado.reindex(columns=todas_colunas_consolidado)
                    
                    # Combinar (muito rápido - apenas concat)
                    df_consolidado_final = pd.concat([df_historico_carregado, df_forecast_carregado], ignore_index=True)
                    
                    # Converter valores antigos 'Forecast' no consolidado também
                    if 'Tipo' in df_consolidado_final.columns:
                        if 'Forecast' in df_consolidado_final['Tipo'].values:
                            df_consolidado_final.loc[df_consolidado_final['Tipo'] == 'Forecast', 'Tipo'] = 'BE'
                    
                    adicionar_mensagem("success", f"✅ Consolidado criado: {len(df_consolidado_final):,} linhas (Histórico: {len(df_historico_carregado):,} + Forecast: {len(df_forecast_carregado):,})")
                    
                    # Salvar arquivo consolidado
                    caminho_consolidado_forecast = os.path.join(pasta_forecast, "df_final_historico_forecast.parquet")
                    df_consolidado_final.to_parquet(caminho_consolidado_forecast, index=False, engine='pyarrow')
                    adicionar_mensagem("success", f"✅ Arquivo consolidado salvo na pasta Forecast: {os.path.basename(caminho_consolidado_forecast)}")
                    
                    # Gerar também em Excel
                    caminho_consolidado_excel = caminho_consolidado_forecast.replace('.parquet', '.xlsx')
                    try:
                        if os.path.exists(caminho_consolidado_excel):
                            try:
                                os.remove(caminho_consolidado_excel)
                            except:
                                pass
                        
                        with pd.ExcelWriter(caminho_consolidado_excel, engine='openpyxl', mode='w') as writer:
                            df_consolidado_final.to_excel(writer, index=False, sheet_name='Dados')
                        
                        if os.path.exists(caminho_consolidado_excel):
                            tamanho = os.path.getsize(caminho_consolidado_excel) / (1024 * 1024)
                            adicionar_mensagem("success", f"✅ Arquivo consolidado Excel salvo: {os.path.basename(caminho_consolidado_excel)} ({tamanho:.2f} MB)")
                    except Exception as e_excel:
                        adicionar_mensagem("warning", f"⚠️ Erro ao salvar Excel consolidado: {str(e_excel)}")
                
                else:
                    # Fallback: se os arquivos não existirem, usar o método antigo (mas otimizado)
                    adicionar_mensagem("warning", "⚠️ Arquivos não encontrados, usando método alternativo...")
                    # Usar df_forecast_completo que já foi criado anteriormente
                    if df_forecast_completo is not None and not df_forecast_completo.empty:
                        df_consolidado_final = df_forecast_completo.copy()
                        caminho_consolidado_forecast = os.path.join(pasta_forecast, "df_final_historico_forecast.parquet")
                        df_consolidado_final.to_parquet(caminho_consolidado_forecast, index=False, engine='pyarrow')
                        adicionar_mensagem("success", f"✅ Arquivo consolidado salvo: {os.path.basename(caminho_consolidado_forecast)}")
                    else:
                        adicionar_mensagem("error", "❌ Não foi possível criar arquivo consolidado: dados não disponíveis")
                        raise Exception("Dados não disponíveis para consolidação")
                
            except Exception as e_consolidado:
                adicionar_mensagem("error", f"❌ Erro ao criar arquivo consolidado: {str(e_consolidado)}")
                import traceback
                adicionar_mensagem("error", f"Detalhes: {traceback.format_exc()}")
            
            # Limpar flag (MESMA LÓGICA DO FORECAST COPY)
            st.session_state.gerar_tabela_completa_forecast = False
        
        except Exception as e:
            # Se mensagens_debug existe, adicionar erro; senão, usar st.error (fallback) (MESMA LÓGICA DO FORECAST COPY)
            if 'mensagens_debug' in st.session_state:
                adicionar_mensagem("error", f"❌ Erro ao gerar tabela completa: {str(e)}")
                import traceback
                adicionar_mensagem("error", f"Detalhes: {traceback.format_exc()}")
            else:
                st.error(f"❌ Erro ao gerar arquivos de forecast: {str(e)}")
                import traceback
                st.error(f"Detalhes: {traceback.format_exc()}")
            st.session_state.gerar_tabela_completa_forecast = False
    
    # 🔧 CORREÇÃO: Limpar cache NOVAMENTE após gerar arquivos
    # Isso garante que os dados sejam recarregados quando o usuário acessar a página Forecast
    try:
        # Limpar cache de calcular_medias_forecast se existir
        calcular_medias_forecast.clear()
    except:
        pass
        try:
            load_data.clear()  # 🔧 CORREÇÃO: Limpar cache de load_data para forçar recarregar
        except:
            pass
        try:
            otimizar_tipos_dados.clear()  # 🔧 OTIMIZAÇÃO: Limpar cache de otimização
        except:
            pass
        try:
            load_volume_data.clear()
        except:
            pass
        try:
            load_volume_historico_data.clear()
        except:
            pass
        # Limpar cache global para garantir que todas as páginas sejam atualizadas
        try:
            st.cache_data.clear()
        except:
            pass
    
    # 🆕 NOVA FUNCIONALIDADE: Marcar que precisa gerar tabela completa com forecast
    # (mesma lógica do Forecast copy linha 1839)
    st.session_state.gerar_tabela_completa_forecast = True
    
    # 🔧 CORREÇÃO: Exibir mensagens ANTES do rerun para garantir que sejam vistas
    # Exibir log de processamento se existir
    if 'mensagens_debug' in st.session_state and st.session_state.mensagens_debug:
        mensagens_debug = st.session_state.mensagens_debug
        # Contar mensagens por tipo para o título
        total_mensagens = len(mensagens_debug)
        tipos_contagem = {}
        for tipo, _ in mensagens_debug:
            tipos_contagem[tipo] = tipos_contagem.get(tipo, 0) + 1
        
        # Criar título resumido
        titulo_expander = f"📊 Log de Processamento ({total_mensagens} mensagens)"
        if tipos_contagem.get('error', 0) > 0:
            titulo_expander += f" ⚠️ {tipos_contagem['error']} erro(s)"
        elif tipos_contagem.get('warning', 0) > 0:
            titulo_expander += f" ⚠️ {tipos_contagem['warning']} aviso(s)"
        
        with st.expander(titulo_expander, expanded=False):
            for tipo, mensagem in mensagens_debug:
                # Usar texto menor e sem caixas coloridas
                if tipo == "error":
                    st.markdown(f"<small style='color: #ff4b4b;'>❌ {mensagem}</small>", unsafe_allow_html=True)
                elif tipo == "warning":
                    st.markdown(f"<small style='color: #ffa500;'>⚠️ {mensagem}</small>", unsafe_allow_html=True)
                elif tipo == "success":
                    st.markdown(f"<small style='color: #00cc00;'>✅ {mensagem}</small>", unsafe_allow_html=True)
                else:  # info
                    st.markdown(f"<small>{mensagem}</small>", unsafe_allow_html=True)
    
    # Exibir status de salvamento se as variáveis foram definidas
    if 'info_historico' in locals() and 'info_forecast' in locals() and 'info_consolidado' in locals() and 'pasta_forecast' in locals():
        st.success("✅ Arquivos de forecast gerados com sucesso!")
        with st.expander("📊 Status de Salvamento dos Arquivos", expanded=False):
            st.markdown(f"📁 Pasta: {os.path.abspath(pasta_forecast)}")
            st.markdown("---")
            
            if info_historico['sucesso']:
                st.markdown(f"{info_historico['parquet']}")
                if info_historico['excel']:
                    st.markdown(f"{info_historico['excel']}")
            else:
                st.markdown(f"{info_historico.get('mensagem', 'Erro desconhecido')}")
            
            st.markdown("---")
            
            if info_forecast['sucesso']:
                st.markdown(f"{info_forecast['parquet']}")
                if info_forecast['excel']:
                    st.markdown(f"{info_forecast['excel']}")
            else:
                st.markdown(f"{info_forecast.get('mensagem', 'Erro desconhecido')}")
            
            st.markdown("---")
            
            if info_consolidado['sucesso']:
                st.markdown(f"{info_consolidado['parquet']}")
                if info_consolidado['excel']:
                    st.markdown(f"{info_consolidado['excel']}")
            else:
                st.markdown(f"{info_consolidado.get('mensagem', 'Erro desconhecido')}")
            
            st.markdown("---")
            st.markdown(f"📊 Histórico: {info_historico.get('linhas', 0):,} linhas | Forecast: {info_forecast.get('linhas', 0):,} linhas | Consolidado: {info_consolidado.get('linhas', 0):,} linhas")
    
    # 🔧 CORREÇÃO: Armazenar mensagens no session_state para exibir após rerun
    st.session_state.forecast_mensagem_sucesso = "✅ **Configurações aplicadas com sucesso!**"
    st.session_state.forecast_mensagem_info = "📊 **Arquivos de Best Estimate gerados!** Acesse a página '3 - Best Estimate - Análise' para visualizar os gráficos e tabelas."
    
    # 🔧 CORREÇÃO: Fazer rerun para atualizar a página
    # As mensagens serão exibidas no início da próxima execução também
    st.rerun()

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

# Rodapé
st.markdown("---")
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)
