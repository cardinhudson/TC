import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

st.set_page_config(
    page_title="Análise Waterfall - TC", 
    page_icon="🌊", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# CSS para evitar quebra de linha nos títulos
st.markdown("""
    <style>
        h1 {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        /* Estilos para botões: reduzir fonte e aproximar */
        .stButton > button {
            font-size: 0.85rem !important;
            padding: 0.4rem 1rem !important;
            margin-bottom: 0.3rem !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("🌊 Análise Waterfall - TC")
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
        key="moeda_selecionada_radio_waterfall",
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
        key="taxa_usd_para_brl_input_waterfall",
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
        key="taxa_eur_para_brl_input_waterfall",
        label_visibility="collapsed"
    )

# Calcular taxas inversas
taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

# Salvar taxas quando alteradas
taxa_usd_atual_key = "taxa_usd_atual_salva_waterfall"
taxa_eur_atual_key = "taxa_eur_atual_salva_waterfall"

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
        key="tipo_visualizacao_top_waterfall"
    )

with col_fator:
    if tipo_visualizacao == "Custo Total":
        fator_conversao = st.radio(
            "🔢 **Fator:**",
            ["Nenhum", "K (milhares)", "M (Milhões)"],
            index=1,
            horizontal=True,
            help="Aplica divisão aos valores para simplificar visualização. Não afeta cálculos.",
            key="fator_conversao_top_waterfall"
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

PT_MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
MES_POS = {m: i + 1 for i, m in enumerate(PT_MESES)}

def sort_mes_unique(values):
    """Ordena valores de meses únicos"""
    vals = list(pd.Series(values).dropna().unique())
    try:
        return sorted(vals, key=lambda x: int(x))
    except Exception:
        return sorted(vals, key=lambda x: MES_POS.get(str(x).lower(), 99))

@st.cache_data(ttl=3600, max_entries=3)
def load_df_historico() -> pd.DataFrame:
    """Carrega dados do arquivo histórico consolidado"""
    caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
    
    if not os.path.exists(caminho_historico):
        st.error("❌ **Arquivo histórico não encontrado**")
        st.error(f"📁 Caminho esperado: {caminho_historico}")
        st.info("💡 **Solução**: Certifique-se de que o arquivo df_final_historico.parquet existe na pasta dados/historico_consolidado/")
        st.stop()
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(caminho_historico)
        
        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                if unique_ratio < 0.5:
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
        
        return df
    except Exception as e:
        st.error(f"❌ **Erro ao carregar dados**: {str(e)}")
        st.stop()
        return pd.DataFrame()

@st.cache_data(ttl=3600, max_entries=3)
def load_df_volume() -> pd.DataFrame:
    """Carrega dados de volume do arquivo histórico consolidado"""
    caminho_volume = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
    
    if not os.path.exists(caminho_volume):
        return pd.DataFrame()  # Retorna vazio se não encontrar
    
    try:
        df = pd.read_parquet(caminho_volume)
        return df
    except Exception:
        return pd.DataFrame()

def obter_semestre_trimestre(mes_str, ano):
    """
    Identifica o semestre e trimestre de um mês.
    Retorna: (semestre, trimestre) onde semestre=1 ou 2, trimestre=1,2,3 ou 4
    """
    meses_semestre = {
        'janeiro': (1, 1), 'fevereiro': (1, 1), 'março': (1, 1),
        'abril': (1, 2), 'maio': (1, 2), 'junho': (1, 2),
        'julho': (2, 3), 'agosto': (2, 3), 'setembro': (2, 3),
        'outubro': (2, 4), 'novembro': (2, 4), 'dezembro': (2, 4)
    }
    mes_lower = mes_str.lower().strip()
    # Remover ano se estiver presente (ex: "Janeiro 2024" -> "janeiro")
    if ' ' in mes_lower:
        mes_lower = mes_lower.split(' ', 1)[0]
    return meses_semestre.get(mes_lower, (1, 1))

def calcular_flex(df_dados, df_volume, mes_inicial, mes_final, col_mes, col_valor, 
                  sensibilidade_fixo=0.0, sensibilidade_variavel=1.0, inflacao=0.0,
                  modo_sensibilidade="Global", dict_sens_fixo=None, dict_sens_variavel=None,
                  modo_inflacao="Global", dict_inflacao=None, col_categoria=None,
                  modo_comparacao="Mês a Mês", ano_inicial=None, ano_final=None,
                  semestre_inicial=None, semestre_final=None, trimestre_inicial=None, trimestre_final=None):
    """
    Calcula o efeito FLEX baseado na variação de volume entre dois períodos,
    aplicando sensibilidade e inflação conforme a lógica do forecast.
    
    Parâmetros:
    - sensibilidade_fixo/variavel: valores globais (modo Global)
    - dict_sens_fixo/variavel: {categoria: sensibilidade} (modo Detalhado)
    - inflacao: valor global (modo Global)
    - dict_inflacao: {categoria: inflacao} (modo Detalhado)
    - col_categoria: coluna usada para categorização no modo Detalhado
    - modo_comparacao: "Mês a Mês", "Ano a Ano", "Semestre" ou "Quarter"
    - ano_inicial/ano_final: anos para comparação (quando modo_comparacao == "Ano a Ano", "Semestre" ou "Quarter")
    - semestre_inicial/semestre_final: semestres (1 ou 2) para comparação
    - trimestre_inicial/trimestre_final: quarters (1, 2, 3 ou 4) para comparação
    
    Retorna: (flex_volume, flex_inflacao, volume_inicial, volume_final)
    - flex_volume: Efeito de volume + sensibilidade
    - flex_inflacao: Efeito da inflação
    - volume_inicial: Volume do período inicial
    - volume_final: Volume do período final
    """
    if df_volume.empty:
        return 0.0, 0.0, 0.0, 0.0
    
    try:
        # Criar coluna Período_Ano no df_volume se necessário
        if 'Período_Ano' not in df_volume.columns and 'Período' in df_volume.columns and 'Ano' in df_volume.columns:
            df_volume = df_volume.copy()
            df_volume['Período_Ano'] = df_volume['Período'].astype(str) + ' ' + df_volume['Ano'].astype(str)
        
        # Obter dados do período inicial baseado no modo de comparação
        if modo_comparacao == "Ano a Ano" and ano_inicial and ano_final:
            # Para Ano a Ano: usar TODOS os dados do ano inicial
            df_mes_inicial = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
        elif modo_comparacao == "Semestre" and ano_inicial and ano_final and semestre_inicial and semestre_final:
            # Para Semestre: filtrar por ano e semestre
            df_temp = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
            # Identificar meses do semestre
            meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                            2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
            meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
            df_mes_inicial = df_temp[df_temp[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial])].copy()
        elif modo_comparacao == "Quarter" and ano_inicial and ano_final and trimestre_inicial and trimestre_final:
            # Para Trimestre: filtrar por ano e trimestre
            df_temp = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
            # Identificar meses do trimestre
            meses_trimestre = {
                1: ['janeiro', 'fevereiro', 'março'],
                2: ['abril', 'maio', 'junho'],
                3: ['julho', 'agosto', 'setembro'],
                4: ['outubro', 'novembro', 'dezembro']
            }
            meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
            df_mes_inicial = df_temp[df_temp[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial])].copy()
        else:
            # Para Mês a Mês: usar dados do mês específico
            df_mes_inicial = df_dados[df_dados[col_mes].astype(str) == str(mes_inicial)].copy()
        
        if df_mes_inicial.empty:
            return 0.0, 0.0, 0.0, 0.0
        
        # Obter volume baseado no modo de comparação
        col_mes_vol = 'Período_Ano' if 'Período_Ano' in df_volume.columns else 'Período'
        
        if modo_comparacao == "Ano a Ano" and ano_inicial and ano_final:
            # Para Ano a Ano: usar volume TOTAL de cada ano
            volume_inicial = df_volume[df_volume['Ano'].astype(str) == str(ano_inicial)]['Volume'].sum()
            volume_final = df_volume[df_volume['Ano'].astype(str) == str(ano_final)]['Volume'].sum()
        elif modo_comparacao == "Semestre" and ano_inicial and ano_final and semestre_inicial and semestre_final:
            # Para Semestre: usar volume TOTAL do semestre
            meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                            2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
            meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
            meses_sem_final = meses_semestre.get(semestre_final, [])
            df_vol_inicial = df_volume[(df_volume['Ano'].astype(str) == str(ano_inicial)) & 
                                      (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial]))]
            df_vol_final = df_volume[(df_volume['Ano'].astype(str) == str(ano_final)) & 
                                    (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_final]))]
            volume_inicial = df_vol_inicial['Volume'].sum()
            volume_final = df_vol_final['Volume'].sum()
        elif modo_comparacao == "Quarter" and ano_inicial and ano_final and trimestre_inicial and trimestre_final:
            # Para Trimestre: usar volume TOTAL do trimestre
            meses_trimestre = {
                1: ['janeiro', 'fevereiro', 'março'],
                2: ['abril', 'maio', 'junho'],
                3: ['julho', 'agosto', 'setembro'],
                4: ['outubro', 'novembro', 'dezembro']
            }
            meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
            meses_trim_final = meses_trimestre.get(trimestre_final, [])
            df_vol_inicial = df_volume[(df_volume['Ano'].astype(str) == str(ano_inicial)) & 
                                      (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial]))]
            df_vol_final = df_volume[(df_volume['Ano'].astype(str) == str(ano_final)) & 
                                    (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_final]))]
            volume_inicial = df_vol_inicial['Volume'].sum()
            volume_final = df_vol_final['Volume'].sum()
        else:
            # Para Mês a Mês: usar volume do mês específico
            # USAR A MESMA LÓGICA DO GRÁFICO "Volume total por período"
            # Agrupar por Ano e Período e somar Volume (igual ao gráfico)
            
            # Extrair mês e ano do mes_inicial e mes_final
            mes_inicial_str = str(mes_inicial).strip()
            mes_final_str = str(mes_final).strip()
            
            # Tentar extrair ano e mês
            ano_mes_inicial = None
            ano_mes_final = None
            mes_nome_inicial = None
            mes_nome_final = None
            
            if ' ' in mes_inicial_str:
                partes = mes_inicial_str.split(' ', 1)
                mes_nome_inicial = partes[0].strip()
                if len(partes) > 1 and partes[1].strip().isdigit():
                    ano_mes_inicial = int(partes[1].strip())
            else:
                mes_nome_inicial = mes_inicial_str
            
            if ' ' in mes_final_str:
                partes = mes_final_str.split(' ', 1)
                mes_nome_final = partes[0].strip()
                if len(partes) > 1 and partes[1].strip().isdigit():
                    ano_mes_final = int(partes[1].strip())
            else:
                mes_nome_final = mes_final_str
            
            # Se temos coluna Ano, agrupar por Ano e Período (MESMA LÓGICA DO GRÁFICO)
            if 'Ano' in df_volume.columns and 'Período' in df_volume.columns:
                # Normalizar Período antes de agrupar (mesma lógica do gráfico - linha 861-881 do app.py)
                df_volume_normalizado = df_volume.copy()
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
                    return periodo_str
                
                df_volume_normalizado['Período'] = df_volume_normalizado['Período'].apply(normalizar_periodo)
                
                # Agrupar por Ano e Período e somar Volume (igual ao gráfico)
                df_volume_agrupado = df_volume_normalizado.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                
                # Normalizar nome do mês buscado (capitalizar primeira letra)
                mes_nome_inicial_norm = mes_nome_inicial.capitalize() if mes_nome_inicial else None
                mes_nome_final_norm = mes_nome_final.capitalize() if mes_nome_final else None
                
                # Buscar volume inicial
                if ano_mes_inicial is not None and mes_nome_inicial_norm is not None:
                    volume_inicial = df_volume_agrupado[
                        (df_volume_agrupado['Ano'].astype(str) == str(ano_mes_inicial)) &
                        (df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_inicial_norm)
                    ]['Volume'].sum()
                else:
                    # Se não temos ano, buscar apenas por período
                    if mes_nome_inicial_norm is not None:
                        volume_inicial = df_volume_agrupado[
                            df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_inicial_norm
                        ]['Volume'].sum()
                    else:
                        volume_inicial = 0.0
                
                # Buscar volume final
                if ano_mes_final is not None and mes_nome_final_norm is not None:
                    volume_final = df_volume_agrupado[
                        (df_volume_agrupado['Ano'].astype(str) == str(ano_mes_final)) &
                        (df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_final_norm)
                    ]['Volume'].sum()
                else:
                    # Se não temos ano, buscar apenas por período
                    if mes_nome_final_norm is not None:
                        volume_final = df_volume_agrupado[
                            df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_final_norm
                        ]['Volume'].sum()
                    else:
                        volume_final = 0.0
            else:
                # Se não temos coluna Ano, agrupar apenas por Período (MESMA LÓGICA DO GRÁFICO)
                # Normalizar Período antes de agrupar (mesma lógica do gráfico)
                df_volume_normalizado = df_volume.copy()
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
                    return periodo_str
                
                df_volume_normalizado['Período'] = df_volume_normalizado['Período'].apply(normalizar_periodo)
                
                # Agrupar apenas por Período e somar Volume (igual ao gráfico)
                df_volume_agrupado = df_volume_normalizado.groupby('Período')['Volume'].sum().reset_index()
                
                # Normalizar nome do mês buscado (capitalizar primeira letra)
                mes_nome_inicial_norm = mes_nome_inicial.capitalize() if mes_nome_inicial else None
                mes_nome_final_norm = mes_nome_final.capitalize() if mes_nome_final else None
                
                # Buscar volume inicial
                if mes_nome_inicial_norm is not None:
                    volume_inicial = df_volume_agrupado[
                        df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_inicial_norm
                    ]['Volume'].sum()
                else:
                    volume_inicial = 0.0
                
                # Buscar volume final
                if mes_nome_final_norm is not None:
                    volume_final = df_volume_agrupado[
                        df_volume_agrupado['Período'].astype(str).str.strip() == mes_nome_final_norm
                    ]['Volume'].sum()
                else:
                    volume_final = 0.0
        
        # Calcular proporção e variação de volume (se houver volume)
        if volume_inicial == 0 or volume_final == 0:
            # Sem volume: FLEX Volume = 0, mas ainda calcular FLEX Inflação se houver inflação
            proporcao_volume = 1.0
            variacao_percentual = 0.0
            tem_volume = False
            # Debug: verificar por que não há volume
            # if modo_comparacao == "Mês a Mês":
            #     st.write(f"DEBUG Volume - mes_inicial: {mes_inicial}, mes_final: {mes_final}, volume_inicial: {volume_inicial}, volume_final: {volume_final}")
            #     st.write(f"DEBUG Volume - col_mes_vol: {col_mes_vol}, valores disponíveis: {df_volume[col_mes_vol].astype(str).unique()[:10]}")
        else:
            proporcao_volume = volume_final / volume_inicial
            variacao_percentual = proporcao_volume - 1.0
            tem_volume = True
        
        # ========== MODO GLOBAL ==========
        if modo_sensibilidade == "Global" and modo_inflacao == "Global":
            # Agrupar por tipo de custo
            if 'Custo' in df_mes_inicial.columns:
                custo_por_tipo = df_mes_inicial.groupby('Custo')[col_valor].sum()
                custo_fixo = float(custo_por_tipo.get('Fixo', 0.0))
                custo_variavel = float(custo_por_tipo.get('Variável', 0.0))
            else:
                custo_fixo = 0.0
                custo_variavel = float(df_mes_inicial[col_valor].sum())
            
            custo_total_inicial = custo_fixo + custo_variavel
            
            if tem_volume:
                # Aplicar sensibilidade (apenas se houver volume)
                variacao_ajustada_fixo = variacao_percentual * sensibilidade_fixo
                variacao_ajustada_variavel = variacao_percentual * sensibilidade_variavel
                fator_variacao_fixo = 1.0 + variacao_ajustada_fixo
                fator_variacao_variavel = 1.0 + variacao_ajustada_variavel
                
                # Calcular custo após volume + sensibilidade (SEM inflação)
                custo_apos_volume = custo_fixo * fator_variacao_fixo + custo_variavel * fator_variacao_variavel
                flex_volume = custo_apos_volume - custo_total_inicial
            else:
                # Sem volume: FLEX Volume = 0, custo após volume = custo inicial
                flex_volume = 0.0
                custo_apos_volume = custo_total_inicial
            
            # Aplicar inflação (sempre, mesmo sem volume)
            fator_inflacao = 1.0 + (inflacao / 100.0)
            custo_final_com_inflacao = custo_apos_volume * fator_inflacao
            flex_inflacao = custo_final_com_inflacao - custo_apos_volume
            
            return float(flex_volume), float(flex_inflacao), float(volume_inicial), float(volume_final)
        
        # ========== MODO DETALHADO ==========
        else:
            flex_volume_total = 0.0
            flex_inflacao_total = 0.0
            
            # Iterar por categoria
            if col_categoria and col_categoria in df_mes_inicial.columns:
                categorias = df_mes_inicial[col_categoria].unique()
                
                for categoria in categorias:
                    df_cat = df_mes_inicial[df_mes_inicial[col_categoria] == categoria]
                    
                    # Obter custos por tipo
                    if 'Custo' in df_cat.columns:
                        custo_por_tipo = df_cat.groupby('Custo')[col_valor].sum()
                        custo_fixo_cat = float(custo_por_tipo.get('Fixo', 0.0))
                        custo_variavel_cat = float(custo_por_tipo.get('Variável', 0.0))
                    else:
                        custo_fixo_cat = 0.0
                        custo_variavel_cat = float(df_cat[col_valor].sum())
                    
                    # Obter sensibilidade para esta categoria
                    if modo_sensibilidade == "Detalhado" and dict_sens_fixo is not None and dict_sens_variavel is not None:
                        sens_fixo_cat = dict_sens_fixo.get(str(categoria), sensibilidade_fixo)
                        sens_var_cat = dict_sens_variavel.get(str(categoria), sensibilidade_variavel)
                    else:
                        sens_fixo_cat = sensibilidade_fixo
                        sens_var_cat = sensibilidade_variavel
                    
                    # Calcular flex volume para esta categoria (apenas se houver volume)
                    if tem_volume:
                        variacao_ajustada_fixo = variacao_percentual * sens_fixo_cat
                        variacao_ajustada_variavel = variacao_percentual * sens_var_cat
                        fator_variacao_fixo = 1.0 + variacao_ajustada_fixo
                        fator_variacao_variavel = 1.0 + variacao_ajustada_variavel
                        
                        custo_apos_volume_cat = (custo_fixo_cat * fator_variacao_fixo + 
                                                custo_variavel_cat * fator_variacao_variavel)
                        custo_inicial_cat = custo_fixo_cat + custo_variavel_cat
                        flex_volume_cat = custo_apos_volume_cat - custo_inicial_cat
                    else:
                        # Sem volume: FLEX Volume = 0, custo após volume = custo inicial
                        custo_inicial_cat = custo_fixo_cat + custo_variavel_cat
                        custo_apos_volume_cat = custo_inicial_cat
                        flex_volume_cat = 0.0
                    
                    # Obter inflação para esta categoria
                    if modo_inflacao == "Detalhado" and dict_inflacao is not None:
                        inflacao_cat = dict_inflacao.get(str(categoria), inflacao)
                    else:
                        inflacao_cat = inflacao
                    
                    # Calcular flex inflação para esta categoria
                    fator_inflacao_cat = 1.0 + (inflacao_cat / 100.0)
                    custo_final_cat = custo_apos_volume_cat * fator_inflacao_cat
                    flex_inflacao_cat = custo_final_cat - custo_apos_volume_cat
                    
                    flex_volume_total += flex_volume_cat
                    flex_inflacao_total += flex_inflacao_cat
            
            return float(flex_volume_total), float(flex_inflacao_total), float(volume_inicial), float(volume_final)
    
    except Exception:
        return 0.0, 0.0, 0.0, 0.0

# Carregar dados
df_base = load_df_historico()
if df_base.empty:
    st.stop()

# Carregar dados de volume
df_volume = load_df_volume()

st.sidebar.success("✅ Dados carregados com sucesso")
st.sidebar.info(f"📊 {len(df_base):,} registros carregados")
if not df_volume.empty:
    st.sidebar.success(f"📈 {len(df_volume):,} registros de volume carregados")

# Aplicar filtros padrão do projeto
st.sidebar.title("Filtros")

# ============================================================================
# 🎚️ CONFIGURAÇÕES GLOBAIS (para cálculo do FLEX)
# ============================================================================
st.sidebar.markdown("---")
st.sidebar.subheader("🎚️ Configurações Globais")
st.sidebar.markdown("Aplicadas ao cálculo do FLEX")

# Inicializar session_state se necessário
if 'sensibilidade_fixo' not in st.session_state:
    st.session_state.sensibilidade_fixo = 0.0
if 'sensibilidade_variavel' not in st.session_state:
    st.session_state.sensibilidade_variavel = 1.0
if 'inflacao' not in st.session_state:
    st.session_state.inflacao = 0.0
if 'modo_sensibilidade' not in st.session_state:
    st.session_state.modo_sensibilidade = "Global"
if 'modo_inflacao' not in st.session_state:
    st.session_state.modo_inflacao = "Global"

# ========== MODO SENSIBILIDADE ==========
st.sidebar.markdown("### 🎯 Sensibilidade")
# Usar o valor do session_state diretamente, ou calcular o index
# Se a key do radio já existe, usar esse valor; senão, usar o valor do modo_sensibilidade
valor_atual_sens = st.session_state.get('radio_modo_sens', st.session_state.get('modo_sensibilidade', 'Global'))
index_sens = 0 if valor_atual_sens == "Global" else 1

modo_sensibilidade = st.sidebar.radio(
    "Modo de Sensibilidade:",
    options=["Global", "Detalhado"],
    index=index_sens,
    key="radio_modo_sens",
    help="Global: mesma sensibilidade para todos | Detalhado: configuração por categoria"
)
# Sincronizar session_state com o valor do radio button
# O Streamlit armazena o valor na key automaticamente, então usamos diretamente
st.session_state.modo_sensibilidade = modo_sensibilidade

if modo_sensibilidade == "Global":
    # Botões de sensibilidade
    col_sens1, col_sens2 = st.sidebar.columns(2)
    with col_sens1:
        if st.button("📌 Fixo: 0%", key="btn_sens_fixo_0"):
            st.session_state.sensibilidade_fixo = 0.0
        if st.button("📌 Fixo: 50%", key="btn_sens_fixo_50"):
            st.session_state.sensibilidade_fixo = 0.5
    with col_sens2:
        if st.button("📌 Var: 50%", key="btn_sens_var_50"):
            st.session_state.sensibilidade_variavel = 0.5
        if st.button("📌 Var: 100%", key="btn_sens_var_100"):
            st.session_state.sensibilidade_variavel = 1.0

    # Sliders
    sensibilidade_fixo = st.sidebar.slider(
        "Sensibilidade - Custo Fixo",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.sensibilidade_fixo,
        step=0.1,
        key="slider_sens_fixo",
        help="0 = Custo não varia com volume | 1 = Custo varia 100% com volume"
    )
    st.session_state.sensibilidade_fixo = sensibilidade_fixo

    sensibilidade_variavel = st.sidebar.slider(
        "Sensibilidade - Custo Variável",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.sensibilidade_variavel,
        step=0.1,
        key="slider_sens_var",
        help="0 = Custo não varia com volume | 1 = Custo varia 100% com volume"
    )
    st.session_state.sensibilidade_variavel = sensibilidade_variavel
else:
    st.sidebar.info("⚙️ Configure sensibilidades detalhadas na seção principal")
    # Usar valores do session_state como padrão
    sensibilidade_fixo = st.session_state.sensibilidade_fixo
    sensibilidade_variavel = st.session_state.sensibilidade_variavel

# ========== MODO INFLAÇÃO ==========
st.sidebar.markdown("### 📈 Inflação")
# Usar o valor do session_state diretamente, ou calcular o index
# Se a key do radio já existe, usar esse valor; senão, usar o valor do modo_inflacao
valor_atual_inf = st.session_state.get('radio_modo_inf', st.session_state.get('modo_inflacao', 'Global'))
index_inf = 0 if valor_atual_inf == "Global" else 1

modo_inflacao = st.sidebar.radio(
    "Modo de Inflação:",
    options=["Global", "Detalhado"],
    index=index_inf,
    key="radio_modo_inf",
    help="Global: mesma inflação para todos | Detalhado: configuração por categoria"
)
# Sincronizar session_state com o valor do radio button
# O Streamlit armazena o valor na key automaticamente, então usamos diretamente
st.session_state.modo_inflacao = modo_inflacao

if modo_inflacao == "Global":
    # Botões de inflação
    col_inf1, col_inf2, col_inf3 = st.sidebar.columns(3)
    with col_inf1:
        if st.button("📈 0%", key="btn_inf_0"):
            st.session_state.inflacao = 0.0
    with col_inf2:
        if st.button("📈 3%", key="btn_inf_3"):
            st.session_state.inflacao = 3.0
    with col_inf3:
        if st.button("📈 5%", key="btn_inf_5"):
            st.session_state.inflacao = 5.0

    inflacao = st.sidebar.number_input(
        "Inflação Global (%)",
        min_value=0.0,
        max_value=100.0,
        value=st.session_state.inflacao,
        step=0.5,
        key="input_inflacao",
        help="Inflação aplicada globalmente ao cálculo do FLEX"
    )
    st.session_state.inflacao = inflacao
else:
    st.sidebar.info("⚙️ Configure inflações detalhadas na seção principal")
    # Usar valor do session_state como padrão
    inflacao = st.session_state.inflacao

# ============================================================================
# APLICAR FATOR DE CONVERSÃO NO df_base (ANTES DOS FILTROS) - IGUAL AO APP.PY
# ============================================================================
# IMPORTANTE: Usar o fator do topo (fator_conversao) que já foi definido ANTES de carregar os dados
# Isso garante que quando o fator muda, o Streamlit recalcula tudo
# O fator_conversao já foi definido no topo da página (linha 244), igual ao app.py
col_valor_base = next((c for c in ["Total", "total", "Valor", "valor"] if c in df_base.columns), None)
if col_valor_base and fator_conversao and fator_conversao != "Nenhum":
    # Aplicar o fator diretamente no df_base (igual ao app.py linha 1088-1094)
    # Não criar cópia aqui, aplicar diretamente para garantir detecção de mudança
    divisor = 1000 if fator_conversao == "K (milhares)" else 1000000
    df_base[col_valor_base] = df_base[col_valor_base] / divisor

st.sidebar.markdown("---")

# Inicializar session_state para filtros
if 'filtro_oficina_waterfall' not in st.session_state:
    st.session_state.filtro_oficina_waterfall = ["Todos"]
if 'filtro_periodo_waterfall' not in st.session_state:
    st.session_state.filtro_periodo_waterfall = "Todos"
if 'filtro_veiculo_waterfall' not in st.session_state:
    st.session_state.filtro_veiculo_waterfall = ["Todos"]
if 'filtro_custo_waterfall' not in st.session_state:
    st.session_state.filtro_custo_waterfall = ["Todos"]
if 'filtro_ano_waterfall' not in st.session_state:
    st.session_state.filtro_ano_waterfall = ["Todos"]

# Filtro 1: Oficina
if 'Oficina' in df_base.columns:
    oficina_opcoes = ["Todos"] + sorted(df_base['Oficina'].dropna().astype(str).unique().tolist())
    default_oficina = st.session_state.filtro_oficina_waterfall if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_waterfall) else ["Todos"]
    oficina_selecionada = st.sidebar.multiselect("Selecione a OFICINA:", oficina_opcoes, default=default_oficina, key="filtro_oficina_waterfall_multiselect")
    st.session_state.filtro_oficina_waterfall = oficina_selecionada if oficina_selecionada else ["Todos"]
    
    if "Todos" in oficina_selecionada or not oficina_selecionada:
        df_filtrado = df_base.copy()
    else:
        df_filtrado = df_base[df_base['Oficina'].astype(str).isin(oficina_selecionada)]
else:
    df_filtrado = df_base.copy()

# Filtro 2: Período
if 'Período' in df_filtrado.columns:
    periodo_opcoes = ["Todos"] + sorted(df_filtrado['Período'].dropna().astype(str).unique().tolist())
    periodo_default = st.session_state.filtro_periodo_waterfall if st.session_state.filtro_periodo_waterfall in periodo_opcoes else "Todos"
    periodo_index = periodo_opcoes.index(periodo_default) if periodo_default in periodo_opcoes else 0
    periodo_selecionado = st.sidebar.selectbox("Selecione o Período:", periodo_opcoes, index=periodo_index, key="filtro_periodo_waterfall_selectbox")
    st.session_state.filtro_periodo_waterfall = periodo_selecionado
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Período'].astype(str) == str(periodo_selecionado)]

# Filtro 3: Veículo
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = ["Todos"] + sorted(df_filtrado['Veículo'].dropna().astype(str).unique().tolist())
    default_veiculo = st.session_state.filtro_veiculo_waterfall if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_waterfall) else ["Todos"]
    veiculo_selecionado = st.sidebar.multiselect("Selecione o VEÍCULO:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_waterfall_multiselect")
    st.session_state.filtro_veiculo_waterfall = veiculo_selecionado if veiculo_selecionado else ["Todos"]
    if veiculo_selecionado and "Todos" not in veiculo_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Veículo'].astype(str).isin(veiculo_selecionado)]

# Filtro 4: Tipo de Custo
if 'Custo' in df_filtrado.columns:
    custo_opcoes = ["Todos"] + sorted(df_filtrado['Custo'].dropna().astype(str).unique().tolist())
    default_custo = st.session_state.filtro_custo_waterfall if all(x in custo_opcoes for x in st.session_state.filtro_custo_waterfall) else ["Todos"]
    custo_selecionado = st.sidebar.multiselect("Selecione o TIPO DE CUSTO:", custo_opcoes, default=default_custo, key="filtro_custo_waterfall_multiselect")
    st.session_state.filtro_custo_waterfall = custo_selecionado if custo_selecionado else ["Todos"]
    if custo_selecionado and "Todos" not in custo_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Custo'].astype(str).isin(custo_selecionado)]

# Cache para opções de filtros (otimização de performance)
@st.cache_data(ttl=1800, max_entries=3)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache para melhor performance"""
    if column_name in df.columns:
        return ["Todos"] + sorted(df[column_name].dropna().astype(str).unique().tolist())
    return ["Todos"]

# Filtros principais (com cache otimizado)
filtros_principais = [
    ("Type 05", "Type 05", "multiselect"),
    ("Type 06", "Type 06", "multiselect"), 
    ("Type 07", "Type 07", "multiselect"),
    ("Account", "Account", "multiselect")
]

for col_name, label, widget_type in filtros_principais:
    if col_name in df_filtrado.columns:
        # Inicializar session_state para cada filtro principal
        filtro_key = f'filtro_{col_name}_waterfall'
        if filtro_key not in st.session_state:
            st.session_state[filtro_key] = ["Todos"]
        
        opcoes = get_filter_options(df_filtrado, col_name)
        if widget_type == "multiselect":
            # Validar valores salvos
            default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
            selecionadas = st.sidebar.multiselect(f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect")
            # Atualizar session_state
            st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
            if selecionadas and "Todos" not in selecionadas:
                df_filtrado = df_filtrado[df_filtrado[col_name].astype(str).isin(selecionadas)]

# Filtro 5: Ano (VISÍVEL na sidebar principal)
if 'Ano' in df_filtrado.columns:
    ano_opcoes = ["Todos"] + sorted(df_filtrado['Ano'].dropna().astype(str).unique().tolist())
    default_ano = st.session_state.filtro_ano_waterfall if all(x in ano_opcoes for x in st.session_state.filtro_ano_waterfall) else ["Todos"]
    ano_selecionado = st.sidebar.multiselect("Selecione o ANO:", ano_opcoes, default=default_ano, key="filtro_ano_waterfall_multiselect")
    st.session_state.filtro_ano_waterfall = ano_selecionado if ano_selecionado else ["Todos"]
    if ano_selecionado and "Todos" not in ano_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Ano'].astype(str).isin(ano_selecionado)]

# Filtros avançados (expansível)
with st.sidebar.expander("🔍 Filtros Avançados"):
    st.info("Filtros adicionais aparecerão aqui conforme necessário")

# Exibir informações dos filtros
st.sidebar.write(f"Número de linhas: {df_filtrado.shape[0]:,}")
st.sidebar.write(f"Número de colunas: {df_filtrado.shape[1]}")
if 'Total' in df_filtrado.columns:
    valor_total = df_filtrado['Total'].sum()
    valor_convertido = converter_moeda(valor_total, moeda_codigo, taxas_cambio)
    st.sidebar.write(f"Soma do Valor total: {moeda_simbolo} {valor_convertido:,.2f}")

# --- Configurações do waterfall ---
# Criar coluna Período_Ano para diferenciar meses de anos diferentes
if 'Período' in df_filtrado.columns and 'Ano' in df_filtrado.columns:
    # Criar uma coluna combinada Período + Ano
    df_filtrado['Período_Ano'] = df_filtrado['Período'].astype(str) + ' ' + df_filtrado['Ano'].astype(str)
    col_mes = 'Período_Ano'
    mes_unicos = sorted(df_filtrado['Período_Ano'].dropna().unique().tolist())
elif 'Período' in df_filtrado.columns:
    col_mes = 'Período'
    mes_unicos = sort_mes_unique(df_filtrado["Período"].astype(str))
else:
    col_mes = None
    mes_unicos = []

col_valor = next((c for c in ["Total", "total", "Valor", "valor"] if c in df_filtrado.columns), None)

# O fator já foi aplicado no df_base ANTES dos filtros (igual ao app.py)
# Então df_filtrado já tem o fator aplicado, não é necessário aplicar novamente aqui

# Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do app.py linha 1096-1100)
# Isso garante que todos os dados derivados já terão a conversão aplicada
# IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
if moeda_codigo != "BRL" and col_valor and col_valor in df_filtrado.columns:
    df_filtrado = converter_coluna_moeda(df_filtrado, col_valor, moeda_codigo, taxas_cambio)

# Dimensão de categoria no mesmo padrão
dims_cat = [c for c in ["Type 05", "Type 06", "Type 07", "Oficina", "Veículo", "Custo", "Account"] if c in df_filtrado.columns]
if not dims_cat or not col_valor or not col_mes:
    st.error("❌ Colunas necessárias não encontradas.")
    st.info(f"Colunas disponíveis: {', '.join(df_filtrado.columns.tolist())}")
    st.info(f"Colunas necessárias: Período, Total, e pelo menos uma dimensão de categoria")
    st.stop()

# ============================================================================
# 📊 ANÁLISE WATERFALL
# ============================================================================

# Função auxiliar para formatar valores com sufixo do fator (mesma lógica do app.py)
def formatar_valor_com_fator(valor, moeda_simbolo="R$"):
    """Formata valor com sufixo do fator de conversão"""
    # Usar o fator_conversao do topo (já definido antes de carregar os dados)
    sufixo = ""
    if fator_conversao and fator_conversao != "Nenhum":
        if fator_conversao == "K (milhares)":
            sufixo = " K"
        elif fator_conversao == "M (Milhões)":
            sufixo = " M"
    return f"{moeda_simbolo} {valor:,.2f}{sufixo}"

# --- Configurações da análise ---
# IMPORTANTE: Criar df_segunda_analise DEPOIS de aplicar o fator
# Isso garante que todos os cálculos subsequentes usem os valores com o fator aplicado
# O fator_conversao_waterfall está implícito aqui através do df_filtrado
df_segunda_analise = df_filtrado.copy()
chosen_dim_2 = st.selectbox("Dimensão da categoria:", dims_cat, index=min(1, len(dims_cat)-1), key="dim_2")

# ========== CONFIGURAÇÕES DETALHADAS (se ativado) ==========
# Usar session_state diretamente para garantir que os valores estejam atualizados
modo_sensibilidade_atual = st.session_state.get('modo_sensibilidade', 'Global')
modo_inflacao_atual = st.session_state.get('modo_inflacao', 'Global')

# Inicializar dicionários de valores temporários (igual ao Forecast)
if 'valores_temp_sens_fixo_2' not in st.session_state:
    st.session_state.valores_temp_sens_fixo_2 = {}
if 'valores_temp_sens_variavel_2' not in st.session_state:
    st.session_state.valores_temp_sens_variavel_2 = {}
if 'valores_temp_infl_2' not in st.session_state:
    st.session_state.valores_temp_infl_2 = {}
if 'widget_key_counter_2' not in st.session_state:
    st.session_state.widget_key_counter_2 = 0

dict_sens_fixo_2 = {}
dict_sens_variavel_2 = {}
dict_inflacao_2 = {}

# Mostrar status dos modos ativos
col_status1, col_status2 = st.columns(2)
with col_status1:
    if modo_sensibilidade_atual == "Detalhado":
        st.info("🎯 **Modo Sensibilidade:** Detalhado")
    else:
        st.success("🎯 **Modo Sensibilidade:** Global")
with col_status2:
    if modo_inflacao_atual == "Detalhado":
        st.info("📈 **Modo Inflação:** Detalhado")
    else:
        st.success("📈 **Modo Inflação:** Global")

# Exibir expander de configurações detalhadas
if modo_sensibilidade_atual == "Detalhado" or modo_inflacao_atual == "Detalhado":
    st.markdown("---")
    with st.expander("⚙️ Configurações Detalhadas por Categoria", expanded=True):
        st.markdown("Configure sensibilidade e inflação específicas para cada categoria")
        
        # Verificar se a dimensão foi selecionada
        if chosen_dim_2 and chosen_dim_2 in df_segunda_analise.columns:
            categorias_unicas = sorted(df_segunda_analise[chosen_dim_2].dropna().astype(str).unique().tolist())
            
            if not categorias_unicas:
                st.warning("⚠️ Nenhuma categoria encontrada para a dimensão selecionada.")
            else:
                st.info(f"📊 Configurando para **{len(categorias_unicas)}** categorias da dimensão **{chosen_dim_2}**")
                
                # Cabeçalho da tabela - 4 colunas: Categoria, Custo Fixo, Custo Variável, Inflação
                st.markdown("---")
                col_header0, col_header1, col_header2, col_header3 = st.columns([2, 2, 2, 2])
                with col_header0:
                    st.markdown("**Categoria**")
                with col_header1:
                    st.markdown("**Custo Fixo**")
                with col_header2:
                    st.markdown("**Custo Variável**")
                with col_header3:
                    st.markdown("**Inflação %**")
                st.markdown("---")
                
                # Criar linha para cada categoria com 4 colunas alinhadas
                for cat in categorias_unicas:
                    col_cat, col_fixo, col_var, col_infl = st.columns([2, 2, 2, 2])
                    
                    # Coluna 0: Nome da categoria
                    with col_cat:
                        st.markdown(f"<small><b>{cat}</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 1: Sensibilidade Custo Fixo
                    with col_fixo:
                        if modo_sensibilidade_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global
                            valor_padrao_sens_fixo = st.session_state.valores_temp_sens_fixo_2.get(str(cat), sensibilidade_fixo)
                            
                            sens_fixo = st.slider(
                                f"Sensibilidade Fixo",
                                min_value=0.0,
                                max_value=1.0,
                                value=valor_padrao_sens_fixo,
                                step=0.1,
                                key=f"sens_fixo_{cat}_2",
                                label_visibility="collapsed",
                                help=f"Sensibilidade do custo fixo para {cat}"
                            )
                            dict_sens_fixo_2[str(cat)] = sens_fixo
                            # Atualizar valor temporário
                            st.session_state.valores_temp_sens_fixo_2[str(cat)] = sens_fixo
                        else:
                            st.markdown(f"<small><b>{sensibilidade_fixo*100:.0f}%</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 2: Sensibilidade Custo Variável
                    with col_var:
                        if modo_sensibilidade_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global
                            valor_padrao_sens_var = st.session_state.valores_temp_sens_variavel_2.get(str(cat), sensibilidade_variavel)
                            
                            sens_var = st.slider(
                                f"Sensibilidade Variável",
                                min_value=0.0,
                                max_value=1.0,
                                value=valor_padrao_sens_var,
                                step=0.1,
                                key=f"sens_var_{cat}_2",
                                label_visibility="collapsed",
                                help=f"Sensibilidade do custo variável para {cat}"
                            )
                            dict_sens_variavel_2[str(cat)] = sens_var
                            # Atualizar valor temporário
                            st.session_state.valores_temp_sens_variavel_2[str(cat)] = sens_var
                        else:
                            st.markdown(f"<small><b>{sensibilidade_variavel*100:.0f}%</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 3: Inflação
                    with col_infl:
                        if modo_inflacao_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global (igual ao Forecast)
                            valor_padrao_infl = st.session_state.valores_temp_infl_2.get(str(cat), inflacao)
                            
                            # Usar contador de widget para forçar recriação quando necessário (igual ao Forecast)
                            widget_key = f"infl_{cat}_2_{st.session_state.widget_key_counter_2}"
                            
                            inflacao_cat = st.number_input(
                                "",
                                min_value=0.0,
                                max_value=100.0,
                                value=valor_padrao_infl,
                                step=0.5,
                                format="%.2f",
                                key=widget_key,
                                help=f"Inflação (%) para {cat}"
                            )
                            dict_inflacao_2[str(cat)] = inflacao_cat
                            # Atualizar valor temporário
                            st.session_state.valores_temp_infl_2[str(cat)] = inflacao_cat
                        else:
                            # Mostrar valor global quando não está em modo detalhado
                            st.markdown(f"<small><b>{inflacao:.2f}%</b></small>", unsafe_allow_html=True)
                    
                    st.markdown("---")
        else:
            st.warning("⚠️ Selecione uma dimensão de categoria acima para configurar valores detalhados.")
    
    st.markdown("---")

# Modo de comparação
st.markdown("### 📅 Modo de Comparação")
modo_comparacao = st.radio(
    "Tipo de comparação:",
    options=["Mês a Mês", "Ano a Ano", "Semestre", "Quarter", "Múltiplos Meses"],
    index=0,
    key="modo_comparacao",
    help="Mês a Mês: compara dois meses | Ano a Ano: compara totais anuais | Semestre: compara semestres | Quarter: compara trimestres | Múltiplos Meses: compara vários meses"
)

st.markdown("---")

if modo_comparacao == "Mês a Mês":
    # Modo original: dois meses
    col_a2, col_b2 = st.columns(2)
    with col_a2:
        mes_inicial_2 = st.selectbox("Mês inicial:", mes_unicos, index=0 if mes_unicos else None, key="mes_inicial_2")
    with col_b2:
        mes_final_2 = st.selectbox("Mês final:", mes_unicos, index=len(mes_unicos) - 1 if mes_unicos else None, key="mes_final_2")
    meses_selecionados_2 = [mes_inicial_2, mes_final_2] if mes_inicial_2 and mes_final_2 else []
    
elif modo_comparacao == "Ano a Ano":
    # Comparação ano a ano
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 2:
            col_ano1, col_ano2 = st.columns(2)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1), key="ano_final")
            mes_inicial_2 = f"Total {ano_inicial}"
            mes_final_2 = f"Total {ano_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 2 anos de dados para comparação ano a ano.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação ano a ano.")
        st.stop()

elif modo_comparacao == "Semestre":
    # Comparação semestre a semestre
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 1:
            col_ano1, col_sem1, col_ano2, col_sem2 = st.columns(4)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_sem")
            with col_sem1:
                semestre_inicial = st.selectbox("Semestre inicial:", [1, 2], index=0, key="semestre_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_sem")
            with col_sem2:
                semestre_final = st.selectbox("Semestre final:", [1, 2], index=1, key="semestre_final")
            mes_inicial_2 = f"{ano_inicial} S{semestre_inicial}"
            mes_final_2 = f"{ano_final} S{semestre_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de semestres.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação de semestres.")
        st.stop()

elif modo_comparacao == "Quarter":
    # Comparação quarter a quarter
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 1:
            col_ano1, col_q1, col_ano2, col_q2 = st.columns(4)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_q")
            with col_q1:
                trimestre_inicial = st.selectbox("Quarter inicial:", [1, 2, 3, 4], index=0, key="trimestre_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_q")
            with col_q2:
                trimestre_final = st.selectbox("Quarter final:", [1, 2, 3, 4], index=1, key="trimestre_final")
            mes_inicial_2 = f"{ano_inicial} Q{trimestre_inicial}"
            mes_final_2 = f"{ano_final} Q{trimestre_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de quarters.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação de quarters.")
        st.stop()
        
else:  # Múltiplos Meses
    meses_selecionados_2 = st.multiselect(
        "Selecione os meses para comparação:",
        mes_unicos,
        default=mes_unicos[:min(3, len(mes_unicos))] if mes_unicos else [],
        key="meses_multiplos"
    )
    if len(meses_selecionados_2) < 2:
        st.warning("⚠️ Selecione pelo menos 2 meses para comparação.")
        st.stop()
    mes_inicial_2 = meses_selecionados_2[0]
    mes_final_2 = meses_selecionados_2[-1]

# Normalizar categorias (strings limpas) e garantir defaults válidos
cats_all_2 = sorted([str(x).strip() for x in df_segunda_analise[chosen_dim_2].dropna().unique().tolist() if str(x).strip() != ""])
total_cats_2 = max(1, len(cats_all_2))
max_cats_2 = st.slider(f"Quantidade de categorias a exibir (Top N) (Total: {total_cats_2}):", 1, total_cats_2, min(total_cats_2, 20), key="max_cats_2")
vol_mf_2 = (df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)].groupby(chosen_dim_2)[col_valor].sum().sort_values(ascending=False))
vol_index_2 = [str(c).strip() for c in list(vol_mf_2.index)]
default_cats_2 = vol_index_2[:max_cats_2] if len(vol_index_2) else cats_all_2[:max_cats_2]

cats_options_2 = ["Todos"] + cats_all_2
# Filtrar defaults não presentes; fallback seguro
default_cats_2 = [c for c in default_cats_2 if c in cats_all_2]
if not default_cats_2:
    default_cats_2 = cats_all_2[:min(10, len(cats_all_2))]

cats_sel_raw_2 = st.multiselect("Categorias (uma ou mais):", cats_options_2, default=default_cats_2, key="cats_2")
if (not cats_sel_raw_2) or ("Todos" in cats_sel_raw_2):
    cats_sel_2 = cats_all_2[:max_cats_2] if max_cats_2 < len(cats_all_2) else cats_all_2
else:
    cats_sel_2 = cats_sel_raw_2

# Calcular totais baseado no modo de comparação
if modo_comparacao == "Ano a Ano":
    # Tratar anos como períodos únicos (mesma lógica de Mês a Mês)
    # Agrupar por ano e calcular totais
    df_ano_inicial = df_segunda_analise[df_segunda_analise['Ano'].astype(str) == str(ano_inicial)]
    df_ano_final = df_segunda_analise[df_segunda_analise['Ano'].astype(str) == str(ano_final)]
    total_m1_all_2 = float(df_ano_inicial[col_valor].sum())
    total_m2_all_2 = float(df_ano_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada ano (mesma lógica de Mês a Mês)
    meses_ano_inicial = sorted(df_ano_inicial[col_mes].dropna().unique().tolist())
    meses_ano_final = sorted(df_ano_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_ano_inicial[0] if meses_ano_inicial else None
    mes_final_flex = meses_ano_final[-1] if meses_ano_final else None
    # Definir mes_inicial_2 e mes_final_2 como os anos para manter compatibilidade
    mes_inicial_2 = f"Ano {ano_inicial}"
    mes_final_2 = f"Ano {ano_final}"

elif modo_comparacao == "Semestre":
    # Tratar semestres como períodos únicos
    meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                     2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
    meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
    meses_sem_final = meses_semestre.get(semestre_final, [])
    
    df_sem_inicial = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_inicial)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial]))
    ]
    df_sem_final = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_final)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_final]))
    ]
    total_m1_all_2 = float(df_sem_inicial[col_valor].sum())
    total_m2_all_2 = float(df_sem_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada semestre
    meses_sem_inicial_list = sorted(df_sem_inicial[col_mes].dropna().unique().tolist())
    meses_sem_final_list = sorted(df_sem_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_sem_inicial_list[0] if meses_sem_inicial_list else None
    mes_final_flex = meses_sem_final_list[-1] if meses_sem_final_list else None

elif modo_comparacao == "Quarter":
    # Tratar quarters como períodos únicos
    meses_trimestre = {
        1: ['janeiro', 'fevereiro', 'março'],
        2: ['abril', 'maio', 'junho'],
        3: ['julho', 'agosto', 'setembro'],
        4: ['outubro', 'novembro', 'dezembro']
    }
    meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
    meses_trim_final = meses_trimestre.get(trimestre_final, [])
    
    df_trim_inicial = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_inicial)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial]))
    ]
    df_trim_final = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_final)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_final]))
    ]
    total_m1_all_2 = float(df_trim_inicial[col_valor].sum())
    total_m2_all_2 = float(df_trim_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada quarter
    meses_trim_inicial_list = sorted(df_trim_inicial[col_mes].dropna().unique().tolist())
    meses_trim_final_list = sorted(df_trim_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_trim_inicial_list[0] if meses_trim_inicial_list else None
    mes_final_flex = meses_trim_final_list[-1] if meses_trim_final_list else None

elif modo_comparacao == "Múltiplos Meses":
    # Para múltiplos meses, calcular do primeiro ao último
    total_m1_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_inicial_2)][col_valor].sum())
    total_m2_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)][col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    mes_inicial_flex = mes_inicial_2
    mes_final_flex = mes_final_2
else:  # Mês a Mês
    # Calcular totais (validação será feita depois)
    total_m1_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_inicial_2)][col_valor].sum())
    total_m2_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)][col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    mes_inicial_flex = mes_inicial_2
    mes_final_flex = mes_final_2

# Validar que os períodos são diferentes (mesma lógica para todos os modos)
if modo_comparacao == "Ano a Ano":
    if ano_inicial == ano_final:
        st.info("⚠️ Selecione anos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Semestre":
    if ano_inicial == ano_final and semestre_inicial == semestre_final:
        st.info("⚠️ Selecione períodos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Quarter":
    if ano_inicial == ano_final and trimestre_inicial == trimestre_final:
        st.info("⚠️ Selecione períodos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Mês a Mês":
    if mes_inicial_2 == mes_final_2:
        st.info("⚠️ Selecione meses diferentes para comparar.")
        st.stop()

# Processar análise se períodos são diferentes
if (modo_comparacao == "Ano a Ano" and ano_inicial != ano_final) or \
   (modo_comparacao == "Semestre" and (ano_inicial != ano_final or semestre_inicial != semestre_final)) or \
   (modo_comparacao == "Quarter" and (ano_inicial != ano_final or trimestre_inicial != trimestre_final)) or \
   (modo_comparacao == "Mês a Mês" and mes_inicial_2 != mes_final_2) or \
   modo_comparacao == "Múltiplos Meses":
    
    # Calcular FLEX para segunda análise (separado em Volume e Inflação)
    # Usar session_state diretamente para garantir valores atualizados
    modo_sensibilidade_atual = st.session_state.get('modo_sensibilidade', 'Global')
    modo_inflacao_atual = st.session_state.get('modo_inflacao', 'Global')
    
    # Aplicar os mesmos filtros ao df_volume que foram aplicados ao df_segunda_analise
    # Isso garante que os volumes sejam calculados apenas para as oficinas/veículos selecionados
    # MESMA LÓGICA DO GRÁFICO "Volume total por período"
    df_volume_filtrado = df_volume.copy()
    
    # Aplicar filtro de Oficina (mesma lógica do gráfico de volume - linha 4204-4227 do app.py)
    if 'Oficina' in df_volume_filtrado.columns and 'Oficina' in df_segunda_analise.columns:
        # Obter as opções de oficina disponíveis no df_segunda_analise (mesmas opções do filtro principal)
        oficina_opcoes_disponiveis = sorted(df_segunda_analise['Oficina'].dropna().astype(str).unique().tolist())
        
        # Obter oficinas selecionadas no filtro
        oficina_selecionada = st.session_state.get('filtro_oficina_waterfall', ["Todos"])
        
        # Se "Todos" estiver selecionado, usar todas as opções disponíveis no filtro
        if "Todos" in oficina_selecionada or not oficina_selecionada:
            # Filtrar apenas pelas oficinas que estão nas opções do filtro (não incluir oficinas que não estão no filtro)
            df_volume_filtrado = df_volume_filtrado[
                df_volume_filtrado['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
            ].copy()
        else:
            # Filtrar apenas pelas oficinas selecionadas (que já estão nas opções do filtro)
            # Garantir que apenas oficinas que estão nas opções sejam consideradas
            oficinas_validas = [o for o in oficina_selecionada if o in oficina_opcoes_disponiveis]
            if oficinas_validas:
                df_volume_filtrado = df_volume_filtrado[
                    df_volume_filtrado['Oficina'].astype(str).isin(oficinas_validas)
                ].copy()
            else:
                # Se nenhuma válida, usar todas as disponíveis
                df_volume_filtrado = df_volume_filtrado[
                    df_volume_filtrado['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
                ].copy()
    
    # Aplicar filtro de Veículo (mesma lógica do gráfico de volume)
    if 'Veículo' in df_volume_filtrado.columns and 'Veículo' in df_segunda_analise.columns:
        # Obter as opções de veículo disponíveis no df_segunda_analise (mesmas opções do filtro principal)
        veiculo_opcoes_disponiveis = sorted(df_segunda_analise['Veículo'].dropna().astype(str).unique().tolist())
        
        # Obter veículos selecionados no filtro
        veiculo_selecionado = st.session_state.get('filtro_veiculo_waterfall', ["Todos"])
        
        # Se "Todos" estiver selecionado, usar todas as opções disponíveis no filtro
        if "Todos" in veiculo_selecionado or not veiculo_selecionado:
            # Filtrar apenas pelos veículos que estão nas opções do filtro
            df_volume_filtrado = df_volume_filtrado[
                df_volume_filtrado['Veículo'].astype(str).isin(veiculo_opcoes_disponiveis)
            ].copy()
        else:
            # Filtrar apenas pelos veículos selecionados (que já estão nas opções do filtro)
            veiculos_validos = [v for v in veiculo_selecionado if v in veiculo_opcoes_disponiveis]
            if veiculos_validos:
                df_volume_filtrado = df_volume_filtrado[
                    df_volume_filtrado['Veículo'].astype(str).isin(veiculos_validos)
                ].copy()
            else:
                # Se nenhum válido, usar todos os disponíveis
                df_volume_filtrado = df_volume_filtrado[
                    df_volume_filtrado['Veículo'].astype(str).isin(veiculo_opcoes_disponiveis)
                ].copy()
    
    # Calcular FLEX para Mês a Mês, Ano a Ano, Semestre e Quarter
    if (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]) and mes_inicial_flex and mes_final_flex:
        # Para períodos agregados (Ano, Semestre, Quarter), passar parâmetros específicos
        # Para Mês a Mês, passar None para usar volumes dos meses específicos
        flex_volume_2, flex_inflacao_2, volume_inicial_2, volume_final_2 = calcular_flex(
            df_segunda_analise, df_volume_filtrado, mes_inicial_flex, mes_final_flex, col_mes, col_valor,
            sensibilidade_fixo, sensibilidade_variavel, inflacao,
            modo_sensibilidade_atual, dict_sens_fixo_2, dict_sens_variavel_2,
            modo_inflacao_atual, dict_inflacao_2, chosen_dim_2,
            modo_comparacao,
            ano_inicial if modo_comparacao in ["Ano a Ano", "Semestre", "Quarter"] else None,
            ano_final if modo_comparacao in ["Ano a Ano", "Semestre", "Quarter"] else None,
            semestre_inicial if modo_comparacao == "Semestre" else None,
            semestre_final if modo_comparacao == "Semestre" else None,
            trimestre_inicial if modo_comparacao == "Quarter" else None,
            trimestre_final if modo_comparacao == "Quarter" else None
        )
    else:
        flex_volume_2 = 0.0
        flex_inflacao_2 = 0.0
        volume_inicial_2 = 0.0
        volume_final_2 = 0.0
    flex_total_2 = flex_volume_2 + flex_inflacao_2

    # Debug: verificar valores do FLEX ANTES da conversão
    # st.write(f"DEBUG ANTES - Flex Volume: {flex_volume_2}, Flex Inflação: {flex_inflacao_2}, Inflação: {inflacao}, Sensibilidade: {sensibilidade_variavel}")

    # IMPORTANTE: O fator e a conversão de moeda já foram aplicados nos dados (df_filtrado[col_valor])
    # Então os valores já vêm com fator e moeda aplicados, não precisamos converter novamente
    # Apenas garantir que são floats
    total_m1_all_2 = float(total_m1_all_2)
    total_m2_all_2 = float(total_m2_all_2)
    flex_volume_2 = float(flex_volume_2)
    flex_inflacao_2 = float(flex_inflacao_2)
    flex_total_2 = float(flex_total_2)
    change_all_2 = float(change_all_2)
    
    # Debug: verificar valores do FLEX DEPOIS da conversão
    # st.write(f"DEBUG DEPOIS - Flex Volume: {flex_volume_2}, Flex Inflação: {flex_inflacao_2}")

    # Filtrar pelas selecionadas - otimizado: pré-converter coluna uma vez
    dff_2 = df_segunda_analise.copy()
    dff_2['_dim_str'] = dff_2[chosen_dim_2].astype(str)
    dff_2 = dff_2[dff_2['_dim_str'].isin(cats_sel_2)].copy()

    # Pré-converter colunas para evitar conversões repetidas
    if 'Ano' in dff_2.columns:
        dff_2['_ano_str'] = dff_2['Ano'].astype(str)
    if col_mes in dff_2.columns:
        dff_2['_mes_str'] = dff_2[col_mes].astype(str)
        # Pré-calcular primeiro mês para filtros de semestre/quarter
        dff_2['_mes_lower'] = dff_2['_mes_str'].str.lower().str.split(' ', n=1).str[0]

    # Calcular grupos - otimizado com colunas pré-convertidas
    if modo_comparacao == "Ano a Ano":
        # Agrupar por ano
        g1_2 = dff_2[dff_2['_ano_str'] == str(ano_inicial)].groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = dff_2[dff_2['_ano_str'] == str(ano_final)].groupby(chosen_dim_2)[col_valor].sum()
    elif modo_comparacao == "Semestre":
        # Agrupar por semestre - otimizado
        meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                         2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
        meses_sem_inicial_set = set(m.lower() for m in meses_semestre.get(semestre_inicial, []))
        meses_sem_final_set = set(m.lower() for m in meses_semestre.get(semestre_final, []))
        df_g1 = dff_2[(dff_2['_ano_str'] == str(ano_inicial)) & (dff_2['_mes_lower'].isin(meses_sem_inicial_set))]
        df_g2 = dff_2[(dff_2['_ano_str'] == str(ano_final)) & (dff_2['_mes_lower'].isin(meses_sem_final_set))]
        g1_2 = df_g1.groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = df_g2.groupby(chosen_dim_2)[col_valor].sum()
    elif modo_comparacao == "Quarter":
        # Agrupar por quarter - otimizado
        meses_trimestre = {
            1: ['janeiro', 'fevereiro', 'março'],
            2: ['abril', 'maio', 'junho'],
            3: ['julho', 'agosto', 'setembro'],
            4: ['outubro', 'novembro', 'dezembro']
        }
        meses_trim_inicial_set = set(m.lower() for m in meses_trimestre.get(trimestre_inicial, []))
        meses_trim_final_set = set(m.lower() for m in meses_trimestre.get(trimestre_final, []))
        df_g1 = dff_2[(dff_2['_ano_str'] == str(ano_inicial)) & (dff_2['_mes_lower'].isin(meses_trim_inicial_set))]
        df_g2 = dff_2[(dff_2['_ano_str'] == str(ano_final)) & (dff_2['_mes_lower'].isin(meses_trim_final_set))]
        g1_2 = df_g1.groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = df_g2.groupby(chosen_dim_2)[col_valor].sum()
    else:
        # Mês a Mês ou Múltiplos Meses - otimizado
        g1_2 = dff_2[dff_2['_mes_str'] == str(mes_inicial_2)].groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = dff_2[dff_2['_mes_str'] == str(mes_final_2)].groupby(chosen_dim_2)[col_valor].sum()

    labels_cats_2, values_cats_2 = [], []
    for cat in sorted(set(g1_2.index).union(set(g2_2.index))):
        delta = float(g2_2.get(cat, 0.0)) - float(g1_2.get(cat, 0.0))
        if abs(delta) > 1e-9:
            labels_cats_2.append(str(cat))
            # IMPORTANTE: O fator e a conversão de moeda já foram aplicados nos dados
            # Então delta já vem com fator e moeda aplicados, não precisamos converter novamente
            values_cats_2.append(float(delta))

    original_len_2 = len(labels_cats_2)
    if len(labels_cats_2) > max_cats_2:
        idx = sorted(range(len(values_cats_2)), key=lambda i: abs(values_cats_2[i]), reverse=True)[:max_cats_2]
        labels_cats_2 = [labels_cats_2[i] for i in idx]
        values_cats_2 = [values_cats_2[i] for i in idx]
    cropped_2 = len(labels_cats_2) < original_len_2

    # Calcular remainder baseado no modo de comparação
    if modo_comparacao == "Múltiplos Meses":
        # Para múltiplos meses, o remainder será calculado depois de incluir as variações dos meses intermediários
        # Por enquanto, calcular apenas com base nas categorias
        remainder_2 = round(change_all_2 - sum(values_cats_2) - flex_total_2, 2)
    else:
        remainder_2 = round(change_all_2 - sum(values_cats_2) - flex_total_2, 2)
    
    all_selected_2 = len(cats_sel_2) >= len(cats_all_2)
    show_outros_2 = (abs(remainder_2) >= 0.01) and (cropped_2 or not all_selected_2 or len(cats_sel_2) < len(cats_all_2))
    if show_outros_2:
        labels_cats_2.append("Outros")
        values_cats_2.append(remainder_2)

    # Inserir FLEX VOLUME e FLEX INFLAÇÃO após o período inicial e antes das categorias
    # (para Mês a Mês, Ano a Ano, Semestre e Quarter, quando houver valores de FLEX)
    if modo_comparacao == "Múltiplos Meses":
        # Para múltiplos meses, não incluir FLEX - usar estrutura com barras azuis para cada mês
        # Estrutura desejada: Barra Azul (Mês1) -> Categorias Mês1 -> Variação -> Barra Azul (Mês2) -> Categorias Mês2 -> Variação -> Barra Azul (Mês3) -> Categorias Mês3
        # Cada mês deve ter suas próprias categorias logo após sua barra azul
        
        # Construir labels e valores para todos os meses
        labels_meses_completos = []
        valores_meses_completos = []
        medidas_meses_completos = []
        
        # Primeiro mês: barra azul inicial (absolute) - já convertido acima
        labels_meses_completos.append(f"{mes_inicial_2}")
        valores_meses_completos.append(total_m1_all_2)  # Já convertido
        medidas_meses_completos.append("absolute")
        
        # As categorias serão calculadas como diferença entre o primeiro e último mês
        # Elas aparecerão logo após o primeiro mês para mostrar a composição
        # Usar as categorias já calculadas (labels_cats_2, values_cats_2) que são a diferença entre primeiro e último
        # Adicionar categorias logo após o primeiro mês
        labels_meses_completos.extend(labels_cats_2)
        valores_meses_completos.extend(values_cats_2)
        medidas_meses_completos.extend(["relative"] * len(labels_cats_2))
        
        total_anterior = total_m1_all_2 + sum(values_cats_2)
        
        # Otimização: pré-calcular totais de todos os meses de uma vez
        meses_intermediarios = meses_selecionados_2[1:-1]
        if meses_intermediarios:
            # Agrupar uma única vez por mês
            df_totais_meses = df_segunda_analise.groupby(col_mes)[col_valor].sum().to_dict()
            totais_meses = {}
            for mes in meses_intermediarios:
                # IMPORTANTE: O fator e a conversão de moeda já foram aplicados nos dados
                # Então total_bruto já vem com fator e moeda aplicados
                total_bruto = float(df_totais_meses.get(str(mes), 0.0))
                totais_meses[mes] = total_bruto
        
        # Para cada mês intermediário (do segundo ao penúltimo)
        for idx, mes in enumerate(meses_intermediarios):
            total_mes = totais_meses.get(mes, 0.0)
            
            # Calcular variação do mês anterior para este mês
            variacao = total_mes - total_anterior
            
            # Adicionar variação (relative) - barra verde/vermelha
            labels_meses_completos.append(f"Δ {mes}")
            valores_meses_completos.append(variacao)
            medidas_meses_completos.append("relative")
            
            # Adicionar mês como barra azul (total) - barra azul
            labels_meses_completos.append(f"{mes}")
            valores_meses_completos.append(total_mes)
            medidas_meses_completos.append("total")
            
            # Atualizar para próximo cálculo
            total_anterior = total_mes
        
        # Se houver apenas 2 meses, não adicionar variação intermediária
        # Se houver 3 ou mais meses, adicionar variação do penúltimo para o último
        if len(meses_selecionados_2) > 2:
            # Calcular variação do penúltimo para o último mês
            variacao_final = total_m2_all_2 - total_anterior
            labels_meses_completos.append(f"Δ {mes_final_2}")
            valores_meses_completos.append(variacao_final)
            medidas_meses_completos.append("relative")
        
        # Adicionar último mês como barra azul (total)
        labels_meses_completos.append(f"{mes_final_2}")
        valores_meses_completos.append(total_m2_all_2)
        medidas_meses_completos.append("total")
        
        # Montar estrutura final
        labels_2 = labels_meses_completos
        values_2 = valores_meses_completos
        measures_2 = medidas_meses_completos
        
        # Para múltiplos meses, não usar remainder tradicional, pois cada mês tem suas próprias categorias
        remainder_2 = 0
    elif (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]):
        # Modos com FLEX: Mês a Mês, Ano a Ano, Semestre, Quarter
        # Sempre incluir FLEX quando há inflação configurada ou quando há valores calculados
        # Incluir FLEX mesmo que seja zero ou muito pequeno (para debug e visualização)
        tem_flex_calculado = (abs(flex_volume_2) > 1e-9 or abs(flex_inflacao_2) > 1e-9) or (inflacao != 0.0)
        if tem_flex_calculado:
            labels_2 = [f"{mes_inicial_2}", "Flex Volume", "Flex Inflação"] + labels_cats_2 + [f"{mes_final_2}"]
            values_2 = [total_m1_all_2, flex_volume_2, flex_inflacao_2] + values_cats_2 + [total_m2_all_2]
            measures_2 = ["absolute", "relative", "relative"] + ["relative"] * len(values_cats_2) + ["total"]
        else:
            # Sem FLEX calculado
            labels_2 = [f"{mes_inicial_2}"] + labels_cats_2 + [f"{mes_final_2}"]
            values_2 = [total_m1_all_2] + values_cats_2 + [total_m2_all_2]
            measures_2 = ["absolute"] + ["relative"] * len(values_cats_2) + ["total"]
    else:
        # Modos sem FLEX ou quando FLEX é zero: Mês a Mês, Ano a Ano, Semestre, Quarter (sem FLEX)
        # Estrutura: Mês Inicial -> Categorias -> Mês Final (SEM deltas)
        labels_2 = [f"{mes_inicial_2}"] + labels_cats_2 + [f"{mes_final_2}"]
        values_2 = [total_m1_all_2] + values_cats_2 + [total_m2_all_2]
        measures_2 = ["absolute"] + ["relative"] * len(values_cats_2) + ["total"]

    # Tema do Streamlit para cores
    theme_base = st.get_option("theme.base") or "light"
    # Garantir que text_color seja sempre definido corretamente baseado no tema
    if theme_base == "dark":
        text_color = "#FAFAFA"  # Branco para dark mode
    else:
        text_color = "#000000"  # Preto para light mode
    grid_color = "rgba(255,255,255,0.12)" if theme_base == "dark" else "rgba(0,0,0,0.12)"
    connector_color = "rgba(255,255,255,0.35)" if theme_base == "dark" else "rgba(0,0,0,0.35)"
    
    # Criar gráfico waterfall 2
    # Definir cores das barras
    cor_vermelha = "#ff5733"  # Vermelho alaranjado para aumentos (mais para vermelho)
    cor_verde = "#1e8449"     # Verde mais escuro para diminuições
    cor_azul = "#1e6ba8"      # Azul para totais (mais escuro)
    cor_flex = "#000000"       # Cor única para FLEX Volume e FLEX Inflação (preto)
    cor_borda_flex_volume = "#333333"  # Borda cinza escuro para FLEX Volume
    cor_borda_flex_inflacao = "#666666"  # Borda cinza médio para FLEX Inflação
    cor_laranja_outros = "#ff9800"  # Laranja para Outros
    
    # Criar anotações de forma otimizada (apenas para barras que precisam)
    annotations_custom = []
    cumulative = 0
    
    # Pré-calcular textos formatados uma única vez
    texts_formatted = [f"{abs(v):,.1f}" for v in values_2]
    
    # Calcular posições acumuladas para anotações
    # Incluir também "Flex Volume" e "Flex Inflação" para texto acima
    for i, (measure, value, label, text_fmt) in enumerate(zip(measures_2, values_2, labels_2, texts_formatted)):
        # Tratar barras FLEX (pretas) - texto acima em preto
        # Elas são "relative", então seguem a mesma lógica das outras barras relativas
        if label == "Flex Volume":
            # Para barra relative positiva, y_pos é cumulative + value
            y_pos = cumulative + value if value >= 0 else cumulative + value
            yshift_val = 15 if value >= 0 else -15  # Rótulo acima se positivo, abaixo se negativo
            annotations_custom.append(dict(
                x=label, y=y_pos, text=text_fmt,
                showarrow=False, font=dict(color=cor_flex, size=9), yshift=yshift_val
            ))
            cumulative += value
            continue
        elif label == "Flex Inflação":
            # Para barra relative positiva, y_pos é cumulative + value
            y_pos = cumulative + value if value >= 0 else cumulative + value
            yshift_val = 15 if value >= 0 else -15  # Rótulo acima se positivo, abaixo se negativo
            annotations_custom.append(dict(
                x=label, y=y_pos, text=text_fmt,
                showarrow=False, font=dict(color=cor_flex, size=9), yshift=yshift_val
            ))
            cumulative += value
            continue
            
        if measure == "absolute":
            y_pos = value  # Topo da barra azul
            cumulative = value
            # Aumentar yshift para ficar acima da barra, não no meio
            yshift_val = 15  # Rótulo acima da barra
            annotations_custom.append(dict(
                x=label, y=y_pos, text=text_fmt,
                showarrow=False, font=dict(color=cor_azul, size=9), yshift=yshift_val
            ))
        elif measure == "relative":
            if value >= 0:
                cor_texto = cor_vermelha
                y_pos = cumulative + value  # Topo da barra
                yshift_val = 15  # Rótulo acima da barra
            else:
                cor_texto = cor_verde
                y_pos = cumulative + value  # Base da barra (valor negativo)
                yshift_val = -15  # Rótulo abaixo da barra
            annotations_custom.append(dict(
                x=label, y=y_pos, text=text_fmt,
                showarrow=False, font=dict(color=cor_texto, size=9), yshift=yshift_val
            ))
            cumulative += value
        elif measure == "total":
            y_pos = value  # Topo da barra azul final
            # Aumentar yshift para ficar acima da barra, não no meio
            yshift_val = 15  # Rótulo acima da barra
            annotations_custom.append(dict(
                x=label, y=y_pos, text=text_fmt,
                showarrow=False, font=dict(color=cor_azul, size=9), yshift=yshift_val
            ))
    
    # Criar figura do waterfall
    fig_2 = go.Figure(go.Waterfall(
        name="Waterfall",
        orientation="v",
        measure=measures_2,
        x=labels_2,
        y=values_2,
        textposition="none",
        connector={"line": {"color": "rgba(0, 0, 0, 0)"}},  # Linha transparente (removida)
        increasing={"marker": {"color": cor_vermelha}},
        decreasing={"marker": {"color": cor_verde}},
        totals={"marker": {"color": cor_azul}}
    ))
    
    # Adicionar overlay para colorir FLEX VOLUME, FLEX INFLAÇÃO e Outros
    # Para FLEX VOLUME (roxo) - para Mês a Mês, Ano a Ano, Semestre e Quarter
    # Verificar se FLEX está nos labels (foi incluído no waterfall principal)
    tem_flex_volume = "Flex Volume" in labels_2
    tem_flex_inflacao = "Flex Inflação" in labels_2
    
    # Adicionar traços overlay para FLEX (sempre que estiver nos labels)
    if tem_flex_volume:
        # Calcular posição base do FLEX Volume
        flex_pos_volume = total_m1_all_2
        flex_height_volume = abs(flex_volume_2) if abs(flex_volume_2) > 1e-6 else 0.01  # Mínimo para visualização
        fig_2.add_trace(go.Bar(
            x=['Flex Volume'],
            y=[flex_height_volume],
            base=[flex_pos_volume if flex_volume_2 >= 0 else flex_pos_volume + flex_volume_2],
            marker_color=cor_flex,  # Mesma cor para ambos
            marker_line=dict(color=cor_borda_flex_volume, width=2),  # Borda diferente
            opacity=1.0,
            hovertemplate=f"<b>Flex Volume</b><br>Valor: {formatar_valor_com_fator(abs(flex_volume_2), moeda_simbolo)}<br>Efeito de Volume + Sensibilidade<extra></extra>",
            showlegend=False,
            name='Flex Volume',
            textposition='none'  # Não mostrar texto dentro, usar anotação acima
        ))
    
    # Para FLEX INFLAÇÃO (laranja claro)
    if tem_flex_inflacao:
        # Calcular posição base do FLEX Inflação (após FLEX Volume)
        flex_pos_inflacao = total_m1_all_2 + flex_volume_2
        flex_height_inflacao = abs(flex_inflacao_2) if abs(flex_inflacao_2) > 1e-6 else 0.01  # Mínimo para visualização
        fig_2.add_trace(go.Bar(
            x=['Flex Inflação'],
            y=[flex_height_inflacao],
            base=[flex_pos_inflacao if flex_inflacao_2 >= 0 else flex_pos_inflacao + flex_inflacao_2],
            marker_color=cor_flex,  # Mesma cor para ambos
            marker_line=dict(color=cor_borda_flex_inflacao, width=2),  # Borda diferente
            opacity=1.0,
            hovertemplate=f"<b>Flex Inflação</b><br>Valor: {formatar_valor_com_fator(abs(flex_inflacao_2), moeda_simbolo)}<br>Efeito da Inflação<extra></extra>",
            showlegend=False,
            name='Flex Inflação',
            textposition='none'  # Não mostrar texto dentro, usar anotação acima
        ))
    
    # Para Outros (laranja)
    if show_outros_2:
        prev_sum_2 = sum(v for lab, v in zip(labels_cats_2, values_cats_2) if lab != "Outros")
        cum_before_2 = total_m1_all_2 + flex_volume_2 + flex_inflacao_2 + prev_sum_2
        base_val_2 = cum_before_2 if remainder_2 >= 0 else cum_before_2 + remainder_2
        height_2 = abs(remainder_2)
        fig_2.add_trace(go.Bar(
            x=['Outros'], 
            y=[height_2], 
            base=[base_val_2], 
            marker_color=cor_laranja_outros,
            opacity=1.0,
            hoverinfo='skip',
            showlegend=False,
            textposition='inside',
            text=[f"{height_2:,.1f}"],
            textfont=dict(color=cor_laranja_outros, size=9)  # Rótulo laranja para corresponder à barra
        ))
    
    # Definir barmode como overlay para sobrepor as barras customizadas
    if show_outros_2 or tem_flex_volume or tem_flex_inflacao:
        fig_2.update_layout(barmode='overlay')

    # Título baseado no modo de comparação
    if modo_comparacao == "Ano a Ano":
        titulo_grafico = f"Análise Waterfall - Ano {ano_inicial} vs Ano {ano_final}"
    elif modo_comparacao == "Semestre":
        titulo_grafico = f"Análise Waterfall - {ano_inicial} S{semestre_inicial} vs {ano_final} S{semestre_final}"
    elif modo_comparacao == "Quarter":
        titulo_grafico = f"Análise Waterfall - {ano_inicial} Q{trimestre_inicial} vs {ano_final} Q{trimestre_final}"
    elif modo_comparacao == "Múltiplos Meses":
        titulo_grafico = f"Análise Waterfall - {len(meses_selecionados_2)} Meses Selecionados"
    else:
        titulo_grafico = f"Análise Waterfall - {mes_inicial_2} para {mes_final_2}"
    
    # Template e fundos transparentes para herdar cor do app
    if theme_base == "dark":
        fig_2.update_layout(template="plotly_dark")
    else:
        fig_2.update_layout(template="plotly_white")

    # Calcular range do eixo Y de forma otimizada (ajustar para reduzir tamanho da barra azul inicial)
    if values_2:
        # Encontrar máximo e mínimo em uma única passada, ignorando valores muito pequenos
        max_value = max((v for v in values_2 if abs(v) > 1e-9), default=0)
        min_value = min((v for v in values_2 if abs(v) > 1e-9), default=0)
        
        # Encontrar o valor da primeira barra azul (absolute) - que é o total_m1_all_2
        # Se não encontrar, usar o primeiro valor positivo
        first_absolute_value = None
        for i, (measure, value) in enumerate(zip(measures_2, values_2)):
            if measure == "absolute":
                first_absolute_value = value
                break
        
        # Se não encontrou absolute, usar o primeiro valor positivo ou o mínimo
        if first_absolute_value is None:
            first_absolute_value = max((v for v in values_2 if v > 0), default=min_value)
        
        # Calcular y_min como uma porcentagem do valor da primeira barra azul (30% como sugerido)
        # Garantir que não seja negativo se o valor inicial for positivo
        if first_absolute_value > 0:
            y_min = first_absolute_value * 0.30  # Começar em 30% da barra azul inicial
        else:
            # Se o valor inicial for negativo ou zero, manter comportamento original
            y_min = min(0, min_value * 1.1) if min_value < 0 else 0
        
        # Calcular o valor máximo considerando o valor acumulado final e as barras overlay
        # O valor final (total) já é o valor acumulado máximo do waterfall
        max_with_overlay = max_value
        
        # Encontrar o valor final (total) que é o valor acumulado máximo
        final_total_value = None
        for i, (measure, value) in enumerate(zip(measures_2, values_2)):
            if measure == "total":
                final_total_value = value
                max_with_overlay = max(max_with_overlay, value)
                break
        
        # Se não encontrou total, calcular o valor acumulado manualmente
        if final_total_value is None:
            cumulative = 0
            for measure, value in zip(measures_2, values_2):
                if measure == "absolute":
                    cumulative = value
                elif measure == "relative":
                    cumulative += value
                elif measure == "total":
                    cumulative = value
                max_with_overlay = max(max_with_overlay, cumulative)
        
        # Calcular o topo máximo considerando FLEX Volume e FLEX Inflação se existirem
        if tem_flex_volume or tem_flex_inflacao:
            # Calcular posição do topo das barras FLEX
            flex_top = total_m1_all_2
            if tem_flex_volume:
                flex_top += abs(flex_volume_2) if flex_volume_2 > 0 else 0
            if tem_flex_inflacao:
                flex_top += abs(flex_inflacao_2) if flex_inflacao_2 > 0 else 0
            max_with_overlay = max(max_with_overlay, flex_top)
        
        # Calcular também o valor máximo considerando todas as posições das anotações
        # As anotações podem estar acima das barras (yshift_val = 10)
        max_with_annotations = max_with_overlay
        cumulative_annot = 0
        for measure, value in zip(measures_2, values_2):
            if measure == "absolute":
                cumulative_annot = value
                # Anotação acima da barra
                max_with_annotations = max(max_with_annotations, value + abs(value) * 0.08)
            elif measure == "relative":
                if value >= 0:
                    # Anotação acima da barra
                    pos_annot = cumulative_annot + value
                    max_with_annotations = max(max_with_annotations, pos_annot + abs(value) * 0.08)
                cumulative_annot += value
            elif measure == "total":
                cumulative_annot = value
                # Anotação acima da barra
                max_with_annotations = max(max_with_annotations, value + abs(value) * 0.08)
        
        # Usar o maior valor entre max_with_overlay e max_with_annotations
        max_final = max(max_with_overlay, max_with_annotations)
        
        # Adicionar margem generosa de 20% no topo para garantir que nada seja cortado
        y_max = max_final * 1.20 if max_final > 0 else 1
    else:
        y_min = 0
        y_max = 1
    
    fig_2.update_layout(
        title={
            "text": titulo_grafico, 
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 14}
        },
        xaxis_title="Mês / Categoria",
        yaxis_title="Valor (R$)",
        height=560,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=100, r=50, t=60, b=50),  # Margem esquerda maior para números do eixo Y
        font=dict(color=text_color),
        xaxis=dict(
            showgrid=False,  # Não mostrar linhas de grade em todo o gráfico
            zeroline=False,  # Remover linha do zero
            showline=True,  # Mostrar linha do eixo X
            linecolor=grid_color,
            linewidth=1,  # Linha fina
            tickmode='linear',
            ticklen=5,  # Comprimento do tick
            tickcolor=grid_color,
            tickwidth=1,
            ticks="outside",  # Ticks fora do gráfico
            range=[-0.5, len(labels_2) - 0.5]  # Começar logo no início, sem espaço antes da primeira categoria
        ),
        yaxis=dict(
            showgrid=False,  # Não mostrar linhas de grade em todo o gráfico
            zeroline=False,  # Remover linha do zero
            showline=True,  # Mostrar linha do eixo Y
            linecolor=grid_color,
            linewidth=1,  # Linha fina
            tickmode='auto',  # Modo automático para evitar muitos ticks
            nticks=8,  # Limitar número de ticks para evitar sobreposição
            ticklen=5,  # Comprimento do tick
            tickcolor=grid_color,
            tickwidth=1,
            ticks="outside",  # Ticks fora do gráfico
            tickangle=0,  # Sem rotação
            range=[y_min, y_max],  # Começar logo no início, sem espaço extra
            tickformat=",.0f",
            tickprefix=f"{moeda_simbolo} ",
            ticksuffix=" K" if (fator_conversao == "K (milhares)") else (" M" if (fator_conversao == "M (Milhões)") else ""),
            automargin=True,  # Ajustar margem automaticamente para evitar sobreposição
            side="left"  # Garantir que os ticks fiquem do lado esquerdo
        ),
        annotations=annotations_custom if annotations_custom else []
    )

    st.plotly_chart(fig_2, use_container_width=True)

    # Exibir informações resumidas da análise
    st.markdown("---")
    # Reduzir tamanho da fonte das métricas
    st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 0.9em !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.85em !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.8em !important;
    }
    </style>
    """, unsafe_allow_html=True)
    if modo_comparacao == "Ano a Ano":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total Ano {ano_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric("FLEX Volume", formatar_valor_com_fator(flex_volume_2, moeda_simbolo),
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", formatar_valor_com_fator(flex_inflacao_2, moeda_simbolo),
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total Ano {ano_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            with col5_2:
                st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total Ano {ano_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric(f"Total Ano {ano_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Semestre":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total {ano_inicial} S{semestre_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric("FLEX Volume", formatar_valor_com_fator(flex_volume_2, moeda_simbolo),
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", formatar_valor_com_fator(flex_inflacao_2, moeda_simbolo),
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total {ano_final} S{semestre_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            with col5_2:
                st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total {ano_inicial} S{semestre_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric(f"Total {ano_final} S{semestre_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Quarter":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total {ano_inicial} Q{trimestre_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric("FLEX Volume", formatar_valor_com_fator(flex_volume_2, moeda_simbolo),
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", formatar_valor_com_fator(flex_inflacao_2, moeda_simbolo),
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total {ano_final} Q{trimestre_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            with col5_2:
                st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total {ano_inicial} Q{trimestre_inicial}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
            with col2_2:
                st.metric(f"Total {ano_final} Q{trimestre_final}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
            st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Múltiplos Meses":
        col1_2, col2_2 = st.columns(2)
        with col1_2:
            st.metric(f"Total {mes_inicial_2}", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
        with col2_2:
            st.metric(f"Total {mes_final_2}", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
        st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        # Mostrar totais dos meses intermediários
        if len(meses_selecionados_2) > 2:
            st.markdown("#### Meses Intermediários")
            cols_inter = st.columns(len(meses_selecionados_2[1:-1]))
            for idx, mes in enumerate(meses_selecionados_2[1:-1]):
                total_mes = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes)][col_valor].sum())
                with cols_inter[idx]:
                    st.metric(f"{mes}", formatar_valor_com_fator(total_mes, moeda_simbolo))
    else:  # Mês a Mês
        # Calcular Mês Flex = Total Mês Inicial + FLEX Volume
        mes_flex_2 = total_m1_all_2 + flex_volume_2
        
        col1_2, col2_2, col3_2, col4_2, col5_2, col6_2 = st.columns(6)
        with col1_2:
            st.metric("Total Mês Inicial", formatar_valor_com_fator(total_m1_all_2, moeda_simbolo))
        with col2_2:
            st.metric("FLEX Volume", formatar_valor_com_fator(flex_volume_2, moeda_simbolo),
                      help="Efeito de Volume + Sensibilidade")
        with col3_2:
            st.metric(f"{mes_inicial_2} Flex", formatar_valor_com_fator(mes_flex_2, moeda_simbolo),
                      help=f"{mes_inicial_2} + FLEX Volume")
        with col4_2:
            st.metric("FLEX Inflação", formatar_valor_com_fator(flex_inflacao_2, moeda_simbolo),
                      help="Efeito da Inflação")
        with col5_2:
            st.metric("Total Mês Final", formatar_valor_com_fator(total_m2_all_2, moeda_simbolo))
        with col6_2:
            st.metric("Variação Total", formatar_valor_com_fator(change_all_2, moeda_simbolo), delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        
        # Adicionar linha com volumes e % de variação
        st.markdown("")  # Espaço
        col_vol1, col_vol2, col_vol3 = st.columns(3)
        with col_vol1:
            st.metric("Volume Inicial", f"{volume_inicial_2:,.0f}")
        with col_vol2:
            st.metric("Volume Final", f"{volume_final_2:,.0f}")
        with col_vol3:
            variacao_volume_pct = ((volume_final_2 - volume_inicial_2) / volume_inicial_2 * 100) if volume_inicial_2 > 0 else 0.0
            variacao_volume_abs = volume_final_2 - volume_inicial_2 if volume_inicial_2 > 0 else 0
            st.metric("Variação Volume", f"{variacao_volume_abs:,.0f}", 
                     delta=f"{variacao_volume_pct:.2f}%")

st.markdown("---")
st.markdown("**📊 Dashboard TC - Análise Waterfall** | Desenvolvido com Streamlit")

