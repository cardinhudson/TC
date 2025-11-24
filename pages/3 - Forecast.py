import streamlit as st
import pandas as pd
import altair as alt
import os
import numpy as np
import re
import shutil
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Forecast - Visualização",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS para customização
st.markdown("""
    <style>
        /* Reduzir títulos em 20% */
        h1 {
            /* Reduzido de 3rem para 2.4rem (20%) */
            font-size: 2.4rem !important;
        }
        h2 {
            /* Reduzido de 2rem para 1.6rem (20%) */
            font-size: 1.6rem !important;
        }
        h3 {
            /* Reduzido de 1.6rem para 1.28rem (20%) */
            font-size: 1.28rem !important;
        }
    </style>
""", unsafe_allow_html=True)

# Título
st.title("📊 Forecast - Visualização")
st.subheader("Gráficos e tabelas gerados com os arquivos do Forecast")

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

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=0,  # "Todos" por padrão
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico"
)

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
def load_data(ano_selecionado_param, mtime_forecast=None):
    """Carrega os dados do arquivo parquet - SEMPRE da pasta Forecast"""
    try:
        # 🔧 CORREÇÃO CRÍTICA: SEMPRE carregar de forecast_completo.parquet (arquivo completo com previsão)
        caminho_forecast = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        
        if not os.path.exists(caminho_forecast):
            st.error(f"❌ Arquivo não encontrado: {caminho_forecast}")
            st.info("💡 Por favor, gere os arquivos de forecast na página '2 - Simulador Forecast'.")
            st.stop()
        
        # 🔧 CORREÇÃO: Usar tempo de modificação do arquivo para invalidar cache quando arquivo é atualizado
        mtime_atual = os.path.getmtime(caminho_forecast) if os.path.exists(caminho_forecast) else 0
        if mtime_forecast is not None and mtime_forecast != mtime_atual:
            # Arquivo foi atualizado, limpar cache desta função
            load_data.clear()
        
        # Carregar dados COMPLETOS (histórico + previsão)
        df = pd.read_parquet(caminho_forecast)
        
        # 🔧 CORREÇÃO: Aplicar filtro de ano APÓS carregar o arquivo completo
        # Isso garante que os dados sempre venham do mesmo arquivo, independente do ano selecionado
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
def load_volume_data(ano_selecionado_param, mtime_vol=None):
    """Carrega os dados de volume do arquivo parquet - SEMPRE da pasta Forecast"""
    try:
        # 🔧 CORREÇÃO CRÍTICA: SEMPRE carregar de df_vol_historico.parquet (arquivo completo)
        caminho_forecast_vol = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        
        if not os.path.exists(caminho_forecast_vol):
            st.warning(f"⚠️ Arquivo de volume não encontrado: {caminho_forecast_vol}")
            st.info("💡 Por favor, gere os arquivos de forecast na página '2 - Simulador Forecast'.")
            return None
        
        # 🔧 CORREÇÃO: Usar tempo de modificação do arquivo para invalidar cache quando arquivo é atualizado
        mtime_atual = os.path.getmtime(caminho_forecast_vol) if os.path.exists(caminho_forecast_vol) else 0
        if mtime_vol is not None and mtime_vol != mtime_atual:
            # Arquivo foi atualizado, limpar cache desta função
            load_volume_data.clear()
        
        # Carregar dados COMPLETOS (histórico + previsão)
        df = pd.read_parquet(caminho_forecast_vol)
        
        # 🔧 CORREÇÃO: Aplicar filtro de ano APÓS carregar o arquivo completo
        # Isso garante que os dados sempre venham do mesmo arquivo, independente do ano selecionado
        if ano_selecionado_param != "Todos" and "Ano" in df.columns:
            df = df[df['Ano'] == int(ano_selecionado_param)].copy()
        
        # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
        df = otimizar_tipos_dados(df)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Erro ao carregar dados de volume: {str(e)}")
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def load_volume_historico_data(mtime_vol=None):
    """Carrega os dados de volume histórico da pasta Forecast - SEMPRE do arquivo completo"""
    try:
        # 🔧 CORREÇÃO CRÍTICA: SEMPRE carregar de df_vol_historico.parquet (arquivo completo)
        caminho_forecast = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
        
        if not os.path.exists(caminho_forecast):
            # FALLBACK: Tentar histórico consolidado apenas se não existir na pasta Forecast
            caminho_historico = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
            if os.path.exists(caminho_historico):
                df = pd.read_parquet(caminho_historico)
                df = otimizar_tipos_dados(df)
                return df
            return None
        
        # 🔧 CORREÇÃO: Usar tempo de modificação do arquivo para invalidar cache quando arquivo é atualizado
        mtime_atual = os.path.getmtime(caminho_forecast) if os.path.exists(caminho_forecast) else 0
        if mtime_vol is not None and mtime_vol != mtime_atual:
            # Arquivo foi atualizado, limpar cache desta função
            load_volume_historico_data.clear()
        
        # Carregar dados COMPLETOS (histórico + previsão)
        df = pd.read_parquet(caminho_forecast)
        # 🔧 OTIMIZAÇÃO: Usar função cacheada para otimização de tipos
        df = otimizar_tipos_dados(df)
        return df
    except Exception:
        return None

# 🔧 CORREÇÃO: Verificar se os arquivos existem ANTES de qualquer verificação de configuração
# Se os arquivos existirem, carregar normalmente e limpar cache se necessário
caminho_forecast_check = os.path.join("dados", "Forecast", "forecast_completo.parquet")
caminho_vol_check = os.path.join("dados", "Forecast", "df_vol_historico.parquet")

arquivos_existem = os.path.exists(caminho_forecast_check) and os.path.exists(caminho_vol_check)

# Se os arquivos existem, verificar tempo de modificação e limpar cache se necessário
if arquivos_existem:
    mtime_forecast_atual = os.path.getmtime(caminho_forecast_check)
    mtime_vol_atual = os.path.getmtime(caminho_vol_check)
    
    # Verificar se os arquivos foram modificados desde a última vez (comparar com session_state)
    if 'mtime_forecast_anterior' in st.session_state:
        if st.session_state.mtime_forecast_anterior != mtime_forecast_atual:
            # Arquivo foi modificado, limpar cache
            load_data.clear()
            load_volume_data.clear()
            load_volume_historico_data.clear()
            # 🔧 CORREÇÃO: Limpar cache apenas se as funções já estiverem definidas
            try:
                aplicar_filtros.clear()
            except NameError:
                pass  # Função ainda não foi definida, será limpa depois
            try:
                get_filter_options.clear()
            except NameError:
                pass  # Função ainda não foi definida, será limpa depois
            st.session_state.mtime_forecast_anterior = mtime_forecast_atual
            st.session_state.mtime_vol_anterior = mtime_vol_atual
    else:
        # Primeira vez, salvar tempos de modificação
        st.session_state.mtime_forecast_anterior = mtime_forecast_atual
        st.session_state.mtime_vol_anterior = mtime_vol_atual
else:
    # Arquivos não existem, mostrar mensagem e parar
    st.warning("⚠️ Arquivos de forecast não encontrados.")
    st.info("ℹ️ Por favor, gere os arquivos de forecast na página **2 - Simulador Forecast**.")
    st.info(f"📁 Arquivos esperados:")
    st.info(f"   - {caminho_forecast_check}")
    st.info(f"   - {caminho_vol_check}")
    st.stop()

# Carregar dados (arquivos existem)
df_total = None
try:
    mtime_forecast_atual = os.path.getmtime(caminho_forecast_check) if os.path.exists(caminho_forecast_check) else 0
    df_total = load_data(ano_selecionado, mtime_forecast_atual)
except Exception as e:
    st.error(f"❌ Erro ao carregar dados: {str(e)}")
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

# Ordem dos meses para ordenação cronológica
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]

# Função para aplicar filtros com cache
@st.cache_data(ttl=3600, max_entries=50, show_spinner=False)
def aplicar_filtros(df_total_cache, oficina_selecionadas_cache, veiculo_selecionados_cache, 
                     usi_selecionada_cache, periodo_selecionado_cache):
    """Aplica filtros ao DataFrame com cache"""
    df_filtrado = df_total_cache.copy()
    
    # Filtro 1: Oficina
    if 'Oficina' in df_filtrado.columns:
        if oficina_selecionadas_cache and "Todos" not in oficina_selecionadas_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_cache)
            ].copy()
    
    # Filtro 2: Veículo
    if 'Veículo' in df_filtrado.columns:
        if veiculo_selecionados_cache and "Todos" not in veiculo_selecionados_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_cache)
            ].copy()
    
    # Filtro 3: USI
    if 'USI' in df_filtrado.columns:
        if usi_selecionada_cache and "Todos" not in usi_selecionada_cache:
            df_filtrado = df_filtrado[
                df_filtrado['USI'].astype(str).isin(usi_selecionada_cache)
            ].copy()
    
    # Filtro 4: Período
    if 'Período' in df_filtrado.columns:
        if periodo_selecionado_cache and "Todos" not in periodo_selecionado_cache:
            df_filtrado = df_filtrado[
                df_filtrado['Período'].astype(str).isin(periodo_selecionado_cache)
            ].copy()
    
    return df_filtrado

# 🔧 OTIMIZAÇÃO: Carregar opções de filtro apenas quando necessário
# Filtro 1: Oficina
oficina_selecionadas = ["Todos"]
if df_total is not None and 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=["Todos"]
    )

# Filtro 2: Veículo
veiculo_selecionados = ["Todos"]
if df_total is not None and 'Veículo' in df_total.columns:
    veiculo_opcoes = get_filter_options(df_total, 'Veículo')
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=["Todos"]
    )

# Filtro 3: USI
usi_selecionada = ["TC Ext"]
if df_total is not None and 'USI' in df_total.columns:
    usi_opcoes = get_filter_options(df_total, 'USI')
    default_usi = ["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"]
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi
    )

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
    
    periodo_selecionado = st.sidebar.multiselect(
        "Selecione o Período:", periodo_opcoes, default=["Todos"]
    )

# 🔧 CORREÇÃO CRÍTICA: Criar cópia ANTES do filtro de período para usar no gráfico (mesma lógica do TC_Ext linha 374)
# Isso garante que o gráfico mostre TODOS os períodos, mesmo quando um período específico está selecionado
df_para_grafico_periodo = None
if df_total is not None:
    df_para_grafico_periodo = aplicar_filtros(
        df_total,
        tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
        tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
        tuple(usi_selecionada) if usi_selecionada else tuple(),
        tuple()  # NÃO aplicar filtro de Período - queremos mostrar todos os períodos no gráfico
    )

# Aplicar todos os filtros com cache (incluindo Período) para df_filtrado
df_filtrado = None
if df_total is not None:
    df_filtrado = aplicar_filtros(
        df_total,
        tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
        tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
        tuple(usi_selecionada) if usi_selecionada else tuple(),
        tuple(periodo_selecionado) if periodo_selecionado else tuple()
    )

# Lista de meses do ano (necessária para a configuração)
meses_ano = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

# 🔧 CORREÇÃO: Verificar configurações apenas se necessário para funcionalidades específicas
# Se os arquivos existem, não bloquear o carregamento mesmo sem config_forecast_aplicada
# A configuração é necessária apenas para algumas funcionalidades de cálculo de médias
config_forecast_disponivel = (
    'config_forecast_aplicada' in st.session_state and 
    st.session_state.config_forecast_aplicada.get('ultimo_periodo_dados') is not None
)

# Usar configurações aplicadas (se existirem) ou temporárias
if config_forecast_disponivel and st.session_state.config_forecast_aplicada.get('ultimo_periodo_dados') is not None:
    # Usar configurações aplicadas
    ultimo_periodo_dados = st.session_state.config_forecast_aplicada['ultimo_periodo_dados']
    num_meses_prever = st.session_state.config_forecast_aplicada['num_meses_prever']
    num_meses_media_salvo = st.session_state.config_forecast_aplicada['num_meses_media']
    meses_excluir_media = st.session_state.config_forecast_aplicada['meses_excluir_media']
    periodos_restantes = st.session_state.config_forecast_aplicada['periodos_restantes']
    periodos_para_media = st.session_state.config_forecast_aplicada['periodos_para_media']
    ultimo_ano_dados = st.session_state.config_forecast_aplicada['ultimo_ano_dados']
    
    # 🔧 CORREÇÃO: Garantir que periodos_para_media sempre tenha o ano
    # Se os períodos não tiverem ano, adicionar o ano do último período
    if periodos_para_media:
        periodos_para_media_corrigidos = []
        ano_para_corrigir = ultimo_ano_dados
        if ' ' in str(ultimo_periodo_dados):
            partes = str(ultimo_periodo_dados).split(' ', 1)
            if len(partes) > 1 and partes[1].isdigit():
                ano_para_corrigir = int(partes[1])
        
        for periodo in periodos_para_media:
            periodo_str = str(periodo).strip()
            # Se não tiver ano (não tem espaço ou o que vem depois do espaço não é um número)
            if ' ' not in periodo_str or not periodo_str.split(' ', 1)[1].isdigit():
                # Adicionar o ano
                periodo_com_ano = f"{periodo_str} {ano_para_corrigir}"
                periodos_para_media_corrigidos.append(periodo_com_ano)
            else:
                periodos_para_media_corrigidos.append(periodo_str)
        
        periodos_para_media = periodos_para_media_corrigidos
    
    # Recalcular índices e meses baseados nas configurações aplicadas
    if ' ' in str(ultimo_periodo_dados):
        ultimo_mes_dados = str(ultimo_periodo_dados).split(' ', 1)[0]
    else:
        ultimo_mes_dados = str(ultimo_periodo_dados)
    ultimo_mes_dados = ultimo_mes_dados.capitalize()
    indice_ultimo_mes = meses_ano.index(ultimo_mes_dados) if ultimo_mes_dados in meses_ano else 0
    
    # 🔧 CORREÇÃO: Recalcular max_meses_media baseado no novo último período e ajustar num_meses_media
    # IMPORTANTE: Limitar ao ano do último período selecionado
    meses_historicos_disponiveis_aplicado = meses_ano[:indice_ultimo_mes + 1]
    
    # Extrair ano do último período primeiro
    ano_referencia_contagem_aplicado = None
    if ' ' in str(ultimo_periodo_dados):
        partes_periodo = str(ultimo_periodo_dados).split(' ', 1)
        if len(partes_periodo) > 1 and partes_periodo[1].isdigit():
            ano_referencia_contagem_aplicado = int(partes_periodo[1])
    if ano_referencia_contagem_aplicado is None:
        ano_referencia_contagem_aplicado = ano_maximo if 'ano_maximo' in locals() else datetime.now().year
    
    meses_com_valor_aplicado = len(meses_historicos_disponiveis_aplicado)  # Valor padrão
    if meses_historicos_disponiveis_aplicado and not df_filtrado.empty and 'Período' in df_filtrado.columns and 'Total' in df_filtrado.columns:
        # Criar lista de períodos até o último mês selecionado, APENAS do ano de referência
        periodos_ate_ultimo_aplicado = []
        for mes in meses_historicos_disponiveis_aplicado:
            periodo_com_ano = f"{mes} {ano_referencia_contagem_aplicado}".lower()
            periodos_ate_ultimo_aplicado.append(periodo_com_ano)
        
        # 🔧 CORREÇÃO CRÍTICA: Filtrar também por Ano se a coluna existir
        # Isso garante que apenas períodos do ano de referência sejam considerados
        df_filtrado_copy_aplicado = df_filtrado.copy()
        if 'Ano' in df_filtrado_copy_aplicado.columns:
            # Filtrar primeiro por ano
            df_filtrado_copy_aplicado = df_filtrado_copy_aplicado[df_filtrado_copy_aplicado['Ano'] == ano_referencia_contagem_aplicado].copy()
        
        # Filtrar df_filtrado para períodos até o último mês (já filtrado por ano)
        periodos_no_df_aplicado = df_filtrado_copy_aplicado['Período'].astype(str).str.strip().str.lower()
        mask_periodos_ate_ultimo_aplicado = periodos_no_df_aplicado.isin(periodos_ate_ultimo_aplicado)
        df_periodos_ate_ultimo_aplicado = df_filtrado_copy_aplicado[mask_periodos_ate_ultimo_aplicado].copy()
        
        # 🔧 CORREÇÃO CRÍTICA: Verificar se a coluna Ano existe e garantir que está filtrando corretamente
        # Se a coluna Ano existir, fazer uma verificação adicional para garantir que apenas períodos do ano correto sejam contados
        if 'Ano' in df_periodos_ate_ultimo_aplicado.columns:
            # Filtrar novamente por ano para garantir que apenas períodos do ano de referência sejam contados
            df_periodos_ate_ultimo_aplicado = df_periodos_ate_ultimo_aplicado[df_periodos_ate_ultimo_aplicado['Ano'] == ano_referencia_contagem_aplicado].copy()
        
        # Contar períodos únicos que têm pelo menos uma linha com Total != 0 (inclui valores negativos, exclui apenas zeros)
        if not df_periodos_ate_ultimo_aplicado.empty:
            # Normalizar períodos para comparação
            df_periodos_ate_ultimo_aplicado_copy = df_periodos_ate_ultimo_aplicado.copy()
            df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'] = df_periodos_ate_ultimo_aplicado_copy['Período'].astype(str).str.strip().str.lower()
            
            # Verificar se há pelo menos uma linha com Total != 0 para cada período
            # IMPORTANTE: Agrupar por Período_Normalizado E Ano (se existir) para garantir unicidade
            if 'Ano' in df_periodos_ate_ultimo_aplicado_copy.columns:
                # Agrupar por Período_Normalizado e Ano para garantir que estamos contando apenas períodos do ano correto
                periodos_unicos_df = df_periodos_ate_ultimo_aplicado_copy[['Período_Normalizado', 'Ano']].drop_duplicates()
                # Filtrar apenas os do ano de referência
                periodos_unicos_df = periodos_unicos_df[periodos_unicos_df['Ano'] == ano_referencia_contagem_aplicado]
                periodos_unicos_aplicado = periodos_unicos_df['Período_Normalizado'].unique()
            else:
                periodos_unicos_aplicado = df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'].unique()
            
            periodos_com_valor_lista_aplicado = []
            for periodo in periodos_unicos_aplicado:
                df_periodo = df_periodos_ate_ultimo_aplicado_copy[df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'] == periodo]
                # Verificar se há pelo menos uma linha com Total != 0
                if (df_periodo['Total'] != 0).any():
                    periodos_com_valor_lista_aplicado.append(periodo)
            meses_com_valor_aplicado = len(periodos_com_valor_lista_aplicado)
            
            # Se não encontrou nenhum período com valor, usar o número de meses históricos disponíveis
            if meses_com_valor_aplicado == 0:
                meses_com_valor_aplicado = len(meses_historicos_disponiveis_aplicado)
        else:
            # Se não encontrou nenhum período, usar o número de meses históricos disponíveis
            meses_com_valor_aplicado = len(meses_historicos_disponiveis_aplicado)
            
            # Se não encontrou nenhum período com valor, usar o número de meses históricos disponíveis
            if meses_com_valor_aplicado == 0:
                meses_com_valor_aplicado = len(meses_historicos_disponiveis_aplicado)
    
    # Ajustar num_meses_media se exceder o novo máximo
    max_meses_media_aplicado = max(1, meses_com_valor_aplicado)
    num_meses_media = min(num_meses_media_salvo, max_meses_media_aplicado)
else:
    # 🔧 CORREÇÃO: Se a configuração não estiver disponível, definir valores padrão
    # Isso permite que a visualização funcione mesmo sem config_forecast_aplicada
    ultimo_periodo_dados = None
    num_meses_prever = 0
    num_meses_media_salvo = 0
    num_meses_media = 0
    meses_excluir_media = []
    periodos_restantes = []
    periodos_para_media = []
    ultimo_ano_dados = None
    meses_historicos_disponiveis_aplicado = []
    meses_com_valor_aplicado = 0
    max_meses_media_aplicado = 0

# 🔧 CORREÇÃO: Garantir que df_visualizacao seja sempre definido
# Criar df_visualizacao baseado em df_filtrado (mesma lógica do bloco config_forecast_disponivel)
if 'df_visualizacao' not in locals() or 'df_visualizacao' not in globals():
    if df_filtrado is not None:
        df_visualizacao = df_filtrado.copy()
        coluna_visualizacao = 'Total' if 'Total' in df_filtrado.columns else 'Valor'
        if 'tipo_visualizacao' not in locals():
            tipo_visualizacao = "Custo Total"
    else:
        df_visualizacao = pd.DataFrame()
        coluna_visualizacao = 'Total'
        tipo_visualizacao = "Custo Total"

# 🔧 CORREÇÃO: Criar df_visualizacao ANTES de ser usado nas tabelas (se config_forecast_disponivel)
# Mesma lógica do "2 - Simular Forecast.py" (linhas 495-870)
if config_forecast_disponivel:
    # 🔧 MODO CPU: Preparar dados para visualização (mesma lógica do TC_Ext)
    if df_filtrado is not None and tipo_visualizacao == "CPU (Custo por Unidade)":
        # 🔧 OTIMIZAÇÃO: Usar função com cache em vez de carregar diretamente
        df_vol_calc = load_volume_historico_data()
        
        if df_vol_calc is not None and 'Volume' in df_vol_calc.columns:
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
            if ano_selecionado != "Todos" and 'Ano' in df_vol_calc.columns:
                df_vol_calc = df_vol_calc[
                    df_vol_calc['Ano'] == int(ano_selecionado)
                ].copy()
            
            # Normalizar Período no volume e df_filtrado (mesma lógica do modo CPU)
            if 'Período' in df_vol_calc.columns:
                df_vol_calc = df_vol_calc.copy()
                df_vol_calc['Período'] = df_vol_calc['Período'].astype(str)
                def normalizar_periodo_volume_cpu(periodo_str):
                    periodo_str = str(periodo_str).strip()
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        mes_nome = partes[0].strip().capitalize()
                        if len(partes) > 1 and partes[1].strip().isdigit():
                            ano_val = int(partes[1].strip())
                            return (mes_nome, ano_val)
                        return (mes_nome, None)
                    return (periodo_str.capitalize(), None)
                
                periodos_normalizados = df_vol_calc['Período'].apply(normalizar_periodo_volume_cpu)
                df_vol_calc['Período'] = periodos_normalizados.apply(lambda x: x[0])
                
                anos_extraidos = periodos_normalizados.apply(lambda x: x[1] if x[1] is not None else None)
                if anos_extraidos.notna().any():
                    if 'Ano' not in df_vol_calc.columns:
                        df_vol_calc['Ano'] = anos_extraidos
                    else:
                        mask_ano_extraido = anos_extraidos.notna()
                        df_vol_calc.loc[mask_ano_extraido, 'Ano'] = anos_extraidos[mask_ano_extraido].astype(int)
            
            if 'Período' in df_filtrado.columns:
                df_filtrado = df_filtrado.copy()
                df_filtrado['Período'] = df_filtrado['Período'].astype(str)
                def normalizar_periodo_forecast_cpu(periodo_str):
                    periodo_str = str(periodo_str).strip()
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        mes_nome = partes[0].strip().capitalize()
                        return mes_nome
                    return periodo_str.capitalize()
                
                df_filtrado['Período'] = df_filtrado['Período'].apply(normalizar_periodo_forecast_cpu)
            
            # Agrupar e calcular CPU
            if ('Oficina' in df_filtrado.columns and 'Período' in df_filtrado.columns):
                tem_veiculo = 'Veículo' in df_filtrado.columns
                tem_ano = 'Ano' in df_filtrado.columns
                
                # Filtrar df_vol_calc pelos mesmos filtros
                df_vol_calc_filtrado = df_vol_calc.copy()
                
                if tem_veiculo and 'Veículo' in df_vol_calc_filtrado.columns:
                    veiculos_filtrados = df_filtrado['Veículo'].dropna().unique()
                    if len(veiculos_filtrados) > 0:
                        df_vol_calc_filtrado = df_vol_calc_filtrado[
                            df_vol_calc_filtrado['Veículo'].isin(veiculos_filtrados)
                        ].copy()
                
                if 'Oficina' in df_filtrado.columns and 'Oficina' in df_vol_calc_filtrado.columns:
                    oficinas_filtradas = df_filtrado['Oficina'].dropna().unique()
                    if len(oficinas_filtradas) > 0:
                        df_vol_calc_filtrado = df_vol_calc_filtrado[
                            df_vol_calc_filtrado['Oficina'].isin(oficinas_filtradas)
                        ].copy()
                
                df_vol_calc = df_vol_calc_filtrado
                
                colunas_agrupamento = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento.append('Veículo')
                
                # Agrupar Total e Volume
                if 'Total' in df_filtrado.columns:
                    df_total_agrupado = df_filtrado.groupby(colunas_agrupamento, as_index=False)['Total'].sum()
                elif 'Valor' in df_filtrado.columns:
                    df_total_agrupado = df_filtrado.groupby(colunas_agrupamento, as_index=False)['Valor'].sum()
                    df_total_agrupado.rename(columns={'Valor': 'Total'}, inplace=True)
                else:
                    df_visualizacao = df_filtrado.copy()
                    coluna_visualizacao = 'Total' if 'Total' in df_filtrado.columns else 'Valor'
                    tipo_visualizacao = "Custo Total"
                    df_vol_calc = None
                
                if df_vol_calc is not None:
                    colunas_agrupamento_vol = ['Oficina', 'Período']
                    if tem_ano and 'Ano' in df_vol_calc.columns:
                        colunas_agrupamento_vol.append('Ano')
                    if 'Veículo' in df_vol_calc.columns:
                        colunas_agrupamento_vol.append('Veículo')
                    
                    df_vol_agrupado = df_vol_calc.groupby(colunas_agrupamento_vol, as_index=False)['Volume'].sum()
                    
                    df_cpu = pd.merge(df_total_agrupado, df_vol_agrupado, on=colunas_agrupamento, how='left')
                    df_cpu['Volume'] = df_cpu['Volume'].fillna(0.0)
                    
                    df_cpu['CPU'] = df_cpu.apply(
                        lambda row: (row['Total'] / row['Volume'] if pd.notnull(row['Volume']) and row['Volume'] != 0 else 0),
                        axis=1
                    )
                    
                    df_visualizacao = df_cpu.copy()
                    coluna_visualizacao = 'CPU'
                else:
                    df_visualizacao = df_filtrado.copy()
                    coluna_visualizacao = 'Total' if 'Total' in df_filtrado.columns else 'Valor'
                    tipo_visualizacao = "Custo Total"
            else:
                df_visualizacao = df_filtrado.copy()
                coluna_visualizacao = 'Total' if 'Total' in df_filtrado.columns else 'Valor'
                tipo_visualizacao = "Custo Total"
        else:
            df_visualizacao = df_filtrado.copy()
            coluna_visualizacao = 'Total' if 'Total' in df_filtrado.columns else 'Valor'
            tipo_visualizacao = "Custo Total"
    else:
        # Usar Total ou Valor diretamente
        if df_filtrado is not None:
            if 'Total' in df_filtrado.columns:
                df_visualizacao = df_filtrado.copy()
                coluna_visualizacao = 'Total'
            elif 'Valor' in df_filtrado.columns:
                df_visualizacao = df_filtrado.copy()
                coluna_visualizacao = 'Valor'
            else:
                df_visualizacao = df_filtrado.copy()
                coluna_visualizacao = 'Total'
        else:
            df_visualizacao = pd.DataFrame()
            coluna_visualizacao = 'Total'
    
    # ====================================================================
    # 📊 GRÁFICOS - EXIBIR QUANDO HÁ CONFIGURAÇÕES APLICADAS
    # ====================================================================
    
    # Função para ordenar por mês (mesma do TC_Ext) - MOVIDA PARA FORA DO BLOCO ELSE
    ORDEM_MESES_GRAFICO = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
                           'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    
    def ordenar_por_mes_forecast(df, coluna_periodo='Período'):
        """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
        df_copy = df.copy()
        
        # 🔧 CORREÇÃO: Extrair apenas o nome do mês do Período (pode conter ano)
        def extrair_mes(periodo_str):
            periodo_str = str(periodo_str).strip().lower()
            # Se contém espaço, pegar apenas a primeira parte (nome do mês)
            if ' ' in periodo_str:
                return periodo_str.split(' ', 1)[0]
            return periodo_str
        
        # Se houver coluna "Ano", sempre ordenar por ano e mês (mesmo que haja apenas um ano)
        # Isso garante que quando "Todos" está selecionado, todos os períodos sejam mostrados ordenados
        if 'Ano' in df_copy.columns:
            # Criar coluna de ordenação: ano primeiro, depois mês
            df_copy['_ordem_ano'] = df_copy['Ano']
            # 🔧 CORREÇÃO: Converter para numérico antes de fillna para evitar erro com Categorical
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].apply(extrair_mes).map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
            ).astype(float).fillna(999)
            df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
            df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
        else:
            # Ordenação simples por mês (comportamento original quando não há coluna Ano)
            # 🔧 CORREÇÃO: Converter para numérico antes de fillna para evitar erro com Categorical
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].apply(extrair_mes).map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
            ).astype(float).fillna(999)
            df_copy = df_copy.sort_values('_ordem_mes')
            df_copy = df_copy.drop(columns=['_ordem_mes'])
        
        return df_copy
    
    # ====================================================================
    # Funções de gráfico replicadas do TC_Ext
    # ====================================================================
    
    # Gráfico: Volume por Veículo (mesma lógica do TC_Ext)
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
                    sort='-y'
                ),
                y=alt.Y('Volume:Q', title='Volume (Unidades)'),
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
                fontSize=12
            ).encode(
                text=alt.Text('Volume:Q', format=',.0f')
            )
            
            return grafico_barras + rotulos
        except Exception as e:
            st.error(f"Erro ao criar gráfico de volume: {e}")
            return None
    
    # Gráfico: Volume por Período (mesma lógica do TC_Ext)
    @st.cache_data(ttl=900, max_entries=2)
    def create_volume_chart(df_data):
        """Cria gráfico de barras de Volume por Período"""
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
                chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                ordem_periodos = chart_data['Período_Completo'].tolist()
                
                # Usar Período_Completo no gráfico
                coluna_periodo_grafico = 'Período_Completo'
            else:
                # Comportamento original: agrupar apenas por Período (quando não há coluna Ano)
                chart_data = df_data.groupby('Período')['Volume'].sum().reset_index()
                chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                ordem_periodos = chart_data['Período'].tolist()
                coluna_periodo_grafico = 'Período'

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    f'{coluna_periodo_grafico}:N',
                    title='Período',
                    sort=ordem_periodos
                ),
                y=alt.Y('Volume:Q', title='Volume Total'),
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
                color='black',
                fontSize=12
            ).encode(
                text=alt.Text('Volume:Q', format=',.2f')
            )

            return grafico_barras + rotulos
        except Exception as e:
            st.error(f"Erro ao criar gráfico: {e}")
            return None
    
    try:
        # 🔧 CORREÇÃO CRÍTICA: Verificar se df_para_grafico_periodo está disponível
        if df_para_grafico_periodo is None or df_para_grafico_periodo.empty:
            st.warning("⚠️ Dados não disponíveis para criar gráficos. Por favor, verifique se os arquivos de forecast foram gerados.")
            st.stop()
        
        # 🔧 CORREÇÃO CRÍTICA: Usar df_para_grafico_periodo (dados ANTES do filtro de período) para o gráfico
        # Isso garante que TODOS os períodos sejam mostrados no gráfico, mesmo quando um período específico está selecionado
        # Mesma lógica do TC_Ext.py linha 1031-1033
        
        # 🔧 CORREÇÃO CRÍTICA: Usar df_para_grafico_periodo que foi criado ANTES do filtro de Período (mesma lógica do TC_Ext)
        # Isso garante que o gráfico use os mesmos dados que o modo CPU, mas sem o filtro de Período
        df_para_grafico_periodo_forecast = df_para_grafico_periodo.copy()
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar Período no df_para_grafico_periodo_forecast (mesma lógica do modo CPU linha 584-598)
        # Isso garante que o formato do Período seja consistente com o que o modo CPU espera
        if 'Período' in df_para_grafico_periodo_forecast.columns:
            df_para_grafico_periodo_forecast = df_para_grafico_periodo_forecast.copy()
            # Converter para string para evitar problemas com CategoricalIndex
            df_para_grafico_periodo_forecast['Período'] = df_para_grafico_periodo_forecast['Período'].astype(str)
            def normalizar_periodo_forecast_cpu(periodo_str):
                periodo_str = str(periodo_str).strip()
                # Se contém espaço, pegar apenas a primeira parte (nome do mês)
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    mes_nome = partes[0].strip().capitalize()
                    return mes_nome
                return periodo_str.capitalize()
            
            df_para_grafico_periodo_forecast['Período'] = df_para_grafico_periodo_forecast['Período'].apply(normalizar_periodo_forecast_cpu)
        
        # Usar df_para_grafico_periodo_forecast para o gráfico (sem filtro de Período)
        df_forecast_grafico = df_para_grafico_periodo_forecast.copy()
        
        # 🔧 CORREÇÃO: Para modo CPU, carregar e fazer merge com volume ANTES dos filtros
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # 🔧 OTIMIZAÇÃO: Usar função com cache em vez de carregar diretamente
            df_vol_grafico = load_volume_historico_data()
            
            # Fazer merge com volume se disponível
            if df_vol_grafico is not None and 'Volume' in df_vol_grafico.columns:
                # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
                # Isso garante que apenas o ano selecionado seja considerado
                if ano_selecionado != "Todos" and 'Ano' in df_vol_grafico.columns:
                    df_vol_grafico = df_vol_grafico[
                        df_vol_grafico['Ano'] == int(ano_selecionado)
                    ].copy()
                
                # 🔧 CORREÇÃO CRÍTICA: Normalizar Período no volume para corresponder ao formato do forecast
                # No forecast: Período = "Novembro", Ano = 2025 (separados)
                # No volume: pode estar como "Novembro 2025" ou "Novembro" com Ano separado
                if 'Período' in df_vol_grafico.columns:
                    df_vol_grafico = df_vol_grafico.copy()
                    # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
                    df_vol_grafico['Período'] = df_vol_grafico['Período'].astype(str)
                    # Extrair apenas o nome do mês do Período (remover ano se estiver incluído)
                    def normalizar_periodo_volume(periodo_str):
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
                    periodos_normalizados = df_vol_grafico['Período'].apply(normalizar_periodo_volume)
                    df_vol_grafico['Período'] = periodos_normalizados.apply(lambda x: x[0])
                    
                    # Extrair anos e atualizar coluna Ano
                    anos_extraidos = periodos_normalizados.apply(lambda x: x[1] if x[1] is not None else None)
                    if anos_extraidos.notna().any():
                        if 'Ano' not in df_vol_grafico.columns:
                            df_vol_grafico['Ano'] = anos_extraidos
                        else:
                            # Atualizar apenas onde o ano foi extraído do Período
                            mask_ano_extraido = anos_extraidos.notna()
                            df_vol_grafico.loc[mask_ano_extraido, 'Ano'] = anos_extraidos[mask_ano_extraido].astype(int)
                
                # 🔧 CORREÇÃO CRÍTICA: Seguir EXATAMENTE a mesma lógica do TC_Ext.py (linha 1043-1101)
                # 1. Filtrar volume pelos mesmos veículos e oficinas do forecast
                # 2. Agrupar Total por ['Oficina', 'Período', 'Ano', 'Veículo']
                # 3. Agrupar Volume por ['Oficina', 'Período', 'Ano', 'Veículo']
                # 4. Fazer merge entre os dois agrupados
                # 5. Calcular CPU
                # 6. Depois agrupar por ['Ano', 'Período'] e recalcular CPU (feito mais abaixo)
                
                # Aplicar mesmos filtros ao volume
                df_vol_calc_filtrado_grafico = df_vol_grafico.copy()
                
                # Filtrar por Veículo (mesma lógica do TC_Ext linha 1044-1049)
                if 'Veículo' in df_para_grafico_periodo_forecast.columns and 'Veículo' in df_vol_calc_filtrado_grafico.columns:
                    veiculos_filtrados = df_para_grafico_periodo_forecast['Veículo'].dropna().unique()
                    if len(veiculos_filtrados) > 0:
                        df_vol_calc_filtrado_grafico = df_vol_calc_filtrado_grafico[
                            df_vol_calc_filtrado_grafico['Veículo'].isin(veiculos_filtrados)
                        ].copy()
                
                # Filtrar por Oficina (mesma lógica do TC_Ext linha 1050-1055)
                if 'Oficina' in df_para_grafico_periodo_forecast.columns and 'Oficina' in df_vol_calc_filtrado_grafico.columns:
                    oficinas_filtradas = df_para_grafico_periodo_forecast['Oficina'].dropna().unique()
                    if len(oficinas_filtradas) > 0:
                        df_vol_calc_filtrado_grafico = df_vol_calc_filtrado_grafico[
                            df_vol_calc_filtrado_grafico['Oficina'].isin(oficinas_filtradas)
                        ].copy()
                
                # Agrupar Total por ['Oficina', 'Período', 'Ano', 'Veículo'] (mesma lógica do TC_Ext linha 1063-1071)
                tem_ano = 'Ano' in df_para_grafico_periodo_forecast.columns
                tem_veiculo = 'Veículo' in df_para_grafico_periodo_forecast.columns
                
                colunas_agrupamento_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_grafico.append('Veículo')
                
                if 'Total' in df_para_grafico_periodo_forecast.columns:
                    df_total_agrupado_grafico = df_para_grafico_periodo_forecast.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Total'].sum()
                else:
                    df_total_agrupado_grafico = df_para_grafico_periodo_forecast.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Valor'].sum()
                    df_total_agrupado_grafico.rename(columns={'Valor': 'Total'}, inplace=True)
                
                # Agrupar Volume por ['Oficina', 'Período', 'Ano', 'Veículo'] (mesma lógica do TC_Ext linha 1079-1081)
                colunas_agrupamento_vol_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_vol_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_vol_grafico.append('Veículo')
                
                df_vol_agrupado_grafico = df_vol_calc_filtrado_grafico.groupby(
                    colunas_agrupamento_vol_grafico, as_index=False
                )['Volume'].sum()
                
                # Fazer merge (mesma lógica do TC_Ext linha 1086-1101)
                if ano_selecionado == "Todos" and tem_ano:
                    df_cpu_grafico = pd.merge(
                        df_total_agrupado_grafico,
                        df_vol_agrupado_grafico,
                        on=colunas_agrupamento_grafico,
                        how='outer'
                    )
                    df_cpu_grafico['Total'] = df_cpu_grafico['Total'].fillna(0)
                    df_cpu_grafico['Volume'] = df_cpu_grafico['Volume'].fillna(0)
                else:
                    df_cpu_grafico = pd.merge(
                        df_total_agrupado_grafico,
                        df_vol_agrupado_grafico,
                        on=colunas_agrupamento_grafico,
                        how='left'
                    )
                
                # Calcular CPU (mesma lógica do TC_Ext linha 1103-1110)
                df_cpu_grafico['CPU'] = df_cpu_grafico.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                
                # Usar df_cpu_grafico para o gráfico (mesma lógica do TC_Ext linha 1112)
                df_forecast_grafico = df_cpu_grafico.copy()
        
        # 🔧 CORREÇÃO CRÍTICA: NÃO aplicar filtros novamente aqui
        # Os filtros já foram aplicados no df_para_grafico_periodo_forecast
        # e o df_cpu_grafico já foi criado a partir desses dados filtrados
        # Aplicar filtros novamente aqui causaria diferenças nos valores
        
        # 🔧 CORREÇÃO: Suportar modo CPU e Custo Total (mesma lógica do TC_Ext)
        # Para CPU, precisamos de Total e Volume para recalcular CPU após agrupamento
        # Para Custo Total, precisamos apenas de Total
        chart_data = None
        coluna_valor_grafico = None
        coluna_periodo_grafico = None
        ordem_periodos = None
        
        if tipo_visualizacao == "CPU (Custo por Unidade)":
                # 🔧 CORREÇÃO CRÍTICA: Seguir a mesma lógica do TC_Ext
                # Primeiro agrupar por Oficina, Período, Ano (se existir), Veículo (se existir)
                # Somar Total e Volume, fazer merge, calcular CPU
                # DEPOIS agrupar por Período (e Ano) para o gráfico
                if 'Total' in df_forecast_grafico.columns and 'Volume' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                    # Converter para numérico
                    df_forecast_grafico['Total'] = pd.to_numeric(df_forecast_grafico['Total'], errors='coerce').fillna(0.0)
                    df_forecast_grafico['Volume'] = pd.to_numeric(df_forecast_grafico['Volume'], errors='coerce').fillna(0.0)
                    
                    tem_ano = 'Ano' in df_forecast_grafico.columns
                    tem_veiculo = 'Veículo' in df_forecast_grafico.columns
                    tem_oficina = 'Oficina' in df_forecast_grafico.columns
                    
                    # Primeiro agrupamento: por Oficina, Período, Ano (se existir), Veículo (se existir)
                    colunas_agrupamento_inicial = []
                    if tem_oficina:
                        colunas_agrupamento_inicial.append('Oficina')
                    colunas_agrupamento_inicial.append('Período')
                    if tem_ano:
                        colunas_agrupamento_inicial.append('Ano')
                    if tem_veiculo:
                        colunas_agrupamento_inicial.append('Veículo')
                    
                    # 🔧 CORREÇÃO CRÍTICA: Seguir EXATAMENTE a mesma lógica do TC_Ext.py
                    # No TC_Ext.py (linha 1064-1110): agrupa Total e Volume por ['Oficina', 'Período', 'Ano', 'Veículo'],
                    # faz merge, calcula CPU uma vez, depois create_period_chart agrupa por ['Ano', 'Período'] e recalcula CPU
                    # Aqui vamos fazer o mesmo: agrupar por ['Ano', 'Período'] diretamente, somar Total e Volume, calcular CPU
                    # (o merge com volume já foi feito antes nas linhas 2125-2131)
                    
                    # Agrupar diretamente por Período (e Ano) para o gráfico
                    # Mesma lógica do create_period_chart do TC_Ext.py (linha 832-844)
                    colunas_agrupamento_final = ['Período']
                    if tem_ano:
                        colunas_agrupamento_final.append('Ano')
                    
                    chart_data = df_forecast_grafico.groupby(colunas_agrupamento_final, as_index=False).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    })
                    
                    # Calcular CPU (mesma lógica do create_period_chart do TC_Ext.py linha 837-843)
                    chart_data['CPU'] = chart_data.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    
                    coluna_valor_grafico = 'CPU'
                    
                    # 🔧 DEBUG: Verificar valores antes de calcular CPU
                    total_agregado = chart_data['Total'].sum()
                    volume_agregado = chart_data['Volume'].sum()
                    if volume_agregado == 0:
                        st.warning(f"⚠️ Volume agregado é zero após agrupamento. Total agregado: {total_agregado:,.2f}")
                    
                    if tem_ano:
                        # Criar Período_Completo
                        def criar_periodo_completo(periodo_str, ano_val):
                            periodo_str = str(periodo_str).strip()
                            ano_str = str(ano_val).strip()
                            if ' ' in periodo_str:
                                partes = periodo_str.split(' ', 1)
                                if len(partes) > 1 and partes[1].isdigit() and partes[1] == ano_str:
                                    return periodo_str
                            return f"{periodo_str} {ano_str}"
                        
                        chart_data['Período_Completo'] = chart_data.apply(
                            lambda row: criar_periodo_completo(row['Período'], row['Ano']), axis=1
                        )
                        chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                        ordem_periodos = chart_data['Período_Completo'].tolist()
                        coluna_periodo_grafico = 'Período_Completo'
                    else:
                        chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                        ordem_periodos = chart_data['Período'].tolist()
                        coluna_periodo_grafico = 'Período'
                else:
                    # Fallback: se não tiver Volume, usar Custo Total
                    if 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                        st.warning("⚠️ Volume não disponível para calcular CPU. Mostrando Custo Total.")
                        # chart_data será None, então continuará para o modo Custo Total abaixo
                    else:
                        st.warning("⚠️ Colunas 'Total', 'Volume' ou 'Período' não encontradas para calcular CPU no gráfico.")
        
        # Modo Custo Total (comportamento original ou fallback do CPU quando Volume não está disponível)
        elif tipo_visualizacao == "Custo Total" and 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                # Modo Custo Total (comportamento original)
                df_forecast_grafico['Total'] = pd.to_numeric(df_forecast_grafico['Total'], errors='coerce').fillna(0.0)
                tem_ano = 'Ano' in df_forecast_grafico.columns
                
                if tem_ano:
                    chart_data = df_forecast_grafico.groupby(['Ano', 'Período'], as_index=False)['Total'].sum()
                    coluna_valor_grafico = 'Total'
                    
                    def criar_periodo_completo(periodo_str, ano_val):
                        periodo_str = str(periodo_str).strip()
                        ano_str = str(ano_val).strip()
                        if ' ' in periodo_str:
                            partes = periodo_str.split(' ', 1)
                            if len(partes) > 1 and partes[1].isdigit() and partes[1] == ano_str:
                                return periodo_str
                        return f"{periodo_str} {ano_str}"
                    
                    chart_data['Período_Completo'] = chart_data.apply(
                        lambda row: criar_periodo_completo(row['Período'], row['Ano']), axis=1
                    )
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período_Completo'].tolist()
                    coluna_periodo_grafico = 'Período_Completo'
                else:
                    chart_data = df_forecast_grafico.groupby('Período', as_index=False)['Total'].sum()
                    coluna_valor_grafico = 'Total'
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período'].tolist()
                    coluna_periodo_grafico = 'Período'
        
        # Fallback: se chart_data ainda for None (modo CPU sem Volume ou erro)
        if chart_data is None and 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                # Fallback para Custo Total quando CPU não tem Volume
                df_forecast_grafico['Total'] = pd.to_numeric(df_forecast_grafico['Total'], errors='coerce').fillna(0.0)
                tem_ano = 'Ano' in df_forecast_grafico.columns
                
                if tem_ano:
                    chart_data = df_forecast_grafico.groupby(['Ano', 'Período'], as_index=False)['Total'].sum()
                    coluna_valor_grafico = 'Total'
                    
                    def criar_periodo_completo(periodo_str, ano_val):
                        periodo_str = str(periodo_str).strip()
                        ano_str = str(ano_val).strip()
                        if ' ' in periodo_str:
                            partes = periodo_str.split(' ', 1)
                            if len(partes) > 1 and partes[1].isdigit() and partes[1] == ano_str:
                                return periodo_str
                        return f"{periodo_str} {ano_str}"
                    
                    chart_data['Período_Completo'] = chart_data.apply(
                        lambda row: criar_periodo_completo(row['Período'], row['Ano']), axis=1
                    )
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período_Completo'].tolist()
                    coluna_periodo_grafico = 'Período_Completo'
                else:
                    chart_data = df_forecast_grafico.groupby('Período', as_index=False)['Total'].sum()
                    coluna_valor_grafico = 'Total'
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período'].tolist()
                    coluna_periodo_grafico = 'Período'
        
        # 🔧 CORREÇÃO: Filtrar valores nulos/zero antes de criar o gráfico
        if chart_data is not None and not chart_data.empty:
            # Remover linhas onde o valor é zero ou nulo
            if coluna_valor_grafico in chart_data.columns:
                # Filtrar valores diferentes de zero e não nulos
                chart_data = chart_data[
                    (chart_data[coluna_valor_grafico] != 0) & 
                    (chart_data[coluna_valor_grafico].notna())
                ].copy()
        
        # Criar gráfico (mesma lógica do TC_Ext) - suporta CPU e Custo Total
        if chart_data is not None and not chart_data.empty and coluna_valor_grafico and coluna_periodo_grafico:
            # Determinar título e label do eixo Y baseado no tipo de visualização
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                titulo_grafico = 'CPU (Custo por Unidade) por Período'
                titulo_eixo_y = 'CPU (R$)'
                formato_tooltip = ',.2f'
            else:
                titulo_grafico = 'Soma do Valor por Período'
                titulo_eixo_y = 'Soma do Valor (R$)'
                formato_tooltip = ',.2f'
            
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    f'{coluna_periodo_grafico}:N',
                    title='Período',
                    sort=ordem_periodos
                ),
                y=alt.Y(f'{coluna_valor_grafico}:Q', title=titulo_eixo_y),
                color=alt.Color(
                    f'{coluna_valor_grafico}:Q',
                    title=coluna_valor_grafico,
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                    alt.Tooltip(f'{coluna_valor_grafico}:Q', title=coluna_valor_grafico, format=formato_tooltip)
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
                color='white',
                fontSize=11
            ).encode(
                text=alt.Text(f'{coluna_valor_grafico}:Q', format=formato_tooltip)
            )
            
            grafico_final = grafico_barras + rotulos
            st.altair_chart(grafico_final, use_container_width=True)
            
            # Mostrar resumo
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                # Para CPU, calcular média ou total agregado
                if 'Total' in chart_data.columns and 'Volume' in chart_data.columns:
                    total_geral = chart_data['Total'].sum()
                    volume_geral = chart_data['Volume'].sum()
                    if volume_geral > 0:
                        cpu_geral = total_geral / volume_geral
                        st.info(f"📊 **CPU Geral:** R$ {cpu_geral:,.2f} (Total: R$ {total_geral:,.2f} / Volume: {volume_geral:,.0f})")
                    else:
                        st.info(f"📊 **CPU Geral:** R$ 0,00 (Volume zero)")
                else:
                    cpu_medio = chart_data['CPU'].mean()
                    st.info(f"📊 **CPU Médio:** R$ {cpu_medio:,.2f}")
            else:
                total_geral = chart_data[coluna_valor_grafico].sum()
                st.info(f"📊 **Total Geral:** R$ {total_geral:,.2f}")
        else:
            st.warning("⚠️ Colunas 'Total' ou 'Período' não encontradas no arquivo forecast.")
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Soma do Valor por Período': {str(e)}")
        import traceback
        st.error(f"Detalhes: {traceback.format_exc()}")
    
    # ====================================================================
    # 🔧 GRÁFICO 2: Volume por Período (mesma lógica do TC_Ext)
    # ====================================================================
    try:
        # IMPORTANTE: Usar a mesma lógica de filtragem em ambos os modos
        # para garantir que os volumes sejam consistentes
        # 🔧 OTIMIZAÇÃO: Usar função com cache em vez de carregar diretamente
        df_vol_grafico = load_volume_historico_data()
        
        if df_vol_grafico is not None:
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
            # Isso garante que apenas o ano selecionado seja considerado
            if ano_selecionado != "Todos" and 'Ano' in df_vol_grafico.columns:
                df_vol_grafico = df_vol_grafico[
                    df_vol_grafico['Ano'] == int(ano_selecionado)
                ].copy()
            
            # Normalizar Período no volume
            if 'Período' in df_vol_grafico.columns:
                df_vol_grafico = df_vol_grafico.copy()
                df_vol_grafico['Período'] = df_vol_grafico['Período'].astype(str)
                # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
                df_vol_grafico['Período'] = df_vol_grafico['Período'].astype(str)
                def normalizar_periodo_vol(periodo_str):
                    periodo_str = str(periodo_str).strip()
                    if ' ' in periodo_str:
                        return periodo_str.split(' ', 1)[0].strip().capitalize()
                    return periodo_str.capitalize()
                df_vol_grafico['Período'] = df_vol_grafico['Período'].apply(normalizar_periodo_vol)
            
            # 🔧 CORREÇÃO: Aplicar TODOS os filtros da sidebar ao df_vol (mesma lógica do TC_Ext)
            # Identificar colunas comuns entre df_filtrado e df_vol_grafico
            colunas_comuns = set(df_filtrado.columns) & set(df_vol_grafico.columns)
            # Remover colunas que não devem ser usadas para filtro
            # Manter Período para aplicar filtro de período também
            colunas_filtro = [
                col for col in colunas_comuns
                if col not in ['Volume', 'Total', 'Valor', 'CPU']
            ]
            
            # Aplicar filtros do df_filtrado ao df_vol usando colunas comuns
            df_vol_filtrado = df_vol_grafico.copy()
            
            for col in colunas_filtro:
                if col in df_filtrado.columns:
                    # Obter valores únicos da coluna no df_filtrado
                    valores_filtrados = df_filtrado[col].dropna().unique()
                    if len(valores_filtrados) > 0:
                        # Filtrar df_vol com os mesmos valores
                        df_vol_filtrado = df_vol_filtrado[
                            df_vol_filtrado[col].isin(valores_filtrados)
                        ].copy()
            
            # 🔧 CORREÇÃO: Aplicar filtro de Período diretamente da sidebar
            if 'Período' in df_vol_filtrado.columns:
                if periodo_selecionado and "Todos" not in periodo_selecionado:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Período'].astype(str).isin(periodo_selecionado)
                    ].copy()
            
            # Usar função create_volume_chart (mesma lógica do TC_Ext)
            if 'Volume' in df_vol_filtrado.columns and 'Período' in df_vol_filtrado.columns:
                st.subheader("📊 Volume Total por Período")
                grafico_volume = create_volume_chart(df_vol_filtrado)
                if grafico_volume:
                    st.altair_chart(grafico_volume, use_container_width=True)
                else:
                    st.info("Não foi possível criar o gráfico de volume.")
            else:
                st.warning(
                    "⚠️ O arquivo df_vol_historico.parquet não contém as colunas "
                    "'Período' e 'Volume' necessárias."
                )
        else:
            st.info(
                "ℹ️ Carregue o arquivo df_vol_historico.parquet para visualizar "
                "o gráfico de volume."
            )
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Volume por Período': {str(e)}")
    
    # ====================================================================
    # 🔧 GRÁFICO 3: Por Oficina (mesma lógica do TC_Ext)
    # ====================================================================
    try:
        caminho_forecast_grafico = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_forecast_grafico):
            df_forecast_oficina = pd.read_parquet(caminho_forecast_grafico)
            
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de aplicar outros filtros
            if ano_selecionado != "Todos" and 'Ano' in df_forecast_oficina.columns:
                df_forecast_oficina = df_forecast_oficina[
                    df_forecast_oficina['Ano'] == int(ano_selecionado)
                ].copy()
            
            # Aplicar TODOS os filtros da sidebar (Oficina, Veículo, USI, Período)
            if 'Oficina' in df_forecast_oficina.columns and oficina_selecionadas and "Todos" not in oficina_selecionadas:
                df_forecast_oficina = df_forecast_oficina[df_forecast_oficina['Oficina'].astype(str).isin(oficina_selecionadas)].copy()
            if 'Veículo' in df_forecast_oficina.columns and veiculo_selecionados and "Todos" not in veiculo_selecionados:
                df_forecast_oficina = df_forecast_oficina[df_forecast_oficina['Veículo'].astype(str).isin(veiculo_selecionados)].copy()
            if 'USI' in df_forecast_oficina.columns and usi_selecionada and "Todos" not in usi_selecionada:
                df_forecast_oficina = df_forecast_oficina[df_forecast_oficina['USI'].astype(str).isin(usi_selecionada)].copy()
            # 🔧 CORREÇÃO: Aplicar filtro de Período
            if 'Período' in df_forecast_oficina.columns:
                if periodo_selecionado and "Todos" not in periodo_selecionado:
                    df_forecast_oficina = df_forecast_oficina[
                        df_forecast_oficina['Período'].astype(str).isin(periodo_selecionado)
                    ].copy()
            
            if 'Oficina' in df_forecast_oficina.columns:
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.subheader("📊 CPU por Oficina")
                else:
                    st.subheader("📊 Soma do Valor por Oficina")
                
                # Preparar dados para gráfico por Oficina
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    # Carregar volume e fazer merge
                    caminho_vol_forecast = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
                    if os.path.exists(caminho_vol_forecast):
                        df_vol_oficina = pd.read_parquet(caminho_vol_forecast)
                        
                        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
                        # Isso garante que apenas o ano selecionado seja considerado
                        if ano_selecionado != "Todos" and 'Ano' in df_vol_oficina.columns:
                            df_vol_oficina = df_vol_oficina[
                                df_vol_oficina['Ano'] == int(ano_selecionado)
                            ].copy()
                        
                        # Normalizar e aplicar filtros no volume
                        if 'Período' in df_vol_oficina.columns:
                            df_vol_oficina = df_vol_oficina.copy()
                            df_vol_oficina['Período'] = df_vol_oficina['Período'].astype(str)
                            # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
                            df_vol_oficina['Período'] = df_vol_oficina['Período'].astype(str)
                            def normalizar_periodo_vol_oficina(periodo_str):
                                periodo_str = str(periodo_str).strip()
                                if ' ' in periodo_str:
                                    return periodo_str.split(' ', 1)[0].strip().capitalize()
                                return periodo_str.capitalize()
                            df_vol_oficina['Período'] = df_vol_oficina['Período'].apply(normalizar_periodo_vol_oficina)
                        
                        # 🔧 CORREÇÃO: Filtrar df_vol_oficina pelos mesmos veículos de df_forecast_oficina
                        # Isso garante que apenas veículos que tiveram consumo sejam considerados
                        if 'Veículo' in df_forecast_oficina.columns and 'Veículo' in df_vol_oficina.columns:
                            veiculos_filtrados = df_forecast_oficina['Veículo'].dropna().unique()
                            if len(veiculos_filtrados) > 0:
                                df_vol_oficina = df_vol_oficina[
                                    df_vol_oficina['Veículo'].isin(veiculos_filtrados)
                                ].copy()
                        
                        if 'Oficina' in df_vol_oficina.columns and oficina_selecionadas and "Todos" not in oficina_selecionadas:
                            df_vol_oficina = df_vol_oficina[df_vol_oficina['Oficina'].astype(str).isin(oficina_selecionadas)].copy()
                        
                        # Agrupar por Oficina, Período, Ano (se existir), Veículo (se existir)
                        # IMPORTANTE: Sempre incluir 'Ano' no agrupamento quando existir
                        colunas_agrupamento_oficina = ['Oficina', 'Período']
                        if 'Ano' in df_forecast_oficina.columns and 'Ano' in df_vol_oficina.columns:
                            colunas_agrupamento_oficina.append('Ano')
                        if 'Veículo' in df_forecast_oficina.columns and 'Veículo' in df_vol_oficina.columns:
                            colunas_agrupamento_oficina.append('Veículo')
                        
                        df_total_oficina = df_forecast_oficina.groupby(colunas_agrupamento_oficina, as_index=False)['Total'].sum()
                        df_vol_oficina_agrupado = df_vol_oficina.groupby(colunas_agrupamento_oficina, as_index=False)['Volume'].sum()
                        
                        df_cpu_oficina = pd.merge(df_total_oficina, df_vol_oficina_agrupado, on=colunas_agrupamento_oficina, how='left')
                        df_cpu_oficina['Volume'] = df_cpu_oficina['Volume'].fillna(0.0)
                        
                        # 🔧 CORREÇÃO: Agrupar por Período+Ano primeiro, depois por Oficina (mesma lógica do TC_Ext)
                        # Se houver múltiplos anos ou Período, agrupar primeiro por Período+Ano
                        if 'Período' in df_cpu_oficina.columns:
                            if 'Ano' in df_cpu_oficina.columns:
                                # Agrupar por Oficina, Período e Ano, somar Total e Volume
                                df_agrupado_periodo = df_cpu_oficina.groupby(['Oficina', 'Período', 'Ano']).agg({
                                    'Total': 'sum',
                                    'Volume': 'sum'
                                }).reset_index()
                            else:
                                # Agrupar por Oficina e Período, somar Total e Volume
                                df_agrupado_periodo = df_cpu_oficina.groupby(['Oficina', 'Período']).agg({
                                    'Total': 'sum',
                                    'Volume': 'sum'
                                }).reset_index()
                            
                            # Agora agrupar por Oficina, somar Total e Volume de todos os períodos
                            chart_data_oficina = df_agrupado_periodo.groupby('Oficina', as_index=False).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            })
                        else:
                            # Se não tiver Período, agrupar apenas por Oficina
                            chart_data_oficina = df_cpu_oficina.groupby('Oficina', as_index=False).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            })
                        
                        chart_data_oficina['CPU'] = chart_data_oficina.apply(
                            lambda row: (row['Total'] / row['Volume'] if pd.notnull(row['Volume']) and row['Volume'] != 0 else 0),
                            axis=1
                        )
                        chart_data_oficina = chart_data_oficina[['Oficina', 'CPU']].sort_values('CPU', ascending=False)
                        coluna_oficina = 'CPU'
                        titulo_y_oficina = "CPU (R$/Unidade)"
                        formato_oficina = ',.4f'
                    else:
                        chart_data_oficina = df_forecast_oficina.groupby('Oficina')['Total'].sum().reset_index().sort_values('Total', ascending=False)
                        coluna_oficina = 'Total'
                        titulo_y_oficina = "Soma do Valor (R$)"
                        formato_oficina = ',.2f'
                else:
                    chart_data_oficina = df_forecast_oficina.groupby('Oficina')['Total'].sum().reset_index().sort_values('Total', ascending=False)
                    coluna_oficina = 'Total'
                    titulo_y_oficina = "Soma do Valor (R$)"
                    formato_oficina = ',.2f'
                
                grafico_oficina = alt.Chart(chart_data_oficina).mark_bar().encode(
                    x=alt.X('Oficina:N', title='Oficina', sort='-y'),
                    y=alt.Y(f'{coluna_oficina}:Q', title=titulo_y_oficina),
                    color=alt.Color(f'{coluna_oficina}:Q', title=coluna_oficina, scale=alt.Scale(scheme='blues')),
                    tooltip=[
                        alt.Tooltip('Oficina:N', title='Oficina'),
                        alt.Tooltip(f'{coluna_oficina}:Q', title=coluna_oficina, format=formato_oficina)
                    ]
                ).properties(
                    title="CPU por Oficina" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Soma do Valor por Oficina",
                    height=400
                )
                
                rotulos_oficina = grafico_oficina.mark_text(
                    align='center', baseline='middle', dy=-10, color='black', fontSize=12
                ).encode(text=alt.Text(f'{coluna_oficina}:Q', format=formato_oficina))
                
                st.altair_chart(grafico_oficina + rotulos_oficina, use_container_width=True)
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Por Oficina': {str(e)}")
    
    # ====================================================================
    # 🔧 GRÁFICO 4: Por Veículo (mesma lógica do TC_Ext)
    # ====================================================================
    try:
        caminho_forecast_grafico = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_forecast_grafico):
            df_forecast_veiculo = pd.read_parquet(caminho_forecast_grafico)
            
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de aplicar outros filtros
            if ano_selecionado != "Todos" and 'Ano' in df_forecast_veiculo.columns:
                df_forecast_veiculo = df_forecast_veiculo[
                    df_forecast_veiculo['Ano'] == int(ano_selecionado)
                ].copy()
            
            # Aplicar TODOS os filtros da sidebar (Oficina, Veículo, USI, Período)
            if 'Oficina' in df_forecast_veiculo.columns and oficina_selecionadas and "Todos" not in oficina_selecionadas:
                df_forecast_veiculo = df_forecast_veiculo[df_forecast_veiculo['Oficina'].astype(str).isin(oficina_selecionadas)].copy()
            if 'Veículo' in df_forecast_veiculo.columns and veiculo_selecionados and "Todos" not in veiculo_selecionados:
                df_forecast_veiculo = df_forecast_veiculo[df_forecast_veiculo['Veículo'].astype(str).isin(veiculo_selecionados)].copy()
            if 'USI' in df_forecast_veiculo.columns and usi_selecionada and "Todos" not in usi_selecionada:
                df_forecast_veiculo = df_forecast_veiculo[df_forecast_veiculo['USI'].astype(str).isin(usi_selecionada)].copy()
            # 🔧 CORREÇÃO: Aplicar filtro de Período
            if 'Período' in df_forecast_veiculo.columns:
                if periodo_selecionado and "Todos" not in periodo_selecionado:
                    df_forecast_veiculo = df_forecast_veiculo[
                        df_forecast_veiculo['Período'].astype(str).isin(periodo_selecionado)
                    ].copy()
            
            if 'Veículo' in df_forecast_veiculo.columns:
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.subheader("📊 CPU por Veículo")
                else:
                    st.subheader("📊 Total por Veículo")
                
                # Preparar dados para gráfico por Veículo
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    # Carregar volume e fazer merge
                    caminho_vol_forecast = os.path.join("dados", "Forecast", "df_vol_historico.parquet")
                    if os.path.exists(caminho_vol_forecast):
                        df_vol_veiculo = pd.read_parquet(caminho_vol_forecast)
                        
                        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de ano ANTES de normalizar
                        # Isso garante que apenas o ano selecionado seja considerado
                        if ano_selecionado != "Todos" and 'Ano' in df_vol_veiculo.columns:
                            df_vol_veiculo = df_vol_veiculo[
                                df_vol_veiculo['Ano'] == int(ano_selecionado)
                            ].copy()
                        
                        # Normalizar e aplicar filtros no volume
                        if 'Período' in df_vol_veiculo.columns:
                            df_vol_veiculo = df_vol_veiculo.copy()
                            df_vol_veiculo['Período'] = df_vol_veiculo['Período'].astype(str)
                            # 🔧 CORREÇÃO: Converter para string para evitar problemas com CategoricalIndex ou MultiIndex
                            df_vol_veiculo['Período'] = df_vol_veiculo['Período'].astype(str)
                            def normalizar_periodo_vol_veiculo(periodo_str):
                                periodo_str = str(periodo_str).strip()
                                if ' ' in periodo_str:
                                    return periodo_str.split(' ', 1)[0].strip().capitalize()
                                return periodo_str.capitalize()
                            df_vol_veiculo['Período'] = df_vol_veiculo['Período'].apply(normalizar_periodo_vol_veiculo)
                        
                        # 🔧 CORREÇÃO: Filtrar df_vol_veiculo pelos mesmos veículos de df_forecast_veiculo
                        # Isso garante que apenas veículos que tiveram consumo sejam considerados
                        if 'Veículo' in df_forecast_veiculo.columns and 'Veículo' in df_vol_veiculo.columns:
                            veiculos_filtrados = df_forecast_veiculo['Veículo'].dropna().unique()
                            if len(veiculos_filtrados) > 0:
                                df_vol_veiculo = df_vol_veiculo[
                                    df_vol_veiculo['Veículo'].isin(veiculos_filtrados)
                                ].copy()
                        
                        if 'Oficina' in df_vol_veiculo.columns and oficina_selecionadas and "Todos" not in oficina_selecionadas:
                            df_vol_veiculo = df_vol_veiculo[df_vol_veiculo['Oficina'].astype(str).isin(oficina_selecionadas)].copy()
                        
                        # Agrupar por Veículo, Período, Ano (se existir)
                        # IMPORTANTE: Sempre incluir 'Ano' no agrupamento quando existir
                        colunas_agrupamento_veiculo = ['Veículo', 'Período']
                        if 'Ano' in df_forecast_veiculo.columns and 'Ano' in df_vol_veiculo.columns:
                            colunas_agrupamento_veiculo.append('Ano')
                        
                        df_total_veiculo = df_forecast_veiculo.groupby(colunas_agrupamento_veiculo, as_index=False)['Total'].sum()
                        df_vol_veiculo_agrupado = df_vol_veiculo.groupby(colunas_agrupamento_veiculo, as_index=False)['Volume'].sum()
                        
                        df_cpu_veiculo = pd.merge(df_total_veiculo, df_vol_veiculo_agrupado, on=colunas_agrupamento_veiculo, how='left')
                        df_cpu_veiculo['Volume'] = df_cpu_veiculo['Volume'].fillna(0.0)
                        
                        # 🔧 CORREÇÃO: Agrupar por Período+Ano primeiro, depois por Veículo (mesma lógica do TC_Ext)
                        # Se houver múltiplos anos ou Período, agrupar primeiro por Período+Ano
                        if 'Período' in df_cpu_veiculo.columns:
                            if 'Ano' in df_cpu_veiculo.columns:
                                # Agrupar por Veículo, Período e Ano, somar Total e Volume
                                df_agrupado_periodo = df_cpu_veiculo.groupby(['Veículo', 'Período', 'Ano']).agg({
                                    'Total': 'sum',
                                    'Volume': 'sum'
                                }).reset_index()
                            else:
                                # Agrupar por Veículo e Período, somar Total e Volume
                                df_agrupado_periodo = df_cpu_veiculo.groupby(['Veículo', 'Período']).agg({
                                    'Total': 'sum',
                                    'Volume': 'sum'
                                }).reset_index()
                            
                            # Agora agrupar por Veículo, somar Total e Volume de todos os períodos
                            chart_data_veiculo = df_agrupado_periodo.groupby('Veículo', as_index=False).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            })
                        else:
                            # Se não tiver Período, agrupar apenas por Veículo
                            chart_data_veiculo = df_cpu_veiculo.groupby('Veículo', as_index=False).agg({
                                'Total': 'sum',
                                'Volume': 'sum'
                            })
                        
                        chart_data_veiculo['CPU'] = chart_data_veiculo.apply(
                            lambda row: (row['Total'] / row['Volume'] if pd.notnull(row['Volume']) and row['Volume'] != 0 else 0),
                            axis=1
                        )
                        chart_data_veiculo = chart_data_veiculo[['Veículo', 'CPU']].sort_values('CPU', ascending=False)
                        coluna_veiculo = 'CPU'
                        titulo_y_veiculo = "CPU (R$/Unidade)"
                        formato_veiculo = ',.4f'
                    else:
                        chart_data_veiculo = df_forecast_veiculo.groupby('Veículo')['Total'].sum().reset_index().sort_values('Total', ascending=False)
                        coluna_veiculo = 'Total'
                        titulo_y_veiculo = "Total (R$)"
                        formato_veiculo = ',.2f'
                else:
                    chart_data_veiculo = df_forecast_veiculo.groupby('Veículo')['Total'].sum().reset_index().sort_values('Total', ascending=False)
                    coluna_veiculo = 'Total'
                    titulo_y_veiculo = "Total (R$)"
                    formato_veiculo = ',.2f'
                
                grafico_veiculo = alt.Chart(chart_data_veiculo).mark_bar().encode(
                    x=alt.X('Veículo:N', title='Veículo', sort='-y'),
                    y=alt.Y(f'{coluna_veiculo}:Q', title=titulo_y_veiculo),
                    color=alt.Color(f'{coluna_veiculo}:Q', title=coluna_veiculo, scale=alt.Scale(scheme='blues')),
                    tooltip=[
                        alt.Tooltip('Veículo:N', title='Veículo'),
                        alt.Tooltip(f'{coluna_veiculo}:Q', title=coluna_veiculo, format=formato_veiculo)
                    ]
                ).properties(
                    title="CPU por Veículo" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Total por Veículo",
                    height=400
                )
                
                rotulos_veiculo = grafico_veiculo.mark_text(
                    align='center', baseline='middle', dy=-10, color='black', fontSize=12
                ).encode(text=alt.Text(f'{coluna_veiculo}:Q', format=formato_veiculo))
                
                st.altair_chart(grafico_veiculo + rotulos_veiculo, use_container_width=True)
                
                # Gráfico de Volume por Veículo (mesma lógica do TC_Ext)
                # 🔧 CORREÇÃO: Usar df_visualizacao diretamente (mesma lógica do TC_Ext linha 2803-2807)
                # No TC_Ext, o Volume já foi adicionado ao df_visualizacao antes desta seção
                # No Forecast copy, também adicionamos Volume ao df_visualizacao na seção anterior
                if 'Volume' in df_visualizacao.columns and 'Veículo' in df_visualizacao.columns:
                    st.subheader("📊 Volume por Veículo")
                    grafico_vol_veiculo = create_volume_veiculo_chart(df_visualizacao)
                    if grafico_vol_veiculo is not None:
                        st.altair_chart(grafico_vol_veiculo, use_container_width=True)
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Por Veículo': {str(e)}")
    
    # ====================================================================
    # 🔧 TABELAS DETALHADAS (mesma lógica do TC_Ext)
    # ====================================================================
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
    
    # Bloco de Tabelas: Veículo, Oficina e Períodos + Total por Veículo
    st.markdown("---")
    
    # 🔧 CORREÇÃO: Preparar df_para_tabela_forecast ANTES do bloco das tabelas
    # Isso garante que esteja disponível para todas as tabelas (incluindo Tabela Dinâmica e Tabela Filtrada)
    # Usar os mesmos dados do gráfico "📊 Soma do Valor por Período (Dados do Forecast)"
    # Isso garante que a tabela mostre apenas os meses de previsão do arquivo forecast_completo.parquet
    # e que os dados sejam calculados da mesma forma
    df_para_tabela_forecast = None
    if 'df_para_grafico_periodo_forecast' in locals() or 'df_para_grafico_periodo_forecast' in globals():
        # Usar os mesmos dados do gráfico se já foram preparados
        try:
            df_para_tabela_forecast = df_para_grafico_periodo_forecast.copy()
        except NameError:
            df_para_tabela_forecast = None
    else:
        # Se não existir, criar da mesma forma que o gráfico
        # Usar df_para_grafico_periodo que foi criado ANTES do filtro de Período
        if 'df_para_grafico_periodo' in locals() or 'df_para_grafico_periodo' in globals():
            try:
                df_para_tabela_forecast = df_para_grafico_periodo.copy()
                # Normalizar Período da mesma forma que o gráfico
                if 'Período' in df_para_tabela_forecast.columns:
                    df_para_tabela_forecast = df_para_tabela_forecast.copy()
                    df_para_tabela_forecast['Período'] = df_para_tabela_forecast['Período'].astype(str)
                    def normalizar_periodo_forecast_cpu(periodo_str):
                        periodo_str = str(periodo_str).strip()
                        if ' ' in periodo_str:
                            partes = periodo_str.split(' ', 1)
                            mes_nome = partes[0].strip().capitalize()
                            return mes_nome
                        return periodo_str.capitalize()
                    df_para_tabela_forecast['Período'] = df_para_tabela_forecast['Período'].apply(normalizar_periodo_forecast_cpu)
            except NameError:
                df_para_tabela_forecast = None
    
    # Se ainda não foi definido, usar df_visualizacao como fallback
    if df_para_tabela_forecast is None:
        df_para_tabela_forecast = df_visualizacao.copy()
    
    # Expander para mostrar/ocultar todo o bloco de tabelas
    with st.expander("📊 **Tabelas Detalhadas**", expanded=False):
        
        # Tabela: Veículo, Oficina e Períodos (seguindo filtros da sidebar)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("📋 Tabela - CPU por Veículo, Oficina e Período")
        else:
            st.subheader("📋 Tabela - Custo Total por Veículo, Oficina e Período")
        
        if tem_veiculo and tem_oficina and tem_periodo and df_para_tabela_forecast is not None:
            # 🔧 CORREÇÃO: Usar df_para_tabela_forecast (mesmos dados do gráfico) em vez de df_visualizacao
            # Isso garante que a tabela mostre apenas os meses de previsão do forecast_completo.parquet
            df_tabela_fonte = df_para_tabela_forecast.copy()
            
            # Usar coluna_visualizacao que já está definida
            if coluna_visualizacao in df_tabela_fonte.columns:
                # Preparar dados para pivot (mesma lógica do gráfico)
                tem_multiplos_anos = 'Ano' in df_tabela_fonte.columns and df_tabela_fonte['Ano'].nunique() > 1
                
                if tem_multiplos_anos:
                    df_tabela_pivot = df_tabela_fonte.copy()
                    df_tabela_pivot['Período_Ano'] = (
                        df_tabela_pivot['Período'].astype(str) + ' ' + 
                        df_tabela_pivot['Ano'].astype(str)
                    )
                    coluna_periodo_pivot = 'Período_Ano'
                else:
                    df_tabela_pivot = df_tabela_fonte.copy()
                    coluna_periodo_pivot = 'Período'
                
                # 🔧 CORREÇÃO CRÍTICA: Para CPU, calcular ANTES de fazer pivot_table
                # Isso garante que usamos Total e Volume corretos (mesmos usados no forecast)
                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_tabela_pivot.columns and 'Volume' in df_tabela_pivot.columns:
                    # Agrupar por Oficina, Veículo e Período, somar Total e Volume
                    # Isso garante que a CPU seja calculada corretamente: CPU = Total_agregado / Volume_agregado
                    df_agrupado = df_tabela_pivot.groupby(['Oficina', 'Veículo', coluna_periodo_pivot]).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    
                    # Recalcular CPU: Total agregado / Volume agregado
                    df_agrupado['CPU'] = df_agrupado.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
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
                    # Para Custo Total, usar pivot_table diretamente
                    df_tabela = df_tabela_pivot.pivot_table(
                        index=['Oficina', 'Veículo'],
                        columns=coluna_periodo_pivot,
                        values=coluna_visualizacao,
                        aggfunc='sum',
                        fill_value=0
                    )
                
                # 🔧 CORREÇÃO: Ordenar meses cronologicamente (capitalizados)
                # ORDEM_MESES está em minúsculas, mas os períodos podem estar capitalizados
                ORDEM_MESES_CAPITALIZADOS = [mes.capitalize() for mes in ORDEM_MESES]
                
                if tem_multiplos_anos:
                    colunas_ordenadas = []
                    anos_unicos = sorted(df_tabela_pivot['Ano'].unique())
                    
                    for ano in anos_unicos:
                        # Tentar com minúsculas e capitalizadas
                        for mes in ORDEM_MESES:
                            # Tentar minúscula
                            coluna_combinada_min = f"{mes} {ano}"
                            # Tentar capitalizada
                            coluna_combinada_cap = f"{mes.capitalize()} {ano}"
                            
                            if coluna_combinada_min in df_tabela.columns:
                                colunas_ordenadas.append(coluna_combinada_min)
                            elif coluna_combinada_cap in df_tabela.columns:
                                colunas_ordenadas.append(coluna_combinada_cap)
                    
                    colunas_restantes = [
                        col for col in df_tabela.columns 
                        if col not in colunas_ordenadas
                    ]
                    colunas_periodos = colunas_ordenadas + colunas_restantes
                else:
                    # Tentar com minúsculas e capitalizadas
                    colunas_existentes = []
                    for mes in ORDEM_MESES:
                        if mes in df_tabela.columns:
                            colunas_existentes.append(mes)
                        elif mes.capitalize() in df_tabela.columns:
                            colunas_existentes.append(mes.capitalize())
                    
                    colunas_restantes = [
                        col for col in df_tabela.columns 
                        if col not in ORDEM_MESES and col not in ORDEM_MESES_CAPITALIZADOS
                    ]
                    colunas_periodos = colunas_existentes + colunas_restantes
                    
                    colunas_excluidas = {
                        'Ano', 'Período', 'Período_Ano', 'Veículo', 'Oficina', 
                        'Total', 'Valor', 'CPU', 'Volume', coluna_visualizacao,
                        'Dt.lçto.', 'Data Lançamento', 'Data de Lançamento',
                        'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
                    }
                    colunas_adicionais = [
                        col for col in df_tabela_fonte.columns 
                        if col not in colunas_excluidas
                    ]
                
                # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                for col in colunas_periodos:
                    if col not in df_tabela.columns:
                        df_tabela[col] = 0
                
                # Reordenar para usar exatamente as mesmas colunas
                df_tabela = df_tabela[colunas_periodos]
                
                # Calcular total por linha
                # Para CPU, recalcular a partir de Total e Volume agregados por Oficina e Veículo
                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_tabela_pivot.columns and 'Volume' in df_tabela_pivot.columns:
                    # Agrupar por Oficina e Veículo, somar Total e Volume, e recalcular CPU
                    df_total_oficina_veiculo = df_tabela_pivot.groupby(['Oficina', 'Veículo']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    df_total_oficina_veiculo['CPU'] = df_total_oficina_veiculo.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
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
                        if col in df_tabela_fonte.columns
                    ]
                    
                    if colunas_adicionais_validas:
                        # Agrupar por Oficina e Veículo e pegar o primeiro valor não nulo de cada coluna adicional
                        # Usar df_tabela_fonte (mesmos dados do gráfico) para ter todas as colunas
                        df_colunas_adicionais = df_tabela_fonte.groupby(['Oficina', 'Veículo'])[colunas_adicionais_validas].first().reset_index()
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
                
                # Formatar valores baseado no tipo de visualização
                def formatar_valor(val, tipo):
                    if isinstance(val, (int, float)):
                        if tipo == "CPU (Custo por Unidade)":
                            return f"{val:,.4f}"
                        else:
                            return f"R$ {val:,.2f}"
                    return val
                
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
                for col in colunas_formatar:
                    df_tabela_formatado[col] = df_tabela_formatado[col].apply(
                        lambda x: formatar_valor(x, tipo_visualizacao)
                    )
                
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
            
                # Botão de download da tabela
                if st.button(
                    "📥 Baixar Tabela por Veículo e Oficina (Excel)",
                    use_container_width=True,
                    key="download_tabela_veiculo_oficina_forecast"
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
                            
                            # Salvar na pasta Forecast (prioridade)
                            pasta_forecast = os.path.join("dados", "Forecast")
                            if not os.path.exists(pasta_forecast):
                                os.makedirs(pasta_forecast)
                            
                            tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                            file_name = f"Forecast_tabela_veiculo_oficina_{tipo_nome}.xlsx"
                            file_path_forecast = os.path.join(pasta_forecast, file_name)
                            
                            # Salvar arquivo na pasta Forecast
                            with pd.ExcelWriter(
                                file_path_forecast, engine='openpyxl'
                            ) as writer:
                                df_download.to_excel(
                                    writer, index=False, sheet_name='Veiculo_Oficina'
                                )
                            
                            # Também salvar na pasta Downloads para facilitar acesso
                            downloads_path = os.path.join(
                                os.path.expanduser("~"), "Downloads"
                            )
                            file_path_downloads = os.path.join(downloads_path, file_name)
                            
                            with pd.ExcelWriter(
                                file_path_downloads, engine='openpyxl'
                            ) as writer:
                                df_download.to_excel(
                                    writer, index=False, sheet_name='Veiculo_Oficina'
                                )
                            
                            st.success(
                                f"✅ Arquivo salvo com sucesso em: {file_path_forecast}"
                            )
                            st.info(
                                f"📁 Também salvo em Downloads: {file_path_downloads}"
                            )
                        except Exception as e:
                            st.error(f"❌ Erro ao salvar arquivo: {str(e)}")
            else:
                st.info(f"ℹ️ Coluna '{coluna_visualizacao}' não encontrada para criar a tabela.")
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
        
        # Usar container em vez de expander para evitar aninhamento
        st.markdown("---")
        with st.container():
            st.markdown(f"### {titulo_expander_total}")
            if tem_veiculo and tem_periodo:
                # 🔧 CORREÇÃO: Usar os mesmos dados do gráfico (df_para_tabela_forecast)
                df_tabela_total_fonte = df_para_tabela_forecast.copy() if df_para_tabela_forecast is not None else df_visualizacao.copy()
                
                # Preparar dados para pivot (mesma lógica do gráfico)
                tem_multiplos_anos_total = 'Ano' in df_tabela_total_fonte.columns and df_tabela_total_fonte['Ano'].nunique() > 1
                
                if tem_multiplos_anos_total:
                    df_tabela_total_pivot = df_tabela_total_fonte.copy()
                    df_tabela_total_pivot['Período_Ano'] = (
                        df_tabela_total_pivot['Período'].astype(str) + ' ' + 
                        df_tabela_total_pivot['Ano'].astype(str)
                    )
                    coluna_periodo_pivot_total = 'Período_Ano'
                else:
                    df_tabela_total_pivot = df_tabela_total_fonte.copy()
                    coluna_periodo_pivot_total = 'Período'
                
                # 🔧 CORREÇÃO: Ordenar meses cronologicamente
                df_tabela_total_ref = df_tabela_total_pivot.pivot_table(
                    index='Veículo',
                    columns=coluna_periodo_pivot_total,
                    values=coluna_visualizacao if coluna_visualizacao in df_tabela_total_pivot.columns else 'Total',
                    aggfunc='sum',
                    fill_value=0
                )
                
                # Ordenar colunas de períodos cronologicamente
                if tem_multiplos_anos_total:
                    colunas_ordenadas_total = []
                    anos_unicos_total = sorted(df_tabela_total_pivot['Ano'].unique())
                    
                    for ano in anos_unicos_total:
                        for mes in ORDEM_MESES:
                            coluna_combinada_min = f"{mes} {ano}"
                            coluna_combinada_cap = f"{mes.capitalize()} {ano}"
                            
                            if coluna_combinada_min in df_tabela_total_ref.columns:
                                colunas_ordenadas_total.append(coluna_combinada_min)
                            elif coluna_combinada_cap in df_tabela_total_ref.columns:
                                colunas_ordenadas_total.append(coluna_combinada_cap)
                    
                    colunas_restantes_total = [
                        col for col in df_tabela_total_ref.columns 
                        if col not in colunas_ordenadas_total
                    ]
                    colunas_periodos_total = colunas_ordenadas_total + colunas_restantes_total
                else:
                    colunas_existentes_total = []
                    for mes in ORDEM_MESES:
                        if mes in df_tabela_total_ref.columns:
                            colunas_existentes_total.append(mes)
                        elif mes.capitalize() in df_tabela_total_ref.columns:
                            colunas_existentes_total.append(mes.capitalize())
                    
                    colunas_restantes_total = [
                        col for col in df_tabela_total_ref.columns 
                        if col not in ORDEM_MESES and col not in [m.capitalize() for m in ORDEM_MESES]
                    ]
                    colunas_periodos_total = colunas_existentes_total + colunas_restantes_total
                
                # Inicializar variáveis para CPU
                df_tabela_total_valores = None
                df_tabela_total_volumes = None
                
                # Para CPU, usar a mesma lógica do gráfico: agrupar diretamente por Veículo e Período+Ano
                # Isso garante que apenas períodos com dados sejam considerados (evita problemas com volumes sem custos)
                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_tabela_total_pivot.columns and 'Volume' in df_tabela_total_pivot.columns:
                    # Verificar se há múltiplos anos (já determinado acima)
                    # tem_multiplos_anos_total já foi determinado acima
                    
                    # Agrupar por Veículo e Período+Ano, somar Total e Volume, calcular CPU
                    # Usar coluna_periodo_pivot_total que foi determinada acima
                    if tem_multiplos_anos_total:
                        # Agrupar por Veículo, Período e Ano
                        df_agrupado_periodo = df_tabela_total_pivot.groupby(['Veículo', 'Período', 'Ano']).agg({
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
                        df_agrupado_periodo = df_tabela_total_pivot.groupby(['Veículo', 'Período']).agg({
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
                    # Usar coluna_periodo_pivot_total que já foi determinada
                    df_tabela_total_valores = df_agrupado_periodo.pivot_table(
                        index='Veículo',
                        columns=coluna_periodo_pivot_total,
                        values='Total',
                        aggfunc='sum',
                        fill_value=0
                    )
                    
                    df_tabela_total_volumes = df_agrupado_periodo.pivot_table(
                        index='Veículo',
                        columns=coluna_periodo_pivot_total,
                        values='Volume',
                        aggfunc='sum',
                        fill_value=0
                    )
                    
                    # Dividir Total / Volume para obter CPU
                    df_tabela_total = df_tabela_total_valores / df_tabela_total_volumes.replace(0, np.nan)
                    df_tabela_total = df_tabela_total.fillna(0)
                    
                    # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                    for col in colunas_periodos_total:
                        if col not in df_tabela_total.columns:
                            df_tabela_total[col] = 0
                    
                    # Reordenar para usar exatamente as mesmas colunas (ordem cronológica)
                    df_tabela_total = df_tabela_total[colunas_periodos_total]
                    
                    # Calcular total por linha: usar EXATAMENTE a mesma lógica do gráfico "CPU por Veículo"
                    # Primeiro agrupar por Veículo e Período+Ano, depois por Veículo
                    if tem_multiplos_anos_total:
                        # Agrupar por Veículo, Período e Ano primeiro (mesma lógica do gráfico linha 2030)
                        df_agrupado_periodo_total = df_tabela_total_pivot.groupby(['Veículo', 'Período', 'Ano']).agg({
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
                        if 'Período' in df_tabela_total_pivot.columns:
                            df_agrupado_periodo_total = df_tabela_total_pivot.groupby(['Veículo', 'Período']).agg({
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
                            df_total_veiculo = df_tabela_total_pivot.groupby('Veículo').agg({
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
                    # Para Custo Total, usar soma normalmente (mesma lógica do gráfico)
                    df_tabela_total = df_tabela_total_pivot.pivot_table(
                        index='Veículo',
                        columns=coluna_periodo_pivot_total,
                        values=coluna_visualizacao if coluna_visualizacao in df_tabela_total_pivot.columns else 'Total',
                        aggfunc='sum',
                        fill_value=0
                    )
                    
                    # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                    for col in colunas_periodos_total:
                        if col not in df_tabela_total.columns:
                            df_tabela_total[col] = 0
                    
                    # Reordenar para usar exatamente as mesmas colunas (ordem cronológica)
                    df_tabela_total = df_tabela_total[colunas_periodos_total]
                    
                    # Calcular total por linha
                    df_tabela_total['Total'] = df_tabela_total.sum(axis=1)
                
                # Resetar índice se ainda estiver como índice
                if df_tabela_total.index.name == 'Veículo' or 'Veículo' not in df_tabela_total.columns:
                    df_tabela_total = df_tabela_total.reset_index()
                
                df_tabela_total = df_tabela_total.sort_values('Veículo')
                
                # Definir colunas adicionais (mesma lógica da primeira tabela)
                colunas_excluidas_total = {
                    'Ano', 'Período', 'Período_Ano', 'Veículo', 'Oficina', 
                    'Total', 'Valor', 'CPU', 'Volume', coluna_visualizacao,
                    'Dt.lçto.', 'Data Lançamento', 'Data de Lançamento',
                    'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
                }
                colunas_adicionais_total = [
                    col for col in df_tabela_total_fonte.columns 
                    if col not in colunas_excluidas_total
                ]
                
                # Adicionar colunas adicionais fazendo merge com o primeiro valor não nulo por Veículo
                if colunas_adicionais_total:
                    # Filtrar apenas colunas que realmente existem no DataFrame
                    colunas_adicionais_validas_total = [
                        col for col in colunas_adicionais_total 
                        if col in df_tabela_total_fonte.columns
                    ]
                    
                    if colunas_adicionais_validas_total:
                        # Agrupar por Veículo e pegar o primeiro valor não nulo de cada coluna adicional
                        # Usar df_tabela_total_fonte (mesmos dados do gráfico) para ter todas as colunas
                        df_colunas_adicionais = df_tabela_total_fonte.groupby('Veículo')[colunas_adicionais_validas_total].first().reset_index()
                        # Fazer merge com a tabela total
                        df_tabela_total = pd.merge(
                            df_tabela_total,
                            df_colunas_adicionais,
                            on='Veículo',
                            how='left'
                        )
                        # Reordenar colunas: Veículo, colunas adicionais (na ordem original), períodos, Total
                        # Manter a ordem original das colunas adicionais
                        colunas_adicionais_ordenadas_total = [
                            col for col in colunas_adicionais_total 
                            if col in colunas_adicionais_validas_total
                        ]
                        colunas_finais = ['Veículo'] + colunas_adicionais_ordenadas_total + colunas_periodos_total + ['Total']
                        # Manter apenas colunas que existem
                        colunas_finais = [col for col in colunas_finais if col in df_tabela_total.columns]
                        df_tabela_total = df_tabela_total[colunas_finais]
                
                # Formatar valores baseado no tipo de visualização
                def formatar_valor(val, tipo):
                    if isinstance(val, (int, float)):
                        if tipo == "CPU (Custo por Unidade)":
                            return f"{val:,.4f}"
                        else:
                            return f"R$ {val:,.2f}"
                    return val
                
                # Aplicar formatação apenas nas colunas numéricas (exceto Veículo e colunas adicionais)
                df_tabela_total_formatado = df_tabela_total.copy()
                # Obter colunas adicionais que foram realmente adicionadas à tabela
                colunas_adicionais_na_tabela = [
                    col for col in df_tabela_total_formatado.columns 
                    if col not in ['Veículo'] + colunas_periodos_total + ['Total']
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
                    key="download_tabela_total_veiculo_forecast"
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
                            
                            # Salvar na pasta Forecast (prioridade)
                            pasta_forecast = os.path.join("dados", "Forecast")
                            if not os.path.exists(pasta_forecast):
                                os.makedirs(pasta_forecast)
                            
                            tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                            file_name = f"Forecast_tabela_total_veiculo_{tipo_nome}.xlsx"
                            file_path_forecast = os.path.join(pasta_forecast, file_name)
                            
                            # Salvar arquivo na pasta Forecast
                            with pd.ExcelWriter(file_path_forecast, engine='openpyxl') as writer:
                                df_total_download.to_excel(
                                    writer, index=False, sheet_name='Total_Veiculo'
                                )
                            
                            # Também salvar na pasta Downloads para facilitar acesso
                            downloads_path = os.path.join(
                                os.path.expanduser("~"), "Downloads"
                            )
                            file_path_downloads = os.path.join(downloads_path, file_name)
                            
                            with pd.ExcelWriter(file_path_downloads, engine='openpyxl') as writer:
                                df_total_download.to_excel(
                                    writer, index=False, sheet_name='Total_Veiculo'
                                )
                            
                            st.success(f"✅ Arquivo salvo com sucesso em: {file_path_forecast}")
                            st.info(f"📁 Também salvo em Downloads: {file_path_downloads}")
                        except Exception as e:
                            st.error(f"❌ Erro ao salvar arquivo: {str(e)}")

# Tabela dinâmica: Valor por Oficina e Período
# 🔧 CORREÇÃO: Verificar se df_visualizacao foi definido antes de usar
if ('df_visualizacao' in locals() or 'df_visualizacao' in globals()) and df_visualizacao is not None and not df_visualizacao.empty:
    if ('Oficina' in df_visualizacao.columns and
            'Período' in df_visualizacao.columns):
        st.markdown("---")
        
        # 🔧 CORREÇÃO: Usar os mesmos dados do gráfico (df_para_tabela_forecast)
        # Verificar se df_para_tabela_forecast foi definido (pode não estar disponível se as tabelas não foram executadas)
        if 'df_para_tabela_forecast' in locals() or 'df_para_tabela_forecast' in globals():
            try:
                df_para_tabela_dinamica = df_para_tabela_forecast.copy() if df_para_tabela_forecast is not None else df_visualizacao.copy()
            except NameError:
                df_para_tabela_dinamica = df_visualizacao.copy()
        else:
            # Se não foi definido, usar df_visualizacao
            df_para_tabela_dinamica = df_visualizacao.copy()
    
    # Determinar título do expander
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        titulo_expander_dinamica = "📋 **Tabela Dinâmica - CPU por Oficina e Período**"
    else:
        titulo_expander_dinamica = "📋 **Tabela Dinâmica - Valor por Oficina e Período**"
    
    with st.expander(titulo_expander_dinamica, expanded=False):
        if coluna_visualizacao in df_para_tabela_dinamica.columns or (tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_para_tabela_dinamica.columns):
            # Verificar se há múltiplos anos e criar coluna combinada se necessário
            tem_multiplos_anos_dinamica = 'Ano' in df_para_tabela_dinamica.columns and df_para_tabela_dinamica['Ano'].nunique() > 1
            
            # 🔧 CORREÇÃO: Para modo CPU, calcular CPU antes de fazer pivot
            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_para_tabela_dinamica.columns and 'Volume' in df_para_tabela_dinamica.columns:
                # Agrupar por Oficina e Período+Ano, somar Total e Volume, calcular CPU
                if tem_multiplos_anos_dinamica:
                    df_agrupado_dinamica = df_para_tabela_dinamica.groupby(['Oficina', 'Período', 'Ano']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    df_agrupado_dinamica['Período_Ano'] = (
                        df_agrupado_dinamica['Período'].astype(str) + ' ' + 
                        df_agrupado_dinamica['Ano'].astype(str)
                    )
                    # Calcular CPU
                    df_agrupado_dinamica['CPU'] = df_agrupado_dinamica.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Verificar se a coluna CPU foi criada corretamente
                    if 'CPU' in df_agrupado_dinamica.columns:
                        # Criar tabela pivot com CPU
                        df_pivot = df_agrupado_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período_Ano',
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        # Fallback: usar Total se CPU não foi criado
                        df_pivot = df_agrupado_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período_Ano',
                            values='Total',
                            aggfunc='sum',
                            fill_value=0
                        )
                    coluna_periodo_dinamica = 'Período_Ano'
                else:
                    df_agrupado_dinamica = df_para_tabela_dinamica.groupby(['Oficina', 'Período']).agg({
                        'Total': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Calcular CPU
                    df_agrupado_dinamica['CPU'] = df_agrupado_dinamica.apply(
                        lambda row: (
                            row['Total'] / row['Volume']
                            if pd.notnull(row['Volume']) and row['Volume'] != 0
                            else 0
                        ),
                        axis=1
                    )
                    # Verificar se a coluna CPU foi criada corretamente
                    if 'CPU' in df_agrupado_dinamica.columns:
                        # Criar tabela pivot com CPU
                        df_pivot = df_agrupado_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período',
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        # Fallback: usar Total se CPU não foi criado
                        df_pivot = df_agrupado_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período',
                            values='Total',
                            aggfunc='sum',
                            fill_value=0
                        )
                    coluna_periodo_dinamica = 'Período'
            else:
                # Para Custo Total, usar soma normalmente
                # 🔧 CORREÇÃO: Verificar se coluna_visualizacao existe, caso contrário usar 'Total'
                coluna_valor_dinamica = coluna_visualizacao
                if coluna_valor_dinamica not in df_para_tabela_dinamica.columns:
                    if 'Total' in df_para_tabela_dinamica.columns:
                        coluna_valor_dinamica = 'Total'
                    elif 'Valor' in df_para_tabela_dinamica.columns:
                        coluna_valor_dinamica = 'Valor'
                    else:
                        st.warning("⚠️ Coluna de visualização não encontrada na tabela dinâmica")
                        coluna_valor_dinamica = None
                
                if coluna_valor_dinamica is not None:
                    if tem_multiplos_anos_dinamica:
                        # Criar coluna combinada Período + Ano para separar meses por ano
                        df_visualizacao_pivot_dinamica = df_para_tabela_dinamica.copy()
                        df_visualizacao_pivot_dinamica['Período_Ano'] = (
                            df_visualizacao_pivot_dinamica['Período'].astype(str) + ' ' + 
                            df_visualizacao_pivot_dinamica['Ano'].astype(str)
                        )
                        
                        # Criar tabela pivot
                        df_pivot = df_visualizacao_pivot_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período_Ano',
                            values=coluna_valor_dinamica,
                            aggfunc='sum',
                            fill_value=0
                        )
                        coluna_periodo_dinamica = 'Período_Ano'
                    else:
                        # Criar tabela pivot
                        df_pivot = df_para_tabela_dinamica.pivot_table(
                            index='Oficina',
                            columns='Período',
                            values=coluna_valor_dinamica,
                            aggfunc='sum',
                            fill_value=0
                        )
                        coluna_periodo_dinamica = 'Período'
                else:
                    # Se não houver coluna válida, criar DataFrame vazio
                    df_pivot = pd.DataFrame()
                    coluna_periodo_dinamica = 'Período'
            
            # 🔧 CORREÇÃO: Ordenar colunas por ordem cronológica dos meses (apenas se df_pivot não estiver vazio)
            if not df_pivot.empty:
                if tem_multiplos_anos_dinamica:
                    colunas_ordenadas_dinamica = []
                    anos_unicos_dinamica = sorted(df_para_tabela_dinamica['Ano'].unique())
                    
                    for ano in anos_unicos_dinamica:
                        for mes in ORDEM_MESES:
                            coluna_combinada_min = f"{mes} {ano}"
                            coluna_combinada_cap = f"{mes.capitalize()} {ano}"
                            
                            if coluna_combinada_min in df_pivot.columns:
                                colunas_ordenadas_dinamica.append(coluna_combinada_min)
                            elif coluna_combinada_cap in df_pivot.columns:
                                colunas_ordenadas_dinamica.append(coluna_combinada_cap)
                    
                    # Adicionar colunas que não são meses (ex: Total, outros períodos)
                    colunas_restantes_dinamica = [
                        col for col in df_pivot.columns 
                        if col not in colunas_ordenadas_dinamica
                    ]
                    df_pivot = df_pivot[colunas_ordenadas_dinamica + colunas_restantes_dinamica]
                else:
                    # Ordenar colunas por ordem cronológica dos meses (tentar minúsculas e capitalizadas)
                    colunas_existentes_dinamica = []
                    for mes in ORDEM_MESES:
                        if mes in df_pivot.columns:
                            colunas_existentes_dinamica.append(mes)
                        elif mes.capitalize() in df_pivot.columns:
                            colunas_existentes_dinamica.append(mes.capitalize())
                    
                    colunas_restantes_dinamica = [
                        col for col in df_pivot.columns 
                        if col not in ORDEM_MESES and col not in [m.capitalize() for m in ORDEM_MESES]
                    ]
                    df_pivot = df_pivot[colunas_existentes_dinamica + colunas_restantes_dinamica]

                # Calcular total por linha
                df_pivot['Total'] = df_pivot.sum(axis=1)
                df_pivot = df_pivot.sort_values('Total', ascending=False)
            else:
                # Se df_pivot estiver vazio, mostrar mensagem
                st.warning("⚠️ Não há dados disponíveis para criar a tabela dinâmica.")
                df_pivot = pd.DataFrame()

            # Formatar valores baseado no tipo de visualização (apenas se df_pivot não estiver vazio)
            if not df_pivot.empty:
                def formatar_valor(val, tipo):
                    if isinstance(val, (int, float)):
                        if tipo == "CPU (Custo por Unidade)":
                            return f"{val:,.4f}"
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
            else:
                st.info("ℹ️ Não há dados para exibir na tabela dinâmica.")

            # Botão de download da Tabela Dinâmica
            if st.button(
                "📥 Baixar Tabela Dinâmica (Excel)",
                use_container_width=True,
                key="download_pivot_forecast"
            ):
                with st.spinner("Gerando arquivo da tabela dinâmica..."):
                    try:
                        # Salvar na pasta Forecast (prioridade)
                        pasta_forecast = os.path.join("dados", "Forecast")
                        if not os.path.exists(pasta_forecast):
                            os.makedirs(pasta_forecast)
                        
                        file_name = "Forecast_tabela_dinamica.xlsx"
                        file_path_forecast = os.path.join(pasta_forecast, file_name)
                        
                        # Salvar arquivo na pasta Forecast
                        with pd.ExcelWriter(
                            file_path_forecast, engine='openpyxl'
                        ) as writer:
                            df_pivot.to_excel(
                                writer, index=True, sheet_name='Tabela_Dinamica'
                            )
                        
                        # Também salvar na pasta Downloads para facilitar acesso
                        downloads_path = os.path.join(
                            os.path.expanduser("~"), "Downloads"
                        )
                        file_path_downloads = os.path.join(downloads_path, file_name)
                        
                        with pd.ExcelWriter(
                            file_path_downloads, engine='openpyxl'
                        ) as writer:
                            df_pivot.to_excel(
                                writer, index=True, sheet_name='Tabela_Dinamica'
                            )

                        st.success(
                            f"✅ Arquivo salvo com sucesso em: {file_path_forecast}"
                        )
                        st.info(
                            f"📁 Também salvo em Downloads: {file_path_downloads}"
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
    # 🔧 CORREÇÃO: Usar os mesmos dados do gráfico (df_para_tabela_forecast)
    # Verificar se df_para_tabela_forecast foi definido (pode não estar disponível se as tabelas não foram executadas)
    if 'df_para_tabela_forecast' in locals() or 'df_para_tabela_forecast' in globals():
        try:
            df_para_tabela_filtrada = df_para_tabela_forecast.copy() if df_para_tabela_forecast is not None else df_visualizacao.copy()
        except NameError:
            df_para_tabela_filtrada = df_visualizacao.copy()
    else:
        # Se não foi definido, usar df_visualizacao
        df_para_tabela_filtrada = df_visualizacao.copy()
    
    # 🔧 CORREÇÃO: Ordenar por Período cronologicamente (se existir)
    if 'Período' in df_para_tabela_filtrada.columns:
        # Criar coluna de ordenação
        def extrair_mes_ordem(periodo_str):
            periodo_str = str(periodo_str).strip().lower()
            if ' ' in periodo_str:
                return periodo_str.split(' ', 1)[0]
            return periodo_str
        
        df_para_tabela_filtrada = df_para_tabela_filtrada.copy()
        # 🔧 CORREÇÃO: Converter para numérico antes de fillna para evitar erro com Categorical
        df_para_tabela_filtrada['_ordem_mes'] = df_para_tabela_filtrada['Período'].apply(extrair_mes_ordem).map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES)}
        ).astype(float).fillna(999)
        
        # Se houver coluna Ano, ordenar por ano e mês
        if 'Ano' in df_para_tabela_filtrada.columns:
            df_para_tabela_filtrada = df_para_tabela_filtrada.sort_values(['Ano', '_ordem_mes'])
        else:
            df_para_tabela_filtrada = df_para_tabela_filtrada.sort_values('_ordem_mes')
        
        # Remover coluna temporária
        df_para_tabela_filtrada = df_para_tabela_filtrada.drop(columns=['_ordem_mes'])
    
    # Usar TODAS as linhas (sem limite)
    df_display = df_para_tabela_filtrada.copy()

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
        key="download_filtered_forecast"
    ):
        with st.spinner("Gerando arquivo da tabela filtrada..."):
            try:
                # Salvar na pasta Forecast (prioridade)
                pasta_forecast = os.path.join("dados", "Forecast")
                if not os.path.exists(pasta_forecast):
                    os.makedirs(pasta_forecast)
                
                file_name = "Forecast_tabela_filtrada.xlsx"
                file_path_forecast = os.path.join(pasta_forecast, file_name)
                
                # Salvar arquivo na pasta Forecast
                with pd.ExcelWriter(file_path_forecast, engine='openpyxl') as writer:
                    df_para_tabela_filtrada.to_excel(
                        writer, index=False, sheet_name='Dados_Filtrados'
                    )
                
                # Também salvar na pasta Downloads para facilitar acesso
                downloads_path = os.path.join(
                    os.path.expanduser("~"), "Downloads"
                )
                file_path_downloads = os.path.join(downloads_path, file_name)
                
                with pd.ExcelWriter(file_path_downloads, engine='openpyxl') as writer:
                    df_visualizacao.to_excel(
                        writer, index=False, sheet_name='Dados_Filtrados'
                    )

                st.success(f"✅ Arquivo salvo com sucesso em: {file_path_forecast}")
                st.info(f"📁 Também salvo em Downloads: {file_path_downloads}")
            except Exception as e:
                st.error(f"❌ Erro ao salvar arquivo: {str(e)}")

st.markdown("---")

# Carregar dados de volume
df_vol = load_volume_data(ano_selecionado)

# 🔧 CORREÇÃO: Filtrar volumes pelas oficinas e veículos selecionados
if df_vol is not None and not df_vol.empty:
    # Filtrar por Oficina
    if 'Oficina' in df_vol.columns:
        if oficina_selecionadas and "Todos" not in oficina_selecionadas:
            df_vol = df_vol[
                df_vol['Oficina'].astype(str).isin(oficina_selecionadas)
            ].copy()
    # Filtrar por Veículo
    if 'Veículo' in df_vol.columns:
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_vol = df_vol[
                df_vol['Veículo'].astype(str).isin(veiculo_selecionados)
            ].copy()

# Carregar dados de volume histórico (prioritário para meses futuros)
df_vol_historico = load_volume_historico_data()

# 🔧 CORREÇÃO: Filtrar volumes históricos pelas oficinas e veículos selecionados
if df_vol_historico is not None and not df_vol_historico.empty:
    # Filtrar por Oficina
    if 'Oficina' in df_vol_historico.columns:
        if oficina_selecionadas and "Todos" not in oficina_selecionadas:
            df_vol_historico = df_vol_historico[
                df_vol_historico['Oficina'].astype(str).isin(oficina_selecionadas)
            ].copy()
    # Filtrar por Veículo
    if 'Veículo' in df_vol_historico.columns:
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_vol_historico = df_vol_historico[
                df_vol_historico['Veículo'].astype(str).isin(veiculo_selecionados)
            ].copy()

# Combinar os dados, priorizando o histórico
if df_vol_historico is not None and not df_vol_historico.empty:
    if df_vol is not None and not df_vol.empty:
        # Combinar: histórico tem prioridade, mas manter dados regulares que não estão no histórico
        # Primeiro, identificar períodos que estão no histórico
        if 'Período' in df_vol_historico.columns and 'Período' in df_vol.columns:
            periodos_historico = df_vol_historico['Período'].unique()
            # Filtrar df_vol para remover períodos que estão no histórico
            df_vol_filtrado = df_vol[~df_vol['Período'].isin(periodos_historico)]
            # Combinar: histórico primeiro, depois dados regulares restantes
            df_vol = pd.concat([df_vol_historico, df_vol_filtrado], ignore_index=True)
        else:
            # Se não tiver coluna Período, apenas usar histórico
            df_vol = df_vol_historico
    else:
        # Se não tiver df_vol, usar apenas histórico
        df_vol = df_vol_historico
    st.info("ℹ️ Dados de volume histórico carregados. Volumes futuros serão priorizados do arquivo histórico.")
elif df_vol is None:
    st.warning("⚠️ Arquivo df_vol.parquet não encontrado. Algumas funcionalidades podem não estar disponíveis.")
    df_vol = pd.DataFrame()

# 🔧 CORREÇÃO: Garantir que volumes finais estejam filtrados pelas oficinas e veículos selecionados
# (aplicar novamente após combinação para garantir que funcione em todos os casos)
if df_vol is not None and not df_vol.empty:
    # Filtrar por Oficina
    if 'Oficina' in df_vol.columns:
        if oficina_selecionadas and "Todos" not in oficina_selecionadas:
            df_vol = df_vol[
                df_vol['Oficina'].astype(str).isin(oficina_selecionadas)
            ].copy()
    # Filtrar por Veículo
    if 'Veículo' in df_vol.columns:
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_vol = df_vol[
                df_vol['Veículo'].astype(str).isin(veiculo_selecionados)
            ].copy()

# 🔧 CORREÇÃO CRÍTICA: Filtrar volume para manter apenas oficinas/veículos que têm valores (Total != 0)
# Isso garante que apenas oficinas com dados reais sejam consideradas no volume
if df_vol is not None and not df_vol.empty and 'df_filtrado' in locals() and df_filtrado is not None and not df_filtrado.empty:
    # Identificar quais oficinas e veículos têm valores (Total != 0) no df_filtrado
    if 'Total' in df_filtrado.columns:
        # Filtrar apenas linhas com Total != 0
        df_com_valores = df_filtrado[df_filtrado['Total'] != 0].copy()
        
        if not df_com_valores.empty:
            # Obter lista de oficinas que têm valores
            oficinas_com_valores = set()
            if 'Oficina' in df_com_valores.columns:
                oficinas_com_valores = set(df_com_valores['Oficina'].astype(str).unique())
            
            # Obter lista de veículos que têm valores
            veiculos_com_valores = set()
            if 'Veículo' in df_com_valores.columns:
                veiculos_com_valores = set(df_com_valores['Veículo'].astype(str).unique())
            
            # Filtrar df_vol para manter apenas oficinas e veículos que têm valores
            if 'Oficina' in df_vol.columns and oficinas_com_valores:
                df_vol = df_vol[
                    df_vol['Oficina'].astype(str).isin(oficinas_com_valores)
                ].copy()
            
            if 'Veículo' in df_vol.columns and veiculos_com_valores:
                df_vol = df_vol[
                    df_vol['Veículo'].astype(str).isin(veiculos_com_valores)
                ].copy()

# Verificar se temos as colunas necessárias (Custo é opcional)
colunas_necessarias = ['Oficina', 'Veículo', 'Período', 'Total']
colunas_faltando = [col for col in colunas_necessarias if col not in df_filtrado.columns]

if colunas_faltando:
    st.error(f"❌ Colunas necessárias não encontradas: {', '.join(colunas_faltando)}")
    st.info("ℹ️ Certifique-se de que o arquivo df_final.parquet contém todas as colunas necessárias.")
    st.stop()
else:
    # 🔧 CORREÇÃO: Criar coluna 'Custo' se não existir (opcional)
    if 'Custo' not in df_filtrado.columns:
        st.info("ℹ️ Coluna 'Custo' não encontrada. Criando coluna 'Custo' com valores padrão.")
        # Criar coluna Custo com valores padrão (pode ser baseado em Tipo_Custo se existir)
        if 'Tipo_Custo' in df_filtrado.columns:
            df_filtrado['Custo'] = df_filtrado['Tipo_Custo'].apply(
                lambda x: 'Fixo' if str(x).upper() in ['FIXO', 'FIX', 'FIXED'] else 'Variável'
            )
        else:
            # Se não tem Tipo_Custo, criar Custo com valor padrão 'Variável'
            df_filtrado['Custo'] = 'Variável'
    
    # Criar coluna Tipo_Custo se não existir (baseada na coluna Custo)
    if 'Tipo_Custo' not in df_filtrado.columns:
        # Função para identificar se é custo fixo ou variável
        def is_custo_fixo(valor_custo):
            """Identifica se o custo é fixo baseado no valor da coluna Custo"""
            if pd.isna(valor_custo):
                return False
            valor_str = str(valor_custo).strip().upper()
            # Considerar como fixo se contém palavras-chave
            palavras_fixo = ['FIXO', 'FIX', 'FIXED']
            return any(palavra in valor_str for palavra in palavras_fixo)
        
        # Criar coluna indicando se é fixo ou variável (baseada na coluna Custo)
        df_filtrado['Tipo_Custo'] = df_filtrado['Custo'].apply(is_custo_fixo)
        df_filtrado['Tipo_Custo'] = df_filtrado['Tipo_Custo'].map({True: 'Fixo', False: 'Variável'})
    else:
        # Se Tipo_Custo já existe, garantir que tenha apenas valores válidos
        valores_validos = df_filtrado['Tipo_Custo'].isin(['Fixo', 'Variável'])
        if not valores_validos.all():
            # Substituir valores inválidos por 'Variável'
            df_filtrado.loc[~valores_validos, 'Tipo_Custo'] = 'Variável'
            st.info("ℹ️ Alguns valores inválidos em 'Tipo_Custo' foram substituídos por 'Variável'.")
    
    # IMPORTANTE: A coluna 'Custo' deve ser preservada durante todo o processamento
    # Ela não deve ser removida em nenhum drop ou merge
    
    # Validação: verificar se há períodos para calcular a média
    # Se não houver períodos configurados, tentar usar períodos disponíveis nos dados
    # 🔧 CORREÇÃO: Só validar se a configuração estiver disponível
    if not config_forecast_disponivel:
        # Se não há configuração, pular validações e permitir visualização básica
        pass
    elif not periodos_para_media:
        # Tentar encontrar períodos disponíveis nos dados até o último mês selecionado
        if 'Período' in df_filtrado.columns:
            periodos_disponiveis_df = df_filtrado['Período'].dropna().unique()
            # Filtrar períodos até o último mês selecionado
            if ultimo_periodo_dados:
                # Extrair mês e ano do último período
                if ' ' in str(ultimo_periodo_dados):
                    ultimo_mes_nome = str(ultimo_periodo_dados).split(' ', 1)[0].lower()
                    ultimo_ano_num = int(str(ultimo_periodo_dados).split(' ', 1)[1]) if str(ultimo_periodo_dados).split(' ', 1)[1].isdigit() else None
                else:
                    ultimo_mes_nome = str(ultimo_periodo_dados).lower()
                    ultimo_ano_num = None
                
                # Tentar encontrar períodos que correspondem aos meses históricos disponíveis
                periodos_encontrados = []
                for periodo_df in periodos_disponiveis_df:
                    periodo_df_str = str(periodo_df).strip().lower()
                    periodo_df_mes = periodo_df_str.split(' ', 1)[0] if ' ' in periodo_df_str else periodo_df_str
                    periodo_df_ano = int(periodo_df_str.split(' ', 1)[1]) if ' ' in periodo_df_str and periodo_df_str.split(' ', 1)[1].isdigit() else None
                    
                    # Verificar se o período está antes ou no último mês selecionado
                    if periodo_df_mes in [m.lower() for m in meses_historicos_disponiveis]:
                        if ultimo_ano_num and periodo_df_ano:
                            if periodo_df_ano < ultimo_ano_num or (periodo_df_ano == ultimo_ano_num and periodo_df_mes <= ultimo_mes_nome):
                                periodos_encontrados.append(str(periodo_df))
                        elif not ultimo_ano_num or not periodo_df_ano:
                            periodos_encontrados.append(str(periodo_df))
                
                if periodos_encontrados:
                    # Pegar os últimos N períodos encontrados
                    periodos_para_media_temp = periodos_encontrados[-num_meses_media:] if len(periodos_encontrados) >= num_meses_media else periodos_encontrados
                    
                    # 🔧 CORREÇÃO: Garantir que todos os períodos tenham o ano
                    periodos_para_media = []
                    ano_para_corrigir = ultimo_ano_dados
                    if ' ' in str(ultimo_periodo_dados):
                        partes = str(ultimo_periodo_dados).split(' ', 1)
                        if len(partes) > 1 and partes[1].isdigit():
                            ano_para_corrigir = int(partes[1])
                    
                    for periodo in periodos_para_media_temp:
                        periodo_str = str(periodo).strip()
                        # Se não tiver ano (não tem espaço ou o que vem depois do espaço não é um número)
                        if ' ' not in periodo_str or not periodo_str.split(' ', 1)[1].isdigit():
                            # Adicionar o ano
                            periodo_com_ano = f"{periodo_str} {ano_para_corrigir}"
                            periodos_para_media.append(periodo_com_ano)
                        else:
                            periodos_para_media.append(periodo_str)
                    
                    st.warning(f"⚠️ **Aviso:** Não foram encontrados todos os {num_meses_media} períodos solicitados. Usando {len(periodos_para_media)} período(s) disponível(is): {', '.join(periodos_para_media)}")
                else:
                    st.error("❌ **Erro de Configuração:** Nenhum período disponível para calcular a média histórica.")
                    st.info("💡 Ajuste a configuração do forecast na sidebar:")
                    st.info("   - Selecione um mês histórico válido")
                    st.info("   - Ajuste os meses a excluir")
                    st.info("   - Verifique se há dados históricos disponíveis")
                    st.stop()
            else:
                st.error("❌ **Erro de Configuração:** Nenhum período disponível para calcular a média histórica.")
                st.info("💡 Ajuste a configuração do forecast na sidebar:")
                st.info("   - Selecione um mês histórico válido")
                st.info("   - Ajuste os meses a excluir")
                st.info("   - Verifique se há dados históricos disponíveis")
                st.stop()
        else:
            st.error("❌ **Erro de Configuração:** Nenhum período disponível para calcular a média histórica.")
            st.info("💡 Ajuste a configuração do forecast na sidebar:")
            st.info("   - Selecione um mês histórico válido")
            st.info("   - Ajuste os meses a excluir")
            st.info("   - Verifique se há dados históricos disponíveis")
            st.stop()
    
    # Validação: verificar se há períodos para prever
    # 🔧 CORREÇÃO: Só validar se a configuração estiver disponível
    if not config_forecast_disponivel:
        # Se não há configuração, pular validações e permitir visualização básica
        pass
    elif not periodos_restantes:
        st.error("❌ **Erro de Configuração:** Nenhum período selecionado para prever.")
        st.info("💡 Ajuste a configuração do forecast na sidebar:")
        st.info("   - Selecione o último mês com dados reais")
        st.info("   - Defina quantos meses prever")
        st.stop()
    
    # Função para calcular médias com cache
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
            
            # Se não encontrou correspondências, tentar encontrar períodos alternativos pelos meses
            # MAS APENAS se estiverem antes do último mês selecionado
            if df_filtrado_media.empty:
                # Tentar encontrar períodos disponíveis nos dados que correspondem aos meses solicitados
                periodos_disponiveis_df = df_filtrado_cache['Período'].dropna().unique()
                periodos_encontrados_alternativos = []
                
                # Extrair apenas os meses dos períodos procurados
                meses_procurados = []
                for periodo_procurado in periodos_para_media_cache:
                    periodo_str = str(periodo_procurado).strip().lower()
                    mes_procurado = periodo_str.split(' ', 1)[0] if ' ' in periodo_str else periodo_str
                    meses_procurados.append(mes_procurado)
                
                # Procurar períodos no DataFrame que correspondem aos meses procurados
                # MAS APENAS se estiverem antes ou no último mês selecionado
                for periodo_df in periodos_disponiveis_df:
                    periodo_df_str = str(periodo_df).strip().lower()
                    periodo_df_mes = periodo_df_str.split(' ', 1)[0] if ' ' in periodo_df_str else periodo_df_str
                    periodo_df_ano = int(periodo_df_str.split(' ', 1)[1]) if ' ' in periodo_df_str and periodo_df_str.split(' ', 1)[1].isdigit() else None
                    
                    # Verificar se o mês corresponde
                    if periodo_df_mes in meses_procurados:
                        # Verificar se está antes ou no último mês selecionado
                        if ultimo_mes_limite and ultimo_ano_limite:
                            if periodo_df_ano:
                                if periodo_df_ano > ultimo_ano_limite:
                                    continue
                                if periodo_df_ano == ultimo_ano_limite:
                                    # Comparar meses usando índice
                                    meses_ano_lower = [m.lower() for m in meses_ano]
                                    if periodo_df_mes in meses_ano_lower and ultimo_mes_limite in meses_ano_lower:
                                        idx_periodo = meses_ano_lower.index(periodo_df_mes)
                                        idx_limite = meses_ano_lower.index(ultimo_mes_limite)
                                        if idx_periodo > idx_limite:
                                            continue
                            elif not periodo_df_ano:
                                # Se não tem ano, verificar pelo mês
                                meses_ano_lower = [m.lower() for m in meses_ano]
                                if periodo_df_mes in meses_ano_lower and ultimo_mes_limite in meses_ano_lower:
                                    idx_periodo = meses_ano_lower.index(periodo_df_mes)
                                    idx_limite = meses_ano_lower.index(ultimo_mes_limite)
                                    if idx_periodo > idx_limite:
                                        continue
                        
                        periodos_encontrados_alternativos.append(str(periodo_df))
                
                # Se encontrou períodos alternativos, usar eles
                if periodos_encontrados_alternativos:
                    periodos_alternativos_normalizados = [p.strip().lower() for p in periodos_encontrados_alternativos]
                    df_filtrado_media = df_filtrado_cache[
                        periodos_no_df.isin(periodos_alternativos_normalizados)
                    ].copy()
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

    # Calcular médias mensais históricas por Oficina, Veículo e Período
    st.markdown("### 📊 Cálculo de Médias Mensais Históricas")
    
    st.markdown("---")

    # Verificar se as colunas Type 05, Type 06 e Account existem
    colunas_adicionais = []
    if 'Type 05' in df_filtrado.columns:
        colunas_adicionais.append('Type 05')
    if 'Type 06' in df_filtrado.columns:
        colunas_adicionais.append('Type 06')
    if 'Account' in df_filtrado.columns:
        colunas_adicionais.append('Account')

    # Calcular médias com cache (usando apenas os períodos selecionados)
    df_medias, df_media_mensal = calcular_medias_forecast(df_filtrado, colunas_adicionais, periodos_para_media, ultimo_periodo_dados)
    
    # Debug: exibir informações da função calcular_medias_forecast (armazenadas em session_state)
    if 'debug_calcular_medias' in st.session_state:
        debug_info = st.session_state['debug_calcular_medias']
        # 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance
        # st.sidebar.info(f"🔍 Debug calcular_medias_forecast:\n- Períodos procurados: {debug_info.get('periodos_procurados', [])}\n- Ano de referência: {debug_info.get('ano_referencia', None)}\n- Períodos encontrados: {debug_info.get('periodos_encontrados', [])}\n- Total de registros: {debug_info.get('total_registros', 0)}")
    
    # ====================================================================
    # 🔧 FUNÇÃO CENTRALIZADA: Calcular média histórica de forma padronizada
    # (Definida aqui para poder ser usada imediatamente)
    # ====================================================================
    def calcular_media_historica_padronizada(df_medias_fonte, periodos_para_media_fonte, filtro_oficina=None, df_forecast_fonte=None, meses_excluir_media_fonte=None):
        """
        Calcula média histórica de forma padronizada usando a mesma lógica do gráfico.
        Retorna: float com a média histórica ou None se não conseguir calcular
        """
        try:
            # OPÇÃO 2: Calcular agregando por período e tirando média (mesma lógica do gráfico)
            if df_medias_fonte is None or df_medias_fonte.empty:
                return None
            
            if 'Período' not in df_medias_fonte.columns or 'Total' not in df_medias_fonte.columns:
                return None
            
            df_temp = df_medias_fonte.copy()
            
            # Filtrar por oficina se especificado
            if filtro_oficina and 'Oficina' in df_temp.columns:
                df_temp = df_temp[df_temp['Oficina'] == filtro_oficina].copy()
                # 🔧 CORREÇÃO: Se após filtrar por oficina não há dados, retornar 0 imediatamente
                if df_temp.empty:
                    return 0.0
            
            # Normalizar Período para incluir ano ANTES do groupby
            ano_referencia = None
            if periodos_para_media_fonte:
                for p in periodos_para_media_fonte:
                    p_str = str(p).strip()
                    if ' ' in p_str:
                        ano_str = p_str.split(' ', 1)[1]
                        if ano_str.isdigit():
                            ano_referencia = int(ano_str)
                            break
            
            if ano_referencia and 'Período' in df_temp.columns:
                def normalizar_periodo_com_ano(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        if len(partes) > 1 and partes[1].isdigit():
                            return periodo_str
                    return f"{periodo_str} {ano_referencia}"
                
                df_temp['Período'] = df_temp['Período'].astype(str)
                df_temp['Período'] = df_temp['Período'].apply(normalizar_periodo_com_ano)
            
            # Filtrar períodos selecionados e excluir meses marcados
            if periodos_para_media_fonte:
                periodos_normalizados = [str(p).strip().lower() for p in periodos_para_media_fonte]
                meses_excluir_media_normalizados = []
                if meses_excluir_media_fonte:
                    for mes_excluir in meses_excluir_media_fonte:
                        mes_str = str(mes_excluir).strip().lower()
                        meses_excluir_media_normalizados.append(mes_str)
                
                def periodo_esta_selecionado(p):
                    p_str = str(p).strip().lower()
                    
                    if meses_excluir_media_normalizados:
                        periodo_mes = None
                        if ' ' in p_str:
                            periodo_mes = p_str.split(' ', 1)[0]
                        else:
                            periodo_mes = p_str
                        if periodo_mes in meses_excluir_media_normalizados:
                            return False
                    
                    if p_str in periodos_normalizados:
                        return True
                    if ' ' in p_str:
                        p_parts = p_str.split(' ', 1)
                        p_mes = p_parts[0]
                        p_ano = p_parts[1] if len(p_parts) > 1 else None
                        for periodo_ref in periodos_normalizados:
                            if ' ' in periodo_ref:
                                ref_parts = periodo_ref.split(' ', 1)
                                ref_mes = ref_parts[0]
                                ref_ano = ref_parts[1] if len(ref_parts) > 1 else None
                                if p_mes == ref_mes and p_ano and ref_ano and p_ano == ref_ano:
                                    return True
                    return False
                
                mask = df_temp['Período'].apply(periodo_esta_selecionado)
                df_temp = df_temp[mask].copy()
            
            if ano_referencia and 'Período' in df_temp.columns:
                def periodo_tem_ano_correto(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        ano_val = periodo_str.split(' ', 1)[1]
                        if ano_val.isdigit():
                            return int(ano_val) == ano_referencia
                    return False
                df_temp = df_temp[df_temp['Período'].apply(periodo_tem_ano_correto)].copy()
            
            if df_temp.empty:
                # 🔧 CORREÇÃO: Se não há dados após filtros, retornar 0 (não None)
                return 0.0
            
            if 'Ano' in df_temp.columns:
                df_agregado = df_temp.groupby(['Ano', 'Período'], as_index=False)['Total'].sum()
            else:
                df_agregado = df_temp.groupby('Período', as_index=False)['Total'].sum()
            
            if len(df_agregado) > 0:
                # Verificar se há pelo menos um valor não-zero
                total_soma = df_agregado['Total'].sum()
                if total_soma == 0 or pd.isna(total_soma):
                    # Se a soma é zero, retornar 0 (não None) para indicar que não há valores
                    return 0.0
                media = float(df_agregado['Total'].mean())
            else:
                # Se não há dados agregados, retornar 0 (não None)
                return 0.0
            
            return media
        except Exception:
            return None
    
    # ====================================================================
    # 🔧 FUNÇÃO CENTRALIZADA: Calcular média histórica de VOLUME de forma padronizada
    # (Similar à função de custo, mas para volume)
    # ====================================================================
    def calcular_media_historica_volume_padronizada(df_vol_fonte, periodos_para_media_fonte, meses_excluir_media_fonte=None):
        """
        Calcula média histórica de volume de forma padronizada usando a MESMA LÓGICA da função de custo.
        Retorna: float com a média histórica de volume ou None se não conseguir calcular
        
        LÓGICA IDÊNTICA À FUNÇÃO DE CUSTO (que está funcionando):
        1. Normalizar Período para incluir ano ANTES do groupby
        2. Filtrar períodos selecionados e excluir meses marcados
        3. Filtrar APENAS períodos do ano de referência
        4. Agregar volumes por período único (mês + ano)
        5. Calcular média dos volumes agregados
        """
        try:
            # OPÇÃO 2: Calcular agregando por período e tirando média (mesma lógica do gráfico e da função de custo)
            if df_vol_fonte is None or df_vol_fonte.empty:
                return None
            
            if 'Período' not in df_vol_fonte.columns or 'Volume' not in df_vol_fonte.columns:
                return None
            
            df_temp = df_vol_fonte.copy()
            
            # Normalizar Período para incluir ano ANTES do groupby (MESMA LÓGICA DA FUNÇÃO DE CUSTO)
            # 🔧 CORREÇÃO: Pegar o ANO MAIS RECENTE, não o primeiro encontrado
            ano_referencia = None
            anos_encontrados = []
            if periodos_para_media_fonte:
                for p in periodos_para_media_fonte:
                    p_str = str(p).strip()
                    if ' ' in p_str:
                        ano_str = p_str.split(' ', 1)[1]
                        if ano_str.isdigit():
                            anos_encontrados.append(int(ano_str))
            
            # Usar o ano mais recente (maior valor)
            if anos_encontrados:
                ano_referencia = max(anos_encontrados)
            
            if ano_referencia and 'Período' in df_temp.columns:
                def normalizar_periodo_com_ano_vol(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        partes = periodo_str.split(' ', 1)
                        if len(partes) > 1 and partes[1].isdigit():
                            return periodo_str
                    return f"{periodo_str} {ano_referencia}"
                
                df_temp['Período'] = df_temp['Período'].astype(str)
                df_temp['Período'] = df_temp['Período'].apply(normalizar_periodo_com_ano_vol)
            
            # Filtrar períodos selecionados e excluir meses marcados (MESMA LÓGICA DA FUNÇÃO DE CUSTO)
            if periodos_para_media_fonte:
                periodos_normalizados = [str(p).strip().lower() for p in periodos_para_media_fonte]
                meses_excluir_media_normalizados = []
                if meses_excluir_media_fonte:
                    for mes_excluir in meses_excluir_media_fonte:
                        mes_str = str(mes_excluir).strip().lower()
                        meses_excluir_media_normalizados.append(mes_str)
                
                def periodo_esta_selecionado_vol(p):
                    p_str = str(p).strip().lower()
                    
                    if meses_excluir_media_normalizados:
                        periodo_mes = None
                        if ' ' in p_str:
                            periodo_mes = p_str.split(' ', 1)[0]
                        else:
                            periodo_mes = p_str
                        if periodo_mes in meses_excluir_media_normalizados:
                            return False
                    
                    if p_str in periodos_normalizados:
                        return True
                    if ' ' in p_str:
                        p_parts = p_str.split(' ', 1)
                        p_mes = p_parts[0]
                        p_ano = p_parts[1] if len(p_parts) > 1 else None
                        for periodo_ref in periodos_normalizados:
                            if ' ' in periodo_ref:
                                ref_parts = periodo_ref.split(' ', 1)
                                ref_mes = ref_parts[0]
                                ref_ano = ref_parts[1] if len(ref_parts) > 1 else None
                                if p_mes == ref_mes and p_ano and ref_ano and p_ano == ref_ano:
                                    return True
                    return False
                
                mask = df_temp['Período'].apply(periodo_esta_selecionado_vol)
                df_temp = df_temp[mask].copy()
            
            # Filtrar APENAS períodos do ano de referência (MESMA LÓGICA DA FUNÇÃO DE CUSTO)
            # 🔧 CORREÇÃO: Filtrar sempre, mesmo quando não há coluna 'Ano' (se o Período incluir o ano)
            tem_coluna_ano = 'Ano' in df_temp.columns
            if ano_referencia and 'Período' in df_temp.columns:
                def periodo_tem_ano_correto_vol(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        ano_val = periodo_str.split(' ', 1)[1]
                        if ano_val.isdigit():
                            return int(ano_val) == ano_referencia
                    # Se o período não tem ano, manter apenas se não houver coluna 'Ano' (caso contrário será filtrado pela coluna Ano)
                    return not tem_coluna_ano
                df_temp = df_temp[df_temp['Período'].apply(periodo_tem_ano_correto_vol)].copy()
            
            # 🔧 CORREÇÃO ADICIONAL: Se há coluna 'Ano', também filtrar por ano mais recente
            if 'Ano' in df_temp.columns and not df_temp.empty:
                # Filtrar apenas o ano mais recente (mesma lógica do gráfico de custos)
                anos_unicos = df_temp['Ano'].dropna().unique()
                if len(anos_unicos) > 1:
                    ano_mais_recente = df_temp['Ano'].max()
                    df_temp = df_temp[df_temp['Ano'] == ano_mais_recente].copy()
            
            if df_temp.empty:
                return None
            
            # Agregar volumes por período (MESMA LÓGICA DA FUNÇÃO DE CUSTO)
            # 🔧 CORREÇÃO: Normalizar Período ANTES de agrupar para garantir consistência
            if 'Ano' in df_temp.columns:
                # Normalizar Período antes de agrupar (garante que períodos com mesmo mês+ano sejam agrupados juntos)
                def normalizar_periodo_vol_func(periodo_str, ano_val):
                    periodo_str = str(periodo_str).strip()
                    ano_str = str(ano_val).strip()
                    # Se o período já contém o ano, retornar como está
                    if ano_str in periodo_str:
                        return periodo_str
                    # Caso contrário, adicionar o ano
                    return periodo_str + ' ' + ano_str
                
                # Normalizar Período antes de agrupar
                df_temp['Período_Normalizado'] = df_temp.apply(
                    lambda row: normalizar_periodo_vol_func(row['Período'], row['Ano']), axis=1
                )
                
                # Agrupar por Ano e Período_Normalizado
                df_agregado = df_temp.groupby(['Ano', 'Período_Normalizado'], as_index=False)['Volume'].sum()
                # Renomear Período_Normalizado de volta para Período
                df_agregado = df_agregado.rename(columns={'Período_Normalizado': 'Período'})
                # Remover coluna Ano (já está incluída no Período)
                df_agregado = df_agregado.drop(columns=['Ano'])
            else:
                df_agregado = df_temp.groupby('Período', as_index=False)['Volume'].sum()
            
            if len(df_agregado) > 0:
                # Calcular média dos volumes totais por período (MESMA LÓGICA DA FUNÇÃO DE CUSTO)
                media_volume = float(df_agregado['Volume'].mean())
            else:
                media_volume = None
            
            return media_volume
        except Exception:
            return None
    
    # 🔧 CORREÇÃO CRÍTICA: Calcular média histórica total padronizada e ajustar médias por linha
    # Isso garante que a soma das médias por linha seja igual à média histórica total do gráfico
    # E que todos os cálculos (forecast, gráficos, tabelas) usem a mesma média padronizada
    media_historica_total_padronizada = calcular_media_historica_padronizada(
        df_medias, periodos_para_media, filtro_oficina=None, 
        df_forecast_fonte=None, meses_excluir_media_fonte=meses_excluir_media
    )
    
    # Se conseguimos calcular a média padronizada, ajustar as médias por linha
    if media_historica_total_padronizada is not None and media_historica_total_padronizada > 0:
        # Calcular soma atual das médias por linha
        soma_medias_linhas = float(df_media_mensal['Total'].sum())
        
        # Se a soma for diferente da média padronizada, ajustar proporcionalmente
        if abs(soma_medias_linhas - media_historica_total_padronizada) > 0.01:
            # Calcular fator de ajuste
            if soma_medias_linhas > 0:
                fator_ajuste = media_historica_total_padronizada / soma_medias_linhas
                # Aplicar ajuste proporcional em todas as linhas
                df_media_mensal['Total'] = df_media_mensal['Total'] * fator_ajuste
                
                # 🔧 VERIFICAÇÃO: Confirmar que o ajuste funcionou
                soma_medias_linhas_apos_ajuste = float(df_media_mensal['Total'].sum())
                if abs(soma_medias_linhas_apos_ajuste - media_historica_total_padronizada) > 0.01:
                    # Se ainda houver diferença, forçar ajuste direto
                    diferenca = media_historica_total_padronizada - soma_medias_linhas_apos_ajuste
                    # Distribuir a diferença proporcionalmente
                    if len(df_media_mensal) > 0:
                        ajuste_adicional = diferenca / len(df_media_mensal)
                        df_media_mensal['Total'] = df_media_mensal['Total'] + ajuste_adicional
    
    # Verificar se encontrou menos períodos do que o solicitado
    if not df_medias.empty and 'Período' in df_medias.columns:
        periodos_encontrados = df_medias['Período'].unique()
        if len(periodos_encontrados) < len(periodos_para_media):
            st.info(f"ℹ️ **Informação:** Foram encontrados {len(periodos_encontrados)} período(s) nos dados (solicitados: {len(periodos_para_media)}). O cálculo será feito com os períodos disponíveis.")
    
    # Função para calcular volumes e CPU com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_volumes_cpu(df_vol_cache, df_medias_cache, colunas_adicionais_cache, periodos_para_media_cache, ultimo_periodo_dados_cache=None, meses_excluir_media_cache=None):
        """
        Calcula volumes e CPU histórico com cache, usando apenas os períodos selecionados
        e EXCLUINDO os meses marcados para exclusão (meses_excluir_media_cache)
        """
        if df_vol_cache.empty or 'Período' not in df_vol_cache.columns or 'Volume' not in df_vol_cache.columns:
            return None, None, None, None
        
        # Filtrar apenas os períodos que serão usados para calcular a média de volume
        if periodos_para_media_cache and 'Período' in df_vol_cache.columns:
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
            periodos_no_df = df_vol_cache['Período'].astype(str).str.strip().str.lower()
            
            # Criar máscara: comparar período completo (mês + ano) quando disponível
            def periodo_corresponde(periodo_df):
                periodo_df_lower = str(periodo_df).strip().lower()
                
                # Verificar se o período está antes ou no último mês selecionado
                if ultimo_mes_limite and ultimo_ano_limite:
                    periodo_df_tem_ano = ' ' in periodo_df_lower and len(periodo_df_lower.split(' ', 1)) > 1
                    if periodo_df_tem_ano:
                        periodo_df_ano = int(periodo_df_lower.split(' ', 1)[1]) if periodo_df_lower.split(' ', 1)[1].isdigit() else None
                        periodo_df_mes = periodo_df_lower.split(' ', 1)[0]
                        
                        # Verificar se está antes do último mês
                        if periodo_df_ano and periodo_df_ano > ultimo_ano_limite:
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
                        periodo_df_mes = periodo_df_lower
                        meses_ano_lower = [m.lower() for m in meses_ano]
                        if periodo_df_mes in meses_ano_lower and ultimo_mes_limite in meses_ano_lower:
                            idx_periodo = meses_ano_lower.index(periodo_df_mes)
                            idx_limite = meses_ano_lower.index(ultimo_mes_limite)
                            if idx_periodo > idx_limite:
                                return False
                
                # Comparação exata primeiro (período completo)
                if periodo_df_lower in periodos_procurados_normalizados:
                    return True
                
                # Se não houver correspondência exata, verificar se ambos têm ano
                periodo_df_tem_ano = ' ' in periodo_df_lower and len(periodo_df_lower.split(' ', 1)) > 1
                
                for periodo_procurado in periodos_procurados_normalizados:
                    periodo_procurado_tem_ano = ' ' in periodo_procurado and len(periodo_procurado.split(' ', 1)) > 1
                    
                    # Se ambos têm ano, comparar período completo (já verificamos exato acima)
                    if periodo_df_tem_ano and periodo_procurado_tem_ano:
                        # Se ambos têm ano mas não são iguais, não corresponde
                        continue
                    
                    # Se nenhum tem ano ou apenas um tem, comparar apenas o mês
                    # (compatibilidade com dados antigos)
                    mes_df = periodo_df_lower.split(' ', 1)[0] if ' ' in periodo_df_lower else periodo_df_lower
                    mes_procurado = periodo_procurado.split(' ', 1)[0] if ' ' in periodo_procurado else periodo_procurado
                    
                    if mes_df == mes_procurado:
                        # Se o período procurado tem ano mas o do DF não tem, não incluir
                        # (para evitar incluir períodos futuros sem ano)
                        if periodo_procurado_tem_ano and not periodo_df_tem_ano:
                            continue
                        return True
                
                return False
            
            df_vol_para_media = df_vol_cache[
                periodos_no_df.apply(periodo_corresponde)
            ].copy()
            
            # 🔧 CORREÇÃO: Excluir meses marcados para exclusão do cálculo do volume
            # Isso garante que o volume médio histórico também exclua os mesmos meses que foram excluídos da média de custo
            if meses_excluir_media_cache and not df_vol_para_media.empty:
                meses_excluir_normalizados = [str(mes).strip().lower() for mes in meses_excluir_media_cache]
                
                def periodo_nao_esta_excluido(periodo_val):
                    periodo_str = str(periodo_val).strip().lower()
                    # Extrair mês do período
                    periodo_mes = None
                    if ' ' in periodo_str:
                        periodo_mes = periodo_str.split(' ', 1)[0]
                    else:
                        periodo_mes = periodo_str
                    # Se o mês está na lista de excluídos, retornar False (não incluir)
                    return periodo_mes not in meses_excluir_normalizados
                
                df_vol_para_media = df_vol_para_media[
                    df_vol_para_media['Período'].apply(periodo_nao_esta_excluido)
            ].copy()

            # Se, por algum motivo, o filtro não encontrar nada, voltar a usar todos os dados
            # para não ficar com volume histórico zero (mantém a regra de negócio funcionando)
            if df_vol_para_media.empty:
                df_vol_para_media = df_vol_cache.copy()
        else:
            # Se não houver períodos selecionados, usar todos os dados (comportamento original)
            # Mas ainda excluir meses marcados para exclusão
            df_vol_para_media = df_vol_cache.copy()
            
            # 🔧 CORREÇÃO: Excluir meses marcados para exclusão mesmo quando não há períodos selecionados
            if meses_excluir_media_cache and not df_vol_para_media.empty:
                meses_excluir_normalizados = [str(mes).strip().lower() for mes in meses_excluir_media_cache]
                
                def periodo_nao_esta_excluido(periodo_val):
                    periodo_str = str(periodo_val).strip().lower()
                    periodo_mes = periodo_str.split(' ', 1)[0] if ' ' in periodo_str else periodo_str
                    return periodo_mes not in meses_excluir_normalizados
                
                df_vol_para_media = df_vol_para_media[
                    df_vol_para_media['Período'].apply(periodo_nao_esta_excluido)
                ].copy()
        
        # Calcular média de volume por período histórico (apenas meses selecionados)
        if not df_vol_para_media.empty:
            # 🔧 CORREÇÃO CRÍTICA: Filtrar apenas volumes do ano mais recente (evita somar 2024 e 2025)
            if 'Ano' in df_vol_para_media.columns:
                anos_unicos = df_vol_para_media['Ano'].dropna().unique()
                if len(anos_unicos) > 1:
                    # Pegar o ano mais recente
                    ano_mais_recente = df_vol_para_media['Ano'].max()
                    df_vol_para_media = df_vol_para_media[df_vol_para_media['Ano'] == ano_mais_recente].copy()
            
            # 🔧 CORREÇÃO: Normalizar Período usando coluna Ano ORIGINAL dos dados (IGUAL TC EXT)
            # Estratégia: Se Período não tem ano, usar coluna Ano original para adicionar ao Período
            if 'Período' in df_vol_para_media.columns:
                df_vol_para_media = df_vol_para_media.copy()
                # 🔧 CORREÇÃO: Converter Período para string ANTES de qualquer operação (pode ser Categorical)
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
                    # 🔧 CORREÇÃO: Converter Período para string antes de concatenar (pode ser Categorical)
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
            
            # Agrupar incluindo Ano (IGUAL TC EXT)
            colunas_groupby_vol_medio = ['Oficina', 'Veículo', 'Período']
            if 'Ano' in df_vol_para_media.columns:
                colunas_groupby_vol_medio.append('Ano')
            df_vol_medio = df_vol_para_media.groupby(colunas_groupby_vol_medio, as_index=False)['Volume'].mean()
            
            # Calcular volume médio mensal (média dos meses selecionados do ano correto)
            df_vol_medio_mensal = df_vol_medio.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
            df_vol_medio_mensal = df_vol_medio_mensal.rename(columns={'Volume': 'Volume_Medio_Historico'})
        else:
            # Se não houver dados, criar DataFrames vazios
            df_vol_medio = pd.DataFrame(columns=['Oficina', 'Veículo', 'Período', 'Volume'])
            df_vol_medio_mensal = pd.DataFrame(columns=['Oficina', 'Veículo', 'Volume_Medio_Historico'])
        
        # Volume por mês (incluindo meses futuros)
        # 🔧 CORREÇÃO CRÍTICA: Filtrar apenas volumes do ano mais recente (ou do ano do período de forecast)
        # Isso evita somar volumes de 2024 e 2025 quando busca um mês específico
        df_vol_para_por_mes = df_vol_cache.copy()
        
        # Se há coluna 'Ano', filtrar apenas o ano mais recente (ou anos de forecast se disponível)
        if 'Ano' in df_vol_para_por_mes.columns:
            anos_unicos = df_vol_para_por_mes['Ano'].dropna().unique()
            if len(anos_unicos) > 1:
                # Pegar o ano mais recente
                ano_mais_recente = df_vol_para_por_mes['Ano'].max()
                # Mas também incluir anos futuros (se houver períodos de forecast com anos diferentes)
                # Por enquanto, usar apenas o ano mais recente para evitar duplicação
                df_vol_para_por_mes = df_vol_para_por_mes[df_vol_para_por_mes['Ano'] == ano_mais_recente].copy()
        
        # 🔧 CORREÇÃO: Incluir 'Ano' no groupby (IGUAL TC EXT) para separar períodos de anos diferentes
        colunas_groupby_vol_por_mes = ['Oficina', 'Veículo', 'Período']
        if 'Ano' in df_vol_para_por_mes.columns:
            colunas_groupby_vol_por_mes.append('Ano')
        df_vol_por_mes = df_vol_para_por_mes.groupby(colunas_groupby_vol_por_mes, as_index=False)['Volume'].sum()
        
        # Calcular relação custo/volume histórica para custos variáveis
        # 🔧 CORREÇÃO: Incluir 'Ano' no merge (IGUAL TC EXT)
        colunas_merge_custo_volume = ['Oficina', 'Veículo', 'Período']
        if 'Ano' in df_medias_cache.columns and 'Ano' in df_vol_medio.columns:
            colunas_merge_custo_volume.append('Ano')
        df_custo_volume = pd.merge(
            df_medias_cache[df_medias_cache['Tipo_Custo'] == 'Variável'],
            df_vol_medio,
            on=colunas_merge_custo_volume,
            how='left'
        )
        
        # Calcular CPU histórico
        df_custo_volume['CPU_Historico'] = df_custo_volume.apply(
            lambda row: row['Total'] / row['Volume'] if pd.notnull(row['Volume']) and row['Volume'] > 0 else 0,
            axis=1
        )
        
        # Calcular CPU médio
        colunas_groupby_cpu = ['Oficina', 'Veículo'] + colunas_adicionais_cache
        df_cpu_medio = df_custo_volume.groupby(colunas_groupby_cpu).agg({
            'CPU_Historico': 'mean',
            'Volume': 'mean'
        }).reset_index()
        df_cpu_medio = df_cpu_medio.rename(columns={'Volume': 'Volume_Medio_Ref'})
        
        return df_vol_medio_mensal, df_vol_por_mes, df_cpu_medio, df_vol_medio

    # Carregar volumes futuros (se disponível) e calcular relação custo/volume
    # O df_vol já contém os volumes futuros que serão usados para o forecast
    # 🔧 CORREÇÃO: Passar meses_excluir_media para que o volume também exclua os meses marcados
    volume_base, volume_por_mes, df_cpu_medio, df_vol_medio = calcular_volumes_cpu(df_vol, df_medias, colunas_adicionais, periodos_para_media, ultimo_periodo_dados, meses_excluir_media)
    
    if volume_base is None:
        st.warning("⚠️ Dados de volume não disponíveis. Usando valores fixos para forecast.")
        volume_base = pd.DataFrame(columns=['Oficina', 'Veículo', 'Volume_Medio_Historico'])
        volume_por_mes = pd.DataFrame(columns=['Oficina', 'Veículo', 'Período', 'Volume'])
        df_cpu_medio = pd.DataFrame(columns=['Oficina', 'Veículo'] + colunas_adicionais + ['CPU_Historico', 'Volume_Medio_Ref'])
    
    # Função para calcular forecast completo com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_forecast_completo(df_media_mensal_cache, volume_base_cache, df_cpu_medio_cache, 
                                    volume_por_mes_cache, colunas_adicionais_cache, meses_restantes_cache,
                                    sensibilidade_fixo_cache, sensibilidade_variavel_cache, sensibilidades_type06_cache,
                                    inflacao_type06_cache):
        """
        Calcula forecast completo linha a linha seguindo lógica matemática clara:
        
        LÓGICA DO CÁLCULO (linha a linha):
        ===================================
        
        1. Para cada linha do forecast:
           - Média histórica do custo (já considera exclusão de meses, último período com dados reais, etc.)
           - Volume do mês futuro (volume realizado do período de forecast)
           - Volume médio histórico (já considera exclusão de meses, último período com dados reais, etc.)
        
        2. Calcular proporção de volume:
           proporcao_volume = Volume_do_mes / Volume_medio_historico
           
           Exemplo: Se volume médio histórico = 100 e volume do mês = 110
           proporcao_volume = 110 / 100 = 1.1
        
        3. Calcular variação percentual:
           variacao_percentual = proporcao_volume - 1.0
           
           Exemplo: Se proporção = 1.1, então variação = 1.1 - 1.0 = 0.1 (10% de aumento)
        
        4. Aplicar sensibilidade (linha a linha, baseado no Tipo_Custo):
           - Se Tipo_Custo == 'Fixo': sensibilidade = sensibilidade_fixo
           - Se Tipo_Custo == 'Variável': sensibilidade = sensibilidade_variavel
           - Se modo Type 06: usar sensibilidade específica do Type 06
           
           variação_ajustada = variacao_percentual * sensibilidade
           
           Exemplos:
           - Se variação = 10% (0.1) e sensibilidade = 0.0: variação_ajustada = 0.1 * 0.0 = 0.0 (0%)
           - Se variação = 10% (0.1) e sensibilidade = 0.5: variação_ajustada = 0.1 * 0.5 = 0.05 (5%)
           - Se variação = 10% (0.1) e sensibilidade = 0.6: variação_ajustada = 0.1 * 0.6 = 0.06 (6%)
        
        5. Calcular forecast:
           fator_variacao = 1.0 + variação_ajustada
           fator_inflacao = 1.0 + (inflacao / 100.0)
           forecast = Média_historica * fator_variacao * fator_inflacao
           
           Se sensibilidade = 0: fator_variacao = 1.0, então forecast = Média_historica * 1.0 = Média_historica
        
        6. Total do forecast = Soma de todas as linhas (não há ajustes manuais)
        
        IMPORTANTE:
        - Volume médio histórico e Média histórica do custo já consideram:
          * Exclusão de meses (meses_excluir_media)
          * Último período com dados reais (ultimo_periodo_dados)
          * Todas as configurações do Forecast
        - Cálculo é feito linha a linha, sem agregações intermediárias
        - O total é sempre a soma das linhas individuais
        """
        # Converter tuple de volta para dict se necessário
        if sensibilidades_type06_cache is not None:
            sensibilidades_type06_dict = dict(sensibilidades_type06_cache)
        else:
            sensibilidades_type06_dict = None
        
        if inflacao_type06_cache is not None:
            inflacao_type06_dict = dict(inflacao_type06_cache)
            # 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance
        else:
            inflacao_type06_dict = None
        
        # 🔧 VERIFICAÇÃO: Garantir que df_media_mensal não tem duplicatas
        # Se houver duplicatas, o merge vai criar linhas multiplicadas
        # 🔧 CORREÇÃO: Incluir 'Ano' na chave se existir (evita agrupar dados de 2024 com 2025)
        colunas_chave_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
        if 'Ano' in df_media_mensal_cache.columns:
            colunas_chave_media.insert(2, 'Ano')  # Inserir Ano após Veículo
        colunas_chave_media_existentes = [col for col in colunas_chave_media if col in df_media_mensal_cache.columns]
        
        if len(colunas_chave_media_existentes) > 0:
            duplicatas_media = df_media_mensal_cache.duplicated(subset=colunas_chave_media_existentes, keep=False)
            if duplicatas_media.any():
                # 🔧 CORREÇÃO CRÍTICA: Se houver duplicatas, SOMAR (não tirar média)
                # Cada linha duplicada representa uma parte do total que deve ser somada
                agg_dict_media_dup = {'Total': 'sum'}  # SOMAR as médias duplicadas (não tirar média)
                df_media_mensal_cache = df_media_mensal_cache.groupby(
                    colunas_chave_media_existentes, as_index=False
                ).agg(agg_dict_media_dup)
        
        # 🔧 VERIFICAÇÃO: Garantir que volume_base não tem duplicatas
        # Se houver múltiplas linhas para mesma Oficina + Veículo, o merge vai duplicar
        # 🔧 CORREÇÃO CRÍTICA: volume_base é volume médio histórico (não específico por ano)
        # NÃO usar 'Ano' no agrupamento - volume_base já é uma média geral
        if not volume_base_cache.empty and 'Oficina' in volume_base_cache.columns and 'Veículo' in volume_base_cache.columns:
            colunas_dup_volume = ['Oficina', 'Veículo']
            # NÃO incluir 'Ano' - volume_base é médio histórico geral
            duplicatas_volume = volume_base_cache.duplicated(subset=colunas_dup_volume, keep=False)
            if duplicatas_volume.any():
                # 🔧 CORREÇÃO CRÍTICA: Se houver duplicatas, SOMAR (não tirar média)
                # Cada linha duplicada representa uma parte do volume que deve ser somada
                volume_base_cache = volume_base_cache.groupby(
                    colunas_dup_volume, as_index=False
                ).agg({'Volume_Medio_Historico': 'sum'})  # SOMAR os volumes duplicados (não tirar média)
        
        # Usar df_media_mensal_cache que já contém a média histórica calculada
        # (a média já foi calculada usando a lógica correta: média dos totais por período)
        df_forecast_base = df_media_mensal_cache.copy()
        
        # Fazer merge com volume_base
        # 🔧 CORREÇÃO CRÍTICA: volume_base é volume médio histórico (não é por mês específico)
        # NÃO usar 'Ano' como chave aqui, pois volume_base é uma média geral
        # Usar apenas Oficina e Veículo (e colunas adicionais se necessário)
        colunas_merge_volume = ['Oficina', 'Veículo']
        # NÃO incluir 'Ano' aqui - volume_base é médio histórico, não específico por ano
        
        df_forecast_base = df_forecast_base.merge(
            volume_base_cache,
            on=colunas_merge_volume,
            how='left'
        )
        
        # Verificar se o merge criou duplicatas
        num_linhas_apos_merge = len(df_forecast_base)
        num_linhas_antes_merge = len(df_media_mensal_cache)
        if num_linhas_apos_merge > num_linhas_antes_merge:
            # Verificar duplicatas por chave completa
            colunas_chave_completa = colunas_merge_volume + ['Tipo_Custo'] + [col for col in colunas_adicionais_cache if col in df_forecast_base.columns]
            colunas_chave_completa_existentes = [col for col in colunas_chave_completa if col in df_forecast_base.columns]
            if len(colunas_chave_completa_existentes) > 0:
                duplicatas_merge = df_forecast_base.duplicated(subset=colunas_chave_completa_existentes, keep=False)
                num_duplicatas_merge = duplicatas_merge.sum()
                if num_duplicatas_merge > 0:
                    st.sidebar.error(f"❌ PROBLEMA CRÍTICO: Merge criou {num_duplicatas_merge} linhas duplicadas! Isso causa valores pela metade!")
                    # 🔧 CORREÇÃO: Agrupar duplicatas somando valores numéricos
                    agg_dict_merge_dup = {}
                    for col in df_forecast_base.columns:
                        if col not in colunas_chave_completa_existentes:
                            if col == 'Média_Mensal_Histórica' or col == 'Total':
                                agg_dict_merge_dup[col] = 'sum'  # Somar médias duplicadas
                            elif col == 'Volume_Medio_Historico':
                                agg_dict_merge_dup[col] = 'sum'  # Somar volumes duplicados
                            else:
                                agg_dict_merge_dup[col] = 'first'
                    df_forecast_base = df_forecast_base.groupby(
                        colunas_chave_completa_existentes, as_index=False
                    ).agg(agg_dict_merge_dup)
                    st.sidebar.success(f"✅ Corrigido: {len(df_forecast_base)} linhas após agrupamento de duplicatas")
        
        # Se não houver volume médio histórico, manter como 0 para não distorcer a proporção
        df_forecast_base['Volume_Medio_Historico'] = df_forecast_base['Volume_Medio_Historico'].fillna(0.0)
        
        # 🔧 VERIFICAÇÃO: Garantir que df_cpu_medio não tem duplicatas
        # Se houver múltiplas linhas para mesma combinação, o merge vai duplicar
        if df_cpu_medio_cache is not None and not df_cpu_medio_cache.empty:
            colunas_merge_cpu = ['Oficina', 'Veículo'] + colunas_adicionais_cache
            colunas_merge_cpu_existentes = [col for col in colunas_merge_cpu if col in df_cpu_medio_cache.columns]
            
            if len(colunas_merge_cpu_existentes) > 0:
                duplicatas_cpu = df_cpu_medio_cache.duplicated(subset=colunas_merge_cpu_existentes, keep=False)
                if duplicatas_cpu.any():
                    # 🔧 CORREÇÃO CRÍTICA: Se houver duplicatas, SOMAR valores numéricos (não tirar média)
                    # Cada linha duplicada representa uma parte do total que deve ser somada
                    colunas_agregar_cpu = [col for col in df_cpu_medio_cache.columns if col not in colunas_merge_cpu_existentes]
                    agg_dict_cpu = {col: 'sum' if df_cpu_medio_cache[col].dtype in ['float64', 'int64'] else 'first' for col in colunas_agregar_cpu}
                    df_cpu_medio_cache = df_cpu_medio_cache.groupby(
                        colunas_merge_cpu_existentes, as_index=False
                    ).agg(agg_dict_cpu)
            
            df_forecast_base = df_forecast_base.merge(
                df_cpu_medio_cache,
                on=colunas_merge_cpu_existentes,
                how='left'
            )
            df_forecast_base['CPU_Historico'] = df_forecast_base['CPU_Historico'].fillna(0)
            df_forecast_base['Volume_Medio_Ref'] = df_forecast_base['Volume_Medio_Ref'].fillna(df_forecast_base['Volume_Medio_Historico'])
        else:
            df_forecast_base['CPU_Historico'] = 0
            df_forecast_base['Volume_Medio_Ref'] = df_forecast_base['Volume_Medio_Historico']
        
        # Renomear 'Total' para 'Média_Mensal_Histórica'
        df_forecast_base = df_forecast_base.rename(columns={'Total': 'Média_Mensal_Histórica'})
        
        # Verificar se há duplicatas finais no df_forecast_base
        colunas_chave_final = ['Oficina', 'Veículo', 'Tipo_Custo'] + [col for col in colunas_adicionais_cache if col in df_forecast_base.columns]
        if 'Ano' in df_forecast_base.columns:
            colunas_chave_final.insert(2, 'Ano')
        colunas_chave_final_existentes = [col for col in colunas_chave_final if col in df_forecast_base.columns]
        if len(colunas_chave_final_existentes) > 0:
            duplicatas_final_base = df_forecast_base.duplicated(subset=colunas_chave_final_existentes, keep=False)
            num_duplicatas_final_base = duplicatas_final_base.sum()
            if num_duplicatas_final_base > 0:
                st.sidebar.error(f"❌ PROBLEMA: {num_duplicatas_final_base} linhas duplicadas finais em df_forecast_base!")
                # 🔧 CORREÇÃO FINAL: Agrupar duplicatas finais
                agg_dict_final_dup = {}
                for col in df_forecast_base.columns:
                    if col not in colunas_chave_final_existentes:
                        if col == 'Média_Mensal_Histórica':
                            agg_dict_final_dup[col] = 'sum'  # Somar médias duplicadas
                        elif col == 'Volume_Medio_Historico':
                            agg_dict_final_dup[col] = 'sum'  # Somar volumes duplicados
                        else:
                            agg_dict_final_dup[col] = 'first'
                df_forecast_base = df_forecast_base.groupby(
                    colunas_chave_final_existentes, as_index=False
                ).agg(agg_dict_final_dup)
                st.sidebar.success(f"✅ Corrigido: {len(df_forecast_base)} linhas únicas após agrupamento final")
        
        # Criar DataFrame final de forecast
        # 🔧 CORREÇÃO: Incluir 'Ano' se existir em df_media_mensal (preservar ano original)
        forecast_cols = ['Oficina', 'Veículo'] + colunas_adicionais_cache + ['Tipo_Custo', 'Média_Mensal_Histórica']
        if 'Ano' in df_forecast_base.columns:
            forecast_cols.insert(2, 'Ano')  # Inserir Ano após Veículo
        df_forecast = df_forecast_base[forecast_cols].copy()
        
        # Calcular forecast para cada período
        for idx_mes, periodo in enumerate(meses_restantes_cache):
            # Buscar volume específico deste período
            # Regra de negócio: só há previsão se existir volume para aquele mês/ano
            if volume_por_mes_cache is not None and not volume_por_mes_cache.empty:
                # Extrair mês e ano do período procurado
                periodo_str = str(periodo).strip()
                if ' ' in periodo_str:
                    mes_procurado = periodo_str.split(' ', 1)[0].lower()
                    ano_procurado = periodo_str.split(' ', 1)[1] if len(periodo_str.split(' ', 1)) > 1 else None
                else:
                    mes_procurado = periodo_str.lower()
                    ano_procurado = None
                
                # Criar função para comparar períodos de forma flexível
                def periodo_corresponde(periodo_df):
                    periodo_df_str = str(periodo_df).strip().lower()
                    if ' ' in periodo_df_str:
                        mes_df = periodo_df_str.split(' ', 1)[0]
                        ano_df = periodo_df_str.split(' ', 1)[1] if len(periodo_df_str.split(' ', 1)) > 1 else None
                    else:
                        mes_df = periodo_df_str
                        ano_df = None
                    
                    # Comparar mês
                    if mes_df != mes_procurado:
                        return False
                    
                    # Se ambos têm ano, comparar ano também
                    if ano_procurado is not None and ano_df is not None:
                        return str(ano_procurado) == str(ano_df)
                    
                    # Se pelo menos um não tem ano, considerar como correspondente (compatibilidade)
                    return True
                
                # Aplicar filtro flexível
                periodos_no_df = volume_por_mes_cache['Período'].astype(str)
                mask_corresponde = periodos_no_df.apply(periodo_corresponde)
                
                # 🔧 CORREÇÃO CRÍTICA: Se há coluna 'Ano' e o período tem ano, também filtrar por Ano
                if ano_procurado is not None and 'Ano' in volume_por_mes_cache.columns:
                    # Converter ano_procurado para o mesmo tipo da coluna Ano
                    try:
                        ano_procurado_num = int(ano_procurado) if ano_procurado.isdigit() else None
                        if ano_procurado_num is not None:
                            # Filtrar também por Ano
                            mask_ano = volume_por_mes_cache['Ano'] == ano_procurado_num
                            mask_corresponde = mask_corresponde & mask_ano
                    except:
                        pass
                
                # Selecionar colunas para merge
                # 🔧 CORREÇÃO CRÍTICA: NÃO usar 'Ano' como chave separada
                # O período já contém mês + ano (ex: "Novembro 2025")
                # Usar apenas Oficina e Veículo para o merge
                colunas_merge_vol = ['Oficina', 'Veículo', 'Volume']
                
                vol_mes_df = volume_por_mes_cache[mask_corresponde][colunas_merge_vol].copy()
                
                if not vol_mes_df.empty:
                    # Agrupar por Oficina e Veículo e SOMAR volumes (não fazer mean)
                    # 🔧 CORREÇÃO: NÃO usar 'Ano' no groupby - o período já foi filtrado corretamente
                    # Se houver múltiplos registros com mesmo Oficina+Veículo para o mesmo período, somar
                    colunas_groupby_vol = ['Oficina', 'Veículo']
                    vol_mes_df = vol_mes_df.groupby(colunas_groupby_vol, as_index=False)['Volume'].sum()
                    
                    # Fazer merge usando apenas Oficina e Veículo
                    # 🔧 CORREÇÃO: NÃO usar 'Ano' no merge - volume_por_mes já foi filtrado pelo período correto
                    colunas_merge_forecast = ['Oficina', 'Veículo']
                    
                    # Verificar se vol_mes_df tem duplicatas
                    duplicatas_vol_mes = vol_mes_df.duplicated(subset=colunas_merge_forecast, keep=False)
                    if duplicatas_vol_mes.any():
                        # Agrupar duplicatas antes do merge
                        vol_mes_df = vol_mes_df.groupby(colunas_merge_forecast, as_index=False)['Volume'].sum()
                    
                    df_vol_mes_merge = df_forecast_base[colunas_merge_forecast].merge(
                        vol_mes_df,
                        on=colunas_merge_forecast,
                        how='left',
                        suffixes=('', '_mes')
                    )
                    
                    volume_mes_serie = df_vol_mes_merge['Volume']
                else:
                    # Sem volume para este período: não calcular forecast (mantém 0)
                    continue
            else:
                # Sem nenhum volume disponível: não calcular forecast para este período
                continue
            
            # Alinhar volume do mês futuro com o índice do df_forecast_base
            if isinstance(volume_mes_serie, pd.Series):
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Historico'])
            else:
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Historico'])
            
            # 🔧 CÁLCULO LINHA A LINHA (sem ajustes manuais):
            # Para cada linha, calcular forecast seguindo a fórmula matemática exata
            
            # Inicializar coluna de forecast
            df_forecast[periodo] = 0.0
            
            # 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance (pode ser reativado se necessário)
            # if 'Média_Mensal_Histórica' in df_forecast_base.columns:
            #     medias_nao_zero = df_forecast_base[df_forecast_base['Média_Mensal_Histórica'] > 0]
            #     if len(medias_nao_zero) == 0:
            #         st.sidebar.warning(f"⚠️ ATENÇÃO: Todas as linhas têm média = 0.00!")
            
            # Calcular forecast linha a linha
            for idx in df_forecast_base.index:
                # 1. Obter valores da linha
                media_historica = float(df_forecast_base.loc[idx, 'Média_Mensal_Histórica'])
                volume_medio_historico = float(df_forecast_base.loc[idx, 'Volume_Medio_Historico'])
                volume_mes = float(volume_mes_aligned.loc[idx])
                tipo_custo = df_forecast_base.loc[idx, 'Tipo_Custo']
                
                # 2. Calcular proporção de volume: Volume_mes / Volume_medio_historico
                if volume_medio_historico > 0:
                    proporcao_volume = volume_mes / volume_medio_historico
                else:
                    # Se não há volume histórico, usar proporção neutra (1.0)
                    proporcao_volume = 1.0
                
                # 3. Calcular variação percentual: proporcao_volume - 1.0
                # Exemplo: se proporção = 1.1, então variação = 0.1 (10% de aumento)
                variacao_percentual = proporcao_volume - 1.0
                
                # 4. Obter sensibilidade (linha a linha, baseado no Tipo_Custo)
                if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_base.columns:
                    # Modo detalhado: usar sensibilidade específica do Type 06
                    type06_valor = df_forecast_base.loc[idx, 'Type 06']
                    if pd.notna(type06_valor) and type06_valor in sensibilidades_type06_dict:
                        sensibilidade = sensibilidades_type06_dict[type06_valor]
                    else:
                        # Se não encontrar Type 06, usar sensibilidade baseada no Tipo_Custo
                        sensibilidade = sensibilidade_fixo_cache if tipo_custo == 'Fixo' else sensibilidade_variavel_cache
                else:
                    # Modo global: usar sensibilidade baseada no Tipo_Custo
                    sensibilidade = sensibilidade_fixo_cache if tipo_custo == 'Fixo' else sensibilidade_variavel_cache
                
                # 5. Aplicar sensibilidade: variação_ajustada = variacao_percentual * sensibilidade
                # Exemplos:
                # - Se variação = 10% (0.1) e sensibilidade = 0.0: variação_ajustada = 0.0 (0%)
                # - Se variação = 10% (0.1) e sensibilidade = 0.5: variação_ajustada = 0.05 (5%)
                # - Se variação = 10% (0.1) e sensibilidade = 0.6: variação_ajustada = 0.06 (6%)
                variacao_ajustada = variacao_percentual * sensibilidade
                
                # 6. Obter inflação (linha a linha)
                # 🔧 CORREÇÃO: Sempre verificar primeiro se há 'GLOBAL', depois Type 06 específico
                if inflacao_type06_dict is not None:
                    # Primeiro, tentar usar 'GLOBAL' se existir (modo global)
                    if 'GLOBAL' in inflacao_type06_dict:
                        inflacao_percentual = inflacao_type06_dict['GLOBAL'] / 100.0
                    # Depois, verificar se está no modo detalhado e há Type 06 específico
                    elif sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_base.columns:
                        # Modo detalhado: usar inflação específica do Type 06
                        type06_valor = df_forecast_base.loc[idx, 'Type 06']
                        if pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                            inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0
                        else:
                            # Se não encontrar Type 06 específico, usar 0
                            inflacao_percentual = 0.0
                    else:
                        # Modo global sem 'GLOBAL': pegar o primeiro valor (todos são iguais no modo global)
                        if len(inflacao_type06_dict) > 0:
                            primeiro_valor = next(iter(inflacao_type06_dict.values()), 0.0)
                            inflacao_percentual = primeiro_valor / 100.0
                        else:
                            inflacao_percentual = 0.0
                else:
                    # Se inflacao_type06_dict é None, inflação é 0
                    inflacao_percentual = 0.0
                
                # 7. Calcular forecast: PRIMEIRO aplicar sensibilidade, DEPOIS aplicar inflação
                # ORDEM CORRETA:
                # 1. Aplicar sensibilidade (variação de volume): forecast_apos_volume = Média_historica × (1 + variação_ajustada)
                # 2. Aplicar inflação: forecast = forecast_apos_volume × (1 + inflação)
                # Fórmula final: forecast = Média_historica × (1 + variação_ajustada) × (1 + inflação)
                #
                # Exemplos:
                # - Se sensibilidade = 0 e inflação = 10%: 
                #   forecast = Média_historica × 1.0 × 1.10 = Média_historica × 1.10 (inflação aplicada!)
                # - Se sensibilidade = 1 e inflação = 0:
                #   forecast = Média_historica × proporcao_volume × 1.0 = Média_historica × proporcao_volume
                # - Se sensibilidade = 0 e inflação = 0:
                #   forecast = Média_historica × 1.0 × 1.0 = Média_historica
                
                # PASSO 1: Aplicar sensibilidade (variação de volume)
                fator_variacao = 1.0 + variacao_ajustada
                forecast_apos_volume = media_historica * fator_variacao
                
                # PASSO 2: Aplicar inflação sobre o resultado do passo 1
                # 🔧 CORREÇÃO: Garantir que inflacao_percentual está em formato decimal (0.05 para 5%)
                # A inflação já vem dividida por 100.0, então está correta
                fator_inflacao = 1.0 + inflacao_percentual
                forecast = forecast_apos_volume * fator_inflacao
                
                # 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance
                # Validações e mensagens de debug foram comentadas para reduzir overhead
                # if inflacao_percentual > 0 and media_historica > 0:
                #     forecast_esperado_com_inflacao = forecast_apos_volume * (1.0 + inflacao_percentual)
                #     if abs(forecast - forecast_esperado_com_inflacao) > 0.01:
                #         if idx == df_forecast_base.index[0] and periodo == meses_restantes_cache[0] if meses_restantes_cache else False:
                #             st.sidebar.error(f"❌ ERRO: Inflação não aplicada corretamente!")
                
                # 🔧 OTIMIZAÇÃO: Validações removidas para melhorar performance
                # if sensibilidade == 0 and inflacao_percentual == 0 and abs(proporcao_volume - 1.0) < 0.01:
                #     if abs(forecast - media_historica) > 0.01:
                #         if idx == df_forecast_base.index[0] and periodo == meses_restantes_cache[0] if meses_restantes_cache else False:
                #             st.sidebar.error(f"❌ PROBLEMA: forecast ≠ média quando deveria ser igual!")
                
                # if abs(sensibilidade - 1.0) < 0.01 and abs(inflacao_percentual) < 0.0001:
                #     cpu_esperado = media_historica / volume_medio_historico if volume_medio_historico > 0 else 0
                #     cpu_calculado = forecast / volume_mes if volume_mes > 0 else 0
                #     if cpu_esperado > 0 and abs(cpu_calculado - cpu_esperado) > 0.01:
                #         if idx == df_forecast_base.index[0] and periodo == meses_restantes_cache[0] if meses_restantes_cache else False:
                #             st.sidebar.warning(f"⚠️ CPU variando quando deveria ser constante")
                
                # 8. Atribuir forecast à linha
                df_forecast.loc[idx, periodo] = forecast
                
                # 🔧 OTIMIZAÇÃO: Debug removido para melhorar performance
                # if idx == df_forecast_base.index[0] and periodo == meses_restantes_cache[0] if meses_restantes_cache else False:
                #     valor_atribuido = df_forecast.loc[idx, periodo]
                #     if abs(valor_atribuido - forecast) > 0.01:
                #         st.sidebar.error(f"❌ ERRO: Forecast não foi atribuído corretamente!")
                #     elif inflacao_percentual > 0 and media_historica > 0:
                #         diferenca = forecast - forecast_apos_volume
                #         st.sidebar.success(f"✅ Forecast atribuído corretamente: {forecast:,.2f}")
                #         st.sidebar.info(f"🔍 Ordem de cálculo: Média={media_historica:,.2f} → Após sensibilidade={forecast_apos_volume:,.2f} → Após inflação={forecast:,.2f}")
                #         st.sidebar.info(f"🔍 Efeito da inflação: +{diferenca:,.2f} = +{inflacao_percentual*100:.2f}% (aplicado sobre {forecast_apos_volume:,.2f})")
            
            # Total do forecast = Soma de todas as linhas (calculado automaticamente pelo pandas)
        
        # Não há necessidade de verificação final ou ajustes manuais
        # O cálculo linha a linha garante que:
        # - Se sensibilidade = 0: variação_ajustada = 0, então forecast = média_historica * 1.0 = média_historica
        # - Se inflação = 0: fator_inflacao = 1.0, então forecast = média_historica * fator_variacao * 1.0
        # - O total é sempre a soma das linhas individuais
        
        return df_forecast

    # Preparar dados para forecast usando operações vetorizadas (mais rápido)
    # Calcular forecast com cache (incluindo sensibilidades e inflação)
    # Inicializar sensibilidades_type06 e inflacao_type06 se não estiverem definidas
    if 'sensibilidades_type06' not in locals() and 'sensibilidades_type06' not in globals():
        sensibilidades_type06 = None
    if 'inflacao_type06' not in locals() and 'inflacao_type06' not in globals():
        inflacao_type06 = None
    
    # Converter sensibilidades_type06 para tuple se for dict (para ser hashable no cache)
    sens_type06_cache = tuple(sorted(sensibilidades_type06.items())) if sensibilidades_type06 is not None else None
    inflacao_type06_cache = tuple(sorted(inflacao_type06.items())) if inflacao_type06 is not None else None
    
    # 🔧 OTIMIZAÇÃO: Debug apenas se habilitado (reduz mensagens na sidebar)
    # Remover debug de produção para melhorar performance
    # if inflacao_type06_cache is not None:
    #     st.sidebar.info(f"🔍 Debug: Inflação sendo passada para calcular_forecast_completo: {dict(inflacao_type06_cache)}")
    # else:
    #     st.sidebar.warning(f"⚠️ Debug: inflacao_type06_cache é None - inflação não será aplicada!")
    
    # 🔧 CORREÇÃO: Passar media_historica_total_padronizada para a função calcular_forecast_completo
    # para garantir que o forecast use a média correta
    # Inicializar sensibilidade_fixo e sensibilidade_variavel se não estiverem definidas
    if 'sensibilidade_fixo' not in locals() and 'sensibilidade_fixo' not in globals():
        sensibilidade_fixo = None
    if 'sensibilidade_variavel' not in locals() and 'sensibilidade_variavel' not in globals():
        sensibilidade_variavel = None
    
    if sensibilidade_fixo is None or sensibilidade_variavel is None:
        st.error(f"❌ Erro: Sensibilidade não configurada corretamente!")
        st.stop()
    
    df_forecast = calcular_forecast_completo(
        df_media_mensal, 
        volume_base if volume_base is not None else pd.DataFrame(columns=['Oficina', 'Veículo', 'Volume_Medio_Historico']),
        df_cpu_medio,
        volume_por_mes if volume_por_mes is not None else pd.DataFrame(columns=['Oficina', 'Veículo', 'Período', 'Volume']),
        colunas_adicionais,
        periodos_restantes,
        sensibilidade_fixo,
        sensibilidade_variavel,
        sens_type06_cache,
        inflacao_type06_cache
    )
    
    # Não há ajustes manuais: o cálculo linha a linha garante que os valores estão corretos
    # A média histórica já foi ajustada anteriormente para corresponder à média padronizada
    # Guardar versão bruta do forecast (antes do agrupamento) para diagnósticos
    df_forecast_bruto = df_forecast.copy()
    
    # 🆕 NOVA FUNCIONALIDADE: Gerar tabela completa com forecast linha a linha
    if st.session_state.get('gerar_tabela_completa_forecast', False):
        try:
            # CRIAR PASTA FORECAST PRIMEIRO (antes de qualquer processamento)
            pasta_dados = "dados"
            pasta_forecast = os.path.join(pasta_dados, "Forecast")
            try:
                # Criar pasta dados se não existir
                if not os.path.exists(pasta_dados):
                    os.makedirs(pasta_dados, exist_ok=True)
                    st.info(f"📁 Pasta 'dados' criada: {os.path.abspath(pasta_dados)}")
                
                # Criar pasta Forecast dentro de dados
                if not os.path.exists(pasta_forecast):
                    os.makedirs(pasta_forecast, exist_ok=True)
                    st.success(f"✅ Pasta Forecast criada: {os.path.abspath(pasta_forecast)}")
                else:
                    st.info(f"📁 Pasta Forecast já existe: {os.path.abspath(pasta_forecast)}")
            except Exception as e_pasta_inicial:
                st.error(f"❌ Erro ao criar pasta Forecast no início: {str(e_pasta_inicial)}")
                import traceback
                st.error(f"Detalhes: {traceback.format_exc()}")
            
            with st.spinner("🔄 Gerando tabela completa com forecast linha a linha..."):
                # Carregar dados completos da base original
                # SEMPRE tentar carregar, mas se não existir, será criado durante o processo
                caminho_base_original = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
                df_base_completo = None
                
                if os.path.exists(caminho_base_original):
                    df_base_completo = pd.read_parquet(caminho_base_original)
                else:
                    # Se não existir, tentar carregar do arquivo forecast ou usar dados filtrados
                    caminho_forecast_original = os.path.join("dados", "historico_consolidado", "df_final_historico_forecast.parquet")
                    if os.path.exists(caminho_forecast_original):
                        st.info(f"ℹ️ Usando arquivo forecast como base: {os.path.basename(caminho_forecast_original)}")
                        df_base_completo = pd.read_parquet(caminho_forecast_original)
                    else:
                        st.warning(f"⚠️ Arquivo base não encontrado: {caminho_base_original}")
                        st.info("ℹ️ O arquivo será criado durante o processo de consolidação.")
                
                # Verificar se df_base_completo foi carregado corretamente
                if df_base_completo is None or df_base_completo.empty:
                    st.error("❌ Erro: Não foi possível carregar dados históricos.")
                    st.error("ℹ️ Por favor, verifique se o arquivo 'df_final_historico.parquet' existe na pasta 'dados/historico_consolidado/'")
                    st.error("⚠️ O processo será interrompido, mas a pasta Forecast será criada mesmo assim.")
                    # Criar pasta mesmo em caso de erro
                    try:
                        pasta_dados = "dados"
                        pasta_forecast = os.path.join(pasta_dados, "Forecast")
                        if not os.path.exists(pasta_dados):
                            os.makedirs(pasta_dados, exist_ok=True)
                        if not os.path.exists(pasta_forecast):
                            os.makedirs(pasta_forecast, exist_ok=True)
                            st.info(f"📁 Pasta Forecast criada: {os.path.abspath(pasta_forecast)}")
                    except:
                        pass
                    st.stop()
                
                # Aplicar filtros (Oficina, Veículo, USI) mas NÃO filtrar por Período
                # para incluir TODOS os períodos históricos no arquivo forecast_completo.parquet
                df_base_filtrado = aplicar_filtros(
                    df_base_completo,
                    tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
                    tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
                    tuple(usi_selecionada) if usi_selecionada else tuple(),
                    "Todos"  # NÃO filtrar por período - incluir todos os períodos históricos
                )
                
                # 🔧 CORREÇÃO: Criar coluna 'Custo' se não existir (opcional)
                if 'Custo' not in df_base_filtrado.columns:
                    st.info("ℹ️ Coluna 'Custo' não encontrada em df_base_filtrado. Criando coluna 'Custo' com valores padrão.")
                    # Criar coluna Custo com valores padrão (pode ser baseado em Tipo_Custo se existir)
                    if 'Tipo_Custo' in df_base_filtrado.columns:
                        df_base_filtrado['Custo'] = df_base_filtrado['Tipo_Custo'].apply(
                            lambda x: 'Fixo' if str(x).upper() in ['FIXO', 'FIX', 'FIXED'] else 'Variável'
                        )
                    else:
                        # Se não tem Tipo_Custo, criar Custo com valor padrão 'Variável'
                        df_base_filtrado['Custo'] = 'Variável'
                
                # 🔧 CORREÇÃO: Criar coluna Tipo_Custo se não existir (mesma lógica do código principal)
                if 'Tipo_Custo' not in df_base_filtrado.columns:
                    def is_custo_fixo(valor_custo):
                        """Identifica se o custo é fixo baseado no valor da coluna Custo"""
                        if pd.isna(valor_custo):
                            return False
                        valor_str = str(valor_custo).strip().upper()
                        # Considerar como fixo se contém palavras-chave
                        palavras_fixo = ['FIXO', 'FIX', 'FIXED']
                        return any(palavra in valor_str for palavra in palavras_fixo)
                    
                    # Criar Tipo_Custo baseado na coluna Custo (que deve estar presente)
                    df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Custo'].apply(is_custo_fixo)
                    df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Tipo_Custo'].map({True: 'Fixo', False: 'Variável'})
                
                # Calcular médias históricas linha a linha (mesma lógica do forecast)
                # Agrupar por chave única (Oficina, Veículo, Tipo_Custo, etc) e período
                colunas_chave_forecast = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
                if 'Ano' in df_base_filtrado.columns:
                    colunas_chave_forecast.insert(2, 'Ano')
                colunas_chave_forecast_existentes = [col for col in colunas_chave_forecast if col in df_base_filtrado.columns]
                
                # Filtrar apenas períodos selecionados para média
                if periodos_para_media and 'Período' in df_base_filtrado.columns:
                    # Normalizar períodos
                    periodos_normalizados = [str(p).strip().lower() for p in periodos_para_media]
                    df_base_filtrado['Período_Norm'] = df_base_filtrado['Período'].astype(str).str.strip().str.lower()
                    df_base_para_media = df_base_filtrado[df_base_filtrado['Período_Norm'].isin(periodos_normalizados)].copy()
                    
                    # Excluir meses marcados
                    if meses_excluir_media:
                        meses_excluir_normalizados = [str(m).strip().lower() for m in meses_excluir_media]
                        df_base_para_media = df_base_para_media[~df_base_para_media['Período_Norm'].isin(meses_excluir_normalizados)].copy()
                else:
                    df_base_para_media = df_base_filtrado.copy()
                
                # Calcular média histórica por chave única
                # 🔧 CORREÇÃO: Sempre usar coluna 'Total' (nunca 'Valor')
                if 'Total' not in df_base_para_media.columns:
                    raise ValueError("❌ Coluna 'Total' não encontrada nos dados! A origem dos dados deve ter a coluna 'Total'.")
                df_medias_linha = df_base_para_media.groupby(colunas_chave_forecast_existentes, as_index=False)['Total'].mean()
                df_medias_linha.rename(columns={'Total': 'Média_Mensal_Histórica'}, inplace=True)
                
                # Remover colunas de normalização se existirem
                colunas_normalizacao_remover = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
                colunas_normalizacao_existentes = [col for col in colunas_normalizacao_remover if col in df_medias_linha.columns]
                if colunas_normalizacao_existentes:
                    df_medias_linha = df_medias_linha.drop(columns=colunas_normalizacao_existentes)
                
                # Fazer merge com volume_base para obter Volume_Medio_Historico
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
                # IMPORTANTE: Usar a mesma lógica do df_forecast_bruto (que é usado no gráfico)
                # O df_forecast_bruto já foi calculado com a função calcular_forecast_completo
                # que usa a mesma lógica do modo Custo Total do gráfico
                
                # Primeiro, criar df_forecast_completo a partir de df_base_filtrado
                # IMPORTANTE: Preservar a coluna 'Custo' durante todo o processamento
                df_forecast_completo = df_base_filtrado.copy()
                
                # 🔧 CORREÇÃO: Criar coluna 'Custo' se não existir (opcional)
                if 'Custo' not in df_forecast_completo.columns:
                    st.info("ℹ️ Coluna 'Custo' não encontrada em df_forecast_completo. Criando coluna 'Custo' com valores padrão.")
                    # Criar coluna Custo com valores padrão (pode ser baseado em Tipo_Custo se existir)
                    if 'Tipo_Custo' in df_forecast_completo.columns:
                        df_forecast_completo['Custo'] = df_forecast_completo['Tipo_Custo'].apply(
                            lambda x: 'Fixo' if str(x).upper() in ['FIXO', 'FIX', 'FIXED'] else 'Variável'
                        )
                    else:
                        # Se não tem Tipo_Custo, criar Custo com valor padrão 'Variável'
                        df_forecast_completo['Custo'] = 'Variável'
                
                # Remover colunas de normalização de período (não devem estar no arquivo final)
                # MAS NUNCA REMOVER 'Custo'
                colunas_normalizacao_remover = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
                colunas_normalizacao_existentes = [col for col in colunas_normalizacao_remover if col in df_forecast_completo.columns]
                if colunas_normalizacao_existentes:
                    df_forecast_completo = df_forecast_completo.drop(columns=colunas_normalizacao_existentes)
                
                # Adicionar Média_Mensal_Histórica e Volume_Medio_Historico via merge
                # IMPORTANTE: Usar apenas as colunas chave que existem em ambos os DataFrames
                colunas_merge_medias = [col for col in colunas_chave_forecast_existentes if col in df_medias_linha.columns]
                # Verificar se as colunas existem em df_medias_linha antes de fazer merge
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
                
                # IMPORTANTE: Tentar usar valores do df_forecast_bruto se disponível (mesma lógica do gráfico)
                # Isso garante que os valores sejam idênticos aos do gráfico
                if 'df_forecast_bruto' in locals() and df_forecast_bruto is not None and not df_forecast_bruto.empty:
                    # Fazer merge com df_forecast_bruto para obter valores de forecast já calculados
                    colunas_merge_forecast = [col for col in colunas_chave_forecast_existentes if col in df_forecast_bruto.columns]
                    if colunas_merge_forecast:
                        # Adicionar colunas de forecast do df_forecast_bruto
                        colunas_forecast_bruto = colunas_merge_forecast + [p for p in periodos_restantes if p in df_forecast_bruto.columns]
                        df_forecast_completo = df_forecast_completo.merge(
                            df_forecast_bruto[colunas_forecast_bruto],
                            on=colunas_merge_forecast,
                            how='left',
                            suffixes=('', '_bruto')
                        )
                        # Preencher valores de forecast do df_forecast_bruto onde disponível
                        for periodo in periodos_restantes:
                            coluna_bruto = f"{periodo}_bruto" if f"{periodo}_bruto" in df_forecast_completo.columns else periodo
                            if coluna_bruto in df_forecast_completo.columns:
                                # Usar valores do df_forecast_bruto onde disponível
                                mask_nao_preenchido = df_forecast_completo[periodo].isna() | (df_forecast_completo[periodo] == 0)
                                if coluna_bruto != periodo:
                                    df_forecast_completo.loc[mask_nao_preenchido, periodo] = df_forecast_completo.loc[mask_nao_preenchido, coluna_bruto].fillna(0.0)
                                    df_forecast_completo = df_forecast_completo.drop(columns=[coluna_bruto])
                
                # IMPORTANTE: Inicializar colunas de forecast com 0.0 se não existirem
                for periodo in periodos_restantes:
                    if periodo not in df_forecast_completo.columns:
                        df_forecast_completo[periodo] = 0.0
                
                # Converter sensibilidades e inflação para dict se necessário
                sensibilidades_type06_dict = None
                if sensibilidades_type06 is not None:
                    if isinstance(sensibilidades_type06, dict):
                        sensibilidades_type06_dict = sensibilidades_type06
                    elif isinstance(sensibilidades_type06, tuple):
                        sensibilidades_type06_dict = dict(sensibilidades_type06)
                
                inflacao_type06_dict = None
                if inflacao_type06 is not None:
                    if isinstance(inflacao_type06, dict):
                        inflacao_type06_dict = inflacao_type06
                    elif isinstance(inflacao_type06, tuple):
                        inflacao_type06_dict = dict(inflacao_type06)
                
                # Calcular forecast para cada período linha a linha
                for periodo in periodos_restantes:
                    # Buscar volume para este período (sem normalização e sem adicionar colunas ao df_forecast_completo)
                    volume_mes_serie = None
                    if volume_por_mes is not None and not volume_por_mes.empty:
                        # Filtrar volume para este período exato (sem normalização)
                        vol_mes_df = volume_por_mes[volume_por_mes['Período'].astype(str).str.strip() == str(periodo).strip()].copy()
                        
                        if not vol_mes_df.empty:
                            # Agrupar por Oficina e Veículo
                            vol_mes_df = vol_mes_df.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].sum()
                            
                            # Criar dicionário de volume sem adicionar coluna ao df_forecast_completo
                            volume_dict = {}
                            for _, row in vol_mes_df.iterrows():
                                chave = (str(row['Oficina']), str(row['Veículo']))
                                volume_dict[chave] = float(row['Volume'])
                            
                            # Criar série de volume baseada no índice do df_forecast_completo (sem adicionar coluna)
                            volume_valores = []
                            for idx in df_forecast_completo.index:
                                chave = (str(df_forecast_completo.loc[idx, 'Oficina']), str(df_forecast_completo.loc[idx, 'Veículo']))
                                if chave in volume_dict:
                                    volume_valores.append(volume_dict[chave])
                                elif 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                    volume_valores.append(float(df_forecast_completo.loc[idx, 'Volume_Medio_Historico']))
                                else:
                                    volume_valores.append(0.0)
                            
                            volume_mes_serie = pd.Series(volume_valores, index=df_forecast_completo.index)
                        else:
                            # Verificar se Volume_Medio_Historico existe antes de usar
                            if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                volume_mes_serie = df_forecast_completo['Volume_Medio_Historico'].copy()
                            else:
                                volume_mes_serie = pd.Series(0.0, index=df_forecast_completo.index)
                    else:
                        # Verificar se Volume_Medio_Historico existe antes de usar
                        if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                            volume_mes_serie = df_forecast_completo['Volume_Medio_Historico'].copy()
                        else:
                            volume_mes_serie = pd.Series(0.0, index=df_forecast_completo.index)
                    
                    # Calcular forecast linha a linha (mesma lógica de calcular_forecast_completo)
                    df_forecast_completo[periodo] = 0.0
                    
                    for idx in df_forecast_completo.index:
                            try:
                                # Verificar se as colunas existem antes de acessá-las
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
                                
                                # Verificar se Tipo_Custo existe
                                if 'Tipo_Custo' not in df_forecast_completo.columns:
                                    # Se não existe, usar padrão 'Variável'
                                    tipo_custo = 'Variável'
                                else:
                                    tipo_custo = df_forecast_completo.loc[idx, 'Tipo_Custo']
                                    # Garantir que tipo_custo seja string válida
                                    if pd.isna(tipo_custo) or tipo_custo not in ['Fixo', 'Variável']:
                                        tipo_custo = 'Variável'
                            except Exception as e:
                                # Em caso de erro, usar valores padrão e continuar
                                st.warning(f"⚠️ Erro ao processar linha {idx}: {str(e)}")
                                continue
                            
                            # Calcular proporção de volume
                            if volume_medio_historico > 0:
                                proporcao_volume = volume_mes / volume_medio_historico
                            else:
                                proporcao_volume = 1.0
                            
                            # Calcular variação percentual
                            variacao_percentual = proporcao_volume - 1.0
                            
                            # Obter sensibilidade
                            if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_completo.columns:
                                type06_valor = df_forecast_completo.loc[idx, 'Type 06']
                                if pd.notna(type06_valor) and type06_valor in sensibilidades_type06_dict:
                                    sensibilidade = sensibilidades_type06_dict[type06_valor]
                                else:
                                    sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                            else:
                                sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                            
                            # Aplicar sensibilidade
                            variacao_ajustada = variacao_percentual * sensibilidade
                            
                            # Obter inflação (mesma lógica da função calcular_forecast_completo)
                            if inflacao_type06_dict is not None:
                                # Verificar se está no modo detalhado (tem Type 06 e sensibilidades_type06_dict)
                                if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_completo.columns:
                                    # Modo detalhado: usar inflação específica do Type 06
                                    type06_valor = df_forecast_completo.loc[idx, 'Type 06']
                                    if pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                                        inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0
                                    else:
                                        # Se não encontrar Type 06 específico, usar 'GLOBAL' se existir, senão 0
                                        if 'GLOBAL' in inflacao_type06_dict:
                                            inflacao_percentual = inflacao_type06_dict['GLOBAL'] / 100.0
                                        else:
                                            inflacao_percentual = 0.0
                                else:
                                    # Modo global: usar inflação global (pode estar em 'GLOBAL' ou qualquer chave)
                                    # 🔧 CORREÇÃO: Verificar primeiro se há chave 'GLOBAL', senão pegar primeiro valor
                                    if 'GLOBAL' in inflacao_type06_dict:
                                        inflacao_percentual = inflacao_type06_dict['GLOBAL'] / 100.0
                                    else:
                                        # Se não há 'GLOBAL', pegar o primeiro valor do dicionário
                                        primeiro_valor = next(iter(inflacao_type06_dict.values()), 0.0)
                                        inflacao_percentual = primeiro_valor / 100.0
                            else:
                                # Se inflacao_type06_dict é None, inflação é 0
                                inflacao_percentual = 0.0
                            
                            # Calcular forecast
                            fator_variacao = 1.0 + variacao_ajustada
                            fator_inflacao = 1.0 + inflacao_percentual
                            forecast = media_historica * fator_variacao * fator_inflacao
                            
                            df_forecast_completo.loc[idx, periodo] = forecast
                    
                    # ====================================================================
                    # 🆕 TRANSFORMAR COLUNAS DE FORECAST EM LINHAS NA COLUNA "Período"
                    # ====================================================================
                    # Ao invés de ter colunas separadas para cada período, criar linhas
                    # onde cada linha tem Período = "Novembro 2025", "Dezembro 2025", etc.
                    # IMPORTANTE: Usar valores do df_forecast_bruto (mesma lógica do gráfico)
                    
                    linhas_finais = []
                    
                    # 1. Adicionar linhas históricas (já estão no df_base_filtrado)
                    # 🔧 CORREÇÃO: Remover períodos que serão previstos para não duplicar
                    df_historico_linhas = df_base_filtrado.copy()
                    
                    # Remover colunas de forecast se existirem
                    for periodo in periodos_restantes:
                        if periodo in df_historico_linhas.columns:
                            df_historico_linhas = df_historico_linhas.drop(columns=[periodo])
                    
                    # 🔧 CORREÇÃO: Normalizar Período no histórico para ter apenas o mês (sem ano)
                    # Garantir que o formato seja consistente: Período = "Novembro", Ano = 2025
                    if 'Período' in df_historico_linhas.columns:
                        def normalizar_periodo_historico(periodo_val, ano_val=None):
                            periodo_str = str(periodo_val).strip()
                            # Se o período contém ano, extrair apenas o mês
                            if ' ' in periodo_str:
                                mes_nome = periodo_str.split(' ', 1)[0].strip().capitalize()
                                return mes_nome
                            # Se não contém ano, usar como está
                            return periodo_str.strip().capitalize()
                        
                        # Normalizar Período para ter apenas o mês
                        if 'Ano' in df_historico_linhas.columns:
                            df_historico_linhas['Período'] = df_historico_linhas.apply(
                                lambda row: normalizar_periodo_historico(row['Período'], row.get('Ano')), axis=1
                            )
                        else:
                            df_historico_linhas['Período'] = df_historico_linhas['Período'].apply(
                                lambda p: normalizar_periodo_historico(p)
                            )
                    
                    # 🔧 CORREÇÃO: Sistema de mensagens em expander único
                    # Coletar todas as mensagens de debug/info para exibir em um único expander
                    mensagens_debug = []
                    
                    def adicionar_mensagem(tipo, mensagem):
                        """Adiciona mensagem à lista de debug"""
                        mensagens_debug.append((tipo, mensagem))
                    
                    # 🔧 CORREÇÃO CRÍTICA: Filtrar períodos históricos para remover os que serão previstos
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
                    
                    # Adicionar coluna Tipo = 'Histórico'
                    df_historico_linhas['Tipo'] = 'Histórico'
                    linhas_finais.append(df_historico_linhas)
                    
                    # 2. Criar linhas de forecast para cada período
                    # IMPORTANTE: Usar df_forecast_bruto para garantir valores iguais ao gráfico
                    df_fonte_forecast = None
                    
                    # 🔧 CORREÇÃO: Verificar se df_forecast_bruto existe no escopo global
                    df_forecast_bruto_disponivel = False
                    if 'df_forecast_bruto' in globals() and df_forecast_bruto is not None and not df_forecast_bruto.empty:
                        df_fonte_forecast = df_forecast_bruto.copy()
                        df_forecast_bruto_disponivel = True
                        adicionar_mensagem("info", "✅ Usando valores do df_forecast_bruto (mesma lógica do gráfico)")
                    elif 'df_forecast_bruto' in locals() and df_forecast_bruto is not None and not df_forecast_bruto.empty:
                        df_fonte_forecast = df_forecast_bruto.copy()
                        df_forecast_bruto_disponivel = True
                        adicionar_mensagem("info", "✅ Usando valores do df_forecast_bruto (mesma lógica do gráfico)")
                    else:
                        # Fallback: usar df_forecast_completo com valores calculados
                        df_fonte_forecast = df_forecast_completo.copy()
                        adicionar_mensagem("info", "ℹ️ Usando valores calculados do df_forecast_completo")
                    
                    # 🔧 CORREÇÃO: Fazer merge com histórico para garantir que colunas importantes estejam presentes
                    colunas_importantes_historico = ['Custo', 'Centocst', 'Fornec.', 'Fornecedor', 'USI']
                    if df_fonte_forecast is not None and df_historico_linhas is not None and not df_historico_linhas.empty:
                        # Identificar colunas que existem no histórico mas não no forecast
                        colunas_faltantes = [col for col in colunas_importantes_historico 
                                           if col in df_historico_linhas.columns and col not in df_fonte_forecast.columns]
                        
                        if colunas_faltantes:
                            # 🔧 CORREÇÃO: Tentar merge com mais colunas na chave para melhor matching
                            # Primeiro tentar com Tipo_Custo e Fornec. se disponíveis
                            colunas_merge = ['Oficina', 'Veículo']
                            if 'Ano' in df_historico_linhas.columns and 'Ano' in df_fonte_forecast.columns:
                                colunas_merge.append('Ano')
                            if 'Tipo_Custo' in df_historico_linhas.columns and 'Tipo_Custo' in df_fonte_forecast.columns:
                                colunas_merge.append('Tipo_Custo')
                            if 'Fornec.' in df_historico_linhas.columns and 'Fornec.' in df_fonte_forecast.columns:
                                colunas_merge.append('Fornec.')
                            
                            # Pegar apenas as colunas que existem em ambos
                            colunas_merge_existentes = [col for col in colunas_merge if col in df_historico_linhas.columns and col in df_fonte_forecast.columns]
                            
                            if colunas_merge_existentes:
                                # OTIMIZAÇÃO: Agrupar histórico uma vez para ter valores únicos por chave
                                # Usar groupby().first() que é mais rápido que drop_duplicates para grandes datasets
                                df_historico_agrupado = df_historico_linhas.groupby(
                                    colunas_merge_existentes,
                                    as_index=False
                                ).first()[colunas_merge_existentes + colunas_faltantes]
                                
                                # Fazer merge uma vez (muito mais rápido que buscar para cada linha)
                                df_fonte_forecast = df_fonte_forecast.merge(
                                    df_historico_agrupado,
                                    on=colunas_merge_existentes,
                                    how='left'
                                )
                                
                                # 🔧 CORREÇÃO: Se ainda houver valores faltantes (especialmente Centocst),
                                # tentar merge adicional com chave mais simples (apenas Oficina + Veículo)
                                colunas_ainda_faltantes = [col for col in colunas_faltantes 
                                                          if col in df_fonte_forecast.columns and df_fonte_forecast[col].isna().any()]
                                if colunas_ainda_faltantes:
                                    # Merge de fallback com chave mais simples
                                    colunas_merge_simples = ['Oficina', 'Veículo']
                                    if 'Ano' in df_historico_linhas.columns and 'Ano' in df_fonte_forecast.columns:
                                        colunas_merge_simples.append('Ano')
                                    colunas_merge_simples_existentes = [col for col in colunas_merge_simples 
                                                                       if col in df_historico_linhas.columns and col in df_fonte_forecast.columns]
                                    
                                    if colunas_merge_simples_existentes and len(colunas_merge_simples_existentes) < len(colunas_merge_existentes):
                                        # Apenas preencher valores que ainda estão faltando
                                        df_historico_fallback = df_historico_linhas.groupby(
                                            colunas_merge_simples_existentes,
                                            as_index=False
                                        ).first()[colunas_merge_simples_existentes + colunas_ainda_faltantes]
                                        
                                        # Fazer merge apenas para preencher valores nulos
                                        for col in colunas_ainda_faltantes:
                                            if col in df_historico_fallback.columns:
                                                # Criar série temporária com valores do histórico
                                                serie_historico = df_historico_fallback.set_index(colunas_merge_simples_existentes)[col]
                                                # Preencher apenas valores nulos
                                                mask_nulos = df_fonte_forecast[col].isna()
                                                if mask_nulos.any():
                                                    indices_para_preencher = df_fonte_forecast.loc[mask_nulos, colunas_merge_simples_existentes].apply(
                                                        lambda row: tuple(row), axis=1
                                                    )
                                                    valores_para_preencher = indices_para_preencher.map(serie_historico)
                                                    df_fonte_forecast.loc[mask_nulos, col] = valores_para_preencher.values
                                
                                adicionar_mensagem("info", f"✅ Colunas adicionadas do histórico via merge: {', '.join(colunas_faltantes)}")
                                # Debug: verificar quantos valores foram preenchidos
                                for col in colunas_faltantes:
                                    if col in df_fonte_forecast.columns:
                                        valores_preenchidos = df_fonte_forecast[col].notna().sum()
                                        total_linhas = len(df_fonte_forecast)
                                        adicionar_mensagem("info", f"📊 {col}: {valores_preenchidos:,} de {total_linhas:,} linhas preenchidas ({valores_preenchidos/total_linhas*100:.1f}%)")
                    
                    # 🔧 DEBUG: Verificar se há períodos para criar forecast
                    adicionar_mensagem("info", f"📊 Períodos restantes para criar forecast: {periodos_restantes}")
                    if df_fonte_forecast is not None and not df_fonte_forecast.empty:
                        adicionar_mensagem("info", f"📊 Total de linhas em df_fonte_forecast: {len(df_fonte_forecast)}")
                        adicionar_mensagem("info", f"📊 Colunas disponíveis em df_fonte_forecast: {list(df_fonte_forecast.columns)[:15]}...")
                        colunas_periodos = [col for col in df_fonte_forecast.columns if col in periodos_restantes]
                        adicionar_mensagem("info", f"📊 Colunas de períodos encontradas: {colunas_periodos}")
                        if not colunas_periodos:
                            adicionar_mensagem("warning", f"⚠️ Nenhuma coluna de período encontrada! Períodos esperados: {periodos_restantes}")
                    
                    # OTIMIZAÇÃO: Coletar todas as linhas de forecast em uma lista de dicionários
                    # e criar um único DataFrame no final (muito mais rápido)
                    linhas_forecast_dicts = []
                    linhas_forecast_criadas = 0
                    
                    # 🔧 DEBUG: Verificar períodos e df_fonte_forecast antes do loop
                    adicionar_mensagem("info", f"📊 Iniciando criação de linhas de forecast para {len(periodos_restantes)} períodos")
                    if df_fonte_forecast is None or df_fonte_forecast.empty:
                        adicionar_mensagem("error", f"❌ df_fonte_forecast está None ou vazio! Não é possível criar forecast.")
                    else:
                        adicionar_mensagem("info", f"📊 df_fonte_forecast tem {len(df_fonte_forecast):,} linhas e {len(df_fonte_forecast.columns)} colunas")
                    
                    for periodo in periodos_restantes:
                        adicionar_mensagem("info", f"📊 Processando período: {periodo}")
                        if df_fonte_forecast is not None and periodo in df_fonte_forecast.columns:
                            adicionar_mensagem("info", f"✅ Período '{periodo}' encontrado em df_fonte_forecast")
                            # Para cada linha única, criar uma nova linha com Período = periodo
                            colunas_chave_linha = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
                            if 'Ano' in df_fonte_forecast.columns:
                                colunas_chave_linha.insert(2, 'Ano')
                            colunas_chave_linha = [col for col in colunas_chave_linha if col in df_fonte_forecast.columns]
                            
                            # 🔧 CORREÇÃO: Incluir colunas importantes que devem ser preservadas
                            colunas_importantes = ['Custo', 'Centocst', 'Fornec.', 'Fornecedor', 'USI']
                            colunas_para_preservar = [col for col in colunas_importantes if col in df_fonte_forecast.columns]
                            
                            # Obter linhas únicas com valores de forecast (incluindo colunas importantes)
                            colunas_para_linha = colunas_chave_linha + colunas_para_preservar + [periodo]
                            colunas_para_linha = [col for col in colunas_para_linha if col in df_fonte_forecast.columns]
                            
                            df_linhas_unicas = df_fonte_forecast[colunas_para_linha].drop_duplicates(
                                subset=colunas_chave_linha
                            )
                            
                            # OTIMIZAÇÃO: Usar to_dict('records') em vez de iterrows() (mais rápido)
                            for linha_original in df_linhas_unicas.to_dict('records'):
                                nova_linha = linha_original.copy()
                                
                                # 🔧 CORREÇÃO: Definir Período apenas com o nome do mês (sem ano)
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
                                
                                # 🔧 CORREÇÃO: Sempre usar coluna 'Total' para o valor de forecast
                                valor_forecast = float(nova_linha.get(periodo, 0.0))
                                nova_linha['Total'] = valor_forecast
                                
                                # Se existe coluna Valor, manter (pode ser usado em outros lugares)
                                # Mas o valor principal fica em 'Total'
                                
                                # 🔧 CORREÇÃO: Garantir que colunas importantes sejam preenchidas
                                # OTIMIZAÇÃO: As colunas já foram adicionadas via merge anterior (linha 3751-3777)
                                # Não é necessário buscar novamente no histórico para cada linha individual
                                # O merge já preencheu todas as colunas disponíveis no histórico
                                # Apenas garantir que colunas que não existem no histórico tenham valores padrão
                                for col_imp in colunas_importantes:
                                    if col_imp not in nova_linha:
                                        # Se a coluna não existe, criar com valor None (será tratado depois se necessário)
                                        nova_linha[col_imp] = None
                                
                                # Remover coluna do período (já está em 'Valor')
                                if periodo in nova_linha:
                                    del nova_linha[periodo]
                                
                                # Remover outras colunas de forecast
                                for p in periodos_restantes:
                                    if p in nova_linha and p != periodo:
                                        del nova_linha[p]
                                
                                # Remover colunas auxiliares se existirem
                                if 'Média_Mensal_Histórica' in nova_linha:
                                    del nova_linha['Média_Mensal_Histórica']
                                if 'Volume_Medio_Historico' in nova_linha:
                                    del nova_linha['Volume_Medio_Historico']
                                
                                # Adicionar coluna Tipo = 'Forecast'
                                nova_linha['Tipo'] = 'Forecast'
                                
                                # OTIMIZAÇÃO: Adicionar dicionário à lista (não criar DataFrame ainda)
                                linhas_forecast_dicts.append(nova_linha)
                                linhas_forecast_criadas += 1
                        else:
                            adicionar_mensagem("warning", f"⚠️ Período '{periodo}' não encontrado nas colunas de df_fonte_forecast")
                    
                    # OTIMIZAÇÃO: Criar um único DataFrame de todas as linhas de forecast de uma vez
                    # 🔧 DEBUG: Verificar se há linhas de forecast
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
                    
                    # 🔧 DEBUG: Mostrar quantas linhas de forecast foram criadas
                    adicionar_mensagem("info", f"📊 Linhas de forecast criadas: {linhas_forecast_criadas}")
                    adicionar_mensagem("info", f"📊 Total de DataFrames em linhas_finais: {len(linhas_finais)}")
                    
                    # 3. Separar histórico e forecast em DataFrames distintos
                    df_historico_final = None
                    df_forecast_final = None
                    df_consolidado_final = None
                    
                    if linhas_finais:
                        df_todos = pd.concat(linhas_finais, ignore_index=True)
                        
                        # Garantir que todas as colunas estejam presentes
                        todas_colunas = sorted(set([col for df in linhas_finais for col in df.columns]))
                        df_todos = df_todos.reindex(columns=todas_colunas)
                        
                        # Separar histórico e forecast
                        if 'Tipo' in df_todos.columns:
                            df_historico_final = df_todos[df_todos['Tipo'] == 'Histórico'].copy()
                            df_forecast_final = df_todos[df_todos['Tipo'] == 'Forecast'].copy()
                            df_consolidado_final = df_todos.copy()
                            
                            # 🔧 DEBUG: Verificar separação
                            adicionar_mensagem("info", f"🔍 DEBUG: Total de linhas em df_todos: {len(df_todos):,}")
                            tipos_unicos = df_todos['Tipo'].unique()
                            adicionar_mensagem("info", f"🔍 DEBUG: Valores únicos na coluna 'Tipo': {tipos_unicos}")
                            for tipo in tipos_unicos:
                                count = len(df_todos[df_todos['Tipo'] == tipo])
                                adicionar_mensagem("info", f"🔍 DEBUG: Linhas com Tipo='{tipo}': {count:,}")
                            adicionar_mensagem("info", f"🔍 DEBUG: df_historico_final: {len(df_historico_final):,} linhas")
                            adicionar_mensagem("info", f"🔍 DEBUG: df_forecast_final: {len(df_forecast_final):,} linhas")
                        else:
                            # Se não tem coluna Tipo, assumir que tudo é histórico
                            adicionar_mensagem("warning", f"⚠️ Coluna 'Tipo' não encontrada em df_todos! Assumindo que tudo é histórico.")
                            adicionar_mensagem("info", f"🔍 DEBUG: Colunas disponíveis: {list(df_todos.columns)[:15]}...")
                            df_historico_final = df_todos.copy()
                            df_forecast_final = pd.DataFrame()
                            df_consolidado_final = df_todos.copy()
                        
                        # Substituir df_forecast_completo pelo resultado consolidado
                        df_forecast_completo = df_consolidado_final
                        
                        adicionar_mensagem("info", f"✅ Tabela criada com {len(df_consolidado_final):,} linhas (histórico + forecast)")
                        adicionar_mensagem("info", f"📊 Histórico: {len(df_historico_final):,} linhas")
                        adicionar_mensagem("info", f"📊 Forecast: {len(df_forecast_final):,} linhas")
                        adicionar_mensagem("info", f"📊 Períodos de forecast incluídos: {', '.join(periodos_restantes)}")
                        
                        # Debug: Verificar períodos únicos e anos
                        if 'Período' in df_forecast_completo.columns:
                            periodos_unicos = df_forecast_completo['Período'].unique()
                            adicionar_mensagem("info", f"📅 Períodos únicos no arquivo: {len(periodos_unicos)} períodos")
                            adicionar_mensagem("info", f"📅 Lista de períodos: {sorted(periodos_unicos)}")
                            if 'Ano' in df_forecast_completo.columns:
                                anos_unicos = df_forecast_completo['Ano'].unique()
                                adicionar_mensagem("info", f"📅 Anos únicos no arquivo: {sorted(anos_unicos)}")
                            
                            # Verificar se há períodos históricos
                            if 'Tipo' in df_forecast_completo.columns:
                                periodos_historicos = df_forecast_completo[df_forecast_completo['Tipo'] == 'Histórico']['Período'].unique()
                                periodos_forecast = df_forecast_completo[df_forecast_completo['Tipo'] == 'Forecast']['Período'].unique()
                                adicionar_mensagem("info", f"📊 Períodos históricos: {len(periodos_historicos)} períodos")
                                adicionar_mensagem("info", f"📊 Períodos de forecast: {len(periodos_forecast)} períodos")
                    else:
                        adicionar_mensagem("warning", "⚠️ Nenhuma linha foi criada!")
                        # Se não criou linhas, usar df_base_filtrado como base
                        df_historico_final = df_base_filtrado.copy()
                        df_historico_final['Tipo'] = 'Histórico'
                        df_forecast_final = pd.DataFrame()
                        df_forecast_completo = df_historico_final.copy()
                        adicionar_mensagem("info", "ℹ️ Usando apenas dados históricos (sem forecast)")
                    
                    # Verificar se df_forecast_completo existe e não está vazio
                    if df_forecast_completo is None or df_forecast_completo.empty:
                        st.error("❌ Erro: DataFrame vazio! Não é possível salvar.")
                        st.stop()
                    
                    # 🔧 CORREÇÃO: Aplicar limpeza também nos DataFrames separados
                    def limpar_dataframe(df):
                        """Aplica limpeza de colunas e linhas em um DataFrame"""
                        if df is None or df.empty:
                            return df
                        
                        df_limpo = df.copy()
                        
                        # Remover colunas especificadas (MAS NUNCA REMOVER 'Custo')
                        colunas_para_remover = ['Nºconta', 'Nºdoc.ref.', 'Dt.lçto.', 'QTD', 'Nºdoc.ref', 'Doc.compra', 'Texto breve', 'Material', 'Usuário']
                        colunas_normalizacao = ['Período_Norm', 'Período_Normalizado', 'Period_Norm', 'Period_Normalizado']
                        colunas_para_remover.extend(colunas_normalizacao)
                        colunas_para_remover = [col for col in colunas_para_remover if col != 'Custo']
                        colunas_para_remover_existentes = [col for col in colunas_para_remover if col in df_limpo.columns]
                        if colunas_para_remover_existentes:
                            df_limpo = df_limpo.drop(columns=colunas_para_remover_existentes)
                        
                        # Remover linhas onde Total é nulo ou zero (sempre usar Total)
                        if 'Total' in df_limpo.columns:
                            # Verificar apenas 'Total' (sempre usar Total)
                            mask_valor_valido = df_limpo['Total'].notna() & (df_limpo['Total'] != 0)
                            df_limpo = df_limpo[mask_valor_valido].copy()
                        # Se não tiver Total, não remover linhas por esse critério
                        
                        # Remover linhas onde colunas críticas são todas nulas
                        colunas_criticas = ['Oficina', 'Veículo', 'Período']
                        colunas_criticas_existentes = [col for col in colunas_criticas if col in df_limpo.columns]
                        if colunas_criticas_existentes:
                            mask_linhas_validas = df_limpo[colunas_criticas_existentes].notna().any(axis=1)
                            df_limpo = df_limpo[mask_linhas_validas].copy()
                        
                        # Remover colunas completamente nulas
                        colunas_todas_nulas = df_limpo.columns[df_limpo.isna().all()].tolist()
                        if colunas_todas_nulas:
                            df_limpo = df_limpo.drop(columns=colunas_todas_nulas)
                        
                        return df_limpo
                    
                    # Aplicar limpeza nos DataFrames separados
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
                    
                    # Aplicar limpeza no consolidado
                    linhas_antes = len(df_forecast_completo)
                    df_forecast_completo = limpar_dataframe(df_forecast_completo)
                    linhas_depois = len(df_forecast_completo)
                    linhas_removidas = linhas_antes - linhas_depois
                    
                    if linhas_removidas > 0:
                        adicionar_mensagem("info", f"🧹 Consolidado: Removidas {linhas_removidas:,} linhas com valores nulos/zerados (de {linhas_antes:,} para {linhas_depois:,})")
                    
                    # 🔧 CORREÇÃO: Padronizar colunas para garantir mesma ordem e nomes consistentes
                    def padronizar_colunas(df, nome_tipo="DataFrame"):
                        """Padroniza colunas do DataFrame para garantir ordem e nomes consistentes"""
                        if df is None or df.empty:
                            return df
                        
                        df_padronizado = df.copy()
                        
                        # Definir ordem padrão das colunas (colunas principais primeiro)
                        ordem_colunas_principal = [
                            'Oficina', 'Veículo', 'Ano', 'Período', 'Tipo_Custo', 'Custo',
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
                    
                    # Padronizar colunas de histórico e forecast ANTES de combinar
                    if df_historico_final is not None and not df_historico_final.empty:
                        df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
                    
                    if df_forecast_final is not None and not df_forecast_final.empty:
                        df_forecast_final = padronizar_colunas(df_forecast_final, "Forecast")
                    
                    # Atualizar consolidado após limpeza e padronização (recombinar histórico e forecast limpos)
                    if df_historico_final is not None and df_forecast_final is not None:
                        if not df_historico_final.empty and not df_forecast_final.empty:
                            # Garantir que ambos tenham exatamente as mesmas colunas na mesma ordem
                            todas_colunas_limpas = sorted(set(list(df_historico_final.columns) + list(df_forecast_final.columns)))
                            
                            # Reindexar ambos para ter as mesmas colunas na mesma ordem
                            df_historico_final = df_historico_final.reindex(columns=todas_colunas_limpas)
                            df_forecast_final = df_forecast_final.reindex(columns=todas_colunas_limpas)
                            
                            # Padronizar novamente após reindex para garantir ordem correta
                            df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
                            df_forecast_final = padronizar_colunas(df_forecast_final, "Forecast")
                            
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
                    
                    # Criar pasta Forecast em dados/Forecast (ANTES de tentar salvar)
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
                    
                    # Usar nome fixo para substituir arquivo existente (não usar timestamp)
                    nome_arquivo_base = "forecast_completo"
                    
                    # ============================================================
                    # PASSO 1: Copiar arquivo completo de volume histórico
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
                    
                    # ============================================================
                    # PASSO 2: Salvar arquivos separados (histórico, forecast, consolidado)
                    # ============================================================
                    
                    # Função auxiliar para salvar arquivo (parquet e excel)
                    def salvar_arquivo(df, nome_base, descricao):
                        """Salva DataFrame em parquet e excel - retorna informações para exibição"""
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
                        
                        # Salvar Parquet
                        caminho_parquet = os.path.join(pasta_forecast, f"{nome_base}.parquet")
                        try:
                            df.to_parquet(caminho_parquet, index=False, engine='pyarrow')
                            if os.path.exists(caminho_parquet):
                                tamanho = os.path.getsize(caminho_parquet) / (1024 * 1024)
                                info_parquet = f"✅ {descricao} Parquet: {tamanho:.2f} MB, {len(df):,} linhas"
                                sucesso_parquet = True
                        except Exception as e:
                            info_parquet = f"❌ Erro ao salvar {descricao} Parquet: {str(e)}"
                        
                        # Salvar Excel
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
                    
                    # 🔧 CORREÇÃO: Garantir que os DataFrames estejam padronizados ANTES de salvar
                    # Isso garante que histórico e forecast tenham as mesmas colunas na mesma ordem
                    if df_historico_final is not None and not df_historico_final.empty:
                        df_historico_final = padronizar_colunas(df_historico_final, "Histórico")
                    if df_forecast_final is not None and not df_forecast_final.empty:
                        df_forecast_final = padronizar_colunas(df_forecast_final, "Forecast")
                    if df_forecast_completo is not None and not df_forecast_completo.empty:
                        df_forecast_completo = padronizar_colunas(df_forecast_completo, "Consolidado")
                    
                    # Verificar se histórico e forecast têm as mesmas colunas na mesma ordem
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
                            df_forecast_final = padronizar_colunas(df_forecast_final, "Forecast")
                            adicionar_mensagem("info", f"✅ Colunas alinhadas e padronizadas: {len(todas_colunas)} colunas")
                    
                    # Salvar arquivos e coletar informações
                    info_historico = salvar_arquivo(df_historico_final, "forecast_historico", "Histórico")
                    info_forecast = salvar_arquivo(df_forecast_final, "forecast_previsao", "Forecast")
                    info_consolidado = salvar_arquivo(df_forecast_completo, nome_arquivo_base, "Consolidado")
                    
                    # 🔧 CORREÇÃO: Exibir todas as mensagens de debug em um único expander
                    if mensagens_debug:
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
                    
                    # Exibir status em expander compacto
                    with st.expander("📊 Status de Salvamento dos Arquivos", expanded=False):
                        st.markdown(f"<small>📁 Pasta: {os.path.abspath(pasta_forecast)}</small>", unsafe_allow_html=True)
                        st.markdown("<small>---</small>", unsafe_allow_html=True)
                        
                        if info_historico['sucesso']:
                            st.markdown(f"<small>{info_historico['parquet']}</small>", unsafe_allow_html=True)
                            if info_historico['excel']:
                                st.markdown(f"<small>{info_historico['excel']}</small>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<small>{info_historico.get('mensagem', 'Erro desconhecido')}</small>", unsafe_allow_html=True)
                        
                        st.markdown("<small>---</small>", unsafe_allow_html=True)
                        
                        if info_forecast['sucesso']:
                            st.markdown(f"<small>{info_forecast['parquet']}</small>", unsafe_allow_html=True)
                            if info_forecast['excel']:
                                st.markdown(f"<small>{info_forecast['excel']}</small>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<small>{info_forecast.get('mensagem', 'Erro desconhecido')}</small>", unsafe_allow_html=True)
                        
                        st.markdown("<small>---</small>", unsafe_allow_html=True)
                        
                        if info_consolidado['sucesso']:
                            st.markdown(f"<small>{info_consolidado['parquet']}</small>", unsafe_allow_html=True)
                            if info_consolidado['excel']:
                                st.markdown(f"<small>{info_consolidado['excel']}</small>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<small>{info_consolidado.get('mensagem', 'Erro desconhecido')}</small>", unsafe_allow_html=True)
                        
                        st.markdown("<small>---</small>", unsafe_allow_html=True)
                        st.markdown(f"<small>📊 Histórico: {info_historico.get('linhas', 0):,} linhas | Forecast: {info_forecast.get('linhas', 0):,} linhas | Consolidado: {info_consolidado.get('linhas', 0):,} linhas</small>", unsafe_allow_html=True)
                    
                    adicionar_mensagem("success", f"✅ Tabela completa gerada com sucesso!")
                    
                    # ====================================================================
                    # 🆕 OTIMIZAÇÃO: Usar arquivos já salvos para criar consolidado (muito mais rápido)
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
                                
                                adicionar_mensagem("info", f"📊 Histórico carregado: {len(df_historico_carregado):,} linhas")
                                adicionar_mensagem("info", f"📊 Forecast carregado: {len(df_forecast_carregado):,} linhas")
                                
                                # OTIMIZAÇÃO: Juntar diretamente sem reprocessar (muito mais rápido)
                                # Garantir que todas as colunas estejam presentes
                                todas_colunas_consolidado = sorted(set(list(df_historico_carregado.columns) + list(df_forecast_carregado.columns)))
                                df_historico_carregado = df_historico_carregado.reindex(columns=todas_colunas_consolidado)
                                df_forecast_carregado = df_forecast_carregado.reindex(columns=todas_colunas_consolidado)
                                
                                # Combinar (muito rápido - apenas concat)
                                df_consolidado_final = pd.concat([df_historico_carregado, df_forecast_carregado], ignore_index=True)
                                
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
                    
                    # ====================================================================
                    # CÓDIGO ANTIGO (MUITO LENTO) - REMOVIDO PARA OTIMIZAÇÃO
                    # O código antigo foi substituído pela versão otimizada acima
                    # que simplesmente carrega os arquivos já salvos e os junta
                    # ====================================================================
                    # O código antigo foi completamente removido para melhorar performance
                    # A versão otimizada carrega os arquivos já salvos e os junta diretamente
                    # Isso é muito mais rápido que reprocessar tudo novamente
                    
                    # Limpar flag
                    st.session_state.gerar_tabela_completa_forecast = False
                    
                    # 🔧 CORREÇÃO: Exibir todas as mensagens de debug em um único expander
                    # IMPORTANTE: Exibir DEPOIS de todas as operações para garantir que todas as mensagens sejam coletadas
                    if 'mensagens_debug' in locals() and mensagens_debug:
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
                    
        except Exception as e:
            # Se mensagens_debug existe, adicionar erro; senão, usar st.error (fallback)
            if 'mensagens_debug' in locals():
                adicionar_mensagem("error", f"❌ Erro ao gerar tabela completa: {str(e)}")
                import traceback
                adicionar_mensagem("error", f"Detalhes: {traceback.format_exc()}")
            else:
                st.error(f"❌ Erro ao gerar tabela completa: {str(e)}")
                import traceback
                st.error(f"Detalhes: {traceback.format_exc()}")
            st.session_state.gerar_tabela_completa_forecast = False
    
    # Não há ajustes manuais: o cálculo linha a linha garante que os valores estão corretos
    # Total_Forecast será calculado depois que colunas_meses for definido
    
    # Função para processar e formatar tabela com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def processar_tabela_forecast(df_forecast_cache, colunas_adicionais_cache, meses_restantes_cache):
        """Processa e formata a tabela de forecast com cache"""
        # Reordenar colunas
        colunas_ordenadas = ['Oficina', 'Veículo'] + colunas_adicionais_cache + ['Tipo_Custo', 'Média_Mensal_Histórica'] + meses_restantes_cache
        colunas_existentes = [col for col in colunas_ordenadas if col in df_forecast_cache.columns]
        df_forecast_processado = df_forecast_cache[colunas_existentes].copy()
        
        # Verificar duplicatas ANTES do agrupamento
        colunas_chave_antes_agrupamento = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo']
        if 'Ano' in df_forecast_processado.columns:
            colunas_chave_antes_agrupamento.insert(2, 'Ano')
        colunas_chave_antes_existentes = [col for col in colunas_chave_antes_agrupamento if col in df_forecast_processado.columns]
        
        if len(colunas_chave_antes_existentes) > 0 and len(df_forecast_processado) > 0:
            duplicatas_antes = df_forecast_processado.duplicated(subset=colunas_chave_antes_existentes, keep=False)
            num_duplicatas_antes = duplicatas_antes.sum()
            if num_duplicatas_antes > 0:
                st.sidebar.error(f"❌ PROBLEMA CRÍTICO: {num_duplicatas_antes} linhas duplicadas ANTES do agrupamento!")
                # Mostrar exemplo de duplicatas
                linhas_dup = df_forecast_processado[duplicatas_antes]
                if len(linhas_dup) > 0:
                    exemplo_dup = linhas_dup.iloc[0]
                    mask_exemplo = True
                    for col in colunas_chave_antes_existentes:
                        mask_exemplo = mask_exemplo & (df_forecast_processado[col] == exemplo_dup[col])
                    linhas_exemplo = df_forecast_processado[mask_exemplo]
                    if len(linhas_exemplo) > 1:
                        st.sidebar.write(f"**Exemplo:** {len(linhas_exemplo)} linhas com mesma chave:")
                        for idx, row in linhas_exemplo.head(3).iterrows():
                            valores_meses = [row[col] for col in meses_restantes_cache if col in row.index]
                            soma_meses_exemplo = sum([v for v in valores_meses if isinstance(v, (int, float))])
                            st.sidebar.write(f"  - Linha {idx}: soma meses = {soma_meses_exemplo:,.2f}")
        
        # Calcular total por linha e identificar colunas de meses
        colunas_meses = [col for col in meses_restantes_cache if col in df_forecast_processado.columns]
        if colunas_meses:
            df_forecast_processado['Total_Forecast'] = df_forecast_processado[colunas_meses].sum(axis=1)
        
        # Agrupar linhas iguais (mesma combinação de Oficina+Veículo+Type+Tipo_Custo)
        # 🔧 CORREÇÃO CRÍTICA: Se houver coluna 'Ano', incluí-la no agrupamento para evitar
        # agrupar linhas de anos diferentes (ex: 2024 e 2025) que devem ser tratadas separadamente
        colunas_agrupamento = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo']
        # Incluir 'Ano' no agrupamento se existir (evita agrupar dados de 2024 com 2025)
        if 'Ano' in df_forecast_processado.columns:
            colunas_agrupamento.append('Ano')
        colunas_agrupamento_existentes = [col for col in colunas_agrupamento if col in df_forecast_processado.columns]
        
        if len(colunas_agrupamento_existentes) > 0:
            # Agrupar: somar valores numéricos (forecasts), somar Média_Mensal_Histórica também
            # NÃO somar Total_Forecast aqui, vamos recalcular depois
            agg_dict_grupo = {}
            for col in df_forecast_processado.columns:
                if col not in colunas_agrupamento_existentes:
                    if col == 'Média_Mensal_Histórica':
                        # 🔧 CORREÇÃO: Ao agrupar, somar as médias históricas (não usar 'first')
                        # porque linhas duplicadas devem ter suas médias somadas para manter a consistência
                        # com a média histórica total padronizada
                        agg_dict_grupo[col] = 'sum'  # Somar médias históricas ao agrupar
                    elif col in colunas_meses:
                        agg_dict_grupo[col] = 'sum'  # Somar forecasts dos meses
                    elif col == 'Total_Forecast':
                        # Não incluir Total_Forecast no agrupamento, vamos recalcular
                        pass
                    else:
                        agg_dict_grupo[col] = 'first'
            df_forecast_processado = df_forecast_processado.groupby(
                colunas_agrupamento_existentes, as_index=False
            ).agg(agg_dict_grupo).reset_index()
            
            # Recalcular Total_Forecast após agrupamento (soma dos meses agrupados)
            if colunas_meses:
                df_forecast_processado['Total_Forecast'] = df_forecast_processado[colunas_meses].sum(axis=1)
        
        # Remover linhas com valores zero
        if colunas_meses:
            soma_meses = df_forecast_processado[colunas_meses].sum(axis=1)
            df_forecast_processado = df_forecast_processado[soma_meses > 0.01].copy()
        
        # Ordenar
        colunas_ordenacao = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo']
        df_forecast_processado = df_forecast_processado.sort_values(colunas_ordenacao)
        
        return df_forecast_processado, colunas_meses

    # Processar tabela com cache (precisa ser feito antes dos gráficos)
    # IMPORTANTE: df_forecast_bruto já foi criado ANTES deste processamento, então contém todas as linhas
    df_forecast, colunas_meses = processar_tabela_forecast(df_forecast, colunas_adicionais, periodos_restantes)
    
    # ====================================================================
    
    # 🔧 CORREÇÃO CRÍTICA: Calcular Total_Forecast no df_forecast_bruto DEPOIS de ter colunas_meses
    # Isso garante que os totais sejam calculados corretamente somando todas as linhas individuais
    # O df_forecast_bruto contém todas as linhas ANTES do agrupamento
    if colunas_meses and all(mes in df_forecast_bruto.columns for mes in colunas_meses):
        df_forecast_bruto['Total_Forecast'] = df_forecast_bruto[colunas_meses].sum(axis=1)
    
    
    # Não há ajustes manuais: o cálculo linha a linha garante que os valores estão corretos
    # O agrupamento na função processar_tabela_forecast soma as médias históricas corretamente
    
    # Gráficos e tabelas removidos - apenas cálculos são mantidos
    # Todos os gráficos e tabelas foram removidos conforme solicitado
    # Apenas os cálculos e a geração de arquivos são mantidos
    
    # ====================================================================
    # 📊 GRÁFICO "SOMA DO VALOR POR PERÍODO" - REMOVIDO (DUPLICADO)
    # Este gráfico já está sendo gerado na linha 1974 quando não há configurações aplicadas
    # Não é necessário gerar novamente aqui para evitar duplicação
    # ====================================================================
    # O gráfico foi removido - já está sendo gerado na seção anterior (linha 1974)
    pass  # Gráfico duplicado removido - todo o código duplicado foi removido

# ====================================================================
# ====================================================================
# GRÁFICO REMOVIDO - Já está sendo gerado nas seções anteriores
# (linha 1730 quando não há configurações aplicadas, linha 4986 quando há)
# Esta seção foi removida para evitar duplicação de gráficos
# ====================================================================

# Footer
st.markdown("---")
st.info("💡 Forecast TC - Análise preditiva e previsões")

