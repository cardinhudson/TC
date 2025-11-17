import streamlit as st
import pandas as pd
import altair as alt
import os
import numpy as np
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Forecast - Previsões TC",
    page_icon="🔮",
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
st.title("🔮 Forecast - Previsões TC")
st.subheader("Análise preditiva e previsões de custos e volumes")

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

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=0,  # "Todos" por padrão
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico"
)

st.sidebar.markdown("---")

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
        arquivo_parquet = encontrar_arquivo_parquet("df_final.parquet", ano_para_busca)

        if arquivo_parquet is None:
            st.error(f"❌ Arquivo não encontrado: df_final.parquet")
            st.info("💡 Verifique se o arquivo existe em:")
            st.info("   - dados/historico_consolidado/df_final_historico.parquet")
            st.info("   - dados/{ANO}/df_final.parquet")
            st.info("   - df_final.parquet (raiz)")
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

@st.cache_data(ttl=3600, show_spinner=False)
def load_volume_historico_data():
    """Carrega os dados de volume histórico consolidado do arquivo parquet"""
    try:
        # Buscar arquivo na pasta dados/historico_consolidado
        caminho_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        caminho_historico = os.path.join(caminho_base, "dados", "historico_consolidado", "df_vol_historico.parquet")
        
        if os.path.exists(caminho_historico):
            df = pd.read_parquet(caminho_historico)
            
            # Otimizar tipos de dados
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        unique_ratio = df[col].nunique() / len(df)
                        if unique_ratio < 0.5:
                            df[col] = df[col].astype('category')
                    except:
                        pass
            
            # Converter floats para tipos menores
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = pd.to_numeric(df[col], downcast='float')
            
            # Converter ints para tipos menores
            for col in df.select_dtypes(include=['int64']).columns:
                df[col] = pd.to_numeric(df[col], downcast='integer')
            
            return df
        else:
            return None
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
    except:
        pass
    
    # Funções que podem não existir se houver colunas faltando
    try:
        calcular_medias_forecast.clear()
        calcular_volumes_cpu.clear()
        calcular_forecast_completo.clear()
        processar_tabela_forecast.clear()
    except:
        pass
    
    st.rerun()


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
        if periodo_selecionado_cache and periodo_selecionado_cache != "Todos":
            df_filtrado = df_filtrado[
                df_filtrado['Período'].astype(str) == str(periodo_selecionado_cache)
            ].copy()
    
    return df_filtrado

# Filtro 1: Oficina
oficina_selecionadas = ["Todos"]
if 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=["Todos"]
    )

# Filtro 2: Veículo
veiculo_selecionados = ["Todos"]
if 'Veículo' in df_total.columns:
    # Usar df_total para opções, mas depois filtrar
    veiculo_opcoes = get_filter_options(df_total, 'Veículo')
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=["Todos"]
    )

# Filtro 3: USI
usi_selecionada = ["TC Ext"]
if 'USI' in df_total.columns:
    usi_opcoes = get_filter_options(df_total, 'USI')
    default_usi = ["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"]
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi
    )

# Filtro 4: Período
periodo_selecionado = "Todos"
if 'Período' in df_total.columns:
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
    
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes
    )

# Aplicar todos os filtros com cache
df_filtrado = aplicar_filtros(
    df_total,
    tuple(oficina_selecionadas) if oficina_selecionadas else tuple(),
    tuple(veiculo_selecionados) if veiculo_selecionados else tuple(),
    tuple(usi_selecionada) if usi_selecionada else tuple(),
    periodo_selecionado
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

# Área principal - Forecast
st.markdown("## 📈 Forecast - Previsão de Custo Total")

# ====================================================================
# 🔮 CONFIGURAÇÃO DO FORECAST - PRIMEIRO (antes dos sliders)
# ====================================================================
st.markdown("### 🔮 Configuração do Forecast")

# Lista de meses do ano (necessária para a configuração)
meses_ano = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

# Verificar se temos dados com múltiplos anos
tem_anos = 'Ano' in df_filtrado.columns and df_filtrado['Ano'].nunique() > 1

# Determinar o ano dos dados
if tem_anos and 'Ano' in df_filtrado.columns:
    anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique())
    ano_maximo = int(df_filtrado['Ano'].max())
else:
    from datetime import datetime
    anos_disponiveis = [datetime.now().year]
    ano_maximo = datetime.now().year

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

# Criar lista de períodos disponíveis com ano (baseado nos dados reais)
periodos_disponiveis = []
if 'Período' in df_filtrado.columns:
    # Pegar períodos únicos dos dados
    periodos_unicos = df_filtrado['Período'].dropna().unique()
    
    # Verificar se os períodos já têm ano ou não
    periodos_com_ano = any(' ' in str(p) and str(p).split(' ', 1)[1].isdigit() for p in periodos_unicos)
    
    # Se não tiver ano nos períodos mas temos múltiplos anos, adicionar ano
    if not periodos_com_ano and tem_anos:
        # Criar períodos com ano baseado no ano dos dados
        periodos_com_ano_lista = []
        for periodo in periodos_unicos:
            periodo_str = str(periodo).strip()
            # Capitalizar primeira letra
            periodo_capitalizado = periodo_str.capitalize() if periodo_str else periodo_str
            # Adicionar ano máximo (ano dos dados)
            periodo_com_ano = f"{periodo_capitalizado} {ano_maximo}"
            periodos_com_ano_lista.append(periodo_com_ano)
        periodos_disponiveis = sorted(periodos_com_ano_lista, key=lambda x: ordenar_periodo_para_select(x))
    else:
        # Se já tem ano ou não tem múltiplos anos, usar como está
        periodos_disponiveis = sorted(periodos_unicos, key=lambda x: ordenar_periodo_para_select(x))
else:
    # Fallback: criar períodos baseado nos meses e anos disponíveis
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
    
    indice_ultimo_mes = meses_ano.index(ultimo_mes_dados) if ultimo_mes_dados in meses_ano else 0
    
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
    meses_historicos_disponiveis = meses_ano[:indice_ultimo_mes + 1]
    
    num_meses_media = st.number_input(
        "📈 Quantos meses usar para a média:",
        min_value=1,
        max_value=len(meses_historicos_disponiveis) if meses_historicos_disponiveis else 12,
        value=min(len(meses_historicos_disponiveis), 6) if meses_historicos_disponiveis else 6,
        step=1,
        help="Número de meses históricos para calcular a média"
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
periodos_para_media = []
meses_para_media = []

if meses_historicos_disponiveis:
    meses_considerados = meses_historicos_disponiveis.copy()
    
    # Remover meses excluídos
    for mes_excluir in meses_excluir_media:
        if mes_excluir in meses_considerados:
            meses_considerados.remove(mes_excluir)
    
    # Pegar os últimos N meses (após excluir)
    if meses_considerados:
        meses_para_media = meses_considerados[-num_meses_media:] if len(meses_considerados) >= num_meses_media else meses_considerados
        
        # Criar períodos com ano se necessário
        if tem_anos:
            for mes in meses_para_media:
                periodo_com_ano = f"{mes} {ultimo_ano_dados}"
                periodos_para_media.append(periodo_com_ano)
        else:
            periodos_para_media = meses_para_media.copy()
    else:
        meses_para_media = []
        periodos_para_media = []
else:
    meses_para_media = []
    periodos_para_media = []

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
            
            # Criar dicionário de inflação global (aplicar a todos)
            if inflacao_global > 0 and 'Type 06' in df_filtrado.columns:
                type06_valores_global = df_filtrado['Type 06'].dropna().unique().tolist()
                inflacao_type06 = {type06: inflacao_global for type06 in type06_valores_global}
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
    
    # Limpar cache das funções de forecast
    try:
        calcular_medias_forecast.clear()
        calcular_volumes_cpu.clear()
        calcular_forecast_completo.clear()
        processar_tabela_forecast.clear()
    except:
        pass
    
    st.success("✅ Configurações aplicadas com sucesso! Recalculando forecast...")
    st.rerun()

# Usar configurações aplicadas (se existirem) ou temporárias
if st.session_state.config_forecast_aplicada['ultimo_periodo_dados'] is not None:
    # Usar configurações aplicadas
    ultimo_periodo_dados = st.session_state.config_forecast_aplicada['ultimo_periodo_dados']
    num_meses_prever = st.session_state.config_forecast_aplicada['num_meses_prever']
    num_meses_media = st.session_state.config_forecast_aplicada['num_meses_media']
    meses_excluir_media = st.session_state.config_forecast_aplicada['meses_excluir_media']
    periodos_restantes = st.session_state.config_forecast_aplicada['periodos_restantes']
    periodos_para_media = st.session_state.config_forecast_aplicada['periodos_para_media']
    ultimo_ano_dados = st.session_state.config_forecast_aplicada['ultimo_ano_dados']
    
    # Recalcular índices e meses baseados nas configurações aplicadas
    if ' ' in str(ultimo_periodo_dados):
        ultimo_mes_dados = str(ultimo_periodo_dados).split(' ', 1)[0]
    else:
        ultimo_mes_dados = str(ultimo_periodo_dados)
    ultimo_mes_dados = ultimo_mes_dados.capitalize()
    indice_ultimo_mes = meses_ano.index(ultimo_mes_dados) if ultimo_mes_dados in meses_ano else 0
else:
    # Primeira vez - usar configurações temporárias mas não calcular ainda
    st.info("ℹ️ Configure os parâmetros acima e clique em 'Aplicar Configurações do Forecast' para calcular o forecast.")
    st.stop()

st.markdown("---")

# Carregar dados de volume
df_vol = load_volume_data(ano_selecionado)

# Carregar dados de volume histórico (prioritário para meses futuros)
df_vol_historico = load_volume_historico_data()

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

# Verificar se temos as colunas necessárias
colunas_necessarias = ['Oficina', 'Veículo', 'Período', 'Total', 'Custo']
colunas_faltando = [col for col in colunas_necessarias if col not in df_filtrado.columns]

if colunas_faltando:
    st.error(f"❌ Colunas necessárias não encontradas: {', '.join(colunas_faltando)}")
    st.info("ℹ️ Certifique-se de que o arquivo df_final.parquet contém todas as colunas necessárias.")
else:
    # Função para identificar se é custo fixo ou variável
    def is_custo_fixo(valor_custo):
        """Identifica se o custo é fixo baseado no valor da coluna Custo"""
        if pd.isna(valor_custo):
            return False
        valor_str = str(valor_custo).strip().upper()
        # Considerar como fixo se contém palavras-chave
        palavras_fixo = ['FIXO', 'FIX', 'FIXED']
        return any(palavra in valor_str for palavra in palavras_fixo)
    
    # Criar coluna indicando se é fixo ou variável
    df_filtrado['Tipo_Custo'] = df_filtrado['Custo'].apply(is_custo_fixo)
    df_filtrado['Tipo_Custo'] = df_filtrado['Tipo_Custo'].map({True: 'Fixo', False: 'Variável'})
    
    # Validação: verificar se há períodos para calcular a média
    # Se não houver períodos configurados, tentar usar períodos disponíveis nos dados
    if not periodos_para_media:
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
                    periodos_para_media = periodos_encontrados[-num_meses_media:] if len(periodos_encontrados) >= num_meses_media else periodos_encontrados
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
    if not periodos_restantes:
        st.error("❌ **Erro de Configuração:** Nenhum período selecionado para prever.")
        st.info("💡 Ajuste a configuração do forecast na sidebar:")
        st.info("   - Selecione o último mês com dados reais")
        st.info("   - Defina quantos meses prever")
        st.stop()
    
    # Função para calcular médias com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_medias_forecast(df_filtrado_cache, colunas_adicionais_cache, periodos_para_media_cache, ultimo_periodo_dados_cache=None):
        """Calcula médias mensais históricas com cache, usando apenas os períodos selecionados"""
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
            
            df_filtrado_media = df_filtrado_cache[
                periodos_no_df.apply(periodo_corresponde)
            ].copy()
            
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
        
        # Agrupar por Oficina, Veículo, Período e Tipo_Custo para obter totais
        # 🔧 CORREÇÃO: Usar 'sum' em vez de 'mean' para ter os valores totais reais
        # A média mensal será calculada depois em df_media_mensal
        colunas_groupby = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais_cache
        colunas_groupby = [col for col in colunas_groupby if col in df_filtrado_media.columns]
        agg_dict = {'Total': 'sum'}  # Usar 'sum' para ter valores totais reais
        df_medias = df_filtrado_media.groupby(colunas_groupby).agg(agg_dict).reset_index()
        
        # 🔧 CORREÇÃO: Filtrar apenas o ano mais recente antes de calcular média mensal
        # Isso evita misturar dados de 2024 e 2025 no mesmo cálculo
        if 'Ano' in df_medias.columns:
            ano_mais_recente = df_medias['Ano'].max()
            df_medias_ano_recente = df_medias[df_medias['Ano'] == ano_mais_recente].copy()
        elif ultimo_periodo_dados_cache:
            # Extrair ano do último período se disponível
            ultimo_periodo_str = str(ultimo_periodo_dados_cache).strip()
            if ' ' in ultimo_periodo_str:
                ano_str = ultimo_periodo_str.split(' ', 1)[1]
                if ano_str.isdigit():
                    ano_limite = int(ano_str)
                    # Filtrar apenas períodos desse ano
                    def periodo_tem_ano(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if ' ' in periodo_str:
                            ano_val = periodo_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                return int(ano_val) == ano_limite
                        return True  # Se não tem ano, manter
                    df_medias_ano_recente = df_medias[
                        df_medias['Período'].apply(periodo_tem_ano)
                    ].copy()
                else:
                    df_medias_ano_recente = df_medias.copy()
            else:
                df_medias_ano_recente = df_medias.copy()
        else:
            df_medias_ano_recente = df_medias.copy()
        
        # 🔧 DEBUG: Verificar quantos anos existem após o filtro
        if not df_medias_ano_recente.empty and 'Período' in df_medias_ano_recente.columns:
            periodos_unicos = df_medias_ano_recente['Período'].unique()
            anos_encontrados = set()
            for p in periodos_unicos:
                p_str = str(p).strip()
                if ' ' in p_str:
                    ano_val = p_str.split(' ', 1)[1]
                    if ano_val.isdigit():
                        anos_encontrados.add(int(ano_val))
            if len(anos_encontrados) > 1:
                # PROBLEMA: Múltiplos anos ainda presentes!
                # Forçar filtro apenas pelo ano mais recente
                ano_final = max(anos_encontrados)
                def tem_ano_final(periodo_val):
                    periodo_str = str(periodo_val).strip()
                    if ' ' in periodo_str:
                        ano_str = periodo_str.split(' ', 1)[1]
                        if ano_str.isdigit():
                            return int(ano_str) == ano_final
                    return True  # Se não tem ano, MANTER (assume ano mais recente)
                df_medias_ano_recente = df_medias_ano_recente[
                    df_medias_ano_recente['Período'].apply(tem_ano_final)
                ].copy()
        
        # Calcular média geral mensal por linha (média das médias dos meses selecionados)
        colunas_groupby_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
        colunas_groupby_media = [col for col in colunas_groupby_media if col in df_medias_ano_recente.columns]
        agg_dict_media = {'Total': 'mean'}
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

        # ================================================================
        # 🔧 Ajuste de coerência REMOVIDO
        # O ajuste anterior distorcia os valores reais das médias por linha,
        # fazendo o forecast ficar incorreto. Agora mantemos os valores reais.
        # A média global do gráfico continuará correta, mas a soma das médias
        # por linha pode ser diferente (isso é esperado e correto).
        # ================================================================
        
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
    
    # Verificar se encontrou menos períodos do que o solicitado
    if not df_medias.empty and 'Período' in df_medias.columns:
        periodos_encontrados = df_medias['Período'].unique()
        if len(periodos_encontrados) < len(periodos_para_media):
            st.info(f"ℹ️ **Informação:** Foram encontrados {len(periodos_encontrados)} período(s) nos dados (solicitados: {len(periodos_para_media)}). O cálculo será feito com os períodos disponíveis.")
    
    # Função para calcular volumes e CPU com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_volumes_cpu(df_vol_cache, df_medias_cache, colunas_adicionais_cache, periodos_para_media_cache, ultimo_periodo_dados_cache=None):
        """Calcula volumes e CPU histórico com cache, usando apenas os períodos selecionados"""
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

            # Se, por algum motivo, o filtro não encontrar nada, voltar a usar todos os dados
            # para não ficar com volume histórico zero (mantém a regra de negócio funcionando)
            if df_vol_para_media.empty:
                df_vol_para_media = df_vol_cache.copy()
        else:
            # Se não houver períodos selecionados, usar todos os dados (comportamento original)
            df_vol_para_media = df_vol_cache.copy()
        
        # Calcular média de volume por período histórico (apenas meses selecionados)
        if not df_vol_para_media.empty:
            df_vol_medio = df_vol_para_media.groupby(['Oficina', 'Veículo', 'Período'], as_index=False)['Volume'].mean()
            
            # 🔧 CORREÇÃO: Filtrar apenas o ano mais recente antes de calcular volume médio mensal
            # Isso evita misturar dados de 2024 e 2025 no mesmo cálculo
            if 'Ano' in df_vol_medio.columns:
                ano_mais_recente_vol = df_vol_medio['Ano'].max()
                df_vol_medio_ano_recente = df_vol_medio[df_vol_medio['Ano'] == ano_mais_recente_vol].copy()
            elif ultimo_periodo_dados_cache:
                # Extrair ano do último período se disponível
                ultimo_periodo_str = str(ultimo_periodo_dados_cache).strip()
                if ' ' in ultimo_periodo_str:
                    ano_str = ultimo_periodo_str.split(' ', 1)[1]
                    if ano_str.isdigit():
                        ano_limite = int(ano_str)
                        # Filtrar apenas períodos desse ano
                        def periodo_tem_ano_vol(periodo_val):
                            periodo_str = str(periodo_val).strip()
                            if ' ' in periodo_str:
                                ano_val = periodo_str.split(' ', 1)[1]
                                if ano_val.isdigit():
                                    return int(ano_val) == ano_limite
                            return True  # Se não tem ano, manter
                        df_vol_medio_ano_recente = df_vol_medio[
                            df_vol_medio['Período'].apply(periodo_tem_ano_vol)
                        ].copy()
                    else:
                        df_vol_medio_ano_recente = df_vol_medio.copy()
                else:
                    df_vol_medio_ano_recente = df_vol_medio.copy()
            else:
                df_vol_medio_ano_recente = df_vol_medio.copy()
            
            # 🔧 DEBUG: Verificar quantos anos existem após o filtro de volumes
            if not df_vol_medio_ano_recente.empty and 'Período' in df_vol_medio_ano_recente.columns:
                periodos_unicos_vol = df_vol_medio_ano_recente['Período'].unique()
                anos_encontrados_vol = set()
                for p in periodos_unicos_vol:
                    p_str = str(p).strip()
                    if ' ' in p_str:
                        ano_val = p_str.split(' ', 1)[1]
                        if ano_val.isdigit():
                            anos_encontrados_vol.add(int(ano_val))
                if len(anos_encontrados_vol) > 1:
                    # PROBLEMA: Múltiplos anos ainda presentes!
                    # Forçar filtro apenas pelo ano mais recente
                    ano_final_vol = max(anos_encontrados_vol)
                    def tem_ano_final_vol(periodo_val):
                        periodo_str = str(periodo_val).strip()
                        if ' ' in periodo_str:
                            ano_str = periodo_str.split(' ', 1)[1]
                            if ano_str.isdigit():
                                return int(ano_str) == ano_final_vol
                        return True  # Se não tem ano, MANTER (assume ano mais recente)
                    df_vol_medio_ano_recente = df_vol_medio_ano_recente[
                        df_vol_medio_ano_recente['Período'].apply(tem_ano_final_vol)
                    ].copy()
            
            # Calcular volume médio mensal (média dos meses selecionados do ano mais recente)
            df_vol_medio_mensal = df_vol_medio_ano_recente.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
            df_vol_medio_mensal = df_vol_medio_mensal.rename(columns={'Volume': 'Volume_Medio_Historico'})
        else:
            # Se não houver dados, criar DataFrames vazios
            df_vol_medio = pd.DataFrame(columns=['Oficina', 'Veículo', 'Período', 'Volume'])
            df_vol_medio_mensal = pd.DataFrame(columns=['Oficina', 'Veículo', 'Volume_Medio_Historico'])
        
        # Volume por mês (incluindo meses futuros)
        df_vol_por_mes = df_vol_cache.groupby(['Oficina', 'Veículo', 'Período'], as_index=False)['Volume'].mean()
        
        # Calcular relação custo/volume histórica para custos variáveis
        df_custo_volume = pd.merge(
            df_medias_cache[df_medias_cache['Tipo_Custo'] == 'Variável'],
            df_vol_medio,
            on=['Oficina', 'Veículo', 'Período'],
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
    volume_base, volume_por_mes, df_cpu_medio, df_vol_medio = calcular_volumes_cpu(df_vol, df_medias, colunas_adicionais, periodos_para_media, ultimo_periodo_dados)
    
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
        """Calcula forecast completo com cache"""
        # Converter tuple de volta para dict se necessário
        if sensibilidades_type06_cache is not None:
            sensibilidades_type06_dict = dict(sensibilidades_type06_cache)
        else:
            sensibilidades_type06_dict = None
        
        if inflacao_type06_cache is not None:
            inflacao_type06_dict = dict(inflacao_type06_cache)
        else:
            inflacao_type06_dict = None
        
        # 🔧 VERIFICAÇÃO: Garantir que df_media_mensal não tem duplicatas
        # Se houver duplicatas, o merge vai criar linhas multiplicadas
        colunas_chave_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
        colunas_chave_media_existentes = [col for col in colunas_chave_media if col in df_media_mensal_cache.columns]
        
        if len(colunas_chave_media_existentes) > 0:
            duplicatas_media = df_media_mensal_cache.duplicated(subset=colunas_chave_media_existentes, keep=False)
            if duplicatas_media.any():
                # Se houver duplicatas, agrupar novamente
                agg_dict_media_dup = {'Total': 'mean'}  # Tirar média das médias duplicadas
                df_media_mensal_cache = df_media_mensal_cache.groupby(
                    colunas_chave_media_existentes, as_index=False
                ).agg(agg_dict_media_dup)
        
        # 🔧 VERIFICAÇÃO: Garantir que volume_base não tem duplicatas
        # Se houver múltiplas linhas para mesma Oficina + Veículo, o merge vai duplicar
        if not volume_base_cache.empty and 'Oficina' in volume_base_cache.columns and 'Veículo' in volume_base_cache.columns:
            duplicatas_volume = volume_base_cache.duplicated(subset=['Oficina', 'Veículo'], keep=False)
            if duplicatas_volume.any():
                # Se houver duplicatas, agrupar (pegar média ou soma do volume)
                volume_base_cache = volume_base_cache.groupby(
                    ['Oficina', 'Veículo'], as_index=False
                ).agg({'Volume_Medio_Historico': 'mean'})  # Tirar média dos volumes duplicados
        
        # Fazer merge com volume_base
        df_forecast_base = df_media_mensal_cache.merge(
            volume_base_cache,
            on=['Oficina', 'Veículo'],
            how='left'
        )
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
                    # Se houver duplicatas, agrupar (pegar média dos valores duplicados)
                    colunas_agregar_cpu = [col for col in df_cpu_medio_cache.columns if col not in colunas_merge_cpu_existentes]
                    agg_dict_cpu = {col: 'mean' if df_cpu_medio_cache[col].dtype in ['float64', 'int64'] else 'first' for col in colunas_agregar_cpu}
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
        
        # Criar DataFrame final de forecast
        forecast_cols = ['Oficina', 'Veículo'] + colunas_adicionais_cache + ['Tipo_Custo', 'Média_Mensal_Histórica']
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
                vol_mes_df = volume_por_mes_cache[mask_corresponde][['Oficina', 'Veículo', 'Volume']]
                
                if not vol_mes_df.empty:
                    vol_mes_df = vol_mes_df.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
                    df_vol_mes_merge = df_forecast_base[['Oficina', 'Veículo']].merge(
                        vol_mes_df,
                        on=['Oficina', 'Veículo'],
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
            
            # Alinhar volume
            if isinstance(volume_mes_serie, pd.Series):
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Ref'])
            else:
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Ref'])
            
            # Calcular forecast com sensibilidade diferenciada
            df_forecast[periodo] = 0.0
            
            # Calcular proporção de volume para TODOS os registros
            volume_medio_positivo = df_forecast_base['Volume_Medio_Historico'] > 0
            # Evitar explosão quando não há volume médio histórico:
            # para casos sem volume histórico, usamos proporção neutra (=1.0)
            denom_volume = df_forecast_base['Volume_Medio_Historico'].where(volume_medio_positivo, 1.0)
            proporcao_volume = volume_mes_aligned / denom_volume
            proporcao_volume = proporcao_volume.where(volume_medio_positivo, 1.0)
            
            # Calcular variação percentual do volume (ex: 1.1 = +10%, 0.9 = -10%)
            variacao_percentual = proporcao_volume - 1.0
            
            # Verificar se deve usar sensibilidade por Type 06 ou global
            if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_base.columns:
                # Modo detalhado: aplicar sensibilidade por Type 06
                for idx in df_forecast_base.index:
                    type06_valor = df_forecast_base.loc[idx, 'Type 06']
                    
                    # Obter sensibilidade específica ou usar padrão
                    if pd.notna(type06_valor) and type06_valor in sensibilidades_type06_dict:
                        sens = sensibilidades_type06_dict[type06_valor]
                    else:
                        # Se não encontrar, usar sensibilidade baseada no tipo
                        tipo_custo = df_forecast_base.loc[idx, 'Tipo_Custo']
                        sens = sensibilidade_fixo_cache if tipo_custo == 'Fixo' else sensibilidade_variavel_cache
                    
                    # Obter inflação específica (se houver)
                    inflacao_percentual = 0.0
                    if inflacao_type06_dict is not None and pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                        inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0  # Converter % para decimal
                    
                    # Aplicar sensibilidade
                    variacao_ajustada = variacao_percentual.loc[idx] * sens
                    proporcao_ajustada = 1.0 + variacao_ajustada
                    
                    # Calcular valor base com sensibilidade
                    valor_base = df_forecast_base.loc[idx, 'Média_Mensal_Histórica'] * proporcao_ajustada
                    
                    # Aplicar inflação UMA ÚNICA VEZ (não acumulada)
                    # A inflação é aplicada ao valor base e mantida em todos os meses
                    fator_inflacao = 1.0 + inflacao_percentual
                    
                    df_forecast.loc[idx, periodo] = valor_base * fator_inflacao
            else:
                # Modo global: aplicar sensibilidade por Fixo/Variável
                # Obter inflação global (se houver) - aplicada UMA ÚNICA VEZ
                fator_inflacao_global = 1.0
                if inflacao_type06_dict is not None:
                    # Pegar qualquer valor do dicionário (todos são iguais no modo global)
                    primeiro_valor = next(iter(inflacao_type06_dict.values()), 0.0)
                    fator_inflacao_global = 1.0 + (primeiro_valor / 100.0)
                
                # Aplicar sensibilidade para Custo Fixo
                mask_fixo = df_forecast_base['Tipo_Custo'] == 'Fixo'
                if mask_fixo.any():
                    variacao_ajustada_fixo = variacao_percentual.loc[mask_fixo] * sensibilidade_fixo_cache
                    proporcao_ajustada_fixo = 1.0 + variacao_ajustada_fixo
                    df_forecast.loc[mask_fixo, periodo] = (
                        df_forecast_base.loc[mask_fixo, 'Média_Mensal_Histórica'] * proporcao_ajustada_fixo * fator_inflacao_global
                    )
                
                # Aplicar sensibilidade para Custo Variável
                mask_variavel = df_forecast_base['Tipo_Custo'] == 'Variável'
                if mask_variavel.any():
                    variacao_ajustada_variavel = variacao_percentual.loc[mask_variavel] * sensibilidade_variavel_cache
                    proporcao_ajustada_variavel = 1.0 + variacao_ajustada_variavel
                    df_forecast.loc[mask_variavel, periodo] = (
                        df_forecast_base.loc[mask_variavel, 'Média_Mensal_Histórica'] * proporcao_ajustada_variavel * fator_inflacao_global
                    )
        
        # 🔧 VERIFICAÇÃO FINAL: Se sensibilidade = 0 e inflação = 0, 
        # o forecast DEVE ser exatamente igual à média histórica
        # Isso garante que não há erros de cálculo ou duplicação
        if (sensibilidade_fixo_cache == 0 and sensibilidade_variavel_cache == 0 and
            (inflacao_type06_dict is None or all(v == 0 for v in inflacao_type06_dict.values()))):
            # Verificar se há diferenças significativas
            for periodo in meses_restantes_cache:
                if periodo in df_forecast.columns:
                    mask_diferente = abs(df_forecast[periodo] - df_forecast_base['Média_Mensal_Histórica']) > 0.01
                    if mask_diferente.any():
                        # Se houver diferenças, forçar igualdade
                        df_forecast.loc[mask_diferente, periodo] = df_forecast_base.loc[mask_diferente, 'Média_Mensal_Histórica']
        
        return df_forecast

    # Criar tabela de forecast
    st.markdown("### 🔮 Tabela de Forecast - Custo Total até Fim do Ano")
    
    # Preparar dados para forecast usando operações vetorizadas (mais rápido)
    # Calcular forecast com cache (incluindo sensibilidades e inflação)
    # Converter sensibilidades_type06 para tuple se for dict (para ser hashable no cache)
    sens_type06_cache = tuple(sorted(sensibilidades_type06.items())) if sensibilidades_type06 is not None else None
    inflacao_type06_cache = tuple(sorted(inflacao_type06.items())) if inflacao_type06 is not None else None
    
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
    # Guardar versão bruta do forecast (antes do agrupamento) para diagnósticos
    df_forecast_bruto = df_forecast.copy()
    
    # Função para processar e formatar tabela com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def processar_tabela_forecast(df_forecast_cache, colunas_adicionais_cache, meses_restantes_cache):
        """Processa e formata a tabela de forecast com cache"""
        # Reordenar colunas
        colunas_ordenadas = ['Oficina', 'Veículo'] + colunas_adicionais_cache + ['Tipo_Custo', 'Média_Mensal_Histórica'] + meses_restantes_cache
        colunas_existentes = [col for col in colunas_ordenadas if col in df_forecast_cache.columns]
        df_forecast_processado = df_forecast_cache[colunas_existentes].copy()
        
        # Calcular total por linha e identificar colunas de meses
        colunas_meses = [col for col in meses_restantes_cache if col in df_forecast_processado.columns]
        if colunas_meses:
            df_forecast_processado['Total_Forecast'] = df_forecast_processado[colunas_meses].sum(axis=1)
        
        # Agrupar linhas iguais (mesma combinação de Oficina+Veículo+Type+Tipo_Custo)
        colunas_agrupamento = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo']
        colunas_agrupamento_existentes = [col for col in colunas_agrupamento if col in df_forecast_processado.columns]
        
        if len(colunas_agrupamento_existentes) > 0:
            # Agrupar: somar valores numéricos (forecasts), usar 'first' para Média_Mensal_Histórica
            # NÃO somar Total_Forecast aqui, vamos recalcular depois
            agg_dict_grupo = {}
            for col in df_forecast_processado.columns:
                if col not in colunas_agrupamento_existentes:
                    if col == 'Média_Mensal_Histórica':
                        agg_dict_grupo[col] = 'first'  # Manter primeira ocorrência
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

    # Processar tabela com cache
    df_forecast, colunas_meses = processar_tabela_forecast(df_forecast, colunas_adicionais, periodos_restantes)
    
    # Criar gráfico de resumo: Premissas da Previsão (Volumes em barras e Custos em linhas)
    st.markdown("### 📊 Gráfico - Premissas da Previsão")
    
    # Preparar dados para o gráfico mostrando todas as premissas
    if colunas_meses:
        # Calcular totais agregados
        # 🔹 Ajuste: calcular a média histórica exatamente como no gráfico de meses históricos
        media_historica_total = None
        try:
            # Usar df_medias (médias por período) para calcular a média mensal dos meses selecionados
            if df_medias is not None and not df_medias.empty and 'Período' in df_medias.columns and 'Total' in df_medias.columns:
                df_medias_temp2 = df_medias.copy()
                
                # 🔧 CORREÇÃO: Filtrar apenas o ano mais recente ANTES de calcular a média
                if 'Ano' in df_medias_temp2.columns:
                    ano_mais_recente_media = df_medias_temp2['Ano'].max()
                    df_medias_temp2 = df_medias_temp2[df_medias_temp2['Ano'] == ano_mais_recente_media].copy()
                elif 'Período' in df_medias_temp2.columns:
                    # Verificar quantos anos existem nos períodos
                    periodos_unicos_media = df_medias_temp2['Período'].unique()
                    anos_encontrados_media = set()
                    for p in periodos_unicos_media:
                        p_str = str(p).strip()
                        if ' ' in p_str:
                            ano_val = p_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                anos_encontrados_media.add(int(ano_val))
                    if len(anos_encontrados_media) > 1:
                        # Múltiplos anos: filtrar apenas o mais recente
                        ano_final_media = max(anos_encontrados_media)
                        def tem_ano_final_media(periodo_val):
                            periodo_str = str(periodo_val).strip()
                            if ' ' in periodo_str:
                                ano_str = periodo_str.split(' ', 1)[1]
                                if ano_str.isdigit():
                                    return int(ano_str) == ano_final_media
                            return True  # Se não tem ano, MANTER
                        df_medias_temp2 = df_medias_temp2[
                            df_medias_temp2['Período'].apply(tem_ano_final_media)
                        ].copy()
                
                df_medias_temp2['Período'] = df_medias_temp2['Período'].astype(str)

                # Filtrar apenas períodos usados na média (periodos_para_media)
                if periodos_para_media:
                    meses_media = []
                    for p in periodos_para_media:
                        p_str = str(p).strip().lower()
                        mes_p = p_str.split(' ', 1)[0] if ' ' in p_str else p_str
                        meses_media.append(mes_p)

                    def periodo_esta_na_media(p):
                        p_str = str(p).strip().lower()
                        mes_p = p_str.split(' ', 1)[0] if ' ' in p_str else p_str
                        return mes_p in meses_media

                    mask_media = df_medias_temp2['Período'].apply(periodo_esta_na_media)
                    df_medias_temp2 = df_medias_temp2[mask_media]

                if not df_medias_temp2.empty:
                    # Agregar custo total por período e tirar a média dos meses
                    df_medias_agregado2 = df_medias_temp2.groupby('Período', as_index=False)['Total'].sum()
                    media_historica_total = float(df_medias_agregado2['Total'].mean())
        except Exception:
            media_historica_total = None

        # Fallback: se por algum motivo não conseguimos calcular pela lógica acima, usar comportamento anterior
        if media_historica_total is None:
            media_historica_total = float(df_forecast['Média_Mensal_Histórica'].sum()) if 'Média_Mensal_Histórica' in df_forecast.columns else 0.0
        
        # Volume médio histórico (soma total para referência)
        volume_medio_historico_total = volume_base['Volume_Medio_Historico'].sum() if not volume_base.empty else 0
        
        # Preparar dados do gráfico
        dados_grafico_premissas = []
        
        # Adicionar barra para média histórica de volume (meses realizados)
        dados_grafico_premissas.append({
            'Período': 'Média Histórica',
            'Custo': media_historica_total,
            'Volume': volume_medio_historico_total,
            'Tipo': 'Histórico'
        })
        
        # Adicionar dados para cada mês futuro
        for mes in colunas_meses:
            # Calcular forecast total do mês
            forecast_mes_total = df_forecast[mes].sum() if mes in df_forecast.columns else 0
            
            # Buscar volume futuro deste mês usando comparação flexível
            volume_futuro_mes = 0
            if not volume_por_mes.empty:
                mes_procurado_str = str(mes).strip().lower()
                mes_procurado_nome = mes_procurado_str.split(' ', 1)[0] if ' ' in mes_procurado_str else mes_procurado_str
                
                def periodo_corresponde_volume(periodo_df):
                    periodo_df_str = str(periodo_df).strip().lower()
                    periodo_df_mes = periodo_df_str.split(' ', 1)[0] if ' ' in periodo_df_str else periodo_df_str
                    return periodo_df_mes == mes_procurado_nome
                
                periodos_no_df = volume_por_mes['Período'].astype(str)
                mask_corresponde = periodos_no_df.apply(periodo_corresponde_volume)
                vol_mes_df = volume_por_mes[mask_corresponde]
                
                if not vol_mes_df.empty:
                    volume_futuro_mes = vol_mes_df['Volume'].sum()
            
            dados_grafico_premissas.append({
                'Período': mes,
                'Custo': forecast_mes_total,
                'Volume': volume_futuro_mes,
                'Tipo': 'Forecast'
            })
        
        df_grafico_premissas = pd.DataFrame(dados_grafico_premissas)
        
        
        # Ordenar: Média Histórica primeiro, depois meses cronologicamente
        ordem_meses_dict = {mes: i for i, mes in enumerate(meses_ano)}
        df_grafico_premissas['_ordem'] = df_grafico_premissas['Período'].map(
            lambda x: -1 if x == 'Média Histórica' else ordem_meses_dict.get(x, 999)
        )
        df_grafico_premissas = df_grafico_premissas.sort_values('_ordem').drop(columns=['_ordem'])
        ordem_periodos_grafico = df_grafico_premissas['Período'].tolist()
        
        # Verificar se há dados válidos
        if df_grafico_premissas.empty or df_grafico_premissas['Custo'].sum() == 0:
            st.warning("⚠️ Não há dados suficientes para gerar o gráfico de premissas.")
        else:
            # Calcular valores máximos para escala
            max_custo = float(df_grafico_premissas['Custo'].max())
            max_volume = float(df_grafico_premissas['Volume'].max())
            
            # Garantir que os dados são numéricos
            df_grafico_premissas['Custo'] = pd.to_numeric(df_grafico_premissas['Custo'], errors='coerce')
            df_grafico_premissas['Volume'] = pd.to_numeric(df_grafico_premissas['Volume'], errors='coerce')
            
            # Criar gráfico de barras para custo (SIMPLIFICADO)
            barras_custo = alt.Chart(df_grafico_premissas).mark_bar(size=80).encode(
                x=alt.X('Período:N', sort=ordem_periodos_grafico),
                y=alt.Y('Custo:Q'),
                color=alt.Color('Tipo:N', 
                    scale=alt.Scale(domain=['Histórico', 'Forecast'], range=['#9467bd', '#ff7f0e'])
                ),
                tooltip=['Período:N', 'Custo:Q', 'Volume:Q', 'Tipo:N']
            ).properties(
                width=800,
                height=400,
                title='Custo Total por Período'
            )
            
            # Adicionar rótulos
            texto_barras = barras_custo.mark_text(
                align='center',
                baseline='bottom',
                dy=-5,
                color='white'
            ).encode(
                text=alt.Text('Custo:Q', format=',.0f')
            )
            
            # Criar gráfico de linhas para volume
            linha_volume = alt.Chart(df_grafico_premissas).mark_line(
                point=True,
                color='#2ca02c',
                strokeWidth=3
            ).encode(
                x=alt.X('Período:N', sort=ordem_periodos_grafico),
                y=alt.Y('Volume:Q'),
                tooltip=['Período:N', 'Volume:Q']
            ).properties(
                width=800,
                height=400,
                title='Volume por Período'
            )
            
            # Adicionar rótulos na linha
            texto_linha = linha_volume.mark_text(
                align='center',
                baseline='bottom',
                dy=-10,
                color='#2ca02c'
            ).encode(
                text=alt.Text('Volume:Q', format=',.0f')
            )
            
            # Mostrar gráficos em linhas separadas (um abaixo do outro)
            st.altair_chart(barras_custo + texto_barras, use_container_width=True)
            st.altair_chart(linha_volume + texto_linha, use_container_width=True)
            
            # Mostrar resumo dos dados
            st.info(f"""
            📊 **Resumo do Gráfico:**
            - Custo Médio Histórico: R$ {media_historica_total:,.2f}
            - Volume Médio Histórico: {volume_medio_historico_total:,.2f}
            - Total de Períodos no Forecast: {len(colunas_meses)}
            """)
        
        # Legenda explicativa
        st.markdown("""
        **Legenda:**
        - 🟣 **Média Histórica de Custo** (barra roxa): Média mensal histórica de custo (meses realizados)
        - 🟠 **Forecast de Custo** (barras laranjas): Custo previsto para cada mês futuro baseado nos volumes
        - 🔵 **Volume Médio Histórico** (linha tracejada azul): Média histórica de volume utilizada como referência
        - 🟢 **Volume Futuro** (linha sólida verde): Volume de entrada do arquivo para cada mês futuro
        """)
        
        # ====================================================================
        # 📊 NOVO GRÁFICO: Meses Individuais + Média Acumulada
        # ====================================================================
        st.markdown("### 📊 Gráfico - Meses Históricos e Média Acumulada")
        
        # Preparar dados dos meses individuais usados para a média
        # Verificar se df_medias está disponível (pode estar em diferentes escopos)
        try:
            df_medias_disponivel = df_medias
            
            # 🔧 CORREÇÃO: Filtrar apenas o ano mais recente para o gráfico
            # (mesmo filtro aplicado ao calcular df_media_mensal)
            if df_medias_disponivel is not None and not df_medias_disponivel.empty:
                if 'Ano' in df_medias_disponivel.columns:
                    ano_mais_recente_grafico = df_medias_disponivel['Ano'].max()
                    df_medias_disponivel = df_medias_disponivel[
                        df_medias_disponivel['Ano'] == ano_mais_recente_grafico
                    ].copy()
                elif 'Período' in df_medias_disponivel.columns:
                    # Verificar quantos anos existem nos períodos
                    periodos_unicos_grafico = df_medias_disponivel['Período'].unique()
                    anos_encontrados_grafico = set()
                    for p in periodos_unicos_grafico:
                        p_str = str(p).strip()
                        if ' ' in p_str:
                            ano_val = p_str.split(' ', 1)[1]
                            if ano_val.isdigit():
                                anos_encontrados_grafico.add(int(ano_val))
                    if len(anos_encontrados_grafico) > 1:
                        # Múltiplos anos: filtrar apenas o mais recente
                        ano_final_grafico = max(anos_encontrados_grafico)
                        def tem_ano_final_grafico(periodo_val):
                            periodo_str = str(periodo_val).strip()
                            if ' ' in periodo_str:
                                ano_str = periodo_str.split(' ', 1)[1]
                                if ano_str.isdigit():
                                    return int(ano_str) == ano_final_grafico
                            return True  # Se não tem ano, MANTER
                        df_medias_disponivel = df_medias_disponivel[
                            df_medias_disponivel['Período'].apply(tem_ano_final_grafico)
                        ].copy()
        except NameError:
            df_medias_disponivel = None
        
        if df_medias_disponivel is not None and not df_medias_disponivel.empty and 'Período' in df_medias_disponivel.columns:
            # Garantir que Período seja string antes do groupby
            df_medias_temp = df_medias_disponivel.copy()
            df_medias_temp = df_medias_temp.reset_index(drop=True)  # Garantir índice simples
            df_medias_temp['Período'] = df_medias_temp['Período'].astype(str)
            
            # Agregar custo total por período
            df_medias_agregado = df_medias_temp.groupby('Período', as_index=False)['Total'].sum()
            df_medias_agregado = df_medias_agregado.reset_index(drop=True)  # Garantir índice simples após groupby
            
            # Ordenar períodos cronologicamente
            def ordenar_periodo_grafico(periodo_str):
                periodo_str = str(periodo_str).strip()
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    mes_nome = partes[0].capitalize()
                    ano = int(partes[1]) if partes[1].isdigit() else 0
                    mes_idx = meses_ano.index(mes_nome) if mes_nome in meses_ano else 0
                    return (ano, mes_idx)
                else:
                    mes_nome = periodo_str.capitalize()
                    mes_idx = meses_ano.index(mes_nome) if mes_nome in meses_ano else 0
                    return (0, mes_idx)
            
            # Criar coluna temporária para ordenação (usar valores convertidos para lista)
            periodos_lista = df_medias_agregado['Período'].tolist()
            ordens = [ordenar_periodo_grafico(p) for p in periodos_lista]
            df_medias_agregado['_ordem'] = ordens
            df_medias_agregado = df_medias_agregado.sort_values('_ordem').drop(columns=['_ordem']).reset_index(drop=True)
            
            # Calcular média acumulada progressiva
            df_medias_agregado['Media_Acumulada'] = df_medias_agregado['Total'].expanding().mean()
            
            # Garantir que a última média acumulada seja igual à média histórica
            if len(df_medias_agregado) > 0:
                ultima_media_acumulada = df_medias_agregado['Media_Acumulada'].iloc[-1]
                # Ajustar se necessário (deve ser igual à média histórica)
                if abs(ultima_media_acumulada - media_historica_total) > 0.01:
                    # Ajustar a última média acumulada para ser exatamente igual à média histórica
                    df_medias_agregado.loc[df_medias_agregado.index[-1], 'Media_Acumulada'] = media_historica_total
            
            # Preparar dados para o gráfico (meses históricos)
            dados_grafico_historico = []
            for idx, row in df_medias_agregado.iterrows():
                periodo_str = str(row['Período'])
                # Garantir que só entram no gráfico os meses realmente usados na média
                if periodos_para_media:
                    # Extrair apenas o mês de periodos_para_media para comparação
                    meses_media = []
                    for p in periodos_para_media:
                        p_str = str(p).strip().lower()
                        mes_p = p_str.split(' ', 1)[0] if ' ' in p_str else p_str
                        meses_media.append(mes_p)
                    periodo_mes = periodo_str.strip().split(' ', 1)[0].lower()
                    if periodo_mes not in meses_media:
                        continue
                dados_grafico_historico.append({
                    'Período': periodo_str,
                    'Custo': float(row['Total']),
                    'Media_Acumulada': float(row['Media_Acumulada']),
                    'Tipo': 'Histórico'
                })
            
            # Adicionar períodos de forecast
            if colunas_meses:
                for mes in colunas_meses:
                    forecast_mes_total = df_forecast[mes].sum() if mes in df_forecast.columns else 0
                    dados_grafico_historico.append({
                        'Período': str(mes),
                        'Custo': float(forecast_mes_total),
                        'Media_Acumulada': None,  # Não calcular média acumulada para forecast
                        'Tipo': 'Forecast'
                    })
            
            df_grafico_historico = pd.DataFrame(dados_grafico_historico)
            
            if not df_grafico_historico.empty:
                # Calcular valores máximos para escala (apenas das barras)
                max_custo_barras = float(df_grafico_historico['Custo'].max())
                
                # Calcular valores da média acumulada (apenas períodos históricos)
                df_medias_hist = df_grafico_historico[df_grafico_historico['Tipo'] == 'Histórico'].copy()
                max_media_valor = float(df_medias_hist['Media_Acumulada'].max()) if not df_medias_hist.empty else 0
                min_media_valor = float(df_medias_hist['Media_Acumulada'].min()) if not df_medias_hist.empty else 0
                
                # Calcular posição desejada da linha (30% acima do maior valor das barras)
                posicao_desejada_linha = max_custo_barras * 1.3
                
                # Calcular fator de escala para mapear valores reais para posição acima das barras
                # A linha deve mostrar a evolução, mas ficar sempre acima das barras
                if max_media_valor > 0:
                    # Escalar para que o máximo da média fique na posição desejada
                    fator_escala = posicao_desejada_linha / max_media_valor
                else:
                    fator_escala = 1.0
                
                # Manter valores reais da média acumulada para tooltips
                df_grafico_historico['Media_Acumulada_Valor'] = df_grafico_historico['Media_Acumulada']
                
                # Aplicar escala aos períodos históricos para posicionamento
                mask_historico = df_grafico_historico['Tipo'] == 'Histórico'
                df_grafico_historico.loc[mask_historico, 'Media_Acumulada_Escalada'] = (
                    df_grafico_historico.loc[mask_historico, 'Media_Acumulada'] * fator_escala
                )
                df_grafico_historico.loc[~mask_historico, 'Media_Acumulada_Escalada'] = None
                
                # Calcular escala máxima para o eixo primário (barras)
                max_escala_barras = max_custo_barras * 1.2
                # Calcular escala máxima para o eixo secundário (linha) - posição desejada + margem
                max_escala_linha = posicao_desejada_linha * 1.1
                
                # Ordenar períodos para o gráfico
                ordem_periodos_historico = df_grafico_historico['Período'].tolist()
                
                # Criar gráfico de barras para meses individuais e forecast (sem legenda)
                barras_meses = alt.Chart(df_grafico_historico).mark_bar(size=40).encode(
                    x=alt.X('Período:N', sort=ordem_periodos_historico, title='Período'),
                    y=alt.Y('Custo:Q', title='Custo', scale=alt.Scale(domain=[0, max_escala_barras])),
                    color=alt.Color('Tipo:N', 
                        scale=alt.Scale(domain=['Histórico', 'Forecast'], range=['#9467bd', '#ff7f0e']),
                        legend=None  # Remover legenda das barras
                    ),
                    tooltip=['Período:N', 'Custo:Q', 'Tipo:N']
                ).properties(
                    width=800,
                    height=400,
                    title='Custo por Mês Histórico e Média Acumulada'
                )
                
                # Adicionar rótulos nas barras
                texto_barras_meses = barras_meses.mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-5,
                    color='white',
                    fontSize=10
                ).encode(
                    text=alt.Text('Custo:Q', format=',.0f')
                )
                
                # Filtrar apenas períodos históricos para a linha (não mostrar linha nos períodos de forecast)
                df_grafico_linha = df_grafico_historico[df_grafico_historico['Tipo'] == 'Histórico'].copy()
                
                # Criar gráfico de linha para média acumulada (pontilhada, escalada para ficar acima das barras)
                linha_media_acumulada = alt.Chart(df_grafico_linha).mark_line(
                    point=True,
                    color='#1f77b4',
                    strokeWidth=3,
                    strokeDash=[5, 5]  # Linha pontilhada
                ).encode(
                    x=alt.X('Período:N', sort=ordem_periodos_historico),
                    y=alt.Y('Media_Acumulada_Escalada:Q', 
                           title='Média Acumulada',
                           scale=alt.Scale(domain=[0, max_escala_linha]),
                           axis=alt.Axis(
                               orient='right', 
                               titleColor='#1f77b4', 
                               labelColor='#1f77b4',
                               labelFlush=True,
                               labelOverlap=False,
                               tickCount=4,
                               format='.2s'
                           )),
                    tooltip=[
                        alt.Tooltip('Período:N'), 
                        alt.Tooltip('Media_Acumulada_Valor:Q', format=',.2f', title='Média Acumulada (Valor Real)')
                    ]
                )
                
                # Adicionar rótulos na linha (mostrar valor real, mas posicionar na linha escalada)
                texto_media_acumulada = alt.Chart(df_grafico_linha).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-10,
                    color='#1f77b4',
                    fontSize=10
                ).encode(
                    x=alt.X('Período:N', sort=ordem_periodos_historico),
                    y=alt.Y('Media_Acumulada_Escalada:Q', 
                           scale=alt.Scale(domain=[0, max_escala_linha])),
                    text=alt.Text('Media_Acumulada_Valor:Q', format=',.2f')
                )
                
                # Combinar gráficos com eixos independentes
                grafico_combinado = (barras_meses + texto_barras_meses + linha_media_acumulada + texto_media_acumulada).resolve_scale(
                    y='independent'
                )
                
                # Mostrar gráfico
                st.altair_chart(grafico_combinado, use_container_width=True)
                
                # Informação adicional
                st.info(f"""
                📊 **Informações do Gráfico:**
                - **Meses utilizados para média:** {len(df_grafico_historico[df_grafico_historico['Tipo'] == 'Histórico'])} períodos
                - **Média Histórica Final:** R$ {media_historica_total:,.2f}
                - **Última Média Acumulada:** R$ {df_grafico_historico['Media_Acumulada_Valor'].dropna().iloc[-1]:,.2f}
                """)
            else:
                st.warning("⚠️ Não há dados históricos suficientes para gerar o gráfico de meses individuais.")
        else:
            st.warning("⚠️ Dados de meses históricos não disponíveis para gerar o gráfico detalhado.")

        # ====================================================================
        # 📊 NOVO GRÁFICO: Volume Histórico x Futuro (Meses Individuais)
        # ====================================================================
        st.markdown("### 📊 Gráfico - Volume Histórico e Futuro (Meses Individuais)")

        try:
            df_vol_medio_disp = df_vol_medio
        except NameError:
            df_vol_medio_disp = None

        if df_vol_medio_disp is not None and not df_vol_medio_disp.empty:
            # Agregar volume histórico por período (apenas meses usados para média)
            df_vol_hist = df_vol_medio_disp.groupby('Período', as_index=False)['Volume'].sum()

            # Ordenar períodos cronologicamente reutilizando a mesma lógica
            def ordenar_periodo_volume(periodo_str):
                periodo_str = str(periodo_str).strip()
                if ' ' in periodo_str:
                    partes = periodo_str.split(' ', 1)
                    mes_nome = partes[0].capitalize()
                    ano = int(partes[1]) if partes[1].isdigit() else 0
                    mes_idx = meses_ano.index(mes_nome) if mes_nome in meses_ano else 0
                    return (ano, mes_idx)
                else:
                    mes_nome = periodo_str.capitalize()
                    mes_idx = meses_ano.index(mes_nome) if mes_nome in meses_ano else 0
                    return (0, mes_idx)

            # Preparar dados para gráfico de volume
            dados_grafico_volume = []

            # Meses históricos (volume médio utilizado na média)
            for _, row in df_vol_hist.iterrows():
                dados_grafico_volume.append({
                    'Período': str(row['Período']),
                    'Volume': float(row['Volume']),
                    'Tipo': 'Histórico'
                })

            # Meses futuros (volume de entrada para cada mês a prever)
            if volume_por_mes is not None and not volume_por_mes.empty and colunas_meses:
                for mes in colunas_meses:
                    volume_futuro_mes = 0.0
                    mes_procurado_str = str(mes).strip().lower()
                    mes_procurado_nome = mes_procurado_str.split(' ', 1)[0] if ' ' in mes_procurado_str else mes_procurado_str

                    def periodo_corresponde_volume(periodo_df):
                        periodo_df_str = str(periodo_df).strip().lower()
                        periodo_df_mes = periodo_df_str.split(' ', 1)[0] if ' ' in periodo_df_str else periodo_df_str
                        return periodo_df_mes == mes_procurado_nome

                    periodos_no_df_vol = volume_por_mes['Período'].astype(str)
                    mask_corresponde_vol = periodos_no_df_vol.apply(periodo_corresponde_volume)
                    vol_mes_df = volume_por_mes[mask_corresponde_vol]

                    if not vol_mes_df.empty:
                        volume_futuro_mes = float(vol_mes_df['Volume'].sum())

                    dados_grafico_volume.append({
                        'Período': str(mes),
                        'Volume': volume_futuro_mes,
                        'Tipo': 'Forecast'
                    })

            df_grafico_volume = pd.DataFrame(dados_grafico_volume)

            if not df_grafico_volume.empty:
                # Ordenar períodos
                df_grafico_volume['_ordem'] = df_grafico_volume['Período'].apply(ordenar_periodo_volume)
                df_grafico_volume = df_grafico_volume.sort_values('_ordem').drop(columns=['_ordem'])
                ordem_periodos_volume = df_grafico_volume['Período'].tolist()

                # Criar gráfico de barras de volume
                barras_volume_mes = alt.Chart(df_grafico_volume).mark_bar(size=40).encode(
                    x=alt.X('Período:N', sort=ordem_periodos_volume, title='Período'),
                    y=alt.Y('Volume:Q', title='Volume'),
                    color=alt.Color(
                        'Tipo:N',
                        scale=alt.Scale(domain=['Histórico', 'Forecast'], range=['#9467bd', '#ff7f0e']),
                        legend=alt.Legend(title='Tipo')
                    ),
                    tooltip=['Período:N', 'Volume:Q', 'Tipo:N']
                ).properties(
                    width=800,
                    height=400,
                    title='Volume Histórico x Futuro - Meses Individuais'
                )

                texto_volume_mes = barras_volume_mes.mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-5,
                    color='white',
                    fontSize=10
                ).encode(
                    text=alt.Text('Volume:Q', format=',.0f')
                )

                st.altair_chart(barras_volume_mes + texto_volume_mes, use_container_width=True)
            else:
                st.warning("⚠️ Não há dados suficientes de volume para gerar o gráfico detalhado.")
        else:
            st.warning("⚠️ Volume histórico não disponível para gerar o gráfico detalhado.")
    
    # ====================================================================
    # Criar tabela agrupada por Oficina com expanders e subtotais
    st.markdown("---")
    st.subheader("📋 Tabela - Forecast por Veículo, Oficina e Período")
    
    # Verificar se tem as colunas necessárias
    tem_oficina = 'Oficina' in df_forecast.columns
    tem_veiculo = 'Veículo' in df_forecast.columns
    
    if tem_oficina and tem_veiculo:
        # Criar versão formatada para exibição (manter original para cálculos)
        df_forecast_display = df_forecast.copy()
        
        # Formatar colunas numéricas
        def formatar_monetario(val):
            if pd.isna(val):
                return '-'
            if isinstance(val, (int, float)):
                return f"R$ {val:,.2f}"
            return val
        
        # Aplicar formatação apenas nas colunas de valores
        colunas_formatar = ['Média_Mensal_Histórica', 'Total_Forecast'] + colunas_meses
        for col in colunas_formatar:
            if col in df_forecast_display.columns:
                df_forecast_display[col] = df_forecast_display[col].apply(formatar_monetario)
        
        # Agrupar por Oficina e criar expanders
        oficinas = df_forecast_display['Oficina'].unique()
        
        for oficina in sorted(oficinas):
            # Filtrar dados da oficina
            df_oficina = df_forecast_display[df_forecast_display['Oficina'] == oficina].copy()
            
            # Calcular total da oficina (usar dados numéricos)
            df_oficina_numerico = df_forecast[df_forecast['Oficina'] == oficina].copy()
            total_oficina = df_oficina_numerico['Total_Forecast'].sum() if 'Total_Forecast' in df_oficina_numerico.columns else 0
            total_formatado = formatar_monetario(total_oficina)
            
            # Contar veículos únicos
            num_veiculos = df_oficina['Veículo'].nunique()
            
            # Criar expander para cada oficina (fechado por padrão)
            with st.expander(
                f"🏭 **{oficina}** - Total: {total_formatado} ({num_veiculos} veículo{'s' if num_veiculos > 1 else ''})",
                expanded=False
            ):
                # Remover coluna Oficina da tabela dentro do expander (já está no título)
                df_oficina_display = df_oficina.drop(columns=['Oficina'])
                
                # Calcular totais por coluna usando dados numéricos
                df_oficina_numerico_display = df_oficina_numerico.drop(columns=['Oficina'])
                
                # Calcular média histórica mensal da oficina usando a mesma lógica
                # do gráfico, mas restrita à oficina atual. Se não for possível,
                # será usado o comportamento anterior (soma das médias da tabela).
                media_historica_oficina = None
                try:
                    if (
                        'Oficina' in df_medias.columns
                        and 'Período' in df_medias.columns
                        and 'Total' in df_medias.columns
                    ):
                        df_medias_oficina = df_medias[
                            df_medias['Oficina'] == oficina
                        ].copy()
                        df_medias_oficina['Período'] = (
                            df_medias_oficina['Período'].astype(str)
                        )

                        if periodos_para_media:
                            meses_media_oficina = []
                            for p in periodos_para_media:
                                p_str = str(p).strip().lower()
                                mes_p = (
                                    p_str.split(' ', 1)[0]
                                    if ' ' in p_str
                                    else p_str
                                )
                                meses_media_oficina.append(mes_p)

                            def periodo_esta_na_media_oficina(p):
                                p_str = str(p).strip().lower()
                                mes_p = (
                                    p_str.split(' ', 1)[0]
                                    if ' ' in p_str
                                    else p_str
                                )
                                return mes_p in meses_media_oficina

                            mask_media_oficina = df_medias_oficina['Período'].apply(
                                periodo_esta_na_media_oficina
                            )
                            df_medias_oficina = df_medias_oficina[
                                mask_media_oficina
                            ]

                        if not df_medias_oficina.empty:
                            df_medias_agregado_oficina = df_medias_oficina.groupby(
                                'Período', as_index=False
                            )['Total'].sum()
                            media_historica_oficina = float(
                                df_medias_agregado_oficina['Total'].mean()
                            )
                except Exception:
                    media_historica_oficina = None
                
                # Criar linha de total
                linha_total = {}
                
                # Adicionar todas as colunas na ordem correta
                # Primeiro, adicionar colunas de identificação
                colunas_id = ['Veículo'] + [col for col in colunas_adicionais if col in df_oficina_display.columns] + ['Tipo_Custo']
                for col in colunas_id:
                    if col in df_oficina_display.columns:
                        linha_total[col] = '**TOTAL**'
                
                # Adicionar Média_Mensal_Histórica
                if 'Média_Mensal_Histórica' in df_oficina_numerico_display.columns:
                    if isinstance(media_historica_oficina, (int, float)):
                        linha_total['Média_Mensal_Histórica'] = (
                            formatar_monetario(media_historica_oficina)
                        )
                    else:
                        total_media = df_oficina_numerico_display[
                            'Média_Mensal_Histórica'
                        ].sum()
                        linha_total['Média_Mensal_Histórica'] = (
                            formatar_monetario(total_media)
                        )
                
                # Adicionar totais por mês
                for col in colunas_meses:
                    if col in df_oficina_numerico_display.columns:
                        total_col = df_oficina_numerico_display[col].sum()
                        linha_total[col] = formatar_monetario(total_col)
                
                # Adicionar Total_Forecast
                if 'Total_Forecast' in df_oficina_numerico_display.columns:
                    total_forecast = df_oficina_numerico_display['Total_Forecast'].sum()
                    linha_total['Total_Forecast'] = formatar_monetario(total_forecast)
                
                # Garantir que a linha de total tenha todas as colunas do DataFrame
                # Criar DataFrame com todas as colunas na ordem correta
                linha_total_ordenada = {}
                for col in df_oficina_display.columns:
                    if col in linha_total:
                        linha_total_ordenada[col] = linha_total[col]
                    else:
                        linha_total_ordenada[col] = ''
                
                # Adicionar linha de total ao DataFrame
                df_oficina_display = pd.concat([
                    df_oficina_display,
                    pd.DataFrame([linha_total_ordenada])
                    ], ignore_index=True)
                
                st.dataframe(df_oficina_display, use_container_width=True)

        # Expander adicional com TOTAL GERAL (todas as linhas, sem quebra por oficina)
        try:
            df_total_numerico = df_forecast.copy()
            total_geral = df_total_numerico['Total_Forecast'].sum() if 'Total_Forecast' in df_total_numerico.columns else 0
            total_geral_formatado = formatar_monetario(total_geral)
            num_veiculos_total = df_total_numerico['Veículo'].nunique() if 'Veículo' in df_total_numerico.columns else 0

            with st.expander(
                f"📊 **TOTAL GERAL** - Total: {total_geral_formatado} ({num_veiculos_total} veículo{'s' if num_veiculos_total > 1 else ''})",
                expanded=False
            ):
                # Tabela com TODAS as linhas (todas oficinas), sem coluna Oficina
                df_total_display = df_forecast_display.copy()
                if 'Oficina' in df_total_display.columns:
                    df_total_display = df_total_display.drop(columns=['Oficina'])

                df_total_numerico_display = df_total_numerico.copy()
                if 'Oficina' in df_total_numerico_display.columns:
                    df_total_numerico_display = df_total_numerico_display.drop(columns=['Oficina'])

                # Criar linha de TOTAL GERAL
                linha_total_geral = {}

                # Colunas de identificação (sem Oficina)
                colunas_id_geral = ['Veículo'] + [col for col in colunas_adicionais if col in df_total_display.columns] + ['Tipo_Custo']
                for col in colunas_id_geral:
                    if col in df_total_display.columns:
                        linha_total_geral[col] = '**TOTAL GERAL**'

                # Média_Mensal_Histórica total
                # 🔧 CORREÇÃO: Usar a mesma média histórica global exibida no gráfico
                # (calculada agregando totais por período e tirando a média)
                if 'Média_Mensal_Histórica' in df_total_numerico_display.columns:
                    try:
                        # Usar media_historica_total que já foi calculada corretamente
                        valor_media_total = media_historica_total
                    except NameError:
                        valor_media_total = None
                    
                    if isinstance(valor_media_total, (int, float)):
                        linha_total_geral['Média_Mensal_Histórica'] = (
                            formatar_monetario(valor_media_total)
                        )
                    else:
                        # Fallback: somar médias individuais (não ideal, mas melhor que nada)
                        total_media_geral = (
                            df_total_numerico_display['Média_Mensal_Histórica'].sum()
                        )
                        linha_total_geral['Média_Mensal_Histórica'] = (
                            formatar_monetario(total_media_geral)
                        )

                # Totais por mês
                for col in colunas_meses:
                    if col in df_total_numerico_display.columns:
                        total_col_geral = df_total_numerico_display[col].sum()
                        linha_total_geral[col] = formatar_monetario(total_col_geral)

                # Total_Forecast geral
                if 'Total_Forecast' in df_total_numerico_display.columns:
                    linha_total_geral['Total_Forecast'] = formatar_monetario(total_geral)

                # Ordenar colunas conforme df_total_display
                linha_total_ordenada_geral = {}
                for col in df_total_display.columns:
                    linha_total_ordenada_geral[col] = linha_total_geral.get(col, '')

                # Adicionar linha TOTAL GERAL ao final
                df_total_display = pd.concat(
                    [df_total_display, pd.DataFrame([linha_total_ordenada_geral])],
                    ignore_index=True
                )

                st.dataframe(df_total_display, use_container_width=True)
        except Exception:
            pass

        # Botão de download da tabela
        if st.button(
            "📥 Baixar Tabela Forecast (Excel)",
            use_container_width=True,
            key="download_tabela_forecast"
        ):
            with st.spinner("Gerando arquivo da tabela..."):
                try:
                    # Criar DataFrame completo para download (com todas as oficinas e totais)
                    df_download_list = []
                    
                    for oficina in sorted(oficinas):
                        # Dados da oficina (sem formatação para manter valores numéricos)
                        df_oficina_download = df_forecast[df_forecast['Oficina'] == oficina].copy()
                        
                        # Adicionar linha de total da oficina
                        linha_total_download = {'Oficina': oficina}
                        df_oficina_numerico = df_forecast[df_forecast['Oficina'] == oficina].copy()
                        df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])
                        
                        # Adicionar colunas de identificação
                        colunas_id = ['Veículo'] + [col for col in colunas_adicionais if col in df_oficina_numerico.columns] + ['Tipo_Custo']
                        for col in colunas_id:
                            if col in df_oficina_numerico.columns:
                                linha_total_download[col] = 'TOTAL'
                        
                        # Adicionar totais
                        colunas_totais = ['Média_Mensal_Histórica'] + colunas_meses + ['Total_Forecast']
                        for col in colunas_totais:
                            if col in df_oficina_numerico.columns:
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
                    file_name = f"Forecast_tabela_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    file_path = os.path.join(downloads_path, file_name)
                    
                    # Salvar arquivo diretamente na pasta Downloads
                    with pd.ExcelWriter(
                        file_path, engine='openpyxl'
                    ) as writer:
                        df_download.to_excel(
                            writer, index=False, sheet_name='Forecast'
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
        if not tem_oficina:
            colunas_faltando.append("Oficina")
        if not tem_veiculo:
            colunas_faltando.append("Veículo")
        st.info(f"ℹ️ Colunas necessárias não encontradas para criar a tabela: {', '.join(colunas_faltando)}")
    
    # Resumo
    st.markdown("#### 📈 Resumo do Forecast")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_forecast = df_forecast['Total_Forecast'].sum() if 'Total_Forecast' in df_forecast.columns else 0
        st.metric("Total Forecast", f"R$ {total_forecast:,.2f}")
    
    with col2:
        custos_fixos = df_forecast[df_forecast['Tipo_Custo'] == 'Fixo']['Total_Forecast'].sum() if 'Total_Forecast' in df_forecast.columns else 0
        st.metric("Custos Fixos", f"R$ {custos_fixos:,.2f}")
    
    with col3:
        custos_variaveis = df_forecast[df_forecast['Tipo_Custo'] == 'Variável']['Total_Forecast'].sum() if 'Total_Forecast' in df_forecast.columns else 0
        st.metric("Custos Variáveis", f"R$ {custos_variaveis:,.2f}")

# Footer
st.markdown("---")
st.info("💡 Forecast TC - Análise preditiva e previsões")

