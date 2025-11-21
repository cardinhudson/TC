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

# Modo de visualização fixo: apenas Custo Total
tipo_visualizacao = "Custo Total"
st.sidebar.markdown("---")

# Botão para limpar cache (útil após mudanças no código)
if st.sidebar.button("🗑️ Limpar Cache", help="Limpa o cache do Streamlit para forçar recálculo"):
    st.cache_data.clear()
    st.sidebar.success("✅ Cache limpo! Recarregue a página.")
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
    # Converter para numérico caso seja categórico
    valor_series = pd.to_numeric(df_filtrado['Valor'], errors='coerce')
    valor_total = valor_series.sum()
    st.sidebar.write(f"**Total Valor:** R$ {valor_total:,.2f}")
if 'Total' in df_filtrado.columns:
    # Converter para numérico caso seja categórico
    total_series = pd.to_numeric(df_filtrado['Total'], errors='coerce')
    total_sum = total_series.sum()
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
    
    # 🔧 NOVA LÓGICA: Contar quantos meses até o último período têm valores (Total != 0)
    # Isso limita o filtro apenas aos meses que realmente têm dados (inclui valores negativos, exclui apenas zeros)
    meses_com_valor = len(meses_historicos_disponiveis)  # Valor padrão
    if meses_historicos_disponiveis and not df_filtrado.empty and 'Período' in df_filtrado.columns and 'Total' in df_filtrado.columns:
        # Extrair ano do último período
        ano_referencia_contagem = None
        if ' ' in str(ultimo_periodo_dados):
            partes_periodo = str(ultimo_periodo_dados).split(' ', 1)
            if len(partes_periodo) > 1 and partes_periodo[1].isdigit():
                ano_referencia_contagem = partes_periodo[1]
        if ano_referencia_contagem is None:
            ano_referencia_contagem = str(ano_maximo) if 'ano_maximo' in locals() else str(datetime.now().year)
        
        # Criar lista de períodos até o último mês selecionado
        periodos_ate_ultimo = []
        for mes in meses_historicos_disponiveis:
            # Adicionar período com ano
            periodo_com_ano = f"{mes} {ano_referencia_contagem}".lower()
            periodos_ate_ultimo.append(periodo_com_ano)
            # Também adicionar apenas o mês (para compatibilidade com dados antigos)
            periodos_ate_ultimo.append(mes.lower())
        
        # Normalizar períodos do DataFrame para comparação
        df_filtrado_copy = df_filtrado.copy()
        df_filtrado_copy['Período_Normalizado'] = df_filtrado_copy['Período'].astype(str).str.strip().str.lower()
        
        # Filtrar df_filtrado para períodos até o último mês
        mask_periodos_ate_ultimo = df_filtrado_copy['Período_Normalizado'].isin(periodos_ate_ultimo)
        df_periodos_ate_ultimo = df_filtrado_copy[mask_periodos_ate_ultimo].copy()
        
        # Contar períodos únicos que têm pelo menos uma linha com Total != 0 (inclui valores negativos, exclui apenas zeros)
        if not df_periodos_ate_ultimo.empty:
            # Verificar se há pelo menos uma linha com Total != 0 para cada período
            # Agrupar por Período_Normalizado e verificar se há algum Total != 0
            periodos_unicos = df_periodos_ate_ultimo['Período_Normalizado'].unique()
            periodos_com_valor_lista = []
            for periodo in periodos_unicos:
                df_periodo = df_periodos_ate_ultimo[df_periodos_ate_ultimo['Período_Normalizado'] == periodo]
                # Verificar se há pelo menos uma linha com Total != 0
                if (df_periodo['Total'] != 0).any():
                    periodos_com_valor_lista.append(periodo)
            meses_com_valor = len(periodos_com_valor_lista)
            
            # Se não encontrou nenhum período com valor, usar o número de meses históricos disponíveis
            if meses_com_valor == 0:
                meses_com_valor = len(meses_historicos_disponiveis)
    
    # Limitar max_value aos meses que têm valores
    max_meses_media = max(1, meses_com_valor)  # Garantir pelo menos 1
    
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
    
    # 🆕 NOVA FUNCIONALIDADE: Marcar que precisa gerar tabela completa com forecast
    st.session_state.gerar_tabela_completa_forecast = True
    
    st.success("✅ Configurações aplicadas com sucesso! Recalculando forecast...")
    st.rerun()

# Usar configurações aplicadas (se existirem) ou temporárias
if st.session_state.config_forecast_aplicada['ultimo_periodo_dados'] is not None:
    # Usar configurações aplicadas
    ultimo_periodo_dados = st.session_state.config_forecast_aplicada['ultimo_periodo_dados']
    num_meses_prever = st.session_state.config_forecast_aplicada['num_meses_prever']
    num_meses_media_salvo = st.session_state.config_forecast_aplicada['num_meses_media']
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
    
    # 🔧 CORREÇÃO: Recalcular max_meses_media baseado no novo último período e ajustar num_meses_media
    meses_historicos_disponiveis_aplicado = meses_ano[:indice_ultimo_mes + 1]
    meses_com_valor_aplicado = len(meses_historicos_disponiveis_aplicado)  # Valor padrão
    if meses_historicos_disponiveis_aplicado and not df_filtrado.empty and 'Período' in df_filtrado.columns and 'Total' in df_filtrado.columns:
        # Extrair ano do último período
        ano_referencia_contagem_aplicado = None
        if ' ' in str(ultimo_periodo_dados):
            partes_periodo = str(ultimo_periodo_dados).split(' ', 1)
            if len(partes_periodo) > 1 and partes_periodo[1].isdigit():
                ano_referencia_contagem_aplicado = partes_periodo[1]
        if ano_referencia_contagem_aplicado is None:
            ano_referencia_contagem_aplicado = str(ano_maximo) if 'ano_maximo' in locals() else str(datetime.now().year)
        
        # Criar lista de períodos até o último mês selecionado
        periodos_ate_ultimo_aplicado = []
        for mes in meses_historicos_disponiveis_aplicado:
            periodo_com_ano = f"{mes} {ano_referencia_contagem_aplicado}"
            periodos_ate_ultimo_aplicado.append(periodo_com_ano.lower())
        
        # Filtrar df_filtrado para períodos até o último mês
        periodos_no_df_aplicado = df_filtrado['Período'].astype(str).str.strip().str.lower()
        mask_periodos_ate_ultimo_aplicado = periodos_no_df_aplicado.isin(periodos_ate_ultimo_aplicado)
        df_periodos_ate_ultimo_aplicado = df_filtrado[mask_periodos_ate_ultimo_aplicado].copy()
        
        # Contar períodos únicos que têm pelo menos uma linha com Total != 0 (inclui valores negativos, exclui apenas zeros)
        if not df_periodos_ate_ultimo_aplicado.empty:
            # Normalizar períodos para comparação
            df_periodos_ate_ultimo_aplicado_copy = df_periodos_ate_ultimo_aplicado.copy()
            df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'] = df_periodos_ate_ultimo_aplicado_copy['Período'].astype(str).str.strip().str.lower()
            
            # Verificar se há pelo menos uma linha com Total != 0 para cada período
            periodos_unicos_aplicado = df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'].unique()
            periodos_com_valor_lista_aplicado = []
            for periodo in periodos_unicos_aplicado:
                df_periodo = df_periodos_ate_ultimo_aplicado_copy[df_periodos_ate_ultimo_aplicado_copy['Período_Normalizado'] == periodo]
                # Verificar se há pelo menos uma linha com Total != 0
                if (df_periodo['Total'] != 0).any():
                    periodos_com_valor_lista_aplicado.append(periodo)
            meses_com_valor_aplicado = len(periodos_com_valor_lista_aplicado)
    
    # Ajustar num_meses_media se exceder o novo máximo
    max_meses_media_aplicado = max(1, meses_com_valor_aplicado)
    num_meses_media = min(num_meses_media_salvo, max_meses_media_aplicado)
else:
    # Primeira vez - usar configurações temporárias mas não calcular ainda
    st.info("ℹ️ Configure os parâmetros acima e clique em 'Aplicar Configurações do Forecast' para calcular o forecast.")
    
    # ====================================================================
    # 📊 GRÁFICO "SOMA DO VALOR POR PERÍODO" - USANDO DADOS DA PASTA FORECAST
    # Este gráfico aparece SEMPRE que houver dados na pasta Forecast
    # ====================================================================
    st.markdown("---")
    st.markdown("### 📊 Soma do Valor por Período (Dados do Forecast)")
    
    # Função para ordenar por mês (mesma do TC_Ext)
    ORDEM_MESES_GRAFICO = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
                           'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    
    def ordenar_por_mes_forecast(df, coluna_periodo='Período'):
        """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
        df_copy = df.copy()
        
        # Se houver coluna "Ano" e múltiplos anos, ordenar por ano e mês
        if 'Ano' in df_copy.columns and df_copy['Ano'].nunique() > 1:
            # Criar coluna de ordenação: ano primeiro, depois mês
            df_copy['_ordem_ano'] = df_copy['Ano']
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
            ).fillna(999)
            df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
            df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
        else:
            # Ordenação simples por mês
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
            ).fillna(999)
            df_copy = df_copy.sort_values('_ordem_mes')
            df_copy = df_copy.drop(columns=['_ordem_mes'])
        
        return df_copy
    
    try:
        # Carregar dados do arquivo forecast gerado na pasta Forecast
        caminho_forecast_grafico = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_forecast_grafico):
            df_forecast_grafico = pd.read_parquet(caminho_forecast_grafico)
            
            # Aplicar filtros (Oficina, Veículo, USI) mas NÃO filtrar por Período
            # As variáveis já estão definidas no escopo global
            if 'Oficina' in df_forecast_grafico.columns:
                if oficina_selecionadas and "Todos" not in oficina_selecionadas:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['Oficina'].astype(str).isin(oficina_selecionadas)
                    ].copy()
            
            if 'Veículo' in df_forecast_grafico.columns:
                if veiculo_selecionados and "Todos" not in veiculo_selecionados:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['Veículo'].astype(str).isin(veiculo_selecionados)
                    ].copy()
            
            if 'USI' in df_forecast_grafico.columns:
                if usi_selecionada and "Todos" not in usi_selecionada:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['USI'].astype(str).isin(usi_selecionada)
                    ].copy()
            
            # Verificar se há coluna Total
            if 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                # Verificar se há múltiplos anos
                tem_multiplos_anos = 'Ano' in df_forecast_grafico.columns and df_forecast_grafico['Ano'].nunique() > 1
                
                if tem_multiplos_anos:
                    # Agrupar por Ano e Período
                    chart_data = df_forecast_grafico.groupby(['Ano', 'Período'])['Total'].sum().reset_index()
                    
                    # Criar coluna combinada para o rótulo do gráfico
                    chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                    
                    # Ordenar por ano e mês (usar função similar ao TC_Ext)
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período_Completo'].tolist()
                    coluna_periodo_grafico = 'Período_Completo'
                else:
                    # Agrupar apenas por Período
                    chart_data = df_forecast_grafico.groupby('Período')['Total'].sum().reset_index()
                    
                    # Ordenar por mês
                    chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                    ordem_periodos = chart_data['Período'].tolist()
                    coluna_periodo_grafico = 'Período'
                
                # Criar gráfico (mesma lógica do TC_Ext)
                grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                    x=alt.X(
                        f'{coluna_periodo_grafico}:N',
                        title='Período',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Total:Q', title='Soma do Valor (R$)'),
                    color=alt.Color(
                        'Total:Q',
                        title='Total',
                        scale=alt.Scale(scheme='blues')
                    ),
                    tooltip=[
                        alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                        alt.Tooltip('Total:Q', title='Soma do Valor', format=',.2f')
                    ]
                ).properties(
                    title='Soma do Valor por Período',
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
                    text=alt.Text('Total:Q', format=',.2f')
                )
                
                grafico_final = grafico_barras + rotulos
                st.altair_chart(grafico_final, use_container_width=True)
                
                # Mostrar resumo
                total_geral = chart_data['Total'].sum()
                st.info(f"📊 **Total Geral:** R$ {total_geral:,.2f}")
            else:
                st.warning("⚠️ Colunas 'Total' ou 'Período' não encontradas no arquivo forecast.")
        else:
            st.warning(f"⚠️ Arquivo não encontrado: {caminho_forecast_grafico}")
            st.info("ℹ️ O arquivo será gerado quando você clicar em 'Aplicar Configurações do Forecast'.")
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Soma do Valor por Período': {str(e)}")
        import traceback
        st.error(f"Detalhes: {traceback.format_exc()}")
    
    st.stop()

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
                
                # 🔧 CORREÇÃO CRÍTICA: Se há ano de referência definido, filtrar APENAS esse ano
                if ano_referencia_filtro:
                    periodo_df_tem_ano = ' ' in periodo_df_lower and len(periodo_df_lower.split(' ', 1)) > 1
                    if periodo_df_tem_ano:
                        periodo_df_ano = int(periodo_df_lower.split(' ', 1)[1]) if periodo_df_lower.split(' ', 1)[1].isdigit() else None
                        # Se o período tem ano diferente do ano de referência, NÃO incluir
                        if periodo_df_ano != ano_referencia_filtro:
                            return False
                    else:
                        # Se o período não tem ano mas há ano de referência, NÃO incluir
                        # (evita incluir períodos sem ano quando há períodos com ano)
                        return False
                
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
                    # MAS APENAS se não houver ano de referência definido
                    if not ano_referencia_filtro:
                        mes_df = periodo_df_lower.split(' ', 1)[0] if ' ' in periodo_df_lower else periodo_df_lower
                        mes_procurado = periodo_procurado.split(' ', 1)[0] if ' ' in periodo_procurado else periodo_procurado
                        
                        if mes_df == mes_procurado:
                            # Se o período procurado tem ano mas o do DF não tem, não incluir
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
        agg_dict = {'Total': 'sum'}  # Usar 'sum' para ter valores totais reais
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
                if sensibilidades_type06_dict is not None and 'Type 06' in df_forecast_base.columns:
                    # Modo detalhado: usar inflação específica do Type 06
                    type06_valor = df_forecast_base.loc[idx, 'Type 06']
                    if inflacao_type06_dict is not None and pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                        inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0
                    else:
                        inflacao_percentual = 0.0
                else:
                    # Modo global: usar inflação global
                    if inflacao_type06_dict is not None:
                        primeiro_valor = next(iter(inflacao_type06_dict.values()), 0.0)
                        inflacao_percentual = primeiro_valor / 100.0
                    else:
                        inflacao_percentual = 0.0
                
                # 7. Calcular forecast: Média_historica * (1 + variação_ajustada) * (1 + inflação)
                # Se sensibilidade = 0: variação_ajustada = 0, então forecast = Média_historica * 1.0 = Média_historica
                fator_variacao = 1.0 + variacao_ajustada
                fator_inflacao = 1.0 + inflacao_percentual
                forecast = media_historica * fator_variacao * fator_inflacao
                
                if sensibilidade == 0 and inflacao_percentual == 0 and abs(proporcao_volume - 1.0) < 0.01:
                    if abs(forecast - media_historica) > 0.01:
                        # Avisar apenas uma vez por período
                        if idx == df_forecast_base.index[0] and periodo == meses_restantes_cache[0] if meses_restantes_cache else False:
                            st.sidebar.error(f"❌ PROBLEMA: forecast ({forecast:,.2f}) ≠ média ({media_historica:,.2f}) quando deveria ser igual!")
                
                # 8. Atribuir forecast à linha
                df_forecast.loc[idx, periodo] = forecast
            
            # Total do forecast = Soma de todas as linhas (calculado automaticamente pelo pandas)
        
        # Não há necessidade de verificação final ou ajustes manuais
        # O cálculo linha a linha garante que:
        # - Se sensibilidade = 0: variação_ajustada = 0, então forecast = média_historica * 1.0 = média_historica
        # - Se inflação = 0: fator_inflacao = 1.0, então forecast = média_historica * fator_variacao * 1.0
        # - O total é sempre a soma das linhas individuais
        
        return df_forecast

    # Preparar dados para forecast usando operações vetorizadas (mais rápido)
    # Calcular forecast com cache (incluindo sensibilidades e inflação)
    # Converter sensibilidades_type06 para tuple se for dict (para ser hashable no cache)
    sens_type06_cache = tuple(sorted(sensibilidades_type06.items())) if sensibilidades_type06 is not None else None
    inflacao_type06_cache = tuple(sorted(inflacao_type06.items())) if inflacao_type06 is not None else None
    
    # 🔧 CORREÇÃO: Passar media_historica_total_padronizada para a função calcular_forecast_completo
    # para garantir que o forecast use a média correta
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
                    
                    # Verificar se existe coluna 'Custo' para determinar Tipo_Custo
                    if 'Custo' in df_base_filtrado.columns:
                        df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Custo'].apply(is_custo_fixo)
                        df_base_filtrado['Tipo_Custo'] = df_base_filtrado['Tipo_Custo'].map({True: 'Fixo', False: 'Variável'})
                    else:
                        # Se não existe coluna Custo, usar padrão 'Variável'
                        df_base_filtrado['Tipo_Custo'] = 'Variável'
                
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
                if 'Total' in df_base_para_media.columns:
                    df_medias_linha = df_base_para_media.groupby(colunas_chave_forecast_existentes, as_index=False)['Total'].mean()
                    df_medias_linha.rename(columns={'Total': 'Média_Mensal_Histórica'}, inplace=True)
                else:
                    df_medias_linha = pd.DataFrame(columns=colunas_chave_forecast_existentes + ['Média_Mensal_Histórica'])
                
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
                df_forecast_completo = df_base_filtrado.copy()
                
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
                    # Buscar volume para este período
                    volume_mes_serie = None
                    if volume_por_mes is not None and not volume_por_mes.empty:
                        periodo_str = str(periodo).strip()
                        mes_procurado = periodo_str.split(' ', 1)[0].lower() if ' ' in periodo_str else periodo_str.lower()
                        
                        # Filtrar volume para este período
                        volume_por_mes_temp = volume_por_mes.copy()
                        volume_por_mes_temp['Período_Normalizado'] = volume_por_mes_temp['Período'].astype(str).str.strip().str.lower().str.split(' ', expand=True)[0]
                        vol_mes_df = volume_por_mes_temp[volume_por_mes_temp['Período_Normalizado'] == mes_procurado].copy()
                        
                        if not vol_mes_df.empty:
                            # Agrupar por Oficina e Veículo
                            vol_mes_df = vol_mes_df.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].sum()
                            
                            # Fazer merge com df_forecast_completo
                            df_forecast_completo = df_forecast_completo.merge(
                                vol_mes_df.rename(columns={'Volume': f'Volume_{periodo}'}),
                                on=['Oficina', 'Veículo'],
                                how='left'
                            )
                            # Verificar se Volume_Medio_Historico existe antes de usar
                            if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                volume_mes_serie = df_forecast_completo[f'Volume_{periodo}'].fillna(df_forecast_completo['Volume_Medio_Historico'])
                            else:
                                volume_mes_serie = df_forecast_completo[f'Volume_{periodo}'].fillna(0.0)
                            df_forecast_completo = df_forecast_completo.drop(columns=[f'Volume_{periodo}'])
                        else:
                            # Verificar se Volume_Medio_Historico existe antes de usar
                            if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                volume_mes_serie = df_forecast_completo['Volume_Medio_Historico']
                            else:
                                volume_mes_serie = pd.Series(0.0, index=df_forecast_completo.index)
                    else:
                        # Verificar se Volume_Medio_Historico existe antes de usar
                        if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                            volume_mes_serie = df_forecast_completo['Volume_Medio_Historico']
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
                            
                            # Obter inflação
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
                    
                    # ====================================================================
                    # 🆕 TRANSFORMAR COLUNAS DE FORECAST EM LINHAS NA COLUNA "Período"
                    # ====================================================================
                    # Ao invés de ter colunas separadas para cada período, criar linhas
                    # onde cada linha tem Período = "Novembro 2025", "Dezembro 2025", etc.
                    # IMPORTANTE: Usar valores do df_forecast_bruto (mesma lógica do gráfico)
                    
                    linhas_finais = []
                    
                    # 1. Adicionar linhas históricas (já estão no df_base_filtrado)
                    # Manter apenas as colunas necessárias e adicionar Tipo = 'Histórico'
                    df_historico_linhas = df_base_filtrado.copy()
                    # Remover colunas de forecast se existirem
                    for periodo in periodos_restantes:
                        if periodo in df_historico_linhas.columns:
                            df_historico_linhas = df_historico_linhas.drop(columns=[periodo])
                    # Adicionar coluna Tipo = 'Histórico'
                    df_historico_linhas['Tipo'] = 'Histórico'
                    linhas_finais.append(df_historico_linhas)
                    
                    # 2. Criar linhas de forecast para cada período
                    # IMPORTANTE: Usar df_forecast_bruto para garantir valores iguais ao gráfico
                    df_fonte_forecast = None
                    if 'df_forecast_bruto' in locals() and df_forecast_bruto is not None and not df_forecast_bruto.empty:
                        df_fonte_forecast = df_forecast_bruto.copy()
                        st.info("✅ Usando valores do df_forecast_bruto (mesma lógica do gráfico)")
                    else:
                        # Fallback: usar df_forecast_completo com valores calculados
                        df_fonte_forecast = df_forecast_completo.copy()
                        st.info("ℹ️ Usando valores calculados do df_forecast_completo")
                    
                    for periodo in periodos_restantes:
                        if periodo in df_fonte_forecast.columns:
                            # Para cada linha única, criar uma nova linha com Período = periodo
                            colunas_chave_linha = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais
                            if 'Ano' in df_fonte_forecast.columns:
                                colunas_chave_linha.insert(2, 'Ano')
                            colunas_chave_linha = [col for col in colunas_chave_linha if col in df_fonte_forecast.columns]
                            
                            # Obter linhas únicas com valores de forecast
                            colunas_para_linha = colunas_chave_linha + [periodo]
                            df_linhas_unicas = df_fonte_forecast[colunas_para_linha].drop_duplicates(
                                subset=colunas_chave_linha
                            )
                            
                            for _, linha_original in df_linhas_unicas.iterrows():
                                nova_linha = linha_original.to_dict()
                                
                                # Definir Período como período de forecast
                                nova_linha['Período'] = str(periodo)
                                
                                # Extrair ano do período se possível
                                periodo_str = str(periodo)
                                if ' ' in periodo_str:
                                    partes = periodo_str.split(' ', 1)
                                    if len(partes) == 2 and partes[1].isdigit():
                                        nova_linha['Ano'] = int(partes[1])
                                
                                # Definir Total como valor de forecast (mesma lógica do gráfico)
                                nova_linha['Total'] = float(nova_linha.get(periodo, 0.0))
                                
                                # Remover coluna do período (já está em 'Total')
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
                                
                                linhas_finais.append(pd.DataFrame([nova_linha]))
                    
                    # 3. Combinar todas as linhas em um único DataFrame
                    if linhas_finais:
                        df_forecast_final = pd.concat(linhas_finais, ignore_index=True)
                        
                        # Garantir que todas as colunas estejam presentes
                        todas_colunas = sorted(set([col for df in linhas_finais for col in df.columns]))
                        df_forecast_final = df_forecast_final.reindex(columns=todas_colunas)
                        
                        # Substituir df_forecast_completo pelo resultado final
                        df_forecast_completo = df_forecast_final
                        
                        st.info(f"✅ Tabela criada com {len(df_forecast_completo):,} linhas (histórico + forecast)")
                        st.info(f"📊 Períodos de forecast incluídos: {', '.join(periodos_restantes)}")
                        
                        # Debug: Verificar períodos únicos e anos
                        if 'Período' in df_forecast_completo.columns:
                            periodos_unicos = df_forecast_completo['Período'].unique()
                            st.info(f"📅 Períodos únicos no arquivo: {len(periodos_unicos)} períodos")
                            if 'Ano' in df_forecast_completo.columns:
                                anos_unicos = df_forecast_completo['Ano'].unique()
                                st.info(f"📅 Anos únicos no arquivo: {sorted(anos_unicos)}")
                    else:
                        st.warning("⚠️ Nenhuma linha foi criada!")
                        # Se não criou linhas, usar df_base_filtrado como base
                        df_forecast_completo = df_base_filtrado.copy()
                        df_forecast_completo['Tipo'] = 'Histórico'
                        st.info("ℹ️ Usando apenas dados históricos (sem forecast)")
                    
                    # Verificar se df_forecast_completo existe e não está vazio
                    if df_forecast_completo is None or df_forecast_completo.empty:
                        st.error("❌ Erro: DataFrame vazio! Não é possível salvar.")
                        st.stop()
                    
                    # Remover colunas especificadas
                    colunas_para_remover = ['Nºconta', 'Nºdoc.ref.', 'Dt.lçto.', 'QTD', 'Nºdoc.ref', 'Doc.compra', 'Texto breve', 'Material', 'Usuário']
                    colunas_para_remover_existentes = [col for col in colunas_para_remover if col in df_forecast_completo.columns]
                    if colunas_para_remover_existentes:
                        df_forecast_completo = df_forecast_completo.drop(columns=colunas_para_remover_existentes)
                    
                    # Remover linhas com valores nulos em colunas importantes para reduzir tamanho do arquivo
                    linhas_antes = len(df_forecast_completo)
                    
                    # 1. Remover linhas onde Total é nulo ou zero (se a coluna Total existir)
                    if 'Total' in df_forecast_completo.columns:
                        mask_total_valido = df_forecast_completo['Total'].notna() & (df_forecast_completo['Total'] != 0)
                        df_forecast_completo = df_forecast_completo[mask_total_valido].copy()
                    
                    # 2. Remover linhas onde colunas críticas são todas nulas
                    colunas_criticas = ['Oficina', 'Veículo', 'Período']
                    colunas_criticas_existentes = [col for col in colunas_criticas if col in df_forecast_completo.columns]
                    
                    if colunas_criticas_existentes:
                        # Remover linhas onde todas as colunas críticas são nulas
                        mask_linhas_validas = df_forecast_completo[colunas_criticas_existentes].notna().any(axis=1)
                        df_forecast_completo = df_forecast_completo[mask_linhas_validas].copy()
                    
                    linhas_depois = len(df_forecast_completo)
                    linhas_removidas = linhas_antes - linhas_depois
                    
                    if linhas_removidas > 0:
                        st.info(f"🧹 Removidas {linhas_removidas:,} linhas com valores nulos/zerados (de {linhas_antes:,} para {linhas_depois:,})")
                    
                    # 3. Remover colunas que são completamente nulas (para otimizar ainda mais)
                    colunas_todas_nulas = df_forecast_completo.columns[df_forecast_completo.isna().all()].tolist()
                    if colunas_todas_nulas:
                        df_forecast_completo = df_forecast_completo.drop(columns=colunas_todas_nulas)
                        st.info(f"🧹 Removidas {len(colunas_todas_nulas)} colunas completamente nulas: {', '.join(colunas_todas_nulas[:5])}{'...' if len(colunas_todas_nulas) > 5 else ''}")
                    
                    # Criar pasta Forecast em dados/Forecast (ANTES de tentar salvar)
                    pasta_dados = "dados"
                    pasta_forecast = os.path.join(pasta_dados, "Forecast")
                    
                    st.info(f"📁 Preparando para salvar em: {os.path.abspath(pasta_forecast)}")
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
                    except Exception as e_pasta:
                        st.error(f"❌ Erro ao criar pasta Forecast: {str(e_pasta)}")
                        import traceback
                        st.error(f"Detalhes: {traceback.format_exc()}")
                        # Fallback: tentar criar na raiz
                        pasta_forecast = "Forecast"
                        try:
                            os.makedirs(pasta_forecast, exist_ok=True)
                            st.warning(f"⚠️ Usando pasta Forecast na raiz: {os.path.abspath(pasta_forecast)}")
                        except:
                            pasta_forecast = "."  # Último fallback: diretório atual
                            st.error(f"❌ Usando diretório atual como fallback: {os.path.abspath(pasta_forecast)}")
                    
                    # Usar nome fixo para substituir arquivo existente (não usar timestamp)
                    nome_arquivo_base = "forecast_completo"
                    
                    # ============================================================
                    # PASSO 1: Copiar arquivo completo de volume histórico
                    # ============================================================
                    try:
                        # Carregar arquivo completo de volume histórico (antes dos filtros)
                        caminho_vol_historico_original = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
                        
                        if os.path.exists(caminho_vol_historico_original):
                            # Copiar arquivo completo para a pasta Forecast
                            caminho_vol_historico_destino = os.path.join(pasta_forecast, "df_vol_historico.parquet")
                            shutil.copy2(caminho_vol_historico_original, caminho_vol_historico_destino)
                            
                            # Também salvar em Excel
                            df_vol_historico_completo = pd.read_parquet(caminho_vol_historico_original)
                            caminho_vol_historico_excel = os.path.join(pasta_forecast, "df_vol_historico.xlsx")
                            df_vol_historico_completo.to_excel(caminho_vol_historico_excel, index=False, engine='openpyxl')
                            
                            st.success(f"✅ Arquivo de volume histórico copiado: {os.path.abspath(caminho_vol_historico_destino)}")
                            st.info(f"   📊 Total de linhas: {len(df_vol_historico_completo):,}")
                        else:
                            st.warning(f"⚠️ Arquivo de volume histórico não encontrado: {caminho_vol_historico_original}")
                    except Exception as e_volume:
                        st.warning(f"⚠️ Erro ao copiar arquivo de volume histórico: {str(e_volume)}")
                        import traceback
                        st.error(f"Detalhes: {traceback.format_exc()}")
                    
                    # ============================================================
                    # PASSO 2: Salvar forecast_completo
                    # ============================================================
                    # Salvar em parquet (sempre substituir se existir)
                    caminho_parquet = os.path.join(pasta_forecast, f"{nome_arquivo_base}.parquet")
                    caminho_parquet_absoluto = os.path.abspath(caminho_parquet)
                    try:
                        st.info(f"💾 Salvando Parquet em: {caminho_parquet_absoluto}")
                        st.info(f"📊 Total de linhas: {len(df_forecast_completo):,}")
                        df_forecast_completo.to_parquet(caminho_parquet, index=False, engine='pyarrow')
                        
                        # Verificar se foi salvo
                        if os.path.exists(caminho_parquet):
                            tamanho_arquivo = os.path.getsize(caminho_parquet) / (1024 * 1024)  # MB
                            st.success(f"✅ Parquet salvo com sucesso!")
                            st.info(f"   📄 Arquivo: {caminho_parquet_absoluto}")
                            st.info(f"   📏 Tamanho: {tamanho_arquivo:.2f} MB")
                        else:
                            st.error(f"❌ Arquivo Parquet não foi criado: {caminho_parquet_absoluto}")
                    except Exception as e_parquet:
                        st.error(f"❌ Erro ao salvar Parquet: {str(e_parquet)}")
                        import traceback
                        st.error(f"Detalhes: {traceback.format_exc()}")
                    
                    # Salvar em excel (sempre substituir se existir)
                    caminho_excel = os.path.join(pasta_forecast, f"{nome_arquivo_base}.xlsx")
                    caminho_excel_absoluto = os.path.abspath(caminho_excel)
                    try:
                        st.info(f"💾 Salvando Excel em: {caminho_excel_absoluto}")
                        st.info(f"📊 Total de linhas para salvar: {len(df_forecast_completo):,}")
                        
                        # Para arquivos grandes, pode ser necessário usar xlsxwriter ou dividir em chunks
                        # Primeiro, tentar salvar normalmente (mode='w' substitui arquivo existente)
                        with pd.ExcelWriter(caminho_excel, engine='openpyxl', mode='w') as writer:
                            df_forecast_completo.to_excel(writer, index=False, sheet_name='Forecast')
                        
                        # Verificar se o arquivo foi criado
                        if os.path.exists(caminho_excel):
                            tamanho_arquivo = os.path.getsize(caminho_excel) / (1024 * 1024)  # Tamanho em MB
                            st.success(f"✅ Excel salvo/substituído com sucesso!")
                            st.info(f"   📄 Arquivo: {caminho_excel_absoluto}")
                            st.info(f"   📏 Tamanho: {tamanho_arquivo:.2f} MB")
                        else:
                            st.error(f"❌ Arquivo Excel não foi criado: {caminho_excel_absoluto}")
                            # Tentar salvar com xlsxwriter como alternativa
                            try:
                                import xlsxwriter
                                st.info(f"🔄 Tentando salvar com xlsxwriter...")
                                with pd.ExcelWriter(caminho_excel, engine='xlsxwriter') as writer:
                                    df_forecast_completo.to_excel(writer, index=False, sheet_name='Forecast')
                                if os.path.exists(caminho_excel):
                                    tamanho_arquivo = os.path.getsize(caminho_excel) / (1024 * 1024)
                                    st.success(f"✅ Excel salvo com xlsxwriter: {os.path.abspath(caminho_excel)} ({tamanho_arquivo:.2f} MB)")
                            except Exception as e_excel_alt:
                                st.error(f"❌ Erro ao salvar Excel com xlsxwriter: {str(e_excel_alt)}")
                                import traceback
                                st.error(f"Detalhes: {traceback.format_exc()}")
                    except Exception as e_excel:
                        st.error(f"❌ Erro ao salvar Excel: {str(e_excel)}")
                        import traceback
                        st.error(f"Detalhes: {traceback.format_exc()}")
                        # Tentar salvar com xlsxwriter como alternativa
                        try:
                            import xlsxwriter
                            st.info(f"🔄 Tentando salvar com xlsxwriter como alternativa...")
                            with pd.ExcelWriter(caminho_excel, engine='xlsxwriter') as writer:
                                df_forecast_completo.to_excel(writer, index=False, sheet_name='Forecast')
                            if os.path.exists(caminho_excel):
                                tamanho_arquivo = os.path.getsize(caminho_excel) / (1024 * 1024)
                                st.success(f"✅ Excel salvo com xlsxwriter: {os.path.abspath(caminho_excel)} ({tamanho_arquivo:.2f} MB)")
                        except Exception as e_excel_alt:
                            st.error(f"❌ Erro ao salvar Excel com xlsxwriter: {str(e_excel_alt)}")
                            import traceback
                            st.error(f"Detalhes: {traceback.format_exc()}")
                    
                    st.success(f"✅ Tabela completa gerada com sucesso!")
                    st.info(f"📁 Arquivos salvos em: **{pasta_forecast}/**")
                    if os.path.exists(caminho_parquet):
                        st.info(f"   ✅ {nome_arquivo_base}.parquet")
                    if os.path.exists(caminho_excel):
                        st.info(f"   ✅ {nome_arquivo_base}.xlsx")
                    st.info(f"📊 Total de linhas: {len(df_forecast_completo):,}")
                    
                    # ====================================================================
                    # 🆕 CRIAR LINHAS DE FORECAST E SALVAR NO df_final_historico.parquet
                    # ====================================================================
                    st.markdown("---")
                    st.markdown("### 📝 Gerando arquivo consolidado com histórico + forecast")
                    
                    try:
                        with st.spinner("🔄 Criando linhas de forecast e consolidando com histórico..."):
                            # 1. Carregar dados históricos originais (sem filtros, para manter todos os dados)
                            caminho_historico_consolidado = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
                            
                            # Sempre tentar carregar o arquivo, mas se não existir, usar df_base_completo
                            df_historico_completo = None
                            if os.path.exists(caminho_historico_consolidado):
                                # Carregar histórico completo do arquivo existente
                                df_historico_completo = pd.read_parquet(caminho_historico_consolidado)
                            else:
                                # Se o arquivo não existir, usar os dados do df_base_completo (já carregado anteriormente)
                                st.info(f"ℹ️ Arquivo {os.path.basename(caminho_historico_consolidado)} não encontrado. Usando dados da base original.")
                                if df_base_completo is not None and not df_base_completo.empty:
                                    df_historico_completo = df_base_completo.copy()
                            
                            # Continuar com o processamento (sempre gerar/substituir o arquivo)
                            # Se não tiver dados históricos, usar df_base_completo como fallback
                            if df_historico_completo is None or df_historico_completo.empty:
                                if df_base_completo is not None and not df_base_completo.empty:
                                    df_historico_completo = df_base_completo.copy()
                                    st.info("ℹ️ Usando dados da base original como histórico.")
                            
                            # SEMPRE continuar para gerar/substituir o arquivo, mesmo que não tenha dados históricos
                            # (os dados de forecast serão adicionados)
                            if df_historico_completo is not None and not df_historico_completo.empty:
                                
                                # Adicionar coluna Tipo se não existir (para identificar histórico vs forecast)
                                if 'Tipo' not in df_historico_completo.columns:
                                    df_historico_completo['Tipo'] = 'Histórico'
                                
                                # 2. Agrupar dados históricos conforme necessário (mesma lógica do modo Custo Total)
                                # Identificar colunas de agrupamento (remover colunas que não devem ser agrupadas)
                                colunas_para_agrupar = ['Oficina', 'Veículo', 'Período']
                                if 'Ano' in df_historico_completo.columns:
                                    colunas_para_agrupar.insert(2, 'Ano')
                                if 'Tipo_Custo' in df_historico_completo.columns:
                                    colunas_para_agrupar.append('Tipo_Custo')
                                
                                # Adicionar colunas adicionais se existirem
                                colunas_adicionais_para_agrupar = [col for col in colunas_adicionais if col in df_historico_completo.columns]
                                colunas_para_agrupar.extend(colunas_adicionais_para_agrupar)
                                
                                # Colunas numéricas para somar
                                colunas_numericas = ['Total']
                                if 'Volume' in df_historico_completo.columns:
                                    colunas_numericas.append('Volume')
                                
                                # Agrupar histórico (somando valores por chave única)
                                df_historico_agrupado = df_historico_completo.groupby(
                                    [col for col in colunas_para_agrupar if col in df_historico_completo.columns],
                                    as_index=False
                                )[colunas_numericas].sum()
                                
                                # Manter outras colunas importantes (pegar primeiro valor de cada grupo)
                                colunas_manter = [col for col in df_historico_completo.columns 
                                                 if col not in colunas_para_agrupar and col not in colunas_numericas and col != 'Tipo']
                                if colunas_manter:
                                    df_historico_agrupado = df_historico_agrupado.merge(
                                        df_historico_completo[colunas_para_agrupar + colunas_manter].drop_duplicates(
                                            subset=colunas_para_agrupar
                                        ),
                                        on=colunas_para_agrupar,
                                        how='left'
                                    )
                                
                                # Garantir que Tipo = 'Histórico'
                                df_historico_agrupado['Tipo'] = 'Histórico'
                                
                                # 3. Criar linhas de forecast a partir de df_forecast_completo
                                linhas_forecast = []
                                
                                # Para cada período de forecast
                                for periodo in periodos_restantes:
                                    # Para cada linha única em df_forecast_completo
                                    colunas_chave_linha = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_para_agrupar
                                    if 'Ano' in df_forecast_completo.columns:
                                        colunas_chave_linha.insert(2, 'Ano')
                                    
                                    # Obter linhas únicas (uma por combinação de chave)
                                    # Verificar se as colunas existem antes de acessá-las
                                    colunas_para_selecionar = colunas_chave_linha.copy()
                                    if 'Média_Mensal_Histórica' in df_forecast_completo.columns:
                                        colunas_para_selecionar.append('Média_Mensal_Histórica')
                                    if 'Volume_Medio_Historico' in df_forecast_completo.columns:
                                        colunas_para_selecionar.append('Volume_Medio_Historico')
                                    
                                    # Filtrar apenas colunas que existem
                                    colunas_para_selecionar = [col for col in colunas_para_selecionar if col in df_forecast_completo.columns]
                                    
                                    df_linhas_unicas = df_forecast_completo[colunas_para_selecionar].drop_duplicates(
                                        subset=colunas_chave_linha
                                    )
                                    
                                    for _, linha_original in df_linhas_unicas.iterrows():
                                        # Criar nova linha de forecast
                                        nova_linha = linha_original.to_dict()
                                        
                                        # Definir Período como período de forecast
                                        nova_linha['Período'] = str(periodo)
                                        
                                        # Extrair ano do período se possível
                                        periodo_str = str(periodo)
                                        if ' ' in periodo_str:
                                            partes = periodo_str.split(' ', 1)
                                            if len(partes) == 2 and partes[1].isdigit():
                                                nova_linha['Ano'] = int(partes[1])
                                        
                                        # Obter valor de forecast calculado
                                        # Buscar linha correspondente em df_forecast_completo
                                        mask = True
                                        for col in colunas_chave_linha:
                                            if col in df_forecast_completo.columns:
                                                mask = mask & (df_forecast_completo[col] == linha_original[col])
                                        
                                        linha_forecast = df_forecast_completo[mask]
                                        if not linha_forecast.empty and periodo in linha_forecast.columns:
                                            valor_forecast = float(linha_forecast[periodo].iloc[0])
                                        else:
                                            # Calcular forecast na hora se não estiver na coluna
                                            media_historica = float(linha_original.get('Média_Mensal_Histórica', 0.0))
                                            volume_medio_historico = float(linha_original.get('Volume_Medio_Historico', 0.0))
                                            
                                            # Buscar volume do período
                                            volume_mes = volume_medio_historico
                                            if volume_por_mes is not None and not volume_por_mes.empty:
                                                periodo_str_norm = str(periodo).strip().lower().split(' ', 1)[0]
                                                volume_por_mes_temp = volume_por_mes.copy()
                                                volume_por_mes_temp['Período_Norm'] = volume_por_mes_temp['Período'].astype(str).str.strip().str.lower().str.split(' ', expand=True)[0]
                                                vol_mes_df = volume_por_mes_temp[volume_por_mes_temp['Período_Norm'] == periodo_str_norm].copy()
                                                
                                                if not vol_mes_df.empty:
                                                    # Filtrar por Oficina e Veículo
                                                    vol_mes_filtrado = vol_mes_df[
                                                        (vol_mes_df['Oficina'] == linha_original['Oficina']) &
                                                        (vol_mes_df['Veículo'] == linha_original['Veículo'])
                                                    ]
                                                    if not vol_mes_filtrado.empty:
                                                        volume_mes = float(vol_mes_filtrado['Volume'].sum())
                                            
                                            # Calcular forecast
                                            tipo_custo = linha_original.get('Tipo_Custo', 'Variável')
                                            if volume_medio_historico > 0:
                                                proporcao_volume = volume_mes / volume_medio_historico
                                                variacao_percentual = proporcao_volume - 1.0
                                            else:
                                                variacao_percentual = 0.0
                                            
                                            # Obter sensibilidade
                                            if sensibilidades_type06_dict is not None and 'Type 06' in linha_original:
                                                type06_valor = linha_original.get('Type 06')
                                                if pd.notna(type06_valor) and type06_valor in sensibilidades_type06_dict:
                                                    sensibilidade = sensibilidades_type06_dict[type06_valor]
                                                else:
                                                    sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                                            else:
                                                sensibilidade = sensibilidade_fixo if tipo_custo == 'Fixo' else sensibilidade_variavel
                                            
                                            variacao_ajustada = variacao_percentual * sensibilidade
                                            
                                            # Obter inflação
                                            if inflacao_type06_dict is not None and 'Type 06' in linha_original:
                                                type06_valor = linha_original.get('Type 06')
                                                if pd.notna(type06_valor) and type06_valor in inflacao_type06_dict:
                                                    inflacao_percentual = inflacao_type06_dict[type06_valor] / 100.0
                                                else:
                                                    inflacao_percentual = 0.0
                                            else:
                                                inflacao_percentual = 0.0
                                            
                                            fator_variacao = 1.0 + variacao_ajustada
                                            fator_inflacao = 1.0 + inflacao_percentual
                                            valor_forecast = media_historica * fator_variacao * fator_inflacao
                                        
                                        # Definir Total como valor de forecast
                                        nova_linha['Total'] = valor_forecast
                                        
                                        # Definir Volume (usar volume do período)
                                        if volume_por_mes is not None and not volume_por_mes.empty:
                                            periodo_str_norm = str(periodo).strip().lower().split(' ', 1)[0]
                                            volume_por_mes_temp = volume_por_mes.copy()
                                            volume_por_mes_temp['Período_Norm'] = volume_por_mes_temp['Período'].astype(str).str.strip().str.lower().str.split(' ', expand=True)[0]
                                            vol_mes_df = volume_por_mes_temp[volume_por_mes_temp['Período_Norm'] == periodo_str_norm].copy()
                                            
                                            if not vol_mes_df.empty:
                                                vol_mes_filtrado = vol_mes_df[
                                                    (vol_mes_df['Oficina'] == linha_original['Oficina']) &
                                                    (vol_mes_df['Veículo'] == linha_original['Veículo'])
                                                ]
                                                if not vol_mes_filtrado.empty:
                                                    nova_linha['Volume'] = float(vol_mes_filtrado['Volume'].sum())
                                                else:
                                                    nova_linha['Volume'] = volume_medio_historico
                                            else:
                                                nova_linha['Volume'] = volume_medio_historico
                                        else:
                                            nova_linha['Volume'] = volume_medio_historico
                                        
                                        # Definir Tipo como 'Forecast'
                                        nova_linha['Tipo'] = 'Forecast'
                                        
                                        # Remover colunas que não devem estar no arquivo final
                                        colunas_remover_linha = ['Média_Mensal_Histórica', 'Volume_Medio_Historico'] + [p for p in periodos_restantes if p != periodo]
                                        for col_remover in colunas_remover_linha:
                                            if col_remover in nova_linha:
                                                del nova_linha[col_remover]
                                        
                                        linhas_forecast.append(nova_linha)
                                
                                # 4. Criar DataFrame com linhas de forecast
                                if linhas_forecast:
                                    df_forecast_linhas = pd.DataFrame(linhas_forecast)
                                    
                                    # 5. Combinar histórico agrupado + forecast
                                    # Garantir que todas as colunas estejam presentes em ambos
                                    colunas_comuns = list(set(df_historico_agrupado.columns) & set(df_forecast_linhas.columns))
                                    colunas_historico_faltantes = [col for col in df_forecast_linhas.columns if col not in df_historico_agrupado.columns]
                                    colunas_forecast_faltantes = [col for col in df_historico_agrupado.columns if col not in df_forecast_linhas.columns]
                                    
                                    # Adicionar colunas faltantes com valores padrão
                                    for col in colunas_historico_faltantes:
                                        df_historico_agrupado[col] = None
                                    for col in colunas_forecast_faltantes:
                                        df_forecast_linhas[col] = None
                                    
                                    # Reordenar colunas para que sejam iguais
                                    todas_colunas = sorted(set(df_historico_agrupado.columns) | set(df_forecast_linhas.columns))
                                    df_historico_agrupado = df_historico_agrupado.reindex(columns=todas_colunas)
                                    df_forecast_linhas = df_forecast_linhas.reindex(columns=todas_colunas)
                                    
                                    # Combinar
                                    df_consolidado_final = pd.concat([df_historico_agrupado, df_forecast_linhas], ignore_index=True)
                                    
                                    # 6. Salvar arquivos na pasta dados\historico_consolidado
                                    try:
                                        pasta_historico_consolidado = os.path.join("dados", "historico_consolidado")
                                        
                                        # Fazer cópia do arquivo original ANTES de atualizar (df_final_historico.parquet)
                                        # Isso preserva o estado anterior antes de adicionar os dados de forecast
                                        caminho_forecast = os.path.join(pasta_historico_consolidado, "df_final_historico_forecast.parquet")
                                        import shutil
                                        
                                        # Sempre fazer cópia se o arquivo original existir
                                        if os.path.exists(caminho_historico_consolidado):
                                            shutil.copy2(caminho_historico_consolidado, caminho_forecast)
                                            st.info(f"📦 Arquivo forecast criado: {os.path.basename(caminho_forecast)}")
                                            
                                            # Gerar também em Excel
                                            caminho_forecast_excel = caminho_forecast.replace('.parquet', '.xlsx')
                                            try:
                                                df_historico_completo.to_excel(caminho_forecast_excel, index=False, engine='openpyxl')
                                                st.info(f"📊 Arquivo forecast Excel criado: {os.path.basename(caminho_forecast_excel)}")
                                            except Exception as e_forecast_excel:
                                                st.warning(f"⚠️ Erro ao criar arquivo forecast Excel: {str(e_forecast_excel)}")
                                        else:
                                            # Se o arquivo original não existir, criar o forecast a partir do consolidado
                                            df_consolidado_final.to_parquet(caminho_forecast, index=False, engine='pyarrow')
                                            st.info(f"📦 Arquivo forecast criado (novo): {os.path.basename(caminho_forecast)}")
                                            
                                            # Gerar também em Excel
                                            caminho_forecast_excel = caminho_forecast.replace('.parquet', '.xlsx')
                                            try:
                                                df_consolidado_final.to_excel(caminho_forecast_excel, index=False, engine='openpyxl')
                                                st.info(f"📊 Arquivo forecast Excel criado: {os.path.basename(caminho_forecast_excel)}")
                                            except Exception as e_forecast_excel:
                                                st.warning(f"⚠️ Erro ao criar arquivo forecast Excel: {str(e_forecast_excel)}")
                                        
                                        # SEMPRE salvar/substituir df_final_historico.parquet (arquivo consolidado com histórico + forecast)
                                        # Criar pasta se não existir
                                        if not os.path.exists(pasta_historico_consolidado):
                                            os.makedirs(pasta_historico_consolidado)
                                        
                                        df_consolidado_final.to_parquet(caminho_historico_consolidado, index=False, engine='pyarrow')
                                        st.success(f"✅ Arquivo consolidado salvo/substituído: {os.path.basename(caminho_historico_consolidado)}")
                                        st.info(f"📊 Total de linhas: {len(df_consolidado_final):,} (Histórico: {len(df_historico_agrupado):,} + Forecast: {len(df_forecast_linhas):,})")
                                        
                                        # Gerar df_ke5z_historico.parquet
                                        # Este arquivo deve conter dados agrupados por KE5Z (se houver coluna relacionada)
                                        # Por enquanto, vamos criar uma versão agrupada do df_consolidado_final
                                        # Se não houver coluna específica para KE5Z, vamos usar uma agregação similar
                                        caminho_ke5z = os.path.join(pasta_historico_consolidado, "df_ke5z_historico.parquet")
                                        
                                        # Verificar se há colunas relacionadas a KE5Z ou agrupar por chave única
                                        # Por padrão, vamos agrupar por Oficina, Veículo, Período, Ano (se existir)
                                        colunas_agrupamento_ke5z = ['Oficina', 'Veículo', 'Período']
                                        if 'Ano' in df_consolidado_final.columns:
                                            colunas_agrupamento_ke5z.insert(2, 'Ano')
                                        if 'Tipo_Custo' in df_consolidado_final.columns:
                                            colunas_agrupamento_ke5z.append('Tipo_Custo')
                                        
                                        # Filtrar apenas colunas que existem
                                        colunas_agrupamento_ke5z = [col for col in colunas_agrupamento_ke5z if col in df_consolidado_final.columns]
                                        
                                        # Agrupar e somar valores numéricos
                                        colunas_numericas_ke5z = ['Total']
                                        if 'Volume' in df_consolidado_final.columns:
                                            colunas_numericas_ke5z.append('Volume')
                                        
                                        df_ke5z_historico = df_consolidado_final.groupby(
                                            colunas_agrupamento_ke5z,
                                            as_index=False
                                        )[colunas_numericas_ke5z].sum()
                                        
                                        # Manter outras colunas importantes (primeiro valor de cada grupo)
                                        colunas_manter_ke5z = [col for col in df_consolidado_final.columns 
                                                               if col not in colunas_agrupamento_ke5z and col not in colunas_numericas_ke5z]
                                        if colunas_manter_ke5z:
                                            df_ke5z_historico = df_ke5z_historico.merge(
                                                df_consolidado_final[colunas_agrupamento_ke5z + colunas_manter_ke5z].drop_duplicates(
                                                    subset=colunas_agrupamento_ke5z
                                                ),
                                                on=colunas_agrupamento_ke5z,
                                                how='left'
                                            )
                                        
                                        # Salvar df_ke5z_historico.parquet
                                        df_ke5z_historico.to_parquet(caminho_ke5z, index=False, engine='pyarrow')
                                        st.success(f"✅ Arquivo KE5Z salvo: {os.path.basename(caminho_ke5z)}")
                                        st.info(f"📊 Total de linhas KE5Z: {len(df_ke5z_historico):,}")
                                        
                                    except Exception as e_salvar:
                                        st.error(f"❌ Erro ao salvar arquivos consolidados: {str(e_salvar)}")
                                        import traceback
                                        st.error(f"Detalhes: {traceback.format_exc()}")
                                else:
                                    st.warning("⚠️ Nenhuma linha de forecast foi criada.")
                    except Exception as e_consolidado:
                        st.error(f"❌ Erro ao consolidar histórico + forecast: {str(e_consolidado)}")
                        import traceback
                        st.error(f"Detalhes: {traceback.format_exc()}")
                    
                    # Limpar flag
                    st.session_state.gerar_tabela_completa_forecast = False
                    
        except Exception as e:
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
    # 📊 GRÁFICO "SOMA DO VALOR POR PERÍODO" - USANDO DADOS DA PASTA FORECAST
    # Este gráfico aparece SEMPRE que houver dados na pasta Forecast
    # ====================================================================
    st.markdown("---")
    st.markdown("### 📊 Soma do Valor por Período (Dados do Forecast)")
    
    # Função para ordenar por mês (mesma do TC_Ext)
    ORDEM_MESES_GRAFICO_FINAL = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
                                  'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    
    def ordenar_por_mes_forecast_final(df, coluna_periodo='Período'):
        """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
        df_copy = df.copy()
        
        # Se houver coluna "Ano" e múltiplos anos, ordenar por ano e mês
        if 'Ano' in df_copy.columns and df_copy['Ano'].nunique() > 1:
            # Criar coluna de ordenação: ano primeiro, depois mês
            df_copy['_ordem_ano'] = df_copy['Ano']
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO_FINAL)}
            ).fillna(999)
            df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
            df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
        else:
            # Ordenação simples por mês
            df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
                {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO_FINAL)}
            ).fillna(999)
            df_copy = df_copy.sort_values('_ordem_mes')
            df_copy = df_copy.drop(columns=['_ordem_mes'])
        
        return df_copy
    
    try:
        # Carregar dados do arquivo forecast gerado na pasta Forecast
        caminho_forecast_grafico = os.path.join("dados", "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_forecast_grafico):
            df_forecast_grafico = pd.read_parquet(caminho_forecast_grafico)
            
            # Aplicar filtros (Oficina, Veículo, USI) mas NÃO filtrar por Período
            # As variáveis já estão definidas no escopo global
            if 'Oficina' in df_forecast_grafico.columns:
                if oficina_selecionadas and "Todos" not in oficina_selecionadas:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['Oficina'].astype(str).isin(oficina_selecionadas)
                    ].copy()
            
            if 'Veículo' in df_forecast_grafico.columns:
                if veiculo_selecionados and "Todos" not in veiculo_selecionados:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['Veículo'].astype(str).isin(veiculo_selecionados)
                    ].copy()
            
            if 'USI' in df_forecast_grafico.columns:
                if usi_selecionada and "Todos" not in usi_selecionada:
                    df_forecast_grafico = df_forecast_grafico[
                        df_forecast_grafico['USI'].astype(str).isin(usi_selecionada)
                    ].copy()
            
            # Verificar se há coluna Total
            if 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
                # Verificar se há múltiplos anos
                tem_multiplos_anos = 'Ano' in df_forecast_grafico.columns and df_forecast_grafico['Ano'].nunique() > 1
                
                # Converter Total para numérico caso seja categórico
                if 'Total' in df_forecast_grafico.columns:
                    df_forecast_grafico['Total'] = pd.to_numeric(df_forecast_grafico['Total'], errors='coerce')
                
                if tem_multiplos_anos:
                    # Agrupar por Ano e Período
                    chart_data = df_forecast_grafico.groupby(['Ano', 'Período'])['Total'].sum().reset_index()
                    
                    # Criar coluna combinada para o rótulo do gráfico
                    chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                    
                    # Ordenar por ano e mês (usar função similar ao TC_Ext)
                    chart_data = ordenar_por_mes_forecast_final(chart_data, 'Período')
                    ordem_periodos = chart_data['Período_Completo'].tolist()
                    coluna_periodo_grafico = 'Período_Completo'
                else:
                    # Agrupar apenas por Período
                    chart_data = df_forecast_grafico.groupby('Período')['Total'].sum().reset_index()
                    
                    # Ordenar por mês
                    chart_data = ordenar_por_mes_forecast_final(chart_data, 'Período')
                    ordem_periodos = chart_data['Período'].tolist()
                    coluna_periodo_grafico = 'Período'
                
                # Criar gráfico (mesma lógica do TC_Ext)
                grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                    x=alt.X(
                        f'{coluna_periodo_grafico}:N',
                        title='Período',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Total:Q', title='Soma do Valor (R$)'),
                    color=alt.Color(
                        'Total:Q',
                        title='Total',
                        scale=alt.Scale(scheme='blues')
                    ),
                    tooltip=[
                        alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                        alt.Tooltip('Total:Q', title='Soma do Valor', format=',.2f')
                    ]
                ).properties(
                    title='Soma do Valor por Período',
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
                    text=alt.Text('Total:Q', format=',.2f')
                )
                
                grafico_final = grafico_barras + rotulos
                st.altair_chart(grafico_final, use_container_width=True)
                
                # Mostrar resumo
                total_geral = chart_data['Total'].sum()
                st.info(f"📊 **Total Geral:** R$ {total_geral:,.2f}")
            else:
                st.warning("⚠️ Colunas 'Total' ou 'Período' não encontradas no arquivo forecast.")
        else:
            st.warning(f"⚠️ Arquivo não encontrado: {caminho_forecast_grafico}")
            st.info("ℹ️ O arquivo será gerado quando você clicar em 'Aplicar Configurações do Forecast'.")
    except Exception as e:
        st.error(f"❌ Erro ao criar gráfico 'Soma do Valor por Período': {str(e)}")
        import traceback
        st.error(f"Detalhes: {traceback.format_exc()}")

# ====================================================================
# 📊 GRÁFICO "SOMA DO VALOR POR PERÍODO" - USANDO DADOS DA PASTA FORECAST
# Este gráfico aparece SEMPRE que houver dados na pasta Forecast
# ====================================================================
st.markdown("---")
st.markdown("### 📊 Soma do Valor por Período (Dados do Forecast)")

# Função para ordenar por mês (mesma do TC_Ext)
ORDEM_MESES_GRAFICO = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
                       'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']

def ordenar_por_mes_forecast(df, coluna_periodo='Período'):
    """Ordena DataFrame por ordem cronológica dos meses, considerando ano se disponível"""
    df_copy = df.copy()
    
    # Se houver coluna "Ano" e múltiplos anos, ordenar por ano e mês
    if 'Ano' in df_copy.columns and df_copy['Ano'].nunique() > 1:
        # Criar coluna de ordenação: ano primeiro, depois mês
        df_copy['_ordem_ano'] = df_copy['Ano']
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
        ).fillna(999)
        df_copy = df_copy.sort_values(['_ordem_ano', '_ordem_mes'])
        df_copy = df_copy.drop(columns=['_ordem_ano', '_ordem_mes'])
    else:
        # Ordenação simples por mês
        df_copy['_ordem_mes'] = df_copy[coluna_periodo].astype(str).str.lower().map(
            {mes: idx for idx, mes in enumerate(ORDEM_MESES_GRAFICO)}
        ).fillna(999)
        df_copy = df_copy.sort_values('_ordem_mes')
        df_copy = df_copy.drop(columns=['_ordem_mes'])
    
    return df_copy

try:
    # Carregar dados do arquivo forecast gerado na pasta Forecast
    caminho_forecast_grafico = os.path.join("dados", "Forecast", "forecast_completo.parquet")
    if os.path.exists(caminho_forecast_grafico):
        df_forecast_grafico = pd.read_parquet(caminho_forecast_grafico)
        
        # Aplicar filtros (Oficina, Veículo, USI) mas NÃO filtrar por Período
        # As variáveis já estão definidas no escopo global
        if 'Oficina' in df_forecast_grafico.columns:
            if oficina_selecionadas and "Todos" not in oficina_selecionadas:
                df_forecast_grafico = df_forecast_grafico[
                    df_forecast_grafico['Oficina'].astype(str).isin(oficina_selecionadas)
                ].copy()
        
        if 'Veículo' in df_forecast_grafico.columns:
            if veiculo_selecionados and "Todos" not in veiculo_selecionados:
                df_forecast_grafico = df_forecast_grafico[
                    df_forecast_grafico['Veículo'].astype(str).isin(veiculo_selecionados)
                ].copy()
        
        if 'USI' in df_forecast_grafico.columns:
            if usi_selecionada and "Todos" not in usi_selecionada:
                df_forecast_grafico = df_forecast_grafico[
                    df_forecast_grafico['USI'].astype(str).isin(usi_selecionada)
                ].copy()
        
        # Verificar se há coluna Total
        if 'Total' in df_forecast_grafico.columns and 'Período' in df_forecast_grafico.columns:
            # Converter Total para numérico caso seja categórico
            df_forecast_grafico['Total'] = pd.to_numeric(df_forecast_grafico['Total'], errors='coerce')
            
            # Verificar se há múltiplos anos
            tem_multiplos_anos = 'Ano' in df_forecast_grafico.columns and df_forecast_grafico['Ano'].nunique() > 1
            
            if tem_multiplos_anos:
                # Agrupar por Ano e Período
                chart_data = df_forecast_grafico.groupby(['Ano', 'Período'])['Total'].sum().reset_index()
                
                # Criar coluna combinada para o rótulo do gráfico
                chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                
                # Ordenar por ano e mês (usar função similar ao TC_Ext)
                chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                ordem_periodos = chart_data['Período_Completo'].tolist()
                coluna_periodo_grafico = 'Período_Completo'
            else:
                # Agrupar apenas por Período
                chart_data = df_forecast_grafico.groupby('Período')['Total'].sum().reset_index()
                
                # Ordenar por mês
                chart_data = ordenar_por_mes_forecast(chart_data, 'Período')
                ordem_periodos = chart_data['Período'].tolist()
                coluna_periodo_grafico = 'Período'
            
            # Criar gráfico (mesma lógica do TC_Ext)
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    f'{coluna_periodo_grafico}:N',
                    title='Período',
                    sort=ordem_periodos
                ),
                y=alt.Y('Total:Q', title='Soma do Valor (R$)'),
                color=alt.Color(
                    'Total:Q',
                    title='Total',
                    scale=alt.Scale(scheme='blues')
                ),
                tooltip=[
                    alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                    alt.Tooltip('Total:Q', title='Soma do Valor', format=',.2f')
                ]
            ).properties(
                title='Soma do Valor por Período',
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
                text=alt.Text('Total:Q', format=',.2f')
            )
            
            grafico_final = grafico_barras + rotulos
            st.altair_chart(grafico_final, use_container_width=True)
            
            # Mostrar resumo
            total_geral = chart_data['Total'].sum()
            st.info(f"📊 **Total Geral:** R$ {total_geral:,.2f}")
        else:
            st.warning("⚠️ Colunas 'Total' ou 'Período' não encontradas no arquivo forecast.")
    else:
        st.warning(f"⚠️ Arquivo não encontrado: {caminho_forecast_grafico}")
        st.info("ℹ️ O arquivo será gerado quando você clicar em 'Aplicar Configurações do Forecast'.")
except Exception as e:
    st.error(f"❌ Erro ao criar gráfico 'Soma do Valor por Período': {str(e)}")
    import traceback
    st.error(f"Detalhes: {traceback.format_exc()}")

# Footer
st.markdown("---")
st.info("💡 Forecast TC - Análise preditiva e previsões")

