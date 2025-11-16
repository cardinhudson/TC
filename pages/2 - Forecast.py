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
            
            # Botão para aplicar
            st.markdown("---")
            col_aplicar1, col_aplicar2, col_aplicar3 = st.columns([1, 2, 1])
            with col_aplicar2:
                aplicar_global = st.button(
                    "✅ Aplicar Configurações (Sensibilidade + Inflação)",
                    use_container_width=True,
                    type="primary",
                    key="aplicar_global"
                )
            
            if aplicar_global:
                st.session_state.sensibilidade_fixo_aplicada = sensibilidade_fixo_temp
                st.session_state.sensibilidade_variavel_aplicada = sensibilidade_variavel_temp
                st.session_state.inflacao_global_aplicada = inflacao_global_temp
                st.success("✅ Configurações aplicadas com sucesso!")
                st.rerun()
            
            # Usar valores aplicados
            sensibilidade_fixo = st.session_state.sensibilidade_fixo_aplicada
            sensibilidade_variavel = st.session_state.sensibilidade_variavel_aplicada
            inflacao_global = st.session_state.inflacao_global_aplicada
            
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
            
            # Botão de aplicar
            col_aplicar1, col_aplicar2, col_aplicar3 = st.columns([1, 2, 1])
            with col_aplicar2:
                aplicar_config = st.button(
                    "✅ Aplicar Configurações (Sensibilidade + Inflação)",
                    use_container_width=True,
                    type="primary",
                    help="Clique para aplicar as configurações de sensibilidade e inflação ao forecast"
                )
            
            # Se clicar em aplicar, salvar as configurações
            if aplicar_config:
                st.session_state.sensibilidades_aplicadas = sensibilidades_type06.copy()
                st.session_state.inflacao_aplicada = inflacao_type06.copy()
                st.success("✅ Configurações aplicadas com sucesso! Recalculando forecast...")
                st.rerun()
            
            # Usar configurações aplicadas ou None
            if st.session_state.sensibilidades_aplicadas is not None:
                sensibilidades_type06 = st.session_state.sensibilidades_aplicadas
                inflacao_type06 = st.session_state.inflacao_aplicada
                st.info(f"ℹ️ Usando configurações aplicadas. Ajuste os valores e clique em 'Aplicar' para atualizar.")
            else:
                st.warning("⚠️ Configure os valores acima e clique em 'Aplicar Configurações' para calcular o forecast.")
                # Não calcular forecast até aplicar
                sensibilidades_type06 = None
                inflacao_type06 = None
            
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

# Carregar dados de volume
df_vol = load_volume_data(ano_selecionado)

if df_vol is None:
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
    
    # Lista de meses do ano
    meses_ano = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    
    # ====================================================================
    # 🔮 CONFIGURAÇÃO DO FORECAST
    # ====================================================================
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔮 Configuração do Forecast**")
    
    # 1. Selecionar último mês com dados reais
    from datetime import datetime
    mes_atual_sistema = datetime.now().month
    indice_mes_atual_padrao = mes_atual_sistema - 1 if mes_atual_sistema <= 12 else 11
    
    ultimo_mes_dados = st.sidebar.selectbox(
        "📅 Último mês com dados reais:",
        options=meses_ano,
        index=indice_mes_atual_padrao,
        help="Selecione o último mês que possui dados históricos reais"
    )
    
    indice_ultimo_mes = meses_ano.index(ultimo_mes_dados)
    
    # 2. Quantos meses prever
    meses_disponiveis_para_prever = len(meses_ano) - (indice_ultimo_mes + 1)
    if meses_disponiveis_para_prever <= 0:
        meses_disponiveis_para_prever = 12  # Se já passou dezembro, permitir prever o próximo ano
    
    num_meses_prever = st.sidebar.number_input(
        "🔮 Quantos meses prever:",
        min_value=1,
        max_value=12,
        value=min(meses_disponiveis_para_prever, 6),
        step=1,
        help="Número de meses futuros para prever"
    )
    
    # Calcular quais meses serão previstos
    meses_restantes = []
    for i in range(num_meses_prever):
        indice_mes = indice_ultimo_mes + 1 + i
        # Se passar de dezembro, continuar no próximo ano
        if indice_mes >= 12:
            indice_mes = indice_mes % 12
        meses_restantes.append(meses_ano[indice_mes])
    
    if meses_restantes:
        st.sidebar.info(f"📊 Meses a prever: {', '.join(meses_restantes)}")
    else:
        st.sidebar.warning("⚠️ Nenhum mês selecionado para prever")
        meses_restantes = []
    
    # 3. Quantos meses usar para calcular a média
    meses_historicos_disponiveis = meses_ano[:indice_ultimo_mes + 1]
    
    num_meses_media = st.sidebar.number_input(
        "📈 Quantos meses usar para a média:",
        min_value=1,
        max_value=len(meses_historicos_disponiveis) if meses_historicos_disponiveis else 12,
        value=min(len(meses_historicos_disponiveis), 6) if meses_historicos_disponiveis else 6,
        step=1,
        help="Número de meses históricos para calcular a média"
    )
    
    # 4. Selecionar quais meses excluir do cálculo da média
    if meses_historicos_disponiveis:
        meses_excluir_media = st.sidebar.multiselect(
            "🚫 Excluir meses do cálculo da média:",
            options=meses_historicos_disponiveis,
            default=[],
            help="Selecione meses que foram fora da curva e devem ser excluídos do cálculo da média"
        )
        
        # Calcular quais meses serão usados para a média (últimos N meses, excluindo os selecionados)
        meses_para_media = []
        meses_considerados = meses_historicos_disponiveis.copy()
        
        # Remover meses excluídos
        for mes_excluir in meses_excluir_media:
            if mes_excluir in meses_considerados:
                meses_considerados.remove(mes_excluir)
        
        # Pegar os últimos N meses (após excluir)
        if meses_considerados:
            meses_para_media = meses_considerados[-num_meses_media:] if len(meses_considerados) >= num_meses_media else meses_considerados
            st.sidebar.success(f"✅ Usando {len(meses_para_media)} meses para média: {', '.join(meses_para_media)}")
        else:
            st.sidebar.error("❌ Nenhum mês disponível para calcular a média!")
            meses_para_media = []
    else:
        meses_excluir_media = []
        meses_para_media = meses_historicos_disponiveis if meses_historicos_disponiveis else []
        st.sidebar.warning("⚠️ Nenhum mês histórico disponível")
    
    st.sidebar.markdown("---")
    
    # Validação: verificar se há meses para calcular a média
    if not meses_para_media:
        st.error("❌ **Erro de Configuração:** Nenhum mês disponível para calcular a média histórica.")
        st.info("💡 Ajuste a configuração do forecast na sidebar:")
        st.info("   - Selecione um mês histórico válido")
        st.info("   - Ajuste os meses a excluir")
        st.info("   - Verifique se há dados históricos disponíveis")
        st.stop()
    
    # Validação: verificar se há meses para prever
    if not meses_restantes:
        st.error("❌ **Erro de Configuração:** Nenhum mês selecionado para prever.")
        st.info("💡 Ajuste a configuração do forecast na sidebar:")
        st.info("   - Selecione o último mês com dados reais")
        st.info("   - Defina quantos meses prever")
        st.stop()
    
    # Função para calcular médias com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_medias_forecast(df_filtrado_cache, colunas_adicionais_cache, meses_para_media_cache):
        """Calcula médias mensais históricas com cache, usando apenas os meses selecionados"""
        # Filtrar apenas os meses que serão usados para calcular a média
        if meses_para_media_cache and 'Período' in df_filtrado_cache.columns:
            # Normalizar nomes dos meses para comparação (case-insensitive)
            df_filtrado_media = df_filtrado_cache[
                df_filtrado_cache['Período'].astype(str).str.strip().str.title().isin(
                    [m.strip().title() for m in meses_para_media_cache]
                )
            ].copy()
        else:
            # Se não houver meses selecionados, usar todos os dados (comportamento original)
            df_filtrado_media = df_filtrado_cache.copy()
        
        if df_filtrado_media.empty:
            # Retornar DataFrames vazios se não houver dados
            colunas_base = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais_cache
            df_medias = pd.DataFrame(columns=colunas_base + ['Total'])
            colunas_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
            df_media_mensal = pd.DataFrame(columns=colunas_media + ['Total'])
            return df_medias, df_media_mensal
        
        # Agrupar por Oficina, Veículo, Período e Tipo_Custo para calcular médias
        colunas_groupby = ['Oficina', 'Veículo', 'Período', 'Tipo_Custo'] + colunas_adicionais_cache
        colunas_groupby = [col for col in colunas_groupby if col in df_filtrado_media.columns]
        agg_dict = {'Total': 'mean'}
        df_medias = df_filtrado_media.groupby(colunas_groupby).agg(agg_dict).reset_index()
        
        # Calcular média geral mensal (média das médias dos meses selecionados)
        colunas_groupby_media = ['Oficina', 'Veículo', 'Tipo_Custo'] + colunas_adicionais_cache
        colunas_groupby_media = [col for col in colunas_groupby_media if col in df_medias.columns]
        agg_dict_media = {'Total': 'mean'}
        df_media_mensal = df_medias.groupby(colunas_groupby_media).agg(agg_dict_media).reset_index()
        
        return df_medias, df_media_mensal

    # Calcular médias mensais históricas por Oficina, Veículo e Período
    st.markdown("### 📊 Cálculo de Médias Mensais Históricas")
    
    # Mostrar configuração do forecast
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"""
        **📈 Meses usados para média:** {len(meses_para_media)} meses
        - {', '.join(meses_para_media)}
        """)
    with col2:
        st.info(f"""
        **🔮 Meses a prever:** {len(meses_restantes)} meses
        - {', '.join(meses_restantes)}
        """)
    
    if meses_excluir_media:
        st.warning(f"⚠️ **Meses excluídos da média:** {', '.join(meses_excluir_media)}")
    
    st.markdown("---")

    # Verificar se as colunas Type 05, Type 06 e Account existem
    colunas_adicionais = []
    if 'Type 05' in df_filtrado.columns:
        colunas_adicionais.append('Type 05')
    if 'Type 06' in df_filtrado.columns:
        colunas_adicionais.append('Type 06')
    if 'Account' in df_filtrado.columns:
        colunas_adicionais.append('Account')

    # Calcular médias com cache (usando apenas os meses selecionados)
    df_medias, df_media_mensal = calcular_medias_forecast(df_filtrado, colunas_adicionais, meses_para_media)
    
    # Função para calcular volumes e CPU com cache
    @st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
    def calcular_volumes_cpu(df_vol_cache, df_medias_cache, colunas_adicionais_cache, meses_para_media_cache):
        """Calcula volumes e CPU histórico com cache, usando apenas os meses selecionados"""
        if df_vol_cache.empty or 'Período' not in df_vol_cache.columns or 'Volume' not in df_vol_cache.columns:
            return None, None, None, None
        
        # Filtrar apenas os meses que serão usados para calcular a média de volume
        if meses_para_media_cache and 'Período' in df_vol_cache.columns:
            df_vol_para_media = df_vol_cache[
                df_vol_cache['Período'].astype(str).str.strip().str.title().isin(
                    [m.strip().title() for m in meses_para_media_cache]
                )
            ].copy()
        else:
            # Se não houver meses selecionados, usar todos os dados (comportamento original)
            df_vol_para_media = df_vol_cache.copy()
        
        # Calcular média de volume por período histórico (apenas meses selecionados)
        if not df_vol_para_media.empty:
            df_vol_medio = df_vol_para_media.groupby(['Oficina', 'Veículo', 'Período'], as_index=False)['Volume'].mean()
            
            # Calcular volume médio mensal (média dos meses selecionados)
            df_vol_medio_mensal = df_vol_medio.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
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
    volume_base, volume_por_mes, df_cpu_medio, df_vol_medio = calcular_volumes_cpu(df_vol, df_medias, colunas_adicionais, meses_para_media)
    
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
        
        # Fazer merge com volume_base
        df_forecast_base = df_media_mensal_cache.merge(
            volume_base_cache,
            on=['Oficina', 'Veículo'],
            how='left'
        )
        df_forecast_base['Volume_Medio_Historico'] = df_forecast_base['Volume_Medio_Historico'].fillna(1.0)
        
        # Fazer merge com df_cpu_medio para custos variáveis
        if df_cpu_medio_cache is not None and not df_cpu_medio_cache.empty:
            colunas_merge_cpu = ['Oficina', 'Veículo'] + colunas_adicionais_cache
            df_forecast_base = df_forecast_base.merge(
                df_cpu_medio_cache,
                on=colunas_merge_cpu,
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
        
        # Calcular forecast para cada mês
        for idx_mes, mes in enumerate(meses_restantes_cache):
            # Buscar volume específico deste mês
            if volume_por_mes_cache is not None and not volume_por_mes_cache.empty:
                vol_mes_df = volume_por_mes_cache[
                    volume_por_mes_cache['Período'].astype(str).str.strip().str.lower() == str(mes).strip().lower()
                ][['Oficina', 'Veículo', 'Volume']]
                
                if not vol_mes_df.empty:
                    vol_mes_df = vol_mes_df.groupby(['Oficina', 'Veículo'], as_index=False)['Volume'].mean()
                    df_vol_mes_merge = df_forecast_base[['Oficina', 'Veículo']].merge(
                        vol_mes_df,
                        on=['Oficina', 'Veículo'],
                        how='left',
                        suffixes=('', '_mes')
                    )
                    volume_mes_serie = df_vol_mes_merge['Volume'].fillna(df_forecast_base['Volume_Medio_Ref'])
                else:
                    volume_mes_serie = df_forecast_base['Volume_Medio_Ref']
            else:
                volume_mes_serie = df_forecast_base['Volume_Medio_Ref']
            
            # Alinhar volume
            if isinstance(volume_mes_serie, pd.Series):
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Ref'])
            else:
                volume_mes_aligned = volume_mes_serie.reindex(df_forecast_base.index).fillna(df_forecast_base['Volume_Medio_Ref'])
            
            # Calcular forecast com sensibilidade diferenciada
            df_forecast[mes] = 0.0
            
            # Calcular proporção de volume para TODOS os registros
            volume_medio_positivo = df_forecast_base['Volume_Medio_Historico'] > 0
            proporcao_volume = volume_mes_aligned / df_forecast_base['Volume_Medio_Historico']
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
                    
                    df_forecast.loc[idx, mes] = valor_base * fator_inflacao
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
                    df_forecast.loc[mask_fixo, mes] = (
                        df_forecast_base.loc[mask_fixo, 'Média_Mensal_Histórica'] * proporcao_ajustada_fixo * fator_inflacao_global
                    )
                
                # Aplicar sensibilidade para Custo Variável
                mask_variavel = df_forecast_base['Tipo_Custo'] == 'Variável'
                if mask_variavel.any():
                    variacao_ajustada_variavel = variacao_percentual.loc[mask_variavel] * sensibilidade_variavel_cache
                    proporcao_ajustada_variavel = 1.0 + variacao_ajustada_variavel
                    df_forecast.loc[mask_variavel, mes] = (
                        df_forecast_base.loc[mask_variavel, 'Média_Mensal_Histórica'] * proporcao_ajustada_variavel * fator_inflacao_global
                    )
        
        return df_forecast

    # Criar tabela de forecast
    st.markdown("### 🔮 Tabela de Forecast - Custo Total até Fim do Ano")
    
    # Preparar dados para forecast usando operações vetorizadas (mais rápido)
    st.info("🔄 Calculando forecast... Isso pode levar alguns segundos.")
    
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
        meses_restantes,
        sensibilidade_fixo,
        sensibilidade_variavel,
        sens_type06_cache,
        inflacao_type06_cache
    )
    
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
        
        # Agrupar linhas iguais
        colunas_agrupamento = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo', 'Média_Mensal_Histórica']
        colunas_agrupamento_existentes = [col for col in colunas_agrupamento if col in df_forecast_processado.columns]
        
        agg_dict_grupo = {}
        for col in colunas_meses + ['Total_Forecast']:
            if col in df_forecast_processado.columns:
                agg_dict_grupo[col] = 'sum'
        
        if agg_dict_grupo and len(colunas_agrupamento_existentes) > 0:
            df_forecast_processado = df_forecast_processado.groupby(colunas_agrupamento_existentes).agg(agg_dict_grupo).reset_index()
        
        # Remover linhas com valores zero
        if colunas_meses:
            soma_meses = df_forecast_processado[colunas_meses].sum(axis=1)
            df_forecast_processado = df_forecast_processado[soma_meses > 0.01].copy()
        
        # Ordenar
        colunas_ordenacao = ['Oficina', 'Veículo'] + [col for col in colunas_adicionais_cache if col in df_forecast_processado.columns] + ['Tipo_Custo']
        df_forecast_processado = df_forecast_processado.sort_values(colunas_ordenacao)
        
        return df_forecast_processado, colunas_meses

    # Processar tabela com cache
    df_forecast, colunas_meses = processar_tabela_forecast(df_forecast, colunas_adicionais, meses_restantes)
    
    # Criar gráfico de resumo: Premissas da Previsão (Volumes em barras e Custos em linhas)
    st.markdown("### 📊 Gráfico - Premissas da Previsão")
    
    # Preparar dados para o gráfico mostrando todas as premissas
    if colunas_meses:
        # Calcular totais agregados
        media_historica_total = df_forecast['Média_Mensal_Histórica'].sum()
        
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
            
            # Buscar volume futuro deste mês
            volume_futuro_mes = 0
            if not volume_por_mes.empty:
                vol_mes_df = volume_por_mes[
                    volume_por_mes['Período'].astype(str).str.strip().str.lower() == str(mes).strip().lower()
                ]
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
                width=600,
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
                width=600,
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
            
            # Mostrar gráficos lado a lado
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                st.altair_chart(barras_custo + texto_barras, use_container_width=True)
            with col_g2:
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
                    total_media = df_oficina_numerico_display['Média_Mensal_Histórica'].sum()
                    linha_total['Média_Mensal_Histórica'] = formatar_monetario(total_media)
                
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

