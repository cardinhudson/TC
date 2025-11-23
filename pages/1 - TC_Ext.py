import streamlit as st
import pandas as pd
import altair as alt
import os
import numpy as np

# Configuração da página
st.set_page_config(
    page_title="Dashboard TC Ext - df_final",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS para reduzir títulos em 20%
st.markdown("""
    <style>
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
st.title("📊 Dashboard - Visualização de Dados TC Ext - df_final")
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

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=0,  # "Todos" por padrão
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico"
)

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
            
            # Debug: mostrar informações detalhadas sobre os dados carregados
            st.sidebar.info(f"📁 Arquivo carregado: {caminho_absoluto}")
            
            if "Ano" in df.columns:
                anos_carregados = sorted(df['Ano'].unique())
                st.sidebar.info(f"📊 Anos disponíveis: {anos_carregados} | Total de registros: {len(df):,}")
                
                # Verificar se há coluna Total e se tem valores
                if 'Total' in df.columns:
                    total_sum = df['Total'].sum() if pd.api.types.is_numeric_dtype(df['Total']) else 0
                    st.sidebar.info(f"💰 Soma Total: R$ {total_sum:,.2f}")
            else:
                st.sidebar.warning("⚠️ Coluna 'Ano' não encontrada nos dados")
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
    
    st.sidebar.success("✅ Dados carregados com sucesso")
    
    # Debug adicional: verificar colunas e valores
    if ano_selecionado == "Todos":
        st.sidebar.info(f"📊 {len(df_total):,} registros (Todos os anos)")
        
        # Verificar se há coluna Total e mostrar soma
        if 'Total' in df_total.columns:
            # Converter para numérico se necessário
            if not pd.api.types.is_numeric_dtype(df_total['Total']):
                df_total['Total'] = pd.to_numeric(df_total['Total'], errors='coerce')
            
            total_sum = df_total['Total'].sum()
            st.sidebar.info(f"💰 Soma Total (df_total): R$ {total_sum:,.2f}")
            
            # Verificar anos disponíveis
            if 'Ano' in df_total.columns:
                anos_disponiveis = sorted(df_total['Ano'].unique())
                st.sidebar.info(f"📅 Anos em df_total: {anos_disponiveis}")
    else:
        st.sidebar.info(f"📊 {len(df_total):,} registros (Ano {ano_selecionado})")
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


# Seletor de tipo de visualização
st.sidebar.markdown("**📊 Tipo de Visualização**")
tipo_visualizacao = st.sidebar.radio(
    "Selecione o tipo:",
    ["Custo Total", "CPU (Custo por Unidade)"],
    index=0
)
st.sidebar.markdown("---")

# Filtro 1: Oficina (com cache otimizado)
if 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=["Todos"]
    )

    # Filtrar o DataFrame com base na Oficina
    if "Todos" in oficina_selecionadas or not oficina_selecionadas:
        df_filtrado = df_total.copy()
    else:
        df_filtrado = df_total[
            df_total['Oficina'].astype(str).isin(oficina_selecionadas)
        ].copy()
else:
    df_filtrado = df_total.copy()

# Filtro 2: Veículo (com cache otimizado)
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=["Todos"]
    )
    if veiculo_selecionados and "Todos" not in veiculo_selecionados:
        df_filtrado = df_filtrado[
            df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
        ].copy()

# Filtro 3: USI (com cache otimizado)
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    default_usi = ["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"]
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi
    )

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

    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes
    )
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Filtro 5: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes
    )
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Filtro 6: Conta contábil (com cache otimizado)
if 'Nºconta' in df_filtrado.columns:
    conta_contabil_opcoes = get_filter_options(df_filtrado, 'Nºconta')[1:]
    conta_contabil_selecionadas = st.sidebar.multiselect(
        "Selecione a Conta contábil:", conta_contabil_opcoes
    )
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
        opcoes = get_filter_options(df_filtrado, col_name)
        if widget_type == "multiselect":
            selecionadas = st.sidebar.multiselect(
                f"Selecione o {label}:", opcoes, default=["Todos"]
            )
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
                selecionadas = st.multiselect(
                    f"Selecione o {label}:", opcoes, default=["Todos"]
                )
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
if 'Volume' in df_filtrado.columns:
    volume_total = df_filtrado['Volume'].sum()
    st.sidebar.write(f"**Total Volume:** {volume_total:,.2f}")
if 'CPU' in df_filtrado.columns:
    df_cpu_positivo = df_filtrado[df_filtrado['CPU'] > 0]
    cpu_medio = (
        df_cpu_positivo['CPU'].mean()
        if len(df_cpu_positivo) > 0 else 0
    )
    st.sidebar.write(f"**CPU Médio:** R$ {cpu_medio:,.4f}")

# Mostrar tipo de visualização selecionado
st.sidebar.info(f"📈 **Visualizando:** {tipo_visualizacao}")


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


# Gráfico 1: Soma do Valor por Período
@st.cache_data(ttl=900, max_entries=2)
def create_period_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras por Período"""
    try:
        if coluna not in df_data.columns or 'Período' not in df_data.columns:
            st.warning(f"⚠️ Colunas necessárias não encontradas. Coluna: {coluna}, Período: {'Período' in df_data.columns}")
            return None

        # Debug: verificar dados recebidos
        st.sidebar.write(f"🔍 Debug create_period_chart:")
        st.sidebar.write(f"   - Total de registros recebidos: {len(df_data):,}")
        st.sidebar.write(f"   - Coluna a ser usada: {coluna}")
        st.sidebar.write(f"   - Tipo de visualização: {tipo_viz}")
        
        if coluna in df_data.columns:
            # Verificar se a coluna tem valores
            if pd.api.types.is_numeric_dtype(df_data[coluna]):
                soma_coluna = df_data[coluna].sum()
                st.sidebar.write(f"   - Soma da coluna {coluna}: {soma_coluna:,.2f}")
            else:
                st.sidebar.write(f"   - Coluna {coluna} não é numérica")

        # Verificar se há coluna Ano - sempre mostrar ano junto com período quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e Período (sempre que houver coluna Ano)
            # IMPORTANTE: Sempre agrupar por Ano e Período para garantir consistência
            # independentemente de "Todos" estar selecionado ou um ano específico
            if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                # MESMA LÓGICA DA TABELA: Agrupar por Ano e Período, somar Total e Volume, calcular CPU
                chart_data = df_data.groupby(['Ano', 'Período']).agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Recalcular CPU (mesma lógica da tabela)
                chart_data[coluna] = chart_data.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
            else:
                # Para Custo Total, também agrupar por Ano e Período para garantir consistência
                # Isso garante que o mesmo ano e mês sempre tenha o mesmo valor
                chart_data = df_data.groupby(['Ano', 'Período'])[coluna].sum().reset_index()
            
            # Debug: verificar dados agrupados
            st.sidebar.write(f"   - Registros após agrupamento: {len(chart_data):,}")
            if coluna in chart_data.columns and pd.api.types.is_numeric_dtype(chart_data[coluna]):
                soma_agrupada = chart_data[coluna].sum()
                st.sidebar.write(f"   - Soma após agrupamento: {soma_agrupada:,.2f}")
            
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
            if tipo_viz == "CPU (Custo por Unidade)" and 'Total' in df_data.columns and 'Volume' in df_data.columns:
                # MESMA LÓGICA DA TABELA: Agrupar por Período, somar Total e Volume, calcular CPU
                chart_data = df_data.groupby('Período').agg({
                    'Total': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Recalcular CPU (mesma lógica da tabela)
                chart_data[coluna] = chart_data.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
            else:
                chart_data = df_data.groupby('Período')[coluna].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        # Definir título do eixo Y baseado no tipo
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = "CPU (R$/Unidade)"
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
            titulo_y = "Soma do Valor (R$)"
            titulo_grafico = "Soma do Valor por Período"

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos
            ),
            y=alt.Y(f'{coluna}:Q', title=titulo_y),
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
            color='black',
            fontSize=12
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Gráfico 2: Volume por Período
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


# Exibir gráfico por Período
if (coluna_visualizacao in df_visualizacao.columns and
        'Período' in df_visualizacao.columns):
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📊 CPU por Período")
    else:
        st.subheader("📊 Soma do Valor por Período")
    
    # IMPORTANTE: Criar df_visualizacao_para_grafico usando df_para_grafico_periodo
    # (dados ANTES do filtro de período) para mostrar TODOS os períodos no gráfico
    # Aplicar a mesma lógica de preparação de dados, mas usando df_para_grafico_periodo
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        df_vol_calc_grafico = load_volume_data(ano_selecionado)
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
                
                df_cpu_grafico['CPU'] = df_cpu_grafico.apply(
                    lambda row: (
                        row['Total'] / row['Volume']
                        if pd.notnull(row['Volume']) and row['Volume'] != 0
                        else 0
                    ),
                    axis=1
                )
                
                df_visualizacao_para_grafico = df_cpu_grafico.copy()
                coluna_visualizacao_grafico = 'CPU'
            else:
                df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
                coluna_visualizacao_grafico = 'Total' if 'Total' in df_para_grafico_periodo.columns else 'Valor'
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
    
    # Criar gráfico com dados filtrados (usar coluna_visualizacao_grafico que foi criada acima)
    # O create_period_chart já faz o agrupamento correto por Ano e Período quando há coluna Ano
    grafico_periodo = create_period_chart(
        df_grafico_periodo, coluna_visualizacao_grafico, tipo_visualizacao
    )
    if grafico_periodo:
        st.altair_chart(grafico_periodo, use_container_width=True)
    
    # Exibir gráfico de Volume logo abaixo, usando os mesmos filtros
    st.subheader("📊 Volume Total por Período")
    
    # IMPORTANTE: Usar a mesma lógica de filtragem em ambos os modos
    # para garantir que os volumes sejam consistentes
    df_vol = load_volume_data(ano_selecionado)
    
    if df_vol is not None:
        # Verificar se tem as colunas necessárias
        if 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
            # Aplicar TODOS os filtros da sidebar ao df_vol (mesma lógica para ambos os modos)
            # Identificar colunas comuns entre df_filtrado e df_vol
            colunas_comuns = set(df_filtrado.columns) & set(df_vol.columns)
            # Remover colunas que não devem ser usadas para filtro
            # Excluir Período para não filtrar por mês (mostrar todos os períodos)
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
                
                # Recalcular CPU
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
                            return f"{val:,.4f}"
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


# Gráfico 2: Soma do Valor por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_oficina_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras por Oficina"""
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

            titulo_y = "CPU (R$/Unidade)"
            titulo_grafico = "CPU por Oficina e Veículo"

            # Criar gráfico de barras agrupadas
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y'),
                y=alt.Y(f'{coluna}:Q', title=titulo_y),
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
                        format=',.4f'
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
                fontSize=10
            ).encode(
                text=alt.Text(f'{coluna}:Q', format=',.4f')
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

            # Definir título do eixo Y baseado no tipo
            if tipo_viz == "CPU (Custo por Unidade)":
                titulo_y = "CPU (R$/Unidade)"
                titulo_grafico = "CPU por Oficina"
            else:
                titulo_y = "Soma do Valor (R$)"
                titulo_grafico = "Soma do Valor por Oficina"

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y'),
                y=alt.Y(f'{coluna}:Q', title=titulo_y),
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
                color='black',
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
        df_visualizacao, coluna_visualizacao, tipo_visualizacao
    )
    if grafico_oficina:
        st.altair_chart(grafico_oficina, use_container_width=True)


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


# Gráfico 4: Total/CPU por Veículo
@st.cache_data(ttl=900, max_entries=2)
def create_total_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras de Total/CPU por Veículo"""
    try:
        if coluna not in df_data.columns:
            return None

        # Definir título e formato baseado no tipo
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = "CPU (R$/Unidade)"
            formato = ',.4f'
            if 'Veículo' in df_data.columns:
                titulo_grafico = "CPU por Veículo"
            else:
                titulo_grafico = "CPU por Período"
        else:
            titulo_y = "Total (R$)"
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

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    'Veículo:N',
                    title='Veículo',
                    sort='-y'
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y),
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
                    sort=ordem_periodos
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y),
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
            fontSize=12
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato)
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico de Total/CPU por Veículo
if 'Veículo' in df_visualizacao.columns:
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        if coluna_visualizacao in df_visualizacao.columns:
            st.subheader("📊 CPU por Veículo")
            grafico_total = create_total_chart(
                df_visualizacao, coluna_visualizacao, tipo_visualizacao
            )
            if grafico_total:
                st.altair_chart(grafico_total, use_container_width=True)
            
            # Gráfico de Volume por Veículo (logo abaixo do gráfico de CPU)
            if 'Volume' in df_visualizacao.columns and 'Veículo' in df_visualizacao.columns:
                st.subheader("📊 Volume por Veículo")
                grafico_volume = create_volume_veiculo_chart(df_visualizacao)
                if grafico_volume is not None:
                    st.altair_chart(grafico_volume, use_container_width=True)
                else:
                    # Debug: mostrar informações sobre os dados
                    with st.expander("🔍 Debug - Informações sobre Volume", expanded=False):
                        st.write(f"**Colunas disponíveis:** {list(df_visualizacao.columns)}")
                        st.write(f"**Total de linhas:** {len(df_visualizacao)}")
                        if 'Volume' in df_visualizacao.columns:
                            st.write(f"**Volume total:** {df_visualizacao['Volume'].sum()}")
                            st.write(f"**Volume não nulo:** {df_visualizacao['Volume'].notna().sum()} linhas")
                            st.write(f"**Volume nulo:** {df_visualizacao['Volume'].isna().sum()} linhas")
                        if 'Veículo' in df_visualizacao.columns:
                            st.write(f"**Veículos únicos:** {df_visualizacao['Veículo'].nunique()}")
                            st.write(f"**Veículos:** {df_visualizacao['Veículo'].unique().tolist()}")
                        # Tentar criar gráfico manualmente para debug
                        try:
                            df_test = df_visualizacao[['Veículo', 'Volume']].dropna()
                            if len(df_test) > 0:
                                df_grouped = df_test.groupby('Veículo')['Volume'].sum().reset_index()
                                st.write("**Dados agrupados:**")
                                st.dataframe(df_grouped)
                        except Exception as e:
                            st.write(f"Erro ao agrupar: {e}")
    elif tipo_visualizacao == "Custo Total":
        if 'Total' in df_filtrado.columns:
            st.subheader("📊 Total por Veículo")
            grafico_total = create_total_chart(
                df_filtrado, 'Total', tipo_visualizacao
            )
            if grafico_total:
                st.altair_chart(grafico_total, use_container_width=True)
        
        # Gráfico de Volume por Veículo (logo abaixo do gráfico de Total)
        # COPIAR EXATAMENTE DO MODO CPU - usar df_visualizacao diretamente
        # No modo CPU funciona porque df_visualizacao já tem Volume e está agrupado corretamente
        if 'Volume' in df_visualizacao.columns and 'Veículo' in df_visualizacao.columns:
            st.subheader("📊 Volume por Veículo")
            grafico_volume = create_volume_veiculo_chart(df_visualizacao)
            if grafico_volume is not None:
                st.altair_chart(grafico_volume, use_container_width=True)
elif 'Período' in df_visualizacao.columns:
    # Fallback para Período se não tiver Veículo
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        if coluna_visualizacao in df_visualizacao.columns:
            st.subheader("📊 CPU por Período")
            grafico_total = create_total_chart(
                df_visualizacao, coluna_visualizacao, tipo_visualizacao
            )
            if grafico_total:
                st.altair_chart(grafico_total, use_container_width=True)
    elif tipo_visualizacao == "Custo Total":
        if 'Total' in df_filtrado.columns:
            st.subheader("📊 Total por Período")
            grafico_total = create_total_chart(
                df_filtrado, 'Total', tipo_visualizacao
            )
            if grafico_total:
                st.altair_chart(grafico_total, use_container_width=True)

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
