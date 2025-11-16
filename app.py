import streamlit as st
import pandas as pd
import altair as alt
import os

# Configuração da página
st.set_page_config(
    page_title="Dashboard TC - KE5Z Group",
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
st.title("📊 Dashboard - Visualização de Dados TC - KE5Z Group")
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

# Filtro 2: USI (com cache otimizado)
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

    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes
    )
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Filtro 4: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes
    )
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Filtro 5: Conta contábil (com cache otimizado)
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
        # Agrupar df_filtrado por Oficina e Período para calcular Valor total
        if ('Oficina' in df_filtrado.columns and
                'Período' in df_filtrado.columns):
            # Agrupar Valor por Oficina e Período
            df_valor_agrupado = df_filtrado.groupby(
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
    # Usar Valor diretamente
    df_visualizacao = df_filtrado.copy()
    coluna_visualizacao = 'Valor'

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
@st.cache_data(ttl=900, max_entries=2)
def create_period_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras por Período"""
    try:
        if coluna not in df_data.columns:
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

        # Definir título do eixo Y baseado no tipo
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = "CPU (R$/Unidade)"
            titulo_grafico = "CPU por Período"
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
            color='black',
            fontSize=12
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        )

        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico por Período
if coluna_visualizacao in df_visualizacao.columns:
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
        
        # Criar gráfico com dados filtrados
        grafico_periodo = create_period_chart(
            df_grafico_periodo, coluna_visualizacao, tipo_visualizacao
        )
    else:
        st.subheader("📊 Soma do Valor por Período")
        grafico_periodo = create_period_chart(
            df_visualizacao, coluna_visualizacao, tipo_visualizacao
        )
    
    if grafico_periodo:
        st.altair_chart(grafico_periodo, use_container_width=True)


# Gráfico 2: Soma do Valor por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_oficina_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras por Oficina"""
    try:
        if (coluna not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

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


# Gráfico 3: Volume por Período (se coluna Volume existir)
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data):
    """Cria gráfico de barras de Volume por Período"""
    try:
        if 'Volume' not in df_data.columns or 'Período' not in df_data.columns:
            return None

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
@st.cache_data(ttl=900, max_entries=2)
def create_total_chart(df_data):
    """Cria gráfico de barras de Total por Período"""
    try:
        if 'Total' not in df_data.columns:
            return None

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
                sort=ordem_periodos
            ),
            y=alt.Y('Total:Q', title='Total (R$)'),
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
            color='black',
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
    grafico_total = create_total_chart(df_filtrado)
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
