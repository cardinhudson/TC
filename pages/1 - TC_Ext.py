import streamlit as st
import pandas as pd
import altair as alt
import os

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
                # Verificar se df_filtrado tem Veículo
                tem_veiculo = 'Veículo' in df_filtrado.columns

                # Agrupar Volume por Oficina e Período (e Veículo)
                if tem_veiculo and 'Veículo' in df_vol_calc.columns:
                    # Agrupar Total incluindo Veículo
                    if 'Total' in df_filtrado.columns:
                        df_total_agrupado = df_filtrado.groupby(
                            ['Oficina', 'Período', 'Veículo'],
                            as_index=False
                        )['Total'].sum()
                    else:
                        df_total_agrupado = df_filtrado.groupby(
                            ['Oficina', 'Período', 'Veículo'],
                            as_index=False
                        )['Valor'].sum()
                        df_total_agrupado.rename(
                            columns={'Valor': 'Total'}, inplace=True
                        )

                    # Agrupar Volume incluindo Veículo
                    df_vol_agrupado = df_vol_calc.groupby(
                        ['Oficina', 'Período', 'Veículo'], as_index=False
                    )['Volume'].sum()

                    # Fazer merge incluindo Veículo
                    df_cpu = pd.merge(
                        df_total_agrupado,
                        df_vol_agrupado,
                        on=['Oficina', 'Período', 'Veículo'],
                        how='left'
                    )
                else:
                    # Agrupar Volume apenas por Oficina e Período
                    df_vol_agrupado = df_vol_calc.groupby(
                        ['Oficina', 'Período'], as_index=False
                    )['Volume'].sum()

                    # Fazer merge
                    df_cpu = pd.merge(
                        df_total_agrupado,
                        df_vol_agrupado,
                        on=['Oficina', 'Período'],
                        how='left'
                    )

                    # Se df_filtrado tem Veículo mas df_vol não, expandir
                    if tem_veiculo:
                        # Fazer merge com df_filtrado para obter Veículo
                        df_filtrado_veiculo = (
                            df_filtrado[['Oficina', 'Período', 'Veículo']]
                            .drop_duplicates()
                        )
                        df_cpu_expandido = pd.merge(
                            df_filtrado_veiculo,
                            df_cpu,
                            on=['Oficina', 'Período'],
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
        if coluna not in df_data.columns or 'Período' not in df_data.columns:
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


# Exibir gráfico por Período
if (coluna_visualizacao in df_visualizacao.columns and
        'Período' in df_visualizacao.columns):
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📊 CPU por Período")
    else:
        st.subheader("📊 Soma do Valor por Período")
    
    # Filtros específicos para este gráfico (multiselect)
    df_grafico_periodo = df_visualizacao.copy()
    
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
    
    # Criar gráfico com dados filtrados
    grafico_periodo = create_period_chart(
        df_grafico_periodo, coluna_visualizacao, tipo_visualizacao
    )
    if grafico_periodo:
        st.altair_chart(grafico_periodo, use_container_width=True)
    
    # Exibir gráfico de Volume logo abaixo, usando os mesmos filtros
    st.subheader("📊 Volume Total por Período")
    
    # Carregar dados de volume do arquivo df_vol.parquet
    df_vol = load_volume_data(ano_selecionado)
    
    if df_vol is not None:
        # Verificar se tem as colunas necessárias
        if 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
            # Aplicar os mesmos filtros do gráfico de período (Oficina e Veículo)
            df_vol_filtrado = df_vol.copy()
            
            # Aplicar filtro de Oficina se foi selecionado
            if 'Oficina' in df_vol_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de Veículo se foi selecionado
            if 'Veículo' in df_vol_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
            
            # Criar gráfico com dados filtrados
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
    
    # Tabela: Veículo, Oficina e Períodos (seguindo filtros da sidebar)
    st.markdown("---")
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        st.subheader("📋 Tabela - CPU por Veículo, Oficina e Período")
    else:
        st.subheader("📋 Tabela - Custo Total por Veículo, Oficina e Período")
    
    # Usar df_visualizacao (já tem os dados calculados com filtros da sidebar)
    # Verificar se tem as colunas necessárias
    tem_veiculo = 'Veículo' in df_visualizacao.columns
    tem_oficina = 'Oficina' in df_visualizacao.columns
    tem_periodo = 'Período' in df_visualizacao.columns
    
    if tem_veiculo and tem_oficina and tem_periodo:
        # Usar coluna_visualizacao que já está definida
        if coluna_visualizacao in df_visualizacao.columns:
            # Criar tabela pivot com Oficina e Veículo como índice (Oficina primeiro)
            df_tabela = df_visualizacao.pivot_table(
                index=['Oficina', 'Veículo'],
                columns='Período',
                values=coluna_visualizacao,
                aggfunc='sum',
                fill_value=0
            )
            
            # Ordenar colunas por ordem cronológica dos meses
            colunas_existentes = [
                col for col in ORDEM_MESES if col in df_tabela.columns
            ]
            colunas_restantes = [
                col for col in df_tabela.columns if col not in ORDEM_MESES
            ]
            df_tabela = df_tabela[colunas_existentes + colunas_restantes]
            
            # Calcular total por linha
            df_tabela['Total'] = df_tabela.sum(axis=1)
            df_tabela = df_tabela.sort_values(['Oficina', 'Veículo'])
            
            # Resetar índice para ter Oficina e Veículo como colunas (Oficina primeiro)
            df_tabela = df_tabela.reset_index()
            
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
            
            # Aplicar formatação apenas nas colunas numéricas (exceto Veículo e Oficina)
            df_tabela_formatado = df_tabela.copy()
            colunas_formatar = [col for col in df_tabela_formatado.columns 
                              if col not in ['Veículo', 'Oficina']]
            for col in colunas_formatar:
                df_tabela_formatado[col] = df_tabela_formatado[col].apply(
                    lambda x: formatar_valor(x, tipo_visualizacao)
                )
            
            # Agrupar por Oficina e criar expanders
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
                
                # Criar expander para cada oficina (fechado por padrão)
                with st.expander(
                    f"🏭 **{oficina}** - Total: {total_formatado} ({len(df_oficina)} veículo{'s' if len(df_oficina) > 1 else ''})",
                    expanded=False
                ):
                    # Remover coluna Oficina da tabela dentro do expander (já está no título)
                    df_oficina_display = df_oficina.drop(columns=['Oficina'])
                    
                    # Calcular totais por coluna (meses) usando dados numéricos
                    df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                    df_oficina_numerico = df_oficina_numerico.drop(columns=['Oficina'])
                    
                    # Criar linha de total
                    linha_total = {'Veículo': '**TOTAL**'}
                    
                    # Adicionar totais por coluna (meses e Total)
                    for col in df_oficina_numerico.columns:
                        if col != 'Veículo':
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


# Gráfico 2: Soma do Valor por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_oficina_chart(df_data, coluna, tipo_viz):
    """Cria gráfico de barras por Oficina"""
    try:
        if (coluna not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

        # Se for CPU e tiver coluna Veículo, agrupar por Oficina e Veículo
        if (tipo_viz == "CPU (Custo por Unidade)" and
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
    elif tipo_visualizacao == "Custo Total":
        if 'Total' in df_filtrado.columns:
            st.subheader("📊 Total por Veículo")
            grafico_total = create_total_chart(
                df_filtrado, 'Total', tipo_visualizacao
            )
            if grafico_total:
                st.altair_chart(grafico_total, use_container_width=True)
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
