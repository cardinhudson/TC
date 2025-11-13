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

# Título
st.title("📊 Dashboard - Visualização de Dados TC - KE5Z Group")
st.subheader("Análise de dados agrupados por Oficina e Período")

st.markdown("---")

# Função para carregar dados com cache


@st.cache_data(
    ttl=3600,
    max_entries=1,
    show_spinner=True
)
def load_data():
    """Carrega os dados do arquivo parquet"""
    try:
        # Caminho do arquivo parquet
        arquivo_parquet = "df_ke5z_group.parquet"

        if not os.path.exists(arquivo_parquet):
            st.error(f"❌ Arquivo não encontrado: {arquivo_parquet}")
            st.stop()
        
        # Carregar dados
        df = pd.read_parquet(arquivo_parquet)

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


# Carregar dados
try:
    df_total = load_data()
    st.sidebar.success("✅ Dados carregados com sucesso")
    st.sidebar.info(f"📊 {len(df_total):,} registros carregados")
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

# Filtro 1: Oficina
if 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    oficina_selecionada = st.sidebar.multiselect(
        "Selecione a Oficina:",
        oficina_opcoes,
        default=["Todos"] if "Todos" in oficina_opcoes else []
    )
    
    # Aplicar filtro
    if "Todos" not in oficina_selecionada and oficina_selecionada:
        df_filtrado = df_total[
            df_total['Oficina'].astype(str).isin(oficina_selecionada)
        ].copy()
    else:
        df_filtrado = df_total.copy()
else:
    df_filtrado = df_total.copy()

# Filtro 2: Período
if 'Período' in df_filtrado.columns:
    periodo_opcoes = get_filter_options(df_filtrado, 'Período')
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes
    )

    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Filtro 3: USI
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:",
        usi_opcoes,
        default=["Todos"] if "Todos" in usi_opcoes else []
    )
    
    if "Todos" not in usi_selecionada and usi_selecionada:
        df_filtrado = df_filtrado[
            df_filtrado['USI'].astype(str).isin(usi_selecionada)
        ].copy()

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


# Gráfico 1: Soma do Valor por Período
@st.cache_data(ttl=900, max_entries=2)
def create_period_chart(df_data):
    """Cria gráfico de barras por Período"""
    try:
        if 'Valor' not in df_data.columns:
            return None
        
        chart_data = df_data.groupby('Período')['Valor'].sum().reset_index()

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Período:N', title='Período', sort='-y'),
            y=alt.Y('Valor:Q', title='Soma do Valor (R$)'),
            color=alt.Color(
                'Valor:Q',
                title='Valor',
                scale=alt.Scale(scheme='redyellowgreen', reverse=True)
            ),
            tooltip=[
                alt.Tooltip('Período:N', title='Período'),
                alt.Tooltip('Valor:Q', title='Valor', format=',.2f')
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
            color='black',
            fontSize=12
        ).encode(
            text=alt.Text('Valor:Q', format=',.2f')
        )
        
        return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico por Período
if 'Valor' in df_filtrado.columns:
    st.subheader("📊 Soma do Valor por Período")
    grafico_periodo = create_period_chart(df_filtrado)
    if grafico_periodo:
        st.altair_chart(grafico_periodo, use_container_width=True)


# Gráfico 2: Soma do Valor por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_oficina_chart(df_data):
    """Cria gráfico de barras por Oficina"""
    try:
        if ('Valor' not in df_data.columns or
                'Oficina' not in df_data.columns):
            return None

        chart_data = df_data.groupby('Oficina')['Valor'].sum().reset_index()
        chart_data = chart_data.sort_values('Valor', ascending=False)

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Oficina:N', title='Oficina', sort='-y'),
            y=alt.Y('Valor:Q', title='Soma do Valor (R$)'),
            color=alt.Color(
                'Valor:Q',
                title='Valor',
                scale=alt.Scale(scheme='redyellowgreen', reverse=True)
            ),
            tooltip=[
                alt.Tooltip('Oficina:N', title='Oficina'),
                alt.Tooltip('Valor:Q', title='Valor', format=',.2f')
            ]
        ).properties(
            title='Soma do Valor por Oficina',
            height=400
        )

        return grafico_barras
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Exibir gráfico por Oficina
if ('Oficina' in df_filtrado.columns and
        'Valor' in df_filtrado.columns):
    st.subheader("📊 Soma do Valor por Oficina")
    grafico_oficina = create_oficina_chart(df_filtrado)
    if grafico_oficina:
        st.altair_chart(grafico_oficina, use_container_width=True)


# Gráfico 3: Volume por Período (se coluna Volume existir)
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data):
    """Cria gráfico de barras de Volume por Período"""
    try:
        if 'Volume' not in df_data.columns:
            return None
        
        chart_data = df_data.groupby('Período')['Volume'].sum().reset_index()

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Período:N', title='Período', sort='-y'),
            y=alt.Y('Volume:Q', title='Volume Total'),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='blues')
            ),
            tooltip=[
                alt.Tooltip('Período:N', title='Período'),
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
if 'Volume' in df_filtrado.columns:
    st.subheader("📊 Volume Total por Período")
    grafico_volume = create_volume_chart(df_filtrado)
    if grafico_volume:
        st.altair_chart(grafico_volume, use_container_width=True)

# Gráfico 4: Total por Período (se coluna Total existir)
@st.cache_data(ttl=900, max_entries=2)
def create_total_chart(df_data):
    """Cria gráfico de barras de Total por Período"""
    try:
        if 'Total' not in df_data.columns:
            return None
        
        chart_data = df_data.groupby('Período')['Total'].sum().reset_index()
        
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Período:N', title='Período', sort='-y'),
            y=alt.Y('Total:Q', title='Total (R$)'),
            color=alt.Color('Total:Q', title='Total',
                          scale=alt.Scale(scheme='redyellowgreen', reverse=True)),
            tooltip=[alt.Tooltip('Período:N', title='Período'),
                    alt.Tooltip('Total:Q', title='Total', format=',.2f')]
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

# Exibir gráfico de Total
if 'Total' in df_filtrado.columns:
    st.subheader("📊 Total por Período")
    grafico_total = create_total_chart(df_filtrado)
    if grafico_total:
        st.altair_chart(grafico_total, use_container_width=True)

# Tabela dinâmica: Valor por Oficina e Período
if 'Oficina' in df_filtrado.columns and 'Período' in df_filtrado.columns:
    st.markdown("---")
    st.subheader("📋 Tabela Dinâmica - Valor por Oficina e Período")
    
    if 'Valor' in df_filtrado.columns:
        df_pivot = df_filtrado.pivot_table(
            index='Oficina',
            columns='Período',
            values='Valor',
            aggfunc='sum',
            fill_value=0
        )
        
        # Calcular total por linha
        df_pivot['Total'] = df_pivot.sum(axis=1)
        df_pivot = df_pivot.sort_values('Total', ascending=False)
        
        # Formatar valores
        def formatar_valor(val):
            if isinstance(val, (int, float)):
                return f"R$ {val:,.2f}"
            return val
        
        # Aplicar formatação
        df_pivot_formatado = df_pivot.copy()
        for col in df_pivot_formatado.columns:
            df_pivot_formatado[col] = df_pivot_formatado[col].apply(formatar_valor)
        
        st.dataframe(df_pivot_formatado, use_container_width=True)

# Exibir tabela filtrada
st.markdown("---")
st.subheader("📋 Tabela Filtrada")
display_limit = 1000
if len(df_filtrado) > display_limit:
    st.info(f"📊 Mostrando {display_limit:,} de {len(df_filtrado):,} registros")
    df_display = df_filtrado.head(display_limit)
else:
    df_display = df_filtrado

st.dataframe(df_display, use_container_width=True)

# Footer
st.markdown("---")
st.info("💡 Dashboard TC - KE5Z Group com visualizações interativas")

