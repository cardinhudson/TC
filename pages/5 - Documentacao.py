import streamlit as st
import pandas as pd

# Configuração da página
st.set_page_config(
    page_title="Documentação - Forecast",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Documentação Completa do Sistema")

# Menu lateral
st.sidebar.title("📑 Navegação")
secao = st.sidebar.radio(
    "Selecione uma seção:",
    [
        "🏠 Visão Geral",
        "📄 Página 1 - TC Ext",
        "📄 Página 2 - Simulador Forecast",
        "📄 Página 3 - Forecast",
        "📄 Página 4 - Waterfall Analysis",
        "📊 Como Funciona o Forecast",
        "🎚️ Sensibilidade ao Volume",
        "📈 Inflação",
        "💡 Exemplos Práticos",
        "🔧 Configuração de Dados",
        "❓ Perguntas Frequentes"
    ]
)

# ===== VISÃO GERAL =====
if secao == "🏠 Visão Geral":
    st.header("🏠 Visão Geral do Sistema")
    
    st.markdown("""
    ## Sistema Completo de Análise de Custos e Forecast
    
    Este sistema é uma plataforma completa desenvolvida em Streamlit para análise de custos,
    visualização de dados históricos, previsão de custos futuros e análise de variações.
    
    ### Estrutura do Sistema:
    
    O sistema é composto por **5 páginas principais**:
    
    1. **📊 Página 1 - TC Ext**: Dashboard de visualização de dados históricos com gráficos interativos
    2. **📈 Página 2 - Simulador Forecast**: Simulador interativo para testar cenários de forecast
    3. **📉 Página 3 - Forecast**: Sistema completo de previsão de custos com sensibilidade e inflação
    4. **🌊 Página 4 - Waterfall Analysis**: Análise de variações entre períodos
    5. **📚 Página 5 - Documentação**: Esta documentação completa
    
    ### Arquitetura Técnica:
    
    - **Framework**: Streamlit
    - **Visualizações**: Altair (gráficos interativos)
    - **Processamento de Dados**: Pandas
    - **Formato de Dados**: Parquet (otimizado para performance)
    - **Cache**: Sistema de cache do Streamlit (@st.cache_data)
    
    ### Estrutura de Arquivos:
    
    ```
    C:\GIT\TC\
    ├── app.py                          # Aplicação principal (página inicial)
    ├── pages\
    │   ├── 1 - TC_Ext.py              # Dashboard de visualização
    │   ├── 2 - Simulador Forecast.py  # Simulador de forecast
    │   ├── 3 - Forecast.py            # Sistema de forecast
    │   ├── 4 - Waterfall_Analysis.py  # Análise waterfall
    │   └── 5 - Documentacao.py        # Documentação
    ├── dados\
    │   ├── historico_consolidado\
    │   │   ├── df_final_historico.parquet
    │   │   ├── df_vol_historico.parquet
    │   │   └── BUD\
    │   │       ├── df_final_historico_BUD.parquet
    │   │       └── df_vol_historico_BUD.parquet
    │   ├── 2024\
    │   └── 2025\
    └── dados.ipynb                     # Notebook para processar dados
    ```
    
    ### Principais Funcionalidades por Página:
    
    **Página 1 - TC Ext:**
    - Visualização de dados históricos
    - Gráficos de barras por período
    - Gráficos de volume
    - Linha tracejada de budget
    - Filtros interativos (Oficina, Veículo, Período)
    - Modos: Custo Total e CPU
    - Tabelas detalhadas com download
    
    **Página 2 - Simulador Forecast:**
    - Simulação interativa de cenários
    - Ajuste de sensibilidade em tempo real
    - Visualização de impactos
    
    **Página 3 - Forecast:**
    - Cálculo de forecast baseado em média histórica
    - Aplicação de sensibilidade ao volume
    - Aplicação de inflação
    - Gráficos de premissas
    - Tabelas detalhadas
    
    **Página 4 - Waterfall Analysis:**
    - Comparação entre períodos
    - Análise de variações
    - Cálculo de FLEX (Volume + Inflação)
    - Gráficos waterfall
    
    ### Dependências Principais:
    
    ```python
    streamlit>=1.28.0
    pandas>=2.0.0
    altair>=5.0.0
    numpy>=1.24.0
    openpyxl>=3.1.0  # Para exportação Excel
    pyarrow>=12.0.0  # Para arquivos Parquet
    ```
    """)
    
    st.info("""
    💡 **Importante:** Esta documentação contém detalhes técnicos completos para permitir
    a reconstrução completa do sistema. Navegue pelas seções específicas de cada página
    para entender todos os detalhes de implementação.
    """)

# ===== PÁGINA 1 - TC EXT =====
elif secao == "📄 Página 1 - TC Ext":
    st.header("📄 Página 1 - TC Ext - Dashboard de Visualização")
    
    st.markdown("""
    ## Visão Geral
    
    A página **TC Ext** é um dashboard completo para visualização de dados históricos de custos,
    com gráficos interativos, filtros avançados e comparação com dados de budget.
    
    ### Localização do Arquivo
    - **Caminho**: `pages/1 - TC_Ext.py`
    - **Título da Página**: "Dashboard TC Ext - df_final"
    - **Ícone**: 📊
    """)
    
    st.markdown("---")
    
    st.subheader("🔧 Estrutura Técnica Completa")
    
    st.markdown("""
    ### 1. Configuração Inicial
    
    ```python
    import streamlit as st
    import pandas as pd
    import altair as alt
    import os
    import numpy as np
    
    st.set_page_config(
        page_title="Dashboard TC Ext - df_final",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    ```
    
    **CSS Customizado:**
    - Redução de 20% no tamanho dos títulos (h1, h2, h3)
    - Aplicado via `st.markdown` com `unsafe_allow_html=True`
    """)
    
    st.markdown("---")
    
    st.subheader("📂 Funções de Carregamento de Dados")
    
    st.markdown("""
    ### 2.1. Função `load_data(ano_selecionado_param)`
    
    **Propósito**: Carrega dados históricos de custos do arquivo consolidado.
    
    **Localização do Arquivo:**
    ```
    dados/historico_consolidado/df_final_historico.parquet
    ```
    
    **Características:**
    - Cache: `@st.cache_data(ttl=3600, max_entries=10)`
    - TTL: 3600 segundos (1 hora)
    - Filtro de ano opcional após carregar
    - Otimização automática de tipos de dados
    
    **Processamento:**
    1. Carrega arquivo parquet do histórico consolidado
    2. Se ano específico selecionado, filtra por `Ano == int(ano_selecionado_param)`
    3. Converte colunas numéricas: `['Valor', 'Total', 'Volume', 'CPU']`
    4. Otimiza tipos: objetos com < 50% valores únicos → category
    5. Downcast de floats e ints para tipos menores
    
    **Colunas Esperadas:**
    - `Oficina` (texto/category)
    - `Veículo` (texto/category)
    - `Período` (texto/category)
    - `Ano` (int)
    - `Total` (float)
    - `Valor` (float, opcional)
    - `Volume` (float, opcional)
    - `CPU` (float, opcional)
    - `Type 05`, `Type 06`, `Account` (texto/category)
    
    ### 2.2. Função `load_volume_data(ano_selecionado_param)`
    
    **Propósito**: Carrega dados de volume histórico.
    
    **Localização do Arquivo:**
    ```
    dados/historico_consolidado/df_vol_historico.parquet
    ```
    
    **Características:**
    - Mesma estrutura de cache e otimização que `load_data`
    - Retorna `None` se arquivo não existir (não gera erro)
    
    **Colunas Esperadas:**
    - `Oficina`, `Veículo`, `Período`, `Ano`, `Volume`
    
    ### 2.3. Função `load_budget_data(ano_selecionado_param)`
    
    **Propósito**: Carrega dados de budget (custos) para comparação.
    
    **Localização do Arquivo:**
    ```
    dados/historico_consolidado/BUD/df_final_historico_BUD.parquet
    ```
    
    **Características:**
    - Mesma estrutura de cache e otimização
    - Usado para linha tracejada no gráfico
    
    ### 2.4. Função `load_budget_volume_data(ano_selecionado_param)`
    
    **Propósito**: Carrega dados de volume de budget.
    
    **Localização do Arquivo:**
    ```
    dados/historico_consolidado/BUD/df_vol_historico_BUD.parquet
    ```
    
    **Uso**: Necessário para cálculo de CPU no modo budget
    """)
    
    st.markdown("---")
    
    st.subheader("🎛️ Sistema de Filtros")
    
    st.markdown("""
    ### 3. Filtros na Sidebar
    
    **Ordem de Aplicação:**
    1. **Seleção de Ano** (radio button)
       - Opções: "Todos" + lista de anos disponíveis
       - Função: `listar_anos_disponiveis()` busca pastas numéricas em `dados/`
    
    2. **Tipo de Visualização** (radio button)
       - Opções: "Custo Total" ou "CPU (Custo por Unidade)"
       - Afeta qual coluna será usada nos gráficos
    
    3. **Filtro de Oficina** (multiselect)
       - Função: `get_filter_options(df, 'Oficina')` com cache
       - Opções: "Todos" + valores únicos ordenados
       - Aplicado via: `df['Oficina'].isin(oficina_selecionadas)`
    
    4. **Filtro de Veículo** (multiselect)
       - Mesma lógica do filtro de Oficina
       - Aplicado após filtro de Oficina
    
    5. **Filtro de USI** (multiselect, se coluna existir)
       - Opcional, só aparece se coluna 'USI' existir
    
    6. **Filtro de Período** (multiselect)
       - Ordenação cronológica usando `ORDEM_MESES`
       - Função `ordenar_por_mes()` garante ordem correta
    
    **Função `get_filter_options(df, column_name)`:**
    ```python
    @st.cache_data(ttl=1800, max_entries=5)
    def get_filter_options(df, column_name):
        if column_name in df.columns:
            opcoes = sorted(df[column_name].dropna().astype(str).unique().tolist())
            return ["Todos"] + opcoes
        return ["Todos"]
    ```
    
    **Função `ordenar_por_mes(df, coluna_periodo)`:**
    - Cria coluna temporária `_ordem_mes` mapeando meses para índices
    - Se houver coluna `Ano`, ordena por `['_ordem_ano', '_ordem_mes']`
    - Remove colunas temporárias após ordenação
    """)
    
    st.markdown("---")
    
    st.subheader("📊 Sistema de Gráficos")
    
    st.markdown("""
    ### 4. Gráfico Principal: `create_period_chart()`
    
    **Assinatura:**
    ```python
    @st.cache_data(ttl=900, max_entries=2)
    def create_period_chart(df_data, coluna, tipo_viz, df_budget=None, df_budget_vol=None):
    ```
    
    **Parâmetros:**
    - `df_data`: DataFrame com dados principais
    - `coluna`: Nome da coluna a visualizar ('Total' ou 'CPU')
    - `tipo_viz`: "Custo Total" ou "CPU (Custo por Unidade)"
    - `df_budget`: DataFrame de budget (opcional)
    - `df_budget_vol`: DataFrame de volume de budget (opcional)
    
    **Lógica de Agrupamento:**
    
    **Se houver coluna 'Ano':**
    - Agrupa por `['Ano', 'Período']`
    - Para CPU: Agrupa Total e Volume separadamente, depois calcula CPU
    - Para Custo Total: Soma diretamente a coluna
    - Cria coluna `'Período_Completo' = Período + ' ' + Ano`
    
    **Se NÃO houver coluna 'Ano':**
    - Agrupa apenas por `'Período'`
    - Mesma lógica de CPU vs Custo Total
    
    **Criação do Gráfico de Barras:**
    ```python
    grafico_barras = alt.Chart(chart_data).mark_bar().encode(
        x=alt.X(f'{coluna_periodo_grafico}:N', title='Período', sort=ordem_periodos),
        y=alt.Y(f'{coluna}:Q', title=titulo_y),
        color=alt.Color(f'{coluna}:Q', title=coluna, scale=alt.Scale(scheme='blues')),
        tooltip=[...]
    ).properties(title=titulo_grafico, height=400)
    ```
    
    **Rótulos nas Barras:**
    ```python
    rotulos = grafico_barras.mark_text(
        align='center', baseline='middle', dy=-10,
        color='black', fontSize=12
    ).encode(text=alt.Text(f'{coluna}:Q', format=formato_rotulo))
    ```
    
    **Linha Tracejada de Budget:**
    
    Se `df_budget` fornecido:
    1. Processa budget seguindo MESMA lógica dos dados principais
    2. Para CPU: Agrupa Total e Volume, calcula CPU
    3. Para Custo Total: Agrupa por Ano+Período ou apenas Período
    4. Filtra apenas períodos que existem em `ordem_periodos`
    5. Cria linha tracejada:
       ```python
       linha_budget = alt.Chart(budget_data).mark_line(
           strokeDash=[10, 5],  # Traço longo, espaço curto
           strokeWidth=2.5,
           color='#FF6B35',  # Laranja
           opacity=0.8
       ).encode(...)
       ```
    6. Adiciona bolinhas nos pontos:
       ```python
       pontos_budget = alt.Chart(budget_data).mark_circle(
           size=80,
           color='#FF6B35',
           opacity=0.9
       ).encode(...)
       ```
    7. Combina: `linha_budget = linha_budget + pontos_budget`
    
    **Combinação Final:**
    ```python
    if linha_budget is not None:
        return alt.layer(
            grafico_barras,
            rotulos,
            linha_budget
        ).resolve_scale(x='shared', y='shared')
    else:
        return grafico_barras + rotulos
    ```
    
    **IMPORTANTE**: `alt.layer()` com `resolve_scale()` garante que todos compartilhem o mesmo eixo X e Y.
    """)
    
    st.markdown("---")
    
    st.subheader("📈 Gráfico de Volume")
    
    st.markdown("""
    ### 5. Função `create_volume_chart()`
    
    **Assinatura:**
    ```python
    @st.cache_data(ttl=900, max_entries=2)
    def create_volume_chart(df_data):
    ```
    
    **Lógica:**
    - Mesma lógica de agrupamento que `create_period_chart`
    - Agrupa por Ano+Período (se houver Ano) ou apenas Período
    - Soma coluna 'Volume'
    - Cria gráfico de barras azuis
    - Adiciona rótulos com valores formatados
    """)
    
    st.markdown("---")
    
    st.subheader("🔗 Integração Budget com Filtros")
    
    st.markdown("""
    ### 6. Aplicação de Filtros ao Budget
    
    **Antes de chamar `create_period_chart()`:**
    
    ```python
    # Carregar dados de budget
    df_budget = load_budget_data(ano_selecionado)
    df_budget_vol = load_budget_volume_data(ano_selecionado)
    
    # Aplicar mesmos filtros de Oficina
    if 'Oficina' in df_budget_filtrado.columns:
        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
            df_budget_filtrado = df_budget_filtrado[
                df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
            ].copy()
    
    # Aplicar mesmos filtros de Veículo
    if 'Veículo' in df_budget_filtrado.columns:
        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
            df_budget_filtrado = df_budget_filtrado[
                df_budget_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
            ].copy()
    
    # Passar para função do gráfico
    grafico_periodo = create_period_chart(
        df_grafico_periodo, 
        coluna_visualizacao_grafico, 
        tipo_visualizacao,
        df_budget_filtrado, 
        df_budget_vol_filtrado
    )
    ```
    
    **IMPORTANTE**: Os filtros de Oficina e Veículo do gráfico são aplicados TAMBÉM aos dados de budget,
    garantindo que a comparação seja feita com os mesmos dados filtrados.
    """)
    
    st.markdown("---")
    
    st.subheader("📊 Cálculo de Flex Bud (FLEX de Volume)")
    
    st.markdown("""
    ### 7. Regra de Cálculo do Flex Bud
    
    O **Flex Bud** é calculado comparando os dados reais com os dados de budget, aplicando a sensibilidade
    de volume de forma fixa conforme a natureza do custo.
    
    #### 7.1. Regra de Sensibilidade Fixa
    
    A sensibilidade é aplicada de forma **fixa e automática** baseada no tipo de custo:
    
    - **Custo Fixo**: Sensibilidade = **0** (não varia com volume)
    - **Custo Variável**: Sensibilidade = **1** (varia 100% com volume)
    
    **IMPORTANTE**: Esta regra é fixa e não pode ser alterada pelo usuário. É aplicada automaticamente
    em todos os cálculos de Flex Bud.
    
    #### 7.2. Fórmula de Cálculo
    
    Para cada período e categoria, o Flex Bud é calculado da seguinte forma:
    
    **Passo 1: Calcular Proporção de Volume**
    ```
    Proporção_Volume = Volume_Budget / Volume_Real
    Variação_% = Proporção_Volume - 1.0
    ```
    
    **Passo 2: Aplicar Sensibilidade por Tipo de Custo**
    ```
    Para Custo Fixo:
    - Variação_Ajustada_Fixo = Variação_% × 0 = 0
    - FLEX_Fixo = Custo_Fixo_Real × 0 = 0
    
    Para Custo Variável:
    - Variação_Ajustada_Variável = Variação_% × 1 = Variação_%
    - FLEX_Variável = Custo_Variável_Real × Variação_%
    ```
    
    **Passo 3: Calcular Flex Bud Total**
    ```
    Flex_Bud_Total = FLEX_Fixo + FLEX_Variável
    ```
    
    **Passo 4: Para Modo CPU**
    ```
    Flex_Bud_CPU = Flex_Bud_Total / Volume_Real
    ```
    
    #### 7.3. Função `calcular_flex_budget()`
    
    **Assinatura:**
    ```python
    def calcular_flex_budget(df_real, df_real_vol, df_budget, df_budget_vol, tipo_viz, tem_ano):
    ```
    
    **Parâmetros:**
    - `df_real`: DataFrame com dados reais (deve ter coluna 'Custo' com valores 'Fixo'/'Variável')
    - `df_real_vol`: DataFrame com volumes reais
    - `df_budget`: DataFrame com dados de budget
    - `df_budget_vol`: DataFrame com volumes de budget
    - `tipo_viz`: "Custo Total" ou "CPU (Custo por Unidade)"
    - `tem_ano`: Boolean indicando se há coluna 'Ano'
    
    **Retorno:**
    - DataFrame com colunas: `Ano` (se tem_ano), `Período`, `FLEX`, `Budget_Total`
    
    **Lógica de Agrupamento:**
    
    **Se tem_ano:**
    - Agrupa volumes reais por `['Ano', 'Período']`
    - Agrupa volumes budget por `['Ano', 'Período']`
    - Agrupa custos reais por `['Ano', 'Período', 'Custo']`
    - Agrupa custos budget por `['Ano', 'Período', 'Custo']`
    
    **Se não tem_ano:**
    - Agrupa volumes reais por `['Período']`
    - Agrupa volumes budget por `['Período']`
    - Agrupa custos reais por `['Período', 'Custo']`
    - Agrupa custos budget por `['Período', 'Custo']`
    
    **Processamento:**
    1. Faz merge de volumes reais e budget por período
    2. Para cada período:
       - Calcula proporção de volume
       - Obtém custos reais separados por Fixo e Variável
       - Calcula FLEX aplicando sensibilidade fixa
       - Calcula Flex Bud Total
       - Se modo CPU: divide por Volume_Real
    3. Retorna DataFrame com FLEX calculado
    
    #### 7.4. Integração no Gráfico
    
    **Modificação da Função `create_period_chart()`:**
    
    A função agora recebe um parâmetro adicional:
    ```python
    def create_period_chart(df_data, coluna, tipo_viz, df_budget=None, df_budget_vol=None, df_real_vol=None):
    ```
    
    **Processamento:**
    1. Se `df_budget` e `df_real_vol` fornecidos:
       - Chama `calcular_flex_budget()` para calcular FLEX
       - Usa valores de FLEX ao invés de budget direto
       - Mantém valores originais de budget em `Budget_Total` para uso futuro
    2. Cria linha tracejada verde mostrando Flex Bud
    3. Tooltips mostram "Flex Bud" ao invés de "Budget"
    4. Legenda do gráfico identifica linha como "Flex Bud"
    
    **Visualização:**
    - Linha tracejada verde (`#2E7D32`)
    - Espessura: 1.5
    - Bolinhas nos pontos (tamanho 80)
    - Rótulos de texto acima da linha
    - Legenda compartilhada com barras (Real vs Flex Bud)
    
    #### 7.5. Tabela de Análise Flex Bud
    
    **Localização:** Abaixo do gráfico "📊 Soma do Valor por Período" (apenas modo "Custo Total")
    
    **Colunas da Tabela:**
    1. **BUD**: Valores originais do budget
    2. **Flex Bud - BUD**: Diferença entre Flex Bud e BUD (Flex_Bud - BUD)
    3. **Flex BUD**: Valores calculados de Flex Bud
    4. **Total - Flex Bud**: Diferença entre Total (real) e Flex Bud (Total - Flex_Bud)
    5. **Total**: Valores reais
    6. **Total / Flex Bud**: Razão entre Total e Flex Bud (Total / Flex_Bud)
    
    **Agrupamento Hierárquico:**
    - **Nível 1**: Custo (Fixo/Variável) - Expander com resumo
    - **Nível 2**: Type 05 - Expander com resumo
    - **Nível 3**: Type 06 - Tabela detalhada
    
    **Funcionalidade:**
    - Cada nível pode ser expandido/colapsado clicando no expander
    - Métricas de resumo em cada nível
    - Tabela detalhada apenas no nível mais baixo
    - Valores formatados como "R$ X,XXX.XX"
    
    **Função `calcular_tabela_flex_bud()`:**
    ```python
    def calcular_tabela_flex_bud(df_real, df_real_vol, df_budget, df_budget_vol, filtros_aplicados):
    ```
    
    **Processamento:**
    1. Agrupa dados reais por Custo, Type 05, Type 06
    2. Agrupa dados budget por Custo, Type 05, Type 06
    3. Agrupa volumes reais e budget por categoria
    4. Para cada categoria:
       - Calcula proporção de volume
       - Calcula Flex Bud aplicando sensibilidade fixa
       - Calcula todas as diferenças e razões
    5. Retorna DataFrame com todas as colunas calculadas
    
    #### 7.6. Exemplo Prático
    
    **Cenário:**
    - Volume Real: 10.000 unidades
    - Volume Budget: 12.000 unidades
    - Custo Fixo Real: R$ 50.000
    - Custo Variável Real: R$ 100.000
    - BUD Total: R$ 160.000
    
    **Cálculo:**
    ```
    Proporção_Volume = 12.000 / 10.000 = 1.2
    Variação_% = 1.2 - 1.0 = 0.2 (20% de aumento)
    
    FLEX_Fixo = 50.000 × 0.2 × 0 = R$ 0
    FLEX_Variável = 100.000 × 0.2 × 1 = R$ 20.000
    
    Flex_Bud_Total = 0 + 20.000 = R$ 20.000
    ```
    
    **Resultado na Tabela:**
    - BUD: R$ 160.000
    - Flex Bud - BUD: R$ 20.000 - R$ 160.000 = -R$ 140.000
    - Flex BUD: R$ 20.000
    - Total - Flex Bud: R$ 150.000 - R$ 20.000 = R$ 130.000
    - Total: R$ 150.000
    - Total / Flex Bud: R$ 150.000 / R$ 20.000 = 7.50
    
    #### 7.7. Pontos Importantes
    
    1. **Sensibilidade Fixa:**
       - Não pode ser alterada pelo usuário
       - Fixo sempre = 0, Variável sempre = 1
       - Aplicada automaticamente em todos os cálculos
    
    2. **Agrupamento por Categoria:**
       - Flex Bud é calculado por categoria (Custo, Type 05, Type 06)
       - Cada categoria tem seu próprio volume e proporção
       - Permite análise detalhada por tipo de custo
    
    3. **Valores Originais Preservados:**
       - Valores originais de BUD são mantidos na coluna `Budget_Total`
       - Permite uso futuro dos valores originais se necessário
    
    4. **Apenas Modo Custo Total:**
       - Tabela de análise aparece apenas no modo "Custo Total"
       - No modo CPU, apenas a linha tracejada é exibida
    """)
    
    st.markdown("---")
    
    st.subheader("📋 Sistema de Tabelas")
    
    st.markdown("""
    ### 7. Tabelas de Dados
    
    **Tabela Principal:**
    - Agrupa por Oficina, Veículo, Período (e Ano se existir)
    - Calcula CPU quando necessário (Total/Volume)
    - Ordena por período cronologicamente
    - Mostra valores formatados
    
    **Tabela Filtrada:**
    - Expander colapsável
    - Mostra TODAS as linhas (sem limite)
    - Remove colunas desnecessárias: `['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']`
    - Botão de download Excel
    
    **Download Excel:**
    ```python
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
    file_name = "TC_Ext_tabela_filtrada.xlsx"
    file_path = os.path.join(downloads_path, file_name)
    
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df_visualizacao.to_excel(writer, index=False, sheet_name='Dados_Filtrados')
    ```
    """)
    
    st.markdown("---")
    
    st.subheader("💾 Resumo na Sidebar")
    
    st.markdown("""
    ### 8. Informações Exibidas
    
    A sidebar mostra:
    - **Linhas**: Total de registros após filtros
    - **Total Valor**: Soma da coluna 'Valor' (se existir)
    - **Total**: Soma da coluna 'Total'
    - **Total Volume**: Soma da coluna 'Volume' (se existir)
    - **CPU Médio**: Média dos valores de CPU > 0
    - **Visualizando**: Tipo de visualização selecionado
    
    **NOTA**: Mensagens de debug foram removidas para manter a sidebar limpa.
    """)
    
    st.markdown("---")
    
    st.subheader("🎯 Pontos Críticos de Implementação")
    
    st.markdown("""
    ### 9. Detalhes Importantes
    
    1. **Agrupamento Consistente:**
       - Sempre agrupar por Ano+Período quando coluna Ano existir
       - Para CPU: SEMPRE somar Total e Volume separadamente, depois calcular
       - NUNCA somar CPUs já calculados
    
    2. **Eixo X Compartilhado:**
       - Usar `alt.layer()` com `resolve_scale(x='shared', y='shared')`
       - Garante que barras e linha de budget usem mesmo eixo X
       - Mantém proporção correta
    
    3. **Filtros de Budget:**
       - Aplicar EXATAMENTE os mesmos filtros de Oficina e Veículo
       - Garantir que comparação seja justa
    
    4. **Ordenação de Períodos:**
       - Usar função `ordenar_por_mes()` sempre
       - Considerar Ano se existir
       - Manter ordem cronológica correta
    
    5. **Cache:**
       - Funções de carregamento: TTL 3600s
       - Funções de gráficos: TTL 900s
       - Cache ajuda performance mas pode precisar limpeza manual
    """)

# ===== PÁGINA 2 - SIMULADOR FORECAST =====
elif secao == "📄 Página 2 - Simulador Forecast":
    st.header("📄 Página 2 - Simulador Forecast")
    
    st.markdown("""
    ## Visão Geral
    
    A página **Simulador Forecast** permite testar cenários de forecast de forma interativa,
    ajustando sensibilidade e visualizando impactos em tempo real.
    
    ### Localização do Arquivo
    - **Caminho**: `pages/2 - Simulador Forecast.py`
    - **Título da Página**: "Simulador Forecast"
    - **Ícone**: 📈
    
    ### Funcionalidades Principais
    
    1. **Simulação Interativa**
       - Ajuste de sensibilidade em tempo real
       - Visualização imediata de resultados
       - Comparação de cenários
    
    2. **Cálculos em Tempo Real**
       - Baseado em dados históricos
       - Aplica sensibilidade configurável
       - Mostra variações percentuais
    
    **NOTA**: Esta página está em desenvolvimento. Detalhes completos serão adicionados conforme
    a implementação for finalizada.
    """)

# ===== PÁGINA 3 - FORECAST =====
elif secao == "📄 Página 3 - Forecast":
    st.header("📄 Página 3 - Forecast - Sistema de Previsão")
    
    st.markdown("""
    ## Visão Geral
    
    A página **Forecast** é o sistema completo de previsão de custos, calculando forecast baseado
    em média histórica, aplicando sensibilidade ao volume e inflação.
    
    ### Localização do Arquivo
    - **Caminho**: `pages/3 - Forecast.py`
    - **Título da Página**: "Forecast"
    - **Ícone**: 📉
    
    ### Funcionalidades Principais
    
    1. **Cálculo de Forecast**
       - Média histórica padronizada
       - Aplicação de sensibilidade ao volume
       - Aplicação de inflação
       - Cálculo linha a linha
    
    2. **Visualizações**
       - Gráficos de premissas (custo e volume)
       - Tabelas detalhadas
       - Download de resultados
    
    3. **Configurações**
       - Sensibilidade global ou detalhada
       - Inflação global ou por Type 06
       - Seleção de períodos para cálculo
    
    **DETALHES COMPLETOS**: Veja a seção "📊 Como Funciona o Forecast" para entender
    toda a metodologia de cálculo implementada.
    """)

# ===== PÁGINA 4 - WATERFALL ANALYSIS =====
elif secao == "📄 Página 4 - Waterfall Analysis":
    st.header("📄 Página 4 - Waterfall Analysis")
    
    st.markdown("""
    ## Visão Geral
    
    A página **Waterfall Analysis** permite comparar custos entre dois períodos e identificar
    as causas das variações, separando os efeitos de volume, sensibilidade e inflação.
    
    ### Localização do Arquivo
    - **Caminho**: `pages/4 - Waterfall_Analysis.py`
    - **Título da Página**: "Waterfall Analysis"
    - **Ícone**: 🌊
    
    ### Funcionalidades Principais
    
    1. **Modos de Comparação**
       - **Mês a Mês**: Compara dois meses específicos
       - **Ano a Ano**: Compara dois anos completos (usa volumes totais)
       - **Múltiplos Meses**: Mostra série temporal completa
    
    2. **Cálculo FLEX**
       - **FLEX Volume**: Efeito da variação de volume + sensibilidade
       - **FLEX Inflação**: Efeito da inflação aplicada
       - Separação clara dos efeitos
    
    3. **Gráficos Waterfall**
       - Visualização de variações
       - Barras coloridas (verde=aumento, vermelho=redução)
       - Tooltips informativos
    
    ### Correção Implementada: Ano a Ano
    
    **Problema Original:**
    - Usava primeiro mês do ano inicial vs último mês do ano final
    - Não considerava volumes totais anuais
    
    **Solução:**
    - Agora usa volume TOTAL do ano inicial
    - Agora usa volume TOTAL do ano final
    - Comparação matematicamente correta
    
    **DETALHES COMPLETOS**: Veja a seção "🌊 Waterfall Analysis" para entender
    toda a metodologia de cálculo FLEX implementada.
    """)

# ===== COMO FUNCIONA O FORECAST =====
elif secao == "📊 Como Funciona o Forecast":
    st.header("📊 Como Funciona o Forecast")
    
    st.markdown("""
    ## Metodologia de Cálculo
    
    O forecast é calculado em várias etapas:
    """)
    
    # Etapa 1
    st.subheader("1️⃣ Cálculo da Média Mensal Histórica")
    st.markdown("""
    Para cada combinação de **Oficina**, **Veículo** e **Tipo de Custo** (Fixo/Variável),
    calculamos a média mensal dos custos históricos usando uma **lógica padronizada** que garante
    consistência entre gráficos, tabelas e cálculos de forecast.
    
    **Lógica Padronizada:**
    1. **Normalização de Períodos**: Períodos sem ano recebem o ano de referência dos períodos selecionados
    2. **Filtro por Períodos Selecionados**: Apenas os períodos marcados para cálculo são considerados
    3. **Exclusão de Meses**: Meses marcados para exclusão são removidos do cálculo
    4. **Filtro por Ano**: Apenas períodos do ano de referência são considerados (evita somar meses de anos diferentes)
    5. **Agregação**: Soma dos totais por período único (mês + ano)
    6. **Média**: Média aritmética dos valores agregados
    
    **Fórmula:**
    ```
    Média_Mensal_Histórica = Média(Soma(Custos_por_Período_Único))
    ```
    
    **Importante:**
    - Cada período (mês + ano) é tratado como único
    - "Julho 2024" e "Julho 2025" são períodos diferentes
    - A média é calculada sobre os totais agregados por período, não sobre linhas individuais
    - Esta mesma lógica é aplicada tanto para custos quanto para volumes
    """)
    
    # Exemplo visual
    st.markdown("**Exemplo:**")
    df_exemplo1 = pd.DataFrame({
        'Mês': ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio'],
        'Custo': [100000, 105000, 98000, 102000, 103000]
    })
    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(df_exemplo1, use_container_width=True)
    with col2:
        media = df_exemplo1['Custo'].mean()
        st.metric("Média Mensal", f"R$ {media:,.2f}")
    
    st.markdown("---")
    
    # Etapa 2
    st.subheader("2️⃣ Cálculo do Volume Médio Histórico")
    st.markdown("""
    Calculamos o volume médio de produção histórico para cada **Oficina** e **Veículo** usando
    a **mesma lógica padronizada** aplicada aos custos, garantindo consistência total.
    
    **Lógica Padronizada (Idêntica à de Custos):**
    1. **Normalização de Períodos**: Períodos sem ano recebem o ano de referência
    2. **Filtro por Períodos Selecionados**: Apenas períodos marcados para cálculo
    3. **Exclusão de Meses**: Meses marcados para exclusão são removidos
    4. **Filtro por Ano**: Apenas períodos do ano de referência (evita duplicação entre anos)
    5. **Agregação**: Soma dos volumes por período único (mês + ano)
    6. **Média**: Média aritmética dos volumes agregados
    
    **Fórmula:**
    ```
    Volume_Médio_Histórico = Média(Soma(Volumes_por_Período_Único))
    ```
    
    **Importante:**
    - A mesma lógica de custos é aplicada para volumes
    - Garante que a média histórica de volume corresponde à média acumulada do gráfico detalhado
    - Evita somar volumes de meses com mesmo nome mas anos diferentes
    """)
    
    df_exemplo2 = pd.DataFrame({
        'Mês': ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio'],
        'Volume': [1000, 1050, 980, 1020, 1030]
    })
    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(df_exemplo2, use_container_width=True)
    with col2:
        media_vol = df_exemplo2['Volume'].mean()
        st.metric("Volume Médio", f"{media_vol:,.0f}")
    
    st.markdown("---")
    
    # Etapa 3
    st.subheader("3️⃣ Cálculo da Proporção de Volume")
    st.markdown("""
    Para cada mês futuro, calculamos a proporção entre o volume futuro e o volume médio histórico.
    
    **Fórmula:**
    ```
    Proporção_Volume = Volume_Futuro / Volume_Médio_Histórico
    ```
    
    **Interpretação:**
    - Proporção = 1.0 → Volume igual ao histórico
    - Proporção > 1.0 → Volume maior que o histórico (ex: 1.2 = +20%)
    - Proporção < 1.0 → Volume menor que o histórico (ex: 0.8 = -20%)
    """)
    
    st.markdown("**Exemplo:**")
    df_exemplo3 = pd.DataFrame({
        'Mês': ['Novembro', 'Dezembro'],
        'Volume Médio Histórico': [1016, 1016],
        'Volume Futuro': [1100, 950],
        'Proporção': [1.083, 0.935],
        'Variação %': ['+8.3%', '-6.5%']
    })
    st.dataframe(df_exemplo3, use_container_width=True)
    
    st.markdown("---")
    
    # Etapa 4
    st.subheader("4️⃣ Aplicação da Sensibilidade")
    st.markdown("""
    A sensibilidade define quanto cada tipo de custo varia em relação à variação do volume.
    
    **Fórmula:**
    ```
    Variação_Percentual = Proporção_Volume - 1.0
    Variação_Ajustada = Variação_Percentual × Sensibilidade
    Proporção_Ajustada = 1.0 + Variação_Ajustada
    ```
    
    **Onde:**
    - **Sensibilidade = 0.0**: Custo não varia (totalmente fixo)
    - **Sensibilidade = 0.5**: Custo varia 50% da variação do volume
    - **Sensibilidade = 1.0**: Custo varia 100% da variação do volume (totalmente variável)
    """)
    
    st.markdown("---")
    
    # Etapa 5
    st.subheader("5️⃣ Cálculo do Forecast Final")
    st.markdown("""
    Finalmente, calculamos o forecast aplicando a proporção ajustada à média histórica.
    O cálculo é feito **linha a linha** para cada combinação de Oficina, Veículo e Tipo de Custo,
    garantindo precisão matemática.
    
    **Fórmula Completa (Linha a Linha):**
    ```
    Proporção_Volume = Volume_do_Mês / Volume_Médio_Histórico
    Variação_Percentual = Proporção_Volume - 1.0
    Variação_Ajustada = Variação_Percentual × Sensibilidade
    Fator_Variação = 1.0 + Variação_Ajustada
    Fator_Inflação = 1.0 + (Inflação / 100.0)
    Forecast = Média_Mensal_Histórica × Fator_Variação × Fator_Inflação
    ```
    
    **Total do Forecast:**
    ```
    Total_Forecast = Soma(Forecast_de_Todas_as_Linhas)
    ```
    
    **Características Importantes:**
    - Cálculo linha a linha, sem ajustes manuais
    - Total é sempre a soma das linhas individuais
    - Se sensibilidade = 0 e inflação = 0, forecast = média histórica
    - Aplicação consistente de sensibilidade baseada no Tipo_Custo (Fixo/Variável)
    """)

# ===== SENSIBILIDADE AO VOLUME =====
elif secao == "🎚️ Sensibilidade ao Volume":
    st.header("🎚️ Sensibilidade ao Volume")
    
    st.markdown("""
    ## O que é Sensibilidade?
    
    A sensibilidade define o quanto um custo varia em relação à variação do volume de produção.
    É um valor entre **0** e **1** que funciona como um multiplicador da variação do volume.
    """)
    
    # Explicação visual
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🔵 Custo Fixo
        
        **Sensibilidade padrão: 0.0**
        
        Custos que tradicionalmente não variam com o volume:
        - Aluguel
        - Salários administrativos
        - Seguros
        - Depreciação
        
        ⚙️ **Ajuste a sensibilidade** se houver alguma variação:
        - 0.0 = Totalmente fixo
        - 0.2 = Varia 20% da variação do volume
        - 0.5 = Varia 50% da variação do volume
        """)
    
    with col2:
        st.markdown("""
        ### 🟠 Custo Variável
        
        **Sensibilidade padrão: 1.0**
        
        Custos que variam diretamente com o volume:
        - Matéria-prima
        - Mão de obra direta
        - Energia (produção)
        - Embalagens
        
        ⚙️ **Ajuste a sensibilidade** para variações parciais:
        - 1.0 = Totalmente variável
        - 0.8 = Varia 80% da variação do volume
        - 0.5 = Varia 50% da variação do volume
        """)
    
    st.markdown("---")
    
    # Simulador interativo
    st.subheader("🧮 Simulador de Sensibilidade")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        custo_base = st.number_input(
            "Custo Base (R$)",
            min_value=0.0,
            value=100000.0,
            step=1000.0,
            format="%.2f"
        )
    
    with col2:
        variacao_volume = st.slider(
            "Variação do Volume (%)",
            min_value=-50,
            max_value=50,
            value=10,
            step=5
        )
    
    with col3:
        sensibilidade = st.slider(
            "Sensibilidade",
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            step=0.1
        )
    
    # Cálculos
    variacao_decimal = variacao_volume / 100
    variacao_ajustada = variacao_decimal * sensibilidade
    proporcao_ajustada = 1.0 + variacao_ajustada
    forecast = custo_base * proporcao_ajustada
    variacao_custo = forecast - custo_base
    variacao_custo_pct = (variacao_custo / custo_base) * 100
    
    # Resultados
    st.markdown("### 📊 Resultados:")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Custo Base",
            f"R$ {custo_base:,.2f}"
        )
    
    with col2:
        st.metric(
            "Variação do Volume",
            f"{variacao_volume:+.0f}%"
        )
    
    with col3:
        st.metric(
            "Variação do Custo",
            f"{variacao_custo_pct:+.1f}%",
            f"R$ {variacao_custo:+,.2f}"
        )
    
    with col4:
        st.metric(
            "Forecast",
            f"R$ {forecast:,.2f}"
        )
    
    # Explicação do cálculo
    st.markdown("### 🔍 Detalhamento do Cálculo:")
    st.code(f"""
1. Variação do Volume: {variacao_volume}% = {variacao_decimal:.2f}
2. Sensibilidade: {sensibilidade:.1f}
3. Variação Ajustada: {variacao_decimal:.2f} × {sensibilidade:.1f} = {variacao_ajustada:.3f}
4. Proporção Ajustada: 1.0 + {variacao_ajustada:.3f} = {proporcao_ajustada:.3f}
5. Forecast: R$ {custo_base:,.2f} × {proporcao_ajustada:.3f} = R$ {forecast:,.2f}
    """)

# ===== INFLAÇÃO =====
elif secao == "📈 Inflação":
    st.header("📈 Inflação no Forecast")
    
    st.markdown("""
    ## Como Funciona a Inflação?
    
    A inflação é um ajuste percentual aplicado aos custos para refletir o aumento de preços esperado.
    No sistema de Forecast, a inflação é aplicada de forma **única** no primeiro mês da previsão.
    """)
    
    # Explicação visual
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### ✅ Como É Aplicado (CORRETO)
        
        A inflação é aplicada **uma única vez** no primeiro mês e o valor ajustado é mantido nos meses seguintes.
        
        **Exemplo: Inflação de 5%**
        - **Agosto**: R$ 100 → R$ 105 (+5%)
        - **Setembro**: R$ 100 → R$ 105 (mantém)
        - **Outubro**: R$ 100 → R$ 105 (mantém)
        - **Novembro**: R$ 100 → R$ 105 (mantém)
        - **Dezembro**: R$ 100 → R$ 105 (mantém)
        
        ✅ O ajuste é feito uma vez e permanece constante.
        """)
    
    with col2:
        st.markdown("""
        ### ❌ Como NÃO É Aplicado
        
        A inflação **NÃO** é acumulada mês a mês (juros compostos).
        
        **Exemplo: Inflação de 5% (se fosse acumulada)**
        - **Agosto**: R$ 100 → R$ 105 (+5%)
        - **Setembro**: R$ 105 → R$ 110.25 (+10.25%)
        - **Outubro**: R$ 110.25 → R$ 115.76 (+15.76%)
        - **Novembro**: R$ 115.76 → R$ 121.55 (+21.55%)
        - **Dezembro**: R$ 121.55 → R$ 127.63 (+27.63%)
        
        ❌ Isso NÃO acontece no sistema.
        """)
    
    st.markdown("---")
    
    # Modos de configuração
    st.markdown("""
    ## 🎛️ Modos de Configuração
    
    ### 1. Modo Global (Fixo/Variável)
    
    No modo global, você pode definir uma **inflação única** que será aplicada a **todos** os itens:
    
    - Digite o percentual no campo "Inflação Mensal Global (%)"
    - Exemplo: `5.0` para 5% de inflação
    - Clique em "Aplicar Configurações"
    - Todos os custos receberão o mesmo ajuste de inflação
    
    **Quando usar:**
    - Quando todos os custos têm a mesma expectativa de inflação
    - Para análises rápidas e simplificadas
    - Quando não há diferenciação de inflação por categoria
    
    ### 2. Modo Detalhado (por Type 06)
    
    No modo detalhado, você pode definir uma **inflação específica** para cada Type 06:
    
    - Cada linha da tabela tem seu próprio campo de inflação
    - Digite valores diferentes para cada categoria
    - Exemplo: 5% para matéria-prima, 3% para mão de obra
    - Clique em "Aplicar Configurações"
    
    **Quando usar:**
    - Quando diferentes categorias têm expectativas de inflação diferentes
    - Para análises mais precisas e detalhadas
    - Quando há informações específicas por tipo de custo
    """)
    
    st.markdown("---")
    
    # Fórmula
    st.markdown("""
    ## 🧮 Fórmula de Cálculo
    
    O valor final do forecast é calculado da seguinte forma:
    """)
    
    st.code("""
Valor Forecast = Média Histórica × (1 + Sensibilidade × Variação Volume) × (1 + Inflação)
    """, language="python")
    
    st.markdown("""
    **Onde:**
    - **Média Histórica**: Custo médio mensal dos meses realizados
    - **Sensibilidade**: Valor entre 0 e 1 (0% a 100%)
    - **Variação Volume**: Percentual de mudança no volume (ex: 1.1 = +10%)
    - **Inflação**: Percentual de inflação (ex: 0.05 = 5%)
    
    ### Exemplo Completo:
    
    **Dados:**
    - Média Histórica: R$ 1.000
    - Sensibilidade: 0.8 (80%)
    - Volume Histórico: 100 unidades
    - Volume Futuro: 110 unidades (+10%)
    - Inflação: 5%
    
    **Cálculo:**
    1. Variação Volume = 110 / 100 = 1.1 (ou +10%)
    2. Variação Percentual = 1.1 - 1.0 = 0.1 (10%)
    3. Variação Ajustada = 0.1 × 0.8 = 0.08 (8%)
    4. Fator Volume = 1.0 + 0.08 = 1.08
    5. Fator Inflação = 1.0 + 0.05 = 1.05
    6. **Valor Final = R$ 1.000 × 1.08 × 1.05 = R$ 1.134**
    
    Este valor de R$ 1.134 será mantido em **todos** os meses da previsão.
    """)
    
    st.markdown("---")
    
    # Dicas
    st.markdown("""
    ## 💡 Dicas Importantes
    
    1. **📊 Valores Típicos**
       - Inflação anual de 6% ≈ 0.5% ao mês
       - Inflação anual de 12% ≈ 1% ao mês
       - Use valores mensais, não anuais!
    
    2. **🎯 Precisão**
       - No modo global: mais rápido, menos preciso
       - No modo detalhado: mais demorado, mais preciso
    
    3. **🔄 Atualização**
       - Sempre clique em "Aplicar Configurações" após ajustar
       - Os valores só são aplicados após clicar no botão
    
    4. **📈 Cenários**
       - Teste diferentes valores de inflação
       - Compare cenários otimista, realista e pessimista
       - Use o botão "Limpar Configurações" para resetar
    """)

# ===== EXEMPLOS PRÁTICOS =====
elif secao == "💡 Exemplos Práticos":
    st.header("💡 Exemplos Práticos")
    
    st.markdown("""
    ## Cenários Reais de Forecast
    
    Veja exemplos práticos de como o sistema calcula o forecast em diferentes situações.
    """)
    
    # Exemplo 1
    st.subheader("📌 Exemplo 1: Aumento de Volume")
    
    st.markdown("""
    **Situação:**
    - Custo Fixo: R$ 100.000/mês (Sensibilidade: 0.0)
    - Custo Variável: R$ 50.000/mês (Sensibilidade: 1.0)
    - Volume Médio Histórico: 1.000 unidades
    - Volume Futuro (Novembro): 1.200 unidades (+20%)
    """)
    
    df_ex1 = pd.DataFrame({
        'Tipo de Custo': ['Fixo', 'Variável', 'Total'],
        'Média Histórica': ['R$ 100.000', 'R$ 50.000', 'R$ 150.000'],
        'Sensibilidade': ['0.0', '1.0', '-'],
        'Variação Volume': ['+20%', '+20%', '+20%'],
        'Variação Aplicada': ['0%', '+20%', '-'],
        'Forecast': ['R$ 100.000', 'R$ 60.000', 'R$ 160.000'],
        'Diferença': ['R$ 0', '+R$ 10.000', '+R$ 10.000']
    })
    
    st.dataframe(df_ex1, use_container_width=True)
    
    st.success("""
    ✅ **Resultado:** O custo fixo permanece em R$ 100.000, enquanto o custo variável aumenta 
    20% para R$ 60.000, resultando em um forecast total de R$ 160.000.
    """)
    
    st.markdown("---")
    
    # Exemplo 2
    st.subheader("📌 Exemplo 2: Redução de Volume")
    
    st.markdown("""
    **Situação:**
    - Custo Fixo: R$ 100.000/mês (Sensibilidade: 0.2)
    - Custo Variável: R$ 50.000/mês (Sensibilidade: 1.0)
    - Volume Médio Histórico: 1.000 unidades
    - Volume Futuro (Dezembro): 800 unidades (-20%)
    """)
    
    df_ex2 = pd.DataFrame({
        'Tipo de Custo': ['Fixo', 'Variável', 'Total'],
        'Média Histórica': ['R$ 100.000', 'R$ 50.000', 'R$ 150.000'],
        'Sensibilidade': ['0.2', '1.0', '-'],
        'Variação Volume': ['-20%', '-20%', '-20%'],
        'Variação Aplicada': ['-4%', '-20%', '-'],
        'Forecast': ['R$ 96.000', 'R$ 40.000', 'R$ 136.000'],
        'Diferença': ['-R$ 4.000', '-R$ 10.000', '-R$ 14.000']
    })
    
    st.dataframe(df_ex2, use_container_width=True)
    
    st.success("""
    ✅ **Resultado:** Com sensibilidade de 0.2 no custo fixo, ele reduz apenas 4% (20% × 0.2) 
    para R$ 96.000. O custo variável reduz 20% para R$ 40.000, totalizando R$ 136.000.
    """)
    
    st.markdown("---")
    
    # Exemplo 3
    st.subheader("📌 Exemplo 3: Sensibilidade Parcial")
    
    st.markdown("""
    **Situação:**
    - Custo Fixo: R$ 100.000/mês (Sensibilidade: 0.3)
    - Custo Variável: R$ 50.000/mês (Sensibilidade: 0.7)
    - Volume Médio Histórico: 1.000 unidades
    - Volume Futuro: 1.150 unidades (+15%)
    """)
    
    df_ex3 = pd.DataFrame({
        'Tipo de Custo': ['Fixo', 'Variável', 'Total'],
        'Média Histórica': ['R$ 100.000', 'R$ 50.000', 'R$ 150.000'],
        'Sensibilidade': ['0.3', '0.7', '-'],
        'Variação Volume': ['+15%', '+15%', '+15%'],
        'Variação Aplicada': ['+4.5%', '+10.5%', '-'],
        'Forecast': ['R$ 104.500', 'R$ 55.250', 'R$ 159.750'],
        'Diferença': ['+R$ 4.500', '+R$ 5.250', '+R$ 9.750']
    })
    
    st.dataframe(df_ex3, use_container_width=True)
    
    st.success("""
    ✅ **Resultado:** Com sensibilidades parciais, o custo fixo varia 4,5% (15% × 0.3) e o 
    variável 10,5% (15% × 0.7), permitindo um ajuste mais fino do forecast.
    """)
    
    st.markdown("---")
    
    # Nova seção sobre padronização
    st.subheader("🔧 Padronização de Cálculos")
    
    st.markdown("""
    **Importante:** O sistema utiliza uma lógica padronizada para garantir que todos os cálculos
    (gráficos, tabelas, forecast) usem exatamente a mesma média histórica.
    
    **Benefícios:**
    - ✅ Consistência entre gráficos e tabelas
    - ✅ Média acumulada do gráfico detalhado = Média histórica do gráfico principal
    - ✅ Total das tabelas = Soma das linhas individuais
    - ✅ Forecast baseado na mesma média usada nos gráficos
    
    **Como Funciona:**
    1. A média histórica é calculada uma única vez usando a função padronizada
    2. Esta média é usada em todos os lugares (gráficos, tabelas, forecast)
    3. Períodos são tratados como únicos (mês + ano)
    4. Meses excluídos são removidos de todos os cálculos
    """)
    
    st.markdown("---")
    
    # Tabela comparativa
    st.subheader("📊 Comparação de Cenários")
    
    st.markdown("""
    Veja como diferentes combinações de sensibilidade afetam o forecast com uma variação de +10% no volume:
    """)
    
    df_comp = pd.DataFrame({
        'Cenário': [
            'Tradicional (Fixo=0, Var=1)',
            'Semi-Fixo (Fixo=0.2, Var=1)',
            'Semi-Variável (Fixo=0, Var=0.7)',
            'Equilibrado (Fixo=0.3, Var=0.7)',
            'Totalmente Variável (Fixo=1, Var=1)'
        ],
        'Custo Fixo': [
            'R$ 100.000',
            'R$ 102.000',
            'R$ 100.000',
            'R$ 103.000',
            'R$ 110.000'
        ],
        'Custo Variável': [
            'R$ 55.000',
            'R$ 55.000',
            'R$ 53.500',
            'R$ 53.500',
            'R$ 55.000'
        ],
        'Total': [
            'R$ 155.000',
            'R$ 157.000',
            'R$ 153.500',
            'R$ 156.500',
            'R$ 165.000'
        ],
        'Variação Total': [
            '+3.3%',
            '+4.7%',
            '+2.3%',
            '+4.3%',
            '+10.0%'
        ]
    })
    
    st.dataframe(df_comp, use_container_width=True)

# ===== WATERFALL ANALYSIS =====
elif secao == "🌊 Waterfall Analysis":
    st.header("🌊 Waterfall Analysis - Análise de Variação")
    
    st.markdown("""
    ## O que é Waterfall Analysis?
    
    A Waterfall Analysis é uma ferramenta para comparar custos entre dois períodos e identificar
    as causas das variações, separando os efeitos de volume, sensibilidade e inflação.
    """)
    
    st.markdown("---")
    
    # Modos de comparação
    st.subheader("📊 Modos de Comparação")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📅 Mês a Mês
        
        Compara dois meses específicos:
        - Exemplo: Janeiro 2024 vs Fevereiro 2024
        - Mostra variação entre os meses
        - Inclui barras FLEX Volume e FLEX Inflação
        """)
    
    with col2:
        st.markdown("""
        ### 📆 Ano a Ano
        
        Compara dois anos completos:
        - Exemplo: 2024 vs 2025
        - Usa volume TOTAL de cada ano
        - Inclui barras FLEX Volume e FLEX Inflação
        - **Correção implementada:** Agora usa volumes totais anuais corretamente
        """)
    
    with col3:
        st.markdown("""
        ### 📋 Múltiplos Meses
        
        Compara o primeiro e último mês de uma série:
        - Exemplo: Janeiro → Março → Maio → Julho
        - Mostra todos os meses intermediários
        - Útil para análises de tendência
        """)
    
    st.markdown("---")
    
    # Cálculo FLEX
    st.subheader("🔧 Cálculo FLEX (Volume + Inflação)")
    
    st.markdown("""
    O FLEX é calculado separando dois efeitos:
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 📈 FLEX Volume
        
        Representa o efeito da variação de volume + sensibilidade:
        
        **Fórmula:**
        ```
        Volume_Inicial = Volume do período inicial
        Volume_Final = Volume do período final
        Proporção = Volume_Final / Volume_Inicial
        Variação % = Proporção - 1.0
        
        Para cada tipo de custo:
        Variação_Ajustada = Variação % × Sensibilidade
        Fator = 1.0 + Variação_Ajustada
        Custo_Após_Volume = Custo_Inicial × Fator
        FLEX_Volume = Custo_Após_Volume - Custo_Inicial
        ```
        
        **Importante para Ano a Ano:**
        - Usa volume TOTAL do ano inicial
        - Usa volume TOTAL do ano final
        - Não usa meses específicos (correção implementada)
        """)
    
    with col2:
        st.markdown("""
        ### 💰 FLEX Inflação
        
        Representa o efeito da inflação aplicada:
        
        **Fórmula:**
        ```
        Fator_Inflação = 1.0 + (Inflação / 100.0)
        Custo_Final = Custo_Após_Volume × Fator_Inflação
        FLEX_Inflação = Custo_Final - Custo_Após_Volume
        ```
        
        **Características:**
        - Aplicado após o efeito de volume
        - Pode ser global ou detalhado por categoria
        - Sempre aparece quando inflação > 0%
        """)
    
    st.markdown("---")
    
    # Exemplo prático
    st.subheader("💡 Exemplo Prático: Ano a Ano")
    
    st.markdown("""
    **Cenário:** Comparar 2024 vs 2025
    
    **Dados:**
    - Volume Total 2024: 12.000 unidades
    - Volume Total 2025: 12.000 unidades (igual!)
    - Custo Total 2024: R$ 1.200.000
      - Fixo: R$ 480.000
      - Variável: R$ 720.000
    - Sensibilidade: Fixo = 0%, Variável = 100%
    - Inflação: 5%
    """)
    
    st.markdown("**Cálculo FLEX Volume:**")
    st.code("""
    Volume Inicial = 12.000
    Volume Final = 12.000
    Proporção = 12.000 / 12.000 = 1.0
    Variação % = 1.0 - 1.0 = 0% (zero!)
    
    Variação Ajustada Fixo = 0% × 0% = 0%
    Variação Ajustada Variável = 0% × 100% = 0%
    
    Custo Após Volume = 480.000 × 1.0 + 720.000 × 1.0 = R$ 1.200.000
    FLEX Volume = 1.200.000 - 1.200.000 = R$ 0,00 ✅
    """)
    
    st.markdown("**Cálculo FLEX Inflação:**")
    st.code("""
    Fator Inflação = 1.0 + 5% = 1.05
    Custo Final = 1.200.000 × 1.05 = R$ 1.260.000
    FLEX Inflação = 1.260.000 - 1.200.000 = R$ 60.000 ✅
    """)
    
    st.success("""
    ✅ **Resultado:** Como os volumes são iguais, FLEX Volume = R$ 0.
    Apenas a inflação de 5% é aplicada, resultando em FLEX Inflação = R$ 60.000.
    """)
    
    st.markdown("---")
    
    # Modos de configuração
    st.subheader("🎛️ Modos de Configuração")
    
    st.markdown("""
    ### 1. Modo Global
    
    Sensibilidade e inflação aplicadas globalmente:
    - Uma sensibilidade para custos fixos
    - Uma sensibilidade para custos variáveis
    - Uma inflação para todos os custos
    
    **Quando usar:**
    - Análises rápidas e simplificadas
    - Quando todos os custos têm comportamento similar
    
    ### 2. Modo Detalhado
    
    Sensibilidade e inflação configuradas por categoria:
    - Sensibilidade específica para cada categoria (fixo e variável)
    - Inflação específica para cada categoria
    - Configuração linha a linha
    
    **Quando usar:**
    - Análises precisas e detalhadas
    - Quando diferentes categorias têm comportamentos distintos
    - Quando há informações específicas por tipo de custo
    """)
    
    st.markdown("---")
    
    # Correção implementada
    st.subheader("🔧 Correção Implementada: Ano a Ano")
    
    st.warning("""
    **Problema Identificado e Corrigido:**
    
    Anteriormente, no modo "Ano a Ano", o sistema estava:
    - Usando o primeiro mês do ano inicial (ex: Janeiro 2024)
    - Usando o último mês do ano final (ex: Dezembro 2025)
    - Comparando volumes desses meses específicos
    
    **Problema:** Se o volume total de 2024 = volume total de 2025, mas Janeiro 2024 ≠ Dezembro 2025,
    o FLEX Volume apareceria incorretamente!
    
    **Solução Implementada:**
    - Agora usa volume TOTAL do ano inicial (soma de todos os meses de 2024)
    - Agora usa volume TOTAL do ano final (soma de todos os meses de 2025)
    - Compara volumes totais anuais corretamente
    
    **Resultado:** O cálculo agora está matematicamente correto! ✅
    """)
    
    st.markdown("---")
    
    # Dicas
    st.subheader("💡 Dicas Importantes")
    
    st.info("""
    1. **Seleção de Períodos:**
       - Para "Ano a Ano", selecione anos diferentes
       - Para "Mês a Mês", selecione meses diferentes
       - Para "Múltiplos Meses", selecione 3 ou mais meses
    
    2. **Interpretação das Barras:**
       - Barras verdes = Aumento
       - Barras vermelhas = Redução
       - FLEX Volume = Efeito de volume + sensibilidade
       - FLEX Inflação = Efeito da inflação
    
    3. **Validação:**
       - Verifique se os totais fazem sentido
       - Compare com os dados originais
       - Use o modo detalhado para análises precisas
    
    4. **Performance:**
       - Modo global é mais rápido
       - Modo detalhado é mais preciso mas mais lento
       - Use filtros para reduzir o volume de dados
    """)

# ===== CONFIGURAÇÃO DE DADOS =====
elif secao == "🔧 Configuração de Dados":
    st.header("🔧 Configuração de Dados")
    
    st.markdown("""
    ## Arquivos Necessários
    
    O sistema utiliza dois arquivos principais em formato Parquet:
    """)
    
    # Arquivo 1
    st.subheader("1️⃣ df_final.parquet")
    
    st.markdown("""
    **Descrição:** Contém os dados históricos de custos.
    
    **Colunas obrigatórias:**
    """)
    
    df_estrutura1 = pd.DataFrame({
        'Coluna': ['Oficina', 'Veículo', 'Período', 'Total', 'Custo', 'Type 05', 'Type 06', 'Account'],
        'Tipo': ['texto', 'texto', 'texto', 'numérico', 'texto', 'texto', 'texto', 'texto'],
        'Descrição': [
            'Nome da oficina',
            'Identificação do veículo',
            'Mês de referência (ex: Janeiro, Fevereiro)',
            'Valor total do custo',
            'Tipo de custo (Fixo ou Variável)',
            'Classificação adicional',
            'Classificação adicional',
            'Conta contábil'
        ]
    })
    
    st.dataframe(df_estrutura1, use_container_width=True)
    
    st.code("""
Exemplo de dados:
Oficina    | Veículo | Período   | Total    | Custo    | Type 05 | Type 06 | Account
-----------|---------|-----------|----------|----------|---------|---------|--------
Oficina A  | V001    | Janeiro   | 50000.00 | Fixo     | T5-001  | T6-001  | ACC-001
Oficina A  | V001    | Janeiro   | 30000.00 | Variável | T5-002  | T6-002  | ACC-002
    """)
    
    st.markdown("---")
    
    # Arquivo 2
    st.subheader("2️⃣ df_vol.parquet")
    
    st.markdown("""
    **Descrição:** Contém os dados de volume (histórico e futuro).
    
    **Colunas obrigatórias:**
    """)
    
    df_estrutura2 = pd.DataFrame({
        'Coluna': ['Oficina', 'Veículo', 'Período', 'Volume'],
        'Tipo': ['texto', 'texto', 'texto', 'numérico'],
        'Descrição': [
            'Nome da oficina (deve corresponder ao df_final)',
            'Identificação do veículo (deve corresponder ao df_final)',
            'Mês de referência (incluindo meses futuros)',
            'Quantidade de volume produzido/previsto'
        ]
    })
    
    st.dataframe(df_estrutura2, use_container_width=True)
    
    st.code("""
Exemplo de dados:
Oficina    | Veículo | Período   | Volume
-----------|---------|-----------|--------
Oficina A  | V001    | Janeiro   | 1000
Oficina A  | V001    | Fevereiro | 1050
Oficina A  | V001    | Novembro  | 1200  ← Futuro
Oficina A  | V001    | Dezembro  | 1150  ← Futuro
    """)
    
    st.markdown("---")
    
    # Dicas importantes
    st.subheader("⚠️ Pontos de Atenção")
    
    st.warning("""
    **Importante:**
    
    1. **Correspondência de Chaves:**
       - Os valores de `Oficina` e `Veículo` devem ser idênticos nos dois arquivos
       - Diferenças de maiúsculas/minúsculas ou espaços podem causar problemas
    
    2. **Formato de Período:**
       - Use nomes de meses consistentes (ex: "Janeiro", "Fevereiro", etc.)
       - Mantenha o mesmo formato em ambos os arquivos
    
    3. **Valores Numéricos:**
       - `Total` e `Volume` devem ser números (não texto)
       - Valores nulos ou zero podem afetar os cálculos
    
    4. **Tipo de Custo:**
       - Valores aceitos: "Fixo" ou "Variável"
       - Outros valores serão tratados como "Variável"
    """)
    
    st.markdown("---")
    
    # Localização dos arquivos
    st.subheader("📁 Localização dos Arquivos")
    
    st.info("""
    Os arquivos devem estar na pasta raiz do projeto:
    
    ```
    C:\\GIT\\TC\\
    ├── df_final.parquet
    ├── df_vol.parquet
    ├── app.py
    └── pages\\
        ├── 1 - TC_Ext.py
        ├── 2 - Forecast.py
        └── 3 - Documentacao.py
    ```
    """)

# ===== PERGUNTAS FREQUENTES =====
elif secao == "❓ Perguntas Frequentes":
    st.header("❓ Perguntas Frequentes")
    
    # FAQ 1
    with st.expander("❓ Por que o forecast não está variando mesmo alterando a sensibilidade?"):
        st.markdown("""
        **Possíveis causas:**
        
        1. **Dados de volume não disponíveis:**
           - Verifique se o arquivo `df_vol.parquet` existe
           - Confirme se há dados de volume para os meses futuros
        
        2. **Volume futuro igual ao histórico:**
           - Se o volume futuro for igual ao médio histórico, não haverá variação
           - Proporção = 1.0 → Nenhuma mudança no custo
        
        3. **Cache ativo:**
           - Clique no botão "🔄 Atualizar Dados" na barra lateral
           - Isso limpará o cache e recalculará com os novos valores
        
        **Solução:** Verifique os dados e atualize o cache.
        """)
    
    # FAQ 2
    with st.expander("❓ Como interpretar a sensibilidade de 0.5?"):
        st.markdown("""
        **Sensibilidade = 0.5 significa:**
        
        O custo variará **50% da variação do volume**.
        
        **Exemplo:**
        - Volume aumenta 20%
        - Sensibilidade = 0.5
        - Custo aumentará: 20% × 0.5 = **10%**
        
        **Casos de uso:**
        - Custos semi-fixos (variam parcialmente)
        - Custos com economias de escala
        - Custos com contratos de volume
        """)
    
    # FAQ 3
    with st.expander("❓ Qual a diferença entre Custo Fixo e Variável?"):
        st.markdown("""
        **Custo Fixo:**
        - Não varia com o volume de produção (sensibilidade padrão = 0.0)
        - Exemplos: Aluguel, salários administrativos, seguros
        - Permanece constante independente da produção
        
        **Custo Variável:**
        - Varia proporcionalmente ao volume (sensibilidade padrão = 1.0)
        - Exemplos: Matéria-prima, mão de obra direta, energia
        - Aumenta/diminui conforme a produção
        
        **No sistema:**
        - A classificação vem da coluna `Custo` no arquivo de dados
        - Você pode ajustar a sensibilidade de ambos os tipos
        """)
    
    # FAQ 4
    with st.expander("❓ Como funciona o cache? Quando devo atualizá-lo?"):
        st.markdown("""
        **O que é cache:**
        - Armazena resultados de cálculos pesados
        - Evita recalcular os mesmos dados repetidamente
        - Melhora significativamente a performance
        
        **Quando atualizar:**
        1. Após alterar os arquivos de dados (.parquet)
        2. Quando os resultados parecem desatualizados
        3. Após mudar filtros ou sensibilidades
        
        **Como atualizar:**
        - Clique no botão "🔄 Atualizar Dados" na barra lateral
        - O sistema recalculará tudo com os dados mais recentes
        
        **TTL (Time To Live):**
        - Cache expira automaticamente após 1 hora (3600 segundos)
        """)
    
    # FAQ 5
    with st.expander("❓ Por que alguns valores aparecem como zero na tabela?"):
        st.markdown("""
        **Possíveis razões:**
        
        1. **Dados históricos zerados:**
           - Se a média histórica for zero, o forecast também será zero
           - Verifique os dados de origem
        
        2. **Filtros aplicados:**
           - Linhas com valores zero são automaticamente removidas
           - Isso mantém a tabela mais limpa e focada
        
        3. **Volume zero:**
           - Se o volume futuro for zero, custos variáveis serão zero
           - Custos fixos permanecerão (dependendo da sensibilidade)
        
        **Solução:** Revise os dados de entrada e filtros aplicados.
        """)
    
    # FAQ 6
    with st.expander("❓ Como fazer download dos dados do forecast?"):
        st.markdown("""
        **Passo a passo:**
        
        1. Navegue até a página **"2 - Forecast"**
        2. Role até a seção **"📋 Tabela - Forecast por Veículo, Oficina e Período"**
        3. Clique no botão **"📥 Download Tabela (Excel)"**
        4. O arquivo será salvo na sua pasta Downloads
        
        **Formato do arquivo:**
        - Excel (.xlsx)
        - Valores numéricos sem formatação
        - Todas as colunas incluídas
        - Nome: `Forecast_tabela_YYYYMMDD_HHMMSS.xlsx`
        
        **Dica:** O arquivo pode ser aberto no Excel para análises adicionais.
        """)
    
    # FAQ 7
    with st.expander("❓ Posso usar sensibilidades diferentes para cada veículo/oficina?"):
        st.markdown("""
        **Atualmente:**
        - As sensibilidades são globais (aplicadas a todos)
        - Fixo: uma sensibilidade para todos os custos fixos
        - Variável: uma sensibilidade para todos os custos variáveis
        
        **Futura implementação:**
        - Sensibilidades por oficina
        - Sensibilidades por veículo
        - Sensibilidades por conta (Account)
        
        **Workaround atual:**
        - Execute o forecast múltiplas vezes com filtros diferentes
        - Baixe os resultados separadamente
        - Combine manualmente no Excel
        """)
    
    # FAQ 8
    with st.expander("❓ Por que a média histórica no gráfico não corresponde à média acumulada?"):
        st.markdown("""
        **Causa comum:**
        
        A média histórica pode estar incorreta se períodos de anos diferentes estiverem sendo somados.
        Por exemplo, se "Julho 2024" e "Julho 2025" estiverem sendo tratados como o mesmo período.
        
        **Solução:**
        
        O sistema já implementa uma lógica padronizada que:
        1. Normaliza períodos com o ano de referência
        2. Filtra apenas períodos do ano correto
        3. Trata cada período (mês + ano) como único
        
        **Verificação:**
        - A média histórica no gráfico principal deve ser igual à última média acumulada do gráfico detalhado
        - Se houver diferença, verifique se os períodos estão com o ano correto
        - Limpe o cache e recarregue os dados
        """)
    
    # FAQ 9
    with st.expander("❓ Como funciona a exclusão de meses do cálculo?"):
        st.markdown("""
        **Funcionalidade:**
        
        Você pode marcar meses para serem excluídos do cálculo da média histórica.
        Isso é útil quando um mês teve valores atípicos ou não representa o padrão normal.
        
        **O que acontece:**
        1. O mês marcado é removido do cálculo da média histórica
        2. O mês também é removido do cálculo do volume médio histórico
        3. O mês não aparece nos gráficos de meses individuais
        4. A média é recalculada sem o mês excluído
        
        **Importante:**
        - A exclusão afeta TODOS os cálculos (custos, volumes, forecast)
        - A média histórica será recalculada automaticamente
        - Os gráficos serão atualizados para refletir a nova média
        """)
    
    # FAQ 10
    with st.expander("❓ O que fazer se o gráfico não aparecer?"):
        st.markdown("""
        **Verificações:**
        
        1. **Dados disponíveis:**
           - Verifique se há dados na seção de debug
           - Confirme se os valores de Custo e Volume são > 0
        
        2. **Navegador:**
           - Tente atualizar a página (F5)
           - Limpe o cache do navegador
           - Teste em outro navegador
        
        3. **Altair/Vega:**
           - Verifique se há erros no console do navegador (F12)
           - Alguns navegadores podem bloquear visualizações
        
        4. **Dados muito grandes:**
           - Aplique filtros para reduzir o volume de dados
           - Gráficos com muitos pontos podem não renderizar
        
        **Solução rápida:** Use os filtros da sidebar para reduzir os dados.
        """)
    
    # FAQ 11 - Waterfall
    with st.expander("❓ Por que o FLEX Volume aparece mesmo quando os volumes são iguais?"):
        st.markdown("""
        **Causa:**
        
        No modo "Ano a Ano", se o sistema estiver usando meses específicos em vez de volumes totais,
        pode haver diferença mesmo quando os totais anuais são iguais.
        
        **Solução:**
        
        O sistema foi corrigido para usar volumes totais anuais. Agora:
        - Se Volume Total 2024 = Volume Total 2025 → FLEX Volume = R$ 0 ✅
        - Se Volume Total 2024 ≠ Volume Total 2025 → FLEX Volume reflete a diferença correta ✅
        
        **Verificação:**
        - Confirme que está usando a versão mais recente
        - Verifique os volumes totais de cada ano
        - Se ainda houver problema, limpe o cache e recarregue
        """)
    
    # FAQ 12 - Waterfall
    with st.expander("❓ Qual a diferença entre Mês a Mês e Ano a Ano no Waterfall?"):
        st.markdown("""
        **Mês a Mês:**
        - Compara dois meses específicos (ex: Janeiro 2024 vs Fevereiro 2024)
        - Usa volume do mês específico
        - Útil para análises de variação mensal
        
        **Ano a Ano:**
        - Compara dois anos completos (ex: 2024 vs 2025)
        - Usa volume TOTAL de cada ano (soma de todos os meses)
        - Útil para análises de variação anual
        - **Correção:** Agora usa volumes totais corretamente
        
        **Quando usar:**
        - Mês a Mês: Para entender variações sazonais ou mensais
        - Ano a Ano: Para comparar performance anual completa
        """)

# Rodapé
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação do Sistema de Forecast | Versão 1.2 | Dezembro 2024
    <br>
    <small>Atualizado com: Lógica padronizada de médias históricas | Waterfall Analysis | Correção FLEX Ano a Ano</small>
</div>
""", unsafe_allow_html=True)

