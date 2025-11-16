import streamlit as st
import pandas as pd

# Configuração da página
st.set_page_config(
    page_title="Documentação - Forecast",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Documentação - Sistema de Forecast")

# Menu lateral
st.sidebar.title("📑 Navegação")
secao = st.sidebar.radio(
    "Selecione uma seção:",
    [
        "🏠 Visão Geral",
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
    ## O que é o Sistema de Forecast?
    
    O Sistema de Forecast é uma ferramenta desenvolvida para prever custos totais até o final do ano,
    considerando a variação de volume de produção e a sensibilidade de cada tipo de custo.
    
    ### Principais Funcionalidades:
    
    1. **📈 Previsão de Custos**
       - Calcula o forecast de custos fixos e variáveis
       - Considera volumes futuros de produção
       - Aplica sensibilidade configurável
    
    2. **🎚️ Análise de Sensibilidade**
       - Ajuste independente para custos fixos e variáveis
       - Varia de 0 (sem variação) a 1 (variação total)
       - Permite simulações de cenários
    
    3. **📈 Ajuste de Inflação**
       - Inflação aplicada uma única vez no primeiro mês
       - Configuração global ou por Type 06
       - Valores mantidos nos meses seguintes
    
    4. **📊 Visualizações**
       - Gráficos de premissas (custo e volume)
       - Tabelas detalhadas por veículo e oficina
       - Agrupamento e download de dados
    
    4. **🔄 Cache Inteligente**
       - Cálculos otimizados
       - Atualização sob demanda
       - Performance melhorada
    """)
    
    st.info("""
    💡 **Dica:** Navegue pelas seções no menu lateral para entender melhor cada funcionalidade.
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
    calculamos a média mensal dos custos históricos.
    
    **Fórmula:**
    ```
    Média_Mensal_Histórica = Soma(Custos_Históricos) / Número_de_Meses
    ```
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
    Calculamos o volume médio de produção histórico para cada **Oficina** e **Veículo**.
    
    **Fórmula:**
    ```
    Volume_Médio_Histórico = Soma(Volumes_Históricos) / Número_de_Meses
    ```
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
    
    **Fórmula:**
    ```
    Forecast = Média_Mensal_Histórica × Proporção_Ajustada
    ```
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

# Rodapé
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação do Sistema de Forecast | Versão 1.0 | Novembro 2024
</div>
""", unsafe_allow_html=True)

