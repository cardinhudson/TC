import streamlit as st
import pandas as pd

# Configuração da página
st.set_page_config(
    page_title="Documentação - Sistema TC",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS para melhorar visualização
st.markdown("""
    <style>
        h1 {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 10px 20px;
            font-weight: 600;
        }
    </style>
""", unsafe_allow_html=True)

st.title("📚 Documentação Completa do Sistema TC")

# Sidebar com índices
st.sidebar.markdown("## 📑 Índice")
st.sidebar.markdown("---")

# Criar dois índices no sidebar
indice_selecionado = st.sidebar.radio(
    "Selecione a seção:",
    ["📐 Regras e Cálculo", "🏗️ Arquitetura e Estrutura"],
    key="indice_documentacao"
)

st.markdown("---")

# ==========================================
# SEÇÃO 1: REGRAS E CÁLCULO
# ==========================================
if indice_selecionado == "📐 Regras e Cálculo":
    st.header("📐 Regras e Cálculo")
    
    st.markdown("""
    Esta seção documenta todas as regras de cálculo, filtros e metodologias utilizadas no projeto.
    **IMPORTANTE:** Esta documentação serve como referência para garantir que todos os cálculos sejam
    reproduzidos de forma idêntica, permitindo que a IA consulte e refaça qualquer cálculo do sistema.
    """)
    
    st.markdown("---")
    
    # Sub-tabs para organização
    tab_calculos, tab_filtros, tab_moeda, tab_volumes, tab_flex = st.tabs([
        "🔢 Cálculos Principais",
        "🔍 Filtros e Perímetros",
        "💱 Moeda e Taxas",
        "📊 Volumes",
        "🔄 Flex Bud"
    ])
    
    # TAB 1: Cálculos Principais
    with tab_calculos:
        st.subheader("🔢 Cálculos Principais")
        
        st.markdown("""
        ### 1. CPU (Custo por Unidade)
        
        **REGRA CRÍTICA:** CPU deve ser calculado APÓS agrupamento, nunca antes.
        
        **Fórmula:**
        ```
        CPU = Total_Agregado / Volume_Agregado
        ```
        
        **Implementação Correta:**
        ```python
        # ✅ CORRETO: Agrupar primeiro, depois calcular CPU
        df_agrupado = df.groupby(['Período', 'Oficina']).agg({
            'Total': 'sum',
            'Volume': 'sum'
        }).reset_index()
        df_agrupado['CPU'] = df_agrupado['Total'] / df_agrupado['Volume'].replace(0, 1)
        ```
        
        **Implementação Incorreta (NUNCA FAZER):**
        ```python
        # ❌ INCORRETO: Calcular CPU antes de agrupar
        df['CPU'] = df['Total'] / df['Volume']
        df_agrupado = df.groupby(['Período', 'Oficina'])['CPU'].mean()  # ❌ ERRADO!
        ```
        
        **Motivo:** A média de CPUs individuais não é igual ao CPU do total agregado.
        
        **Exemplo:**
        - Linha 1: Total = 100, Volume = 10 → CPU = 10
        - Linha 2: Total = 200, Volume = 20 → CPU = 10
        - Total Agregado: Total = 300, Volume = 30 → CPU = 10 ✅
        - Média de CPUs: (10 + 10) / 2 = 10 ✅ (coincidência)
        
        Mas se:
        - Linha 1: Total = 100, Volume = 10 → CPU = 10
        - Linha 2: Total = 200, Volume = 40 → CPU = 5
        - Total Agregado: Total = 300, Volume = 50 → CPU = 6 ✅
        - Média de CPUs: (10 + 5) / 2 = 7.5 ❌ (ERRADO!)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 2. Custo Total
        
        **Fórmula:**
        ```
        Custo_Total = Soma(Total_Individual)
        ```
        
        **Implementação:**
        ```python
        # Agrupar e somar
        df_agrupado = df.groupby(['Período', 'Oficina'])['Total'].sum().reset_index()
        ```
        
        **Observações:**
        - Sempre somar valores individuais
        - Não calcular média de totais
        - Aplicar filtros antes de agrupar
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 3. Fator de Conversão (K/M)
        
        **REGRA CRÍTICA:** Fator de conversão NÃO deve ser aplicado no modo CPU.
        
        **Aplicação:**
        ```python
        # Aplicar fator APENAS em modo Custo Total
        if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
            if fator_conversao == "K (milhares)":
                df['Total'] = df['Total'] / 1000
            elif fator_conversao == "M (Milhões)":
                df['Total'] = df['Total'] / 1000000
        ```
        
        **Motivo:** CPU já é uma razão (Total/Volume), aplicar fator resultaria em divisão dupla.
        
        **Ordem de Aplicação:**
        1. Primeiro: Fator de conversão (K/M)
        2. Depois: Conversão de moeda
        3. Depois: Cálculos (CPU, Flex Bud, etc.)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 4. Agrupamento por Período
        
        **Com Ano:**
        ```python
        # Criar coluna Período_Ano
        if 'Ano' in df.columns and 'Período' in df.columns:
            df['Período_Ano'] = df['Período'].astype(str) + ' ' + df['Ano'].astype(str)
            colunas_agrupamento = ['Ano', 'Período']
        ```
        
        **Sem Ano:**
        ```python
        colunas_agrupamento = ['Período']
        ```
        
        **Agrupamento:**
        ```python
        df_agrupado = df.groupby(colunas_agrupamento).agg({
            'Total': 'sum',
            'Volume': 'sum'
        }).reset_index()
        ```
        """)
    
        st.markdown("---")
        
        st.markdown("""
        ### 5. Cálculo de Diferenças
        
        **Flex Bud - BUD:**
        ```
        Flex_Bud_Menos_BUD = Flex_BUD - BUD
        ```
        
        **Total - Flex Bud:**
        ```
        Total_Menos_Flex_Bud = Total - Flex_BUD
        ```
        
        **Total / Flex Bud (Ratio):**
        ```
        Ratio = Total / Flex_BUD
        Percentual = Ratio × 100
        ```
        
        **Interpretação do Ratio:**
        - < 100%: Total < Flex Bud (melhor que esperado)
        - = 100%: Total = Flex Bud (exatamente como esperado)
        - > 100%: Total > Flex Bud (pior que esperado)
        """)
    
    # TAB 2: Filtros e Perímetros
    with tab_filtros:
        st.subheader("🔍 Filtros e Perímetros")
        
        st.markdown("""
        ### Filtros da Sidebar (Ordem de Aplicação)
        
        Os filtros são aplicados na seguinte ordem:
        
        1. **Ano** (Radio button)
        2. **Oficina** (Multiselect)
        3. **Veículo** (Multiselect)
        4. **USI** (Multiselect)
        5. **Período** (Selectbox)
        6. **Centro cst** (Selectbox)
        7. **Conta contábil** (Multiselect)
        8. **Type 05** (Multiselect)
        9. **Type 06** (Multiselect)
        10. **Fornecedor** (Multiselect)
        11. **Fornec.** (Multiselect)
        12. **Tipo** (Multiselect)
        13. **Filtros Avançados:**
            - Usuário
            - Material
            - Dt.lçto.
            - Texto breve
            - Account
        
        **Implementação:**
                ```python
        # Aplicar filtros sequencialmente
        df_filtrado = df_total.copy()
        
        # Filtro de Oficina
        if oficinas_selecionadas and "Todos" not in oficinas_selecionadas:
            df_filtrado = df_filtrado[
                df_filtrado['Oficina'].astype(str).isin(oficinas_selecionadas)
            ].copy()
        
        # Filtro de Veículo
        if veiculos_selecionados and "Todos" not in veiculos_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['Veículo'].astype(str).isin(veiculos_selecionados)
            ].copy()
        
        # ... (aplicar todos os outros filtros)
                ```
                """)
    
        st.markdown("---")
        
        st.markdown("""
        ### Perímetro de Filtros para Volumes
        
        **REGRA CRÍTICA:** Volumes devem usar EXATAMENTE os mesmos filtros dos dados principais.
        
        **Implementação:**
        ```python
        # Criar df_vol_filtrado aplicando TODOS os filtros
        df_vol_filtrado = df_volume.copy()
        
        # Aplicar filtros usando valores únicos de df_filtrado_waterfall
        if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
            # Filtro de Veículo
            if 'Veículo' in df_filtrado_waterfall.columns and 'Veículo' in df_vol_filtrado.columns:
                veiculos_filtrados = df_filtrado_waterfall['Veículo'].dropna().unique()
                if len(veiculos_filtrados) > 0:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Veículo'].isin(veiculos_filtrados)
                    ].copy()
            
            # Filtro de Oficina
            if 'Oficina' in df_filtrado_waterfall.columns and 'Oficina' in df_vol_filtrado.columns:
                oficinas_filtradas = df_filtrado_waterfall['Oficina'].dropna().unique()
                if len(oficinas_filtradas) > 0:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['Oficina'].isin(oficinas_filtradas)
                    ].copy()
            
            # Filtro de USI
            if 'USI' in df_filtrado_waterfall.columns and 'USI' in df_vol_filtrado.columns:
                usi_filtradas = df_filtrado_waterfall['USI'].dropna().unique()
                if len(usi_filtradas) > 0:
                    df_vol_filtrado = df_vol_filtrado[
                        df_vol_filtrado['USI'].isin(usi_filtradas)
                    ].copy()
            
            # Outros filtros comuns
            colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 
                                     'Fornecedor', 'Fornec.', 'Tipo']
            for col_filtro in colunas_filtro_comuns:
                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_vol_filtrado.columns:
                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                    if len(valores_filtrados) > 0:
                        df_vol_filtrado = df_vol_filtrado[
                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                        ].copy()
            
            # Filtros avançados
            colunas_filtro_avancados = ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve', 'Account']
            for col_filtro in colunas_filtro_avancados:
                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_vol_filtrado.columns:
                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                    if len(valores_filtrados) > 0:
                        df_vol_filtrado = df_vol_filtrado[
                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                        ].copy()
        ```
        
        **IMPORTANTE:** Sempre usar valores únicos de `df_filtrado_waterfall` (ou `df_filtrado`) para aplicar filtros ao volume.
        Isso garante que volumes e valores totais usem o mesmo perímetro de filtros.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Filtros para Budget
        
        **REGRA:** Budget deve usar os mesmos filtros dos dados reais.
        
        **Implementação:**
        ```python
        # Aplicar TODOS os filtros que existem em df_para_grafico_periodo
        if 'df_para_grafico_periodo' in locals() and df_para_grafico_periodo is not None:
            # Aplicar filtro de Veículo
            if 'Veículo' in df_para_grafico_periodo.columns and 'Veículo' in df_budget.columns:
                veiculos_filtrados = df_para_grafico_periodo['Veículo'].dropna().unique()
                if len(veiculos_filtrados) > 0:
                    df_budget = df_budget[df_budget['Veículo'].isin(veiculos_filtrados)].copy()
            
            # Aplicar filtro de Oficina
            if 'Oficina' in df_para_grafico_periodo.columns and 'Oficina' in df_budget.columns:
                oficinas_filtradas = df_para_grafico_periodo['Oficina'].dropna().unique()
                if len(oficinas_filtradas) > 0:
                    df_budget = df_budget[df_budget['Oficina'].isin(oficinas_filtradas)].copy()
            
            # ... (aplicar todos os outros filtros)
        ```
        """)
    
    # TAB 3: Moeda e Taxas
    with tab_moeda:
        st.subheader("💱 Moeda e Taxas de Câmbio")
        
        st.markdown("""
        ### Moedas Suportadas
        
        - **BRL (R$):** Real Brasileiro (moeda base)
        - **USD ($):** Dólar Americano
        - **EUR (€):** Euro
        
        ### Taxas de Câmbio
        
        **Definição:**
        - Taxas são definidas como: `1 BRL = X USD` e `1 BRL = Y EUR`
        - Exemplo: Se 1 BRL = 0.20 USD, então taxa_usd_para_brl = 0.20
        
        **Armazenamento:**
        ```python
        taxas_cambio = {
            "BRL": 1.0,
            "USD": taxa_brl_para_usd,  # Ex: 0.20
            "EUR": taxa_brl_para_eur   # Ex: 0.18
        }
        ```
        
        **Conversão:**
        ```python
        def converter_moeda(valor, moeda_destino, taxas):
            if valor is None or pd.isna(valor):
                return valor
            if moeda_destino == "BRL":
                return valor
            taxa = taxas.get(moeda_destino, 1.0)
            return valor * taxa
        ```
        
        **Aplicação em DataFrame:**
        ```python
        def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
            if moeda_destino == "BRL":
                return df
            taxa = taxas.get(moeda_destino, 1.0)
            df[coluna] = df[coluna] * taxa
            return df
        ```
        
        **Ordem de Aplicação:**
        1. Primeiro: Fator de conversão (K/M) - se aplicável
        2. Depois: Conversão de moeda
        3. Depois: Cálculos (CPU, Flex Bud, etc.)
        
        **Exemplo:**
        - Valor original: R$ 1.000.000
        - Com fator K: R$ 1.000 K
        - Com conversão USD (taxa 0.20): $ 200 K
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Persistência de Taxas
        
        **Armazenamento:**
        - Taxas são salvas em banco de dados ou arquivo JSON
        - Valores padrão se não houver taxas salvas
        
        **Atualização:**
        ```python
        # Salvar novas taxas
        def salvar_taxas_banco(novas_taxas):
            # Salvar em banco de dados ou arquivo
            pass
        ```
        """)
    
    # TAB 4: Volumes
    with tab_volumes:
        st.subheader("📊 Cálculo de Volumes")
        
        st.markdown("""
        ### Fonte de Dados de Volume
        
        **Arquivo:**
        - `df_volume` ou `df_vol_historico.parquet`
        - Colunas obrigatórias: `Volume`, `Período`, `Oficina`, `Veículo`
        - Coluna opcional: `Ano`
        
        ### Filtragem de Volumes
        
        **REGRA CRÍTICA:** Volumes devem usar EXATAMENTE os mesmos filtros dos dados principais.
        
        **Processo:**
        1. Criar `df_vol_filtrado` a partir de `df_volume`
        2. Aplicar TODOS os filtros usando valores únicos de `df_filtrado`
        3. Filtrar por período específico
        4. Agrupar por Período e somar
        
        **Implementação:**
        ```python
        # 1. Criar df_vol_filtrado com todos os filtros
        df_vol_filtrado = df_volume.copy()
        # ... (aplicar todos os filtros como mostrado na seção de Filtros)
        
        # 2. Filtrar por período
        if modo_comparacao == "Mês a Mês":
            if col_mes_waterfall:
                df_vol_m1 = df_vol_filtrado[
                    df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_inicial)
                ].copy()
            else:
                df_vol_m1 = df_vol_filtrado[
                    df_vol_filtrado['Período'].astype(str) == str(mes_inicial)
                ].copy()
        
        # 3. Calcular volume: agrupar por Período primeiro, depois somar
        if not df_vol_m1.empty and 'Período' in df_vol_m1.columns:
            volume_m1 = df_vol_m1.groupby('Período')['Volume'].sum().sum()
        elif not df_vol_m1.empty:
            volume_m1 = df_vol_m1['Volume'].sum()
        else:
            volume_m1 = 0
        ```
        
        **IMPORTANTE:** Sempre agrupar por Período primeiro, depois somar, para garantir consistência.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Volumes por Categoria
        
        **Para cálculo de CPU por categoria:**
        ```python
        # Agrupar volumes por categoria
        if chosen_dim_waterfall in df_vol_m1_graph.columns:
            vol_m1_por_cat = df_vol_m1_graph.groupby(chosen_dim_waterfall)['Volume'].sum()
                    else:
            # Se não tiver a coluna, usar volume total
            vol_m1_por_cat = pd.Series()
        
        # Calcular CPU por categoria
        if not vol_m1_por_cat.empty:
            total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall)['Total'].sum()
            g1 = total_m1_por_cat / vol_m1_por_cat.replace(0, 1)
        else:
            # Fallback: usar volume total
            total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall)['Total'].sum()
            g1 = total_m1_por_cat / volume_m1_graph if volume_m1_graph > 0 else total_m1_por_cat / 1
        ```
        """)
    
        st.markdown("---")
        
        st.markdown("""
        ### Volumes para Modos de Comparação
        
        **Mês a Mês:**
        - Filtrar por `col_mes_waterfall` ou `Período`
        - Agrupar por Período e somar
        
        **Ano a Ano:**
        - Filtrar por `Ano`
        - Agrupar por Ano e somar (volume total do ano)
        
        **Semestre:**
        - Filtrar por `Ano` e `Período` (meses do semestre)
        - Agrupar por Período e somar
        
        **Quarter:**
        - Filtrar por `Ano` e `Período` (meses do trimestre)
        - Agrupar por Período e somar
        """)
    
    # TAB 5: Flex Bud
    with tab_flex:
        st.subheader("🔄 Cálculo de Flex Bud")
        
        st.markdown("""
        ### Conceito
        
        **Flex Bud** (Budget Flexível) é um valor ajustado que considera a variação de volume,
        aplicando regras diferentes para custos fixos e variáveis.
        
        **IMPORTANTE:** Existem dois contextos diferentes de cálculo:
        1. **Real x Real** (Waterfall): Compara dois períodos reais (Mês 1 vs Mês 2)
        2. **Real x Budget** (TC Ext): Compara período real vs budget planejado
        """)
        
        st.markdown("---")
        
        st.markdown("## 📋 Regras Fundamentais: Fixo vs Variável")
        
        st.markdown("""
        ### Regra Geral para Custos Fixos
        
        **Princípio:** Custos fixos NÃO variam com o volume de produção.
        
        **Fórmula Geral:**
        ```
        Flex_Fixo = Valor_Original_Fixo
        ```
        
        **Explicação:**
        - Independente da variação de volume, o custo fixo permanece constante
        - Exemplos: Aluguel, salários fixos, depreciação
        - Sensibilidade ao volume: **0%** (zero por cento)
        
        **Implementação:**
        ```python
        # Sempre manter o valor original
        flex_fixo = custo_fixo_original
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Regra Geral para Custos Variáveis
        
        **Princípio:** Custos variáveis variam PROPORCIONALMENTE ao volume de produção.
        
        **Fórmula Geral:**
        ```
        Flex_Variável = Valor_Original_Variável × (Volume_Novo / Volume_Original)
        ```
        
        **Explicação:**
        - Se o volume dobra, o custo variável dobra
        - Se o volume reduz pela metade, o custo variável reduz pela metade
        - Exemplos: Matéria-prima, energia variável, comissões
        - Sensibilidade ao volume: **100%** (cem por cento)
        
        **Implementação:**
        ```python
        # Calcular proporção de volume
        proporcao = volume_novo / volume_original
        
        # Aplicar proporção ao custo variável
        flex_variavel = custo_variavel_original * proporcao
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Identificação de Fixo vs Variável
        
        **Coluna 'Custo' no DataFrame:**
        - Deve conter os valores: `'Fixo'` ou `'Variável'`
        - Cada linha de dados deve ter esta classificação
        
        **Implementação:**
        ```python
        # Separar Fixo e Variável
        if 'Custo' in df.columns:
            custo_fixo = df[df['Custo'] == 'Fixo']['Total'].sum()
            custo_variavel = df[df['Custo'] == 'Variável']['Total'].sum()
        else:
            # Se não tiver coluna Custo, assumir tudo como variável
            custo_fixo = 0
            custo_variavel = df['Total'].sum()
        ```
        """)
        
        st.markdown("---")
        
        # Sub-seções para separar os dois casos
        st.markdown("## 📊 CASO 1: Flex para Comparação Real x Real (Waterfall)")
        
        st.markdown("""
        ### Contexto
        
        Usado na página **Waterfall Analysis** para comparar dois períodos reais:
        - **Mês 1** (período inicial real)
        - **Mês 2** (período final real)
        
        **Objetivo:** Calcular o que seria o custo do Mês 1 ajustado pelo volume do Mês 2.
            """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Regras de Cálculo - Real x Real
        
        **Passo 1: Identificar Custos do Mês 1**
        ```python
        # Separar Fixo e Variável do Mês 1
        C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
        C1_Variavel = df_m1[df_m1['Custo'] == 'Variável']['Total'].sum()
        C1_Total = C1_Fixo + C1_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V1 = volume_real_mes1  # Volume do Mês 1
        V2 = volume_real_mes2  # Volume do Mês 2
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V2 / V1  # Proporção de volume
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Mes1_Fixo = C1_Fixo
        # Explicação: Mantém o valor original, independente da variação de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Fixo = C₁_Fixo
        ```
        **Por que não multiplica pela proporção?**
        - Custos fixos são independentes do volume de produção
        - Exemplos: Aluguel, salários fixos, depreciação
        - Mesmo que o volume dobre, o custo fixo permanece igual
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Mes1_Variavel = C1_Variavel * rho
                             = C1_Variavel * (V2 / V1)
        # Explicação: Multiplica pelo fator de proporção de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Variável = C₁_Variável × ρ
                              = C₁_Variável × (V₂ / V₁)
        ```
        **Por que multiplica pela proporção?**
        - Custos variáveis variam proporcionalmente ao volume
        - Se o volume dobra, o custo variável dobra
        - Se o volume reduz pela metade, o custo variável reduz pela metade
        - Exemplos: Matéria-prima, energia variável, comissões
        
        **Passo 5: Calcular Flex Mês 1 Total**
        ```python
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
                         = C1_Fixo + (C1_Variavel * rho)
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Total = Flex_Mês1_Fixo + Flex_Mês1_Variável
                        = C₁_Fixo + (C₁_Variável × ρ)
                        = C₁_Fixo + C₁_Variável × (V₂ / V₁)
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Real
        
        **Definições:**
        - `V₁` = Volume Real do Mês 1
        - `V₂` = Volume Real do Mês 2
        - `C₁_Fixo` = Custo Total Fixo do Mês 1
        - `C₁_Variável` = Custo Total Variável do Mês 1
        - `C₁_Total` = Custo Total do Mês 1 = `C₁_Fixo + C₁_Variável`
        
        **Proporção de Volume:**
        ```
        ρ = V₂ / V₁
        ```
        Onde:
        - `ρ > 1` significa que o volume aumentou
        - `ρ < 1` significa que o volume diminuiu
        - `ρ = 1` significa que o volume permaneceu igual
        
        **Cálculo de Flex Mês 1 (em Custo Total):**
        
        Para **Custo Fixo:**
        ```
        Flex_Mês1_Fixo = C₁_Fixo
        ```
        **Regra Aplicada:** Fixo não varia com volume
        - Valor original mantido: `C₁_Fixo`
        - Não multiplica pela proporção de volume
        - Motivo: Custos fixos são independentes do volume de produção
        
        Para **Custo Variável:**
        ```
        Flex_Mês1_Variável = C₁_Variável × ρ
                              = C₁_Variável × (V₂ / V₁)
        ```
        **Regra Aplicada:** Variável varia proporcionalmente ao volume
        - Valor original: `C₁_Variável`
        - Multiplica pela proporção: `ρ = V₂ / V₁`
        - Motivo: Custos variáveis aumentam/diminuem na mesma proporção do volume
        
        **Flex Mês 1 Total (em Custo Total):**
        ```
        Flex_Mês1_Total = Flex_Mês1_Fixo + Flex_Mês1_Variável
                        = C₁_Fixo + (C₁_Variável × ρ)
                        = C₁_Fixo + C₁_Variável × (V₂ / V₁)
        ```
        **Regra Aplicada:** Soma do Fixo (inalterado) + Variável (ajustado)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Cálculo em CPU (Custo por Unidade) - Real x Real
        
        **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD (Mês 1) em CPU:**
        ```
        BUD_CPU = C₁_Total / V₁
                 = (C₁_Fixo + C₁_Variável) / V₁
        ```
        
        **Flex Mês 1 em CPU:**
        ```
        Flex_Mês1_CPU = Flex_Mês1_Total / V₂
                       = [C₁_Fixo + C₁_Variável × (V₂ / V₁)] / V₂
                       = (C₁_Fixo / V₂) + (C₁_Variável / V₁)
        ```
        
        **Diferença (Flex Mês 1 - Mês 1):**
        ```
        Δ_Flex = Flex_Mês1_CPU - BUD_CPU
               = [(C₁_Fixo / V₂) + (C₁_Variável / V₁)] - [(C₁_Fixo + C₁_Variável) / V₁]
               = (C₁_Fixo / V₂) - (C₁_Fixo / V₁)
               = C₁_Fixo × (1/V₂ - 1/V₁)
               = C₁_Fixo × (V₁ - V₂) / (V₁ × V₂)
        ```
        
        **Interpretação:**
        - Se `V₂ > V₁`: `Δ_Flex < 0` (CPU diminui porque custo fixo é diluído em mais volume)
        - Se `V₂ < V₁`: `Δ_Flex > 0` (CPU aumenta porque custo fixo é concentrado em menos volume)
        - Se `V₂ = V₁`: `Δ_Flex = 0` (sem variação)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Implementação - Real x Real
        
        ```python
        # 1. Obter dados do Mês 1
        df_m1 = df_filtrado[df_filtrado['Período'] == mes_inicial]
        
        # 2. Separar Fixo e Variável
        if 'Custo' in df_m1.columns:
            C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
            C1_Variavel = df_m1[df_m1['Custo'] == 'Variável']['Total'].sum()
        else:
            C1_Fixo = 0
            C1_Variavel = df_m1['Total'].sum()  # Tudo é variável
        
        C1_Total = C1_Fixo + C1_Variavel
        
        # 3. Obter volumes
        volume_m1 = df_vol_m1['Volume'].sum()
        volume_m2 = df_vol_m2['Volume'].sum()
        
        # 4. Calcular proporção
        rho = volume_m2 / volume_m1 if volume_m1 != 0 else 1.0
        
        # 5. Calcular Flex Mês 1 (em Custo Total)
        Flex_Mes1_Fixo = C1_Fixo  # Não varia
        Flex_Mes1_Variavel = C1_Variavel * rho  # Varia proporcionalmente
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
        
        # 6. Converter para CPU (se necessário)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            BUD_CPU = C1_Total / volume_m1 if volume_m1 != 0 else 0
            Flex_Mes1_CPU = Flex_Mes1_Total / volume_m2 if volume_m2 != 0 else 0
            Delta_Flex = Flex_Mes1_CPU - BUD_CPU
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Exemplo Prático - Real x Real
        
        **Dados:**
        - Volume Real Mês 1 (`V₁`): 40,848 unidades
        - Volume Real Mês 2 (`V₂`): 60,333 unidades
        - Custo Total Fixo Mês 1 (`C₁_Fixo`): R$ 126.91
        - Custo Total Variável Mês 1 (`C₁_Variável`): R$ 755.36
        - Custo Total Mês 1 (`C₁_Total`): R$ 882.27
        
        **Cálculo:**
        ```
        ρ = V₂ / V₁ = 60,333 / 40,848 = 1.482373
        
        Flex_Mês1_Fixo = R$ 126.91
        Flex_Mês1_Variável = R$ 755.36 × 1.482373 = R$ 1,119.72
        Flex_Mês1_Total = R$ 126.91 + R$ 1,119.72 = R$ 1,246.63
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 882.27 / 40,848 = R$ 0.0216 por unidade
        Flex_Mês1_CPU = R$ 1,246.63 / 60,333 = R$ 0.0207 por unidade
        Δ_Flex = R$ 0.0207 - R$ 0.0216 = -R$ 0.0009 por unidade
        ```
        
        **Interpretação:**
        - O volume aumentou 48.24% (`ρ = 1.482373`)
        - O custo variável aumentou proporcionalmente: R$ 755.36 → R$ 1,119.72
        - O custo fixo permaneceu igual: R$ 126.91
        - Em CPU, o custo por unidade diminuiu porque o custo fixo foi diluído em mais volume
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Modos de Comparação - Real x Real
        
        **Mês a Mês:**
        - `V₁` = Volume do mês inicial
        - `V₂` = Volume do mês final
        
        **Ano a Ano:**
        - `V₁` = Volume total do ano inicial
        - `V₂` = Volume total do ano final
        
        **Semestre:**
        - `V₁` = Volume total do semestre inicial
        - `V₂` = Volume total do semestre final
        
        **Quarter:**
        - `V₁` = Volume total do trimestre inicial
        - `V₂` = Volume total do trimestre final
    """)
    
        st.markdown("---")
        
        st.markdown("## 💰 CASO 2: Flex para Comparação Real x Budget (TC Ext)")
        
        st.markdown("""
        ### Contexto
        
        Usado na página **TC Ext** para comparar período real vs budget planejado:
        - **Real** = Dados reais do período
        - **Budget** = Dados planejados do período
        
        **Objetivo:** Calcular o que seria o budget ajustado pelo volume real.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Regras de Cálculo - Real x Budget
        
        **Passo 1: Identificar Custos do Budget**
        ```python
        # Separar Fixo e Variável do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do período
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V_Real / V_Budget  # Proporção de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Bud_Fixo = B_Fixo
        # Explicação: Mantém o valor do budget, independente da variação de volume
        ```
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # Explicação: Ajusta o budget variável pela proporção de volume real vs planejado
        ```
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Budget
        
        **Definições:**
        ```python
        # Separar Fixo e Variável do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do período
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V_Real / V_Budget  # Proporção de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Bud_Fixo = B_Fixo
        # Explicação: Mantém o valor do budget, independente da variação de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Por que não multiplica pela proporção?**
        - Custos fixos são independentes do volume de produção
        - O budget fixo foi planejado e não deve ser ajustado
        - Exemplos: Aluguel, salários fixos, depreciação
        - Mesmo que o volume real seja diferente do planejado, o custo fixo permanece igual
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # Explicação: Ajusta o budget variável pela proporção de volume real vs planejado
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Variável = B_Variável × ρ
                           = B_Variável × (V_Real / V_Budget)
        ```
        **Por que multiplica pela proporção?**
        - Custos variáveis variam proporcionalmente ao volume
        - Se o volume real for maior que o planejado, o custo variável deve aumentar
        - Se o volume real for menor que o planejado, o custo variável deve diminuir
        - Exemplos: Matéria-prima, energia variável, comissões
        - O budget variável precisa ser ajustado para refletir o volume real
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                       = B_Fixo + (B_Variável × ρ)
                       = B_Fixo + B_Variável × (V_Real / V_Budget)
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Budget
        
        **Definições:**
        - `V_Real` = Volume Real do período
        - `V_Budget` = Volume Budget planejado do período
        - `B_Fixo` = Custo Total Fixo do Budget
        - `B_Variável` = Custo Total Variável do Budget
        - `B_Total` = Custo Total do Budget = `B_Fixo + B_Variável`
        - `R_Total` = Custo Total Real do período
        
        **Proporção de Volume:**
        ```
        ρ = V_Real / V_Budget
        ```
        Onde:
        - `ρ > 1` significa que o volume real foi maior que o planejado
        - `ρ < 1` significa que o volume real foi menor que o planejado
        - `ρ = 1` significa que o volume real foi exatamente o planejado
        
        **Cálculo de Flex Bud (em Custo Total):**
        
        Para **Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Regra Aplicada:** Fixo não varia com volume
        - Valor do budget mantido: `B_Fixo`
        - Não multiplica pela proporção de volume
        - Motivo: Custos fixos são independentes do volume, então mantém o valor planejado
        
        Para **Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável × ρ
                           = B_Variável × (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Variável varia proporcionalmente ao volume
        - Valor do budget: `B_Variável`
        - Multiplica pela proporção: `ρ = V_Real / V_Budget`
        - Motivo: Se o volume real for maior que o planejado, o custo variável deve aumentar proporcionalmente
        
        **Flex Bud Total (em Custo Total):**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                       = B_Fixo + (B_Variável × ρ)
                       = B_Fixo + B_Variável × (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Soma do Fixo (inalterado) + Variável (ajustado)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Cálculo em CPU (Custo por Unidade) - Real x Budget
        
        **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD em CPU:**
        ```
        BUD_CPU = B_Total / V_Budget
                 = (B_Fixo + B_Variável) / V_Budget
        ```
        
        **Flex Bud em CPU:**
        ```
        Flex_Bud_CPU = Flex_Bud_Total / V_Real
                     = [B_Fixo + B_Variável × (V_Real / V_Budget)] / V_Real
                     = (B_Fixo / V_Real) + (B_Variável / V_Budget)
        ```
        
        **Total Real em CPU:**
        ```
        Total_Real_CPU = R_Total / V_Real
        ```
        
        **Diferenças:**
        
        **Flex Bud - BUD:**
        ```
        Δ_Flex_Bud = Flex_Bud_CPU - BUD_CPU
                   = [(B_Fixo / V_Real) + (B_Variável / V_Budget)] - [(B_Fixo + B_Variável) / V_Budget]
                   = (B_Fixo / V_Real) - (B_Fixo / V_Budget)
                   = B_Fixo × (1/V_Real - 1/V_Budget)
                   = B_Fixo × (V_Budget - V_Real) / (V_Real × V_Budget)
        ```
        
        **Total - Flex Bud:**
        ```
        Δ_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
                     = (R_Total / V_Real) - [(B_Fixo / V_Real) + (B_Variável / V_Budget)]
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Implementação - Real x Budget
        
        ```python
        # 1. Obter dados de Budget
        df_budget = load_budget_data(ano_selecionado)
        
        # 2. Separar Fixo e Variável do Budget
        if 'Custo' in df_budget.columns:
            B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
            B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        else:
            B_Fixo = 0
            B_Variavel = df_budget['Total'].sum()  # Tudo é variável
        
        B_Total = B_Fixo + B_Variavel
        
        # 3. Obter volumes
        volume_real = df_vol_real['Volume'].sum()
        volume_budget = df_vol_budget['Volume'].sum()
        
        # 4. Calcular proporção
        rho = volume_real / volume_budget if volume_budget != 0 else 1.0
        
        # 5. Calcular Flex Bud (em Custo Total)
        Flex_Bud_Fixo = B_Fixo  # Não varia
        Flex_Bud_Variavel = B_Variavel * rho  # Varia proporcionalmente
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
        
        # 6. Converter para CPU (se necessário)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            BUD_CPU = B_Total / volume_budget if volume_budget != 0 else 0
            Flex_Bud_CPU = Flex_Bud_Total / volume_real if volume_real != 0 else 0
            Total_Real_CPU = df_real['Total'].sum() / volume_real if volume_real != 0 else 0
            
            Delta_Flex_Bud = Flex_Bud_CPU - BUD_CPU
            Delta_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Exemplo Prático - Real x Budget
        
        **Dados:**
        - Volume Real (`V_Real`): 50,000 unidades
        - Volume Budget (`V_Budget`): 60,000 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total Variável Budget (`B_Variável`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        - Custo Total Real (`R_Total`): R$ 550,000
        
        **Cálculo Passo a Passo:**
        
        **1. Calcular Proporção de Volume:**
        ```
        ρ = V_Real / V_Budget = 50,000 / 60,000 = 0.833333
        ```
        *Interpretação: Volume real foi 16.67% menor que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia → mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável × ρ
                           = R$ 400,000 × 0.833333
                           = R$ 333,333.33
        ```
        *Regra: Variável varia proporcionalmente → ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                        = R$ 200,000 + R$ 333,333.33
                        = R$ 533,333.33
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,000 = R$ 10.00 por unidade
        Flex_Bud_CPU = R$ 533,333.33 / 50,000 = R$ 10.67 por unidade
        Total_Real_CPU = R$ 550,000 / 50,000 = R$ 11.00 por unidade
        
        Δ_Flex_Bud = R$ 10.67 - R$ 10.00 = R$ 0.67 por unidade
        Δ_Total_Flex = R$ 11.00 - R$ 10.67 = R$ 0.33 por unidade
        ```
        
        **Interpretação:**
        - O volume real foi 16.67% menor que o planejado (`ρ = 0.833333`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 → R$ 333,333.33
        - O budget fixo permaneceu igual: R$ 200,000
        - Em CPU, o Flex Bud aumentou porque o custo fixo foi concentrado em menos volume
        - O Total Real está R$ 0.33 acima do Flex Bud, indicando ineficiência operacional
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Exemplo Prático 2 - Real x Budget (Volume Real > Volume Budget)
        
        **Dados:**
        - Volume Real (`V_Real`): 62,208 unidades
        - Volume Budget (`V_Budget`): 60,120 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total Variável Budget (`B_Variável`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        
        **Cálculo Passo a Passo:**
        
        **1. Calcular Proporção de Volume:**
        ```
        ρ = V_Real / V_Budget = 62,208 / 60,120 = 1.0347
        ```
        *Interpretação: Volume real foi 3.47% maior que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia → mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável × ρ
                           = R$ 400,000 × 1.0347
                           = R$ 413,880
        ```
        *Regra: Variável varia proporcionalmente → ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                        = R$ 200,000 + R$ 413,880
                        = R$ 613,880
        ```
        *Resultado: Flex_Bud_Total (R$ 613,880) > BUD_Total (R$ 600,000) ✅*
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,120 = R$ 9.98 por unidade
        Flex_Bud_CPU = R$ 613,880 / 62,208 = R$ 9.87 por unidade
        ```
        
        **Diferenças:**
        ```
        Δ_Flex_Bud (Custo Total) = R$ 613,880 - R$ 600,000 = R$ 13,880 (positivo) ✅
        Δ_Flex_Bud (CPU) = R$ 9.87 - R$ 9.98 = -R$ 0.11 (negativo)
        ```
        
        **Interpretação:**
        - O volume real foi 3.47% maior que o planejado (`ρ = 1.0347`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 → R$ 413,880
        - O budget fixo permaneceu igual: R$ 200,000
        - **Em Custo Total:** Flex_Bud_Total > BUD_Total (porque o custo variável aumentou)
        - **Em CPU:** Flex_Bud_CPU < BUD_CPU (porque o custo fixo foi diluído em mais volume)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Comparação: Real x Real vs Real x Budget
        
        | Aspecto | Real x Real (Waterfall) | Real x Budget (TC Ext) |
        |---------|------------------------|------------------------|
        | **Base** | Custo Real Mês 1 | Custo Budget |
        | **Volume Referência** | Volume Real Mês 1 | Volume Budget |
        | **Volume Ajuste** | Volume Real Mês 2 | Volume Real |
        | **Proporção** | `V₂ / V₁` | `V_Real / V_Budget` |
        | **Objetivo** | Ajustar Mês 1 pelo volume do Mês 2 | Ajustar Budget pelo volume Real |
        | **Uso** | Comparar dois períodos reais | Comparar Real vs Planejado |
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Regras Gerais Aplicáveis a Ambos os Casos
        
        **1. Custo Fixo:**
        - Sempre mantém o valor original (não varia com volume)
        - `Flex_Fixo = Valor_Original`
        
        **2. Custo Variável:**
        - Varia proporcionalmente ao volume
        - `Flex_Variável = Valor_Original × (Volume_Novo / Volume_Original)`
        
        **3. Ordem de Cálculo:**
        1. Calcular em **Custo Total** primeiro
        2. Separar Fixo e Variável
        3. Aplicar proporção de volume apenas ao Variável
        4. Somar Fixo + Variável ajustado
        5. Se necessário, converter para **CPU** dividindo pelo volume final
        
        **4. Tratamento de Divisão por Zero:**
        - Se `Volume_Original = 0`: usar `ρ = 1.0` (sem ajuste)
        - Se `Volume_Final = 0`: usar `Flex_CPU = 0`
        """)

# ==========================================
# SEÇÃO 2: ARQUITETURA E ESTRUTURA
# ==========================================
else:
    st.header("🏗️ Arquitetura e Estrutura do Projeto")
    
    st.markdown("""
    Esta seção documenta a arquitetura, estrutura de arquivos, tecnologias utilizadas
    e informações sobre a equipe responsável pelo desenvolvimento do projeto.
    """)
    
    st.markdown("---")
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("💻 Linhas de Código", "20.000+", "Sistema completo")
    
    with col2:
        st.metric("📊 Páginas", "5", "Funcionalidades completas")
    
    with col3:
        st.metric("⚡ Otimização", "70%+", "Memória reduzida")
    
    with col4:
        st.metric("📁 Arquivos", "Parquet", "Formato otimizado")
        
        st.markdown("---")
        
    # Sub-tabs para organização
    tab_estrutura, tab_tecnologias, tab_equipe = st.tabs([
        "📁 Estrutura de Arquivos",
        "💻 Tecnologias e Bibliotecas",
        "👥 Equipe do Projeto"
    ])
    
    # TAB 1: Estrutura de Arquivos
    with tab_estrutura:
        st.subheader("📁 Estrutura de Arquivos")
        
        st.markdown("""
        ### Estrutura do Projeto
        
        ```
        C:\\GIT\\TC\\
        ├── app.py                                    # Aplicação principal - Dashboard TC Ext (~9.800 linhas)
        ├── pages\\
        │   ├── 2 - Simulador Forecast.py            # Simulador de forecast (~4.300 linhas)
        │   ├── 3 - Forecast.py                      # Sistema de forecast (~7.400 linhas)
        │   ├── 4 - Waterfall.py                     # Análise waterfall (~2.400 linhas)
        │   ├── 4 - Waterfall_Analysis.py            # Análise waterfall (legado)
        │   └── 5 - Documentacao.py                 # Documentação (este arquivo)
        ├── dados\\
        │   ├── historico_consolidado\\
        │   │   ├── df_final_historico.parquet
        │   │   ├── df_ke5z_historico.parquet
        │   │   ├── df_vol_historico.parquet
        │   │   └── BUD\\
        │   │       ├── df_final_historico_BUD.parquet
        │   │       ├── df_ke5z_historico_BUD.parquet
        │   │       └── df_vol_historico_BUD.parquet
        │   ├── 2024\\
        │   │   ├── df_final.parquet
        │   │   ├── df_vol.parquet
        │   │   └── ... (outros arquivos)
        │   ├── 2025\\
        │   │   ├── df_final.parquet
        │   │   ├── df_vol.parquet
        │   │   └── BUD\\
        │   │       ├── df_final_BUD.parquet
        │   │       └── df_vol_BUD.parquet
        │   └── Forecast\\
        │       ├── df_final_historico_forecast.parquet
        │       ├── df_vol_historico.parquet
        │       ├── forecast_completo.parquet
        │       ├── forecast_historico.parquet
        │       └── forecast_previsao.parquet
        └── dados.ipynb                               # Notebook para processar dados
        ```
        
        **Observações:**
        - Arquivos principais: `historico_consolidado/` (usado pelo sistema)
        - Dados por ano: `2024/` e `2025/` (para filtros específicos)
        - Forecast: `Forecast/` (dados processados para previsões)
        - Formato: Parquet para performance otimizada
        """)
        
        st.markdown("---")
        
        st.subheader("📄 Arquivos Principais")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **app.py** (~9.800 linhas)
            - Dashboard principal TC Ext
            - Análise de custos com comparação Budget
            - Cálculo Flex Bud
            - Gráficos interativos
            - Tabelas hierárquicas
            - Exportação Excel
            
            **pages/2 - Simulador Forecast.py** (~4.300 linhas)
            - Simulação interativa de forecast
            - Ajuste de sensibilidade em tempo real
            - Configuração de inflação
            - Gráficos de premissas
            """)
        
        with col2:
            st.markdown("""
            **pages/3 - Forecast.py** (~7.400 linhas)
            - Sistema completo de forecast
            - Cálculo baseado em média histórica
            - Aplicação de sensibilidade e inflação
            - Visualizações e tabelas detalhadas
            
            **pages/4 - Waterfall.py** (~2.400 linhas)
            - Análise waterfall entre períodos
            - Cálculo Flex Mês 1
            - Gráficos waterfall interativos
            - Tabelas com hierarquia
            """)
    
    # TAB 2: Tecnologias
    with tab_tecnologias:
        st.subheader("💻 Tecnologias e Bibliotecas")
        
        st.markdown("""
        ### Stack Tecnológico
        
        **Framework Principal:**
        - **Streamlit** 1.28.0+ - Framework web para aplicações de dados
        
        **Linguagem:**
        - **Python** 3.8+ - Linguagem de programação
        
        **Processamento de Dados:**
        - **Pandas** 2.0.0+ - Manipulação e análise de dados
        - **NumPy** 1.24.0+ - Operações numéricas
        
        **Visualizações:**
        - **Altair** 5.0.0+ - Gráficos interativos
        - **Plotly** - Gráficos waterfall avançados
        
        **Formato de Dados:**
        - **PyArrow** 12.0.0+ - Suporte a Parquet
        - **Parquet** - Formato de dados otimizado
        
        **Exportação:**
        - **OpenPyXL** 3.1.0+ - Geração de arquivos Excel
        """)
        
        st.markdown("---")
        
        st.subheader("🔧 Dependências Principais")
        
        st.code("""
# requirements.txt
streamlit>=1.28.0
pandas>=2.0.0
altair>=5.0.0
numpy>=1.24.0
openpyxl>=3.1.0
pyarrow>=12.0.0
plotly>=5.0.0
        """, language="text")
        
        st.markdown("---")
        
        st.subheader("⚡ Otimizações Implementadas")
        
        st.markdown("""
        **Gestão de Memória:**
        - Cache inteligente com TTL configurável
        - Otimização de tipos: Category para strings repetidas
        - Downcast: Float64 → Float32, Int64 → Int32
        - Redução de cópias: Apenas quando necessário
        
        **Operações Vetorizadas:**
        - Substituição de `iterrows()` por merge e `np.where()`
        - Substituição de `apply()` por operações vetorizadas
        - Filtros booleanos ao invés de loops
        - Agrupamento otimizado com `agg()` direto
        
        **Cálculos Otimizados:**
        - CPU calculado após agrupamento (nunca antes)
        - Flex Bud com merge ao invés de loops
        - Volume sincronizado entre tabelas e gráficos
        - Cache de filtros para opções repetidas
        """)
    
    # TAB 3: Equipe
    with tab_equipe:
        st.subheader("👥 Equipe do Projeto")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### 🔧 Hudson Cardin
            
            **Responsável pelo desenvolvimento do projeto**
            
            **Contribuições:**
            - Arquitetura e estrutura do sistema
            - Implementação de cálculos e regras de negócio
            - Otimizações de performance
            - Desenvolvimento de funcionalidades principais
            - Documentação técnica
            """)
        
        with col2:
            st.markdown("""
            ### 📊 Lauro Paiva
            
            **Responsável pelo desenvolvimento do projeto**
            
            **Contribuições:**
            - Análise de requisitos e regras de negócio
            - Validação de cálculos e resultados
            - Testes e garantia de qualidade
            - Documentação de processos
            - Suporte e manutenção
            """)
        
        st.markdown("---")
        
        st.subheader("🎯 Objetivos do Projeto")
        
        st.markdown("""
        **🎯 Objetivos Principais:**
        - 📈 **Análise avançada de custos** com visualizações interativas
        - ⚡ **Performance otimizada** para grandes volumes (70%+ redução de memória)
        - 📊 **Dashboards especializados:** TC Ext, Forecast, Waterfall Analysis
        - 🔄 **Cálculo Flex Bud:** Budget flexível ajustado por volume
        - 📉 **Sistema de Forecast:** Previsões baseadas em média histórica
        - 🌊 **Análise Waterfall:** Comparação entre períodos com FLEX
        - 📥 **Exportação Excel:** Downloads formatados e filtrados
        - 🚀 **Cache inteligente:** TTL e otimização de tipos de dados
        - 📦 **Formato Parquet:** Dados comprimidos e otimizados
        - 🎨 **Interface moderna:** Tabs organizadas e gráficos com gradientes
        """)
        
        st.markdown("---")
        
        st.subheader("⚠️ Desafios Principais & Soluções")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div style="padding: 1.5rem; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); border-radius: 10px; margin: 1rem 0; color: white;">
                <h4 style="color: white; margin: 0; font-weight: 600;">
                    📊 DESAFIOS IDENTIFICADOS
                </h4>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            - **📁 Dados grandes:** Milhões de registros causando lentidão
            - **💾 Uso de memória:** Excedia limites de processamento
            - **❌ Instabilidade:** Sistema lento com muitos filtros
            - **🐌 Cálculos complexos:** Flex Bud e Forecast demorados
            - **🔄 Sincronização:** Dados de tabela vs gráficos diferentes
            - **📊 Visualizações:** Gráficos sem gradientes e pouco informativos
            """)
        
        with col2:
            st.markdown("""
            <div style="padding: 1.5rem; background: linear-gradient(135deg, #00b894 0%, #00a085 100%); border-radius: 10px; margin: 1rem 0; color: white;">
                <h4 style="color: white; margin: 0; font-weight: 600;">
                    ✅ SOLUÇÕES IMPLEMENTADAS
                </h4>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("""
            - **📊 Otimização de dados:** Parquet com tipos categóricos
            - **⚡ Cache estratégico:** TTL configurável por tipo de dado
            - **🔄 Operações vetorizadas:** Substituição de iterrows() e apply()
            - **📈 Cálculos otimizados:** Flex Bud e CPU após agrupamento
            - **🎯 Sincronização:** Mesma fonte de dados para tabelas e gráficos
            - **🎨 Visualizações melhoradas:** Gradientes, delta charts, barras HTML
            """)
        
        st.info("🎆 **Resultado Final:** Sistema 100% estável com performance otimizada e visualizações profissionais!")
        
        st.markdown("---")
        
        st.subheader("📊 Estatísticas do Sistema")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            with st.expander("💾 **DADOS E PERFORMANCE**", expanded=True):
                st.markdown("""
                **📁 Arquivos Principais:**
                - `df_final_historico.parquet` (dados históricos)
                - `df_vol_historico.parquet` (volumes)
                - `df_final_historico_BUD.parquet` (budget)
                
                **⚡ Otimizações:**
                - Tipos categóricos para strings
                - Downcast de numéricos
                - Compressão Parquet
                - Cache com TTL
                """)
        
        with col2:
            with st.expander("📊 **PÁGINAS DO SISTEMA**", expanded=True):
                st.markdown("""
                **📄 Páginas Disponíveis:**
                - `app.py` - Dashboard principal TC Ext (~9.800 linhas)
                - `2 - Simulador Forecast.py` - Simulação (~4.300 linhas)
                - `3 - Forecast.py` - Visualização (~7.400 linhas)
                - `4 - Waterfall.py` - Análise (~2.400 linhas)
                - `5 - Documentacao.py` - Documentação
                
                **📊 Total:** ~20.000+ linhas de código
                """)
        
        with col3:
            with st.expander("🔧 **TECNOLOGIAS**", expanded=True):
                st.markdown("""
                **✅ Stack Tecnológico:**
                - 🐍 Python 3.8+
                - 🌊 Streamlit (Web Framework)
                - 🐼 Pandas (Análise de Dados)
                - 📊 Altair (Gráficos)
                - 💾 PyArrow (Parquet)
                - 📋 OpenPyXL (Excel)
                - 🔢 NumPy (Cálculos)
                - 📈 Plotly (Waterfall)
        """)

# Rodapé
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão 3.0 | Janeiro 2025
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)
