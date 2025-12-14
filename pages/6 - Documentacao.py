import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import base64
import sys
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Documentação - Sistema TC",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Função para obter data e hora de atualização dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da última atualização dos arquivos de dados"""
    arquivos_dados = [
        os.path.join("dados", "historico_consolidado", "df_final_historico.parquet"),
        os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet"),
        os.path.join("dados", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
    ]
    
    data_atualizacao = None
    for arquivo in arquivos_dados:
        if os.path.exists(arquivo):
            data_modificacao = os.path.getmtime(arquivo)
            if data_atualizacao is None or data_modificacao > data_atualizacao:
                data_atualizacao = data_modificacao
    
    if data_atualizacao:
        dt = datetime.fromtimestamp(data_atualizacao)
        meses = {
            1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
            5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
            9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
        }
        return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
    return "Não disponível"

# Exibir data de atualização dos dados no topo
data_atualizacao = obter_data_atualizacao_dados()
st.markdown(f"""
<div style='text-align: right; color: #666; padding: 5px 10px; font-size: 0.85rem;'>
    📅 Dados atualizados em: {data_atualizacao}
</div>
""", unsafe_allow_html=True)

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

# Função para detectar caminho base correto
def get_base_path():
    """Retorna o caminho base correto para LEITURA de dados"""
    import sys
    if hasattr(sys, '_MEIPASS'):
        # Rodando no executável PyInstaller - apontar para _internal
        return sys._MEIPASS
    else:
        # Rodando em desenvolvimento
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Funções para persistir dados da equipe
def salvar_dados_equipe(dados):
    """Salva os dados da equipe em arquivo JSON"""
    try:
        base_path = get_base_path()
        dados_path = os.path.join(base_path, 'dados_equipe.json')
        with open(dados_path, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar dados: {e}")
        return False

def carregar_dados_equipe():
    """Carrega os dados da equipe do arquivo JSON"""
    try:
        base_path = get_base_path()
        dados_path = os.path.join(base_path, 'dados_equipe.json')
        if os.path.exists(dados_path):
            with open(dados_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        st.warning(f"Aviso ao carregar dados: {e}")
    
    # Retorna estrutura vazia se não conseguir carregar
    return {
        'hudson': {
            'cargo': '',
            'empresa': '',
            'experiencia': '',
            'linkedin': '',
            'foto': None
        },
        'lauro': {
            'cargo': '',
            'empresa': '',
            'experiencia': '',
            'linkedin': '',
            'foto': None
        }
    }

def salvar_foto_base64(foto_bytes, nome_arquivo):
    """Converte foto para base64 para salvar no JSON"""
    try:
        return base64.b64encode(foto_bytes).decode('utf-8')
    except:
        return None

def carregar_foto_base64(foto_base64):
    """Converte base64 de volta para bytes"""
    try:
        if foto_base64:
            return base64.b64decode(foto_base64)
    except:
        pass
    return None

# Sidebar com índices
st.sidebar.markdown("## 📑 Índice")
st.sidebar.markdown("---")

# Criar quatro índices no sidebar
indice_selecionado = st.sidebar.radio(
    "Selecione a seção:",
    ["👥 Equipe do Projeto", "📐 Regras e Cálculo", "🏗️ Arquitetura e Estrutura", "📥 Guia de Extração de Dados"],
    key="indice_documentacao"
)

st.markdown("---")

# ==========================================
# SEÇÃO 1: EQUIPE DO PROJETO
# ==========================================
if indice_selecionado == "👥 Equipe do Projeto":
    st.header("👥 Equipe do Projeto")
    
    st.markdown("""
    Esta seção apresenta informações sobre os membros da equipe responsáveis pelo desenvolvimento
    e manutenção do Sistema TC, incluindo suas experiências profissionais e contribuições ao projeto.
    """)
    
    st.markdown("---")
    
    # Carregar dados salvos
    dados_equipe = carregar_dados_equipe()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔧 Hudson Cardin")
        
        # Upload de foto para Hudson
        foto_hudson = st.file_uploader(
            "📸 Upload da foto do Hudson",
            type=['png', 'jpg', 'jpeg'],
            key="foto_hudson",
            help="Faça upload de uma foto do perfil do Hudson (formato: PNG, JPG, JPEG)"
        )
        
        # Mostrar foto salva ou nova foto
        if foto_hudson is not None:
            st.image(foto_hudson, width=200, caption="Hudson Cardin")
            # Salvar nova foto
            dados_equipe['hudson']['foto'] = salvar_foto_base64(foto_hudson.read(), "hudson.jpg")
        elif dados_equipe['hudson']['foto']:
            # Mostrar foto salva
            foto_bytes = carregar_foto_base64(dados_equipe['hudson']['foto'])
            if foto_bytes:
                st.image(foto_bytes, width=200, caption="Hudson Cardin")
            else:
                st.info("👤 Aguardando upload da foto")
        else:
            st.info("👤 Aguardando upload da foto")
        
        # Campos para informações do Hudson
        st.markdown("**📋 Informações Profissionais:**")
        
        with st.expander("✏️ Editar informações do Hudson", expanded=False):
            with st.form("form_hudson"):
                cargo_hudson = st.text_input(
                    "💼 Cargo atual:", 
                    value=dados_equipe['hudson']['cargo'],
                    placeholder="Ex: Analista de Sistemas", 
                    key="cargo_hudson"
                )
                empresa_hudson = st.text_input(
                    "🏢 Empresa:", 
                    value=dados_equipe['hudson']['empresa'],
                    placeholder="Ex: Empresa XYZ", 
                    key="empresa_hudson"
                )
                experiencia_hudson = st.text_area(
                    "🎯 Experiência:", 
                    value=dados_equipe['hudson']['experiencia'],
                    placeholder="Descreva a experiência profissional...", 
                    key="exp_hudson"
                )
                linkedin_hudson = st.text_input(
                    "🔗 LinkedIn:", 
                    value=dados_equipe['hudson']['linkedin'],
                    placeholder="https://linkedin.com/in/hudson-cardin", 
                    key="linkedin_hudson"
                )
                
                if st.form_submit_button("💾 Salvar informações do Hudson", use_container_width=True):
                    dados_equipe['hudson']['cargo'] = cargo_hudson
                    dados_equipe['hudson']['empresa'] = empresa_hudson
                    dados_equipe['hudson']['experiencia'] = experiencia_hudson
                    dados_equipe['hudson']['linkedin'] = linkedin_hudson
                    
                    if salvar_dados_equipe(dados_equipe):
                        st.success("✅ Informações do Hudson salvas com sucesso!")
                        st.rerun()
        
        # Expander para perfil profissional
        with st.expander("👨‍💻 Perfil Profissional", expanded=False):
            if dados_equipe['hudson']['cargo'] and dados_equipe['hudson']['empresa']:
                st.write(f"💼 **{dados_equipe['hudson']['cargo']}** na **{dados_equipe['hudson']['empresa']}**")
            elif dados_equipe['hudson']['cargo']:
                st.write(f"💼 **{dados_equipe['hudson']['cargo']}**")
            elif dados_equipe['hudson']['empresa']:
                st.write(f"🏢 **{dados_equipe['hudson']['empresa']}**")
            else:
                st.write("💼 *Cargo não informado*")
            
            if dados_equipe['hudson']['experiencia']:
                st.write(f"🎯 {dados_equipe['hudson']['experiencia']}")
            else:
                st.write("🎯 *Experiência não informada*")
            
            if dados_equipe['hudson']['linkedin']:
                st.markdown(f"🔗 [Perfil no LinkedIn]({dados_equipe['hudson']['linkedin']})")
            else:
                st.write("🔗 *LinkedIn não informado*")

    with col2:
        st.subheader("📊 Lauro Paiva Junior")
        
        # Upload de foto para Lauro
        foto_lauro = st.file_uploader(
            "📸 Upload da foto do Lauro",
            type=['png', 'jpg', 'jpeg'],
            key="foto_lauro",
            help="Faça upload de uma foto do perfil do Lauro (formato: PNG, JPG, JPEG)"
        )
        
        # Mostrar foto salva ou nova foto
        if foto_lauro is not None:
            st.image(foto_lauro, width=200, caption="Lauro Paiva Junior")
            # Salvar nova foto
            dados_equipe['lauro']['foto'] = salvar_foto_base64(foto_lauro.read(), "lauro.jpg")
        elif dados_equipe['lauro']['foto']:
            # Mostrar foto salva
            foto_bytes = carregar_foto_base64(dados_equipe['lauro']['foto'])
            if foto_bytes:
                st.image(foto_bytes, width=200, caption="Lauro Paiva Junior")
            else:
                st.info("👤 Aguardando upload da foto")
        else:
            st.info("👤 Aguardando upload da foto")
        
        # Campos para informações do Lauro
        st.markdown("**📋 Informações Profissionais:**")
        
        with st.expander("✏️ Editar informações do Lauro", expanded=False):
            with st.form("form_lauro"):
                cargo_lauro = st.text_input(
                    "💼 Cargo atual:", 
                    value=dados_equipe['lauro']['cargo'],
                    placeholder="Ex: Analista Financeiro", 
                    key="cargo_lauro"
                )
                empresa_lauro = st.text_input(
                    "🏢 Empresa:", 
                    value=dados_equipe['lauro']['empresa'],
                    placeholder="Ex: Empresa ABC", 
                    key="empresa_lauro"
                )
                experiencia_lauro = st.text_area(
                    "🎯 Experiência:", 
                    value=dados_equipe['lauro']['experiencia'],
                    placeholder="Descreva a experiência profissional...", 
                    key="exp_lauro"
                )
                linkedin_lauro = st.text_input(
                    "🔗 LinkedIn:", 
                    value=dados_equipe['lauro']['linkedin'],
                    placeholder="https://linkedin.com/in/lauro-paiva", 
                    key="linkedin_lauro"
                )
                
                if st.form_submit_button("💾 Salvar informações do Lauro", use_container_width=True):
                    dados_equipe['lauro']['cargo'] = cargo_lauro
                    dados_equipe['lauro']['empresa'] = empresa_lauro
                    dados_equipe['lauro']['experiencia'] = experiencia_lauro
                    dados_equipe['lauro']['linkedin'] = linkedin_lauro
                    
                    if salvar_dados_equipe(dados_equipe):
                        st.success("✅ Informações do Lauro salvas com sucesso!")
                        st.rerun()
        
        # Expander para perfil profissional
        with st.expander("👨‍💼 Perfil Profissional", expanded=False):
            if dados_equipe['lauro']['cargo'] and dados_equipe['lauro']['empresa']:
                st.write(f"💼 **{dados_equipe['lauro']['cargo']}** na **{dados_equipe['lauro']['empresa']}**")
            elif dados_equipe['lauro']['cargo']:
                st.write(f"💼 **{dados_equipe['lauro']['cargo']}**")
            elif dados_equipe['lauro']['empresa']:
                st.write(f"🏢 **{dados_equipe['lauro']['empresa']}**")
            else:
                st.write("💼 *Cargo não informado*")
            
            if dados_equipe['lauro']['experiencia']:
                st.write(f"🎯 {dados_equipe['lauro']['experiencia']}")
            else:
                st.write("🎯 *Experiência não informada*")
            
            if dados_equipe['lauro']['linkedin']:
                st.markdown(f"🔗 [Perfil no LinkedIn]({dados_equipe['lauro']['linkedin']})")
            else:
                st.write("🔗 *LinkedIn não informado*")
    
    st.markdown("---")
    
    st.markdown("""
    ### 🎯 Objetivos do Projeto
    
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

# ==========================================
# SEÇÃO 2: REGRAS E CÁLCULO
# ==========================================
elif indice_selecionado == "📐 Regras e Cálculo":
    st.header("📐 Regras e Cálculo")
    
    st.markdown("""
    Esta seção documenta todas as regras de cálculo, filtros e metodologias utilizadas no projeto.
    **IMPORTANTE:** Esta documentação serve como referência para garantir que todos os cálculos sejam
    reproduzidos de forma idêntica, permitindo que a IA consulte e refaça qualquer cálculo do sistema.
    
    A documentação está organizada em expanders para facilitar a navegação. Cada seção contém explicações
    detalhadas das regras, fórmulas matemáticas completas e exemplos práticos para facilitar o entendimento.
    """)
    
    st.markdown("---")
    
    # EXPANDER 1: Cálculos Principais
    with st.expander("🔢 **Cálculos Principais e Métricas Fundamentais**", expanded=False):
        with st.expander("📊 **CPU (Custo por Unidade)**", expanded=False):
            st.markdown("""
            ### 📊 CPU (Custo por Unidade)
            
            O **CPU (Custo por Unidade)** é uma métrica fundamental que representa o custo médio por unidade de produção.
            É calculado dividindo o custo total pelo volume de produção.
            
            **Fórmula Matemática:**
            ```
            CPU = Custo_Total / Volume_Total
            ```
            
            Onde:
            - `Custo_Total` = Soma de todos os custos individuais após agrupamento
            - `Volume_Total` = Soma de todos os volumes após agrupamento
            
            **⚠️ REGRA CRÍTICA:** O CPU deve ser calculado **APÓS** o agrupamento dos dados, nunca antes.
            Esta é uma das regras mais importantes do sistema, pois calcular CPU antes de agrupar resulta em valores incorretos.
            
            **Por que calcular após agrupamento?**
            
            A média aritmética de CPUs individuais não é igual ao CPU do total agregado. Isso ocorre porque o CPU é uma
            razão (divisão), e a média de razões não é igual à razão das médias.
            
            **Exemplo Ilustrativo:**
            
            Considere duas linhas de dados:
            - **Linha 1:** Custo Total = R$ 100, Volume = 10 unidades -> CPU = R$ 10,00/unidade
            - **Linha 2:** Custo Total = R$ 200, Volume = 40 unidades -> CPU = R$ 5,00/unidade
            
            **Método Incorreto (calcular CPU antes de agrupar):**
            - CPU médio = (R$ 10,00 + R$ 5,00) / 2 = **R$ 7,50/unidade** [INCORRETO]
            
            **Método Correto (calcular CPU após agrupar):**
            - Custo Total Agregado = R$ 100 + R$ 200 = R$ 300
            - Volume Total Agregado = 10 + 40 = 50 unidades
            - CPU Agregado = R$ 300 / 50 = **R$ 6,00/unidade** [CORRETO]
            
            A diferença entre R$ 7,50 e R$ 6,00 pode parecer pequena, mas em grandes volumes de dados essa discrepância
            se acumula e resulta em análises completamente incorretas.
            
            **Interpretação do CPU:**
            - **CPU baixo:** Indica eficiência operacional, menor custo por unidade produzida
            - **CPU alto:** Indica ineficiência ou custos elevados por unidade produzida
            - **Variação de CPU:** Mudanças no CPU entre períodos indicam variações na eficiência operacional
            """)
        
        with st.expander("💰 **Custo Total**", expanded=False):
            st.markdown("""
            ### 💰 Custo Total
        
        O **Custo Total** representa a soma de todos os custos individuais após a aplicação de filtros e agrupamentos.
        
        **Fórmula Matemática:**
        ```
        Custo_Total = Σ(Custo_Individual)
        ```
        
        Onde `Σ` representa a soma de todos os custos individuais que atendem aos critérios de filtragem.
        
        **Regras de Cálculo:**
        - Sempre somar valores individuais, nunca calcular média
        - Aplicar todos os filtros antes de realizar o agrupamento
        - Considerar apenas valores que atendem aos critérios de seleção
        - Não incluir valores nulos ou zerados no cálculo
        
        **Agrupamento por Dimensões:**
        
        O custo total pode ser calculado para diferentes níveis de agregação:
        - Por período (mês, trimestre, semestre, ano)
        - Por oficina
        - Por veículo
        - Por categoria de custo (Type 05, Type 06, Account)
        - Por combinação de dimensões
        
        **Interpretação:**
        - **Custo Total crescente:** Indica aumento nos gastos operacionais
        - **Custo Total decrescente:** Indica redução nos gastos operacionais
        - **Comparação entre períodos:** Permite identificar tendências e variações
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 🔄 Fator de Conversão (K/M)
        
        Os **Fatores de Conversão** são utilizados para facilitar a visualização de valores muito grandes,
        convertendo-os para unidades mais legíveis (milhares ou milhões).
        
        **Fatores Disponíveis:**
        - **K (milhares):** Divide o valor por 1.000
        - **M (Milhões):** Divide o valor por 1.000.000
        - **Nenhum:** Mantém o valor original
        
        **Fórmulas Matemáticas:**
        ```
        Valor_K = Valor_Original / 1.000
        Valor_M = Valor_Original / 1.000.000
        ```
        
        **⚠️ REGRA CRÍTICA:** O fator de conversão **NÃO** deve ser aplicado no modo **CPU (Custo por Unidade)**.
        
        **Por que não aplicar em CPU?**
        
        O CPU já é uma razão (divisão entre Custo Total e Volume). Se aplicarmos o fator de conversão ao Custo Total
        antes de calcular o CPU, estaríamos dividindo duas vezes, o que resultaria em valores completamente incorretos.
        
        **Exemplo:**
        - Custo Total Original: R$ 1.000.000
        - Volume: 10.000 unidades
        - CPU Correto: R$ 1.000.000 / 10.000 = **R$ 100,00/unidade** [CORRETO]
        
        Se aplicássemos o fator K antes:
        - Custo Total com K: R$ 1.000 K
        - CPU Incorreto: R$ 1.000 K / 10.000 = **R$ 0,10/unidade** [INCORRETO] (1000 vezes menor!)
        
        **Ordem de Aplicação das Transformações:**
        
        1. **Primeiro:** Aplicar fator de conversão (K/M) - apenas em modo Custo Total
        2. **Segundo:** Converter moeda (se necessário)
        3. **Terceiro:** Realizar cálculos (CPU, Flex Bud, diferenças, etc.)
        
        Esta ordem garante que todas as transformações sejam aplicadas corretamente e que os resultados finais
        sejam consistentes e precisos.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 📅 Agrupamento por Período
        
        O **Agrupamento por Período** permite consolidar dados em diferentes intervalos de tempo, facilitando
        análises comparativas e identificação de tendências.
        
        **Estrutura de Períodos:**
        
        Quando os dados contêm informação de **Ano**, o sistema cria uma coluna combinada `Período_Ano` que
        agrupa tanto o período quanto o ano:
        ```
        Período_Ano = Período + " " + Ano
        ```
        
        Exemplo: "Janeiro 2024", "Fevereiro 2024", etc.
        
        **Agrupamento com Ano:**
        - Dimensões de agrupamento: `['Ano', 'Período']`
        - Permite comparações ano a ano
        - Facilita análises de tendências de longo prazo
        
        **Agrupamento sem Ano:**
        - Dimensões de agrupamento: `['Período']`
        - Útil quando todos os dados são do mesmo ano
        - Simplifica análises mensais ou trimestrais
        
        **Fórmula de Agregação:**
        ```
        Custo_Total_Agrupado = Σ(Custo_Individual) agrupado por Período
        Volume_Total_Agrupado = Σ(Volume_Individual) agrupado por Período
        ```
        
        **Interpretação:**
        - Permite identificar sazonalidades e padrões temporais
        - Facilita comparações entre períodos equivalentes
        - Suporta análises de tendências e projeções
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 📈 Cálculo de Diferenças e Ratios
        
        As **Diferenças e Ratios** são métricas essenciais para análise de desempenho, permitindo comparar
        valores reais com valores planejados ou ajustados.
        
        **1. Diferença Flex Bud - BUD:**
        
        Esta métrica compara o Budget Flexível (ajustado pelo volume real) com o Budget original planejado.
        
        **Fórmula:**
        ```
        Delta_Flex_Bud = Flex_BUD - BUD
        ```
        
        **Interpretação:**
        - **Valor positivo:** Flex Bud > BUD (custo ajustado maior que o planejado)
        - **Valor negativo:** Flex Bud < BUD (custo ajustado menor que o planejado)
        - **Zero:** Flex Bud = BUD (custo ajustado igual ao planejado)
        
        **2. Diferença Total - Flex Bud:**
        
        Esta métrica compara o custo real com o Budget Flexível, indicando a eficiência operacional.
        
        **Fórmula:**
        ```
        Delta_Total_Flex = Total - Flex_BUD
        ```
        
        **Interpretação:**
        - **Valor positivo:** Total > Flex Bud (ineficiência operacional)
        - **Valor negativo:** Total < Flex Bud (eficiência operacional)
        - **Zero:** Total = Flex Bud (desempenho exatamente como esperado)
        
        **3. Ratio Total / Flex Bud:**
        
        Esta métrica expressa o desempenho real como percentual do Budget Flexível.
        
        **Fórmula:**
        ```
        Ratio = Total / Flex_BUD
        Percentual = Ratio * 100%
        ```
        
        **Interpretação:**
        - **< 100%:** Total < Flex Bud (melhor que esperado, eficiência operacional)
        - **= 100%:** Total = Flex Bud (exatamente como esperado)
        - **> 100%:** Total > Flex Bud (pior que esperado, ineficiência operacional)
        
        **Exemplo Prático:**
        - Flex Bud = R$ 500.000
        - Total Real = R$ 520.000
        - Ratio = 520.000 / 500.000 = 1,04 = **104%**
        - Interpretação: O custo real está 4% acima do Budget Flexível, indicando ineficiência operacional
        """)
    
    # EXPANDER 2: Flex Bud
    with st.expander("🔄 **Cálculo de Flex Bud (Budget Flexível)**", expanded=False):
        with st.expander("📋 **Conceito e Regras Fundamentais**", expanded=False):
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
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### Regra Geral para Custos Variáveis
            
            **Princípio:** Custos variáveis variam PROPORCIONALMENTE ao volume de produção.
            
            **Fórmula Geral:**
            ```
            Flex_Variável = Valor_Original_Variável * (Volume_Novo / Volume_Original)
            ```
            
            **Explicação:**
            - Se o volume dobra, o custo variável dobra
            - Se o volume reduz pela metade, o custo variável reduz pela metade
            - Exemplos: Matéria-prima, energia variável, comissões
            - Sensibilidade ao volume: **100%** (cem por cento)
            """)
        
        # Ler o conteúdo do Flex Bud que está mais abaixo no arquivo
        # Por enquanto, vou adicionar um placeholder e depois mover o conteúdo correto
        st.info("📚 Conteúdo detalhado do Flex Bud será movido para cá...")
    
    # EXPANDER 3: Volumes
    with st.expander("📊 **Cálculo de Volumes**", expanded=False):
        with st.expander("📁 **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### 📁 Fonte de Dados de Volume
            
            Os dados de volume são armazenados em arquivos Parquet otimizados para garantir performance e eficiência
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de análise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados históricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano específico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histórico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contêm as seguintes colunas obrigatórias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`Período`:** Período de referência (mês, trimestre, etc.)
            - **`Oficina`:** Identificação da oficina
            - **`Veículo`:** Identificação do veículo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referência (quando disponível)
            
            **Sincronização com Dados de Custo:**
            
            Os dados de volume são estruturados de forma a permitir sincronização perfeita com os dados de custo,
            garantindo que o cálculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Filtragem de Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra é absolutamente crítica para garantir a precisão do cálculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cópia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores únicos extraídos dos dados de custo filtrados
            3. **Filtrar por período:** Aplicar filtro específico para o período de análise
            4. **Agrupar e somar:** Agrupar por período e somar os volumes
            
            **Fórmula de Agregação:**
            
            ```
            Volume_Total = Σ(Volume_Individual) agrupado por Período
            ```
            
            Onde `Σ` representa a soma de todos os volumes individuais que atendem aos critérios de filtragem.
            
            **Importância da Consistência:**
            
            A consistência entre os filtros aplicados aos dados de custo e volume é essencial porque:
            - O CPU é calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante será completamente incorreto
            - Análises baseadas em CPU incorreto podem levar a decisões de negócio equivocadas
            
            **Exemplo de Impacto:**
            
            Se você filtrar os custos para uma oficina específica mas não filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (não filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    # EXPANDER 4: Moeda e Taxas
    with st.expander("💱 **Moeda e Taxas de Câmbio**", expanded=False):
        with st.expander("💱 **Moedas Suportadas**", expanded=False):
            st.markdown("""
            ### 💱 Moedas Suportadas
            
            O sistema suporta conversão entre diferentes moedas para facilitar análises internacionais e comparações
            com dados de outras unidades de negócio. As moedas disponíveis são:
            
            - **BRL (R$):** Real Brasileiro - moeda base do sistema
            - **USD ($):** Dólar Americano
            - **EUR:** Euro
            
            **Moeda Base:**
            
            O Real Brasileiro (BRL) é a moeda base do sistema. Todos os valores são originalmente armazenados em BRL,
            e as conversões para outras moedas são realizadas multiplicando os valores pela taxa de câmbio correspondente.
            """)
        
        with st.expander("📊 **Taxas de Câmbio**", expanded=False):
            st.markdown("""
            ### 📊 Taxas de Câmbio
            
            As **Taxas de Câmbio** definem a relação de conversão entre a moeda base (BRL) e as outras moedas suportadas.
            
            **Definição Matemática:**
            
            As taxas são definidas como a quantidade de moeda estrangeira equivalente a 1 Real Brasileiro:
            ```
            1 BRL = Taxa_USD USD
            1 BRL = Taxa_EUR EUR
            ```
            
            **Exemplo Prático:**
            
            Se a taxa de câmbio USD for 0,20, isso significa que:
            - 1 Real Brasileiro = 0,20 Dólares Americanos
            - Para converter R$ 100,00 para USD: R$ 100,00 * 0,20 = $ 20,00
            
            **Fórmula de Conversão:**
            
            Para converter um valor de BRL para outra moeda:
            ```
            Valor_Convertido = Valor_BRL * Taxa_Cambio
            ```
            
            Onde:
            - `Valor_BRL` = Valor original em Real Brasileiro
            - `Taxa_Câmbio` = Taxa de câmbio da moeda de destino
            - `Valor_Convertido` = Valor convertido para a moeda de destino
            
            **Ordem de Aplicação das Transformações:**
            
            Quando múltiplas transformações são aplicadas (fator de conversão K/M e conversão de moeda), a ordem
            é crítica para garantir resultados corretos:
            
            1. **Primeiro:** Aplicar fator de conversão (K/M) - apenas em modo Custo Total
            2. **Segundo:** Converter moeda (se necessário)
            3. **Terceiro:** Realizar cálculos (CPU, Flex Bud, diferenças, etc.)
            
            **Exemplo Completo de Transformação:**
            
            Considere um valor original de R$ 1.000.000,00:
            
            - **Passo 1 (Fator K):** R$ 1.000.000,00 / 1.000 = R$ 1.000 K
            - **Passo 2 (Conversão USD, taxa 0,20):** R$ 1.000 K * 0,20 = $ 200 K
            - **Resultado Final:** $ 200 K (duzentos mil dólares)
            """)
        
        with st.expander("💾 **Persistência e Atualização de Taxas**", expanded=False):
            st.markdown("""
            ### 💾 Persistência e Atualização de Taxas
            
            As taxas de câmbio são armazenadas de forma persistente para garantir que as conversões sejam
            consistentes entre diferentes sessões de análise.
            
            **Armazenamento:**
            
            - As taxas são salvas em banco de dados ou arquivo de configuração
            - Valores padrão são utilizados caso não existam taxas salvas
            - As taxas podem ser atualizadas a qualquer momento através da interface do sistema
            
            **Atualização de Taxas:**
            
            As taxas de câmbio podem ser atualizadas para refletir as condições de mercado atuais. Quando uma
            nova taxa é definida, ela é aplicada a todos os cálculos subsequentes, garantindo que as análises
            estejam sempre baseadas nas taxas mais recentes.
            
            **Importância da Atualização:**
            
            Manter as taxas de câmbio atualizadas é essencial para garantir a precisão das análises, especialmente
            em períodos de alta volatilidade cambial. Taxas desatualizadas podem resultar em comparações e
            análises completamente incorretas.
            """)
    
    # EXPANDER 5: Filtros e Perímetros
    with st.expander("🔍 **Filtros e Perímetros de Análise**", expanded=False):
        with st.expander("🎯 **Sistema de Filtros da Interface**", expanded=False):
            st.markdown("""
            ### 🎯 Sistema de Filtros da Interface
            
            O sistema possui um conjunto abrangente de filtros que permitem refinar a análise de dados de forma
            precisa e flexível. Os filtros são aplicados sequencialmente, criando um perímetro de análise cada vez
            mais específico conforme o usuário seleciona diferentes critérios.
            
            **Ordem de Aplicação dos Filtros:**
            
            Os filtros são aplicados na seguinte ordem hierárquica, garantindo que cada filtro refine o resultado
            do filtro anterior:
            
            1. **Ano** - Seleção do ano de análise (Radio button)
            2. **Oficina** - Seleção de uma ou mais oficinas (Multiselect)
            3. **Veículo** - Seleção de um ou mais veículos (Multiselect)
            4. **USI** - Seleção de unidades de serviço (Multiselect)
            5. **Período** - Seleção de período específico (Selectbox)
            6. **Centro cst** - Seleção de centro de custo (Selectbox)
            7. **Conta contábil** - Seleção de contas contábeis (Multiselect)
            8. **Type 5** - Seleção de categorias Type 05 (Multiselect)
            9. **Type 6** - Seleção de categorias Type 06 (Multiselect)
            10. **Fornecedor** - Seleção de fornecedores (Multiselect)
            11. **Fornec.** - Seleção adicional de fornecedores (Multiselect)
            12. **Tipo** - Seleção de tipos de custo (Multiselect)
            13. **Filtros Avançados:**
                - **Usuário** - Filtro por usuário responsável
                - **Material** - Filtro por material utilizado
                - **Dt.lçto.** - Filtro por data de lançamento
                - **Texto breve** - Filtro por texto descritivo
                - **Account** - Filtro por conta contábil específica
            
            **Princípio de Funcionamento:**
            
            Cada filtro atua como um "funil" que reduz progressivamente o conjunto de dados analisados. Quando
            múltiplos filtros são aplicados, apenas os registros que atendem a **TODOS** os critérios selecionados
            são incluídos na análise final.
            
            **Exemplo de Aplicação Sequencial:**
            
            Imagine que você selecionou:
            - Ano: 2024
            - Oficina: "Oficina A" e "Oficina B"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            O sistema primeiro filtra todos os dados de 2024, depois mantém apenas os registros das Oficinas A e B,
            em seguida mantém apenas os registros do Veículo X, e finalmente mantém apenas os registros de Janeiro.
            O resultado final contém apenas os registros que atendem a todos esses critérios simultaneamente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Perímetro de Filtros para Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Perímetro de Filtros para Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta é uma das regras mais importantes do sistema, pois garante que o cálculo de
            CPU seja preciso e consistente.
            
            **Por que esta regra é crítica?**
            
            O CPU é calculado como a razão entre Custo Total e Volume. Se os filtros aplicados ao custo forem
            diferentes dos filtros aplicados ao volume, o CPU resultante será completamente incorreto.
            
            **Exemplo Ilustrativo:**
            
            Imagine que você filtrou os dados de custo para incluir apenas:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            Se o volume não for filtrado da mesma forma, você poderia estar dividindo:
            - Custo Total (filtrado): R$ 100.000 (apenas Oficina A, Veículo X, Janeiro)
            - Volume Total (não filtrado): 50.000 unidades (todas as oficinas, todos os veículos, todos os períodos)
            - CPU Incorreto: R$ 100.000 / 50.000 = R$ 2,00/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 100.000 (Oficina A, Veículo X, Janeiro)
            - Volume Total (filtrado): 10.000 unidades (Oficina A, Veículo X, Janeiro)
            - CPU Correto: R$ 100.000 / 10.000 = R$ 10,00/unidade [CORRETO]
            
            **Mecanismo de Sincronização:**
            
            O sistema garante a sincronização dos filtros extraindo os valores únicos das dimensões filtradas dos
            dados principais e aplicando esses mesmos valores aos dados de volume. Isso garante que o perímetro de
            análise seja idêntico para ambos os conjuntos de dados.
            
            **Dimensões Sincronizadas:**
            
            As seguintes dimensões são sempre sincronizadas entre dados de custo e volume:
            - Veículo
            - Oficina
            - USI
            - Centro de Custo
            - Conta Contábil
            - Type 05
            - Type 06
            - Fornecedor
            - Tipo
            - E todos os filtros avançados (Usuário, Material, Data, etc.)
            """)
        
        with st.expander("📊 **Sincronização de Filtros para Budget**", expanded=False):
            st.markdown("""
            ### 📊 Sincronização de Filtros para Budget
            
            **Regra Fundamental:** O Budget deve usar os mesmos filtros aplicados aos dados reais para garantir
            comparações justas e precisas.
            
            **Por que sincronizar filtros do Budget?**
            
            Quando comparamos dados reais com budget, precisamos garantir que estamos comparando "maçãs com maçãs".
            Se os dados reais estão filtrados para uma oficina específica, o budget também deve estar filtrado para
            a mesma oficina, caso contrário a comparação não terá sentido.
            
            **Exemplo:**
            
            Se você filtrar os dados reais para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            O budget também será automaticamente filtrado para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            Isso garante que a comparação entre Real e Budget seja feita no mesmo contexto operacional.
            
            **Mecanismo de Aplicação:**
            
            O sistema extrai os valores únicos de todas as dimensões filtradas dos dados reais e aplica esses mesmos
            valores como filtros ao budget. Isso garante que o perímetro de análise seja idêntico para ambos os
            conjuntos de dados, permitindo comparações precisas e significativas.
            """)
    
    
    # EXPANDER 3: Volumes
    with st.expander("📊 **Cálculo de Volumes**", expanded=False):
        with st.expander("📁 **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### 📁 Fonte de Dados de Volume
            
            Os dados de volume são armazenados em arquivos Parquet otimizados para garantir performance e eficiência
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de análise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados históricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano específico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histórico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contêm as seguintes colunas obrigatórias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`Período`:** Período de referência (mês, trimestre, etc.)
            - **`Oficina`:** Identificação da oficina
            - **`Veículo`:** Identificação do veículo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referência (quando disponível)
            
            **Sincronização com Dados de Custo:**
            
            Os dados de volume são estruturados de forma a permitir sincronização perfeita com os dados de custo,
            garantindo que o cálculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Filtragem de Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra é absolutamente crítica para garantir a precisão do cálculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cópia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores únicos extraídos dos dados de custo filtrados
            3. **Filtrar por período:** Aplicar filtro específico para o período de análise
            4. **Agrupar e somar:** Agrupar por período e somar os volumes
            
            **Fórmula de Agregação:**
            
            ```
            Volume_Total = Σ(Volume_Individual) agrupado por Período
            ```
            
            Onde `Σ` representa a soma de todos os volumes individuais que atendem aos critérios de filtragem.
            
            **Importância da Consistência:**
            
            A consistência entre os filtros aplicados aos dados de custo e volume é essencial porque:
            - O CPU é calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante será completamente incorreto
            - Análises baseadas em CPU incorreto podem levar a decisões de negócio equivocadas
            
            **Exemplo de Impacto:**
            
            Se você filtrar os custos para uma oficina específica mas não filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (não filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    # EXPANDER 5: Filtros e Perímetros
    with st.expander("🔍 **Filtros e Perímetros de Análise**", expanded=False):
        with st.expander("🎯 **Sistema de Filtros da Interface**", expanded=False):
            st.markdown("""
            ### 🎯 Sistema de Filtros da Interface
            
            O sistema possui um conjunto abrangente de filtros que permitem refinar a análise de dados de forma
            precisa e flexível. Os filtros são aplicados sequencialmente, criando um perímetro de análise cada vez
            mais específico conforme o usuário seleciona diferentes critérios.
            
            **Ordem de Aplicação dos Filtros:**
            
            Os filtros são aplicados na seguinte ordem hierárquica, garantindo que cada filtro refine o resultado
            do filtro anterior:
            
            1. **Ano** - Seleção do ano de análise (Radio button)
            2. **Oficina** - Seleção de uma ou mais oficinas (Multiselect)
            3. **Veículo** - Seleção de um ou mais veículos (Multiselect)
            4. **USI** - Seleção de unidades de serviço (Multiselect)
            5. **Período** - Seleção de período específico (Selectbox)
            6. **Centro cst** - Seleção de centro de custo (Selectbox)
            7. **Conta contábil** - Seleção de contas contábeis (Multiselect)
            8. **Type 5** - Seleção de categorias Type 5 (Multiselect)
            9. **Type 6** - Seleção de categorias Type 6 (Multiselect)
            10. **Fornecedor** - Seleção de fornecedores (Multiselect)
            11. **Fornec.** - Seleção adicional de fornecedores (Multiselect)
            12. **Tipo** - Seleção de tipos de custo (Multiselect)
            13. **Filtros Avançados:**
                - **Usuário** - Filtro por usuário responsável
                - **Material** - Filtro por material utilizado
                - **Dt.lçto.** - Filtro por data de lançamento
                - **Texto breve** - Filtro por texto descritivo
                - **Account** - Filtro por conta contábil específica
            
            **Princípio de Funcionamento:**
            
            Cada filtro atua como um "funil" que reduz progressivamente o conjunto de dados analisados. Quando
            múltiplos filtros são aplicados, apenas os registros que atendem a **TODOS** os critérios selecionados
            são incluídos na análise final.
            
            **Exemplo de Aplicação Sequencial:**
            
            Imagine que você selecionou:
            - Ano: 2024
            - Oficina: "Oficina A" e "Oficina B"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            O sistema primeiro filtra todos os dados de 2024, depois mantém apenas os registros das Oficinas A e B,
            em seguida mantém apenas os registros do Veículo X, e finalmente mantém apenas os registros de Janeiro.
            O resultado final contém apenas os registros que atendem a todos esses critérios simultaneamente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Perímetro de Filtros para Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Perímetro de Filtros para Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta é uma das regras mais importantes do sistema, pois garante que o cálculo de
            CPU seja preciso e consistente.
            
            **Por que esta regra é crítica?**
            
            O CPU é calculado como a razão entre Custo Total e Volume. Se os filtros aplicados ao custo forem
            diferentes dos filtros aplicados ao volume, o CPU resultante será completamente incorreto.
            
            **Exemplo Ilustrativo:**
            
            Imagine que você filtrou os dados de custo para incluir apenas:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            Se o volume não for filtrado da mesma forma, você poderia estar dividindo:
            - Custo Total (filtrado): R$ 100.000 (apenas Oficina A, Veículo X, Janeiro)
            - Volume Total (não filtrado): 50.000 unidades (todas as oficinas, todos os veículos, todos os períodos)
            - CPU Incorreto: R$ 100.000 / 50.000 = R$ 2,00/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 100.000 (Oficina A, Veículo X, Janeiro)
            - Volume Total (filtrado): 10.000 unidades (Oficina A, Veículo X, Janeiro)
            - CPU Correto: R$ 100.000 / 10.000 = R$ 10,00/unidade [CORRETO]
            
            **Mecanismo de Sincronização:**
            
            O sistema garante a sincronização dos filtros extraindo os valores únicos das dimensões filtradas dos
            dados principais e aplicando esses mesmos valores aos dados de volume. Isso garante que o perímetro de
            análise seja idêntico para ambos os conjuntos de dados.
            
            **Dimensões Sincronizadas:**
            
            As seguintes dimensões são sempre sincronizadas entre dados de custo e volume:
            - Veículo
            - Oficina
            - USI
            - Centro de Custo
            - Conta Contábil
            - Type 5
            - Type 6
            - Fornecedor
            - Tipo
            - E todos os filtros avançados (Usuário, Material, Data, etc.)
            """)
        
        with st.expander("📊 **Sincronização de Filtros para Budget**", expanded=False):
            st.markdown("""
            ### 📊 Sincronização de Filtros para Budget
            
            **Regra Fundamental:** O Budget deve usar os mesmos filtros aplicados aos dados reais para garantir
            comparações justas e precisas.
            
            **Por que sincronizar filtros do Budget?**
            
            Quando comparamos dados reais com budget, precisamos garantir que estamos comparando "maçãs com maçãs".
            Se os dados reais estão filtrados para uma oficina específica, o budget também deve estar filtrado para
            a mesma oficina, caso contrário a comparação não terá sentido.
            
            **Exemplo:**
            
            Se você filtrar os dados reais para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            O budget também será automaticamente filtrado para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            Isso garante que a comparação entre Real e Budget seja feita no mesmo contexto operacional.
            
            **Mecanismo de Aplicação:**
            
            O sistema extrai os valores únicos de todas as dimensões filtradas dos dados reais e aplica esses mesmos
            valores como filtros ao budget. Isso garante que o perímetro de análise seja idêntico para ambos os
            conjuntos de dados, permitindo comparações precisas e significativas.
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
        Flex_Variável = Valor_Original_Variável * (Volume_Novo / Volume_Original)
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
        Flex_Mês1_Fixo = C_1_Fixo
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
        Flex_Mês1_Variável = C_1_Variável * rho
                              = C_1_Variável * (V_2 / V_1)
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
                        = C_1_Fixo + (C_1_Variável * rho)
                        = C_1_Fixo + C_1_Variável * (V_2 / V_1)
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Real
        
        **Definições:**
        - `V_1` = Volume Real do Mês 1
        - `V_2` = Volume Real do Mês 2
        - `C_1_Fixo` = Custo Total Fixo do Mês 1
        - `C_1_Variável` = Custo Total Variável do Mês 1
        - `C_1_Total` = Custo Total do Mês 1 = `C_1_Fixo + C_1_Variável`
        
        **Proporção de Volume:**
        ```
        rho = V_2 / V_1
        ```
        Onde:
        - `rho > 1` significa que o volume aumentou
        - `rho < 1` significa que o volume diminuiu
        - `rho = 1` significa que o volume permaneceu igual
        
        **Cálculo de Flex Mês 1 (em Custo Total):**
        
        Para **Custo Fixo:**
        ```
        Flex_Mês1_Fixo = C_1_Fixo
        ```
        **Regra Aplicada:** Fixo não varia com volume
        - Valor original mantido: `C_1_Fixo`
        - Não multiplica pela proporção de volume
        - Motivo: Custos fixos são independentes do volume de produção
        
        Para **Custo Variável:**
        ```
        Flex_Mês1_Variável = C_1_Variável * rho
                              = C_1_Variável * (V_2 / V_1)
        ```
        **Regra Aplicada:** Variável varia proporcionalmente ao volume
        - Valor original: `C_1_Variável`
        - Multiplica pela proporção: `rho = V_2 / V_1`
        - Motivo: Custos variáveis aumentam/diminuem na mesma proporção do volume
        
        **Flex Mês 1 Total (em Custo Total):**
        ```
        Flex_Mês1_Total = Flex_Mês1_Fixo + Flex_Mês1_Variável
                        = C_1_Fixo + (C_1_Variável * rho)
                        = C_1_Fixo + C_1_Variável * (V_2 / V_1)
        ```
        **Regra Aplicada:** Soma do Fixo (inalterado) + Variável (ajustado)
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Cálculo em CPU (Custo por Unidade) - Real x Real
        
        **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD (Mês 1) em CPU:**
        ```
        BUD_CPU = C_1_Total / V_1
                 = (C_1_Fixo + C_1_Variável) / V_1
        ```
        
        **Flex Mês 1 em CPU:**
        ```
        Flex_Mês1_CPU = Flex_Mês1_Total / V_2
                       = [C_1_Fixo + C_1_Variável * (V_2 / V_1)] / V_2
                       = (C_1_Fixo / V_2) + (C_1_Variável / V_1)
        ```
        
        **Diferença (Flex Mês 1 - Mês 1):**
        ```
        Delta_Flex = Flex_Mês1_CPU - BUD_CPU
               = [(C_1_Fixo / V_2) + (C_1_Variável / V_1)] - [(C_1_Fixo + C_1_Variável) / V_1]
               = (C_1_Fixo / V_2) - (C_1_Fixo / V_1)
               = C_1_Fixo * (1/V_2 - 1/V_1)
               = C_1_Fixo * (V_1 - V_2) / (V_1 * V_2)
        ```
        
        **Interpretação:**
        - Se `V_2 > V_1`: `Delta_Flex < 0` (CPU diminui porque custo fixo é diluído em mais volume)
        - Se `V_2 < V_1`: `Delta_Flex > 0` (CPU aumenta porque custo fixo é concentrado em menos volume)
        - Se `V_2 = V_1`: `Delta_Flex = 0` (sem variação)
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
        - Volume Real Mês 1 (`V_1`): 40,848 unidades
        - Volume Real Mês 2 (`V_2`): 60,333 unidades
        - Custo Total Fixo Mês 1 (`C_1_Fixo`): R$ 126.91
        - Custo Total Variável Mês 1 (`C_1_Variável`): R$ 755.36
        - Custo Total Mês 1 (`C_1_Total`): R$ 882.27
        
        **Cálculo:**
        ```
        rho = V_2 / V_1 = 60,333 / 40,848 = 1.482373
        
        Flex_Mês1_Fixo = R$ 126.91
        Flex_Mês1_Variável = R$ 755.36 * 1.482373 = R$ 1,119.72
        Flex_Mês1_Total = R$ 126.91 + R$ 1,119.72 = R$ 1,246.63
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 882.27 / 40,848 = R$ 0.0216 por unidade
        Flex_Mês1_CPU = R$ 1,246.63 / 60,333 = R$ 0.0207 por unidade
        Delta_Flex = R$ 0.0207 - R$ 0.0216 = -R$ 0.0009 por unidade
        ```
        
        **Interpretação:**
        - O volume aumentou 48.24% (`rho = 1.482373`)
        - O custo variável aumentou proporcionalmente: R$ 755.36 -> R$ 1,119.72
        - O custo fixo permaneceu igual: R$ 126.91
        - Em CPU, o custo por unidade diminuiu porque o custo fixo foi diluído em mais volume
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### Modos de Comparação - Real x Real
        
        **Mês a Mês:**
        - `V_1` = Volume do mês inicial
        - `V_2` = Volume do mês final
        
        **Ano a Ano:**
        - `V_1` = Volume total do ano inicial
        - `V_2` = Volume total do ano final
        
        **Semestre:**
        - `V_1` = Volume total do semestre inicial
        - `V_2` = Volume total do semestre final
        
        **Quarter:**
        - `V_1` = Volume total do trimestre inicial
        - `V_2` = Volume total do trimestre final
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
        Flex_Bud_Variável = B_Variável * rho
                           = B_Variável * (V_Real / V_Budget)
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
                       = B_Fixo + (B_Variável * rho)
                       = B_Fixo + B_Variável * (V_Real / V_Budget)
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
        rho = V_Real / V_Budget
        ```
        Onde:
        - `rho > 1` significa que o volume real foi maior que o planejado
        - `rho < 1` significa que o volume real foi menor que o planejado
        - `rho = 1` significa que o volume real foi exatamente o planejado
        
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
        Flex_Bud_Variável = B_Variável * rho
                           = B_Variável * (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Variável varia proporcionalmente ao volume
        - Valor do budget: `B_Variável`
        - Multiplica pela proporção: `rho = V_Real / V_Budget`
        - Motivo: Se o volume real for maior que o planejado, o custo variável deve aumentar proporcionalmente
        
        **Flex Bud Total (em Custo Total):**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                       = B_Fixo + (B_Variável * rho)
                       = B_Fixo + B_Variável * (V_Real / V_Budget)
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
                     = [B_Fixo + B_Variável * (V_Real / V_Budget)] / V_Real
                     = (B_Fixo / V_Real) + (B_Variável / V_Budget)
        ```
        
        **Total Real em CPU:**
        ```
        Total_Real_CPU = R_Total / V_Real
        ```
        
        **Diferenças:**
        
        **Flex Bud - BUD:**
        ```
        Delta_Flex_Bud = Flex_Bud_CPU - BUD_CPU
                   = [(B_Fixo / V_Real) + (B_Variável / V_Budget)] - [(B_Fixo + B_Variável) / V_Budget]
                   = (B_Fixo / V_Real) - (B_Fixo / V_Budget)
                   = B_Fixo * (1/V_Real - 1/V_Budget)
                   = B_Fixo * (V_Budget - V_Real) / (V_Real * V_Budget)
        ```
        
        **Total - Flex Bud:**
        ```
        Delta_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
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
        rho = V_Real / V_Budget = 50,000 / 60,000 = 0.833333
        ```
        *Interpretação: Volume real foi 16.67% menor que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia -> mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = R$ 400,000 * 0.833333
                           = R$ 333,333.33
        ```
        *Regra: Variável varia proporcionalmente -> ajusta pelo volume real*
        
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
        
        Delta_Flex_Bud = R$ 10.67 - R$ 10.00 = R$ 0.67 por unidade
        Delta_Total_Flex = R$ 11.00 - R$ 10.67 = R$ 0.33 por unidade
        ```
        
        **Interpretação:**
        - O volume real foi 16.67% menor que o planejado (`rho = 0.833333`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 -> R$ 333,333.33
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
        rho = V_Real / V_Budget = 62,208 / 60,120 = 1.0347
        ```
        *Interpretação: Volume real foi 3.47% maior que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia -> mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = R$ 400,000 * 1.0347
                           = R$ 413,880
        ```
        *Regra: Variável varia proporcionalmente -> ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                        = R$ 200,000 + R$ 413,880
                        = R$ 613,880
        ```
        *Resultado: Flex_Bud_Total (R$ 613,880) > BUD_Total (R$ 600,000) [CORRETO]*
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,120 = R$ 9.98 por unidade
        Flex_Bud_CPU = R$ 613,880 / 62,208 = R$ 9.87 por unidade
        ```
        
        **Diferenças:**
        ```
        Delta_Flex_Bud (Custo Total) = R$ 613,880 - R$ 600,000 = R$ 13,880 (positivo) [CORRETO]
        Delta_Flex_Bud (CPU) = R$ 9.87 - R$ 9.98 = -R$ 0.11 (negativo)
        ```
        
        **Interpretação:**
        - O volume real foi 3.47% maior que o planejado (`rho = 1.0347`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 -> R$ 413,880
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
        | **Proporção** | `V_2 / V_1` | `V_Real / V_Budget` |
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
        - `Flex_Variável = Valor_Original * (Volume_Novo / Volume_Original)`
        
        **3. Ordem de Cálculo:**
        1. Calcular em **Custo Total** primeiro
        2. Separar Fixo e Variável
        3. Aplicar proporção de volume apenas ao Variável
        4. Somar Fixo + Variável ajustado
        5. Se necessário, converter para **CPU** dividindo pelo volume final
        
        **4. Tratamento de Divisão por Zero:**
        - Se `Volume_Original = 0`: usar `rho = 1.0` (sem ajuste)
        - Se `Volume_Final = 0`: usar `Flex_CPU = 0`
        """)

# ==========================================
# SEÇÃO 2: ARQUITETURA E ESTRUTURA
# ==========================================
elif indice_selecionado == "🏗️ Arquitetura e Estrutura":
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
        
    # EXPANDER 1: Estrutura de Arquivos
    with st.expander("📁 **Estrutura de Arquivos e Organização do Projeto**", expanded=False):
        st.subheader("📁 Estrutura de Arquivos")
        
        st.markdown("""
        ### Estrutura do Projeto
        
        ```
        C:\\GIT\\TC\\
        ├── app.py                                    # Aplicação principal - Dashboard TC Ext (~9.800 linhas)
        ├── pages\\
               │   ├── 2 - Best Estimate - Simulador.py     # Simulador de Best Estimate (~4.300 linhas)
               │   ├── 3 - Best Estimate - Análise.py       # Análise de Best Estimate (~7.400 linhas)
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
            
            **pages/2 - Best Estimate - Simulador.py** (~4.300 linhas)
            - Simulação interativa de Best Estimate
            - Ajuste de sensibilidade em tempo real
            - Configuração de inflação
            - Gráficos de premissas
            """)
        
        with col2:
            st.markdown("""
            **pages/3 - Best Estimate - Análise.py** (~7.400 linhas)
            - Sistema completo de Best Estimate
            - Cálculo baseado em média histórica
            - Aplicação de sensibilidade e inflação
            - Visualizações e tabelas detalhadas
            
            **pages/4 - Waterfall.py** (~2.400 linhas)
            - Análise waterfall entre períodos
            - Cálculo Flex Mês 1
            - Gráficos waterfall interativos
            - Tabelas com hierarquia
            """)
        
        st.markdown("---")
        
        # Sub-expander: Estrutura da Pasta dados
        with st.expander("📂 **Estrutura e Funcionamento da Pasta `dados/`**", expanded=False):
            st.markdown("""
            ### 📂 Organização da Pasta `dados/`
            
            A pasta `dados/` é o coração do sistema, onde todos os arquivos processados são armazenados.
            Ela é organizada de forma hierárquica para facilitar o gerenciamento e acesso aos dados.
            
            **Estrutura Completa:**
            ```
            dados/
            ├── historico_consolidado/          # 📚 Dados históricos consolidados (PRINCIPAL)
            │   ├── df_final_historico.parquet  # Dados de custos históricos consolidados
            │   ├── df_ke5z_historico.parquet  # Dados KE5Z históricos consolidados
            │   ├── df_vol_historico.parquet    # Volumes históricos consolidados
            │   └── BUD/                        # 📊 Dados de Budget históricos
            │       ├── df_final_historico_BUD.parquet
            │       ├── df_ke5z_historico_BUD.parquet
            │       └── df_vol_historico_BUD.parquet
            │
            ├── 2024/                           # 📅 Dados específicos do ano 2024
            │   ├── df_final.parquet           # Dados de custos do ano
            │   ├── df_vol.parquet             # Volumes do ano
            │   ├── df_ke5z_group.parquet      # Dados KE5Z agrupados
            │   └── Dados SAPIENS.xlsx          # Arquivo fonte (entrada)
            │   └── Reporting fluxo anexo.xlsx  # Arquivo fonte (entrada)
            │
            ├── 2025/                           # 📅 Dados específicos do ano 2025
            │   ├── df_final.parquet
            │   ├── df_vol.parquet
            │   ├── df_ke5z_group.parquet
            │   ├── BUD/                        # 📊 Budget do ano 2025
            │   │   ├── df_final_BUD.parquet
            │   │   └── df_vol_BUD.parquet
            │   └── ... (arquivos fonte)
            │
            └── Forecast/                       # 🔮 Dados processados para Forecast
                ├── df_final_historico_forecast.parquet
                ├── df_vol_historico.parquet
                ├── forecast_completo.parquet
                ├── forecast_historico.parquet
                └── forecast_previsao.parquet
            ```
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### 🔄 Como as Pastas São Criadas e Atualizadas
            
            **1. Criação Inicial da Estrutura:**
            
            Quando o sistema é executado pela primeira vez ou quando novos dados são processados,
            o sistema verifica e cria automaticamente as pastas necessárias:
            
            ```python
            # Exemplo de criação de pastas (dados.ipynb)
            PASTA_ANO = f'dados/{ANO_ATUAL}'  # Ex: dados/2025
            PASTA_HISTORICO = 'dados/historico_consolidado'
            PASTA_BUD = f'dados/{ANO_ATUAL}/BUD'
            
            # Criar estrutura de pastas
            os.makedirs(PASTA_ANO, exist_ok=True)
            os.makedirs(PASTA_HISTORICO, exist_ok=True)
            os.makedirs(PASTA_BUD, exist_ok=True)
            ```
            
            **2. Processo de Atualização:**
            
            **a) Processamento de Dados do Ano:**
            - Os arquivos Excel (`Dados SAPIENS.xlsx`, `Reporting fluxo anexo.xlsx`) são colocados na pasta do ano (ex: `dados/2025/`)
            - O notebook `dados.ipynb` processa esses arquivos e gera os arquivos Parquet
            - Os arquivos Parquet são salvos na mesma pasta do ano
            - **Simultaneamente**, os dados são consolidados no histórico
            
            **b) Consolidação no Histórico:**
            - Após processar os dados do ano, o sistema **concatena** os novos dados com o histórico existente
            - Os arquivos em `historico_consolidado/` são **atualizados** (não substituídos)
            - Isso permite que o sistema tenha acesso a **todos os dados históricos** em um único lugar
            
            **c) Processamento de Budget:**
            - Similar ao processo de dados do ano, mas os arquivos são processados pelo `dados_BUD.ipynb`
            - Os dados de Budget são salvos em `dados/{ANO}/BUD/`
            - O histórico de Budget é consolidado em `historico_consolidado/BUD/`
            
            **d) Processamento de Forecast:**
            - Quando o Forecast é gerado (páginas 2 ou 3), a pasta `dados/Forecast/` é criada automaticamente
            - Os arquivos de forecast são salvos diretamente nesta pasta
            - A pasta é criada dinamicamente se não existir:
            ```python
            pasta_dados = "dados"
            pasta_forecast = os.path.join(pasta_dados, "Forecast")
            
            if not os.path.exists(pasta_forecast):
                os.makedirs(pasta_forecast, exist_ok=True)
            ```
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### 🔗 Como as Pastas Funcionam Entre Si
            
            **1. Relação entre Pastas por Ano e Histórico:**
            
            ```
            dados/2025/df_final.parquet  ──┐
                                           ├──> Concatena ──> dados/historico_consolidado/df_final_historico.parquet
            dados/2024/df_final.parquet  ──┘
            ```
            
            - **Dados do Ano:** Contêm apenas os dados do ano específico (útil para filtros rápidos)
            - **Histórico Consolidado:** Contém **TODOS** os anos concatenados (usado pelo sistema principal)
            - O sistema **prioriza** o histórico consolidado para análises que precisam de múltiplos anos
            
            **2. Fluxo de Dados:**
            
            ```
            Arquivos Excel (entrada)
                │
                ├──> Processamento (dados.ipynb)
                │       │
                │       ├──> Salva em dados/{ANO}/ (dados do ano)
                │       │
                │       └──> Concatena em historico_consolidado/ (histórico completo)
                │
                └──> Sistema Streamlit lê de historico_consolidado/ (fonte principal)
            ```
            
            **3. Separação de Budget:**
            
            - **Dados Reais:** `dados/{ANO}/` e `historico_consolidado/`
            - **Dados Budget:** `dados/{ANO}/BUD/` e `historico_consolidado/BUD/`
            - Esta separação permite comparações **Real vs Budget** sem misturar os dados
            
            **4. Forecast como Dados Derivados:**
            
            - A pasta `Forecast/` contém dados **processados e calculados** pelo sistema
            - Não são dados de entrada, mas sim **resultados** de cálculos de forecast
            - São gerados dinamicamente quando o usuário executa o Forecast
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### ⚙️ Regras de Criação e Atualização
            
            **Regra 1: Criação Automática**
            - Todas as pastas são criadas automaticamente quando necessário
            - O parâmetro `exist_ok=True` garante que não há erro se a pasta já existir
            - Não é necessário criar manualmente nenhuma pasta
            
            **Regra 2: Consolidação Incremental**
            - O histórico é **atualizado** (não substituído) a cada processamento
            - Novos dados são **adicionados** ao histórico existente
            - Isso mantém a integridade dos dados históricos
            
            **Regra 3: Separação por Tipo**
            - Dados Reais e Budget são mantidos **separados** em pastas diferentes
            - Isso evita confusão e permite comparações precisas
            - O sistema sabe qual pasta usar baseado no modo de comparação selecionado
            
            **Regra 4: Formato Parquet**
            - Todos os arquivos processados são salvos em formato **Parquet**
            - Parquet oferece compressão e leitura rápida
            - Formato otimizado para grandes volumes de dados
            """)
    
    # EXPANDER 2: Tecnologias
    with st.expander("💻 **Tecnologias e Bibliotecas**", expanded=False):
        st.subheader("💻 Tecnologias e Bibliotecas")
        
        st.markdown(f"""
        ### Stack Tecnológico
        
        **Framework Principal:**
        - **Streamlit** {st.__version__} - Framework web para aplicações de dados
        
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
        - Downcast: Float64 -> Float32, Int64 -> Int32
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
    
    # EXPANDER 3: Desafios e Soluções
    with st.expander("⚠️ **Desafios Principais & Soluções Implementadas**", expanded=False):
        st.markdown("""
        ### 📊 Desafios Identificados
        
        - **📁 Dados grandes:** Milhões de registros causando lentidão
        - **💾 Uso de memória:** Excedia limites de processamento
        - **Instabilidade:** Sistema lento com muitos filtros
        - **🐌 Cálculos complexos:** Flex Bud e Forecast demorados
        - **🔄 Sincronização:** Dados de tabela vs gráficos diferentes
        - **📊 Visualizações:** Gráficos sem gradientes e pouco informativos
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ✅ Soluções Implementadas
        
        - **📊 Otimização de dados:** Parquet com tipos categóricos
        - **⚡ Cache estratégico:** TTL configurável por tipo de dado
        - **🔄 Operações vetorizadas:** Substituição de iterrows() e apply()
        - **📈 Cálculos otimizados:** Flex Bud e CPU após agrupamento
        - **🎯 Sincronização:** Mesma fonte de dados para tabelas e gráficos
        - **🎨 Visualizações melhoradas:** Gradientes, delta charts, barras HTML
        """)
        
        st.info("🎆 **Resultado Final:** Sistema 100% estável com performance otimizada e visualizações profissionais!")
    
    # EXPANDER 4: Estatísticas do Sistema
    with st.expander("📊 **Estatísticas e Métricas do Sistema**", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 💾 Dados e Performance
            
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
            st.markdown("""
            ### 📊 Páginas do Sistema
            
            **📄 Páginas Disponíveis:**
            - `app.py` - Dashboard principal TC Ext (~9.800 linhas)
                   - `2 - Best Estimate - Simulador.py` - Simulação (~4.300 linhas)
                   - `3 - Best Estimate - Análise.py` - Análise (~7.400 linhas)
            - `4 - Waterfall.py` - Análise (~2.400 linhas)
            - `5 - Documentacao.py` - Documentação
            
            **📊 Total:** ~20.000+ linhas de código
            """)
        
        with col3:
            st.markdown(f"""
            ### 🔧 Tecnologias
            
            **Stack Principal:**
            - Streamlit {st.__version__}
            - Pandas {pd.__version__}
            - NumPy {np.__version__}
            - Altair (versão instalada)
            - Plotly (versão instalada)
            - OpenPyXL (versão instalada)
            """)


# Função para obter mês atual em português
def obter_mes_atual():
    """Retorna o mês atual em português"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# Rodapé
st.markdown("---")
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão 1.0 | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)
