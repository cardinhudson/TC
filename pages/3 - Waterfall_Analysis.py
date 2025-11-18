import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

st.set_page_config(
    page_title="Análise Waterfall - TC", 
    page_icon="🌊", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

st.title("🌊 Análise Waterfall - TC")
st.markdown("---")

PT_MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
MES_POS = {m: i + 1 for i, m in enumerate(PT_MESES)}

def sort_mes_unique(values):
    """Ordena valores de meses únicos"""
    vals = list(pd.Series(values).dropna().unique())
    try:
        return sorted(vals, key=lambda x: int(x))
    except Exception:
        return sorted(vals, key=lambda x: MES_POS.get(str(x).lower(), 99))

@st.cache_data(ttl=3600, max_entries=3)
def load_df_historico() -> pd.DataFrame:
    """Carrega dados do arquivo histórico consolidado"""
    caminho_historico = os.path.join("dados", "historico_consolidado", "df_final_historico.parquet")
    
    if not os.path.exists(caminho_historico):
        st.error("❌ **Arquivo histórico não encontrado**")
        st.error(f"📁 Caminho esperado: {caminho_historico}")
        st.info("💡 **Solução**: Certifique-se de que o arquivo df_final_historico.parquet existe na pasta dados/historico_consolidado/")
        st.stop()
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(caminho_historico)
        
        # Otimizar tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                if unique_ratio < 0.5:
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
        
        return df
    except Exception as e:
        st.error(f"❌ **Erro ao carregar dados**: {str(e)}")
        st.stop()
        return pd.DataFrame()

@st.cache_data(ttl=3600, max_entries=3)
def load_df_volume() -> pd.DataFrame:
    """Carrega dados de volume do arquivo histórico consolidado"""
    caminho_volume = os.path.join("dados", "historico_consolidado", "df_vol_historico.parquet")
    
    if not os.path.exists(caminho_volume):
        return pd.DataFrame()  # Retorna vazio se não encontrar
    
    try:
        df = pd.read_parquet(caminho_volume)
        return df
    except Exception:
        return pd.DataFrame()

def obter_semestre_trimestre(mes_str, ano):
    """
    Identifica o semestre e trimestre de um mês.
    Retorna: (semestre, trimestre) onde semestre=1 ou 2, trimestre=1,2,3 ou 4
    """
    meses_semestre = {
        'janeiro': (1, 1), 'fevereiro': (1, 1), 'março': (1, 1),
        'abril': (1, 2), 'maio': (1, 2), 'junho': (1, 2),
        'julho': (2, 3), 'agosto': (2, 3), 'setembro': (2, 3),
        'outubro': (2, 4), 'novembro': (2, 4), 'dezembro': (2, 4)
    }
    mes_lower = mes_str.lower().strip()
    # Remover ano se estiver presente (ex: "Janeiro 2024" -> "janeiro")
    if ' ' in mes_lower:
        mes_lower = mes_lower.split(' ', 1)[0]
    return meses_semestre.get(mes_lower, (1, 1))

def calcular_flex(df_dados, df_volume, mes_inicial, mes_final, col_mes, col_valor, 
                  sensibilidade_fixo=0.0, sensibilidade_variavel=1.0, inflacao=0.0,
                  modo_sensibilidade="Global", dict_sens_fixo=None, dict_sens_variavel=None,
                  modo_inflacao="Global", dict_inflacao=None, col_categoria=None,
                  modo_comparacao="Mês a Mês", ano_inicial=None, ano_final=None,
                  semestre_inicial=None, semestre_final=None, trimestre_inicial=None, trimestre_final=None):
    """
    Calcula o efeito FLEX baseado na variação de volume entre dois períodos,
    aplicando sensibilidade e inflação conforme a lógica do forecast.
    
    Parâmetros:
    - sensibilidade_fixo/variavel: valores globais (modo Global)
    - dict_sens_fixo/variavel: {categoria: sensibilidade} (modo Detalhado)
    - inflacao: valor global (modo Global)
    - dict_inflacao: {categoria: inflacao} (modo Detalhado)
    - col_categoria: coluna usada para categorização no modo Detalhado
    - modo_comparacao: "Mês a Mês", "Ano a Ano", "Semestre" ou "Quarter"
    - ano_inicial/ano_final: anos para comparação (quando modo_comparacao == "Ano a Ano", "Semestre" ou "Quarter")
    - semestre_inicial/semestre_final: semestres (1 ou 2) para comparação
    - trimestre_inicial/trimestre_final: quarters (1, 2, 3 ou 4) para comparação
    
    Retorna: (flex_volume, flex_inflacao)
    - flex_volume: Efeito de volume + sensibilidade
    - flex_inflacao: Efeito da inflação
    """
    if df_volume.empty:
        return 0.0, 0.0
    
    try:
        # Criar coluna Período_Ano no df_volume se necessário
        if 'Período_Ano' not in df_volume.columns and 'Período' in df_volume.columns and 'Ano' in df_volume.columns:
            df_volume = df_volume.copy()
            df_volume['Período_Ano'] = df_volume['Período'].astype(str) + ' ' + df_volume['Ano'].astype(str)
        
        # Obter dados do período inicial baseado no modo de comparação
        if modo_comparacao == "Ano a Ano" and ano_inicial and ano_final:
            # Para Ano a Ano: usar TODOS os dados do ano inicial
            df_mes_inicial = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
        elif modo_comparacao == "Semestre" and ano_inicial and ano_final and semestre_inicial and semestre_final:
            # Para Semestre: filtrar por ano e semestre
            df_temp = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
            # Identificar meses do semestre
            meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                            2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
            meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
            df_mes_inicial = df_temp[df_temp[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial])].copy()
        elif modo_comparacao == "Quarter" and ano_inicial and ano_final and trimestre_inicial and trimestre_final:
            # Para Trimestre: filtrar por ano e trimestre
            df_temp = df_dados[df_dados['Ano'].astype(str) == str(ano_inicial)].copy()
            # Identificar meses do trimestre
            meses_trimestre = {
                1: ['janeiro', 'fevereiro', 'março'],
                2: ['abril', 'maio', 'junho'],
                3: ['julho', 'agosto', 'setembro'],
                4: ['outubro', 'novembro', 'dezembro']
            }
            meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
            df_mes_inicial = df_temp[df_temp[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial])].copy()
        else:
            # Para Mês a Mês: usar dados do mês específico
            df_mes_inicial = df_dados[df_dados[col_mes].astype(str) == str(mes_inicial)].copy()
        
        if df_mes_inicial.empty:
            return 0.0, 0.0
        
        # Obter volume baseado no modo de comparação
        col_mes_vol = 'Período_Ano' if 'Período_Ano' in df_volume.columns else 'Período'
        
        if modo_comparacao == "Ano a Ano" and ano_inicial and ano_final:
            # Para Ano a Ano: usar volume TOTAL de cada ano
            volume_inicial = df_volume[df_volume['Ano'].astype(str) == str(ano_inicial)]['Volume'].sum()
            volume_final = df_volume[df_volume['Ano'].astype(str) == str(ano_final)]['Volume'].sum()
        elif modo_comparacao == "Semestre" and ano_inicial and ano_final and semestre_inicial and semestre_final:
            # Para Semestre: usar volume TOTAL do semestre
            meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                            2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
            meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
            meses_sem_final = meses_semestre.get(semestre_final, [])
            df_vol_inicial = df_volume[(df_volume['Ano'].astype(str) == str(ano_inicial)) & 
                                      (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial]))]
            df_vol_final = df_volume[(df_volume['Ano'].astype(str) == str(ano_final)) & 
                                    (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_final]))]
            volume_inicial = df_vol_inicial['Volume'].sum()
            volume_final = df_vol_final['Volume'].sum()
        elif modo_comparacao == "Quarter" and ano_inicial and ano_final and trimestre_inicial and trimestre_final:
            # Para Trimestre: usar volume TOTAL do trimestre
            meses_trimestre = {
                1: ['janeiro', 'fevereiro', 'março'],
                2: ['abril', 'maio', 'junho'],
                3: ['julho', 'agosto', 'setembro'],
                4: ['outubro', 'novembro', 'dezembro']
            }
            meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
            meses_trim_final = meses_trimestre.get(trimestre_final, [])
            df_vol_inicial = df_volume[(df_volume['Ano'].astype(str) == str(ano_inicial)) & 
                                      (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial]))]
            df_vol_final = df_volume[(df_volume['Ano'].astype(str) == str(ano_final)) & 
                                    (df_volume[col_mes_vol].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_final]))]
            volume_inicial = df_vol_inicial['Volume'].sum()
            volume_final = df_vol_final['Volume'].sum()
        else:
            # Para Mês a Mês: usar volume do mês específico
            volume_inicial = df_volume[df_volume[col_mes_vol].astype(str) == str(mes_inicial)]['Volume'].sum()
            volume_final = df_volume[df_volume[col_mes_vol].astype(str) == str(mes_final)]['Volume'].sum()
        
        if volume_inicial == 0 or volume_final == 0:
            return 0.0, 0.0
        
        # Calcular proporção e variação de volume
        proporcao_volume = volume_final / volume_inicial
        variacao_percentual = proporcao_volume - 1.0
        
        # ========== MODO GLOBAL ==========
        if modo_sensibilidade == "Global" and modo_inflacao == "Global":
            # Agrupar por tipo de custo
            if 'Custo' in df_mes_inicial.columns:
                custo_por_tipo = df_mes_inicial.groupby('Custo')[col_valor].sum()
                custo_fixo = float(custo_por_tipo.get('Fixo', 0.0))
                custo_variavel = float(custo_por_tipo.get('Variável', 0.0))
            else:
                custo_fixo = 0.0
                custo_variavel = float(df_mes_inicial[col_valor].sum())
            
            custo_total_inicial = custo_fixo + custo_variavel
            
            # Aplicar sensibilidade
            variacao_ajustada_fixo = variacao_percentual * sensibilidade_fixo
            variacao_ajustada_variavel = variacao_percentual * sensibilidade_variavel
            fator_variacao_fixo = 1.0 + variacao_ajustada_fixo
            fator_variacao_variavel = 1.0 + variacao_ajustada_variavel
            
            # Calcular custo após volume + sensibilidade (SEM inflação)
            custo_apos_volume = custo_fixo * fator_variacao_fixo + custo_variavel * fator_variacao_variavel
            flex_volume = custo_apos_volume - custo_total_inicial
            
            # Aplicar inflação
            fator_inflacao = 1.0 + (inflacao / 100.0)
            custo_final_com_inflacao = custo_apos_volume * fator_inflacao
            flex_inflacao = custo_final_com_inflacao - custo_apos_volume
            
            return float(flex_volume), float(flex_inflacao)
        
        # ========== MODO DETALHADO ==========
        else:
            flex_volume_total = 0.0
            flex_inflacao_total = 0.0
            
            # Iterar por categoria
            if col_categoria and col_categoria in df_mes_inicial.columns:
                categorias = df_mes_inicial[col_categoria].unique()
                
                for categoria in categorias:
                    df_cat = df_mes_inicial[df_mes_inicial[col_categoria] == categoria]
                    
                    # Obter custos por tipo
                    if 'Custo' in df_cat.columns:
                        custo_por_tipo = df_cat.groupby('Custo')[col_valor].sum()
                        custo_fixo_cat = float(custo_por_tipo.get('Fixo', 0.0))
                        custo_variavel_cat = float(custo_por_tipo.get('Variável', 0.0))
                    else:
                        custo_fixo_cat = 0.0
                        custo_variavel_cat = float(df_cat[col_valor].sum())
                    
                    # Obter sensibilidade para esta categoria
                    if modo_sensibilidade == "Detalhado" and dict_sens_fixo and dict_sens_variavel:
                        sens_fixo_cat = dict_sens_fixo.get(str(categoria), sensibilidade_fixo)
                        sens_var_cat = dict_sens_variavel.get(str(categoria), sensibilidade_variavel)
                    else:
                        sens_fixo_cat = sensibilidade_fixo
                        sens_var_cat = sensibilidade_variavel
                    
                    # Calcular flex volume para esta categoria
                    variacao_ajustada_fixo = variacao_percentual * sens_fixo_cat
                    variacao_ajustada_variavel = variacao_percentual * sens_var_cat
                    fator_variacao_fixo = 1.0 + variacao_ajustada_fixo
                    fator_variacao_variavel = 1.0 + variacao_ajustada_variavel
                    
                    custo_apos_volume_cat = (custo_fixo_cat * fator_variacao_fixo + 
                                            custo_variavel_cat * fator_variacao_variavel)
                    custo_inicial_cat = custo_fixo_cat + custo_variavel_cat
                    flex_volume_cat = custo_apos_volume_cat - custo_inicial_cat
                    
                    # Obter inflação para esta categoria
                    if modo_inflacao == "Detalhado" and dict_inflacao:
                        inflacao_cat = dict_inflacao.get(str(categoria), inflacao)
                    else:
                        inflacao_cat = inflacao
                    
                    # Calcular flex inflação para esta categoria
                    fator_inflacao_cat = 1.0 + (inflacao_cat / 100.0)
                    custo_final_cat = custo_apos_volume_cat * fator_inflacao_cat
                    flex_inflacao_cat = custo_final_cat - custo_apos_volume_cat
                    
                    flex_volume_total += flex_volume_cat
                    flex_inflacao_total += flex_inflacao_cat
            
            return float(flex_volume_total), float(flex_inflacao_total)
    
    except Exception:
        return 0.0, 0.0

# Carregar dados
df_base = load_df_historico()
if df_base.empty:
    st.stop()

# Carregar dados de volume
df_volume = load_df_volume()

st.sidebar.success("✅ Dados carregados com sucesso")
st.sidebar.info(f"📊 {len(df_base):,} registros carregados")
if not df_volume.empty:
    st.sidebar.success(f"📈 {len(df_volume):,} registros de volume carregados")

# Aplicar filtros padrão do projeto
st.sidebar.title("Filtros")

# ============================================================================
# 🎚️ CONFIGURAÇÕES GLOBAIS (para cálculo do FLEX)
# ============================================================================
st.sidebar.markdown("---")
st.sidebar.subheader("🎚️ Configurações Globais")
st.sidebar.markdown("Aplicadas ao cálculo do FLEX")

# Inicializar session_state se necessário
if 'sensibilidade_fixo' not in st.session_state:
    st.session_state.sensibilidade_fixo = 0.0
if 'sensibilidade_variavel' not in st.session_state:
    st.session_state.sensibilidade_variavel = 1.0
if 'inflacao' not in st.session_state:
    st.session_state.inflacao = 0.0
if 'modo_sensibilidade' not in st.session_state:
    st.session_state.modo_sensibilidade = "Global"
if 'modo_inflacao' not in st.session_state:
    st.session_state.modo_inflacao = "Global"

# ========== MODO SENSIBILIDADE ==========
st.sidebar.markdown("### 🎯 Sensibilidade")
modo_sensibilidade = st.sidebar.radio(
    "Modo de Sensibilidade:",
    options=["Global", "Detalhado"],
    index=0 if st.session_state.modo_sensibilidade == "Global" else 1,
    key="radio_modo_sens",
    help="Global: mesma sensibilidade para todos | Detalhado: configuração por categoria"
)
st.session_state.modo_sensibilidade = modo_sensibilidade

if modo_sensibilidade == "Global":
    # Botões de sensibilidade
    col_sens1, col_sens2 = st.sidebar.columns(2)
    with col_sens1:
        if st.button("📌 Fixo: 0%", key="btn_sens_fixo_0"):
            st.session_state.sensibilidade_fixo = 0.0
        if st.button("📌 Fixo: 50%", key="btn_sens_fixo_50"):
            st.session_state.sensibilidade_fixo = 0.5
    with col_sens2:
        if st.button("📌 Var: 50%", key="btn_sens_var_50"):
            st.session_state.sensibilidade_variavel = 0.5
        if st.button("📌 Var: 100%", key="btn_sens_var_100"):
            st.session_state.sensibilidade_variavel = 1.0

    # Sliders
    sensibilidade_fixo = st.sidebar.slider(
        "Sensibilidade - Custo Fixo",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.sensibilidade_fixo,
        step=0.1,
        key="slider_sens_fixo",
        help="0 = Custo não varia com volume | 1 = Custo varia 100% com volume"
    )
    st.session_state.sensibilidade_fixo = sensibilidade_fixo

    sensibilidade_variavel = st.sidebar.slider(
        "Sensibilidade - Custo Variável",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.sensibilidade_variavel,
        step=0.1,
        key="slider_sens_var",
        help="0 = Custo não varia com volume | 1 = Custo varia 100% com volume"
    )
    st.session_state.sensibilidade_variavel = sensibilidade_variavel
else:
    st.sidebar.info("⚙️ Configure sensibilidades detalhadas na seção principal")
    # Usar valores do session_state como padrão
    sensibilidade_fixo = st.session_state.sensibilidade_fixo
    sensibilidade_variavel = st.session_state.sensibilidade_variavel

# ========== MODO INFLAÇÃO ==========
st.sidebar.markdown("### 📈 Inflação")
modo_inflacao = st.sidebar.radio(
    "Modo de Inflação:",
    options=["Global", "Detalhado"],
    index=0 if st.session_state.modo_inflacao == "Global" else 1,
    key="radio_modo_inf",
    help="Global: mesma inflação para todos | Detalhado: configuração por categoria"
)
st.session_state.modo_inflacao = modo_inflacao

if modo_inflacao == "Global":
    # Botões de inflação
    col_inf1, col_inf2, col_inf3 = st.sidebar.columns(3)
    with col_inf1:
        if st.button("📈 0%", key="btn_inf_0"):
            st.session_state.inflacao = 0.0
    with col_inf2:
        if st.button("📈 3%", key="btn_inf_3"):
            st.session_state.inflacao = 3.0
    with col_inf3:
        if st.button("📈 5%", key="btn_inf_5"):
            st.session_state.inflacao = 5.0

    inflacao = st.sidebar.number_input(
        "Inflação Global (%)",
        min_value=0.0,
        max_value=100.0,
        value=st.session_state.inflacao,
        step=0.5,
        key="input_inflacao",
        help="Inflação aplicada globalmente ao cálculo do FLEX"
    )
    st.session_state.inflacao = inflacao
else:
    st.sidebar.info("⚙️ Configure inflações detalhadas na seção principal")
    # Usar valor do session_state como padrão
    inflacao = st.session_state.inflacao

st.sidebar.markdown("---")

# Filtro 1: Oficina
if 'Oficina' in df_base.columns:
    oficina_opcoes = ["Todos"] + sorted(df_base['Oficina'].dropna().astype(str).unique().tolist())
    oficina_selecionada = st.sidebar.multiselect("Selecione a OFICINA:", oficina_opcoes, default=["Todos"])
    
    if "Todos" in oficina_selecionada or not oficina_selecionada:
        df_filtrado = df_base.copy()
    else:
        df_filtrado = df_base[df_base['Oficina'].astype(str).isin(oficina_selecionada)]
else:
    df_filtrado = df_base.copy()

# Filtro 2: Período
if 'Período' in df_filtrado.columns:
    periodo_opcoes = ["Todos"] + sorted(df_filtrado['Período'].dropna().astype(str).unique().tolist())
    periodo_selecionado = st.sidebar.selectbox("Selecione o Período:", periodo_opcoes)
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Período'].astype(str) == str(periodo_selecionado)]

# Filtro 3: Veículo
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = ["Todos"] + sorted(df_filtrado['Veículo'].dropna().astype(str).unique().tolist())
    veiculo_selecionado = st.sidebar.multiselect("Selecione o VEÍCULO:", veiculo_opcoes, default=["Todos"])
    if veiculo_selecionado and "Todos" not in veiculo_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Veículo'].astype(str).isin(veiculo_selecionado)]

# Filtro 4: Tipo de Custo
if 'Custo' in df_filtrado.columns:
    custo_opcoes = ["Todos"] + sorted(df_filtrado['Custo'].dropna().astype(str).unique().tolist())
    custo_selecionado = st.sidebar.multiselect("Selecione o TIPO DE CUSTO:", custo_opcoes, default=["Todos"])
    if custo_selecionado and "Todos" not in custo_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Custo'].astype(str).isin(custo_selecionado)]

# Cache para opções de filtros (otimização de performance)
@st.cache_data(ttl=1800, max_entries=3)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache para melhor performance"""
    if column_name in df.columns:
        return ["Todos"] + sorted(df[column_name].dropna().astype(str).unique().tolist())
    return ["Todos"]

# Filtros principais (com cache otimizado)
filtros_principais = [
    ("Type 05", "Type 05", "multiselect"),
    ("Type 06", "Type 06", "multiselect"), 
    ("Type 07", "Type 07", "multiselect"),
    ("Account", "Account", "multiselect")
]

for col_name, label, widget_type in filtros_principais:
    if col_name in df_filtrado.columns:
        opcoes = get_filter_options(df_filtrado, col_name)
        if widget_type == "multiselect":
            selecionadas = st.sidebar.multiselect(f"Selecione o {label}:", opcoes, default=["Todos"])
            if selecionadas and "Todos" not in selecionadas:
                df_filtrado = df_filtrado[df_filtrado[col_name].astype(str).isin(selecionadas)]

# Filtro 5: Ano (VISÍVEL na sidebar principal)
if 'Ano' in df_filtrado.columns:
    ano_opcoes = ["Todos"] + sorted(df_filtrado['Ano'].dropna().astype(str).unique().tolist())
    ano_selecionado = st.sidebar.multiselect("Selecione o ANO:", ano_opcoes, default=["Todos"])
    if ano_selecionado and "Todos" not in ano_selecionado:
        df_filtrado = df_filtrado[df_filtrado['Ano'].astype(str).isin(ano_selecionado)]

# Filtros avançados (expansível)
with st.sidebar.expander("🔍 Filtros Avançados"):
    st.info("Filtros adicionais aparecerão aqui conforme necessário")

# Exibir informações dos filtros
st.sidebar.write(f"Número de linhas: {df_filtrado.shape[0]:,}")
st.sidebar.write(f"Número de colunas: {df_filtrado.shape[1]}")
if 'Total' in df_filtrado.columns:
    st.sidebar.write(f"Soma do Valor total: R$ {df_filtrado['Total'].sum():,.2f}")

# --- Configurações do waterfall ---
# Criar coluna Período_Ano para diferenciar meses de anos diferentes
if 'Período' in df_filtrado.columns and 'Ano' in df_filtrado.columns:
    # Criar uma coluna combinada Período + Ano
    df_filtrado['Período_Ano'] = df_filtrado['Período'].astype(str) + ' ' + df_filtrado['Ano'].astype(str)
    col_mes = 'Período_Ano'
    mes_unicos = sorted(df_filtrado['Período_Ano'].dropna().unique().tolist())
elif 'Período' in df_filtrado.columns:
    col_mes = 'Período'
    mes_unicos = sort_mes_unique(df_filtrado["Período"].astype(str))
else:
    col_mes = None
    mes_unicos = []

col_valor = next((c for c in ["Total", "total", "Valor", "valor"] if c in df_filtrado.columns), None)

# Dimensão de categoria no mesmo padrão
dims_cat = [c for c in ["Type 05", "Type 06", "Type 07", "Oficina", "Veículo", "Custo", "Account"] if c in df_filtrado.columns]
if not dims_cat or not col_valor or not col_mes:
    st.error("❌ Colunas necessárias não encontradas.")
    st.info(f"Colunas disponíveis: {', '.join(df_filtrado.columns.tolist())}")
    st.info(f"Colunas necessárias: Período, Total, e pelo menos uma dimensão de categoria")
    st.stop()

# ============================================================================
# 📊 ANÁLISE WATERFALL
# ============================================================================

# --- Configurações da análise ---
df_segunda_analise = df_filtrado.copy()
chosen_dim_2 = st.selectbox("Dimensão da categoria:", dims_cat, index=min(1, len(dims_cat)-1), key="dim_2")

# ========== CONFIGURAÇÕES DETALHADAS (se ativado) ==========
# Usar session_state diretamente para garantir que os valores estejam atualizados
modo_sensibilidade_atual = st.session_state.get('modo_sensibilidade', 'Global')
modo_inflacao_atual = st.session_state.get('modo_inflacao', 'Global')

# Inicializar dicionários de valores temporários (igual ao Forecast)
if 'valores_temp_sens_fixo_2' not in st.session_state:
    st.session_state.valores_temp_sens_fixo_2 = {}
if 'valores_temp_sens_variavel_2' not in st.session_state:
    st.session_state.valores_temp_sens_variavel_2 = {}
if 'valores_temp_infl_2' not in st.session_state:
    st.session_state.valores_temp_infl_2 = {}
if 'widget_key_counter_2' not in st.session_state:
    st.session_state.widget_key_counter_2 = 0

dict_sens_fixo_2 = {}
dict_sens_variavel_2 = {}
dict_inflacao_2 = {}

# Mostrar status dos modos ativos
col_status1, col_status2 = st.columns(2)
with col_status1:
    if modo_sensibilidade_atual == "Detalhado":
        st.info("🎯 **Modo Sensibilidade:** Detalhado")
    else:
        st.success("🎯 **Modo Sensibilidade:** Global")
with col_status2:
    if modo_inflacao_atual == "Detalhado":
        st.info("📈 **Modo Inflação:** Detalhado")
    else:
        st.success("📈 **Modo Inflação:** Global")

# Exibir expander de configurações detalhadas
if modo_sensibilidade_atual == "Detalhado" or modo_inflacao_atual == "Detalhado":
    st.markdown("---")
    with st.expander("⚙️ Configurações Detalhadas por Categoria", expanded=True):
        st.markdown("Configure sensibilidade e inflação específicas para cada categoria")
        
        # Verificar se a dimensão foi selecionada
        if chosen_dim_2 and chosen_dim_2 in df_segunda_analise.columns:
            categorias_unicas = sorted(df_segunda_analise[chosen_dim_2].dropna().astype(str).unique().tolist())
            
            if not categorias_unicas:
                st.warning("⚠️ Nenhuma categoria encontrada para a dimensão selecionada.")
            else:
                st.info(f"📊 Configurando para **{len(categorias_unicas)}** categorias da dimensão **{chosen_dim_2}**")
                
                # Cabeçalho da tabela - 4 colunas: Categoria, Custo Fixo, Custo Variável, Inflação
                st.markdown("---")
                col_header0, col_header1, col_header2, col_header3 = st.columns([2, 2, 2, 2])
                with col_header0:
                    st.markdown("**Categoria**")
                with col_header1:
                    st.markdown("**Custo Fixo**")
                with col_header2:
                    st.markdown("**Custo Variável**")
                with col_header3:
                    st.markdown("**Inflação %**")
                st.markdown("---")
                
                # Criar linha para cada categoria com 4 colunas alinhadas
                for cat in categorias_unicas:
                    col_cat, col_fixo, col_var, col_infl = st.columns([2, 2, 2, 2])
                    
                    # Coluna 0: Nome da categoria
                    with col_cat:
                        st.markdown(f"<small><b>{cat}</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 1: Sensibilidade Custo Fixo
                    with col_fixo:
                        if modo_sensibilidade_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global
                            valor_padrao_sens_fixo = st.session_state.valores_temp_sens_fixo_2.get(str(cat), sensibilidade_fixo)
                            
                            sens_fixo = st.slider(
                                f"Sensibilidade Fixo",
                                min_value=0.0,
                                max_value=1.0,
                                value=valor_padrao_sens_fixo,
                                step=0.1,
                                key=f"sens_fixo_{cat}_2",
                                label_visibility="collapsed",
                                help=f"Sensibilidade do custo fixo para {cat}"
                            )
                            dict_sens_fixo_2[str(cat)] = sens_fixo
                            # Atualizar valor temporário
                            st.session_state.valores_temp_sens_fixo_2[str(cat)] = sens_fixo
                        else:
                            st.markdown(f"<small><b>{sensibilidade_fixo*100:.0f}%</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 2: Sensibilidade Custo Variável
                    with col_var:
                        if modo_sensibilidade_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global
                            valor_padrao_sens_var = st.session_state.valores_temp_sens_variavel_2.get(str(cat), sensibilidade_variavel)
                            
                            sens_var = st.slider(
                                f"Sensibilidade Variável",
                                min_value=0.0,
                                max_value=1.0,
                                value=valor_padrao_sens_var,
                                step=0.1,
                                key=f"sens_var_{cat}_2",
                                label_visibility="collapsed",
                                help=f"Sensibilidade do custo variável para {cat}"
                            )
                            dict_sens_variavel_2[str(cat)] = sens_var
                            # Atualizar valor temporário
                            st.session_state.valores_temp_sens_variavel_2[str(cat)] = sens_var
                        else:
                            st.markdown(f"<small><b>{sensibilidade_variavel*100:.0f}%</b></small>", unsafe_allow_html=True)
                    
                    # Coluna 3: Inflação
                    with col_infl:
                        if modo_inflacao_atual == "Detalhado":
                            # Obter valor padrão do session_state ou usar valor global (igual ao Forecast)
                            valor_padrao_infl = st.session_state.valores_temp_infl_2.get(str(cat), inflacao)
                            
                            # Usar contador de widget para forçar recriação quando necessário (igual ao Forecast)
                            widget_key = f"infl_{cat}_2_{st.session_state.widget_key_counter_2}"
                            
                            inflacao_cat = st.number_input(
                                "",
                                min_value=0.0,
                                max_value=100.0,
                                value=valor_padrao_infl,
                                step=0.5,
                                format="%.2f",
                                key=widget_key,
                                help=f"Inflação (%) para {cat}"
                            )
                            dict_inflacao_2[str(cat)] = inflacao_cat
                            # Atualizar valor temporário
                            st.session_state.valores_temp_infl_2[str(cat)] = inflacao_cat
                        else:
                            # Mostrar valor global quando não está em modo detalhado
                            st.markdown(f"<small><b>{inflacao:.2f}%</b></small>", unsafe_allow_html=True)
                    
                    st.markdown("---")
        else:
            st.warning("⚠️ Selecione uma dimensão de categoria acima para configurar valores detalhados.")
    
    st.markdown("---")

# Modo de comparação
st.markdown("### 📅 Modo de Comparação")
modo_comparacao = st.radio(
    "Tipo de comparação:",
    options=["Mês a Mês", "Ano a Ano", "Semestre", "Quarter", "Múltiplos Meses"],
    index=0,
    key="modo_comparacao",
    help="Mês a Mês: compara dois meses | Ano a Ano: compara totais anuais | Semestre: compara semestres | Quarter: compara trimestres | Múltiplos Meses: compara vários meses"
)

st.markdown("---")

if modo_comparacao == "Mês a Mês":
    # Modo original: dois meses
    col_a2, col_b2 = st.columns(2)
    with col_a2:
        mes_inicial_2 = st.selectbox("Mês inicial:", mes_unicos, index=0 if mes_unicos else None, key="mes_inicial_2")
    with col_b2:
        mes_final_2 = st.selectbox("Mês final:", mes_unicos, index=len(mes_unicos) - 1 if mes_unicos else None, key="mes_final_2")
    meses_selecionados_2 = [mes_inicial_2, mes_final_2] if mes_inicial_2 and mes_final_2 else []
    
elif modo_comparacao == "Ano a Ano":
    # Comparação ano a ano
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 2:
            col_ano1, col_ano2 = st.columns(2)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1), key="ano_final")
            mes_inicial_2 = f"Total {ano_inicial}"
            mes_final_2 = f"Total {ano_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 2 anos de dados para comparação ano a ano.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação ano a ano.")
        st.stop()

elif modo_comparacao == "Semestre":
    # Comparação semestre a semestre
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 1:
            col_ano1, col_sem1, col_ano2, col_sem2 = st.columns(4)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_sem")
            with col_sem1:
                semestre_inicial = st.selectbox("Semestre inicial:", [1, 2], index=0, key="semestre_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_sem")
            with col_sem2:
                semestre_final = st.selectbox("Semestre final:", [1, 2], index=1, key="semestre_final")
            mes_inicial_2 = f"{ano_inicial} S{semestre_inicial}"
            mes_final_2 = f"{ano_final} S{semestre_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de semestres.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação de semestres.")
        st.stop()

elif modo_comparacao == "Quarter":
    # Comparação quarter a quarter
    if 'Ano' in df_segunda_analise.columns:
        anos_disponiveis = sorted(df_segunda_analise['Ano'].dropna().unique().tolist())
        if len(anos_disponiveis) >= 1:
            col_ano1, col_q1, col_ano2, col_q2 = st.columns(4)
            with col_ano1:
                ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_q")
            with col_q1:
                trimestre_inicial = st.selectbox("Quarter inicial:", [1, 2, 3, 4], index=0, key="trimestre_inicial")
            with col_ano2:
                ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_q")
            with col_q2:
                trimestre_final = st.selectbox("Quarter final:", [1, 2, 3, 4], index=1, key="trimestre_final")
            mes_inicial_2 = f"{ano_inicial} Q{trimestre_inicial}"
            mes_final_2 = f"{ano_final} Q{trimestre_final}"
            meses_selecionados_2 = [mes_inicial_2, mes_final_2]
        else:
            st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de quarters.")
            st.stop()
    else:
        st.warning("⚠️ Coluna 'Ano' não encontrada. Não é possível fazer comparação de quarters.")
        st.stop()
        
else:  # Múltiplos Meses
    meses_selecionados_2 = st.multiselect(
        "Selecione os meses para comparação:",
        mes_unicos,
        default=mes_unicos[:min(3, len(mes_unicos))] if mes_unicos else [],
        key="meses_multiplos"
    )
    if len(meses_selecionados_2) < 2:
        st.warning("⚠️ Selecione pelo menos 2 meses para comparação.")
        st.stop()
    mes_inicial_2 = meses_selecionados_2[0]
    mes_final_2 = meses_selecionados_2[-1]

# Normalizar categorias (strings limpas) e garantir defaults válidos
cats_all_2 = sorted([str(x).strip() for x in df_segunda_analise[chosen_dim_2].dropna().unique().tolist() if str(x).strip() != ""])
total_cats_2 = max(1, len(cats_all_2))
max_cats_2 = st.slider(f"Quantidade de categorias a exibir (Top N) (Total: {total_cats_2}):", 1, total_cats_2, min(total_cats_2, 20), key="max_cats_2")
vol_mf_2 = (df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)].groupby(chosen_dim_2)[col_valor].sum().sort_values(ascending=False))
vol_index_2 = [str(c).strip() for c in list(vol_mf_2.index)]
default_cats_2 = vol_index_2[:max_cats_2] if len(vol_index_2) else cats_all_2[:max_cats_2]

cats_options_2 = ["Todos"] + cats_all_2
# Filtrar defaults não presentes; fallback seguro
default_cats_2 = [c for c in default_cats_2 if c in cats_all_2]
if not default_cats_2:
    default_cats_2 = cats_all_2[:min(10, len(cats_all_2))]

cats_sel_raw_2 = st.multiselect("Categorias (uma ou mais):", cats_options_2, default=default_cats_2, key="cats_2")
if (not cats_sel_raw_2) or ("Todos" in cats_sel_raw_2):
    cats_sel_2 = cats_all_2[:max_cats_2] if max_cats_2 < len(cats_all_2) else cats_all_2
else:
    cats_sel_2 = cats_sel_raw_2

# Calcular totais baseado no modo de comparação
if modo_comparacao == "Ano a Ano":
    # Tratar anos como períodos únicos (mesma lógica de Mês a Mês)
    # Agrupar por ano e calcular totais
    df_ano_inicial = df_segunda_analise[df_segunda_analise['Ano'].astype(str) == str(ano_inicial)]
    df_ano_final = df_segunda_analise[df_segunda_analise['Ano'].astype(str) == str(ano_final)]
    total_m1_all_2 = float(df_ano_inicial[col_valor].sum())
    total_m2_all_2 = float(df_ano_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada ano (mesma lógica de Mês a Mês)
    meses_ano_inicial = sorted(df_ano_inicial[col_mes].dropna().unique().tolist())
    meses_ano_final = sorted(df_ano_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_ano_inicial[0] if meses_ano_inicial else None
    mes_final_flex = meses_ano_final[-1] if meses_ano_final else None
    # Definir mes_inicial_2 e mes_final_2 como os anos para manter compatibilidade
    mes_inicial_2 = f"Ano {ano_inicial}"
    mes_final_2 = f"Ano {ano_final}"

elif modo_comparacao == "Semestre":
    # Tratar semestres como períodos únicos
    meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                     2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
    meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
    meses_sem_final = meses_semestre.get(semestre_final, [])
    
    df_sem_inicial = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_inicial)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial]))
    ]
    df_sem_final = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_final)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_final]))
    ]
    total_m1_all_2 = float(df_sem_inicial[col_valor].sum())
    total_m2_all_2 = float(df_sem_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada semestre
    meses_sem_inicial_list = sorted(df_sem_inicial[col_mes].dropna().unique().tolist())
    meses_sem_final_list = sorted(df_sem_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_sem_inicial_list[0] if meses_sem_inicial_list else None
    mes_final_flex = meses_sem_final_list[-1] if meses_sem_final_list else None

elif modo_comparacao == "Quarter":
    # Tratar quarters como períodos únicos
    meses_trimestre = {
        1: ['janeiro', 'fevereiro', 'março'],
        2: ['abril', 'maio', 'junho'],
        3: ['julho', 'agosto', 'setembro'],
        4: ['outubro', 'novembro', 'dezembro']
    }
    meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
    meses_trim_final = meses_trimestre.get(trimestre_final, [])
    
    df_trim_inicial = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_inicial)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial]))
    ]
    df_trim_final = df_segunda_analise[
        (df_segunda_analise['Ano'].astype(str) == str(ano_final)) &
            (df_segunda_analise[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_final]))
    ]
    total_m1_all_2 = float(df_trim_inicial[col_valor].sum())
    total_m2_all_2 = float(df_trim_final[col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    # Para FLEX, usar o primeiro e último mês de cada quarter
    meses_trim_inicial_list = sorted(df_trim_inicial[col_mes].dropna().unique().tolist())
    meses_trim_final_list = sorted(df_trim_final[col_mes].dropna().unique().tolist())
    mes_inicial_flex = meses_trim_inicial_list[0] if meses_trim_inicial_list else None
    mes_final_flex = meses_trim_final_list[-1] if meses_trim_final_list else None

elif modo_comparacao == "Múltiplos Meses":
    # Para múltiplos meses, calcular do primeiro ao último
    total_m1_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_inicial_2)][col_valor].sum())
    total_m2_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)][col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    mes_inicial_flex = mes_inicial_2
    mes_final_flex = mes_final_2
else:  # Mês a Mês
    # Calcular totais (validação será feita depois)
    total_m1_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_inicial_2)][col_valor].sum())
    total_m2_all_2 = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes_final_2)][col_valor].sum())
    change_all_2 = total_m2_all_2 - total_m1_all_2
    mes_inicial_flex = mes_inicial_2
    mes_final_flex = mes_final_2

# Validar que os períodos são diferentes (mesma lógica para todos os modos)
if modo_comparacao == "Ano a Ano":
    if ano_inicial == ano_final:
        st.info("⚠️ Selecione anos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Semestre":
    if ano_inicial == ano_final and semestre_inicial == semestre_final:
        st.info("⚠️ Selecione períodos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Quarter":
    if ano_inicial == ano_final and trimestre_inicial == trimestre_final:
        st.info("⚠️ Selecione períodos diferentes para comparar.")
        st.stop()
elif modo_comparacao == "Mês a Mês":
    if mes_inicial_2 == mes_final_2:
        st.info("⚠️ Selecione meses diferentes para comparar.")
        st.stop()

# Processar análise se períodos são diferentes
if (modo_comparacao == "Ano a Ano" and ano_inicial != ano_final) or \
   (modo_comparacao == "Semestre" and (ano_inicial != ano_final or semestre_inicial != semestre_final)) or \
   (modo_comparacao == "Quarter" and (ano_inicial != ano_final or trimestre_inicial != trimestre_final)) or \
   (modo_comparacao == "Mês a Mês" and mes_inicial_2 != mes_final_2) or \
   modo_comparacao == "Múltiplos Meses":
    
    # Calcular FLEX para segunda análise (separado em Volume e Inflação)
    # Usar session_state diretamente para garantir valores atualizados
    modo_sensibilidade_atual = st.session_state.get('modo_sensibilidade', 'Global')
    modo_inflacao_atual = st.session_state.get('modo_inflacao', 'Global')
    
    # Calcular FLEX para Mês a Mês, Ano a Ano, Semestre e Quarter
    if (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]) and mes_inicial_flex and mes_final_flex:
        # Para períodos agregados (Ano, Semestre, Quarter), passar parâmetros específicos
        # Para Mês a Mês, passar None para usar volumes dos meses específicos
        flex_volume_2, flex_inflacao_2 = calcular_flex(
            df_segunda_analise, df_volume, mes_inicial_flex, mes_final_flex, col_mes, col_valor,
            sensibilidade_fixo, sensibilidade_variavel, inflacao,
            modo_sensibilidade_atual, dict_sens_fixo_2, dict_sens_variavel_2,
            modo_inflacao_atual, dict_inflacao_2, chosen_dim_2,
            modo_comparacao,
            ano_inicial if modo_comparacao in ["Ano a Ano", "Semestre", "Quarter"] else None,
            ano_final if modo_comparacao in ["Ano a Ano", "Semestre", "Quarter"] else None,
            semestre_inicial if modo_comparacao == "Semestre" else None,
            semestre_final if modo_comparacao == "Semestre" else None,
            trimestre_inicial if modo_comparacao == "Quarter" else None,
            trimestre_final if modo_comparacao == "Quarter" else None
        )
    else:
        flex_volume_2 = 0.0
        flex_inflacao_2 = 0.0
    flex_total_2 = flex_volume_2 + flex_inflacao_2

    # Filtrar pelas selecionadas
    dff_2 = df_segunda_analise[df_segunda_analise[chosen_dim_2].astype(str).isin(cats_sel_2)].copy()

    # Calcular grupos - mesma lógica para todos os modos
    if modo_comparacao == "Ano a Ano":
        # Agrupar por ano
        g1_2 = (dff_2[dff_2['Ano'].astype(str) == str(ano_inicial)].groupby(chosen_dim_2)[col_valor].sum())
        g2_2 = (dff_2[dff_2['Ano'].astype(str) == str(ano_final)].groupby(chosen_dim_2)[col_valor].sum())
    elif modo_comparacao == "Semestre":
        # Agrupar por semestre
        meses_semestre = {1: ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho'],
                         2: ['julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']}
        meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
        meses_sem_final = meses_semestre.get(semestre_final, [])
        df_g1 = dff_2[
            (dff_2['Ano'].astype(str) == str(ano_inicial)) &
            (dff_2[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_inicial]))
        ]
        df_g2 = dff_2[
            (dff_2['Ano'].astype(str) == str(ano_final)) &
            (dff_2[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_sem_final]))
        ]
        g1_2 = df_g1.groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = df_g2.groupby(chosen_dim_2)[col_valor].sum()
    elif modo_comparacao == "Quarter":
        # Agrupar por quarter
        meses_trimestre = {
            1: ['janeiro', 'fevereiro', 'março'],
            2: ['abril', 'maio', 'junho'],
            3: ['julho', 'agosto', 'setembro'],
            4: ['outubro', 'novembro', 'dezembro']
        }
        meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
        meses_trim_final = meses_trimestre.get(trimestre_final, [])
        df_g1 = dff_2[
            (dff_2['Ano'].astype(str) == str(ano_inicial)) &
            (dff_2[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_inicial]))
        ]
        df_g2 = dff_2[
            (dff_2['Ano'].astype(str) == str(ano_final)) &
            (dff_2[col_mes].astype(str).str.lower().str.split(' ', n=1).str[0].isin([m.lower() for m in meses_trim_final]))
        ]
        g1_2 = df_g1.groupby(chosen_dim_2)[col_valor].sum()
        g2_2 = df_g2.groupby(chosen_dim_2)[col_valor].sum()
    else:
        # Mês a Mês ou Múltiplos Meses
        g1_2 = (dff_2[dff_2[col_mes].astype(str) == str(mes_inicial_2)].groupby(chosen_dim_2)[col_valor].sum())
        g2_2 = (dff_2[dff_2[col_mes].astype(str) == str(mes_final_2)].groupby(chosen_dim_2)[col_valor].sum())

    labels_cats_2, values_cats_2 = [], []
    for cat in sorted(set(g1_2.index).union(set(g2_2.index))):
        delta = float(g2_2.get(cat, 0.0)) - float(g1_2.get(cat, 0.0))
        if abs(delta) > 1e-9:
            labels_cats_2.append(str(cat))
            values_cats_2.append(delta)

    original_len_2 = len(labels_cats_2)
    if len(labels_cats_2) > max_cats_2:
        idx = sorted(range(len(values_cats_2)), key=lambda i: abs(values_cats_2[i]), reverse=True)[:max_cats_2]
        labels_cats_2 = [labels_cats_2[i] for i in idx]
        values_cats_2 = [values_cats_2[i] for i in idx]
    cropped_2 = len(labels_cats_2) < original_len_2

    # Calcular remainder baseado no modo de comparação
    if modo_comparacao == "Múltiplos Meses":
        # Para múltiplos meses, o remainder será calculado depois de incluir as variações dos meses intermediários
        # Por enquanto, calcular apenas com base nas categorias
        remainder_2 = round(change_all_2 - sum(values_cats_2) - flex_total_2, 2)
    else:
        remainder_2 = round(change_all_2 - sum(values_cats_2) - flex_total_2, 2)
    
    all_selected_2 = len(cats_sel_2) >= len(cats_all_2)
    show_outros_2 = (abs(remainder_2) >= 0.01) and (cropped_2 or not all_selected_2 or len(cats_sel_2) < len(cats_all_2))
    if show_outros_2:
        labels_cats_2.append("Outros")
        values_cats_2.append(remainder_2)

    # Inserir FLEX VOLUME e FLEX INFLAÇÃO após o período inicial e antes das categorias
    # (para Mês a Mês, Ano a Ano, Semestre e Quarter, quando houver valores de FLEX)
    if modo_comparacao == "Múltiplos Meses":
        # Para múltiplos meses, não incluir FLEX - usar estrutura com barras azuis para cada mês
        # Estrutura desejada: Barra Azul (Mês1) -> Categorias Mês1 -> Variação -> Barra Azul (Mês2) -> Categorias Mês2 -> Variação -> Barra Azul (Mês3) -> Categorias Mês3
        # Cada mês deve ter suas próprias categorias logo após sua barra azul
        
        # Construir labels e valores para todos os meses
        labels_meses_completos = []
        valores_meses_completos = []
        medidas_meses_completos = []
        
        # Primeiro mês: barra azul inicial (absolute)
        labels_meses_completos.append(f"{mes_inicial_2}")
        valores_meses_completos.append(total_m1_all_2)
        medidas_meses_completos.append("absolute")
        
        # As categorias serão calculadas como diferença entre o primeiro e último mês
        # Elas aparecerão logo após o primeiro mês para mostrar a composição
        # Usar as categorias já calculadas (labels_cats_2, values_cats_2) que são a diferença entre primeiro e último
        # Adicionar categorias logo após o primeiro mês
        labels_meses_completos.extend(labels_cats_2)
        valores_meses_completos.extend(values_cats_2)
        medidas_meses_completos.extend(["relative"] * len(labels_cats_2))
        
        total_anterior = total_m1_all_2 + sum(values_cats_2)
        
        # Para cada mês intermediário (do segundo ao penúltimo)
        for idx, mes in enumerate(meses_selecionados_2[1:-1]):
            total_mes = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes)][col_valor].sum())
            
            # Calcular variação do mês anterior para este mês
            variacao = total_mes - total_anterior
            
            # Adicionar variação (relative) - barra verde/vermelha
            labels_meses_completos.append(f"Δ {mes}")
            valores_meses_completos.append(variacao)
            medidas_meses_completos.append("relative")
            
            # Adicionar mês como barra azul (total) - barra azul
            labels_meses_completos.append(f"{mes}")
            valores_meses_completos.append(total_mes)
            medidas_meses_completos.append("total")
            
            # Não adicionar categorias para meses intermediários
            # As categorias já foram adicionadas após o primeiro mês
            # Atualizar para próximo cálculo
            total_anterior = total_mes
        
        # Se houver apenas 2 meses, não adicionar variação intermediária
        # Se houver 3 ou mais meses, adicionar variação do penúltimo para o último
        if len(meses_selecionados_2) > 2:
            # Calcular variação do penúltimo para o último mês
            variacao_final = total_m2_all_2 - total_anterior
            labels_meses_completos.append(f"Δ {mes_final_2}")
            valores_meses_completos.append(variacao_final)
            medidas_meses_completos.append("relative")
        
        # Adicionar último mês como barra azul (total)
        labels_meses_completos.append(f"{mes_final_2}")
        valores_meses_completos.append(total_m2_all_2)
        medidas_meses_completos.append("total")
        
        # Montar estrutura final
        labels_2 = labels_meses_completos
        values_2 = valores_meses_completos
        measures_2 = medidas_meses_completos
        
        # Para múltiplos meses, não usar remainder tradicional, pois cada mês tem suas próprias categorias
        remainder_2 = 0
    elif (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]) and (abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01):
        # Modos com FLEX: Mês a Mês, Ano a Ano, Semestre, Quarter (quando há FLEX)
        labels_2 = [f"{mes_inicial_2}", "Flex Volume", "Flex Inflação"] + labels_cats_2 + [f"{mes_final_2}"]
        values_2 = [total_m1_all_2, flex_volume_2, flex_inflacao_2] + values_cats_2 + [total_m2_all_2]
        measures_2 = ["absolute", "relative", "relative"] + ["relative"] * len(values_cats_2) + ["total"]
    else:
        # Modos sem FLEX ou quando FLEX é zero: Mês a Mês, Ano a Ano, Semestre, Quarter (sem FLEX)
        # Estrutura: Mês Inicial -> Categorias -> Mês Final (SEM deltas)
        labels_2 = [f"{mes_inicial_2}"] + labels_cats_2 + [f"{mes_final_2}"]
        values_2 = [total_m1_all_2] + values_cats_2 + [total_m2_all_2]
        measures_2 = ["absolute"] + ["relative"] * len(values_cats_2) + ["total"]

    # Tema do Streamlit para cores
    theme_base = st.get_option("theme.base") or "light"
    text_color = st.get_option("theme.textColor") or ("#FAFAFA" if theme_base == "dark" else "#000000")
    grid_color = "rgba(255,255,255,0.12)" if theme_base == "dark" else "rgba(0,0,0,0.12)"
    connector_color = "rgba(255,255,255,0.35)" if theme_base == "dark" else "rgba(0,0,0,0.35)"
    
    # Criar gráfico waterfall 2
    fig_2 = go.Figure(go.Waterfall(
        name="Waterfall",
        orientation="v",
        measure=measures_2,
        x=labels_2,
        y=values_2,
        textposition="outside",
        text=[f"R$ {v:,.0f}" for v in values_2],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#e74c3c"}},
        decreasing={"marker": {"color": "#27ae60"}},
        totals={"marker": {"color": "#3498db"}},
        hovertemplate="<b>%{x}</b><br>Valor: R$ %{y:,.2f}<extra></extra>"
    ))

    fig_2.update_traces(textfont=dict(color=text_color, size=10))

    # Adicionar overlay para colorir FLEX VOLUME, FLEX INFLAÇÃO e Outros
    # Para FLEX VOLUME (roxo) - para Mês a Mês, Ano a Ano, Semestre e Quarter
    if (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]) and abs(flex_volume_2) > 0.01:
        flex_pos_volume = total_m1_all_2
        fig_2.add_trace(go.Bar(
            x=['Flex Volume'],
            y=[abs(flex_volume_2)],
            base=[flex_pos_volume if flex_volume_2 >= 0 else flex_pos_volume + flex_volume_2],
            marker_color='#9b59b6',  # Roxo
            opacity=1.0,
            hovertemplate="<b>Flex Volume</b><br>Valor: R$ %{y:,.2f}<br>Efeito de Volume + Sensibilidade<extra></extra>",
            showlegend=False,
            name='Flex Volume',
            text=f"R$ {flex_volume_2:,.0f}",
            textposition="outside",
            textfont=dict(color=text_color, size=10)
        ))
    
    # Para FLEX INFLAÇÃO (laranja claro) - para Mês a Mês, Ano a Ano, Semestre e Quarter
    if (modo_comparacao in ["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"]) and abs(flex_inflacao_2) > 0.01:
        flex_pos_inflacao = total_m1_all_2 + flex_volume_2
        fig_2.add_trace(go.Bar(
            x=['Flex Inflação'],
            y=[abs(flex_inflacao_2)],
            base=[flex_pos_inflacao if flex_inflacao_2 >= 0 else flex_pos_inflacao + flex_inflacao_2],
            marker_color='#f39c12',  # Laranja claro
            opacity=1.0,
            hovertemplate="<b>Flex Inflação</b><br>Valor: R$ %{y:,.2f}<br>Efeito da Inflação<extra></extra>",
            showlegend=False,
            name='Flex Inflação',
            text=f"R$ {flex_inflacao_2:,.0f}",
            textposition="outside",
            textfont=dict(color=text_color, size=10)
        ))
    
    # Para Outros (laranja escuro)
    if show_outros_2:
        prev_sum_2 = sum(v for lab, v in zip(labels_cats_2, values_cats_2) if lab != "Outros")
        cum_before_2 = total_m1_all_2 + flex_volume_2 + flex_inflacao_2 + prev_sum_2
        base_val_2 = cum_before_2 if remainder_2 >= 0 else cum_before_2 + remainder_2
        height_2 = abs(remainder_2)
        fig_2.add_trace(go.Bar(
            x=['Outros'], 
            y=[height_2], 
            base=[base_val_2], 
            marker_color='#e67e22',  # Laranja escuro
            opacity=1.0,
            hovertemplate="<b>Outros</b><br>Valor: R$ %{y:,.2f}<extra></extra>",
            showlegend=False,
            name='Outros',
            text=f"R$ {remainder_2:,.0f}",
            textposition="outside",
            textfont=dict(color=text_color, size=10)
        ))
    
    # Definir barmode como overlay para sobrepor as barras customizadas
    fig_2.update_layout(barmode='overlay')

    if theme_base == "dark":
        fig_2.update_layout(template="plotly_dark")
    else:
        fig_2.update_layout(template="plotly_white")

    # Título baseado no modo de comparação
    if modo_comparacao == "Ano a Ano":
        titulo_grafico = f"Análise Waterfall - Ano {ano_inicial} vs Ano {ano_final}"
    elif modo_comparacao == "Semestre":
        titulo_grafico = f"Análise Waterfall - {ano_inicial} S{semestre_inicial} vs {ano_final} S{semestre_final}"
    elif modo_comparacao == "Quarter":
        titulo_grafico = f"Análise Waterfall - {ano_inicial} Q{trimestre_inicial} vs {ano_final} Q{trimestre_final}"
    elif modo_comparacao == "Múltiplos Meses":
        titulo_grafico = f"Análise Waterfall - {len(meses_selecionados_2)} Meses Selecionados"
    else:
        titulo_grafico = f"Análise Waterfall - {mes_inicial_2} para {mes_final_2}"
    
    fig_2.update_layout(
        title={"text": titulo_grafico, "x": 0.5},
        xaxis_title="Mês / Categoria",
        yaxis_title="Valor (R$)",
        height=560,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=text_color),
        xaxis=dict(gridcolor=grid_color, zerolinecolor=grid_color, linecolor=grid_color),
        yaxis=dict(gridcolor=grid_color, zerolinecolor=grid_color, linecolor=grid_color),
    )

    fig_2.update_yaxes(tickformat=",.0f", tickprefix="R$ ")

    st.plotly_chart(fig_2, use_container_width=True)

    # Exibir informações resumidas da análise
    st.markdown("---")
    # Reduzir tamanho da fonte das métricas
    st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 0.9em !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.85em !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.8em !important;
    }
    </style>
    """, unsafe_allow_html=True)
    if modo_comparacao == "Ano a Ano":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total Ano {ano_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric("FLEX Volume", f"R$ {flex_volume_2:,.2f}",
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", f"R$ {flex_inflacao_2:,.2f}",
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total Ano {ano_final}", f"R$ {total_m2_all_2:,.2f}")
            with col5_2:
                st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total Ano {ano_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric(f"Total Ano {ano_final}", f"R$ {total_m2_all_2:,.2f}")
            st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Semestre":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total {ano_inicial} S{semestre_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric("FLEX Volume", f"R$ {flex_volume_2:,.2f}",
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", f"R$ {flex_inflacao_2:,.2f}",
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total {ano_final} S{semestre_final}", f"R$ {total_m2_all_2:,.2f}")
            with col5_2:
                st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total {ano_inicial} S{semestre_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric(f"Total {ano_final} S{semestre_final}", f"R$ {total_m2_all_2:,.2f}")
            st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Quarter":
        # Mostrar FLEX se houver valores
        if abs(flex_volume_2) > 0.01 or abs(flex_inflacao_2) > 0.01:
            col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
            with col1_2:
                st.metric(f"Total {ano_inicial} Q{trimestre_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric("FLEX Volume", f"R$ {flex_volume_2:,.2f}",
                          help="Efeito de Volume + Sensibilidade")
            with col3_2:
                st.metric("FLEX Inflação", f"R$ {flex_inflacao_2:,.2f}",
                          help="Efeito da Inflação")
            with col4_2:
                st.metric(f"Total {ano_final} Q{trimestre_final}", f"R$ {total_m2_all_2:,.2f}")
            with col5_2:
                st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        else:
            col1_2, col2_2 = st.columns(2)
            with col1_2:
                st.metric(f"Total {ano_inicial} Q{trimestre_inicial}", f"R$ {total_m1_all_2:,.2f}")
            with col2_2:
                st.metric(f"Total {ano_final} Q{trimestre_final}", f"R$ {total_m2_all_2:,.2f}")
            st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
    elif modo_comparacao == "Múltiplos Meses":
        col1_2, col2_2 = st.columns(2)
        with col1_2:
            st.metric(f"Total {mes_inicial_2}", f"R$ {total_m1_all_2:,.2f}")
        with col2_2:
            st.metric(f"Total {mes_final_2}", f"R$ {total_m2_all_2:,.2f}")
        st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")
        # Mostrar totais dos meses intermediários
        if len(meses_selecionados_2) > 2:
            st.markdown("#### Meses Intermediários")
            cols_inter = st.columns(len(meses_selecionados_2[1:-1]))
            for idx, mes in enumerate(meses_selecionados_2[1:-1]):
                total_mes = float(df_segunda_analise[df_segunda_analise[col_mes].astype(str) == str(mes)][col_valor].sum())
                with cols_inter[idx]:
                    st.metric(f"{mes}", f"R$ {total_mes:,.2f}")
    else:  # Mês a Mês
        col1_2, col2_2, col3_2, col4_2, col5_2 = st.columns(5)
        with col1_2:
            st.metric("Total Mês Inicial", f"R$ {total_m1_all_2:,.2f}")
        with col2_2:
            st.metric("FLEX Volume", f"R$ {flex_volume_2:,.2f}",
                      help="Efeito de Volume + Sensibilidade")
        with col3_2:
            st.metric("FLEX Inflação", f"R$ {flex_inflacao_2:,.2f}",
                      help="Efeito da Inflação")
        with col4_2:
            st.metric("Total Mês Final", f"R$ {total_m2_all_2:,.2f}")
        with col5_2:
            st.metric("Variação Total", f"R$ {change_all_2:,.2f}", delta=f"{change_all_2/total_m1_all_2*100:.2f}%" if total_m1_all_2 > 0 else "0%")

st.markdown("---")
st.markdown("**📊 Dashboard TC - Análise Waterfall** | Desenvolvido com Streamlit")

