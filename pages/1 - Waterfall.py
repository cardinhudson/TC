import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
import os
import sys

# Configurar Plotly para usar o engine JSON padrão em vez de orjson (evita problemas de importação circular)
# Isso força o Plotly a usar o json padrão do Python em vez de orjson
try:
    import json
    # Forçar uso do engine json padrão
    pio.json.config.default_engine = 'json'
    # Desabilitar orjson se disponível
    if hasattr(pio.json, 'config'):
        pio.json.config.validate = False
except Exception:
    pass

# Função helper para exibir gráficos Plotly com tratamento de erro para orjson
def plotly_chart_safe(fig, use_container_width=True):
    """
    Exibe um gráfico Plotly com tratamento de erro para problemas com orjson.
    """
    try:
        st.plotly_chart(fig, use_container_width=use_container_width)
    except (AttributeError, TypeError) as e:
        error_msg = str(e)
        if 'orjson' in error_msg or 'OPT_NON_STR_KEYS' in error_msg:
            st.error("⚠️ **Erro ao renderizar gráfico:** Problema com o módulo `orjson`")
            st.markdown("**Solução:** Execute no terminal:")
            st.code("pip install --upgrade --force-reinstall orjson", language="bash")
            st.markdown("Ou alternativamente:")
            st.code("pip uninstall orjson\npip install orjson", language="bash")
            st.warning("Após reinstalar, recarregue a página (F5 ou Ctrl+R)")
        else:
            # Re-raise outros erros
            raise

# Adicionar o diretório raiz ao path para importar funções do app.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar funções necessárias do app.py
from app import (
    load_data, load_volume_data, load_budget_data, load_budget_volume_data,
    formatar_periodo_abreviado, formatar_ratio_com_barra, criar_tabela_html_com_barra,
    calcular_resumo_tabela_flex, exibir_caixas_resumo_dinamico, exibir_caixas_resumo,
    converter_moeda, converter_coluna_moeda, obter_simbolo_moeda,
    listar_anos_disponiveis, encontrar_arquivo_parquet,
    carregar_taxas_banco, salvar_taxas_banco, inicializar_banco_taxas,
    reordenar_colunas_padrao
)

# Configuração da página
st.set_page_config(
    page_title="Waterfall Analysis - TC",
    page_icon="🌊",
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

# CSS
st.markdown("""
    <style>
        h1 {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("🌊 Waterfall Analysis")
st.markdown("---")

# ========== CONFIGURAÇÕES INICIAIS (mesmas do app.py) ==========
# Inicializar banco de taxas
inicializar_banco_taxas()

# Filtros na sidebar - ANTES de carregar dados
st.sidebar.markdown("---")
st.sidebar.markdown("**📅 Seleção de Ano**")

# Listar anos disponíveis
anos_disponiveis = listar_anos_disponiveis()
opcoes_ano = ["Todos"] + [str(ano) for ano in anos_disponiveis]

# Determinar índice padrão: ano atual se disponível, senão "Todos" (índice 0)
from datetime import datetime
ano_atual = datetime.now().year
ano_atual_str = str(ano_atual)
if ano_atual_str in opcoes_ano:
    index_padrao = opcoes_ano.index(ano_atual_str)
else:
    index_padrao = 0  # "Todos" se ano atual não estiver disponível

# Inicializar session_state para manter valores dos filtros
if 'filtro_ano_waterfall' not in st.session_state:
    st.session_state.filtro_ano_waterfall = opcoes_ano[index_padrao] if index_padrao < len(opcoes_ano) else "Todos"

# Seletor de ano
ano_selecionado = st.sidebar.selectbox(
    "Selecione o ano:",
    options=opcoes_ano,
    index=opcoes_ano.index(st.session_state.filtro_ano_waterfall) if st.session_state.filtro_ano_waterfall in opcoes_ano else index_padrao,
    help="Selecione 'Todos' para ver dados consolidados ou um ano específico",
    key="filtro_ano_waterfall_selectbox"
)
# Atualizar session_state
st.session_state.filtro_ano_waterfall = ano_selecionado

# Carregar taxas
try:
    taxas_cambio_banco = carregar_taxas_banco()
except Exception as e:
    taxas_cambio_banco = {"USD": 5.00, "EUR": 5.50}

taxa_usd_para_brl_padrao = taxas_cambio_banco.get("USD", 5.00)
taxa_eur_para_brl_padrao = taxas_cambio_banco.get("EUR", 5.50)

# Inicializar estado da moeda se não existir
if 'moeda_selecionada' not in st.session_state:
    st.session_state.moeda_selecionada = "🇧🇷 R$"
if 'moeda_selecionada_radio' not in st.session_state:
    st.session_state.moeda_selecionada_radio = "🇧🇷 R$"

# URLs das bandeiras
bandeira_brasil_url = "https://flagcdn.com/br.svg"
bandeira_eua_url = "https://flagcdn.com/us.svg"
bandeira_europa_url = "https://flagcdn.com/eu.svg"

# Seleção de moeda com bandeiras ao lado
col_moeda1, col_moeda2 = st.columns([3, 1])

with col_moeda1:
    st.markdown("💱 **Moeda:**", unsafe_allow_html=True)
    opcoes_moeda = ["🇧🇷 R$", "🇺🇸 $", "🇪🇺 €"]
    
    moeda_atual_para_index = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
    index_moeda = opcoes_moeda.index(moeda_atual_para_index) if moeda_atual_para_index in opcoes_moeda else 0
    
    def atualizar_moeda():
        if 'moeda_selecionada_radio_waterfall' in st.session_state:
            st.session_state.moeda_selecionada = st.session_state.moeda_selecionada_radio_waterfall
    
    moeda_selecionada = st.radio(
        "",
        opcoes_moeda,
        index=index_moeda,
        horizontal=True,
        help="Selecione a moeda para exibição nos gráficos",
        key="moeda_selecionada_radio_waterfall",
        label_visibility="visible",
        on_change=atualizar_moeda
    )
    
    if st.session_state.moeda_selecionada != moeda_selecionada:
        st.session_state.moeda_selecionada = moeda_selecionada

# Obter moeda atual do session_state
moeda_atual = st.session_state.get('moeda_selecionada', '🇧🇷 R$')
flag_selecionada_brl = moeda_atual == '🇧🇷 R$'
flag_selecionada_usd = moeda_atual == '🇺🇸 $'
flag_selecionada_eur = moeda_atual == '🇪🇺 €'

with col_moeda2:
    st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento vertical
    st.markdown(f"""
    <div style="display: flex; flex-direction: row; gap: 0.5rem; align-items: center; margin-top: 0.5rem; justify-content: center;">
        <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_brl else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_brl else 'transparent'};">
            <img src="{bandeira_brasil_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_brl else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_brl else 'none'};">
        </div>
        <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_usd else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_usd else 'transparent'};">
            <img src="{bandeira_eua_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_usd else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_usd else 'none'};">
        </div>
        <div style="padding: 4px; border-radius: 6px; border: 2px solid {'#ff4b4b' if flag_selecionada_eur else 'transparent'}; background-color: {'rgba(255, 75, 75, 0.1)' if flag_selecionada_eur else 'transparent'};">
            <img src="{bandeira_europa_url}" style="width: 40px; height: 28px; border-radius: 3px; border: {'2px solid #ff4b4b' if flag_selecionada_eur else '1px solid rgba(255, 255, 255, 0.2)'}; object-fit: cover; display: block; box-shadow: {'0 0 6px rgba(255, 75, 75, 0.6)' if flag_selecionada_eur else 'none'};">
        </div>
    </div>
    """, unsafe_allow_html=True)

# Taxas de câmbio
st.markdown("📝 **Entrada de Taxas:**", unsafe_allow_html=True)
col_taxa1, col_taxa2 = st.columns([1.1, 1.1], gap="small")

with col_taxa1:
    st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇺🇸 1 $ (USD) = R$</p>', unsafe_allow_html=True)
    taxa_usd_para_brl = st.number_input(
        "",
        min_value=0.01,
        max_value=100.0,
        value=float(taxa_usd_para_brl_padrao),
        step=0.01,
        format="%.2f",
        help="Digite quanto vale 1 Dólar Americano em Reais Brasileiros.",
        key="taxa_usd_para_brl_waterfall",
        label_visibility="collapsed"
    )

with col_taxa2:
    st.markdown('<p style="font-size: 0.7rem; margin-bottom: 0.2rem;">🇪🇺 1 € (EUR) = R$</p>', unsafe_allow_html=True)
    taxa_eur_para_brl = st.number_input(
        "",
        min_value=0.01,
        max_value=100.0,
        value=float(taxa_eur_para_brl_padrao),
        step=0.01,
        format="%.2f",
        help="Digite quanto vale 1 Euro em Reais Brasileiros.",
        key="taxa_eur_para_brl_waterfall",
        label_visibility="collapsed"
    )

taxa_brl_para_usd = 1.0 / taxa_usd_para_brl if taxa_usd_para_brl > 0 else 0.20
taxa_brl_para_eur = 1.0 / taxa_eur_para_brl if taxa_eur_para_brl > 0 else 0.18

# Salvar taxas
taxa_usd_atual_key = "taxa_usd_atual_salva_waterfall"
taxa_eur_atual_key = "taxa_eur_atual_salva_waterfall"

taxa_usd_mudou = (taxa_usd_atual_key not in st.session_state or 
                  st.session_state.get(taxa_usd_atual_key) != taxa_usd_para_brl)
taxa_eur_mudou = (taxa_eur_atual_key not in st.session_state or 
                  st.session_state.get(taxa_eur_atual_key) != taxa_eur_para_brl)

if taxa_usd_mudou or taxa_eur_mudou:
    novas_taxas = {
        "USD": float(taxa_usd_para_brl),
        "EUR": float(taxa_eur_para_brl)
    }
    try:
        salvar_taxas_banco(novas_taxas)
        st.session_state[taxa_usd_atual_key] = taxa_usd_para_brl
        st.session_state[taxa_eur_atual_key] = taxa_eur_para_brl
    except Exception as e:
        st.error(f"❌ Erro ao salvar taxas: {e}")

taxas_cambio = {
    "BRL": 1.0,
    "USD": taxa_brl_para_usd,
    "EUR": taxa_brl_para_eur
}

st.markdown("---")

# Seletores no topo
col_tipo, col_fator = st.columns([1.3, 1.2], gap="small")

with col_tipo:
    tipo_visualizacao = st.radio(
        "📊 **Tipo:**",
        ["Custo Total", "CPU (Custo por Unidade)"],
        index=0,
        horizontal=True,
        key="tipo_visualizacao_waterfall"
    )

with col_fator:
    if tipo_visualizacao == "Custo Total":
        fator_conversao = st.radio(
            "🔢 **Fator:**",
            ["Nenhum", "K (milhares)", "M (Milhões)"],
            index=1,
            horizontal=True,
            help="Aplica divisão aos valores para simplificar visualização.",
            key="fator_conversao_waterfall"
        )
    else:
        fator_conversao = None

# Obter a moeda selecionada do session state (já está atualizado acima)
moeda_selecionada = st.session_state.get('moeda_selecionada', '🇧🇷 R$')

if moeda_selecionada == "🇧🇷 R$":
    moeda_codigo = "BRL"
    moeda_simbolo = "R$"
elif moeda_selecionada == "🇺🇸 $":
    moeda_codigo = "USD"
    moeda_simbolo = "$"
elif moeda_selecionada == "🇪🇺 €":
    moeda_codigo = "EUR"
    moeda_simbolo = "€"
else:
    moeda_codigo = "BRL"
    moeda_simbolo = "R$"

st.markdown("---")

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
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
    import traceback
    st.error(f"Detalhes: {traceback.format_exc()}")
    st.stop()

# Carregar dados de volume e budget
df_volume = load_volume_data(ano_selecionado)
df_budget = load_budget_data(ano_selecionado)
df_budget_vol = load_budget_volume_data(ano_selecionado)

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

# Aplicar fator de conversão nas colunas Total (antes de qualquer processamento)
# 🔧 CORREÇÃO CRÍTICA: NÃO aplicar fator de conversão quando está em modo CPU
if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
    if fator_conversao == "K (milhares)":
        if 'Total' in df_total.columns:
            df_total['Total'] = df_total['Total'] / 1000
    elif fator_conversao == "M (Milhões)":
        if 'Total' in df_total.columns:
            df_total['Total'] = df_total['Total'] / 1000000

# Aplicar conversão de moeda DEPOIS do fator de conversão
# IMPORTANTE: Aplicar conversão em AMBOS os modos (Custo Total e CPU)
# No modo CPU, o Total convertido será usado para calcular CPU = Total convertido / Volume
if moeda_codigo != "BRL" and 'Total' in df_total.columns:
    df_total = converter_coluna_moeda(df_total, 'Total', moeda_codigo, taxas_cambio)

st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Filtros**")

# Inicializar session_state para filtros
if 'filtro_oficina_waterfall' not in st.session_state:
    st.session_state.filtro_oficina_waterfall = ["Todos"]

# Filtro 1: Oficina (com cache otimizado)
if 'Oficina' in df_total.columns:
    oficina_opcoes = get_filter_options(df_total, 'Oficina')
    # Validar valores salvos
    default_oficina = st.session_state.filtro_oficina_waterfall if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_waterfall) else ["Todos"]
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=default_oficina, key="filtro_oficina_waterfall_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_oficina_waterfall = oficina_selecionadas if oficina_selecionadas else ["Todos"]

    # Filtrar o DataFrame com base na Oficina
    if "Todos" in oficina_selecionadas or not oficina_selecionadas:
        df_filtrado = df_total.copy()
    else:
        df_filtrado = df_total[
            df_total['Oficina'].astype(str).isin(oficina_selecionadas)
        ].copy()
else:
    df_filtrado = df_total.copy()

# Inicializar session_state para Veículo
if 'filtro_veiculo_waterfall' not in st.session_state:
    st.session_state.filtro_veiculo_waterfall = ["Todos"]

# Filtro 2: Veículo (com cache otimizado)
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
    # Validar valores salvos
    default_veiculo = st.session_state.filtro_veiculo_waterfall if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_waterfall) else ["Todos"]
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_waterfall_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_veiculo_waterfall = veiculo_selecionados if veiculo_selecionados else ["Todos"]
    if veiculo_selecionados and "Todos" not in veiculo_selecionados:
        df_filtrado = df_filtrado[
            df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
        ].copy()

# Inicializar session_state para USI
if 'filtro_usi_waterfall' not in st.session_state:
    if 'USI' in df_total.columns:
        usi_opcoes_temp = get_filter_options(df_total, 'USI')
        st.session_state.filtro_usi_waterfall = ["TC Ext"] if "TC Ext" in usi_opcoes_temp else ["Todos"]
    else:
        st.session_state.filtro_usi_waterfall = ["Todos"]

# Filtro 3: USI (com cache otimizado)
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    # Validar valores salvos
    default_usi = st.session_state.filtro_usi_waterfall if all(x in usi_opcoes for x in st.session_state.filtro_usi_waterfall) else (["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"])
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_waterfall_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_usi_waterfall = usi_selecionada if usi_selecionada else ["Todos"]

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

    # Inicializar session_state para Período
    if 'filtro_periodo_waterfall' not in st.session_state:
        st.session_state.filtro_periodo_waterfall = "Todos"
    
    # Validar valor salvo
    periodo_default = st.session_state.filtro_periodo_waterfall if st.session_state.filtro_periodo_waterfall in periodo_opcoes else "Todos"
    periodo_index = periodo_opcoes.index(periodo_default) if periodo_default in periodo_opcoes else 0
    
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes, index=periodo_index, key="filtro_periodo_waterfall_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_periodo_waterfall = periodo_selecionado
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Inicializar session_state para Centro cst
if 'filtro_centro_cst_waterfall' not in st.session_state:
    st.session_state.filtro_centro_cst_waterfall = "Todos"

# Filtro 5: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    # Validar valor salvo
    centro_cst_default = st.session_state.filtro_centro_cst_waterfall if st.session_state.filtro_centro_cst_waterfall in centro_cst_opcoes else "Todos"
    centro_cst_index = centro_cst_opcoes.index(centro_cst_default) if centro_cst_default in centro_cst_opcoes else 0
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes, index=centro_cst_index, key="filtro_centro_cst_waterfall_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_centro_cst_waterfall = centro_cst_selecionado
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Inicializar session_state para Conta contábil
if 'filtro_conta_contabil_waterfall' not in st.session_state:
    st.session_state.filtro_conta_contabil_waterfall = []

# Filtro 6: Conta contábil (com cache otimizado)
if 'Nºconta' in df_filtrado.columns:
    conta_contabil_opcoes = get_filter_options(df_filtrado, 'Nºconta')[1:]
    # Validar valores salvos
    default_conta = [x for x in st.session_state.filtro_conta_contabil_waterfall if x in conta_contabil_opcoes] if st.session_state.filtro_conta_contabil_waterfall else []
    conta_contabil_selecionadas = st.sidebar.multiselect(
        "Selecione a Conta contábil:", conta_contabil_opcoes, default=default_conta, key="filtro_conta_contabil_waterfall_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_conta_contabil_waterfall = conta_contabil_selecionadas
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
        # Inicializar session_state para cada filtro principal
        filtro_key = f'filtro_{col_name}_waterfall'
        if filtro_key not in st.session_state:
            st.session_state[filtro_key] = ["Todos"]
        
        opcoes = get_filter_options(df_filtrado, col_name)
        if widget_type == "multiselect":
            # Validar valores salvos
            default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
            selecionadas = st.sidebar.multiselect(
                f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect"
            )
            # Atualizar session_state
            st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
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
                # Inicializar session_state para cada filtro avançado
                filtro_key = f'filtro_avancado_{col_name}_waterfall'
                if filtro_key not in st.session_state:
                    st.session_state[filtro_key] = ["Todos"]
                
                # Validar valores salvos
                default_val = st.session_state[filtro_key] if all(x in opcoes for x in st.session_state[filtro_key]) else ["Todos"]
                selecionadas = st.multiselect(
                    f"Selecione o {label}:", opcoes, default=default_val, key=f"{filtro_key}_multiselect"
                )
                # Atualizar session_state
                st.session_state[filtro_key] = selecionadas if selecionadas else ["Todos"]
                if selecionadas and "Todos" not in selecionadas:
                    df_filtrado = df_filtrado[
                        df_filtrado[col_name].astype(str).isin(selecionadas)
                    ].copy()

# Resumo na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Resumo**")
st.sidebar.write(f"**Linhas:** {df_filtrado.shape[0]:,}")

# Calcular totais se as colunas existirem
if 'Valor' in df_filtrado.columns:
    valor_total = df_filtrado['Valor'].sum()
    st.sidebar.write(f"**Total Valor:** {moeda_simbolo} {valor_total:,.2f}")
if 'Total' in df_filtrado.columns:
    total_sum = df_filtrado['Total'].sum()
    st.sidebar.write(f"**Total:** {moeda_simbolo} {total_sum:,.2f}")

# IMPORTANTE: df_filtrado é uma cópia de df_total que JÁ TEM a conversão de moeda aplicada (linha 342)
# NÃO aplicar conversão novamente aqui para evitar duplicação

# Preparar dados para visualização
# IMPORTANTE: df_visualizacao é uma cópia de df_filtrado que JÁ TEM a conversão de moeda aplicada
# NÃO aplicar conversão novamente aqui para evitar duplicação
df_visualizacao = df_filtrado.copy()

# ========== CÓDIGO DO TAB5 (adaptado) ==========
st.markdown("---")

# Verificar se Type 06 existe nos dados
if 'Type 06' not in df_filtrado.columns:
    st.warning("⚠️ Coluna 'Type 06' não encontrada nos dados. A análise waterfall requer esta coluna.")
    st.info("💡 Certifique-se de que os dados contêm a coluna 'Type 06' para categorização.")
else:
    # Dimensão da categoria: Type 06 (fixo conforme solicitado)
    chosen_dim = "Type 06"
    
    # Obter valores únicos de Type 06
    type06_valores = sorted(df_filtrado['Type 06'].dropna().unique().tolist())
    
    if len(type06_valores) == 0:
        st.warning("⚠️ Nenhum valor de Type 06 encontrado nos dados filtrados.")
    else:
        # Criar dois sub-tabs: Real e Budget
        tab_real, tab_budget = st.tabs(["📊 Real", "💰 Budget"])
        
        # Função para ordenar meses cronologicamente
        def sort_mes_unique_waterfall(values):
            """Ordena valores de meses únicos cronologicamente"""
            MES_POS = {
                'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
                'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
            }
            vals = list(pd.Series(values).dropna().unique())
            
            def ordenar_cronologico(x):
                x_str = str(x).strip()
                if ' ' in x_str:
                    partes = x_str.split(' ', 1)
                    mes_nome = partes[0].lower()
                    try:
                        ano = int(partes[1])
                        mes_idx = MES_POS.get(mes_nome, 99)
                        return (ano, mes_idx)
                    except (ValueError, IndexError):
                        return (0, MES_POS.get(mes_nome, 99))
                else:
                    try:
                        return (0, int(x_str))
                    except ValueError:
                        return (0, MES_POS.get(x_str.lower(), 99))
            
            try:
                return sorted(vals, key=ordenar_cronologico)
            except Exception:
                return sorted(vals)
        
        # Criar coluna Período_Ano
        if 'Período' in df_filtrado.columns and 'Ano' in df_filtrado.columns:
            df_filtrado_waterfall = df_filtrado.copy()
            df_filtrado_waterfall['Período_Ano'] = df_filtrado_waterfall['Período'].astype(str) + ' ' + df_filtrado_waterfall['Ano'].astype(str)
            col_mes_waterfall = 'Período_Ano'
            periodos_unicos = sort_mes_unique_waterfall(df_filtrado_waterfall['Período_Ano'].dropna().unique().tolist())
        elif 'Período' in df_filtrado.columns:
            df_filtrado_waterfall = df_filtrado.copy()
            col_mes_waterfall = 'Período'
            periodos_unicos = sort_mes_unique_waterfall(df_filtrado['Período'].dropna().unique().tolist())
        else:
            st.warning("⚠️ Coluna 'Período' não encontrada nos dados.")
            periodos_unicos = []
            df_filtrado_waterfall = df_filtrado.copy()
            col_mes_waterfall = None
        
        # Variáveis para armazenar seleções
        mes_inicial = None
        mes_final = None
        ano_inicial = None
        ano_final = None
        semestre_inicial = None
        semestre_final = None
        trimestre_inicial = None
        trimestre_final = None
        meses_selecionados = []
        
        st.markdown("---")
        
        st.markdown("---")
        
        # TAB REAL
        with tab_real:
            st.subheader("📊 Análise Real")
            
            # Modo de Comparação
            st.markdown("### 📅 Modo de Comparação")
            modo_comparacao = st.radio(
                "Tipo de comparação:",
                options=["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"],
                index=0,
                key="modo_comparacao_waterfall",
                horizontal=True,
                help="Selecione o modo de comparação entre períodos"
            )
            
            # Seleção de períodos
            if modo_comparacao == "Mês a Mês":
                if periodos_unicos:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        mes_inicial = st.selectbox("Mês inicial:", periodos_unicos, index=0, key="mes_inicial_waterfall")
                    with col_b:
                        mes_final = st.selectbox("Mês final:", periodos_unicos, index=len(periodos_unicos)-1, key="mes_final_waterfall")
                    meses_selecionados = [mes_inicial, mes_final]
                else:
                    st.warning("⚠️ Nenhum período encontrado nos dados.")
            
            elif modo_comparacao == "Ano a Ano":
                if 'Ano' in df_filtrado.columns:
                    anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                    if len(anos_disponiveis) >= 2:
                        col_ano1, col_ano2 = st.columns(2)
                        with col_ano1:
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_waterfall")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1), key="ano_final_waterfall")
                        mes_inicial = f"Total {ano_inicial}"
                        mes_final = f"Total {ano_final}"
                        meses_selecionados = [mes_inicial, mes_final]
                    else:
                        st.warning("⚠️ É necessário ter pelo menos 2 anos de dados para comparação ano a ano.")
                else:
                    st.warning("⚠️ Coluna 'Ano' não encontrada nos dados.")
            
            elif modo_comparacao == "Semestre":
                if 'Ano' in df_filtrado.columns:
                    anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                    if len(anos_disponiveis) >= 1:
                        col_ano1, col_sem1, col_ano2, col_sem2 = st.columns(4)
                        with col_ano1:
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_sem_waterfall")
                        with col_sem1:
                            semestre_inicial = st.selectbox("Semestre inicial:", [1, 2], index=0, key="semestre_inicial_waterfall")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_sem_waterfall")
                        with col_sem2:
                            semestre_final = st.selectbox("Semestre final:", [1, 2], index=1, key="semestre_final_waterfall")
                        mes_inicial = f"{ano_inicial} S{semestre_inicial}"
                        mes_final = f"{ano_final} S{semestre_final}"
                        meses_selecionados = [mes_inicial, mes_final]
                    else:
                        st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de semestres.")
                else:
                    st.warning("⚠️ Coluna 'Ano' não encontrada nos dados.")
            
            elif modo_comparacao == "Quarter":
                if 'Ano' in df_filtrado.columns:
                    anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                    if len(anos_disponiveis) >= 1:
                        col_ano1, col_q1, col_ano2, col_q2 = st.columns(4)
                        with col_ano1:
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_q_waterfall")
                        with col_q1:
                            trimestre_inicial = st.selectbox("Quarter inicial:", [1, 2, 3, 4], index=0, key="trimestre_inicial_waterfall")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_q_waterfall")
                        with col_q2:
                            trimestre_final = st.selectbox("Quarter final:", [1, 2, 3, 4], index=1, key="trimestre_final_waterfall")
                        mes_inicial = f"{ano_inicial} Q{trimestre_inicial}"
                        mes_final = f"{ano_final} Q{trimestre_final}"
                        meses_selecionados = [mes_inicial, mes_final]
                    else:
                        st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de quarters.")
                else:
                    st.warning("⚠️ Coluna 'Ano' não encontrada nos dados.")
            
            st.markdown("---")
            
            if not meses_selecionados or len(meses_selecionados) < 2:
                st.info("ℹ️ Selecione os períodos para comparação acima para visualizar a análise waterfall.")
            else:
                try:
                    # Determinar coluna de valor baseado no tipo de visualização
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        # Para CPU, precisamos de 'Total' e acesso ao df_volume
                        if 'Total' in df_filtrado.columns:
                            # Verificar se temos acesso ao df_volume para calcular CPU
                            if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                col_valor = 'Total'
                            elif 'CPU' in df_visualizacao.columns:
                                col_valor = 'CPU'
                            else:
                                st.error("❌ Dados insuficientes para calcular CPU. É necessário ter dados de volume disponíveis.")
                                st.stop()
                        else:
                            st.error("❌ Coluna 'Total' não encontrada para calcular CPU.")
                            st.stop()
                    else:
                        if 'Total' in df_filtrado.columns:
                            col_valor = 'Total'
                        elif 'Valor' in df_filtrado.columns:
                            col_valor = 'Valor'
                        else:
                            st.error("❌ Coluna 'Total' ou 'Valor' não encontrada.")
                            st.stop()
                    
                    # Preparar dados para análise
                    df_analise = df_filtrado_waterfall.copy()
                    
                    # Criar df_vol_filtrado aplicando os mesmos filtros (necessário para cálculo do Flex Volume no gráfico)
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros que existem em df_filtrado_waterfall (mesma lógica do app.py)
                    df_vol_filtrado = None
                    if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                        df_vol_filtrado = df_volume.copy()
                        
                        # Aplicar TODOS os filtros que existem em df_filtrado_waterfall (mesma lógica do app.py linha 4756)
                        if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
                            # Aplicar filtro de Veículo
                            if 'Veículo' in df_filtrado_waterfall.columns and 'Veículo' in df_vol_filtrado.columns:
                                veiculos_filtrados = df_filtrado_waterfall['Veículo'].dropna().unique()
                                if len(veiculos_filtrados) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['Veículo'].isin(veiculos_filtrados)
                                    ].copy()
                            
                            # Aplicar filtro de Oficina
                            if 'Oficina' in df_filtrado_waterfall.columns and 'Oficina' in df_vol_filtrado.columns:
                                oficinas_filtradas = df_filtrado_waterfall['Oficina'].dropna().unique()
                                if len(oficinas_filtradas) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['Oficina'].isin(oficinas_filtradas)
                                    ].copy()
                            
                            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de USI (importante para TC Ext)
                            if 'USI' in df_filtrado_waterfall.columns and 'USI' in df_vol_filtrado.columns:
                                usi_filtradas = df_filtrado_waterfall['USI'].dropna().unique()
                                if len(usi_filtradas) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['USI'].isin(usi_filtradas)
                                    ].copy()
                            
                            # Aplicar outros filtros comuns (se existirem) - mesma lógica do app.py linha 4782
                            colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                            for col_filtro in colunas_filtro_comuns:
                                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_vol_filtrado.columns:
                                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                    if len(valores_filtrados) > 0:
                                        df_vol_filtrado = df_vol_filtrado[
                                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                                        ].copy()
                            
                            # Aplicar filtros avançados (se existirem)
                            colunas_filtro_avancados = ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve', 'Account']
                            for col_filtro in colunas_filtro_avancados:
                                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_vol_filtrado.columns:
                                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                    if len(valores_filtrados) > 0:
                                        df_vol_filtrado = df_vol_filtrado[
                                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                                        ].copy()
                        else:
                            # Fallback: usar filtros do session_state (comportamento antigo)
                            # Filtros de Oficina
                            oficinas_selecionadas = st.session_state.get('filtro_oficina_waterfall', ["Todos"])
                            if oficinas_selecionadas and "Todos" not in oficinas_selecionadas and len(oficinas_selecionadas) > 0:
                                if 'Oficina' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Oficina'].isin(oficinas_selecionadas)]
                            
                            # Filtros de Veículo
                            veiculos_selecionados = st.session_state.get('filtro_veiculo_waterfall', ["Todos"])
                            if veiculos_selecionados and "Todos" not in veiculos_selecionados and len(veiculos_selecionados) > 0:
                                if 'Veículo' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Veículo'].isin(veiculos_selecionados)]
                            
                            # Filtros de USI
                            usis_selecionadas = st.session_state.get('filtro_usi_waterfall', ["Todos"])
                            if usis_selecionadas and "Todos" not in usis_selecionadas and len(usis_selecionadas) > 0:
                                if 'USI' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['USI'].isin(usis_selecionadas)]
                        
                        # Criar coluna Período_Ano no df_vol_filtrado se necessário (mesma lógica do df_filtrado_waterfall)
                        if col_mes_waterfall == 'Período_Ano':
                            if 'Período' in df_vol_filtrado.columns and 'Ano' in df_vol_filtrado.columns:
                                df_vol_filtrado['Período_Ano'] = df_vol_filtrado['Período'].astype(str) + ' ' + df_vol_filtrado['Ano'].astype(str)
                    
                    # Filtrar e agrupar dados baseado no modo de comparação para obter categorias
                    if modo_comparacao == "Mês a Mês":
                        if col_mes_waterfall:
                            df_temp = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                        else:
                            df_temp = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                    elif modo_comparacao == "Ano a Ano":
                        df_temp = df_analise[df_analise['Ano'].astype(str) == str(ano_final)].copy()
                    elif modo_comparacao == "Semestre":
                        meses_semestre = {
                            1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                            2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
                        }
                        meses_sem_final = meses_semestre.get(semestre_final, [])
                        df_temp = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_final)) &
                            (df_analise['Período'].isin(meses_sem_final))
                        ].copy()
                    elif modo_comparacao == "Quarter":
                        meses_trimestre = {
                            1: ['Janeiro', 'Fevereiro', 'Março'],
                            2: ['Abril', 'Maio', 'Junho'],
                            3: ['Julho', 'Agosto', 'Setembro'],
                            4: ['Outubro', 'Novembro', 'Dezembro']
                        }
                        meses_trim_final = meses_trimestre.get(trimestre_final, [])
                        df_temp = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_final)) &
                            (df_analise['Período'].isin(meses_trim_final))
                        ].copy()
                    else:
                        # Modo Mês a Mês (fallback)
                        if col_mes_waterfall:
                            df_temp = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                        else:
                            df_temp = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                    
                    # Obter dimensões de categoria disponíveis
                    dims_cat = [c for c in ["Type 05", "Type 06", "Type 07", "Oficina", "Veículo", "Custo", "Account"] if c in df_analise.columns]
                    
                    if not dims_cat:
                        st.warning("⚠️ Nenhuma dimensão de categoria encontrada nos dados.")
                    else:
                        # Filtro de dimensão da categoria
                        chosen_dim_waterfall = st.selectbox(
                            "Dimensão da categoria:",
                            dims_cat,
                            index=min(1, len(dims_cat)-1) if len(dims_cat) > 1 else 0,
                            key="dim_waterfall_real"
                        )
                        
                        # Obter todas as categorias disponíveis
                        cats_all = sorted([str(x).strip() for x in df_analise[chosen_dim_waterfall].dropna().unique().tolist() if str(x).strip() != ""])
                        total_cats = max(1, len(cats_all))
                        
                        # Controle: Quantidade de categorias a exibir (Top N)
                        # Limitar sempre a no máximo 20 categorias
                        max_cats_limit = min(total_cats, 20)
                        # Valor padrão: usar o mínimo entre total_cats e 20, mas limitar a 20
                        default_value = min(total_cats, 20)
                        max_cats = st.slider(
                            f"Quantidade de categorias a exibir (Top N) (Total: {total_cats}):",
                            min_value=1,
                            max_value=20,  # Sempre limitar a 20
                            value=default_value,
                            key="max_cats_waterfall"
                        )
                        # Garantir que não exceda 20
                        max_cats = min(max_cats, 20)
                    
                        # Calcular categorias padrão baseado no mês final
                        if not df_temp.empty:
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_temp.columns and 'Volume' in df_temp.columns:
                                vol_mf = (df_temp.groupby(chosen_dim_waterfall).agg({'Total': 'sum', 'Volume': 'sum'}).reset_index())
                                vol_mf['CPU'] = vol_mf['Total'] / vol_mf['Volume'].replace(0, 1)
                                vol_mf = vol_mf.sort_values('CPU', ascending=False)
                                vol_index = [str(c).strip() for c in list(vol_mf[chosen_dim_waterfall])]
                            else:
                                vol_mf = (df_temp.groupby(chosen_dim_waterfall)[col_valor].sum().sort_values(ascending=False))
                                vol_index = [str(c).strip() for c in list(vol_mf.index)]
                        else:
                            vol_index = []
                        
                        default_cats = vol_index[:max_cats] if len(vol_index) else cats_all[:max_cats]
                        
                        # Opções de categorias
                        cats_options = ["Todos"] + cats_all
                        default_cats = [c for c in default_cats if c in cats_all]
                        if not default_cats:
                            default_cats = cats_all[:min(10, len(cats_all))]
                        
                        # Controle: Categorias (uma ou mais)
                        cats_sel_raw = st.multiselect(
                            "Categorias (uma ou mais):",
                            cats_options,
                            default=default_cats,
                            key="cats_waterfall"
                        )
                        
                        if (not cats_sel_raw) or ("Todos" in cats_sel_raw):
                            cats_sel = cats_all[:max_cats] if max_cats < len(cats_all) else cats_all
                        else:
                            cats_sel = cats_sel_raw
                    
                    st.markdown("---")
                    
                    # Filtrar e agrupar dados baseado no modo de comparação
                    if modo_comparacao == "Mês a Mês":
                        if col_mes_waterfall:
                            df_m1 = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                        else:
                            df_m1 = df_analise[df_analise['Período'].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                            
                    elif modo_comparacao == "Ano a Ano":
                        df_m1 = df_analise[df_analise['Ano'].astype(str) == str(ano_inicial)].copy()
                        df_m2 = df_analise[df_analise['Ano'].astype(str) == str(ano_final)].copy()
                        
                    elif modo_comparacao == "Semestre":
                        meses_semestre = {
                            1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                            2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
                        }
                        meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
                        meses_sem_final = meses_semestre.get(semestre_final, [])
                        df_m1 = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_inicial)) &
                            (df_analise['Período'].isin(meses_sem_inicial))
                        ].copy()
                        df_m2 = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_final)) &
                            (df_analise['Período'].isin(meses_sem_final))
                        ].copy()
                        
                    elif modo_comparacao == "Quarter":
                        meses_trimestre = {
                            1: ['Janeiro', 'Fevereiro', 'Março'],
                            2: ['Abril', 'Maio', 'Junho'],
                            3: ['Julho', 'Agosto', 'Setembro'],
                            4: ['Outubro', 'Novembro', 'Dezembro']
                        }
                        meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
                        meses_trim_final = meses_trimestre.get(trimestre_final, [])
                        df_m1 = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_inicial)) &
                            (df_analise['Período'].isin(meses_trim_inicial))
                        ].copy()
                        df_m2 = df_analise[
                            (df_analise['Ano'].astype(str) == str(ano_final)) &
                            (df_analise['Período'].isin(meses_trim_final))
                        ].copy()
                    else:
                        if col_mes_waterfall:
                            df_m1 = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                        else:
                            df_m1 = df_analise[df_analise['Período'].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                    
                    if df_m1.empty or df_m2.empty:
                        st.warning("⚠️ Não há dados suficientes para os períodos selecionados.")
                    else:
                        # Calcular totais por dimensão selecionada
                        # IMPORTANTE: No modo CPU, usar volumes do df_volume (não do df_m1/df_m2)
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_m1.columns:
                            # Buscar volumes do df_volume filtrado
                            if df_vol_filtrado is not None and not df_vol_filtrado.empty:
                                # Filtrar volumes pelos períodos
                                if modo_comparacao == "Mês a Mês":
                                    if col_mes_waterfall:
                                        df_vol_m1_graph = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2_graph = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                                    else:
                                        df_vol_m1_graph = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2_graph = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_final)].copy()
                                elif modo_comparacao == "Ano a Ano":
                                    df_vol_m1_graph = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)].copy()
                                    df_vol_m2_graph = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_final)].copy()
                                elif modo_comparacao == "Semestre":
                                    meses_semestre = {1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                      2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']}
                                    meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
                                    meses_sem_final = meses_semestre.get(semestre_final, [])
                                    df_vol_m1_graph = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                        (df_vol_filtrado['Período'].isin(meses_sem_inicial))
                                    ].copy()
                                    df_vol_m2_graph = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                        (df_vol_filtrado['Período'].isin(meses_sem_final))
                                    ].copy()
                                elif modo_comparacao == "Quarter":
                                    meses_trimestre = {1: ['Janeiro', 'Fevereiro', 'Março'],
                                                       2: ['Abril', 'Maio', 'Junho'],
                                                       3: ['Julho', 'Agosto', 'Setembro'],
                                                       4: ['Outubro', 'Novembro', 'Dezembro']}
                                    meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
                                    meses_trim_final = meses_trimestre.get(trimestre_final, [])
                                    df_vol_m1_graph = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                        (df_vol_filtrado['Período'].isin(meses_trim_inicial))
                                    ].copy()
                                    df_vol_m2_graph = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                        (df_vol_filtrado['Período'].isin(meses_trim_final))
                                    ].copy()
                                else:
                                    df_vol_m1_graph = pd.DataFrame()
                                    df_vol_m2_graph = pd.DataFrame()
                                
                                # Calcular volumes
                                if not df_vol_m1_graph.empty and 'Período' in df_vol_m1_graph.columns:
                                    volume_m1_graph = df_vol_m1_graph.groupby('Período')['Volume'].sum().sum()
                                elif not df_vol_m1_graph.empty:
                                    volume_m1_graph = df_vol_m1_graph['Volume'].sum()
                                else:
                                    volume_m1_graph = 0
                                
                                if not df_vol_m2_graph.empty and 'Período' in df_vol_m2_graph.columns:
                                    volume_m2_graph = df_vol_m2_graph.groupby('Período')['Volume'].sum().sum()
                                elif not df_vol_m2_graph.empty:
                                    volume_m2_graph = df_vol_m2_graph['Volume'].sum()
                                else:
                                    volume_m2_graph = 0
                                
                                # Calcular CPU por categoria usando volumes do df_volume
                                # Agrupar volumes por categoria
                                if chosen_dim_waterfall in df_vol_m1_graph.columns:
                                    vol_m1_por_cat = df_vol_m1_graph.groupby(chosen_dim_waterfall)['Volume'].sum()
                                else:
                                    vol_m1_por_cat = pd.Series()
                                
                                if chosen_dim_waterfall in df_vol_m2_graph.columns:
                                    vol_m2_por_cat = df_vol_m2_graph.groupby(chosen_dim_waterfall)['Volume'].sum()
                                else:
                                    vol_m2_por_cat = pd.Series()
                            else:
                                volume_m1_graph = 0
                                volume_m2_graph = 0
                                vol_m1_por_cat = pd.Series()
                                vol_m2_por_cat = pd.Series()
                            
                            # Calcular CPU por categoria
                            if not vol_m1_por_cat.empty:
                                total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall)['Total'].sum()
                                g1 = total_m1_por_cat / vol_m1_por_cat.replace(0, 1)
                                g1 = g1.fillna(0)
                            else:
                                # Se não temos volumes por categoria, usar volume total para calcular CPU
                                total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall)['Total'].sum()
                                g1 = total_m1_por_cat / volume_m1_graph if volume_m1_graph > 0 else total_m1_por_cat / 1
                                g1 = g1.fillna(0)
                            
                            if not vol_m2_por_cat.empty:
                                total_m2_por_cat = df_m2.groupby(chosen_dim_waterfall)['Total'].sum()
                                g2 = total_m2_por_cat / vol_m2_por_cat.replace(0, 1)
                                g2 = g2.fillna(0)
                            else:
                                # Se não temos volumes por categoria, usar volume total para calcular CPU
                                total_m2_por_cat = df_m2.groupby(chosen_dim_waterfall)['Total'].sum()
                                g2 = total_m2_por_cat / volume_m2_graph if volume_m2_graph > 0 else total_m2_por_cat / 1
                                g2 = g2.fillna(0)
                            
                            # Calcular totais em CPU usando volumes do df_volume
                            total_m1_all = (df_m1['Total'].sum() / volume_m1_graph) if volume_m1_graph > 0 else 0
                            total_m2_all = (df_m2['Total'].sum() / volume_m2_graph) if volume_m2_graph > 0 else 0
                        else:
                            g1 = df_m1.groupby(chosen_dim_waterfall)[col_valor].sum()
                            g2 = df_m2.groupby(chosen_dim_waterfall)[col_valor].sum()
                            
                            total_m1_all = df_m1[col_valor].sum()
                            total_m2_all = df_m2[col_valor].sum()
                            volume_m1_graph = 0
                            volume_m2_graph = 0
                        
                        # Calcular Flex Mês 1 por categoria (mesma lógica do Flex BUD total)
                        # Primeiro, precisamos calcular o Flex Mês 1 para cada categoria
                        g_flex = {}  # Dicionário para armazenar Flex Mês 1 por categoria
                        
                        # Buscar volumes (já calculados anteriormente se df_vol_filtrado estiver disponível)
                        if df_vol_filtrado is not None and not df_vol_filtrado.empty:
                            # Filtrar volumes pelos períodos (mesma lógica anterior)
                            if modo_comparacao == "Mês a Mês":
                                if col_mes_waterfall:
                                    df_vol_m1_cat = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_inicial)].copy()
                                    df_vol_m2_cat = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                                else:
                                    df_vol_m1_cat = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_inicial)].copy()
                                    df_vol_m2_cat = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_final)].copy()
                            elif modo_comparacao == "Ano a Ano":
                                df_vol_m1_cat = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)].copy()
                                df_vol_m2_cat = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_final)].copy()
                            elif modo_comparacao == "Semestre":
                                meses_semestre = {1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                  2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']}
                                meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
                                meses_sem_final = meses_semestre.get(semestre_final, [])
                                df_vol_m1_cat = df_vol_filtrado[
                                    (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                    (df_vol_filtrado['Período'].isin(meses_sem_inicial))
                                ].copy()
                                df_vol_m2_cat = df_vol_filtrado[
                                    (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                    (df_vol_filtrado['Período'].isin(meses_sem_final))
                                ].copy()
                            elif modo_comparacao == "Quarter":
                                meses_trimestre = {1: ['Janeiro', 'Fevereiro', 'Março'],
                                                   2: ['Abril', 'Maio', 'Junho'],
                                                   3: ['Julho', 'Agosto', 'Setembro'],
                                                   4: ['Outubro', 'Novembro', 'Dezembro']}
                                meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
                                meses_trim_final = meses_trimestre.get(trimestre_final, [])
                                df_vol_m1_cat = df_vol_filtrado[
                                    (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                    (df_vol_filtrado['Período'].isin(meses_trim_inicial))
                                ].copy()
                                df_vol_m2_cat = df_vol_filtrado[
                                    (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                    (df_vol_filtrado['Período'].isin(meses_trim_final))
                                ].copy()
                            else:
                                df_vol_m1_cat = pd.DataFrame()
                                df_vol_m2_cat = pd.DataFrame()
                            
                            # Calcular volumes totais
                            if not df_vol_m1_cat.empty and 'Período' in df_vol_m1_cat.columns:
                                volume_m1_cat = df_vol_m1_cat.groupby('Período')['Volume'].sum().sum()
                            elif not df_vol_m1_cat.empty:
                                volume_m1_cat = df_vol_m1_cat['Volume'].sum()
                            else:
                                volume_m1_cat = 0
                            
                            if not df_vol_m2_cat.empty and 'Período' in df_vol_m2_cat.columns:
                                volume_m2_cat = df_vol_m2_cat.groupby('Período')['Volume'].sum().sum()
                            elif not df_vol_m2_cat.empty:
                                volume_m2_cat = df_vol_m2_cat['Volume'].sum()
                            else:
                                volume_m2_cat = 0
                            
                            proporcao_volume_cat = volume_m2_cat / volume_m1_cat if volume_m1_cat != 0 else 1.0
                            proporcao_volume_cat = proporcao_volume_cat if pd.notna(proporcao_volume_cat) else 1.0
                        else:
                            # Sem volume, usar proporção 1.0
                            proporcao_volume_cat = 1.0
                        
                        # Calcular Flex Mês 1 para cada categoria (mesma lógica da tabela)
                        for cat in cats_sel:
                            if cat in g1.index:
                                valor_m1_cat = float(g1.get(cat, 0.0))
                                
                                # IMPORTANTE: No modo CPU, precisamos calcular volumes por categoria
                                if tipo_visualizacao == "CPU (Custo por Unidade)" and df_vol_filtrado is not None and not df_vol_filtrado.empty:
                                    # Filtrar volumes por categoria
                                    if chosen_dim_waterfall in df_vol_m1_cat.columns:
                                        volume_m1_cat_especifico = df_vol_m1_cat[df_vol_m1_cat[chosen_dim_waterfall].astype(str) == str(cat)]['Volume'].sum()
                                    else:
                                        volume_m1_cat_especifico = volume_m1_cat  # Usar volume total se não tiver a coluna
                                    
                                    if chosen_dim_waterfall in df_vol_m2_cat.columns:
                                        volume_m2_cat_especifico = df_vol_m2_cat[df_vol_m2_cat[chosen_dim_waterfall].astype(str) == str(cat)]['Volume'].sum()
                                    else:
                                        volume_m2_cat_especifico = volume_m2_cat  # Usar volume total se não tiver a coluna
                                    
                                    proporcao_volume_cat_especifico = volume_m2_cat_especifico / volume_m1_cat_especifico if volume_m1_cat_especifico != 0 else 1.0
                                else:
                                    volume_m1_cat_especifico = volume_m1_cat
                                    volume_m2_cat_especifico = volume_m2_cat
                                    proporcao_volume_cat_especifico = proporcao_volume_cat
                                
                                # Calcular Flex Mês 1 para esta categoria (mesma lógica do total)
                                if 'Custo' in df_m1.columns:
                                    # Filtrar por categoria e separar Fixo/Variável
                                    df_cat_m1 = df_m1[df_m1[chosen_dim_waterfall].astype(str) == str(cat)]
                                    if not df_cat_m1.empty:
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            # Em CPU, usar 'Total' (Custo Total) para calcular Flex
                                            total_fixo_cat = df_cat_m1[df_cat_m1['Custo'] == 'Fixo']['Total'].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Fixo']) > 0 else 0
                                            total_variavel_cat = df_cat_m1[df_cat_m1['Custo'] == 'Variável']['Total'].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Variável']) > 0 else 0
                                        else:
                                            total_fixo_cat = df_cat_m1[df_cat_m1['Custo'] == 'Fixo'][col_valor].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Fixo']) > 0 else 0
                                            total_variavel_cat = df_cat_m1[df_cat_m1['Custo'] == 'Variável'][col_valor].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Variável']) > 0 else 0
                                    else:
                                        total_fixo_cat = 0
                                        total_variavel_cat = valor_m1_cat if tipo_visualizacao != "CPU (Custo por Unidade)" else (valor_m1_cat * volume_m1_cat_especifico if volume_m1_cat_especifico != 0 else 0)
                                else:
                                    # Sem coluna Custo, tudo é variável
                                    total_fixo_cat = 0
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        # Em CPU, reverter para Custo Total
                                        total_variavel_cat = valor_m1_cat * volume_m1_cat_especifico if volume_m1_cat_especifico != 0 else 0
                                    else:
                                        total_variavel_cat = valor_m1_cat
                                
                                # Flex Mês 1 = Fixo (não varia) + Variável × proporção
                                flex_m1_cat_custo = total_fixo_cat + (total_variavel_cat * proporcao_volume_cat_especifico)
                                
                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                    # Converter para CPU: Flex Mês 1 = Flex Mês 1 (Custo Total) / volume_m2
                                    flex_m1_cat = flex_m1_cat_custo / volume_m2_cat_especifico if volume_m2_cat_especifico != 0 else 0
                                    g_flex[cat] = flex_m1_cat
                                else:
                                    # Em Custo Total, usar diretamente
                                    g_flex[cat] = flex_m1_cat_custo
                            else:
                                g_flex[cat] = 0.0
                        
                        # Calcular variações por categoria: Mês 2 - Flex Mês 1 (não Mês 2 - Mês 1)
                        labels_cats = []
                        values_cats = []
                        for cat in cats_sel:
                            if cat in g2.index or cat in g_flex:
                                valor_m2 = float(g2.get(cat, 0.0))
                                valor_flex_m1 = float(g_flex.get(cat, 0.0))
                                # Delta = Mês 2 - Flex Mês 1 (não Mês 2 - Mês 1)
                                delta = valor_m2 - valor_flex_m1
                                if abs(delta) > 1e-9:
                                    labels_cats.append(str(cat))
                                    values_cats.append(float(delta))
                        
                        # Ordenar por valor absoluto
                        if labels_cats:
                            sorted_idx = sorted(range(len(values_cats)), key=lambda i: abs(values_cats[i]), reverse=True)
                            labels_cats = [labels_cats[i] for i in sorted_idx]
                            values_cats = [values_cats[i] for i in sorted_idx]
                            
                            # LIMITAR quantidade de categorias baseado no slider max_cats
                            if len(labels_cats) > max_cats:
                                labels_cats = labels_cats[:max_cats]
                                values_cats = values_cats[:max_cats]
                        
                        # Calcular Flex Volume = Flex Mês 1 - Mês 1 (usando exatamente a mesma lógica das tabelas)
                        # Nas tabelas: Flex Mês 1 - Mês 1 = Flex BUD - BUD
                        # Calcular usando a mesma lógica das tabelas (antes da tabela ser criada)
                        flex_volume_delta = 0
                        
                        try:
                            # CORREÇÃO: Buscar volumes do df_vol_filtrado (mesma lógica das tabelas, linhas 1360-1427)
                            # Verificar se df_vol_filtrado está disponível
                            if df_vol_filtrado is None or df_vol_filtrado.empty:
                                st.warning("⚠️ Dados de volume não disponíveis para cálculo do Flex Volume.")
                                volume_real_m1 = 0
                                volume_real_m2 = 0
                            else:
                                # Filtrar df_vol_filtrado pelos períodos corretos baseado no modo de comparação
                                if modo_comparacao == "Mês a Mês":
                                    if col_mes_waterfall:
                                        df_vol_m1 = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2 = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall].astype(str) == str(mes_final)].copy()
                                    else:
                                        df_vol_m1 = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2 = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_final)].copy()
                                elif modo_comparacao == "Ano a Ano":
                                    df_vol_m1 = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)].copy()
                                    df_vol_m2 = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_final)].copy()
                                elif modo_comparacao == "Semestre":
                                    meses_semestre = {1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                      2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']}
                                    meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
                                    meses_sem_final = meses_semestre.get(semestre_final, [])
                                    df_vol_m1 = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                        (df_vol_filtrado['Período'].isin(meses_sem_inicial))
                                    ].copy()
                                    df_vol_m2 = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                        (df_vol_filtrado['Período'].isin(meses_sem_final))
                                    ].copy()
                                elif modo_comparacao == "Quarter":
                                    meses_trimestre = {1: ['Janeiro', 'Fevereiro', 'Março'],
                                                       2: ['Abril', 'Maio', 'Junho'],
                                                       3: ['Julho', 'Agosto', 'Setembro'],
                                                       4: ['Outubro', 'Novembro', 'Dezembro']}
                                    meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
                                    meses_trim_final = meses_trimestre.get(trimestre_final, [])
                                    df_vol_m1 = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                        (df_vol_filtrado['Período'].isin(meses_trim_inicial))
                                    ].copy()
                                    df_vol_m2 = df_vol_filtrado[
                                        (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                        (df_vol_filtrado['Período'].isin(meses_trim_final))
                                    ].copy()
                                else:
                                    df_vol_m1 = pd.DataFrame()
                                    df_vol_m2 = pd.DataFrame()
                                
                                # Calcular volumes: agrupar por Período primeiro, depois somar (igual ao gráfico)
                                if not df_vol_m1.empty and 'Período' in df_vol_m1.columns:
                                    volume_real_m1 = df_vol_m1.groupby('Período')['Volume'].sum().sum()
                                elif not df_vol_m1.empty:
                                    volume_real_m1 = df_vol_m1['Volume'].sum()
                                else:
                                    volume_real_m1 = 0
                                
                                if not df_vol_m2.empty and 'Período' in df_vol_m2.columns:
                                    volume_real_m2 = df_vol_m2.groupby('Período')['Volume'].sum().sum()
                                elif not df_vol_m2.empty:
                                    volume_real_m2 = df_vol_m2['Volume'].sum()
                                else:
                                    volume_real_m2 = 0
                            
                            # Base_Total = Total do mês 1 em Custo Total
                            # IMPORTANTE: No modo CPU, total_m1_all já está em CPU, então usar df_m1['Total'].sum() diretamente
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                base_total = df_m1['Total'].sum()  # Total em Custo Total
                            else:
                                base_total = total_m1_all  # Já está em Custo Total
                            
                            # Calcular Flex BUD usando a mesma regra das tabelas
                            if 'Custo' in df_m1.columns:
                                # Separar Fixo e Variável
                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                    # Em CPU, usar coluna 'Total' (Custo Total) para calcular Flex
                                    total_fixo_m1 = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum() if len(df_m1[df_m1['Custo'] == 'Fixo']) > 0 else 0
                                    total_variavel_m1 = df_m1[df_m1['Custo'] == 'Variável']['Total'].sum() if len(df_m1[df_m1['Custo'] == 'Variável']) > 0 else 0
                                else:
                                    total_fixo_m1 = df_m1[df_m1['Custo'] == 'Fixo'][col_valor].sum() if len(df_m1[df_m1['Custo'] == 'Fixo']) > 0 else 0
                                    total_variavel_m1 = df_m1[df_m1['Custo'] == 'Variável'][col_valor].sum() if len(df_m1[df_m1['Custo'] == 'Variável']) > 0 else 0
                            else:
                                # Se não tem coluna Custo, tudo é variável
                                total_fixo_m1 = 0
                                total_variavel_m1 = base_total
                            
                            # Calcular proporção de volume (mesma lógica da tabela linha 1415)
                            proporcao_volume = volume_real_m2 / volume_real_m1 if volume_real_m1 != 0 else 1.0
                            proporcao_volume = proporcao_volume if pd.notna(proporcao_volume) else 1.0
                            
                            # Flex Bud Fixo = Base_Total onde Custo == 'Fixo' (não varia) - linha 1420
                            flex_bud_fixo = total_fixo_m1
                            
                            # Flex Bud Variável = Base_Total × proporcao_volume onde Custo == 'Variável' - linha 1421
                            flex_bud_variavel = total_variavel_m1 * proporcao_volume
                            
                            # Flex BUD = Flex Bud Fixo + Flex Bud Variável
                            flex_bud_total = flex_bud_fixo + flex_bud_variavel
                            
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                # Usar os mesmos volumes usados no cálculo de total_m1_all e total_m2_all
                                volume_m1_para_calculo = volume_m1_graph if 'volume_m1_graph' in locals() and volume_m1_graph > 0 else volume_real_m1
                                volume_m2_para_calculo = volume_m2_graph if 'volume_m2_graph' in locals() and volume_m2_graph > 0 else volume_real_m2
                                
                                # Converter para CPU (mesma lógica da tabela linhas 1428-1432)
                                # BUD (Mês 1) = Base_Total (Custo Total) / volume_m1
                                bud = base_total / volume_m1_para_calculo if volume_m1_para_calculo != 0 else 0
                                
                                # Flex BUD (Flex Mês 1) = Flex Bud Total (Custo Total) / volume_m2
                                flex_bud = flex_bud_total / volume_m2_para_calculo if volume_m2_para_calculo != 0 else 0
                                
                                # Flex Mês 1 - Mês 1 = Flex BUD - BUD (subtração)
                                flex_volume_delta = flex_bud - bud
                            else:
                                # Custo Total: Flex Mês 1 - Mês 1 = Flex BUD - BUD (subtração)
                                bud = base_total
                                flex_bud = flex_bud_total
                                
                                flex_volume_delta = flex_bud - bud
                            
                        except Exception as e:
                            flex_volume_delta = 0
                            st.error(f"❌ Erro ao calcular Flex Volume: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())
                        
                        # Garantir que bud e flex_bud existam mesmo se não entrar no try
                        if 'bud' not in locals():
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                # Se não calculou, usar total_m1_all (já está em CPU)
                                bud = total_m1_all
                                flex_bud = total_m1_all
                            else:
                                bud = total_m1_all
                                flex_bud = total_m1_all
                        
                        # Calcular remainder
                        # Agora que as categorias são "Mês 2 - Flex Mês 1", o cálculo é:
                        # Mês 1 + (Flex Mês 1 - Mês 1) + sum(Mês 2 - Flex Mês 1) = Mês 2
                        # No modo CPU, usar bud (BUD em CPU), no modo Custo Total usar total_m1_all
                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                            valor_inicial_para_remainder = bud
                        else:
                            valor_inicial_para_remainder = total_m1_all
                        
                        remainder = round(total_m2_all - (valor_inicial_para_remainder + flex_volume_delta + sum(values_cats)), 2)
                        
                        # IMPORTANTE: No modo CPU, usar bud (BUD em CPU) calculado corretamente
                        # No modo Custo Total, usar total_m1_all diretamente (como estava antes)
                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                            valor_inicial_grafico = bud
                            valor_final_grafico = total_m2_all
                        else:
                            valor_inicial_grafico = total_m1_all
                            valor_final_grafico = total_m2_all
                        
                        # Adicionar "Outros" se remainder for significativo
                        if abs(remainder) >= 0.01:
                            labels_cats.append("Outros")
                            values_cats.append(remainder)
                        
                        # Montar estrutura do waterfall
                        labels_waterfall = [f"{mes_inicial}"]
                        values_waterfall = [valor_inicial_grafico]
                        measures_waterfall = ["absolute"]
                        
                        # Adicionar Flex Volume = Flex Mês 1 - Mês 1 sempre que o valor for diferente de zero
                        # IMPORTANTE: flex_volume_delta já está calculado corretamente (em CPU ou Custo Total conforme o modo)
                        # Usar tolerância muito pequena para garantir que apareça
                        tem_flex_volume_significativo = abs(flex_volume_delta) > 1e-10
                        if tem_flex_volume_significativo:
                            labels_waterfall.append("Flex Mês 1 - Mês 1")  # Nome da barra
                            values_waterfall.append(flex_volume_delta)
                            measures_waterfall.append("relative")
                        
                        # Adicionar categorias (já calculadas como Mês 2 - Flex Mês 1)
                        labels_waterfall.extend(labels_cats)
                        values_waterfall.extend(values_cats)
                        measures_waterfall.extend(["relative"] * len(labels_cats))
                        
                        # Adicionar barra final
                        labels_waterfall.append(f"{mes_final}")
                        values_waterfall.append(valor_final_grafico)
                        measures_waterfall.append("total")
                        
                        # Criar gráfico waterfall
                        theme_base = st.get_option("theme.base") or "light"
                        if theme_base == "dark":
                            text_color = "#FAFAFA"
                        else:
                            text_color = "#000000"
                        grid_color = "rgba(255,255,255,0.12)" if theme_base == "dark" else "rgba(0,0,0,0.12)"
                        
                        # Cores
                        cor_vermelha = "#ff5733"
                        cor_verde = "#1e8449"
                        cor_azul = "#1e6ba8"
                        cor_laranja = "#ff9800"
                        cor_amarela = "#ffd700"  # Amarelo para Flex Volume
                        
                        # Criar anotações
                        annotations_custom = []
                        cumulative = 0
                        
                        for measure, value, label in zip(measures_waterfall, values_waterfall, labels_waterfall):
                            if value >= 0:
                                text_fmt = f"+{value:,.1f}"
                            else:
                                text_fmt = f"{value:,.1f}"
                            
                            if measure == "absolute":
                                y_pos = value
                                cumulative = value
                                # 🔧 Mostrar valor com sinal positivo ou negativo
                                if value >= 0:
                                    text_fmt_abs = f"+{value:,.1f}"
                                else:
                                    text_fmt_abs = f"{value:,.1f}"
                                annotations_custom.append(dict(
                                    x=label, y=y_pos, text=text_fmt_abs,
                                    showarrow=False, font=dict(color=cor_azul, size=8), yshift=15,
                                    xref="x", yref="y"
                                ))
                            elif measure == "relative":
                                if value >= 0:
                                    cor_texto = cor_vermelha
                                    y_pos = cumulative + value
                                    yshift_val = 15
                                else:
                                    cor_texto = cor_verde
                                    y_pos = cumulative + value
                                    yshift_val = -15
                                
                                if label == "Flex Mês 1 - Mês 1":
                                    cor_texto = cor_amarela
                                elif label == "Outros":
                                    cor_texto = cor_laranja
                                
                                annotations_custom.append(dict(
                                    x=label, y=y_pos, text=text_fmt,
                                    showarrow=False, font=dict(color=cor_texto, size=8), yshift=yshift_val,
                                    xref="x", yref="y", yanchor="middle" if value >= 0 else "middle"
                                ))
                                cumulative += value
                            elif measure == "total":
                                y_pos = value
                                # 🔧 Mostrar valor com sinal positivo ou negativo
                                if value >= 0:
                                    text_fmt_total = f"+{value:,.1f}"
                                else:
                                    text_fmt_total = f"{value:,.1f}"
                                annotations_custom.append(dict(
                                    x=label, y=y_pos, text=text_fmt_total,
                                    showarrow=False, font=dict(color=cor_azul, size=8), yshift=20,
                                    xref="x", yref="y", yanchor="bottom"
                                ))
                        
                        # Criar figura do waterfall
                        fig = go.Figure(go.Waterfall(
                            name="Waterfall",
                            orientation="v",
                            measure=measures_waterfall,
                            x=labels_waterfall,
                            y=values_waterfall,
                            textposition="none",
                            connector={"line": {"color": "rgba(0, 0, 0, 0)"}},
                            increasing={"marker": {"color": cor_vermelha, "line": {"width": 0}}},
                            decreasing={"marker": {"color": cor_verde, "line": {"width": 0}}},
                            totals={"marker": {"color": cor_azul, "line": {"width": 0}}}
                        ))
                        
                        # Adicionar overlay para "Flex Mês 1 - Mês 1" (amarelo)
                        if "Flex Mês 1 - Mês 1" in labels_waterfall:
                            idx_flex = labels_waterfall.index("Flex Mês 1 - Mês 1")
                            valor_flex = values_waterfall[idx_flex]
                            
                            # Calcular a posição base exata (mesma lógica do Plotly Waterfall)
                            # Usar valor_inicial_grafico (bud no modo CPU, total_m1_all no modo Custo Total)
                            cumulative_flex = valor_inicial_grafico
                            for i in range(1, idx_flex):
                                cumulative_flex += values_waterfall[i]
                            
                            # Para barras positivas: base = cumulative
                            # Para barras negativas: base = cumulative + valor (porque vai para baixo)
                            if valor_flex >= 0:
                                base_flex = cumulative_flex
                            else:
                                base_flex = cumulative_flex + valor_flex
                            
                            # Adicionar overlay exatamente na mesma posição da barra do waterfall
                            fig.add_trace(go.Bar(
                                x=['Flex Mês 1 - Mês 1'],
                                y=[abs(valor_flex)],
                                base=[base_flex],
                                marker_color=cor_amarela,
                                marker_line=dict(width=2, color=cor_amarela),
                                opacity=1.0,
                                showlegend=False,
                                textposition='none',
                                width=0.8,  # Mesma largura padrão do Plotly Waterfall
                                offsetgroup='1',  # Mesmo grupo do waterfall principal
                                alignmentgroup='1'  # Alinhar com o waterfall principal
                            ))
                        
                        # Adicionar overlay para "Outros" (laranja)
                        if "Outros" in labels_waterfall:
                            idx_outros = labels_waterfall.index("Outros")
                            valor_outros = values_waterfall[idx_outros]
                            
                            # Calcular a posição base exata (mesma lógica do Plotly Waterfall)
                            # Usar valor_inicial_grafico (bud no modo CPU, total_m1_all no modo Custo Total)
                            cumulative_outros = valor_inicial_grafico
                            for i in range(1, idx_outros):
                                cumulative_outros += values_waterfall[i]
                            
                            # Para barras positivas: base = cumulative
                            # Para barras negativas: base = cumulative + valor (porque vai para baixo)
                            if valor_outros >= 0:
                                base_outros = cumulative_outros
                            else:
                                base_outros = cumulative_outros + valor_outros
                            
                            # Adicionar overlay exatamente na mesma posição da barra do waterfall
                            fig.add_trace(go.Bar(
                                x=['Outros'],
                                y=[abs(valor_outros)],
                                base=[base_outros],
                                marker_color=cor_laranja,
                                marker_line=dict(width=2, color=cor_laranja),
                                opacity=1.0,
                                showlegend=False,
                                textposition='none',
                                width=0.8,  # Mesma largura padrão do Plotly Waterfall
                                offsetgroup='1',  # Mesmo grupo do waterfall principal
                                alignmentgroup='1'  # Alinhar com o waterfall principal
                            ))
                        
                        # Calcular range do eixo Y
                        if values_waterfall:
                            cumulative = 0
                            all_y_positions = []
                            
                            for measure, value in zip(measures_waterfall, values_waterfall):
                                if measure == "absolute":
                                    cumulative = value
                                    all_y_positions.append(value)
                                elif measure == "relative":
                                    cumulative += value
                                    all_y_positions.append(cumulative)
                                elif measure == "total":
                                    all_y_positions.append(value)
                            
                            if all_y_positions:
                                min_bar_pos = min(all_y_positions)
                                max_bar_pos = max(all_y_positions)
                                
                                min_ann_pos = min_bar_pos
                                max_ann_pos = max_bar_pos
                                
                                for ann in annotations_custom:
                                    y_ann = ann['y']
                                    yshift = ann.get('yshift', 0)
                                    if yshift > 0:
                                        ann_top = y_ann + abs(y_ann) * 0.08
                                        if ann_top > max_ann_pos:
                                            max_ann_pos = ann_top
                                    elif yshift < 0:
                                        ann_bottom = y_ann - abs(y_ann) * 0.08
                                        if ann_bottom < min_ann_pos:
                                            min_ann_pos = ann_bottom
                                
                                range_span = max_ann_pos - min_ann_pos
                                if range_span > 0:
                                    y_min = min_ann_pos - range_span * 0.10
                                    y_max = max_ann_pos + range_span * 0.10
                                else:
                                    y_min = min_ann_pos * 0.90 if min_ann_pos > 0 else min_ann_pos * 1.10
                                    y_max = max_ann_pos * 1.10 if max_ann_pos > 0 else max_ann_pos * 0.90
                                
                                if min_bar_pos >= 0 and y_min < 0:
                                    y_min = 0
                            else:
                                y_min = 0
                                y_max = 1
                        else:
                            y_min = 0
                            y_max = 1
                        
                        # Atualizar layout
                        titulo_grafico = f"Waterfall Analysis - {tipo_visualizacao}"
                        if modo_comparacao != "Mês a Mês":
                            titulo_grafico += f" ({modo_comparacao})"
                        
                        fig.update_layout(
                            title={
                                "text": titulo_grafico,
                                "x": 0.5,
                                "xanchor": "center",
                                "font": {"size": 12}
                            },
                            xaxis_title="Categoria / Período",
                            yaxis_title=f"{tipo_visualizacao} ({moeda_simbolo})",
                            height=560,
                            showlegend=False,
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=80, r=40, t=50, b=40),
                            font=dict(color=text_color, size=10),
                            xaxis=dict(
                                showgrid=False,
                                zeroline=False,
                                showline=True,
                                linecolor=grid_color,
                                linewidth=1,
                                tickmode='linear',
                                ticklen=5,
                                tickcolor=grid_color,
                                tickwidth=1,
                                ticks="outside",
                                title=dict(font=dict(size=10)),
                                tickfont=dict(size=9)
                            ),
                            yaxis=dict(
                                showgrid=False,
                                zeroline=False,
                                showline=True,
                                linecolor=grid_color,
                                linewidth=1,
                                tickmode='auto',
                                nticks=8,
                                ticklen=5,
                                tickcolor=grid_color,
                                tickwidth=1,
                                ticks="outside",
                                range=[y_min, y_max],
                                tickformat=",.0f",
                                title=dict(font=dict(size=10)),
                                tickfont=dict(size=9)
                            ),
                            annotations=annotations_custom if annotations_custom else []
                        )
                        
                        # Anotação removida - não exibir texto "Flex Mês 1 - Mês 1" no gráfico
                        
                        # Exibir gráfico
                        plotly_chart_safe(fig, use_container_width=True)
                        
                        # Exibir informações resumidas
                        st.markdown("---")
                        st.markdown("""
                            <style>
                            [data-testid="stMetricValue"] {
                                font-size: 0.8rem !important;
                            }
                            [data-testid="stMetricLabel"] {
                                font-size: 0.7rem !important;
                            }
                            [data-testid="stMetricDelta"] {
                                font-size: 0.7rem !important;
                            }
                            </style>
                        """, unsafe_allow_html=True)
                        col1, col2, col3, col4 = st.columns(4)
                        # Calcular variação total
                        # No modo CPU, usar bud (BUD em CPU), no modo Custo Total usar total_m1_all
                        valor_inicial_para_change = bud if tipo_visualizacao == "CPU (Custo por Unidade)" else total_m1_all
                        change_all = total_m2_all - valor_inicial_para_change
                        
                        with col1:
                            st.metric("Período Inicial", mes_inicial, f"{total_m1_all:,.2f}")
                        with col2:
                            st.metric("Período Final", mes_final, f"{total_m2_all:,.2f}")
                        with col3:
                            # Calcular percentual usando o valor inicial correto
                            percentual_change = (change_all / valor_inicial_para_change * 100) if valor_inicial_para_change != 0 else 0
                            st.metric("Variação Total", f"{change_all:,.2f}", f"{percentual_change:.1f}%")
                        with col4:
                            st.metric("Categorias", len(labels_cats), "")
                        
                        # ==========================================
                        # TABELA: Análise Flex Bud por Categoria (usando mês inicial como base)
                        # ==========================================
                        st.markdown("---")
                        st.markdown('<div id="analise-flex-bud-por-categoria-waterfall"></div>', unsafe_allow_html=True)
                        st.subheader(f"📊 Análise Flex por Categoria (Base: {mes_inicial})")
                        
                        # Verificar se temos os dados necessários
                        if 'df_m1' in locals() and df_m1 is not None and len(df_m1) > 0 and 'df_m2' in locals() and df_m2 is not None and len(df_m2) > 0:
                            # Verificar se temos coluna 'Custo' nos dados
                            tem_custo_real = False
                            if 'Custo' in df_m1.columns and 'Custo' in df_m2.columns:
                                tem_custo_real = True
                            
                            if tem_custo_real:
                                try:
                                    # Preparar dados: usar df_m1 como base (equivalente a BUD) e df_m2 como Total
                                    df_real_base = df_m1.copy()
                                    df_real_final = df_m2.copy()
                                    
                                    # IMPORTANTE: df_m1 e df_m2 já vêm de df_filtrado_waterfall que tem a conversão de moeda aplicada
                                    # NÃO aplicar conversão novamente aqui para evitar duplicação
                                    
                                    # Agrupar dados por categoria
                                    colunas_agrupamento = ['Custo']
                                    if 'Type 05' in df_real_base.columns:
                                        colunas_agrupamento.append('Type 05')
                                    if 'Type 06' in df_real_base.columns:
                                        colunas_agrupamento.append('Type 06')
                                    if 'Account' in df_real_base.columns:
                                        colunas_agrupamento.append('Account')
                                    
                                    # Agrupar dados do mês inicial (base)
                                    df_base_agrupado = df_real_base.groupby(colunas_agrupamento)['Total'].sum().reset_index()
                                    df_base_agrupado = df_base_agrupado.rename(columns={'Total': 'Base_Total'})
                                    
                                    # Agrupar dados do mês final
                                    df_final_agrupado = df_real_final.groupby(colunas_agrupamento)['Total'].sum().reset_index()
                                    df_final_agrupado = df_final_agrupado.rename(columns={'Total': 'Total'})
                                    
                                    # Fazer merge
                                    df_tabela_flex_waterfall = df_final_agrupado.merge(
                                        df_base_agrupado,
                                        on=colunas_agrupamento,
                                        how='outer'
                                    )
                                    df_tabela_flex_waterfall['Base_Total'] = df_tabela_flex_waterfall['Base_Total'].fillna(0)
                                    df_tabela_flex_waterfall['Total'] = df_tabela_flex_waterfall['Total'].fillna(0)
                                    
                                    # Obter volumes usando df_vol_filtrado que já foi criado anteriormente com TODOS os filtros aplicados
                                    # IMPORTANTE: Usar o mesmo df_vol_filtrado que foi criado na linha 768 (já tem todos os filtros)
                                    volume_m1 = 0
                                    volume_m2 = 0
                                    
                                    # Usar df_vol_filtrado que já foi criado anteriormente (linha 768) com todos os filtros aplicados
                                    if df_vol_filtrado is not None and not df_vol_filtrado.empty and 'Volume' in df_vol_filtrado.columns:
                                        # Determinar coluna de período (já foi criada no df_vol_filtrado anteriormente)
                                        col_mes_vol = col_mes_waterfall if col_mes_waterfall else 'Período'
                                        
                                        # Filtrar volume para o período inicial (Mês 1)
                                        if modo_comparacao == "Mês a Mês":
                                            if col_mes_vol and col_mes_vol in df_vol_filtrado.columns:
                                                df_vol_m1 = df_vol_filtrado[df_vol_filtrado[col_mes_vol].astype(str) == str(mes_inicial)].copy()
                                            elif 'Período' in df_vol_filtrado.columns:
                                                df_vol_m1 = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_inicial)].copy()
                                            else:
                                                df_vol_m1 = pd.DataFrame()
                                            
                                            if col_mes_vol and col_mes_vol in df_vol_filtrado.columns:
                                                df_vol_m2 = df_vol_filtrado[df_vol_filtrado[col_mes_vol].astype(str) == str(mes_final)].copy()
                                            elif 'Período' in df_vol_filtrado.columns:
                                                df_vol_m2 = df_vol_filtrado[df_vol_filtrado['Período'].astype(str) == str(mes_final)].copy()
                                            else:
                                                df_vol_m2 = pd.DataFrame()
                                                
                                        elif modo_comparacao == "Ano a Ano":
                                            df_vol_m1 = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)].copy()
                                            df_vol_m2 = df_vol_filtrado[df_vol_filtrado['Ano'].astype(str) == str(ano_final)].copy()
                                            
                                        elif modo_comparacao == "Semestre":
                                            meses_semestre = {1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                              2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']}
                                            meses_sem_inicial = meses_semestre.get(semestre_inicial, [])
                                            meses_sem_final = meses_semestre.get(semestre_final, [])
                                            df_vol_m1 = df_vol_filtrado[
                                                (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                                (df_vol_filtrado['Período'].isin(meses_sem_inicial))
                                            ].copy()
                                            df_vol_m2 = df_vol_filtrado[
                                                (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                                (df_vol_filtrado['Período'].isin(meses_sem_final))
                                            ].copy()
                                            
                                        elif modo_comparacao == "Quarter":
                                            meses_trimestre = {1: ['Janeiro', 'Fevereiro', 'Março'],
                                                               2: ['Abril', 'Maio', 'Junho'],
                                                               3: ['Julho', 'Agosto', 'Setembro'],
                                                               4: ['Outubro', 'Novembro', 'Dezembro']}
                                            meses_trim_inicial = meses_trimestre.get(trimestre_inicial, [])
                                            meses_trim_final = meses_trimestre.get(trimestre_final, [])
                                            df_vol_m1 = df_vol_filtrado[
                                                (df_vol_filtrado['Ano'].astype(str) == str(ano_inicial)) &
                                                (df_vol_filtrado['Período'].isin(meses_trim_inicial))
                                            ].copy()
                                            df_vol_m2 = df_vol_filtrado[
                                                (df_vol_filtrado['Ano'].astype(str) == str(ano_final)) &
                                                (df_vol_filtrado['Período'].isin(meses_trim_final))
                                            ].copy()
                                        
                                        # Calcular volumes: agrupar por Período primeiro, depois somar (igual ao gráfico)
                                        if not df_vol_m1.empty and 'Período' in df_vol_m1.columns:
                                            volume_m1 = df_vol_m1.groupby('Período')['Volume'].sum().sum()
                                        elif not df_vol_m1.empty:
                                            volume_m1 = df_vol_m1['Volume'].sum()
                                        else:
                                            volume_m1 = 0
                                        
                                        if not df_vol_m2.empty and 'Período' in df_vol_m2.columns:
                                            volume_m2 = df_vol_m2.groupby('Período')['Volume'].sum().sum()
                                        elif not df_vol_m2.empty:
                                            volume_m2 = df_vol_m2['Volume'].sum()
                                        else:
                                            volume_m2 = 0
                                    
                                    # Calcular Flex Bud usando a mesma lógica do TC Ext
                                    # Fixo: Total Mês 1 (não varia) = Base_Total onde Custo == 'Fixo'
                                    # Variável: Total Mês 1 × (Volume Real Mês 2 / Volume Mês 1) = Base_Total × proporcao onde Custo == 'Variável'
                                    proporcao_volume = volume_m2 / volume_m1 if volume_m1 != 0 else 1.0
                                    proporcao_volume = proporcao_volume if pd.notna(proporcao_volume) else 1.0
                                    
                                    # Calcular Flex Bud Fixo e Variável (mesma lógica do TC Ext)
                                    df_tabela_flex_waterfall['_Proporcao_Volume'] = proporcao_volume
                                    df_tabela_flex_waterfall['_Flex_Bud_Fixo'] = df_tabela_flex_waterfall['Base_Total'].where(df_tabela_flex_waterfall['Custo'] == 'Fixo', 0)
                                    df_tabela_flex_waterfall['_Flex_Bud_Variavel'] = (df_tabela_flex_waterfall['Base_Total'] * df_tabela_flex_waterfall['_Proporcao_Volume']).where(df_tabela_flex_waterfall['Custo'] == 'Variável', 0)
                                    
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        # Calcular Flex Bud em Custo Total primeiro
                                        df_tabela_flex_waterfall['_Flex_Bud_Total_Custo'] = df_tabela_flex_waterfall['_Flex_Bud_Fixo'] + df_tabela_flex_waterfall['_Flex_Bud_Variavel']
                                        
                                        # Converter para CPU: Flex BUD = Flex Bud Total Custo / Volume Real Mês 2
                                        df_tabela_flex_waterfall['Flex BUD'] = df_tabela_flex_waterfall['_Flex_Bud_Total_Custo'] / volume_m2 if volume_m2 != 0 else 0
                                        df_tabela_flex_waterfall['Flex BUD'] = df_tabela_flex_waterfall['Flex BUD'].fillna(0)
                                        
                                        df_tabela_flex_waterfall['BUD'] = df_tabela_flex_waterfall['Base_Total'] / volume_m1 if volume_m1 != 0 else 0
                                        df_tabela_flex_waterfall['BUD'] = df_tabela_flex_waterfall['BUD'].fillna(0)
                                        
                                        df_tabela_flex_waterfall['Total'] = df_tabela_flex_waterfall['Total'] / volume_m2 if volume_m2 != 0 else 0
                                        df_tabela_flex_waterfall['Total'] = df_tabela_flex_waterfall['Total'].fillna(0)
                                        
                                        df_tabela_flex_waterfall['_Flex_Bud_Total'] = df_tabela_flex_waterfall['_Flex_Bud_Total_Custo']
                                        df_tabela_flex_waterfall['_Total_Custo_Total'] = df_tabela_flex_waterfall['Total'] * volume_m2
                                    else:
                                        # Custo Total: Flex BUD = Flex Bud Fixo + Flex Bud Variável
                                        df_tabela_flex_waterfall['Flex BUD'] = df_tabela_flex_waterfall['_Flex_Bud_Fixo'] + df_tabela_flex_waterfall['_Flex_Bud_Variavel']
                                        df_tabela_flex_waterfall['BUD'] = df_tabela_flex_waterfall['Base_Total']
                                        
                                        df_tabela_flex_waterfall['_Flex_Bud_Total'] = df_tabela_flex_waterfall['Flex BUD']
                                        df_tabela_flex_waterfall['_Total_Custo_Total'] = df_tabela_flex_waterfall['Total']
                                    
                                    # Usar nomes de colunas padronizados (genéricos)
                                    # Colunas: Mês 1, Flex Mês 1 - Mês 1, Flex Mês 1, Mês 2 - Flex Mês 1, Mês 2, % Mês 2/Flex Mês 1
                                    
                                    # Formatar nome do período para exibição (não para colunas)
                                    # Formato: para semestre "2024 S1" → "2024/S1", para quarter "2024 Q1" → "2024/Q1", para mês "Setembro 2024" → "Set/24"
                                    if modo_comparacao == "Ano a Ano":
                                        nome_periodo_inicial_display = f"{ano_inicial}"
                                        nome_periodo_final_display = f"{ano_final}"
                                    elif modo_comparacao == "Semestre":
                                        nome_periodo_inicial_display = f"{ano_inicial}/S{semestre_inicial}"
                                        nome_periodo_final_display = f"{ano_final}/S{semestre_final}"
                                    elif modo_comparacao == "Quarter":
                                        nome_periodo_inicial_display = f"{ano_inicial}/Q{trimestre_inicial}"
                                        nome_periodo_final_display = f"{ano_final}/Q{trimestre_final}"
                                    else:  # Mês a Mês
                                        nome_periodo_inicial_display = formatar_periodo_abreviado(mes_inicial)
                                        nome_periodo_final_display = formatar_periodo_abreviado(mes_final)
                                    
                                    # Obter colunas de identificação
                                    colunas_id_waterfall = [col for col in ['Custo', 'Type 05', 'Type 06', 'Account'] if col in df_tabela_flex_waterfall.columns]
                                    
                                    # Criar DataFrame com colunas reorganizadas usando nomes fixos
                                    df_reorganizado = df_tabela_flex_waterfall[colunas_id_waterfall].copy()
                                    
                                    # Adicionar colunas na ordem especificada com nomes padronizados
                                    df_reorganizado['Mês 1'] = df_tabela_flex_waterfall['BUD']
                                    df_reorganizado['Flex Mês 1 - Mês 1'] = df_tabela_flex_waterfall['Flex BUD'] - df_tabela_flex_waterfall['BUD']
                                    df_reorganizado['Flex Mês 1'] = df_tabela_flex_waterfall['Flex BUD']
                                    df_reorganizado['Mês 2 - Flex Mês 1'] = df_tabela_flex_waterfall['Total'] - df_tabela_flex_waterfall['Flex BUD']
                                    df_reorganizado['Mês 2'] = df_tabela_flex_waterfall['Total']
                                    df_reorganizado['% Mês 2/Flex Mês 1'] = df_reorganizado.apply(
                                        lambda row: (row['Mês 2'] / row['Flex Mês 1'] * 100) if row['Flex Mês 1'] != 0 and pd.notnull(row['Flex Mês 1']) else 0,
                                        axis=1
                                    )
                                    
                                    df_tabela_flex_waterfall = df_reorganizado
                                    
                                    # Remover colunas auxiliares (incluindo as que não devem aparecer na tabela)
                                    colunas_remover = ['Base_Total', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel', '_Flex_Bud_Total', '_Total_Custo_Total']
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        colunas_remover.append('_Flex_Bud_Total_Custo')
                                    df_tabela_flex_waterfall = df_tabela_flex_waterfall.drop(columns=[col for col in colunas_remover if col in df_tabela_flex_waterfall.columns])
                                    
                                    # Selecionador de visualização
                                    modo_tabela_flex_waterfall = st.radio(
                                        "📊 **Visualização:**",
                                        ["Fixo/Variável", "Total"],
                                        index=0,
                                        horizontal=True,
                                        key="modo_tabela_flex_waterfall"
                                    )
                                    
                                    # Resumo geral
                                    if len(df_tabela_flex_waterfall) > 0:
                                        linha_resumo_geral = {
                                            'Mês 1': df_tabela_flex_waterfall['Mês 1'].sum(),
                                            'Flex Mês 1 - Mês 1': df_tabela_flex_waterfall['Flex Mês 1 - Mês 1'].sum(),
                                            'Flex Mês 1': df_tabela_flex_waterfall['Flex Mês 1'].sum(),
                                            'Mês 2 - Flex Mês 1': df_tabela_flex_waterfall['Mês 2 - Flex Mês 1'].sum(),
                                            'Mês 2': df_tabela_flex_waterfall['Mês 2'].sum(),
                                            '% Mês 2/Flex Mês 1': df_tabela_flex_waterfall['% Mês 2/Flex Mês 1'].sum()
                                        }
                                        
                                        # Formatar resumo (seguindo a ordem correta das colunas)
                                        linha_resumo_geral_formatado = {}
                                        # Ordem correta: Mês 1, Flex Mês 1 - Mês 1, Flex Mês 1, Mês 2 - Flex Mês 1, Mês 2, % Mês 2/Flex Mês 1
                                        colunas_resumo_ordenadas = [
                                            'Mês 1',
                                            'Flex Mês 1 - Mês 1',
                                            'Flex Mês 1',
                                            'Mês 2 - Flex Mês 1',
                                            'Mês 2',
                                            '% Mês 2/Flex Mês 1'
                                        ]
                                        for col in colunas_resumo_ordenadas:
                                            if col.startswith('%'):
                                                linha_resumo_geral_formatado[col] = f"{linha_resumo_geral[col]:,.2f}%"
                                            elif tipo_visualizacao == "CPU (Custo por Unidade)":
                                                linha_resumo_geral_formatado[col] = f"{linha_resumo_geral[col]:,.2f}"
                                            else:
                                                sufixo = ""
                                                if fator_conversao:
                                                    if fator_conversao == "K (milhares)":
                                                        sufixo = " K"
                                                    elif fator_conversao == "M (Milhões)":
                                                        sufixo = " M"
                                                linha_resumo_geral_formatado[col] = f"{linha_resumo_geral[col]:,.2f}{sufixo}"
                                        
                                        st.markdown("---")
                                        st.markdown("### 📊 Resumo Geral")
                                        # Exibir informação dos períodos selecionados
                                        st.markdown(f"**Mês 1:** {nome_periodo_inicial_display} | **Mês 2:** {nome_periodo_final_display}")
                                        st.markdown("<br>", unsafe_allow_html=True)
                                        # Exibir caixas na ordem correta: Mês 1, Flex Mês 1 - Mês 1, Flex Mês 1, Mês 2 - Flex Mês 1, Mês 2, % Mês 2/Flex Mês 1
                                        ordem_colunas_waterfall = [
                                            'Mês 1',
                                            'Flex Mês 1 - Mês 1',
                                            'Flex Mês 1',
                                            'Mês 2 - Flex Mês 1',
                                            'Mês 2',
                                            '% Mês 2/Flex Mês 1'
                                        ]
                                        num_colunas = min(len(ordem_colunas_waterfall), 6)
                                        if num_colunas > 0:
                                            cols = st.columns(num_colunas, gap="small")
                                            for idx, col_nome in enumerate(ordem_colunas_waterfall[:num_colunas]):
                                                if col_nome in linha_resumo_geral_formatado:
                                                    with cols[idx]:
                                                        valor_formatado = linha_resumo_geral_formatado.get(col_nome, '-')
                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                        
                                        # Adicionar linha de volumes abaixo do resumo geral
                                        st.markdown("<br>", unsafe_allow_html=True)
                                        # Usar os mesmos volumes já calculados
                                        # Formatar volumes (sempre exibir, mesmo se zero)
                                        try:
                                            volume_m1_val = float(volume_m1) if pd.notna(volume_m1) else 0.0
                                            volume_m2_val = float(volume_m2) if pd.notna(volume_m2) else 0.0
                                        except (ValueError, TypeError):
                                            volume_m1_val = 0.0
                                            volume_m2_val = 0.0
                                        
                                        volume_m1_formatado = f"{volume_m1_val:,.0f}"
                                        volume_m2_formatado = f"{volume_m2_val:,.0f}"
                                        
                                        col_vol1, col_vol2 = st.columns(2, gap="small")
                                        with col_vol1:
                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Mês 1 ({nome_periodo_inicial_display}):</strong> {volume_m1_formatado}</div>", unsafe_allow_html=True)
                                        with col_vol2:
                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Mês 2 ({nome_periodo_final_display}):</strong> {volume_m2_formatado}</div>", unsafe_allow_html=True)
                                        st.markdown("<br>", unsafe_allow_html=True)
                                    
                                    # Criar estrutura hierárquica
                                    if modo_tabela_flex_waterfall == "Fixo/Variável":
                                        for custo in ['Fixo', 'Variável']:
                                            df_custo = df_tabela_flex_waterfall[df_tabela_flex_waterfall['Custo'] == custo].copy()
                                            
                                            if len(df_custo) > 0:
                                                total_custo = df_custo['Mês 2'].sum() if 'Mês 2' in df_custo.columns else 0
                                                total_custo_formatado = f"{total_custo:,.2f}"
                                                
                                                # Não exibir se o total for zero
                                                if total_custo != 0 and pd.notna(total_custo):
                                                    with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=False):
                                                        # Nível 2: Type 05
                                                        if 'Type 05' in df_custo.columns:
                                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                                
                                                                if len(df_type05) > 0:
                                                                    total_type05 = df_type05['Mês 2'].sum() if 'Mês 2' in df_type05.columns else 0
                                                                    total_type05_formatado = f"{total_type05:,.2f}"
                                                                    
                                                                    # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                                    # A filtragem de linhas zeradas já é feita dentro do loop do Type 06
                                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                                        # Nível 3: Type 06
                                                                        if 'Type 06' in df_type05.columns:
                                                                                for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                                    df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                                    
                                                                                    if len(df_type06) > 0:
                                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores não zerados
                                                                                        colunas_numericas_check = [col for col in df_type06.columns 
                                                                                                                  if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                                  and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                        if colunas_numericas_check:
                                                                                            # Verificar se há pelo menos uma linha com valores não zerados
                                                                                            df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                                            tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                                            if not tem_valores_nao_zerados:
                                                                                                continue  # Pular Type 06 completamente zerado
                                                                                        else:
                                                                                            # Se não há colunas numéricas, verificar se Mês 2 existe e é zero
                                                                                            if 'Mês 2' in df_type06.columns:
                                                                                                if df_type06['Mês 2'].fillna(0).abs().sum() <= 0.0001:
                                                                                                    continue  # Pular Type 06 completamente zerado
                                                                                        
                                                                                        # Verificar se a coluna 'Mês 2' existe antes de acessá-la
                                                                                        if 'Mês 2' in df_type06.columns:
                                                                                            total_type06 = df_type06['Mês 2'].sum()
                                                                                        else:
                                                                                            total_type06 = 0.0
                                                                                        total_type06_formatado = f"{total_type06:,.2f}"
                                                                                        
                                                                                        # Nível 4: Account (se existir)
                                                                                        if 'Account' in df_type06.columns:
                                                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                            if colunas_numericas:
                                                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                                                df_type06_filtrado = df_type06[
                                                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                                                ].copy()
                                                                                            else:
                                                                                                df_type06_filtrado = df_type06.copy()
                                                                                            
                                                                                            # Só exibir se houver dados após filtrar
                                                                                            if len(df_type06_filtrado) > 0:
                                                                                                # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                                                st.markdown("---")  # Separador visual
                                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                                with st.container():
                                                                                                        # Criar tabela
                                                                                                        colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                                        colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                                            if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                                                        colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                                        colunas_display = colunas_id + colunas_ordenadas
                                                                                                        df_display = df_type06_filtrado[colunas_display].copy()
                                                                                                        
                                                                                                        # Formatar valores
                                                                                                        for col in df_display.columns:
                                                                                                            if col not in colunas_id:
                                                                                                                if col.startswith('%'):
                                                                                                                    df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                                                elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                                    else:
                                                                                                                        sufixo = ""
                                                                                                                        if fator_conversao:
                                                                                                                            if fator_conversao == "K (milhares)":
                                                                                                                                sufixo = " K"
                                                                                                                            elif fator_conversao == "M (Milhões)":
                                                                                                                                sufixo = " M"
                                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                                        
                                                                                                        # Calcular resumo diretamente com nomes das colunas dinâmicas
                                                                                                        linha_resumo_type06 = {}
                                                                                                        linha_resumo_formatado_type06 = {}
                                                                                                        
                                                                                                        # Calcular valores usando as colunas dinâmicas
                                                                                                        linha_resumo_type06['Mês 1'] = df_type06_filtrado['Mês 1'].sum()
                                                                                                        linha_resumo_type06['Flex Mês 1 - Mês 1'] = df_type06_filtrado['Flex Mês 1 - Mês 1'].sum()
                                                                                                        linha_resumo_type06['Flex Mês 1'] = df_type06_filtrado['Flex Mês 1'].sum()
                                                                                                        linha_resumo_type06['Mês 2 - Flex Mês 1'] = df_type06_filtrado['Mês 2 - Flex Mês 1'].sum()
                                                                                                        linha_resumo_type06['Mês 2'] = df_type06_filtrado['Mês 2'].sum()
                                                                                                        linha_resumo_type06['% Mês 2/Flex Mês 1'] = df_type06_filtrado['% Mês 2/Flex Mês 1'].sum()
                                                                                                        
                                                                                                        # Formatar valores
                                                                                                        for col in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2']:
                                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}"
                                                                                                            else:
                                                                                                                sufixo = ""
                                                                                                                if fator_conversao:
                                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                                        sufixo = " K"
                                                                                                                    elif fator_conversao == "M (Milhões)":
                                                                                                                        sufixo = " M"
                                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}{sufixo}"
                                                                                                        
                                                                                                        # Formatar percentual
                                                                                                        linha_resumo_formatado_type06['% Mês 2/Flex Mês 1'] = f"{linha_resumo_type06['% Mês 2/Flex Mês 1']:,.2f}%"
                                                                                                        
                                                                                                        # Exibir resumo e tabela usando ordem explícita
                                                                                                        ordem_colunas_waterfall = [
                                                                                                            'Mês 1',
                                                                                                            'Flex Mês 1 - Mês 1',
                                                                                                            'Flex Mês 1',
                                                                                                            'Mês 2 - Flex Mês 1',
                                                                                                            'Mês 2',
                                                                                                            '% Mês 2/Flex Mês 1'
                                                                                                        ]
                                                                                                        num_colunas = min(len(ordem_colunas_waterfall), 6)
                                                                                                        if num_colunas > 0:
                                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall[:num_colunas]):
                                                                                                                if col_nome in linha_resumo_formatado_type06:
                                                                                                                    with cols[idx]:
                                                                                                                        valor_formatado = linha_resumo_formatado_type06.get(col_nome, '-')
                                                                                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                                        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                                        st.markdown(html_table, unsafe_allow_html=True)
                                                                                        else:
                                                                                            # Sem Account: exibir Type 06 diretamente
                                                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                            if colunas_numericas:
                                                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                                                df_type06_filtrado = df_type06[
                                                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                                                ].copy()
                                                                                            else:
                                                                                                df_type06_filtrado = df_type06.copy()
                                                                                            
                                                                                            # Só exibir se houver dados após filtrar
                                                                                            if len(df_type06_filtrado) > 0:
                                                                                                # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                                                st.markdown("---")  # Separador visual
                                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                                with st.container():
                                                                                                        colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                                                        colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                                            if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'Período']]
                                                                                                        colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                                        colunas_display = colunas_id + colunas_ordenadas
                                                                                                        df_display = df_type06_filtrado[colunas_display].copy()
                                                                                                        
                                                                                                        # Formatar valores
                                                                                                        for col in df_display.columns:
                                                                                                            if col not in colunas_id:
                                                                                                                if col.startswith('%'):
                                                                                                                    df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                                                elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                                    else:
                                                                                                                        sufixo = ""
                                                                                                                        if fator_conversao:
                                                                                                                            if fator_conversao == "K (milhares)":
                                                                                                                                sufixo = " K"
                                                                                                                            elif fator_conversao == "M (Milhões)":
                                                                                                                                sufixo = " M"
                                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                                        
                                                                                                        # Calcular resumo diretamente com nomes das colunas dinâmicas
                                                                                                        linha_resumo_type06 = {}
                                                                                                        linha_resumo_formatado_type06 = {}
                                                                                                        
                                                                                                        # Calcular valores usando as colunas dinâmicas
                                                                                                        linha_resumo_type06['Mês 1'] = df_type06_filtrado['Mês 1'].sum()
                                                                                                        linha_resumo_type06['Flex Mês 1 - Mês 1'] = df_type06_filtrado['Flex Mês 1 - Mês 1'].sum()
                                                                                                        linha_resumo_type06['Flex Mês 1'] = df_type06_filtrado['Flex Mês 1'].sum()
                                                                                                        linha_resumo_type06['Mês 2 - Flex Mês 1'] = df_type06_filtrado['Mês 2 - Flex Mês 1'].sum()
                                                                                                        linha_resumo_type06['Mês 2'] = df_type06_filtrado['Mês 2'].sum()
                                                                                                        linha_resumo_type06['% Mês 2/Flex Mês 1'] = df_type06_filtrado['% Mês 2/Flex Mês 1'].sum()
                                                                                                        
                                                                                                        # Formatar valores
                                                                                                        for col in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2']:
                                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}"
                                                                                                            else:
                                                                                                                sufixo = ""
                                                                                                                if fator_conversao:
                                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                                        sufixo = " K"
                                                                                                                    elif fator_conversao == "M (Milhões)":
                                                                                                                        sufixo = " M"
                                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}{sufixo}"
                                                                                                        
                                                                                                        # Formatar percentual
                                                                                                        linha_resumo_formatado_type06['% Mês 2/Flex Mês 1'] = f"{linha_resumo_type06['% Mês 2/Flex Mês 1']:,.2f}%"
                                                                                                        
                                                                                                        # Exibir resumo usando ordem explícita
                                                                                                        ordem_colunas_waterfall = [
                                                                                                            'Mês 1',
                                                                                                            'Flex Mês 1 - Mês 1',
                                                                                                            'Flex Mês 1',
                                                                                                            'Mês 2 - Flex Mês 1',
                                                                                                            'Mês 2',
                                                                                                            '% Mês 2/Flex Mês 1'
                                                                                                        ]
                                                                                                        num_colunas = min(len(ordem_colunas_waterfall), 6)
                                                                                                        if num_colunas > 0:
                                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall[:num_colunas]):
                                                                                                                if col_nome in linha_resumo_formatado_type06:
                                                                                                                    with cols[idx]:
                                                                                                                        valor_formatado = linha_resumo_formatado_type06.get(col_nome, '-')
                                                                                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                                        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                                        st.markdown(html_table, unsafe_allow_html=True)
                                                                                            else:
                                                                                                # Sem dados filtrados: não exibir nada
                                                                                                pass
                                                                                    else:
                                                                                        # Sem Type 06: exibir Type 05 diretamente
                                                                                        colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                                                        colunas_numericas = [col for col in df_type05.columns 
                                                                                                            if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'Período']]
                                                                                        colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                        colunas_display = colunas_id + colunas_ordenadas
                                                                                        df_display = df_type05[colunas_display].copy()
                                                                                        
                                                                                        # Formatar valores
                                                                                        for col in df_display.columns:
                                                                                            if col not in colunas_id:
                                                                                                if col.startswith('%'):
                                                                                                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}%" if pd.notna(x) and isinstance(x, (int, float)) else (x if pd.notna(x) else "-"))
                                                                                                elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                    else:
                                                                                                        sufixo = ""
                                                                                                        if fator_conversao:
                                                                                                            if fator_conversao == "K (milhares)":
                                                                                                                sufixo = " K"
                                                                                                            elif fator_conversao == "M (Milhões)":
                                                                                                                sufixo = " M"
                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                        
                                                                                        # Calcular resumo usando os nomes corretos das colunas
                                                                                        linha_resumo_type05 = {
                                                                                            'Mês 1': df_type05['Mês 1'].sum(),
                                                                                            'Flex Mês 1 - Mês 1': df_type05['Flex Mês 1 - Mês 1'].sum(),
                                                                                            'Flex Mês 1': df_type05['Flex Mês 1'].sum(),
                                                                                            'Mês 2 - Flex Mês 1': df_type05['Mês 2 - Flex Mês 1'].sum(),
                                                                                            'Mês 2': df_type05['Mês 2'].sum(),
                                                                                            '% Mês 2/Flex Mês 1': df_type05['% Mês 2/Flex Mês 1'].sum()
                                                                                        }
                                                                                        
                                                                                        # Formatar resumo
                                                                                        linha_resumo_type05_formatado = {}
                                                                                        for col in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2']:
                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                linha_resumo_type05_formatado[col] = f"{linha_resumo_type05[col]:,.2f}"
                                                                                            else:
                                                                                                sufixo = ""
                                                                                                if fator_conversao:
                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                        sufixo = " K"
                                                                                                    elif fator_conversao == "M (Milhões)":
                                                                                                        sufixo = " M"
                                                                                                linha_resumo_type05_formatado[col] = f"{linha_resumo_type05[col]:,.2f}{sufixo}"
                                                                                        
                                                                                        # Formatar percentual
                                                                                        linha_resumo_type05_formatado['% Mês 2/Flex Mês 1'] = f"{linha_resumo_type05['% Mês 2/Flex Mês 1']:,.2f}%"
                                                                                        
                                                                                        # Exibir caixas na ordem correta: Mês 1, Flex Mês 1 - Mês 1, Flex Mês 1, Mês 2 - Flex Mês 1, Mês 2, % Mês 2/Flex Mês 1
                                                                                        ordem_colunas_waterfall = [
                                                                                            'Mês 1',
                                                                                            'Flex Mês 1 - Mês 1',
                                                                                            'Flex Mês 1',
                                                                                            'Mês 2 - Flex Mês 1',
                                                                                            'Mês 2',
                                                                                            '% Mês 2/Flex Mês 1'
                                                                                        ]
                                                                                        num_colunas = min(len(ordem_colunas_waterfall), 6)
                                                                                        if num_colunas > 0:
                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall[:num_colunas]):
                                                                                                if col_nome in linha_resumo_type05_formatado:
                                                                                                    with cols[idx]:
                                                                                                        valor_formatado = linha_resumo_type05_formatado.get(col_nome, '-')
                                                                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                        
                                                                                        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_type05_formatado)
                                                                                        st.markdown(html_table, unsafe_allow_html=True)
                                    else:
                                        # Modo "Total": estrutura hierárquica sem separação Fixo/Variável
                                        # Agrupar por Type 05, Type 06, Account (somando Fixo + Variável)
                                        colunas_agrupamento_total = []
                                        if 'Type 05' in df_tabela_flex_waterfall.columns:
                                            colunas_agrupamento_total.append('Type 05')
                                        if 'Type 06' in df_tabela_flex_waterfall.columns:
                                            colunas_agrupamento_total.append('Type 06')
                                        if 'Account' in df_tabela_flex_waterfall.columns:
                                            colunas_agrupamento_total.append('Account')
                                        
                                        if len(colunas_agrupamento_total) > 0:
                                            df_tabela_total_agrupado = df_tabela_flex_waterfall.groupby(colunas_agrupamento_total).agg({
                                                'Mês 1': 'sum',
                                                'Flex Mês 1 - Mês 1': 'sum',
                                                'Flex Mês 1': 'sum',
                                                'Mês 2 - Flex Mês 1': 'sum',
                                                'Mês 2': 'sum',
                                                '% Mês 2/Flex Mês 1': 'sum'
                                            }).reset_index()
                                            
                                            # Recalcular percentual após agrupamento
                                            df_tabela_total_agrupado['% Mês 2/Flex Mês 1'] = df_tabela_total_agrupado.apply(
                                                lambda row: (row['Mês 2'] / row['Flex Mês 1'] * 100) if row['Flex Mês 1'] != 0 and pd.notnull(row['Flex Mês 1']) else 0,
                                                axis=1
                                            )
                                        else:
                                            df_tabela_total_agrupado = df_tabela_flex_waterfall.copy()
                                        
                                        # Filtrar linhas zeradas
                                        colunas_numericas = [col for col in df_tabela_total_agrupado.columns 
                                                            if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Período']]
                                        if colunas_numericas:
                                            df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas].fillna(0)
                                            df_tabela_total_agrupado = df_tabela_total_agrupado[
                                                df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                            ].copy()
                                        
                                        # Estrutura hierárquica com expanders (sem Custo)
                                        if 'Type 05' in df_tabela_total_agrupado.columns:
                                            for type05 in sorted(df_tabela_total_agrupado['Type 05'].dropna().unique()):
                                                df_type05 = df_tabela_total_agrupado[df_tabela_total_agrupado['Type 05'] == type05].copy()
                                                
                                                if len(df_type05) > 0:
                                                    total_type05 = df_type05['Mês 2'].sum()
                                                    total_type05_formatado = f"{total_type05:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type05:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type05:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type05:,.2f}")
                                                    
                                                    # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                        # Nível 2: Type 06
                                                        if 'Type 06' in df_type05.columns:
                                                                for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                    df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                    
                                                                    if len(df_type06) > 0:
                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores não zerados
                                                                        colunas_numericas_check = [col for col in df_type06.columns 
                                                                                                  if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                  and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                        if colunas_numericas_check:
                                                                            df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                            tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                            if not tem_valores_nao_zerados:
                                                                                continue  # Pular Type 06 completamente zerado
                                                                        else:
                                                                            if 'Mês 2' in df_type06.columns:
                                                                                if df_type06['Mês 2'].fillna(0).abs().sum() <= 0.0001:
                                                                                    continue  # Pular Type 06 completamente zerado
                                                                        
                                                                        # Verificar se a coluna 'Mês 2' existe antes de acessá-la
                                                                        if 'Mês 2' in df_type06.columns:
                                                                            total_type06 = df_type06['Mês 2'].sum()
                                                                        else:
                                                                            total_type06 = 0.0
                                                                        total_type06_formatado = f"{total_type06:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type06:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type06:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type06:,.2f}")
                                                                        
                                                                        # Nível 3: Account (se existir)
                                                                        if 'Account' in df_type06.columns:
                                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                            if colunas_numericas:
                                                                                df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                                df_type06_filtrado = df_type06[
                                                                                    df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                                ].copy()
                                                                            else:
                                                                                df_type06_filtrado = df_type06.copy()
                                                                            
                                                                            # Só exibir se houver dados após filtrar
                                                                            if len(df_type06_filtrado) > 0:
                                                                                # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                                # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                                st.markdown("---")  # Separador visual
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                with st.container():
                                                                                    # Criar tabela
                                                                                        colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                        colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                            if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Período']]
                                                                                        colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                        colunas_display = colunas_id + colunas_ordenadas
                                                                                        df_display = df_type06_filtrado[colunas_display].copy()
                                                                                        
                                                                                        # Formatar valores
                                                                                        for col in df_display.columns:
                                                                                            if col not in colunas_id:
                                                                                                if col.startswith('%'):
                                                                                                    df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                                elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                    else:
                                                                                                        sufixo = ""
                                                                                                        if fator_conversao:
                                                                                                            if fator_conversao == "K (milhares)":
                                                                                                                sufixo = " K"
                                                                                                            elif fator_conversao == "M (Milhões)":
                                                                                                                sufixo = " M"
                                                                                                        df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                        
                                                                                        # Calcular resumo
                                                                                        linha_resumo_type06 = {}
                                                                                        linha_resumo_formatado_type06 = {}
                                                                                        
                                                                                        linha_resumo_type06['Mês 1'] = df_type06_filtrado['Mês 1'].sum()
                                                                                        linha_resumo_type06['Flex Mês 1 - Mês 1'] = df_type06_filtrado['Flex Mês 1 - Mês 1'].sum()
                                                                                        linha_resumo_type06['Flex Mês 1'] = df_type06_filtrado['Flex Mês 1'].sum()
                                                                                        linha_resumo_type06['Mês 2 - Flex Mês 1'] = df_type06_filtrado['Mês 2 - Flex Mês 1'].sum()
                                                                                        linha_resumo_type06['Mês 2'] = df_type06_filtrado['Mês 2'].sum()
                                                                                        linha_resumo_type06['% Mês 2/Flex Mês 1'] = df_type06_filtrado['% Mês 2/Flex Mês 1'].sum()
                                                                                        
                                                                                        # Formatar valores
                                                                                        for col in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2']:
                                                                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}"
                                                                                            else:
                                                                                                sufixo = ""
                                                                                                if fator_conversao:
                                                                                                    if fator_conversao == "K (milhares)":
                                                                                                        sufixo = " K"
                                                                                                    elif fator_conversao == "M (Milhões)":
                                                                                                        sufixo = " M"
                                                                                                linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}{sufixo}"
                                                                                        
                                                                                        linha_resumo_formatado_type06['% Mês 2/Flex Mês 1'] = f"{linha_resumo_type06['% Mês 2/Flex Mês 1']:,.2f}%"
                                                                                        
                                                                                        # Exibir resumo e tabela
                                                                                        ordem_colunas_waterfall = [
                                                                                            'Mês 1',
                                                                                            'Flex Mês 1 - Mês 1',
                                                                                            'Flex Mês 1',
                                                                                            'Mês 2 - Flex Mês 1',
                                                                                            'Mês 2',
                                                                                            '% Mês 2/Flex Mês 1'
                                                                                        ]
                                                                                        num_colunas = min(len(ordem_colunas_waterfall), 6)
                                                                                        if num_colunas > 0:
                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall[:num_colunas]):
                                                                                                if col_nome in linha_resumo_formatado_type06:
                                                                                                    with cols[idx]:
                                                                                                        valor_formatado = linha_resumo_formatado_type06.get(col_nome, '-')
                                                                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                        
                                                                                        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                        st.markdown(html_table, unsafe_allow_html=True)
                                        else:
                                            # Sem Type 05: não exibir nada (não deve acontecer)
                                            st.info("ℹ️ Dados sem estrutura hierárquica (Type 05).")
                                
                                except Exception as e:
                                    st.error(f"❌ Erro ao gerar tabela: {str(e)}")
                                    import traceback
                                    st.code(traceback.format_exc())
                            else:
                                st.info("ℹ️ A tabela requer dados com coluna 'Custo' (Fixo/Variável).")
                        else:
                            st.info("ℹ️ Selecione os períodos para comparação acima para visualizar a tabela.")
                
                except Exception as e:
                    st.error(f"❌ Erro ao gerar gráfico waterfall: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
            
            # TAB BUDGET
            with tab_budget:
                st.subheader("💰 Análise Budget")
                
                # Carregar dados de budget e aplicar mesmos filtros
                try:
                    # Carregar dados de budget
                    df_budget = load_budget_data(ano_selecionado)
                    df_budget_vol = load_budget_volume_data(ano_selecionado)
                    
                    if df_budget is None or df_budget.empty:
                        st.warning("⚠️ Dados de budget não disponíveis.")
                    elif df_budget_vol is None or df_budget_vol.empty:
                        st.warning("⚠️ Dados de volume de budget não disponíveis.")
                    else:
                        # Preparar dados para análise (usar df_filtrado_waterfall)
                        df_analise_budget = df_filtrado_waterfall.copy() if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0 else pd.DataFrame()
                        
                        if df_analise_budget.empty:
                            st.warning("⚠️ Nenhum dado real disponível para comparação.")
                        else:
                            # Seleção de período (pode selecionar múltiplos)
                            st.markdown("### 📅 Seleção de Período")
                            
                            # Modo de agregação
                            modo_agregacao_budget = st.radio(
                                "Agrupar por:",
                                options=["Mês", "Semestre", "Quarter"],
                                index=0,
                                horizontal=True,
                                key="modo_agregacao_budget"
                            )
                            
                            # Obter períodos disponíveis
                            if col_mes_waterfall == 'Período_Ano':
                                periodos_disponiveis_budget = sort_mes_unique_waterfall(df_analise_budget['Período_Ano'].dropna().unique().tolist())
                            elif 'Período' in df_analise_budget.columns:
                                periodos_disponiveis_budget = sort_mes_unique_waterfall(df_analise_budget['Período'].dropna().unique().tolist())
                            else:
                                periodos_disponiveis_budget = []
                            
                            if not periodos_disponiveis_budget:
                                st.warning("⚠️ Nenhum período encontrado nos dados.")
                            else:
                                if modo_agregacao_budget == "Mês":
                                    periodos_selecionados_budget = st.multiselect(
                                        "Selecione o(s) período(s):",
                                        periodos_disponiveis_budget,
                                        default=[periodos_disponiveis_budget[-1]] if len(periodos_disponiveis_budget) > 0 else [],
                                        key="periodos_budget_waterfall"
                                    )
                                elif modo_agregacao_budget == "Semestre":
                                    if 'Ano' in df_analise_budget.columns:
                                        anos_disponiveis_budget = sorted(df_analise_budget['Ano'].dropna().unique().tolist())
                                        if len(anos_disponiveis_budget) > 0:
                                            col_ano_sem, col_sem = st.columns(2)
                                            with col_ano_sem:
                                                ano_sem_budget = st.selectbox("Ano:", anos_disponiveis_budget, index=0, key="ano_sem_budget")
                                            with col_sem:
                                                semestre_budget = st.selectbox("Semestre:", [1, 2], index=0, key="semestre_budget")
                                            
                                            meses_semestre = {
                                                1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
                                            }
                                            meses_sem = meses_semestre.get(semestre_budget, [])
                                            periodos_selecionados_budget = df_analise_budget[
                                                (df_analise_budget['Ano'].astype(str) == str(ano_sem_budget)) &
                                                (df_analise_budget['Período'].isin(meses_sem))
                                            ]['Período_Ano' if col_mes_waterfall == 'Período_Ano' else 'Período'].dropna().unique().tolist()
                                        else:
                                            periodos_selecionados_budget = []
                                    else:
                                        st.warning("⚠️ Coluna 'Ano' não encontrada para agrupamento por semestre.")
                                        periodos_selecionados_budget = []
                                elif modo_agregacao_budget == "Quarter":
                                    if 'Ano' in df_analise_budget.columns:
                                        anos_disponiveis_budget = sorted(df_analise_budget['Ano'].dropna().unique().tolist())
                                        if len(anos_disponiveis_budget) > 0:
                                            col_ano_q, col_q = st.columns(2)
                                            with col_ano_q:
                                                ano_q_budget = st.selectbox("Ano:", anos_disponiveis_budget, index=0, key="ano_q_budget")
                                            with col_q:
                                                quarter_budget = st.selectbox("Quarter:", [1, 2, 3, 4], index=0, key="quarter_budget")
                                            
                                            meses_trimestre = {
                                                1: ['Janeiro', 'Fevereiro', 'Março'],
                                                2: ['Abril', 'Maio', 'Junho'],
                                                3: ['Julho', 'Agosto', 'Setembro'],
                                                4: ['Outubro', 'Novembro', 'Dezembro']
                                            }
                                            meses_q = meses_trimestre.get(quarter_budget, [])
                                            periodos_selecionados_budget = df_analise_budget[
                                                (df_analise_budget['Ano'].astype(str) == str(ano_q_budget)) &
                                                (df_analise_budget['Período'].isin(meses_q))
                                            ]['Período_Ano' if col_mes_waterfall == 'Período_Ano' else 'Período'].dropna().unique().tolist()
                                        else:
                                            periodos_selecionados_budget = []
                                    else:
                                        st.warning("⚠️ Coluna 'Ano' não encontrada para agrupamento por quarter.")
                                        periodos_selecionados_budget = []
                                
                                if not periodos_selecionados_budget:
                                    st.warning("⚠️ Nenhum período selecionado.")
                                else:
                                    st.markdown("---")
                                
                                # Obter dimensões de categoria disponíveis
                                dims_cat_budget = [c for c in ["Type 05", "Type 06", "Type 07", "Oficina", "Veículo", "Custo", "Account"] if c in df_analise_budget.columns]
                                
                                if not dims_cat_budget:
                                    st.warning("⚠️ Nenhuma dimensão de categoria encontrada nos dados.")
                                else:
                                    # Filtro de dimensão da categoria
                                    chosen_dim_budget = st.selectbox(
                                        "Dimensão da categoria:",
                                        dims_cat_budget,
                                        index=min(1, len(dims_cat_budget)-1) if len(dims_cat_budget) > 1 else 0,
                                        key="dim_waterfall_budget"
                                    )
                                    
                                    # Filtrar dados pelos períodos selecionados
                                    if col_mes_waterfall == 'Período_Ano':
                                        df_temp_budget = df_analise_budget[df_analise_budget[col_mes_waterfall].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                    elif 'Período' in df_analise_budget.columns:
                                        df_temp_budget = df_analise_budget[df_analise_budget['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                    else:
                                        df_temp_budget = df_analise_budget.copy()
                                    
                                    # Obter todas as categorias disponíveis
                                    cats_all_budget = sorted([str(x).strip() for x in df_analise_budget[chosen_dim_budget].dropna().unique().tolist() if str(x).strip() != ""])
                                    total_cats_budget = max(1, len(cats_all_budget))
                                    
                                    # Controle: Quantidade de categorias a exibir (Top N)
                                    max_cats_limit_budget = min(total_cats_budget, 20)
                                    default_value_budget = min(total_cats_budget, 20)
                                    max_cats_budget = st.slider(
                                        f"Quantidade de categorias a exibir (Top N) (Total: {total_cats_budget}):",
                                        min_value=1,
                                        max_value=20,
                                        value=default_value_budget,
                                        key="max_cats_budget_waterfall"
                                    )
                                    max_cats_budget = min(max_cats_budget, 20)
                                
                                    # Calcular categorias padrão baseado nos períodos selecionados
                                    if not df_temp_budget.empty:
                                        if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Total' in df_temp_budget.columns:
                                            if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                                # Filtrar volume pelos períodos
                                                # Criar coluna Período_Ano se necessário (mesma lógica do df_filtrado_waterfall)
                                                if col_mes_waterfall == 'Período_Ano':
                                                    # Verificar se a coluna existe, se não, criar
                                                    if 'Período_Ano' not in df_volume.columns:
                                                        if 'Período' in df_volume.columns and 'Ano' in df_volume.columns:
                                                            df_volume = df_volume.copy()
                                                            df_volume['Período_Ano'] = df_volume['Período'].astype(str) + ' ' + df_volume['Ano'].astype(str)
                                                    if 'Período_Ano' in df_volume.columns:
                                                        df_vol_temp = df_volume[df_volume['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                    else:
                                                        df_vol_temp = df_volume.copy()
                                                elif 'Período' in df_volume.columns:
                                                    df_vol_temp = df_volume[df_volume['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                else:
                                                    df_vol_temp = df_volume.copy()
                                                
                                                if not df_vol_temp.empty and chosen_dim_budget in df_vol_temp.columns:
                                                    vol_mf_budget = (df_temp_budget.groupby(chosen_dim_budget).agg({'Total': 'sum'}).reset_index())
                                                    vol_mf_budget = vol_mf_budget.merge(
                                                        df_vol_temp.groupby(chosen_dim_budget)['Volume'].sum().reset_index(),
                                                        on=chosen_dim_budget,
                                                        how='left'
                                                    )
                                                    vol_mf_budget['Volume'] = vol_mf_budget['Volume'].fillna(0)
                                                    vol_mf_budget['CPU'] = vol_mf_budget['Total'] / vol_mf_budget['Volume'].replace(0, 1)
                                                    vol_mf_budget = vol_mf_budget.sort_values('CPU', ascending=False)
                                                    vol_index_budget = [str(c).strip() for c in list(vol_mf_budget[chosen_dim_budget])]
                                                else:
                                                    vol_mf_budget = (df_temp_budget.groupby(chosen_dim_budget)['Total'].sum().sort_values(ascending=False))
                                                    vol_index_budget = [str(c).strip() for c in list(vol_mf_budget.index)]
                                            else:
                                                vol_mf_budget = (df_temp_budget.groupby(chosen_dim_budget)['Total'].sum().sort_values(ascending=False))
                                                vol_index_budget = [str(c).strip() for c in list(vol_mf_budget.index)]
                                        else:
                                            vol_mf_budget = (df_temp_budget.groupby(chosen_dim_budget)['Total'].sum().sort_values(ascending=False))
                                            vol_index_budget = [str(c).strip() for c in list(vol_mf_budget.index)]
                                    else:
                                        vol_index_budget = []
                                    
                                    default_cats_budget = vol_index_budget[:max_cats_budget] if len(vol_index_budget) else cats_all_budget[:max_cats_budget]
                                    
                                    # Opções de categorias
                                    cats_options_budget = ["Todos"] + cats_all_budget
                                    default_cats_budget = [c for c in default_cats_budget if c in cats_all_budget]
                                    if not default_cats_budget:
                                        default_cats_budget = cats_all_budget[:min(10, len(cats_all_budget))]
                                    
                                    # Controle: Categorias (uma ou mais)
                                    cats_sel_raw_budget = st.multiselect(
                                        "Categorias (uma ou mais):",
                                        cats_options_budget,
                                        default=default_cats_budget,
                                        key="cats_budget_waterfall"
                                    )
                                    
                                    if (not cats_sel_raw_budget) or ("Todos" in cats_sel_raw_budget):
                                        cats_sel_budget = cats_all_budget[:max_cats_budget] if max_cats_budget < len(cats_all_budget) else cats_all_budget
                                    else:
                                        cats_sel_budget = cats_sel_raw_budget
                                    
                                    st.markdown("---")
                                    
                                    # Filtrar dados pelos períodos selecionados
                                    if col_mes_waterfall == 'Período_Ano':
                                        df_real_periodo = df_analise_budget[df_analise_budget[col_mes_waterfall].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                    elif 'Período' in df_analise_budget.columns:
                                        df_real_periodo = df_analise_budget[df_analise_budget['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                    else:
                                        df_real_periodo = df_analise_budget.copy()
                                    
                                    # Aplicar fator de conversão e conversão de moeda ao budget (mesma lógica do app.py)
                                    df_budget_work = df_budget.copy()
                                    
                                    # Aplicar fator de conversão
                                    if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Total' in df_budget_work.columns:
                                        if fator_conversao == "K (milhares)":
                                            df_budget_work['Total'] = df_budget_work['Total'] / 1000
                                        elif fator_conversao == "M (Milhões)":
                                            df_budget_work['Total'] = df_budget_work['Total'] / 1000000
                                    
                                    # Aplicar conversão de moeda
                                    if moeda_codigo != "BRL" and 'Total' in df_budget_work.columns:
                                        df_budget_work = converter_coluna_moeda(df_budget_work, 'Total', moeda_codigo, taxas_cambio)
                                    
                                    # Aplicar TODOS os filtros que existem em df_filtrado_waterfall
                                    df_budget_filtrado = df_budget_work.copy()
                                    df_budget_vol_filtrado = df_budget_vol.copy()
                                    
                                    if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
                                        # Aplicar filtro de Veículo
                                        if 'Veículo' in df_filtrado_waterfall.columns and 'Veículo' in df_budget_filtrado.columns:
                                            veiculos_filtrados = df_filtrado_waterfall['Veículo'].dropna().unique()
                                            if len(veiculos_filtrados) > 0:
                                                df_budget_filtrado = df_budget_filtrado[df_budget_filtrado['Veículo'].isin(veiculos_filtrados)].copy()
                                        
                                        # Aplicar filtro de Oficina
                                        if 'Oficina' in df_filtrado_waterfall.columns and 'Oficina' in df_budget_filtrado.columns:
                                            oficinas_filtradas = df_filtrado_waterfall['Oficina'].dropna().unique()
                                            if len(oficinas_filtradas) > 0:
                                                df_budget_filtrado = df_budget_filtrado[df_budget_filtrado['Oficina'].isin(oficinas_filtradas)].copy()
                                        
                                        # Aplicar filtro de USI
                                        if 'USI' in df_filtrado_waterfall.columns and 'USI' in df_budget_filtrado.columns:
                                            usi_filtradas = df_filtrado_waterfall['USI'].dropna().unique()
                                            if len(usi_filtradas) > 0:
                                                df_budget_filtrado = df_budget_filtrado[df_budget_filtrado['USI'].isin(usi_filtradas)].copy()
                                        
                                        # Aplicar outros filtros comuns
                                        colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                                        for col_filtro in colunas_filtro_comuns:
                                            if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_budget_filtrado.columns:
                                                valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                                if len(valores_filtrados) > 0:
                                                    df_budget_filtrado = df_budget_filtrado[df_budget_filtrado[col_filtro].isin(valores_filtrados)].copy()
                                        
                                        # Aplicar mesmos filtros ao volume de budget
                                        if 'Veículo' in df_filtrado_waterfall.columns and 'Veículo' in df_budget_vol_filtrado.columns:
                                            veiculos_filtrados = df_filtrado_waterfall['Veículo'].dropna().unique()
                                            if len(veiculos_filtrados) > 0:
                                                df_budget_vol_filtrado = df_budget_vol_filtrado[df_budget_vol_filtrado['Veículo'].isin(veiculos_filtrados)].copy()
                                        
                                        if 'Oficina' in df_filtrado_waterfall.columns and 'Oficina' in df_budget_vol_filtrado.columns:
                                            oficinas_filtradas = df_filtrado_waterfall['Oficina'].dropna().unique()
                                            if len(oficinas_filtradas) > 0:
                                                df_budget_vol_filtrado = df_budget_vol_filtrado[df_budget_vol_filtrado['Oficina'].isin(oficinas_filtradas)].copy()
                                        
                                        if 'USI' in df_filtrado_waterfall.columns and 'USI' in df_budget_vol_filtrado.columns:
                                            usi_filtradas = df_filtrado_waterfall['USI'].dropna().unique()
                                            if len(usi_filtradas) > 0:
                                                df_budget_vol_filtrado = df_budget_vol_filtrado[df_budget_vol_filtrado['USI'].isin(usi_filtradas)].copy()
                                    
                                    # Filtrar budget pelos períodos selecionados
                                    # IMPORTANTE: Sempre filtrar pelos períodos selecionados para garantir que o BUD seja calculado corretamente
                                    if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                        if col_mes_waterfall == 'Período_Ano' and 'Período_Ano' in df_budget_filtrado.columns:
                                            df_budget_periodo = df_budget_filtrado[df_budget_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                        elif 'Período' in df_budget_filtrado.columns:
                                            # Tentar fazer match com o formato do período (pode ser "Julho 2025" ou "Julho")
                                            periodos_str = [str(p) for p in periodos_selecionados_budget]
                                            df_budget_periodo = df_budget_filtrado[df_budget_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                            
                                            # Se não encontrou nada, tentar fazer match apenas com o mês (sem o ano)
                                            if len(df_budget_periodo) == 0:
                                                meses_periodos = []
                                                for p in periodos_selecionados_budget:
                                                    p_str = str(p)
                                                    # Extrair apenas o mês (primeira palavra)
                                                    if ' ' in p_str:
                                                        mes = p_str.split(' ')[0]
                                                        meses_periodos.append(mes)
                                                if meses_periodos:
                                                    df_budget_periodo = df_budget_filtrado[df_budget_filtrado['Período'].astype(str).isin(meses_periodos)].copy()
                                        else:
                                            df_budget_periodo = pd.DataFrame()  # DataFrame vazio se não há coluna Período
                                    else:
                                        df_budget_periodo = pd.DataFrame()  # DataFrame vazio se não há períodos selecionados
                                    
                                    # Preparar volume real filtrado pelos períodos selecionados
                                    df_volume_real_filtrado = None
                                    if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                        df_volume_real_filtrado = df_volume.copy()
                                        
                                        # Aplicar mesmos filtros do df_filtrado_waterfall
                                        if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
                                            colunas_filtro_vol = ['Veículo', 'Oficina', 'USI', 'Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                                            for col_filtro in colunas_filtro_vol:
                                                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_volume_real_filtrado.columns:
                                                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                                    if len(valores_filtrados) > 0:
                                                        df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado[col_filtro].isin(valores_filtrados)].copy()
                                        
                                        # Filtrar pelos períodos selecionados (mesma lógica do budget)
                                        if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                            if col_mes_waterfall == 'Período_Ano' and 'Período_Ano' in df_volume_real_filtrado.columns:
                                                df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                            elif 'Período' in df_volume_real_filtrado.columns:
                                                periodos_str = [str(p) for p in periodos_selecionados_budget]
                                                df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                                
                                                # Se não encontrou nada, tentar fazer match apenas com o mês (sem o ano)
                                                if len(df_volume_real_filtrado) == 0:
                                                    meses_periodos = []
                                                    for p in periodos_selecionados_budget:
                                                        p_str = str(p)
                                                        if ' ' in p_str:
                                                            mes = p_str.split(' ')[0]
                                                            meses_periodos.append(mes)
                                                    if meses_periodos:
                                                        df_volume_real_filtrado = df_volume.copy()
                                                        # Reaplicar filtros
                                                        if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
                                                            for col_filtro in colunas_filtro_vol:
                                                                if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_volume_real_filtrado.columns:
                                                                    valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                                                    if len(valores_filtrados) > 0:
                                                                        df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado[col_filtro].isin(valores_filtrados)].copy()
                                                        # Filtrar por mês
                                                        df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período'].astype(str).isin(meses_periodos)].copy()
                                    
                                    # Verificar se temos coluna Account
                                    if 'Account' not in df_real_periodo.columns:
                                        st.warning("⚠️ Coluna 'Account' não encontrada nos dados.")
                                    elif 'Custo' not in df_real_periodo.columns:
                                        st.warning("⚠️ Coluna 'Custo' não encontrada nos dados. A tabela requer classificação Fixo/Variável.")
                                    else:
                                        # df_budget_periodo já foi criado acima, apenas usar
                                        
                                        # Agrupar dados reais por Account (sem Período, pois já está filtrado)
                                        colunas_agrupamento = ['Account', 'Custo']
                                        if 'Type 05' in df_real_periodo.columns:
                                            colunas_agrupamento.insert(0, 'Type 05')
                                        if 'Type 06' in df_real_periodo.columns:
                                            if 'Type 05' in colunas_agrupamento:
                                                colunas_agrupamento.insert(1, 'Type 06')
                                            else:
                                                colunas_agrupamento.insert(0, 'Type 06')
                                        
                                        # IMPORTANTE: df_real_periodo já vem de df_analise_budget que é uma cópia de df_filtrado_waterfall
                                        # que tem a conversão de moeda aplicada. NÃO aplicar conversão novamente aqui para evitar duplicação
                                        
                                        df_real_agrupado = df_real_periodo.groupby(colunas_agrupamento)['Total'].sum().reset_index()
                                        
                                        # Agrupar dados de budget por Account (sem Período, pois já está filtrado)
                                        colunas_agrupamento_budget = ['Account', 'Custo']
                                        if 'Type 05' in df_budget_periodo.columns:
                                            colunas_agrupamento_budget.insert(0, 'Type 05')
                                        if 'Type 06' in df_budget_periodo.columns:
                                            if 'Type 05' in colunas_agrupamento_budget:
                                                colunas_agrupamento_budget.insert(1, 'Type 06')
                                            else:
                                                colunas_agrupamento_budget.insert(0, 'Type 06')
                                        
                                        # Agrupar dados de budget por Account (sem Período, pois já está filtrado)
                                        # IMPORTANTE: Sempre usar df_budget_periodo que já foi filtrado pelos períodos selecionados
                                        # IMPORTANTE: df_budget_periodo já vem de df_budget_work que tem a conversão de moeda aplicada (linha 2743)
                                        # NÃO aplicar conversão novamente aqui para evitar duplicação
                                        if df_budget_periodo is not None and len(df_budget_periodo) > 0:
                                            df_budget_agrupado = df_budget_periodo.groupby(colunas_agrupamento_budget)['Total'].sum().reset_index()
                                            df_budget_agrupado = df_budget_agrupado.rename(columns={'Total': 'Total_Budget'})
                                        else:
                                            # Se não há dados para os períodos selecionados, criar DataFrame vazio
                                            df_budget_agrupado = pd.DataFrame(columns=colunas_agrupamento_budget + ['Total_Budget'])
                                        
                                        # Agrupar volumes reais (somar todos os períodos selecionados)
                                        df_vol_real_agrupado = pd.DataFrame()
                                        if df_volume_real_filtrado is not None and not df_volume_real_filtrado.empty and 'Volume' in df_volume_real_filtrado.columns:
                                            volume_total_real = df_volume_real_filtrado['Volume'].sum()
                                            df_vol_real_agrupado = pd.DataFrame({'Volume': [volume_total_real]})
                                        else:
                                            df_vol_real_agrupado = pd.DataFrame({'Volume': [0]})
                                        
                                        # Agrupar volumes de budget (somar todos os períodos selecionados)
                                        df_vol_budget_agrupado = pd.DataFrame()
                                        if df_budget_vol_filtrado is not None and not df_budget_vol_filtrado.empty and 'Volume' in df_budget_vol_filtrado.columns:
                                            # Filtrar budget volume pelos períodos selecionados (mesma lógica do budget)
                                            if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                                if col_mes_waterfall == 'Período_Ano' and 'Período_Ano' in df_budget_vol_filtrado.columns:
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                elif 'Período' in df_budget_vol_filtrado.columns:
                                                    periodos_str = [str(p) for p in periodos_selecionados_budget]
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                                    
                                                    # Se não encontrou nada, tentar fazer match apenas com o mês (sem o ano)
                                                    if len(df_budget_vol_periodo) == 0:
                                                        meses_periodos = []
                                                        for p in periodos_selecionados_budget:
                                                            p_str = str(p)
                                                            if ' ' in p_str:
                                                                mes = p_str.split(' ')[0]
                                                                meses_periodos.append(mes)
                                                        if meses_periodos:
                                                            df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].astype(str).isin(meses_periodos)].copy()
                                                else:
                                                    df_budget_vol_periodo = pd.DataFrame()
                                                
                                                if len(df_budget_vol_periodo) > 0:
                                                    volume_total_budget = df_budget_vol_periodo['Volume'].sum()
                                                    df_vol_budget_agrupado = pd.DataFrame({'Volume': [volume_total_budget]})
                                                else:
                                                    df_vol_budget_agrupado = pd.DataFrame({'Volume': [0]})
                                            else:
                                                df_vol_budget_agrupado = pd.DataFrame({'Volume': [0]})
                                        else:
                                            df_vol_budget_agrupado = pd.DataFrame({'Volume': [0]})
                                        
                                        # Merge Real + Budget (mesma lógica do app.py)
                                        colunas_agrupamento_com_periodo = [col for col in colunas_agrupamento if col != 'Total']
                                        df_tabela_flex = df_real_agrupado.merge(
                                            df_budget_agrupado,
                                            on=colunas_agrupamento_com_periodo,
                                            how='outer',
                                            suffixes=('', '_Budget')
                                        )
                                        df_tabela_flex['Total'] = df_tabela_flex['Total'].fillna(0)
                                        df_tabela_flex['Total_Budget'] = df_tabela_flex['Total_Budget'].fillna(0)
                                        df_tabela_flex['Budget_Total_Custo'] = df_tabela_flex['Total_Budget']
                                        df_tabela_flex['Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                                        
                                        # Adicionar volumes (já estão agregados, não precisam merge por período)
                                        volume_total_real = df_vol_real_agrupado['Volume'].sum() if len(df_vol_real_agrupado) > 0 and 'Volume' in df_vol_real_agrupado.columns else 0
                                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum() if len(df_vol_budget_agrupado) > 0 and 'Volume' in df_vol_budget_agrupado.columns else 0
                                        
                                        # IMPORTANTE: Volume_Budget deve ser sempre o volume de budget, nunca usar volume_real como fallback
                                        # Se volume_budget for 0, usar 1 para evitar divisão por zero, mas não substituir por volume_real
                                        df_tabela_flex['Volume_Real'] = volume_total_real
                                        # IMPORTANTE: Volume_Budget deve ser sempre o volume de budget real, nunca usar volume_real como fallback
                                        # Se volume_budget for 0, usar o mesmo valor do volume_real apenas para evitar divisão por zero,
                                        # mas isso deve ser raro e indica que não há dados de budget
                                        df_tabela_flex['Volume_Budget'] = volume_total_budget if volume_total_budget > 0 else (volume_total_real if volume_total_real > 0 else 1)
                                        
                                        # Calcular Flex Bud (mesma lógica do app.py)
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                                            df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                                            
                                            df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(df_tabela_flex['Custo'] == 'Fixo', 0)
                                            df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(df_tabela_flex['Custo'] == 'Variável', 0)
                                            df_tabela_flex['_Flex_Bud_Total_Custo'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                                            
                                            df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Total_Custo'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                                            df_tabela_flex['Flex BUD'] = df_tabela_flex['Flex BUD'].fillna(0)
                                            
                                            df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                                            df_tabela_flex['BUD'] = df_tabela_flex['BUD'].fillna(0)
                                            
                                            df_tabela_flex['Total'] = df_tabela_flex['Total'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                                            df_tabela_flex['Total'] = df_tabela_flex['Total'].fillna(0)
                                        else:
                                            df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                                            df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                                            
                                            df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(df_tabela_flex['Custo'] == 'Fixo', 0)
                                            df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(df_tabela_flex['Custo'] == 'Variável', 0)
                                            df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                                            df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo']
                                        
                                        # Calcular diferenças
                                        df_tabela_flex['Flex Bud - BUD'] = df_tabela_flex['Flex BUD'] - df_tabela_flex['BUD']
                                        df_tabela_flex['Total - Flex Bud'] = df_tabela_flex['Total'] - df_tabela_flex['Flex BUD']
                                        df_tabela_flex['Total / Flex Bud'] = df_tabela_flex.apply(
                                            lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                            axis=1
                                        )
                                        
                                        # Remover colunas auxiliares
                                        colunas_remover = ['Budget_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel', 'Total_Budget', 'Volume_Real', 'Volume_Budget']
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            colunas_remover.append('_Flex_Bud_Total_Custo')
                                        df_tabela_flex = df_tabela_flex.drop(columns=[col for col in colunas_remover if col in df_tabela_flex.columns])
                                        
                                        # Agrupar mantendo Type 05 e Type 06 para estrutura hierárquica
                                        colunas_agrupamento_tabela = ['Account', 'Custo']
                                        if 'Type 05' in df_tabela_flex.columns:
                                            colunas_agrupamento_tabela.insert(0, 'Type 05')
                                        if 'Type 06' in df_tabela_flex.columns:
                                            if 'Type 05' in colunas_agrupamento_tabela:
                                                colunas_agrupamento_tabela.insert(1, 'Type 06')
                                            else:
                                                colunas_agrupamento_tabela.insert(0, 'Type 06')
                                        
                                        # Não precisa agrupar por Período, pois já está filtrado
                                        df_tabela_total_agrupado = df_tabela_flex.groupby(colunas_agrupamento_tabela).agg({
                                            'BUD': 'sum',
                                            'Flex Bud - BUD': 'sum',
                                            'Flex BUD': 'sum',
                                            'Total - Flex Bud': 'sum',
                                            'Total': 'sum'
                                        }).reset_index()
                                        
                                        # Recalcular Total / Flex Bud após agrupamento
                                        df_tabela_total_agrupado['Total / Flex Bud'] = df_tabela_total_agrupado.apply(
                                            lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                            axis=1
                                        )
                                        
                                        # Filtrar linhas zeradas
                                        colunas_numericas = [col for col in df_tabela_total_agrupado.columns 
                                                            if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                            and col not in ['Account', 'Custo', 'Type 05', 'Type 06', 'Período']]
                                        if colunas_numericas:
                                            df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas].fillna(0)
                                            df_tabela_total_agrupado = df_tabela_total_agrupado[
                                                df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                            ].copy()
                                        
                                        # Calcular valores para gráfico waterfall
                                        # Agrupar por Account para o gráfico
                                        df_grafico = df_tabela_total_agrupado.groupby('Account').agg({
                                            'BUD': 'sum',
                                            'Flex Bud - BUD': 'sum',
                                            'Flex BUD': 'sum',
                                            'Total - Flex Bud': 'sum',
                                            'Total': 'sum'
                                        }).reset_index()
                                        
                                        # Preparar dados para gráfico waterfall
                                        bud_total = df_grafico['BUD'].sum()
                                        flex_bud_total = df_grafico['Flex BUD'].sum()
                                        total_real = df_grafico['Total'].sum()
                                        flex_bud_menos_bud = flex_bud_total - bud_total
                                        total_menos_flex_bud = total_real - flex_bud_total
                                        
                                        # Calcular variações por Account (Total - Flex Bud)
                                        labels_cats = []
                                        values_cats = []
                                        for _, row in df_grafico.iterrows():
                                            account = str(row['Account'])
                                            delta = float(row['Total - Flex Bud'])
                                            if abs(delta) > 1e-9:
                                                labels_cats.append(account)
                                                values_cats.append(float(delta))
                                        
                                        # Ordenar por valor absoluto e limitar
                                        if labels_cats:
                                            sorted_idx = sorted(range(len(values_cats)), key=lambda i: abs(values_cats[i]), reverse=True)
                                            labels_cats = [labels_cats[i] for i in sorted_idx]
                                            values_cats = [values_cats[i] for i in sorted_idx]
                                            
                                            # Aplicar limite usando max_cats_budget
                                            if len(labels_cats) > max_cats_budget:
                                                labels_cats = labels_cats[:max_cats_budget]
                                                values_cats = values_cats[:max_cats_budget]
                                        
                                        # Calcular remainder
                                        remainder = round(total_real - (bud_total + flex_bud_menos_bud + sum(values_cats)), 2)
                                        
                                        # Montar estrutura do waterfall
                                        labels_waterfall = ["BUD"]
                                        values_waterfall = [bud_total]
                                        measures_waterfall = ["absolute"]
                                        
                                        # Adicionar Flex Bud - BUD
                                        if abs(flex_bud_menos_bud) > 1e-10:
                                            labels_waterfall.append("Flex Bud - BUD")
                                            values_waterfall.append(flex_bud_menos_bud)
                                            measures_waterfall.append("relative")
                                        
                                        # Adicionar categorias
                                        labels_waterfall.extend(labels_cats)
                                        values_waterfall.extend(values_cats)
                                        measures_waterfall.extend(["relative"] * len(labels_cats))
                                        
                                        # Adicionar "Outros" se remainder for significativo
                                        if abs(remainder) >= 0.01:
                                            labels_waterfall.append("Outros")
                                            values_waterfall.append(remainder)
                                            measures_waterfall.append("relative")
                                        
                                        # Adicionar barra final
                                        labels_waterfall.append("Total")
                                        values_waterfall.append(total_real)
                                        measures_waterfall.append("total")
                                        
                                        # Criar gráfico waterfall
                                        theme_base = st.get_option("theme.base") or "light"
                                        if theme_base == "dark":
                                            text_color = "#FAFAFA"
                                        else:
                                            text_color = "#000000"
                                        grid_color = "rgba(255,255,255,0.12)" if theme_base == "dark" else "rgba(0,0,0,0.12)"
                                        
                                        cor_vermelha = "#ff5733"
                                        cor_verde = "#1e8449"
                                        cor_azul = "#1e6ba8"
                                        cor_laranja = "#ff9800"
                                        cor_amarela = "#ffd700"
                                        
                                        # Criar anotações
                                        annotations_custom = []
                                        cumulative = 0
                                        
                                        for measure, value, label in zip(measures_waterfall, values_waterfall, labels_waterfall):
                                            if value >= 0:
                                                text_fmt = f"+{value:,.1f}"
                                            else:
                                                text_fmt = f"{value:,.1f}"
                                            
                                            if measure == "absolute":
                                                y_pos = value
                                                cumulative = value
                                                text_fmt_abs = f"{abs(value):,.1f}"
                                                annotations_custom.append(dict(
                                                    x=label, y=y_pos, text=text_fmt_abs,
                                                    showarrow=False, font=dict(color=cor_azul, size=8), yshift=15,
                                                    xref="x", yref="y"
                                                ))
                                            elif measure == "relative":
                                                if value >= 0:
                                                    cor_texto = cor_vermelha
                                                    y_pos = cumulative + value
                                                    yshift_val = 15
                                                else:
                                                    cor_texto = cor_verde
                                                    y_pos = cumulative + value
                                                    yshift_val = -15
                                                
                                                if label == "Flex Bud - BUD":
                                                    cor_texto = cor_amarela
                                                elif label == "Outros":
                                                    cor_texto = cor_laranja
                                                
                                                annotations_custom.append(dict(
                                                    x=label, y=y_pos, text=text_fmt,
                                                    showarrow=False, font=dict(color=cor_texto, size=8), yshift=yshift_val,
                                                    xref="x", yref="y", yanchor="middle"
                                                ))
                                                cumulative += value
                                            elif measure == "total":
                                                y_pos = value
                                                text_fmt_total = f"{abs(value):,.1f}"
                                                annotations_custom.append(dict(
                                                    x=label, y=y_pos, text=text_fmt_total,
                                                    showarrow=False, font=dict(color=cor_azul, size=8), yshift=20,
                                                    xref="x", yref="y", yanchor="bottom"
                                                ))
                                        
                                        # Criar figura do waterfall
                                        fig = go.Figure(go.Waterfall(
                                            name="Waterfall",
                                            orientation="v",
                                            measure=measures_waterfall,
                                            x=labels_waterfall,
                                            y=values_waterfall,
                                            textposition="none",
                                            connector={"line": {"color": "rgba(0, 0, 0, 0)"}},
                                            increasing={"marker": {"color": cor_vermelha, "line": {"width": 0}}},
                                            decreasing={"marker": {"color": cor_verde, "line": {"width": 0}}},
                                            totals={"marker": {"color": cor_azul, "line": {"width": 0}}}
                                        ))
                                        
                                        # Adicionar overlay para "Flex Bud - BUD" (amarelo)
                                        if "Flex Bud - BUD" in labels_waterfall:
                                            idx_flex = labels_waterfall.index("Flex Bud - BUD")
                                            valor_flex = values_waterfall[idx_flex]
                                            cumulative_flex = bud_total
                                            if valor_flex >= 0:
                                                base_flex = cumulative_flex
                                            else:
                                                base_flex = cumulative_flex + valor_flex
                                            
                                            fig.add_trace(go.Bar(
                                                x=['Flex Bud - BUD'],
                                                y=[abs(valor_flex)],
                                                base=[base_flex],
                                                marker_color=cor_amarela,
                                                marker_line=dict(width=2, color=cor_amarela),
                                                opacity=1.0,
                                                showlegend=False,
                                                textposition='none',
                                                width=0.8,
                                                offsetgroup='1',
                                                alignmentgroup='1'
                                            ))
                                        
                                        # Adicionar overlay para "Outros" (laranja)
                                        if "Outros" in labels_waterfall:
                                            idx_outros = labels_waterfall.index("Outros")
                                            valor_outros = values_waterfall[idx_outros]
                                            cumulative_outros = bud_total
                                            if "Flex Bud - BUD" in labels_waterfall:
                                                cumulative_outros += values_waterfall[labels_waterfall.index("Flex Bud - BUD")]
                                            for i in range(2, idx_outros):
                                                cumulative_outros += values_waterfall[i]
                                            
                                            if valor_outros >= 0:
                                                base_outros = cumulative_outros
                                            else:
                                                base_outros = cumulative_outros + valor_outros
                                            
                                            fig.add_trace(go.Bar(
                                                x=['Outros'],
                                                y=[abs(valor_outros)],
                                                base=[base_outros],
                                                marker_color=cor_laranja,
                                                marker_line=dict(width=2, color=cor_laranja),
                                                opacity=1.0,
                                                showlegend=False,
                                                textposition='none',
                                                width=0.8,
                                                offsetgroup='1',
                                                alignmentgroup='1'
                                            ))
                                        
                                        # Calcular range do eixo Y
                                        if values_waterfall:
                                            cumulative = 0
                                            all_y_positions = []
                                            
                                            for measure, value in zip(measures_waterfall, values_waterfall):
                                                if measure == "absolute":
                                                    cumulative = value
                                                    all_y_positions.append(value)
                                                elif measure == "relative":
                                                    cumulative += value
                                                    all_y_positions.append(cumulative)
                                                elif measure == "total":
                                                    all_y_positions.append(value)
                                            
                                            if all_y_positions:
                                                min_bar_pos = min(all_y_positions)
                                                max_bar_pos = max(all_y_positions)
                                                min_ann_pos = min_bar_pos
                                                max_ann_pos = max_bar_pos
                                                
                                                for ann in annotations_custom:
                                                    y_ann = ann['y']
                                                    yshift = ann.get('yshift', 0)
                                                    if yshift > 0:
                                                        ann_top = y_ann + abs(y_ann) * 0.08
                                                        if ann_top > max_ann_pos:
                                                            max_ann_pos = ann_top
                                                    elif yshift < 0:
                                                        ann_bottom = y_ann - abs(y_ann) * 0.08
                                                        if ann_bottom < min_ann_pos:
                                                            min_ann_pos = ann_bottom
                                                
                                                range_span = max_ann_pos - min_ann_pos
                                                if range_span > 0:
                                                    y_min = min_ann_pos - range_span * 0.10
                                                    y_max = max_ann_pos + range_span * 0.10
                                                else:
                                                    y_min = min_ann_pos * 0.90 if min_ann_pos > 0 else min_ann_pos * 1.10
                                                    y_max = max_ann_pos * 1.10 if max_ann_pos > 0 else max_ann_pos * 0.90
                                                
                                                if min_bar_pos >= 0 and y_min < 0:
                                                    y_min = 0
                                            else:
                                                y_min = 0
                                                y_max = 1
                                        else:
                                            y_min = 0
                                            y_max = 1
                                        
                                        # Atualizar layout
                                        titulo_grafico = f"Waterfall Analysis - {tipo_visualizacao} (Real x Budget)"
                                        fig.update_layout(
                                            title={
                                                "text": titulo_grafico,
                                                "x": 0.5,
                                                "xanchor": "center",
                                                "font": {"size": 12}
                                            },
                                            xaxis_title="Categoria",
                                            yaxis_title=f"{tipo_visualizacao} ({moeda_simbolo})",
                                            height=560,
                                            showlegend=False,
                                            plot_bgcolor="rgba(0,0,0,0)",
                                            paper_bgcolor="rgba(0,0,0,0)",
                                            margin=dict(l=80, r=40, t=50, b=40),
                                            font=dict(color=text_color, size=10),
                                            xaxis=dict(
                                                showgrid=False,
                                                zeroline=False,
                                                showline=True,
                                                linecolor=grid_color,
                                                linewidth=1,
                                                tickmode='linear',
                                                ticklen=5,
                                                tickcolor=grid_color,
                                                tickwidth=1,
                                                ticks="outside",
                                                title=dict(font=dict(size=10)),
                                                tickfont=dict(size=9)
                                            ),
                                            yaxis=dict(
                                                showgrid=False,
                                                zeroline=False,
                                                showline=True,
                                                linecolor=grid_color,
                                                linewidth=1,
                                                range=[y_min, y_max],
                                                title=dict(font=dict(size=10)),
                                                tickfont=dict(size=9)
                                            )
                                        )
                                        
                                        # Adicionar anotações
                                        fig.update_layout(annotations=annotations_custom)
                                        
                                        # Exibir gráfico
                                        plotly_chart_safe(fig, use_container_width=True)
                                        
                                        # Exibir resumo (mesmo formato da tab Real)
                                        st.markdown("---")
                                        col1, col2, col3, col4 = st.columns(4)
                                        
                                        # Calcular valores para o resumo
                                        valor_inicial_budget = bud_total  # BUD (valor inicial)
                                        valor_final_budget = total_real  # Total (valor final)
                                        variacao_total_budget = total_real - bud_total  # Total - BUD
                                        
                                        # Calcular percentual de variação
                                        percentual_change_budget = (variacao_total_budget / bud_total * 100) if bud_total != 0 else 0
                                        
                                        # Formatar valores conforme tipo de visualização
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            valor_inicial_formatado = f"{valor_inicial_budget:,.2f}"
                                            valor_final_formatado = f"{valor_final_budget:,.2f}"
                                            variacao_formatado = f"{variacao_total_budget:,.2f}"
                                        else:
                                            sufixo = ""
                                            if fator_conversao:
                                                if fator_conversao == "K (milhares)":
                                                    sufixo = " K"
                                                elif fator_conversao == "M (Milhões)":
                                                    sufixo = " M"
                                            valor_inicial_formatado = f"{valor_inicial_budget:,.2f}{sufixo}"
                                            valor_final_formatado = f"{valor_final_budget:,.2f}{sufixo}"
                                            variacao_formatado = f"{variacao_total_budget:,.2f}{sufixo}"
                                        
                                        # Formatar período selecionado para exibição
                                        if periodos_selecionados_budget:
                                            if len(periodos_selecionados_budget) == 1:
                                                periodo_display = str(periodos_selecionados_budget[0])
                                            else:
                                                periodo_display = f"{len(periodos_selecionados_budget)} períodos"
                                        else:
                                            periodo_display = "N/A"
                                        
                                        with col1:
                                            st.metric("BUD", periodo_display, valor_inicial_formatado)
                                        with col2:
                                            st.metric("Total", periodo_display, valor_final_formatado)
                                        with col3:
                                            st.metric("Variação Total", variacao_formatado, f"{percentual_change_budget:.1f}%")
                                        with col4:
                                            st.metric("Categorias", len(labels_cats), "")
                                        
                                        st.markdown("---")
                                        
                                        # Selecionador de visualização
                                        modo_tabela = st.radio(
                                            "📊 **Visualização:**",
                                            ["Fixo/Variável", "Total"],
                                            index=0,
                                            horizontal=True,
                                            key="modo_tabela_budget"
                                        )
                                        
                                        # Calcular resumo geral
                                        linha_resumo_geral = {
                                            'BUD': df_tabela_total_agrupado['BUD'].sum(),
                                            'Flex Bud - BUD': df_tabela_total_agrupado['Flex Bud - BUD'].sum(),
                                            'Flex BUD': df_tabela_total_agrupado['Flex BUD'].sum(),
                                            'Total - Flex Bud': df_tabela_total_agrupado['Total - Flex Bud'].sum(),
                                            'Total': df_tabela_total_agrupado['Total'].sum(),
                                            'Total / Flex Bud': df_tabela_total_agrupado['Total'].sum() / df_tabela_total_agrupado['Flex BUD'].sum() if df_tabela_total_agrupado['Flex BUD'].sum() != 0 else 0
                                        }
                                        
                                        # Formatar resumo
                                        linha_resumo_geral_formatado = {}
                                        for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                linha_resumo_geral_formatado[col] = f"{linha_resumo_geral[col]:,.2f}"
                                            else:
                                                sufixo = ""
                                                if fator_conversao:
                                                    if fator_conversao == "K (milhares)":
                                                        sufixo = " K"
                                                    elif fator_conversao == "M (Milhões)":
                                                        sufixo = " M"
                                                linha_resumo_geral_formatado[col] = f"{linha_resumo_geral[col]:,.2f}{sufixo}"
                                        
                                        linha_resumo_geral_formatado['Total / Flex Bud'] = formatar_ratio_com_barra(linha_resumo_geral['Total / Flex Bud'])
                                        
                                        st.markdown("### 📊 Resumo Geral")
                                        ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                        num_colunas = min(len(ordem_colunas), 6)
                                        if num_colunas > 0:
                                            cols = st.columns(num_colunas, gap="small")
                                            for idx, col_nome in enumerate(ordem_colunas[:num_colunas]):
                                                if col_nome in linha_resumo_geral_formatado:
                                                    with cols[idx]:
                                                        valor_formatado = linha_resumo_geral_formatado.get(col_nome, '-')
                                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                        
                                        # Exibir volumes (Real e Budget) logo abaixo do Resumo Geral (igual à tab Real)
                                        # Recalcular volumes a partir dos dados filtrados para garantir que estejam corretos
                                        st.markdown("<br>", unsafe_allow_html=True)
                                        
                                        # Recalcular volume real dos dados filtrados
                                        volume_real_calculado = 0
                                        if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                            # Aplicar mesmos filtros do df_filtrado_waterfall
                                            df_vol_real_para_calculo = df_volume.copy()
                                            if df_filtrado_waterfall is not None and len(df_filtrado_waterfall) > 0:
                                                colunas_filtro_vol = ['Veículo', 'Oficina', 'USI', 'Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                                                for col_filtro in colunas_filtro_vol:
                                                    if col_filtro in df_filtrado_waterfall.columns and col_filtro in df_vol_real_para_calculo.columns:
                                                        valores_filtrados = df_filtrado_waterfall[col_filtro].dropna().unique()
                                                        if len(valores_filtrados) > 0:
                                                            df_vol_real_para_calculo = df_vol_real_para_calculo[df_vol_real_para_calculo[col_filtro].isin(valores_filtrados)].copy()
                                            
                                            # Filtrar pelos períodos selecionados (mesma lógica do budget)
                                            if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                                if col_mes_waterfall == 'Período_Ano' and 'Período_Ano' in df_vol_real_para_calculo.columns:
                                                    df_vol_real_periodo = df_vol_real_para_calculo[df_vol_real_para_calculo['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                elif 'Período' in df_vol_real_para_calculo.columns:
                                                    periodos_str = [str(p) for p in periodos_selecionados_budget]
                                                    df_vol_real_periodo = df_vol_real_para_calculo[df_vol_real_para_calculo['Período'].astype(str).isin(periodos_str)].copy()
                                                    
                                                    # Se não encontrou nada, tentar fazer match apenas com o mês (sem o ano)
                                                    if len(df_vol_real_periodo) == 0:
                                                        meses_periodos = []
                                                        for p in periodos_selecionados_budget:
                                                            p_str = str(p)
                                                            if ' ' in p_str:
                                                                mes = p_str.split(' ')[0]
                                                                meses_periodos.append(mes)
                                                        if meses_periodos:
                                                            df_vol_real_periodo = df_vol_real_para_calculo[df_vol_real_para_calculo['Período'].astype(str).isin(meses_periodos)].copy()
                                                else:
                                                    df_vol_real_periodo = pd.DataFrame()
                                                
                                                if len(df_vol_real_periodo) > 0:
                                                    volume_real_calculado = df_vol_real_periodo['Volume'].sum()
                                            else:
                                                # Se não há períodos selecionados, usar todos os dados filtrados
                                                volume_real_calculado = df_vol_real_para_calculo['Volume'].sum()
                                        
                                        # Recalcular volume budget dos dados filtrados
                                        volume_budget_calculado = 0
                                        if df_budget_vol_filtrado is not None and not df_budget_vol_filtrado.empty and 'Volume' in df_budget_vol_filtrado.columns:
                                            # Filtrar budget volume pelos períodos selecionados
                                            if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                                if col_mes_waterfall == 'Período_Ano' and 'Período_Ano' in df_budget_vol_filtrado.columns:
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                elif 'Período' in df_budget_vol_filtrado.columns:
                                                    periodos_str = [str(p) for p in periodos_selecionados_budget]
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                                    
                                                    # Se não encontrou nada, tentar fazer match apenas com o mês (sem o ano)
                                                    if len(df_budget_vol_periodo) == 0:
                                                        meses_periodos = []
                                                        for p in periodos_selecionados_budget:
                                                            p_str = str(p)
                                                            if ' ' in p_str:
                                                                mes = p_str.split(' ')[0]
                                                                meses_periodos.append(mes)
                                                        if meses_periodos:
                                                            df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].astype(str).isin(meses_periodos)].copy()
                                                else:
                                                    df_budget_vol_periodo = pd.DataFrame()
                                                
                                                if len(df_budget_vol_periodo) > 0:
                                                    volume_budget_calculado = df_budget_vol_periodo['Volume'].sum()
                                        
                                        try:
                                            volume_real_val = float(volume_real_calculado) if pd.notna(volume_real_calculado) else 0.0
                                            volume_budget_val = float(volume_budget_calculado) if pd.notna(volume_budget_calculado) else 0.0
                                        except (ValueError, TypeError):
                                            volume_real_val = 0.0
                                            volume_budget_val = 0.0
                                        
                                        volume_real_formatado = f"{volume_real_val:,.0f}"
                                        volume_budget_formatado = f"{volume_budget_val:,.0f}"
                                        
                                        col_vol_budget, col_vol_real = st.columns(2, gap="small")
                                        with col_vol_budget:
                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget ({periodo_display}):</strong> {volume_budget_formatado}</div>", unsafe_allow_html=True)
                                        with col_vol_real:
                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real ({periodo_display}):</strong> {volume_real_formatado}</div>", unsafe_allow_html=True)
                                        st.markdown("<br>", unsafe_allow_html=True)
                                        
                                        # Criar estrutura hierárquica com expanders
                                        if modo_tabela == "Fixo/Variável":
                                            for custo in ['Fixo', 'Variável']:
                                                df_custo = df_tabela_total_agrupado[df_tabela_total_agrupado['Custo'] == custo].copy()
                                                
                                                if len(df_custo) > 0:
                                                    total_custo = df_custo['Total'].sum()
                                                    total_custo_formatado = f"{total_custo:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_custo:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_custo:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_custo:,.2f}")
                                                    
                                                    if total_custo != 0 and pd.notna(total_custo):
                                                        with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=False):
                                                            # Nível 2: Type 05
                                                            if 'Type 05' in df_custo.columns:
                                                                for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                                    df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                                    
                                                                    if len(df_type05) > 0:
                                                                        total_type05 = df_type05['Total'].sum()
                                                                        total_type05_formatado = f"{total_type05:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type05:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type05:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type05:,.2f}")
                                                                        
                                                                        # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                                        with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                                            # Nível 3: Type 06
                                                                            if 'Type 06' in df_type05.columns:
                                                                                    for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                                        df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                                        
                                                                                        if len(df_type06) > 0:
                                                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores não zerados
                                                                                            colunas_numericas_check = [col for col in df_type06.columns 
                                                                                                                      if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                                      and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                            if colunas_numericas_check:
                                                                                                df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                                                tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                                                if not tem_valores_nao_zerados:
                                                                                                    continue  # Pular Type 06 completamente zerado
                                                                                            else:
                                                                                                if 'Total' in df_type06.columns:
                                                                                                    if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                                                        continue  # Pular Type 06 completamente zerado
                                                                                            
                                                                                            # Verificar se a coluna 'Total' existe antes de acessá-la
                                                                                            if 'Total' in df_type06.columns:
                                                                                                total_type06 = df_type06['Total'].sum()
                                                                                            else:
                                                                                                total_type06 = 0.0
                                                                                            total_type06_formatado = f"{total_type06:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type06:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type06:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type06:,.2f}")
                                                                                            
                                                                                            # Nível 4: Account (se existir)
                                                                                            if 'Account' in df_type06.columns:
                                                                                                    colunas_numericas = [col for col in df_type06.columns 
                                                                                                                        if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                                        and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                                    if colunas_numericas:
                                                                                                        df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                                                        df_type06_filtrado = df_type06[
                                                                                                            df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                                                        ].copy()
                                                                                                    else:
                                                                                                        df_type06_filtrado = df_type06.copy()
                                                                                                    
                                                                                                    if len(df_type06_filtrado) > 0:
                                                                                                        # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                                                        # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                                                        st.markdown("---")  # Separador visual
                                                                                                        st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                                        with st.container():
                                                                                                            # Criar tabela
                                                                                                            colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                                            colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                                                if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                                                            colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                                            colunas_display = colunas_id + colunas_ordenadas
                                                                                                            df_display = df_type06_filtrado[colunas_display].copy()
                                                                                                            
                                                                                                            # Formatar valores
                                                                                                            for col in df_display.columns:
                                                                                                                if col not in colunas_id:
                                                                                                                    if col == 'Total / Flex Bud':
                                                                                                                        df_display[col] = df_display[col].apply(
                                                                                                                            lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0)
                                                                                                                        )
                                                                                                                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                                        else:
                                                                                                                            sufixo = ""
                                                                                                                            if fator_conversao:
                                                                                                                                if fator_conversao == "K (milhares)":
                                                                                                                                    sufixo = " K"
                                                                                                                                elif fator_conversao == "M (Milhões)":
                                                                                                                                    sufixo = " M"
                                                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                                            
                                                                                                            # Calcular resumo
                                                                                                            linha_resumo_type06 = {}
                                                                                                            linha_resumo_formatado_type06 = {}
                                                                                                            
                                                                                                            linha_resumo_type06['BUD'] = df_type06_filtrado['BUD'].sum()
                                                                                                            linha_resumo_type06['Flex Bud - BUD'] = df_type06_filtrado['Flex Bud - BUD'].sum()
                                                                                                            linha_resumo_type06['Flex BUD'] = df_type06_filtrado['Flex BUD'].sum()
                                                                                                            linha_resumo_type06['Total - Flex Bud'] = df_type06_filtrado['Total - Flex Bud'].sum()
                                                                                                            linha_resumo_type06['Total'] = df_type06_filtrado['Total'].sum()
                                                                                                            linha_resumo_type06['Total / Flex Bud'] = df_type06_filtrado['Total'].sum() / df_type06_filtrado['Flex BUD'].sum() if df_type06_filtrado['Flex BUD'].sum() != 0 else 0
                                                                                                            
                                                                                                            # Formatar valores
                                                                                                            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                                                                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                                    linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}"
                                                                                                                else:
                                                                                                                    sufixo = ""
                                                                                                                    if fator_conversao:
                                                                                                                        if fator_conversao == "K (milhares)":
                                                                                                                            sufixo = " K"
                                                                                                                        elif fator_conversao == "M (Milhões)":
                                                                                                                            sufixo = " M"
                                                                                                                    linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}{sufixo}"
                                                                                                            
                                                                                                            linha_resumo_formatado_type06['Total / Flex Bud'] = formatar_ratio_com_barra(linha_resumo_type06['Total / Flex Bud'])
                                                                                                            
                                                                                                            # Exibir resumo e tabela
                                                                                                            ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                                            num_colunas = min(len(ordem_colunas), 6)
                                                                                                            if num_colunas > 0:
                                                                                                                cols = st.columns(num_colunas, gap="small")
                                                                                                                for idx, col_nome in enumerate(ordem_colunas[:num_colunas]):
                                                                                                                    if col_nome in linha_resumo_formatado_type06:
                                                                                                                        with cols[idx]:
                                                                                                                            valor_formatado = linha_resumo_formatado_type06.get(col_nome, '-')
                                                                                                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                                            
                                                                                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                                            st.markdown(html_table, unsafe_allow_html=True)
                                        else:
                                            # Modo Total: estrutura hierárquica sem separação Fixo/Variável
                                            # Agrupar por Type 05, Type 06, Account (somando Fixo + Variável)
                                            colunas_agrupamento_total = []
                                            if 'Type 05' in df_tabela_total_agrupado.columns:
                                                colunas_agrupamento_total.append('Type 05')
                                            if 'Type 06' in df_tabela_total_agrupado.columns:
                                                colunas_agrupamento_total.append('Type 06')
                                            if 'Account' in df_tabela_total_agrupado.columns:
                                                colunas_agrupamento_total.append('Account')
                                            
                                            if len(colunas_agrupamento_total) > 0:
                                                df_total_agrupado = df_tabela_total_agrupado.groupby(colunas_agrupamento_total).agg({
                                                    'BUD': 'sum',
                                                    'Flex Bud - BUD': 'sum',
                                                    'Flex BUD': 'sum',
                                                    'Total - Flex Bud': 'sum',
                                                    'Total': 'sum'
                                                }).reset_index()
                                                
                                                # Recalcular Total / Flex Bud após agrupamento
                                                df_total_agrupado['Total / Flex Bud'] = df_total_agrupado.apply(
                                                    lambda row: row['Total'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                                    axis=1
                                                )
                                            else:
                                                df_total_agrupado = df_tabela_total_agrupado.copy()
                                            
                                            # Filtrar linhas zeradas
                                            colunas_numericas = [col for col in df_total_agrupado.columns 
                                                                if pd.api.types.is_numeric_dtype(df_total_agrupado[col]) 
                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Período']]
                                            if colunas_numericas:
                                                df_total_agrupado_temp = df_total_agrupado[colunas_numericas].fillna(0)
                                                df_total_agrupado = df_total_agrupado[
                                                    df_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                                ].copy()
                                            
                                            # Estrutura hierárquica com expanders (sem Custo)
                                            if 'Type 05' in df_total_agrupado.columns:
                                                for type05 in sorted(df_total_agrupado['Type 05'].dropna().unique()):
                                                    df_type05 = df_total_agrupado[df_total_agrupado['Type 05'] == type05].copy()
                                                    
                                                    if len(df_type05) > 0:
                                                        total_type05 = df_type05['Total'].sum()
                                                        total_type05_formatado = f"{total_type05:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type05:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type05:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type05:,.2f}")
                                                        
                                                        # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                        with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                            # Nível 2: Type 06
                                                            if 'Type 06' in df_type05.columns:
                                                                    for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                        df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                        
                                                                        if len(df_type06) > 0:
                                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Verificar se Type 06 tem valores não zerados
                                                                            colunas_numericas_check = [col for col in df_type06.columns 
                                                                                                      if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                      and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                            if colunas_numericas_check:
                                                                                df_type06_check = df_type06[colunas_numericas_check].fillna(0)
                                                                                tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                                                if not tem_valores_nao_zerados:
                                                                                    continue  # Pular Type 06 completamente zerado
                                                                            else:
                                                                                if 'Total' in df_type06.columns:
                                                                                    if df_type06['Total'].fillna(0).abs().sum() <= 0.0001:
                                                                                        continue  # Pular Type 06 completamente zerado
                                                                            
                                                                            # Verificar se a coluna 'Total' existe antes de acessá-la
                                                                            if 'Total' in df_type06.columns:
                                                                                total_type06 = df_type06['Total'].sum()
                                                                            else:
                                                                                total_type06 = 0.0
                                                                            total_type06_formatado = f"{total_type06:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type06:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type06:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type06:,.2f}")
                                                                            
                                                                            # Nível 3: Account (se existir)
                                                                            if 'Account' in df_type06.columns:
                                                                                # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                                colunas_numericas = [col for col in df_type06.columns 
                                                                                                    if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                if colunas_numericas:
                                                                                    df_type06_temp = df_type06[colunas_numericas].fillna(0)
                                                                                    df_type06_filtrado = df_type06[
                                                                                        df_type06_temp.abs().sum(axis=1) > 0.0001
                                                                                    ].copy()
                                                                                else:
                                                                                    df_type06_filtrado = df_type06.copy()
                                                                                
                                                                                # Só exibir se houver dados após filtrar
                                                                                if len(df_type06_filtrado) > 0:
                                                                                    # 🔧 CORREÇÃO: Usar container em vez de expander para evitar problema de 3 níveis aninhados
                                                                                    # O Streamlit 1.50.0 pode ter problemas com expanders aninhados em 3 camadas
                                                                                    st.markdown("---")  # Separador visual
                                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                    with st.container():
                                                                                            # Criar tabela
                                                                                            colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                            colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                                if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Período']]
                                                                                            colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                                                                            colunas_display = colunas_id + colunas_ordenadas
                                                                                            df_display = df_type06_filtrado[colunas_display].copy()
                                                                                            
                                                                                            # Formatar valores
                                                                                            for col in df_display.columns:
                                                                                                if col not in colunas_id:
                                                                                                    if col == 'Total / Flex Bud':
                                                                                                        df_display[col] = df_display[col].apply(
                                                                                                            lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0)
                                                                                                        )
                                                                                                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                                                                                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                                                                                                        else:
                                                                                                            sufixo = ""
                                                                                                            if fator_conversao:
                                                                                                                if fator_conversao == "K (milhares)":
                                                                                                                    sufixo = " K"
                                                                                                                elif fator_conversao == "M (Milhões)":
                                                                                                                    sufixo = " M"
                                                                                                            df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{sufixo}")
                                                                                            
                                                                                            # Calcular resumo
                                                                                            linha_resumo_type06 = {}
                                                                                            linha_resumo_formatado_type06 = {}
                                                                                            
                                                                                            linha_resumo_type06['BUD'] = df_type06_filtrado['BUD'].sum()
                                                                                            linha_resumo_type06['Flex Bud - BUD'] = df_type06_filtrado['Flex Bud - BUD'].sum()
                                                                                            linha_resumo_type06['Flex BUD'] = df_type06_filtrado['Flex BUD'].sum()
                                                                                            linha_resumo_type06['Total - Flex Bud'] = df_type06_filtrado['Total - Flex Bud'].sum()
                                                                                            linha_resumo_type06['Total'] = df_type06_filtrado['Total'].sum()
                                                                                            linha_resumo_type06['Total / Flex Bud'] = df_type06_filtrado['Total'].sum() / df_type06_filtrado['Flex BUD'].sum() if df_type06_filtrado['Flex BUD'].sum() != 0 else 0
                                                                                            
                                                                                            # Formatar valores
                                                                                            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total']:
                                                                                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                                                                    linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}"
                                                                                                else:
                                                                                                    sufixo = ""
                                                                                                    if fator_conversao:
                                                                                                        if fator_conversao == "K (milhares)":
                                                                                                            sufixo = " K"
                                                                                                        elif fator_conversao == "M (Milhões)":
                                                                                                            sufixo = " M"
                                                                                                    linha_resumo_formatado_type06[col] = f"{linha_resumo_type06[col]:,.2f}{sufixo}"
                                                                                            
                                                                                            linha_resumo_formatado_type06['Total / Flex Bud'] = formatar_ratio_com_barra(linha_resumo_type06['Total / Flex Bud'])
                                                                                            
                                                                                            # Exibir resumo e tabela
                                                                                            ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud']
                                                                                            num_colunas = min(len(ordem_colunas), 6)
                                                                                            if num_colunas > 0:
                                                                                                cols = st.columns(num_colunas, gap="small")
                                                                                                for idx, col_nome in enumerate(ordem_colunas[:num_colunas]):
                                                                                                    if col_nome in linha_resumo_formatado_type06:
                                                                                                        with cols[idx]:
                                                                                                            valor_formatado = linha_resumo_formatado_type06.get(col_nome, '-')
                                                                                                            st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
                                                                                            
                                                                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                            st.markdown(html_table, unsafe_allow_html=True)
                                            else:
                                                # Sem Type 05: não exibir nada (não deve acontecer)
                                                st.info("ℹ️ Dados sem estrutura hierárquica (Type 05).")
                            
                except Exception as e:
                    st.error(f"❌ Erro ao gerar tabela Budget: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

st.markdown("---")

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
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão 1.0 | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)

