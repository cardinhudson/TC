import sys
import os as _os
_ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import altair as alt
import os
import numpy as np
import unicodedata
import json
import sqlite3
from datetime import datetime
import plotly.graph_objects as go
from tc_principal.ui_components import (injetar_css_global, render_header, render_sidebar_global)

# Camada core (refatoração incremental): helpers compartilhados, sem depender de app.py
from tc_core.data.paths import (
    encontrar_arquivo_parquet as _core_encontrar_arquivo_parquet,
    listar_anos_disponiveis as _core_listar_anos_disponiveis,
)
from tc_core.finance.currency import (
    converter_coluna_moeda as _core_converter_coluna_moeda,
    converter_moeda as _core_converter_moeda,
    obter_simbolo_moeda as _core_obter_simbolo_moeda,
)
from tc_core.finance.currency_db import (
    carregar_taxas_banco as _core_carregar_taxas_banco,
    inicializar_banco_taxas as _core_inicializar_banco_taxas,
    salvar_taxas_banco as _core_salvar_taxas_banco,
)

from tc_ext.normalizacao import padronizar_colunas
from tc_ext.metricas_tc_ext import cpu_por_chaves


def _cpu_por_chaves_tc(*args, **kwargs):
    """Wrapper: calcula CPU e renomeia coluna de saída Total → Custo FP."""
    kwargs.setdefault('coluna_custo', 'Custo FP')
    result = cpu_por_chaves(*args, **kwargs)
    if "Total" in result.columns and "Custo FP" not in result.columns:
        result = result.rename(columns={"Total": "Custo FP"})
    return result


def _normalizar_texto_sem_acento(valor) -> str:
    if pd.isna(valor):
        return ""
    return (
        unicodedata.normalize('NFKD', str(valor))
        .encode('ascii', 'ignore')
        .decode('ascii')
        .strip()
        .lower()
    )


def _normalizar_rotulo_custo(valor):
    texto = _normalizar_texto_sem_acento(valor)
    if not texto:
        return valor
    if texto.startswith('fix'):
        return 'Fixo'
    if texto.startswith('var'):
        return 'Variável'
    return str(valor).strip()


def _mask_custo_fixo(serie: pd.Series) -> pd.Series:
    return serie.astype(str).map(_normalizar_texto_sem_acento).str.startswith('fix')


def _remover_linhas_sem_valores_para_exibicao(
    df: pd.DataFrame,
    colunas_ignorar: list[str] | None = None,
    eps: float = 0.0001,
) -> pd.DataFrame:
    """Remove linhas 100% nulas/zeradas APENAS para melhorar a visibilidade.

    Governança: sempre esconder linhas sem impacto (não muda somatórios),
    e nunca usar esse filtro para cálculos de resumo/totais.
    """
    if df is None or df.empty:
        return df

    colunas_ignorar = colunas_ignorar or []
    colunas_numericas = [
        col
        for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col]) and col not in colunas_ignorar
    ]
    if not colunas_numericas:
        return df

    df_tmp = df[colunas_numericas].fillna(0)
    mask_mantem = df_tmp.abs().sum(axis=1) > eps
    return df.loc[mask_mantem].copy()


# Configuração da página fica no app.py (roteador) para evitar chamadas duplicadas


def _get_project_root_tc_veic():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
def get_budget_oficinas_opcoes(ano_selecionado_param):
    """Retorna lista de oficinas existentes no Budget (custos) (histórico consolidado BUD)."""
    try:
        project_root = _get_project_root_tc_veic()
        caminho_budget = os.path.join(
            project_root,
            "dados",
            "TC_Principal",
            "historico_consolidado",
            "BUD",
            "df_principal_historico_BUD.parquet",
        )
        if not os.path.exists(caminho_budget):
            return []

        try:
            df = pd.read_parquet(caminho_budget, columns=["Oficina", "Ano"])
        except Exception:
            df = pd.read_parquet(caminho_budget)

        df = padronizar_colunas(df)

        if df is None or df.empty or 'Oficina' not in df.columns:
            return []

        if (
            ano_selecionado_param
            and ano_selecionado_param != "Todos"
            and 'Ano' in df.columns
        ):
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                pass

        return sorted(set(df['Oficina'].dropna().astype(str).unique().tolist()))
    except Exception:
        return []


@st.cache_data(ttl=3600, max_entries=10, show_spinner=False)
def get_budget_volume_oficinas_opcoes(ano_selecionado_param):
    """Retorna lista de oficinas existentes no Budget de Volume (histórico consolidado BUD)."""
    try:
        project_root = _get_project_root_tc_veic()
        caminho_budget_vol = os.path.join(
            project_root,
            "dados",
            "TC_Principal",
            "historico_consolidado",
            "BUD",
            "df_vol_historico_BUD.parquet",
        )
        if not os.path.exists(caminho_budget_vol):
            return []

        try:
            df = pd.read_parquet(caminho_budget_vol, columns=["Oficina", "Ano"])
        except Exception:
            df = pd.read_parquet(caminho_budget_vol)

        df = padronizar_colunas(df)

        if df is None or df.empty or 'Oficina' not in df.columns:
            return []

        if (
            ano_selecionado_param
            and ano_selecionado_param != "Todos"
            and 'Ano' in df.columns
        ):
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                pass

        return sorted(set(df['Oficina'].dropna().astype(str).unique().tolist()))
    except Exception:
        return []

# Cabeçalho compacto com data de atualização
injetar_css_global()
render_header()

is_main_page = True

st.title("\U0001f52e BE (Análise) — Base Home (TC Ext)")
st.subheader("Análise de dados agrupados por Oficina e Período")
st.markdown("---")

# ── Sidebar Global (Ano, Moeda, Taxas, Tipo, Fator, Tema) ──
cfg = render_sidebar_global('be_ana', incluir_todos=True)
ano_selecionado = cfg['ano']
moeda_codigo = cfg['moeda']
moeda_simbolo = cfg['simbolo']
taxas_cambio = cfg['taxas']
tipo_visualizacao = cfg['tipo']
fator_conversao = cfg['fator']

# Manter compatibilidade com session_state legado
st.session_state.filtro_ano_tc_veic = ano_selecionado


# Função para carregar dados com cache (disponível para todas as páginas - deve estar antes do uso)
@st.cache_data(
    ttl=3600,
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_data(ano_selecionado_param, mtime_forecast=None):
    """Carrega os dados do arquivo parquet - SEMPRE da pasta Forecast (BE Análise)."""
    try:
        caminho_forecast = os.path.join("dados", "TC_Principal", "Forecast", "forecast_completo.parquet")
        if not os.path.exists(caminho_forecast):
            st.error(f"❌ Arquivo não encontrado: {caminho_forecast}")
            st.info("💡 Por favor, gere os arquivos de Best Estimate na página '2 - Best Estimate - Simulador'.")
            st.stop()

        mtime_atual = os.path.getmtime(caminho_forecast) if os.path.exists(caminho_forecast) else 0
        if mtime_forecast is not None and mtime_forecast != mtime_atual:
            load_data.clear()

        df = pd.read_parquet(caminho_forecast)
        df = padronizar_colunas(df)

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
        # Isso evita que sejam convertidas para categorical
        colunas_numericas = ['Despesa Primaria', 'Custo FP', 'Volume', 'CPU']
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


# Função auxiliar para obter opções de filtro (disponível para todas as páginas - deve estar antes do uso)
@st.cache_data(ttl=1800, max_entries=5)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache"""
    if column_name in df.columns:
        opcoes = sorted(
            df[column_name].dropna().astype(str).unique().tolist()
        )
        return ["Todos"] + opcoes
    return ["Todos"]

# Continuar apenas se estivermos na página principal
if is_main_page:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔍 Filtros**")

    # Alinhar com o comportamento esperado da análise de Best Estimate (página legacy removida):
    # - garantir que os mesmos arquivos do simulador existem
    # - invalidar cache quando os parquets são atualizados (mtime)
    caminho_forecast_check = os.path.join("dados", "TC_Principal", "Forecast", "forecast_completo.parquet")
    caminho_vol_check = os.path.join("dados", "TC_Principal", "Forecast", "df_vol_historico.parquet")
    arquivos_existem = os.path.exists(caminho_forecast_check) and os.path.exists(caminho_vol_check)
    if not arquivos_existem:
        st.warning("⚠️ Arquivos de forecast não encontrados.")
        st.info("ℹ️ Por favor, gere os arquivos de Best Estimate na página **2 - Best Estimate - Simulador**.")
        st.info("📁 Arquivos esperados:")
        st.info(f"   - {caminho_forecast_check}")
        st.info(f"   - {caminho_vol_check}")
        st.stop()

    mtime_forecast_atual = os.path.getmtime(caminho_forecast_check) if os.path.exists(caminho_forecast_check) else 0
    mtime_vol_atual = os.path.getmtime(caminho_vol_check) if os.path.exists(caminho_vol_check) else 0
    try:
        if st.session_state.get('mtime_forecast_anterior_be_analise_home_tc') != mtime_forecast_atual:
            load_data.clear()
            # load_volume_data ainda pode não estar definido nesta altura do arquivo
            try:
                load_volume_data.clear()
            except Exception:
                pass
        st.session_state['mtime_forecast_anterior_be_analise_home_tc'] = mtime_forecast_atual
        st.session_state['mtime_vol_anterior_be_analise_home_tc'] = mtime_vol_atual
    except Exception:
        pass

    # Carregar dados com o ano selecionado
    try:
        df_total = load_data(ano_selecionado, mtime_forecast_atual)
        # Evitar mutações no cache
        if df_total is not None:
            df_total = df_total.copy()
        
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

    # Default: quando existir coluna Tipo, iniciar mostrando BE (sem sobrescrever escolhas do usuário)
    # Objetivo: deixar evidente a previsão gerada pelo simulador.
    if 'Tipo' in df_total.columns and 'filtro_Tipo_tc_veic' not in st.session_state:
        try:
            tipos_disponiveis = set(df_total['Tipo'].dropna().astype(str).unique().tolist())
            if 'BE' in tipos_disponiveis and ano_selecionado != "Todos":
                st.session_state['filtro_Tipo_tc_veic'] = ["BE"]
        except Exception:
            pass

    # Aplicar fator de conversão nas colunas Total e BUD (antes de qualquer processamento)
    # Isso simplifica os cálculos pois o fator é aplicado uma única vez na origem
    # Mantém os dados na mesma unidade para comparações consistentes
    # 🔧 CORREÇÃO CRÍTICA: NÃO aplicar fator de conversão quando está em modo CPU
    # No modo CPU, o fator não deve ser aplicado pois CPU já é uma razão (Total/Volume)
    if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
        if fator_conversao == "K (milhares)":
            if 'Custo FP' in df_total.columns:
                df_total['Custo FP'] = df_total['Custo FP'] / 1000
        elif fator_conversao == "M (Milhões)":
            if 'Custo FP' in df_total.columns:
                df_total['Custo FP'] = df_total['Custo FP'] / 1000000

    # Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
    # Isso garante que todos os dados derivados já terão a conversão aplicada
    # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
    # IMPORTANTE: Aplicar conversão em AMBOS os modos (Custo Total e CPU)
    # No modo CPU, o Total convertido será usado para calcular CPU = Total convertido / Volume
    if moeda_codigo != "BRL" and 'Custo FP' in df_total.columns:
        df_total = _core_converter_coluna_moeda(df_total, 'Custo FP', moeda_codigo, taxas_cambio)

    def _sync_oficina_from_sidebar():
        selecionadas = st.session_state.get('filtro_oficina_tc_veic_multiselect', ["Todos"]) or ["Todos"]
        st.session_state.filtro_oficina_tc_veic = selecionadas
        st.session_state['filtro_oficina_grafico_periodo_tc'] = selecionadas

    def _sync_veiculo_from_sidebar():
        selecionadas = st.session_state.get('filtro_veiculo_tc_veic_multiselect', ["Todos"]) or ["Todos"]
        st.session_state.filtro_veiculo_tc_veic = selecionadas
        st.session_state['filtro_veiculo_grafico_periodo_tc'] = selecionadas

    # Inicializar session_state para filtros
    if 'filtro_oficina_tc_veic' not in st.session_state:
        st.session_state.filtro_oficina_tc_veic = ["Todos"]

    # Filtro 1: Oficina (com cache otimizado)
    if 'Oficina' in df_total.columns:
        # 🔧 Ajuste: incluir oficinas disponíveis no Budget (custos) e no Budget de Volume
        oficinas_set = set(df_total['Oficina'].dropna().astype(str).unique().tolist())
        oficinas_set.update(get_budget_oficinas_opcoes(ano_selecionado))
        oficinas_set.update(get_budget_volume_oficinas_opcoes(ano_selecionado))
        oficina_opcoes = ["Todos"] + sorted(oficinas_set)
        st.session_state['_oficina_opcoes_tc_veic'] = oficina_opcoes
        if 'filtro_oficina_tc_veic_multiselect' not in st.session_state:
            st.session_state['filtro_oficina_tc_veic_multiselect'] = st.session_state.filtro_oficina_tc_veic
        # Validar valores salvos
        default_oficina = st.session_state.filtro_oficina_tc_veic if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_tc_veic) else ["Todos"]
        oficina_selecionadas = st.sidebar.multiselect(
            "Selecione a Oficina:",
            oficina_opcoes,
            default=default_oficina,
            key="filtro_oficina_tc_veic_multiselect",
            on_change=_sync_oficina_from_sidebar,
        )
        # Atualizar session_state
        st.session_state.filtro_oficina_tc_veic = oficina_selecionadas if oficina_selecionadas else ["Todos"]

        # Filtrar o DataFrame com base na Oficina
        if "Todos" in oficina_selecionadas or not oficina_selecionadas:
            df_filtrado = df_total.copy()
        else:
            df_filtrado = df_total[
                df_total['Oficina'].astype(str).isin(oficina_selecionadas)
            ].copy()
    else:
        df_filtrado = df_total.copy()

    # Filtro 2: Veículo
    if 'filtro_veiculo_tc_veic' not in st.session_state:
        st.session_state.filtro_veiculo_tc_veic = ["Todos"]
    
    if 'Veículo' in df_filtrado.columns:
        veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
        st.session_state['_veiculo_opcoes_tc_veic'] = veiculo_opcoes
        if 'filtro_veiculo_tc_veic_multiselect' not in st.session_state:
            st.session_state['filtro_veiculo_tc_veic_multiselect'] = st.session_state.filtro_veiculo_tc_veic
        default_veiculo = st.session_state.filtro_veiculo_tc_veic if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_tc_veic) else ["Todos"]
        veiculo_selecionados = st.sidebar.multiselect(
            "Selecione o Veículo:",
            veiculo_opcoes,
            default=default_veiculo,
            key="filtro_veiculo_tc_veic_multiselect",
            on_change=_sync_veiculo_from_sidebar,
        )
        st.session_state.filtro_veiculo_tc_veic = veiculo_selecionados if veiculo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no Veículo
        if veiculo_selecionados and "Todos" not in veiculo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
            ].copy()

    # Filtro 3: USI
    if 'filtro_usi_tc_veic' not in st.session_state:
        st.session_state.filtro_usi_tc_veic = ["Todos"]
    
    if 'USI' in df_filtrado.columns:
        usi_opcoes = get_filter_options(df_filtrado, 'USI')
        default_usi = st.session_state.filtro_usi_tc_veic if all(x in usi_opcoes for x in st.session_state.filtro_usi_tc_veic) else ["Todos"]
        usi_selecionadas = st.sidebar.multiselect(
            "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_tc_veic_multiselect"
        )
        st.session_state.filtro_usi_tc_veic = usi_selecionadas if usi_selecionadas else ["Todos"]
        
        # Filtrar o DataFrame com base na USI
        if usi_selecionadas and "Todos" not in usi_selecionadas:
            df_filtrado = df_filtrado[
                df_filtrado['USI'].astype(str).isin(usi_selecionadas)
            ].copy()

    # Filtro 4: Período
    if 'filtro_periodo_tc_veic' not in st.session_state:
        st.session_state.filtro_periodo_tc_veic = ["Todos"]
    
    if 'Período' in df_filtrado.columns:
        # 🔧 CORREÇÃO: não limitar a meses do realizado.
        # Sempre oferecer todos os meses (e também o que existir em Budget/Volume Budget).
        ordem_meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                      'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

        periodos_set = set(ordem_meses)
        try:
            periodo_opcoes_real = get_filter_options(df_total, 'Período') if 'Período' in df_total.columns else []
            periodos_set.update([p for p in periodo_opcoes_real if p and p != 'Todos'])
        except Exception:
            pass

        # Trazer períodos do Budget (custos) e do Budget (volume), quando disponíveis
        try:
            df_budget_opcoes = load_budget_data(ano_selecionado)
            if df_budget_opcoes is not None and 'Período' in df_budget_opcoes.columns:
                periodos_set.update(df_budget_opcoes['Período'].dropna().astype(str).unique().tolist())
        except Exception:
            pass

        try:
            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
            if df_budget_vol_opcoes is not None and 'Período' in df_budget_vol_opcoes.columns:
                periodos_set.update(df_budget_vol_opcoes['Período'].dropna().astype(str).unique().tolist())
        except Exception:
            pass

        # Montar lista ordenada: meses (sempre) + outros períodos
        outros_periodos = sorted([p for p in periodos_set if p not in ordem_meses and p not in (None, '', 'Todos')])
        periodo_opcoes_ordenados = ["Todos"] + ordem_meses + outros_periodos
        
        default_periodo = st.session_state.filtro_periodo_tc_veic if all(x in periodo_opcoes_ordenados for x in st.session_state.filtro_periodo_tc_veic) else ["Todos"]
        periodo_selecionados = st.sidebar.multiselect(
            "Selecione o Período:", periodo_opcoes_ordenados, default=default_periodo, key="filtro_periodo_tc_veic_multiselect"
        )
        st.session_state.filtro_periodo_tc_veic = periodo_selecionados if periodo_selecionados else ["Todos"]
        
        # Filtrar o DataFrame com base no Período
        if periodo_selecionados and "Todos" not in periodo_selecionados:
            df_filtrado = df_filtrado[
                df_filtrado['Período'].astype(str).isin(periodo_selecionados)
            ].copy()

    # Filtros principais adicionais
    filtros_principais = [
        ("Type 05", "Type 05", "multiselect"),
        ("Type 06", "Type 06", "multiselect"),
        ("Account", "Account", "multiselect"),
        ("Fornecedor", "Fornecedor", "multiselect"),
        ("Fornec.", "Fornec.", "multiselect"),
        ("Tipo", "Tipo", "multiselect")
    ]

    for col_name, label, widget_type in filtros_principais:
        if col_name in df_filtrado.columns:
            # Inicializar session_state para cada filtro principal
            filtro_key = f'filtro_{col_name}_tc_veic'
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
            ("Custo", "Custo"),
            ("Type 07", "Type 07"),
            ("Texto breve", "Texto breve"),
            ("Material", "Material"),
            ("Pedido", "Pedido"),
            ("Ordem", "Ordem"),
            ("CtAtvFixo", "CtAtvFixo")
        ]
        
        for col_name, label in filtros_avancados:
            if col_name in df_filtrado.columns:
                # Inicializar session_state para cada filtro avançado
                filtro_key = f'filtro_{col_name}_tc_veic_av'
                if filtro_key not in st.session_state:
                    st.session_state[filtro_key] = ["Todos"]
                
                opcoes = get_filter_options(df_filtrado, col_name)
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

# Função auxiliar para encontrar arquivo parquet na ordem de prioridade (disponível para todas as páginas)
def encontrar_arquivo_parquet(nome_arquivo, ano_selecionado=None):
    """Busca arquivo parquet no TC_Principal (TC Veículos).

    Ordem de prioridade:
    1. Forecast
    2. Ano específico
    3. Histórico consolidado
    4. Ano mais recente
    """
    from tc_core.data.paths import PASTA_TC_PRINCIPAL
    pasta = PASTA_TC_PRINCIPAL

    # 1. Forecast
    if nome_arquivo == "df_principal.parquet":
        caminho_fc = os.path.join(pasta, "Forecast", "forecast_completo.parquet")
        if os.path.exists(caminho_fc):
            return caminho_fc
    if "vol" in nome_arquivo:
        caminho_fc_vol = os.path.join(pasta, "Forecast", "df_vol_historico.parquet")
        if os.path.exists(caminho_fc_vol):
            return caminho_fc_vol

    # 2. Ano específico
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join(pasta, str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano

    # 3. Histórico consolidado
    caminho_hist = os.path.join(pasta, "historico_consolidado",
                                 nome_arquivo.replace(".parquet", "_historico.parquet"))
    if os.path.exists(caminho_hist):
        return caminho_hist
    # Volume fallback
    if "vol" in nome_arquivo:
        caminho_hist_vol = os.path.join(pasta, "historico_consolidado", "df_vol_historico.parquet")
        if os.path.exists(caminho_hist_vol):
            return caminho_hist_vol

    # 4. Ano mais recente
    if os.path.exists(pasta):
        anos = [d for d in os.listdir(pasta)
                if os.path.isdir(os.path.join(pasta, d)) and d.isdigit()]
        if anos:
            ano_recente = max(anos, key=int)
            caminho = os.path.join(pasta, ano_recente, nome_arquivo)
            if os.path.exists(caminho):
                return caminho

    return None


# Função para converter valor de R$ para outra moeda
def converter_moeda(valor, moeda_destino, taxas):
    """Converte valor de R$ (BRL) para a moeda de destino."""
    return _core_converter_moeda(valor, moeda_destino, taxas)

# Função para converter coluna inteira de DataFrame
def converter_coluna_moeda(df, coluna, moeda_destino, taxas):
    """Converte uma coluna inteira de R$ para outra moeda."""
    return _core_converter_coluna_moeda(df, coluna, moeda_destino, taxas)

# Função para obter símbolo da moeda (disponível para todas as páginas)
def obter_simbolo_moeda(moeda_codigo):
    """Retorna o símbolo da moeda."""
    return _core_obter_simbolo_moeda(moeda_codigo)

## load_data: definição única fica acima (com mtime), alinhada com a BE Análise.


# Função para carregar dados de volume com cache
@st.cache_data(
    ttl=60,  # 🔧 REDUZIDO para 60 segundos para forçar atualização mais frequente
    max_entries=10,  # Aumentar para cachear diferentes anos
    show_spinner=True
)
def load_volume_data(ano_selecionado_param):
    """Carrega os dados de volume do arquivo parquet - SEMPRE do Best Estimate consolidado"""
    try:
        # IMPORTANTE: Sempre carregar do Best Estimate consolidado para garantir consistência
        # Apenas aplicar filtro de ano quando necessário
        caminho_historico = os.path.join("dados", "TC_Principal", "Forecast", "df_vol_historico.parquet")
        
        if not os.path.exists(caminho_historico):
            return None

        # Invalidar cache se o arquivo foi atualizado (mesma ideia da BE Análise)
        try:
            mtime_atual = os.path.getmtime(caminho_historico) if os.path.exists(caminho_historico) else 0
            mtime_anterior = st.session_state.get('mtime_vol_anterior_be_analise_home_tc')
            if mtime_anterior is not None and mtime_anterior != mtime_atual:
                load_volume_data.clear()
            st.session_state['mtime_vol_anterior_be_analise_home_tc'] = mtime_atual
        except Exception:
            pass

        if os.path.exists(caminho_historico):
            # 🔧 CORREÇÃO: Garantir que Volume seja sempre numérico ao carregar
            df = pd.read_parquet(caminho_historico)
            df = padronizar_colunas(df)
            if 'Volume' in df.columns:
                df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')

        # Se um ano específico foi selecionado, filtrar após carregar
        # Isso garante que sempre usamos a mesma fonte de dados (histórico consolidado)
        # e apenas filtramos pelo ano, mantendo consistência
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
        # Isso evita que sejam convertidas para categorical
        colunas_numericas = ['Despesa Primaria', 'Custo FP', 'Volume', 'CPU']
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


def filtrar_volume_com_sidebar(df_vol, df_total_base):
    """Aplica os filtros da sidebar aos dados de volume."""
    if df_vol is None:
        return None

    df_vol_filtrado = df_vol.copy()

    # Filtro 1: Oficina
    if 'Oficina' in df_vol_filtrado.columns and df_total_base is not None:
        # 🔧 Ajuste: incluir também oficinas disponíveis no Budget (custos e volume)
        oficinas_set = set(get_filter_options(df_total_base, 'Oficina'))
        oficinas_set.discard("Todos")
        try:
            df_budget_opcoes = load_budget_data(ano_selecionado)
            if df_budget_opcoes is not None and 'Oficina' in df_budget_opcoes.columns:
                oficinas_set.update(df_budget_opcoes['Oficina'].dropna().astype(str).unique().tolist())
        except Exception:
            pass
        try:
            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
            if df_budget_vol_opcoes is not None and 'Oficina' in df_budget_vol_opcoes.columns:
                oficinas_set.update(df_budget_vol_opcoes['Oficina'].dropna().astype(str).unique().tolist())
        except Exception:
            pass
        oficina_opcoes_disponiveis = sorted(oficinas_set)
        oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_veic', ["Todos"])

        if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
            ].copy()
        else:
            oficinas_validas = [o for o in oficina_selecionadas_sidebar if o in oficina_opcoes_disponiveis]
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Oficina'].astype(str).isin(oficinas_validas)
            ].copy()

    # Filtro 2: Veículo
    if 'Veículo' in df_vol_filtrado.columns:
        veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_veic', ["Todos"])
        if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_sidebar)
            ].copy()

    # Filtro 3: USI
    if 'USI' in df_vol_filtrado.columns:
        usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_veic', ["Todos"])
        if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['USI'].astype(str).isin(usi_selecionada_sidebar)
            ].copy()

    # Filtro 5: Centro cst
    if 'Centrocst' in df_vol_filtrado.columns:
        centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_veic', "Todos")
        if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
            ].copy()

    # Filtro 6: Conta contábil
    if 'Nºconta' in df_vol_filtrado.columns:
        conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_veic', [])
        if conta_contabil_selecionadas_sidebar:
            df_vol_filtrado = df_vol_filtrado[
                df_vol_filtrado['Nºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
            ].copy()

    # Filtros principais
    filtros_principais_nomes = ["Type 05", "Type 06", "Fornecedor", "Fornec.", "Tipo"]
    for col_name in filtros_principais_nomes:
        if col_name in df_vol_filtrado.columns:
            filtro_key = f'filtro_{col_name}_tc_veic'
            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                df_vol_filtrado = df_vol_filtrado[
                    df_vol_filtrado[col_name].astype(str).isin(selecionadas_sidebar)
                ].copy()

    # Filtros avançados
    filtros_avancados_nomes = ["Usuário", "Material", "Dt.lçto.", "Texto breve", "Account"]
    for col_name in filtros_avancados_nomes:
        if col_name in df_vol_filtrado.columns:
            filtro_key = f'filtro_avancado_{col_name}_tc_veic'
            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                df_vol_filtrado = df_vol_filtrado[
                    df_vol_filtrado[col_name].astype(str).isin(selecionadas_sidebar)
                ].copy()

    return df_vol_filtrado


def _merge_volume_com_fallback(df_base, df_volume):
    """Garante coluna Volume em df_base usando chaves disponíveis; fallback por Oficina."""
    if df_base is None or df_volume is None:
        return df_base
    if 'Volume' not in df_volume.columns:
        return df_base

    df_out = df_base.copy()

    # Se Volume já existe e parece válido (tem valores não-nulos e não-zerados), não mexer.
    if 'Volume' in df_out.columns:
        try:
            vol_existing = pd.to_numeric(df_out['Volume'], errors='coerce')
            if vol_existing.notna().any() and float(vol_existing.fillna(0).abs().sum()) > 0.0:
                return df_out
        except Exception:
            # Se der problema de conversão, tentar recalcular por merge.
            pass

        # Caso esteja todo NaN/0, vamos substituir via merge.
        try:
            df_out = df_out.drop(columns=['Volume'])
        except Exception:
            pass

    # Normalizar dimensões para reduzir mismatch (spaces/categorias)
    df_base_tmp = df_out.copy()
    df_vol_tmp = df_volume.copy()
    for col in ['Oficina', 'Veículo', 'Período', 'Ano']:
        if col in df_base_tmp.columns and col in df_vol_tmp.columns:
            df_base_tmp[col] = df_base_tmp[col].astype(str).str.strip()
            df_vol_tmp[col] = df_vol_tmp[col].astype(str).str.strip()

    df_vol_tmp['Volume'] = pd.to_numeric(df_vol_tmp['Volume'], errors='coerce')

    # Tentar merges do mais granular para o mais agregador (para evitar conflito de filtros)
    candidatos = []
    chaves_full = [
        col for col in ['Oficina', 'Veículo', 'Período', 'Ano']
        if col in df_base_tmp.columns and col in df_vol_tmp.columns
    ]
    if chaves_full:
        candidatos.append(chaves_full)

    for ks in [
        ['Oficina', 'Período', 'Ano'],
        ['Oficina', 'Período'],
        ['Oficina', 'Ano'],
        ['Oficina'],
    ]:
        ks_ok = [c for c in ks if c in df_base_tmp.columns and c in df_vol_tmp.columns]
        if ks_ok and ks_ok not in candidatos:
            candidatos.append(ks_ok)

    for chaves in candidatos:
        try:
            vol_agr = df_vol_tmp.groupby(chaves)['Volume'].sum().reset_index()
            merged = df_base_tmp.merge(vol_agr, on=chaves, how='left')
            vol_m = pd.to_numeric(merged['Volume'], errors='coerce')
            if vol_m.notna().any() and float(vol_m.fillna(0).abs().sum()) > 0.0:
                return merged
        except Exception:
            continue

    # Último fallback: adiciona Volume=0 para evitar falhas posteriores
    df_base_tmp['Volume'] = 0
    return df_base_tmp


# Função para carregar dados de budget (Total) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_data(ano_selecionado_param):
    """Carrega os dados de budget do arquivo parquet - SEMPRE do histórico consolidado BUD"""
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        caminho_budget = os.path.join(
            project_root,
            "dados",
            "TC_Principal",
            "historico_consolidado",
            "BUD",
            "df_principal_historico_BUD.parquet",
        )
        
        if os.path.exists(caminho_budget):
            df = pd.read_parquet(caminho_budget)
            df = padronizar_colunas(df)
        else:
            return None

        # Se um ano específico foi selecionado, filtrar após carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
        colunas_numericas = ['Despesa Primaria', 'Custo FP', 'Volume', 'CPU']
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


# Função para carregar dados de budget (Volume) com cache
@st.cache_data(
    ttl=3600,
    max_entries=10,
    show_spinner=True
)
def load_budget_volume_data(ano_selecionado_param):
    """Carrega os dados de volume de budget do arquivo parquet - SEMPRE do histórico consolidado BUD"""
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        caminho_budget_vol = os.path.join(
            project_root,
            "dados",
            "TC_Principal",
            "historico_consolidado",
            "BUD",
            "df_vol_historico_BUD.parquet",
        )
        
        if os.path.exists(caminho_budget_vol):
            df = pd.read_parquet(caminho_budget_vol)
            df = padronizar_colunas(df)
        else:
            return None

        # Governança: Volume BUD precisa ter 'Veículo'. Se não tiver, é erro de extração.
        if 'Veículo' not in df.columns:
            raise ValueError(
                "❌ ERRO NA EXTRAÇÃO: o arquivo 'df_vol_historico_BUD.parquet' não contém a coluna 'Veículo'. "
                "Refaça a extração do BUDGET e verifique a aba 'Volume BDG'."
            )

        # Se um ano específico foi selecionado, filtrar após carregar
        if ano_selecionado_param and ano_selecionado_param != "Todos" and "Ano" in df.columns:
            try:
                df = df[df['Ano'] == int(ano_selecionado_param)].copy()
            except (ValueError, TypeError):
                # Se não conseguir converter para int, não filtrar por ano
                pass

        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos para formato capitalizado (primeira letra maiúscula)
        # Isso garante consistência com o resto do código que espera períodos capitalizados
        if 'Período' in df.columns:
            mapeamento_meses = {
                'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
            }
            
            def normalizar_periodo(periodo):
                """Normaliza período para formato capitalizado"""
                if pd.isna(periodo):
                    return periodo
                periodo_str = str(periodo).strip()
                periodo_lower = periodo_str.lower()
                if periodo_lower in mapeamento_meses:
                    return mapeamento_meses[periodo_lower]
                return periodo_str  # Retornar original se não for um mês conhecido
            
            df['Período'] = df['Período'].apply(normalizar_periodo)

        # Converter colunas numéricas conhecidas para numérico ANTES da otimização
        colunas_numericas = ['Volume']
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
        # Não mascarar erro de governança (Volume BUD sem Veículo)
        if isinstance(e, ValueError) and "ERRO NA EXTRAÇÃO" in str(e):
            raise
        return None

# Ordem dos meses para ordenação cronológica (disponível para todas as páginas)
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]


def _formatar_num_ptbr(valor, casas=2):
    """Formata número no padrão pt-BR (1.234,56)."""
    try:
        if pd.isna(valor):
            return "-"
        v = float(valor)
        s = f"{v:,.{casas}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "-"


def _normalizar_mes_lower(periodo):
    if pd.isna(periodo):
        return periodo
    p = str(periodo).strip()
    if not p:
        return p
    pl = p.lower()
    mapeamento = {
        'janeiro': 'janeiro',
        'fevereiro': 'fevereiro',
        'março': 'março',
        'marco': 'março',
        'abril': 'abril',
        'maio': 'maio',
        'junho': 'junho',
        'julho': 'julho',
        'agosto': 'agosto',
        'setembro': 'setembro',
        'outubro': 'outubro',
        'novembro': 'novembro',
        'dezembro': 'dezembro',
    }
    return mapeamento.get(pl, pl)


def _montar_tabela_resumo_oficinas(
    df_valores,
    tipo_visualizacao,
    index_name,
    coluna_valor_preferida=None,
    df_volume=None,
):
    """Gera tabela (Oficina x mês + Ano) alinhada aos filtros e ao modo CPU."""
    try:
        if df_valores is None or getattr(df_valores, "empty", True):
            return None

        if 'Oficina' not in df_valores.columns or 'Período' not in df_valores.columns:
            return None

        base = df_valores.copy()
        base['Oficina'] = base['Oficina'].astype(str).str.strip()
        base['Período'] = base['Período'].apply(_normalizar_mes_lower)

        # Para CPU precisamos de Total (custo) e Volume no MESMO nível da linha.
        # Regra: CPU sempre é calculado como soma(Total)/soma(Volume) no nível desejado.
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if 'Custo FP' not in base.columns:
                return None

            if df_volume is None or getattr(df_volume, "empty", True) or 'Volume' not in df_volume.columns:
                return None

            # 1) Numerador: custo agregado por Oficina+Período
            custo = base[['Oficina', 'Período', 'Custo FP']].copy()
            custo['Custo FP'] = pd.to_numeric(custo['Custo FP'], errors='coerce').fillna(0)
            custo_mes = custo.groupby(['Oficina', 'Período'], as_index=False)['Custo FP'].sum()
            custo_ano = custo.groupby(['Oficina'], as_index=False)['Custo FP'].sum().rename(columns={'Custo FP': 'Total_Ano'})

            # 2) Denominador: volume agregado por Oficina+Período (NÃO mergear volume na granularidade de custo)
            vol = df_volume.copy()
            if 'Oficina' not in vol.columns or 'Período' not in vol.columns:
                return None
            vol['Oficina'] = vol['Oficina'].astype(str).str.strip()
            vol['Período'] = vol['Período'].apply(_normalizar_mes_lower)
            vol['Volume'] = pd.to_numeric(vol['Volume'], errors='coerce').fillna(0)
            vol_mes = vol.groupby(['Oficina', 'Período'], as_index=False)['Volume'].sum()
            vol_ano = vol.groupby(['Oficina'], as_index=False)['Volume'].sum().rename(columns={'Volume': 'Volume_Ano'})

            # 3) Juntar no nível correto e calcular CPU por célula
            agr_mes = custo_mes.merge(vol_mes, on=['Oficina', 'Período'], how='outer')
            agr_mes['Custo FP'] = pd.to_numeric(agr_mes.get('Custo FP'), errors='coerce').fillna(0)
            agr_mes['Volume'] = pd.to_numeric(agr_mes.get('Volume'), errors='coerce').fillna(0)
            agr_mes['Metrica'] = np.where(agr_mes['Volume'] != 0, agr_mes['Custo FP'] / agr_mes['Volume'], np.nan)

            agr_ano = custo_ano.merge(vol_ano, on=['Oficina'], how='outer')
            agr_ano['Total_Ano'] = pd.to_numeric(agr_ano.get('Total_Ano'), errors='coerce').fillna(0)
            agr_ano['Volume_Ano'] = pd.to_numeric(agr_ano.get('Volume_Ano'), errors='coerce').fillna(0)
            agr_ano['Ano'] = np.where(agr_ano['Volume_Ano'] != 0, agr_ano['Total_Ano'] / agr_ano['Volume_Ano'], np.nan)

            piv = agr_mes.pivot_table(index='Oficina', columns='Período', values='Metrica', aggfunc='sum')
            piv['Ano'] = piv.index.to_series().map(dict(zip(agr_ano['Oficina'], agr_ano['Ano'])))

            # Linha Total (CPU total = soma(Total)/soma(Volume) por Período e no Ano)
            tot_mes = agr_mes.groupby('Período', as_index=False).agg({'Custo FP': 'sum', 'Volume': 'sum'})
            tot_mes['CPU'] = np.where(tot_mes['Volume'] != 0, tot_mes['Custo FP'] / tot_mes['Volume'], np.nan)
            total_row = {str(p): np.nan for p in ORDEM_MESES}
            for _, r in tot_mes.iterrows():
                p = r.get('Período')
                if pd.notna(p):
                    total_row[str(p)] = r.get('CPU')
            total_total = float(pd.to_numeric(agr_mes['Custo FP'], errors='coerce').fillna(0).sum())
            total_volume = float(pd.to_numeric(agr_mes['Volume'], errors='coerce').fillna(0).sum())
            total_row['Ano'] = (total_total / total_volume) if total_volume != 0 else np.nan
            piv.loc['Custo FP'] = pd.Series(total_row)
        else:
            col_valor = coluna_valor_preferida if coluna_valor_preferida in base.columns else None
            if col_valor is None:
                col_valor = 'Custo FP' if 'Custo FP' in base.columns else None
            if col_valor is None:
                return None

            base[col_valor] = pd.to_numeric(base[col_valor], errors='coerce')
            agr_mes = base.groupby(['Oficina', 'Período'], as_index=False)[col_valor].sum()
            agr_ano = base.groupby(['Oficina'], as_index=False)[col_valor].sum().rename(columns={col_valor: 'Ano'})
            piv = agr_mes.pivot_table(index='Oficina', columns='Período', values=col_valor, aggfunc='sum')
            piv['Ano'] = piv.index.to_series().map(dict(zip(agr_ano['Oficina'], agr_ano['Ano'])))

            # Linha Total
            tot_mes = agr_mes.groupby('Período', as_index=False)[col_valor].sum()
            total_row = {str(p): np.nan for p in ORDEM_MESES}
            for _, r in tot_mes.iterrows():
                p = r.get('Período')
                if pd.notna(p):
                    total_row[str(p)] = r.get(col_valor)
            total_row['Ano'] = float(pd.to_numeric(agr_ano['Ano'], errors='coerce').fillna(0).sum()) if 'Ano' in agr_ano.columns else np.nan
            piv.loc['Custo FP'] = pd.Series(total_row)

        # Ordenar colunas (sempre exibir os 12 meses + Ano)
        cols = [c for c in piv.columns if isinstance(c, str)]
        outros = [c for c in cols if c not in ORDEM_MESES and c != 'Ano']
        ordem_final = list(ORDEM_MESES) + outros
        if 'Ano' in piv.columns:
            ordem_final += ['Ano']
        piv = piv.reindex(columns=ordem_final)

        # Ordenar oficinas alfabeticamente e manter "Total" no final
        if 'Custo FP' in piv.index:
            idx = [i for i in piv.index.tolist() if i != 'Custo FP']
            idx_sorted = sorted(idx)
            piv = piv.reindex(idx_sorted + ['Custo FP'])
        else:
            piv = piv.sort_index()
        piv.index.name = index_name
        return piv
    except Exception:
        return None

# (Código de filtros movido para dentro do bloco if is_main_page:)


def formatar_ratio_com_barra(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com barra de progresso em HTML"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular largura da barra: 100% = barra cheia, acima de 100% também fica cheia
    if percentual >= 100:
        largura_barra = 100  # Barra cheia para 100% ou mais
    else:
        largura_barra = percentual  # Proporcional até 100%
    
    # Calcular cor: verde até 90%, depois gradiente até vermelho em 100%
    if percentual <= 0:
        r, g, b = 0, 170, 0  # Verde (#00AA00)
    elif percentual <= 90:
        r, g, b = 0, 170, 0  # Verde puro até 90%
    elif percentual >= 100:
        r, g, b = 255, 0, 0  # Vermelho (#FF0000) quando 100% ou mais
    else:
        # Gradiente de verde para vermelho entre 90% e 100%
        # progresso vai de 0 (em 90%) a 1 (em 100%)
        progresso = (percentual - 90) / 10
        r = int(255 * progresso)  # 0 em 90%, 255 em 100%
        g = int(170 * (1 - progresso))  # 170 em 90%, 0 em 100%
        b = 0
    
    cor = f"rgb({r}, {g}, {b})"
    
    # Detectar tema para adaptar cor do texto (igual às outras colunas)
    try:
        theme_base = st.get_option("theme.base") or "light"
        # Usar a mesma cor que o Streamlit usa para texto em tabelas
        # Dark mode: rgb(250, 250, 250) ou #FAFAFA
        # Light mode: rgb(49, 51, 63) ou #31333F (cor padrão do Streamlit para texto)
        if theme_base == "dark":
            texto_cor = "#FAFAFA"  # Branco claro para dark mode
        else:
            texto_cor = "#31333F"  # Cinza escuro para light mode (cor padrão do Streamlit)
    except:
        # Fallback: tentar detectar via CSS do Streamlit
        texto_cor = "var(--text-color, #31333F)"  # Usar variável CSS se disponível, senão usar cor padrão
    
    html = f"""
    <div style="display: flex; align-items: center; gap: 5px; width: 100%; justify-content: flex-start; margin: 0; padding: 0; vertical-align: middle;">
        <div style="width: 64px; background-color: #333; border-radius: 3px; height: 11px; position: relative; overflow: hidden; flex-shrink: 0; margin: 0;">
            <div style="width: {largura_barra}%; height: 100%; background-color: {cor}; transition: width 0.3s;"></div>
        </div>
        <span style="width: 65px; text-align: left; font-weight: normal; color: {texto_cor}; font-size: 0.75rem; flex-shrink: 0; line-height: 1.2; margin: 0;">{percentual:.0f}%</span>
    </div>
    """
    return html

def criar_tabela_html_com_barra(df_display, linha_resumo=None, linha_volumes=None):
    """Cria uma tabela HTML customizada no padrão Streamlit para renderizar HTML nas células
    
    Args:
        df_display: DataFrame com os dados a serem exibidos
        linha_resumo: Dicionário opcional com valores de resumo formatados para adicionar como primeira linha
        linha_volumes: Dicionário opcional com volumes para adicionar como última linha (ex: {'Volume Real': '1,000', 'Volume Budget': '1,200'})
    """
    # Usar o padrão de estilos do Streamlit para st.dataframe
    try:
        theme_base = st.get_option("theme.base") or "light"
        if theme_base == "dark":
            # Cores transparentes no padrão Streamlit dark mode
            header_bg = "rgba(38, 39, 48, 0.15)"  # Cabeçalho mais transparente
            resumo_bg = "rgba(38, 39, 48, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(250, 250, 250, 0.1)"
        else:
            # Cores transparentes no padrão Streamlit light mode
            header_bg = "rgba(240, 242, 246, 0.15)"  # Cabeçalho mais transparente
            resumo_bg = "rgba(240, 242, 246, 0.15)"  # Linha de resumo mais transparente
            row_bg = "transparent"  # Todas as linhas transparentes
            border_color = "rgba(49, 51, 63, 0.1)"
    except:
        header_bg = "rgba(38, 39, 48, 0.15)"
        resumo_bg = "rgba(38, 39, 48, 0.15)"
        row_bg = "transparent"
        border_color = "rgba(250, 250, 250, 0.1)"
    
    # Criar tabela no padrão Streamlit
    html_table = """
    <div class='stDataFrame' style='overflow-x: auto; margin: 1rem 0;'>
        <style>
            .flex-bud-table {
                width: 100%;
                border-collapse: collapse;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            }
            .flex-bud-table thead tr {
                background-color: """ + header_bg + """;
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table th {
                padding: 0.75rem 1rem;
                text-align: left;
                font-weight: 600;
                font-size: 0.75rem;
                color: inherit;
            }
            .flex-bud-table tbody tr {
                border-bottom: 1px solid """ + border_color + """;
            }
            .flex-bud-table tbody tr:last-child {
                border-bottom: none;
            }
            .flex-bud-table .resumo-row {
                border-top: 2px solid """ + border_color + """;
            }
            .flex-bud-table td {
                padding: 0.75rem 1rem;
                font-size: 0.75rem;
                vertical-align: middle;
                font-weight: normal;
            }
            .flex-bud-table .resumo-row {
                background-color: """ + resumo_bg + """;
                font-weight: 600;
            }
            .flex-bud-table .number-cell {
                text-align: right;
                font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
                font-variant-numeric: tabular-nums;
                font-size: 0.7rem;
                font-weight: normal;
            }
            .flex-bud-table .total-flex-bud-col {
                max-width: 140px;
                width: 140px;
                white-space: nowrap;
            }
        </style>
        <table class='flex-bud-table'>
    """

    # Cabeçalho
    html_table += "<thead><tr>"
    for col in df_display.columns:
        if col == 'Total / Flex Bud':
            html_table += f"<th class='total-flex-bud-col'>{col}</th>"
        else:
            html_table += f"<th>{col}</th>"
    html_table += "</tr></thead><tbody>"
    
    # Linhas de dados - todas transparentes
    for idx, row in df_display.iterrows():
        html_table += f"<tr style='background-color: {row_bg};'>"
        for col in df_display.columns:
            if col == 'Total / Flex Bud':
                # O valor já deve estar formatado como HTML (com barrinha e percentual)
                # Se não estiver formatado, formatar agora
                valor_celula = row[col]
                if isinstance(valor_celula, str) and '<div' in valor_celula:
                    # Já está formatado como HTML
                    html_table += f"<td class='total-flex-bud-col'>{valor_celula}</td>"
                else:
                    # Formatar agora se ainda não estiver formatado
                    valor_num = float(valor_celula) if pd.notna(valor_celula) and isinstance(valor_celula, (int, float)) else 0
                    html_formatado = formatar_ratio_com_barra(valor_num)
                    html_table += f"<td class='total-flex-bud-col'>{html_formatado}</td>"
            else:
                valor_celula = str(row[col])
                if any(char.isdigit() or char in ['$', '€', 'R$', ',', '.', 'K', 'M'] for char in valor_celula):
                    html_table += f"<td class='number-cell'>{valor_celula}</td>"
                else:
                    html_table += f"<td>{valor_celula}</td>"
        html_table += "</tr>"
    
    # Linha de resumo removida - os resumos agora são exibidos separadamente com caixas de texto
    
    # Adicionar linha de volumes se fornecida
    if linha_volumes:
        html_table += f"<tr class='resumo-row' style='background-color: {resumo_bg}; border-top: 2px solid {border_color};'>"
        for col in df_display.columns:
            valor_volume = linha_volumes.get(col, '-')
            html_table += f"<td class='number-cell' style='font-weight: 600;'>{valor_volume}</td>"
        html_table += "</tr>"
    
    html_table += "</tbody></table></div>"
    return html_table

def formatar_periodo_abreviado(periodo_str, ano=None, usar_ano_completo=False):
    """Formata período para formato abreviado (ex: Setembro 2024 -> Set/24 ou Set/2024 se usar_ano_completo=True)
    
    Args:
        periodo_str: String do período (ex: "Setembro 2024", "Total 2024", "2024 S1", "2024 Q1")
        ano: Ano opcional (se None, será extraído de periodo_str)
        usar_ano_completo: Se True, usa ano com 4 dígitos (para Ano a Ano, Semestre, Quarter)
    """
    # Mapeamento de meses para abreviações
    meses_abrev = {
        'janeiro': 'Jan', 'fevereiro': 'Fev', 'março': 'Mar', 'abril': 'Abr',
        'maio': 'Mai', 'junho': 'Jun', 'julho': 'Jul', 'agosto': 'Ago',
        'setembro': 'Set', 'outubro': 'Out', 'novembro': 'Nov', 'dezembro': 'Dez'
    }
    
    periodo_str = str(periodo_str).strip()
    mes_abrev = None
    ano_extraido = None
    
    # Verificar se é formato especial (Ano a Ano, Semestre, Quarter)
    # Exemplos: "Total 2024", "2024 S1", "2024 Q1"
    if periodo_str.startswith('Total '):
        # Formato: "Total 2024" → retornar "Total/2024"
        partes = periodo_str.split(' ', 1)
        if len(partes) > 1:
            ano_str = partes[1].strip()
            if ano_str.isdigit():
                return f"Total/{ano_str}"
        return "Total"
    elif ' S' in periodo_str:
        # Formato: "2024 S1" ou "2024 S2" → retornar "2024/1" ou "2024/2"
        partes = periodo_str.split(' S')
        if len(partes) == 2:
            ano_str = partes[0].strip()
            semestre = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{semestre}"
        return periodo_str
    elif ' Q' in periodo_str:
        # Formato: "2024 Q1", "2024 Q2", etc. → retornar "2024/1", "2024/2", etc.
        partes = periodo_str.split(' Q')
        if len(partes) == 2:
            ano_str = partes[0].strip()
            quarter = partes[1].strip()
            if ano_str.isdigit():
                return f"{ano_str}/{quarter}"
        return periodo_str
    else:
        # Formato normal: "Setembro 2024" ou "setembro 2024"
        if ' ' in periodo_str:
            partes = periodo_str.split(' ', 1)
            mes_nome = partes[0].lower().strip()
            if len(partes) > 1:
                ano_str = partes[1].strip()
                # Tentar extrair ano (pode ser apenas número)
                if ano_str.isdigit():
                    ano_extraido = int(ano_str)
                # Se não for apenas número, tentar extrair primeiro número encontrado
                elif any(c.isdigit() for c in ano_str):
                    # Extrair primeiro sequência de dígitos
                    numero_str = ''.join([c for c in ano_str if c.isdigit()])[:4]  # Limitar a 4 dígitos
                    if numero_str:
                        ano_extraido = int(numero_str)
            
            # Obter abreviação do mês
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
        else:
            mes_nome = periodo_str.lower().strip()
            mes_abrev = meses_abrev.get(mes_nome, mes_nome[:3].capitalize() if len(mes_nome) >= 3 else mes_nome.capitalize())
    
    # Usar ano fornecido como parâmetro ou o extraído
    if ano is not None:
        ano_final = ano
    elif ano_extraido is not None:
        ano_final = ano_extraido
    else:
        ano_final = None
    
    # Formatar resultado
    if mes_abrev:
        if ano_final:
            # Usar últimos 2 dígitos para meses normais
            ano_abrev = str(ano_final)[-2:]
            return f"{mes_abrev}/{ano_abrev}"
        else:
            return mes_abrev
    else:
        return periodo_str

def reordenar_colunas_padrao(colunas_numericas):
    """Reordena colunas numéricas na ordem padrão: BUD, Flex Bud - BUD, Flex BUD, Total - Flex Bud, Total, Total / Flex Bud"""
    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
    colunas_ordenadas = []
    for col_ordem in ordem_colunas:
        if col_ordem in colunas_numericas:
            colunas_ordenadas.append(col_ordem)
    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
    for col in colunas_numericas:
        if col not in colunas_ordenadas:
            colunas_ordenadas.append(col)
    return colunas_ordenadas

def reorganizar_colunas_por_periodo(df_tabela_flex, periodos_selecionados, tipo_visualizacao):
    """Reorganiza a tabela para mostrar colunas por período na ordem de seleção"""
    if len(periodos_selecionados) <= 1 or 'Período' not in df_tabela_flex.columns:
        # Se há apenas 1 período ou não há coluna Período, retornar tabela original
        return df_tabela_flex
    
    # Manter a ordem de seleção dos períodos
    periodos_ordenados = periodos_selecionados.copy()
    
    # Criar lista de colunas na ordem especificada
    colunas_finais = []
    
    # Colunas de identificação (Type 05, Type 06, Account, etc.)
    colunas_id = []
    for col in ['Type 05', 'Type 06', 'Account', 'Custo']:
        if col in df_tabela_flex.columns:
            colunas_id.append(col)
    
    colunas_finais.extend(colunas_id)
    
    # Para cada período na ordem de seleção
    primeiro_periodo = periodos_ordenados[0]
    primeiro_periodo_abrev = formatar_periodo_abreviado(primeiro_periodo)
    
    # Primeiro período: Total, Flex (removendo coluna redundante "Flex - Total")
    colunas_finais.append(f"{primeiro_periodo_abrev}")
    colunas_finais.append(f"Flex {primeiro_periodo_abrev.lower()}")
    
    # Demais períodos: Período - Flex primeiro, Período, % Período/Flex primeiro
    for periodo in periodos_ordenados[1:]:
        periodo_abrev = formatar_periodo_abreviado(periodo)
        colunas_finais.append(f"{periodo_abrev} - Flex {primeiro_periodo_abrev.lower()}")
        colunas_finais.append(f"{periodo_abrev.lower()}")
        colunas_finais.append(f"% {periodo_abrev.lower()}/flex {primeiro_periodo_abrev.lower()}")
    
    # Criar DataFrame pivot por período
    # Primeiro, precisamos ter os dados separados por período
    # Vou criar uma estrutura que agrupa por categoria e período
    colunas_agrupamento = [col for col in ['Type 05', 'Type 06', 'Account', 'Custo'] if col in df_tabela_flex.columns]
    
    # Se não houver dados separados por período, retornar tabela original
    if 'Período' not in df_tabela_flex.columns or df_tabela_flex['Período'].nunique() <= 1:
        return df_tabela_flex
    
    # Criar pivot table com períodos como colunas
    df_pivot = df_tabela_flex.pivot_table(
        index=colunas_agrupamento if colunas_agrupamento else ['Type 06'],
        columns='Período',
        values=['Custo FP', 'Flex BUD', 'BUD'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Flatten column names
    df_pivot.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else str(col) for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Reorganizar colunas conforme especificado
    # Por enquanto, retornar a estrutura pivot básica
    # A reorganização completa será feita na exibição
    return df_pivot

def calcular_resumo_tabela_flex(df_original, tipo_visualizacao, moeda_simbolo, fator_conversao=None):
    """Calcula linha de resumo (totais) para tabela Flex Bud
    
    Args:
        df_original: DataFrame com valores numéricos originais (antes da formatação)
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        moeda_simbolo: Símbolo da moeda (R$, $, €)
        fator_conversao: Fator de conversão opcional (K, M)
    
    Returns:
        Dicionário com valores de resumo formatados (valores numéricos e formatados)
    """
    linha_resumo = {}
    linha_resumo_formatado = {}
    
    # Primeira coluna: "TOTAL"
    primeira_col = df_original.columns[0]
    linha_resumo[primeira_col] = "**TOTAL**"
    linha_resumo_formatado[primeira_col] = "**TOTAL**"
    
    # 🔧 CORREÇÃO: Para CPU, recalcular usando valores em Custo Total se disponíveis
    # (mesma lógica do gráfico - não somar valores em CPU diretamente)
    if tipo_visualizacao == "CPU (Custo por Unidade)":
        # Verificar se temos colunas auxiliares para recalcular corretamente
        if '_Flex_Bud_Total' in df_original.columns and '_Total_Custo_Total' in df_original.columns and '_Volume_Real' in df_original.columns:
            # 🔧 CORREÇÃO CRÍTICA: O gráfico calcula Flex Bud por período (sem categoria)
            # Quando há múltiplos períodos, o gráfico mostra cada período separadamente
            # Mas o valor total que o usuário vê é a soma de Flex Bud Total de TODOS os períodos e categorias
            # dividido pela soma dos volumes de todos os períodos
            
            # Somar TODAS as categorias e períodos (mesma lógica do gráfico)
            flex_bud_total_custo = df_original['_Flex_Bud_Total'].sum()  # Soma de TODAS as categorias e períodos
            total_custo_total = df_original['_Total_Custo_Total'].sum()  # Soma de TODAS as categorias e períodos
            
            # 🔧 CORREÇÃO CRÍTICA: _Volume_Real contém o volume total por período (não por categoria)
            # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
            # IMPORTANTE: Este valor já é a SOMA dos volumes de todos os períodos (calculado na linha 4668)
            # Então devemos usar o primeiro valor (todos são iguais)
            # 🔧 CORREÇÃO: Obter volume real corretamente
            volumes_reais = df_original['_Volume_Real'].dropna()
            if len(volumes_reais) > 0:
                # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
                # Usar o primeiro valor (todos são iguais)
                volume_total_real = float(volumes_reais.iloc[0]) if len(volumes_reais) > 0 else 0.0
            else:
                volume_total_real = 0.0
            
            # Recalcular CPU a partir dos totais (mesma lógica do gráfico)
            # Flex BUD CPU Total = (Soma de Flex Bud Total de todas as categorias) / (Volume Total)
            flex_bud_cpu = flex_bud_total_custo / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            total_cpu = total_custo_total / volume_total_real if volume_total_real != 0 and pd.notnull(volume_total_real) else 0
            
            # Calcular BUD também
            volume_total_budget = 0  # Inicializar
            if '_Budget_Total' in df_original.columns and '_Volume_Budget' in df_original.columns:
                budget_total_custo = df_original['_Budget_Total'].sum()  # Soma de TODAS as categorias
                # Mesma lógica para volume de budget
                # 🔧 CORREÇÃO: Obter volume budget corretamente
                volumes_budget = df_original['_Volume_Budget'].dropna()
                if len(volumes_budget) > 0:
                    # Quando há múltiplos períodos agregados, todos os volumes são iguais (volume total de todos os períodos)
                    # Usar o primeiro valor (todos são iguais)
                    volume_total_budget = float(volumes_budget.iloc[0]) if len(volumes_budget) > 0 else 0.0
                else:
                    volume_total_budget = 0.0
                bud_cpu = budget_total_custo / volume_total_budget if volume_total_budget != 0 and pd.notnull(volume_total_budget) else 0
            else:
                # Se não tiver colunas auxiliares, usar soma direta
                bud_cpu = df_original['BUD'].sum() if 'BUD' in df_original.columns else 0
                volume_total_budget = 0  # Não temos volume de budget disponível
            
            linha_resumo['Flex BUD'] = flex_bud_cpu
            linha_resumo['Custo FP'] = total_cpu
            linha_resumo['BUD'] = bud_cpu
            linha_resumo['Flex Bud - BUD'] = flex_bud_cpu - bud_cpu
            linha_resumo['Total - Flex Bud'] = total_cpu - flex_bud_cpu
            
            # 🔧 ADICIONAR: Incluir volumes usados nos cálculos (apenas para resumo geral)
            linha_resumo['_Volume_Real_Calculo'] = volume_total_real
            linha_resumo['_Volume_Budget_Calculo'] = volume_total_budget
            
            # Formatação
            linha_resumo_formatado['Flex BUD'] = f"{flex_bud_cpu:,.2f}"
            linha_resumo_formatado['Custo FP'] = f"{total_cpu:,.2f}"
            linha_resumo_formatado['BUD'] = f"{bud_cpu:,.2f}"
            linha_resumo_formatado['Flex Bud - BUD'] = f"{flex_bud_cpu - bud_cpu:,.2f}"
            linha_resumo_formatado['Total - Flex Bud'] = f"{total_cpu - flex_bud_cpu:,.2f}"
            # 🔧 ADICIONAR: Formatar volumes usados nos cálculos (sem casas decimais)
            linha_resumo_formatado['_Volume_Real_Calculo'] = f"{volume_total_real:,.0f}"
            linha_resumo_formatado['_Volume_Budget_Calculo'] = f"{volume_total_budget:,.0f}"
        else:
            # Se não tiver colunas auxiliares, somar diretamente (comportamento antigo)
            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                if col in df_original.columns:
                    soma = df_original[col].sum()
                    linha_resumo[col] = soma
                    linha_resumo_formatado[col] = f"{soma:,.2f}"
            
            # 🔧 ADICIONAR: Tentar obter volumes mesmo sem colunas auxiliares (se disponíveis)
            if '_Volume_Real' in df_original.columns:
                volumes_reais = df_original['_Volume_Real'].dropna()
                if len(volumes_reais) > 0:
                    volume_total_real = float(volumes_reais.iloc[0])
                else:
                    volume_total_real = 0.0
                linha_resumo['_Volume_Real_Calculo'] = volume_total_real
                linha_resumo_formatado['_Volume_Real_Calculo'] = f"{volume_total_real:,.0f}"
            else:
                linha_resumo['_Volume_Real_Calculo'] = 0
                linha_resumo_formatado['_Volume_Real_Calculo'] = "0"
            
            if '_Volume_Budget' in df_original.columns:
                volumes_budget = df_original['_Volume_Budget'].dropna()
                if len(volumes_budget) > 0:
                    volume_total_budget = float(volumes_budget.iloc[0])
                else:
                    volume_total_budget = 0.0
                linha_resumo['_Volume_Budget_Calculo'] = volume_total_budget
                linha_resumo_formatado['_Volume_Budget_Calculo'] = f"{volume_total_budget:,.0f}"
            else:
                linha_resumo['_Volume_Budget_Calculo'] = 0
                linha_resumo_formatado['_Volume_Budget_Calculo'] = "0"
    else:
        # Para Custo Total: apenas somar
        for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
            if col in df_original.columns:
                soma = df_original[col].sum()
                linha_resumo[col] = soma
                sufixo = ""
                if fator_conversao:
                    if fator_conversao == "K (milhares)":
                        sufixo = " K"
                    elif fator_conversao == "M (Milhões)":
                        sufixo = " M"
                linha_resumo_formatado[col] = f"{soma:,.2f}{sufixo}"
    
    # Recalcular Total / Flex Bud
    if 'Custo FP' in linha_resumo and 'Flex BUD' in linha_resumo:
        total_soma = linha_resumo['Custo FP']
        flex_bud_soma = linha_resumo['Flex BUD']
        ratio_resumo = total_soma / flex_bud_soma if flex_bud_soma != 0 and pd.notnull(flex_bud_soma) else 0
        linha_resumo['Total / Flex Bud'] = ratio_resumo
        linha_resumo_formatado['Total / Flex Bud'] = ratio_resumo
    
    return linha_resumo, linha_resumo_formatado

def exibir_caixas_resumo_dinamico(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    """Exibe caixas de texto com valores de resumo usando nomes de colunas dinâmicas (ex: Set/24, Flex set/24, etc.)
    
    Args:
        linha_resumo: Dicionário com valores numéricos (usando nomes de colunas dinâmicas)
        linha_resumo_formatado: Dicionário com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget
    """
    # Obter colunas numéricas (excluindo volumes e colunas auxiliares)
    colunas_auxiliares = ['_Volume_Real_Calculo', '_Volume_Budget_Calculo']
    colunas_numericas = [col for col in linha_resumo.keys() if col not in colunas_auxiliares]
    
    # Ordenar colunas na ordem exata: Jul/25, Flex jul/25 - jul/25, Flex jul/25, Nov/25 - Flex jul/25, nov/25, % nov/25/flex jul/25
    # Detectar primeiro e segundo períodos
    primeiro_periodo = None
    segundo_periodo_maiuscula = None
    segundo_periodo_minuscula = None
    flex_primeiro_menos_primeiro = None
    flex_primeiro = None
    percentual = None
    
    # Primeiro, identificar todas as colunas
    for col in colunas_numericas:
        # Primeiro período: não começa com 'Flex' ou '%', não tem '-', começa com maiúscula
        if not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].isupper():
            primeiro_periodo = col
        # Flex primeiro - primeiro: começa com 'Flex' e tem '-'
        elif col.startswith('Flex') and '-' in col:
            flex_primeiro_menos_primeiro = col
        # Flex primeiro: começa com 'Flex' e não tem '-'
        elif col.startswith('Flex') and '-' not in col:
            flex_primeiro = col
        # Segundo período maiúscula: não começa com 'Flex' ou '%', tem '-', começa com maiúscula (ex: Nov/25 - Flex jul/25)
        elif '-' in col and not col.startswith('%') and not col.startswith('Flex') and len(col) > 0 and col[0].isupper():
            segundo_periodo_maiuscula = col
        # Segundo período minúscula: não começa com 'Flex' ou '%', não tem '-', começa com minúscula (ex: nov/25)
        elif not col.startswith('%') and not col.startswith('Flex') and '-' not in col and len(col) > 0 and col[0].islower():
            segundo_periodo_minuscula = col
        # Percentual: começa com '%'
        elif col.startswith('%'):
            percentual = col
    
    # Criar ordem explícita na ordem correta
    ordem_explicita = []
    
    # 1. Primeiro período (ex: Jul/25)
    if primeiro_periodo:
        ordem_explicita.append(primeiro_periodo)
    
    # 2. Flex primeiro - primeiro (ex: Flex jul/25 - jul/25)
    if flex_primeiro_menos_primeiro:
        ordem_explicita.append(flex_primeiro_menos_primeiro)
    
    # 3. Flex primeiro (ex: Flex jul/25)
    if flex_primeiro:
        ordem_explicita.append(flex_primeiro)
    
    # 4. Segundo período - Flex primeiro (ex: Nov/25 - Flex jul/25)
    if segundo_periodo_maiuscula:
        ordem_explicita.append(segundo_periodo_maiuscula)
    
    # 5. Segundo período minúscula (ex: nov/25)
    if segundo_periodo_minuscula:
        ordem_explicita.append(segundo_periodo_minuscula)
    
    # 6. Percentual (ex: % nov/25/flex jul/25)
    if percentual:
        ordem_explicita.append(percentual)
    
    # Se a ordem explícita não capturou todas as colunas, adicionar as restantes no final
    colunas_restantes = [col for col in colunas_numericas if col not in ordem_explicita]
    ordem_explicita.extend(colunas_restantes)
    
    colunas_ordenadas = ordem_explicita
    
    # Exibir caixas (máximo 6 colunas principais)
    num_colunas = min(len(colunas_ordenadas), 6)
    if num_colunas > 0:
        cols = st.columns(num_colunas, gap="small")
        for idx, col_nome in enumerate(colunas_ordenadas[:num_colunas]):
            with cols[idx]:
                valor_formatado = linha_resumo_formatado.get(col_nome, '-')
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>{col_nome}</strong><br>{valor_formatado}</div>", unsafe_allow_html=True)
    
    # Exibir volumes se solicitado
    if mostrar_volumes:
        volume_real_display = linha_resumo_formatado.get('_Volume_Real_Calculo', '-')
        volume_budget_display = linha_resumo_formatado.get('_Volume_Budget_Calculo', '-')
        
        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>", unsafe_allow_html=True)
        with col_vol2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>", unsafe_allow_html=True)

def exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao, mostrar_volumes=False):
    """Exibe caixas de texto com os valores de resumo (BUD, Flex BUD, Total, etc.) com fonte menor
    
    Args:
        linha_resumo: Dicionário com valores numéricos
        linha_resumo_formatado: Dicionário com valores formatados
        tipo_visualizacao: "CPU (Custo por Unidade)" ou "Custo Total"
        mostrar_volumes: Se True, exibe volumes Real e Budget usados nos cálculos (apenas para resumo geral)
    """
    if mostrar_volumes:
        # Exibir com volumes (resumo geral) - valores principais
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")
        
        with col1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>", unsafe_allow_html=True)
        with col4:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>", unsafe_allow_html=True)
        with col5:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Custo FP', '-')}</div>", unsafe_allow_html=True)
        with col6:
            ratio_valor = linha_resumo.get('Total / Flex Bud', 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                # Usar a função formatar_ratio_com_barra para exibir a barra de percentual
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>", unsafe_allow_html=True)
        
        # Espaçamento entre as caixas de texto e os volumes
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 🔧 ADICIONAR: Exibir volumes abaixo da linha de valores
        # Tentar obter volumes do dicionário formatado primeiro
        volume_real_display = linha_resumo_formatado.get('_Volume_Real_Calculo', None)
        volume_budget_display = linha_resumo_formatado.get('_Volume_Budget_Calculo', None)
        
        # Se os volumes não estiverem formatados, tentar obter do dicionário numérico e formatar
        if volume_real_display is None or volume_real_display == '-':
            if '_Volume_Real_Calculo' in linha_resumo:
                volume_real_valor = linha_resumo['_Volume_Real_Calculo']
                if isinstance(volume_real_valor, (int, float)) and not pd.isna(volume_real_valor) and volume_real_valor != 0:
                    volume_real_display = f"{volume_real_valor:,.0f}"
                else:
                    volume_real_display = '-'
            else:
                volume_real_display = '-'
        
        if volume_budget_display is None or volume_budget_display == '-':
            if '_Volume_Budget_Calculo' in linha_resumo:
                volume_budget_valor = linha_resumo['_Volume_Budget_Calculo']
                if isinstance(volume_budget_valor, (int, float)) and not pd.isna(volume_budget_valor) and volume_budget_valor != 0:
                    volume_budget_display = f"{volume_budget_valor:,.0f}"
                else:
                    volume_budget_display = '-'
            else:
                volume_budget_display = '-'
        
        # Exibir volumes sempre que mostrar_volumes=True (mesmo padrão das caixas acima, com valor na frente na mesma linha)
        col_vol1, col_vol2 = st.columns(2, gap="small")
        with col_vol1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real:</strong> {volume_real_display}</div>", unsafe_allow_html=True)
        with col_vol2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget:</strong> {volume_budget_display}</div>", unsafe_allow_html=True)
    else:
        # Exibir sem volumes (resumos de categorias)
        col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")
        
        with col1:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>BUD</strong><br>{linha_resumo_formatado.get('BUD', '-')}</div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex Bud - BUD</strong><br>{linha_resumo_formatado.get('Flex Bud - BUD', '-')}</div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Flex BUD</strong><br>{linha_resumo_formatado.get('Flex BUD', '-')}</div>", unsafe_allow_html=True)
        with col4:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total - Flex Bud</strong><br>{linha_resumo_formatado.get('Total - Flex Bud', '-')}</div>", unsafe_allow_html=True)
        with col5:
            st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total</strong><br>{linha_resumo_formatado.get('Custo FP', '-')}</div>", unsafe_allow_html=True)
        with col6:
            ratio_valor = linha_resumo.get('Total / Flex Bud', 0)
            if isinstance(ratio_valor, (int, float)) and not pd.isna(ratio_valor):
                # Usar a função formatar_ratio_com_barra para exibir a barra de percentual
                html_barra = formatar_ratio_com_barra(ratio_valor)
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>{html_barra}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='font-size: 0.75rem;'><strong>Total / Flex Bud</strong><br>-</div>", unsafe_allow_html=True)

def formatar_ratio_para_tabela(valor):
    """Formata um valor de ratio (Total/Flex Bud) como percentual com indicador visual para tabelas"""
    if pd.isna(valor) or valor == 0:
        percentual = 0
    else:
        # Converter para percentual
        percentual = valor * 100
    
    # Calcular número de barras: 100% = barra cheia (10 barras)
    if percentual >= 100:
        num_barras = 10  # Barra cheia para 100% ou mais
    else:
        num_barras = int(percentual / 10)  # Proporcional até 100%
    
    # Criar barra visual com gradiente verde->vermelho usando emojis coloridos
    # Usar caracteres Unicode para criar efeito de gradiente
    barras_preenchidas = num_barras
    barras_vazias = 10 - num_barras
    
    # Para valores acima de 100%, mostrar barra cheia
    if percentual >= 100:
        barra = "█" * 10
    else:
        barra = "█" * barras_preenchidas + "░" * barras_vazias
    
    return f"{percentual:.1f}% {barra}"

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


# Função para calcular FLEX de volume comparando dados reais vs budget
def calcular_flex_budget(df_real, df_real_vol, df_budget, df_budget_vol, tipo_viz, tem_ano):
    """
    Calcula FLEX de volume comparando dados reais vs budget.
    
    Regra:
    - Custo Fixo: sensibilidade = 0 (não varia)
    - Custo Variável: sensibilidade = 1 (varia 100% do volume)
    
    Fórmula:
    - Proporção_Volume = Volume_Budget / Volume_Real
    - Variação_% = Proporção_Volume - 1.0
    - FLEX_Fixo = Custo_Fixo_Real × Variação_% × 0 = 0
    - FLEX_Variável = Custo_Variável_Real × Variação_% × 1
    - FLEX_Total = FLEX_Fixo + FLEX_Variável
    
    Para CPU:
    - FLEX_CPU = FLEX_Total / Volume_Real
    
    Retorna DataFrame com colunas: Ano (se tem_ano), Período, FLEX, Budget_Total (valores originais do budget)
    """
    try:
        if df_budget is None or df_real is None:
            return None
        
        # 🔧 CORREÇÃO CRÍTICA: Normalizar períodos em todos os DataFrames ANTES de agrupar
        # Mapear meses para formato capitalizado (primeira letra maiúscula) - MESMA LÓGICA DO NOTEBOOK
        mapeamento_meses = {
            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
        }
        
        def normalizar_periodo(periodo):
            """Normaliza período para formato capitalizado"""
            if pd.isna(periodo):
                return periodo
            periodo_str = str(periodo).strip()
            for mes_min, mes_cap in mapeamento_meses.items():
                if periodo_str.lower() == mes_min.lower():
                    return mes_cap
            return periodo_str  # Retornar original se não for um mês conhecido
        
        # Normalizar períodos em todos os DataFrames
        if 'Período' in df_real.columns:
            df_real = df_real.copy()
            df_real['Período'] = df_real['Período'].apply(normalizar_periodo)
        if 'Custo' in df_real.columns:
            df_real = df_real.copy()
            df_real['Custo'] = df_real['Custo'].apply(_normalizar_rotulo_custo)
        if df_real_vol is not None and 'Período' in df_real_vol.columns:
            df_real_vol = df_real_vol.copy()
            df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
        if 'Período' in df_budget.columns:
            df_budget = df_budget.copy()
            df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
        if 'Custo' in df_budget.columns:
            df_budget = df_budget.copy()
            df_budget['Custo'] = df_budget['Custo'].apply(_normalizar_rotulo_custo)
        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
            df_budget_vol = df_budget_vol.copy()
            df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)
        
        # Agrupar dados reais por período
        if tem_ano:
            # Agrupar por Ano e Período
            if 'Custo' in df_real.columns and 'Custo FP' in df_real.columns:
                real_agrupado = df_real.groupby(['Ano', 'Período', 'Custo'])['Custo FP'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            else:
                return None
            
            # Agrupar budget por período
            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Custo'])['Custo FP'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
            else:
                return None
            
            # 🔧 CORREÇÃO: Normalizar períodos antes do merge para garantir correspondência
            # Normalizar períodos para string e remover espaços extras (já normalizados acima, mas garantir)
            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os períodos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on=['Ano', 'Período'],
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # 🔧 CORREÇÃO CRÍTICA: NÃO filtrar períodos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando não houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas períodos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada período
            flex_data = []
            for _, vol_row in volumes.iterrows():
                ano = vol_row['Ano']
                periodo = vol_row['Período']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporção de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este período
                custos_real = real_agrupado[
                    (real_agrupado['Ano'] == ano) & 
                    (real_agrupado['Período'] == periodo)
                ]
                
                # Obter valores originais do budget para este período
                custos_budget = budget_agrupado[
                    (budget_agrupado['Ano'] == ano) & 
                    (budget_agrupado['Período'] == periodo)
                ]
                
                # 🔧 CORREÇÃO: Se não encontrar budget para este período, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                else:
                    budget_total = custos_budget['Custo FP'].sum()
                    mask_fixo = _mask_custo_fixo(custos_budget['Custo']) if 'Custo' in custos_budget.columns else pd.Series(False, index=custos_budget.index)
                    custo_fixo_budget = custos_budget.loc[mask_fixo, 'Custo FP'].sum()

                # 🔧 CORREÇÃO CRÍTICA (Flex): Não ignorar categorias fora de 'Variável'.
                # Regra: tudo que NÃO é Fixo é flexível (escala com Volume Real/Budget).
                custo_nao_fixo_budget = budget_total - custo_fixo_budget
                
                # 🔧 NOVO: Se não houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Custo FP'].sum()
                
                # NOTA: A conversão de moeda já foi aplicada no df_budget (linha ~2563)
                # Portanto, budget_total e custo_fixo_budget (e o não-fixo derivado) já estão convertidos
                
                # Proporção para flexionar o componente não-fixo
                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)

                if tipo_viz == "CPU (Custo por Unidade)":
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Flex Bud Total (Custo Total)
                    flex_valor = flex_bud_total_custo_total
                    budget_valor = budget_total
                
                flex_data.append({
                    'Ano': ano,
                    'Período': periodo,
                    'FLEX': flex_valor,  # Agora contém Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
        else:
            # Sem coluna Ano: agrupar apenas por Período
            # (Períodos já foram normalizados acima)
            if 'Custo' in df_real.columns and 'Custo FP' in df_real.columns:
                real_agrupado = df_real.groupby(['Período', 'Custo'])['Custo FP'].sum().reset_index()
            else:
                return None
            
            if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                real_vol_agrupado = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
            else:
                return None
            
            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns:
                budget_agrupado = df_budget.groupby(['Período', 'Custo'])['Custo FP'].sum().reset_index()
            else:
                return None
            
            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                budget_vol_agrupado = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
            else:
                return None
            
            # 🔧 CORREÇÃO: Normalizar períodos antes do merge para garantir correspondência
            # Normalizar períodos para string e remover espaços extras (já normalizados acima, mas garantir)
            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
            
            # Fazer merge de volumes (usar outer para ver todos os períodos)
            volumes = pd.merge(
                real_vol_agrupado,
                budget_vol_agrupado,
                on='Período',
                how='outer',  # 🔧 MUDANÇA: usar outer para ver todos os períodos
                suffixes=('_real', '_budget')
            )
            
            # 🔧 CORREÇÃO CRÍTICA: NÃO filtrar períodos sem volume real
            # Queremos mostrar o ano todo, usando Budget quando não houver dados reais
            # Preencher volumes de real ausentes com volumes de budget (para calcular Flex Bud)
            volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
            
            # Preencher volumes de budget ausentes com 0 (para períodos sem budget)
            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
            
            # Filtrar apenas períodos com algum volume (real ou budget)
            volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)].copy()
            
            # Calcular FLEX para cada período
            flex_data = []
            for _, vol_row in volumes.iterrows():
                periodo = vol_row['Período']
                volume_real = vol_row['Volume_real']
                volume_budget = vol_row['Volume_budget']
                
                if volume_real == 0 or pd.isna(volume_real):
                    continue
                
                # Calcular proporção de volume
                proporcao_volume = volume_budget / volume_real if volume_real != 0 else 1.0
                variacao_percentual = proporcao_volume - 1.0
                
                # Obter custos reais para este período
                custos_real = real_agrupado[real_agrupado['Período'] == periodo]
                
                # Obter valores originais do budget para este período
                custos_budget = budget_agrupado[budget_agrupado['Período'] == periodo]
                
                # 🔧 CORREÇÃO: Se não encontrar budget para este período, usar valores zero
                if len(custos_budget) == 0:
                    budget_total = 0
                    custo_fixo_budget = 0
                else:
                    budget_total = custos_budget['Custo FP'].sum()
                    mask_fixo = _mask_custo_fixo(custos_budget['Custo']) if 'Custo' in custos_budget.columns else pd.Series(False, index=custos_budget.index)
                    custo_fixo_budget = custos_budget.loc[mask_fixo, 'Custo FP'].sum()

                # 🔧 CORREÇÃO CRÍTICA (Flex): Não ignorar categorias fora de 'Variável'.
                # Regra: tudo que NÃO é Fixo é flexível (escala com Volume Real/Budget).
                custo_nao_fixo_budget = budget_total - custo_fixo_budget
                
                # 🔧 NOVO: Se não houver custos reais, usar os valores do budget
                if len(custos_real) == 0:
                    real_total = budget_total
                else:
                    real_total = custos_real['Custo FP'].sum()
                
                # NOTA: A conversão de moeda já foi aplicada no df_budget (linha ~2550)
                # Portanto, budget_total e custo_fixo_budget (e o não-fixo derivado) já estão convertidos
                
                # Proporção para flexionar o componente não-fixo
                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)

                if tipo_viz == "CPU (Custo por Unidade)":
                    # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                    flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                    # Budget CPU = Budget_Total / Volume_Budget
                    budget_valor = budget_total / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 0
                else:
                    # Para Custo Total: Flex Bud Total (Custo Total)
                    flex_valor = flex_bud_total_custo_total
                    budget_valor = budget_total
                
                flex_data.append({
                    'Período': periodo,
                    'FLEX': flex_valor,  # Agora contém Flex Bud (Budget + FLEX)
                    'Budget_Total': budget_valor
                })
            
            if len(flex_data) == 0:
                return None
            
            return pd.DataFrame(flex_data)
            
    except Exception as e:
        st.sidebar.warning(f"⚠️ Erro ao calcular FLEX: {e}")
        return None


# Gráfico 1: Soma do Valor por Período
# Cache removido: DataFrames grandes podem causar problemas de hash
def create_period_chart(df_data, coluna, tipo_viz, df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, moeda_simbolo="R$", debug=False, debug_context=""):
    """Cria gráfico de barras por Período com linha pontilhada de FLEX (budget) opcional"""
    try:
        # Detectar tema para adaptar cores (dark/light mode)
        theme_base = st.get_option("theme.base") or "light"
        text_color = "#FAFAFA" if theme_base == "dark" else "#000000"
        
        # Validações iniciais
        if df_data is None or df_data.empty:
            st.warning("⚠️ Dados vazios ou None passados para o gráfico")
            return None
        
        if 'Período' not in df_data.columns:
            st.warning(f"⚠️ Coluna 'Período' não encontrada. Colunas disponíveis: {list(df_data.columns)[:10]}")
            return None

        # 🔧 Padronização (CPU): recomputar a base via custo+volume com merge OUTER.
        # Assim o gráfico por período não depende de Volume já mergeado (que pode ter sido perdido/duplicado).
        if tipo_viz == "CPU (Custo por Unidade)" and df_real_vol is not None:
            try:
                base_custo = df_real_original if df_real_original is not None else df_data
                base_vol = df_real_vol

                try:
                    oficinas_recorte = set(df_data['Oficina'].astype(str).str.strip().dropna().unique().tolist()) if 'Oficina' in df_data.columns else set()
                except Exception:
                    oficinas_recorte = set()
                try:
                    veiculos_recorte = set(df_data['Veículo'].astype(str).str.strip().dropna().unique().tolist()) if 'Veículo' in df_data.columns else None
                except Exception:
                    veiculos_recorte = None
                try:
                    anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist()) if 'Ano' in df_data.columns else None
                except Exception:
                    anos_recorte = None
                try:
                    periodos_recorte = set(df_data['Período'].astype(str).str.strip().dropna().unique().tolist()) if 'Período' in df_data.columns else None
                except Exception:
                    periodos_recorte = None

                def _recortar(df_in: pd.DataFrame | None) -> pd.DataFrame | None:
                    if df_in is None:
                        return None
                    df_out = df_in
                    if oficinas_recorte and 'Oficina' in df_out.columns:
                        df_out = df_out[df_out['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()
                    if veiculos_recorte is not None and 'Veículo' in df_out.columns:
                        df_out = df_out[df_out['Veículo'].astype(str).str.strip().isin(veiculos_recorte)].copy()
                    if periodos_recorte is not None and 'Período' in df_out.columns:
                        df_out = df_out[df_out['Período'].astype(str).str.strip().isin(periodos_recorte)].copy()
                    if anos_recorte is not None and 'Ano' in df_out.columns:
                        ano_num = pd.to_numeric(df_out['Ano'], errors='coerce')
                        df_out = df_out[ano_num.isin(anos_recorte)].copy()
                    return df_out

                base_custo = _recortar(base_custo)
                base_vol = _recortar(base_vol)

                df_cpu_periodo = _cpu_por_chaves_tc(
                    base_custo,
                    base_vol,
                    chaves_preferidas=("Ano", "Período"),
                    coluna_custo="Total",
                    coluna_volume="Volume",
                )
                if df_cpu_periodo is not None and not df_cpu_periodo.empty:
                    df_data = df_cpu_periodo
            except Exception:
                pass

        # ==============================
        # DEBUG: auditoria de Flex/Volume
        # ==============================
        if debug and df_budget is not None and df_budget_vol is not None and df_real_vol is not None:
            try:
                with st.expander(f"🛠️ Debug Flex/Volume {debug_context}".strip(), expanded=False):
                    st.caption("Valida: (1) Flex BUD vs BUD, (2) Volume Real vs Budget, (3) por Oficina. Usa o mesmo recorte do gráfico.")

                    # 🔧 IMPORTANTE: não filtrar volume por "recorte do custo".
                    # Isso pode remover veículos/oficinas que não aparecem no realizado (custo),
                    # mas existem no volume, gerando volume total errado.
                    df_real_vol_dbg = df_real_vol.copy()
                    df_budget_vol_dbg = df_budget_vol.copy()
                    df_budget_dbg = df_budget.copy()

                    # Normalizar Período para evitar mismatch bobo de merge
                    for _df in [df_budget_dbg, df_real_vol_dbg, df_budget_vol_dbg]:
                        if _df is not None and len(_df) > 0 and 'Período' in _df.columns:
                            _df['Período'] = _df['Período'].astype(str).str.strip()

                    tem_ano = 'Ano' in df_budget_dbg.columns and 'Ano' in df_real_vol_dbg.columns and 'Ano' in df_budget_vol_dbg.columns
                    chaves = ['Período']
                    if tem_ano:
                        chaves = ['Ano', 'Período']

                    # Custos Budget por período
                    if 'Custo FP' not in df_budget_dbg.columns:
                        st.warning("Debug: df_budget não tem coluna 'Custo FP'.")
                    else:
                        bud_total = df_budget_dbg.groupby(chaves)['Custo FP'].sum().reset_index().rename(columns={'Custo FP': 'BUD_Total'})
                        bud_fixo = df_budget_dbg[df_budget_dbg.get('Custo', '').astype(str) == 'Fixo'].groupby(chaves)['Custo FP'].sum().reset_index().rename(columns={'Custo FP': 'BUD_Fixo'})
                        df_dbg = bud_total.merge(bud_fixo, on=chaves, how='left')
                        df_dbg['BUD_Fixo'] = df_dbg['BUD_Fixo'].fillna(0.0)
                        df_dbg['BUD_NaoFixo'] = df_dbg['BUD_Total'] - df_dbg['BUD_Fixo']

                        # Volumes por período
                        vol_real = df_real_vol_dbg.groupby(chaves)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Real'})
                        vol_bud = df_budget_vol_dbg.groupby(chaves)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Budget'})
                        df_dbg = df_dbg.merge(vol_real, on=chaves, how='left').merge(vol_bud, on=chaves, how='left')
                        df_dbg['Volume_Real'] = df_dbg['Volume_Real'].fillna(0.0)
                        df_dbg['Volume_Budget'] = df_dbg['Volume_Budget'].fillna(0.0)

                        df_dbg['Proporcao_Real_Bud'] = (df_dbg['Volume_Real'] / df_dbg['Volume_Budget'].replace(0, 1)).fillna(1.0)
                        df_dbg['Flex_BUD_CustoTotal'] = df_dbg['BUD_Fixo'] + (df_dbg['BUD_NaoFixo'] * df_dbg['Proporcao_Real_Bud'])
                        df_dbg['Flex_minus_BUD'] = df_dbg['Flex_BUD_CustoTotal'] - df_dbg['BUD_Total']

                        # Totais
                        st.markdown("**Totais do recorte (somatório por período)**")
                        total_real_recorte = None
                        cpu_real_recorte = None
                        try:
                            if df_data is not None and len(df_data) > 0 and 'Custo FP' in df_data.columns:
                                total_real_recorte = float(pd.to_numeric(df_data['Custo FP'], errors='coerce').fillna(0).sum())
                                vol_real_recorte = float(pd.to_numeric(df_dbg['Volume_Real'], errors='coerce').fillna(0).sum())
                                cpu_real_recorte = (total_real_recorte / vol_real_recorte) if vol_real_recorte not in (0, None) else 0.0
                        except Exception:
                            total_real_recorte = None
                            cpu_real_recorte = None
                        st.write({
                            'BUD_Total': float(df_dbg['BUD_Total'].sum()),
                            'Flex_BUD_CustoTotal': float(df_dbg['Flex_BUD_CustoTotal'].sum()),
                            'Dif_Flex_minus_BUD': float(df_dbg['Flex_minus_BUD'].sum()),
                            'Volume_Real': float(df_dbg['Volume_Real'].sum()),
                            'Volume_Budget': float(df_dbg['Volume_Budget'].sum()),
                            'Real_Total': total_real_recorte,
                            'Real_CPU_(Total/VolReal)': cpu_real_recorte,
                        })

                        # Mostrar por período (evidencia distribuição diferente)
                        st.markdown("**Por período (BUD vs Flex e volumes)**")
                        cols_show = chaves + ['BUD_Total', 'BUD_Fixo', 'BUD_NaoFixo', 'Volume_Real', 'Volume_Budget', 'Proporcao_Real_Bud', 'Flex_BUD_CustoTotal', 'Flex_minus_BUD']
                        st.dataframe(df_dbg[cols_show].sort_values(chaves), width="stretch")

                        # Por oficina (se existir nas bases)
                        if 'Oficina' in df_budget_dbg.columns and 'Oficina' in df_real_vol_dbg.columns and 'Oficina' in df_budget_vol_dbg.columns:
                            chaves_of = chaves + ['Oficina']
                            bud_total_of = df_budget_dbg.groupby(chaves_of)['Custo FP'].sum().reset_index().rename(columns={'Custo FP': 'BUD_Total'})
                            bud_fixo_of = df_budget_dbg[df_budget_dbg.get('Custo', '').astype(str) == 'Fixo'].groupby(chaves_of)['Custo FP'].sum().reset_index().rename(columns={'Custo FP': 'BUD_Fixo'})
                            dbg_of = bud_total_of.merge(bud_fixo_of, on=chaves_of, how='left')
                            dbg_of['BUD_Fixo'] = dbg_of['BUD_Fixo'].fillna(0.0)
                            dbg_of['BUD_NaoFixo'] = dbg_of['BUD_Total'] - dbg_of['BUD_Fixo']
                            vol_real_of = df_real_vol_dbg.groupby(chaves_of)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Real'})
                            vol_bud_of = df_budget_vol_dbg.groupby(chaves_of)['Volume'].sum().reset_index().rename(columns={'Volume': 'Volume_Budget'})
                            dbg_of = dbg_of.merge(vol_real_of, on=chaves_of, how='left').merge(vol_bud_of, on=chaves_of, how='left')
                            dbg_of['Volume_Real'] = dbg_of['Volume_Real'].fillna(0.0)
                            dbg_of['Volume_Budget'] = dbg_of['Volume_Budget'].fillna(0.0)
                            dbg_of['Proporcao_Real_Bud'] = (dbg_of['Volume_Real'] / dbg_of['Volume_Budget'].replace(0, 1)).fillna(1.0)
                            dbg_of['Flex_BUD_CustoTotal'] = dbg_of['BUD_Fixo'] + (dbg_of['BUD_NaoFixo'] * dbg_of['Proporcao_Real_Bud'])
                            dbg_of['Flex_minus_BUD'] = dbg_of['Flex_BUD_CustoTotal'] - dbg_of['BUD_Total']

                            # Agregar por oficina (somando períodos) e ordenar pelos maiores gaps
                            agg_cols = ['BUD_Total', 'Flex_BUD_CustoTotal', 'Flex_minus_BUD', 'Volume_Real', 'Volume_Budget']
                            dbg_of_tot = dbg_of.groupby('Oficina')[agg_cols].sum().reset_index()
                            dbg_of_tot = dbg_of_tot.sort_values('Flex_minus_BUD', key=lambda s: s.abs(), ascending=False)
                            st.markdown("**Por oficina (maiores diferenças)**")
                            st.dataframe(dbg_of_tot.head(50), width="stretch")

                        # Diagnóstico: quais categorias estão vindo no Budget
                        if 'Custo' in df_budget_dbg.columns:
                            st.markdown("**Budget por categoria Custo (para ver 'Outros')**")
                            df_cat = df_budget_dbg.groupby('Custo')['Custo FP'].sum().reset_index().sort_values('Custo FP', ascending=False)
                            st.dataframe(df_cat, width="stretch")
            except Exception as _e:
                st.warning(f"Debug Flex falhou: {_e}")
        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns):
                st.warning(f"⚠️ Coluna necessária não encontrada: {coluna}")
                st.warning(f"⚠️ Colunas disponíveis: {list(df_data.columns)[:10]}")
                return None

        # Verificar se há coluna Ano - sempre mostrar ano junto com período quando existir
        tem_ano = 'Ano' in df_data.columns
        
        if tem_ano:
            # Agrupar por Ano e Período (sempre que houver coluna Ano)
            # IMPORTANTE: Sempre agrupar por Ano e Período para garantir consistência
            # independentemente de "Todos" estar selecionado ou um ano específico
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                # Verificar se temos Total e Volume, ou se precisamos usar CPU existente
                if 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA: Agrupar por Ano e Período, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessárias para o agrupamento
                    colunas_agrupamento = ['Ano', 'Período']
                    chart_data = df_data[colunas_agrupamento + ['Custo FP', 'Volume']].groupby(colunas_agrupamento).agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lógica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Custo FP'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU já existe mas temos Total e Volume, recalcular
                    colunas_agrupamento = ['Ano', 'Período']
                    chart_data = df_data[colunas_agrupamento + ['Custo FP', 'Volume']].groupby(colunas_agrupamento).agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Custo FP'] / chart_data['Volume'],
                        0
                    )
                else:
                    # Fallback: agrupar apenas por Ano e Período
                    chart_data = df_data.groupby(['Ano', 'Período'])[coluna].sum().reset_index()
            else:
                # Para Custo Total, também agrupar por Ano e Período para garantir consistência
                # Otimizar: usar apenas as colunas necessárias
                chart_data = df_data[['Ano', 'Período', coluna]].groupby(['Ano', 'Período'])[coluna].sum().reset_index()
            
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
            if tipo_viz == "CPU (Custo por Unidade)":
                # IMPORTANTE: Sempre recalcular CPU a partir de Total e Volume agregados
                if 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA: Agrupar por Período, somar Total e Volume, calcular CPU
                    # Otimizar: usar apenas as colunas necessárias
                    chart_data = df_data[['Período', 'Custo FP', 'Volume']].groupby('Período').agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (mesma lógica da tabela) - VETORIZADO
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Custo FP'] / chart_data['Volume'],
                        0
                    )
                elif 'CPU' in df_data.columns and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    # Se CPU já existe mas temos Total e Volume, recalcular
                    # Otimizar: usar apenas as colunas necessárias
                    chart_data = df_data[['Período', 'Custo FP', 'Volume']].groupby('Período').agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    chart_data[coluna] = np.where(
                        (chart_data['Volume'].notna()) & (chart_data['Volume'] != 0),
                        chart_data['Custo FP'] / chart_data['Volume'],
                        0
                    )
                else:
                    # Fallback: agrupar apenas por Período
                    chart_data = df_data[['Período', coluna]].groupby('Período')[coluna].sum().reset_index()
            else:
                # Otimizar: usar apenas as colunas necessárias
                chart_data = df_data[['Período', coluna]].groupby('Período')[coluna].sum().reset_index()
            chart_data = ordenar_por_mes(chart_data, 'Período')
            ordem_periodos = chart_data['Período'].tolist()
            coluna_periodo_grafico = 'Período'

        # Definir título do eixo Y baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Período"
        else:
            titulo_y = f"Soma do Valor ({moeda_simbolo})"
            titulo_grafico = "Soma do Valor por Período"

        # Garantir que todos os períodos do Budget apareçam (com realizado = 0)
        if df_budget is not None and not df_budget.empty and 'Período' in df_budget.columns:
            # Guardar períodos reais antes do reindex
            if tem_ano:
                periodos_reais_set = set(chart_data[['Ano', 'Período']].apply(tuple, axis=1))
            else:
                periodos_reais_set = set(chart_data['Período'].tolist())

            if tem_ano and 'Ano' in df_budget.columns:
                periodos_budget = df_budget[['Ano', 'Período']].dropna().drop_duplicates()
                index_full = pd.MultiIndex.from_frame(periodos_budget)
                chart_data = chart_data.set_index(['Ano', 'Período']).reindex(index_full).reset_index()
                # Zerar realizado quando não há dado real
                mask_real = chart_data[['Ano', 'Período']].apply(tuple, axis=1).isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan
            else:
                periodos_budget = df_budget['Período'].dropna().drop_duplicates().tolist()
                chart_data = chart_data.set_index('Período').reindex(periodos_budget).reset_index()
                mask_real = chart_data['Período'].isin(periodos_reais_set)
                if coluna in chart_data.columns:
                    chart_data.loc[~mask_real, coluna] = np.nan

            # Preencher colunas numéricas com zero (sem usar budget)
            colunas_zero = [col for col in chart_data.columns if pd.api.types.is_numeric_dtype(chart_data[col]) and col != coluna]
            for col in colunas_zero:
                chart_data[col] = chart_data[col].fillna(0)

            # Reordenar após completar períodos
            chart_data = ordenar_por_mes(chart_data, 'Período')
            if tem_ano:
                chart_data['Período_Completo'] = chart_data['Período'].astype(str) + ' ' + chart_data['Ano'].astype(str)
                ordem_periodos = chart_data['Período_Completo'].tolist()
                coluna_periodo_grafico = 'Período_Completo'
            else:
                ordem_periodos = chart_data['Período'].tolist()
                coluna_periodo_grafico = 'Período'

        # Validar se chart_data tem dados após agrupamento e filtros
        if chart_data is None or chart_data.empty:
            st.warning("⚠️ Nenhum dado após agrupamento. Verifique os filtros aplicados.")
            return None
            
        # Verificar se a coluna tem valores válidos
        if coluna not in chart_data.columns:
            st.warning(f"⚠️ Coluna '{coluna}' não encontrada após agrupamento. Colunas disponíveis: {list(chart_data.columns)}")
            return None
            
        # Se houver volume real, zerar/ocultar realizado em períodos sem volume
        if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Período' in df_real_vol.columns:
            if tem_ano and 'Ano' in df_real_vol.columns and 'Ano' in chart_data.columns:
                vol_agr = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on=['Ano', 'Período'], how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])
            else:
                vol_agr = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
                chart_data = chart_data.merge(vol_agr, on='Período', how='left')
                if 'Volume' in chart_data.columns:
                    chart_data.loc[
                        chart_data['Volume'].isna() | (chart_data['Volume'] == 0),
                        coluna
                    ] = np.nan
                    chart_data = chart_data.drop(columns=['Volume'])

        # Não cortar meses futuros aqui.
        # O gráfico deve refletir os dados disponíveis (ex.: Budget/Forecast pode ter o ano completo).

        # Garantir que os valores sejam numéricos (preservar NaN para não desenhar barras)
        chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce')
            
        # Verificar se há valores não-nulos (apenas para Custo Total, CPU já filtra zeros)
        if tipo_viz != "CPU (Custo por Unidade)":
            valores_validos = chart_data[coluna].notna() & (chart_data[coluna] != 0)
            if not valores_validos.any():
                # Não bloquear, apenas avisar - pode haver valores muito pequenos após conversão
                st.info(f"ℹ️ Todos os valores na coluna '{coluna}' são zero após agrupamento. Mostrando gráfico mesmo assim.")
        
        # Verificar se chart_data está vazio
        if chart_data is None or chart_data.empty or len(chart_data) == 0:
            st.warning("⚠️ Nenhum dado disponível após agrupamento e filtros.")
            return None

        # Usar gradiente baseado no valor da coluna (como na figura 1)
        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
            color=alt.Color(
                f'{coluna}:Q',
                title=coluna,
                scale=alt.Scale(scheme='blues'),
                legend=alt.Legend(title=coluna, orient='right', titleFontSize=10, labelFontSize=9)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip(
                    f'{coluna}:Q',
                    title=coluna,
                    format=',.2f' if tipo_viz == "CPU (Custo por Unidade)"
                    else ',.2f'
                )
            ]
        ).properties(
            height=altura_grafico,
            width='container'
        )

        # Adicionar rótulos com valores nas barras
        formato_rotulo = (
            ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
        )
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
        ).transform_filter(
            (alt.datum[coluna] != None) & (alt.datum[coluna] != 0)
        )

        # Processar dados de budget e calcular FLEX se fornecidos
        linha_budget = None
        budget_data = None  # Inicializar para uso no gráfico de delta
        # IMPORTANTE: No modo CPU, df_data pode não ter a coluna 'Custo' necessária para calcular FLEX
        # Usar df_real_original se disponível, caso contrário usar df_data
        df_real_para_flex = df_real_original if df_real_original is not None else df_data
        
        # 🔧 CORREÇÃO: Verificar se os dados necessários estão disponíveis
        # Verificar se df_budget existe e tem a coluna Período
        tem_budget = df_budget is not None and not df_budget.empty and 'Período' in df_budget.columns
        # Verificar se df_real_vol existe e tem a coluna Volume
        tem_real_vol = df_real_vol is not None and not df_real_vol.empty and 'Volume' in df_real_vol.columns
        # Verificar se df_budget_vol existe (pode ser None, mas se existir deve ter Volume)
        tem_budget_vol = df_budget_vol is not None and not df_budget_vol.empty and 'Volume' in df_budget_vol.columns
        
        dados_budget_disponiveis = tem_budget and tem_real_vol
        
        if dados_budget_disponiveis:
            # Verificar se temos dados com coluna 'Custo' para calcular FLEX
            if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                try:
                    def _normalizar_custo_label(valor):
                        if pd.isna(valor):
                            return valor
                        txt = str(valor).strip()
                        txt_sem_acento = unicodedata.normalize('NFKD', txt).encode('ascii', 'ignore').decode('ascii')
                        txt_norm = txt_sem_acento.strip().lower()
                        if txt_norm == 'fixo':
                            return 'Fixo'
                        if txt_norm == 'variavel':
                            return 'Variável'
                        return txt

                    # 🔧 CORREÇÃO: Usar a MESMA lógica do gráfico de Oficina (que funciona!)
                    # Calcular Flex Bud diretamente em vez de usar calcular_flex_budget
                    # Normalizar períodos ANTES de agrupar
                    mapeamento_meses = {
                        'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                        'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                        'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                        'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                    }
                    
                    def normalizar_periodo(periodo):
                        """Normaliza período para formato capitalizado"""
                        if pd.isna(periodo):
                            return periodo
                        periodo_str = str(periodo).strip()
                        for mes_min, mes_cap in mapeamento_meses.items():
                            if periodo_str.lower() == mes_min.lower():
                                return mes_cap
                        return periodo_str
                    
                    # Normalizar períodos em todos os DataFrames
                    if 'Período' in df_real_para_flex.columns:
                        df_real_para_flex = df_real_para_flex.copy()
                        df_real_para_flex['Período'] = df_real_para_flex['Período'].apply(normalizar_periodo)
                    if 'Custo' in df_real_para_flex.columns:
                        df_real_para_flex['Custo'] = df_real_para_flex['Custo'].apply(_normalizar_custo_label)
                    if df_real_vol is not None and 'Período' in df_real_vol.columns:
                        df_real_vol = df_real_vol.copy()
                        df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
                    if 'Período' in df_budget.columns:
                        df_budget = df_budget.copy()
                        df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
                    if 'Custo' in df_budget.columns:
                        df_budget['Custo'] = df_budget['Custo'].apply(_normalizar_custo_label)
                    if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
                        df_budget_vol = df_budget_vol.copy()
                        df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)

                    # 🔧 IMPORTANTE: não filtrar volume por "recorte do custo".
                    # O volume pode conter veículos/oficinas sem custo realizado; cortar aqui
                    # gera volumes totais incorretos e distorce o Flex.
                    
                    # Agrupar dados reais por Período (mesma lógica do gráfico de Oficina)
                    if tem_ano:
                        if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Custo'])['Custo FP'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Custo'])['Custo FP'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    else:
                        if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                            real_agrupado = df_real_para_flex.groupby(['Período', 'Custo'])['Custo FP'].sum().reset_index()
                        else:
                            real_agrupado = None
                        
                        if df_real_vol is not None and 'Volume' in df_real_vol.columns:
                            real_vol_agrupado = df_real_vol.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            real_vol_agrupado = None
                        
                        if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns:
                            budget_agrupado = df_budget.groupby(['Período', 'Custo'])['Custo FP'].sum().reset_index()
                        else:
                            budget_agrupado = None
                        
                        if df_budget_vol is not None and 'Volume' in df_budget_vol.columns:
                            budget_vol_agrupado = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            budget_vol_agrupado = None
                    
                    # Verificar se temos todos os dados necessários
                    if (real_agrupado is None or real_vol_agrupado is None or 
                        budget_agrupado is None or budget_vol_agrupado is None):
                        flex_data = None
                    else:
                        # Normalizar períodos nos DataFrames agrupados antes do merge
                        if tem_ano:
                            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                        else:
                            real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                            budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                            real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                            budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                        
                        # Fazer merge de volumes por Período
                        if tem_ano:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on=['Ano', 'Período'],
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        else:
                            volumes = pd.merge(
                                real_vol_agrupado,
                                budget_vol_agrupado,
                                on='Período',
                                how='left',
                                suffixes=('_real', '_budget')
                            )
                            volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                        
                        # Calcular FLEX para cada Período (mesma lógica do gráfico de Oficina)
                        flex_data = []
                        for _, vol_row in volumes.iterrows():
                            if tem_ano:
                                ano = vol_row['Ano']
                                periodo = vol_row['Período']
                            else:
                                periodo = vol_row['Período']
                            
                            volume_real = vol_row['Volume_real']
                            volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                            
                            if volume_real == 0 or pd.isna(volume_real):
                                continue
                            
                            # Obter custos reais para este Período
                            if tem_ano:
                                custos_real = real_agrupado[
                                    (real_agrupado['Ano'] == ano) & 
                                    (real_agrupado['Período'] == periodo)
                                ]
                                custos_budget = budget_agrupado[
                                    (budget_agrupado['Ano'] == ano) & 
                                    (budget_agrupado['Período'] == periodo)
                                ]
                            else:
                                custos_real = real_agrupado[real_agrupado['Período'] == periodo]
                                custos_budget = budget_agrupado[budget_agrupado['Período'] == periodo]
                            
                            # Se não houver dados de budget para este período, usar zeros
                            if len(custos_budget) == 0:
                                budget_total = 0
                                custo_fixo_budget = 0
                            else:
                                budget_total = custos_budget['Custo FP'].sum()
                                custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Custo FP'].sum()

                            # 🔧 CORREÇÃO CRÍTICA (Flex): tudo que NÃO é Fixo é flexível
                            custo_nao_fixo_budget = budget_total - custo_fixo_budget
                            
                            # Calcular Flex Bud (mesma lógica do gráfico de Oficina)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)
                                # Flex Bud CPU = Flex Bud (Custo Total) / Volume Real
                                flex_valor = flex_bud_total_custo_total / volume_real if volume_real != 0 and pd.notnull(volume_real) else 0
                            else:
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_valor = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)
                            
                            # Adicionar ao flex_data
                            if tem_ano:
                                flex_data.append({
                                    'Ano': ano,
                                    'Período': periodo,
                                    'FLEX': flex_valor
                                })
                            else:
                                flex_data.append({
                                    'Período': periodo,
                                    'FLEX': flex_valor
                                })
                        
                        if len(flex_data) == 0:
                            flex_data = None
                        else:
                            flex_data = pd.DataFrame(flex_data)
                    
                    if flex_data is None:
                        budget_data = None
                    
                    if flex_data is not None and len(flex_data) > 0:
                        # Renomear coluna FLEX para o nome da coluna do gráfico
                        flex_data.rename(columns={'FLEX': coluna}, inplace=True)
                        
                        # 🔧 CORREÇÃO CRÍTICA: Fazer merge com chart_data para garantir correspondência de períodos
                        # Isso garante que budget_data tenha os mesmos períodos que chart_data
                        if tem_ano:
                            # Criar coluna combinada para o rótulo do gráfico no flex_data
                            flex_data['Período_Completo'] = flex_data['Período'].astype(str) + ' ' + flex_data['Ano'].astype(str)
                            # Ordenar por ano e mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            
                            # Fazer merge com chart_data para garantir correspondência
                            # Usar left join para manter todos os períodos do chart_data
                            budget_data = chart_data[['Período_Completo']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['Período_Completo', coluna]],
                                on='Período_Completo',
                                how='left'
                            )
                        else:
                            # Ordenar por mês
                            flex_data = ordenar_por_mes(flex_data, 'Período')
                            
                            # Fazer merge com chart_data para garantir correspondência
                            # Usar left join para manter todos os períodos do chart_data
                            budget_data = chart_data[['Período']].copy()
                            budget_data = budget_data.merge(
                                flex_data[['Período', coluna]],
                                on='Período',
                                how='left'
                            )
                        
                        # Preencher valores NaN com 0 (períodos sem Flex Bud)
                        budget_data[coluna] = budget_data[coluna].fillna(0)
                        
                        # Criar linha pontilhada se budget_data tiver dados
                        # IMPORTANTE: Criar mesmo que alguns valores sejam zero, desde que tenha dados
                        if len(budget_data) > 0:
                            # Determinar campo do eixo X baseado em tem_ano
                            campo_x = 'Período_Completo' if tem_ano else 'Período'
                            
                            # Criar linha tracejada de Flex Bud usando EXATAMENTE o mesmo eixo X das barras
                            # Usar o mesmo campo e sort garante que compartilhem o mesmo eixo X
                            # Adicionar coluna de legenda para identificar a linha
                            budget_data_legenda = budget_data.copy()
                            budget_data_legenda['Tipo'] = 'Flex Bud'
                            
                            linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                strokeDash=[10, 5],
                                strokeWidth=1.5,
                                opacity=0.8
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='Período',
                                    sort=ordem_periodos,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                color=alt.Color(
                                    'Tipo:N',
                                    title='Legenda',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                    legend=alt.Legend(
                                        title='Legenda', 
                                        orient='bottom', 
                                        titleFontSize=10, 
                                        labelFontSize=9,
                                        titleAnchor='middle',
                                        direction='horizontal',
                                        symbolType='square'
                                    )
                                ),
                                strokeDash=alt.StrokeDash(
                                    'Tipo:N',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=[[0], [10, 5]]),
                                    legend=None
                                ),
                                tooltip=[
                                    alt.Tooltip(f'{campo_x}:N', title='Período'),
                                    alt.Tooltip('Tipo:N', title='Tipo'),
                                    alt.Tooltip(
                                        f'{coluna}:Q',
                                        title='Flex Bud',
                                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                    )
                                ]
                            )
                            
                            # Adicionar bolinhas nos pontos da linha
                            pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                size=80,
                                opacity=0.9
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='Período',
                                    sort=ordem_periodos,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y,
                                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                                ),
                                color=alt.Color(
                                    'Tipo:N',
                                    title='Legenda',
                                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                    legend=None
                                ),
                                tooltip=[
                                    alt.Tooltip(f'{campo_x}:N', title='Período'),
                                    alt.Tooltip('Tipo:N', title='Tipo'),
                                    alt.Tooltip(
                                        f'{coluna}:Q',
                                        title='Flex Bud',
                                        format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                    )
                                ]
                            )
                            
                            # Adicionar rótulos de texto na linha pontilhada
                            formato_rotulo_budget = ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                            rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                align='center',
                                baseline='bottom',
                                dy=-15,
                                color='#FF6B35',
                                fontSize=9,
                                fontWeight='bold'
                            ).encode(
                                x=alt.X(
                                    f'{campo_x}:N',
                                    title='Período',
                                    sort=ordem_periodos
                                ),
                                y=alt.Y(
                                    f'{coluna}:Q',
                                    title=titulo_y
                                ),
                                text=alt.Text(f'{coluna}:Q', format=formato_rotulo_budget)
                            )
                            
                            # Combinar linha, pontos e rótulos
                            linha_budget = linha_budget + pontos_budget + rotulos_budget
                        else:
                            # Se budget_data não tem valores não-zero, não criar linha
                            linha_budget = None
                            budget_data = None
                    else:
                        # Se budget_data foi criado mas está vazio, definir como None
                        budget_data = None
                except Exception as e:
                    budget_data = None
                    linha_budget = None
            else:
                budget_data = None

        # Criar gráfico de delta (Real - Flex Bud) se budget_data estiver disponível
        # IMPORTANTE: No modo CPU, garantir que budget_data seja usado mesmo se estiver vazio
        grafico_delta = None
        if budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Flex Bud - Real
                # Fazer merge dos dados de Real e Flex Bud para calcular delta
                delta_data = chart_data.copy()
                
                # Determinar campo do eixo X baseado em tem_ano
                campo_x_delta = 'Período_Completo' if tem_ano else 'Período'
                
                # Fazer merge com budget_data para obter valores de Flex Bud
                # Renomear coluna antes do merge para evitar conflito
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                
                if tem_ano:
                    # Garantir que budget_data_merge tenha a coluna Período_Completo
                    # budget_data já foi criado com Período_Completo no merge anterior
                    if campo_x_delta not in budget_data_merge.columns:
                        # Se não tiver, criar a partir de Período e Ano
                        if 'Período' in budget_data_merge.columns and 'Ano' in budget_data_merge.columns:
                            budget_data_merge[campo_x_delta] = budget_data_merge['Período'].astype(str) + ' ' + budget_data_merge['Ano'].astype(str)
                    
                    # Garantir que delta_data também tenha Período_Completo
                    if campo_x_delta not in delta_data.columns:
                        delta_data[campo_x_delta] = delta_data['Período'].astype(str) + ' ' + delta_data['Ano'].astype(str)
                    
                    delta_data = delta_data.merge(
                        budget_data_merge[[campo_x_delta, f'{coluna}_FlexBud']],
                        on=campo_x_delta,
                        how='left'
                    )
                else:
                    delta_data = delta_data.merge(
                        budget_data_merge[['Período', f'{coluna}_FlexBud']],
                        on='Período',
                        how='left'
                    )
                
                # Calcular delta: Real - Flex Bud
                coluna_real = coluna  # A coluna original já é o Real
                coluna_flex = f'{coluna}_FlexBud'
                # Preencher valores NaN com 0 antes de calcular delta
                delta_data[coluna_flex] = delta_data[coluna_flex].fillna(0)
                delta_data['Delta'] = delta_data[coluna_real].fillna(0) - delta_data[coluna_flex].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domínio simétrico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Criar gráfico de barras para delta (mais baixo)
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20  # Barras mais finas
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos,
                        axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)  # Sem linha, ticks ou labels no eixo X
                    ),
                    y=alt.Y(
                        'Delta:Q',
                        title='Delta (Real - Flex Bud)',
                        axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                    ),
                    color=alt.Color(
                        'Delta:Q',
                        title='Delta',
                        scale=alt.Scale(
                            domain=[delta_min, 0, delta_max],
                            range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None  # Sem legenda para evitar duplicação - o gráfico principal já tem sua legenda
                    ),
                    tooltip=[
                        alt.Tooltip(f'{campo_x_delta}:N', title='Período'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                        alt.Tooltip(f'{coluna_real}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna_flex}:Q', title='Flex Bud', format=',.2f')
                    ]
                ).properties(
                    height=38  # Gráfico mais baixo/fino
                )
                
                # Adicionar rótulos de dados no gráfico de delta
                # Posicionar acima para valores positivos e abaixo para negativos
                # Usar a mesma cor das barras (verde para negativo, vermelho para positivo)
                rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Delta:Q', title=''),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                    align='center',
                    baseline='top',
                    dy=12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X(
                        f'{campo_x_delta}:N',
                        title='',
                        sort=ordem_periodos
                    ),
                    y=alt.Y('Delta:Q', title=''),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                # Combinar gráfico de delta com rótulos
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                grafico_delta = None
        
        # Combinar gráfico de barras com linha de budget se disponível
        if linha_budget is not None:
            # Criar gráfico principal com barras, rótulos e linha
            grafico_principal = alt.layer(
                grafico_barras,
                rotulos,
                linha_budget
            ).resolve_scale(
                x='shared',
                y='shared'
            )
            
            # Se temos gráfico de delta, combinar verticalmente (delta em cima)
            if grafico_delta is not None:
                # Combinar gráficos verticalmente compartilhando eixo X
                # Delta fica em cima (primeiro), gráfico principal embaixo
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_principal
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os gráficos
                )
            else:
                grafico_final = grafico_principal
        else:
            # Se não há linha de budget, mas temos gráfico de delta, combinar com gráfico de barras
            if grafico_delta is not None:
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_barras + rotulos
                ).resolve_scale(
                    x='shared'  # Compartilhar eixo X entre os gráficos
                )
            else:
                grafico_final = grafico_barras + rotulos
        
        return grafico_final
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


# Gráfico 2: Volume por Período
@st.cache_data(ttl=900, max_entries=2)
def create_volume_chart(df_data, df_budget_vol=None):
    """Cria gráfico de barras de Volume por Período com linha pontilhada de volume do Budget opcional"""
    try:
        altura_grafico = 260
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

        n_periodos = len(ordem_periodos) if 'ordem_periodos' in locals() else 0
        altura_grafico = min(520, max(260, 18 * n_periodos + 120)) if n_periodos else 260

        # Usar gradiente verde baseado no valor do Volume (como no gráfico Volume por Veículo)
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                f'{coluna_periodo_grafico}:N',
                title='Período',
                sort=ordem_periodos,
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume Total', axis=alt.Axis(grid=False, domain=True, ticks=True)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens'),
                legend=alt.Legend(title='Volume', orient='right', titleFontSize=10, labelFontSize=9)
            ),
            tooltip=[
                alt.Tooltip(f'{coluna_periodo_grafico}:N', title='Período'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=altura_grafico,
            width='container'
            # Título removido para evitar duplicação com st.subheader
        )

        # Adicionar rótulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )

        # Processar dados de volume do budget se fornecidos
        linha_budget_vol = None
        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
            try:
                # Processar volume do budget seguindo a mesma lógica dos dados principais
                tem_ano_budget_vol = 'Ano' in df_budget_vol.columns
                
                if tem_ano_budget_vol:
                    # Agrupar por Ano e Período (mesma lógica dos dados principais)
                    budget_vol_data = df_budget_vol.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                    
                    # Criar coluna combinada para o rótulo do gráfico
                    budget_vol_data['Período_Completo'] = budget_vol_data['Período'].astype(str) + ' ' + budget_vol_data['Ano'].astype(str)
                    # Ordenar por ano e mês
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'Período')
                    # Filtrar apenas períodos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['Período_Completo'].isin(ordem_periodos)].copy()
                else:
                    # Sem coluna Ano: agrupar apenas por Período
                    budget_vol_data = df_budget_vol.groupby('Período')['Volume'].sum().reset_index()
                    # Ordenar por mês
                    budget_vol_data = ordenar_por_mes(budget_vol_data, 'Período')
                    # Filtrar apenas períodos que existem no chart_data
                    budget_vol_data = budget_vol_data[budget_vol_data['Período'].isin(ordem_periodos)].copy()
                
                if len(budget_vol_data) > 0:
                    # Determinar campo do eixo X baseado em tem_ano
                    campo_x = 'Período_Completo' if tem_ano else 'Período'
                    
                    # Adicionar coluna de legenda
                    budget_vol_data_legenda = budget_vol_data.copy()
                    budget_vol_data_legenda['Tipo'] = 'Volume Budget'
                    
                    # Criar linha tracejada de volume do budget
                    linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                        strokeDash=[10, 5],
                        strokeWidth=1.5,
                        color='#FF6B35',
                        opacity=0.8
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='Período',
                            sort=ordem_periodos,
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total',
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        color=alt.Color(
                            'Tipo:N',
                            title='Legenda',
                            scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                            legend=alt.Legend(title='Legenda', orient='right', titleFontSize=10, labelFontSize=9)
                        ),
                        strokeDash=alt.StrokeDash(
                            'Tipo:N',
                            scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip(f'{campo_x}:N', title='Período'),
                            alt.Tooltip('Tipo:N', title='Tipo'),
                            alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                        ]
                    )
                    
                    # Adicionar bolinhas nos pontos da linha
                    pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                        size=80,
                        color='#FF6B35',
                        opacity=0.9
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='Período',
                            sort=ordem_periodos,
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total',
                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                        ),
                        color=alt.Color(
                            'Tipo:N',
                            title='Legenda',
                            scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip(f'{campo_x}:N', title='Período'),
                            alt.Tooltip('Tipo:N', title='Tipo'),
                            alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                        ]
                    )
                    
                    # Adicionar rótulos de texto na linha pontilhada
                    rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                        align='center',
                        baseline='bottom',
                        dy=-15,
                        color='#FF6B35',
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X(
                            f'{campo_x}:N',
                            title='Período',
                            sort=ordem_periodos
                        ),
                        y=alt.Y(
                            'Volume:Q',
                            title='Volume Total'
                        ),
                        text=alt.Text('Volume:Q', format=',.0f')
                    )
                    
                    # Combinar linha, pontos e rótulos
                    linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception as e:
                st.sidebar.warning(f"⚠️ Erro ao processar dados de volume do budget: {e}")

        # Combinar gráfico de barras com linha de budget se disponível
        if linha_budget_vol is not None:
            return alt.layer(
                grafico_barras,
                rotulos,
                linha_budget_vol
            ).resolve_scale(
                x='shared',
                y='shared'
            )
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Gráfico 4.5: Volume por Veículo
@st.cache_data(ttl=900, max_entries=2)
def create_volume_veiculo_chart(df_data, df_budget_vol=None, df_despesas=None):
    """Cria gráfico de barras de Volume por Veículo com linha pontilhada de volume do Budget opcional
    df_despesas: parâmetro legado (não usado)."""
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
        
        # Determinar ordem dos veículos (usar a mesma ordem para barras e linha)
        ordem_veiculos = chart_data['Veículo'].tolist()
        
        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Veículo:N',
                title='Veículo',
                sort=ordem_veiculos,
                scale=alt.Scale(domain=ordem_veiculos),
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume (Unidades)', axis=alt.Axis(grid=False)),
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
            height=360,
            width='container'
            # Título removido para evitar duplicação com st.subheader
        )
        
        # Adicionar rótulos
        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )
        
        # Processar dados de volume do budget se fornecidos
        # Observação: não aplicar filtros por "meses com despesa" aqui. Volume é independente de custo.
        linha_budget_vol = None
        if df_budget_vol is not None and 'Veículo' in df_budget_vol.columns:
            try:
                # Filtrar linhas com Volume e Veículo não nulos
                df_budget_vol_filtrado = df_budget_vol[df_budget_vol['Volume'].notna() & df_budget_vol['Veículo'].notna()].copy()
                
                if len(df_budget_vol_filtrado) > 0:
                    # Agrupar por Veículo seguindo a mesma lógica dos dados principais
                    tem_multiplos_anos_budget = 'Ano' in df_budget_vol_filtrado.columns and df_budget_vol_filtrado['Ano'].nunique() > 1
                    
                    if tem_multiplos_anos_budget and 'Período' in df_budget_vol_filtrado.columns:
                        # Agrupar por Veículo, Período e Ano, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Veículo', 'Período', 'Ano']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por Veículo, somar Volume de todos os períodos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    elif 'Período' in df_budget_vol_filtrado.columns:
                        # Agrupar por Veículo e Período, somar Volume
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Veículo', 'Período']).agg({
                            'Volume': 'sum'
                        }).reset_index()
                        # Agora agrupar por Veículo, somar Volume de todos os períodos
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        # Se não tiver Período, agrupar apenas por Veículo
                        budget_vol_data = df_budget_vol_filtrado.groupby('Veículo').agg({
                            'Volume': 'sum'
                        }).reset_index()
                    
                    # IMPORTANTE: Garantir que todos os veículos do realizado estejam no budget
                    # Criar DataFrame completo com todos os veículos do realizado
                    budget_vol_data_completo = pd.DataFrame({'Veículo': ordem_veiculos})
                    
                    # Fazer merge com os dados de budget (left join para manter todos os veículos do realizado)
                    budget_vol_data = budget_vol_data_completo.merge(
                        budget_vol_data,
                        on='Veículo',
                        how='left'
                    )
                    
                    # Preencher valores faltantes com 0
                    budget_vol_data['Volume'] = budget_vol_data['Volume'].fillna(0)
                    
                    if len(budget_vol_data) > 0:
                        # Adicionar coluna de legenda
                        budget_vol_data_legenda = budget_vol_data.copy()
                        budget_vol_data_legenda['Tipo'] = 'Volume Budget'
                        
                        # Garantir que está na ordem correta (já está na ordem correta por causa do merge)
                        # Mas vamos garantir explicitamente
                        ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos)}
                        budget_vol_data_legenda['_ordem'] = budget_vol_data_legenda['Veículo'].map(ordem_dict)
                        budget_vol_data_legenda = budget_vol_data_legenda.sort_values('_ordem')
                        budget_vol_data_legenda = budget_vol_data_legenda.drop(columns=['_ordem'])
                        
                        # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras (ordem_veiculos)
                        # Isso garante que a linha do budget apareça na mesma ordem do realizado
                        ordem_veiculos_budget = ordem_veiculos
                        
                        # Criar linha tracejada de volume do budget
                        linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                            x=alt.X(
                                'Veículo:N',
                                title='Veículo',
                                sort=ordem_veiculos_budget,
                                scale=alt.Scale(domain=ordem_veiculos_budget),
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            y=alt.Y(
                                'Volume:Q',
                                title='Volume (Unidades)',
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            color=alt.Color(
                                'Tipo:N',
                                title='Legenda',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=alt.Legend(
                                    title='Legenda',
                                    orient='right',
                                    titleFontSize=10,
                                    labelFontSize=9
                                )
                            ),
                            strokeDash=alt.StrokeDash(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Veículo:N', title='Veículo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar bolinhas nos pontos da linha
                        pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('Veículo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='Veículo'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Veículo:N', title='Veículo'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )
                        
                        # Adicionar rótulos nos pontos
                        rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('Veículo:N', sort=ordem_veiculos_budget, scale=alt.Scale(domain=ordem_veiculos_budget), title='Veículo'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            text=alt.Text('Volume:Q', format=',.0f'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            )
                        )
                        
                        linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception as e:
                # Silenciar erro, apenas não mostrar linha do budget
                pass
        
        # Combinar gráfico de barras com linha do budget se existir
        if linha_budget_vol is not None:
            return grafico_barras + rotulos + linha_budget_vol
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico de volume: {e}")
        return None


# Gráfico 4.6: Volume por Oficina
@st.cache_data(ttl=900, max_entries=2)
def create_volume_oficina_chart(df_data, df_budget_vol=None, df_despesas=None):
    """Cria gráfico de barras de Volume por Oficina com linha pontilhada de volume do Budget opcional
    df_despesas: parâmetro legado (não usado)."""
    try:
        if 'Volume' not in df_data.columns or 'Oficina' not in df_data.columns:
            return None

        df_data = df_data[df_data['Volume'].notna() & df_data['Oficina'].notna()].copy()
        if len(df_data) == 0:
            return None

        tem_multiplos_anos = 'Ano' in df_data.columns and df_data['Ano'].nunique() > 1
        if tem_multiplos_anos and 'Período' in df_data.columns:
            df_agrupado_periodo = df_data.groupby(['Oficina', 'Período', 'Ano']).agg({'Volume': 'sum'}).reset_index()
            chart_data = df_agrupado_periodo.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
        elif 'Período' in df_data.columns:
            df_agrupado_periodo = df_data.groupby(['Oficina', 'Período']).agg({'Volume': 'sum'}).reset_index()
            chart_data = df_agrupado_periodo.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
        else:
            chart_data = df_data.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()

        if len(chart_data) == 0:
            return None

        chart_data = chart_data[chart_data['Volume'].notna()].copy()
        if len(chart_data) == 0:
            return None

        chart_data = chart_data.sort_values('Volume', ascending=False)
        ordem_oficinas = chart_data['Oficina'].tolist()

        grafico_barras = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X(
                'Oficina:N',
                title='Oficina',
                sort=ordem_oficinas,
                scale=alt.Scale(domain=ordem_oficinas),
                axis=alt.Axis(grid=False, domain=True, ticks=True)
            ),
            y=alt.Y('Volume:Q', title='Volume (Unidades)', axis=alt.Axis(grid=False)),
            color=alt.Color(
                'Volume:Q',
                title='Volume',
                scale=alt.Scale(scheme='greens')
            ),
            tooltip=[
                alt.Tooltip('Oficina:N', title='Oficina'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f')
            ]
        ).properties(
            height=360,
            width='container'
        )

        rotulos = grafico_barras.mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            color='black',
            fontSize=9
        ).encode(
            text=alt.Text('Volume:Q', format=',.0f')
        )

        linha_budget_vol = None
        if df_budget_vol is not None and 'Oficina' in df_budget_vol.columns:
            try:
                df_budget_vol_filtrado = df_budget_vol[df_budget_vol['Volume'].notna() & df_budget_vol['Oficina'].notna()].copy()
                if len(df_budget_vol_filtrado) > 0:
                    tem_multiplos_anos_budget = 'Ano' in df_budget_vol_filtrado.columns and df_budget_vol_filtrado['Ano'].nunique() > 1

                    if tem_multiplos_anos_budget and 'Período' in df_budget_vol_filtrado.columns:
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Oficina', 'Período', 'Ano']).agg({'Volume': 'sum'}).reset_index()
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
                    elif 'Período' in df_budget_vol_filtrado.columns:
                        df_agrupado_periodo_budget = df_budget_vol_filtrado.groupby(['Oficina', 'Período']).agg({'Volume': 'sum'}).reset_index()
                        budget_vol_data = df_agrupado_periodo_budget.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()
                    else:
                        budget_vol_data = df_budget_vol_filtrado.groupby('Oficina').agg({'Volume': 'sum'}).reset_index()

                    budget_vol_data_completo = pd.DataFrame({'Oficina': ordem_oficinas})
                    budget_vol_data = budget_vol_data_completo.merge(
                        budget_vol_data,
                        on='Oficina',
                        how='left'
                    )
                    budget_vol_data['Volume'] = budget_vol_data['Volume'].fillna(0)

                    if len(budget_vol_data) > 0:
                        budget_vol_data_legenda = budget_vol_data.copy()
                        budget_vol_data_legenda['Tipo'] = 'Volume Budget'

                        ordem_dict = {oficina: idx for idx, oficina in enumerate(ordem_oficinas)}
                        budget_vol_data_legenda['_ordem'] = budget_vol_data_legenda['Oficina'].map(ordem_dict)
                        budget_vol_data_legenda = budget_vol_data_legenda.sort_values('_ordem')
                        budget_vol_data_legenda = budget_vol_data_legenda.drop(columns=['_ordem'])

                        ordem_oficinas_budget = ordem_oficinas

                        linha_budget_vol = alt.Chart(budget_vol_data_legenda).mark_line(
                            strokeDash=[10, 5],
                            strokeWidth=1.5,
                            opacity=0.8
                        ).encode(
                            x=alt.X(
                                'Oficina:N',
                                title='Oficina',
                                sort=ordem_oficinas_budget,
                                scale=alt.Scale(domain=ordem_oficinas_budget),
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            y=alt.Y(
                                'Volume:Q',
                                title='Volume (Unidades)',
                                axis=alt.Axis(grid=False, domain=True, ticks=True)
                            ),
                            color=alt.Color(
                                'Tipo:N',
                                title='Legenda',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=alt.Legend(
                                    title='Legenda',
                                    orient='right',
                                    titleFontSize=10,
                                    labelFontSize=9
                                )
                            ),
                            strokeDash=alt.StrokeDash(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=[[10, 5]]),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Oficina:N', title='Oficina'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )

                        pontos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_circle(
                            size=80,
                            opacity=0.9
                        ).encode(
                            x=alt.X('Oficina:N', sort=ordem_oficinas_budget, scale=alt.Scale(domain=ordem_oficinas_budget), title='Oficina'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            ),
                            tooltip=[
                                alt.Tooltip('Oficina:N', title='Oficina'),
                                alt.Tooltip('Tipo:N', title='Tipo'),
                                alt.Tooltip('Volume:Q', title='Volume Budget', format=',.0f')
                            ]
                        )

                        rotulos_budget_vol = alt.Chart(budget_vol_data_legenda).mark_text(
                            align='center',
                            baseline='bottom',
                            dy=-15,
                            fontSize=9,
                            fontWeight='bold'
                        ).encode(
                            x=alt.X('Oficina:N', sort=ordem_oficinas_budget, scale=alt.Scale(domain=ordem_oficinas_budget), title='Oficina'),
                            y=alt.Y('Volume:Q', title='Volume (Unidades)'),
                            text=alt.Text('Volume:Q', format=',.0f'),
                            color=alt.Color(
                                'Tipo:N',
                                scale=alt.Scale(domain=['Volume Budget'], range=['#FF6B35']),
                                legend=None
                            )
                        )

                        linha_budget_vol = linha_budget_vol + pontos_budget_vol + rotulos_budget_vol
            except Exception:
                pass

        if linha_budget_vol is not None:
            return grafico_barras + rotulos + linha_budget_vol
        else:
            return grafico_barras + rotulos
    except Exception as e:
        st.error(f"Erro ao criar gráfico de volume por oficina: {e}")
        return None


# Inicializar session_state para manter a tab selecionada
# Usar uma chave mais específica para evitar conflitos
if 'tab_selecionada_tc_veic_persistente' not in st.session_state:
    st.session_state.tab_selecionada_tc_veic_persistente = 0

# Verificar se há parâmetro de tab na URL e atualizar session_state
# Isso garante que a tab seja mantida mesmo após recarregamento por filtros
tab_from_url = st.query_params.get('tab', None)
if tab_from_url is not None:
    try:
        tab_index = int(tab_from_url)
        if 0 <= tab_index <= 3:  # Validar índice (0-3 para 4 tabs)
            st.session_state.tab_selecionada_tc_veic_persistente = tab_index
    except ValueError:
        pass
# Se não houver parâmetro na URL, manter o valor atual do session_state
# Isso evita que a tab seja resetada quando há mudanças de filtros
# O valor já foi inicializado acima se não existir

# Manter compatibilidade com a chave antiga
st.session_state.tab_selecionada_tc_veic = st.session_state.tab_selecionada_tc_veic_persistente

# Só criar tabs e JavaScript se estivermos na página principal
if is_main_page:
    # JavaScript ANTES das tabs para interceptar a criação
    # Este script será executado antes que o Streamlit defina a primeira tab como padrão
    st.markdown(f"""
<script>
(function() {{
    // Obter índice da tab da URL
    function obterTabIndex() {{
        const urlParams = new URLSearchParams(window.location.search);
        const tabIndexUrl = urlParams.get('tab');
        if (tabIndexUrl !== null) {{
            const index = parseInt(tabIndexUrl);
            if (index >= 0 && index <= 4) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_veic_persistente};
    }}
    
    const tabIndexDesejado = obterTabIndex();
    
    // Interceptar a criação das tabs ANTES que sejam renderizadas
    // Usar MutationObserver para detectar quando as tabs são criadas
    const observerPrecoce = new MutationObserver(function(mutations) {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length >= 5) {{
            // Tabs foram criadas, verificar se a primeira está selecionada
            const primeiraTab = tabs[0];
            if (primeiraTab && primeiraTab.getAttribute('aria-selected') === 'true' && tabIndexDesejado !== 0) {{
                // Primeira tab está selecionada mas não deveria estar
                // Clicar na tab correta IMEDIATAMENTE
                if (tabs[tabIndexDesejado]) {{
                    // Usar requestAnimationFrame para garantir execução no próximo frame
                    requestAnimationFrame(function() {{
                        tabs[tabIndexDesejado].click();
                    }});
                }}
            }}
        }}
    }});
    
    // Começar a observar imediatamente
    observerPrecoce.observe(document.body, {{
        childList: true,
        subtree: true
    }});
    
    // Também tentar executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            const tabs = document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs.length >= 5 && tabIndexDesejado !== 0) {{
                const primeiraTab = tabs[0];
                if (primeiraTab && primeiraTab.getAttribute('aria-selected') === 'true') {{
                    requestAnimationFrame(function() {{
                        if (tabs[tabIndexDesejado]) {{
                            tabs[tabIndexDesejado].click();
                        }}
                    }});
                }}
            }}
        }});
    }}
}})();
</script>
""", unsafe_allow_html=True)

    # Criar estrutura de tabs para organização
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 TC Ext",
        "📈 Volume",
        "🚗 TC Ext por Veíc",
        "📋 Detalhe Real",
        "🧾 Detalhe Budget",
    ])
else:
    # Se não estamos na página principal, criar tabs vazias para evitar erros
    # Mas não renderizar conteúdo
    tab1 = tab2 = tab3 = tab4 = tab5 = None

# JavaScript DEPOIS das tabs para manter a seleção
st.markdown(f"""
<script>
(function() {{
    // Obter índice da tab da URL
    function obterTabIndex() {{
        const urlParams = new URLSearchParams(window.location.search);
        const tabIndexUrl = urlParams.get('tab');
        if (tabIndexUrl !== null) {{
            const index = parseInt(tabIndexUrl);
            if (index >= 0 && index <= 4) {{
                return index;
            }}
        }}
        return {st.session_state.tab_selecionada_tc_veic_persistente};
    }}
    
    let tabIndexSalvo = obterTabIndex();
    let restauracaoEmAndamento = false;
    
    // Função para forçar a seleção da tab correta
    function forcarSelecaoTab(index) {{
        if (restauracaoEmAndamento) return;
        
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0 || index < 0 || index >= tabs.length) return;
        
        const tabAlvo = tabs[index];
        if (!tabAlvo) return;
        
        // Verificar se já está selecionada
        if (tabAlvo.getAttribute('aria-selected') === 'true') {{
            return; // Já está selecionada
        }}
        
        restauracaoEmAndamento = true;
        
        // Múltiplas tentativas de clicar
        function tentarClicar() {{
            tabAlvo.click();
            
            // Verificar se funcionou
            setTimeout(function() {{
                if (tabAlvo.getAttribute('aria-selected') === 'true') {{
                    restauracaoEmAndamento = false;
                }} else {{
                    // Tentar novamente
                    requestAnimationFrame(tentarClicar);
                }}
            }}, 50);
        }}
        
        requestAnimationFrame(tentarClicar);
    }}
    
    // Função para verificar e restaurar
    function verificarERestaurar() {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length === 0) return;
        
        // Atualizar da URL
        tabIndexSalvo = obterTabIndex();
        
        // Verificar qual tab está selecionada
        let tabAtual = -1;
        tabs.forEach((tab, index) => {{
            if (tab.getAttribute('aria-selected') === 'true') {{
                tabAtual = index;
            }}
        }});
        
        // Se não está na tab correta, restaurar
        if (tabAtual !== tabIndexSalvo) {{
            forcarSelecaoTab(tabIndexSalvo);
        }}
    }}
    
    // Configurar listeners para salvar na URL quando clicar
    function configurarListeners() {{
        const tabs = document.querySelectorAll('[data-baseweb="tab"]');
        tabs.forEach((tab, index) => {{
            // Remover listeners antigos se existirem
            const novoTab = tab.cloneNode(true);
            if (tab.parentNode) {{
                tab.parentNode.replaceChild(novoTab, tab);
            }}
            
            // Adicionar novo listener com captura (true) para interceptar antes do Streamlit
            novoTab.addEventListener('click', function(e) {{
                tabIndexSalvo = index;
                const url = new URL(window.location);
                url.searchParams.set('tab', index);
                window.history.replaceState({{}}, '', url);
                
                // Também salvar no sessionStorage para persistência entre recarregamentos
                sessionStorage.setItem('tab_selecionada_tc_veic', index);
                
                // Atualizar session_state no Streamlit via query params
                // Isso garante que o Streamlit saiba qual tab está selecionada
                if (window.parent && window.parent.postMessage) {{
                    window.parent.postMessage({{
                        type: 'streamlit:setFrameHeight',
                        height: document.body.scrollHeight
                    }}, '*');
                }}
            }}, true);
        }});
    }}
    
    // Tentar restaurar do sessionStorage se não houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_veic');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // Só usar sessionStorage se não houver parâmetro na URL
                    tabIndexSalvo = index;
                    const url = new URL(window.location);
                    url.searchParams.set('tab', index);
                    window.history.replaceState({{}}, '', url);
                }}
            }}
        }}
    }}
    
    // Tentar restaurar do sessionStorage se não houver na URL
    function restaurarDeSessionStorage() {{
        const tabSalva = sessionStorage.getItem('tab_selecionada_tc_veic');
        if (tabSalva !== null) {{
            const index = parseInt(tabSalva);
            if (index >= 0 && index <= 3) {{
                const urlParams = new URLSearchParams(window.location.search);
                if (!urlParams.has('tab')) {{
                    // Só usar sessionStorage se não houver parâmetro na URL
                    tabIndexSalvo = index;
                    const url = new URL(window.location);
                    url.searchParams.set('tab', index);
                    window.history.replaceState({{}}, '', url);
                }}
            }}
        }}
    }}
    
    // Restaurar do sessionStorage primeiro
    restaurarDeSessionStorage();
    
    // Executar imediatamente usando requestAnimationFrame
    requestAnimationFrame(function() {{
        verificarERestaurar();
        configurarListeners();
    }});
    
    // Executar quando o DOM estiver pronto
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', function() {{
            restaurarDeSessionStorage();
            verificarERestaurar();
            configurarListeners();
        }});
    }} else {{
        // DOM já está pronto
        restaurarDeSessionStorage();
        verificarERestaurar();
        configurarListeners();
    }}
    
    // Executar periodicamente (mais frequente para garantir)
    // IMPORTANTE: Reduzir frequência para evitar conflitos com recarregamentos do Streamlit
    setInterval(function() {{
        verificarERestaurar();
    }}, 200);
    
    // Observar mudanças no DOM
    const observer = new MutationObserver(function() {{
        verificarERestaurar();
        configurarListeners();
    }});
    
    // Observar o container principal
    setTimeout(function() {{
        const mainContainer = document.querySelector('main') || document.body;
        if (mainContainer) {{
            observer.observe(mainContainer, {{
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['aria-selected']
            }});
        }}
        
        const tabsContainer = document.querySelector('[data-baseweb="tabs"]');
        if (tabsContainer) {{
            observer.observe(tabsContainer, {{
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['aria-selected']
            }});
        }}
    }}, 50);
    
    // Executar em múltiplos momentos para garantir
    [100, 200, 300, 500, 1000].forEach(function(delay) {{
        setTimeout(function() {{
            verificarERestaurar();
            configurarListeners();
        }}, delay);
    }});
}})();
</script>
""", unsafe_allow_html=True)

# ==========================================
# TAB 1: TC Ext
# ==========================================
# Só exibir conteúdo das tabs se estivermos na página principal
if is_main_page:
    # Preparar volume filtrado pela sidebar para uso em CPU (antes das tabs)
    df_vol_filtrado_sidebar = None
    try:
        df_vol_base = load_volume_data(ano_selecionado)
        if df_vol_base is not None and 'Volume' in df_vol_base.columns:
            df_vol_filtrado_sidebar = filtrar_volume_com_sidebar(df_vol_base, df_total)
    except Exception:
        df_vol_filtrado_sidebar = None

    # Criar df_visualizacao a partir de df_filtrado antes de usar nas tabs
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_visualizacao = df_filtrado.copy()
        # Definir coluna_visualizacao baseado no tipo_visualizacao
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            coluna_visualizacao = 'CPU'
            if 'Volume' not in df_visualizacao.columns:
                if df_vol_filtrado_sidebar is None:
                    try:
                        df_vol_base = load_volume_data(ano_selecionado)
                        if df_vol_base is not None and 'Volume' in df_vol_base.columns:
                            df_vol_filtrado_sidebar = filtrar_volume_com_sidebar(df_vol_base, df_total)
                    except Exception:
                        df_vol_filtrado_sidebar = None
                if df_vol_filtrado_sidebar is not None and 'Volume' in df_vol_filtrado_sidebar.columns:
                    chaves_merge = [
                        col for col in ['Oficina', 'Veículo', 'Período', 'Ano']
                        if col in df_visualizacao.columns and col in df_vol_filtrado_sidebar.columns
                    ]
                    if chaves_merge:
                        df_visualizacao = df_visualizacao.merge(
                            df_vol_filtrado_sidebar[chaves_merge + ['Volume']],
                            on=chaves_merge,
                            how='left'
                        )
            if 'Custo FP' in df_visualizacao.columns and 'Volume' in df_visualizacao.columns:
                df_visualizacao['CPU'] = np.where(
                    (df_visualizacao['Volume'].notna()) & (df_visualizacao['Volume'] != 0),
                    df_visualizacao['Custo FP'] / df_visualizacao['Volume'],
                    0
                )
        else:
            coluna_visualizacao = 'Custo FP' if 'Custo FP' in df_visualizacao.columns else 'Despesa Primaria'
    else:
        # Se df_filtrado não estiver disponível, criar DataFrame vazio
        df_visualizacao = pd.DataFrame()
        coluna_visualizacao = 'Custo FP'
    
    # Criar df_para_grafico_periodo a partir de df_filtrado (antes do filtro de período)
    if 'df_filtrado' in locals() and df_filtrado is not None:
        df_para_grafico_periodo = df_filtrado.copy()
    else:
        df_para_grafico_periodo = pd.DataFrame()
    
    with tab1:
        # Exibir gráfico por Período
        # No modo CPU, a coluna 'CPU' pode não existir ainda em df_visualizacao,
        # mas será criada dentro do bloco. Verificar apenas se 'Período' existe.
        if 'Período' in df_visualizacao.columns:
            # IMPORTANTE: Criar df_visualizacao_para_grafico usando df_para_grafico_periodo
            # (dados ANTES do filtro de período) para mostrar TODOS os períodos no gráfico
            # Aplicar a mesma lógica de preparação de dados, mas usando df_para_grafico_periodo
            
            # Carregar dados de volume reais (necessário para cálculo de FLEX)
            df_vol_calc_grafico = load_volume_data(ano_selecionado)
        
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
                if ('Oficina' in df_para_grafico_periodo.columns and
                        'Período' in df_para_grafico_periodo.columns):
                    tem_veiculo = 'Veículo' in df_para_grafico_periodo.columns
                    tem_ano = 'Ano' in df_para_grafico_periodo.columns
                    
                    # Aplicar mesmos filtros de Veículo e Oficina ao volume
                    # Preferir volume já filtrado com a sidebar (mesma base das tabelas)
                    if (
                        'df_vol_filtrado_sidebar' in locals()
                        and df_vol_filtrado_sidebar is not None
                        and hasattr(df_vol_filtrado_sidebar, 'columns')
                        and 'Volume' in df_vol_filtrado_sidebar.columns
                    ):
                        df_vol_calc_filtrado_grafico = df_vol_filtrado_sidebar.copy()
                    else:
                        df_vol_calc_filtrado_grafico = df_vol_calc_grafico.copy()
                # 🔧 IMPORTANTE: NÃO recortar o volume usando "quais veículos/oficinas aparecem no custo".
                # Veículos/oficinas podem ter volume mesmo sem despesa; nesse caso custo=0 e o volume deve entrar no denominador.
                
                colunas_agrupamento_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_grafico.append('Veículo')
                
                if 'Custo FP' in df_para_grafico_periodo.columns:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Custo FP'].sum()
                else:
                    df_total_agrupado_grafico = df_para_grafico_periodo.groupby(
                        colunas_agrupamento_grafico, as_index=False
                    )['Despesa Primaria'].sum()
                    df_total_agrupado_grafico.rename(columns={'Despesa Primaria': 'Custo FP'}, inplace=True)
                
                colunas_agrupamento_vol_grafico = ['Oficina', 'Período']
                if tem_ano:
                    colunas_agrupamento_vol_grafico.append('Ano')
                if tem_veiculo:
                    colunas_agrupamento_vol_grafico.append('Veículo')
                
                df_vol_agrupado_grafico = df_vol_calc_filtrado_grafico.groupby(
                    colunas_agrupamento_vol_grafico, as_index=False
                )['Volume'].sum()
                
                # 🔧 CORREÇÃO CRÍTICA: Volume não pode ser recortado pela existência de custo.
                # Se existir volume em um Veículo/Período sem despesa, o custo é 0 e o volume deve entrar no denominador.
                df_cpu_grafico = pd.merge(
                    df_total_agrupado_grafico,
                    df_vol_agrupado_grafico,
                    on=colunas_agrupamento_grafico,
                    how='outer'
                )
                df_cpu_grafico['Volume'] = pd.to_numeric(df_cpu_grafico.get('Volume'), errors='coerce').fillna(0)
                df_cpu_grafico['Custo FP'] = pd.to_numeric(df_cpu_grafico.get('Custo FP'), errors='coerce').fillna(0)
                
                # Calcular CPU - VETORIZADO
                df_cpu_grafico['CPU'] = np.where(
                    (df_cpu_grafico['Volume'].notna()) & (df_cpu_grafico['Volume'] != 0),
                    df_cpu_grafico['Custo FP'] / df_cpu_grafico['Volume'],
                    0
                )
                
                # IMPORTANTE: Manter colunas Total e Volume para que o gráfico possa recalcular CPU corretamente
                # O gráfico agrupa por Ano e Período e recalcula CPU a partir de Total e Volume agregados
                df_visualizacao_para_grafico = df_cpu_grafico.copy()
                coluna_visualizacao_grafico = 'CPU'
            else:
                df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
                coluna_visualizacao_grafico = 'Custo FP' if 'Custo FP' in df_para_grafico_periodo.columns else 'Despesa Primaria'
        else:
            df_visualizacao_para_grafico = df_para_grafico_periodo.copy()
            coluna_visualizacao_grafico = 'Custo FP' if 'Custo FP' in df_para_grafico_periodo.columns else 'Despesa Primaria'
        
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
                oficina_opcoes_grafico = st.session_state.get('_oficina_opcoes_tc_veic')
                if not isinstance(oficina_opcoes_grafico, list) or not oficina_opcoes_grafico:
                    oficinas_set = set(df_grafico_periodo['Oficina'].dropna().astype(str).unique().tolist())
                    oficinas_set.update(get_budget_oficinas_opcoes(ano_selecionado))
                    oficinas_set.update(get_budget_volume_oficinas_opcoes(ano_selecionado))
                    oficina_opcoes_grafico = ["Todos"] + sorted(oficinas_set)

                if 'filtro_oficina_grafico_periodo_tc' not in st.session_state:
                    st.session_state['filtro_oficina_grafico_periodo_tc'] = st.session_state.get('filtro_oficina_tc_veic', ["Todos"])

                default_grafico = st.session_state.get('filtro_oficina_tc_veic', ["Todos"])
                if not all(x in oficina_opcoes_grafico for x in default_grafico):
                    default_grafico = ["Todos"]
                oficina_selecionadas_grafico = st.multiselect(
                    "🏭 Filtrar por Oficina:",
                    oficina_opcoes_grafico,
                    default=default_grafico,
                    key="filtro_oficina_grafico_periodo_tc",
                )
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
        
        # Filtro de Veículo
        with col2:
            if 'Veículo' in df_grafico_periodo.columns:
                veiculo_opcoes_grafico = st.session_state.get('_veiculo_opcoes_tc_veic')
                if not isinstance(veiculo_opcoes_grafico, list) or not veiculo_opcoes_grafico:
                    veiculo_opcoes_grafico = get_filter_options(df_grafico_periodo, 'Veículo')

                if 'filtro_veiculo_grafico_periodo_tc' not in st.session_state:
                    st.session_state['filtro_veiculo_grafico_periodo_tc'] = st.session_state.get('filtro_veiculo_tc_veic', ["Todos"])

                default_veiculo_grafico = st.session_state.get('filtro_veiculo_tc_veic', ["Todos"])
                if not all(x in veiculo_opcoes_grafico for x in default_veiculo_grafico):
                    default_veiculo_grafico = ["Todos"]
                veiculo_selecionados_grafico = st.multiselect(
                    "🚗 Filtrar por Veículo:",
                    veiculo_opcoes_grafico,
                    default=default_veiculo_grafico,
                    key="filtro_veiculo_grafico_periodo_tc",
                )
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_grafico_periodo = df_grafico_periodo[
                        df_grafico_periodo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        # IMPORTANTE: Quando "Todos" está selecionado, garantir que todos os períodos de todos os anos sejam mostrados
        # O create_period_chart já faz o agrupamento correto por Ano e Período quando há coluna Ano
        
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico aos dados de volume e budget DEPOIS que os filtros são definidos
        # Os filtros de Oficina e Veículo do gráfico devem ser aplicados a TODOS os dados (volumes, budget, etc.)
        
        # Carregar dados de budget e aplicar mesmos filtros
        df_budget_filtrado = None
        df_budget_vol_filtrado = None
        
        try:
            # Carregar dados de budget
            df_budget = load_budget_data(ano_selecionado)
            df_budget_vol = load_budget_volume_data(ano_selecionado)

            def _aplicar_filtro_selecionado(df_in, coluna_filtro, chave_state):
                if df_in is None or coluna_filtro not in df_in.columns:
                    return df_in
                selecionadas = st.session_state.get(chave_state, ["Todos"])
                if isinstance(selecionadas, tuple):
                    selecionadas = list(selecionadas)
                if selecionadas and "Todos" not in selecionadas:
                    df_in = df_in[df_in[coluna_filtro].astype(str).isin([str(x) for x in selecionadas])].copy()
                return df_in
            
            if df_budget is not None:
                # 🔧 CORREÇÃO CRÍTICA: Aplicar fator de conversão na coluna Total do budget (mesma unidade que Total real)
                # Isso mantém os dados na mesma unidade para comparações consistentes
                # IMPORTANTE: NÃO aplicar fator quando está em modo CPU (CPU já é uma razão)
                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Custo FP' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Custo FP'] = df_budget['Custo FP'] / 1000
                    elif fator_conversao == "M (Milhões)":
                        df_budget['Custo FP'] = df_budget['Custo FP'] / 1000000
                
                # Aplicar conversão de moeda DEPOIS do fator de conversão (mesma lógica do fator)
                # IMPORTANTE: Aplicar na mesma ordem: primeiro fator, depois moeda
                if moeda_codigo != "BRL" and 'Custo FP' in df_budget.columns:
                    df_budget = converter_coluna_moeda(df_budget, 'Custo FP', moeda_codigo, taxas_cambio)
                
                # ✅ Aplicar apenas filtros efetivamente selecionados (sidebar), sem interseção com o Real
                df_budget_filtrado = df_budget.copy()

                # Filtros principais
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'Oficina', 'filtro_oficina_tc_veic')
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'Veículo', 'filtro_veiculo_tc_veic')
                df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, 'USI', 'filtro_usi_tc_veic')

                # Filtros adicionais (mesmos nomes usados na sidebar)
                for col_filtro in ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                    df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, col_filtro, f'filtro_{col_filtro}_tc_veic')

                for col_filtro in ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve']:
                    df_budget_filtrado = _aplicar_filtro_selecionado(df_budget_filtrado, col_filtro, f'filtro_avancado_{col_filtro}_tc_veic')

                # Filtros do gráfico (Oficina/Veículo) - normalmente sincronizados, mas mantidos por segurança
                if 'Oficina' in df_budget_filtrado.columns and oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
                if 'Veículo' in df_budget_filtrado.columns and veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
            
            if df_budget_vol is not None:
                # ✅ Aplicar apenas filtros efetivamente selecionados (sidebar), sem interseção com o Real
                df_budget_vol_filtrado = df_budget_vol.copy()

                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'Oficina', 'filtro_oficina_tc_veic')
                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'Veículo', 'filtro_veiculo_tc_veic')
                df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, 'USI', 'filtro_usi_tc_veic')

                for col_filtro in ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                    df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, col_filtro, f'filtro_{col_filtro}_tc_veic')

                for col_filtro in ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve']:
                    df_budget_vol_filtrado = _aplicar_filtro_selecionado(df_budget_vol_filtrado, col_filtro, f'filtro_avancado_{col_filtro}_tc_veic')

                if 'Oficina' in df_budget_vol_filtrado.columns and oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
                if 'Veículo' in df_budget_vol_filtrado.columns and veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        except Exception as e:
            st.sidebar.warning(f"⚠️ Erro ao carregar dados de budget: {e}")
        
        # Criar gráfico com dados filtrados (usar coluna_visualizacao_grafico que foi criada acima)
        # O create_period_chart já faz o agrupamento correto por Ano e Período quando há coluna Ano
        # Preparar dados de volume reais para cálculo de FLEX
        # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros da sidebar ao volume (mesmos de df_para_grafico_periodo)
        # O volume precisa ter os mesmos filtros que os dados reais para garantir consistência
        df_volume_real_filtrado = None
        if df_vol_calc_grafico is not None and 'Volume' in df_vol_calc_grafico.columns:
            # ✅ Usar a mesma lógica da aba Volume: filtros da sidebar, sem interseção com o Real (custo)
            # Isso evita cortar volume para apenas veículos/oficinas que aparecem no realizado.
            df_volume_real_filtrado = filtrar_volume_com_sidebar(df_vol_calc_grafico, df_total)
            
            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) ao volume DEPOIS que os filtros são definidos
            # Isso garante que o volume responda aos filtros do gráfico
            if df_volume_real_filtrado is not None:
                # Aplicar filtro de Oficina do gráfico
                if 'Oficina' in df_volume_real_filtrado.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Aplicar filtro de Veículo do gráfico
                if 'Veículo' in df_volume_real_filtrado.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_volume_real_filtrado = df_volume_real_filtrado[
                            df_volume_real_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) aos dados de budget DEPOIS que os filtros são definidos
        # Isso garante que os dados de budget respondam aos filtros do gráfico
        if df_budget_filtrado is not None:
            # Aplicar filtro de Oficina do gráfico
            if 'Oficina' in df_budget_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de Veículo do gráfico
            if 'Veículo' in df_budget_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_filtrado = df_budget_filtrado[
                        df_budget_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()
        
        if df_budget_vol_filtrado is not None:
            # Aplicar filtro de Oficina do gráfico
            if 'Oficina' in df_budget_vol_filtrado.columns:
                if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                    ].copy()
            
            # Aplicar filtro de Veículo do gráfico
            if 'Veículo' in df_budget_vol_filtrado.columns:
                if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                    df_budget_vol_filtrado = df_budget_vol_filtrado[
                        df_budget_vol_filtrado['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                    ].copy()

        # 📊 Resumo TC Ext (acima do gráfico) - mesmos indicadores da tabela Flex
        try:
            if (
                df_budget_filtrado is not None
                and df_budget_vol_filtrado is not None
                and df_volume_real_filtrado is not None
                and 'Custo FP' in df_budget_filtrado.columns
                and 'Custo FP' in df_filtrado.columns
            ):
                # Totais (já estão na mesma moeda/fator aplicados nos DataFrames)
                total_real_custo = pd.to_numeric(df_filtrado['Custo FP'], errors='coerce').fillna(0).sum()

                # Aplicar também os filtros do gráfico (Oficina e Veículo) ao total real do resumo
                df_real_para_resumo = df_filtrado.copy()
                if 'Oficina' in df_real_para_resumo.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'Veículo' in df_real_para_resumo.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_para_resumo = df_real_para_resumo[
                            df_real_para_resumo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                total_real_custo = pd.to_numeric(df_real_para_resumo['Custo FP'], errors='coerce').fillna(0).sum()

                bud_total_custo = pd.to_numeric(df_budget_filtrado['Custo FP'], errors='coerce').fillna(0).sum()

                volume_real_total = 0.0
                if 'Volume' in df_volume_real_filtrado.columns:
                    df_vol_real_para_resumo = df_volume_real_filtrado
                    # 🔧 CORREÇÃO: aplicar mesmos filtros do gráfico também nos volumes
                    if 'Oficina' in df_vol_real_para_resumo.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_vol_real_para_resumo = df_vol_real_para_resumo[
                                df_vol_real_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'Veículo' in df_vol_real_para_resumo.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_vol_real_para_resumo = df_vol_real_para_resumo[
                                df_vol_real_para_resumo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                    volume_real_total = float(pd.to_numeric(df_vol_real_para_resumo['Volume'], errors='coerce').fillna(0).sum())

                volume_budget_total = 0.0
                if 'Volume' in df_budget_vol_filtrado.columns:
                    df_vol_bud_para_resumo = df_budget_vol_filtrado
                    # 🔧 CORREÇÃO: aplicar mesmos filtros do gráfico também nos volumes
                    if 'Oficina' in df_vol_bud_para_resumo.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_vol_bud_para_resumo = df_vol_bud_para_resumo[
                                df_vol_bud_para_resumo['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'Veículo' in df_vol_bud_para_resumo.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_vol_bud_para_resumo = df_vol_bud_para_resumo[
                                df_vol_bud_para_resumo['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                    volume_budget_total = float(pd.to_numeric(df_vol_bud_para_resumo['Volume'], errors='coerce').fillna(0).sum())

                # Split budget por custo (Fixo / Não-Fixo) para cálculo do Flex BUD
                # Regra: tudo que NÃO é Fixo é flexível
                bud_fixo = 0.0
                if 'Custo' in df_budget_filtrado.columns:
                    custo_str = df_budget_filtrado['Custo'].astype(str).str.lower()
                    mask_fixo = custo_str.str.startswith('fix')
                    bud_fixo = pd.to_numeric(df_budget_filtrado.loc[mask_fixo, 'Custo FP'], errors='coerce').fillna(0).sum()
                bud_nao_fixo = float(bud_total_custo - bud_fixo)

                proporcao_volume = (volume_real_total / volume_budget_total) if volume_budget_total not in (0, None) else 1.0
                flex_bud_total_custo = float(bud_fixo + (bud_nao_fixo * proporcao_volume))

                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    total_exibir = (total_real_custo / volume_real_total) if volume_real_total not in (0, None) else 0.0
                    flex_exibir = (flex_bud_total_custo / volume_real_total) if volume_real_total not in (0, None) else 0.0
                    bud_exibir = (bud_total_custo / volume_budget_total) if volume_budget_total not in (0, None) else 0.0
                    sufixo = ""
                else:
                    total_exibir = float(total_real_custo)
                    flex_exibir = float(flex_bud_total_custo)
                    bud_exibir = float(bud_total_custo)
                    sufixo = ""
                    if fator_conversao:
                        if fator_conversao == "K (milhares)":
                            sufixo = " K"
                        elif fator_conversao == "M (Milhões)":
                            sufixo = " M"

                flex_menos_bud = flex_exibir - bud_exibir
                total_menos_flex = total_exibir - flex_exibir
                total_div_flex = (total_exibir / flex_exibir) if flex_exibir not in (0, None) else 0.0

                def _fmt_val(v):
                    return f"{v:,.2f}{sufixo}"

                st.subheader("📊 Resumo TC Ext")
                st.markdown(
                    """
                    <style>
                    .tc-kpi-card {padding: 0.6rem 0.8rem; border: 1px solid rgba(49, 51, 63, 0.15); border-radius: 8px; background: rgba(0, 0, 0, 0.02);}
                    .tc-kpi-label {opacity: 0.75;}
                    .tc-kpi-value {font-size: 1.1em; font-weight: 600;}
                    .tc-kpi-spacer {display: block; height: 1.75rem;}
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                def _render_tc_kpi(label, value):
                    st.markdown(
                        f"""
                        <div class=\"tc-kpi-card\">
                            <div class=\"tc-kpi-label\">{label}</div>
                            <div class=\"tc-kpi-value\">{value}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                c1, c2, c3, c4, c5, c6 = st.columns(6)
                with c1:
                    _render_tc_kpi("BUD", _fmt_val(bud_exibir))
                with c2:
                    _render_tc_kpi("Flex Bud - BUD", _fmt_val(flex_menos_bud))
                with c3:
                    _render_tc_kpi("Flex BUD", _fmt_val(flex_exibir))
                with c4:
                    _render_tc_kpi("Total - Flex Bud", _fmt_val(total_menos_flex))
                with c5:
                    _render_tc_kpi("Total", _fmt_val(total_exibir))
                with c6:
                    _render_tc_kpi("Total / Flex Bud", f"{total_div_flex:.0%}")

                st.markdown("<div class='tc-kpi-spacer'></div>", unsafe_allow_html=True)
        except Exception:
            # Se algo der errado no resumo, não quebrar a tela.
            pass
        
        # No modo CPU, precisamos passar os dados originais (com 'Custo') para calcular FLEX
        # 🔧 CORREÇÃO CRÍTICA: Usar df_total diretamente (que tem 'Custo') em vez de df_para_grafico_periodo
        # porque df_para_grafico_periodo pode não ter 'Custo' se foi processado
        df_real_original_grafico = None
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            # IMPORTANTE: No modo CPU, usar df_filtrado diretamente que já tem TODOS os filtros aplicados
            # e tem a coluna 'Custo' necessária para calcular Flex Bud
            # df_filtrado já tem a conversão de moeda aplicada e todos os filtros da sidebar
            if 'Custo' in df_filtrado.columns and 'Custo FP' in df_filtrado.columns:
                df_real_original_grafico = df_filtrado.copy()
                
                # Aplicar apenas os filtros do gráfico (Oficina e Veículo) se diferentes dos da sidebar
                # Filtro de Oficina do gráfico
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                
                # Filtro de Veículo do gráfico
                if 'Veículo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
                
                # NOTA: A conversão de moeda já foi aplicada no df_total (linha 1104) e df_filtrado herda isso
                # Portanto, df_real_original_grafico['Custo FP'] já está convertido
                
                # 🔧 VERIFICAÇÃO: Garantir que df_real_original_grafico tem dados válidos após aplicar filtros
                if len(df_real_original_grafico) == 0:
                    st.warning("⚠️ Aviso: df_real_original_grafico está vazio após aplicar filtros. Verifique os filtros selecionados.")
                elif 'Custo FP' in df_real_original_grafico.columns and abs(df_real_original_grafico['Custo FP'].sum()) < 0.0001:
                    st.warning("⚠️ Aviso: df_real_original_grafico tem Total muito próximo de zero. Verifique os dados e filtros.")
            else:
                # Fallback: tentar usar df_para_grafico_periodo se df_total não tiver 'Custo'
                df_real_original_grafico = df_para_grafico_periodo.copy()
                # Aplicar mesmos filtros de Oficina e Veículo
                if 'Oficina' in df_real_original_grafico.columns:
                    if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                        ].copy()
                if 'Veículo' in df_real_original_grafico.columns:
                    if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                        df_real_original_grafico = df_real_original_grafico[
                            df_real_original_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                        ].copy()
        
        # Observação: meses faltantes são tratados no create_period_chart
        # (períodos do budget entram apenas no eixo; realizado fica vazio/zero)

        # =============================
        # Resumo (tabelas) Budget x Real por Oficina
        # =============================
        with st.expander("📋 Resumo Budget e Real Oficinas", expanded=False):
            # Base Real (usar df_filtrado + filtros do gráfico para consistência)
            df_real_resumo_tab1 = None
            try:
                if 'df_filtrado' in locals() and df_filtrado is not None:
                    df_real_resumo_tab1 = df_filtrado.copy()
                    if 'Oficina' in df_real_resumo_tab1.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_resumo_tab1 = df_real_resumo_tab1[
                                df_real_resumo_tab1['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    if 'Veículo' in df_real_resumo_tab1.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_resumo_tab1 = df_real_resumo_tab1[
                                df_real_resumo_tab1['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
            except Exception:
                df_real_resumo_tab1 = None

            # Budget (BDG)
            df_tab_budget = _montar_tabela_resumo_oficinas(
                df_budget_filtrado,
                tipo_visualizacao,
                index_name="BDG",
                coluna_valor_preferida="Total",
                df_volume=df_budget_vol_filtrado,
            )
            if df_tab_budget is None or df_tab_budget.empty:
                st.info("ℹ️ Sem dados de Budget para exibir no resumo.")
            else:
                st.markdown("**Budget (BDG)**")
                st.dataframe(
                    df_tab_budget.style.format(lambda x: _formatar_num_ptbr(x, 2)),
                    use_container_width=True,
                )

            # Real
            df_tab_real = _montar_tabela_resumo_oficinas(
                df_real_resumo_tab1,
                tipo_visualizacao,
                index_name="REAL",
                coluna_valor_preferida="Total",
                df_volume=df_volume_real_filtrado,
            )
            if df_tab_real is None or df_tab_real.empty:
                st.info("ℹ️ Sem dados de Real para exibir no resumo.")
            else:
                st.markdown("**Real (Realizado)**")
                st.dataframe(
                    df_tab_real.style.format(lambda x: _formatar_num_ptbr(x, 2)),
                    use_container_width=True,
                )
        
        # Exibir título do gráfico após os filtros para evitar sobreposição
        st.markdown("<br>", unsafe_allow_html=True)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            st.subheader("📊 CPU por Período")
        else:
            st.subheader("📊 Soma do Valor por Período")

        # Validar dados antes de criar gráfico
        if df_grafico_periodo is None or df_grafico_periodo.empty:
            st.warning("⚠️ Dados do gráfico estão vazios. Verifique os filtros aplicados.")
        elif coluna_visualizacao_grafico not in df_grafico_periodo.columns:
            st.warning(f"⚠️ Coluna '{coluna_visualizacao_grafico}' não encontrada nos dados do gráfico.")
            st.warning(f"⚠️ Colunas disponíveis: {list(df_grafico_periodo.columns)[:10]}")
        else:
            # Criar placeholder para o gráfico (força renderização imediata)
            chart_placeholder = st.empty()
            
            # Criar gráfico (sem spinner para evitar bloqueio de renderização)
            try:
                if 'Período' not in df_grafico_periodo.columns:
                    chart_placeholder.error("❌ Coluna 'Período' não encontrada nos dados do gráfico.")
                elif df_grafico_periodo[coluna_visualizacao_grafico].isna().all():
                    chart_placeholder.warning("⚠️ Todos os valores na coluna são NaN. Verifique os dados.")
                else:
                    grafico_periodo = create_period_chart(
                        df_grafico_periodo, coluna_visualizacao_grafico, tipo_visualizacao,
                        df_budget_filtrado, df_budget_vol_filtrado, df_volume_real_filtrado,
                        df_real_original_grafico,  # Dados originais com 'Custo' para calcular FLEX
                        moeda_simbolo,  # Passar símbolo da moeda para o gráfico
                        debug=False,
                        debug_context=""
                    )
                    if grafico_periodo is not None:
                        # Exibir gráfico no placeholder (renderização imediata)
                        chart_placeholder.altair_chart(grafico_periodo, use_container_width=True)
                    else:
                        chart_placeholder.warning("⚠️ O gráfico não pôde ser criado. Verifique os dados e filtros aplicados.")
            except Exception as e:
                import traceback
                chart_placeholder.error(f"❌ Erro ao criar gráfico: {str(e)}")
                chart_placeholder.code(traceback.format_exc())
        
        # Tabela: Análise Flex Bud por Categoria
        if df_budget_filtrado is not None and df_budget_vol_filtrado is not None and df_volume_real_filtrado is not None:
            st.markdown("---")
            # Adicionar elemento com ID para scroll
            st.markdown('<div id="analise-flex-bud-por-categoria"></div>', unsafe_allow_html=True)
            st.subheader("📊 Análise Flex por Categoria")
            
            # Verificar se temos coluna 'Custo' nos dados
            tem_custo_real = False
            if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns:
                tem_custo_real = True
            elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns:
                tem_custo_real = True
            
            if 'Custo' in df_budget_filtrado.columns and tem_custo_real:
                # 🔧 CORREÇÃO CRÍTICA: Preparar dados reais para a tabela
                # IMPORTANTE: No modo CPU, precisamos de dados com Total em Custo Total (não em CPU)
                # Priorizar df_real_original_grafico que vem diretamente de df_total (sem processamento de CPU)
                if df_real_original_grafico is not None and 'Custo' in df_real_original_grafico.columns and 'Custo FP' in df_real_original_grafico.columns:
                    df_real_tabela = df_real_original_grafico.copy()
                elif 'df_filtrado' in locals() and df_filtrado is not None and 'Custo' in df_filtrado.columns and 'Custo FP' in df_filtrado.columns:
                    # Usar df_filtrado que tem Total em Custo Total (sem processamento de CPU)
                    df_real_tabela = df_filtrado.copy()
                elif df_grafico_periodo is not None and 'Custo' in df_grafico_periodo.columns and 'Custo FP' in df_grafico_periodo.columns:
                    # Fallback: usar df_grafico_periodo se tiver Total (pode estar em CPU, mas vamos verificar)
                    # Se estiver em modo CPU e df_grafico_periodo tem CPU mas não Total, não usar
                    df_real_tabela = df_grafico_periodo.copy()
                else:
                    df_real_tabela = None
                
                # 🔧 CORREÇÃO CRÍTICA: Aplicar filtros do gráfico (Oficina e Veículo) aos dados reais da tabela
                # Isso garante que a tabela responda aos filtros do gráfico
                if df_real_tabela is not None:
                    # Aplicar filtro de Oficina do gráfico
                    if 'Oficina' in df_real_tabela.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de Veículo do gráfico
                    if 'Veículo' in df_real_tabela.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_real_tabela = df_real_tabela[
                                df_real_tabela['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                
                if df_real_tabela is None or len(df_real_tabela) == 0:
                    st.info("ℹ️ Não há dados reais disponíveis para criar a tabela Flex Bud.")
                elif 'Custo' not in df_real_tabela.columns:
                    st.error("❌ Erro: df_real_tabela não tem coluna 'Custo'. Verifique a origem dos dados.")
                elif 'Custo FP' not in df_real_tabela.columns:
                    st.error("❌ Erro: df_real_tabela não tem coluna 'Custo FP'. Verifique a origem dos dados.")
                else:
                    # Agrupar dados reais por Custo, Type 05, Type 06, Account (se existir)
                    colunas_agrupamento = ['Custo']
                    if 'Type 05' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 05')
                    if 'Type 06' in df_real_tabela.columns:
                        colunas_agrupamento.append('Type 06')
                    if 'Account' in df_real_tabela.columns:
                        colunas_agrupamento.append('Account')
                    
                    # 🔧 CORREÇÃO: Calcular Flex Bud POR PERÍODO primeiro (mesma lógica do gráfico)
                    # Incluir Período no agrupamento para calcular por período
                    colunas_agrupamento_com_periodo = colunas_agrupamento.copy()
                    if 'Período' in df_real_tabela.columns:
                        colunas_agrupamento_com_periodo.append('Período')
                    if 'Ano' in df_real_tabela.columns:
                        colunas_agrupamento_com_periodo.append('Ano')
                    
                    # 🔧 VERIFICAÇÃO: Garantir que df_real_tabela tem Total em Custo Total (não em CPU)
                    # Se df_real_tabela tem coluna 'CPU' mas não 'Custo FP', há problema
                    if 'Custo FP' not in df_real_tabela.columns:
                        st.error("❌ Erro: df_real_tabela não tem coluna 'Custo FP'. Verifique a origem dos dados.")
                        df_real_agrupado = pd.DataFrame()
                    else:
                        # Normalizar o rótulo de Custo para garantir governança (Fixo é fixo; Variável 100% variável)
                        df_real_tabela = df_real_tabela.copy()
                        df_real_tabela['Custo'] = df_real_tabela['Custo'].apply(_normalizar_rotulo_custo)
                        df_budget_filtrado = df_budget_filtrado.copy()
                        if 'Custo' in df_budget_filtrado.columns:
                            df_budget_filtrado['Custo'] = df_budget_filtrado['Custo'].apply(_normalizar_rotulo_custo)
                        # Normalizar Período para evitar mismatch (ex: 'janeiro' vs 'Janeiro')
                        mapeamento_meses = {
                            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                        }

                        def _normalizar_periodo_local(periodo):
                            if pd.isna(periodo):
                                return periodo
                            periodo_str = str(periodo).strip()
                            for mes_min, mes_cap in mapeamento_meses.items():
                                if periodo_str.lower() == mes_min.lower():
                                    return mes_cap
                            return periodo_str

                        if 'Período' in df_real_tabela.columns:
                            df_real_tabela['Período'] = df_real_tabela['Período'].apply(_normalizar_periodo_local)
                        if 'Período' in df_budget_filtrado.columns:
                            df_budget_filtrado['Período'] = df_budget_filtrado['Período'].apply(_normalizar_periodo_local)
                        if df_volume_real_filtrado is not None and 'Período' in df_volume_real_filtrado.columns:
                            df_volume_real_filtrado = df_volume_real_filtrado.copy()
                            df_volume_real_filtrado['Período'] = df_volume_real_filtrado['Período'].apply(_normalizar_periodo_local)
                        if df_budget_vol_filtrado is not None and 'Período' in df_budget_vol_filtrado.columns:
                            df_budget_vol_filtrado = df_budget_vol_filtrado.copy()
                            df_budget_vol_filtrado['Período'] = df_budget_vol_filtrado['Período'].apply(_normalizar_periodo_local)

                        # Agrupar dados reais por categoria E período
                        # IMPORTANTE: Não verificar se Total está zerado antes de agrupar, pois pode haver
                        # valores positivos e negativos que se cancelam no total, mas são válidos por categoria
                        df_real_agrupado = df_real_tabela.groupby(colunas_agrupamento_com_periodo)['Custo FP'].sum().reset_index()
                    
                    # Agrupar dados de budget por categoria E período
                    colunas_budget_periodo = [col for col in colunas_agrupamento_com_periodo if col in df_budget_filtrado.columns]
                    df_budget_agrupado = df_budget_filtrado.groupby(colunas_budget_periodo)['Custo FP'].sum().reset_index()
                    
                    # 🔧 CORREÇÃO: Tab TC Ext usa dados de BUDGET reais (df_budget_agrupado)
                    # Aplicar mesmos filtros de categoria nos volumes (real e budget)
                    df_vol_real_agrupado = pd.DataFrame()
                    if 'Volume' in df_volume_real_filtrado.columns:
                        df_vol_real_para_agrupar = df_volume_real_filtrado.copy()
                        for col_filtro in ['Type 05', 'Type 06', 'Account']:
                            if col_filtro in df_vol_real_para_agrupar.columns and col_filtro in df_real_tabela.columns:
                                valores_presentes = df_real_tabela[col_filtro].dropna().unique()
                                if len(valores_presentes) > 0:
                                    df_vol_real_para_agrupar = df_vol_real_para_agrupar[
                                        df_vol_real_para_agrupar[col_filtro].isin(valores_presentes)
                                    ]
                        if 'Ano' in df_vol_real_para_agrupar.columns and 'Ano' in df_real_tabela.columns and 'Período' in df_vol_real_para_agrupar.columns:
                            df_vol_real_agrupado = df_vol_real_para_agrupar.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        elif 'Período' in df_vol_real_para_agrupar.columns:
                            df_vol_real_agrupado = df_vol_real_para_agrupar.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            volume_total = df_vol_real_para_agrupar['Volume'].sum()
                            df_vol_real_agrupado = pd.DataFrame({'Volume': [volume_total]})
                    
                    df_vol_budget_agrupado = pd.DataFrame()
                    if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                        df_vol_budget_para_agrupar = df_budget_vol_filtrado.copy()
                        for col_filtro in ['Type 05', 'Type 06', 'Account']:
                            if col_filtro in df_vol_budget_para_agrupar.columns and col_filtro in df_budget_filtrado.columns:
                                valores_presentes = df_budget_filtrado[col_filtro].dropna().unique()
                                if len(valores_presentes) > 0:
                                    df_vol_budget_para_agrupar = df_vol_budget_para_agrupar[
                                        df_vol_budget_para_agrupar[col_filtro].isin(valores_presentes)
                                    ]
                        if 'Ano' in df_vol_budget_para_agrupar.columns and 'Ano' in df_budget_filtrado.columns and 'Período' in df_vol_budget_para_agrupar.columns:
                            df_vol_budget_agrupado = df_vol_budget_para_agrupar.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                        elif 'Período' in df_vol_budget_para_agrupar.columns:
                            df_vol_budget_agrupado = df_vol_budget_para_agrupar.groupby('Período')['Volume'].sum().reset_index()
                        else:
                            volume_total = df_vol_budget_para_agrupar['Volume'].sum()
                            df_vol_budget_agrupado = pd.DataFrame({'Volume': [volume_total]})
                    
                    # Merge Real + Budget custos
                    df_tabela_flex = df_real_agrupado.merge(
                        df_budget_agrupado,
                        on=colunas_agrupamento_com_periodo,
                        how='outer',
                        suffixes=('', '_Budget')
                    )
                    df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'].fillna(0)
                    df_tabela_flex['Total_Budget'] = df_tabela_flex['Total_Budget'].fillna(0)
                    df_tabela_flex['Budget_Total_Custo'] = df_tabela_flex['Total_Budget']
                    df_tabela_flex['Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                    
                    # Merge com volumes (real e budget) por Período (+ Ano quando existir)
                    chaves_merge_vol = ['Período']
                    if 'Ano' in df_tabela_flex.columns and len(df_vol_real_agrupado) > 0 and 'Ano' in df_vol_real_agrupado.columns:
                        chaves_merge_vol = ['Ano', 'Período']

                    if len(df_vol_real_agrupado) > 0 and all(c in df_vol_real_agrupado.columns for c in chaves_merge_vol):
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_real_agrupado[chaves_merge_vol + ['Volume']].rename(columns={'Volume': 'Volume_Real'}),
                            on=chaves_merge_vol,
                            how='left'
                        )
                        df_tabela_flex['Volume_Real'] = df_tabela_flex['Volume_Real'].fillna(0)
                    elif len(df_vol_real_agrupado) > 0:
                        volume_total_real = df_vol_real_agrupado['Volume'].sum() if 'Volume' in df_vol_real_agrupado.columns else 0
                        df_tabela_flex['Volume_Real'] = volume_total_real
                    else:
                        df_tabela_flex['Volume_Real'] = 0
                    
                    if len(df_vol_budget_agrupado) > 0 and all(c in df_vol_budget_agrupado.columns for c in chaves_merge_vol):
                        df_tabela_flex = df_tabela_flex.merge(
                            df_vol_budget_agrupado[chaves_merge_vol + ['Volume']].rename(columns={'Volume': 'Volume_Budget'}),
                            on=chaves_merge_vol,
                            how='left'
                        )
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex['Volume_Budget'].fillna(0)
                    elif len(df_vol_budget_agrupado) > 0:
                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum() if 'Volume' in df_vol_budget_agrupado.columns else 0
                        df_tabela_flex['Volume_Budget'] = volume_total_budget
                    else:
                        df_tabela_flex['Volume_Budget'] = df_tabela_flex.get('Volume_Real', 0)
                    
                    # Calcular Flex Bud usando operações vetorizadas (muito mais rápido)
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        # Calcular Flex Bud em Custo Total primeiro
                        # Fixo: Flex Bud = Budget
                        # Não-Fixo (Variável + Outros): Flex Bud = Budget * (Volume Real / Volume Budget)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operações vetorizadas ao invés de apply (muito mais rápido)
                        # 🔒 Governança: Fixo não flexibiliza (robusto a variações de texto)
                        mask_fixo = _mask_custo_fixo(df_tabela_flex['Custo']) if 'Custo' in df_tabela_flex.columns else pd.Series(False, index=df_tabela_flex.index)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(mask_fixo, 0)
                        df_tabela_flex['_Flex_Bud_NaoFixo'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~mask_fixo, 0)
                        
                        df_tabela_flex['_Flex_Bud_Total_Custo'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_NaoFixo']
                        
                        # Converter para CPU
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Total_Custo'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['Flex BUD'].fillna(0)
                        
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['BUD'] = df_tabela_flex['BUD'].fillna(0)
                        
                        df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                        df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'].fillna(0)
                        
                        # Guardar valores para agregação
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['_Flex_Bud_Total_Custo']
                        # 🔧 CORREÇÃO: _Total_Custo_Total deve ser o Total em Custo Total (antes da conversão para CPU)
                        # Total já está em CPU, então precisamos reverter multiplicando por Volume_Real
                        # Mas Volume_Real é o mesmo para todas as categorias do mesmo período (volume total do período)
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Custo FP'] * df_tabela_flex['Volume_Real']  # Reverter para Custo Total
                    else:
                        # Custo Total
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)
                        
                        # Usar operações vetorizadas ao invés de apply (muito mais rápido)
                        # 🔒 Governança: Fixo não flexibiliza
                        mask_fixo = _mask_custo_fixo(df_tabela_flex['Custo']) if 'Custo' in df_tabela_flex.columns else pd.Series(False, index=df_tabela_flex.index)
                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(mask_fixo, 0)
                        df_tabela_flex['_Flex_Bud_NaoFixo'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~mask_fixo, 0)
                        
                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_NaoFixo']
                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo']
                        
                        # Guardar valores para agregação
                        df_tabela_flex['_Flex_Bud_Total'] = df_tabela_flex['Flex BUD']
                        df_tabela_flex['_Total_Custo_Total'] = df_tabela_flex['Custo FP']
                    
                    # Guardar valores auxiliares
                    df_tabela_flex['_Budget_Total'] = df_tabela_flex['Budget_Total_Custo']
                    
                    # Calcular diferenças
                    df_tabela_flex['Flex Bud - BUD'] = df_tabela_flex['Flex BUD'] - df_tabela_flex['BUD']
                    df_tabela_flex['Total - Flex Bud'] = df_tabela_flex['Custo FP'] - df_tabela_flex['Flex BUD']
                    # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                    df_tabela_flex['Total / Flex Bud'] = df_tabela_flex.apply(
                        lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                        axis=1
                    )
                    
                    # Remover colunas auxiliares temporárias
                    colunas_remover_temp = ['Budget_Total', 'Budget_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_NaoFixo']
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        colunas_remover_temp.append('_Flex_Bud_Total_Custo')
                    df_tabela_flex = df_tabela_flex.drop(columns=[col for col in colunas_remover_temp if col in df_tabela_flex.columns])

                    # 🔧 FILTRAR: remover linhas totalmente zeradas/nulas (limpa o Resumo Geral)
                    if len(df_tabela_flex) > 0:
                        colunas_numericas_gerais = [
                            col for col in df_tabela_flex.columns
                            if pd.api.types.is_numeric_dtype(df_tabela_flex[col])
                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']
                        ]
                        if colunas_numericas_gerais:
                            df_tmp = df_tabela_flex[colunas_numericas_gerais].fillna(0)
                            df_tabela_flex = df_tabela_flex[df_tmp.abs().sum(axis=1) > 0.0001].copy()
                    
                    if len(df_tabela_flex) > 0:
                        
                        # Seletor de período (linha superior)
                        if 'Período' in df_real_tabela.columns:
                            # 🔧 CORREÇÃO: não limitar a meses do realizado.
                            # Usar união Real + Budget + Volume Budget para listar todos os meses.
                            periodos_set = set(df_real_tabela['Período'].dropna().astype(str).unique().tolist())
                            if 'df_budget_filtrado' in locals() and df_budget_filtrado is not None and 'Período' in df_budget_filtrado.columns:
                                periodos_set.update(df_budget_filtrado['Período'].dropna().astype(str).unique().tolist())
                            if 'df_budget_vol_filtrado' in locals() and df_budget_vol_filtrado is not None and 'Período' in df_budget_vol_filtrado.columns:
                                periodos_set.update(df_budget_vol_filtrado['Período'].dropna().astype(str).unique().tolist())
                            # Garantir todos os meses sempre
                            periodos_set.update(ORDEM_MESES)
                            periodos_disponiveis = sorted([p for p in periodos_set if p and p != 'Todos'])
                            # Ordenar meses cronologicamente
                            meses_ordenados = []
                            outros_periodos = []
                            for periodo in periodos_disponiveis:
                                periodo_lower = str(periodo).lower()
                                if periodo_lower in ORDEM_MESES:
                                    meses_ordenados.append(periodo)
                                else:
                                    outros_periodos.append(periodo)
                            
                            meses_ordenados.sort(
                                key=lambda x: ORDEM_MESES.index(str(x).lower())
                                if str(x).lower() in ORDEM_MESES else 999
                            )
                            periodos_ordenados = meses_ordenados + outros_periodos
                            
                            # Novo filtro de períodos - versão simplificada
                            periodo_tabela_key = "filtro_periodo_tabela_flex"
                            
                            # Adicionar opção "Todos" no início da lista
                            opcoes_com_todos = ["Todos"] + periodos_ordenados
                            
                            # Inicializar session_state se necessário
                            if periodo_tabela_key not in st.session_state:
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Validar valores salvos
                            periodos_salvos = st.session_state[periodo_tabela_key]
                            periodos_validos = [p for p in periodos_salvos if p in opcoes_com_todos]
                            
                            # Se não houver períodos válidos, resetar para "Todos"
                            if not periodos_validos:
                                periodos_validos = ["Todos"]
                                st.session_state[periodo_tabela_key] = ["Todos"]
                            
                            # Adicionar CSS simples para prevenir scroll automático
                            st.markdown("""
                            <style>
                                /* Prevenir scroll automático do Streamlit */
                                html {
                                    scroll-behavior: auto !important;
                                }
                                /* Prevenir foco automático que causa scroll */
                                [data-testid="stMultiSelect"] input:focus {
                                    scroll-margin: 0 !important;
                                }
                                /* Prevenir scroll quando o multiselect recebe foco */
                                [data-testid="stMultiSelect"] {
                                    scroll-margin: 0 !important;
                                }
                            </style>
                            """, unsafe_allow_html=True)
                            
                            # Criar o multiselect DEPOIS do JavaScript
                            # 🔧 CORREÇÃO: Remover 'default' e usar apenas 'key' para evitar conflito
                            # O Streamlit automaticamente sincroniza o valor do widget com session_state[key]
                            periodos_tabela_raw = st.multiselect(
                                "📅 **Período(s):**",
                                opcoes_com_todos,
                                key=periodo_tabela_key
                            )
                            
                            # Atualizar session_state com o valor selecionado (caso tenha mudado)
                            if periodos_tabela_raw != st.session_state[periodo_tabela_key]:
                                st.session_state[periodo_tabela_key] = periodos_tabela_raw
                            
                            # Processar seleção
                            if "Todos" in periodos_tabela_raw:
                                # Se "Todos" está selecionado, selecionar todos os períodos para filtro
                                periodos_tabela = periodos_ordenados.copy()
                            else:
                                # Se "Todos" não está selecionado, usar apenas os períodos selecionados
                                periodos_tabela = [p for p in periodos_tabela_raw if p != "Todos"]
                            
                            # Se nenhum período foi selecionado, usar todos (mas mostrar apenas "Todos")
                            if not periodos_tabela:
                                periodos_tabela = periodos_ordenados.copy()
                        else:
                            periodos_tabela = []
                        
                        # Filtrar df_tabela_flex por períodos selecionados
                        # Inicializar variáveis
                        usar_colunas_por_periodo = False
                        periodos_ordenados_selecao = []
                        
                        if len(periodos_tabela) > 0 and 'Período' in df_tabela_flex.columns and len(df_tabela_flex) > 0:
                            # 🔧 IMPORTANTE: Salvar dados originais ANTES de filtrar (para usar em colunas por período)
                            df_tabela_flex_original = df_tabela_flex.copy()
                            
                            df_tabela_flex = df_tabela_flex[df_tabela_flex['Período'].isin(periodos_tabela)].copy()
                            
                            # 🔧 CRÍTICO: Salvar df_tabela_flex DEPOIS do filtro de período, mas ANTES de qualquer transformação
                            # Esta versão tem as colunas BUD, Flex BUD, Total originais e já está filtrada pelos períodos selecionados
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()
                            
                            # 🔧 NOVA LÓGICA: Se há múltiplos períodos, criar colunas separadas por período
                            # Manter a ordem de seleção dos períodos (periodos_tabela_raw mantém a ordem)
                            if len(periodos_tabela) > 1:
                                # Manter a ordem de seleção (usar periodos_tabela_raw se disponível, senão usar periodos_tabela)
                                if 'periodos_tabela_raw' in locals() and len(periodos_tabela_raw) > 0:
                                    periodos_ordenados_selecao = [p for p in periodos_tabela_raw if p != "Todos" and p in periodos_tabela]
                                else:
                                    periodos_ordenados_selecao = periodos_tabela.copy()
                                
                                # Se ainda não temos a ordem correta, usar periodos_tabela
                                if not periodos_ordenados_selecao:
                                    periodos_ordenados_selecao = periodos_tabela.copy()
                                
                                # Criar flag para indicar que vamos usar colunas por período
                                usar_colunas_por_periodo = True
                            else:
                                periodos_ordenados_selecao = periodos_tabela.copy()
                                usar_colunas_por_periodo = False
                            
                            # 🔧 CORREÇÃO CRÍTICA: Agregar corretamente quando há 1 ou múltiplos períodos
                            # (mesma lógica do gráfico - calcular Flex Bud por período primeiro, depois agregar)
                            # O gráfico sempre soma todas as categorias primeiro e depois calcula Flex Bud Total
                            # A tabela deve fazer o mesmo: somar _Flex_Bud_Total de todas as categorias e dividir pelo volume total
                            if len(periodos_tabela) >= 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "CPU (Custo por Unidade)":
                                # 🔧 CORREÇÃO CRÍTICA: O gráfico calcula Flex Bud por período, então devemos fazer o mesmo
                                # 1. Calcular Flex Bud por período e categoria (já feito acima)
                                # 2. Agregar por categoria somando Flex Bud Total e Volume Total
                                
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # 🔧 CORREÇÃO CRÍTICA: Usar volume TOTAL de todos os períodos selecionados (não por categoria)
                                # O gráfico calcula por período usando volume total do período, então devemos usar o mesmo aqui
                                # IMPORTANTE: O gráfico agrupa volumes por Período ANTES de calcular Flex BUD
                                # A tabela já tem df_vol_real_agrupado e df_vol_budget_agrupado que foram agrupados por Período
                                # Então devemos usar esses DataFrames agrupados para garantir consistência
                                if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                    # Usar o DataFrame já agrupado por Período (igual ao gráfico)
                                    volume_total_real = df_vol_real_agrupado[df_vol_real_agrupado['Período'].isin(periodos_tabela)]['Volume'].sum()
                                elif 'Período' in df_volume_real_filtrado.columns:
                                    # Fallback: agrupar por Período primeiro (igual ao gráfico), depois filtrar e somar
                                    df_vol_real_por_periodo = df_volume_real_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                    volume_total_real = df_vol_real_por_periodo[df_vol_real_por_periodo['Período'].isin(periodos_tabela)]['Volume'].sum()
                                else:
                                    volume_total_real = df_volume_real_filtrado['Volume'].sum()
                                
                                # Volume Budget: usar os dados de volume de BUDGET (não o real)
                                if len(df_vol_budget_agrupado) > 0 and 'Volume' in df_vol_budget_agrupado.columns:
                                    if 'Período' in df_vol_budget_agrupado.columns and len(periodos_tabela) > 0:
                                        if 'Ano' in df_vol_budget_agrupado.columns and 'Ano' in df_tabela_flex.columns:
                                            # Se houver Ano, filtrar também pelo Ano presente em df_tabela_flex
                                            anos_sel = df_tabela_flex['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex.columns else []
                                            df_tmp = df_vol_budget_agrupado.copy()
                                            if anos_sel:
                                                df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                            volume_total_budget = df_tmp[df_tmp['Período'].isin(periodos_tabela)]['Volume'].sum()
                                        else:
                                            volume_total_budget = df_vol_budget_agrupado[df_vol_budget_agrupado['Período'].isin(periodos_tabela)]['Volume'].sum()
                                    else:
                                        volume_total_budget = df_vol_budget_agrupado['Volume'].sum()
                                elif df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                                    if 'Período' in df_budget_vol_filtrado.columns and len(periodos_tabela) > 0:
                                        if 'Ano' in df_budget_vol_filtrado.columns and 'Ano' in df_tabela_flex.columns:
                                            anos_sel = df_tabela_flex['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex.columns else []
                                            df_tmp = df_budget_vol_filtrado.copy()
                                            if anos_sel:
                                                df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                            volume_total_budget = df_tmp[df_tmp['Período'].isin(periodos_tabela)]['Volume'].sum()
                                        else:
                                            volume_total_budget = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].isin(periodos_tabela)]['Volume'].sum()
                                    else:
                                        volume_total_budget = df_budget_vol_filtrado['Volume'].sum()
                                else:
                                    volume_total_budget = volume_total_real
                                
                                # 🔧 CORREÇÃO: Agrupar por categoria (sem período) - somar valores em Custo Total
                                # IMPORTANTE: Somar _Flex_Bud_Total que já está em Custo Total (calculado por período)
                                # df_tabela_flex_original já foi criado antes do filtro (linha acima)
                                
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    '_Flex_Bud_Total': 'sum',  # Flex Bud Total em Custo Total (soma de todos os períodos)
                                    '_Total_Custo_Total': 'sum',  # Total em Custo Total (soma de todos os períodos)
                                    '_Budget_Total': 'sum'  # Budget em Custo Total (soma de todos os períodos)
                                }).reset_index()
                                
                                # 🔧 CORREÇÃO CRÍTICA: Usar volume TOTAL de todos os períodos (não somar por categoria)
                                # O gráfico usa volume total por período, então quando agregamos múltiplos períodos,
                                # devemos usar a SOMA dos volumes de todos os períodos selecionados
                                df_agregado['_Volume_Real'] = volume_total_real
                                df_agregado['_Volume_Budget'] = volume_total_budget
                                
                                # Recalcular CPU usando operações vetorizadas (muito mais rápido)
                                # Flex BUD CPU = (Soma de Flex Bud Total de todos os períodos) / (Soma de Volume Real de todos os períodos)
                                df_agregado['Flex BUD'] = (df_agregado['_Flex_Bud_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['Custo FP'] = (df_agregado['_Total_Custo_Total'] / df_agregado['_Volume_Real'].replace(0, 1)).fillna(0)
                                df_agregado['BUD'] = (df_agregado['_Budget_Total'] / df_agregado['_Volume_Budget'].replace(0, 1)).fillna(0)
                                
                                # Recalcular diferenças
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Custo FP'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # 🔧 CORREÇÃO: Manter colunas auxiliares para o resumo geral recalcular corretamente
                                # Não remover ainda - serão removidas após o cálculo do resumo
                                # colunas_remover = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget']
                                # df_agregado = df_agregado.drop(columns=[col for col in colunas_remover if col in df_agregado.columns])
                                
                                # Se há múltiplos períodos e devemos usar colunas por período, criar estrutura pivot
                                if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1 and 'Período' in df_tabela_flex_original.columns:
                                    # Usar dados originais antes da agregação (ainda tem Período)
                                    df_tabela_flex_por_periodo = df_tabela_flex_original.copy()
                                    
                                    # Criar pivot table com períodos como colunas
                                    colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex_por_periodo.columns]
                                    
                                    # Criar pivot para cada métrica
                                    df_pivot_total = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='Custo FP',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_flex = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='Flex BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    df_pivot_bud = df_tabela_flex_por_periodo.pivot_table(
                                        index=colunas_agrup_final if colunas_agrup_final else ['Type 06'],
                                        columns='Período',
                                        values='BUD',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
                                    
                                    # Reorganizar colunas na ordem de seleção
                                    periodos_ordenados_selecao_clean = [p for p in periodos_ordenados_selecao if p in df_pivot_total.columns]
                                    
                                    if periodos_ordenados_selecao_clean:
                                        df_pivot_total = df_pivot_total[periodos_ordenados_selecao_clean]
                                        df_pivot_flex = df_pivot_flex[periodos_ordenados_selecao_clean]
                                        df_pivot_bud = df_pivot_bud[periodos_ordenados_selecao_clean]
                                    
                                    # Criar DataFrame final com colunas reorganizadas
                                    df_final = df_pivot_total.reset_index()
                                    
                                    # Adicionar colunas na ordem especificada
                                    primeiro_periodo = periodos_ordenados_selecao_clean[0] if periodos_ordenados_selecao_clean else None
                                    primeiro_periodo_abrev = formatar_periodo_abreviado(primeiro_periodo) if primeiro_periodo else ""
                                    
                                    # Reorganizar colunas na ordem exata especificada
                                    # Ordem: Set/24, Flex set/24 - set/24, Flex set/24, Out/24 - Flex set/24, out/24, % out/24/flex set/24
                                    
                                    # Remover colunas de período do pivot (vamos criar novas colunas)
                                    for col in df_pivot_total.columns:
                                        if col in df_final.columns:
                                            df_final = df_final.drop(columns=[col])
                                    
                                    # Primeiro período: Set/24, Flex set/24 (removendo coluna redundante)
                                    if primeiro_periodo and primeiro_periodo in df_pivot_total.columns:
                                        df_final[f"{primeiro_periodo_abrev}"] = df_pivot_total[primeiro_periodo].values
                                        df_final[f"Flex {primeiro_periodo_abrev.lower()}"] = df_pivot_flex[primeiro_periodo].values
                                    
                                    # Demais períodos: Out/24 - Flex set/24, out/24, % out/24/flex set/24
                                    for periodo in periodos_ordenados_selecao_clean[1:]:
                                        periodo_abrev = formatar_periodo_abreviado(periodo)
                                        if periodo in df_pivot_total.columns and primeiro_periodo and primeiro_periodo in df_pivot_flex.columns:
                                            df_final[f"{periodo_abrev} - Flex {primeiro_periodo_abrev.lower()}"] = (df_pivot_total[periodo] - df_pivot_flex[primeiro_periodo]).values
                                            df_final[f"{periodo_abrev.lower()}"] = df_pivot_total[periodo].values
                                            # Calcular percentual
                                            df_final[f"% {periodo_abrev.lower()}/flex {primeiro_periodo_abrev.lower()}"] = (
                                                (df_pivot_total[periodo] / df_pivot_flex[primeiro_periodo].replace(0, 1)) * 100
                                            ).fillna(0).values
                                    
                                    df_tabela_flex = df_final
                                else:
                                    # Adicionar Período: se houver apenas 1 período, manter o nome; se múltiplos, mostrar lista
                                    if len(periodos_tabela) == 1:
                                        df_agregado['Período'] = periodos_tabela[0]
                                    else:
                                        df_agregado['Período'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} períodos"
                                    
                                    df_tabela_flex = df_agregado
                            elif len(periodos_tabela) > 1 and len(df_tabela_flex) > 0 and tipo_visualizacao == "Custo Total":
                                # Para Custo Total: apenas somar por categoria (sem período)
                                colunas_agrup_final = [col for col in colunas_agrupamento if col in df_tabela_flex.columns]
                                
                                # Agrupar por categoria (sem período) e somar
                                df_agregado = df_tabela_flex.groupby(colunas_agrup_final).agg({
                                    'BUD': 'sum',
                                    'Flex BUD': 'sum',
                                    'Custo FP': 'sum'
                                }).reset_index()
                                
                                # Recalcular diferenças
                                df_agregado['Flex Bud - BUD'] = df_agregado['Flex BUD'] - df_agregado['BUD']
                                df_agregado['Total - Flex Bud'] = df_agregado['Custo FP'] - df_agregado['Flex BUD']
                                # Calcular Total / Flex Bud: Total dividido por Flex BUD (resultado em decimal, será convertido para % na formatação)
                                df_agregado['Total / Flex Bud'] = df_agregado.apply(
                                    lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                    axis=1
                                )
                                
                                # Adicionar Período como "Todos" ou lista de períodos
                                df_agregado['Período'] = ", ".join(periodos_tabela) if len(periodos_tabela) <= 3 else f"{len(periodos_tabela)} períodos"
                                
                                df_tabela_flex = df_agregado
                        else:
                            # Se não houver filtro de período, usar df_tabela_flex diretamente
                            df_tabela_flex_para_resumo = df_tabela_flex.copy()
                        
                        # 🔧 CORREÇÃO: Remover colunas auxiliares da tabela principal (para exibição)
                        # df_tabela_flex_para_resumo já foi salvo DEPOIS do filtro de período, mas ANTES das transformações
                        colunas_auxiliares = ['_Flex_Bud_Total', '_Volume_Real', '_Total_Custo_Total', '_Budget_Total', '_Volume_Budget', '_Flex_Bud_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_NaoFixo', 'Volume_Real', 'Volume_Budget', 'Total_Budget']
                        colunas_para_remover = [col for col in colunas_auxiliares if col in df_tabela_flex.columns]
                        
                        # Remover colunas auxiliares da tabela principal (para exibição)
                        if colunas_para_remover:
                            df_tabela_flex = df_tabela_flex.drop(columns=colunas_para_remover)
                        
                        # Selecionador de visualização (linha inferior)
                        modo_tabela_flex = st.radio(
                            "📊 **Visualização:**",
                            ["Fixo/Variável", "Total"],
                            index=0,
                            horizontal=True,
                            key="modo_tabela_flex_bud_tc"
                        )
                        
                        # Resumo geral (fora dos expanders)
                        # 🔧 CORREÇÃO: Usar DataFrame com colunas auxiliares para recalcular corretamente
                        if len(df_tabela_flex) > 0:
                            # 🔧 CORREÇÃO CRÍTICA: Obter volumes EXATAMENTE como o gráfico (mesmos DataFrames)
                            # O gráfico usa df_vol_real_agrupado e df_vol_budget_agrupado agrupados por Período
                            # IMPORTANTE: Usar os mesmos DataFrames e a mesma lógica do gráfico
                            volume_real_para_resumo = 0.0
                            volume_budget_para_resumo = 0.0
                            
                            # Obter períodos selecionados (mesma lógica usada acima)
                            periodos_para_volume = periodos_tabela if 'periodos_tabela' in locals() else []
                            if not periodos_para_volume:
                                # Se não houver períodos selecionados, usar todos os períodos disponíveis
                                if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                    periodos_para_volume = df_vol_real_agrupado['Período'].unique().tolist()
                            
                            # Obter volumes dos mesmos DataFrames que o gráfico usa
                            if len(df_vol_real_agrupado) > 0 and 'Período' in df_vol_real_agrupado.columns:
                                # Usar o DataFrame já agrupado por Período (igual ao gráfico)
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_agrupado[df_vol_real_agrupado['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_agrupado['Volume'].sum()
                            elif 'Período' in df_volume_real_filtrado.columns:
                                # Fallback: agrupar por Período e somar (igual ao gráfico)
                                df_vol_real_temp = df_volume_real_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                if len(periodos_para_volume) > 0:
                                    volume_real_para_resumo = df_vol_real_temp[df_vol_real_temp['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                else:
                                    volume_real_para_resumo = df_vol_real_temp['Volume'].sum()
                            else:
                                volume_real_para_resumo = df_volume_real_filtrado['Volume'].sum() if 'Volume' in df_volume_real_filtrado.columns else 0.0
                            
                            # Volume Budget: usar df_budget_vol_filtrado (volume do budget, não do real)
                            if df_budget_vol_filtrado is not None and 'Volume' in df_budget_vol_filtrado.columns:
                                # Agrupar volume de budget por Período
                                if 'Ano' in df_budget_vol_filtrado.columns and 'Ano' in df_tabela_flex_para_resumo.columns and 'Período' in df_budget_vol_filtrado.columns:
                                    anos_sel = df_tabela_flex_para_resumo['Ano'].dropna().unique().tolist() if 'Ano' in df_tabela_flex_para_resumo.columns else []
                                    df_tmp = df_budget_vol_filtrado.copy()
                                    if anos_sel:
                                        df_tmp = df_tmp[df_tmp['Ano'].isin(anos_sel)]
                                    df_vol_budget_temp = df_tmp.groupby(['Ano', 'Período'])['Volume'].sum().reset_index()
                                    if len(periodos_para_volume) > 0:
                                        volume_budget_para_resumo = df_vol_budget_temp[df_vol_budget_temp['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                    else:
                                        volume_budget_para_resumo = df_vol_budget_temp['Volume'].sum()
                                elif 'Período' in df_budget_vol_filtrado.columns:
                                    df_vol_budget_temp = df_budget_vol_filtrado.groupby('Período')['Volume'].sum().reset_index()
                                    if len(periodos_para_volume) > 0:
                                        volume_budget_para_resumo = df_vol_budget_temp[df_vol_budget_temp['Período'].isin(periodos_para_volume)]['Volume'].sum()
                                    else:
                                        volume_budget_para_resumo = df_vol_budget_temp['Volume'].sum()
                                else:
                                    volume_budget_para_resumo = df_budget_vol_filtrado['Volume'].sum()
                            else:
                                # Fallback: se não houver volume de budget, usar volume real (comportamento antigo)
                                volume_budget_para_resumo = volume_real_para_resumo
                            
                            # 🔧 CORREÇÃO: Adaptar resumo para usar nomes das colunas dinâmicas (se usar_colunas_por_periodo)
                            if usar_colunas_por_periodo and len(periodos_ordenados_selecao) > 1:
                                # Obter nomes das colunas dinâmicas do DataFrame
                                primeiro_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[0]) if len(periodos_ordenados_selecao) > 0 else ""
                                segundo_periodo_abrev = formatar_periodo_abreviado(periodos_ordenados_selecao[1]) if len(periodos_ordenados_selecao) > 1 else ""
                                
                                # Criar resumo com nomes dinâmicos
                                linha_resumo_geral = {}
                                linha_resumo_geral_formatado = {}
                                
                                # Obter colunas numéricas do DataFrame
                                colunas_numericas = [col for col in df_tabela_flex_para_resumo.columns 
                                                    if pd.api.types.is_numeric_dtype(df_tabela_flex_para_resumo[col]) 
                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                
                                for col in colunas_numericas:
                                    valor = df_tabela_flex_para_resumo[col].sum()
                                    linha_resumo_geral[col] = valor
                                    
                                    # Formatar valor
                                    if col.startswith('%'):
                                        # Usar formatar_ratio_com_barra para percentuais (dividir por 100 pois está em %)
                                        if pd.notna(valor) and isinstance(valor, (int, float)):
                                            linha_resumo_geral_formatado[col] = formatar_ratio_com_barra(valor / 100)
                                        else:
                                            linha_resumo_geral_formatado[col] = "-"
                                    elif tipo_visualizacao == "CPU (Custo por Unidade)":
                                        linha_resumo_geral_formatado[col] = f"{valor:,.2f}"
                                    else:
                                        sufixo = ""
                                        if fator_conversao:
                                            if fator_conversao == "K (milhares)":
                                                sufixo = " K"
                                            elif fator_conversao == "M (Milhões)":
                                                sufixo = " M"
                                        linha_resumo_geral_formatado[col] = f"{valor:,.2f}{sufixo}"
                                
                                # Adicionar volumes
                                if pd.isna(volume_real_para_resumo) or volume_real_para_resumo is None:
                                    volume_real_para_resumo = 0.0
                                if pd.isna(volume_budget_para_resumo) or volume_budget_para_resumo is None:
                                    volume_budget_para_resumo = 0.0
                                
                                linha_resumo_geral['_Volume_Real_Calculo'] = float(volume_real_para_resumo)
                                linha_resumo_geral['_Volume_Budget_Calculo'] = float(volume_budget_para_resumo)
                                linha_resumo_geral_formatado['_Volume_Real_Calculo'] = f"{float(volume_real_para_resumo):,.0f}"
                                linha_resumo_geral_formatado['_Volume_Budget_Calculo'] = f"{float(volume_budget_para_resumo):,.0f}"
                            else:
                                # Usar função padrão para colunas fixas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(df_tabela_flex_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                
                                # 🔧 CORREÇÃO: Usar volumes do DataFrame (que já têm os filtros corretos aplicados)
                                # Os volumes em df_tabela_flex_para_resumo já foram calculados com todos os filtros
                                if 'Volume_Real' in df_tabela_flex_para_resumo.columns:
                                    # Volume Real: somar todos os volumes únicos (mesmo período tem mesmo volume)
                                    if 'Período' in df_tabela_flex_para_resumo.columns:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo.groupby('Período')['Volume_Real'].first().sum()
                                    else:
                                        volume_real_para_resumo = df_tabela_flex_para_resumo['Volume_Real'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_real_para_resumo = linha_resumo_geral.get('_Volume_Real_Calculo', 0.0)
                                
                                if 'Volume_Budget' in df_tabela_flex_para_resumo.columns:
                                    # Volume Budget: somar volumes únicos por período
                                    if 'Período' in df_tabela_flex_para_resumo.columns:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo.groupby('Período')['Volume_Budget'].first().sum()
                                    else:
                                        volume_budget_para_resumo = df_tabela_flex_para_resumo['Volume_Budget'].iloc[0] if len(df_tabela_flex_para_resumo) > 0 else 0.0
                                else:
                                    volume_budget_para_resumo = linha_resumo_geral.get('_Volume_Budget_Calculo', 0.0)
                                
                                # Garantir que os volumes sejam sempre números (não NaN ou None)
                                if pd.isna(volume_real_para_resumo) or volume_real_para_resumo is None:
                                    volume_real_para_resumo = 0.0
                                if pd.isna(volume_budget_para_resumo) or volume_budget_para_resumo is None:
                                    volume_budget_para_resumo = 0.0
                                
                                linha_resumo_geral['_Volume_Real_Calculo'] = float(volume_real_para_resumo)
                                linha_resumo_geral['_Volume_Budget_Calculo'] = float(volume_budget_para_resumo)
                                linha_resumo_geral_formatado['_Volume_Real_Calculo'] = f"{float(volume_real_para_resumo):,.0f}"
                                linha_resumo_geral_formatado['_Volume_Budget_Calculo'] = f"{float(volume_budget_para_resumo):,.0f}"
                            
                            st.markdown("### 📊 Resumo Geral")
                            # Exibir caixas de resumo com volumes
                            exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=True)
                            st.markdown("<br>", unsafe_allow_html=True)  # Pequeno espaço antes das tabelas
                        # Criar estrutura hierárquica com expanders
                        if modo_tabela_flex == "Fixo/Variável":
                            # 🔧 CORREÇÃO: Usar df_tabela_flex_para_resumo para cálculos de resumo (tem colunas originais)
                            # df_tabela_flex pode ter colunas por período (Jul, Ago, etc.) que não servem para resumo
                            df_para_resumo_fixo_variavel = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # Nível 1: Custo (Fixo/Variável) - separado
                            for custo in ['Fixo', 'Variável']:
                                df_custo = df_tabela_flex[df_tabela_flex['Custo'] == custo].copy()
                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                df_custo_para_resumo = df_para_resumo_fixo_variavel[df_para_resumo_fixo_variavel['Custo'] == custo].copy() if 'Custo' in df_para_resumo_fixo_variavel.columns else df_custo.copy()
                                
                                if len(df_custo) > 0:
                                    # 🔧 FILTRAR: Verificar se Custo tem valores não zerados
                                    colunas_numericas_custo_check = [col for col in df_custo.columns 
                                                                     if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                    if colunas_numericas_custo_check:
                                        df_custo_check = df_custo[colunas_numericas_custo_check].fillna(0)
                                        tem_valores_nao_zerados = (df_custo_check.abs().sum(axis=1) > 0.0001).any()
                                        if not tem_valores_nao_zerados:
                                            continue  # Pular Custo completamente zerado
                                    else:
                                        if 'Custo FP' in df_custo.columns:
                                            if df_custo['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                continue  # Pular Custo completamente zerado
                                    
                                    # Verificar se a coluna 'Custo FP' existe antes de acessá-la
                                    # 🔧 CORREÇÃO: Tentar usar df_custo_para_resumo primeiro (tem colunas originais)
                                    if 'Custo FP' in df_custo_para_resumo.columns:
                                        total_custo = df_custo_para_resumo['Custo FP'].sum()
                                    elif 'Custo FP' in df_custo.columns:
                                        total_custo = df_custo['Custo FP'].sum()
                                    else:
                                        # Se não houver coluna 'Custo FP', usar 0 ou calcular a partir de outras colunas
                                        total_custo = 0.0
                                    total_custo_formatado = f"{total_custo:,.2f}"
                                    
                                    with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=False):
                                        # Resumo do Custo (Fixo ou Variável)
                                        # 🔧 CORREÇÃO: Usar df_custo_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                        linha_resumo_custo, linha_resumo_custo_formatado = calcular_resumo_tabela_flex(df_custo_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                        exibir_caixas_resumo(linha_resumo_custo, linha_resumo_custo_formatado, tipo_visualizacao)
                                        st.markdown("---")
                                        
                                        # Nível 2: Type 05 (se existir)
                                        if 'Type 05' in df_custo.columns:
                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                                df_type05_para_resumo = df_custo_para_resumo[df_custo_para_resumo['Type 05'] == type05].copy() if 'Type 05' in df_custo_para_resumo.columns else df_type05.copy()
                                                
                                                if len(df_type05) > 0:
                                                    # 🔧 FILTRAR: Verificar se Type 05 tem valores não zerados
                                                    colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                                     if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                     and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                    if colunas_numericas_type05_check:
                                                        df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                                        tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                                        if not tem_valores_nao_zerados:
                                                            continue  # Pular Type 05 completamente zerado
                                                    else:
                                                        if 'Custo FP' in df_type05_para_resumo.columns:
                                                            if df_type05_para_resumo['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                continue  # Pular Type 05 completamente zerado
                                                        elif 'Custo FP' in df_type05.columns:
                                                            if df_type05['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                continue  # Pular Type 05 completamente zerado
                                                    
                                                    # Verificar se a coluna 'Custo FP' existe antes de acessá-la
                                                    if 'Custo FP' in df_type05_para_resumo.columns:
                                                        total_type05 = df_type05_para_resumo['Custo FP'].sum()
                                                    elif 'Custo FP' in df_type05.columns:
                                                        total_type05 = df_type05['Custo FP'].sum()
                                                    else:
                                                        total_type05 = 0.0
                                                    total_type05_formatado = f"{total_type05:,.2f}"
                                                    
                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                                        # Nível 3: Type 06 (se existir)
                                                        if 'Type 06' in df_type05.columns:
                                                            for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                                df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                                                df_type06_para_resumo = df_type05_para_resumo[df_type05_para_resumo['Type 06'] == type06].copy() if 'Type 06' in df_type05_para_resumo.columns else df_type06.copy()
                                                                
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
                                                                        # Se não há colunas numéricas, verificar se Total existe e é zero
                                                                        if 'Custo FP' in df_type06_para_resumo.columns:
                                                                            if df_type06_para_resumo['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                                continue  # Pular Type 06 completamente zerado
                                                                        elif 'Custo FP' in df_type06.columns:
                                                                            if df_type06['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                                continue  # Pular Type 06 completamente zerado
                                                                    
                                                                    # Verificar se a coluna 'Custo FP' existe antes de acessá-la
                                                                    if 'Custo FP' in df_type06_para_resumo.columns:
                                                                        total_type06 = df_type06_para_resumo['Custo FP'].sum()
                                                                    elif 'Custo FP' in df_type06.columns:
                                                                        total_type06 = df_type06['Custo FP'].sum()
                                                                    else:
                                                                        total_type06 = 0.0
                                                                    total_type06_formatado = f"{total_type06:,.2f}"
                                                                    
                                                                    # Nível 4: Account (se existir)
                                                                    if 'Account' in df_type06.columns:
                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                        # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
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
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar uma única tabela com todas as Accounts
                                                                                # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                                colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                                
                                                                                # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                                for col in colunas_numericas:
                                                                                    if col not in colunas_ordenadas:
                                                                                        colunas_ordenadas.append(col)
                                                                                
                                                                                colunas_display = colunas_id + colunas_ordenadas
                                                                                df_display = df_type06_filtrado[colunas_display].copy()

                                                                                # Exibição: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                                                                if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                                    df_display = df_display.drop(columns=['Ano'])

                                                                                # Visibilidade: remover linhas 100% zeradas/nulas somente na exibição
                                                                                df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                                    df_display,
                                                                                    colunas_ignorar=colunas_id,
                                                                                )
                                                                                
                                                                                # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                                                                for col in df_display.columns:
                                                                                    if col not in colunas_id:
                                                                                        # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, não percentual)
                                                                                        if col == 'Total / Flex Bud':
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                                        # Formatar percentuais de forma especial com barrinha
                                                                                        elif col.startswith('%'):
                                                                                            # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                            df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                                        # Formatar outras colunas numéricas
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
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # 🔧 CORREÇÃO: Usar versão para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                if 'Account' in df_type06_para_resumo.columns:
                                                                                    # Aplicar mesmo filtro de linhas zeradas na versão para resumo
                                                                                    colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                                if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                    if colunas_numericas_resumo:
                                                                                        df_type06_para_resumo_temp = df_type06_para_resumo[colunas_numericas_resumo].fillna(0)
                                                                                        df_type06_para_resumo_filtrado = df_type06_para_resumo[
                                                                                            df_type06_para_resumo_temp.abs().sum(axis=1) > 0.0001
                                                                                        ].copy()
                                                                                    else:
                                                                                        df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                else:
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                
                                                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_para_resumo_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                                
                                                                                # Exibir caixas de resumo
                                                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                                
                                                                                # Exibir tabela com resumo (todas as Accounts em uma única tabela)
                                                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                                st.markdown(html_table, unsafe_allow_html=True)
                                                                    else:
                                                                        # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                                        # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                                        colunas_numericas = [col for col in df_type06.columns 
                                                                                            if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                        if colunas_numericas:
                                                                            # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                            # Preencher nulos com 0 antes de calcular
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
                                                                            with st.container():
                                                                                st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                                # Criar tabela para este Type 06
                                                                                # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                                colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                                colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                                    if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'Período']]
                                                                                
                                                                                # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                                                colunas_ordenadas = []
                                                                                for col_ordem in ordem_colunas:
                                                                                    if col_ordem in colunas_numericas:
                                                                                        colunas_ordenadas.append(col_ordem)
                                                                                # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                                for col in colunas_numericas:
                                                                                    if col not in colunas_ordenadas:
                                                                                        colunas_ordenadas.append(col)
                                                                                
                                                                                colunas_display = colunas_id + colunas_ordenadas
                                                                                df_display = df_type06_filtrado[colunas_display].copy()
                                                                                
                                                                                # Formatar valores
                                                                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                                                                    if col in df_display.columns:
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
                                                                                
                                                                                # Formatar Total / Flex Bud com barra HTML
                                                                                if 'Total / Flex Bud' in df_display.columns:
                                                                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                                                
                                                                                # Calcular linha de resumo
                                                                                # 🔧 CORREÇÃO: Usar versão para resumo com colunas originais, mas aplicar mesmo filtro de linhas zeradas
                                                                                colunas_numericas_resumo = [col for col in df_type06_para_resumo.columns 
                                                                                                            if pd.api.types.is_numeric_dtype(df_type06_para_resumo[col]) 
                                                                                                            and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                                                if colunas_numericas_resumo:
                                                                                    df_type06_para_resumo_temp = df_type06_para_resumo[colunas_numericas_resumo].fillna(0)
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo[
                                                                                        df_type06_para_resumo_temp.abs().sum(axis=1) > 0.0001
                                                                                    ].copy()
                                                                                else:
                                                                                    df_type06_para_resumo_filtrado = df_type06_para_resumo.copy()
                                                                                
                                                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_para_resumo_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                                
                                                                                # Exibir caixas de resumo
                                                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                                
                                                                                # Exibir tabela com resumo
                                                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                                st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            # Sem Type 06: exibir diretamente Type 05
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            colunas_numericas_type05 = [col for col in df_type05.columns 
                                                                                        if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                        and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas_type05:
                                                                df_type05_temp = df_type05[colunas_numericas_type05].fillna(0)
                                                                df_type05 = df_type05[
                                                                    df_type05_temp.abs().sum(axis=1) > 0.0001
                                                                ].copy()
                                                            
                                                            # Criar tabela para este Type 05
                                                            # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                            colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                            colunas_numericas = [col for col in df_type05.columns 
                                                                                if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'Período']]
                                                            
                                                            # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                            ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                            colunas_ordenadas = []
                                                            for col_ordem in ordem_colunas:
                                                                if col_ordem in colunas_numericas:
                                                                    colunas_ordenadas.append(col_ordem)
                                                            # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                            for col in colunas_numericas:
                                                                if col not in colunas_ordenadas:
                                                                    colunas_ordenadas.append(col)
                                                            
                                                            colunas_display = colunas_id + colunas_ordenadas
                                                            df_display = df_type05[colunas_display].copy()

                                                            # Exibição: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                                            if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                df_display = df_display.drop(columns=['Ano'])

                                                            # Visibilidade: remover linhas 100% zeradas/nulas somente na exibição
                                                            df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                df_display,
                                                                colunas_ignorar=colunas_id,
                                                            )
                                                            
                                                            # Formatar valores
                                                            for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                                                if col in df_display.columns:
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
                                                            
                                                            # Formatar Total / Flex Bud com barra HTML
                                                            if 'Total / Flex Bud' in df_display.columns:
                                                                df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                            
                                                            # Calcular linha de resumo
                                                            linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type05, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                            
                                                            # Exibir caixas de resumo
                                                            exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                            
                                                            # Exibir tabela com resumo
                                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                            st.markdown(html_table, unsafe_allow_html=True)
                                        else:
                                            # Sem Type 05: exibir diretamente Custo
                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                            colunas_numericas_custo = [col for col in df_custo.columns 
                                                                       if pd.api.types.is_numeric_dtype(df_custo[col]) 
                                                                       and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                            if colunas_numericas_custo:
                                                df_custo_temp = df_custo[colunas_numericas_custo].fillna(0)
                                                df_custo = df_custo[
                                                    df_custo_temp.abs().sum(axis=1) > 0.0001
                                                ].copy()
                                            
                                            # Criar tabela para este Custo
                                            # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                            colunas_id = ['Custo'] if 'Custo' in df_custo.columns else []
                                            colunas_numericas = [col for col in df_custo.columns 
                                                                if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Account', 'Período']]
                                            colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas)
                                            colunas_display = colunas_id + colunas_ordenadas
                                            df_display = df_custo[colunas_display].copy()

                                            # Exibição: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                            if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                df_display = df_display.drop(columns=['Ano'])

                                            # Visibilidade: remover linhas 100% zeradas/nulas somente na exibição
                                            df_display = _remover_linhas_sem_valores_para_exibicao(
                                                df_display,
                                                colunas_ignorar=colunas_id,
                                            )
                                            
                                            # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                            for col in df_display.columns:
                                                if col not in colunas_id:
                                                    # Formatar percentuais de forma especial com barrinha
                                                    if col.startswith('%'):
                                                        # Usar formatar_ratio_com_barra para colunas de percentual (dividir por 100 pois está em %)
                                                        df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                    # Formatar outras colunas numéricas
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
                                            
                                            # Calcular linha de resumo
                                            linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_custo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                            
                                            # Exibir caixas de resumo
                                            exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                            
                                            # Exibir tabela com resumo
                                            html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                            st.markdown(html_table, unsafe_allow_html=True)
                        else:
                            # Modo "Total": não separar por Fixo/Variável
                            # Agrupar todos os dados sem separar por Custo
                            # Remover coluna Custo do agrupamento para exibição
                            df_tabela_total = df_tabela_flex.copy()
                            
                            # Verificar se df_tabela_total tem dados
                            if len(df_tabela_total) == 0:
                                st.warning("⚠️ Nenhum dado disponível para exibição no modo Total.")
                                df_tabela_total_agrupado = pd.DataFrame()
                            else:
                                # Agrupar por Type 05, Type 06, Account (se existirem) somando valores
                                colunas_agrupamento_total = []
                                if 'Type 05' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Type 05')
                                if 'Type 06' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Type 06')
                                if 'Account' in df_tabela_total.columns:
                                    colunas_agrupamento_total.append('Account')
                                
                                if len(colunas_agrupamento_total) > 0:
                                    # Verificar quais colunas existem antes de agrupar
                                    colunas_para_agregar = []
                                    colunas_esperadas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']
                                    for col in colunas_esperadas:
                                        if col in df_tabela_total.columns:
                                            colunas_para_agregar.append(col)
                                    
                                    if len(colunas_para_agregar) > 0:
                                        # Agrupar somando os valores
                                        df_tabela_total_agrupado = df_tabela_total.groupby(colunas_agrupamento_total).agg({
                                            col: 'sum' for col in colunas_para_agregar
                                        }).reset_index()
                                    else:
                                        # Se não há colunas para agregar, usar todas as colunas numéricas disponíveis
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in colunas_agrupamento_total 
                                                           and col not in ['Custo', 'Período']]
                                        if len(colunas_numericas) > 0:
                                            df_tabela_total_agrupado = df_tabela_total.groupby(colunas_agrupamento_total).agg({
                                                col: 'sum' for col in colunas_numericas
                                            }).reset_index()
                                        else:
                                            st.warning("⚠️ Nenhuma coluna numérica encontrada em df_tabela_total. Colunas disponíveis: " + ", ".join(df_tabela_total.columns.tolist()))
                                            df_tabela_total_agrupado = pd.DataFrame(columns=colunas_agrupamento_total)
                                
                                # Recalcular Total / Flex Bud após agrupamento (se as colunas necessárias existirem)
                                if len(df_tabela_total_agrupado) > 0 and 'Custo FP' in df_tabela_total_agrupado.columns and 'Flex BUD' in df_tabela_total_agrupado.columns:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = df_tabela_total_agrupado.apply(
                                        lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                        axis=1
                                    )
                                elif len(df_tabela_total_agrupado) > 0:
                                    df_tabela_total_agrupado['Total / Flex Bud'] = 0
                                
                                # 🔧 FILTRAR LINHAS ZERADAS E NULAS após agrupamento
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                                  and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                    if colunas_numericas_agrupado:
                                        df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0)
                                        df_tabela_total_agrupado = df_tabela_total_agrupado[
                                            df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                        ].copy()
                                else:
                                    # Se não houver colunas de agrupamento, somar tudo
                                    # Verificar quais colunas existem antes de somar
                                    valores_soma = {}
                                    colunas_esperadas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']
                                    for col in colunas_esperadas:
                                        if col in df_tabela_total.columns:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Se não encontrou as colunas esperadas, tentar usar todas as colunas numéricas
                                    if len(valores_soma) == 0:
                                        colunas_numericas = [col for col in df_tabela_total.columns 
                                                           if pd.api.types.is_numeric_dtype(df_tabela_total[col]) 
                                                           and col not in ['Custo', 'Período']]
                                        for col in colunas_numericas:
                                            valores_soma[col] = df_tabela_total[col].sum()
                                    
                                    # Calcular Total / Flex Bud se as colunas necessárias existirem
                                    if 'Custo FP' in valores_soma and 'Flex BUD' in valores_soma:
                                        if valores_soma['Flex BUD'] != 0 and pd.notnull(valores_soma['Flex BUD']):
                                            valores_soma['Total / Flex Bud'] = valores_soma['Custo FP'] / valores_soma['Flex BUD']
                                        else:
                                            valores_soma['Total / Flex Bud'] = 0
                                    
                                    if len(valores_soma) > 0:
                                        df_tabela_total_agrupado = pd.DataFrame([valores_soma])
                                    else:
                                        # Se não há colunas para somar, criar DataFrame vazio
                                        st.warning("⚠️ Nenhuma coluna numérica encontrada em df_tabela_total. Colunas disponíveis: " + ", ".join(df_tabela_total.columns.tolist()))
                                        df_tabela_total_agrupado = pd.DataFrame()
                                
                                # 🔧 FILTRAR: Se a linha única tiver todos os valores zerados, não exibir
                                if len(df_tabela_total_agrupado) > 0:
                                    colunas_numericas_agrupado = [col for col in df_tabela_total_agrupado.columns 
                                                                  if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col])]
                                    if colunas_numericas_agrupado:
                                        soma_absoluta = df_tabela_total_agrupado[colunas_numericas_agrupado].fillna(0).abs().sum(axis=1).iloc[0]
                                        if soma_absoluta <= 0.0001:
                                            df_tabela_total_agrupado = pd.DataFrame()  # DataFrame vazio para não exibir
                            
                            # 🔧 ADICIONAR: Exibir Resumo Geral no modo Total
                            if len(df_tabela_total_agrupado) > 0:
                                st.markdown("### 📊 Resumo Geral")
                                
                                # Usar df_tabela_flex_para_resumo (salvo ANTES da transformação em colunas por período)
                                # Se não existir (caso de período único), usar df_tabela_flex
                                df_para_resumo = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                                # Calcular resumo geral usando df_para_resumo
                                # Isso garante que todos os dados sejam incluídos no resumo com as colunas corretas
                                linha_resumo_geral, linha_resumo_geral_formatado = calcular_resumo_tabela_flex(
                                    df_para_resumo, 
                                    tipo_visualizacao, 
                                    moeda_simbolo, 
                                    fator_conversao
                                )
                                
                                # Exibir caixas de resumo
                                exibir_caixas_resumo(linha_resumo_geral, linha_resumo_geral_formatado, tipo_visualizacao, mostrar_volumes=False)
                                st.markdown("---")
                            
                            # Criar estrutura hierárquica sem separação por Custo
                            # 🔧 CORREÇÃO: Usar df_tabela_flex_para_resumo para cálculos de resumo (tem colunas originais)
                            df_para_resumo_total = df_tabela_flex_para_resumo if 'df_tabela_flex_para_resumo' in locals() else df_tabela_flex
                            
                            # Nível 1: Type 05 (se existir)
                            if 'Type 05' in df_tabela_total_agrupado.columns:
                                for type05 in sorted(df_tabela_total_agrupado['Type 05'].dropna().unique()):
                                    df_type05 = df_tabela_total_agrupado[df_tabela_total_agrupado['Type 05'] == type05].copy()
                                    # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                    df_type05_para_resumo = df_para_resumo_total[df_para_resumo_total['Type 05'] == type05].copy() if 'Type 05' in df_para_resumo_total.columns else df_type05.copy()
                                    
                                    if len(df_type05) > 0:
                                        # 🔧 FILTRAR: Verificar se Type 05 tem valores não zerados
                                        colunas_numericas_type05_check = [col for col in df_type05.columns 
                                                                         if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                         and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                        if colunas_numericas_type05_check:
                                            df_type05_check = df_type05[colunas_numericas_type05_check].fillna(0)
                                            tem_valores_nao_zerados = (df_type05_check.abs().sum(axis=1) > 0.0001).any()
                                            if not tem_valores_nao_zerados:
                                                continue  # Pular Type 05 completamente zerado
                                        else:
                                            if 'Custo FP' in df_type05_para_resumo.columns:
                                                if df_type05_para_resumo['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                    continue  # Pular Type 05 completamente zerado
                                            elif 'Custo FP' in df_type05.columns:
                                                if df_type05['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                    continue  # Pular Type 05 completamente zerado
                                        
                                        # Verificar se a coluna 'Custo FP' existe antes de acessá-la
                                        if 'Custo FP' in df_type05_para_resumo.columns:
                                            total_type05 = df_type05_para_resumo['Custo FP'].sum()
                                        elif 'Custo FP' in df_type05.columns:
                                            total_type05 = df_type05['Custo FP'].sum()
                                        else:
                                            total_type05 = 0.0
                                        total_type05_formatado = f"{total_type05:,.2f}"
                                        
                                        with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=False):
                                            # Resumo do Type 05
                                            # 🔧 CORREÇÃO: Usar df_type05_para_resumo que tem as colunas originais (BUD, Flex BUD, Total)
                                            linha_resumo_type05, linha_resumo_type05_formatado = calcular_resumo_tabela_flex(df_type05_para_resumo, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                            exibir_caixas_resumo(linha_resumo_type05, linha_resumo_type05_formatado, tipo_visualizacao)
                                            st.markdown("---")
                                            
                                            # Nível 2: Type 06 (se existir)
                                            if 'Type 06' in df_type05.columns:
                                                for type06 in sorted(df_type05['Type 06'].dropna().unique()):
                                                    df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                    # 🔧 CORREÇÃO: Criar versão para resumo com colunas originais
                                                    df_type06_para_resumo = df_type05_para_resumo[df_type05_para_resumo['Type 06'] == type06].copy() if 'Type 06' in df_type05_para_resumo.columns else df_type06.copy()
                                                    
                                                    if len(df_type06) > 0:
                                                        # 🔧 FILTRAR: Verificar se Type 06 tem valores não zerados
                                                        colunas_numericas_type06_check = [col for col in df_type06.columns 
                                                                                          if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                          and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                        if colunas_numericas_type06_check:
                                                            df_type06_check = df_type06[colunas_numericas_type06_check].fillna(0)
                                                            tem_valores_nao_zerados = (df_type06_check.abs().sum(axis=1) > 0.0001).any()
                                                            if not tem_valores_nao_zerados:
                                                                continue  # Pular Type 06 completamente zerado
                                                        else:
                                                            if 'Custo FP' in df_type06.columns:
                                                                if df_type06['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                    continue  # Pular Type 06 completamente zerado
                                                        
                                                        # Verificar se a coluna 'Custo FP' existe antes de acessá-la
                                                        if 'Custo FP' in df_type06_para_resumo.columns:
                                                            total_type06 = df_type06_para_resumo['Custo FP'].sum()
                                                        elif 'Custo FP' in df_type06.columns:
                                                            total_type06 = df_type06['Custo FP'].sum()
                                                        else:
                                                            total_type06 = 0.0
                                                        total_type06_formatado = f"{total_type06:,.2f}"
                                                        
                                                        # Nível 3: Account (se existir)
                                                        if 'Account' in df_type06.columns:
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
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
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar uma única tabela com todas as Accounts
                                                                    # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                    colunas_id = ['Account'] if 'Account' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Type 06', 'Custo', 'Período']]
                                                                    
                                                                    # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                    for col in colunas_numericas:
                                                                        if col not in colunas_ordenadas:
                                                                            colunas_ordenadas.append(col)
                                                                    
                                                                    colunas_display = colunas_id + colunas_ordenadas
                                                                    df_display = df_type06_filtrado[colunas_display].copy()
                                                                    
                                                                    # Formatar valores (formatar todas as colunas numéricas dinamicamente)
                                                                    for col in df_display.columns:
                                                                        if col not in colunas_id:
                                                                            # Formatar Total / Flex Bud com barra HTML (deve estar em formato decimal, não percentual)
                                                                            if col == 'Total / Flex Bud':
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0))
                                                                            # Formatar percentuais de forma especial com barrinha
                                                                            elif col.startswith('%'):
                                                                                # Usar formatar_ratio_com_barra para colunas de percentual
                                                                                df_display[col] = df_display[col].map(lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
                                                                            # Formatar outras colunas numéricas
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
                                                                    
                                                                    # Calcular linha de resumo (usar dados filtrados)
                                                                    linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                    
                                                                    # Exibir caixas de resumo
                                                                    exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                    
                                                                    # Exibir tabela com resumo (todas as Accounts em uma única tabela)
                                                                    html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                    st.markdown(html_table, unsafe_allow_html=True)
                                                        else:
                                                            # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                            # Usar todas as colunas numéricas (incluindo colunas dinâmicas)
                                                            colunas_numericas = [col for col in df_type06.columns 
                                                                                if pd.api.types.is_numeric_dtype(df_type06[col]) 
                                                                                and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                            if colunas_numericas:
                                                                # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                                                # Preencher nulos com 0 antes de calcular
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
                                                                with st.container():
                                                                    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_formatado}**")
                                                                    # Criar tabela para este Type 06
                                                                    # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                                    colunas_id = ['Type 06'] if 'Type 06' in df_type06_filtrado.columns else []
                                                                    colunas_numericas = [col for col in df_type06_filtrado.columns 
                                                                                        if col not in colunas_id and col not in ['Type 05', 'Account', 'Custo', 'Período']]
                                                                    
                                                                    # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                                    colunas_ordenadas = []
                                                                    for col_ordem in ordem_colunas:
                                                                        if col_ordem in colunas_numericas:
                                                                            colunas_ordenadas.append(col_ordem)
                                                                    # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                                    for col in colunas_numericas:
                                                                        if col not in colunas_ordenadas:
                                                                            colunas_ordenadas.append(col)
                                                                    
                                                                    colunas_display = colunas_id + colunas_ordenadas
                                                                    df_display = df_type06_filtrado[colunas_display].copy()

                                                                    # Exibição: remover coluna 'Ano' (evita manter linhas 0 por conter 2025, e evita formatar como moeda)
                                                                    if 'Ano' in df_display.columns and 'Ano' not in colunas_id:
                                                                        df_display = df_display.drop(columns=['Ano'])

                                                                    # Visibilidade: remover linhas 100% zeradas/nulas somente na exibição
                                                                    df_display = _remover_linhas_sem_valores_para_exibicao(
                                                                        df_display,
                                                                        colunas_ignorar=colunas_id,
                                                                    )
                                                                    
                                                                    # Formatar valores
                                                                    for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                                                        if col in df_display.columns:
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
                                                                    
                                                                    # Formatar Total / Flex Bud com barra HTML
                                                                    if 'Total / Flex Bud' in df_display.columns:
                                                                        df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                                    
                                                                    # Calcular linha de resumo (usar dados filtrados)
                                                                    linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type06_filtrado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                                    
                                                                    # Exibir caixas de resumo
                                                                    exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                                    
                                                                    # Exibir tabela com resumo
                                                                    html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                                    st.markdown(html_table, unsafe_allow_html=True)
                                            else:
                                                # Sem Type 06: exibir diretamente Type 05
                                                # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                                colunas_numericas_type05_total = [col for col in df_type05.columns 
                                                                                   if pd.api.types.is_numeric_dtype(df_type05[col]) 
                                                                                   and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                                if colunas_numericas_type05_total:
                                                    df_type05_temp_total = df_type05[colunas_numericas_type05_total].fillna(0)
                                                    df_type05 = df_type05[
                                                        df_type05_temp_total.abs().sum(axis=1) > 0.0001
                                                    ].copy()
                                                
                                                # Criar tabela para este Type 05
                                                # Usar colunas dinâmicas (pode ter colunas por período ou colunas padrão)
                                                colunas_id = ['Type 05'] if 'Type 05' in df_type05.columns else []
                                                colunas_numericas = [col for col in df_type05.columns 
                                                                    if col not in colunas_id and col not in ['Type 06', 'Account', 'Custo', 'Período']]
                                                
                                                # 🔧 CORREÇÃO: Reordenar colunas na ordem correta
                                                ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                                colunas_ordenadas = []
                                                for col_ordem in ordem_colunas:
                                                    if col_ordem in colunas_numericas:
                                                        colunas_ordenadas.append(col_ordem)
                                                # Adicionar outras colunas numéricas que não estão na ordem padrão (colunas dinâmicas)
                                                for col in colunas_numericas:
                                                    if col not in colunas_ordenadas:
                                                        colunas_ordenadas.append(col)
                                                
                                                colunas_display = colunas_id + colunas_ordenadas
                                                df_display = df_type05[colunas_display].copy()
                                                
                                                # Formatar valores
                                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                                    if col in df_display.columns:
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
                                                
                                                # Formatar Total / Flex Bud com barra HTML
                                                if 'Total / Flex Bud' in df_display.columns:
                                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].map(formatar_ratio_com_barra)
                                                
                                                # Calcular linha de resumo
                                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_type05, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                                
                                                # Exibir caixas de resumo
                                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                                
                                                # Exibir tabela com resumo
                                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                                st.markdown(html_table, unsafe_allow_html=True)
                            else:
                                # Sem Type 05: exibir tabela total diretamente
                                # Criar tabela única com todos os dados agregados
                                
                                # 🔧 FILTRAR LINHAS ZERADAS E NULAS: Remover linhas onde todos os valores numéricos são zero ou nulos
                                colunas_numericas = [col for col in df_tabela_total_agrupado.columns 
                                                    if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                    and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
                                if colunas_numericas:
                                    # Filtrar linhas onde a soma absoluta de todas as colunas numéricas é zero ou nula
                                    df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas].fillna(0)
                                    df_tabela_total_agrupado = df_tabela_total_agrupado[
                                        df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                    ].copy()
                                
                                total_geral = df_tabela_total_agrupado['Custo FP'].sum() if len(df_tabela_total_agrupado) > 0 else 0
                                total_geral_formatado = f"{total_geral:,.2f}"
                                
                                st.markdown(f"**Total Geral: {total_geral_formatado}**")
                                
                                # Criar tabela (usar colunas dinâmicas se disponíveis)
                                colunas_padrao = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
                                colunas_existentes = [col for col in colunas_padrao if col in df_tabela_total_agrupado.columns]
                                # Se não tiver colunas padrão, usar todas as colunas numéricas
                                if not colunas_existentes:
                                    colunas_existentes = colunas_numericas if colunas_numericas else df_tabela_total_agrupado.columns.tolist()
                                
                                df_display = df_tabela_total_agrupado[colunas_existentes].copy()

                                # Visibilidade: remover linhas 100% zeradas/nulas somente na exibição
                                df_display = _remover_linhas_sem_valores_para_exibicao(df_display)
                                
                                # Formatar valores
                                for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                    if col in df_display.columns:
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
                                
                                # Formatar Total / Flex Bud com barra HTML (garantir que está em formato de ratio, não percentual)
                                if 'Total / Flex Bud' in df_display.columns:
                                    # Garantir que os valores estão em formato de ratio (0.95 = 95%), não em percentual
                                    df_display['Total / Flex Bud'] = df_display['Total / Flex Bud'].apply(
                                        lambda x: formatar_ratio_com_barra(x) if pd.notna(x) and isinstance(x, (int, float)) else formatar_ratio_com_barra(0)
                                    )
                                
                                # Calcular linha de resumo
                                linha_resumo, linha_resumo_formatado = calcular_resumo_tabela_flex(df_tabela_total_agrupado, tipo_visualizacao, moeda_simbolo, fator_conversao)
                                
                                # Exibir caixas de resumo
                                exibir_caixas_resumo(linha_resumo, linha_resumo_formatado, tipo_visualizacao)
                                
                                # Exibir tabela com resumo
                                html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado)
                                st.markdown(html_table, unsafe_allow_html=True)
                        
                        # Botão de download da tabela Flex Bud
                        if st.button(
                            "📥 Baixar Tabela Flex Bud (Excel)",
                            width="stretch",
                            key="download_tabela_flex_bud_tc"
                        ):
                            with st.spinner("Gerando arquivo da tabela..."):
                                try:
                                    # Preparar DataFrame para download (usar dados originais antes da formatação)
                                    df_download = df_tabela_flex.copy()
                                    
                                    # Remover coluna 'Período' se existir (já foi filtrada)
                                    if 'Período' in df_download.columns:
                                        df_download = df_download.drop(columns=['Período'])
                                    
                                    # Formatar valores numéricos para o Excel (manter valores originais)
                                    # As colunas numéricas já estão com valores corretos, apenas garantir formato
                                    for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
                                        if col in df_download.columns:
                                            # Garantir que são numéricos
                                            df_download[col] = pd.to_numeric(df_download[col], errors='coerce')
                                    
                                    # Formatar 'Total / Flex Bud' como percentual (0.95 = 95%)
                                    if 'Total / Flex Bud' in df_download.columns:
                                        df_download['Total / Flex Bud'] = pd.to_numeric(df_download['Total / Flex Bud'], errors='coerce')
                                        # Converter para percentual se necessário (se estiver entre 0 e 1)
                                        df_download['Total / Flex Bud'] = df_download['Total / Flex Bud'].apply(
                                            lambda x: x * 100 if pd.notnull(x) and x <= 1 else x
                                        )

                                    # ============================
                                    # Aba 2: Realizado (no mesmo arquivo)
                                    # ============================
                                    df_real_download = None
                                    try:
                                        # Preferir a base real usada no cálculo da tabela Flex
                                        if 'df_real_tabela' in locals() and df_real_tabela is not None and len(df_real_tabela) > 0:
                                            df_real_download = df_real_tabela.copy()
                                        elif 'df_real_original_grafico' in locals() and df_real_original_grafico is not None and len(df_real_original_grafico) > 0:
                                            df_real_download = df_real_original_grafico.copy()
                                        elif 'df_filtrado' in locals() and df_filtrado is not None and len(df_filtrado) > 0:
                                            df_real_download = df_filtrado.copy()

                                        # Aplicar filtro de período (se existir na tela)
                                        if df_real_download is not None and 'Período' in df_real_download.columns and 'periodos_tabela' in locals() and periodos_tabela:
                                            df_real_download = df_real_download.copy()
                                            df_real_download['Período'] = df_real_download['Período'].apply(_normalizar_mes_lower)
                                            periodos_norm = [
                                                _normalizar_mes_lower(p)
                                                for p in periodos_tabela
                                                if p is not None and str(p).strip() != ''
                                            ]
                                            if periodos_norm:
                                                df_real_download = df_real_download[
                                                    df_real_download['Período'].isin(periodos_norm)
                                                ].copy()

                                        # Agregar Realizado no mesmo nível da visualização (Fixo/Variável vs Total)
                                        if df_real_download is not None and 'Custo FP' in df_real_download.columns:
                                            df_real_download['Custo FP'] = pd.to_numeric(df_real_download['Custo FP'], errors='coerce').fillna(0)
                                            group_cols = []
                                            if 'modo_tabela_flex' in locals() and modo_tabela_flex == "Fixo/Variável" and 'Custo' in df_real_download.columns:
                                                group_cols.append('Custo')
                                            for col in ['Type 05', 'Type 06', 'Account']:
                                                if col in df_real_download.columns:
                                                    group_cols.append(col)

                                            if group_cols:
                                                df_real_aggr = df_real_download.groupby(group_cols, as_index=False)['Custo FP'].sum()
                                            else:
                                                df_real_aggr = pd.DataFrame({'Custo FP': [df_real_download['Custo FP'].sum()]})

                                            # No modo CPU, exportar também CPU do Realizado (Total/Volume total do recorte)
                                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                                volume_real_total = 0.0
                                                if 'df_volume_real_filtrado' in locals() and df_volume_real_filtrado is not None and 'Volume' in df_volume_real_filtrado.columns:
                                                    vol_tmp = df_volume_real_filtrado.copy()
                                                    if 'Período' in vol_tmp.columns:
                                                        vol_tmp['Período'] = vol_tmp['Período'].apply(_normalizar_mes_lower)
                                                        if 'periodos_tabela' in locals() and periodos_tabela:
                                                            vol_tmp = vol_tmp[vol_tmp['Período'].isin(periodos_norm)].copy() if 'periodos_norm' in locals() else vol_tmp
                                                    volume_real_total = float(pd.to_numeric(vol_tmp['Volume'], errors='coerce').fillna(0).sum())

                                                df_real_aggr = df_real_aggr.rename(columns={'Custo FP': 'Total_Custo'})
                                                df_real_aggr['Volume_Real_Total'] = volume_real_total
                                                df_real_aggr['CPU'] = (
                                                    df_real_aggr['Total_Custo'] / volume_real_total
                                                    if volume_real_total not in (0, None) else 0.0
                                                )

                                            df_real_download = df_real_aggr
                                    except Exception:
                                        df_real_download = None
                                    
                                    # Obter pasta Downloads do usuário
                                    downloads_path = os.path.join(
                                        os.path.expanduser("~"), "Downloads"
                                    )
                                    tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                                    modo_nome = "Fixo_Variavel" if modo_tabela_flex == "Fixo/Variável" else "Total"
                                    file_name = f"TC_Flex_Bud_{modo_nome}_{tipo_nome}.xlsx"
                                    file_path = os.path.join(downloads_path, file_name)
                                    
                                    # Salvar arquivo diretamente na pasta Downloads
                                    with pd.ExcelWriter(
                                        file_path, engine='openpyxl'
                                    ) as writer:
                                        df_download.to_excel(
                                            writer, index=False, sheet_name='Flex_Bud'
                                        )

                                        # Adicionar segunda aba com Realizado (se disponível)
                                        if df_real_download is not None and hasattr(df_real_download, 'empty') and not df_real_download.empty:
                                            df_real_download.to_excel(
                                                writer, index=False, sheet_name='Realizado'
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
                st.info("ℹ️ Tabela Flex Bud disponível apenas quando há dados de budget e coluna 'Custo' nos dados.")

# ==========================================
# TAB 2: Volume
# ==========================================
if is_main_page:
    with tab2:
        # IMPORTANTE: Usar a mesma lógica de filtragem em ambos os modos
        # para garantir que os volumes sejam consistentes
        df_vol = load_volume_data(ano_selecionado)
        
        # Carregar dados de volume do budget para o gráfico de volume
        df_budget_vol_grafico = load_budget_volume_data(ano_selecionado)
        
        if df_vol is not None:
            # Verificar se tem as colunas necessárias
            if 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
                # Aplicar filtros da sidebar de forma centralizada
                df_vol_filtrado = filtrar_volume_com_sidebar(df_vol, df_total)
                if df_vol_filtrado is None:
                    df_vol_filtrado = df_vol.copy()
                
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
                # Aplicar mesmos filtros ao volume do budget
                df_budget_vol_filtrado_grafico = None
                if df_budget_vol_grafico is not None:
                    df_budget_vol_filtrado_grafico = df_budget_vol_grafico.copy()
                    
                    # Aplicar TODOS os filtros da sidebar diretamente ao df_budget_vol (mesma lógica do volume real)
                    # Filtro 1: Oficina
                    # CORREÇÃO: Garantir que apenas oficinas presentes nas opções do filtro sejam consideradas
                    if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                        # Obter as opções de oficina disponíveis (união Real + Budget - mesmas do filtro principal)
                        oficinas_set = set(get_filter_options(df_total, 'Oficina'))
                        oficinas_set.discard("Todos")
                        try:
                            df_budget_opcoes = load_budget_data(ano_selecionado)
                            if df_budget_opcoes is not None and 'Oficina' in df_budget_opcoes.columns:
                                oficinas_set.update(df_budget_opcoes['Oficina'].dropna().astype(str).unique().tolist())
                        except Exception:
                            pass
                        try:
                            df_budget_vol_opcoes = load_budget_volume_data(ano_selecionado)
                            if df_budget_vol_opcoes is not None and 'Oficina' in df_budget_vol_opcoes.columns:
                                oficinas_set.update(df_budget_vol_opcoes['Oficina'].dropna().astype(str).unique().tolist())
                        except Exception:
                            pass
                        oficina_opcoes_disponiveis = sorted(oficinas_set)
                        
                        # Obter oficinas selecionadas no filtro
                        oficina_selecionadas_sidebar = st.session_state.get('filtro_oficina_tc_veic', ["Todos"])
                        
                        # Se "Todos" estiver selecionado, usar todas as opções disponíveis no filtro
                        if "Todos" in oficina_selecionadas_sidebar or not oficina_selecionadas_sidebar:
                            # Filtrar apenas pelas oficinas que estão nas opções do filtro (não incluir oficinas que não estão no filtro)
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_opcoes_disponiveis)
                            ].copy()
                        else:
                            # Filtrar apenas pelas oficinas selecionadas (que já estão nas opções do filtro)
                            # Garantir que apenas oficinas que estão nas opções sejam consideradas
                            oficinas_validas = [o for o in oficina_selecionadas_sidebar if o in oficina_opcoes_disponiveis]
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficinas_validas)
                            ].copy()
                    
                    # Filtro 2: Veículo
                    if 'Veículo' in df_budget_vol_filtrado_grafico.columns:
                        veiculo_selecionados_sidebar = st.session_state.get('filtro_veiculo_tc_veic', ["Todos"])
                        if veiculo_selecionados_sidebar and "Todos" not in veiculo_selecionados_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Veículo'].astype(str).isin(veiculo_selecionados_sidebar)
                            ].copy()
                    
                    # Filtro 3: USI
                    if 'USI' in df_budget_vol_filtrado_grafico.columns:
                        usi_selecionada_sidebar = st.session_state.get('filtro_usi_tc_veic', ["Todos"])
                        if usi_selecionada_sidebar and "Todos" not in usi_selecionada_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['USI'].astype(str).isin(usi_selecionada_sidebar)
                            ].copy()
                    
                    # Filtro 4: Período - NÃO aplicar aqui, mostrar todos os períodos no gráfico
                    
                    # Filtro 5: Centro cst
                    if 'Centrocst' in df_budget_vol_filtrado_grafico.columns:
                        centro_cst_selecionado_sidebar = st.session_state.get('filtro_centro_cst_tc_veic', "Todos")
                        if centro_cst_selecionado_sidebar and centro_cst_selecionado_sidebar != "Todos":
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Centrocst'].astype(str) == str(centro_cst_selecionado_sidebar)
                            ].copy()
                    
                    # Filtro 6: Conta contábil
                    if 'Nºconta' in df_budget_vol_filtrado_grafico.columns:
                        conta_contabil_selecionadas_sidebar = st.session_state.get('filtro_conta_contabil_tc_veic', [])
                        if conta_contabil_selecionadas_sidebar:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Nºconta'].astype(str).isin(conta_contabil_selecionadas_sidebar)
                            ].copy()
                    
                    # Filtros principais
                    filtros_principais_nomes = ["Type 05", "Type 06", "Fornecedor", "Fornec.", "Tipo"]
                    for col_name in filtros_principais_nomes:
                        if col_name in df_budget_vol_filtrado_grafico.columns:
                            filtro_key = f'filtro_{col_name}_tc_veic'
                            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
                            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                                df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                    df_budget_vol_filtrado_grafico[col_name].astype(str).isin(selecionadas_sidebar)
                                ].copy()
                    
                    # Filtros avançados
                    filtros_avancados_nomes = ["Usuário", "Material", "Dt.lçto.", "Texto breve", "Account"]
                    for col_name in filtros_avancados_nomes:
                        if col_name in df_budget_vol_filtrado_grafico.columns:
                            filtro_key = f'filtro_avancado_{col_name}_tc_veic'
                            selecionadas_sidebar = st.session_state.get(filtro_key, ["Todos"])
                            if selecionadas_sidebar and "Todos" not in selecionadas_sidebar:
                                df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                    df_budget_vol_filtrado_grafico[col_name].astype(str).isin(selecionadas_sidebar)
                                ].copy()
                    
                    # Aplicar filtro de Oficina do gráfico
                    if 'Oficina' in df_budget_vol_filtrado_grafico.columns:
                        if oficina_selecionadas_grafico and "Todos" not in oficina_selecionadas_grafico:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Oficina'].astype(str).isin(oficina_selecionadas_grafico)
                            ].copy()
                    
                    # Aplicar filtro de Veículo do gráfico
                    if 'Veículo' in df_budget_vol_filtrado_grafico.columns:
                        if veiculo_selecionados_grafico and "Todos" not in veiculo_selecionados_grafico:
                            df_budget_vol_filtrado_grafico = df_budget_vol_filtrado_grafico[
                                df_budget_vol_filtrado_grafico['Veículo'].astype(str).isin(veiculo_selecionados_grafico)
                            ].copy()
                
                # 📊 Resumo Volume (acima do gráfico)
                volume_real_total = pd.to_numeric(
                    df_vol_filtrado['Volume'], errors='coerce'
                ).fillna(0).sum()
                volume_budget_total = None
                if (
                    df_budget_vol_filtrado_grafico is not None
                    and 'Volume' in df_budget_vol_filtrado_grafico.columns
                ):
                    volume_budget_total = pd.to_numeric(
                        df_budget_vol_filtrado_grafico['Volume'], errors='coerce'
                    ).fillna(0).sum()

                if volume_budget_total is not None:
                    diferenca_real_bud = volume_real_total - volume_budget_total
                    percentual_real_bud = (
                        volume_real_total / volume_budget_total
                        if volume_budget_total != 0
                        else None
                    )
                else:
                    diferenca_real_bud = None
                    percentual_real_bud = None

                st.subheader("📊 Resumo Volume")
                st.markdown(
                    """
                    <style>
                    .volume-summary-card {padding: 0.6rem 0.8rem; border: 1px solid rgba(49, 51, 63, 0.15); border-radius: 8px; background: rgba(0, 0, 0, 0.02);}
                    .volume-summary-label {opacity: 0.75;}
                    .volume-summary-value {font-size: 1.1em; font-weight: 600;}
                    .volume-summary-spacer {display: block; height: 1.75rem;}
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                def _render_volume_card(label, value):
                    st.markdown(
                        f"""
                        <div class="volume-summary-card">
                            <div class="volume-summary-label">{label}</div>
                            <div class="volume-summary-value">{value}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                col_res_1, col_res_2, col_res_3, col_res_4 = st.columns(4)
                with col_res_1:
                    _render_volume_card(
                        "Volume Budget",
                        f"{volume_budget_total:,.0f}" if volume_budget_total is not None else "-",
                    )
                with col_res_2:
                    _render_volume_card("Volume Real", f"{volume_real_total:,.0f}")
                with col_res_3:
                    _render_volume_card(
                        "Diferença Real - Bud",
                        f"{diferenca_real_bud:,.0f}" if diferenca_real_bud is not None else "-",
                    )
                with col_res_4:
                    _render_volume_card(
                        "Percentual Real/Bud",
                        f"{percentual_real_bud:,.1%}" if percentual_real_bud is not None else "-",
                    )

                st.markdown("<div class='volume-summary-spacer'></div>", unsafe_allow_html=True)

                # Exibir gráfico de Volume logo abaixo, usando os mesmos filtros
                st.subheader("📊 Volume Total por Período")
                if 'J516' in df_vol_filtrado['Veículo'].values:
                    j516_vol_total = df_vol_filtrado[df_vol_filtrado['Veículo'] == 'J516']['Volume'].sum()
                grafico_volume = create_volume_chart(df_vol_filtrado, df_budget_vol_filtrado_grafico)
                if grafico_volume:
                    st.altair_chart(grafico_volume, use_container_width=True)
        
        # Gráfico de Volume por Veículo (dentro da aba Volume)
        # 🔧 CORREÇÃO CRÍTICA: Usar df_vol_filtrado (mesmo DataFrame usado no gráfico "Volume Total")
        # para garantir que os mesmos filtros de Oficina sejam aplicados
        if df_vol is not None and 'Período' in df_vol.columns and 'Volume' in df_vol.columns:
            if df_vol_filtrado is not None and 'Volume' in df_vol_filtrado.columns and 'Veículo' in df_vol_filtrado.columns:
                st.subheader("📊 Volume por Oficina")

                # Usar df_budget_vol_filtrado_grafico se disponível (mesma variável usada no gráfico de volume por período)
                df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
                # Volume não deve ser recortado por existência de despesa.
                grafico_volume_oficina = create_volume_oficina_chart(df_vol_filtrado, df_budget_vol_para_grafico)
                if grafico_volume_oficina is not None:
                    st.altair_chart(grafico_volume_oficina, use_container_width=True)

                st.subheader("📊 Volume por Veículo")
                
                if 'J516' in df_vol_filtrado['Veículo'].values:
                    j516_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Veículo'] == 'J516']['Volume'].sum()
                # Usar df_budget_vol_filtrado_grafico se disponível (mesma variável usada no gráfico de volume por período)
                df_budget_vol_para_grafico = df_budget_vol_filtrado_grafico if 'df_budget_vol_filtrado_grafico' in locals() else None
                # Volume não deve ser recortado por existência de despesa.
                grafico_volume_veiculo = create_volume_veiculo_chart(df_vol_filtrado, df_budget_vol_para_grafico)
                if grafico_volume_veiculo is not None:
                    st.altair_chart(grafico_volume_veiculo, use_container_width=True)

# Gráfico 2: Soma do Valor por Oficina
# Cache removido temporariamente para forçar atualização
def create_oficina_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None):
    """Cria gráfico de barras por Oficina com linha de Flex Bud opcional"""
    try:
        # Robustez: garantir nomes/valores canônicos (ex.: Per\uFFFDodo -> Período)
        try:
            df_data = padronizar_colunas(df_data)
            df_budget = padronizar_colunas(df_budget) if df_budget is not None else None
            df_budget_vol = padronizar_colunas(df_budget_vol) if df_budget_vol is not None else None
            df_real_vol = padronizar_colunas(df_real_vol) if df_real_vol is not None else None
            df_real_original = padronizar_colunas(df_real_original) if df_real_original is not None else None
        except Exception:
            pass

        if 'Oficina' not in df_data.columns:
            return None
        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns):
                return None

        # 🔧 CORREÇÃO: No modo CPU, sempre agrupar apenas por Oficina (sem Veículo) para padronizar com Custo Total
        # Removido o bloco que agrupava por Veículo - agora sempre usa a lógica do bloco "else" abaixo
        if (tipo_viz == "CPU (Custo por Unidade)" and
                'Veículo' in df_data.columns and
                'Custo FP' not in df_data.columns):
            chart_data = df_data.groupby(
                ['Oficina', 'Veículo'], as_index=False
            )[coluna].sum()

            # Ordenar por Oficina e depois por CPU decrescente
            chart_data = chart_data.sort_values(
                ['Oficina', coluna], ascending=[True, False]
            )

            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            titulo_grafico = "CPU por Oficina e Veículo"

            # Criar gráfico de barras agrupadas
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort='-y', axis=alt.Axis(grid=False)),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
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
                        format=',.2f'
                    )
                ]
            ).properties(
                height=300,
                width='container'
                # Título removido para evitar duplicação com st.subheader
            )

            # Adicionar rótulos com valores nas barras
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                text=alt.Text(f'{coluna}:Q', format=',.2f')
            )

            return grafico_barras + rotulos
        else:
            # Gráfico normal sem separação por veículo
            # Para CPU, calcular SEM depender de Volume já mergeado em df_data.
            # Regra: CPU = soma(Total) / soma(Volume) no mesmo recorte/filtros.
            if tipo_viz == "CPU (Custo por Unidade)" and df_real_vol is not None:
                try:
                    df_custo_base = df_real_original if df_real_original is not None else df_data

                    try:
                        oficinas_recorte = set(df_data['Oficina'].astype(str).str.strip().dropna().unique().tolist()) if 'Oficina' in df_data.columns else set()
                    except Exception:
                        oficinas_recorte = set()
                    try:
                        periodos_recorte = set(df_data['Período'].astype(str).str.strip().dropna().unique().tolist()) if 'Período' in df_data.columns else None
                    except Exception:
                        periodos_recorte = None
                    try:
                        anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist()) if 'Ano' in df_data.columns else None
                    except Exception:
                        anos_recorte = None
                    try:
                        veiculos_recorte = set(df_data['Veículo'].astype(str).str.strip().dropna().unique().tolist()) if 'Veículo' in df_data.columns else None
                    except Exception:
                        veiculos_recorte = None

                    def _recortar(df_in: pd.DataFrame | None) -> pd.DataFrame | None:
                        if df_in is None:
                            return None
                        df_out = df_in
                        if oficinas_recorte and 'Oficina' in df_out.columns:
                            df_out = df_out[df_out['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()
                        if periodos_recorte is not None and 'Período' in df_out.columns:
                            df_out = df_out[df_out['Período'].astype(str).str.strip().isin(periodos_recorte)].copy()
                        if anos_recorte is not None and 'Ano' in df_out.columns:
                            ano_num = pd.to_numeric(df_out['Ano'], errors='coerce')
                            df_out = df_out[ano_num.isin(anos_recorte)].copy()
                        if veiculos_recorte is not None and 'Veículo' in df_out.columns:
                            df_out = df_out[df_out['Veículo'].astype(str).str.strip().isin(veiculos_recorte)].copy()
                        return df_out

                    df_custo_base = _recortar(df_custo_base)
                    df_vol_base = _recortar(df_real_vol)

                    df_cpu = _cpu_por_chaves_tc(
                        df_custo_base,
                        df_vol_base,
                        chaves_preferidas=("Ano", "Período", "Oficina"),
                        coluna_custo="Total",
                        coluna_volume="Volume",
                    )
                    if df_cpu is not None and not df_cpu.empty:
                        chart_data = (
                            df_cpu.groupby('Oficina', as_index=False)
                            .agg({'Custo FP': 'sum', 'Volume': 'sum'})
                        )
                        vol = pd.to_numeric(chart_data['Volume'], errors='coerce').fillna(0)
                        tot = pd.to_numeric(chart_data['Custo FP'], errors='coerce').fillna(0)
                        chart_data[coluna] = np.where(vol != 0, tot / vol, 0)
                        chart_data = chart_data[['Oficina', coluna]]
                    else:
                        chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
                except Exception:
                    chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()
            else:
                # Caminho quando não tem Total/Volume ou não é CPU
                chart_data = df_data.groupby('Oficina')[coluna].sum().reset_index()

            # Sanitização mínima para evitar gráficos vazios/legendas NaN
            chart_data = chart_data.copy()
            chart_data['Oficina'] = chart_data['Oficina'].astype(str).str.strip()
            chart_data = chart_data[
                chart_data['Oficina'].notna()
                & (chart_data['Oficina'] != '')
                & (~chart_data['Oficina'].str.lower().isin(['nan', 'none']))
            ].copy()
            chart_data[coluna] = pd.to_numeric(chart_data[coluna], errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0)
            chart_data['Tipo'] = 'Real'

            if chart_data.empty:
                return None

            # Se, após filtros/limpeza, todos os valores ficaram zerados,
            # o gráfico de barras pode parecer "em branco". Retornar um aviso visual.
            try:
                if float(chart_data[coluna].abs().sum()) == 0.0:
                    aviso_df = pd.DataFrame({"msg": ["Sem valores para exibir após filtros"]})
                    return (
                        alt.Chart(aviso_df)
                        .mark_text(size=14, color="#666", align="center")
                        .encode(text="msg:N")
                        .properties(height=80, width="container")
                    )
            except Exception:
                pass
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores válidos
            if coluna not in chart_data.columns:
                return None
            
            chart_data = chart_data.sort_values(coluna, ascending=False)
            
            # Determinar ordem das oficinas (usar a mesma ordem para barras e linha)
            ordem_oficinas_barras = chart_data['Oficina'].tolist()

            # Definir título do eixo Y baseado no tipo e moeda
            if tipo_viz == "CPU (Custo por Unidade)":
                titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
                titulo_grafico = "CPU por Oficina"
            else:
                titulo_y = f"Soma do Valor ({moeda_simbolo})"
                titulo_grafico = "Soma do Valor por Oficina"

            # Processar dados de budget e calcular FLEX se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'Oficina' in df_budget.columns and df_real_vol is not None:
                # A função precisa da coluna 'Custo'
                # Se não tiver, tentar usar df_real_original ou df_total que deve ter
                if 'Custo' not in df_real_para_flex.columns:
                    # Tentar usar df_total global que deve ter a coluna 'Custo'
                    if 'df_total' in globals() and 'Custo' in globals()['df_total'].columns:
                        df_real_para_flex = globals()['df_total'].copy()
                    else:
                        df_real_para_flex = None
                
                if df_real_para_flex is not None and 'Custo' in df_real_para_flex.columns:
                    try:
                        # 🔧 CORREÇÃO: Normalizar períodos ANTES de agrupar (mesma lógica do calcular_flex_budget)
                        mapeamento_meses = {
                            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                        }
                        
                        def normalizar_periodo(periodo):
                            """Normaliza período para formato capitalizado"""
                            if pd.isna(periodo):
                                return periodo
                            periodo_str = str(periodo).strip()
                            for mes_min, mes_cap in mapeamento_meses.items():
                                if periodo_str.lower() == mes_min.lower():
                                    return mes_cap
                            return periodo_str
                        
                        # Normalizar períodos em todos os DataFrames
                        if 'Período' in df_real_para_flex.columns:
                            df_real_para_flex = df_real_para_flex.copy()
                            df_real_para_flex['Período'] = df_real_para_flex['Período'].apply(normalizar_periodo)
                        if df_real_vol is not None and 'Período' in df_real_vol.columns:
                            df_real_vol = df_real_vol.copy()
                            df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
                        if 'Período' in df_budget.columns:
                            df_budget = df_budget.copy()
                            df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
                        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
                            df_budget_vol = df_budget_vol.copy()
                            df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)

                        # 🔧 CORREÇÃO CRÍTICA: o Flex Bud/Delta DEVEM respeitar o mesmo recorte do gráfico (Ano/Período/Oficina).
                        # Sem isso, com filtro em um mês (ex.: Agosto/2026), a linha/delta pode acabar somando o ano inteiro.
                        try:
                            oficinas_recorte = set(chart_data['Oficina'].astype(str).str.strip().unique().tolist())
                        except Exception:
                            oficinas_recorte = set()

                        anos_recorte = None
                        if 'Ano' in df_data.columns:
                            try:
                                anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist())
                            except Exception:
                                anos_recorte = None

                        periodos_recorte = None
                        if 'Período' in df_data.columns:
                            try:
                                periodos_recorte = set(df_data['Período'].astype(str).str.strip().unique().tolist())
                            except Exception:
                                periodos_recorte = None

                        def _aplicar_recorte(df_in: pd.DataFrame | None) -> pd.DataFrame | None:
                            if df_in is None:
                                return None
                            df_out = df_in
                            if oficinas_recorte and 'Oficina' in df_out.columns:
                                df_out = df_out[df_out['Oficina'].astype(str).str.strip().isin(oficinas_recorte)].copy()
                            if periodos_recorte is not None and 'Período' in df_out.columns:
                                df_out = df_out[df_out['Período'].astype(str).str.strip().isin(periodos_recorte)].copy()
                            if anos_recorte is not None and 'Ano' in df_out.columns:
                                ano_num = pd.to_numeric(df_out['Ano'], errors='coerce')
                                df_out = df_out[ano_num.isin(anos_recorte)].copy()
                            return df_out

                        df_real_para_flex = _aplicar_recorte(df_real_para_flex)
                        df_real_vol = _aplicar_recorte(df_real_vol)
                        df_budget = _aplicar_recorte(df_budget)
                        df_budget_vol = _aplicar_recorte(df_budget_vol)
                        
                        # Calcular FLEX agrupado por Oficina seguindo a mesma lógica do gráfico por Período
                        # Primeiro calcular FLEX por Período e Oficina, depois agrupar por Oficina
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        
                        # Calcular Flex Bud por Período e Oficina (mesma lógica do gráfico por Período)
                        # Agrupar dados reais por Período e Oficina
                        if tem_ano:
                            if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Oficina', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            # Agrupar budget por Período e Oficina
                            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Oficina', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        else:
                            if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Período', 'Oficina', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Oficina' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns and 'Oficina' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Período', 'Oficina', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Oficina' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Período', 'Oficina'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        
                        # Verificar se temos todos os dados necessários
                        if (real_agrupado is None or real_vol_agrupado is None or 
                            budget_agrupado is None or budget_vol_agrupado is None):
                            flex_data = None
                        else:
                            # 🔧 CORREÇÃO: Normalizar períodos nos DataFrames agrupados antes do merge
                            if tem_ano:
                                real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                                budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                                real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                                budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                            else:
                                real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                                budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                                real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                                budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                            
                            # Fazer merge de volumes por Período e Oficina
                            # Usar 'left' para incluir todas as oficinas dos dados reais, mesmo sem budget
                            if tem_ano:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Ano', 'Período', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se não houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            else:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Período', 'Oficina'],
                                    how='left',  # Incluir todas as oficinas dos dados reais
                                    suffixes=('_real', '_budget')
                                )
                                # Preencher volume_budget com 0 se não houver budget
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            
                            # 🔧 CORREÇÃO: Para CPU, não podemos somar os CPUs de cada período
                            # Devemos calcular o Flex Bud Total (Custo Total) por período e oficina,
                            # depois agregar por oficina e recalcular o CPU final
                            
                            # Calcular Flex Bud Total (Custo Total) por Período e Oficina (vetorizado)
                            try:
                                chaves_base = ['Período', 'Oficina']
                                if tem_ano:
                                    chaves_base = ['Ano'] + chaves_base

                                # Budget: pivot por Custo para obter Fixo e Total
                                budget_agrupado = budget_agrupado.copy()
                                if 'Custo' in budget_agrupado.columns:
                                    budget_agrupado['Custo'] = budget_agrupado['Custo'].apply(_normalizar_rotulo_custo)
                                budget_piv = budget_agrupado.pivot_table(
                                    index=chaves_base,
                                    columns='Custo',
                                    values='Custo FP',
                                    aggfunc='sum',
                                    fill_value=0
                                )
                                custo_fixo_budget = budget_piv['Fixo'] if 'Fixo' in budget_piv.columns else 0
                                budget_total = budget_piv.sum(axis=1)
                                budget_sum = pd.DataFrame({
                                    'Budget_Total': budget_total,
                                    'Custo_Fixo_Budget': custo_fixo_budget,
                                }).reset_index()

                                flex_df = volumes.merge(budget_sum, on=chaves_base, how='left')
                                flex_df['Budget_Total'] = pd.to_numeric(flex_df['Budget_Total'], errors='coerce').fillna(0)
                                flex_df['Custo_Fixo_Budget'] = pd.to_numeric(flex_df['Custo_Fixo_Budget'], errors='coerce').fillna(0)

                                flex_df['Volume_real'] = pd.to_numeric(flex_df['Volume_real'], errors='coerce').fillna(0)
                                flex_df['Volume_budget'] = pd.to_numeric(flex_df['Volume_budget'], errors='coerce').fillna(0)
                                custo_nao_fixo = flex_df['Budget_Total'] - flex_df['Custo_Fixo_Budget']
                                proporcao = np.where(flex_df['Volume_budget'] != 0, flex_df['Volume_real'] / flex_df['Volume_budget'], 1.0)
                                flex_df['Flex_Bud_Total'] = flex_df['Custo_Fixo_Budget'] + (custo_nao_fixo * proporcao)
                                flex_df['Volume_Real'] = flex_df['Volume_real']

                                flex_data = flex_df[chaves_base + ['Flex_Bud_Total', 'Volume_Real']].copy()
                            except Exception:
                                flex_data = None
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud Total e Volume Real por Oficina (somar todos os períodos)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Para CPU: somar Flex Bud Total e Volume Real, depois recalcular CPU
                                budget_data = flex_data.groupby('Oficina').agg({
                                    'Flex_Bud_Total': 'sum',
                                    'Volume_Real': 'sum'
                                }).reset_index()
                                
                                # Recalcular CPU: Flex Bud Total agregado / Volume Real agregado (vetorizado)
                                volr = pd.to_numeric(budget_data['Volume_Real'], errors='coerce').fillna(0)
                                fbt = pd.to_numeric(budget_data['Flex_Bud_Total'], errors='coerce').fillna(0)
                                budget_data[coluna] = np.where(volr != 0, fbt / volr, 0)
                                
                                # Manter apenas colunas necessárias
                                budget_data = budget_data[['Oficina', coluna]]
                            else:
                                # Para Custo Total: apenas somar Flex Bud Total
                                budget_data = flex_data.groupby('Oficina')['Flex_Bud_Total'].sum().reset_index()
                                budget_data.rename(columns={'Flex_Bud_Total': coluna}, inplace=True)
                            
                            # Filtrar apenas oficinas que existem no chart_data
                            budget_data['Oficina'] = budget_data['Oficina'].astype(str).str.strip()
                            budget_data = budget_data[budget_data['Oficina'].isin(chart_data['Oficina'])].copy()
                            
                            if len(budget_data) > 0:
                                # Criar linha tracejada de Flex Bud
                                budget_data_legenda = budget_data.copy()
                                budget_data_legenda['Tipo'] = 'Flex Bud'
                                
                                # IMPORTANTE: Usar EXATAMENTE a mesma ordem das barras
                                # Reordenar budget_data_legenda para seguir a ordem de ordem_oficinas_barras
                                # Criar um dicionário de mapeamento de ordem
                                ordem_dict = {oficina: idx for idx, oficina in enumerate(ordem_oficinas_barras)}
                                # Adicionar coluna de ordem para ordenar
                                budget_data_legenda['_ordem'] = budget_data_legenda['Oficina'].map(ordem_dict)
                                # Filtrar apenas oficinas que existem na ordem e ordenar
                                budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                                budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                                budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                                
                                # Usar a mesma ordem das barras (filtrando apenas oficinas que existem no budget)
                                ordem_oficinas = [o for o in ordem_oficinas_barras if o in budget_data_legenda['Oficina'].tolist()]
                                
                                # Criar linha tracejada de Flex Bud (igual ao gráfico por Período)
                                linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                    strokeDash=[10, 5],
                                    strokeWidth=1.5,
                                    opacity=0.8
                                ).encode(
                                    x=alt.X(
                                        'Oficina:N',
                                        title='Oficina',
                                        sort=ordem_oficinas,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    y=alt.Y(
                                        f'{coluna}:Q',
                                        title=titulo_y,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    color=alt.Color(
                                        'Tipo:N',
                                        title='Legenda',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                        legend=None
                                    ),
                                    strokeDash=alt.StrokeDash(
                                        'Tipo:N',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=[[0], [10, 5]]),
                                        legend=None
                                    ),
                                    tooltip=[
                                        alt.Tooltip('Oficina:N', title='Oficina'),
                                        alt.Tooltip('Tipo:N', title='Tipo'),
                                        alt.Tooltip(
                                            f'{coluna}:Q',
                                            title='Flex Bud',
                                            format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                        )
                                    ]
                                )
                                
                                # Adicionar bolinhas nos pontos da linha (usar mesma ordem)
                                pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                    size=80,
                                    opacity=0.9
                                ).encode(
                                    x=alt.X(
                                        'Oficina:N',
                                        title='Oficina',
                                        sort=ordem_oficinas,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    y=alt.Y(
                                        f'{coluna}:Q',
                                        title=titulo_y,
                                        axis=alt.Axis(grid=False, domain=True, ticks=True)
                                    ),
                                    color=alt.Color(
                                        'Tipo:N',
                                        title='Legenda',
                                        scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                        legend=None
                                    ),
                                    tooltip=[
                                        alt.Tooltip('Oficina:N', title='Oficina'),
                                        alt.Tooltip('Tipo:N', title='Tipo'),
                                        alt.Tooltip(
                                            f'{coluna}:Q',
                                            title='Flex Bud',
                                            format=',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                        )
                                    ]
                                )
                                
                                # Adicionar rótulos
                                formato_rotulo_budget = ',.2f' if tipo_viz == "CPU (Custo por Unidade)" else ',.2f'
                                rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                    align='center',
                                    baseline='bottom',
                                    dy=-20,
                                    color='#FF6B35',
                                    fontSize=9,
                                    fontWeight='bold'
                                ).encode(
                                    x=alt.X('Oficina:N', sort=ordem_oficinas),
                                    y=alt.Y(f'{coluna}:Q'),
                                    text=alt.Text(f'{coluna}:Q', format=formato_rotulo_budget)
                                )
                                
                                # Combinar linha, pontos e rótulos
                                linha_budget = linha_budget + pontos_budget + rotulos_budget
                    except Exception as e:
                        # Silenciar erro, apenas não mostrar linha de budget
                        pass

            # Usar a ordem explícita para garantir sincronização com a linha pontilhada
            max_abs_cpu = None
            if tipo_viz == "CPU (Custo por Unidade)":
                try:
                    max_abs_cpu = float(pd.to_numeric(chart_data[coluna], errors='coerce').abs().max())
                except Exception:
                    max_abs_cpu = None

            if tipo_viz == "CPU (Custo por Unidade)":
                y_enc = alt.Y(
                    f'{coluna}:Q',
                    title=titulo_y,
                    axis=alt.Axis(
                        grid=False,
                        domain=True,
                        ticks=True,
                        format=',.4f' if max_abs_cpu is not None and max_abs_cpu < 1 else ',.2f'
                    ),
                    scale=alt.Scale(zero=False)
                )
                formato_rotulo = ',.4f' if max_abs_cpu is not None and max_abs_cpu < 1 else ',.2f'
            else:
                y_enc = alt.Y(
                    f'{coluna}:Q',
                    title=titulo_y,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                )
                formato_rotulo = ',.2f'

            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X('Oficina:N', title='Oficina', sort=ordem_oficinas_barras, axis=alt.Axis(grid=False, domain=True, ticks=True)),
                y=y_enc,
                color=alt.Color(
                    'Tipo:N',
                    title='Legenda',
                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                    legend=alt.Legend(
                        title='Legenda',
                        orient='bottom',
                        titleFontSize=10,
                        labelFontSize=9,
                        titleAnchor='middle',
                        direction='horizontal',
                        symbolType='square'
                    )
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('Tipo:N', title='Tipo'),
                    alt.Tooltip(
                        f'{coluna}:Q',
                        title=coluna,
                        format=formato_rotulo
                    )
                ]
            ).properties(
                height=300,
                width='container'
                # Título removido para evitar duplicação com st.subheader
            )

            # Adicionar rótulos com valores nas barras
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                x=alt.X('Oficina:N', sort=ordem_oficinas_barras, title='Oficina'),
                text=alt.Text(f'{coluna}:Q', format=formato_rotulo)
            )

            # Pontos para garantir visibilidade quando os valores são muito pequenos
            pontos_real = alt.Chart(chart_data).mark_circle(size=70, opacity=0.9).encode(
                x=alt.X('Oficina:N', sort=ordem_oficinas_barras, title='Oficina'),
                y=y_enc,
                color=alt.Color(
                    'Tipo:N',
                    title='Legenda',
                    scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                    legend=None
                ),
                tooltip=[
                    alt.Tooltip('Oficina:N', title='Oficina'),
                    alt.Tooltip('Tipo:N', title='Tipo'),
                    alt.Tooltip(f'{coluna}:Q', title=coluna, format=formato_rotulo)
                ]
            )

            # Criar gráfico de delta (Real - Flex Bud) se linha_budget estiver disponível
            grafico_delta = None
            if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
                try:
                    # Calcular delta: Real - Flex Bud
                    delta_data = chart_data.copy()
                    budget_data_merge = budget_data.copy()
                    budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                    
                    delta_data = delta_data.merge(
                        budget_data_merge[['Oficina', f'{coluna}_FlexBud']],
                        on='Oficina',
                        how='left'
                    )
                    
                    # Calcular delta: Real - Flex Bud
                    delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data[f'{coluna}_FlexBud'].fillna(0)
                    
                    # Calcular min e max do delta para a escala de cores
                    # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                    delta_min_abs = abs(delta_data['Delta'].min())
                    delta_max_abs = abs(delta_data['Delta'].max())
                    delta_abs_max = max(delta_min_abs, delta_max_abs)
                    
                    # Criar domínio simétrico baseado no maior valor absoluto
                    # Isso garante que zero sempre fique no centro, independente dos filtros
                    delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                    delta_max = delta_abs_max if delta_abs_max > 0 else 1
                    
                    # Ordenar por valor para manter ordem consistente
                    delta_data = delta_data.sort_values(coluna, ascending=False)
                    ordem_oficinas_delta = delta_data['Oficina'].tolist()
                    
                    # Criar gráfico de barras para delta
                    grafico_delta = alt.Chart(delta_data).mark_bar(
                        size=20
                    ).encode(
                        x=alt.X(
                            'Oficina:N',
                            title='',
                            sort=ordem_oficinas_delta,
                            axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)
                        ),
                        y=alt.Y(
                            'Delta:Q',
                            title='Delta (Real - Flex Bud)',
                            axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                        ),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[delta_min, 0, delta_max],
                                range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        ),
                        tooltip=[
                            alt.Tooltip('Oficina:N', title='Oficina'),
                            alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                            alt.Tooltip(f'{coluna}:Q', title='Real', format=',.2f'),
                            alt.Tooltip(f'{coluna}_FlexBud:Q', title='Flex Bud', format=',.2f')
                        ]
                    ).properties(
                        height=38
                    )
                    
                    # Adicionar rótulos de dados no gráfico de delta
                    # Usar a mesma cor das barras (escala baseada no valor do Delta)
                    rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                        align='center',
                        baseline='bottom',
                        dy=-12,
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X('Oficina:N', sort=ordem_oficinas_delta),
                        y=alt.Y('Delta:Q'),
                        text=alt.Text('Delta:Q', format=',.2f'),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[0, delta_max],
                                range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        )
                    )
                    
                    rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                        align='center',
                        baseline='top',
                        dy=12,
                        fontSize=9,
                        fontWeight='bold'
                    ).encode(
                        x=alt.X('Oficina:N', sort=ordem_oficinas_delta),
                        y=alt.Y('Delta:Q'),
                        text=alt.Text('Delta:Q', format=',.2f'),
                        color=alt.Color(
                            'Delta:Q',
                            scale=alt.Scale(
                                domain=[delta_min, 0],
                                range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                                type='linear',
                                nice=False
                            ),
                            legend=None
                        )
                    )
                    
                    grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
                except Exception as e:
                    pass  # Silenciar erro, apenas não mostrar delta
            
            # Combinar gráfico de barras com linha de budget se disponível
            if linha_budget is not None:
                grafico_principal = alt.layer(
                    grafico_barras,
                    rotulos,
                    pontos_real,
                    linha_budget
                ).resolve_scale(
                    x='shared',
                    y='shared',
                    color='shared'
                ).resolve_legend(
                    color='shared'
                )
                
                # Se temos gráfico de delta, combinar verticalmente (delta em cima)
                if grafico_delta is not None:
                    grafico_final = alt.vconcat(
                        grafico_delta,
                        grafico_principal
                    ).resolve_scale(
                        x='shared'
                    )
                else:
                    grafico_final = grafico_principal
            else:
                grafico_final = alt.layer(grafico_barras, rotulos, pontos_real)
            
            return grafico_final
    except Exception as e:
        st.error(f"Erro ao criar gráfico: {e}")
        return None


# Gráfico 4: Total/CPU por Veículo
# Cache removido temporariamente para forçar atualização
def create_total_chart(df_data, coluna, tipo_viz, moeda_simbolo="R$", df_budget=None, df_budget_vol=None, df_real_vol=None, df_real_original=None, df_visualizacao_volume=None, df_total_completo=None, df_despesas=None, df_total_filtrado=None, df_volume_filtrado_grafico=None):
    """Cria gráfico de barras de Total/CPU por Veículo com linha de Flex Bud opcional"""
    try:
        # Robustez: garantir nomes/valores canônicos (ex.: Per\uFFFDodo -> Período)
        try:
            df_data = padronizar_colunas(df_data)
            df_budget = padronizar_colunas(df_budget) if df_budget is not None else None
            df_budget_vol = padronizar_colunas(df_budget_vol) if df_budget_vol is not None else None
            df_real_vol = padronizar_colunas(df_real_vol) if df_real_vol is not None else None
            df_real_original = padronizar_colunas(df_real_original) if df_real_original is not None else None
            df_total_filtrado = padronizar_colunas(df_total_filtrado) if df_total_filtrado is not None else None
            df_volume_filtrado_grafico = (
                padronizar_colunas(df_volume_filtrado_grafico)
                if df_volume_filtrado_grafico is not None
                else None
            )
        except Exception:
            pass

        if coluna not in df_data.columns:
            if not (tipo_viz == "CPU (Custo por Unidade)" and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns):
                return None

        # Definir título e formato baseado no tipo e moeda
        if tipo_viz == "CPU (Custo por Unidade)":
            titulo_y = f"CPU ({moeda_simbolo}/Unidade)"
            formato = ',.2f'
            if 'Veículo' in df_data.columns:
                titulo_grafico = "CPU por Veículo"
            else:
                titulo_grafico = "CPU por Período"
        else:
            titulo_y = f"Total ({moeda_simbolo})"
            formato = ',.2f'
            if 'Veículo' in df_data.columns:
                titulo_grafico = "Total por Veículo"
            else:
                titulo_grafico = "Total por Período"

        # Verificar se tem coluna Veículo
        if 'Veículo' in df_data.columns:
            # Para CPU, recalcular a partir de Total e Volume agregados
            # 🔧 CORREÇÃO CRÍTICA: No modo CPU, df_data (df_visualizacao) tem Total e Volume já agregados por Oficina+Período+Veículo
            # Quando agrupamos apenas por Veículo, estamos somando valores que podem estar duplicados
            # Precisamos usar o Total DIRETO de df_real_original (dados originais) e Volume DIRETO de df_real_vol (arquivo original)
            if tipo_viz == "CPU (Custo por Unidade)":
                base_custo = df_total_filtrado if df_total_filtrado is not None else (df_real_original if df_real_original is not None else df_data)
                base_vol = df_volume_filtrado_grafico if df_volume_filtrado_grafico is not None else (df_real_vol if df_real_vol is not None else df_visualizacao_volume)

                if base_custo is None or base_vol is None:
                    return None
                if 'Custo FP' not in base_custo.columns or 'Veículo' not in base_custo.columns:
                    return None
                if 'Volume' not in base_vol.columns or 'Veículo' not in base_vol.columns:
                    return None

                # Respeitar recorte Ano/Período para evitar somar fora do filtro.
                try:
                    periodos_recorte = set(df_data['Período'].astype(str).str.strip().dropna().unique().tolist()) if 'Período' in df_data.columns else None
                except Exception:
                    periodos_recorte = None
                try:
                    anos_recorte = set(pd.to_numeric(df_data['Ano'], errors='coerce').dropna().unique().tolist()) if 'Ano' in df_data.columns else None
                except Exception:
                    anos_recorte = None

                if periodos_recorte is not None and 'Período' in base_custo.columns:
                    base_custo = base_custo[base_custo['Período'].astype(str).str.strip().isin(periodos_recorte)].copy()
                if periodos_recorte is not None and 'Período' in base_vol.columns:
                    base_vol = base_vol[base_vol['Período'].astype(str).str.strip().isin(periodos_recorte)].copy()
                if anos_recorte is not None and 'Ano' in base_custo.columns:
                    ano_num = pd.to_numeric(base_custo['Ano'], errors='coerce')
                    base_custo = base_custo[ano_num.isin(anos_recorte)].copy()
                if anos_recorte is not None and 'Ano' in base_vol.columns:
                    ano_num = pd.to_numeric(base_vol['Ano'], errors='coerce')
                    base_vol = base_vol[ano_num.isin(anos_recorte)].copy()

                df_cpu = _cpu_por_chaves_tc(
                    base_custo,
                    base_vol,
                    chaves_preferidas=("Ano", "Período", "Veículo"),
                    coluna_custo="Total",
                    coluna_volume="Volume",
                )
                if df_cpu is None or df_cpu.empty:
                    return None

                chart_data = (
                    df_cpu.groupby('Veículo', as_index=False)
                    .agg({'Custo FP': 'sum', 'Volume': 'sum'})
                )
                vol = pd.to_numeric(chart_data['Volume'], errors='coerce').fillna(0)
                tot = pd.to_numeric(chart_data['Custo FP'], errors='coerce').fillna(0)
                chart_data[coluna] = np.where(vol != 0, tot / vol, 0)
                chart_data = chart_data[['Veículo', coluna]]
            else:
                chart_data = (
                    df_data.groupby('Veículo')[coluna].sum().reset_index()
                )
            
            # Validar se chart_data tem dados
            if chart_data is None or chart_data.empty:
                return None
            
            # Validar se a coluna tem valores válidos
            if coluna not in chart_data.columns:
                return None
            
            chart_data = chart_data.sort_values(coluna, ascending=False)

            # Determinar ordem dos veículos (usar a mesma ordem para barras e linha)
            ordem_veiculos_barras = chart_data['Veículo'].tolist()

            # Processar dados de budget e calcular FLEX se fornecidos
            linha_budget = None
            budget_data = None
            df_real_para_flex = df_real_original if df_real_original is not None else df_data
            
            if df_budget is not None and 'Veículo' in df_budget.columns and df_real_vol is not None:
                if 'Custo' in df_real_para_flex.columns:
                    try:
                        # 🔧 CORREÇÃO: Usar a MESMA lógica do gráfico de Oficina (que funciona!)
                        # Calcular Flex Bud diretamente em vez de usar calcular_flex_budget
                        # Normalizar períodos ANTES de agrupar
                        mapeamento_meses = {
                            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
                            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
                            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
                            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
                        }
                        
                        def normalizar_periodo(periodo):
                            """Normaliza período para formato capitalizado"""
                            if pd.isna(periodo):
                                return periodo
                            periodo_str = str(periodo).strip()
                            for mes_min, mes_cap in mapeamento_meses.items():
                                if periodo_str.lower() == mes_min.lower():
                                    return mes_cap
                            return periodo_str
                        
                        # Normalizar períodos em todos os DataFrames
                        if 'Período' in df_real_para_flex.columns:
                            df_real_para_flex = df_real_para_flex.copy()
                            df_real_para_flex['Período'] = df_real_para_flex['Período'].apply(normalizar_periodo)
                        if df_real_vol is not None and 'Período' in df_real_vol.columns:
                            df_real_vol = df_real_vol.copy()
                            df_real_vol['Período'] = df_real_vol['Período'].apply(normalizar_periodo)
                        if 'Período' in df_budget.columns:
                            df_budget = df_budget.copy()
                            df_budget['Período'] = df_budget['Período'].apply(normalizar_periodo)
                        if df_budget_vol is not None and 'Período' in df_budget_vol.columns:
                            df_budget_vol = df_budget_vol.copy()
                            df_budget_vol['Período'] = df_budget_vol['Período'].apply(normalizar_periodo)
                        
                        # Calcular Flex Bud por Período e Veículo (mesma lógica do gráfico de Oficina)
                        tem_ano = 'Ano' in df_real_para_flex.columns
                        
                        # Agrupar dados reais por Período e Veículo
                        if tem_ano:
                            if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Ano', 'Período', 'Veículo', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Veículo' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Ano', 'Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns and 'Veículo' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Ano', 'Período', 'Veículo', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Veículo' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Ano', 'Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        else:
                            if 'Custo' in df_real_para_flex.columns and 'Custo FP' in df_real_para_flex.columns:
                                real_agrupado = df_real_para_flex.groupby(['Período', 'Veículo', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                real_agrupado = None
                            
                            if df_real_vol is not None and 'Volume' in df_real_vol.columns and 'Veículo' in df_real_vol.columns:
                                real_vol_agrupado = df_real_vol.groupby(['Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                real_vol_agrupado = None
                            
                            if 'Custo' in df_budget.columns and 'Custo FP' in df_budget.columns and 'Veículo' in df_budget.columns:
                                budget_agrupado = df_budget.groupby(['Período', 'Veículo', 'Custo'])['Custo FP'].sum().reset_index()
                            else:
                                budget_agrupado = None
                            
                            if df_budget_vol is not None and 'Volume' in df_budget_vol.columns and 'Veículo' in df_budget_vol.columns:
                                budget_vol_agrupado = df_budget_vol.groupby(['Período', 'Veículo'])['Volume'].sum().reset_index()
                            else:
                                budget_vol_agrupado = None
                        
                        # Verificar se temos todos os dados necessários
                        if (real_agrupado is None or real_vol_agrupado is None or 
                            budget_agrupado is None or budget_vol_agrupado is None):
                            flex_data = None
                        else:
                            # Normalizar períodos nos DataFrames agrupados antes do merge
                            if tem_ano:
                                real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                                budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                                real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                                budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                            else:
                                real_vol_agrupado['Período'] = real_vol_agrupado['Período'].astype(str).str.strip()
                                budget_vol_agrupado['Período'] = budget_vol_agrupado['Período'].astype(str).str.strip()
                                real_agrupado['Período'] = real_agrupado['Período'].astype(str).str.strip()
                                budget_agrupado['Período'] = budget_agrupado['Período'].astype(str).str.strip()
                            
                            # Fazer merge de volumes por Período e Veículo
                            if tem_ano:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Ano', 'Período', 'Veículo'],
                                    how='left',
                                    suffixes=('_real', '_budget')
                                )
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            else:
                                volumes = pd.merge(
                                    real_vol_agrupado,
                                    budget_vol_agrupado,
                                    on=['Período', 'Veículo'],
                                    how='left',
                                    suffixes=('_real', '_budget')
                                )
                                volumes['Volume_budget'] = volumes['Volume_budget'].fillna(0)
                            
                            # Calcular FLEX para cada Período e Veículo (mesma lógica do gráfico de Oficina)
                            flex_data = []
                            for _, vol_row in volumes.iterrows():
                                if tem_ano:
                                    ano = vol_row['Ano']
                                    periodo = vol_row['Período']
                                    veiculo = vol_row['Veículo']
                                else:
                                    periodo = vol_row['Período']
                                    veiculo = vol_row['Veículo']
                                
                                volume_real = vol_row['Volume_real']
                                volume_budget = vol_row['Volume_budget'] if pd.notna(vol_row['Volume_budget']) else 0
                                
                                if volume_real == 0 or pd.isna(volume_real):
                                    continue
                                
                                # Obter custos reais para este Período e Veículo
                                if tem_ano:
                                    custos_real = real_agrupado[
                                        (real_agrupado['Ano'] == ano) & 
                                        (real_agrupado['Período'] == periodo) &
                                        (real_agrupado['Veículo'] == veiculo)
                                    ]
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Ano'] == ano) & 
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Veículo'] == veiculo)
                                    ]
                                else:
                                    custos_real = real_agrupado[
                                        (real_agrupado['Período'] == periodo) &
                                        (real_agrupado['Veículo'] == veiculo)
                                    ]
                                    custos_budget = budget_agrupado[
                                        (budget_agrupado['Período'] == periodo) &
                                        (budget_agrupado['Veículo'] == veiculo)
                                    ]
                                
                                # Se não houver dados de budget para este veículo, usar zeros
                                if len(custos_budget) == 0:
                                    budget_total = 0
                                    custo_fixo_budget = 0
                                else:
                                    budget_total = custos_budget['Custo FP'].sum()
                                    custo_fixo_budget = custos_budget[custos_budget['Custo'] == 'Fixo']['Custo FP'].sum()

                                # 🔧 CORREÇÃO CRÍTICA (Flex): tudo que NÃO é Fixo é flexível
                                custo_nao_fixo_budget = budget_total - custo_fixo_budget
                                
                                # 🔧 CORREÇÃO: Calcular Flex Bud Total (Custo Total) para este período e veículo
                                # Flex Bud Fixo = BUD Fixo (não varia com volume)
                                proporcao_volume_real_bud = volume_real / volume_budget if volume_budget != 0 and pd.notnull(volume_budget) else 1.0
                                flex_bud_total_custo_total = custo_fixo_budget + (custo_nao_fixo_budget * proporcao_volume_real_bud)
                                
                                # Adicionar ao flex_data com Veículo (armazenar Custo Total, não CPU)
                                if tem_ano:
                                    flex_data.append({
                                        'Ano': ano,
                                        'Período': periodo,
                                        'Veículo': veiculo,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                                else:
                                    flex_data.append({
                                        'Período': periodo,
                                        'Veículo': veiculo,
                                        'Flex_Bud_Total': flex_bud_total_custo_total,
                                        'Volume_Real': volume_real
                                    })
                            
                            if len(flex_data) == 0:
                                flex_data = None
                            else:
                                flex_data = pd.DataFrame(flex_data)
                        
                        if flex_data is not None and len(flex_data) > 0:
                            # Agrupar Flex Bud Total e Volume Real por Veículo (somar todos os períodos)
                            if tipo_viz == "CPU (Custo por Unidade)":
                                # Para CPU: somar Flex Bud Total e Volume Real, depois recalcular CPU
                                budget_data = flex_data.groupby('Veículo').agg({
                                    'Flex_Bud_Total': 'sum',
                                    'Volume_Real': 'sum'
                                }).reset_index()
                                
                                # Recalcular CPU: Flex Bud Total agregado / Volume Real agregado
                                budget_data[coluna] = budget_data.apply(
                                    lambda row: (
                                        row['Flex_Bud_Total'] / row['Volume_Real']
                                        if pd.notnull(row['Volume_Real']) and row['Volume_Real'] != 0
                                        else 0
                                    ),
                                    axis=1
                                )
                                
                                # Manter apenas colunas necessárias
                                budget_data = budget_data[['Veículo', coluna]]
                            else:
                                # Para Custo Total: apenas somar Flex Bud Total
                                budget_data = flex_data.groupby('Veículo')['Flex_Bud_Total'].sum().reset_index()
                                budget_data.rename(columns={'Flex_Bud_Total': coluna}, inplace=True)
                            
                            # Filtrar apenas veículos que existem no chart_data
                            budget_data = budget_data[budget_data['Veículo'].isin(chart_data['Veículo'])].copy()
                            
                            if len(budget_data) > 0:
                                    # Criar linha tracejada de Flex Bud
                                    budget_data_legenda = budget_data.copy()
                                    budget_data_legenda['Tipo'] = 'Flex Bud'
                                    
                                    # IMPORTANTE: Usar a mesma ordem das barras (ordem_veiculos_barras)
                                    # Reordenar budget_data_legenda para seguir a ordem de ordem_veiculos_barras
                                    ordem_dict = {veiculo: idx for idx, veiculo in enumerate(ordem_veiculos_barras)}
                                    budget_data_legenda['_ordem'] = budget_data_legenda['Veículo'].map(ordem_dict)
                                    budget_data_legenda = budget_data_legenda[budget_data_legenda['_ordem'].notna()].copy()
                                    budget_data_legenda = budget_data_legenda.sort_values('_ordem')
                                    budget_data_legenda = budget_data_legenda.drop(columns=['_ordem'])
                                    
                                    # Usar a mesma ordem das barras (filtrando apenas veículos que existem no budget)
                                    ordem_veiculos = [v for v in ordem_veiculos_barras if v in budget_data_legenda['Veículo'].tolist()]
                                    
                                    linha_budget = alt.Chart(budget_data_legenda).mark_line(
                                        strokeDash=[10, 5],
                                        strokeWidth=1.5,
                                        opacity=0.8
                                    ).encode(
                                        x=alt.X(
                                            'Veículo:N',
                                            title='Veículo',
                                            sort=ordem_veiculos,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        y=alt.Y(
                                            f'{coluna}:Q',
                                            title=titulo_y,
                                            axis=alt.Axis(grid=False, domain=True, ticks=True)
                                        ),
                                        color=alt.Color(
                                            'Tipo:N',
                                            title='Legenda',
                                            scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                            legend=alt.Legend(
                                                title='Legenda',
                                                orient='bottom',
                                                titleFontSize=10,
                                                labelFontSize=9,
                                                titleAnchor='middle',
                                                direction='horizontal',
                                                symbolType='square'
                                            )
                                        ),
                                        strokeDash=alt.StrokeDash(
                                            'Tipo:N',
                                            scale=alt.Scale(domain=['Real', 'Flex Bud'], range=[[0], [10, 5]]),
                                            legend=None
                                        ),
                                        tooltip=[
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip('Tipo:N', title='Tipo'),
                                            alt.Tooltip(
                                                f'{coluna}:Q',
                                                title='Flex Bud',
                                                format=formato
                                            )
                                        ]
                                    )
                                    
                                    # Adicionar pontos na linha
                                    pontos_budget = alt.Chart(budget_data_legenda).mark_circle(
                                        size=80,
                                        opacity=0.9
                                    ).encode(
                                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                                        y=alt.Y(f'{coluna}:Q'),
                                        color=alt.Color(
                                            'Tipo:N',
                                            scale=alt.Scale(domain=['Real', 'Flex Bud'], range=['#4A90E2', '#FF6B35']),
                                            legend=None
                                        ),
                                        tooltip=[
                                            alt.Tooltip('Veículo:N', title='Veículo'),
                                            alt.Tooltip('Tipo:N', title='Tipo'),
                                            alt.Tooltip(
                                                f'{coluna}:Q',
                                                title='Flex Bud',
                                                format=formato
                                            )
                                        ]
                                    )
                                    
                                    # Adicionar rótulos
                                    rotulos_budget = alt.Chart(budget_data_legenda).mark_text(
                                        align='center',
                                        baseline='bottom',
                                        dy=-15,
                                        color='#FF6B35',
                                        fontSize=9,
                                        fontWeight='bold'
                                    ).encode(
                                        x=alt.X('Veículo:N', sort=ordem_veiculos),
                                        y=alt.Y(f'{coluna}:Q'),
                                        text=alt.Text(f'{coluna}:Q', format=formato)
                                    )
                                    
                                    linha_budget = linha_budget + pontos_budget + rotulos_budget
                    except Exception as e:
                        pass  # Silenciar erro, apenas não mostrar Flex Bud

            # Usar a ordem explícita para garantir sincronização com a linha pontilhada
            grafico_barras = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X(
                    'Veículo:N',
                    title='Veículo',
                    sort=ordem_veiculos_barras,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
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
                height=300,
                width='container'
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
                if tipo_viz == "CPU (Custo por Unidade)" and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    # MESMA LÓGICA DA TABELA (linha 1577-1589): Agrupar por Ano e Período, somar Total e Volume, calcular CPU
                    # Isso garante que valores sejam calculados corretamente, não somando CPUs já calculados
                    chart_data = df_data.groupby(['Ano', 'Período']).agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU (EXATAMENTE como a tabela linha 1582-1588)
                    chart_data[coluna] = chart_data.apply(
                        lambda row: (
                            row['Custo FP'] / row['Volume']
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
                if tipo_viz == "CPU (Custo por Unidade)" and 'Custo FP' in df_data.columns and 'Volume' in df_data.columns:
                    chart_data = df_data.groupby('Período').agg({
                        'Custo FP': 'sum',
                        'Volume': 'sum'
                    }).reset_index()
                    # Recalcular CPU
                    chart_data[coluna] = chart_data.apply(
                        lambda row: (
                            row['Custo FP'] / row['Volume']
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
                    sort=ordem_periodos,
                    axis=alt.Axis(grid=False, domain=True, ticks=True)
                ),
                y=alt.Y(f'{coluna}:Q', title=titulo_y, axis=alt.Axis(grid=False, domain=True, ticks=True)),
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
                height=300,
                width='container'
            )

        # Adicionar rótulos
        if 'Veículo' in df_data.columns:
            # Usar a ordem explícita para garantir sincronização
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                x=alt.X('Veículo:N', sort=ordem_veiculos_barras, title='Veículo'),
                text=alt.Text(f'{coluna}:Q', format=formato)
            )
        else:
            rotulos = grafico_barras.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='black',
                fontSize=9
            ).encode(
                text=alt.Text(f'{coluna}:Q', format=formato)
            )

        # Criar gráfico de delta (Real - Flex Bud) se linha_budget estiver disponível
        grafico_delta = None
        if linha_budget is not None and budget_data is not None and len(budget_data) > 0:
            try:
                # Calcular delta: Real - Flex Bud
                delta_data = chart_data.copy()
                budget_data_merge = budget_data.copy()
                budget_data_merge = budget_data_merge.rename(columns={coluna: f'{coluna}_FlexBud'})
                
                delta_data = delta_data.merge(
                    budget_data_merge[['Veículo', f'{coluna}_FlexBud']],
                    on='Veículo',
                    how='left'
                )
                
                # Calcular delta: Real - Flex Bud
                delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data[f'{coluna}_FlexBud'].fillna(0)
                
                # Calcular min e max do delta para a escala de cores
                # Usar valores absolutos simétricos para garantir que zero sempre seja o centro
                delta_min_abs = abs(delta_data['Delta'].min())
                delta_max_abs = abs(delta_data['Delta'].max())
                delta_abs_max = max(delta_min_abs, delta_max_abs)
                
                # Criar domínio simétrico baseado no maior valor absoluto
                # Isso garante que zero sempre fique no centro, independente dos filtros
                delta_min = -delta_abs_max if delta_abs_max > 0 else -1
                delta_max = delta_abs_max if delta_abs_max > 0 else 1
                
                # Usar a mesma ordem das barras para manter consistência
                ordem_veiculos_delta = ordem_veiculos_barras
                
                # Criar gráfico de barras para delta
                grafico_delta = alt.Chart(delta_data).mark_bar(
                    size=20
                ).encode(
                    x=alt.X(
                        'Veículo:N',
                        title='',
                        sort=ordem_veiculos_delta,
                        axis=alt.Axis(grid=False, domain=False, ticks=False, labels=False)
                    ),
                    y=alt.Y(
                        'Delta:Q',
                        title='Delta (Real - Flex Bud)',
                        axis=alt.Axis(grid=False, domain=True, ticks=True, labels=True)
                    ),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0, delta_max],
                            range=['#00AA00', '#FFFFFF', '#FF0000'],  # Verde (negativo) -> Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    ),
                    tooltip=[
                        alt.Tooltip('Veículo:N', title='Veículo'),
                        alt.Tooltip('Delta:Q', title='Delta (Real - Flex Bud)', format=',.2f'),
                        alt.Tooltip(f'{coluna}:Q', title='Real', format=',.2f'),
                        alt.Tooltip(f'{coluna}_FlexBud:Q', title='Flex Bud', format=',.2f')
                    ]
                ).properties(
                    height=38
                )
                
                # Adicionar rótulos de dados no gráfico de delta
                # Usar a mesma cor das barras (escala baseada no valor do Delta)
                rotulos_delta_positivos = alt.Chart(delta_data[delta_data['Delta'] >= 0]).mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[0, delta_max],
                            range=['#FFFFFF', '#FF0000'],  # Branco (zero) -> Vermelho (positivo)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                rotulos_delta_negativos = alt.Chart(delta_data[delta_data['Delta'] < 0]).mark_text(
                    align='center',
                    baseline='top',
                    dy=12,
                    fontSize=9,
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Veículo:N', sort=ordem_veiculos_delta),
                    y=alt.Y('Delta:Q'),
                    text=alt.Text('Delta:Q', format=',.2f'),
                    color=alt.Color(
                        'Delta:Q',
                        scale=alt.Scale(
                            domain=[delta_min, 0],
                            range=['#00AA00', '#FFFFFF'],  # Verde (negativo) -> Branco (zero)
                            type='linear',
                            nice=False
                        ),
                        legend=None
                    )
                )
                
                grafico_delta = grafico_delta + rotulos_delta_positivos + rotulos_delta_negativos
            except Exception as e:
                pass  # Silenciar erro, apenas não mostrar delta
        
        # Combinar gráfico de barras com linha de budget se disponível
        if linha_budget is not None:
            grafico_principal = alt.layer(
                grafico_barras,
                rotulos,
                linha_budget
            ).resolve_scale(
                x='shared',
                y='shared'
            )
            
            # Se temos gráfico de delta, combinar verticalmente (delta em cima)
            if grafico_delta is not None:
                grafico_final = alt.vconcat(
                    grafico_delta,
                    grafico_principal
                ).resolve_scale(
                    x='shared'
                )
            else:
                grafico_final = grafico_principal
        else:
            grafico_final = grafico_barras + rotulos

        return grafico_final
    except Exception as e:
        import traceback
        st.error(f"Erro ao criar gráfico Total por Veículo: {e}")
        st.error(traceback.format_exc())
        return None


# ==========================================
# TAB 3: TC Ext por Veíc
# ==========================================
if is_main_page:
    with tab3:
        st.subheader("🚗 TC Ext por Veíc")

        # Filtros locais (não impactam outras abas)
        filtros_col1, filtros_col2 = st.columns([1, 2])
        with filtros_col1:
            st.markdown("**📅 Ano**")
            tab3_ano_opcoes = ["Todos"]
            try:
                if 'df_total' in globals() and df_total is not None and 'Ano' in df_total.columns:
                    tab3_ano_opcoes += sorted(pd.to_numeric(df_total['Ano'], errors='coerce').dropna().unique().tolist())
            except Exception:
                pass
            tab3_ano_selecionado = st.selectbox(
                "Ano (Tab 3)",
                options=tab3_ano_opcoes,
                index=0,
                key="tab3_filtro_ano_be_tc",
                label_visibility="collapsed"
            )
        with filtros_col2:
            st.markdown("**🗓️ Período**")
            tab3_periodo_opcoes = ["Todos"]
            try:
                if 'df_total' in globals() and df_total is not None and 'Período' in df_total.columns:
                    periodos = df_total['Período'].dropna().astype(str).unique().tolist()
                    tab3_periodo_opcoes += periodos
            except Exception:
                pass
            tab3_periodos_selecionados = st.multiselect(
                "Período (Tab 3)",
                options=tab3_periodo_opcoes,
                default=["Todos"],
                key="tab3_filtro_periodo_be_tc",
                label_visibility="collapsed"
            )

        def _base_real_tab3():
            try:
                if df_filtrado is not None:
                    return df_filtrado
            except NameError:
                pass
            try:
                if df_total is not None:
                    return df_total
            except NameError:
                pass
            return None

        def _base_volume_tab3():
            try:
                if df_vol_filtrado_sidebar is not None:
                    return df_vol_filtrado_sidebar
            except NameError:
                pass
            return None

        def _aplicar_filtros_locais(df_in):
            if df_in is None or df_in.empty:
                return df_in
            df_out = df_in.copy()
            if tab3_ano_selecionado != "Todos" and 'Ano' in df_out.columns:
                df_out = df_out[pd.to_numeric(df_out['Ano'], errors='coerce') == tab3_ano_selecionado].copy()
            if tab3_periodos_selecionados and "Todos" not in tab3_periodos_selecionados and 'Período' in df_out.columns:
                df_out = df_out[df_out['Período'].astype(str).isin([str(x) for x in tab3_periodos_selecionados])].copy()
            return df_out

        def _carregar_budget_filtrado():
            try:
                df_budget = load_budget_data(ano_selecionado)
                df_budget_vol = load_budget_volume_data(ano_selecionado)
            except Exception:
                return None, None

            if df_budget is not None:
                df_budget = df_budget.copy()
                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Custo FP' in df_budget.columns:
                    if fator_conversao == "K (milhares)":
                        df_budget['Custo FP'] = df_budget['Custo FP'] / 1000
                    elif fator_conversao == "M (Milhões)":
                        df_budget['Custo FP'] = df_budget['Custo FP'] / 1000000
                if moeda_codigo != "BRL" and 'Custo FP' in df_budget.columns:
                    df_budget = _core_converter_coluna_moeda(df_budget, 'Custo FP', moeda_codigo, taxas_cambio)

            if df_budget_vol is not None:
                df_budget_vol = df_budget_vol.copy()

            df_budget = _aplicar_filtros_locais(df_budget)
            df_budget_vol = _aplicar_filtros_locais(df_budget_vol)
            return df_budget, df_budget_vol

        def _agregar_total(base_df, group_cols, coluna_valor):
            if base_df is None or base_df.empty or coluna_valor not in base_df.columns:
                return pd.DataFrame()
            df_tmp = base_df.copy()
            df_tmp = padronizar_colunas(df_tmp)
            df_tmp[coluna_valor] = pd.to_numeric(df_tmp[coluna_valor], errors='coerce').fillna(0)
            for col in group_cols:
                if col in df_tmp.columns:
                    df_tmp[col] = df_tmp[col].astype(str)
            return (
                df_tmp.groupby(group_cols, dropna=False)[coluna_valor]
                .sum()
                .reset_index()
                .rename(columns={coluna_valor: 'Despesa Primaria'})
            )

        def _agregar_cpu(base_df, vol_df, group_cols):
            if base_df is None or base_df.empty or 'Custo FP' not in base_df.columns:
                return pd.DataFrame()
            if vol_df is None or vol_df.empty or 'Volume' not in vol_df.columns:
                return pd.DataFrame()

            custo = base_df.copy()
            vol = vol_df.copy()
            custo = padronizar_colunas(custo)
            vol = padronizar_colunas(vol)

            custo['Custo FP'] = pd.to_numeric(custo['Custo FP'], errors='coerce').fillna(0)
            vol['Volume'] = pd.to_numeric(vol['Volume'], errors='coerce').fillna(0)

            for col in group_cols:
                if col in custo.columns:
                    custo[col] = custo[col].astype(str)
                if col in vol.columns:
                    vol[col] = vol[col].astype(str)

            custo_agr = custo.groupby(group_cols, dropna=False)['Custo FP'].sum().reset_index()
            vol_agr = vol.groupby(group_cols, dropna=False)['Volume'].sum().reset_index()

            df_cpu = custo_agr.merge(vol_agr, on=group_cols, how='left')
            df_cpu['CPU'] = np.where(
                (df_cpu['Volume'].notna()) & (df_cpu['Volume'] != 0),
                df_cpu['Custo FP'] / df_cpu['Volume'],
                0
            )
            return df_cpu

        def _flex_por_categoria(df_budget, df_budget_vol, df_real_vol, categoria, tipo_viz):
            if df_budget is None or df_budget_vol is None or df_real_vol is None:
                return None
            if categoria not in df_budget.columns or categoria not in df_budget_vol.columns or categoria not in df_real_vol.columns:
                return None
            if 'Custo FP' not in df_budget.columns or 'Volume' not in df_budget_vol.columns or 'Volume' not in df_real_vol.columns:
                return None
            if 'Custo' not in df_budget.columns:
                return None

            bud = df_budget.copy()
            bud_vol = df_budget_vol.copy()
            real_vol = df_real_vol.copy()

            bud['Custo FP'] = pd.to_numeric(bud['Custo FP'], errors='coerce').fillna(0)
            bud_vol['Volume'] = pd.to_numeric(bud_vol['Volume'], errors='coerce').fillna(0)
            real_vol['Volume'] = pd.to_numeric(real_vol['Volume'], errors='coerce').fillna(0)

            bud[categoria] = bud[categoria].astype(str)
            bud_vol[categoria] = bud_vol[categoria].astype(str)
            real_vol[categoria] = real_vol[categoria].astype(str)

            bud['Custo'] = bud['Custo'].apply(_normalizar_rotulo_custo)
            mask_fixo = _mask_custo_fixo(bud['Custo'])

            bud_total = bud.groupby(categoria, dropna=False)['Custo FP'].sum().reset_index()
            bud_fixo = bud.loc[mask_fixo].groupby(categoria, dropna=False)['Custo FP'].sum().reset_index()
            bud_fixo = bud_fixo.rename(columns={'Custo FP': 'Fixo'})

            bud_base = bud_total.merge(bud_fixo, on=categoria, how='left')
            bud_base['Fixo'] = bud_base['Fixo'].fillna(0)
            bud_base['NaoFixo'] = bud_base['Custo FP'] - bud_base['Fixo']

            vol_budget = bud_vol.groupby(categoria, dropna=False)['Volume'].sum().reset_index().rename(columns={'Volume': 'VolBudget'})
            vol_real = real_vol.groupby(categoria, dropna=False)['Volume'].sum().reset_index().rename(columns={'Volume': 'VolReal'})

            base = bud_base.merge(vol_budget, on=categoria, how='left').merge(vol_real, on=categoria, how='left')
            base['VolBudget'] = base['VolBudget'].fillna(0)
            base['VolReal'] = base['VolReal'].fillna(0)

            def _calc_flex(row):
                if row['VolBudget'] and row['VolBudget'] != 0:
                    return row['Fixo'] + (row['NaoFixo'] * (row['VolReal'] / row['VolBudget']))
                return row['Fixo'] + row['NaoFixo']

            base['Flex'] = base.apply(_calc_flex, axis=1)

            if tipo_viz == "CPU (Custo por Unidade)":
                base['CPU'] = np.where(base['VolReal'] != 0, base['Flex'] / base['VolReal'], 0)
                return base[[categoria, 'CPU']].rename(columns={categoria: categoria})

            return base[[categoria, 'Flex']].rename(columns={'Flex': 'Despesa Primaria'})

        def _resumo_tab3(base_real, base_vol, df_budget, df_budget_vol, tipo_viz, moeda):
            try:
                if base_real is None or base_real.empty or base_vol is None or base_vol.empty:
                    return None, None
                if df_budget is None or df_budget.empty or df_budget_vol is None or df_budget_vol.empty:
                    return None, None

                real_total = pd.to_numeric(base_real.get('Custo FP', 0), errors='coerce').fillna(0).sum()
                vol_real = pd.to_numeric(base_vol.get('Volume', 0), errors='coerce').fillna(0).sum()

                bud = df_budget.copy()
                bud['Custo FP'] = pd.to_numeric(bud.get('Custo FP', 0), errors='coerce').fillna(0)
                bud['Custo'] = bud.get('Custo', '').apply(_normalizar_rotulo_custo)
                mask_fixo = _mask_custo_fixo(bud['Custo']) if 'Custo' in bud.columns else pd.Series(False, index=bud.index)
                bud_total = bud['Custo FP'].sum()
                bud_fixo = bud.loc[mask_fixo, 'Custo FP'].sum() if 'Custo' in bud.columns else 0
                bud_nao_fixo = bud_total - bud_fixo

                bud_vol_total = pd.to_numeric(df_budget_vol.get('Volume', 0), errors='coerce').fillna(0).sum()

                if bud_vol_total and bud_vol_total != 0:
                    flex_total = bud_fixo + (bud_nao_fixo * (vol_real / bud_vol_total))
                else:
                    flex_total = bud_total

                if tipo_viz == "CPU (Custo por Unidade)":
                    total_val = (real_total / vol_real) if vol_real else 0
                    flex_val = (flex_total / vol_real) if vol_real else 0
                    bud_val = (bud_total / bud_vol_total) if bud_vol_total else 0
                else:
                    total_val = real_total
                    flex_val = flex_total
                    bud_val = bud_total

                linha_resumo = {
                    'BUD': bud_val,
                    'Flex BUD': flex_val,
                    'Custo FP': total_val,
                    'Flex Bud - BUD': flex_val - bud_val,
                    'Total - Flex Bud': total_val - flex_val,
                    'Total / Flex Bud': (total_val / flex_val) if flex_val else 0,
                    '_Volume_Real_Calculo': vol_real,
                    '_Volume_Budget_Calculo': bud_vol_total,
                }

                linha_resumo_formatado = {
                    'BUD': f"{moeda} {_formatar_num_ptbr(bud_val, 2)}",
                    'Flex BUD': f"{moeda} {_formatar_num_ptbr(flex_val, 2)}",
                    'Custo FP': f"{moeda} {_formatar_num_ptbr(total_val, 2)}",
                    'Flex Bud - BUD': f"{moeda} {_formatar_num_ptbr(flex_val - bud_val, 2)}",
                    'Total - Flex Bud': f"{moeda} {_formatar_num_ptbr(total_val - flex_val, 2)}",
                    '_Volume_Real_Calculo': _formatar_num_ptbr(vol_real, 0),
                    '_Volume_Budget_Calculo': _formatar_num_ptbr(bud_vol_total, 0),
                }

                return linha_resumo, linha_resumo_formatado
            except Exception:
                return None, None

        def _formatar_volume_por_categoria(df_in, categoria):
            if df_in is None or df_in.empty:
                return "-"
            if categoria not in df_in.columns or 'Volume' not in df_in.columns:
                return "-"
            df_tmp = df_in[[categoria, 'Volume']].copy()
            df_tmp = df_tmp[df_tmp[categoria].notna()]
            if df_tmp.empty:
                return "-"
            df_tmp['Volume'] = pd.to_numeric(df_tmp['Volume'], errors='coerce').fillna(0)
            agg = (
                df_tmp.groupby(categoria, dropna=False)['Volume']
                .sum()
                .reset_index()
                .sort_values('Volume', ascending=False)
            )
            partes = [f"{row[categoria]}: {_formatar_num_ptbr(row['Volume'], 0)}" for _, row in agg.iterrows()]
            return " | ".join(partes) if partes else "-"

        def _plot_rank(df_rank, coluna_valor, titulo, moeda, df_flex_line=None):
            if df_rank is None or df_rank.empty or coluna_valor not in df_rank.columns:
                st.info("ℹ️ Sem dados para o gráfico com os filtros atuais.")
                return

            df_plot = df_rank.copy()
            df_plot[coluna_valor] = pd.to_numeric(df_plot[coluna_valor], errors='coerce').fillna(0)
            df_plot = df_plot.sort_values(by=coluna_valor, ascending=False)

            eixo_x = df_plot.columns[0]
            valores = df_plot[coluna_valor].tolist()
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=df_plot[eixo_x],
                        y=valores,
                        text=[f"{v:,.2f}" for v in valores],
                        textposition='outside',
                        marker=dict(
                            color=valores,
                            colorscale='Blues',
                            showscale=False
                        ),
                        hovertemplate=(
                            f"%{{x}}<br>{coluna_valor}: %{{y:,.2f}}<extra></extra>"
                        ),
                    )
                ]
            )

            if df_flex_line is not None and not df_flex_line.empty and coluna_valor in df_flex_line.columns:
                df_line = df_flex_line.copy()
                df_line = df_line[df_line[eixo_x].isin(df_plot[eixo_x])]
                df_line = df_line.set_index(eixo_x).reindex(df_plot[eixo_x]).reset_index()
                fig.add_trace(
                    go.Scatter(
                        x=df_line[eixo_x],
                        y=df_line[coluna_valor],
                        mode='lines+markers+text',
                        name='Flex Bud',
                        line=dict(color='#FF6B35', width=2, dash='dash'),
                        marker=dict(size=6),
                        text=[f"{v:,.2f}" for v in df_line[coluna_valor].fillna(0).tolist()],
                        textposition='top center',
                        hovertemplate=(
                            f"%{{x}}<br>Flex Bud: %{{y:,.2f}}<extra></extra>"
                        )
                    )
                )

            fig.update_layout(
                title=titulo,
                xaxis_title=eixo_x,
                yaxis_title=f"{coluna_valor} ({moeda})" if coluna_valor != 'CPU' else coluna_valor,
                margin=dict(l=20, r=20, t=60, b=40),
                height=460,
            )
            fig.update_xaxes(showgrid=False, zeroline=False)
            fig.update_yaxes(showgrid=False, zeroline=False)
            st.plotly_chart(fig, use_container_width=True)

        base_real = _aplicar_filtros_locais(_base_real_tab3())
        base_vol = _aplicar_filtros_locais(_base_volume_tab3())
        df_budget_tab3, df_budget_vol_tab3 = _carregar_budget_filtrado()

        if base_real is None or base_real.empty:
            st.warning("⚠️ Sem dados Real para o Tab 3 com os filtros atuais.")
        else:
            moeda_label = moeda_simbolo if 'moeda_simbolo' in locals() else "R$"

            # Resumo (estilo Tab 1)
            linha_resumo_tab3, linha_resumo_tab3_formatado = _resumo_tab3(
                base_real,
                base_vol,
                df_budget_tab3,
                df_budget_vol_tab3,
                tipo_visualizacao,
                moeda_label
            )
            if linha_resumo_tab3 and linha_resumo_tab3_formatado:
                exibir_caixas_resumo(linha_resumo_tab3, linha_resumo_tab3_formatado, tipo_visualizacao, mostrar_volumes=True)
                st.markdown("---")

            # Gráfico por Oficina
            if 'Oficina' in base_real.columns:
                st.markdown(
                    f"**📦 Volume Real (Oficinas):** {_formatar_volume_por_categoria(base_vol, 'Oficina')}")
                st.markdown(
                    f"**📦 Volume Budget (Oficinas):** {_formatar_volume_por_categoria(df_budget_vol_tab3, 'Oficina')}")
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    df_cpu_of = _agregar_cpu(base_real, base_vol, ['Oficina'])
                    df_flex_of = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_of = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Oficina',
                                "CPU (Custo por Unidade)"
                            )
                        except Exception:
                            df_flex_of = None
                    _plot_rank(df_cpu_of, 'CPU', "📊 CPU por Oficina", moeda_label, df_flex_of)
                else:
                    coluna_valor = 'Custo FP' if 'Custo FP' in base_real.columns else 'Despesa Primaria'
                    df_tot_of = _agregar_total(base_real, ['Oficina'], coluna_valor)
                    df_flex_of = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_of = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Oficina',
                                "Custo Total"
                            )
                        except Exception:
                            df_flex_of = None
                    _plot_rank(df_tot_of, 'Despesa Primaria', "📊 Custo Total por Oficina", moeda_label, df_flex_of)
            else:
                st.info("ℹ️ Coluna 'Oficina' não encontrada para o gráfico por Oficina.")

            # Gráfico por Veículo
            if 'Veículo' in base_real.columns:
                st.markdown(
                    f"**📦 Volume Real (Veículos):** {_formatar_volume_por_categoria(base_vol, 'Veículo')}")
                st.markdown(
                    f"**📦 Volume Budget (Veículos):** {_formatar_volume_por_categoria(df_budget_vol_tab3, 'Veículo')}")
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    df_cpu_veic = _agregar_cpu(base_real, base_vol, ['Veículo'])
                    df_flex_veic = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_veic = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Veículo',
                                "CPU (Custo por Unidade)"
                            )
                        except Exception:
                            df_flex_veic = None
                    _plot_rank(df_cpu_veic, 'CPU', "📊 CPU por Veículo", moeda_label, df_flex_veic)
                else:
                    coluna_valor = 'Custo FP' if 'Custo FP' in base_real.columns else 'Despesa Primaria'
                    df_tot_veic = _agregar_total(base_real, ['Veículo'], coluna_valor)
                    df_flex_veic = None
                    if df_budget_tab3 is not None and df_budget_vol_tab3 is not None:
                        try:
                            df_flex_veic = _flex_por_categoria(
                                df_budget_tab3,
                                df_budget_vol_tab3,
                                base_vol,
                                'Veículo',
                                "Custo Total"
                            )
                        except Exception:
                            df_flex_veic = None
                    _plot_rank(df_tot_veic, 'Despesa Primaria', "📊 Custo Total por Veículo", moeda_label, df_flex_veic)
            else:
                st.info("ℹ️ Coluna 'Veículo' não encontrada para o gráfico por Veículo.")

    # Variáveis necessárias para o tab4 (definidas dentro do bloco is_main_page)
    tem_veiculo = 'Veículo' in df_visualizacao.columns
    tem_oficina = 'Oficina' in df_visualizacao.columns
    tem_periodo = 'Período' in df_visualizacao.columns

    # ==========================================
    # TAB 4: Detalhe Real
    # ==========================================
    with tab4:
        # Expander para mostrar/ocultar todo o bloco de tabelas
        with st.expander("📊 **Tabelas Detalhadas**", expanded=False):
            # Tabela: Veículo, Oficina e Períodos (seguindo filtros da sidebar)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                st.subheader("📋 Tabela - CPU por Veículo, Oficina e Período")
            else:
                st.subheader("📋 Tabela - Custo Total por Veículo, Oficina e Período")
                
            if tem_veiculo and tem_oficina and tem_periodo:
                # 🔧 Garantir variáveis sempre definidas (evita NameError em caminhos alternativos)
                colunas_periodos = []
                coluna_periodo_pivot = 'Período'
                df_visualizacao_pivot = df_visualizacao.copy()
                df_real_agr_cpu = None

                # Base de volume real já filtrada com a sidebar (não deve ser mergeada linha-a-linha no custo)
                df_volume_real_base = None
                if 'df_vol_filtrado_sidebar' in locals() and df_vol_filtrado_sidebar is not None and hasattr(df_vol_filtrado_sidebar, 'columns'):
                    df_volume_real_base = df_vol_filtrado_sidebar.copy()
                    if 'Volume' in df_volume_real_base.columns:
                        df_volume_real_base['Volume'] = pd.to_numeric(df_volume_real_base['Volume'], errors='coerce')

                # Em CPU, NÃO fazer merge de Volume linha-a-linha (isso multiplica o volume por linha de custo).
                # O CPU deve ser calculado após agregação: sum(Total)/sum(Volume).

                # Usar coluna_visualizacao que já está definida
                if coluna_visualizacao in df_visualizacao.columns:
                    # As variáveis colunas_periodos, coluna_periodo_pivot e colunas_adicionais
                    # já foram definidas no bloco anterior (tabela de total). Se não foram, criar agora.
                    # Definir tem_multiplos_anos antes do try para garantir que está disponível
                    tem_multiplos_anos = 'Ano' in df_visualizacao.columns and df_visualizacao['Ano'].nunique() > 1
                    
                    try:
                        # Tentar usar as variáveis já definidas
                        _ = colunas_periodos
                        _ = coluna_periodo_pivot
                        _ = df_visualizacao_pivot
                        _ = colunas_adicionais
                    except NameError:
                        # Se não existirem, criar agora (mesma lógica)
                        pass
                        
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
                        
                    if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_visualizacao_pivot.columns:
                        # Agregar custo e volume separadamente no grão correto
                        chaves_agr = ['Oficina', 'Veículo', 'Período']
                        if tem_multiplos_anos and 'Ano' in df_visualizacao_pivot.columns:
                            chaves_agr.append('Ano')

                        df_custo_agr = (
                            df_visualizacao_pivot.groupby(chaves_agr, dropna=False)
                            .agg({'Custo FP': 'sum'})
                            .reset_index()
                        )

                        if (
                            df_volume_real_base is not None
                            and 'Volume' in df_volume_real_base.columns
                            and all(k in df_volume_real_base.columns for k in chaves_agr)
                        ):
                            df_vol_agr = (
                                df_volume_real_base.groupby(chaves_agr, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                        else:
                            df_vol_agr = df_custo_agr[chaves_agr].copy()
                            df_vol_agr['Volume'] = 0

                        df_real_agr = pd.merge(df_custo_agr, df_vol_agr, on=chaves_agr, how='outer')
                        df_real_agr['Volume'] = pd.to_numeric(df_real_agr['Volume'], errors='coerce').fillna(0)
                        df_real_agr['Custo FP'] = pd.to_numeric(df_real_agr['Custo FP'], errors='coerce').fillna(0)
                        df_real_agr['CPU'] = np.where(
                            (df_real_agr['Volume'].notna()) & (df_real_agr['Volume'] != 0),
                            df_real_agr['Custo FP'] / df_real_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos:
                            df_real_agr['Período_Ano'] = (
                                df_real_agr['Período'].astype(str) + ' ' + df_real_agr['Ano'].astype(str)
                            )
                            col_pivot_cpu = 'Período_Ano'
                        else:
                            col_pivot_cpu = 'Período'

                        df_real_agr_cpu = df_real_agr.copy()

                        df_tabela_ref = df_real_agr.pivot_table(
                            index=['Oficina', 'Veículo'],
                            columns=col_pivot_cpu,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    elif coluna_visualizacao in df_visualizacao_pivot.columns:
                        df_tabela_ref = df_visualizacao_pivot.pivot_table(
                            index=['Oficina', 'Veículo'],
                            columns=coluna_periodo_pivot,
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )
                    else:
                        st.warning("⚠️ Não foi possível montar a tabela em CPU (colunas Total/Volume ausentes).")
                        df_tabela_ref = pd.DataFrame(index=pd.MultiIndex(levels=[[], []], codes=[[], []], names=['Oficina', 'Veículo']))
                        
                    if tem_multiplos_anos:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_visualizacao_pivot['Ano'].unique())
                            
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                coluna_combinada = f"{mes.capitalize()} {ano}"
                                if coluna_combinada in df_tabela_ref.columns:
                                    colunas_ordenadas.append(coluna_combinada)
                            
                        colunas_restantes = [
                            col for col in df_tabela_ref.columns 
                            if col not in colunas_ordenadas
                        ]
                        colunas_periodos = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [col for col in meses_ordem if col in df_tabela_ref.columns]
                        colunas_restantes = [col for col in df_tabela_ref.columns if col not in meses_ordem]
                        colunas_periodos = colunas_existentes + colunas_restantes
                    
                # Definir colunas_adicionais também
                colunas_excluidas = {
                    'Ano', 'Período', 'Período_Ano', 'Veículo', 'Oficina', 
                    'Custo FP', 'Despesa Primaria', 'CPU', 'Volume', coluna_visualizacao,
                    'Dt.lçto.', 'Data Lançamento', 'Data de Lançamento',
                    'Soma de Percentual', 'Soma Percentual', 'Percentual', 'Soma %'
                }
                # Manter a ordem original das colunas do DataFrame
                colunas_adicionais = [
                    col for col in df_visualizacao.columns 
                    if col not in colunas_excluidas
                ]
                
            # Usar as mesmas colunas de períodos já determinadas
            # Para CPU: calcular SEMPRE a partir de custo agregado + volume agregado (evita volume duplicado)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                if df_real_agr_cpu is None:
                    # Se ainda não foi calculado acima (por algum caminho alternativo), calcular aqui
                    chaves_agr = ['Oficina', 'Veículo', 'Período']
                    tem_multiplos_anos = 'Ano' in df_visualizacao_pivot.columns and df_visualizacao_pivot['Ano'].nunique() > 1
                    if tem_multiplos_anos and 'Ano' in df_visualizacao_pivot.columns:
                        chaves_agr.append('Ano')

                    if 'Custo FP' not in df_visualizacao_pivot.columns:
                        if 'Despesa Primaria' in df_visualizacao_pivot.columns:
                            df_visualizacao_pivot['Custo FP'] = df_visualizacao_pivot['Despesa Primaria']
                        elif coluna_visualizacao in df_visualizacao_pivot.columns:
                            df_visualizacao_pivot['Custo FP'] = df_visualizacao_pivot[coluna_visualizacao]

                    df_custo_agr = (
                        df_visualizacao_pivot.groupby(chaves_agr, dropna=False)
                        .agg({'Custo FP': 'sum'})
                        .reset_index()
                    )

                    if (
                        df_volume_real_base is not None
                        and 'Volume' in df_volume_real_base.columns
                        and all(k in df_volume_real_base.columns for k in chaves_agr)
                    ):
                        df_vol_agr = (
                            df_volume_real_base.groupby(chaves_agr, dropna=False)
                            .agg({'Volume': 'sum'})
                            .reset_index()
                        )
                    else:
                        df_vol_agr = df_custo_agr[chaves_agr].copy()
                        df_vol_agr['Volume'] = 0

                    df_real_agr_cpu = pd.merge(df_custo_agr, df_vol_agr, on=chaves_agr, how='outer')
                    df_real_agr_cpu['Volume'] = pd.to_numeric(df_real_agr_cpu['Volume'], errors='coerce').fillna(0)
                    df_real_agr_cpu['Custo FP'] = pd.to_numeric(df_real_agr_cpu['Custo FP'], errors='coerce').fillna(0)
                    df_real_agr_cpu['CPU'] = np.where(
                        (df_real_agr_cpu['Volume'].notna()) & (df_real_agr_cpu['Volume'] != 0),
                        df_real_agr_cpu['Custo FP'] / df_real_agr_cpu['Volume'],
                        0
                    )

                    if tem_multiplos_anos:
                        df_real_agr_cpu['Período_Ano'] = (
                            df_real_agr_cpu['Período'].astype(str) + ' ' + df_real_agr_cpu['Ano'].astype(str)
                        )
                        coluna_periodo_pivot = 'Período_Ano'
                    else:
                        coluna_periodo_pivot = 'Período'

                df_tabela = df_real_agr_cpu.pivot_table(
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

            # Se não foi possível determinar colunas_periodos acima, usar as colunas da própria tabela
            if not colunas_periodos:
                colunas_periodos = list(df_tabela.columns)
            
            # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
            for col in colunas_periodos:
                if col not in df_tabela.columns:
                    df_tabela[col] = 0
                
            # Reordenar para usar exatamente as mesmas colunas
            df_tabela = df_tabela[colunas_periodos]
                
            # Calcular total por linha
            # Para CPU, recalcular a partir de Total e Volume agregados por Oficina e Veículo (sem volume duplicado)
            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                df_total_oficina_veiculo = df_real_agr_cpu.groupby(['Oficina', 'Veículo'], dropna=False).agg({
                    'Custo FP': 'sum',
                    'Volume': 'sum'
                }).reset_index()
                # Calcular CPU - VETORIZADO
                df_total_oficina_veiculo['CPU'] = np.where(
                    (df_total_oficina_veiculo['Volume'].notna()) & (df_total_oficina_veiculo['Volume'] != 0),
                    df_total_oficina_veiculo['Custo FP'] / df_total_oficina_veiculo['Volume'],
                    0
                )
                # Fazer merge com df_tabela para adicionar coluna Total
                df_tabela = df_tabela.reset_index()
                df_tabela = pd.merge(
                    df_tabela,
                    df_total_oficina_veiculo[['Oficina', 'Veículo', 'CPU']],
                    on=['Oficina', 'Veículo'],
                    how='left'
                )
                df_tabela.rename(columns={'CPU': 'Custo FP'}, inplace=True)
                df_tabela = df_tabela.set_index(['Oficina', 'Veículo'])
            else:
                df_tabela['Custo FP'] = df_tabela.sum(axis=1)
            try:
                df_tabela = df_tabela.sort_values(['Oficina', 'Veículo'])
            except KeyError:
                if isinstance(df_tabela.index, pd.MultiIndex):
                    df_tabela = df_tabela.sort_index()
                else:
                    if 'Oficina' not in df_tabela.columns:
                        df_tabela['Oficina'] = pd.NA
                    if 'Veículo' not in df_tabela.columns:
                        df_tabela['Veículo'] = pd.NA
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
                    colunas_finais = ['Oficina', 'Veículo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Custo FP']
                    # Manter apenas colunas que existem
                    colunas_finais = [col for col in colunas_finais if col in df_tabela.columns]
                    df_tabela = df_tabela[colunas_finais]
            else:
                # Reordenar colunas para garantir que Oficina venha antes de Veículo
                colunas_ordenadas = ['Oficina', 'Veículo'] + [col for col in df_tabela.columns 
                                                              if col not in ['Oficina', 'Veículo']]
                df_tabela = df_tabela[colunas_ordenadas]
                
            # Formatar valores baseado no tipo de visualização - OTIMIZADO
            # Aplicar formatação apenas nas colunas numéricas (exceto Veículo, Oficina e colunas adicionais)
            df_tabela_formatado = df_tabela.copy()
            # Obter colunas adicionais que foram realmente adicionadas à tabela
            colunas_adicionais_na_tabela = [
                col for col in df_tabela_formatado.columns 
                if col not in ['Oficina', 'Veículo'] + colunas_periodos + ['Custo FP']
            ]
            colunas_formatar = [
                col for col in df_tabela_formatado.columns 
                if col not in ['Veículo', 'Oficina'] + colunas_adicionais_na_tabela and
                df_tabela_formatado[col].dtype in ['float64', 'float32', 'int64', 'int32']
            ]
            # Formatação vetorizada
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                for col in colunas_formatar:
                    df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
            else:
                # Adicionar sufixo baseado no fator de conversão
                sufixo = ""
                if fator_conversao:
                    if fator_conversao == "K (milhares)":
                        sufixo = " K"
                    elif fator_conversao == "M (Milhões)":
                        sufixo = " M"
                for col in colunas_formatar:
                    df_tabela_formatado[col] = df_tabela_formatado[col].map(lambda x: f"{x:,.2f}{sufixo}" if isinstance(x, (int, float)) else x)
            
            # Função para formatar valores (definida antes de ser usada)
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    # NOTA: Os dados já estão convertidos na base, então apenas formatamos
                    simbolo = obter_simbolo_moeda(moeda_codigo)
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        # Adicionar sufixo baseado no fator de conversão (apenas para Custo Total)
                        sufixo = ""
                        if tipo_visualizacao == "Custo Total" and fator_conversao:
                            if fator_conversao == "K (milhares)":
                                sufixo = " K"
                            elif fator_conversao == "M (Milhões)":
                                sufixo = " M"
                        return f"{simbolo} {val:,.2f}{sufixo}"
                return val
            
            # Agrupar por Oficina e renderizar blocos
            oficinas = df_tabela_formatado['Oficina'].unique()

            if len(oficinas) == 0:
                st.info("Nenhum dado encontrado para exibir por Oficina.")
            else:
                for oficina in sorted(oficinas):
                    # Filtrar dados da oficina
                    df_oficina = df_tabela_formatado[df_tabela_formatado['Oficina'] == oficina].copy()

                    # Calcular total da oficina
                    if 'Custo FP' in df_oficina.columns:
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                            df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                            total_custo_of = float(pd.to_numeric(df_base_of['Custo FP'], errors='coerce').fillna(0).sum())
                            vol_of = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                            total_oficina = (total_custo_of / vol_of) if vol_of not in (0, None) else 0.0
                        else:
                            df_oficina_numerico = df_tabela[df_tabela['Oficina'] == oficina].copy()
                            total_oficina = float(pd.to_numeric(df_oficina_numerico['Custo FP'], errors='coerce').fillna(0).sum())
                        total_formatado = formatar_valor(total_oficina, tipo_visualizacao)
                    else:
                        total_formatado = "N/A"

                    # Criar container para cada oficina (substituindo expander para evitar aninhamento)
                    with st.container():
                        st.markdown(
                            f"### 🏭 **{oficina}** - Total: {total_formatado} ("
                            f"{len(df_oficina)} veículo{'s' if len(df_oficina) > 1 else ''})"
                        )

                        # Em CPU, o TOTAL por mês é ponderado por Volume (Total/Volume).
                        # Se o volume muda entre meses, o TOTAL pode mudar mesmo com CPUs por veículo parecidas.
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                            with st.expander("📦 Volume por período (explica variações do TOTAL em CPU)", expanded=False):
                                df_tmp_vol = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                col_per = (
                                    'Período_Ano'
                                    if coluna_periodo_pivot == 'Período_Ano' and 'Período_Ano' in df_tmp_vol.columns
                                    else 'Período'
                                )
                                vol_por_periodo = (
                                    df_tmp_vol.groupby(col_per, dropna=False)['Volume']
                                    .sum()
                                    .astype(float)
                                )

                                # Ordenar colunas no mesmo padrão exibido na tabela
                                ordem_cols = [c for c in colunas_periodos if c in vol_por_periodo.index]
                                extras = [c for c in vol_por_periodo.index if c not in set(ordem_cols)]
                                vol_por_periodo = vol_por_periodo.reindex(ordem_cols + extras, fill_value=0.0)

                                st.dataframe(pd.DataFrame([vol_por_periodo]), width="stretch")
                                st.caption(
                                    "No modo CPU, o TOTAL do mês é calculado como sum(Total)/sum(Volume). "
                                    "Se o volume muda entre meses, o TOTAL pode mudar mesmo com CPUs por veículo iguais após arredondamento."
                                )

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
                            if col not in ['Veículo'] + colunas_periodos + ['Custo FP']
                        ]

                        # Adicionar valores vazios para colunas adicionais na linha de total
                        for col in colunas_adicionais_na_tabela:
                            if col in df_oficina_numerico.columns:
                                linha_total[col] = pd.NA

                        # Adicionar totais por coluna (meses e Total)
                        for col in df_oficina_numerico.columns:
                            if col not in ['Veículo'] + colunas_adicionais_na_tabela:
                                if col in colunas_periodos:
                                    # Para colunas de período, se for CPU, calcular Total/Volume do período
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                        df_tmp = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                        if coluna_periodo_pivot == 'Período_Ano' and 'Período_Ano' in df_tmp.columns:
                                            df_tmp_p = df_tmp[df_tmp['Período_Ano'] == col]
                                        else:
                                            df_tmp_p = df_tmp[df_tmp['Período'] == col]

                                        total_periodo = float(pd.to_numeric(df_tmp_p['Custo FP'], errors='coerce').fillna(0).sum())
                                        volume_periodo = float(pd.to_numeric(df_tmp_p['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_periodo = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0
                                        linha_total[col] = formatar_valor(cpu_periodo, tipo_visualizacao)
                                    else:
                                        # Para Custo Total, somar normalmente
                                        if df_oficina_numerico[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                                            total_col = df_oficina_numerico[col].sum()
                                            linha_total[col] = formatar_valor(total_col, tipo_visualizacao)
                                elif col == 'Custo FP':
                                    # Para a coluna Total, se for CPU, calcular Total/Volume geral da oficina
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                        df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                        total_geral = float(pd.to_numeric(df_base_of['Custo FP'], errors='coerce').fillna(0).sum())
                                        volume_geral = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
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

                        st.dataframe(df_oficina_display, width="stretch")
            
            # Botão de download da tabela (dentro do expander, fora do loop)
            if st.button(
                "📥 Baixar Tabela por Veículo e Oficina (Excel)",
                width="stretch",
                key="download_tabela_veiculo_oficina_tc"
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
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_real_agr_cpu is not None:
                                df_base_of = df_real_agr_cpu[df_real_agr_cpu['Oficina'] == oficina].copy()
                                for col in colunas_periodos:
                                    if col in df_oficina_download.columns:
                                        if coluna_periodo_pivot == 'Período_Ano' and 'Período_Ano' in df_base_of.columns:
                                            df_p = df_base_of[df_base_of['Período_Ano'] == col]
                                        else:
                                            df_p = df_base_of[df_base_of['Período'] == col]

                                        total_p = float(pd.to_numeric(df_p['Custo FP'], errors='coerce').fillna(0).sum())
                                        vol_p = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                                        linha_total_download[col] = (total_p / vol_p) if vol_p not in (0, None) else 0.0

                                if 'Custo FP' in df_oficina_download.columns:
                                    total_geral = float(pd.to_numeric(df_base_of['Custo FP'], errors='coerce').fillna(0).sum())
                                    vol_geral = float(pd.to_numeric(df_base_of['Volume'], errors='coerce').fillna(0).sum())
                                    linha_total_download['Custo FP'] = (total_geral / vol_geral) if vol_geral not in (0, None) else 0.0
                            else:
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
                        file_name = f"TC_Veiculos_tabela_veiculo_oficina_{tipo_nome}.xlsx"
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
                colunas_faltando = []
                if not tem_veiculo:
                    colunas_faltando.append("Veículo")
                if not tem_oficina:
                    colunas_faltando.append("Oficina")
                if not tem_periodo:
                    colunas_faltando.append("Período")
                st.info(f"ℹ️ Colunas necessárias não encontradas para criar a tabela: {', '.join(colunas_faltando)}")
            
            # Tabela dinâmica: Valor por Oficina e Período
            if ('Oficina' in df_visualizacao.columns and
                    'Período' in df_visualizacao.columns):
                # Determinar título
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.subheader("📋 Tabela Dinâmica - CPU por Oficina e Período")
                else:
                    st.subheader("📋 Tabela Dinâmica - Valor por Oficina e Período")
                
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
                                coluna_combinada = f"{mes.capitalize()} {ano}"
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
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [col for col in meses_ordem if col in df_pivot.columns]
                        colunas_restantes = [col for col in df_pivot.columns if col not in meses_ordem]
                        df_pivot = df_pivot[colunas_existentes + colunas_restantes]

                    # Calcular total por linha
                    # Regra crítica (documentação): em CPU, o total deve ser ponderado por volume.
                    # Importante: NÃO usar Volume mergeado no df_visualizacao (duplica volume por linha de custo).
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_visualizacao_cpu = df_visualizacao.copy()
                        if 'Custo FP' not in df_visualizacao_cpu.columns:
                            if 'Despesa Primaria' in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Custo FP'] = df_visualizacao_cpu['Despesa Primaria']
                            elif 'Custo' in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Custo FP'] = df_visualizacao_cpu['Custo']
                            elif coluna_visualizacao in df_visualizacao_cpu.columns:
                                df_visualizacao_cpu['Custo FP'] = df_visualizacao_cpu[coluna_visualizacao]

                        chaves_cpu = ['Oficina', 'Período']
                        if tem_multiplos_anos and 'Ano' in df_visualizacao_cpu.columns:
                            chaves_cpu.append('Ano')

                        df_custo_agr = (
                            df_visualizacao_cpu.groupby(chaves_cpu, dropna=False)
                            .agg({'Custo FP': 'sum'})
                            .reset_index()
                        )

                        df_volume_base = df_vol_filtrado_sidebar.copy() if 'df_vol_filtrado_sidebar' in locals() else pd.DataFrame()
                        if (not df_volume_base.empty) and 'Volume' in df_volume_base.columns and all(k in df_volume_base.columns for k in chaves_cpu):
                            df_vol_agr = (
                                df_volume_base.groupby(chaves_cpu, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                        else:
                            df_vol_agr = df_custo_agr[chaves_cpu].copy()
                            df_vol_agr['Volume'] = 0

                        # 🔧 CORREÇÃO CRÍTICA: incluir chaves com volume mesmo sem custo (Total=0)
                        df_cpu_agr = pd.merge(df_custo_agr, df_vol_agr, on=chaves_cpu, how='outer')
                        df_cpu_agr['Volume'] = pd.to_numeric(df_cpu_agr['Volume'], errors='coerce').fillna(0)
                        df_cpu_agr['Custo FP'] = pd.to_numeric(df_cpu_agr['Custo FP'], errors='coerce').fillna(0)
                        df_cpu_agr['CPU'] = np.where(
                            (df_cpu_agr['Volume'].notna()) & (df_cpu_agr['Volume'] != 0),
                            df_cpu_agr['Custo FP'] / df_cpu_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos and 'Ano' in df_cpu_agr.columns:
                            df_cpu_agr['Período_Ano'] = df_cpu_agr['Período'].astype(str) + ' ' + df_cpu_agr['Ano'].astype(str)
                            col_periodo_cpu = 'Período_Ano'
                        else:
                            col_periodo_cpu = 'Período'

                        df_pivot = df_cpu_agr.pivot_table(
                            index='Oficina',
                            columns=col_periodo_cpu,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )

                        # Ordenação de colunas (cronológica)
                        if tem_multiplos_anos and 'Ano' in df_cpu_agr.columns:
                            colunas_ordenadas = []
                            anos_unicos = sorted(df_cpu_agr['Ano'].unique())
                            for ano in anos_unicos:
                                for mes in ORDEM_MESES:
                                    coluna_combinada = f"{mes.capitalize()} {ano}"
                                    if coluna_combinada in df_pivot.columns:
                                        colunas_ordenadas.append(coluna_combinada)
                            colunas_restantes = [col for col in df_pivot.columns if col not in colunas_ordenadas]
                            df_pivot = df_pivot[colunas_ordenadas + colunas_restantes]
                        else:
                            meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                            colunas_existentes = [col for col in meses_ordem if col in df_pivot.columns]
                            colunas_restantes = [col for col in df_pivot.columns if col not in meses_ordem]
                            df_pivot = df_pivot[colunas_existentes + colunas_restantes]

                        # Evitar colisão quando existe um Período chamado "Total".
                        # Se a pivot tiver uma coluna "Total" vinda do Período, ela conflita com a coluna de total geral.
                        if 'Custo FP' in df_pivot.columns:
                            df_pivot = df_pivot.rename(columns={'Custo FP': 'Total (Período)'})

                        # Total ponderado por volume por Oficina (não somar CPUs)
                        df_total_oficina = df_cpu_agr.groupby('Oficina', dropna=False).agg({'Custo FP': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_oficina['CPU_Total'] = np.where(
                            (df_total_oficina['Volume'].notna()) & (df_total_oficina['Volume'] != 0),
                            df_total_oficina['Custo FP'] / df_total_oficina['Volume'],
                            0
                        )
                        df_pivot['Custo FP'] = df_pivot.index.to_series().map(
                            df_total_oficina.set_index('Oficina')['CPU_Total']
                        ).fillna(0)
                    else:
                        # Evitar colisão quando existe um Período chamado "Total".
                        if 'Custo FP' in df_pivot.columns:
                            df_pivot = df_pivot.rename(columns={'Custo FP': 'Total (Período)'})

                        df_pivot['Custo FP'] = df_pivot.sum(axis=1)

                    df_pivot = df_pivot.sort_values('Custo FP', ascending=False)

                    # Formatar valores baseado no tipo de visualização
                    def formatar_valor(val, tipo):
                        if isinstance(val, (int, float)):
                            if tipo == "CPU (Custo por Unidade)":
                                return f"{val:,.2f}"
                            else:
                                return f"R$ {val:,.2f}"
                        return val

                    # Aplicar formatação
                    df_pivot_formatado = df_pivot.copy()
                    for col in df_pivot_formatado.columns:
                        df_pivot_formatado[col] = df_pivot_formatado[col].apply(
                            lambda x: formatar_valor(x, tipo_visualizacao)
                        )

                    # Adicionar linha de somatório (TOTAL)
                    try:
                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                            # Total ponderado por Volume: CPU = sum(Total)/sum(Volume)
                            if 'df_cpu_agr' in locals() and df_cpu_agr is not None and not df_cpu_agr.empty and 'Custo FP' in df_cpu_agr.columns and 'Volume' in df_cpu_agr.columns:
                                df_tot_periodo = df_cpu_agr.groupby(col_periodo_cpu, dropna=False).agg({'Custo FP': 'sum', 'Volume': 'sum'}).reset_index()
                                df_tot_periodo['CPU_TOTAL'] = np.where(
                                    (df_tot_periodo['Volume'].notna()) & (df_tot_periodo['Volume'] != 0),
                                    df_tot_periodo['Custo FP'] / df_tot_periodo['Volume'],
                                    0
                                )
                                mapa_tot = df_tot_periodo.set_index(col_periodo_cpu)['CPU_TOTAL'].to_dict()

                                total_geral = float(pd.to_numeric(df_cpu_agr['Custo FP'], errors='coerce').fillna(0).sum())
                                volume_geral = float(pd.to_numeric(df_cpu_agr['Volume'], errors='coerce').fillna(0).sum())
                                cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0

                                linha_total_fmt = {c: '' for c in df_pivot_formatado.columns}
                                for c in df_pivot_formatado.columns:
                                    if c == 'Custo FP':
                                        linha_total_fmt[c] = formatar_valor(cpu_geral, tipo_visualizacao)
                                    elif c in mapa_tot:
                                        linha_total_fmt[c] = formatar_valor(float(mapa_tot.get(c, 0) or 0), tipo_visualizacao)
                                df_pivot_formatado = pd.concat(
                                    [df_pivot_formatado, pd.DataFrame([linha_total_fmt], index=['**TOTAL**'])]
                                )
                        else:
                            # Custo Total: somar colunas
                            linha_total = df_pivot.sum(axis=0, numeric_only=True)
                            linha_total_fmt = {}
                            for c in df_pivot_formatado.columns:
                                if c in linha_total.index:
                                    linha_total_fmt[c] = formatar_valor(float(linha_total[c]), tipo_visualizacao)
                                else:
                                    linha_total_fmt[c] = ''
                            df_pivot_formatado = pd.concat(
                                [df_pivot_formatado, pd.DataFrame([linha_total_fmt], index=['**TOTAL**'])]
                            )
                    except Exception:
                        # Se falhar, apenas não exibe o total (não quebrar a tela)
                        pass
                    
                    # Remover colunas 'mes', 'Mes', 'QTD', 'soma_percentuais' e 'Soma_Percentuais' se existirem
                    colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                    for col in colunas_para_remover:
                        if col in df_pivot_formatado.columns:
                            df_pivot_formatado = df_pivot_formatado.drop(columns=[col])

                    st.dataframe(df_pivot_formatado, width="stretch")

                    # Botão de download da Tabela Dinâmica
                    if st.button(
                        "📥 Baixar Tabela Dinâmica (Excel)",
                        width="stretch",
                        key="download_pivot_tc"
                    ):
                        with st.spinner("Gerando arquivo da tabela dinâmica..."):
                            try:
                                # Obter pasta Downloads do usuário
                                downloads_path = os.path.join(
                                    os.path.expanduser("~"), "Downloads"
                                )
                                file_name = "TC_Veiculos_tabela_dinamica.xlsx"
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
        
        # Tabela: Total por Veículo e Períodos (sem Oficina) - no final do bloco
        # Determinar título do expander
        # ATUALIZADO: Usando mesma lógica do gráfico para linha de total geral
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            titulo_expander_total = "📋 **Tabela - CPU Total por Veículo e Período**"
        else:
            titulo_expander_total = "📋 **Tabela - Custo Total por Veículo e Período**"
            
        # Usar expander no mesmo formato do expander de "Tabelas Detalhadas"
        with st.expander(titulo_expander_total, expanded=False):
            if tem_veiculo and tem_periodo:
                # 🔧 Para CPU: garantir insumos no dataframe-base (df_visualizacao)
                # A tabela total usa df_visualizacao para agrupar/calcular CPU; se Volume/Total não estiverem aqui,
                # ela cai em fallbacks e pode renderizar tudo como None.
                df_visualizacao_total = df_visualizacao.copy()
                df_agrupado_periodo = None
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    if 'Custo FP' not in df_visualizacao_total.columns:
                        if 'Despesa Primaria' in df_visualizacao_total.columns:
                            df_visualizacao_total['Custo FP'] = df_visualizacao_total['Despesa Primaria']
                        elif 'Custo' in df_visualizacao_total.columns:
                            df_visualizacao_total['Custo FP'] = df_visualizacao_total['Custo']
                        elif coluna_visualizacao in df_visualizacao_total.columns:
                            df_visualizacao_total['Custo FP'] = df_visualizacao_total[coluna_visualizacao]

                    if 'Custo FP' in df_visualizacao_total.columns:
                        df_visualizacao_total['Custo FP'] = pd.to_numeric(
                            df_visualizacao_total['Custo FP'], errors='coerce'
                        )

                # Em CPU, NUNCA usar Volume mergeado em df_visualizacao_total (duplica volume por linha de custo).

                # Em CPU, evitamos merge de volume linha-a-linha para não inflar o denominador.
                # Inicializar variáveis para CPU
                df_tabela_total_valores = None
                df_tabela_total_volumes = None
                
                # Para CPU, usar a mesma lógica do gráfico: agrupar diretamente por Veículo e Período+Ano
                # Isso garante que apenas períodos com dados sejam considerados (evita problemas com volumes sem custos)
                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_visualizacao_total.columns:
                    # Verificar se há múltiplos anos
                    tem_multiplos_anos = 'Ano' in df_visualizacao_total.columns and df_visualizacao_total['Ano'].nunique() > 1

                    # CPU deve ser calculado por (Total agregado) / (Volume agregado).
                    # Volume real frequentemente vem no grão Oficina+Veículo+Período; para a tabela TOTAL por veículo,
                    # somamos volume sobre oficinas antes do merge.
                    chaves_merge = ['Veículo', 'Período']
                    if tem_multiplos_anos and 'Ano' in df_visualizacao_total.columns:
                        chaves_merge.append('Ano')

                    df_custo_agr = (
                        df_visualizacao_total.groupby(chaves_merge, dropna=False)
                        .agg({'Custo FP': 'sum'})
                        .reset_index()
                    )

                    df_volume_base = df_vol_filtrado_sidebar.copy() if 'df_vol_filtrado_sidebar' in locals() else pd.DataFrame()
                    if not df_volume_base.empty and 'Volume' in df_volume_base.columns and all(k in df_volume_base.columns for k in chaves_merge):
                        df_volume_agr = (
                            df_volume_base.groupby(chaves_merge, dropna=False)
                            .agg({'Volume': 'sum'})
                            .reset_index()
                        )
                    else:
                        # Sem volume válido no grão necessário -> cria volume 0 para evitar tabela nula/None
                        df_volume_agr = df_custo_agr[chaves_merge].copy()
                        df_volume_agr['Volume'] = 0

                    # 🔧 CORREÇÃO CRÍTICA: incluir veículos/períodos com volume mesmo sem custo (custo=0)
                    df_agrupado_periodo = pd.merge(
                        df_custo_agr,
                        df_volume_agr,
                        on=chaves_merge,
                        how='outer'
                    )
                    df_agrupado_periodo['Volume'] = pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0)
                    df_agrupado_periodo['Custo FP'] = pd.to_numeric(df_agrupado_periodo['Custo FP'], errors='coerce').fillna(0)
                        
                    # Agrupar por Veículo e Período+Ano, somar Total e Volume, calcular CPU
                    # Usar a mesma coluna_periodo_pivot que foi determinada anteriormente
                    if tem_multiplos_anos:
                        # Criar coluna Período_Ano para fazer o pivot (usar o mesmo formato)
                        df_agrupado_periodo[coluna_periodo_pivot] = (
                            df_agrupado_periodo['Período'].astype(str) + ' ' + 
                            df_agrupado_periodo['Ano'].astype(str)
                        )
                    else:
                        # Sem múltiplos anos: coluna_periodo_pivot permanece 'Período'
                        pass
                        
                    # Calcular CPU por período (mesma lógica do gráfico) - vetorizado
                    df_agrupado_periodo['CPU'] = np.where(
                        (df_agrupado_periodo['Volume'].notna()) & (df_agrupado_periodo['Volume'] != 0),
                        df_agrupado_periodo['Custo FP'] / df_agrupado_periodo['Volume'],
                        0
                    )
                        
                    # Criar tabelas pivot de Total e Volume apenas com dados existentes
                    # Usar coluna_periodo_pivot que já foi determinada
                    df_tabela_total_valores = df_agrupado_periodo.pivot_table(
                        index='Veículo',
                        columns=coluna_periodo_pivot,
                        values='Custo FP',
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

                    # Garantir nome do índice para reset_index consistente
                    df_tabela_total.index.name = 'Veículo'
                        
                    # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                    for col in colunas_periodos:
                        if col not in df_tabela_total.columns:
                            df_tabela_total[col] = 0
                        
                    # Reordenar para usar exatamente as mesmas colunas
                    df_tabela_total = df_tabela_total[colunas_periodos]
                        
                    # Calcular total por linha: usar EXATAMENTE a mesma lógica do gráfico "CPU por Veículo"
                    # Primeiro agrupar por Veículo e Período+Ano, depois por Veículo
                    if tem_multiplos_anos:
                        df_total_veiculo = df_agrupado_periodo.groupby('Veículo', dropna=False).agg({
                            'Custo FP': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                    else:
                        df_total_veiculo = df_agrupado_periodo.groupby('Veículo', dropna=False).agg({
                            'Custo FP': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        
                    # Recalcular CPU (mesma lógica do gráfico linha 2080)
                    df_total_veiculo['CPU'] = df_total_veiculo.apply(
                        lambda row: (
                            row['Custo FP'] / row['Volume']
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
                    df_tabela_total.rename(columns={'CPU': 'Custo FP'}, inplace=True)
                elif tipo_visualizacao == "CPU (Custo por Unidade)" and coluna_visualizacao in df_visualizacao_pivot.columns:
                    try:
                        df_tabela_total = df_visualizacao_pivot.pivot_table(
                            index='Veículo',
                            columns=coluna_periodo_pivot,
                            values=coluna_visualizacao,
                            aggfunc='sum',
                            fill_value=0
                        )
                    except KeyError:
                        st.warning(f"⚠️ Coluna '{coluna_visualizacao}' não encontrada para montar a tabela por veículo.")
                        df_tabela_total = pd.DataFrame(index=pd.Index([], name='Veículo'))
                elif tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.warning("⚠️ Não foi possível montar a tabela em CPU (faltam colunas Total/Volume).")
                    df_tabela_total = pd.DataFrame(index=pd.Index([], name='Veículo'))
                else:
                    # Para Custo Total, usar soma normalmente
                    if coluna_visualizacao not in df_visualizacao_pivot.columns:
                        st.warning(f"⚠️ Coluna '{coluna_visualizacao}' não encontrada para montar a tabela por veículo.")
                        df_tabela_total = pd.DataFrame(index=pd.Index([], name='Veículo'))
                    else:
                        try:
                            df_tabela_total = df_visualizacao_pivot.pivot_table(
                                index='Veículo',
                                columns=coluna_periodo_pivot,
                                values=coluna_visualizacao,
                                aggfunc='sum',
                                fill_value=0
                            )
                        except KeyError:
                            st.warning(f"⚠️ Coluna '{coluna_visualizacao}' não encontrada para montar a tabela por veículo.")
                            df_tabela_total = pd.DataFrame(index=pd.Index([], name='Veículo'))
                    
                # Se "Veículo" ficou como índice em algum caminho, trazer para coluna antes de reordenar
                if df_tabela_total.index.name == 'Veículo' and 'Veículo' not in df_tabela_total.columns:
                    df_tabela_total = df_tabela_total.reset_index()

                # Garantir que tenha as mesmas colunas (adicionar colunas faltantes com 0)
                for col in colunas_periodos:
                    if col not in df_tabela_total.columns:
                        df_tabela_total[col] = 0

                # Reordenar mantendo a coluna "Veículo" (não pode sumir, senão vira RangeIndex numérico)
                # e preservando (quando existir) a coluna "Total" ponderada (CPU)
                colunas_base = ['Veículo'] if 'Veículo' in df_tabela_total.columns else []
                colunas_reordenadas = colunas_base + colunas_periodos
                if 'Custo FP' in df_tabela_total.columns:
                    colunas_reordenadas = colunas_reordenadas + ['Custo FP']
                df_tabela_total = df_tabela_total[colunas_reordenadas]

                # Calcular total por linha
                # Em CPU, manter o Total ponderado por Volume (não somar CPUs)
                if tipo_visualizacao != "CPU (Custo por Unidade)":
                    df_tabela_total['Custo FP'] = df_tabela_total[colunas_periodos].sum(axis=1)
                else:
                    if 'Custo FP' not in df_tabela_total.columns:
                        df_tabela_total['Custo FP'] = 0
                
            # Resetar índice apenas se necessário e seguro
            if df_tabela_total.index.name == 'Veículo' and 'Veículo' not in df_tabela_total.columns:
                df_tabela_total = df_tabela_total.reset_index()
            
            # Ordenar com segurança (evita KeyError quando a coluna não existe)
            if 'Veículo' in df_tabela_total.columns:
                df_tabela_total = df_tabela_total.sort_values('Veículo')
            else:
                df_tabela_total['Veículo'] = pd.NA
                
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
                    df_colunas_adicionais = df_visualizacao_total.groupby('Veículo')[colunas_adicionais_validas].first().reset_index()
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
                    colunas_finais = ['Veículo'] + colunas_adicionais_ordenadas + colunas_periodos + ['Custo FP']
                    # Manter apenas colunas que existem
                    colunas_finais = [col for col in colunas_finais if col in df_tabela_total.columns]
                    df_tabela_total = df_tabela_total[colunas_finais]
                
            # Formatar valores baseado no tipo de visualização
            def formatar_valor(val, tipo):
                if isinstance(val, (int, float)):
                    if tipo == "CPU (Custo por Unidade)":
                        return f"{val:,.2f}"
                    else:
                        return f"R$ {val:,.2f}"
                return val
                
            # Aplicar formatação apenas nas colunas numéricas (exceto Veículo e colunas adicionais)
            df_tabela_total_formatado = df_tabela_total.copy()
            # Obter colunas adicionais que foram realmente adicionadas à tabela
            colunas_adicionais_na_tabela = [
                col for col in df_tabela_total_formatado.columns 
                if col not in ['Veículo'] + colunas_periodos + ['Custo FP']
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
                    linha_total_geral[col] = pd.NA
                
            # Adicionar totais por coluna (meses e Total)
            # LÓGICA CORRIGIDA: Quando filtra por um veículo, o total deve ser o valor desse veículo
            if tipo_visualizacao == "CPU (Custo por Unidade)" and df_agrupado_periodo is not None:
                # Total por período e Total geral: sempre baseado na base agregada (custo+volume)
                for col in colunas_periodos:
                    if col in df_tabela_total.columns:
                        df_p = df_agrupado_periodo[df_agrupado_periodo[coluna_periodo_pivot] == col]
                        total_periodo = float(pd.to_numeric(df_p['Custo FP'], errors='coerce').fillna(0).sum())
                        volume_periodo = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                        cpu_periodo = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0
                        linha_total_geral[col] = formatar_valor(cpu_periodo, tipo_visualizacao)

                if 'Custo FP' in df_tabela_total.columns:
                    total_geral = float(pd.to_numeric(df_agrupado_periodo['Custo FP'], errors='coerce').fillna(0).sum())
                    volume_geral = float(pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0).sum())
                    cpu_geral = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
                    linha_total_geral['Custo FP'] = formatar_valor(cpu_geral, tipo_visualizacao)
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
                
            st.dataframe(df_tabela_total_display, width="stretch")
                
            # Botão de download da tabela total
            if st.button(
                "📥 Baixar Tabela Total por Veículo (Excel)",
                width="stretch",
                key="download_tabela_total_veiculo_tc"
            ):
                with st.spinner("Gerando arquivo da tabela total..."):
                    try:
                        # Criar DataFrame completo para download (com linha de total)
                        df_total_download = df_tabela_total.copy()
                            
                        # Adicionar linha de total
                        linha_total_download = {'Veículo': 'TOTAL'}
                        # Em CPU, a linha TOTAL deve ser ponderada por volume (sem usar Volume mergeado em linhas de custo)
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and df_agrupado_periodo is not None:
                            for col in colunas_periodos:
                                if col in df_tabela_total.columns:
                                    df_p = df_agrupado_periodo[df_agrupado_periodo[coluna_periodo_pivot] == col]
                                    total_periodo = float(pd.to_numeric(df_p['Custo FP'], errors='coerce').fillna(0).sum())
                                    volume_periodo = float(pd.to_numeric(df_p['Volume'], errors='coerce').fillna(0).sum())
                                    linha_total_download[col] = (total_periodo / volume_periodo) if volume_periodo not in (0, None) else 0.0

                            if 'Custo FP' in df_tabela_total.columns:
                                total_geral = float(pd.to_numeric(df_agrupado_periodo['Custo FP'], errors='coerce').fillna(0).sum())
                                volume_geral = float(pd.to_numeric(df_agrupado_periodo['Volume'], errors='coerce').fillna(0).sum())
                                linha_total_download['Custo FP'] = (total_geral / volume_geral) if volume_geral not in (0, None) else 0.0
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
                        file_name = f"TC_Veiculos_tabela_total_veiculo_{tipo_nome}.xlsx"
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

        # Exibir tabela filtrada (TODAS as linhas)
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
            st.dataframe(df_display, width="stretch")

            # Botão de download da Tabela Filtrada
            if st.button(
                "📥 Baixar Tabela Filtrada (Excel)",
                width="stretch",
                key="download_filtered_tc"
            ):
                with st.spinner("Gerando arquivo da tabela filtrada..."):
                    try:
                        # Obter pasta Downloads do usuário
                        downloads_path = os.path.join(
                            os.path.expanduser("~"), "Downloads"
                        )
                        file_name = "TC_Veiculos_tabela_filtrada.xlsx"
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

    # ==========================================
    # TAB 5: Detalhe Budget
    # ==========================================
    with tab5:
        # Bases do Budget (custos e volume) já são filtradas pelos mesmos filtros da sidebar no TAB 1
        df_budget_base = df_budget_filtrado if 'df_budget_filtrado' in locals() else None
        df_budget_vol_base = df_budget_vol_filtrado if 'df_budget_vol_filtrado' in locals() else None

        if df_budget_base is None or df_budget_base is False or (hasattr(df_budget_base, 'empty') and df_budget_base.empty):
            st.info("ℹ️ Sem dados de Budget para exibir com os filtros atuais.")
        else:
            df_visualizacao_budget = df_budget_base.copy()

            # Definir coluna de visualização para o Budget
            # OBS: no modo CPU, a coluna 'CPU' não deve ser calculada linha-a-linha;
            # será calculada após agregações usando custos + volume agregados (regra da documentação).
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                coluna_visualizacao_budget = 'Custo FP'
            else:
                coluna_visualizacao_budget = 'Custo FP' if 'Custo FP' in df_visualizacao_budget.columns else 'Despesa Primaria'

            # Garantir Volume no Budget quando necessário (CPU será calculado após agregações)
            if tipo_visualizacao == "CPU (Custo por Unidade)":
                if df_budget_vol_base is None or (hasattr(df_budget_vol_base, 'empty') and df_budget_vol_base.empty) or 'Volume' not in getattr(df_budget_vol_base, 'columns', []):
                    try:
                        df_budget_vol_base = load_budget_volume_data(ano_selecionado)
                    except Exception:
                        df_budget_vol_base = None

                if 'Custo FP' not in df_visualizacao_budget.columns:
                    if 'Despesa Primaria' in df_visualizacao_budget.columns:
                        df_visualizacao_budget['Custo FP'] = df_visualizacao_budget['Despesa Primaria']
                    elif 'Custo' in df_visualizacao_budget.columns:
                        df_visualizacao_budget['Custo FP'] = df_visualizacao_budget['Custo']

                if 'Custo FP' in df_visualizacao_budget.columns:
                    df_visualizacao_budget['Custo FP'] = pd.to_numeric(df_visualizacao_budget['Custo FP'], errors='coerce')
                if df_budget_vol_base is not None and 'Volume' in getattr(df_budget_vol_base, 'columns', []):
                    df_budget_vol_base = df_budget_vol_base.copy()
                    df_budget_vol_base['Volume'] = pd.to_numeric(df_budget_vol_base['Volume'], errors='coerce')

            tem_veiculo_budget = 'Veículo' in df_visualizacao_budget.columns
            tem_oficina_budget = 'Oficina' in df_visualizacao_budget.columns
            tem_periodo_budget = 'Período' in df_visualizacao_budget.columns

            # ------------------------------
            # Tabelas detalhadas (Budget)
            # ------------------------------
            with st.expander("📊 **Tabelas Detalhadas (Budget)**", expanded=False):
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    st.subheader("📋 Tabela - CPU (Budget) por Veículo, Oficina e Período")
                else:
                    st.subheader("📋 Tabela - Custo Total (Budget) por Veículo, Oficina e Período")

                if tem_veiculo_budget and tem_oficina_budget and tem_periodo_budget and coluna_visualizacao_budget in df_visualizacao_budget.columns:
                    df_budget_pivot = df_visualizacao_budget.copy()

                    df_budget_agr_cpu = None

                    tem_multiplos_anos_budget = 'Ano' in df_budget_pivot.columns and df_budget_pivot['Ano'].nunique() > 1

                    # Chaves reais (não usar Período_Ano para merge/agrupamento)
                    chaves_agr_budget = ['Oficina', 'Veículo', 'Período']
                    if tem_multiplos_anos_budget and 'Ano' in df_budget_pivot.columns:
                        chaves_agr_budget.append('Ano')

                    # Para CPU: agregar custo e volume separadamente (evita multiplicar Volume por linha de custo)
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_custo_agr = df_budget_pivot.groupby(chaves_agr_budget, dropna=False).agg({'Custo FP': 'sum'}).reset_index()

                        # Volume Budget pode não ter Veículo.
                        # Se NÃO tiver Veículo, ratear o Volume do Budget por veículo usando a participação do volume Real
                        # (mesmos filtros). Isso evita dividir custo de um veículo pelo volume total da oficina.
                        bud_vol_tem_veiculo = (
                            df_budget_vol_base is not None
                            and hasattr(df_budget_vol_base, 'columns')
                            and 'Veículo' in df_budget_vol_base.columns
                        )

                        if (
                            (not bud_vol_tem_veiculo)
                            and df_budget_vol_base is not None
                            and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                        ):
                            st.error(
                                "❌ ERRO NA EXTRAÇÃO: o Volume do Budget precisa conter a coluna 'Veículo'. "
                                "Não é mais permitido rateio/fallback no app."
                            )
                            st.info(
                                "💡 Refaça a extração do BUDGET (página 'Extração de Dados') e corrija a aba 'Volume BDG' no Excel."
                            )
                            st.stop()
                        else:
                            # Volume Budget tem Veículo (ou não existe volume): usar merge direto no mesmo grão
                            vol_keys = []
                            if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                                vol_keys = [k for k in chaves_agr_budget if k in df_budget_vol_base.columns]

                            if (
                                df_budget_vol_base is not None
                                and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                                and vol_keys
                            ):
                                df_vol_agr = (
                                    df_budget_vol_base
                                    .groupby(vol_keys, dropna=False)
                                    .agg({'Volume': 'sum'})
                                    .reset_index()
                                )
                                df_budget_agr = pd.merge(df_custo_agr, df_vol_agr, on=vol_keys, how='left')
                            else:
                                df_budget_agr = df_custo_agr.copy()
                                df_budget_agr['Volume'] = 0

                        df_budget_agr['Volume'] = pd.to_numeric(df_budget_agr['Volume'], errors='coerce').fillna(0)

                        df_budget_agr['CPU'] = np.where(
                            (df_budget_agr['Volume'].notna()) & (df_budget_agr['Volume'] != 0),
                            df_budget_agr['Custo FP'] / df_budget_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos_budget:
                            df_budget_agr['Período_Ano'] = (
                                df_budget_agr['Período'].astype(str) + ' ' + df_budget_agr['Ano'].astype(str)
                            )
                            coluna_periodo_pivot_budget = 'Período_Ano'
                        else:
                            coluna_periodo_pivot_budget = 'Período'

                        # Guardar base agregada (Total/Volume) para cálculos de TOTAL por oficina/período
                        df_budget_agr_cpu = df_budget_agr.copy()

                        df_tabela_budget = df_budget_agr.pivot_table(
                            index=['Oficina', 'Veículo'],
                            columns=coluna_periodo_pivot_budget,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        # Custo Total: soma normal
                        coluna_periodo_pivot_budget = 'Período'
                        if tem_multiplos_anos_budget:
                            df_budget_pivot['Período_Ano'] = (
                                df_budget_pivot['Período'].astype(str) + ' ' + df_budget_pivot['Ano'].astype(str)
                            )
                            coluna_periodo_pivot_budget = 'Período_Ano'

                        df_tabela_budget = df_budget_pivot.pivot_table(
                            index=['Oficina', 'Veículo'],
                            columns=coluna_periodo_pivot_budget,
                            values=coluna_visualizacao_budget,
                            aggfunc='sum',
                            fill_value=0
                        )

                    # Ordenar colunas de períodos
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_budget_pivot['Ano'].unique())
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in df_tabela_budget.columns:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in df_tabela_budget.columns if c not in colunas_ordenadas]
                        colunas_periodos_budget = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [c for c in meses_ordem if c in df_tabela_budget.columns]
                        colunas_restantes = [c for c in df_tabela_budget.columns if c not in meses_ordem]
                        colunas_periodos_budget = colunas_existentes + colunas_restantes

                    for col in colunas_periodos_budget:
                        if col not in df_tabela_budget.columns:
                            df_tabela_budget[col] = 0
                    df_tabela_budget = df_tabela_budget[colunas_periodos_budget]

                    # Total por linha
                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_budget_agr_cpu is not None:
                        df_total_oficina_veiculo = df_budget_agr_cpu.groupby(['Oficina', 'Veículo'], dropna=False).agg({'Custo FP': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_oficina_veiculo['CPU'] = np.where(
                            (df_total_oficina_veiculo['Volume'].notna()) & (df_total_oficina_veiculo['Volume'] != 0),
                            df_total_oficina_veiculo['Custo FP'] / df_total_oficina_veiculo['Volume'],
                            0
                        )
                        df_tabela_budget = df_tabela_budget.reset_index()
                        df_tabela_budget = pd.merge(
                            df_tabela_budget,
                            df_total_oficina_veiculo[['Oficina', 'Veículo', 'CPU']],
                            on=['Oficina', 'Veículo'],
                            how='left'
                        )
                        df_tabela_budget.rename(columns={'CPU': 'Custo FP'}, inplace=True)
                    else:
                        df_tabela_budget = df_tabela_budget.reset_index()
                        df_tabela_budget['Custo FP'] = df_tabela_budget[colunas_periodos_budget].sum(axis=1)

                    # Formatação simples
                    df_tabela_budget_fmt = df_tabela_budget.copy()
                    cols_num = [c for c in df_tabela_budget_fmt.columns if c not in ['Oficina', 'Veículo']]
                    for c in cols_num:
                        if c in df_tabela_budget_fmt.columns:
                            df_tabela_budget_fmt[c] = df_tabela_budget_fmt[c].map(
                                lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x
                            )

                    # Renderizar por Oficina (igual ao Detalhe Real): subtotal no título e tabela sem coluna Oficina
                    oficinas_budget = df_tabela_budget_fmt['Oficina'].dropna().unique().tolist()
                    if len(oficinas_budget) == 0:
                        st.info("Nenhum dado encontrado para exibir por Oficina (Budget).")
                    else:
                        for oficina in sorted(oficinas_budget):
                            df_oficina_fmt = df_tabela_budget_fmt[df_tabela_budget_fmt['Oficina'] == oficina].copy()
                            df_oficina_num = df_tabela_budget[df_tabela_budget['Oficina'] == oficina].copy()

                            # Subtotal da oficina
                            if (
                                tipo_visualizacao == "CPU (Custo por Unidade)"
                                and df_budget_agr_cpu is not None
                                and 'Custo FP' in df_budget_agr_cpu.columns
                                and 'Volume' in df_budget_agr_cpu.columns
                            ):
                                df_base_oficina = df_budget_agr_cpu[df_budget_agr_cpu['Oficina'] == oficina].copy()
                                total_of = float(pd.to_numeric(df_base_oficina['Custo FP'], errors='coerce').fillna(0).sum())
                                vol_of = float(pd.to_numeric(df_base_oficina['Volume'], errors='coerce').fillna(0).sum())
                                subtotal_of = (total_of / vol_of) if vol_of not in (0, None) else 0.0
                            else:
                                subtotal_of = float(pd.to_numeric(df_oficina_num.get('Custo FP', 0), errors='coerce').fillna(0).sum())

                            subtotal_of_fmt = f"{subtotal_of:,.2f}"

                            with st.container():
                                st.markdown(
                                    f"### 🏭 **{oficina}** - Total: {subtotal_of_fmt} ("
                                    f"{len(df_oficina_fmt)} veículo{'s' if len(df_oficina_fmt) > 1 else ''})"
                                )

                                # Remover coluna Oficina (já está no título)
                                df_oficina_display = df_oficina_fmt.drop(columns=['Oficina'])

                                # Linha TOTAL por oficina
                                linha_total = {'Veículo': '**TOTAL**'}
                                for col in colunas_periodos_budget:
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and df_budget_agr_cpu is not None:
                                        df_tmp = df_budget_agr_cpu[df_budget_agr_cpu['Oficina'] == oficina]
                                        df_tmp_p = df_tmp[df_tmp[coluna_periodo_pivot_budget] == col]
                                        total_p = float(pd.to_numeric(df_tmp_p['Custo FP'], errors='coerce').fillna(0).sum())
                                        vol_p = float(pd.to_numeric(df_tmp_p['Volume'], errors='coerce').fillna(0).sum())
                                        cpu_p = (total_p / vol_p) if vol_p not in (0, None) else 0.0
                                        linha_total[col] = f"{cpu_p:,.2f}"
                                    else:
                                        soma_p = float(pd.to_numeric(df_oficina_num.get(col, 0), errors='coerce').fillna(0).sum())
                                        linha_total[col] = f"{soma_p:,.2f}"

                                # Coluna Total
                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                    linha_total['Custo FP'] = f"{subtotal_of:,.2f}"
                                else:
                                    soma_total = float(pd.to_numeric(df_oficina_num.get('Custo FP', 0), errors='coerce').fillna(0).sum())
                                    linha_total['Custo FP'] = f"{soma_total:,.2f}"

                                df_oficina_display = pd.concat(
                                    [df_oficina_display, pd.DataFrame([linha_total])],
                                    ignore_index=True
                                )

                                st.dataframe(df_oficina_display, width="stretch")
                else:
                    st.info("ℹ️ Colunas necessárias não encontradas para montar a tabela detalhada de Budget.")

                # Tabela dinâmica: Budget por Oficina e Período
                if (
                    'Oficina' in df_visualizacao_budget.columns
                    and 'Período' in df_visualizacao_budget.columns
                    and coluna_visualizacao_budget in df_visualizacao_budget.columns
                ):
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        st.subheader("📋 Tabela Dinâmica - CPU (Budget) por Oficina e Período")
                    else:
                        st.subheader("📋 Tabela Dinâmica - Valor (Budget) por Oficina e Período")

                    df_pivot_budget = df_visualizacao_budget.copy()
                    tem_multiplos_anos_budget = 'Ano' in df_pivot_budget.columns and df_pivot_budget['Ano'].nunique() > 1

                    # Chaves reais para agregação (sem Período_Ano)
                    chaves_of = ['Oficina', 'Período']
                    if tem_multiplos_anos_budget and 'Ano' in df_pivot_budget.columns:
                        chaves_of.append('Ano')

                    if tem_multiplos_anos_budget:
                        col_pivot_budget = 'Período_Ano'
                    else:
                        col_pivot_budget = 'Período'

                    # Regra crítica (documentação): em CPU, calcular a partir de custo agregado e volume agregado
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_custo_of = df_pivot_budget.groupby(chaves_of, dropna=False).agg({'Custo FP': 'sum'}).reset_index()

                        vol_keys_of = []
                        if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                            vol_keys_of = [k for k in chaves_of if k in df_budget_vol_base.columns]

                        if (
                            df_budget_vol_base is not None
                            and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                            and vol_keys_of
                        ):
                            df_vol_of = (
                                df_budget_vol_base
                                .groupby(vol_keys_of, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                            df_cpu_agr = pd.merge(df_custo_of, df_vol_of, on=vol_keys_of, how='left')
                        else:
                            df_cpu_agr = df_custo_of.copy()
                            df_cpu_agr['Volume'] = 0

                        df_cpu_agr['Volume'] = pd.to_numeric(df_cpu_agr['Volume'], errors='coerce').fillna(0)
                        df_cpu_agr['CPU'] = np.where(
                            (df_cpu_agr['Volume'].notna()) & (df_cpu_agr['Volume'] != 0),
                            df_cpu_agr['Custo FP'] / df_cpu_agr['Volume'],
                            0
                        )

                        if tem_multiplos_anos_budget:
                            df_cpu_agr['Período_Ano'] = (
                                df_cpu_agr['Período'].astype(str) + ' ' + df_cpu_agr['Ano'].astype(str)
                            )

                        df_pivot = df_cpu_agr.pivot_table(
                            index='Oficina',
                            columns=col_pivot_budget,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )

                        # Total ponderado por Oficina
                        df_total_of = df_cpu_agr.groupby('Oficina', dropna=False).agg({'Custo FP': 'sum', 'Volume': 'sum'}).reset_index()
                        df_total_of['Total_CPU'] = np.where(
                            (df_total_of['Volume'].notna()) & (df_total_of['Volume'] != 0),
                            df_total_of['Custo FP'] / df_total_of['Volume'],
                            0
                        )
                        df_pivot['Custo FP'] = df_pivot.index.to_series().map(
                            df_total_of.set_index('Oficina')['Total_CPU']
                        ).fillna(0)
                    else:
                        # Custo Total: soma normal
                        if tem_multiplos_anos_budget:
                            df_pivot_budget['Período_Ano'] = (
                                df_pivot_budget['Período'].astype(str) + ' ' + df_pivot_budget['Ano'].astype(str)
                            )
                        df_pivot = df_pivot_budget.pivot_table(
                            index='Oficina',
                            columns=col_pivot_budget,
                            values=coluna_visualizacao_budget,
                            aggfunc='sum',
                            fill_value=0
                        )
                        df_pivot['Custo FP'] = df_pivot.sum(axis=1)

                    # Ordenar colunas de períodos (sem mexer na coluna Total)
                    colunas_periodos_pivot = [c for c in df_pivot.columns if c != 'Custo FP']
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        anos_unicos = sorted(df_pivot_budget['Ano'].unique())
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in colunas_periodos_pivot:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in colunas_periodos_pivot if c not in colunas_ordenadas]
                        df_pivot = df_pivot[colunas_ordenadas + colunas_restantes + ['Custo FP']]
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_existentes = [c for c in meses_ordem if c in colunas_periodos_pivot]
                        colunas_restantes = [c for c in colunas_periodos_pivot if c not in meses_ordem]
                        df_pivot = df_pivot[colunas_existentes + colunas_restantes + ['Custo FP']]

                    df_pivot = df_pivot.sort_values('Custo FP', ascending=False)
                    df_pivot_fmt = df_pivot.applymap(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
                    st.dataframe(df_pivot_fmt, width="stretch")

            # ------------------------------
            # Tabela total por Veículo e Período (Budget)
            # ------------------------------
            titulo_total_budget = (
                "📋 **Tabela - CPU Total (Budget) por Veículo e Período**"
                if tipo_visualizacao == "CPU (Custo por Unidade)"
                else "📋 **Tabela - Custo Total (Budget) por Veículo e Período**"
            )
            with st.expander(titulo_total_budget, expanded=False):
                if tem_veiculo_budget and tem_periodo_budget:
                    df_budget_total = df_visualizacao_budget.copy()

                    tem_multiplos_anos_budget = 'Ano' in df_budget_total.columns and df_budget_total['Ano'].nunique() > 1
                    chaves = ['Veículo', 'Período'] + (['Ano'] if tem_multiplos_anos_budget and 'Ano' in df_budget_total.columns else [])

                    # Custo agregado
                    if 'Custo FP' not in df_budget_total.columns and 'Despesa Primaria' in df_budget_total.columns:
                        df_budget_total['Custo FP'] = df_budget_total['Despesa Primaria']
                    df_budget_total['Custo FP'] = pd.to_numeric(df_budget_total.get('Custo FP', 0), errors='coerce').fillna(0)

                    df_custo_agr = df_budget_total.groupby(chaves, dropna=False).agg({'Custo FP': 'sum'}).reset_index()

                    # Volume agregado
                    bud_vol_tem_veiculo = (
                        df_budget_vol_base is not None
                        and hasattr(df_budget_vol_base, 'columns')
                        and 'Veículo' in df_budget_vol_base.columns
                    )

                    if (
                        tipo_visualizacao == "CPU (Custo por Unidade)"
                        and (not bud_vol_tem_veiculo)
                        and df_budget_vol_base is not None
                        and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                    ):
                        st.error(
                            "❌ ERRO NA EXTRAÇÃO: o Volume do Budget precisa conter a coluna 'Veículo'. "
                            "Não é mais permitido rateio/fallback no app."
                        )
                        st.info(
                            "💡 Refaça a extração do BUDGET (página 'Extração de Dados') e corrija a aba 'Volume BDG' no Excel."
                        )
                        st.stop()
                    else:
                        # Volume agregado direto no grão disponível
                        vol_keys_tot = []
                        if df_budget_vol_base is not None and hasattr(df_budget_vol_base, 'columns'):
                            vol_keys_tot = [k for k in chaves if k in df_budget_vol_base.columns]

                        if (
                            df_budget_vol_base is not None
                            and 'Volume' in getattr(df_budget_vol_base, 'columns', [])
                            and vol_keys_tot
                        ):
                            df_volume_agr = (
                                df_budget_vol_base
                                .groupby(vol_keys_tot, dropna=False)
                                .agg({'Volume': 'sum'})
                                .reset_index()
                            )
                            df_total_agr = pd.merge(df_custo_agr, df_volume_agr, on=vol_keys_tot, how='left')
                        else:
                            df_total_agr = df_custo_agr.copy()
                            df_total_agr['Volume'] = 0

                    df_total_agr['Volume'] = pd.to_numeric(df_total_agr['Volume'], errors='coerce').fillna(0)

                    # Coluna de pivot
                    if tem_multiplos_anos_budget:
                        df_total_agr['Período_Ano'] = (
                            df_total_agr['Período'].astype(str) + ' ' + df_total_agr['Ano'].astype(str)
                        )
                        col_pivot = 'Período_Ano'
                    else:
                        col_pivot = 'Período'

                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_total_agr['CPU'] = np.where(
                            (df_total_agr['Volume'].notna()) & (df_total_agr['Volume'] != 0),
                            df_total_agr['Custo FP'] / df_total_agr['Volume'],
                            0
                        )
                        df_tabela_total = df_total_agr.pivot_table(
                            index='Veículo',
                            columns=col_pivot,
                            values='CPU',
                            aggfunc='first',
                            fill_value=0
                        )
                    else:
                        df_tabela_total = df_total_agr.pivot_table(
                            index='Veículo',
                            columns=col_pivot,
                            values='Custo FP',
                            aggfunc='sum',
                            fill_value=0
                        )

                    df_tabela_total.index.name = 'Veículo'
                    df_tabela_total = df_tabela_total.reset_index()

                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        df_total_veiculo = df_total_agr.groupby('Veículo', dropna=False).agg({
                            'Custo FP': 'sum',
                            'Volume': 'sum'
                        }).reset_index()
                        df_total_veiculo['CPU_Total'] = np.where(
                            (df_total_veiculo['Volume'].notna()) & (df_total_veiculo['Volume'] != 0),
                            df_total_veiculo['Custo FP'] / df_total_veiculo['Volume'],
                            0
                        )
                        df_tabela_total = pd.merge(
                            df_tabela_total,
                            df_total_veiculo[['Veículo', 'CPU_Total']],
                            on='Veículo',
                            how='left'
                        )
                        df_tabela_total.rename(columns={'CPU_Total': 'Custo FP'}, inplace=True)
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['Veículo', 'Custo FP']]
                    else:
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['Veículo']]
                        df_tabela_total['Custo FP'] = df_tabela_total[colunas_periodos].sum(axis=1)
                        colunas_periodos = [c for c in df_tabela_total.columns if c not in ['Veículo', 'Custo FP']]

                    # Ordenar colunas de meses cronologicamente
                    if tem_multiplos_anos_budget:
                        colunas_ordenadas = []
                        try:
                            anos_unicos = sorted(df_total_agr['Ano'].dropna().unique().tolist())
                        except Exception:
                            anos_unicos = []
                        for ano in anos_unicos:
                            for mes in ORDEM_MESES:
                                col = f"{mes.capitalize()} {ano}"
                                if col in colunas_periodos:
                                    colunas_ordenadas.append(col)
                        colunas_restantes = [c for c in colunas_periodos if c not in colunas_ordenadas]
                        colunas_periodos = colunas_ordenadas + colunas_restantes
                    else:
                        meses_ordem = [m.capitalize() for m in ORDEM_MESES]
                        colunas_ordenadas = [c for c in meses_ordem if c in colunas_periodos]
                        colunas_restantes = [c for c in colunas_periodos if c not in meses_ordem]
                        colunas_periodos = colunas_ordenadas + colunas_restantes

                    # Reordenar DataFrame final
                    colunas_finais = ['Veículo'] + colunas_periodos + ['Custo FP']
                    colunas_finais = [c for c in colunas_finais if c in df_tabela_total.columns]
                    df_tabela_total = df_tabela_total[colunas_finais]

                    df_tabela_total_fmt = df_tabela_total.copy()
                    for c in colunas_periodos + ['Custo FP']:
                        if c in df_tabela_total_fmt.columns:
                            df_tabela_total_fmt[c] = df_tabela_total_fmt[c].map(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)
                    st.dataframe(df_tabela_total_fmt, width="stretch")

                    if st.button(
                        "📥 Baixar Tabela Total por Veículo (Budget) (Excel)",
                        width="stretch",
                        key="download_tabela_total_veiculo_budget_tc"
                    ):
                        with st.spinner("Gerando arquivo da tabela total (Budget)..."):
                            try:
                                downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                                tipo_nome = "CPU" if tipo_visualizacao == "CPU (Custo por Unidade)" else "Custo_Total"
                                file_name = f"TC_Veiculos_tabela_total_veiculo_Budget_{tipo_nome}.xlsx"
                                file_path = os.path.join(downloads_path, file_name)
                                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                                    df_tabela_total.to_excel(writer, index=False, sheet_name='Total_Veiculo_Budget')
                                st.success(f"✅ Arquivo salvo com sucesso em: {file_path}")
                            except Exception as e:
                                st.error(f"❌ Erro ao salvar arquivo: {str(e)}")
                else:
                    st.info("ℹ️ Colunas necessárias não encontradas para criar a tabela total de Budget.")

            # ------------------------------
            # Tabela filtrada (Budget)
            # ------------------------------
            titulo_filtrada_budget = (
                "📋 **Tabela Filtrada - CPU (Budget) (Todas as Linhas)**"
                if tipo_visualizacao == "CPU (Custo por Unidade)"
                else "📋 **Tabela Filtrada (Budget) (Todas as Linhas)**"
            )
            with st.expander(titulo_filtrada_budget, expanded=False):
                df_display_budget = df_visualizacao_budget.copy()
                colunas_para_remover = ['mes', 'Mes', 'QTD', 'soma_percentuais', 'Soma_Percentuais']
                for col in colunas_para_remover:
                    if col in df_display_budget.columns:
                        df_display_budget = df_display_budget.drop(columns=[col])
                st.info(f"📊 Exibindo todas as {len(df_display_budget):,} linhas e {len(df_display_budget.columns)} colunas")
                st.dataframe(df_display_budget, width="stretch")

                if st.button(
                    "📥 Baixar Tabela Filtrada (Budget) (Excel)",
                    width="stretch",
                    key="download_filtered_budget_tc"
                ):
                    with st.spinner("Gerando arquivo da tabela filtrada (Budget)..."):
                        try:
                            downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                            file_name = "TC_Veiculos_tabela_filtrada_Budget.xlsx"
                            file_path = os.path.join(downloads_path, file_name)
                            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                                df_visualizacao_budget.to_excel(writer, index=False, sheet_name='Dados_Budget')
                            st.success(f"✅ Arquivo salvo com sucesso em: {file_path}")
                        except Exception as e:
                            st.error(f"❌ Erro ao salvar arquivo: {str(e)}")

# ==========================================
# TAB 6: Waterfall - MOVED TO pages/4 - Waterfall.py
# ==========================================
# O código do Waterfall foi movido para uma página separada (pages/4 - Waterfall.py)
# O código completo do tab5 (linhas 9659-11008) foi extraído para a nova página
# Removido: todo o código do tab5 foi movido para pages/4 - Waterfall.py

# Fechar bloco condicional do dashboard principal
# (O rodapé abaixo será exibido em todas as páginas)

# Rodapé
try:
    with open("versao.json", "r") as _vf:
        versao_atual = json.load(_vf).get("versao", "?")
except Exception:
    versao_atual = "?"

_meses_rodape = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
_agora = datetime.now()
mes_atual = _meses_rodape[_agora.month]
ano_atual = _agora.year
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    \U0001f4da Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)
