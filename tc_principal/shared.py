"""
TC Veículos — Módulo Compartilhado
Constantes, data loaders, helpers e cálculos centralizados.
Todas as páginas do TC Veículos importam daqui.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import json
import unicodedata
from datetime import datetime

from tc_core.constants import ORDEM_MESES  # noqa: F401 — re-exportado para as páginas

# ═══════════════════════════════════════════════════════════════
#  CONSTANTES
# ═══════════════════════════════════════════════════════════════

CORES_VEICULOS = {
    'J516 biton': '#1f77b4', 'J516 monoton': '#aec7e8',
    'CC21 biton': '#ff7f0e', 'CC21 monoton': '#ffbb78',
    'CC22 biton': '#2ca02c', 'CC22 monoton': '#98df8a',
    'CC24 biton 5L': '#d62728', 'CC24 monoton 5L': '#ff9896',
    'CC24 biton 7L': '#9467bd', 'CC24 monoton 7L': '#c5b0d5',
}

MESES_ABREV = {
    'Janeiro': 'Jan', 'Fevereiro': 'Fev', 'Março': 'Mar',
    'Abril': 'Abr', 'Maio': 'Mai', 'Junho': 'Jun',
    'Julho': 'Jul', 'Agosto': 'Ago', 'Setembro': 'Set',
    'Outubro': 'Out', 'Novembro': 'Nov', 'Dezembro': 'Dez',
}

# Colunas monetárias padrão do TC Veículos
# Nota: Redis NÃO é mais coluna — é identificado por Account='Redis' nas linhas
COLUNAS_MONETARIAS = [
    'Despesa Primaria', 'Custo FA', 'Custo FP',
    'D&A dedicado', 'FP sem Dedicada',
]

# Identificador de linhas Redis na tabela principal
ACCOUNT_REDIS = 'Redis'


def extrair_redis(df: pd.DataFrame) -> float:
    """Extrai soma das linhas Redis (Account='Redis') da tabela principal."""
    if df is None or 'Account' not in df.columns:
        return 0.0
    mask = df['Account'] == ACCOUNT_REDIS
    return df.loc[mask, 'Despesa Primaria'].sum() if 'Despesa Primaria' in df.columns else 0.0


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING (cached)
# ═══════════════════════════════════════════════════════════════

def _pasta_tc_principal(ano):
    """Caminho da pasta de dados TC Veículos Budget para um ano."""
    # Estrutura: dados/TC_Principal/{ano}/BUD/
    return os.path.join('dados', 'TC_Principal', str(ano), 'BUD')


def _pasta_tc_principal_real(ano):
    """Caminho da pasta de dados TC Veículos Real para um ano."""
    # Estrutura: dados/TC_Principal/{ano}/ (raiz, sem subfolder)
    return os.path.join('dados', 'TC_Principal', str(ano))


def descobrir_anos_tc_principal():
    """Descobre anos que possuem dados processados em dados/TC_Principal/{ano}/BUD/."""
    anos = []
    pasta_base = os.path.join('dados', 'TC_Principal')
    if os.path.exists(pasta_base):
        for d in sorted(os.listdir(pasta_base), reverse=True):
            pasta_parquets = os.path.join(pasta_base, d, 'BUD')
            if os.path.isdir(pasta_parquets) and any(f.endswith('.parquet') for f in os.listdir(pasta_parquets)):
                try:
                    anos.append(int(d))
                except ValueError:
                    pass
    return anos


def obter_timestamp_parquets(ano):
    """Retorna timestamp mais recente entre os parquets do ano."""
    pasta = _pasta_tc_principal(ano)
    if not os.path.isdir(pasta):
        return None
    ts_max = 0
    for f in os.listdir(pasta):
        if f.endswith('.parquet'):
            ts = os.path.getmtime(os.path.join(pasta, f))
            if ts > ts_max:
                ts_max = ts
    return ts_max if ts_max > 0 else None


@st.cache_data(ttl=3600, show_spinner=True)
def load_principal(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_principal_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_volume_bud(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_vol_veiculos_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner=True)
def load_volume_actual(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_vol_veiculos_actual.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner=True)
def load_tempo_veiculos(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_tempo_veiculos_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_dea_dedicado(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_dea_dedicado_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_volume_fa(ano):
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_volume_fa_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — Novos parquets de veículos (Fases 13–17)
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=True)
def load_fp_sem_da_veiculos(ano):
    """Custo FP sem D&A Dedicado (base de rateio)."""
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_fp_sem_da_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_percentual_rateio_veiculos(ano):
    """Percentuais de rateio por veículo."""
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_percentual_rateio_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_custo_rateado_veiculos(ano):
    """Custo rateado por veículo (FP sem Ded × Percentual)."""
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_custo_rateado_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_custo_fp_veiculo(ano):
    """Custo FP final por veículo (rateado + D&A)."""
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_custo_fp_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_cpu_veiculo(ano):
    """CPU (Custo Por Unidade) por modelo de veículo."""
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_cpu_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — REAL (cached)
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=True)
def load_principal_real(ano):
    """Tabela principal Real (Sapiens)."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_principal.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_volume_fa_real(ano):
    """Volume FA + Tempo FA Real."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_volume_fa.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_tempo_veiculos_real(ano):
    """Tempo Veículo Real."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_tempo_veiculos.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_comparativo(ano):
    """Comparativo Real × Budget."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_comparativo_real_budget.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_custo_fp_veiculo_real(ano):
    """Custo FP final por veículo (rateado + D&A) — Real."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_veiculos_custo_fp.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_cpu_veiculo_real(ano):
    """CPU (Custo Por Unidade) por modelo de veículo — Real."""
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_veiculos_cpu.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — HISTÓRICO CONSOLIDADO (multi-ano)
# ═══════════════════════════════════════════════════════════════

def _pasta_historico():
    return os.path.join('dados', 'TC_Principal', 'historico_consolidado')


def _pasta_historico_bud():
    return os.path.join('dados', 'TC_Principal', 'historico_consolidado', 'BUD')


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_principal():
    """Tabela principal consolidada multi-ano (Real)."""
    caminho = os.path.join(_pasta_historico(), 'df_principal_historico.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_volume():
    """Volume consolidado multi-ano (Real)."""
    caminho = os.path.join(_pasta_historico(), 'df_vol_historico.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_custo_fp_veiculo():
    """Custo FP por veículo consolidado multi-ano (Real)."""
    caminho = os.path.join(_pasta_historico(), 'df_veiculos_custo_fp_historico.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_principal_bud():
    """Tabela principal consolidada multi-ano (Budget)."""
    caminho = os.path.join(_pasta_historico_bud(), 'df_principal_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_volume_bud():
    """Volume consolidado multi-ano (Budget)."""
    caminho = os.path.join(_pasta_historico_bud(), 'df_vol_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=3600, show_spinner=True)
def load_historico_custo_fp_veiculo_bud():
    """Custo FP por veículo consolidado multi-ano (Budget)."""
    caminho = os.path.join(_pasta_historico_bud(), 'df_veiculos_custo_fp_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — FORECAST / BEST ESTIMATE
# ═══════════════════════════════════════════════════════════════

def _pasta_forecast_tc():
    return os.path.join('dados', 'TC_Principal', 'Forecast')


@st.cache_data(ttl=3600, show_spinner=True)
def load_forecast_completo():
    """Forecast completo (Real + BE) — gerado pelo BE Simulador."""
    caminho = os.path.join(_pasta_forecast_tc(), 'forecast_completo.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=3600, show_spinner=True)
def load_forecast_volume():
    """Volume do forecast — gerado pelo BE Simulador."""
    caminho = os.path.join(_pasta_forecast_tc(), 'df_vol_historico.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


# ═══════════════════════════════════════════════════════════════
#  HELPERS DE PERÍODO
# ═══════════════════════════════════════════════════════════════

def normalizar_periodo(df):
    """Garante que a coluna 'Período' exista (corrige mojibake)."""
    if 'Período' not in df.columns:
        for col in df.columns:
            if 'per' in str(col).lower() and 'odo' in str(col).lower():
                df = df.rename(columns={col: 'Período'})
                break
    return df


def ordenar_por_mes(df, col='Período'):
    """Ordena DataFrame pelo mês usando CategoricalDtype."""
    if col in df.columns:
        cat = pd.CategoricalDtype(categories=ORDEM_MESES, ordered=True)
        df[col] = df[col].astype(cat)
        df = df.sort_values(col)
    return df


# ═══════════════════════════════════════════════════════════════
#  CUSTO FIXO / VARIÁVEL
# ═══════════════════════════════════════════════════════════════

def _normalizar_texto_sem_acento(valor) -> str:
    """Remove acentos e converte para lowercase."""
    if pd.isna(valor):
        return ""
    return (
        unicodedata.normalize('NFKD', str(valor))
        .encode('ascii', 'ignore')
        .decode('ascii')
        .strip()
        .lower()
    )


def mask_custo_fixo(serie: pd.Series) -> pd.Series:
    """Retorna máscara booleana: True onde Custo começa com 'fix'."""
    return serie.astype(str).map(_normalizar_texto_sem_acento).str.startswith('fix')


# ═══════════════════════════════════════════════════════════════
#  BUDGET FLEX
# ═══════════════════════════════════════════════════════════════

def calcular_flex_budget(df_principal, df_vol_bud, df_vol_actual, col_custo='Custo FP', tem_ano=False):
    """
    Calcula Budget Flex por Período (e Ano se tem_ano=True).

    Flex = Fixo_Budget + (NãoFixo_Budget × Volume_Actual / Volume_Budget)

    Returns DataFrame com colunas:
      Período, [Ano], Custo_Fixo, Custo_NaoFixo, Custo_Total_Bud,
      Vol_Budget, Vol_Actual, Proporcao, Flex_Bud
    Retorna None se dados insuficientes.
    """
    if df_vol_bud is None or df_vol_actual is None:
        return None
    if 'Custo' not in df_principal.columns:
        return None

    # Determinar colunas de agrupamento
    cols_agrup = ['Ano', 'Período'] if tem_ano and 'Ano' in df_principal.columns else ['Período']

    # Volume por período (e ano se aplicável)
    vol_bud_per = df_vol_bud.groupby(cols_agrup, as_index=False)['Volume'].sum()
    vol_bud_per = vol_bud_per.rename(columns={'Volume': 'Vol_Budget'})

    vol_act_per = df_vol_actual.groupby(cols_agrup, as_index=False)['Volume'].sum()
    vol_act_per = vol_act_per.rename(columns={'Volume': 'Vol_Actual'})

    # Custo fixo e total por período (e ano se aplicável)
    fixo_mask = mask_custo_fixo(df_principal['Custo'])

    custo_fixo = (df_principal[fixo_mask]
                  .groupby(cols_agrup, as_index=False)[col_custo].sum()
                  .rename(columns={col_custo: 'Custo_Fixo'}))

    custo_total = (df_principal
                   .groupby(cols_agrup, as_index=False)[col_custo].sum()
                   .rename(columns={col_custo: 'Custo_Total_Bud'}))

    # Montar tabela flex
    df_flex = custo_total.merge(custo_fixo, on=cols_agrup, how='left')
    df_flex['Custo_Fixo'] = df_flex['Custo_Fixo'].fillna(0)
    df_flex['Custo_NaoFixo'] = df_flex['Custo_Total_Bud'] - df_flex['Custo_Fixo']

    df_flex = df_flex.merge(vol_bud_per, on=cols_agrup, how='left')
    df_flex = df_flex.merge(vol_act_per, on=cols_agrup, how='left')
    df_flex['Vol_Budget'] = df_flex['Vol_Budget'].fillna(0)
    df_flex['Vol_Actual'] = df_flex['Vol_Actual'].fillna(0)

    df_flex['Proporcao'] = np.where(
        df_flex['Vol_Budget'] != 0,
        df_flex['Vol_Actual'] / df_flex['Vol_Budget'],
        1.0
    )

    df_flex['Flex_Bud'] = (
        df_flex['Custo_Fixo']
        + df_flex['Custo_NaoFixo'] * df_flex['Proporcao']
    )

    return df_flex


# ═══════════════════════════════════════════════════════════════
#  APLICAR FATOR E MOEDA
# ═══════════════════════════════════════════════════════════════

def aplicar_fator(valor, fator_sel):
    """Aplica fator de conversão K/M a um valor."""
    if fator_sel == 'K (milhares)':
        return valor / 1_000
    elif fator_sel == 'M (milhões)':
        return valor / 1_000_000
    return valor


def aplicar_fator_df(df, colunas, fator_sel):
    """Aplica fator K/M a múltiplas colunas de um DataFrame."""
    if fator_sel == 'Nenhum':
        return df
    df = df.copy()
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: aplicar_fator(x, fator_sel))
    return df


def obter_sufixo_fator(fator_sel):
    """Retorna sufixo para exibição (ex: ' K', ' M', '')."""
    if fator_sel == 'K (milhares)':
        return ' K'
    elif fator_sel == 'M (milhões)':
        return ' M'
    return ''


def converter_moeda_df(df, colunas, moeda, taxas):
    """Converte colunas monetárias de BRL para moeda selecionada."""
    from tc_core.finance.currency import converter_moeda
    if moeda == 'BRL':
        return df
    if taxas is None or moeda not in taxas:
        # Sem taxa disponível — mantém BRL para não perder dados
        return df
    df = df.copy()
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: converter_moeda(x, moeda, taxas))
    return df


def calcular_cpu(custo, volume):
    """CPU = Custo / Volume (protegido contra divisão por zero)."""
    return np.where(volume != 0, custo / volume, 0.0)
