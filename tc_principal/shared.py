"""
TC Veículos — Módulo Compartilhado
Constantes, data loaders, helpers e cálculos centralizados.
Todas as páginas do TC Veículos importam daqui.
"""

import sys as _sys
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import os
import json
import unicodedata
from datetime import datetime

from tc_core.constants import ORDEM_MESES  # noqa: F401 — re-exportado para as páginas
from tc_core.utils.portabilidade import get_base_path, get_data_root
from tc_core.data_source import read_table
from tc_core.feature_flags import get_flag
from tc_core.data_router import read_optimized
from tc_core.telemetry import log_data_source, perf_timer

# ═══════════════════════════════════════════════════════════════
#  ORDEM CANÔNICA DE COLUNAS — BE DETALHADO (24 colunas)
# ═══════════════════════════════════════════════════════════════
COLUNAS_BE_DETALHADO = [
    'Mes', 'Período', 'Nºconta', 'Centrocst', 'Nºdoc.ref.', 'Dt.lçto.',
    'Valor', 'QTD', 'Type 05', 'Type 06', 'Account', 'USI', 'Oficina',
    'Doc.compra', 'Texto breve', 'Fornecedor', 'Material', 'Usuário',
    'Fornec.', 'Tipo', 'Custo', 'massa FA - Actual', 'massa FP - Actual',
    'Total',
]

COLUNAS_BE_DETALHADO_VEICULO = [
    'Mes', 'Período', 'Nºconta', 'Centrocst', 'Nºdoc.ref.', 'Dt.lçto.',
    'Valor', 'QTD', 'Type 05', 'Type 06', 'Account', 'USI', 'Oficina',
    'Veículo',
    'Doc.compra', 'Texto breve', 'Fornecedor', 'Material', 'Usuário',
    'Fornec.', 'Tipo', 'Custo', 'massa FA - Actual', 'massa FP - Actual',
    'Custo FP Veiculo', 'Total',
]


def reordenar_colunas_be(df: pd.DataFrame, colunas=None) -> pd.DataFrame:
    """Reordena *df* conforme COLUNAS_BE_DETALHADO.

    Colunas ausentes são criadas com valor vazio.
    Colunas extras (não listadas) são mantidas ao final.
    """
    if df is None or df.empty:
        return df
    ordem = list(colunas or COLUNAS_BE_DETALHADO)
    for col in ordem:
        if col not in df.columns:
            df[col] = ''
    extras = [c for c in df.columns if c not in ordem]
    return df[ordem + extras]


def _gerar_excel_bytes_direto(df: pd.DataFrame) -> bytes:
    """Gera bytes Excel diretamente de um DataFrame."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Dados')
    return buf.getvalue()


_LIMITE_EXCEL_LINHAS = 30_000  # Acima disso usa CSV (muito mais rápido)


def download_excel_button(
    st_mod, df: pd.DataFrame, label: str, file_name: str, key: str,
) -> None:
    """Botão de download Excel/CSV via BytesIO (sem gravar no servidor).

    Para DataFrames grandes (>30K linhas), gera CSV ao invés de Excel
    para evitar travamento de 30-60s na serialização openpyxl.
    """
    try:
        if len(df) <= _LIMITE_EXCEL_LINHAS:
            data = _gerar_excel_bytes_direto(df)
            st_mod.download_button(
                label,
                data=data,
                file_name=file_name,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key=key,
                use_container_width=True,
            )
        else:
            csv_data = df.to_csv(index=False).encode('utf-8-sig')
            csv_name = file_name.rsplit('.', 1)[0] + '.csv'
            st_mod.download_button(
                label.replace('Excel', 'CSV'),
                data=csv_data,
                file_name=csv_name,
                mime='text/csv',
                key=key,
                use_container_width=True,
            )
    except Exception as e:
        st_mod.error(f"❌ Erro ao gerar arquivo: {e}")


# ═══════════════════════════════════════════════════════════════
#  RAIZ DO PROJETO (compatível com EXE PyInstaller)
# ═══════════════════════════════════════════════════════════════
if hasattr(_sys, '_MEIPASS'):
    _ROOT = _sys._MEIPASS          # EXE → aponta para _internal/
else:
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # dev

_BASE_ROOT = str(get_base_path())
_DATA_ROOT = str(get_data_root())


def _data_root_str() -> str:
    """Resolve DATA_ROOT em tempo de uso para refletir o ambiente atual."""
    return str(get_data_root())


def _join_data_root(*parts: str) -> str:
    return os.path.join(_data_root_str(), *parts)


def _use_snowflake() -> bool:
    """True quando o backend de dados é Snowflake."""
    return get_flag("SCI_DATA_BACKEND", default="local") == "snowflake"

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
COLUNAS_MONETARIAS = [
    'Despesa Primaria', 'Custo FA', 'Custo FP',
    'D&A dedicado', 'FP sem Dedicada',
]


def extrair_redis(df: pd.DataFrame) -> float:
    """Extrai soma da Despesa Primaria das linhas originadas da aba massa-REDIS.

    Usa a coluna booleana ``_fonte_redis`` inserida pelos pipelines de
    processamento (BUD e Real).  Caso a coluna não exista (parquets antigos),
    retorna 0.0 sem erro.
    """
    if df is None or df.empty:
        return 0.0
    if '_fonte_redis' in df.columns:
        mask = df['_fonte_redis'] == True  # noqa: E712
        return float(df.loc[mask, 'Despesa Primaria'].sum()) if 'Despesa Primaria' in df.columns else 0.0
    return 0.0


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING (cached)
# ═══════════════════════════════════════════════════════════════

def _pasta_tc_principal(ano):
    """Caminho da pasta de dados TC Veículos Budget para um ano."""
    # Estrutura: dados/TC_Principal/{ano}/BUD/
    return _join_data_root('TC_Principal', str(ano), 'BUD')


def _pasta_tc_principal_real(ano):
    """Caminho da pasta de dados TC Veículos Real para um ano."""
    # Estrutura: dados/TC_Principal/{ano}/ (raiz, sem subfolder)
    return _join_data_root('TC_Principal', str(ano))


@st.cache_data(ttl=300, show_spinner=False)
def descobrir_anos_tc_principal():
    """Descobre anos que possuem dados processados em dados/TC_Principal/{ano}/BUD/."""
    from tc_core.data_source import list_available_years
    if _use_snowflake():
        return list_available_years("TC_Principal", "BUD")

    from tc_core.utils.portabilidade import cloud_path_exists, cloud_listdir, cloud_isdir

    anos = []
    pasta_base = _join_data_root('TC_Principal')
    if cloud_path_exists(pasta_base):
        for d in sorted(cloud_listdir(pasta_base), reverse=True):
            pasta_parquets = os.path.join(pasta_base, d, 'BUD')
            if cloud_isdir(pasta_parquets) and any(f.endswith('.parquet') for f in cloud_listdir(pasta_parquets)):
                try:
                    anos.append(int(d))
                except ValueError:
                    pass
    return anos


@st.cache_data(ttl=300, show_spinner=False)
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


def _read_parquet_traced(caminho: str, consumer: str, columns=None) -> "pd.DataFrame | None":
    """Lê parquet direto com telemetria (para datasets sem variante otimizada)."""
    if not os.path.exists(caminho):
        return None
    with perf_timer() as t:
        df = pd.read_parquet(caminho, columns=columns)
    log_data_source(
        consumer=consumer,
        logical_dataset=os.path.basename(caminho).replace('.parquet', ''),
        physical_path=caminho,
        mode="FULL",
        nrows=len(df),
        ncols=len(df.columns),
        load_ms=t.elapsed_ms,
    )
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_principal(ano, columns=None):
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_principal_BUD', columns)
    return read_optimized('TC_Principal', str(ano), 'BUD', 'df_principal_BUD', prefer='thin', columns=columns, consumer='load_principal')


@st.cache_data(ttl=300, show_spinner=False)
def load_volume_bud(ano, columns=None):
    if _use_snowflake():
        df = read_table('TC_Principal', str(ano), 'BUD', 'df_vol_veiculos_BUD', columns)
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_vol_veiculos_BUD.parquet')
    df = _read_parquet_traced(caminho, 'load_volume_bud', columns=columns)
    if df is not None and 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_volume_actual(ano, columns=None):
    if _use_snowflake():
        df = read_table('TC_Principal', str(ano), 'BUD', 'df_vol_veiculos_actual', columns)
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_vol_veiculos_actual.parquet')
    df = _read_parquet_traced(caminho, 'load_volume_actual', columns=columns)
    if df is not None and 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_tempo_veiculos(ano):
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_tempo_veiculos_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_tempo_veiculos_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_tempo_veiculos')


@st.cache_data(ttl=300, show_spinner=False)
def load_dea_dedicado(ano):
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_dea_dedicado_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_dea_dedicado_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_dea_dedicado')


@st.cache_data(ttl=300, show_spinner=False)
def load_volume_fa(ano):
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_volume_fa_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_volume_fa_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_volume_fa')


@st.cache_data(ttl=300, show_spinner=False)
def load_tc_sapiens(ano, columns=None):
    """Carrega df_tc_sapiens.parquet — dados Sapiens detalhados com todas as colunas.
    O arquivo é gerado pela fase10b e salvo na pasta Real (não BUD).
    """
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_tc_sapiens', columns)
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_tc_sapiens.parquet')
    return _read_parquet_traced(caminho, 'load_tc_sapiens', columns=columns)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — Novos parquets de veículos (Fases 13–17)
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def load_fp_sem_da_veiculos(ano):
    """Custo FP sem D&A Dedicado (base de rateio)."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_veiculos_fp_sem_da_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_fp_sem_da_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_fp_sem_da_veiculos')


@st.cache_data(ttl=300, show_spinner=False)
def load_percentual_rateio_veiculos(ano):
    """Percentuais de rateio por veículo."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_veiculos_percentual_rateio_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_percentual_rateio_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_percentual_rateio_veiculos')


@st.cache_data(ttl=300, show_spinner=False)
def load_custo_rateado_veiculos(ano):
    """Custo rateado por veículo (FP sem Ded × Percentual)."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_veiculos_custo_rateado_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_custo_rateado_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_custo_rateado_veiculos')


@st.cache_data(ttl=300, show_spinner=False)
def load_custo_fp_veiculo(ano, columns=None):
    """Custo FP final por veículo (rateado + D&A)."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_veiculos_custo_fp_BUD', columns)
    return read_optimized('TC_Principal', str(ano), 'BUD', 'df_veiculos_custo_fp_BUD', prefer='agg', columns=columns, consumer='load_custo_fp_veiculo')


@st.cache_data(ttl=300, show_spinner=False)
def load_cpu_veiculo(ano):
    """CPU (Custo Por Unidade) por modelo de veículo."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), 'BUD', 'df_veiculos_cpu_BUD')
    caminho = os.path.join(_pasta_tc_principal(ano), 'df_veiculos_cpu_BUD.parquet')
    return _read_parquet_traced(caminho, 'load_cpu_veiculo')


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — REAL (cached)
# ═══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def load_principal_real(ano, columns=None):
    """Tabela principal Real (Sapiens)."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_principal', columns)
    return read_optimized('TC_Principal', str(ano), '', 'df_principal', prefer='thin', columns=columns, consumer='load_principal_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_volume_fa_real(ano):
    """Volume FA + Tempo FA Real."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_volume_fa')
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_volume_fa.parquet')
    return _read_parquet_traced(caminho, 'load_volume_fa_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_tempo_veiculos_real(ano):
    """Tempo Veículo Real."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_tempo_veiculos')
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_tempo_veiculos.parquet')
    return _read_parquet_traced(caminho, 'load_tempo_veiculos_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_volume_veiculos_real(ano):
    """Volume de veículos Real (processado da aba 'Volume Actual')."""
    if _use_snowflake():
        df = read_table('TC_Principal', str(ano), '', 'df_vol_veiculos')
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_vol_veiculos.parquet')
    df = _read_parquet_traced(caminho, 'load_volume_veiculos_real')
    if df is not None and 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_comparativo(ano):
    """Comparativo Real × Budget."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_comparativo_real_budget')
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_comparativo_real_budget.parquet')
    return _read_parquet_traced(caminho, 'load_comparativo')


@st.cache_data(ttl=300, show_spinner=False)
def load_custo_fp_veiculo_real(ano):
    """Custo FP final por veículo (rateado + D&A) — Real."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_veiculos_custo_fp')
    return read_optimized('TC_Principal', str(ano), '', 'df_veiculos_custo_fp', prefer='agg', consumer='load_custo_fp_veiculo_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_cpu_veiculo_real(ano):
    """CPU (Custo Por Unidade) por modelo de veículo — Real."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_veiculos_cpu')
    caminho = os.path.join(_pasta_tc_principal_real(ano), 'df_veiculos_cpu.parquet')
    return _read_parquet_traced(caminho, 'load_cpu_veiculo_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_percentual_rateio_veiculos_real(ano):
    """Percentuais de rateio por veículo — Real (baseados em tempos reais)."""
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_veiculos_percentual_rateio')
    caminho = os.path.join(
        _pasta_tc_principal_real(ano),
        'df_veiculos_percentual_rateio.parquet',
    )
    return _read_parquet_traced(caminho, 'load_percentual_rateio_veiculos_real')


@st.cache_data(ttl=300, show_spinner=False)
def load_dea_dedicado_real(ano: int = 2026):
    """
    Carrega arquivo de D&A dedicado por veículo do processamento Real.
    Usado para fazer rateio idêntico ao processamento Real.
    """
    if _use_snowflake():
        return read_table('TC_Principal', str(ano), '', 'df_dea_dedicado')
    caminho = _join_data_root('TC_Principal', str(ano), 'df_dea_dedicado.parquet')
    return _read_parquet_traced(caminho, 'load_dea_dedicado_real')


@st.cache_data(ttl=300, show_spinner=False)
def ratear_be_por_veiculo(df_be, df_percentual, col_custo='Custo FP', df_dea=None):
    """
    Distribui dados de BE por veículo usando EXATAMENTE a mesma lógica do
    processamento Real (fases 12-14 do processamento_dados_veiculos.py).

    IMPORTANTE: O cálculo correto é:
      1. Custo Rateado = FP sem Dedicada × Percentual
      2. Custo FP Veiculo = Custo Rateado + D&A dedicado (por veículo)

    Isso é DIFERENTE de simplesmente fazer Custo FP × Percentual, porque
    D&A dedicado é alocado por veículo específico, não rateado.

    Os percentuais são gerados na extração Real (fase12) usando a fórmula:
      Tempo Veic  = EST × Volume  (fase4)
      Percentual  = Tempo Veic / Σ(Tempo Veic por Oficina+Período) (fase12)

    Parâmetros
    ----------
    df_be : DataFrame
        Dados do Best Estimate com colunas [Oficina, Período, FP sem Dedicada, D&A dedicado, ...].
    df_percentual : DataFrame
        Percentuais de rateio com [Oficina, Veículo, Período, Percentual].
    col_custo : str
        Coluna de custo final (apenas para compatibilidade, não usado no cálculo).
    df_dea : DataFrame, opcional
        Arquivo de D&A dedicado por veículo. Se None, rateia D&A pelo mesmo percentual.

    Retorna
    -------
    DataFrame com coluna Veículo e Custo FP Veiculo adicionadas.
    """
    if df_be is None or df_be.empty:
        return None
    if df_percentual is None or df_percentual.empty:
        return None

    # ── Verificar colunas necessárias ──
    tem_fp_sem_ded = 'FP sem Dedicada' in df_be.columns
    tem_dea = 'D&A dedicado' in df_be.columns

    # Se não tem as colunas do cálculo correto, usa fallback antigo
    if not tem_fp_sem_ded:
        if col_custo not in df_be.columns:
            return None
        # Fallback: usar Custo FP diretamente (menos preciso)
        return _ratear_be_por_veiculo_simples(df_be, df_percentual, col_custo)

    # ── Evitar colisão de colunas no merge ──
    # Inclui 'D&A dedicado' para evitar contaminação de valores pré-existentes
    # no df_be; a coluna será recalculada a partir de df_dea (Fase 14).
    colunas_dropar = ['Veículo', 'Percentual', 'Custo FP Veiculo', 'Custo Rateado', 'D&A dedicado']
    df_be_limpo = df_be.drop(
        columns=[c for c in colunas_dropar if c in df_be.columns],
        errors='ignore',
    )

    pct = df_percentual[['Oficina', 'Veículo', 'Período', 'Percentual']].copy()

    # ══════════════════════════════════════════════════════════════════════
    # FASE 13 (igual processamento Real): Custo Rateado = FP sem Ded × %
    # ══════════════════════════════════════════════════════════════════════
    df = pd.merge(df_be_limpo, pct, on=['Oficina', 'Período'], how='left')

    # Fallback para linhas sem veículo — distribuir pro-rata por período
    # (mesma lógica de fase13_custo_rateado_veiculos do Real)
    mask_sem = df['Veículo'].isna()
    if mask_sem.any():
        df_com = df[~mask_sem].copy()
        df_sem = df[mask_sem].drop(columns=['Veículo', 'Percentual'], errors='ignore')
        # Distribuição média por Período entre veículos conhecidos
        dist_periodo = (
            pct.groupby(['Período', 'Veículo'])['Percentual']
            .mean().reset_index()
        )
        # Normalizar para soma por Período = 1.0
        soma_per = dist_periodo.groupby('Período')['Percentual'].transform('sum')
        dist_periodo['Percentual'] = dist_periodo['Percentual'] / soma_per.replace(0, 1)
        # Expandir linhas sem veículo usando distribuição do período
        df_sem_expanded = pd.merge(df_sem, dist_periodo, on='Período', how='left')
        # Fallback global para períodos sem nenhum veículo conhecido
        mask_still = df_sem_expanded['Veículo'].isna()
        if mask_still.any():
            veiculos_unicos = pct['Veículo'].dropna().unique()
            n_veic = max(1, len(veiculos_unicos))
            linhas_orfas = df_sem_expanded[mask_still].drop(
                columns=['Veículo', 'Percentual'], errors='ignore',
            )
            expansoes = []
            for v in veiculos_unicos:
                tmp = linhas_orfas.copy()
                tmp['Veículo'] = v
                tmp['Percentual'] = 1.0 / n_veic
                expansoes.append(tmp)
            df_sem_expanded = pd.concat(
                [df_sem_expanded[~mask_still]] + expansoes, ignore_index=True
            )
        df = pd.concat([df_com, df_sem_expanded], ignore_index=True)

    df['Percentual'] = df['Percentual'].fillna(0)

    # ══════════════════════════════════════════════════════════════════════
    # FASE 13 (igual processamento Real): Custo Rateado = FP sem Ded × %
    # ══════════════════════════════════════════════════════════════════════
    df['Custo Rateado'] = df['FP sem Dedicada'] * df['Percentual']

    # ══════════════════════════════════════════════════════════════════════
    # FASE 14 (igual processamento Real): D&A dedicado por veículo
    # D&A é adicionado por veículo usando dados reais + média para meses forecast
    # ══════════════════════════════════════════════════════════════════════
    if df_dea is not None and not df_dea.empty and 'Veículo' in df_dea.columns:
        # Usar D&A do arquivo Real que já tem alocação por veículo
        cols_merge_dea = ['Oficina', 'Veículo', 'Account', 'Período']
        cols_merge_dea = [c for c in cols_merge_dea if c in df_dea.columns and c in df.columns]

        if len(cols_merge_dea) >= 2:
            # Agregar D&A por chaves de merge
            dea_agg = df_dea.groupby(cols_merge_dea, as_index=False)['D&A dedicado'].sum()

            # Expandir D&A para meses forecast: usar média dos meses históricos
            periodos_dea = set(dea_agg['Período'].unique()) if 'Período' in dea_agg.columns else set()
            periodos_be = set(df['Período'].unique()) if 'Período' in df.columns else set()
            periodos_faltantes = periodos_be - periodos_dea
            if periodos_faltantes and 'Período' in cols_merge_dea:
                # Calcular média D&A por (chaves sem Período)
                cols_media = [c for c in cols_merge_dea if c != 'Período']
                if cols_media:
                    dea_media = dea_agg.groupby(cols_media, as_index=False)['D&A dedicado'].mean()
                    # Criar linhas para cada período faltante
                    linhas_expand = []
                    for p in periodos_faltantes:
                        tmp = dea_media.copy()
                        tmp['Período'] = p
                        linhas_expand.append(tmp)
                    if linhas_expand:
                        dea_agg = pd.concat([dea_agg] + linhas_expand, ignore_index=True)

            dea_agg = dea_agg.rename(columns={'D&A dedicado': '_dea_veiculo'})

            # Merge com dados rateados
            df = pd.merge(df, dea_agg, on=cols_merge_dea, how='left')
            df['_dea_veiculo'] = df['_dea_veiculo'].fillna(0)

            # Distribuir D&A pro-rata entre linhas do mesmo grupo
            _n_rows = df.groupby(cols_merge_dea)['Custo Rateado'].transform('count')
            df['D&A dedicado'] = df['_dea_veiculo'] / _n_rows.replace(0, 1)
            df.drop(columns=['_dea_veiculo'], inplace=True, errors='ignore')
    else:
        # Sem df_dea — D&A dedicado = 0 (coluna foi dropada na limpeza)
        if 'D&A dedicado' not in df.columns:
            df['D&A dedicado'] = 0

    # Garantir coluna D&A dedicado existe
    if 'D&A dedicado' not in df.columns:
        df['D&A dedicado'] = 0

    # ══════════════════════════════════════════════════════════════════════
    # Custo FP Veiculo = Custo Rateado + D&A dedicado
    # ══════════════════════════════════════════════════════════════════════
    df['Custo FP Veiculo'] = df['Custo Rateado'] + df['D&A dedicado']

    # ══════════════════════════════════════════════════════════════════════
    # Validação de fechamento (igual Real fase 14)
    # ══════════════════════════════════════════════════════════════════════
    _custo_fp_orig = df_be[col_custo].sum() if col_custo in df_be.columns else 0
    _custo_fp_veic = df['Custo FP Veiculo'].sum()
    _diff_fech = abs(_custo_fp_orig - _custo_fp_veic)
    if _diff_fech > 1.0:
        import logging
        logging.warning(
            'ratear_be_por_veiculo: diferenca de fechamento R$ %.2f '
            '(original=%.2f, veiculos=%.2f)',
            _diff_fech, _custo_fp_orig, _custo_fp_veic,
        )

    return df


def _ratear_be_por_veiculo_simples(df_be, df_percentual, col_custo='Custo FP'):
    """
    Fallback: rateio simples usando Custo FP × Percentual.
    Usado quando não há colunas FP sem Dedicada / D&A dedicado.
    NOTA: Este método é MENOS PRECISO que o cálculo completo.
    """
    colunas_dropar = ['Veículo', 'Percentual', 'Custo FP Veiculo']
    df_be_limpo = df_be.drop(
        columns=[c for c in colunas_dropar if c in df_be.columns],
        errors='ignore',
    )

    pct = df_percentual[['Oficina', 'Veículo', 'Período', 'Percentual']].copy()
    df = pd.merge(df_be_limpo, pct, on=['Oficina', 'Período'], how='left')

    mask_sem = df['Veículo'].isna()
    if mask_sem.any():
        df_com = df[~mask_sem].copy()
        df_sem = df[mask_sem].drop(columns=['Veículo', 'Percentual'], errors='ignore')
        # Distribuição média por Período (mesma lógica do Real fase13)
        dist_periodo = (
            pct.groupby(['Período', 'Veículo'])['Percentual']
            .mean().reset_index()
        )
        soma_per = dist_periodo.groupby('Período')['Percentual'].transform('sum')
        dist_periodo['Percentual'] = dist_periodo['Percentual'] / soma_per.replace(0, 1)
        df_sem_expanded = pd.merge(df_sem, dist_periodo, on='Período', how='left')
        mask_still = df_sem_expanded['Veículo'].isna()
        if mask_still.any():
            veiculos_unicos = pct['Veículo'].dropna().unique()
            n_veic = max(1, len(veiculos_unicos))
            linhas_orfas = df_sem_expanded[mask_still].drop(
                columns=['Veículo', 'Percentual'], errors='ignore',
            )
            expansoes = []
            for v in veiculos_unicos:
                tmp = linhas_orfas.copy()
                tmp['Veículo'] = v
                tmp['Percentual'] = 1.0 / n_veic
                expansoes.append(tmp)
            df_sem_expanded = pd.concat(
                [df_sem_expanded[~mask_still]] + expansoes, ignore_index=True
            )
        df = pd.concat([df_com, df_sem_expanded], ignore_index=True)

    df['Percentual'] = df['Percentual'].fillna(0)
    df['Custo FP Veiculo'] = df[col_custo] * df['Percentual']

    return df


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — HISTÓRICO CONSOLIDADO (multi-ano)
# ═══════════════════════════════════════════════════════════════

def _pasta_historico():
    return _join_data_root('TC_Principal', 'historico_consolidado')


def _pasta_historico_bud():
    return _join_data_root('TC_Principal', 'historico_consolidado', 'BUD')


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_principal():
    """Tabela principal consolidada multi-ano (Real)."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'historico_consolidado', 'df_principal_historico')
    caminho = os.path.join(_pasta_historico(), 'df_principal_historico.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_volume():
    """Volume consolidado multi-ano (Real)."""
    if _use_snowflake():
        df = read_table('TC_Principal', '', 'historico_consolidado', 'df_vol_historico')
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_historico(), 'df_vol_historico.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_custo_fp_veiculo():
    """Custo FP por veículo consolidado multi-ano (Real)."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'historico_consolidado', 'df_veiculos_custo_fp_historico')
    caminho = os.path.join(_pasta_historico(), 'df_veiculos_custo_fp_historico.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_principal_bud():
    """Tabela principal consolidada multi-ano (Budget)."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'historico_consolidado/BUD', 'df_principal_historico_BUD')
    caminho = os.path.join(_pasta_historico_bud(), 'df_principal_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_volume_bud():
    """Volume consolidado multi-ano (Budget)."""
    if _use_snowflake():
        df = read_table('TC_Principal', '', 'historico_consolidado/BUD', 'df_vol_historico_BUD')
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_historico_bud(), 'df_vol_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    df = pd.read_parquet(caminho)
    if 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_historico_custo_fp_veiculo_bud():
    """Custo FP por veículo consolidado multi-ano (Budget)."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'historico_consolidado/BUD', 'df_veiculos_custo_fp_historico_BUD')
    caminho = os.path.join(_pasta_historico_bud(), 'df_veiculos_custo_fp_historico_BUD.parquet')
    if not os.path.exists(caminho):
        return None
    return pd.read_parquet(caminho)


# ═══════════════════════════════════════════════════════════════
#  DATA LOADING — FORECAST / BEST ESTIMATE
# ═══════════════════════════════════════════════════════════════

def _pasta_forecast_tc():
    return _join_data_root('TC_Principal', 'Forecast')


@st.cache_data(ttl=120, show_spinner=False)
def load_forecast_completo():
    """Forecast completo (Real + BE) — gerado pelo BE Simulador."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'Forecast', 'forecast_completo')
    return read_optimized('TC_Principal', '', 'Forecast', 'forecast_completo', prefer='thin', consumer='load_forecast_completo')


@st.cache_data(ttl=120, show_spinner=False)
def load_forecast_agg():
    """Forecast agregado (macro: Ano/Período/Oficina/Tipo) para charts Home."""
    if _use_snowflake():
        return read_table('TC_Principal', '', 'Forecast', 'forecast_completo')
    return read_optimized('TC_Principal', '', 'Forecast', 'forecast_completo', prefer='agg', consumer='load_forecast_agg')


@st.cache_data(ttl=120, show_spinner=False)
def load_forecast_volume():
    """Volume do forecast — gerado pelo BE Simulador."""
    if _use_snowflake():
        df = read_table('TC_Principal', '', 'Forecast', 'df_vol_historico')
        if df is not None and 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        return df
    caminho = os.path.join(_pasta_forecast_tc(), 'df_vol_historico.parquet')
    df = _read_parquet_traced(caminho, 'load_forecast_volume')
    if df is not None and 'Volume' in df.columns:
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    return df


def _get_file_mtime(caminho):
    """Retorna o timestamp de modificação do arquivo para invalidar cache."""
    if os.path.exists(caminho):
        return int(os.path.getmtime(caminho))
    return 0


@st.cache_data(ttl=120, show_spinner=False)
def load_custo_fp_veiculo_forecast(_file_mtime=None):
    """Custo FP por veículo do Forecast — gerado pelo BE Simulador.
    
    Este arquivo é gerado automaticamente ao salvar o forecast,
    usando os percentuais de rateio do Real para distribuir
    os custos entre os veículos.
    
    O parâmetro _file_mtime é usado para invalidar o cache quando o arquivo muda.
    """
    if _use_snowflake():
        return read_table('TC_Principal', '', 'Forecast', 'forecast_veiculos_custo_fp')
    caminho = os.path.join(_pasta_forecast_tc(), 'forecast_veiculos_custo_fp.parquet')
    return _read_parquet_traced(caminho, 'load_custo_fp_veiculo_forecast')


def load_custo_fp_veiculo_forecast_fresh():
    """Carrega forecast com veículo, invalidando cache se arquivo foi modificado."""
    caminho = os.path.join(_pasta_forecast_tc(), 'forecast_veiculos_custo_fp.parquet')
    mtime = _get_file_mtime(caminho)
    return load_custo_fp_veiculo_forecast(_file_mtime=mtime)


# ═══════════════════════════════════════════════════════════════
#  INVALIDAÇÃO SELETIVA DE CACHE
# ═══════════════════════════════════════════════════════════════

def invalidar_cache_dados():
    """Limpa seletivamente o cache dos data loaders (parquets).

    Diferente de ``st.cache_data.clear()`` que limpa TUDO (incluindo
    filtros e opções com TTL longo), esta função invalida apenas as
    funções que carregam dados de parquet — garantindo que a próxima
    leitura reflita os arquivos mais recentes sem perder caches de UX.
    """
    import time as _time

    _loaders = [
        obter_timestamp_parquets, descobrir_anos_tc_principal,
        load_principal, load_volume_bud, load_volume_actual,
        load_tempo_veiculos, load_dea_dedicado, load_volume_fa,
        load_tc_sapiens,
        load_fp_sem_da_veiculos, load_percentual_rateio_veiculos,
        load_custo_rateado_veiculos, load_custo_fp_veiculo, load_cpu_veiculo,
        load_principal_real, load_volume_fa_real, load_tempo_veiculos_real,
        load_volume_veiculos_real, load_comparativo,
        load_custo_fp_veiculo_real, load_cpu_veiculo_real,
        load_percentual_rateio_veiculos_real, load_dea_dedicado_real,
        load_historico_principal, load_historico_volume,
        load_historico_custo_fp_veiculo,
        load_historico_principal_bud, load_historico_volume_bud,
        load_historico_custo_fp_veiculo_bud,
        load_forecast_completo, load_forecast_volume,
        load_custo_fp_veiculo_forecast,
    ]
    for fn in _loaders:
        try:
            fn.clear()
        except Exception:
            pass

    st.session_state['ultima_extracao_ts'] = _time.time()


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
    if df_principal.empty or df_vol_bud.empty or df_vol_actual.empty:
        return None

    _has_custo = 'Custo' in df_principal.columns

    # Determinar colunas de agrupamento
    cols_agrup = ['Ano', 'Período'] if tem_ano and 'Ano' in df_principal.columns else ['Período']

    # Volume por período (e ano se aplicável)
    vol_bud_per = df_vol_bud.groupby(cols_agrup, as_index=False)['Volume'].sum()
    vol_bud_per = vol_bud_per.rename(columns={'Volume': 'Vol_Budget'})

    vol_act_per = df_vol_actual.groupby(cols_agrup, as_index=False)['Volume'].sum()
    vol_act_per = vol_act_per.rename(columns={'Volume': 'Vol_Actual'})

    # Custo fixo e total por período (e ano se aplicável)
    if _has_custo:
        fixo_mask = mask_custo_fixo(df_principal['Custo'])
    else:
        # Sem coluna 'Custo': tratar tudo como variável (Flex = Total × Proporção)
        fixo_mask = pd.Series(False, index=df_principal.index)

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
    elif fator_sel in ('M (Milhões)', 'M (milhões)'):
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
    elif fator_sel in ('M (Milhões)', 'M (milhões)'):
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


# ═══════════════════════════════════════════════════════════════
#  TOOLTIP RICO — CPU POR PERÍODO (Barras + Budget)
# ═══════════════════════════════════════════════════════════════

def build_cpu_tooltip_payload(
    df_detail: pd.DataFrame,
    periodos: list[str],
    valor_col: str,
    volume_por_periodo: dict[str, float],
    moeda_simbolo: str,
    sufixo: str,
    serie_label: str = "Real",
    n_type05: int = 5,
    n_type06: int = 5,
    is_cpu: bool = False,
    group_col: str = "Período",
    flex_mode: bool = False,
    vol_actual_dict: dict[str, float] | None = None,
) -> dict[str, str]:
    """Pré-computa tooltip rico (nível Waterfall) por grupo (Período, Oficina, etc.).

    Retorna ``{grupo: hover_html_string}`` pronto para ``hovertext`` do Plotly.

    Parameters
    ----------
    df_detail : DataFrame com colunas ``group_col``, ``valor_col``, e opcionalmente
        ``Type 05``, ``Type 06``.
    periodos : lista de valores do grupo presentes no gráfico (strings).
    valor_col : coluna monetária a agregar (``'Custo FP'`` ou ``'Total'``).
    volume_por_periodo : dicionário ``{grupo: volume}``.
    moeda_simbolo : ``'€'``, ``'$'``, ``'R$'``.
    sufixo : ``''``, ``' K'``, ``' M'``.
    serie_label : rótulo da série (``'Real'``, ``'BE'``, ``'Flex Bud'``, etc.).
    n_type05 : quantos Type 05 exibir (top N).
    n_type06 : quantos Type 06 exibir dentro de cada Type 05 (top N).
    is_cpu : True quando o gráfico já está em modo CPU (valores /veíc).
    group_col : coluna de agrupamento (``'Período'``, ``'Oficina'``, ``'Veículo'``).
    flex_mode : Se True, calcula Flex Budget (Fixo + NãoFixo × Vol_Actual/Vol_Budget).
    vol_actual_dict : Volume actual por período (necessário quando flex_mode=True).
    """
    result: dict[str, str] = {}
    if df_detail is None or df_detail.empty:
        return result
    if valor_col not in df_detail.columns or group_col not in df_detail.columns:
        return result

    has_t05 = 'Type 05' in df_detail.columns
    has_t06 = 'Type 06' in df_detail.columns

    df = df_detail.copy()
    df[valor_col] = pd.to_numeric(df[valor_col], errors='coerce').fillna(0)
    df['_grp'] = df[group_col].astype(str).str.strip()

    # Flex Budget: classificar fixa/variável
    if flex_mode:
        if 'Custo' in df.columns:
            df['_is_fixo'] = mask_custo_fixo(df['Custo'])
        else:
            df['_is_fixo'] = False

    def _calc_flex_sum(d_slice, prop):
        if not flex_mode:
            return d_slice[valor_col].sum()
        fixo = d_slice.loc[d_slice['_is_fixo'], valor_col].sum()
        nao_fixo = d_slice.loc[~d_slice['_is_fixo'], valor_col].sum()
        return fixo + nao_fixo * prop

    for per in periodos:
        per_str = str(per).strip()
        mask = df['_grp'] == per_str
        df_per = df.loc[mask]
        if df_per.empty:
            result[per_str] = f'<b>{per_str}</b> — {serie_label}<br>Sem dados'
            continue

        # Calcular total (Flex se flex_mode)
        _prop = 1.0
        if flex_mode:
            _vr = (vol_actual_dict or {}).get(per_str, 0)
            _vb = volume_por_periodo.get(per_str, 0)
            _prop = (_vr / _vb) if _vb else 1.0
            total_per = _calc_flex_sum(df_per, _prop)
            vol = _vr  # CPU usa vol actual (consistente com gráfico)
        else:
            total_per = df_per[valor_col].sum()
            vol = volume_por_periodo.get(per_str, 0)
        cpu_val = (total_per / vol) if vol else 0.0

        # ── Cabeçalho ──
        parts: list[str] = [f'<b>{per_str}</b> — {serie_label}']
        if is_cpu:
            parts.append(f'CPU: {moeda_simbolo} {cpu_val:,.2f}/veíc')
            parts.append(f'Total: {moeda_simbolo} {total_per:,.2f}{sufixo}')
        else:
            parts.append(f'Total: {moeda_simbolo} {total_per:,.2f}{sufixo}')
            if vol:
                parts.append(f'CPU: {moeda_simbolo} {cpu_val:,.2f}/veíc')
        if vol:
            parts.append(f'Volume: {vol:,.0f} unid.')

        # ── Breakdown Type 05 → Type 06 ──
        if has_t05:
            abs_total = abs(total_per) if total_per != 0 else 1
            if flex_mode:
                _t05_rows = []
                for _t05k in df_per['Type 05'].unique():
                    _t05s = df_per.loc[df_per['Type 05'] == _t05k]
                    _t05_rows.append({'Type 05': _t05k, valor_col: _calc_flex_sum(_t05s, _prop)})
                t05_agg = (pd.DataFrame(_t05_rows).sort_values(valor_col, key=lambda s: s.abs(), ascending=False)
                           if _t05_rows else pd.DataFrame(columns=['Type 05', valor_col]))
            else:
                t05_agg = (
                    df_per.groupby('Type 05', as_index=False)[valor_col]
                    .sum()
                    .sort_values(valor_col, key=lambda s: s.abs(), ascending=False)
                )
            top_t05 = t05_agg.head(n_type05)
            if not top_t05.empty:
                parts.append('──────────')
                parts.append(f'<b>Type 05 (Top {min(n_type05, len(top_t05))}):</b>')
                for _, row05 in top_t05.iterrows():
                    t05_name = str(row05['Type 05']).strip()
                    t05_val = row05[valor_col]
                    if abs(t05_val) < 0.5:
                        continue
                    t05_pct = t05_val / abs_total * 100 if abs_total else 0
                    if is_cpu and vol:
                        _t05_cpu = t05_val / vol
                        parts.append(
                            f'• <b>{t05_name}</b>: {moeda_simbolo} {_t05_cpu:,.2f}/veíc ({t05_pct:.1f}%)'
                        )
                    else:
                        parts.append(
                            f'• <b>{t05_name}</b>: {moeda_simbolo} {t05_val:,.0f}{sufixo} ({t05_pct:.1f}%)'
                        )
                    # Sub-breakdown Type 06
                    if has_t06:
                        df_t05 = df_per.loc[df_per['Type 05'].astype(str).str.strip() == t05_name]
                        abs_t05 = abs(t05_val) if t05_val != 0 else 1
                        if flex_mode:
                            _t06_rows = []
                            for _t06k in df_t05['Type 06'].unique():
                                _t06s = df_t05.loc[df_t05['Type 06'] == _t06k]
                                _t06_rows.append({'Type 06': _t06k, valor_col: _calc_flex_sum(_t06s, _prop)})
                            t06_agg = (pd.DataFrame(_t06_rows).sort_values(valor_col, key=lambda s: s.abs(), ascending=False)
                                       if _t06_rows else pd.DataFrame(columns=['Type 06', valor_col]))
                        else:
                            t06_agg = (
                                df_t05.groupby('Type 06', as_index=False)[valor_col]
                                .sum()
                                .sort_values(valor_col, key=lambda s: s.abs(), ascending=False)
                            )
                        top_t06 = t06_agg.head(n_type06)
                        top_t06_sum = top_t06[valor_col].sum()
                        for _, row06 in top_t06.iterrows():
                            t06_name = str(row06['Type 06']).strip()
                            if len(t06_name) > 30:
                                t06_name = t06_name[:27] + '...'
                            t06_val = row06[valor_col]
                            if abs(t06_val) < 0.5:
                                continue
                            t06_pct = t06_val / abs_t05 * 100 if abs_t05 else 0
                            if is_cpu and vol:
                                _t06_cpu = t06_val / vol
                                parts.append(
                                    f'   {t06_name}: {moeda_simbolo} {_t06_cpu:,.2f}/veíc ({t06_pct:.1f}%)'
                                )
                            else:
                                parts.append(
                                    f'   {t06_name}: {moeda_simbolo} {t06_val:,.0f}{sufixo} ({t06_pct:.1f}%)'
                                )
                        outros_t06 = t05_val - top_t06_sum
                        if abs(outros_t06) > 0.5:
                            pct_o = outros_t06 / abs_t05 * 100 if abs_t05 else 0
                            if is_cpu and vol:
                                _o_cpu = outros_t06 / vol
                                parts.append(
                                    f'   Outros: {moeda_simbolo} {_o_cpu:,.2f}/veíc ({pct_o:.1f}%)'
                                )
                            else:
                                parts.append(
                                    f'   Outros: {moeda_simbolo} {outros_t06:,.0f}{sufixo} ({pct_o:.1f}%)'
                                )
            # Outros Type 05 (restante)
            outros_t05_val = total_per - top_t05[valor_col].sum()
            if abs(outros_t05_val) > 0.5:
                pct_o5 = outros_t05_val / abs_total * 100 if abs_total else 0
                if is_cpu and vol:
                    _o5_cpu = outros_t05_val / vol
                    parts.append(
                        f'• Outros Type 05: {moeda_simbolo} {_o5_cpu:,.2f}/veíc ({pct_o5:.1f}%)'
                    )
                else:
                    parts.append(
                        f'• Outros Type 05: {moeda_simbolo} {outros_t05_val:,.0f}{sufixo} ({pct_o5:.1f}%)'
                    )

        result[per_str] = '<br>'.join(parts)

    return result


def build_delta_tooltip_payload(
    df_real: pd.DataFrame,
    df_bud: pd.DataFrame,
    periodos: list[str],
    valor_col: str,
    vol_real: dict[str, float],
    vol_bud: dict[str, float],
    moeda_simbolo: str,
    sufixo: str,
    serie_label: str = "Delta (Real - Flex Bud)",
    n_type05: int = 5,
    n_type06: int = 5,
    is_cpu: bool = False,
    group_col: str = "Período",
) -> dict[str, str]:
    """Pré-computa tooltip rico para o gráfico Delta (Real − Flex Bud).

    Calcula o Flex Budget por Type 05/06 usando a mesma fórmula de
    ``calcular_flex_budget`` (Fixo + NãoFixo × Vol_Actual/Vol_Budget) para
    garantir coerência com o valor exibido no gráfico.

    Retorna ``{grupo: hover_html}`` pronto para ``hovertext`` do Plotly.
    """
    result: dict[str, str] = {}
    if df_real is None or df_real.empty or df_bud is None or df_bud.empty:
        return result
    if valor_col not in df_real.columns or group_col not in df_real.columns:
        return result
    if valor_col not in df_bud.columns or group_col not in df_bud.columns:
        return result

    has_t05 = 'Type 05' in df_real.columns and 'Type 05' in df_bud.columns
    has_t06 = 'Type 06' in df_real.columns and 'Type 06' in df_bud.columns
    has_custo = 'Custo' in df_bud.columns

    dfr = df_real.copy()
    dfb = df_bud.copy()
    dfr[valor_col] = pd.to_numeric(dfr[valor_col], errors='coerce').fillna(0)
    dfb[valor_col] = pd.to_numeric(dfb[valor_col], errors='coerce').fillna(0)
    dfr['_grp'] = dfr[group_col].astype(str).str.strip()
    dfb['_grp'] = dfb[group_col].astype(str).str.strip()

    # Classificar cada linha do budget como fixa ou variável
    if has_custo:
        dfb['_is_fixo'] = mask_custo_fixo(dfb['Custo'])
    else:
        dfb['_is_fixo'] = False  # sem coluna Custo, tudo variável

    def _calc_flex(db_slice: pd.DataFrame, proporcao: float) -> float:
        """Calcula Flex = Fixo + NãoFixo × proporção para um slice do budget."""
        fixo = db_slice.loc[db_slice['_is_fixo'], valor_col].sum()
        nao_fixo = db_slice.loc[~db_slice['_is_fixo'], valor_col].sum()
        return fixo + nao_fixo * proporcao

    for per in periodos:
        per_str = str(per).strip()
        dr = dfr.loc[dfr['_grp'] == per_str]
        db = dfb.loc[dfb['_grp'] == per_str]

        total_real = dr[valor_col].sum() if not dr.empty else 0.0

        # Calcular Flex Budget com a mesma fórmula do gráfico
        vr = vol_real.get(per_str, 0)
        vb = vol_bud.get(per_str, 0)
        proporcao = (vr / vb) if vb else 1.0
        total_flex = _calc_flex(db, proporcao) if not db.empty else 0.0
        delta_total = total_real - total_flex

        delta_vol = vr - vb
        cpu_real = (total_real / vr) if vr else 0.0
        cpu_flex = (total_flex / vr) if vr else 0.0  # CPU Flex usa Vol_Actual
        delta_cpu = cpu_real - cpu_flex

        # ── Cabeçalho (somente deltas) ──
        parts: list[str] = [f'<b>{per_str}</b> — {serie_label}']
        if is_cpu:
            parts.append(f'Δ CPU: {moeda_simbolo} {delta_cpu:+,.2f}/veíc')
            parts.append(f'Δ Total: {moeda_simbolo} {delta_total:+,.2f}{sufixo}')
        else:
            parts.append(f'Δ Total: {moeda_simbolo} {delta_total:+,.2f}{sufixo}')
            if vr or vb:
                parts.append(f'Δ CPU: {moeda_simbolo} {delta_cpu:+,.2f}/veíc')
        if vr or vb:
            parts.append(f'Δ Volume: {delta_vol:+,.0f} unid. (Real: {vr:,.0f} | Bud: {vb:,.0f})')

        # ── Breakdown Type 05 → Type 06 (delta com Flex) ──
        if has_t05:
            # Real por Type 05
            r05 = dr.groupby('Type 05', as_index=False)[valor_col].sum().rename(
                columns={valor_col: '_real'}
            )
            # Flex por Type 05 (Fixo + NãoFixo × proporção)
            all_t05 = db['Type 05'].unique() if not db.empty else []
            flex05_rows = []
            for t05 in all_t05:
                db_t05 = db.loc[db['Type 05'] == t05]
                flex05_rows.append({'Type 05': t05, '_flex': _calc_flex(db_t05, proporcao)})
            b05 = pd.DataFrame(flex05_rows) if flex05_rows else pd.DataFrame(columns=['Type 05', '_flex'])

            m05 = r05.merge(b05, on='Type 05', how='outer').fillna(0)
            m05['_delta'] = m05['_real'] - m05['_flex']
            m05 = m05.sort_values('_delta', key=lambda s: s.abs(), ascending=False)
            top_t05 = m05.head(n_type05)

            abs_delta_total = abs(delta_total) if delta_total != 0 else 1

            if not top_t05.empty:
                parts.append('──────────')
                parts.append(f'<b>Type 05 Δ (Top {min(n_type05, len(top_t05))}):</b>')
                for _, row05 in top_t05.iterrows():
                    t05_name = str(row05['Type 05']).strip()
                    t05_delta = row05['_delta']
                    if abs(t05_delta) < 0.5:
                        continue
                    t05_pct = t05_delta / abs_delta_total * 100 if abs_delta_total else 0
                    if is_cpu and vr:
                        _t05_cpu = t05_delta / vr
                        parts.append(
                            f'• <b>{t05_name}</b>: {moeda_simbolo} {_t05_cpu:+,.2f}/veíc ({t05_pct:+.1f}%)'
                        )
                    else:
                        parts.append(
                            f'• <b>{t05_name}</b>: {moeda_simbolo} {t05_delta:+,.0f}{sufixo} ({t05_pct:+.1f}%)'
                        )
                    # Sub-breakdown Type 06
                    if has_t06:
                        r06 = dr.loc[dr['Type 05'].astype(str).str.strip() == t05_name]
                        r06_agg = r06.groupby('Type 06', as_index=False)[valor_col].sum().rename(
                            columns={valor_col: '_real'}
                        )
                        db_t05 = db.loc[db['Type 05'].astype(str).str.strip() == t05_name]
                        all_t06 = db_t05['Type 06'].unique() if not db_t05.empty else []
                        flex06_rows = []
                        for t06 in all_t06:
                            db_t06 = db_t05.loc[db_t05['Type 06'] == t06]
                            flex06_rows.append({'Type 06': t06, '_flex': _calc_flex(db_t06, proporcao)})
                        b06_agg = pd.DataFrame(flex06_rows) if flex06_rows else pd.DataFrame(columns=['Type 06', '_flex'])

                        m06 = r06_agg.merge(b06_agg, on='Type 06', how='outer').fillna(0)
                        m06['_delta'] = m06['_real'] - m06['_flex']
                        m06 = m06.sort_values('_delta', key=lambda s: s.abs(), ascending=False)
                        top_t06 = m06.head(n_type06)
                        for _, row06 in top_t06.iterrows():
                            t06_name = str(row06['Type 06']).strip()
                            if len(t06_name) > 30:
                                t06_name = t06_name[:27] + '...'
                            t06_delta = row06['_delta']
                            if abs(t06_delta) < 0.5:
                                continue
                            t06_pct = t06_delta / abs_delta_total * 100 if abs_delta_total else 0
                            if is_cpu and vr:
                                _t06_cpu = t06_delta / vr
                                parts.append(
                                    f'   {t06_name}: {moeda_simbolo} {_t06_cpu:+,.2f}/veíc ({t06_pct:+.1f}%)'
                                )
                            else:
                                parts.append(
                                    f'   {t06_name}: {moeda_simbolo} {t06_delta:+,.0f}{sufixo} ({t06_pct:+.1f}%)'
                                )
                        outros_t06 = t05_delta - top_t06['_delta'].sum()
                        if abs(outros_t06) > 0.5:
                            pct_o = outros_t06 / abs_delta_total * 100 if abs_delta_total else 0
                            if is_cpu and vr:
                                _o_cpu = outros_t06 / vr
                                parts.append(
                                    f'   Outros: {moeda_simbolo} {_o_cpu:+,.2f}/veíc ({pct_o:+.1f}%)'
                                )
                            else:
                                parts.append(
                                    f'   Outros: {moeda_simbolo} {outros_t06:+,.0f}{sufixo} ({pct_o:+.1f}%)'
                                )
                # Outros Type 05
                outros_t05 = delta_total - top_t05['_delta'].sum()
                if abs(outros_t05) > 0.5:
                    pct_o5 = outros_t05 / abs_delta_total * 100 if abs_delta_total else 0
                    if is_cpu and vr:
                        _o5_cpu = outros_t05 / vr
                        parts.append(
                            f'• Outros Type 05: {moeda_simbolo} {_o5_cpu:+,.2f}/veíc ({pct_o5:+.1f}%)'
                        )
                    else:
                        parts.append(
                            f'• Outros Type 05: {moeda_simbolo} {outros_t05:+,.0f}{sufixo} ({pct_o5:+.1f}%)'
                        )

        result[per_str] = '<br>'.join(parts)

    return result


# ═══════════════════════════════════════════════════════════════
#  FORMATAÇÃO PT-BR
# ═══════════════════════════════════════════════════════════════

def _fmt_ptbr(valor, decimais=2):
    """Formata número para padrão pt-BR: 1.234.567,89"""
    if pd.isna(valor) or valor is None:
        return ""
    try:
        s = f"{float(valor):,.{decimais}f}"          # 1,234,567.89
        return s.replace(',', 'X').replace('.', ',').replace('X', '.')
    except (ValueError, TypeError):
        return str(valor)


# ═══════════════════════════════════════════════════════════════
#  FLEX BUDGET DETALHADO (com dimensões)
# ═══════════════════════════════════════════════════════════════

def calcular_flex_budget_detalhado(
    df_principal, df_vol_bud, df_vol_actual,
    col_custo='Custo FP', tem_ano=False,
):
    """
    Calcula Budget Flex **mantendo** as colunas dimensionais
    (Oficina, Type 05, Type 06, Account, Custo).

    Volume não tem essas dimensões, então a proporção
    Vol_Actual / Vol_Budget é calculada por Período (global)
    e aplicada a cada linha dimensional.

    Retorna DataFrame com:
      Oficina, Type 05, Type 06, Account, Custo, Período,
      [Ano], Flex_Bud
    ou None se dados insuficientes.
    """
    if df_vol_bud is None or df_vol_actual is None:
        return None
    if df_principal.empty or df_vol_bud.empty or df_vol_actual.empty:
        return None

    _has_custo = 'Custo' in df_principal.columns

    cols_agrup_vol = (
        ['Ano', 'Período']
        if tem_ano and 'Ano' in df_principal.columns
        else ['Período']
    )

    # Proporção de volume por Período (agregado — sem dimensões)
    vol_bud_per = (
        df_vol_bud.groupby(cols_agrup_vol, as_index=False)['Volume']
        .sum().rename(columns={'Volume': 'Vol_Budget'})
    )
    vol_act_per = (
        df_vol_actual.groupby(cols_agrup_vol, as_index=False)['Volume']
        .sum().rename(columns={'Volume': 'Vol_Actual'})
    )
    prop = vol_bud_per.merge(vol_act_per, on=cols_agrup_vol, how='left')
    prop['Vol_Budget'] = prop['Vol_Budget'].fillna(0)
    prop['Vol_Actual'] = prop['Vol_Actual'].fillna(0)
    prop['Proporcao'] = np.where(
        prop['Vol_Budget'] != 0,
        prop['Vol_Actual'] / prop['Vol_Budget'], 1.0,
    )
    prop = prop[cols_agrup_vol + ['Proporcao']]

    # Dimensões disponíveis
    dim_cols = [c for c in _DIM_COLS_DETALHE if c in df_principal.columns]
    if not dim_cols:
        return None  # sem dimensões, usar calcular_flex_budget padrão

    # Agregar custo por (dimensões + Período)
    grp_cols = dim_cols + cols_agrup_vol
    agr = df_principal.groupby(grp_cols, as_index=False)[col_custo].sum()

    # Classificar Fixo / Variável
    if _has_custo and 'Custo' in agr.columns:
        agr['_is_fixo'] = mask_custo_fixo(agr['Custo'])
    else:
        # Sem coluna 'Custo': tratar tudo como variável
        agr['_is_fixo'] = False

    # Juntar proporção
    agr = agr.merge(prop, on=cols_agrup_vol, how='left')
    agr['Proporcao'] = agr['Proporcao'].fillna(1.0)

    # Flex = Fixo puro + Variável × proporção
    agr['Flex_Bud'] = np.where(
        agr['_is_fixo'],
        agr[col_custo],                         # Fixo: não flexibiliza
        agr[col_custo] * agr['Proporcao'],       # Variável: flexibiliza
    )

    # Limpar e retornar
    resultado = agr[grp_cols + ['Flex_Bud']].copy()
    return resultado


# ═══════════════════════════════════════════════════════════════
#  TAB DADOS DETALHADOS — HELPERS
# ═══════════════════════════════════════════════════════════════

# Colunas dimensionais que queremos manter nas tabelas detalhadas
_DIM_COLS_DETALHE = ['Oficina', 'Type 05', 'Type 06', 'Account', 'Custo']


def _pivotar_detalhado(df, col_valor, col_periodo='Período'):
    """
    Pivota DataFrame (long) → wide com TODAS as dimensões.

    index = [Oficina, Type 05, Type 06, Account, Custo] (as que existirem)
    columns = Período (meses)  +  coluna Total

    Oculta linhas onde TODOS os meses são 0 ou NaN (somente exibição).
    Adiciona linha TOTAL ao final.

    Retorna (df_pivot_num, oficinas_list).
    """
    if df is None or df.empty or col_valor not in df.columns:
        return pd.DataFrame(), []

    df_w = df
    if col_periodo not in df_w.columns:
        return pd.DataFrame(), []

    # ── Determinar colunas de index disponíveis ──
    idx_cols = [c for c in _DIM_COLS_DETALHE if c in df_w.columns]
    if not idx_cols:
        idx_cols = ['_dim']
        df_w['_dim'] = 'Total'

    # ── Agregar ──
    agr = df_w.groupby(idx_cols + [col_periodo], as_index=False)[col_valor].sum()

    piv = agr.pivot_table(
        index=idx_cols, columns=col_periodo,
        values=col_valor, aggfunc='sum', fill_value=0,
    )

    # Reordenar colunas pelos meses
    cols_ord = [m for m in ORDEM_MESES if m in piv.columns]
    extras = [c for c in piv.columns if c not in cols_ord]
    piv = piv[cols_ord + extras]

    # Coluna Total
    piv['Total'] = piv[cols_ord + extras].sum(axis=1)

    # ── Linha TOTAL (calculada ANTES de remover zeros) ──
    linha_total = piv.sum(numeric_only=True)

    # ── Ocultar linhas 100 % zero/NaN ──
    num_cols_check = cols_ord + extras + ['Total']
    mask_nonzero = piv[num_cols_check].abs().sum(axis=1) > 0.005
    piv = piv.loc[mask_nonzero]

    piv = piv.reset_index()
    total_row = {c: 'TOTAL' if i == 0 else '' for i, c in enumerate(idx_cols)}
    for c in piv.columns:
        if c not in idx_cols:
            total_row[c] = linha_total.get(c, 0)
    piv = pd.concat([piv, pd.DataFrame([total_row])], ignore_index=True)

    oficinas = []
    if 'Oficina' in piv.columns:
        oficinas = sorted([
            o for o in piv['Oficina'].unique()
            if o not in ('TOTAL', '')
        ])

    return piv, oficinas


def _pivotar_flex(df_flex, col_periodo='Período'):
    """Pivota o df_flex → wide (linha única com Flex_Bud total).

    Usado como fallback quando não há Flex detalhado.
    """
    if df_flex is None or df_flex.empty:
        return pd.DataFrame(), []
    if col_periodo not in df_flex.columns:
        return pd.DataFrame(), []

    df_w = df_flex.copy()
    col_usar = 'Flex_Bud'
    label = 'Flex Budget'

    if col_usar not in df_w.columns:
        return pd.DataFrame(), []

    agr = df_w.groupby(col_periodo, as_index=False)[col_usar].sum()
    piv = agr.set_index(col_periodo).T

    cols_ord = [m for m in ORDEM_MESES if m in piv.columns]
    extras = [c for c in piv.columns if c not in cols_ord]
    piv = piv[cols_ord + extras]
    piv['Total'] = piv.sum(axis=1)
    piv = piv.reset_index(drop=True)
    piv.insert(0, 'Descrição', [label])
    return piv, []


def _render_tabela_fmt(st_obj, df_show, colunas_num):
    """Renderiza dataframe com formatação pt-BR."""
    df_fmt = df_show.copy()
    for c in colunas_num:
        if c in df_fmt.columns:
            df_fmt[c] = df_show[c].apply(lambda v: _fmt_ptbr(v))
    st_obj.dataframe(df_fmt, width="stretch", hide_index=True)


def render_secao_tabela_detalhe(
    df_pivot, oficinas, titulo, icone, page_key, ano,
    simbolo='R$', sufixo='', expanded=False,
    modo='Total',
):
    """
    Renderiza seção: expander com tabela resumo por oficina +
    sub-expanders com linhas detalhadas + download Excel.

    ``modo``:
        - 'Total'          → tabela completa (sem separação)
        - 'Fixo/Variável'  → dentro de cada oficina divide
                              em **Fixo** e **Variável**
    """
    import streamlit as _st

    if df_pivot.empty:
        _st.info(f"ℹ️ Dados de {titulo} não disponíveis.")
        return

    # Detectar colunas dimensionais e numéricas
    dim_cols = [c for c in _DIM_COLS_DETALHE if c in df_pivot.columns]
    if not dim_cols and 'Descrição' in df_pivot.columns:
        dim_cols = ['Descrição']
    if '_dim' in df_pivot.columns:
        dim_cols = ['_dim']
    colunas_num = [c for c in df_pivot.columns if c not in dim_cols]

    separar_custo = (
        modo == 'Fixo/Variável'
        and 'Custo' in df_pivot.columns
        and 'Oficina' in df_pivot.columns
    )

    with _st.expander(f"{icone} **{titulo}**", expanded=expanded):
        # ── Tabela resumo: agrupar por Oficina × Meses ──
        if 'Oficina' in df_pivot.columns and oficinas:
            resumo = df_pivot[
                ~df_pivot['Oficina'].isin(['TOTAL', ''])
            ].groupby('Oficina', as_index=False)[colunas_num].sum()
            # Ocultar oficinas zeradas no resumo
            _num_only = [c for c in colunas_num if c in resumo.columns]
            if _num_only:
                resumo = resumo.loc[
                    resumo[_num_only].abs().sum(axis=1) > 0.005
                ]
            # Linha TOTAL no resumo
            if not resumo.empty:
                total_resumo = resumo[_num_only].sum()
                total_row_r = {'Oficina': 'TOTAL'}
                for c in _num_only:
                    total_row_r[c] = total_resumo.get(c, 0)
                resumo = pd.concat(
                    [resumo, pd.DataFrame([total_row_r])],
                    ignore_index=True,
                )

            _st.markdown("**Resumo por Oficina**")
            _render_tabela_fmt(_st, resumo, colunas_num)
        else:
            _render_tabela_fmt(_st, df_pivot, colunas_num)

        # ── Sub-expanders por oficina ──
        if oficinas:
            for ofc in oficinas:
                mask = df_pivot['Oficina'] == ofc
                df_ofc = df_pivot.loc[mask].copy()
                if df_ofc.empty:
                    continue
                total_ofc = (
                    df_ofc['Total'].sum()
                    if 'Total' in df_ofc.columns else 0
                )
                if abs(total_ofc) < 0.005:
                    continue  # Pular oficinas com total zero
                total_str = f"{simbolo} {_fmt_ptbr(total_ofc)}{sufixo}"
                n_linhas = len(df_ofc)

                with _st.expander(
                    f"🏭 **{ofc}** — Total: {total_str}"
                    f"  ({n_linhas} linha{'s' if n_linhas > 1 else ''})"
                ):
                    show_cols = [
                        c for c in df_ofc.columns if c != 'Oficina'
                    ]

                    if separar_custo:
                        # ── Dividir em Fixo e Variável ──
                        is_fixo = mask_custo_fixo(df_ofc['Custo'])
                        df_fixo = df_ofc.loc[is_fixo, show_cols]
                        df_var = df_ofc.loc[~is_fixo, show_cols]

                        if not df_fixo.empty:
                            _st.markdown("**🟢 Custo Fixo**")
                            _render_tabela_fmt(_st, df_fixo, colunas_num)

                        if not df_var.empty:
                            _st.markdown("**🔵 Custo Variável**")
                            _render_tabela_fmt(_st, df_var, colunas_num)
                    else:
                        # ── Modo Total: tabela única ──
                        _render_tabela_fmt(
                            _st, df_ofc[show_cols], colunas_num,
                        )

            # ── Expander TOTAL (re-agregar TODAS as oficinas) ──
            # Pegar todas as linhas reais (exceto a linha sintética TOTAL)
            df_all_real = df_pivot[
                ~df_pivot['Oficina'].isin(['TOTAL', ''])
            ].copy()
            if not df_all_real.empty:
                # Colunas de dimensão sem Oficina
                _dim_sem_ofc = [
                    c for c in _DIM_COLS_DETALHE
                    if c in df_all_real.columns and c != 'Oficina'
                ]
                # Re-agregar: somar valores de TODAS as oficinas
                # por (Type 05, Type 06, Account, Custo)
                df_total_reag = df_all_real.groupby(
                    _dim_sem_ofc, as_index=False, dropna=False,
                )[colunas_num].sum()
                # Recalcular coluna Total
                _meses_cols_t = [
                    c for c in colunas_num if c != 'Total'
                ]
                if _meses_cols_t:
                    df_total_reag['Total'] = (
                        df_total_reag[_meses_cols_t].sum(axis=1)
                    )
                # Filtrar linhas 100% zero
                _mask_nz = (
                    df_total_reag[colunas_num].abs().sum(axis=1)
                    > 0.005
                )
                df_total_reag = df_total_reag.loc[_mask_nz]

                total_geral = (
                    df_total_reag['Total'].sum()
                    if 'Total' in df_total_reag.columns else 0
                )
                total_geral_str = (
                    f"{simbolo} {_fmt_ptbr(total_geral)}{sufixo}"
                )
                n_total = len(df_total_reag)
                with _st.expander(
                    f"📊 **TOTAL** — Total: {total_geral_str}"
                    f"  ({n_total} linha{'s' if n_total > 1 else ''})",
                    expanded=False,
                ):
                    if separar_custo and 'Custo' in df_total_reag.columns:
                        is_fixo_t = mask_custo_fixo(
                            df_total_reag['Custo']
                        )
                        df_fixo_t = df_total_reag.loc[is_fixo_t]
                        df_var_t = df_total_reag.loc[~is_fixo_t]

                        if not df_fixo_t.empty:
                            _st.markdown("**🟢 Custo Fixo**")
                            _render_tabela_fmt(
                                _st, df_fixo_t, colunas_num,
                            )

                        if not df_var_t.empty:
                            _st.markdown("**🔵 Custo Variável**")
                            _render_tabela_fmt(
                                _st, df_var_t, colunas_num,
                            )
                    else:
                        _render_tabela_fmt(
                            _st, df_total_reag, colunas_num,
                        )

        # ── Download Excel (via browser) ──
        try:
            nome_limpo = titulo.replace(' ', '_').replace('/', '_')
            fname = f"TC_Veiculos_{nome_limpo}_{ano}.xlsx"
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as w:
                df_pivot.to_excel(
                    w, index=False, sheet_name=nome_limpo[:31],
                )
            _st.download_button(
                f"📥 Baixar {titulo} (Excel)",
                data=buf.getvalue(),
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{page_key}_{ano}",
                use_container_width=True,
            )
        except Exception as e:
            _st.error(f"❌ Erro ao gerar Excel: {e}")
