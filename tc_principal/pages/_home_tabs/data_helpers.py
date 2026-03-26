"""Data helper functions for home_tc dashboard."""
import os
import json
import unicodedata
import streamlit as st
import pandas as pd

from tc_core.utils.portabilidade import get_data_root
from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    load_forecast_agg, load_forecast_completo,
    normalizar_periodo, _render_tabela_fmt,
)

_DATA_ROOT = str(get_data_root())

def _resumo_por_veiculo(st_obj, df, col_valor):
    """Cria tabela resumo por veículo (1 linha por veículo + TOTAL)."""
    if df is None or df.empty:
        return
    if 'Veículo' not in df.columns or 'Período' not in df.columns:
        return
    if col_valor not in df.columns:
        return
    _agg = df.groupby(
        ['Veículo', 'Período'], as_index=False,
    )[col_valor].sum()
    _piv = _agg.pivot_table(
        index='Veículo', columns='Período',
        values=col_valor, aggfunc='sum', fill_value=0,
    )
    _cols_ord = [m for m in ORDEM_MESES if m in _piv.columns]
    _extras = [c for c in _piv.columns if c not in _cols_ord]
    _piv = _piv[_cols_ord + _extras]
    _piv['Total'] = _piv.sum(axis=1)
    _piv = _piv.loc[_piv.abs().sum(axis=1) > 0.005]
    _piv = _piv.reset_index()
    _num_cols = [c for c in _piv.columns if c != 'Veículo']
    if _piv.empty:
        return
    _total = {'Veículo': 'TOTAL'}
    for c in _num_cols:
        _total[c] = _piv[c].sum()
    _piv = pd.concat(
        [_piv, pd.DataFrame([_total])], ignore_index=True,
    )
    st_obj.markdown("**Resumo por Veículo**")
    _render_tabela_fmt(st_obj, _piv, _num_cols)


def _carregar_rateios_manuais():
    """Carrega rateios manuais (QY/GS/SM) do arquivo raiz do projeto."""
    caminho = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'rateios_manuais.json'
    )
    padrao = {'QY': 0.087526, 'GS': 0.086982, 'SM': 0.075452}
    try:
        if os.path.exists(caminho):
            with open(caminho, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {
                'QY': float(data.get('QY', padrao['QY'])),
                'GS': float(data.get('GS', padrao['GS'])),
                'SM': float(data.get('SM', padrao['SM'])),
            }
    except Exception:
        pass
    return padrao


_MAP_PER = {
    'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
    'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
    'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
    'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro',
}


def _forecast_mtime():
    """Retorna mtime do forecast para invalidação de cache."""
    p = os.path.join(
        _DATA_ROOT, "TC_Principal", "Forecast",
        "forecast_completo.parquet",
    )
    try:
        return os.path.getmtime(p)
    except OSError:
        return 0


@st.cache_data(ttl=3600, show_spinner=True)
def _load_forecast(ano=None, file_mtime=0):
    """Carrega forecast via Data Router (AGG para macro charts Home)."""
    df = load_forecast_agg()
    if df is None:
        return None
    df = normalizar_periodo(df)
    if 'Período' in df.columns:
        df['Período'] = (
            df['Período'].astype(str).str.strip().str.lower()
            .map(_MAP_PER).fillna(df['Período'])
        )
    for c in COLUNAS_MONETARIAS + ['Total', 'Volume', 'CPU']:
        if c in df.columns and df[c].dtype == 'object':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    if ano and ano != "Todos" and 'Ano' in df.columns:
        try:
            df = df[df['Ano'] == int(ano)].copy()
        except (ValueError, TypeError):
            pass
    # Normalizar coluna Tipo: Histórico / BE
    if 'Tipo' in df.columns:
        def _norm_tipo(v):
            if pd.isna(v):
                return 'BE'
            txt = str(v).replace('\ufffd', '').strip().lower()
            txt = (
                unicodedata.normalize('NFKD', txt)
                .encode('ascii', 'ignore')
                .decode('ascii')
            )
            if 'hist' in txt:
                return 'Histórico'
            return 'BE'
        df['Tipo'] = df['Tipo'].apply(_norm_tipo)
    return df


@st.cache_data(ttl=3600, show_spinner=True)
def _load_forecast_full(ano=None, file_mtime=0):
    """Carrega forecast COMPLETO (todas as colunas, para tabs detalhe/flex)."""
    df = load_forecast_completo()
    if df is None:
        return None
    df = normalizar_periodo(df)
    if 'Período' in df.columns:
        df['Período'] = (
            df['Período'].astype(str).str.strip().str.lower()
            .map(_MAP_PER).fillna(df['Período'])
        )
    for c in COLUNAS_MONETARIAS + ['Total', 'Volume', 'CPU']:
        if c in df.columns and df[c].dtype == 'object':
            df[c] = pd.to_numeric(df[c], errors='coerce')
    if ano and ano != "Todos" and 'Ano' in df.columns:
        try:
            df = df[df['Ano'] == int(ano)].copy()
        except (ValueError, TypeError):
            pass
    if 'Tipo' in df.columns:
        def _norm_tipo(v):
            if pd.isna(v):
                return 'BE'
            txt = str(v).replace('\ufffd', '').strip().lower()
            txt = (
                unicodedata.normalize('NFKD', txt)
                .encode('ascii', 'ignore')
                .decode('ascii')
            )
            if 'hist' in txt:
                return 'Histórico'
            return 'BE'
        df['Tipo'] = df['Tipo'].apply(_norm_tipo)
    return df
