"""
TC Veículos — Home
Dashboard com visão geral do custo de produção de veículos.
Padrão visual TC Ext: Altair, CSS global, seletores universais.
"""

import sys as _sys
import os as _os
if hasattr(_sys, '_MEIPASS'):
    _ROOT = _sys._MEIPASS
else:
    _ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
from tc_core.utils.portabilidade import get_data_root
from tc_core.telemetry import perf_timer
from tc_core.feature_flags import get_flag

_DATA_ROOT = str(get_data_root())

from tc_principal.shared import (
    COLUNAS_MONETARIAS,
    load_principal, load_principal_real,
    load_volume_bud, load_volume_actual,
    load_tempo_veiculos,
    load_custo_fp_veiculo, load_custo_fp_veiculo_real,
    load_custo_fp_veiculo_forecast_fresh,
    normalizar_periodo,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    aplicar_fator_df,
    converter_moeda_df, obter_sufixo_fator,
    extrair_redis,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header,
    render_sidebar_global, aplicar_filtros,
)

_ALTAIR_IMPORT_ERROR = None
try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception as exc:
    alt = None
    _ALTAIR_IMPORT_ERROR = exc

# Dicionário de meses em português
meses_pt = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro',
}



# ── Módulos extraídos ──
from tc_principal.pages._home_tabs.chart_utils import create_periodo_chart, _preparar_flex  # noqa: F401
from tc_principal.pages._home_tabs.data_helpers import (
    _resumo_por_veiculo, _carregar_rateios_manuais,  # noqa: F401
    _forecast_mtime, _load_forecast, _load_forecast_full,
    _MAP_PER,
)
from tc_principal.pages._home_tabs import tab_tc_veiculos as _tab1
from tc_principal.pages._home_tabs import tab_volume as _tab2
from tc_principal.pages._home_tabs import tab_analise_flex as _tab3
from tc_principal.pages._home_tabs import tab_tempo_producao as _tab4
from tc_principal.pages._home_tabs import tab_dados_detalhados as _tab5

def render():
    """Renderiza a página Home do TC Veículos."""

    if alt is None:
        st.error(
            "❌ O Altair não pôde ser carregado nesta execução do executável.\n\n"
            f"Detalhe: {_ALTAIR_IMPORT_ERROR}"
        )
        st.info(
            "💡 Copie a pasta completa do executável para um diretório local e gere um novo build se o problema persistir."
        )
        return

    injetar_css_global()
    render_header()

    # ── Banner de dados recém-atualizados ──
    _ext_ts = st.session_state.get('ultima_extracao_ts')
    if _ext_ts:
        from datetime import datetime as _dt
        _ext_dt = _dt.fromtimestamp(_ext_ts)
        st.success(
            f"✅ Dados atualizados pela extração de "
            f"{_ext_dt.strftime('%d/%m/%Y %H:%M:%S')}",
            icon="✅",
        )

    st.title("🏭 Dashboard TC Veículos")
    st.subheader("Custo de Produção de Veículos • Real")

    # ── Sidebar Global (carrega sempre, independente de haver dados) ──
    cfg = render_sidebar_global('home')
    ano, moeda, simbolo = cfg['ano'], cfg['moeda'], cfg['simbolo']
    taxas, tipo, fator = cfg['taxas'], cfg['tipo'], cfg['fator']
    sufixo = obter_sufixo_fator(fator)

    # ── Carregar dados ──
    _perf_trace = get_flag("SCI_DEBUG_PERF_TRACE", default="false") == "true"
    _perf_log: list[tuple[str, float]] = []

    with perf_timer() as _t_principal:
        df_principal = load_principal(ano)
    if _perf_trace:
        _perf_log.append(("load_principal", _t_principal.elapsed_ms))

    with perf_timer() as _t_real:
        df_real_raw = load_principal_real(ano)
    if _perf_trace:
        _perf_log.append(("load_principal_real", _t_real.elapsed_ms))

    with perf_timer() as _t_be:
        df_be_raw = _load_forecast(ano, file_mtime=_forecast_mtime())
    if _perf_trace:
        _perf_log.append(("_load_forecast (AGG)", _t_be.elapsed_ms))

    df_vol_bud = load_volume_bud(ano)
    df_vol_actual = load_volume_actual(ano)
    df_tempo_veic = load_tempo_veiculos(ano)

    # ── Carregar dados rateados por veículo ──
    df_veic_bud_raw = load_custo_fp_veiculo(ano)
    df_veic_real_raw = load_custo_fp_veiculo_real(ano)
    # Forecast com veículo: lazy load (250K+ linhas, só usado em tabs específicas)
    _df_veic_be_loaded = [None, False]  # [dados, já_carregou]
    def _get_veic_be_raw():
        if not _df_veic_be_loaded[1]:
            _df_veic_be_loaded[0] = load_custo_fp_veiculo_forecast_fresh()
            _df_veic_be_loaded[1] = True
        return _df_veic_be_loaded[0]

    # Forecast FULL: lazy load (25K+ linhas, só quando Tab1-veículo/Tab3/Tab5 precisam)
    _df_be_full_loaded = [None, False]
    def _get_be_full():
        """Retorna forecast COMPLETO (todas as colunas). Lazy load."""
        if not _df_be_full_loaded[1]:
            _raw = _load_forecast_full(ano, file_mtime=_forecast_mtime())
            _df_be_full_loaded[0] = normalizar_periodo(_raw.copy()) if _raw is not None else None
            _df_be_full_loaded[1] = True
        return _df_be_full_loaded[0]

    if df_principal is None:
        st.info(
            f"📊 **Dados não encontrados para {ano}.**\n\n"
            "Execute o processamento na página **📥 Extração de Dados** para gerar os parquets.\n\n"
            "Após o processamento, volte aqui para ver o dashboard completo."
        )
        st.stop()

    df_principal = normalizar_periodo(df_principal)
    # Normalizar demais DataFrames uma vez (operação idempotente — evita chamadas redundantes)
    if df_real_raw is not None:
        df_real_raw = normalizar_periodo(df_real_raw)
    if df_vol_bud is not None:
        df_vol_bud = normalizar_periodo(df_vol_bud)
    if df_vol_actual is not None:
        df_vol_actual = normalizar_periodo(df_vol_actual)
    if df_veic_bud_raw is not None:
        df_veic_bud_raw = normalizar_periodo(df_veic_bud_raw)
    if df_veic_real_raw is not None:
        df_veic_real_raw = normalizar_periodo(df_veic_real_raw)
    if df_tempo_veic is not None:
        df_tempo_veic = normalizar_periodo(df_tempo_veic)

    # ── Cópias raw para filtros locais da Tab 1 ──
    _raw_df_principal = df_principal.copy()
    _raw_df_real = df_real_raw.copy() if df_real_raw is not None else None
    _raw_df_be = normalizar_periodo(df_be_raw.copy()) if df_be_raw is not None else None
    _raw_df_vol_bud = df_vol_bud.copy() if df_vol_bud is not None else None
    _raw_df_vol_actual = df_vol_actual.copy() if df_vol_actual is not None else None

    # ══════════════════════════════════════════════════════════════
    # 🔍 Sidebar de Filtros (padrão Waterfall TC Veículos)
    # ══════════════════════════════════════════════════════════════
    def _filter_opts(d, col):
        if col in d.columns:
            return ["Todos"] + sorted(d[col].dropna().astype(str).unique().tolist())
        return ["Todos"]

    _ORDEM_MESES_LC = [
        'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
        'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
    ]

    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔍 Filtros**")

    df_sb = df_principal.copy()  # base para cascata
    filtros_sel = {}

    # ── Oficina ──
    if 'filtro_oficina_home_tc' not in st.session_state:
        st.session_state.filtro_oficina_home_tc = ["Todos"]
    if 'Oficina' in df_sb.columns:
        _ofi_opts = _filter_opts(df_sb, 'Oficina')
        _ofi_def = st.session_state.filtro_oficina_home_tc if all(x in _ofi_opts for x in st.session_state.filtro_oficina_home_tc) else ["Todos"]
        _ofi_sel = st.sidebar.multiselect("Selecione a Oficina:", _ofi_opts, default=_ofi_def, key="filtro_oficina_home_tc_ms")
        st.session_state.filtro_oficina_home_tc = _ofi_sel if _ofi_sel else ["Todos"]
        if "Todos" not in _ofi_sel and _ofi_sel:
            df_sb = df_sb[df_sb['Oficina'].astype(str).isin(_ofi_sel)].copy()
        filtros_sel['oficinas'] = list(df_sb['Oficina'].dropna().unique()) if "Todos" in (_ofi_sel or ["Todos"]) else [x for x in _ofi_sel if x != "Todos"]
    else:
        filtros_sel['oficinas'] = []

    # ── Veículo (multiselect, cascateado por Oficina) ──
    if 'filtro_veiculo_home_tc' not in st.session_state:
        st.session_state.filtro_veiculo_home_tc = ["Todos"]
    if 'Veículo' in df_sb.columns:
        _veic_opts = _filter_opts(df_sb, 'Veículo')
        _veic_def = st.session_state.filtro_veiculo_home_tc if all(x in _veic_opts for x in st.session_state.filtro_veiculo_home_tc) else ["Todos"]
        _veic_sel = st.sidebar.multiselect("Selecione o Veículo:", _veic_opts, default=_veic_def, key="filtro_veiculo_home_tc_ms")
        st.session_state.filtro_veiculo_home_tc = _veic_sel if _veic_sel else ["Todos"]
        if _veic_sel and "Todos" not in _veic_sel:
            df_sb = df_sb[df_sb['Veículo'].astype(str).isin(_veic_sel)].copy()
        filtros_sel['veiculos'] = [x for x in _veic_sel if x != "Todos"] if (_veic_sel and "Todos" not in _veic_sel) else list(df_sb['Veículo'].dropna().unique())
        filtros_sel['veiculo_todos'] = not _veic_sel or "Todos" in _veic_sel
    else:
        filtros_sel['veiculos'] = []
        filtros_sel['veiculo_todos'] = True

    # ── USI ──
    if 'USI' in df_sb.columns:
        if 'filtro_usi_home_tc' not in st.session_state:
            _usi_init = _filter_opts(df_sb, 'USI')
            st.session_state.filtro_usi_home_tc = ["TC Ext"] if "TC Ext" in _usi_init else ["Todos"]
        _usi_opts = _filter_opts(df_sb, 'USI')
        _usi_def = st.session_state.filtro_usi_home_tc if all(x in _usi_opts for x in st.session_state.filtro_usi_home_tc) else (["TC Ext"] if "TC Ext" in _usi_opts else ["Todos"])
        _usi_sel = st.sidebar.multiselect("Selecione a USI:", _usi_opts, default=_usi_def, key="filtro_usi_home_tc_ms")
        st.session_state.filtro_usi_home_tc = _usi_sel if _usi_sel else ["Todos"]
        if _usi_sel and "Todos" not in _usi_sel:
            df_sb = df_sb[df_sb['USI'].astype(str).isin(_usi_sel)].copy()

    # ── Período ──
    if 'Período' in df_sb.columns:
        _per_raw = _filter_opts(df_sb, 'Período')
        _meses_ord = sorted(
            [p for p in _per_raw[1:] if str(p).lower() in _ORDEM_MESES_LC],
            key=lambda x: _ORDEM_MESES_LC.index(str(x).lower()) if str(x).lower() in _ORDEM_MESES_LC else 999
        )
        _outros_per = [p for p in _per_raw[1:] if str(p).lower() not in _ORDEM_MESES_LC]
        _per_opcoes = ["Todos"] + _meses_ord + _outros_per

        if 'filtro_periodo_home_tc' not in st.session_state:
            st.session_state.filtro_periodo_home_tc = "Todos"
        _per_def = st.session_state.filtro_periodo_home_tc if st.session_state.filtro_periodo_home_tc in _per_opcoes else "Todos"
        _per_idx = _per_opcoes.index(_per_def) if _per_def in _per_opcoes else 0
        _per_sel = st.sidebar.selectbox("Selecione o Período:", _per_opcoes, index=_per_idx, key="filtro_periodo_home_tc_sb")
        st.session_state.filtro_periodo_home_tc = _per_sel
        if _per_sel != "Todos":
            df_sb = df_sb[df_sb['Período'].astype(str) == str(_per_sel)].copy()
            filtros_sel['periodos'] = [_per_sel]
        else:
            filtros_sel['periodos'] = list(df_sb['Período'].dropna().unique())

    # ── Centro cst ──
    if 'Centrocst' in df_sb.columns:
        if 'filtro_centro_cst_home_tc' not in st.session_state:
            st.session_state.filtro_centro_cst_home_tc = "Todos"
        _cc_opts = _filter_opts(df_sb, 'Centrocst')
        _cc_def = st.session_state.filtro_centro_cst_home_tc if st.session_state.filtro_centro_cst_home_tc in _cc_opts else "Todos"
        _cc_idx = _cc_opts.index(_cc_def) if _cc_def in _cc_opts else 0
        _cc_sel = st.sidebar.selectbox("Selecione o Centro cst:", _cc_opts, index=_cc_idx, key="filtro_centro_cst_home_tc_sb")
        st.session_state.filtro_centro_cst_home_tc = _cc_sel
        if _cc_sel != "Todos":
            df_sb = df_sb[df_sb['Centrocst'].astype(str) == str(_cc_sel)].copy()

    # ── Conta contábil ──
    if 'Nºconta' in df_sb.columns:
        if 'filtro_conta_contabil_home_tc' not in st.session_state:
            st.session_state.filtro_conta_contabil_home_tc = []
        _nc_opts = _filter_opts(df_sb, 'Nºconta')[1:]
        _nc_def = [x for x in st.session_state.filtro_conta_contabil_home_tc if x in _nc_opts]
        _nc_sel = st.sidebar.multiselect("Selecione a Conta contábil:", _nc_opts, default=_nc_def, key="filtro_conta_contabil_home_tc_ms")
        st.session_state.filtro_conta_contabil_home_tc = _nc_sel
        if _nc_sel:
            df_sb = df_sb[df_sb['Nºconta'].astype(str).isin(_nc_sel)].copy()

    # ── Filtros principais dinâmicos ──
    _filtros_princ = [
        ("Type 05", "Type 05", "multiselect"),
        ("Type 06", "Type 06", "multiselect"),
        ("Type 07", "Type 07", "multiselect"),
        ("Account", "Account", "multiselect"),
        ("Custo", "Custo", "multiselect"),
        ("Fornecedor", "Fornecedor", "multiselect"),
        ("Fornec.", "Fornec.", "multiselect"),
        ("Tipo", "Tipo", "multiselect"),
    ]
    for _col_n, _lbl, _ in _filtros_princ:
        if _col_n in df_sb.columns:
            _fk = f'filtro_{_col_n}_home_tc'
            if _fk not in st.session_state:
                st.session_state[_fk] = ["Todos"]
            _fp_opts = _filter_opts(df_sb, _col_n)
            _fp_def = st.session_state[_fk] if all(x in _fp_opts for x in st.session_state[_fk]) else ["Todos"]
            _fp_sel = st.sidebar.multiselect(f"Selecione o {_lbl}:", _fp_opts, default=_fp_def, key=f"{_fk}_ms")
            st.session_state[_fk] = _fp_sel if _fp_sel else ["Todos"]
            if _fp_sel and "Todos" not in _fp_sel:
                df_sb = df_sb[df_sb[_col_n].astype(str).isin(_fp_sel)].copy()
            # Propagar Custo para filtros_sel
            if _col_n == 'Custo' and 'Custo' in df_sb.columns:
                filtros_sel['custos'] = list(df_sb['Custo'].dropna().unique()) if "Todos" in (_fp_sel or ["Todos"]) else [x for x in _fp_sel if x != "Todos"]

    # ── Filtros avançados ──
    with st.sidebar.expander("🔍 Filtros Avançados"):
        _filtros_avanc = [
            ("Usuário", "Usuário", "multiselect"),
            ("Material", "Material", "multiselect"),
            ("Dt.lçto.", "Data Lançamento", "multiselect"),
            ("Texto breve", "Texto breve", "multiselect"),
        ]
        for _col_n, _lbl, _ in _filtros_avanc:
            if _col_n in df_sb.columns:
                _fa_opts = _filter_opts(df_sb, _col_n)
                if len(_fa_opts) > 101:
                    _fa_opts = _fa_opts[:101]
                    st.caption(f"⚠️ {_lbl}: Limitado a 100 opções para performance")
                _fk = f'filtro_avancado_{_col_n}_home_tc'
                if _fk not in st.session_state:
                    st.session_state[_fk] = ["Todos"]
                _fa_def = st.session_state[_fk] if all(x in _fa_opts for x in st.session_state[_fk]) else ["Todos"]
                _fa_sel = st.multiselect(f"Selecione o {_lbl}:", _fa_opts, default=_fa_def, key=f"{_fk}_ms")
                st.session_state[_fk] = _fa_sel if _fa_sel else ["Todos"]
                if _fa_sel and "Todos" not in _fa_sel:
                    df_sb = df_sb[df_sb[_col_n].astype(str).isin(_fa_sel)].copy()

    # ── Determinar se usa dados rateados por veículo ──
    usar_rateado = not filtros_sel.get('veiculo_todos', True)

    if usar_rateado and df_veic_bud_raw is not None:
        # Dados rateados por veículo — BUD
        _df_base_bud = df_veic_bud_raw.copy()
        if 'Custo FP Veiculo' in _df_base_bud.columns:
            _df_base_bud['Custo FP'] = _df_base_bud['Custo FP Veiculo']
        df = aplicar_filtros(_df_base_bud, filtros_sel)

        # Dados rateados por veículo — Real
        df_real = None
        if df_veic_real_raw is not None:
            _df_base_real = df_veic_real_raw.copy()
            if 'Custo FP Veiculo' in _df_base_real.columns:
                _df_base_real['Custo FP'] = _df_base_real['Custo FP Veiculo']
            _df_real_filt = aplicar_filtros(_df_base_real, filtros_sel)
            if not _df_real_filt.empty:
                df_real = _df_real_filt

        # Volumes filtrados pelo veículo selecionado
        veiculos_sel = filtros_sel.get('veiculos', [])
        if df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
            df_vol_bud = df_vol_bud.copy()
            df_vol_bud = df_vol_bud[df_vol_bud['Veículo'].isin(veiculos_sel)]
        if df_vol_actual is not None and 'Veículo' in df_vol_actual.columns:
            df_vol_actual = df_vol_actual.copy()
            df_vol_actual = df_vol_actual[df_vol_actual['Veículo'].isin(veiculos_sel)]
    else:
        # Dados consolidados (principal)
        df = aplicar_filtros(df_principal, filtros_sel)
        df_real = None
        if df_real_raw is not None:
            df_real_temp = df_real_raw.copy()
            df_real_temp = aplicar_filtros(df_real_temp, filtros_sel)
            if not df_real_temp.empty:
                df_real = df_real_temp

    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        st.stop()

    # ── Aplicar fator e moeda ──
    cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]
    df = aplicar_fator_df(df, cols_val, fator)
    df = converter_moeda_df(df, cols_val, moeda, taxas)

    if df_real is not None:
        cols_val_real = [c for c in COLUNAS_MONETARIAS if c in df_real.columns]
        df_real = aplicar_fator_df(df_real, cols_val_real, fator)
        df_real = converter_moeda_df(df_real, cols_val_real, moeda, taxas)

    # ── Budget Flex (calculado com dados filtrados) ──
    tem_ano_df = 'Ano' in df.columns
    df_flex = calcular_flex_budget(df, df_vol_bud, df_vol_actual, tem_ano=tem_ano_df)
    # IMPORTANTE: NÃO aplicar fator/moeda aqui - df já tem fator/moeda aplicados,
    # então Flex_Bud calculado a partir dele já está na escala correta

    # ── Flex detalhado (com dimensões Oficina/Type05/06/Account/Custo) ──
    df_flex_det = calcular_flex_budget_detalhado(
        df, df_vol_bud, df_vol_actual,
        col_custo='Custo FP', tem_ano=tem_ano_df,
    )
    # NOTA: df_flex_det já herda a escala (fator/moeda) do df de entrada,
    # pois Flex_Bud = (Custo / Vol_Bud) * Vol_Actual usa valores já convertidos.
    # NÃO reaplicar fator/moeda aqui para evitar dupla conversão.

    # ── df_bud = Budget, df = Real (ou Budget se sem Real) ──
    df_bud = df.copy()
    tem_real = df_real is not None
    if tem_real:
        df = df_real  # A partir daqui, df = Real para todas exibições

    # ════════════════════════════════════════
    #  MÉTRICAS RESUMO
    # ════════════════════════════════════════
    label_valor = 'CPU' if tipo == 'CPU (Custo por Unidade)' else 'Custo'
    vol_total = df_vol_bud['Volume'].sum() if df_vol_bud is not None else 0

    if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
        soma = {c: df[c].sum() / vol_total for c in cols_val if c in df.columns}
    else:
        soma = {c: df[c].sum() for c in cols_val if c in df.columns}

    # Redis vem de linhas originadas da aba massa-REDIS (marcadas com _fonte_redis), não de coluna separada
    redis_total = extrair_redis(df)
    if tipo == 'CPU (Custo por Unidade)' and vol_total > 0:
        redis_val = redis_total / vol_total
    else:
        redis_val = redis_total

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(f"📦 {label_valor} Desp. Primária", f"{simbolo} {soma.get('Despesa Primaria', 0):,.2f}{sufixo}")
    c2.metric(f"🏭 {label_valor} FA", f"{simbolo} {soma.get('Custo FA', 0):,.2f}{sufixo}")
    c3.metric("💰 Redis", f"{simbolo} {redis_val:,.2f}{sufixo}")
    c4.metric(f"🚗 {label_valor} FP", f"{simbolo} {soma.get('Custo FP', 0):,.2f}{sufixo}")
    c5.metric("📉 D&A Dedicada", f"{simbolo} {soma.get('D&A dedicado', 0):,.2f}{sufixo}")
    c6.metric("✅ FP sem Dedicada", f"{simbolo} {soma.get('FP sem Dedicada', 0):,.2f}{sufixo}")

    # ── Perf trace (visível apenas com SCI_DEBUG_PERF_TRACE=true) ──
    if _perf_trace and _perf_log:
        with st.expander("⏱️ Perf Trace — Early Load", expanded=False):
            for _lbl, _ms in _perf_log:
                st.text(f"{_lbl}: {_ms:.1f} ms")
            _total = sum(m for _, m in _perf_log)
            st.text(f"TOTAL: {_total:.1f} ms")

    # ════════════════════════════════════════
    #  TABS
    # ════════════════════════════════════════
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🚗 TC Veículos", "📈 Volume",
        "📉 Análise Flex",
        "🚗 Tempo de Produção", "📋 Dados Detalhados",
    ])

    # ── TAB 1: TC Veículos ──
    # @st.fragment isola variáveis: tab1 NÃO modifica escopo externo

    # ══════════════════════════════════════════════════════════════
    #  CONTEXTO COMPARTILHADO PARA MÓDULOS DE ABAS
    # ══════════════════════════════════════════════════════════════
    from tc_principal.pages._home_tabs import HomeContext
    ctx = HomeContext(
        ano=ano,
        moeda=moeda,
        simbolo=simbolo,
        taxas=taxas,
        tipo=tipo,
        fator=fator,
        sufixo=sufixo,
        label_valor=label_valor,
        cols_val=cols_val,
        vol_total=vol_total,
        df=df,
        df_bud=df_bud,
        df_real=df_real,
        df_principal=df_principal,
        tem_real=tem_real,
        tem_ano_df=tem_ano_df,
        df_flex=df_flex,
        df_flex_det=df_flex_det,
        df_vol_bud=df_vol_bud,
        df_vol_actual=df_vol_actual,
        df_veic_bud_raw=df_veic_bud_raw,
        df_veic_real_raw=df_veic_real_raw,
        df_tempo_veic=df_tempo_veic,
        _raw_df_principal=_raw_df_principal,
        _raw_df_real=_raw_df_real,
        _raw_df_be=_raw_df_be,
        _raw_df_vol_bud=_raw_df_vol_bud,
        _raw_df_vol_actual=_raw_df_vol_actual,
        _get_veic_be_raw=_get_veic_be_raw,
        _get_be_full=_get_be_full,
        usar_rateado=usar_rateado,
        filtros_sel=filtros_sel,
    )

    with tab1:
        @st.fragment
        def _render_tc_veiculos():
            _tab1.render(ctx)
        _render_tc_veiculos()

    with tab2:
        @st.fragment
        def _render_volume():
            _tab2.render(ctx)
        _render_volume()

    with tab3:
        @st.fragment
        def _render_analise_flex():
            _tab3.render(ctx)
        _render_analise_flex()

    with tab4:
        @st.fragment
        def _render_tempo_producao():
            _tab4.render(ctx)
        _render_tempo_producao()

    with tab5:
        @st.fragment
        def _render_dados_detalhados():
            _tab5.render(ctx)
        _render_dados_detalhados()


    st.divider()

    # Rodapé padrão TC Ext
    mes_rodape = meses_pt.get(datetime.now().month, '')
    ano_rodape = datetime.now().year
    versao_rodape = '1.91'
    try:
        with open('versao.json', 'r', encoding='utf-8') as f:
            versao_rodape = json.load(f).get('versao', '1.91')
    except Exception:
        pass
    st.markdown(f"""
    <div style='text-align: center; color: #666; padding: 20px;'>
        📚 Stellantis Cost Intelligence (SCI) | Versão {versao_rodape} | {mes_rodape} {ano_rodape}
        <br>
        <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    render()
