"""Tab 1: TC Veículos — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    COLUNAS_BE_DETALHADO, COLUNAS_BE_DETALHADO_VEICULO,
    reordenar_colunas_be, download_excel_button,
    load_principal, load_principal_real,
    load_volume_bud, load_volume_actual,
    load_custo_fp_veiculo, load_custo_fp_veiculo_real,
    load_custo_fp_veiculo_forecast_fresh,
    load_percentual_rateio_veiculos_real, ratear_be_por_veiculo,
    load_dea_dedicado_real,
    normalizar_periodo, ordenar_por_mes,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    aplicar_fator_df, converter_moeda_df, obter_sufixo_fator, calcular_cpu,
    build_cpu_tooltip_payload, build_delta_tooltip_payload,
    extrair_redis,
    _pivotar_detalhado, _pivotar_flex, render_secao_tabela_detalhe,
    _render_tabela_fmt,
)
from tc_principal.ui_components import (
    aplicar_filtros, criar_tabela_html, render_kpi, render_kpi_spacer,
    formatar_ratio_com_barra, criar_tabela_html_flex, render_inline_summary_metrics,
)
from tc_principal.pages._home_tabs.chart_utils import create_periodo_chart
from tc_principal.pages._home_tabs.data_helpers import _carregar_rateios_manuais, _DATA_ROOT

try:
    from processamento_dados_veiculos import executar_conferencias
except ImportError:
    executar_conferencias = None


def render(ctx):
    """Renderiza a aba TC Veículos."""
    # ── Desempacotar contexto ──
    ano = ctx.ano
    moeda = ctx.moeda
    simbolo = ctx.simbolo
    taxas = ctx.taxas
    tipo = ctx.tipo
    fator = ctx.fator
    sufixo = ctx.sufixo
    label_valor = ctx.label_valor
    cols_val = ctx.cols_val
    vol_total = ctx.vol_total
    df = ctx.df
    df_bud = ctx.df_bud
    df_real = ctx.df_real
    df_principal = ctx.df_principal
    tem_real = ctx.tem_real
    df_flex = ctx.df_flex
    df_flex_det = ctx.df_flex_det
    df_vol_bud = ctx.df_vol_bud
    df_vol_actual = ctx.df_vol_actual
    df_veic_bud_raw = ctx.df_veic_bud_raw
    df_veic_real_raw = ctx.df_veic_real_raw
    _raw_df_principal = ctx._raw_df_principal
    _raw_df_real = ctx._raw_df_real
    _raw_df_be = ctx._raw_df_be
    _raw_df_vol_bud = ctx._raw_df_vol_bud
    _raw_df_vol_actual = ctx._raw_df_vol_actual
    _get_veic_be_raw = ctx._get_veic_be_raw
    _get_be_full = ctx._get_be_full
    tem_ano_df = ctx.tem_ano_df
    usar_rateado = ctx.usar_rateado
    filtros_sel = ctx.filtros_sel

    st.markdown("---")

    _fonte_dados_t1 = st.radio(
        "📊 Fonte de Dados",
        ["Real", "BE (Simulado)"],
        index=0,
        horizontal=True,
        key="t1_fonte_dados",
    )
    _usar_be_t1 = _fonte_dados_t1 == "BE (Simulado)"
    if _usar_be_t1 and (_raw_df_be is None or _raw_df_be.empty):
        st.warning(
            "⚠️ **Forecast (Best Estimate) ainda não foi gerado.**\n\n"
            "Para gerar o forecast:\n"
            "1. Acesse a página **🔮 Best Estimate — Simulador**\n"
            "2. Configure os parâmetros (períodos, sensibilidade, inflação)\n"
            "3. Clique em **✅ Aplicar Configurações do Best Estimate**\n"
            "4. Aguarde o processamento e volte a esta página\n\n"
            "Exibindo dados **Reais** como fallback."
        )
        try:
            st.page_link(
                "pages/2_Best_Estimate.py",
                label="🔮 Ir para o Simulador BE",
                icon="🔮",
            )
        except Exception:
            pass

    # ════════════════════════════════════════
    # 🔍 Filtros da Aba (Oficina + Veículo)
    # ════════════════════════════════════════
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        _oficinas_all = sorted(
            _raw_df_principal['Oficina'].dropna().unique()
        ) if 'Oficina' in _raw_df_principal.columns else []
        _sel_ofi_t1 = st.multiselect(
            "🏭 Oficina", ["Todos"] + _oficinas_all,
            default=["Todos"], key="t1_oficina"
        )
        _ofi_t1 = (
            _oficinas_all if "Todos" in _sel_ofi_t1
            else [x for x in _sel_ofi_t1 if x != "Todos"]
        )
    with col_f2:
        # Veículos do arquivo rateado (df_veic_bud_raw), pois df_principal não tem coluna Veículo
        _df_veic_src = None
        if df_veic_bud_raw is not None:
            _df_veic_src = df_veic_bud_raw.copy()
            # Filtrar por oficinas selecionadas (cascata)
            if _ofi_t1 and 'Oficina' in _df_veic_src.columns:
                _df_veic_src = _df_veic_src[_df_veic_src['Oficina'].isin(_ofi_t1)]
        _veiculos_all = sorted(
            _df_veic_src['Veículo'].dropna().unique()
        ) if _df_veic_src is not None and 'Veículo' in _df_veic_src.columns else []
        _sel_veic_t1 = st.selectbox(
            "🚗 Veículo", ["Todos"] + _veiculos_all,
            index=0, key="t1_veiculo"
        )

    # Períodos: usar todos disponíveis (filtro de período fica na seção Análise Flex)
    _periodos_all = [
        m for m in ORDEM_MESES
        if m in _raw_df_principal['Período'].unique()
    ]
    _per_t1 = _periodos_all

    # ── Reconstruir dados locais com filtros da aba ──
    _filtros_t1 = {
        'oficinas': _ofi_t1,
        'periodos': _per_t1,
    }
    # Só incluir veiculos no filtro quando um veículo específico for selecionado
    # (df_principal não tem coluna Veículo; apenas os dados rateados têm)
    if _sel_veic_t1 != "Todos":
        _filtros_t1['veiculos'] = [_sel_veic_t1]
    _usar_rateado_t1 = _sel_veic_t1 != "Todos"
    _be_t1 = None

    if _usar_rateado_t1 and df_veic_bud_raw is not None:
        _bud_t1 = df_veic_bud_raw.copy()
        if 'Custo FP Veiculo' in _bud_t1.columns:
            _bud_t1['Custo FP'] = _bud_t1['Custo FP Veiculo']
        df_bud = aplicar_filtros(_bud_t1, _filtros_t1)

        _real_t1 = None
        if df_veic_real_raw is not None:
            _r_t1 = df_veic_real_raw.copy()
            if 'Custo FP Veiculo' in _r_t1.columns:
                _r_t1['Custo FP'] = _r_t1['Custo FP Veiculo']
            _rt = aplicar_filtros(_r_t1, _filtros_t1)
            if not _rt.empty:
                _real_t1 = _rt

        if _raw_df_be is not None:
            _filtros_be_t1 = {'oficinas': _ofi_t1, 'periodos': _per_t1}
            # Para rateio por veículo, precisamos FULL (AGG não tem Veículo)
            _be_full_t1_tmp = _get_be_full()
            _be_full_t1 = _be_full_t1_tmp if _be_full_t1_tmp is not None else _raw_df_be

            # PRIORIDADE: Usar arquivo pré-gerado
            _veic_be_raw = _get_veic_be_raw()
            if _veic_be_raw is not None and not _veic_be_raw.empty:
                _be_veic_raw_t1 = normalizar_periodo(_veic_be_raw.copy())
                if 'Ano' in _be_veic_raw_t1.columns:
                    _be_veic_raw_t1 = _be_veic_raw_t1[_be_veic_raw_t1['Ano'] == int(ano)].copy()
                if 'Custo FP Veiculo' in _be_veic_raw_t1.columns:
                    _be_veic_raw_t1['Custo FP'] = _be_veic_raw_t1['Custo FP Veiculo']
                _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                _rt_be = aplicar_filtros(_be_veic_raw_t1, _filtros_be_t1)
                if not _rt_be.empty:
                    _be_t1 = _rt_be
            else:
                # Fallback: verificar se dados originais têm veículo
                _be_tem_veiculo = (
                    'Veículo' in _be_full_t1.columns
                    and _be_full_t1['Veículo'].notna().any()
                    and _sel_veic_t1 in _be_full_t1['Veículo'].values
                )
                if _be_tem_veiculo and _sel_veic_t1 != 'Todos':
                    _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                    _rt_be = aplicar_filtros(_be_full_t1, _filtros_be_t1)
                else:
                    # Último fallback: ratear em runtime (mesma lógica do Real)
                    _pct_be_t1 = load_percentual_rateio_veiculos_real(ano)
                    _dea_be_t1 = load_dea_dedicado_real(ano)
                    _be_rateado_t1 = ratear_be_por_veiculo(
                        _be_full_t1, _pct_be_t1, df_dea=_dea_be_t1
                    )
                    if (
                        _be_rateado_t1 is not None
                        and 'Veículo' in _be_rateado_t1.columns
                    ):
                        if 'Custo FP Veiculo' in _be_rateado_t1.columns:
                            _be_rateado_t1['Custo FP'] = (
                                _be_rateado_t1['Custo FP Veiculo']
                            )
                        _filtros_be_t1['veiculos'] = [_sel_veic_t1]
                        _rt_be = aplicar_filtros(
                            _be_rateado_t1, _filtros_be_t1
                        )
                    else:
                        _rt_be = aplicar_filtros(_be_full_t1, _filtros_be_t1)
                if not _rt_be.empty:
                    _be_t1 = _rt_be

        # ── Garantir meses históricos no BE = Real (zero diferença) ──
        # O forecast pode ter sido gerado com código antigo; para meses
        # que já possuem Real, substituímos diretamente pelo dado Real.
        if (
            _be_t1 is not None
            and _real_t1 is not None
            and 'Tipo' in _be_t1.columns
        ):
            _hist_per = _be_t1.loc[
                _be_t1['Tipo'] == 'Histórico', 'Período'
            ].unique()
            if len(_hist_per) > 0:
                _be_fc_only = _be_t1[
                    _be_t1['Tipo'] != 'Histórico'
                ].copy()
                _real_as_hist = _real_t1[
                    _real_t1['Período'].isin(_hist_per)
                ].copy()
                _real_as_hist['Tipo'] = 'Histórico'
                _be_t1 = pd.concat(
                    [_real_as_hist, _be_fc_only],
                    ignore_index=True,
                )

        df_vol_bud = _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
        if df_vol_bud is not None and 'Veículo' in df_vol_bud.columns:
            df_vol_bud = df_vol_bud[df_vol_bud['Veículo'] == _sel_veic_t1]
        df_vol_actual = _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None
        if df_vol_actual is not None and 'Veículo' in df_vol_actual.columns:
            df_vol_actual = df_vol_actual[df_vol_actual['Veículo'] == _sel_veic_t1]
    else:
        df_bud = aplicar_filtros(_raw_df_principal, _filtros_t1)
        _real_t1 = None
        if _raw_df_real is not None:
            _rt = aplicar_filtros(_raw_df_real, _filtros_t1)
            if not _rt.empty:
                _real_t1 = _rt
        if _raw_df_be is not None:
            _rt_be = aplicar_filtros(_raw_df_be, _filtros_t1)
            if not _rt_be.empty:
                _be_t1 = _rt_be
        # ── Garantir meses históricos no BE = Real ──
        if (
            _be_t1 is not None
            and _real_t1 is not None
            and 'Tipo' in _be_t1.columns
        ):
            _hist_per = _be_t1.loc[
                _be_t1['Tipo'] == 'Histórico', 'Período'
            ].unique()
            if len(_hist_per) > 0:
                _be_fc_only = _be_t1[
                    _be_t1['Tipo'] != 'Histórico'
                ].copy()
                _real_as_hist = _real_t1[
                    _real_t1['Período'].isin(_hist_per)
                ].copy()
                _real_as_hist['Tipo'] = 'Histórico'
                _be_t1 = pd.concat(
                    [_real_as_hist, _be_fc_only],
                    ignore_index=True,
                )
        df_vol_bud = _raw_df_vol_bud.copy() if _raw_df_vol_bud is not None else None
        df_vol_actual = _raw_df_vol_actual.copy() if _raw_df_vol_actual is not None else None

    if df_bud.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        df = df_bud.copy()
        df_flex = None
        vol_total = 0
        cols_val = []
        tem_real = False
    else:
        # Aplicar fator e moeda aos dados locais
        cols_val = [c for c in COLUNAS_MONETARIAS if c in df_bud.columns]
        df_bud = aplicar_fator_df(df_bud, cols_val, fator)
        df_bud = converter_moeda_df(df_bud, cols_val, moeda, taxas)

        if _real_t1 is not None:
            _cv_t1 = [c for c in COLUNAS_MONETARIAS if c in _real_t1.columns]
            _real_t1 = aplicar_fator_df(_real_t1, _cv_t1, fator)
            _real_t1 = converter_moeda_df(_real_t1, _cv_t1, moeda, taxas)

        if _be_t1 is not None:
            _cv_be_t1 = [c for c in COLUNAS_MONETARIAS if c in _be_t1.columns]
            _be_t1 = aplicar_fator_df(_be_t1, _cv_be_t1, fator)
            _be_t1 = converter_moeda_df(_be_t1, _cv_be_t1, moeda, taxas)

        tem_real = _real_t1 is not None
        tem_be_t1 = _be_t1 is not None
        if _usar_be_t1:
            if tem_be_t1:
                df = _be_t1
            elif tem_real:
                df = _real_t1.copy()
            else:
                df = df_bud.copy()
        else:
            df = _real_t1 if tem_real else df_bud.copy()

        _tem_ano_t1 = 'Ano' in df.columns
        df_flex = calcular_flex_budget(
            df_bud, df_vol_bud, df_vol_actual, tem_ano=_tem_ano_t1
        )
        # IMPORTANTE: NÃO aplicar fator/moeda - já aplicado em df_bud

        vol_total = (
            df_vol_bud['Volume'].sum()
            if df_vol_bud is not None and 'Volume' in df_vol_bud.columns
            else 0
        )
        cols_val = [c for c in COLUNAS_MONETARIAS if c in df.columns]

    st.markdown("---")

    # ════════════════════════════════════════
    # 📊 Resumo TC Veículos (KPIs dentro da tab)
    # ════════════════════════════════════════
    st.subheader(
        "📊 Resumo Best Estimate"
        if _usar_be_t1 else
        "📊 Resumo TC Veículos"
    )

    # Calcular BUD e Flex BUD usando dados do Budget (já filtrados pela sidebar)
    df_resumo_bud = df_bud.copy()
    if 'Custo' in df_resumo_bud.columns:
        df_resumo_bud['Custo_str'] = df_resumo_bud['Custo'].astype(str).str.lower()
    else:
        df_resumo_bud['Custo_str'] = 'variável'
    df_resumo_bud['Categoria'] = np.where(
        df_resumo_bud['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
    )

    # Calcular totais por categoria (Budget)
    _col_fp = 'Custo FP' if 'Custo FP' in df_resumo_bud.columns else None
    bud_fixo = df_resumo_bud[df_resumo_bud['Categoria'] == 'Fixo'][_col_fp].sum() if _col_fp else 0
    bud_variavel = df_resumo_bud[df_resumo_bud['Categoria'] == 'Variável'][_col_fp].sum() if _col_fp else 0
    bud_total = bud_fixo + bud_variavel

    # Calcular proporção global de volume
    if df_vol_bud is not None and df_vol_actual is not None:
        vol_budget_total = df_vol_bud['Volume'].sum()
        vol_actual_total = df_vol_actual['Volume'].sum()
        proporcao_global_tc = (vol_actual_total / vol_budget_total) if vol_budget_total > 0 else 1
    else:
        vol_budget_total = 0
        vol_actual_total = 0
        proporcao_global_tc = 1

    # Calcular Flex BUD: Fixo + (Variável × Proporção Global)
    flex_bud_total = bud_fixo + (bud_variavel * proporcao_global_tc)

    # Total Real
    total_custo = df['Custo FP'].sum() if 'Custo FP' in df.columns else 0

    # Aplicar CPU se necessário
    if tipo == 'CPU (Custo por Unidade)' and vol_actual_total > 0:
        bud_exibir = bud_total / vol_actual_total
        flex_exibir = flex_bud_total / vol_actual_total
        total_exibir = total_custo / vol_actual_total
    else:
        bud_exibir = bud_total
        flex_exibir = flex_bud_total
        total_exibir = total_custo

    flex_menos_bud = flex_exibir - bud_exibir
    total_menos_flex = total_exibir - flex_exibir
    total_div_flex = (
        (total_exibir / flex_exibir) if flex_exibir != 0 else 0
    )

    def _fmt_val(v):
        return f"{simbolo} {v:,.2f}{sufixo}"

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        render_kpi("BUD", _fmt_val(bud_exibir))
    with k2:
        render_kpi("Flex Bud - BUD", _fmt_val(flex_menos_bud))
    with k3:
        render_kpi("Flex BUD", _fmt_val(flex_exibir))
    with k4:
        render_kpi(
            "BE - Flex Bud" if _usar_be_t1 else "Real - Flex Bud",
            _fmt_val(total_menos_flex)
        )
    with k5:
        render_kpi(
            "Best Estimate" if _usar_be_t1 else "Real",
            _fmt_val(total_exibir)
        )
    with k6:
        render_kpi(
            "BE / Flex Bud" if _usar_be_t1 else "Real / Flex Bud",
            f"{total_div_flex:.0%}"
        )

    render_kpi_spacer()

    # Alerta sobre volumes iguais
    volumes_iguais = abs(vol_budget_total - vol_actual_total) < 1
    if volumes_iguais:
        st.warning(
            f"⚠️ **Volume Budget ({vol_budget_total:,.0f}) = "
            f"Volume Realizado ({vol_actual_total:,.0f})**  \n"
            f"Proporção = {proporcao_global_tc:.2%} → Flex BUD = BUD.  \n"
            "Verifique os dados de volume na aba **📈 Volume**."
        )

    st.divider()

    # ════════════════════════════════════════
    # Gráfico: Custo FP por Período + Série selecionada + Linha Flex BUD
    # ════════════════════════════════════════
    st.markdown(
        "### Custo FP por Período — Best Estimate"
        if _usar_be_t1 else
        "### Custo FP por Período — Real"
    )

    # Detectar se há coluna Ano (padrão TC Ext)
    tem_ano = 'Ano' in df.columns

    # ── Barras = série selecionada (Real ou BE) ──
    df_periodo = None
    _col_tipo_graf = None
    if 'Custo FP' in df.columns:
        df_graf = df.copy()
        cols_val_graf = [c for c in COLUNAS_MONETARIAS if c in df_graf.columns]
        _grp_cols_per = ['Período']
        if tem_ano and 'Ano' in df_graf.columns:
            _grp_cols_per = ['Ano', 'Período']
        # No modo BE, incluir Tipo no agrupamento para cores Histórico/BE
        if _usar_be_t1 and 'Tipo' in df_graf.columns:
            _grp_cols_per = _grp_cols_per + ['Tipo']
            _col_tipo_graf = 'Tipo'
        df_periodo = df_graf.groupby(_grp_cols_per, as_index=False).agg({
            c: 'sum' for c in cols_val_graf
        })
        df_periodo = ordenar_por_mes(df_periodo)
        df_periodo['Período'] = df_periodo['Período'].astype(str)
        if tem_ano and 'Ano' in df_periodo.columns:
            df_periodo['Ano'] = df_periodo['Ano'].astype(str)

        # Aplicar CPU à série selecionada se necessário
        if tipo == 'CPU (Custo por Unidade)' and df_vol_actual is not None:
            vol_act_norm = df_vol_actual.copy()
            cols_agrup_vol = ['Ano', 'Período'] if tem_ano and 'Ano' in vol_act_norm.columns else ['Período']
            vol_per = vol_act_norm.groupby(cols_agrup_vol, as_index=False)['Volume'].sum()
            vol_per['Período'] = vol_per['Período'].astype(str)
            if tem_ano and 'Ano' in vol_per.columns:
                vol_per['Ano'] = vol_per['Ano'].astype(str)
            df_periodo = df_periodo.merge(vol_per, on=cols_agrup_vol, how='left')
            df_periodo['Volume'] = df_periodo['Volume'].fillna(0)
            if df_periodo['Volume'].sum() > 0:
                for c in cols_val_graf:
                    if c in df_periodo.columns:
                        df_periodo[c] = calcular_cpu(
                            df_periodo[c], df_periodo['Volume']
                        )

    # Ordenação cronológica usando lista filtrada de ORDEM_MESES
    if df_periodo is None or len(df_periodo) == 0 or 'Custo FP' not in df_periodo.columns:
        st.info(
            "ℹ️ Nenhum dado de Best Estimate disponível para exibir no gráfico."
            if _usar_be_t1 else
            "ℹ️ Nenhum dado de Realizado disponível para exibir no gráfico."
        )
    else:
        # Criar lista de ordem de períodos
        if tem_ano and 'Período_Completo' not in df_periodo.columns:
            # Criar Período_Completo temporariamente só para ordenação
            df_periodo['Período_Completo'] = df_periodo['Período'] + ' ' + df_periodo['Ano']

        periodos_presentes = df_periodo['Período'].unique().tolist()
        ordem_per = [m for m in ORDEM_MESES if m in periodos_presentes]

        # Se tem ano, precisamos criar ordem com Período_Completo
        if tem_ano:
            ordem_per = df_periodo['Período_Completo'].tolist()

        # ── Pré-computar tooltip rico (Type 05 → Type 06) ──
        _hover_bar = None
        _hover_bud = None
        _is_cpu = tipo == 'CPU (Custo por Unidade)'
        _serie_lbl = 'BE' if _usar_be_t1 else 'Real'
        try:
            if 'Type 05' in df.columns:
                # Quando tem_ano, o eixo X usa "Mês Ano" (ex: Janeiro 2026).
                # Precisamos gerar as chaves do tooltip no mesmo formato.
                if tem_ano and 'Ano' in df.columns:
                    _grp_col_tt = '_PeriodoCompleto'
                    df[_grp_col_tt] = df['Período'].astype(str).str.strip() + ' ' + df['Ano'].astype(str).str.strip()
                    _periodos_tt = ordem_per  # já no formato "Janeiro 2026"
                else:
                    _grp_col_tt = 'Período'
                    _periodos_tt = periodos_presentes

                # Volume por período (para exibir no tooltip)
                _vol_dict: dict[str, float] = {}
                if (df_vol_actual is not None
                        and 'Volume' in df_vol_actual.columns
                        and 'Período' in df_vol_actual.columns):
                    _tmp_v = df_vol_actual.copy()
                    if tem_ano and 'Ano' in _tmp_v.columns:
                        _tmp_v['_key'] = _tmp_v['Período'].astype(str).str.strip() + ' ' + _tmp_v['Ano'].astype(str).str.strip()
                    else:
                        _tmp_v['_key'] = _tmp_v['Período'].astype(str).str.strip()
                    _tmp_v['Volume'] = pd.to_numeric(_tmp_v['Volume'], errors='coerce').fillna(0)
                    _vg = _tmp_v.groupby('_key', as_index=False)['Volume'].sum()
                    _vol_dict = dict(zip(_vg['_key'], _vg['Volume']))

                _hover_bar = build_cpu_tooltip_payload(
                    df, _periodos_tt, 'Custo FP', _vol_dict,
                    simbolo, sufixo, serie_label=_serie_lbl,
                    is_cpu=_is_cpu, group_col=_grp_col_tt,
                )
                # Budget
                _vol_bud_dict: dict[str, float] = {}
                if (df_vol_bud is not None
                        and 'Volume' in df_vol_bud.columns
                        and 'Período' in df_vol_bud.columns):
                    _tmp_vb = df_vol_bud.copy()
                    if tem_ano and 'Ano' in _tmp_vb.columns:
                        _tmp_vb['_key'] = _tmp_vb['Período'].astype(str).str.strip() + ' ' + _tmp_vb['Ano'].astype(str).str.strip()
                    else:
                        _tmp_vb['_key'] = _tmp_vb['Período'].astype(str).str.strip()
                    _tmp_vb['Volume'] = pd.to_numeric(_tmp_vb['Volume'], errors='coerce').fillna(0)
                    _vgb = _tmp_vb.groupby('_key', as_index=False)['Volume'].sum()
                    _vol_bud_dict = dict(zip(_vgb['_key'], _vgb['Volume']))

                if df_bud is not None and 'Type 05' in df_bud.columns:
                    # Criar coluna composta no budget também
                    _df_bud_tt = df_bud.copy()
                    if tem_ano and 'Ano' in _df_bud_tt.columns:
                        _df_bud_tt[_grp_col_tt] = _df_bud_tt['Período'].astype(str).str.strip() + ' ' + _df_bud_tt['Ano'].astype(str).str.strip()
                    _hover_bud = build_cpu_tooltip_payload(
                        _df_bud_tt, _periodos_tt, 'Custo FP',
                        _vol_bud_dict if _vol_bud_dict else _vol_dict,
                        simbolo, sufixo, serie_label='Flex Bud',
                        is_cpu=_is_cpu, group_col=_grp_col_tt,
                        flex_mode=True, vol_actual_dict=_vol_dict,
                    )
        except Exception:
            _hover_bar = None
            _hover_bud = None

        # ── Pré-computar tooltip delta (Real − Flex Bud por Type 05/06) ──
        _hover_delta = None
        try:
            if 'Type 05' in df.columns and df_bud is not None and 'Type 05' in df_bud.columns:
                # Preparar df_bud com coluna composta se tem_ano
                _df_bud_delta = df_bud.copy()
                if tem_ano and 'Ano' in _df_bud_delta.columns:
                    _df_bud_delta[_grp_col_tt] = (
                        _df_bud_delta['Período'].astype(str).str.strip()
                        + ' ' + _df_bud_delta['Ano'].astype(str).str.strip()
                    )
                # Volume dicts (Real e Budget)
                _vol_r_delta: dict[str, float] = {}
                _vol_b_delta: dict[str, float] = {}
                if (df_vol_actual is not None
                        and 'Volume' in df_vol_actual.columns
                        and 'Período' in df_vol_actual.columns):
                    _tvr = df_vol_actual.copy()
                    if tem_ano and 'Ano' in _tvr.columns:
                        _tvr['_key'] = _tvr['Período'].astype(str).str.strip() + ' ' + _tvr['Ano'].astype(str).str.strip()
                    else:
                        _tvr['_key'] = _tvr['Período'].astype(str).str.strip()
                    _tvr['Volume'] = pd.to_numeric(_tvr['Volume'], errors='coerce').fillna(0)
                    _vgr = _tvr.groupby('_key', as_index=False)['Volume'].sum()
                    _vol_r_delta = dict(zip(_vgr['_key'], _vgr['Volume']))
                if (df_vol_bud is not None
                        and 'Volume' in df_vol_bud.columns
                        and 'Período' in df_vol_bud.columns):
                    _tvb = df_vol_bud.copy()
                    if tem_ano and 'Ano' in _tvb.columns:
                        _tvb['_key'] = _tvb['Período'].astype(str).str.strip() + ' ' + _tvb['Ano'].astype(str).str.strip()
                    else:
                        _tvb['_key'] = _tvb['Período'].astype(str).str.strip()
                    _tvb['Volume'] = pd.to_numeric(_tvb['Volume'], errors='coerce').fillna(0)
                    _vgb = _tvb.groupby('_key', as_index=False)['Volume'].sum()
                    _vol_b_delta = dict(zip(_vgb['_key'], _vgb['Volume']))
                _delta_lbl = f"Delta ({'BE' if _usar_be_t1 else 'Real'} - Flex Bud)"
                _hover_delta = build_delta_tooltip_payload(
                    df, _df_bud_delta, _periodos_tt, 'Custo FP',
                    _vol_r_delta, _vol_b_delta,
                    simbolo, sufixo, serie_label=_delta_lbl,
                    is_cpu=_is_cpu, group_col=_grp_col_tt,
                )
        except Exception:
            _hover_delta = None

        # Criar gráfico usando função separada (padrão TC Ext)
        grafico_final = create_periodo_chart(
            df_periodo, df_flex, tipo, label_valor,
            simbolo, sufixo, ordem_per, tem_ano,
            col_tipo=_col_tipo_graf,
            modo_be=_usar_be_t1,
            hover_payloads_bar=_hover_bar,
            hover_payloads_budget=_hover_bud,
            hover_payloads_delta=_hover_delta,
        )

        # Renderizar gráfico diretamente (sem placeholder)
        try:
            if grafico_final is not None:
                st.plotly_chart(grafico_final, use_container_width=True)
            else:
                st.warning("⚠️ O gráfico não pôde ser criado.")
        except Exception as e:
            import traceback
            st.error(f"❌ Erro ao renderizar gráfico: {str(e)}")
            st.code(traceback.format_exc())

    st.divider()

    # ════════════════════════════════════════
    # 📊 Análise Flex por Categoria (padrão TC Ext)
    # ════════════════════════════════════════
    st.subheader("📊 Análise Flex por Categoria")

    # Períodos disponíveis no Budget
    _periodos_flex_all = sorted(
        df_bud['Período'].dropna().unique().tolist(),
        key=lambda x: ORDEM_MESES.index(x) if x in ORDEM_MESES else 99
    )

    # Determinar mês atual para default
    from datetime import date as _date_flex
    _mes_atual_idx = _date_flex.today().month - 1
    _mes_atual_nome = ORDEM_MESES[_mes_atual_idx] if _mes_atual_idx < len(ORDEM_MESES) else None
    _default_flex = [_mes_atual_nome] if _mes_atual_nome and _mes_atual_nome in _periodos_flex_all else ["Todos"]

    # Forçar session_state para mês atual se ainda não foi inicializado
    if "flex_periodo" not in st.session_state:
        st.session_state["flex_periodo"] = _default_flex

    if df_flex is not None and 'Custo' in df.columns:
        # Controles de visualização, período e download
        col_viz1, col_viz2, col_viz3 = st.columns([1.2, 1.5, 0.8])
        with col_viz1:
            modo_visualizacao = st.radio(
                "📊 **Visualização:**",
                ["Fixo/Variável", "Total"],
                index=0,
                horizontal=True,
                key="flex_modo_visualizacao"
            )
        with col_viz2:
            _sel_per_flex = st.multiselect(
                "📅 **Período(s):**",
                ["Todos"] + _periodos_flex_all,
                default=_default_flex,
                key="flex_periodo"
            )
            periodos_flex = (
                _periodos_flex_all if "Todos" in _sel_per_flex
                else [x for x in _sel_per_flex if x != "Todos"]
            )
            if not periodos_flex:
                periodos_flex = _periodos_flex_all
        with col_viz3:
            btn_excel = st.button(
                "📥 Baixar Excel",
                key="flex_download_excel",
                use_container_width=True
            )
        st.markdown("---")
        # ── BUD: agrupar do Budget (tem todos os meses) ──
        df_bud_cat = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
        if 'Custo' in df_bud_cat.columns:
            df_bud_cat['Custo_str'] = df_bud_cat['Custo'].astype(str).str.lower()
        else:
            df_bud_cat['Custo_str'] = 'variável'
        df_bud_cat['Categoria'] = np.where(
            df_bud_cat['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
        )
        if 'Custo FP' in df_bud_cat.columns:
            df_bud_cat_agg = df_bud_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})
        else:
            df_bud_cat_agg = pd.DataFrame(columns=['Categoria', 'Período', 'BUD'])
        df_bud_cat_agg['Período'] = df_bud_cat_agg['Período'].astype(str)
        df_bud_cat_agg = ordenar_por_mes(df_bud_cat_agg)

        # ── Real: agrupar do Real (pode ter menos meses) ──
        df_real_cat = df[df['Período'].isin(periodos_flex)].copy()
        if not df_real_cat.empty and 'Custo' in df_real_cat.columns and 'Custo FP' in df_real_cat.columns:
            df_real_cat['Custo_str'] = df_real_cat['Custo'].astype(str).str.lower()
            df_real_cat['Categoria'] = np.where(
                df_real_cat['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
            )
            df_real_cat_agg = df_real_cat.groupby(
                ['Categoria', 'Período'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
            df_real_cat_agg['Período'] = df_real_cat_agg['Período'].astype(str)
        else:
            df_real_cat_agg = pd.DataFrame(columns=['Categoria', 'Período', 'Total'])

        # Preparar dados de volume para cálculo de Flex
        if df_vol_bud is not None and df_vol_actual is not None:
            df_vol_bud_norm = df_vol_bud.copy()
            df_vol_bud_norm = df_vol_bud_norm[
                df_vol_bud_norm['Período'].isin(periodos_flex)
            ].copy()
            df_vol_act_norm = df_vol_actual.copy()
            df_vol_act_norm = df_vol_act_norm[
                df_vol_act_norm['Período'].isin(periodos_flex)
            ].copy()
            vol_total_budget = df_vol_bud_norm['Volume'].sum()
            vol_total_actual = df_vol_act_norm['Volume'].sum()
            proporcao_global = (vol_total_actual / vol_total_budget) if vol_total_budget > 0 else 1
        else:
            proporcao_global = 1

        # Merge: BUD como base (ano completo), Real onde disponível
        df_cat_agg = df_bud_cat_agg.merge(
            df_real_cat_agg, on=['Categoria', 'Período'], how='left'
        )
        df_cat_agg['Total'] = df_cat_agg['Total'].fillna(0)

        # Calcular Flex BUD usando proporção GLOBAL
        df_cat_agg['Flex BUD'] = np.where(
            df_cat_agg['Categoria'] == 'Fixo',
            df_cat_agg['BUD'],
            df_cat_agg['BUD'] * proporcao_global
        )

        # Aplicar CPU se necessário
        if tipo == 'CPU (Custo por Unidade)':
            if 'vol_total_budget' in locals() and 'vol_total_actual' in locals():
                df_cat_agg['Total'] = calcular_cpu(
                    df_cat_agg['Total'], vol_total_actual
                )
                df_cat_agg['BUD'] = calcular_cpu(
                    df_cat_agg['BUD'], vol_total_budget
                )
                df_cat_agg['Flex BUD'] = calcular_cpu(
                    df_cat_agg['Flex BUD'], vol_total_actual
                )

        # Calcular diferenças
        df_cat_agg['Flex Bud - BUD'] = (
            df_cat_agg['Flex BUD'] - df_cat_agg['BUD']
        )
        df_cat_agg['Total - Flex Bud'] = (
            df_cat_agg['Total'] - df_cat_agg['Flex BUD']
        )
        df_cat_agg['Total / Flex Bud'] = np.where(
            df_cat_agg['Flex BUD'] != 0,
            df_cat_agg['Total'] / df_cat_agg['Flex BUD'],
            0
        )

        # ═══════════════════════════════════════
        # 📊 Resumo Geral
        # ═══════════════════════════════════════
        st.markdown("### 📊 Resumo Geral")

        # Calcular totais
        total_bud = df_cat_agg['BUD'].sum()
        total_flex_bud = df_cat_agg['Flex BUD'].sum()
        total_real = df_cat_agg['Total'].sum()
        total_flex_diff = total_flex_bud - total_bud
        total_real_diff = total_real - total_flex_bud
        total_ratio = (total_real / total_flex_bud) if total_flex_bud != 0 else 0

        # KPIs de Resumo - 6 em linha única
        kr1, kr2, kr3, kr4, kr5, kr6 = st.columns(6)
        with kr1:
            render_kpi("BUD", f"{simbolo} {total_bud:,.2f}{sufixo}")
        with kr2:
            render_kpi("Flex - BUD", f"{simbolo} {total_flex_diff:+,.2f}{sufixo}")
        with kr3:
            render_kpi("Flex BUD", f"{simbolo} {total_flex_bud:,.2f}{sufixo}")
        with kr4:
            render_kpi("Total - Flex", f"{simbolo} {total_real_diff:+,.2f}{sufixo}")
        with kr5:
            render_kpi(
                "Best Estimate" if _usar_be_t1 else "Total Real",
                f"{simbolo} {total_real:,.2f}{sufixo}"
            )
        with kr6:
            render_kpi("Total / Flex", f"{total_ratio:.0%}")

        render_kpi_spacer()
        st.markdown("---")

        # ═══════════════════════════════════════
        # 📥 Exportar para Excel (se botão clicado)
        # ═══════════════════════════════════════
        if btn_excel:
            try:
                # Preparar DataFrame para download
                df_download = df_cat_agg[['Categoria', 'Período', 'BUD',
                                          'Flex Bud - BUD', 'Flex BUD',
                                          'Total - Flex Bud', 'Total',
                                          'Total / Flex Bud']].copy()
                # Formatar ratio como percentual
                df_download['Total / Flex Bud'] = df_download['Total / Flex Bud'].apply(
                    lambda x: f"{x:.2%}"
                )

                # Salvar na pasta Downloads
                downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                tipo_nome = "CPU" if tipo == "CPU (Custo por Unidade)" else "Custo_Total"
                modo_nome = "Fixo_Variavel" if modo_visualizacao == "Fixo/Variável" else "Total"
                file_name = f"TC_Principal_Flex_{modo_nome}_{tipo_nome}_{ano}.xlsx"
                file_path = os.path.join(downloads_path, file_name)

                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    df_download.to_excel(writer, index=False, sheet_name='Flex_Bud')

                st.success(f"✅ Arquivo salvo em: {file_path}")
            except Exception as e:
                st.error(f"❌ Erro ao exportar: {e}")

        def _preparar_tabela_flex_hierarquia(
            df_base: pd.DataFrame,
            coluna_id: str,
        ) -> pd.DataFrame:
            colunas_tabela = [
                col for col in [
                    coluna_id,
                    'BUD',
                    'Flex Bud - BUD',
                    'Flex BUD',
                    'Total - Flex Bud',
                    'Total',
                    'Total / Flex Bud',
                ]
                if col in df_base.columns
            ]
            if not colunas_tabela:
                return pd.DataFrame()

            df_tabela = df_base[colunas_tabela].copy()
            for col in ['BUD', 'Flex BUD', 'Total']:
                if col not in df_tabela.columns:
                    df_tabela[col] = 0.0

            df_tabela = df_tabela[
                (df_tabela['Total'].abs() > 0.01)
                | (df_tabela['BUD'].abs() > 0.01)
                | (df_tabela['Flex BUD'].abs() > 0.01)
            ].copy()
            return df_tabela

        # ═══════════════════════════════════════
        # Expanders 💰 Fixo e 💰 Variável com hierarquia Type 05 → Type 06 → Account
        # ═══════════════════════════════════════
        expand_state_key = 'home_tc_flex_expand_all'
        if expand_state_key not in st.session_state:
            st.session_state[expand_state_key] = False

        ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 3])
        with ctrl_col1:
            if st.button('Expandir tudo', key='home_tc_expandir_flex'):
                st.session_state[expand_state_key] = True
        with ctrl_col2:
            if st.button('Recolher tudo', key='home_tc_recolher_flex'):
                st.session_state[expand_state_key] = False
        with ctrl_col3:
            st.caption('Controle aplicado aos expanders desta tabela Flex.')

        expandir_flex = st.session_state[expand_state_key]

        # Mostrar expanders apenas se visualização for Fixo/Variável
        if modo_visualizacao == "Fixo/Variável":
            # ── BUD: hierarquia do Budget (todos os accounts) ──
            df_bud_hier_base = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
            if 'Custo' in df_bud_hier_base.columns:
                df_bud_hier_base['Custo_str'] = df_bud_hier_base['Custo'].astype(str).str.lower()
            else:
                df_bud_hier_base['Custo_str'] = 'variável'
            df_bud_hier_base['Categoria'] = np.where(
                df_bud_hier_base['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
            )
            if 'Type 05' not in df_bud_hier_base.columns:
                df_bud_hier_base['Type 05'] = 'N/A'
            if 'Type 06' not in df_bud_hier_base.columns:
                df_bud_hier_base['Type 06'] = 'N/A'
            if 'Account' not in df_bud_hier_base.columns:
                df_bud_hier_base['Account'] = 'N/A'
            if 'Custo FP' in df_bud_hier_base.columns:
                df_bud_hier = df_bud_hier_base.groupby(
                    ['Categoria', 'Type 05', 'Type 06', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})
            else:
                df_bud_hier = pd.DataFrame(columns=['Categoria', 'Type 05', 'Type 06', 'Account', 'BUD'])

            # ── Real: hierarquia do Real (pode ter menos accounts) ──
            df_real_hier = df[df['Período'].isin(periodos_flex)].copy()
            if not df_real_hier.empty and 'Custo' in df_real_hier.columns and 'Custo FP' in df_real_hier.columns:
                df_real_hier['Custo_str'] = df_real_hier['Custo'].astype(str).str.lower()
                df_real_hier['Categoria'] = np.where(
                    df_real_hier['Custo_str'].str.startswith('fix'), 'Fixo', 'Variável'
                )
                if 'Type 05' not in df_real_hier.columns:
                    df_real_hier['Type 05'] = 'N/A'
                if 'Type 06' not in df_real_hier.columns:
                    df_real_hier['Type 06'] = 'N/A'
                if 'Account' not in df_real_hier.columns:
                    df_real_hier['Account'] = 'N/A'
                df_real_hier_agg = df_real_hier.groupby(
                    ['Categoria', 'Type 05', 'Type 06', 'Account'], as_index=False
                )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})
            else:
                df_real_hier_agg = pd.DataFrame(
                    columns=['Categoria', 'Type 05', 'Type 06', 'Account', 'Total']
                )

            # Merge com volumes para cálculo de Flex (filtrados por período)
            if df_vol_bud is not None:
                df_vol_bud_filt = df_vol_bud.copy()
                df_vol_bud_filt = df_vol_bud_filt[
                    df_vol_bud_filt['Período'].isin(periodos_flex)
                ].copy()
                vol_total_bud = df_vol_bud_filt['Volume'].sum()
            else:
                vol_total_bud = 1

            if df_vol_actual is not None:
                df_vol_act_filt = df_vol_actual.copy()
                df_vol_act_filt = df_vol_act_filt[
                    df_vol_act_filt['Período'].isin(periodos_flex)
                ].copy()
                vol_total_act = df_vol_act_filt['Volume'].sum()
            else:
                vol_total_act = vol_total_bud
            proporcao_global = vol_total_act / vol_total_bud if vol_total_bud > 0 else 1

            # Merge: BUD como base, Real onde disponível
            df_hier_agg = df_bud_hier.merge(
                df_real_hier_agg,
                on=['Categoria', 'Type 05', 'Type 06', 'Account'],
                how='left'
            )
            df_hier_agg['Total'] = df_hier_agg['Total'].fillna(0)

            # Calcular Flex BUD (Fixo = BUD, Variável = BUD * Proporção)
            df_hier_agg['Flex BUD'] = np.where(
                df_hier_agg['Categoria'] == 'Fixo',
                df_hier_agg['BUD'],
                df_hier_agg['BUD'] * proporcao_global
            )

            # Aplicar CPU se necessário
            if tipo == 'CPU (Custo por Unidade)':
                df_hier_agg['Total'] = calcular_cpu(
                    df_hier_agg['Total'], vol_total_act
                )
                df_hier_agg['BUD'] = calcular_cpu(
                    df_hier_agg['BUD'], vol_total_bud
                )
                df_hier_agg['Flex BUD'] = calcular_cpu(
                    df_hier_agg['Flex BUD'], vol_total_act
                )

            # Calcular diferenças e ratio
            df_hier_agg['Flex Bud - BUD'] = df_hier_agg['Flex BUD'] - df_hier_agg['BUD']
            df_hier_agg['Total - Flex Bud'] = df_hier_agg['Total'] - df_hier_agg['Flex BUD']
            df_hier_agg['Total / Flex Bud'] = np.where(
                df_hier_agg['Flex BUD'] != 0,
                df_hier_agg['Total'] / df_hier_agg['Flex BUD'],
                0
            )

            for categoria in ['Fixo', 'Variável']:
                df_cat_hier = df_hier_agg[
                    df_hier_agg['Categoria'] == categoria
                ].copy()

                if len(df_cat_hier) == 0:
                    continue

                # Totais da categoria
                cat_bud = df_cat_hier['BUD'].sum()
                cat_flex = df_cat_hier['Flex BUD'].sum()
                cat_total = df_cat_hier['Total'].sum()
                cat_flex_diff = cat_flex - cat_bud
                cat_real_diff = cat_total - cat_flex
                cat_ratio = cat_total / cat_flex if cat_flex != 0 else 0
                total_cat_fmt = f"{simbolo} {cat_total:,.2f}{sufixo}"

                with st.expander(
                    f"💰 {categoria} - Total: {total_cat_fmt}",
                    expanded=expandir_flex
                ):
                    # KPIs da categoria - 6 em linha única
                    ck1, ck2, ck3, ck4, ck5, ck6 = st.columns(6)
                    with ck1:
                        render_kpi("BUD", f"{simbolo} {cat_bud:,.2f}{sufixo}")
                    with ck2:
                        render_kpi("Flex - BUD", f"{simbolo} {cat_flex_diff:+,.2f}{sufixo}")
                    with ck3:
                        render_kpi("Flex BUD", f"{simbolo} {cat_flex:,.2f}{sufixo}")
                    with ck4:
                        render_kpi("Total - Flex", f"{simbolo} {cat_real_diff:+,.2f}{sufixo}")
                    with ck5:
                        render_kpi("Total", f"{simbolo} {cat_total:,.2f}{sufixo}")
                    with ck6:
                        render_kpi("Total / Flex", f"{cat_ratio:.0%}")

                    render_kpi_spacer()

                    # Sub-expanders por Type 05
                    type05_list = df_cat_hier['Type 05'].unique()
                    for type05 in type05_list:
                        df_type05 = df_cat_hier[
                            df_cat_hier['Type 05'] == type05
                        ].copy()

                        # Totais do Type 05
                        t05_bud = df_type05['BUD'].sum()
                        t05_flex = df_type05['Flex BUD'].sum()
                        t05_total = df_type05['Total'].sum()
                        t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                        with st.expander(
                            f"📊 Type 05: {type05} - Total: {t05_fmt}",
                            expanded=expandir_flex
                        ):
                            type06_list = df_type05['Type 06'].unique()
                            exibiu_type06 = False
                            for type06 in type06_list:
                                df_type06 = df_type05[
                                    df_type05['Type 06'] == type06
                                ].copy()
                                df_tabela = _preparar_tabela_flex_hierarquia(
                                    df_type06,
                                    'Account',
                                )

                                if len(df_tabela) == 0:
                                    continue

                                exibiu_type06 = True
                                t06_total = df_type06['Total'].sum()
                                t06_fmt = f"{simbolo} {t06_total:,.2f}{sufixo}"

                                with st.expander(
                                    f"🔹 Type 06: {type06} — Total: {t06_fmt}",
                                    expanded=expandir_flex,
                                ):
                                    summary_values = {
                                        'BUD': df_type06['BUD'].sum(),
                                        'Flex Bud - BUD': df_type06['Flex Bud - BUD'].sum(),
                                        'Flex BUD': df_type06['Flex BUD'].sum(),
                                        'Total - Flex Bud': df_type06['Total - Flex Bud'].sum(),
                                        'Total': df_type06['Total'].sum(),
                                        'Total / Flex Bud': (
                                            df_type06['Total'].sum() / df_type06['Flex BUD'].sum()
                                            if df_type06['Flex BUD'].sum() != 0 else 0
                                        ),
                                    }
                                    render_inline_summary_metrics(
                                        summary_values,
                                        ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud'],
                                        currency_columns={'BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total'},
                                        ratio_columns={'Total / Flex Bud'},
                                        number_prefix=f"{simbolo} ",
                                        number_suffix=sufixo,
                                    )
                                    html_tabela = criar_tabela_html_flex(
                                        df_tabela, simbolo, sufixo
                                    )
                                    st.markdown(html_tabela, unsafe_allow_html=True)

                            if not exibiu_type06:
                                st.info("Sem dados para exibir.")
        else:
            # Modo Total: expanders direto por Type 05 → Type 06 → Account (sem Fixo/Variável)
            # ── BUD: agrupar do Budget (todos os accounts) ──
            df_bud_total_base = df_bud[df_bud['Período'].isin(periodos_flex)].copy()
            if 'Type 05' not in df_bud_total_base.columns:
                df_bud_total_base['Type 05'] = 'N/A'
            if 'Type 06' not in df_bud_total_base.columns:
                df_bud_total_base['Type 06'] = 'N/A'
            if 'Account' not in df_bud_total_base.columns:
                df_bud_total_base['Account'] = 'N/A'
            df_bud_total = df_bud_total_base.groupby(
                ['Type 05', 'Type 06', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'BUD'})

            # ── Real: agrupar do Real (pode ter menos accounts) ──
            df_real_total = df[df['Período'].isin(periodos_flex)].copy()
            if 'Type 05' not in df_real_total.columns:
                df_real_total['Type 05'] = 'N/A'
            if 'Type 06' not in df_real_total.columns:
                df_real_total['Type 06'] = 'N/A'
            if 'Account' not in df_real_total.columns:
                df_real_total['Account'] = 'N/A'
            df_real_total_agg = df_real_total.groupby(
                ['Type 05', 'Type 06', 'Account'], as_index=False
            )['Custo FP'].sum().rename(columns={'Custo FP': 'Total'})

            # Merge com volumes para cálculo de Flex (filtrados por período)
            if df_vol_bud is not None:
                df_vol_bud_filt = df_vol_bud.copy()
                df_vol_bud_filt = df_vol_bud_filt[
                    df_vol_bud_filt['Período'].isin(periodos_flex)
                ].copy()
                vol_total_bud = df_vol_bud_filt['Volume'].sum()
            else:
                vol_total_bud = 1

            if df_vol_actual is not None:
                df_vol_act_filt = df_vol_actual.copy()
                df_vol_act_filt = df_vol_act_filt[
                    df_vol_act_filt['Período'].isin(periodos_flex)
                ].copy()
                vol_total_act = df_vol_act_filt['Volume'].sum()
            else:
                vol_total_act = vol_total_bud
            proporcao_global = (vol_total_act / vol_total_bud
                               if vol_total_bud > 0 else 1)

            # Merge: BUD como base, Real onde disponível
            df_total_agg = df_bud_total.merge(
                df_real_total_agg,
                on=['Type 05', 'Type 06', 'Account'],
                how='left'
            )
            df_total_agg['Total'] = df_total_agg['Total'].fillna(0)

            # Flex BUD (média de Fixo e Variável = BUD * proporcao parcial)
            df_total_agg['Flex BUD'] = df_total_agg['BUD'] * proporcao_global

            # Aplicar CPU se necessário
            if tipo == 'CPU (Custo por Unidade)':
                df_total_agg['Total'] = calcular_cpu(
                    df_total_agg['Total'], vol_total_act
                )
                df_total_agg['BUD'] = calcular_cpu(
                    df_total_agg['BUD'], vol_total_bud
                )
                df_total_agg['Flex BUD'] = calcular_cpu(
                    df_total_agg['Flex BUD'], vol_total_act
                )

            # Calcular diferenças e ratio
            df_total_agg['Flex Bud - BUD'] = (
                df_total_agg['Flex BUD'] - df_total_agg['BUD']
            )
            df_total_agg['Total - Flex Bud'] = (
                df_total_agg['Total'] - df_total_agg['Flex BUD']
            )
            df_total_agg['Total / Flex Bud'] = np.where(
                df_total_agg['Flex BUD'] != 0,
                df_total_agg['Total'] / df_total_agg['Flex BUD'],
                0
            )

            # Expanders por Type 05 (diretamente, sem Fixo/Variável)
            type05_list = df_total_agg['Type 05'].unique()
            for type05 in type05_list:
                df_type05 = df_total_agg[
                    df_total_agg['Type 05'] == type05
                ].copy()

                # Filtrar linhas zeradas/nulas
                df_type05 = df_type05[
                    (df_type05['Total'].abs() > 0.01) |
                    (df_type05['BUD'].abs() > 0.01)
                ].copy()

                if len(df_type05) == 0:
                    continue

                # Totais do Type 05
                t05_total = df_type05['Total'].sum()
                t05_fmt = f"{simbolo} {t05_total:,.2f}{sufixo}"

                with st.expander(
                    f"📊 Type 05: {type05} - Total: {t05_fmt}",
                    expanded=expandir_flex
                ):
                    # KPIs do Type 05
                    t05_bud = df_type05['BUD'].sum()
                    t05_flex = df_type05['Flex BUD'].sum()
                    t05_flex_diff = t05_flex - t05_bud
                    t05_real_diff = t05_total - t05_flex
                    t05_ratio = t05_total / t05_flex if t05_flex != 0 else 0

                    tk1, tk2, tk3, tk4, tk5, tk6 = st.columns(6)
                    with tk1:
                        render_kpi("BUD", f"{simbolo} {t05_bud:,.2f}{sufixo}")
                    with tk2:
                        render_kpi("Flex-BUD", f"{simbolo} {t05_flex_diff:+,.2f}{sufixo}")
                    with tk3:
                        render_kpi("Flex BUD", f"{simbolo} {t05_flex:,.2f}{sufixo}")
                    with tk4:
                        render_kpi("Total-Flex", f"{simbolo} {t05_real_diff:+,.2f}{sufixo}")
                    with tk5:
                        render_kpi("Total", f"{simbolo} {t05_total:,.2f}{sufixo}")
                    with tk6:
                        render_kpi("Total/Flex", f"{t05_ratio:.0%}")

                    render_kpi_spacer()

                    type06_list = df_type05['Type 06'].unique()
                    exibiu_type06 = False
                    for type06 in type06_list:
                        df_type06 = df_type05[
                            df_type05['Type 06'] == type06
                        ].copy()
                        df_tabela = _preparar_tabela_flex_hierarquia(
                            df_type06,
                            'Account',
                        )

                        if len(df_tabela) == 0:
                            continue

                        exibiu_type06 = True
                        t06_total = df_type06['Total'].sum()
                        t06_fmt = f"{simbolo} {t06_total:,.2f}{sufixo}"

                        with st.expander(
                            f"🔹 Type 06: {type06} — Total: {t06_fmt}",
                            expanded=expandir_flex,
                        ):
                            summary_values = {
                                'BUD': df_type06['BUD'].sum(),
                                'Flex Bud - BUD': df_type06['Flex Bud - BUD'].sum(),
                                'Flex BUD': df_type06['Flex BUD'].sum(),
                                'Total - Flex Bud': df_type06['Total - Flex Bud'].sum(),
                                'Total': df_type06['Total'].sum(),
                                'Total / Flex Bud': (
                                    df_type06['Total'].sum() / df_type06['Flex BUD'].sum()
                                    if df_type06['Flex BUD'].sum() != 0 else 0
                                ),
                            }
                            render_inline_summary_metrics(
                                summary_values,
                                ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total', 'Total / Flex Bud'],
                                currency_columns={'BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Total'},
                                ratio_columns={'Total / Flex Bud'},
                                number_prefix=f"{simbolo} ",
                                number_suffix=sufixo,
                            )
                            html_tabela = criar_tabela_html_flex(
                                df_tabela, simbolo, sufixo
                            )
                            st.markdown(html_tabela, unsafe_allow_html=True)

                    if not exibiu_type06:
                        st.info("Sem dados para exibir.")

    else:
        st.info(
            "ℹ️ Dados de categoria (Custo) não disponíveis para "
            "análise Flex."
        )

    # ════════════════════════════════════════
    # 🔍 VALIDAÇÃO DE CONSISTÊNCIA COMPLETA
    # ════════════════════════════════════════
    st.markdown("---")
    with st.expander("🔍 Validação de Consistência de Dados (Todos os Datasets)", expanded=False):
        st.markdown("""
        **Objetivo:** Garantir a confiabilidade do projeto comparando **TODOS** os datasets:
        - 📋 **Dados Detalhados** (Tab 6 - valores diretos)
        - 📊 **Agregação por Período** (como aparecem nos gráficos)

        **Validando:** Budget, Flex Budget, Real, Best Estimate (Total e Por Veículo)
        """)

        try:
            # ══════════════════════════════════════════════
            # FUNÇÃO AUXILIAR: Validar consistência dataset
            # ══════════════════════════════════════════════
            def _validar_dataset(df_raw, col_valor, nome_dataset, icone):
                """Valida se total direto = total agregado por período"""
                if df_raw is None or df_raw.empty or col_valor not in df_raw.columns:
                    return None

                # Total direto (como no Tab 6) - arredondado para 2 casas
                total_direto = round(float(df_raw[col_valor].sum()), 2)

                # Agregar por período (como no gráfico)
                tem_ano = 'Ano' in df_raw.columns
                grp_cols = ['Ano', 'Período'] if tem_ano else ['Período']

                df_periodo_agg = df_raw.groupby(grp_cols, as_index=False)[col_valor].sum()
                total_periodo = round(float(df_periodo_agg[col_valor].sum()), 2)

                # Diferença em valor absoluto (arredondado para 2 casas)
                diff_valor = round(total_periodo - total_direto, 2)

                # Calcular diferença percentual
                if total_direto > 0:
                    diff_perc = round(((total_periodo - total_direto) / total_direto) * 100, 2)
                else:
                    diff_perc = 0.0

                # Status baseado na diferença em valor (mais preciso após arredondamento)
                if abs(diff_valor) < 0.01:
                    status = "✅"
                    status_text = "OK"
                elif abs(diff_perc) < 1.0:
                    status = "⚠️"
                    status_text = "Pequena dif."
                else:
                    status = "❌"
                    status_text = "INCONSISTENTE"

                return {
                    'icone': icone,
                    'nome': nome_dataset,
                    'total_direto': total_direto,
                    'total_periodo': total_periodo,
                    'diff_valor': diff_valor,
                    'diff_perc': diff_perc,
                    'status': status,
                    'status_text': status_text,
                }

            # ══════════════════════════════════════════════
            # PREPARAR DADOS (sem filtros, com fator/moeda)
            # ══════════════════════════════════════════════
            _cols_mon = [c for c in COLUNAS_MONETARIAS if c in df_principal.columns]

            # 1. Budget Total
            _df_bud_val = _raw_df_principal.copy()
            _df_bud_val = aplicar_fator_df(_df_bud_val, _cols_mon, fator)
            _df_bud_val = converter_moeda_df(_df_bud_val, _cols_mon, moeda, taxas)

            # 2. Flex Budget Total
            # NOTA: df_flex já vem com fator/moeda aplicados (calculado a partir de df convertido)
            # NÃO reaplicar fator/moeda para evitar dupla conversão
            _df_flex_val = None
            if df_flex is not None and not df_flex.empty:
                _df_flex_val = df_flex.copy()
                # Flex_Bud já está na escala correta

            # 3. Real Total
            _df_real_val = None
            if _raw_df_real is not None and not _raw_df_real.empty:
                _df_real_val = _raw_df_real.copy()
                _cols_r = [c for c in COLUNAS_MONETARIAS if c in _df_real_val.columns]
                _df_real_val = aplicar_fator_df(_df_real_val, _cols_r, fator)
                _df_real_val = converter_moeda_df(_df_real_val, _cols_r, moeda, taxas)

            # 4. Best Estimate Total
            _df_be_val = None
            _be_val_tmp = _get_be_full()
            _be_val_src = _be_val_tmp if _be_val_tmp is not None else _raw_df_be
            if _be_val_src is not None and not _be_val_src.empty:
                _df_be_val = _be_val_src.copy()
                _cols_be = [c for c in COLUNAS_MONETARIAS if c in _df_be_val.columns]
                _df_be_val = aplicar_fator_df(_df_be_val, _cols_be, fator)
                _df_be_val = converter_moeda_df(_df_be_val, _cols_be, moeda, taxas)

            # 5. Budget Por Veículo
            _df_vbud_val = None
            if df_veic_bud_raw is not None and not df_veic_bud_raw.empty:
                _df_vbud_val = df_veic_bud_raw.copy()
                if 'Custo FP Veiculo' in _df_vbud_val.columns:
                    _df_vbud_val['Custo FP'] = _df_vbud_val['Custo FP Veiculo']
                _cols_vb = [c for c in COLUNAS_MONETARIAS if c in _df_vbud_val.columns]
                _df_vbud_val = aplicar_fator_df(_df_vbud_val, _cols_vb, fator)
                _df_vbud_val = converter_moeda_df(_df_vbud_val, _cols_vb, moeda, taxas)

            # 6. Flex Budget Por Veículo (calcular a partir de Budget Por Veículo)
            _df_vflex_val = None
            if _df_vbud_val is not None and df_vol_bud is not None:
                try:
                    _df_vflex_val = calcular_flex_budget_detalhado(
                        _df_vbud_val, df_vol_bud.copy(), df_vol_actual.copy() if df_vol_actual is not None else None
                    )
                    if _df_vflex_val is not None and 'Flex_Bud' in _df_vflex_val.columns:
                        # Já está com fator/moeda aplicados
                        pass
                except Exception:
                    _df_vflex_val = None

            # 7. Real Por Veículo
            _df_vreal_val = None
            if df_veic_real_raw is not None and not df_veic_real_raw.empty:
                _df_vreal_val = df_veic_real_raw.copy()
                if 'Custo FP Veiculo' in _df_vreal_val.columns:
                    _df_vreal_val['Custo FP'] = _df_vreal_val['Custo FP Veiculo']
                _cols_vr = [c for c in COLUNAS_MONETARIAS if c in _df_vreal_val.columns]
                _df_vreal_val = aplicar_fator_df(_df_vreal_val, _cols_vr, fator)
                _df_vreal_val = converter_moeda_df(_df_vreal_val, _cols_vr, moeda, taxas)

            # 8. Best Estimate Por Veículo (prioridade: arquivo pré-gerado)
            _df_vbe_val = None

            # Prioridade: usar arquivo pré-gerado (igual Budget/Real)
            _veic_be_raw_val = _get_veic_be_raw()
            if _veic_be_raw_val is not None and not _veic_be_raw_val.empty:
                try:
                    _df_vbe_val = normalizar_periodo(_veic_be_raw_val.copy())
                    # Validacao cruzada precisa comparar o mesmo universo do
                    # forecast total. Nao aplicar filtros de tela aqui, senao
                    # o total anual passa a ser comparado com um recorte local.
                    if 'Ano' in _df_vbe_val.columns:
                        _df_vbe_val = _df_vbe_val[
                            _df_vbe_val['Ano'] == int(ano)
                        ].copy()
                    if _df_vbe_val is not None and 'Custo FP Veiculo' in _df_vbe_val.columns:
                        _df_vbe_val['Custo FP'] = _df_vbe_val['Custo FP Veiculo']
                    _cols_vbe = [c for c in COLUNAS_MONETARIAS if c in _df_vbe_val.columns]
                    _df_vbe_val = aplicar_fator_df(_df_vbe_val, _cols_vbe, fator)
                    _df_vbe_val = converter_moeda_df(_df_vbe_val, _cols_vbe, moeda, taxas)
                except Exception:
                    _df_vbe_val = None

            # Fallback: ratear em runtime se arquivo não existe (mesma lógica do Real)
            if _df_vbe_val is None and _df_be_val is not None:
                try:
                    _pct_rateio = load_percentual_rateio_veiculos_real(ano)
                    _dea_rateio = load_dea_dedicado_real(ano)
                    if _pct_rateio is not None:
                        _df_vbe_val = ratear_be_por_veiculo(_df_be_val, _pct_rateio, df_dea=_dea_rateio)
                        if _df_vbe_val is not None and 'Custo FP Veiculo' in _df_vbe_val.columns:
                            _df_vbe_val['Custo FP'] = _df_vbe_val['Custo FP Veiculo']
                except Exception:
                    _df_vbe_val = None

            # ══════════════════════════════════════════════
            # FUNÇÃO AUXILIAR: Validar Total vs Por Veículo
            # ══════════════════════════════════════════════
            def _validar_total_vs_veiculo(df_total, df_veic, col_valor, nome, icone):
                """Valida se Total == Soma(Por Veículo) para detectar erros de escala"""
                if df_total is None or df_total.empty or col_valor not in df_total.columns:
                    return None
                if df_veic is None or df_veic.empty or col_valor not in df_veic.columns:
                    return None

                total_val = round(float(df_total[col_valor].sum()), 2)
                veic_val = round(float(df_veic[col_valor].sum()), 2)

                diff_valor = round(veic_val - total_val, 2)

                if total_val > 0:
                    diff_perc = round(((veic_val - total_val) / total_val) * 100, 2)
                else:
                    diff_perc = 0.0

                # Detectar erro de escala (ex: K vs M = 1000x diferença)
                if total_val > 0 and veic_val > 0:
                    ratio = veic_val / total_val
                    if ratio > 500:  # Por Veículo é 500x+ maior que Total
                        return {
                            'icone': icone, 'nome': f'{nome}: ESCALA ERRADA',
                            'total_direto': total_val, 'total_periodo': veic_val,
                            'diff_valor': diff_valor, 'diff_perc': diff_perc,
                            'status': "❌", 'status_text': "ESCALA!",
                        }
                    elif ratio < 0.002:  # Por Veículo é 500x+ menor que Total
                        return {
                            'icone': icone, 'nome': f'{nome}: ESCALA ERRADA',
                            'total_direto': total_val, 'total_periodo': veic_val,
                            'diff_valor': diff_valor, 'diff_perc': diff_perc,
                            'status': "❌", 'status_text': "ESCALA!",
                        }

                if abs(diff_valor) < 0.01:
                    status, status_text = "✅", "OK"
                elif abs(diff_perc) < 1.0:
                    status, status_text = "⚠️", "Pequena dif."
                else:
                    status, status_text = "❌", "INCONSISTENTE"

                return {
                    'icone': icone, 'nome': nome,
                    'total_direto': total_val, 'total_periodo': veic_val,
                    'diff_valor': diff_valor, 'diff_perc': diff_perc,
                    'status': status, 'status_text': status_text,
                }

            # ══════════════════════════════════════════════
            # EXECUTAR VALIDAÇÕES
            # ══════════════════════════════════════════════
            resultados = []
            resultados_cruzados = []

            # Validações internas (Total Direto vs Agregado)
            for df_val, col, nome, icone in [
                (_df_bud_val, 'Custo FP', 'Budget', '💰'),
                (_df_flex_val, 'Flex_Bud', 'Flex Budget', '📐'),
                (_df_real_val, 'Custo FP', 'Real', '✅'),
                (_df_be_val, 'Custo FP', 'Best Estimate', '🔮'),
            ]:
                r = _validar_dataset(df_val, col, nome, icone)
                if r: resultados.append(r)

            # Validações cruzadas (Total vs Por Veículo)
            for df_t, df_v, col, nome, icone in [
                (_df_bud_val, _df_vbud_val, 'Custo FP', 'Budget: Total vs Veículo', '🚗💰'),
                (_df_flex_val, _df_vflex_val, 'Flex_Bud', 'Flex Budget: Total vs Veículo', '🚗📐'),
                (_df_real_val, _df_vreal_val, 'Custo FP', 'Real: Total vs Veículo', '🚗✅'),
                (_df_be_val, _df_vbe_val, 'Custo FP', 'Best Estimate: Total vs Veículo', '🚗🔮'),
            ]:
                r = _validar_total_vs_veiculo(df_t, df_v, col, nome, icone)
                if r: resultados_cruzados.append(r)

            # ══════════════════════════════════════════════
            # RENDERIZAR TABELA HTML
            # ══════════════════════════════════════════════
            # TABELA 1: Validações INTERNAS (Direto vs Agregado)
            # ══════════════════════════════════════════════
            if len(resultados) > 0:
                st.markdown("#### 📊 Validação Interna (Direto vs Agregado)")
                html = f"""
                <style>
                .validacao-table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-family: 'Segoe UI', sans-serif;
                    font-size: 12px;
                    margin: 10px 0;
                }}
                .validacao-table th {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 10px 6px;
                    text-align: center;
                    font-weight: 600;
                    border-bottom: 2px solid #764ba2;
                    font-size: 11px;
                }}
                .validacao-table td {{
                    padding: 8px 6px;
                    border-bottom: 1px solid #e5e7eb;
                }}
                .validacao-table tr:hover {{
                    background-color: #f9fafb;
                }}
                .valor-num {{
                    text-align: right;
                    font-family: 'Consolas', monospace;
                    font-weight: 500;
                }}
                .status-ok {{ color: #10b981; font-weight: bold; }}
                .status-warn {{ color: #f59e0b; font-weight: bold; }}
                .status-erro {{ color: #ef4444; font-weight: bold; }}
                .diff-positivo {{ color: #ef4444; }}
                .diff-negativo {{ color: #10b981; }}
                .diff-zero {{ color: #6b7280; }}
                </style>
                <table class="validacao-table">
                    <thead>
                        <tr>
                            <th style="width: 50px;">Status</th>
                            <th style="text-align: left;">Dataset</th>
                            <th>Total Direto</th>
                            <th>Total Agregado</th>
                            <th>Diferença ({simbolo})</th>
                            <th>Erro %</th>
                        </tr>
                    </thead>
                    <tbody>
                """

                for res in resultados:
                    status_class = "status-ok" if res['status'] == "✅" else ("status-warn" if res['status'] == "⚠️" else "status-erro")

                    # Classe para diferença em valor
                    if abs(res['diff_valor']) < 0.01:
                        diff_class = "diff-zero"
                    elif res['diff_valor'] > 0:
                        diff_class = "diff-positivo"
                    else:
                        diff_class = "diff-negativo"

                    html += f"""
                    <tr>
                        <td style="text-align: center; font-size: 16px;">{res['status']}</td>
                        <td style="text-align: left;"><strong>{res['icone']} {res['nome']}</strong></td>
                        <td class="valor-num">{simbolo}{res['total_direto']:,.2f}{sufixo}</td>
                        <td class="valor-num">{simbolo}{res['total_periodo']:,.2f}{sufixo}</td>
                        <td class="valor-num {diff_class}">{res['diff_valor']:+,.2f}{sufixo}</td>
                        <td class="valor-num {status_class}">{res['diff_perc']:+.2f}%</td>
                    </tr>
                    """

                html += """
                    </tbody>
                </table>
                """

                st.markdown(html, unsafe_allow_html=True)

            # ══════════════════════════════════════════════
            # TABELA 2: Validações CRUZADAS (Total vs Por Veículo)
            # ══════════════════════════════════════════════
            if len(resultados_cruzados) > 0:
                st.markdown(
                    "#### 🔗 Validação Cruzada: Total Anual Consolidado vs Σ Veículos"
                )
                st.caption(
                    "Compara o universo anual consolidado, sem filtros de tela, "
                    "com a soma agregada por veículo."
                )

                html2 = f"""
                <table class="validacao-table">
                    <thead>
                        <tr>
                            <th style="width: 50px;">Status</th>
                            <th style="text-align: left;">Comparação</th>
                            <th>Total</th>
                            <th>Σ Veículos</th>
                            <th>Diferença ({simbolo})</th>
                            <th>Erro %</th>
                        </tr>
                    </thead>
                    <tbody>
                """

                for res in resultados_cruzados:
                    status_class = "status-ok" if res['status'] == "✅" else ("status-warn" if res['status'] == "⚠️" else "status-erro")
                    if abs(res['diff_valor']) < 0.01:
                        diff_class = "diff-zero"
                    elif res['diff_valor'] > 0:
                        diff_class = "diff-positivo"
                    else:
                        diff_class = "diff-negativo"

                    html2 += f"""
                    <tr>
                        <td style="text-align: center; font-size: 16px;">{res['status']}</td>
                        <td style="text-align: left;"><strong>{res['icone']} {res['nome']}</strong></td>
                        <td class="valor-num">{simbolo}{res['total_direto']:,.2f}{sufixo}</td>
                        <td class="valor-num">{simbolo}{res['total_periodo']:,.2f}{sufixo}</td>
                        <td class="valor-num {diff_class}">{res['diff_valor']:+,.2f}{sufixo}</td>
                        <td class="valor-num {status_class}">{res['diff_perc']:+.2f}%</td>
                    </tr>
                    """

                html2 += """
                    </tbody>
                </table>
                """

                st.markdown(html2, unsafe_allow_html=True)

                # Resumo combinado
                st.markdown("---")
                all_results = resultados + resultados_cruzados
                ok_count = sum(1 for r in all_results if r['status'] == "✅")
                warn_count = sum(1 for r in all_results if r['status'] == "⚠️")
                erro_count = sum(1 for r in all_results if r['status'] == "❌")

                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("✅ Consistentes", f"{ok_count}/{len(all_results)}")
                with col_s2:
                    st.metric("⚠️ Pequenas Dif.", f"{warn_count}/{len(all_results)}")
                with col_s3:
                    st.metric("❌ Inconsistentes", f"{erro_count}/{len(all_results)}")

                if erro_count == 0 and warn_count == 0:
                    st.success("🎉 **Todos os datasets estão consistentes!** A integridade dos dados está garantida.")
                elif erro_count == 0:
                    st.info("ℹ️ Pequenas diferenças detectadas (< 1%) - provavelmente arredondamento.")
                else:
                    st.error(f"⚠️ {erro_count} validação(ões) com erro - investigar processamento.")

            elif len(resultados) > 0:
                # Só validações internas, sem cruzadas
                st.markdown("---")
                ok_count = sum(1 for r in resultados if r['status'] == "✅")
                warn_count = sum(1 for r in resultados if r['status'] == "⚠️")
                erro_count = sum(1 for r in resultados if r['status'] == "❌")

                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("✅ Consistentes", f"{ok_count}/{len(resultados)}")
                with col_s2:
                    st.metric("⚠️ Pequenas Dif.", f"{warn_count}/{len(resultados)}")
                with col_s3:
                    st.metric("❌ Inconsistentes", f"{erro_count}/{len(resultados)}")

                if erro_count == 0 and warn_count == 0:
                    st.success("🎉 **Todos os datasets estão consistentes!** A integridade dos dados está garantida.")
                elif erro_count == 0:
                    st.info("ℹ️ Pequenas diferenças detectadas (< 1%) - provavelmente arredondamento.")
                else:
                    st.error(f"⚠️ {erro_count} validação(ões) com erro - investigar processamento.")

            else:
                st.warning("⚠️ Nenhum dataset disponível para validação.")

        except Exception as e:
            st.error(f"❌ Erro ao calcular validação: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

    # ════════════════════════════════════════
    # 🔍 VALIDAÇÃO EXCEL × SCI (Fontes vs Calculados)
    # ════════════════════════════════════════
    with st.expander("🔍 Validação de Consistência Excel × SCI (Fontes vs Calculados)", expanded=False):
        st.markdown("""
        <div style="padding: 0.8rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 8px; margin-bottom: 1rem; color: white;">
        <h4 style="color: white; margin: 0; font-size: 1rem;">🔍 Validação Automática: Excel (fonte) × Parquets (SCI)</h4>
        <p style="color: #f0f0f0; margin: 0.3rem 0 0 0; font-size: 0.85rem;">
            Confere se os dados calculados pelo SCI estão consistentes com os dados brutos do Excel
        </p>
        </div>
        """, unsafe_allow_html=True)

        # Caminho do Excel
        _excel_path_val = os.path.join(_DATA_ROOT, 'TC_Principal', str(ano), 'Reporting veículos.xlsx')

        if not os.path.exists(_excel_path_val):
            st.warning(f"⚠️ Arquivo Excel não encontrado: `{_excel_path_val}`")
            st.info("Para executar esta validação, o arquivo 'Reporting veículos.xlsx' deve estar presente na pasta do ano.")
        else:
            st.success(f"✅ Excel encontrado: `Reporting veículos.xlsx` (Ano {ano})")

            if executar_conferencias is None:
                st.info(
                    "Validação Excel × SCI indisponível neste runtime. "
                    "Publique também o módulo de processamento para habilitar esta função."
                )
            elif st.button("🔄 Executar Validação Excel × SCI", key="btn_val_excel_sci_home", use_container_width=True):
                with st.spinner("Executando conferências..."):
                    # ── Budget ──
                    st.markdown("#### 📊 Budget")
                    df_conf_bud = executar_conferencias(ano, 'budget')
                    _ok_b = (df_conf_bud['Status'] == '✅').sum()
                    _total_b = len(df_conf_bud)
                    st.dataframe(df_conf_bud, width="stretch", hide_index=True)
                    if _ok_b == _total_b:
                        st.success(f"🎉 Budget: {_ok_b}/{_total_b} conferências OK")
                    else:
                        st.warning(f"⚠️ Budget: {_ok_b}/{_total_b} conferências OK")

                    st.markdown("---")

                    # ── Real ──
                    st.markdown("#### 📊 Real")
                    df_conf_real = executar_conferencias(ano, 'real')
                    _ok_r = (df_conf_real['Status'] == '✅').sum()
                    _total_r = len(df_conf_real)
                    st.dataframe(df_conf_real, width="stretch", hide_index=True)
                    if _ok_r == _total_r:
                        st.success(f"🎉 Real: {_ok_r}/{_total_r} conferências OK")
                    else:
                        st.warning(f"⚠️ Real: {_ok_r}/{_total_r} conferências OK")

        st.markdown("""
        ---
        **Legenda:** ✅ OK (< 0,01%) | ⚠️ Atenção (0,01% - 1%) | ❌ Divergência (> 1%)

        **Conferências:** Despesa Primária (fonte real), Redis, Volume FA, Custo FA/FP (BDG), Prova cruzada DP=FA+FP.
        """)

