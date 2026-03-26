"""Tab 5: Dados Detalhados — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    COLUNAS_BE_DETALHADO, COLUNAS_BE_DETALHADO_VEICULO,
    reordenar_colunas_be, download_excel_button,
    load_tc_sapiens, load_forecast_completo,
    load_dea_dedicado_real,
    load_percentual_rateio_veiculos_real, ratear_be_por_veiculo,
    normalizar_periodo,
    aplicar_fator_df, converter_moeda_df,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    _pivotar_detalhado, _pivotar_flex, render_secao_tabela_detalhe,
)
from tc_principal.ui_components import aplicar_filtros
from tc_principal.pages._home_tabs.data_helpers import _resumo_por_veiculo, _DATA_ROOT


def render(ctx):
    """Renderiza a aba Dados Detalhados."""
    # ── Desempacotar contexto ──
    ano = ctx.ano
    moeda = ctx.moeda
    simbolo = ctx.simbolo
    taxas = ctx.taxas
    fator = ctx.fator
    sufixo = ctx.sufixo
    tem_ano_df = ctx.tem_ano_df
    df = ctx.df
    df_bud = ctx.df_bud
    df_real = ctx.df_real
    df_flex = ctx.df_flex
    df_flex_det = ctx.df_flex_det
    df_vol_bud = ctx.df_vol_bud
    df_vol_actual = ctx.df_vol_actual
    df_veic_bud_raw = ctx.df_veic_bud_raw
    df_veic_real_raw = ctx.df_veic_real_raw
    _raw_df_be = ctx._raw_df_be
    _raw_df_vol_bud = ctx._raw_df_vol_bud
    _raw_df_vol_actual = ctx._raw_df_vol_actual
    _get_be_full = ctx._get_be_full
    _get_veic_be_raw = ctx._get_veic_be_raw
    filtros_sel = ctx.filtros_sel

    st.subheader("📋 Dados Detalhados")

    # Seletor de visualização: Total ou Fixo/Variável
    _col_viz, _ = st.columns([1.3, 3])
    with _col_viz:
        modo_tab6 = st.radio(
            "📊 **Visualização:**",
            ["Total", "Fixo/Variável"],
            index=0, horizontal=True,
            key="home_tab6_viz",
        )

    col_valor_tab6 = 'Custo FP'

    # ═══════════════════════════════════════════════════════
    # 📊 TABELAS TC TOTAL
    # ═══════════════════════════════════════════════════════
    st.markdown("## 📊 Tabelas TC Total")

    # Tabela — Budget Total
    piv_bud, ofc_bud = _pivotar_detalhado(df_bud, col_valor_tab6)
    render_secao_tabela_detalhe(
        piv_bud, ofc_bud, "Budget Total", "💰",
        "home_bud", ano, simbolo, sufixo,
        expanded=True, modo=modo_tab6,
    )

    # Tabela — Flex Budget Total
    # Tabela — Flex Budget Total
    _piv_flex_total = None  # para download consolidado
    if df_flex_det is not None and not df_flex_det.empty:
        piv_flex_d, ofc_flex_d = _pivotar_detalhado(
            df_flex_det, 'Flex_Bud',
        )
        _piv_flex_total = piv_flex_d
        render_secao_tabela_detalhe(
            piv_flex_d, ofc_flex_d, "Flex Budget", "📐",
            "home_flex", ano, simbolo, sufixo,
            expanded=False, modo=modo_tab6,
        )
    else:
        piv_flex, ofc_flex = _pivotar_flex(df_flex)
        _piv_flex_total = piv_flex
        render_secao_tabela_detalhe(
            piv_flex, ofc_flex, "Flex Budget", "📐",
            "home_flex", ano, simbolo, sufixo,
            expanded=False, modo=modo_tab6,
        )

    # Tabela — Real Total
    piv_real, ofc_real = _pivotar_detalhado(df, col_valor_tab6)
    render_secao_tabela_detalhe(
        piv_real, ofc_real, "Real Total", "✅",
        "home_real", ano, simbolo, sufixo,
        expanded=False, modo=modo_tab6,
    )

    # Tabela — Best Estimate Total
    # IMPORTANTE: Usar mesmos filtros da Tab 1 (sidebar) para consistência
    # EXCETO filtro de veículo (para permitir rateio correto posteriormente)
    _df_be_tab6 = None
    _df_be_tab6_com_filtro_veiculo = None
    try:
        _be_tab6_tmp = _get_be_full()
        _be_tab6_src = _be_tab6_tmp if _be_tab6_tmp is not None else _raw_df_be
        if _be_tab6_src is not None and not _be_tab6_src.empty:
            # Criar filtros SEM veículo para BE Por Veículo poder ratear
            filtros_sem_veiculo = {k: v for k, v in filtros_sel.items() if k != 'Veículo'}

            _fc = _be_tab6_src.copy()
            _fc = aplicar_filtros(_fc, filtros_sem_veiculo)

            if not _fc.empty:
                _cv = [c for c in COLUNAS_MONETARIAS if c in _fc.columns]
                _fc = aplicar_fator_df(_fc, _cv, fator)
                _fc = converter_moeda_df(_fc, _cv, moeda, taxas)
                _df_be_tab6 = _fc

                # Para tabela BE Total, aplicar também filtro de veículo
                if 'Veículo' in filtros_sel and filtros_sel['Veículo']:
                    _df_be_tab6_com_filtro_veiculo = aplicar_filtros(_fc.copy(), {'Veículo': filtros_sel['Veículo']})
                else:
                    _df_be_tab6_com_filtro_veiculo = _fc.copy()
    except Exception:
        pass

    _df_be_para_tabela = _df_be_tab6_com_filtro_veiculo if _df_be_tab6_com_filtro_veiculo is not None else _df_be_tab6
    if _df_be_para_tabela is not None:
        piv_be, ofc_be = _pivotar_detalhado(
            _df_be_para_tabela, col_valor_tab6,
        )
        render_secao_tabela_detalhe(
            piv_be, ofc_be, "Best Estimate", "🔮",
            "home_be", ano, simbolo, sufixo,
            expanded=False, modo=modo_tab6,
        )

    # ── Download consolidado: 1 Excel com 4 abas (TC Total) ──
    try:
        _sheets: list[tuple[str, pd.DataFrame]] = []
        _sheets.append(("Budget_Total", piv_bud))
        if _piv_flex_total is not None:
            _sheets.append(("Flex_Budget", _piv_flex_total))
        _sheets.append(("Real_Total", piv_real))
        if _df_be_para_tabela is not None:
            _sheets.append(("Best_Estimate", piv_be))

        if _sheets:
            _buf_tc = BytesIO()
            with pd.ExcelWriter(_buf_tc, engine='openpyxl') as _w:
                for _nome_aba, _df_aba in _sheets:
                    if _df_aba is not None and not _df_aba.empty:
                        _df_aba.to_excel(_w, index=False, sheet_name=_nome_aba[:31])
            st.download_button(
                "📥 Baixar Tabelas TC Total (Excel)",
                data=_buf_tc.getvalue(),
                file_name=f"TC_Total_{ano}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_tc_total_{ano}",
                use_container_width=True,
            )
    except Exception:
        pass

    # ── Download consolidado: só TOTAIS sem divisão por oficina ──
    try:
        def _total_sem_oficina(piv):
            """Re-agrega pivotado removendo Oficina (como o expander TOTAL)."""
            if piv is None or piv.empty:
                return None
            if 'Oficina' not in piv.columns:
                return piv
            _dim = [c for c in ['Type 05', 'Type 06', 'Account', 'Custo']
                    if c in piv.columns]
            _num = [c for c in piv.columns if c not in _dim and c != 'Oficina']
            _df = piv[~piv['Oficina'].isin(['TOTAL', ''])].copy()
            if _df.empty or not _dim:
                return None
            _agg = _df.groupby(_dim, as_index=False, dropna=False)[_num].sum()
            _meses = [c for c in _num if c != 'Total']
            if _meses:
                _agg['Total'] = _agg[_meses].sum(axis=1)
            _agg = _agg.loc[_agg[_num].abs().sum(axis=1) > 0.005]
            return _agg if not _agg.empty else None

        _sheets_tot: list[tuple[str, pd.DataFrame]] = []
        _t = _total_sem_oficina(piv_bud)
        if _t is not None:
            _sheets_tot.append(("Budget_Total", _t))
        if _piv_flex_total is not None:
            _t = _total_sem_oficina(_piv_flex_total)
            if _t is not None:
                _sheets_tot.append(("Flex_Budget", _t))
        _t = _total_sem_oficina(piv_real)
        if _t is not None:
            _sheets_tot.append(("Real_Total", _t))
        if _df_be_para_tabela is not None:
            _t = _total_sem_oficina(piv_be)
            if _t is not None:
                _sheets_tot.append(("Best_Estimate", _t))

        if _sheets_tot:
            _buf_tot = BytesIO()
            with pd.ExcelWriter(_buf_tot, engine='openpyxl') as _wt:
                for _nome_aba, _df_aba in _sheets_tot:
                    _df_aba.to_excel(_wt, index=False, sheet_name=_nome_aba[:31])
            st.download_button(
                "📥 Baixar Totais TC (sem Oficina) (Excel)",
                data=_buf_tot.getvalue(),
                file_name=f"TC_Totais_{ano}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_tc_totais_{ano}",
                use_container_width=True,
            )
    except Exception:
        pass

    # ═══════════════════════════════════════════════════════
    # 🚗 TABELAS TC POR VEÍCULOS
    # ═══════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 🚗 Tabelas TC Por Veículos")

    # ── Budget por Veículo ──
    veiculos_bud = []
    _df_veic_bud_tab6 = None
    if df_veic_bud_raw is not None:
        _vb = df_veic_bud_raw.copy()
        if 'Custo FP Veiculo' in _vb.columns:
            _vb['Custo FP'] = _vb['Custo FP Veiculo']
        _cv_vb = [c for c in COLUNAS_MONETARIAS if c in _vb.columns]
        _vb = aplicar_fator_df(_vb, _cv_vb, fator)
        _vb = converter_moeda_df(_vb, _cv_vb, moeda, taxas)
        if not _vb.empty:
            _df_veic_bud_tab6 = _vb

    with st.expander("💰 Budget Por Veículo", expanded=False):
        if _df_veic_bud_tab6 is not None and 'Veículo' in _df_veic_bud_tab6.columns:
            veiculos_bud = sorted(
                _df_veic_bud_tab6['Veículo'].dropna().unique()
            )
            _resumo_por_veiculo(st, _df_veic_bud_tab6, col_valor_tab6)
            for veic in veiculos_bud:
                _dv = _df_veic_bud_tab6[
                    _df_veic_bud_tab6['Veículo'] == veic
                ].copy()
                if _dv.empty:
                    continue
                piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                render_secao_tabela_detalhe(
                    piv_v, ofc_v,
                    f"Budget — {veic}", "🚗",
                    f"home_vbud_{veic}", ano, simbolo, sufixo,
                    expanded=False, modo=modo_tab6,
                )
        else:
            st.info("ℹ️ Dados de Budget por veículo não disponíveis.")

    # ── Flex Budget por Veículo ──
    _flex_resumo_parts = []
    with st.expander("📐 Flex Budget Por Veículo", expanded=False):
        if _df_veic_bud_tab6 is not None and 'Veículo' in _df_veic_bud_tab6.columns:
            _flex_resumo_ph = st.empty()
            for veic in veiculos_bud:
                _dv_fb = _df_veic_bud_tab6[
                    _df_veic_bud_tab6['Veículo'] == veic
                ].copy()
                if _dv_fb.empty:
                    continue
                # Calcular flex budget por veículo (detalhado)
                _vol_bud_v = None
                _vol_act_v = None
                if _raw_df_vol_bud is not None and 'Veículo' in _raw_df_vol_bud.columns:
                    _vol_bud_v = _raw_df_vol_bud[
                        _raw_df_vol_bud['Veículo'] == veic
                    ].copy()
                if _raw_df_vol_actual is not None and 'Veículo' in _raw_df_vol_actual.columns:
                    _vol_act_v = _raw_df_vol_actual[
                        _raw_df_vol_actual['Veículo'] == veic
                    ].copy()
                # Tentar versão detalhada (preserva dimensões)
                _fx_v_det = calcular_flex_budget_detalhado(
                    _dv_fb, _vol_bud_v, _vol_act_v,
                    col_custo='Custo FP',
                    tem_ano='Ano' in _dv_fb.columns,
                )
                if _fx_v_det is not None and not _fx_v_det.empty:
                    _fx_copy = _fx_v_det.copy()
                    _fx_copy['Veículo'] = veic
                    _flex_resumo_parts.append(_fx_copy)
                    piv_fv, ofc_fv = _pivotar_detalhado(
                        _fx_v_det, 'Flex_Bud',
                    )
                else:
                    # Fallback: versão agregada
                    _fx_v = calcular_flex_budget(
                        _dv_fb, _vol_bud_v, _vol_act_v,
                        tem_ano='Ano' in _dv_fb.columns,
                    )
                    if _fx_v is not None and not _fx_v.empty:
                        _fx_copy = _fx_v.copy()
                        _fx_copy['Veículo'] = veic
                        _flex_resumo_parts.append(_fx_copy)
                        piv_fv, ofc_fv = _pivotar_flex(_fx_v)
                    else:
                        continue
                render_secao_tabela_detalhe(
                    piv_fv, ofc_fv,
                    f"Flex Budget — {veic}", "📐",
                    f"home_vflex_{veic}", ano, simbolo, sufixo,
                    expanded=False, modo=modo_tab6,
                )
            if _flex_resumo_parts:
                _flex_all = pd.concat(_flex_resumo_parts, ignore_index=True)
                with _flex_resumo_ph.container():
                    _resumo_por_veiculo(st, _flex_all, 'Flex_Bud')
        else:
            st.info("ℹ️ Dados de Flex Budget por veículo não disponíveis.")

    # ── Real por Veículo ──
    _df_veic_real_tab6 = None
    if df_veic_real_raw is not None:
        _vr = df_veic_real_raw.copy()
        if 'Custo FP Veiculo' in _vr.columns:
            _vr['Custo FP'] = _vr['Custo FP Veiculo']
        _cv_vr = [c for c in COLUNAS_MONETARIAS if c in _vr.columns]
        _vr = aplicar_fator_df(_vr, _cv_vr, fator)
        _vr = converter_moeda_df(_vr, _cv_vr, moeda, taxas)
        if not _vr.empty:
            _df_veic_real_tab6 = _vr

    with st.expander("✅ Real Por Veículo", expanded=False):
        if _df_veic_real_tab6 is not None and 'Veículo' in _df_veic_real_tab6.columns:
            veiculos_real = sorted(
                _df_veic_real_tab6['Veículo'].dropna().unique()
            )
            _resumo_por_veiculo(st, _df_veic_real_tab6, col_valor_tab6)
            for veic in veiculos_real:
                _dv = _df_veic_real_tab6[
                    _df_veic_real_tab6['Veículo'] == veic
                ].copy()
                if _dv.empty:
                    continue
                piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                render_secao_tabela_detalhe(
                    piv_v, ofc_v,
                    f"Real — {veic}", "🚗",
                    f"home_vreal_{veic}", ano, simbolo, sufixo,
                    expanded=False, modo=modo_tab6,
                )
        else:
            st.info("ℹ️ Dados Real por veículo não disponíveis.")

    # ── Best Estimate por Veículo ──
    # PRIORIDADE: Usar arquivo pré-gerado (forecast_veiculos_custo_fp.parquet)
    # FALLBACK: Ratear em runtime usando percentuais Real
    _df_be_veic_tab6 = None
    with st.expander("🔮 Best Estimate Por Veículo", expanded=False):

        # Tentar usar arquivo pré-gerado (igual Budget/Real)
        _veic_be_raw_t6 = _get_veic_be_raw()
        if _veic_be_raw_t6 is not None and not _veic_be_raw_t6.empty:
            _vbe = normalizar_periodo(_veic_be_raw_t6.copy())
            if 'Ano' in _vbe.columns:
                _vbe = _vbe[_vbe['Ano'] == int(ano)].copy()

            # Aplicar filtros (exceto Veículo para mostrar todos)
            filtros_sem_veiculo = {k: v for k, v in filtros_sel.items() if k != 'Veículo'}
            _vbe = aplicar_filtros(_vbe, filtros_sem_veiculo)

            if not _vbe.empty:
                # Usar 'Custo FP Veiculo' como 'Custo FP'
                if 'Custo FP Veiculo' in _vbe.columns:
                    _vbe['Custo FP'] = _vbe['Custo FP Veiculo']

                # Aplicar fator/moeda
                _cv_vbe = [c for c in COLUNAS_MONETARIAS if c in _vbe.columns]
                _vbe = aplicar_fator_df(_vbe, _cv_vbe, fator)
                _vbe = converter_moeda_df(_vbe, _cv_vbe, moeda, taxas)
                _df_be_veic_tab6 = _vbe

        # Usar arquivo pré-gerado se disponível
        if _df_be_veic_tab6 is not None and 'Veículo' in _df_be_veic_tab6.columns:
            veiculos_be = sorted(_df_be_veic_tab6['Veículo'].dropna().unique())

            if len(veiculos_be) > 0:
                _resumo_por_veiculo(st, _df_be_veic_tab6, col_valor_tab6)
                for veic in veiculos_be:
                    _dv = _df_be_veic_tab6[_df_be_veic_tab6['Veículo'] == veic].copy()
                    if _dv.empty:
                        continue
                    piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                    render_secao_tabela_detalhe(
                        piv_v, ofc_v,
                        f"Best Estimate — {veic}", "🔮",
                        f"home_vbe_{veic}", ano, simbolo, sufixo,
                        expanded=False, modo=modo_tab6,
                    )
            else:
                st.info("ℹ️ Nenhum veículo encontrado nos dados de BE.")

        # Fallback: ratear em runtime se arquivo não existe
        elif _df_be_tab6 is not None and not _df_be_tab6.empty:
            st.info("ℹ️ Arquivo com veículo não encontrado. Aplicando rateio...")

            _df_be_para_ratear = _df_be_tab6.drop(columns=['Veículo'], errors='ignore')
            _pct_real = load_percentual_rateio_veiculos_real(ano)
            _dea_real = load_dea_dedicado_real(ano)

            if _pct_real is None or _pct_real.empty:
                st.warning(
                    "⚠️ Percentuais de rateio Real não encontrados. "
                    "Execute o processamento Real ou regenere o Forecast."
                )
            else:
                _df_be_veic = ratear_be_por_veiculo(_df_be_para_ratear, _pct_real, df_dea=_dea_real)

                if _df_be_veic is not None and 'Veículo' in _df_be_veic.columns:
                    if 'Custo FP Veiculo' in _df_be_veic.columns:
                        _df_be_veic['Custo FP'] = _df_be_veic['Custo FP Veiculo']

                    veiculos_be = sorted(_df_be_veic['Veículo'].dropna().unique())
                    _resumo_por_veiculo(st, _df_be_veic, col_valor_tab6)

                    for veic in veiculos_be:
                        _dv = _df_be_veic[_df_be_veic['Veículo'] == veic].copy()
                        if _dv.empty:
                            continue
                        piv_v, ofc_v = _pivotar_detalhado(_dv, col_valor_tab6)
                        render_secao_tabela_detalhe(
                            piv_v, ofc_v,
                            f"Best Estimate — {veic}", "🔮",
                            f"home_vbe_{veic}", ano, simbolo, sufixo,
                            expanded=False, modo=modo_tab6,
                        )
                else:
                    st.error(
                        "❌ Erro ao ratear BE por veículo. "
                        "Regenere o Forecast no BE Simulador."
                    )
        else:
            st.info("ℹ️ Dados de BE por veículo não disponíveis. Gere um Forecast primeiro.")

    # ── Download consolidado: 1 Excel com abas (TC Por Veículos) ──
    try:
        def _pivot_veic_sheet(df_raw, col_val):
            """Pivota por veículo e concatena com coluna Veículo."""
            if df_raw is None or df_raw.empty or 'Veículo' not in df_raw.columns:
                return None
            parts = []
            for v in sorted(df_raw['Veículo'].dropna().unique()):
                _dv = df_raw[df_raw['Veículo'] == v]
                pv, _ = _pivotar_detalhado(_dv, col_val)
                if not pv.empty:
                    pv.insert(0, 'Veículo', v)
                    parts.append(pv)
            return pd.concat(parts, ignore_index=True) if parts else None

        _sheets_veic: list[tuple[str, pd.DataFrame]] = []
        _piv_bud_veic = _pivot_veic_sheet(_df_veic_bud_tab6, col_valor_tab6)
        if _piv_bud_veic is not None:
            _sheets_veic.append(("Budget_Veiculos", _piv_bud_veic))

        if _flex_resumo_parts:
            _flex_all_dl = pd.concat(_flex_resumo_parts, ignore_index=True)
            _piv_flex_veic = _pivot_veic_sheet(_flex_all_dl, 'Flex_Bud')
            if _piv_flex_veic is not None:
                _sheets_veic.append(("Flex_Budget_Veiculos", _piv_flex_veic))

        _piv_real_veic = _pivot_veic_sheet(_df_veic_real_tab6, col_valor_tab6)
        if _piv_real_veic is not None:
            _sheets_veic.append(("Real_Veiculos", _piv_real_veic))

        _df_be_veic_dl = _df_be_veic_tab6 if _df_be_veic_tab6 is not None else None
        _piv_be_veic = _pivot_veic_sheet(_df_be_veic_dl, col_valor_tab6)
        if _piv_be_veic is not None:
            _sheets_veic.append(("Best_Estimate_Veiculos", _piv_be_veic))

        if _sheets_veic:
            _buf_veic = BytesIO()
            with pd.ExcelWriter(_buf_veic, engine='openpyxl') as _wv:
                for _nome_aba, _df_aba in _sheets_veic:
                    if _df_aba is not None and not _df_aba.empty:
                        _df_aba.to_excel(_wv, index=False, sheet_name=_nome_aba[:31])
            st.download_button(
                "📥 Baixar Tabelas TC Por Veículos (Excel)",
                data=_buf_veic.getvalue(),
                file_name=f"TC_Veiculos_{ano}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_tc_veiculos_{ano}",
                use_container_width=True,
            )
    except Exception:
        pass

    # ═══════════════════════════════════════════════════════
    # 📑 TABELA TC SAPIENS (dados detalhados com todas as colunas)
    # ═══════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 📑 Dados Sapiens Detalhados")

    df_sapiens = load_tc_sapiens(ano)
    if df_sapiens is not None and not df_sapiens.empty:
        with st.expander("📑 Tabela Sapiens — Todas as Colunas", expanded=False):
            # ── Filtros locais ──
            _flt_c1, _flt_c2, _flt_c3 = st.columns(3)
            with _flt_c1:
                _ofc_opts = sorted(df_sapiens['Oficina'].dropna().unique()) if 'Oficina' in df_sapiens.columns else []
                _ofc_sel = st.multiselect(
                    "🏭 Oficina", _ofc_opts, default=[], key="sap_oficina",
                )
            with _flt_c2:
                _per_opts = sorted(df_sapiens['Período'].dropna().unique()) if 'Período' in df_sapiens.columns else []
                _per_sel = st.multiselect(
                    "📅 Período", _per_opts, default=[], key="sap_periodo",
                )
            with _flt_c3:
                _t05_opts = sorted(df_sapiens['Type 05'].dropna().unique()) if 'Type 05' in df_sapiens.columns else []
                _t05_sel = st.multiselect(
                    "📂 Type 05", _t05_opts, default=[], key="sap_type05",
                )

            _df_sap_filt = df_sapiens.copy()
            if _ofc_sel:
                _df_sap_filt = _df_sap_filt[_df_sap_filt['Oficina'].isin(_ofc_sel)]
            if _per_sel:
                _df_sap_filt = _df_sap_filt[_df_sap_filt['Período'].isin(_per_sel)]
            if _t05_sel:
                _df_sap_filt = _df_sap_filt[_df_sap_filt['Type 05'].isin(_t05_sel)]

            st.caption(f"📊 {len(_df_sap_filt):,} linhas × {len(_df_sap_filt.columns)} colunas")
            _df_sap_filt = reordenar_colunas_be(_df_sap_filt)
            st.dataframe(_df_sap_filt, width="stretch", height=500)

            # ── Download Excel (cacheado) ──
            download_excel_button(
                st, _df_sap_filt,
                "📥 Baixar Sapiens Detalhado (Excel)",
                f"TC_Sapiens_Detalhado_{ano}.xlsx",
                "dl_sapiens_det",
            )
    else:
        st.info(
            "ℹ️ Dados Sapiens detalhados não disponíveis. "
            "Execute o processamento Real na página **Extração de Dados** para gerar."
        )

    # ═══════════════════════════════════════════════════════
    # 🪄 DADOS BE DETALHADOS
    # ═══════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 🪄 Dados BE Detalhados")
    try:
        _df_be_raw = load_forecast_completo()
        if _df_be_raw is not None and not _df_be_raw.empty:
            _df_be_raw = _df_be_raw.copy()
            if 'Custo FP' in _df_be_raw.columns:
                _df_be_raw['Total'] = pd.to_numeric(_df_be_raw['Custo FP'], errors='coerce').fillna(0.0)
            _df_be_raw = reordenar_colunas_be(_df_be_raw)
            _colunas_finais = [c for c in COLUNAS_BE_DETALHADO if c in _df_be_raw.columns]
            _df_be_display = _df_be_raw[_colunas_finais]
            with st.expander("🪄 Tabela BE — Todos os Dados", expanded=False):
                _be2 = _df_be_display
                _bc1, _bc2, _bc3 = st.columns(3)
                with _bc1:
                    _be2_ofc = sorted(_be2['Oficina'].dropna().unique()) if 'Oficina' in _be2.columns else []
                    _be2_ofc_s = st.multiselect("🏭 Oficina", _be2_ofc, default=[], key="be_det_oficina_tc")
                with _bc2:
                    _be2_per = sorted(_be2['Período'].dropna().unique()) if 'Período' in _be2.columns else []
                    _be2_per_s = st.multiselect("📅 Período", _be2_per, default=[], key="be_det_periodo_tc")
                with _bc3:
                    _be2_tipo = sorted(_be2['Tipo'].dropna().unique()) if 'Tipo' in _be2.columns else []
                    _be2_tipo_s = st.multiselect("🏷️ Tipo", _be2_tipo, default=[], key="be_det_tipo_tc")
                if _be2_ofc_s:
                    _be2 = _be2[_be2['Oficina'].isin(_be2_ofc_s)]
                if _be2_per_s:
                    _be2 = _be2[_be2['Período'].isin(_be2_per_s)]
                if _be2_tipo_s:
                    _be2 = _be2[_be2['Tipo'].isin(_be2_tipo_s)]
                st.caption(f"📊 {len(_be2):,} linhas × {len(_be2.columns)} colunas")
                st.dataframe(_be2, width="stretch", height=500)
                _hoje = datetime.now().strftime('%Y%m%d')
                download_excel_button(
                    st, _df_be_raw,
                    "📥 Baixar BE Detalhado (Excel)",
                    f"TC_Principal_BE_Detalhado_{ano}_{_hoje}.xlsx",
                    "dl_be_det_tc",
                )
                # Botão para baixar custos específicos
                _custos_esp_path = os.path.join(
                    _DATA_ROOT, "TC_Principal", "Forecast", "custos_especificos.parquet"
                )
                if os.path.exists(_custos_esp_path):
                    try:
                        _df_custos_esp = pd.read_parquet(_custos_esp_path)
                        if not _df_custos_esp.empty:
                            if 'Descricao' in _df_custos_esp.columns:
                                _df_custos_esp = _df_custos_esp.rename(columns={'Descricao': 'Texto breve'})
                            download_excel_button(
                                st, _df_custos_esp,
                                "📥 Baixar Custos Específicos (Excel)",
                                f"TC_Principal_Custos_Especificos_{ano}_{_hoje}.xlsx",
                                "dl_custos_esp_home_tc",
                            )
                    except Exception:
                        pass
        else:
            st.info("ℹ️ Nenhum dado BE encontrado. Gere um Forecast no Best Estimate Simulador.")
    except Exception as _e:
        st.error(f"❌ Erro ao carregar BE Detalhados: {_e}")

    # ═══════════════════════════════════════════════════════
    # 🚗 DADOS BE DETALHADOS POR VEÍCULO
    # ═══════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 🚗 Dados BE Detalhados por Veículo")
    try:
        _df_vbe = _get_veic_be_raw()
        if _df_vbe is not None and not _df_vbe.empty:
            _df_vbe = _df_vbe.copy()
            if 'Ano' in _df_vbe.columns:
                _df_vbe = _df_vbe[_df_vbe['Ano'] == int(ano)]
            if 'Custo FP Veiculo' in _df_vbe.columns:
                _df_vbe['Total'] = pd.to_numeric(_df_vbe['Custo FP Veiculo'], errors='coerce').fillna(0.0)
            _df_vbe = reordenar_colunas_be(_df_vbe, COLUNAS_BE_DETALHADO_VEICULO)
            _colunas_vbe = [c for c in COLUNAS_BE_DETALHADO_VEICULO if c in _df_vbe.columns]
            _df_vbe_display = _df_vbe[_colunas_vbe]
            with st.expander("🚗 Tabela BE por Veículo — Todos os Dados", expanded=False):
                _vbe2 = _df_vbe_display
                _vc1, _vc2, _vc3, _vc4 = st.columns(4)
                with _vc1:
                    _vbe_ofc = sorted(_vbe2['Oficina'].dropna().unique()) if 'Oficina' in _vbe2.columns else []
                    _vbe_ofc_s = st.multiselect("🏭 Oficina", _vbe_ofc, default=[], key="be_det_veiculo_oficina_tc")
                with _vc2:
                    _vbe_per = sorted(_vbe2['Período'].dropna().unique()) if 'Período' in _vbe2.columns else []
                    _vbe_per_s = st.multiselect("📅 Período", _vbe_per, default=[], key="be_det_veiculo_periodo_tc")
                with _vc3:
                    _vbe_tipo = sorted(_vbe2['Tipo'].dropna().unique()) if 'Tipo' in _vbe2.columns else []
                    _vbe_tipo_s = st.multiselect("🏷️ Tipo", _vbe_tipo, default=[], key="be_det_veiculo_tipo_tc")
                with _vc4:
                    _vbe_vec = sorted(_vbe2['Veículo'].dropna().unique()) if 'Veículo' in _vbe2.columns else []
                    _vbe_vec_s = st.multiselect("🚗 Veículo", _vbe_vec, default=[], key="be_det_veiculo_veiculo_tc")
                if _vbe_ofc_s:
                    _vbe2 = _vbe2[_vbe2['Oficina'].isin(_vbe_ofc_s)]
                if _vbe_per_s:
                    _vbe2 = _vbe2[_vbe2['Período'].isin(_vbe_per_s)]
                if _vbe_tipo_s:
                    _vbe2 = _vbe2[_vbe2['Tipo'].isin(_vbe_tipo_s)]
                if _vbe_vec_s:
                    _vbe2 = _vbe2[_vbe2['Veículo'].isin(_vbe_vec_s)]
                _total_vbe = len(_vbe2)
                _LIMITE_VBE = 100
                if _total_vbe > _LIMITE_VBE:
                    st.caption(f"📊 Exibindo {_LIMITE_VBE} de {_total_vbe:,} linhas × {len(_vbe2.columns)} colunas  —  use o botão abaixo para baixar tudo")
                    st.dataframe(_vbe2.head(_LIMITE_VBE), width="stretch", height=500)
                else:
                    st.caption(f"📊 {_total_vbe:,} linhas × {len(_vbe2.columns)} colunas")
                    st.dataframe(_vbe2, width="stretch", height=500)
                _hoje_v = datetime.now().strftime('%Y%m%d')
                download_excel_button(
                    st, _df_vbe,
                    "📥 Baixar BE Detalhado por Veículo (Excel)",
                    f"TC_Principal_BE_Detalhado_Veiculo_{ano}_{_hoje_v}.xlsx",
                    "dl_be_det_veiculo_tc",
                )
        else:
            st.info("ℹ️ Nenhum dado BE por veículo encontrado. Gere um Forecast no Best Estimate Simulador.")
    except Exception as _e_v:
        st.error(f"❌ Erro ao carregar BE Detalhados por Veículo: {_e_v}")

