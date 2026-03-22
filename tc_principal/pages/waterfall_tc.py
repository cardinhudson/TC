import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
import os
import sys
from datetime import datetime

# Configurar Plotly para usar o engine JSON padrão
try:
    import json
    pio.json.config.default_engine = 'json'
    if hasattr(pio.json, 'config'):
        pio.json.config.validate = False
except Exception:
    pass

# Adicionar diretório raiz ao path
if hasattr(sys, '_MEIPASS'):
    _ROOT = sys._MEIPASS
else:
    _ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Função helper para exibir gráficos Plotly com tratamento de erro para orjson
def plotly_chart_safe(fig, use_container_width=True):
    """Exibe um gráfico Plotly com tratamento de erro para problemas com orjson."""
    try:
        st.plotly_chart(fig, use_container_width=use_container_width)
    except (AttributeError, TypeError) as e:
        error_msg = str(e)
        if 'orjson' in error_msg or 'OPT_NON_STR_KEYS' in error_msg:
            st.error("⚠️ **Erro ao renderizar gráfico:** Problema com o módulo `orjson`")
            st.markdown("**Solução:** Execute no terminal:")
            st.code("pip install --upgrade --force-reinstall orjson", language="bash")
        else:
            raise

# Importar API estável do tc_exports (funções utilitárias genéricas)
from tc_exports import (
    formatar_periodo_abreviado, formatar_ratio_com_barra, criar_tabela_html_com_barra,
    calcular_resumo_tabela_flex, exibir_caixas_resumo_dinamico, exibir_caixas_resumo,
    converter_moeda, converter_coluna_moeda, obter_simbolo_moeda,
    listar_anos_disponiveis, encontrar_arquivo_parquet,
    carregar_taxas_banco, salvar_taxas_banco, inicializar_banco_taxas,
    reordenar_colunas_padrao
)

# Importar loaders e helpers do TC Veículos
from tc_principal.shared import (
    load_principal, load_principal_real,
    load_volume_bud, load_volume_actual,
    load_custo_fp_veiculo, load_custo_fp_veiculo_real,
    descobrir_anos_tc_principal,
    load_tc_sapiens,
    calcular_flex_budget, normalizar_periodo, ordenar_por_mes,
    mask_custo_fixo, aplicar_fator_df, converter_moeda_df,
    COLUNAS_MONETARIAS,
    load_forecast_completo, load_forecast_volume,
    ratear_be_por_veiculo, load_percentual_rateio_veiculos_real,
    reordenar_colunas_be, download_excel_button,
)
from tc_principal.ui_components import (
    injetar_css_global, render_header, render_sidebar_global, render_inline_summary_metrics,
)

# ── Helpers de carregamento para o Waterfall TC Veículos ─────────────
def _agregar_sem_veiculo(df):
    """Remove dimensão Veículo, agregando colunas numéricas.
    Usado quando o usuário escolhe 'Todos (TC Total)'."""
    if df is None or df.empty or 'Veículo' not in df.columns:
        return df
    # Colunas que são numéricas mas representam identificação/dimensão,
    # não devem ser somadas (ex: Ano=2026 viraria 20260).
    _force_id = {'Ano', 'Mes', 'Mês', 'Ano_num'}
    cols_id = [c for c in df.columns
               if c != 'Veículo' and (not pd.api.types.is_numeric_dtype(df[c]) or c in _force_id)]
    cols_num = [c for c in df.columns
                if pd.api.types.is_numeric_dtype(df[c]) and c not in _force_id]
    if not cols_num:
        return df.drop(columns=['Veículo'])
    return df.groupby(cols_id, as_index=False, dropna=False)[cols_num].sum()


def _render_type06_block(df_type06, type06, tipo_visualizacao, fator_conversao):
    """Renderiza um bloco Type 06 (cabeçalho + tabela Account) dentro de um expander Type 05.
    Retorna True se renderizou algo, False caso contrário."""
    import traceback as _tb
    if df_type06 is None or df_type06.empty:
        return False

    # Verificar valores não zerados
    colunas_numericas_check = [col for col in df_type06.columns
                               if pd.api.types.is_numeric_dtype(df_type06[col])
                               and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
    if colunas_numericas_check:
        df_check = df_type06[colunas_numericas_check].fillna(0)
        if not (df_check.abs().sum(axis=1) > 0.0001).any():
            return False
    else:
        if 'Mês 2' in df_type06.columns:
            if df_type06['Mês 2'].fillna(0).abs().sum() <= 0.0001:
                return False

    total_type06 = df_type06['Mês 2'].sum() if 'Mês 2' in df_type06.columns else 0.0
    total_type06_fmt = f"{total_type06:,.2f}"

    # Filtrar linhas zeradas
    colunas_num = [col for col in df_type06.columns
                   if pd.api.types.is_numeric_dtype(df_type06[col])
                   and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
    if colunas_num:
        _temp = df_type06[colunas_num].fillna(0)
        df_filt = df_type06[_temp.abs().sum(axis=1) > 0.0001].copy()
    else:
        df_filt = df_type06.copy()

    if df_filt.empty:
        return False

    # Coluna ID de exibição
    if 'Account' in df_filt.columns:
        colunas_id = ['Account']
    elif 'Type 06' in df_filt.columns:
        colunas_id = ['Type 06']
    else:
        colunas_id = []

    colunas_numericas_disp = [col for col in df_filt.columns
                              if col not in colunas_id
                              and col not in ['Type 05', 'Type 06', 'Account', 'Custo', 'Período']]
    colunas_ordenadas = reordenar_colunas_padrao(colunas_numericas_disp)
    colunas_display = colunas_id + colunas_ordenadas
    # Garantir que todas as colunas existem
    colunas_display = [c for c in colunas_display if c in df_filt.columns]
    df_display = df_filt[colunas_display].copy()

    # Formatar valores
    for col in df_display.columns:
        if col not in colunas_id:
            if col.startswith('%'):
                df_display[col] = df_display[col].map(
                    lambda x: formatar_ratio_com_barra(x / 100) if pd.notna(x) and isinstance(x, (int, float)) else "-")
            elif pd.api.types.is_numeric_dtype(df_display[col]):
                if tipo_visualizacao == "CPU (Custo por Unidade)":
                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}")
                else:
                    _suf = ""
                    if fator_conversao:
                        if fator_conversao == "K (milhares)":
                            _suf = " K"
                        elif fator_conversao == "M (Milhões)":
                            _suf = " M"
                    df_display[col] = df_display[col].map(lambda x: f"{x:,.2f}{_suf}")

    # Resumo
    linha_resumo_t06 = {}
    linha_resumo_fmt_t06 = {}
    for c in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2', '% Mês 2/Flex Mês 1']:
        linha_resumo_t06[c] = df_filt[c].sum() if c in df_filt.columns else 0
    for c in ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2']:
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            linha_resumo_fmt_t06[c] = f"{linha_resumo_t06[c]:,.2f}"
        else:
            _suf = ""
            if fator_conversao:
                if fator_conversao == "K (milhares)":
                    _suf = " K"
                elif fator_conversao == "M (Milhões)":
                    _suf = " M"
            linha_resumo_fmt_t06[c] = f"{linha_resumo_t06[c]:,.2f}{_suf}"
    pct_val = linha_resumo_t06.get('% Mês 2/Flex Mês 1', 0)
    linha_resumo_fmt_t06['% Mês 2/Flex Mês 1'] = formatar_ratio_com_barra(pct_val / 100 if pct_val else 0)

    # Renderizar
    st.markdown("---")
    st.markdown(f"#### **Type 06: {type06} - Total: {total_type06_fmt}**")
    with st.container():
        render_inline_summary_metrics(
            linha_resumo_t06,
            ['Mês 1', 'Flex Mês 1 - Mês 1', 'Flex Mês 1', 'Mês 2 - Flex Mês 1', 'Mês 2', '% Mês 2/Flex Mês 1'],
            percent_columns={'% Mês 2/Flex Mês 1'},
            percent_input='percent',
        )
        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_fmt_t06)
        st.markdown(html_table, unsafe_allow_html=True)
    return True


def _enrich_with_vehicle(df_main, df_veic):
    """Usa dados por veículo (com coluna Veículo) se disponíveis.
    Renomeia 'Custo FP Veiculo' → 'Custo FP' para compatibilidade."""
    if df_veic is not None and not df_veic.empty and 'Veículo' in df_veic.columns:
        if 'Custo FP Veiculo' in df_veic.columns:
            df_veic = df_veic.copy()
            df_veic['Custo FP'] = df_veic['Custo FP Veiculo']
        return df_veic
    return df_main


@st.cache_data(ttl=7200, max_entries=12, show_spinner=False)
def _load_tc_veiculos_real(ano_sel):
    """Carrega dados Real do TC Veículos com coluna Veículo."""
    if ano_sel == "Todos":
        frames = []
        for ano in descobrir_anos_tc_principal():
            df = _enrich_with_vehicle(load_principal_real(ano), load_custo_fp_veiculo_real(ano))
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()
    else:
        df = _enrich_with_vehicle(load_principal_real(int(ano_sel)), load_custo_fp_veiculo_real(int(ano_sel)))
        return df if df is not None else pd.DataFrame()


@st.cache_data(ttl=7200, max_entries=12, show_spinner=False)
def _load_tc_veiculos_budget(ano_sel):
    """Carrega dados Budget do TC Veículos com coluna Veículo."""
    if ano_sel == "Todos":
        frames = []
        for ano in descobrir_anos_tc_principal():
            df = _enrich_with_vehicle(load_principal(ano), load_custo_fp_veiculo(ano))
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()
    else:
        df = _enrich_with_vehicle(load_principal(int(ano_sel)), load_custo_fp_veiculo(int(ano_sel)))
        return df if df is not None else pd.DataFrame()


@st.cache_data(ttl=7200, max_entries=12, show_spinner=False)
def _load_tc_veiculos_volume(ano_sel):
    """Carrega dados de volume Real do TC Veículos."""
    if ano_sel == "Todos":
        frames = []
        for ano in descobrir_anos_tc_principal():
            df = load_volume_actual(ano)
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()
    else:
        df = load_volume_actual(int(ano_sel))
        return df if df is not None else pd.DataFrame()


@st.cache_data(ttl=7200, max_entries=12, show_spinner=False)
def _load_tc_veiculos_budget_volume(ano_sel):
    """Carrega dados de volume Budget do TC Veículos."""
    if ano_sel == "Todos":
        frames = []
        for ano in descobrir_anos_tc_principal():
            df = load_volume_bud(ano)
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()
    else:
        df = load_volume_bud(int(ano_sel))
        return df if df is not None else pd.DataFrame()


@st.cache_data(ttl=7200, max_entries=12, show_spinner=False)
def _load_tc_veiculos_sapiens(ano_sel):
    """Carrega o parquet detalhado do Sapiens para um ano ou para todos."""
    if ano_sel == "Todos":
        frames = []
        for ano in descobrir_anos_tc_principal():
            df = load_tc_sapiens(ano)
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    df = load_tc_sapiens(int(ano_sel))
    return df if df is not None else pd.DataFrame()


def _sanitize_filter_key(texto):
    return ''.join(ch.lower() if ch.isalnum() else '_' for ch in str(texto))


def _apply_local_filters_sapiens(df_sapiens_base, key_prefix):
    """Aplica filtros locais amplos no expander do Sapiens sem depender do recorte do waterfall."""
    if df_sapiens_base is None or df_sapiens_base.empty:
        return pd.DataFrame()

    filtros_relevantes = [
        ('Ano', '📆 Ano'),
        ('Período', '📅 Período'),
        ('Oficina', '🏭 Oficina'),
        ('USI', '🏗️ USI'),
        ('Veículo', '🚗 Veículo'),
        ('Centrocst', '🏢 Centro cst'),
        ('Nºconta', '🔢 Conta contábil'),
        ('Type 05', '📂 Type 05'),
        ('Type 06', '🧩 Type 06'),
        ('Account', '🗂️ Account'),
        ('Fornecedor', '🏷️ Fornecedor'),
        ('Fornec.', '🏷️ Fornec.'),
        ('Tipo', '📝 Tipo'),
        ('Usuário', '👤 Usuário'),
        ('Material', '📦 Material'),
        ('Dt.lçto.', '🗓️ Data lançamento'),
        ('Texto breve', '💬 Texto breve'),
    ]
    colunas_disponiveis = [item for item in filtros_relevantes if item[0] in df_sapiens_base.columns]
    filtros_aplicados = []

    for start in range(0, len(colunas_disponiveis), 4):
        chunk = colunas_disponiveis[start:start + 4]
        cols_ui = st.columns(len(chunk))
        for idx, (coluna, label) in enumerate(chunk):
            valores = [str(v) for v in df_sapiens_base[coluna].dropna().astype(str).unique().tolist() if str(v).strip()]
            valores = sorted(valores)
            key_slug = _sanitize_filter_key(coluna)
            with cols_ui[idx]:
                if len(valores) <= 200:
                    selecionados = st.multiselect(
                        label,
                        valores,
                        default=[],
                        key=f"{key_prefix}_{key_slug}_multiselect",
                    )
                    if selecionados:
                        filtros_aplicados.append((coluna, 'isin', selecionados))
                else:
                    termo = st.text_input(
                        f"{label} contém",
                        key=f"{key_prefix}_{key_slug}_contains",
                    ).strip()
                    if termo:
                        filtros_aplicados.append((coluna, 'contains', termo))

    df_filtrado = df_sapiens_base.copy()
    for coluna, tipo_filtro, valor in filtros_aplicados:
        serie = df_filtrado[coluna].astype(str)
        if tipo_filtro == 'isin':
            df_filtrado = df_filtrado[serie.isin([str(v) for v in valor])].copy()
        elif tipo_filtro == 'contains':
            df_filtrado = df_filtrado[
                serie.str.contains(str(valor), case=False, na=False, regex=False)
            ].copy()

    return df_filtrado


def _render_sapiens_table_waterfall(df_sapiens_base, ano_label, key_prefix):
    """Renderiza a tabela detalhada do Sapiens com filtros locais expandidos."""
    if df_sapiens_base is None or df_sapiens_base.empty:
        st.info(
            "ℹ️ Dados Sapiens detalhados não disponíveis para o ano selecionado. "
            "Execute o processamento Real na página Extração de Dados, se necessário."
        )
        return

    with st.expander("📑 Tabela Sapiens — Todas as Colunas", expanded=False):
        st.caption("Use os filtros abaixo para refinar a tabela Sapiens sem depender do recorte do Waterfall.")
        df_sap_filt = _apply_local_filters_sapiens(df_sapiens_base, key_prefix)

        st.caption(f"📊 {len(df_sap_filt):,} linhas × {len(df_sap_filt.columns)} colunas")
        if df_sap_filt.empty:
            st.warning("⚠️ Nenhuma linha encontrada com os filtros locais selecionados.")
        df_sap_filt = reordenar_colunas_be(df_sap_filt)
        st.dataframe(df_sap_filt, use_container_width=True, height=500)

        _hoje_wf = datetime.now().strftime('%Y%m%d')
        download_excel_button(
            st, df_sap_filt,
            "📥 Baixar Sapiens Detalhado (Excel)",
            f"TC_Sapiens_Detalhado_{ano_label}_{_hoje_wf}.xlsx",
            f"{key_prefix}_download",
        )


def _render_sapiens_section_waterfall(ano_label, key_prefix):
    """Exibe a seção Sapiens no Waterfall sem recortar previamente os dados."""
    st.markdown("---")
    st.markdown("## 📑 Dados Sapiens Detalhados")
    df_sapiens_base = _load_tc_veiculos_sapiens(ano_label)
    _render_sapiens_table_waterfall(df_sapiens_base, ano_label, key_prefix)


def _build_selected_period_pairs(*frames):
    pares = []
    for df in frames:
        if df is None or df.empty or 'Período' not in df.columns:
            continue
        cols = ['Período']
        if 'Ano' in df.columns:
            cols.insert(0, 'Ano')
        pares.append(df[cols].copy())

    if not pares:
        return pd.DataFrame()

    df_pares = pd.concat(pares, ignore_index=True).drop_duplicates()
    if 'Ano' in df_pares.columns:
        df_pares['Ano'] = df_pares['Ano'].astype(str)
    df_pares['Período'] = df_pares['Período'].astype(str)
    return df_pares


def _filter_df_by_period_pairs(df_base, df_pairs):
    if df_base is None or df_base.empty or df_pairs is None or df_pairs.empty:
        return df_base

    if 'Período' not in df_base.columns:
        return df_base

    df_out = df_base.copy()
    if 'Ano' in df_pairs.columns and 'Ano' in df_out.columns:
        df_out['Ano'] = df_out['Ano'].astype(str)
        df_out['Período'] = df_out['Período'].astype(str)
        return df_out.merge(df_pairs[['Ano', 'Período']], on=['Ano', 'Período'], how='inner')

    pares_periodo = df_pairs['Período'].astype(str).unique().tolist()
    return df_out[df_out['Período'].astype(str).isin(pares_periodo)].copy()


def _apply_session_filters_to_sapiens_tc(df_base):
    if df_base is None or df_base.empty:
        return pd.DataFrame()

    df_out = df_base.copy()

    filtros_multiselect = [
        ('Oficina', 'filtro_oficina_waterfall_tc', True),
        ('Veículo', 'filtro_veiculo_waterfall_tc', True),
        ('USI', 'filtro_usi_waterfall_tc', True),
        ('Nºconta', 'filtro_conta_contabil_waterfall_tc', False),
        ('Type 05', 'filtro_Type 05_waterfall_tc', True),
        ('Type 06', 'filtro_Type 06_waterfall_tc', True),
        ('Account', 'filtro_Account_waterfall_tc', True),
        ('Fornecedor', 'filtro_Fornecedor_waterfall_tc', True),
        ('Fornec.', 'filtro_Fornec._waterfall_tc', True),
        ('Tipo', 'filtro_Tipo_waterfall_tc', True),
        ('Usuário', 'filtro_avancado_Usuário_waterfall_tc', True),
        ('Material', 'filtro_avancado_Material_waterfall_tc', True),
        ('Dt.lçto.', 'filtro_avancado_Dt.lçto._waterfall_tc', True),
        ('Texto breve', 'filtro_avancado_Texto breve_waterfall_tc', True),
    ]

    for coluna, state_key, aceitar_todos in filtros_multiselect:
        selecionados = st.session_state.get(state_key)
        if coluna not in df_out.columns or not selecionados:
            continue
        selecionados = [str(v) for v in selecionados if str(v).strip()]
        if not selecionados:
            continue
        if aceitar_todos and 'Todos' in selecionados:
            continue
        df_out = df_out[df_out[coluna].astype(str).isin(selecionados)].copy()

    centro_sel = st.session_state.get('filtro_centro_cst_waterfall_tc', 'Todos')
    if 'Centrocst' in df_out.columns and centro_sel and str(centro_sel) != 'Todos':
        df_out = df_out[df_out['Centrocst'].astype(str) == str(centro_sel)].copy()

    return df_out


def _build_top_gastos_por_categoria(
    df_sapiens, cat_labels, cat_column, valor_col, moeda_simbolo, sufixo,
    n_hover=10, n_tabela=10,
):
    """Pré-calcula TOP gastos por categoria para tooltip analítico do Waterfall.

    Retorna dict {categoria: {hover_str, df_top}} com:
    - hover_str: HTML com TOP 10 (Texto breve | Oficina | Valor | %) + "Outros gastos"
    - df_top: DataFrame TOP 10 para tabela drill-down
    """
    result = {}
    if df_sapiens is None or df_sapiens.empty or not valor_col:
        return result
    if cat_column not in df_sapiens.columns or 'Texto breve' not in df_sapiens.columns:
        return result
    if valor_col not in df_sapiens.columns:
        return result
    df_sap = df_sapiens.copy()
    df_sap[valor_col] = pd.to_numeric(df_sap[valor_col], errors='coerce').fillna(0)
    df_sap['_cat_norm'] = df_sap[cat_column].astype(str).str.strip()
    df_sap['_texto'] = df_sap['Texto breve'].astype(str).str.strip().str[:80]
    _has_oficina = 'Oficina' in df_sap.columns and cat_column != 'Oficina'
    if _has_oficina:
        df_sap['_oficina'] = df_sap['Oficina'].astype(str).str.strip()
    for cat in cat_labels:
        cat_str = str(cat).strip()
        df_cat = df_sap.loc[df_sap['_cat_norm'] == cat_str]
        if df_cat.empty:
            result[cat_str] = {'hover_str': '', 'df_top': pd.DataFrame()}
            continue
        total_cat = df_cat[valor_col].sum()
        abs_total = abs(total_cat) if total_cat != 0 else 1
        grp_cols = ['_texto'] + (['_oficina'] if _has_oficina else [])
        agg = df_cat.groupby(grp_cols, as_index=False)[valor_col].sum()
        agg = agg.sort_values(valor_col, ascending=False)
        top = agg.head(n_hover)
        top_sum = top[valor_col].sum()
        outros_val = total_cat - top_sum
        df_top = top.copy()
        df_top['%'] = (df_top[valor_col] / abs_total * 100).round(1)
        if _has_oficina:
            df_top = df_top.rename(columns={'_texto': 'Texto breve', '_oficina': 'Oficina', valor_col: 'Valor'})
            df_top = df_top[['Texto breve', 'Oficina', 'Valor', '%']]
        else:
            df_top = df_top.rename(columns={'_texto': 'Texto breve', valor_col: 'Valor'})
            df_top = df_top[['Texto breve', 'Valor', '%']]
        lines = []
        for idx, (_, row) in enumerate(top.iterrows()):
            txt = row['_texto'] if len(row['_texto']) <= 40 else row['_texto'][:37] + '...'
            val = row[valor_col]
            pct = val / abs_total * 100
            ofc_part = f" | {row['_oficina']}" if _has_oficina else ''
            lines.append(f"{idx+1}. {txt}{ofc_part} | {moeda_simbolo} {val:,.0f}{sufixo} ({pct:.1f}%)")
        if abs(outros_val) > 0.01:
            pct_outros = outros_val / abs_total * 100
            lines.append(f"Outros (fora do Top {n_hover}): {moeda_simbolo} {outros_val:,.0f}{sufixo} ({pct_outros:.1f}%)")
        hover_str = '<br>'.join(lines)
        result[cat_str] = {'hover_str': hover_str, 'df_top': df_top}
    return result


def _build_office_waterfall_figure(
    df_m1,
    df_m2,
    df_vol_m1,
    df_vol_m2,
    total_inicial,
    total_final,
    label_inicial,
    label_final,
    label_flex,
    tipo_visualizacao,
    moeda_simbolo,
    fator_conversao,
    value_column,
    flex_delta_override=None,
    office_labels_override=None,
    office_values_override=None,
    top_gastos_dict=None,
):
    def _normalize_oficina_label(valor):
        texto = str(valor).strip() if pd.notna(valor) else ''
        return texto if texto else 'Sem Oficina'

    def _wrap_office_label(valor, max_chars=14):
        texto = _normalize_oficina_label(valor)
        if len(texto) <= max_chars:
            return texto
        partes = texto.split()
        linhas = []
        atual = ''
        for parte in partes:
            candidato = f'{atual} {parte}'.strip()
            if atual and len(candidato) > max_chars:
                linhas.append(atual)
                atual = parte
            else:
                atual = candidato
        if atual:
            linhas.append(atual)
        return '<br>'.join(linhas[:3])

    def _volume_total(df_vol):
        if df_vol is None or df_vol.empty or 'Volume' not in df_vol.columns:
            return 0.0
        return float(pd.to_numeric(df_vol['Volume'], errors='coerce').fillna(0).sum())

    def _series_real():
        custo_m1 = df_m1.groupby('Oficina')[value_column].sum()
        custo_m2 = df_m2.groupby('Oficina')[value_column].sum()
        idx = custo_m1.index.union(custo_m2.index)

        def _volume_por_oficina(df_vol):
            if df_vol is None or df_vol.empty or 'Oficina' not in df_vol.columns or 'Volume' not in df_vol.columns:
                return pd.Series(dtype=float)
            return df_vol.groupby('Oficina')['Volume'].sum()

        vol_m1 = _volume_por_oficina(df_vol_m1).reindex(idx, fill_value=0)
        vol_m2 = _volume_por_oficina(df_vol_m2).reindex(idx, fill_value=0)
        custo_m1 = custo_m1.reindex(idx, fill_value=0)
        custo_m2 = custo_m2.reindex(idx, fill_value=0)

        volume_total_m1 = float(vol_m1.sum())
        volume_total_m2 = float(vol_m2.sum())
        ratio_global = volume_total_m2 / volume_total_m1 if volume_total_m1 else 1.0

        # Calcular flex em CUSTO TOTAL por oficina (não em CPU)
        flex_custo_por_ofc = {}
        for oficina in idx.tolist():
            df_ofi_m1 = df_m1[df_m1['Oficina'].astype(str) == str(oficina)].copy()
            if df_ofi_m1.empty:
                flex_custo_por_ofc[oficina] = 0.0
                continue

            vol_ofi_m1 = float(vol_m1.get(oficina, 0.0))
            vol_ofi_m2 = float(vol_m2.get(oficina, 0.0))
            ratio_ofi = vol_ofi_m2 / vol_ofi_m1 if vol_ofi_m1 else ratio_global

            if 'Custo' in df_ofi_m1.columns:
                fixo = df_ofi_m1[df_ofi_m1['Custo'] == 'Fixo'][value_column].sum()
                variavel = df_ofi_m1[df_ofi_m1['Custo'] == 'Variável'][value_column].sum()
            else:
                fixo = 0.0
                variavel = df_ofi_m1[value_column].sum()

            flex_custo_por_ofc[oficina] = float(fixo + (variavel * ratio_ofi))

        if 'Custo' in df_m1.columns:
            fixo_total = float(df_m1[df_m1['Custo'] == 'Fixo'][value_column].sum())
            variavel_total = float(df_m1[df_m1['Custo'] == 'Variável'][value_column].sum())
        else:
            fixo_total = 0.0
            variavel_total = float(df_m1[value_column].sum())

        flex_total_custo = fixo_total + (variavel_total * ratio_global)
        if tipo_visualizacao == 'CPU (Custo por Unidade)':
            flex_total = flex_total_custo / volume_total_m2 if volume_total_m2 else 0.0
        else:
            flex_total = flex_total_custo
        flex_delta = float(flex_total - total_inicial)

        # Calcular delta (contribuição) por oficina
        # No modo CPU, calcular contribuição como (custo_m2 - flex_custo) / volume_total
        # Isso garante que sum(delta_ofc) = (total_m2 - flex_total) / volume_total_m2
        if tipo_visualizacao == 'CPU (Custo por Unidade)':
            delta = pd.Series({
                oficina: float((custo_m2.get(oficina, 0.0) - flex_custo_por_ofc.get(oficina, 0.0)) / volume_total_m2)
                if volume_total_m2 else 0.0
                for oficina in idx.tolist()
            }).sort_values(key=lambda serie: serie.abs(), ascending=False)
        else:
            delta = pd.Series({
                oficina: float(custo_m2.get(oficina, 0.0) - flex_custo_por_ofc.get(oficina, 0.0))
                for oficina in idx.tolist()
            }).sort_values(key=lambda serie: serie.abs(), ascending=False)

        office_labels = [str(oficina) for oficina in delta.index.tolist()]
        office_values = [float(valor) for valor in delta.tolist()]

        return office_labels, office_values, flex_delta

    def _series_budget():
        group_cols = ['Oficina']
        if 'Custo' in df_m1.columns or 'Custo' in df_m2.columns:
            group_cols.append('Custo')

        df_budget_local = df_m1.copy()
        df_real_local = df_m2.copy()
        for _df in (df_budget_local, df_real_local):
            _df['Oficina'] = _df['Oficina'].apply(_normalize_oficina_label)
            for _col in group_cols:
                if _col not in _df.columns:
                    _df[_col] = '(Nao informado)'
                serie = _df[_col]
                if pd.api.types.is_categorical_dtype(serie):
                    if '(Nao informado)' not in serie.cat.categories:
                        serie = serie.cat.add_categories(['(Nao informado)'])
                    _df[_col] = serie.fillna('(Nao informado)')
                else:
                    _df[_col] = serie.fillna('(Nao informado)')

        df_budget_grouped = df_budget_local.groupby(group_cols)[value_column].sum().reset_index()
        df_budget_grouped = df_budget_grouped.rename(columns={value_column: 'BUD'})
        df_real_grouped = df_real_local.groupby(group_cols)[value_column].sum().reset_index()
        df_real_grouped = df_real_grouped.rename(columns={value_column: 'Total'})

        df_merge = df_real_grouped.merge(df_budget_grouped, on=group_cols, how='outer')
        df_merge['BUD'] = pd.to_numeric(df_merge['BUD'], errors='coerce').fillna(0)
        df_merge['Total'] = pd.to_numeric(df_merge['Total'], errors='coerce').fillna(0)

        volume_budget = _volume_total(df_vol_m1)
        volume_real = _volume_total(df_vol_m2)
        base_budget = volume_budget if volume_budget > 0 else 1.0
        base_real = volume_real if volume_real > 0 else 1.0
        proporcao_volume = (volume_real / base_budget) if base_budget else 1.0
        proporcao_volume = proporcao_volume if pd.notna(proporcao_volume) else 1.0

        custo_norm = df_merge['Custo'].astype(str).str.strip().str.lower() if 'Custo' in df_merge.columns else pd.Series('', index=df_merge.index)
        is_fixo = custo_norm == 'fixo'
        df_merge['Flex BUD'] = df_merge['BUD'].where(is_fixo, 0) + (df_merge['BUD'] * proporcao_volume).where(~is_fixo, 0)

        if tipo_visualizacao == 'CPU (Custo por Unidade)':
            df_merge['Flex BUD'] = df_merge['Flex BUD'] / base_real
            df_merge['BUD'] = df_merge['BUD'] / base_budget
            df_merge['Total'] = df_merge['Total'] / base_real

        df_merge['Flex Bud - BUD'] = df_merge['Flex BUD'] - df_merge['BUD']
        df_merge['Total - Flex Bud'] = df_merge['Total'] - df_merge['Flex BUD']

        df_grouped = df_merge.groupby('Oficina', dropna=False).agg({
            'Flex Bud - BUD': 'sum',
            'Total - Flex Bud': 'sum',
        }).reset_index()
        df_grouped = df_grouped.sort_values('Total - Flex Bud', key=lambda serie: serie.abs(), ascending=False)

        office_labels = [
            _normalize_oficina_label(oficina)
            for oficina in df_grouped['Oficina'].tolist()
        ]
        office_values = [
            float(pd.to_numeric(valor, errors='coerce'))
            for valor in df_grouped['Total - Flex Bud'].tolist()
        ]

        flex_delta = float(pd.to_numeric(df_grouped['Flex Bud - BUD'], errors='coerce').fillna(0).sum())
        return office_labels, office_values, flex_delta

    if (
        df_m1 is None or df_m1.empty or df_m2 is None or df_m2.empty or
        'Oficina' not in df_m1.columns or 'Oficina' not in df_m2.columns or
        value_column not in df_m1.columns or value_column not in df_m2.columns
    ):
        return None

    df_m1 = df_m1.copy()
    df_m2 = df_m2.copy()
    if df_vol_m1 is not None and not df_vol_m1.empty and 'Oficina' in df_vol_m1.columns:
        df_vol_m1 = df_vol_m1.copy()
        df_vol_m1['Oficina'] = df_vol_m1['Oficina'].apply(_normalize_oficina_label)
    if df_vol_m2 is not None and not df_vol_m2.empty and 'Oficina' in df_vol_m2.columns:
        df_vol_m2 = df_vol_m2.copy()
        df_vol_m2['Oficina'] = df_vol_m2['Oficina'].apply(_normalize_oficina_label)
    df_m1['Oficina'] = df_m1['Oficina'].apply(_normalize_oficina_label)
    df_m2['Oficina'] = df_m2['Oficina'].apply(_normalize_oficina_label)

    if str(label_flex) == 'Flex Bud - BUD':
        office_labels, office_values, flex_delta = _series_budget()
    else:
        office_labels, office_values, flex_delta = _series_real()

    # Se foram fornecidos overrides do gráfico principal (quando Dimensão = Oficina), usar eles
    if office_labels_override is not None and office_values_override is not None:
        office_labels = list(office_labels_override)
        office_values = [float(v) for v in office_values_override]

    if flex_delta_override is not None:
        flex_delta = float(flex_delta_override)

    office_count = len(office_labels)
    tick_angle = -90 if office_count > 18 else (-55 if office_count > 10 else -38)
    tick_size = 10 if office_count > 18 else (11 if office_count > 10 else 12)
    bottom_margin = 155 if office_count > 18 else (120 if office_count > 10 else 95)

    labels = [str(label_inicial)]
    values = [float(total_inicial)]
    measures = ['absolute']

    if abs(flex_delta) > 1e-10:
        labels.append(str(label_flex))
        values.append(float(flex_delta))
        measures.append('relative')

    labels.extend(office_labels)
    values.extend(office_values)
    measures.extend(['relative'] * len(office_values))
    labels.append(str(label_final))
    values.append(float(total_final))
    measures.append('total')

    tick_labels = []
    office_set = set(office_labels)
    for label in labels:
        if label in office_set:
            wrapped = _wrap_office_label(label)
            tick_labels.append(wrapped)
        else:
            tick_labels.append(str(label))

    sufixo = ''
    if fator_conversao == 'K (milhares)':
        sufixo = ' K'
    elif fator_conversao == 'M (Milhões)':
        sufixo = ' M'

    hover_texts = []
    acumulado = float(total_inicial)
    for label, value, measure in zip(labels, values, measures):
        if measure == 'absolute':
            acumulado = float(value)
            tipo_barra = 'Inicial'
        elif measure == 'total':
            acumulado = float(value)
            tipo_barra = 'Final'
        else:
            acumulado += float(value)
            tipo_barra = 'Impacto por oficina'
        parts = [
            f'<b>{label}</b>',
            f'Tipo: {tipo_barra}',
            f'Valor: {moeda_simbolo} {value:,.2f}{sufixo}',
            f'Acumulado: {moeda_simbolo} {acumulado:,.2f}{sufixo}',
        ]
        if top_gastos_dict and measure == 'relative' and label != str(label_flex):
            top_entry = top_gastos_dict.get(str(label).strip(), {})
            top_str = top_entry.get('hover_str', '')
            if top_str:
                parts.append('──────────')
                parts.append('<b>TOP 10 gastos:</b>')
                parts.append(top_str)
        hover_texts.append('<br>'.join(parts))

    cor_verde = '#1e8449'
    cor_vermelha = '#ff5733'
    cor_azul = '#1e6ba8'
    cor_amarela = '#ffd700'

    annotations = []
    acumulado_labels = 0.0
    for label, value, measure in zip(labels, values, measures):
        if measure == 'absolute':
            acumulado_labels = float(value)
            y_pos = float(value)
            cor_texto = cor_azul
            texto = f'{value:,.1f}{sufixo}'
            yshift = 15
        elif measure == 'total':
            acumulado_labels = float(value)
            y_pos = float(value)
            cor_texto = cor_azul
            texto = f'{value:,.1f}{sufixo}'
            yshift = 15 if value >= 0 else -15
        else:
            acumulado_labels += float(value)
            y_pos = acumulado_labels  # topo (positivo) ou fundo (negativo)
            cor_texto = cor_amarela if label == str(label_flex) else (cor_vermelha if value >= 0 else cor_verde)
            texto = f'{value:+,.1f}{sufixo}'
            yshift = 15 if value >= 0 else -15

        annotations.append(dict(
            x=label,
            y=y_pos,
            text=texto,
            showarrow=False,
            font=dict(color=cor_texto, size=11),
            xref='x',
            yref='y',
            yshift=yshift,
        ))

    # ── Calcular range do eixo Y para garantir que rótulos sempre apareçam ──
    y_min, y_max = 0, 1
    if values:
        cumulative_r = 0
        all_y_pos = []
        for m, v in zip(measures, values):
            if m == 'absolute':
                cumulative_r = float(v)
                all_y_pos.append(cumulative_r)
            elif m == 'relative':
                cumulative_r += float(v)
                all_y_pos.append(cumulative_r)
            elif m == 'total':
                all_y_pos.append(float(v))
        if all_y_pos:
            min_bar = min(all_y_pos)
            max_bar = max(all_y_pos)
            min_ann, max_ann = min_bar, max_bar
            for ann in annotations:
                y_a = ann['y']
                ys = ann.get('yshift', 0)
                if ys > 0:
                    top = y_a + abs(y_a) * 0.08
                    if top > max_ann:
                        max_ann = top
                elif ys < 0:
                    bot = y_a - abs(y_a) * 0.08
                    if bot < min_ann:
                        min_ann = bot
            span = max_ann - min_ann
            if span > 0:
                y_min = min_ann - span * 0.10
                y_max = max_ann + span * 0.10
            else:
                y_min = min_ann * 0.90 if min_ann > 0 else min_ann * 1.10
                y_max = max_ann * 1.10 if max_ann > 0 else max_ann * 0.90
            if min_bar >= 0 and y_min < 0:
                y_min = 0

    fig = go.Figure(
        go.Waterfall(
            orientation='v',
            measure=measures,
            x=labels,
            y=values,
            decreasing={'marker': {'color': cor_verde}},
            increasing={'marker': {'color': cor_vermelha}},
            totals={'marker': {'color': cor_azul}},
            connector={'line': {'color': 'rgba(120,120,120,0.18)'}},
            hovertext=hover_texts,
            hovertemplate='%{hovertext}<extra></extra>',
            textposition='none',
            cliponaxis=False,
        )
    )

    if str(label_flex) in labels:
        idx_flex = labels.index(str(label_flex))
        valor_flex = float(values[idx_flex])
        cumulative_base = float(total_inicial)
        for i in range(1, idx_flex):
            cumulative_base += float(values[i])
        base_flex = cumulative_base if valor_flex >= 0 else cumulative_base + valor_flex
        fig.add_trace(go.Bar(
            x=[str(label_flex)],
            y=[abs(valor_flex)],
            base=[base_flex],
            marker_color=cor_amarela,
            marker_line=dict(width=2, color=cor_amarela),
            opacity=1.0,
            showlegend=False,
            textposition='none',
            width=0.82,
            hoverinfo='skip',
        ))

    fig.update_layout(
        height=380,
        margin=dict(l=30, r=20, t=18, b=bottom_margin),
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(size=10),
        barmode='overlay',
        annotations=annotations,
    )
    fig.update_xaxes(
        tickangle=tick_angle,
        showgrid=False,
        tickmode='array',
        tickvals=labels,
        ticktext=tick_labels,
        categoryorder='array',
        categoryarray=labels,
        tickfont=dict(size=tick_size),
        automargin=True,
    )
    fig.update_yaxes(showgrid=False, zeroline=True, zerolinecolor='rgba(120,120,120,0.25)', range=[y_min, y_max])
    return fig


def _prepare_sapiens_minimal_table_tc(
    ano_label,
    df_pairs,
    key_prefix,
    tipo_visualizacao,
    fator_conversao,
    moeda_codigo,
    taxas_cambio,
    oficinas_sel=None,
    accounts_sel=None,
):
    df_detail = _load_tc_veiculos_sapiens(ano_label)
    df_detail = _apply_session_filters_to_sapiens_tc(df_detail)
    df_detail = _filter_df_by_period_pairs(df_detail, df_pairs)

    if df_detail is None or df_detail.empty:
        return pd.DataFrame(), None

    if oficinas_sel and 'Oficina' in df_detail.columns:
        df_detail = df_detail[df_detail['Oficina'].astype(str).isin(oficinas_sel)].copy()
    if accounts_sel and 'Account' in df_detail.columns:
        df_detail = df_detail[df_detail['Account'].astype(str).isin(accounts_sel)].copy()

    valor_col = None
    for candidata in ['Despesa Primaria', 'Custo FP', 'Total']:
        if candidata in df_detail.columns:
            valor_col = candidata
            break

    if valor_col and tipo_visualizacao == 'Custo Total':
        if fator_conversao and fator_conversao != 'Nenhum':
            if fator_conversao == 'K (milhares)':
                df_detail[valor_col] = pd.to_numeric(df_detail[valor_col], errors='coerce') / 1000
            elif fator_conversao == 'M (Milhões)':
                df_detail[valor_col] = pd.to_numeric(df_detail[valor_col], errors='coerce') / 1000000
        if moeda_codigo != 'BRL':
            df_detail = converter_coluna_moeda(df_detail, valor_col, moeda_codigo, taxas_cambio)

    colunas_base = [
        ('Oficina', 'Ofic.'),
        ('Account', 'Acct'),
        ('Centrocst', 'CCst'),
        ('Texto breve', 'Texto'),
    ]
    if valor_col:
        nome_valor = 'Desp. Prim.' if valor_col == 'Despesa Primaria' else valor_col
        colunas_base.append((valor_col, nome_valor))

    colunas_existentes = [col for col, _ in colunas_base if col in df_detail.columns]
    if not colunas_existentes:
        return pd.DataFrame(), valor_col

    df_show = df_detail[colunas_existentes].copy()
    if 'Texto breve' in df_show.columns:
        df_show['Texto breve'] = df_show['Texto breve'].astype(str).str.strip().str.lower().str.slice(0, 96)
    if valor_col and valor_col in df_show.columns:
        df_show[valor_col] = pd.to_numeric(df_show[valor_col], errors='coerce').fillna(0)
        df_show = df_show.sort_values(valor_col, ascending=False)

    rename_map = {orig: novo for orig, novo in colunas_base if orig in df_show.columns}
    return df_show.rename(columns=rename_map), valor_col


def _render_minimal_text_table_tc(df_show, valor_col=None):
    import html

    if df_show is None or df_show.empty:
        return

    col_styles = {}
    total_cols = max(len(df_show.columns), 1)
    for col in df_show.columns:
        if col == 'Texto':
            col_styles[col] = 'width:46%;min-width:260px;max-width:420px;'
        elif valor_col and col == valor_col:
            col_styles[col] = 'width:14%;min-width:92px;max-width:120px;'
        else:
            col_styles[col] = f'width:{max(10, int(40 / total_cols))}%;min-width:78px;'

    rows = []
    for _, row in df_show.head(150).iterrows():
        cells = []
        for col in df_show.columns:
            value = row[col]
            if pd.isna(value):
                text = ''
            elif valor_col and col == valor_col and pd.api.types.is_number(value):
                text = f'{float(value):,.2f}'
            else:
                text = str(value)
            align = 'right' if valor_col and col == valor_col else 'left'
            weight = '600' if valor_col and col == valor_col else '400'
            extra_style = col_styles.get(col, '')
            cell_text_style = 'text-transform:none;' if col == 'Texto' else ''
            cells.append(
                f"<td style='padding:4px 6px;border-bottom:1px solid rgba(120,120,120,0.10);text-align:{align};font-weight:{weight};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:1px;font-size:0.74rem;{extra_style}{cell_text_style}'>{html.escape(text)}</td>"
            )
        rows.append('<tr>' + ''.join(cells) + '</tr>')

    header_cells = ''.join(
        f"<th style='position:sticky;top:0;z-index:2;padding:5px 6px;border-bottom:1px solid rgba(120,120,120,0.18);text-align:{'right' if valor_col and col == valor_col else 'left'};font-size:0.68rem;font-weight:600;color:rgba(90,90,90,0.96);letter-spacing:0.02em;text-transform:uppercase;background:rgba(248,249,251,0.98);backdrop-filter:blur(2px);{col_styles.get(col, '')}'>{html.escape(str(col))}</th>"
        for col in df_show.columns
    )
    table_html = (
        "<div style='max-height:340px;overflow:auto;border:1px solid rgba(120,120,120,0.12);border-radius:10px;'>"
        "<table style='width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;font-size:0.74rem;line-height:1.1;background:transparent'>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def _render_post_waterfall_panel_tc(
    ano_label,
    df_m1,
    df_m2,
    df_vol_m1,
    df_vol_m2,
    total_inicial,
    total_final,
    label_inicial,
    label_final,
    label_flex,
    tipo_visualizacao,
    moeda_simbolo,
    fator_conversao,
    moeda_codigo,
    taxas_cambio,
    value_column,
    key_prefix,
    flex_delta_override=None,
    office_labels_override=None,
    office_values_override=None,
):
    # --- Filtros Type 05 / Type 06 / Oficina / Account (filtram gráfico por oficina e Detalhe Sapiens) ---
    _src = pd.concat([df_m1, df_m2], ignore_index=True) if (not df_m1.empty or not df_m2.empty) else pd.DataFrame()
    col_t05_of, col_t06_of, col_ofic_f, col_acct_f = st.columns(4)
    with col_t05_of:
        _t05_opts = ["Todos"]
        if not _src.empty and 'Type 05' in _src.columns:
            _t05_opts += sorted(_src['Type 05'].dropna().astype(str).unique().tolist())
        _t05_sel = st.multiselect("Type 05:", _t05_opts, default=["Todos"], key=f"t05_oficina_{key_prefix}")
    with col_t06_of:
        _t06_opts = ["Todos"]
        if not _src.empty and 'Type 06' in _src.columns:
            _t06_opts += sorted(_src['Type 06'].dropna().astype(str).unique().tolist())
        _t06_sel = st.multiselect("Type 06:", _t06_opts, default=["Todos"], key=f"t06_oficina_{key_prefix}")
    with col_ofic_f:
        _ofic_opts = ["Todos"]
        if not _src.empty and 'Oficina' in _src.columns:
            _ofic_opts += sorted(_src['Oficina'].dropna().astype(str).unique().tolist())
        _ofic_sel = st.multiselect("Oficina:", _ofic_opts, default=["Todos"], key=f"ofic_oficina_{key_prefix}")
    with col_acct_f:
        _acct_opts = ["Todos"]
        if not _src.empty and 'Account' in _src.columns:
            _acct_opts += sorted(_src['Account'].dropna().astype(str).unique().tolist())
        _acct_sel = st.multiselect("Account:", _acct_opts, default=["Todos"], key=f"acct_oficina_{key_prefix}")

    _filtering_active = False
    if _t05_sel and "Todos" not in _t05_sel:
        _filtering_active = True
        if 'Type 05' in df_m1.columns:
            df_m1 = df_m1[df_m1['Type 05'].astype(str).isin(_t05_sel)].copy()
        if 'Type 05' in df_m2.columns:
            df_m2 = df_m2[df_m2['Type 05'].astype(str).isin(_t05_sel)].copy()
        if df_vol_m1 is not None and 'Type 05' in df_vol_m1.columns:
            df_vol_m1 = df_vol_m1[df_vol_m1['Type 05'].astype(str).isin(_t05_sel)].copy()
        if df_vol_m2 is not None and 'Type 05' in df_vol_m2.columns:
            df_vol_m2 = df_vol_m2[df_vol_m2['Type 05'].astype(str).isin(_t05_sel)].copy()
    if _t06_sel and "Todos" not in _t06_sel:
        _filtering_active = True
        if 'Type 06' in df_m1.columns:
            df_m1 = df_m1[df_m1['Type 06'].astype(str).isin(_t06_sel)].copy()
        if 'Type 06' in df_m2.columns:
            df_m2 = df_m2[df_m2['Type 06'].astype(str).isin(_t06_sel)].copy()
        if df_vol_m1 is not None and 'Type 06' in df_vol_m1.columns:
            df_vol_m1 = df_vol_m1[df_vol_m1['Type 06'].astype(str).isin(_t06_sel)].copy()
        if df_vol_m2 is not None and 'Type 06' in df_vol_m2.columns:
            df_vol_m2 = df_vol_m2[df_vol_m2['Type 06'].astype(str).isin(_t06_sel)].copy()
    if _ofic_sel and "Todos" not in _ofic_sel:
        _filtering_active = True
        if 'Oficina' in df_m1.columns:
            df_m1 = df_m1[df_m1['Oficina'].astype(str).isin(_ofic_sel)].copy()
        if 'Oficina' in df_m2.columns:
            df_m2 = df_m2[df_m2['Oficina'].astype(str).isin(_ofic_sel)].copy()
        if df_vol_m1 is not None and 'Oficina' in df_vol_m1.columns:
            df_vol_m1 = df_vol_m1[df_vol_m1['Oficina'].astype(str).isin(_ofic_sel)].copy()
        if df_vol_m2 is not None and 'Oficina' in df_vol_m2.columns:
            df_vol_m2 = df_vol_m2[df_vol_m2['Oficina'].astype(str).isin(_ofic_sel)].copy()
    if _acct_sel and "Todos" not in _acct_sel:
        _filtering_active = True
        if 'Account' in df_m1.columns:
            df_m1 = df_m1[df_m1['Account'].astype(str).isin(_acct_sel)].copy()
        if 'Account' in df_m2.columns:
            df_m2 = df_m2[df_m2['Account'].astype(str).isin(_acct_sel)].copy()
        if df_vol_m1 is not None and 'Account' in df_vol_m1.columns:
            df_vol_m1 = df_vol_m1[df_vol_m1['Account'].astype(str).isin(_acct_sel)].copy()
        if df_vol_m2 is not None and 'Account' in df_vol_m2.columns:
            df_vol_m2 = df_vol_m2[df_vol_m2['Account'].astype(str).isin(_acct_sel)].copy()
    # Se filtrou, invalida overrides pré-computados
    if _filtering_active:
        office_labels_override = None
        office_values_override = None

    st.markdown("#### 🧭 Ganho e Perda por Oficina")

    # ── Pré-calcular TOP gastos por oficina (para tooltip enriquecido) ──
    _sufixo_top = ''
    if fator_conversao == 'K (milhares)':
        _sufixo_top = ' K'
    elif fator_conversao == 'M (Milhões)':
        _sufixo_top = ' M'
    df_pairs = _build_selected_period_pairs(df_m1, df_m2)
    _df_sap_top = _load_tc_veiculos_sapiens(ano_label)
    _df_sap_top = _apply_session_filters_to_sapiens_tc(_df_sap_top)
    _df_sap_top = _filter_df_by_period_pairs(_df_sap_top, df_pairs)
    if _ofic_sel and 'Todos' not in _ofic_sel and 'Oficina' in _df_sap_top.columns:
        _df_sap_top = _df_sap_top[_df_sap_top['Oficina'].astype(str).isin(_ofic_sel)].copy()
    if _acct_sel and 'Todos' not in _acct_sel and 'Account' in _df_sap_top.columns:
        _df_sap_top = _df_sap_top[_df_sap_top['Account'].astype(str).isin(_acct_sel)].copy()
    if _t05_sel and 'Todos' not in _t05_sel and 'Type 05' in _df_sap_top.columns:
        _df_sap_top = _df_sap_top[_df_sap_top['Type 05'].astype(str).isin(_t05_sel)].copy()
    if _t06_sel and 'Todos' not in _t06_sel and 'Type 06' in _df_sap_top.columns:
        _df_sap_top = _df_sap_top[_df_sap_top['Type 06'].astype(str).isin(_t06_sel)].copy()
    _valor_col_top = None
    for _c in ['Despesa Primaria', 'Custo FP', 'Total']:
        if _c in _df_sap_top.columns:
            _valor_col_top = _c
            break
    if _valor_col_top and tipo_visualizacao == 'Custo Total':
        if fator_conversao == 'K (milhares)':
            _df_sap_top[_valor_col_top] = pd.to_numeric(_df_sap_top[_valor_col_top], errors='coerce') / 1000
        elif fator_conversao == 'M (Milhões)':
            _df_sap_top[_valor_col_top] = pd.to_numeric(_df_sap_top[_valor_col_top], errors='coerce') / 1000000
        if moeda_codigo != 'BRL':
            _df_sap_top = converter_coluna_moeda(_df_sap_top, _valor_col_top, moeda_codigo, taxas_cambio)
    # Obter labels das oficinas para o TOP (usando mesmos dados do waterfall)
    _all_oficinas = sorted(set(
        df_m1['Oficina'].astype(str).str.strip().unique().tolist() +
        df_m2['Oficina'].astype(str).str.strip().unique().tolist()
    )) if not df_m1.empty or not df_m2.empty else []
    _top_gastos = _build_top_gastos_por_categoria(
        _df_sap_top, _all_oficinas, 'Oficina', _valor_col_top, moeda_simbolo, _sufixo_top,
    )

    col_chart, col_table = st.columns([1.15, 0.85], gap='small')

    with col_chart:
        fig_oficinas = _build_office_waterfall_figure(
            df_m1=df_m1,
            df_m2=df_m2,
            df_vol_m1=df_vol_m1,
            df_vol_m2=df_vol_m2,
            total_inicial=total_inicial,
            total_final=total_final,
            label_inicial=label_inicial,
            label_final=label_final,
            label_flex=label_flex,
            tipo_visualizacao=tipo_visualizacao,
            moeda_simbolo=moeda_simbolo,
            fator_conversao=fator_conversao,
            value_column=value_column,
            flex_delta_override=flex_delta_override,
            office_labels_override=office_labels_override,
            office_values_override=office_values_override,
            top_gastos_dict=_top_gastos,
        )
        st.markdown("<div style='margin-bottom:60px;'><h5 style='margin:0;'>🏭 Waterfall por Oficina</h5></div>", unsafe_allow_html=True)
        if fig_oficinas is None:
            st.info('Nao ha dados suficientes por oficina para montar o grafico neste recorte.')
        else:
            plotly_chart_safe(fig_oficinas, use_container_width=True)
            st.caption('Vermelho indica aumento de custo; verde indica ganho/queda de custo.')

        # ── Tabela TOP 10 interativa por oficina (drill-down) ──
        if _top_gastos:
            _oficinas_com_top = [o for o in _all_oficinas if _top_gastos.get(o, {}).get('df_top') is not None and not _top_gastos[o]['df_top'].empty]
            if _oficinas_com_top:
                with st.expander('📋 Drill-down: TOP 10 gastos por Oficina', expanded=False):
                    _sel_ofc_top = st.selectbox(
                        'Selecione a oficina:', _oficinas_com_top,
                        key=f'top10_oficina_{key_prefix}',
                    )
                    if _sel_ofc_top and _sel_ofc_top in _top_gastos:
                        _df_t10 = _top_gastos[_sel_ofc_top]['df_top'].copy()
                        _df_t10['Valor'] = _df_t10['Valor'].apply(lambda x: f'{moeda_simbolo} {x:,.2f}{_sufixo_top}')
                        _df_t10['%'] = _df_t10['%'].apply(lambda x: f'{x:.1f}%')
                        _df_t10.index = range(1, len(_df_t10) + 1)
                        _df_t10.index.name = '#'
                        st.dataframe(_df_t10, use_container_width=True)

    with col_table:
        st.markdown('##### 📄 Detalhe Sapiens')
        df_pairs = _build_selected_period_pairs(df_m1, df_m2)
        df_show, valor_col = _prepare_sapiens_minimal_table_tc(
            ano_label=ano_label,
            df_pairs=df_pairs,
            key_prefix=key_prefix,
            tipo_visualizacao=tipo_visualizacao,
            fator_conversao=fator_conversao,
            moeda_codigo=moeda_codigo,
            taxas_cambio=taxas_cambio,
            oficinas_sel=_ofic_sel if _ofic_sel and "Todos" not in _ofic_sel else None,
            accounts_sel=_acct_sel if _acct_sel and "Todos" not in _acct_sel else None,
        )
        if df_show.empty:
            st.info('Nao ha linhas detalhadas do Sapiens para o recorte atual.')
        else:
            qtd_total = len(df_show)
            st.caption(f'{qtd_total:,} linhas detalhadas no recorte atual.')
            valor_col_renamed = 'Desp. Prim.' if valor_col == 'Despesa Primaria' else valor_col
            _render_minimal_text_table_tc(df_show, valor_col=valor_col_renamed)
            if qtd_total > 150:
                st.caption('Exibindo as 150 linhas com maior despesa primaria.')

# ═══════════════════════════════════════════════════════════════
#  UI: CSS + Header + Sidebar global
# ═══════════════════════════════════════════════════════════════
injetar_css_global()
render_header()

st.markdown("""
    <style>
        h1 {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("🌊 Waterfall Analysis — TC Veículos")

# ── Sidebar Global (Ano, Moeda, Taxas, Tipo, Fator, Tema) ──
cfg = render_sidebar_global('wf', incluir_todos=True)

# Variáveis-ponte: mantém os nomes originais usados no restante do arquivo
ano_selecionado = cfg['ano']          # int ou "Todos"
moeda_codigo    = cfg['moeda']        # 'BRL', 'USD' ou 'EUR'
moeda_simbolo   = cfg['simbolo']      # 'R$', '$' ou '€'
taxas_cambio    = cfg['taxas']        # {'BRL': 1.0, 'USD': ..., 'EUR': ...}
tipo_visualizacao = cfg['tipo']       # 'Custo Total' ou 'CPU (Custo por Unidade)'
fator_conversao = cfg['fator']        # 'Nenhum', 'K (milhares)' ou 'M (Milhões)'

# Carregar dados com o ano selecionado
try:
    df_total = _load_tc_veiculos_real(ano_selecionado)
    
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

# ═══ Merge BE para meses sem dados Real ═══
_be_merged = False
try:
    _df_fc = load_forecast_completo()
    if _df_fc is not None and not _df_fc.empty:
        _df_fc = normalizar_periodo(_df_fc)
        # Filtrar apenas linhas BE
        if 'Tipo' in _df_fc.columns:
            _df_be = _df_fc[_df_fc['Tipo'] == 'BE'].copy()
        else:
            _df_be = pd.DataFrame()

        if not _df_be.empty and 'Período' in _df_be.columns:
            # Filtrar pelo ano selecionado
            if ano_selecionado != "Todos" and 'Ano' in _df_be.columns:
                try:
                    _df_be = _df_be[_df_be['Ano'] == int(ano_selecionado)].copy()
                except (ValueError, TypeError):
                    pass

            # Meses que já existem no Real
            _meses_real = set()
            if 'Período' in df_total.columns:
                _meses_real = set(df_total['Período'].dropna().unique())

            # Meses BE que NÃO existem no Real
            _meses_be = set(_df_be['Período'].dropna().unique())
            _meses_novos = _meses_be - _meses_real

            if _meses_novos:
                _df_be_novos = _df_be[_df_be['Período'].isin(_meses_novos)].copy()
                # Marcar fonte
                df_total['Fonte'] = 'Real'
                _df_be_novos['Fonte'] = 'BE'
                # Alinhar colunas
                for c in df_total.columns:
                    if c not in _df_be_novos.columns:
                        _df_be_novos[c] = np.nan if pd.api.types.is_numeric_dtype(df_total[c]) else ''
                _df_be_novos = _df_be_novos[[c for c in df_total.columns if c in _df_be_novos.columns]]
                df_total = pd.concat([df_total, _df_be_novos], ignore_index=True)
                _be_merged = True

                # ═══ A1: Ratear dados BE por veículo (se BE não tem Veículo) ═══
                if 'Veículo' in df_total.columns:
                    _mask_be_sem_veic = (
                        (df_total['Fonte'] == 'BE') &
                        (df_total['Veículo'].isna() | (df_total['Veículo'].astype(str).str.strip() == ''))
                    )
                    if _mask_be_sem_veic.any():
                        try:
                            _ano_int = int(ano_selecionado) if ano_selecionado != "Todos" else None
                            _anos_pct = [_ano_int] if _ano_int else descobrir_anos_tc_principal()
                            _df_pct_all = []
                            for _a in _anos_pct:
                                _pct = load_percentual_rateio_veiculos_real(_a)
                                if _pct is not None and not _pct.empty:
                                    _df_pct_all.append(_pct)
                            if _df_pct_all:
                                _df_pct_concat = pd.concat(_df_pct_all, ignore_index=True)
                                _df_be_sem_v = df_total[_mask_be_sem_veic].copy()
                                _df_be_com_v = ratear_be_por_veiculo(
                                    _df_be_sem_v, _df_pct_concat, col_custo='Custo FP',
                                )
                                if _df_be_com_v is not None and not _df_be_com_v.empty:
                                    if 'Custo FP Veiculo' in _df_be_com_v.columns:
                                        _df_be_com_v['Custo FP'] = _df_be_com_v['Custo FP Veiculo']
                                    df_total = pd.concat(
                                        [df_total[~_mask_be_sem_veic], _df_be_com_v],
                                        ignore_index=True,
                                    )
                        except Exception:
                            pass  # falha silenciosa — mantém dados originais
            else:
                df_total['Fonte'] = 'Real'
        else:
            df_total['Fonte'] = 'Real'
    else:
        df_total['Fonte'] = 'Real'
except Exception:
    if 'Fonte' not in df_total.columns:
        df_total['Fonte'] = 'Real'

# Carregar dados de volume e budget
df_volume = _load_tc_veiculos_volume(ano_selecionado)
df_budget = _load_tc_veiculos_budget(ano_selecionado)
df_budget_vol = _load_tc_veiculos_budget_volume(ano_selecionado)

# Função auxiliar para obter opções de filtro
@st.cache_data(ttl=7200, max_entries=20, show_spinner=False)
def get_filter_options(df, column_name):
    """Obtém opções de filtro com cache"""
    if column_name in df.columns:
        opcoes = sorted(
            df[column_name].dropna().astype(str).unique().tolist()
        )
        return ["Todos"] + opcoes
    return ["Todos"]


def _apply_display_transforms_waterfall(df_base, tipo_visualizacao, fator_conversao, moeda_codigo, taxas_cambio):
    """Aplica fator de conversão e moeda apenas ao recorte final usado na tela."""
    if df_base is None or df_base.empty or 'Custo FP' not in df_base.columns:
        return df_base

    df_out = df_base.copy()

    # Não converter para K/M quando o modo está em CPU.
    if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total":
        if fator_conversao == "K (milhares)":
            df_out['Custo FP'] = df_out['Custo FP'] / 1000
        elif fator_conversao == "M (Milhões)":
            df_out['Custo FP'] = df_out['Custo FP'] / 1000000

    # Em CPU e em Custo Total a moeda precisa refletir o dataset exibido.
    if moeda_codigo != "BRL":
        df_out = converter_coluna_moeda(df_out, 'Custo FP', moeda_codigo, taxas_cambio)

    return df_out

# Ordem dos meses para ordenação cronológica
ORDEM_MESES = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]

st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Filtros**")

# Inicializar session_state para filtros
if 'filtro_oficina_waterfall_tc' not in st.session_state:
    st.session_state.filtro_oficina_waterfall_tc = ["Todos"]

# Filtro 1: Oficina (com cache otimizado)
if 'Oficina' in df_total.columns:
    # 🔧 Ajuste: incluir também oficinas disponíveis no Budget (união Real + Budget)
    oficinas_set = set(df_total['Oficina'].dropna().astype(str).unique().tolist())
    if df_budget is not None and 'Oficina' in df_budget.columns:
        oficinas_set.update(df_budget['Oficina'].dropna().astype(str).unique().tolist())
    # Incluir também oficinas disponíveis no Budget de Volume
    if df_budget_vol is not None and 'Oficina' in df_budget_vol.columns:
        oficinas_set.update(df_budget_vol['Oficina'].dropna().astype(str).unique().tolist())
    oficina_opcoes = ["Todos"] + sorted(oficinas_set)
    # Validar valores salvos
    default_oficina = st.session_state.filtro_oficina_waterfall_tc if all(x in oficina_opcoes for x in st.session_state.filtro_oficina_waterfall_tc) else ["Todos"]
    oficina_selecionadas = st.sidebar.multiselect(
        "Selecione a Oficina:", oficina_opcoes, default=default_oficina, key="filtro_oficina_waterfall_tc_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_oficina_waterfall_tc = oficina_selecionadas if oficina_selecionadas else ["Todos"]

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
if 'filtro_veiculo_waterfall_tc' not in st.session_state:
    st.session_state.filtro_veiculo_waterfall_tc = ["Todos"]

# Filtro 2: Veículo (com cache otimizado)
if 'Veículo' in df_filtrado.columns:
    veiculo_opcoes = get_filter_options(df_filtrado, 'Veículo')
    # Validar valores salvos
    default_veiculo = st.session_state.filtro_veiculo_waterfall_tc if all(x in veiculo_opcoes for x in st.session_state.filtro_veiculo_waterfall_tc) else ["Todos"]
    veiculo_selecionados = st.sidebar.multiselect(
        "Selecione o Veículo:", veiculo_opcoes, default=default_veiculo, key="filtro_veiculo_waterfall_tc_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_veiculo_waterfall_tc = veiculo_selecionados if veiculo_selecionados else ["Todos"]
    if veiculo_selecionados and "Todos" not in veiculo_selecionados:
        df_filtrado = df_filtrado[
            df_filtrado['Veículo'].astype(str).isin(veiculo_selecionados)
        ].copy()

# Inicializar session_state para USI
if 'filtro_usi_waterfall_tc' not in st.session_state:
    if 'USI' in df_total.columns:
        usi_opcoes_temp = get_filter_options(df_total, 'USI')
        st.session_state.filtro_usi_waterfall_tc = ["TC Ext"] if "TC Ext" in usi_opcoes_temp else ["Todos"]
    else:
        st.session_state.filtro_usi_waterfall_tc = ["Todos"]

# Filtro 3: USI (com cache otimizado)
if 'USI' in df_filtrado.columns:
    usi_opcoes = get_filter_options(df_filtrado, 'USI')
    # Validar valores salvos
    default_usi = st.session_state.filtro_usi_waterfall_tc if all(x in usi_opcoes for x in st.session_state.filtro_usi_waterfall_tc) else (["TC Ext"] if "TC Ext" in usi_opcoes else ["Todos"])
    usi_selecionada = st.sidebar.multiselect(
        "Selecione a USI:", usi_opcoes, default=default_usi, key="filtro_usi_waterfall_tc_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_usi_waterfall_tc = usi_selecionada if usi_selecionada else ["Todos"]

    # Filtrar o DataFrame com base na USI
    if "Todos" in usi_selecionada or not usi_selecionada:
        pass  # Manter df_filtrado como está
    else:
        df_filtrado = df_filtrado[
            df_filtrado['USI'].astype(str).isin(usi_selecionada)
        ].copy()

# Filtro 4: Período (com cache otimizado)

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
    if 'filtro_periodo_waterfall_tc' not in st.session_state:
        st.session_state.filtro_periodo_waterfall_tc = "Todos"
    
    # Validar valor salvo
    periodo_default = st.session_state.filtro_periodo_waterfall_tc if st.session_state.filtro_periodo_waterfall_tc in periodo_opcoes else "Todos"
    periodo_index = periodo_opcoes.index(periodo_default) if periodo_default in periodo_opcoes else 0
    
    periodo_selecionado = st.sidebar.selectbox(
        "Selecione o Período:", periodo_opcoes, index=periodo_index, key="filtro_periodo_waterfall_tc_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_periodo_waterfall_tc = periodo_selecionado
    if periodo_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Período'].astype(str) == str(periodo_selecionado)
        ].copy()

# Inicializar session_state para Centro cst
if 'filtro_centro_cst_waterfall_tc' not in st.session_state:
    st.session_state.filtro_centro_cst_waterfall_tc = "Todos"

# Filtro 5: Centro cst (com cache otimizado)
if 'Centrocst' in df_filtrado.columns:
    centro_cst_opcoes = get_filter_options(df_filtrado, 'Centrocst')
    # Validar valor salvo
    centro_cst_default = st.session_state.filtro_centro_cst_waterfall_tc if st.session_state.filtro_centro_cst_waterfall_tc in centro_cst_opcoes else "Todos"
    centro_cst_index = centro_cst_opcoes.index(centro_cst_default) if centro_cst_default in centro_cst_opcoes else 0
    centro_cst_selecionado = st.sidebar.selectbox(
        "Selecione o Centro cst:", centro_cst_opcoes, index=centro_cst_index, key="filtro_centro_cst_waterfall_tc_selectbox"
    )
    # Atualizar session_state
    st.session_state.filtro_centro_cst_waterfall_tc = centro_cst_selecionado
    if centro_cst_selecionado != "Todos":
        df_filtrado = df_filtrado[
            df_filtrado['Centrocst'].astype(str) == str(centro_cst_selecionado)
        ].copy()

# Inicializar session_state para Conta contábil
if 'filtro_conta_contabil_waterfall_tc' not in st.session_state:
    st.session_state.filtro_conta_contabil_waterfall_tc = []

# Filtro 6: Conta contábil (com cache otimizado)
if 'Nºconta' in df_filtrado.columns:
    conta_contabil_opcoes = get_filter_options(df_filtrado, 'Nºconta')[1:]
    # Validar valores salvos
    default_conta = [x for x in st.session_state.filtro_conta_contabil_waterfall_tc if x in conta_contabil_opcoes] if st.session_state.filtro_conta_contabil_waterfall_tc else []
    conta_contabil_selecionadas = st.sidebar.multiselect(
        "Selecione a Conta contábil:", conta_contabil_opcoes, default=default_conta, key="filtro_conta_contabil_waterfall_tc_multiselect"
    )
    # Atualizar session_state
    st.session_state.filtro_conta_contabil_waterfall_tc = conta_contabil_selecionadas
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
    ("Type 07", "Type 07", "multiselect"),
    ("Account", "Account", "multiselect"),
    ("Custo", "Custo", "multiselect"),
    ("Fornecedor", "Fornecedor", "multiselect"),
    ("Fornec.", "Fornec.", "multiselect"),
    ("Tipo", "Tipo", "multiselect")
]

for col_name, label, widget_type in filtros_principais:
    if col_name in df_filtrado.columns:
        # Inicializar session_state para cada filtro principal
        filtro_key = f'filtro_{col_name}_waterfall_tc'
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
        ("Texto breve", "Texto breve", "multiselect")
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
                filtro_key = f'filtro_avancado_{col_name}_waterfall_tc'
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

# Aplicar transformações caras apenas depois do funil completo de filtros.
df_filtrado = _apply_display_transforms_waterfall(
    df_filtrado,
    tipo_visualizacao,
    fator_conversao,
    moeda_codigo,
    taxas_cambio,
)

# Resumo na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**📊 Resumo**")
st.sidebar.write(f"**Linhas:** {df_filtrado.shape[0]:,}")

# Calcular totais se as colunas existirem
if 'Despesa Primaria' in df_filtrado.columns:
    valor_total = df_filtrado['Despesa Primaria'].sum()
    st.sidebar.write(f"**Total Desp. Primária:** {moeda_simbolo} {valor_total:,.2f}")
if 'Custo FP' in df_filtrado.columns:
    total_sum = df_filtrado['Custo FP'].sum()
    st.sidebar.write(f"**Total:** {moeda_simbolo} {total_sum:,.2f}")

# Preparar dados para visualização
# IMPORTANTE: df_visualizacao já nasce do recorte final convertido.
df_visualizacao = df_filtrado.copy()

# ========== CÓDIGO DO TAB5 (adaptado) ==========

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
        # ═══ PERSISTÊNCIA DE TAB: usar radio button em vez de st.tabs para manter estado ═══
        if "active_tab_waterfall_tc" not in st.session_state:
            st.session_state.active_tab_waterfall_tc = "📊 Real"
        
        active_tab_waterfall_tc = st.radio(
            "Selecionar análise:",
            options=["📊 Real", "💰 Budget"],
            index=0 if st.session_state.active_tab_waterfall_tc == "📊 Real" else 1,
            key="active_tab_waterfall_tc",
            horizontal=True,
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Função para ordenar meses cronologicamente
        def sort_mes_unique_waterfall_tc(values):
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
            df_filtrado_waterfall_tc = df_filtrado.copy()
            df_filtrado_waterfall_tc['Período_Ano'] = df_filtrado_waterfall_tc['Período'].astype(str) + ' ' + df_filtrado_waterfall_tc['Ano'].astype(str)
            col_mes_waterfall_tc = 'Período_Ano'
            periodos_unicos = sort_mes_unique_waterfall_tc(df_filtrado_waterfall_tc['Período_Ano'].dropna().unique().tolist())
        elif 'Período' in df_filtrado.columns:
            df_filtrado_waterfall_tc = df_filtrado.copy()
            col_mes_waterfall_tc = 'Período'
            periodos_unicos = sort_mes_unique_waterfall_tc(df_filtrado['Período'].dropna().unique().tolist())
        else:
            st.warning("⚠️ Coluna 'Período' não encontrada nos dados.")
            periodos_unicos = []
            df_filtrado_waterfall_tc = df_filtrado.copy()
            col_mes_waterfall_tc = None
        
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
        
        # TAB REAL
        if active_tab_waterfall_tc == "📊 Real":
            st.subheader("📊 Análise Real")

            # ── Escopo de Veículo (Todos vs modelo específico) ──
            _veic_opcoes_real = []
            if 'Veículo' in df_filtrado_waterfall_tc.columns:
                _veic_opcoes_real = sorted(
                    [v for v in df_filtrado_waterfall_tc['Veículo'].dropna().unique().tolist()
                     if v and str(v).strip() and v != 'Sem Veículo']
                )
            escopo_veiculo_real = st.radio(
                "🚗 Escopo de Veículo:",
                ["Todos (TC Total)"] + _veic_opcoes_real,
                index=0, horizontal=True,
                key="escopo_veiculo_real_wf",
                help="Todos = visão consolidada sem quebra por modelo. Selecione um modelo para análise específica.",
            )

            modo_comparacao = st.radio(
                "Tipo de comparação:",
                options=["Mês a Mês", "Ano a Ano", "Semestre", "Quarter"],
                index=0,
                key="modo_comparacao_waterfall_tc",
                horizontal=True,
                help="Selecione o modo de comparação entre períodos"
            )
            
            # Seleção de períodos
            if modo_comparacao == "Mês a Mês":
                if periodos_unicos:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        # Inicializar session_state para mes_inicial se não existir (ANTES do widget)
                        if 'mes_inicial_waterfall_tc' not in st.session_state:
                            # Usar primeiro período disponível
                            st.session_state.mes_inicial_waterfall_tc = periodos_unicos[0] if len(periodos_unicos) > 0 else None
                        
                        # Verificar se o valor salvo ainda é válido (ANTES do widget)
                        if st.session_state.mes_inicial_waterfall_tc not in periodos_unicos:
                            st.session_state.mes_inicial_waterfall_tc = periodos_unicos[0] if len(periodos_unicos) > 0 else None
                        
                        index_inicial = periodos_unicos.index(st.session_state.mes_inicial_waterfall_tc) if st.session_state.mes_inicial_waterfall_tc in periodos_unicos else 0
                        # O selectbox automaticamente atualiza o session_state, não precisamos fazer manualmente
                        mes_inicial = st.selectbox("Mês inicial:", periodos_unicos, index=index_inicial, key="mes_inicial_waterfall_tc")
                        
                    with col_b:
                        # Inicializar session_state para mes_final se não existir (ANTES do widget)
                        if 'mes_final_waterfall_tc' not in st.session_state:
                            # Usar último período disponível
                            st.session_state.mes_final_waterfall_tc = periodos_unicos[-1] if len(periodos_unicos) > 0 else None
                        
                        # Verificar se o valor salvo ainda é válido (ANTES do widget)
                        if st.session_state.mes_final_waterfall_tc not in periodos_unicos:
                            st.session_state.mes_final_waterfall_tc = periodos_unicos[-1] if len(periodos_unicos) > 0 else None
                        
                        index_final = periodos_unicos.index(st.session_state.mes_final_waterfall_tc) if st.session_state.mes_final_waterfall_tc in periodos_unicos else (len(periodos_unicos)-1 if len(periodos_unicos) > 0 else 0)
                        # O selectbox automaticamente atualiza o session_state, não precisamos fazer manualmente
                        mes_final = st.selectbox("Mês final:", periodos_unicos, index=index_final, key="mes_final_waterfall_tc")
                        
                    meses_selecionados = [mes_inicial, mes_final]
                else:
                    st.warning("⚠️ Nenhum período encontrado nos dados.")
            
            elif modo_comparacao == "Ano a Ano":
                if 'Ano' in df_filtrado.columns:
                    anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                    if len(anos_disponiveis) >= 2:
                        col_ano1, col_ano2 = st.columns(2)
                        with col_ano1:
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_waterfall_tc")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1), key="ano_final_waterfall_tc")
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
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_sem_waterfall_tc")
                        with col_sem1:
                            semestre_inicial = st.selectbox("Semestre inicial:", [1, 2], index=0, key="semestre_inicial_waterfall_tc")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_sem_waterfall_tc")
                        with col_sem2:
                            semestre_final = st.selectbox("Semestre final:", [1, 2], index=1, key="semestre_final_waterfall_tc")
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
                            ano_inicial = st.selectbox("Ano inicial:", anos_disponiveis, index=0, key="ano_inicial_q_waterfall_tc")
                        with col_q1:
                            trimestre_inicial = st.selectbox("Quarter inicial:", [1, 2, 3, 4], index=0, key="trimestre_inicial_waterfall_tc")
                        with col_ano2:
                            ano_final = st.selectbox("Ano final:", anos_disponiveis, index=min(1, len(anos_disponiveis)-1) if len(anos_disponiveis) > 1 else 0, key="ano_final_q_waterfall_tc")
                        with col_q2:
                            trimestre_final = st.selectbox("Quarter final:", [1, 2, 3, 4], index=1, key="trimestre_final_waterfall_tc")
                        mes_inicial = f"{ano_inicial} Q{trimestre_inicial}"
                        mes_final = f"{ano_final} Q{trimestre_final}"
                        meses_selecionados = [mes_inicial, mes_final]
                    else:
                        st.warning("⚠️ É necessário ter pelo menos 1 ano de dados para comparação de quarters.")
                else:
                    st.warning("⚠️ Coluna 'Ano' não encontrada nos dados.")
            
            # Garantir que períodos sejam selecionados automaticamente na primeira vez
            if not meses_selecionados or len(meses_selecionados) < 2:
                # Tentar usar valores padrão se disponíveis
                if modo_comparacao == "Mês a Mês" and periodos_unicos and len(periodos_unicos) >= 2:
                    if 'mes_inicial_waterfall_tc' in st.session_state and 'mes_final_waterfall_tc' in st.session_state:
                        mes_inicial = st.session_state.mes_inicial_waterfall_tc
                        mes_final = st.session_state.mes_final_waterfall_tc
                        meses_selecionados = [mes_inicial, mes_final]
                    else:
                        mes_inicial = periodos_unicos[0]
                        mes_final = periodos_unicos[-1] if len(periodos_unicos) > 1 else periodos_unicos[0]
                        meses_selecionados = [mes_inicial, mes_final]
                        st.session_state.mes_inicial_waterfall_tc = mes_inicial
                        st.session_state.mes_final_waterfall_tc = mes_final
            
            # Verificar se períodos foram selecionados e se existem dados válidos
            periodos_validos = False
            if meses_selecionados and len(meses_selecionados) >= 2:
                # Verificar se os períodos selecionados existem nos dados antes de gerar gráfico
                if modo_comparacao == "Mês a Mês" and periodos_unicos:
                    # Verificar se ambos os períodos existem na lista de períodos únicos
                    if mes_inicial in periodos_unicos and mes_final in periodos_unicos:
                        periodos_validos = True
                elif modo_comparacao == "Ano a Ano":
                    # Verificar se os anos existem nos dados
                    if 'Ano' in df_filtrado.columns:
                        anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                        if ano_inicial in anos_disponiveis and ano_final in anos_disponiveis:
                            periodos_validos = True
                elif modo_comparacao in ["Semestre", "Quarter"]:
                    # Verificar se os anos existem nos dados
                    if 'Ano' in df_filtrado.columns:
                        anos_disponiveis = sorted(df_filtrado['Ano'].dropna().unique().tolist())
                        if ano_inicial in anos_disponiveis and ano_final in anos_disponiveis:
                            periodos_validos = True
            
            # Exibir gráfico waterfall se períodos são válidos
            if not meses_selecionados or len(meses_selecionados) < 2 or not periodos_validos:
                st.info("ℹ️ Selecione os períodos para comparação acima para visualizar a análise waterfall.")
            else:
                try:
                    # Determinar coluna de valor baseado no tipo de visualização
                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                        # Para CPU, precisamos de 'Total' e acesso ao df_volume
                        if 'Custo FP' in df_filtrado.columns:
                            # Verificar se temos acesso ao df_volume para calcular CPU
                            if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                col_valor = 'Custo FP'
                            elif 'CPU' in df_visualizacao.columns:
                                col_valor = 'CPU'
                            else:
                                st.error("❌ Dados insuficientes para calcular CPU. É necessário ter dados de volume disponíveis.")
                                st.stop()
                        else:
                            st.error("❌ Coluna 'Custo FP' não encontrada para calcular CPU.")
                            st.stop()
                    else:
                        if 'Custo FP' in df_filtrado.columns:
                            col_valor = 'Custo FP'
                        elif 'Despesa Primaria' in df_filtrado.columns:
                            col_valor = 'Despesa Primaria'
                        else:
                            st.error("❌ Coluna 'Custo FP' ou 'Despesa Primaria' não encontrada.")
                            st.stop()
                    
                    # Preparar dados para análise
                    df_analise = df_filtrado_waterfall_tc.copy()

                    # ═══ Bloco 3: Filtro defensivo — remover 'Sem Veículo' de parquets antigos ═══
                    if 'Veículo' in df_analise.columns:
                        df_analise = df_analise[df_analise['Veículo'] != 'Sem Veículo'].copy()

                    # Aplicar escopo de veículo selecionado
                    # ═══ Bloco 4: quando "Todos", NÃO agregar imediatamente — manter coluna Veículo
                    #     para que apareça como dimensão disponível. A agregação é feita DEPOIS
                    #     que o usuário escolhe a dimensão (lazy aggregation). ═══
                    _todos_mode_real = False
                    if escopo_veiculo_real == "Todos (TC Total)":
                        _todos_mode_real = True
                        # NÃO chamar _agregar_sem_veiculo() aqui
                    elif 'Veículo' in df_analise.columns:
                        df_analise = df_analise[
                            df_analise['Veículo'] == escopo_veiculo_real
                        ].copy()

                    # Criar df_vol_filtrado aplicando os mesmos filtros (necessário para cálculo do Flex Volume no gráfico)
                    # 🔧 CORREÇÃO CRÍTICA: Aplicar TODOS os filtros que existem em df_filtrado_waterfall_tc (mesma lógica do app.py)
                    df_vol_filtrado = None
                    if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                        df_vol_filtrado = df_volume.copy()
                        
                        # Aplicar TODOS os filtros que existem em df_filtrado_waterfall_tc (mesma lógica do app.py linha 4756)
                        if df_filtrado_waterfall_tc is not None and len(df_filtrado_waterfall_tc) > 0:
                            # Aplicar filtro de Veículo
                            if 'Veículo' in df_filtrado_waterfall_tc.columns and 'Veículo' in df_vol_filtrado.columns:
                                veiculos_filtrados = df_filtrado_waterfall_tc['Veículo'].dropna().unique()
                                if len(veiculos_filtrados) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['Veículo'].isin(veiculos_filtrados)
                                    ].copy()
                            
                            # Aplicar filtro de Oficina
                            if 'Oficina' in df_filtrado_waterfall_tc.columns and 'Oficina' in df_vol_filtrado.columns:
                                oficinas_filtradas = df_filtrado_waterfall_tc['Oficina'].dropna().unique()
                                if len(oficinas_filtradas) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['Oficina'].isin(oficinas_filtradas)
                                    ].copy()
                            
                            # 🔧 CORREÇÃO CRÍTICA: Aplicar filtro de USI (importante para TC Ext)
                            if 'USI' in df_filtrado_waterfall_tc.columns and 'USI' in df_vol_filtrado.columns:
                                usi_filtradas = df_filtrado_waterfall_tc['USI'].dropna().unique()
                                if len(usi_filtradas) > 0:
                                    df_vol_filtrado = df_vol_filtrado[
                                        df_vol_filtrado['USI'].isin(usi_filtradas)
                                    ].copy()
                            
                            # Aplicar outros filtros comuns (se existirem) - mesma lógica do app.py linha 4782
                            colunas_filtro_comuns = ['Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                            for col_filtro in colunas_filtro_comuns:
                                if col_filtro in df_filtrado_waterfall_tc.columns and col_filtro in df_vol_filtrado.columns:
                                    valores_filtrados = df_filtrado_waterfall_tc[col_filtro].dropna().unique()
                                    if len(valores_filtrados) > 0:
                                        df_vol_filtrado = df_vol_filtrado[
                                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                                        ].copy()
                            
                            # Aplicar filtros avançados (se existirem)
                            colunas_filtro_avancados = ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve', 'Account']
                            for col_filtro in colunas_filtro_avancados:
                                if col_filtro in df_filtrado_waterfall_tc.columns and col_filtro in df_vol_filtrado.columns:
                                    valores_filtrados = df_filtrado_waterfall_tc[col_filtro].dropna().unique()
                                    if len(valores_filtrados) > 0:
                                        df_vol_filtrado = df_vol_filtrado[
                                            df_vol_filtrado[col_filtro].isin(valores_filtrados)
                                        ].copy()
                        else:
                            # Fallback: usar filtros do session_state (comportamento antigo)
                            # Filtros de Oficina
                            oficinas_selecionadas = st.session_state.get('filtro_oficina_waterfall_tc', ["Todos"])
                            if oficinas_selecionadas and "Todos" not in oficinas_selecionadas and len(oficinas_selecionadas) > 0:
                                if 'Oficina' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Oficina'].isin(oficinas_selecionadas)]
                            
                            # Filtros de Veículo
                            veiculos_selecionados = st.session_state.get('filtro_veiculo_waterfall_tc', ["Todos"])
                            if veiculos_selecionados and "Todos" not in veiculos_selecionados and len(veiculos_selecionados) > 0:
                                if 'Veículo' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['Veículo'].isin(veiculos_selecionados)]
                            
                            # Filtros de USI
                            usis_selecionadas = st.session_state.get('filtro_usi_waterfall_tc', ["Todos"])
                            if usis_selecionadas and "Todos" not in usis_selecionadas and len(usis_selecionadas) > 0:
                                if 'USI' in df_vol_filtrado.columns:
                                    df_vol_filtrado = df_vol_filtrado[df_vol_filtrado['USI'].isin(usis_selecionadas)]
                        
                        # ═══ CORREÇÃO: Aplicar escopo de veículo ao volume (mesma lógica do Home) ═══
                        if escopo_veiculo_real != "Todos (TC Total)" and df_vol_filtrado is not None:
                            if 'Veículo' in df_vol_filtrado.columns:
                                df_vol_filtrado = df_vol_filtrado[
                                    df_vol_filtrado['Veículo'] == escopo_veiculo_real
                                ].copy()
                        
                        # Criar coluna Período_Ano no df_vol_filtrado se necessário (mesma lógica do df_filtrado_waterfall_tc)
                        if col_mes_waterfall_tc == 'Período_Ano':
                            if 'Período' in df_vol_filtrado.columns and 'Ano' in df_vol_filtrado.columns:
                                df_vol_filtrado['Período_Ano'] = df_vol_filtrado['Período'].astype(str) + ' ' + df_vol_filtrado['Ano'].astype(str)
                    
                    # Filtrar e agrupar dados baseado no modo de comparação para obter categorias
                    if modo_comparacao == "Mês a Mês":
                        if col_mes_waterfall_tc:
                            df_temp = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
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
                        if col_mes_waterfall_tc:
                            df_temp = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
                        else:
                            df_temp = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                    
                    # Obter dimensões de categoria disponíveis
                    dims_base_real = [
                        "Type 05", "Type 06", "Type 07", "Oficina",
                        "Veículo", "Custo", "Account", "Texto breve"
                    ]
                    colunas_real_disponiveis = set(df_analise.columns) & set(df_temp.columns)
                    dims_cat = [
                        c for c in dims_base_real
                        if c in colunas_real_disponiveis
                    ]
                    
                    if not dims_cat:
                        st.warning("⚠️ Nenhuma dimensão de categoria encontrada nos dados.")
                    else:
                        # Filtro de Type 05/06 + dimensão da categoria + Oficina lado a lado
                        col_t05_tc_real, col_t06_tc_real, col_dim_tc_real, col_ofic_tc_real = st.columns(4)
                        with col_t05_tc_real:
                            type05_options_tc_real = ["Todos"]
                            if 'Type 05' in df_analise.columns:
                                type05_options_tc_real += sorted(df_analise['Type 05'].dropna().astype(str).unique().tolist())
                            type05_inline_sel_tc_real = st.multiselect(
                                "Type 05:",
                                type05_options_tc_real,
                                default=["Todos"],
                                key="type05_inline_waterfall_tc_real"
                            )
                        with col_t06_tc_real:
                            type06_options_tc_real = ["Todos"]
                            if 'Type 06' in df_analise.columns:
                                type06_options_tc_real += sorted(df_analise['Type 06'].dropna().astype(str).unique().tolist())
                            type06_inline_sel_tc_real = st.multiselect(
                                "Type 06:",
                                type06_options_tc_real,
                                default=["Todos"],
                                key="type06_inline_waterfall_tc_real"
                            )
                        with col_dim_tc_real:
                            chosen_dim_waterfall_tc = st.selectbox(
                                "Dimensão da categoria:",
                                dims_cat,
                                index=min(1, len(dims_cat)-1) if len(dims_cat) > 1 else 0,
                                key="dim_waterfall_tc_real"
                            )
                        with col_ofic_tc_real:
                            # Filtro de oficina inline
                            oficinas_inline_tc_real = ["Todos"]
                            if 'Oficina' in df_analise.columns:
                                oficinas_inline_tc_real += sorted(df_analise['Oficina'].dropna().astype(str).unique().tolist())
                            oficina_inline_sel_tc_real = st.multiselect(
                                "Oficina:",
                                oficinas_inline_tc_real,
                                default=["Todos"],
                                key="oficina_inline_waterfall_tc_real"
                            )
                        
                        # ═══ IMPORTANTE: Salvar cópia ANTES do filtro de oficina para o painel inferior ═══
                        # O painel por oficina precisa de TODOS os dados, sem filtro de oficina
                        df_analise_painel_base = df_analise.copy()
                        df_vol_filtrado_painel_base = df_vol_filtrado.copy() if df_vol_filtrado is not None else None
                        
                        # Aplicar filtro de oficina inline (apenas para o gráfico principal)
                        if 'Oficina' in df_analise.columns and oficina_inline_sel_tc_real and "Todos" not in oficina_inline_sel_tc_real:
                            df_analise = df_analise[df_analise['Oficina'].astype(str).isin(oficina_inline_sel_tc_real)].copy()
                            if 'Oficina' in df_temp.columns:
                                df_temp = df_temp[df_temp['Oficina'].astype(str).isin(oficina_inline_sel_tc_real)].copy()
                        
                        # Aplicar filtros inline de Type 05 e Type 06 (para gráfico E tabela)
                        if 'Type 05' in df_analise.columns and type05_inline_sel_tc_real and "Todos" not in type05_inline_sel_tc_real:
                            df_analise = df_analise[df_analise['Type 05'].astype(str).isin(type05_inline_sel_tc_real)].copy()
                            if 'Type 05' in df_temp.columns:
                                df_temp = df_temp[df_temp['Type 05'].astype(str).isin(type05_inline_sel_tc_real)].copy()
                        if 'Type 06' in df_analise.columns and type06_inline_sel_tc_real and "Todos" not in type06_inline_sel_tc_real:
                            df_analise = df_analise[df_analise['Type 06'].astype(str).isin(type06_inline_sel_tc_real)].copy()
                            if 'Type 06' in df_temp.columns:
                                df_temp = df_temp[df_temp['Type 06'].astype(str).isin(type06_inline_sel_tc_real)].copy()
                        
                        # ═══ Salvar df_analise ANTES da agregação para a tabela Resumo Geral ═══
                        # A tabela agrupa por Custo/Type 05/Type 06/Account e NÃO deve depender
                        # da dimensão selecionada (que pode alterar df_analise via _agregar_sem_veiculo).
                        df_analise_tabela = df_analise.copy()

                        # ═══ Bloco 4: Lazy aggregation — se "Todos" e dim ≠ Veículo, agregar agora ═══
                        if _todos_mode_real and chosen_dim_waterfall_tc != "Veículo":
                            df_analise = _agregar_sem_veiculo(df_analise)
                            # Recalcular df_temp sem veículo
                            if 'Veículo' in df_temp.columns:
                                df_temp = _agregar_sem_veiculo(df_temp)
                        
                        # Obter todas as categorias disponíveis
                        cats_all = sorted([str(x).strip() for x in df_analise[chosen_dim_waterfall_tc].dropna().unique().tolist() if str(x).strip() != ""])
                        total_cats = max(1, len(cats_all))
                        
                        # Calcular categorias ordenadas por impacto absoluto ANTES do slider
                        # Isso permite ordenar corretamente por valor absoluto (maiores impactos, + ou -)
                        cats_ordenadas_por_impacto = []
                        if not df_temp.empty:
                            if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_temp.columns and 'Volume' in df_temp.columns:
                                vol_mf = (df_temp.groupby(chosen_dim_waterfall_tc).agg({'Custo FP': 'sum', 'Volume': 'sum'}).reset_index())
                                vol_mf['CPU'] = vol_mf['Custo FP'] / vol_mf['Volume'].replace(0, 1)
                                # Ordenar por valor absoluto (maiores impactos, positivos ou negativos)
                                vol_mf['abs_CPU'] = vol_mf['CPU'].abs()
                                vol_mf = vol_mf.sort_values('abs_CPU', ascending=False)
                                cats_ordenadas_por_impacto = [str(c).strip() for c in list(vol_mf[chosen_dim_waterfall_tc])]
                            else:
                                vol_mf = df_temp.groupby(chosen_dim_waterfall_tc)[col_valor].sum().reset_index()
                                # Ordenar por valor absoluto (maiores impactos, positivos ou negativos)
                                vol_mf['abs_valor'] = vol_mf[col_valor].abs()
                                vol_mf = vol_mf.sort_values('abs_valor', ascending=False)
                                cats_ordenadas_por_impacto = [str(c).strip() for c in list(vol_mf[chosen_dim_waterfall_tc])]
                        
                        # Se não conseguiu ordenar, usar lista original
                        if not cats_ordenadas_por_impacto:
                            cats_ordenadas_por_impacto = cats_all
                        
                        # Controle: Quantidade de categorias a exibir (Top N)
                        # REMOVIDO limite de 20 - agora permite todas as categorias disponíveis
                        # Garantir que o slider tenha um range válido (min < max)
                        if total_cats <= 1:
                            # Se há apenas 1 categoria ou nenhuma, não mostrar slider
                            max_cats = total_cats
                            st.info(f"ℹ️ Apenas {total_cats} categoria disponível para esta dimensão.")
                        else:
                            # Verificar se há um valor desejado do slider (quando usuário seleciona categorias manualmente)
                            slider_desired_key = "max_cats_waterfall_tc_desired"
                            if slider_desired_key in st.session_state:
                                # Usar o valor desejado e remover da session_state para próxima execução
                                default_value = st.session_state[slider_desired_key]
                                del st.session_state[slider_desired_key]
                                # Garantir que o valor está dentro dos limites
                                default_value = max(1, min(default_value, total_cats))
                            else:
                                default_value = min(total_cats, 20)  # Valor padrão ainda 20 para não sobrecarregar inicialmente
                            
                            # Sanitizar session_state do slider para o novo range (evita exceção quando dimensão muda)
                            _slider_key_real = "max_cats_waterfall_tc"
                            if _slider_key_real in st.session_state:
                                st.session_state[_slider_key_real] = max(1, min(int(st.session_state[_slider_key_real]), total_cats))
                            max_cats = st.slider(
                                f"Quantidade de categorias a exibir (Top N) (Total: {total_cats}):",
                                min_value=1,
                                max_value=total_cats,
                                value=default_value,
                                key=_slider_key_real
                            )
                        
                        # Selecionar top N categorias baseado no slider (ordenadas por impacto absoluto)
                        # Se max_cats = total_cats, selecionar TODAS as categorias
                        if max_cats >= total_cats:
                            top_cats_selecionadas = cats_all  # Todas as categorias
                        else:
                            top_cats_selecionadas = cats_ordenadas_por_impacto[:max_cats]  # Top N por impacto absoluto
                        
                        # Opções de categorias
                        cats_options = ["Todos"] + cats_all
                        
                        # IMPORTANTE: Filtrar top_cats_selecionadas para garantir que todas existem em cats_all
                        top_cats_selecionadas = [c for c in top_cats_selecionadas if c in cats_all]
                        
                        # Verificar se o slider mudou comparando com o valor anterior
                        slider_key_prev = f"max_cats_waterfall_tc_prev"
                        if slider_key_prev not in st.session_state:
                            st.session_state[slider_key_prev] = max_cats
                        
                        slider_mudou = st.session_state[slider_key_prev] != max_cats
                        st.session_state[slider_key_prev] = max_cats
                        
                        # Se o slider mudou, forçar atualização do multiselect
                        # Usar uma chave única baseada no valor do slider para forçar recriação do widget
                        multiselect_key = f"cats_waterfall_tc_{max_cats}"
                        
                        # Controle: Categorias (uma ou mais)
                        # Usar uma chave fixa para o multiselect
                        multiselect_key_fixed = "cats_waterfall_tc_multiselect"
                        
                        # Determinar o valor inicial do multiselect
                        if slider_mudou:
                            # Se o slider mudou, atualizar o valor no session_state ANTES de criar o widget
                            cats_selecionadas_atual = top_cats_selecionadas
                            # Atualizar diretamente no session_state - isso força o multiselect a usar o novo valor
                            st.session_state[multiselect_key_fixed] = cats_selecionadas_atual
                            st.session_state["cats_waterfall_tc_saved_current"] = cats_selecionadas_atual
                        elif multiselect_key_fixed in st.session_state:
                            # Se já existe valor no multiselect (usuário selecionou manualmente), usar ele
                            cats_selecionadas_atual = st.session_state[multiselect_key_fixed]
                            # Verificar se ainda são válidas
                            cats_selecionadas_atual = [c for c in cats_selecionadas_atual if c in cats_all]
                        else:
                            # Primeira vez, usar top_cats_selecionadas
                            cats_selecionadas_atual = top_cats_selecionadas
                            st.session_state[multiselect_key_fixed] = cats_selecionadas_atual
                            st.session_state["cats_waterfall_tc_saved_current"] = cats_selecionadas_atual
                        
                        # Garantir que cats_selecionadas_atual contém apenas valores válidos
                        cats_selecionadas_atual = [c for c in cats_selecionadas_atual if c in cats_all]
                        
                        # Sanitizar session_state: remover valores que não existem mais nas opções
                        if multiselect_key_fixed in st.session_state:
                            _vals_limpos = [v for v in st.session_state[multiselect_key_fixed] if v in cats_options]
                            if not _vals_limpos:
                                _vals_limpos = top_cats_selecionadas[:min(max_cats, len(top_cats_selecionadas))]
                            st.session_state[multiselect_key_fixed] = _vals_limpos
                            cats_selecionadas_atual = _vals_limpos

                        # Criar o multiselect
                        cats_sel_raw = st.multiselect(
                            "Categorias (uma ou mais):",
                            cats_options,
                            default=cats_selecionadas_atual,
                            key=multiselect_key_fixed
                        )
                        
                        # Processar seleção do usuário
                        # IMPORTANTE: Quando o usuário seleciona categorias manualmente, NÃO atualizar o slider
                        # Apenas salvar a seleção e usar para o gráfico
                        if cats_sel_raw and len(cats_sel_raw) > 0:
                            # Remover "Todos" se estiver presente e usar todas as categorias
                            if "Todos" in cats_sel_raw:
                                cats_sel = cats_all
                            else:
                                # Usar as categorias selecionadas pelo usuário
                                cats_sel = cats_sel_raw
                            # Salvar a seleção atual (sem atualizar o slider)
                            st.session_state["cats_waterfall_tc_saved_current"] = cats_sel
                        else:
                            # Se vazio, usar exatamente o que o slider indica
                            cats_sel = top_cats_selecionadas
                            st.session_state["cats_waterfall_tc_saved_current"] = cats_sel
                    
                    # Filtrar e agrupar dados baseado no modo de comparação
                    if modo_comparacao == "Mês a Mês":
                        if col_mes_waterfall_tc:
                            df_m1 = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
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
                        if col_mes_waterfall_tc:
                            df_m1 = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
                        else:
                            df_m1 = df_analise[df_analise['Período'].astype(str) == str(mes_inicial)].copy()
                            df_m2 = df_analise[df_analise['Período'].astype(str) == str(mes_final)].copy()
                    
                    # Verificar se os períodos foram realmente selecionados antes de processar
                    if not periodos_validos or not meses_selecionados or len(meses_selecionados) < 2:
                        st.info("ℹ️ Selecione os períodos para comparação acima para visualizar a análise waterfall.")
                    elif df_m1.empty or df_m2.empty:
                        _msg_vazio = []
                        if df_m1.empty:
                            _msg_vazio.append(f"Período inicial ({mes_inicial if modo_comparacao == 'Mês a Mês' else ano_inicial})")
                        if df_m2.empty:
                            _msg_vazio.append(f"Período final ({mes_final if modo_comparacao == 'Mês a Mês' else ano_final})")
                        _veiculo_info = f" para veículo '{escopo_veiculo_real}'" if escopo_veiculo_real != 'Todos (TC Total)' else ''
                        st.warning(f"⚠️ Não há dados suficientes{_veiculo_info}. Sem dados em: {', '.join(_msg_vazio)}.")
                    else:
                        if chosen_dim_waterfall_tc not in df_m1.columns or chosen_dim_waterfall_tc not in df_m2.columns:
                            st.warning(
                                f"⚠️ A dimensão '{chosen_dim_waterfall_tc}' não está disponível no Real para este recorte. "
                                "Usando '(Não informado)' como agrupador."
                            )
                            if chosen_dim_waterfall_tc not in df_m1.columns:
                                df_m1 = df_m1.copy()
                                df_m1[chosen_dim_waterfall_tc] = "(Não informado)"
                            if chosen_dim_waterfall_tc not in df_m2.columns:
                                df_m2 = df_m2.copy()
                                df_m2[chosen_dim_waterfall_tc] = "(Não informado)"

                        # Calcular totais por dimensão selecionada
                        # IMPORTANTE: No modo CPU, usar volumes do df_volume (não do df_m1/df_m2)
                        if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_m1.columns:
                            # Buscar volumes do df_volume filtrado
                            if df_vol_filtrado is not None and not df_vol_filtrado.empty:
                                # Filtrar volumes pelos períodos
                                if modo_comparacao == "Mês a Mês":
                                    if col_mes_waterfall_tc:
                                        df_vol_m1_graph = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2_graph = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
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
                                if chosen_dim_waterfall_tc in df_vol_m1_graph.columns:
                                    vol_m1_por_cat = df_vol_m1_graph.groupby(chosen_dim_waterfall_tc)['Volume'].sum()
                                else:
                                    vol_m1_por_cat = pd.Series()
                                
                                if chosen_dim_waterfall_tc in df_vol_m2_graph.columns:
                                    vol_m2_por_cat = df_vol_m2_graph.groupby(chosen_dim_waterfall_tc)['Volume'].sum()
                                else:
                                    vol_m2_por_cat = pd.Series()
                            else:
                                volume_m1_graph = 0
                                volume_m2_graph = 0
                                vol_m1_por_cat = pd.Series()
                                vol_m2_por_cat = pd.Series()
                            
                            # Calcular CPU por categoria
                            if not vol_m1_por_cat.empty:
                                total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall_tc)['Custo FP'].sum()
                                g1 = total_m1_por_cat / vol_m1_por_cat.replace(0, 1)
                                g1 = g1.fillna(0)
                            else:
                                # Se não temos volumes por categoria, usar volume total para calcular CPU
                                total_m1_por_cat = df_m1.groupby(chosen_dim_waterfall_tc)['Custo FP'].sum()
                                g1 = total_m1_por_cat / volume_m1_graph if volume_m1_graph > 0 else total_m1_por_cat / 1
                                g1 = g1.fillna(0)
                            
                            if not vol_m2_por_cat.empty:
                                total_m2_por_cat = df_m2.groupby(chosen_dim_waterfall_tc)['Custo FP'].sum()
                                g2 = total_m2_por_cat / vol_m2_por_cat.replace(0, 1)
                                g2 = g2.fillna(0)
                            else:
                                # Se não temos volumes por categoria, usar volume total para calcular CPU
                                total_m2_por_cat = df_m2.groupby(chosen_dim_waterfall_tc)['Custo FP'].sum()
                                g2 = total_m2_por_cat / volume_m2_graph if volume_m2_graph > 0 else total_m2_por_cat / 1
                                g2 = g2.fillna(0)
                            
                            # Calcular totais em CPU usando volumes do df_volume
                            total_m1_all = (df_m1['Custo FP'].sum() / volume_m1_graph) if volume_m1_graph > 0 else 0
                            total_m2_all = (df_m2['Custo FP'].sum() / volume_m2_graph) if volume_m2_graph > 0 else 0
                        else:
                            g1 = df_m1.groupby(chosen_dim_waterfall_tc)[col_valor].sum()
                            g2 = df_m2.groupby(chosen_dim_waterfall_tc)[col_valor].sum()
                            
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
                                if col_mes_waterfall_tc:
                                    df_vol_m1_cat = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_inicial)].copy()
                                    df_vol_m2_cat = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
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
                            volume_m1_cat = 0
                            volume_m2_cat = 0
                            df_vol_m1_cat = pd.DataFrame()
                            df_vol_m2_cat = pd.DataFrame()
                        
                        # Calcular Flex Mês 1 para cada categoria (mesma lógica da tabela)
                        for cat in cats_sel:
                            if cat in g1.index:
                                valor_m1_cat = float(g1.get(cat, 0.0))
                                
                                # IMPORTANTE: No modo CPU, precisamos calcular volumes por categoria
                                if tipo_visualizacao == "CPU (Custo por Unidade)" and df_vol_filtrado is not None and not df_vol_filtrado.empty:
                                    # Filtrar volumes por categoria
                                    if chosen_dim_waterfall_tc in df_vol_m1_cat.columns:
                                        volume_m1_cat_especifico = df_vol_m1_cat[df_vol_m1_cat[chosen_dim_waterfall_tc].astype(str) == str(cat)]['Volume'].sum()
                                    else:
                                        volume_m1_cat_especifico = volume_m1_cat  # Usar volume total se não tiver a coluna
                                    
                                    if chosen_dim_waterfall_tc in df_vol_m2_cat.columns:
                                        volume_m2_cat_especifico = df_vol_m2_cat[df_vol_m2_cat[chosen_dim_waterfall_tc].astype(str) == str(cat)]['Volume'].sum()
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
                                    df_cat_m1 = df_m1[df_m1[chosen_dim_waterfall_tc].astype(str) == str(cat)]
                                    if not df_cat_m1.empty:
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            # Em CPU, usar 'Total' (Custo Total) para calcular Flex
                                            total_fixo_cat = df_cat_m1[df_cat_m1['Custo'] == 'Fixo']['Custo FP'].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Fixo']) > 0 else 0
                                            total_variavel_cat = df_cat_m1[df_cat_m1['Custo'] == 'Variável']['Custo FP'].sum() if len(df_cat_m1[df_cat_m1['Custo'] == 'Variável']) > 0 else 0
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
                        # IMPORTANTE: Não limitar pelo slider - usar todas as categorias selecionadas (cats_sel)
                        if labels_cats:
                            sorted_idx = sorted(range(len(values_cats)), key=lambda i: abs(values_cats[i]), reverse=True)
                            labels_cats = [labels_cats[i] for i in sorted_idx]
                            values_cats = [values_cats[i] for i in sorted_idx]
                            
                            # NÃO limitar - usar todas as categorias selecionadas pelo usuário
                            # O gráfico deve refletir exatamente o que foi selecionado no multiselect
                        
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
                                    if col_mes_waterfall_tc:
                                        df_vol_m1 = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_inicial)].copy()
                                        df_vol_m2 = df_vol_filtrado[df_vol_filtrado[col_mes_waterfall_tc].astype(str) == str(mes_final)].copy()
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
                            
                            # Base_CustoFP = Total do mês 1 em Custo Total
                            # IMPORTANTE: No modo CPU, total_m1_all já está em CPU, então usar df_m1['Custo FP'].sum() diretamente
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                base_total = df_m1['Custo FP'].sum()  # Total em Custo Total
                            else:
                                base_total = total_m1_all  # Já está em Custo Total
                            
                            # Calcular Flex BUD usando a mesma regra das tabelas
                            if 'Custo' in df_m1.columns:
                                # Separar Fixo e Variável
                                if tipo_visualizacao == "CPU (Custo por Unidade)":
                                    # Em CPU, usar coluna 'Total' (Custo Total) para calcular Flex
                                    total_fixo_m1 = df_m1[df_m1['Custo'] == 'Fixo']['Custo FP'].sum() if len(df_m1[df_m1['Custo'] == 'Fixo']) > 0 else 0
                                    total_variavel_m1 = df_m1[df_m1['Custo'] == 'Variável']['Custo FP'].sum() if len(df_m1[df_m1['Custo'] == 'Variável']) > 0 else 0
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
                            
                            # Flex Bud Fixo = Base_CustoFP onde Custo == 'Fixo' (não varia) - linha 1420
                            flex_bud_fixo = total_fixo_m1
                            
                            # Flex Bud Variável = Base_CustoFP × proporcao_volume onde Custo == 'Variável' - linha 1421
                            flex_bud_variavel = total_variavel_m1 * proporcao_volume
                            
                            # Flex BUD = Flex Bud Fixo + Flex Bud Variável
                            flex_bud_total = flex_bud_fixo + flex_bud_variavel
                            
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                # Usar os mesmos volumes usados no cálculo de total_m1_all e total_m2_all
                                volume_m1_para_calculo = volume_m1_graph if 'volume_m1_graph' in locals() and volume_m1_graph > 0 else volume_real_m1
                                volume_m2_para_calculo = volume_m2_graph if 'volume_m2_graph' in locals() and volume_m2_graph > 0 else volume_real_m2
                                
                                # Converter para CPU (mesma lógica da tabela linhas 1428-1432)
                                # BUD (Mês 1) = Base_CustoFP (Custo Total) / volume_m1
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
                        
                        # Totais para o gráfico (sem separação Redis — Redis já distribuído nos Accounts reais)
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
                        labels_waterfall_tc = [f"{mes_inicial}"]
                        values_waterfall_tc = [valor_inicial_grafico]
                        measures_waterfall_tc = ["absolute"]
                        
                        # Adicionar Flex Volume = Flex Mês 1 - Mês 1 sempre que o valor for diferente de zero
                        tem_flex_volume_significativo = abs(flex_volume_delta) > 1e-10
                        if tem_flex_volume_significativo:
                            labels_waterfall_tc.append("Flex Mês 1 - Mês 1")
                            values_waterfall_tc.append(flex_volume_delta)
                            measures_waterfall_tc.append("relative")
                        
                        # Adicionar categorias (já calculadas como Mês 2 - Flex Mês 1)
                        labels_waterfall_tc.extend(labels_cats)
                        values_waterfall_tc.extend(values_cats)
                        measures_waterfall_tc.extend(["relative"] * len(labels_cats))
                        
                        # Adicionar barra final
                        labels_waterfall_tc.append(f"{mes_final}")
                        values_waterfall_tc.append(valor_final_grafico)
                        measures_waterfall_tc.append("total")
                        
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
                        cor_be = "#C4B5FD"       # Roxo claro para barras BE
                        cor_historico = "#4C1D95" # Roxo escuro para barras Histórico

                        # Detectar se os períodos contêm dados de BE
                        _m1_is_be = False
                        _m2_is_be = False
                        if 'Fonte' in df_analise.columns:
                            if modo_comparacao == "Mês a Mês" and col_mes_waterfall_tc:
                                _m1_fontes = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_inicial)]['Fonte'].unique()
                                _m2_fontes = df_analise[df_analise[col_mes_waterfall_tc].astype(str) == str(mes_final)]['Fonte'].unique()
                                _m1_is_be = 'BE' in _m1_fontes
                                _m2_is_be = 'BE' in _m2_fontes
                        
                        # Criar anotações
                        annotations_custom = []
                        cumulative = 0
                        
                        for measure, value, label in zip(measures_waterfall_tc, values_waterfall_tc, labels_waterfall_tc):
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
                                    showarrow=False, font=dict(color=cor_azul, size=11), yshift=15,
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
                                    showarrow=False, font=dict(color=cor_texto, size=11), yshift=yshift_val,
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
                                    showarrow=False, font=dict(color=cor_azul, size=11), yshift=20,
                                    xref="x", yref="y", yanchor="bottom"
                                ))
                        
                        # Preparar informações detalhadas para o tooltip
                        # ── Pré-calcular TOP gastos por categoria para tooltip enriquecido ──
                        _sufixo_adv = ''
                        if fator_conversao:
                            if fator_conversao == 'K (milhares)':
                                _sufixo_adv = ' K'
                            elif fator_conversao == 'M (Milhões)':
                                _sufixo_adv = ' M'
                        _cat_labels_adv = [l for l, m in zip(labels_waterfall_tc, measures_waterfall_tc) if m == 'relative' and l != 'Flex Mês 1 - Mês 1' and l != 'Outros']
                        _df_sap_adv = _load_tc_veiculos_sapiens(ano_selecionado)
                        _df_sap_adv = _apply_session_filters_to_sapiens_tc(_df_sap_adv)
                        if col_mes_waterfall_tc and not _df_sap_adv.empty:
                            _periodos_adv = set()
                            if 'Período' in df_m1.columns:
                                _periodos_adv.update(df_m1['Período'].astype(str).unique())
                            if 'Período' in df_m2.columns:
                                _periodos_adv.update(df_m2['Período'].astype(str).unique())
                            if _periodos_adv and 'Período' in _df_sap_adv.columns:
                                _df_sap_adv = _df_sap_adv[_df_sap_adv['Período'].astype(str).isin(_periodos_adv)].copy()
                        _vc_adv = None
                        for _c in ['Despesa Primaria', 'Custo FP', 'Total']:
                            if _c in _df_sap_adv.columns:
                                _vc_adv = _c
                                break
                        if _vc_adv and tipo_visualizacao == 'Custo Total':
                            if fator_conversao == 'K (milhares)':
                                _df_sap_adv[_vc_adv] = pd.to_numeric(_df_sap_adv[_vc_adv], errors='coerce') / 1000
                            elif fator_conversao == 'M (Milhões)':
                                _df_sap_adv[_vc_adv] = pd.to_numeric(_df_sap_adv[_vc_adv], errors='coerce') / 1000000
                            if moeda_codigo != 'BRL':
                                _df_sap_adv = converter_coluna_moeda(_df_sap_adv, _vc_adv, moeda_codigo, taxas_cambio)
                        _top_adv = _build_top_gastos_por_categoria(
                            _df_sap_adv, _cat_labels_adv, chosen_dim_waterfall_tc, _vc_adv, moeda_simbolo, _sufixo_adv,
                        )

                        hovertexts = []
                        for i, (label, value, measure) in enumerate(zip(labels_waterfall_tc, values_waterfall_tc, measures_waterfall_tc)):
                            # Calcular valor acumulado até este ponto
                            if measure == "absolute":
                                acumulado = value
                                tipo_medida = "Valor Inicial"
                            elif measure == "total":
                                acumulado = value
                                tipo_medida = "Valor Final"
                            else:
                                # Calcular acumulado para medidas relativas
                                acumulado = valor_inicial_grafico
                                for j in range(1, i):
                                    if measures_waterfall_tc[j] == "relative":
                                        acumulado += values_waterfall_tc[j]
                                acumulado += value
                                tipo_medida = "Variação"
                            
                            # Formatar valor
                            if tipo_visualizacao == "CPU (Custo por Unidade)":
                                valor_formatado = f"{value:,.2f}"
                                acumulado_formatado = f"{acumulado:,.2f}"
                            else:
                                sufixo = ""
                                if fator_conversao:
                                    if fator_conversao == "K (milhares)":
                                        sufixo = " K"
                                    elif fator_conversao == "M (Milhões)":
                                        sufixo = " M"
                                valor_formatado = f"{value:,.2f}{sufixo}"
                                acumulado_formatado = f"{acumulado:,.2f}{sufixo}"
                            
                            # Criar texto do tooltip detalhado
                            hover_text = (
                                f"<b>{label}</b><br>"
                                f"Tipo: {tipo_medida}<br>"
                                f"Valor: {moeda_simbolo} {valor_formatado}<br>"
                                f"Acumulado: {moeda_simbolo} {acumulado_formatado}<br>"
                                f"Período: {mes_inicial} → {mes_final}<br>"
                                f"Modo: {modo_comparacao}"
                            )
                            if measure == 'relative' and label != 'Flex Mês 1 - Mês 1' and label != 'Outros':
                                _top_entry_adv = _top_adv.get(str(label).strip(), {})
                                _top_str_adv = _top_entry_adv.get('hover_str', '')
                                if _top_str_adv:
                                    hover_text += (
                                        '<br>──────────<br>'
                                        '<b>TOP 10 gastos:</b><br>'
                                        + _top_str_adv
                                    )
                            hovertexts.append(hover_text)
                        
                        # Criar figura do waterfall
                        fig = go.Figure(go.Waterfall(
                            name="",  # Remover nome para evitar "undefined"
                            orientation="v",
                            measure=measures_waterfall_tc,
                            x=labels_waterfall_tc,
                            y=values_waterfall_tc,
                            textposition="none",
                            hovertext=hovertexts,
                            hovertemplate="%{hovertext}<extra></extra>",
                            connector={"line": {"color": "rgba(0, 0, 0, 0)"}},
                            increasing={"marker": {"color": cor_vermelha, "line": {"width": 0}}},
                            decreasing={"marker": {"color": cor_verde, "line": {"width": 0}}},
                            totals={"marker": {"color": cor_azul, "line": {"width": 0}}}
                        ))
                        # Forçar largura do waterfall para alinhar com overlays go.Bar
                        fig.update_traces(width=0.8, selector=dict(type="waterfall"))
                        
                        # Adicionar overlay para "Flex Mês 1 - Mês 1" (amarelo)
                        if "Flex Mês 1 - Mês 1" in labels_waterfall_tc:
                            idx_flex = labels_waterfall_tc.index("Flex Mês 1 - Mês 1")
                            valor_flex = values_waterfall_tc[idx_flex]
                            
                            # Calcular a posição base exata (mesma lógica do Plotly Waterfall)
                            # Usar valor_inicial_grafico (bud no modo CPU, total_m1_all no modo Custo Total)
                            cumulative_flex = valor_inicial_grafico
                            for i in range(1, idx_flex):
                                cumulative_flex += values_waterfall_tc[i]
                            
                            # Para barras positivas: base = cumulative
                            # Para barras negativas: base = cumulative + valor (porque vai para baixo)
                            if valor_flex >= 0:
                                base_flex = cumulative_flex
                            else:
                                base_flex = cumulative_flex + valor_flex
                            
                            # Overlay com borda na mesma cor para cobrir completamente a barra do waterfall
                            fig.add_trace(go.Bar(
                                x=['Flex Mês 1 - Mês 1'],
                                y=[abs(valor_flex)],
                                base=[base_flex],
                                marker_color=cor_amarela,
                                marker_line=dict(width=2, color=cor_amarela),
                                opacity=1.0,
                                showlegend=False,
                                textposition='none',
                                width=0.82,
                            ))
                        
                        # Adicionar overlay para "Outros" (laranja)
                        if "Outros" in labels_waterfall_tc:
                            idx_outros = labels_waterfall_tc.index("Outros")
                            valor_outros = values_waterfall_tc[idx_outros]
                            
                            # Calcular a posição base exata (mesma lógica do Plotly Waterfall)
                            # Usar valor_inicial_grafico (bud no modo CPU, total_m1_all no modo Custo Total)
                            cumulative_outros = valor_inicial_grafico
                            for i in range(1, idx_outros):
                                cumulative_outros += values_waterfall_tc[i]
                            
                            # Para barras positivas: base = cumulative
                            # Para barras negativas: base = cumulative + valor (porque vai para baixo)
                            if valor_outros >= 0:
                                base_outros = cumulative_outros
                            else:
                                base_outros = cumulative_outros + valor_outros
                            
                            # Overlay com borda na mesma cor para cobrir completamente a barra do waterfall
                            fig.add_trace(go.Bar(
                                x=['Outros'],
                                y=[abs(valor_outros)],
                                base=[base_outros],
                                marker_color=cor_laranja,
                                marker_line=dict(width=2, color=cor_laranja),
                                opacity=1.0,
                                showlegend=False,
                                textposition='none',
                                width=0.82,
                            ))
                        

                        
                        # Calcular range do eixo Y
                        if values_waterfall_tc:
                            cumulative = 0
                            all_y_positions = []
                            
                            for measure, value in zip(measures_waterfall_tc, values_waterfall_tc):
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
                        
                        # Atualizar layout (sem título no gráfico)
                        fig.update_layout(
                            barmode='overlay',
                            title="",  # Remover título do gráfico completamente
                            xaxis_title="Categoria / Período",
                            yaxis_title=f"{tipo_visualizacao} ({moeda_simbolo})",
                            height=560,
                            showlegend=False,
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=80, r=40, t=50, b=40),
                            font=dict(color=text_color, size=10),
                            hovermode='closest',
                            hoverlabel=dict(
                                bgcolor="rgba(255, 255, 255, 0.95)",
                                bordercolor="#1e6ba8",
                                font=dict(
                                    size=12,
                                    family="Arial",
                                    color="#000000"
                                ),
                                align="left"
                            ),
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
                                tickfont=dict(size=12)
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
                                tickfont=dict(size=12)
                            ),
                            annotations=annotations_custom if annotations_custom else []
                        )
                        
                        # Anotação removida - não exibir texto "Flex Mês 1 - Mês 1" no gráfico
                        
                        # Exibir título acima do gráfico
                        st.markdown("### 🌊 Waterfall Analysis — TC Veículos")
                        
                        # Exibir gráfico
                        plotly_chart_safe(fig, use_container_width=True)
                        
                        # ═══ Criar dados para o painel por oficina (usando dados SEM filtro de oficina) ═══
                        # O painel inferior sempre mostra TODAS as oficinas, independente do filtro inline
                        if 'df_analise_painel_base' in locals() and df_analise_painel_base is not None:
                            # Filtrar por período usando a base sem filtro de oficina
                            if modo_comparacao == "Mês a Mês":
                                if col_mes_waterfall_tc:
                                    df_m1_panel = df_analise_painel_base[df_analise_painel_base[col_mes_waterfall_tc].astype(str) == str(mes_inicial)]
                                    df_m2_panel = df_analise_painel_base[df_analise_painel_base[col_mes_waterfall_tc].astype(str) == str(mes_final)]
                                else:
                                    df_m1_panel = df_analise_painel_base[df_analise_painel_base['Período'].astype(str) == str(mes_inicial)]
                                    df_m2_panel = df_analise_painel_base[df_analise_painel_base['Período'].astype(str) == str(mes_final)]
                            elif modo_comparacao == "Ano a Ano":
                                df_m1_panel = df_analise_painel_base[df_analise_painel_base['Ano'].astype(str) == str(ano_inicial)]
                                df_m2_panel = df_analise_painel_base[df_analise_painel_base['Ano'].astype(str) == str(ano_final)]
                            elif modo_comparacao == "Semestre":
                                meses_semestre_panel = {1: ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho'],
                                                        2: ['Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']}
                                df_m1_panel = df_analise_painel_base[
                                    (df_analise_painel_base['Ano'].astype(str) == str(ano_inicial)) &
                                    (df_analise_painel_base['Período'].isin(meses_semestre_panel.get(semestre_inicial, [])))
                                ]
                                df_m2_panel = df_analise_painel_base[
                                    (df_analise_painel_base['Ano'].astype(str) == str(ano_final)) &
                                    (df_analise_painel_base['Período'].isin(meses_semestre_panel.get(semestre_final, [])))
                                ]
                            elif modo_comparacao == "Quarter":
                                meses_trimestre_panel = {1: ['Janeiro', 'Fevereiro', 'Março'], 2: ['Abril', 'Maio', 'Junho'],
                                                         3: ['Julho', 'Agosto', 'Setembro'], 4: ['Outubro', 'Novembro', 'Dezembro']}
                                df_m1_panel = df_analise_painel_base[
                                    (df_analise_painel_base['Ano'].astype(str) == str(ano_inicial)) &
                                    (df_analise_painel_base['Período'].isin(meses_trimestre_panel.get(trimestre_inicial, [])))
                                ]
                                df_m2_panel = df_analise_painel_base[
                                    (df_analise_painel_base['Ano'].astype(str) == str(ano_final)) &
                                    (df_analise_painel_base['Período'].isin(meses_trimestre_panel.get(trimestre_final, [])))
                                ]
                            else:
                                df_m1_panel = df_m1
                                df_m2_panel = df_m2
                            
                            # Volumes para o painel (sem filtro de oficina)
                            if 'df_vol_filtrado_painel_base' in locals() and df_vol_filtrado_painel_base is not None:
                                if modo_comparacao == "Mês a Mês":
                                    if col_mes_waterfall_tc:
                                        df_vol_m1_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base[col_mes_waterfall_tc].astype(str) == str(mes_inicial)]
                                        df_vol_m2_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base[col_mes_waterfall_tc].astype(str) == str(mes_final)]
                                    else:
                                        df_vol_m1_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base['Período'].astype(str) == str(mes_inicial)]
                                        df_vol_m2_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base['Período'].astype(str) == str(mes_final)]
                                elif modo_comparacao == "Ano a Ano":
                                    df_vol_m1_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base['Ano'].astype(str) == str(ano_inicial)]
                                    df_vol_m2_panel = df_vol_filtrado_painel_base[df_vol_filtrado_painel_base['Ano'].astype(str) == str(ano_final)]
                                else:
                                    df_vol_m1_panel = df_vol_m1 if 'df_vol_m1' in locals() and df_vol_m1 is not None else None
                                    df_vol_m2_panel = df_vol_m2 if 'df_vol_m2' in locals() and df_vol_m2 is not None else None
                            else:
                                df_vol_m1_panel = df_vol_m1 if 'df_vol_m1' in locals() and df_vol_m1 is not None else None
                                df_vol_m2_panel = df_vol_m2 if 'df_vol_m2' in locals() and df_vol_m2 is not None else None
                        else:
                            df_m1_panel = df_m1
                            df_m2_panel = df_m2
                            df_vol_m1_panel = df_vol_m1 if 'df_vol_m1' in locals() and df_vol_m1 is not None else None
                            df_vol_m2_panel = df_vol_m2 if 'df_vol_m2' in locals() and df_vol_m2 is not None else None
                        
                        # ═══ SEMPRE calcular valores fixos por Oficina para o painel inferior ═══
                        # Usa a mesma lógica do gráfico de cima, como se Dimensão=Oficina estivesse selecionada
                        _fixed_office_labels = []
                        _fixed_office_values = []
                        try:
                            _dim_oficina = "Oficina"
                            if _dim_oficina in df_m1_panel.columns and _dim_oficina in df_m2_panel.columns:
                                # Calcular g1_ofc e g2_ofc (mesma lógica de g1/g2 do gráfico principal)
                                if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_m1_panel.columns:
                                    _vol_m1_p = df_vol_m1_panel if df_vol_m1_panel is not None else pd.DataFrame()
                                    _vol_m2_p = df_vol_m2_panel if df_vol_m2_panel is not None else pd.DataFrame()
                                    _vm1_total = float(_vol_m1_p['Volume'].sum()) if not _vol_m1_p.empty and 'Volume' in _vol_m1_p.columns else 0
                                    _vm2_total = float(_vol_m2_p['Volume'].sum()) if not _vol_m2_p.empty and 'Volume' in _vol_m2_p.columns else 0

                                    _g1_ofc_custo = df_m1_panel.groupby(_dim_oficina)['Custo FP'].sum()
                                    _g2_ofc_custo = df_m2_panel.groupby(_dim_oficina)['Custo FP'].sum()
                                    _g1_ofc = _g1_ofc_custo / _vm1_total if _vm1_total > 0 else _g1_ofc_custo
                                    _g2_ofc = _g2_ofc_custo / _vm2_total if _vm2_total > 0 else _g2_ofc_custo
                                    _g1_ofc = _g1_ofc.fillna(0)
                                    _g2_ofc = _g2_ofc.fillna(0)

                                    # Proporção de volume para flex
                                    _prop_vol_ofc = _vm2_total / _vm1_total if _vm1_total > 0 else 1.0
                                    # Volumes por oficina para cálculo de flex
                                    _vol_m1_por_ofc = _vol_m1_p.groupby(_dim_oficina)['Volume'].sum() if not _vol_m1_p.empty and _dim_oficina in _vol_m1_p.columns else pd.Series(dtype=float)
                                    _vol_m2_por_ofc = _vol_m2_p.groupby(_dim_oficina)['Volume'].sum() if not _vol_m2_p.empty and _dim_oficina in _vol_m2_p.columns else pd.Series(dtype=float)
                                else:
                                    _g1_ofc = df_m1_panel.groupby(_dim_oficina)[col_valor].sum()
                                    _g2_ofc = df_m2_panel.groupby(_dim_oficina)[col_valor].sum()
                                    _vm1_total = 0
                                    _vm2_total = 0
                                    _prop_vol_ofc = 1.0
                                    _vol_m1_por_ofc = pd.Series(dtype=float)
                                    _vol_m2_por_ofc = pd.Series(dtype=float)

                                _all_ofcs = sorted(set(_g1_ofc.index.tolist()) | set(_g2_ofc.index.tolist()))

                                # Calcular g_flex por oficina (mesma lógica de g_flex do gráfico de cima)
                                for _ofc in _all_ofcs:
                                    _v_m2 = float(_g2_ofc.get(_ofc, 0.0))

                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_m1_panel.columns:
                                        # Calcular flex em custo total, depois dividir por volume m2
                                        _df_ofc_m1 = df_m1_panel[df_m1_panel[_dim_oficina].astype(str) == str(_ofc)]
                                        _vm1_ofc = float(_vol_m1_por_ofc.get(_ofc, 0.0)) if not _vol_m1_por_ofc.empty else 0
                                        _vm2_ofc = float(_vol_m2_por_ofc.get(_ofc, 0.0)) if not _vol_m2_por_ofc.empty else 0
                                        _prop_ofc = _vm2_ofc / _vm1_ofc if _vm1_ofc > 0 else _prop_vol_ofc

                                        if 'Custo' in _df_ofc_m1.columns:
                                            _fixo_ofc = _df_ofc_m1[_df_ofc_m1['Custo'] == 'Fixo']['Custo FP'].sum()
                                            _var_ofc = _df_ofc_m1[_df_ofc_m1['Custo'] == 'Variável']['Custo FP'].sum()
                                        else:
                                            _fixo_ofc = 0
                                            _var_ofc = _df_ofc_m1['Custo FP'].sum()

                                        _flex_custo_ofc = _fixo_ofc + (_var_ofc * _prop_ofc)
                                        _flex_cpu_ofc = _flex_custo_ofc / _vm2_total if _vm2_total > 0 else 0
                                    else:
                                        # Custo Total
                                        _df_ofc_m1 = df_m1_panel[df_m1_panel[_dim_oficina].astype(str) == str(_ofc)]
                                        if 'Custo' in _df_ofc_m1.columns:
                                            _fixo_ofc = _df_ofc_m1[_df_ofc_m1['Custo'] == 'Fixo'][col_valor].sum()
                                            _var_ofc = _df_ofc_m1[_df_ofc_m1['Custo'] == 'Variável'][col_valor].sum()
                                        else:
                                            _fixo_ofc = 0
                                            _var_ofc = _df_ofc_m1[col_valor].sum()

                                        _flex_cpu_ofc = _fixo_ofc + (_var_ofc * _prop_vol_ofc)

                                    _delta_ofc = _v_m2 - _flex_cpu_ofc
                                    if abs(_delta_ofc) > 1e-9:
                                        _fixed_office_labels.append(str(_ofc))
                                        _fixed_office_values.append(float(_delta_ofc))

                                # Ordenar por valor absoluto
                                if _fixed_office_labels:
                                    _sorted = sorted(range(len(_fixed_office_values)), key=lambda i: abs(_fixed_office_values[i]), reverse=True)
                                    _fixed_office_labels = [_fixed_office_labels[i] for i in _sorted]
                                    _fixed_office_values = [_fixed_office_values[i] for i in _sorted]
                        except Exception:
                            _fixed_office_labels = []
                            _fixed_office_values = []
                        
                        _render_post_waterfall_panel_tc(
                            ano_label=ano_selecionado,
                            df_m1=df_m1_panel,
                            df_m2=df_m2_panel,
                            df_vol_m1=df_vol_m1_panel,
                            df_vol_m2=df_vol_m2_panel,
                            total_inicial=total_m1_all,
                            total_final=total_m2_all,
                            label_inicial=mes_inicial,
                            label_final=mes_final,
                            label_flex='Flex Mês 1 - Mês 1',
                            tipo_visualizacao=tipo_visualizacao,
                            moeda_simbolo=moeda_simbolo,
                            fator_conversao=fator_conversao,
                            moeda_codigo=moeda_codigo,
                            taxas_cambio=taxas_cambio,
                            value_column=col_valor,
                            key_prefix='wf_tc_real',
                            flex_delta_override=flex_volume_delta,
                            office_labels_override=_fixed_office_labels if _fixed_office_labels else None,
                            office_values_override=_fixed_office_values if _fixed_office_values else None,
                        )
                        
                        # ==========================================
                        # TABELA: Análise Flex Bud por Categoria (usando mês inicial como base)
                        # ==========================================
                        st.markdown("---")
                        st.markdown('<div id="analise-flex-bud-por-categoria-waterfall"></div>', unsafe_allow_html=True)
                        st.subheader(f"📊 Análise Flex por Categoria (Base: {mes_inicial})")
                        
                        # ═══ Construir df_m1_tabela / df_m2_tabela a partir de df_analise_tabela ═══
                        # Esses DataFrames NÃO sofrem influência de chosen_dim_waterfall_tc
                        _df_src_tab = df_analise_tabela if 'df_analise_tabela' in locals() and df_analise_tabela is not None else df_analise
                        if modo_comparacao == "Mês a Mês":
                            if col_mes_waterfall_tc:
                                df_m1_tabela = _df_src_tab[_df_src_tab[col_mes_waterfall_tc].astype(str) == str(mes_inicial)]
                                df_m2_tabela = _df_src_tab[_df_src_tab[col_mes_waterfall_tc].astype(str) == str(mes_final)]
                            else:
                                df_m1_tabela = _df_src_tab[_df_src_tab['Período'].astype(str) == str(mes_inicial)]
                                df_m2_tabela = _df_src_tab[_df_src_tab['Período'].astype(str) == str(mes_final)]
                        elif modo_comparacao == "Ano a Ano":
                            df_m1_tabela = _df_src_tab[_df_src_tab['Ano'].astype(str) == str(ano_inicial)]
                            df_m2_tabela = _df_src_tab[_df_src_tab['Ano'].astype(str) == str(ano_final)]
                        elif modo_comparacao == "Semestre":
                            _meses_sem = {1: ['Janeiro','Fevereiro','Março','Abril','Maio','Junho'], 2: ['Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']}
                            df_m1_tabela = _df_src_tab[(_df_src_tab['Ano'].astype(str)==str(ano_inicial)) & (_df_src_tab['Período'].isin(_meses_sem.get(semestre_inicial,[])))]
                            df_m2_tabela = _df_src_tab[(_df_src_tab['Ano'].astype(str)==str(ano_final)) & (_df_src_tab['Período'].isin(_meses_sem.get(semestre_final,[])))]
                        elif modo_comparacao == "Quarter":
                            _meses_tri = {1:['Janeiro','Fevereiro','Março'],2:['Abril','Maio','Junho'],3:['Julho','Agosto','Setembro'],4:['Outubro','Novembro','Dezembro']}
                            df_m1_tabela = _df_src_tab[(_df_src_tab['Ano'].astype(str)==str(ano_inicial)) & (_df_src_tab['Período'].isin(_meses_tri.get(trimestre_inicial,[])))]
                            df_m2_tabela = _df_src_tab[(_df_src_tab['Ano'].astype(str)==str(ano_final)) & (_df_src_tab['Período'].isin(_meses_tri.get(trimestre_final,[])))]
                        else:
                            df_m1_tabela = df_m1
                            df_m2_tabela = df_m2

                        # Verificar se temos os dados necessários
                        if 'df_m1_tabela' in locals() and df_m1_tabela is not None and len(df_m1_tabela) > 0 and 'df_m2_tabela' in locals() and df_m2_tabela is not None and len(df_m2_tabela) > 0:
                            # Verificar se temos coluna 'Custo' nos dados
                            tem_custo_real = False
                            if 'Custo' in df_m1_tabela.columns and 'Custo' in df_m2_tabela.columns:
                                tem_custo_real = True
                            
                            if tem_custo_real:
                                try:
                                    # Preparar dados: usar df_m1_tabela como base e df_m2_tabela como Total
                                    # Estes dados NÃO dependem da dimensão selecionada
                                    df_real_base = df_m1_tabela.copy()
                                    df_real_final = df_m2_tabela.copy()
                                    
                                    # IMPORTANTE: df_m1 e df_m2 já vêm de df_filtrado_waterfall_tc que tem a conversão de moeda aplicada
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
                                    df_base_agrupado = df_real_base.groupby(colunas_agrupamento)['Custo FP'].sum().reset_index()
                                    df_base_agrupado = df_base_agrupado.rename(columns={'Custo FP': 'Base_CustoFP'})
                                    
                                    # Agrupar dados do mês final
                                    df_final_agrupado = df_real_final.groupby(colunas_agrupamento)['Custo FP'].sum().reset_index()
                                    # TC Veículos: coluna já se chama 'Custo FP' (no-op)
                                    
                                    # Fazer merge
                                    df_tabela_flex_waterfall_tc = df_final_agrupado.merge(
                                        df_base_agrupado,
                                        on=colunas_agrupamento,
                                        how='outer'
                                    )
                                    df_tabela_flex_waterfall_tc['Base_CustoFP'] = df_tabela_flex_waterfall_tc['Base_CustoFP'].fillna(0)
                                    df_tabela_flex_waterfall_tc['Custo FP'] = df_tabela_flex_waterfall_tc['Custo FP'].fillna(0)
                                    
                                    # Obter volumes usando df_vol_filtrado que já foi criado anteriormente com TODOS os filtros aplicados
                                    # IMPORTANTE: Usar o mesmo df_vol_filtrado que foi criado na linha 768 (já tem todos os filtros)
                                    volume_m1 = 0
                                    volume_m2 = 0
                                    
                                    # Usar df_vol_filtrado que já foi criado anteriormente (linha 768) com todos os filtros aplicados
                                    if df_vol_filtrado is not None and not df_vol_filtrado.empty and 'Volume' in df_vol_filtrado.columns:
                                        # Determinar coluna de período (já foi criada no df_vol_filtrado anteriormente)
                                        col_mes_vol = col_mes_waterfall_tc if col_mes_waterfall_tc else 'Período'
                                        
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
                                    # Fixo: Total Mês 1 (não varia) = Base_CustoFP onde Custo == 'Fixo'
                                    # Variável: Total Mês 1 × (Volume Real Mês 2 / Volume Mês 1) = Base_CustoFP × proporcao onde Custo == 'Variável'
                                    proporcao_volume = volume_m2 / volume_m1 if volume_m1 != 0 else 1.0
                                    proporcao_volume = proporcao_volume if pd.notna(proporcao_volume) else 1.0
                                    
                                    # Calcular Flex Bud Fixo e Variável (mesma lógica do TC Ext)
                                    df_tabela_flex_waterfall_tc['_Proporcao_Volume'] = proporcao_volume
                                    df_tabela_flex_waterfall_tc['_Flex_Bud_Fixo'] = df_tabela_flex_waterfall_tc['Base_CustoFP'].where(df_tabela_flex_waterfall_tc['Custo'] == 'Fixo', 0)
                                    df_tabela_flex_waterfall_tc['_Flex_Bud_Variavel'] = (df_tabela_flex_waterfall_tc['Base_CustoFP'] * df_tabela_flex_waterfall_tc['_Proporcao_Volume']).where(df_tabela_flex_waterfall_tc['Custo'] == 'Variável', 0)
                                    
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        # Calcular Flex Bud em Custo Total primeiro
                                        df_tabela_flex_waterfall_tc['_Flex_Bud_CustoFP_Custo'] = df_tabela_flex_waterfall_tc['_Flex_Bud_Fixo'] + df_tabela_flex_waterfall_tc['_Flex_Bud_Variavel']
                                        
                                        # Converter para CPU: Flex BUD = Flex Bud Total Custo / Volume Real Mês 2
                                        df_tabela_flex_waterfall_tc['Flex BUD'] = df_tabela_flex_waterfall_tc['_Flex_Bud_CustoFP_Custo'] / volume_m2 if volume_m2 != 0 else 0
                                        df_tabela_flex_waterfall_tc['Flex BUD'] = df_tabela_flex_waterfall_tc['Flex BUD'].fillna(0)
                                        
                                        df_tabela_flex_waterfall_tc['BUD'] = df_tabela_flex_waterfall_tc['Base_CustoFP'] / volume_m1 if volume_m1 != 0 else 0
                                        df_tabela_flex_waterfall_tc['BUD'] = df_tabela_flex_waterfall_tc['BUD'].fillna(0)
                                        
                                        df_tabela_flex_waterfall_tc['Custo FP'] = df_tabela_flex_waterfall_tc['Custo FP'] / volume_m2 if volume_m2 != 0 else 0
                                        df_tabela_flex_waterfall_tc['Custo FP'] = df_tabela_flex_waterfall_tc['Custo FP'].fillna(0)
                                        
                                        df_tabela_flex_waterfall_tc['_Flex_Bud_CustoFP'] = df_tabela_flex_waterfall_tc['_Flex_Bud_CustoFP_Custo']
                                        df_tabela_flex_waterfall_tc['_CustoFP_Custo_CustoFP'] = df_tabela_flex_waterfall_tc['Custo FP'] * volume_m2
                                    else:
                                        # Custo Total: Flex BUD = Flex Bud Fixo + Flex Bud Variável
                                        df_tabela_flex_waterfall_tc['Flex BUD'] = df_tabela_flex_waterfall_tc['_Flex_Bud_Fixo'] + df_tabela_flex_waterfall_tc['_Flex_Bud_Variavel']
                                        df_tabela_flex_waterfall_tc['BUD'] = df_tabela_flex_waterfall_tc['Base_CustoFP']
                                        
                                        df_tabela_flex_waterfall_tc['_Flex_Bud_CustoFP'] = df_tabela_flex_waterfall_tc['Flex BUD']
                                        df_tabela_flex_waterfall_tc['_CustoFP_Custo_CustoFP'] = df_tabela_flex_waterfall_tc['Custo FP']
                                    
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
                                    colunas_id_waterfall_tc = [col for col in ['Custo', 'Type 05', 'Type 06', 'Account'] if col in df_tabela_flex_waterfall_tc.columns]
                                    
                                    # Criar DataFrame com colunas reorganizadas usando nomes fixos
                                    df_reorganizado = df_tabela_flex_waterfall_tc[colunas_id_waterfall_tc].copy()
                                    
                                    # Adicionar colunas na ordem especificada com nomes padronizados
                                    df_reorganizado['Mês 1'] = df_tabela_flex_waterfall_tc['BUD']
                                    df_reorganizado['Flex Mês 1 - Mês 1'] = df_tabela_flex_waterfall_tc['Flex BUD'] - df_tabela_flex_waterfall_tc['BUD']
                                    df_reorganizado['Flex Mês 1'] = df_tabela_flex_waterfall_tc['Flex BUD']
                                    df_reorganizado['Mês 2 - Flex Mês 1'] = df_tabela_flex_waterfall_tc['Custo FP'] - df_tabela_flex_waterfall_tc['Flex BUD']
                                    df_reorganizado['Mês 2'] = df_tabela_flex_waterfall_tc['Custo FP']
                                    df_reorganizado['% Mês 2/Flex Mês 1'] = df_reorganizado.apply(
                                        lambda row: (row['Mês 2'] / row['Flex Mês 1'] * 100) if row['Flex Mês 1'] != 0 and pd.notnull(row['Flex Mês 1']) else 0,
                                        axis=1
                                    )
                                    
                                    df_tabela_flex_waterfall_tc = df_reorganizado
                                    
                                    # Remover colunas auxiliares (incluindo as que não devem aparecer na tabela)
                                    colunas_remover = ['Base_CustoFP', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel', '_Flex_Bud_CustoFP', '_CustoFP_Custo_CustoFP', '_Proporcao_Volume']
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        colunas_remover.append('_Flex_Bud_CustoFP_Custo')
                                    df_tabela_flex_waterfall_tc = df_tabela_flex_waterfall_tc.drop(columns=[col for col in colunas_remover if col in df_tabela_flex_waterfall_tc.columns])
                                    
                                    # Selecionador de visualização
                                    modo_tabela_flex_waterfall_tc = st.radio(
                                        "📊 **Visualização:**",
                                        ["Fixo/Variável", "Total"],
                                        index=0,
                                        horizontal=True,
                                        key="modo_tabela_flex_waterfall_tc"
                                    )
                                    
                                    # Resumo geral
                                    if len(df_tabela_flex_waterfall_tc) > 0:
                                        linha_resumo_geral = {
                                            'Mês 1': df_tabela_flex_waterfall_tc['Mês 1'].sum(),
                                            'Flex Mês 1 - Mês 1': df_tabela_flex_waterfall_tc['Flex Mês 1 - Mês 1'].sum(),
                                            'Flex Mês 1': df_tabela_flex_waterfall_tc['Flex Mês 1'].sum(),
                                            'Mês 2 - Flex Mês 1': df_tabela_flex_waterfall_tc['Mês 2 - Flex Mês 1'].sum(),
                                            'Mês 2': df_tabela_flex_waterfall_tc['Mês 2'].sum(),
                                            '% Mês 2/Flex Mês 1': df_tabela_flex_waterfall_tc['% Mês 2/Flex Mês 1'].sum()
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
                                                # Usar formatar_ratio_com_barra para exibir barra de progresso
                                                # O valor está em percentual (ex: 95.5), precisa converter para decimal (0.955)
                                                valor_percentual = linha_resumo_geral[col]
                                                linha_resumo_geral_formatado[col] = formatar_ratio_com_barra(valor_percentual / 100)
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
                                        ordem_colunas_waterfall_tc = [
                                            'Mês 1',
                                            'Flex Mês 1 - Mês 1',
                                            'Flex Mês 1',
                                            'Mês 2 - Flex Mês 1',
                                            'Mês 2',
                                            '% Mês 2/Flex Mês 1'
                                        ]
                                        num_colunas = min(len(ordem_colunas_waterfall_tc), 6)
                                        if num_colunas > 0:
                                            cols = st.columns(num_colunas, gap="small")
                                            for idx, col_nome in enumerate(ordem_colunas_waterfall_tc[:num_colunas]):
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

                                    # Linha de separação e espaçamento entre Resumo Geral e controles da tabela
                                    st.markdown("---")
                                    st.markdown("<div style='height:40px;'></div>", unsafe_allow_html=True)
                                    
                                    # Criar estrutura hierárquica
                                    expand_state_key = 'waterfall_tc_real_expand_all'
                                    if expand_state_key not in st.session_state:
                                        st.session_state[expand_state_key] = False

                                    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 3])
                                    with ctrl_col1:
                                        if st.button('Expandir tudo', key='waterfall_tc_real_expandir'):
                                            st.session_state[expand_state_key] = True
                                    with ctrl_col2:
                                        if st.button('Recolher tudo', key='waterfall_tc_real_recolher'):
                                            st.session_state[expand_state_key] = False
                                    with ctrl_col3:
                                        st.caption('Controle aplicado aos expanders desta tabela Waterfall.')

                                    expandir_waterfall_real = st.session_state[expand_state_key]

                                    if modo_tabela_flex_waterfall_tc == "Fixo/Variável":
                                        for custo in ['Fixo', 'Variável']:
                                            df_custo = df_tabela_flex_waterfall_tc[df_tabela_flex_waterfall_tc['Custo'] == custo].copy()
                                            
                                            if len(df_custo) > 0:
                                                total_custo = df_custo['Mês 2'].sum() if 'Mês 2' in df_custo.columns else 0
                                                total_custo_formatado = f"{total_custo:,.2f}"
                                                
                                                # Não exibir se o total for zero
                                                if total_custo != 0 and pd.notna(total_custo):
                                                    with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=expandir_waterfall_real):
                                                        # Nível 2: Type 05
                                                        if 'Type 05' in df_custo.columns:
                                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                                
                                                                if len(df_type05) > 0:
                                                                    total_type05 = df_type05['Mês 2'].sum() if 'Mês 2' in df_type05.columns else 0
                                                                    total_type05_formatado = f"{total_type05:,.2f}"
                                                                    
                                                                    # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                                    # A filtragem de linhas zeradas já é feita dentro do loop do Type 06
                                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=expandir_waterfall_real):
                                                                        # Nível 3: Type 06
                                                                        if 'Type 06' in df_type05.columns:
                                                                                _t06_vals = sorted(df_type05['Type 06'].dropna().unique())
                                                                                if not _t06_vals:
                                                                                    st.caption("Sem dados de Type 06 para este Type 05.")
                                                                                _algum_t06_renderizado = False
                                                                                for type06 in _t06_vals:
                                                                                    try:
                                                                                        df_type06 = df_type05[df_type05['Type 06'] == type06].copy()
                                                                                        if _render_type06_block(df_type06, type06, tipo_visualizacao, fator_conversao):
                                                                                            _algum_t06_renderizado = True
                                                                                    except Exception as _e_t06:
                                                                                        st.error(f"Erro ao renderizar Type 06 '{type06}': {_e_t06}")
                                                                                        import traceback
                                                                                        st.code(traceback.format_exc())
                                                                                if not _algum_t06_renderizado:
                                                                                    st.caption("Nenhum Type 06 com valores significativos.")
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
                                                                                        
                                                                                        # Formatar percentual com barra de progresso
                                                                                        # O valor está em percentual (ex: 95.5), precisa converter para decimal (0.955)
                                                                                        valor_percentual = linha_resumo_type05['% Mês 2/Flex Mês 1']
                                                                                        linha_resumo_type05_formatado['% Mês 2/Flex Mês 1'] = formatar_ratio_com_barra(valor_percentual / 100)
                                                                                        
                                                                                        # Exibir caixas na ordem correta: Mês 1, Flex Mês 1 - Mês 1, Flex Mês 1, Mês 2 - Flex Mês 1, Mês 2, % Mês 2/Flex Mês 1
                                                                                        ordem_colunas_waterfall_tc = [
                                                                                            'Mês 1',
                                                                                            'Flex Mês 1 - Mês 1',
                                                                                            'Flex Mês 1',
                                                                                            'Mês 2 - Flex Mês 1',
                                                                                            'Mês 2',
                                                                                            '% Mês 2/Flex Mês 1'
                                                                                        ]
                                                                                        num_colunas = min(len(ordem_colunas_waterfall_tc), 6)
                                                                                        if num_colunas > 0:
                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall_tc[:num_colunas]):
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
                                        if 'Type 05' in df_tabela_flex_waterfall_tc.columns:
                                            colunas_agrupamento_total.append('Type 05')
                                        if 'Type 06' in df_tabela_flex_waterfall_tc.columns:
                                            colunas_agrupamento_total.append('Type 06')
                                        if 'Account' in df_tabela_flex_waterfall_tc.columns:
                                            colunas_agrupamento_total.append('Account')
                                        
                                        if len(colunas_agrupamento_total) > 0:
                                            df_tabela_total_agrupado = df_tabela_flex_waterfall_tc.groupby(colunas_agrupamento_total).agg({
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
                                            df_tabela_total_agrupado = df_tabela_flex_waterfall_tc.copy()
                                        
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
                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=expandir_waterfall_real):
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
                                                                                        
                                                                                        # Formatar percentual com barra de progresso
                                                                                        # O valor está em percentual (ex: 95.5), precisa converter para decimal (0.955)
                                                                                        valor_percentual = linha_resumo_type06['% Mês 2/Flex Mês 1']
                                                                                        linha_resumo_formatado_type06['% Mês 2/Flex Mês 1'] = formatar_ratio_com_barra(valor_percentual / 100)
                                                                                        
                                                                                        # Exibir resumo e tabela
                                                                                        ordem_colunas_waterfall_tc = [
                                                                                            'Mês 1',
                                                                                            'Flex Mês 1 - Mês 1',
                                                                                            'Flex Mês 1',
                                                                                            'Mês 2 - Flex Mês 1',
                                                                                            'Mês 2',
                                                                                            '% Mês 2/Flex Mês 1'
                                                                                        ]
                                                                                        num_colunas = min(len(ordem_colunas_waterfall_tc), 6)
                                                                                        if num_colunas > 0:
                                                                                            cols = st.columns(num_colunas, gap="small")
                                                                                            for idx, col_nome in enumerate(ordem_colunas_waterfall_tc[:num_colunas]):
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

                                _render_sapiens_section_waterfall(
                                    ano_selecionado,
                                    "waterfall_real_sapiens",
                                )
                            else:
                                st.info("ℹ️ A tabela requer dados com coluna 'Custo' (Fixo/Variável).")
                        else:
                            st.info("ℹ️ Selecione os períodos para comparação acima para visualizar a tabela.")
                
                except Exception as e:
                    st.error(f"❌ Erro ao gerar gráfico waterfall: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
        
        # TAB BUDGET
        if active_tab_waterfall_tc == "💰 Budget":
            st.subheader("💰 Análise Budget")

            # ── Escopo de Veículo (Todos vs modelo específico) ──
            _veic_opcoes_bud = []
            if 'Veículo' in df_filtrado_waterfall_tc.columns:
                _veic_opcoes_bud = sorted(
                    [v for v in df_filtrado_waterfall_tc['Veículo'].dropna().unique().tolist()
                     if v and str(v).strip() and v != 'Sem Veículo']
                )
            escopo_veiculo_budget = st.radio(
                "🚗 Escopo de Veículo:",
                ["Todos (TC Total)"] + _veic_opcoes_bud,
                index=0, horizontal=True,
                key="escopo_veiculo_budget_wf",
                help="Todos = visão consolidada sem quebra por modelo. Selecione um modelo para análise específica.",
            )

            # Carregar dados de budget e aplicar mesmos filtros
            try:
                # Carregar dados de budget
                df_budget = _load_tc_veiculos_budget(ano_selecionado)
                df_budget_vol = _load_tc_veiculos_budget_volume(ano_selecionado)
                    
                if df_budget is None or df_budget.empty:
                    st.warning("⚠️ Dados de budget não disponíveis.")
                elif df_budget_vol is None or df_budget_vol.empty:
                    st.warning("⚠️ Dados de volume de budget não disponíveis.")
                else:
                    # Preparar dados para análise (usar df_filtrado_waterfall_tc)
                    df_analise_budget = df_filtrado_waterfall_tc.copy() if df_filtrado_waterfall_tc is not None and len(df_filtrado_waterfall_tc) > 0 else pd.DataFrame()

                    # ═══ Bloco 3: Filtro defensivo — remover 'Sem Veículo' de parquets antigos ═══
                    if 'Veículo' in df_analise_budget.columns:
                        df_analise_budget = df_analise_budget[df_analise_budget['Veículo'] != 'Sem Veículo'].copy()

                    # Aplicar escopo de veículo selecionado
                    # ═══ Bloco 4: quando "Todos", NÃO agregar imediatamente ═══
                    _todos_mode_budget = False
                    if escopo_veiculo_budget == "Todos (TC Total)":
                        _todos_mode_budget = True
                        # NÃO chamar _agregar_sem_veiculo() aqui
                    elif 'Veículo' in df_analise_budget.columns:
                        df_analise_budget = df_analise_budget[
                            df_analise_budget['Veículo'] == escopo_veiculo_budget
                        ].copy()

                    # 🔒 Budget Waterfall: sempre considerar Mês/Ano quando houver Ano.
                    # Isso evita somar (ex.: Novembro/2025 + Novembro/2026) quando o usuário quer um mês específico de um ano.
                    col_mes_budget = col_mes_waterfall_tc
                    if not df_analise_budget.empty and 'Ano' in df_analise_budget.columns and 'Período' in df_analise_budget.columns:
                        if 'Período_Ano' not in df_analise_budget.columns:
                            df_analise_budget['Período_Ano'] = (
                                df_analise_budget['Período'].astype(str).str.strip() + ' ' +
                                df_analise_budget['Ano'].astype(str).str.strip()
                            )
                        col_mes_budget = 'Período_Ano'
                        
                    if df_analise_budget.empty:
                        st.warning("⚠️ Nenhum dado real disponível para comparação.")
                    else:
                        # Definir col_valor para a aba Budget (mesmo padrão da aba Real)
                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                            col_valor = 'CPU' if 'CPU' in df_analise_budget.columns else 'Custo FP'
                        else:
                            col_valor = 'Custo FP' if 'Custo FP' in df_analise_budget.columns else 'Despesa Primaria'
                        
                        # Modo de agregação
                        modo_agregacao_budget = st.radio(
                            "Agrupar por:",
                            options=["Mês", "Semestre", "Quarter"],
                            index=0,
                            horizontal=True,
                            key="modo_agregacao_budget"
                        )
                            
                        # Obter períodos disponíveis
                        if col_mes_budget == 'Período_Ano':
                            periodos_disponiveis_budget = sort_mes_unique_waterfall_tc(df_analise_budget['Período_Ano'].dropna().unique().tolist())
                        elif 'Período' in df_analise_budget.columns:
                            periodos_disponiveis_budget = sort_mes_unique_waterfall_tc(df_analise_budget['Período'].dropna().unique().tolist())
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
                                    key="periodos_budget_waterfall_tc"
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
                                        ]['Período_Ano' if col_mes_waterfall_tc == 'Período_Ano' else 'Período'].dropna().unique().tolist()
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
                                        ]['Período_Ano' if col_mes_waterfall_tc == 'Período_Ano' else 'Período'].dropna().unique().tolist()
                                    else:
                                        periodos_selecionados_budget = []
                                else:
                                    st.warning("⚠️ Coluna 'Ano' não encontrada para agrupamento por quarter.")
                                    periodos_selecionados_budget = []
                                
                            if not periodos_selecionados_budget:
                                st.warning("⚠️ Nenhum período selecionado.")
                                
                            # Obter dimensões de categoria disponíveis
                            dims_base_budget = [
                                "Type 05", "Type 06", "Type 07", "Oficina",
                                "Veículo", "Custo", "Account", "Texto breve"
                            ]
                            colunas_budget_disponiveis = set(df_analise_budget.columns)
                            if df_budget is not None and not df_budget.empty:
                                colunas_budget_disponiveis &= set(df_budget.columns)
                            dims_cat_budget = [
                                c for c in dims_base_budget
                                if c in colunas_budget_disponiveis
                            ]
                                
                            if not dims_cat_budget:
                                st.warning("⚠️ Nenhuma dimensão de categoria encontrada nos dados.")
                            else:
                                # Filtro de Type 05/06 + dimensão da categoria + Oficina lado a lado
                                col_t05_tc_bud, col_t06_tc_bud, col_dim_tc_bud, col_ofic_tc_bud = st.columns(4)
                                with col_t05_tc_bud:
                                    type05_options_tc_bud = ["Todos"]
                                    if 'Type 05' in df_analise_budget.columns:
                                        type05_options_tc_bud += sorted(df_analise_budget['Type 05'].dropna().astype(str).unique().tolist())
                                    type05_inline_sel_tc_bud = st.multiselect(
                                        "Type 05:",
                                        type05_options_tc_bud,
                                        default=["Todos"],
                                        key="type05_inline_waterfall_tc_budget"
                                    )
                                with col_t06_tc_bud:
                                    type06_options_tc_bud = ["Todos"]
                                    if 'Type 06' in df_analise_budget.columns:
                                        type06_options_tc_bud += sorted(df_analise_budget['Type 06'].dropna().astype(str).unique().tolist())
                                    type06_inline_sel_tc_bud = st.multiselect(
                                        "Type 06:",
                                        type06_options_tc_bud,
                                        default=["Todos"],
                                        key="type06_inline_waterfall_tc_budget"
                                    )
                                with col_dim_tc_bud:
                                    chosen_dim_budget = st.selectbox(
                                        "Dimensão da categoria:",
                                        dims_cat_budget,
                                        index=min(1, len(dims_cat_budget)-1) if len(dims_cat_budget) > 1 else 0,
                                        key="dim_waterfall_tc_budget"
                                    )
                                with col_ofic_tc_bud:
                                    # Filtro de oficina inline
                                    oficinas_inline_tc_bud = ["Todos"]
                                    if 'Oficina' in df_analise_budget.columns:
                                        oficinas_inline_tc_bud += sorted(df_analise_budget['Oficina'].dropna().astype(str).unique().tolist())
                                    oficina_inline_sel_tc_bud = st.multiselect(
                                        "Oficina:",
                                        oficinas_inline_tc_bud,
                                        default=["Todos"],
                                        key="oficina_inline_waterfall_tc_budget"
                                    )
                                    
                                # ═══ IMPORTANTE: Salvar cópia ANTES do filtro de oficina para o painel inferior ═══
                                df_analise_budget_painel_base = df_analise_budget.copy()
                                
                                # Aplicar filtro de oficina inline (apenas para o gráfico principal)
                                if 'Oficina' in df_analise_budget.columns and oficina_inline_sel_tc_bud and "Todos" not in oficina_inline_sel_tc_bud:
                                    df_analise_budget = df_analise_budget[df_analise_budget['Oficina'].astype(str).isin(oficina_inline_sel_tc_bud)].copy()
                                    
                                # Aplicar filtros inline de Type 05 e Type 06 (para gráfico E tabela)
                                if 'Type 05' in df_analise_budget.columns and type05_inline_sel_tc_bud and "Todos" not in type05_inline_sel_tc_bud:
                                    df_analise_budget = df_analise_budget[df_analise_budget['Type 05'].astype(str).isin(type05_inline_sel_tc_bud)].copy()
                                if 'Type 06' in df_analise_budget.columns and type06_inline_sel_tc_bud and "Todos" not in type06_inline_sel_tc_bud:
                                    df_analise_budget = df_analise_budget[df_analise_budget['Type 06'].astype(str).isin(type06_inline_sel_tc_bud)].copy()
                                    
                                # ═══ Bloco 4: Lazy aggregation Budget ═══
                                if _todos_mode_budget and chosen_dim_budget != "Veículo":
                                    df_analise_budget = _agregar_sem_veiculo(df_analise_budget)
                                    
                                # Filtrar dados pelos períodos selecionados
                                if col_mes_budget == 'Período_Ano':
                                    df_temp_budget = df_analise_budget[df_analise_budget['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                elif 'Período' in df_analise_budget.columns:
                                    df_temp_budget = df_analise_budget[df_analise_budget['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                else:
                                    df_temp_budget = df_analise_budget.copy()
                                    
                                # Obter todas as categorias disponíveis
                                cats_all_budget = sorted([str(x).strip() for x in df_analise_budget[chosen_dim_budget].dropna().unique().tolist() if str(x).strip() != ""])
                                total_cats_budget = max(1, len(cats_all_budget))
                                    
                                # Calcular categorias ordenadas por impacto absoluto ANTES do slider
                                # Isso permite ordenar corretamente por valor absoluto (maiores impactos, + ou -)
                                cats_ordenadas_por_impacto_budget = []
                                if not df_temp_budget.empty:
                                    if tipo_visualizacao == "CPU (Custo por Unidade)" and 'Custo FP' in df_temp_budget.columns:
                                        if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                            # Filtrar volume pelos períodos selecionados
                                            if col_mes_waterfall_tc == 'Período_Ano':
                                                if 'Período_Ano' not in df_volume.columns:
                                                    if 'Período' in df_volume.columns and 'Ano' in df_volume.columns:
                                                        df_volume_temp = df_volume.copy()
                                                        df_volume_temp['Período_Ano'] = df_volume_temp['Período'].astype(str) + ' ' + df_volume_temp['Ano'].astype(str)
                                                    else:
                                                        df_volume_temp = df_volume.copy()
                                                else:
                                                    df_volume_temp = df_volume.copy()
                                                if 'Período_Ano' in df_volume_temp.columns:
                                                    df_vol_temp = df_volume_temp[df_volume_temp['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                else:
                                                    df_vol_temp = df_volume_temp.copy()
                                            elif 'Período' in df_volume.columns:
                                                df_vol_temp = df_volume[df_volume['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                            else:
                                                df_vol_temp = df_volume.copy()
                                                
                                            # Calcular CPU e ordenar por valor absoluto
                                            if not df_vol_temp.empty and chosen_dim_budget in df_vol_temp.columns:
                                                vol_mf_budget = (df_temp_budget.groupby(chosen_dim_budget).agg({'Custo FP': 'sum'}).reset_index())
                                                vol_mf_budget = vol_mf_budget.merge(
                                                    df_vol_temp.groupby(chosen_dim_budget)['Volume'].sum().reset_index(),
                                                    on=chosen_dim_budget,
                                                    how='left'
                                                )
                                                vol_mf_budget['Volume'] = vol_mf_budget['Volume'].fillna(0)
                                                vol_mf_budget['CPU'] = vol_mf_budget['Custo FP'] / vol_mf_budget['Volume'].replace(0, 1)
                                                vol_mf_budget['abs_CPU'] = vol_mf_budget['CPU'].abs()
                                                vol_mf_budget = vol_mf_budget.sort_values('abs_CPU', ascending=False)
                                                cats_ordenadas_por_impacto_budget = [str(c).strip() for c in list(vol_mf_budget[chosen_dim_budget])]
                                            else:
                                                # Se não tem volume filtrado, ordenar por Total absoluto
                                                vol_mf_budget = df_temp_budget.groupby(chosen_dim_budget)['Custo FP'].sum().reset_index()
                                                vol_mf_budget['abs_Total'] = vol_mf_budget['Custo FP'].abs()
                                                vol_mf_budget = vol_mf_budget.sort_values('abs_Total', ascending=False)
                                                cats_ordenadas_por_impacto_budget = [str(c).strip() for c in list(vol_mf_budget[chosen_dim_budget])]
                                        else:
                                            # Se não tem volume, ordenar por Total absoluto
                                            vol_mf_budget = df_temp_budget.groupby(chosen_dim_budget)['Custo FP'].sum().reset_index()
                                            vol_mf_budget['abs_Total'] = vol_mf_budget['Custo FP'].abs()
                                            vol_mf_budget = vol_mf_budget.sort_values('abs_Total', ascending=False)
                                            cats_ordenadas_por_impacto_budget = [str(c).strip() for c in list(vol_mf_budget[chosen_dim_budget])]
                                    else:
                                        # Para Custo Total, ordenar por valor absoluto
                                        if col_valor in df_temp_budget.columns:
                                            vol_mf_budget = df_temp_budget.groupby(chosen_dim_budget)[col_valor].sum().reset_index()
                                            vol_mf_budget['abs_valor'] = vol_mf_budget[col_valor].abs()
                                            vol_mf_budget = vol_mf_budget.sort_values('abs_valor', ascending=False)
                                            cats_ordenadas_por_impacto_budget = [str(c).strip() for c in list(vol_mf_budget[chosen_dim_budget])]
                                    
                                # Se não conseguiu ordenar, usar lista original
                                if not cats_ordenadas_por_impacto_budget:
                                    cats_ordenadas_por_impacto_budget = cats_all_budget
                                    
                                # ═══ Detecção de troca de dimensão (Bug 5d fix) ═══
                                _prev_dim_key_budget = "_prev_dim_budget_waterfall_tc"
                                _dim_changed_budget = (
                                    _prev_dim_key_budget in st.session_state
                                    and st.session_state[_prev_dim_key_budget] != chosen_dim_budget
                                )
                                st.session_state[_prev_dim_key_budget] = chosen_dim_budget
                                    
                                # Se a dimensão mudou, limpar multiselect para evitar categorias fantasma
                                _multiselect_key_budget_fixed = "cats_budget_waterfall_tc_multiselect"
                                if _dim_changed_budget:
                                    for _k in list(st.session_state.keys()):
                                        if _k.startswith("cats_budget_waterfall_tc"):
                                            del st.session_state[_k]
                                    
                                # Controle: Quantidade de categorias a exibir (Top N)
                                if total_cats_budget <= 1:
                                    max_cats_budget = total_cats_budget
                                    st.info(f"ℹ️ Apenas {total_cats_budget} categoria disponível para esta dimensão.")
                                else:
                                    # Padrão _desired: ler valor desejado ANTES de renderizar o slider (Bug 5a fix)
                                    _slider_desired_key_bud = "max_cats_budget_waterfall_tc_desired"
                                    if _slider_desired_key_bud in st.session_state:
                                        default_value_budget = st.session_state[_slider_desired_key_bud]
                                        del st.session_state[_slider_desired_key_bud]
                                        default_value_budget = max(1, min(default_value_budget, total_cats_budget))
                                    elif _dim_changed_budget:
                                        default_value_budget = min(total_cats_budget, 20)
                                    else:
                                        default_value_budget = min(total_cats_budget, 20)
                                        
                                    # Sanitizar session_state do slider para o novo range (evita exceção quando dimensão muda)
                                    _slider_key_bud = "max_cats_budget_waterfall_tc"
                                    if _slider_key_bud in st.session_state:
                                        st.session_state[_slider_key_bud] = max(1, min(int(st.session_state[_slider_key_bud]), total_cats_budget))
                                    max_cats_budget = st.slider(
                                        f"Quantidade de categorias a exibir (Top N) (Total: {total_cats_budget}):",
                                        min_value=1,
                                        max_value=total_cats_budget,
                                        value=default_value_budget,
                                        key=_slider_key_bud
                                    )
                                    
                                # Selecionar top N categorias
                                if max_cats_budget >= total_cats_budget:
                                    top_cats_selecionadas_budget = cats_all_budget
                                else:
                                    top_cats_selecionadas_budget = cats_ordenadas_por_impacto_budget[:max_cats_budget]
                                    
                                cats_options_budget = ["Todos"] + cats_all_budget
                                top_cats_selecionadas_budget = [c for c in top_cats_selecionadas_budget if c in cats_all_budget]
                                    
                                # ═══ Multiselect com KEY FIXA (Bug 5b fix — padrão da aba Real) ═══
                                _slider_prev_key_bud = "max_cats_budget_waterfall_tc_prev"
                                if _slider_prev_key_bud not in st.session_state:
                                    st.session_state[_slider_prev_key_bud] = max_cats_budget
                                    
                                slider_mudou_budget = st.session_state[_slider_prev_key_bud] != max_cats_budget
                                st.session_state[_slider_prev_key_bud] = max_cats_budget
                                    
                                if slider_mudou_budget or _dim_changed_budget:
                                    cats_selecionadas_atual_budget = top_cats_selecionadas_budget
                                    st.session_state[_multiselect_key_budget_fixed] = cats_selecionadas_atual_budget
                                    st.session_state["cats_budget_waterfall_tc_saved_current"] = cats_selecionadas_atual_budget
                                elif _multiselect_key_budget_fixed in st.session_state:
                                    cats_selecionadas_atual_budget = st.session_state[_multiselect_key_budget_fixed]
                                    cats_selecionadas_atual_budget = [c for c in cats_selecionadas_atual_budget if c in cats_all_budget]
                                else:
                                    cats_selecionadas_atual_budget = top_cats_selecionadas_budget
                                    st.session_state[_multiselect_key_budget_fixed] = cats_selecionadas_atual_budget
                                    st.session_state["cats_budget_waterfall_tc_saved_current"] = cats_selecionadas_atual_budget
                                    
                                # Garantir validade
                                cats_selecionadas_atual_budget = [c for c in cats_selecionadas_atual_budget if c in cats_all_budget]
                                if not cats_selecionadas_atual_budget:
                                    cats_selecionadas_atual_budget = top_cats_selecionadas_budget[:min(max_cats_budget, len(top_cats_selecionadas_budget))]
                                    
                                # Sanitizar session_state da chave fixa
                                if _multiselect_key_budget_fixed in st.session_state:
                                    _bud_limpos = [v for v in st.session_state[_multiselect_key_budget_fixed] if v in cats_options_budget]
                                    if not _bud_limpos:
                                        _bud_limpos = cats_selecionadas_atual_budget
                                    st.session_state[_multiselect_key_budget_fixed] = _bud_limpos
                                    cats_selecionadas_atual_budget = _bud_limpos
                                    
                                # Criar multiselect com key fixa
                                cats_sel_raw_budget = st.multiselect(
                                    "Categorias (uma ou mais):",
                                    cats_options_budget,
                                    default=cats_selecionadas_atual_budget,
                                    key=_multiselect_key_budget_fixed
                                )
                                    
                                # Processar seleção — NÃO atualizar o slider (Bug 5a fix)
                                if cats_sel_raw_budget and len(cats_sel_raw_budget) > 0:
                                    if "Todos" in cats_sel_raw_budget:
                                        cats_sel_budget = cats_all_budget
                                    else:
                                        cats_sel_budget = cats_sel_raw_budget
                                    st.session_state["cats_budget_waterfall_tc_saved_current"] = cats_sel_budget
                                else:
                                    cats_sel_budget = top_cats_selecionadas_budget
                                    st.session_state["cats_budget_waterfall_tc_saved_current"] = cats_sel_budget
                                    
                                # Filtrar dados pelos períodos selecionados
                                if col_mes_budget == 'Período_Ano' and 'Período_Ano' in df_analise_budget.columns:
                                    df_real_periodo = df_analise_budget[df_analise_budget['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                elif 'Período' in df_analise_budget.columns:
                                    df_real_periodo = df_analise_budget[df_analise_budget['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                else:
                                    df_real_periodo = df_analise_budget.copy()
                                    
                                # Aplicar fator de conversão e conversão de moeda ao budget (mesma lógica do app.py)
                                df_budget_work = df_budget.copy()
                                    
                                # Aplicar fator de conversão
                                if fator_conversao and fator_conversao != "Nenhum" and tipo_visualizacao == "Custo Total" and 'Custo FP' in df_budget_work.columns:
                                    if fator_conversao == "K (milhares)":
                                        df_budget_work['Custo FP'] = df_budget_work['Custo FP'] / 1000
                                    elif fator_conversao == "M (Milhões)":
                                        df_budget_work['Custo FP'] = df_budget_work['Custo FP'] / 1000000
                                    
                                # Aplicar conversão de moeda
                                if moeda_codigo != "BRL" and 'Custo FP' in df_budget_work.columns:
                                    df_budget_work = converter_coluna_moeda(df_budget_work, 'Custo FP', moeda_codigo, taxas_cambio)
                                    
                                # ═══ Aplicar escopo de veículo (radio Budget tab) ═══
                                if escopo_veiculo_budget != "Todos (TC Total)":
                                    if 'Veículo' in df_budget_work.columns:
                                        df_budget_work = df_budget_work[df_budget_work['Veículo'] == escopo_veiculo_budget].copy()
                                    if df_budget_vol is not None and 'Veículo' in df_budget_vol.columns:
                                        df_budget_vol = df_budget_vol[df_budget_vol['Veículo'] == escopo_veiculo_budget].copy()

                                # Aplicar TODOS os filtros que existem em df_filtrado_waterfall_tc
                                df_budget_filtrado = df_budget_work.copy()
                                df_budget_vol_filtrado = df_budget_vol.copy()

                                # Garantir chave Mês/Ano nos dados de Budget e Volume Budget quando possível
                                if col_mes_budget == 'Período_Ano':
                                    for _df_name, _df in [('df_budget_filtrado', df_budget_filtrado), ('df_budget_vol_filtrado', df_budget_vol_filtrado)]:
                                        if _df is not None and len(_df) > 0 and 'Período_Ano' not in _df.columns and 'Período' in _df.columns and 'Ano' in _df.columns:
                                            _df['Período_Ano'] = _df['Período'].astype(str).str.strip() + ' ' + _df['Ano'].astype(str).str.strip()

                                # ✅ Aplicar filtros do usuário (sidebar) ao Budget/Volume Budget sem intersectar com o Real.
                                # Se o usuário está em "Todos", NÃO restringir pelo que existe no Real (isso causava 1477 vs 1515).
                                def _aplicar_filtro_ms(_df, _col, _state_key):
                                    if _df is None or len(_df) == 0 or _col not in _df.columns:
                                        return _df
                                    sel = st.session_state.get(_state_key, ["Todos"])
                                    if not sel or "Todos" in sel:
                                        return _df
                                    sel_str = [str(x) for x in sel]
                                    return _df[_df[_col].astype(str).isin(sel_str)].copy()

                                def _aplicar_filtro_sb(_df, _col, _state_key):
                                    if _df is None or len(_df) == 0 or _col not in _df.columns:
                                        return _df
                                    sel = st.session_state.get(_state_key, "Todos")
                                    if sel is None or str(sel) == "Todos":
                                        return _df
                                    return _df[_df[_col].astype(str) == str(sel)].copy()

                                def _aplicar_filtro_lista(_df, _col, _state_key):
                                    if _df is None or len(_df) == 0 or _col not in _df.columns:
                                        return _df
                                    sel = st.session_state.get(_state_key, [])
                                    if not sel:
                                        return _df
                                    sel_str = [str(x) for x in sel]
                                    return _df[_df[_col].astype(str).isin(sel_str)].copy()

                                for _df_ref, _is_vol in [("df_budget_filtrado", False), ("df_budget_vol_filtrado", True)]:
                                    _df = df_budget_vol_filtrado if _is_vol else df_budget_filtrado

                                    _df = _aplicar_filtro_ms(_df, 'Oficina', 'filtro_oficina_waterfall_tc')
                                    _df = _aplicar_filtro_ms(_df, 'Veículo', 'filtro_veiculo_waterfall_tc')
                                    _df = _aplicar_filtro_ms(_df, 'USI', 'filtro_usi_waterfall_tc')
                                    _df = _aplicar_filtro_sb(_df, 'Centrocst', 'filtro_centro_cst_waterfall_tc')
                                    _df = _aplicar_filtro_lista(_df, 'Nºconta', 'filtro_conta_contabil_waterfall_tc')

                                    # Filtros principais (multiselect com "Todos")
                                    for _col in ['Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                                        _df = _aplicar_filtro_ms(_df, _col, f'filtro_{_col}_waterfall_tc')

                                    # Filtros avançados
                                    for _col in ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve']:
                                        _df = _aplicar_filtro_ms(_df, _col, f'filtro_avancado_{_col}_waterfall_tc')

                                    if _is_vol:
                                        df_budget_vol_filtrado = _df
                                    else:
                                        df_budget_filtrado = _df
                                    
                                # ═══ IMPORTANTE: Salvar cópia ANTES do filtro de oficina para o painel inferior ═══
                                df_budget_filtrado_painel_base = df_budget_filtrado.copy()
                                df_budget_vol_filtrado_painel_base = df_budget_vol_filtrado.copy() if df_budget_vol_filtrado is not None else None
                                
                                # Aplicar filtro inline de Oficina ao Budget (apenas para o gráfico principal)
                                if oficina_inline_sel_tc_bud and "Todos" not in oficina_inline_sel_tc_bud:
                                    if 'Oficina' in df_budget_filtrado.columns:
                                        df_budget_filtrado = df_budget_filtrado[df_budget_filtrado['Oficina'].astype(str).isin(oficina_inline_sel_tc_bud)].copy()
                                    if df_budget_vol_filtrado is not None and 'Oficina' in df_budget_vol_filtrado.columns:
                                        df_budget_vol_filtrado = df_budget_vol_filtrado[df_budget_vol_filtrado['Oficina'].astype(str).isin(oficina_inline_sel_tc_bud)].copy()
                                    
                                # Filtrar budget pelos períodos selecionados
                                # IMPORTANTE: Sempre filtrar pelos períodos selecionados para garantir que o BUD seja calculado corretamente
                                if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                    import re

                                    def _periodos_tem_ano(periodos):
                                        return any(re.search(r"\b20\d{2}\b", str(p)) for p in periodos)

                                    periodos_str = [str(p) for p in periodos_selecionados_budget]
                                    sel_tem_ano = _periodos_tem_ano(periodos_selecionados_budget)

                                    if col_mes_budget == 'Período_Ano' and 'Período_Ano' in df_budget_filtrado.columns:
                                        df_budget_periodo = df_budget_filtrado[df_budget_filtrado['Período_Ano'].astype(str).isin(periodos_str)].copy()
                                    elif 'Período' in df_budget_filtrado.columns:
                                        # Preferir match por "Mês Ano" quando existir a seleção com ano e a coluna Ano estiver presente.
                                        if sel_tem_ano and 'Ano' in df_budget_filtrado.columns:
                                            df_tmp = df_budget_filtrado.copy()
                                            df_tmp['_Período_Ano_tmp'] = (
                                                df_tmp['Período'].astype(str).str.strip() + " " + df_tmp['Ano'].astype(str).str.strip()
                                            )
                                            df_budget_periodo = df_tmp[df_tmp['_Período_Ano_tmp'].astype(str).isin(periodos_str)].drop(columns=['_Período_Ano_tmp']).copy()
                                        else:
                                            # Match direto por Período (ex.: "Novembro")
                                            df_budget_periodo = df_budget_filtrado[df_budget_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                    else:
                                        df_budget_periodo = pd.DataFrame()  # DataFrame vazio se não há coluna Período
                                else:
                                    df_budget_periodo = pd.DataFrame()  # DataFrame vazio se não há períodos selecionados
                                    
                                # Preparar volume real filtrado pelos períodos selecionados
                                df_volume_real_filtrado = None
                                if df_volume is not None and not df_volume.empty and 'Volume' in df_volume.columns:
                                    df_volume_real_filtrado = df_volume.copy()
                                        
                                    # Aplicar filtros do usuário (sidebar) ao volume real (sem intersectar com o custo).
                                    # Isso garante que Volume siga o mesmo recorte de filtros, não o "recorte do custo".
                                    def _aplicar_filtro_ms_vol(_df, _col, _state_key):
                                        if _df is None or len(_df) == 0 or _col not in _df.columns:
                                            return _df
                                        sel = st.session_state.get(_state_key, ["Todos"])
                                        if not sel or "Todos" in sel:
                                            return _df
                                        sel_str = [str(x) for x in sel]
                                        return _df[_df[_col].astype(str).isin(sel_str)].copy()

                                    def _aplicar_filtro_sb_vol(_df, _col, _state_key):
                                        if _df is None or len(_df) == 0 or _col not in _df.columns:
                                            return _df
                                        sel = st.session_state.get(_state_key, "Todos")
                                        if sel is None or str(sel) == "Todos":
                                            return _df
                                        return _df[_df[_col].astype(str) == str(sel)].copy()

                                    def _aplicar_filtro_lista_vol(_df, _col, _state_key):
                                        if _df is None or len(_df) == 0 or _col not in _df.columns:
                                            return _df
                                        sel = st.session_state.get(_state_key, [])
                                        if not sel:
                                            return _df
                                        sel_str = [str(x) for x in sel]
                                        return _df[_df[_col].astype(str).isin(sel_str)].copy()

                                    df_volume_real_filtrado = _aplicar_filtro_ms_vol(df_volume_real_filtrado, 'Oficina', 'filtro_oficina_waterfall_tc')
                                    df_volume_real_filtrado = _aplicar_filtro_ms_vol(df_volume_real_filtrado, 'Veículo', 'filtro_veiculo_waterfall_tc')
                                    df_volume_real_filtrado = _aplicar_filtro_ms_vol(df_volume_real_filtrado, 'USI', 'filtro_usi_waterfall_tc')
                                    df_volume_real_filtrado = _aplicar_filtro_sb_vol(df_volume_real_filtrado, 'Centrocst', 'filtro_centro_cst_waterfall_tc')
                                    df_volume_real_filtrado = _aplicar_filtro_lista_vol(df_volume_real_filtrado, 'Nºconta', 'filtro_conta_contabil_waterfall_tc')

                                    for _col in ['Type 05', 'Type 06', 'Account', 'Fornecedor', 'Fornec.', 'Tipo']:
                                        df_volume_real_filtrado = _aplicar_filtro_ms_vol(df_volume_real_filtrado, _col, f'filtro_{_col}_waterfall_tc')

                                    for _col in ['Usuário', 'Material', 'Dt.lçto.', 'Texto breve']:
                                        df_volume_real_filtrado = _aplicar_filtro_ms_vol(df_volume_real_filtrado, _col, f'filtro_avancado_{_col}_waterfall_tc')
                                        
                                    # ═══ CORREÇÃO: Aplicar escopo de veículo ao volume real (mesma lógica do Home) ═══
                                    if escopo_veiculo_budget != "Todos (TC Total)" and df_volume_real_filtrado is not None:
                                        if 'Veículo' in df_volume_real_filtrado.columns:
                                            df_volume_real_filtrado = df_volume_real_filtrado[
                                                df_volume_real_filtrado['Veículo'] == escopo_veiculo_budget
                                            ].copy()
                                        
                                    # Filtrar pelos períodos selecionados (mesma lógica do budget)
                                    if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                        if col_mes_budget == 'Período_Ano':
                                            if 'Período_Ano' not in df_volume_real_filtrado.columns and 'Período' in df_volume_real_filtrado.columns and 'Ano' in df_volume_real_filtrado.columns:
                                                df_volume_real_filtrado['Período_Ano'] = (
                                                    df_volume_real_filtrado['Período'].astype(str).str.strip() + ' ' +
                                                    df_volume_real_filtrado['Ano'].astype(str).str.strip()
                                                )
                                            if 'Período_Ano' in df_volume_real_filtrado.columns:
                                                df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                        elif 'Período_Ano' in df_volume_real_filtrado.columns and col_mes_budget == 'Período_Ano':
                                            df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                        elif col_mes_waterfall_tc == 'Período_Ano' and 'Período_Ano' in df_volume_real_filtrado.columns:
                                            df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                        elif 'Período' in df_volume_real_filtrado.columns:
                                            periodos_str = [str(p) for p in periodos_selecionados_budget]
                                            # Se houver coluna Ano e o seletor traz "Mês Ano", filtrar com chave composta.
                                            if any(' ' in str(p) and str(p).split(' ')[-1].isdigit() for p in periodos_selecionados_budget) and 'Ano' in df_volume_real_filtrado.columns:
                                                df_tmp = df_volume_real_filtrado.copy()
                                                df_tmp['_Período_Ano_tmp'] = (
                                                    df_tmp['Período'].astype(str).str.strip() + " " + df_tmp['Ano'].astype(str).str.strip()
                                                )
                                                df_volume_real_filtrado = df_tmp[df_tmp['_Período_Ano_tmp'].astype(str).isin(periodos_str)].drop(columns=['_Período_Ano_tmp']).copy()
                                            else:
                                                df_volume_real_filtrado = df_volume_real_filtrado[df_volume_real_filtrado['Período'].astype(str).isin(periodos_str)].copy()
                                    
                                # Verificar se temos a coluna da dimensão selecionada
                                if chosen_dim_budget not in df_real_periodo.columns:
                                    st.warning(f"⚠️ Coluna '{chosen_dim_budget}' não encontrada nos dados.")
                                elif 'Custo' not in df_real_periodo.columns:
                                    st.warning("⚠️ Coluna 'Custo' não encontrada nos dados. A tabela requer classificação Fixo/Variável.")
                                else:
                                    # df_budget_periodo já foi criado acima, apenas usar
                                        
                                    # Agrupar dados reais e budget usando a mesma estrutura de chaves.
                                    colunas_agrupamento = [chosen_dim_budget]
                                    if 'Custo' != chosen_dim_budget and (
                                        'Custo' in df_real_periodo.columns or 'Custo' in df_budget_periodo.columns
                                    ):
                                        colunas_agrupamento.append('Custo')
                                    for _extra_col in ['Type 05', 'Type 06']:
                                        if _extra_col != chosen_dim_budget and (
                                            _extra_col in df_real_periodo.columns or _extra_col in df_budget_periodo.columns
                                        ):
                                            colunas_agrupamento.insert(0, _extra_col)

                                    # Remover duplicidades preservando a ordem.
                                    colunas_agrupamento = list(dict.fromkeys(colunas_agrupamento))

                                    df_real_periodo = df_real_periodo.copy()
                                    df_budget_periodo = df_budget_periodo.copy() if df_budget_periodo is not None else pd.DataFrame()

                                    # Garantir que ambos os lados possuam as mesmas colunas de agrupamento.
                                    for _col in colunas_agrupamento:
                                        if _col not in df_real_periodo.columns:
                                            df_real_periodo[_col] = "(Não informado)"
                                        if df_budget_periodo is not None and _col not in df_budget_periodo.columns:
                                            df_budget_periodo[_col] = "(Não informado)"
                                        
                                    # IMPORTANTE: df_real_periodo já vem de df_analise_budget que é uma cópia de df_filtrado_waterfall_tc
                                    # que tem a conversão de moeda aplicada. NÃO aplicar conversão novamente aqui para evitar duplicação

                                    # 🔒 Não perder linhas com chaves nulas: groupby descarta NaN.
                                    # O TC Ext calcula totais sem depender de Account/Type; aqui precisamos preservar essas linhas.
                                    for _col in colunas_agrupamento:
                                        if _col in df_real_periodo.columns:
                                            _s = df_real_periodo[_col]
                                            if pd.api.types.is_categorical_dtype(_s):
                                                if "(Não informado)" not in _s.cat.categories:
                                                    _s = _s.cat.add_categories(["(Não informado)"])
                                                df_real_periodo[_col] = _s.fillna("(Não informado)")
                                            else:
                                                df_real_periodo[_col] = _s.fillna("(Não informado)")
                                        
                                    df_real_agrupado = df_real_periodo.groupby(colunas_agrupamento)['Custo FP'].sum().reset_index()
                                        
                                    # Agrupar dados de budget pela mesma estrutura usada no Real.
                                    colunas_agrupamento_budget = colunas_agrupamento.copy()
                                        
                                    # Agrupar dados de budget por Account (sem Período, pois já está filtrado)
                                    # IMPORTANTE: Sempre usar df_budget_periodo que já foi filtrado pelos períodos selecionados
                                    # IMPORTANTE: df_budget_periodo já vem de df_budget_work que tem a conversão de moeda aplicada (linha 2743)
                                    # NÃO aplicar conversão novamente aqui para evitar duplicação
                                    if df_budget_periodo is not None and len(df_budget_periodo) > 0:
                                        if chosen_dim_budget not in df_budget_periodo.columns:
                                            st.warning(
                                                f"⚠️ A dimensão '{chosen_dim_budget}' não está disponível no Budget para este recorte. "
                                                "Usando '(Não informado)' como agrupador no Budget."
                                            )
                                            df_budget_periodo[chosen_dim_budget] = "(Não informado)"

                                        # 🔒 Não perder linhas com chaves nulas: groupby descarta NaN.
                                        for _col in colunas_agrupamento_budget:
                                            if _col in df_budget_periodo.columns:
                                                _s = df_budget_periodo[_col]
                                                if pd.api.types.is_categorical_dtype(_s):
                                                    if "(Não informado)" not in _s.cat.categories:
                                                        _s = _s.cat.add_categories(["(Não informado)"])
                                                    df_budget_periodo[_col] = _s.fillna("(Não informado)")
                                                else:
                                                    df_budget_periodo[_col] = _s.fillna("(Não informado)")

                                        df_budget_agrupado = df_budget_periodo.groupby(colunas_agrupamento_budget)['Custo FP'].sum().reset_index()
                                        df_budget_agrupado = df_budget_agrupado.rename(columns={'Custo FP': 'CustoFP_Budget'})
                                    else:
                                        # Se não há dados para os períodos selecionados, criar DataFrame vazio
                                        df_budget_agrupado = pd.DataFrame(columns=colunas_agrupamento_budget + ['CustoFP_Budget'])
                                        
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
                                            if col_mes_budget == 'Período_Ano':
                                                if 'Período_Ano' not in df_budget_vol_filtrado.columns and 'Período' in df_budget_vol_filtrado.columns and 'Ano' in df_budget_vol_filtrado.columns:
                                                    df_budget_vol_filtrado['Período_Ano'] = (
                                                        df_budget_vol_filtrado['Período'].astype(str).str.strip() + ' ' +
                                                        df_budget_vol_filtrado['Ano'].astype(str).str.strip()
                                                    )
                                                if 'Período_Ano' in df_budget_vol_filtrado.columns:
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                                else:
                                                    df_budget_vol_periodo = pd.DataFrame()
                                            elif col_mes_waterfall_tc == 'Período_Ano' and 'Período_Ano' in df_budget_vol_filtrado.columns:
                                                df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])].copy()
                                            elif 'Período' in df_budget_vol_filtrado.columns:
                                                periodos_str = [str(p) for p in periodos_selecionados_budget]
                                                # Se houver coluna Ano e o seletor traz "Mês Ano", filtrar com chave composta.
                                                if any(' ' in str(p) and str(p).split(' ')[-1].isdigit() for p in periodos_selecionados_budget) and 'Ano' in df_budget_vol_filtrado.columns:
                                                    df_tmp = df_budget_vol_filtrado.copy()
                                                    df_tmp['_Período_Ano_tmp'] = (
                                                        df_tmp['Período'].astype(str).str.strip() + " " + df_tmp['Ano'].astype(str).str.strip()
                                                    )
                                                    df_budget_vol_periodo = df_tmp[df_tmp['_Período_Ano_tmp'].astype(str).isin(periodos_str)].drop(columns=['_Período_Ano_tmp']).copy()
                                                else:
                                                    df_budget_vol_periodo = df_budget_vol_filtrado[df_budget_vol_filtrado['Período'].astype(str).isin(periodos_str)].copy()
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
                                    colunas_agrupamento_com_periodo = [col for col in colunas_agrupamento if col != 'Custo FP']
                                    df_tabela_flex = df_real_agrupado.merge(
                                        df_budget_agrupado,
                                        on=colunas_agrupamento_com_periodo,
                                        how='outer',
                                        suffixes=('', '_Budget')
                                    )
                                    df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'].fillna(0)
                                    df_tabela_flex['CustoFP_Budget'] = df_tabela_flex['CustoFP_Budget'].fillna(0)
                                    df_tabela_flex['Budget_Total_Custo'] = df_tabela_flex['CustoFP_Budget']
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

                                        # Regra de Flex (alinhada com TC Ext): tudo que NÃO é Fixo flexiona.
                                        custo_norm = df_tabela_flex['Custo'].astype(str).str.strip().str.lower()
                                        is_fixo = custo_norm == 'fixo'
                                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(is_fixo, 0)
                                        df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~is_fixo, 0)
                                        df_tabela_flex['_Flex_Bud_CustoFP_Custo'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                                            
                                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_CustoFP_Custo'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                                        df_tabela_flex['Flex BUD'] = df_tabela_flex['Flex BUD'].fillna(0)
                                            
                                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                                        df_tabela_flex['BUD'] = df_tabela_flex['BUD'].fillna(0)
                                            
                                        df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'] / df_tabela_flex['Volume_Real'].replace(0, 1)
                                        df_tabela_flex['Custo FP'] = df_tabela_flex['Custo FP'].fillna(0)
                                    else:
                                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['Volume_Real'] / df_tabela_flex['Volume_Budget'].replace(0, 1)
                                        df_tabela_flex['_Proporcao_Volume'] = df_tabela_flex['_Proporcao_Volume'].fillna(1.0)

                                        # Regra de Flex (alinhada com TC Ext): tudo que NÃO é Fixo flexiona.
                                        custo_norm = df_tabela_flex['Custo'].astype(str).str.strip().str.lower()
                                        is_fixo = custo_norm == 'fixo'
                                        df_tabela_flex['_Flex_Bud_Fixo'] = df_tabela_flex['Budget_Total_Custo'].where(is_fixo, 0)
                                        df_tabela_flex['_Flex_Bud_Variavel'] = (df_tabela_flex['Budget_Total_Custo'] * df_tabela_flex['_Proporcao_Volume']).where(~is_fixo, 0)
                                        df_tabela_flex['Flex BUD'] = df_tabela_flex['_Flex_Bud_Fixo'] + df_tabela_flex['_Flex_Bud_Variavel']
                                        df_tabela_flex['BUD'] = df_tabela_flex['Budget_Total_Custo']
                                        
                                    # Calcular diferenças
                                    df_tabela_flex['Flex Bud - BUD'] = df_tabela_flex['Flex BUD'] - df_tabela_flex['BUD']
                                    df_tabela_flex['Total - Flex Bud'] = df_tabela_flex['Custo FP'] - df_tabela_flex['Flex BUD']
                                    df_tabela_flex['Total / Flex Bud'] = df_tabela_flex.apply(
                                        lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                        axis=1
                                    )
                                        
                                    # Remover colunas auxiliares
                                    colunas_remover = ['Budget_Total_Custo', '_Proporcao_Volume', '_Flex_Bud_Fixo', '_Flex_Bud_Variavel', 'CustoFP_Budget', 'Volume_Real', 'Volume_Budget']
                                    if tipo_visualizacao == "CPU (Custo por Unidade)":
                                        colunas_remover.append('_Flex_Bud_CustoFP_Custo')
                                    df_tabela_flex = df_tabela_flex.drop(columns=[col for col in colunas_remover if col in df_tabela_flex.columns])
                                        
                                    # Agrupar mantendo estrutura hierárquica pela dimensão selecionada
                                    colunas_agrupamento_tabela = [chosen_dim_budget]
                                    if 'Custo' != chosen_dim_budget and 'Custo' in df_tabela_flex.columns:
                                        colunas_agrupamento_tabela.append('Custo')
                                    for _extra_col in ['Type 05', 'Type 06']:
                                        if _extra_col != chosen_dim_budget and _extra_col in df_tabela_flex.columns:
                                            colunas_agrupamento_tabela.insert(0, _extra_col)
                                        
                                    # Não precisa agrupar por Período, pois já está filtrado
                                    df_tabela_total_agrupado = df_tabela_flex.groupby(colunas_agrupamento_tabela).agg({
                                        'BUD': 'sum',
                                        'Flex Bud - BUD': 'sum',
                                        'Flex BUD': 'sum',
                                        'Total - Flex Bud': 'sum',
                                        'Custo FP': 'sum'
                                    }).reset_index()
                                        
                                    # Recalcular Total / Flex Bud após agrupamento
                                    df_tabela_total_agrupado['Total / Flex Bud'] = df_tabela_total_agrupado.apply(
                                        lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
                                        axis=1
                                    )
                                        
                                    # Filtrar linhas zeradas
                                    colunas_numericas = [col for col in df_tabela_total_agrupado.columns 
                                                        if pd.api.types.is_numeric_dtype(df_tabela_total_agrupado[col]) 
                                                        and col not in [chosen_dim_budget, 'Account', 'Custo', 'Type 05', 'Type 06', 'Período']]
                                    if colunas_numericas:
                                        df_tabela_total_agrupado_temp = df_tabela_total_agrupado[colunas_numericas].fillna(0)
                                        df_tabela_total_agrupado = df_tabela_total_agrupado[
                                            df_tabela_total_agrupado_temp.abs().sum(axis=1) > 0.0001
                                        ].copy()
                                        
                                    # Calcular valores para gráfico waterfall
                                    # Agrupar pela dimensão selecionada para o gráfico
                                    df_grafico = df_tabela_total_agrupado.groupby(chosen_dim_budget).agg({
                                        'BUD': 'sum',
                                        'Flex Bud - BUD': 'sum',
                                        'Flex BUD': 'sum',
                                        'Total - Flex Bud': 'sum',
                                        'Custo FP': 'sum'
                                    }).reset_index()
                                        
                                    # Preparar dados para gráfico waterfall
                                    bud_total = float(pd.to_numeric(df_grafico.get('BUD', 0), errors='coerce').fillna(0).sum())
                                    flex_bud_total = float(pd.to_numeric(df_grafico.get('Flex BUD', 0), errors='coerce').fillna(0).sum())
                                    total_real = float(pd.to_numeric(df_grafico['Custo FP'], errors='coerce').fillna(0).sum()) if 'Custo FP' in df_grafico.columns else 0.0
                                    flex_bud_menos_bud = float((flex_bud_total - bud_total) if pd.notna(flex_bud_total) and pd.notna(bud_total) else 0.0)
                                    total_menos_flex_bud = float((total_real - flex_bud_total) if pd.notna(total_real) and pd.notna(flex_bud_total) else 0.0)
                                        
                                    # ═══ Calcular variações APENAS pelas categorias selecionadas (Bug 5c fix) ═══
                                    # Indexar df_grafico pela dimensão para lookup rápido
                                    _grafico_idx = df_grafico.set_index(chosen_dim_budget)['Total - Flex Bud'].to_dict()
                                        
                                    labels_cats = []
                                    values_cats = []
                                    for cat in cats_sel_budget:
                                        delta = float(_grafico_idx.get(cat, 0.0))
                                        if abs(delta) > 1e-9:
                                            labels_cats.append(str(cat))
                                            values_cats.append(delta)
                                        
                                    # Ordenar por valor absoluto
                                    if labels_cats:
                                        sorted_idx = sorted(range(len(values_cats)), key=lambda i: abs(values_cats[i]), reverse=True)
                                        labels_cats = [labels_cats[i] for i in sorted_idx]
                                        values_cats = [values_cats[i] for i in sorted_idx]
                                        
                                    # Calcular remainder (categorias não selecionadas vão para "Outros")
                                    remainder = round(total_real - (bud_total + flex_bud_menos_bud + sum(values_cats)), 2)
                                        
                                    # Montar estrutura do waterfall
                                    labels_waterfall_tc = ["BUD"]
                                    values_waterfall_tc = [bud_total]
                                    measures_waterfall_tc = ["absolute"]
                                        
                                    # Adicionar Flex Bud - BUD (sempre incluir a etapa; é a barra amarela)
                                    labels_waterfall_tc.append("Flex Bud - BUD")
                                    values_waterfall_tc.append(flex_bud_menos_bud)
                                    measures_waterfall_tc.append("relative")
                                        
                                    # Adicionar categorias
                                    labels_waterfall_tc.extend(labels_cats)
                                    values_waterfall_tc.extend(values_cats)
                                    measures_waterfall_tc.extend(["relative"] * len(labels_cats))
                                        
                                    # Adicionar "Outros" se remainder for significativo
                                    if abs(remainder) >= 0.01:
                                        labels_waterfall_tc.append("Outros")
                                        values_waterfall_tc.append(remainder)
                                        measures_waterfall_tc.append("relative")
                                        
                                    # Adicionar barra final
                                    labels_waterfall_tc.append("Total")
                                    values_waterfall_tc.append(total_real)
                                    measures_waterfall_tc.append("total")
                                        
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
                                        
                                    for measure, value, label in zip(measures_waterfall_tc, values_waterfall_tc, labels_waterfall_tc):
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
                                                showarrow=False, font=dict(color=cor_azul, size=11), yshift=15,
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
                                                showarrow=False, font=dict(color=cor_texto, size=11), yshift=yshift_val,
                                                xref="x", yref="y", yanchor="middle"
                                            ))
                                            cumulative += value
                                        elif measure == "total":
                                            y_pos = value
                                            text_fmt_total = f"{abs(value):,.1f}"
                                            annotations_custom.append(dict(
                                                x=label, y=y_pos, text=text_fmt_total,
                                                showarrow=False, font=dict(color=cor_azul, size=11), yshift=20,
                                                xref="x", yref="y", yanchor="bottom"
                                            ))
                                        
                                    # Preparar informações detalhadas para o tooltip
                                    # ── Pré-calcular TOP gastos por categoria (Budget) ──
                                    _sufixo_bud_adv = ''
                                    if fator_conversao:
                                        if fator_conversao == 'K (milhares)':
                                            _sufixo_bud_adv = ' K'
                                        elif fator_conversao == 'M (Milhões)':
                                            _sufixo_bud_adv = ' M'
                                    _cat_labels_bud_adv = [l for l, m in zip(labels_waterfall_tc, measures_waterfall_tc) if m == 'relative' and l != 'Flex Bud - BUD' and l != 'Outros']
                                    _df_sap_bud_adv = _load_tc_veiculos_sapiens(ano_selecionado)
                                    _df_sap_bud_adv = _apply_session_filters_to_sapiens_tc(_df_sap_bud_adv)
                                    _vc_bud_adv = None
                                    for _c in ['Despesa Primaria', 'Custo FP', 'Total']:
                                        if _c in _df_sap_bud_adv.columns:
                                            _vc_bud_adv = _c
                                            break
                                    if _vc_bud_adv and tipo_visualizacao == 'Custo Total':
                                        if fator_conversao == 'K (milhares)':
                                            _df_sap_bud_adv[_vc_bud_adv] = pd.to_numeric(_df_sap_bud_adv[_vc_bud_adv], errors='coerce') / 1000
                                        elif fator_conversao == 'M (Milhões)':
                                            _df_sap_bud_adv[_vc_bud_adv] = pd.to_numeric(_df_sap_bud_adv[_vc_bud_adv], errors='coerce') / 1000000
                                        if moeda_codigo != 'BRL':
                                            _df_sap_bud_adv = converter_coluna_moeda(_df_sap_bud_adv, _vc_bud_adv, moeda_codigo, taxas_cambio)
                                    _top_bud_adv = _build_top_gastos_por_categoria(
                                        _df_sap_bud_adv, _cat_labels_bud_adv, chosen_dim_budget, _vc_bud_adv, moeda_simbolo, _sufixo_bud_adv,
                                    )

                                    hovertexts_budget = []
                                    for i, (label, value, measure) in enumerate(zip(labels_waterfall_tc, values_waterfall_tc, measures_waterfall_tc)):
                                        # Calcular valor acumulado até este ponto
                                        if measure == "absolute":
                                            acumulado = value
                                            tipo_medida = "Valor Inicial (BUD)"
                                        elif measure == "total":
                                            acumulado = value
                                            tipo_medida = "Valor Final (Total)"
                                        else:
                                            # Calcular acumulado para medidas relativas
                                            acumulado = bud_total
                                            for j in range(1, i):
                                                if measures_waterfall_tc[j] == "relative":
                                                    acumulado += values_waterfall_tc[j]
                                            acumulado += value
                                            tipo_medida = "Variação"
                                            
                                        # Formatar valor
                                        if tipo_visualizacao == "CPU (Custo por Unidade)":
                                            valor_formatado = f"{value:,.2f}"
                                            acumulado_formatado = f"{acumulado:,.2f}"
                                        else:
                                            sufixo = ""
                                            if fator_conversao:
                                                if fator_conversao == "K (milhares)":
                                                    sufixo = " K"
                                                elif fator_conversao == "M (Milhões)":
                                                    sufixo = " M"
                                            valor_formatado = f"{value:,.2f}{sufixo}"
                                            acumulado_formatado = f"{acumulado:,.2f}{sufixo}"
                                            
                                        # Criar texto do tooltip detalhado
                                        hover_text = (
                                            f"<b>{label}</b><br>"
                                            f"Tipo: {tipo_medida}<br>"
                                            f"Valor: {moeda_simbolo} {valor_formatado}<br>"
                                            f"Acumulado: {moeda_simbolo} {acumulado_formatado}<br>"
                                            f"Análise: Real x Budget<br>"
                                            f"Modo: {tipo_visualizacao}"
                                        )
                                        if measure == 'relative' and label != 'Flex Bud - BUD' and label != 'Outros':
                                            _top_entry_bud = _top_bud_adv.get(str(label).strip(), {})
                                            _top_str_bud = _top_entry_bud.get('hover_str', '')
                                            if _top_str_bud:
                                                hover_text += (
                                                    '<br>──────────<br>'
                                                    '<b>TOP 10 gastos:</b><br>'
                                                    + _top_str_bud
                                                )
                                        hovertexts_budget.append(hover_text)
                                        
                                    # Criar figura do waterfall
                                    fig = go.Figure(go.Waterfall(
                                        name="",  # Remover nome para evitar "undefined"
                                        orientation="v",
                                        measure=measures_waterfall_tc,
                                        x=labels_waterfall_tc,
                                        y=values_waterfall_tc,
                                        textposition="none",
                                        hovertext=hovertexts_budget,
                                        hovertemplate="%{hovertext}<extra></extra>",
                                        connector={"line": {"color": "rgba(0, 0, 0, 0)"}},
                                        increasing={"marker": {"color": cor_vermelha, "line": {"width": 0}}},
                                        decreasing={"marker": {"color": cor_verde, "line": {"width": 0}}},
                                        totals={"marker": {"color": cor_azul, "line": {"width": 0}}}
                                    ))
                                    # Forçar largura do waterfall para alinhar com overlays go.Bar
                                    fig.update_traces(width=0.8, selector=dict(type="waterfall"))
                                        
                                    # Adicionar overlay para "Flex Bud - BUD" (amarelo)
                                    if "Flex Bud - BUD" in labels_waterfall_tc:
                                        idx_flex = labels_waterfall_tc.index("Flex Bud - BUD")
                                        valor_flex = values_waterfall_tc[idx_flex]
                                        cumulative_flex = bud_total
                                        if valor_flex >= 0:
                                            base_flex = cumulative_flex
                                        else:
                                            base_flex = cumulative_flex + valor_flex
                                            
                                        # Overlay com borda na mesma cor para cobrir completamente a barra do waterfall
                                        fig.add_trace(go.Bar(
                                            x=['Flex Bud - BUD'],
                                            y=[abs(valor_flex)],
                                            base=[base_flex],
                                            marker_color=cor_amarela,
                                            marker_line=dict(width=2, color=cor_amarela),
                                            opacity=1.0,
                                            showlegend=False,
                                            textposition='none',
                                            width=0.82,
                                        ))
                                        
                                    # Adicionar overlay para "Outros" (laranja)
                                    if "Outros" in labels_waterfall_tc:
                                        idx_outros = labels_waterfall_tc.index("Outros")
                                        valor_outros = values_waterfall_tc[idx_outros]
                                        cumulative_outros = bud_total
                                        if "Flex Bud - BUD" in labels_waterfall_tc:
                                            cumulative_outros += values_waterfall_tc[labels_waterfall_tc.index("Flex Bud - BUD")]
                                        for i in range(2, idx_outros):
                                            cumulative_outros += values_waterfall_tc[i]
                                            
                                        if valor_outros >= 0:
                                            base_outros = cumulative_outros
                                        else:
                                            base_outros = cumulative_outros + valor_outros
                                            
                                        # Overlay com borda na mesma cor para cobrir completamente a barra do waterfall
                                        fig.add_trace(go.Bar(
                                            x=['Outros'],
                                            y=[abs(valor_outros)],
                                            base=[base_outros],
                                            marker_color=cor_laranja,
                                            marker_line=dict(width=2, color=cor_laranja),
                                            opacity=1.0,
                                            showlegend=False,
                                            textposition='none',
                                            width=0.82,
                                        ))
                                        
                                    # Calcular range do eixo Y
                                    if values_waterfall_tc:
                                        cumulative = 0
                                        all_y_positions = []
                                            
                                        for measure, value in zip(measures_waterfall_tc, values_waterfall_tc):
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
                                        
                                    # Atualizar layout (sem título no gráfico)
                                    fig.update_layout(
                                        barmode='overlay',
                                        title="",  # Remover título do gráfico completamente
                                        xaxis_title="Categoria",
                                        yaxis_title=f"{tipo_visualizacao} ({moeda_simbolo})",
                                        height=560,
                                        showlegend=False,
                                        plot_bgcolor="rgba(0,0,0,0)",
                                        paper_bgcolor="rgba(0,0,0,0)",
                                        margin=dict(l=80, r=40, t=50, b=40),
                                        font=dict(color=text_color, size=10),
                                        hovermode='closest',
                                        hoverlabel=dict(
                                            bgcolor="rgba(255, 255, 255, 0.95)",
                                            bordercolor="#1e6ba8",
                                            font=dict(
                                                size=12,
                                                family="Arial",
                                                color="#000000"
                                            ),
                                            align="left"
                                        ),
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
                                            tickfont=dict(size=12)
                                        ),
                                        yaxis=dict(
                                            showgrid=False,
                                            zeroline=False,
                                            showline=True,
                                            linecolor=grid_color,
                                            linewidth=1,
                                            range=[y_min, y_max],
                                            title=dict(font=dict(size=10)),
                                            tickfont=dict(size=12)
                                        )
                                    )
                                        
                                    # Adicionar anotações
                                    fig.update_layout(annotations=annotations_custom)
                                        
                                    # Exibir título acima do gráfico
                                    st.markdown("### 🌊 Waterfall Analysis — TC Veículos")
                                        
                                    # Exibir gráfico
                                    plotly_chart_safe(fig, use_container_width=True)
                                    
                                    # ═══ Criar dados para o painel por oficina (usando dados SEM filtro de oficina) ═══
                                    # O painel inferior sempre mostra TODAS as oficinas, independente do filtro inline
                                    if 'df_budget_filtrado_painel_base' in locals() and df_budget_filtrado_painel_base is not None:
                                        # Filtrar pelos períodos usando a base sem filtro de oficina
                                        if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                            periodos_str_panel = [str(p) for p in periodos_selecionados_budget]
                                            import re
                                            sel_tem_ano_panel = any(re.search(r"\b20\d{2}\b", str(p)) for p in periodos_selecionados_budget)
                                            
                                            if sel_tem_ano_panel:
                                                # Extrair meses e anos
                                                meses_panel = []
                                                anos_panel = []
                                                for p in periodos_str_panel:
                                                    match = re.match(r"(.+?)\s+(20\d{2})", p)
                                                    if match:
                                                        meses_panel.append(match.group(1).strip())
                                                        anos_panel.append(int(match.group(2)))
                                                
                                                if meses_panel and anos_panel:
                                                    df_budget_periodo_panel = df_budget_filtrado_painel_base[
                                                        df_budget_filtrado_painel_base['Período'].astype(str).str.strip().isin(meses_panel) &
                                                        df_budget_filtrado_painel_base['Ano'].isin(anos_panel)
                                                    ]
                                                else:
                                                    df_budget_periodo_panel = df_budget_filtrado_painel_base
                                            else:
                                                if 'Período' in df_budget_filtrado_painel_base.columns:
                                                    df_budget_periodo_panel = df_budget_filtrado_painel_base[
                                                        df_budget_filtrado_painel_base['Período'].astype(str).str.strip().isin(periodos_str_panel)
                                                    ]
                                                else:
                                                    df_budget_periodo_panel = df_budget_filtrado_painel_base
                                        else:
                                            df_budget_periodo_panel = df_budget_filtrado_painel_base
                                        
                                        # Real para painel (mesma lógica)
                                        if 'df_analise_budget_painel_base' in locals() and df_analise_budget_painel_base is not None:
                                            if col_mes_budget == 'Período_Ano' and 'Período_Ano' in df_analise_budget_painel_base.columns:
                                                df_real_periodo_panel = df_analise_budget_painel_base[
                                                    df_analise_budget_painel_base['Período_Ano'].astype(str).isin([str(p) for p in periodos_selecionados_budget])
                                                ]
                                            elif 'Período' in df_analise_budget_painel_base.columns:
                                                df_real_periodo_panel = df_analise_budget_painel_base[
                                                    df_analise_budget_painel_base['Período'].astype(str).isin([str(p) for p in periodos_selecionados_budget])
                                                ]
                                            else:
                                                df_real_periodo_panel = df_analise_budget_painel_base
                                        else:
                                            df_real_periodo_panel = df_real_periodo
                                        
                                        # Volumes para painel — FILTRAR por período (mesma lógica do gráfico de cima)
                                        df_budget_vol_periodo_panel = None
                                        if df_budget_vol_filtrado_painel_base is not None and not df_budget_vol_filtrado_painel_base.empty and 'Volume' in df_budget_vol_filtrado_painel_base.columns:
                                            _bvpb = df_budget_vol_filtrado_painel_base
                                            if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                                _per_str = [str(p) for p in periodos_selecionados_budget]
                                                if col_mes_budget == 'Período_Ano':
                                                    if 'Período_Ano' not in _bvpb.columns and 'Período' in _bvpb.columns and 'Ano' in _bvpb.columns:
                                                        _bvpb = _bvpb.copy()
                                                        _bvpb['Período_Ano'] = _bvpb['Período'].astype(str).str.strip() + ' ' + _bvpb['Ano'].astype(str).str.strip()
                                                    if 'Período_Ano' in _bvpb.columns:
                                                        df_budget_vol_periodo_panel = _bvpb[_bvpb['Período_Ano'].astype(str).isin(_per_str)]
                                                if df_budget_vol_periodo_panel is None:
                                                    if 'Período' in _bvpb.columns:
                                                        if any(' ' in str(p) and str(p).split(' ')[-1].isdigit() for p in periodos_selecionados_budget) and 'Ano' in _bvpb.columns:
                                                            _bvpb2 = _bvpb.copy()
                                                            _bvpb2['_PA_tmp'] = _bvpb2['Período'].astype(str).str.strip() + ' ' + _bvpb2['Ano'].astype(str).str.strip()
                                                            df_budget_vol_periodo_panel = _bvpb2[_bvpb2['_PA_tmp'].astype(str).isin(_per_str)].drop(columns=['_PA_tmp'])
                                                        else:
                                                            df_budget_vol_periodo_panel = _bvpb[_bvpb['Período'].astype(str).isin(_per_str)]
                                            if df_budget_vol_periodo_panel is None:
                                                df_budget_vol_periodo_panel = _bvpb
                                        df_volume_real_filtrado_panel = df_volume_real_filtrado if 'df_volume_real_filtrado' in locals() and df_volume_real_filtrado is not None else None
                                    else:
                                        df_budget_periodo_panel = df_budget_periodo if df_budget_periodo is not None else pd.DataFrame()
                                        df_real_periodo_panel = df_real_periodo
                                        df_budget_vol_periodo_panel = df_budget_vol_periodo if 'df_budget_vol_periodo' in locals() and df_budget_vol_periodo is not None else None
                                        df_volume_real_filtrado_panel = df_volume_real_filtrado if 'df_volume_real_filtrado' in locals() and df_volume_real_filtrado is not None else None
                                    
                                    _render_post_waterfall_panel_tc(
                                        ano_label=ano_selecionado,
                                        df_m1=df_budget_periodo_panel,
                                        df_m2=df_real_periodo_panel,
                                        df_vol_m1=df_budget_vol_periodo_panel,
                                        df_vol_m2=df_volume_real_filtrado_panel,
                                        total_inicial=bud_total,
                                        total_final=total_real,
                                        label_inicial='BUD',
                                        label_final='Total',
                                        label_flex='Flex Bud - BUD',
                                        tipo_visualizacao=tipo_visualizacao,
                                        moeda_simbolo=moeda_simbolo,
                                        fator_conversao=fator_conversao,
                                        moeda_codigo=moeda_codigo,
                                        taxas_cambio=taxas_cambio,
                                        value_column='Custo FP',
                                        key_prefix='wf_tc_budget',
                                        flex_delta_override=flex_bud_menos_bud,
                                    )
                                        
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
                                        'Custo FP': df_tabela_total_agrupado['Custo FP'].sum(),
                                        'Total / Flex Bud': df_tabela_total_agrupado['Custo FP'].sum() / df_tabela_total_agrupado['Flex BUD'].sum() if df_tabela_total_agrupado['Flex BUD'].sum() != 0 else 0
                                    }
                                        
                                    # Formatar resumo
                                    linha_resumo_geral_formatado = {}
                                    for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
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
                                    ordem_colunas = ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud']
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
                                        # Aplicar mesmos filtros do df_filtrado_waterfall_tc
                                        df_vol_real_para_calculo = df_volume.copy()
                                        if df_filtrado_waterfall_tc is not None and len(df_filtrado_waterfall_tc) > 0:
                                            colunas_filtro_vol = ['Veículo', 'Oficina', 'USI', 'Centrocst', 'Nºconta', 'Type 05', 'Type 06', 'Fornecedor', 'Fornec.', 'Tipo']
                                            for col_filtro in colunas_filtro_vol:
                                                if col_filtro in df_filtrado_waterfall_tc.columns and col_filtro in df_vol_real_para_calculo.columns:
                                                    valores_filtrados = df_filtrado_waterfall_tc[col_filtro].dropna().unique()
                                                    if len(valores_filtrados) > 0:
                                                        df_vol_real_para_calculo = df_vol_real_para_calculo[df_vol_real_para_calculo[col_filtro].isin(valores_filtrados)].copy()
                                            
                                        # Filtrar pelos períodos selecionados (mesma lógica do budget)
                                        if periodos_selecionados_budget and len(periodos_selecionados_budget) > 0:
                                            if col_mes_waterfall_tc == 'Período_Ano' and 'Período_Ano' in df_vol_real_para_calculo.columns:
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
                                            if col_mes_waterfall_tc == 'Período_Ano' and 'Período_Ano' in df_budget_vol_filtrado.columns:
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

                                    if periodos_selecionados_budget:
                                        if len(periodos_selecionados_budget) == 1:
                                            periodo_display = str(periodos_selecionados_budget[0])
                                        else:
                                            periodo_display = f"{len(periodos_selecionados_budget)} períodos"
                                    else:
                                        periodo_display = "N/A"
                                        
                                    volume_real_formatado = f"{volume_real_val:,.0f}"
                                    volume_budget_formatado = f"{volume_budget_val:,.0f}"
                                        
                                    col_vol_budget, col_vol_real = st.columns(2, gap="small")
                                    with col_vol_budget:
                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Budget ({periodo_display}):</strong> {volume_budget_formatado}</div>", unsafe_allow_html=True)
                                    with col_vol_real:
                                        st.markdown(f"<div style='font-size: 0.75rem;'><strong>Volume Real ({periodo_display}):</strong> {volume_real_formatado}</div>", unsafe_allow_html=True)
                                    st.markdown("<br>", unsafe_allow_html=True)
                                    
                                    # Linha de separação e espaçamento entre Resumo Geral e controles da tabela
                                    st.markdown("---")
                                    st.markdown("<div style='height:40px;'></div>", unsafe_allow_html=True)
                                        
                                    # Criar estrutura hierárquica com expanders
                                    expand_state_key = 'waterfall_tc_budget_expand_all'
                                    if expand_state_key not in st.session_state:
                                        st.session_state[expand_state_key] = False

                                    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 3])
                                    with ctrl_col1:
                                        if st.button('Expandir tudo', key='waterfall_tc_budget_expandir'):
                                            st.session_state[expand_state_key] = True
                                    with ctrl_col2:
                                        if st.button('Recolher tudo', key='waterfall_tc_budget_recolher'):
                                            st.session_state[expand_state_key] = False
                                    with ctrl_col3:
                                        st.caption('Controle aplicado aos expanders desta tabela Waterfall.')

                                    expandir_waterfall_budget = st.session_state[expand_state_key]

                                    if modo_tabela == "Fixo/Variável":
                                        for custo in ['Fixo', 'Variável']:
                                            df_custo = df_tabela_total_agrupado[df_tabela_total_agrupado['Custo'] == custo].copy()
                                                
                                            if len(df_custo) > 0:
                                                total_custo = df_custo['Custo FP'].sum()
                                                total_bud_custo = df_custo['BUD'].sum() if 'BUD' in df_custo.columns else 0
                                                total_custo_formatado = f"{total_custo:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_custo:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_custo:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_custo:,.2f}")
                                                    
                                                # Mostrar se há valor real OU budget (não ignorar categorias só com budget)
                                                if (abs(total_custo) > 0.0001 or abs(total_bud_custo) > 0.0001) and pd.notna(total_custo):
                                                    with st.expander(f"💰 {custo} - Total: {total_custo_formatado}", expanded=expandir_waterfall_budget):
                                                        # Nível 2: Type 05
                                                        if 'Type 05' in df_custo.columns:
                                                            for type05 in sorted(df_custo['Type 05'].dropna().unique()):
                                                                df_type05 = df_custo[df_custo['Type 05'] == type05].copy()
                                                                    
                                                                if len(df_type05) > 0:
                                                                    total_type05 = df_type05['Custo FP'].sum()
                                                                    total_type05_formatado = f"{total_type05:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type05:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type05:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type05:,.2f}")
                                                                        
                                                                    # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=expandir_waterfall_budget):
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
                                                                                            if 'Custo FP' in df_type06.columns:
                                                                                                if df_type06['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                                                    continue  # Pular Type 06 completamente zerado
                                                                                            
                                                                                        # Verificar se a coluna 'Total' existe antes de acessá-la
                                                                                        if 'Custo FP' in df_type06.columns:
                                                                                            total_type06 = df_type06['Custo FP'].sum()
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
                                                                                                        linha_resumo_type06['Custo FP'] = df_type06_filtrado['Custo FP'].sum()
                                                                                                        linha_resumo_type06['Total / Flex Bud'] = df_type06_filtrado['Custo FP'].sum() / df_type06_filtrado['Flex BUD'].sum() if df_type06_filtrado['Flex BUD'].sum() != 0 else 0
                                                                                                            
                                                                                                        # Formatar valores
                                                                                                        for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
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
                                                                                                            
                                                                                                        render_inline_summary_metrics(
                                                                                                            linha_resumo_type06,
                                                                                                            ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud'],
                                                                                                            ratio_columns={'Total / Flex Bud'},
                                                                                                            number_suffix=(
                                                                                                                ' K' if fator_conversao == 'K (milhares)'
                                                                                                                else ' M' if fator_conversao == 'M (Milhões)'
                                                                                                                else ''
                                                                                                            ),
                                                                                                        )
                                                                                                            
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
                                        if chosen_dim_budget in df_tabela_total_agrupado.columns and chosen_dim_budget not in colunas_agrupamento_total:
                                            colunas_agrupamento_total.append(chosen_dim_budget)
                                            
                                        if len(colunas_agrupamento_total) > 0:
                                            df_total_agrupado = df_tabela_total_agrupado.groupby(colunas_agrupamento_total).agg({
                                                'BUD': 'sum',
                                                'Flex Bud - BUD': 'sum',
                                                'Flex BUD': 'sum',
                                                'Total - Flex Bud': 'sum',
                                                'Custo FP': 'sum'
                                            }).reset_index()
                                                
                                            # Recalcular Total / Flex Bud após agrupamento
                                            df_total_agrupado['Total / Flex Bud'] = df_total_agrupado.apply(
                                                lambda row: row['Custo FP'] / row['Flex BUD'] if row['Flex BUD'] != 0 and pd.notnull(row['Flex BUD']) else 0,
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
                                                    total_type05 = df_type05['Custo FP'].sum()
                                                    total_type05_formatado = f"{total_type05:,.2f}" if tipo_visualizacao == "CPU (Custo por Unidade)" else (f"{total_type05:,.2f} K" if fator_conversao == "K (milhares)" else f"{total_type05:,.2f} M" if fator_conversao == "M (Milhões)" else f"{total_type05:,.2f}")
                                                        
                                                    # 🔧 CORREÇÃO: Remover condição restritiva para permitir exibição mesmo com total zero
                                                    with st.expander(f"📊 Type 05: {type05} - Total: {total_type05_formatado}", expanded=expandir_waterfall_budget):
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
                                                                            if 'Custo FP' in df_type06.columns:
                                                                                if df_type06['Custo FP'].fillna(0).abs().sum() <= 0.0001:
                                                                                    continue  # Pular Type 06 completamente zerado
                                                                            
                                                                        # Verificar se a coluna 'Total' existe antes de acessá-la
                                                                        if 'Custo FP' in df_type06.columns:
                                                                            total_type06 = df_type06['Custo FP'].sum()
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
                                                                                        linha_resumo_type06['Custo FP'] = df_type06_filtrado['Custo FP'].sum()
                                                                                        linha_resumo_type06['Total / Flex Bud'] = df_type06_filtrado['Custo FP'].sum() / df_type06_filtrado['Flex BUD'].sum() if df_type06_filtrado['Flex BUD'].sum() != 0 else 0
                                                                                            
                                                                                        # Formatar valores
                                                                                        for col in ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP']:
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
                                                                                            
                                                                                        render_inline_summary_metrics(
                                                                                            linha_resumo_type06,
                                                                                            ['BUD', 'Flex Bud - BUD', 'Flex BUD', 'Total - Flex Bud', 'Custo FP', 'Total / Flex Bud'],
                                                                                            ratio_columns={'Total / Flex Bud'},
                                                                                            number_suffix=(
                                                                                                ' K' if fator_conversao == 'K (milhares)'
                                                                                                else ' M' if fator_conversao == 'M (Milhões)'
                                                                                                else ''
                                                                                            ),
                                                                                        )
                                                                                            
                                                                                        html_table = criar_tabela_html_com_barra(df_display, linha_resumo_formatado_type06)
                                                                                        st.markdown(html_table, unsafe_allow_html=True)
                                        else:
                                            # Sem Type 05: não exibir nada (não deve acontecer)
                                            st.info("ℹ️ Dados sem estrutura hierárquica (Type 05).")

                    _render_sapiens_section_waterfall(
                        ano_selecionado,
                        "waterfall_budget_sapiens",
                    )
                            
            except Exception as e:
                st.error(f"❌ Erro ao gerar tabela Budget: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

# Função para obter mês atual em português
# Rodapé
try:
    import json as _json
    with open('versao.json', 'r', encoding='utf-8') as _f:
        _versao_str = _json.load(_f).get('versao', '1.91')
except Exception:
    _versao_str = '1.91'
_meses_pt = {1:'Janeiro',2:'Fevereiro',3:'Março',4:'Abril',5:'Maio',6:'Junho',
             7:'Julho',8:'Agosto',9:'Setembro',10:'Outubro',11:'Novembro',12:'Dezembro'}
_agora = datetime.now()
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Stellantis Cost Intelligence (SCI) | Versão {_versao_str} | {_meses_pt[_agora.month]} {_agora.year}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)

