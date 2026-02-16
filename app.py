import streamlit as st

from tc_principal.pages.home_tc import render as render_home_tc

from tc_principal.pages.extracao_dados_tc import (
    render as render_extracao_dados_tc,
)
from tc_principal.pages.debug_calculos_tc import (
    render as render_debug_calculos_tc,
)


st.set_page_config(
    page_title="SCI | Stellantis Cost Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Stellantis Cost Intelligence (SCI)")
st.caption("Selecione o módulo no menu lateral.")

PAGES = {
    "TC Ext (Linhas Secundárias)": [
        st.Page(
            "tc_ext/pages/home_ext.py",
            title="Home (TC Ext)",
            url_path="tc-ext",
        ),
        st.Page(
            "pages/1 - Waterfall.py",
            title="Waterfall",
            url_path="tc-ext-waterfall",
        ),
        st.Page(
            "pages/2 - Best Estimate - Simulador.py",
            title="Best Estimate (Simulador)",
            url_path="tc-ext-best-estimate-simulador",
        ),
        st.Page(
            "tc_ext/pages/be_analise_ext.py",
            title="Best Estimate (Análise)",
            url_path="tc-ext-best-estimate-analise",
        ),
        st.Page(
            "pages/5 - Extração de Dados.py",
            title="Extração de Dados",
            url_path="tc-ext-extracao",
        ),
    ],
    "TC Veículos": [
        st.Page(render_home_tc, title="Home (TC Veículos)", url_path="tc"),
        st.Page(
            "tc_principal/pages/waterfall_tc.py",
            title="Waterfall",
            url_path="tc-waterfall",
        ),
        st.Page(
            "tc_principal/pages/best_estimate_simulador_tc.py",
            title="Best Estimate (Simulador)",
            url_path="tc-best-estimate-simulador",
        ),
        st.Page(
            "tc_principal/pages/best_estimate_analise_tc.py",
            title="Best Estimate (Análise)",
            url_path="tc-best-estimate-analise",
        ),
        st.Page(
            render_extracao_dados_tc,
            title="Extração de Dados",
            url_path="tc-extracao",
        ),
        st.Page(
            render_debug_calculos_tc,
            title="Debug de Cálculos",
            url_path="tc-debug",
        ),
    ],
    "Documentação": [
        st.Page(
            "pages/6 - Documentacao.py",
            title="Documentação (Projeto)",
            url_path="documentacao",
        ),
    ],
}

pg = st.navigation(PAGES)
pg.run()
