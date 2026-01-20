import streamlit as st

from tc_principal.pages.home_tc import render as render_home_tc
from tc_principal.pages.waterfall_tc import render as render_waterfall_tc
from tc_principal.pages.waterfall_analysis_tc import (
    render as render_waterfall_analysis_tc,
)
from tc_principal.pages.best_estimate_simulador_tc import (
    render as render_best_estimate_simulador_tc,
)
from tc_principal.pages.best_estimate_analise_tc import (
    render as render_best_estimate_analise_tc,
)
from tc_principal.pages.extracao_dados_tc import (
    render as render_extracao_dados_tc,
)


st.set_page_config(
    page_title="TC | Portal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Portal TC")
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
            "pages/4 - Waterfall_Analysis.py",
            title="Waterfall (Análise)",
            url_path="tc-ext-waterfall-analise",
        ),
        st.Page(
            "pages/2 - Best Estimate - Simulador.py",
            title="Best Estimate (Simulador)",
            url_path="tc-ext-best-estimate-simulador",
        ),
        st.Page(
            "pages/3 - Best Estimate - Análise.py",
            title="Best Estimate (Análise)",
            url_path="tc-ext-best-estimate-analise",
        ),
        st.Page(
            "pages/5 - Extração de Dados.py",
            title="Extração de Dados",
            url_path="tc-ext-extracao",
        ),
    ],
    "TC (Planta Principal)": [
        st.Page(render_home_tc, title="Home (TC)", url_path="tc"),
        st.Page(
            render_waterfall_tc,
            title="Waterfall",
            url_path="tc-waterfall",
        ),
        st.Page(
            render_waterfall_analysis_tc,
            title="Waterfall (Análise)",
            url_path="tc-waterfall-analise",
        ),
        st.Page(
            render_best_estimate_simulador_tc,
            title="Best Estimate (Simulador)",
            url_path="tc-best-estimate-simulador",
        ),
        st.Page(
            render_best_estimate_analise_tc,
            title="Best Estimate (Análise)",
            url_path="tc-best-estimate-analise",
        ),
        st.Page(
            render_extracao_dados_tc,
            title="Extração de Dados",
            url_path="tc-extracao",
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
