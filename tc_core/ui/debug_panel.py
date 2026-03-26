"""
tc_core/ui/debug_panel.py
=========================
Painel opcional de debug para visualizar logs de telemetria de dados.

Renderiza apenas quando ``SCI_DEBUG_DATA_TRACE=true``.
"""

from __future__ import annotations


def render_data_trace_panel() -> None:
    """Renderiza expander com tabela de leituras acumuladas na sessão."""
    from tc_core.feature_flags import get_flag

    if get_flag("SCI_DEBUG_DATA_TRACE", default="false") != "true":
        return

    import streamlit as st
    from tc_core.telemetry import get_trace_log, clear_trace_log

    entries = get_trace_log()
    if not entries:
        return

    with st.expander(f"🔍 Data Trace Log ({len(entries)} leituras)", expanded=False):
        import pandas as pd

        df = pd.DataFrame(entries)
        col_order = [
            c for c in ["consumer", "logical_dataset", "mode", "nrows", "ncols", "load_ms", "physical_path"]
            if c in df.columns
        ]
        st.dataframe(df[col_order], use_container_width=True, hide_index=True)
        if st.button("Limpar trace log"):
            clear_trace_log()
            st.rerun()
