"""
tc_core/telemetry.py
====================
Telemetria leve para rastreamento de leituras de dados no SCI.

Quando ``SCI_DEBUG_DATA_TRACE=true``, acumula logs em ``st.session_state``
para exibição no debug panel. Sempre loga via ``logging`` para stdout
(visível no Databricks).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from tc_core.feature_flags import get_flag

logger = logging.getLogger("sci.telemetry")

# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def log_data_source(
    *,
    consumer: str,
    logical_dataset: str,
    physical_path: str,
    mode: str,
    nrows: int,
    ncols: int,
    load_ms: float,
) -> None:
    """Registra uma leitura de dados para diagnóstico.

    Parameters
    ----------
    consumer : str
        Identificador do visual/loader (ex: ``"home_kpi_cpu"``).
    logical_dataset : str
        Nome lógico do dataset (ex: ``"df_principal_BUD"``).
    physical_path : str
        Caminho físico lido.
    mode : str
        ``"FULL"`` | ``"THIN"`` | ``"AGG"``.
    nrows, ncols : int
        Dimensões do DataFrame resultante.
    load_ms : float
        Tempo de leitura em milissegundos.
    """
    msg = (
        f"[DATA] consumer={consumer} | dataset={logical_dataset} | "
        f"mode={mode} | rows={nrows} cols={ncols} | "
        f"load_ms={load_ms:.1f} | path={physical_path}"
    )
    logger.info(msg)

    if _debug_trace_enabled():
        _accumulate_trace(
            consumer=consumer,
            logical_dataset=logical_dataset,
            physical_path=physical_path,
            mode=mode,
            nrows=nrows,
            ncols=ncols,
            load_ms=load_ms,
        )


def get_trace_log() -> list[dict]:
    """Retorna os registros acumulados na sessão (lista de dicts)."""
    try:
        import streamlit as st
        return list(st.session_state.get("_data_trace_log", []))
    except Exception:
        return []


def clear_trace_log() -> None:
    """Limpa os registros acumulados."""
    try:
        import streamlit as st
        st.session_state["_data_trace_log"] = []
    except Exception:
        pass


class perf_timer:
    """Context-manager simples para medir tempo em ms."""

    __slots__ = ("elapsed_ms", "_t0")

    def __enter__(self) -> "perf_timer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        self.elapsed_ms = (time.perf_counter() - self._t0) * 1000


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _debug_trace_enabled() -> bool:
    return get_flag("SCI_DEBUG_DATA_TRACE", default="false") == "true"


def _accumulate_trace(**kwargs: object) -> None:
    try:
        import streamlit as st
        if "_data_trace_log" not in st.session_state:
            st.session_state["_data_trace_log"] = []
        st.session_state["_data_trace_log"].append(kwargs)
    except Exception:
        pass
