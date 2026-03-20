"""
tc_core/secrets.py
==================
Abstração unificada para leitura de segredos.

Ordem de prioridade (primeira fonte que responder ganha):
  1. Databricks Secret Scope  (dbutils.secrets.get)
  2. Variável de ambiente       (os.environ)
  3. Arquivo .env local         (python-dotenv) — apenas em dev
  4. Streamlit secrets          (st.secrets)

Em cloud (Databricks Apps / Jobs) o Scope é a fonte canônica.
Em desktop/dev os fallbacks garantem retrocompatibilidade total.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

_DBUTILS: object | None = None  # lazy-loaded


def _get_dbutils():
    """Retorna dbutils se rodando em Databricks, senão None."""
    global _DBUTILS
    if _DBUTILS is not None:
        return _DBUTILS
    try:
        # Dentro de notebook Databricks ou Databricks Apps
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession  # type: ignore[import-untyped]

        spark = SparkSession.getActiveSession()
        if spark is not None:
            _DBUTILS = DBUtils(spark)
            return _DBUTILS
    except Exception:  # noqa: BLE001
        pass
    return None


@lru_cache(maxsize=1)
def _load_dotenv_once() -> None:
    """Carrega .env uma única vez (noop se python-dotenv ausente)."""
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]

        # Procura .env na raiz do projeto
        from tc_core.utils.portabilidade import get_base_path

        env_path = get_base_path() / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
    except ImportError:
        pass


def _streamlit_secret(key: str) -> Optional[str]:
    """Tenta ler de st.secrets sem importar Streamlit globalmente."""
    try:
        import streamlit as st  # noqa: PLC0415

        return st.secrets.get(key)
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

_DEFAULT_SCOPE = "sci"


def get_secret(
    key: str,
    *,
    scope: str = _DEFAULT_SCOPE,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Retorna o valor do segredo *key*.

    Parameters
    ----------
    key : str
        Nome do segredo (ex: ``"OPENAI_API_KEY"``, ``"SNF_ACCOUNT"``).
    scope : str
        Scope no Databricks Secret Scope (padrão ``"sci"``).
    default : str | None
        Valor padrão caso nenhuma fonte responda.

    Returns
    -------
    str | None
    """
    # 1. Databricks Secret Scope
    dbu = _get_dbutils()
    if dbu is not None:
        try:
            val = dbu.secrets.get(scope=scope, key=key)
            if val:
                return val
        except Exception:  # noqa: BLE001
            logger.debug("Databricks scope=%s key=%s não encontrado", scope, key)

    # 1b. Databricks SDK (fallback para Databricks Apps sem dbutils)
    try:
        import base64 as _b64
        from databricks.sdk import WorkspaceClient as _WC  # noqa: PLC0415

        _resp = _WC().secrets.get_secret(scope=scope, key=key)
        if _resp.value:
            val = _b64.b64decode(_resp.value).decode("utf-8")
            if val.strip():
                return val.strip()
    except Exception:  # noqa: BLE001
        logger.debug("Databricks SDK scope=%s key=%s falhou", scope, key)

    # 2. Variável de ambiente
    val = os.environ.get(key)
    if val:
        return val

    # 3. .env local (apenas dev)
    _load_dotenv_once()
    val = os.environ.get(key)
    if val:
        return val

    # 4. Streamlit secrets
    val = _streamlit_secret(key)
    if val:
        return val

    return default


def is_databricks() -> bool:
    """True se executando dentro de um ambiente Databricks."""
    return _get_dbutils() is not None
