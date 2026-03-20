"""
tc_core/feature_flags.py
========================
Feature-flags leves para rollout incremental da migração cloud.

Prioridade de leitura:
  1. Variável de ambiente  (``SCI_<FLAG>``)
  2. Databricks widget / job param  (experimental)
  3. Valor padrão compilado

Flags atuais
-------------
``SCI_DATA_BACKEND``   — ``"local"`` | ``"snowflake"``  (padrão: local)
``SCI_EMAIL_BACKEND``  — ``"graph"`` | ``"smtp"``       (padrão: graph)
``SCI_LLM_PROVIDER``   — ``"openai"`` | ``"azure_openai"`` | ``"databricks"``
``SCI_SCHEDULER``      — ``"apscheduler"`` | ``"databricks_jobs"``
"""

from __future__ import annotations

import os
from typing import Optional

# ---------------------------------------------------------------------------
# Defaults compilados — alterados apenas via env / Databricks config
# ---------------------------------------------------------------------------
_DEFAULTS: dict[str, str] = {
    "SCI_DATA_BACKEND": "local",
    "SCI_EMAIL_BACKEND": "graph",
    "SCI_LLM_PROVIDER": "openai",
    "SCI_SCHEDULER": "apscheduler",
}


def get_flag(name: str, *, default: Optional[str] = None) -> str:
    """Retorna o valor de uma feature-flag.

    Parameters
    ----------
    name : str
        Nome da flag (ex: ``"SCI_DATA_BACKEND"``).
    default : str | None
        Override do default compilado.

    Returns
    -------
    str
        Valor efetivo da flag (sempre lowercase).
    """
    compiled_default = _DEFAULTS.get(name, default or "")
    val = os.environ.get(name, compiled_default)
    return val.lower().strip() if val else ""


def set_flag(name: str, value: str) -> None:
    """Define uma feature-flag em runtime (útil para testes e previews)."""
    os.environ[name] = value


def all_flags() -> dict[str, str]:
    """Retorna snapshot de todas as flags com seus valores efetivos."""
    return {name: get_flag(name) for name in _DEFAULTS}
