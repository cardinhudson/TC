"""
tc_core/data_source.py
======================
Abstração de leitura de dados — Parquet local **ou** Snowflake.

Controlada pela feature-flag ``SCI_DATA_BACKEND``:
  * ``"local"``     → pd.read_parquet  (padrão; comportamento atual)
  * ``"snowflake"`` → SELECT via snowflake-connector-python

Em **qualquer** backend o contrato público retorna ``pd.DataFrame | None``.
A troca é transparente para os ~30 loaders de ``tc_principal/shared.py``.

Uso rápido
----------
>>> from tc_core.data_source import read_table
>>> df = read_table("TC_Principal", "2025", "BUD", "df_principal_BUD")
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from tc_core.feature_flags import get_flag
from tc_core.utils.portabilidade import get_data_root

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Backend: LOCAL (parquet)
# ---------------------------------------------------------------------------


def _parquet_path(domain: str, ano: str, subfolder: str, table: str) -> Path:
    """Monta caminho: <data_root>/<domain>/<ano>[/<subfolder>]/<table>.parquet"""
    parts = [get_data_root(), domain, ano]
    if subfolder:
        parts.append(subfolder)
    return Path(*parts) / f"{table}.parquet"


def _read_local(
    domain: str,
    ano: str,
    subfolder: str,
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> Optional[pd.DataFrame]:
    path = _parquet_path(domain, ano, subfolder, table)
    if not path.exists():
        logger.debug("Parquet não encontrado: %s", path)
        return None
    return pd.read_parquet(path, columns=columns)


# ---------------------------------------------------------------------------
# Backend: SNOWFLAKE
# ---------------------------------------------------------------------------

_SNF_CONN: object | None = None

# Mapeamento parquet → tabela Snowflake (SCI_CURATED schema)
_TABLE_MAP: dict[str, str] = {
    # TC_Principal BUD
    "TC_PRINCIPAL/BUD/DF_PRINCIPAL_BUD": "SCI_TC_PRINCIPAL_BUD",
    "TC_PRINCIPAL/BUD/DF_VOL_VEICULOS_BUD": "SCI_TC_PRINCIPAL_VOL_BUD",
    "TC_PRINCIPAL/BUD/DF_VOLUME_FA_BUD": "SCI_TC_PRINCIPAL_VOL_FA_BUD",
    "TC_PRINCIPAL/BUD/DF_TEMPO_VEICULOS_BUD": "SCI_TC_PRINCIPAL_TEMPO_BUD",
    "TC_PRINCIPAL/BUD/DF_DEA_DEDICADO_BUD": "SCI_TC_PRINCIPAL_DEA_BUD",
    "TC_PRINCIPAL/BUD/DF_VEICULOS_CPU_BUD": "SCI_TC_PRINCIPAL_CPU_BUD",
    "TC_PRINCIPAL/BUD/DF_VEICULOS_CUSTO_FP_BUD": "SCI_TC_PRINCIPAL_CUSTO_FP_BUD",
    "TC_PRINCIPAL/BUD/DF_VEICULOS_PERCENTUAL_RATEIO_BUD": "SCI_TC_PRINCIPAL_RATEIO_BUD",
    "TC_PRINCIPAL/BUD/DF_VOL_VEICULOS_ACTUAL": "SCI_TC_PRINCIPAL_VOL_ACTUAL",
    "TC_PRINCIPAL/BUD/DF_VEICULOS_FP_SEM_DA_BUD": "SCI_TC_PRINCIPAL_FP_SEM_DA_BUD",
    "TC_PRINCIPAL/BUD/DF_VEICULOS_CUSTO_RATEADO_BUD": "SCI_TC_PRINCIPAL_CUSTO_RATEADO_BUD",
    # TC_Principal Real
    "TC_PRINCIPAL//DF_PRINCIPAL": "SCI_TC_PRINCIPAL_REAL",
    "TC_PRINCIPAL//DF_VOL_VEICULOS": "SCI_TC_PRINCIPAL_VOL_REAL",
    "TC_PRINCIPAL//DF_VOLUME_FA": "SCI_TC_PRINCIPAL_VOL_FA_REAL",
    "TC_PRINCIPAL//DF_TEMPO_VEICULOS": "SCI_TC_PRINCIPAL_TEMPO_REAL",
    "TC_PRINCIPAL//DF_TC_SAPIENS": "SCI_TC_SAPIENS",
    "TC_PRINCIPAL//DF_COMPARATIVO_REAL_BUDGET": "SCI_TC_COMPARATIVO",
    "TC_PRINCIPAL//DF_VEICULOS_CPU": "SCI_TC_PRINCIPAL_CPU_REAL",
    "TC_PRINCIPAL//DF_VEICULOS_CUSTO_FP": "SCI_TC_PRINCIPAL_CUSTO_FP_REAL",
    "TC_PRINCIPAL//DF_VEICULOS_PERCENTUAL_RATEIO": "SCI_TC_PRINCIPAL_RATEIO_REAL",
    "TC_PRINCIPAL//DF_DEA_DEDICADO": "SCI_TC_PRINCIPAL_DEA_REAL",
    # TC_Ext
    "TC_EXT//DF_FINAL": "SCI_TC_EXT_REAL",
    "TC_EXT//DF_VOL": "SCI_TC_EXT_VOL_REAL",
    "TC_EXT/BUD/DF_FINAL_BUD": "SCI_TC_EXT_BUD",
    "TC_EXT/BUD/DF_VOL_BUD": "SCI_TC_EXT_VOL_BUD",
    # Histórico consolidado
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/DF_PRINCIPAL_HISTORICO": "SCI_HIST_PRINCIPAL_REAL",
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/DF_VOL_HISTORICO": "SCI_HIST_VOL_REAL",
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/BUD/DF_PRINCIPAL_HISTORICO_BUD": "SCI_HIST_PRINCIPAL_BUD",
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/BUD/DF_VOL_HISTORICO_BUD": "SCI_HIST_VOL_BUD",
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/DF_VEICULOS_CUSTO_FP_HISTORICO": "SCI_HIST_CUSTO_FP_REAL",
    "TC_PRINCIPAL/HISTORICO_CONSOLIDADO/BUD/DF_VEICULOS_CUSTO_FP_HISTORICO_BUD": "SCI_HIST_CUSTO_FP_BUD",
    # Forecast
    "TC_PRINCIPAL/FORECAST/FORECAST_COMPLETO": "SCI_FORECAST_COMPLETO",
    "TC_PRINCIPAL/FORECAST/DF_VOL_HISTORICO": "SCI_FORECAST_VOL",
    "TC_PRINCIPAL/FORECAST/FORECAST_VEICULOS_CUSTO_FP": "SCI_FORECAST_CUSTO_FP",
}


def _snowflake_table_name(domain: str, subfolder: str, table: str) -> str:
    """Resolve o nome da tabela Snowflake a partir do lookup map."""
    key = f"{domain.upper()}/{subfolder.upper()}/{table.upper()}"
    name = _TABLE_MAP.get(key)
    if name:
        return name
    # Fallback genérico
    parts = [p for p in [domain, subfolder, table] if p]
    return "SCI_" + "_".join(parts).upper()


def _get_snowflake_conn():
    """Cria (ou reutiliza) conexão Snowflake via credenciais do secrets.py."""
    global _SNF_CONN
    if _SNF_CONN is not None:
        try:
            _SNF_CONN.cursor().execute("SELECT 1")
            return _SNF_CONN
        except Exception:  # noqa: BLE001 — conexão morta, reconectar
            _SNF_CONN = None

    from tc_core.secrets import get_secret

    account = get_secret("SNF_ACCOUNT")
    user = get_secret("SNF_USER")
    warehouse = get_secret("SNF_WAREHOUSE", default="WH_LAB_FIN")
    database = get_secret("SNF_DATABASE", default="DB_BL_FIN")
    schema = get_secret("SNF_SCHEMA", default="SCI_CURATED")
    role = get_secret("SNF_ROLE", default="ZSNF.FINdtsana")
    private_key_pem = get_secret("SNF_PRIVATE_KEY")
    # Em Databricks, a chave pode estar sob o GUID do secret scope
    if not private_key_pem:
        private_key_pem = get_secret("snf-key-40543c161db811f18782752f4f83e915")

    if not all([account, user]):
        raise RuntimeError(
            "Credenciais Snowflake ausentes. "
            "Defina SNF_ACCOUNT e SNF_USER em secrets / env."
        )

    import snowflake.connector  # type: ignore[import-untyped]

    connect_kwargs: dict = dict(
        account=account,
        user=user,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )

    if private_key_pem:
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
            load_pem_private_key,
        )

        p_key = load_pem_private_key(private_key_pem.encode(), password=None)
        pkb = p_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
        connect_kwargs["private_key"] = pkb
    else:
        # Fallback: senha (dev local)
        password = get_secret("SNF_PASSWORD")
        if password:
            connect_kwargs["password"] = password

    _SNF_CONN = snowflake.connector.connect(**connect_kwargs)
    return _SNF_CONN


def _read_snowflake(
    domain: str,
    ano: str,
    subfolder: str,
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> Optional[pd.DataFrame]:
    """Lê uma tabela/view Snowflake e retorna DataFrame.

    Convenção de nomes no Snowflake:
      ``DB_BL_FIN.SCI_CURATED.SCI_<IDENTITY>``
    Ex: ``DB_BL_FIN.SCI_CURATED.SCI_TC_PRINCIPAL_BUD``
    """
    conn = _get_snowflake_conn()

    fq_table = _snowflake_table_name(domain, subfolder, table)
    col_clause = ", ".join(f'"{c}"' for c in columns) if columns else "*"

    # Tabelas históricas/consolidadas não têm filtro por ano
    if ano:
        query = f'SELECT {col_clause} FROM "{fq_table}" WHERE "Ano" = %s'  # noqa: S608
        params: list = [int(ano)]
    else:
        query = f'SELECT {col_clause} FROM "{fq_table}"'  # noqa: S608
        params = []

    cur = conn.cursor()
    try:
        cur.execute(query, params) if params else cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        if not rows:
            return None
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def read_table(
    domain: str,
    ano: str,
    subfolder: str = "",
    table: str = "",
    columns: Optional[Sequence[str]] = None,
) -> Optional[pd.DataFrame]:
    """
    Lê dados de forma transparente — Parquet local ou Snowflake.

    Parameters
    ----------
    domain : str
        Domínio de dados (ex: ``"TC_Principal"``, ``"TC_Ext"``).
    ano : str
        Ano dos dados (ex: ``"2025"``).
    subfolder : str
        Subpasta dentro do domínio (ex: ``"BUD"``). Vazio se raiz.
    table : str
        Nome da tabela/arquivo sem extensão (ex: ``"df_principal_BUD"``).
    columns : list[str] | None
        Colunas a carregar (None = todas).

    Returns
    -------
    pd.DataFrame | None
        DataFrame ou None se tabela/arquivo não existe.
    """
    backend = get_flag("SCI_DATA_BACKEND", default="local")

    if backend == "snowflake":
        return _read_snowflake(domain, ano, subfolder, table, columns)

    return _read_local(domain, ano, subfolder, table, columns)


def list_available_years(domain: str = "TC_Principal", require_subfolder: str = "BUD") -> list[int]:
    """Lista anos com dados disponíveis (ambos backends)."""
    backend = get_flag("SCI_DATA_BACKEND", default="local")

    if backend == "snowflake":
        conn = _get_snowflake_conn()
        tbl = _snowflake_table_name(domain, require_subfolder, "df_principal" + ("_BUD" if require_subfolder == "BUD" else ""))
        cur = conn.cursor()
        try:
            cur.execute(
                f'SELECT DISTINCT "Ano" FROM "{tbl}" ORDER BY "Ano" DESC'  # noqa: S608
            )
            return [int(row[0]) for row in cur.fetchall()]
        except Exception:  # noqa: BLE001
            return []
        finally:
            cur.close()

    # Local
    base = get_data_root() / domain
    anos: list[int] = []
    if base.exists():
        for d in sorted(base.iterdir(), reverse=True):
            if d.is_dir() and d.name.isdigit():
                sub = d / require_subfolder if require_subfolder else d
                if sub.is_dir() and any(sub.glob("*.parquet")):
                    anos.append(int(d.name))
    return anos
