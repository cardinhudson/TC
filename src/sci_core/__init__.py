"""Utilitários do fluxo SCI 100% Databricks."""

from sci_core.backend import (
    DEFAULT_REPO_ROOT,
    DEFAULT_WORKSPACE_DATA_ROOT,
    DEFAULT_WORKSPACE_PUBLISH_ROOT,
    get_backend,
    get_delta_root,
    get_publish_root,
    resolve_data_root,
)
from sci_core.io_delta import read_dataset, to_spark_safe, write_delta
from sci_core.io_excel import read_principal_excel
from sci_core.io_workspace import read_parquet_dataset, write_parquet_dataset
from sci_core.path_utils import resolve_excel_path, slugify_filename
from sci_core.transform_principal import (
    build_comparativo,
    processar_budget,
    processar_real,
)

__all__ = [
    "DEFAULT_REPO_ROOT",
    "DEFAULT_WORKSPACE_DATA_ROOT",
    "DEFAULT_WORKSPACE_PUBLISH_ROOT",
    "build_comparativo",
    "get_backend",
    "get_delta_root",
    "get_publish_root",
    "processar_budget",
    "processar_real",
    "read_dataset",
    "read_parquet_dataset",
    "read_principal_excel",
    "resolve_data_root",
    "to_spark_safe",
    "write_delta",
    "write_parquet_dataset",
]
