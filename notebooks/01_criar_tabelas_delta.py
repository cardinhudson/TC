# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 01 Criar Estrutura Workspace
# MAGIC
# MAGIC Cria a estrutura de publicação em Workspace Files para o backend atual.

# COMMAND ----------

import importlib
import os
import sys
from pathlib import Path

dbutils = globals().get("dbutils")

dbutils.widgets.text(
    "REPO_ROOT",
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci",
    "Repo root",
)
dbutils.widgets.text("PUBLISH_ROOT", "", "Publish root opcional")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
PUBLISH_ROOT_INPUT = dbutils.widgets.get("PUBLISH_ROOT").strip()

for entry in (REPO_ROOT, f"{REPO_ROOT}/src"):
    if entry not in sys.path:
        sys.path.insert(0, entry)

backend_module = importlib.import_module("sci_core.backend")
get_publish_root = backend_module.get_publish_root

os.environ["SCI_DATA_BACKEND"] = "databricks"
PUBLISH_ROOT = get_publish_root(
    PUBLISH_ROOT_INPUT,
    repo_root=REPO_ROOT,
    log=print,
)

TABLE_DIRS = {
    "tc_principal_bud": PUBLISH_ROOT / "tc_principal_bud",
    "tc_principal_real": PUBLISH_ROOT / "tc_principal_real",
    "tc_comparativo": PUBLISH_ROOT / "tc_comparativo",
    "tc_execucao_log": PUBLISH_ROOT / "tc_execucao_log",
}

for table_name, path in TABLE_DIRS.items():
    path.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(path) / "_manifest.txt"
    manifest_path.write_text(
        "table="
        f"{table_name}\nbackend=databricks\npath={path}\n"
        "format=parquet_workspace_files\n",
        encoding="utf-8",
    )
    print(f"[INFO] Estrutura pronta: {table_name} -> {path}")
    for child in sorted(Path(path).iterdir()):
        print(f"  - {child.name}")

print(f"[INFO] PUBLISH_ROOT pronto: {PUBLISH_ROOT}")
