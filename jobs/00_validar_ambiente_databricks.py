# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — Validação Inicial Databricks
# MAGIC
# MAGIC Use este notebook como primeira etapa.
# MAGIC Ele valida se o ambiente Databricks está pronto para executar a pipeline
# MAGIC completa na nuvem usando DBFS agora e Volumes quando liberado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros

# COMMAND ----------

import os
import sys
from pathlib import Path

dbutils.widgets.text("REPO_ROOT", "/Workspace/Users/u235107@inetpsa.com/Drafts/sci", "Repo root no Databricks")
dbutils.widgets.text("DATA_ROOT", "", "Raiz cloud dos dados (opcional)")
dbutils.widgets.text("ANO", "2026", "Ano")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tc_core.utils.portabilidade import check_excel_exists, resolve_data_root
from tc_core.utils.snowflake_spark import smoke_test_snowflake_oauth

DATA_ROOT = str(resolve_data_root(DATA_ROOT_INPUT, log=print))

os.environ["SCI_SHARED_DATA_ROOT"] = DATA_ROOT
os.environ["SCI_CLOUD"] = "1"

print(f"REPO_ROOT: {REPO_ROOT}")
print(f"DATA_ROOT: {DATA_ROOT}")
print(f"ANO: {ANO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validar caminhos cloud

# COMMAND ----------

repo_path = Path(REPO_ROOT)
checks = {
    "repo_root_existe": repo_path.exists(),
    "data_root_existe": Path(DATA_ROOT).exists(),
}

for name, ok in checks.items():
    print(f"{'OK' if ok else 'ERRO'} - {name}")

if not checks["repo_root_existe"]:
    raise FileNotFoundError(f"Repo não encontrado: {repo_path}")
if not checks["data_root_existe"]:
    raise FileNotFoundError(f"DATA_ROOT não encontrado: {DATA_ROOT}")

excel_path = check_excel_exists(ANO, DATA_ROOT, log=print)
print(f"Excel localizado: {excel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validar imports do projeto

# COMMAND ----------

import importlib

modules = [
    "processamento_dados_veiculos_BUD",
    "processamento_dados_veiculos",
    "tc_core.data_source",
    "tc_core.secrets",
    "tc_principal.shared",
]

for module_name in modules:
    importlib.import_module(module_name)
    print(f"OK - import {module_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validar dependências Python

# COMMAND ----------

packages = [
    "pandas",
    "pyarrow",
    "openpyxl",
    "pyspark",
]

for package_name in packages:
    importlib.import_module(package_name)
    print(f"OK - pacote {package_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Testar conexão Snowflake via Spark OAuth

# COMMAND ----------

snowflake_smoke_df = smoke_test_snowflake_oauth(spark, schema="PUBLIC")
row = snowflake_smoke_df.collect()[0]
print(
    "OK - Snowflake conectado: "
    f"user={row['USER_NAME']} role={row['ROLE_NAME']} "
    f"warehouse={row['WAREHOUSE_NAME']} db={row['DB_NAME']} schema={row['SCHEMA_NAME']} teste={row['TESTE']}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultado

# COMMAND ----------

print("AMBIENTE VALIDADO ✅")
print("Próximo notebook: jobs/01_criar_schema.py")