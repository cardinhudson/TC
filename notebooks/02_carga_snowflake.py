# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 02 Carga Snowflake
# MAGIC
# MAGIC **PENDENTE ICT**
# MAGIC
# MAGIC Este notebook não deve ser executado agora. Ele existe para manter o
# MAGIC fluxo dual-backend pronto quando houver:
# MAGIC 1. Secret Scope do Snowflake liberado
# MAGIC 2. Permissões ABPZA/B/C ativas
# MAGIC 3. Conector Spark-Snowflake operacional no cluster

# COMMAND ----------

import os
import sys
from typing import Any

dbutils: Any = globals().get("dbutils")

dbutils.widgets.text(
    "REPO_ROOT",
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci",
    "Repo root",
)
dbutils.widgets.text("DELTA_ROOT", "", "Delta root opcional")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"

for entry in (REPO_ROOT, f"{REPO_ROOT}/src"):
    if entry not in sys.path:
        sys.path.insert(0, entry)

os.environ["SCI_DATA_BACKEND"] = "snowflake"

raise RuntimeError(
    "PENDENTE ICT: habilite Secret Scope, permissões ABPZA/B/C e o conector "
    "Spark-Snowflake antes de executar este notebook. O fluxo Workspace atual "
    "permanece o mesmo; a futura carga lerá os mesmos datasets Parquet do "
    "Workspace."
)

# COMMAND ----------

# Quando o ICT liberar o ambiente, este notebook deve:
# 1. Ler Parquet em /Workspace/Users/.../Drafts/sci/workspace_publish
# 2. Mapear dataset -> tabela Snowflake
# 3. Carregar SCI_CURATED usando Secret Scope + key-pair
# 4. Validar contagens finais por tabela
