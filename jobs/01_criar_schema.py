# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — Criar Schema Curated no Snowflake
# MAGIC
# MAGIC Cria o schema `SCI_CURATED` e todas as tabelas necessárias no Snowflake.
# MAGIC Executar uma única vez ou reexecutar quando necessário.
# MAGIC Usa o conector Spark-Snowflake com OAuth e não depende de Secret Scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bootstrap via Spark-Snowflake OAuth

# COMMAND ----------

from tc_core.utils.snowflake_spark import (
    bootstrap_schema_objects,
    get_snowflake_spark_options,
    smoke_test_snowflake_oauth,
)

smoke_df = smoke_test_snowflake_oauth(spark, schema="PUBLIC")
print("Conectado:", smoke_df.collect()[0].asDict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar Schema e Tabelas Placeholder

# COMMAND ----------

# Todas as tabelas SCI — criadas com schema flexível (VARIANT)
# Na carga real, usaremos CREATE OR REPLACE TABLE ... AS SELECT para definir schema a partir dos parquets
TABLES = [
    # TC_Principal BUD
    "SCI_TC_PRINCIPAL_BUD",
    "SCI_TC_PRINCIPAL_VOL_BUD",
    "SCI_TC_PRINCIPAL_VOL_FA_BUD",
    "SCI_TC_PRINCIPAL_TEMPO_BUD",
    "SCI_TC_PRINCIPAL_DEA_BUD",
    "SCI_TC_PRINCIPAL_CPU_BUD",
    "SCI_TC_PRINCIPAL_CUSTO_FP_BUD",
    "SCI_TC_PRINCIPAL_RATEIO_BUD",
    "SCI_TC_PRINCIPAL_VOL_ACTUAL",
    "SCI_TC_PRINCIPAL_FP_SEM_DA_BUD",
    "SCI_TC_PRINCIPAL_CUSTO_RATEADO_BUD",
    # TC_Principal Real
    "SCI_TC_PRINCIPAL_REAL",
    "SCI_TC_PRINCIPAL_VOL_REAL",
    "SCI_TC_PRINCIPAL_VOL_FA_REAL",
    "SCI_TC_PRINCIPAL_TEMPO_REAL",
    "SCI_TC_SAPIENS",
    "SCI_TC_COMPARATIVO",
    "SCI_TC_PRINCIPAL_CPU_REAL",
    "SCI_TC_PRINCIPAL_CUSTO_FP_REAL",
    "SCI_TC_PRINCIPAL_RATEIO_REAL",
    "SCI_TC_PRINCIPAL_DEA_REAL",
    # TC_Ext
    "SCI_TC_EXT_REAL",
    "SCI_TC_EXT_VOL_REAL",
    "SCI_TC_EXT_BUD",
    "SCI_TC_EXT_VOL_BUD",
    # Histórico Consolidado
    "SCI_HIST_PRINCIPAL_REAL",
    "SCI_HIST_VOL_REAL",
    "SCI_HIST_PRINCIPAL_BUD",
    "SCI_HIST_VOL_BUD",
    "SCI_HIST_CUSTO_FP_REAL",
    "SCI_HIST_CUSTO_FP_BUD",
    # Forecast
    "SCI_FORECAST_COMPLETO",
    "SCI_FORECAST_VOL",
    "SCI_FORECAST_CUSTO_FP",
]

bootstrap_schema_objects(spark, TABLES, target_schema="SCI_CURATED")
print(f"\n{len(TABLES)} tabelas criadas/verificadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar

# COMMAND ----------

tables_df = (
    spark.read.format("snowflake")
    .options(**get_snowflake_spark_options(schema="SCI_CURATED"))
    .option("query", "SHOW TABLES IN SCHEMA DB_BL_FIN.SCI_CURATED")
    .load()
)

print(f"Tabelas no schema SCI_CURATED: {tables_df.count()}")
for row in tables_df.collect():
    row_dict = row.asDict(recursive=True)
    table_name = row_dict.get("name") or row_dict.get("NAME") or str(row_dict)
    print(f"  - {table_name}")
