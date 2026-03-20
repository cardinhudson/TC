# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — Processar e Carregar 100% na Nuvem
# MAGIC
# MAGIC Executa a extração Budget/Real diretamente no Databricks, gravando os
# MAGIC parquets em storage cloud e depois carregando tudo no Snowflake.
# MAGIC
# MAGIC Requisitos:
# MAGIC 1. O repositório deve estar disponível no Workspace local.
# MAGIC 2. O Excel deve estar em storage cloud, no caminho:
# MAGIC    DATA_ROOT/TC_Principal/<ANO>/Reporting veículos.xlsx
# MAGIC 3. O cluster precisa ter o conector Spark-Snowflake com OAuth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros

# COMMAND ----------

import os
import sys
from pathlib import Path
from pyspark.sql.functions import lit

dbutils.widgets.text("REPO_ROOT", "/Workspace/Users/u235107@inetpsa.com/Drafts/sci", "Repo root no Databricks")
dbutils.widgets.text("DATA_ROOT", "", "Raiz cloud dos dados (opcional)")
dbutils.widgets.text("ANO", "2026", "Ano")
dbutils.widgets.dropdown("RUN_BUDGET", "true", ["true", "false"], "Executar Budget")
dbutils.widgets.dropdown("RUN_REAL", "true", ["true", "false"], "Executar Real")
dbutils.widgets.dropdown("RUN_LOAD", "true", ["true", "false"], "Carregar Snowflake")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))
RUN_BUDGET = dbutils.widgets.get("RUN_BUDGET") == "true"
RUN_REAL = dbutils.widgets.get("RUN_REAL") == "true"
RUN_LOAD = dbutils.widgets.get("RUN_LOAD") == "true"

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tc_core.utils.portabilidade import check_excel_exists, resolve_data_root
from tc_core.utils.snowflake_spark import build_delete_preactions_for_years, write_snowflake_dataframe

DATA_ROOT = str(resolve_data_root(DATA_ROOT_INPUT, log=print))

os.environ["SCI_SHARED_DATA_ROOT"] = DATA_ROOT
os.environ["SCI_CLOUD"] = "1"

print(f"REPO_ROOT: {REPO_ROOT}")
print(f"DATA_ROOT: {DATA_ROOT}")
print(f"ANO: {ANO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validar arquivos de entrada na nuvem

# COMMAND ----------

excel_path = check_excel_exists(ANO, DATA_ROOT, log=print)
print(f"Excel encontrado: {excel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar extração na nuvem

# COMMAND ----------

from processamento_dados_veiculos_BUD import processar_veiculos_budget
from processamento_dados_veiculos import processar_veiculos_real

if RUN_BUDGET:
    print("\n=== PROCESSAMENTO BUDGET ===")
    budget_result = processar_veiculos_budget(ANO)
    print("Budget concluído ✅")
else:
    budget_result = None
    print("Budget ignorado")

if RUN_REAL:
    print("\n=== PROCESSAMENTO REAL ===")
    real_result = processar_veiculos_real(ANO)
    print("Real concluído ✅")
else:
    real_result = None
    print("Real ignorado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga Snowflake

# COMMAND ----------

if not RUN_LOAD:
    print("Carga Snowflake ignorada")
else:
    PARQUET_TO_TABLE = {
        "TC_Principal/{ano}/BUD/df_principal_BUD.parquet": "SCI_TC_PRINCIPAL_BUD",
        "TC_Principal/{ano}/BUD/df_vol_veiculos_BUD.parquet": "SCI_TC_PRINCIPAL_VOL_BUD",
        "TC_Principal/{ano}/BUD/df_volume_fa_BUD.parquet": "SCI_TC_PRINCIPAL_VOL_FA_BUD",
        "TC_Principal/{ano}/BUD/df_tempo_veiculos_BUD.parquet": "SCI_TC_PRINCIPAL_TEMPO_BUD",
        "TC_Principal/{ano}/BUD/df_dea_dedicado_BUD.parquet": "SCI_TC_PRINCIPAL_DEA_BUD",
        "TC_Principal/{ano}/BUD/df_veiculos_cpu_BUD.parquet": "SCI_TC_PRINCIPAL_CPU_BUD",
        "TC_Principal/{ano}/BUD/df_veiculos_custo_fp_BUD.parquet": "SCI_TC_PRINCIPAL_CUSTO_FP_BUD",
        "TC_Principal/{ano}/BUD/df_veiculos_percentual_rateio_BUD.parquet": "SCI_TC_PRINCIPAL_RATEIO_BUD",
        "TC_Principal/{ano}/BUD/df_vol_veiculos_actual.parquet": "SCI_TC_PRINCIPAL_VOL_ACTUAL",
        "TC_Principal/{ano}/BUD/df_veiculos_fp_sem_da_BUD.parquet": "SCI_TC_PRINCIPAL_FP_SEM_DA_BUD",
        "TC_Principal/{ano}/BUD/df_veiculos_custo_rateado_BUD.parquet": "SCI_TC_PRINCIPAL_CUSTO_RATEADO_BUD",
        "TC_Principal/{ano}/df_principal.parquet": "SCI_TC_PRINCIPAL_REAL",
        "TC_Principal/{ano}/df_vol_veiculos.parquet": "SCI_TC_PRINCIPAL_VOL_REAL",
        "TC_Principal/{ano}/df_volume_fa.parquet": "SCI_TC_PRINCIPAL_VOL_FA_REAL",
        "TC_Principal/{ano}/df_tempo_veiculos.parquet": "SCI_TC_PRINCIPAL_TEMPO_REAL",
        "TC_Principal/{ano}/df_tc_sapiens.parquet": "SCI_TC_SAPIENS",
        "TC_Principal/{ano}/df_comparativo_real_budget.parquet": "SCI_TC_COMPARATIVO",
        "TC_Principal/{ano}/df_veiculos_cpu.parquet": "SCI_TC_PRINCIPAL_CPU_REAL",
        "TC_Principal/{ano}/df_veiculos_custo_fp.parquet": "SCI_TC_PRINCIPAL_CUSTO_FP_REAL",
        "TC_Principal/{ano}/df_veiculos_percentual_rateio.parquet": "SCI_TC_PRINCIPAL_RATEIO_REAL",
        "TC_Principal/{ano}/df_dea_dedicado.parquet": "SCI_TC_PRINCIPAL_DEA_REAL",
        "TC_Ext/{ano}/df_final.parquet": "SCI_TC_EXT_REAL",
        "TC_Ext/{ano}/df_vol.parquet": "SCI_TC_EXT_VOL_REAL",
        "TC_Ext/{ano}/BUD/df_final_BUD.parquet": "SCI_TC_EXT_BUD",
        "TC_Ext/{ano}/BUD/df_vol_BUD.parquet": "SCI_TC_EXT_VOL_BUD",
        "TC_Principal/historico_consolidado/df_principal_historico.parquet": "SCI_HIST_PRINCIPAL_REAL",
        "TC_Principal/historico_consolidado/df_vol_historico.parquet": "SCI_HIST_VOL_REAL",
        "TC_Principal/historico_consolidado/BUD/df_principal_historico_BUD.parquet": "SCI_HIST_PRINCIPAL_BUD",
        "TC_Principal/historico_consolidado/BUD/df_vol_historico_BUD.parquet": "SCI_HIST_VOL_BUD",
        "TC_Principal/historico_consolidado/df_veiculos_custo_fp_historico.parquet": "SCI_HIST_CUSTO_FP_REAL",
        "TC_Principal/historico_consolidado/BUD/df_veiculos_custo_fp_historico_BUD.parquet": "SCI_HIST_CUSTO_FP_BUD",
        "TC_Principal/Forecast/forecast_completo.parquet": "SCI_FORECAST_COMPLETO",
        "TC_Principal/Forecast/df_vol_historico.parquet": "SCI_FORECAST_VOL",
        "TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet": "SCI_FORECAST_CUSTO_FP",
    }

    ok_count = 0
    err_count = 0
    skip_count = 0

    for pattern, table_name in PARQUET_TO_TABLE.items():
        path = Path(DATA_ROOT) / pattern.replace("{ano}", str(ANO))
        if not path.exists():
            skip_count += 1
            continue

        try:
            df = spark.read.parquet(str(path))
            if "{ano}" in pattern and "Ano" not in df.columns:
                df = df.withColumn("Ano", lit(int(ANO)))

            if "{ano}" in pattern:
                write_snowflake_dataframe(
                    df,
                    table_name,
                    schema="SCI_CURATED",
                    mode="append",
                    preactions=build_delete_preactions_for_years(table_name, [ANO]),
                )
            else:
                write_snowflake_dataframe(df, table_name, schema="SCI_CURATED", mode="overwrite")

            print(f"✅ {table_name}")
            ok_count += 1
        except Exception as exc:
            print(f"❌ {table_name}: {exc}")
            err_count += 1

    print(f"Resultado carga: {ok_count} OK | {err_count} ERRO | {skip_count} SKIP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultado

# COMMAND ----------

print("Pipeline cloud concluída ✅")