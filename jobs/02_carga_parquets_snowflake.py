# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — Carga de Parquets → Snowflake
# MAGIC
# MAGIC Lê os arquivos `.parquet` gerados pela própria extração e grava nas tabelas
# MAGIC do schema `DB_BL_FIN.SCI_CURATED` no Snowflake.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC 1. Executar `01_criar_schema.py` pelo menos uma vez.
# MAGIC 2. Executar a extração apontando `SCI_SHARED_DATA_ROOT` para a mesma pasta usada aqui.
# MAGIC 3. O cluster precisa ter o conector Spark-Snowflake disponível com OAuth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import os
from pathlib import Path
from pyspark.sql.functions import lit

from tc_core.utils.portabilidade import resolve_data_root
from tc_core.utils.snowflake_spark import (
    build_delete_preactions_for_years,
    get_snowflake_spark_options,
    write_snowflake_dataframe,
)

# === EDITE AQUI ===
# Pasta base onde a extração grava os parquets.
# Se SCI_SHARED_DATA_ROOT vier vazio, tenta Volumes e cai automaticamente para DBFS.
DATA_ROOT = str(resolve_data_root(os.environ.get("SCI_SHARED_DATA_ROOT", ""), log=print))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mapeamento parquet → tabela Snowflake

# COMMAND ----------

# Mapeamento: (caminho relativo dentro de DATA_ROOT) → (nome tabela Snowflake)
# A pasta deve conter a estrutura:
#   TC_Principal/2025/BUD/df_principal_BUD.parquet
#   TC_Principal/2025/df_principal.parquet
#   TC_Ext/2025/BUD/df_final_BUD.parquet
#   etc.

PARQUET_TO_TABLE = {
    # TC_Principal BUD
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
    # TC_Principal Real
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
    # TC_Ext
    "TC_Ext/{ano}/df_final.parquet": "SCI_TC_EXT_REAL",
    "TC_Ext/{ano}/df_vol.parquet": "SCI_TC_EXT_VOL_REAL",
    "TC_Ext/{ano}/BUD/df_final_BUD.parquet": "SCI_TC_EXT_BUD",
    "TC_Ext/{ano}/BUD/df_vol_BUD.parquet": "SCI_TC_EXT_VOL_BUD",
    # Histórico Consolidado
    "TC_Principal/historico_consolidado/df_principal_historico.parquet": "SCI_HIST_PRINCIPAL_REAL",
    "TC_Principal/historico_consolidado/df_vol_historico.parquet": "SCI_HIST_VOL_REAL",
    "TC_Principal/historico_consolidado/BUD/df_principal_historico_BUD.parquet": "SCI_HIST_PRINCIPAL_BUD",
    "TC_Principal/historico_consolidado/BUD/df_vol_historico_BUD.parquet": "SCI_HIST_VOL_BUD",
    "TC_Principal/historico_consolidado/df_veiculos_custo_fp_historico.parquet": "SCI_HIST_CUSTO_FP_REAL",
    "TC_Principal/historico_consolidado/BUD/df_veiculos_custo_fp_historico_BUD.parquet": "SCI_HIST_CUSTO_FP_BUD",
    # Forecast
    "TC_Principal/Forecast/forecast_completo.parquet": "SCI_FORECAST_COMPLETO",
    "TC_Principal/Forecast/df_vol_historico.parquet": "SCI_FORECAST_VOL",
    "TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet": "SCI_FORECAST_CUSTO_FP",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Descobrir anos disponíveis na pasta gerada pela extração

# COMMAND ----------

def discover_anos(base_path, domain="TC_Principal"):
    """Descobre anos com dados processados na raiz de dados."""
    domain_path = Path(base_path) / domain
    anos = []
    if domain_path.exists():
        for child in domain_path.iterdir():
            if child.is_dir() and child.name.isdigit():
                anos.append(int(child.name))
    return sorted(anos)

anos_disponiveis = discover_anos(DATA_ROOT)
print(f"DATA_ROOT: {DATA_ROOT}")
print(f"Anos encontrados: {anos_disponiveis}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carregar Parquets → Snowflake (via Spark-Snowflake)

# COMMAND ----------

print("Conector Spark-Snowflake será usado com authentication=oauth ✅")

# COMMAND ----------

ok_count = 0
err_count = 0
skip_count = 0

for pattern, table_name in PARQUET_TO_TABLE.items():
    if "{ano}" in pattern:
        # Iterar sobre cada ano
        for ano in anos_disponiveis:
            path = Path(DATA_ROOT) / pattern.replace("{ano}", str(ano))
            if not path.exists():
                skip_count += 1
                continue
            try:
                df = spark.read.parquet(str(path))
                if "Ano" not in df.columns:
                    df = df.withColumn("Ano", lit(int(ano)))
                write_snowflake_dataframe(
                    df,
                    table_name,
                    schema="SCI_CURATED",
                    mode="append",
                    preactions=build_delete_preactions_for_years(table_name, [ano]),
                )
                print(f"  ✅ {table_name} ano={ano}")
                ok_count += 1
            except Exception as e:
                print(f"  ❌ {table_name} ano={ano}: {e}")
                err_count += 1
    else:
        # Tabela sem {ano} (histórico, forecast) → carga completa
        path = Path(DATA_ROOT) / pattern
        if not path.exists():
            skip_count += 1
            continue
        try:
            df = spark.read.parquet(str(path))
            write_snowflake_dataframe(df, table_name, schema="SCI_CURATED", mode="overwrite")
            print(f"  ✅ {table_name}")
            ok_count += 1
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
            err_count += 1

print(f"\n{'='*50}")
print(f"Resultado: {ok_count} OK | {err_count} ERRO | {skip_count} SKIP (não encontrado)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação

# COMMAND ----------

for table_name in sorted(set(PARQUET_TO_TABLE.values())):
    try:
        count_df = (
            spark.read.format("snowflake")
            .options(**get_snowflake_spark_options(schema="SCI_CURATED"))
            .option("query", f'SELECT COUNT(*) AS CNT FROM DB_BL_FIN.SCI_CURATED."{table_name}"')
            .load()
        )
        cnt = count_df.collect()[0]["CNT"]
        status = "✅" if cnt > 0 else "⚠️ VAZIA"
        print(f"  {status} {table_name}: {cnt}")
    except Exception as e:
        print(f"  ❌ {table_name}: {e}")
