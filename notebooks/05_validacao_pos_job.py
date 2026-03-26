# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 05 Validação Pós-Job
# MAGIC
# MAGIC Verifica se os parquets foram gerados corretamente pelos scripts de processamento.
# MAGIC Se algum check falhar, levanta RuntimeError para que o Databricks marque a task como FAILED.

# COMMAND ----------

import os
import sys
from pathlib import Path

import pandas as pd

dbutils = globals().get("dbutils")

dbutils.widgets.text(
    "REPO_ROOT",
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci",
    "Repo root",
)
dbutils.widgets.text("DATA_ROOT", "", "Data root opcional")
dbutils.widgets.text("ANO", "2026", "Ano")
dbutils.widgets.text("DATASET_KEY", "TC_Principal", "Dataset key")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))
DATASET_KEY = dbutils.widgets.get("DATASET_KEY").strip() or "TC_Principal"

DATA_ROOT = Path(DATA_ROOT_INPUT or f"{REPO_ROOT}/dados")

print(f"[INFO] DATA_ROOT   = {DATA_ROOT}")
print(f"[INFO] ANO         = {ANO}")
print(f"[INFO] DATASET_KEY = {DATASET_KEY}")

if DATASET_KEY == "TC_Ext":
    # ── Parquets TC_Ext obrigatórios após o processamento ──
    parquets_budget = [
        DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_BUD.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_vol_BUD.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_thin_BUD.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_agg_BUD.parquet",
    ]
    parquets_real = [
        DATA_ROOT / "TC_Ext" / str(ANO) / "df_final.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "df_vol.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "df_final_thin.parquet",
        DATA_ROOT / "TC_Ext" / str(ANO) / "df_final_agg.parquet",
    ]
    parquets_historico = [
        DATA_ROOT / "TC_Ext" / "historico_consolidado" / "df_final_historico.parquet",
        DATA_ROOT / "TC_Ext" / "historico_consolidado" / "df_final_historico_thin.parquet",
        DATA_ROOT / "TC_Ext" / "historico_consolidado" / "df_vol_historico.parquet",
        DATA_ROOT / "TC_Ext" / "historico_consolidado" / "BUD" / "df_final_historico_BUD.parquet",
        DATA_ROOT / "TC_Ext" / "historico_consolidado" / "BUD" / "df_vol_historico_BUD.parquet",
    ]
else:
    # ── Parquets TC_Principal obrigatórios após o processamento ──
    parquets_budget = [
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_BUD.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_thin_BUD.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_agg_home_BUD.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_vol_veiculos_BUD.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_veiculos_custo_fp_BUD.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_veiculos_agg_home_BUD.parquet",
    ]
    parquets_real = [
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal_thin.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal_agg_home.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_vol_veiculos.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_veiculos_custo_fp.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_veiculos_agg_home.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_tc_sapiens.parquet",
        DATA_ROOT / "TC_Principal" / str(ANO) / "df_tc_sapiens_thin.parquet",
    ]
    parquets_historico = []

resultados = []
erros = []

def checar(path: Path, label: str) -> None:
    if not path.exists():
        msg = f"AUSENTE: {label}"
        resultados.append({"Arquivo": label, "Existe": False, "Linhas": 0, "Status": "ERRO", "Mensagem": "arquivo não encontrado"})
        erros.append(msg)
        print(f"  ❌ {msg}")
        return
    try:
        df = pd.read_parquet(path)
        linhas = len(df)
        anos_presentes = []
        if "Ano" in df.columns:
            anos_presentes = sorted(pd.Series(df["Ano"]).dropna().astype(int).unique().tolist())
        ano_ok = (not anos_presentes) or (ANO in anos_presentes)
        if linhas == 0:
            msg = f"VAZIO: {label}"
            resultados.append({"Arquivo": label, "Existe": True, "Linhas": 0, "Status": "ERRO", "Mensagem": "parquet sem linhas"})
            erros.append(msg)
            print(f"  ❌ {msg}")
        elif not ano_ok:
            msg = f"ANO {ANO} ausente em {label} (anos={anos_presentes})"
            resultados.append({"Arquivo": label, "Existe": True, "Linhas": linhas, "Status": "ERRO", "Mensagem": msg})
            erros.append(msg)
            print(f"  ❌ {msg}")
        else:
            status_msg = f"{linhas:,} linhas" + (f" | anos={anos_presentes}" if anos_presentes else "")
            resultados.append({"Arquivo": label, "Existe": True, "Linhas": linhas, "Status": "OK", "Mensagem": status_msg})
            print(f"  ✅ {label}: {status_msg}")
    except Exception as exc:
        msg = f"ERRO ao ler {label}: {exc}"
        resultados.append({"Arquivo": label, "Existe": True, "Linhas": 0, "Status": "ERRO", "Mensagem": str(exc)})
        erros.append(msg)
        print(f"  ❌ {msg}")

print("\n[CHECK] Parquets Budget:")
for p in parquets_budget:
    checar(p, f"BUD/{p.name}")

print("\n[CHECK] Parquets Real:")
for p in parquets_real:
    checar(p, f"Real/{p.name}")

if parquets_historico:
    print("\n[CHECK] Histórico consolidado:")
    for p in parquets_historico:
        rel = str(p).replace(str(DATA_ROOT), "").lstrip("/\\")
        checar(p, rel)

resumo = pd.DataFrame(resultados)
print("\n[INFO] RESUMO:")
print(resumo.to_string(index=False))

n_ok = int((resumo["Status"] == "OK").sum())
n_err = int((resumo["Status"] == "ERRO").sum())
print(f"\n[INFO] RESULTADO: {n_ok} OK | {n_err} ERRO(S)")

if erros:
    mensagem_final = (
        f"Validação pós-job REPROVADA — {n_err} erro(s):\n"
        + "\n".join(f"  - {e}" for e in erros)
    )
    print(f"\n❌ {mensagem_final}")
    raise RuntimeError(mensagem_final)

print("\n✅ Validação pós-job APROVADA — todos os parquets OK")
