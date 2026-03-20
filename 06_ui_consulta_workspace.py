# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — UI Simples de Consulta

# COMMAND ----------

import importlib
import subprocess
import sys
from pathlib import Path

import pandas as pd

dbutils = globals().get("dbutils")
display = globals().get("display")


def _ensure_openpyxl() -> None:
    try:
        importlib.import_module("openpyxl")
        return
    except ModuleNotFoundError:
        print(
            "[WARN] openpyxl nao instalado. "
            "Instalando no ambiente do cluster..."
        )

    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "openpyxl>=3.1,<4"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Falha ao instalar openpyxl no cluster. "
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )

    importlib.invalidate_caches()
    module = importlib.import_module("openpyxl")
    print(f"[INFO] openpyxl pronto: {getattr(module, '__version__', 'n/a')}")



dbutils.widgets.text(
    "REPO_ROOT",
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci",
    "Repo root",
)
dbutils.widgets.text("DATA_ROOT", "", "Data root opcional")
dbutils.widgets.text("ANO", "2026", "Ano")
dbutils.widgets.dropdown(
    "DATASET",
    "real_principal",
    [
        "budget_principal",
        "budget_volume",
        "budget_custo_fp",
        "real_principal",
        "real_volume",
        "real_custo_fp",
        "excel_input",
    ],
    "Dataset",
)
dbutils.widgets.text("LIMIT", "20", "Linhas")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT = (
    dbutils.widgets.get("DATA_ROOT").strip()
    or f"{REPO_ROOT}/dados"
)
ANO = int(dbutils.widgets.get("ANO") or "2026")
DATASET = dbutils.widgets.get("DATASET")
LIMIT = int(dbutils.widgets.get("LIMIT") or "20")

dataset_map = {
    "budget_principal": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "BUD" / "df_principal_BUD.parquet",
    "budget_volume": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "BUD" / "df_vol_veiculos_BUD.parquet",
    "budget_custo_fp": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "BUD" / "df_veiculos_custo_fp_BUD.parquet",
    "real_principal": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "df_principal.parquet",
    "real_volume": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "df_vol_veiculos.parquet",
    "real_custo_fp": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "df_veiculos_custo_fp.parquet",
    "excel_input": Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "Reporting veículos.xlsx",
}

dataset_path = dataset_map[DATASET]

print(f"[INFO] DATA_ROOT={DATA_ROOT}")
print(f"[INFO] ANO={ANO}")
print(f"[INFO] DATASET={DATASET}")
print(f"[INFO] PATH={dataset_path}")
print(f"[INFO] EXISTS={dataset_path.exists()}")

if DATASET == "excel_input":
    _ensure_openpyxl()
    if not dataset_path.exists():
        alt = dataset_path.with_name("Reporting veiculos.xlsx")
        print(f"[INFO] ALT_PATH={alt}")
        print(f"[INFO] ALT_EXISTS={alt.exists()}")
        if alt.exists():
            dataset_path = alt
    if not dataset_path.exists():
        raise FileNotFoundError(
            "Excel nao encontrado. Caminhos testados: "
            f"{dataset_map['excel_input']} | {dataset_map['excel_input'].with_name('Reporting veiculos.xlsx')}"
        )
    xl = pd.ExcelFile(dataset_path)
    info_df = pd.DataFrame({"sheet_name": xl.sheet_names})
    print(f"[INFO] ABAS={xl.sheet_names}")
    if display is not None:
        display(info_df)
    else:
        print(info_df)
else:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Parquet nao encontrado: {dataset_path}")
    df = pd.read_parquet(dataset_path, engine="pyarrow")

    print(f"[INFO] LINHAS={len(df)}")
    print(f"[INFO] COLUNAS={list(df.columns)}")

    if "Ano" in df.columns:
        anos = sorted(pd.Series(df["Ano"]).dropna().astype(int).unique().tolist())
        print(f"[INFO] ANOS={anos}")

    if display is not None:
        display(df.head(LIMIT))
    else:
        print(df.head(LIMIT))
