# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 00 Validar Ambiente Databricks
# MAGIC
# MAGIC Valida a execução 100% Databricks usando apenas Workspace Files.

# COMMAND ----------

import importlib
import os
import subprocess
import sys
from pathlib import Path

dbutils = globals().get("dbutils")


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

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))

for entry in (REPO_ROOT, f"{REPO_ROOT}/src"):
    if entry not in sys.path:
        sys.path.insert(0, entry)

backend_module = importlib.import_module("sci_core.backend")
resolve_data_root = backend_module.resolve_data_root

os.environ["SCI_DATA_BACKEND"] = "databricks"
DATA_ROOT = str(
    resolve_data_root(DATA_ROOT_INPUT, repo_root=REPO_ROOT, log=print)
)
os.environ["SCI_SHARED_DATA_ROOT"] = DATA_ROOT

_ensure_openpyxl()

repo_path = Path(REPO_ROOT)
excel_candidates = [
    Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "Reporting veículos.xlsx",
    Path(DATA_ROOT) / "TC_Principal" / str(ANO) / "Reporting veiculos.xlsx",
]
excel_path = next((path for path in excel_candidates if path.exists()), None)

print(f"[INFO] REPO_ROOT = {repo_path}")
print(f"[INFO] DATA_ROOT = {DATA_ROOT}")
print(f"[INFO] Backend = {os.environ['SCI_DATA_BACKEND']}")

if not repo_path.exists():
    raise FileNotFoundError(f"REPO_ROOT não encontrado: {repo_path}")

if excel_path is None:
    raise FileNotFoundError(
        "Excel não encontrado. Esperado em: "
        + ", ".join(str(path) for path in excel_candidates)
    )

print(f"[INFO] Excel localizado: {excel_path}")
print("[INFO] Listagem da pasta do ano:")
for child in sorted((Path(DATA_ROOT) / "TC_Principal" / str(ANO)).iterdir()):
    print(f"  - {child.name}")

for package_name in ("pandas", "pyarrow", "openpyxl"):
    module = importlib.import_module(package_name)
    version = getattr(module, "__version__", "n/a")
    print(f"[INFO] Import OK: {package_name} {version}")

print(
    "[INFO] LOG FINAL | EXCEL_OK=True | "
    f"BACKEND={os.environ['SCI_DATA_BACKEND']} | DATA_ROOT={DATA_ROOT}"
)
