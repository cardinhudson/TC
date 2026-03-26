# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 03 Processar e Publicar
# MAGIC
# MAGIC Executa o pipeline de processamento TC Veículos no Databricks.
# MAGIC Usa exatamente os mesmos scripts de processamento do ambiente local,
# MAGIC garantindo paridade total de schema e caminhos de saída.

# COMMAND ----------

import importlib
import os
import subprocess
import sys
from datetime import datetime
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
dbutils.widgets.dropdown(
    "RUN_BUDGET",
    "true",
    ["true", "false"],
    "Executar Budget",
)
dbutils.widgets.dropdown(
    "RUN_REAL",
    "true",
    ["true", "false"],
    "Executar Real",
)
dbutils.widgets.text("DATASET_KEY", "TC_Principal", "Dataset key")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))
RUN_BUDGET = dbutils.widgets.get("RUN_BUDGET") == "true"
RUN_REAL = dbutils.widgets.get("RUN_REAL") == "true"
DATASET_KEY = dbutils.widgets.get("DATASET_KEY").strip() or "TC_Principal"

# ── Resolver DATA_ROOT ──
DATA_ROOT = Path(
    DATA_ROOT_INPUT or f"{REPO_ROOT}/dados"
)

print(f"[INFO] REPO_ROOT = {REPO_ROOT}")
print(f"[INFO] DATA_ROOT = {DATA_ROOT}")
print(f"[INFO] ANO       = {ANO}")
print(f"[INFO] RUN_BUDGET= {RUN_BUDGET}")
print(f"[INFO] RUN_REAL  = {RUN_REAL}")

_ensure_openpyxl()

# ── Expor variável de ambiente para os módulos de processamento ──
os.environ["SCI_SHARED_DATA_ROOT"] = str(DATA_ROOT)

# ── Garantir que os módulos do projeto estejam acessíveis ──
for entry in [REPO_ROOT, f"{REPO_ROOT}/src"]:
    if entry not in sys.path:
        sys.path.insert(0, entry)

# ── Verificar que o Excel de entrada existe ──
try:
    from sci_core.path_utils import resolve_excel_path
except ModuleNotFoundError:
    _TMP_CACHE_ROOT = "/tmp/sci_data_cache"
    _DBFS_DATA_ROOT = "/dbfs/sci_data"
    _EXCEL_CANDIDATES = (
        "Reporting_veiculos.xlsx",
        "Reporting veículos.xlsx",
        "Reporting veiculos.xlsx",
    )

    def resolve_excel_path(ano: int | str, data_root: str | None = None, dataset_key: str = "TC_Principal") -> str:
        ano_str = str(ano)
        effective_root = data_root or os.environ.get(
            "SCI_SHARED_DATA_ROOT",
            str(Path.cwd() / "dados"),
        )

        tested: list[str] = []
        roots = [
            os.path.join(effective_root, dataset_key, ano_str),
            os.path.join(_DBFS_DATA_ROOT, dataset_key, ano_str),
            os.path.join(_TMP_CACHE_ROOT, dataset_key, ano_str),
        ]
        for root in roots:
            for candidate in _EXCEL_CANDIDATES:
                path = os.path.join(root, candidate)
                tested.append(path)
                if os.path.exists(path):
                    if "/tmp" in root:
                        tag = "[FALLBACK /tmp]"
                    elif root.startswith(_DBFS_DATA_ROOT):
                        tag = "[DBFS]"
                    else:
                        tag = "[Workspace]"
                    print(f"[INFO] Excel encontrado {tag}: {path}")
                    return path

        raise FileNotFoundError(
            "Excel de entrada não encontrado. Caminhos testados:\n"
            + "\n".join(f"  - {p}" for p in tested)
            + "\n\nUse o uploader do app para enviar o arquivo."
        )

# ── Resolver DELTA_ROOT ──
DELTA_ROOT = Path(
    os.environ.get("SCI_DELTA_ROOT", f"{REPO_ROOT}/delta")
)

print(f"[INFO] DATASET_KEY = {DATASET_KEY}")
print(f"[INFO] DELTA_ROOT  = {DELTA_ROOT}")

# ── Para TC_Principal: verificar Excel de entrada ──
if DATASET_KEY == "TC_Principal":
    excel_path = Path(resolve_excel_path(ANO, str(DATA_ROOT), dataset_key="TC_Principal"))
else:
    excel_path = None  # TC_Ext usa arquivos diferentes


def _assert_parquets_esperados() -> None:
    faltando = []
    if DATASET_KEY == "TC_Ext":
        if RUN_BUDGET:
            for path in [
                DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_BUD.parquet",
                DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_thin_BUD.parquet",
                DATA_ROOT / "TC_Ext" / str(ANO) / "BUD" / "df_final_agg_BUD.parquet",
                DATA_ROOT / "TC_Ext" / "historico_consolidado" / "BUD" / "df_final_historico_BUD.parquet",
            ]:
                if not path.exists():
                    faltando.append(str(path))

        if RUN_REAL:
            for path in [
                DATA_ROOT / "TC_Ext" / str(ANO) / "df_final.parquet",
                DATA_ROOT / "TC_Ext" / str(ANO) / "df_final_thin.parquet",
                DATA_ROOT / "TC_Ext" / str(ANO) / "df_final_agg.parquet",
                DATA_ROOT / "TC_Ext" / "historico_consolidado" / "df_final_historico.parquet",
                DATA_ROOT / "TC_Ext" / "historico_consolidado" / "df_final_historico_thin.parquet",
            ]:
                if not path.exists():
                    faltando.append(str(path))
    else:
        if RUN_BUDGET:
            for path in [
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_BUD.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_thin_BUD.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_principal_agg_home_BUD.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_vol_veiculos_BUD.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_veiculos_custo_fp_BUD.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "BUD" / "df_veiculos_agg_home_BUD.parquet",
            ]:
                if not path.exists():
                    faltando.append(str(path))

        if RUN_REAL:
            for path in [
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal_thin.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_principal_agg_home.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_vol_veiculos.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_veiculos_custo_fp.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_veiculos_agg_home.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_tc_sapiens.parquet",
                DATA_ROOT / "TC_Principal" / str(ANO) / "df_tc_sapiens_thin.parquet",
            ]:
                if not path.exists():
                    faltando.append(str(path))

    if faltando:
        raise RuntimeError(
            "Processamento não gerou os parquets obrigatórios. Ausentes:\n"
            + "\n".join(f"  - {item}" for item in faltando)
        )

# COMMAND ----------

# MAGIC %md ## Budget

# COMMAND ----------

if DATASET_KEY == "TC_Ext":
    # ── TC_Ext: Processar dados Budget e Real com processamento_dados_BUD.py / processamento_dados.py ──
    if RUN_BUDGET:
        print(f"\n{'='*60}")
        print(f"[INFO] TC_Ext — Iniciando processamento BUDGET — {ANO}")
        print(f"{'='*60}")
        from processamento_dados_BUD import processar_completo_bud
        resultado_bud_ext = processar_completo_bud(ano=ANO)
        print(f"[INFO] TC_Ext Budget concluído — {resultado_bud_ext}")
    else:
        print("[INFO] TC_Ext Budget ignorado (RUN_BUDGET=false)")

    if RUN_REAL:
        print(f"\n{'='*60}")
        print(f"[INFO] TC_Ext — Iniciando processamento REAL — {ANO}")
        print(f"{'='*60}")
        from processamento_dados import processar_completo
        resultado_real_ext = processar_completo(ano=ANO)
        print(f"[INFO] TC_Ext Real concluído — {resultado_real_ext}")
    else:
        print("[INFO] TC_Ext Real ignorado (RUN_REAL=false)")

else:
    # ── TC_Principal: pipeline de veículos (NÃO TOCAR) ──
    if RUN_BUDGET:
        print(f"\n{'='*60}")
        print(f"[INFO] Iniciando processamento BUDGET — {ANO}")
        print(f"{'='*60}")
        from processamento_dados_veiculos_BUD import processar_veiculos_budget
        resultado_bud = processar_veiculos_budget(ano=ANO)
        pasta_bud = DATA_ROOT / "TC_Principal" / str(ANO) / "BUD"
        arquivos_bud = list(pasta_bud.glob("*.parquet")) if pasta_bud.exists() else []
        print(f"[INFO] Budget concluído — {len(arquivos_bud)} parquets em {pasta_bud}")
        for p in sorted(arquivos_bud):
            tam_kb = p.stat().st_size / 1024
            print(f"  ✅ {p.name} ({tam_kb:.0f} KB)")
    else:
        print("[INFO] Budget ignorado (RUN_BUDGET=false)")

# COMMAND ----------

# MAGIC %md ## Real (Sapiens)

# COMMAND ----------

if DATASET_KEY != "TC_Ext":
    # TC_Ext já processou RUN_REAL acima
    if RUN_REAL:
        print(f"\n{'='*60}")
        print(f"[INFO] Iniciando processamento REAL — {ANO}")
        print(f"{'='*60}")
        from processamento_dados_veiculos import processar_veiculos_real
        resultado_real = processar_veiculos_real(ano=ANO)
        pasta_real = DATA_ROOT / "TC_Principal" / str(ANO)
        parquets_real = [p for p in pasta_real.glob("*.parquet")] if pasta_real.exists() else []
        print(f"[INFO] Real concluído — {len(parquets_real)} parquets em {pasta_real}")
        for p in sorted(parquets_real):
            tam_kb = p.stat().st_size / 1024
            print(f"  ✅ {p.name} ({tam_kb:.0f} KB)")
    else:
        print("[INFO] Real ignorado (RUN_REAL=false)")

# COMMAND ----------

# MAGIC %md ## Resumo

# COMMAND ----------

print(f"\n{'='*60}")
print(f"[INFO] RESUMO FINAL — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
print(f"{'='*60}")
print(f"[INFO] DATASET_KEY = {DATASET_KEY}")

_assert_parquets_esperados()

if DATASET_KEY == "TC_Ext":
    pasta_ext_ano = DATA_ROOT / "TC_Ext" / str(ANO)
    pasta_ext_bud = pasta_ext_ano / "BUD"
    pasta_ext_hist = DATA_ROOT / "TC_Ext" / "historico_consolidado"
    total_parquets = 0
    total_bytes = 0
    erros = []

    for subdir, label in [
        (pasta_ext_bud, "TC_Ext/BUD"),
        (pasta_ext_ano, "TC_Ext/Real"),
        (pasta_ext_hist / "BUD", "TC_Ext/hist/BUD"),
        (pasta_ext_hist, "TC_Ext/hist"),
    ]:
        if not subdir.exists():
            print(f"  ⚠️  {label}: pasta não existe")
            continue
        for p in sorted(subdir.glob("*.parquet")):
            sz = p.stat().st_size
            total_parquets += 1
            total_bytes += sz
            try:
                import pandas as pd
                df_tmp = pd.read_parquet(p)
                print(f"  ✅ {label}/{p.name}: {len(df_tmp):,} linhas ({sz/1024:.0f} KB)")
            except Exception as exc:
                erros.append(f"{label}/{p.name}: {exc}")
                print(f"  ❌ {label}/{p.name}: ERRO ao ler — {exc}")
else:
    pasta_ano = DATA_ROOT / "TC_Principal" / str(ANO)
    total_parquets = 0
    total_bytes = 0
    erros = []

    for subdir, label in [(pasta_ano / "BUD", "BUD"), (pasta_ano, "Real/Raiz")]:
        if not subdir.exists():
            print(f"  ⚠️  {label}: pasta não existe")
            continue
        for p in sorted(subdir.glob("*.parquet")):
            sz = p.stat().st_size
            total_parquets += 1
            total_bytes += sz
            try:
                import pandas as pd
                df_tmp = pd.read_parquet(p)
                print(f"  ✅ {label}/{p.name}: {len(df_tmp):,} linhas ({sz/1024:.0f} KB)")
            except Exception as exc:
                erros.append(f"{label}/{p.name}: {exc}")
                print(f"  ❌ {label}/{p.name}: ERRO ao ler — {exc}")

print(f"\n[INFO] Total: {total_parquets} parquets | {total_bytes/1024/1024:.1f} MB")
if erros:
    raise RuntimeError(
        f"Processamento concluído com {len(erros)} erro(s):\n"
        + "\n".join(erros)
    )
print("[INFO] Pipeline concluído com sucesso ✅")
