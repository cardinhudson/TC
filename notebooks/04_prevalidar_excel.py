# Databricks notebook source
# MAGIC %md
# MAGIC # SCI — 04 Pré-validar Excel

# COMMAND ----------

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

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
dbutils.widgets.text("TIPO_EXTRACAO", "🔄 Ambos", "Tipo extracao")
dbutils.widgets.text("DATASET_KEY", "TC_Principal", "Dataset key")

REPO_ROOT = dbutils.widgets.get("REPO_ROOT").strip() or \
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DATA_ROOT_INPUT = dbutils.widgets.get("DATA_ROOT").strip()
ANO = int(dbutils.widgets.get("ANO"))
TIPO_EXTRACAO = dbutils.widgets.get("TIPO_EXTRACAO").strip() or "🔄 Ambos"
DATASET_KEY = dbutils.widgets.get("DATASET_KEY").strip() or "TC_Principal"

DATA_ROOT = Path(DATA_ROOT_INPUT or f"{REPO_ROOT}/dados")

_ensure_openpyxl()

# Garantir que src/ esteja acessível para sci_core
for _entry in [REPO_ROOT, f"{REPO_ROOT}/src"]:
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

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


def _emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    dbutils.notebook.exit(json.dumps(payload, ensure_ascii=False))


def _validar_abas(caminho: Path, abas_obrigatorias: list[str]) -> tuple[bool, list[str]]:
    try:
        xl = pd.ExcelFile(caminho)
        abas = xl.sheet_names
    except Exception as exc:
        return False, [f"❌ Não foi possível abrir o Excel: {exc}"]

    faltando = [aba for aba in abas_obrigatorias if aba not in abas]
    if faltando:
        return False, [
            f"❌ Abas faltando: {faltando}",
            f"   Abas disponíveis: {abas}",
        ]
    return True, [f"✅ Abas OK: {abas_obrigatorias}"]


def _validar_budget(caminho: Path) -> tuple[bool, list[str]]:
    msgs = []
    ok_abas, abas_msgs = _validar_abas(
        caminho,
        [
            "massa primária - BDG",
            "massa - REDIS",
            "Volume e EST PdR - BDG",
            "Volume BDG",
            "Volume Actual",
            "EST veículos - BDG",
            "massa - D&A dedicado",
        ],
    )
    msgs.extend(abas_msgs)
    return ok_abas, msgs


def _validar_real(caminho: Path) -> tuple[bool, list[str]]:
    msgs = []
    ok_abas, abas_msgs = _validar_abas(
        caminho,
        [
            "Sapiens",
            "massa - REDIS",
            "Volume e EST PdR - Actual",
            "Volume Actual",
            "EST veículos - Actual",
        ],
    )
    msgs.extend(abas_msgs)
    return ok_abas, msgs


try:
    caminho_excel = Path(resolve_excel_path(ANO, str(DATA_ROOT), dataset_key=DATASET_KEY))
except FileNotFoundError as exc:
    if DATASET_KEY == "TC_Ext":
        # TC_Ext usa Dados SAPIENS.xlsx + Reporting fluxo anexo.xlsx, não Reporting veículos.xlsx
        _ext_root = os.path.join(str(DATA_ROOT), "TC_Ext", str(ANO))
        caminho_sapiens_ext = Path(_ext_root) / "Dados SAPIENS.xlsx"
        caminho_rateio_ext = Path(_ext_root) / "Reporting fluxo anexo.xlsx"
        arquivos_faltando = []
        if not caminho_sapiens_ext.exists():
            arquivos_faltando.append(str(caminho_sapiens_ext))
        if not caminho_rateio_ext.exists():
            arquivos_faltando.append(str(caminho_rateio_ext))
        if arquivos_faltando:
            _emit({
                "ok": False,
                "messages": [
                    f"❌ Arquivos TC_Ext não encontrados em {_ext_root}:",
                    *[f"  - {p}" for p in arquivos_faltando],
                ],
            })
        # Pré-validação TC_Ext: verificar abas obrigatórias
        mensagens_ext = [
            f"📄 Dataset: TC_Ext | ANO: {ANO}",
            f"📂 Raiz: {_ext_root}",
        ]
        ok_ext = True
        if caminho_sapiens_ext.exists():
            ok_s, msgs_s = _validar_abas(caminho_sapiens_ext, ["Base conso"])
            ok_ext &= ok_s
            mensagens_ext.append("─── Dados SAPIENS.xlsx ───")
            mensagens_ext.extend(msgs_s)
        if caminho_rateio_ext.exists():
            ok_r_real, msgs_r_real = _validar_abas(caminho_rateio_ext, ["Sapiens", "Rateio", "Volume"])
            ok_r_bud, msgs_r_bud = _validar_abas(caminho_rateio_ext, ["Voz de custo BDG", "Rateio BDG", "Volume BDG"])
            ok_ext &= ok_r_real
            ok_ext &= ok_r_bud
            mensagens_ext.append("─── Reporting fluxo anexo.xlsx (REAL) ───")
            mensagens_ext.extend(msgs_r_real)
            mensagens_ext.append("─── Reporting fluxo anexo.xlsx (BUD) ───")
            mensagens_ext.extend(msgs_r_bud)
        _emit({"ok": ok_ext, "messages": mensagens_ext})
    else:
        _emit({
            "ok": False,
            "messages": [
                "❌ Excel não encontrado.",
                str(exc),
            ],
        })

mensagens = [f"📄 Excel encontrado: {caminho_excel}"]
ok_total = True

if TIPO_EXTRACAO in ["📊 Dados REAIS", "🔄 Ambos"]:
    ok_real, msgs_real = _validar_real(caminho_excel)
    ok_total &= ok_real
    mensagens.append("─── 📊 REAIS ───")
    mensagens.extend(msgs_real)

if TIPO_EXTRACAO in ["💰 Dados BUDGET", "🔄 Ambos"]:
    ok_bud, msgs_bud = _validar_budget(caminho_excel)
    ok_total &= ok_bud
    mensagens.append("─── 💰 BUDGET ───")
    mensagens.extend(msgs_bud)

_emit({"ok": ok_total, "messages": mensagens})
