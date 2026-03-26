"""
tc_core/utils/portabilidade.py
==============================
Utilitários de portabilidade para execução dual (Dev ↔ EXE).

Regras:
  - Dev  (python app.py):  caminhos relativos à raiz do repositório.
  - EXE  (PyInstaller):    TUDO dentro de _internal/ (sys._MEIPASS).
                            Tanto leitura (dados/) quanto escrita (JSONs)
                            operam sobre sys._MEIPASS, que é uma pasta
                            normal e gravável em one-dir.

Uso:
    from tc_core.utils.portabilidade import get_base_path, get_assets_path, IS_FROZEN
"""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Callable, Sequence
from uuid import uuid4

DEFAULT_DATABRICKS_VOLUME_ROOT = Path("/Volumes/hive_metastore/default/sci/dados")
DEFAULT_DATABRICKS_DBFS_ROOT = Path("/dbfs/FileStore/sci/dados")
_EXCEL_FILE_CANDIDATES = ("Reporting veículos.xlsx", "Reporting veiculos.xlsx", "Reporting_veiculos.xlsx")

# ---------------------------------------------------------------------------
# Flag: estamos rodando dentro de um bundle PyInstaller?
# ---------------------------------------------------------------------------
IS_FROZEN: bool = getattr(sys, "frozen", False)


def is_cloud() -> bool:
    """True se executando em ambiente cloud (Databricks Apps / Jobs)."""
    # Databricks define DATABRICKS_RUNTIME_VERSION no runtime
    return bool(
        os.environ.get("DATABRICKS_RUNTIME_VERSION")
        or os.environ.get("SCI_CLOUD", "")
    )


def _normalize_runtime_path(path: str | Path) -> Path:
    """Normaliza paths do runtime Databricks sem alterar paths locais."""
    raw = str(path).strip()
    if not raw:
        return Path(raw)

    if is_cloud():
        if raw.startswith("/Users/"):
            return Path(f"/Workspace{raw}")
        if raw.startswith("Users/"):
            return Path(f"/Workspace/{raw}")
        if raw.startswith("Workspace/Users/"):
            return Path(f"/{raw}")

    if raw.startswith("~"):
        return Path(raw).expanduser()
    return Path(raw)


def _get_env_data_root() -> Path | None:
    """Retorna o diretório de dados compartilhados definido por ambiente, se houver."""
    raw = (sys.environ.get("SCI_SHARED_DATA_ROOT") if hasattr(sys, "environ") else None)
    if not raw:
        import os
        raw = os.environ.get("SCI_SHARED_DATA_ROOT")
    if not raw:
        return None
    return _normalize_runtime_path(raw)


def get_base_path() -> Path:
    """
    Retorna o diretório base para dados, configs e assets.

    - Dev : raiz do repositório (pasta que contém app.py)
    - EXE : sys._MEIPASS (_internal/) — onde o PyInstaller extrai
            TODOS os datas, incluindo dados/, páginas e JSONs.
            A pasta _internal/ é gravável em one-dir, então
            versao.json, dados_equipe.json etc. também funcionam.
    """
    if IS_FROZEN:
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    # __file__ = .../tc_core/utils/portabilidade.py → dois levels acima = raiz
    return Path(__file__).resolve().parents[2]


def get_data_root() -> Path:
    """
    Retorna a pasta base de dados (`dados/`).

    - Padrão: `<base_path>/dados`
    - Override: variável de ambiente `SCI_SHARED_DATA_ROOT`
    """
    env_root = _get_env_data_root()
    if env_root is not None:
        return env_root
    return get_base_path() / "dados"


def get_workspace_upload_root() -> str:
    """Path persistente do Workspace Databricks para uploads. Nunca retorna /tmp.

    Usa SCI_WORKSPACE_DATA_ROOT (env var preservada antes de qualquer fallback para /tmp).
    Fallback: path hardcoded para compatibilidade legada.
    """
    return os.environ.get(
        "SCI_WORKSPACE_DATA_ROOT",
        "/Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados",
    ).rstrip("/")


def _safe_path_exists(path: Path) -> tuple[bool, str]:
    """Retorna (existe, motivo) sem levantar em paths inacessíveis."""
    try:
        return path.exists(), "ok"
    except PermissionError as exc:
        return False, f"sem permissão ({exc})"
    except OSError as exc:
        return False, f"erro de I/O ({exc})"


def probe_write_access(path: str | Path) -> tuple[bool, str]:
    """Verifica se o processo atual consegue gravar no caminho informado."""
    candidate = _normalize_runtime_path(path)

    # Limita a subida: no maximo ate o DATA_ROOT ou 3 niveis acima
    env_root = _get_env_data_root()
    stop_at = env_root.parent if env_root else None
    max_levels = 5
    levels = 0

    current = candidate
    while True:
        exists, reason = _safe_path_exists(current)
        if exists:
            break
        levels += 1
        if current.parent == current or levels > max_levels:
            return False, f"caminho base nao existe: {candidate}"
        if stop_at and current == stop_at:
            return False, f"DATA_ROOT nao acessivel como diretorio: {env_root}"
        current = current.parent

    test_file = current / f".sci_write_test_{uuid4().hex}"
    try:
        with open(test_file, "w", encoding="utf-8") as handle:
            handle.write("ok")
        test_file.unlink(missing_ok=True)
        return True, "ok"
    except PermissionError as exc:
        return False, f"sem permissao de escrita em {current} ({exc})"
    except OSError as exc:
        return False, f"erro de escrita em {current} ({exc})"


def dbfs_uri_to_posix(path: str | Path) -> Path:
    """Converte dbfs:/... para /dbfs/... quando necessário."""
    raw = str(path).strip()
    if raw.lower().startswith("dbfs:/"):
        suffix = raw[5:].lstrip("/")
        return Path("/dbfs") / suffix
    return _normalize_runtime_path(raw)


def posix_to_dbfs_uri(path: str | Path) -> str:
    """Converte /dbfs/... para dbfs:/... quando necessário."""
    raw = str(path).strip()
    if raw.lower().startswith("/dbfs/"):
        return "dbfs:/" + raw[6:]
    return raw


def resolve_data_root(
    preferred: str | Path | None = None,
    *,
    volume_candidates: Sequence[str | Path] | None = None,
    dbfs_fallback: str | Path | None = None,
    log: Callable[[str], None] | None = None,
) -> Path:
    """Resolve a raiz de dados em Databricks com fallback Volumes -> DBFS.

    Ordem:
      1. preferred, se informado e acessível
      2. volume_candidates
      3. dbfs_fallback
    """
    emit = log or (lambda _msg: None)
    candidates: list[Path] = []

    if preferred:
        candidates.append(dbfs_uri_to_posix(preferred))

    for candidate in volume_candidates or (DEFAULT_DATABRICKS_VOLUME_ROOT,):
        candidates.append(dbfs_uri_to_posix(candidate))

    candidates.append(dbfs_uri_to_posix(dbfs_fallback or DEFAULT_DATABRICKS_DBFS_ROOT))

    last_reason = "nenhum candidato testado"
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)

        exists, reason = _safe_path_exists(candidate)
        if exists:
            emit(f"[SCI] DATA_ROOT selecionado: {candidate}")
            return candidate

        last_reason = f"{candidate}: {reason}"
        emit(f"[SCI] DATA_ROOT indisponível: {candidate} | motivo: {reason}")

    raise FileNotFoundError(
        "Nenhuma raiz de dados disponível. "
        f"Último motivo: {last_reason}. "
        "Crie a pasta em /dbfs/FileStore/sci/dados ou libere acesso a /Volumes."
    )


def check_excel_exists(
    ano: int,
    data_root: str | Path | None = None,
    *,
    log: Callable[[str], None] | None = None,
) -> Path:
    """Valida a existência do Excel esperado e retorna o path encontrado."""
    emit = log or (lambda _msg: None)
    root = dbfs_uri_to_posix(data_root) if data_root is not None else resolve_data_root(log=log)
    base_dir = root / "TC_Principal" / str(ano)

    emit(f"[SCI] Validando Excel em: {base_dir}")

    for filename in _EXCEL_FILE_CANDIDATES:
        excel_path = base_dir / filename
        exists, _reason = _safe_path_exists(excel_path)
        if exists:
            emit(f"[SCI] Excel localizado com sucesso: {excel_path}")
            return excel_path

    if str(root).lower().startswith("/dbfs/"):
        emit("[SCI] Excel não encontrado no backend DBFS.")
        emit("[SCI] Faça upload para um destes caminhos:")
        for filename in _EXCEL_FILE_CANDIDATES:
            emit(f"       {base_dir / filename}")
        emit("[SCI] Use dbutils.fs e display com dbfs:/..., e pandas/os com /dbfs/...")
    elif str(root).lower().startswith("/volumes/"):
        emit("[SCI] Excel não encontrado no backend Volumes.")
        emit("[SCI] Faça upload para um destes caminhos:")
        for filename in _EXCEL_FILE_CANDIDATES:
            emit(f"       {base_dir / filename}")

    raise FileNotFoundError(
        "Excel não encontrado. Caminhos testados: "
        + ", ".join(str(base_dir / filename) for filename in _EXCEL_FILE_CANDIDATES)
    )


def is_shared_data_override_active() -> bool:
    """Indica se o app está usando uma raiz de dados compartilhada externa."""
    return _get_env_data_root() is not None


def get_assets_path() -> Path:
    """
    Retorna o diretório de assets estáticos bundled (imagens, .streamlit/).
    Idêntico a get_base_path() — ambos apontam para o mesmo lugar.
    """
    return get_base_path()


def get_output_path(subdir: str = "") -> Path:
    """
    Retorna o diretório para escrita de artefatos (logs, exports).

    - Dev : raiz do repositório
    - EXE : sys._MEIPASS (_internal/) — gravável em one-dir

    Exemplo:
        get_output_path("logs")  →  _internal/logs/
    """
    base = get_base_path()
    if subdir:
        path = base / subdir
        path.mkdir(parents=True, exist_ok=True)
        return path
    return base


def resolve_path(relativo: str) -> Path:
    """
    Converte um caminho relativo (ex: 'dados/TC_Principal') para absoluto,
    usando get_base_path() como âncora.  Funciona igual nos dois modos.

    Uso:
        caminho = resolve_path("dados/TC_Principal/2025/BUD")
    """
    return get_base_path() / relativo


def resolve_data_path(*parts: str) -> Path:
    """Resolve caminhos relativos à pasta de dados efetiva."""
    return get_data_root().joinpath(*parts)


# ---------------------------------------------------------------------------
#  Cloud filesystem helpers — Databricks Workspace API fallback
# ---------------------------------------------------------------------------
#  Quando o Databricks App roda em container isolado, os paths /Workspace/...
#  externos ao diretório da app NÃO são acessíveis via os.path / os.listdir.
#  As funções abaixo tentam primeiro o Databricks SDK (auto-auth do App)
#  e usam a REST API com token apenas como fallback.
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _get_workspace_client():
    """Retorna um WorkspaceClient configurado automaticamente, ou None."""
    import logging as _logging  # noqa: PLC0415
    _log = _logging.getLogger("sci.portabilidade")
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        client = WorkspaceClient()
        # Verificar credenciais rapidamente
        _log.info("[SCI] WorkspaceClient criado com sucesso (host=%s)",
                  getattr(client.config, 'host', '?'))
        return client
    except Exception as exc:
        _log.warning("[SCI] WorkspaceClient falhou: %s: %s", type(exc).__name__, exc)
        return None


def _object_type_name(value: object) -> str:
    """Normaliza enums/strings retornados pelo SDK ou pela REST API."""
    if value is None:
        return ""
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, str):
        return enum_value.upper()
    return str(value).upper()


def _workspace_list_sdk(ws_path: str) -> list[dict] | None:
    """List objects in a Databricks workspace directory via SDK."""
    client = _get_workspace_client()
    if client is None:
        return None
    try:
        objects = []
        for obj in client.workspace.list(ws_path):
            objects.append(
                {
                    "path": getattr(obj, "path", ""),
                    "object_type": _object_type_name(
                        getattr(obj, "object_type", "")
                    ),
                    "file_size": getattr(obj, "file_size", 0) or 0,
                }
            )
        return objects
    except Exception as exc:
        import logging as _logging  # noqa: PLC0415
        _logging.getLogger("sci.portabilidade").debug(
            "[SCI] SDK workspace.list(%s) falhou: %s", ws_path, exc)
        return None


def _workspace_get_status_sdk(ws_path: str) -> dict | None:
    """Get status of a workspace object via SDK."""
    client = _get_workspace_client()
    if client is None:
        return None
    try:
        obj = client.workspace.get_status(ws_path)
        return {
            "path": getattr(obj, "path", ""),
            "object_type": _object_type_name(
                getattr(obj, "object_type", "")
            ),
        }
    except Exception as exc:
        import logging as _logging  # noqa: PLC0415
        _logging.getLogger("sci.portabilidade").debug(
            "[SCI] SDK workspace.get_status(%s) falhou: %s", ws_path, exc)
        return None


def _workspace_download_sdk(ws_path: str) -> bytes | None:
    """Download a file from the Databricks workspace via SDK."""
    client = _get_workspace_client()
    if client is None:
        return None
    try:
        with client.workspace.download(ws_path) as handle:
            return handle.read()
    except Exception as exc:
        import logging as _logging  # noqa: PLC0415
        _logging.getLogger("sci.portabilidade").debug(
            "[SCI] SDK workspace.download(%s) falhou: %s", ws_path, exc)
        return None



def _databricks_api_get(
    endpoint: str,
    params: dict[str, str] | None = None,
    *,
    timeout: int = 30,
    raw: bool = False,
) -> bytes | None:
    """Execute a GET request against the Databricks REST API (stdlib only)."""
    import urllib.request
    import urllib.parse

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    # Normalizar: plataforma Databricks pode injetar host sem scheme
    if host and not host.startswith("http"):
        host = f"https://{host}"
    token = os.environ.get("DATABRICKS_TOKEN")
    # Fallback: usar get_secret() (cobre Secret Scope, .env, st.secrets)
    if not token:
        try:
            from tc_core.secrets import get_secret as _get_secret  # noqa: PLC0415
            token = _get_secret("DATABRICKS_TOKEN") or _get_secret("DBX_TOKEN")
        except Exception:  # noqa: BLE001
            pass
    if not host or not token:
        return None

    url = f"{host}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.read()
    except Exception:
        return None


def _workspace_list(ws_path: str) -> list[dict] | None:
    """List objects in a Databricks workspace directory via SDK/REST."""
    import json as _json

    sdk_objects = _workspace_list_sdk(ws_path)
    if sdk_objects is not None:
        return sdk_objects

    data = _databricks_api_get("/api/2.0/workspace/list", {"path": ws_path})
    if data is None:
        return None
    try:
        return _json.loads(data).get("objects", [])
    except (ValueError, KeyError):
        return None


def _workspace_get_status(ws_path: str) -> dict | None:
    """Get status of a workspace object via SDK/REST."""
    import json as _json

    sdk_status = _workspace_get_status_sdk(ws_path)
    if sdk_status is not None:
        return sdk_status

    data = _databricks_api_get("/api/2.0/workspace/get-status", {"path": ws_path})
    if data is None:
        return None
    try:
        return _json.loads(data)
    except ValueError:
        return None


def _workspace_download(ws_path: str, *, timeout: int = 120) -> bytes | None:
    """Download a file from the Databricks workspace via SDK/REST."""
    sdk_content = _workspace_download_sdk(ws_path)
    if sdk_content is not None:
        return sdk_content

    return _databricks_api_get(
        "/api/2.0/workspace/export",
        {"path": ws_path, "format": "AUTO", "direct_download": "true"},
        timeout=timeout,
    )


def cloud_path_exists(path: str | Path) -> bool:
    """Check path existence, with Databricks Workspace API fallback."""
    try:
        if Path(path).exists():
            return True
    except (OSError, PermissionError):
        pass

    str_path = str(path)
    if str_path.startswith("/Workspace/"):
        status = _workspace_get_status(str_path)
        return status is not None and "object_type" in status

    return False


def cloud_isdir(path: str | Path) -> bool:
    """Check if path is a directory, with Databricks Workspace API fallback."""
    try:
        if os.path.isdir(path):
            return True
    except (OSError, PermissionError):
        pass

    str_path = str(path)
    if str_path.startswith("/Workspace/"):
        status = _workspace_get_status(str_path)
        return status is not None and status.get("object_type") in (
            "DIRECTORY",
            "REPO",
        )

    return False


def cloud_listdir(path: str | Path) -> list[str]:
    """List directory contents, with Databricks Workspace API fallback."""
    try:
        entries = os.listdir(path)
        if entries:
            return entries
    except (OSError, PermissionError):
        pass

    str_path = str(path)
    if str_path.startswith("/Workspace/"):
        objects = _workspace_list(str_path)
        if objects:
            return [obj["path"].rsplit("/", 1)[-1] for obj in objects]

    return []


def mirror_workspace_tree(
    ws_root: str,
    local_root: str | Path,
    *,
    extensions: set[str] | None = None,
    log: Callable[[str], None] | None = None,
) -> bool:
    """Mirror a workspace directory tree to a local path.

    Downloads files from the Databricks Workspace to a local directory,
    preserving the directory structure.  Used when /Workspace/ paths
    aren't filesystem-accessible (e.g., in Databricks Apps containers).

    Returns True if the local root contains usable data after the operation.
    """
    emit = log or (lambda _msg: None)
    extensions = extensions or {".parquet", ".json", ".xlsx"}
    local = Path(local_root)
    downloaded = 0

    def _mirror_dir(ws_path: str, local_path: Path) -> None:
        nonlocal downloaded
        objects = _workspace_list(ws_path)
        if not objects:
            return
        for obj in objects:
            name = obj["path"].rsplit("/", 1)[-1]
            obj_type = obj.get("object_type", "")
            if obj_type == "DIRECTORY":
                sub = local_path / name
                sub.mkdir(parents=True, exist_ok=True)
                _mirror_dir(obj["path"], sub)
            elif obj_type == "FILE":
                if any(name.lower().endswith(ext) for ext in extensions):
                    target = local_path / name
                    if target.exists():
                        # Verificar se tamanho mudou (cache stale)
                        remote_size = obj.get("file_size", 0)
                        if remote_size:
                            local_size = target.stat().st_size
                            if abs(remote_size - local_size) < 10:
                                continue  # tamanho compatível → cache válido
                            emit(f"[SCI] ♻ {name} (tamanho mudou: {local_size}→{remote_size})")
                        else:
                            continue  # sem info de tamanho remoto → manter cache
                    else:
                        emit(f"[SCI] ⬇ {name}")
                    content = _workspace_download(obj["path"])
                    if content:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(content)
                        downloaded += 1

    try:
        local.mkdir(parents=True, exist_ok=True)
        _mirror_dir(ws_root, local)
        emit(f"[SCI] Mirror concluído — {downloaded} arquivo(s) baixados.")
    except Exception as exc:
        emit(f"[SCI] Mirror falhou: {exc}")

    # Consider success if the local root has any content
    return local.exists() and any(local.iterdir())
