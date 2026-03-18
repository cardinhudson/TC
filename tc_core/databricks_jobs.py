from __future__ import annotations

import base64
import os
from typing import Any

import requests

from tc_core.secrets import get_secret

DEFAULT_REPO_ROOT = "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DEFAULT_PIPELINE_NOTEBOOK = (
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci/"
    "03_processar_e_publicar_delta"
)
DEFAULT_VALIDATION_NOTEBOOK = (
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci/"
    "05_validacao_pos_job"
)
DEFAULT_PREVALIDATION_NOTEBOOK = (
    "/Workspace/Users/u235107@inetpsa.com/Drafts/sci/"
    "04_prevalidar_excel"
)

ACTIVE_LIFE_CYCLE_STATES = {
    "BLOCKED",
    "PENDING",
    "QUEUED",
    "RUNNING",
    "TERMINATING",
    "WAITING_FOR_RETRY",
}

TERMINAL_LIFE_CYCLE_STATES = {
    "INTERNAL_ERROR",
    "SKIPPED",
    "TERMINATED",
}


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return None


def _get_host() -> str:
    host = _first_non_empty(
        get_secret("DATABRICKS_HOST"),
        get_secret("DATABRICKS_URL"),
        os.environ.get("DATABRICKS_HOST"),
        os.environ.get("DATABRICKS_URL"),
    )
    if not host:
        raise RuntimeError(
            "DATABRICKS_HOST nao configurado. "
            "Defina em Secret Scope, env ou st.secrets."
        )
    host = host.rstrip("/")
    # Garante scheme https:// (env/secret pode conter apenas o hostname)
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


def _get_token() -> str:
    token = _first_non_empty(
        get_secret("DATABRICKS_TOKEN"),
        get_secret("DBX_TOKEN"),
        os.environ.get("DATABRICKS_TOKEN"),
        os.environ.get("DBX_TOKEN"),
    )
    if token:
        return token

    # Fallback: extrair Bearer token via SDK OAuth (Databricks Apps com CLIENT_ID/SECRET)
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415
        w = WorkspaceClient()
        headers = w.config.authenticate()
        if isinstance(headers, dict):
            auth = headers.get("Authorization", "")
        elif callable(headers):
            auth = headers().get("Authorization", "")
        else:
            auth = ""
        bearer = auth.removeprefix("Bearer ").strip()
        if bearer:
            return bearer
    except Exception:
        pass

    raise RuntimeError(
        "DATABRICKS_TOKEN nao configurado. "
        "Defina em Secret Scope, env ou st.secrets."
    )


def _get_cluster_id() -> str:
    cluster_id = _first_non_empty(
        get_secret("SCI_PIPELINE_CLUSTER_ID"),
        get_secret("DATABRICKS_CLUSTER_ID"),
        os.environ.get("SCI_PIPELINE_CLUSTER_ID"),
        os.environ.get("DATABRICKS_CLUSTER_ID"),
    )
    if not cluster_id:
        raise RuntimeError(
            "SCI_PIPELINE_CLUSTER_ID nao configurado. "
            "Informe o cluster do workflow no Secret Scope ou env."
        )
    return cluster_id


def _request_databricks_api(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    host = _get_host()
    token = _get_token()
    response = requests.request(
        method,
        f"{host}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        params=params,
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def submit_tc_pipeline_run(
    *,
    ano: int,
    run_budget: bool,
    run_real: bool,
    repo_root: str = DEFAULT_REPO_ROOT,
    data_root: str = "",
    publish_root: str = "",
) -> dict[str, Any]:
    if not run_budget and not run_real:
        raise ValueError(
            "Ao menos uma opcao deve ser selecionada: Budget e/ou Real."
        )

    cluster_id = _get_cluster_id()

    effective_data_root = data_root or f"{repo_root}/dados"
    effective_publish_root = publish_root or f"{repo_root}/workspace_publish"
    payload = {
        "run_name": f"SCI App pipeline {ano}",
        "tasks": [
            {
                "task_key": "pipeline_workspace",
                "notebook_task": {
                    "notebook_path": DEFAULT_PIPELINE_NOTEBOOK,
                    "base_parameters": {
                        "REPO_ROOT": repo_root,
                        "DATA_ROOT": effective_data_root,
                        "PUBLISH_ROOT": effective_publish_root,
                        "ANO": str(ano),
                        "RUN_BUDGET": str(run_budget).lower(),
                        "RUN_REAL": str(run_real).lower(),
                    },
                },
                "existing_cluster_id": cluster_id,
            },
            {
                "task_key": "pos_validacao",
                "depends_on": [{"task_key": "pipeline_workspace"}],
                "notebook_task": {
                    "notebook_path": DEFAULT_VALIDATION_NOTEBOOK,
                    "base_parameters": {
                        "REPO_ROOT": repo_root,
                        "DATA_ROOT": effective_data_root,
                        "PUBLISH_ROOT": effective_publish_root,
                        "ANO": str(ano),
                    },
                },
                "existing_cluster_id": cluster_id,
            },
        ],
    }

    return _request_databricks_api(
        "POST",
        "/api/2.1/jobs/runs/submit",
        payload=payload,
    )


def submit_tc_prevalidation_run(
    *,
    ano: int,
    tipo_extracao: str,
    repo_root: str = DEFAULT_REPO_ROOT,
    data_root: str = "",
) -> dict[str, Any]:
    cluster_id = _get_cluster_id()
    effective_data_root = data_root or f"{repo_root}/dados"
    payload = {
        "run_name": f"SCI App pre-validacao {ano}",
        "tasks": [
            {
                "task_key": "prevalidacao_excel",
                "notebook_task": {
                    "notebook_path": DEFAULT_PREVALIDATION_NOTEBOOK,
                    "base_parameters": {
                        "REPO_ROOT": repo_root,
                        "DATA_ROOT": effective_data_root,
                        "ANO": str(ano),
                        "TIPO_EXTRACAO": tipo_extracao,
                    },
                },
                "existing_cluster_id": cluster_id,
            },
        ],
    }
    return _request_databricks_api(
        "POST",
        "/api/2.1/jobs/runs/submit",
        payload=payload,
    )


def get_databricks_run_output(*, run_id: int) -> dict[str, Any]:
    return _request_databricks_api(
        "GET",
        "/api/2.1/jobs/runs/get-output",
        params={"run_id": run_id},
    )


def get_tc_pipeline_run_status(*, run_id: int) -> dict[str, Any]:
    raw = _request_databricks_api(
        "GET",
        "/api/2.1/jobs/runs/get",
        params={"run_id": run_id},
    )

    state = raw.get("state") or {}
    life_cycle_state = state.get("life_cycle_state") or "UNKNOWN"
    tasks = []
    for task in raw.get("tasks") or []:
        task_state = task.get("state") or {}
        tasks.append(
            {
                "task_key": task.get("task_key"),
                "run_id": task.get("run_id"),
                "life_cycle_state": task_state.get("life_cycle_state"),
                "result_state": task_state.get("result_state"),
                "state_message": task_state.get("state_message"),
                "run_page_url": task.get("run_page_url"),
            }
        )

    return {
        "run_id": raw.get("run_id", run_id),
        "run_name": raw.get("run_name"),
        "job_id": raw.get("job_id"),
        "run_page_url": raw.get("run_page_url"),
        "life_cycle_state": life_cycle_state,
        "result_state": state.get("result_state"),
        "state_message": state.get("state_message"),
        "start_time": raw.get("start_time"),
        "end_time": raw.get("end_time"),
        "setup_duration": raw.get("setup_duration"),
        "execution_duration": raw.get("execution_duration"),
        "cleanup_duration": raw.get("cleanup_duration"),
        "is_terminal": life_cycle_state in TERMINAL_LIFE_CYCLE_STATES,
        "is_active": life_cycle_state in ACTIVE_LIFE_CYCLE_STATES,
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Upload de arquivos para o Databricks Workspace
# ---------------------------------------------------------------------------

def upload_file_to_workspace(
    local_path: str,
    workspace_path: str,
    *,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Faz upload de um arquivo binário para o Databricks Workspace.

    Tenta, em ordem:
    1. REST API (workspace-files → workspace/import) com token explícito
    2. Databricks SDK auto-auth (funciona em Databricks Apps sem PAT)

    Retorna dict com chave 'ok' (bool) e 'message' (str).
    """
    if not os.path.isfile(local_path):
        return {"ok": False, "message": f"Arquivo local não encontrado: {local_path}"}

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)

    # Slugify do nome do arquivo destino (evita acentos/espaços na URL)
    import unicodedata as _ud
    def _slugify(name: str) -> str:
        return _ud.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii").replace(" ", "_")
    if "/" in workspace_path:
        ws_dir, ws_name = workspace_path.rsplit("/", 1)
        workspace_path = f"{ws_dir}/{_slugify(ws_name)}"

    rest_error = "token não configurado"

    # --- Tentativa 1: REST API com token ---
    try:
        host = _get_host()
        token = _get_token()

        # Garantir que pasta destino existe
        parent_dir = workspace_path.rsplit("/", 1)[0] if "/" in workspace_path else "/"
        _ensure_workspace_dir(host, token, parent_dir)

        result1 = _upload_via_workspace_files(host, token, local_path, workspace_path, file_size_mb, overwrite)
        if result1["ok"]:
            return result1

        result2 = _upload_via_workspace_import(host, token, local_path, workspace_path, file_size_mb)
        if result2["ok"]:
            return result2

        rest_error = f"workspace-files: {result1['message'][:120]}; import: {result2['message'][:120]}"
    except RuntimeError:
        # Token não configurado — tentar SDK
        pass
    except Exception as exc:
        rest_error = str(exc)

    # --- Tentativa 2: SDK auto-auth (Databricks Apps) ---
    try:
        result_sdk = _upload_workspace_via_sdk(local_path, workspace_path, overwrite)
        return result_sdk
    except Exception as exc_sdk:
        sdk_error = str(exc_sdk)

    return {
        "ok": False,
        "message": (
            f"Todas as tentativas de upload Workspace falharam.\n"
            f"   Destino: `{workspace_path}`\n"
            f"   REST API: {rest_error}\n"
            f"   SDK: {sdk_error}"
        ),
    }


def _ensure_workspace_dir(host: str, token: str, path: str) -> None:
    """Cria recursivamente o diretório no workspace se não existir."""
    requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"path": path},
        timeout=30,
    )


def _upload_via_workspace_files(
    host: str,
    token: str,
    local_path: str,
    workspace_path: str,
    file_size_mb: float,
    overwrite: bool,
) -> dict[str, Any]:
    """Upload via PUT /api/2.0/workspace-files/import-file/{path}."""
    from urllib.parse import quote

    # A API espera o path SEM barra inicial após /import-file/
    # Ex: /Workspace/Users/... → Workspace/Users/...
    api_path = workspace_path.lstrip("/")
    # Garantir que começa com Workspace/
    if not api_path.startswith("Workspace/"):
        api_path = f"Workspace/{api_path}"
    # Codificar cada segmento individualmente (preservar / e @)
    encoded_path = quote(api_path, safe="/@")
    ow_param = "true" if overwrite else "false"

    url = f"{host}/api/2.0/workspace-files/import-file/{encoded_path}?overwrite={ow_param}"

    with open(local_path, "rb") as f:
        resp = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
            },
            data=f,
            timeout=180,
        )

    if resp.status_code in (200, 204):
        return {
            "ok": True,
            "message": (
                f"Upload concluído ({file_size_mb:.1f} MB)\n"
                f"   Destino: `{workspace_path}`"
            ),
        }

    return {
        "ok": False,
        "message": f"HTTP {resp.status_code}: {resp.text[:200]}",
    }


def _upload_via_workspace_import(
    host: str,
    token: str,
    local_path: str,
    workspace_path: str,
    file_size_mb: float,
) -> dict[str, Any]:
    """Último recurso: upload via workspace/import com base64 (para notebooks)."""
    if file_size_mb > 10:
        return {
            "ok": False,
            "message": (
                f"[WARN] base64 ignorado — arquivo ({file_size_mb:.1f} MB) "
                "excede limite de 10 MB do workspace/import"
            ),
        }

    with open(local_path, "rb") as f:
        content_b64 = base64.standard_b64encode(f.read()).decode("ascii")

    resp = requests.post(
        f"{host}/api/2.0/workspace/import",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={
            "path": workspace_path,
            "format": "AUTO",
            "content": content_b64,
            "overwrite": True,
        },
        timeout=180,
    )

    if resp.status_code == 200:
        return {
            "ok": True,
            "message": (
                f"Upload concluído ({file_size_mb:.1f} MB)\n"
                f"   Destino: `{workspace_path}`"
            ),
        }

    return {
        "ok": False,
        "message": f"workspace/import HTTP {resp.status_code}: {resp.text[:200]}",
    }


# ---------------------------------------------------------------------------
# Upload via DBFS (funciona em qualquer Databricks, sem limite de tamanho)
# ---------------------------------------------------------------------------

def _upload_via_dbfs_chunks(
    host: str,
    token: str,
    local_path: str,
    dbfs_path: str,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Upload para DBFS usando API create/add-block/close (suporta qualquer tamanho).

    Cada bloco envia até 700 KB (~1 MB em base64, dentro do limite da API).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    # 1. Criar handle
    resp = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": overwrite},
        timeout=30,
    )
    resp.raise_for_status()
    handle = resp.json()["handle"]

    # 2. Enviar em blocos de 700 KB
    _CHUNK_BYTES = 700 * 1024
    with open(local_path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_BYTES)
            if not chunk:
                break
            block_resp = requests.post(
                f"{host}/api/2.0/dbfs/add-block",
                headers=headers,
                json={
                    "handle": handle,
                    "data": base64.standard_b64encode(chunk).decode("ascii"),
                },
                timeout=60,
            )
            block_resp.raise_for_status()

    # 3. Fechar
    close_resp = requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=headers,
        json={"handle": handle},
        timeout=30,
    )
    close_resp.raise_for_status()

    return {"ok": True, "message": f"Upload DBFS concluído: `{dbfs_path}`"}


def _upload_workspace_via_sdk(
    local_path: str,
    workspace_path: str,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Upload para Workspace Files via Databricks SDK (auto-auth).

    - Arquivos <= 10 MB: usa workspace.import_ (base64).
    - Arquivos > 10 MB: usa REST PUT workspace-files com headers OAuth
      do SDK (suporta até 500 MB sem base64).
    """
    from databricks.sdk import WorkspaceClient  # noqa: PLC0415

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    w = WorkspaceClient()

    # Garantir que pasta existe
    parent_dir = workspace_path.rsplit("/", 1)[0] if "/" in workspace_path else "/"
    try:
        w.workspace.mkdirs(parent_dir)
    except Exception:
        pass  # Pode já existir

    if file_size_mb <= 10:
        # Caminho rápido: import_ com base64 (funciona para arquivos pequenos)
        import base64 as _b64
        from databricks.sdk.service.workspace import ImportFormat  # noqa: PLC0415

        with open(local_path, "rb") as fh:
            content = fh.read()
        w.workspace.import_(
            path=workspace_path,
            content=_b64.b64encode(content).decode(),
            format=ImportFormat.AUTO,
            overwrite=overwrite,
        )
        return {"ok": True, "message": f"Upload Workspace (SDK import) concluído: `{workspace_path}`"}

    # Arquivo > 10 MB: REST PUT binário (workspace-files, até 500 MB)
    from urllib.parse import quote

    host = w.config.host.rstrip("/")
    # Obter headers de autenticação OAuth do SDK
    auth_result = w.config.authenticate()
    if callable(auth_result):
        auth_headers = auth_result()
    elif isinstance(auth_result, dict):
        auth_headers = auth_result
    else:
        raise RuntimeError("SDK authenticate() retornou tipo inesperado")

    api_path = workspace_path.lstrip("/")
    if not api_path.startswith("Workspace/"):
        api_path = f"Workspace/{api_path}"
    encoded_path = quote(api_path, safe="/@")
    ow_param = "true" if overwrite else "false"
    url = f"{host}/api/2.0/workspace-files/import-file/{encoded_path}?overwrite={ow_param}"

    with open(local_path, "rb") as f:
        resp = requests.put(
            url,
            headers={
                **auth_headers,
                "Content-Type": "application/octet-stream",
            },
            data=f,
            timeout=300,
        )

    if resp.status_code in (200, 204):
        return {
            "ok": True,
            "message": (
                f"Upload Workspace (SDK+REST) concluído ({file_size_mb:.1f} MB)\n"
                f"   Destino: `{workspace_path}`"
            ),
        }

    return {
        "ok": False,
        "message": f"SDK+REST HTTP {resp.status_code}: {resp.text[:300]}",
    }


def _upload_via_sdk(
    local_path: str,
    dbfs_path: str,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Upload via Databricks SDK (auto-auth em Databricks Apps)."""
    from databricks.sdk import WorkspaceClient  # noqa: PLC0415

    w = WorkspaceClient()
    # SDK espera path sem prefixo "dbfs:"
    sdk_path = dbfs_path.replace("dbfs:", "", 1) if dbfs_path.startswith("dbfs:") else dbfs_path
    with open(local_path, "rb") as fh:
        w.dbfs.upload(sdk_path, fh, overwrite=overwrite)
    return {"ok": True, "message": f"Upload DBFS (SDK) concluído: `{dbfs_path}`"}


def upload_file_to_dbfs(
    local_path: str,
    dbfs_path: str,
    *,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Upload de arquivo para DBFS.

    Tenta, em ordem:
    1. REST API chunked (com token explícito)
    2. Databricks SDK auto-auth (funciona dentro de Databricks Apps sem token PAT)

    O arquivo ficará acessível nos notebooks Databricks como ``/dbfs/<path>``.
    Retorna dict com chave ``ok`` (bool) e ``message`` (str).
    """
    if not os.path.isfile(local_path):
        return {"ok": False, "message": f"Arquivo local não encontrado: {local_path}"}

    if not dbfs_path.startswith("dbfs:"):
        dbfs_path = "dbfs:" + dbfs_path if dbfs_path.startswith("/") else f"dbfs:/{dbfs_path}"

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    rest_error = "token não configurado"

    # --- Tentativa 1: REST API com token ---
    try:
        host = _get_host()
        token = _get_token()
        result = _upload_via_dbfs_chunks(host, token, local_path, dbfs_path, overwrite)
        result["message"] += f"\n   Tamanho: {file_size_mb:.1f} MB"
        return result
    except RuntimeError:
        # Token não configurado — tentar SDK
        pass
    except Exception as exc:
        # Outro erro na REST API — tentar SDK como fallback
        rest_error = str(exc)

    # --- Tentativa 2: SDK auto-auth (Databricks Apps) ---
    try:
        result = _upload_via_sdk(local_path, dbfs_path, overwrite)
        result["message"] += f"\n   Tamanho: {file_size_mb:.1f} MB"
        return result
    except Exception as exc_sdk:
        sdk_error = str(exc_sdk)

    return {
        "ok": False,
        "message": (
            f"Todas as tentativas de upload DBFS falharam.\n"
            f"   REST API: {rest_error}\n"
            f"   SDK: {sdk_error}"
        ),
    }
