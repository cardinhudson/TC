from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

DEFAULT_REPO_ROOT = "/Workspace/Users/u235107@inetpsa.com/Drafts/sci"
DEFAULT_WORKSPACE_DATA_ROOT = Path(DEFAULT_REPO_ROOT) / "dados"
DEFAULT_WORKSPACE_PUBLISH_ROOT = Path(DEFAULT_REPO_ROOT) / "workspace_publish"
VALID_BACKENDS = {"databricks", "snowflake"}


def _emit(log: Callable[[str], None] | None, level: str, message: str) -> None:
    writer = log or print
    writer(f"[{level}] {message}")


def get_backend(default: str = "databricks") -> str:
    """Retorna o backend de dados ativo para o SCI."""
    backend = os.environ.get("SCI_DATA_BACKEND", default).strip().lower()
    if backend not in VALID_BACKENDS:
        os.environ["SCI_DATA_BACKEND"] = default
        return default
    return backend


def _path_is_available(path: Path) -> tuple[bool, str]:
    try:
        return path.exists(), "ok"
    except PermissionError as exc:
        return False, f"sem permissão ({exc})"
    except OSError as exc:
        return False, f"erro de I/O ({exc})"


def resolve_data_root(
    user_value: str | Path | None = None,
    *,
    repo_root: str | Path | None = None,
    log: Callable[[str], None] | None = None,
) -> Path:
    """Seleciona a raiz de dados em Workspace Files.

    No ambiente atual, DBFS, Volumes e dbutils.fs estão bloqueados.
    Por isso, a raiz padrão passa a ser um diretório sob o próprio REPO_ROOT.
    """
    repo_base = Path(str(repo_root or DEFAULT_REPO_ROOT).strip())
    raw_value = user_value or os.environ.get("SCI_SHARED_DATA_ROOT")
    candidate = (
        Path(str(raw_value).strip())
        if raw_value
        else repo_base / "dados"
    )

    exists, reason = _path_is_available(candidate)
    if exists:
        _emit(
            log,
            "INFO",
            f"DATA_ROOT selecionado em Workspace Files: {candidate}",
        )
        os.environ["SCI_SHARED_DATA_ROOT"] = str(candidate)
        return candidate

    _emit(
        log,
        "WARN",
        "DATA_ROOT inexistente em Workspace Files: "
        f"{candidate} | motivo: {reason}",
    )
    candidate.mkdir(parents=True, exist_ok=True)
    _emit(log, "INFO", f"DATA_ROOT criado em Workspace Files: {candidate}")
    os.environ["SCI_SHARED_DATA_ROOT"] = str(candidate)
    return candidate


def get_publish_root(
    user_value: str | Path | None = None,
    *,
    repo_root: str | Path | None = None,
    log: Callable[[str], None] | None = None,
) -> Path:
    """Retorna a raiz publicada em Workspace Files.

    Path operacional validado no Databricks: ``workspace_publish``.
    """
    repo_base = Path(str(repo_root or DEFAULT_REPO_ROOT).strip())
    raw = (
        user_value
        or os.environ.get("SCI_PUBLISH_ROOT")
        or os.environ.get("SCI_DELTA_ROOT")
        or repo_base / "workspace_publish"
    )
    path = Path(str(raw).strip())
    path.mkdir(parents=True, exist_ok=True)
    _emit(log, "INFO", f"PUBLISH_ROOT selecionado: {path}")
    os.environ["SCI_PUBLISH_ROOT"] = str(path)
    os.environ["SCI_DELTA_ROOT"] = str(path)
    return path


def get_delta_root(
    user_value: str | Path | None = None,
    *,
    repo_root: str | Path | None = None,
    log: Callable[[str], None] | None = None,
) -> Path:
    """**Deprecado** — use ``get_publish_root`` em vez desta."""
    return get_publish_root(user_value, repo_root=repo_root, log=log)
