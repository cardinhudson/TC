"""
launcher.py — Ponto de entrada para o bundle PyInstaller.
==========================================================
Abre o Streamlit como app desktop usando pywebview.
Se pywebview não estiver disponível, abre no navegador padrão.

Em dev, use normalmente:  streamlit run app.py
"""
from __future__ import annotations

import atexit
import hashlib
import importlib
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def _looks_like_synced_path(path: Path) -> bool:
    """Detecta caminhos com alta chance de sincronização em nuvem."""
    text = str(path).lower()
    markers = [
        "onedrive",
        "sharepoint",
        "partagei",
        "stellantis",
        "geib",
    ]
    return any(marker in text for marker in markers)


def _get_exe_dir() -> Path:
    """Retorna a pasta do executável ou do script em dev."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _is_local_runtime_active() -> bool:
    """Evita relançamentos em loop quando já estamos rodando do cache local."""
    return os.environ.get("SCI_LOCAL_RUNTIME_ACTIVE") == "1"


def _shared_data_root_from_bundle(base_dir: Path) -> Path:
    """Dados continuam apontando para a origem compartilhada do bundle."""
    return base_dir / "dados"


def _file_signature(path: Path) -> str:
    """Gera assinatura simples para invalidar cache quando o build muda."""
    try:
        stat = path.stat()
        return f"{stat.st_size}:{stat.st_mtime_ns}"
    except Exception:
        return "missing"


def _runtime_artifact_issues(base_dir: Path) -> list[str]:
    """Valida artefatos bundled essenciais para o EXE."""
    issues: list[str] = []

    altair_schema = base_dir / "altair" / "vegalite" / "v5" / "schema" / "vega-lite-schema.json"
    if not altair_schema.exists():
        issues.append(f"Altair schema ausente: {altair_schema}")

    draft3_schema = base_dir / "jsonschema_specifications" / "schemas" / "draft3" / "metaschema.json"
    if not draft3_schema.exists():
        issues.append(f"JSON Schema draft3 ausente: {draft3_schema}")

    numpy_core = base_dir / "numpy" / "_core"
    numpy_libs = base_dir / "numpy.libs"
    if not numpy_core.exists():
        issues.append(f"Diretorio NumPy _core ausente: {numpy_core}")
    if not numpy_libs.exists():
        issues.append(f"Diretorio numpy.libs ausente: {numpy_libs}")

    return issues


def _runtime_cache_dir(source_exe_dir: Path) -> Path:
    """Calcula a pasta de cache local do runtime para uma origem compartilhada."""
    local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    source_exe = source_exe_dir / Path(sys.executable).name
    signature_parts = [
        str(source_exe_dir),
        _file_signature(source_exe),
        _file_signature(source_exe_dir / "_internal" / "versao.json"),
        _file_signature(source_exe_dir / "_internal" / "altair" / "vegalite" / "v5" / "schema" / "vega-lite-schema.json"),
        _file_signature(source_exe_dir / "_internal" / "jsonschema_specifications" / "schemas" / "draft3" / "metaschema.json"),
    ]
    digest = hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:12]
    return local_app_data / "SCI" / "runtime" / digest


def _copy_runtime_tree(source_dir: Path, target_dir: Path) -> None:
    """Espelha a pasta do executável para cache local."""
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    shutil.copytree(source_dir, target_dir)


def _ensure_local_runtime_copy(source_exe_dir: Path) -> Path:
    """Garante que existe uma cópia local atualizada do runtime."""
    target_dir = _runtime_cache_dir(source_exe_dir)
    source_exe = source_exe_dir / Path(sys.executable).name
    target_exe = target_dir / source_exe.name
    source_base_dir = source_exe_dir / "_internal"
    target_base_dir = target_dir / "_internal"

    needs_refresh = True
    if target_exe.exists():
        try:
            needs_refresh = source_exe.stat().st_mtime > target_exe.stat().st_mtime
        except Exception:
            needs_refresh = True

    if not needs_refresh and _runtime_artifact_issues(target_base_dir):
        needs_refresh = True

    # Nao propaga um runtime de origem incompleto para o cache local.
    source_issues = _runtime_artifact_issues(source_base_dir)
    if source_issues:
        raise RuntimeError("Origem compartilhada com artefatos ausentes: " + "; ".join(source_issues))

    if needs_refresh:
        _copy_runtime_tree(source_exe_dir, target_dir)

    target_issues = _runtime_artifact_issues(target_base_dir)
    if target_issues:
        raise RuntimeError("Cache local incompleto apos copia: " + "; ".join(target_issues))

    return target_exe


def _relaunch_from_local_runtime(source_exe_dir: Path, base_dir: Path) -> bool:
    """Relaunch do EXE a partir de cache local, mantendo dados no compartilhado."""
    if not getattr(sys, "frozen", False) or _is_local_runtime_active():
        return False
    if not _looks_like_synced_path(source_exe_dir):
        return False

    try:
        target_exe = _ensure_local_runtime_copy(source_exe_dir)
    except Exception as exc:
        _mostrar_erro_e_logar(
            "Stellantis Cost Intelligence - Runtime Local",
            "Nao foi possivel preparar o runtime local do executavel.\n\n"
            f"Detalhe: {exc}"
        )
        return False

    env = os.environ.copy()
    env["SCI_LOCAL_RUNTIME_ACTIVE"] = "1"
    env["SCI_SHARED_DATA_ROOT"] = str(_shared_data_root_from_bundle(base_dir))
    env["SCI_SHARED_SOURCE_DIR"] = str(source_exe_dir)

    kwargs = {
        "cwd": str(target_exe.parent),
        "env": env,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    subprocess.Popen([str(target_exe), *sys.argv[1:]], **kwargs)
    return True


def _critical_import_issues() -> list[str]:
    """Tenta importar dependências críticas para detectar falhas cedo."""
    issues: list[str] = []
    for mod_name in ("numpy", "pandas", "altair", "pyarrow"):
        try:
            importlib.import_module(mod_name)
        except Exception as exc:
            issues.append(f"Falha ao importar {mod_name}: {exc}")

    try:
        importlib.import_module("pyarrow.parquet")
    except Exception as exc:
        issues.append(f"Falha ao importar pyarrow.parquet: {exc}")
    return issues


def _run_preflight_checks(base_dir: Path) -> None:
    """Executa verificações de portabilidade antes de subir o Streamlit."""
    if not getattr(sys, "frozen", False):
        return

    exe_dir = _get_exe_dir()
    issues = _runtime_artifact_issues(base_dir)
    issues.extend(_critical_import_issues())

    if issues:
        location_hint = ""
        if _looks_like_synced_path(exe_dir):
            location_hint = (
                "\n\nO executavel esta em pasta sincronizada (OneDrive/SharePoint/Partagei). "
                "Copie a pasta completa do executavel para um diretorio local antes de executar."
            )

        _mostrar_erro_e_logar(
            "Stellantis Cost Intelligence - Dependencias",
            "Falha no preflight do executavel:\n\n- " + "\n- ".join(issues) + location_hint,
        )
        sys.exit(1)

    if _looks_like_synced_path(exe_dir):
        _log_startup_event(
            f"launcher: aviso - executavel em pasta sincronizada: {exe_dir}"
        )


def is_port_open(host: str, port: int, timeout_seconds: int = 1) -> bool:
    """Verifica se a porta está aberta."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_seconds)
    try:
        sock.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def wait_for_server(host: str, port: int, max_wait_seconds: int = 90) -> bool:
    """Aguarda até que o servidor Streamlit esteja pronto."""
    start = time.time()
    while time.time() - start < max_wait_seconds:
        if is_port_open(host, port, timeout_seconds=1):
            return True
        time.sleep(0.5)
    return False


def wait_for_streamlit_http_ready(base_url: str, max_wait_seconds: int = 30) -> bool:
    """Aguarda o endpoint HTTP de health do Streamlit responder com sucesso."""
    start = time.time()
    health_urls = [
        f"{base_url}/_stcore/health",
        f"{base_url}/healthz",
        base_url,
    ]
    while time.time() - start < max_wait_seconds:
        for url in health_urls:
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if 200 <= getattr(response, "status", 200) < 500:
                        return True
            except Exception:
                continue
        time.sleep(0.5)
    return False


def _mostrar_erro_e_logar(titulo: str, mensagem: str) -> None:
    """Mostra caixa de diálogo de erro e grava em SCI_error.log."""
    try:
        exe_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) \
                  else Path(__file__).parent
        log_path = exe_dir / "SCI_error.log"
        with open(log_path, "a", encoding="utf-8") as f:
            import datetime
            f.write(f"\n{'='*60}\n")
            f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {titulo}\n")
            f.write(f"{mensagem}\n")
    except Exception:
        pass

    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(
            None, mensagem, titulo, 0x10  # MB_ICONERROR
        )
    except Exception:
        pass


def _log_startup_event(message: str) -> None:
    """Registra eventos simples de startup para comparar tempo entre versões."""
    try:
        exe_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
        log_path = exe_dir / "SCI_startup.log"
        with open(log_path, "a", encoding="utf-8") as f:
            import datetime
            f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")
    except Exception:
        pass


def _run_streamlit_server():
    """Executa o servidor Streamlit internamente (sem subprocess)."""
    # Configurar argumentos para o Streamlit CLI
    sys.argv = [
        "streamlit",
        "run",
        "app.py",
        "--server.port=8501",
        "--server.headless=true",
        "--server.address=127.0.0.1",
        "--global.developmentMode=false",
        "--browser.gatherUsageStats=false",
    ]
    
    try:
        from streamlit.web import cli as stcli
        stcli.main(standalone_mode=False)
    except SystemExit:
        pass  # Streamlit chama sys.exit() ao encerrar
    except Exception as e:
        _mostrar_erro_e_logar("Erro Streamlit", str(e))


def _get_runtime_dir() -> Path:
    """Retorna a pasta base usada para localizar app.py e recursos bundled."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def _is_server_mode() -> bool:
    """Indica se esta instância deve apenas subir o servidor Streamlit."""
    return "--streamlit-server" in sys.argv or os.environ.get("SCI_STREAMLIT_SERVER") == "1"


def _start_streamlit_subprocess(base_dir: Path) -> subprocess.Popen:
    """Inicia o servidor Streamlit em um subprocesso separado do launcher UI."""
    env = os.environ.copy()
    env["BROWSER"] = "none"
    env["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"
    env["SCI_STREAMLIT_SERVER"] = "1"

    if getattr(sys, "frozen", False):
        cmd = [sys.executable, "--streamlit-server"]
    else:
        cmd = [sys.executable, str(Path(__file__).resolve()), "--streamlit-server"]

    kwargs = {
        "cwd": str(base_dir),
        "env": env,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    return subprocess.Popen(cmd, **kwargs)


def _stop_process(process: subprocess.Popen | None) -> None:
    """Encerra o subprocesso do servidor sem deixar órfãos."""
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=10)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _iter_prewarm_files(base_dir: Path) -> list[Path]:
    """Lista arquivos mais relevantes para aquecimento inicial do bundle."""
    roots = [base_dir / "dados", base_dir / ".streamlit"]
    allowed_exts = {".parquet", ".json", ".toml", ".png", ".md"}
    priority_names = {
        "df_principal_BUD.parquet",
        "df_principal.parquet",
        "df_vol_veiculos_BUD.parquet",
        "df_vol_veiculos.parquet",
        "df_final_historico.parquet",
        "df_vol_historico.parquet",
        "df_final_historico_BUD.parquet",
        "df_vol_historico_BUD.parquet",
        "SCI_faixa.png",
        "config.toml",
    }
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in allowed_exts:
                files.append(path)
    files.sort(key=lambda item: (item.name not in priority_names, len(str(item))))
    return files


def _touch_file(path: Path) -> None:
    """Lê um trecho do arquivo para antecipar IO/extracao do bundle."""
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size <= 2_000_000:
                handle.read()
                return

            # Aquecer cabeçalho/início do arquivo traz mais benefício para parquet/json.
            handle.read(min(size, 2_000_000))

            # Para arquivos maiores, tocar também o fim melhora a extração/cache local.
            if size > 8_000_000:
                tail_size = min(1_000_000, size)
                handle.seek(max(0, size - tail_size))
                handle.read(tail_size)
    except Exception:
        pass


def _open_url_in_default_browser(url: str) -> bool:
    """Abre a URL no navegador padrão com fallbacks para Windows."""
    try:
        import webbrowser

        if webbrowser.open(url, new=2, autoraise=True):
            return True
    except Exception:
        pass

    if os.name == "nt":
        try:
            subprocess.Popen(["cmd", "/c", "start", "", url], shell=False)
            return True
        except Exception:
            return False
    return False


def _prewarm_bundle(base_dir: Path) -> None:
    """Aquece recursos bundled logo na primeira abertura do EXE."""
    files = _iter_prewarm_files(base_dir)
    if not files:
        return
    max_workers = min(6, max(2, (os.cpu_count() or 4) // 2))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(_touch_file, files))


def main() -> None:
    server_process: subprocess.Popen | None = None
    try:
        started_at = time.perf_counter()
        is_frozen = getattr(sys, "frozen", False)
        base_dir = _get_runtime_dir()
        _log_startup_event("launcher: inicio")

        if is_frozen:
            if not _is_server_mode() and _relaunch_from_local_runtime(_get_exe_dir(), base_dir):
                return
            os.chdir(base_dir)
            _run_preflight_checks(base_dir)

        if _is_server_mode():
            _run_streamlit_server()
            return

        # Evitar abertura do navegador pelo Streamlit
        os.environ["BROWSER"] = "none"
        os.environ["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"

        if is_frozen:
            warmup_started = time.perf_counter()
            _prewarm_bundle(base_dir)
            _log_startup_event(
                f"launcher: warmup concluido em {time.perf_counter() - warmup_started:.2f}s"
            )

        # Iniciar servidor Streamlit em subprocesso separado para evitar erros
        # de signal() fora da main thread do interpretador.
        server_process = _start_streamlit_subprocess(base_dir)
        atexit.register(_stop_process, server_process)
        _log_startup_event("launcher: subprocesso do streamlit iniciado")

        server_url = "http://127.0.0.1:8501"

        # Aguardar servidor ficar pronto
        if not wait_for_server("127.0.0.1", 8501, max_wait_seconds=90):
            if server_process and server_process.poll() is not None:
                _mostrar_erro_e_logar(
                    "Stellantis Cost Intelligence",
                    f"Servidor Streamlit encerrou prematuramente com código {server_process.returncode}."
                )
            _stop_process(server_process)
            _mostrar_erro_e_logar(
                "Stellantis Cost Intelligence",
                "Servidor nao respondeu na porta 8501.\n\n"
                "Verifique se outra instancia esta rodando."
            )
            sys.exit(1)

        _log_startup_event(
            f"launcher: porta pronta em {time.perf_counter() - started_at:.2f}s"
        )

        if server_process and server_process.poll() is not None:
            _mostrar_erro_e_logar(
                "Stellantis Cost Intelligence",
                f"Servidor Streamlit encerrou prematuramente com código {server_process.returncode}."
            )
            sys.exit(1)

        # Porta aberta não garante que o front-end terminou a inicialização.
        wait_for_streamlit_http_ready(server_url, max_wait_seconds=30)
        time.sleep(0.8)
        _log_startup_event(
            f"launcher: http pronto em {time.perf_counter() - started_at:.2f}s"
        )

        # ─── Fluxo principal: abrir no navegador padrão ─────────────────────
        opened_in_browser = _open_url_in_default_browser(server_url)
        if opened_in_browser:
            _log_startup_event(
                f"launcher: navegador aberto em {time.perf_counter() - started_at:.2f}s"
            )
        else:
            _mostrar_erro_e_logar(
                "Stellantis Cost Intelligence",
                "Nao foi possivel abrir o navegador automaticamente.\n\n"
                f"Acesse manualmente: {server_url}"
            )

        try:
            while server_process and server_process.poll() is None:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            _stop_process(server_process)

    except Exception as e:
        import traceback
        _stop_process(server_process)
        _mostrar_erro_e_logar(
            "Stellantis Cost Intelligence - Erro",
            f"Erro ao iniciar: {e}\n\n{traceback.format_exc()}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
