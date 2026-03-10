"""
launcher.py — Ponto de entrada para o bundle PyInstaller.
==========================================================
Abre o Streamlit como app desktop usando pywebview.
Se pywebview não estiver disponível, abre no navegador padrão.

Em dev, use normalmente:  streamlit run app.py
"""
from __future__ import annotations

import atexit
import os
import socket
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


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

    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(
            None, mensagem, titulo, 0x10  # MB_ICONERROR
        )
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
            os.chdir(base_dir)

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
        try:
            opened_in_browser = _open_url_in_default_browser(server_url)
            if not opened_in_browser:
                raise RuntimeError("Falha ao abrir no navegador padrão")
            _log_startup_event(
                f"launcher: navegador aberto em {time.perf_counter() - started_at:.2f}s"
            )

            try:
                while server_process and server_process.poll() is None:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            finally:
                _stop_process(server_process)

        except Exception:
            # Fallback opcional: janela desktop via pywebview, se disponível.
            try:
                import webview

                webview.create_window(
                    title="Stellantis Cost Intelligence",
                    url=server_url,
                    width=1400,
                    height=900,
                    resizable=True,
                )
                webview.start()
                _log_startup_event(
                    f"launcher: fallback pywebview aberto em {time.perf_counter() - started_at:.2f}s"
                )
                _stop_process(server_process)
            except Exception as browser_exc:
                _stop_process(server_process)
                _mostrar_erro_e_logar(
                    "Stellantis Cost Intelligence",
                    "Nao foi possivel abrir a interface automaticamente.\n\n"
                    f"Acesse manualmente: {server_url}\n\n"
                    f"Detalhe: {browser_exc}"
                )
                sys.exit(1)

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
