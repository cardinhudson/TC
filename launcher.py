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
        bytes_to_read = size if size <= 2_000_000 else min(size, 512_000)
        with path.open("rb") as handle:
            handle.read(bytes_to_read)
    except Exception:
        pass


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
        is_frozen = getattr(sys, "frozen", False)
        base_dir = _get_runtime_dir()

        if is_frozen:
            os.chdir(base_dir)

        if _is_server_mode():
            _run_streamlit_server()
            return

        # Evitar abertura do navegador pelo Streamlit
        os.environ["BROWSER"] = "none"
        os.environ["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"

        if is_frozen:
            _prewarm_bundle(base_dir)

        # Iniciar servidor Streamlit em subprocesso separado para evitar erros
        # de signal() fora da main thread do interpretador.
        server_process = _start_streamlit_subprocess(base_dir)
        atexit.register(_stop_process, server_process)

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

        # ─── Tentar abrir com pywebview (app desktop) ───────────────────────
        try:
            import webview

            window = webview.create_window(
                title="Stellantis Cost Intelligence",
                url="http://127.0.0.1:8501",
                width=1400,
                height=900,
                resizable=True,
            )

            webview.start()
            _stop_process(server_process)

        except ImportError:
            # pywebview não disponível - usar navegador padrão
            import webbrowser
            webbrowser.open("http://127.0.0.1:8501")
            # Manter processo vivo
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
