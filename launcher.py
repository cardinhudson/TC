"""
launcher.py — Ponto de entrada para o bundle PyInstaller.
==========================================================
Abre o Streamlit como app desktop usando pywebview.
Se pywebview não estiver disponível, abre no navegador padrão.

Em dev, use normalmente:  streamlit run app.py
"""
from __future__ import annotations

import os
import sys
import socket
import time
import threading
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


def main() -> None:
    try:
        is_frozen = getattr(sys, "frozen", False)
        
        if is_frozen and hasattr(sys, "_MEIPASS"):
            os.chdir(sys._MEIPASS)

        # Evitar abertura do navegador pelo Streamlit
        os.environ["BROWSER"] = "none"
        os.environ["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"

        # Iniciar servidor Streamlit em thread separada (não subprocess!)
        server_thread = threading.Thread(target=_run_streamlit_server, daemon=True)
        server_thread.start()

        # Aguardar servidor ficar pronto
        if not wait_for_server("127.0.0.1", 8501, max_wait_seconds=90):
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

        except ImportError:
            # pywebview não disponível - usar navegador padrão
            import webbrowser
            webbrowser.open("http://127.0.0.1:8501")
            # Manter processo vivo
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

    except Exception as e:
        import traceback
        _mostrar_erro_e_logar(
            "Stellantis Cost Intelligence - Erro",
            f"Erro ao iniciar: {e}\n\n{traceback.format_exc()}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
