"""
TC Copilot — Configuração centralizada.

Carrega chave OpenAI, define paths, idioma e modelo LLM.
Prioridade da chave: .env → openai_key.txt → os.environ
"""

import os
import shutil
import sys
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
#  RAIZ DO PROJETO (compatível com EXE PyInstaller)
# ═══════════════════════════════════════════════════════════════
if hasattr(sys, '_MEIPASS'):
    ROOT = Path(sys._MEIPASS)
else:
    ROOT = Path(__file__).resolve().parents[1]

# ═══════════════════════════════════════════════════════════════
#  CONSTANTES
# ═══════════════════════════════════════════════════════════════
PASTA_RELATORIOS = ROOT / "documentacao_anual"
PASTA_DADOS = ROOT / "dados"
DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_LANGUAGE = "pt-BR"

IDIOMAS = {
    "pt-BR": "Português (Brasil)",
    "en": "English",
}

MODELOS_LLM = [
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1-mini",
    "gpt-4.1",
]


# ═══════════════════════════════════════════════════════════════
#  CARREGAR CHAVE OPENAI
# ═══════════════════════════════════════════════════════════════
def carregar_api_key() -> str | None:
    """
    Carrega a chave da OpenAI com prioridade:
      1. Variável de ambiente OPENAI_API_KEY (pode vir do .env)
      2. Arquivo openai_key.txt na raiz do projeto
      3. st.secrets (se disponível)

    Retorna None se não encontrada.
    """
    # 1. Tentar carregar do .env (se python-dotenv instalado)
    try:
        from dotenv import load_dotenv
        env_path = ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
    except ImportError:
        pass

    # Chave via variável de ambiente (inclui .env carregado acima)
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if key:
        return key

    # Fallback: ler direto do arquivo .env (caso dotenv não funcione)
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("OPENAI_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
                if key:
                    return key
    if key:
        return key

    # 2. Arquivo openai_key.txt
    key_file = ROOT / "openai_key.txt"
    if key_file.exists():
        key = key_file.read_text(encoding="utf-8").strip()
        if key:
            return key

    # 3. Streamlit secrets
    try:
        import streamlit as st
        key = st.secrets.get("OPENAI_API_KEY", "").strip()
        if key:
            return key
    except Exception:
        pass

    return None


def carregar_modelo() -> str:
    """Retorna o modelo LLM configurado (default: gpt-4o-mini)."""
    try:
        from dotenv import load_dotenv
        env_path = ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass
    return os.environ.get("LLM_MODEL", DEFAULT_MODEL).strip()


def carregar_idioma() -> str:
    """Retorna o idioma configurado (default: pt-BR)."""
    try:
        from dotenv import load_dotenv
        env_path = ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass
    return os.environ.get("REPORT_LANGUAGE", DEFAULT_LANGUAGE).strip()


def salvar_api_key(key: str) -> None:
    """Salva a chave da OpenAI no arquivo .env (cria se não existir)."""
    env_path = ROOT / ".env"
    lines = []
    found = False

    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("OPENAI_API_KEY="):
                lines.append(f"OPENAI_API_KEY={key}")
                found = True
            else:
                lines.append(line)

    if not found:
        lines.append(f"OPENAI_API_KEY={key}")

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.environ["OPENAI_API_KEY"] = key


def garantir_pasta_relatorios() -> Path:
    """Cria a pasta de relatórios se não existir e retorna o path."""
    PASTA_RELATORIOS.mkdir(parents=True, exist_ok=True)
    return PASTA_RELATORIOS


def em_execucao_empacotada() -> bool:
    """Indica se o app está rodando como executável PyInstaller."""
    return getattr(sys, "frozen", False)


def caminho_downloads_usuario() -> Path:
    """Retorna a pasta Downloads do usuário, criando-a se necessário."""
    downloads = Path.home() / "Downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    return downloads


def salvar_bytes_em_downloads(file_name: str, payload: bytes) -> Path:
    """Salva bytes diretamente na pasta Downloads do usuário."""
    destino = caminho_downloads_usuario() / file_name
    destino.write_bytes(payload)
    return destino


def copiar_arquivo_para_downloads(
    origem: str | Path,
    file_name: str | None = None,
) -> Path:
    """Copia um arquivo existente para a pasta Downloads do usuário."""
    origem_path = Path(origem)
    destino = caminho_downloads_usuario() / (file_name or origem_path.name)
    shutil.copy2(origem_path, destino)
    return destino


def caminho_relatorio(ano: int) -> Path:
    """Retorna o path do PDF anual."""
    return garantir_pasta_relatorios() / f"relatorio_{ano}.pdf"


def caminho_relatorio_local(ano: int) -> Path:
    """Retorna o path do PDF local (sem API) anual."""
    return garantir_pasta_relatorios() / f"relatorio_{ano}_local.pdf"


def caminho_dados_relatorio(ano: int) -> Path:
    """Retorna o path do JSON intermediário com dados acumulados."""
    return garantir_pasta_relatorios() / f".relatorio_{ano}_dados.json"


def caminho_dados_relatorio_local(ano: int) -> Path:
    """Retorna o path do JSON intermediário (modo local) com dados acumulados."""
    return garantir_pasta_relatorios() / f".relatorio_{ano}_local_dados.json"


def caminho_relatorio_mensal(ano: int, mes: int, modo: str = "local") -> Path:
    """Retorna o path do PDF mensal individual.

    Args:
        ano: Ano do relatório.
        mes: Mês (1-12).
        modo: 'local' (sem API) ou 'ia' (com LLM).
    """
    sufixo = "_local" if modo == "local" else ""
    return (
        garantir_pasta_relatorios()
        / f"relatorio_{ano}{sufixo}_mes_{mes:02d}.pdf"
    )


# ═══════════════════════════════════════════════════════════════
#  TOGGLE COPILOT (habilitar / desabilitar)
# ═══════════════════════════════════════════════════════════════
def carregar_copilot_habilitado() -> bool:
    """Retorna True se o Copilot está habilitado (default: True)."""
    try:
        from dotenv import load_dotenv
        env_path = ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass
    val = os.environ.get("COPILOT_ENABLED", "1").strip()
    return val not in ("0", "false", "False", "no")


def salvar_copilot_habilitado(habilitado: bool) -> None:
    """Salva COPILOT_ENABLED=1|0 no .env."""
    env_path = ROOT / ".env"
    lines = []
    found = False
    valor = "1" if habilitado else "0"

    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("COPILOT_ENABLED="):
                lines.append(f"COPILOT_ENABLED={valor}")
                found = True
            else:
                lines.append(line)

    if not found:
        lines.append(f"COPILOT_ENABLED={valor}")

    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.environ["COPILOT_ENABLED"] = valor
