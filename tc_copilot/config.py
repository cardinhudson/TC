"""
TC Copilot — Configuração centralizada.

Carrega chave OpenAI, define paths, idioma e modelo LLM.
Prioridade da chave: .env → openai_key.txt → os.environ
"""

import os
import shutil
import sys
from pathlib import Path

from tc_core.secrets import get_secret
from tc_core.utils.portabilidade import is_cloud

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

DEFAULT_CLAUDE_MODEL = "databricks-claude-opus-4-6"
MODELOS_DATABRICKS = ["databricks-claude-opus-4-6"]


def _load_env_file(*, override: bool = False) -> None:
    try:
        from dotenv import load_dotenv

        env_path = ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=override)
    except ImportError:
        pass


# ═══════════════════════════════════════════════════════════════
#  CARREGAR CHAVE OPENAI
# ═══════════════════════════════════════════════════════════════
def carregar_api_key() -> str | None:
    """
    Carrega a chave da OpenAI com prioridade:
      1. Camada unificada de segredos
      2. Arquivo openai_key.txt na raiz do projeto (fallback legado local)

    Retorna None se não encontrada.
    """
    _load_env_file(override=True)

    key = (get_secret("OPENAI_API_KEY") or "").strip()
    if key:
        return key

    # 2. Arquivo openai_key.txt
    key_file = ROOT / "openai_key.txt"
    if key_file.exists():
        key = key_file.read_text(encoding="utf-8").strip()
        if key:
            return key

    return None


def carregar_modelo() -> str:
    """Retorna o modelo LLM configurado conforme o provider ativo."""
    _load_env_file()
    modelo = os.environ.get("LLM_MODEL", "").strip()
    if modelo:
        return modelo
    if carregar_provider() == "databricks_claude":
        return DEFAULT_CLAUDE_MODEL
    return DEFAULT_MODEL


def carregar_idioma() -> str:
    """Retorna o idioma configurado (default: pt-BR)."""
    _load_env_file()
    return os.environ.get("REPORT_LANGUAGE", DEFAULT_LANGUAGE).strip()


def carregar_provider() -> str:
    """Retorna o provider LLM: 'databricks_claude' (default) ou 'openai'."""
    _load_env_file()
    return os.environ.get("TC_LLM_PROVIDER", "databricks_claude").strip().lower()


def carregar_databricks_cfg() -> dict:
    """Retorna config do Databricks Model Serving para Claude."""
    _load_env_file(override=True)
    return {
        "url": (
            os.environ.get("TC_DATABRICKS_URL")
            or os.environ.get("DATABRICKS_HOST", "")
        ).strip().rstrip("/"),
        "endpoint": os.environ.get(
            "TC_DATABRICKS_ENDPOINT",
            DEFAULT_CLAUDE_MODEL,
        ).strip(),
        "token": (
            os.environ.get("TC_DATABRICKS_TOKEN")
            or os.environ.get("DATABRICKS_TOKEN", "")
        ).strip(),
    }


def _salvar_variaveis_env(valores: dict[str, str]) -> None:
    """Atualiza ou adiciona variaveis no .env local apenas fora de cloud."""
    for chave, valor in valores.items():
        os.environ[chave] = valor

    if is_cloud():
        return

    env_path = ROOT / ".env"
    linhas = (
        env_path.read_text(encoding="utf-8").splitlines()
        if env_path.exists()
        else []
    )

    for chave, valor in valores.items():
        prefixo = f"{chave}="
        atualizado = False
        for indice, linha in enumerate(linhas):
            if linha.strip().startswith(prefixo):
                linhas[indice] = f"{chave}={valor}"
                atualizado = True
                break
        if not atualizado:
            linhas.append(f"{chave}={valor}")

    env_path.write_text("\n".join(linhas) + "\n", encoding="utf-8")


def salvar_api_key(key: str) -> None:
    """Salva a chave localmente apenas fora de cloud."""
    _salvar_variaveis_env({"OPENAI_API_KEY": key})


def salvar_provider(provider: str) -> None:
    """Salva o provider LLM selecionado."""
    _salvar_variaveis_env({"TC_LLM_PROVIDER": provider.strip().lower()})


def salvar_modelo(modelo: str) -> None:
    """Salva o modelo LLM selecionado."""
    _salvar_variaveis_env({"LLM_MODEL": modelo.strip()})


def salvar_idioma(idioma: str) -> None:
    """Salva o idioma do relatorio."""
    _salvar_variaveis_env({"REPORT_LANGUAGE": idioma.strip()})


def salvar_databricks_cfg(url: str, endpoint: str, token: str) -> None:
    """Salva a configuracao do Databricks Model Serving."""
    _salvar_variaveis_env(
        {
            "TC_DATABRICKS_URL": url.strip().rstrip("/"),
            "TC_DATABRICKS_ENDPOINT": endpoint.strip(),
            "TC_DATABRICKS_TOKEN": token.strip(),
        }
    )


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
    """Retorna o path do JSON intermediario do modo local."""
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
    _load_env_file()
    val = os.environ.get("COPILOT_ENABLED", "1").strip()
    return val not in ("0", "false", "False", "no")


def salvar_copilot_habilitado(habilitado: bool) -> None:
    """Salva COPILOT_ENABLED=1|0 no .env apenas fora de cloud."""
    valor = "1" if habilitado else "0"
    _salvar_variaveis_env({"COPILOT_ENABLED": valor})
