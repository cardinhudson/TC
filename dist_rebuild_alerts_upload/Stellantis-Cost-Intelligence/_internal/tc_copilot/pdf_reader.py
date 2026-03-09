"""
TC Copilot — Leitura e busca em PDFs gerados.

Extrai texto dos PDFs para uso como contexto na consulta inteligente.
"""

from __future__ import annotations

import logging
import os

from tc_copilot.config import caminho_relatorio

logger = logging.getLogger(__name__)


def extrair_texto_pdf(caminho: str) -> str:
    """
    Extrai todo o texto de um PDF.

    Returns:
        Texto completo do PDF ou string vazia se falhar.
    """
    if not os.path.exists(caminho):
        logger.warning("PDF não encontrado: %s", caminho)
        return ""

    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(caminho)
        paginas = []
        for page in reader.pages:
            texto = page.extract_text()
            if texto:
                paginas.append(texto)
        return "\n\n".join(paginas)
    except ImportError:
        logger.error("PyPDF2 não instalado. Instale com: pip install PyPDF2")
        return ""
    except Exception as e:
        logger.error("Erro ao extrair texto do PDF: %s", e)
        return ""


def extrair_texto_relatorio(ano: int) -> str:
    """Extrai texto do relatório anual de um ano específico."""
    caminho = caminho_relatorio(ano)
    return extrair_texto_pdf(caminho)


def buscar_no_pdf(texto_pdf: str, termo: str) -> list[str]:
    """
    Busca um termo no texto do PDF e retorna os parágrafos que contêm o termo.

    Args:
        texto_pdf: Texto completo extraído do PDF
        termo: Termo a buscar (case-insensitive)

    Returns:
        Lista de trechos relevantes (parágrafos contendo o termo)
    """
    if not texto_pdf or not termo:
        return []

    termo_lower = termo.lower()
    paragrafos = texto_pdf.split("\n\n")
    resultados = []

    for paragrafo in paragrafos:
        if termo_lower in paragrafo.lower():
            # Limitar tamanho do trecho
            trecho = paragrafo.strip()
            if len(trecho) > 500:
                # Encontrar posição do termo e retornar janela
                pos = trecho.lower().find(termo_lower)
                inicio = max(0, pos - 200)
                fim = min(len(trecho), pos + len(termo) + 200)
                trecho = "..." + trecho[inicio:fim] + "..."
            resultados.append(trecho)

    return resultados


def obter_contexto_para_consulta(ano: int, max_chars: int = 15000) -> str:
    """
    Obtém o contexto do relatório para enviar à LLM na consulta.

    Limita o tamanho do texto para não exceder o limite de tokens.

    Args:
        ano: Ano do relatório
        max_chars: Máximo de caracteres a retornar

    Returns:
        Texto do relatório (truncado se necessário)
    """
    texto = extrair_texto_relatorio(ano)
    if not texto:
        return ""

    if len(texto) <= max_chars:
        return texto

    # Truncar preservando parágrafos completos
    truncado = texto[:max_chars]
    ultimo_paragrafo = truncado.rfind("\n\n")
    if ultimo_paragrafo > max_chars * 0.8:
        truncado = truncado[:ultimo_paragrafo]

    return truncado + "\n\n[... texto truncado por limite de contexto ...]"


def relatorio_existe(ano: int) -> bool:
    """Verifica se o PDF do relatório já foi gerado para o ano."""
    caminho = caminho_relatorio(ano)
    return os.path.exists(caminho) and os.path.getsize(caminho) > 0
