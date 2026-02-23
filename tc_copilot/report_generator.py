"""
TC Copilot — Geração de PDF com ReportLab.

Cria relatórios anuais com capa, sumário, capítulos mensais e rodapé.
Usa JSON intermediário para persistência cumulativa.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from tc_copilot.config import (
    PASTA_RELATORIOS,
    caminho_dados_relatorio,
    caminho_relatorio,
    garantir_pasta_relatorios,
)
from tc_copilot.prompts import LABELS, obter_nome_mes

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  CORES CORPORATIVAS
# ═══════════════════════════════════════════════════════════════
COR_PRIMARIA = colors.HexColor("#003366")       # Azul escuro
COR_SECUNDARIA = colors.HexColor("#005A9E")      # Azul médio
COR_DESTAQUE = colors.HexColor("#0078D4")        # Azul claro
COR_FUNDO_HEADER = colors.HexColor("#E8F0FE")    # Azul muito claro
COR_TEXTO = colors.HexColor("#1A1A1A")           # Quase preto
COR_CINZA = colors.HexColor("#666666")           # Cinza subtítulos

# ═══════════════════════════════════════════════════════════════
#  ESTILOS DE TEXTO
# ═══════════════════════════════════════════════════════════════


def _criar_estilos() -> dict[str, ParagraphStyle]:
    """Cria estilos personalizados para o PDF."""
    base = getSampleStyleSheet()

    estilos = {
        "titulo_capa": ParagraphStyle(
            "TituloCapa",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=28,
            leading=34,
            textColor=COR_PRIMARIA,
            alignment=TA_CENTER,
            spaceAfter=20,
        ),
        "subtitulo_capa": ParagraphStyle(
            "SubtituloCapa",
            parent=base["Title"],
            fontName="Helvetica",
            fontSize=16,
            leading=20,
            textColor=COR_SECUNDARIA,
            alignment=TA_CENTER,
            spaceAfter=10,
        ),
        "data_capa": ParagraphStyle(
            "DataCapa",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=12,
            textColor=COR_CINZA,
            alignment=TA_CENTER,
            spaceBefore=40,
        ),
        "titulo_capitulo": ParagraphStyle(
            "TituloCapitulo",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=26,
            textColor=COR_PRIMARIA,
            spaceBefore=0,
            spaceAfter=16,
            borderWidth=2,
            borderColor=COR_DESTAQUE,
            borderPadding=8,
        ),
        "titulo_secao": ParagraphStyle(
            "TituloSecao",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            leading=18,
            textColor=COR_SECUNDARIA,
            spaceBefore=16,
            spaceAfter=8,
        ),
        "corpo": ParagraphStyle(
            "Corpo",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=COR_TEXTO,
            alignment=TA_JUSTIFY,
            spaceBefore=4,
            spaceAfter=4,
        ),
        "corpo_destaque": ParagraphStyle(
            "CorpoDestaque",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=10,
            leading=14,
            textColor=COR_PRIMARIA,
            spaceBefore=4,
            spaceAfter=4,
        ),
        "toc_item": ParagraphStyle(
            "TocItem",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=12,
            leading=20,
            textColor=COR_SECUNDARIA,
            leftIndent=20,
        ),
        "rodape": ParagraphStyle(
            "Rodape",
            fontName="Helvetica",
            fontSize=8,
            textColor=COR_CINZA,
            alignment=TA_CENTER,
        ),
    }
    return estilos


# ═══════════════════════════════════════════════════════════════
#  HEADER / FOOTER
# ═══════════════════════════════════════════════════════════════

def _header_footer(canvas, doc, ano: int):
    """Desenha cabeçalho e rodapé em cada página (exceto capa)."""
    canvas.saveState()
    width, height = A4

    # ── Rodapé ──
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(COR_CINZA)

    # Linha separadora
    canvas.setStrokeColor(colors.HexColor("#CCCCCC"))
    canvas.setLineWidth(0.5)
    canvas.line(2 * cm, 1.5 * cm, width - 2 * cm, 1.5 * cm)

    # Texto rodapé
    canvas.drawString(2 * cm, 1 * cm, f"SCI — TC Copilot  |  Relatório Anual {ano}")
    canvas.drawRightString(
        width - 2 * cm,
        1 * cm,
        f"Página {doc.page}",
    )

    # ── Cabeçalho fino ──
    canvas.setStrokeColor(COR_DESTAQUE)
    canvas.setLineWidth(1)
    canvas.line(2 * cm, height - 1.5 * cm, width - 2 * cm, height - 1.5 * cm)

    canvas.restoreState()


# ═══════════════════════════════════════════════════════════════
#  JSON INTERMEDIÁRIO (persistência cumulativa)
# ═══════════════════════════════════════════════════════════════

def carregar_dados_relatorio(ano: int) -> dict[str, Any]:
    """Carrega dados do relatório já salvos em JSON."""
    caminho = caminho_dados_relatorio(ano)
    if os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Erro ao carregar JSON do relatório: %s", e)
    return {"ano": ano, "meses": {}}


def salvar_dados_relatorio(ano: int, dados: dict[str, Any]):
    """Salva dados do relatório em JSON para persistência."""
    garantir_pasta_relatorios()
    caminho = caminho_dados_relatorio(ano)
    try:
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Erro ao salvar JSON do relatório: %s", e)


def adicionar_mes_ao_relatorio(
    ano: int,
    mes_numero: int,
    secoes: dict[str, str],
) -> dict[str, Any]:
    """
    Adiciona ou atualiza um mês no JSON intermediário.

    Args:
        ano: Ano do relatório
        mes_numero: Número do mês (1-12)
        secoes: Dict com chave = tipo_secao, valor = texto gerado

    Returns:
        Dados completos do relatório atualizado
    """
    dados = carregar_dados_relatorio(ano)
    dados["ano"] = ano

    str_mes = str(mes_numero)
    dados["meses"][str_mes] = {
        "mes_numero": mes_numero,
        "mes_nome": obter_nome_mes(mes_numero, "pt-BR"),
        "gerado_em": datetime.now().isoformat(),
        "secoes": secoes,
    }

    salvar_dados_relatorio(ano, dados)
    return dados


# ═══════════════════════════════════════════════════════════════
#  CONSTRUÇÃO DO PDF
# ═══════════════════════════════════════════════════════════════

def _construir_capa(elements: list, estilos: dict, ano: int, idioma: str):
    """Adiciona capa ao documento."""
    elements.append(Spacer(1, 4 * cm))

    titulo = "Relatório Anual de Custos" if idioma == "pt-BR" else "Annual Cost Report"
    subtitulo = (
        "Stellantis Cost Intelligence — TC Copilot"
    )
    elements.append(Paragraph(titulo, estilos["titulo_capa"]))
    elements.append(Spacer(1, 1 * cm))
    elements.append(Paragraph(subtitulo, estilos["subtitulo_capa"]))
    elements.append(Spacer(1, 2 * cm))

    elements.append(Paragraph(str(ano), ParagraphStyle(
        "AnoCapa",
        fontName="Helvetica-Bold",
        fontSize=48,
        textColor=COR_DESTAQUE,
        alignment=TA_CENTER,
    )))

    elements.append(Spacer(1, 3 * cm))
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    elements.append(Paragraph(
        f"Gerado em: {agora}" if idioma == "pt-BR" else f"Generated: {agora}",
        estilos["data_capa"],
    ))

    elements.append(PageBreak())


def _construir_sumario(
    elements: list,
    estilos: dict,
    dados_relatorio: dict,
    idioma: str,
):
    """Adiciona página de sumário."""
    titulo = "Sumário" if idioma == "pt-BR" else "Table of Contents"
    elements.append(Paragraph(titulo, estilos["titulo_capitulo"]))
    elements.append(Spacer(1, 1 * cm))

    meses_ordenados = sorted(dados_relatorio.get("meses", {}).items(), key=lambda x: int(x[0]))

    for str_mes, info in meses_ordenados:
        mes_nome = info.get("mes_nome", f"Mês {str_mes}")
        cap_label = f"Capítulo {int(str_mes)}" if idioma == "pt-BR" else f"Chapter {int(str_mes)}"
        elements.append(Paragraph(
            f"<b>{cap_label}</b> — {mes_nome}",
            estilos["toc_item"],
        ))

    elements.append(PageBreak())


def _texto_para_paragraphs(texto: str, estilo: ParagraphStyle) -> list:
    """Converte texto multi-linha em lista de Paragraphs."""
    elements = []
    # Prevenir XML inválido em tags de ReportLab
    texto = texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    for linha in texto.split("\n"):
        linha = linha.strip()
        if not linha:
            elements.append(Spacer(1, 4 * mm))
        else:
            elements.append(Paragraph(linha, estilo))
    return elements


def _construir_capitulo_mes(
    elements: list,
    estilos: dict,
    mes_numero: int,
    info_mes: dict,
    idioma: str,
):
    """Adiciona capítulo de um mês ao documento."""
    mes_nome = info_mes.get("mes_nome", f"Mês {mes_numero}")

    # Título do capítulo
    cap_label = f"Capítulo {mes_numero}" if idioma == "pt-BR" else f"Chapter {mes_numero}"
    elements.append(Paragraph(
        f"{cap_label} — {mes_nome}",
        estilos["titulo_capitulo"],
    ))
    elements.append(Spacer(1, 0.5 * cm))

    # Seções do capítulo
    secoes = info_mes.get("secoes", {})
    labels = LABELS.get(idioma, LABELS["pt-BR"])

    secoes_ordem = [
        "volume_completo",
        "comparativos",
        "conclusoes",
    ]

    # Suporte legado: se secoes contém chaves v1, incluí-las também
    legacy_map = {
        "analise_volume": "sec_volume",
        "variacoes_modelo": "sec_variacoes",
        "anomalias": "sec_anomalias",
        "observacoes_finais": "sec_obs_finais",
    }
    for legacy_key in legacy_map:
        if legacy_key in secoes and legacy_key not in secoes_ordem:
            secoes_ordem.append(legacy_key)

    # Mapeamento tipo_secao → label key (LABELS usa prefixo sec_)
    secao_label_map = {
        "volume_completo": "sec_volume_completo",
        "comparativos": "sec_comparativos",
        "conclusoes": "sec_conclusoes",
        # Legado
        "analise_volume": "sec_volume",
        "variacoes_modelo": "sec_variacoes",
        "anomalias": "sec_anomalias",
        "observacoes_finais": "sec_obs_finais",
    }

    for tipo_secao in secoes_ordem:
        texto = secoes.get(tipo_secao, "")
        if not texto:
            continue

        label_key = secao_label_map.get(tipo_secao, tipo_secao)
        titulo_secao = labels.get(label_key, tipo_secao)
        elements.append(Paragraph(titulo_secao, estilos["titulo_secao"]))

        # Adicionar parágrafos do texto
        paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
        elements.extend(paragraphs)
        elements.append(Spacer(1, 0.5 * cm))

    # Seções de Oficina
    oficina_keys = sorted([k for k in secoes if k.startswith("oficina_")])
    for ofc_key in oficina_keys:
        texto = secoes.get(ofc_key, "")
        if not texto:
            continue
        ofc_nome = ofc_key.replace("oficina_", "")
        titulo_template = labels.get("sec_oficina", "🏭 Oficina {oficina}")
        titulo_ofc = titulo_template.format(oficina=ofc_nome)
        elements.append(Paragraph(titulo_ofc, estilos["titulo_secao"]))
        paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
        elements.extend(paragraphs)
        elements.append(Spacer(1, 0.5 * cm))

    elements.append(PageBreak())


def gerar_pdf(ano: int, idioma: str = "pt-BR") -> str:
    """
    Gera (ou regenera) o PDF completo do relatório anual.

    Lê todos os meses previamente salvos no JSON intermediário
    e gera o PDF final com capa, sumário e capítulos.

    Returns:
        Caminho absoluto do PDF gerado.
    """
    garantir_pasta_relatorios()
    dados_relatorio = carregar_dados_relatorio(ano)
    estilos = _criar_estilos()

    pdf_path = str(caminho_relatorio(ano))

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Relatório Anual TC {ano}",
        author="SCI — TC Copilot",
    )

    elements: list = []

    # ── Capa ──
    _construir_capa(elements, estilos, ano, idioma)

    # ── Sumário ──
    _construir_sumario(elements, estilos, dados_relatorio, idioma)

    # ── Capítulos mensais ──
    meses_ordenados = sorted(
        dados_relatorio.get("meses", {}).items(),
        key=lambda x: int(x[0]),
    )

    for str_mes, info_mes in meses_ordenados:
        _construir_capitulo_mes(
            elements,
            estilos,
            int(str_mes),
            info_mes,
            idioma,
        )

    # ── Build ──
    def _on_page(canvas, doc_):
        _header_footer(canvas, doc_, ano)

    def _on_first_page(canvas, doc_):
        pass  # Capa sem header/footer

    try:
        doc.build(
            elements,
            onFirstPage=_on_first_page,
            onLaterPages=_on_page,
        )
        logger.info("PDF gerado: %s", pdf_path)
    except Exception as e:
        logger.error("Erro ao gerar PDF: %s", e)
        raise

    return pdf_path


# ═══════════════════════════════════════════════════════════════
#  GERAR RELATÓRIO COMPLETO PARA UM MÊS
# ═══════════════════════════════════════════════════════════════

def gerar_relatorio_mes(
    ano: int,
    mes_numero: int,
    api_key: str | None,
    modelo: str = "gpt-4o-mini",
    idioma: str = "pt-BR",
) -> str:
    """
    Pipeline completo: coleta dados → gera texto LLM → salva JSON → gera PDF.

    Args:
        ano: Ano do relatório
        mes_numero: Mês a gerar (1-12)
        api_key: Chave OpenAI (pode ser None → fallback sem LLM)
        modelo: Modelo LLM
        idioma: 'pt-BR' ou 'en'

    Returns:
        Caminho do PDF gerado
    """
    from tc_copilot.data_collector import (
        calcular_variacoes,
        coletar_dados_mes,
        descobrir_oficinas,
        formatar_dados_comparativos_agrupado,
        formatar_dados_conclusoes,
        formatar_dados_oficina,
        formatar_dados_volume_completo,
    )
    from tc_copilot.llm_integration import gerar_secao_relatorio

    # 1. Coletar dados
    dados = coletar_dados_mes(ano, mes_numero)
    variacoes = calcular_variacoes(dados)

    mes_nome = dados["mes_nome"]
    ano_anterior = dados["ano_anterior"]

    # 2. Preparar dados formatados por seção (3 seções globais v2)
    dados_por_secao = {
        "volume_completo": formatar_dados_volume_completo(dados, variacoes),
        "comparativos": formatar_dados_comparativos_agrupado(dados, variacoes),
        "conclusoes": formatar_dados_conclusoes(dados, variacoes),
    }

    # 3. Gerar texto de cada seção via LLM (ou fallback)
    secoes_geradas = {}
    for tipo_secao, dados_formatados in dados_por_secao.items():
        secoes_geradas[tipo_secao] = gerar_secao_relatorio(
            tipo_secao=tipo_secao,
            dados_formatados=dados_formatados,
            mes=mes_nome,
            ano=ano,
            idioma=idioma,
            api_key=api_key,
            model=modelo,
            ano_anterior=ano_anterior,
        )

    # 4. Gerar seções por oficina
    oficinas = descobrir_oficinas(dados)
    for ofc in oficinas:
        dados_ofc_dict = formatar_dados_oficina(dados, variacoes, ofc)
        # formatar_dados_oficina retorna dict; LLM recebe texto_completo
        dados_ofc_texto = (
            dados_ofc_dict["texto_completo"]
            if isinstance(dados_ofc_dict, dict)
            else str(dados_ofc_dict)
        )
        chave = f"oficina_{ofc}"
        secoes_geradas[chave] = gerar_secao_relatorio(
            tipo_secao="oficina",
            dados_formatados=dados_ofc_texto,
            mes=mes_nome,
            ano=ano,
            idioma=idioma,
            api_key=api_key,
            model=modelo,
            ano_anterior=ano_anterior,
            oficina=ofc,
        )

    # 5. Salvar no JSON intermediário
    adicionar_mes_ao_relatorio(ano, mes_numero, secoes_geradas)

    # 6. Gerar PDF completo (cumulativo)
    return gerar_pdf(ano, idioma)


def meses_ja_gerados(ano: int) -> list[int]:
    """Retorna lista de meses já gerados no relatório."""
    dados = carregar_dados_relatorio(ano)
    return sorted(int(m) for m in dados.get("meses", {}).keys())
