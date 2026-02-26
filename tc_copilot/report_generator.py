"""
TC Copilot — Geração de PDF com ReportLab.

Cria relatórios anuais com capa, sumário, capítulos mensais e rodapé.
Usa JSON intermediário para persistência cumulativa.
"""

from __future__ import annotations

import json
import logging
import os
import re as _re
from datetime import datetime
from io import BytesIO as _BytesIO
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
    Image,
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
    ROOT,
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
    dados_graficos: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Adiciona ou atualiza um mês no JSON intermediário.

    Args:
        ano: Ano do relatório
        mes_numero: Número do mês (1-12)
        secoes: Dict com chave = tipo_secao, valor = texto gerado
        dados_graficos: Dados numéricos para geração de gráficos waterfall.
            Estrutura esperada::

                {
                    "global": {"fp_bud": ..., "fp_flex": ..., "fp_real": ...,
                               "fp_mes_anterior": ..., "fp_ano_anterior": ...,
                               "ano_anterior": ...},
                    "oficinas": {
                        "Compras": {"fp_bud": ..., "fp_flex": ..., "fp_real": ...},
                        ...
                    }
                }

    Returns:
        Dados completos do relatório atualizado
    """
    dados = carregar_dados_relatorio(ano)
    dados["ano"] = ano

    str_mes = str(mes_numero)
    entry: dict[str, Any] = {
        "mes_numero": mes_numero,
        "mes_nome": obter_nome_mes(mes_numero, "pt-BR"),
        "gerado_em": datetime.now().isoformat(),
        "secoes": secoes,
    }
    if dados_graficos:
        entry["dados_graficos"] = dados_graficos

    dados["meses"][str_mes] = entry

    salvar_dados_relatorio(ano, dados)
    return dados


# ═══════════════════════════════════════════════════════════════
#  CONSTRUÇÃO DO PDF
# ═══════════════════════════════════════════════════════════════

def _construir_capa(elements: list, estilos: dict, ano: int, idioma: str):
    """Adiciona capa ao documento com logo SCI_faixa abaixo da data."""

    elements.append(Spacer(1, 4 * cm))

    # ── Título e subtítulo ────────────────────────────────────
    titulo = "Relatório Anual de Custos" if idioma == "pt-BR" else "Annual Cost Report"
    subtitulo = "Stellantis Cost Intelligence — TC Copilot"
    elements.append(Paragraph(titulo, estilos["titulo_capa"]))
    elements.append(Spacer(1, 0.8 * cm))
    elements.append(Paragraph(subtitulo, estilos["subtitulo_capa"]))
    elements.append(Spacer(1, 1.5 * cm))

    # ── Ano em destaque ───────────────────────────────────────
    elements.append(Paragraph(str(ano), ParagraphStyle(
        "AnoCapa",
        fontName="Helvetica-Bold",
        fontSize=48,
        textColor=COR_DESTAQUE,
        alignment=TA_CENTER,
    )))
    elements.append(Spacer(1, 1.5 * cm))

    # ── Data de geração ───────────────────────────────────────
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    elements.append(Paragraph(
        f"Gerado em: {agora}" if idioma == "pt-BR" else f"Generated: {agora}",
        estilos["data_capa"],
    ))
    elements.append(Spacer(1, 1.5 * cm))

    # ── Logo SCI (faixa) abaixo da data ─────────────────────
    # SCI_faixa.png (1240×457) → 14 cm de largura, proporcional
    logo_faixa = ROOT / "SCI_faixa.png"
    if logo_faixa.exists():
        img_w = 14 * cm
        img_h = img_w * (457 / 1240)
        elements.append(Image(str(logo_faixa), width=img_w, height=img_h))

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


# ═══════════════════════════════════════════════════════════════
#  EMOJIS → TEXTO (ReportLab não renderiza emojis Unicode)
# ═══════════════════════════════════════════════════════════════
import re as _re

_EMOJI_MAP: dict[str, str] = {
    # ── Indicadores de cor → bullets coloridos (via _COLOR_MAP) ──
    "\U0001f534": "__CLR_RED__",     # 🔴
    "\U0001f7e2": "__CLR_GREEN__",   # 🟢
    "\U0001f7e1": "__CLR_YELLOW__",  # 🟡
    # ── Setas → caracteres ASCII que o ReportLab renderiza ──
    "\u2b06":     "\u2191",          # ⬆ → ↑
    "\u2b07":     "\u2193",          # ⬇ → ↓
    "\u27a1":     "\u2192",          # ➡ → →
    # ── Checks / alertas → símbolos simples ──
    "\u2705":     "\u2713 ",         # ✅ → ✓
    "\u274c":     "x ",              # ❌ → x
    "\u26a0":     "(!)",             # ⚠  → (!)
    # ── Todos os demais emojis decorativos → removidos (string vazia) ──
    "\U0001f4ca": "",   # 📊
    "\U0001f4c8": "",   # 📈
    "\U0001f4a1": "",   # 💡
    "\U0001f3ed": "",   # 🏭
    "\U0001f6e0": "",   # 🛠
    "\U0001f4b0": "",   # 💰
    "\U0001f4b5": "",   # 💵
    "\U0001f4c9": "",   # 📉
    "\U0001f4c5": "",   # 📅
    "\U0001f4cb": "",   # 📋
    "\U0001f4dd": "",   # 📝
    "\U0001f50d": "",   # 🔍
    "\U0001f680": "",   # 🚀
    "\U0001f3af": "",   # 🎯
    "\U0001f4e6": "",   # 📦
    "\U0001f4e2": "",   # 📢
    "\U0001f4b2": "",   # 💲
    "\U0001f4c4": "",   # 📄
    "\U0001f527": "",   # 🔧
    "\U0001f6a8": "",   # 🚨
    "\U0001f91d": "",   # 🤝
    "\u2139":     "",   # ℹ
}

# Regex que captura qualquer emoji Unicode (blocos emoji comuns)
_EMOJI_RE = _re.compile(
    "["
    "\U0001F300-\U0001F9FF"  # Miscellaneous Symbols, Emoticons, etc.
    "\U00002702-\U000027B0"  # Dingbats
    "\U0000FE00-\U0000FE0F"  # Variation Selectors
    "\U0000200D"             # ZWJ
    "\U000025A0-\U000025FF"  # Geometric shapes
    "\U00002600-\U000026FF"  # Misc symbols
    "\U00002B05-\U00002B07"  # Arrows
    "\U00002B1B-\U00002B1C"  # Squares
    "\U00002934-\U00002935"  # Arrows
    "\U00003030"             # Wavy dash
    "\U0000303D"             # Part alternation mark
    "\U00003297"             # Circled Ideograph Congratulation
    "\U00003299"             # Circled Ideograph Secret
    "]+",
    flags=_re.UNICODE,
)


def _substituir_emojis(texto: str) -> str:
    """Substitui emojis por equivalentes textuais para o PDF.

    Primeiro tenta o mapa conhecido; depois remove qualquer emoji restante.
    Não altera o texto original no sistema — só o PDF."""
    for emoji_char, label in _EMOJI_MAP.items():
        texto = texto.replace(emoji_char, label)
    # Remover emojis remanescentes nao mapeados
    texto = _EMOJI_RE.sub("", texto)
    return texto


# ── Mapa de placeholders de cor → HTML colorido do ReportLab ──
_COLOR_MAP: dict[str, str] = {
    "__CLR_RED__":    '<font color="#CC0000" size="12">\u25CF</font>',
    "__CLR_GREEN__":  '<font color="#228B22" size="12">\u25CF</font>',
    "__CLR_YELLOW__": '<font color="#DAA520" size="12">\u25CF</font>',
}

# Placeholders de cor para detecção (evitar bullet duplicado)
_CLR_PLACEHOLDERS = ("__CLR_RED__", "__CLR_GREEN__", "__CLR_YELLOW__")


def _aplicar_formatacao(texto: str) -> str:
    """Converte placeholders de cor em tags <font> e Markdown básico em HTML.

    Deve ser chamada DEPOIS do XML-escape para que as tags HTML
    inseridas aqui não sejam escapadas."""
    # Placeholders de cor → <font color="...">●</font>
    for placeholder, html in _COLOR_MAP.items():
        texto = texto.replace(placeholder, html)
    # **negrito** → <b>negrito</b>
    texto = _re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', texto)
    return texto


def _texto_para_paragraphs(texto: str, estilo: ParagraphStyle) -> list:
    """Converte texto multi-linha em Paragraphs com cores, bold e bullets."""
    elements: list = []
    # 1. Substituir emojis (inclui placeholders de cor)
    texto = _substituir_emojis(texto)
    # 2. XML-escape (protege & < >)
    texto = texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Estilos derivados para bullets e headings
    estilo_bullet = ParagraphStyle(
        "CorpoBullet", parent=estilo, leftIndent=15,
    )
    estilo_sub_bullet = ParagraphStyle(
        "CorpoSubBullet", parent=estilo, leftIndent=30,
    )
    estilo_heading = ParagraphStyle(
        "CorpoHeading", parent=estilo,
        fontName="Helvetica-Bold",
        fontSize=estilo.fontSize + 2,
        textColor=COR_SECUNDARIA,
        spaceBefore=10, spaceAfter=6,
    )

    for linha in texto.split("\n"):
        indent = len(linha) - len(linha.lstrip())
        linha_strip = linha.strip()
        if not linha_strip:
            elements.append(Spacer(1, 4 * mm))
            continue

        # 3. Aplicar cores e Markdown (pós-escape)
        linha_fmt = _aplicar_formatacao(linha_strip)

        # ### heading → negrito com estilo de sub-seção
        heading_m = _re.match(r'^#{1,6}\s+(.+)', linha_fmt)
        if heading_m:
            elements.append(Paragraph(
                f"<b>{heading_m.group(1)}</b>", estilo_heading,
            ))
        # Bullet point (com sub-nível por indentação)
        elif linha_strip.startswith("- "):
            corpo = linha_fmt[2:]
            # Se corpo já tem indicador colorido (●), não prefixar outro bullet
            has_color = any(p in linha_strip for p in _CLR_PLACEHOLDERS) or '<font color=' in corpo
            prefixo = "" if has_color else "\u2022 "
            if indent >= 2:
                elements.append(Paragraph(f"{prefixo}{corpo}", estilo_sub_bullet))
            else:
                elements.append(Paragraph(f"{prefixo}{corpo}", estilo_bullet))
        else:
            elements.append(Paragraph(linha_fmt, estilo))

    return elements


def _inserir_grafico(
    elements: list,
    png_bytes: bytes | None,
    largura_max: float = 14 * cm,
) -> None:
    """Insere imagem PNG (bytes) como flowable no PDF, centralizada."""
    if not png_bytes:
        return
    buf = _BytesIO(png_bytes)
    img = Image(buf)
    # Escalar para caber na largura da página mantendo proporção
    ratio = img.imageWidth / img.imageHeight if img.imageHeight else 1
    img_width = min(largura_max, img.imageWidth)
    img_height = img_width / ratio
    img.drawWidth = img_width
    img.drawHeight = img_height
    img.hAlign = "CENTER"
    elements.append(img)
    elements.append(Spacer(1, 0.3 * cm))


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

    # ── Dados de gráficos (armazenados no JSON) ──
    dados_graf = info_mes.get("dados_graficos", {})
    graf_global = dados_graf.get("global", {})
    graf_oficinas = dados_graf.get("oficinas", {})

    # Seções do capítulo
    secoes = info_mes.get("secoes", {})
    labels = LABELS.get(idioma, LABELS["pt-BR"])

    secoes_ordem = [
        "resumo_executivo",
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
        "resumo_executivo": "sec_resumo_executivo",
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
        titulo_secao = _aplicar_formatacao(_substituir_emojis(labels.get(label_key, tipo_secao)))
        elements.append(Paragraph(titulo_secao, estilos["titulo_secao"]))

        # ── Inserir gráficos waterfall na seção Comparativos ──
        if tipo_secao == "comparativos" and graf_global:
            _inserir_graficos_comparativos(elements, graf_global, mes_nome, info_mes)

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
        titulo_ofc = _aplicar_formatacao(_substituir_emojis(titulo_template.format(oficina=ofc_nome)))
        elements.append(Paragraph(titulo_ofc, estilos["titulo_secao"]))

        # ── Inserir gráfico waterfall da oficina ──
        if ofc_nome in graf_oficinas:
            _inserir_grafico_oficina(elements, graf_oficinas[ofc_nome], ofc_nome, mes_nome, info_mes)

        paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
        elements.extend(paragraphs)
        elements.append(Spacer(1, 0.5 * cm))

    elements.append(PageBreak())


def _inserir_graficos_comparativos(
    elements: list,
    graf_global: dict,
    mes_nome: str,
    info_mes: dict,
) -> None:
    """Gera e insere os gráficos waterfall completos (CPU, com todas as categorias)."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        logger.warning("chart_generator não disponível — gráficos omitidos.")
        return

    ano_rel = graf_global.get("ano", "")

    # 2.1 Waterfall Budget completo (BUD → Flex → categorias → Real)
    wf_bud_labels = graf_global.get("wf_budget_labels", [])
    wf_bud_values = graf_global.get("wf_budget_values", [])
    if wf_bud_labels and len(wf_bud_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_bud_labels, "values": wf_bud_values},
            titulo=f"Waterfall Budget — CPU (R$/veíc) — {mes_nome}/{ano_rel}",
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)

    # 2.2 Waterfall Mensal completo (Mês Ant → Flex → categorias → Real)
    wf_men_labels = graf_global.get("wf_mensal_labels", [])
    wf_men_values = graf_global.get("wf_mensal_values", [])
    if wf_men_labels and len(wf_men_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_men_labels, "values": wf_men_values},
            titulo=f"Waterfall Mensal — CPU (R$/veíc) — {mes_nome}/{ano_rel}",
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)


def _inserir_grafico_oficina(
    elements: list,
    graf_ofc: dict,
    ofc_nome: str,
    mes_nome: str,
    info_mes: dict,
) -> None:
    """Gera e insere gráfico waterfall Budget completo (CPU) para uma oficina."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    ano_rel = graf_ofc.get("ano", info_mes.get("dados_graficos", {}).get("global", {}).get("ano", ""))
    wf_labels = graf_ofc.get("wf_budget_labels", [])
    wf_values = graf_ofc.get("wf_budget_values", [])

    if wf_labels and len(wf_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_labels, "values": wf_values},
            titulo=f"Waterfall Budget — {ofc_nome} — CPU (R$/veíc) — {mes_nome}/{ano_rel}",
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)


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
        _filtrar_por_oficina,
        calcular_variacoes,
        coletar_dados_mes,
        descobrir_oficinas,
        formatar_dados_comparativos_agrupado,
        formatar_dados_conclusoes,
        formatar_dados_oficina,
        formatar_dados_resumo_executivo,
        formatar_dados_volume_completo,
    )
    from tc_copilot.llm_integration import gerar_secao_relatorio

    # 1. Coletar dados
    dados = coletar_dados_mes(ano, mes_numero)
    variacoes = calcular_variacoes(dados)

    mes_nome = dados["mes_nome"]
    ano_anterior = dados["ano_anterior"]

    # 2. Preparar dados formatados por seção (3 seções globais v2 + resumo exec)
    dados_por_secao = {
        "volume_completo": formatar_dados_volume_completo(dados, variacoes),
        "comparativos": formatar_dados_comparativos_agrupado(dados, variacoes),
        "conclusoes": formatar_dados_conclusoes(dados, variacoes),
    }

    # Dados para o resumo executivo (compilado a partir de todos)
    dados_resumo = formatar_dados_resumo_executivo(dados, variacoes)

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

    # 5. Gerar Resumo Executivo via LLM (APÓS todas as seções, pois resume tudo)
    secoes_geradas["resumo_executivo"] = gerar_secao_relatorio(
        tipo_secao="resumo_executivo",
        dados_formatados=dados_resumo,
        mes=mes_nome,
        ano=ano,
        idioma=idioma,
        api_key=api_key,
        model=modelo,
        ano_anterior=ano_anterior,
    )

    # 5b. Preparar dados numéricos para gráficos waterfall (CPU — R$/veíc)
    from tc_copilot.chart_generator import (
        calcular_waterfall_budget_cpu,
        calcular_waterfall_mensal_cpu,
    )

    # Waterfall global — Budget (BUD → Flex → categorias → Real)
    wf_budget = calcular_waterfall_budget_cpu(
        custo_real=dados.get("custo_real"),
        custo_bud=dados.get("custo_bud"),
        vol_real=dados.get("volume_real"),
        vol_bud=dados.get("volume_bud"),
    )

    # Waterfall global — Mensal (Mês Ant → Flex → categorias → Real)
    wf_mensal = calcular_waterfall_mensal_cpu(
        custo_real=dados.get("custo_real"),
        custo_ant=dados.get("custo_real_ant"),
        vol_real=dados.get("volume_real"),
        vol_ant=dados.get("volume_real_ant"),
        label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
        label_real=mes_nome,
    )

    dados_graficos: dict[str, Any] = {
        "global": {
            "wf_budget_labels": wf_budget.get("labels", []),
            "wf_budget_values": [float(v) for v in wf_budget.get("values", [])],
            "wf_mensal_labels": wf_mensal.get("labels", []),
            "wf_mensal_values": [float(v) for v in wf_mensal.get("values", [])],
            "ano": ano,
            "ano_anterior": ano_anterior,
        },
        "oficinas": {},
    }
    for ofc in oficinas:
        try:
            dados_ofc = _filtrar_por_oficina(dados, ofc)
            # Waterfall Budget por oficina
            wf_ofc_budget = calcular_waterfall_budget_cpu(
                custo_real=dados_ofc.get("custo_real"),
                custo_bud=dados_ofc.get("custo_bud"),
                vol_real=dados.get("volume_real"),
                vol_bud=dados.get("volume_bud"),
            )
            dados_graficos["oficinas"][ofc] = {
                "wf_budget_labels": wf_ofc_budget.get("labels", []),
                "wf_budget_values": [float(v) for v in wf_ofc_budget.get("values", [])],
                "ano": ano,
            }
        except Exception as e:
            logger.warning("Falha ao coletar dados de gráfico para oficina %s: %s", ofc, e)

    # 6. Salvar no JSON intermediário (inclui dados de gráficos)
    adicionar_mes_ao_relatorio(ano, mes_numero, secoes_geradas, dados_graficos)

    # 7. Gerar PDF completo (cumulativo)
    return gerar_pdf(ano, idioma)


def meses_ja_gerados(ano: int) -> list[int]:
    """Retorna lista de meses já gerados no relatório."""
    dados = carregar_dados_relatorio(ano)
    return sorted(int(m) for m in dados.get("meses", {}).keys())
