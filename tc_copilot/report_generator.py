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
    caminho_dados_relatorio_local,
    caminho_relatorio,
    caminho_relatorio_local,
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
    simbolo_moeda: str = "R$",
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

        # ── Volume: texto + gráfico Budget ──
        if tipo_secao == "volume_completo":
            paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
            elements.extend(paragraphs)
            if graf_global:
                _inserir_waterfall_budget(elements, graf_global, mes_nome, info_mes, simbolo_moeda)

        # ── Comparativos: interleavar gráficos nos sub-tópicos ──
        elif tipo_secao == "comparativos":
            _renderizar_comparativos_pdf(
                elements, estilos, texto, graf_global, mes_nome, info_mes, simbolo_moeda,
            )

        else:
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

        graf_ofc = graf_oficinas.get(ofc_nome, {})

        # Interleavar gráficos waterfall nos sub-tópicos da oficina
        if "<!-- SPLIT -->" in texto and graf_ofc:
            import re as _re_ofc
            blocos_ofc = [b.strip() for b in texto.split("<!-- SPLIT -->") if b.strip()]
            for idx_b, bloco_ofc in enumerate(blocos_ofc):
                # Separar título (1ª linha) do corpo
                _lo = bloco_ofc.split("\n", 1)
                titulo_ofc_linha = _lo[0].strip()
                corpo_ofc = _lo[1].strip() if len(_lo) > 1 else ""

                # 1) Título
                elements.extend(_texto_para_paragraphs(titulo_ofc_linha, estilos["corpo"]))
                # 2) Waterfall: 1o bloco → budget, 2o → mensal, 3o → ano_anterior
                _tipos_ofc = ["budget", "mensal", "ano_anterior"]
                if idx_b < len(_tipos_ofc):
                    _inserir_grafico_oficina(
                        elements, graf_ofc, ofc_nome, mes_nome, info_mes,
                        simbolo_moeda, tipo_waterfall=_tipos_ofc[idx_b],
                    )
                # 3) Corpo do texto
                if corpo_ofc:
                    paragraphs = _texto_para_paragraphs(corpo_ofc, estilos["corpo"])
                    elements.extend(paragraphs)
        else:
            # Fallback: um único gráfico budget antes do texto
            if graf_ofc:
                _inserir_grafico_oficina(
                    elements, graf_ofc, ofc_nome, mes_nome, info_mes,
                    simbolo_moeda, tipo_waterfall="budget",
                )
            paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
            elements.extend(paragraphs)

        elements.append(Spacer(1, 0.5 * cm))

    elements.append(PageBreak())


def _renderizar_comparativos_pdf(
    elements: list,
    estilos: dict,
    texto: str,
    graf_global: dict | None,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Renderiza seção Comparativos no PDF intercalando gráficos nos sub-tópicos.

    - Após bloco 2.1 → chart waterfall Budget
    - Após bloco 2.2 → chart waterfall Mensal
    """
    import re

    # Separar sub-seções pelo marcador (compatível com textos legados)
    if "<!-- SPLIT -->" in texto:
        blocos = [b.strip() for b in texto.split("<!-- SPLIT -->") if b.strip()]
    else:
        blocos = [b.strip() for b in re.split(r"(?=### 2\.)", texto) if b.strip()]

    for bloco in blocos:
        # Separar título (1ª linha) do corpo analítico
        _linhas = bloco.split("\n", 1)
        titulo_linha = _linhas[0].strip()
        corpo = _linhas[1].strip() if len(_linhas) > 1 else ""

        # 1) Renderizar título do sub-tópico
        elements.extend(_texto_para_paragraphs(titulo_linha, estilos["corpo"]))

        # 2) Inserir gráfico logo abaixo do título
        _tl = titulo_linha.lstrip("# ")
        if graf_global:
            if _tl.startswith("2.1") or _tl.startswith("**2.1"):
                _inserir_waterfall_budget(elements, graf_global, mes_nome, info_mes, simbolo_moeda)
            elif _tl.startswith("2.2") or _tl.startswith("**2.2"):
                _inserir_waterfall_mensal(elements, graf_global, mes_nome, info_mes, simbolo_moeda)
            elif _tl.startswith("2.3") or _tl.startswith("**2.3"):
                _inserir_waterfall_ano_anterior(elements, graf_global, mes_nome, info_mes, simbolo_moeda)

        # 3) Renderizar corpo do texto
        if corpo:
            paragraphs = _texto_para_paragraphs(corpo, estilos["corpo"])
            elements.extend(paragraphs)

        elements.append(Spacer(1, 0.3 * cm))


def _inserir_waterfall_budget(
    elements: list,
    graf_global: dict,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Insere gráfico waterfall Budget (CPU) no PDF."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"
    ano_rel = graf_global.get("ano", "")
    wf_bud_labels = graf_global.get("wf_budget_labels", [])
    wf_bud_values = graf_global.get("wf_budget_values", [])
    if wf_bud_labels and len(wf_bud_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_bud_labels, "values": wf_bud_values},
            titulo=f"Waterfall Budget — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
            y_label=cpu_label,
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)


def _inserir_waterfall_mensal(
    elements: list,
    graf_global: dict,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Insere gráfico waterfall Mensal (CPU) no PDF."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"
    ano_rel = graf_global.get("ano", "")
    wf_men_labels = graf_global.get("wf_mensal_labels", [])
    wf_men_values = graf_global.get("wf_mensal_values", [])
    if wf_men_labels and len(wf_men_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_men_labels, "values": wf_men_values},
            titulo=f"Waterfall Mensal — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
            y_label=cpu_label,
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)


def _inserir_waterfall_ano_anterior(
    elements: list,
    graf_global: dict,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Insere gráfico waterfall Ano Anterior (YoY, CPU) no PDF."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"
    ano_rel = graf_global.get("ano", "")
    ano_ant_rel = graf_global.get("ano_anterior", "")
    wf_aa_labels = graf_global.get("wf_ano_ant_labels", [])
    wf_aa_values = graf_global.get("wf_ano_ant_values", [])
    if wf_aa_labels and len(wf_aa_labels) >= 3:
        png = gerar_waterfall_from_arrays(
            {"labels": wf_aa_labels, "values": wf_aa_values},
            titulo=f"Waterfall Ano Anterior — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
            y_label=cpu_label,
        )
        _inserir_grafico(elements, png, largura_max=17 * cm)


def _inserir_grafico_oficina(
    elements: list,
    graf_ofc: dict,
    ofc_nome: str,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
    tipo_waterfall: str = "ambos",
) -> None:
    """Gera e insere gráficos waterfall (CPU) para uma oficina.

    tipo_waterfall: "budget", "mensal", "ano_anterior" ou "ambos".
    """
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"
    ano_rel = graf_ofc.get("ano", info_mes.get("dados_graficos", {}).get("global", {}).get("ano", ""))
    ano_ant_rel = graf_ofc.get("ano_anterior", "")

    # Budget
    if tipo_waterfall in ("budget", "ambos"):
        wf_labels = graf_ofc.get("wf_budget_labels", [])
        wf_values = graf_ofc.get("wf_budget_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Waterfall Budget — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                y_label=cpu_label,
            )
            _inserir_grafico(elements, png, largura_max=17 * cm)

    # Mensal
    if tipo_waterfall in ("mensal", "ambos"):
        wf_labels = graf_ofc.get("wf_mensal_labels", [])
        wf_values = graf_ofc.get("wf_mensal_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Waterfall Mensal — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                y_label=cpu_label,
            )
            _inserir_grafico(elements, png, largura_max=17 * cm)

    # Ano Anterior (YoY)
    if tipo_waterfall in ("ano_anterior", "ambos"):
        wf_labels = graf_ofc.get("wf_ano_ant_labels", [])
        wf_values = graf_ofc.get("wf_ano_ant_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Waterfall Ano Anterior — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
                y_label=cpu_label,
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
        "comparativos": "\n\n".join(formatar_dados_comparativos_agrupado(dados, variacoes).values()),
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


# ═══════════════════════════════════════════════════════════════
#  RELATÓRIO LOCAL (SEM API) — JSON + PDF separados
# ═══════════════════════════════════════════════════════════════

def carregar_dados_relatorio_local(ano: int) -> dict[str, Any]:
    """Carrega dados do relatório LOCAL já salvos em JSON."""
    caminho = caminho_dados_relatorio_local(ano)
    if os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Erro ao carregar JSON local: %s", e)
    return {"ano": ano, "modo": "local", "meses": {}}


def salvar_dados_relatorio_local(ano: int, dados: dict[str, Any]):
    """Salva dados do relatório LOCAL em JSON."""
    garantir_pasta_relatorios()
    caminho = caminho_dados_relatorio_local(ano)
    try:
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Erro ao salvar JSON local: %s", e)


def adicionar_mes_ao_relatorio_local(
    ano: int,
    mes_numero: int,
    secoes: dict[str, str],
    dados_graficos: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Adiciona/atualiza um mês no JSON do relatório LOCAL."""
    dados = carregar_dados_relatorio_local(ano)
    dados["ano"] = ano
    dados["modo"] = "local"

    str_mes = str(mes_numero)
    entry: dict[str, Any] = {
        "mes_numero": mes_numero,
        "mes_nome": obter_nome_mes(mes_numero, "pt-BR"),
        "gerado_em": datetime.now().isoformat(),
        "modo": "local",
        "secoes": secoes,
    }
    if dados_graficos:
        entry["dados_graficos"] = dados_graficos

    dados["meses"][str_mes] = entry
    salvar_dados_relatorio_local(ano, dados)
    return dados


def gerar_pdf_local(ano: int, idioma: str = "pt-BR", simbolo_moeda: str = "R$") -> str:
    """
    Gera PDF do relatório LOCAL (sem API).
    Mesma estrutura do gerar_pdf() mas usa JSON e caminho separados.
    """
    garantir_pasta_relatorios()
    dados_relatorio = carregar_dados_relatorio_local(ano)
    estilos = _criar_estilos()

    pdf_path = str(caminho_relatorio_local(ano))

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Relatório Anual TC {ano} (Automático)",
        author="SCI — TC Copilot (modo local)",
    )

    elements: list = []

    # Capa, sumário e capítulos — reutiliza as mesmas funções
    _construir_capa(elements, estilos, ano, idioma)
    _construir_sumario(elements, estilos, dados_relatorio, idioma)

    meses_ordenados = sorted(
        dados_relatorio.get("meses", {}).items(),
        key=lambda x: int(x[0]),
    )
    for str_mes, info_mes in meses_ordenados:
        _construir_capitulo_mes(elements, estilos, int(str_mes), info_mes, idioma, simbolo_moeda)

    def _on_page(canvas, doc_):
        _header_footer(canvas, doc_, ano)

    def _on_first_page(canvas, doc_):
        pass

    try:
        doc.build(elements, onFirstPage=_on_first_page, onLaterPages=_on_page)
        logger.info("PDF local gerado: %s", pdf_path)
    except Exception as e:
        logger.error("Erro ao gerar PDF local: %s", e)
        raise

    return pdf_path


def meses_ja_gerados_local(ano: int) -> list[int]:
    """Retorna lista de meses já gerados no relatório LOCAL."""
    dados = carregar_dados_relatorio_local(ano)
    return sorted(int(m) for m in dados.get("meses", {}).keys())


def gerar_relatorio_mes_local(
    ano: int,
    mes_numero: int,
    idioma: str = "pt-BR",
    moeda: str = "BRL",
    taxas: dict[str, float] | None = None,
) -> str:
    """
    Pipeline completo SEM API: coleta dados → gera texto via templates → JSON → PDF.

    Não requer chave OpenAI. Textos gerados por text_templates.py.

    Args:
        moeda: Código da moeda para o relatório (BRL, USD, EUR).
        taxas: Dict com taxas multiplicativas {\"USD\": 0.20, \"EUR\": 0.18} (1 BRL → X moeda).

    Returns:
        Caminho do PDF gerado.
    """
    from tc_copilot.data_collector import (
        _filtrar_por_oficina,
        calcular_variacoes,
        coletar_dados_mes,
        descobrir_oficinas,
        formatar_dados_oficina,
    )
    from tc_copilot.text_templates import gerar_todas_secoes_local
    from tc_core.finance.currency import converter_coluna_moeda, obter_simbolo_moeda

    if taxas is None:
        taxas = {}
    simbolo = obter_simbolo_moeda(moeda)

    # 1. Coletar dados
    dados = coletar_dados_mes(ano, mes_numero)

    # 1b. Converter custos para moeda selecionada (se != BRL)
    if moeda != "BRL":
        _colunas_custo = ["Custo FP", "Custo MP", "Custo Log.", "Custo Emb.",
                          "Custo Total", "Amort. Fer.", "Amort. Eng.",
                          "Delta Volume", "Delta Mix"]
        for chave_df in ("custo_real", "custo_bud", "custo_real_ant", "custo_bud_ant",
                         "custo_real_ano_ant", "custo_bud_ano_ant"):
            df = dados.get(chave_df)
            if df is not None and not df.empty:
                for col in _colunas_custo:
                    dados[chave_df] = converter_coluna_moeda(
                        dados[chave_df], col, moeda, taxas,
                    )

    variacoes = calcular_variacoes(dados)

    mes_nome = dados["mes_nome"]

    # 2. Preparar dados de oficinas
    oficinas = descobrir_oficinas(dados)
    oficinas_info: dict[str, dict] = {}
    for ofc in oficinas:
        oficinas_info[ofc] = formatar_dados_oficina(dados, variacoes, ofc)

    # 3. Gerar gráficos waterfall (CPU) — mesma lógica do relatório com API
    from tc_copilot.chart_generator import (
        calcular_waterfall_budget_cpu,
        calcular_waterfall_mensal_cpu,
    )

    wf_budget = calcular_waterfall_budget_cpu(
        custo_real=dados.get("custo_real"),
        custo_bud=dados.get("custo_bud"),
        vol_real=dados.get("volume_real"),
        vol_bud=dados.get("volume_bud"),
    )

    # Só gera waterfall mensal se houver dados de mês anterior
    sem_mes_anterior = variacoes.get("sem_mes_anterior", False)
    if not sem_mes_anterior:
        wf_mensal = calcular_waterfall_mensal_cpu(
            custo_real=dados.get("custo_real"),
            custo_ant=dados.get("custo_real_ant"),
            vol_real=dados.get("volume_real"),
            vol_ant=dados.get("volume_real_ant"),
            label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
            label_real=mes_nome,
        )
    else:
        wf_mensal = {}

    # Waterfall Ano Anterior (YoY) — filtrar historico_custo/vol pelo mesmo mês do ano anterior
    sem_ano_anterior = variacoes.get("sem_ano_anterior", False)
    wf_ano_ant: dict[str, Any] = {}
    if not sem_ano_anterior:
        import pandas as _pd
        hist_custo = dados.get("historico_custo")
        hist_vol = dados.get("historico_vol")
        _ano_ant = dados.get("ano_anterior", ano - 1)
        if hist_custo is not None and not hist_custo.empty:
            _mask_c = _pd.Series(True, index=hist_custo.index)
            if "Ano" in hist_custo.columns:
                _mask_c = _mask_c & (hist_custo["Ano"] == _ano_ant)
            if "Período" in hist_custo.columns:
                _mask_c = _mask_c & (hist_custo["Período"] == mes_nome)
            custo_ano_ant = hist_custo.loc[_mask_c].copy() if _mask_c.any() else None
        else:
            custo_ano_ant = None
        if hist_vol is not None and not hist_vol.empty:
            _mask_v = _pd.Series(True, index=hist_vol.index)
            if "Ano" in hist_vol.columns:
                _mask_v = _mask_v & (hist_vol["Ano"] == _ano_ant)
            if "Período" in hist_vol.columns:
                _mask_v = _mask_v & (hist_vol["Período"] == mes_nome)
            vol_ano_ant = hist_vol.loc[_mask_v].copy() if _mask_v.any() else None
        else:
            vol_ano_ant = None
        if custo_ano_ant is not None and not custo_ano_ant.empty:
            wf_ano_ant = calcular_waterfall_mensal_cpu(
                custo_real=dados.get("custo_real"),
                custo_ant=custo_ano_ant,
                vol_real=dados.get("volume_real"),
                vol_ant=vol_ano_ant,
                label_ant=f"{mes_nome}/{_ano_ant}",
                label_real=f"{mes_nome}/{ano}",
            )

    dados_graficos: dict[str, Any] = {
        "global": {
            "wf_budget_labels": wf_budget.get("labels", []),
            "wf_budget_values": [float(v) for v in wf_budget.get("values", [])],
            "wf_mensal_labels": wf_mensal.get("labels", []),
            "wf_mensal_values": [float(v) for v in wf_mensal.get("values", [])],
            "wf_ano_ant_labels": wf_ano_ant.get("labels", []),
            "wf_ano_ant_values": [float(v) for v in wf_ano_ant.get("values", [])],
            "ano": ano,
            "ano_anterior": dados.get("ano_anterior", ano - 1),
        },
        "oficinas": {},
    }
    for ofc in oficinas:
        try:
            dados_ofc = _filtrar_por_oficina(dados, ofc)

            # 1) Waterfall Budget (oficina)
            wf_ofc_budget = calcular_waterfall_budget_cpu(
                custo_real=dados_ofc.get("custo_real"),
                custo_bud=dados_ofc.get("custo_bud"),
                vol_real=dados.get("volume_real"),
                vol_bud=dados.get("volume_bud"),
            )

            # 2) Waterfall Mensal (oficina)
            wf_ofc_mensal: dict[str, Any] = {}
            if not sem_mes_anterior:
                wf_ofc_mensal = calcular_waterfall_mensal_cpu(
                    custo_real=dados_ofc.get("custo_real"),
                    custo_ant=dados_ofc.get("custo_real_ant"),
                    vol_real=dados.get("volume_real"),
                    vol_ant=dados.get("volume_real_ant"),
                    label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
                    label_real=mes_nome,
                )

            # 3) Waterfall Ano Anterior (oficina)
            wf_ofc_ano_ant: dict[str, Any] = {}
            if not sem_ano_anterior:
                hist_custo_ofc = dados_ofc.get("historico_custo")
                if hist_custo_ofc is not None and not hist_custo_ofc.empty:
                    _mask_oc = _pd.Series(True, index=hist_custo_ofc.index)
                    if "Ano" in hist_custo_ofc.columns:
                        _mask_oc = _mask_oc & (hist_custo_ofc["Ano"] == _ano_ant)
                    if "Período" in hist_custo_ofc.columns:
                        _mask_oc = _mask_oc & (hist_custo_ofc["Período"] == mes_nome)
                    custo_aa_ofc = hist_custo_ofc.loc[_mask_oc].copy() if _mask_oc.any() else None
                else:
                    custo_aa_ofc = None
                # Volume YoY usa volume global (oficinas não possuem volume próprio)
                if custo_aa_ofc is not None and not custo_aa_ofc.empty:
                    wf_ofc_ano_ant = calcular_waterfall_mensal_cpu(
                        custo_real=dados_ofc.get("custo_real"),
                        custo_ant=custo_aa_ofc,
                        vol_real=dados.get("volume_real"),
                        vol_ant=vol_ano_ant,
                        label_ant=f"{mes_nome}/{_ano_ant}",
                        label_real=f"{mes_nome}/{ano}",
                    )

            dados_graficos["oficinas"][ofc] = {
                "wf_budget_labels": wf_ofc_budget.get("labels", []),
                "wf_budget_values": [float(v) for v in wf_ofc_budget.get("values", [])],
                "wf_mensal_labels": wf_ofc_mensal.get("labels", []),
                "wf_mensal_values": [float(v) for v in wf_ofc_mensal.get("values", [])],
                "wf_ano_ant_labels": wf_ofc_ano_ant.get("labels", []),
                "wf_ano_ant_values": [float(v) for v in wf_ofc_ano_ant.get("values", [])],
                "ano": ano,
                "ano_anterior": dados.get("ano_anterior", ano - 1),
            }
        except Exception as e:
            logger.warning("Falha gráfico oficina %s: %s", ofc, e)

    # 4. Gerar textos via templates (SEM API)
    secoes_geradas = gerar_todas_secoes_local(
        dados=dados,
        variacoes=variacoes,
        dados_graficos=dados_graficos,
        oficinas_info=oficinas_info,
        idioma=idioma,
        moeda=moeda,
        simbolo=simbolo,
    )

    # 5. Salvar no JSON local
    adicionar_mes_ao_relatorio_local(ano, mes_numero, secoes_geradas, dados_graficos)

    # 6. Gerar PDF local
    return gerar_pdf_local(ano, idioma, simbolo_moeda=simbolo)
