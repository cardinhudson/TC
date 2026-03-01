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
    KeepTogether,
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
    caminho_relatorio_mensal,
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
            keepWithNext=1,
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
            keepWithNext=1,
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
        "toc_nivel2": ParagraphStyle(
            "TocNivel2",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=11,
            leading=16,
            textColor=COR_SECUNDARIA,
            leftIndent=40,
        ),
        "toc_nivel3": ParagraphStyle(
            "TocNivel3",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=COR_CINZA,
            leftIndent=60,
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
    """Adiciona página de sumário com 3 níveis e links clicáveis."""
    titulo = "Sumário" if idioma == "pt-BR" else "Table of Contents"
    elements.append(Paragraph(titulo, estilos["titulo_capitulo"]))
    elements.append(Spacer(1, 1 * cm))

    labels = LABELS.get(idioma, LABELS["pt-BR"])

    secao_label_map = {
        "resumo_executivo": "sec_resumo_executivo",
        "volume_completo": "sec_volume_completo",
        "comparativos": "sec_comparativos",
        "conclusoes": "sec_conclusoes",
    }
    secoes_ordem = [
        "resumo_executivo", "volume_completo",
        "comparativos", "conclusoes",
    ]
    sub_comparativos = [
        ("2.1", "sec_real_vs_budget_flex"),
        ("2.2", "sec_real_vs_mes_ant"),
        ("2.3", "sec_real_vs_ano_ant"),
    ]

    import re as _re_toc

    def _sub_presente(sub_id: str, txt: str) -> bool:
        """Verifica se um sub-tópico existe como header real no texto."""
        pattern = rf'(^|\n)\s*#{{1,4}}\s*\**{_re_toc.escape(sub_id)}'
        return bool(_re_toc.search(pattern, txt))

    meses_ordenados = sorted(
        dados_relatorio.get("meses", {}).items(),
        key=lambda x: int(x[0]),
    )

    for str_mes, info in meses_ordenados:
        mes_nome = info.get("mes_nome", f"Mês {str_mes}")
        cap_n = int(str_mes)
        cap_lbl = (
            f"Capítulo {cap_n}" if idioma == "pt-BR"
            else f"Chapter {cap_n}"
        )
        # Nível 1 — Mês
        elements.append(Paragraph(
            f'<a href="#cap{cap_n}" color="#1a3c6e">'
            f'<b>{cap_lbl}</b> — {mes_nome}</a>',
            estilos["toc_item"],
        ))

        secoes = info.get("secoes", {})

        # Nível 2 — Seções do mês
        for tipo in secoes_ordem:
            if tipo not in secoes:
                continue
            lbl_key = secao_label_map.get(tipo, tipo)
            sec_titulo = _substituir_emojis(
                labels.get(lbl_key, tipo)
            )
            anchor = f"cap{cap_n}_{tipo}"
            elements.append(Paragraph(
                f'<a href="#{anchor}" color="#2c5282">'
                f'{sec_titulo}</a>',
                estilos["toc_nivel2"],
            ))

            # Nível 3 — Sub-tópicos de Comparativos
            if tipo == "comparativos":
                _txt_comp = secoes.get("comparativos", "")
                for sub_id, sub_lbl in sub_comparativos:
                    # Só incluir no TOC se o sub-tópico existe como header real
                    if not _sub_presente(sub_id, _txt_comp):
                        continue
                    sub_titulo = _substituir_emojis(
                        labels.get(sub_lbl, sub_id)
                    )
                    anc_sub = f"cap{cap_n}_sub{sub_id.replace('.', '_')}"
                    elements.append(Paragraph(
                        f'<a href="#{anc_sub}" color="#666666">'
                        f'{sub_titulo}</a>',
                        estilos["toc_nivel3"],
                    ))

            # Nível 3 — Anexos (Tabelas de Conclusões)
            if tipo == "conclusoes":
                graf = info.get("dados_graficos", {})
                tabelas_g = graf.get("global", {}).get("tabelas", [])
                if tabelas_g:
                    anc_anx = f"cap{cap_n}_anexos"
                    _anx_lbl = _substituir_emojis(
                        labels.get(
                            "sec_anexos_tabelas",
                            "Anexos — Tabelas Principais Despesas",
                        )
                    )
                    elements.append(Paragraph(
                        f'<a href="#{anc_anx}" color="#666666">'
                        f'{_anx_lbl}</a>',
                        estilos["toc_nivel3"],
                    ))

        # Nível 2 — Oficinas header
        oficina_keys = sorted(
            k for k in secoes if k.startswith("oficina_")
        )
        if oficina_keys:
            ofc_header = _substituir_emojis(
                labels.get("sec_oficinas_header", "4. Oficinas")
            )
            elements.append(Paragraph(
                f'<a href="#cap{cap_n}_oficinas" color="#2c5282">'
                f'{ofc_header}</a>',
                estilos["toc_nivel2"],
            ))
            # Nível 3 — Cada oficina
            graf = info.get("dados_graficos", {})
            for idx_o, ofc_k in enumerate(oficina_keys):
                ofc_nome = ofc_k.replace("oficina_", "")
                ofc_tpl = labels.get(
                    "sec_oficina",
                    "4.{idx} Oficina {oficina}",
                )
                ofc_titulo = _substituir_emojis(
                    ofc_tpl.format(idx=idx_o + 1, oficina=ofc_nome)
                )
                anc_o = f"cap{cap_n}_ofc{idx_o + 1}"
                elements.append(Paragraph(
                    f'<a href="#{anc_o}" color="#666666">'
                    f'{ofc_titulo}</a>',
                    estilos["toc_nivel3"],
                ))

    elements.append(PageBreak())


def _construir_sumario_mensal(
    elements: list,
    estilos: dict,
    mes_numero: int,
    info_mes: dict,
    idioma: str,
):
    """Adiciona sumário de um único mês com links clicáveis."""
    titulo = "Sumário" if idioma == "pt-BR" else "Table of Contents"
    elements.append(Paragraph(titulo, estilos["titulo_capitulo"]))
    elements.append(Spacer(1, 0.8 * cm))

    labels = LABELS.get(idioma, LABELS["pt-BR"])
    cap_n = mes_numero
    secoes = info_mes.get("secoes", {})

    secao_label_map = {
        "resumo_executivo": "sec_resumo_executivo",
        "volume_completo": "sec_volume_completo",
        "comparativos": "sec_comparativos",
        "conclusoes": "sec_conclusoes",
    }
    secoes_ordem = [
        "resumo_executivo", "volume_completo",
        "comparativos", "conclusoes",
    ]
    sub_comp = [
        ("2.1", "sec_real_vs_budget_flex"),
        ("2.2", "sec_real_vs_mes_ant"),
        ("2.3", "sec_real_vs_ano_ant"),
    ]

    import re as _re_toc_m

    def _sub_presente_m(sub_id: str, txt: str) -> bool:
        """Verifica se um sub-tópico existe como header real no texto."""
        pattern = rf'(^|\n)\s*#{{1,4}}\s*\**{_re_toc_m.escape(sub_id)}'
        return bool(_re_toc_m.search(pattern, txt))

    for tipo in secoes_ordem:
        if tipo not in secoes:
            continue
        lbl_key = secao_label_map.get(tipo, tipo)
        sec_titulo = _substituir_emojis(labels.get(lbl_key, tipo))
        anchor = f"cap{cap_n}_{tipo}"
        elements.append(Paragraph(
            f'<a href="#{anchor}" color="#2c5282">'
            f'{sec_titulo}</a>',
            estilos["toc_nivel2"],
        ))
        if tipo == "comparativos":
            _txt_comp = secoes.get("comparativos", "")
            for sub_id, sub_lbl in sub_comp:
                # Só incluir no TOC se o sub-tópico existe como header real
                if not _sub_presente_m(sub_id, _txt_comp):
                    continue
                sub_titulo = _substituir_emojis(
                    labels.get(sub_lbl, sub_id)
                )
                anc_sub = (
                    f"cap{cap_n}_sub{sub_id.replace('.', '_')}"
                )
                elements.append(Paragraph(
                    f'<a href="#{anc_sub}" color="#666666">'
                    f'{sub_titulo}</a>',
                    estilos["toc_nivel3"],
                ))
        if tipo == "conclusoes":
            graf = info_mes.get("dados_graficos", {})
            tabelas_g = graf.get("global", {}).get("tabelas", [])
            if tabelas_g:
                anc_anx = f"cap{cap_n}_anexos"
                _anx_lbl = _substituir_emojis(
                    labels.get(
                        "sec_anexos_tabelas",
                        "Anexos — Tabelas Principais Despesas",
                    )
                )
                elements.append(Paragraph(
                    f'<a href="#{anc_anx}" '
                    f'color="#666666">{_anx_lbl}</a>',
                    estilos["toc_nivel3"],
                ))

    oficina_keys = sorted(
        k for k in secoes if k.startswith("oficina_")
    )
    if oficina_keys:
        ofc_header = _substituir_emojis(
            labels.get("sec_oficinas_header", "4. Oficinas")
        )
        elements.append(Paragraph(
            f'<a href="#cap{cap_n}_oficinas" color="#2c5282">'
            f'{ofc_header}</a>',
            estilos["toc_nivel2"],
        ))
        for idx_o, ofc_k in enumerate(oficina_keys):
            ofc_nome = ofc_k.replace("oficina_", "")
            ofc_tpl = labels.get(
                "sec_oficina", "4.{idx} Oficina {oficina}",
            )
            ofc_titulo = _substituir_emojis(
                ofc_tpl.format(idx=idx_o + 1, oficina=ofc_nome)
            )
            elements.append(Paragraph(
                f'<a href="#cap{cap_n}_ofc{idx_o + 1}" '
                f'color="#666666">{ofc_titulo}</a>',
                estilos["toc_nivel3"],
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

    # Título do capítulo (com anchor para TOC)
    cap_label = f"Capítulo {mes_numero}" if idioma == "pt-BR" else f"Chapter {mes_numero}"
    elements.append(Paragraph(
        f'<a name="cap{mes_numero}"/>{cap_label} — {mes_nome}',
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
        titulo_secao = _aplicar_formatacao(_substituir_emojis(
            labels.get(label_key, tipo_secao)
        ))
        # Anchor para TOC
        anchor_sec = f'<a name="cap{mes_numero}_{tipo_secao}"/>'
        elements.append(Paragraph(
            f'{anchor_sec}{titulo_secao}',
            estilos["titulo_secao"],
        ))

        # ── Volume: gráfico de volume por veículo + texto ──
        if tipo_secao == "volume_completo":
            if graf_global:
                _inserir_grafico_volume_pdf(
                    elements, graf_global, mes_nome,
                    info_mes, simbolo_moeda,
                )
            paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
            elements.extend(paragraphs)

        # ── Comparativos: interleavar gráficos nos sub-tópicos ──
        elif tipo_secao == "comparativos":
            _renderizar_comparativos_pdf(
                elements, estilos, texto, graf_global,
                mes_nome, info_mes, simbolo_moeda,
                mes_numero=mes_numero,
            )

        # ── Conclusões: texto + tabelas globais ──
        elif tipo_secao == "conclusoes":
            paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
            elements.extend(paragraphs)
            # Tabelas de análise detalhada (global) — Anexos
            tabelas_g = graf_global.get("tabelas", [])
            if tabelas_g:
                elements.append(Spacer(1, 0.5 * cm))
                _anx_titulo = _aplicar_formatacao(_substituir_emojis(
                    labels.get(
                        "sec_anexos_tabelas",
                        "Anexos — Tabelas Principais Despesas",
                    )
                ))
                anc_anx = f'<a name="cap{mes_numero}_anexos"/>'
                elements.append(Paragraph(
                    f'{anc_anx}{_anx_titulo}',
                    estilos["titulo_secao"],
                ))
                for idx_t, tab_data in enumerate(tabelas_g):
                    t_num = chr(65 + idx_t)
                    anc = f"cap{mes_numero}_tab3{t_num}"
                    _renderizar_tabela_pdf(
                        elements, estilos, tab_data,
                        anchor_name=anc,
                        numero=f"{t_num}",
                        simbolo_moeda=simbolo_moeda,
                    )

        else:
            paragraphs = _texto_para_paragraphs(texto, estilos["corpo"])
            elements.extend(paragraphs)

        elements.append(Spacer(1, 0.5 * cm))

    # ═══ Seção 4 — Oficinas (header + waterfall global + intro + sub-seções) ═══
    oficina_keys = sorted([k for k in secoes if k.startswith("oficina_")])
    if oficina_keys:
        # 4.0 Título principal "4. Oficinas" (com anchor)
        titulo_oficinas_header = labels.get(
            "sec_oficinas_header", "4. 🏭 Oficinas"
        )
        titulo_oficinas_header = _aplicar_formatacao(
            _substituir_emojis(titulo_oficinas_header)
        )
        anc_ofc_hdr = f'<a name="cap{mes_numero}_oficinas"/>'
        elements.append(Paragraph(
            f'{anc_ofc_hdr}{titulo_oficinas_header}',
            estilos["titulo_secao"],
        ))

        # 4.0.1 Gráfico waterfall global por oficina (Budget vs Real)
        wf_ofc_labels = graf_global.get("wf_oficinas_labels", [])
        wf_ofc_values = graf_global.get("wf_oficinas_values", [])
        if wf_ofc_labels and len(wf_ofc_labels) >= 3:
            try:
                from tc_copilot.chart_generator import gerar_waterfall_from_arrays
                cpu_label = f"{simbolo_moeda}/veíc"
                ano_rel = graf_global.get("ano", "")
                png_ofc_global = gerar_waterfall_from_arrays(
                    {"labels": wf_ofc_labels, "values": wf_ofc_values},
                    titulo=f"Account — Waterfall Budget vs Real por Oficina — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                    y_label=cpu_label,
                )
                _inserir_grafico(elements, png_ofc_global, largura_max=17 * cm)
            except Exception:
                pass

        # 4.0.2 Texto introdutório das oficinas
        texto_intro_oficinas = secoes.get("oficinas_intro", "")
        if texto_intro_oficinas:
            paragraphs = _texto_para_paragraphs(texto_intro_oficinas, estilos["corpo"])
            elements.extend(paragraphs)
            elements.append(Spacer(1, 0.3 * cm))

    # Sub-seções de oficina (4.1, 4.2, ...)
    for idx_ofc, ofc_key in enumerate(oficina_keys):
        texto = secoes.get(ofc_key, "")
        if not texto:
            continue
        ofc_nome = ofc_key.replace("oficina_", "")
        titulo_template = labels.get(
            "sec_oficina", "4.{idx} 🏭 Oficina {oficina}"
        )
        titulo_ofc = _aplicar_formatacao(_substituir_emojis(
            titulo_template.format(idx=idx_ofc + 1, oficina=ofc_nome)
        ))
        # Anchor para TOC
        anc_ofc = f'<a name="cap{mes_numero}_ofc{idx_ofc + 1}"/>'
        elements.append(Paragraph(
            f'{anc_ofc}{titulo_ofc}', estilos["titulo_secao"],
        ))

        graf_ofc = graf_oficinas.get(ofc_nome, {})

        # Interleavar gráficos waterfall nos sub-tópicos da oficina
        if "<!-- SPLIT -->" in texto and graf_ofc:
            blocos_ofc = [b.strip() for b in texto.split("<!-- SPLIT -->") if b.strip()]
            for idx_b, bloco_ofc in enumerate(blocos_ofc):
                _lo = bloco_ofc.split("\n", 1)
                titulo_ofc_linha = _lo[0].strip()
                corpo_ofc = _lo[1].strip() if len(_lo) > 1 else ""

                elements.extend(_texto_para_paragraphs(
                    titulo_ofc_linha, estilos["corpo"],
                ))
                _tipos_ofc = ["budget", "mensal", "ano_anterior"]
                if idx_b < len(_tipos_ofc):
                    _inserir_grafico_oficina(
                        elements, graf_ofc, ofc_nome,
                        mes_nome, info_mes, simbolo_moeda,
                        tipo_waterfall=_tipos_ofc[idx_b],
                    )
                if corpo_ofc:
                    paragraphs = _texto_para_paragraphs(
                        corpo_ofc, estilos["corpo"],
                    )
                    elements.extend(paragraphs)
        else:
            if graf_ofc:
                _inserir_grafico_oficina(
                    elements, graf_ofc, ofc_nome,
                    mes_nome, info_mes, simbolo_moeda,
                    tipo_waterfall="budget",
                )
            paragraphs = _texto_para_paragraphs(
                texto, estilos["corpo"],
            )
            elements.extend(paragraphs)

        # Tabelas de análise detalhada por oficina — Anexos
        tabelas_ofc = graf_ofc.get("tabelas", [])
        if tabelas_ofc:
            _anx_ofc_titulo = _aplicar_formatacao(
                _substituir_emojis(
                    labels.get(
                        "sec_anexos_tabelas",
                        "Anexos — Tabelas Principais Despesas",
                    )
                )
            )
            anc_anx_ofc = (
                f'<a name="cap{mes_numero}'
                f'_ofc{idx_ofc + 1}_anexos"/>'
            )
            elements.append(Paragraph(
                f'{anc_anx_ofc}{_anx_ofc_titulo}',
                estilos["titulo_secao"],
            ))
            for idx_t, tab_data in enumerate(tabelas_ofc):
                t_num = chr(65 + idx_t)
                anc_t = (
                    f"cap{mes_numero}_ofc{idx_ofc + 1}"
                    f"_tab{t_num}"
                )
                _renderizar_tabela_pdf(
                    elements, estilos, tab_data,
                    anchor_name=anc_t,
                    numero=f"{t_num}",
                    simbolo_moeda=simbolo_moeda,
                )

        elements.append(Spacer(1, 0.5 * cm))

    elements.append(PageBreak())


def _inserir_grafico_volume_pdf(
    elements: list,
    graf_global: dict,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Insere gráfico de barras Volume Real vs Budget por veículo no PDF."""
    try:
        from tc_copilot.chart_generator import gerar_grafico_volume_por_veiculo
    except ImportError:
        return

    vol_real = graf_global.get("vol_modelos_real", {})
    vol_bud = graf_global.get("vol_modelos_budget", {})
    if not vol_real and not vol_bud:
        return

    ano_rel = graf_global.get("ano", "")
    png = gerar_grafico_volume_por_veiculo(
        vol_modelos_real=vol_real,
        vol_modelos_budget=vol_bud,
        titulo=f"Volume por Veículo — Real vs Budget — {mes_nome}/{ano_rel}",
    )
    _inserir_grafico(elements, png, largura_max=17 * cm)


def _renderizar_comparativos_pdf(
    elements: list,
    estilos: dict,
    texto: str,
    graf_global: dict | None,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
    mes_numero: int = 0,
) -> None:
    """Renderiza seção Comparativos no PDF intercalando gráficos nos sub-tópicos.

    Para cada sub-tópico (2.1, 2.2, 2.3):
      1. Anchor + título
      2. Waterfall pair (Type 05 × Type 06)  ← empilhados verticalmente
      3. Waterfall principal (Account)
      4. Texto analítico
    """
    import re

    # Separar sub-seções pelo marcador (compatível com textos legados)
    if "<!-- SPLIT -->" in texto:
        blocos = [b.strip() for b in texto.split("<!-- SPLIT -->") if b.strip()]
    else:
        blocos = [b.strip() for b in re.split(r"(?=### 2\.)", texto) if b.strip()]

    # Mapa sub-tópico → (prefixo_t05, prefixo_t06, inserir_account)
    _sub_map = {
        "2.1": ("wf_budget_type05", "wf_budget_type06",
                _inserir_waterfall_budget),
        "2.2": ("wf_mensal_type05", "wf_mensal_type06",
                _inserir_waterfall_mensal),
        "2.3": ("wf_ano_ant_type05", "wf_ano_ant_type06",
                _inserir_waterfall_ano_anterior),
    }

    for bloco in blocos:
        _linhas = bloco.split("\n", 1)
        titulo_linha = _linhas[0].strip()
        corpo = _linhas[1].strip() if len(_linhas) > 1 else ""

        # Detectar sub-tópico (2.1, 2.2, 2.3)
        _tl = titulo_linha.lstrip("# ")
        sub_id = ""
        for sid in ("2.1", "2.2", "2.3"):
            if _tl.startswith(sid) or _tl.startswith(f"**{sid}"):
                sub_id = sid
                break

        # 1) Anchor + título
        if sub_id and mes_numero:
            anc_sub = f"cap{mes_numero}_sub{sub_id.replace('.', '_')}"
            elements.append(Paragraph(
                f'<a name="{anc_sub}"/>', estilos["corpo"],
            ))
        elements.extend(
            _texto_para_paragraphs(titulo_linha, estilos["corpo"])
        )

        # 2-3) Gráficos
        if graf_global and sub_id and sub_id in _sub_map:
            pref_t05, pref_t06, fn_account = _sub_map[sub_id]
            # Side-by-side Type 05 × Type 06
            _inserir_waterfall_pair(
                elements, graf_global, pref_t05, pref_t06,
                mes_nome, info_mes, simbolo_moeda,
            )
            # Waterfall principal (Account)
            fn_account(
                elements, graf_global, mes_nome,
                info_mes, simbolo_moeda,
            )

        # 4) Texto analítico
        if corpo:
            paragraphs = _texto_para_paragraphs(
                corpo, estilos["corpo"],
            )
            elements.extend(paragraphs)

        elements.append(Spacer(1, 0.3 * cm))


# ═══════════════════════════════════════════════════════════════
#  TABELAS PDF (ReportLab Table)
# ═══════════════════════════════════════════════════════════════

def _renderizar_tabela_pdf(
    elements: list,
    estilos: dict,
    tabela_data: dict[str, Any],
    anchor_name: str = "",
    numero: str = "",
    simbolo_moeda: str = "R$",
) -> None:
    """Renderiza uma tabela de análise detalhada no PDF.

    Args:
        tabela_data: {"titulo": str, "colunas": [...], "linhas": [[...],...]}
        anchor_name: Nome do anchor para TOC.
        numero: Numeração da tabela (ex: "3.A").
        simbolo_moeda: Símbolo de moeda para formatar valores.
    """
    titulo = tabela_data.get("titulo", "")
    colunas = tabela_data.get("colunas", [])
    linhas = tabela_data.get("linhas", [])

    if not linhas:
        return

    # Título com anchor
    titulo_fmt = f"{numero} {titulo}" if numero else titulo
    titulo_fmt = _aplicar_formatacao(_substituir_emojis(titulo_fmt))
    if anchor_name:
        titulo_fmt = f'<a name="{anchor_name}"/>{titulo_fmt}'

    elements.append(Spacer(1, 0.4 * cm))
    elements.append(Paragraph(
        titulo_fmt,
        ParagraphStyle(
            "TabelaTitulo",
            parent=estilos.get("titulo_secao", estilos["corpo"]),
            fontSize=11,
            leading=14,
            spaceBefore=8,
            spaceAfter=4,
        ),
    ))

    # Formatar header (estilo leve, sem fundo colorido)
    header_style = ParagraphStyle(
        "TabHeader", fontName="Helvetica-Bold",
        fontSize=6, leading=8, textColor=COR_TEXTO,
        alignment=TA_CENTER,
    )
    cell_style = ParagraphStyle(
        "TabCell", fontName="Helvetica",
        fontSize=6, leading=8, textColor=COR_TEXTO,
    )
    cell_num_style = ParagraphStyle(
        "TabCellNum", fontName="Helvetica",
        fontSize=6, leading=8, textColor=COR_TEXTO,
        alignment=TA_CENTER,
    )

    # Renomear "Real" para incluir moeda
    col_display = []
    for c in colunas:
        if c == "Real":
            col_display.append(f"real (k{simbolo_moeda})")
        else:
            col_display.append(c.lower())

    header_row = [Paragraph(c, header_style) for c in col_display]

    # Linhas de dados (texto em minúsculas)
    data_rows = []
    for linha in linhas:
        row = []
        for i, val in enumerate(linha):
            if isinstance(val, (int, float)):
                # Formatar como kMoeda (dividir por 1000)
                v_k = val / 1000.0
                txt = f"{v_k:,.1f}"
                row.append(Paragraph(txt, cell_num_style))
            else:
                # Truncar texto longo e converter para minúsculas
                txt = str(val)[:30] if len(str(val)) > 30 else str(val)
                txt = txt.lower()
                row.append(Paragraph(txt, cell_style))
        data_rows.append(row)

    table_data = [header_row] + data_rows

    # Calcular largura das colunas (proporcional)
    n_cols = len(colunas)
    largura_total = 17 * cm
    col_widths = [largura_total / n_cols] * n_cols

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)

    # Estilo leve: sem fundo, linhas finas
    _cor_linha = colors.HexColor("#CCCCCC")
    style_cmds = [
        # Header
        ("TEXTCOLOR", (0, 0), (-1, 0), COR_TEXTO),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 6),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 3),
        ("TOPPADDING", (0, 0), (-1, 0), 3),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, COR_TEXTO),
        # Cells
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 6),
        ("TOPPADDING", (0, 1), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        # Linhas horizontais finas entre cada linha
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, _cor_linha),
        # Valor numérico alinhado à direita
        ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
    ]

    tbl.setStyle(TableStyle(style_cmds))
    elements.append(tbl)
    elements.append(Spacer(1, 0.3 * cm))


def _inserir_waterfall_pair(
    elements: list,
    graf_global: dict,
    prefixo_t05: str,
    prefixo_t06: str,
    mes_nome: str,
    info_mes: dict,
    simbolo_moeda: str = "R$",
) -> None:
    """Insere dois waterfalls (Type 05 × Type 06) empilhados verticalmente."""
    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"
    ano_rel = graf_global.get("ano", "")

    for pref, dim_label in [
        (prefixo_t05, "Type 05"), (prefixo_t06, "Type 06"),
    ]:
        lbls = graf_global.get(f"{pref}_labels", [])
        vals = graf_global.get(f"{pref}_values", [])
        if not lbls or len(lbls) < 3:
            continue
        png = gerar_waterfall_from_arrays(
            {"labels": lbls, "values": vals},
            titulo=f"{dim_label} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
            y_label=cpu_label,
            width=14,
            height=5,
        )
        if not png:
            continue
        buf = _BytesIO(png)
        img = Image(buf)
        target_w = 17 * cm
        ratio = img.imageWidth / img.imageHeight if img.imageHeight else 1
        img.drawWidth = target_w
        img.drawHeight = target_w / ratio
        img.hAlign = "CENTER"
        elements.append(img)
        elements.append(Spacer(1, 0.2 * cm))


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
            titulo=f"Account — Waterfall Budget — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
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
            titulo=f"Account — Waterfall Mensal — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
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
            titulo=f"Account — Waterfall Ano Anterior — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
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
        # Type 05 / Type 06 primeiro
        _inserir_waterfall_pair(
            elements, graf_ofc,
            "wf_budget_type05", "wf_budget_type06",
            mes_nome, info_mes, simbolo_moeda,
        )
        # Depois Account
        wf_labels = graf_ofc.get("wf_budget_labels", [])
        wf_values = graf_ofc.get("wf_budget_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Account — Waterfall Budget — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                y_label=cpu_label,
            )
            _inserir_grafico(elements, png, largura_max=17 * cm)

    # Mensal
    if tipo_waterfall in ("mensal", "ambos"):
        _inserir_waterfall_pair(
            elements, graf_ofc,
            "wf_mensal_type05", "wf_mensal_type06",
            mes_nome, info_mes, simbolo_moeda,
        )
        wf_labels = graf_ofc.get("wf_mensal_labels", [])
        wf_values = graf_ofc.get("wf_mensal_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Account — Waterfall Mensal — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                y_label=cpu_label,
            )
            _inserir_grafico(elements, png, largura_max=17 * cm)

    # Ano Anterior (YoY)
    if tipo_waterfall in ("ano_anterior", "ambos"):
        _inserir_waterfall_pair(
            elements, graf_ofc,
            "wf_ano_ant_type05", "wf_ano_ant_type06",
            mes_nome, info_mes, simbolo_moeda,
        )
        wf_labels = graf_ofc.get("wf_ano_ant_labels", [])
        wf_values = graf_ofc.get("wf_ano_ant_values", [])
        if wf_labels and len(wf_labels) >= 3:
            png = gerar_waterfall_from_arrays(
                {"labels": wf_labels, "values": wf_values},
                titulo=f"Account — Waterfall Ano Anterior — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
                y_label=cpu_label,
            )
            _inserir_grafico(elements, png, largura_max=17 * cm)


def gerar_pdf(ano: int, idioma: str = "pt-BR", simbolo_moeda: str = "€") -> str:
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
            simbolo_moeda,
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
#  HELPER — PRÉ-CÁLCULO DE TABELAS PARA O PDF
# ═══════════════════════════════════════════════════════════════

_TABELA_COLS_GLOBAL = [
    "Type 05", "Type 06", "Account",
    "Centrocst", "Texto breve", "Fornecedor",
]
_TABELA_COLS_OFC = [
    "Type 05", "Type 06", "Account",
    "Centrocst", "Texto breve", "Fornecedor",
]
_COL_VALOR = "Custo FP"


def _construir_tabela_top(
    df,
    account_filter: str | None,
    colunas: list[str],
    titulo: str,
    top_n: int = 10,
    col_valor: str = _COL_VALOR,
) -> dict[str, Any] | None:
    """Constrói dict de tabela para serialização JSON.

    Returns:
        {"titulo": str, "colunas": [...], "linhas": [[...], ...]}
        ou None se vazio.
    """
    import pandas as pd

    if df is None or df.empty:
        return None

    df_f = df.copy()
    if account_filter and "Account" in df_f.columns:
        df_f = df_f[df_f["Account"] == account_filter]
    if df_f.empty:
        return None

    # Selecionar colunas presentes
    cols_disp = [c for c in colunas if c in df_f.columns]
    if col_valor not in df_f.columns:
        return None

    # Agrupar para evitar linhas duplicadas
    group_cols = [c for c in cols_disp if c in df_f.columns]
    if group_cols:
        df_agg = (
            df_f.groupby(group_cols, dropna=False)[col_valor]
            .sum()
            .reset_index()
        )
    else:
        df_agg = df_f[[col_valor]].copy()

    # Ordenar por valor absoluto descendente e pegar top N
    df_agg["_abs"] = df_agg[col_valor].abs()
    df_agg = df_agg.sort_values("_abs", ascending=False).head(top_n)
    df_agg = df_agg.drop(columns=["_abs"])

    if df_agg.empty:
        return None

    # Converter para lista de listas
    col_final = cols_disp + ["Real"]
    linhas = []
    for _, row in df_agg.iterrows():
        linha = []
        for c in cols_disp:
            val = row.get(c, "")
            linha.append("" if pd.isna(val) else str(val))
        linha.append(float(row[col_valor]))
        linhas.append(linha)

    return {"titulo": titulo, "colunas": col_final, "linhas": linhas}


def _extrair_piores_accounts(
    wf_labels: list, wf_values: list, excluir: set | None = None,
    n: int = 2,
) -> list[str]:
    """Extrai os N accounts com maior delta positivo (pior desempenho).

    Ignora barras totais (primeira, última), Flex e Outros.
    """
    if not wf_labels or len(wf_labels) < 4:
        return []
    skip = {"Outros", "Others"}
    if excluir:
        skip |= excluir

    pares = []
    for lbl, val in zip(wf_labels[1:-1], wf_values[1:-1]):
        clean = lbl.replace("\n", " ").strip()
        if clean in skip or "flex" in clean.lower():
            continue
        pares.append((clean, val))

    # Maior delta positivo = pior (custo real acima do budget)
    pares.sort(key=lambda x: x[1], reverse=True)
    return [p[0] for p in pares[:n] if p[1] > 0]


def _calcular_tabelas_secao(
    custo_real,
    wf_budget_labels: list,
    wf_budget_values: list,
    colunas: list[str],
    labels_dict: dict,
    prefixo_titulo: str = "",
) -> list[dict[str, Any]]:
    """Calcula as 4 tabelas de uma seção (global ou oficina).

    1. Material — Top 10
    2. Supplier Failure Recovery
    3-4. Dois piores accounts dinâmicos

    Returns:
        Lista de dicts de tabela (sem None).
    """
    tabelas: list[dict[str, Any]] = []

    lbl_mat = labels_dict.get(
        "sec_tabela_material", "Material — Top 10 Real"
    )
    lbl_sfr = labels_dict.get(
        "sec_tabela_sfr", "Supplier Failure Recovery"
    )
    if prefixo_titulo:
        lbl_mat = f"{prefixo_titulo} — {lbl_mat}"
        lbl_sfr = f"{prefixo_titulo} — {lbl_sfr}"

    # 1. Material
    t = _construir_tabela_top(
        custo_real, "Material", colunas, lbl_mat, top_n=10,
    )
    if t:
        tabelas.append(t)

    # 2. Supplier Failure Recovery
    t = _construir_tabela_top(
        custo_real, "Supplier Failure Recovery",
        colunas, lbl_sfr, top_n=10,
    )
    if t:
        tabelas.append(t)

    # 3-4. Accounts dinâmicos (piores desempenhos)
    excluir = {"Material", "Supplier Failure Recovery"}
    piores = _extrair_piores_accounts(
        wf_budget_labels, wf_budget_values, excluir, n=2,
    )
    for acc in piores:
        titulo_acc = f"{prefixo_titulo} — {acc}" if prefixo_titulo else acc
        t = _construir_tabela_top(
            custo_real, acc, colunas, titulo_acc, top_n=10,
        )
        if t:
            tabelas.append(t)

    return tabelas


# ═══════════════════════════════════════════════════════════════
#  GERAR RELATÓRIO COMPLETO PARA UM MÊS
# ═══════════════════════════════════════════════════════════════

def gerar_relatorio_mes(
    ano: int,
    mes_numero: int,
    api_key: str | None,
    modelo: str = "gpt-4o-mini",
    idioma: str = "pt-BR",
    moeda: str = "EUR",
    taxas: dict[str, float] | None = None,
) -> str:
    """
    Pipeline completo: coleta dados → gera texto LLM → salva JSON → gera PDF.

    Args:
        ano: Ano do relatório
        mes_numero: Mês a gerar (1-12)
        api_key: Chave OpenAI (pode ser None → fallback sem LLM)
        modelo: Modelo LLM
        idioma: 'pt-BR' ou 'en'
        moeda: Código da moeda (BRL, USD, EUR)
        taxas: Dict com taxas multiplicativas {"USD": 0.20, "EUR": 0.18}

    Returns:
        Caminho do PDF gerado
    """
    from tc_copilot.data_collector import (
        _filtrar_por_oficina,
        calcular_variacoes,
        coletar_dados_mes,
        configurar_moeda_formatacao,
        descobrir_oficinas,
        formatar_dados_comparativos_agrupado,
        formatar_dados_conclusoes,
        formatar_dados_oficina,
        formatar_dados_resumo_executivo,
        formatar_dados_volume_completo,
    )
    from tc_copilot.llm_integration import gerar_secao_relatorio
    from tc_core.finance.currency import converter_coluna_moeda, obter_simbolo_moeda

    if taxas is None:
        taxas = {}
    # Carregar taxas do banco se não fornecidas e moeda != BRL
    if moeda != "BRL" and not taxas.get(moeda):
        try:
            from tc_core.finance.currency_db import carregar_taxas_banco
            _taxas_banco = carregar_taxas_banco()
            for _mk, _mv in _taxas_banco.items():
                taxas.setdefault(_mk, 1.0 / _mv if _mv > 0 else 1.0)
        except Exception:
            taxas.setdefault("USD", 0.20)
            taxas.setdefault("EUR", 0.18)
    simbolo = obter_simbolo_moeda(moeda)

    # Configurar moeda ativa para formatação automática
    configurar_moeda_formatacao(moeda, simbolo)

    # 1. Coletar dados
    dados = coletar_dados_mes(ano, mes_numero)

    # 1b. Converter custos para moeda selecionada (se != BRL)
    if moeda != "BRL":
        _colunas_custo = ["Custo FP", "Custo MP", "Custo Log.", "Custo Emb.",
                          "Custo Total", "Amort. Fer.", "Amort. Eng.",
                          "Delta Volume", "Delta Mix"]
        for chave_df in ("custo_real", "custo_bud", "custo_real_ant",
                         "custo_fp_real", "custo_fp_bud",
                         "cpu_real", "cpu_bud",
                         "_custo_bud_full", "_custo_real_full",
                         "historico_custo"):
            df = dados.get(chave_df)
            if df is not None and not df.empty:
                for col in _colunas_custo:
                    dados[chave_df] = converter_coluna_moeda(
                        dados[chave_df], col, moeda, taxas,
                    )

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

    # 5a. Gerar introdução da seção Oficinas (texto local, não via LLM)
    try:
        from tc_copilot.data_collector import _filtrar_por_oficina, _safe_sum
        from tc_copilot.text_templates import gerar_texto_intro_oficinas
        oficinas_resumo_ia = []
        for ofc in oficinas:
            dados_ofc = _filtrar_por_oficina(dados, ofc)
            fp_real_ofc = _safe_sum(dados_ofc.get("custo_real"), "Custo FP")
            fp_bud_ofc = _safe_sum(dados_ofc.get("custo_bud"), "Custo FP")
            oficinas_resumo_ia.append((ofc, fp_real_ofc, fp_real_ofc - fp_bud_ofc))
        # dados_graficos ainda não existe aqui — será criado em 5b.
        # Será preenchido após a montagem dos dados_graficos (ver abaixo).
        secoes_geradas["_oficinas_resumo_ia"] = oficinas_resumo_ia  # temp
    except Exception:
        pass

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

    # Waterfall global — Ano Anterior (YoY)
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

    # Extrair volume por veículo para gráfico de barras
    _var_modelos = variacoes.get("variacao_modelos", {})
    _vol_real_modelos = {m: float(v.get("vol_real", 0)) for m, v in _var_modelos.items() if v.get("vol_real", 0) > 0}
    _vol_bud_modelos = {m: float(v.get("vol_budget", 0)) for m, v in _var_modelos.items() if v.get("vol_budget", 0) > 0}

    dados_graficos: dict[str, Any] = {
        "global": {
            "wf_budget_labels": wf_budget.get("labels", []),
            "wf_budget_values": [float(v) for v in wf_budget.get("values", [])],
            "wf_mensal_labels": wf_mensal.get("labels", []),
            "wf_mensal_values": [float(v) for v in wf_mensal.get("values", [])],
            "wf_ano_ant_labels": wf_ano_ant.get("labels", []),
            "wf_ano_ant_values": [float(v) for v in wf_ano_ant.get("values", [])],
            "vol_modelos_real": _vol_real_modelos,
            "vol_modelos_budget": _vol_bud_modelos,
            "ano": ano,
            "ano_anterior": ano_anterior,
        },
        "oficinas": {},
    }

    # Waterfalls Type 05 e Type 06 (side-by-side no PDF)
    _cr = dados.get("custo_real")
    _cb = dados.get("custo_bud")
    _vr = dados.get("volume_real")
    _vb = dados.get("volume_bud")
    for _dim_t in ("Type 05", "Type 06"):
        _sfx = _dim_t.replace(" ", "").lower()  # type05, type06
        try:
            _wf_b = calcular_waterfall_budget_cpu(
                custo_real=_cr, custo_bud=_cb,
                vol_real=_vr, vol_bud=_vb, dim=_dim_t,
            )
        except Exception:
            _wf_b = {}
        dados_graficos["global"][f"wf_budget_{_sfx}_labels"] = _wf_b.get("labels", [])
        dados_graficos["global"][f"wf_budget_{_sfx}_values"] = [
            float(v) for v in _wf_b.get("values", [])]
        # Mensal
        _wf_m: dict[str, Any] = {}
        if not sem_mes_anterior:
            try:
                _wf_m = calcular_waterfall_mensal_cpu(
                    custo_real=_cr,
                    custo_ant=dados.get("custo_real_ant"),
                    vol_real=_vr,
                    vol_ant=dados.get("volume_real_ant"),
                    label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
                    label_real=mes_nome, dim=_dim_t,
                )
            except Exception:
                _wf_m = {}
        dados_graficos["global"][f"wf_mensal_{_sfx}_labels"] = _wf_m.get("labels", [])
        dados_graficos["global"][f"wf_mensal_{_sfx}_values"] = [
            float(v) for v in _wf_m.get("values", [])]
        # Ano anterior
        _wf_a: dict[str, Any] = {}
        if not sem_ano_anterior and custo_ano_ant is not None:
            try:
                _wf_a = calcular_waterfall_mensal_cpu(
                    custo_real=_cr, custo_ant=custo_ano_ant,
                    vol_real=_vr, vol_ant=vol_ano_ant,
                    label_ant=f"{mes_nome}/{_ano_ant}",
                    label_real=f"{mes_nome}/{ano}", dim=_dim_t,
                )
            except Exception:
                _wf_a = {}
        dados_graficos["global"][f"wf_ano_ant_{_sfx}_labels"] = _wf_a.get("labels", [])
        dados_graficos["global"][f"wf_ano_ant_{_sfx}_values"] = [
            float(v) for v in _wf_a.get("values", [])]

    # Waterfall global com dim="Oficina" (para seção 4 do relatório)
    try:
        wf_oficinas_global = calcular_waterfall_budget_cpu(
            custo_real=dados.get("custo_real"),
            custo_bud=dados.get("custo_bud"),
            vol_real=dados.get("volume_real"),
            vol_bud=dados.get("volume_bud"),
            dim="Oficina",
        )
        dados_graficos["global"]["wf_oficinas_labels"] = wf_oficinas_global.get("labels", [])
        dados_graficos["global"]["wf_oficinas_values"] = [float(v) for v in wf_oficinas_global.get("values", [])]
    except Exception as e:
        logger.warning("Falha ao gerar waterfall global por oficina: %s", e)
        dados_graficos["global"]["wf_oficinas_labels"] = []
        dados_graficos["global"]["wf_oficinas_values"] = []

    for ofc in oficinas:
        try:
            dados_ofc = _filtrar_por_oficina(dados, ofc)

            # 1) Waterfall Budget por oficina
            wf_ofc_budget = calcular_waterfall_budget_cpu(
                custo_real=dados_ofc.get("custo_real"),
                custo_bud=dados_ofc.get("custo_bud"),
                vol_real=dados.get("volume_real"),
                vol_bud=dados.get("volume_bud"),
            )

            # 2) Waterfall Mensal por oficina
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

            # 3) Waterfall Ano Anterior por oficina
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
                if custo_aa_ofc is not None and not custo_aa_ofc.empty:
                    wf_ofc_ano_ant = calcular_waterfall_mensal_cpu(
                        custo_real=dados_ofc.get("custo_real"),
                        custo_ant=custo_aa_ofc,
                        vol_real=dados.get("volume_real"),
                        vol_ant=vol_ano_ant,
                        label_ant=f"{mes_nome}/{_ano_ant}",
                        label_real=f"{mes_nome}/{ano}",
                    )

            # 4) Waterfalls Type 05 e Type 06 por oficina
            _ofc_type_data: dict[str, Any] = {}
            _cr_ofc = dados_ofc.get("custo_real")
            _cb_ofc = dados_ofc.get("custo_bud")
            for _dim_t in ("Type 05", "Type 06"):
                _sfx = _dim_t.replace(" ", "").lower()
                try:
                    _wf_bt = calcular_waterfall_budget_cpu(
                        custo_real=_cr_ofc, custo_bud=_cb_ofc,
                        vol_real=dados.get("volume_real"),
                        vol_bud=dados.get("volume_bud"), dim=_dim_t,
                    )
                except Exception:
                    _wf_bt = {}
                _ofc_type_data[f"wf_budget_{_sfx}_labels"] = _wf_bt.get("labels", [])
                _ofc_type_data[f"wf_budget_{_sfx}_values"] = [float(v) for v in _wf_bt.get("values", [])]

                _wf_mt: dict[str, Any] = {}
                if not sem_mes_anterior:
                    try:
                        _wf_mt = calcular_waterfall_mensal_cpu(
                            custo_real=_cr_ofc,
                            custo_ant=dados_ofc.get("custo_real_ant"),
                            vol_real=dados.get("volume_real"),
                            vol_ant=dados.get("volume_real_ant"),
                            label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
                            label_real=mes_nome, dim=_dim_t,
                        )
                    except Exception:
                        _wf_mt = {}
                _ofc_type_data[f"wf_mensal_{_sfx}_labels"] = _wf_mt.get("labels", [])
                _ofc_type_data[f"wf_mensal_{_sfx}_values"] = [float(v) for v in _wf_mt.get("values", [])]

                _wf_at: dict[str, Any] = {}
                if not sem_ano_anterior and custo_aa_ofc is not None:
                    try:
                        _wf_at = calcular_waterfall_mensal_cpu(
                            custo_real=_cr_ofc, custo_ant=custo_aa_ofc,
                            vol_real=dados.get("volume_real"),
                            vol_ant=vol_ano_ant,
                            label_ant=f"{mes_nome}/{_ano_ant}",
                            label_real=f"{mes_nome}/{ano}", dim=_dim_t,
                        )
                    except Exception:
                        _wf_at = {}
                _ofc_type_data[f"wf_ano_ant_{_sfx}_labels"] = _wf_at.get("labels", [])
                _ofc_type_data[f"wf_ano_ant_{_sfx}_values"] = [float(v) for v in _wf_at.get("values", [])]

            dados_graficos["oficinas"][ofc] = {
                "wf_budget_labels": wf_ofc_budget.get("labels", []),
                "wf_budget_values": [float(v) for v in wf_ofc_budget.get("values", [])],
                "wf_mensal_labels": wf_ofc_mensal.get("labels", []),
                "wf_mensal_values": [float(v) for v in wf_ofc_mensal.get("values", [])],
                "wf_ano_ant_labels": wf_ofc_ano_ant.get("labels", []),
                "wf_ano_ant_values": [float(v) for v in wf_ofc_ano_ant.get("values", [])],
                "ano": ano,
                "ano_anterior": dados.get("ano_anterior", ano - 1),
                **_ofc_type_data,
            }
        except Exception as e:
            logger.warning("Falha ao coletar dados de gráfico para oficina %s: %s", ofc, e)

    # 5c. Finalizar oficinas_intro (agora que dados_graficos existe)
    _ofc_resumo_tmp = secoes_geradas.pop("_oficinas_resumo_ia", None)
    if _ofc_resumo_tmp is not None:
        try:
            from tc_copilot.text_templates import gerar_texto_intro_oficinas as _gen_intro
            secoes_geradas["oficinas_intro"] = _gen_intro(
                oficinas_resumo=_ofc_resumo_tmp,
                dados_graficos=dados_graficos,
                mes_nome=mes_nome,
                ano=ano,
                moeda=moeda,
                simbolo=simbolo,
            )
        except Exception as e:
            logger.warning("Falha ao gerar oficinas_intro (IA): %s", e)

    # 5d. Pré-calcular tabelas de análise detalhada
    try:
        _labels_tab = LABELS.get(idioma, LABELS["pt-BR"])
        _g = dados_graficos["global"]
        # Tabelas globais (com coluna Oficina)
        _g["tabelas"] = _calcular_tabelas_secao(
            custo_real=dados.get("custo_real"),
            wf_budget_labels=_g.get("wf_budget_labels", []),
            wf_budget_values=_g.get("wf_budget_values", []),
            colunas=_TABELA_COLS_GLOBAL,
            labels_dict=_labels_tab,
        )
        # Tabelas por oficina (sem coluna Oficina)
        for ofc in oficinas:
            _ofc_graf = dados_graficos.get("oficinas", {}).get(ofc, {})
            _dados_ofc = _filtrar_por_oficina(dados, ofc)
            _ofc_graf["tabelas"] = _calcular_tabelas_secao(
                custo_real=_dados_ofc.get("custo_real"),
                wf_budget_labels=_ofc_graf.get("wf_budget_labels", []),
                wf_budget_values=_ofc_graf.get("wf_budget_values", []),
                colunas=_TABELA_COLS_OFC,
                labels_dict=_labels_tab,
                prefixo_titulo=ofc,
            )
    except Exception as e:
        logger.warning("Falha ao calcular tabelas (IA): %s", e)

    # 6. Salvar no JSON intermediário (inclui dados de gráficos)
    adicionar_mes_ao_relatorio(ano, mes_numero, secoes_geradas, dados_graficos)

    # 7. PDF mensal individual + registro no banco
    try:
        from tc_copilot.relatorios_db import registrar_pdf
        pdf_mensal = gerar_pdf_mensal(ano, mes_numero, modo="ia", idioma=idioma, simbolo_moeda=simbolo)
        if pdf_mensal:
            registrar_pdf(ano, mes_numero, modo="ia", moeda=moeda, caminho=pdf_mensal)
    except Exception as e:
        logger.warning("Falha ao gerar PDF mensal (IA) mês %s: %s", mes_numero, e)

    # 8. Gerar PDF completo (cumulativo)
    pdf_anual = gerar_pdf(ano, idioma, simbolo_moeda=simbolo)

    # 9. Registrar PDF anual no banco
    try:
        from tc_copilot.relatorios_db import registrar_pdf as _reg
        _reg(ano, 0, modo="ia", moeda=moeda, caminho=str(pdf_anual) if pdf_anual else "")
    except Exception as _e:
        logger.warning("Falha ao registrar PDF anual (IA): %s", _e)

    return pdf_anual


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


# ═══════════════════════════════════════════════════════════════
#  PDF MENSAL INDIVIDUAL (1 mês = 1 PDF)
# ═══════════════════════════════════════════════════════════════

def gerar_pdf_mensal(
    ano: int,
    mes_numero: int,
    modo: str = "local",
    idioma: str = "pt-BR",
    simbolo_moeda: str = "€",
) -> str | None:
    """Gera PDF individual de um único mês.

    Reutiliza ``_construir_capitulo_mes`` para montar o conteúdo.
    O PDF inclui uma mini-capa e o capítulo completo (gráficos + texto).

    Args:
        ano: Ano.
        mes_numero: Mês (1-12).
        modo: 'local' ou 'ia'.
        idioma: 'pt-BR' ou 'en'.
        simbolo_moeda: Símbolo da moeda para labels de CPU no PDF.

    Returns:
        Caminho absoluto do PDF gerado, ou None em caso de erro.
    """
    garantir_pasta_relatorios()

    # Carregar o JSON correto
    if modo == "local":
        dados_relatorio = carregar_dados_relatorio_local(ano)
    else:
        dados_relatorio = carregar_dados_relatorio(ano)

    info_mes = dados_relatorio.get("meses", {}).get(str(mes_numero))
    if not info_mes:
        logger.warning("gerar_pdf_mensal: mês %s não encontrado no JSON (%s)", mes_numero, modo)
        return None

    mes_nome = info_mes.get("mes_nome", obter_nome_mes(mes_numero, idioma))
    estilos = _criar_estilos()

    pdf_path = str(caminho_relatorio_mensal(ano, mes_numero, modo))

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Relatório TC — {mes_nome}/{ano}",
        author="SCI — TC Copilot",
    )

    elements: list = []

    # ── Mini-capa ──
    elements.append(Spacer(1, 4 * cm))
    elements.append(Paragraph(
        "Stellantis Cost Intelligence",
        estilos.get("titulo_capa", estilos.get("titulo_capitulo")),
    ))
    elements.append(Spacer(1, 0.8 * cm))
    elements.append(Paragraph(
        f"Relatório Mensal — {mes_nome} / {ano}",
        estilos.get("subtitulo_capa", estilos.get("titulo_capitulo")),
    ))
    elements.append(Spacer(1, 0.5 * cm))
    modo_label = "Automático" if modo == "local" else "Com IA"
    elements.append(Paragraph(
        f"Modo: {modo_label} | Moeda: {simbolo_moeda}",
        estilos.get("corpo", estilos.get("titulo_capitulo")),
    ))
    elements.append(Spacer(1, 1.5 * cm))

    # ── Logo SCI (faixa) ──
    logo_faixa = ROOT / "SCI_faixa.png"
    if logo_faixa.exists():
        img_w = 14 * cm
        img_h = img_w * (457 / 1240)
        elements.append(Image(
            str(logo_faixa), width=img_w, height=img_h,
        ))

    elements.append(PageBreak())

    # ── Sumário do mês ──
    _construir_sumario_mensal(
        elements, estilos, mes_numero, info_mes, idioma,
    )

    # ── Capítulo do mês ──
    _construir_capitulo_mes(elements, estilos, mes_numero, info_mes, idioma, simbolo_moeda)

    # ── Build ──
    def _on_page(canvas, doc_):
        _header_footer(canvas, doc_, ano)

    def _on_first_page(canvas, doc_):
        pass

    try:
        doc.build(elements, onFirstPage=_on_first_page, onLaterPages=_on_page)
        logger.info("PDF mensal gerado: %s", pdf_path)
    except Exception as e:
        logger.error("Erro ao gerar PDF mensal %s/%s: %s", mes_numero, ano, e)
        return None

    return pdf_path


def gerar_relatorio_mes_local(
    ano: int,
    mes_numero: int,
    idioma: str = "pt-BR",
    moeda: str = "EUR",
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
        configurar_moeda_formatacao,
        descobrir_oficinas,
        formatar_dados_oficina,
    )
    from tc_copilot.text_templates import gerar_todas_secoes_local
    from tc_core.finance.currency import converter_coluna_moeda, obter_simbolo_moeda

    if taxas is None:
        taxas = {}
    # Carregar taxas do banco se não fornecidas e moeda != BRL
    if moeda != "BRL" and not taxas.get(moeda):
        try:
            from tc_core.finance.currency_db import carregar_taxas_banco
            _taxas_banco = carregar_taxas_banco()
            for _mk, _mv in _taxas_banco.items():
                taxas.setdefault(_mk, 1.0 / _mv if _mv > 0 else 1.0)
        except Exception:
            taxas.setdefault("USD", 0.20)
            taxas.setdefault("EUR", 0.18)
    simbolo = obter_simbolo_moeda(moeda)

    # Configurar moeda ativa para formatação automática
    configurar_moeda_formatacao(moeda, simbolo)

    # 1. Coletar dados
    dados = coletar_dados_mes(ano, mes_numero)

    # 1b. Converter custos para moeda selecionada (se != BRL)
    if moeda != "BRL":
        _colunas_custo = ["Custo FP", "Custo MP", "Custo Log.", "Custo Emb.",
                          "Custo Total", "Amort. Fer.", "Amort. Eng.",
                          "Delta Volume", "Delta Mix"]
        for chave_df in ("custo_real", "custo_bud", "custo_real_ant",
                         "custo_fp_real", "custo_fp_bud",
                         "cpu_real", "cpu_bud",
                         "_custo_bud_full", "_custo_real_full",
                         "historico_custo"):
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

    # Extrair volume por veículo para gráfico de barras
    _var_modelos = variacoes.get("variacao_modelos", {})
    _vol_real_modelos = {m: float(v.get("vol_real", 0)) for m, v in _var_modelos.items() if v.get("vol_real", 0) > 0}
    _vol_bud_modelos = {m: float(v.get("vol_budget", 0)) for m, v in _var_modelos.items() if v.get("vol_budget", 0) > 0}

    dados_graficos: dict[str, Any] = {
        "global": {
            "wf_budget_labels": wf_budget.get("labels", []),
            "wf_budget_values": [float(v) for v in wf_budget.get("values", [])],
            "wf_mensal_labels": wf_mensal.get("labels", []),
            "wf_mensal_values": [float(v) for v in wf_mensal.get("values", [])],
            "wf_ano_ant_labels": wf_ano_ant.get("labels", []),
            "wf_ano_ant_values": [float(v) for v in wf_ano_ant.get("values", [])],
            "vol_modelos_real": _vol_real_modelos,
            "vol_modelos_budget": _vol_bud_modelos,
            "ano": ano,
            "ano_anterior": dados.get("ano_anterior", ano - 1),
        },
        "oficinas": {},
    }

    # Waterfalls Type 05 e Type 06 (side-by-side no PDF)
    _cr = dados.get("custo_real")
    _cb = dados.get("custo_bud")
    _vr = dados.get("volume_real")
    _vb = dados.get("volume_bud")
    for _dim_t in ("Type 05", "Type 06"):
        _sfx = _dim_t.replace(" ", "").lower()
        try:
            _wf_b = calcular_waterfall_budget_cpu(
                custo_real=_cr, custo_bud=_cb,
                vol_real=_vr, vol_bud=_vb, dim=_dim_t,
            )
        except Exception:
            _wf_b = {}
        dados_graficos["global"][f"wf_budget_{_sfx}_labels"] = _wf_b.get("labels", [])
        dados_graficos["global"][f"wf_budget_{_sfx}_values"] = [
            float(v) for v in _wf_b.get("values", [])]
        _wf_m: dict[str, Any] = {}
        if not sem_mes_anterior:
            try:
                _wf_m = calcular_waterfall_mensal_cpu(
                    custo_real=_cr,
                    custo_ant=dados.get("custo_real_ant"),
                    vol_real=_vr,
                    vol_ant=dados.get("volume_real_ant"),
                    label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
                    label_real=mes_nome, dim=_dim_t,
                )
            except Exception:
                _wf_m = {}
        dados_graficos["global"][f"wf_mensal_{_sfx}_labels"] = _wf_m.get("labels", [])
        dados_graficos["global"][f"wf_mensal_{_sfx}_values"] = [
            float(v) for v in _wf_m.get("values", [])]
        _wf_a: dict[str, Any] = {}
        if not sem_ano_anterior and custo_ano_ant is not None:
            try:
                _wf_a = calcular_waterfall_mensal_cpu(
                    custo_real=_cr, custo_ant=custo_ano_ant,
                    vol_real=_vr, vol_ant=vol_ano_ant,
                    label_ant=f"{mes_nome}/{_ano_ant}",
                    label_real=f"{mes_nome}/{ano}", dim=_dim_t,
                )
            except Exception:
                _wf_a = {}
        dados_graficos["global"][f"wf_ano_ant_{_sfx}_labels"] = _wf_a.get("labels", [])
        dados_graficos["global"][f"wf_ano_ant_{_sfx}_values"] = [
            float(v) for v in _wf_a.get("values", [])]

    # Waterfall global com dim="Oficina" (para seção 4 do relatório)
    try:
        wf_oficinas_global = calcular_waterfall_budget_cpu(
            custo_real=dados.get("custo_real"),
            custo_bud=dados.get("custo_bud"),
            vol_real=dados.get("volume_real"),
            vol_bud=dados.get("volume_bud"),
            dim="Oficina",
        )
        dados_graficos["global"]["wf_oficinas_labels"] = wf_oficinas_global.get("labels", [])
        dados_graficos["global"]["wf_oficinas_values"] = [float(v) for v in wf_oficinas_global.get("values", [])]
    except Exception as e:
        logger.warning("Falha ao gerar waterfall global por oficina (local): %s", e)
        dados_graficos["global"]["wf_oficinas_labels"] = []
        dados_graficos["global"]["wf_oficinas_values"] = []

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

            # 4) Waterfalls Type 05 e Type 06 por oficina
            _ofc_type_data_l: dict[str, Any] = {}
            _cr_ofc_l = dados_ofc.get("custo_real")
            _cb_ofc_l = dados_ofc.get("custo_bud")
            for _dim_t in ("Type 05", "Type 06"):
                _sfx = _dim_t.replace(" ", "").lower()
                try:
                    _wf_bt = calcular_waterfall_budget_cpu(
                        custo_real=_cr_ofc_l, custo_bud=_cb_ofc_l,
                        vol_real=dados.get("volume_real"),
                        vol_bud=dados.get("volume_bud"), dim=_dim_t,
                    )
                except Exception:
                    _wf_bt = {}
                _ofc_type_data_l[f"wf_budget_{_sfx}_labels"] = _wf_bt.get("labels", [])
                _ofc_type_data_l[f"wf_budget_{_sfx}_values"] = [float(v) for v in _wf_bt.get("values", [])]

                _wf_mt: dict[str, Any] = {}
                if not sem_mes_anterior:
                    try:
                        _wf_mt = calcular_waterfall_mensal_cpu(
                            custo_real=_cr_ofc_l,
                            custo_ant=dados_ofc.get("custo_real_ant"),
                            vol_real=dados.get("volume_real"),
                            vol_ant=dados.get("volume_real_ant"),
                            label_ant=dados.get("mes_nome_anterior", "Mês Ant"),
                            label_real=mes_nome, dim=_dim_t,
                        )
                    except Exception:
                        _wf_mt = {}
                _ofc_type_data_l[f"wf_mensal_{_sfx}_labels"] = _wf_mt.get("labels", [])
                _ofc_type_data_l[f"wf_mensal_{_sfx}_values"] = [float(v) for v in _wf_mt.get("values", [])]

                _wf_at: dict[str, Any] = {}
                if not sem_ano_anterior and custo_aa_ofc is not None:
                    try:
                        _wf_at = calcular_waterfall_mensal_cpu(
                            custo_real=_cr_ofc_l, custo_ant=custo_aa_ofc,
                            vol_real=dados.get("volume_real"),
                            vol_ant=vol_ano_ant,
                            label_ant=f"{mes_nome}/{_ano_ant}",
                            label_real=f"{mes_nome}/{ano}", dim=_dim_t,
                        )
                    except Exception:
                        _wf_at = {}
                _ofc_type_data_l[f"wf_ano_ant_{_sfx}_labels"] = _wf_at.get("labels", [])
                _ofc_type_data_l[f"wf_ano_ant_{_sfx}_values"] = [float(v) for v in _wf_at.get("values", [])]

            dados_graficos["oficinas"][ofc] = {
                "wf_budget_labels": wf_ofc_budget.get("labels", []),
                "wf_budget_values": [float(v) for v in wf_ofc_budget.get("values", [])],
                "wf_mensal_labels": wf_ofc_mensal.get("labels", []),
                "wf_mensal_values": [float(v) for v in wf_ofc_mensal.get("values", [])],
                "wf_ano_ant_labels": wf_ofc_ano_ant.get("labels", []),
                "wf_ano_ant_values": [float(v) for v in wf_ofc_ano_ant.get("values", [])],
                "ano": ano,
                "ano_anterior": dados.get("ano_anterior", ano - 1),
                **_ofc_type_data_l,
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

    # 4b. Pré-calcular tabelas de análise detalhada
    try:
        _labels_tab = LABELS.get(idioma, LABELS["pt-BR"])
        _g = dados_graficos["global"]
        _g["tabelas"] = _calcular_tabelas_secao(
            custo_real=dados.get("custo_real"),
            wf_budget_labels=_g.get("wf_budget_labels", []),
            wf_budget_values=_g.get("wf_budget_values", []),
            colunas=_TABELA_COLS_GLOBAL,
            labels_dict=_labels_tab,
        )
        for ofc in oficinas:
            _ofc_graf = dados_graficos.get("oficinas", {}).get(ofc, {})
            _dados_ofc = _filtrar_por_oficina(dados, ofc)
            _ofc_graf["tabelas"] = _calcular_tabelas_secao(
                custo_real=_dados_ofc.get("custo_real"),
                wf_budget_labels=_ofc_graf.get("wf_budget_labels", []),
                wf_budget_values=_ofc_graf.get("wf_budget_values", []),
                colunas=_TABELA_COLS_OFC,
                labels_dict=_labels_tab,
                prefixo_titulo=ofc,
            )
    except Exception as e:
        logger.warning("Falha ao calcular tabelas (local): %s", e)

    # 5. Salvar no JSON local
    adicionar_mes_ao_relatorio_local(ano, mes_numero, secoes_geradas, dados_graficos)

    # 6. PDF mensal individual + registro no banco
    try:
        from tc_copilot.relatorios_db import registrar_pdf
        pdf_mensal = gerar_pdf_mensal(ano, mes_numero, modo="local", idioma=idioma, simbolo_moeda=simbolo)
        if pdf_mensal:
            registrar_pdf(ano, mes_numero, modo="local", moeda=moeda, caminho=pdf_mensal)
    except Exception as e:
        logger.warning("Falha ao gerar PDF mensal (local) mês %s: %s", mes_numero, e)

    # 7. Gerar PDF local (cumulativo)
    pdf_anual = gerar_pdf_local(ano, idioma, simbolo_moeda=simbolo)

    # 8. Registrar PDF anual no banco
    try:
        from tc_copilot.relatorios_db import registrar_pdf as _reg
        _reg(ano, 0, modo="local", moeda=moeda, caminho=str(pdf_anual) if pdf_anual else "")
    except Exception as _e:
        logger.warning("Falha ao registrar PDF anual (local): %s", _e)

    return pdf_anual
