"""
TC Copilot — Geração de gráficos waterfall para PDF.

Replica a lógica completa do waterfall_tc.py (tabs Budget e Real),
agrupando por Type 05, em modo CPU (R$/veíc).

Usa matplotlib (backend Agg) e exporta como PNG em bytes para
inclusão no relatório PDF com ReportLab.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  CORES (mesmas do app — waterfall_tc.py)
# ═══════════════════════════════════════════════════════════════
COR_VERMELHA = "#ff5733"    # Aumento de custo (desfavorável)
COR_VERDE = "#1e8449"       # Redução de custo (favorável)
COR_AZUL = "#1e6ba8"        # Barras totais (Budget, Real)
COR_AMARELA = "#ffd700"     # Efeito Flex Volume
COR_LARANJA = "#ff9800"     # Outros / Secundário
COR_FUNDO = "#FFFFFF"       # Fundo branco
COR_TITULO = "#003366"      # Azul escuro corporativo


# ═══════════════════════════════════════════════════════════════
#  HELPERS INTERNOS  (flex por categoria, CPU split)
# ═══════════════════════════════════════════════════════════════

def _is_fixo(serie: pd.Series) -> pd.Series:
    """Retorna máscara booleana: True onde Custo == 'Fixo'."""
    return serie.astype(str).str.strip().str.lower() == "fixo"


def _safe_vol(df: pd.DataFrame | None) -> float:
    """Soma Volume de um DataFrame, retornando 0 se vazio/None."""
    if df is None or df.empty or "Volume" not in df.columns:
        return 0.0
    return float(df["Volume"].sum())


# ═══════════════════════════════════════════════════════════════
#  CÁLCULO: WATERFALL BUDGET → CPU  (Tab Budget do waterfall_tc)
# ═══════════════════════════════════════════════════════════════

def calcular_waterfall_budget_cpu(
    custo_real: pd.DataFrame | None,
    custo_bud: pd.DataFrame | None,
    vol_real: pd.DataFrame | None,
    vol_bud: pd.DataFrame | None,
    dim: str = "Account",
) -> dict[str, Any]:
    """
    Calcula arrays do waterfall Budget completo em CPU (R$/veíc).

    Mesma lógica do waterfall_tc.py tab Budget:
      BUD → Flex Bud – BUD → [deltas por categoria dim] → Outros → Real

    Todos os valores são em R$/veíc (Custo FP / Volume).

    Returns:
        dict: { "labels": [...], "values": [...] }
        Onde values[0] e values[-1] são absolutos e o resto relativo.
    """
    empty = {"labels": [], "values": []}

    if custo_real is None or custo_bud is None:
        return empty
    if custo_real.empty or custo_bud.empty:
        return empty
    if "Custo FP" not in custo_real.columns or "Custo FP" not in custo_bud.columns:
        return empty

    v_real = _safe_vol(vol_real)
    v_bud = _safe_vol(vol_bud)
    if v_real == 0 or v_bud == 0:
        return empty

    # ── Merge Real + Budget por dimensões (mesma lógica waterfall_tc) ──
    cols_merge = [c for c in [dim, "Custo"] if c in custo_real.columns and c in custo_bud.columns]
    if dim not in cols_merge:
        return empty

    real_grp = custo_real.groupby(cols_merge, as_index=False)["Custo FP"].sum()
    bud_grp = custo_bud.groupby(cols_merge, as_index=False)["Custo FP"].sum()

    df = real_grp.merge(bud_grp, on=cols_merge, how="outer", suffixes=("_Real", "_Bud"))
    df["Custo FP_Real"] = df["Custo FP_Real"].fillna(0)
    df["Custo FP_Bud"] = df["Custo FP_Bud"].fillna(0)

    # ── Flex por linha (mesma regra: Fixo inalterado, Variável × proporção) ──
    proporcao = v_real / v_bud
    if "Custo" in df.columns:
        fixo_mask = _is_fixo(df["Custo"])
        df["Flex_Bud_Custo"] = np.where(
            fixo_mask,
            df["Custo FP_Bud"],
            df["Custo FP_Bud"] * proporcao,
        )
    else:
        df["Flex_Bud_Custo"] = df["Custo FP_Bud"] * proporcao

    # ── Converter para CPU ──
    df["BUD_cpu"] = df["Custo FP_Bud"] / v_bud
    df["Flex_cpu"] = df["Flex_Bud_Custo"] / v_real
    df["Real_cpu"] = df["Custo FP_Real"] / v_real

    # ── Agrupar por dimensão (somando Fixo+Variável) ──
    agg = df.groupby(dim, as_index=False).agg(
        BUD_cpu=("BUD_cpu", "sum"),
        Flex_cpu=("Flex_cpu", "sum"),
        Real_cpu=("Real_cpu", "sum"),
    )
    agg["Delta"] = agg["Real_cpu"] - agg["Flex_cpu"]

    # Totais
    bud_total_cpu = float(agg["BUD_cpu"].sum())
    flex_total_cpu = float(agg["Flex_cpu"].sum())
    real_total_cpu = float(agg["Real_cpu"].sum())
    flex_minus_bud = flex_total_cpu - bud_total_cpu

    # ── Categorias com delta ≠ 0, ordenadas por |delta| ──
    cats = agg[agg["Delta"].abs() > 1e-9].copy()
    cats = cats.sort_values("Delta", key=abs, ascending=False)

    labels_cats = cats[dim].astype(str).tolist()
    values_cats = cats["Delta"].tolist()

    # ── Limitar categorias; excedente vai para "Outros" ──
    # 15 barras max = BUD(1) + Flex(1) + cats + Outros(1) + Real(1)
    MAX_CAT_BARS = 11
    if len(labels_cats) > MAX_CAT_BARS:
        labels_cats_top = labels_cats[:MAX_CAT_BARS]
        values_cats_top = values_cats[:MAX_CAT_BARS]
    else:
        labels_cats_top = labels_cats
        values_cats_top = values_cats

    # Remainder = tudo que falta p/ fechar Real (categorias cortadas + arredondamento)
    remainder = round(
        real_total_cpu - (bud_total_cpu + flex_minus_bud + sum(values_cats_top)), 6
    )

    # ── Montar waterfall ──
    labels = ["BUD"]
    values = [bud_total_cpu]

    if abs(flex_minus_bud) > 1e-10:
        labels.append("Flex Bud\n− BUD")
        values.append(flex_minus_bud)

    labels.extend(labels_cats_top)
    values.extend(values_cats_top)

    if abs(remainder) >= 0.01:
        labels.append("Outros")
        values.append(remainder)

    labels.append("Real")
    values.append(real_total_cpu)

    return {"labels": labels, "values": values}


# ═══════════════════════════════════════════════════════════════
#  CÁLCULO: WATERFALL MENSAL → CPU (Tab Real do waterfall_tc)
# ═══════════════════════════════════════════════════════════════

def calcular_waterfall_mensal_cpu(
    custo_real: pd.DataFrame | None,
    custo_ant: pd.DataFrame | None,
    vol_real: pd.DataFrame | None,
    vol_ant: pd.DataFrame | None,
    label_ant: str = "Mês Ant",
    label_real: str = "Real",
    dim: str = "Account",
) -> dict[str, Any]:
    """
    Calcula arrays do waterfall mensal completo em CPU (R$/veíc).

    Mesma lógica do waterfall_tc.py tab Real (Mês a Mês):
      Mês Ant → Flex Mês Ant − Mês Ant → [deltas por categoria] → Outros → Real

    Flex por categoria usa separação Fixo/Variável com volume do mês real.

    Returns:
        dict: { "labels": [...], "values": [...] }
    """
    empty = {"labels": [], "values": []}

    if custo_real is None or custo_ant is None:
        return empty
    if custo_real.empty or custo_ant.empty:
        return empty
    if "Custo FP" not in custo_real.columns or "Custo FP" not in custo_ant.columns:
        return empty

    v_real = _safe_vol(vol_real)
    v_ant = _safe_vol(vol_ant)
    if v_real == 0 or v_ant == 0:
        return empty

    proporcao = v_real / v_ant

    # ── Totais CPU ──
    total_ant_custo = float(custo_ant["Custo FP"].sum())
    total_real_custo = float(custo_real["Custo FP"].sum())
    bud_cpu = total_ant_custo / v_ant   # "Mês Anterior" em CPU
    real_cpu = total_real_custo / v_real

    # ── Flex total (Fixo + Variável × proporcao) / v_real ──
    if "Custo" in custo_ant.columns:
        fixo_total = float(custo_ant.loc[_is_fixo(custo_ant["Custo"]), "Custo FP"].sum())
        var_total = total_ant_custo - fixo_total
    else:
        fixo_total = 0.0
        var_total = total_ant_custo
    flex_total_custo = fixo_total + var_total * proporcao
    flex_cpu = flex_total_custo / v_real
    flex_volume_delta = flex_cpu - bud_cpu

    # ── CPU por categoria ──
    has_custo = "Custo" in custo_ant.columns and "Custo" in custo_real.columns
    cols_grp_ant = [dim] if dim in custo_ant.columns else []
    cols_grp_real = [dim] if dim in custo_real.columns else []
    if not cols_grp_ant or not cols_grp_real:
        return empty

    g_ant = custo_ant.groupby(dim)["Custo FP"].sum()
    g_real = custo_real.groupby(dim)["Custo FP"].sum()

    # g1 (Mês Ant CPU) e g2 (Real CPU) — agrupados por dim (volume total, não por cat)
    g1 = g_ant / v_ant
    g2 = g_real / v_real

    # ── Flex por categoria (mesma lógica waterfall_tc) ──
    g_flex: dict[str, float] = {}
    all_cats = sorted(set(g_ant.index.tolist() + g_real.index.tolist()), key=str)

    for cat in all_cats:
        if cat not in g_ant.index:
            g_flex[cat] = 0.0
            continue

        df_cat = custo_ant[custo_ant[dim].astype(str) == str(cat)]
        if df_cat.empty:
            g_flex[cat] = 0.0
            continue

        if has_custo and "Custo" in df_cat.columns:
            fixo_cat = float(df_cat.loc[_is_fixo(df_cat["Custo"]), "Custo FP"].sum())
            var_cat = float(df_cat["Custo FP"].sum()) - fixo_cat
        else:
            fixo_cat = 0.0
            var_cat = float(df_cat["Custo FP"].sum())

        flex_cat_custo = fixo_cat + var_cat * proporcao
        g_flex[cat] = flex_cat_custo / v_real if v_real != 0 else 0.0

    # ── Deltas por categoria: g2 − flex ──
    labels_cats: list[str] = []
    values_cats: list[float] = []
    for cat in all_cats:
        val_real_cat = float(g2.get(cat, 0.0))
        val_flex_cat = float(g_flex.get(cat, 0.0))
        delta = val_real_cat - val_flex_cat
        if abs(delta) > 1e-9:
            labels_cats.append(str(cat))
            values_cats.append(delta)

    # Ordenar por |delta|
    if labels_cats:
        idx_sorted = sorted(range(len(values_cats)), key=lambda i: abs(values_cats[i]), reverse=True)
        labels_cats = [labels_cats[i] for i in idx_sorted]
        values_cats = [values_cats[i] for i in idx_sorted]

    # ── Limitar categorias; excedente vai para "Outros" ──
    MAX_CAT_BARS = 11
    if len(labels_cats) > MAX_CAT_BARS:
        labels_cats_top = labels_cats[:MAX_CAT_BARS]
        values_cats_top = values_cats[:MAX_CAT_BARS]
    else:
        labels_cats_top = labels_cats
        values_cats_top = values_cats

    remainder = round(
        real_cpu - (bud_cpu + flex_volume_delta + sum(values_cats_top)), 6
    )

    # ── Montar waterfall ──
    labels: list[str] = [label_ant]
    values: list[float] = [bud_cpu]

    if abs(flex_volume_delta) > 1e-10:
        labels.append("Flex Vol")
        values.append(flex_volume_delta)

    labels.extend(labels_cats_top)
    values.extend(values_cats_top)

    if abs(remainder) >= 0.01:
        labels.append("Outros")
        values.append(remainder)

    labels.append(label_real)
    values.append(real_cpu)

    return {"labels": labels, "values": values}


# ═══════════════════════════════════════════════════════════════
#  GERAR PNG A PARTIR DE ARRAYS PRÉ-CALCULADOS
# ═══════════════════════════════════════════════════════════════

def gerar_waterfall_from_arrays(
    wf_data: dict[str, Any],
    titulo: str = "Waterfall",
    width: int = 14,
    height: int = 5,
    transparent: bool = False,
) -> Optional[bytes]:
    """
    Renderiza waterfall a partir de arrays pré-calculados
    (saída de ``calcular_waterfall_*``).

    Aplica cores automaticamente:
      - Primeira e última barras: azul (absolutos)
      - Barra "Flex…" / "Flex Vol": amarelo
      - Barras positivas (aumento): vermelho
      - Barras negativas (redução): verde
      - "Outros": laranja
    """
    labels = wf_data.get("labels", [])
    values = wf_data.get("values", [])
    if not labels or len(labels) < 3:
        return None

    n = len(values)
    cores: list[str] = []
    for i, (lbl, val) in enumerate(zip(labels, values)):
        if i == 0 or i == n - 1:
            cores.append(COR_AZUL)
        elif "flex" in lbl.lower():
            cores.append(COR_AMARELA)
        elif lbl == "Outros":
            cores.append(COR_LARANJA)
        elif val >= 0:
            cores.append(COR_VERMELHA)
        else:
            cores.append(COR_VERDE)

    return _render_waterfall(labels, values, cores, titulo, width, height,
                             y_label="R$/veíc", transparent=transparent)


# ═══════════════════════════════════════════════════════════════
#  RENDERIZAÇÃO MATPLOTLIB
# ═══════════════════════════════════════════════════════════════

def _render_waterfall(
    labels: list[str],
    values: list[float],
    cores: list[str],
    titulo: str,
    width: int = 10,
    height: int = 4,
    y_label: str = "R$/veíc",
    transparent: bool = False,
) -> Optional[bytes]:
    """
    Renderiza gráfico estilo waterfall usando matplotlib.

    - Primeira e última barras: "absolutas" (base em 0)
    - Barras intermediárias: "relativas" (empilhadas sobre a anterior)

    Returns:
        Bytes PNG ou None se falhar.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # Backend sem GUI
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        logger.warning("matplotlib não disponível — gráfico waterfall não gerado.")
        return None

    try:
        n = len(values)
        # Calcular bases e alturas para cada barra
        bases: list[float] = []
        heights: list[float] = []
        cumulative = 0.0

        for i, val in enumerate(values):
            if i == 0 or i == n - 1:
                # Barra absoluta (começa no 0)
                bases.append(0)
                heights.append(val)
                if i == 0:
                    cumulative = val
            else:
                # Barra relativa (empilhada)
                if val >= 0:
                    bases.append(cumulative)
                    heights.append(val)
                else:
                    bases.append(cumulative + val)
                    heights.append(abs(val))
                cumulative += val

        # Criar figura
        fig, ax = plt.subplots(figsize=(width, height))
        bg = "none" if transparent else COR_FUNDO
        fig.patch.set_facecolor(bg)
        fig.patch.set_alpha(0.0 if transparent else 1.0)
        ax.set_facecolor(bg)
        ax.patch.set_alpha(0.0 if transparent else 1.0)
        # Cor de texto adaptável ao fundo
        txt_color = "#E0E0E0" if transparent else "#333333"
        txt_color_secondary = "#AAAAAA" if transparent else "#666666"

        x_pos = range(n)
        # Largura adaptável ao nº de barras
        bar_width = max(0.30, min(0.55, 8.0 / max(n, 1)))

        # Desenhar barras
        ax.bar(
            x_pos,
            heights,
            bottom=bases,
            width=bar_width,
            color=cores,
            edgecolor="none",
            zorder=3,
        )

        # Conectores (linhas tracejadas entre barras)
        for i in range(n - 1):
            top_i = bases[i] + heights[i]
            ax.plot(
                [i + bar_width / 2, i + 1 - bar_width / 2],
                [top_i, top_i],
                color="#CCCCCC",
                linewidth=0.8,
                linestyle="--",
                zorder=2,
            )

        # Formato numérico adaptável
        max_abs = max(abs(v) for v in values) if values else 1
        if max_abs >= 100:
            fmt_abs = lambda v: f"{v:,.0f}"
            fmt_rel = lambda v: f"{'+' if v >= 0 else ''}{v:,.0f}"
        elif max_abs >= 1:
            fmt_abs = lambda v: f"{v:,.1f}"
            fmt_rel = lambda v: f"{'+' if v >= 0 else ''}{v:,.1f}"
        else:
            fmt_abs = lambda v: f"{v:,.2f}"
            fmt_rel = lambda v: f"{'+' if v >= 0 else ''}{v:,.2f}"

        # Anotações de valor
        # Tamanho da fonte — maior com 15 barras
        fsize = max(7, min(10, 150 // max(n, 1)))
        for i, val_original in enumerate(values):
            top = bases[i] + heights[i]
            if i == 0 or i == n - 1:
                txt = fmt_abs(val_original)
                y_pos = top
                va = "bottom"
                cor_txt = COR_AZUL
            else:
                txt = fmt_rel(val_original)
                if val_original >= 0:
                    y_pos = top
                    va = "bottom"
                else:
                    y_pos = bases[i]
                    va = "top"
                cor_txt = cores[i]

            ax.annotate(
                txt,
                xy=(i, y_pos),
                ha="center",
                va=va,
                fontsize=fsize,
                fontweight="bold",
                color=cor_txt,
                xytext=(0, 4 if va == "bottom" else -4),
                textcoords="offset points",
            )

        # Eixos e título
        ax.set_xticks(list(x_pos))
        lbl_fsize = max(7, min(10, 140 // max(n, 1)))
        rotation = 45 if n > 8 else 0
        ha = "right" if rotation > 0 else "center"
        ax.set_xticklabels(labels, fontsize=lbl_fsize, color=txt_color,
                           rotation=rotation, ha=ha)
        ax.set_ylabel(y_label, fontsize=9, color=txt_color_secondary)
        ax.tick_params(axis="y", labelsize=8, colors=txt_color_secondary)
        titulo_color = txt_color if transparent else COR_TITULO
        ax.set_title(titulo, fontsize=11, fontweight="bold", color=titulo_color, pad=10)

        # Grid sutil
        ax.yaxis.grid(True, alpha=0.2, color="#CCCCCC", zorder=1)
        ax.set_axisbelow(True)

        # Remover bordas desnecessárias
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        spine_color = "#555555" if transparent else "#CCCCCC"
        ax.spines["left"].set_color(spine_color)
        ax.spines["bottom"].set_color(spine_color)

        # Formatter para eixo Y
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}"))

        plt.tight_layout()

        # Exportar para bytes
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=200, bbox_inches="tight",
                    facecolor=bg, edgecolor="none", transparent=transparent)
        plt.close(fig)
        buf.seek(0)
        png_bytes = buf.getvalue()
        logger.info("Gráfico waterfall exportado: %d bytes", len(png_bytes))
        return png_bytes

    except Exception as e:
        logger.warning("Falha ao gerar gráfico waterfall: %s", e)
        return None
