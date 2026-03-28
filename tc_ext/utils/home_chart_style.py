"""
Style constants & helpers extracted from TC Veículos (home_tc.py / chart_utils.py).
Applied ONLY to TC Ext (home_ext.py) for visual consistency.
TC Veículos itself is NEVER modified — this module replicates its "Design System".
"""
from __future__ import annotations

import altair as alt
import numpy as np
from tc_core.feature_flags import get_flag as _get_flag

# ═══════════════════════════════════════════════════════════════
#  FEATURE FLAG — rollback instantâneo
# ═══════════════════════════════════════════════════════════════
PADRONIZAR_HOME_TCEXT: bool = str(
    _get_flag("PADRONIZAR_HOME_TCEXT", default="true")
).lower().strip() in ("true", "1", "yes", "")

# ═══════════════════════════════════════════════════════════════
#  PALETA DE CORES  (referência: chart_utils.py + home_tc.py)
# ═══════════════════════════════════════════════════════════════
PURPLE_LIGHT = "#D8B4FE"
PURPLE_DARK = "#4C1D95"
PURPLE_BE = "#C4B5FD"

ORANGE_FLEX = "#FF6B35"

DELTA_GREEN = "#00AA00"   # negativo = bom
DELTA_RED = "#FF0000"     # positivo = ruim
DELTA_LABEL_COLOR = "#000000"  # rótulo do delta (preto para legibilidade)

LABEL_BG = "rgba(220,220,220,0.75)"
LABEL_FG = "#333333"
LABEL_FONT_SIZE = 10

BAR_LABEL_DY = 14  # offset para posicionar no interior inferior da barra (Altair)

# ═══════════════════════════════════════════════════════════════
#  FUNÇÕES DE ESTILO
# ═══════════════════════════════════════════════════════════════

def purple_gradient_scale(values) -> alt.Scale:
    """Retorna escala Altair com degradê roxo claro→escuro baseado no range dos dados."""
    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    v_min = float(v.min()) if len(v) > 0 else 0
    v_max = float(v.max()) if len(v) > 0 else 1
    if v_max == v_min:
        v_max = v_min + 1
    return alt.Scale(
        domain=[v_min, v_max],
        range=[PURPLE_LIGHT, PURPLE_DARK],
        type="linear",
    )


def purple_colorscale_plotly():
    """Colorscale Plotly no degradê roxo (para _plot_rank)."""
    return [
        [0.0, PURPLE_LIGHT],
        [1.0, PURPLE_DARK],
    ]


def flex_line_config() -> dict:
    """Parâmetros do mark_line Altair para linha Flex Bud (estilo TC Veículos)."""
    return dict(
        strokeDash=[4, 4],
        strokeWidth=2,
        opacity=0.9,
    )


def flex_circle_size() -> int:
    """Tamanho do mark_circle para pontos da linha Flex."""
    return 50


def flex_label_config() -> dict:
    """Parâmetros do mark_text Altair para rótulos da Flex."""
    return dict(
        align="center",
        baseline="bottom",
        dy=-12,
        color=ORANGE_FLEX,
        fontSize=11,
        fontWeight="bold",
    )


def delta_bar_size() -> int:
    """Largura (size) das mini-barras de delta no topo."""
    return 15


def delta_label_config_pos() -> dict:
    """mark_text para rótulos do delta (valores positivos, acima)."""
    return dict(
        align="center",
        baseline="bottom",
        dy=-5,
        fontSize=10,
        fontWeight="bold",
        color=DELTA_LABEL_COLOR,
    )


def delta_label_config_neg() -> dict:
    """mark_text para rótulos do delta (valores negativos, abaixo)."""
    return dict(
        align="center",
        baseline="top",
        dy=5,
        fontSize=10,
        fontWeight="bold",
        color=DELTA_LABEL_COLOR,
    )


def apply_chart_height(n_periodos: int, tem_flex: bool) -> int:
    """Calcula altura adaptativa do gráfico (fórmula TC Veículos)."""
    base = min(620, max(350, 22 * n_periodos + 180))
    _altura_base = base + (100 if tem_flex else 0)
    return int(_altura_base * 1.24) if tem_flex else base


def legend_bottom() -> alt.Legend:
    """Legenda horizontal no rodapé do gráfico (estilo TC Veículos)."""
    return alt.Legend(
        orient="bottom",
        direction="horizontal",
        titleAnchor="middle",
        titleFontSize=10,
        labelFontSize=10,
    )


def transparent_view_config() -> dict:
    """Dicionário para .configure_view() com fundo transparente."""
    return dict(
        strokeWidth=0,
    )


def bar_label_bg_config() -> dict:
    """Parâmetros do mark_text Altair para halo cinza atrás dos rótulos de barra."""
    return dict(
        align="center",
        baseline="bottom",
        dy=BAR_LABEL_DY,
        fontSize=LABEL_FONT_SIZE,
        color=LABEL_BG,
        strokeWidth=4,
        stroke=LABEL_BG,
    )


def bar_label_fg_config() -> dict:
    """Parâmetros do mark_text Altair para rótulos de barra (texto)."""
    return dict(
        align="center",
        baseline="bottom",
        dy=BAR_LABEL_DY,
        color=LABEL_FG,
        fontSize=LABEL_FONT_SIZE,
    )


def rank_layout_config() -> dict:
    """Parâmetros de fig.update_layout para gráficos Plotly de ranking."""
    return dict(
        margin=dict(l=60, r=30, t=60, b=60),
        height=460,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
