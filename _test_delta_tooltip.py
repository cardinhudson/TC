"""Teste isolado: verifica se o gráfico Plotly com subplot delta mostra tooltip."""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Dados simulados ──
periodos = ['Jan 2026', 'Feb 2026', 'Mar 2026']
real_vals = [100.0, 150.0, 120.0]
flex_vals = [110.0, 130.0, 125.0]
delta_vals = [r - f for r, f in zip(real_vals, flex_vals)]

# ── Tooltip payloads ──
hover_delta = {
    'Jan 2026': '<b>Jan 2026</b> — Delta<br>Δ Total: R$ -10,00<br>Real: R$ 100<br>Flex: R$ 110',
    'Feb 2026': '<b>Feb 2026</b> — Delta<br>Δ Total: R$ +20,00<br>Real: R$ 150<br>Flex: R$ 130',
    'Mar 2026': '<b>Mar 2026</b> — Delta<br>Δ Total: R$ -5,00<br>Real: R$ 120<br>Flex: R$ 125',
}

# ── Figura com subplots ──
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.17, row_heights=[0.162, 0.838],
)

# Barras principais
fig.add_trace(go.Bar(
    x=periodos, y=real_vals, name='Real',
    marker_color=['#D8B4FE', '#A78BFA', '#7C3AED'],
    hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>Real</extra>',
), row=2, col=1)

# Linha Flex Bud
fig.add_trace(go.Scatter(
    x=periodos, y=flex_vals, name='Flex Bud',
    mode='lines+markers', line=dict(color='#FF6B35', dash='dot'),
    hovertemplate='%{x}<br>Flex Bud: %{y:,.2f}<extra>Flex Bud</extra>',
), row=2, col=1)

# ── Delta trace (rico) ──
delta_colors = ['#00AA00' if d < 0 else '#FF0000' for d in delta_vals]
_delta_hover = [hover_delta.get(p, '') for p in periodos]
print(f"_delta_hover preenchido: {[bool(h) for h in _delta_hover]}")
print(f"any(_delta_hover): {any(_delta_hover)}")

fig.add_trace(go.Bar(
    x=periodos,
    y=delta_vals,
    name='Delta (Real - Flex Bud)',
    marker_color=delta_colors,
    width=0.315,
    text=delta_vals,
    texttemplate='%{y:,.2f}',
    textposition='inside',
    insidetextanchor='start',
    cliponaxis=False,
    textfont=dict(size=11, color='#D9D9D9'),
    showlegend=False,
    hovertext=_delta_hover,
    hovertemplate='%{hovertext}<extra></extra>',
), row=1, col=1)

# ── Layout ──
fig.update_yaxes(title_text='Delta', row=1, col=1, showgrid=False, zeroline=True)
fig.update_xaxes(row=1, col=1, showline=False, showgrid=False, ticks='')
fig.update_xaxes(showticklabels=False, row=1, col=1,
                 categoryorder='array', categoryarray=periodos)
fig.update_xaxes(title_text='Período', row=2, col=1,
                 categoryorder='array', categoryarray=periodos)

fig.update_layout(
    height=600,
    hovermode='closest',
    barmode='group',
    margin=dict(l=60, r=30, t=30, b=80),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
)

# ── Inspecionar traces ──
for i, trace in enumerate(fig.data):
    print(f"\nTrace {i}: {trace.name}")
    print(f"  type: {trace.type}")
    print(f"  hovertemplate: {trace.hovertemplate}")
    if hasattr(trace, 'hovertext') and trace.hovertext is not None:
        print(f"  hovertext: {[h[:40] + '...' if len(h) > 40 else h for h in trace.hovertext]}")
    if hasattr(trace, 'customdata') and trace.customdata is not None:
        print(f"  customdata: {trace.customdata}")
    if hasattr(trace, 'hoverinfo'):
        print(f"  hoverinfo: {trace.hoverinfo}")

# Salvar HTML para teste visual
fig.write_html("_test_delta_tooltip.html", auto_open=False)
print("\n✅ Arquivo _test_delta_tooltip.html criado. Abra no browser para testar o tooltip.")
