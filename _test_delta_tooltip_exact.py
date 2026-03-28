"""Teste: verificar se hoverdistance=-1 resolve tooltip no subplot delta pequeno."""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

periodos = ['Jan 2026', 'Feb 2026', 'Mar 2026']
real_vals = [1000.0, 1500.0, 1200.0]
flex_vals = [1100.0, 1300.0, 1250.0]
delta_vals = [r - f for r, f in zip(real_vals, flex_vals)]
# Delta: [-100, +200, -50] -> percentualmente pequeno

# Simular tooltip rico
hover_delta = {
    'Jan 2026': '<b>Jan 2026</b> — Delta<br>Δ Total: R$ -100,00<br>Real: R$ 1.000<br>Flex: R$ 1.100',
    'Feb 2026': '<b>Feb 2026</b> — Delta<br>Δ Total: R$ +200,00<br>Real: R$ 1.500<br>Flex: R$ 1.300',
    'Mar 2026': '<b>Mar 2026</b> — Delta<br>Δ Total: R$ -50,00<br>Real: R$ 1.200<br>Flex: R$ 1.250',
}

# Reproduzir exatamente o layout do _render_period_plotly
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.17, row_heights=[0.162, 0.838],
)

# Barras principais (row 2)
fig.add_trace(go.Bar(
    x=periodos, y=real_vals, name='Real',
    marker_color='#7C3AED',
    textposition='none',
    showlegend=False,
    hovertemplate='%{x}<br>Custo FP: %{y:,.2f}<extra>Real</extra>',
), row=2, col=1)

# Linha Flex Bud (row 2)
fig.add_trace(go.Scatter(
    x=periodos, y=flex_vals, name='Flex Bud',
    mode='lines+markers+text',
    line=dict(color='#FF6B35', width=2, dash='dot'),
    marker=dict(color='#FF6B35', size=7),
    text=flex_vals,
    texttemplate='%{y:,.2f}',
    textposition='top center',
    cliponaxis=False,
    textfont=dict(size=11, color='#FF6B35'),
    hovertemplate='%{x}<br>Flex Bud: %{y:,.2f}<extra>Flex Bud</extra>',
), row=2, col=1)

# Delta (row 1) — com hovertext rico
delta_colors = ['#00AA00' if d < 0 else '#FF0000' for d in delta_vals]
_delta_hover = [hover_delta[p] for p in periodos]
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

# Delta Y axis
fig.update_yaxes(
    title_text='Delta (Real - Flex Bud)', row=1, col=1,
    showgrid=False, zeroline=True,
    zerolinecolor='rgba(160,160,160,0.35)', zerolinewidth=0.5,
    tickfont=dict(size=11),
)
fig.update_xaxes(
    row=1, col=1,
    showline=False, showgrid=False,
    linecolor='rgba(0,0,0,0)', linewidth=0, ticks='',
)

# Layout (EXATAMENTE como no código)
fig.update_yaxes(title_text='Custo FP (R$ /veíc)', row=2, col=1,
                 showgrid=False, automargin=True)
fig.update_xaxes(title_text='Período', row=2, col=1,
                 categoryorder='array', categoryarray=periodos,
                 automargin=True, title_standoff=20)
fig.update_xaxes(showticklabels=False, row=1, col=1,
                 categoryorder='array', categoryarray=periodos)
fig.update_xaxes(showline=False, row=1, col=1)
fig.update_xaxes(showline=False, row=2, col=1)

fig.update_layout(
    height=600,
    hovermode='closest',
    barmode='group',
    legend=dict(
        orientation='h', yanchor='top', y=-0.24,
        xanchor='center', x=0.5, font=dict(size=10),
    ),
    margin=dict(l=60, r=30, t=30, b=80),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
)

fig.write_html("_test_delta_tooltip_exact.html", auto_open=False)
print("✅ _test_delta_tooltip_exact.html — SEM hoverdistance")

# COM hoverdistance=-1
fig.update_layout(hoverdistance=-1)
fig.write_html("_test_delta_tooltip_hoverdist.html", auto_open=False)
print("✅ _test_delta_tooltip_hoverdist.html — COM hoverdistance=-1")
