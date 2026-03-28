"""Test: replica EXATAMENTE _render_period_plotly do TC Ext (padrão TC Veículos)."""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

periodos = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio']
real_vals = [120000, 180000, 150000, 200000, 160000]
flex_vals = [110000, 190000, 140000, 195000, 170000]
delta_vals = [r - f for r, f in zip(real_vals, flex_vals)]

# ── make_subplots: EXATAMENTE como TC Veículos ──
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.17, row_heights=[0.162, 0.838],
)

# Barras principais (row=2)
fig.add_trace(go.Bar(
    x=periodos, y=real_vals, name='Real',
    marker_color=['#D8B4FE','#C4A0F0','#B08CE2','#9C78D4','#8864C6'],
    textposition='none', showlegend=False,
    hovertemplate='%{x}<br>Total: %{y:,.2f}<extra>Real</extra>',
), row=2, col=1)

# Linha Flex (row=2)
fig.add_trace(go.Scatter(
    x=periodos, y=flex_vals, name='Flex Bud',
    mode='lines+markers+text',
    line=dict(color='#FF6B35', width=2, dash='dot'),
    marker=dict(color='#FF6B35', size=7),
    text=flex_vals, texttemplate='%{y:,.2f}', textposition='top center',
    cliponaxis=False, textfont=dict(size=11, color='#FF6B35'),
    hovertemplate='%{x}<br>Flex Bud: %{y:,.2f}<extra>Flex Bud</extra>',
), row=2, col=1)

# ── Delta (row=1): EXATAMENTE como TC Veículos ──
delta_colors = ['#00AA00' if d < 0 else '#FF0000' for d in delta_vals]
fig.add_trace(go.Bar(
    x=periodos, y=delta_vals,
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
    hovertemplate='%{x}<br>Delta: %{y:,.2f}<extra>Delta (Real - Flex Bud)</extra>',
), row=1, col=1)

# ── Axes: EXATAMENTE como TC Veículos ──
fig.update_yaxes(title_text='Delta (Real - Flex Bud)', row=1, col=1,
                 showgrid=False, zeroline=True,
                 zerolinecolor='rgba(160,160,160,0.35)', zerolinewidth=0.5,
                 tickfont=dict(size=11))
fig.update_xaxes(row=1, col=1, showline=False, showgrid=False,
                 linecolor='rgba(0,0,0,0)', linewidth=0, ticks='')
fig.update_yaxes(title_text='Custo Total (R$)', row=2, col=1,
                 showgrid=False, automargin=True)
fig.update_xaxes(title_text='Período', row=2, col=1,
                 categoryorder='array', categoryarray=periodos,
                 automargin=True, title_standoff=20)
fig.update_xaxes(showticklabels=False, row=1, col=1,
                 categoryorder='array', categoryarray=periodos)
fig.update_xaxes(showline=False, row=1, col=1)
fig.update_xaxes(showline=False, row=2, col=1)

# ── Layout: EXATAMENTE como TC Veículos ──
fig.update_layout(
    height=620, barmode='group',
    legend=dict(orientation='h', yanchor='top', y=-0.24,
                xanchor='center', x=0.5, font=dict(size=10)),
    margin=dict(l=60, r=30, t=130, b=130),
    plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
)

fig.write_html("_test_delta_final.html", auto_open=False)

for i, t in enumerate(fig.data):
    htm = getattr(t, 'hovertemplate', None)
    print(f"Trace {i} [{t.type}] {t.name}: hover={'YES' if htm else 'NO'}")

print(f"\nHTML: _test_delta_final.html — Abra no browser e teste o hover nas barras do delta.")
