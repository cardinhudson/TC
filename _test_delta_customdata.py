"""Teste: customdata com list(zip) vs np.column_stack para Plotly."""
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

periodos = ['Jan 2026', 'Feb 2026', 'Mar 2026']
real_vals = [100.0, 150.0, 120.0]
flex_vals = [110.0, 130.0, 125.0]
delta_vals = [r - f for r, f in zip(real_vals, flex_vals)]

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                    vertical_spacing=0.17, row_heights=[0.162, 0.838])

# Delta com customdata fallback (list of tuples)
cd = list(zip(real_vals, flex_vals))
print(f"customdata (list of tuples): {cd}")

fig.add_trace(go.Bar(
    x=periodos, y=delta_vals,
    name='Delta',
    marker_color=['#00AA00', '#FF0000', '#00AA00'],
    width=0.315,
    text=delta_vals,
    texttemplate='%{y:,.2f}',
    textposition='inside',
    insidetextanchor='start',
    cliponaxis=False,
    textfont=dict(size=11, color='#D9D9D9'),
    showlegend=False,
    customdata=cd,
    hovertemplate=(
        '<b>%{x}</b> — Delta (Real - Flex Bud)<br>'
        'Δ Total: R$ %{y:+,.2f}<br>'
        'Real: R$ %{customdata[0]:,.2f}<br>'
        'Flex Bud: R$ %{customdata[1]:,.2f}'
        '<extra></extra>'
    ),
), row=1, col=1)

fig.add_trace(go.Bar(
    x=periodos, y=real_vals, name='Real',
    marker_color='#7C3AED',
), row=2, col=1)

fig.update_layout(height=600, hovermode='closest',
                  margin=dict(l=60, r=30, t=30, b=80))
fig.update_xaxes(showticklabels=False, row=1, col=1,
                 categoryorder='array', categoryarray=periodos)
fig.update_xaxes(row=2, col=1,
                 categoryorder='array', categoryarray=periodos)

# Inspect
for i, trace in enumerate(fig.data):
    print(f"\nTrace {i}: {trace.name}")
    print(f"  hovertemplate: {trace.hovertemplate}")
    if trace.customdata is not None:
        print(f"  customdata: {trace.customdata}")

fig.write_html("_test_delta_customdata.html", auto_open=False)
print("\n✅ _test_delta_customdata.html criado")
