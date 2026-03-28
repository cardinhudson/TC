"""Test: Gera HTML com EXATAMENTE o mesmo codigo de _render_period_plotly para testar hover."""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Constantes (copiadas de home_chart_style) ──
ORANGE_FLEX = '#FF6B35'
DELTA_GREEN = '#00AA00'
DELTA_RED = '#FF0000'
DELTA_LABEL_COLOR = '#D9D9D9'
LABEL_BG = 'rgba(50,50,50,0.75)'
LABEL_FG = '#FAFAFA'
LABEL_FONT_SIZE = 11

# ── Dados simulados ──
periodos = ['Janeiro 2025', 'Fevereiro 2025', 'Março 2025', 'Abril 2025', 'Maio 2025']
np.random.seed(42)

chart_data = pd.DataFrame({
    'Período_Completo': periodos * 2,
    'Total': [120000, 180000, 150000, 200000, 160000,
              60000, 90000, 70000, 100000, 80000],
    'Oficina': ['MEC']*5 + ['ELE']*5,
})
budget_data = pd.DataFrame({
    'Período_Completo': periodos * 2,
    'Total': [110000, 190000, 140000, 195000, 170000,
              55000, 85000, 65000, 95000, 75000],
    'Oficina': ['MEC']*5 + ['ELE']*5,
})

# Tooltip rico de exemplo
hover_delta = {}
for p in periodos:
    hover_delta[p] = f'<b>{p}</b> — Delta<br>Δ Total: R$ +5.000<br>• MOD: R$ +3.000<br>• MOI: R$ +2.000'

x_col = 'Período_Completo'
coluna = 'Total'
ordem_periodos = periodos

# ═══════════════════════════════════════════
# REPLICAR EXATAMENTE _render_period_plotly
# ═══════════════════════════════════════════
tem_flex = budget_data is not None and len(budget_data) > 0
n_rows = 2 if tem_flex else 1
row_heights = [0.25, 0.75] if tem_flex else [1.0]
fig = make_subplots(
    rows=n_rows, cols=1, shared_xaxes=False,
    vertical_spacing=0.08, row_heights=row_heights,
)
bar_row = n_rows

# BARRAS
df_agg = chart_data.copy()
df_agg['_hover_key'] = df_agg[x_col].astype(str).str.strip()
df_agg[x_col] = pd.Categorical(df_agg[x_col], categories=ordem_periodos, ordered=True)
df_agg = df_agg.sort_values(x_col)
# Agregar (como faria o real)
df_agg = df_agg.groupby(x_col, as_index=False, observed=False)[coluna].sum()
vals = df_agg[coluna].fillna(0).values

v_min, v_max = float(vals.min()), float(vals.max())
if v_max == v_min: v_max = v_min + 1
bar_colors = []
for v in vals:
    t = (v - v_min) / (v_max - v_min)
    r = int(216 + t * (76 - 216))
    g = int(180 + t * (29 - 180))
    b = int(254 + t * (149 - 254))
    bar_colors.append(f'rgb({r},{g},{b})')

fig.add_trace(go.Bar(
    x=df_agg[x_col].astype(str), y=vals, name='Real',
    marker_color=bar_colors, textposition='none', showlegend=False,
    hovertemplate=f'%{{x}}<br>{coluna}: %{{y:,.2f}}<extra>Real</extra>',
), row=bar_row, col=1)

# FLEX
bud = budget_data.copy()
bud[x_col] = pd.Categorical(bud[x_col], categories=ordem_periodos, ordered=True)
bud = bud.sort_values(x_col)
bud_agg = bud.groupby(x_col, as_index=False, observed=False)[coluna].sum()
flex_vals = bud_agg[coluna].fillna(0).values

fig.add_trace(go.Scatter(
    x=bud_agg[x_col].astype(str), y=flex_vals, name='Flex Bud',
    mode='lines+markers+text',
    line=dict(color=ORANGE_FLEX, width=2, dash='dot'),
    marker=dict(color=ORANGE_FLEX, size=7),
    text=flex_vals, texttemplate='%{y:,.2f}', textposition='top center',
    cliponaxis=False, textfont=dict(size=11, color=ORANGE_FLEX),
    hovertemplate=f'%{{x}}<br>Flex Bud: %{{y:,.2f}}<extra>Flex Bud</extra>',
), row=bar_row, col=1)

# DELTA
delta_real = df_agg.copy()
delta_flex_agg = bud_agg.copy().rename(columns={coluna: '_Flex'})
delta_data = delta_real.merge(delta_flex_agg, on=x_col, how='left')
delta_data['_Flex'] = delta_data['_Flex'].fillna(0)
delta_data['Delta'] = delta_data[coluna].fillna(0) - delta_data['_Flex']
delta_data['_hover_key'] = delta_data[x_col].astype(str).str.strip()
delta_data[x_col] = pd.Categorical(delta_data[x_col], categories=ordem_periodos, ordered=True)
delta_data = delta_data.sort_values(x_col)

delta_colors = [DELTA_GREEN if d < 0 else DELTA_RED for d in delta_data['Delta']]

# COM tooltip rico
_delta_hover = [hover_delta.get(p, '') for p in delta_data['_hover_key']]
print(f'hover_delta keys: {list(hover_delta.keys())[:3]}...')
print(f'_hover_key values: {list(delta_data["_hover_key"])}')
print(f'_delta_hover populated: {[bool(h) for h in _delta_hover]}')
print(f'any(_delta_hover): {any(_delta_hover)}')

if not any(_delta_hover):
    _delta_hover = None

_delta_kw = (
    dict(hovertext=_delta_hover, hovertemplate='%{hovertext}<extra></extra>')
    if _delta_hover
    else dict(hovertemplate='%{x}<br>Delta: %{y:,.2f}<extra>Delta (Real - Flex Bud)</extra>')
)

fig.add_trace(go.Bar(
    x=delta_data[x_col].astype(str),
    y=delta_data['Delta'],
    name='Delta (Real - Flex Bud)',
    marker_color=delta_colors,
    width=0.5,
    text=delta_data['Delta'],
    texttemplate='%{y:,.2f}',
    textposition='outside',
    cliponaxis=False,
    textfont=dict(size=11, color=DELTA_LABEL_COLOR),
    showlegend=False,
    **_delta_kw,
), row=1, col=1)

fig.update_yaxes(title_text='Delta', row=1, col=1, showgrid=False, zeroline=True,
                 zerolinecolor='rgba(160,160,160,0.35)', zerolinewidth=0.5, tickfont=dict(size=11))
fig.update_xaxes(row=1, col=1, showline=False, showgrid=False,
                 linecolor='rgba(0,0,0,0)', linewidth=0, ticks='')

fig.update_yaxes(title_text='Custo Total (R$)', row=bar_row, col=1, showgrid=False, automargin=True)
fig.update_xaxes(title_text='Período', row=bar_row, col=1,
                 categoryorder='array', categoryarray=ordem_periodos,
                 automargin=True, title_standoff=20)
fig.update_xaxes(showticklabels=False, row=1, col=1,
                 categoryorder='array', categoryarray=ordem_periodos,
                 showline=False, showgrid=False, matches=None)
fig.update_xaxes(showline=False, row=bar_row, col=1)

fig.update_layout(
    height=700, barmode='group',
    legend=dict(orientation='h', yanchor='top', y=-0.24, xanchor='center', x=0.5, font=dict(size=10)),
    margin=dict(l=60, r=30, t=130, b=130),
    plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
)

# ═══════════════════════════════════════════
# INSPECAO
# ═══════════════════════════════════════════
print(f'\n=== FIGURE INSPECTION ===')
print(f'Traces: {len(fig.data)}')
for i, t in enumerate(fig.data):
    ht = getattr(t, 'hovertext', None)
    htm = getattr(t, 'hovertemplate', None)
    print(f'  T{i} [{t.type}] {t.name}: hovertext={type(ht).__name__}, hovertemplate={"YES" if htm else "NO"}')
    if htm:
        print(f'    template: {str(htm)[:120]}')
    if isinstance(ht, (list, tuple)) and len(ht) > 0:
        print(f'    hovertext[0]: {str(ht[0])[:100]}')

print(f'\nhovermode: {fig.layout.hovermode}')
print(f'hoverdistance: {fig.layout.hoverdistance}')
print(f'yaxis domain (delta): {fig.layout.yaxis.domain}')
print(f'yaxis2 domain (bars): {fig.layout.yaxis2.domain}')

# Verificar se barmode afeta cross-subplot
print(f'barmode: {fig.layout.barmode}')
print(f'xaxis (delta): {fig.layout.xaxis.matches}')
print(f'xaxis2 (bars): {fig.layout.xaxis2.matches}')

fig.write_html('_test_REAL_delta.html', auto_open=False)

# Tambem gerar versao com shared_xaxes para comparar
fig_shared = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.25, 0.75])
# Copiar traces
for t in fig.data:
    row_target = 1 if t.name == 'Delta (Real - Flex Bud)' else 2
    fig_shared.add_trace(t, row=row_target, col=1)
fig_shared.update_layout(height=700, barmode='group', margin=dict(l=60, r=30, t=130, b=130),
                          plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
fig_shared.update_xaxes(categoryorder='array', categoryarray=ordem_periodos)
fig_shared.write_html('_test_REAL_delta_SHARED.html', auto_open=False)

print('\n==> _test_REAL_delta.html      (shared_xaxes=False) - DEVE FUNCIONAR')
print('==> _test_REAL_delta_SHARED.html (shared_xaxes=True)  - PODE NAO FUNCIONAR')
print('Abra AMBOS no browser e compare hover no delta!')
print('DONE')
