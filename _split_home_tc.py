"""
Script para dividir home_tc.py em módulos.
Extrai tabs e helpers em arquivos separados dentro de _home_tabs/.
"""
import os
import re

SRC = os.path.join("tc_principal", "pages", "home_tc.py")
DST_DIR = os.path.join("tc_principal", "pages", "_home_tabs")

with open(SRC, "r", encoding="utf-8") as f:
    lines = f.readlines()

total = len(lines)
print(f"Total de linhas em home_tc.py: {total}")

# ── Identify key line numbers (0-indexed) ──
# Helper functions before render()
# _resumo_por_veiculo: starts at "def _resumo_por_veiculo"
# _carregar_rateios_manuais: starts at "def _carregar_rateios_manuais"
# _MAP_PER: starts at "_MAP_PER ="
# _forecast_mtime: starts at "def _forecast_mtime"
# _load_forecast: starts at first "@st.cache_data" before "def _load_forecast("
# _load_forecast_full: starts at "@st.cache_data" before "def _load_forecast_full("
# create_periodo_chart: starts at "def create_periodo_chart"
# _preparar_flex: starts at "def _preparar_flex"
# render(): starts at "def render():"

# Tab boundaries (with tabN: ... _render_*())
# We need to find where each tab's content starts and ends

def find_line(pattern, start=0):
    """Find first line matching pattern (0-indexed)."""
    for i in range(start, total):
        if re.search(pattern, lines[i]):
            return i
    return -1

def find_all_lines(pattern, start=0, end=None):
    """Find all lines matching pattern."""
    if end is None:
        end = total
    results = []
    for i in range(start, end):
        if re.search(pattern, lines[i]):
            results.append(i)
    return results

# Find key markers
i_resumo = find_line(r'^def _resumo_por_veiculo\(')
i_rateios = find_line(r'^def _carregar_rateios_manuais\(')
i_map_per = find_line(r'^_MAP_PER\s*=')
i_forecast_mtime = find_line(r'^def _forecast_mtime\(')
i_load_forecast_cache = find_line(r'@st\.cache_data', i_forecast_mtime)
i_load_forecast = find_line(r'^def _load_forecast\(', i_load_forecast_cache)
i_load_forecast_full_cache = find_line(r'@st\.cache_data', i_load_forecast + 1)
i_load_forecast_full = find_line(r'^def _load_forecast_full\(', i_load_forecast_full_cache)
i_create_chart = find_line(r'^def create_periodo_chart\(')
i_preparar_flex = find_line(r'^def _preparar_flex\(')
i_render = find_line(r'^def render\(\):')

print(f"_resumo_por_veiculo: L{i_resumo+1}")
print(f"_carregar_rateios_manuais: L{i_rateios+1}")
print(f"_MAP_PER: L{i_map_per+1}")
print(f"_forecast_mtime: L{i_forecast_mtime+1}")
print(f"_load_forecast: L{i_load_forecast+1} (cache at L{i_load_forecast_cache+1})")
print(f"_load_forecast_full: L{i_load_forecast_full+1} (cache at L{i_load_forecast_full_cache+1})")
print(f"create_periodo_chart: L{i_create_chart+1}")
print(f"_preparar_flex: L{i_preparar_flex+1}")
print(f"render(): L{i_render+1}")

# Find tab boundaries
# Pattern: "    with tabN:" at 4-space indent (inside render)
tab_starts = []
for i in range(i_render, total):
    m = re.match(r'^    with tab(\d):', lines[i])
    if m:
        tab_starts.append((int(m.group(1)), i))

print(f"\nTab starts: {[(t, l+1) for t, l in tab_starts]}")

# Find the @st.fragment and def lines for each tab
tab_info = []
for idx, (tab_num, tab_line) in enumerate(tab_starts):
    # Find @st.fragment after tab_line
    i_frag = find_line(r'@st\.fragment', tab_line)
    # Find def _render_* after fragment
    i_def = find_line(r'def _render_\w+\(\):', i_frag)
    # Find the matching call: _render_*() at exactly 8-space indent
    # The call is the last line of the with block
    func_name = re.search(r'def (_render_\w+)\(\)', lines[i_def]).group(1)
    # Find the call: "        func_name()"  right before the next "    with tab" or end of render
    if idx + 1 < len(tab_starts):
        search_end = tab_starts[idx + 1][1]
    else:
        search_end = total
    i_call = -1
    for j in range(search_end - 1, i_def, -1):
        if lines[j].strip() == f'{func_name}()':
            i_call = j
            break
    
    # The body is from (i_def + 1) to (i_call - 1) inclusive
    body_start = i_def + 1
    body_end = i_call - 1  # last line of body (before the call)
    
    tab_info.append({
        'num': tab_num,
        'with_line': tab_line,
        'frag_line': i_frag,
        'def_line': i_def,
        'call_line': i_call,
        'func_name': func_name,
        'body_start': body_start,
        'body_end': body_end,
        'body_lines': body_end - body_start + 1,
    })
    print(f"Tab {tab_num}: with=L{tab_line+1}, def=L{i_def+1}({func_name}), body=L{body_start+1}-L{body_end+1} ({body_end-body_start+1} lines), call=L{i_call+1}")

# ── Create output directory ──
os.makedirs(DST_DIR, exist_ok=True)

# ── Write __init__.py ──
init_content = '''"""Home tabs package — modular decomposition of home_tc.py."""
from types import SimpleNamespace

# Alias for readability — holds all shared state from render()
HomeContext = SimpleNamespace
'''
with open(os.path.join(DST_DIR, "__init__.py"), "w", encoding="utf-8") as f:
    f.write(init_content)
print(f"\n✅ Created __init__.py")

# ── Write chart_utils.py ──
# Extract create_periodo_chart (from i_create_chart to i_preparar_flex-1)
# and _preparar_flex (from i_preparar_flex to i_render-1)
# Find the end of _preparar_flex (last non-blank line before render)
i_prep_end = i_render - 1
while i_prep_end > i_preparar_flex and lines[i_prep_end].strip() == '':
    i_prep_end -= 1

chart_imports = '''"""Chart utility functions for home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tc_principal.shared import (
    ordenar_por_mes, calcular_cpu,
)

'''
chart_body = ''.join(lines[i_create_chart:i_prep_end + 1])
with open(os.path.join(DST_DIR, "chart_utils.py"), "w", encoding="utf-8") as f:
    f.write(chart_imports + chart_body)
print(f"✅ Created chart_utils.py ({i_prep_end - i_create_chart + 1} lines of code)")

# ── Write data_helpers.py ──
# Extract: _resumo_por_veiculo, _carregar_rateios_manuais, _MAP_PER, _forecast_mtime, _load_forecast, _load_forecast_full
# Find end of _load_forecast_full (last line before create_periodo_chart)
i_helpers_end = i_create_chart - 1
while i_helpers_end > i_load_forecast_full and lines[i_helpers_end].strip() == '':
    i_helpers_end -= 1

helpers_imports = '''"""Data helper functions for home_tc dashboard."""
import os
import json
import unicodedata
import streamlit as st
import pandas as pd

from tc_core.utils.portabilidade import get_data_root
from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    load_forecast_agg, load_forecast_completo,
    normalizar_periodo, _render_tabela_fmt,
)

_DATA_ROOT = str(get_data_root())

'''
helpers_body = ''.join(lines[i_resumo:i_helpers_end + 1])
with open(os.path.join(DST_DIR, "data_helpers.py"), "w", encoding="utf-8") as f:
    f.write(helpers_imports + helpers_body)
print(f"✅ Created data_helpers.py ({i_helpers_end - i_resumo + 1} lines of code)")

# ── Write tab modules ──
# Each tab module needs:
# 1. Proper imports
# 2. A render(ctx) function that unpacks the context and runs the tab body

# Map of tab info to module names and context variables needed
TAB_MODULES = {
    1: {
        'name': 'tab_tc_veiculos',
        'title': 'TC Veículos',
        'ctx_vars': [
            'ano', 'moeda', 'simbolo', 'taxas', 'tipo', 'fator', 'sufixo',
            'label_valor', 'cols_val', 'vol_total',
            'df', 'df_bud', 'df_real', 'df_principal', 'tem_real',
            'df_flex', 'df_flex_det',
            'df_vol_bud', 'df_vol_actual',
            'df_veic_bud_raw', 'df_veic_real_raw',
            '_raw_df_principal', '_raw_df_real', '_raw_df_be',
            '_raw_df_vol_bud', '_raw_df_vol_actual',
            '_get_veic_be_raw', '_get_be_full',
            'tem_ano_df', 'usar_rateado', 'filtros_sel',
        ],
        'extra_imports': '''from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    COLUNAS_BE_DETALHADO, COLUNAS_BE_DETALHADO_VEICULO,
    reordenar_colunas_be, download_excel_button,
    load_principal, load_principal_real,
    load_volume_bud, load_volume_actual,
    load_custo_fp_veiculo, load_custo_fp_veiculo_real,
    load_custo_fp_veiculo_forecast_fresh,
    load_percentual_rateio_veiculos_real, ratear_be_por_veiculo,
    load_dea_dedicado_real,
    normalizar_periodo, ordenar_por_mes,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    aplicar_fator_df, converter_moeda_df, obter_sufixo_fator, calcular_cpu,
    build_cpu_tooltip_payload, build_delta_tooltip_payload,
    extrair_redis,
    _pivotar_detalhado, _pivotar_flex, render_secao_tabela_detalhe,
    _render_tabela_fmt,
)
from tc_principal.ui_components import (
    aplicar_filtros, criar_tabela_html, render_kpi, render_kpi_spacer,
    formatar_ratio_com_barra, criar_tabela_html_flex, render_inline_summary_metrics,
)
from tc_principal.pages._home_tabs.chart_utils import create_periodo_chart
from tc_principal.pages._home_tabs.data_helpers import _carregar_rateios_manuais

try:
    from processamento_dados_veiculos import executar_conferencias
except ImportError:
    executar_conferencias = None
''',
    },
    2: {
        'name': 'tab_volume',
        'title': 'Volume',
        'ctx_vars': [
            'ano', 'simbolo', 'sufixo',
            'df_vol_bud', 'df_vol_actual',
        ],
        'extra_imports': '''from tc_principal.shared import (
    ORDEM_MESES, ordenar_por_mes,
)
from tc_principal.ui_components import render_kpi, render_kpi_spacer

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None
''',
    },
    3: {
        'name': 'tab_analise_flex',
        'title': 'Análise Flex',
        'ctx_vars': [
            'ano', 'moeda', 'simbolo', 'taxas', 'tipo', 'fator', 'sufixo',
            'label_valor',
            'df', 'df_bud', 'df_flex',
            'df_vol_bud', 'df_vol_actual',
            '_raw_df_be', '_get_be_full',
            'tem_ano_df',
        ],
        'extra_imports': '''from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    ordenar_por_mes,
    aplicar_fator_df, converter_moeda_df,
    calcular_flex_budget, calcular_cpu,
)
from tc_principal.ui_components import render_kpi, render_kpi_spacer

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None
''',
    },
    4: {
        'name': 'tab_tempo_producao',
        'title': 'Tempo de Produção',
        'ctx_vars': [
            'ano', 'tipo', 'label_valor', 'simbolo', 'sufixo',
            'df', 'cols_val',
            'df_vol_bud',
        ],
        'extra_imports': '''from tc_principal.shared import (
    ORDEM_MESES, CORES_VEICULOS, COLUNAS_MONETARIAS,
    normalizar_periodo, ordenar_por_mes,
    calcular_cpu,
    load_tempo_veiculos, load_tempo_veiculos_real,
    load_volume_fa, load_volume_fa_real,
    load_percentual_rateio_veiculos_real,
)
from tc_principal.ui_components import (
    criar_tabela_html, render_kpi, render_kpi_spacer,
)
from tc_principal.pages._home_tabs.data_helpers import _carregar_rateios_manuais

try:
    import altair as alt
    alt.data_transformers.disable_max_rows()
except Exception:
    alt = None
''',
    },
    5: {
        'name': 'tab_dados_detalhados',
        'title': 'Dados Detalhados',
        'ctx_vars': [
            'ano', 'moeda', 'simbolo', 'taxas', 'fator', 'sufixo',
            'tem_ano_df',
            'df', 'df_bud', 'df_real', 'df_flex', 'df_flex_det',
            'df_vol_bud', 'df_vol_actual',
            'df_veic_bud_raw', 'df_veic_real_raw',
            '_raw_df_be', '_raw_df_vol_bud', '_raw_df_vol_actual',
            '_get_be_full', '_get_veic_be_raw',
            'filtros_sel',
        ],
        'extra_imports': '''from tc_principal.shared import (
    ORDEM_MESES, COLUNAS_MONETARIAS,
    COLUNAS_BE_DETALHADO, COLUNAS_BE_DETALHADO_VEICULO,
    reordenar_colunas_be, download_excel_button,
    load_tc_sapiens, load_forecast_completo,
    load_dea_dedicado_real,
    load_percentual_rateio_veiculos_real, ratear_be_por_veiculo,
    normalizar_periodo,
    aplicar_fator_df, converter_moeda_df,
    calcular_flex_budget, calcular_flex_budget_detalhado,
    _pivotar_detalhado, _pivotar_flex, render_secao_tabela_detalhe,
)
from tc_principal.ui_components import aplicar_filtros
from tc_principal.pages._home_tabs.data_helpers import _resumo_por_veiculo, _DATA_ROOT
''',
    },
}

for tab_num, info in TAB_MODULES.items():
    tab = next(t for t in tab_info if t['num'] == tab_num)
    mod_name = info['name']
    
    # Build context unpacking code
    ctx_unpack = '\n'.join(f'    {v} = ctx.{v}' for v in info['ctx_vars'])
    
    # Extract tab body (remove one level of indentation: 12 spaces → 4 spaces for render body)
    # The tab body is indented at 12 spaces (3 levels: render > with tab > def _render_*)
    body_lines_raw = lines[tab['body_start']:tab['body_end'] + 1]
    
    # Dedent: remove exactly 8 spaces (keep 4 as the function body indent)
    dedented = []
    for line in body_lines_raw:
        if line.strip() == '':
            dedented.append('\n')
        elif line.startswith('            '):  # 12 spaces
            dedented.append('    ' + line[12:])  # Keep 4-space indent
        elif line.startswith('        '):  # 8 spaces (e.g. inner function defs)
            dedented.append(line[8:])  # Remove 8 spaces
        else:
            # Shouldn't happen but keep as-is
            dedented.append(line)
    
    body_code = ''.join(dedented)
    
    module_content = f'''"""Tab {tab_num}: {info['title']} — home_tc dashboard."""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import os

{info['extra_imports']}

def render(ctx):
    """Renderiza a aba {info['title']}."""
    # ── Desempacotar contexto ──
{ctx_unpack}

{body_code}'''
    
    filepath = os.path.join(DST_DIR, f"{mod_name}.py")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(module_content)
    print(f"✅ Created {mod_name}.py ({len(body_lines_raw)} lines of tab body)")

# ── Rewrite home_tc.py ──
# Keep: imports (L1-80), meses_pt (L72-78), render() up to tab creation
# Replace: helper functions with imports from modules, tab bodies with thin wrappers

# New home_tc.py structure:
# 1. Imports (original + new module imports)
# 2. meses_pt dict
# 3. render() function:
#    - Everything from L579 to tab creation (L957)
#    - Build context
#    - Thin tab wrappers
#    - Footer

# Find where meses_pt ends (line after the closing brace)
i_meses = find_line(r'^meses_pt\s*=')
i_meses_end = i_meses
brace_count = 0
for i in range(i_meses, total):
    brace_count += lines[i].count('{') - lines[i].count('}')
    if brace_count == 0:
        i_meses_end = i
        break

print(f"\nmeses_pt: L{i_meses+1}-L{i_meses_end+1}")

# The render() function body before tabs: from i_render to the first "with tab1:"
i_first_tab = tab_info[0]['with_line']

# Build new home_tc.py
new_lines = []

# Part 1: Original imports (L0 to i_resumo-1), but remove _DATA_ROOT if it exists
# Actually keep all imports up to but not including _resumo_por_veiculo
for i in range(0, i_resumo):
    new_lines.append(lines[i])

# Part 2: Add imports for new modules
new_lines.append('\n')
new_lines.append('# ── Módulos extraídos ──\n')
new_lines.append('from tc_principal.pages._home_tabs.chart_utils import create_periodo_chart, _preparar_flex  # noqa: F401\n')
new_lines.append('from tc_principal.pages._home_tabs.data_helpers import (\n')
new_lines.append('    _resumo_por_veiculo, _carregar_rateios_manuais,  # noqa: F401\n')
new_lines.append('    _forecast_mtime, _load_forecast, _load_forecast_full,\n')
new_lines.append('    _MAP_PER,\n')
new_lines.append(')\n')
new_lines.append('from tc_principal.pages._home_tabs import tab_tc_veiculos as _tab1\n')
new_lines.append('from tc_principal.pages._home_tabs import tab_volume as _tab2\n')
new_lines.append('from tc_principal.pages._home_tabs import tab_analise_flex as _tab3\n')
new_lines.append('from tc_principal.pages._home_tabs import tab_tempo_producao as _tab4\n')
new_lines.append('from tc_principal.pages._home_tabs import tab_dados_detalhados as _tab5\n')
new_lines.append('\n')

# Part 3: render() function - from i_render to before first tab
for i in range(i_render, i_first_tab):
    new_lines.append(lines[i])

# Part 4: Build context and thin tab wrappers
new_lines.append('\n')
new_lines.append('    # ══════════════════════════════════════════════════════════════\n')
new_lines.append('    #  CONTEXTO COMPARTILHADO PARA MÓDULOS DE ABAS\n')
new_lines.append('    # ══════════════════════════════════════════════════════════════\n')
new_lines.append('    from tc_principal.pages._home_tabs import HomeContext\n')
new_lines.append('    ctx = HomeContext(\n')
ctx_fields = [
    'ano=ano', 'moeda=moeda', 'simbolo=simbolo', 'taxas=taxas',
    'tipo=tipo', 'fator=fator', 'sufixo=sufixo',
    'label_valor=label_valor', 'cols_val=cols_val', 'vol_total=vol_total',
    'df=df', 'df_bud=df_bud', 'df_real=df_real', 'df_principal=df_principal',
    'tem_real=tem_real', 'tem_ano_df=tem_ano_df',
    'df_flex=df_flex', 'df_flex_det=df_flex_det',
    'df_vol_bud=df_vol_bud', 'df_vol_actual=df_vol_actual',
    'df_veic_bud_raw=df_veic_bud_raw', 'df_veic_real_raw=df_veic_real_raw',
    'df_tempo_veic=df_tempo_veic',
    '_raw_df_principal=_raw_df_principal', '_raw_df_real=_raw_df_real',
    '_raw_df_be=_raw_df_be',
    '_raw_df_vol_bud=_raw_df_vol_bud', '_raw_df_vol_actual=_raw_df_vol_actual',
    '_get_veic_be_raw=_get_veic_be_raw', '_get_be_full=_get_be_full',
    'usar_rateado=usar_rateado', 'filtros_sel=filtros_sel',
]
for field in ctx_fields:
    new_lines.append(f'        {field},\n')
new_lines.append('    )\n')
new_lines.append('\n')

# Tab wrappers
tab_wrappers = [
    (1, '_render_tc_veiculos', '_tab1'),
    (2, '_render_volume', '_tab2'),
    (3, '_render_analise_flex', '_tab3'),
    (4, '_render_tempo_producao', '_tab4'),
    (5, '_render_dados_detalhados', '_tab5'),
]
for tab_num, func_name, mod_alias in tab_wrappers:
    new_lines.append(f'    with tab{tab_num}:\n')
    new_lines.append(f'        @st.fragment\n')
    new_lines.append(f'        def {func_name}():\n')
    new_lines.append(f'            {mod_alias}.render(ctx)\n')
    new_lines.append(f'        {func_name}()\n')
    new_lines.append('\n')

# Part 5: Footer (after last tab call)
i_footer_start = tab_info[-1]['call_line'] + 1
for i in range(i_footer_start, total):
    new_lines.append(lines[i])

# Write new home_tc.py
new_content = ''.join(new_lines)
new_total_lines = new_content.count('\n') + (0 if new_content.endswith('\n') else 1)

# Backup original
backup_path = SRC + '.bak'
with open(backup_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)
print(f"\n📦 Backup criado: {backup_path}")

with open(SRC, 'w', encoding='utf-8') as f:
    f.write(new_content)
print(f"✅ home_tc.py reescrito: {total} → ~{new_total_lines} linhas")

print("\n🎉 Split concluído!")
print(f"   Arquivos criados em {DST_DIR}/:")
for fname in sorted(os.listdir(DST_DIR)):
    fpath = os.path.join(DST_DIR, fname)
    with open(fpath, 'r', encoding='utf-8') as f:
        lc = sum(1 for _ in f)
    print(f"   - {fname}: {lc} linhas")
