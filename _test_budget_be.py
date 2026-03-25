"""
Teste: Verificar que a aba Budget vai comparar Budget contra Real+BE.
Simula o que o Waterfall fará ao carregar _df_forecast_be para a aba Budget.
"""
import os, sys, pandas as pd, unicodedata
sys.path.insert(0, '.')
from tc_core.utils.portabilidade import get_data_root

DATA_ROOT = str(get_data_root())
_map_per = {
    'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
    'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
    'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
    'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro',
}

def norm_tipo(v):
    if pd.isna(v):
        return 'BE'
    txt = str(v).replace('\ufffd', '').strip().lower()
    txt = unicodedata.normalize('NFKD', txt).encode('ascii', 'ignore').decode('ascii')
    return 'Histórico' if 'hist' in txt else 'BE'

# ============ TC EXT ============
print("=" * 70)
print("TC EXT - Aba Budget: dados Real+BE que serão usados")
print("=" * 70)
fc_path = os.path.join(DATA_ROOT, "TC_Ext", "Forecast", "forecast_completo.parquet")
df = pd.read_parquet(fc_path)

if 'Período' in df.columns:
    df['Período'] = df['Período'].astype(str).str.strip().str.lower().map(_map_per).fillna(df['Período'])
for c in ['Custo FP', 'Total']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')
if 'Custo FP' in df.columns:
    df['Custo FP'] = df['Custo FP'].fillna(0.0)
    if 'Total' in df.columns:
        mask = df['Total'].isna() | (df['Total'] == 0)
        df.loc[mask, 'Total'] = df.loc[mask, 'Custo FP']
    else:
        df['Total'] = df['Custo FP']
if 'Tipo' in df.columns:
    df['Tipo'] = df['Tipo'].apply(norm_tipo)
    df['Fonte'] = df['Tipo'].apply(lambda x: 'Real' if x == 'Histórico' else 'BE')

# Filtrar 2026
if 'Ano' in df.columns:
    df = df[df['Ano'] == 2026].copy()

print("Dados Real+BE por mês (comparação com Budget):")
val_col = 'Total'
resumo = df.groupby(['Fonte', 'Período'])[val_col].sum().reset_index()
# Mostrar em ordem cronológica
meses_ord = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
for m in meses_ord:
    rows = resumo[resumo['Período'] == m]
    for _, r in rows.iterrows():
        print(f"  {r['Fonte']:6s} | {r['Período']:12s} | {r[val_col]:>15,.2f}")
print(f"  {'':6s} | {'TOTAL':12s} | {df[val_col].sum():>15,.2f}")

# Carregar Budget para comparar
from tc_exports import load_budget_data
try:
    import streamlit
except:
    pass
df_bud = load_budget_data("2026")
if df_bud is not None and not df_bud.empty:
    print(f"\nBudget: {df_bud.shape[0]} linhas")
    bud_val = 'Total' if 'Total' in df_bud.columns else ('Custo FP' if 'Custo FP' in df_bud.columns else None)
    if bud_val and 'Período' in df_bud.columns:
        df_bud[bud_val] = pd.to_numeric(df_bud[bud_val], errors='coerce')
        bud_resumo = df_bud.groupby('Período')[bud_val].sum().reset_index()
        print(f"\nBudget por mês ({bud_val}):")
        for m in meses_ord:
            rows = bud_resumo[bud_resumo['Período'] == m]
            for _, r in rows.iterrows():
                print(f"  BUD    | {r['Período']:12s} | {r[bud_val]:>15,.2f}")
        print(f"  {'BUD':6s} | {'TOTAL':12s} | {df_bud[bud_val].sum():>15,.2f}")
else:
    print("Budget: não disponível")

# ============ TC PRINCIPAL ============
print("\n" + "=" * 70)
print("TC PRINCIPAL - Aba Budget: dados Real+BE que serão usados")
print("=" * 70)
fc_path2 = os.path.join(DATA_ROOT, "TC_Principal", "Forecast", "forecast_completo.parquet")
df2 = pd.read_parquet(fc_path2)

if 'Período' in df2.columns:
    df2['Período'] = df2['Período'].astype(str).str.strip().str.lower().map(_map_per).fillna(df2['Período'])
for c in ['Custo FP', 'Total']:
    if c in df2.columns:
        df2[c] = pd.to_numeric(df2[c], errors='coerce')
if 'Custo FP' in df2.columns:
    df2['Custo FP'] = df2['Custo FP'].fillna(0.0)
    if 'Total' in df2.columns:
        mask2 = df2['Total'].isna() | (df2['Total'] == 0)
        df2.loc[mask2, 'Total'] = df2.loc[mask2, 'Custo FP']
    else:
        df2['Total'] = df2['Custo FP']
if 'Tipo' in df2.columns:
    df2['Tipo'] = df2['Tipo'].apply(norm_tipo)
    df2['Fonte'] = df2['Tipo'].apply(lambda x: 'Real' if x == 'Histórico' else 'BE')

if 'Ano' in df2.columns:
    df2 = df2[df2['Ano'] == 2026].copy()

val_col2 = 'Custo FP'
print("Dados Real+BE por mês (comparação com Budget):")
resumo2 = df2.groupby(['Fonte', 'Período'])[val_col2].sum().reset_index()
for m in meses_ord:
    rows = resumo2[resumo2['Período'] == m]
    for _, r in rows.iterrows():
        print(f"  {r['Fonte']:6s} | {r['Período']:12s} | {r[val_col2]:>15,.2f}")
print(f"  {'':6s} | {'TOTAL':12s} | {df2[val_col2].sum():>15,.2f}")

print("\nDONE")
