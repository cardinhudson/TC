"""
Simulação do que a aba BE do Waterfall vai processar.
Compara com os valores esperados para validar que estão corretos.
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
print("TC EXT - Simulação aba BE Waterfall")
print("=" * 70)
fc_path = os.path.join(DATA_ROOT, "TC_Ext", "Forecast", "forecast_completo.parquet")
df = pd.read_parquet(fc_path)

# Normalizar Período
if 'Período' in df.columns:
    df['Período'] = df['Período'].astype(str).str.strip().str.lower().map(_map_per).fillna(df['Período'])

# Converter numéricos
for c in ['Custo FP', 'Total']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

# Garantir Total (fallback Custo FP)
if 'Custo FP' in df.columns:
    df['Custo FP'] = df['Custo FP'].fillna(0.0)
    if 'Total' in df.columns:
        mask = df['Total'].isna() | (df['Total'] == 0)
        df.loc[mask, 'Total'] = df.loc[mask, 'Custo FP']
    else:
        df['Total'] = df['Custo FP']

# Normalizar Tipo
if 'Tipo' in df.columns:
    df['Tipo'] = df['Tipo'].apply(norm_tipo)
    df['Fonte'] = df['Tipo'].apply(lambda x: 'Real' if x == 'Histórico' else 'BE')

# Filtrar ano 2025
if 'Ano' in df.columns:
    df_2025 = df[df['Ano'] == 2025].copy()
    df_2026 = df[df['Ano'] == 2026].copy()
else:
    df_2025 = df.copy()
    df_2026 = pd.DataFrame()

for label, d in [("Ano 2025", df_2025), ("Ano 2026", df_2026)]:
    if d.empty:
        print(f"\n{label}: sem dados")
        continue
    print(f"\n{label} - Dados que serão mostrados no Waterfall BE:")
    resumo = d.groupby(['Fonte', 'Período'])['Total'].sum().reset_index()
    resumo = resumo.sort_values('Período')
    for _, r in resumo.iterrows():
        print(f"  {r['Fonte']:6s} | {r['Período']:12s} | {r['Total']:>15,.2f}")
    print(f"  {'':6s} | {'TOTAL':12s} | {d['Total'].sum():>15,.2f}")

# ============ TC PRINCIPAL ============
print("\n" + "=" * 70)
print("TC PRINCIPAL - Simulação aba BE Waterfall")
print("=" * 70)
fc_path2 = os.path.join(DATA_ROOT, "TC_Principal", "Forecast", "forecast_completo.parquet")
df2 = pd.read_parquet(fc_path2)

# Normalizar Período
if 'Período' in df2.columns:
    df2['Período'] = df2['Período'].astype(str).str.strip().str.lower().map(_map_per).fillna(df2['Período'])

# Converter numéricos
for c in ['Custo FP', 'Total']:
    if c in df2.columns:
        df2[c] = pd.to_numeric(df2[c], errors='coerce')

# Garantir Total (fallback Custo FP)
if 'Custo FP' in df2.columns:
    df2['Custo FP'] = df2['Custo FP'].fillna(0.0)
    if 'Total' in df2.columns:
        mask2 = df2['Total'].isna() | (df2['Total'] == 0)
        df2.loc[mask2, 'Total'] = df2.loc[mask2, 'Custo FP']
    else:
        df2['Total'] = df2['Custo FP']

# Normalizar Tipo
if 'Tipo' in df2.columns:
    df2['Tipo'] = df2['Tipo'].apply(norm_tipo)
    df2['Fonte'] = df2['Tipo'].apply(lambda x: 'Real' if x == 'Histórico' else 'BE')

# Ano 2026
if 'Ano' in df2.columns:
    df2_2026 = df2[df2['Ano'] == 2026].copy()
else:
    df2_2026 = df2.copy()

print(f"\nAno 2026 - Dados que serão mostrados no Waterfall BE:")
if not df2_2026.empty:
    # Usar Custo FP como valor principal (é o que o waterfall TC usa)
    val_col = 'Custo FP'
    resumo2 = df2_2026.groupby(['Fonte', 'Período'])[val_col].sum().reset_index()
    resumo2 = resumo2.sort_values('Período')
    for _, r in resumo2.iterrows():
        print(f"  {r['Fonte']:6s} | {r['Período']:12s} | {r[val_col]:>15,.2f}")
    print(f"  {'':6s} | {'TOTAL':12s} | {df2_2026[val_col].sum():>15,.2f}")

print("\nDONE")
