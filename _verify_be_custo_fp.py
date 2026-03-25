"""Verificar Custo FP no TC Principal forecast_completo"""
import os, sys, pandas as pd
sys.path.insert(0, '.')
from tc_core.utils.portabilidade import get_data_root

DATA_ROOT = str(get_data_root())
fc = os.path.join(DATA_ROOT, "TC_Principal", "Forecast", "forecast_completo.parquet")
df = pd.read_parquet(fc)

print("Colunas monetárias:")
for c in ['Total', 'Custo FP', 'Custo FA', 'Despesa Primaria', 'FP sem Dedicada', 'D&A dedicado']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')
        non_zero = df[df[c] != 0]
        print(f"  {c}: {non_zero.shape[0]} linhas nao-zero, sum={df[c].sum():,.2f}")

# Verificar Custo FP por Tipo e Período
if 'Custo FP' in df.columns and 'Tipo' in df.columns:
    df['Custo FP'] = pd.to_numeric(df['Custo FP'], errors='coerce')
    resumo = df.groupby(['Tipo', 'Período'])['Custo FP'].sum().reset_index()
    print("\nResumo Custo FP por Tipo e Período:")
    for _, row in resumo.iterrows():
        print(f"  {row['Tipo']:12s} | {str(row['Período']):12s} | {row['Custo FP']:>15,.2f}")

# Sample BE rows
print("\nSample BE rows (primeiras 3):")
be_rows = df[df['Tipo'] == 'BE'].head(3)
for _, r in be_rows.iterrows():
    print(f"  Período={r.get('Período','?')}, Custo FP={r.get('Custo FP','?')}, Total={r.get('Total','?')}")

print("\nDONE")
