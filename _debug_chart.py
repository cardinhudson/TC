"""Debug script to check BE chart data flow."""
import pandas as pd
import numpy as np
import unicodedata

df = pd.read_parquet('dados/TC_Principal/Forecast/forecast_completo.parquet')

def _norm_tipo(v):
    if pd.isna(v):
        return 'BE'
    txt = str(v).replace('\ufffd', '').strip().lower()
    txt = unicodedata.normalize('NFKD', txt).encode('ascii', 'ignore').decode('ascii')
    if 'hist' in txt:
        return 'Historico'
    return 'BE'

df['Tipo'] = df['Tipo'].apply(_norm_tipo)
print("=== Tipo by Periodo ===")
ct = df.groupby(['Tipo'])['Custo FP'].agg(['count', 'sum'])
print(ct)
print()

# Simulate chart groupby
_grp = ['Ano', 'Periodo', 'Tipo'] if 'Ano' in df.columns else ['Periodo', 'Tipo']
# Fix periodo column name
per_col = [c for c in df.columns if 'per' in c.lower() and 'odo' in c.lower()]
print("Periodo columns:", per_col)

if per_col:
    pcol = per_col[0]
    _grp = ['Ano', pcol, 'Tipo']
    agg = df.groupby(_grp, as_index=False)['Custo FP'].sum()
    agg = agg.sort_values(pcol)
    print()
    print("=== Aggregated chart data ===")
    for _, row in agg.iterrows():
        print(f"  {row[pcol]:12s}  {row['Ano']}  Tipo={row['Tipo']:12s}  CustoFP={row['Custo FP']:>14,.2f}")
