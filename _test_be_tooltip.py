"""Teste: verificar forecast agg tem Type 05/06 no load."""
import os, sys
sys.path.insert(0, r'C:\user\U235107\GitHub\TC')
os.chdir(r'C:\user\U235107\GitHub\TC')
os.environ['SCI_USE_OPTIMIZED_PARQUETS'] = 'true'

from tc_principal.shared import load_forecast_agg

df = load_forecast_agg()
if df is not None:
    t05 = 'Type 05' in df.columns
    t06 = 'Type 06' in df.columns
    print(f"forecast_agg: {df.shape}, Type05={t05}, Type06={t06}")
    print(f"columns: {list(df.columns)}")
    if t05:
        print(f"Type 05 unique count: {df['Type 05'].nunique()}")
    if t06:
        print(f"Type 06 unique count: {df['Type 06'].nunique()}")
else:
    print("forecast_agg returned None")
