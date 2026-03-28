"""Teste rápido do fluxo de dados do waterfall TC Veículos."""
import os, sys
sys.path.insert(0, r'C:\user\U235107\GitHub\TC')
os.chdir(r'C:\user\U235107\GitHub\TC')

# Simular Databricks: flag ativa
os.environ['SCI_USE_OPTIMIZED_PARQUETS'] = 'true'

import pandas as pd
from tc_principal.shared import load_principal_real, load_custo_fp_veiculo_real

df_main = load_principal_real(2026)
df_veic = load_custo_fp_veiculo_real(2026)

print("=== load_principal_real(2026) ===")
if df_main is not None:
    print(f"  shape: {df_main.shape}")
    has_t06 = "Type 06" in df_main.columns
    has_veic = "Veículo" in df_main.columns
    print(f"  has_Type06={has_t06}  has_Veiculo={has_veic}")
    print(f"  columns[:15]: {list(df_main.columns[:15])}")
else:
    print("  None")

print()
print("=== load_custo_fp_veiculo_real(2026) ===")
if df_veic is not None:
    print(f"  shape: {df_veic.shape}")
    has_t06 = "Type 06" in df_veic.columns
    has_veic = "Veículo" in df_veic.columns
    has_cfpv = "Custo FP Veiculo" in df_veic.columns
    print(f"  has_Type06={has_t06}  has_Veiculo={has_veic}  has_CustoFPVeiculo={has_cfpv}")
    print(f"  columns: {list(df_veic.columns)}")
else:
    print("  None")

# Simular _enrich_with_vehicle
def _enrich_with_vehicle(df_main, df_veic):
    if df_veic is not None and not df_veic.empty and 'Veículo' in df_veic.columns:
        if 'Custo FP Veiculo' in df_veic.columns:
            df_veic = df_veic.copy()
            df_veic['Custo FP'] = df_veic['Custo FP Veiculo']
        return df_veic
    return df_main

df_result = _enrich_with_vehicle(df_main, df_veic)
print()
print("=== After _enrich_with_vehicle ===")
if df_result is not None:
    print(f"  shape: {df_result.shape}")
    has_t06 = "Type 06" in df_result.columns
    has_veic = "Veículo" in df_result.columns
    print(f"  has_Type06={has_t06}  has_Veiculo={has_veic}")
else:
    print("  None")
