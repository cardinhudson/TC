"""Diagnostic 3: D&A dedicado analysis for CC21 biton Feb"""
import pandas as pd
import os, sys
sys.path.insert(0, '.')

ano = '2026'

# 1. Check D&A dedicado from Real vehicle parquet
real_vpath = f'dados/TC_Principal/{ano}/df_veiculos_custo_fp.parquet'
df_real = pd.read_parquet(real_vpath)
mask = df_real['Veículo'].str.contains('CC21', case=False, na=False) & df_real['Veículo'].str.contains('biton', case=False, na=False)
df_r = df_real[mask]
df_r_fev = df_r[df_r['Período'].str.lower().str.startswith('fev')]

print("=== REAL CC21 biton Fev: D&A dedicado ===")
if 'D&A dedicado' in df_r_fev.columns:
    dea_total = df_r_fev['D&A dedicado'].sum()
    print(f"Total D&A dedicado = {dea_total:,.2f}")
    # By oficina
    by_ofi = df_r_fev.groupby('Oficina')['D&A dedicado'].sum()
    for ofi, val in by_ofi.items():
        print(f"  {ofi}: {val:,.2f}")

if 'FP sem Dedicada' in df_r_fev.columns:
    fp_sem = df_r_fev['FP sem Dedicada'].sum()
    print(f"\nFP sem Dedicada sum = {fp_sem:,.2f}")
    
if 'Custo Rateado' in df_r_fev.columns:
    cr = df_r_fev['Custo Rateado'].sum()
    print(f"Custo Rateado sum = {cr:,.2f}")

cfpv = df_r_fev['Custo FP Veiculo'].sum()
print(f"Custo FP Veiculo sum = {cfpv:,.2f}")
print(f"\nVerificação: FP sem Ded * Pct + D&A = Custo FP Veiculo?")
if 'Custo Rateado' in df_r_fev.columns and 'D&A dedicado' in df_r_fev.columns:
    calc = df_r_fev['Custo Rateado'].sum() + df_r_fev['D&A dedicado'].sum()
    print(f"Custo Rateado + D&A = {calc:,.2f}")
    print(f"Diff = {cfpv - calc:,.6f}")

# 2. Check BE parquet
print("\n=== BE CC21 biton Fev: D&A dedicado ===")
be_vpath = 'dados/TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet'
df_be = pd.read_parquet(be_vpath)
mask_b = df_be['Veículo'].str.contains('CC21', case=False, na=False) & df_be['Veículo'].str.contains('biton', case=False, na=False)
df_b = df_be[mask_b]
df_b_fev = df_b[df_b['Período'].str.lower().str.startswith('fev')]
if 'D&A dedicado' in df_b_fev.columns:
    dea_be = df_b_fev['D&A dedicado'].sum()
    print(f"Total D&A dedicado = {dea_be:,.2f}")
    by_ofi = df_b_fev.groupby('Oficina')['D&A dedicado'].sum()
    for ofi, val in by_ofi.items():
        print(f"  {ofi}: {val:,.2f}")
if 'FP sem Dedicada' in df_b_fev.columns:
    fps_be = df_b_fev['FP sem Dedicada'].sum()
    print(f"\nFP sem Dedicada sum = {fps_be:,.2f}")
if 'Custo Rateado' in df_b_fev.columns:
    cr_be = df_b_fev['Custo Rateado'].sum()
    print(f"Custo Rateado sum = {cr_be:,.2f}")
cfpv_be = df_b_fev['Custo FP Veiculo'].sum()
print(f"Custo FP Veiculo sum = {cfpv_be:,.2f}")
if 'Custo Rateado' in df_b_fev.columns and 'D&A dedicado' in df_b_fev.columns:
    calc_be = df_b_fev['Custo Rateado'].sum() + df_b_fev['D&A dedicado'].sum()
    print(f"Custo Rateado + D&A = {calc_be:,.2f}")
    print(f"Diff = {cfpv_be - calc_be:,.6f}")

# 3. Row count comparison
print(f"\n=== ROW COUNTS ===")
print(f"Real CC21 biton Fev: {len(df_r_fev)} rows")
print(f"BE CC21 biton Fev: {len(df_b_fev)} rows")
print(f"Diff: {len(df_r_fev) - len(df_b_fev)}")

# 4. Check D&A dedicado parquet
dea_path = f'dados/TC_Principal/{ano}/df_dea_dedicado.parquet'
if os.path.exists(dea_path):
    print(f"\n=== D&A DEDICADO FILE ===")
    df_dea = pd.read_parquet(dea_path)
    print(f"Colunas: {df_dea.columns.tolist()}")
    if 'Veículo' in df_dea.columns:
        mask_d = df_dea['Veículo'].str.contains('CC21', case=False, na=False) & df_dea['Veículo'].str.contains('biton', case=False, na=False)
        df_d = df_dea[mask_d]
        pc = 'Período' if 'Período' in df_d.columns else 'Periodo'
        df_d_fev = df_d[df_d[pc].str.lower().str.startswith('fev')]
        print(f"CC21 biton Fev: {len(df_d_fev)} rows")
        if 'D&A dedicado' in df_d_fev.columns:
            print(f"D&A dedicado sum = {df_d_fev['D&A dedicado'].sum():,.2f}")
            if 'Account' in df_d_fev.columns:
                by_acc = df_d_fev.groupby('Account')['D&A dedicado'].sum()
                for acc, val in by_acc.items():
                    print(f"  Account {acc}: {val:,.2f}")
else:
    print(f"\n{dea_path} not found")

# 5. Target diff: R$ 18,308.75 (from 1565688.32 - 1547379.57)
# Does this match any D&A value?
print(f"\n=== DIFF ANALYSIS ===")
print(f"Real CPU = 3736.73, implied cost = 3736.73 * 419 = {3736.73 * 419:,.2f}")
print(f"BE CPU = 3693.03, implied cost = 3693.03 * 419 = {3693.03 * 419:,.2f}")
print(f"Implied cost diff = {(3736.73 - 3693.03) * 419:,.2f}")

# 6. Now simulate the OLD rateio (before our fix) to see if it produces 3693.03
print("\n=== SIMULAR RATEIO ANTIGO (sem D&A em colunas_dropar) ===")
# Load forecast_completo for the rateio base
fc_path = 'dados/TC_Principal/Forecast/forecast_completo.parquet'
df_fc = pd.read_parquet(fc_path)
# Filter Feb
df_fc_fev = df_fc[df_fc['Período'].str.lower().str.startswith('fev')]
print(f"Forecast completo Fev rows: {len(df_fc_fev)}")
if 'D&A dedicado' in df_fc_fev.columns:
    print(f"D&A dedicado in forecast_completo: {df_fc_fev['D&A dedicado'].sum():,.2f}")
if 'FP sem Dedicada' in df_fc_fev.columns:
    print(f"FP sem Dedicada in forecast_completo: {df_fc_fev['FP sem Dedicada'].sum():,.2f}")
if 'Custo FP' in df_fc_fev.columns:
    print(f"Custo FP in forecast_completo: {df_fc_fev['Custo FP'].sum():,.2f}")
