"""Diagnostic 2: Volume and CPU"""
import pandas as pd
import os

ano = '2026'
vol_path = f'dados/TC_Principal/{ano}/df_vol_veiculos_actual.parquet'
print(f'Checking: {vol_path}')
if os.path.exists(vol_path):
    df_vol = pd.read_parquet(vol_path)
    print(f'Colunas: {df_vol.columns.tolist()}')
    print(f'Shape: {df_vol.shape}')
    vc = 'Veículo' if 'Veículo' in df_vol.columns else 'Veiculo'
    if vc in df_vol.columns:
        mask = df_vol[vc].str.contains('CC21', case=False, na=False) & df_vol[vc].str.contains('biton', case=False, na=False)
        df_v = df_vol[mask]
        pc = 'Período' if 'Período' in df_v.columns else 'Periodo'
        print(f'CC21 biton total rows: {len(df_v)}')
        for p in sorted(df_v[pc].unique()):
            sub = df_v[df_v[pc] == p]
            v = sub['Volume'].sum()
            print(f'  {p}: Volume = {v:,.2f}')
    else:
        print('No vehicle column')
else:
    print('NOT FOUND')
    base = f'dados/TC_Principal/{ano}'
    for f in sorted(os.listdir(base)):
        if 'vol' in f.lower():
            print(f'  Found: {f}')

# Now simulate exactly what the graph does
print('\n=== SIMULAR GRÁFICO HOME_TC ===')
print('Mode: CPU com veículo CC21 biton selecionado')

# Load BE vehicle data (exactly as home_tc does)
be_vpath = 'dados/TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet'
df_be = pd.read_parquet(be_vpath)

# Filter CC21 biton
vc = 'Veículo'
mask = df_be[vc].str.contains('CC21', case=False, na=False) & df_be[vc].str.contains('biton', case=False, na=False)
df_be_cc21 = df_be[mask].copy()

# Map Custo FP Veiculo -> Custo FP (as home_tc does)
if 'Custo FP Veiculo' in df_be_cc21.columns:
    df_be_cc21['Custo FP'] = df_be_cc21['Custo FP Veiculo']

# Group by Period (+ Tipo if present), sum Custo FP
pc = 'Período'
grp_cols = [pc]
if 'Tipo' in df_be_cc21.columns:
    grp_cols = [pc, 'Tipo']

df_periodo = df_be_cc21.groupby(grp_cols, as_index=False).agg({'Custo FP': 'sum'})
print(f'\nBE agrupado por período:')
for _, row in df_periodo.iterrows():
    tipo_str = f" ({row['Tipo']})" if 'Tipo' in df_periodo.columns else ''
    print(f"  {row[pc]}{tipo_str}: Custo FP = {row['Custo FP']:,.2f}")

# Load volume and compute CPU
if os.path.exists(vol_path):
    df_vol = pd.read_parquet(vol_path)
    vc_v = 'Veículo' if 'Veículo' in df_vol.columns else 'Veiculo' 
    if vc_v in df_vol.columns:
        df_vol_cc21 = df_vol[df_vol[vc_v].str.contains('CC21', case=False, na=False) & df_vol[vc_v].str.contains('biton', case=False, na=False)]
    else:
        df_vol_cc21 = df_vol.copy()
    
    pc_v = 'Período' if 'Período' in df_vol_cc21.columns else 'Periodo'
    vol_per = df_vol_cc21.groupby(pc_v, as_index=False)['Volume'].sum()
    vol_per[pc_v] = vol_per[pc_v].astype(str)
    df_periodo[pc] = df_periodo[pc].astype(str)
    
    # Merge
    df_merged = df_periodo.merge(vol_per, left_on=pc, right_on=pc_v, how='left')
    df_merged['Volume'] = df_merged['Volume'].fillna(0)
    
    import numpy as np
    df_merged['CPU'] = np.where(df_merged['Volume'] != 0, df_merged['Custo FP'] / df_merged['Volume'], 0.0)
    
    print(f'\nBE com CPU:')
    for _, row in df_merged.iterrows():
        tipo_str = f" ({row['Tipo']})" if 'Tipo' in df_merged.columns else ''
        print(f"  {row[pc]}{tipo_str}: Custo FP = {row['Custo FP']:,.2f}, Volume = {row.get('Volume', 'N/A')}, CPU = {row['CPU']:,.2f}")

# Compare with Real
print('\n=== REAL CPU (do parquet) ===')
cpu_path = f'dados/TC_Principal/{ano}/df_veiculos_cpu.parquet'
df_cpu = pd.read_parquet(cpu_path)
mask = df_cpu['Veículo'].str.contains('CC21', case=False, na=False) & df_cpu['Veículo'].str.contains('biton', case=False, na=False)
df_cpu_cc21 = df_cpu[mask]
for _, row in df_cpu_cc21.iterrows():
    print(f"  {row['Período']}: CPU = {row['CPU']:,.2f}, Volume = {row['Volume']:,.0f}, Custo FP Veiculo = {row['Custo FP Veiculo']:,.2f}")

# Now simulate Real mode (no BE) - same graph but with Real data
print('\n=== SIMULAR GRÁFICO REAL (sem BE) ===')
real_vpath = f'dados/TC_Principal/{ano}/df_veiculos_custo_fp.parquet'
df_real = pd.read_parquet(real_vpath)
mask_r = df_real[vc].str.contains('CC21', case=False, na=False) & df_real[vc].str.contains('biton', case=False, na=False)
df_real_cc21 = df_real[mask_r].copy()
if 'Custo FP Veiculo' in df_real_cc21.columns:
    df_real_cc21['Custo FP'] = df_real_cc21['Custo FP Veiculo']

df_real_per = df_real_cc21.groupby(pc, as_index=False).agg({'Custo FP': 'sum'})
if os.path.exists(vol_path):
    df_real_per[pc] = df_real_per[pc].astype(str)
    df_real_merged = df_real_per.merge(vol_per, left_on=pc, right_on=pc_v, how='left')
    df_real_merged['Volume'] = df_real_merged['Volume'].fillna(0)
    df_real_merged['CPU'] = np.where(df_real_merged['Volume'] != 0, df_real_merged['Custo FP'] / df_real_merged['Volume'], 0.0)
    
    print('Real com CPU:')
    for _, row in df_real_merged.iterrows():
        print(f"  {row[pc]}: Custo FP = {row['Custo FP']:,.2f}, Volume = {row.get('Volume', 'N/A')}, CPU = {row['CPU']:,.2f}")
