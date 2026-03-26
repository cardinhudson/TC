"""Diagnostic: Compare Real vs BE TC for CC21 biton February"""
import pandas as pd
import os

ano = '2026'
base = f'dados/TC_Principal/{ano}'
fbase = 'dados/TC_Principal/Forecast'

# 1. Volume actual
vol_path = f'{base}/df_volume_actual.parquet'
if os.path.exists(vol_path):
    df_vol = pd.read_parquet(vol_path)
    print("=== VOLUME ACTUAL ===")
    print("Colunas:", df_vol.columns.tolist())
    
    vc = 'Veículo' if 'Veículo' in df_vol.columns else 'Veiculo'
    if vc in df_vol.columns:
        mask = df_vol[vc].str.contains('CC21', case=False, na=False) & df_vol[vc].str.contains('biton', case=False, na=False)
        df_v = df_vol[mask]
        pc = 'Período' if 'Período' in df_v.columns else 'Periodo'
        df_v_fev = df_v[df_v[pc].str.lower().str.startswith('fev')]
        vol_sum = df_v_fev['Volume'].sum()
        print(f"CC21 biton Fev: {len(df_v_fev)} rows, Volume sum = {vol_sum:,.2f}")
        print(df_v_fev[['Volume']].to_string())
    else:
        print("No vehicle column. First 3 rows:")
        print(df_vol.head(3))
else:
    print(f"NOT FOUND: {vol_path}")

# 2. CPU parquet (Real)
cpu_path = f'{base}/df_veiculos_cpu.parquet'
if os.path.exists(cpu_path):
    print("\n=== CPU PARQUET (Real) ===")
    df_cpu = pd.read_parquet(cpu_path)
    print("Colunas:", df_cpu.columns.tolist())
    vc = 'Veículo' if 'Veículo' in df_cpu.columns else 'Veiculo'
    mask = df_cpu[vc].str.contains('CC21', case=False, na=False) & df_cpu[vc].str.contains('biton', case=False, na=False)
    df_c = df_cpu[mask]
    pc = 'Período' if 'Período' in df_c.columns else 'Periodo'
    df_c_fev = df_c[df_c[pc].str.lower().str.startswith('fev')]
    print(f"CC21 biton Fev: {len(df_c_fev)} rows")
    for col in df_c_fev.select_dtypes(include='number').columns:
        print(f"  {col} sum = {df_c_fev[col].sum():,.6f}")
else:
    print(f"\nNOT FOUND: {cpu_path}")

# 3. BE forecast completo
fc_path = f'{fbase}/forecast_completo.parquet'
if os.path.exists(fc_path):
    print("\n=== FORECAST COMPLETO ===")
    df_fc = pd.read_parquet(fc_path)
    print("Colunas:", df_fc.columns.tolist())
    # Check if it has vehicle
    if 'Veículo' in df_fc.columns:
        mask = df_fc['Veículo'].str.contains('CC21', case=False, na=False) & df_fc['Veículo'].str.contains('biton', case=False, na=False)
        df_fcc = df_fc[mask]
        pc = 'Período' if 'Período' in df_fcc.columns else 'Periodo'
        df_fcc_fev = df_fcc[df_fcc[pc].str.lower().str.startswith('fev')]
        print(f"CC21 biton Fev rows: {len(df_fcc_fev)}")
        if not df_fcc_fev.empty:
            for col in ['Custo FP', 'Custo FP Veiculo', 'Tipo']:
                if col in df_fcc_fev.columns:
                    if df_fcc_fev[col].dtype in ['float64', 'int64']:
                        print(f"  {col} sum = {df_fcc_fev[col].sum():,.6f}")
                    else:
                        print(f"  {col} unique = {df_fcc_fev[col].unique().tolist()[:5]}")
    else:
        print("No Veículo column in forecast_completo")
else:
    print(f"\nNOT FOUND: {fc_path}")

# 4. Compute CPU for CC21 biton Feb using the graph logic
print("\n=== SIMULAÇÃO CPU (lógica do gráfico) ===")
real_vpath = f'{base}/df_veiculos_custo_fp.parquet'
be_vpath = f'{fbase}/forecast_veiculos_custo_fp.parquet'

if os.path.exists(real_vpath) and os.path.exists(vol_path):
    df_real = pd.read_parquet(real_vpath)
    df_vol = pd.read_parquet(vol_path)
    
    vc = 'Veículo' if 'Veículo' in df_real.columns else 'Veiculo'
    pc = 'Período' if 'Período' in df_real.columns else 'Periodo'
    
    # Filter real for CC21 biton Feb
    mask_r = df_real[vc].str.contains('CC21', case=False, na=False) & df_real[vc].str.contains('biton', case=False, na=False)
    df_r = df_real[mask_r]
    df_r_fev = df_r[df_r[pc].str.lower().str.startswith('fev')]
    custo_real = df_r_fev['Custo FP Veiculo'].sum() if 'Custo FP Veiculo' in df_r_fev.columns else df_r_fev['Custo FP'].sum()
    
    # Volume for CC21 biton Feb
    vc_v = 'Veículo' if 'Veículo' in df_vol.columns else 'Veiculo'
    pc_v = 'Período' if 'Período' in df_vol.columns else 'Periodo'
    if vc_v in df_vol.columns:
        mask_v = df_vol[vc_v].str.contains('CC21', case=False, na=False) & df_vol[vc_v].str.contains('biton', case=False, na=False)
        df_v = df_vol[mask_v]
        df_v_fev = df_v[df_v[pc_v].str.lower().str.startswith('fev')]
        vol_fev = df_v_fev['Volume'].sum()
    else:
        # Volume without vehicle - total for period
        df_v_fev = df_vol[df_vol[pc_v].str.lower().str.startswith('fev')]
        vol_fev = df_v_fev['Volume'].sum()
        print("NOTE: Volume has NO vehicle column - using total volume!")
    
    print(f"REAL: Custo FP Veiculo CC21 biton Fev = {custo_real:,.2f}")
    print(f"Volume CC21 biton Fev = {vol_fev:,.2f}")
    if vol_fev > 0:
        cpu_real = custo_real / vol_fev
        print(f"CPU (Real) = {cpu_real:,.2f}")
    
    # BE
    if os.path.exists(be_vpath):
        df_be = pd.read_parquet(be_vpath)
        mask_b = df_be[vc].str.contains('CC21', case=False, na=False) & df_be[vc].str.contains('biton', case=False, na=False)
        df_b = df_be[mask_b]
        df_b_fev = df_b[df_b[pc].str.lower().str.startswith('fev')]
        custo_be = df_b_fev['Custo FP Veiculo'].sum() if 'Custo FP Veiculo' in df_b_fev.columns else df_b_fev['Custo FP'].sum()
        print(f"\nBE: Custo FP Veiculo CC21 biton Fev = {custo_be:,.2f}")
        if vol_fev > 0:
            cpu_be = custo_be / vol_fev
            print(f"CPU (BE) = {cpu_be:,.2f}")
        print(f"\nDiff Custo = {custo_real - custo_be:,.2f}")
        if vol_fev > 0:
            print(f"Diff CPU = {cpu_real - cpu_be:,.2f}")

# 5. Check if maybe the values are in K
print("\n=== VALORES EM DIFERENTES FATORES ===")
print(f"Custo FP Veiculo = R$ 1,565,688.32")
print(f"Custo FP Veiculo em K = R$ {1565688.32/1000:,.2f}")
print(f"Custo FP Total = R$ 13,344,190.43")
print(f"Custo FP Total em K = R$ {13344190.43/1000:,.2f}")

# 6. Check ALL parquets in Forecast folder
print("\n=== FORECAST FILES ===")
for f in sorted(os.listdir(fbase)):
    if f.endswith('.parquet'):
        fpath = os.path.join(fbase, f)
        sz = os.path.getsize(fpath)
        print(f"  {f} ({sz:,} bytes)")

# 7. Check if there's a forecast volume file
for f in os.listdir(fbase):
    if 'vol' in f.lower():
        print(f"\nFORECAST VOLUME FILE: {f}")
        df_fv = pd.read_parquet(os.path.join(fbase, f))
        print(f"  Colunas: {df_fv.columns.tolist()}")
        if 'Veículo' in df_fv.columns or 'Veiculo' in df_fv.columns:
            vc = 'Veículo' if 'Veículo' in df_fv.columns else 'Veiculo'
            mask = df_fv[vc].str.contains('CC21', case=False, na=False) & df_fv[vc].str.contains('biton', case=False, na=False)
            d = df_fv[mask]
            pc = 'Período' if 'Período' in d.columns else 'Periodo'
            d_fev = d[d[pc].str.lower().str.startswith('fev')]
            print(f"  CC21 biton Fev Vol = {d_fev['Volume'].sum():,.2f}")
