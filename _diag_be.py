import pandas as pd

pr = pd.read_parquet("dados/TC_Principal/2026/df_principal.parquet")
meses = ["Janeiro", "Fevereiro", "Março", "Abril"]
for m in meses:
    mask = pr["Período"] == m
    cnt = mask.sum()
    t = pr.loc[mask, "Tipo"].value_counts(dropna=False).head(3).to_dict()
    print(f"{m}: {cnt} rows, Tipo: {t}")

print()
vol = pd.read_parquet("dados/TC_Principal/2026/df_vol_veiculos.parquet")
cpu = pd.read_parquet("dados/TC_Principal/2026/df_veiculos_cpu.parquet")
for m in meses:
    v = vol[vol["Período"] == m]
    c = cpu[cpu["Período"] == m]
    print(f"vol {m}: {len(v)} | cpu {m}: {len(c)}")

print()
fv = pd.read_parquet("dados/TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet")
abril_fv = fv[fv["Período"] == "Abril"]
print(f"forecast_veiculos Abril: {len(abril_fv)} rows, veiculos: {abril_fv['Veículo'].nunique()}")
print(f"Has Volume: {'Volume' in fv.columns}")
print(f"Has CPU: {'CPU' in fv.columns}")
extra = [c for c in fv.columns if c not in pr.columns]
print(f"Extra cols vs df_principal: {extra}")

print()
# Check if we can derive volume from forecast
# Budget volume for April
bv = pd.read_parquet("dados/TC_Principal/2026/BUD/df_vol_veiculos_BUD.parquet")
abril_bv = bv[bv["Período"] == "Abril"]
print(f"Budget vol Abril: {len(abril_bv)} rows, total: {abril_bv['Volume'].sum():.0f}")

# Real vol April
abril_vol = vol[vol["Período"] == "Abril"]
print(f"Real vol Abril: {len(abril_vol)} rows, total: {abril_vol['Volume'].sum():.0f}")

# CPU Real April
abril_cpu = cpu[cpu["Período"] == "Abril"]
print(f"Real cpu Abril: {len(abril_cpu)} rows")
if not abril_cpu.empty:
    print(f"  CPU values: {abril_cpu[['Veículo','CPU']].head().to_dict()}")
