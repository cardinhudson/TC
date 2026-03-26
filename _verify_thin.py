"""Quick verification: THIN preserves filter columns, numbers match."""
import pandas as pd

# 1. Check REAL columns
full_r = pd.read_parquet('dados/TC_Principal/2026/df_principal.parquet')
thin_r = pd.read_parquet('dados/TC_Principal/2026/df_principal_thin.parquet')

print("=== Real: Full vs Thin columns ===")
print(f"Full: {len(full_r.columns)} cols | Thin: {len(thin_r.columns)} cols")
print(f"Full cols: {list(full_r.columns)}")
print(f"Thin cols: {list(thin_r.columns)}")

filtros = ['Centrocst', 'Nºconta', 'Tipo', 'Fornecedor', 'Fornec.', 'Usuário']
dropped = ['Texto breve', 'Material', 'Doc.compra', 'Nºdoc.ref.', 'Dt.lçto.', 'QTD']

print("\nFilter columns (should be PRESENT in THIN):")
for c in filtros:
    in_full = c in full_r.columns
    in_thin = c in thin_r.columns
    if in_full and in_thin:
        print(f"  ✅ {c}: preserved")
    elif in_full and not in_thin:
        print(f"  ❌ {c}: REMOVED from THIN!")
    else:
        print(f"  ➖ {c}: not in full either")

print("\nDrop columns (should be ABSENT from THIN):")
for c in dropped:
    in_full = c in full_r.columns
    in_thin = c in thin_r.columns
    if in_full and not in_thin:
        print(f"  ✅ {c}: correctly removed")
    elif in_full and in_thin:
        print(f"  ❌ {c}: still in THIN!")
    else:
        print(f"  ➖ {c}: not in full either")

# 2. Numerical comparison
print("\n=== Numerical comparison (Real) ===")
for col in ['Despesa Primaria', 'Custo FA', 'Custo FP', 'D&A dedicado', 'FP sem Dedicada']:
    if col in full_r.columns and col in thin_r.columns:
        f_sum = full_r[col].sum()
        t_sum = thin_r[col].sum()
        diff = abs(f_sum - t_sum)
        ok = "✅" if diff < 0.01 else "❌"
        print(f"  {ok} {col}: Full={f_sum:,.2f} | Thin={t_sum:,.2f} | Diff={diff:.4f}")

print(f"\nRow count: Full={len(full_r):,} | Thin={len(thin_r):,} | Match={len(full_r)==len(thin_r)}")

# 3. DataRouter test with flag ON
print("\n=== DataRouter integration test ===")
import os
os.environ['SCI_USE_OPTIMIZED_PARQUETS'] = 'true'
from tc_core.data_router import read_optimized
df_via_router = read_optimized('TC_Principal', '2026', '', 'df_principal', prefer='thin')
if df_via_router is not None:
    print(f"  ✅ Router returned THIN: {len(df_via_router)} rows, {len(df_via_router.columns)} cols")
    custo_fp_router = df_via_router['Custo FP'].sum() if 'Custo FP' in df_via_router.columns else 0
    custo_fp_full = full_r['Custo FP'].sum()
    diff = abs(custo_fp_router - custo_fp_full)
    ok = "✅" if diff < 0.01 else "❌"
    print(f"  {ok} Custo FP via Router={custo_fp_router:,.2f} vs Full={custo_fp_full:,.2f} (diff={diff:.4f})")
else:
    print("  ❌ Router returned None!")

# Test fallback with flag OFF
os.environ['SCI_USE_OPTIMIZED_PARQUETS'] = 'false'
df_via_router_off = read_optimized('TC_Principal', '2026', '', 'df_principal', prefer='thin')
if df_via_router_off is not None:
    print(f"  ✅ Router (flag OFF) returned FULL: {len(df_via_router_off)} rows, {len(df_via_router_off.columns)} cols")
else:
    print("  ❌ Router (flag OFF) returned None!")

print("\n=== DONE ===")
