"""
Verificação: valores que a aba BE do Waterfall vai mostrar
vs dados do forecast_completo usado pela Home.
"""
import os, sys, pandas as pd

sys.path.insert(0, '.')
from tc_core.utils.portabilidade import get_data_root

DATA_ROOT = str(get_data_root())

# ---------- TC Ext ----------
print("=" * 60)
print("TC EXT - forecast_completo.parquet")
print("=" * 60)
fc_ext = os.path.join(DATA_ROOT, "TC_Ext", "Forecast", "forecast_completo.parquet")
if os.path.exists(fc_ext):
    df_ext = pd.read_parquet(fc_ext)
    print(f"Shape: {df_ext.shape}")
    print(f"Colunas: {list(df_ext.columns)}")
    if 'Tipo' in df_ext.columns:
        print(f"\nValores unicos Tipo: {df_ext['Tipo'].unique().tolist()}")
    if 'Período' in df_ext.columns:
        per_col = 'Período'
    else:
        per_col = None
        for c in df_ext.columns:
            if 'per' in str(c).lower() and 'odo' in str(c).lower():
                per_col = c
                break
    if per_col:
        print(f"Coluna período: {per_col}")
        print(f"Periodos: {sorted(df_ext[per_col].dropna().unique().tolist())}")
    if 'Ano' in df_ext.columns:
        print(f"Anos: {sorted(df_ext['Ano'].dropna().unique().tolist())}")
    
    # Resumo por Tipo e mês
    val_col = 'Total' if 'Total' in df_ext.columns else ('Custo FP' if 'Custo FP' in df_ext.columns else None)
    if val_col and per_col and 'Tipo' in df_ext.columns:
        df_ext[val_col] = pd.to_numeric(df_ext[val_col], errors='coerce')
        resumo = df_ext.groupby(['Tipo', per_col])[val_col].sum().reset_index()
        print(f"\nResumo por Tipo e {per_col} ({val_col}):")
        for _, row in resumo.iterrows():
            print(f"  {row['Tipo']:12s} | {str(row[per_col]):12s} | {row[val_col]:>15,.2f}")
else:
    print("ARQUIVO NÃO ENCONTRADO!")

# ---------- TC Principal ----------
print("\n" + "=" * 60)
print("TC PRINCIPAL - forecast_completo.parquet")
print("=" * 60)
fc_princ = os.path.join(DATA_ROOT, "TC_Principal", "Forecast", "forecast_completo.parquet")
if os.path.exists(fc_princ):
    df_princ = pd.read_parquet(fc_princ)
    print(f"Shape: {df_princ.shape}")
    print(f"Colunas: {list(df_princ.columns)}")
    if 'Tipo' in df_princ.columns:
        print(f"\nValores unicos Tipo: {df_princ['Tipo'].unique().tolist()}")
    per_col2 = 'Período' if 'Período' in df_princ.columns else None
    if not per_col2:
        for c in df_princ.columns:
            if 'per' in str(c).lower() and 'odo' in str(c).lower():
                per_col2 = c
                break
    if per_col2:
        print(f"Coluna período: {per_col2}")
        print(f"Periodos: {sorted(df_princ[per_col2].dropna().unique().tolist())}")
    if 'Ano' in df_princ.columns:
        print(f"Anos: {sorted(df_princ['Ano'].dropna().unique().tolist())}")
    
    val_col2 = 'Total' if 'Total' in df_princ.columns else ('Custo FP' if 'Custo FP' in df_princ.columns else None)
    if val_col2 and per_col2 and 'Tipo' in df_princ.columns:
        df_princ[val_col2] = pd.to_numeric(df_princ[val_col2], errors='coerce')
        resumo2 = df_princ.groupby(['Tipo', per_col2])[val_col2].sum().reset_index()
        print(f"\nResumo por Tipo e {per_col2} ({val_col2}):")
        for _, row in resumo2.iterrows():
            print(f"  {row['Tipo']:12s} | {str(row[per_col2]):12s} | {row[val_col2]:>15,.2f}")
else:
    print("ARQUIVO NÃO ENCONTRADO!")

# ---------- Comparação com Real data ----------
print("\n" + "=" * 60)
print("DADOS REAL (SAP) - TC Ext")
print("=" * 60)
from tc_exports import load_data
try:
    df_real = load_data("Todos")
    if df_real is not None and not df_real.empty:
        print(f"Shape: {df_real.shape}")
        if 'Período' in df_real.columns:
            print(f"Periodos Real: {sorted(df_real['Período'].dropna().unique().tolist())}")
            val_r = 'Total' if 'Total' in df_real.columns else ('Custo FP' if 'Custo FP' in df_real.columns else None)
            if val_r:
                df_real[val_r] = pd.to_numeric(df_real[val_r], errors='coerce')
                resumo_real = df_real.groupby('Período')[val_r].sum().reset_index()
                print(f"\nResumo Real por Período ({val_r}):")
                for _, row in resumo_real.iterrows():
                    print(f"  {str(row['Período']):12s} | {row[val_r]:>15,.2f}")
except Exception as e:
    print(f"Erro ao carregar dados Real: {e}")

print("\nDONE")
