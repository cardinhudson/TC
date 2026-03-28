"""Teste: verificar normalização de Ano no TC Ext waterfall."""
import os, sys
sys.path.insert(0, r'C:\user\U235107\GitHub\TC')
os.chdir(r'C:\user\U235107\GitHub\TC')
import pandas as pd

from tc_exports import carregar_dados_ext
df = carregar_dados_ext(2026)
if df is not None and not df.empty:
    col_ano = df["Ano"]
    print("TC Ext carregar_dados_ext(2026):")
    print("  Ano dtype:", col_ano.dtype)
    print("  Ano unique:", sorted(col_ano.unique()))
    pa = df["Período"].astype(str) + " " + df["Ano"].astype(str)
    print("  Periodo_Ano sample:", pa.unique()[:5].tolist())
else:
    print("Nao carregou")
