"""
Diagnóstico das abas do Reporting após limpeza
"""
import pandas as pd

print("=" * 80)
print("DIAGNÓSTICO: Reporting fluxo anexo.xlsx - Aba Sapiens")
print("=" * 80)

arquivo = 'dados/TC_Ext/2026/Reporting fluxo anexo.xlsx'

# Ler aba Sapiens com header=1
df = pd.read_excel(arquivo, sheet_name='Sapiens', header=1)

print(f"\n📊 Colunas encontradas ({len(df.columns)}):")
for i, col in enumerate(df.columns):
    print(f"   {i}: {col}")

print(f"\n📊 Primeiras 5 linhas:")
print(df.head())

print(f"\n📊 Info do DataFrame:")
print(df.info())
