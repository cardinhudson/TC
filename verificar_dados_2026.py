import pandas as pd
import os

# Verificar arquivo SAPIENS
if os.path.exists('dados/TC_Ext/2026/Dados SAPIENS.xlsx'):
    print("📁 Verificando Dados SAPIENS.xlsx...")
    df = pd.read_excel('dados/TC_Ext/2026/Dados SAPIENS.xlsx', sheet_name='Base conso')
    if 'Período' in df.columns:
        periodos = sorted(df['Período'].unique())
        print(f"Períodos encontrados: {periodos}")
        print(f"\nContagem por período:")
        print(df['Período'].value_counts().sort_index())
    else:
        print("Coluna 'Período' não encontrada")
        print(f"Colunas disponíveis: {df.columns.tolist()[:15]}")
else:
    print("❌ Arquivo Dados SAPIENS.xlsx não encontrado")

print("\n" + "="*50 + "\n")

# Verificar arquivo Reporting
if os.path.exists('dados/TC_Ext/2026/Reporting fluxo anexo.xlsx'):
    print("📁 Verificando Reporting fluxo anexo.xlsx...")
    df = pd.read_excel('dados/TC_Ext/2026/Reporting fluxo anexo.xlsx', sheet_name='Sapiens', header=1)
    if 'Período' in df.columns:
        periodos = sorted(df['Período'].unique())
        print(f"Períodos encontrados: {periodos}")
        print(f"\nContagem por período:")
        print(df['Período'].value_counts().sort_index())
    else:
        print("Coluna 'Período' não encontrada")
        print(f"Colunas disponíveis: {df.columns.tolist()[:15]}")
else:
    print("❌ Arquivo Reporting fluxo anexo.xlsx não encontrado")
