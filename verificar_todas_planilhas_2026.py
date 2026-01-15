"""
Script para verificar TODAS as planilhas dos arquivos Excel de 2026
e identificar se há dados de outros meses
"""
import pandas as pd
import os

print("🔍 Verificando TODAS as planilhas dos arquivos Excel de 2026...\n")

# Verificar Dados SAPIENS
arquivo1 = 'dados/2026/Dados SAPIENS.xlsx'
if os.path.exists(arquivo1):
    print(f"📁 {arquivo1}")
    print("=" * 70)
    
    # Listar todas as planilhas
    xl = pd.ExcelFile(arquivo1)
    print(f"Planilhas disponíveis: {xl.sheet_names}\n")
    
    for sheet in xl.sheet_names:
        print(f"\n📄 Planilha: {sheet}")
        try:
            df = pd.read_excel(arquivo1, sheet_name=sheet, nrows=5)
            print(f"  Colunas: {df.columns.tolist()}")
            
            # Ler planilha completa para verificar períodos
            df_full = pd.read_excel(arquivo1, sheet_name=sheet)
            if 'Período' in df_full.columns:
                periodos = sorted(df_full['Período'].dropna().unique())
                print(f"  ✅ PERÍODOS ENCONTRADOS: {periodos}")
                print(f"  Total de registros: {len(df_full)}")
            else:
                print(f"  ℹ️ Sem coluna 'Período'")
        except Exception as e:
            print(f"  ⚠️ Erro ao ler: {e}")
else:
    print(f"❌ {arquivo1} não encontrado")

print("\n" + "=" * 70 + "\n")

# Verificar Reporting fluxo anexo
arquivo2 = 'dados/2026/Reporting fluxo anexo.xlsx'
if os.path.exists(arquivo2):
    print(f"📁 {arquivo2}")
    print("=" * 70)
    
    # Listar todas as planilhas
    xl = pd.ExcelFile(arquivo2)
    print(f"Planilhas disponíveis: {xl.sheet_names}\n")
    
    for sheet in xl.sheet_names:
        print(f"\n📄 Planilha: {sheet}")
        try:
            # Tentar com header=1 (padrão para Sapiens)
            if sheet == 'Sapiens':
                df_full = pd.read_excel(arquivo2, sheet_name=sheet, header=1)
            else:
                df_full = pd.read_excel(arquivo2, sheet_name=sheet)
            
            print(f"  Colunas: {df_full.columns.tolist()[:15]}")
            
            if 'Período' in df_full.columns:
                periodos = sorted(df_full['Período'].dropna().unique())
                print(f"  ✅ PERÍODOS ENCONTRADOS: {periodos}")
                print(f"  Total de registros: {len(df_full)}")
                
                # Contar registros por período
                print(f"\n  📊 Contagem por período:")
                contagem = df_full['Período'].value_counts().sort_index()
                for periodo, count in contagem.items():
                    print(f"    {periodo}: {count}")
            else:
                print(f"  ℹ️ Sem coluna 'Período'")
                
        except Exception as e:
            print(f"  ⚠️ Erro ao ler: {e}")
else:
    print(f"❌ {arquivo2} não encontrado")

print("\n" + "=" * 70)
print("\n📋 CONCLUSÃO:")
print("Se não houver dados de janeiro a junho e dezembro nas planilhas acima,")
print("significa que esses meses ainda não foram incluídos nos arquivos Excel de origem.")
