"""Verificar se há colunas duplicadas nos arquivos Excel de origem"""
import pandas as pd
import os

# Verificar arquivo 2026
pasta_2026 = os.path.join('dados', '2026')
arquivos_excel = []

# Procurar todos os arquivos Excel em 2026
for arquivo in os.listdir(pasta_2026):
    if arquivo.endswith('.xlsx') or arquivo.endswith('.xls'):
        arquivos_excel.append(os.path.join(pasta_2026, arquivo))

print(f"📂 Encontrados {len(arquivos_excel)} arquivos Excel em dados/2026/")
print("=" * 80)

for caminho in arquivos_excel:
    nome_arquivo = os.path.basename(caminho)
    print(f"\n📄 {nome_arquivo}")
    
    try:
        df = pd.read_excel(caminho)
        colunas_total = len(df.columns)
        
        # Verificar duplicatas
        colunas_com_ponto = [c for c in df.columns if '.' in str(c) and str(c).rsplit('.', 1)[-1].isdigit()]
        colunas_unnamed = [c for c in df.columns if 'Unnamed' in str(c)]
        
        if len(colunas_com_ponto) > 0:
            print(f"   🔴 {len(colunas_com_ponto)} colunas com sufixo numérico: {colunas_com_ponto}")
        
        if len(colunas_unnamed) > 0:
            print(f"   🔴 {len(colunas_unnamed)} colunas Unnamed: {colunas_unnamed}")
        
        if len(colunas_com_ponto) == 0 and len(colunas_unnamed) == 0:
            print(f"   ✅ OK - {colunas_total} colunas sem duplicatas")
        
    except Exception as e:
        print(f"   ❌ Erro: {e}")

print("\n" + "=" * 80)
print("✅ Verificação concluída!")
