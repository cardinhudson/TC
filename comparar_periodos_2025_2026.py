"""
Script para comparar períodos/meses entre arquivos Excel de 2025 e 2026
e identificar a origem dos meses duplicados
"""
import pandas as pd
import os

print("=" * 80)
print("COMPARAÇÃO DE PERÍODOS: 2025 vs 2026")
print("=" * 80)

# Arquivos a verificar
arquivos = [
    ("2025", "dados/2025/df_final.xlsx"),
    ("2026", "dados/2026/df_final.xlsx"),
    ("2025", "dados/2025/Dados SAPIENS.xlsx"),
    ("2026", "dados/2026/Dados SAPIENS.xlsx"),
    ("2025", "dados/2025/Reporting fluxo anexo.xlsx"),
    ("2026", "dados/2026/Reporting fluxo anexo.xlsx"),
]

resultados = {}

for ano, caminho in arquivos:
    if not os.path.exists(caminho):
        print(f"⚠️  Arquivo não encontrado: {caminho}")
        continue
    
    print(f"\n{'='*80}")
    print(f"📂 Analisando: {caminho}")
    print(f"{'='*80}")
    
    try:
        # Tentar ler todas as abas do Excel
        xls = pd.ExcelFile(caminho)
        print(f"   Abas encontradas: {xls.sheet_names}")
        
        for sheet_name in xls.sheet_names:
            print(f"\n   📊 Aba: {sheet_name}")
            df = pd.read_excel(caminho, sheet_name=sheet_name)
            
            # Mostrar as primeiras colunas
            print(f"      Colunas: {list(df.columns[:10])}")
            
            # Procurar colunas que possam conter períodos/meses
            colunas_periodo = [col for col in df.columns if any(
                palavra in str(col).lower() 
                for palavra in ['período', 'periodo', 'mês', 'mes', 'month', 'data', 'date']
            )]
            
            if colunas_periodo:
                print(f"      ✅ Colunas de período encontradas: {colunas_periodo}")
                for col in colunas_periodo:
                    valores_unicos = df[col].dropna().unique()
                    print(f"         └─ {col}: {len(valores_unicos)} valores únicos")
                    print(f"            Exemplos: {list(valores_unicos[:10])}")
                    
                    # Verificar se há sufixos .1, .2, etc
                    valores_com_sufixo = [
                        v for v in valores_unicos 
                        if isinstance(v, str) and '.' in v and v.rsplit('.', 1)[-1].isdigit()
                    ]
                    if valores_com_sufixo:
                        print(f"            ⚠️  VALORES COM SUFIXO NUMÉRICO ENCONTRADOS:")
                        print(f"            {valores_com_sufixo}")
                        resultados[f"{ano}_{os.path.basename(caminho)}_{sheet_name}_{col}"] = valores_com_sufixo
            
            # Também verificar se há colunas de meses como cabeçalhos
            colunas_meses = [col for col in df.columns if any(
                mes in str(col).lower() 
                for mes in ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
                           'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro',
                           'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
            )]
            
            if colunas_meses:
                print(f"      📅 Colunas com nomes de meses no cabeçalho:")
                colunas_com_sufixo = [
                    col for col in colunas_meses 
                    if '.' in str(col) and str(col).rsplit('.', 1)[-1].isdigit()
                ]
                if colunas_com_sufixo:
                    print(f"         ⚠️  COLUNAS COM SUFIXO NUMÉRICO:")
                    for col in colunas_com_sufixo:
                        print(f"            - {col}")
                    resultados[f"{ano}_{os.path.basename(caminho)}_{sheet_name}_COLUNAS"] = colunas_com_sufixo
                else:
                    print(f"         ✅ Todas as {len(colunas_meses)} colunas de meses estão limpas")
                    print(f"         Exemplos: {colunas_meses[:5]}")
    
    except Exception as e:
        print(f"   ❌ Erro ao ler arquivo: {str(e)}")

# Resumo final
print("\n" + "=" * 80)
print("RESUMO: ARQUIVOS COM PROBLEMAS")
print("=" * 80)

if resultados:
    print("\n⚠️  ENCONTRADOS VALORES/COLUNAS COM SUFIXOS NUMÉRICOS:")
    for chave, valores in resultados.items():
        print(f"\n   {chave}:")
        for v in valores:
            print(f"      - {v}")
else:
    print("\n✅ Nenhum valor ou coluna com sufixo numérico encontrado nos arquivos Excel")

print("\n" + "=" * 80)
