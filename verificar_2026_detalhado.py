"""
Verificação detalhada dos dados de 2026
"""
import pandas as pd
import os

print("=" * 80)
print("VERIFICAÇÃO DETALHADA: DADOS 2026")
print("=" * 80)

# 1. Verificar parquet de 2026
arquivo_2026 = 'dados/2026/df_final.parquet'
if os.path.exists(arquivo_2026):
    print(f"\n📂 Arquivo: {arquivo_2026}")
    df = pd.read_parquet(arquivo_2026)
    print(f"   Linhas: {len(df)}")
    print(f"   Colunas: {list(df.columns)}")
    
    if 'Período' in df.columns:
        periodos = df['Período'].unique()
        print(f"\n   📅 Períodos únicos ({len(periodos)}):")
        for p in sorted(periodos):
            count = len(df[df['Período'] == p])
            print(f"      - '{p}' ({count} registros)")
    
    if 'Ano' in df.columns:
        anos = df['Ano'].unique()
        print(f"\n   📅 Anos únicos: {sorted(anos)}")
    
    print(f"\n   📊 Primeiras 3 linhas:")
    print(df.head(3))
else:
    print(f"❌ Arquivo não encontrado: {arquivo_2026}")

# 2. Verificar histórico consolidado
arquivo_historico = 'dados/historico_consolidado/df_final_historico.parquet'
if os.path.exists(arquivo_historico):
    print(f"\n{'='*80}")
    print(f"📂 Arquivo: {arquivo_historico}")
    df_hist = pd.read_parquet(arquivo_historico)
    
    # Filtrar apenas 2026
    df_2026 = df_hist[df_hist['Ano'] == 2026]
    print(f"   Linhas totais: {len(df_hist)}")
    print(f"   Linhas de 2026: {len(df_2026)}")
    
    if len(df_2026) > 0:
        periodos_2026 = df_2026['Período'].unique()
        print(f"\n   📅 Períodos de 2026 ({len(periodos_2026)}):")
        for p in sorted(periodos_2026):
            count = len(df_2026[df_2026['Período'] == p])
            valores = df_2026[df_2026['Período'] == p]['Valor'].sum() if 'Valor' in df_2026.columns else 0
            print(f"      - '{p}' ({count} registros, Valor total: {valores:,.2f})")
        
        print(f"\n   📊 Amostra de dados 2026:")
        print(df_2026[['Ano', 'Período', 'Valor']].head(10))
    else:
        print("   ⚠️  Nenhum dado de 2026 encontrado no histórico!")
else:
    print(f"❌ Arquivo não encontrado: {arquivo_historico}")

# 3. Verificar Excel original
arquivo_excel = 'dados/2026/Reporting fluxo anexo.xlsx'
if os.path.exists(arquivo_excel):
    print(f"\n{'='*80}")
    print(f"📂 Verificando Excel: {arquivo_excel}")
    
    # Ler aba Sapiens
    df_sapiens = pd.read_excel(arquivo_excel, sheet_name='Sapiens', header=1)
    print(f"\n   Aba Sapiens:")
    print(f"   Colunas ({len(df_sapiens.columns)}): {list(df_sapiens.columns[:15])}")
    
    # Verificar se há colunas com sufixos
    colunas_com_sufixo = [col for col in df_sapiens.columns if '.' in str(col) and str(col).rsplit('.', 1)[-1].isdigit()]
    if colunas_com_sufixo:
        print(f"   ⚠️  Colunas com sufixo numérico: {colunas_com_sufixo[:10]}")
    else:
        print(f"   ✅ Nenhuma coluna com sufixo numérico")

print("\n" + "=" * 80)
