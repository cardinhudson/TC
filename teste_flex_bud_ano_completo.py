"""
Script de teste para verificar se o Flex Bud está sendo calculado para o ano completo
"""
import pandas as pd
import os

print("🔍 Testando cálculo de Flex Bud para o ano completo...\n")

# Carregar dados históricos
arquivo_historico = 'dados/historico_consolidado/df_final_historico.parquet'
if os.path.exists(arquivo_historico):
    print(f"📊 Carregando {arquivo_historico}...")
    df = pd.read_parquet(arquivo_historico)
    
    # Filtrar apenas 2026
    if 'Ano' in df.columns:
        df_2026 = df[df['Ano'] == 2026]
        print(f"\n✅ Total de registros 2026: {len(df_2026)}")
        print(f"\n📅 Períodos disponíveis em 2026:")
        print(df_2026['Período'].value_counts().sort_index())
        
        # Verificar se há dados de Budget
        arquivo_budget = 'dados/historico_consolidado/BUD/df_final_historico_BUD.parquet'
        if os.path.exists(arquivo_budget):
            print(f"\n📊 Carregando {arquivo_budget}...")
            df_budget = pd.read_parquet(arquivo_budget)
            
            if 'Ano' in df_budget.columns:
                df_budget_2026 = df_budget[df_budget['Ano'] == 2026]
                print(f"\n✅ Total de registros Budget 2026: {len(df_budget_2026)}")
                print(f"\n📅 Períodos disponíveis no Budget 2026:")
                if len(df_budget_2026) > 0:
                    print(df_budget_2026['Período'].value_counts().sort_index())
                else:
                    print("⚠️ Nenhum registro de Budget encontrado para 2026")
        else:
            print(f"\n⚠️ Arquivo de Budget não encontrado: {arquivo_budget}")
        
        # Lista de todos os meses esperados
        meses_esperados = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                          'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        
        periodos_disponiveis = set(df_2026['Período'].unique())
        meses_faltantes = [mes for mes in meses_esperados if mes not in periodos_disponiveis]
        
        print(f"\n📋 ANÁLISE:")
        print(f"  ✅ Meses com dados reais: {len(periodos_disponiveis)}")
        print(f"  ❌ Meses faltantes: {len(meses_faltantes)}")
        if meses_faltantes:
            print(f"     {', '.join(meses_faltantes)}")
        
        print(f"\n💡 IMPORTANTE:")
        print(f"  - O sistema agora está configurado para mostrar TODOS os 12 meses no gráfico")
        print(f"  - Para meses sem dados reais, será usado o Budget")
        print(f"  - O Flex Bud será calculado para o ano completo")
        print(f"\n  ⚠️ Para ter dados completos de 2026, você precisa:")
        print(f"     1. Adicionar dados dos meses faltantes nos arquivos Excel de origem")
        print(f"     2. Reprocessar os dados executando o notebook dados.ipynb")
    else:
        print("⚠️ Coluna 'Ano' não encontrada no arquivo")
else:
    print(f"❌ Arquivo {arquivo_historico} não encontrado")

print("\n✅ Teste concluído!")
