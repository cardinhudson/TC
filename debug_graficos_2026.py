"""Script para debugar por que os gráficos não aparecem em 2026"""
import pandas as pd
import os

# Carregar dados de 2026
caminho_2026 = "dados/TC_Ext/2026/df_final.parquet"
caminho_vol_2026 = "dados/TC_Ext/2026/df_vol.parquet"

print("=" * 80)
print("DEBUG: Verificando dados disponíveis para gráficos em 2026")
print("=" * 80)

if os.path.exists(caminho_2026):
    df_2026 = pd.read_parquet(caminho_2026)
    print(f"\n✅ df_final.parquet encontrado")
    print(f"   - Registros: {len(df_2026)}")
    print(f"   - Colunas: {list(df_2026.columns)}")
    
    if 'Período' in df_2026.columns:
        periodos = df_2026['Período'].unique()
        print(f"   - Períodos únicos: {sorted(periodos)}")
    
    if 'Total' in df_2026.columns:
        total_sum = df_2026['Total'].sum()
        print(f"   - Soma Total: {total_sum:,.2f}")
        print(f"   - Total por período:")
        if 'Período' in df_2026.columns:
            for periodo, total in df_2026.groupby('Período')['Total'].sum().items():
                print(f"      - {periodo}: {total:,.2f}")
    
    if 'Veículo' in df_2026.columns:
        veiculos = df_2026['Veículo'].unique()
        print(f"   - Veículos únicos ({len(veiculos)}): {sorted(veiculos)[:10]}")
    
    if 'Oficina' in df_2026.columns:
        oficinas = df_2026['Oficina'].unique()
        print(f"   - Oficinas únicas ({len(oficinas)}): {sorted(oficinas)}")
        
else:
    print(f"\n❌ Arquivo não encontrado: {caminho_2026}")

if os.path.exists(caminho_vol_2026):
    df_vol_2026 = pd.read_parquet(caminho_vol_2026)
    print(f"\n✅ df_vol.parquet encontrado")
    print(f"   - Registros: {len(df_vol_2026)}")
    print(f"   - Colunas: {list(df_vol_2026.columns)}")
    
    if 'Período' in df_vol_2026.columns:
        periodos = df_vol_2026['Período'].unique()
        print(f"   - Períodos únicos: {sorted(periodos)}")
    
    if 'Volume' in df_vol_2026.columns:
        volume_sum = df_vol_2026['Volume'].sum()
        print(f"   - Soma Volume: {volume_sum:,.2f}")
        print(f"   - Volume por período:")
        if 'Período' in df_vol_2026.columns:
            for periodo, volume in df_vol_2026.groupby('Período')['Volume'].sum().items():
                print(f"      - {periodo}: {volume:,.2f}")
    
    if 'Veículo' in df_vol_2026.columns:
        veiculos = df_vol_2026['Veículo'].unique()
        print(f"   - Veículos únicos ({len(veiculos)}): {sorted(veiculos)[:10]}")
else:
    print(f"\n❌ Arquivo não encontrado: {caminho_vol_2026}")

# Verificar arquivos de budget
caminho_budget = "dados/TC_Ext/historico_consolidado/BUD/df_final_historico_BUD.parquet"
if os.path.exists(caminho_budget):
    df_budget = pd.read_parquet(caminho_budget)
    if 'Ano' in df_budget.columns:
        df_budget_2026 = df_budget[df_budget['Ano'] == 2026]
        print(f"\n✅ Budget 2026 encontrado")
        print(f"   - Registros: {len(df_budget_2026)}")
        if 'Período' in df_budget_2026.columns:
            periodos = df_budget_2026['Período'].unique()
            print(f"   - Períodos únicos: {sorted(periodos)}")
            # Verificar se há sufixos
            periodos_com_sufixo = [p for p in periodos if '.1' in str(p) or '.2' in str(p)]
            if periodos_com_sufixo:
                print(f"   ⚠️ PERÍODOS COM SUFIXO ENCONTRADOS: {periodos_com_sufixo}")
else:
    print(f"\n❌ Arquivo de budget não encontrado: {caminho_budget}")

print("\n" + "=" * 80)
print("DIAGNÓSTICO")
print("=" * 80)
print("\nPara os gráficos aparecerem, é necessário:")
print("1. ✓ df_final.parquet com coluna 'Período' e 'Total'")
print("2. ✓ df_vol.parquet com coluna 'Período' e 'Volume'")  
print("3. ✓ Dados não podem estar vazios após agrupamento")
print("4. ✓ Valores não podem ser todos zero/NaN")
