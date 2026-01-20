"""
Script de debug detalhado para verificar por que os gráficos não aparecem em 2026
"""
import pandas as pd
import re
from pathlib import Path

def limpar_periodo_sufixos(df):
    """Remove sufixos .1, .2, .3 etc dos valores na coluna 'Período'"""
    if 'Período' not in df.columns:
        return df
    
    if pd.api.types.is_categorical_dtype(df['Período']):
        df['Período'] = df['Período'].astype(str)
    
    df['Período'] = df['Período'].str.replace(r'\.\d+$', '', regex=True)
    return df

# Carregar dados como o app.py faz
ano = 2026

print("="*80)
print(f"DEBUG DETALHADO - VERIFICANDO DADOS PARA {ano}")
print("="*80)

# 1. Verificar dados reais
arquivo_real = Path(f'dados/{ano}/df_final.parquet')
if arquivo_real.exists():
    df_real = pd.read_parquet(arquivo_real)
    df_real = limpar_periodo_sufixos(df_real)
    
    print(f"\n1. DADOS REAIS {ano}")
    print(f"   Arquivo: {arquivo_real}")
    print(f"   Total de registros: {len(df_real)}")
    print(f"   Colunas: {df_real.columns.tolist()}")
    print(f"   Períodos únicos: {sorted(df_real['Período'].unique())}")
    print(f"   Total somado: {df_real['Total'].sum():,.2f}")
    
    # Verificar se há períodos com sufixos
    periodos_com_sufixo = df_real[df_real['Período'].str.contains(r'\.\d+$', regex=True, na=False)]
    if len(periodos_com_sufixo) > 0:
        print(f"   ⚠️ PERÍODOS COM SUFIXO: {periodos_com_sufixo['Período'].unique()}")
    else:
        print(f"   ✅ Nenhum período com sufixo encontrado")
else:
    print(f"\n1. DADOS REAIS {ano}")
    print(f"   ❌ Arquivo não encontrado: {arquivo_real}")

# 2. Verificar dados de volume
arquivo_vol = Path(f'dados/{ano}/df_vol.parquet')
if arquivo_vol.exists():
    df_vol = pd.read_parquet(arquivo_vol)
    df_vol = limpar_periodo_sufixos(df_vol)
    
    print(f"\n2. DADOS VOLUME {ano}")
    print(f"   Arquivo: {arquivo_vol}")
    print(f"   Total de registros: {len(df_vol)}")
    print(f"   Colunas: {df_vol.columns.tolist()}")
    print(f"   Períodos únicos: {sorted(df_vol['Período'].unique())}")
    print(f"   Volume total: {df_vol['Volume'].sum():,.2f}")
    
    # Verificar se há períodos com sufixos
    periodos_com_sufixo = df_vol[df_vol['Período'].str.contains(r'\.\d+$', regex=True, na=False)]
    if len(periodos_com_sufixo) > 0:
        print(f"   ⚠️ PERÍODOS COM SUFIXO: {periodos_com_sufixo['Período'].unique()}")
    else:
        print(f"   ✅ Nenhum período com sufixo encontrado")
else:
    print(f"\n2. DADOS VOLUME {ano}")
    print(f"   ❌ Arquivo não encontrado: {arquivo_vol}")

# 3. Verificar dados de budget
arquivo_budget = Path('dados/historico_consolidado/BUD/df_final_historico_BUD.parquet')
if arquivo_budget.exists():
    df_budget = pd.read_parquet(arquivo_budget)
    df_budget = limpar_periodo_sufixos(df_budget)
    df_budget_ano = df_budget[df_budget['Ano'] == ano]
    
    print(f"\n3. DADOS BUDGET {ano}")
    print(f"   Arquivo: {arquivo_budget}")
    print(f"   Total de registros (ano {ano}): {len(df_budget_ano)}")
    if len(df_budget_ano) > 0:
        print(f"   Colunas: {df_budget_ano.columns.tolist()}")
        print(f"   Períodos únicos: {sorted(df_budget_ano['Período'].unique())}")
        print(f"   Total somado: {df_budget_ano['Total'].sum():,.2f}")
        
        # Verificar se há períodos com sufixos
        periodos_com_sufixo = df_budget_ano[df_budget_ano['Período'].str.contains(r'\.\d+$', regex=True, na=False)]
        if len(periodos_com_sufixo) > 0:
            print(f"   ⚠️ PERÍODOS COM SUFIXO: {periodos_com_sufixo['Período'].unique()}")
        else:
            print(f"   ✅ Nenhum período com sufixo encontrado")
    else:
        print(f"   ⚠️ Nenhum registro encontrado para o ano {ano}")
else:
    print(f"\n3. DADOS BUDGET {ano}")
    print(f"   ❌ Arquivo não encontrado: {arquivo_budget}")

# 4. Verificar dados de volume budget
arquivo_vol_budget = Path('dados/historico_consolidado/BUD/df_vol_historico_BUD.parquet')
if arquivo_vol_budget.exists():
    df_vol_budget = pd.read_parquet(arquivo_vol_budget)
    df_vol_budget = limpar_periodo_sufixos(df_vol_budget)
    df_vol_budget_ano = df_vol_budget[df_vol_budget['Ano'] == ano]
    
    print(f"\n4. DADOS VOLUME BUDGET {ano}")
    print(f"   Arquivo: {arquivo_vol_budget}")
    print(f"   Total de registros (ano {ano}): {len(df_vol_budget_ano)}")
    if len(df_vol_budget_ano) > 0:
        print(f"   Colunas: {df_vol_budget_ano.columns.tolist()}")
        print(f"   Períodos únicos: {sorted(df_vol_budget_ano['Período'].unique())}")
        print(f"   Volume total: {df_vol_budget_ano['Volume'].sum():,.2f}")
        
        # Verificar se há períodos com sufixos
        periodos_com_sufixo = df_vol_budget_ano[df_vol_budget_ano['Período'].str.contains(r'\.\d+$', regex=True, na=False)]
        if len(periodos_com_sufixo) > 0:
            print(f"   ⚠️ PERÍODOS COM SUFIXO: {periodos_com_sufixo['Período'].unique()}")
        else:
            print(f"   ✅ Nenhum período com sufixo encontrado")
    else:
        print(f"   ⚠️ Nenhum registro encontrado para o ano {ano}")
else:
    print(f"\n4. DADOS VOLUME BUDGET {ano}")
    print(f"   ❌ Arquivo não encontrado: {arquivo_vol_budget}")

# 5. SIMULAR CRIAÇÃO DE GRÁFICO DE PERÍODO
print("\n" + "="*80)
print("5. SIMULAÇÃO DE CRIAÇÃO DO GRÁFICO DE PERÍODO")
print("="*80)

try:
    # Simular o que o app.py faz
    if arquivo_real.exists() and arquivo_vol.exists() and arquivo_budget.exists():
        df_real_grafico = df_real.copy()
        df_vol_grafico = df_vol.copy()
        df_budget_grafico = df_budget_ano.copy()
        
        # Agregar por período
        real_periodo = df_real_grafico.groupby('Período')['Total'].sum().reset_index()
        real_periodo['Tipo'] = 'Real'
        
        budget_periodo = df_budget_grafico.groupby('Período')['Total'].sum().reset_index()
        budget_periodo['Tipo'] = 'Budget'
        
        # Combinar
        df_grafico = pd.concat([real_periodo, budget_periodo], ignore_index=True)
        
        print(f"DataFrame combinado:")
        print(f"  Total de linhas: {len(df_grafico)}")
        print(f"  Colunas: {df_grafico.columns.tolist()}")
        print(f"\nDados:")
        print(df_grafico.to_string())
        
        if len(df_grafico) == 0:
            print("\n❌ PROBLEMA: DataFrame vazio - gráfico não será criado!")
        else:
            print("\n✅ DataFrame tem dados - gráfico deveria aparecer")
            
except Exception as e:
    print(f"❌ ERRO ao simular gráfico: {e}")
    import traceback
    traceback.print_exc()

# 6. SIMULAR CRIAÇÃO DE GRÁFICO DE VOLUME POR VEÍCULO
print("\n" + "="*80)
print("6. SIMULAÇÃO DE GRÁFICO DE VOLUME POR VEÍCULO")
print("="*80)

try:
    if arquivo_vol.exists() and arquivo_vol_budget.exists():
        # Agregar por veículo
        real_veiculo = df_vol.groupby('Veículo')['Volume'].sum().reset_index()
        real_veiculo['Tipo'] = 'Real'
        
        budget_veiculo = df_vol_budget_ano.groupby('Veículo')['Volume'].sum().reset_index()
        budget_veiculo['Tipo'] = 'Budget'
        
        # Combinar
        df_grafico_veiculo = pd.concat([real_veiculo, budget_veiculo], ignore_index=True)
        
        print(f"DataFrame volume por veículo:")
        print(f"  Total de linhas: {len(df_grafico_veiculo)}")
        print(f"  Colunas: {df_grafico_veiculo.columns.tolist()}")
        print(f"\nDados:")
        print(df_grafico_veiculo.to_string())
        
        if len(df_grafico_veiculo) == 0:
            print("\n❌ PROBLEMA: DataFrame vazio - gráfico não será criado!")
        else:
            print("\n✅ DataFrame tem dados - gráfico deveria aparecer")
            
except Exception as e:
    print(f"❌ ERRO ao simular gráfico: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("FIM DO DEBUG")
print("="*80)
