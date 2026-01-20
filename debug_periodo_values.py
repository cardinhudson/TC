"""
Debug: Verificar valores na coluna Período que podem ter sufixos .1, .2, etc
"""
import pandas as pd
import os

# Carregar o arquivo consolidado
caminho = os.path.join('dados', 'historico_consolidado', 'df_final_historico.parquet')
df = pd.read_parquet(caminho)

print("=" * 80)
print("ANÁLISE DE VALORES NA COLUNA 'PERÍODO'")
print("=" * 80)

# Verificar se há valores com sufixo .1, .2, etc na coluna Período
if 'Período' in df.columns:
    periodos_unicos = df['Período'].unique()
    print(f"\n📅 Total de períodos únicos: {len(periodos_unicos)}")
    
    # Verificar se há períodos com sufixo numérico
    periodos_com_sufixo = []
    for periodo in periodos_unicos:
        periodo_str = str(periodo)
        if '.' in periodo_str:
            partes = periodo_str.rsplit('.', 1)
            if len(partes) == 2 and partes[1].isdigit():
                periodos_com_sufixo.append(periodo_str)
    
    if periodos_com_sufixo:
        print(f"\n🔴 ENCONTRADOS {len(periodos_com_sufixo)} PERÍODOS COM SUFIXO NUMÉRICO:")
        for p in sorted(periodos_com_sufixo):
            count = len(df[df['Período'] == p])
            print(f"   - '{p}' ({count} registros)")
    else:
        print(f"\n✅ Nenhum período com sufixo numérico encontrado")
    
    print(f"\n📊 Todos os períodos únicos:")
    for p in sorted(periodos_unicos):
        count = len(df[df['Período'] == p])
        print(f"   - '{p}' ({count} registros)")
else:
    print("❌ Coluna 'Período' não encontrada no DataFrame")

# Verificar também a coluna Mes se existir
if 'Mes' in df.columns:
    meses_unicos = df['Mes'].unique()
    print(f"\n📅 Valores únicos na coluna 'Mes': {sorted(meses_unicos)}")

print("\n" + "=" * 80)
print("✅ Análise concluída!")
