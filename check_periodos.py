"""
Testar se há períodos/meses duplicados nos dados
"""
import pandas as pd
import os

# Carregar o arquivo consolidado
caminho = os.path.join('dados', 'historico_consolidado', 'df_final_historico.parquet')
df = pd.read_parquet(caminho)

print("=" * 80)
print("ANÁLISE DE PERÍODOS NO DF_FINAL_HISTORICO")
print("=" * 80)

# Verificar coluna Período
if 'Período' in df.columns:
    periodos_unicos = df['Período'].unique()
    print(f"\n📅 Total de períodos únicos: {len(periodos_unicos)}")
    print("\nPrimeiros 20 períodos:")
    for i, periodo in enumerate(sorted(periodos_unicos)[:20], 1):
        print(f"   {i}. {periodo}")
    
    # Verificar se há duplicatas por ano
    if 'Ano' in df.columns:
        print(f"\n📊 Períodos por ano:")
        for ano in sorted(df['Ano'].unique()):
            periodos_ano = df[df['Ano'] == ano]['Período'].unique()
            print(f"\n   Ano {ano}: {len(periodos_ano)} períodos")
            # Verificar se há sufixos .1, .2
            periodos_com_sufixo = [p for p in periodos_ano if '.' in str(p) and str(p).rsplit('.', 1)[-1].isdigit()]
            if periodos_com_sufixo:
                print(f"      🔴 ENCONTROU {len(periodos_com_sufixo)} períodos COM SUFIXO NUMÉRICO:")
                for p in sorted(periodos_com_sufixo):
                    print(f"         - {p}")
            
            # Mostrar alguns exemplos
            print(f"      Exemplos: {sorted(periodos_ano)[:10]}")

# Verificar coluna Mes se existir
if 'Mes' in df.columns:
    meses_unicos = df['Mes'].unique()
    print(f"\n📅 Total de meses únicos na coluna 'Mes': {len(meses_unicos)}")
    print("\nMeses:")
    for i, mes in enumerate(sorted(meses_unicos)[:20], 1):
        print(f"   {i}. {mes}")

print("\n" + "=" * 80)
print("✅ Análise concluída!")
