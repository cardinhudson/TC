"""
Verificar dados de BUD de 2026
"""
import pandas as pd
import os

print("=" * 80)
print("VERIFICAÇÃO: DADOS DE BUDGET 2026")
print("=" * 80)

arquivos_budget = [
    'dados/TC_Ext/2026/BUD/df_final_BUD.parquet',
    'dados/TC_Ext/historico_consolidado/BUD/df_final_historico_BUD.parquet'
]

for arq in arquivos_budget:
    if os.path.exists(arq):
        print(f"\n📂 {arq}")
        df = pd.read_parquet(arq)
        
        if 'Ano' in df.columns:
            df_2026 = df[df['Ano'] == 2026]
            print(f"   Total 2026: {len(df_2026)} linhas")
            
            if 'Período' in df_2026.columns:
                periodos = sorted(df_2026['Período'].unique())
                print(f"   Períodos de 2026 ({len(periodos)}): {periodos}")
                
                # Ver quantos valores por período
                for per in periodos:
                    count = len(df_2026[df_2026['Período'] == per])
                    valor_total = df_2026[df_2026['Período'] == per]['Total'].sum() if 'Total' in df_2026.columns else 0
                    print(f"      - '{per}': {count} registros, Total: {valor_total:,.2f}")
        else:
            print(f"   ⚠️  Coluna 'Ano' não encontrada")
    else:
        print(f"\n❌ {arq}: Não encontrado")

print("\n" + "=" * 80)
