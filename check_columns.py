import pandas as pd
import os

caminho = os.path.join('dados', 'historico_consolidado', 'df_final_historico.parquet')
df = pd.read_parquet(caminho)

print(f'Total de colunas: {len(df.columns)}')

# Verificar colunas com sufixos numéricos
colunas_com_ponto = [c for c in df.columns if '.' in str(c) and str(c).rsplit('.', 1)[-1].isdigit()]
print(f'\n🔴 Colunas com sufixo numérico (.1, .2, .3): {len(colunas_com_ponto)}')
if colunas_com_ponto:
    print(f'Exemplos: {colunas_com_ponto}')

# Verificar colunas Unnamed
colunas_unnamed = [c for c in df.columns if 'Unnamed' in str(c) or 'unnamed' in str(c).lower()]
print(f'\n🔴 Colunas Unnamed: {len(colunas_unnamed)}')
if colunas_unnamed:
    print(f'Exemplos: {colunas_unnamed}')

print(f'\n=== Todas as colunas ({len(df.columns)}): ===')
for i, c in enumerate(df.columns, 1):
    print(f'{i}. {c}')
