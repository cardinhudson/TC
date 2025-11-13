# %%
# ler o arquivo em excel KE5Z_veiculos.xls
import pandas as pd

df_KE5Z  = pd.read_excel('KE5Z_veiculos.xlsx')

# mostrar o arquivo em excel df_KE5Z
df_KE5Z.head(50)

# Fazer o somatorio da coluna Valor e imprimir na tela
print(df_KE5Z['Valor'].sum())

# exibir as primeiras linhas
df_KE5Z.head(20)












# %%
# Ler o arquivo em excel Reporting fluxo anexo.xlsx, ler a guia Rateio,
# excluir a primeira linha (linha de referência) e usar a segunda linha como cabeçalho (meses)

# Ler a guia "Rateio" do arquivo Excel, sem header para manipular manualmente
df_raw = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Rateio', header=None)

# Excluir a primeira linha (linha de referência)
df = df_raw.iloc[1:].reset_index(drop=True)

# Usar a primeira linha (que agora é a linha dos nomes/meses) como cabeçalho real
df.columns = df.iloc[0]

# Excluir a linha usada como cabeçalho
df = df.iloc[1:].reset_index(drop=True)

# Remover colunas totalmente NaN (colunas extras do Excel)
df = df.loc[:, df.notna().any(axis=0)]

# Filtrar colunas que possuem todos os valores NaN (antes do melt)
df = df.dropna(axis=1, how='all')

# Identificar as colunas que são meses (janeiro a dezembro)
meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
         'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

# Encontrar as colunas que são meses (desconsiderando capitalização)
colunas_meses = [col for col in df.columns if any(mes.lower() in str(col).lower() for mes in meses)]

# Identificar as colunas que NÃO são meses (para usar como id_vars)
colunas_id = [col for col in df.columns if col not in colunas_meses and pd.notna(col)]

# Remover colunas com nome NaN
df = df.loc[:, df.columns.notna()]



# Agora transformar as colunas de meses em linhas
df = df.melt(id_vars=colunas_id, value_vars=colunas_meses, var_name='Mês', value_name='Rateio')

# Converter a coluna Rateio para numérico, substituir NaN por zero
# NÃO arredondar para manter máxima precisão e evitar erros de arredondamento
df['Rateio'] = pd.to_numeric(df['Rateio'], errors='coerce').fillna(0)

# substituir o nome da coluna Mês por Período
df = df.rename(columns={'Mês': 'Período'})

# filtrar na tabela df e linha Oficina tudo que é diferente de veículo
df = df[df['Oficina'] != 'Veículos']

# tirar o nan no filtro
df = df[df['Oficina'].notna()]



# mostrar um somatorio da coluna Rateio
print(df['Rateio'].sum())
# exibir as primeiras linhas
df.head(100)







# %%


# %%
# --- VERIFICAÇÃO DE ERROS E USO DAS CHAVES 'Oficina' e 'Período' ---

# 1. Conferir colunas presentes
print("Colunas em df_KE5Z:", df_KE5Z.columns.tolist())
print("Colunas em df    :", df.columns.tolist())

# 2. Checar existência das colunas essenciais
erros = []
for nome_df, dfx in [('df_KE5Z', df_KE5Z), ('df', df)]:
    for col in ['Oficina', 'Período']:
        if col not in dfx.columns:
            erros.append(f"Coluna '{col}' ausente no {nome_df}.")

if erros:
    for erro in erros:
        print("ERRO:", erro)
    raise KeyError(" ".join(erros))

# 3. Conferir quantidade de linhas antes do merge
print(f"\nUsando 'Oficina' e 'Período' como chaves de merge")
print(f"Linhas em df_KE5Z: {len(df_KE5Z)}")
print(f"Linhas em df     : {len(df)}")

# 4. Realizar merge tendo certeza do nome correto das chaves
try:
    df_merge = pd.merge(df_KE5Z, df, on=['Oficina', 'Período'], how='left', suffixes=('', '_df'))
except Exception as e:
    print("Erro ao realizar o merge usando as colunas 'Oficina' e 'Período'.")
    raise

print(f"Linhas após merge: {len(df_merge)}")

# 5. Checar se a coluna Rateio veio corretamente
if 'Rateio' in df_merge.columns:
    rateio_nao_nulo = df_merge['Rateio'].notna().sum()
    print(f"Linhas com Rateio encontrado: {rateio_nao_nulo}")
else:
    print("AVISO: Coluna 'Rateio' não encontrada após o merge.")

# 6. Validar presença da coluna 'Veículo' para o pivot
if 'Veículo' not in df_merge.columns:
    raise KeyError("Coluna 'Veículo' não encontrada em df_merge. Verifique se esta coluna existe e está corretamente capitalizada em ambos DataFrames.")

# 7. Pivot para transformar veículos em colunas de Rateio
# Usar 'mean' para agregar valores duplicados (mais apropriado para rateios)
try:
    df_pivot = df_merge.pivot_table(
        index=['Oficina', 'Período'],
        columns='Veículo',
        values='Rateio',
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None
    print(f"\nLinhas após pivot (Oficina + Período): {len(df_pivot)}")
except Exception as e:
    print("Erro ao executar pivot_table em df_merge usando 'Veículo'.")
    raise

# 8. Merge reverso: incluir dados originais do KE5Z
try:
    df_final = pd.merge(df_KE5Z, df_pivot, on=['Oficina', 'Período'], how='left')
except Exception as e:
    print("Erro ao executar o merge final com pivot.")
    raise

# 9. Determinar colunas novas (veículos criados) e renomear para %
veiculos_cols = [col for col in df_final.columns if col not in df_KE5Z.columns and col not in ['Oficina', 'Período']]
rename_dict = {col: f"{col}%" for col in veiculos_cols}
df_final = df_final.rename(columns=rename_dict)

# Atualizar lista de veiculos_cols para aquelas com %
veiculos_cols_pct = [f"{col}%" for col in veiculos_cols]
veiculos_cols = [col for col in veiculos_cols_pct if col in df_final.columns]

# 10. Garantir que todas as colunas de veículos estejam em float64, sem perder casas decimais, e NaN->0
import numpy as np
for col in veiculos_cols:
    # Remover eventualmente o símbolo % para padronizar antes da conversão, se vier por engano
    if df_final[col].dtype == "object":
        df_final[col] = df_final[col].astype(str).str.replace('%', '', regex=False).str.strip()
    # Converter para float64 (não arredonda nem corta casas decimais)
    df_final[col] = pd.to_numeric(df_final[col], errors='coerce').astype(np.float64).fillna(0.0)

# 11. Diagnóstico final
print(f"\nDataFrame final criado com {len(df_final)} linhas e {len(df_final.columns)} colunas")
print(f"Colunas de veículos criadas: {len(veiculos_cols)}")
print("Colunas de veículos:", veiculos_cols)
print("\nTipos das colunas de veículos:")
for col in veiculos_cols:
    print(f"  {col}: {df_final[col].dtype} (exemplo valor: {df_final[col].dropna().iloc[0] if not df_final[col].dropna().empty else 'N/A'})")

# 12. Conferir dados de HVAC
if 'Oficina' in df_final.columns:
    df_hvac = df_final[df_final['Oficina'] == 'HVAC']
    print(f"\nLinhas com HVAC: {len(df_hvac)}")
    display(df_hvac.head(7))
else:
    print("AVISO: Coluna 'Oficina' não existe em df_final.")
    df_hvac = pd.DataFrame()


# %%


# %%
# Criar novas colunas calculando: Coluna% * Valor
# As colunas de percentual estão como float (ex: 0.419 para 41.9%)
# Multiplicar diretamente pela coluna Valor

print("Criando colunas de cálculo (Percentual * Valor)...")

# Verificar se a coluna 'Valor' existe
if 'Valor' not in df_final.columns:
    print("ERRO: Coluna 'Valor' não encontrada no DataFrame!")
    print(f"Colunas disponíveis: {df_final.columns.tolist()}")
else:
    print(f"Coluna 'Valor' encontrada. Tipo: {df_final['Valor'].dtype}")
    
    # Converter coluna Valor para numérico se necessário
    df_final['Valor'] = pd.to_numeric(df_final['Valor'], errors='coerce').fillna(0)
    
    # Lista de colunas de veículos com %
    veiculos_cols_pct = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']
    
    # Criar uma coluna para cada veículo com o cálculo
    for col_pct in veiculos_cols_pct:
        if col_pct in df_final.columns:
            # Nome da nova coluna sem o "%" (ex: "CC21")
            col_nome = col_pct.replace('%', '')
            
            # Calcular: Percentual * Valor
            # Como o percentual já está em decimal (ex: 0.419), multiplicar diretamente
            df_final[col_nome] = df_final[col_pct] * df_final['Valor']
            
            print(f"  Criada coluna '{col_nome}' = {col_pct} * Valor")
        else:
            print(f"  AVISO: Coluna '{col_pct}' não encontrada")
    
    print(f"\nTotal de colunas no DataFrame final: {len(df_final.columns)}")
    print(f"Novas colunas criadas: {[col.replace('%', '') for col in veiculos_cols_pct if col in df_final.columns]}")
    
    # Exibir as primeiras linhas para verificar
    print("\nPrimeiras linhas do DataFrame final:")
    display(df_final.head(10))

    # fazer somatorio da coluna Valor
    print(df_final['Valor'].sum())

    # somar as colunas CC21, CC22, CC24, CC24 5L, CC24 7L, J516
    print(df_final['CC21'].sum() + df_final['CC22'].sum() + df_final['CC24'].sum() + df_final['CC24 5L'].sum() + df_final['CC24 7L'].sum() + df_final['J516'].sum())

# gerar um excel com o df_final
df_final.to_excel('df_final.xlsx', index=False)









# %%
# ANÁLISE: Verificar se a soma dos percentuais está dando 100% em cada linha
print("="*70)
print("ANÁLISE: SOMA DOS PERCENTUAIS POR LINHA")
print("="*70)

# Lista de colunas de percentual
veiculos_cols_pct = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']

# Calcular a soma dos percentuais para cada linha
df_final['Soma_Percentuais'] = df_final[veiculos_cols_pct].sum(axis=1)

# Verificar quantas linhas têm rateios (soma > 0)
linhas_com_rateio = (df_final['Soma_Percentuais'] > 0).sum()
linhas_sem_rateio = (df_final['Soma_Percentuais'] == 0).sum()

print(f"\n1. DISTRIBUIÇÃO DE LINHAS:")
print(f"   Linhas COM rateios (soma > 0): {linhas_com_rateio:,}")
print(f"   Linhas SEM rateios (soma = 0): {linhas_sem_rateio:,}")
print(f"   Total de linhas: {len(df_final):,}")

# Verificar se a soma está próxima de 1.0 (100%) nas linhas com rateio
df_com_rateio = df_final[df_final['Soma_Percentuais'] > 0]
if len(df_com_rateio) > 0:
    print(f"\n2. ANÁLISE DAS LINHAS COM RATEIOS:")
    print(f"   Soma média dos percentuais: {df_com_rateio['Soma_Percentuais'].mean():.4f}")
    print(f"   Soma mínima: {df_com_rateio['Soma_Percentuais'].min():.4f}")
    print(f"   Soma máxima: {df_com_rateio['Soma_Percentuais'].max():.4f}")
    
    # Contar linhas onde a soma não está próxima de 1.0
    linhas_fora_100 = df_com_rateio[abs(df_com_rateio['Soma_Percentuais'] - 1.0) > 0.01]
    print(f"\n   ⚠️ Linhas onde soma ≠ 100% (diferença > 1%): {len(linhas_fora_100)}")
    
    if len(linhas_fora_100) > 0:
        print(f"\n   Exemplos de linhas com soma diferente de 100%:")
        display(linhas_fora_100[['Oficina', 'Período', 'Valor', 'Soma_Percentuais'] + veiculos_cols_pct].head(10))

# Verificar especificamente julho e agosto
print(f"\n3. ANÁLISE ESPECÍFICA: JULHO E AGOSTO")
df_jul_ago = df_final[df_final['Período'].isin(['Julho', 'Agosto', 'julho', 'agosto'])]
if len(df_jul_ago) > 0:
    print(f"   Total de linhas: {len(df_jul_ago):,}")
    linhas_com_rateio_jul_ago = (df_jul_ago['Soma_Percentuais'] > 0).sum()
    print(f"   Linhas COM rateios: {linhas_com_rateio_jul_ago:,}")
    
    if linhas_com_rateio_jul_ago > 0:
        df_jul_ago_com_rateio = df_jul_ago[df_jul_ago['Soma_Percentuais'] > 0]
        print(f"   Soma média dos percentuais: {df_jul_ago_com_rateio['Soma_Percentuais'].mean():.4f}")
        
        linhas_fora_100_jul_ago = df_jul_ago_com_rateio[abs(df_jul_ago_com_rateio['Soma_Percentuais'] - 1.0) > 0.01]
        print(f"   ⚠️ Linhas onde soma ≠ 100%: {len(linhas_fora_100_jul_ago)}")
        
        if len(linhas_fora_100_jul_ago) > 0:
            print(f"\n   Exemplos de linhas problemáticas em Julho/Agosto:")
            display(linhas_fora_100_jul_ago[['Oficina', 'Período', 'Valor', 'Soma_Percentuais'] + veiculos_cols_pct].head(10))

# Verificar totais
print(f"\n4. VERIFICAÇÃO DE TOTAIS:")
soma_valor_total = df_final['Valor'].sum()
soma_valor_com_rateio = df_com_rateio['Valor'].sum() if len(df_com_rateio) > 0 else 0
soma_calc_total = df_final[['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']].sum().sum()

print(f"   Soma total da coluna Valor: {soma_valor_total:,.2f}")
print(f"   Soma da coluna Valor (apenas linhas com rateio): {soma_valor_com_rateio:,.2f}")
print(f"   Soma das colunas calculadas: {soma_calc_total:,.2f}")
print(f"   Diferença: {soma_valor_total - soma_calc_total:,.2f}")
print(f"   Percentual coberto: {(soma_calc_total / soma_valor_total * 100):.2f}%")

print("\n" + "="*70)


# %%
# Calcular a somatória de cada coluna (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)

print("="*60)
print("SOMATÓRIA DE CADA COLUNA")
print("="*60)

# Lista de colunas para somar
colunas_para_somar = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']

# Calcular e exibir a soma de cada coluna
soma_total = 0
for col in colunas_para_somar:
    if col in df_final.columns:
        # Converter para numérico e somar
        soma = pd.to_numeric(df_final[col], errors='coerce').fillna(0).sum()
        soma_total += soma
        print(f"Soma da coluna {col:12s}: {soma:,.2f}")
    else:
        print(f"Coluna {col:12s}: NÃO ENCONTRADA")

print("="*60)
print(f"SOMA TOTAL:                 {soma_total:,.2f}")
print("="*60)

#


# %%
# Apagar as colunas de percentual uma a uma
print("Removendo colunas de percentual...")
colunas_para_remover = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']

for col in colunas_para_remover:
    if col in df_final.columns:
        df_final = df_final.drop(columns=[col])
        print(f"  Coluna '{col}' removida")
    else:
        print(f"  AVISO: Coluna '{col}' não encontrada")

print(f"\nTotal de colunas após remoção: {len(df_final.columns)}")
print(f"Colunas restantes: {df_final.columns.tolist()}")

# filtrar na tabela df_final a USI = TC Ext
df_final = df_final[df_final['USI'] == 'TC Ext']

# mostrar o df_final_usi_tc_ext
display(df_final)




# %%
# Transformar as colunas CC21, CC22, CC24, CC24 5L, CC24 7L, J516 em linhas, mantendo todas as outras colunas
colunas_veiculos = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
colunas_veiculos_existentes = [col for col in colunas_veiculos if col in df_final.columns]

if len(colunas_veiculos_existentes) > 0:
    colunas_id = [col for col in df_final.columns if col not in colunas_veiculos]
    df_final = df_final.melt(id_vars=colunas_id, value_vars=colunas_veiculos_existentes, var_name='Veículo', value_name='Total')
else:
    print("AVISO: Nenhuma das colunas de veículos foi encontrada!")
    print(f"Colunas disponíveis: {df_final.columns.tolist()}")

# mostrar o df_final
display(df_final)

# somar a coluna Total
print(df_final['Total'].sum())




# %%
# ler o arquivo em excel KE5Z_veiculos.xls na guia volume e considerar a linha 51 como cabeçalho
# Ler o arquivo Excel 'KE5Z_veiculos.xls' na guia 'volume', considerando a linha 51 como cabeçalho (header=50, 0-indexed)
df_ke5z_volume = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Volume', header=50)

# exluir a coluna Unnamed: 14
df_ke5z_volume = df_ke5z_volume.drop(columns=['Unnamed: 14'])


# transformar as counas dos meses (de janeiro a fevereiro) em linhas
colunas_meses = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']

# Derreter o DataFrame para transformar as colunas em linhas
df_vol = pd.melt(
    df_ke5z_volume,
    id_vars=[col for col in df_ke5z_volume.columns if col not in colunas_meses],
    value_vars=colunas_meses,
    var_name='Período',
    value_name='Volume'
)
# Transformar a coluna Volume em numerico
df_vol['Volume'] = pd.to_numeric(df_vol['Volume'], errors='coerce').fillna(0)


# Remover linhas duplicadas
df_vol = df_vol.drop_duplicates()  


# Remover linhas com NaN
df_vol = df_vol.dropna()

# Exibir as primeiras linhas para conferência
display(df_vol)



# %%

# Fazer o merge do df_final com o df_vol utilizando as chaves Oficina, Veículo e Período, trazendo apenas a coluna Volume
# Remover todas as colunas Volume do df_final se já existirem (Volume, Volume_x, Volume_y, etc.)
colunas_volume = [col for col in df_final.columns if 'Volume' in str(col)]
if colunas_volume:
    df_final = df_final.drop(columns=colunas_volume)
    print(f"Colunas Volume removidas: {colunas_volume}")

# Criar um DataFrame temporário apenas com Volume do df_vol (sem outras colunas que possam duplicar)
df_vol_merge = df_vol[['Oficina', 'Veículo', 'Período', 'Volume']].copy()

# Fazer o merge usando apenas as chaves e a coluna Volume
df_final = pd.merge(
    df_final, 
    df_vol_merge,
    on=['Oficina', 'Veículo', 'Período'], 
    how='left'
)

# Verificar e remover qualquer coluna Volume duplicada que possa ter sido criada
colunas_volume_final = [col for col in df_final.columns if 'Volume' in str(col) and col != 'Volume']
if colunas_volume_final:
    df_final = df_final.drop(columns=colunas_volume_final)
    print(f"Colunas Volume duplicadas removidas: {colunas_volume_final}")

# Passar a coluna Volume para float
df_final['Volume'] = pd.to_numeric(df_final['Volume'], errors='coerce').fillna(0)



# Criar a coluna com o custo unitario 'CPU', evitando divisão por zero e retornando 0 onde Volume é zero ou nulo
df_final['CPU'] = df_final.apply(
    lambda row: row['Total'] / row['Volume'] if pd.notnull(row['Volume']) and row['Volume'] != 0 else 0, 
    axis=1
)

# Passar a coluna CPU para float
df_final['CPU'] = pd.to_numeric(df_final['CPU'], errors='coerce').fillna(0) 

# filtrar account diferente de NaN, 0 ou TC Ext
df_final = df_final[df_final['Account'].notna() & (df_final['Account'] != 0) & (df_final['Account'] != 'TC Ext')]

# Gerar excel com o df_final
df_final.to_excel('df_final_cpu.xlsx', index=False)

# Filtrar USI = TC Ext
df_final = df_final[df_final['USI'] == 'TC Ext']

# mostrar o df_final
display(df_final)



# somar a coluna CPU, coluna Valor e coluna Total, volume'
print('Volume = ', df_final['Volume'].sum())
print('CPU = ', df_final['CPU'].sum())
print('Valor = ', df_final['Valor'].sum())
print('Total = ', df_final['Total'].sum())

# Gerar arquivo parquet com o df_final
df_final.to_parquet('df_final.parquet')







# %%

# 3. Agrupar Volume
try:
    df_vol_group = (
        df_vol.groupby(['Oficina', 'Período'], as_index=False)['Volume']
        .sum()
    )
except Exception as e:
    print("ERRO ao agrupar df_vol:", e)
    raise

# Exibir as primeiras 100 linhas
display(df_vol_group.head(100))

# 4. Garantir que 'USI' está presente em df_KE5Z e filtrar corretamente
if 'USI' not in df_KE5Z.columns:
    raise ValueError("df_KE5Z não contém coluna 'USI'")
df_KE5Z = df_KE5Z[df_KE5Z['USI'] == 'TC Ext']

# 5. Garantir coluna 'Account' presente e aplicar filtros
if 'Account' not in df_KE5Z.columns:
    raise ValueError("df_KE5Z não contém coluna 'Account'")
df_KE5Z = df_KE5Z[df_KE5Z['Account'].notna() & (df_KE5Z['Account'] != 0) & (df_KE5Z['Account'] != '')]

# 6. Checar se as colunas 'Volume' e 'Total' (ou 'Valor') existem e são numéricas
# Volume
if 'Volume' not in df_KE5Z.columns:
    print("Coluna 'Volume' não encontrada em df_KE5Z, criando com zeros.")
    df_KE5Z['Volume'] = 0

df_KE5Z['Volume'] = pd.to_numeric(df_KE5Z['Volume'], errors='coerce').fillna(0)

# Total
if 'Total' not in df_KE5Z.columns:
    if 'Valor' in df_KE5Z.columns:
        df_KE5Z['Total'] = df_KE5Z['Valor']
        print("Coluna 'Total' criada a partir de 'Valor' em df_KE5Z.")
    else:
        print("Coluna 'Total' e 'Valor' não encontradas em df_KE5Z, criando 'Total' com zeros.")
        df_KE5Z['Total'] = 0

df_KE5Z['Total'] = pd.to_numeric(df_KE5Z['Total'], errors='coerce').fillna(0)

# Fazer o merge entre df_KE5Z e df_vol_group pela chave Oficina e Período
# Garante que só haverá uma coluna 'Volume' ao final
df_ke5z_group = pd.merge(
    df_KE5Z.drop(columns=[col for col in df_KE5Z.columns if col.lower() == 'volume']),  # remove 'Volume' antes!
    df_vol_group,
    on=['Oficina', 'Período'],
    how='left'
)

# Se após o merge ainda existir mais de uma coluna Volume, remove as extras mantendo apenas 'Volume'
colunas_volume = [col for col in df_ke5z_group.columns if 'Volume' in str(col) and col != 'Volume']
if colunas_volume:
    df_ke5z_group = df_ke5z_group.drop(columns=colunas_volume)
    print(f"Colunas Volume duplicadas removidas: {colunas_volume}")

# Mostrar o df_ke5z_group
display(df_ke5z_group)

# Somar coluna Total
print('Total = ', df_ke5z_group['Total'].sum()) 


# Gerar excel com o df_ke5z_group
df_ke5z_group.to_excel('df_ke5z_group.xlsx', index=False)
# gerar um arquivo parquet com o df_ke5z_group
df_ke5z_group.to_parquet('df_ke5z_group.parquet')








