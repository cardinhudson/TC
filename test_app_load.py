"""test_app_load.py

Smoke test rápido:
- Se existir o histórico do TC Ext, carrega e valida colunas duplicadas/"Unnamed".
- Se não existir (ambiente limpo/primeira execução), não falha.
"""
import pandas as pd
import os
import sys

# Adicionar funções do app.py
def limpar_colunas_duplicadas(df):
    """Remove colunas duplicadas (.1, .2, .3) e Unnamed:"""
    if df is None or df.empty:
        return df, []
    
    df = df.copy()
    colunas_para_manter = []
    colunas_ja_vistas = set()
    colunas_removidas = []
    
    for col in df.columns:
        col_str = str(col)
        
        # 1. Remover colunas Unnamed:
        if 'Unnamed:' in col_str or 'unnamed:' in col_str.lower():
            colunas_removidas.append(col_str)
            continue
        
        # 2. Verificar sufixo numérico (.1, .2, etc)
        if '.' in col_str:
            partes = col_str.rsplit('.', 1)
            if len(partes) == 2 and partes[1].isdigit():
                colunas_removidas.append(col_str)
                continue
        
        # 3. Verificar duplicação
        if col_str in colunas_ja_vistas:
            colunas_removidas.append(col_str)
            continue
        
        colunas_para_manter.append(col)
        colunas_ja_vistas.add(col_str)
    
    return df[colunas_para_manter].copy(), colunas_removidas

# Simular o que o app.py faz
print("🔄 Simulando carregamento como no app.py...")
print("=" * 80)

# Carregar dados (mesmo padrão do app: TC_Ext/historico_consolidado)
candidatos = [
    os.path.join('dados', 'TC_Ext', 'historico_consolidado', 'df_final_historico.parquet'),
    os.path.join('.', 'dados', 'TC_Ext', 'historico_consolidado', 'df_final_historico.parquet'),
]

caminho = next((p for p in candidatos if os.path.exists(p)), None)
if not caminho:
    print("\n⚠️ Histórico não encontrado. Nada a validar ainda.")
    print("   Gere os parquets rodando a extração (TC Ext) e reexecute este teste.")
    sys.exit(0)

print(f"\n📂 Carregando: {caminho}")
df = pd.read_parquet(caminho)
print(f"   Colunas antes da limpeza: {len(df.columns)}")
print(f"   Linhas: {len(df)}")

# Aplicar limpeza
df_limpo, colunas_removidas = limpar_colunas_duplicadas(df)
print(f"   Colunas depois da limpeza: {len(df_limpo.columns)}")

if len(colunas_removidas) > 0:
    print(f"\n   🔴 REMOVIDAS {len(colunas_removidas)} COLUNAS:")
    for col in colunas_removidas:
        print(f"      - {col}")
else:
    print(f"\n   ✅ Nenhuma coluna removida (dados já estão limpos)")

# Verificar coluna Período
if 'Período' in df_limpo.columns:
    periodos = df_limpo['Período'].unique()
    print(f"\n📅 Períodos únicos encontrados: {len(periodos)}")
    print(f"   {sorted(periodos)}")
    
    # Verificar se há períodos com sufixos
    periodos_com_sufixo = [p for p in periodos if '.' in str(p) and str(p).rsplit('.', 1)[-1].isdigit()]
    if periodos_com_sufixo:
        print(f"\n   🔴 ATENÇÃO: Encontrados {len(periodos_com_sufixo)} períodos COM SUFIXO:")
        for p in periodos_com_sufixo:
            print(f"      - {p}")
    else:
        print(f"   ✅ Nenhum período com sufixo numérico")

print("\n" + "=" * 80)
print("✅ Teste concluído!")
