"""
Script para limpar colunas duplicadas de TODOS os arquivos parquet existentes
"""

import pandas as pd
import os

def limpar_colunas_duplicadas(df):
    """Remove colunas duplicadas (.1, .2, .3) e Unnamed:"""
    if df is None or df.empty:
        return df
    
    df = df.copy()
    colunas_para_manter = []
    colunas_ja_vistas = set()
    colunas_removidas = []
    
    for col in df.columns:
        col_str = str(col)
        
        # 1. Remover colunas Unnamed:
        if 'Unnamed:' in col_str or 'unnamed' in col_str.lower():
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


print("🔄 Limpando todos os arquivos parquet de colunas duplicadas...")
print("=" * 80)

# Lista de todos os arquivos parquet a processar
arquivos = []

# Arquivos por ano (2024, 2025, 2026)
for ano in ['2024', '2025', '2026']:
    pasta_ano = os.path.join('dados', ano)
    if os.path.exists(pasta_ano):
        for arquivo in ['df_final.parquet', 'df_vol.parquet', 'df_ke5z_group.parquet']:
            caminho = os.path.join(pasta_ano, arquivo)
            if os.path.exists(caminho):
                arquivos.append(caminho)

# Arquivos consolidados
pasta_consolidado = os.path.join('dados', 'historico_consolidado')
if os.path.exists(pasta_consolidado):
    for arquivo in ['df_final_historico.parquet', 'df_vol_historico.parquet', 'df_ke5z_group_historico.parquet']:
        caminho = os.path.join(pasta_consolidado, arquivo)
        if os.path.exists(caminho):
            arquivos.append(caminho)

# Processar cada arquivo
total_arquivos = len(arquivos)
arquivos_modificados = 0
total_colunas_removidas = 0

for i, caminho in enumerate(arquivos, 1):
    print(f"\n[{i}/{total_arquivos}] 📄 {caminho}")
    
    try:
        # Carregar
        df = pd.read_parquet(caminho)
        colunas_antes = len(df.columns)
        
        # Limpar
        df_limpo, colunas_removidas = limpar_colunas_duplicadas(df)
        colunas_depois = len(df_limpo.columns)
        
        if len(colunas_removidas) > 0:
            # Salvar
            df_limpo.to_parquet(caminho)
            print(f"   ✅ MODIFICADO: {colunas_antes} → {colunas_depois} colunas")
            print(f"   🗑️  Removidas ({len(colunas_removidas)}): {colunas_removidas}")
            arquivos_modificados += 1
            total_colunas_removidas += len(colunas_removidas)
        else:
            print(f"   ✓ OK: {colunas_antes} colunas (sem duplicatas)")
            
    except Exception as e:
        print(f"   ❌ ERRO: {e}")

print("\n" + "=" * 80)
print(f"📊 RESUMO:")
print(f"   • Total de arquivos processados: {total_arquivos}")
print(f"   • Arquivos modificados: {arquivos_modificados}")
print(f"   • Total de colunas removidas: {total_colunas_removidas}")
print("✅ Limpeza concluída!")
