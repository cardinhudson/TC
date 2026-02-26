"""
Script para corrigir colunas duplicadas (.1, .2, etc) nos arquivos Excel fonte
Remove as colunas com sufixos numéricos e salva os arquivos corrigidos
"""
import pandas as pd
import os
from openpyxl import load_workbook

def limpar_colunas_duplicadas(df):
    """Remove colunas com sufixos .1, .2, .3, etc"""
    if df is None or df.empty:
        return df
    
    df = df.copy()
    colunas_para_manter = []
    colunas_ja_vistas = set()
    colunas_removidas = []
    
    for col in df.columns:
        col_str = str(col)
        
        # Remover colunas Unnamed:
        if 'Unnamed:' in col_str or 'unnamed:' in col_str.lower():
            colunas_removidas.append(col_str)
            continue
        
        # Verificar se é coluna duplicada com sufixo numérico (.1, .2, etc)
        if '.' in col_str:
            partes = col_str.rsplit('.', 1)
            if len(partes) == 2 and partes[1].isdigit():
                colunas_removidas.append(col_str)
                continue
        
        # Verificar se já vimos esta coluna
        if col_str in colunas_ja_vistas:
            colunas_removidas.append(col_str)
            continue
        
        colunas_para_manter.append(col)
        colunas_ja_vistas.add(col_str)
    
    df_limpo = df[colunas_para_manter].copy()
    
    if len(colunas_removidas) > 0:
        return df_limpo, colunas_removidas
    
    return df_limpo, []

def corrigir_arquivo_excel(caminho_arquivo):
    """Corrige todas as abas de um arquivo Excel"""
    if not os.path.exists(caminho_arquivo):
        print(f"⚠️  Arquivo não encontrado: {caminho_arquivo}")
        return False
    
    print(f"\n{'='*80}")
    print(f"📂 Processando: {caminho_arquivo}")
    print(f"{'='*80}")
    
    try:
        # Criar backup
        backup_path = caminho_arquivo.replace('.xlsx', '_BACKUP.xlsx')
        if not os.path.exists(backup_path):
            import shutil
            shutil.copy2(caminho_arquivo, backup_path)
            print(f"✅ Backup criado: {backup_path}")
        
        # Ler todas as abas
        xls = pd.ExcelFile(caminho_arquivo)
        abas_corrigidas = []
        total_colunas_removidas = 0
        
        # Criar um dicionário com DataFrames corrigidos
        abas_limpas = {}
        
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(caminho_arquivo, sheet_name=sheet_name)
            resultado = limpar_colunas_duplicadas(df)
            
            # Verificar se resultado é uma tupla (df_limpo, colunas_removidas) ou apenas df
            if isinstance(resultado, tuple):
                df_limpo, colunas_removidas = resultado
            else:
                df_limpo = resultado
                colunas_removidas = []
            
            if colunas_removidas:
                print(f"\n   📊 Aba: {sheet_name}")
                print(f"      ⚠️  {len(colunas_removidas)} colunas duplicadas removidas:")
                for col in colunas_removidas[:5]:  # Mostrar apenas as primeiras 5
                    print(f"         - {col}")
                if len(colunas_removidas) > 5:
                    print(f"         ... e mais {len(colunas_removidas) - 5} colunas")
                abas_corrigidas.append(sheet_name)
                total_colunas_removidas += len(colunas_removidas)
            
            abas_limpas[sheet_name] = df_limpo
        
        # Salvar apenas se houver correções
        if abas_corrigidas:
            print(f"\n   💾 Salvando arquivo corrigido...")
            with pd.ExcelWriter(caminho_arquivo, engine='openpyxl', mode='w') as writer:
                for sheet_name, df_limpo in abas_limpas.items():
                    df_limpo.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"   ✅ Arquivo corrigido: {len(abas_corrigidas)} abas, {total_colunas_removidas} colunas removidas")
            return True
        else:
            print(f"   ✅ Nenhuma coluna duplicada encontrada")
            return False
    
    except Exception as e:
        print(f"   ❌ Erro ao processar arquivo: {str(e)}")
        return False

# Processar arquivos
print("=" * 80)
print("CORREÇÃO DE COLUNAS DUPLICADAS EM ARQUIVOS EXCEL")
print("=" * 80)

arquivos_para_corrigir = [
    'dados/TC_Ext/2025/Reporting fluxo anexo.xlsx',
    'dados/TC_Ext/2026/Reporting fluxo anexo.xlsx',
]

arquivos_corrigidos = 0

for arquivo in arquivos_para_corrigir:
    if corrigir_arquivo_excel(arquivo):
        arquivos_corrigidos += 1

print("\n" + "=" * 80)
print(f"RESUMO: {arquivos_corrigidos} arquivos corrigidos")
print("=" * 80)
print("\n⚠️  IMPORTANTE: Após corrigir os arquivos Excel, você precisa:")
print("   1. Reprocessar os dados executando o notebook tc_ext/notebooks/dados.ipynb")
print("   2. Reiniciar o Streamlit para carregar os novos dados")
print("=" * 80)
