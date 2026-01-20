"""
Script para remover colunas Unnamed: que causam duplicações
"""

import pandas as pd
import os
import glob


def limpar_colunas_unnamed(df):
    """Remove colunas Unnamed: vazias ou desnecessárias"""
    colunas_para_remover = [col for col in df.columns if 'Unnamed:' in str(col)]
    
    if colunas_para_remover:
        print(f"   → Removendo {len(colunas_para_remover)} colunas 'Unnamed:'")
        df = df.drop(columns=colunas_para_remover)
    
    return df, len(colunas_para_remover)


def processar_arquivo(caminho_arquivo):
    """Processa um arquivo parquet e remove colunas Unnamed:"""
    print(f"\n📄 Processando: {caminho_arquivo}")
    
    try:
        # Ler arquivo
        df = pd.read_parquet(caminho_arquivo)
        print(f"   Colunas originais: {len(df.columns)}")
        
        # Verificar se há colunas Unnamed:
        colunas_unnamed = [col for col in df.columns if 'Unnamed:' in str(col)]
        
        if not colunas_unnamed:
            print("   ✅ Nenhuma coluna 'Unnamed:' encontrada")
            return False
        
        print(f"   ⚠️ Encontradas {len(colunas_unnamed)} colunas 'Unnamed:'")
        print(f"   Primeiras 5: {colunas_unnamed[:5]}")
        
        # Criar backup
        backup_path = caminho_arquivo + '.bkp2'
        if not os.path.exists(backup_path):
            df.to_parquet(backup_path)
            print(f"   💾 Backup criado: {backup_path}")
        
        # Remover colunas Unnamed:
        df_limpo, num_removidas = limpar_colunas_unnamed(df)
        print(f"   Colunas após limpeza: {len(df_limpo.columns)}")
        print(f"   Colunas restantes: {list(df_limpo.columns)}")
        
        # Salvar arquivo corrigido
        df_limpo.to_parquet(caminho_arquivo)
        print(f"   ✅ Arquivo corrigido salvo ({num_removidas} colunas removidas)")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Erro ao processar arquivo: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Função principal"""
    print("=" * 70)
    print("🔧 REMOÇÃO DE COLUNAS UNNAMED: EM ARQUIVOS PARQUET")
    print("=" * 70)
    
    # Encontrar todos os arquivos parquet
    pasta_dados = 'dados'
    
    if not os.path.exists(pasta_dados):
        print(f"❌ Pasta '{pasta_dados}' não encontrada")
        return
    
    # Padrões de busca
    padroes = [
        os.path.join(pasta_dados, '*', '*.parquet'),
        os.path.join(pasta_dados, '*', 'BUD', '*.parquet'),
        os.path.join(pasta_dados, 'historico_consolidado', '*.parquet'),
        os.path.join(pasta_dados, 'historico_consolidado', 'BUD', '*.parquet'),
    ]
    
    arquivos = []
    for padrao in padroes:
        arquivos.extend(glob.glob(padrao))
    
    arquivos = list(set(arquivos))  # Remover duplicatas
    arquivos = [f for f in arquivos if not f.endswith('.backup') and not f.endswith('.bkp2')]  # Ignorar backups
    
    print(f"📁 Encontrados {len(arquivos)} arquivos parquet\n")
    
    # Processar cada arquivo
    arquivos_corrigidos = 0
    arquivos_sem_problema = 0
    
    for arquivo in sorted(arquivos):
        foi_corrigido = processar_arquivo(arquivo)
        if foi_corrigido:
            arquivos_corrigidos += 1
        else:
            arquivos_sem_problema += 1
    
    # Resumo
    print("\n" + "=" * 70)
    print("📊 RESUMO DA LIMPEZA")
    print("=" * 70)
    print(f"Total de arquivos processados: {len(arquivos)}")
    print(f"✅ Arquivos corrigidos: {arquivos_corrigidos}")
    print(f"ℹ️ Arquivos sem problema: {arquivos_sem_problema}")
    
    print("\n💡 IMPORTANTE:")
    print("   As colunas 'Unnamed:' geralmente vêm de colunas vazias no Excel.")
    print("   Verifique o arquivo Excel de origem para evitar futuras ocorrências.")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
