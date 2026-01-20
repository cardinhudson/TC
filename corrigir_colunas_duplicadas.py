"""
Script para corrigir colunas duplicadas em arquivos parquet existentes
Resolve problema de colunas como "Abril.1 2026" causadas por consolidações anteriores
"""

import pandas as pd
import os
import glob
from datetime import datetime


def remover_colunas_duplicadas(df):
    """Remove colunas duplicadas com sufixos .1, .2, etc"""
    colunas_originais = []
    colunas_para_remover = []
    colunas_mapeamento = {}
    
    for col in df.columns:
        # Verificar se é uma coluna duplicada (tem sufixo .1, .2, etc)
        if '.' in str(col):
            partes = str(col).split('.')
            if len(partes) >= 2 and partes[-1].isdigit():
                col_base = '.'.join(partes[:-1])  # Remover último elemento se for número
                if col_base not in colunas_originais:
                    # Se a coluna base não existe, esta deve ser renomeada
                    colunas_mapeamento[col] = col_base
                    colunas_originais.append(col_base)
                else:
                    # Se a coluna base já existe, esta é duplicada e deve ser removida
                    colunas_para_remover.append(col)
                continue
        
        # Coluna normal sem duplicação
        if col not in colunas_originais:
            colunas_originais.append(col)
    
    # Primeiro renomear colunas que devem ter sufixo removido
    if colunas_mapeamento:
        print(f"   → Renomeando {len(colunas_mapeamento)} colunas:")
        for antiga, nova in list(colunas_mapeamento.items())[:5]:
            print(f"      {antiga} → {nova}")
        if len(colunas_mapeamento) > 5:
            print(f"      ... e mais {len(colunas_mapeamento) - 5} colunas")
        df = df.rename(columns=colunas_mapeamento)
    
    # Depois remover duplicatas reais
    if colunas_para_remover:
        print(f"   → Removendo {len(colunas_para_remover)} colunas duplicadas:")
        for col in colunas_para_remover[:5]:
            print(f"      {col}")
        if len(colunas_para_remover) > 5:
            print(f"      ... e mais {len(colunas_para_remover) - 5} colunas")
        df = df.drop(columns=colunas_para_remover)
    
    return df, len(colunas_mapeamento) + len(colunas_para_remover)


def processar_arquivo(caminho_arquivo):
    """Processa um arquivo parquet e remove colunas duplicadas"""
    print(f"\n📄 Processando: {caminho_arquivo}")
    
    try:
        # Ler arquivo
        df = pd.read_parquet(caminho_arquivo)
        print(f"   Colunas originais: {len(df.columns)}")
        
        # Verificar se há colunas duplicadas
        colunas_com_sufixo = [col for col in df.columns if '.' in str(col) and str(col).split('.')[-1].isdigit()]
        
        if not colunas_com_sufixo:
            print("   ✅ Nenhuma coluna duplicada encontrada")
            return False
        
        print(f"   ⚠️ Encontradas {len(colunas_com_sufixo)} colunas com sufixo numérico")
        print(f"   Exemplos: {colunas_com_sufixo[:3]}")
        
        # Criar backup
        backup_path = caminho_arquivo + '.backup'
        if not os.path.exists(backup_path):
            df.to_parquet(backup_path)
            print(f"   💾 Backup criado: {backup_path}")
        
        # Remover duplicadas
        df_limpo, num_corrigidas = remover_colunas_duplicadas(df)
        print(f"   Colunas após limpeza: {len(df_limpo.columns)}")
        
        # Salvar arquivo corrigido
        df_limpo.to_parquet(caminho_arquivo)
        print(f"   ✅ Arquivo corrigido salvo ({num_corrigidas} colunas corrigidas)")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Erro ao processar arquivo: {e}")
        return False


def main():
    """Função principal"""
    print("=" * 70)
    print("🔧 CORREÇÃO DE COLUNAS DUPLICADAS EM ARQUIVOS PARQUET")
    print("=" * 70)
    print(f"\nData/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
    
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
    arquivos = [f for f in arquivos if not f.endswith('.backup')]  # Ignorar backups
    
    print(f"📁 Encontrados {len(arquivos)} arquivos parquet\n")
    
    if not arquivos:
        print("❌ Nenhum arquivo parquet encontrado")
        return
    
    # Processar cada arquivo
    arquivos_corrigidos = 0
    arquivos_sem_problema = 0
    arquivos_com_erro = 0
    
    for arquivo in sorted(arquivos):
        foi_corrigido = processar_arquivo(arquivo)
        if foi_corrigido:
            arquivos_corrigidos += 1
        elif foi_corrigido is False:
            arquivos_sem_problema += 1
        else:
            arquivos_com_erro += 1
    
    # Resumo
    print("\n" + "=" * 70)
    print("📊 RESUMO DA CORREÇÃO")
    print("=" * 70)
    print(f"Total de arquivos processados: {len(arquivos)}")
    print(f"✅ Arquivos corrigidos: {arquivos_corrigidos}")
    print(f"ℹ️ Arquivos sem problema: {arquivos_sem_problema}")
    if arquivos_com_erro > 0:
        print(f"❌ Arquivos com erro: {arquivos_com_erro}")
    
    if arquivos_corrigidos > 0:
        print("\n💡 PRÓXIMOS PASSOS:")
        print("   1. Execute novamente o processamento de dados (dados.ipynb ou dados_BUD.ipynb)")
        print("   2. Isso irá recriar o histórico consolidado sem colunas duplicadas")
        print("   3. Recarregue a aplicação Streamlit para ver as correções")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
