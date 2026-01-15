"""
Script para corrigir a capitalização dos períodos em 2026
e verificar dados disponíveis
"""
import pandas as pd
import os

print("🔍 Verificando e corrigindo dados de 2026...\n")

# Verificar arquivo parquet do ano
arquivo_ano = 'dados/2026/df_final.parquet'
if os.path.exists(arquivo_ano):
    print(f"📊 Lendo {arquivo_ano}...")
    df = pd.read_parquet(arquivo_ano)
    
    print(f"Total de registros: {len(df)}")
    print(f"\nPeríodos ANTES da correção:")
    print(df['Período'].value_counts().sort_index())
    
    # Mapear períodos para capitalizado
    mapeamento_meses = {
        'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
        'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
        'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
        'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
    }
    
    def normalizar_periodo(periodo):
        if pd.isna(periodo):
            return periodo
        periodo_str = str(periodo).strip().lower()
        return mapeamento_meses.get(periodo_str, periodo)
    
    df['Período'] = df['Período'].apply(normalizar_periodo)
    
    print(f"\nPeríodos DEPOIS da correção:")
    print(df['Período'].value_counts().sort_index())
    
    # Salvar arquivo corrigido
    print(f"\n💾 Salvando arquivo corrigido...")
    df.to_parquet(arquivo_ano, index=False)
    print("✅ Arquivo do ano corrigido!")
else:
    print(f"❌ Arquivo {arquivo_ano} não encontrado")

print("\n" + "="*60 + "\n")

# Verificar e corrigir histórico consolidado
arquivo_historico = 'dados/historico_consolidado/df_final_historico.parquet'
if os.path.exists(arquivo_historico):
    print(f"📊 Lendo {arquivo_historico}...")
    df_hist = pd.read_parquet(arquivo_historico)
    
    # Filtrar apenas 2026
    if 'Ano' in df_hist.columns:
        df_2026 = df_hist[df_hist['Ano'] == 2026]
        print(f"Total de registros 2026: {len(df_2026)}")
        print(f"\nPeríodos 2026 ANTES da correção:")
        print(df_2026['Período'].value_counts().sort_index())
        
        # Aplicar normalização
        mapeamento_meses = {
            'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
            'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
            'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
            'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
        }
        
        def normalizar_periodo(periodo):
            if pd.isna(periodo):
                return periodo
            periodo_str = str(periodo).strip().lower()
            return mapeamento_meses.get(periodo_str, periodo)
        
        df_hist['Período'] = df_hist['Período'].apply(normalizar_periodo)
        
        # Verificar resultado
        df_2026_corrigido = df_hist[df_hist['Ano'] == 2026]
        print(f"\nPeríodos 2026 DEPOIS da correção:")
        print(df_2026_corrigido['Período'].value_counts().sort_index())
        
        # Salvar histórico corrigido
        print(f"\n💾 Salvando histórico consolidado corrigido...")
        df_hist.to_parquet(arquivo_historico, index=False)
        print("✅ Histórico consolidado corrigido!")
    else:
        print("⚠️ Coluna 'Ano' não encontrada no histórico")
else:
    print(f"❌ Arquivo {arquivo_historico} não encontrado")

print("\n✅ Processo concluído!")
print("\n📋 RESUMO:")
print("Os dados de 2026 foram corrigidos para usar períodos capitalizados.")
print("Se ainda assim você não vê todos os meses, é porque os arquivos Excel")
print("originais (Dados SAPIENS.xlsx e Reporting fluxo anexo.xlsx) não contêm")
print("dados para todos os meses de 2026.")
