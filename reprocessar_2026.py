"""
Script para reprocessar dados de 2026 após correção dos arquivos Excel
"""
import processamento_dados

print("=" * 80)
print("REPROCESSAMENTO DE DADOS 2026")
print("=" * 80)

try:
    resultado = processamento_dados.processar_completo(
        ano=2026,
        continuar_sem_arquivos=False,
        progress_callback=lambda msg: print(f"   {msg}")
    )
    
    print("\n" + "=" * 80)
    print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print("=" * 80)
    print(f"   Ano processado: {resultado['ano']}")
    print(f"   df_final: {resultado['df_final_linhas']} linhas")
    print(f"   df_vol: {resultado['df_vol_linhas']} linhas")
    print(f"   df_ke5z_group: {resultado['df_ke5z_group_linhas']} linhas")
    print("=" * 80)
    
except Exception as e:
    print("\n" + "=" * 80)
    print("❌ ERRO NO PROCESSAMENTO")
    print("=" * 80)
    print(f"   {str(e)}")
    print("=" * 80)
