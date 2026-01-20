"""
Script para reprocessar dados de BUDGET de 2026 após correção dos arquivos Excel
"""
import processamento_dados_BUD

print("=" * 80)
print("REPROCESSAMENTO DE DADOS BUDGET 2026")
print("=" * 80)

try:
    resultado = processamento_dados_BUD.processar_completo_bud(
        ano=2026,
        continuar_sem_arquivos=False,
        progress_callback=lambda msg: print(f"   {msg}")
    )
    
    print("\n" + "=" * 80)
    print("✅ PROCESSAMENTO BUDGET CONCLUÍDO COM SUCESSO!")
    print("=" * 80)
    print(f"   Ano processado: {resultado['ano']}")
    print(f"   df_final_BUD: {resultado['df_final_bud_linhas']} linhas")
    print(f"   df_vol_BUD: {resultado['df_vol_bud_linhas']} linhas")
    print(f"   df_ke5z_group_BUD: {resultado['df_ke5z_group_bud_linhas']} linhas")
    print("=" * 80)
    
except Exception as e:
    print("\n" + "=" * 80)
    print("❌ ERRO NO PROCESSAMENTO BUDGET")
    print("=" * 80)
    print(f"   {str(e)}")
    import traceback
    traceback.print_exc()
    print("=" * 80)
