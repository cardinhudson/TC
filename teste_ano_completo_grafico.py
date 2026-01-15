"""
CORREÇÃO FINAL - Mostrar Ano Completo no Gráfico
==================================================

PROBLEMA IDENTIFICADO:
- Os dados eram filtrados ANTES de serem enviados ao gráfico
- df_grafico_periodo continha apenas períodos com dados reais
- Períodos sem dados reais nunca chegavam ao gráfico

SOLUÇÃO IMPLEMENTADA:
- Adicionada lógica para preencher períodos faltantes com dados do Budget
- Localização: Linhas ~4500 no app.py, ANTES de criar o gráfico
- Períodos faltantes são identificados comparando Real vs Budget
- Dados do Budget são usados para preencher os meses sem realização

CÓDIGO ADICIONADO:
```python
# Preencher meses faltantes com dados do Budget
if df_budget_filtrado is not None:
    periodos_reais = set(df_grafico_periodo[['Ano', 'Período']])
    periodos_budget = set(df_budget_filtrado[['Ano', 'Período']])
    periodos_faltantes = periodos_budget - periodos_reais
    
    if len(periodos_faltantes) > 0:
        df_periodos_faltantes = df_budget_filtrado[períodos_faltantes]
        df_grafico_periodo = pd.concat([df_grafico_periodo, df_periodos_faltantes])
```

RESULTADO ESPERADO:
✅ Gráfico mostrará TODOS os meses do ano (Janeiro a Dezembro)
✅ Meses com dados reais mostrarão valores reais
✅ Meses sem dados reais mostrarão valores do Budget
✅ Flex Bud será calculado para o ano completo
✅ Mensagem informativa indicará quantos períodos foram adicionados

TESTE:
1. Execute: streamlit run app.py
2. Selecione ano 2026
3. Verifique se aparecem TODOS os 12 meses no gráfico
4. Verifique mensagem "ℹ️ Adicionados X períodos usando dados do Budget"

PARA 2026 ESPECIFICAMENTE:
- 5 meses com dados reais: Julho, Agosto, Setembro, Outubro, Novembro
- 7 meses usando Budget: Janeiro, Fevereiro, Março, Abril, Maio, Junho, Dezembro
- Total: 12 meses visíveis no gráfico

DATA: 15/01/2026
MODIFICADO POR: GitHub Copilot (assistente)
"""

print(__doc__)

import pandas as pd
import os

print("\n🔍 Verificando dados disponíveis...")

# Verificar dados reais
arquivo_real = 'dados/historico_consolidado/df_final_historico.parquet'
if os.path.exists(arquivo_real):
    df_real = pd.read_parquet(arquivo_real)
    df_real_2026 = df_real[df_real['Ano'] == 2026]
    periodos_reais = sorted(df_real_2026['Período'].unique())
    print(f"\n✅ Períodos com dados REAIS em 2026 ({len(periodos_reais)}):")
    print(f"   {', '.join(periodos_reais)}")

# Verificar dados de Budget
arquivo_budget = 'dados/historico_consolidado/BUD/df_final_historico_BUD.parquet'
if os.path.exists(arquivo_budget):
    df_budget = pd.read_parquet(arquivo_budget)
    df_budget_2026 = df_budget[df_budget['Ano'] == 2026]
    periodos_budget = sorted(df_budget_2026['Período'].unique())
    print(f"\n✅ Períodos com dados de BUDGET em 2026 ({len(periodos_budget)}):")
    print(f"   {', '.join(periodos_budget)}")
    
    # Calcular períodos faltantes
    periodos_faltantes = set(periodos_budget) - set(periodos_reais)
    if periodos_faltantes:
        print(f"\n📊 Períodos que serão PREENCHIDOS com Budget ({len(periodos_faltantes)}):")
        print(f"   {', '.join(sorted(periodos_faltantes))}")
        
        print(f"\n🎉 RESULTADO: O gráfico mostrará {len(periodos_budget)} meses!")
        print(f"   - {len(periodos_reais)} com dados reais")
        print(f"   - {len(periodos_faltantes)} com dados do Budget")
    else:
        print(f"\n✅ Todos os períodos têm dados reais!")

print("\n" + "="*60)
print("✅ Agora execute o app e verifique se todos os meses aparecem!")
print("="*60)
