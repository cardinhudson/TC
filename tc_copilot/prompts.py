"""
TC Copilot — Templates de prompts para a LLM.

Cada prompt é bilíngue (pt-BR / en), orientado para análise de custos
industriais automotivos, com foco em clareza e contexto executivo.
"""

# ═══════════════════════════════════════════════════════════════
#  SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════

SYSTEM_PROMPTS = {
    "pt-BR": (
        "Você é um analista sênior de custos industriais automotivos na Stellantis. "
        "Sua função é redigir textos analíticos claros, objetivos e executivos para "
        "relatórios mensais de custos de produção de veículos. "
        "Valores monetários estão em kBRL (milhares de BRL). "
        "Convenção de sinais para comparações com Budget/Flex: Δ negativo = ganho (economia), Δ positivo = perda (gasto acima do previsto). "
        "Para comparações mês a mês: Δ negativo = redução, Δ positivo = aumento. "
        "NUNCA traduza nomes de colunas, categorias ou contas dos dados — use SEMPRE "
        "os nomes originais tal como aparecem nos dados (ex: 'Labor', 'Burden', 'D&A', "
        "'Benefits', 'Direct Labor', 'Energy', 'Gas', 'Main/Principal'). "
        "Vá direto ao ponto, sem cabeçalhos como 'No detalhamento por Type 05'. "
        "Apresente valores absolutos (kBRL) E percentuais para cada comparação. "
        "Use linguagem profissional e direta, sem jargões desnecessários. "
        "Mantenha cada seção separada, sem misturar análises de seções diferentes. "
        "Não invente dados — use SOMENTE os dados fornecidos. "
        "IMPORTANTE: Use símbolos visuais para destacar informações-chave: "
        "📈 para crescimento/aumento, 📉 para queda/redução, "
        "⚠️ para alertas/riscos, ✅ para resultados positivos/economia, "
        "❌ para desvios negativos, 💡 para insights/recomendações, "
        "🏭 para oficinas, 📊 para dados/métricas, "
        "🟢 para ganhos e 🔴 para perdas. "
        "Quando um valor de referência for zero ou não existir, diga explicitamente "
        "(ex: '⚠️ Budget não disponível para esta conta') em vez de calcular delta enganoso."
    ),
    "en": (
        "You are a senior automotive industrial cost analyst at Stellantis. "
        "Your role is to write clear, objective, and executive analytical texts for "
        "monthly vehicle production cost reports. "
        "Monetary values are in kBRL (thousands of BRL). "
        "Sign convention for Budget/Flex comparisons: negative Δ = saving (gain), positive Δ = overspend (loss). "
        "For month-over-month comparisons: negative Δ = reduction, positive Δ = increase. "
        "NEVER translate column names, categories or account names from the data — always use "
        "the original names as they appear (e.g., 'Labor', 'Burden', 'D&A', "
        "'Benefits', 'Direct Labor', 'Energy', 'Gas', 'Main/Principal'). "
        "Go straight to the point, no headers like 'In Type 05 detail'. "
        "Always present absolute values (kBRL) AND percentages for each comparison. "
        "Use professional and direct language without unnecessary jargon. "
        "Keep each section separate, without mixing analyses from different sections. "
        "Do not invent data — use ONLY the provided data. "
        "IMPORTANT: Use visual symbols to highlight key information: "
        "📈 for growth/increase, 📉 for decline/reduction, "
        "⚠️ for alerts/risks, ✅ for positive results/savings, "
        "❌ for negative deviations, 💡 for insights/recommendations, "
        "🏭 for shops, 📊 for data/metrics, "
        "🟢 for gains and 🔴 for losses. "
        "When a reference value is zero or does not exist, state this explicitly "
        "(e.g., '⚠️ Budget not available for this account') instead of computing misleading deltas."
    ),
}


# ═══════════════════════════════════════════════════════════════
#  PROMPTS POR SEÇÃO
# ═══════════════════════════════════════════════════════════════

PROMPTS = {
    "analise_volume": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva uma análise de volume de produção "
            "para o mês de {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Informe o volume total do mês em unidades.\n"
            "- Compare com o mês anterior: variação absoluta (unidades) e percentual.\n"
            "- Compare com o Flex Budget: variação absoluta e percentual.\n"
            "- Compare com o Budget: variação absoluta e percentual.\n"
            "- Compare com o mesmo mês do ano anterior: variação absoluta e percentual.\n"
            "- Destaque os modelos de veículo com maior crescimento e maior redução.\n"
            "- Use frases como: 'O volume total em {mes} de {ano} foi de X unidades, "
            "um aumento/redução de Y% (+/-Z unidades) em relação a...'\n"
            "- Limite-se a 2-3 parágrafos."
        ),
        "en": (
            "Based on the data below, write a production volume analysis "
            "for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- State the total volume for the month in units.\n"
            "- Compare with previous month: absolute (units) and percentage variation.\n"
            "- Compare with Flex Budget: absolute and percentage variation.\n"
            "- Compare with Budget: absolute and percentage variation.\n"
            "- Compare with same month previous year: absolute and percentage variation.\n"
            "- Highlight vehicle models with highest growth and highest decline.\n"
            "- Limit to 2-3 paragraphs."
        ),
    },

    "variacoes_modelo": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva sobre as maiores variações "
            "por modelo de veículo em {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Liste os modelos com maiores variações de volume e resultado.\n"
            "- Contextualize possíveis causas e impactos.\n"
            "- Destaque os destaques positivos e negativos do mês.\n"
            "- Apresente valores absolutos e percentuais.\n"
            "- Limite-se a 2-3 parágrafos."
        ),
        "en": (
            "Based on the data below, write about the biggest variations "
            "by vehicle model in {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- List models with largest volume and cost variations.\n"
            "- Contextualize possible causes and impacts.\n"
            "- Highlight positive and negative highlights.\n"
            "- Present absolute values and percentages.\n"
            "- Limit to 2-3 paragraphs."
        ),
    },

    "real_vs_mes_anterior": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva a análise comparativa "
            "Real vs Mês Anterior para {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Comece com o Custo FP total e a variação vs mês anterior.\n"
            "- Vá direto ao ponto, sem cabeçalhos intermediários como 'No detalhamento por Type 05'.\n"
            "- Para cada Type 05 (Labor, Burden, D&A), informe a variação total "
            "e liste os Type 06 com maiores impactos (ganhos e perdas).\n"
            "- Para cada Type 06 mencionado, cite os 2-3 Accounts que mais contribuíram para a variação.\n"
            "- NUNCA traduza nomes de colunas ou valores dos dados: use sempre os nomes originais "
            "(ex: 'Labor', 'Burden', 'D&A', 'Benefits', 'Direct Labor', 'Gas', "
            "'Main/Principal', 'Consumption Material', 'Third Part Services / Uma', etc.).\n"
            "- Valores em kBRL (milhares de BRL). Ex: '448,7 kBRL', não traduza a unidade.\n"
            "- Exemplo de narrativa: 'Labor ficou acima do mês anterior em 449 kBRL (+5,3%), "
            "puxado por Benefits (+2.500 kBRL) — onde Restaurant-BC (+2.019 kBRL) e "
            "Health-BC (+1.636 kBRL) foram os maiores impactos — parcialmente compensado "
            "por Direct Labor (-3.665 kBRL), com queda na Account Main/Principal.'\n"
            "- Inclua brevemente a análise de CPU por modelo ao final.\n"
            "- Limite-se a 3-4 parágrafos, em tom executivo."
        ),
        "en": (
            "Based on the data below, write the comparative analysis "
            "Actual vs Previous Month for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Start with total FP Cost and variation vs previous month.\n"
            "- Go straight to the point, no intermediate headers like 'In Type 05 detail'.\n"
            "- For each Type 05 (Labor, Burden, D&A), state total variation "
            "and list the Type 06 items with largest impacts (gains and losses).\n"
            "- For each Type 06 mentioned, cite the 2-3 Accounts that contributed most.\n"
            "- NEVER translate column or value names: always use the original names "
            "(e.g., 'Labor', 'Burden', 'D&A', 'Benefits', 'Direct Labor', 'Gas', "
            "'Main/Principal', 'Consumption Material', 'Third Part Services / Uma', etc.).\n"
            "- Values in kBRL (thousands of BRL). E.g., '448.7 kBRL'.\n"
            "- Briefly include CPU analysis by model at the end.\n"
            "- Limit to 3-4 paragraphs, executive tone."
        ),
    },

    "real_vs_flex": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva a análise comparativa "
            "Real vs Flex Budget para {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- O Flex Budget já ajusta pelo volume real, portanto a diferença reflete "
            "eficiência operacional. Mencione isso brevemente e vá direto à análise.\n"
            "- Vá direto ao ponto, sem cabeçalhos intermediários como 'No detalhamento por Type 05'.\n"
            "- Para cada Type 05 (Labor, Burden, D&A), informe a variação total "
            "e liste os Type 06 com maiores impactos (ganhos e perdas).\n"
            "- Para cada Type 06 mencionado, cite os 2-3 Accounts que mais contribuíram para a variação.\n"
            "- NUNCA traduza nomes de colunas ou valores dos dados: use sempre os nomes originais "
            "(ex: 'Labor', 'Burden', 'D&A', 'Benefits', 'Direct Labor', 'Energy', "
            "'Gas', 'Main/Principal', 'Consumption Material', 'Third Part Services / Uma', etc.).\n"
            "- Valores em kBRL (milhares de BRL). Ex: '568,4 kBRL', não traduza a unidade.\n"
            "- Exemplo de narrativa: 'Burden ficou 568 kBRL acima do Flex (+18,0%), "
            "com destaque para Energy (+150 kBRL) — puxada por Gas (+692,5 kBRL) — "
            "e Expenses (+1.046 kBRL) via Third Part Services / Uma (+1.107 kBRL). "
            "Material Losses apresentou ganho de -380 kBRL, atenuando o impacto.'\n"
            "- Inclua brevemente a análise de CPU por modelo ao final.\n"
            "- Limite-se a 3-4 parágrafos, em tom executivo."
        ),
        "en": (
            "Based on the data below, write the comparative analysis "
            "Actual vs Flex Budget for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Flex Budget adjusts for actual volume, so the difference reflects "
            "operational efficiency. Mention briefly and go straight to analysis.\n"
            "- Go straight to the point, no intermediate headers like 'In Type 05 detail'.\n"
            "- For each Type 05 (Labor, Burden, D&A), state total variation "
            "and list the Type 06 items with largest impacts (gains and losses).\n"
            "- For each Type 06 mentioned, cite the 2-3 Accounts that contributed most.\n"
            "- NEVER translate column or value names: always use the original names "
            "(e.g., 'Labor', 'Burden', 'D&A', 'Benefits', 'Direct Labor', 'Energy', "
            "'Gas', 'Main/Principal', 'Consumption Material', 'Third Part Services / Uma', etc.).\n"
            "- Values in kBRL (thousands of BRL). E.g., '568.4 kBRL'.\n"
            "- Briefly include CPU analysis by model at the end.\n"
            "- Limit to 3-4 paragraphs, executive tone."
        ),
    },

    "real_vs_budget": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva a análise comparativa "
            "Real vs Budget para {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Comece com o Custo FP total e a variação vs Budget.\n"
            "- Vá direto ao ponto, sem cabeçalhos intermediários.\n"
            "- Para cada Type 05 (Labor, Burden, D&A), informe a variação total "
            "e liste os Type 06 com maiores impactos (ganhos e perdas).\n"
            "- Para cada Type 06, cite os 2-3 Accounts que mais contribuíram.\n"
            "- NUNCA traduza nomes de colunas ou valores dos dados.\n"
            "- Valores em kBRL. Convenção: Δ negativo = ganho (economia), Δ positivo = perda.\n"
            "- Inclua brevemente a análise de CPU por modelo ao final.\n"
            "- Limite-se a 3-4 parágrafos, em tom executivo."
        ),
        "en": (
            "Based on the data below, write the comparative analysis "
            "Actual vs Budget for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Start with total FP Cost and variation vs Budget.\n"
            "- Go straight to the point, no intermediate headers.\n"
            "- For each Type 05 (Labor, Burden, D&A), state total variation "
            "and list the Type 06 items with largest impacts.\n"
            "- For each Type 06, cite the 2-3 Accounts that contributed most.\n"
            "- NEVER translate column or value names.\n"
            "- Values in kBRL. Convention: negative Δ = saving, positive Δ = overspend.\n"
            "- Briefly include CPU analysis by model at the end.\n"
            "- Limit to 3-4 paragraphs, executive tone."
        ),
    },

    "real_vs_ano_anterior": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva a análise comparativa "
            "Real vs Mesmo Mês do Ano Anterior para {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Compare o resultado Real do mês atual com o mesmo mês de {ano_anterior}.\n"
            "- Vá direto ao ponto, sem cabeçalhos intermediários.\n"
            "- Para cada Type 05 (Labor, Burden, D&A), informe a variação total "
            "e liste os Type 06 com maiores impactos.\n"
            "- Para cada Type 06, cite os 2-3 Accounts que mais contribuíram.\n"
            "- NUNCA traduza nomes de colunas ou valores dos dados.\n"
            "- Valores em kBRL. Convenção: Δ negativo = redução, Δ positivo = aumento.\n"
            "- Inclua brevemente a análise de CPU por modelo ao final.\n"
            "- Limite-se a 3-4 parágrafos, em tom executivo."
        ),
        "en": (
            "Based on the data below, write the comparative analysis "
            "Actual vs Same Month Previous Year for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Compare current month actual with same month of {ano_anterior}.\n"
            "- Go straight to the point, no intermediate headers.\n"
            "- For each Type 05 (Labor, Burden, D&A), state total variation "
            "and list the Type 06 items with largest impacts.\n"
            "- For each Type 06, cite the 2-3 Accounts that contributed most.\n"
            "- NEVER translate column or value names.\n"
            "- Values in kBRL. Convention: negative Δ = reduction, positive Δ = increase.\n"
            "- Briefly include CPU analysis by model at the end.\n"
            "- Limit to 3-4 paragraphs, executive tone."
        ),
    },

    "anomalias": {
        "pt-BR": (
            "Com base nos dados consolidados abaixo do mês de {mes}/{ano}, "
            "liste os principais alertas, anomalias e destaques.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Liste os principais alertas e anomalias detectadas.\n"
            "- Identifique tendências preocupantes ou positivas.\n"
            "- Sugira recomendações de ação.\n"
            "- Use formato de lista com bullet points.\n"
            "- Limite-se a 5-8 itens."
        ),
        "en": (
            "Based on the consolidated data below for {mes}/{ano}, "
            "list the main alerts, anomalies and highlights.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- List main alerts and detected anomalies.\n"
            "- Identify concerning or positive trends.\n"
            "- Suggest action recommendations.\n"
            "- Use bullet point format.\n"
            "- Limit to 5-8 items."
        ),
    },

    "observacoes_finais": {
        "pt-BR": (
            "Com base nos dados e análises do mês de {mes}/{ano} abaixo, "
            "escreva um texto executivo de fechamento.\n\n"
            "RESUMO DO MÊS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Resuma os principais aprendizados do mês.\n"
            "- Destaque os pontos de atenção para o próximo mês.\n"
            "- Inclua recomendações estratégicas.\n"
            "- Tom profissional e executivo.\n"
            "- Limite-se a 2-3 parágrafos."
        ),
        "en": (
            "Based on the data and analyses for {mes}/{ano} below, "
            "write an executive closing text.\n\n"
            "MONTH SUMMARY:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Summarize main learnings of the month.\n"
            "- Highlight attention points for next month.\n"
            "- Include strategic recommendations.\n"
            "- Professional and executive tone.\n"
            "- Limit to 2-3 paragraphs."
        ),
    },

    "comparativos": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva a análise dos 4 comparativos "
            "para {mes}/{ano}. Os dados já estão organizados em sub-seções 3.1 a 3.4.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Mantenha a estrutura de 4 sub-seções: 3.1 Real vs Flex Budget, "
            "3.2 Real vs Mês Anterior, 3.3 Real vs Budget, 3.4 Real vs Ano Anterior.\n"
            "- Para CADA sub-seção, use este formato:\n"
            "  * Comece com o Custo FP total e a variação\n"
            "  * Agrupe por Type 05 (Labor, Burden, D&A)\n"
            "  * Dentro de cada Type 05, liste os Type 06 mais relevantes\n"
            "  * Para cada Type 06, cite os 2-3 Accounts com maiores impactos\n"
            "- Use markdown: ### para cada sub-seção, **negrito** para Type 05, itaálico para destaques.\n"
            "- Convenção Budget/Flex: Δ negativo = ganho, Δ positivo = perda.\n"
            "- Convenção mês a mês/ano anterior: Δ negativo = redução, Δ positivo = aumento.\n"
            "- NUNCA traduza nomes de colunas ou valores dos dados.\n"
            "- Valores em kBRL. Apresente absolutos E percentuais.\n"
            "- Use símbolos visuais: 🟢 para ganhos, 🔴 para perdas, ⚠️ para alertas, 💡 para insights.\n"
            "- Busque explicações PROFUNDAS: correlacione variações entre Type 05, "
            "identifique padrões (ex: alta em Benefits compensando queda em Direct Labor), "
            "sugira causas-raiz operacionais e impactos cruzados entre contas.\n"
            "- Quando referência for zero ou indisponível, declare explicitamente.\n"
            "- Tom executivo, direto ao ponto."
        ),
        "en": (
            "Based on the data below, write the analysis of 4 comparatives "
            "for {mes}/{ano}. Data is already organized in sub-sections 3.1 to 3.4.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Maintain structure of 4 sub-sections: 3.1 Actual vs Flex Budget, "
            "3.2 Actual vs Previous Month, 3.3 Actual vs Budget, 3.4 Actual vs Previous Year.\n"
            "- For EACH sub-section, use this format:\n"
            "  * Start with total FP Cost and variation\n"
            "  * Group by Type 05 (Labor, Burden, D&A)\n"
            "  * Within each Type 05, list the most relevant Type 06\n"
            "  * For each Type 06, cite the 2-3 Accounts with largest impacts\n"
            "- Use markdown: ### for each sub-section, **bold** for Type 05, italic for highlights.\n"
            "- Convention Budget/Flex: negative Δ = saving, positive Δ = overspend.\n"
            "- Convention month-over-month/year-over-year: negative Δ = reduction, positive Δ = increase.\n"
            "- NEVER translate column or value names.\n"
            "- Values in kBRL. Present absolutes AND percentages.\n"
            "- Use visual symbols: 🟢 for gains, 🔴 for losses, ⚠️ for alerts, 💡 for insights.\n"
            "- Seek DEEP explanations: correlate variations across Type 05, "
            "identify patterns (e.g., Benefits increase offsetting Direct Labor drop), "
            "suggest operational root causes and cross-account impacts.\n"
            "- When reference is zero or unavailable, state explicitly.\n"
            "- Executive tone, straight to the point."
        ),
    },

    # ── Prompts consolidados v2 ──────────────────────────────────

    "volume_completo": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva uma análise COMPLETA de volume de produção "
            "e variações por modelo para o mês de {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Os dados já estão organizados em 4 sub-tópicos (1.1 a 1.4). "
            "MANTENHA essa estrutura na análise.\n"
            "- 1.1 Volume Total: informe volumes Real, Actual e Budget.\n"
            "- 1.2 Real vs Budget: delta total + Top 10 modelos por impacto.\n"
            "- 1.3 Real vs Mês Anterior: delta total + Top 10 modelos.\n"
            "- 1.4 Real vs Ano Anterior: delta total + Top 10 modelos (se disponível).\n"
            "- Para cada comparação, apresente variação absoluta (unidades) E percentual.\n"
            "- Explique o IMPACTO das variações de volume sobre os custos "
            "(ex: volume maior dilui custos fixos, volume menor concentra).\n"
            "- Contextualize possíveis causas (sazonalidade, lançamentos, paradas).\n"
            "- Correlacione os modelos com impactos no custo: se um modelo de alto "
            "custo cresceu, quantifique o efeito potencial.\n"
            "- Use símbolos visuais: 📈 para crescimento, 📉 para queda, "
            "⚠️ para variações extremas, 💡 para insights.\n"
            "- Quando referência for zero ou indisponível, diga explicitamente.\n"
            "- Tom executivo e analítico. Limite: 4-6 parágrafos."
        ),
        "en": (
            "Based on the data below, write a COMPLETE analysis of production volume "
            "and model variations for {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Data is organized in 4 sub-topics (1.1–1.4). "
            "MAINTAIN that structure in the analysis.\n"
            "- 1.1 Total Volume: state Real, Actual and Budget volumes.\n"
            "- 1.2 Actual vs Budget: total delta + Top 10 models by impact.\n"
            "- 1.3 Actual vs Previous Month: total delta + Top 10 models.\n"
            "- 1.4 Actual vs Previous Year: total delta + Top 10 models (if available).\n"
            "- For each comparison, present absolute (units) AND percentage variation.\n"
            "- Explain the IMPACT of volume variations on costs "
            "(e.g., higher volume dilutes fixed costs, lower volume concentrates).\n"
            "- Contextualize possible causes (seasonality, launches, shutdowns).\n"
            "- Correlate models with cost impacts: if a high-cost model grew, "
            "quantify the potential effect.\n"
            "- Use visual symbols: 📈 for growth, 📉 for decline, "
            "⚠️ for extreme variations, 💡 for insights.\n"
            "- When reference is zero or unavailable, state explicitly.\n"
            "- Executive and analytical tone. Limit: 4-6 paragraphs."
        ),
    },

    "conclusoes": {
        "pt-BR": (
            "Com base nos dados e análises consolidados do mês de {mes}/{ano} abaixo, "
            "escreva um texto executivo abrangente de CONCLUSÕES E RECOMENDAÇÕES.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- PARTE 1 — Alertas e Anomalias:\n"
            "  * Liste os principais desvios detectados (vs Budget, Flex, Mês Anterior).\n"
            "  * Identifique tendências preocupantes ou positivas.\n"
            "  * Destaque qualquer inconsistência ou valor atípico.\n"
            "- PARTE 2 — Aprendizados e Observações Finais:\n"
            "  * Resuma os principais aprendizados do mês.\n"
            "  * Destaque pontos de atenção para o próximo mês.\n"
            "- PARTE 3 — Recomendações Estratégicas:\n"
            "  * Sugira ações concretas para redução de custos.\n"
            "  * Indique áreas que necessitam investigação adicional.\n"
            "  * Priorize as recomendações por impacto potencial.\n"
            "- Use símbolos visuais: ⚠️ para alertas, ✅ para resultados positivos, "
            "❌ para desvios negativos, 💡 para recomendações, 📊 para métricas.\n"
            "- Quando referência for zero ou indisponível, diga explicitamente.\n"
            "- Tom executivo. Formato: bullet points para alertas, parágrafos para recomendações.\n"
            "- Limite: 4-5 parágrafos."
        ),
        "en": (
            "Based on the consolidated data and analyses for {mes}/{ano} below, "
            "write a comprehensive executive text of CONCLUSIONS AND RECOMMENDATIONS.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- PART 1 — Alerts and Anomalies:\n"
            "  * List main deviations detected (vs Budget, Flex, Previous Month).\n"
            "  * Identify concerning or positive trends.\n"
            "  * Highlight any inconsistency or atypical value.\n"
            "- PART 2 — Learnings and Final Observations:\n"
            "  * Summarize main learnings of the month.\n"
            "  * Highlight attention points for next month.\n"
            "- PART 3 — Strategic Recommendations:\n"
            "  * Suggest concrete actions for cost reduction.\n"
            "  * Indicate areas requiring additional investigation.\n"
            "  * Prioritize recommendations by potential impact.\n"
            "- Use visual symbols: ⚠️ for alerts, ✅ for positive results, "
            "❌ for negative deviations, 💡 for recommendations, 📊 for metrics.\n"
            "- When reference is zero or unavailable, state explicitly.\n"
            "- Executive tone. Format: bullet points for alerts, paragraphs for recommendations.\n"
            "- Limit: 4-5 paragraphs."
        ),
    },

    "oficina": {
        "pt-BR": (
            "Com base nos dados abaixo, escreva uma análise resumida "
            "da oficina {oficina} para {mes}/{ano}.\n\n"
            "DADOS:\n{dados}\n\n"
            "INSTRUÇÕES:\n"
            "- Apresente o Custo FP total da oficina e a variação vs Flex Budget.\n"
            "- A análise contém 4 comparativos (Flex, Mês Anterior, Budget e Ano Anterior). "
            "Comente cada um.\n"
            "- Agrupe por Type 05 (Labor, Burden, D&A) com sub-accounts.\n"
            "- Não repita a análise de volume (já está na seção principal).\n"
            "- NUNCA traduza nomes de colunas ou valores.\n"
            "- Valores em kBRL. Tom executivo e conciso.\n"
            "- Use símbolos visuais: 🟢 para ganhos/economia, 🔴 para perdas, "
            "⚠️ para alertas, 💡 para insights.\n"
            "- Quando referência for zero, diga explicitamente ('⚠️ Ref. não disponível').\n"
            "- Busque explicações aprofundadas: correlacione variações entre "
            "Type 06 e Accounts, sugira causas-raiz e impactos cruzados.\n"
            "- Limite-se a 3-4 parágrafos."
        ),
        "en": (
            "Based on the data below, write a concise analysis "
            "for shop {oficina} in {mes}/{ano}.\n\n"
            "DATA:\n{dados}\n\n"
            "INSTRUCTIONS:\n"
            "- Present total FP Cost for the shop and variation vs Flex Budget.\n"
            "- The analysis contains 4 comparatives (Flex, Previous Month, Budget, Previous Year). "
            "Comment on each.\n"
            "- Group by Type 05 (Labor, Burden, D&A) with sub-accounts.\n"
            "- Do not repeat volume analysis (already in main section).\n"
            "- NEVER translate column or value names.\n"
            "- Values in kBRL. Executive and concise tone.\n"
            "- Use visual symbols: 🟢 for gains/savings, 🔴 for losses, "
            "⚠️ for alerts, 💡 for insights.\n"
            "- When reference is zero, state explicitly ('⚠️ Ref. not available').\n"
            "- Seek deep explanations: correlate variations across "
            "Type 06 and Accounts, suggest root causes and cross impacts.\n"
            "- Limit to 3-4 paragraphs."
        ),
    },

    "consulta_live": {
        "pt-BR": (
            "Você tem acesso aos dados abaixo extraídos dos parquets de custos industriais "
            "da Stellantis. Responda a pergunta do usuário de forma clara, direta e "
            "citando valores específicos dos dados.\n\n"
            "DADOS DISPONÍVEIS:\n{contexto}\n\n"
            "PERGUNTA: {pergunta}\n\n"
            "INSTRUÇÕES:\n"
            "- Responda com base EXCLUSIVAMENTE nos dados fornecidos.\n"
            "- Cite valores numéricos específicos sempre que possível (em kBRL).\n"
            "- Se não houver dados para responder, diga claramente.\n"
            "- NUNCA traduza nomes de colunas ou categorias.\n"
            "- Use símbolos visuais: 📈📉⚠️✅❌💡🏭📊🟢🔴 para destacar informações.\n"
            "- Tom profissional e direto."
        ),
        "en": (
            "You have access to the data below extracted from Stellantis industrial cost "
            "parquets. Answer the user's question clearly, directly, and "
            "citing specific values from the data.\n\n"
            "AVAILABLE DATA:\n{contexto}\n\n"
            "QUESTION: {pergunta}\n\n"
            "INSTRUCTIONS:\n"
            "- Answer based EXCLUSIVELY on the provided data.\n"
            "- Cite specific numeric values whenever possible (in kBRL).\n"
            "- If there is no data to answer, say so clearly.\n"
            "- NEVER translate column names or categories.\n"
            "- Use visual symbols: 📈📉⚠️✅❌💡🏭📊🟢🔴 to highlight info.\n"
            "- Professional and direct tone."
        ),
    },

    "consulta_pdf": {
        "pt-BR": (
            "Você tem acesso ao conteúdo do relatório anual de custos abaixo. "
            "Responda à pergunta do usuário de forma clara e objetiva, "
            "citando dados específicos quando disponíveis.\n\n"
            "CONTEÚDO DO RELATÓRIO:\n{contexto}\n\n"
            "PERGUNTA: {pergunta}\n\n"
            "Responda em português, de forma direta e com dados específicos."
        ),
        "en": (
            "You have access to the annual cost report content below. "
            "Answer the user's question clearly and objectively, "
            "citing specific data when available.\n\n"
            "REPORT CONTENT:\n{contexto}\n\n"
            "QUESTION: {pergunta}\n\n"
            "Answer in English, directly and with specific data."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════
#  LABELS BILÍNGUES PARA A INTERFACE E PDF
# ═══════════════════════════════════════════════════════════════

LABELS = {
    "pt-BR": {
        "titulo_relatorio": "Relatório Anual de Custos",
        "titulo_mes": "Relatório de {mes} de {ano}",
        # Seções v2 (3 seções consolidadas)
        "sec_volume_completo": "1. 📊 Análise de Volume e Variações por Modelo",
        "sec_comparativos": "2. 📈 Comparativos",
        "sec_conclusoes": "3. 💡 Conclusões e Recomendações",
        # Sub-seções de comparativos
        "sec_real_vs_flex": "2.1 Real vs Flex Budget",
        "sec_real_vs_mes_ant": "2.2 Real vs Mês Anterior",
        "sec_real_vs_budget": "2.3 Real vs Budget",
        "sec_real_vs_ano_ant": "2.4 Real vs Mesmo Mês do Ano Anterior",
        # Seções legadas (compat. com relatórios já gerados)
        "sec_volume": "1. Análise de Volume",
        "sec_variacoes": "2. Maiores Variações por Modelo de Veículo",
        "sec_anomalias": "4. Sumário de Anomalias e Destaques",
        "sec_obs_finais": "5. Observações Finais",
        "sec_oficina": "🏭 Oficina {oficina}",
        "capa_subtitulo": "Stellantis Cost Intelligence (SCI)",
        "capa_gerado": "Gerado em",
        "capa_versao": "Versão",
        "sumario": "Sumário",
        "rodape": "Gerado pelo TC Copilot — SCI v{versao} — {data}",
        "meses": [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
        ],
    },
    "en": {
        "titulo_relatorio": "Annual Cost Report",
        "titulo_mes": "{mes} {ano} Report",
        # Seções v2 (3 consolidated sections)
        "sec_volume_completo": "1. 📊 Volume Analysis and Model Variations",
        "sec_comparativos": "2. 📈 Comparatives",
        "sec_conclusoes": "3. 💡 Conclusions and Recommendations",
        # Comparative sub-sections
        "sec_real_vs_flex": "2.1 Actual vs Flex Budget",
        "sec_real_vs_mes_ant": "2.2 Actual vs Previous Month",
        "sec_real_vs_budget": "2.3 Actual vs Budget",
        "sec_real_vs_ano_ant": "2.4 Actual vs Same Month Previous Year",
        # Legacy sections (compat. with already-generated reports)
        "sec_volume": "1. Volume Analysis",
        "sec_variacoes": "2. Largest Variations by Vehicle Model",
        "sec_anomalias": "4. Anomalies and Highlights Summary",
        "sec_obs_finais": "5. Final Observations",
        "sec_oficina": "🏭 Shop {oficina}",
        "capa_subtitulo": "Stellantis Cost Intelligence (SCI)",
        "capa_gerado": "Generated on",
        "capa_versao": "Version",
        "sumario": "Table of Contents",
        "rodape": "Generated by TC Copilot — SCI v{versao} — {data}",
        "meses": [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
    },
}


def obter_nome_mes(mes_numero: int, idioma: str = "pt-BR") -> str:
    """Retorna o nome do mês pelo número (1-12)."""
    nomes = LABELS.get(idioma, LABELS["pt-BR"])["meses"]
    if 1 <= mes_numero <= 12:
        return nomes[mes_numero - 1]
    return str(mes_numero)
