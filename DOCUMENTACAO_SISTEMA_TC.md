# 📚 Documentação Técnica Completa — Sistema TC (TC Extendido + Arquitetura Geral)

> Documentação completa do TC Extendido e componentes transversais do SCI.

---

## 1) Visão Geral do Sistema

O Sistema TC é um conjunto de dashboards (Streamlit) para análise de custos e volumes de uma operação industrial:

- **TC Ext (Real)**: análise de custo total e CPU por período, veículo e oficina
- **Budget (BUD)**: dados planejados para comparação
- **Flex Bud**: budget flexível ajustado por volume real (fixos × variáveis)
- **Waterfall Analysis**: decomposição das diferenças entre períodos
- **Best Estimate / Forecast**: projeções e simulações
- **Exportação**: download de tabelas em Excel
- **TC Copilot**: agente de IA para relatórios e chatbot

Dados em **Parquet** (performático), aceita dados brutos via **Excel**.

---

## 2) Regras e Cálculo — TC Extendido

### CPU (Custo por Unidade)

```
CPU = Custo_Total / Volume_Total
```

⚠️ REGRA CRÍTICA: CPU calculado APÓS agrupamento, nunca antes.

**Exemplo:**
- Linha 1: Custo R$ 100, Volume 10 → CPU R$ 10/un
- Linha 2: Custo R$ 200, Volume 40 → CPU R$ 5/un
- Incorreto (média): (10 + 5) / 2 = R$ 7,50/un
- Correto: R$ 300 / 50 = **R$ 6,00/un**

### Custo Total

```
Custo_Total = Σ(Custo_Individual)
```

Sempre somar valores individuais, nunca calcular média.

### Fator de Conversão (K/M)

- **K (milhares):** Valor / 1.000
- **M (Milhões):** Valor / 1.000.000
- **Nenhum:** Valor original

⚠️ O fator de conversão NÃO deve ser aplicado no modo CPU.

**Ordem de Aplicação:**
1. Aplicar fator (K/M) — apenas em Custo Total
2. Converter moeda
3. Realizar cálculos (CPU, Flex, diferenças)

### Cálculo de Diferenças e Ratios

| Métrica | Fórmula | Interpretação |
|---------|---------|---------------|
| Delta Flex-BUD | Flex_BUD - BUD | Efeito do volume |
| Delta Total-Flex | Total - Flex_BUD | Efeito do custo |
| Ratio Total/Flex | Total / Flex_BUD × 100% | < 100% = eficiência |

### Flex Bud (Budget Flexível) — TC Extendido

**Conceito:** Ajusta o budget considerando a variação de volume.

**Regra para Fixos:** `Flex_Fixo = Valor_Original_Fixo` (sensibilidade 0%)
**Regra para Variáveis:** `Flex_Variável = Valor_Original × (Volume_Novo / Volume_Original)` (sensibilidade 100%)

**Identificação:** Coluna `Custo`: valores `'Fixo'` ou `'Variável'`

#### CASO 1: Flex para Real x Real (Waterfall)

Compara dois períodos reais (Mês 1 vs Mês 2):
```
rho = V_2 / V_1
Flex_Mês1 = C_1_Fixo + C_1_Variável × rho
```

**Exemplo:** V₁ = 40.848, V₂ = 60.333, C₁_Fixo = R$ 126,91, C₁_Var = R$ 755,36
→ rho = 1,4824 → Flex = R$ 126,91 + R$ 1.119,72 = R$ 1.246,63

#### CASO 2: Flex para Real x Budget (TC Ext)

```
rho = V_Real / V_Budget
Flex_Bud = B_Fixo + B_Variável × rho
```

**Exemplo:** V_Real = 50.000, V_Budget = 60.000 → rho = 0,8333
B_Fixo = R$ 200.000, B_Var = R$ 400.000 → Flex = R$ 533.333,33

| Aspecto | Real x Real | Real x Budget |
|---------|-------------|---------------|
| Base | Custo Real Mês 1 | Custo Budget |
| Proporção | V₂ / V₁ | V_Real / V_Budget |

### Fonte de Dados de Volume

- `df_vol_historico.parquet`: histórico consolidado
- `df_vol.parquet`: volume por ano
- Colunas obrigatórias: Volume, Período, Oficina, Veículo

⚠️ Volumes devem usar os MESMOS filtros aplicados aos dados de custo.

### Moedas Suportadas

| Moeda | Símbolo | Nota |
|-------|---------|------|
| BRL | R$ | Moeda base |
| USD | $ | Taxa USD→BRL |
| EUR | € | Taxa EUR→BRL |

### Sistema de Filtros

Ordem hierárquica:
1. Ano
2. Oficina
3. Veículo
4. USI
5. Período
6. Centro cst
7. Conta contábil
8. Type 5, Type 6
9. Fornecedor
10. Filtros Avançados: Usuário, Material, Dt.lçto., Texto breve, Account

---

## 3) Arquitetura — TC Extendido

### Estrutura do Projeto

```
C:\GIT\TC\
├── app.py                    # Portal / Router (st.navigation)
├── pages\
│   ├── 1 - Waterfall.py     # Waterfall (~4.000 linhas)
│   ├── 2 - Best Estimate - Simulador.py
│   ├── 5 - Extração de Dados.py
│   └── 6 - Documentacao.py
├── tc_ext\
│   └── pages\
│       ├── home_ext.py       # Home TC Ext
│       └── be_analise_ext.py # BE Análise
├── tc_principal\             # TC Veículos
├── tc_core\                  # Módulos compartilhados
├── tc_copilot\               # Agente de IA
└── dados\                    # Dados Parquet
```

### Estrutura da Pasta dados/

```
dados/
├── historico_consolidado/    # Dados consolidados (PRINCIPAL)
│   ├── df_final_historico.parquet
│   ├── df_ke5z_historico.parquet
│   ├── df_vol_historico.parquet
│   └── BUD/
│       └── (mesmos com sufixo _BUD)
├── {ano}/                    # Dados por ano
│   ├── df_final.parquet
│   ├── df_vol.parquet
│   ├── Dados SAPIENS.xlsx
│   └── BUD/
├── TC_Principal/             # TC Veículos
│   ├── {ano}/
│   ├── Forecast/
│   └── historico_consolidado/
└── Forecast/                 # Forecast TC Ext
```

Prioriza histórico consolidado para análises multi-anos. Budget e Real separados. Histórico sempre concatenado, nunca substituído.

### Stack Tecnológico

- **Streamlit** — Framework web
- **Python** 3.8+
- **Pandas** 2.0.0+ — Manipulação de dados
- **NumPy** 1.24.0+
- **Altair** 5.0.0+ — Gráficos interativos
- **Plotly** — Gráficos waterfall
- **PyArrow** 12.0.0+ — Suporte Parquet
- **OpenPyXL** 3.1.0+ — Geração Excel

### Otimizações

- Cache com TTL, Category para strings, Downcast Float64→Float32
- Substituição de iterrows()/apply() por merge/np.where
- CPU após agrupamento, Flex Bud com merge, Volume sincronizado

---

## 4) Colunas do DataFrame Final (df_final.parquet — TC Ext)

Mes, Período, Ano, Nºconta, Centrocst, Nºdoc.ref., Dt.lçto., Valor, QTD, Volume, Type 05, Type 06, Account, Custo, USI, Oficina, Doc.compra, Texto breve, Fornecedor, Material, Usuário, Fornec., Tipo, CC21, CC22, CC24, CC24 5L, CC24 7L, J516, Soma_Percentuais

---

## 5) Extração — TC Extendido

### Notebooks

| Aspecto | dados.ipynb (REAL) | dados_BUD.ipynb (BUDGET) |
|---------|-------------------|--------------------------|
| Guia Dados | "Sapiens" | "Voz de custo BDG" |
| Guia Rateio | "Rateio" | "Rateio BDG" |
| Pasta Saída | dados/{ANO}/ | dados/{ANO}/BUD/ |
| Sufixo | Sem | _BUD |

### Fluxo

```
Excel → Notebook → Merges → Parquet → Consolidar Histórico
```

### Células do Notebook
- **0:** Configuração (ANO, pastas)
- **1:** Leitura Sapiens (20 colunas)
- **2:** Merge com Base Conso (coluna Custo via Account)
- **3:** Rateio (melt meses → linhas)
- **4:** Merge KE5Z↔Rateio, pivot, cálculo CC21=CC21%×Valor
- **5:** Volume (header=50, melt)
- **6:** Merge df_final↔df_vol
- **7:** Salvamento + Consolidação Histórico

### Merges

| Merge | Chave | Tipo | Resultado |
|-------|-------|------|-----------|
| KE5Z ↔ Base Conso | Account | left | Coluna Custo |
| KE5Z ↔ Rateio | [Oficina, Período] | left | Colunas CC21%… |
| KE5Z ↔ Volume | [Oficina, Período, Veículo] | left | Coluna Volume |

### Regras Críticas
1. Chaves de Merge nunca alterar
2. Normalização de Período: sempre capitalizado
3. Cálculo: CC21 = CC21% × Valor
4. Histórico: concatenar, nunca substituir
5. Volume: sempre float64
6. Sufixo BUD: sempre _BUD em pasta BUD/

---

## 6) Best Estimate — TC Extendido

### Fórmula Passo a Passo
1. `proporção_volume = Volume_Futuro / Volume_Médio_Histórico`
2. `variação_percentual = proporção - 1.0`
3. `variação_ajustada = variação × sensibilidade`
4. `fator_variação = 1.0 + variação_ajustada`
5. `fator_inflação = 1.0 + (inflação / 100.0)`
6. `BE = Média_Histórica × fator_variação × fator_inflação`

### Simulador — Funcionalidades
- Configuração interativa (períodos, sensibilidades, inflação, volume)
- Custos Específicos (BE Manual): Pontual ou Constante, rateio automático
- Salvos em `dados/Forecast/custos_especificos.parquet`

### Arquivos Gerados
- `forecast_completo.parquet`
- `forecast_historico.parquet`
- `forecast_previsao.parquet`
- `df_final_historico_forecast.parquet`
- `custos_especificos.parquet`

**Nomenclatura:** "Histórico" (real), "BE" (forecast), "BE Manual" (custos específicos)

---

## 7) Flex Bud — Ano Completo e Governança

### Ano completo (12 meses)
- Gráficos/tabelas exibem 12 meses do ano
- Meses sem realizado: Real = 0 (nunca puxa Budget)
- Budget e Flex Bud continuam visíveis

### Flex Bud em meses sem realizado
- Sem volume real → usa volume Budget como base
- Flex Bud tende a ser igual ao Budget

### Governança de Custo Fixo
- Custo Fixo NUNCA flexibiliza no cálculo padrão de Flex Bud
- Flex_Fixo = BUD_Fixo (sensibilidade = 0%)
- Apenas no Simulador BE pode-se atribuir sensibilidade > 0% a fixos

### Regras Críticas de CPU
- CPU = Total / Volume (APÓS agrupamento)
- Nunca somar/mediar CPUs de linhas individuais
- Fator K/M NÃO se aplica em modo CPU

---

## 8) Apresentação Visual (Roteiro 5 Minutos)

**0:00–0:30** — O que é o SCI: plataforma para decisão estratégica em custos de manufatura
**0:30–1:30** — TC Extendido: custo total e CPU, Flex Budget
**1:30–3:00** — TC Veículos: cadeia completa, rateio por tempo de produção
**3:00–4:30** — Funcionalidades compartilhadas: Waterfall, Best Estimate, Multi-moeda
**4:30–5:00** — Benefícios: horas→segundos, zero erros, cenários "what-if"

### Comparativo TC Ext vs TC Veículos

| Aspecto | TC Extendido | TC Veículos |
|---------|-------------|-------------|
| Visão | Agregada | Por veículo |
| Coluna | Total | Custo FP |
| Rateio | Não há | Proporcional (tempo) |
| Pasta | dados/TC_Ext/ | dados/TC_Principal/ |

---

## 9) Guia de Build (EXE)

### Comando
```powershell
streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence
```

### Pós-build — copiar para _internal/
Copiar: `dados/`, `pages/`, `tc_core/`, `tc_principal/`, `tc_ext/`, `tc_copilot/`, `.streamlit/`, scripts `.py`, JSONs, imagens.

AgGrid:
```powershell
$dest = 'dist\Stellantis-Cost-Intelligence\_internal'
Copy-Item '.venv\Lib\site-packages\st_aggrid' -Destination ($dest + '\st_aggrid') -Recurse -Force
```

### sys.path e _MEIPASS
```python
if hasattr(sys, '_MEIPASS'):
    project_root = sys._MEIPASS
else:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
```

### Validação
- Abrir o .exe
- Confirmar resposta em `http://localhost:8501`
- Testar extração e parquets

### O que NÃO fazer
Não rodar `pyinstaller` direto do `.spec`.

---

## 10) Chatbot de Documentação

Assistente virtual que responde perguntas sobre o sistema baseado na documentação completa.

**Perguntas Sugeridas:**
- O que é o Stellantis Cost Intelligence (SCI)?
- Como funciona o Best Estimate?
- O que é Flex Bud?
- Como funciona o rateio por veículo?
- Qual a diferença entre TC Ext e TC Veículos?
- O que é CPU (Custo por Unidade)?
- Como funciona o Waterfall?

---

## 11) TC Copilot — Próximos Passos (GENAI Gateway)

### Integração GENAI Stellantis
- Modelos: GPT-4, Llama 3, Mistral, Cohere via GENAI Gateway
- OAuth2 (PingFederate) + mTLS + GraphQL
- Vector Store (OpenSearch) para embeddings

### Roadmap
1. **Fase 1:** Preparação dos dados (consolidar parquets, documentar métricas)
2. **Fase 2:** Integração GENAI (credenciais, GraphQL, embeddings)
3. **Fase 3:** Habilidades (perguntas, resumos, anomalias)
4. **Fase 4:** Produção (teste, validação, logs)

### Segurança
- 🔒 Nenhum dado sai da Stellantis
- 🔐 mTLS + PingFederate
- ✅ API homologada
- 📋 Logs de auditoria

---

*📚 Stellantis Cost Intelligence (SCI) | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus*
