# 🚗 TC Veículos — Documentação Completa (Fonte Única de Verdade)

> **Objetivo**: documentar o módulo **TC Veículos** de forma completa e fiel ao código, para uso executivo, técnico e pelo chatbot de IA.

---

## 1) Resumo Executivo e Objetivos do Projeto

O **Stellantis Cost Intelligence (SCI)** é uma plataforma de análise de custos industriais composta por dois módulos complementares:

**🚗 TC Veículos (TC Principal)**
- Cadeia completa: Despesa Primária → Custo FA → Custo FP → D&A → FP sem Dedicada
- Rateio proporcional por veículo (tempo de produção)
- 6 tabs especializadas: TC Veículos, Análise Flex, Volume, Custos por Oficina, Tempo de Produção, Dados Detalhados
- Best Estimate: simulador de premissas (sensibilidade, inflação, volume) com geração de Forecast

**📊 TC Extendido (TC Ext)**
- Análise de custos por oficina, conta e período
- Visualização Normal (Custo Total) e CPU (Custo por Unidade)
- Dashboard interativo com filtros

**🔧 Capacidades Transversais**
- Cache inteligente com TTL e otimização de tipos de dados
- Dados em formato Parquet comprimido
- Conversão multi-moeda (BRL, USD, EUR)
- Fator de escala configurável (Nenhum / K / M)
- Interface moderna com tabs, gráficos Altair e gradientes

**👥 Equipe do Projeto:**
- 🔧 Hudson Cardin — Full-Stack Developer
- 📊 Lauro Paiva Junior — Full-Stack Developer
- 🧭 Frederico Cesar de Jesus — Tech Advisor (Manufacturing Finance Controller, Stellantis)

---

## 2) Cadeia de Custos TC Veículos

```
Despesa Primária
    × Rateio FA
    = Custo FA (Fluxo Anexo)

Custo FP (Fluxo Principal)
    = Despesa Primária − Custo FA

D&A Dedicado = parcela de D&A atribuída diretamente ao veículo
FP sem Dedicada = Custo FP − D&A Dedicado
```

**Colunas Monetárias** (recebem conversão de moeda e fator):
- `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`

**Redis** — Não é uma coluna nem um Account fixo. Redis entra como linhas adicionais vindas da aba **massa - REDIS**, marcadas com `_fonte_redis=True`.

> Redis = Σ Despesa Primaria nas linhas com `_fonte_redis=True` (valores tipicamente negativos por serem receita)

---

## 3) Processo de Rateio por Veículo

O custo da oficina é **rateado** aos veículos proporcionalmente ao **tempo de produção**:

- **Percentual(v,o)** = TempoVeic(v,o) / Σ TempoVeic(v,o)
- **CustoRateado(v,o)** = FPsemDedicada(o) × Percentual(v,o)
- **CustoFPVeiculo(v,o)** = CustoRateado(v,o) + D&A Dedicado(v,o)

**Dados Consolidados vs Rateados:**

| Seleção | Fonte BUD | Fonte Real |
|---------|-----------|------------|
| Todos | `df_principal_BUD.parquet` | `df_principal.parquet` |
| Veículo específico | `df_veiculos_custo_fp_BUD.parquet` | `df_veiculos_custo_fp.parquet` |

> Quando Veículo = "Todos": dados consolidados. Quando Veículo = modelo específico: dados rateados com `Custo FP Veiculo`.

---

## 4) Flex Budget (TC Veículos)

O Budget Flex ajusta o orçamento pela proporção de volume realizado:
- Custos **fixos** permanecem iguais ao Budget
- Custos **variáveis** são ajustados pela proporção de volume

**Fórmulas:**
- **Proporção** = Volume Realizado / Volume Budget
- **Flex fixo** = BUD fixo (sem alteração)
- **Flex variável** = BUD variável × Proporção
- **Flex total** = Flex fixo + Flex variável

**Classificação Fixo/Variável:**
A coluna `Custo` determina a classificação:
- Valores que começam com `"Fix"` (case-insensitive) → **Fixo**
- Todos os demais → **Variável**

---

## 5) CPU (Custo por Unidade)

**CPU = Custo Total / Volume Total**

⚠️ REGRA CRÍTICA: O CPU deve ser calculado APÓS o agrupamento dos dados, nunca antes.

**Exemplo:**
- Linha 1: Custo = R$ 100, Volume = 10 → CPU = R$ 10,00/un
- Linha 2: Custo = R$ 200, Volume = 40 → CPU = R$ 5,00/un
- Incorreto (média de CPUs): (10 + 5) / 2 = R$ 7,50/un
- Correto (CPU após agrupar): R$ 300 / 50 = **R$ 6,00/un**

Quando o tipo de visualização é CPU:
- Cada métrica é dividida pelo volume total
- O sistema recalcula CPU após agregações

---

## 6) KPIs do TC Veículos

**KPIs do Topo (fora das tabs):**

| KPI | Fórmula |
|-----|---------|
| Desp. Primária | Σ Despesa Primaria |
| Custo FA | Σ Custo FA |
| Redis | Σ Despesa Primaria (linhas `_fonte_redis=True`) |
| Custo FP | Σ Custo FP |
| D&A Dedicada | Σ D&A dedicado |
| FP sem Dedicada | Σ FP sem Dedicada |

**KPIs do Resumo:**

| KPI | Fórmula |
|-----|---------|
| BUD | BUD fixo + BUD variável |
| Flex Bud − BUD | Flex total − BUD total |
| Flex BUD | BUD fixo + BUD variável × Proporção |
| Real − Flex Bud | Real total − Flex total |
| Real | Σ Custo FP Real |
| Real / Flex Bud | Real / Flex BUD (%) |

---

## 7) Filtros do TC Veículos

| Filtro | Tipo | Comportamento |
|--------|------|---------------|
| Oficina | multiselect | "Todos" ou seleção múltipla |
| Tipo Custo | multiselect | Fixo/Variável ou todos |
| Veículo | selectbox | "Todos" (consolidado) ou 1 veículo (rateado) |
| Período | multiselect | "Todos" ou seleção de meses |

Cascading: A seleção de Oficina filtra os Veículos disponíveis.

---

## 8) Visualizações e Gráficos

### Modos de Visualização

- **Fixo/Variável**: Expanders Fixo e Variável, sub-expanders por Type 05 → tabela por Account
- **Total**: Expanders direto por Type 05 → tabela por Account

### Tabela Flex por Account

| Coluna | Cálculo |
|--------|---------|
| Account | Nome da conta |
| BUD | Σ Custo FP Budget |
| Flex Bud − BUD | Flex − BUD |
| Flex BUD | Fixo: BUD / Variável: BUD × Proporção |
| Total − Flex Bud | Real − Flex |
| Total | Σ Custo FP Real |
| Total / Flex Bud | Real/Flex (%) |

### Barrinha de Progresso
- 🟢 Verde: ≤ 90%
- 🟡 Gradiente: 90%–100%
- 🔴 Vermelho: ≥ 100%

### Gráficos do TC Veículos

**Custo FP por Período:**
- Barras: Real por período (degradê roxo, scheme='purples')
- Linha pontilhada: Flex BUD (laranja, strokeDash=[10,5])
- Delta: Real − Flex BUD (verde/vermelho)
- Biblioteca: Altair

**Cores do Best Estimate:**
- 🟣 Roxo escuro (#4C1D95): meses Históricos (realizados)
- 🟣 Roxo claro (#C4B5FD): meses de Best Estimate (projetados)

### Organização em Tabs

| Tab | Conteúdo |
|-----|----------|
| 🚗 TC Veículos | KPIs + Gráfico Custo FP × Flex BUD |
| 📊 Análise Flex | Fixo/Variável com Type 05 → Account |
| 📈 Volume | Budget vs Realizado |
| 🏢 Custos por Oficina | Custo FP e Rateio FA |
| ⏱️ Tempo de Produção | Tempo Veículo vs Tempo FA |
| 📋 Dados Detalhados | Tabelas exportáveis + Sapiens detalhado |

---

## 9) Premissas do Simulador Best Estimate

**Fórmula Geral:**
```
BE = Média_Histórica × Fator_Variação × Fator_Inflação
```

Onde:
- Fator_Variação = 1 + (Variação_Volume × Sensibilidade)
- Fator_Inflação = 1 + (Inflação / 100)
- Variação_Volume = (Volume_Futuro / Volume_Médio_Histórico) − 1

**Resultado por tipo de custo:**
- **Custo Fixo BE** = Média Histórica × (1 + Inflação%) — sem ajuste de volume
- **Custo Variável BE** = Média Histórica × (Vol_Futuro / Vol_Histórico) × (1 + Inflação%)

**Sensibilidade:**
| Tipo | Sensibilidade | Fórmula |
|------|---------------|---------|
| Fixo | 0% | BE = Média × 1,0 × (1 + Inflação%) |
| Variável | 100% | BE = Média × (Vol_Futuro / Vol_Histórico) × (1 + Inflação%) |
| Semi-variável | 0% < s < 100% | BE = Média × (1 + Var_Volume × s) × (1 + Inflação%) |

**Geração de Forecast:**
- `forecast_completo.parquet` — Projeção mês a mês
- `premissas.json` — Premissas utilizadas

**Função `ratear_be_por_veiculo()`:**
Distribui custo BE proporcionalmente usando percentuais de rateio.
Fallback: se não encontrar percentual → distribui igualitariamente (1/N).

---

## 10) Arquitetura TC Veículos

### Estrutura de Pastas

```
dados/TC_Principal/
├── {ano}/
│   ├── BUD/
│   │   ├── df_principal_BUD.parquet
│   │   ├── df_vol_veiculos_BUD.parquet
│   │   ├── df_veiculos_custo_fp_BUD.parquet
│   │   └── ...
│   ├── df_principal.parquet
│   ├── df_tc_sapiens.parquet          ← todas as colunas Sapiens
│   ├── df_veiculos_custo_fp.parquet
│   └── df_vol_veiculos_actual.parquet
├── Forecast/
│   ├── forecast_completo.parquet
│   └── premissas.json
└── historico_consolidado/
```

### Schema — df_principal

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| Oficina | str | Centro de custo |
| Veículo | str | Modelo do veículo |
| Type 05 | str | Classificação nível 1 |
| Type 06 | str | Classificação nível 2 |
| Custo | str | Fixo ou Variável |
| Account | str | Conta contábil |
| Período | str | Mês por extenso |
| Despesa Primaria | float | Despesa primária (R$) |
| Custo FA | float | Custo do Fluxo Anexo |
| Custo FP | float | Custo FP consolidado |
| D&A dedicado | float | D&A dedicada |
| FP sem Dedicada | float | Custo FP sem D&A |

### Schema — df_tc_sapiens

Inclui TODAS as colunas acima mais as colunas extras do SAP:
Centrocst, Nºdoc.ref., Dt.lçto., Doc.compra, Texto breve, Fornecedor, Material, Usuário, Fornec., Tipo, USI, QTD, Rateio FA

### Estrutura do Código

```
tc_principal/
├── shared.py              # Constantes, loaders, helpers
├── ui_components.py       # Sidebar, CSS, KPIs
└── pages/
    ├── home_tc.py                     # Página principal (6 tabs)
    ├── best_estimate_simulador_tc.py  # Simulador BE
    ├── best_estimate_analise_tc.py    # Dashboard BE
    └── waterfall_tc.py                # Waterfall
```

### Pipeline de Processamento (processamento_dados_veiculos.py)

```
18 fases:
  1. Sapiens (leitura, todas as colunas)
  1B. Redis (aba massa-REDIS)
  2. Volume e EST PdR (Actual)
  3. Volume veículos (Actual)
  4. Tempo veículos (EST × Volume)
  5. Rateio FA
  6. Custo FA = Rateio FA × Despesa Primaria
  7. Custo FP = Despesa Primaria − Custo FA
  8. D&A Dedicado (do Budget)
  9. FP sem Dedicada = Custo FP − D&A dedicado
  10. Salvamento principal (parquets)
  10B. Parquet Sapiens detalhado (todas as colunas)
  11-18. Rateio por veículo, CPU, salvamento final
```

### Moeda e Fator

| Código | Símbolo | Conversão |
|--------|---------|-----------|
| BRL | R$ | 1.0 (base) |
| USD | $ | 1/Taxa USD→BRL |
| EUR | € | 1/Taxa EUR→BRL |

---

## 11) Guia de Extração de Dados

### Fluxo

```
Arquivos Excel (Entrada)
    ├── processamento_dados_BUD.py → df_principal_BUD.parquet + rateio veículos
    └── processamento_dados_veiculos.py → df_principal.parquet + df_tc_sapiens.parquet + rateio
```

**Busca de arquivos:**
1. `dados/{ANO}/Nome_do_Arquivo.xlsx` (prioridade)
2. `./Nome_do_Arquivo.xlsx` (raiz)

---

## 12) TC Copilot — Agente de IA

### Capacidades
- Relatório mensal com 3 seções: Volume e Variações, Comparativos, Conclusões
- Análise por oficina (AS, BS, GS, PL, PS, QY, SM)
- Chatbot live com contexto dos dados e documentação
- Emojis visuais (📈📉⚠️✅❌💡🏭📊🟢🔴)
- Tratamento de referências ausentes ("sem ref.", "sem base (ref.=0)")

### Arquitetura

```
tc_copilot/
├── data_collector.py     # Leitura de parquets, variações
├── prompts.py            # Prompts bilíngues
├── report_generator.py   # Pipeline PDF
├── llm_integration.py    # Integração OpenAI
└── pages/home_copilot.py # Interface Streamlit
```

---

## 13) Guia de Build (EXE)

- `streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence`
- Pós-build: copiar `dados/`, módulos, páginas para `dist/<NOME>/_internal/`
- No EXE, `sys._MEIPASS` aponta para `_internal/`
- AgGrid precisa ser copiado manualmente do `.venv`

---

*📚 Stellantis Cost Intelligence (SCI) | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus*
