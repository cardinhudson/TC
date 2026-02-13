# 🚗 Documentação Técnica — TC Veículos

> **Objetivo**: Documentação completa do módulo TC Veículos — análise de custo de produção de veículos, com visão consolidada e rateada por veículo.

---

## 1) Visão Geral

O módulo **TC Veículos** é o dashboard de análise de custos de produção de veículos. Ele complementa o TC Extendido, focando nos custos diretos de fabricação rateados por modelo de veículo.

### 1.1 Funcionalidades Principais
- **KPIs Resumo**: Despesa Primária, Custo FA, Redis, Custo FP, D&A Dedicado, FP sem Dedicada
- **TC Veículos**: Custo FP por período com gráfico Real + linha Flex BUD pontilhada
- **Análise Flex por Categoria**: Fixo/Variável com hierarquia Type 05 → Account
- **Volume**: Budget vs Realizado, por período e por veículo
- **Custos por Oficina**: Custo FP e Rateio FA por oficina
- **Análise Flex**: Fixo vs Variável com gráficos empilhados e pizza
- **Tempo de Produção**: Tempo Veículo vs Tempo FA por oficina
- **Dados Detalhados**: Tabelas exportáveis de Real e Budget

### 1.2 Arquitetura de Filtros
| Filtro | Tipo | Comportamento |
|--------|------|---------------|
| Oficina | multiselect | "Todos" ou seleção múltipla |
| Tipo Custo | multiselect | Fixo/Variável ou todos |
| Veículo | **selectbox** | "Todos" (consolidado) ou **1 veículo** (rateado) |
| Período | multiselect | "Todos" ou seleção de meses |

- Quando **Veículo = "Todos"**: dados consolidados (`df_principal_BUD.parquet`)
- Quando **Veículo = modelo específico**: dados rateados (`df_veiculos_custo_fp_BUD.parquet`)
- Filtros são **globais** — afetam KPIs, gráficos e Análise Flex simultaneamente

---

## 2) Contratos de Dados (Parquets)

### 2.1 Estrutura de Pastas
```
dados/TC_Principal/
├── {ano}/
│   ├── BUD/                          # Budget
│   │   ├── df_principal_BUD.parquet       # Custo consolidado
│   │   ├── df_vol_veiculos_BUD.parquet    # Volume por veículo (Budget)
│   │   ├── df_veiculos_custo_fp_BUD.parquet  # Custo FP rateado por veículo
│   │   ├── df_veiculos_cpu_BUD.parquet    # CPU por veículo
│   │   ├── df_tempo_veiculos_BUD.parquet  # Tempo de produção
│   │   ├── df_dea_dedicado_BUD.parquet    # D&A Dedicado
│   │   └── df_volume_fa_BUD.parquet       # Volume Fluxo Anexo
│   ├── df_principal.parquet               # Custo Real consolidado
│   ├── df_vol_veiculos_actual.parquet     # Volume Realizado
│   ├── df_veiculos_custo_fp.parquet       # Custo FP Real rateado
│   └── df_veiculos_cpu.parquet            # CPU Real
```

### 2.2 Schema — Principal BUD
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `Oficina` | str | Centro de custo (oficina de produção) |
| `Veículo` | str | Modelo do veículo |
| `Type 05` | str | Classificação hierárquica nível 1 |
| `Type 06` | str | Classificação hierárquica nível 2 |
| `Custo` | str | Tipo: Fixo ou Variável |
| `Account` | str | Conta contábil (inclui "Redis") |
| `Período` | str | Mês por extenso (Janeiro, Fevereiro, ...) |
| `Despesa Primaria` | float | Despesa primária (R$) |
| `Custo FA` | float | Custo do Fluxo Anexo (R$) |
| `Custo FP` | float | Custo FP consolidado (R$) |
| `D&A dedicado` | float | Depreciação & Amortização dedicada (R$) |
| `FP sem Dedicada` | float | Custo FP sem D&A dedicada (R$) |

### 2.3 Schema — Veículos Rateado (BUD)
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `Oficina` | str | Centro de custo |
| `Veículo` | str | Modelo do veículo |
| `Type 05` | str | Classificação nível 1 |
| `Type 06` | str | Classificação nível 2 |
| `Custo` | str | Tipo: Fixo ou Variável |
| `Account` | str | Conta contábil |
| `Período` | str | Mês por extenso |
| `Custo Rateado` | float | Custo rateado pelo percentual do veículo |
| `D&A dedicado` | float | D&A dedicada direta do veículo |
| `Custo FP Veiculo` | float | Custo FP final do veículo (Rateado + D&A) |
| `Ano` | int | Ano de referência |

> **Nota**: O parquet BUD veículos tem `Custo FP Veiculo` (não `Custo FP`). O sistema faz o mapeamento automático: `Custo FP = Custo FP Veiculo`.

---

## 3) Composição de Custos

### 3.1 Cadeia de Custos
```
Despesa Primária
  + Custo FA (Fluxo Anexo × Rateio FA)
  = Custo FP (Fabricação Principal)
  
Custo FP = Despesa Primária + Custo FA
D&A Dedicado = parcela de D&A atribuída diretamente ao veículo
FP sem Dedicada = Custo FP − D&A Dedicado
```

### 3.2 Colunas Monetárias
As seguintes colunas recebem conversão de moeda e fator:
- `Despesa Primaria`
- `Custo FA`
- `Custo FP`
- `D&A dedicado`
- `FP sem Dedicada`

### 3.3 Redis
Redis **não é uma coluna** — é identificado por linhas onde `Account = 'Redis'`:
$$\text{Redis} = \sum_{\{Account=Redis\}} \text{Despesa Primária}$$

---

## 4) Rateio por Veículo

### 4.1 Processo de Rateio
O custo da oficina é rateado aos veículos proporcionalmente ao tempo de produção:

$$\text{Percentual}_{v,o} = \frac{\text{TempoVeic}_{v,o}}{\sum_v \text{TempoVeic}_{v,o}}$$

$$\text{CustoRateado}_{v,o} = \text{FPsemDedicada}_{o} \times \text{Percentual}_{v,o}$$

$$\text{CustoFPVeiculo}_{v,o} = \text{CustoRateado}_{v,o} + \text{D\&A Dedicado}_{v,o}$$

### 4.2 Dados Consolidados vs Rateados
| Seleção | Fonte BUD | Fonte Real |
|---------|-----------|------------|
| Todos | `df_principal_BUD.parquet` | `df_principal.parquet` |
| Veículo específico | `df_veiculos_custo_fp_BUD.parquet` | `df_veiculos_custo_fp.parquet` |

---

## 5) Flex Budget

### 5.1 Conceito
O Budget Flex ajusta o orçamento pela proporção de volume realizado:
- Custos **fixos** permanecem iguais ao Budget
- Custos **variáveis** são ajustados pela proporção de volume

### 5.2 Fórmulas

$$\text{Proporção} = \frac{Volume_{Realizado}}{Volume_{Budget}}$$

$$Flex_{fixo} = BUD_{fixo}$$

$$Flex_{variável} = BUD_{variável} \times \text{Proporção}$$

$$Flex_{total} = Flex_{fixo} + Flex_{variável}$$

### 5.3 Classificação Fixo/Variável
A coluna `Custo` determina a classificação:
- Valores que começam com `"Fix"` (case-insensitive, sem acentos) → **Fixo**
- Todos os demais → **Variável**

```python
# Implementação
mask_fixo = df['Custo'].str.lower().str.startswith('fix')
```

---

## 6) CPU (Custo por Unidade)

### 6.1 Fórmula

$$CPU = \frac{\text{Custo Total}}{\text{Volume Total}}$$

Com proteção contra divisão por zero:
```python
CPU = np.where(volume != 0, custo / volume, 0.0)
```

### 6.2 Aplicação
Quando o tipo de visualização é **CPU**:
- Cada métrica é dividida pelo volume total
- O fator K/M **não é aplicado** (sempre "Nenhum")
- Volumes de BUD e Actual são usados conforme o contexto

---

## 7) KPIs — Resumo TC Veículos

### 7.1 KPIs do Topo (fora das tabs)
| KPI | Fórmula |
|-----|---------|
| Desp. Primária | $\sum \text{Despesa Primaria}$ |
| Custo FA | $\sum \text{Custo FA}$ |
| Redis | $\sum \text{Despesa Primaria}$ onde $Account = Redis$ |
| Custo FP | $\sum \text{Custo FP}$ |
| D&A Dedicada | $\sum \text{D\&A dedicado}$ |
| FP sem Dedicada | $\sum \text{FP sem Dedicada}$ |

### 7.2 KPIs do Resumo TC Veículos (tab TC Veículos)
| KPI | Fórmula |
|-----|---------|
| BUD | $BUD_{fixo} + BUD_{variável}$ |
| Flex Bud − BUD | $Flex_{total} - BUD_{total}$ |
| Flex BUD | $BUD_{fixo} + BUD_{variável} \times Proporção$ |
| Real − Flex Bud | $Real_{total} - Flex_{total}$ |
| Real | $\sum \text{Custo FP Real}$ |
| Real / Flex Bud | $\frac{Real}{Flex BUD}$ (%) |

---

## 8) Análise Flex por Categoria

### 8.1 Modos de Visualização
- **Fixo/Variável**: Expanders `💰 Fixo` e `💰 Variável`, cada um com sub-expanders por `Type 05` → tabela por `Account`
- **Total**: Expanders direto por `Type 05` → tabela por `Account`

### 8.2 Tabela Flex por Account
| Coluna | Cálculo |
|--------|---------|
| Account | Nome da conta |
| BUD | $\sum \text{Custo FP Budget}$ |
| Flex Bud − BUD | $Flex - BUD$ |
| Flex BUD | Fixo: $BUD$, Variável: $BUD \times Proporção$ |
| Total − Flex Bud | $Real - Flex$ |
| Total | $\sum \text{Custo FP Real}$ |
| Total / Flex Bud | $\frac{Real}{Flex}$ (com barrinha de progresso) |

### 8.3 Barrinha de Progresso
- Verde: ≤ 90%
- Gradiente verde→vermelho: 90%–100%
- Vermelho: ≥ 100%

---

## 9) Gráficos

### 9.1 Custo FP por Período
- **Barras**: Real por período com degradê roxo (`scheme='purples'`)
- **Linha pontilhada**: Flex BUD (laranja, `strokeDash=[10,5]`)
- **Delta**: Gráfico inferior com `Real − Flex BUD` (verde = positivo, vermelho = negativo)
- Biblioteca: **Altair** com `data_transformers.disable_max_rows()`

### 9.2 Volume
- **Barras**: Volume Budget (degradê verde)
- **Linha tracejada**: Volume Realizado (laranja)
- **Por Veículo**: Barras agrupadas por modelo

### 9.3 Custos por Oficina
- Barras Custo FP por Oficina
- Barras Rateio FA por Oficina (verde/vermelho)
- Tabela BUD vs Flex pivotada Oficina × Período

---

## 10) Filtros — Arquitetura Unificada

### 10.1 Fluxo de Dados
```
    Sidebar filters
         │
         ├── Veículo = "Todos" ──► usar_rateado = False
         │         ├── df_principal_BUD  → df_bud
         │         ├── df_principal_Real → df
         │         └── Volumes: todos os veículos
         │
         └── Veículo = "CC21 biton" ──► usar_rateado = True
                   ├── df_veiculos_custo_fp_BUD → df_bud (filtrado)
                   ├── df_veiculos_custo_fp_Real → df (filtrado)
                   └── Volumes: filtrado pelo veículo
         │
    aplicar_fator_df() + converter_moeda_df()
         │
    calcular_flex_budget()
         │
    df_bud = Budget     df = Real (ou Budget se sem Real)
         │
    ┌────┴────────────────────────┐
    │  Todos os tabs usam        │
    │  df_bud, df, df_vol_bud,   │
    │  df_vol_actual, df_flex    │
    └─────────────────────────────┘
```

### 10.2 Cascading
A seleção de **Oficina** filtra os **Veículos** disponíveis no selectbox:
```python
_df_filt_ofi = df[df['Oficina'].isin(oficinas_selecionadas)]
veiculos = sorted(_df_filt_ofi['Veículo'].dropna().unique())
```

---

## 11) ETL e Processamento

### 11.1 Arquivos de Processamento
| Arquivo | Função |
|---------|--------|
| `processamento_dados_BUD.py` | Processa dados Budget (principal + veículos) |
| `processamento_dados_veiculos_BUD.py` | Rateio por veículo + CPU |
| `processamento_dados.py` | Processa dados Real (Sapiens) |

### 11.2 Pipeline de Processamento
1. Extração dos dados brutos (Excel/SAP)
2. Normalização de colunas e períodos
3. Cálculo de composição de custos (Desp. Primária → FA → FP)
4. Rateio por veículo (tempo de produção)
5. Cálculo de CPU por veículo
6. Gravação em Parquet na pasta `dados/TC_Principal/{ano}/`

### 11.3 Cache
- `@st.cache_data(ttl=3600)` em todos os loaders
- Botão "🔄 Limpar Cache" na sidebar para forçar recarga

---

## 12) Configurações Globais

### 12.1 Moeda
| Código | Símbolo | Conversão |
|--------|---------|-----------|
| BRL | R$ | 1.0 (base) |
| USD | $ | $\frac{1}{Taxa_{USD→BRL}}$ |
| EUR | € | $\frac{1}{Taxa_{EUR→BRL}}$ |

### 12.2 Fator
| Opção | Divisor |
|-------|---------|
| Nenhum | 1 |
| K (milhares) | 1.000 |
| M (milhões) | 1.000.000 |

### 12.3 Tipo de Visualização
| Tipo | Comportamento |
|------|---------------|
| Custo Total | Valores absolutos em R$/USD/EUR |
| CPU | Custo ÷ Volume (fator = Nenhum) |

---

*Documentação gerada automaticamente — TC Veículos v1.91*
