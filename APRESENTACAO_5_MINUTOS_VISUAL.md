# 📊 Stellantis Cost Intelligence (SCI)
## Apresentação Visual — 5 Minutos

<div align="center">

**Equipe:** Hudson Cardin · Lauro Paiva · Frederico Cesar de Jesus  
**Versão:** Sistema completo com versionamento automático  
**Data:** 2026

---

</div>

---

## 🎯 SLIDE 1: O QUE É O SCI
### ⏱️ 30 segundos

<div align="center">

# 📊 Stellantis Cost Intelligence (SCI) — Sistema de Análise de Custos

### Plataforma de Dashboards para decisão estratégica em custos de manufatura

</div>

---

### 🎯 Visão Geral

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  📊 SCI — Dois módulos complementares em um único sistema     │
│                                                                     │
│  ┌────────────────────────────┐  ┌────────────────────────────────┐ │
│  │  📊 TC EXTENDIDO           │  │  🚗 TC VEÍCULOS                │ │
│  │  ─────────────────────────│  │  ──────────────────────────────│ │
│  │  Visão agregada de        │  │  Visão detalhada por           │ │
│  │  custos por oficina,      │  │  veículo, com cadeia de        │ │
│  │  período e veículo.       │  │  custos e rateio por           │ │
│  │                           │  │  tempo de produção.            │ │
│  │  Coluna: Total            │  │  Coluna: Custo FP              │ │
│  │  Modo: Custo Total ↔ CPU  │  │  Modo: Cadeia FA → FP → D&A   │ │
│  └────────────────────────────┘  └────────────────────────────────┘ │
│                                                                     │
│  🔗 Funcionalidades compartilhadas:                                 │
│     Waterfall · Best Estimate · Extração · Multi-moeda · Export     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**🎯 Objetivo:**
> Transformar dados brutos (Excel/SAPIENS) em **insights acionáveis** — comparar Real vs Budget,
> decompor variações e projetar cenários futuros com Best Estimate.

---

## 📊 SLIDE 2: TC EXTENDIDO
### ⏱️ 1 minuto

<div align="center">

# 📊 Módulo 1 — TC Extendido

### Análise agregada de custo total e CPU por período

</div>

---

### 📊 O que é o TC Extendido?

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  📊 TC EXTENDIDO — Linhas Secundárias                               │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  🎯 Propósito:                                                      │
│     Análise de custo total agregado e CPU (Custo por Unidade)       │
│     por período, oficina e veículo — visão de linhas secundárias    │
│                                                                     │
│  📐 Coluna principal de custo: Total                                │
│                                                                     │
│  🔄 Dois modos de visualização:                                     │
│     ┌──────────────────────┐   ┌──────────────────────┐            │
│     │  💰 Custo Total      │   │  📏 CPU              │            │
│     │  sum(Total)          │   │  sum(Total)          │            │
│     │  Soma direta         │   │  ─────────           │            │
│     │                      │   │  sum(Volume)         │            │
│     └──────────────────────┘   └──────────────────────┘            │
│                                                                     │
│  ⚠️  Regra crítica:                                                 │
│     CPU NUNCA é somada/mediada — sempre recalculada como razão      │
│     ponderada: sum(Total) / sum(Volume)                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 📋 Filtros e Dimensões

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🔍 FILTROS DISPONÍVEIS                                             │
│                                                                     │
│  Básicos:  Ano · Período · Oficina · Veículo · USI                  │
│  Custo:    Type 05 · Type 06 · Account · Fornecedor                 │
│  Avançado: Type 07 · Material · Pedido · Ordem · Origem             │
│                                                                     │
│  📊 DADOS DE ENTRADA                                                │
│                                                                     │
│  Pasta:     dados/TC_Ext/{ano}/                                     │
│  Histórico: dados/TC_Ext/historico_consolidado/                     │
│  Parquets:  df_final.parquet, df_vol.parquet                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 📈 Flex Budget (TC Ext)

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  📈 FLEX BUDGET — Ajuste por volume                                 │
│                                                                     │
│  Custo Fixo:                                                        │
│     Flex = Budget fixo (inalterado)                                 │
│                                                                     │
│  Custo Variável:                                                    │
│     Flex = Budget variável × (Volume Real / Volume Budget)          │
│                                                                     │
│  🎯 Permite isolar: variação veio do volume ou do custo?            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚗 SLIDE 3: TC VEÍCULOS
### ⏱️ 1 minuto 30 segundos

<div align="center">

# 🚗 Módulo 2 — TC Veículos

### Análise de custo do Fluxo Principal (Custo FP) rateado por modelo de veículo

</div>

---

### 🚗 O que é o TC Veículos?

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🚗 TC VEÍCULOS — Custo do Fluxo Principal (FP) por Modelo          │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  🎯 Propósito:                                                      │
│     Análise detalhada do custo do Fluxo Principal (Custo FP) por    │
│     veículo, com cadeia completa de custos e rateio proporcional    │
│     ao tempo de produção em cada oficina.                           │
│                                                                     │
│  📐 Coluna principal de custo: Custo FP                             │
│                                                                     │
│  🔗 Cadeia de custos:                                               │
│     ┌──────────────────────────────────────────────────────────┐   │
│     │ Despesa Primária = Custo FA + Custo FP                    │   │
│     │                                                          │   │
│     │ Custo FA = Rateio FA × Despesa Primária                   │   │
│     │ Custo FP = Despesa Primária − Custo FA                    │   │
│     └──────────────────────────────────────────────────────────┘   │
│                               │                                     │
│                    ┌──────────▼──────────┐                          │
│                    │ D&A Dedicado        │                          │
│                    │ (atribuído ao veíc.)│                          │
│                    └──────────┬──────────┘                          │
│                               │                                     │
│                    ┌──────────▼──────────┐                          │
│                    │ FP sem Dedicada     │                          │
│                    │ = Custo FP − D&A    │                          │
│                    └─────────────────────┘                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 🔄 Rateio por Veículo

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🔄 RATEIO PROPORCIONAL AO TEMPO DE PRODUÇÃO                       │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  Quando um veículo é selecionado, o sistema rateia os custos:       │
│                                                                     │
│  1. Percentual = Tempo_Veículo(v,o) / Σ Tempo_Veículo(v,o)         │
│  2. Custo Rateado(v,o) = FP_sem_Dedicada(o) × Percentual(v,o)      │
│  3. Custo FP Veículo(v,o) = Custo_Rateado(v,o) + D&A_Dedicado(v,o) │
│                                                                     │
│  Exemplo:                                                           │
│  ┌───────────────────────────────────────────────────────┐          │
│  │  Oficina: Pintura                                     │          │
│  │  Veículo A: 40 min  │  Veículo B: 60 min              │          │
│  │                                                       │          │
│  │  % Veíc A = 40/(40+60) = 40%                          │          │
│  │  % Veíc B = 60/(40+60) = 60%                          │          │
│  │                                                       │          │
│  │  FP sem Dedicada = R$ 100.000                         │          │
│  │  Rateio Veíc A = 100.000 × 40% = R$ 40.000           │          │
│  │  Rateio Veíc B = 100.000 × 60% = R$ 60.000           │          │
│  └───────────────────────────────────────────────────────┘          │
│                                                                     │
│  Veículo = "Todos" → dados consolidados (sem rateio)                │
│  Veículo específico → dados rateados com Custo FP Veículo           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 📋 6 Tabs da Home TC Veículos

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🚗 HOME TC VEÍCULOS — 6 Tabs de Análise                           │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐                  │
│  │ 🚗 TC Veículos      │  │ 📊 Análise Flex     │                  │
│  │ KPIs resumo +       │  │ Fixo/Variável com   │                  │
│  │ Custo FP × Flex BUD │  │ hierarquia          │                  │
│  │ por período         │  │ Type 05 → Account   │                  │
│  └─────────────────────┘  └─────────────────────┘                  │
│                                                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐                  │
│  │ 📈 Volume           │  │ 🏢 Custos p/ Ofic.  │                  │
│  │ Budget vs Realizado │  │ Custo FP e Rateio   │                  │
│  │ por período e       │  │ FA por oficina      │                  │
│  │ por veículo         │  │                     │                  │
│  └─────────────────────┘  └─────────────────────┘                  │
│                                                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐                  │
│  │ ⏱️ Tempo Produção   │  │ 📋 Dados Detalhados │                  │
│  │ Tempo Veículo vs    │  │ Tabelas exportáveis │                  │
│  │ Tempo FA p/ oficina │  │ de Real e Budget    │                  │
│  └─────────────────────┘  └─────────────────────┘                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 📊 Dados e Filtros (TC Veículos)

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  📊 DADOS DE ENTRADA                                                │
│                                                                     │
│  Pasta:     dados/TC_Principal/{ano}/                               │
│  Budget:    dados/TC_Principal/{ano}/BUD/                           │
│  Histórico: dados/TC_Principal/historico_consolidado/               │
│  Parquets:  df_principal, df_veiculos_custo_fp,                     │
│             df_vol_veiculos (Real) / df_vol_veiculos_actual (Actual),│
│             df_dea_dedicado, df_volume_fa                           │
│                                                                     │
│  🔍 FILTROS: Oficina · Veículo (ativa rateio) · Type 05/06 ·       │
│              Account · Custo (Fixo/Variável)                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔗 SLIDE 4: FUNCIONALIDADES COMPARTILHADAS
### ⏱️ 1 minuto 30 segundos

<div align="center">

# 🔗 Funcionalidades Compartilhadas

### Disponíveis em ambos os módulos (TC Ext e TC Veículos)

</div>

---

### 📈 Waterfall — Análise de Variações

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  📈 WATERFALL — "O que mudou entre dois períodos?"                  │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  Compara dois períodos e decompõe a variação:                       │
│                                                                     │
│     Mês 1  ──▶  Efeito Volume  ──▶  Efeito Custo  ──▶  Mês 2      │
│      100          +15                  +5                120        │
│                                                                     │
│  O Flex Budget SEPARA as causas:                                    │
│     • Variação por Volume — quanto mudou por ter produzido mais/    │
│       menos (ajuste Flex)                                           │
│     • Variação por Custo — quanto mudou pelo custo em si            │
│                                                                     │
│  📊 TC Ext: coluna Total                                            │
│  🚗 TC Veículos: cadeia Desp Primária → Redis → FA → D&A → FP      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 🔮 Best Estimate — Simulador + consumo do Forecast

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🔮 BEST ESTIMATE — Projeções inteligentes                         │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  🎛️ SIMULADOR: define premissas e gera Forecast                    │
│     ┌─────────────────────────────────────────────────────────┐    │
│     │  Premissa        Descrição                              │    │
│     │  ─────────────── ───────────────────────────────────── │    │
│     │  Sensibilidade   O quanto o custo responde ao volume    │    │
│     │                  (0% = fixo, 100% = variável)           │    │
│     │  Inflação        % reajuste SOBRE TODOS os custos       │    │
│     │  Volume          Produção projetada por veículo/mês     │    │
│     └─────────────────────────────────────────────────────────┘    │
│                                                                     │
│  🧮 Fórmula:                                                       │
│     BE = Média_Histórica × (1 + Var_Volume × Sensib) × (1 + Infl)  │
│                                                                     │
│     Fixo:     sensibilidade = 0% → sem ajuste de volume             │
│     Variável: sensibilidade = 100% → escala com volume              │
│     Inflação aplicada APÓS sensibilidade, a TODOS os custos         │
│                                                                     │
│  📊 CONSUMO: a Home do TC Veículos usa os outputs do Forecast       │
│     quando disponíveis (mantém regras de CPU e fator/moeda).        │
│                                                                     │
│  Saída: dados/*/Forecast/forecast_completo.parquet                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 🌐 Recursos Transversais

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  🔄 MULTI-MOEDA                 📥 EXTRAÇÃO DE DADOS               │
│     R$ · USD · EUR                 Upload Excel → Notebooks →       │
│     Conversão com câmbio           Parquet consolidado               │
│     persistido em SQLite                                            │
│                                                                     │
│  📊 FATOR DE ESCALA             📤 EXPORTAÇÃO EXCEL                 │
│     Nenhum · K (÷1.000)           Downloads formatados com          │
│     · M (÷1.000.000)              filtros do usuário aplicados      │
│     Nunca em CPU                                                    │
│                                                                     │
│  🔄 VERSIONAMENTO               📚 DOCUMENTAÇÃO                    │
│     Automático — incrementa        Página única e integrada         │
│     quando páginas mudam           no próprio sistema               │
│                                                                     │
│  ⚡ CACHE INTELIGENTE                                               │
│     TTL + otimização de tipos — consultas rápidas e eficientes      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📐 SLIDE 5: COMPARATIVO TC EXT vs TC VEÍCULOS
### ⏱️ 30 segundos

<div align="center">

# 📐 Comparativo entre os Módulos

</div>

---

### 📋 Diferenças Resumidas

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  ASPECTO              📊 TC EXTENDIDO       🚗 TC VEÍCULOS          │
│  ─────────────────── ──────────────────── ──────────────────────── │
│  Visão                Agregada             Detalhada por veículo    │
│  Coluna de custo      Total                Custo FP (cadeia)        │
│  Modo de exibição     Custo Total ↔ CPU    Custo FP + cadeia        │
│  Rateio               Não há               Proporcional (tempo)     │
│  Volume               Simples              Por veículo + FA         │
│  Pasta de dados       dados/TC_Ext/        dados/TC_Principal/      │
│  Filtros avançados    Material, Pedido,    Type 05/06, Account,     │
│                       Ordem, Origem...     Custo Fixo/Variável      │
│  Tabs na Home         Consolidada          6 tabs especializadas    │
│  Flex Budget          Total                Cadeia completa          │
│  Waterfall            Coluna Total         Cadeia FA→FP→D&A         │
│                                                                     │
│  ─────────────────────────────────────────────────────────────────  │
│  COMPARTILHADO: Waterfall · Best Estimate · Extração · Export ·     │
│                 Multi-moeda · Cache · Versionamento · Documentação  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ SLIDE 6: ARQUITETURA TÉCNICA
### ⏱️ 30 segundos

<div align="center">

# 🏗️ Arquitetura do Sistema

</div>

---

### 📐 Estrutura Modular

```
                    ┌─────────────────────┐
                    │   📊 app.py         │
                    │   Portal / Router   │
                    │   (st.navigation)   │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
  ┌───────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐
  │ 📊 TC Extendido │  │ 🚗 TC Veículos │  │ 📚 Documentação │
  │ Visão agregada  │  │ Visão veículo  │  │ Página única    │
  └───────┬────────┘  └───────┬────────┘  └────────────────┘
          │                    │
  ┌───────▼────────┐  ┌───────▼────────┐
  │ tc_ext/        │  │ tc_principal/  │
  │ • home_ext     │  │ • home_tc      │
  │ • be_analise   │  │ • waterfall_tc │
  │ • normalizacao │  │ • be_simulador │
  │ • metricas     │  │ • be_analise   │
  └────────────────┘  │ • extracao     │
                      │ • shared       │
                      └────────────────┘
```

### 🔄 Fluxo de Dados

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   📥 ENTRADA │ ───▶ │  ⚙️ PROCESSO │ ───▶ │ 💾 ARMAZENA  │
│              │      │              │      │              │
│ Excel Files  │      │ Notebooks    │      │ Parquet      │
│ (SAPIENS)    │      │ Python ETL   │      │ (70% menos   │
│              │      │ Automatizado │      │  memória)    │
└──────────────┘      └──────────────┘      └──────────────┘
                                                    │
                                                    ▼
                                          ┌──────────────┐
                                          │ 📊 DASHBOARD │
                                          │  Streamlit   │
                                          │  Interativo  │
                                          └──────────────┘
```

### 🛠️ Stack Tecnológico

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  🐍 Python 3.13         Linguagem principal         │
│  🌐 Streamlit           Interface web interativa    │
│  🐼 Pandas              Processamento de dados      │
│  📦 Parquet             Armazenamento otimizado     │
│  📊 Plotly / Altair     Visualizações interativas   │
│  🗃️ SQLite              Câmbio persistido           │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 📈 SLIDE 7: RESULTADOS E ENCERRAMENTO
### ⏱️ 30 segundos

<div align="center">

# 📈 Resultados e Equipe

</div>

---

### ✅ Benefícios Alcançados

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  ⚡ EFICIÊNCIA          De horas para segundos no processamento     │
│  ✅ PRECISÃO            Zero erros manuais — cálculos padronizados  │
│  💡 INSIGHTS            Visualizações claras e comparativas         │
│  🔮 PREVISÕES           Best Estimate com cenários "what-if"        │
│  📈 ESCALABILIDADE      Novos anos/períodos adicionados facilmente  │
│  🔄 RASTREABILIDADE     Versionamento e documentação integrados     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 👥 Equipe

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  👥 EQUIPE                                                          │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                     │
│  👨‍💻 Hudson Cardin              Full-Stack Developer                │
│     Interface + lógica + cálculos do sistema                        │
│                                                                     │
│  👨‍💻 Lauro Paiva                Full-Stack Developer                │
│     Interface + lógica + cálculos do sistema                        │
│                                                                     │
│  👨‍💼 Frederico Cesar de Jesus   Tech Advisor                       │
│     Manufacturing Finance Controller, Stellantis                    │
│     Orientação técnica estratégica e validações                     │
│                                                                     │
│  📚 Documentação: Página 6 do sistema                               │
│  🔢 Versão: Consultar rodapé                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🎤 GUIA DE APRESENTAÇÃO

### ⏱️ Timing Detalhado (5 minutos)

```
SLIDE 1 — O que é o SCI              30 segundos
SLIDE 2 — TC Extendido                1 minuto
SLIDE 3 — TC Veículos                 1 minuto 30 segundos
SLIDE 4 — Funcionalidades compart.    1 minuto 30 segundos
  • Waterfall                         30s
  • Best Estimate                     45s
  • Recursos transversais             15s
SLIDE 5 — Comparativo                 —  (slide de apoio)
SLIDE 6 — Arquitetura                 —  (slide de apoio)
SLIDE 7 — Resultados e equipe         30 segundos
────────────────────────────────────────────────────
TOTAL                                 5 minutos
```

---

### 💡 Dicas para o Apresentador

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  ✅ Demonstre o sistema ao vivo se possível         │
│  ✅ Comece diferenciando TC Ext vs TC Veículos      │
│  ✅ Mostre o rateio por veículo — é o diferencial   │
│  ✅ No Best Estimate, explique a fórmula com o      │
│     exemplo numérico (senão fica abstrato)          │
│  ✅ Se perguntarem sobre variações de TOTAL em CPU, │
│     abra o expander "Volume por período"            │
│  ✅ Enfatize os ganhos de tempo e precisão          │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### ❓ Perguntas Frequentes

**Q: Qual a diferença entre TC Ext e TC Veículos?**
A: TC Ext analisa custo total agregado (com CPU). TC Veículos detalha o custo por modelo de veículo, com cadeia de custos (FA→FP→D&A) e rateio por tempo de produção.

**Q: Como funciona o rateio por veículo?**
A: O custo FP sem Dedicada é distribuído proporcionalmente ao tempo de produção de cada veículo na oficina. D&A Dedicado é somado diretamente ao veículo.

**Q: A inflação só se aplica a custos fixos?**
A: Não. A inflação se aplica a **todos** os custos (fixos e variáveis), após o ajuste por sensibilidade.

**Q: Quanto tempo leva processar novos dados?**
A: Menos de 1 minuto para um ano completo.

**Q: Os dados ficam na nuvem?**
A: Não. Todos os dados ficam no servidor local, sem envio externo.

---

## 🚀 Boa Apresentação!

**Stellantis Cost Intelligence (SCI) — Transformando dados em decisões**

<div align="center">

<img src="SCI_faixa.png" alt="SCI" style="max-width: 900px; width: 100%; height: auto;" />

</div>

![SCI](SCI_faixa.png)
