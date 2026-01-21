# 📊 Sistema TC Extendido
## Apresentação Visual - 5 Minutos

<div align="center">

**Desenvolvido por:** Hudson Cardin e Lauro Paiva  
**Versão:** Sistema completo com versionamento automático  
**Data:** 2026

---

</div>

---

## 🎯 SLIDE 1: INTRODUÇÃO
### ⏱️ 30 segundos

<div align="center">

# 📊 Sistema TC Extendido

### Sistema de Análise de Custos e Previsões para Porto Real

</div>

---

### 🎯 O que é o Sistema TC?

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  📊 Dashboard Interativo                            │
│     └─ Múltiplas análises em tempo real             │
│                                                     │
│  ⚙️  Processamento Automatizado                     │
│     └─ Transforma dados brutos em insights          │
│                                                     │
│  🔮 Previsões Inteligentes (Best Estimate)          │
│     └─ Forecast baseado em dados históricos         │
│                                                     │
│  📈 Análise Comparativa                             │
│     └─ Real vs Budget com ajustes por volume        │
│                                                     │
└─────────────────────────────────────────────────────┘
```

**🎯 Objetivo Principal:**
> Transformar dados brutos em **insights acionáveis** para tomada de decisão estratégica.

---

## 🔍 SLIDE 2: PROBLEMA E NECESSIDADE
### ⏱️ 30 segundos

<div align="center">

# ❌ ANTES vs ✅ AGORA

</div>

---

### 📉 Desafios Anteriores

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  ❌ ANTES                                           |
│  ─────────────────────────────────────────────────  │
│                                                     │
│  📝 Processamento Manual                            │
│     ⏱️  Horas de trabalho manual                    │ 
│     ❌ Propenso a erros                             │
│                                                      │
│  📊 Análises Demoradas                              │
│     ⏱️  Dias para gerar relatórios                  │
│     ❌ Dificuldade em comparar períodos             │
│                                                     │
│  🔮 Sem Previsões                                   │
│     ❌ Falta de estrutura para forecast             │
│     ❌ Decisões baseadas em intuição                │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### ✅ Solução Implementada

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  ✅ AGORA                                           │
│  ───────────────────────────────────────────────── │
│                                                     │
│  ⚡ Processamento Automatizado                      │
│     ⚡ Segundos para processar                       │
│     ✅ Zero erros manuais                           │
│                                                     │
│  📊 Análises em Tempo Real                          │
│     ⚡ Resultados instantâneos                      │
│     ✅ Comparações automáticas                      │
│                                                     │
│  🔮 Previsões Estruturadas                          │
│     ✅ Baseadas em dados históricos                 │
│     ✅ Cenários "what-if" em tempo real             │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🏗️ SLIDE 3: ARQUITETURA DO SISTEMA
### ⏱️ 1 minuto

<div align="center">

# 🏗️ Arquitetura Modular

</div>

---

### 📐 Estrutura do Sistema (Portal TC)

```
                    ┌─────────────────────┐
                    │   📊 app.py         │
                    │   Portal / Router   │
                    │   (st.navigation)   │
                    └──────────┬──────────┘
                               │
                ┌──────────────┼───────────────┬──────────────┐
                │              │               │              │
        ┌───────▼────────┐ ┌───▼─────────┐ ┌──▼──────────┐
        │ 🧩 TC Ext       │ │ 🏭 TC (PP)   │ │ 📚 Doc      │
        │ Home + análises │ │ (stubs)      │ │ Única       │
        └─────────────────┘ └─────────────┘ └─────────────┘
```

---

### 🔄 Fluxo de Dados

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   📥 ENTRADA │ ───▶ │  ⚙️ PROCESSO │ ───▶ 💾 ARMAZENA │
│              │      │              │      │              │
│ Excel Files  │      │ Notebooks    │      │ Parquet      │
│ (SAPIENS)    │      │ Python       │      │ (70% menos   │
│ (Reporting)  │      │ Automatizado │      │  memória)    │
└──────────────┘      └──────────────┘      └──────────────┘
                                                      │
                                                      ▼
                                            ┌──────────────┐
                                            │ 📊 VISUALIZA │
                                            │              │
                                            │ Dashboard    │
                                            │ Streamlit    │
                                            │ Interativo   │
                                            └──────────────┘
```

---

### 🛠️ Stack Tecnológico

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  🐍 Python 3.13                                     │
│     └─ Linguagem principal do sistema              │
│                                                     │
│  🌐 Streamlit                                       │
│     └─ Interface web interativa e responsiva       │
│                                                     │
│  🐼 Pandas                                          │
│     └─ Processamento eficiente de grandes volumes  │
│                                                     │
│  📦 Parquet                                         │
│     └─ Armazenamento otimizado (70% menos memória) │
│                                                     │
│  📊 Plotly / Altair                                 │
│     └─ Visualizações avançadas e interativas       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## ⚡ SLIDE 4: PRINCIPAIS FUNCIONALIDADES
### ⏱️ 2 minutos

<div align="center">

# ⚡ Funcionalidades Principais

</div>

---

### 1️⃣ 📈 Waterfall - Análise de Variações
### ⏱️ 30 segundos

```
┌────────────────────────────────────────────────────┐
│                                                    │
│  📈 WATERFALL ANALYSIS                             │
│  ───────────────────────────────────────────────── │
│                                                    │
│  🔍 O que faz:                                     │
│     • Compara períodos (Mês 1 vs Mês 2)            │
│     • Identifica variações de custos               │
│     • Calcula Flex Bud (ajuste por volume)         │
│     • Visualiza impactos linha a linha             │
│                                                    │
│  ⭐ Destaque:                                      │
│     Gráficos waterfall interativos mostrando       │
│     exatamente onde os custos variaram             │
│                                                    │
│  📊 Visualização:                                 │
│     ┌───────────────────────────────────────┐      │
│     │  Mês 1  ──▶  Variação  ──▶  Mês 2   │       │ 
│     │   100         +20           120      │       │
│     └───────────────────────────────────────┘      │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

### 2️⃣ 🔮 Best Estimate - Previsões Inteligentes
### ⏱️ 45 segundos

```
┌────────────────────────────────────────────────────┐
│                                                    │
│  🔮 BEST ESTIMATE                                  │
│  ───────────────────────────────────────────────── │
│                                                    │
│  🎯 O que faz:                                     │
│     • Calcula previsões baseadas em médias         │
│     • Aplica sensibilidade (Fixo vs Variável)      │
│     • Considera inflação e variação de volume      │
│     • Gera forecasts para períodos futuros         │
│                                                    │
│  🧮 Fórmula:                                       │
│     ┌───────────────────────────────────────┐      │
│     │  Média Histórica                      │      │
│     │      ×                                │      │
│     │  Fator Volume                         │      │
│     │      ×                                │      │
│     │  Fator Inflação                       │      │
│     │      =                                │      │
│     │  Best Estimate                        │      │
│     └───────────────────────────────────────┘      │
│                                                    │
│  ⭐ Destaques:                                     │
│     🔮 Simulador: Testa cenários "what-if"         │
│        → gera `dados/Forecast/*.parquet`           │
│     📊 Análise: layout da Home + dados Forecast    │
│        → total em CPU é ponderado por Volume       │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

### 3️⃣ 📥 Extração e Processamento de Dados
### ⏱️ 30 segundos

```
┌────────────────────────────────────────────────────┐
│                                                    │
│  📥 EXTRAÇÃO DE DADOS                              │
│  ───────────────────────────────────────────────── │
│                                                    │
│  🔄 Processo:                                      │
│     ┌───────────┐    ┌───────────┐    ┌───────────┐│
│     │ Upload    │───▶│ Processa  │───▶│ Consolida││
│     │ Excel     │    │ Notebooks │    │ Histórico ││
│     └───────────┘    └───────────┘    └───────────┘│
│                                                    │
│  ✅ Funcionalidades:                               │
│     • Upload automatizado de arquivos Excel        │
│     • Processamento via notebooks Python           │
│     • Consolidação histórica automática            │
│     • Validação de dados                           │
│                                                    │
│  ⚡ Performance:                                   │
│     Interface simples que processa                 │
│     milhares de linhas em segundos                 │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

### 4️⃣ 📊 Análises Comparativas
### ⏱️ 15 segundos

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  📊 REAL vs BUDGET                                  │
│  ───────────────────────────────────────────────── │
│                                                     │
│  🔍 Comparações:                                    │
│     • Planejado vs Realizado                        │
│     • Identificação de desvios                      │
│     • Flex Bud ajustado por volume                  │
│                                                     │
│  📈 Dimensões de Análise:                           │
│     • Oficina                                       │
│     • Veículo                                       │
│     • Type 05                                       │
│     • Type 06                                       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🎨 SLIDE 5: DESTAQUES TÉCNICOS
### ⏱️ 1 minuto

<div align="center">

# 🎨 Destaques Técnicos

</div>

---

### ⚡ Performance e Otimização

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  📊 MÉTRICAS DO SISTEMA                             │
│  ───────────────────────────────────────────────── │
│                                                     │
│  💻 20.000+ linhas de código                        │
│     └─ Bem estruturadas e documentadas              │
│                                                     │
│  💾 70% redução de memória                          │
│     └─ Formato Parquet otimizado                    │
│                                                     │
│  ⚡ Cache inteligente                               │
│     └─ Consultas rápidas e eficientes               │
│                                                     │
│  🔄 Versionamento automático                        │
│     └─ Versão incrementa quando páginas mudam       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### 🌟 Funcionalidades Avançadas

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  🔄 MULTI-MOEDA                                     │
│     R$ • USD • EUR                                  │
│     Conversão em tempo real                         │
│                                                     │
│  📅 FILTROS DINÂMICOS                               │
│     Ano • Período • Oficina • Veículo               │
│                                                     │
│  📊 VISUALIZAÇÕES INTERATIVAS                        │
│     Gráficos que respondem aos filtros              │
│                                                     │
│  📥 EXPORTAÇÃO                                      │
│     Excel • Parquet • Múltiplos formatos           │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### 🔄 Sistema de Versionamento

```
┌────────────────────────────────────────────────────┐
│                                                    │
│  🔄 VERSIONAMENTO AUTOMÁTICO                       │
│  ───────────────────────────────────────────────── │
│                                                    │
│  📈 Sequência:                                     │
│     1.0 → 1.01 → 1.02 → ... → 1.09                 │
│     → 1.1 → 1.11 → 1.12 → ...                      │
│                                                    │
│  🔍 Detecção Automática:                           │
│     • Monitora mudanças nas páginas                │
│     • Incrementa versão automaticamente            │
│     • Mantém histórico completo                    │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 📈 SLIDE 6: RESULTADOS E IMPACTO
### ⏱️ 30 segundos

<div align="center">

# 📈 Resultados Alcançados

</div>

---

### ✅ Benefícios Mensuráveis

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  ⚡ EFICIÊNCIA                                     │
│  ─────────────────────────────────────────────────  │
│     • 90% redução no tempo de processamento         │
│     • Horas → Segundos                              │
│                                                     │
│  ✅ PRECISÃO                                        │
│  ─────────────────────────────────────────────────  │
│     • Zero erros manuais                            │
│     • Cálculos padronizados e validados             │
│                                                     │
│  💡 INSIGHTS                                        │
│  ─────────────────────────────────────────────────  │
│     • Visualizações claras e acionáveis             │
│     • Previsões baseadas em dados históricos        │
│                                                     │
│  📈 ESCALABILIDADE                                  │
│  ─────────────────────────────────────────────────  │
│     • Preparado para crescimento                    │
│     • Fácil adição de novos anos e períodos         │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🎯 SLIDE 7: CONCLUSÃO
### ⏱️ 30 segundos

<div align="center">

# 🎯 Conclusão

</div>

---

### 📋 Resumo Executivo

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  🎯 SISTEMA TC EXTENDIDO                            │
│  ─────────────────────────────────────────────────  │
│                                                     │
│  ✅ Automatiza                                      │
│     Processamento de dados financeiros              │
│                                                     │
│  ✅ Facilita                                        │
│     Análises comparativas e previsões               │
│                                                     │
│  ✅ Otimiza                                         │
│     Uso de recursos (memória, tempo)                │
│                                                     │
│  ✅ Documenta                                       │
│     Todas as funcionalidades e regras               │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### 🚀 Próximos Passos

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  📅 Expansão para novos anos                        │
│  🔄 Melhorias contínuas baseadas em feedback        │
│  🔗 Integração com novos sistemas                   │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 📞 CONTATO E INFORMAÇÕES

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  👥 DESENVOLVIDO POR:                               │
│     • Hudson Cardin                                 │
│     • Lauro Paiva                                   │
│                                                     │
│  📚 DOCUMENTAÇÃO:                                   │
│     Disponível na página 6 do sistema               │
│                                                     │
│  🔢 VERSÃO ATUAL:                                   │
│     Consultar rodapé do sistema                     │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🎤 GUIA DE APRESENTAÇÃO

### ⏱️ Timing Detalhado (5 minutos total)

```
SLIDE 1 - Introdução            30 segundos
SLIDE 2 - Problema  30 segundos    │
SLIDE 3 - Arquitetura  1 minuto       │
SLIDE 4 - Funcionalidades  2 minutos      │
• Waterfall  30s            │
• Best Estimate  45s            │
• Extração  30s            │
• Comparações  15s            │
SLIDE 5 - Destaques  1 minuto       │
SLIDE 6 - Resultados  30 segundos    │
SLIDE 7 - Conclusão  30 segundos    │
────────────────────────────────────────────────────
TOTAL  5 minutos      ```

---

### 💡 Pontos de Atenção

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│  ✅ Demonstre o sistema ao vivo se possível         │
│  ✅ Destaque a facilidade de uso                    │
│  ✅ Mencione a documentação completa                │
│  ✅ Enfatize os ganhos de tempo e precisão          │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### ❓ Perguntas Frequentes

**Q: Quanto tempo leva para processar novos dados?**  
A: Depende do volume, mas geralmente menos de 1 minuto para um ano completo.

**Q: É possível adicionar novos filtros?**  
A: Sim, o sistema é modular e facilmente extensível.

**Q: Os dados são seguros?**  
A: Sim, todos os dados ficam no servidor local, sem envio para nuvem externa.

**Q: Como funciona o versionamento?**  
A: A versão incrementa automaticamente quando qualquer página é modificada.

---

## 🚀 Boa Apresentação!

**Sistema TC Extendido - Transformando dados em decisões**
