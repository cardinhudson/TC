# 📊 Sistema TC Extendido - Apresentação 5 Minutos

**Desenvolvido por:** Hudson Cardin e Lauro Paiva  
**Versão:** Sistema completo com versionamento automático  
**Data:** 2026

---

## 🎯 Slide 1: Introdução (30 segundos)

### O que é o Sistema TC?

**Sistema de Análise de Custos e Previsões** para Porto Real

- **Dashboard interativo** com múltiplas análises
- **Processamento automatizado** de dados financeiros
- **Previsões inteligentes** (Best Estimate)
- **Análise comparativa** Real vs Budget

**Objetivo:** Transformar dados brutos em insights acionáveis para tomada de decisão estratégica.

---

## 🔍 Slide 2: Problema e Necessidade (30 segundos)

### Desafios Resolvidos

❌ **Antes:**
- Processamento manual de planilhas Excel
- Análises demoradas e propensas a erros
- Dificuldade em comparar períodos
- Falta de previsões estruturadas

✅ **Agora:**
- Processamento automatizado
- Análises em tempo real
- Comparações instantâneas
- Previsões baseadas em dados históricos

### 🆕 Por que adicionamos novas funcionalidades?

- **Cobrir lacunas de dados:** algumas oficinas têm pouco/nenhum realizado, mas possuem **Budget (BUD)** completo.
- **Melhorar comparabilidade:** permitir previsões e análises coerentes também a partir do **planejado (Budget)**.
- **Aumentar robustez:** evitar filtros/gráficos vazios quando a oficina só existe no Budget.
- **Preparar expansão:** Portal único já separa **TC Ext** e **TC Veículos** com páginas funcionais.
- **Padronizar cálculo:** Best Estimate (Análise) segue o padrão visual/lógico da Home e mantém coerência no modo **CPU** (Total/Volume).

---

## 🏗️ Slide 3: Arquitetura do Sistema (1 minuto)

### Estrutura Modular

```
📊 Portal (app.py) — Router (st.navigation)
    │
    ├── 🧩 TC Ext (Linhas Secundárias)
    │     ├── 🏠 Home: tc_ext/pages/home_ext.py
    │     ├── 📈 Waterfall: pages/1 - Waterfall.py
    │     ├── 🔮 Best Estimate (Simulador): pages/2 - Best Estimate - Simulador.py
    │     ├── 📊 Best Estimate (Análise): tc_ext/pages/be_analise_ext.py
    │     └── 📥 Extração de Dados: pages/5 - Extração de Dados.py
    │
    ├── 🏭 TC Veículos — páginas funcionais
    │     ├── Home: tc_principal/pages/home_tc.py
    │     ├── Waterfall: tc_principal/pages/waterfall_tc.py
    │     ├── Best Estimate (Simulador): tc_principal/pages/best_estimate_simulador_tc.py
    │     ├── Best Estimate (Análise): tc_principal/pages/best_estimate_analise_tc.py
    │     └── Extração de Dados: tc_principal/pages/extracao_dados_tc.py
    │
    └── 📚 Documentação: pages/6 - Documentacao.py
```

### Camadas Internas (o que sustenta as páginas)

- **tc_core/**: helpers compartilhados (paths, moeda, DB de câmbio, UI)
- **tc_ext/**: normalização e métricas (ex.: CPU) + páginas do TC Ext
- **dados/**: histórico consolidado + Budget (BUD) + outputs do Forecast

### Fluxo de Dados

1. **Entrada:** Arquivos Excel (SAPIENS, Reporting)
2. **Processamento:** Notebooks Python automatizados
3. **Armazenamento:** Parquet otimizado (70% menos memória)
4. **Visualização:** Dashboard Streamlit interativo

### Tecnologias Principais

- **Python 3.13** - Linguagem principal
- **Streamlit** - Interface web interativa
- **Pandas** - Processamento de dados
- **Parquet** - Armazenamento otimizado
- **Plotly/Altair** - Visualizações avançadas

---

## ⚡ Slide 4: Principais Funcionalidades (2 minutos)

### 1. 📈 Waterfall - Análise de Variações (30s)

**O que faz:**
- Compara períodos (Mês 1 vs Mês 2)
- Identifica variações de custos
- Calcula Flex Bud (ajuste por volume)
- Visualiza impactos linha a linha

**Destaque:** Gráficos waterfall interativos mostrando exatamente onde os custos variaram.

---

### 2. 🔮 Best Estimate - Previsões Inteligentes (45s)

**O que faz:**
- Calcula previsões baseadas em médias históricas
- Aplica sensibilidade (Fixo vs Variável)
- Considera inflação e variação de volume
- Gera forecasts para períodos futuros

**Como funciona:**
```
Média Histórica × Fator Volume × Fator Inflação = Best Estimate
```

**Destaque:** 
- **Simulador:** Testa cenários "what-if" em tempo real
- **Análise:** Visualizações detalhadas e comparações

**Novidade importante:**
- **Referência por oficina (Realizado x Budget):** ao marcar oficinas, o Best Estimate passa a usar **Budget mês-a-mês** como base (jan com jan, etc.).
- **Cobertura de oficinas só no Budget:** o simulador consegue “semear” dimensões do BUD para gerar Forecast mesmo sem histórico.

---

### 3. 📥 Extração e Processamento de Dados (30s)

**O que faz:**
- Upload automatizado de arquivos Excel
- Processamento via notebooks Python
- Consolidação histórica automática
- Validação de dados

**Destaque:** Interface simples que processa milhares de linhas em segundos.

---

### 4. 📊 Análises Comparativas (15s)

**Real vs Budget:**
- Compara o que foi planejado vs realizado
- Identifica desvios
- Calcula Flex Bud ajustado por volume
- Análise por múltiplas dimensões (Oficina, Veículo, Type 05, Type 06)

---

## 🎨 Slide 5: Destaques Técnicos (1 minuto)

### Performance e Otimização

- ✅ **20.000+ linhas de código** bem estruturadas
- ✅ **70% redução de memória** com formato Parquet
- ✅ **Cache inteligente** para consultas rápidas
- ✅ **Versionamento automático** - versão incrementa quando páginas são modificadas

### Funcionalidades Avançadas

- 🔄 **Multi-moeda:** R$, USD, EUR com conversão em tempo real
- 📅 **Filtros dinâmicos:** Por ano, período, oficina, veículo
- 📊 **Visualizações interativas:** Gráficos que respondem aos filtros
- 📥 **Exportação:** Excel, Parquet, múltiplos formatos

### Sistema de Versionamento

- Versão automática: `1.0 → 1.01 → 1.02 → ... → 1.09 → 1.1 → 1.11`
- Detecta mudanças nas páginas automaticamente
- Histórico completo de versões

---

## 📈 Slide 6: Resultados e Impacto (30 segundos)

### Benefícios Alcançados

✅ **Eficiência:**
- Redução de 90% no tempo de processamento
- Análises que antes levavam horas, agora em segundos

✅ **Precisão:**
- Eliminação de erros manuais
- Cálculos padronizados e validados

✅ **Insights:**
- Visualizações claras e acionáveis
- Previsões baseadas em dados históricos

✅ **Escalabilidade:**
- Sistema preparado para crescimento
- Fácil adição de novos anos e períodos

---

## 🎯 Slide 7: Conclusão (30 segundos)

### Resumo

**Sistema TC Extendido** é uma solução completa que:

1. **Automatiza** o processamento de dados financeiros
2. **Facilita** análises comparativas e previsões
3. **Otimiza** o uso de recursos (memória, tempo)
4. **Documenta** todas as funcionalidades e regras

### Próximos Passos

- Expansão para novos anos
- Melhorias contínuas baseadas em feedback
- Integração com novos sistemas

---

## 📞 Contato e Informações

**Desenvolvido por:**
- Hudson Cardin
- Lauro Paiva

**Documentação Completa:** Disponível na página 6 do sistema  
**Versão Atual:** Consultar rodapé do sistema

---

## 🎤 Dicas para Apresentação

### Timing Sugerido (5 minutos total):

1. **Slide 1 - Introdução:** 30s
2. **Slide 2 - Problema:** 30s
3. **Slide 3 - Arquitetura:** 1min
4. **Slide 4 - Funcionalidades:** 2min
   - Waterfall: 30s
   - Best Estimate: 45s
   - Extração: 30s
   - Comparações: 15s
5. **Slide 5 - Destaques:** 1min
6. **Slide 6 - Resultados:** 30s
7. **Slide 7 - Conclusão:** 30s

### Pontos de Atenção:

- ✅ **Demonstre** o sistema ao vivo se possível
- ✅ **Destaque** a facilidade de uso
- ✅ **Mencione** a documentação completa
- ✅ **Enfatize** os ganhos de tempo e precisão

### Perguntas Frequentes (Prepare-se):

**Q: Quanto tempo leva para processar novos dados?**  
A: Depende do volume, mas geralmente menos de 1 minuto para um ano completo.

**Q: É possível adicionar novos filtros?**  
A: Sim, o sistema é modular e facilmente extensível.

**Q: Os dados são seguros?**  
A: Sim, todos os dados ficam no servidor local, sem envio para nuvem externa.

**Q: Como funciona o versionamento?**  
A: A versão incrementa automaticamente quando qualquer página é modificada.

---

**Boa apresentação! 🚀**
