# 📊 Alterações no Sistema - Flex Bud Ano Completo

## 🎯 Objetivo

Modificar o sistema para mostrar o **Flex Bud para o ano completo** (todos os 12 meses), mesmo quando não houver dados realizados para alguns meses.

## ✅ Problema Resolvido

**ANTES:**
- ❌ Gráficos mostravam apenas meses com dados reais
- ❌ Flex Bud não era calculado para meses sem realização
- ❌ Comparação anual incompleta

**AGORA:**
- ✅ Gráficos mostram todos os 12 meses do ano
- ✅ Flex Bud calculado para o ano completo
- ✅ Meses sem dados reais exibem **Realizado = 0** (não “puxa” Budget para o Real)
- ✅ Meses sem dados reais continuam permitindo cálculo/visualização de **Budget e Flex Bud**
- ✅ Comparação anual completa e consistente

## 🔧 Alterações Técnicas

### 1. Cálculo do Flex Bud
**Arquivo:** `app.py` (linhas ~2398-2413 e ~2537-2552)

**Mudança principal:**
```python
# ANTES
volumes = volumes[volumes['Volume_real'].notna() & (volumes['Volume_real'] > 0)]

# AGORA
volumes['Volume_real'] = volumes['Volume_real'].fillna(volumes['Volume_budget'])
volumes = volumes[(volumes['Volume_real'] > 0) | (volumes['Volume_budget'] > 0)]
```

**Resultado:** Períodos sem volume real podem usar o volume do Budget como base para manter o ano completo.

### 2. Filtro de Períodos nos Gráficos
**Arquivo:** `app.py` (linhas ~3607-3642)

**Mudança principal:**
```python
# ANTES: Incluía apenas meses desde o primeiro com despesa
for periodo in ORDEM_MESES[idx_primeiro:]

# AGORA: Inclui TODOS os 12 meses
for periodo in ORDEM_MESES
```

**Resultado:** Todos os meses do ano são incluídos na visualização

### 3. Filtro de Budget
**Arquivo:** `app.py` (linhas ~3804-3870)

**Mudança principal:** Mesma lógica aplicada aos dados de Budget para garantir consistência
# 📊 Documentação — Flex Bud (Ano Completo) e Regras de CPU

## 🎯 Objetivo

Manter uma referência clara de:
- como o sistema garante **12 meses** no Flex Bud (ano completo), mesmo com meses sem realizado;
- quais são as **regras críticas de CPU** que garantem que gráficos e tabelas “fechem” sempre;
- como evoluir a **previsão (Forecast/Best Estimate)** para comparar também com **Budget**.

## 📌 Onde isso está implementado hoje

- TC Ext (Real): `tc_ext/pages/home_ext.py`
- Best Estimate (Análise): `tc_ext/pages/be_analise_ext.py`
- Helper de CPU (padrão do sistema): `tc_ext/metricas_tc_ext.py::cpu_por_chaves()`

Observação: `app.py` é o portal/roteador (menu), não concentra a regra de cálculo.

---

## ✅ Ano completo (12 meses) — como funciona

**Problema original**
- Os gráficos acabavam mostrando apenas meses com dados reais.

**Comportamento esperado**
- Os gráficos podem exibir os **12 meses do ano**.
- Meses sem realizado exibem **Real = 0** (o Real nunca “puxa” Budget).
- Budget e Flex Bud continuam visíveis nesses meses.

**Flex Bud em meses sem realizado**
- Se não houver volume real, o sistema pode usar o **volume do Budget** como base.
- Isso faz com que, nesses meses, o **Flex Bud tenda a ser igual ao Budget** (não há ajuste por volume real).

---

## 🧮 CPU (Custo por Unidade) — regra que evita divergência

CPU é sempre calculado como razão ponderada:

$$CPU = \frac{\sum Total}{\sum Volume}$$

Regras críticas:
- Nunca somar/média de CPU (nem em totais).
- Totais em tabelas e gráficos devem usar $\sum Total / \sum Volume$ no mesmo recorte.
- Ao combinar custo e volume, preservar linhas que existam apenas no volume (custo = 0) usando `merge how='outer'`.

Helper recomendado:
- `cpu_por_chaves(...)` em `tc_ext/metricas_tc_ext.py`.

---

## 🔮 Previsão (Forecast / Best Estimate) — possibilidade de comparação com Budget

O sistema tem páginas de Forecast/Best Estimate (simulação + análise).

Evolução recomendada (para consulta/planejamento):
- permitir baseline “**Budget**” além de “**Flex Bud**” nas análises de previsão.

Regras para CPU permanecem as mesmas:
- sempre $CPU = \sum Total / \sum Volume$ no mesmo grão;
- se o volume do Budget não tiver `Veículo`, usar **rateio** (alocação) por share de volume real para estimar volume budget por veículo.

---

## ✅ Checklist rápido (para não errar de novo)

- CPU sempre como razão ponderada (nunca soma/média).
- Volume deve respeitar os mesmos filtros do custo, mas não pode ser “intersectado” pela existência de custo.
- Ao fazer merge custo×volume: preferir `outer` e preencher custo ausente com 0.
- Em Budget CPU por Veículo sem volume por veículo: usar rateio por share de volume real.

---

**Última atualização:** 23/01/2026
1. ✅ Capitalização dos períodos corrigida (minúsculas → Capitalizadas)
