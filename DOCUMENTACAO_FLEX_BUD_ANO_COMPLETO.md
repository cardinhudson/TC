# 📊 Documentação — Flex Bud (Ano Completo), Governança e Regras de CPU

## 🎯 Objetivo

Manter uma referência clara de:
- como o sistema garante **12 meses** no Flex Bud (ano completo), mesmo com meses sem realizado;
- qual é a governança de **Custo Fixo** (não flexibiliza fora da simulação);
- quais são as **regras críticas de CPU** para evitar divergências em totais;
- como o Real/Budget/Flex Bud deve ser combinado sem “contaminar” o Real.

## 📌 Onde isso está implementado hoje

- TC Ext (Real): `tc_ext/pages/home_ext.py`
- Best Estimate (Análise): `tc_ext/pages/be_analise_ext.py`
- Helper de CPU (padrão do sistema): `tc_ext/metricas_tc_ext.py::cpu_por_chaves()`

Observação: `app.py` é o portal/roteador (menu) e não concentra as regras de cálculo.

---

## ✅ Ano completo (12 meses) — comportamento esperado

- Os gráficos/tabelas podem exibir os **12 meses do ano**.
- Meses sem realizado exibem **Real = 0** (o Real nunca “puxa” Budget).
- Budget e Flex Bud continuam visíveis nesses meses.

### Flex Bud em meses sem realizado

- Se não houver volume real no recorte, o sistema pode usar o **volume do Budget** como base de continuidade.
- Nesses meses, o **Flex Bud tende a ser igual ao Budget** (não há ajuste por volume real).

---

## 🧾 Flex Bud — fórmulas e governança

Para um período/dimensão:

- Flex Fixo: $$Flex_{fixo} = BUD_{fixo}$$
- Flex Não‑Fixo: $$Flex_{naofixo} = BUD_{naofixo} \times \frac{Volume_{real}}{Volume_{bud}}$$
- Flex Total: $$Flex_{total} = Flex_{fixo} + Flex_{naofixo}$$

### Governança: Custo Fixo

- **Custo Fixo não flexibiliza** fora do contexto de simulação.
- Em comparativos Real x Budget/Flex Bud (Home e Best Estimate - Análise), o componente Fixo permanece igual ao Budget.

### Governança: Volume Budget

- O Volume Budget **deve** conter a coluna `Veículo`.
- Se `Veículo` estiver ausente no Volume Budget, isso é **erro de extração** (o app não faz rateio/fallback).

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

## ✅ Checklist rápido (para não errar de novo)

- CPU sempre como razão ponderada (nunca soma/média).
- Volume deve respeitar os mesmos filtros do custo, mas não pode ser “intersectado” pela existência de custo.
- Ao fazer merge custo×volume: preferir `outer` e preencher custo ausente com 0.
- Fixo não flex fora simulação (Flex Bud: fixo = budget fixo).
- Volume Budget precisa conter `Veículo` (sem rateio/fallback no app).

---

**Última atualização:** 25/01/2026
