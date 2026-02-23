# 📊 Documentação — Flex Bud (Ano Completo), Governança e Regras de CPU

## 🎯 Objetivo

Referência clara de:
- Como o sistema garante **12 meses** no Flex Bud (ano completo), mesmo com meses sem realizado
- Governança de **Custo Fixo** (não flexibiliza fora da simulação)
- Regras críticas de **CPU** para evitar divergências em totais
- Como Real/Budget/Flex Bud deve ser combinado sem "contaminar" o Real

## 📌 Onde isso está implementado

- TC Ext (Real): `tc_ext/pages/home_ext.py`
- Best Estimate (Análise): `tc_ext/pages/be_analise_ext.py`
- Helper de CPU: `tc_ext/metricas_tc_ext.py::cpu_por_chaves()`
- TC Veículos: `tc_principal/pages/home_tc.py`

---

## ✅ Ano completo (12 meses)

- Gráficos/tabelas exibem os **12 meses do ano**
- Meses sem realizado: **Real = 0** (nunca "puxa" Budget)
- Budget e Flex Bud continuam visíveis nesses meses

### Flex Bud em meses sem realizado

- Sem volume real → usa **volume Budget** como base
- Nesses meses, **Flex Bud ≈ Budget** (sem ajuste por volume real)

---

## 📐 Fórmulas Detalhadas

### Flex para Real x Real (Waterfall)

```
rho = V_2 / V_1
Flex_Mês1_Fixo = C_1_Fixo
Flex_Mês1_Variável = C_1_Variável × rho
Flex_Mês1_Total = C_1_Fixo + C_1_Variável × (V_2 / V_1)
```

**Em CPU:**
```
BUD_CPU = C_1_Total / V_1
Flex_CPU = Flex_Total / V_2 = (C_1_Fixo / V_2) + (C_1_Variável / V_1)
```

### Flex para Real x Budget (TC Ext / TC Veículos)

```
rho = V_Real / V_Budget
Flex_Bud_Fixo = B_Fixo
Flex_Bud_Variável = B_Variável × rho
Flex_Bud_Total = B_Fixo + B_Variável × (V_Real / V_Budget)
```

**Em CPU:**
```
BUD_CPU = B_Total / V_Budget
Flex_CPU = Flex_Total / V_Real
Total_Real_CPU = R_Total / V_Real
```

### Divisão por zero
- V_Original = 0 → rho = 1,0
- V_Final = 0 → Flex_CPU = 0

---

## 🔒 Governança de Custo Fixo

- Custo Fixo **NUNCA** flexibiliza no cálculo padrão de Flex Bud
- `Flex_Fixo = BUD_Fixo` (sensibilidade = 0%)
- Apenas no **Simulador BE** pode-se atribuir sensibilidade > 0% a fixos

### Classificação Fixo/Variável

```python
mask_fixo = df['Custo'].str.lower().str.startswith('fix')
```

---

## 📏 Regras Críticas de CPU

1. **CPU = Σ Custo / Σ Volume** (APÓS agrupamento)
2. **NUNCA** somar/mediar CPUs de linhas individuais
3. **Fator K/M NÃO** se aplica em modo CPU
4. **Ordem:** agrupar → somar custos → somar volumes → dividir

### Exemplo

| Oficina | Custo | Volume | CPU Individual |
|---------|-------|--------|----------------|
| AS | 100.000 | 10.000 | 10,00 |
| BS | 200.000 | 40.000 | 5,00 |
| **Total** | **300.000** | **50.000** | **6,00** (correto) |

- ❌ Média de CPUs: (10 + 5) / 2 = 7,50
- ✅ CPU correto: 300.000 / 50.000 = 6,00

---

## ⚙️ Aplicação de Fator e Moeda

**Ordem de aplicação:**
1. Aplicar fator de conversão (K/M) — apenas em modo Custo Total
2. Converter moeda (BRL → USD/EUR)
3. Realizar cálculos (CPU, Flex, diferenças)

**Em modo CPU:** fator = Nenhum (1), CPU calculado após conversão de moeda.

---

*📚 SCI — Flex Bud, Governança e CPU*
