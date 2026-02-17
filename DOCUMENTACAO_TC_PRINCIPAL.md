# 🚗 TC Veículos — Documentação (Fonte Única de Verdade)

> **Objetivo**: documentar o módulo **TC Veículos** de forma fiel ao código e sustentável (para uso executivo e técnico).

---

## 1) Resumo Executivo

O **TC Veículos** é o módulo do SCI que explica **quanto custa fabricar cada modelo de veículo** e **por que o custo mudou** (Real vs Budget/Flex, com opção de projeção via Best Estimate).

Ele opera com uma cadeia de custos padronizada e auditável:

$$\text{Despesa Primaria} = \text{Custo FA} + \text{Custo FP}$$

- **Custo FA**: parcela da Despesa Primaria atribuída ao *Fluxo Anexo* (por oficina/período)
- **Custo FP**: parcela remanescente (Fabricação Principal)
- **D\&A dedicado**: depreciação/amortização dedicada (entrada externa)
- **FP sem Dedicada**: base que será rateada por veículo usando tempo de produção
- **Redis**: receita (negativa) vinda da aba **massa - REDIS**, incorporada na própria base e rateada para veículos via tempo

---

## 2) Páginas do Módulo (no app)

Rotas atuais no [app.py](app.py#L1):

- Home (TC Veículos): [tc_principal/pages/home_tc.py](tc_principal/pages/home_tc.py)
- Waterfall: [tc_principal/pages/waterfall_tc.py](tc_principal/pages/waterfall_tc.py)
- Best Estimate (Simulador): [tc_principal/pages/best_estimate_simulador_tc.py](tc_principal/pages/best_estimate_simulador_tc.py)
- Extração de Dados: [tc_principal/pages/extracao_dados_tc.py](tc_principal/pages/extracao_dados_tc.py)
- Debug de Cálculos: [tc_principal/pages/debug_calculos_tc.py](tc_principal/pages/debug_calculos_tc.py)

---

## 3) Contratos de Dados (Parquets)

### 3.1 Estrutura de Pastas (atual)

```
dados/TC_Principal/
├── {ano}/
│   ├── df_principal.parquet
│   ├── df_vol_veiculos.parquet
│   ├── df_volume_fa.parquet
│   ├── df_dea_dedicado.parquet
│   ├── df_tempo_veiculos.parquet
│   ├── df_veiculos_percentual_rateio.parquet
│   ├── df_veiculos_fp_sem_da.parquet
│   ├── df_veiculos_custo_rateado.parquet
│   ├── df_veiculos_custo_fp.parquet
│   ├── df_veiculos_cpu.parquet
│   ├── df_comparativo_real_budget.parquet
│   └── BUD/
│       ├── df_principal_BUD.parquet
│       ├── df_vol_veiculos_BUD.parquet
│       ├── df_vol_veiculos_actual.parquet
│       ├── df_volume_fa_BUD.parquet
│       ├── df_dea_dedicado_BUD.parquet
│       ├── df_tempo_veiculos_BUD.parquet
│       ├── df_veiculos_percentual_rateio_BUD.parquet
│       ├── df_veiculos_fp_sem_da_BUD.parquet
│       ├── df_veiculos_custo_rateado_BUD.parquet
│       ├── df_veiculos_custo_fp_BUD.parquet
│       └── df_veiculos_cpu_BUD.parquet
└── Forecast/
          ├── forecast_completo.parquet
          ├── forecast_previsao.parquet
          ├── forecast_historico.parquet
          ├── df_vol_historico.parquet
          └── custos_especificos.parquet
```

### 3.2 Schema (alto nível)

**Tabela principal** (Real/BUD): grão típico = `(Oficina, Account, Período, Type 05, Type 06, Custo)`.

- Dimensões (principais): `Oficina`, `Período`, `Type 05`, `Type 06`, `Account`, `Custo`
- Métricas monetárias: `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`
- Campo de rastreio: `_fonte_redis` (booleano) quando a linha veio da **massa - REDIS**

**Tabela por veículo** (Real/BUD): grão típico = `(Oficina, Veículo, Account, Período, ...)`.

- Métricas: `Custo Rateado`, `D&A dedicado`, `Custo FP Veiculo` (ou `Custo FP`, dependendo do parquet)

---

## 4) Regras de Cálculo (FP/FA/D\&A)

### 4.1 Rateio FA (por oficina/período)

O sistema calcula `Rateio FA` usando tempo de produção (Tempo FA vs Tempo Veic) e/ou regras manuais por oficina.

### 4.2 Custo FA e Custo FP (split da Despesa Primaria)

$$\text{Custo FA} = \text{Rateio FA} \times \text{Despesa Primaria}$$

$$\text{Custo FP} = \text{Despesa Primaria} - \text{Custo FA}$$

Isso significa que a Despesa Primaria é **particionada** em FA + FP (não somada).

### 4.3 D\&A dedicado e FP sem Dedicada

O `D&A dedicado` entra por planilha e é distribuído:

- A base principal não tem `Veículo`, então o D\&A é agregado por `(Oficina, Account, Período)` e distribuído **pro-rata** por `Custo FP` dentro do grupo.

Depois:

$$\text{FP sem Dedicada} = \text{Custo FP} - \text{D\&A dedicado}$$

---

## 5) Redis (regra crítica)

### 5.1 O que é Redis no sistema

- **Redis não é “uma linha única”**.
- Redis entra como **linhas adicionais** oriundas da aba **massa - REDIS**, com `_fonte_redis=True`.
- Essas linhas carregam `Despesa Primaria` **negativa** (receita), e são concatenadas na base principal.

### 5.2 Como o KPI Redis é calculado

O KPI Redis é a soma da `Despesa Primaria` das linhas com `_fonte_redis=True`.

Observação importante: o helper `extrair_redis()` em [tc_principal/shared.py](tc_principal/shared.py#L43) retorna 0 quando `_fonte_redis` não existe (parquets antigos).

### 5.3 Redis × Oficina × Veículo (como Redis “vira” custo por modelo)

Redis nasce por **Oficina/Período** (e dimensões contábeis) e passa pela cadeia normal:

- No cálculo de FA: para linhas Redis, `Rateio FA` é forçado para 0 (Redis vai integralmente para FP).
- No rateio por veículo: Redis está dentro do `FP sem Dedicada` por oficina/período, então ele é **distribuído para veículos** proporcionalmente ao tempo de produção.

---

## 6) Rateio por Veículo

O rateio por veículo é feito com base no **tempo de produção**:

$$\text{Tempo Veic} = \text{Volume} \times \text{EST}$$

$$\text{Percentual}_{v,o,p} = \frac{\text{Tempo Veic}_{v,o,p}}{\sum_v \text{Tempo Veic}_{v,o,p}}$$

Aplicação:

$$\text{Custo Rateado}_{v,o,p} = \text{FP sem Dedicada}_{o,p} \times \text{Percentual}_{v,o,p}$$

Depois, o D\&A dedicado por veículo é somado:

$$\text{Custo FP Veiculo}_{v,o,p} = \text{Custo Rateado}_{v,o,p} + \text{D\&A dedicado}_{v,o,p}$$

---

## 7) Flex Budget

O **Flex Bud** ajusta apenas a parcela **variável** do Budget pela proporção de volume.

$$\text{Proporção} = \frac{Volume_{Real}}{Volume_{Budget}}$$

$$Flex_{fixo} = BUD_{fixo}$$

$$Flex_{variável} = BUD_{variável} \times \text{Proporção}$$

$$Flex_{total} = Flex_{fixo} + Flex_{variável}$$

---

## 8) CPU (Custo por Unidade) — regra crítica

$$CPU = \frac{\sum \text{Custo}}{\sum \text{Volume}}$$

- CPU **nunca** é somado nem tirado média; sempre recalculado após agregação.
- CPU é uma razão; **não faz sentido aplicar K/M no valor final**.
- No dashboard, o fator K/M é aplicado nas colunas monetárias antes do cálculo; para CPU em unidade monetária “real”, usar `Fator = Nenhum`.

---

## 9) Best Estimate (Simulador)

Página: [tc_principal/pages/best_estimate_simulador_tc.py](tc_principal/pages/best_estimate_simulador_tc.py)

- Gera Forecast a partir de histórico + premissas (sensibilidade e inflação) e volume projetado.
- Salva outputs em `dados/TC_Principal/Forecast/` (ex.: `forecast_completo.parquet`, `df_vol_historico.parquet`).

---

## 10) Debug de Cálculos (auditoria)

Página: [tc_principal/pages/debug_calculos_tc.py](tc_principal/pages/debug_calculos_tc.py)

Usada para:

- Conferir parquets existentes (Real/BUD) e seus shapes
- Validar fechamentos (ex.: $\sum \text{Custo Rateado} \approx \sum \text{FP sem Dedicada}$)
- Conferir que Redis está marcado por `_fonte_redis` e que `Rateio FA=0` nessas linhas

---

*TC Veículos — documentação alinhada ao código do repositório.*
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
