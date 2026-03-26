# Relatório de Performance — Home TC Veículos (Modo BE)

## Resumo da Otimização

**Problema**: No modo Best Estimate, a página Home carrega eagerly o `forecast_completo.parquet`
(25.030 linhas × 27 colunas, 1.241 KB) para renderizar o gráfico macro de Custo FP por Período,
que precisa de apenas 7 colunas agregadas.

**Solução**: Estratégia **dual-loader** com parquet otimizado `forecast_agg.parquet`
(93 linhas × 7 colunas, 5,9 KB) para a renderização macro inicial, e lazy-load do
forecast completo apenas quando tabs de detalhe (Tab3 Flex, Tab5 Detalhado) são acessadas.

---

## Métricas — Antes vs Depois

| Métrica                          | Antes (FULL)      | Depois (AGG + lazy) | Redução        |
|----------------------------------|-------------------|---------------------|----------------|
| Linhas carregadas (eager)        | 25.030            | 93                  | **99,6%**      |
| Colunas carregadas (eager)       | 27                | 7                   | **74,1%**      |
| Tamanho parquet (eager)          | 1.241 KB          | 5,9 KB              | **99,5%**      |
| Memória DataFrame (estimada)     | ~5,2 MB           | ~6 KB               | **~99,9%**     |
| Tabs que usam AGG (rápido)       | 0/5               | 1/5 (Tab1 consol.)  | —              |
| Tabs que precisam FULL (lazy)    | 5/5               | 3/5                 | —              |

### Detalhamento por Tab

| Tab                   | Dados necessários            | Fonte          | Impacto              |
|-----------------------|------------------------------|----------------|----------------------|
| Tab1 Consolidado      | Período, Tipo, Custo FP      | **AGG (93 r)** | ✅ Render imediato   |
| Tab1 Veículo          | + Veículo, rateio            | FULL (lazy)    | Sem mudança          |
| Tab2 Volume           | Sem BE                       | N/A            | Sem mudança          |
| Tab3 Flex             | + Type 06, Custo             | FULL (lazy)    | Sem mudança          |
| Tab5 Detalhado        | Todas as colunas             | FULL (lazy)    | Sem mudança          |

---

## Arquitetura da Solução

```
Fluxo Early Load (toda renderização):
  _load_forecast() → load_forecast_agg() → read_optimized(prefer='agg')
    ↓ SCI_USE_OPTIMIZED_PARQUETS=true?
    ├─ Sim → forecast_agg.parquet (93 rows, 5.9 KB)
    └─ Não → forecast_completo.parquet (25K rows, fallback seguro)

Fluxo Lazy Load (apenas se Tab1-veículo / Tab3 / Tab5 ativados):
  _get_be_full() → _load_forecast_full() → load_forecast_completo()
    → forecast_completo.parquet (25K rows, carrega sob demanda)
```

### Componentes modificados

| Arquivo                          | Mudança                                    |
|----------------------------------|--------------------------------------------|
| `tc_principal/shared.py`         | + `load_forecast_agg()` (prefer='agg')     |
| `tc_principal/pages/home_tc.py`  | Dual-loader: AGG eager + FULL lazy         |
| `tc_core/feature_flags.py`       | + `SCI_DEBUG_PERF_TRACE` flag              |
| `tests/test_forecast_agg.py`     | 7 novos testes de routing e fallback       |

---

## Feature Flags

| Flag                           | Default   | Efeito                                    |
|--------------------------------|-----------|-------------------------------------------|
| `SCI_USE_OPTIMIZED_PARQUETS`   | `false`   | Habilita roteamento para parquets AGG     |
| `SCI_DEBUG_PERF_TRACE`         | `false`   | Exibe painel ⏱️ Perf Trace no dashboard  |

Para ativar: definir variáveis de ambiente ou em `app.yaml` (Databricks).

---

## Otimizações Adicionais (v2.31)

### Fix 0 — Ativação do roteamento local
Os scripts `ativar_ambiente.ps1` e `.bat` agora definem `SCI_USE_OPTIMIZED_PARQUETS=true`
automaticamente ao ativar o venv. Antes, essa flag só era `true` no Databricks (via `app.yaml`),
então **todo o desenvolvimento local usava parquets FULL** mesmo que AGG/THIN existissem.

### Fix 1 — Debug panel lazy import
O `render_data_trace_panel()` era importado **incondicionalmente** em toda renderização de página.
Agora o import só ocorre quando `SCI_DEBUG_DATA_TRACE=true`, eliminando overhead de import desnecessário.

### Fix 2 — Cache TTL: 60s → 300s (forecast: 120s)
Os 34 loaders em `shared.py` tinham `ttl=60`, causando releitura de disco a cada minuto.
- **Dados estáveis** (Budget, Real, Histórico): `ttl=300` (5 min)
- **Dados dinâmicos** (Forecast/BE): `ttl=120` (2 min) — balanceio entre freshness e performance.

### Fix 3 — show_spinner=False
Todos os `@st.cache_data` agora usam `show_spinner=False`, eliminando o "bonequinho carregando"
que aparecia em releituras rápidas de cache (sub-segundo mas visualmente travava a UI).

### Resumo de impacto

| Fix | Descrição               | Impacto esperado                          |
|-----|-------------------------|-------------------------------------------|
| #0  | Routing local ativado   | AGG/THIN parquets usados no dev local     |
| #1  | Debug panel condicional | ~50ms economia por page render            |
| #2  | TTL 60→300s / 120s      | 5x menos releituras de disco por sessão   |
| #3  | show_spinner=False      | UI não trava em cache hits rápidos        |

---

## Testes

- **99/99** testes passando (7 novos + 92 existentes)
- Validação de somas: `forecast_agg` somas == `forecast_completo` somas (diff = 0.0000)
- Fallback testado: se forecast_agg.parquet não existe → lê FULL transparentemente

---

## Como medir em produção

1. Definir `SCI_DEBUG_PERF_TRACE=true` em `app.yaml`
2. Acessar a Home no modo BE
3. O expander **⏱️ Perf Trace — Early Load** aparece antes das tabs
4. Comparar tempo de `_load_forecast (AGG)` vs cenário anterior

---

*Gerado em: v2.31 — Otimização Home BE + Performance Fixes*
