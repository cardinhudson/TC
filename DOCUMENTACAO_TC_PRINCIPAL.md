# ðŸš— TC VeÃ­culos â€” DocumentaÃ§Ã£o Completa (Fonte Ãšnica de Verdade)

> **Objetivo**: documentar o mÃ³dulo **TC VeÃ­culos** de forma completa e fiel ao cÃ³digo, para uso executivo, tÃ©cnico e pelo chatbot de IA.

---

## 1) Resumo Executivo e Objetivos do Projeto

O **Stellantis Cost Intelligence (SCI)** Ã© uma plataforma de anÃ¡lise de custos industriais composta por dois mÃ³dulos complementares:

**ðŸš— TC VeÃ­culos (TC Principal)**
- Cadeia completa: Despesa PrimÃ¡ria â†’ Custo FA â†’ Custo FP â†’ D&A â†’ FP sem Dedicada
- Rateio proporcional por veÃ­culo (tempo de produÃ§Ã£o)
- 6 tabs especializadas: TC VeÃ­culos, AnÃ¡lise Flex, Volume, Custos por Oficina, Tempo de ProduÃ§Ã£o, Dados Detalhados
- Best Estimate: simulador de premissas (sensibilidade, inflaÃ§Ã£o, volume) com geraÃ§Ã£o de Forecast

**ðŸ“Š TC Estendido (TC Ext)**
- AnÃ¡lise de custos por oficina, conta e perÃ­odo
- VisualizaÃ§Ã£o Normal (Custo Total) e CPU (Custo por Unidade)
- Dashboard interativo com filtros

**ðŸ”§ Capacidades Transversais**
- Cache inteligente com TTL e otimizaÃ§Ã£o de tipos de dados
- Dados em formato Parquet comprimido
- ConversÃ£o multi-moeda (BRL, USD, EUR)
- Fator de escala configurÃ¡vel (Nenhum / K / M)
- Interface moderna com tabs, grÃ¡ficos Altair e gradientes

**ðŸ‘¥ Equipe do Projeto:**
- ðŸ”§ Hudson Cardin â€” Full-Stack Developer
- ðŸ“Š Lauro Paiva Junior â€” Full-Stack Developer
- ðŸ§­ Frederico Cesar de Jesus â€” Tech Advisor (Manufacturing Finance Controller, Stellantis)

---

## 2) Cadeia de Custos TC VeÃ­culos

```
Despesa PrimÃ¡ria
    Ã— Rateio FA
    = Custo FA (Fluxo Anexo)

Custo FP (Fluxo Principal)
    = Despesa PrimÃ¡ria âˆ’ Custo FA

D&A Dedicado = parcela de D&A atribuÃ­da diretamente ao veÃ­culo
FP sem Dedicada = Custo FP âˆ’ D&A Dedicado
```

**Colunas MonetÃ¡rias** (recebem conversÃ£o de moeda e fator):
- `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`

**Redis** â€” NÃ£o Ã© uma coluna nem um Account fixo. Redis entra como linhas adicionais vindas da aba **massa - REDIS**, marcadas com `_fonte_redis=True`.

> Redis = Î£ Despesa Primaria nas linhas com `_fonte_redis=True` (valores tipicamente negativos por serem receita)

---

## 3) Processo de Rateio por VeÃ­culo

O custo da oficina Ã© **rateado** aos veÃ­culos proporcionalmente ao **tempo de produÃ§Ã£o**:

- **Percentual(v,o)** = TempoVeic(v,o) / Î£ TempoVeic(v,o)
- **CustoRateado(v,o)** = FPsemDedicada(o) Ã— Percentual(v,o)
- **CustoFPVeiculo(v,o)** = CustoRateado(v,o) + D&A Dedicado(v,o)

**Dados Consolidados vs Rateados:**

| SeleÃ§Ã£o | Fonte BUD | Fonte Real |
|---------|-----------|------------|
| Todos | `df_principal_BUD.parquet` | `df_principal.parquet` |
| VeÃ­culo especÃ­fico | `df_veiculos_custo_fp_BUD.parquet` | `df_veiculos_custo_fp.parquet` |

> Quando VeÃ­culo = "Todos": dados consolidados. Quando VeÃ­culo = modelo especÃ­fico: dados rateados com `Custo FP Veiculo`.

---

## 4) Flex Budget (TC VeÃ­culos)

O Budget Flex ajusta o orÃ§amento pela proporÃ§Ã£o de volume realizado:
- Custos **fixos** permanecem iguais ao Budget
- Custos **variÃ¡veis** sÃ£o ajustados pela proporÃ§Ã£o de volume

**FÃ³rmulas:**
- **ProporÃ§Ã£o** = Volume Realizado / Volume Budget
- **Flex fixo** = BUD fixo (sem alteraÃ§Ã£o)
- **Flex variÃ¡vel** = BUD variÃ¡vel Ã— ProporÃ§Ã£o
- **Flex total** = Flex fixo + Flex variÃ¡vel

**ClassificaÃ§Ã£o Fixo/VariÃ¡vel:**
A coluna `Custo` determina a classificaÃ§Ã£o:
- Valores que comeÃ§am com `"Fix"` (case-insensitive) â†’ **Fixo**
- Todos os demais â†’ **VariÃ¡vel**

---

## 5) CPU (Custo por Unidade)

**CPU = Custo Total / Volume Total**

âš ï¸ REGRA CRÃTICA: O CPU deve ser calculado APÃ“S o agrupamento dos dados, nunca antes.

**Exemplo:**
- Linha 1: Custo = R$ 100, Volume = 10 â†’ CPU = R$ 10,00/un
- Linha 2: Custo = R$ 200, Volume = 40 â†’ CPU = R$ 5,00/un
- Incorreto (mÃ©dia de CPUs): (10 + 5) / 2 = R$ 7,50/un
- Correto (CPU apÃ³s agrupar): R$ 300 / 50 = **R$ 6,00/un**

Quando o tipo de visualizaÃ§Ã£o Ã© CPU:
- Cada mÃ©trica Ã© dividida pelo volume total
- O sistema recalcula CPU apÃ³s agregaÃ§Ãµes

---

## 6) KPIs do TC VeÃ­culos

**KPIs do Topo (fora das tabs):**

| KPI | FÃ³rmula |
|-----|---------|
| Desp. PrimÃ¡ria | Î£ Despesa Primaria |
| Custo FA | Î£ Custo FA |
| Redis | Î£ Despesa Primaria (linhas `_fonte_redis=True`) |
| Custo FP | Î£ Custo FP |
| D&A Dedicada | Î£ D&A dedicado |
| FP sem Dedicada | Î£ FP sem Dedicada |

**KPIs do Resumo:**

| KPI | FÃ³rmula |
|-----|---------|
| BUD | BUD fixo + BUD variÃ¡vel |
| Flex Bud âˆ’ BUD | Flex total âˆ’ BUD total |
| Flex BUD | BUD fixo + BUD variÃ¡vel Ã— ProporÃ§Ã£o |
| Real âˆ’ Flex Bud | Real total âˆ’ Flex total |
| Real | Î£ Custo FP Real |
| Real / Flex Bud | Real / Flex BUD (%) |

---

## 7) Filtros do TC VeÃ­culos

| Filtro | Tipo | Comportamento |
|--------|------|---------------|
| Oficina | multiselect | "Todos" ou seleÃ§Ã£o mÃºltipla |
| Tipo Custo | multiselect | Fixo/VariÃ¡vel ou todos |
| VeÃ­culo | selectbox | "Todos" (consolidado) ou 1 veÃ­culo (rateado) |
| PerÃ­odo | multiselect | "Todos" ou seleÃ§Ã£o de meses |

Cascading: A seleÃ§Ã£o de Oficina filtra os VeÃ­culos disponÃ­veis.

---

## 8) VisualizaÃ§Ãµes e GrÃ¡ficos

### Modos de VisualizaÃ§Ã£o

- **Fixo/VariÃ¡vel**: Expanders Fixo e VariÃ¡vel, sub-expanders por Type 05 â†’ tabela por Account
- **Total**: Expanders direto por Type 05 â†’ tabela por Account

### Tabela Flex por Account

| Coluna | CÃ¡lculo |
|--------|---------|
| Account | Nome da conta |
| BUD | Î£ Custo FP Budget |
| Flex Bud âˆ’ BUD | Flex âˆ’ BUD |
| Flex BUD | Fixo: BUD / VariÃ¡vel: BUD Ã— ProporÃ§Ã£o |
| Total âˆ’ Flex Bud | Real âˆ’ Flex |
| Total | Î£ Custo FP Real |
| Total / Flex Bud | Real/Flex (%) |

### Barrinha de Progresso
- ðŸŸ¢ Verde: â‰¤ 90%
- ðŸŸ¡ Gradiente: 90%â€“100%
- ðŸ”´ Vermelho: â‰¥ 100%

### GrÃ¡ficos do TC VeÃ­culos

**Custo FP por PerÃ­odo:**
- Barras: Real por perÃ­odo (degradÃª roxo, scheme='purples')
- Linha pontilhada: Flex BUD (laranja, strokeDash=[10,5])
- Delta: Real âˆ’ Flex BUD (verde/vermelho)
- Biblioteca: Altair

**Cores do Best Estimate:**
- ðŸŸ£ Roxo escuro (#4C1D95): meses HistÃ³ricos (realizados)
- ðŸŸ£ Roxo claro (#C4B5FD): meses de Best Estimate (projetados)

### OrganizaÃ§Ã£o em Tabs

| Tab | ConteÃºdo |
|-----|----------|
| ðŸš— TC VeÃ­culos | KPIs + GrÃ¡fico Custo FP Ã— Flex BUD |
| ðŸ“Š AnÃ¡lise Flex | Fixo/VariÃ¡vel com Type 05 â†’ Account |
| ðŸ“ˆ Volume | Budget vs Realizado |
| ðŸ¢ Custos por Oficina | Custo FP e Rateio FA |
| â±ï¸ Tempo de ProduÃ§Ã£o | Tempo VeÃ­culo vs Tempo FA |
| ðŸ“‹ Dados Detalhados | Tabelas exportÃ¡veis + Sapiens detalhado |

---

## 9) Premissas do Simulador Best Estimate

**FÃ³rmula Geral:**
```
BE = MÃ©dia_HistÃ³rica Ã— Fator_VariaÃ§Ã£o Ã— Fator_MonetÃ¡rio
```

Onde:
- Fator_VariaÃ§Ã£o = 1 + (VariaÃ§Ã£o_Volume Ã— Sensibilidade)
- Fator_MonetÃ¡rio = (1 + InflaÃ§Ã£o / 100) Ã— (1 - Produtividade / 100)
- VariaÃ§Ã£o_Volume = (Volume_Futuro / Volume_MÃ©dio_HistÃ³rico) âˆ’ 1

**SequÃªncia correta do cÃ¡lculo:**
1. Carregar apenas a base histÃ³rica real vÃ¡lida.
2. Remover linhas antigas de BE / BE Manual / Forecast da base usada para mÃ©dia.
3. Calcular mÃ©dia histÃ³rica e volume mÃ©dio histÃ³rico por combinaÃ§Ã£o de chaves.
4. Aplicar o volume futuro configurado.
5. Aplicar sensibilidade.
6. Aplicar inflaÃ§Ã£o e produtividade no bloco monetÃ¡rio final.
7. Consolidar HistÃ³rico + BE + BE Manual.
8. Gerar o arquivo rateado por veÃ­culo com a mesma lÃ³gica econÃ´mica do Real.
9. Validar convergÃªncia dos meses histÃ³ricos contra o Real.

**Resultado por tipo de custo:**
- **Custo Fixo BE** = MÃ©dia HistÃ³rica Ã— (1 + InflaÃ§Ã£o%) Ã— (1 - Produtividade%) â€” sem ajuste de volume
- **Custo VariÃ¡vel BE** = MÃ©dia HistÃ³rica Ã— (Vol_Futuro / Vol_HistÃ³rico) Ã— (1 + InflaÃ§Ã£o%) Ã— (1 - Produtividade%)

**Sensibilidade:**
| Tipo | Sensibilidade | FÃ³rmula |
|------|---------------|---------|
| Fixo | 0% | BE = MÃ©dia Ã— 1,0 Ã— (1 + InflaÃ§Ã£o%) Ã— (1 - Produtividade%) |
| VariÃ¡vel | 100% | BE = MÃ©dia Ã— (Vol_Futuro / Vol_HistÃ³rico) Ã— (1 + InflaÃ§Ã£o%) Ã— (1 - Produtividade%) |
| Semi-variÃ¡vel | 0% < s < 100% | BE = MÃ©dia Ã— (1 + Var_Volume Ã— s) Ã— (1 + InflaÃ§Ã£o%) Ã— (1 - Produtividade%) |

**GeraÃ§Ã£o de Forecast:**
- `forecast_historico.parquet` â€” HistÃ³rico isolado
- `forecast_previsao.parquet` â€” Apenas meses projetados
- `forecast_completo.parquet` â€” Consolidado com HistÃ³rico + BE + BE Manual
- `forecast_veiculos_custo_fp.parquet` â€” Consolidado rateado por veÃ­culo
- `custos_especificos.parquet` â€” Camada manual do BE
- `config_forecast.json` â€” Premissas utilizadas

**FunÃ§Ã£o `ratear_be_por_veiculo()`:**
Distribui o forecast por veÃ­culo usando a mesma regra do Real:
- `Custo Rateado = FP sem Dedicada Ã— Percentual`
- `Custo FP Veiculo = Custo Rateado + D&A dedicado`

Os percentuais vÃªm do Real (`df_veiculos_percentual_rateio.parquet`) e o D&A dedicado tambÃ©m vem do Real (`df_dea_dedicado.parquet`).

Fallback correto:
- primeiro tenta distribuiÃ§Ã£o mÃ©dia por perÃ­odo entre veÃ­culos conhecidos;
- distribuiÃ§Ã£o uniforme sÃ³ Ã© usada como Ãºltimo fallback, para perÃ­odos totalmente Ã³rfÃ£os.

**Garantia na anÃ¡lise:**
Na leitura analÃ­tica do BE, meses marcados como `HistÃ³rico` sÃ£o sobrepostos pelos dados reais antes da exibiÃ§Ã£o. Isso garante que Jan/Fev e demais meses histÃ³ricos fechem exatamente com o Real mesmo se um parquet antigo ainda existir no ambiente.

---

## 10) Contrato dos Parquets Otimizados e Regras de NÃ£o RegressÃ£o

O SCI possui uma camada de leitura otimizada controlada pela flag `SCI_USE_OPTIMIZED_PARQUETS`.

Regra crÃ­tica:
- local pode funcionar com a flag desligada e mascarar regressÃµes;
- no Databricks a flag pode estar ligada e forÃ§ar leitura de variantes `agg` ou `thin`.

Por isso, qualquer mudanÃ§a em pipeline, forecast, waterfall ou tooltip deve respeitar o contrato abaixo.

### Contratos AGG que nÃ£o podem regredir

`df_principal_agg_home` e `df_principal_agg_home_BUD`
- Chaves obrigatÃ³rias: `Ano`, `PerÃ­odo`, `Oficina`, `Type 05`, `Type 06`, `Account`, `Custo`
- MÃ©tricas obrigatÃ³rias: `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`

`df_veiculos_agg_home` e `df_veiculos_agg_home_BUD`
- Chaves obrigatÃ³rias: `Ano`, `PerÃ­odo`, `Oficina`, `VeÃ­culo`, `Type 05`, `Type 06`, `Account`, `Custo`
- MÃ©tricas obrigatÃ³rias: `Custo FP Veiculo`, `Custo Rateado`, `D&A dedicado`

`forecast_agg`
- Chaves obrigatÃ³rias: `Ano`, `PerÃ­odo`, `Oficina`, `Tipo`, `Type 05`, `Type 06`
- MÃ©tricas obrigatÃ³rias: `Custo FP`, `FP sem Dedicada`, `D&A dedicado`

### RegressÃµes jÃ¡ observadas e causa raiz

1. Waterfall de veÃ­culos exibindo `Type 06 nÃ£o encontrada`.
    Causa: `load_custo_fp_veiculo()` e `load_custo_fp_veiculo_real()` liam `df_veiculos_agg_home*` com `prefer='agg'`, mas os AGG antigos nÃ£o tinham `Type 05`, `Type 06`, `Account` e `Custo`.

2. Tooltip do Best Estimate concentrando tudo em `Outros`.
    Causa: `forecast_agg.parquet` nÃ£o tinha `Type 05` e `Type 06`, entÃ£o o builder do tooltip perdia a granularidade.

3. DivergÃªncia local x Databricks.
    Causa: local com flag desligada e Databricks com flag ligada, fazendo caminhos de leitura diferentes.

### ProteÃ§Ã£o adicional implementada

O `tc_core/data_router.py` valida AGG contra `tc_core/parquet_schemas.py`.

Se o parquet otimizado estiver desatualizado e faltarem colunas do schema:
- o app registra warning;
- faz fallback automÃ¡tico para o parquet full;
- evita quebra imediata em produÃ§Ã£o atÃ© a reprocessaÃ§Ã£o dos dados.

### Checklist obrigatÃ³rio antes de publicar

1. Validar com `SCI_USE_OPTIMIZED_PARQUETS=true`, nÃ£o apenas no fluxo local padrÃ£o.
2. Confirmar que `Type 05`, `Type 06`, `Account` e `Custo` existem nos AGG usados por waterfall, home e anÃ¡lises de tooltip.
3. Se houve mudanÃ§a em groupby, atualizar `tc_core/parquet_schemas.py` antes de regenerar qualquer parquet.
4. Regenerar os AGG afetados a partir dos FULL originais.
5. Testar Waterfall, Home TC VeÃ­culos e Best Estimate com a flag otimizada ligada.
6. SÃ³ depois sincronizar e fazer deploy.

---

## 11) Arquitetura TC VeÃ­culos

### Estrutura de Pastas

```
dados/TC_Principal/
â”œâ”€â”€ {ano}/
â”‚   â”œâ”€â”€ BUD/
â”‚   â”‚   â”œâ”€â”€ df_principal_BUD.parquet
â”‚   â”‚   â”œâ”€â”€ df_vol_veiculos_BUD.parquet
â”‚   â”‚   â”œâ”€â”€ df_veiculos_custo_fp_BUD.parquet
â”‚   â”‚   â””â”€â”€ ...
â”‚   â”œâ”€â”€ df_principal.parquet
â”‚   â”œâ”€â”€ df_tc_sapiens.parquet          â† todas as colunas Sapiens
â”‚   â”œâ”€â”€ df_veiculos_custo_fp.parquet
â”‚   â””â”€â”€ df_vol_veiculos_actual.parquet
â”œâ”€â”€ Forecast/
â”‚   â”œâ”€â”€ forecast_historico.parquet
â”‚   â”œâ”€â”€ forecast_previsao.parquet
â”‚   â”œâ”€â”€ forecast_completo.parquet
â”‚   â”œâ”€â”€ forecast_veiculos_custo_fp.parquet
â”‚   â”œâ”€â”€ custos_especificos.parquet
â”‚   â””â”€â”€ config_forecast.json
â””â”€â”€ historico_consolidado/
```

### Schema â€” df_principal

| Coluna | Tipo | DescriÃ§Ã£o |
|--------|------|-----------|
| Oficina | str | Centro de custo |
| VeÃ­culo | str | Modelo do veÃ­culo |
| Type 05 | str | ClassificaÃ§Ã£o nÃ­vel 1 |
| Type 06 | str | ClassificaÃ§Ã£o nÃ­vel 2 |
| Custo | str | Fixo ou VariÃ¡vel |
| Account | str | Conta contÃ¡bil |
| PerÃ­odo | str | MÃªs por extenso |
| Despesa Primaria | float | Despesa primÃ¡ria (R$) |
| Custo FA | float | Custo do Fluxo Anexo |
| Custo FP | float | Custo FP consolidado |
| D&A dedicado | float | D&A dedicada |
| FP sem Dedicada | float | Custo FP sem D&A |

### Schema â€” df_tc_sapiens

Inclui TODAS as colunas acima mais as colunas extras do SAP:
Centrocst, NÂºdoc.ref., Dt.lÃ§to., Doc.compra, Texto breve, Fornecedor, Material, UsuÃ¡rio, Fornec., Tipo, USI, QTD, Rateio FA

### Estrutura do CÃ³digo

```
tc_principal/
â”œâ”€â”€ shared.py              # Constantes, loaders, helpers
â”œâ”€â”€ ui_components.py       # Sidebar, CSS, KPIs
â””â”€â”€ pages/
    â”œâ”€â”€ home_tc.py                     # PÃ¡gina principal (6 tabs)
    â”œâ”€â”€ best_estimate_simulador_tc.py  # Simulador BE
    â””â”€â”€ waterfall_tc.py                # Waterfall

Obs.: a **anÃ¡lise** de Best Estimate / Forecast (Real + BE) Ã© exibida no prÃ³prio `home_tc.py`
consumindo `dados/TC_Principal/Forecast/forecast_completo.parquet` e, quando hÃ¡ granularidade veicular, `dados/TC_Principal/Forecast/forecast_veiculos_custo_fp.parquet`.
```

### Pipeline de Processamento (processamento_dados_veiculos.py)

```
18 fases:
  1. Sapiens (leitura, todas as colunas)
  1B. Redis (aba massa-REDIS)
  2. Volume e EST PdR (Actual)
  3. Volume veÃ­culos (Actual)
  4. Tempo veÃ­culos (EST Ã— Volume)
  5. Rateio FA
  6. Custo FA = Rateio FA Ã— Despesa Primaria
  7. Custo FP = Despesa Primaria âˆ’ Custo FA
  8. D&A Dedicado (do Budget)
  9. FP sem Dedicada = Custo FP âˆ’ D&A dedicado
  10. Salvamento principal (parquets)
  10B. Parquet Sapiens detalhado (todas as colunas)
  11-18. Rateio por veÃ­culo, CPU, salvamento final
```

### Moeda e Fator

| CÃ³digo | SÃ­mbolo | ConversÃ£o |
|--------|---------|-----------|
| BRL | R$ | 1.0 (base) |
| USD | $ | 1/Taxa USDâ†’BRL |
| EUR | â‚¬ | 1/Taxa EURâ†’BRL |

---

## 12) Guia de ExtraÃ§Ã£o de Dados

### Fluxo

```
Arquivos Excel (Entrada)
    â”œâ”€â”€ processamento_dados_veiculos_BUD.py (Budget)
    â”‚     â†’ df_principal_BUD.parquet + parquets de volume/tempo + rateio por veÃ­culo (BUD)
    â””â”€â”€ processamento_dados_veiculos.py (Real)
          â†’ df_principal.parquet + df_tc_sapiens.parquet (detalhado) + rateio por veÃ­culo (Real)
```

**PÃ¡gina Streamlit (orquestraÃ§Ã£o):** `tc_principal/pages/extracao_dados_tc.py`
- Budget: `processar_veiculos_budget`
- Real: `processar_veiculos_real`

### Arquivo de entrada (fonte Ãºnica)

- **Arquivo principal:** `Reporting veÃ­culos.xlsx`
- **Local esperado:** `dados/TC_Principal/{ANO}/Reporting veÃ­culos.xlsx`
- A pÃ¡gina de extraÃ§Ã£o permite **upload** do arquivo com proteÃ§Ã£o contra sobrescrita.

### Abas obrigatÃ³rias â€” Budget (no Excel)

- `massa primÃ¡ria - BDG`
- `massa - REDIS`
- `Volume e EST PdR - BDG`
- `Volume BDG`
- `Volume Actual`
- `EST veÃ­culos - BDG`
- `massa - D&A dedicado`

### Abas obrigatÃ³rias â€” Real (no Excel)

- `Sapiens`
- `Volume e EST PdR - Actual`
- `Volume Actual`
- `EST veÃ­culos - Actual`

### PrÃ©-validaÃ§Ã£o (recomendado)

A prÃ³pria pÃ¡gina `extracao_dados_tc.py` executa uma prÃ©-validaÃ§Ã£o para reduzir falhas durante o processamento, por exemplo:

- Confere se as abas obrigatÃ³rias existem.
- Budget: checa colunas mÃ­nimas em `massa primÃ¡ria - BDG` (ex.: `Oficina`, `Account`) e `massa - REDIS` (ex.: `Oficina`).
- Budget: tenta detectar meses na aba `Volume BDG` (testando mÃºltiplos headers).
- Real: em `Sapiens`, checa colunas mÃ­nimas (ex.: `Oficina`, `Account`, `Valor`).

### DependÃªncia importante (Budget â†’ Real)

Para o fluxo completo do **Real**, a extraÃ§Ã£o emite aviso se nÃ£o existir o parquet de D&A dedicado do Budget:
- `dados/TC_Principal/{ANO}/BUD/df_dea_dedicado_BUD.parquet`

Na prÃ¡tica: **rode o Budget antes do Real** quando estiver montando um ano novo.

### Rateios manuais (PdR)

Os rateios manuais QY/GS/SM sÃ£o persistidos em `rateios_manuais.json` e sÃ£o usados no cÃ¡lculo da taxa PdR.

**Principais saÃ­das (por ano):**
- Real: `dados/TC_Principal/{ANO}/df_principal.parquet`, `df_veiculos_custo_fp.parquet`, `df_veiculos_cpu.parquet`
- Budget: `dados/TC_Principal/{ANO}/BUD/df_principal_BUD.parquet`, `df_veiculos_custo_fp_BUD.parquet`, `df_veiculos_cpu_BUD.parquet`

**Busca de arquivos:**
1. `dados/TC_Principal/{ANO}/Nome_do_Arquivo.xlsx` (prioridade)
2. `./Nome_do_Arquivo.xlsx` (raiz)

---

## 12) TC Copilot â€” Agente de IA

### Capacidades
- RelatÃ³rio mensal com 3 seÃ§Ãµes: Volume e VariaÃ§Ãµes, Comparativos, ConclusÃµes
- AnÃ¡lise por oficina (AS, BS, GS, PL, PS, QY, SM)
- Chatbot live com contexto dos dados e documentaÃ§Ã£o
- Emojis visuais (ðŸ“ˆðŸ“‰âš ï¸âœ…âŒðŸ’¡ðŸ­ðŸ“ŠðŸŸ¢ðŸ”´)
- Tratamento de referÃªncias ausentes ("sem ref.", "sem base (ref.=0)")

### Arquitetura

```
tc_copilot/
â”œâ”€â”€ data_collector.py     # Leitura de parquets, variaÃ§Ãµes
â”œâ”€â”€ prompts.py            # Prompts bilÃ­ngues
â”œâ”€â”€ report_generator.py   # Pipeline PDF
â”œâ”€â”€ llm_integration.py    # IntegraÃ§Ã£o OpenAI
â””â”€â”€ pages/home_copilot.py # Interface Streamlit
```

---

## 13) Otimização de Dados — THIN / AGG / Data Router

### Motivação
Parquets grandes (>10 mil linhas) são desnecessários para KPIs macro e gráficos de período.
O sistema gera variantes otimizadas após cada processamento de dados.

### Variantes

| Variante | Descrição | Exemplo |
|----------|-----------|---------|
| **FULL** | Parquet original com todas as colunas | `df_principal.parquet` |
| **THIN** | Remove colunas de detalhe (`Texto breve`, `Material`, `Fornec.`, `Centrocst`, `Nºconta`, `Dt.lçto.`, `Usuário`) | `df_principal_thin.parquet` |
| **AGG**  | Pré-agregado por dimensões macro (`Ano`, `Período`, `Oficina`/`Tipo`) | `df_principal_agg_home.parquet` |

### Schemas (tc_core/parquet_schemas.py)
Cada variante tem schema explícito definindo:
- `remove_columns` (THIN) — colunas a remover
- `group_keys` + `sum_columns` (AGG) — chaves de agrupamento e métricas

### Data Router (tc_core/data_router.py)
Feature flag: `SCI_USE_OPTIMIZED_PARQUETS` (default: True)

```
read_optimized(domain, ano, subfolder, table, prefer='thin'|'agg', consumer='...')
```

Lógica: tenta variante preferida → se não existir, fallback para FULL.
Telemetria registra qual fonte (THIN/AGG/FULL) foi usada.

### Parquets Otimizados Gerados

**TC_Principal (Real/BUD):**
- `df_principal_thin.parquet` / `df_principal_agg_home.parquet`
- `df_veiculos_agg_home.parquet`

**TC_Ext (Real/BUD):**
- `df_final_thin.parquet` / `df_final_agg.parquet`

**Forecast (Best Estimate):**
- `forecast_agg.parquet` — pré-agregado por [Ano, Período, Oficina, Tipo]

### Debug Panel
Ativar `SCI_DEBUG_DATA_TRACE=true` (ou via feature flags) para ver painel de telemetria
no rodapé do app mostrando fonte (THIN/AGG/FULL) de cada dataset carregado.

---

## 14) Guia de Build (EXE)

- `streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence`
- PÃ³s-build: copiar `dados/`, mÃ³dulos, pÃ¡ginas para `dist/<NOME>/_internal/`
- No EXE, `sys._MEIPASS` aponta para `_internal/`
- AgGrid precisa ser copiado manualmente do `.venv`

---

*ðŸ“š Stellantis Cost Intelligence (SCI) | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus*

