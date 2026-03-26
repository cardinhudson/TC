# ðŸ“š DocumentaÃ§Ã£o TÃ©cnica Completa â€” Sistema TC (TC Estendido + Arquitetura Geral)

> DocumentaÃ§Ã£o completa do TC Estendido e componentes transversais do SCI.

---

## 1) VisÃ£o Geral do Sistema

O Sistema TC Ã© um conjunto de dashboards (Streamlit) para anÃ¡lise de custos e volumes de uma operaÃ§Ã£o industrial:

- **TC Ext (Real)**: anÃ¡lise de custo total e CPU por perÃ­odo, veÃ­culo e oficina
- **Budget (BUD)**: dados planejados para comparaÃ§Ã£o
- **Flex Bud**: budget flexÃ­vel ajustado por volume real (fixos Ã— variÃ¡veis)
- **Waterfall Analysis**: decomposiÃ§Ã£o das diferenÃ§as entre perÃ­odos
- **Best Estimate / Forecast**: projeÃ§Ãµes e simulaÃ§Ãµes
- **ExportaÃ§Ã£o**: download de tabelas em Excel
- **TC Copilot**: agente de IA para relatÃ³rios e chatbot

Dados em **Parquet** (performÃ¡tico), aceita dados brutos via **Excel**.

---

## 2) Regras e CÃ¡lculo â€” TC Estendido

### CPU (Custo por Unidade)

```
CPU = Custo_Total / Volume_Total
```

âš ï¸ REGRA CRÃTICA: CPU calculado APÃ“S agrupamento, nunca antes.

**Exemplo:**
- Linha 1: Custo R$ 100, Volume 10 â†’ CPU R$ 10/un
- Linha 2: Custo R$ 200, Volume 40 â†’ CPU R$ 5/un
- Incorreto (mÃ©dia): (10 + 5) / 2 = R$ 7,50/un
- Correto: R$ 300 / 50 = **R$ 6,00/un**

### Custo Total

```
Custo_Total = Î£(Custo_Individual)
```

Sempre somar valores individuais, nunca calcular mÃ©dia.

### Fator de ConversÃ£o (K/M)

- **K (milhares):** Valor / 1.000
- **M (MilhÃµes):** Valor / 1.000.000
- **Nenhum:** Valor original

âš ï¸ O fator de conversÃ£o NÃƒO deve ser aplicado no modo CPU.

**Ordem de AplicaÃ§Ã£o:**
1. Aplicar fator (K/M) â€” apenas em Custo Total
2. Converter moeda
3. Realizar cÃ¡lculos (CPU, Flex, diferenÃ§as)

### CÃ¡lculo de DiferenÃ§as e Ratios

| MÃ©trica | FÃ³rmula | InterpretaÃ§Ã£o |
|---------|---------|---------------|
| Delta Flex-BUD | Flex_BUD - BUD | Efeito do volume |
| Delta Total-Flex | Total - Flex_BUD | Efeito do custo |
| Ratio Total/Flex | Total / Flex_BUD Ã— 100% | < 100% = eficiÃªncia |

### Flex Bud (Budget FlexÃ­vel) â€” TC Estendido

**Conceito:** Ajusta o budget considerando a variaÃ§Ã£o de volume.

**Regra para Fixos:** `Flex_Fixo = Valor_Original_Fixo` (sensibilidade 0%)
**Regra para VariÃ¡veis:** `Flex_VariÃ¡vel = Valor_Original Ã— (Volume_Novo / Volume_Original)` (sensibilidade 100%)

**IdentificaÃ§Ã£o:** Coluna `Custo`: valores `'Fixo'` ou `'VariÃ¡vel'`

#### CASO 1: Flex para Real x Real (Waterfall)

Compara dois perÃ­odos reais (MÃªs 1 vs MÃªs 2):
```
rho = V_2 / V_1
Flex_MÃªs1 = C_1_Fixo + C_1_VariÃ¡vel Ã— rho
```

**Exemplo:** Vâ‚ = 40.848, Vâ‚‚ = 60.333, Câ‚_Fixo = R$ 126,91, Câ‚_Var = R$ 755,36
â†’ rho = 1,4824 â†’ Flex = R$ 126,91 + R$ 1.119,72 = R$ 1.246,63

#### CASO 2: Flex para Real x Budget (TC Ext)

```
rho = V_Real / V_Budget
Flex_Bud = B_Fixo + B_VariÃ¡vel Ã— rho
```

**Exemplo:** V_Real = 50.000, V_Budget = 60.000 â†’ rho = 0,8333
B_Fixo = R$ 200.000, B_Var = R$ 400.000 â†’ Flex = R$ 533.333,33

| Aspecto | Real x Real | Real x Budget |
|---------|-------------|---------------|
| Base | Custo Real MÃªs 1 | Custo Budget |
| ProporÃ§Ã£o | Vâ‚‚ / Vâ‚ | V_Real / V_Budget |

### Fonte de Dados de Volume

- `df_vol_historico.parquet`: histÃ³rico consolidado
- `df_vol.parquet`: volume por ano
- Colunas obrigatÃ³rias: Volume, PerÃ­odo, Oficina, VeÃ­culo

âš ï¸ Volumes devem usar os MESMOS filtros aplicados aos dados de custo.

### Moedas Suportadas

| Moeda | SÃ­mbolo | Nota |
|-------|---------|------|
| BRL | R$ | Moeda base |
| USD | $ | Taxa USDâ†’BRL |
| EUR | â‚¬ | Taxa EURâ†’BRL |

### Sistema de Filtros

Ordem hierÃ¡rquica:
1. Ano
2. Oficina
3. VeÃ­culo
4. USI
5. PerÃ­odo
6. Centro cst
7. Conta contÃ¡bil
8. Type 5, Type 6
9. Fornecedor
10. Filtros AvanÃ§ados: UsuÃ¡rio, Material, Dt.lÃ§to., Texto breve, Account

---

## 3) Arquitetura â€” TC Estendido

### Estrutura do Projeto

```
TC/
â”œâ”€â”€ app.py                        # Portal / Router (st.navigation)
â”œâ”€â”€ pages/                        # PÃ¡ginas legadas (TC Ext)
â”‚   â”œâ”€â”€ 1 - Waterfall.py
â”‚   â”œâ”€â”€ 2 - Best Estimate - Simulador.py
â”‚   â”œâ”€â”€ 5 - ExtraÃ§Ã£o de Dados.py
â”‚   â””â”€â”€ 6 - Documentacao.py
â”œâ”€â”€ tc_ext/                       # MÃ³dulo TC Ext (Linhas SecundÃ¡rias)
â”‚   â”œâ”€â”€ metricas_tc_ext.py
â”‚   â”œâ”€â”€ normalizacao.py
â”‚   â””â”€â”€ pages/
â”‚       â”œâ”€â”€ home_ext.py
â”‚       â””â”€â”€ be_analise_ext.py
â”œâ”€â”€ tc_principal/                 # MÃ³dulo TC VeÃ­culos (TC Principal)
â”‚   â”œâ”€â”€ shared.py
â”‚   â”œâ”€â”€ ui_components.py
â”‚   â””â”€â”€ pages/
â”‚       â”œâ”€â”€ home_tc.py
â”‚       â”œâ”€â”€ waterfall_tc.py
â”‚       â”œâ”€â”€ best_estimate_simulador_tc.py
â”‚       â”œâ”€â”€ extracao_dados_tc.py
â”‚       â””â”€â”€ debug_calculos_tc.py
â”œâ”€â”€ tc_core/                      # UtilitÃ¡rios compartilhados (paths, perÃ­odos, schema, moedas, UI)
â”‚   â”œâ”€â”€ data/paths.py             # Constantes PASTA_TC_EXT / PASTA_TC_PRINCIPAL
â”‚   â””â”€â”€ utils/portabilidade.py    # get_base_path() (Dev â†” EXE)
â”œâ”€â”€ tc_copilot/                   # Agente de IA (chat + relatÃ³rio PDF)
â””â”€â”€ dados/                        # Dados (Parquet/Excel) por mÃ³dulo
```

### Estrutura da Pasta dados/

```
dados/
â”œâ”€â”€ TC_Ext/                       # TC Ext (Linhas SecundÃ¡rias)
â”‚   â”œâ”€â”€ {ANO}/
â”‚   â”‚   â”œâ”€â”€ df_final.parquet
â”‚   â”‚   â”œâ”€â”€ df_vol.parquet
â”‚   â”‚   â”œâ”€â”€ df_ke5z_group.parquet
â”‚   â”‚   â”œâ”€â”€ Dados SAPIENS.xlsx
â”‚   â”‚   â”œâ”€â”€ Reporting fluxo anexo.xlsx
â”‚   â”‚   â””â”€â”€ BUD/
â”‚   â”‚       â”œâ”€â”€ df_final_BUD.parquet
â”‚   â”‚       â”œâ”€â”€ df_vol_BUD.parquet
â”‚   â”‚       â””â”€â”€ df_ke5z_group_BUD.parquet
â”‚   â”œâ”€â”€ historico_consolidado/
â”‚   â”‚   â”œâ”€â”€ df_final_historico.parquet
â”‚   â”‚   â”œâ”€â”€ df_vol_historico.parquet
â”‚   â”‚   â”œâ”€â”€ df_ke5z_historico.parquet
â”‚   â”‚   â””â”€â”€ BUD/
â”‚   â”‚       â”œâ”€â”€ df_final_historico_BUD.parquet
â”‚   â”‚       â”œâ”€â”€ df_vol_historico_BUD.parquet
â”‚   â”‚       â””â”€â”€ df_ke5z_historico_BUD.parquet
â”‚   â””â”€â”€ Forecast/                 # Outputs do Best Estimate / Forecast (TC Ext)
â””â”€â”€ TC_Principal/                 # TC VeÃ­culos (TC Principal)
    â”œâ”€â”€ {ANO}/
    â”‚   â”œâ”€â”€ df_principal.parquet
    â”‚   â”œâ”€â”€ df_tc_sapiens.parquet
    â”‚   â”œâ”€â”€ df_veiculos_custo_fp.parquet
    â”‚   â”œâ”€â”€ df_vol_veiculos_actual.parquet
    â”‚   â””â”€â”€ BUD/
    â”‚       â”œâ”€â”€ df_principal_BUD.parquet
    â”‚       â”œâ”€â”€ df_veiculos_custo_fp_BUD.parquet
    â”‚       â””â”€â”€ df_vol_veiculos_BUD.parquet
    â”œâ”€â”€ historico_consolidado/
    â””â”€â”€ Forecast/                 # Outputs do Best Estimate (TC VeÃ­culos)
        â”œâ”€â”€ forecast_historico.parquet
        â”œâ”€â”€ forecast_previsao.parquet
        â”œâ”€â”€ forecast_completo.parquet
        â”œâ”€â”€ forecast_veiculos_custo_fp.parquet
        â”œâ”€â”€ custos_especificos.parquet
        â””â”€â”€ config_forecast.json
```

Prioriza histÃ³rico consolidado para anÃ¡lises multi-anos. Budget e Real separados. HistÃ³rico sempre concatenado, nunca substituÃ­do.

### Stack TecnolÃ³gico

- **Streamlit** â€” Framework web
- **Python** 3.11+ (ambiente do projeto testado em 3.13)
- **Pandas** 2.0.0+ â€” ManipulaÃ§Ã£o de dados
- **NumPy** 1.24.0+
- **Altair** 5.0.0+ â€” GrÃ¡ficos interativos
- **Plotly** â€” GrÃ¡ficos waterfall
- **PyArrow** 12.0.0+ â€” Suporte Parquet
- **OpenPyXL** 3.1.0+ â€” GeraÃ§Ã£o Excel

### OtimizaÃ§Ãµes

- Cache com TTL, Category para strings, Downcast Float64â†’Float32
- SubstituiÃ§Ã£o de iterrows()/apply() por merge/np.where
- CPU apÃ³s agrupamento, Flex Bud com merge, Volume sincronizado

---

## 4) Colunas do DataFrame Final (df_final.parquet â€” TC Ext)

Mes, PerÃ­odo, Ano, NÂºconta, Centrocst, NÂºdoc.ref., Dt.lÃ§to., Valor, QTD, Volume, Type 05, Type 06, Account, Custo, USI, Oficina, Doc.compra, Texto breve, Fornecedor, Material, UsuÃ¡rio, Fornec., Tipo, CC21, CC22, CC24, CC24 5L, CC24 7L, J516, Soma_Percentuais

---

## 5) ExtraÃ§Ã£o â€” TC Estendido

### Fonte de verdade (produÃ§Ã£o)

- **PÃ¡gina Streamlit (orquestraÃ§Ã£o):** `pages/5 - ExtraÃ§Ã£o de Dados.py`
- **Processamento:** `processamento_dados.py` (REAIS) e `processamento_dados_BUD.py` (BUDGET)
- **Notebooks (referÃªncia/base):** `tc_ext/notebooks/dados.ipynb` e `tc_ext/notebooks/dados_BUD.ipynb`
    - Quando necessÃ¡rio, o projeto sincroniza a lÃ³gica dos notebooks para `.py` via `sincronizar_notebooks.py`.

### Arquivos de entrada (fonte Ãºnica por ano)

**Local padrÃ£o (recomendado):** `dados/TC_Ext/{ANO}/`

- `Dados SAPIENS.xlsx`
    - Aba obrigatÃ³ria: `Base conso` (usada tanto em REAIS quanto em BUDGET)
- `Reporting fluxo anexo.xlsx`
    - **REAIS:** abas `Sapiens`, `Rateio`, `Volume`
    - **BUDGET:** abas `Voz de custo BDG`, `Rateio BDG`, `Volume BDG`

### PrÃ©-validaÃ§Ã£o (o que o app checa antes de processar)

A pÃ¡gina de extraÃ§Ã£o executa uma checagem rÃ¡pida para reduzir falhas durante o processamento:

- Confere se os 2 arquivos existem.
- Confere se as **abas obrigatÃ³rias** existem no Excel.
- Valida **colunas mÃ­nimas** e detecÃ§Ã£o de meses:
    - **REAIS / aba `Sapiens`** (lida com `header=1`): mÃ­nimo `Valor`, `QTD`, `Oficina`, `PerÃ­odo`, `Account`, `USI`.
    - **REAIS / aba `Rateio`** e **BUDGET / aba `Rateio BDG`**:
        - exige `Oficina`, `VeÃ­culo` (ou `Veiculo`) e colunas de meses (Janeiro..Dezembro).
    - **REAIS / aba `Volume`** e **BUDGET / aba `Volume BDG`**:
        - tenta ler com `header=50/0/1/2` (layout antigo e novo);
        - exige `Oficina`, `VeÃ­culo` e colunas de meses.
    - **BUDGET / aba `Voz de custo BDG`**: mÃ­nimo `Oficina`, `Account`.

### SaÃ­das e histÃ³rico (Parquet)

- SaÃ­da REAIS: `dados/TC_Ext/{ANO}/...`
- SaÃ­da BUDGET: `dados/TC_Ext/{ANO}/BUD/...`
- **HistÃ³rico consolidado (multi-ano):** `dados/TC_Ext/historico_consolidado/`
    - Regra: **concatena e regrava** (nÃ£o substitui por ano).

### Notebooks

| Aspecto | dados.ipynb (REAL) | dados_BUD.ipynb (BUDGET) |
|---------|-------------------|--------------------------|
| Guia Dados | "Sapiens" | "Voz de custo BDG" |
| Guia Rateio | "Rateio" | "Rateio BDG" |
| Pasta SaÃ­da | dados/TC_Ext/{ANO}/ | dados/TC_Ext/{ANO}/BUD/ |
| Sufixo | Sem | _BUD |

### Fluxo

```
Excel â†’ Notebook â†’ Merges â†’ Parquet â†’ Consolidar HistÃ³rico
```

### CÃ©lulas do Notebook
- **0:** ConfiguraÃ§Ã£o (ANO, pastas)
- **1:** Leitura Sapiens (20 colunas)
- **2:** Merge com Base Conso (coluna Custo via Account)
- **3:** Rateio (melt meses â†’ linhas)
- **4:** Merge KE5Zâ†”Rateio, pivot, cÃ¡lculo CC21=CC21%Ã—Valor
- **5:** Volume (header=50, melt)
- **6:** Merge df_finalâ†”df_vol
- **7:** Salvamento + ConsolidaÃ§Ã£o HistÃ³rico

### Merges

| Merge | Chave | Tipo | Resultado |
|-------|-------|------|-----------|
| KE5Z â†” Base Conso | Account | left | Coluna Custo |
| KE5Z â†” Rateio | [Oficina, PerÃ­odo] | left | Colunas CC21%â€¦ |
| KE5Z â†” Volume | [Oficina, PerÃ­odo, VeÃ­culo] | left | Coluna Volume |

### Regras CrÃ­ticas
1. Chaves de Merge nunca alterar
2. NormalizaÃ§Ã£o de PerÃ­odo: sempre capitalizado
3. CÃ¡lculo: CC21 = CC21% Ã— Valor
4. HistÃ³rico: concatenar, nunca substituir
5. Volume: sempre float64
6. Sufixo BUD: sempre _BUD em pasta BUD/

---

## 6) Best Estimate â€” TC Estendido

### FÃ³rmula Passo a Passo
1. `proporÃ§Ã£o_volume = Volume_Futuro / Volume_MÃ©dio_HistÃ³rico`
2. `variaÃ§Ã£o_percentual = proporÃ§Ã£o - 1.0`
3. `variaÃ§Ã£o_ajustada = variaÃ§Ã£o Ã— sensibilidade`
4. `fator_variaÃ§Ã£o = 1.0 + variaÃ§Ã£o_ajustada`
5. `fator_monetÃ¡rio = (1.0 + inflaÃ§Ã£o / 100.0) Ã— (1.0 - produtividade / 100.0)`
6. `BE = MÃ©dia_HistÃ³rica Ã— fator_variaÃ§Ã£o Ã— fator_monetÃ¡rio`

### Simulador â€” Funcionalidades
- ConfiguraÃ§Ã£o interativa (perÃ­odos, sensibilidades, inflaÃ§Ã£o, produtividade e volume)
- Custos EspecÃ­ficos (BE Manual): Pontual ou Constante, rateio automÃ¡tico
- PersistÃªncia das premissas em `config_forecast.json`
- ValidaÃ§Ã£o de convergÃªncia dos meses histÃ³ricos no fluxo veicular

### Arquivos Gerados
- `forecast_completo.parquet`
- `forecast_historico.parquet`
- `forecast_previsao.parquet`
- `df_final_historico_forecast.parquet`
- `custos_especificos.parquet`
- `config_forecast.json`

### Regra CrÃ­tica do TC VeÃ­culos
- No fluxo veicular, o arquivo `forecast_veiculos_custo_fp.parquet` Ã© gerado com a mesma lÃ³gica do Real:
    - `Custo Rateado = FP sem Dedicada Ã— Percentual`
    - `Custo FP Veiculo = Custo Rateado + D&A dedicado`
- Na anÃ¡lise, meses `HistÃ³rico` sÃ£o sobrepostos pelo Real para garantir igualdade numÃ©rica com o realizado.

**Nomenclatura:** "HistÃ³rico" (real), "BE" (forecast), "BE Manual" (custos especÃ­ficos)

---

## 7) Flex Bud â€” Ano Completo e GovernanÃ§a

### Ano completo (12 meses)
- GrÃ¡ficos/tabelas exibem 12 meses do ano
- Meses sem realizado: Real = 0 (nunca puxa Budget)
- Budget e Flex Bud continuam visÃ­veis

### Flex Bud em meses sem realizado
- Sem volume real â†’ usa volume Budget como base
- Flex Bud tende a ser igual ao Budget

### GovernanÃ§a de Custo Fixo
- Custo Fixo NUNCA flexibiliza no cÃ¡lculo padrÃ£o de Flex Bud
- Flex_Fixo = BUD_Fixo (sensibilidade = 0%)
- Apenas no Simulador BE pode-se atribuir sensibilidade > 0% a fixos

### Regras CrÃ­ticas de CPU
- CPU = Total / Volume (APÃ“S agrupamento)
- Nunca somar/mediar CPUs de linhas individuais
- Fator K/M NÃƒO se aplica em modo CPU

---

## 8) ApresentaÃ§Ã£o Visual (Roteiro 5 Minutos)

**0:00â€“0:30** â€” O que Ã© o SCI: plataforma para decisÃ£o estratÃ©gica em custos de manufatura
**0:30â€“1:30** â€” TC Estendido: custo total e CPU, Flex Budget
**1:30â€“3:00** â€” TC VeÃ­culos: cadeia completa, rateio por tempo de produÃ§Ã£o
**3:00â€“4:30** â€” Funcionalidades compartilhadas: Waterfall, Best Estimate, Multi-moeda
**4:30â€“5:00** â€” BenefÃ­cios: horasâ†’segundos, zero erros, cenÃ¡rios "what-if"

### Comparativo TC Ext vs TC VeÃ­culos

| Aspecto | TC Estendido | TC VeÃ­culos |
|---------|-------------|-------------|
| VisÃ£o | Agregada | Por veÃ­culo |
| Coluna | Total | Custo FP |
| Rateio | NÃ£o hÃ¡ | Proporcional (tempo) |
| Pasta | dados/TC_Ext/ | dados/TC_Principal/ |

---

## 9) Guia de Build (EXE)

### Comando
```powershell
streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence
```

### PÃ³s-build â€” copiar para _internal/
Copiar: `dados/`, `pages/`, `tc_core/`, `tc_principal/`, `tc_ext/`, `tc_copilot/`, `.streamlit/`, scripts `.py`, JSONs, imagens.

AgGrid:
```powershell
$dest = 'dist\Stellantis-Cost-Intelligence\_internal'
Copy-Item '.venv\Lib\site-packages\st_aggrid' -Destination ($dest + '\st_aggrid') -Recurse -Force
```

### sys.path e _MEIPASS
```python
if hasattr(sys, '_MEIPASS'):
    project_root = sys._MEIPASS
else:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
```

### ValidaÃ§Ã£o
- Abrir o .exe
- Confirmar resposta em `http://localhost:8501`
- Testar extraÃ§Ã£o e parquets

### O que NÃƒO fazer
NÃ£o rodar `pyinstaller` direto do `.spec`.

---

## 10) Chatbot de DocumentaÃ§Ã£o

Assistente virtual que responde perguntas sobre o sistema baseado na documentaÃ§Ã£o completa.

**Perguntas Sugeridas:**
- O que Ã© o Stellantis Cost Intelligence (SCI)?
- Como funciona o Best Estimate?
- O que Ã© Flex Bud?
- Como funciona o rateio por veÃ­culo?
- Qual a diferenÃ§a entre TC Ext e TC VeÃ­culos?
- O que Ã© CPU (Custo por Unidade)?
- Como funciona o Waterfall?

---

## 11) TC Copilot â€” PrÃ³ximos Passos (GENAI Gateway)

### IntegraÃ§Ã£o GENAI Stellantis
- Modelos: GPT-4, Llama 3, Mistral, Cohere via GENAI Gateway
- OAuth2 (PingFederate) + mTLS + GraphQL
- Vector Store (OpenSearch) para embeddings

### Roadmap
1. **Fase 1:** PreparaÃ§Ã£o dos dados (consolidar parquets, documentar mÃ©tricas)
2. **Fase 2:** IntegraÃ§Ã£o GENAI (credenciais, GraphQL, embeddings)
3. **Fase 3:** Habilidades (perguntas, resumos, anomalias)
4. **Fase 4:** ProduÃ§Ã£o (teste, validaÃ§Ã£o, logs)

### SeguranÃ§a
- ðŸ”’ Nenhum dado sai da Stellantis
- ðŸ” mTLS + PingFederate
- âœ… API homologada
- ðŸ“‹ Logs de auditoria

---

*ðŸ“š Stellantis Cost Intelligence (SCI) | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus*

