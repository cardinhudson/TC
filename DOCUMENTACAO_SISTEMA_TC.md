# 📚 Documentação Técnica Completa — Sistema TC (Especificação para Reescrita)

> Objetivo deste documento: servir como **fonte única de verdade** (single source of truth) para reescrever o projeto com IA **mantendo 100% das funcionalidades, regras de cálculo e comportamento da interface**.
>
> Escopo: Dashboard Streamlit (página principal + páginas em `pages/`), processamento de dados (scripts/notebooks) e chatbot de documentação.

---

## 1) Visão geral (o que o sistema faz)

O Sistema TC é um conjunto de dashboards (Streamlit) para análise de custos e volumes de uma operação industrial, com foco em:

- **TC Ext (Real)**: análise de custo total e CPU (custo por unidade) por período, veículo e oficina.
- **Budget (BUD)**: dados planejados (custo e volume) para comparação.
- **Flex Bud**: cálculo de budget flexível (ajustado por volume real), distinguindo custos **fixos** e **variáveis**.
- **Waterfall Analysis**: decomposição das diferenças (efeito volume, custo fixo, custo variável, etc.) entre períodos.
- **Best Estimate / Forecast**: projeções e simulações (páginas específicas).
- **Exportação**: download de tabelas em Excel.
- **Chatbot**: busca local sobre a documentação (sem API externa).

O sistema foi desenhado para trabalhar com dados em **Parquet** (performático) e para aceitar dados brutos via **Excel**, que são processados e consolidados.

---

## 1.1) Mudanças recentes (Jan/2026)

- TC Ext: criada uma nova página de análise de Best Estimate baseada na Home (mantém o “jeito certo” de calcular/formatar).
  - Fonte: lê os outputs do simulador em `dados/Forecast/` (ex.: `forecast_completo.parquet` e `df_vol_historico.parquet`).
  - Objetivo: substituir a análise legada e reduzir divergências entre tabelas/gráficos.
- TC Ext: aba **“🚗 TC Ext por Veíc”** (Home e Best Estimate) foi reescrita com **Plotly** para evitar problemas de renderização em tabs ocultas (Altair/Vega às vezes não desenha no primeiro carregamento).
  - Inclui filtros locais (Ano/Período), rótulos, gradientes e linha de **Flex Bud** com labels.
  - Inclui resumo de volumes por **Oficina** e por **Veículo** com separação **Real x Budget** por categoria.
- CPU: regra reforçada em pontos críticos — **nunca somar/média de CPU**; sempre recalcular como $CPU = \sum Total / \sum Volume$ no nível de agrupamento.
  - Padronização por helper: `tc_ext/metricas_tc_ext.py::cpu_por_chaves()` (agrega custo+volume e faz merge `outer`).
- Gráficos por período: removido o corte por “mês atual” quando existem valores futuros (Forecast), evitando esconder Fev–Dez no ano corrente.
- Diagnósticos: adicionados expanders com prova da fonte de dados (paths/mtimes/shapes) e checagens de sanidade de CPU.
- Governança (Budget): **Volume BUDGET deve conter `Veículo`**. Se não existir `Veículo`, isso é **erro de extração** (o app não faz mais rateio/fallback).
- Extração (inputs): arquivos de entrada ficam em `dados/TC_Ext/{ano}/` (TC Ext) e `dados/TC_Principal/{ano}/` (TC Veículos); outputs de Budget seguem em subpasta `BUD/`.
- Governança (Flex Bud): **Custo Fixo nunca é flexibilizado** fora do contexto de simulação; no comparativo Real x Budget/Flex Bud, Fixo permanece igual ao Budget.
- Home (Budget): correção de totais (ex.: `Type 05`) para evitar divergência entre base de exibição e base de resumo.
- UI (exibição): remoção de linhas 100% zero/NaN e remoção da coluna `Ano` **somente para exibição** (não altera cálculos nem totais).

## 1.2) Mudanças recentes — Reestruturação de pastas e Real no TC Veículos

- **Estrutura de pastas separada por módulo**: os dados agora ficam em pastas separadas para cada módulo:
  - `dados/TC_Ext/{ano}/` — inputs e outputs do TC Ext (Real + BUD)
  - `dados/TC_Ext/historico_consolidado/` — histórico consolidado do TC Ext
  - `dados/TC_Principal/{ano}/` — inputs e outputs do TC Veículos (Real na raiz, BUD em `BUD/`)
  - `dados/TC_Principal/{ano}/BUD/` — parquets de Budget do TC Veículos
- **Páginas de extração corrigidas**: `pages/5 - Extração de Dados.py` e `tc_principal/pages/extracao_dados_tc.py` adaptadas para usar os novos caminhos.
- **TC Ext — gráficos corrigidos**: 8 referências de path em `home_ext.py` e `be_analise_ext.py` corrigidas para incluir o segmento `TC_Ext`, restaurando a linha Flex Bud e o gráfico Delta.
- **TC Veículos — Real no gráfico**: o gráfico de Custo FP por Período em `home_tc.py` agora exibe:
  - **Barras roxas (largas)**: Budget
  - **Barras azuis (estreitas)**: Real (quando disponível, via `load_principal_real`)
  - **Linha vermelha pontilhada**: Flex Bud (cálculo já usa Volume Actual)
  - **Delta**: Real - Flex Bud (anteriormente Budget - Flex Bud)
  - KPIs "Total" renomeados para "Real" quando há dados reais disponíveis
- **Rateio de veículos Real**: já implementado nas fases 11-16 de `processamento_dados_veiculos.py`.

## 1.3) Mudanças recentes — Implementação completa do TC Veículos (Jun/2026)

Todas as páginas do módulo TC Veículos foram implementadas com funcionalidade completa, deixando de ser stubs:

### Alterações de infraestrutura
- **Paths corrigidos** (Bloco 1): 25+ referências de caminhos quebrados consertadas em `tc_exports.py`, `pages/1 - Waterfall.py`, `pages/6 - Documentacao.py`, `pages/2 - Best Estimate - Simulador.py`, `tc_ext/pages/home_ext.py`.
- **Renomeação UI** (Bloco 2): "TC Principal" / "Planta Principal" → "TC Veículos" em todos os arquivos Python e Markdown.
- **Pipeline historico_consolidado** (Bloco 5): funções `consolidar_historico_tc_veiculos()` adicionadas em `processamento_dados_veiculos.py` e `processamento_dados_veiculos_BUD.py`; 9 loaders novos em `shared.py` para dados multi-ano e Forecast.
- **Import circular corrigido**: `processamento_dados_veiculos_BUD.py` — import movido para dentro da função (lazy import).

### Páginas implementadas
- **Waterfall TC Veículos** (`waterfall_tc.py`, ~375 linhas): 3 tabs (💰 Budget, 📊 Real, 📈 Budget vs Real) com waterfalls Plotly de decomposição (Desp.Primária→Redis→CustoFA→D&ADed→CustoFP), gráficos mensais, análise por oficina e bridge Budget→Flex→Real.
- **BE Simulador TC Veículos** (`best_estimate_simulador_tc.py`, ~480 linhas): geração de Forecast (Real + simulação futura) com parâmetros de sensibilidade fixo/variável, simulador de volume por veículo, rateio FA, e salvamento em `dados/TC_Principal/Forecast/`.
- **BE Análise TC Veículos** (`best_estimate_analise_tc.py`, ~490 linhas): 4 tabs (🔮 Visão Forecast, 📊 KPIs e CPU, 📈 Tendências Mensais, 🏭 Análise por Oficina) espelhando a Home, alimentado por dados de Forecast com fallback para Budget.

### Loaders adicionados em `shared.py`
| Função | Descrição |
|--------|-----------|
| `load_historico_principal()` | Dados consolidados Real multi-ano |
| `load_historico_volume()` | Volume consolidado Real multi-ano |
| `load_historico_custo_fp_veiculo()` | Custo FP por veículo consolidado |
| `load_historico_principal_bud()` | Budget consolidado multi-ano |
| `load_historico_volume_bud()` | Volume Budget consolidado |
| `load_historico_custo_fp_veiculo_bud()` | Custo FP por veículo Budget |
| `load_forecast_completo()` | Forecast (Real + BE) |
| `load_forecast_volume()` | Volume do Forecast |

## 1.4) Mudanças recentes — Fev/2026 (Simulador BE + Gráficos TC Veículos)

### Custos Específicos — Tabela Editável (`st.data_editor`)

O formulário "➕ Adicionar Custo" nos simuladores BE (TC Ext e TC Veículos) foi substituído por uma **tabela editável** usando `st.data_editor`:

- **Layout**: tabela com colunas `Oficina`, `Account`, `Jan`, `Fev`, ..., `Dez`, `Descrição`
  - `Oficina`: `SelectboxColumn` com oficinas disponíveis nos dados
  - `Account`: `SelectboxColumn` com Accounts disponíveis (auto-lookup de Type 06/05/Custo/USI no save)
  - `Jan` a `Dez`: `NumberColumn` (formato `R$ %.2f`) — o usuário coloca o valor no mês desejado
  - `Descrição`: texto livre
- **Referência Account**: expander com tabela de referência Account → Type 06/Type 05/Custo/USI
- **Preview**: ao preencher, mostra preview das linhas que serão criadas com Types resolvidos
- **Salvamento**: ao clicar "💾 Salvar Custos", cada mês com valor > 0 gera uma linha no parquet
  - Salva valor total (sem rateio por veículo)
  - Colunas: `Oficina`, `Período`, `Total`/`Custo FP`, `Account`, `Type 06`, `Type 05`, `Custo`, `USI`, `Descrição`, `Ano`, `Tipo='BE Manual'`
  - Storage: `dados/TC_Ext/Forecast/custos_especificos.parquet` (TC Ext) / `dados/TC_Principal/Forecast/custos_especificos.parquet` (TC Veículos)
- **Rateio por veículo**: movido para a **geração do forecast** (merge). No merge:
  - Para cada custo específico, chama `buscar_rateios_arquivo(oficina, periodo, ano)` para obter % por veículo
  - Expande em N linhas (CC21, CC22, CC24, CC24 5L, CC24 7L, J516) com `valor_rateado = total × rateio_veiculo`
  - Se rateio não encontrado → distribuição igual (1/6 por veículo)
- **Coluna Veículo removida** do editor (custo sempre se aplica a todos; rateio é automático)
- **Campos removidos**: `Tipo_Aplicacao`, `Mes_Inicial`, `Meses_Especificos` — substituídos pelas colunas de meses Jan-Dez (mais flexível: valores diferentes por mês)
- Tab "📋 Visualizar Custos" com AgGrid (visualização + deleção via checkboxes) permanece inalterada

### Gráfico BE Analysis — Flex Bud vermelho + legenda unificada

No gráfico de Custo FP por Período em `tc_principal/pages/best_estimate_analise_tc.py`:

- **Linha Flex Bud**: cor alterada de `#FF6B35` (laranja) para **`#DC2626` (vermelho)**
- **Legendas unificadas**: as 2 legendas separadas (barras à direita + Flex Bud embaixo) foram unificadas em **1 legenda na parte inferior** com 3 itens:
  - `Histórico` (#4C1D95 — roxo escuro)
  - `BE` (#C4B5FD — roxo claro)
  - `Flex Bud` (#DC2626 — vermelho)
- Pontos e rótulos Flex Bud também em vermelho (`#DC2626`)

### Tempo de Produção — Cores roxas, data labels e gráfico de evolução

No dashboard Home TC Veículos (`tc_principal/pages/home_tc.py`), aba "Tempo de Produção":

- **Cores**: todos os gráficos de tempo trocaram de azul (#4A90E2) para **gradiente roxo** (#7C3AED escuro, #C4B5FD claro)
- **Data labels** (`mark_text`): adicionados em todos os gráficos de barras de tempo (formato `,.1f`)
- **Conversor minutos ↔ horas**: toggle `st.radio` no topo da seção permite alternar a unidade de exibição entre minutos e horas. Fator de conversão `÷ 60` aplicado automaticamente em:
  - Gráfico "Tempo Veículo por Oficina"
  - Gráfico "Tempo Veículo vs Tempo FA"
  - Tabela "EST e Tempo por Veículo e Oficina" (coluna renomeada para incluir unidade)
  - Texto indicativo: "⏱️ Valores exibidos em **minutos/horas**"

### Novo gráfico: Evolução Tempo Veículo vs Tempo FA por Período

Adicionado **antes** dos gráficos existentes de tempo, com layout em 2 colunas:

- **Coluna esquerda — Barras empilhadas**: evolução mensal de Tempo Veíc + Tempo FA empilhados por período. Total no topo de cada barra. Cores roxas (#7C3AED / #C4B5FD). Eixo X ordenado por `ORDEM_MESES`.
- **Coluna direita — Linhas % por oficina**: `% Tempo Veículo = Tempo Veic / (Tempo Veic + Tempo FA)` por oficina/período. Uma linha por oficina (BS, GS, PL, PS, QY, SM). Eixo Y em formato percentual (`.0%`).

---

## 2) Stack, execução e ambiente

### Tecnologias
- Python
- Streamlit (UI)
- Pandas / NumPy (ETL e agregações)
- Altair (gráficos no app principal)
- Plotly (gráficos nas páginas Waterfall/Best Estimate)
- OpenPyXL (Excel)
- PyArrow (Parquet)

### Dependências (versões fixas)
As versões estão travadas em `requirements.txt` por estabilidade do Streamlit/pandas.

### Execução
- Portal/roteador (entrada do sistema): `streamlit run app.py`
  - O `app.py` define o menu e roteia as páginas via `st.navigation()`.
  - As páginas não dependem do carregamento automático de `pages/`; o portal explicita quais páginas aparecem no menu.
  - Para depuração pontual, é possível executar uma página diretamente (ex.: `streamlit run "pages/1 - Waterfall.py"`).

---

## 3) Estrutura do repositório (módulos e responsabilidades)

### Arquivos principais
- `app.py`: **Stellantis Cost Intelligence (SCI)** (menu/roteamento) que agrupa:
  - TC Ext (Linhas Secundárias)
  - TC Veículos
  - Documentação (única, global)
- `tc_ext/pages/home_ext.py`: **Home do TC Ext** (código que antes estava no `app.py`).
- `pages/1 - Waterfall.py`: análise Waterfall.
- `pages/2 - Best Estimate - Simulador.py`: simulador.
- `tc_ext/pages/be_analise_ext.py`: **Best Estimate (Análise)** no TC Ext (baseada na Home; usa `dados/Forecast/`).
- *(removido)* `pages/4 - Waterfall_Analysis.py`: página duplicada removida.
- `pages/5 - Extração de Dados.py`: guia/rotinas para extração.
- `pages/6 - Documentacao.py`: documentação dentro do Streamlit.

### Módulo TC Veículos
- `tc_principal/pages/*.py`: páginas funcionais espelhando a estrutura do TC Ext, com lógica completa de análise de Custo FP por veículo.

### Nota sobre roteamento (Streamlit)
- Ao usar `st.navigation()`, o conjunto de páginas exibidas no menu fica centralizado no `app.py`.
- Para evitar conflitos de rota, cada página deve ter `url_path` único (especialmente quando várias páginas exportam uma função `render()`).

### Camada core (refatoração incremental)
- `tc_core/`:
  - `tc_core/data/paths.py`: resolução de caminhos e anos disponíveis.
  - `tc_core/data/schema.py`: checagens mínimas de schema e normalizações de encoding.
  - `tc_core/data/periodos.py`: normalização de meses/períodos.
  - `tc_core/finance/currency.py`: conversão de moeda e símbolo.
  - `tc_core/finance/currency_db.py`: persistência de taxas em SQLite.

### API estável para pages
- `tc_exports.py`: **API pública estável** para páginas em `pages/`.
  - Regra: páginas devem importar daqui (e não de `app.py` e nem de `tc_ext/pages/home_ext.py`) para evitar efeitos colaterais de renderização.

### Processamento de dados
- `processamento_dados.py`: processamento de dados **reais** (convertido de notebook).
- `processamento_dados_BUD.py`: processamento de dados **budget** (convertido de notebook).
- Notebooks: `tc_ext/notebooks/dados.ipynb`, `tc_ext/notebooks/dados_BUD.ipynb` (fontes originais de lógica).

### Chatbot
- `chatbot_documentacao.py`: busca semântica local sobre documentação.

---

## 4) Contratos de dados (schema + semântica)

### 4.1 Arquivos Parquet usados pelo app
O app carrega sempre do **histórico consolidado** e filtra por ano depois (para consistência):

- Real (custo): `dados/historico_consolidado/df_final_historico.parquet`
- Real (volume): `dados/historico_consolidado/df_vol_historico.parquet`
- Budget (custo): `dados/historico_consolidado/BUD/df_final_historico_BUD.parquet`
- Budget (volume): `dados/historico_consolidado/BUD/df_vol_historico_BUD.parquet`

### 4.2 Colunas mínimas (requisitos)
Requisitos mínimos (validados na camada core):

- Real (df_final): `Ano`, `Período`, `Total`
- Volume (df_vol): `Ano`, `Período`, `Volume`

Outras colunas normalmente presentes e usadas para filtros/agregações:
- `Oficina`, `Veículo`, `USI`
- `Custo` (ex.: `Fixo`, `Variável`) — essencial para Flex Bud
- `Type 05`, `Type 06`, `Account`, `Fornecedor`, `Fornec.`, `Tipo`
- Campos avançados: `Custo`, `Type 07`, `Texto breve`, `Material`, `Pedido`, `Ordem`, `CtAtvFixo`, etc.

### 4.3 Período (meses) — normalização obrigatória
Regra crítica: a coluna `Período` deve ser normalizada para meses capitalizados:
- `janeiro` → `Janeiro`
- `marco` → `Março`

Isso evita falhas de merge/ordenação entre fontes.

### 4.4 Tipos de dados e performance
- Colunas numéricas (ex.: `Total`, `Valor`, `Volume`) são convertidas para numérico.
- Colunas `object` com baixa cardinalidade podem virar `category` para reduzir memória.
- O app evita mutação de DataFrames retornados por cache (`df = df.copy()`).

---

## 5) Regras de interface (UI) e filtros

### 5.1 Seleção de ano
- Opções: `Todos` ou anos disponíveis em `dados/<ANO>/`.
- Mesmo selecionando um ano, a fonte continua sendo o histórico consolidado; o filtro é aplicado após o load.

### 5.2 Ordem de aplicação dos filtros (sidebar)
Os filtros refinam `df_total` em `df_filtrado` nesta ordem:
1) `Oficina`
2) `Veículo`
3) `USI`
4) `Período` (multiselect)
5) filtros principais: `Type 05`, `Type 06`, `Account`, `Fornecedor`, `Fornec.`, `Tipo`
6) filtros avançados (expander): `Custo`, `Type 07`, `Texto breve`, `Material`, `Pedido`, `Ordem`, `CtAtvFixo`, etc.

Regra crítica: **o perímetro de filtros aplicado ao custo deve ser replicado no volume** (para CPU e para Flex Bud),
porém **o volume não pode ser recortado pela “existência de custo”**.

Em termos práticos:
- ao combinar custo e volume, manter chaves que existam apenas no volume (custo = 0) — ex.: `merge how='outer'`.
- nunca “intersectar” volume com o conjunto de chaves que aparece no Real.

### 5.3 Modos de visualização
- **Custo Total**: trabalha com `Total` (ou `Valor` quando necessário).
- **CPU (Custo por Unidade)**: sempre deriva de `Total / Volume` e deve ser calculado após agregações.

### 5.4 Regras de exibição (sem mudar cálculo)
Para melhorar legibilidade sem alterar números:
- Linhas que estão **100% zero/NaN** podem ser removidas **apenas na tabela exibida**.
- A coluna `Ano` pode ser removida **apenas na exibição** em tabelas que já estão agregadas por período/recorte, para evitar poluição visual.

Regra: totais, gráficos e exportações devem usar os DataFrames de cálculo (sem esses cortes de exibição).

---

## 6) Conversões e formatação

### 6.1 Fator de conversão (K/M)
- Apenas em **Custo Total**.
- `K`: divide valores por 1.000
- `M`: divide valores por 1.000.000

Regra crítica: **não aplicar fator K/M diretamente em CPU**, porque CPU é uma razão.

### 6.2 Conversão de moeda (BRL, USD, EUR)
A interface recebe taxas no formato:
- `1 USD = R$ X`
- `1 EUR = R$ Y`

O sistema deriva taxas para conversão **BRL → moeda destino**:
- `taxa_brl_para_usd = 1 / taxa_usd_para_brl`
- `taxa_brl_para_eur = 1 / taxa_eur_para_brl`

Conversão aplicada multiplicando:

$$\text{ValorMoeda} = \text{ValorBRL} \times \text{taxa\_brl\_para\_moeda}$$

Regra de ordem:
1) (se Custo Total) aplicar K/M
2) aplicar conversão de moeda
3) então executar cálculos (CPU/Flex Bud/etc.)

Persistência:
- taxas são salvas em SQLite via `tc_core/finance/currency_db.py`.

---

## 7) CPU (Custo por Unidade) — regra crítica

### Definição
$$CPU = \frac{\sum Total}{\sum Volume}$$

### Regra crítica
CPU **sempre** deve ser calculado **após** o agrupamento desejado.

Ex.: para CPU por Oficina e Período:
1) agrupar por (`Oficina`, `Período`, `Ano` opcional) somando `Total` e `Volume`
2) CPU = Total_agregado / Volume_agregado

Importante:
- Em uma linha "Oficina X" (ou qualquer outra dimensão), o denominador **tem que ser o Volume agregado daquela mesma chave** (ex.: Volume da Oficina X no Período).
- Para a linha **Total**, aí sim: $$CPU_{total} = \frac{\sum Total}{\sum Volume}$$ usando o **Volume total** (somado) do mesmo recorte de filtros.
- Evitar calcular CPU a partir de um merge de Volume em dados de custo muito detalhados (ex.: por conta/material), porque isso **duplica Volume** e distorce o resultado. O correto é agregar custo e volume separadamente no mesmo nível e só então dividir.

Nunca calcular CPU linha a linha e depois tirar média/soma.

Tratamento de zeros:
- se `Volume == 0` ou nulo → CPU = 0 (ou NaN dependendo do contexto de exibição; o sistema usa 0 em várias tabelas).

---

## 8) Flex Bud (Budget Flexível) — especificação completa

### 8.1 Conceito
Flex Bud ajusta o budget ao volume real, distinguindo:
- **Custo Fixo**: não varia com volume
- **Custo Não‑Fixo**: tudo que **não** é Fixo (engloba Variável e demais classificações não-fixas)

### 8.2 Fórmulas base
Para um período/dimensão:
- Flex Fixo: $$Flex_{fixo} = BUD_{fixo}$$
- Flex Não‑Fixo: $$Flex_{naofixo} = BUD_{naofixo} \times \frac{Volume_{real}}{Volume_{bud}}$$
- Flex Total: $$Flex_{total} = Flex_{fixo} + Flex_{naofixo}$$

### 8.3 Contexto TC Ext (Real x Budget)
Dimensões típicas (dependem do gráfico/tabela):
- por `Período` (e opcionalmente `Ano`)
- por `Oficina`
- por `Veículo`

Algoritmo (alto nível):
1) Agregar Real por dimensão + `Custo` (Fixo/Variável) somando `Total`.
2) Agregar Volume Real por dimensão somando `Volume`.
3) Agregar Budget por dimensão + `Custo` somando `Total`.
4) Agregar Volume Budget por dimensão somando `Volume`.
5) Para cada chave (dimensão), calcular Flex Bud via fórmulas.

Observação importante (UI):
- Filtros locais de alguns gráficos (ex.: "Filtrar por Oficina" / "Filtrar por Veículo" dentro do gráfico) **não devem alterar** os filtros do sidebar.
- O sidebar define o perímetro global; filtros locais afetam apenas aquela visualização.

### 8.4 Flex Bud no modo CPU
Regra crítica: **não somar CPUs**.

Para CPU, os cálculos internos usam custo total e volume:
- FlexBud_CPU = FlexBud_Total / Volume_Real
- Total_CPU = Total_Real / Volume_Real
- BUD_CPU pode ser calculado como BUD_Total / Volume_Budget (quando disponível)

### 8.5 Meses sem realizado (ano completo)
Regras de exibição/continuidade temporal:
- Os gráficos podem mostrar os 12 meses mesmo sem registros reais.
- Meses sem realizado exibem **Real = 0**.
- Para Flex Bud nesses meses, o comportamento esperado é:
  - se não há volume real, usar o volume do budget como base (equivale a Flex Bud = BUD quando não há ajuste).

---

## 9) Gráficos e tabelas (comportamento esperado)

### 9.1 Gráfico por Período (principal)
Mostra séries (dependendo do modo):
- Custo Total: Total Real, BUD, Flex Bud e deltas/ratios.
- CPU: Total/Volume, Flex/Volume e comparativos.

Regras:
- Ordem cronológica por mês (ORDEM_MESES).
- Sempre evitar “contaminar” Real com valores de Budget.

### 9.2 Gráficos de Volume
- Volume Real e (opcional) Volume Budget.
- Além do gráfico por período, existe visualização por **Oficina** e por **Veículo** (barras), com linha pontilhada de **Volume Budget**.

### 9.3 Gráfico por Oficina
- Agrega por `Oficina`.
- Pode ter linha/indicador de Flex Bud agregado por oficina.

### 9.4 Gráfico por Veículo
- Agrega por `Veículo`.

### 9.5 Tabelas detalhadas
- Tabelas pivot por Oficina/Veículo/Período.
- Para CPU, colunas totais devem ser recalculadas a partir de Total e Volume agregados.

### 9.6 Guia de cálculo por visualização (Normal vs CPU)

Esta seção existe para consulta rápida e para evitar regressões (principalmente em CPU).

#### 9.6.1 Regras globais (valem para qualquer gráfico/tabela)

- **CPU sempre depois de agregar**:

$$CPU = \frac{\sum Total}{\sum Volume}$$

- **Nunca** somar/média de CPU (inclusive em totais).
- **Custo e Volume precisam do mesmo recorte de filtros**, mas:
  - o volume **não pode** ser recortado pela “existência de custo”;
  - ao combinar custo e volume, preservar chaves que existam apenas no volume (custo = 0) → `merge how='outer'`.
- **Normalização de colunas**: usar `padronizar_colunas()` antes de merges (evita `Veículo`/`Veiculo`/`Perdodo`/`Per
3odo` etc.).
- **Conversões**:
  - K/M apenas em Custo Total;
  - conversão de moeda antes dos cálculos;
  - CPU não recebe fator K/M diretamente.

Helper recomendado:
- `tc_ext/metricas_tc_ext.py::cpu_por_chaves(df_custo, df_volume, chaves_preferidas, ...)`.

#### 9.6.2 Gráfico por Período (Real)

- **Modo Custo Total**
  - Real(periodo) = $\sum Total$ agrupado por (`Ano`, `Período`) quando existir `Ano`, senão por `Período`.
- **Modo CPU**
  - Real_CPU(periodo) = $\sum Total / \sum Volume$ no grão (`Ano`, `Período`).
  - Implementação recomendada: `cpu_por_chaves(base_custo, base_vol, chaves_preferidas=("Ano","Período"))`.

#### 9.6.3 Gráfico por Oficina (Real)

- **Modo Custo Total**
  - Real(oficina) = $\sum Total$ agrupado por `Oficina`.
- **Modo CPU**
  - Real_CPU(oficina) = $\sum Total / \sum Volume$ no mesmo recorte de filtros.
  - Cálculo recomendado em 2 etapas:
    1) calcular base por (`Ano`, `Período`, `Oficina`) via `cpu_por_chaves()`;
    2) para exibição “por oficina”, agregar somando `Total` e `Volume` e dividir novamente.

#### 9.6.4 Gráfico por Veículo (Real)

- **Modo Custo Total**
  - Real(veiculo) = $\sum Total$ agrupado por `Veículo`.
- **Modo CPU**
  - Real_CPU(veiculo) = $\sum Total / \sum Volume$ no grão (`Ano`, `Período`, `Veículo`).
  - Implementação recomendada: `cpu_por_chaves(base_custo, base_vol, chaves_preferidas=("Ano","Período","Veículo"))`.

#### 9.6.5 Tabela detalhada (Oficina x Veículo x Período)

- **Modo Custo Total**
  - Célula = $\sum Total$ no grão (`Oficina`, `Veículo`, `Período`, `Ano` opcional).
  - Total da linha = soma das colunas.
- **Modo CPU**
  - Célula = $\sum Total / \sum Volume$ no mesmo grão.
  - **Total da linha e Total geral**: recalcular como razão ponderada:

$$CPU_{total} = \frac{\sum Total}{\sum Volume}$$

  - Nunca somar as CPUs dos meses.

#### 9.6.6 Linhas de comparação (Budget / Flex Bud / Forecast)

**Budget (BUD)**
- Custo Budget vem de `df_final_historico_BUD.parquet` (ou equivalente filtrado).
- Volume Budget vem de `df_vol_historico_BUD.parquet`.
- Para CPU:
  - BUD_CPU(chave) = $BUD_{Total}(chave) / Volume_{Budget}(chave)$.

**Flex Bud**
- Flex Bud ajusta Budget ao volume real, separando **Fixo** e **Não-Fixo**.
- Para CPU:
  - FlexBud_CPU(chave) = $FlexBud_{Total}(chave) / Volume_{Real}(chave)$.

**Governança do Volume Budget (`Veículo`)**
- O Volume Budget **deve** conter `Veículo`.
- Se `Veículo` estiver ausente no Volume Budget, isso é **erro de extração** (o app não faz mais rateio/fallback).
- Motivo: evitar comparativos inconsistentes e impedir que linhas “budget-only” sejam perdidas por falta de denominador.

#### 9.6.7 Waterfall (Normal vs CPU)

Regras específicas da página Waterfall (principalmente para evitar divergências Budget/Flex):

- **Calcular em Total primeiro**: os efeitos do Waterfall (BUD, Real, Flex Bud, deltas) são calculados em **custo total**; CPU é derivada depois como razão ponderada.
- **CPU no Waterfall (regra)**: para qualquer barra/tabela exibida em CPU,

$$CPU = \frac{\sum Total}{\sum Volume}$$

- **Período com ano (anti-mistura 2025/2026)**: quando houver seleção de ano (ou quando `Ano` existir), filtrar por (`Ano`, `Período`) (ex.: chave `Período_Ano`) e evitar matching “mês-only”.
- **Filtros (anti-intersection)**: Budget e Volume Budget no Waterfall **não** devem ser filtrados pelo conjunto de valores presentes no Real; aplicar apenas os filtros do sidebar (Oficina/Veículo/USI/Type/...).
- **Flex Bud no modo Budget (Waterfall)**:
  - `Fixo` permanece inalterado.
  - Tudo que **não** for `Fixo` é tratado como flexível (não depender de string exata `Variável`).
  - Fórmula em custo:

$$Total_{Flex} = Total_{Fixo} + Total_{NaoFixo} \times \frac{Volume_{Real}}{Volume_{Budget}}$$

  - Em CPU: $CPU_{Flex} = Total_{Flex} / Volume_{Real}$ (com tratamento para volume zero).

**Forecast / Best Estimate**
- O Forecast (Best Estimate) usa custo projetado (outputs do simulador em `dados/Forecast/`).
- Para comparação “Forecast vs Budget”, a regra é a mesma: alinhar o grão e calcular CPU como razão ponderada.
- Recomendação: expor um seletor de baseline (Budget vs Flex Bud) mantendo as mesmas fórmulas acima.

---

## 10) Processamento de dados (ETL)

### 10.1 Real
Fonte: Excel (ex.: `Dados SAPIENS.xlsx`, `Reporting fluxo anexo.xlsx`).
Processamento:
- Ler planilhas, limpar colunas duplicadas/Unnamed.
- Normalizar período.
- Garantir numéricos (especialmente `Volume`).
Saída anual:
- `dados/<ANO>/df_final.parquet`
- `dados/<ANO>/df_vol.parquet`

Consolidação:
- gerar/atualizar `dados/historico_consolidado/df_final_historico.parquet` e `df_vol_historico.parquet`.

### 10.2 Budget
Processamento similar, com saídas em:
- `dados/<ANO>/BUD/df_final_BUD.parquet`
- `dados/<ANO>/BUD/df_vol_BUD.parquet`

Consolidação:
- `dados/historico_consolidado/BUD/df_final_historico_BUD.parquet`
- `dados/historico_consolidado/BUD/df_vol_historico_BUD.parquet`

---

## 11) Critérios de aceitação (para reescrita com IA)

Uma reescrita deve passar nos seguintes critérios:

1) **Consistência de filtros**: qualquer filtro aplicado ao custo deve reduzir o volume no mesmo perímetro.
2) **CPU correto**: CPU sempre = soma(Total)/soma(Volume) no nível de agregação exibido.
3) **Flex Bud correto**: fixo não muda; variável escala por VolumeReal/VolumeBudget.
4) **CPU Flex**: calculado por custos e volumes agregados (nunca soma/média de CPU).
5) **Ano completo**: gráficos podem exibir 12 meses; meses sem realizado com Real = 0.
6) **Conversões**: fator K/M somente no modo Custo Total; moeda aplicada conforme taxa derivada.
7) **Sem mutação do cache**: DataFrames cacheados não devem ser alterados in-place.
8) **Exportações**: downloads devem refletir exatamente os filtros do usuário.

---

## 12) Pontos sensíveis / armadilhas comuns

- Misturar meses minúsculos e maiúsculos quebra merge e ordenação → normalização é obrigatória.
- CPU calculado antes de agrupar gera erro silencioso (resultados errados sem crash).
- Volume não filtrado igual ao custo “destrói” CPU e Flex Bud.
- Mutação de DataFrames retornados por `st.cache_data` causa efeitos colaterais entre execuções.

---

## 13) Onde alterar/extender com segurança

- Novas regras de cálculo: ideal extrair para `tc_core/` e expor por `tc_exports.py`.
- Novos dashboards/páginas: criar em `pages/` e importar helpers de `tc_exports.py`.
- Novos contratos de dados: documentar no topo deste arquivo e criar check em `tc_core/data/schema.py`.

---

**Última atualização:** 2026-02-15
