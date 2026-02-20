# 📚 Documentação Técnica Completa — Stellantis Cost Intelligence (SCI) (Especificação para Reescrita)

> Objetivo deste documento: servir como **fonte única de verdade** (single source of truth) para reescrever o projeto com IA **mantendo 100% das funcionalidades, regras de cálculo e comportamento da interface**.
>
> Escopo: Dashboard Streamlit (página principal + páginas em `pages/`), processamento de dados (scripts/notebooks) e chatbot de documentação.

---

## 1) Visão geral (o que o sistema faz)

O Stellantis Cost Intelligence (SCI) é um conjunto de dashboards (Streamlit) para análise de custos e volumes de uma operação industrial, com foco em:

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

**Nota de contexto (linha do tempo):** apesar do título desta seção, o conteúdo descreve o estado consolidado do módulo conforme implementado e mantido até **Fev/2026** neste repositório.

### Alterações de infraestrutura
- **Paths corrigidos** (Bloco 1): 25+ referências de caminhos quebrados consertadas em `tc_exports.py`, `pages/1 - Waterfall.py`, `pages/6 - Documentacao.py`, `pages/2 - Best Estimate - Simulador.py`, `tc_ext/pages/home_ext.py`.
- **Renomeação UI** (Bloco 2): "TC Principal" / "Planta Principal" → "TC Veículos" em todos os arquivos Python e Markdown.
- **Pipeline historico_consolidado** (Bloco 5): funções `consolidar_historico_tc_veiculos()` adicionadas em `processamento_dados_veiculos.py` e `processamento_dados_veiculos_BUD.py`; 9 loaders novos em `shared.py` para dados multi-ano e Forecast.
- **Import circular corrigido**: `processamento_dados_veiculos_BUD.py` — import movido para dentro da função (lazy import).

### Páginas implementadas
- **Waterfall TC Veículos** (`waterfall_tc.py`, ~375 linhas): 3 tabs (💰 Budget, 📊 Real, 📈 Budget vs Real) com waterfalls Plotly de decomposição (Desp.Primária→Redis→CustoFA→D&ADed→CustoFP), gráficos mensais, análise por oficina e bridge Budget→Flex→Real.
- **BE Simulador TC Veículos** (`best_estimate_simulador_tc.py`, ~480 linhas): geração de Forecast (Real + simulação futura) com parâmetros de sensibilidade fixo/variável, simulador de volume por veículo, rateio FA, e salvamento em `dados/TC_Principal/Forecast/`.
- **Debug de Cálculos TC Veículos** (`debug_calculos_tc.py`): auditoria (parquets, fechamentos e provas cruzadas) para rastrear FA/FP/D&A/rateios/CPU.

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

### Forecast (TC Veículos) — onde aparece no sistema

- O Forecast é gerado no simulador (`tc_principal/pages/best_estimate_simulador_tc.py`) e salvo em `dados/TC_Principal/Forecast/`.
- A Home do TC Veículos consome esses outputs quando disponíveis (mantendo as regras de CPU e de fator/moeda).
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

## 1.5) Correção crítica — EXE fecha silencioso (`@st.cache_data` violations)

### Causa raiz do crash silencioso no EXE

Quando o SCI é empacotado como executável PyInstaller (one-dir), qualquer chamada a funções Streamlit **dentro** de uma função decorada com `@st.cache_data` mata o processo Streamlit sem exibir mensagem de erro.

Funções proibidas dentro de `@st.cache_data`:

| Proibido | Efeito |
|---|---|
| `st.error(...)` | Crash silencioso |
| `st.info(...)` | Crash silencioso |
| `st.stop()` | Crash silencioso |
| `st.session_state[x] = ...` | Crash silencioso |
| `st.session_state.get(x)` | Crash silencioso |
| `st.code(...)` | Crash silencioso |

### Padrão correto (aplicado a todas as funções afetadas)

```python
# ❌ ERRADO — trava o EXE silenciosamente:
@st.cache_data(ttl=3600)
def load_data(ano):
    if not os.path.exists(arquivo):
        st.error("Arquivo não encontrado")  # PROIBIDO
        st.stop()                            # PROIBIDO

# ✅ CORRETO — levanta exceção, UI trata no call site:
@st.cache_data(ttl=3600)
def load_data(ano):
    if not os.path.exists(arquivo):
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")
    try:
        ...
    except (FileNotFoundError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar dados: {e}") from e

# Call site (fora do cache, na renderização da página):
try:
    df = load_data(ano)
except Exception as e:
    st.error(str(e))
    st.stop()
```

### Arquivos corrigidos (Jul/2026)

| Arquivo | Função | Violação removida |
|---|---|---|
| `tc_ext/pages/home_ext.py` | `load_data()` (x2) | `st.error/info/stop` |
| `tc_ext/pages/be_analise_ext.py` | `load_data()` | `st.error/info/stop` |
| `tc_ext/pages/be_analise_ext.py` | `load_volume_data()` | `st.session_state` |
| `tc_ext/pages/be_analise_ext.py` | `create_volume_chart()`, `create_volume_veiculo_chart()`, `create_volume_oficina_chart()` | `st.error/st.code` → `print()` |
| `tc_ext/pages/home_ext.py` | `create_volume_chart()`, `create_volume_veiculo_chart()`, `create_volume_oficina_chart()` | `st.error/st.code` → `print()` |
| `pages/2 - Best Estimate - Simulador.py` | `load_data()` | `st.error/info/stop` |
| `pages/2 - Best Estimate - Simulador.py` | `calcular_medias_forecast()` | `st.session_state` |
| `tc_principal/pages/best_estimate_simulador_tc.py` | `load_data()` | `st.error/info/stop` |
| `tc_principal/pages/best_estimate_simulador_tc.py` | `calcular_medias_forecast()` | `st.session_state` |
| `tc_core/utils/portabilidade.py` | — | Removidas definições duplicadas de `get_output_path()` e `resolve_path()` |
| `app.py` | — | Removido BOM (U+FEFF); `Path(__file__)` → `get_base_path()` |

### Regra permanente

> **Nunca** use `st.*` ou `st.session_state.*` dentro de qualquer função decorada com `@st.cache_data` ou `@st.cache_resource`.  
> Para debug, use `print()` (vai para o log do servidor Streamlit).  
> Para invalidação por mtime, passe o mtime como parâmetro da função (o Streamlit invalida automaticamente quando o argumento muda).

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
- Aplicado às **colunas monetárias** exibidas (custos/receitas), antes dos cálculos subsequentes.
- `K`: divide valores por 1.000
- `M`: divide valores por 1.000.000

Regra crítica (CPU): CPU é uma razão; **não faz sentido aplicar K/M no valor final**.
No dashboard, o fator K/M é aplicado nas colunas monetárias antes do cálculo, então a CPU exibida também escala com o fator.
Para CPU em unidade monetária “real”, usar `Fator = Nenhum`.

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
1) aplicar K/M nas colunas monetárias (quando selecionado)
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

Observação de implementação (dashboard): o sistema aplica fator/moeda nas colunas de custo e depois recalcula CPU como `Total/Volume`.
Ou seja: a CPU acompanha a moeda selecionada e também é afetada pelo fator K/M se ele estiver ativo.

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

## 10) Processamento de Dados — TC Ext (Linhas Secundárias)

> Estes pipelines processam dados do TC Ext. Para o TC Veículos, veja a **seção 14**.

### 10.1 Real (TC Ext)
Fonte: Excel (`Reporting fluxo anexo.xlsx` + `Dados SAPIENS.xlsx`).
Processamento (`processamento_dados.py`):
- Ler planilhas, limpar colunas duplicadas/Unnamed.
- Normalizar período.
- Garantir numéricos (especialmente `Volume`).
Saída anual:
- `dados/TC_Ext/<ANO>/df_final.parquet`
- `dados/TC_Ext/<ANO>/df_vol.parquet`

Consolidação:
- `dados/TC_Ext/historico_consolidado/df_final_historico.parquet` e `df_vol_historico.parquet`.

### 10.2 Budget (TC Ext)
Processamento (`processamento_dados_BUD.py`), com saídas em:
- `dados/TC_Ext/<ANO>/BUD/df_final_BUD.parquet`
- `dados/TC_Ext/<ANO>/BUD/df_vol_BUD.parquet`

Consolidação:
- `dados/TC_Ext/historico_consolidado/BUD/df_final_historico_BUD.parquet`
- `dados/TC_Ext/historico_consolidado/BUD/df_vol_historico_BUD.parquet`

---

## 14) Processamento de Dados — TC Veículos (Passo a Passo)

> Esta seção documenta com 100% de fidelidade ao código cada etapa dos pipelines de processamento do módulo TC Veículos. Todos os nomes de colunas, fórmulas e ordem de execução são os **exatos** do código.

### 14.1 Constantes Globais

| Constante | Valor | Arquivo |
|-----------|-------|---------|
| `OFICINAS_RATEIO_AUTOMATICO` | `['BS', 'PS', 'PL']` | `processamento_dados_veiculos_BUD.py` |
| `OFICINAS_RATEIO_MANUAL` | `['QY', 'GS', 'SM']` | `processamento_dados_veiculos_BUD.py` |
| `OFICINAS_EXCLUIR_DENOM_TAXA_PDR` | `['GS', 'SM']` | `processamento_dados_veiculos_BUD.py` |

**Fatores de rateio manual** (`rateios_manuais.json`):
```json
{"QY": 0.087526, "GS": 0.086982, "SM": 0.075452}
```

### 14.2 Arquivo Excel Fonte

**Arquivo:** `dados/TC_Principal/{ano}/Reporting veículos.xlsx`

**Abas utilizadas pelo Pipeline Real:**
| Aba | Conteúdo |
|-----|----------|
| `Sapiens` | Despesas primárias (formato longo — 1 linha por período) |
| `massa - REDIS` | Receita Redis (formato wide — meses como colunas) |
| `Volume e EST PdR - Actual` | Volume FA e EST (peças) por oficina |
| `Volume Actual` | Volume de veículos por período |
| `EST veículos - Actual` | EST (tempo padrão) por veículo e oficina |

**Abas adicionais utilizadas pelo Pipeline Budget:**
| Aba | Conteúdo |
|-----|----------|
| `massa primária - BDG` | Despesas primárias Budget (formato wide) |
| `Volume e EST PdR - BDG` | Volume FA e EST Budget |
| `Volume BDG` | Volume de veículos Budget |
| `EST veículos - BDG` | EST por veículo Budget |
| `massa - D&A dedicado` | D&A Dedicado por veículo/oficina |

---

### 14.3 Pipeline Real (Sapiens) — `processamento_dados_veiculos.py`

**Função orquestradora:** `processar_veiculos_real(ano)`

**Pasta de saída:** `dados/TC_Principal/{ano}/` (raiz, sem subpasta BUD)

#### Fase 0 — Configuração do Ambiente
**Função:** `configurar_ambiente(ano)`
1. Define caminhos para pastas de dados, BUD e histórico
2. Localiza `Reporting veículos.xlsx`
3. Valida existência das 5 abas obrigatórias
4. Carrega `rateios_manuais.json`
5. **Detecta oficinas válidas** lendo `df_principal_BUD.parquet` (coluna `Oficina`, valores únicos) → armazena em `config['OFICINAS_BUD']` para filtro posterior

#### Fase 1 — Sapiens → Despesa Primária
**Função:** `fase1_sapiens(config)`
1. Lê aba `Sapiens` com `header=1`
2. Remove colunas duplicadas (sufixo `.1`, `.2`)
3. Corrige mojibake nos nomes de colunas (`Per�odo` → `Período`)
4. Aplica alias de colunas (`N°conta → Nºconta`, `Veiculo → Veículo`, etc.)
5. Exige colunas: `Oficina`, `Account`, `Período`, `Valor`
6. **Renomeia** `Valor` → `Despesa Primaria`
7. Converte `Despesa Primaria` para numérico (`pd.to_numeric`, errors='coerce', fillna 0)
8. Normaliza `Período` (capitaliza: `janeiro → Janeiro`, corrige mojibake)
9. Remove linhas onde `Oficina` é NaN ou vazia
10. Remove linhas onde `Despesa Primaria == 0`
11. **⚠️ EXCLUI linhas com `Account == 'Redis'`** — Redis vem da aba própria, não do Sapiens
12. **⚠️ EXCLUI oficinas ausentes no Budget** — filtra por `config['OFICINAS_BUD']` usando `isin()`
13. Remove coluna `Ano` se existir
14. Garante existência de `Type 05`, `Type 06`, `Custo` (cria vazias se ausentes)

**Formato:** os dados do Sapiens já vêm no formato **longo** (1 linha por período) — não requer melt.

#### Fase 1B — massa - REDIS → Receita
**Função:** `fase1b_redis(config)`
1. Lê aba `massa - REDIS`
2. Corrige mojibake nas colunas
3. Detecta colunas de meses (prefixo `jan`, `fev`, `mar`, etc.)
4. Remove coluna `Ano` se existir
5. **Melt** colunas de meses → `Período` + `Despesa Primaria`
6. Normaliza `Período`
7. Converte `Despesa Primaria` para numérico
8. **⚠️ Inversão de sinal:** `Despesa Primaria = -abs(Despesa Primaria)` — Redis é receita, SEMPRE fica negativo
9. Remove linhas sem `Oficina` ou com `Despesa Primaria == 0`
10. Garante colunas `Type 05`, `Type 06`, `Account`, `Custo` (preserva valores do Excel se existirem)
11. **Marca** coluna `_fonte_redis = True`
12. Exclui oficinas ausentes no Budget
13. **Agrega** por `[Oficina, Período, Type 05, Type 06, Account, Custo, _fonte_redis]` somando `Despesa Primaria`

#### Concatenação — Sapiens + Redis
1. Marca `_fonte_redis = False` nas linhas do Sapiens
2. `pd.concat([df_sapiens, df_redis], ignore_index=True)` → `df_principal`
3. Preenche NaN em `Despesa Primaria` com 0 e `_fonte_redis` com `False`

#### Fase 2 — Volume e EST PdR (Actual) → Tempo FA
**Função:** `fase2_volume_est_fa(config)`
1. Lê aba `Volume e EST PdR - Actual` (header detectado automaticamente)
2. Corrige mojibake, renomeia: `ref* → REF FER`, `oficina → Oficina`, `est → EST`
3. Exige `Oficina`, `EST`; converte `EST` para numérico
4. Detecta colunas de meses, **melt** → `Período` + `Vol FA`
5. Normaliza `Período`, converte `Vol FA` para numérico
6. **Calcula:**

$$\text{Tempo FA} = \text{Vol FA} \times \text{EST}$$

7. Remove linhas sem `Oficina`

#### Fase 3 — Volume Veículos (Actual)
**Função:** `fase3_volume_veiculos(config)`
1. Lê aba `Volume Actual` com `header=1`
2. Renomeia primeira coluna para `Veículo`
3. Remove linhas `Veículo` = 'total', 'nan', ''
4. **Melt** colunas de meses → `Período` + `Volume`
5. Normaliza `Período`, converte `Volume` para numérico

> **Nota:** Esta aba NÃO tem coluna `Oficina` — é volume por Veículo/Período apenas.

#### Fase 4 — Tempo Veículos (EST × Volume)
**Função:** `fase4_tempo_veiculos(config, df_vol)`
1. Lê aba `EST veículos - Actual` com `header=1`
2. Renomeia colunas: `oficina → Oficina`, `est → EST`, `veículo/modelo → Veículo`
3. Mantém apenas `[Oficina, Veículo, EST]`
4. Converte `EST` para numérico
5. **Merge inner** com `df_vol` (Fase 3) em `[Veículo]`
6. **Calcula:**

$$\text{Tempo Veic} = \text{Volume} \times \text{EST}$$

#### Fase 5 — Rateio FA ⭐
**Função:** `fase5_rateio_fa(config, df_fa, df_tempo_veic)`

> Esta é a fase mais complexa e crítica do pipeline.

**Preparação:**
1. Agrega `Tempo FA` por `(Oficina, Período)` → `Tempo FA Total`
2. Agrega `Tempo Veic` por `(Oficina, Período)` → `Tempo Veic Total`
3. **Merge outer** dos dois (garante que oficinas que existem apenas em um lado sejam incluídas)
4. Preenche NaN com 0

**Rateio Automático** (oficinas **BS, PS, PL**):

$$\text{Rateio FA} = \frac{\text{Tempo FA Total}}{\text{Tempo FA Total} + \text{Tempo Veic Total}}$$

Se denominador = 0, Rateio FA = 0.

**Rateio Manual** (oficinas **QY, GS, SM**):

Para cada período único:
1. Calcula tempo FA global (TODAS as oficinas naquele período):

$$\text{TFA}_{\text{global}} = \sum_{\text{todas oficinas}} \text{Tempo FA Total}_{\text{período}}$$

2. Calcula tempo veículo global, **excluindo GS e SM do denominador**:

$$\text{TVC}_{\text{global}} = \sum_{\substack{\text{todas oficinas} \\ \text{exceto GS, SM}}} \text{Tempo Veic Total}_{\text{período}}$$

3. Calcula taxa PdR:

$$\text{Taxa PdR} = \frac{\text{TFA}_{\text{global}}}{\text{TVC}_{\text{global}}}$$

4. Para cada oficina manual, aplica o fator do JSON:

$$\text{Rateio FA} = \text{fator\_manual} \times \text{Taxa PdR}$$

Onde `fator_manual` é: QY = 0.087526, GS = 0.086982, SM = 0.075452.

5. Se a combinação (Oficina, Período) não existir nos dados, uma nova linha é criada.
6. **Clipa** todos os valores de `Rateio FA` ao intervalo **[0, 1]**.

**Saída:** DataFrame com colunas `[Oficina, Período, Rateio FA]`

#### Fase 6 — Custo FA
**Função:** `fase6_custo_fa(df_principal, df_rateio)`
1. **Merge left** de `df_principal` com `df_rateio` em `[Oficina, Período]`
2. Preenche `Rateio FA` NaN com 0
3. **⚠️ Regra Redis:** para linhas onde `_fonte_redis == True`, força `Rateio FA = 0` — **Redis NÃO participa do rateio FA e vai integralmente para FP**
4. **Calcula:**

$$\text{Custo FA} = \text{Rateio FA} \times \text{Despesa Primaria}$$

#### Fase 7 — Custo FP
**Função:** `fase7_custo_fp(df_principal)`

$$\text{Custo FP} = \text{Despesa Primaria} - \text{Custo FA}$$

**Prova cruzada:** valida que $|\text{DP} - \text{FA} - \text{FP}| < 0{,}01$ para cada linha.

#### Fase 8 — D&A Dedicado (do Budget)
**Função:** `fase8_dea_dedicado(config)`
1. Carrega `df_dea_dedicado_BUD.parquet` da pasta BUD
2. Se não existir, retorna `None` (D&A = 0 nas fases seguintes)

> **Nota:** Para o Real, a D&A vem do Budget (não muda mês a mês).

#### Fase 9 — FP sem Dedicada
**Função:** `fase9_fp_sem_dedicada(df_principal, df_dea)`

Se D&A disponível:
1. Agrega D&A por `[Oficina, Account, Período]` → `_dea_grupo`
2. Merge com `df_principal`
3. **Distribuição pro-rata por Custo FP:**

$$\text{D\&A dedicado}_{\text{linha}} = \text{D\&A}_{\text{grupo}} \times \frac{\text{Custo FP}_{\text{linha}}}{\sum \text{Custo FP}_{\text{grupo}}}$$

4. **Calcula:**

$$\text{FP sem Dedicada} = \text{Custo FP} - \text{D\&A dedicado}$$

Se D&A indisponível: `D&A dedicado = 0` e `FP sem Dedicada = Custo FP`.

#### Fase 10 — Salvamento Principal
**Função:** `fase10_salvamento(config, ...)`

Adiciona coluna `Ano` e salva na pasta `dados/TC_Principal/{ano}/`:

| Arquivo | Conteúdo |
|---------|----------|
| `df_principal.parquet` | Tabela principal (DP, FA, FP, D&A, FP sem Ded) |
| `df_volume_fa.parquet` | Volume FA + Tempo FA |
| `df_tempo_veiculos.parquet` | Tempo veículos (EST × Volume) |
| `df_vol_veiculos.parquet` | Volumes veículos (Actual) |
| `df_dea_dedicado.parquet` | D&A Dedicado (se disponível) |

#### Fase 11 — Isolamento FP sem D&A
**Função:** `fase11_custo_fp_sem_da(df_principal)`

Extrai subconjunto de colunas para rastreabilidade: `[Oficina, Account, Período, Type 05, Type 06, Custo, Custo FP, D&A dedicado, FP sem Dedicada]`

#### Fase 12 — Percentual de Rateio por Veículo
**Função:** `fase12_percentual_rateio_veiculos(df_tempo_veic)`
1. Agrega `Tempo Veic` por `(Oficina, Período)` → `Total_Tempo_Oficina`
2. **Calcula:**

$$\text{Percentual} = \frac{\text{Tempo Veic}_{\text{veículo}}}{\text{Total\_Tempo\_Oficina}}$$

3. **Validação:** $\sum \text{Percentual} = 1{,}0$ por `(Oficina, Período)`

#### Fase 13 — Custo Rateado por Veículo
**Função:** `fase13_custo_rateado_veiculos(df_principal, df_percentual)`
1. Merge de `df_principal` × `df_percentual` em `[Oficina, Período]` → **expande** cada linha para N veículos
2. Fallback para linhas sem veículo: distribuição pro-rata pela média do período
3. **Calcula:**

$$\text{Custo Rateado} = \text{FP sem Dedicada} \times \text{Percentual}$$

4. **Validação:** $\sum \text{Custo Rateado} \approx \sum \text{FP sem Dedicada}$

#### Fase 14 — Custo FP Veículo
**Função:** `fase14_custo_fp_veiculo(df_custo_rateado, df_dea, df_principal)`
1. Se D&A tem `Veículo`: agrega D&A por `[Oficina, Veículo, Account, Período]`, distribui por count (1/N linhas no grupo)
2. Se sem D&A: `D&A dedicado = 0`
3. **Calcula:**

$$\text{Custo FP Veiculo} = \text{Custo Rateado} + \text{D\&A dedicado}$$

4. **Validação:** $|\sum \text{Custo FP original} - \sum \text{Custo FP Veiculo}| < 1{,}0$

#### Fase 15 — CPU por Veículo
**Função:** `fase15_cpu_veiculo(df_custo_fp_veiculo, df_vol)`
1. Agrega `Custo FP Veiculo` por `(Veículo, Período)` → soma todas oficinas/accounts
2. Agrega `Volume` por `(Veículo, Período)`
3. **Calcula:**

$$\text{CPU} = \frac{\sum \text{Custo FP Veiculo}}{\text{Volume}}$$

Se Volume = 0: CPU = 0.

#### Fase 16 — Salvamento Veículos
**Função:** `fase16_salvamento_veiculos(config, ...)`

Salva na pasta `dados/TC_Principal/{ano}/`:

| Arquivo | Conteúdo |
|---------|----------|
| `df_veiculos_fp_sem_da.parquet` | FP sem D&A (base rateio) |
| `df_veiculos_percentual_rateio.parquet` | % rateio por veículo |
| `df_veiculos_custo_rateado.parquet` | FP sem Ded × Percentual |
| `df_veiculos_custo_fp.parquet` | Custo FP final por veículo |
| `df_veiculos_cpu.parquet` | CPU por veículo |

#### Fase 17 — Comparativo Real × Budget
**Função:** `fase17_comparativo(config, df_principal_real)`
1. Carrega `df_principal_BUD.parquet`
2. Agrega ambos por `(Oficina, Período)` somando `Despesa Primaria`, `Custo FA`, `Custo FP`
3. Merge outer com sufixos `_Real` / `_Budget`
4. Calcula diferenças: `Diff_X = X_Real − X_Budget`
5. Salva `df_comparativo_real_budget.parquet`

#### Fase 18 — Validação Final
**Função:** `validacao_final(config, arquivos)`

Verifica existência de cada parquet, presença da coluna `Ano`, número de períodos, e prova cruzada: $|\text{DP} - \text{FA} - \text{FP}| < 0{,}01$.

#### Fase 19 — Consolidação Histórico
**Função:** `consolidar_historico_tc_veiculos(tipo='real')`

Descobre anos em `dados/TC_Principal/`, concatena parquets multi-ano em `dados/TC_Principal/historico_consolidado/`:

| Arquivo Consolidado | Fonte por ano |
|---------------------|---------------|
| `df_principal_historico.parquet` | `df_principal.parquet` |
| `df_vol_historico.parquet` | `df_vol_veiculos.parquet` |
| `df_cpu_historico.parquet` | `df_veiculos_cpu.parquet` |
| `df_veiculos_custo_fp_historico.parquet` | `df_veiculos_custo_fp.parquet` |

---

### 14.4 Pipeline Budget (BDG) — `processamento_dados_veiculos_BUD.py`

**Função orquestradora:** `processar_veiculos_budget(ano)`

**Pasta de saída:** `dados/TC_Principal/{ano}/BUD/`

> O pipeline BUD segue a mesma lógica do Real com as seguintes diferenças:

#### Diferenças na leitura de dados

| Fase | Real (Sapiens) | Budget (BDG) |
|------|----------------|--------------|
| 1 | Lê `Sapiens` (formato **longo**) | Lê `massa primária - BDG` (formato **wide** → **melt** necessário) |
| 2 | Volume de `Volume e EST PdR - Actual` | Volume de `Volume e EST PdR - BDG` |
| 3 | Volume de `Volume Actual` | Volume de `Volume BDG` **+ também** `Volume Actual` (para Flex Bud) |
| 4 | EST de `EST veículos - Actual` | EST de `EST veículos - BDG` |
| 8/10 | D&A carregado do parquet BUD | D&A lido diretamente do Excel (`massa - D&A dedicado`) |

#### Fase 1 BUD — massa primária - BDG → Despesa Primária
**Função:** `fase1_voz_de_custo(config)`

**Diferença-chave:** os dados BDG vêm no formato wide (meses como colunas), exigindo **melt**:
1. Lê aba `massa primária - BDG`
2. Detecta colunas de meses (prefixo jan/fev/mar...)
3. **Melt:** dimensões × meses → `Período` + `Despesa Primaria`
4. Normaliza `Período`, preenche NaN com 0
5. Exige: `Oficina, Account, Período, Despesa Primaria`

#### Fase 2 BUD — massa - REDIS (idêntica ao Real)
Mesma lógica: lê `massa - REDIS`, melt, inverte sinal `(-abs())`, marca `_fonte_redis=True`.

#### Fases 7-9 (Rateio FA, Custo FA, Custo FP) — Idênticas ao Real
Mesmas fórmulas, mesmos fatores manuais do `rateios_manuais.json`.

#### Fase 10 BUD — D&A Dedicado
**Função:** `fase10_dea_dedicado(config)`

**Diferença-chave:** lê diretamente do Excel (aba `massa - D&A dedicado`) em vez de carregar parquet:
1. Lê a aba com meses como colunas
2. **Melt** → `Período` + `D&A dedicado`
3. Identifica coluna `Veículo` (se existir)

#### Arquivos de Saída

| Arquivo | Conteúdo |
|---------|----------|
| `df_principal_BUD.parquet` | Tabela principal BUD |
| `df_volume_fa_BUD.parquet` | Volume FA + Tempo FA |
| `df_tempo_veiculos_BUD.parquet` | Tempo veículos |
| `df_vol_veiculos_BUD.parquet` | Volumes veículos (BDG) |
| `df_vol_veiculos_actual.parquet` | Volumes veículos (Actual) |
| `df_dea_dedicado_BUD.parquet` | D&A Dedicado |
| `df_veiculos_fp_sem_da_BUD.parquet` | FP sem D&A |
| `df_veiculos_percentual_rateio_BUD.parquet` | % rateio por veículo |
| `df_veiculos_custo_rateado_BUD.parquet` | Custo rateado |
| `df_veiculos_custo_fp_BUD.parquet` | Custo FP final por veículo |
| `df_veiculos_cpu_BUD.parquet` | CPU por veículo |

#### Consolidação Histórico BUD

| Arquivo Consolidado | Pasta |
|---------------------|-------|
| `df_principal_historico_BUD.parquet` | `historico_consolidado/BUD/` |
| `df_vol_historico_BUD.parquet` | `historico_consolidado/BUD/` |
| `df_cpu_historico_BUD.parquet` | `historico_consolidado/BUD/` |
| `df_veiculos_custo_fp_historico_BUD.parquet` | `historico_consolidado/BUD/` |

---

### 14.5 Construção da Simulação BE (Forecast) — `best_estimate_simulador_tc.py`

**Pasta de saída:** `dados/TC_Principal/Forecast/`

#### Visão Geral do Fluxo

```
Dados Históricos (Real)          Custos Específicos (manuais)
         ↓                                  ↓
   Motor de Forecast               Rateio por veículo
         ↓                                  ↓
   Tipo = 'BE'                      Tipo = 'BE Manual'
         ↓                                  ↓
         └──────── Concat ──────────────────┘
                      ↓
              Tipo = 'Histórico'  (meses já realizados)
                      +
                 forecast_completo.parquet
```

#### Etapa 1 — Carregamento dos Dados Base

1. **Dados Históricos Real:** carrega de `df_principal_historico.parquet` ou `df_principal.parquet` (via prioridade Forecast → ano → histórico)
2. **Volume Histórico:** carrega de `df_vol_historico.parquet`
3. **Volume Budget:** carrega de `df_vol_veiculos_BUD.parquet` (usado como fallback de volume)
4. **Custos Específicos:** carrega de `dados/TC_Principal/Forecast/custos_especificos.parquet` (se existir)

#### Etapa 2 — Configuração do Usuário (UI)

O simulador oferece controles interativos:
- **Último período realizado**: qual o último mês com dados reais
- **Meses para prever**: quais meses devem receber projeção
- **Meses base para média**: quais meses históricos usar como referência
- **Sensibilidade global**: sliders separados para Fixo (default 0%) e Variável (default 100%)
- **Sensibilidade por Type 06**: override granular por classificação contábil
- **Inflação**: percentual aplicado sobre a projeção
- **Modelos ref Budget**: veículos que devem usar o custo BUD como base ao invés da média histórica

#### Etapa 3 — Motor de Forecast (Fórmula Central)

Para cada linha de custo e cada período futuro:

$$\text{Forecast} = \bar{C}_{\text{hist}} \times \left(1 + \left(\frac{V_{\text{mês}}}{\bar{V}_{\text{hist}}} - 1\right) \times S\right) \times (1 + I)$$

Onde:
- $\bar{C}_{\text{hist}}$ = Média Mensal Histórica do custo (soma / nº períodos efetivos)
- $V_{\text{mês}}$ = Volume do mês sendo projetado
- $\bar{V}_{\text{hist}}$ = Volume Médio Histórico dos períodos selecionados
- $S$ = Sensibilidade (0.0 para Fixo, 1.0 para Variável, ou valor customizado por Type 06)
- $I$ = Inflação (`inflacao_percentual / 100`)

**Lógica de volume com fallbacks:**
1. Volume Real do mês (match por mês + ano)
2. Volume Budget (fallback via `_buscar_volume_bud_periodo()`)
3. Se nenhum encontrado: `proporcão_volume = 1.0` (sensibilidade neutra)

**Linhas com referência Budget:** quando o veículo está marcado como "ref Budget", a `média_histórica` é substituída pelo `custo BUD` daquele mês (em vez de calcular média).

#### Etapa 4 — Custos Específicos (Manuais)

1. O usuário adiciona custos via `st.data_editor` (tabela editável com colunas Jan-Dez)
2. Cada mês com valor > 0 gera uma linha em `custos_especificos.parquet`
3. No merge final, cada custo específico é **rateado por veículo**:
   - Usa `buscar_rateios_arquivo(oficina, periodo, ano)` para obter percentuais
   - Expande em N linhas para veículos: `CC21, CC22, CC24, CC24 5L, CC24 7L, J516`
   - `valor_rateado = total × rateio_veiculo`
   - Se rateio não encontrado → distribuição igual (`1/N` por veículo)
4. Marcadas com `Tipo = 'BE Manual'`

#### Etapa 5 — Classificação da Coluna Tipo

| Valor | Significado | Origem |
|-------|-------------|--------|
| `Histórico` | Dados já realizados (do Sapiens) | Períodos ≤ último realizado |
| `BE` | Linhas de forecast geradas pelo motor | Motor de cálculo (Etapa 3) |
| `BE Manual` | Custos específicos manuais | Custos adicionados pelo usuário (Etapa 4) |

#### Etapa 6 — Salvamento

| Arquivo | Conteúdo |
|---------|----------|
| `forecast_historico.parquet` | Linhas Tipo = 'Histórico' |
| `forecast_previsao.parquet` | Linhas Tipo ∈ {'BE', 'BE Manual'} |
| **`forecast_completo.parquet`** | Histórico + Forecast consolidado (arquivo consumido pelo dashboard) |
| `df_vol_historico.parquet` | Cópia do volume histórico completo |
| `custos_especificos.parquet` | Custos manuais adicionados |

---

### 14.6 Carregamento no Dashboard — `home_tc.py`

#### Como o forecast é carregado e normalizado

**Função:** `_load_forecast(ano)` — cacheada com TTL de 1h

1. Lê `dados/TC_Principal/Forecast/forecast_completo.parquet`
2. Normaliza coluna `Período` (capitaliza, corrige mojibake)
3. Converte colunas monetárias + Total/Volume/CPU para numérico
4. Filtra por `ano` se fornecido
5. **Normaliza coluna `Tipo`** via `_norm_tipo()`:
   - Se contém `"hist"` (case-insensitive, sem acentos) → `"Histórico"`
   - Senão → `"BE"` (inclui tanto 'BE' quanto 'BE Manual')

#### Toggle Real / BE (Simulado)

Na Tab 1 do dashboard:
```
📊 Fonte de Dados: [Real] [BE (Simulado)]
```
- **Real:** usa `df_principal.parquet` (dados do Sapiens processados)
- **BE (Simulado):** usa `forecast_completo.parquet` (dados de forecast)

#### Rateio BE por veículo no dashboard

Quando o usuário filtra por veículo no modo BE:
1. Carrega percentuais de rateio Real via `load_percentual_rateio_veiculos_real(ano)`
2. Chama `ratear_be_por_veiculo(df_be, df_percentual)`:
  - Remove colunas `Veículo`, `Percentual`, `Custo FP Veiculo`, `Custo Rateado` existentes
  - Merge em `[Oficina, Período]` → expande para N veículos
  - Calcula **Fase 13 (igual Real):** `Custo Rateado = FP sem Dedicada × Percentual`
  - **Correção (Forecast):** se `FP sem Dedicada = 0` e `Custo FP != 0`, usa `Custo FP` como base (marca como previsão)
  - Calcula **Fase 14 (igual Real):** D&A dedicado por veículo
    - Se existe arquivo Real de D&A por veículo, usa essa alocação
    - Caso contrário, rateia `D&A dedicado × Percentual`
    - Para linhas de **previsão**, D&A é zerado (já está embutido no `Custo FP`)
  - Final: `Custo FP Veiculo = Custo Rateado + D&A dedicado`

#### Flex Budget no dashboard

**Função:** `calcular_flex_budget(df_principal, df_vol_bud, df_vol_actual)` (`shared.py`)

1. Identifica custos fixos via `mask_custo_fixo()`: normaliza texto sem acento, verifica se começa com `'fix'`
2. Separa BUD em Fixo e NãoFixo
3. Agrega volumes por Período
4. **Fórmula:**

$$\text{Flex Bud} = \text{Fixo}_{\text{BUD}} + \text{NãoFixo}_{\text{BUD}} \times \frac{\text{Vol Actual}}{\text{Vol Budget}}$$

Se Vol Budget = 0: proporção = 1.0 (Flex Bud = BUD).

---

### 14.7 Fórmulas de Referência

| Grandeza | Fórmula | Onde |
|----------|---------|------|
| **Tempo FA** | $\text{Vol FA} \times \text{EST}$ | Fase 2 |
| **Tempo Veic** | $\text{Volume} \times \text{EST}$ | Fase 4 |
| **Rateio FA (auto)** | $\frac{\text{Tempo FA}}{\text{Tempo FA} + \text{Tempo Veic}}$ | Fase 5 |
| **Taxa PdR** | $\frac{\sum \text{Tempo FA global}}{\sum \text{Tempo Veic global (excl. GS, SM)}}$ | Fase 5 |
| **Rateio FA (manual)** | $\text{fator\_manual} \times \text{Taxa PdR}$ | Fase 5 |
| **Custo FA** | $\text{Rateio FA} \times \text{Despesa Primaria}$ (Redis: FA = 0) | Fase 6 |
| **Custo FP** | $\text{Despesa Primaria} - \text{Custo FA}$ | Fase 7 |
| **D&A pro-rata** | $\text{D\&A}_{\text{grupo}} \times \frac{\text{FP}_{\text{linha}}}{\sum \text{FP}_{\text{grupo}}}$ | Fase 9 |
| **FP sem Dedicada** | $\text{Custo FP} - \text{D\&A dedicado}$ | Fase 9 |
| **% Rateio Veíc** | $\frac{\text{Tempo Veic}_{\text{veíc}}}{\sum \text{Tempo Veic}_{\text{oficina}}}$ | Fase 12 |
| **Custo Rateado** | $\text{FP sem Dedicada} \times \text{Percentual}$ | Fase 13 |
| **Custo FP Veículo** | $\text{Custo Rateado} + \text{D\&A dedicado}$ | Fase 14 |
| **CPU** | $\frac{\sum \text{Custo FP Veiculo}}{\text{Volume}}$ | Fase 15 |
| **Flex Bud** | $\text{Fixo} + \text{NãoFixo} \times \frac{V_{\text{Actual}}}{V_{\text{Budget}}}$ | Dashboard |
| **Forecast** | $\bar{C} \times (1 + (\frac{V}{V_m} - 1) \times S) \times (1 + I)$ | Simulador |
| **Prova cruzada** | $\text{DP} - \text{FA} - \text{FP} = 0$ | Validação |

---

### 14.8 Mapa Completo de Parquets Gerados

#### TC Veículos — Real (`dados/TC_Principal/{ano}/`)

| Arquivo | Fase | Conteúdo |
|---------|------|----------|
| `df_principal.parquet` | 10 | DP, FA, FP, D&A, FP sem Ded, Rateio FA |
| `df_volume_fa.parquet` | 10 | Vol FA, EST, Tempo FA |
| `df_tempo_veiculos.parquet` | 10 | Volume × EST por veículo |
| `df_vol_veiculos.parquet` | 10 | Volumes de veículos (Actual) |
| `df_dea_dedicado.parquet` | 10 | D&A Dedicado (copiado do BUD) |
| `df_veiculos_fp_sem_da.parquet` | 16 | Base para rateio veicular |
| `df_veiculos_percentual_rateio.parquet` | 16 | % rateio por veículo |
| `df_veiculos_custo_rateado.parquet` | 16 | FP sem Ded × Percentual |
| `df_veiculos_custo_fp.parquet` | 16 | Custo FP final por veículo |
| `df_veiculos_cpu.parquet` | 16 | CPU por veículo |
| `df_comparativo_real_budget.parquet` | 17 | Real × Budget (diferenças) |

#### TC Veículos — Budget (`dados/TC_Principal/{ano}/BUD/`)

| Arquivo | Conteúdo |
|---------|----------|
| `df_principal_BUD.parquet` | DP, FA, FP, D&A, FP sem Ded |
| `df_volume_fa_BUD.parquet` | Vol FA + Tempo FA |
| `df_tempo_veiculos_BUD.parquet` | Tempo veículos |
| `df_vol_veiculos_BUD.parquet` | Volumes veículos (BDG) |
| `df_vol_veiculos_actual.parquet` | Volumes veículos (Actual) |
| `df_dea_dedicado_BUD.parquet` | D&A Dedicado |
| `df_veiculos_fp_sem_da_BUD.parquet` | FP sem D&A |
| `df_veiculos_percentual_rateio_BUD.parquet` | % rateio por veículo |
| `df_veiculos_custo_rateado_BUD.parquet` | Custo rateado |
| `df_veiculos_custo_fp_BUD.parquet` | Custo FP por veículo |
| `df_veiculos_cpu_BUD.parquet` | CPU por veículo |

#### TC Veículos — Forecast (`dados/TC_Principal/Forecast/`)

| Arquivo | Conteúdo |
|---------|----------|
| `forecast_completo.parquet` | Histórico + BE + BE Manual |
| `forecast_historico.parquet` | Apenas Tipo = 'Histórico' |
| `forecast_previsao.parquet` | Apenas Tipo ∈ {'BE', 'BE Manual'} |
| `df_vol_historico.parquet` | Volume histórico completo |
| `custos_especificos.parquet` | Custos manuais |

#### TC Veículos — Histórico Consolidado (`dados/TC_Principal/historico_consolidado/`)

| Arquivo | Tipo |
|---------|------|
| `df_principal_historico.parquet` | Real |
| `df_vol_historico.parquet` | Real |
| `df_cpu_historico.parquet` | Real |
| `df_veiculos_custo_fp_historico.parquet` | Real |
| `BUD/df_principal_historico_BUD.parquet` | Budget |
| `BUD/df_vol_historico_BUD.parquet` | Budget |
| `BUD/df_cpu_historico_BUD.parquet` | Budget |
| `BUD/df_veiculos_custo_fp_historico_BUD.parquet` | Budget |

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

**Última atualização:** 2026-02-16
