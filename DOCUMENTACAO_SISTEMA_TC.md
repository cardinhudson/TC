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
- `app.py`: **Portal TC** (menu/roteamento) que agrupa:
  - TC Ext (Linhas Secundárias)
  - TC (Planta Principal)
  - Documentação (única, global)
- `tc_ext/pages/home_ext.py`: **Home do TC Ext** (código que antes estava no `app.py`).
- `pages/1 - Waterfall.py`: análise Waterfall.
- `pages/2 - Best Estimate - Simulador.py`: simulador.
- `pages/3 - Best Estimate - Análise.py`: análise BE.
- `pages/4 - Waterfall_Analysis.py`: variações/derivações de waterfall.
- `pages/5 - Extração de Dados.py`: guia/rotinas para extração.
- `pages/6 - Documentacao.py`: documentação dentro do Streamlit.

### Módulo TC (Planta Principal)
- `tc_principal/pages/*.py`: páginas **stub** espelhando a estrutura do TC Ext (sem lógica ainda).

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
- Notebooks: `dados.ipynb`, `dados_BUD.ipynb` (fontes originais de lógica).

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

Regra crítica: **o perímetro de filtros aplicado ao custo deve ser replicado no volume** (para CPU e para Flex Bud).

### 5.3 Modos de visualização
- **Custo Total**: trabalha com `Total` (ou `Valor` quando necessário).
- **CPU (Custo por Unidade)**: sempre deriva de `Total / Volume` e deve ser calculado após agregações.

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

Nunca calcular CPU linha a linha e depois tirar média/soma.

Tratamento de zeros:
- se `Volume == 0` ou nulo → CPU = 0 (ou NaN dependendo do contexto de exibição; o sistema usa 0 em várias tabelas).

---

## 8) Flex Bud (Budget Flexível) — especificação completa

### 8.1 Conceito
Flex Bud ajusta o budget ao volume real, distinguindo:
- **Custo Fixo**: não varia com volume
- **Custo Variável**: varia proporcionalmente ao volume

### 8.2 Fórmulas base
Para um período/dimensão:
- Flex Fixo: $$Flex_{fixo} = BUD_{fixo}$$
- Flex Variável: $$Flex_{var} = BUD_{var} \times \frac{Volume_{real}}{Volume_{bud}}$$
- Flex Total: $$Flex_{total} = Flex_{fixo} + Flex_{var}$$

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

### 9.3 Gráfico por Oficina
- Agrega por `Oficina`.
- Pode ter linha/indicador de Flex Bud agregado por oficina.

### 9.4 Gráfico por Veículo
- Agrega por `Veículo`.

### 9.5 Tabelas detalhadas
- Tabelas pivot por Oficina/Veículo/Período.
- Para CPU, colunas totais devem ser recalculadas a partir de Total e Volume agregados.

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

**Última atualização:** 2026-01-20
