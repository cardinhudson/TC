# Documentacao Tecnica Completa — TC Cloud / Databricks

> Fonte de verdade operacional para o ambiente Databricks que hoje executa o SCI com o modulo TC Veiculos funcionando no cloud.

---

## 1) Objetivo

Este documento consolida tudo o que foi ajustado no ambiente Databricks para:

- manter o SCI rodando 100% no cloud
- preservar o funcionamento do modulo TC Veiculos
- evitar regressao quando houver mudancas locais no repositorio
- deixar claro o fluxo entre app, pipeline, dados, notebooks, jobs e sincronizacao

---

## 2) Arquitetura validada no Databricks

Separacao de responsabilidade no Workspace:

```text
/Workspace/Users/u235107@inetpsa.com/Drafts/sci
├── dados/
├── notebooks/
├── jobs/
├── src/sci_core/
└── workspace_publish/

/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app/sci_app
├── app.py
├── app.yaml
├── pages/
├── tc_core/
├── tc_principal/
├── tc_ext/
├── tc_copilot/
└── alertas/
```

Regra operacional:

- `sci` = pipeline, notebooks, jobs, dados e outputs publicados
- `sci_app/sci_app` = apenas o codigo do Databricks App

Essa separacao foi importante para evitar mistura entre dados pesados, pipeline e interface do app.

---

## 3) Backend atual em producao

Estado atual validado:

- `SCI_DATA_BACKEND=databricks`
- leitura de Excel e Parquet em Workspace Files
- publicacao em `workspace_publish/`
- notebooks e jobs no Workspace
- sem dependencia de Snowflake no fluxo atual
- sem dependencia operacional de Secret Scope para a leitura de dados do app

Tecnologias principais em uso no cloud:

- Databricks Apps
- Workspace Files
- Streamlit
- Python
- pandas
- openpyxl
- pyarrow
- databricks-sdk
- Jobs Databricks
- notebooks Python no Workspace

---

## 4) Caminhos principais

### Pipeline / dados

- `REPO_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci`
- `DATA_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados`
- `PUBLISH_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci/workspace_publish`

### App

- `APP_SOURCE=/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app/sci_app`

### Espelhos locais usados no desenvolvimento

- repositorio atual: `C:/user/U235107/GitHub/TC`
- espelho 1: `C:/user/U235107/GitHub/TC/Databricks/sci_app`
- espelho 2: `C:/user/U235107/GitHub/TC/Databricks/sci`
- copia oficial de publicacao local: `C:/user/U235107/GitSTLA/TC-Cloud/sci_app`

---

## 5) Correcao estrutural do startup do app

O app passou a configurar a raiz de dados compartilhada antes dos imports das paginas.

### O que foi ajustado

1. `app.py` configura `SCI_SHARED_DATA_ROOT` no startup.
2. O app tenta primeiro acesso direto ao path configurado.
3. Se isso falhar, tenta Volumes / DBFS.
4. Se ainda falhar, tenta espelhar o Workspace para `/tmp/sci_data_cache`.
5. So depois disso as paginas sao importadas.

### Motivo

Sem isso, paginas como a Home TC podiam iniciar com caminho de dados errado, congelar paths no import ou abrir apenas o cabecalho sem carregar os anos disponiveis.

---

## 6) Ajustes de resolucao de caminho

Correcoes relevantes para evitar regressao:

1. `get_data_root()` e `get_base_path()` centralizados em `tc_core/utils/portabilidade.py`
2. remocao de dependencia de paths rigidos como `dados/` em paginas do app
3. paths internos do modulo de dados passaram a ser resolvidos dinamicamente
4. paginas legadas foram renomeadas com underscore para evitar inconsistencias em Workspace Files:
   - `1_Waterfall.py`
   - `2_Best_Estimate.py`
   - `5_Extracao_Dados.py`
   - `6_Documentacao.py`

---

## 7) Fluxo que deixa o TC Veiculos funcionando no cloud

### Entrada

- Excel em `dados/TC_Principal/{ANO}/`
- Budget e Real lidos pelos scripts de processamento do proprio projeto

### Processamento

- `processamento_dados_veiculos_BUD.py`
- `processamento_dados_veiculos.py`

### Saida principal

- `df_principal_BUD.parquet`
- `df_principal.parquet`
- `df_vol_veiculos_BUD.parquet`
- `df_vol_veiculos.parquet`
- `df_veiculos_custo_fp_BUD.parquet`
- `df_veiculos_custo_fp.parquet`

### Consumo pelo app

O modulo TC Veiculos do app le exatamente esses parquets e usa a mesma regra de negocio do ambiente local.

---

## 8) Notebooks e jobs validados

Notebooks principais do fluxo Databricks:

1. `notebooks/00_validar_ambiente_databricks.py`
2. `notebooks/01_criar_tabelas_delta.py`
3. `notebooks/03_processar_e_publicar_delta.py`
4. `notebooks/05_validacao_pos_job.py`
5. `notebooks/06_ui_consulta_workspace.py`

Status do notebook 02:

- `notebooks/02_carga_snowflake.py` permanece apenas como esqueleto futuro
- nao faz parte do fluxo atual que esta funcionando

Workflow esperado:

1. validar ambiente
2. preparar estrutura de publicacao
3. processar Real e Budget
4. validar saude da publicacao
5. consultar outputs quando necessario

---

## 9) Publicacao do app no Workspace

### Problema identificado

Tentativas anteriores de upload falhavam com:

- limite operacional em caminhos antigos de importacao
- problemas de path com `@` no usuario do Workspace
- overwrite pouco confiavel em arquivos ja existentes

### Solucao aplicada

1. usar `workspace.upload()` via SDK
2. antes de subir um arquivo que ja existe, remover o antigo
3. evitar os fluxos antigos baseados em `workspace.import_()` e rotas REST anteriores

Regra para nao regredir:

- manter a publicacao do app via upload SDK controlado
- evitar voltar para mecanismos antigos de importacao

---

## 10) Sincronizacao local sem regressao

### Fonte de verdade operacional

Quando o app no Databricks estiver funcionando melhor que a copia local, a fonte de verdade passa a ser o Workspace remoto do Databricks App.

Fluxo:

1. exportar o workspace remoto do app com `databricks workspace export-dir`
2. salvar o espelho em `Databricks/pulled_from_workspace`
3. propagar para a raiz do repo e espelhos locais sem apagar artefatos extras locais
4. validar que os arquivos puxados e os arquivos locais publicados estao identicos

Objetivo:

- impedir divergencia silenciosa entre as copias locais
- preservar rapidamente o que esta funcionando no cloud

---

## 11) Contrato dos parquets otimizados no cloud

Quando `SCI_USE_OPTIMIZED_PARQUETS=true`, o Databricks App pode ler variantes `agg` ou `thin` em vez dos parquets full.

Regra critica:
- se o schema AGG estiver incompleto, o cloud quebra mesmo quando o local parece normal.

Contratos obrigatorios hoje:

`df_veiculos_agg_home` e `df_veiculos_agg_home_BUD`
- chaves: `Ano`, `Período`, `Oficina`, `Veículo`, `Type 05`, `Type 06`, `Account`, `Custo`

`forecast_agg`
- chaves: `Ano`, `Período`, `Oficina`, `Tipo`, `Type 05`, `Type 06`

`df_final_agg` e `df_final_agg_BUD`
- chaves: `Ano`, `Período`, `Oficina`, `Veículo`, `Type 05`, `Type 06`, `Account`, `Custo`

Se qualquer uma dessas colunas sair do AGG, os sintomas mais provaveis sao:
- waterfall com `Type 06 nao encontrada`;
- tooltip do Best Estimate jogando tudo em `Outros`;
- divergencia entre local e Databricks.

---

## 12) Protecao anti-regressao no app

O `tc_core/data_router.py` passou a validar cada AGG contra o schema central em `tc_core/parquet_schemas.py`.

Comportamento esperado:
- AGG valido: leitura otimizada normal;
- AGG desatualizado: warning no log e fallback automatico para o parquet full.

Isso reduz risco operacional, mas nao substitui a reprocessacao correta dos dados.

---

## 13) Incidentes corrigidos e licao operacional

Incidentes recentes corrigidos:

1. Waterfall de veiculos quebrando no cloud por falta de `Type 06` nos AGG veiculares.
2. Tooltip do Best Estimate perdendo quebra por `Type 05/Type 06` porque `forecast_agg` estava subagregado demais.
3. AGG de TC Ext gerado sem `Type 05`, `Type 06`, `Account` e `Custo`, abrindo risco de regressao futura.
4. Rotulo delta com baixa legibilidade, corrigido para preto nas paginas relevantes.
5. Workspace remoto ficando mais atualizado que o local, exigindo sincronizacao reversa Databricks -> local.

Licao operacional:
- quando o remoto estiver mais correto, ele passa a ser a fonte de verdade operacional ate o repositorio local ser alinhado.

---

## 14) Correcao especifica da page de Documentacao

Problema observado no app:

```text
Arquivo nao encontrado: /app/python/source_code/DOCUMENTACAO_SISTEMA_TC.md
```

### Causa

Os arquivos markdown tecnicos estavam na raiz do repositorio local, mas nao eram incluidos na arvore publicada do app no Databricks.

### Correcao adotada

Os arquivos abaixo passam a ser considerados artefatos publicados do app:

- `DOCUMENTACAO_SISTEMA_TC.md`
- `DOCUMENTACAO_TC_PRINCIPAL.md`
- `DOCUMENTACAO_TC_CLOUD.md`
- `README_DATABRICKS.md`
- `README_cloud.md`
- `GUIA_DATABRICKS_100_NUVEM.md`
- `APRESENTACAO_5_MINUTOS_VISUAL.md`

Resultado esperado:

- a page de Documentacao encontra os markdowns dentro da raiz do app
- o item novo de TC Cloud fica disponivel no proprio ambiente do Databricks

---

## 15) Item novo no indice da page de Documentacao

Novo item funcional na sidebar:

- `☁️ Índice TC Cloud`

Esse item deve ser usado como referencia interna para:

- arquitetura do ambiente cloud
- tecnologias em uso
- fluxo de processamento validado
- regras de sincronizacao local e remota
- checklist anti-regressao

---

## 16) Slide novo na Apresentacao Visual

Foi incluido um slide especifico para o ambiente cloud / Databricks, seguindo o mesmo layout, cabecalho, duracao, destaque e apoio visual dos demais slides.

Mensagem central desse slide:

- o TC Veiculos esta estavel no Databricks porque app, pipeline e dados foram separados
- o backend atual e Workspace Files
- a sincronizacao local agora tem fluxo controlado para nao sobrescrever o que esta funcionando no cloud

---

## 17) Checklist anti-regressao

Antes de publicar novas alteracoes locais, validar:

1. o app continua lendo `SCI_SHARED_DATA_ROOT` corretamente
2. `app.py` ainda configura o ambiente antes de importar paginas
3. os notebooks 00, 01, 03 e 05 continuam coerentes com o backend atual
4. os AGG mantem `Type 05`, `Type 06`, `Account` e `Custo` quando o consumidor depende dessas dimensoes
5. `forecast_agg` mantem `Type 05` e `Type 06` para o tooltip do Best Estimate
6. o `data_router.py` continua com fallback para FULL quando encontrar AGG desatualizado
7. a page de Documentacao encontra seus arquivos markdown dentro do app
8. o fluxo de pull Databricks -> local continua disponivel para recuperar a fonte de verdade operacional
9. uploads do Workspace continuam usando `workspace.upload()` com remocao previa quando necessario

---

## 18) Resumo executivo final

Hoje, o estado considerado estavel do ambiente cloud e:

- TC Veiculos funcionando no Databricks
- dados lidos de Workspace Files
- pipeline e app separados
- notebooks e jobs organizados para o backend atual
- sincronizacao local preparada para preservar o que esta funcional no cloud
- documentacao tecnica do ambiente cloud integrada ao proprio app