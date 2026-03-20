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

### Utilitario criado

Arquivo local:

- `tools/databricks/pull_databricks_app.py`

Fluxo:

1. pull remoto do app no Databricks
2. atualizacao de `TC-Cloud/sci_app`
3. propagacao para raiz do repo e espelhos locais
4. validacao final dos destinos

Objetivo:

- impedir divergencia silenciosa entre as copias locais
- preservar rapidamente o que esta funcionando no cloud

---

## 11) Correcao especifica da page de Documentacao

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

## 12) Item novo no indice da page de Documentacao

Novo item funcional na sidebar:

- `☁️ Índice TC Cloud`

Esse item deve ser usado como referencia interna para:

- arquitetura do ambiente cloud
- tecnologias em uso
- fluxo de processamento validado
- regras de sincronizacao local e remota
- checklist anti-regressao

---

## 13) Slide novo na Apresentacao Visual

Foi incluido um slide especifico para o ambiente cloud / Databricks, seguindo o mesmo layout, cabecalho, duracao, destaque e apoio visual dos demais slides.

Mensagem central desse slide:

- o TC Veiculos esta estavel no Databricks porque app, pipeline e dados foram separados
- o backend atual e Workspace Files
- a sincronizacao local agora tem fluxo controlado para nao sobrescrever o que esta funcionando no cloud

---

## 14) Checklist anti-regressao

Antes de publicar novas alteracoes locais, validar:

1. o app continua lendo `SCI_SHARED_DATA_ROOT` corretamente
2. `app.py` ainda configura o ambiente antes de importar paginas
3. os notebooks 00, 01, 03 e 05 continuam coerentes com o backend atual
4. o modulo TC Veiculos continua lendo os mesmos parquets esperados
5. a page de Documentacao encontra seus arquivos markdown dentro do app
6. o fluxo de sync local nao remove artefatos necessarios do app
7. uploads do Workspace continuam usando `workspace.upload()` com remocao previa quando necessario

---

## 15) Resumo executivo final

Hoje, o estado considerado estavel do ambiente cloud e:

- TC Veiculos funcionando no Databricks
- dados lidos de Workspace Files
- pipeline e app separados
- notebooks e jobs organizados para o backend atual
- sincronizacao local preparada para preservar o que esta funcional no cloud
- documentacao tecnica do ambiente cloud integrada ao proprio app