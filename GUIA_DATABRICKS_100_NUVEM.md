# SCI — Guia Databricks 100% Agora

## Objetivo

Rodar o SCI agora 100% no Databricks, sem Snowflake, sem Secret Scope, sem DBFS e sem Volumes UC.

## Backend atual

- SCI_DATA_BACKEND=databricks
- entrada em /Workspace/Users/.../Drafts/sci/dados
- publicação em /Workspace/Users/.../Drafts/sci/workspace_publish
- Excel lido com pandas/openpyxl
- datasets publicados em Parquet path-based

## Backend futuro

- SCI_DATA_BACKEND=snowflake
- mesmo fluxo de transformação
- notebook 02 faz apenas a carga a partir do Delta
- dependente de ICT

## Arquivos que você deve usar agora

- [README_cloud.md](README_cloud.md)
- [notebooks/00_validar_ambiente_databricks.py](notebooks/00_validar_ambiente_databricks.py)
- [notebooks/01_criar_tabelas_delta.py](notebooks/01_criar_tabelas_delta.py)
- [notebooks/03_processar_e_publicar_delta.py](notebooks/03_processar_e_publicar_delta.py)
- [notebooks/02_carga_snowflake.py](notebooks/02_carga_snowflake.py)

## Defaults

```text
REPO_ROOT = /Workspace/Users/u235107@inetpsa.com/Drafts/sci
DATA_ROOT =
PUBLISH_ROOT =
ANO = 2026
RUN_BUDGET = true
RUN_REAL = true
```

Se DATA_ROOT ficar vazio, o notebook usa:

1. /Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados

Se PUBLISH_ROOT ficar vazio, o notebook usa:

1. /Workspace/Users/u235107@inetpsa.com/Drafts/sci/workspace_publish

## Ordem obrigatória

1. Rodar [notebooks/00_validar_ambiente_databricks.py](notebooks/00_validar_ambiente_databricks.py)
2. Rodar [notebooks/01_criar_tabelas_delta.py](notebooks/01_criar_tabelas_delta.py)
3. Rodar [notebooks/03_processar_e_publicar_delta.py](notebooks/03_processar_e_publicar_delta.py)
4. Não rodar [notebooks/02_carga_snowflake.py](notebooks/02_carga_snowflake.py) agora

## Aceite esperado

### Notebook 00

- REPO_ROOT válido
- Excel localizado
- imports OK
- log final com EXCEL_OK=True

### Notebook 01

- PUBLISH_ROOT criado
- pastas de publicação listadas

### Notebook 03

- abas do Excel lidas
- datasets Parquet gravados
- resumo final com linhas por tabela

## Se falhar

Enviar:

1. notebook executado
2. parâmetros usados
3. traceback completo
4. última saída antes do erro

## Job diário

Template disponível em [jobs/job_diario.json](jobs/job_diario.json).

## Observação importante

O guia operacional detalhado agora é [README_cloud.md](README_cloud.md). Este arquivo existe como atalho curto para a execução imediata.