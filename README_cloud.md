# SCI — Operação Databricks Agora

## Plano curto

- Backend atual: `SCI_DATA_BACKEND=databricks`
- Fluxo: Excel em Workspace Files -> processamento BUD/REAL -> Parquet em `workspace_publish/` -> Job diário -> validação pós-job -> UI simples de leitura
- Backend futuro: `SCI_DATA_BACKEND=snowflake`, reaproveitando as mesmas transformações; o notebook 02 entra como etapa adicional de carga

## Defaults

- `REPO_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci`
- `DATA_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados`
- `PUBLISH_ROOT=/Workspace/Users/u235107@inetpsa.com/Drafts/sci/workspace_publish`
- `ANO=2026`

## Artefatos principais

- `src/sci_core/backend.py`
- `src/sci_core/io_excel.py`
- `src/sci_core/io_delta.py`
- `src/sci_core/transform_principal.py`
- `notebooks/00_validar_ambiente_databricks.py`
- `notebooks/01_criar_tabelas_delta.py`
- `notebooks/03_processar_e_publicar_delta.py`
- `notebooks/05_validacao_pos_job.py`
- `notebooks/06_ui_consulta_workspace.py`
- `jobs/job_diario.json`

## Ordem de execução

1. Rodar `notebooks/00_validar_ambiente_databricks.py`
2. Rodar `notebooks/01_criar_tabelas_delta.py`
3. Rodar `notebooks/03_processar_e_publicar_delta.py`
4. Rodar `notebooks/05_validacao_pos_job.py`
5. Usar `notebooks/06_ui_consulta_workspace.py` como UI simples

## Critérios de aceite

- `00`: `EXCEL_OK=true` e `IMPORTS_OK=true`
- `03`: `RESUMO FINAL` com contagens `>0` e caminho de saída em `workspace_publish`
- `05`: `CHECK SAÚDE: OK`
- `Job`: tasks `pipeline_workspace` e `pos_validacao` com `Succeeded`
- `UI`: leitura básica dos datasets e últimas execuções

## Troubleshooting curto

- `Missing optional dependency 'openpyxl'`: rodar `%pip install openpyxl pandas pyarrow` e `dbutils.library.restartPython()`
- `ModuleNotFoundError: sci_core`: garantir `REPO_ROOT/src` no `sys.path` ou usar a versão autocontida do notebook no Workspace
- `Falha ao gravar Delta`: manter o backend operacional em `workspace_publish`, que foi o caminho validado no ambiente atual

## Virada futura para Snowflake

- Alterar `SCI_DATA_BACKEND` para `snowflake`
- Habilitar Secret Scope + key-pair
- Ativar `notebooks/02_carga_snowflake.py` após o notebook 03
- Demais notebooks e fluxo permanecem iguais