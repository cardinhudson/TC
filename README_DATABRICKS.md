# SCI — Guia Operacional Databricks

> **Objetivo**: colocar o sistema SCI (Stellantis Cost Intelligence) em
> funcionamento 100% no Databricks usando apenas Workspace Files.
> Sem DBFS, sem Volumes, sem Secrets, sem Snowflake, sem Delta obrigatório.

> **v2.29** — Inclui: Data Router (THIN/AGG/FULL), Telemetria, Debug Panel,
> forecast_agg.parquet, Waterfall dimension fix, Home forecast bypass fix.

---

## Arquitetura atual (Fase 1)

Separacao padrao no Workspace:

- /Workspace/Users/u235107@inetpsa.com/Drafts/sci = pipeline, notebooks, jobs, dados e workspace_publish
- /Workspace/Users/u235107@inetpsa.com/Drafts/sci_app = codigo do Databricks App

```
/Workspace/Users/u235107@inetpsa.com/Drafts/sci/      ← REPO_ROOT
├── src/sci_core/        ← módulos de negócio (não alterar nos notebooks)
│   ├── __init__.py
│   ├── backend.py       ← resolve caminhos + flag de backend
│   ├── io_excel.py      ← leitura segura do Excel principal
│   ├── io_delta.py      ← to_spark_safe + write_delta (fallback Parquet)
│   ├── io_workspace.py  ← read/write Parquet em Workspace Files
│   └── transform_principal.py  ← processar_budget / processar_real / build_comparativo
├── notebooks/
│   ├── 00_validar_ambiente_databricks.py   ← smoke-test do ambiente
│   ├── 01_criar_tabelas_delta.py           ← cria estrutura de diretórios
│   ├── 02_carga_snowflake.py               ← PENDENTE ICT (skeleton)
│   ├── 03_processar_e_publicar_delta.py    ← pipeline principal ← JOB task 1
│   ├── 05_validacao_pos_job.py             ← saúde pós-job      ← JOB task 2
│   └── 06_ui_consulta_workspace.py         ← UI de consulta ad hoc
├── jobs/
│   └── job_diario.json  ← definição do Workflow (importar na UI)
├── dados/
│   └── TC_Principal/
│       └── {ANO}/
│           └── Reporting veículos.xlsx     ← input obrigatório
└── workspace_publish/   ← output gerado automaticamente
    ├── tc_principal_bud/
    ├── tc_principal_real/
    ├── tc_comparativo/
    └── tc_execucao_log/
```

```text
/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app/  ← APP_SOURCE
├── app.py
├── app.yaml
├── requirements.txt
├── pages/
├── tc_principal/
├── tc_ext/
├── tc_core/
├── tc_copilot/
└── alertas/
```

### Arquitetura futura (Fase 2 — PENDENTE ICT)

```
Workspace Files (local) → Snowflake SCI_CURATED (carga via 02_carga_snowflake.py)
SCI_DATA_BACKEND=snowflake → leitura direta via Spark-Snowflake connector
```

---

## Pré-requisitos

### 1. Cluster Databricks

| Parâmetro         | Valor mínimo recomendado                    |
|-------------------|---------------------------------------------|
| Databricks Runtime | 13.3 LTS ou superior                       |
| Python            | 3.10+                                       |
| Tipo              | All-Purpose ou Job compute                  |
| Libraries         | `openpyxl>=3.1`, `pyarrow>=12`, `pandas>=2` |

Verifique se as libs estão instaladas no cluster ou adicione via **Cluster > Libraries > Install > PyPI**.

### 2. Arquivo Excel no Workspace

Suba o arquivo para o Databricks:

```
Databricks UI → Workspace → Users → u235107@inetpsa.com → Drafts/sci/dados
  → Create folder TC_Principal → Create folder {ANO}
  → Upload "Reporting veículos.xlsx"
```

Caminho final esperado:
```
/Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados/TC_Principal/2026/Reporting veículos.xlsx
```

> **Alternativa com acento**: o notebook 03 testa também `Reporting veiculos.xlsx`
> (sem cedilha). Suba com qualquer um dos nomes.

### 3. Repositório no Workspace

Clone ou sincronize o repositório Git:

```
Databricks UI → Repos → Add Repo
  URL: <seu repo Git>
  Path: /Workspace/Users/u235107@inetpsa.com/Drafts/sci
```

Ou faça upload manual da pasta `src/` e `notebooks/` para o mesmo caminho.

### 4. Codigo do App no Workspace

Para o Databricks App, use uma pasta separada do pipeline:

```text
/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app
```

Suba para essa pasta o conteudo de [Databricks/sci_app](Databricks/sci_app).
Nao aponte o Databricks App para a pasta sci quando a intencao for manter dados e publicacao separados do codigo da interface.

---

## Passo a passo (execução inicial)

Execute os notebooks **na ordem abaixo** em um cluster ativo.

---

## Utilitario local de pull e sincronizacao

Para trazer de volta o conteudo do app que esta funcionando no Databricks e
replicar nas copias locais, use o utilitario versionado em:

`tools/databricks/pull_databricks_app.py`

Fluxo padrao do utilitario:

1. Faz pull de `/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app/sci_app`
  para `C:\user\U235107\GitSTLA\TC-Cloud\sci_app`
2. Propaga os itens publicados para:
  - raiz do repo atual
  - `Databricks/sci_app`
  - `Databricks/sci`
3. Valida se os destinos ficaram completos

Exemplos:

```bash
python tools/databricks/pull_databricks_app.py
python tools/databricks/pull_databricks_app.py --skip-download
python tools/databricks/pull_databricks_app.py --validate-only
```

Requisitos locais:

- `.env` na raiz com `DATABRICKS_HOST` e `DATABRICKS_TOKEN`
- `databricks-sdk` e `python-dotenv` instalados no ambiente Python

Perfis multiplos do Databricks CLI:

- E possivel manter mais de um usuario no arquivo `.databrickscfg`, por exemplo `[antigo]` e `[atual]`.
- O script `scripts/sync_databricks_app.ps1` aceita `-Profile <nome>`.
- O script `scripts/sync_databricks_app.ps1` agora tambem aceita `-DeployOnly`, `-SkipDeploy` e `-AppName <nome>`.
- Os wrappers `sync_app.bat` e `sync_and_run.bat` tambem aceitam perfil por argumento ou pela variavel `SCI_DATABRICKS_PROFILE`.
- Exemplos: `sync_app.bat watch atual`, `sync_app.bat deploy atual`, `sync_app.bat nodeploy atual` ou `sync_and_run.bat atual`.

Observacao: o utilitario fica fora de `scripts/` e fora das pastas espelhadas
do app para nao ser removido no proximo pull exato.

### Passo 1 — Validar ambiente

**Notebook**: `00_validar_ambiente_databricks.py`

Widgets:
| Widget     | Valor padrão                                          | Descrição           |
|------------|-------------------------------------------------------|---------------------|
| REPO_ROOT  | `/Workspace/Users/u235107@inetpsa.com/Drafts/sci`    | Não alterar         |
| DATA_ROOT  | *(vazio)*                                             | Usa padrão          |
| ANO        | `2026`                                                | Ano a processar     |

**Critério de sucesso**: última linha contém
```
[INFO] LOG FINAL | EXCEL_OK=True | BACKEND=databricks | DATA_ROOT=...
```

---

### Passo 2 — Criar estrutura de publicação

**Notebook**: `01_criar_tabelas_delta.py`

Widgets:
| Widget       | Valor padrão                                       |
|--------------|----------------------------------------------------|
| REPO_ROOT    | `/Workspace/Users/u235107@inetpsa.com/Drafts/sci` |
| PUBLISH_ROOT | *(vazio — usa `workspace_publish` automático)*     |

**Critério de sucesso**: uma linha por tabela:
```
[INFO] Estrutura pronta: tc_principal_bud -> .../workspace_publish/tc_principal_bud
[INFO] Estrutura pronta: tc_principal_real -> ...
[INFO] Estrutura pronta: tc_comparativo -> ...
[INFO] Estrutura pronta: tc_execucao_log -> ...
[INFO] PUBLISH_ROOT pronto: .../workspace_publish
```

---

### Passo 3 — Executar pipeline principal

**Notebook**: `03_processar_e_publicar_delta.py`

Widgets:
| Widget       | Valor padrão | Descrição                     |
|--------------|-------------|-------------------------------|
| REPO_ROOT    | default     | Não alterar                    |
| DATA_ROOT    | *(vazio)*   | Usa padrão                    |
| PUBLISH_ROOT | *(vazio)*   | Usa padrão                    |
| ANO          | `2026`      | Ano                           |
| RUN_BUDGET   | `true`      | Processar Budget              |
| RUN_REAL     | `true`      | Processar Real                |

**Critério de sucesso**: tabela RESUMO FINAL com `Status=OK` para todas as saídas.

```
[INFO] RESUMO FINAL
+----+------------------+-------+------+----------------------------------+
|Ano |Tabela            |Linhas |Status|Mensagem                          |
+----+------------------+-------+------+----------------------------------+
|2026|tc_comparativo    |2      |OK    |.../workspace_publish/tc_compar...|
|2026|tc_principal_bud  |N      |OK    |.../workspace_publish/tc_princi...|
|2026|tc_principal_real |N      |OK    |.../workspace_publish/tc_princi...|
+----+------------------+-------+------+----------------------------------+
```

---

### Passo 4 — Validação pós-pipeline

**Notebook**: `05_validacao_pos_job.py`

**Critério de sucesso**: última linha:
```
[INFO] CHECK SAÚDE: OK
```

Se algum dataset mostrar `Status=ERRO`, significa que o parquet está ausente ou
vazio para o ano configurado. Execute o passo 3 novamente.

---

### Passo 5 — Consulta ad hoc (opcional)

**Notebook**: `06_ui_consulta_workspace.py`

Widgets:
| Widget       | Opções disponíveis                                                            |
|--------------|--------------------------------------------------------------------------------|
| DATASET      | `tc_principal_bud`, `tc_principal_real`, `tc_comparativo`, `tc_execucao_log` |
| LIMIT        | número de linhas a exibir                                                     |

O resultado aparece como tabela interativa quando executado via `display()`.

---

## Databricks App

### Source code path recomendado

```text
/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app
```

### Shared data root recomendado

```text
/Workspace/Users/u235107@inetpsa.com/Drafts/sci/dados
```

Esse desenho evita reempacotar arquivos grandes de dados dentro do App e reduz o risco de deploy falhar por limite de tamanho.

---

## Configuração do Job diário

### Importar via UI

1. Abra **Workflows → Jobs → Create Job**
2. Selecione **Import** e carregue `jobs/job_diario.json`
3. Para cada task, clique em `existing_cluster_id` e troque
   `__DEFINIR_NA_UI__` pelo ID do seu cluster
4. Ajuste o horário (`schedule.quartz_cron_expression`):
   - `0 0 6 * * ?` = todo dia às 06:00 (Europa/Paris)
5. Troque `pause_status` de `PAUSED` para `UNPAUSED` quando pronto

### Estrutura do Job

```
pipeline_workspace (03_processar_e_publicar_delta.py)
    ANO=2026, RUN_BUDGET=true, RUN_REAL=true
    max_retries=2, retry_interval=5min
        ↓ depends_on
pos_validacao (05_validacao_pos_job.py)
    ANO=2026
    max_retries=1, retry_interval=5min
```

---

## Variáveis de ambiente relevantes

| Variável             | Valor esperado    | Onde setar                    |
|----------------------|-------------------|-------------------------------|
| `SCI_DATA_BACKEND`   | `databricks`      | Setado pelos notebooks        |
| `SCI_SHARED_DATA_ROOT` | path Workspace  | Setado por `resolve_data_root` |
| `SCI_PUBLISH_ROOT`   | path Workspace    | Setado por `get_publish_root`  |
| `SCI_CLOUD`          | `1`               | Cluster env vars (opcional)   |

Para configurar `SCI_CLOUD=1` no cluster:
**Cluster → Edit → Advanced Options → Spark → Environment Variables**
```
SCI_CLOUD=1
SCI_DATA_BACKEND=databricks
```

---

## Módulos `src/sci_core/`

### `backend.py`
```python
from sci_core.backend import resolve_data_root, get_publish_root, get_backend

DATA_ROOT = resolve_data_root(user_input, repo_root=REPO_ROOT, log=print)
PUBLISH_ROOT = get_publish_root(user_input, repo_root=REPO_ROOT, log=print)
backend = get_backend()  # "databricks" | "snowflake"
```

### `io_excel.py`
```python
from sci_core.io_excel import read_principal_excel

frames: dict[str, pd.DataFrame] = read_principal_excel(excel_path, ano=2026)
# frames["massa primária - BDG"], frames["Sapiens"], etc.
```

### `io_delta.py`
```python
from sci_core.io_delta import to_spark_safe, write_delta, read_dataset

spark_df = to_spark_safe(spark, pandas_df, log=print)
mode = write_delta(spark_df, path, partition_by=["Ano"], log=print)
# mode == "delta" ou "parquet" (fallback automático)
df = read_dataset(spark, path, log=print)
```

### `io_workspace.py`
```python
from sci_core.io_workspace import write_parquet_dataset, read_parquet_dataset

write_parquet_dataset(df, path, mode="overwrite", partition_by=["Ano"])
pdf = read_parquet_dataset(path)  # retorna pandas DataFrame
```

### `transform_principal.py`
```python
from sci_core.transform_principal import processar_budget, processar_real, build_comparativo

bud_outputs = processar_budget(spark, ano=2026, frames=frames)
# {"tc_principal_bud": Spark DataFrame}

real_outputs = processar_real(spark, ano=2026, frames=frames)
# {"tc_principal_real": Spark DataFrame}

comp_df = build_comparativo(bud_outputs["tc_principal_bud"],
                             real_outputs["tc_principal_real"])
# Spark DataFrame com colunas: Ano, Origem, QtdLinhas, ValorTotal, IngestionTs
```

---

## Schema dos datasets publicados

### `tc_principal_bud` e `tc_principal_real`

| Coluna      | Tipo    | Descrição                        |
|-------------|---------|----------------------------------|
| Ano         | Long    | Ano de referência                |
| Origem      | String  | "BUDGET" ou "REAL"               |
| Aba         | String  | Nome da aba do Excel             |
| Linha       | Long    | Número sequencial da linha       |
| Chave1      | String  | Primeira coluna chave            |
| Chave2      | String  | Segunda coluna chave             |
| Valor       | Double  | Valor numérico principal         |
| PayloadJson | String  | Linha completa em JSON           |
| IngestionTs | Timestamp | Timestamp de ingestão          |

### `tc_comparativo`

| Coluna     | Tipo      | Descrição               |
|------------|-----------|-------------------------|
| Ano        | Long      | Ano de referência       |
| Origem     | String    | "BUDGET" ou "REAL"      |
| QtdLinhas  | Long      | Contagem de registros   |
| ValorTotal | Double    | Soma dos valores        |
| IngestionTs | Timestamp | Timestamp de ingestão  |

### `tc_execucao_log`

| Coluna   | Tipo      | Descrição                     |
|----------|-----------|-------------------------------|
| Ano      | Long      | Ano de referência             |
| Tabela   | String    | Nome da tabela gravada        |
| Linhas   | Long      | Quantidade de linhas          |
| Status   | String    | "OK"                          |
| Mensagem | String    | Caminho completo              |

---

## Troubleshooting

### `FileNotFoundError: Excel não encontrado`

Verifique se o arquivo está no caminho correto:
```
/Workspace/.../sci/dados/TC_Principal/2026/Reporting veículos.xlsx
```
O notebook tenta também `Reporting veiculos.xlsx` (sem cedilha).

Upload via: **Workspace → pasta do ano → Upload file**

---

### `ImportError: No module named 'sci_core'`

O `sys.path` não incluiu `REPO_ROOT/src`. Verifique se o notebook tem:
```python
for entry in (REPO_ROOT, f"{REPO_ROOT}/src"):
    if entry not in sys.path:
        sys.path.insert(0, entry)
```

---

### `ModuleNotFoundError: No module named 'openpyxl'`

Instale no cluster: **Cluster → Libraries → Install → PyPI → `openpyxl`**

---

### `write_delta falhou — usando Parquet`

Log esperado quando Delta Lake não está disponível no cluster:
```
[WARN] Falha ao gravar Delta em .../tc_principal_bud: ...
[WARN] Fallback para Parquet em Workspace Files
[INFO] Delta gravado com sucesso em ... (Parquet)
```
Isso é **comportamento normal**. Os datasets funcionam igualmente em Parquet.
Para ativar Delta, use um cluster com **Databricks Runtime ML** ou
instale a library `delta-spark`.

---

### `CHECK SAÚDE: ERRO` no notebook 05

Dataset ausente ou vazio para o ano configurado. Execute:
1. O notebook 03 (`RUN_BUDGET=true`, `RUN_REAL=true`, `ANO=2026`)
2. Depois o notebook 05 novamente

---

### Arrow serialization error em `to_spark_safe`

A função desliga o Arrow automaticamente:
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
```
Se o erro persistir, verifique se o pandas DataFrame tem colunas com tipos
mistos (ex: coluna com int e string). Isso é normalizado por `_normalize_pandas`.

---

## Deploy v2.29 — Passo a Passo Completo

### O que mudou nesta versão

| Componente | Alteração |
|-----------|-----------|
| `tc_core/data_router.py` | Data Router com `read_optimized()` — seleciona THIN/AGG/FULL |
| `tc_core/telemetry.py` | Módulo de telemetria (`log_data_source`, `perf_timer`) |
| `tc_core/feature_flags.py` | Flag `SCI_DEBUG_DATA_TRACE` adicionada |
| `tc_core/ui/debug_panel.py` | Painel de debug no rodapé do app |
| `tc_core/parquet_schemas.py` | Schemas THIN/AGG + documentação multi-planta |
| `tc_principal/shared.py` | Loaders com telemetria + `read_optimized` |
| `tc_principal/pages/home_tc.py` | Fix: Home usa `load_forecast_completo()` (bypass correto) |
| `tc_principal/pages/waterfall_tc.py` | Fix: Remove "Account"/"Texto breve" das dimensões base |
| `tc_principal/pages/best_estimate_simulador_tc.py` | Gera `forecast_agg.parquet` ao salvar |
| `pages/1_Waterfall.py` | Fix: Mesma limpeza de dimensões para TC_Ext |
| `tc_ext/pages/home_ext.py` | Telemetria de carregamento de dados |
| `app.py` | Debug panel integrado |
| `app.yaml` | `SCI_USE_OPTIMIZED_PARQUETS=true` |

### Pré-requisito: Configurar Databricks CLI

O script `sync_databricks_app.ps1` precisa de autenticação no Databricks CLI.

**Opção A — Token pessoal (.databrickscfg)**:

1. No Databricks: **Settings → Developer → Access tokens → Generate new token**
2. Crie o arquivo `%USERPROFILE%\.databrickscfg`:
```ini
[DEFAULT]
host = https://adb-5678659344564033.13.azuredatabricks.net
token = dapi_SEU_TOKEN_AQUI
```

**Opção B — Variáveis de ambiente (.env)**:

Crie `.env` na raiz do projeto:
```
DATABRICKS_HOST=https://adb-5678659344564033.13.azuredatabricks.net
DATABRICKS_TOKEN=dapi_SEU_TOKEN_AQUI
```

**Opção C — Profile nomeado (múltiplos ambientes)**:

```ini
# %USERPROFILE%\.databrickscfg
[meu_profile]
host = https://adb-5678659344564033.13.azuredatabricks.net
token = dapi_SEU_TOKEN_AQUI
```

E defina a variável `SCI_DATABRICKS_PROFILE=meu_profile` no `.env` ou execute:
```powershell
.\scripts\sync_databricks_app.ps1 -Profile meu_profile
```

### Passo 1 — Testar autenticação

```powershell
databricks current-user me
```

Sucesso: retorna JSON com seu `userName`. Se der erro, revise `.databrickscfg`.

### Passo 2 — Sincronizar e fazer deploy

```powershell
# Sync completo (espelho local + upload + deploy SNAPSHOT)
.\scripts\sync_databricks_app.ps1

# Se quiser só upload sem deploy:
.\scripts\sync_databricks_app.ps1 -SkipDeploy

# Se quiser só deploy (código já está no Workspace):
.\scripts\sync_databricks_app.ps1 -DeployOnly

# Modo watch (sync contínuo em tempo real):
.\scripts\sync_databricks_app.ps1 -Watch
```

O script faz:
1. Atualiza espelhos locais: `Databricks/sci_app/` e `Databricks/sci/`
2. Upload de todos os arquivos para `/Workspace/.../sci_app/sci_app`
3. Deploy SNAPSHOT do app `sci` com `--no-wait`

### Passo 3 — Executar notebooks de processamento (no Databricks)

Abra o Databricks e execute **na ordem**:

1. **`notebooks/00_validar_ambiente_databricks.py`** — confirma que Excel e paths OK
2. **`notebooks/03_processar_e_publicar_delta.py`** (`ANO=2026`, `RUN_BUDGET=true`, `RUN_REAL=true`)
   - Gera parquets: FULL + THIN + AGG
3. **`notebooks/05_validacao_pos_job.py`** (`ANO=2026`)
   - Confirma se todos os parquets obrigatórios existem

> O `forecast_agg.parquet` NÃO é gerado pelos notebooks — é gerado automaticamente
> pelo **Best Estimate Simulator** quando o usuário salva o forecast no app.

### Passo 4 — Validação end-to-end no App

Acesse o app SCI no Databricks e execute:

| Teste | O que verificar | Critério |
|-------|----------------|----------|
| **Home** | Gráfico de Custo FP por período | Barras roxas + Flex Budget laranja |
| **Home + BE** | Ativar checkbox "Incluir BE" | Barras históricas + BE roxo claro aparecem |
| **Waterfall** | Gráfico cascata por Oficina | Sem colunas fantasma "Account"/"Texto breve" |
| **Waterfall** coluna BE | Meses futuros aparecem em roxo claro | Valores batem com forecast |
| **BE Simulator** | Salvar forecast | Deve gerar `forecast_agg.parquet` automaticamente |
| **Debug Panel** | Definir `SCI_DEBUG_DATA_TRACE=true` no app.yaml | Painel no rodapé mostra THIN/AGG/FULL por dataset |

### Passo 5 — Ativar debug de telemetria (opcional)

Adicione no `app.yaml`:
```yaml
  - name: SCI_DEBUG_DATA_TRACE
    value: "true"
```

Depois de deploy, o rodapé do app mostrará uma tabela com:
- Dataset carregado → fonte usada (THIN/AGG/FULL) → nrows → tempo de load

Para desativar, remova a variável ou mude para `"false"`.

### Passo 6 — Limpeza pós-deploy

Após validação completa, você pode:
1. Desativar `SCI_DEBUG_DATA_TRACE` no `app.yaml`
2. Confirmar que o Job diário está configurado para rodar `notebooks/03` + `notebooks/05`
3. Verificar que `SCI_USE_OPTIMIZED_PARQUETS=true` está no `app.yaml` (já está)

---

## Checklist de aceite

Execute após cada deploy ou mudança:

- [ ] Notebook 00 → última linha `EXCEL_OK=True`
- [ ] Notebook 01 → 4 diretórios criados em `workspace_publish/`
- [ ] Notebook 03 → tabela RESUMO FINAL com `Status=OK` para todas as tabelas
- [ ] Notebook 05 → `CHECK SAÚDE: OK`
- [ ] Notebook 06 → exibe dados de `tc_principal_real` com `LINHAS > 0`
- [ ] Home → gráfico Custo FP por período sem erros
- [ ] Waterfall → sem colunas fantasma "Account" / "Texto breve"
- [ ] BE Simulator → salvar gera `forecast_agg.parquet`
- [ ] Debug Panel → ativando `SCI_DEBUG_DATA_TRACE=true`, mostra tabela de telemetria
- [ ] Job configurado → `existing_cluster_id` não é `__DEFINIR_NA_UI__`
- [ ] Job agendado → `pause_status = UNPAUSED`

---

## Roadmap fase 2 (PENDENTE ICT)

| Item                              | Bloqueio                          |
|-----------------------------------|-----------------------------------|
| Carga Snowflake (`02_carga_snowflake.py`) | Secret Scope + permissões ABPZA/B/C |
| `SCI_DATA_BACKEND=snowflake`       | Spark-Snowflake connector no cluster |
| Alertas por e-mail (Graph API)    | Azure AD client_id + tenant_id    |
| Secret Scope `sci`                | ICT deve criar + conceder acesso  |

Quando desbloqueados, os módulos já estão preparados:
- `tc_core/secrets.py` → escopo `"sci"` já configurado
- `alertas/email_graph.py` → guard `is_cloud()` já implementado
- `alertas/scheduler.py` → desabilitado em cloud, pronto para reabilitar

---

*Última atualização: gerado automaticamente pelo GitHub Copilot.*
