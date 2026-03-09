# Guia de Geração do Executável - Stellantis Cost Intelligence (SCI)

## Visão Geral

O sistema SCI pode ser distribuído como um **executável standalone** que não requer Python instalado no computador de destino. Este guia documenta o processo completo de geração e distribuição.

---

## Pré-requisitos

### No computador de desenvolvimento:
- Python 3.11+ instalado
- Ambiente virtual `.venv` configurado com todas as dependências
- Acesso à pasta do projeto `TC`

### Pacotes necessários:
```bash
pip install pyinstaller pywebview
```

---

## Passo a Passo para Gerar o Executável

### 1. Abrir terminal na pasta do projeto
```powershell
cd C:\user\U235107\GitHub\TC
```

### 2. Ativar ambiente virtual
```powershell
.venv\Scripts\Activate.ps1
```

### 3. Executar o script de build
**Opção A - Usando o script automatizado:**
```powershell
.\build_exe.bat
```

**Opção B - Comando manual (mínimo):**
```powershell
C:/User/U235107/GitHub/TC/.venv/Scripts/python.exe -m PyInstaller --clean --noconfirm SCI.spec
```

> Importante: o fluxo atual usa a `SCI.spec`, porque ela centraliza `hiddenimports`,
> metadados e pacotes com import dinâmico como `python-pptx`, `reportlab`, `msal`
> e os módulos recentes da `Central de Alertas`.

#### Observações importantes (para reprodução fiel)

- O método oficial usado aqui é **`PyInstaller` com `SCI.spec`**.
- Se durante o build aparecer um aviso do PyInstaller dizendo que a pasta `dist\Stellantis-Cost-Intelligence` (e todo o conteúdo) será removida e pedindo confirmação, responda `Y`.
- Se o build falhar com `SyntaxError: invalid non-printable character U+FEFF` em `app.py`, o arquivo está com **BOM (Byte Order Mark)**. Corrija com PowerShell:

```powershell
$content = Get-Content "app.py" -Raw
$content = $content -replace '^\xEF\xBB\xBF', ''
[System.IO.File]::WriteAllText("app.py", $content, [System.Text.UTF8Encoding]::new($false))
```

### 4. Copiar dados para _internal (se necessário)
O script `build_exe.bat` já faz isso automaticamente, mas caso precise manualmente:

```powershell
$dest = "dist\Stellantis-Cost-Intelligence\_internal"

# Pastas de dados
xcopy "dados" "$dest\dados\" /E /I /Y /Q
xcopy "pages" "$dest\pages\" /E /I /Y /Q
xcopy "tc_core" "$dest\tc_core\" /E /I /Y /Q
xcopy "tc_principal" "$dest\tc_principal\" /E /I /Y /Q
xcopy "tc_ext" "$dest\tc_ext\" /E /I /Y /Q
xcopy "tc_copilot" "$dest\tc_copilot\" /E /I /Y /Q
xcopy "alertas" "$dest\alertas\" /E /I /Y /Q
xcopy ".streamlit" "$dest\.streamlit\" /E /I /Y /Q

# Arquivos Python de processamento
Copy-Item "processamento_dados.py" $dest
Copy-Item "processamento_dados_BUD.py" $dest
Copy-Item "processamento_dados_veiculos_BUD.py" $dest
Copy-Item "versionamento.py" $dest
Copy-Item "sincronizar_notebooks.py" $dest
Copy-Item "tc_exports.py" $dest

# Configurações
Copy-Item "versao.json" $dest
Copy-Item "dados_equipe.json" $dest
Copy-Item "rateios_manuais.json" $dest
Copy-Item "controle_paginas.json" $dest

# Imagens e documentação
Copy-Item "SCI_faixa.png" $dest
Copy-Item "Designer.png" $dest
Copy-Item "DOCUMENTACAO_SISTEMA_TC.md" $dest
Copy-Item "DOCUMENTACAO_TC_PRINCIPAL.md" $dest

# (NOVO) AgGrid no executável: copiar o pacote para dentro do _internal
# Motivo: páginas do Streamlit são carregadas em runtime, e o empacotador pode não incluir o st_aggrid.
Copy-Item ".venv\Lib\site-packages\st_aggrid" -Destination "$dest\st_aggrid" -Recurse -Force
Copy-Item ".venv\Lib\site-packages\streamlit_aggrid-*.dist-info" -Destination $dest -Recurse -Force
```

---

## Estrutura do Executável Gerado

```
dist/Stellantis-Cost-Intelligence/
├── Stellantis-Cost-Intelligence.exe    ← Executável principal (duplo-clique)
└── _internal/                          ← Todos os recursos bundled
    ├── app.py                          ← Aplicação Streamlit principal
    ├── pages/                          ← Páginas multipage
    │   ├── 1 - Waterfall.py
    │   ├── 2 - Best Estimate - Simulador.py
    │   ├── 5 - Extração de Dados.py
    │   └── 6 - Documentacao.py
    ├── dados/                          ← Dados Parquet e Excel
    │   ├── TC_Ext/
    │   │   ├── 2025/
    │   │   ├── 2026/
    │   │   ├── Forecast/
    │   │   └── historico_consolidado/
    │   └── TC_Principal/
    │       ├── 2025/
    │       ├── 2026/
    │       └── ...
    ├── tc_core/                        ← Módulos core
    ├── tc_principal/                   ← Módulos TC Principal
    ├── tc_ext/                         ← Módulos TC Ext
   ├── tc_copilot/                     ← Relatórios e assistente IA
   ├── alertas/                        ← Central de Alertas + Graph/Teams
    ├── .streamlit/                     ← Configurações Streamlit
    ├── processamento_dados.py          ← Processamento de dados REAIS
    ├── processamento_dados_BUD.py      ← Processamento de dados BUDGET
    ├── versionamento.py                ← Sistema de versões
    ├── versao.json                     ← Versão atual
    ├── dados_equipe.json               ← Configuração da equipe
    ├── rateios_manuais.json            ← Rateios configurados
    ├── controle_paginas.json           ← Controle de acesso a páginas
    ├── streamlit/                      ← Pacote Streamlit bundled
    ├── streamlit-1.50.0.dist-info/     ← Metadados (CRÍTICO!)
    └── ... (DLLs e dependências)
```

---

## Portabilidade e Independência

### ✅ O executável é 100% independente porque:

1. **Python Runtime Bundled**: O interpretador Python está incluído em `_internal/`
2. **Todas as dependências**: Streamlit, Pandas, NumPy, etc. estão bundled
3. **Dados locais**: A pasta `dados/` dentro de `_internal/` é usada para leitura e escrita
4. **Caminhos relativos**: O código usa `get_base_path()` que aponta para `_internal/` no modo EXE

### Como funciona a portabilidade:

```python
# tc_core/utils/portabilidade.py
def get_base_path() -> Path:
    if getattr(sys, "frozen", False):  # Modo EXE
        return Path(sys._MEIPASS)       # → _internal/
    return Path(__file__).parents[2]    # Modo Dev → raiz do repo
```

### Extrações e Verificações:

| Funcionalidade | Caminho no EXE |
|----------------|----------------|
| Dados TC_Ext | `_internal/dados/TC_Ext/` |
| Dados TC_Principal | `_internal/dados/TC_Principal/` |
| Processamento REAIS | `_internal/processamento_dados.py` |
| Processamento BUDGET | `_internal/processamento_dados_BUD.py` |
| Versão do sistema | `_internal/versao.json` |
| Configuração equipe | `_internal/dados_equipe.json` |

---

## Distribuição

### Para distribuir o sistema:

1. **Copie a pasta inteira**:
   ```
   dist/Stellantis-Cost-Intelligence/  (~500-800 MB)
   ```

2. **No computador destino**:
   - Extraia/cole a pasta em qualquer local
   - Execute `Stellantis-Cost-Intelligence.exe`
   - O sistema abre automaticamente no navegador/janela desktop

### Requisitos do computador destino:
- Windows 10/11 (64-bit)
- ~1 GB de espaço em disco
- **NÃO precisa de Python instalado**
- **NÃO precisa de bibliotecas adicionais**

---

## Atualizando Dados no Executável

### Para atualizar os dados Parquet:

1. Substitua os arquivos em:
   ```
   dist/Stellantis-Cost-Intelligence/_internal/dados/TC_Ext/
   dist/Stellantis-Cost-Intelligence/_internal/dados/TC_Principal/
   ```

2. Ou execute a extração dentro do próprio executável (página "Extração de Dados")

### Para atualizar configurações:

Edite diretamente os arquivos JSON em `_internal/`:
- `versao.json` - Versão do sistema
- `dados_equipe.json` - Configuração da equipe
- `rateios_manuais.json` - Rateios manuais

---

## Solução de Problemas

### Erro: "O sistema não abre / fecha imediatamente"
1. Verifique se `_internal/streamlit-1.50.0.dist-info/` existe
2. Execute com console para ver erros:
   - Abra CMD na pasta do EXE
   - Execute: `Stellantis-Cost-Intelligence.exe`
   - Observe as mensagens de erro

### Erro: "Arquivos não encontrados"
- Verifique se `_internal/dados/` contém os arquivos Parquet

### Aviso/Erro: "⚠️ Tabelas interativas (AgGrid) indisponíveis: módulo 'st_aggrid' não encontrado"
**Causa típica no EXE:** o pacote `st_aggrid` não foi incluído automaticamente pelo empacotamento.

**Correção (recomendado):** gere o EXE com `build_exe.bat` (ele já copia `st_aggrid` para `_internal/`).

**Se já tiver um EXE pronto:** copie manualmente para `dist/<NOME>/_internal/`:
- Pasta `st_aggrid/`
- Pasta `streamlit_aggrid-*.dist-info/`

**Fallback:** quando o AgGrid não existe, o app usa modo simplificado. Neste modo, a exclusão agora
funciona via `st.data_editor` com checkbox (inclui opção **Selecionar Todos**).
- Confirme que `processamento_dados.py` está em `_internal/`

### Erro: "Porta 8501 em uso"
- Feche outras instâncias do SCI
- Ou execute: `taskkill /F /IM "Stellantis-Cost-Intelligence.exe"`

### Log de erros:
Verifique `SCI_error.log` ao lado do executável ou em `_internal/`

---

## Ferramenta de Build

O sistema usa **`PyInstaller` com `SCI.spec`** para gerar o executável, que:
- Usa `launcher.py` como ponto de entrada para abrir o Streamlit em janela desktop com `pywebview`
- Centraliza `hiddenimports`, metadados e recursos do projeto em um único arquivo versionado
- Inclui pacotes com import dinâmico usados no SCI, como `python-pptx`, `reportlab`, `openai`, `msal` e módulos da `Central de Alertas`

---

## Arquivos Importantes do Projeto

| Arquivo | Descrição |
|---------|-----------|
| `build_exe.bat` | Script automatizado de build |
| `Stellantis-Cost-Intelligence.spec` | Configuração PyInstaller (gerado) |
| `tc_core/utils/portabilidade.py` | Lógica de portabilidade Dev↔EXE |
| `.streamlit/config.toml` | Configurações do Streamlit |

---

## Notas Técnicas

### Por que `streamlit-desktop-app`?
- Resolve automaticamente o problema de metadados do Streamlit
- Gera launcher que abre como janela desktop (pywebview)
- Funciona igual ao DashAPPwin11 (sistema de referência)

### Diferença entre modos:
| Aspecto | Modo Dev | Modo EXE |
|---------|----------|----------|
| `sys.frozen` | `False` | `True` |
| `get_base_path()` | Raiz do repo | `sys._MEIPASS` (_internal/) |
| Servidor | `streamlit run app.py` | Subprocess interno |
| Interface | Navegador padrão | pywebview (janela nativa) |

---

*Última atualização: Fevereiro/2026*
*Desenvolvido por Hudson Cardin e Lauro Paiva*
