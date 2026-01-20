# Como Selecionar o Ambiente Virtual

O ambiente virtual do projeto (preferencialmente `.venv`) pode ser usado automaticamente pelo VS Code/Cursor.

## ✅ Configuração Automática

O arquivo `.vscode/settings.json` já está configurado para:
- Usar automaticamente o ambiente virtual `.venv/Scripts/python.exe`
- Ativar o ambiente virtual nos terminais integrados
- Configurar o PYTHONPATH corretamente

## 🔧 Selecionar Manualmente (se necessário)

### No VS Code / Cursor:

1. **Pressione `Ctrl+Shift+P`** (ou `Cmd+Shift+P` no Mac)
2. Digite: **"Python: Select Interpreter"**
3. Selecione: **`.\.venv\Scripts\python.exe`**

Ou:

1. Clique no **seletor de interpretador Python** na barra inferior (canto inferior direito)
2. Selecione: **`.\.venv\Scripts\python.exe`**

### Verificar se está selecionado:

- Na barra inferior do VS Code/Cursor, você deve ver algo como: **`Python 3.13.x ('.venv': venv)`**
- Ou o caminho: **`.\.venv\Scripts\python.exe`**

## 🚀 Ativar no Terminal

### PowerShell:
```powershell
\.\.venv\Scripts\Activate.ps1
```

### CMD:
```cmd
.venv\Scripts\activate.bat
```

### Scripts Automáticos:
- **Windows CMD:** Dê duplo clique em `ativar_ambiente.bat`
- **PowerShell:** Execute `.\ativar_ambiente.ps1`

## ✅ Verificar Instalação

Para verificar se o ambiente virtual está funcionando:

```bash
python --version
# Deve mostrar: Python 3.13.7

pip list
# Deve mostrar streamlit 1.50.0, pandas 2.3.3, etc.
```

## 🔍 Solução de Problemas

### O ambiente virtual não aparece na lista:

1. Feche e reabra o VS Code/Cursor
2. Certifique-se de que a pasta `.venv` existe (ou `venv`, se você usa esse nome)
3. Verifique se o arquivo `.vscode/settings.json` existe e está correto

### O terminal não ativa automaticamente:

1. Verifique se `python.terminal.activateEnvironment` está como `true` no `.vscode/settings.json`
2. Feche e reabra o terminal integrado (`Ctrl+Shift+` `)

### Erro de política de execução no PowerShell:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 📝 Notas

- O ambiente virtual recomendado está na pasta `.venv/`
- O interpretador Python está em `.venv/Scripts/python.exe`
- Todas as dependências estão instaladas no ambiente virtual
- O arquivo `.gitignore` já está configurado para ignorar `.venv/` (ou `venv/`, se existir)


