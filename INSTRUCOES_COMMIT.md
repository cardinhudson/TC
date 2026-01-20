# 📝 Instruções para Commit - Sistema TC

## ✅ Configuração Concluída

O projeto foi configurado para **excluir arquivos grandes** do Git:

### Arquivos Excluídos (via .gitignore):
- ✅ **Arquivos Parquet** (*.parquet) - Dados processados
- ✅ **Arquivos Excel** (*.xlsx, *.xls) - Arquivos de dados de origem
- ✅ **Arquivos de log** (.processamento_log.txt)
- ✅ **Cache** (*.db, cache/)
- ✅ **Arquivos temporários** (~$*.xlsx)

## 🚀 Como Fazer o Commit

### Opção 1: Script Automatizado (Recomendado)
```powershell
.\fazer_commit.ps1
```

Este script irá:
1. Adicionar arquivos importantes
2. Mostrar o status
3. Solicitar mensagem do commit
4. Perguntar se deseja fazer push

### Opção 2: Manual
```powershell
# 1. Adicionar arquivos
git add .gitignore app.py versao.json controle_paginas.json taxas_cambio.db
git add *.md *.py requirements*.txt

# 2. Verificar status
git status

# 3. Fazer commit
git commit -m "Feat: Implementação de visualização completa do ano no Flex Bud"

# 4. Fazer push
git push
```

## 📊 Arquivos que Serão Commitados

### Arquivos Principais:
- ✅ `app.py` - Portal/roteador (entrada do Streamlit)
- ✅ `tc_ext/` - Módulo do TC Ext (Home em `tc_ext/pages/home_ext.py`)
- ✅ `tc_principal/` - Módulo do TC (Planta Principal) (stubs)
- ✅ `.gitignore` - Configuração atualizada
- ✅ `versao.json` - Controle de versão
- ✅ `controle_paginas.json` - Controle de páginas
- ✅ `taxas_cambio.db` - Taxas de câmbio

### Arquivos de Documentação:
- ✅ `DOCUMENTACAO_FLEX_BUD_ANO_COMPLETO.md`
- ✅ Outros arquivos .md

### Scripts Auxiliares:
- ✅ Scripts Python (*.py)
- ✅ `requirements.txt`
- ✅ `requirements-chatbot.txt` (opcional, para modo semântico do chatbot)

## ⚠️ Arquivos NÃO Serão Commitados

### Dados Grandes (já removidos do Git):
- ❌ `dados/**/*.parquet` (arquivos de dados processados)
- ❌ `dados/**/*.xlsx` (arquivos Excel de origem)
- ❌ `cache/*.db` (cache do sistema)

Estes arquivos permanecem **localmente** mas não vão para o repositório Git.

## 🎯 Mensagem de Commit (modelo)

```
Feat: Ajustes no portal e documentação

- Atualiza portal/roteamento do Streamlit
- Mantém documentação como fonte única
- Alinha instruções de ambiente/instalação
```

## ✅ Próximos Passos

1. Execute `.\fazer_commit.ps1`
2. Digite a mensagem do commit
3. Confirme o push
4. Pronto! Suas alterações estarão no repositório

---
**Data:** 20/01/2026  
**Modificado por:** GitHub Copilot
