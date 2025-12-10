# Instruções para Sincronizar o Repositório no Outro PC

## Status Atual do Repositório
- **Repositório**: https://github.com/cardinhudson/TC.git
- **Branch**: main
- **Último commit**: 38ef3ba - "Atualizar taxas de câmbio"
- **Tag criada**: v20251210-132051

## Comandos para Executar no Outro PC

### Opção 1: Pull Normal (se não houver conflitos)
```bash
cd C:\GIT\TC
git fetch origin
git pull origin main
```

### Opção 2: Reset Hard (se houver problemas de sincronização)
⚠️ **ATENÇÃO**: Este comando vai descartar todas as alterações locais não commitadas!

```bash
cd C:\GIT\TC
git fetch origin
git reset --hard origin/main
```

### Opção 3: Clone Limpo (se nada funcionar)
```bash
# Fazer backup do diretório atual primeiro!
cd C:\GIT
# Renomear o diretório atual para backup
mv TC TC_backup
# Clonar novamente
git clone https://github.com/cardinhudson/TC.git
```

## Verificar se Está Sincronizado

Após executar os comandos, verifique:

```bash
git status
git log --oneline -5
```

Deve mostrar:
- `Your branch is up to date with 'origin/main'`
- Último commit: `38ef3ba Atualizar taxas de câmbio`

## Arquivos Principais Atualizados

- `pages/1 - Waterfall.py` - Tabelas no modo Total corrigidas
- `taxas_cambio.db` - Banco de dados atualizado
- Todas as alterações anteriores estão commitadas

## Se Ainda Não Funcionar

1. Verificar conexão com GitHub:
   ```bash
   git remote -v
   ```

2. Forçar atualização:
   ```bash
   git fetch --all
   git reset --hard origin/main
   ```

3. Limpar cache do Git (se necessário):
   ```bash
   git clean -fd
   git reset --hard origin/main
   ```

