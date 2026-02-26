# Instruções para Sincronizar o Repositório no Outro PC

## Status Atual do Repositório
- **Repositório**: https://github.com/cardinhudson/TC.git
- **Branch**: main

> Observação: não fixe hash/tag aqui (desatualiza rápido). Use `git log --oneline -5` para ver o estado real.

## Comandos para Executar no Outro PC

### Opção 1: Pull Normal (se não houver conflitos)
```bash
cd <CAMINHO_PARA_O_REPO>\TC
git fetch origin
git pull origin main
```

### Opção 2: Reset Hard (se houver problemas de sincronização)
⚠️ **ATENÇÃO**: Este comando vai descartar todas as alterações locais não commitadas!

```bash
cd <CAMINHO_PARA_O_REPO>\TC
git fetch origin
git reset --hard origin/main
```

### Opção 3: Clone Limpo (se nada funcionar)
```bash
# Fazer backup do diretório atual primeiro!
cd <CAMINHO_PARA_ONDE_VOCE_QUER_O_REPO>
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
- Os commits mais recentes do branch `main`

## Arquivos Principais Atualizados

- `app.py` - portal/roteador de navegação (entrada do Streamlit)
- `tc_ext/pages/home_ext.py` - Home do TC Ext (código principal legado)
- `pages/*` - páginas legadas (Waterfall/Best Estimate/Extração/Documentação)
- `tc_principal/pages/*` - páginas do TC Veículos

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

