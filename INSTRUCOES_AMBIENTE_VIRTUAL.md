# Instruções para Usar o Ambiente Virtual

## Ambiente Virtual Criado

O projeto usa um ambiente virtual Python com todas as dependências necessárias.

Padrão atual recomendado: `.venv/` (mais comum em projetos modernos).

Compatibilidade: se você já usa `venv/`, o projeto continua funcionando — mas as instruções abaixo assumem `.venv/`.

Observação prática: neste repositório podem existir automações (ex.: tasks do VS Code) apontando para `venv/`. Se o seu ambiente estiver em `.venv/`, ajuste o caminho da task (ou crie o ambiente com o mesmo nome esperado).

## Como Usar no Outro PC

### Opção 1: Recriar o Ambiente Virtual (Recomendado)

Como ambientes virtuais não são portáveis entre diferentes sistemas operacionais ou arquiteturas, a melhor opção é recriar o ambiente virtual no outro PC:

1. **Copie o arquivo `requirements.txt`** para o outro PC

2. **Crie um novo ambiente virtual:**
   ```bash
   python -m venv .venv
   ```

3. **Ative o ambiente virtual:**
   
   **No Windows (PowerShell):**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
   
   **No Windows (CMD):**
   ```cmd
   .venv\Scripts\activate.bat
   ```
   
   **No Linux/Mac:**
   ```bash
   source .venv/bin/activate
   ```

4. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Execute a aplicação:**
   ```bash
   streamlit run app.py
   ```

### Opção 2: Usar o Ambiente Virtual Existente (Apenas Windows com mesma arquitetura)

Se ambos os PCs forem Windows com a mesma arquitetura (ex: ambos 64-bit), você pode:

1. **Copiar a pasta `.venv` inteira** para o outro PC (mantendo a mesma estrutura de pastas)

2. **Ativar o ambiente virtual:**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

3. **Executar a aplicação:**
   ```bash
   streamlit run app.py
   ```

⚠️ **Nota:** Esta opção pode não funcionar se os PCs tiverem versões diferentes do Python ou arquiteturas diferentes.

## Dependências Instaladas

O ambiente virtual contém versões **EXATAS** (não >=) para garantir compatibilidade:

- **streamlit==1.50.0** (versão específica para garantir compatibilidade com `st.expander`)
- **pandas==2.3.3** (versão específica para renderização consistente de tabelas)
- **numpy==2.3.5** (versão específica compatível com pandas 2.3.3)
- **altair==5.5.0** (versão específica para gráficos Altair)
- **plotly==6.5.0** (versão específica para gráficos Plotly)
- **openpyxl==3.1.5** (versão específica para leitura/escrita de arquivos Excel)
- E todas as dependências transitivas necessárias

### ⚠️ IMPORTANTE: Por que versões exatas?

**Problema comum:** Se usar `>=` em vez de `==`, diferentes PCs podem instalar versões diferentes das bibliotecas, causando:
- Problemas de renderização de tabelas (`st.dataframe()`)
- Comportamento inconsistente com tipos de dados (category, downcast)
- Erros ao usar `st.expander`
- Problemas na formatação de DataFrames
- Diferenças na renderização de gráficos
- **Erro específico:** "não podemos usar tabelas aninhadas com 3 camadas" - O código foi corrigido para usar containers no nível 3 em vez de expanders aninhados

**Solução:** Sempre use as versões EXATAS especificadas no `requirements.txt`.

### 🔧 Correção Aplicada: Expanders Aninhados

O código foi ajustado para evitar o problema de "tabelas aninhadas com 3 camadas" que pode ocorrer no Streamlit 1.50.0:
- **Problema:** Expanders aninhados em 3 níveis (Custo → Type 05 → Type 06) podem causar erros de renderização
- **Solução:** O terceiro nível (Type 06) agora usa `st.container()` em vez de `st.expander()`, mantendo a funcionalidade mas evitando o problema de aninhamento
- **Resultado:** As tabelas funcionam corretamente em todos os PCs, independente de configurações específicas

## Verificar Instalação

Para verificar se tudo está instalado corretamente:

```bash
pip list
```

Ou verificar uma biblioteca específica:

```bash
pip show streamlit
```

## Solução de Problemas

### Erro ao ativar no PowerShell
Se receber um erro de política de execução no PowerShell, execute:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Erro de versão do Streamlit ou problemas com tabelas
Se houver problemas com renderização de tabelas ou erros relacionados a versões:

1. **Desinstale todas as dependências:**
   ```bash
   pip uninstall streamlit pandas numpy altair plotly openpyxl -y
   ```

2. **Reinstale usando o requirements.txt:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verifique as versões instaladas:**
   ```bash
   pip list | findstr /i "streamlit pandas numpy altair plotly openpyxl"
   ```
   
   Deve mostrar exatamente:
   - streamlit 1.50.0
   - pandas 2.3.3
   - numpy 2.3.5
   - altair 5.5.0
   - plotly 6.5.0
   - openpyxl 3.1.5

### Limpar e reinstalar
Se houver problemas, você pode recriar o ambiente:
```bash
# Remover o ambiente antigo
rmdir /s .venv  # Windows
# ou
rm -rf .venv    # Linux/Mac

# Recriar
python -m venv .venv
\.\.venv\Scripts\activate  # ou source .venv/bin/activate no Linux/Mac
pip install -r requirements.txt
```

