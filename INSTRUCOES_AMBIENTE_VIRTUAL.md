# Instruções para Usar o Ambiente Virtual

## Ambiente Virtual Criado

Foi criado um ambiente virtual Python na pasta `venv` com todas as dependências necessárias para o projeto.

## Como Usar no Outro PC

### Opção 1: Recriar o Ambiente Virtual (Recomendado)

Como ambientes virtuais não são portáveis entre diferentes sistemas operacionais ou arquiteturas, a melhor opção é recriar o ambiente virtual no outro PC:

1. **Copie o arquivo `requirements.txt`** para o outro PC

2. **Crie um novo ambiente virtual:**
   ```bash
   python -m venv venv
   ```

3. **Ative o ambiente virtual:**
   
   **No Windows (PowerShell):**
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```
   
   **No Windows (CMD):**
   ```cmd
   venv\Scripts\activate.bat
   ```
   
   **No Linux/Mac:**
   ```bash
   source venv/bin/activate
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

1. **Copiar a pasta `venv` inteira** para o outro PC (mantendo a mesma estrutura de pastas)

2. **Ativar o ambiente virtual:**
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```

3. **Executar a aplicação:**
   ```bash
   streamlit run app.py
   ```

⚠️ **Nota:** Esta opção pode não funcionar se os PCs tiverem versões diferentes do Python ou arquiteturas diferentes.

## Dependências Instaladas

O ambiente virtual contém:
- **streamlit==1.50.0** (versão específica para garantir compatibilidade com `st.expander`)
- pandas>=2.0.0
- numpy>=1.24.0
- altair>=5.0.0
- plotly>=5.17.0
- openpyxl>=3.1.0
- E todas as dependências transitivas necessárias

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

### Erro de versão do Streamlit
Certifique-se de que está usando exatamente a versão 1.50.0:
```bash
pip install streamlit==1.50.0
```

### Limpar e reinstalar
Se houver problemas, você pode recriar o ambiente:
```bash
# Remover o ambiente antigo
rmdir /s venv  # Windows
# ou
rm -rf venv    # Linux/Mac

# Recriar
python -m venv venv
.\venv\Scripts\activate  # ou source venv/bin/activate no Linux/Mac
pip install -r requirements.txt
```

