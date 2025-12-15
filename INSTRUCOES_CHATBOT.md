# 🤖 Instruções de Instalação - Chatbot Melhorado

## ✅ Melhorias Implementadas

O chatbot foi atualizado para usar **embeddings vetoriais** com **sentence-transformers** e **FAISS**, proporcionando:

- ✅ **Busca semântica** - Entende sinônimos e contexto
- ✅ **Melhor qualidade** - Respostas mais precisas e relevantes
- ✅ **Performance otimizada** - Cache de embeddings para respostas rápidas
- ✅ **100% local** - Sem APIs externas, tudo roda localmente

## 📦 Instalação das Dependências

### Opção 1: Instalação Completa (Recomendado)

```bash
pip install -r requirements.txt
```

Isso instalará todas as dependências, incluindo as novas:
- `sentence-transformers==2.2.2` - Para criar embeddings vetoriais
- `faiss-cpu==1.7.4` - Para busca rápida em vetores
- `torch==2.0.1` - Dependência do sentence-transformers
- `transformers==4.35.2` - Dependência do sentence-transformers

### Opção 2: Instalação Manual

Se preferir instalar apenas as novas dependências:

```bash
pip install sentence-transformers==2.2.2 faiss-cpu==1.7.4 torch==2.0.1 transformers==4.35.2
```

## ⚠️ Primeira Execução

Na **primeira vez** que o chatbot for usado:

1. O modelo de embeddings será baixado automaticamente (~420MB)
   - Modelo: `paraphrase-multilingual-MiniLM-L12-v2`
   - Suporta português e outros idiomas
   - Será salvo em cache local (não precisa baixar novamente)

2. Os embeddings da documentação serão criados (pode levar alguns minutos)
   - Serão salvos em cache na pasta `cache/`
   - Próximas execuções serão muito mais rápidas

## 🔄 Fallback Automático

Se as bibliotecas não estiverem instaladas, o chatbot **automaticamente** usará o método antigo (busca por palavras-chave). O sistema continuará funcionando, mas com qualidade reduzida.

## 📝 Notas Importantes

- **Espaço em disco**: O modelo de embeddings ocupa ~420MB
- **Memória RAM**: Recomendado pelo menos 4GB disponíveis
- **Cache**: Os embeddings são salvos em `cache/embeddings_*.pkl`
  - Se a documentação mudar, novos embeddings serão criados automaticamente
  - Você pode deletar arquivos de cache antigos se necessário

## 🚀 Uso

Após a instalação, o chatbot funcionará automaticamente com as melhorias. Não é necessário alterar nenhum código - a interface do Streamlit permanece a mesma.

## ❓ Solução de Problemas

### Erro ao instalar faiss-cpu

Se houver problemas com `faiss-cpu`, tente:

```bash
pip install faiss-cpu --no-cache-dir
```

### Erro ao baixar modelo

Se o download do modelo falhar, verifique sua conexão com a internet. O modelo só precisa ser baixado uma vez.

### Memória insuficiente

Se houver problemas de memória, você pode:
- Reduzir o tamanho dos segmentos no código (parâmetro `tamanho_segmento`)
- Usar o método antigo (removendo as dependências)

## 📊 Comparação de Performance

| Método | Qualidade | Velocidade | Semântica |
|--------|-----------|------------|-----------|
| **Antigo** (palavras-chave) | ⭐⭐ | ⚡⚡⚡ | ❌ |
| **Novo** (embeddings) | ⭐⭐⭐⭐⭐ | ⚡⚡ | ✅ |

