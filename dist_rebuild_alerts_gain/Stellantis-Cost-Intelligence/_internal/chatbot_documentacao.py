"""
Chatbot de Documentação do Stellantis Cost Intelligence (SCI)

Sistema de busca e resposta baseado na documentação completa do sistema.
Utiliza embeddings vetoriais (sentence-transformers) e FAISS para busca semântica local.
Não utiliza APIs externas - toda a busca é feita localmente.
"""

import os
import re
import pickle
import hashlib
from typing import List, Tuple, Dict, Optional
import numpy as np

# Importações opcionais - com fallback para método antigo se não disponível
try:
    from sentence_transformers import SentenceTransformer
    import faiss
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    faiss = None  # Definir como None se não disponível para evitar NameError
    print("⚠️ Aviso: sentence-transformers ou faiss não disponíveis. Usando método de busca simples.")


# Cache global para modelo e embeddings
_modelo_embedding = None
_embeddings_cache = {}
_indice_faiss = None
_segmentos_cache = []


def carregar_documentacao() -> str:
    """
    Carrega todo o conteúdo da documentação do sistema.
    Prioriza a documentação completa sobre a apresentação resumida.
    
    Returns:
        str: Conteúdo completo da documentação
    """
    base_path = os.path.dirname(os.path.abspath(__file__))

    # Fontes de documentação (priorizar especificação técnica e docs completas;
    # NÃO incluir apresentações curtas)
    fontes = [
        ("python", os.path.join(base_path, "pages", "6 - Documentacao.py")),
        ("md", os.path.join(base_path, "DOCUMENTACAO_SISTEMA_TC.md")),
        ("md", os.path.join(base_path, "DOCUMENTACAO_FLEX_BUD_ANO_COMPLETO.md")),
        ("md", os.path.join(base_path, "INSTRUCOES_AMBIENTE_VIRTUAL.md")),
        ("md", os.path.join(base_path, "INSTRUCOES_CHATBOT.md")),
        ("md", os.path.join(base_path, "SELECIONAR_AMBIENTE_VIRTUAL.md")),
        ("md", os.path.join(base_path, "INSTRUCOES_SINCRONIZACAO.md")),
    ]

    conteudos: List[str] = []

    for tipo, caminho in fontes:
        if not os.path.exists(caminho):
            continue

        try:
            with open(caminho, "r", encoding="utf-8") as f:
                bruto = f.read()
        except Exception as e:
            print(f"Erro ao carregar documentação ({caminho}): {e}")
            continue

        if tipo == "python":
            extraido = extrair_texto_documentacao(bruto)
            if extraido:
                conteudos.append(extraido)
        else:
            # Markdown: usar conteúdo integral
            conteudos.append(bruto)

    return "\n\n".join([c for c in conteudos if c and c.strip()])


def extrair_texto_documentacao(codigo_python: str) -> str:
    """
    Extrai texto de documentação de um arquivo Python.
    Remove código e mantém TODAS as strings markdown e comentários.
    Preserva o máximo de conteúdo possível da documentação.
    """
    texto_extraido = []
    
    # MÉTODO 1: Buscar TODAS as strings markdown com triple quotes (mais completo)
    # Padrão mais abrangente que captura strings mesmo com quebras de linha complexas
    padroes_markdown = [
        r'st\.markdown\(["\']{3}(.*?)["\']{3}',  # st.markdown("""...""")
        r'st\.markdown\(f["\']{3}(.*?)["\']{3}',  # st.markdown(f"""...""")
        r'st\.markdown\(r["\']{3}(.*?)["\']{3}',  # st.markdown(r"""...""")
    ]
    
    for padrao in padroes_markdown:
        matches = re.finditer(padrao, codigo_python, re.DOTALL)
        for match in matches:
            texto = match.group(1)
            # Preservar estrutura markdown (títulos, listas, etc)
            # Remover apenas tags de estilo CSS
            texto = re.sub(r'<style[^>]*>.*?</style>', '', texto, flags=re.DOTALL)
            # Extrair texto de dentro de tags HTML mas preservar estrutura
            # Ex: <h2>Título</h2> -> Título (mas manter quebra de linha)
            texto = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n\1\n\n', texto, flags=re.DOTALL | re.IGNORECASE)
            texto = re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n\n', texto, flags=re.DOTALL | re.IGNORECASE)
            texto = re.sub(r'<div[^>]*>(.*?)</div>', r'\1', texto, flags=re.DOTALL | re.IGNORECASE)
            # Remover outras tags mas preservar conteúdo
            texto = re.sub(r'<[^>]+>', ' ', texto)
            # Remover atributos HTML soltos
            texto = re.sub(r'\bstyle\s*=\s*[^,\s\)]+', '', texto, flags=re.IGNORECASE)
            texto = re.sub(r'\bunsafe_allow_html\s*=\s*(True|False)', '', texto, flags=re.IGNORECASE)
            # Remover código Python que possa ter sido capturado
            texto = re.sub(r'st\.\w+\s*\([^)]*\)', '', texto)
            texto = re.sub(r'with\s+st\.\w+', '', texto)
            texto = re.sub(r'elif\s+[^:]+:', '', texto)
            # Limpar espaços múltiplos mas preservar quebras de linha
            texto = re.sub(r'[ \t]+', ' ', texto)  # Espaços e tabs
            texto = re.sub(r' \n', '\n', texto)  # Espaço antes de quebra
            texto = re.sub(r'\n{4,}', '\n\n\n', texto)  # Máximo 3 quebras consecutivas
            # Remover linhas que são claramente código Python ou HTML mal formatado
            linhas = texto.split('\n')
            linhas_limpas = []
            for l in linhas:
                l_stripped = l.strip()
                # Pular linhas que são código ou HTML mal formatado
                if (l_stripped.startswith('st.') or
                    l_stripped.startswith('with ') or
                    l_stripped.startswith('elif ') or
                    l_stripped.startswith('# =') or
                    'unsafe_allow_html' in l_stripped.lower() or
                    l_stripped.startswith('<') or  # HTML mal formatado
                    (l_stripped.startswith('#') and ('CAPÍTULO' in l.upper() or 'SEÇÃO' in l.upper())) or
                    len(l_stripped) < 3):  # Linhas muito curtas
                    continue
                linhas_limpas.append(l)
            texto = '\n'.join(linhas_limpas)
            if len(texto.strip()) > 15:  # Textos significativos
                texto_extraido.append(texto.strip())
    
    # MÉTODO 2: Buscar strings markdown com unsafe_allow_html (extrair conteúdo completo)
    # Padrão mais específico para capturar strings com unsafe_allow_html
    padrao_unsafe = r'st\.markdown\(([^,)]+),\s*unsafe_allow_html\s*=\s*True'
    matches = re.finditer(padrao_unsafe, codigo_python, re.DOTALL)
    for match in matches:
        conteudo = match.group(1)
        # Extrair strings triple quotes do conteúdo
        strings_triple = re.findall(r'["\']{3}(.*?)["\']{3}', conteudo, re.DOTALL)
        for texto in strings_triple:
            # Processar texto HTML - extrair conteúdo de texto
            texto = re.sub(r'<style[^>]*>.*?</style>', '', texto, flags=re.DOTALL)
            # Extrair texto de dentro de tags (ex: <h2>Texto</h2> -> Texto)
            texto = re.sub(r'<([^>]+)>([^<]+)</\1>', r'\2', texto)  # Tags com fechamento
            texto = re.sub(r'<[^>]+>', ' ', texto)  # Outras tags
            texto = re.sub(r'[ \t]+', ' ', texto)
            texto = re.sub(r'\n{3,}', '\n\n', texto)
            if len(texto.strip()) > 15:
                texto_extraido.append(texto.strip())
    
    # MÉTODO 3: Buscar strings em outras funções Streamlit (st.info, st.warning, etc)
    padrao_st_funcoes = r'st\.(markdown|info|warning|success|error|subheader|header|title)\(["\']([^"\']{20,})["\']'
    matches = re.finditer(padrao_st_funcoes, codigo_python)
    for match in matches:
        texto = match.group(2)
        if len(texto.strip()) > 15:
            texto_extraido.append(texto.strip())
    
    # MÉTODO 4: Buscar strings longas em qualquer contexto (pode ser documentação)
    # Padrão para capturar strings longas que podem conter documentação
    padrao_strings_longas = r'["\']([^"\']{50,})["\']'
    matches = re.finditer(padrao_strings_longas, codigo_python)
    palavras_relevantes = ['sistema', 'funcionalidade', 'cálculo', 'processo', 'dados', 'análise', 
                           'best estimate', 'waterfall', 'forecast', 'flex bud', 'sensibilidade',
                           'média', 'histórico', 'previsão', 'documentação', 'extração', 'versão',
                           'arquitetura', 'estrutura', 'página', 'funcionalidades', 'notebook',
                           'merge', 'consolidação', 'parquet', 'excel', 'oficina', 'veículo']
    for match in matches:
        texto = match.group(1)
        # Filtrar apenas strings que parecem documentação
        if any(palavra in texto.lower() for palavra in palavras_relevantes):
            # Limpar código Python que possa estar na string
            texto = re.sub(r'```python.*?```', '', texto, flags=re.DOTALL)
            if len(texto.strip()) > 20:
                texto_extraido.append(texto.strip())
    
    # MÉTODO 5: Buscar comentários de documentação (seções, títulos, explicações)
    # REMOVIDO - estava capturando código Python junto com comentários
    # Os comentários importantes já são capturados nos métodos anteriores
    
    # Remover duplicatas mantendo ordem e preservando textos mais longos
    texto_extraido_unico = []
    textos_vistos = set()
    for texto in texto_extraido:
        # Usar hash do texto para detectar duplicatas exatas
        texto_hash = hash(texto.strip()[:200])  # Primeiros 200 chars para hash
        if texto_hash not in textos_vistos and len(texto.strip()) > 15:
            textos_vistos.add(texto_hash)
            texto_extraido_unico.append(texto.strip())
    
    return "\n\n".join(texto_extraido_unico)


def dividir_em_segmentos(texto: str, tamanho_segmento: int = 700) -> List[str]:
    """
    Divide o texto em segmentos menores para busca.
    
    Args:
        texto: Texto completo
        tamanho_segmento: Tamanho aproximado de cada segmento
    
    Returns:
        List[str]: Lista de segmentos
    """
    # Dividir por parágrafos primeiro
    paragrafos = texto.split('\n\n')
    segmentos = []
    segmento_atual = []
    tamanho_atual = 0
    
    for paragrafo in paragrafos:
        tamanho_paragrafo = len(paragrafo)
        
        if tamanho_atual + tamanho_paragrafo > tamanho_segmento and segmento_atual:
            segmentos.append('\n\n'.join(segmento_atual))
            segmento_atual = [paragrafo]
            tamanho_atual = tamanho_paragrafo
        else:
            segmento_atual.append(paragrafo)
            tamanho_atual += tamanho_paragrafo
    
    if segmento_atual:
        segmentos.append('\n\n'.join(segmento_atual))
    
    return segmentos


def obter_modelo_embedding():
    """
    Obtém ou carrega o modelo de embeddings.
    Usa cache global para não recarregar o modelo a cada chamada.
    
    Returns:
        SentenceTransformer: Modelo de embeddings
    """
    global _modelo_embedding
    
    if not EMBEDDINGS_AVAILABLE:
        return None
    
    if _modelo_embedding is None:
        try:
            # Modelo multilingual que suporta português
            # Primeira execução baixa o modelo (~420MB), depois usa cache local
            _modelo_embedding = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        except Exception as e:
            print(f"Erro ao carregar modelo de embeddings: {e}")
            return None
    
    return _modelo_embedding


def criar_embeddings_documentacao(documentacao: str) -> Tuple[List[str], Optional[np.ndarray], Optional[any]]:
    """
    Cria embeddings vetoriais para a documentação.
    Usa cache para não recalcular se a documentação não mudou.
    
    Args:
        documentacao: Texto completo da documentação
    
    Returns:
        Tuple[List[str], Optional[np.ndarray], Optional[any]]: 
            (segmentos, embeddings, indice_faiss)
    """
    global _segmentos_cache, _indice_faiss
    
    if not EMBEDDINGS_AVAILABLE:
        # Fallback: retornar segmentos sem embeddings
        segmentos = dividir_em_segmentos(documentacao)
        return segmentos, None, None
    
    # Verificar cache baseado no hash da documentação
    doc_hash = hashlib.md5(documentacao.encode('utf-8')).hexdigest()
    cache_file = os.path.join(os.path.dirname(__file__), 'cache', f'embeddings_{doc_hash}.pkl')
    
    # Tentar carregar do cache
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
                segmentos = cache_data['segmentos']
                embeddings = cache_data['embeddings']
                print(f"✅ Embeddings carregados do cache ({len(segmentos)} segmentos)")
                
                # Criar índice FAISS
                if faiss is not None:
                    dimensao = embeddings.shape[1]
                    indice = faiss.IndexFlatL2(dimensao)
                    indice.add(embeddings.astype('float32'))
                else:
                    indice = None
                
                return segmentos, embeddings, indice
        except Exception as e:
            print(f"⚠️ Erro ao carregar cache: {e}. Recriando embeddings...")
    
    # Criar embeddings se não houver cache
    modelo = obter_modelo_embedding()
    if modelo is None:
        segmentos = dividir_em_segmentos(documentacao)
        return segmentos, None, None
    
    print("🔄 Criando embeddings vetoriais (isso pode levar alguns minutos na primeira vez)...")
    segmentos = dividir_em_segmentos(documentacao)
    
    # Criar embeddings em lotes para melhor performance
    batch_size = 32
    embeddings_list = []
    
    for i in range(0, len(segmentos), batch_size):
        batch = segmentos[i:i + batch_size]
        batch_embeddings = modelo.encode(batch, show_progress_bar=False)
        embeddings_list.append(batch_embeddings)
    
    embeddings = np.vstack(embeddings_list)
    
    # Criar índice FAISS para busca rápida
    if faiss is not None:
        dimensao = embeddings.shape[1]
        indice = faiss.IndexFlatL2(dimensao)
        indice.add(embeddings.astype('float32'))
    else:
        indice = None
    
    # Salvar no cache
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'wb') as f:
            pickle.dump({
                'segmentos': segmentos,
                'embeddings': embeddings,
                'doc_hash': doc_hash
            }, f)
        print(f"✅ Embeddings salvos no cache ({len(segmentos)} segmentos)")
    except Exception as e:
        print(f"⚠️ Erro ao salvar cache: {e}")
    
    return segmentos, embeddings, indice


def calcular_similaridade_vetorial(pergunta: str, segmentos: List[str], 
                                   embeddings: np.ndarray, indice_faiss: any,
                                   top_n: int = 15) -> List[Tuple[str, float]]:
    """
    Calcula similaridade usando embeddings vetoriais e FAISS.
    
    Args:
        pergunta: Pergunta do usuário
        segmentos: Lista de segmentos da documentação
        embeddings: Array numpy com embeddings dos segmentos
        indice_faiss: Índice FAISS para busca rápida
        top_n: Número de resultados a retornar
    
    Returns:
        List[Tuple[str, float]]: Lista de (segmento, score) ordenada por relevância
    """
    modelo = obter_modelo_embedding()
    if modelo is None:
        return []
    
    # Criar embedding da pergunta
    query_embedding = modelo.encode([pergunta])
    
    # Buscar no índice FAISS - aumentar k para ter mais opções
    k = min(top_n * 2, len(segmentos))  # Buscar mais resultados para ter variedade
    distances, indices = indice_faiss.search(query_embedding.astype('float32'), k)
    
    # Converter distâncias em scores de similaridade (0-1)
    # FAISS retorna distâncias L2, menores = mais similar
    # Usar normalização que garante scores entre 0 e 1
    if len(distances[0]) > 0:
        min_distance = distances[0].min()
        max_distance = distances[0].max()
        if max_distance > min_distance:
            # Normalizar entre 0 e 1, invertendo (menor distância = maior score)
            # Usar função sigmoide suave para garantir que scores fiquem entre 0 e 1
            normalized = (distances[0] - min_distance) / (max_distance - min_distance + 1e-6)
            scores = 1.0 - normalized
            # Garantir que scores estejam entre 0 e 1
            scores = np.clip(scores, 0.0, 1.0)
        else:
            scores = np.ones(len(distances[0])) * 0.9  # Score padrão alto mas não 100%
    else:
        scores = np.array([])
    
    # Retornar segmentos com scores, garantindo variedade
    resultados = []
    segmentos_vistos = set()
    
    # Ordenar por score (maior primeiro) para garantir melhor qualidade
    indices_scores = list(zip(indices[0], scores))
    indices_scores.sort(key=lambda x: x[1], reverse=True)
    
    for idx, score in indices_scores:
        if idx < len(segmentos):
            segmento = segmentos[idx]
            # Limpar código Python antes de adicionar
            segmento_limpo = limpar_codigo_python(segmento)
            
            # Usar hash dos primeiros 200 chars para detectar duplicatas
            segmento_hash = hash(segmento_limpo[:200])
            if segmento_hash not in segmentos_vistos and len(segmento_limpo.strip()) > 50:
                segmentos_vistos.add(segmento_hash)
                resultados.append((segmento_limpo, float(score)))
                if len(resultados) >= top_n:
                    break
    
    return resultados


def calcular_similaridade(texto1: str, texto2: str) -> float:
    """
    Calcula similaridade entre dois textos usando SequenceMatcher (fallback).
    
    Args:
        texto1: Primeiro texto
        texto2: Segundo texto
    
    Returns:
        float: Similaridade entre 0 e 1
    """
    from difflib import SequenceMatcher
    return SequenceMatcher(None, texto1.lower(), texto2.lower()).ratio()


def buscar_palavras_chave(texto: str, palavras: List[str]) -> int:
    """
    Conta quantas palavras-chave aparecem no texto.
    
    Args:
        texto: Texto para buscar
        palavras: Lista de palavras-chave
    
    Returns:
        int: Número de palavras encontradas
    """
    texto_lower = texto.lower()
    encontradas = sum(1 for palavra in palavras if palavra.lower() in texto_lower)
    return encontradas


def classificar_tipo_segmento(segmento: str) -> str:
    """
    Classifica um segmento como 'tecnico' ou 'teorico' baseado no conteúdo.
    
    Args:
        segmento: Texto do segmento
    
    Returns:
        str: 'tecnico' ou 'teorico'
    """
    segmento_lower = segmento.lower()
    
    # Palavras-chave técnicas (programação, implementação, estrutura)
    palavras_tecnicas = [
        'python', 'streamlit', 'pandas', 'parquet', 'notebook', 'arquivo', 'pasta',
        'código', 'função', 'classe', 'método', 'import', 'processamento', 'upload',
        'estrutura', 'organização', 'sistema', 'página', 'interface', 'dashboard',
        'execução', 'carregamento', 'salvamento', 'validação', 'erro', 'tratamento',
        'merge', 'dataframe', 'coluna', 'linha', 'filtro', 'busca', 'caminho',
        'diretório', 'extensão', 'formato', 'encoding', 'utf-8', 'json', 'excel'
    ]
    
    # Palavras-chave teóricas (cálculos, fórmulas, conceitos, teoria)
    palavras_teoricas = [
        'cálculo', 'fórmula', 'média', 'histórico', 'previsão', 'forecast',
        'best estimate', 'sensibilidade', 'inflação', 'volume', 'proporção',
        'teoria', 'conceito', 'fundamento', 'princípio', 'metodologia',
        'fixo', 'variável', 'ajuste', 'fator', 'multiplicador', 'rateio',
        'comparação', 'análise', 'tendência', 'padrão', 'comportamento',
        'matemática', 'estatística', 'projeção', 'estimativa', 'acurácia'
    ]
    
    # Contar ocorrências
    count_tecnico = sum(1 for palavra in palavras_tecnicas if palavra in segmento_lower)
    count_teorico = sum(1 for palavra in palavras_teoricas if palavra in segmento_lower)
    
    # Classificar baseado na maioria
    if count_teorico > count_tecnico:
        return 'teorico'
    elif count_tecnico > count_teorico:
        return 'tecnico'
    else:
        # Em caso de empate, verificar palavras-chave mais específicas
        if any(palavra in segmento_lower for palavra in ['fórmula', 'cálculo', 'média', 'best estimate']):
            return 'teorico'
        else:
            return 'tecnico'


def buscar_resposta(pergunta: str, documentacao: str, top_n: int = 15) -> List[Tuple[str, float, str]]:
    """
    Busca as melhores respostas na documentação baseado na pergunta.
    Usa embeddings vetoriais se disponível, senão usa método tradicional.
    
    Args:
        pergunta: Pergunta do usuário
        documentacao: Texto completo da documentação
        top_n: Número de respostas a retornar
    
    Returns:
        List[Tuple[str, float, str]]: Lista de (resposta, score, tipo) ordenada por relevância
        onde tipo é 'tecnico' ou 'teorico'
    """
    # Criar embeddings e índice
    segmentos, embeddings, indice_faiss = criar_embeddings_documentacao(documentacao)
    
    # Usar busca vetorial se disponível
    if embeddings is not None and indice_faiss is not None:
        resultados_vetoriais = calcular_similaridade_vetorial(
            pergunta, segmentos, embeddings, indice_faiss, top_n=top_n
        )
        
        # Classificar tipo de cada resultado
        resultados_com_tipo = []
        for segmento, score in resultados_vetoriais:
            tipo = classificar_tipo_segmento(segmento)
            resultados_com_tipo.append((segmento, score, tipo))
        
        return resultados_com_tipo
    
    # Fallback: método tradicional baseado em palavras-chave e similaridade
    palavras_pergunta = re.findall(r'\b\w+\b', pergunta.lower())
    palavras_comuns = {'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                      'um', 'uma', 'uns', 'umas', 'é', 'são', 'como', 'que', 'qual', 'quais', 'para', 'por', 'com'}
    palavras_chave = [p for p in palavras_pergunta if p not in palavras_comuns and len(p) > 2]
    
    scores = []
    for segmento in segmentos:
        # Limpar código Python do segmento
        segmento_limpo = limpar_codigo_python(segmento)
        
        # Pular segmentos muito pequenos após limpeza
        if len(segmento_limpo.strip()) < 50:
            continue
        
        similaridade = calcular_similaridade(pergunta, segmento_limpo)
        palavras_encontradas = buscar_palavras_chave(segmento_limpo, palavras_chave)
        score_palavras = palavras_encontradas / max(len(palavras_chave), 1) if palavras_chave else 0
        
        if palavras_chave and palavras_encontradas == len(palavras_chave):
            score_palavras *= 1.5
        elif palavras_chave and palavras_encontradas >= len(palavras_chave) * 0.7:
            score_palavras *= 1.2
        
        score_final = (similaridade * 0.3) + (score_palavras * 0.7)
        
        if len(segmento_limpo) > 1500:
            score_final *= 0.95
        elif 300 <= len(segmento_limpo) <= 1000:
            score_final *= 1.05
        
        tipo_segmento = classificar_tipo_segmento(segmento_limpo)
        scores.append((segmento_limpo, score_final, tipo_segmento))
    
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_n]


def extrair_trecho_relevante(segmento: str, palavras_chave: List[str], max_caracteres: int = 5000) -> str:
    """
    Extrai o trecho mais relevante de um segmento baseado nas palavras-chave da pergunta.
    Inclui contexto adjacente extenso para respostas mais completas e bem contextualizadas.
    
    Args:
        segmento: Texto completo do segmento
        palavras_chave: Lista de palavras-chave da pergunta
        max_caracteres: Tamanho máximo do trecho
    
    Returns:
        str: Trecho mais relevante com contexto amplo
    """
    if not palavras_chave:
        if len(segmento) <= max_caracteres:
            return segmento
        corte = segmento[:max_caracteres]
        ultimo_paragrafo = corte.rfind('\n\n')
        if ultimo_paragrafo > max_caracteres * 0.7:
            return segmento[:ultimo_paragrafo]
        return segmento[:max_caracteres]
    
    paragrafos = segmento.split('\n\n')
    paragrafos_com_indice = [(i, p) for i, p in enumerate(paragrafos)]
    paragrafos_relevantes = []
    
    for idx, paragrafo in paragrafos_com_indice:
        paragrafo_lower = paragrafo.lower()
        palavras_no_paragrafo = sum(1 for palavra in palavras_chave if palavra.lower() in paragrafo_lower)
        if palavras_no_paragrafo > 0:
            paragrafos_relevantes.append((idx, paragrafo, palavras_no_paragrafo))
    
    if paragrafos_relevantes:
        paragrafos_relevantes.sort(key=lambda x: x[2], reverse=True)
        
        indices_relevantes = set()
        for idx, _, _ in paragrafos_relevantes[:5]:
            indices_relevantes.add(idx)
            for offset in range(-2, 3):
                idx_adjacente = idx + offset
                if 0 <= idx_adjacente < len(paragrafos):
                    indices_relevantes.add(idx_adjacente)
        
        for idx, _, _ in paragrafos_relevantes[5:10]:
            if len(indices_relevantes) * 400 < max_caracteres:
                indices_relevantes.add(idx)
                for offset in range(-1, 2):
                    idx_adjacente = idx + offset
                    if 0 <= idx_adjacente < len(paragrafos):
                        indices_relevantes.add(idx_adjacente)
        
        indices_ordenados = sorted(indices_relevantes)
        
        trecho = []
        tamanho_atual = 0
        for idx in indices_ordenados:
            paragrafo = paragrafos[idx]
            if tamanho_atual + len(paragrafo) <= max_caracteres:
                trecho.append(paragrafo)
                tamanho_atual += len(paragrafo) + 2
            else:
                espaco_restante = max_caracteres - tamanho_atual
                if espaco_restante > 200:
                    paragrafo_cortado = paragrafo[:espaco_restante]
                    ultimo_ponto = paragrafo_cortado.rfind('.')
                    if ultimo_ponto > espaco_restante * 0.6:
                        trecho.append(paragrafo[:ultimo_ponto + 1])
                    else:
                        ultima_virgula = paragrafo_cortado.rfind(',')
                        if ultima_virgula > espaco_restante * 0.5:
                            trecho.append(paragrafo[:ultima_virgula + 1] + "...")
                        else:
                            trecho.append(paragrafo_cortado + "...")
                break
        
        if trecho:
            return '\n\n'.join(trecho)
    
    if len(segmento) <= max_caracteres:
        return segmento
    
    corte = segmento[:max_caracteres]
    ultimo_paragrafo = corte.rfind('\n\n')
    if ultimo_paragrafo > max_caracteres * 0.5:
        return segmento[:ultimo_paragrafo]
    
    return segmento[:max_caracteres]


def limpar_codigo_python(texto: str) -> str:
    """
    Remove código Python e HTML que possa ter sido capturado acidentalmente.
    
    Args:
        texto: Texto que pode conter código Python e HTML
    
    Returns:
        str: Texto limpo sem código Python e HTML
    """
    # Remover blocos de código markdown
    texto = re.sub(r'```python.*?```', '', texto, flags=re.DOTALL)
    texto = re.sub(r'```.*?```', '', texto, flags=re.DOTALL)
    
    # Remover HTML mal formatado e tags HTML
    texto = re.sub(r'<style[^>]*>.*?</style>', '', texto, flags=re.DOTALL | re.IGNORECASE)
    texto = re.sub(r'<h[1-6][^>]*>', '', texto, flags=re.IGNORECASE)  # Remover tags de abertura
    texto = re.sub(r'</h[1-6]>', '', texto, flags=re.IGNORECASE)  # Remover tags de fechamento
    texto = re.sub(r'<p[^>]*>', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'</p>', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'<div[^>]*>', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'</div>', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'<[^>]+>', '', texto)  # Remover qualquer tag HTML restante
    
    # Remover atributos HTML soltos (style=, class=, etc)
    texto = re.sub(r'\bstyle\s*=\s*[^,\s\)]+', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'\bclass\s*=\s*[^,\s\)]+', '', texto, flags=re.IGNORECASE)
    texto = re.sub(r'\bunsafe_allow_html\s*=\s*(True|False)', '', texto, flags=re.IGNORECASE)
    
    # Remover chamadas de funções Streamlit que aparecem como texto
    texto = re.sub(r'st\.\w+\([^)]*\)', '', texto)
    texto = re.sub(r'st\.\w+\s*\(', '', texto)
    texto = re.sub(r'elif\s+[^:]+:', '', texto)
    texto = re.sub(r'if\s+[^:]+:', '', texto)
    texto = re.sub(r'else:', '', texto)
    texto = re.sub(r'try:', '', texto)
    texto = re.sub(r'except\s+[^:]+:', '', texto)
    
    # Remover padrões comuns de código Python que aparecem como texto
    linhas = texto.split('\n')
    linhas_limpas = []
    
    for linha in linhas:
        linha_limpa = linha.strip()
        
        # Pular linhas que são claramente código Python ou HTML mal formatado
        if (linha_limpa.startswith('st.') or 
            linha_limpa.startswith('with ') or
            linha_limpa.startswith('elif ') or
            linha_limpa.startswith('if ') or
            linha_limpa.startswith('else:') or
            linha_limpa.startswith('try:') or
            linha_limpa.startswith('except ') or
            (linha_limpa.startswith('for ') and ':' in linha_limpa) or
            linha_limpa.startswith('def ') or
            linha_limpa.startswith('import ') or
            linha_limpa.startswith('from ') or
            linha_limpa.startswith('# =') or
            linha_limpa.startswith('base_path') or
            linha_limpa.startswith('sys.path') or
            linha_limpa.startswith('os.path') or
            'unsafe_allow_html' in linha_limpa.lower() or
            linha_limpa.startswith('<') or  # Linhas que começam com HTML
            (linha_limpa.startswith('#') and 'CAPÍTULO' in linha_limpa.upper()) or
            (linha_limpa.startswith('#') and 'SEÇÃO' in linha_limpa.upper())):
            continue
        
        # Limpar código Python que aparece no meio do texto
        linha_limpa = re.sub(r'\bst\.\w+\s*\([^)]*\)', '', linha_limpa)
        linha_limpa = re.sub(r'\bwith\s+st\.\w+', '', linha_limpa)
        linha_limpa = re.sub(r'\bif\s+[^:]+:\s*$', '', linha_limpa)
        linha_limpa = re.sub(r'\belif\s+[^:]+:\s*$', '', linha_limpa)
        
        # Remover parênteses e vírgulas soltas que são resquícios de código
        linha_limpa = re.sub(r'^\s*[\)\,]\s*$', '', linha_limpa)
        linha_limpa = re.sub(r'^\s*\(\s*$', '', linha_limpa)
        
        # Remover atributos HTML soltos
        linha_limpa = re.sub(r'\bstyle\s*=\s*[^,\s\)]+', '', linha_limpa, flags=re.IGNORECASE)
        linha_limpa = re.sub(r'\bunsafe_allow_html\s*=\s*(True|False)', '', linha_limpa, flags=re.IGNORECASE)
        
        # Remover tags HTML restantes
        linha_limpa = re.sub(r'<[^>]+>', '', linha_limpa)
        
        if linha_limpa.strip() and len(linha_limpa.strip()) > 3:  # Ignorar linhas muito curtas
            linhas_limpas.append(linha_limpa)
    
    texto_limpo = '\n'.join(linhas_limpas)
    
    # Remover múltiplas quebras de linha
    texto_limpo = re.sub(r'\n{3,}', '\n\n', texto_limpo)
    
    # Remover espaços múltiplos
    texto_limpo = re.sub(r' {2,}', ' ', texto_limpo)
    
    # Remover linhas que são apenas pontuação ou símbolos
    linhas_finais = []
    for linha in texto_limpo.split('\n'):
        linha_stripped = linha.strip()
        # Manter apenas linhas com conteúdo real (mais de 3 caracteres e não só símbolos)
        if len(linha_stripped) > 3 and not re.match(r'^[^\w\s]+$', linha_stripped):
            linhas_finais.append(linha)
    
    texto_limpo = '\n'.join(linhas_finais)
    
    return texto_limpo.strip()


def formatar_resposta(segmento: str, max_caracteres: int = 2000, palavras_chave: List[str] = None) -> str:
    """
    Formata um segmento de resposta para exibição, focando nas partes mais relevantes.
    
    Args:
        segmento: Texto do segmento
        max_caracteres: Número máximo de caracteres
        palavras_chave: Palavras-chave da pergunta para extrair trecho relevante
    
    Returns:
        str: Resposta formatada
    """
    # Limpar código Python primeiro
    segmento = limpar_codigo_python(segmento)
    
    if palavras_chave:
        segmento = extrair_trecho_relevante(segmento, palavras_chave, max_caracteres)
    
    if len(segmento) > max_caracteres:
        corte = segmento[:max_caracteres]
        ultimo_paragrafo = corte.rfind('\n\n')
        if ultimo_paragrafo > max_caracteres * 0.6:
            segmento = segmento[:ultimo_paragrafo]
        else:
            ultimo_ponto = corte.rfind('.')
            if ultimo_ponto > max_caracteres * 0.7:
                segmento = segmento[:ultimo_ponto + 1]
            else:
                segmento = segmento[:max_caracteres] + "..."
    
    # Limpar novamente após extração para garantir
    segmento = limpar_codigo_python(segmento)
    
    return segmento.strip()


def responder_pergunta(pergunta: str) -> Dict[str, any]:
    """
    Responde uma pergunta baseada na documentação.
    
    Args:
        pergunta: Pergunta do usuário
    
    Returns:
        Dict com 'resposta', 'score' e 'segmentos_encontrados'
    """
    documentacao = carregar_documentacao()
    
    if not documentacao:
        return {
            'resposta': 'Desculpe, não foi possível carregar a documentação.',
            'score': 0.0,
            'segmentos_encontrados': []
        }
    
    resultados = buscar_resposta(pergunta, documentacao, top_n=15)
    
    if not resultados or resultados[0][1] < 0.12:
        return {
            'resposta': 'Desculpe, não encontrei informações relevantes na documentação para sua pergunta. Tente reformular ou usar palavras-chave diferentes.',
            'score': 0.0,
            'segmentos_encontrados': []
        }
    
    palavras_pergunta = re.findall(r'\b\w+\b', pergunta.lower())
    palavras_comuns = {'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                      'um', 'uma', 'uns', 'umas', 'é', 'são', 'como', 'que', 'qual', 'quais', 'para', 'por', 'com'}
    palavras_chave = [p for p in palavras_pergunta if p not in palavras_comuns and len(p) > 2]
    
    threshold_minimo = 0.12
    
    resultados_tecnicos = [(seg, score) for seg, score, tipo in resultados 
                          if tipo == 'tecnico' and score >= threshold_minimo]
    resultados_teoricos = [(seg, score) for seg, score, tipo in resultados 
                          if tipo == 'teorico' and score >= threshold_minimo]
    
    resposta_tecnica = None
    resposta_teorica = None
    
    if resultados_tecnicos:
        melhor_tecnico, score_tecnico = resultados_tecnicos[0]
        if palavras_chave:
            palavras_no_segmento = sum(1 for p in palavras_chave if p.lower() in melhor_tecnico.lower())
            if palavras_no_segmento >= max(1, len(palavras_chave) * 0.25):
                resposta_tecnica = formatar_resposta(melhor_tecnico, max_caracteres=4500, palavras_chave=palavras_chave)
        else:
            resposta_tecnica = formatar_resposta(melhor_tecnico, max_caracteres=4500, palavras_chave=palavras_chave)
    
    if resultados_teoricos:
        melhor_teorico, score_teorico = resultados_teoricos[0]
        if palavras_chave:
            palavras_no_segmento = sum(1 for p in palavras_chave if p.lower() in melhor_teorico.lower())
            if palavras_no_segmento >= max(1, len(palavras_chave) * 0.25):
                resposta_teorica = formatar_resposta(melhor_teorico, max_caracteres=3500, palavras_chave=palavras_chave)
        else:
            resposta_teorica = formatar_resposta(melhor_teorico, max_caracteres=3500, palavras_chave=palavras_chave)
    
    respostas_combinadas = []
    if resposta_tecnica:
        respostas_combinadas.append(f"**🔧 Resposta Técnica/Implementação:**\n\n{resposta_tecnica}")
    if resposta_teorica:
        respostas_combinadas.append(f"**📚 Resposta Teórica/Cálculos:**\n\n{resposta_teorica}")
    
    if not resposta_tecnica and not resposta_teorica:
        melhor_segmento, melhor_score = resultados[0][0], resultados[0][1]
        if melhor_score >= threshold_minimo:
            resposta_formatada = formatar_resposta(melhor_segmento, max_caracteres=3000, palavras_chave=palavras_chave)
            respostas_combinadas = [resposta_formatada]
    elif not resposta_tecnica:
        for seg, score, tipo in resultados:
            if tipo == 'tecnico' and score >= threshold_minimo * 0.7:
                if palavras_chave:
                    palavras_no_segmento = sum(1 for p in palavras_chave if p.lower() in seg.lower())
                    if palavras_no_segmento >= max(1, len(palavras_chave) * 0.2):
                        resposta_tecnica_alt = formatar_resposta(seg, max_caracteres=4500, palavras_chave=palavras_chave)
                        respostas_combinadas.insert(0, f"**🔧 Resposta Técnica/Implementação:**\n\n{resposta_tecnica_alt}")
                        break
                else:
                    resposta_tecnica_alt = formatar_resposta(seg, max_caracteres=4500, palavras_chave=palavras_chave)
                    respostas_combinadas.insert(0, f"**🔧 Resposta Técnica/Implementação:**\n\n{resposta_tecnica_alt}")
                    break
    elif not resposta_teorica:
        for seg, score, tipo in resultados:
            if tipo == 'teorico' and score >= threshold_minimo * 0.7:
                if palavras_chave:
                    palavras_no_segmento = sum(1 for p in palavras_chave if p.lower() in seg.lower())
                    if palavras_no_segmento >= max(1, len(palavras_chave) * 0.2):
                        resposta_teorica_alt = formatar_resposta(seg, max_caracteres=3500, palavras_chave=palavras_chave)
                        respostas_combinadas.append(f"**📚 Resposta Teórica/Cálculos:**\n\n{resposta_teorica_alt}")
                        break
                else:
                    resposta_teorica_alt = formatar_resposta(seg, max_caracteres=3500, palavras_chave=palavras_chave)
                    respostas_combinadas.append(f"**📚 Resposta Teórica/Cálculos:**\n\n{resposta_teorica_alt}")
                    break
    
    resposta_formatada = "\n\n---\n\n".join(respostas_combinadas)
    
    # Melhor score para retorno (garantir que não ultrapasse 100%)
    melhor_score = max([r[1] for r in resultados[:2]]) if len(resultados) >= 2 else resultados[0][1]
    melhor_score = min(melhor_score, 1.0)  # Limitar a 100% (1.0)
    
    segmentos_adicionais = []
    
    if len(resultados_tecnicos) > 1:
        segundo_tecnico = resultados_tecnicos[1][0]
        if calcular_similaridade(resultados_tecnicos[0][0], segundo_tecnico) < 0.7:
            seg_formatado = formatar_resposta(segundo_tecnico, max_caracteres=800, palavras_chave=palavras_chave)
            if seg_formatado and len(seg_formatado) > 100:
                segmentos_adicionais.append(seg_formatado)
    
    if len(resultados_teoricos) > 1:
        segundo_teorico = resultados_teoricos[1][0]
        if calcular_similaridade(resultados_teoricos[0][0], segundo_teorico) < 0.7:
            seg_formatado = formatar_resposta(segundo_teorico, max_caracteres=800, palavras_chave=palavras_chave)
            if seg_formatado and len(seg_formatado) > 100:
                segmentos_adicionais.append(seg_formatado)
    
    return {
        'resposta': resposta_formatada,
        'score': melhor_score,
        'segmentos_encontrados': segmentos_adicionais
    }
