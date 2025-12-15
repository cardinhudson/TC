"""
Chatbot de Documentação do Sistema TC

Sistema de busca e resposta baseado na documentação completa do sistema.
Não utiliza APIs externas - toda a busca é feita localmente.
"""

import os
import re
from typing import List, Tuple, Dict
from difflib import SequenceMatcher


def carregar_documentacao() -> str:
    """
    Carrega todo o conteúdo da documentação do sistema.
    Prioriza a documentação completa sobre a apresentação resumida.
    
    Returns:
        str: Conteúdo completo da documentação
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    # PRIORIDADE 1: Documentação completa (mais detalhada)
    arquivo_doc_completa = os.path.join(base_path, "pages", "6 - Documentacao.py")
    
    # PRIORIDADE 2: Apresentação (mais resumida, apenas como complemento)
    arquivos_apresentacao = [
        os.path.join(base_path, "APRESENTACAO_5_MINUTOS_VISUAL.md"),
        os.path.join(base_path, "APRESENTACAO_5_MINUTOS.md"),
    ]
    
    conteudo_completo = []
    
    # Carregar documentação completa primeiro (prioridade)
    if os.path.exists(arquivo_doc_completa):
        try:
            with open(arquivo_doc_completa, 'r', encoding='utf-8') as f:
                conteudo = f.read()
                # Extrair TODO o conteúdo markdown da documentação
                conteudo_extraido = extrair_texto_documentacao(conteudo)
                if conteudo_extraido:
                    conteudo_completo.append(conteudo_extraido)
        except Exception as e:
            print(f"Erro ao carregar documentação completa: {e}")
    
    # Carregar apresentações como complemento (menor prioridade)
    for arquivo in arquivos_apresentacao:
        if os.path.exists(arquivo):
            try:
                with open(arquivo, 'r', encoding='utf-8') as f:
                    conteudo = f.read()
                    # Apresentações já são markdown, usar diretamente
                    conteudo_completo.append(conteudo)
            except Exception as e:
                print(f"Erro ao carregar {arquivo}: {e}")
    
    # Separar documentação completa da apresentação para aplicar pesos diferentes
    doc_completa = conteudo_completo[0] if conteudo_completo else ""
    apresentacoes = "\n\n".join(conteudo_completo[1:]) if len(conteudo_completo) > 1 else ""
    
    # Combinar com marcador para identificar origem (priorizar documentação completa)
    # Adicionar prefixo para identificar origem na busca
    if doc_completa:
        doc_completa = "[DOC_COMPLETA]\n" + doc_completa
    if apresentacoes:
        apresentacoes = "[APRESENTACAO]\n" + apresentacoes
    
    return "\n\n".join([doc_completa, apresentacoes]) if apresentacoes else doc_completa


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
            # Limpar espaços múltiplos mas preservar quebras de linha
            texto = re.sub(r'[ \t]+', ' ', texto)  # Espaços e tabs
            texto = re.sub(r' \n', '\n', texto)  # Espaço antes de quebra
            texto = re.sub(r'\n{4,}', '\n\n\n', texto)  # Máximo 3 quebras consecutivas
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
    padroes_comentarios = [
        r'#\s+([A-Z][^#\n]{10,})',  # Comentários que começam com maiúscula
        r'#\s+SEÇÃO\s+\d+[:\s]+(.*)',  # Seções numeradas
        r'#\s+CAPÍTULO\s+\d+[:\s]+(.*)',  # Capítulos
    ]
    for padrao in padroes_comentarios:
        matches = re.finditer(padrao, codigo_python, re.IGNORECASE)
        for match in matches:
            texto = match.group(1).strip() if match.lastindex else match.group(0).strip()
            if len(texto) > 10:
                texto_extraido.append(texto)
    
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


def dividir_em_segmentos(texto: str, tamanho_segmento: int = 800) -> List[str]:
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


def calcular_similaridade(texto1: str, texto2: str) -> float:
    """
    Calcula similaridade entre dois textos usando SequenceMatcher.
    
    Args:
        texto1: Primeiro texto
        texto2: Segundo texto
    
    Returns:
        float: Similaridade entre 0 e 1
    """
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


def buscar_resposta(pergunta: str, documentacao: str, top_n: int = 5) -> List[Tuple[str, float]]:
    """
    Busca as melhores respostas na documentação baseado na pergunta.
    
    Args:
        pergunta: Pergunta do usuário
        documentacao: Texto completo da documentação
        top_n: Número de respostas a retornar
    
    Returns:
        List[Tuple[str, float]]: Lista de (resposta, score) ordenada por relevância
    """
    # Dividir documentação em segmentos
    segmentos = dividir_em_segmentos(documentacao)
    
    # Extrair palavras-chave da pergunta
    palavras_pergunta = re.findall(r'\b\w+\b', pergunta.lower())
    # Remover palavras muito comuns
    palavras_comuns = {'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                      'um', 'uma', 'uns', 'umas', 'é', 'são', 'como', 'que', 'qual', 'quais', 'para', 'por', 'com'}
    palavras_chave = [p for p in palavras_pergunta if p not in palavras_comuns and len(p) > 2]
    
    # Calcular score para cada segmento
    scores = []
    for segmento in segmentos:
        # Verificar se é da documentação completa ou apresentação
        peso_origem = 1.5 if segmento.startswith("[DOC_COMPLETA]") else 0.8  # Priorizar doc completa
        # Remover marcador para cálculo
        segmento_limpo = segmento.replace("[DOC_COMPLETA]\n", "").replace("[APRESENTACAO]\n", "")
        
        # Score baseado em similaridade
        similaridade = calcular_similaridade(pergunta, segmento_limpo)
        
        # Score baseado em palavras-chave
        palavras_encontradas = buscar_palavras_chave(segmento_limpo, palavras_chave)
        score_palavras = palavras_encontradas / max(len(palavras_chave), 1) if palavras_chave else 0
        
        # Score combinado (priorizar palavras-chave)
        score_final = (similaridade * 0.3) + (score_palavras * 0.7)
        
        # Aplicar peso de origem (documentação completa tem prioridade)
        score_final *= peso_origem
        
        # Bonus para segmentos maiores (mais completos)
        if len(segmento_limpo) > 500:
            score_final *= 1.15  # 15% de bonus para respostas mais completas
        
        # Usar segmento limpo (sem marcador) no resultado
        scores.append((segmento_limpo, score_final))
    
    # Ordenar por score e retornar top N
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_n]


def formatar_resposta(segmento: str, max_caracteres: int = 3000) -> str:
    """
    Formata um segmento de resposta para exibição.
    
    Args:
        segmento: Texto do segmento
        max_caracteres: Número máximo de caracteres (aumentado para respostas mais completas)
    
    Returns:
        str: Resposta formatada
    """
    # Limpar código Python mas manter estrutura markdown
    # Remover apenas código Python, manter diagramas ASCII e markdown
    segmento = re.sub(r'```python.*?```', '', segmento, flags=re.DOTALL)
    # Manter diagramas ASCII (boxes com ┌ ┐ └ ┘ │ ─)
    segmento = re.sub(r'st\.\w+\([^)]*\)', '', segmento)
    
    # Limitar tamanho apenas se muito grande, mas preservar parágrafos completos
    if len(segmento) > max_caracteres:
        # Tentar cortar em um ponto de parágrafo
        corte = segmento[:max_caracteres]
        ultimo_paragrafo = corte.rfind('\n\n')
        if ultimo_paragrafo > max_caracteres * 0.7:  # Se encontrou parágrafo em 70% do tamanho
            segmento = segmento[:ultimo_paragrafo] + "\n\n..."
        else:
            segmento = segmento[:max_caracteres] + "..."
    
    return segmento.strip()


def responder_pergunta(pergunta: str) -> Dict[str, any]:
    """
    Responde uma pergunta baseada na documentação.
    
    Args:
        pergunta: Pergunta do usuário
    
    Returns:
        Dict com 'resposta', 'score' e 'segmentos_encontrados'
    """
    # Carregar documentação
    documentacao = carregar_documentacao()
    
    if not documentacao:
        return {
            'resposta': 'Desculpe, não foi possível carregar a documentação.',
            'score': 0.0,
            'segmentos_encontrados': []
        }
    
    # Buscar mais respostas para combinar
    resultados = buscar_resposta(pergunta, documentacao, top_n=5)
    
    if not resultados or resultados[0][1] < 0.1:
        return {
            'resposta': 'Desculpe, não encontrei informações relevantes na documentação para sua pergunta. Tente reformular ou usar palavras-chave diferentes.',
            'score': 0.0,
            'segmentos_encontrados': []
        }
    
    # Combinar múltiplos segmentos relevantes para resposta mais completa
    melhor_segmento, melhor_score = resultados[0]
    
    # Combinar com segmentos adicionais relevantes (score > 0.2 para incluir mais conteúdo)
    segmentos_combinados = [melhor_segmento]
    for segmento, score in resultados[1:]:
        if score > 0.2:  # Segmentos com relevância razoável
            # Verificar se não é muito similar ao segmento principal
            similaridade_com_principal = calcular_similaridade(melhor_segmento, segmento)
            if similaridade_com_principal < 0.85:  # Se não for muito similar, adicionar
                segmentos_combinados.append(segmento)
                if len(segmentos_combinados) >= 4:  # Até 4 segmentos para resposta mais completa
                    break
    
    # Combinar segmentos em uma resposta completa
    resposta_completa = "\n\n---\n\n".join(segmentos_combinados)
    resposta_formatada = formatar_resposta(resposta_completa, max_caracteres=5000)  # Aumentado para 5000
    
    # Segmentos adicionais para expanders (com score médio)
    segmentos_adicionais = []
    for segmento, score in resultados[len(segmentos_combinados):]:
        if score > 0.2:  # Segmentos com relevância média
            segmento_formatado = formatar_resposta(segmento, max_caracteres=1000)
            if segmento_formatado and len(segmento_formatado) > 50:
                segmentos_adicionais.append(segmento_formatado)
            if len(segmentos_adicionais) >= 2:  # Máximo 2 segmentos adicionais
                break
    
    return {
        'resposta': resposta_formatada,
        'score': melhor_score,
        'segmentos_encontrados': segmentos_adicionais
    }
