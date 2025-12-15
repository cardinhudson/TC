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
    
    # Carregar APENAS a documentação completa (NÃO incluir apresentações)
    arquivo_doc_completa = os.path.join(base_path, "pages", "6 - Documentacao.py")
    
    conteudo_completo = ""
    
    # Carregar apenas documentação completa
    if os.path.exists(arquivo_doc_completa):
        try:
            with open(arquivo_doc_completa, 'r', encoding='utf-8') as f:
                conteudo = f.read()
                # Extrair TODO o conteúdo markdown da documentação
                conteudo_extraido = extrair_texto_documentacao(conteudo)
                if conteudo_extraido:
                    conteudo_completo = conteudo_extraido
        except Exception as e:
            print(f"Erro ao carregar documentação completa: {e}")
    
    return conteudo_completo


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


def dividir_em_segmentos(texto: str, tamanho_segmento: int = 600) -> List[str]:
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


def buscar_resposta(pergunta: str, documentacao: str, top_n: int = 10) -> List[Tuple[str, float, str]]:
    """
    Busca as melhores respostas na documentação baseado na pergunta.
    
    Args:
        pergunta: Pergunta do usuário
        documentacao: Texto completo da documentação
        top_n: Número de respostas a retornar
    
    Returns:
        List[Tuple[str, float, str]]: Lista de (resposta, score, tipo) ordenada por relevância
        onde tipo é 'tecnico' ou 'teorico'
    """
    # Dividir documentação em segmentos
    segmentos = dividir_em_segmentos(documentacao)
    
    # Extrair palavras-chave da pergunta
    palavras_pergunta = re.findall(r'\b\w+\b', pergunta.lower())
    # Remover palavras muito comuns
    palavras_comuns = {'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                      'um', 'uma', 'uns', 'umas', 'é', 'são', 'como', 'que', 'qual', 'quais', 'para', 'por', 'com'}
    palavras_chave = [p for p in palavras_pergunta if p not in palavras_comuns and len(p) > 2]
    
    # Calcular score para cada segmento e classificar tipo
    scores = []
    for segmento in segmentos:
        # Score baseado em similaridade
        similaridade = calcular_similaridade(pergunta, segmento)
        
        # Score baseado em palavras-chave (mais rigoroso)
        palavras_encontradas = buscar_palavras_chave(segmento, palavras_chave)
        score_palavras = palavras_encontradas / max(len(palavras_chave), 1) if palavras_chave else 0
        
        # Bonus se encontrar TODAS as palavras-chave importantes
        if palavras_chave and palavras_encontradas == len(palavras_chave):
            score_palavras *= 1.3  # 30% de bonus se encontrar todas
        
        # Score combinado (priorizar palavras-chave ainda mais)
        score_final = (similaridade * 0.25) + (score_palavras * 0.75)
        
        # Penalizar segmentos muito grandes (priorizar respostas mais diretas)
        if len(segmento) > 1000:
            score_final *= 0.9  # 10% de penalidade para segmentos muito longos
        elif len(segmento) < 200:
            score_final *= 1.1  # 10% de bonus para segmentos concisos e diretos
        
        # Classificar tipo do segmento
        tipo_segmento = classificar_tipo_segmento(segmento)
        
        # Adicionar com tipo
        scores.append((segmento, score_final, tipo_segmento))
    
    # Ordenar por score e retornar top N
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_n]


def extrair_trecho_relevante(segmento: str, palavras_chave: List[str], max_caracteres: int = 2000) -> str:
    """
    Extrai o trecho mais relevante de um segmento baseado nas palavras-chave da pergunta.
    
    Args:
        segmento: Texto completo do segmento
        palavras_chave: Lista de palavras-chave da pergunta
        max_caracteres: Tamanho máximo do trecho
    
    Returns:
        str: Trecho mais relevante
    """
    if not palavras_chave:
        return segmento[:max_caracteres]
    
    # Encontrar parágrafos que contêm palavras-chave
    paragrafos = segmento.split('\n\n')
    paragrafos_relevantes = []
    
    for paragrafo in paragrafos:
        paragrafo_lower = paragrafo.lower()
        # Contar quantas palavras-chave aparecem neste parágrafo
        palavras_no_paragrafo = sum(1 for palavra in palavras_chave if palavra.lower() in paragrafo_lower)
        if palavras_no_paragrafo > 0:
            paragrafos_relevantes.append((paragrafo, palavras_no_paragrafo))
    
    # Se encontrou parágrafos relevantes, priorizar eles
    if paragrafos_relevantes:
        # Ordenar por relevância (mais palavras-chave primeiro)
        paragrafos_relevantes.sort(key=lambda x: x[1], reverse=True)
        # Pegar os mais relevantes até o limite
        trecho = []
        tamanho_atual = 0
        for paragrafo, _ in paragrafos_relevantes:
            if tamanho_atual + len(paragrafo) <= max_caracteres:
                trecho.append(paragrafo)
                tamanho_atual += len(paragrafo)
            else:
                break
        
        if trecho:
            return '\n\n'.join(trecho)
    
    # Se não encontrou parágrafos específicos, retornar início do segmento
    return segmento[:max_caracteres]


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
    # Limpar código Python mas manter estrutura markdown
    segmento = re.sub(r'```python.*?```', '', segmento, flags=re.DOTALL)
    segmento = re.sub(r'st\.\w+\([^)]*\)', '', segmento)
    
    # Se temos palavras-chave, extrair trecho mais relevante
    if palavras_chave:
        segmento = extrair_trecho_relevante(segmento, palavras_chave, max_caracteres)
    
    # Limitar tamanho se necessário, preservando parágrafos completos
    if len(segmento) > max_caracteres:
        corte = segmento[:max_caracteres]
        ultimo_paragrafo = corte.rfind('\n\n')
        if ultimo_paragrafo > max_caracteres * 0.7:
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
    
    # Buscar respostas (aumentar top_n para ter mais opções de classificação)
    resultados = buscar_resposta(pergunta, documentacao, top_n=10)
    
    # Aumentar threshold mínimo para respostas mais precisas
    if not resultados or resultados[0][1] < 0.15:
        return {
            'resposta': 'Desculpe, não encontrei informações relevantes na documentação para sua pergunta. Tente reformular ou usar palavras-chave diferentes.',
            'score': 0.0,
            'segmentos_encontrados': []
        }
    
    # Extrair palavras-chave da pergunta para focar a resposta
    palavras_pergunta = re.findall(r'\b\w+\b', pergunta.lower())
    palavras_comuns = {'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                      'um', 'uma', 'uns', 'umas', 'é', 'são', 'como', 'que', 'qual', 'quais', 'para', 'por', 'com'}
    palavras_chave = [p for p in palavras_pergunta if p not in palavras_comuns and len(p) > 2]
    
    # Separar resultados por tipo (técnico e teórico)
    resultados_tecnicos = [(seg, score) for seg, score, tipo in resultados if tipo == 'tecnico' and score >= 0.15]
    resultados_teoricos = [(seg, score) for seg, score, tipo in resultados if tipo == 'teorico' and score >= 0.15]
    
    # Sempre buscar 2 respostas: uma técnica e uma teórica
    resposta_tecnica = None
    resposta_teorica = None
    
    # Buscar melhor resposta técnica
    if resultados_tecnicos:
        melhor_tecnico, score_tecnico = resultados_tecnicos[0]
        resposta_tecnica = formatar_resposta(melhor_tecnico, max_caracteres=1800, palavras_chave=palavras_chave)
    
    # Buscar melhor resposta teórica
    if resultados_teoricos:
        melhor_teorico, score_teorico = resultados_teoricos[0]
        resposta_teorica = formatar_resposta(melhor_teorico, max_caracteres=1800, palavras_chave=palavras_chave)
    
    # Combinar as duas respostas
    respostas_combinadas = []
    if resposta_tecnica:
        respostas_combinadas.append(f"**🔧 Resposta Técnica/Implementação:**\n\n{resposta_tecnica}")
    if resposta_teorica:
        respostas_combinadas.append(f"**📚 Resposta Teórica/Cálculos:**\n\n{resposta_teorica}")
    
    # Se não encontrou ambos os tipos, usar os melhores disponíveis
    if not resposta_tecnica and not resposta_teorica:
        # Fallback: usar melhor resultado geral
        melhor_segmento, melhor_score = resultados[0][0], resultados[0][1]
        resposta_formatada = formatar_resposta(melhor_segmento, max_caracteres=2000, palavras_chave=palavras_chave)
        respostas_combinadas = [resposta_formatada]
    elif not resposta_tecnica:
        # Se só tem teórica, buscar segunda melhor técnica ou melhor geral
        if len(resultados) > 1:
            segundo_melhor = resultados[1][0]
            resposta_tecnica_alt = formatar_resposta(segundo_melhor, max_caracteres=1800, palavras_chave=palavras_chave)
            respostas_combinadas.insert(0, f"**🔧 Resposta Técnica/Implementação:**\n\n{resposta_tecnica_alt}")
    elif not resposta_teorica:
        # Se só tem técnica, buscar segunda melhor teórica ou melhor geral
        if len(resultados) > 1:
            segundo_melhor = resultados[1][0]
            resposta_teorica_alt = formatar_resposta(segundo_melhor, max_caracteres=1800, palavras_chave=palavras_chave)
            respostas_combinadas.append(f"**📚 Resposta Teórica/Cálculos:**\n\n{resposta_teorica_alt}")
    
    # Combinar respostas
    resposta_formatada = "\n\n---\n\n".join(respostas_combinadas)
    
    # Melhor score para retorno
    melhor_score = max([r[1] for r in resultados[:2]]) if len(resultados) >= 2 else resultados[0][1]
    
    # Segmentos adicionais (terceira melhor de cada tipo se disponível)
    segmentos_adicionais = []
    if len(resultados_tecnicos) > 1 and len(segmentos_adicionais) < 1:
        segundo_tecnico = resultados_tecnicos[1][0]
        if calcular_similaridade(resultados_tecnicos[0][0], segundo_tecnico) < 0.7:
            seg_formatado = formatar_resposta(segundo_tecnico, max_caracteres=600, palavras_chave=palavras_chave)
            if seg_formatado and len(seg_formatado) > 50:
                segmentos_adicionais.append(seg_formatado)
    
    if len(resultados_teoricos) > 1 and len(segmentos_adicionais) < 1:
        segundo_teorico = resultados_teoricos[1][0]
        if calcular_similaridade(resultados_teoricos[0][0], segundo_teorico) < 0.7:
            seg_formatado = formatar_resposta(segundo_teorico, max_caracteres=600, palavras_chave=palavras_chave)
            if seg_formatado and len(seg_formatado) > 50:
                segmentos_adicionais.append(seg_formatado)
    
    return {
        'resposta': resposta_formatada,
        'score': melhor_score,
        'segmentos_encontrados': segmentos_adicionais
    }
