"""
Sistema de Versionamento Automático do Stellantis Cost Intelligence (SCI)

Este módulo gerencia a versão do sistema de forma automática.
A versão incrementa automaticamente a cada execução seguindo o padrão:
1.0 -> 1.01 -> 1.02 -> ... -> 1.09 -> 1.1 -> 1.11 -> etc.

Permite também resetar a versão manualmente quando necessário.
"""

import json
import os
import sys
from datetime import datetime


def get_versao_path():
    """Retorna o caminho do arquivo de versão"""
    if getattr(sys, 'frozen', False):
        # Executável PyInstaller: versao.json está em _internal/ (sys._MEIPASS)
        base_path = sys._MEIPASS  # type: ignore[attr-defined]
    else:
        # Desenvolvimento: raiz do repositório
        base_path = os.path.dirname(os.path.abspath(__file__))
    
    return os.path.join(base_path, 'versao.json')


def carregar_versao():
    """Carrega a versão atual do arquivo JSON"""
    versao_path = get_versao_path()
    
    # Se o arquivo não existe, cria com versão inicial 1.0
    if not os.path.exists(versao_path):
        versao_atual = "1.0"
        salvar_versao(versao_atual)
        return versao_atual
    
    try:
        with open(versao_path, 'r', encoding='utf-8') as f:
            dados = json.load(f)
            return dados.get('versao', '1.0')
    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        # Se houver erro, retorna versão padrão
        versao_atual = "1.0"
        salvar_versao(versao_atual)
        return versao_atual


def salvar_versao(versao):
    """Salva a versão no arquivo JSON"""
    versao_path = get_versao_path()
    
    dados = {
        'versao': versao,
        'ultima_atualizacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(versao_path, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)


def incrementar_versao():
    """
    Incrementa a versão automaticamente seguindo o padrão:
    1.0 -> 1.01 -> 1.02 -> ... -> 1.09 -> 1.1 -> 1.11 -> 1.12 -> etc.
    """
    versao_atual = carregar_versao()
    
    # Separa versão em parte inteira e decimal
    partes = versao_atual.split('.')
    parte_inteira = int(partes[0])
    parte_decimal_str = partes[1] if len(partes) > 1 else "0"
    
    # Determina se a versão atual usa 2 dígitos decimais
    usa_dois_digitos = len(parte_decimal_str) == 2
    
    # Converte parte decimal para número inteiro
    # Se for "1" (de 1.1), trata como 10 para facilitar o cálculo
    if parte_decimal_str == "1" and not usa_dois_digitos:
        parte_decimal = 10
    else:
        parte_decimal = int(parte_decimal_str)
    
    # Incrementa a parte decimal
    parte_decimal += 1
    
    # Lógica de formatação seguindo o padrão:
    # 1.0 -> 1.01 -> 1.02 -> ... -> 1.09 -> 1.1 -> 1.11 -> 1.12 -> ...
    if versao_atual == "1.0":
        # 1.0 -> 1.01 (primeiro incremento usa 2 dígitos)
        nova_versao = f"{parte_inteira}.01"
    elif parte_decimal == 10 and usa_dois_digitos:
        # 1.09 -> 1.1 (remove zero à esquerda)
        nova_versao = f"{parte_inteira}.1"
    elif parte_decimal == 11:
        # 1.1 -> 1.11 (volta a usar 2 dígitos)
        nova_versao = f"{parte_inteira}.11"
    elif usa_dois_digitos:
        # 1.01 -> 1.02, 1.11 -> 1.12, etc. (mantém 2 dígitos)
        nova_versao = f"{parte_inteira}.{parte_decimal:02d}"
    elif parte_decimal >= 10:
        # 1.9 -> 2.0 (incrementa parte inteira)
        parte_inteira += 1
        parte_decimal = parte_decimal - 10
        nova_versao = f"{parte_inteira}.{parte_decimal}"
    else:
        # Caso padrão: mantém formato atual
        nova_versao = f"{parte_inteira}.{parte_decimal}"
    
    salvar_versao(nova_versao)
    return nova_versao


def obter_versao_atual():
    """
    Retorna a versão atual sem incrementar.
    Use esta função para exibir a versão no rodapé.
    """
    return carregar_versao()


def resetar_versao(nova_versao="1.0"):
    """
    Reseta a versão para um valor específico.
    
    Args:
        nova_versao (str): Nova versão (ex: "2.0", "1.5", etc.)
    """
    salvar_versao(nova_versao)
    return nova_versao


def obter_versao_com_incremento():
    """
    Retorna a versão atual e incrementa automaticamente.
    Use esta função quando quiser que a versão seja incrementada a cada execução.
    """
    versao_atual = carregar_versao()
    incrementar_versao()
    return versao_atual


def get_controle_paginas_path():
    """Retorna o caminho do arquivo de controle de timestamps das páginas"""
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, 'controle_paginas.json')


def obter_timestamps_paginas():
    """Obtém os timestamps de modificação de todas as páginas do sistema"""
    base_path = os.path.dirname(os.path.abspath(__file__))
    pasta_pages = os.path.join(base_path, 'pages')
    
    timestamps = {}
    
    if not os.path.exists(pasta_pages):
        return timestamps
    
    # Lista todas as páginas .py na pasta pages
    for arquivo in os.listdir(pasta_pages):
        if arquivo.endswith('.py'):
            caminho_completo = os.path.join(pasta_pages, arquivo)
            if os.path.isfile(caminho_completo):
                # Obtém timestamp de modificação
                timestamp = os.path.getmtime(caminho_completo)
                timestamps[arquivo] = timestamp
    
    return timestamps


def carregar_controle_paginas():
    """Carrega o arquivo de controle com timestamps anteriores das páginas"""
    controle_path = get_controle_paginas_path()
    
    if not os.path.exists(controle_path):
        return {}
    
    try:
        with open(controle_path, 'r', encoding='utf-8') as f:
            dados = json.load(f)
            return dados.get('timestamps', {})
    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        return {}


def salvar_controle_paginas(timestamps):
    """Salva o arquivo de controle com timestamps atuais das páginas"""
    controle_path = get_controle_paginas_path()
    
    dados = {
        'timestamps': timestamps,
        'ultima_verificacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(controle_path, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)


def verificar_mudancas_paginas():
    """
    Verifica se houve mudanças nas páginas do sistema.
    Se detectar mudanças, incrementa a versão automaticamente.
    
    Returns:
        tuple: (houve_mudanca: bool, versao_atual: str)
    """
    # Obter timestamps atuais das páginas
    timestamps_atuais = obter_timestamps_paginas()
    
    # Carregar timestamps anteriores
    timestamps_anteriores = carregar_controle_paginas()
    
    # Se não há controle anterior, criar um e não incrementar (primeira execução)
    if not timestamps_anteriores:
        salvar_controle_paginas(timestamps_atuais)
        return False, carregar_versao()
    
    # Verificar se houve mudanças
    houve_mudanca = False
    
    # Verificar se alguma página foi modificada ou nova página foi adicionada
    for arquivo, timestamp_atual in timestamps_atuais.items():
        if arquivo not in timestamps_anteriores:
            # Nova página adicionada
            houve_mudanca = True
            break
        elif timestamps_anteriores[arquivo] != timestamp_atual:
            # Página foi modificada
            houve_mudanca = True
            break
    
    # Verificar se alguma página foi removida
    if not houve_mudanca:
        for arquivo in timestamps_anteriores:
            if arquivo not in timestamps_atuais:
                houve_mudanca = True
                break
    
    # Se houve mudança, incrementar versão
    if houve_mudanca:
        nova_versao = incrementar_versao()
        # Atualizar controle com timestamps atuais
        salvar_controle_paginas(timestamps_atuais)
        return True, nova_versao
    else:
        # Atualizar controle mesmo sem mudanças (para manter sincronizado)
        salvar_controle_paginas(timestamps_atuais)
        return False, carregar_versao()

