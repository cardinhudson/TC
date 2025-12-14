"""
Sistema de Versionamento Automático do Sistema TC

Este módulo gerencia a versão do sistema de forma automática.
A versão incrementa automaticamente a cada execução seguindo o padrão:
1.0 -> 1.01 -> 1.02 -> ... -> 1.09 -> 1.1 -> 1.11 -> etc.

Permite também resetar a versão manualmente quando necessário.
"""

import json
import os
from datetime import datetime


def get_versao_path():
    """Retorna o caminho do arquivo de versão"""
    # Tenta encontrar o diretório base do projeto
    if hasattr(os, '_MEIPASS'):
        # Executável PyInstaller
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    else:
        # Desenvolvimento
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
    parte_decimal = int(parte_decimal_str)
    
    # Determina se a versão atual usa 2 dígitos decimais (ANTES de incrementar)
    usa_dois_digitos_original = len(parte_decimal_str) == 2 or versao_atual == "1.0"
    eh_versao_1_1 = versao_atual == "1.1"
    
    # Se versão é "1.1", trata como se parte_decimal fosse 10 (não 1)
    # Isso permite que 1.1 -> 1.11 (não 1.02)
    if eh_versao_1_1:
        parte_decimal = 10  # Trata como se fosse 1.10
    
    # Incrementa a parte decimal
    parte_decimal += 1
    
    # Lógica de incremento e formatação:
    # - Se parte_decimal == 10 e estava usando 2 dígitos: transforma em 1 (sem zero) -> 1.09 -> 1.1
    # - Se parte_decimal == 11 (vindo de 1.1): formata como 1.11 (com 2 dígitos)
    # - Se parte_decimal < 10 e estava usando 2 dígitos: mantém 2 dígitos -> 1.01 -> 1.02, 1.11 -> 1.12
    # - Se parte_decimal == 20 (vindo de 1.19): transforma em 2 (sem zero) -> 1.19 -> 1.2
    # - Se parte_decimal >= 10 e não estava usando 2 dígitos: incrementa parte inteira -> 1.9 -> 2.0
    
    if parte_decimal == 10 and usa_dois_digitos_original:
        # 1.09 -> 1.1 (sem zero à esquerda)
        parte_decimal = 1
        usa_dois_digitos = False
    elif parte_decimal == 11:
        # 1.1 -> 1.11 (volta a usar 2 dígitos)
        parte_decimal = 11
        usa_dois_digitos = True
    elif parte_decimal >= 10 and not usa_dois_digitos_original and not eh_versao_1_1:
        # 1.9 -> 2.0 (incrementa parte inteira)
        parte_inteira += 1
        parte_decimal = parte_decimal - 10
        usa_dois_digitos = False  # 2.0 não usa 2 dígitos
    elif usa_dois_digitos_original:
        # Mantém 2 dígitos (1.01 -> 1.02, 1.11 -> 1.12, etc.)
        usa_dois_digitos = True
    else:
        # Caso padrão: mantém formato atual
        usa_dois_digitos = False
    
    # Formata a nova versão
    if parte_decimal == 11:
        # 1.11 (mantém como está)
        nova_versao = f"{parte_inteira}.{parte_decimal}"
    elif usa_dois_digitos and parte_decimal < 10:
        # 1.01, 1.02, etc. (com 2 dígitos)
        nova_versao = f"{parte_inteira}.{parte_decimal:02d}"
    else:
        # 1.1, 1.2, etc. (sem zero à esquerda)
        nova_versao = f"{parte_inteira}.{parte_decimal}"
    
    # Formata a nova versão
    if usa_dois_digitos and parte_decimal < 10:
        nova_versao = f"{parte_inteira}.{parte_decimal:02d}"
    else:
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

