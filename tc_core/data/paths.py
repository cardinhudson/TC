from __future__ import annotations

import os
from typing import Optional

# Constantes de caminhos base
PASTA_TC_EXT = os.path.join("dados", "TC_Ext")
PASTA_TC_PRINCIPAL = os.path.join("dados", "TC_Principal")


def listar_anos_disponiveis(pasta_dados: str = None) -> list[int]:
    """Lista anos disponíveis no TC_Ext."""
    if pasta_dados is None:
        pasta_dados = PASTA_TC_EXT
    anos: list[int] = []
    if os.path.exists(pasta_dados):
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos.append(int(item))
    return sorted(anos, reverse=True)


def encontrar_arquivo_parquet(nome_arquivo: str, ano_selecionado: Optional[str] = None) -> Optional[str]:
    """Procura arquivo parquet no TC_Ext."""
    # Procurar no ano específico
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join(PASTA_TC_EXT, str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano

    # Procurar no histórico consolidado
    caminho_historico = os.path.join(
        PASTA_TC_EXT,
        "historico_consolidado",
        nome_arquivo.replace(".parquet", "_historico.parquet"),
    )
    if os.path.exists(caminho_historico):
        return caminho_historico

    # Procurar no ano mais recente
    if os.path.exists(PASTA_TC_EXT):
        anos = listar_anos_disponiveis(PASTA_TC_EXT)
        if anos:
            caminho_ano = os.path.join(PASTA_TC_EXT, str(anos[0]), nome_arquivo)
            if os.path.exists(caminho_ano):
                return caminho_ano

    # Fallback para caminho direto
    if os.path.exists(nome_arquivo):
        return nome_arquivo

    return None


def obter_timestamp_atualizacao(arquivos: list[str]) -> Optional[float]:
    data_atualizacao: Optional[float] = None
    for arquivo in arquivos:
        if os.path.exists(arquivo):
            try:
                ts = os.path.getmtime(arquivo)
            except (OSError, ValueError):
                continue
            if ts and ts > 0 and (data_atualizacao is None or ts > data_atualizacao):
                data_atualizacao = ts
    return data_atualizacao
