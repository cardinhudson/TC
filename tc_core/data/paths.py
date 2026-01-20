from __future__ import annotations

import os
from typing import Optional


def listar_anos_disponiveis(pasta_dados: str = "dados") -> list[int]:
    anos: list[int] = []
    if os.path.exists(pasta_dados):
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos.append(int(item))
    return sorted(anos, reverse=True)


def encontrar_arquivo_parquet(nome_arquivo: str, ano_selecionado: Optional[str] = None) -> Optional[str]:
    """Procura arquivo parquet na mesma ordem usada no app atual."""
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join("dados", str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano

    caminho_historico = os.path.join(
        "dados",
        "historico_consolidado",
        nome_arquivo.replace(".parquet", "_historico.parquet"),
    )
    if os.path.exists(caminho_historico):
        return caminho_historico

    pasta_dados = "dados"
    if os.path.exists(pasta_dados):
        anos = listar_anos_disponiveis(pasta_dados)
        if anos:
            caminho_ano = os.path.join(pasta_dados, str(anos[0]), nome_arquivo)
            if os.path.exists(caminho_ano):
                return caminho_ano

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
