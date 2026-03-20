from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from tc_core.utils.portabilidade import get_data_root


def _pasta_tc_ext() -> str:
    """Caminho dinâmico para a pasta TC_Ext (resolvido em tempo de uso)."""
    return str(get_data_root() / "TC_Ext")


def _pasta_tc_principal() -> str:
    """Caminho dinâmico para a pasta TC_Principal (resolvido em tempo de uso)."""
    return str(get_data_root() / "TC_Principal")


# Backward-compat: mantém nomes antigos como propriedades avaliadas sob demanda.
# Código existente que importa PASTA_TC_EXT / PASTA_TC_PRINCIPAL continua funcionando.
class _LazyStr(str):
    """str que avalia o callable a cada uso, mas se comporta como str para isinstance."""
    def __new__(cls, func):
        obj = super().__new__(cls, func())
        obj._func = func
        return obj
    def __str__(self):  # noqa: D105
        return self._func()
    def __fspath__(self):  # noqa: D105
        return self._func()
    def __add__(self, other):
        return self._func() + other
    def __radd__(self, other):
        return other + self._func()


PASTA_TC_EXT = _LazyStr(_pasta_tc_ext)
PASTA_TC_PRINCIPAL = _LazyStr(_pasta_tc_principal)


def listar_anos_disponiveis(pasta_dados: str = None) -> list[int]:
    """Lista anos disponíveis no TC_Ext."""
    if pasta_dados is None:
        pasta_dados = _pasta_tc_ext()
    anos: list[int] = []
    if os.path.exists(pasta_dados):
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos.append(int(item))
    return sorted(anos, reverse=True)


def encontrar_arquivo_parquet(nome_arquivo: str, ano_selecionado: Optional[str] = None) -> Optional[str]:
    """Procura arquivo parquet no TC_Ext."""
    pasta = _pasta_tc_ext()
    # Procurar no ano específico
    if ano_selecionado is not None and ano_selecionado != "Todos":
        caminho_ano = os.path.join(pasta, str(ano_selecionado), nome_arquivo)
        if os.path.exists(caminho_ano):
            return caminho_ano

    # Procurar no histórico consolidado
    caminho_historico = os.path.join(
        pasta,
        "historico_consolidado",
        nome_arquivo.replace(".parquet", "_historico.parquet"),
    )
    if os.path.exists(caminho_historico):
        return caminho_historico

    # Procurar no ano mais recente
    if os.path.exists(pasta):
        anos = listar_anos_disponiveis(pasta)
        if anos:
            caminho_ano = os.path.join(pasta, str(anos[0]), nome_arquivo)
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
