"""Utilitários de data/tempo para o módulo de alertas."""

from __future__ import annotations

import calendar
from datetime import date, datetime

from tc_core.constants import MESES_NUMERO


def dias_corridos_mes(data: date) -> int:
    """Retorna quantidade de dias corridos do mês até *data* (inclusive)."""
    return data.day


def dias_totais_mes(data: date) -> int:
    """Retorna o total de dias do mês de *data*."""
    return calendar.monthrange(data.year, data.month)[1]


def proporcao_mes(data: date) -> float:
    """Retorna a fração do mês decorrida: dias_corridos / dias_totais."""
    total = dias_totais_mes(data)
    if total == 0:
        return 0.0
    return dias_corridos_mes(data) / total


def mes_atual_nome(data: date | None = None) -> str:
    """Nome do mês em pt-BR, compatível com ``ORDEM_MESES`` de *tc_core.constants*.

    Se *data* não for fornecida, usa ``date.today()``.
    """
    if data is None:
        data = date.today()
    return MESES_NUMERO.get(data.month, "")


def is_fechamento(data: date) -> bool:
    """Retorna ``True`` se *data* é o último dia útil do mês.

    Considera sábado (5) e domingo (6) como não-úteis.
    """
    ultimo_dia = dias_totais_mes(data)
    # Percorre de trás para frente até achar dia útil
    d = date(data.year, data.month, ultimo_dia)
    while d.weekday() >= 5:  # sáb=5, dom=6
        d = d.replace(day=d.day - 1)
    return data == d


def timestamp_agora_iso() -> str:
    """Retorna timestamp ISO-8601 do instante atual."""
    return datetime.now().isoformat(timespec="seconds")
