"""Utilitários matemáticos e de dados para o módulo de alertas."""

from __future__ import annotations

import statistics
from typing import Sequence


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """Divisão segura — retorna *default* quando *b* é zero."""
    if b == 0:
        return default
    return a / b


def desvio_absoluto(real: float, esperado: float) -> float:
    """Retorna desvio absoluto ``real - esperado``."""
    return real - esperado


def desvio_percentual(real: float, esperado: float) -> float:
    """Retorna desvio percentual ``(real - esperado) / esperado * 100``.

    Retorna 0.0 quando *esperado* é zero.
    """
    return safe_divide(real - esperado, esperado) * 100.0


def detectar_anomalia_historica(
    serie: Sequence[float],
    valor_atual: float,
    n_desvios: float = 2.0,
) -> bool:
    """Retorna ``True`` se *valor_atual* extrapola média ± *n_desvios* × stdev da *serie*.

    Requer pelo menos 2 pontos; caso contrário retorna ``False``.
    """
    vals = [v for v in serie if v is not None]
    if len(vals) < 2:
        return False
    media = statistics.mean(vals)
    std = statistics.stdev(vals)
    if std == 0:
        return valor_atual != media
    return abs(valor_atual - media) > n_desvios * std


def calcular_expected_parcial(valor_total: float, proporcao: float) -> float:
    """Custo esperado proporcional: ``valor_total × P``."""
    return valor_total * proporcao


def calcular_volume_parcial(volume_full_month: float, proporcao: float) -> float:
    """Volume proporcional: ``volume_full_month × P``.

    Volume nos Parquets é SEMPRE a previsão do mês inteiro; esta função
    aplica a proporção do mês para obter o volume parcial comparável
    ao custo real acumulado.
    """
    return volume_full_month * proporcao


def calcular_cpu_esperado(
    expected_cost_parcial: float,
    expected_volume_parcial: float,
) -> float:
    """CPU esperado proporcional — ambos os termos já devem estar proporcionais."""
    return safe_divide(expected_cost_parcial, expected_volume_parcial)


def calcular_flex_bud_parcial(
    budget_fixo: float,
    budget_variavel: float,
    proporcao: float,
    ratio_volume: float,
) -> float:
    """Flex Budget proporcional ao mês.

    - Fixos proporcionais: ``budget_fixo × P``
    - Variáveis proporcionais com ajuste de volume:
      ``budget_variavel × P × ratio_volume``
    - *ratio_volume* = real_volume_parcial / expected_volume_parcial
    """
    flex_fixo = budget_fixo * proporcao
    flex_variavel = budget_variavel * proporcao * ratio_volume
    return flex_fixo + flex_variavel
