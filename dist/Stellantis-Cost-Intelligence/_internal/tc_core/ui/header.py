from __future__ import annotations

from datetime import datetime
from typing import Optional

from tc_core.constants import MESES_NUMERO


def obter_mes_atual() -> str:
    agora = datetime.now()
    return MESES_NUMERO.get(agora.month, str(agora.month))


def formatar_timestamp_ptbr(ts: float) -> Optional[str]:
    if not ts or ts <= 0:
        return None
    try:
        dt = datetime.fromtimestamp(ts)
    except (OSError, ValueError):
        return None

    mes = MESES_NUMERO.get(dt.month, str(dt.month))
    return f"{dt.day:02d} de {mes} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
