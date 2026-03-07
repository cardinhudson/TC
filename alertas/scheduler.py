"""Agendamento automático de alertas diários via APScheduler."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_scheduler = None


def _job_daily_check() -> None:
    """Job executado diariamente — chama run_daily_check()."""
    try:
        from alertas.alert_engine import run_daily_check
        logger.info("Executando verificação diária de alertas...")
        alertas = run_daily_check()
        logger.info("Verificação diária concluída — %d alertas gerados.", len(alertas))
    except Exception:
        logger.exception("Erro na verificação diária de alertas.")


def init_scheduler(hour: int = 8, minute: int = 0) -> None:
    """Inicia o BackgroundScheduler com job diário no horário indicado."""
    global _scheduler  # noqa: PLW0603

    stop_scheduler()

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.warning(
            "APScheduler não instalado. Execute: pip install APScheduler"
        )
        return

    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler.add_job(
        _job_daily_check,
        trigger=CronTrigger(hour=hour, minute=minute),
        id="daily_alert_check",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Scheduler iniciado — job diário às %02d:%02d", hour, minute)


def stop_scheduler() -> None:
    """Para o scheduler se estiver rodando."""
    global _scheduler  # noqa: PLW0603
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        _scheduler = None


def restart_scheduler() -> None:
    """Relê a config e reinicia o scheduler conforme configuração salva."""
    from alertas.alert_engine import load_alert_rules

    rules_data = load_alert_rules()
    config = rules_data.get("config", {})
    sched_cfg = config.get("schedule", {})

    if sched_cfg.get("enabled"):
        hour = sched_cfg.get("hour", 8)
        init_scheduler(hour=hour)
    else:
        stop_scheduler()


def is_running() -> bool:
    """Retorna True se o scheduler está ativo."""
    return _scheduler is not None and _scheduler.running
