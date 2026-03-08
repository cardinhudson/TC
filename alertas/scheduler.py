"""Agendamento automático de alertas diários via APScheduler."""

from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)

_scheduler = None

_DAY_OF_WEEK_MAP = {
    "Segunda": "mon",
    "Terca": "tue",
    "Quarta": "wed",
    "Quinta": "thu",
    "Sexta": "fri",
    "Sabado": "sat",
    "Domingo": "sun",
}


def _job_daily_check() -> None:
    """Job executado diariamente — chama run_daily_check()."""
    try:
        from alertas.alert_engine import run_daily_check
        logger.info("Executando verificação diária de alertas...")
        alertas = run_daily_check()
        logger.info("Verificação diária concluída — %d alertas gerados.", len(alertas))
    except Exception:
        logger.exception("Erro na verificação diária de alertas.")


def _job_rule_check(rule_id: str) -> None:
    """Executa uma regra individual agendada."""
    try:
        from alertas.alert_engine import load_alert_rules, run_rule_check
        rules_data = load_alert_rules()
        rule = next(
            (item for item in rules_data.get("rules", []) if item.get("id") == rule_id),
            None,
        )
        if not rule or not _schedule_matches_date(rule.get("schedule", {}), date.today()):
            return
        logger.info("Executando regra agendada: %s", rule_id)
        run_rule_check(rule_id)
    except Exception:
        logger.exception("Erro na execucao agendada da regra %s.", rule_id)


def _schedule_matches_date(schedule: dict, data_ref: date) -> bool:
    """Valida regras que precisam de filtro adicional alem do CronTrigger."""
    if not schedule.get("enabled", False):
        return False

    frequency = schedule.get("frequency", "daily")
    if frequency == "daily":
        return data_ref.day >= int(schedule.get("start_day_of_month", 1))
    return True


def _build_rule_trigger_kwargs(schedule: dict) -> dict:
    """Converte o schedule da regra em kwargs de CronTrigger."""
    kwargs = {
        "hour": int(schedule.get("hour", 8)),
        "minute": int(schedule.get("minute", 0)),
    }
    frequency = schedule.get("frequency", "daily")
    if frequency == "weekly":
        days = schedule.get("days_of_week", [])
        if days:
            kwargs["day_of_week"] = ",".join(
                _DAY_OF_WEEK_MAP.get(day, str(day).lower()) for day in days
            )
    elif frequency == "monthly":
        days = schedule.get("days_of_month", [])
        if days:
            kwargs["day"] = ",".join(str(int(day)) for day in days)
    return kwargs


def _iter_rule_job_specs(rules_data: dict) -> list[dict]:
    """Retorna os jobs a registrar a partir das regras ativas."""
    config = rules_data.get("config", {})
    if not config.get("schedule", {}).get("enabled", False):
        return []

    jobs = []
    for rule in rules_data.get("rules", []):
        if not rule.get("ativo", True):
            continue
        schedule = rule.get("schedule", {})
        if not schedule.get("enabled", False):
            continue
        jobs.append({
            "id": f"rule_alert_check_{rule.get('id', '')}",
            "rule_id": rule.get("id", ""),
            "trigger_kwargs": _build_rule_trigger_kwargs(schedule),
        })
    return jobs


def init_scheduler(
    hour: int = 8,
    minute: int = 0,
    rules_data: dict | None = None,
) -> None:
    """Inicia o scheduler em modo legado ou com jobs por regra."""
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
    if rules_data is None:
        _scheduler.add_job(
            _job_daily_check,
            trigger=CronTrigger(hour=hour, minute=minute),
            id="daily_alert_check",
            replace_existing=True,
        )
    else:
        for job in _iter_rule_job_specs(rules_data):
            _scheduler.add_job(
                _job_rule_check,
                trigger=CronTrigger(**job["trigger_kwargs"]),
                args=[job["rule_id"]],
                id=job["id"],
                replace_existing=True,
            )
    _scheduler.start()
    logger.info("Scheduler iniciado")


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
    if config.get("schedule", {}).get("enabled"):
        init_scheduler(rules_data=rules_data)
    else:
        stop_scheduler()


def is_running() -> bool:
    """Retorna True se o scheduler está ativo."""
    return _scheduler is not None and _scheduler.running
