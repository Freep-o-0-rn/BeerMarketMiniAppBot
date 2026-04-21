import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional


from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from tzlocal import get_localzone

try:
    from .parse_tara_report import (
        DEFAULT_BAD_PATH,
        DEFAULT_LOG_PATH,
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        DEFAULT_SKIPPED_PATH,
        DOWNLOADS_DIR,
        SERVICE_DIR,
        load_rules,
        parse_report,
        save_json,
        setup_logger,
    )
except ImportError:
    from parse_tara_report import (
        DEFAULT_BAD_PATH,
        DEFAULT_LOG_PATH,
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        DEFAULT_SKIPPED_PATH,
        DOWNLOADS_DIR,
        SERVICE_DIR,
        load_rules,
        parse_report,
        save_json,
        setup_logger,
    )


REPORT_NAME_HINT = "ведомость по переданной возвратной таре"
STATE_PATH = SERVICE_DIR / "tara_update_state.json"
RETRY_JOB_ID = "tara_refresh_retry"


def now_local_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def default_state() -> Dict[str, Any]:
    return {
        "last_processed_file": None,
        "last_processed_mtime": None,
        "last_processed_size": None,
        "last_processed_at": None,
        "last_check_at": None,
        "last_check_result": None,
        "last_missing_check_at": None,
        "last_missing_reason": None,
        "retry_scheduled_for": None,
    }


def load_update_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        state = default_state()
        save_update_state(state)
        return state

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    state = default_state()
    state.update(raw)
    return state


def save_update_state(state: Dict[str, Any]) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def file_signature(path: Path) -> Dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "name": path.name,
        "mtime": stat.st_mtime,
        "size": stat.st_size,
    }


def find_latest_tara_report() -> Path:
    candidates = []

    for path in DOWNLOADS_DIR.iterdir():
        if not path.is_file():
            continue

        if path.suffix.lower() not in {".xls", ".xlsx"}:
            continue

        if REPORT_NAME_HINT not in path.name.lower():
            continue

        candidates.append(path)

    if not candidates:
        raise FileNotFoundError(
            "В папке downloads не найден отчет по таре: {0}".format(DOWNLOADS_DIR)
        )

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def is_new_report(latest_file: Path, state: Dict[str, Any]) -> bool:
    sig = file_signature(latest_file)

    last_file = state.get("last_processed_file")
    last_mtime = state.get("last_processed_mtime")
    last_size = state.get("last_processed_size")

    if not last_file:
        return True

    if sig["path"] != last_file:
        return True

    if sig["mtime"] != last_mtime:
        return True

    if sig["size"] != last_size:
        return True

    return False


def clear_retry_job(scheduler: AsyncIOScheduler, logger) -> None:
    job = scheduler.get_job(RETRY_JOB_ID)
    if job:
        scheduler.remove_job(RETRY_JOB_ID)
        logger.info("Удалена ожидающая retry-задача")

    state = load_update_state()
    state["retry_scheduled_for"] = None
    save_update_state(state)


def schedule_retry_in_5_minutes(scheduler: AsyncIOScheduler, logger, reason: str) -> None:
    run_at = datetime.now() + timedelta(minutes=5)

    scheduler.add_job(
        run_tara_refresh_retry_async,
        trigger=DateTrigger(run_date=run_at, timezone=get_localzone()),
        id=RETRY_JOB_ID,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )

    state = load_update_state()
    state["retry_scheduled_for"] = run_at.strftime("%Y-%m-%d %H:%M:%S")
    state["last_missing_check_at"] = now_local_str()
    state["last_missing_reason"] = reason
    save_update_state(state)

    logger.warning(
        "Новый файл не найден. Повторная попытка запланирована на %s. Причина: %s",
        run_at.strftime("%Y-%m-%d %H:%M:%S"),
        reason,
    )


def update_state_success(report_path: Path) -> None:
    sig = file_signature(report_path)
    state = load_update_state()

    state["last_processed_file"] = sig["path"]
    state["last_processed_mtime"] = sig["mtime"]
    state["last_processed_size"] = sig["size"]
    state["last_processed_at"] = now_local_str()
    state["last_check_at"] = now_local_str()
    state["last_check_result"] = "updated"
    state["last_missing_check_at"] = None
    state["last_missing_reason"] = None
    state["retry_scheduled_for"] = None

    save_update_state(state)


def update_state_no_new_file(reason: str) -> None:
    state = load_update_state()
    state["last_check_at"] = now_local_str()
    state["last_check_result"] = "no_new_file"
    state["last_missing_check_at"] = now_local_str()
    state["last_missing_reason"] = reason
    save_update_state(state)


def update_state_error(reason: str) -> None:
    state = load_update_state()
    state["last_check_at"] = now_local_str()
    state["last_check_result"] = "error"
    state["last_missing_reason"] = reason
    save_update_state(state)


def process_tara_refresh(scheduler: AsyncIOScheduler, trigger_name: str) -> None:
    logger = setup_logger(str(DEFAULT_LOG_PATH))
    logger.info("Автообновление тары: старт, trigger=%s", trigger_name)

    try:
        rules = load_rules(str(DEFAULT_RULES_PATH))
        latest_report = find_latest_tara_report()
        state = load_update_state()

        logger.info("Найден самый свежий отчет: %s", latest_report)

        if not is_new_report(latest_report, state):
            reason = "Последний файл не изменился: {0}".format(latest_report.name)
            logger.warning(reason)
            update_state_no_new_file(reason)
            schedule_retry_in_5_minutes(scheduler, logger, reason)
            return

        logger.info("Обнаружен новый или измененный файл: %s", latest_report)

        result = parse_report(str(latest_report), logger, rules)

        save_json(str(DEFAULT_PARSED_PATH), result)
        save_json(str(DEFAULT_BAD_PATH), result["bad_clients"])
        save_json(str(DEFAULT_SKIPPED_PATH), result["skipped_rows"])

        update_state_success(latest_report)
        clear_retry_job(scheduler, logger)

        logger.info("Автообновление тары: успешно завершено")
        logger.info("JSON сохранен: %s", DEFAULT_PARSED_PATH)
        logger.info("BAD сохранен: %s", DEFAULT_BAD_PATH)
        logger.info("SKIPPED сохранен: %s", DEFAULT_SKIPPED_PATH)

    except FileNotFoundError as e:
        reason = str(e)
        logger.warning("Автообновление тары: %s", reason)
        update_state_no_new_file(reason)
        schedule_retry_in_5_minutes(scheduler, logger, reason)

    except Exception as e:
        reason = "Ошибка автообновления тары: {0}".format(e)
        logger.exception(reason)
        update_state_error(reason)
        schedule_retry_in_5_minutes(scheduler, logger, reason)


async def run_tara_refresh_async(trigger_name: str) -> None:
    scheduler = _scheduler_ref
    await asyncio.to_thread(process_tara_refresh, scheduler, trigger_name)


async def run_tara_refresh_retry_async() -> None:
    scheduler = _scheduler_ref
    await asyncio.to_thread(process_tara_refresh, scheduler, "retry_5min")


def register_tara_update_jobs(scheduler: AsyncIOScheduler) -> None:
    run_times = [
        (10, 31),
        (15, 31),
        (17, 1),
    ]

    for hour, minute in run_times:
        scheduler.add_job(
            run_tara_refresh_async,
            trigger=CronTrigger(
                hour=hour,
                minute=minute,
                timezone=get_localzone(),
            ),
            kwargs={"trigger_name": "scheduled_{0:02d}_{1:02d}".format(hour, minute)},
            id="tara_refresh_{0:02d}_{1:02d}".format(hour, minute),
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=1800,
        )


_scheduler_ref: Optional[AsyncIOScheduler] = None


def create_tara_scheduler() -> AsyncIOScheduler:
    global _scheduler_ref

    scheduler = AsyncIOScheduler(timezone=get_localzone())
    register_tara_update_jobs(scheduler)
    _scheduler_ref = scheduler

    if not STATE_PATH.exists():
        save_update_state(default_state())

    return scheduler