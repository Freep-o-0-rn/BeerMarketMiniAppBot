# -*- coding: utf-8 -*-

import argparse
import os
import atexit
import tempfile
import signal
import asyncio
import logging
import sys
import openpyxl
import subprocess
import importlib
from typing import Optional
from telegram_bot import run_bot, set_last_update


# --- До импортов всего остального: загрузка .env и настройка петли ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# .env — грузим как можно раньше
try:
    from dotenv import load_dotenv, find_dotenv
except ModuleNotFoundError:
    # Если даже dotenv не установлен — ставим его и продолжаем
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-dotenv"])
    from dotenv import load_dotenv, find_dotenv


# ----------------- АВТО-УСТАНОВКА ЗАВИСИМОСТЕЙ -----------------
REQ_FILE = os.path.join(os.getcwd(), "requirements.txt")

def _pip_install(args):
    """Вызов pip с выводом лога."""
    logging.getLogger(__name__).info("[DEPS] pip install %s", " ".join(args))
    subprocess.check_call([sys.executable, "-m", "pip", "install", *args])

def ensure_module(mod_name: str, *, pip_name: Optional[str] = None):
    """
    Пытаемся импортнуть модуль; если не вышло — ставим зависимости и пробуем снова.
    Сначала пробуем requirements.txt (если есть), затем точечную установку пакета.
    """
    try:
        return importlib.import_module(mod_name)
    except ModuleNotFoundError:
        # 1) requirements.txt (если есть)
        if os.path.exists(REQ_FILE):
            try:
                _pip_install(["-r", REQ_FILE])
                return importlib.import_module(mod_name)
            except Exception:
                pass
        # 2) точечная установка
        name = pip_name or mod_name
        _pip_install([name])
        return importlib.import_module(mod_name)

# Эти модули могут часто отсутствовать — страхуемся:
psutil = ensure_module("psutil")
aiogram_utils_token = ensure_module("aiogram.utils.token", pip_name="aiogram")
aiogram_exceptions = ensure_module("aiogram.exceptions", pip_name="aiogram")

# Остальные импорты после ensure_module:
from aiogram.utils.token import validate_token, TokenValidationError
from config import BOT_TOKEN
from mail_agent import fetch_latest_file
from file_processor import find_latest_download, process_file, process_tara_file
from file_processor import _fix_missing_sharedstrings_via_zip
from telegram_bot import run_bot, set_last_update  # set_last_update для метки "manual"

# ----------------- ЛОГИ -----------------
def setup_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO),
                        format=fmt, datefmt=datefmt)
    # файл-лог, если указан LOG_DIR
    log_dir = os.getenv("LOG_DIR")
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            fh = logging.FileHandler(os.path.join(log_dir, "app.log"), encoding="utf-8")
            fh.setLevel(getattr(logging, log_level, logging.INFO))
            fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
            logging.getLogger().addHandler(fh)
        except Exception as e:
            logging.getLogger(__name__).warning("Не удалось создать файл-лог: %s", e)

logger = logging.getLogger(__name__)

# ----------------- SINGLE INSTANCE LOCK -----------------
LOCK_FILE = os.getenv("BOT_LOCK_FILE", os.path.join(tempfile.gettempdir(), "BeerMarketBot.lock"))

def _read_lock_pid() -> Optional[int]:
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                pid_txt = (f.read() or "").strip()
                return int(pid_txt) if pid_txt.isdigit() else None
    except Exception:
        return None
    return None

def acquire_single_instance_lock() -> None:
    """Один экземпляр процесса."""
    pid = _read_lock_pid()
    if pid and psutil.pid_exists(pid):
        logger.info("[LOCK] Bot already running (PID %s). Exit.", pid)
        sys.exit(0)
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass
    with open(LOCK_FILE, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))
    logger.debug("Lock acquired: %s", LOCK_FILE)

def release_single_instance_lock() -> None:
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.debug("Lock released: %s", LOCK_FILE)
    except Exception:
        pass

# ----------------- КОМАНДЫ -----------------
def cmd_check_token() -> None:
    try:
        validate_token(BOT_TOKEN)
        logger.info("BOT_TOKEN валиден.")
    except TokenValidationError:
        masked = f"{BOT_TOKEN[:6]}...{BOT_TOKEN[-6:]}" if BOT_TOKEN else "<empty>"
        logger.error("BOT_TOKEN НЕ валиден. Сейчас вижу: %s (len=%s)", masked, len(BOT_TOKEN) if BOT_TOKEN else 0)
        raise SystemExit(2)

def cmd_fetch() -> None:
    subject = os.getenv("MAIL_SUBJECT", "ДЕБИТОРКА")
    try:
        path = fetch_latest_file(subject)
        if path:
            logger.info("Файл получен: %s", path)
            # считаем это ручным обновлением (из CLI)
            try:
                set_last_update("manual")
            except Exception:
                pass
        else:
            logger.warning("Письмо не найдено или вложения не подошли.")
    except Exception as e:
        logger.exception("Ошибка fetch: %s", e)
        raise SystemExit(3)

def cmd_report() -> None:
    path = find_latest_download()
    if not path:
        logger.warning("Файл не найден в папке downloads/. Сначала выполните fetch.")
        return
    try:
        res = process_file(path)
    except Exception as e:
        logger.exception("Ошибка разбора файла: %s", e)
        return
    items = res.get("items", [])
    rd = res.get("report_date") or "-"
    logger.info("Отчёт на %s | клиентов: %s", rd, len(items))
    for i, it in enumerate(items[:10], 1):
        total = it.get('total_amount', 0.0) or 0.0
        overdue = it.get('overdue_amount', 0.0) or 0.0
        our = it.get('our_debt', 0.0) or 0.0
        logger.info("%02d. %s | сум: %.2f | проср: %.2f | наш долг: %.2f | доков: %s",
                    i, it.get('client', '-'), total, overdue, our, it.get('realizations_count', 0))

async def cmd_bot() -> None:
    await run_bot()

def cmd_stop() -> None:
    """Остановить ранее запущенного бота по PID в lock-файле."""
    pid = _read_lock_pid()
    if not pid:
        logger.info("Lock-файл не найден — нечего останавливать.")
        return
    if not psutil.pid_exists(pid):
        logger.info("Процесс %s уже не существует. Чищу lock.", pid)
        release_single_instance_lock()
        return
    try:
        p = psutil.Process(pid)
        logger.info("Останавливаю процесс %s (%s)...", pid, " ".join(p.cmdline() or []))
        # Сигналом помягче
        try:
            if hasattr(signal, "SIGINT"):
                p.send_signal(signal.SIGINT)
        except Exception:
            pass
        p.terminate()
        try:
            p.wait(timeout=5)
        except psutil.TimeoutExpired:
            logger.warning("Не завершился за 5с — kill()")
            p.kill()
        release_single_instance_lock()
        logger.info("Остановлен.")
    except Exception as e:
        logger.exception("Не удалось остановить процесс %s: %s", pid, e)

# ----------------- MAIN -----------------
def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="BeerMarkenBot — главная точка входа")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("bot", help="Запустить Telegram-бота")
    sub.add_parser("fetch", help="Забрать последний отчёт из почты")
    sub.add_parser("report", help="Быстрый консольный отчёт по последнему файлу")
    sub.add_parser("check-token", help="Проверить валидность BOT_TOKEN")
    sub.add_parser("stop", help="Остановить ранее запущенного бота (по lock-файлу)")
    sub.add_parser("all", help="Последовательно: check-token → fetch → report → bot")

    args = parser.parse_args()
    command = args.command or "all"

    # singleton lock — только для режимов, где действительно запускаем бота
    if command in ("bot", "all"):
        acquire_single_instance_lock()
        atexit.register(release_single_instance_lock)

    try:
        if command == "check-token":
            cmd_check_token()
        elif command == "fetch":
            cmd_fetch()
        elif command == "report":
            cmd_report()
        elif command == "stop":
            cmd_stop()
        elif command == "bot":
            cmd_check_token()
            asyncio.run(cmd_bot())
        elif command == "all":
            cmd_check_token()
            cmd_fetch()
            cmd_report()
            asyncio.run(cmd_bot())
        else:
            parser.print_help()
    finally:
        # если бот не запускался — чистим lock на всякий случай
        if command not in ("bot", "all"):
            release_single_instance_lock()

if __name__ == "__main__":
    main()
