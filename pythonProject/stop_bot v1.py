# stop_bot.py
# Завершает все процессы бота (Python 3.8, psutil).
# Поиск по ключевым словам в командной строке и рабочей папке процесса.

import os
import sys
import time
import tempfile
from typing import List
import psutil

# Ключевые слова для идентификации процессов бота
def _default_keywords() -> List[str]:
    here = os.path.basename(os.path.abspath(os.getcwd()))
    # покрываем разные версии папок/скриптов
    return [here, "BeerMarketBot", "main.py", "telegram_bot.py", "aiogram"]

KEYWORDS = [k.lower() for k in _default_keywords()]

LOCK_FILE = os.getenv(
    "BOT_LOCK_FILE",
    os.path.join(tempfile.gettempdir(), "BeerMarketBot.lock")
)

def is_bot_process(p: psutil.Process) -> bool:
    try:
        if p.pid == os.getpid():
            return False
        name = (p.name() or "").lower()
        if not name.startswith("python"):  # python/pythonw и т.п.
            return False
        cmdline = " ".join(p.cmdline() or []).lower()
        cwd = (p.cwd() or "").lower()
        for kw in KEYWORDS:
            if kw and (kw in cmdline or kw in cwd):
                return True
        return False
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
        return False

def terminate_tree(p: psutil.Process) -> bool:
    """Аккуратно останавливаем процесс и его детей. Возвращаем True, если убили."""
    killed = False
    try:
        children = p.children(recursive=True)
        # Сначала дети
        for c in children:
            try:
                c.terminate()
            except Exception:
                pass
        _, alive = psutil.wait_procs(children, timeout=2.0)

        for c in alive:
            try:
                c.kill()
            except Exception:
                pass

        # Теперь сам процесс
        try:
            p.terminate()
        except Exception:
            pass
        try:
            p.wait(timeout=3.0)
            killed = True
        except Exception:
            try:
                p.kill()
                p.wait(timeout=2.0)
                killed = True
            except Exception:
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return killed

def remove_lock_file():
    try:
        if LOCK_FILE and os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            print(f"[OK] Удалён лок-файл: {LOCK_FILE}")
    except Exception as e:
        print(f"[WARN] Не удалось удалить лок-файл: {e}")

def main():
    # Собираем кандидатов
    candidates = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "cwd"]):
        if is_bot_process(p):
            candidates.append(p)

    if not candidates:
        print("Процессы бота не найдены.")
        remove_lock_file()
        return

    print("Найдены процессы бота:")
    for p in candidates:
        try:
            print(f" - PID {p.pid}: {p.name()} | {' '.join(p.cmdline() or [])}")
        except Exception:
            print(f" - PID {p.pid}")

    # Убиваем
    cnt = 0
    for p in candidates:
        if terminate_tree(p):
            cnt += 1

    print(f"[DONE] Завершено процессов: {cnt} из {len(candidates)}.")
    # Чуть подождём, затем удалим лок
    time.sleep(0.5)
    remove_lock_file()

if __name__ == "__main__":
    # Требуется psutil: pip install psutil
    main()
