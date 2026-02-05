# -*- coding: utf-8 -*-
"""
stop_bot.py — завершает все процессы BeerMarketBot и удаляет lock-файл.
Python 3.8+, psutil.

Логика:
1) Если существует lock-файл (по умолчанию %TEMP%/BeerMarketBot.lock) — читаем PID и завершаем этот процесс.
   Lock-файл удаляем (в т.ч. «битый»).
2) Дополнительно ищем процессы по шаблону (по умолчанию "BeerMarketBot") в name/cmdline/cwd/exe и завершаем.

Параметры:
  --lock     путь к lock-файлу (по умолчанию из env BOT_LOCK_FILE или %TEMP%/BeerMarketBot.lock)
  --pattern  подстрока для поиска процессов (по умолчанию из env BOT_PROCESS_PATTERN или "BeerMarketBot")
  --dry-run  только показать, какие процессы будут завершены (ничего не завершаем)
"""

import os
import sys
import psutil
import tempfile
import argparse

DEFAULT_LOCK = os.getenv("BOT_LOCK_FILE", os.path.join(tempfile.gettempdir(), "BeerMarketBot.lock"))
DEFAULT_PATTERN = os.getenv("BOT_PROCESS_PATTERN", "BeerMarketBot")


def kill_pid(pid: int, reason: str, dry: bool = False) -> bool:
    try:
        p = psutil.Process(pid)
    except psutil.NoSuchProcess:
        print(f"[skip] PID {pid} не существует ({reason})")
        return False

    if dry:
        print(f"[dry] Завершил бы PID {pid} {p.name()} ({reason})")
        return False

    print(f"[try] Завершаю PID {pid} {p.name()} ({reason})")
    try:
        p.terminate()
        p.wait(5)
        print(f"[ok] PID {pid} завершён мягко")
        return True
    except psutil.TimeoutExpired:
        try:
            p.kill()
            p.wait(3)
            print(f"[ok] PID {pid} убит принудительно")
            return True
        except psutil.TimeoutExpired:
            print(f"[warn] PID {pid} не удалось завершить")
            return False
    except (psutil.AccessDenied, psutil.Error) as e:
        print(f"[warn] Не удалось завершить PID {pid}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser(description="Остановить все процессы BeerMarketBot")
    ap.add_argument("--lock", default=DEFAULT_LOCK, help="Путь к lock-файлу (если иной)")
    ap.add_argument("--pattern", default=DEFAULT_PATTERN, help="Подстрока для поиска процессов")
    ap.add_argument("--dry-run", action="store_true", help="Только показать найденные процессы")
    args = ap.parse_args()

    dry = args.dry_run
    killed = set()

    # 1) По lock-файлу
    if os.path.exists(args.lock):
        pid = None
        try:
            with open(args.lock, "r", encoding="utf-8") as f:
                pid_txt = (f.read() or "").strip()
            pid = int(pid_txt) if pid_txt.isdigit() else None
        except Exception:
            pid = None

        if pid and psutil.pid_exists(pid):
            if kill_pid(pid, f"lock {args.lock}", dry=dry):
                killed.add(pid)

        # Удаляем lock (и «битый»)
        try:
            if not dry:
                os.remove(args.lock)
            print(f"[ok] Lock удалён: {args.lock}")
        except Exception as e:
            print(f"[warn] Не удалось удалить lock {args.lock}: {e}")

    # 2) Поиск по шаблону
    patt = (args.pattern or "").lower()
    if patt:
        for p in psutil.process_iter(["pid", "name", "cmdline", "cwd", "exe"]):
            try:
                pid = p.info["pid"]
                if pid in killed or pid == os.getpid():
                    continue
                name = (p.info.get("name") or "").lower()
                cmd = " ".join(p.info.get("cmdline") or []).lower()
                cwd = (p.info.get("cwd") or "").lower()
                exe = (p.info.get("exe") or "").lower()
                hay = " ".join([name, cmd, cwd, exe])
                if patt and (patt in hay):
                    if kill_pid(pid, f"pattern '{args.pattern}'", dry=dry):
                        killed.add(pid)
                    elif dry:
                        # уже напечатано в kill_pid(dry=True)
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    if not killed and not dry:
        print("[info] Ничего не завершено. Возможно, бот не запущен.")
    elif dry:
        print("[dry] Завершение не выполнялось (dry-run).")


if __name__ == "__main__":
    main()
