# config.py
import os
import re
import json
from typing import Dict, Any
from dotenv import load_dotenv, find_dotenv

# Загружаем .env, но приоритет ниже, чем у config.json (мы сами решим порядок)
load_dotenv(override=True)

# Путь к json-файлу с настраиваемыми параметрами
SETTINGS_PATH = os.getenv("SETTINGS_PATH", "settings/config.json")

# --- утилиты ---
def _clean_env_str(val: str) -> str:
    if val is None:
        return ""
    v = val.replace("\ufeff", "").strip()
    v = re.sub(r"\s+", "", v)
    return v

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _read_json() -> Dict[str, Any]:
    try:
        if os.path.exists(SETTINGS_PATH):
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def _write_json(cfg: Dict[str, Any]) -> None:
    _ensure_dir(SETTINGS_PATH)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

_cfg = _read_json()

def _get(key: str, default: str = "") -> str:
    """
    Порядок приоритета:
      1) settings/config.json (можно менять из бота)
      2) переменные окружения (.env / реальные ENV)
      3) default
    """
    if key in _cfg and _cfg[key] not in (None, ""):
        raw = str(_cfg[key])
    else:
        raw = os.getenv(key, default)
    # чистим только для тех ключей, где пробелы точно лишние
    if key in {"BOT_TOKEN", "IMAP_SERVER", "EMAIL_ACCOUNT", "IMAP_FOLDER",
               "SAVE_PATH", "LOG_LEVEL", "LOG_DIR"}:
        return _clean_env_str(str(raw))
    # пароли не трогаем (вдруг есть пробелы)
    return str(raw or "")

# --- публичные настройки (модульные переменные) ---
# Telegram
BOT_TOKEN = _get("BOT_TOKEN", "")

# IMAP
IMAP_SERVER    = _get("IMAP_SERVER", "imap.yandex.ru")
EMAIL_ACCOUNT  = _get("EMAIL_ACCOUNT", "")
EMAIL_PASSWORD = _get("EMAIL_PASSWORD", "")
IMAP_FOLDER    = _get("IMAP_FOLDER", "Debitor")

# Paths
SAVE_PATH = _get("SAVE_PATH", "downloads")

# Logging
LOG_LEVEL = _get("LOG_LEVEL", "INFO")
LOG_DIR   = _get("LOG_DIR", "logs")

def update_setting(key: str, value: str) -> None:
    """
    Обновляет значение в settings/config.json и в модульных переменных.
    Применение для уже запущенных компонентов не гарантируется (например, для BOT_TOKEN нужен рестарт бота).
    """
    global BOT_TOKEN, IMAP_SERVER, EMAIL_ACCOUNT, EMAIL_PASSWORD, IMAP_FOLDER, SAVE_PATH, LOG_LEVEL, LOG_DIR, _cfg

    # Обновляем JSON
    _cfg = _read_json()
    _cfg[str(key)] = value
    _write_json(_cfg)

    # Обновляем модульные переменные (чтобы в текущем процессе сразу было видно)
    if key == "BOT_TOKEN":
        BOT_TOKEN = _get("BOT_TOKEN", "")
    elif key == "IMAP_SERVER":
        IMAP_SERVER = _get("IMAP_SERVER", "imap.yandex.ru")
    elif key == "EMAIL_ACCOUNT":
        EMAIL_ACCOUNT = _get("EMAIL_ACCOUNT", "")
    elif key == "EMAIL_PASSWORD":
        EMAIL_PASSWORD = _get("EMAIL_PASSWORD", "")
    elif key == "IMAP_FOLDER":
        IMAP_FOLDER = _get("IMAP_FOLDER", "Debitor")
    elif key == "SAVE_PATH":
        SAVE_PATH = _get("SAVE_PATH", "downloads")
    elif key == "LOG_LEVEL":
        LOG_LEVEL = _get("LOG_LEVEL", "INFO")
    elif key == "LOG_DIR":
        LOG_DIR = _get("LOG_DIR", "logs")
    # Также поднимем в окружение для совместимости с кодом, который читает os.getenv
    os.environ[str(key)] = str(value)
