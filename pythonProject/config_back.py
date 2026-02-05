# config.py
import os, json, re
from dotenv import load_dotenv

load_dotenv(override=True)

SETTINGS_PATH = "settings/config.json"

def _read_json():
    try:
        if os.path.exists(SETTINGS_PATH):
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_json(cfg):
    os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

_cfg = _read_json()

def _get(key: str, default: str = "") -> str:
    v = os.getenv(key, _cfg.get(key, default))
    if v is None:
        return ""
    return re.sub(r"\s+", "", v.strip())

def _set(key: str, value: str):
    _cfg[key] = value
    _save_json(_cfg)

# Telegram
BOT_TOKEN = _get("BOT_TOKEN")

# IMAP
IMAP_SERVER   = _get("IMAP_SERVER", "imap.yandex.ru")
EMAIL_ACCOUNT = _get("EMAIL_ACCOUNT", "")
EMAIL_PASSWORD= _get("EMAIL_PASSWORD", "")
IMAP_FOLDER   = _get("IMAP_FOLDER", "Debitor")

# Доп. методы для обновления
def update_setting(key: str, value: str):
    _set(key, value)
