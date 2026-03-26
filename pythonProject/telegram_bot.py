import asyncio
import logging
import html as _html
import ssl
import hmac
from aiogram.types import FSInputFile  # aiogram v3
import uuid
import io
import calendar as _cal
import contextlib
from aiogram.types import InputMediaPhoto
import aiohttp
import time
import os
import re
import json
from io import BytesIO
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from aiogram.fsm.state import StatesGroup, State
from aiogram import BaseMiddleware
from pathlib import Path
from hashlib import md5, sha256
from urllib.parse import quote_plus
from datetime import datetime, date, timedelta
import pytz
from aiogram.types import Message, CallbackQuery
import openpyxl
from aiohttp import ClientSession, ClientTimeout, web
from dotenv import load_dotenv
load_dotenv(override=True)
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram import F
from aiogram.filters import StateFilter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardRemove,
    User,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.token import validate_token, TokenValidationError
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest  # retry на флуд-контроль
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import StatesGroup, State
from config import BOT_TOKEN, update_setting
from file_processor import process_file, find_latest_download, process_tara_file, find_latest_downloads, read_debt_file, parse_clients, split_report_client_label
from mail_agent import fetch_latest_file
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram import BaseMiddleware
from typing import Optional, Tuple, Dict, Any
from client_cards_db import ClientCardsDB, format_client_card, DEFAULT_POSITIONS, _split_addresses


ROOT_DIR = Path(__file__).resolve().parent
SETTINGS_DIR = ROOT_DIR / "settings"
CLIENTS_DB = ClientCardsDB(SETTINGS_DIR / "clients.sqlite3")

logger = logging.getLogger(__name__)

#Прайсы: сортировка по алфавиту ---
PRICES_SORT_ALPHA = True   # вырубить — поставьте False
#акции
PROMO_DIR = Path("promos")
PROMO_DIR.mkdir(parents=True, exist_ok=True)
PROMO_INDEX = PROMO_DIR / "promos.json"
PROMO_PAGE_SIZE = 8
ALLOWED_PROMO_IMG = {"jpg","jpeg","png","webp"}
ALLOWED_PROMO_DOC = {"pdf"}  # документ (отправим как файл)
NEWS_INDEX = ROOT_DIR / "news.json"
NEWS_CATEGORIES = {"Новость", "Обновление", "Акция", "Сервис"}
#календарь
_RU_MONTHS = ["", "Январь","Февраль","Март","Апрель","Май","Июнь",
              "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
_RU_DOW = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
# --- logging setup -----------------------------------------------------------
import logging, json, os, time, uuid
from logging.handlers import RotatingFileHandler

LOG_DIR    = os.getenv("LOG_DIR", "logs")
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON   = os.getenv("LOG_JSON", "1") in ("1", "true", "yes")

os.makedirs(LOG_DIR, exist_ok=True)

class JSONLineFormatter(logging.Formatter):
    """Если msg — dict, логируем одну JSON-строку. Иначе обычный формат."""
    def format(self, record: logging.LogRecord) -> str:
        if isinstance(record.msg, dict):
            return json.dumps(record.msg, ensure_ascii=False, separators=(",", ":"))
        return super().format(record)

def setup_logging():
    root = logging.getLogger()
    if root.handlers:  # чтобы не дублировать при повторных запусках
        return
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Тех. логи (в файл + консоль)
    tech_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_h = RotatingFileHandler(os.path.join(LOG_DIR, "bot.log"),
                                 maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    file_h.setFormatter(tech_fmt)
    root.addHandler(file_h)

    cons_h = logging.StreamHandler()
    cons_h.setFormatter(tech_fmt)
    root.addHandler(cons_h)

    # Аудит-лог (отдельный logger с JSON-строками)
    audit_logger = logging.getLogger("audit")
    audit_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    audit_h = RotatingFileHandler(os.path.join(LOG_DIR, "audit.log"),
                                  maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    audit_h.setFormatter(JSONLineFormatter())
    audit_logger.addHandler(audit_h)

setup_logging()
logger = logging.getLogger(__name__)
AUDIT = logging.getLogger("audit")
# ---------------------------------------------------------------------------


# --- Валидация токена при импорте ---
try:
    validate_token(BOT_TOKEN)
except TokenValidationError:
    masked = f"{BOT_TOKEN[:6]}...{BOT_TOKEN[-6:]}" if BOT_TOKEN else "<empty>"
    raise SystemExit(
        f"BOT_TOKEN не прошёл валидацию.\n"
        f"Сейчас вижу: {masked} (len={len(BOT_TOKEN) if BOT_TOKEN else 0}). Проверь settings/config.json или .env."
    )

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router(name="root")
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

# --- Настройки ---
OVERDUE_DAYS_DEFAULT = int(os.getenv("OVERDUE_DAYS_DEFAULT", "7"))
IMPORTED_CLIENT_OVERDUE_DAYS_DEFAULT = 7
CLIENT_OVERDUE_JSON = os.getenv("CLIENT_OVERDUE_JSON", "settings/client_overdue_days.json")
MIN_DEBT_JSON = os.getenv("MIN_DEBT_JSON", "settings/filters.json")
MAX_TG = 3900

TZ = pytz.timezone(os.getenv("TZ", "Europe/Berlin"))
CRON_TIMES = [(10, 31), (15, 31)]
MAIL_SUBJECT = os.getenv("MAIL_SUBJECT", "ДЕБИТОРКА")
LAST_UPDATE_FILE = os.getenv("LAST_UPDATE_FILE", os.path.join("downloads", ".last_update.json"))

# Роли/онбординг
USER_ROLES_JSON = os.getenv("USER_ROLES_JSON", "settings/user_roles.json")
ROLE_DEFS_JSON = os.getenv("ROLE_DEFS_JSON", "settings/roles.json")
ADMIN_ONBOARD_PASSWORD = os.getenv("ADMIN_ONBOARD_PASSWORD", "99654511")
LEGACY_USER_ROLES_JSON = os.path.join(os.getcwd(), "user_roles.json")

#Константы/пути (в раздел настроек)
PRICES_DIR = Path(os.getenv("PRICES_DIR", "Price"))
PRICES_INDEX = PRICES_DIR / "prices.json"
PRICES_PAGE_SIZE = 10
ALLOWED_PRICE_EXT = {"pdf","xls","xlsx","png","jpg","jpeg"}
PRICES_INDEX.parent.mkdir(parents=True, exist_ok=True)


_ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())

# --- FSM states ---
class SearchStates(StatesGroup):
    waiting_query = State()

class SearchTaraStates(StatesGroup):
    waiting_query = State()

class OverdueSetStates(StatesGroup):
    waiting_key = State()
    waiting_days = State()

class OverdueEditStates(StatesGroup):
    waiting_days = State()

class OverdueDelStates(StatesGroup):
    waiting_key = State()

class FilterStates(StatesGroup):
    wait_value = State()

class FilterSetState(StatesGroup):
    waiting_value = State()

class ConfigStates(StatesGroup):
    waiting_bot_token = State()
    waiting_imap_server = State()
    waiting_email_account = State()
    waiting_email_password = State()

# NEW: онбординг и клиентское имя
class OnboardStates(StatesGroup):
    waiting_role = State()
    waiting_admin_password = State()
    waiting_client_name = State()
    waiting_phone_contact = State()

class ClientEditStates(StatesGroup):
    waiting_new_name = State()

class ClientCardStates(StatesGroup):
    waiting_legal_form = State()
    waiting_legal_name = State()
    waiting_store_name = State()
    waiting_address = State()
    waiting_overdue_days = State()
    waiting_contact_name = State()
    waiting_contact_phone = State()
    waiting_contact_position = State()
    waiting_more_contacts = State()
    waiting_technician_select = State()
    waiting_sales_rep = State()
    waiting_network_name = State()
    waiting_additional_contact_name = State()
    waiting_additional_contact_phone = State()
    waiting_additional_contact_position = State()
    waiting_edit_value = State()

class TechnicianStates(StatesGroup):
    waiting_full_name = State()
    waiting_phone = State()
    waiting_points = State()

class PriceStates(StatesGroup):
    waiting_new_title = State()
    waiting_new_file  = State()
    waiting_replace_file = State()
    waiting_rename = State()
    waiting_delete_confirm = State()

#состояния
class PromoStates(StatesGroup):
    waiting_promo_title = State()
    waiting_promo_text = State()
    waiting_promo_media = State()
    waiting_promo_dates_new = State()
    waiting_promo_dates_edit = State()
    waiting_promo_edit_text = State()
    waiting_promo_replace_img = State()
    waiting_promo_replace_doc = State()
    waiting_promo_rename = State()             # было: waiting_rename
    waiting_promo_delete_confirm = State()

class TTNStates(StatesGroup):
    waiting_number = State()

#Состояния для админских действий
class ScheduleStates(StatesGroup):
    waiting_photo = State()
    waiting_text = State()

class AdminUserEditStates(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_delete_confirm = State()

@dataclass
class Promo:
    id: str
    title: str
    text: str
    image: Optional[str]
    doc: Optional[str]
    starts_at: Optional[str]  # ISO YYYY-MM-DD
    ends_at: Optional[str]  # ISO YYYY-MM-DD
    active: bool
    created_at: str
    updated_at: str


#----------Модель/хранилище-----------
@dataclass
class PriceItem:
    id: str
    title: str
    filename: str       # относительный путь в Price/
    created_at: str
    updated_at: str


#----------------------------конец классы ---


def _prices_load() -> List[Dict[str, Any]]:
    if PRICES_INDEX.exists():
        try:
            return json.loads(PRICES_INDEX.read_text(encoding="utf-8")) or []
        except Exception:
            return []
    return []

def _prices_save(items: List[Dict[str, Any]]) -> None:
    PRICES_INDEX.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

# --- Прайсы: сортировка по алфавиту ---
def _ru_norm(s: str) -> str:
    # для «человеческой» сортировки по-русски:
    # игнор регистра + считаем Ё как Е
    return (s or "").strip().replace("Ё", "Е").replace("ё", "е")

def _price_title_key(it: Dict[str, Any]) -> str:
    return _ru_norm(it.get("title", "")).casefold()


def _price_get_all() -> List[Dict[str, Any]]:
    items = _prices_load()
    out: List[Dict[str, Any]] = []
    for it in items:
        f = PRICES_DIR / it.get("filename", "")
        if f.exists():
            out.append(it)

    if PRICES_SORT_ALPHA:
        out.sort(key=_price_title_key)   # <— новая строка

    return out

def _price_find(pid: str) -> Optional[Dict[str, Any]]:
    for it in _prices_load():
        if it.get("id") == pid:
            return it
    return None

def _price_set(item: Dict[str, Any]) -> None:
    items = _prices_load()
    for i, it in enumerate(items):
        if it.get("id") == item["id"]:
            items[i] = item
            _prices_save(items)
            return
    items.append(item)
    _prices_save(items)

def _price_delete(pid: str) -> None:
    items = [it for it in _prices_load() if it.get("id")!=pid]
    _prices_save(items)
#--------------------------------------


# --- Форматирование/утилиты ---
def fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(x)

def money0(x) -> float:
    if x in (None, "", "—"):
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0
#Форматтер количества и форматтер блока тары
def fmt_qty0(x: float) -> str:
    """Целые количества с разделителем тысяч: 5,000"""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)

def fmt_qty_units(x):
    """Округляем и добавляем 'шт.'"""
    try:
        return f"{int(round(float(x)))} шт."
    except Exception:
        return str(x)

def build_tara_text(b: Dict[str, Any]) -> str:
    """
    b = {"client": str, "total": float, "items": [(name:str, qty:float), ...]}
    """
    lines = [
        f"<b>{esc(b['client'])}</b>",
        f"всего: {fmt_qty_units(b.get('total', 0))}"
    ]
    for name, qty in (b.get("items") or []):
        lines.append(f"{esc(name)} — {fmt_qty_units(qty)}")
    return "\n".join(lines)

# ==================== Телефоны клиентов ===================
# ==================== Телефоны клиентов ====================
import os, json, re
from pathlib import Path
from hashlib import md5
from typing import Optional, Tuple, Dict, Any, List

# используем единый файл ролей/имен/телефонов
USER_ROLES_PATH = Path(USER_ROLES_JSON)
ROLE_DEFS_PATH = Path(ROLE_DEFS_JSON)

DEFAULT_ROLE_DEFS: Dict[str, Dict[str, Any]] = {
    "guest": {
        "label": "Гость",
        "description": "Ограниченный доступ до назначения роли администратором.",
        "permissions": [
            "view_prices",
        ],
    },
    "admin": {
        "label": "Администратор",
        "description": "Полный доступ к управлению ботом, пользователями, прайсами и акциями.",
        "permissions": [
            "admin",
            "manage_users",
            "manage_prices",
            "manage_promos",
            "manage_schedule",
            "manage_settings",
            "refresh_data",
            "view_reports",
            "view_ttn",
        ],
    },
    "client": {
        "label": "Клиент",
        "description": "Доступ к просмотру своих данных, прайсов и графика.",
        "permissions": [
            "view_prices",
            "view_promos",
            "view_schedule",
        ],
    },
    "sales_rep": {
        "label": "Торговый представитель",
        "description": "Работает со своими клиентами и их карточками.",
        "permissions": [
            "view_prices",
            "view_promos",
            "view_schedule",
            "view_reports",
            "view_ttn",
            "manage_clients",
        ],
    },
    "moderator": {
        "label": "Модератор",
        "description": "Расширенный просмотр данных без доступа к админским настройкам.",
        "permissions": [
            "view_prices",
            "view_promos",
            "view_schedule",
            "view_reports",
            "view_ttn",
            "view_clients",
            "receive_notifications",
        ],
    },
}


def _ensure_file_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _normalize_role_defs(data: dict) -> Dict[str, Dict[str, Any]]:
    data = data if isinstance(data, dict) else {}
    merged: Dict[str, Dict[str, Any]] = {}
    for role, defaults in DEFAULT_ROLE_DEFS.items():
        merged[role] = dict(defaults)
    for role, payload in data.items():
        if not isinstance(role, str) or not isinstance(payload, dict):
            continue
        current = merged.get(role, {})
        merged[role] = {
            "label": payload.get("label") or current.get("label") or role,
            "description": payload.get("description") or current.get("description") or "",
            "permissions": list(payload.get("permissions") or current.get("permissions") or []),
        }
    return merged

def _role_defs_load() -> Dict[str, Dict[str, Any]]:
    _ensure_file_parent(ROLE_DEFS_PATH)
    try:
        raw = ROLE_DEFS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw) if raw else {}
    except FileNotFoundError:
        data = {}
    except Exception:
        data = {}
    merged = _normalize_role_defs(data)
    if not ROLE_DEFS_PATH.exists():
        ROLE_DEFS_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    return merged

_ROLE_DEFS = _role_defs_load()

def _role_defs_reload() -> Dict[str, Dict[str, Any]]:
    global _ROLE_DEFS
    _ROLE_DEFS = _role_defs_load()
    return _ROLE_DEFS

def get_role_def(role: Optional[str]) -> Dict[str, Any]:
    key = (role or "guest").strip().lower()
    return _ROLE_DEFS.get(key) or _ROLE_DEFS.get("guest", {})

def get_role_permissions(role: Optional[str]) -> set:
    return set(get_role_def(role).get("permissions") or [])

def normalize_role(role: Optional[str]) -> str:
    key = (role or "guest").strip().lower()
    if key in _ROLE_DEFS:
        return key
    return "guest"

def role_label(role: Optional[str]) -> str:
    return str(get_role_def(role).get("label") or role or "guest")

def user_has_permission(user_id: Optional[int], permission: str) -> bool:
    return permission in get_role_permissions(get_user_role(user_id))

def _normalize_access_overrides(value: Any) -> Dict[str, bool]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, bool] = {}
    for key, enabled in value.items():
        if isinstance(key, str) and isinstance(enabled, bool):
            out[key] = enabled
    return out

def _normalize_notification_settings(value: Any) -> Dict[str, bool]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, bool] = {}
    for key, enabled in value.items():
        if isinstance(key, str) and isinstance(enabled, bool):
            out[key] = enabled
    return out

def _normalize_user_roles_schema(data: dict) -> dict:
    """Гарантируем структуру и не трогаем неизвестные ключи."""
    data = (data or {})
    # телефоны клиентов — словарь
    if not isinstance(data.get("client_phones"), dict):
        data["client_phones"] = {}
    # записи по user_id должны быть словарями; если вдруг строка — мигрируем
    for k, v in list(data.items()):
        if k == "client_phones":
            continue
        if not isinstance(v, dict):
            data[k] = {"role": "guest", "name": str(v)}
        else:
            v["role"] = normalize_role(v.get("role"))
            v["access_overrides"] = _normalize_access_overrides(v.get("access_overrides"))
            v["notification_settings"] = _normalize_notification_settings(v.get("notification_settings"))
    return data

def _roles_load() -> dict:
    _ensure_file_parent(USER_ROLES_PATH)
    candidates = [USER_ROLES_PATH]
    legacy_path = Path(LEGACY_USER_ROLES_JSON)
    if legacy_path != USER_ROLES_PATH:
        candidates.append(legacy_path)
    backup_path = USER_ROLES_PATH.with_suffix(USER_ROLES_PATH.suffix + ".bak")
    candidates.append(backup_path)
    try:
        data = {}
        for path in candidates:
            if not path.exists():
                continue
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw else {}
            if data:
                break
    except FileNotFoundError:
        data = {}
    except Exception:
        data = {}
    return _normalize_user_roles_schema(data)

def _roles_save_atomic(data: dict) -> None:
    """Атомарная запись + синхронизация кэша."""
    _ensure_file_parent(USER_ROLES_PATH)
    if USER_ROLES_PATH.exists():
        try:
            backup_path = USER_ROLES_PATH.with_suffix(USER_ROLES_PATH.suffix + ".bak")
            backup_path.write_text(USER_ROLES_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            logger.exception("roles: backup failed")
    tmp = USER_ROLES_PATH.with_suffix(USER_ROLES_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, USER_ROLES_PATH)
    # обновим глобальный кэш
    global _USER_ROLES
    _USER_ROLES = data

def _roles_merge_and_save(patch: dict) -> dict:
    """
    Загружаем с диска, поверхностно мержим (по верхним ключам), сохраняем.
    Пример patch: {"client_phones": {...}} или {"<uid>": {"role": "admin"}}
    """
    data = _roles_load()
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(data.get(k), dict):
            data[k].update(v)           # merge словарей (client_phones, запись пользователя)
        else:
            data[k] = v                 # новые ключи/примитивы
    _roles_save_atomic(data)
    return data

def _client_phones_ref(data: dict) -> dict:
    if "client_phones" not in data or not isinstance(data["client_phones"], dict):
        data["client_phones"] = {}
    return data["client_phones"]

def _norm_key(s: str) -> str:
    # нормализуем ключ клиента: без регистра, ё→е, схлопываем пробелы
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold().replace("ё", "е")

def _base_client_name_for_debt(full: str) -> str:
    # базовое имя без адреса и без « - Колягин»
    s = (full or "")
    s = s.replace(" - Колягин", "").replace("- Колягин", "")
    s = re.sub(r"\(([^)]*)\)", "", s)              # убрать (адрес)
    s = re.sub(r"\s+", " ", s).strip(" -\u00A0")
    return s

def normalize_client_name(raw: str) -> str:
    name = (raw or "").strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(
        r'^(?:(?:["«“„\']\s*)?(?:ооо|ип)\.?(?:\s*["»”"\'])?\s*)+',
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = name.strip()
    if name:
        name = re.sub(r'^[«"“”„\']+|[»"“”„\']+$', "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name

def client_name_was_corrected(raw: str, normalized: str) -> bool:
    raw_clean = re.sub(r"\s+", " ", (raw or "").strip())
    normalized_clean = re.sub(r"\s+", " ", (normalized or "").strip())
    return raw_clean.casefold() != normalized_clean.casefold()

def client_key(full_client_name: str) -> str:
    base = _base_client_name_for_debt(full_client_name)
    return md5(_norm_key(base).encode("utf-8")).hexdigest()[:12]

def get_client_phone(full_client_name: str) -> Optional[str]:
    data = _roles_load()
    phones = _client_phones_ref(data)
    base = _base_client_name_for_debt(full_client_name)
    return phones.get(_norm_key(base))

def set_client_phone(full_client_name: str, phone_e164: str) -> None:
    """Сохраняет/обновляет телефон клиента, не затирая остальные данные файла."""
    data = _roles_load()
    phones = _client_phones_ref(data)
    base = _base_client_name_for_debt(full_client_name)
    phones[_norm_key(base)] = phone_e164
    _roles_merge_and_save({"client_phones": phones})

# --------- Нормализация телефона, wa-номер и текст WhatsApp ---------
def normalize_phone_ru(raw: str) -> Tuple[bool, str, str]:
    """
    Возвращает (is_valid, e164, display)
    e164: +7XXXXXXXXXX
    display: +7 999 999-99-99
    """
    if not raw:
        return False, "", ""
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return False, "", ""
    # Привести к 11 цифрам, Россия
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    elif digits.startswith("7") and len(digits) == 11:
        pass
    elif digits.startswith("9") and len(digits) == 10:
        digits = "7" + digits
    elif digits.startswith("0") or len(digits) < 10:
        return False, "", ""
    elif digits.startswith("00"):  # 007...
        if digits.startswith("007"):
            digits = "7" + digits[3:]
        else:
            return False, "", ""

    if not (digits.startswith("7") and len(digits) == 11):
        return False, "", ""

    e164 = "+" + digits
    disp = f"+7 {digits[1:4]} {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    return True, e164, disp

def wa_number_from_e164(e164: str) -> str:
    # wa.me принимает без «+»
    return re.sub(r"^\+", "", e164)

def client_card_kb(item: Dict[str, Any], report_date: Optional[str]) -> Optional[InlineKeyboardMarkup]:
    total = float(item.get("total_amount") or 0.0)
    has_debt = total > 0.009

    phone = get_client_phone(item.get("client") or "")
    buttons = []

    return InlineKeyboardMarkup(inline_keyboard=buttons)

#--------------------------Аудит действий логи ЛОГИ Логи-------
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery

def _short_text(s: str, n: int = 512) -> str:
    s = (s or "").strip().replace("\u0000", "")
    return s if len(s) <= n else (s[: n-1] + "…")


def _extract_media_meta(m: Message) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Возвращает (kind, meta) для сообщений с медиа.
    kind: 'photo' | 'document' | 'video' | 'voice' | 'audio' | 'sticker' | 'text' | ...
    meta: компактная сводка по файлу (имя, mime, size, w/h, duration и т.п.)
    """
    try:
        # PHOTO: берем последний (самый большой) размер
        if m.photo:
            ph = m.photo[-1]
            return "photo", {
                "photo": {
                    "w": getattr(ph, "width", None),
                    "h": getattr(ph, "height", None),
                    "size": getattr(ph, "file_size", None),
                }
            }

        # DOCUMENT: имя, mime, размер
        if m.document:
            d = m.document
            return "document", {
                "document": {
                    "name": getattr(d, "file_name", None),
                    "mime": getattr(d, "mime_type", None),
                    "size": getattr(d, "file_size", None),
                }
            }

        # VIDEO
        if m.video:
            v = m.video
            return "video", {
                "video": {
                    "w": getattr(v, "width", None),
                    "h": getattr(v, "height", None),
                    "duration": getattr(v, "duration", None),
                    "mime": getattr(v, "mime_type", None),
                    "size": getattr(v, "file_size", None),
                }
            }

        # VOICE
        if m.voice:
            v = m.voice
            return "voice", {
                "voice": {
                    "duration": getattr(v, "duration", None),
                    "mime": getattr(v, "mime_type", None),
                    "size": getattr(v, "file_size", None),
                }
            }

        # AUDIO
        if m.audio:
            a = m.audio
            return "audio", {
                "audio": {
                    "title": getattr(a, "title", None),
                    "performer": getattr(a, "performer", None),
                    "duration": getattr(a, "duration", None),
                    "mime": getattr(a, "mime_type", None),
                    "size": getattr(a, "file_size", None),
                }
            }

        # STICKER
        if m.sticker:
            s = m.sticker
            return "sticker", {
                "sticker": {
                    "is_animated": getattr(s, "is_animated", None),
                    "is_video": getattr(s, "is_video", None),
                    "set_name": getattr(s, "set_name", None),
                }
            }

        # TEXT / прочее
        kind = getattr(m, "content_type", None) or ("text" if m.text else "unknown")
        return kind, None
    except Exception:
        # никаких падений из-за аудита
        return "unknown", None

BLOCKED_USER_TEXT = "Ваш доступ заблокирован. Обратитесь к администратору."


def _blocked_actor_id(event: Any) -> Optional[int]:
    if isinstance(event, (Message, CallbackQuery)):
        user = getattr(event, "from_user", None)
        return getattr(user, "id", None)
    return None


class BlockedUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = _blocked_actor_id(event)
        if not is_user_blocked(user_id):
            return await handler(event, data)

        state = data.get("state")
        if state is not None:
            with contextlib.suppress(Exception):
                await state.clear()

        if isinstance(event, CallbackQuery):
            with contextlib.suppress(Exception):
                await event.answer(BLOCKED_USER_TEXT, show_alert=True)
            if event.message is not None:
                with contextlib.suppress(Exception):
                    await event.message.answer(BLOCKED_USER_TEXT)
            return None

        if isinstance(event, Message):
            with contextlib.suppress(Exception):
                await event.answer(BLOCKED_USER_TEXT)
            return None

        return None

class AuditMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        req_id = uuid.uuid4().hex[:8]
        t0 = time.perf_counter()
        ok = True
        exc: Optional[str] = None

        try:
            return await handler(event, data)
        except Exception as e:
            ok = False
            exc = f"{type(e).__name__}: {e}"
            logger.exception("Unhandled exception (req_id=%s)", req_id)
            raise
        finally:
            dt_ms = int((time.perf_counter() - t0) * 1000)

            # текущий FSM state (если есть)
            cur_state = None
            try:
                state = data.get("state")
                if state is not None:
                    cur_state = await state.get_state()
            except Exception:
                cur_state = None

            # bot id (если нужен)
            bot_id = None
            try:
                bot = data.get("bot")
                bot_id = getattr(bot, "id", None)
            except Exception:
                pass

            try:
                if isinstance(event, Message):
                    u = event.from_user
                    chat = event.chat

                    kind, media = _extract_media_meta(event)

                    AUDIT.info({
                        "t": "msg",
                        "ok": ok,
                        "ms": dt_ms,
                        "req": req_id,

                        "bot": bot_id,

                        "uid": getattr(u, "id", None),
                        "user": getattr(u, "username", None),
                        "name": getattr(u, "full_name", None),
                        "role": get_user_role(getattr(u, "id", None)),

                        "chat": getattr(chat, "id", None),
                        "chat_type": getattr(chat, "type", None),
                        "msg_id": getattr(event, "message_id", None),

                        "kind": kind,
                        "text": _short_text(event.text or event.caption),
                        "caption_len": len(event.caption or "") if hasattr(event, "caption") and event.caption else 0,
                        "entities": len(event.entities or []) if getattr(event, "entities", None) else 0,

                        "state": cur_state,
                        **({"media": media} if media else {}),
                    })

                elif isinstance(event, CallbackQuery):
                    u = event.from_user

                    AUDIT.info({
                        "t": "cb",
                        "ok": ok,
                        "ms": dt_ms,
                        "req": req_id,

                        "bot": bot_id,

                        "uid": getattr(u, "id", None),
                        "user": getattr(u, "username", None),
                        "name": getattr(u, "full_name", None),
                        "role": get_user_role(getattr(u, "id", None)),

                        "chat": getattr(event.message.chat, "id", None) if event.message else None,
                        "cb_id": getattr(event, "id", None),
                        "msg_id": getattr(getattr(event, "message", None), "message_id", None),

                        "data": _short_text(event.data, 256),
                        "state": cur_state,
                    })

                if exc:
                    AUDIT.info({"t": "error", "req": req_id, "exc": exc})

            except Exception:
                # аудит не должен ломать обработку никогда
                pass

# Подключаем к вашему router (aiogram v3):
router.message.middleware(BlockedUserMiddleware())
router.callback_query.middleware(BlockedUserMiddleware())
router.message.middleware(AuditMiddleware())
router.callback_query.middleware(AuditMiddleware())

def audit_event(user_id: int, action: str, **fields):
    AUDIT.info({"t": "event", "uid": user_id, "action": action, **fields})
def _tail(path: str, n: int = 200) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        tail = "".join(lines[-n:])
        return tail[-3500:]  # чтобы не упереться в лимит TG
    except Exception as e:
        return f"<error: {e}>"

@router.message(Command("logs"))
async def cmd_logs(m: Message):
    if not is_admin(getattr(m.from_user, "id", None)):
        return
    kind = ((m.text or "").split(maxsplit=1)[1].strip().lower()
            if (m.text or "").strip() != "/logs" and len((m.text or "").split())>1 else "tech")
    if kind.startswith("audit"):
        path = os.path.join(LOG_DIR, "audit.log")
        await m.answer("<b>audit.log</b>\n<pre>" + esc(_tail(path, 200)) + "</pre>", disable_web_page_preview=True)
    else:
        path = os.path.join(LOG_DIR, "bot.log")
        await m.answer("<b>bot.log</b>\n<pre>" + esc(_tail(path, 200)) + "</pre>", disable_web_page_preview=True)

#----------Конец Логов----------------------------

# --- Группировка тары по клиенту и адресам ---
def _tara_client_parts(entry_or_name: Any) -> Dict[str, str]:
    if isinstance(entry_or_name, dict):
        client_name = (entry_or_name.get("client_name") or "").strip()
        sales_rep = (entry_or_name.get("sales_rep_name") or "").strip()
        address = (entry_or_name.get("address") or "").strip()
        if client_name or sales_rep or address:
            return {"client_name": client_name, "sales_rep": sales_rep, "address": address}
        raw = entry_or_name.get("client") or ""
    else:
        raw = entry_or_name or ""
    return split_report_client_label(str(raw))

def _tara_base_name(full: str) -> str:
    """Базовое имя клиента без адреса и без торгового представителя."""
    return _tara_client_parts(full).get("client_name", "")

def _tara_address(full: str) -> str:
    """Адрес клиента из круглых скобок."""
    return _tara_client_parts(full).get("address", "")

def build_tara_group_text(base_name: str, entries: list) -> str:
    """Форматирование блока по одному клиенту с адресами и позициями."""
    total_all = sum(float(e.get("total", 0) or 0) for e in entries)

    def _key_addr(e):
        a = _tara_client_parts(e).get("address", "")
        return a.casefold().replace("ё", "е")

    entries_sorted = sorted(entries, key=_key_addr)

    lines = [f"<b>{esc(base_name)}</b> — всего: {fmt_qty_units(total_all)}"]
    for e in entries_sorted:
        addr = _tara_client_parts(e).get("address", "")
        if addr:
            lines.append(f"• <b>({esc(addr)})</b> — {fmt_qty_units(e.get('total', 0))}")
            prefix = "    — "
        else:
            prefix = "— "
        for name, qty in (e.get("items") or []):
            lines.append(f"{prefix}{esc(name)} — {fmt_qty_units(qty)}")
    return "\n".join(lines)



def esc(s: Optional[str]) -> str:
    s = s or ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def parse_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M:%S"):  # <-- тут %M латинская
        try:
            return datetime.strptime(d, fmt).date()
        except ValueError:
            pass
    return None

def compute_days(doc_date_str: Optional[str], report_date_str: Optional[str], fallback_days: Optional[int]) -> Optional[int]:
    rd = parse_date(report_date_str) if report_date_str else None
    dd = parse_date(doc_date_str) if doc_date_str else None
    if rd and dd:
        return max(0, (rd - dd).days)
    return fallback_days

def is_overdue(days: Optional[int], threshold: int) -> bool:
    return (days is not None) and (days > threshold)



#--------------------------Меню админа /start /help
def _help_user_name(first_name: Optional[str], *, guest_mode: bool = False) -> str:
    if guest_mode:
        return "Гость"
    cleaned = (first_name or "").strip()
    return cleaned or "Гость"


def _help_title(role_label: str, first_name: Optional[str], *, guest_mode: bool = False) -> str:
    bot_name = _help_user_name(first_name, guest_mode=guest_mode)
    return f"<b>BeerMarket🍺. {esc(role_label)} ({esc(bot_name)})</b>\n\n"


def help_text_admin(first_name: Optional[str]) -> str:
    return (
        f"{_help_title('Админ', first_name)}"
        "📌 <b>Кнопки</b>:\n"
        "• 🧾 <b>Общий отчёт</b> — все клиенты\n"
        "• ⏰ <b>Просрочено</b> — только с просрочкой\n"
        "• 💰 <b>Переплаты</b> — только с переплатой\n"
        "• 📦 <b>Тара</b> — отчёт по возвратной таре\n"
        "• 🔎 <b>Поиск</b> — по частям названия/адреса\n"
        "• 🔎 <b>Поиск тары</b> — поиск по ведомости тары\n"
        "• 📑 <b>Прайсы</b> — отправка прайс-листов\n"
        "• 🎁 <b>Акции</b> — управление акциями\n"
        "• 🚚 <b>График развоза</b> — показать/управлять фото и текстом\n"
        "• 📦 <b>Проверить ТТН</b> — проверка статуса фактуры в ЕГАИС\n"
        "• 🔄 <b>Обновить</b> — скачать свежие файлы из почты\n"
        "• ⚙️ <b>Отсрочки</b> — персональные сроки для клиентов\n"
        "• ⚙️ <b>Фильтры</b> — <i>Порог долга</i> и <i>Мин. дней просрочки</i>\n\n"
        "🎨 <b>Легенда цветов по строкам</b>:\n"
        "• 🟢 младше персональной отсрочки\n"
        "• 🟡 просрочка 1-6 дней\n"
        "• 🔴 просрочка 7+ и старше\n"
        "• ⚪ нулевая сумма строки (закрытая)\n"
        "• 💰 нулевая сумма <u>и</u> есть переплата (старинка закрыта переплатой)\n\n"
        "🧰 <b>Команды</b>:\n"
        "• /bakalar — напоминалка про бакалар\n"
        "• /report — общий отчёт\n"
        "• /report просрочено [слова] — только просрочка\n"
        "• /report переплаты [слова] — только переплаты\n"
        "• /tara — отчёт по таре\n"
        "• /refresh [debt|tara] — обновить файлы\n"
        "• /settings — параметры подключения (админам)\n"
        "• /reset_role — сброс своей роли\n"
        "• /logs — последние строки bot.log\n"
        "• /logs audit — последние строки audit.log\n"
        "• /help — эта справка\n"
    )

def help_text_moderator(first_name: Optional[str]) -> str:
    return (
        f"{_help_title('Модератор', first_name)}"
        "📌 <b>Кнопки</b>:\n"
        "• 🔎 <b>Поиск</b> — поиск по части названия/адреса\n"
        "• 🔎 <b>Поиск тары</b> — поиск по ведомости тары\n"
        "• ⏰ <b>Просрочено</b> — отчёт по просрочке\n"
        "• 💰 <b>Переплаты</b> — отчёт по переплатам\n"
        "• 🏢 <b>Клиенты</b> — просмотр карточек клиентов\n"
        "• 📑 <b>Прайсы</b> — просмотр прайсов\n"
        "• 🎁 <b>Акции</b> — просмотр акций\n"
        "• 🚚 <b>График развоза</b> — фото и правила приёма заявок\n"
        "• 📦 <b>Проверить ТТН</b> — проверка статуса фактуры в ЕГАИС\n"
        "• 🔔 <b>Уведомления</b> — включить или отключить уведомления (новые пользователи, смена ролей, авторизация)\n\n"
        "🧰 <b>Команды</b>:\n"
        "• /help — эта справка\n"
    )

def help_text_guest(first_name: Optional[str]) -> str:
    return (
        f"{_help_title('Гость', first_name, guest_mode=True)}"
        "Сейчас вам доступен только базовый просмотр.\n\n"
        "📌 <b>Кнопки</b>:\n"
        "• 📑 <b>Прайсы</b> — посмотреть прайс-листы\n"
        "• 🎁 <b>Акции</b> — посмотреть акции\n"
        "• 🚚 <b>График развоза</b> — посмотреть график и правила приёма заявок\n\n"
        "Чтобы получить роль клиента, торгового представителя или администратора, обратитесь к администратору."
    )


def help_text_client(first_name: Optional[str], current_name: str) -> str:
    hint = f'Текущее название: <b>«{esc(current_name)}»</b>' if current_name else "<b>Название не задано.</b>"
    return (
        f"{_help_title('Клиент', first_name)}"
        f"{hint}\n\n"
        "📌 <b>Кнопки (клиент)</b>:\n"
        "• 🔎<b> Поиск</b> — найти свои данные по задолженности\n"
        "• 🔎<b> Поиск тары</b> — найти свои остатки по кегам и оборудованию\n"
        "•🎨 Обозначения задолженности:\n"
        "   • 🟢 младше персональной отсрочки\n"
        "   • 🟡 просрочка 1-6 дней\n"
        "   • 🔴 просрочка 7+ и старше\n"
        "   • ⚪️💰 переплата по данной фактуре\n"
        "• 🚚 <b>График развоза</b> — фото графика и правила приёма заявок\n"
        "• ✏️ Изменить название — изменить название Вашей организации ООО или ИП(<b>Без ООО, ИП</b>).\n\n\n"
        "• <b>‼️ График обновлений‼️</b>\n"
        "• 📊 <b>Дебиторская задолженность</b> — ежедневно в <b>10:30</b> и <b>15:30</b>\n"
        "• 📦 <b>Отчёт по таре</b> — ежедневно в <b>12:00</b> (еженедельно).\n\n\n"
        "• ✉️ <a href='https://t.me/Re1ze_r'>Написать администратору в Telegram</a>\n"
    )

def help_text_sales_rep(first_name: Optional[str]) -> str:
    return (
        f"{_help_title('Торговый представитель', first_name)}"
        "📌 <b>Кнопки</b>:\n"
        "• 🔎 <b>Поиск</b> — поиск по части названия/адреса\n"
        "• 🔎 <b>Поиск тары</b> — поиск по ведомости тары\n"
        "• ⏰ <b>Просрочено</b> — отчёт по своим клиентам с просрочкой\n"
        "• 💰 <b>Переплаты</b> — отчёт по своим клиентам с переплатой\n"
        "• 🏢 <b>Клиенты</b> — просмотр и ведение карточек своих клиентов\n"
        "• 📑 <b>Прайсы</b> — просмотр прайсов\n"
        "• 🎁 <b>Акции</b> — просмотр акций\n"
        "• 🚚 <b>График развоза</b> — фото и правила приёма заявок\n"
        "• 📦 <b>Проверить ТТН</b> — проверка статуса фактуры в ЕГАИС\n\n"
        "🧰 <b>Команды</b>:\n"
        "• /help — эта справка\n"
    )


# --- Хранилище ролей/названий клиентов ---
# Глобальный кэш (может пригодиться другим частям кода)
_USER_ROLES = _roles_load()

def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def get_user_role(user_id: Optional[int]) -> str:
    if not user_id:
        return "guest"
    uid = str(user_id)
    if _ADMIN_IDS and user_id in _ADMIN_IDS:
        return "admin"
    rec = (_USER_ROLES.get(uid) or {})
    return normalize_role(rec.get("role") or "guest")

def _user_record(user_id: Optional[int]) -> Dict[str, Any]:
    if not user_id:
        return {}
    return (_roles_load().get(str(user_id)) or {})

def _has_user_onboarding_data(rec: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(rec, dict):
        return False
    return any(
        rec.get(key)
        for key in ("phone", "role", "name", "organization_type", "onboard_completed")
    )

def update_user_profile_from_message(m: Message) -> None:
    user = getattr(m, "from_user", None)
    if not user:
        return
    patch: Dict[str, Any] = {}
    if user.username:
        patch["username"] = user.username
    if user.first_name:
        patch["first_name"] = user.first_name
    if user.last_name:
        patch["last_name"] = user.last_name
    if user.language_code:
        patch["language_code"] = user.language_code
    if getattr(user, "is_premium", None) is not None:
        patch["is_premium"] = bool(user.is_premium)
    if patch:
        update_user_record(user.id, patch)

def _new_user_notification_text(user_id: int) -> str:
    rec = _user_record(user_id)
    role = role_label(rec.get("role") or "guest")
    username = rec.get("username") or "—"
    first_name = rec.get("first_name") or "—"
    last_name = rec.get("last_name") or "—"
    phone = rec.get("phone") or "—"
    name = rec.get("name") or "—"
    verified = "да" if rec.get("phone_verified") else "нет"
    return (
        "🆕 <b>Новый пользователь в боте</b>\n"
        f"ID: <code>{user_id}</code>\n"
        f"Роль: <b>{esc(role)}</b>\n"
        f"Username: <b>{esc(str(username))}</b>\n"
        f"Имя: <b>{esc(str(first_name))}</b>\n"
        f"Фамилия: <b>{esc(str(last_name))}</b>\n"
        f"Контактное имя: <b>{esc(str(name))}</b>\n"
        f"Телефон: <b>{esc(str(phone))}</b>\n"
        f"Телефон подтверждён: <b>{verified}</b>"
    )

def _notification_recipients_new_users() -> List[int]:
    data = _roles_load()
    recipients = {int(uid) for uid in _ADMIN_IDS if isinstance(uid, int)}
    for uid, rec in data.items():
        if uid == "client_phones" or not isinstance(rec, dict) or not uid.isdigit():
            continue
        role = normalize_role(rec.get("role"))
        if role not in {"admin", "moderator"}:
            continue
        if notification_enabled(int(uid), "new_users"):
            recipients.add(int(uid))
    return sorted(recipients)

async def notify_admins_about_new_user(user_id: int) -> None:
    rec = _user_record(user_id)
    if not isinstance(rec, dict):
        return
    if rec.get("new_user_notified"):
        return
    text = _new_user_notification_text(user_id)
    sent = False
    for recipient in _notification_recipients_new_users():
        if recipient == user_id:
            continue
        try:
            await bot.send_message(recipient, text)
            sent = True
        except Exception:
            logger.exception("new-user-notify: failed recipient=%s user=%s", recipient, user_id)
    update_user_record(user_id, {"new_user_notified": sent or bool(rec.get("new_user_notified"))})

def set_user_role(user_id: int, role: str) -> None:
    uid = str(user_id)
    role_val = normalize_role(role)
    # merge с тем, что уже на диске
    cur = _roles_load().get(uid, {})
    cur["role"] = role_val
    _roles_merge_and_save({uid: cur})

def get_client_name(user_id: Optional[int]) -> str:
    if not user_id:
        return ""
    uid = str(user_id)
    return str(((_USER_ROLES.get(uid) or {}).get("name")) or "").strip()

def set_user_phone(user_id: int, phone_e164: str, *, verified: bool = False) -> None:
    uid = str(user_id)
    cur = _roles_load().get(uid, {})
    cur["phone"] = (phone_e164 or "").strip()
    cur["phone_verified"] = bool(verified)
    _roles_merge_and_save({uid: cur})

def get_user_phone(user_id: Optional[int]) -> str:
    if not user_id:
        return ""
    uid = str(user_id)
    rec = _roles_load().get(uid, {})
    return str((rec or {}).get("phone") or "").strip()

def update_user_record(user_id: Any, patch: Dict[str, Any]) -> None:
    uid = str(user_id)
    cur = _roles_load().get(uid, {})
    if not isinstance(cur, dict):
        cur = {"role": "guest", "name": str(cur)}
    patch = dict(patch or {})
    if "role" in patch:
        patch["role"] = normalize_role(patch.get("role"))
    if "access_overrides" in patch:
        patch["access_overrides"] = _normalize_access_overrides(patch.get("access_overrides"))
    if "notification_settings" in patch:
        patch["notification_settings"] = _normalize_notification_settings(patch.get("notification_settings"))
    cur.update(patch)
    _roles_merge_and_save({uid: cur})

def get_user_access_overrides(user_id: Optional[int]) -> Dict[str, bool]:
    return _normalize_access_overrides(_user_record(user_id).get("access_overrides"))

def set_user_action_override(user_id: Any, action: str, enabled: Optional[bool]) -> None:
    rec = _user_record(user_id)
    overrides = _normalize_access_overrides(rec.get("access_overrides"))
    if enabled is None:
        overrides.pop(action, None)
    else:
        overrides[action] = bool(enabled)
    update_user_record(user_id, {"access_overrides": overrides})

def reset_user_action_overrides(user_id: Any) -> None:
    update_user_record(user_id, {"access_overrides": {}})

def get_user_notification_settings(user_id: Optional[int]) -> Dict[str, bool]:
    return _normalize_notification_settings(_user_record(user_id).get("notification_settings"))


NOTIFICATION_META: Dict[str, Dict[str, str]] = {
    "new_users": {"label": "Новые пользователи"},
    "role_changes": {"label": "Смена ролей"},
    "auth_changes": {"label": "Авторизация/блокировка"},
}
NOTIFICATION_ORDER: List[str] = ["new_users", "role_changes", "auth_changes"]


def notification_label(key: str) -> str:
    return str((NOTIFICATION_META.get(key) or {}).get("label") or key)

def notification_enabled(user_id: Optional[int], key: str, *, default: Optional[bool] = None) -> bool:
    rec = _user_record(user_id)
    settings = _normalize_notification_settings(rec.get("notification_settings"))
    if key in settings:
        return settings[key]
    role = normalize_role(rec.get("role") or get_user_role(user_id))
    if default is not None:
        return default
    if key == "new_users":
        return role in {"admin", "moderator"}
    if key in {"role_changes", "auth_changes"}:
        return True
    return False

def set_user_notification_setting(user_id: Any, key: str, enabled: bool) -> None:
    rec = _user_record(user_id)
    settings = _normalize_notification_settings(rec.get("notification_settings"))
    settings[key] = bool(enabled)
    update_user_record(user_id, {"notification_settings": settings})

def _notification_toggle_text(user_id: int, key: str) -> str:
    enabled = notification_enabled(user_id, key)
    return f"{'✅' if enabled else '❌'} {notification_label(key)}"


def _notification_recipients_role_changes() -> List[int]:
    data = _roles_load()
    recipients = {int(uid) for uid in _ADMIN_IDS if isinstance(uid, int)}
    for uid, rec in data.items():
        if uid == "client_phones" or not isinstance(rec, dict) or not uid.isdigit():
            continue
        role = normalize_role(rec.get("role"))
        if role not in {"admin", "moderator"}:
            continue
        if notification_enabled(int(uid), "role_changes"):
            recipients.add(int(uid))
    return sorted(recipients)


async def notify_about_role_change(
    *,
    actor_id: Optional[int],
    target_user_id: int,
    old_role: str,
    new_role: str,
) -> None:
    if normalize_role(old_role) == normalize_role(new_role):
        return
    actor_name = "Система"
    if actor_id:
        actor = _user_record(actor_id)
        actor_name = (actor.get("name") or actor.get("username") or str(actor_id)).strip()
    old_label = role_label(old_role)
    new_label = role_label(new_role)
    text_for_user = (
        "🔐 <b>Изменение роли</b>\n"
        f"Ваша роль изменена: <b>{esc(old_label)}</b> → <b>{esc(new_label)}</b>.\n"
        f"Инициатор: <b>{esc(actor_name)}</b>."
    )
    if notification_enabled(target_user_id, "role_changes"):
        try:
            await bot.send_message(target_user_id, text_for_user)
        except Exception:
            logger.exception("role-change-notify: failed target=%s", target_user_id)
    text_for_admins = (
        "🔔 <b>Смена роли пользователя</b>\n"
        f"Пользователь: <code>{target_user_id}</code>\n"
        f"Роль: <b>{esc(old_label)}</b> → <b>{esc(new_label)}</b>\n"
        f"Инициатор: <b>{esc(actor_name)}</b>"
    )
    for recipient in _notification_recipients_role_changes():
        if recipient in {target_user_id, int(actor_id or 0)}:
            continue
        try:
            await bot.send_message(recipient, text_for_admins)
        except Exception:
            logger.exception("role-change-notify: failed recipient=%s target=%s", recipient, target_user_id)


async def notify_about_access_change(
    *,
    actor_id: Optional[int],
    target_user_id: int,
    event_label: str,
    new_value_label: str,
) -> None:
    if not notification_enabled(target_user_id, "auth_changes"):
        return
    actor_name = "Система"
    if actor_id:
        actor = _user_record(actor_id)
        actor_name = (actor.get("name") or actor.get("username") or str(actor_id)).strip()
    text = (
        "🔔 <b>Изменение доступа</b>\n"
        f"Событие: <b>{esc(event_label)}</b>\n"
        f"Новое состояние: <b>{esc(new_value_label)}</b>\n"
        f"Инициатор: <b>{esc(actor_name)}</b>"
    )
    try:
        await bot.send_message(target_user_id, text)
    except Exception:
        logger.exception("access-change-notify: failed target=%s event=%s", target_user_id, event_label)


def delete_user_record(user_id: Any) -> bool:
    uid = str(user_id)
    data = _roles_load()
    if uid not in data:
        return False
    data.pop(uid, None)
    _save_user_roles(data)
    return True

def is_user_blocked(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    uid = str(user_id)
    rec = _roles_load().get(uid, {})
    return bool((rec or {}).get("blocked"))

def set_client_name(user_id: int, name: str) -> None:
    uid = str(user_id)
    cur = _roles_load().get(uid, {})
    cur["name"] = (name or "").strip()
    _roles_merge_and_save({uid: cur})

def _save_user_roles(data: Dict[str, Any]) -> None:
    _roles_save_atomic(_normalize_user_roles_schema(data or {}))

# ---------------- Фильтры  -----------------
def _ensure_filters_dir():
    d = os.path.dirname(MIN_DEBT_JSON)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# filters.py (или в вашем модуле конфигурации)
# ---------------- Фильтры (единая версия) -----------------
import os, json

# Используем тот же путь, что и раньше для min_debt (обычно "settings/filters.json")
FILTERS_PATH = os.getenv("FILTERS_PATH", MIN_DEBT_JSON)

DEFAULT_FILTERS = {
    "min_debt": 999.0,
    "min_overdue_days": 20,  # новый порог по дням для отчёта «Просрочка»
}

def load_filters() -> dict:
    try:
        with open(FILTERS_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f) or {}
    except FileNotFoundError:
        cfg = {}
    # подставим дефолты, если ключей нет
    for k, v in DEFAULT_FILTERS.items():
        cfg.setdefault(k, v)
    return cfg

def save_filters(cfg: dict) -> None:
    # атомарная запись + создадим директорию при необходимости
    d = os.path.dirname(FILTERS_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)
    tmp = FILTERS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, FILTERS_PATH)

FILTERS = load_filters()

def get_min_debt() -> float:
    try:
        return float(FILTERS.get("min_debt", DEFAULT_FILTERS["min_debt"]))
    except Exception:
        return float(DEFAULT_FILTERS["min_debt"])

def set_min_debt(val: float) -> None:
    FILTERS["min_debt"] = float(max(0.0, val))
    save_filters(FILTERS)

def get_min_overdue_days() -> int:
    try:
        return int(FILTERS.get("min_overdue_days", DEFAULT_FILTERS["min_overdue_days"]))
    except Exception:
        return int(DEFAULT_FILTERS["min_overdue_days"])

def set_min_overdue_days(n: int) -> None:
    FILTERS["min_overdue_days"] = int(max(0, n))
    save_filters(FILTERS)


# --- Pages for "Фильтры отображения" (inline) ---
FILTER_PAGES = [
    {
        "key": "min_debt",
        "title": "Порог долга",
        "units": "₽",
        "desc": "Показывать клиентов, если нетто-долг ≥ этому порогу.",
        "get": get_min_debt,
        "set": set_min_debt,
        "default": DEFAULT_FILTERS["min_debt"],
        "parse": lambda s: float((s or "").replace(",", ".").strip() or "0"),
        "validate": lambda v: (0.0 <= v <= 1e9, "Число 0..1e9"),
        "fmt": lambda v: f"{float(v):,.2f} ₽".replace(",", " ").replace(".00",""),
    },
    {
        "key": "min_overdue_days",
        "title": "Мин. дней просрочки",
        "units": "дн.",
        "desc": "В «⏰ Просрочено»: скрывать строки моложе этого возраста.",
        "get": get_min_overdue_days,
        "set": set_min_overdue_days,
        "default": DEFAULT_FILTERS["min_overdue_days"],
        "parse": lambda s: int((s or "0").strip() or "0"),
        "validate": lambda v: (0 <= v <= 365, "Целое 0..365"),
        "fmt": lambda v: f"{int(v)} дн.",
    },
]

def _filters_page_text(idx: int) -> str:
    page = FILTER_PAGES[idx]
    cur = page["get"]()
    total = len(FILTER_PAGES)
    return (
        f"<b>Фильтры отображения</b> — страница {idx+1}/{total}\n"
        f"<b>{page['title']}</b>\n"
        f"Текущее значение: <code>{page['fmt'](cur)}</code>\n"
        f"{page['desc']}"
    )

def _filters_page_kb(idx: int) -> InlineKeyboardMarkup:
    total = len(FILTER_PAGES)
    prev_idx = (idx - 1) % total
    next_idx = (idx + 1) % total
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"flt:chg:{idx}"),
         InlineKeyboardButton(text="↩️ Сброс",   callback_data=f"flt:reset:{idx}")],
        [InlineKeyboardButton(text="⬅️ Назад",   callback_data=f"flt:nav:{prev_idx}"),
         InlineKeyboardButton(text="Вперёд ➡️",  callback_data=f"flt:nav:{next_idx}")],
    ])


async def _filters_safe_edit(msg, text: str, reply_markup):
    try:
        await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
        return
    except TelegramBadRequest as e:
        s = str(e).lower()
        # текст тот же — пробуем обновить только клавиатуру
        if "message is not modified" in s:
            try:
                await msg.edit_reply_markup(reply_markup=reply_markup)
                return
            except TelegramBadRequest as e2:
                # и клавиатура тоже та же — просто игнорируем
                if "message is not modified" in str(e2).lower():
                    return
                raise
        raise



# --- Персональные отсрочки ---
def _ensure_settings_dir():
    d = os.path.dirname(CLIENT_OVERDUE_JSON)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _load_overdue_map() -> Dict[str, int]:
    _ensure_settings_dir()
    try:
        if os.path.exists(CLIENT_OVERDUE_JSON):
            with open(CLIENT_OVERDUE_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {str(k).strip().casefold(): int(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("Не удалось прочитать %s: %s", CLIENT_OVERDUE_JSON, e)
    return {}

def _save_overdue_map(m: Dict[str, int]) -> None:
    _ensure_settings_dir()
    try:
        with open(CLIENT_OVERDUE_JSON, "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Не удалось сохранить %s: %s", CLIENT_OVERDUE_JSON, e)

def _sync_client_cards_overdue_from_map() -> int:
    """Синхронизирует индивидуальные отсрочки из JSON в актуальную БД карточек клиентов."""
    if not _CLIENT_OD_MAP:
        return 0

    updates: Dict[str, int] = {}
    for card in CLIENTS_DB.list_clients():
        legal_name = (card.get("legal_name") or "").strip().casefold()
        store_name = (card.get("store_name") or "").strip().casefold()
        address = (card.get("address") or "").strip().casefold()
        best_days: Optional[int] = None

        for key, days in _CLIENT_OD_MAP.items():
            k = (key or "").strip().casefold()
            if not k:
                continue
            if k in legal_name or k in store_name or k in address:
                if best_days is None or int(days) > best_days:
                    best_days = int(days)

        if best_days is not None:
            updates[str(card.get("id"))] = best_days

    if not updates:
        return 0
    changed = CLIENTS_DB.sync_overdue_days(updates)
    if changed:
        logger.info("Синхронизировано отсрочек в карточки клиентов: %s", changed)
    return changed

_CLIENT_OD_MAP = _load_overdue_map()
_sync_client_cards_overdue_from_map()

def get_overdue_days_for_client(client_name: str) -> int:
    base = OVERDUE_DAYS_DEFAULT
    if not client_name:
        return base
    low = client_name.casefold()
    best = base
    for key, days in _CLIENT_OD_MAP.items():
        if key and key in low:
            if days > best:
                best = days
    return best

# --- Время последнего обновления ---
def _ensure_dir_of(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _now_iso() -> str:
    return datetime.now(TZ).isoformat()

def set_last_update(kind: str):
    try:
        _ensure_dir_of(LAST_UPDATE_FILE)
        data = {}
        if os.path.exists(LAST_UPDATE_FILE):
            with open(LAST_UPDATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        data[kind] = _now_iso()  # 'auto' | 'manual'
        with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("Не удалось сохранить время обновления: %s", e)

def get_last_update() -> Tuple[Optional[datetime], Optional[str]]:
    try:
        if not os.path.exists(LAST_UPDATE_FILE):
            return None, None
        with open(LAST_UPDATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        best_dt, best_kind = None, None
        for k in ("manual", "auto"):
            v = data.get(k)
            if not v:
                continue
            try:
                dt = datetime.fromisoformat(v)
                if dt.tzinfo is None:
                    dt = TZ.localize(dt)
            except Exception:
                continue
            if best_dt is None or dt > best_dt:
                best_dt, best_kind = dt, k
        return best_dt, best_kind
    except Exception as e:
        logger.warning("Не удалось прочитать время обновления: %s", e)
        return None, None

def fmt_dt_local(dt: Optional[datetime]) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    else:
        dt = dt.astimezone(TZ)
    return dt.strftime("%d.%m.%Y %H:%M")

def fmt_hhmm(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    else:
        dt = dt.astimezone(TZ)
    return dt.strftime("%H:%M")

# --- UI интерфейс ---
TTN_BTN  = "📦 Проверить ТТН"
TARE_BTN = "📦 Тара"
SCHEDULE_BTN = "🚚 График развоза"
SCHEDULE_IMG_PATH = Path("settings/schedule_image.jpg")     # сюда сохраним картинку
SCHEDULE_NOTE_PATH = Path("settings/schedule_note.txt")     # сюда сохраним подпись
DEFAULT_SCHEDULE_NOTE = "Заявки за день понедельник-пятница до 15:00. Вс до 13:00."


#main_menu_kb() КЛАВИАТУРА АДМИНА
def main_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    return build_user_menu_kb(user_id=user_id, role="admin")


def _append_button_row_if_any(keyboard: List[List[KeyboardButton]], buttons: List[KeyboardButton]) -> None:
    if buttons:
        keyboard.append(buttons)


def _management_button_text(role: str) -> str:
    return "🏢 Моя карточка" if role == "client" else "🏢 Клиенты"


def build_user_menu_kb(user_id: Optional[int] = None, role: Optional[str] = None) -> ReplyKeyboardMarkup:
    role = normalize_role(role or get_user_role(user_id))
    last_dt, _ = get_last_update()
    upd_label = "🔄 Обновить"
    hhmm = fmt_hhmm(last_dt)
    if hhmm:
        upd_label = f"{upd_label} ({hhmm})"
    keyboard: List[List[KeyboardButton]] = []
    row: List[KeyboardButton] = []
    if user_allows_action(user_id, "search.debt"):
        row.append(KeyboardButton(text="🔎 Поиск"))
    if user_allows_action(user_id, "search.tara"):
        row.append(KeyboardButton(text="🔎 Поиск тары"))
    _append_button_row_if_any(keyboard, row)
    row = []
    if user_allows_action(user_id, "reports.general"):
        row.append(KeyboardButton(text="🧾 Общий отчёт"))
    if user_allows_action(user_id, "reports.tara"):
        row.append(KeyboardButton(text=TARE_BTN))
    _append_button_row_if_any(keyboard, row)
    row = []
    if user_allows_action(user_id, "reports.overdue"):
        row.append(KeyboardButton(text="⏰ Просрочено"))
    if user_allows_action(user_id, "reports.overpaid"):
        row.append(KeyboardButton(text="💰 Переплаты"))
    _append_button_row_if_any(keyboard, row)
    row = []
    if user_allows_action(user_id, "prices.view"):
        row.append(KeyboardButton(text="📑 Прайсы"))
    if user_allows_action(user_id, "promos.view"):
        row.append(KeyboardButton(text="🎁 Акции"))
    _append_button_row_if_any(keyboard, row)
    row = []
    if user_allows_action(user_id, "schedule.view"):
        row.append(KeyboardButton(text=SCHEDULE_BTN))
    if user_allows_action(user_id, "ttn.lookup"):
        row.append(KeyboardButton(text=TTN_BTN))
    _append_button_row_if_any(keyboard, row)
    if role in {"admin", "sales_rep"}:
        keyboard.append([KeyboardButton(text="⚙️ Отсрочки"), KeyboardButton(text="⚙️ Фильтры")])
    management_row: List[KeyboardButton] = []
    if user_allows_action(user_id, "users.manage") or user_allows_action(user_id, "users.view"):
        management_row.append(KeyboardButton(text="👥 Пользователи"))
    if user_allows_action(user_id, "client_cards.view"):
        management_row.append(KeyboardButton(text=_management_button_text(role)))
    if user_allows_action(user_id, "technicians.manage"):
        management_row.append(KeyboardButton(text="🛠 Техники"))
    if role == "client":
        management_row.append(KeyboardButton(text="✏️ Изменить название"))
    _append_button_row_if_any(keyboard, management_row)
    if user_allows_action(user_id, "notifications.manage"):
        keyboard.append([KeyboardButton(text="🔔 Уведомления")])
    start_row = [KeyboardButton(text="▶️ Старт")]
    if role == "admin" or user_allows_action(user_id, "updates.mail"):
        start_row.append(KeyboardButton(text=upd_label))
    keyboard.append(start_row)
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )

def sales_rep_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    return build_user_menu_kb(user_id=user_id, role="sales_rep")


def moderator_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    return build_user_menu_kb(user_id=user_id, role="moderator")


#первый запуск.
def onboard_role_kb() -> InlineKeyboardMarkup:
    """Инлайн-кнопки для выбора роли при первом запуске."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Я админ", callback_data="ob:admin")],
        [InlineKeyboardButton(text="Я клиент", callback_data="ob:client")]
    ])

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить контакт", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def organization_guest_choice_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="👋 Остаться гостем")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

async def send_phone_request(m: Message) -> None:
    await m.answer(
        "Для продолжения нужна авторизация по номеру телефона.\n"
        "Нажмите кнопку «📱 Отправить контакт».",
        reply_markup=phone_request_kb(),
    )

#меню обновить
def update_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📒 Дебиторка", callback_data="upd:debt")],
        [InlineKeyboardButton(text="📦 Тара",       callback_data="upd:tara")],
        [InlineKeyboardButton(text="⬅️ Назад",     callback_data="menu:back")],
    ])


#график развоза
def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def load_schedule_note() -> str:
    try:
        txt = SCHEDULE_NOTE_PATH.read_text(encoding="utf-8").strip()
        if txt:
            return txt
    except FileNotFoundError:
        pass
    return DEFAULT_SCHEDULE_NOTE

def save_schedule_note(text: str) -> None:
    _ensure_parent(SCHEDULE_NOTE_PATH)
    SCHEDULE_NOTE_PATH.write_text((text or "").strip(), encoding="utf-8")



def schedule_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Показать", callback_data="schedule:show")],
        [InlineKeyboardButton(text="🆙 Заменить фото", callback_data="schedule:upload")],
        [InlineKeyboardButton(text="📝 Изменить текст", callback_data="schedule:note")],
        [InlineKeyboardButton(text="🗑 Удалить фото", callback_data="schedule:delete")],
    ])

@router.message(F.text == SCHEDULE_BTN)
async def schedule_show_button(m: Message):
    if is_user_blocked(getattr(m.from_user, "id", None)):
        await m.answer(BLOCKED_USER_TEXT)
        return
    if not await ensure_message_access(m, "schedule.view"):
        return
    note = load_schedule_note()
    if SCHEDULE_IMG_PATH.exists():
        try:
            await m.answer_photo(FSInputFile(SCHEDULE_IMG_PATH), caption=f"<b>График развоза</b>\n\n{note}")
        except Exception as e:
            logger.exception("schedule: send photo failed")
            await m.answer(f"<b>График развоза</b>\n\n{note}\n\n(не удалось отправить фото: {e})")
    else:
        await m.answer(f"<b>График развоза</b>\n\n{note}\n\n<i>Фото пока не загружено.</i>")

    # если админ — доп. панель управления
    uid = getattr(m.from_user, "id", None)
    role = get_user_role(uid)
    if role == "admin":
        await m.answer("Управление графиком:", reply_markup=schedule_admin_kb())

@router.callback_query(F.data == "schedule:show")
async def sch_admin_show(cq: CallbackQuery):
    if is_user_blocked(getattr(cq.from_user, "id", None)):
        await cq.answer(BLOCKED_USER_TEXT, show_alert=True)
        return
    if not await ensure_callback_access(cq, "schedule.view"):
        return
    note = load_schedule_note()
    try:
        if SCHEDULE_IMG_PATH.exists():
            await cq.message.answer_photo(FSInputFile(SCHEDULE_IMG_PATH), caption=f"<b>График развоза</b>\n\n{note}")
        else:
            await cq.message.answer(f"<b>График развоза</b>\n\n{note}\n\n<i>Фото пока не загружено.</i>")
    finally:
        await cq.answer()

@router.callback_query(F.data == "schedule:upload")
async def sch_admin_upload(cq: CallbackQuery, state: FSMContext):
    await state.set_state(ScheduleStates.waiting_photo)
    await cq.message.answer("Пришлите фото с графиком (одним изображением).")
    await cq.answer()

@router.callback_query(F.data == "schedule:note")
async def sch_admin_note(cq: CallbackQuery, state: FSMContext):
    await state.set_state(ScheduleStates.waiting_text)
    cur = load_schedule_note()
    await cq.message.answer(f"Текущий текст:\n\n{cur}\n\nПришлите новый текст (или отправьте «/cancel» для отмены).")
    await cq.answer()

@router.callback_query(F.data == "schedule:delete")
async def sch_admin_delete(cq: CallbackQuery):
    try:
        if SCHEDULE_IMG_PATH.exists():
            SCHEDULE_IMG_PATH.unlink()
            await cq.message.answer("Фото удалено.")
        else:
            await cq.message.answer("Фото ещё не загружено.")
    except Exception as e:
        logger.exception("schedule: delete failed")
        await cq.message.answer(f"Ошибка удаления: {e}")
    finally:
        await cq.answer()

@router.message(ScheduleStates.waiting_photo, F.photo)
async def sch_receive_photo(m: Message, state: FSMContext):
    try:
        _ensure_parent(SCHEDULE_IMG_PATH)
        # Берём самое большое превью
        ph = m.photo[-1]
        # aiogram v3: безопасная загрузка с fallback
        try:
            await m.bot.download(ph, destination=SCHEDULE_IMG_PATH)
        except Exception:
            file = await m.bot.get_file(ph.file_id)
            await m.bot.download_file(file.file_path, destination=SCHEDULE_IMG_PATH)

        await m.answer("Фото сохранено. Показываю для проверки…")
        await schedule_show_button(m)
    except Exception as e:
        logger.exception("schedule: save photo failed")
        await m.answer(f"Не удалось сохранить фото: {e}")
    finally:
        await state.clear()

@router.message(ScheduleStates.waiting_photo)
async def sch_expect_photo_only(m: Message, state: FSMContext):
    await m.answer("Нужно прислать именно фото. Попробуйте ещё раз или отправьте «/cancel» для отмены.")

@router.message(ScheduleStates.waiting_text, F.text)
async def sch_receive_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if text.lower() in ("/cancel", "отмена"):
        await m.answer("Изменение текста отменено.")
        await state.clear()
        return
    try:
        save_schedule_note(text)
        await m.answer("Текст обновлён. Показываю…")
        await schedule_show_button(m)
    except Exception as e:
        logger.exception("schedule: save note failed")
        await m.answer(f"Не удалось сохранить текст: {e}")
    finally:
        await state.clear()

@router.message(ScheduleStates.waiting_text)
async def sch_expect_text_only(m: Message, state: FSMContext):
    await m.answer("Пришлите новый текст одной строкой (или «/cancel» для отмены).")


#----------------------------------------------
#------------UI Интерфейс клиента--------------
#----------------------------------------------
def guest_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    return build_user_menu_kb(user_id=user_id, role="guest")


def client_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    """Клавиатура клиента: видимые кнопки определяются доступами пользователя."""
    return build_user_menu_kb(user_id=user_id, role="client")

def _user_sort_key(item: Tuple[str, Dict[str, Any]]) -> Tuple[int, str]:
    uid, rec = item
    name = (rec.get("name") or "").strip().casefold()
    return (0 if name else 1, name or uid)

def users_list_kb(page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    data = _roles_load()
    items: List[Tuple[str, Dict[str, Any]]] = []
    for k, v in data.items():
        if k == "client_phones":
            continue
        if not isinstance(v, dict):
            v = {"role": "guest", "name": str(v)}
        items.append((k, v))
    items.sort(key=_user_sort_key)

    total = len(items)
    page = max(0, page)
    start = page * page_size
    end = min(total, start + page_size)
    rows: List[List[InlineKeyboardButton]] = []
    for uid, rec in items[start:end]:
        name = (rec.get("name") or "unknown").strip()
        role = normalize_role(rec.get("role") or "guest")
        rows.append([InlineKeyboardButton(text=f"{name} · {role_label(role)}", callback_data=f"usr:sel:{uid}:{page}")])
    nav: List[InlineKeyboardButton] = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"usr:list:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"usr:list:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def user_detail_kb(
    uid: str,
    page: int = 0,
    is_authorized: bool = False,
    *,
    can_manage: bool = True,
) -> InlineKeyboardMarkup:
    auth_btn_text = "🚫 Снять авторизацию" if is_authorized else "✅ Авторизовать"
    rows: List[List[InlineKeyboardButton]] = []
    if can_manage:
        rows.extend([
            [
                InlineKeyboardButton(text="✅ Сделать админом", callback_data=f"usr:setrole:{uid}:admin"),
                InlineKeyboardButton(text="👤 Сделать клиентом", callback_data=f"usr:setrole:{uid}:client"),
            ],
            [
                InlineKeyboardButton(text="🧑‍💼 Сделать торговым представителем", callback_data=f"usr:setrole:{uid}:sales_rep"),
                InlineKeyboardButton(text="👋 Сделать гостем", callback_data=f"usr:setrole:{uid}:guest"),
            ],
            [
                InlineKeyboardButton(text="🛡 Сделать модератором", callback_data=f"usr:setrole:{uid}:moderator"),
            ],
            [
                InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data=f"usr:del:{uid}:{page}"),
                InlineKeyboardButton(text=auth_btn_text, callback_data=f"usr:auth:{uid}:{page}"),
            ],
            [
                InlineKeyboardButton(text="✏️ Изменить имя", callback_data=f"usr:editname:{uid}"),
                InlineKeyboardButton(text="📞 Изменить телефон", callback_data=f"usr:editphone:{uid}"),
            ],
            [
                InlineKeyboardButton(text="🔔 Уведомления пользователя", callback_data=f"usr:notifymenu:{uid}:{page}"),
            ],
        ])
    rows.append([InlineKeyboardButton(text="🔐 Права доступа", callback_data=f"usr:perms:{uid}:0")])
    if can_manage:
        rows.append([
            InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"usr:block:{uid}"),
            InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"usr:unblock:{uid}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К списку", callback_data=f"usr:list:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


MANAGED_ACTIONS: List[Tuple[str, str]] = [
    ("prices.view", "📑 Прайсы"),
    ("promos.view", "🎁 Акции"),
    ("schedule.view", "🚚 График"),
    ("search.debt", "🔎 Поиск"),
    ("search.tara", "🔎 Поиск тары"),
    ("reports.general", "🧾 Общий отчёт"),
    ("reports.overdue", "⏰ Просрочено"),
    ("reports.overpaid", "💰 Переплаты"),
    ("reports.tara", "📦 Тара"),
    ("ttn.lookup", "📦 Проверить ТТН"),
    ("updates.mail", "🔄 Обновить"),
    ("client_cards.view", "🏢 Клиенты"),
    ("client_cards.manage", "✏️ Карточки"),
    ("technicians.manage", "🛠 Техники"),
    ("users.view", "👥 Пользователи (просмотр)"),
    ("users.manage", "👥 Пользователи (управление)"),
    ("notifications.manage", "🔔 Уведомления"),
]
MANAGED_ACTIONS_BY_TOKEN: Dict[str, str] = {str(i): action for i, (action, _) in enumerate(MANAGED_ACTIONS)}
MANAGED_ACTIONS_LABELS: Dict[str, str] = {action: label for action, label in MANAGED_ACTIONS}
MANAGED_ACTIONS_PAGE_SIZE = 6

def user_permissions_kb(uid: str, page: int = 0, *, can_manage: bool = True) -> InlineKeyboardMarkup:
    rec = _roles_load().get(uid, {}) if uid else {}
    role = normalize_role((rec or {}).get("role") or "guest")
    total = len(MANAGED_ACTIONS)
    last_page = max(0, (total - 1) // MANAGED_ACTIONS_PAGE_SIZE) if total else 0
    page = max(0, min(page, last_page))
    start = page * MANAGED_ACTIONS_PAGE_SIZE
    end = min(total, start + MANAGED_ACTIONS_PAGE_SIZE)
    rows: List[List[InlineKeyboardButton]] = []
    if can_manage:
        rows.extend([[
            InlineKeyboardButton(text="👑 Админ", callback_data=f"usr:setrole:{uid}:admin"),
            InlineKeyboardButton(text="🛡 Модератор", callback_data=f"usr:setrole:{uid}:moderator"),
        ], [
            InlineKeyboardButton(text="🧑‍💼 Торговый", callback_data=f"usr:setrole:{uid}:sales_rep"),
            InlineKeyboardButton(text="👤 Клиент", callback_data=f"usr:setrole:{uid}:client"),
        ], [
            InlineKeyboardButton(text="👋 Гость", callback_data=f"usr:setrole:{uid}:guest"),
            InlineKeyboardButton(text="♻️ Сбросить права", callback_data=f"usr:permreset:{uid}:{page}"),
        ]])
    for idx in range(start, end):
        action, label = MANAGED_ACTIONS[idx]
        enabled = user_allows_action(int(uid), action) if uid.isdigit() else role_allows_action(role, action)
        icon = "✅" if enabled else "❌"
        if can_manage:
            rows.append([
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"usr:permtoggle:{uid}:{idx}:{page}"
                )
            ])
        else:
            rows.append([
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data="usr:perms:noop"
                )
            ])
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"usr:perms:{uid}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{last_page+1}", callback_data="usr:perms:noop"))
    if page < last_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"usr:perms:{uid}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"usr:sel:{uid}:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def safe_user_detail_kb(uid: str, page: int, is_authorized: bool, *, can_manage: bool) -> InlineKeyboardMarkup:
    """
    Защита от падения в рантайме, если в конкретной сборке отсутствует user_detail_kb.
    """
    builder = globals().get("user_detail_kb")
    if callable(builder):
        return builder(uid, page=page, is_authorized=is_authorized, can_manage=can_manage)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Права доступа", callback_data=f"usr:perms:{uid}:0")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data=f"usr:list:{page}")],
    ])


def safe_user_permissions_kb(uid: str, page: int, *, can_manage: bool) -> InlineKeyboardMarkup:
    """
    Защита от падения в рантайме, если в конкретной сборке отсутствует user_permissions_kb.
    """
    builder = globals().get("user_permissions_kb")
    if callable(builder):
        return builder(uid, page=page, can_manage=can_manage)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"usr:sel:{uid}:0")],
    ])

def notifications_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in NOTIFICATION_ORDER:
        rows.append([
            InlineKeyboardButton(
                text=_notification_toggle_text(user_id, key),
                callback_data=f"notify:toggle:{key}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_user_notifications_kb(uid: str, page: int = 0) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    user_id = int(uid) if uid.isdigit() else 0
    for key in NOTIFICATION_ORDER:
        rows.append([
            InlineKeyboardButton(
                text=_notification_toggle_text(user_id, key),
                callback_data=f"usr:notify:{uid}:{key}:{page}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"usr:sel:{uid}:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def overdue_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список", callback_data="od:list")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="od:add")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="od:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="od:del")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")]
    ])

def back_only_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")]
    ])

def settings_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 BOT_TOKEN",      callback_data="cfg:bot")],
        [InlineKeyboardButton(text="🌐 IMAP_SERVER",    callback_data="cfg:imap")],
        [InlineKeyboardButton(text="📧 EMAIL_ACCOUNT",  callback_data="cfg:email")],
        [InlineKeyboardButton(text="🔐 EMAIL_PASSWORD", callback_data="cfg:pass")],
        [InlineKeyboardButton(text="⬅️ Назад",          callback_data="menu:back")]
    ])

#карточка клиента ----------------------
#ТЕХНИКИ--------------------------------
def technicians_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список техников", callback_data="tc:list")],
        [InlineKeyboardButton(text="➕ Добавить техника", callback_data="tc:new")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
    ])


def technicians_list_kb(items: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for it in items[:50]:
        rows.append([
            InlineKeyboardButton(
                text=f"{it.get('full_name')} · {it.get('phone')}",
                callback_data=f"tc:view:{it.get('id')}",
            )
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить техника", callback_data="tc:new")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="tc:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def technician_actions_kb(technician_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"tc:edit:{technician_id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"tc:del:{technician_id}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="tc:list")],
    ])

def client_card_edit_technician_pick_kb(
    client_id: str,
    address_key: str,
    technicians: List[Dict[str, Any]],
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, it in enumerate(technicians):
        rows.append([
            InlineKeyboardButton(
                text=f"{it.get('full_name')} · {it.get('phone')}",
                callback_data=f"cc:edittechsel:{client_id}:{address_key}:{idx}",
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="Не обслуживаем",
            callback_data=f"cc:edittechskip:{client_id}:{address_key}",
        )
    ])
    rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"cc:view:{client_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def client_card_edit_technician_address_kb(client_id: str, addresses: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, address in enumerate(addresses[:30]):
        rows.append([
            InlineKeyboardButton(
                text=address,
                callback_data=f"cc:edittechaddr:{client_id}:{idx}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"cc:view:{client_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def client_card_technician_pick_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for it in CLIENTS_DB.list_technicians()[:50]:
        rows.append([InlineKeyboardButton(text=f"{it.get('full_name')} · {it.get('phone')}", callback_data=f"cc:tech:sel:{it.get('id')}")])
    rows.append([InlineKeyboardButton(text="— По умолчанию (ТЕСТ)", callback_data="cc:tech:skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def client_card_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def client_card_skip_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⏭ Пропустить")], [KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def _cc_is_cancel(text: Optional[str]) -> bool:
    return (text or "").strip().lower() in {"/cancel", "отмена", "❌ отмена"}


def _cc_is_skip(text: Optional[str]) -> bool:
    return (text or "").strip().lower() in {"пропустить", "⏭ пропустить", "-"}

def client_card_actions_kb(client_id: str, role: str) -> InlineKeyboardMarkup:
    if role in {"admin", "sales_rep"}:
        rows = [[InlineKeyboardButton(text="➕ Контакт", callback_data=f"cc:addcontact:{client_id}")]]
        rows.append([InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"cc:edit:{client_id}")])
        rows.append([InlineKeyboardButton(text="✏️ Привязать к сети", callback_data=f"cc:net:{client_id}")])
        rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"cc:del:{client_id}")])
    else:
        rows = []
    back_target = "menu:back" if role == "client" else "cc:list"
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_target)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def client_cards_list_kb(items: List[Dict[str, Any]], role: str, page: int = 0, page_size: int = 20) -> InlineKeyboardMarkup:
    total = len(items)
    last_page = max(0, (total - 1) // page_size) if total else 0
    page = max(0, min(page, last_page))
    start = page * page_size
    end = min(total, start + page_size)
    rows: List[List[InlineKeyboardButton]] = []
    for it in items[start:end]:
        title = f"{it.get('legal_form')} {it.get('legal_name')}"
        rows.append([InlineKeyboardButton(text=title[:60], callback_data=f"cc:view:{it.get('id')}")])
    if total > page_size:
        nav: List[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"cc:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{last_page + 1}", callback_data="cc:list:noop"))
        if page < last_page:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"cc:list:{page + 1}"))
        rows.append(nav)
    if role in {"admin", "sales_rep"}:
        rows.append([InlineKeyboardButton(text="➕ Новая карточка", callback_data="cc:new")])
    if role in {"admin", "sales_rep"}:
        rows.append([InlineKeyboardButton(text="📥 Импорт из дебиторки", callback_data="cc:import:debt")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_sales_rep_input(raw: str) -> Tuple[Optional[int], str]:
    txt = (raw or "").strip()
    if not txt:
        return None, ""
    m = re.search(r"(\d{4,})", txt)
    uid = int(m.group(1)) if m else None
    name = re.sub(r"\(.*?\)", "", txt).strip()
    return uid, name

def _extract_legal_form_and_name(raw: str) -> Tuple[str, str]:
    txt = re.sub(r"\s+", " ", (raw or "").strip())
    txt = re.sub(r"^\d+\s+", "", txt)
    m = re.match(r"^[«\"'\s]*(ООО|ИП)\b[\s\.\-]*([^\n]+)$", txt, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper(), m.group(2).strip(" -")
    m_full = re.match(
        r"^[«\"'\s]*(индивидуальный\s+предприниматель|общество\s+с\s+ограниченной\s+ответственностью)\b[\s\.\-]*([^\n]+)$",
        txt,
        flags=re.IGNORECASE,
    )
    if m_full:
        form = "ИП" if "предприниматель" in m_full.group(1).casefold() else "ООО"
        return form, m_full.group(2).strip(" -")
    if re.search(r"\b(ИП|индивидуальный\s+предприниматель)\b", txt, flags=re.IGNORECASE):
        return "ИП", txt
    if re.search(
            r"\b(ООО|общество\s+с\s+ограниченной\s+ответственностью)\b",
            txt,
            flags=re.IGNORECASE,
    ):
        return "ООО", txt
    return "ООО", txt

def _normalize_legal_name(legal_name: str) -> str:
    txt = (legal_name or "").strip()
    if not txt:
        return ""
    txt = re.sub(
        r"\b(ООО|ИП|индивидуальный\s+предприниматель|общество\s+с\s+ограниченной\s+ответственностью)\b",
        " ",
        txt,
        flags=re.IGNORECASE,
    )
    txt = re.sub(r"\s+", " ", txt).strip(" -")
    return txt


def _extract_sales_rep_and_address(raw: str) -> Tuple[str, str, str]:
    txt = (raw or "").strip()
    inside = ""
    m = re.search(r"\(([^)]+)\)\s*$", txt)
    if m:
        inside = m.group(1).strip()
        txt = txt[:m.start()].strip()

    sales_rep = ""
    if "-" in txt:
        left, right = txt.rsplit("-", 1)
        txt = left.strip()
        sales_rep = right.strip()

    address = inside
    return txt, sales_rep, address


def parse_client_row_for_card(raw: str) -> Optional[Dict[str, str]]:
    txt = (raw or "").strip()
    if not txt:
        return None
    legal_form, legal_name = _extract_legal_form_and_name(txt)
    legal_name, sales_rep, address = _extract_sales_rep_and_address(legal_name)
    legal_name = _normalize_legal_name(legal_name)

    if not legal_name:
        return None

    return {
        "legal_form": legal_form if legal_form in {"ООО", "ИП"} else "ООО",
        "legal_name": legal_name,
        "store_name": "",
        "address": address,
        "sales_rep_name": sales_rep,
    }


def _normalize_person_text(value: str) -> str:
    return " ".join((value or "").strip().casefold().split())


def _extract_surname(value: str) -> str:
    normalized = _normalize_person_text(value)
    if not normalized:
        return ""
    m = re.search(r"[a-zа-яё]+", normalized, flags=re.IGNORECASE)
    return m.group(0) if m else ""


def _can_import_debt_row_for_user(*, user_id: int, role: str, raw_client_name: str, parsed: Dict[str, str]) -> bool:
    if role == "admin":
        return True

    if role == "sales_rep":
        rec = _user_record(user_id)
        user_surname = (
            _extract_surname(rec.get("name") or "")
            or _extract_surname(rec.get("last_name") or "")
            or _extract_surname(rec.get("first_name") or "")
        )
        sales_rep_surname = _extract_surname(parsed.get("sales_rep_name") or "")
        if not user_surname or not sales_rep_surname:
            return False
        return sales_rep_surname == user_surname

    if role == "client":
        cname = _normalize_person_text(get_client_name(user_id))
        if not cname:
            return False
        legal_name = _normalize_person_text(parsed.get("legal_name") or "")
        raw = _normalize_person_text(raw_client_name)
        return cname in legal_name or cname in raw

    return False

def import_clients_from_latest_debt(owner_user_id: int, role: str) -> Tuple[int, int]:
    path = find_latest_download(report_type="debt")
    if not path:
        return 0, 0
    df, _ = read_debt_file(path)
    items = parse_clients(df)
    created = 0
    skipped = 0
    for it in items:
        raw_client = it.get("client") or ""
        parsed = parse_client_row_for_card(raw_client)
        if not parsed:
            skipped += 1
            continue
        if not _can_import_debt_row_for_user(
                user_id=owner_user_id,
                role=role,
                raw_client_name=raw_client,
                parsed=parsed,
        ):
            skipped += 1
            continue
        CLIENTS_DB.consolidate_client_duplicates(parsed["legal_name"])
        existing = CLIENTS_DB.find_client(parsed["legal_form"], parsed["legal_name"], parsed["address"])
        if existing:
            skipped += 1
            CLIENTS_DB.set_user_link(owner_user_id, existing["id"], can_edit=True)
            if role == "sales_rep" and not existing.get("sales_rep_user_id"):
                CLIENTS_DB.update_client(existing["id"], {
                    "sales_rep_user_id": owner_user_id,
                    "sales_rep_name": parsed["sales_rep_name"],
                })
            continue
        by_name = CLIENTS_DB.find_clients_by_name(parsed["legal_name"])
        if by_name:
            CLIENTS_DB.append_address(by_name[0]["id"], parsed["address"])
            CLIENTS_DB.set_user_link(owner_user_id, by_name[0]["id"], can_edit=True)
            if role == "sales_rep" and not by_name[0].get("sales_rep_user_id"):
                CLIENTS_DB.update_client(by_name[0]["id"], {
                    "sales_rep_user_id": owner_user_id,
                    "sales_rep_name": parsed["sales_rep_name"],
                })
            skipped += 1
            continue
        payload = {
            "legal_form": parsed["legal_form"],
            "legal_name": parsed["legal_name"],
            "store_name": parsed["store_name"],
            "address": parsed["address"],
            "overdue_days": IMPORTED_CLIENT_OVERDUE_DAYS_DEFAULT,
            "technician_name": "",
            "technician_phone": "",
            "technician_id": None,
            "sales_rep_user_id": owner_user_id if role == "sales_rep" else None,
            "sales_rep_name": parsed["sales_rep_name"],
            "owner_user_id": owner_user_id,
            "network_id": None,
        }
        contact = [{"contact_name": "", "contact_phone": "", "contact_position": ""}]
        CLIENTS_DB.create_client(payload, contact)
        created += 1
    return created, skipped


def _client_cards_for_user(user_id: int, role: str) -> List[Dict[str, Any]]:
    if role == "admin":
        return CLIENTS_DB.list_clients()
    if role == "moderator":
        return CLIENTS_DB.list_clients()
    if role == "sales_rep":
        return CLIENTS_DB.list_clients(sales_rep_user_id=user_id)
    direct = CLIENTS_DB.list_clients(owner_user_id=user_id)
    if role != "client":
        return direct
    return _client_cards_for_client(user_id, direct=direct)

def _normalize_client_card_lookup(value: str) -> str:
    value = normalize_client_name(value or "")
    value = re.sub(r"\s+", " ", value).strip().casefold()
    return value


def _client_card_match_score(query: str, item: Dict[str, Any]) -> int:
    if not query:
        return -1
    names = [
        _normalize_client_card_lookup(item.get("legal_name") or ""),
        _normalize_client_card_lookup(item.get("store_name") or ""),
        _normalize_client_card_lookup(f"{item.get('legal_form') or ''} {item.get('legal_name') or ''}"),
    ]
    best = -1
    for candidate in names:
        if not candidate:
            continue
        if candidate == query:
            best = max(best, 300)
        elif candidate.startswith(query + " "):
            best = max(best, 220)
        elif query.startswith(candidate + " "):
            best = max(best, 210)
        elif query in candidate:
            best = max(best, 120)
        elif candidate in query:
            best = max(best, 110)
    return best


def _pick_best_client_card(items: List[Dict[str, Any]], query: str) -> Optional[Dict[str, Any]]:
    scored = []
    for item in items:
        score = _client_card_match_score(query, item)
        if score >= 0:
            scored.append((score, item))
    if not scored:
        return None
    scored.sort(
        key=lambda pair: (
            pair[0],
            _normalize_client_card_lookup(pair[1].get("legal_name") or ""),
            _normalize_client_card_lookup(pair[1].get("store_name") or ""),
            pair[1].get("updated_at") or "",
        ),
        reverse=True,
    )
    return scored[0][1]


def _client_cards_for_client(user_id: int, *, direct: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    direct = list(direct or [])
    query = _normalize_client_card_lookup(get_client_name(user_id))

    best_direct = _pick_best_client_card(direct, query)
    if best_direct:
        return [best_direct]
    if len(direct) == 1:
        return direct
    if direct:
        return [direct[0]]
    return []

def _format_client_card_for_user(card: Dict[str, Any], *, user_id: int, role: str) -> str:
    return format_client_card(
        card,
        viewer_role=role,
        viewer_user_id=user_id,
    )

def _has_client_card_access(user_id: int, role: str, client_id: str) -> bool:
    if role == "moderator":
        return CLIENTS_DB.get_client(client_id) is not None
    if role == "client":
        ids = {it.get("id") for it in _client_cards_for_user(user_id, role)}
        return client_id in ids
    if CLIENTS_DB.user_can_access(user_id, role, client_id):
        return True
    return False


#----------------Инлайн меню прайсы 4. Список прайсов — без админ-кнопок клиентам
def _price_list_page(items: List[Dict[str, Any]], page: int, admin: bool) -> InlineKeyboardMarkup:
    items = sorted(items, key=_price_title_key)
    total = len(items)
    page = max(0, page)
    start = page * PRICES_PAGE_SIZE
    end = min(total, start + PRICES_PAGE_SIZE)
    rows: List[List[InlineKeyboardButton]] = []

    # Нет прайсов
    if total == 0:
        if admin:
            rows.append([InlineKeyboardButton(text="➕ Добавить", callback_data="pr:add")])
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # Элементы
    for it in items[start:end]:
        text = f"📄 {it.get('title', 'Без названия')}"
        cb = f"pr:item:{it['id']}" if admin else f"pr:send:{it['id']}"
        rows.append([InlineKeyboardButton(text=text, callback_data=cb)])

    # Навигация
    nav: List[InlineKeyboardButton] = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pr:list:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pr:list:{page+1}"))
    if nav:
        rows.append(nav)

    # Админ-кнопка
    if admin:
        rows.append([InlineKeyboardButton(text="➕ Добавить", callback_data="pr:add")])

    # Назад
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _price_item_kb(pid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить", callback_data=f"pr:send:{pid}")],
        [InlineKeyboardButton(text="♻️ Обновить файл", callback_data=f"pr:replace:{pid}")],
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"pr:rename:{pid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"pr:del:{pid}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="pr:list:0")],
    ])

#-----------------------Хелперы сохранения/загрузки файлов из Telegram
def _guess_ext_from_message(m: Message) -> Optional[str]:
    if m.document and m.document.file_name:
        ext = Path(m.document.file_name).suffix.lower().lstrip(".")
        return ext or None
    if m.document and m.document.mime_type:
        mt = m.document.mime_type
        if mt.endswith("/pdf"): return "pdf"
        if mt.endswith("/jpeg"): return "jpg"
        if mt.endswith("/png"): return "png"
        if "spreadsheet" in mt or mt.endswith("excel"): return "xlsx"
    if m.photo:
        return "jpg"
    return None

async def _save_incoming_price_file(m: Message, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if m.document:
        await bot.download(m.document, destination=dest)
    elif m.photo:
        await bot.download(m.photo[-1], destination=dest)
    else:
        raise ValueError("Пришлите документ (PDF/XLS/XLSX/PNG/JPG) или фото.")



# --- Отправка длинного текста частями ---
def send_chunked_text_builder():
    async def _send(m: Message, text: str, **kwargs):
        s = text
        first = True
        while s:
            chunk = s[:MAX_TG]
            cut = chunk.rfind("\n")
            if 1500 < cut < MAX_TG:
                chunk = s[:cut]
            try:
                if first:
                    await m.answer(chunk, disable_web_page_preview=True, **kwargs)
                    first = False
                else:
                    await m.answer(chunk, disable_web_page_preview=True)
            except TelegramRetryAfter as e:
                await asyncio.sleep(getattr(e, "retry_after", 3) + 1)
                if first:
                    await m.answer(chunk, disable_web_page_preview=True, **kwargs)
                    first = False
                else:
                    await m.answer(chunk, disable_web_page_preview=True)
            s = s[len(chunk):]
    return _send

send_long = send_chunked_text_builder()

# ------------------ Логика фильтров -----------------
def client_is_overpaid(item: Dict[str, Any]) -> bool:
    overpay = float(item.get("our_debt") or 0.0)
    total = float(item.get("total_amount") or 0.0)
    return overpay > (total + 0.009)

def _visible_overdue(days: Optional[int], personal: int, min_days: int) -> bool:
    if days is None:
        return False
    if days < max(0, int(min_days)):
        return False
    return days > max(0, int(personal))



def client_has_overdue(item: Dict[str, Any], report_date: Optional[str]) -> bool:
    raw = item.get("client") or ""
    base = _base_client_name_for_debt(raw)  # если есть такая функция; иначе raw
    threshold = get_overdue_days_for_client(base)
    min_days = get_min_overdue_days()

    for d in (item.get("docs") or []):
        amt = float(d.get("amount") or 0.0)
        if amt <= 0.009:
            continue
        days = compute_days(d.get("doc_date"), report_date, d.get("days"))
        if _visible_overdue(days, threshold, min_days):
            return True
    return False




# ----------------- Карточка клиента ------------------
# ----------------- Карточка клиента ------------------
def build_client_text(item: Dict[str, Any], idx: int, report_date: Optional[str]) -> str:
    threshold = get_overdue_days_for_client(item.get('client') or '')
    docs: List[Dict[str, Any]] = item.get("docs") or []

    # суммы/флаги для статуса
    overdue_sum = 0.0
    has_any_overdue = False          # учитываем ТОЛЬКО просрочки с положительной суммой
    overpay = float(item.get("our_debt") or 0.0)
    total   = float(item.get("total_amount") or 0.0)

    # подготовим расчёты по каждой строке
    prepared_docs: List[Dict[str, Any]] = []
    for d in docs:
        days_calc = compute_days(d.get("doc_date"), report_date, d.get("days"))
        overdue_real = is_overdue(days_calc, threshold)
        amt = money0(d.get('amount'))

        if overdue_real and (amt > 0.009):
            has_any_overdue = True
            overdue_sum += amt

        prepared_docs.append({
            **d,
            "__days_calc":    days_calc,
            "__overdue_real": overdue_real,
            "__amt":          amt,
            "__is_zero_paid": (amt <= 0.009),
            "__has_overpay":  (overpay > 0.009),
        })

    # статус клиента
    is_overpaid = overpay > (total + 0.009)
    if is_overpaid:
        status_line = f"Статус: 🟡 Переплата (наш: {fmt_money(overpay)})"
    else:
        status_line = f"Статус: {'🔴 Просрочка' if has_any_overdue else '🟢 Ок'} (порог: >{threshold} дн.)"

    # заголовок карточки с разделёнными полями клиента/торгового/адреса
    client_name = (item.get("client_name") or item.get("client") or "").strip()
    sales_rep_name = (item.get("sales_rep_name") or "").strip()
    title = client_name or (item.get("client") or "")
    if sales_rep_name:
        title += f" — {sales_rep_name}"

    head = f"<b>{idx:02d}. {esc(title)}</b>\n"
    if item.get("address"):
        head += f"{esc(item['address'])}\n"
    head += status_line + "\n"

    head += (
        f"Реализаций: <b>{item.get('realizations_count') or len(docs)}</b> | "
        f"Сумма: <b>{fmt_money(total)}</b> ₽ | "
        f"Просрч.: <b>{fmt_money(overdue_sum)}</b> ₽\n"
    )
    head += f"{'Переплата: ' + fmt_money(overpay)+' ₽' if overpay > 0.009 else 'Переплаты нет'}\n"

    # несоответствие «шапка vs по строкам»
    if item.get("our_debt_hdr") is not None:
        hdr  = fmt_money(item.get("our_debt_hdr"))
        rows = fmt_money(item.get("our_debt_sum_rows"))
        if hdr != rows:
            head += f"<i>Примечание: шапка {hdr}, по строкам {rows}</i>\n"

    # строки реализаций (добавили badge у каждой строки)
    if prepared_docs:
        head += "\n<b>Реализации:</b>\n"
        for n, d in enumerate(prepared_docs, 1):
            nums         = ", ".join(d.get("doc_numbers") or []) or "—"
            doc_date_str = d.get("doc_date") or "—"
            days_txt     = str(d["__days_calc"]) if d["__days_calc"] is not None else "—"

            # цветовая метка строки: белая для нулевой суммы
            row_badge = overdue_badge(d["__days_calc"], threshold, zero_amount=d["__is_zero_paid"])

            # оформление строки — только 💰 для "ноль + переплата"
            is_zero_with_overpay = d["__is_zero_paid"] and d["__has_overpay"]
            prefix = "💰 " if is_zero_with_overpay else ""
            overdue_for_text = False if is_zero_with_overpay else bool(d["__overdue_real"])

            # текст «Просрочена — …» показываем как есть, но для «нулевая+переплата» принудительно "нет"
            overdue_for_text = False if is_zero_with_overpay else bool(d["__overdue_real"])

            line = (
                f"{row_badge} {prefix}{n}. \tРеализация товаров и услуг <code>{esc(nums)}</code> "
                f"от {esc(doc_date_str)}\tCумма долга<b>{fmt_money(d['__amt'])}</b> ₽\t|\tДней <b>{days_txt}</b>\t |\t"
                f"Просрочена — {'да' if overdue_for_text else 'нет'}"
            )
            head += line + "\n"
    else:
        head += "\n<i>Нет строк реализаций</i>\n"

    if report_date:
        head += f"\n<i>Отчёт на {esc(report_date)}</i>"
    return head

# --- Поиск/мультипоиск ---
def _tokenize_query(raw: str) -> List[str]:
    return [t for t in re.split(r"\s+", (raw or "").strip()) if t]

def parse_report_args(text: str) -> Tuple[str, List[str], Optional[float]]:
    mode = "all"
    keywords: List[str] = []
    min_override: Optional[float] = None

    if not text:
        return mode, keywords, min_override
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return mode, keywords, min_override
    q = parts[1].strip()
    if not q:
        return mode, keywords, min_override

    toks = _tokenize_query(q)
    if not toks:
        return mode, keywords, min_override

    first = toks[0].casefold()
    pos = 0
    if first == "просрочено":
        mode = "overdue"; pos = 1
    elif first in ("переплаты", "переплата", "переплачено"):
        mode = "overpaid"; pos = 1

    i = pos
    while i < len(toks):
        t = toks[i]
        low = t.casefold()
        m = re.fullmatch(r"(мин|min|минимум)[:=]?(\d+(?:[.,]\d{1,2})?)", low)
        if m:
            try:
                min_override = float(m.group(2).replace(",", "."))
            except Exception:
                pass
            i += 1
            continue
        if low in ("мин", "min", "минимум") and i + 1 < len(toks):
            nxt = toks[i + 1].replace(",", ".")
            try:
                min_override = float(nxt)
                i += 2
                continue
            except Exception:
                pass
        keywords.append(t)
        i += 1

    return mode, [k.casefold() for k in keywords], min_override

def client_matches_any_keyword(item: Dict[str, Any], keywords: List[str]) -> bool:
    if not keywords:
        return True
    name = (item.get("client_name") or item.get("client") or "").casefold()
    addr = (item.get("address") or "").casefold()
    for kw in keywords:
        if kw and (kw in name or kw in addr):
            return True
    return False

# --- Авто-обновление из почты ---
def _today_dt(h: int, m: int) -> datetime:
    now = datetime.now(TZ)
    return TZ.localize(datetime(now.year, now.month, now.day, h, m, 0))

def seconds_until_next_run(now: datetime) -> float:
    targets_today = [_today_dt(h, m) for (h, m) in CRON_TIMES]
    targets_today.sort()
    for t in targets_today:
        if now < t:
            return (t - now).total_seconds()
    tomorrow = now + timedelta(days=1)
    first_tomorrow = TZ.localize(datetime(tomorrow.year, tomorrow.month, tomorrow.day, CRON_TIMES[0][0], CRON_TIMES[0][1], 0))
    return (first_tomorrow - now).total_seconds()

async def daily_fetch_worker():
    while True:
        now = datetime.now(TZ)
        wait_s = max(1.0, seconds_until_next_run(now))
        logger.info("Daily fetch: next run in %.0f sec (now %s, tz %s)", wait_s, now.strftime("%Y-%m-%d %H:%M:%S"), TZ)
        await asyncio.sleep(wait_s)
        try:
            path = fetch_latest_file(MAIL_SUBJECT)
            logger.info("Daily fetch: downloaded %s", path)
            if path:
                set_last_update("auto")
        except Exception as e:
            logger.exception("Daily fetch failed: %s", e)
        await asyncio.sleep(2.0)

# --- Универсальный рендер отчёта ---
async def render_report(chat: Message, *, mode: str, keywords: List[str], min_debt: Optional[float] = None):
    path = find_latest_download()
    menu_kb = menu_for_message(chat)
    if not path:
        await chat.answer("Файл отчёта не найден. Сначала загрузите его (например, /refresh).", reply_markup=menu_kb)
        return

    try:
        res = process_file(path)
    except Exception as e:
        logger.exception("Ошибка при разборе файла")
        await chat.answer(f"Не удалось разобрать файл: {e}", reply_markup=menu_kb)
        return

    items: List[Dict[str, Any]] = (res or {}).get("items") or []
    if not items:
        await chat.answer("В отчёте нет данных.", reply_markup=menu_kb)
        return

    report_date = (res or {}).get("report_date")

    filtered = [it for it in items if client_matches_any_keyword(it, keywords)]

    if mode == "overdue":
        filtered = [it for it in filtered if client_has_overdue(it, report_date) and not client_is_overpaid(it)]
    elif mode == "overpaid":
        filtered = [it for it in filtered if client_is_overpaid(it)]

    def net_debt(it: Dict[str, Any]) -> float:
        total = float(it.get("total_amount") or 0.0)
        our = float(it.get("our_debt") or 0.0)
        return max(total - our, 0.0)

    eff_min = get_min_debt() if min_debt is None else max(0.0, float(min_debt))
    if mode in ("all", "overdue") and eff_min > 0.0:
        filtered = [it for it in filtered if (net_debt(it) + 0.009) >= eff_min]

    if not filtered:
        last_dt, last_kind = get_last_update()
        last_line = f"\n<i>Последнее обновление: {fmt_dt_local(last_dt)}{(' ('+last_kind+')') if last_kind else ''}</i>"
        await chat.answer("Ничего не найдено.\nПроверьте фильтры (⚙️ Фильтры) и ключевые слова" + last_line, reply_markup=menu_kb)
        return

    chips = []
    if mode == "overdue":
        chips.append("только с просрочкой")
        chips.append(f"дни ≥ {get_min_overdue_days()}")
    elif mode == "overpaid":
        chips.append("только переплаты")
    if keywords:
        chips.append("ключи: " + ", ".join(f"«{esc(k)}»" for k in keywords))
    if mode in ("all", "overdue") and eff_min > 0.0:
        chips.append(f"мин: ≥ {fmt_money(eff_min)} ₽")
    title_suffix = (" (" + "; ".join(chips) + ")") if chips else ""

    last_dt, last_kind = get_last_update()
    last_line = f"\n<i>Последнее обновление: {fmt_dt_local(last_dt)}{(' ('+last_kind+')') if last_kind else ''}</i>"

    await chat.answer(
        f"<b>Отчёт по дебиторской задолженности</b>"
        f"{' на ' + esc(report_date) if report_date else ''}{title_suffix}. Клиентов: {len(filtered)}"
        f"{last_line}",
        disable_web_page_preview=True,
        reply_markup=menu_kb,
    )

    # Внутри render_report(...) в конце, в цикле по filtered:
    for i, it in enumerate(filtered, 1):
        text = build_client_text(it, i, report_date)
        kb = client_card_kb(it, report_date)
        await send_long(chat, text, reply_markup=kb)


#Рендер «Отчёта по таре»
async def render_tara_report(chat: Message):
    # было: path = find_latest_download(report_type="tara")
    paths = find_latest_downloads(report_type="tara", max_count=5)
    if not paths:
        await chat.answer(
            "Файл по таре не найден. Обновите: 🔄 Обновить → Тара или /refresh tara",
            reply_markup=main_menu_kb()
        )
        return

    last_err = None
    for path in paths:
        try:
            #загрузка
            res = process_tara_file(path)
            # успех — шлём заголовок
            items = (res or {}).get("items") or []
            if not items:
                continue
            report_date = (res or {}).get("report_date")
            last_dt, last_kind = get_last_update()
            last_line = f"\n<i>Последнее обновление: {fmt_dt_local(last_dt)}{(' ('+last_kind+')') if last_kind else ''}</i>"
            await chat.answer(
                f"<b>Отчёт по возвратной таре</b>{(' на ' + esc(report_date)) if report_date else ''}\n"
                f"Источник: <code>{esc(os.path.basename(path))}</code>\n"
                f"Клиентов: {len(items)}{last_line}",
                disable_web_page_preview=True,
                reply_markup=main_menu_kb()
            )
            # группируем по базовому имени (без адресов/«Колягин»)
            groups = {}
            for b in items:
                base = _tara_base_name(b.get("client") or "")
                groups.setdefault(base, []).append(b)

            for base in sorted(groups.keys(), key=lambda k: (k or '').casefold().replace('ё','е')):
                text = build_tara_group_text(base, groups[base])
                await send_long(chat, text)
            return

        except PermissionError:
            # файл может быть открыт Excel (~$ lock) — идём к следующему
            last_err = "Файл занят другим процессом (Excel открыт?)"
            continue
        except Exception as e:
            last_err = str(e)
            continue

    await chat.answer(
        f"Не удалось разобрать файл(ы) тары. Последняя ошибка: {esc(str(last_err) or 'unknown')}",
        reply_markup=main_menu_kb()
    )


# --- Хелперы ---
def _has(text: Optional[str], *needles: str) -> bool:
    t = (text or "").strip().casefold().replace("ё", "е")
    return any(n.strip().casefold().replace("ё","е") in t for n in needles)

def get_client_names() -> List[str]:
    path = find_latest_download()
    if not path:
        return []
    try:
        res = process_file(path)
    except Exception:
        return []
    items: List[Dict[str, Any]] = res.get("items") or []
    names = [it.get("client") for it in items if it.get("client")]
    return names

def _short(text: str, maxlen: int = 40) -> str:
    t = text or ""
    return t if len(t) <= maxlen else (t[:maxlen - 1] + "…")

from typing import Optional, Dict, Any, List

def overdue_badge(days: Optional[int], personal_threshold: int, *, zero_amount: bool = False) -> str:
    """
    Цвет по возрасту долга относительно персональной отсрочки:
    <T      → ⚪ белый
    T..T+6  → 🟡 жёлтый
    T+7..29 → 🔴 красный
    30+     → 🟥 ярко-красный
    """
    if zero_amount:
        return "⚪"  # нулевая сумма всегда белая
    if days is None:
        return "🟢"
    d = int(days)
    T = max(0, int(personal_threshold))
    if d < T:
        return "🟢"
    if d < T + 7:
        return "🟡"
    if d < 30:
        return "🔴"
    return "🔴"

def client_badge_for_item(item: Dict[str, Any], report_date: Optional[str]) -> str:
    client = item.get("client") or ""
    personal = get_overdue_days_for_client(client)
    max_days = None
    for d in (item.get("docs") or []):
        amt = float(d.get("amount") or 0.0)
        if amt <= 0.009:
            continue
        days = compute_days(d.get("doc_date"), report_date, d.get("days"))
        if days is None:
            continue
        if (max_days is None) or (days > max_days):
            max_days = days
    return overdue_badge(max_days, personal)


def build_edit_keyboard(page: int, names: List[str], page_size: int = 10) -> InlineKeyboardMarkup:
    total = len(names)
    start = max(0, page * page_size)
    end = min(total, start + page_size)
    rows: List[List[InlineKeyboardButton]] = []
    for i, name in enumerate(names[start:end], start):
        days = get_overdue_days_for_client(name or "")
        rows.append([InlineKeyboardButton(text=f"{i+1}. {_short(name)} · {days} дн.", callback_data=f"od:sel:{i}")])
    nav: List[InlineKeyboardButton] = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"od:pick:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"od:pick:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def is_admin(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    # 1) если явно указан в ADMIN_IDS — админ
    if _ADMIN_IDS and int(user_id) in _ADMIN_IDS:
        return True
    # 2) иначе по сохранённой роли онбординга
    return user_has_permission(user_id, "admin")
  # если список пуст — разрешаем всем

def _is_client(msg: Message) -> bool:
    return get_user_role(getattr(msg.from_user, "id", None)) in {"client", "sales_rep"}

def _is_client_only(msg: Message) -> bool:
    return get_user_role(getattr(msg.from_user, "id", None)) == "client"

def menu_for_role(role: str, user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    role = (role or "").strip().lower()
    if role == "admin":
        return main_menu_kb(user_id)
    if role == "moderator":
        return moderator_menu_kb(user_id)
    if role == "sales_rep":
        return sales_rep_menu_kb(user_id)
    if role == "guest":
        return guest_menu_kb(user_id)
    return client_menu_kb(user_id)

def menu_for_message(msg: Message) -> ReplyKeyboardMarkup:
    return menu_for_user_id(getattr(msg.from_user, "id", None))

def menu_for_user_id(user_id: Optional[int]) -> ReplyKeyboardMarkup:
    return menu_for_role(get_user_role(user_id), user_id=user_id)

def menu_for_callback(cq: CallbackQuery) -> ReplyKeyboardMarkup:
    return menu_for_user_id(getattr(cq.from_user, "id", None))

async def push_user_menu_refresh(user_id: Any, text: str = "🔄 Ваше меню обновлено.") -> None:
    uid = str(user_id or "").strip()
    if not uid.isdigit():
        return
    try:
        await bot.send_message(int(uid), text, reply_markup=menu_for_user_id(int(uid)))
    except Exception:
        logger.exception("menu-refresh: failed for user=%s", uid)

ACCESS_MATRIX: Dict[str, set] = {
    "prices.view": {"guest", "client", "sales_rep", "moderator", "admin"},
    "promos.view": {"guest", "client", "sales_rep", "moderator", "admin"},
    "schedule.view": {"guest", "client", "sales_rep", "moderator", "admin"},
    "search.debt": {"client", "sales_rep", "moderator", "admin"},
    "search.tara": {"client", "sales_rep", "moderator", "admin"},
    "reports.general": {"admin"},
    "reports.overdue": {"admin", "sales_rep", "moderator"},
    "reports.overpaid": {"admin", "sales_rep", "moderator"},
    "reports.tara": {"admin"},
    "updates.mail": {"admin"},
    "ttn.lookup": {"admin", "sales_rep", "moderator"},
    "client_cards.view": {"admin", "sales_rep", "moderator", "client"},
    "client_cards.manage": {"admin", "sales_rep"},
    "technicians.manage": {"admin"},
    "users.view": {"admin", "moderator"},
    "users.manage": {"admin"},
    "notifications.manage": {"admin", "moderator"},
}

ACCESS_LABELS: Dict[str, str] = {
    "prices.view": "прайсы",
    "promos.view": "акции",
    "schedule.view": "график развоза",
    "search.debt": "поиск по дебиторке",
    "search.tara": "поиск по таре",
    "reports.general": "общий отчёт",
    "reports.overdue": "отчёт по просрочке",
    "reports.overpaid": "отчёт по переплатам",
    "reports.tara": "отчёт по таре",
    "updates.mail": "обновление из почты",
    "ttn.lookup": "проверка ТТН",
    "client_cards.view": "карточки клиентов",
    "client_cards.manage": "управление карточками клиентов",
    "technicians.manage": "управление техниками",
    "users.view": "просмотр пользователей",
    "users.manage": "управление пользователями",
    "notifications.manage": "управление уведомлениями",
}


def _allowed_roles_for(action: str) -> set:
    return set(ACCESS_MATRIX.get(action) or set())


def role_allows_action(role: Optional[str], action: str) -> bool:
    return role in _allowed_roles_for(action)

def user_allows_action(user_id: Optional[int], action: str) -> bool:
    role = get_user_role(user_id)
    base_allowed = role_allows_action(role, action)
    if user_id is None:
        return base_allowed
    overrides = get_user_access_overrides(user_id)
    if action in overrides:
        return overrides[action]
    return base_allowed

def _deny_text(action: str, role: Optional[str]) -> str:
    label = ACCESS_LABELS.get(action) or "это действие"
    role_name = role_label(role)
    return f"⛔ У роли «{role_name}» нет доступа к разделу «{label}»."


async def deny_message_access(m: Message, action: str) -> None:
    role = get_user_role(getattr(m.from_user, "id", None))
    await m.answer(_deny_text(action, role), reply_markup=menu_for_message(m))


async def deny_callback_access(cq: CallbackQuery, action: str, *, show_alert: bool = True) -> None:
    role = get_user_role(getattr(cq.from_user, "id", None))
    text = _deny_text(action, role)
    await cq.answer(text, show_alert=show_alert)
    try:
        await cq.message.answer(text, reply_markup=menu_for_callback(cq))
    except Exception:
        logger.exception("access: failed to send callback deny message")


async def ensure_message_access(
        m: Message,
        action: str,
        *,
        state: Optional[FSMContext] = None,
) -> Optional[str]:
    user_id = getattr(m.from_user, "id", None)
    role = get_user_role(user_id)
    if user_allows_action(user_id, action):
        return role
    if state is not None:
        await state.clear()
    await deny_message_access(m, action)
    return None


async def ensure_callback_access(
        cq: CallbackQuery,
        action: str,
        *,
        state: Optional[FSMContext] = None,
        show_alert: bool = True,
) -> Optional[str]:
    user_id = getattr(cq.from_user, "id", None)
    role = get_user_role(user_id)
    if user_allows_action(user_id, action):
        return role
    if state is not None:
        await state.clear()
    await deny_callback_access(cq, action, show_alert=show_alert)
    return None

def client_name_prompt_text() -> str:
    return (
        "Введите название вашей организации без «ИП»/«ООО», "
        "например: <code>себекин</code> или <code>большая рыба</code>.\n\n"
        "Если пока хотите пользоваться ботом без привязки к организации, нажмите «👋 Остаться гостем»."
    )

async def _continue_after_phone(m: Message, state: FSMContext) -> None:
    update_user_profile_from_message(m)
    uid = getattr(m.from_user, "id", None)
    key = str(uid) if uid is not None else None
    data = _roles_load()
    rec = (data.get(key) if key else {}) or {}
    role = normalize_role(rec.get("role") or "guest")
    if uid is not None and _ADMIN_IDS and uid in _ADMIN_IDS and key and role != "admin":
        rec["role"] = "admin"
        _roles_merge_and_save({key: rec})
        role = "admin"
    elif not rec.get("role") and key:
        rec["role"] = role
        _roles_merge_and_save({key: rec})

    if role == "admin":
        await m.answer(help_text_admin(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "moderator":
        await m.answer(help_text_moderator(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=moderator_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "guest":
        await m.answer(help_text_guest(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=guest_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = rec.get("name") or get_client_name(uid)
    if not cname:
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text(), reply_markup=organization_guest_choice_kb())
        return
    await m.answer(help_text_client(getattr(getattr(m, "from_user", None), "first_name", None), cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))

# --- Хендлеры ---
@router.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    await state.clear()
    update_user_profile_from_message(m)

    uid = getattr(m.from_user, "id", None)
    key = str(uid) if uid is not None else None
    global _USER_ROLES
    _USER_ROLES = _roles_load()
    rec = (_USER_ROLES.get(key) if key else {}) or {}
    role = normalize_role(rec.get("role") or "guest")
    if rec.get("blocked"):
        await m.answer(BLOCKED_USER_TEXT)
        return

    # Админ по whitelist (_ADMIN_IDS) — фиксируем и сохраняем, чтобы не спрашивать снова.
    if uid is not None and _ADMIN_IDS and uid in _ADMIN_IDS:
        if role != "admin":
            rec["role"] = "admin"
            _USER_ROLES[key] = rec
            _save_user_roles(_USER_ROLES)
        role = "admin"
    # Первый визит: запрос номера телефона
    if uid is not None and not rec.get("phone"):
        await state.set_state(OnboardStates.waiting_phone_contact)
        await send_phone_request(m)
        return
    # Первый визит: НЕТ записи или НЕТ поля role -> спрашиваем 1 раз.
    if not rec.get("role") and key:
        rec["role"] = role
        _USER_ROLES[key] = rec
        _save_user_roles(_USER_ROLES)

    # Известная роль — показываем соответствующее меню.
    if role == "admin":
        await m.answer(help_text_admin(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "moderator":
        await m.answer(help_text_moderator(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=moderator_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "guest":
        await m.answer(help_text_guest(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=guest_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = rec.get("name") or get_client_name(uid)
    if not cname:
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text(), reply_markup=organization_guest_choice_kb())
        return
    await m.answer(help_text_client(getattr(getattr(m, "from_user", None), "first_name", None), cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))


@router.message(Command("help"))
async def on_help(m: Message):
    if is_user_blocked(getattr(m.from_user, "id", None)):
        await m.answer(BLOCKED_USER_TEXT)
        return
    update_user_profile_from_message(m)
    role = get_user_role(getattr(m.from_user, "id", None))
    if role == "admin":
        await m.answer(help_text_admin(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "moderator":
        await m.answer(help_text_moderator(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=moderator_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "guest":
        await m.answer(help_text_guest(getattr(getattr(m, "from_user", None), "first_name", None)), reply_markup=guest_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = get_client_name(getattr(m.from_user, "id", None))
    await m.answer(help_text_client(getattr(getattr(m, "from_user", None), "first_name", None), cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))


# --- Онбординг роли/пароля/названия ---
@router.callback_query(F.data == "ob:admin")
async def ob_admin(cq: CallbackQuery, state: FSMContext):
    await state.set_state(OnboardStates.waiting_admin_password)
    await cq.message.edit_text("Введите пароль администратора:")
    await cq.answer()

@router.callback_query(F.data == "ob:client")
async def ob_client(cq: CallbackQuery, state: FSMContext):
    await state.set_state(OnboardStates.waiting_client_name)
    await cq.message.edit_text(client_name_prompt_text())
    await cq.answer()

@router.message(OnboardStates.waiting_phone_contact, F.contact)
async def ob_phone_contact(m: Message, state: FSMContext):
    is_new_user = not bool(_user_record(m.from_user.id))
    contact = m.contact
    if contact.user_id and contact.user_id != m.from_user.id:
        await m.answer("Пожалуйста, отправьте <b>ваш</b> контакт через кнопку.")
        return
    ok, e164, disp = normalize_phone_ru(contact.phone_number or "")
    if not ok:
        await m.answer("Не удалось распознать номер. Попробуйте ещё раз.")
        return
    set_user_phone(m.from_user.id, e164, verified=True)
    await m.answer(f"✅ Номер сохранён: {disp}", reply_markup=ReplyKeyboardRemove())
    if is_new_user and not (_ADMIN_IDS and m.from_user.id in _ADMIN_IDS):
        await notify_admins_about_new_user(m.from_user.id)
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text(), reply_markup=organization_guest_choice_kb())
        return
    await state.clear()
    if is_new_user:
        await notify_admins_about_new_user(m.from_user.id)
    await _continue_after_phone(m, state)

@router.message(OnboardStates.waiting_phone_contact)
async def ob_phone_contact_text(m: Message, state: FSMContext):
    is_new_user = not bool(_user_record(m.from_user.id))
    ok, e164, disp = normalize_phone_ru(m.text or "")
    if not ok:
        await m.answer("Нужно отправить контакт кнопкой или введите номер в формате +7XXXXXXXXXX.")
        return
    set_user_phone(m.from_user.id, e164, verified=False)
    await m.answer(f"✅ Номер сохранён: {disp}", reply_markup=ReplyKeyboardRemove())
    if is_new_user and not (_ADMIN_IDS and m.from_user.id in _ADMIN_IDS):
        await notify_admins_about_new_user(m.from_user.id)
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text(), reply_markup=organization_guest_choice_kb())
        return
    await state.clear()
    if is_new_user:
        await notify_admins_about_new_user(m.from_user.id)
    await _continue_after_phone(m, state)

@router.message(OnboardStates.waiting_admin_password)
async def ob_admin_pwd(m: Message, state: FSMContext):
    if (m.text or "").strip() == ADMIN_ONBOARD_PASSWORD:
        set_user_role(m.from_user.id, "admin")
        await state.clear()
        await m.answer("✅ Админ доступ выдан.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        await on_start(m, state)
    else:
        await m.answer("❌ Неверный пароль. Попробуйте снова или выберите «Я клиент».",
                       reply_markup=onboard_role_kb())

@router.message(OnboardStates.waiting_client_name)
async def ob_client_name(m: Message, state: FSMContext):
    raw_name = (m.text or "").strip()
    if raw_name == "👋 Остаться гостем":
        set_user_role(m.from_user.id, "guest")
        await state.clear()
        await m.answer(
            "✅ Оставил вас гостем. Название организации можно добавить позже.",
            reply_markup=guest_menu_kb(getattr(m.from_user, "id", None))
        )
        await on_start(m, state)
        return
    name = normalize_client_name(raw_name)
    was_corrected = client_name_was_corrected(raw_name, name)
    if not name or len(name) < 2:
        await m.answer(
            "Введите корректное название организации (минимум 2 символа) "
            "или нажмите «👋 Остаться гостем».",
            reply_markup=organization_guest_choice_kb(),
        )
        return

    # сохраняем роль и имя клиента
    set_user_role(m.from_user.id, "client")
    set_client_name(m.from_user.id, name)

    await state.clear()

    # Сообщение + клиентское меню
    saved_text = f"✅ Сохранено: «{esc(name)}». Режим клиента активирован."
    if was_corrected:
        saved_text = (
            f"✅ Сохранено: «{esc(name)}». "
            "Убрал из названия префикс «ООО/ИП», чтобы сохранить только имя организации. "
            "Режим клиента активирован."
        )
    await m.answer(
        saved_text,
        reply_markup=client_menu_kb(getattr(m.from_user, "id", None))
    )

    # Автоматически показать стартовый экран/хелп клиента
    await on_start(m, state)

##---------------Обработчики сообщений/колбэков “Прайсы”-------------------
# Кнопка в меню
@router.message(F.text == "📑 Прайсы", StateFilter(None))
async def btn_prices(m: Message):
    if not await ensure_message_access(m, "prices.view"):
        return
    admin = is_admin(getattr(m.from_user, "id", None))
    items = _price_get_all()
    kb = _price_list_page(items, page=0, admin=admin)
    await m.answer(
        "<b>Прайс-листы</b>\nВыберите нужный файл:",
        reply_markup=kb
    )


# Пагинация списка
@router.callback_query(F.data.startswith("pr:list:"), StateFilter(None))
async def cb_prices_list(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "prices.view"):
        return
    page = int(cq.data.split(":")[-1])
    admin = is_admin(getattr(cq.from_user, "id", None))
    items = _price_get_all()
    await cq.message.edit_text("<b>Прайс-листы</b>\nВыберите пункт:",
                               reply_markup=_price_list_page(items, page, admin),
                               disable_web_page_preview=True)
    await cq.answer()

# Клиент: отправка файла (и в админском подменю тоже)
@router.callback_query(F.data.startswith("pr:send:"))
async def cb_price_send(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "prices.view"):
        return
    pid = cq.data.split(":")[-1]
    it = _price_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True)
        return
    path = PRICES_DIR / it["filename"]
    try:
        await cq.message.answer_document(FSInputFile(path), caption=it["title"])
    except Exception as e:
        await cq.message.answer(f"Не удалось отправить файл: {esc(str(e))}")
    await cq.answer()

# Админ: открыть карточку элемента
@router.callback_query(F.data.startswith("pr:item:"))
async def cb_price_item(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _price_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True); return
    text = f"<b>{esc(it['title'])}</b>\nФайл: <code>{esc(it['filename'])}</code>"
    await cq.message.edit_text(text, reply_markup=_price_item_kb(pid), disable_web_page_preview=True)
    await cq.answer()

# Админ: добавить
@router.callback_query(F.data == "pr:add")
async def cb_price_add(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Только для админов", show_alert=True); return
    await state.set_state(PriceStates.waiting_new_title)
    await cq.message.answer("Введите <b>название прайса</b> (как увидят клиенты):")
    await cq.answer()

@router.message(PriceStates.waiting_new_title)
async def price_new_title(m: Message, state: FSMContext):
    if not is_admin(getattr(m.from_user, "id", None)):
        await state.clear(); await m.answer("Добавление прайсов только для администраторов."); return
    title = (m.text or "").strip()
    if len(title) < 2:
        await m.answer("Слишком коротко. Введите название ещё раз."); return
    await state.update_data(new_title=title)
    await state.set_state(PriceStates.waiting_new_file)
    await m.answer("Теперь пришлите <b>файл прайса</b> (PDF/XLS/XLSX/PNG/JPG) или фото с прайсом.")

@router.message(PriceStates.waiting_new_file)
async def price_new_file(m: Message, state: FSMContext):
    if not is_admin(getattr(m.from_user, "id", None)):
        await state.clear(); await m.answer("Добавление прайсов только для администраторов."); return
    data = await state.get_data()
    title = data.get("new_title","").strip()
    ext = _guess_ext_from_message(m)
    if not ext or ext not in ALLOWED_PRICE_EXT:
        await m.answer("Нужен файл формата: pdf, xls, xlsx, png, jpg, jpeg."); return

    pid = uuid.uuid4().hex[:12]
    filename = f"{pid}.{ext}"
    dest = PRICES_DIR / filename
    try:
        await _save_incoming_price_file(m, dest)
    except Exception as e:
        await m.answer(f"Не удалось сохранить файл: {esc(str(e))}"); return

    now = datetime.now(TZ).isoformat()
    _price_set({
        "id": pid, "title": title, "filename": filename,
        "created_at": now, "updated_at": now
    })
    await state.clear()
    await m.answer(f"✅ Прайс «{esc(title)}» добавлен.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

# Админ: заменить файл
@router.callback_query(F.data.startswith("pr:replace:"))
async def cb_price_replace(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    if not _price_find(pid):
        await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(replace_id=pid)
    await state.set_state(PriceStates.waiting_replace_file)
    await cq.message.answer("Пришлите новый файл (PDF/XLS/XLSX/PNG/JPG). Старый будет перезаписан.")
    await cq.answer()

@router.message(PriceStates.waiting_replace_file)
async def price_do_replace(m: Message, state: FSMContext):
    if not is_admin(getattr(m.from_user, "id", None)):
        await state.clear(); await m.answer("Редактирование прайсов только для администраторов."); return
    data = await state.get_data()
    pid = data.get("replace_id")
    it = _price_find(pid)
    if not it:
        await state.clear(); await m.answer("Элемент не найден."); return
    ext = _guess_ext_from_message(m)
    if not ext or ext not in ALLOWED_PRICE_EXT:
        await m.answer("Нужен файл формата: pdf, xls, xlsx, png, jpg, jpeg."); return

    new_name = f"{pid}.{ext}"
    dest = PRICES_DIR / new_name
    try:
        await _save_incoming_price_file(m, dest)
    except Exception as e:
        await m.answer(f"Ошибка: {esc(str(e))}"); return

    it["filename"] = new_name
    it["updated_at"] = datetime.now(TZ).isoformat()
    _price_set(it)
    await state.clear()
    await m.answer("✅ Файл обновлён.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

# Админ: переименовать
@router.callback_query(F.data.startswith("pr:rename:"))
async def cb_price_rename(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _price_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(rename_id=pid)
    await state.set_state(PriceStates.waiting_rename)
    await cq.message.answer(f"Текущее название: «{esc(it['title'])}».\nВведите новое название:")
    await cq.answer()

@router.message(PriceStates.waiting_rename)
async def price_do_rename(m: Message, state: FSMContext):
    if not is_admin(getattr(m.from_user, "id", None)):
        await state.clear(); await m.answer("Редактирование прайсов только для администраторов."); return
    data = await state.get_data()
    pid = data.get("rename_id")
    it = _price_find(pid)
    if not it:
        await state.clear(); await m.answer("Элемент не найден."); return
    title = (m.text or "").strip()
    if len(title) < 2:
        await m.answer("Слишком коротко. Введите название ещё раз."); return
    it["title"] = title
    it["updated_at"] = datetime.now(TZ).isoformat()
    _price_set(it)
    await state.clear()
    await m.answer("✅ Название обновлено.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

# Админ: удалить
@router.callback_query(F.data.startswith("pr:del:"))
async def cb_price_del(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("⛔ Только для администраторов", show_alert=True)
        return

    pid = cq.data.split(":")[-1]
    it = _price_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True)
        return

    await state.update_data(del_id=pid)
    await state.set_state(PriceStates.waiting_delete_confirm)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data="pr:confirm_del:yes"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="pr:confirm_del:no"),
            ]
        ]
    )
    await cq.message.answer(f"Удалить «{it['title']}»?", reply_markup=kb)
    await cq.answer()


@router.callback_query(F.data.startswith("pr:confirm_del:"))
async def cb_price_del_confirm(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("⛔ Только для администраторов", show_alert=True)
        return

    action = cq.data.split(":")[-1]
    data = await state.get_data()
    pid = data.get("del_id")
    await state.clear()

    if action == "no":
        await cq.message.answer("❎ Отменено.")
        await cq.answer()
        return

    it = _price_find(pid)
    if not it:
        await cq.message.answer("⚠️ Элемент не найден.")
        await cq.answer()
        return

    try:
        (PRICES_DIR / it["filename"]).unlink(missing_ok=True)
    except Exception as e:
        await cq.message.answer(f"Ошибка удаления файла: {e}")

    _price_delete(pid)
    await cq.message.answer(f"✅ Прайс «{it['title']}» удалён.")
    await cq.answer()


@router.message(Command("prices"))
async def cmd_prices(m: Message):
    await btn_prices(m)


@router.message(StateFilter(None), F.document | F.photo)
async def block_client_uploads(m: Message, state: FSMContext):
    if is_admin_event(m):
        return  # админам в "нулевом" состоянии не мешаем

    user_id = getattr(m.from_user, "id", None)
    username = getattr(m.from_user, "username", None)
    logger.info(f"Блокировка файла от клиента: user_id={user_id}, username={username}, content_type={m.content_type}")

    await m.answer("❌ Клиентам нельзя присылать файлы. Напишите текстом, пожалуйста.")
    await m.answer("📂 Отправка файлов недоступна. Используйте меню «📑 Прайсы».")


#---------------------------------------

# Кнопки (гибкий матч)
@router.message(F.text.func(lambda t: isinstance(t, str) and (t.startswith("▶️") or "старт" in t.lower())))
async def btn_start(m: Message, state: FSMContext):
    await on_start(m, state)

@router.message(F.text.func(lambda t: _has(t, "общий отчет", "общий отчёт") or (t or "").startswith("🧾")))
async def btn_all(m: Message):
    if not await ensure_message_access(m, "reports.general"):
        return
    await render_report(m, mode="all", keywords=[], min_debt=None)

@router.message(F.text == TARE_BTN)
async def btn_tara(m: Message):
    if not await ensure_message_access(m, "reports.tara"):
        return
    await render_tara_report(m)

@router.message(F.text == TTN_BTN)
async def btn_ttn(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "ttn.lookup", state=state):
        return
    _cleanup_flows()
    logger.info("ttn: entry by user=%s role=%s", getattr(m.from_user, "id", None), get_user_role(getattr(m.from_user, "id", None)))
    await state.set_state(TTNStates.waiting_number)
    await m.answer(
        "Введите номер ТТN.",
        reply_markup=back_only_kb()
    )

@router.message(F.text.func(lambda t: _has(t, "просрочено") or (t or "").startswith("⏰")))
async def btn_overdue(m: Message):
    if not await ensure_message_access(m, "reports.overdue"):
        return
    await render_report(m, mode="overdue", keywords=[], min_debt=None)

@router.message(F.text.func(lambda t: _has(t, "переплат") or (t or "").startswith("💰")))
async def btn_overpaid(m: Message):
    if not await ensure_message_access(m, "reports.overpaid"):
        return
    await render_report(m, mode="overpaid", keywords=[], min_debt=None)

@router.message(F.text == "🔎 Поиск")
async def btn_search(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "search.debt", state=state):
        return
    if _is_client_only(m):
        cname = get_client_name(getattr(m.from_user, "id", None))
        if cname:
            await run_client_search(m, cname)
            return
        await state.set_state(SearchStates.waiting_query)
        await m.answer("Введите часть названия/адреса для поиска:", reply_markup=menu_for_message(m))
        return

    # админ: старое поведение
    await state.set_state(SearchStates.waiting_query)
    await m.answer(
        "Введите одну или несколько подстрок для поиска (через пробел), например:\n"
        "<code>Волков Смирнов Заря</code>",
        reply_markup=back_only_kb()
    )

@router.message(SearchStates.waiting_query)
async def search_flow(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "search.debt", state=state):
        return
    q = (m.text or "").strip()
    if not q or q.startswith("/"):
        await state.clear()
        await m.answer("Поиск отменён.", reply_markup=menu_for_message(m))
        return

    if _is_client_only(m):
        await run_client_search(m, q)
        await state.clear()
        return

    keywords = [t.casefold() for t in _tokenize_query(q)]
    await render_report(m, mode="all", keywords=keywords)
    await state.clear()
    await m.answer("Готово.", reply_markup=menu_for_message(m))

# --- Поиск по возвратной таре ---
async def render_tara_search(chat: Message, keywords: List[str]):
    role = get_user_role(getattr(chat.from_user, 'id', None))
    kb = menu_for_role(role, getattr(chat.from_user, "id", None))
    paths = find_latest_downloads(report_type="tara", max_count=5)
    if not paths:
        await chat.answer(
            "Файл по таре не найден. Обновите: 🔄 Обновить → Тара или /refresh tara",
            reply_markup=kb
        )
        return

    last_err = None
    for path in paths:
        try:
            res = process_tara_file(path)
            items = (res or {}).get("items") or []
            if items is None:
                items = []
            report_date = (res or {}).get("report_date")

            kws = [k for k in (keywords or []) if k]
            def match(b: dict) -> bool:
                name = (b.get("client_name") or b.get("client") or "").strip().casefold()
                addr = (b.get("address") or "").strip().casefold()
                if not kws:
                    return False
                return any(k in name or k in addr for k in kws)

            filtered = [b for b in items if match(b)]
            if filtered:
                chips = []
                if kws:
                    chips.append("ключи: " + ", ".join(f"«{esc(k)}»" for k in kws))
                title_suffix = (" (" + "; ".join(chips) + ")") if chips else ""

                last_dt, last_kind = get_last_update()
                last_line = f"\n<i>Последнее обновление: {fmt_dt_local(last_dt)}{(' ('+last_kind+')') if last_kind else ''}</i>"

                await chat.answer(
                    f"<b>Поиск по возвратной таре</b>{(' на ' + esc(report_date)) if report_date else ''}{title_suffix}\n"
                    f"Источник: <code>{esc(os.path.basename(path))}</code>\n"
                    f"Клиентов: {len(filtered)}{last_line}",
                    disable_web_page_preview=True,
                    reply_markup=kb
                )
                groups = {}
                for b in filtered:
                    base = _tara_base_name(b.get("client") or "")
                    groups.setdefault(base, []).append(b)

                for base in sorted(groups.keys(), key=lambda k: (k or '').casefold().replace('ё','е')):
                    text = build_tara_group_text(base, groups[base])
                    await send_long(chat, text)
                return


            # если в этом файле нет совпадений — пробуем следующий
            continue
        except PermissionError:
            last_err = "Файл занят другим процессом (Excel открыт?)"
            continue
        except Exception as e:
            last_err = str(e)
            continue

    await chat.answer("Ничего не найдено по заданным ключам.", reply_markup=kb)


@router.message(F.text == "🔎 Поиск тары")
async def btn_search_tara(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "search.tara", state=state):
        return
    # Клиент: ищем по сохранённому названию клиента
    if _is_client_only(m):
        cname = get_client_name(getattr(m.from_user, "id", None))
        if not cname:
            await m.answer("Сначала укажите название: кнопка «✏️ Изменить название».", reply_markup=menu_for_message(m))
            return
        keywords = [t.casefold() for t in _tokenize_query(cname)]
        await render_tara_search(m, keywords)
        return

    # Админ: обычный интерактив с вводом строки
    await state.set_state(SearchTaraStates.waiting_query)
    await m.answer(
        "Введите часть названия клиента для поиска по <b>ведомости тары</b>.\n"
        "Можно несколько слов через пробел: <code>Волков Заря</code>",
        reply_markup=back_only_kb()
    )
@router.message(SearchTaraStates.waiting_query)
async def search_tara_flow(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "search.tara", state=state):
        return
    q = (m.text or "").strip()
    if not q or q.startswith("/"):
        await state.clear()
        await m.answer("Поиск отменён.", reply_markup=menu_for_message(m))
        return
    keywords = [t.casefold() for t in _tokenize_query(q)]
    await render_tara_search(m, keywords)
    await state.clear()
    await m.answer("Готово.", reply_markup=menu_for_message(m))


# --- Клиент: изменить название ---
@router.message(F.text == "✏️ Изменить название")
async def client_change_name(m: Message, state: FSMContext):
    if not _is_client(m):
        await m.answer("Эта команда для клиента.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    await state.set_state(ClientEditStates.waiting_new_name)
    await m.answer("Введите новое название вашей организации:", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))

@router.message(ClientEditStates.waiting_new_name)
async def client_set_new_name(m: Message, state: FSMContext):
    raw_name = (m.text or "").strip()
    name = normalize_client_name(raw_name)
    was_corrected = client_name_was_corrected(raw_name, name)
    if not name or len(name) < 2:
        await m.answer("Введите корректное название (минимум 2 символа).", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return
    set_client_name(m.from_user.id, name)
    await state.clear()
    text = f"✅ Обновлено. Название: «{esc(name)}»."
    if was_corrected:
        text += " Убрал из названия «ООО/ИП»."
    await m.answer(text, reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))

# --- Обновить (кнопка всегда разрешена), /refresh — только админ ---
async def _do_mail_refresh(m: Message):
    await m.answer("Обновляю отчёт из почты…")
    try:
        path = fetch_latest_file(MAIL_SUBJECT)
        if path:
            set_last_update("manual")
            await m.answer(f"Готово. Файл: <code>{esc(path)}</code>",
                           reply_markup=menu_for_message(m))
        else:
            await m.answer("Письмо не найдено или подходящих вложений нет.",
                           reply_markup=menu_for_message(m))
    except Exception as e:
        logger.exception("Manual refresh failed")
        await m.answer(f"Не удалось обновить: {e}",
                       reply_markup=main_menu_kb(getattr(m.from_user, "id", None)) if not _is_client(m) else client_menu_kb(getattr(m.from_user, "id", None)))

@router.message(F.text.func(lambda t: isinstance(t, str) and t.startswith("🔄 Обновить")))
async def btn_refresh(m: Message):
    if not await ensure_message_access(m, "updates.mail"):
        return
    await m.answer("Что обновить?", reply_markup=update_menu_kb())

@router.message(F.text == "🛠 Техники")
async def technicians_menu(m: Message):
    if not await ensure_message_access(m, "technicians.manage"):
        return
    await m.answer("Управление техниками:", reply_markup=technicians_menu_kb())


@router.callback_query(F.data == "tc:menu")
async def tc_menu(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "technicians.manage"):
        return
    await cq.message.answer("Управление техниками:", reply_markup=technicians_menu_kb())
    await cq.answer()


@router.callback_query(F.data == "tc:list")
async def tc_list(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "technicians.manage"):
        return
    items = CLIENTS_DB.list_technicians()
    if not items:
        await cq.message.answer("Список техников пуст. Добавьте первого техника.", reply_markup=technicians_menu_kb())
    else:
        await cq.message.answer("Техники:", reply_markup=technicians_list_kb(items))
    await cq.answer()


@router.callback_query(F.data == "tc:new")
async def tc_new(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "technicians.manage", state=state):
        return
    await state.clear()
    await state.set_state(TechnicianStates.waiting_full_name)
    await cq.message.answer("Введите имя и фамилию техника.")
    await cq.answer()


@router.callback_query(F.data.startswith("tc:view:"))
async def tc_view(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "technicians.manage"):
        return
    technician_id = cq.data.split(":", 2)[2]
    it = CLIENTS_DB.get_technician(technician_id)
    if not it:
        await cq.answer("Техник не найден", show_alert=True)
        return
    txt = f"<b>{it.get('full_name')}</b>\nТелефон: {it.get('phone')}\nТочки: {it.get('points_csv') or '—'}"
    await cq.message.answer(txt, reply_markup=technician_actions_kb(technician_id))
    await cq.answer()


@router.callback_query(F.data.startswith("tc:edit:"))
async def tc_edit(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "technicians.manage", state=state):
        return
    technician_id = cq.data.split(":", 2)[2]
    it = CLIENTS_DB.get_technician(technician_id)
    if not it:
        await cq.answer("Техник не найден", show_alert=True)
        return
    await state.clear()
    await state.update_data(edit_technician_id=technician_id)
    await state.set_state(TechnicianStates.waiting_full_name)
    await cq.message.answer(f"Новое имя и фамилия (сейчас: {it.get('full_name')}).")
    await cq.answer()


@router.callback_query(F.data.startswith("tc:del:"))
async def tc_delete(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "technicians.manage"):
        return
    technician_id = cq.data.split(":", 2)[2]
    CLIENTS_DB.delete_technician(technician_id)
    await cq.message.answer("✅ Техник удалён.")
    items = CLIENTS_DB.list_technicians()
    await cq.message.answer("Техники:", reply_markup=technicians_list_kb(items) if items else technicians_menu_kb())
    await cq.answer()


@router.message(TechnicianStates.waiting_full_name)
async def tc_wait_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "technicians.manage", state=state):
        return
    full_name = (m.text or "").strip()
    if len(full_name) < 3:
        await m.answer("Введите имя и фамилию (минимум 3 символа).")
        return
    await state.update_data(technician_full_name=full_name)
    await state.set_state(TechnicianStates.waiting_phone)
    await m.answer("Введите номер техника.")


@router.message(TechnicianStates.waiting_phone)
async def tc_wait_phone(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "technicians.manage", state=state):
        return
    phone = (m.text or "").strip()
    if len(re.sub(r"\D", "", phone)) < 10:
        await m.answer("Неверный формат номера. Повторите ввод.")
        return
    await state.update_data(technician_phone=phone)
    await state.set_state(TechnicianStates.waiting_points)
    await m.answer("Введите точки техника через запятую (или '-' если пусто).")


@router.message(TechnicianStates.waiting_points)
async def tc_wait_points(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "technicians.manage", state=state):
        return
    points = (m.text or "").strip()
    points_csv = "" if points in {"", "-"} else points
    data = await state.get_data()
    edit_id = data.get("edit_technician_id")
    if edit_id:
        CLIENTS_DB.update_technician(edit_id, data.get("technician_full_name"), data.get("technician_phone"), points_csv)
        await m.answer("✅ Техник обновлён.")
    else:
        CLIENTS_DB.create_technician(data.get("technician_full_name"), data.get("technician_phone"), points_csv)
        await m.answer("✅ Техник добавлен.")
    await state.clear()
    await m.answer("Управление техниками:", reply_markup=technicians_menu_kb())


@router.message(F.text.in_({"🏢 Клиенты", "🏢 Моя карточка"}))
async def client_cards_entry(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.view", state=state):
        return
    await state.clear()
    uid = int(getattr(m.from_user, "id", 0) or 0)
    role = get_user_role(uid)
    items = _client_cards_for_user(uid, role)
    if not items and role in {"admin", "sales_rep"}:
        await m.answer("Карточек пока нет. Создайте первую.", reply_markup=client_cards_list_kb([], role))
        return
    if not items:
        await m.answer("Вам пока не назначена карточка клиента. Обратитесь к администратору.", reply_markup=menu_for_message(m))
        return
    if role == "client":
        card = CLIENTS_DB.get_client(items[0]["id"])
        if not card:
            await m.answer("Карточка не найдена. Обратитесь к администратору.", reply_markup=menu_for_message(m))
            return
        await m.answer(
            "Моя карточка. Показываем только ваш профиль, а юрлица сети — только если у сети один владелец.",
        )
        await m.answer(
            _format_client_card_for_user(card, user_id=uid, role=role),
            reply_markup=client_card_actions_kb(card["id"], role),
        )
        return
    title = "Моя карточка:" if role == "client" else "Карточки клиентов:"
    await m.answer(title, reply_markup=client_cards_list_kb(items, role, page=0))

@router.callback_query(F.data.func(lambda d: d and d.startswith("cc:list")))
async def cc_list(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "client_cards.view"):
        return
    page = 0
    parts = (cq.data or "").split(":")
    if len(parts) >= 3 and parts[2].isdigit():
        page = int(parts[2])
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = get_user_role(uid)
    items = _client_cards_for_user(uid, role)
    if len(parts) >= 3 and parts[2] == "noop":
        await cq.answer()
        return

    title = "Моя карточка:" if role == "client" else "Карточки клиентов:"
    await cq.message.edit_text(title, reply_markup=client_cards_list_kb(items, role, page=page))
    await cq.answer()

@router.callback_query(F.data.startswith("cc:view:"))
async def cc_view(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "client_cards.view"):
        return
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = get_user_role(uid)
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.view")
        return
    card = CLIENTS_DB.get_client(client_id)
    if not card:
        await cq.answer("Карточка не найдена", show_alert=True)
        return
    await cq.message.edit_text(
        _format_client_card_for_user(card, user_id=uid, role=role),
        reply_markup=client_card_actions_kb(client_id, role),
    )
    await cq.answer()

@router.callback_query(F.data == "cc:new")
async def cc_new(cq: CallbackQuery, state: FSMContext):
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    await state.clear()
    await state.update_data(client_contacts=[])
    await state.set_state(ClientCardStates.waiting_legal_form)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ООО", callback_data="cc:lf:ООО"),
         InlineKeyboardButton(text="ИП", callback_data="cc:lf:ИП")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cc:create:cancel")],
    ])
    await cq.message.answer("Создание карточки. Выберите форму: ООО или ИП.", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data == "cc:create:cancel", ClientCardStates.waiting_legal_form)
async def cc_create_cancel(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.answer("Создание карточки отменено.")
    await cq.answer()

@router.callback_query(F.data.startswith("cc:lf:"), ClientCardStates.waiting_legal_form)
async def cc_pick_legal_form(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "client_cards.manage", state=state):
        return
    lf = cq.data.split(":", 2)[2]
    await state.update_data(legal_form=lf)
    await state.set_state(ClientCardStates.waiting_legal_name)
    await cq.message.answer("Введите юр. название клиента (без формы).", reply_markup=client_card_cancel_kb())
    await cq.answer()

@router.message(ClientCardStates.waiting_legal_name)
async def cc_legal_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    v = (m.text or "").strip()
    if len(v) < 2:
        await m.answer("Слишком короткое название.", reply_markup=client_card_cancel_kb())
        return
    await state.update_data(legal_name=v)
    await state.set_state(ClientCardStates.waiting_store_name)
    await m.answer("Введите название магазина. (Можно пропустить)", reply_markup=client_card_skip_cancel_kb())

@router.message(ClientCardStates.waiting_store_name)
async def cc_store_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    if _cc_is_skip(m.text):
        await state.update_data(store_name="—")
        await state.set_state(ClientCardStates.waiting_address)
        await m.answer("Введите адрес клиента. (Можно пропустить)", reply_markup=client_card_skip_cancel_kb())
        return
    v = (m.text or "").strip()
    if len(v) < 2:
        await m.answer("Введите корректное название магазина или нажмите «⏭ Пропустить».", reply_markup=client_card_skip_cancel_kb())
        return
    await state.update_data(store_name=v)
    await state.set_state(ClientCardStates.waiting_address)
    await m.answer("Введите адрес клиента. (Можно пропустить)", reply_markup=client_card_skip_cancel_kb())

@router.message(ClientCardStates.waiting_address)
async def cc_address(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    if _cc_is_skip(m.text):
        await state.update_data(address="—")
        await state.set_state(ClientCardStates.waiting_overdue_days)
        await m.answer("Введите кол-во дней отсрочки (число). (Можно пропустить, по умолчанию 7)",
                       reply_markup=client_card_skip_cancel_kb())
        return
    v = (m.text or "").strip()
    if len(v) < 5:
        await m.answer("Адрес слишком короткий или нажмите «⏭ Пропустить».", reply_markup=client_card_skip_cancel_kb())
        return
    await state.update_data(address=v)
    await state.set_state(ClientCardStates.waiting_overdue_days)
    await m.answer("Введите кол-во дней отсрочки. (Можно пропустить, по умолчанию 7)", reply_markup=client_card_skip_cancel_kb())

@router.message(ClientCardStates.waiting_overdue_days)
async def cc_overdue_days(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    if _cc_is_skip(m.text):
        days = 7
    else:
        try:
            days = max(0, int((m.text or "").strip()))
        except Exception:
            await m.answer("Нужно целое число или нажмите «⏭ Пропустить».", reply_markup=client_card_skip_cancel_kb())
            return
    await state.update_data(overdue_days=days)
    await state.set_state(ClientCardStates.waiting_contact_name)
    await m.answer("Введите имя контактного лица. (Можно пропустить)", reply_markup=client_card_skip_cancel_kb())

@router.message(ClientCardStates.waiting_contact_name)
async def cc_contact_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    if _cc_is_skip(m.text):
        await state.update_data(contact_name="", contact_phone="", contact_position="")
        await state.set_state(ClientCardStates.waiting_technician_select)
        await m.answer("Выберите техника для карточки.", reply_markup=ReplyKeyboardRemove())
        await m.answer("Выберите техника для карточки.", reply_markup=client_card_technician_pick_kb())
        return
    v = (m.text or "").strip()
    if len(v) < 2:
        await m.answer("Введите имя контакта или нажмите «⏭ Пропустить».", reply_markup=client_card_skip_cancel_kb())
        return
    await state.update_data(contact_name=v)
    await state.set_state(ClientCardStates.waiting_contact_phone)
    await m.answer("Введите телефон контакта (например +79990000000).", reply_markup=client_card_cancel_kb())

@router.message(ClientCardStates.waiting_contact_phone)
async def cc_contact_phone(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    v = (m.text or "").strip()
    if len(re.sub(r"\D", "", v)) < 10:
        await m.answer("Неверный формат телефона.", reply_markup=client_card_cancel_kb())
        return
    await state.update_data(contact_phone=v)
    await state.set_state(ClientCardStates.waiting_contact_position)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=pos, callback_data=f"cc:pos:{pos}")] for pos in DEFAULT_POSITIONS
    ] + [[InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="cc:pos:custom")]])
    await m.answer("Выберите должность контакта или введите вручную.", reply_markup=kb)

@router.callback_query(F.data.startswith("cc:pos:"), ClientCardStates.waiting_contact_position)
async def cc_contact_position_pick(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "client_cards.manage", state=state):
        return
    pos = cq.data.split(":", 2)[2]
    if pos == "custom":
        await cq.message.answer("Введите должность вручную.")
        await cq.answer()
        return
    await state.update_data(contact_position=pos)
    await state.set_state(ClientCardStates.waiting_more_contacts)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить ещё", callback_data="cc:more:yes"), InlineKeyboardButton(text="✅ Продолжить", callback_data="cc:more:no")]])
    await cq.message.answer("Добавить ещё контакт?", reply_markup=kb)
    await cq.answer()

@router.message(ClientCardStates.waiting_contact_position)
async def cc_contact_position_text(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    pos = (m.text or "").strip()
    if len(pos) < 2:
        await m.answer("Введите должность.", reply_markup=client_card_cancel_kb())
        return
    await state.update_data(contact_position=pos)
    await state.set_state(ClientCardStates.waiting_more_contacts)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить ещё", callback_data="cc:more:yes"), InlineKeyboardButton(text="✅ Продолжить", callback_data="cc:more:no")]])
    await m.answer("Добавить ещё контакт?", reply_markup=kb)

@router.callback_query(F.data.startswith("cc:more:"), ClientCardStates.waiting_more_contacts)
async def cc_more_contacts(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "client_cards.manage", state=state):
        return
    answer = cq.data.split(":", 2)[2]
    data = await state.get_data()
    contacts = data.get("client_contacts") or []
    base_contact = {
        "contact_name": data.get("contact_name"),
        "contact_phone": data.get("contact_phone"),
        "contact_position": data.get("contact_position") or "Контакт",
    }
    if base_contact["contact_name"] and (not contacts or contacts[-1] != base_contact):
        contacts.append(base_contact)
    await state.update_data(client_contacts=contacts)
    if answer == "yes":
        await state.set_state(ClientCardStates.waiting_additional_contact_name)
        await cq.message.answer("Введите имя дополнительного контакта.")
    else:
        await state.set_state(ClientCardStates.waiting_technician_select)
        await cq.message.answer("Выберите техника для карточки.", reply_markup=client_card_technician_pick_kb())
    await cq.answer()

@router.message(ClientCardStates.waiting_additional_contact_name)
async def cc_add_contact_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    await state.update_data(add_contact_name=(m.text or "").strip())
    await state.set_state(ClientCardStates.waiting_additional_contact_phone)
    await m.answer("Телефон доп. контакта:")

@router.message(ClientCardStates.waiting_additional_contact_phone)
async def cc_add_contact_phone(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    await state.update_data(add_contact_phone=(m.text or "").strip())
    await state.set_state(ClientCardStates.waiting_additional_contact_position)
    await m.answer("Должность доп. контакта:")

@router.message(ClientCardStates.waiting_additional_contact_position)
async def cc_add_contact_position(m: Message, state: FSMContext):
    role = await ensure_message_access(m, "client_cards.manage", state=state)
    if not role:
        return
    data = await state.get_data()
    edit_client_id = data.get("edit_client_id")
    c_name = data.get("add_contact_name") or "Контакт"
    c_phone = data.get("add_contact_phone") or ""
    c_pos = (m.text or "").strip() or "Контакт"
    if edit_client_id:
        CLIENTS_DB.add_contact(edit_client_id, c_name, c_phone, c_pos)
        await state.clear()
        card = CLIENTS_DB.get_client(edit_client_id)
        await m.answer("✅ Контакт добавлен.")
        await m.answer(
            _format_client_card_for_user(card, user_id=int(getattr(m.from_user, "id", 0) or 0), role=role),
            reply_markup=client_card_actions_kb(edit_client_id, role),
        )
        return
    contacts = data.get("client_contacts") or []
    contacts.append({
        "contact_name": c_name,
        "contact_phone": c_phone,
        "contact_position": c_pos,
    })
    await state.update_data(client_contacts=contacts, add_contact_name=None, add_contact_phone=None)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить ещё", callback_data="cc:more:yes"), InlineKeyboardButton(text="✅ Продолжить", callback_data="cc:more:no")]])
    await state.set_state(ClientCardStates.waiting_more_contacts)
    await m.answer("Контакт добавлен. Добавить ещё?", reply_markup=kb)

@router.callback_query(F.data.startswith("cc:tech:sel:"), ClientCardStates.waiting_technician_select)
async def cc_technician_pick(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "client_cards.manage", state=state):
        return
    technician_id = cq.data.split(":", 3)[3]
    tech = CLIENTS_DB.get_technician(technician_id)
    if not tech:
        await cq.answer("Техник не найден", show_alert=True)
        return
    await state.update_data(
        technician_id=technician_id,
        technician_name=tech.get("full_name"),
        technician_phone=tech.get("phone"),
    )
    await state.set_state(ClientCardStates.waiting_sales_rep)
    await cq.message.answer("Укажите торгового представителя (имя или 'Имя (123456)').", reply_markup=client_card_skip_cancel_kb())
    await cq.answer()

@router.callback_query(F.data == "cc:tech:skip", ClientCardStates.waiting_technician_select)
async def cc_technician_skip(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "client_cards.manage", state=state):
        return
    await state.update_data(technician_id=None, technician_name="ТЕСТ", technician_phone="+79999999999")
    await state.set_state(ClientCardStates.waiting_sales_rep)
    await cq.message.answer("Укажите торгового представителя (имя или 'Имя (123456)').", reply_markup=client_card_skip_cancel_kb())
    await cq.answer()

@router.message(ClientCardStates.waiting_sales_rep)
async def cc_sales_rep(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "client_cards.manage", state=state):
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    if _cc_is_skip(m.text):
        uid, name = None, ""
    else:
        uid, name = _parse_sales_rep_input(m.text or "")
    await state.update_data(sales_rep_user_id=uid, sales_rep_name=name)
    await state.set_state(ClientCardStates.waiting_network_name)
    await m.answer("Введите название сети для связки юрлиц (или '-' если без сети).",
                   reply_markup=client_card_skip_cancel_kb())

@router.message(ClientCardStates.waiting_network_name)
async def cc_finish_create(m: Message, state: FSMContext):
    role = await ensure_message_access(m, "client_cards.manage", state=state)
    if not role:
        return
    if _cc_is_cancel(m.text):
        await state.clear()
        await m.answer("Создание карточки отменено.", reply_markup=ReplyKeyboardRemove())
        return
    network_raw = (m.text or "").strip()
    data = await state.get_data()
    network_id = None
    if network_raw and network_raw != "-":
        network_id = CLIENTS_DB.ensure_network(network_raw)
    edit_client_id = data.get("edit_client_id")
    if edit_client_id:
        CLIENTS_DB.update_client(edit_client_id, {"network_id": network_id})
        await state.clear()
        card = CLIENTS_DB.get_client(edit_client_id)
        await m.answer("✅ Сеть клиента обновлена.")
        await m.answer(
            _format_client_card_for_user(card, user_id=int(getattr(m.from_user, "id", 0) or 0), role=role),
            reply_markup=client_card_actions_kb(edit_client_id, role),
        )
        return

    payload = {
        "legal_form": data.get("legal_form"),
        "legal_name": data.get("legal_name"),
        "store_name": data.get("store_name"),
        "address": data.get("address"),
        "overdue_days": int(data.get("overdue_days") or 0),
        "technician_name": data.get("technician_name") or "ТЕСТ",
        "technician_phone": data.get("technician_phone") or "+79999999999",
        "technician_id": data.get("technician_id"),
        "sales_rep_user_id": data.get("sales_rep_user_id"),
        "sales_rep_name": data.get("sales_rep_name") or "",
        "owner_user_id": getattr(m.from_user, "id", None),
        "network_id": network_id,
    }
    contacts = data.get("client_contacts") or []
    if not contacts:
        contacts = [{
            "contact_name": data.get("contact_name") or "Контакт",
            "contact_phone": data.get("contact_phone") or "",
            "contact_position": data.get("contact_position") or "Контакт",
        }]
    cid = CLIENTS_DB.create_client(payload, contacts)
    await state.clear()
    card = CLIENTS_DB.get_client(cid)
    await m.answer("✅ Карточка клиента создана.", reply_markup=ReplyKeyboardRemove())
    await m.answer(
        _format_client_card_for_user(card, user_id=int(getattr(m.from_user, "id", 0) or 0), role=role),
        reply_markup=client_card_actions_kb(cid, role),
    )

@router.callback_query(F.data == "cc:import:debt")
async def cc_import_debt(cq: CallbackQuery):
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage")
    if not role:
        return
    try:
        created, skipped = import_clients_from_latest_debt(uid, role)
    except Exception as e:
        await cq.message.answer(f"Не удалось выполнить импорт: {e}")
        await cq.answer()
        return
    await cq.message.answer(f"✅ Импорт завершён. Добавлено: {created}, пропущено: {skipped}.")
    items = _client_cards_for_user(uid, role)
    await cq.message.answer("Карточки клиентов:", reply_markup=client_cards_list_kb(items, role, page=0))
    await cq.answer()


@router.callback_query(F.data.startswith("cc:edit:"))
async def cc_edit_start(cq: CallbackQuery, state: FSMContext):
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Форма (ООО/ИП)", callback_data=f"cc:editfield:{client_id}:legal_form")],
        [InlineKeyboardButton(text="Юр. название", callback_data=f"cc:editfield:{client_id}:legal_name")],
        [InlineKeyboardButton(text="Название магазина", callback_data=f"cc:editfield:{client_id}:store_name")],
        [InlineKeyboardButton(text="Адрес", callback_data=f"cc:editfield:{client_id}:address")],
        [InlineKeyboardButton(text="Отсрочка (дни)", callback_data=f"cc:editfield:{client_id}:overdue_days")],
        [InlineKeyboardButton(text="Техник", callback_data=f"cc:edittech:{client_id}")],
        [InlineKeyboardButton(text="Торг. представитель", callback_data=f"cc:editfield:{client_id}:sales_rep_name")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"cc:view:{client_id}")],
    ])
    await state.clear()
    await cq.message.answer("Выберите поле для редактирования:", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("cc:edittech:"))
async def cc_edit_technician_start(cq: CallbackQuery, state: FSMContext):
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return

    card = CLIENTS_DB.get_client(client_id)
    if not card:
        await cq.answer("Карточка не найдена", show_alert=True)
        return
    addresses = [x for x in _split_addresses(card.get("address") or "") if x]
    if not addresses:
        await cq.answer("В карточке не указан адрес", show_alert=True)
        return
    technicians = CLIENTS_DB.list_technicians()
    if not technicians:
        await cq.answer("Список техников пуст", show_alert=True)
        return
    if len(addresses) == 1:
        await cq.message.answer(
            "Выберите нового техника:",
            reply_markup=client_card_edit_technician_pick_kb(client_id, "0", technicians),
        )
    else:
        await cq.message.answer(
            "Выберите адрес, для которого нужно изменить техника:",
            reply_markup=client_card_edit_technician_address_kb(client_id, addresses),
        )
    await cq.answer()


@router.callback_query(F.data.startswith("cc:edittechaddr:"))
async def cc_edit_technician_address_pick(cq: CallbackQuery):
    _, _, client_id, address_idx = cq.data.split(":", 3)
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage")
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    card = CLIENTS_DB.get_client(client_id)
    if not card:
        await cq.answer("Карточка не найдена", show_alert=True)
        return
    addresses = [x for x in _split_addresses(card.get("address") or "") if x]
    idx = int(address_idx)
    if idx < 0 or idx >= len(addresses):
        await cq.answer("Адрес не найден", show_alert=True)
        return
    technicians = CLIENTS_DB.list_technicians()
    if not technicians:
        await cq.answer("Список техников пуст", show_alert=True)
        return
    await cq.message.answer(
        f"Выберите нового техника для адреса:\n{addresses[idx]}",
        reply_markup=client_card_edit_technician_pick_kb(client_id, address_idx, technicians),
    )
    await cq.answer()


@router.callback_query(F.data.startswith("cc:edittechsel:"))
async def cc_edit_technician_pick(cq: CallbackQuery, state: FSMContext):
    _, _, client_id, address_idx, technician_idx_raw = cq.data.split(":", 4)
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return

    technicians = CLIENTS_DB.list_technicians()
    try:
        technician_idx = int(technician_idx_raw)
    except ValueError:
        await cq.answer("Некорректный техник", show_alert=True)
        return
    if technician_idx < 0 or technician_idx >= len(technicians):
        await cq.answer("Техник не найден", show_alert=True)
        return
    tech = technicians[technician_idx]
    technician_id = tech.get("id")
    if not technician_id:
        await cq.answer("Техник не найден", show_alert=True)
        return

    card_before = CLIENTS_DB.get_client(client_id)
    if not card_before:
        await cq.answer("Карточка не найдена", show_alert=True)
        return
    addresses = [x for x in _split_addresses(card_before.get("address") or "") if x]
    idx = int(address_idx)
    if idx < 0 or idx >= len(addresses):
        await cq.answer("Адрес не найден", show_alert=True)
        return
    CLIENTS_DB.set_client_address_technician(client_id, addresses[idx], technician_id)
    if len(addresses) == 1:
        CLIENTS_DB.update_client(client_id, {
            "technician_id": technician_id,
            "technician_name": tech.get("full_name") or "",
            "technician_phone": tech.get("phone") or "",
        })
    await state.clear()
    card = CLIENTS_DB.get_client(client_id)
    await cq.message.answer("✅ Техник обновлён.")
    await cq.message.answer(
        _format_client_card_for_user(card, user_id=uid, role=role),
        reply_markup=client_card_actions_kb(client_id, role),
    )
    await cq.answer()


@router.callback_query(F.data.startswith("cc:edittechskip:"))
async def cc_edit_technician_skip(cq: CallbackQuery):
    _, _, client_id, address_idx = cq.data.split(":", 3)
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage")
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return

    card_before = CLIENTS_DB.get_client(client_id)
    if not card_before:
        await cq.answer("Карточка не найдена", show_alert=True)
        return
    addresses = [x for x in _split_addresses(card_before.get("address") or "") if x]
    idx = int(address_idx)
    if idx < 0 or idx >= len(addresses):
        await cq.answer("Адрес не найден", show_alert=True)
        return
    CLIENTS_DB.set_client_address_technician(client_id, addresses[idx], None)
    if len(addresses) == 1:
        CLIENTS_DB.update_client(client_id, {
            "technician_id": None,
            "technician_name": "ТЕСТ",
            "technician_phone": "+79999999999",
        })
    card = CLIENTS_DB.get_client(client_id)
    await cq.message.answer("✅ Техник обновлён.")
    await cq.message.answer(
        _format_client_card_for_user(card, user_id=uid, role=role),
        reply_markup=client_card_actions_kb(client_id, role),
    )
    await cq.answer()


@router.callback_query(F.data.startswith("cc:editfield:"))
async def cc_edit_field_pick(cq: CallbackQuery, state: FSMContext):
    _, _, client_id, field = cq.data.split(":", 3)
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    prompts = {
        "legal_form": "Введите форму: ООО или ИП",
        "legal_name": "Введите новое юр. название:",
        "store_name": "Введите новое название магазина:",
        "address": "Введите новый адрес:",
        "overdue_days": "Введите число дней отсрочки:",
        "sales_rep_name": "Введите ФИО торгового представителя:",
    }
    if field not in prompts:
        await cq.answer("Поле недоступно", show_alert=True)
        return
    await state.clear()
    await state.update_data(edit_client_id=client_id, edit_field=field)
    await state.set_state(ClientCardStates.waiting_edit_value)
    await cq.message.answer(prompts[field])
    await cq.answer()


@router.message(ClientCardStates.waiting_edit_value)
async def cc_edit_field_value(m: Message, state: FSMContext):
    role = await ensure_message_access(m, "client_cards.manage", state=state)
    if not role:
        return
    data = await state.get_data()
    client_id = data.get("edit_client_id")
    field = data.get("edit_field")
    if not client_id or not field:
        await state.clear()
        await m.answer("Сессия редактирования потеряна.")
        return
    raw = (m.text or "").strip()
    patch: Dict[str, Any] = {}
    if field == "legal_form":
        val = raw.upper()
        if val not in {"ООО", "ИП"}:
            await m.answer("Допустимо только ООО или ИП.")
            return
        patch[field] = val
    elif field == "overdue_days":
        try:
            patch[field] = max(0, int(raw))
        except Exception:
            await m.answer("Введите целое число.")
            return
    else:
        patch[field] = raw

    CLIENTS_DB.update_client(client_id, patch)
    await state.clear()
    card = CLIENTS_DB.get_client(client_id)
    await m.answer("✅ Карточка обновлена.")
    await m.answer(
        _format_client_card_for_user(card, user_id=int(getattr(m.from_user, "id", 0) or 0), role=role),
        reply_markup=client_card_actions_kb(client_id, role),
    )


@router.callback_query(F.data.startswith("cc:del:"))
async def cc_delete_client(cq: CallbackQuery):
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage")
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    CLIENTS_DB.delete_client(client_id)
    await cq.message.answer("✅ Карточка клиента удалена.")
    items = _client_cards_for_user(uid, role)
    await cq.message.answer("Карточки клиентов:", reply_markup=client_cards_list_kb(items, role, page=0))
    await cq.answer()

@router.callback_query(F.data.startswith("cc:addcontact:"))
async def cc_add_contact_start(cq: CallbackQuery, state: FSMContext):
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    await state.clear()
    await state.update_data(edit_client_id=client_id)
    await state.set_state(ClientCardStates.waiting_additional_contact_name)
    await cq.message.answer("Имя нового контакта:")
    await cq.answer()

@router.callback_query(F.data.startswith("cc:net:"))
async def cc_set_network_start(cq: CallbackQuery, state: FSMContext):
    client_id = cq.data.split(":", 2)[2]
    uid = int(getattr(cq.from_user, "id", 0) or 0)
    role = await ensure_callback_access(cq, "client_cards.manage", state=state)
    if not role:
        return
    if not _has_client_card_access(uid, role, client_id):
        await deny_callback_access(cq, "client_cards.manage")
        return
    await state.clear()
    await state.update_data(edit_client_id=client_id)
    await state.set_state(ClientCardStates.waiting_network_name)
    await cq.message.answer("Введите название сети для этой карточки.")
    await cq.answer()

@router.message(F.text == "⚙️ Отсрочки")
async def btn_overdue_menu(m: Message):
    if _is_client_only(m):
        await m.answer("Доступно только для админов или торговых.", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return
    await m.answer("Меню отсрочек:", reply_markup=overdue_menu_kb())

@router.message(F.text.in_({"⚙️ Фильтры", "⚙️ Фильтры отображения"}))
async def filters_entry(m: Message, state: FSMContext):
    if _is_client_only(m):
        await m.answer("Доступно только для админов или торговых.", reply_markup=menu_for_message(m))
        return
    logger.info("filters: entry by %s (%s)", m.from_user.id, m.from_user.username)
    await state.clear()
    idx = 0
    await m.answer(_filters_page_text(idx), reply_markup=_filters_page_kb(idx), disable_web_page_preview=True)

@router.message(F.text == "📦 Проверить ТТН")
async def btn_ttn(m: Message, state: FSMContext):
    await state.set_state(TTNStates.waiting_number)
    await m.answer(
        "Введите номер(а) ТТН.\n"
        "Можно несколько через пробел или с новой строки.",
        reply_markup=back_only_kb()
    )

#----------------------------------------
#---------------TTN (FSRAR + captcha) капча----

TTN_BTN  = "📦 Проверить ТТН"

class TTNStates(StatesGroup):
    waiting_number = State()
    waiting_captcha = State()

# вытаскиваем номер ТТН, допускаем префикс TTN/ТТН и разделители
_TTN_INPUT_RE = re.compile(
    r"(?:^|[\s,;:])(?:ttn|ттн)?[-\s]*([0-9]{8,20})(?=$|[\s,;:])",
    re.I
)

TTN_OK = 0
TTN_ERR_BAD_FORMAT = 1
TTN_ERR_NOT_FOUND = 2
TTN_ERR_PROVIDER_UNAVAILABLE = 3
TTN_ERR_INTERNAL = 9

@dataclass
class TTNResult:
    number: str
    code: int
    status: str
    title: str
    last_event: Optional[str]
    last_time: Optional[str]
    carrier: Optional[str]
    extra: Optional[str] = None

def extract_ttns(raw: str) -> list:
    """Достаёт список номеров ТТН (только цифры), из строки(строк)."""
    raw = (raw or "")
    nums = [m.group(1) for m in _TTN_INPUT_RE.finditer(raw)]
    if nums:
        return nums
    # запасной вариант — любые 8+ подряд идущих цифр
    return re.findall(r"[0-9]{8,}", raw)

def normalize_ttn(num: str) -> str:
    """Оставляем только цифры. Валидно, если длина 8..20."""
    digits = re.sub(r"\D+", "", num or "")
    return digits if 8 <= len(digits) <= 20 else ""

def _ttn_bad_format(num: str) -> TTNResult:
    return TTNResult(num, TTN_ERR_BAD_FORMAT, "bad_format", "Неверный формат", None, None, None)

def _ttn_not_found(num: str) -> TTNResult:
    return TTNResult(num, TTN_ERR_NOT_FOUND, "not_found", "Не найдено", None, None, None)

def _ttn_provider_unavail(num: str, msg: str) -> TTNResult:
    return TTNResult(num, TTN_ERR_PROVIDER_UNAVAILABLE, "unavailable", "Провайдер недоступен", None, None, None, msg)

def _ttn_ok(num: str, status: str, title: str,
            last_event: Optional[str], last_time: Optional[str], carrier: Optional[str]) -> TTNResult:
    return TTNResult(num, TTN_OK, status, title, last_event, last_time, carrier)

def _valid_ttn(num: str) -> bool:
    return bool(normalize_ttn(num))

# ---------- парсинг формы и капчи ----------
_FORM_RE = re.compile(r"<form[^>]*?(action=['\"]?([^'\"> ]+)['\"]?)?[^>]*>", re.I)
_CAPTCHA_IMG = re.compile(r"<img[^>]+src=['\"]([^'\"]*BotDetectCaptcha[^'\"]*)['\"][^>]*>", re.I)
_BOTDETECT_INIT = re.compile(r"BotDetect\.Init\('([^']+)','([0-9a-fA-F]+)'.*?'CaptchaCode'", re.I)
_HIDDEN_INSTANCE = re.compile(r"name=['\"]LBD_VCID_[^'\"]+['\"][^>]*value=['\"]([0-9a-fA-F]+)['\"]", re.I)

def _parse_botdetect(html: str) -> Tuple[str, str]:
    """Возвращает (CaptchaId, InstanceId). Обычно ('SampleCaptcha', '<hex>')."""
    m = _BOTDETECT_INIT.search(html or "")
    if m:
        return m.group(1), m.group(2)
    ih = _HIDDEN_INSTANCE.search(html or "")
    if ih:
        return "SampleCaptcha", ih.group(1)
    return "SampleCaptcha", ""

def _parse_form_action(html: str) -> str:
    m = _FORM_RE.search(html or "")
    if not m:
        return "/"
    return m.group(2) or "/"

def _find_captcha_src(html: str) -> Optional[str]:
    m = _CAPTCHA_IMG.search(html or "")
    return _html.unescape(m.group(1)) if m else None

# ---------- хранение потоков ----------
@dataclass
class _CaptchaFlow:
    sess: aiohttp.ClientSession
    base: str
    fr_id: str
    ttn: str
    captcha_id: str
    instance_id: str
    created_ts: float

_TTN_FLOWS: Dict[int, _CaptchaFlow] = {}
_TTN_FLOW_TTL = 180.0

def _cleanup_flows():
    now = time.time()
    for uid, f in list(_TTN_FLOWS.items()):
        if now - f.created_ts > _TTN_FLOW_TTL or f.sess.closed:
            try:
                asyncio.create_task(f.sess.close())
            except Exception:
                pass
            _TTN_FLOWS.pop(uid, None)

# ---------- aiohttp session (SSL: certifi -> fallback ssl=False) ----------
def _build_http_session(skip_verify: bool = False) -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(total=20)
    env_skip = str(os.getenv("FSRAR_SKIP_SSL_VERIFY", "0")).strip().lower() in ("1","true","yes")
    skip = skip_verify or env_skip
    if skip:
        logger.warning("ttn: SSL verification is DISABLED for this session (skip=%s)", skip)
        return aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=False))
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
        return aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=ctx))
    except Exception as e:
        logger.warning("ttn: certifi not available (%s), using default SSL context", e)
        return aiohttp.ClientSession(timeout=timeout)


def _captcha_preview_bytes(img_bytes: bytes,
                          canvas_w: int = 800,
                          canvas_h: int = 450,
                          pad_px: int = 24) -> bytes:
    """
    Делает превью-изображение для Telegram:
    - если капча очень широкая -> квадратный холст 800x800 (исключает боковой кроп),
    - иначе портрет 720x960 (3:4).
    """
    try:
        from PIL import Image
    except Exception:
        return img_bytes

    import io
    try:
        with Image.open(io.BytesIO(img_bytes)) as im:
            im = im.convert("RGB")

            max_w = max(1, canvas_w - 2 * pad_px)
            max_h = max(1, canvas_h - 2 * pad_px)

            scale = min(max_w / im.width, max_h / im.height)
            new_w = max(1, int(im.width * scale))
            new_h = max(1, int(im.height * scale))
            if (new_w, new_h) != im.size:
                im = im.resize((new_w, new_h), Image.LANCZOS)

            canvas = Image.new("RGB", (canvas_w, canvas_h), (255, 255, 255))
            x = (canvas_w - im.width) // 2
            y = (canvas_h - im.height) // 2
            canvas.paste(im, (x, y))

            buf = io.BytesIO()
            canvas.save(buf, format="PNG")
            return buf.getvalue()
    except Exception:
        return img_bytes


# ---------- шаг 1: получить страницу + картинку капчи ----------
async def _fsrar_get_captcha(fr_id: str, base: str, ttn: str) -> Tuple[bytes, _CaptchaFlow]:
    base = base.rstrip("/")
    url  = f"{base}/"
    params = {"fsrar": fr_id, "ttn": ttn}

    try:
        sess = _build_http_session()
        async with sess.get(url, params=params) as resp:
            html = await resp.text()
    except Exception:
        logger.exception("ttn: first GET failed, fallback to ssl=False")
        try:
            await sess.close()
        except Exception:
            pass
        sess = _build_http_session(skip_verify=True)
        async with sess.get(url, params=params) as resp:
            html = await resp.text()

    cap_src = _find_captcha_src(html)
    if not cap_src:
        try:
            Path("settings").mkdir(parents=True, exist_ok=True)
            Path("settings/ttn_last_ajax.html").write_text(html, encoding="utf-8")
        except Exception:
            pass
        await sess.close()
        raise RuntimeError("captcha_src_not_found")

    captcha_id, instance_id = _parse_botdetect(html)
    cap_url = urljoin(base + "/", cap_src)
    logger.info("ttn: captcha src -> %s", cap_url)

    try:
        async with sess.get(cap_url) as rimg:
            if rimg.status != 200:
                await sess.close()
                raise RuntimeError(f"captcha_http_{rimg.status}")
            img_bytes = await rimg.read()
    except Exception:
        logger.exception("ttn: captcha download failed")
        await sess.close()
        raise

    flow = _CaptchaFlow(
        sess=sess, base=base, fr_id=fr_id, ttn=ttn,
        captcha_id=captcha_id, instance_id=instance_id, created_ts=time.time()
    )
    logger.info("ttn: flow prepared (captcha_id=%s, instance=%s)", captcha_id, instance_id)
    return img_bytes, flow

# ---------- шаг 2: реальный AJAX POST (/MobileApi/transportwb) ----------
async def _fsrar_submit_ajax(flow: _CaptchaFlow, user_input: str) -> str:
    url = urljoin(flow.base + "/", "/MobileApi/transportwb")
    data = {
        "id": flow.ttn,
        "owner_id": flow.fr_id,
        "owner_receiver": "",
        "CaptchaId": flow.captcha_id,
        "InstanceId": flow.instance_id,
        "UserInput": (user_input or "").strip().upper(),
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": flow.base,
        "Referer": flow.base + "/",
    }

    async with flow.sess.post(url, data=data, headers=headers) as resp:
        txt = await resp.text()

    # Сохраним «как есть»
    try:
        Path("settings").mkdir(parents=True, exist_ok=True)
        Path("settings/ttn_last_post.html").write_text(txt, encoding="utf-8")
    except Exception:
        pass

    # Некоторые инсталляции возвращают ИМЕННО JSON-СТРОКУ:  "<div>...<\/div>"
    # Преобразуем её в обычный HTML.
    if txt[:1] == '"' and txt[-1:] == '"':
        try:
            txt = json.loads(txt)  # превращает \u003c в < и т.п.
            return txt
        except Exception:
            return txt

    # Вариант с объектом { data: "<div>...</div>" }
    if txt.startswith("{") or txt.startswith("["):
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                for k in ("data", "text", "html", "content"):
                    if isinstance(obj.get(k), str):
                        return obj[k]
        except Exception:
            pass

    return txt

_TAG_RE = re.compile(r"<[^>]+>")
_BR_RE  = re.compile(r"<\s*br\s*/?\s*>", re.I)

def _clean_html_text(s: str) -> str:
    if not s:
        return ""
    s = _BR_RE.sub("\n", s)
    s = _TAG_RE.sub("", s)
    s = _html.unescape(s)
    s = s.replace("\xa0", " ").strip()
    return s

# Блоки на странице
_RX_BLOCK_NAKL  = re.compile(r"Накладная:\s*</h2>\s*(.*?)\s*(?:<h2>|<h1>|\Z)", re.I | re.S)
_RX_BLOCK_SEND  = re.compile(r"отправка\s+получателю\s*:\s*</h2>\s*<div[^>]*class=['\"][^'\"]*infocontainer[^'\"]*['\"][^>]*>(.*?)</div>", re.I | re.S)
_RX_BLOCK_DOCS  = re.compile(r"Связанные документы\s*:\s*</h2>\s*(.*?)\Z", re.I | re.S)

# Пары "ключ: значение" внутри инфоблоков
_RX_SENDER   = re.compile(r"Отправитель:\s*(.*?)\s*Ид:\s*([0-9]+)", re.I | re.S)
_RX_RECEIVER = re.compile(r"Получатель:\s*(.*?)\s*Ид:\s*([0-9]+)", re.I | re.S)
_RX_NUMDATE  = re.compile(r"Номер:\s*([^<\n]+)\s*Дата:\s*([^<\n]+)", re.I)
_RX_STATUS   = re.compile(r"Статус:\s*([^<\n]+)", re.I)
_RX_INS      = re.compile(r"Дата вставки:\s*([^<\n]+)", re.I)
_RX_CHG      = re.compile(r"Дата смены статуса:\s*([^<\n]+)", re.I)

# Заголовок про акт подтверждения
_RX_ACT_TITLE = re.compile(r"Получателем\s*\((.*?)\)\s*составлен\s*Акт\s*подтверждения\s*с\s*номером\s*([0-9]+)\s*от\s*([0-9.: ]+)", re.I)

def _parse_block_nakl(html: str) -> dict:
    """1-й блок: Накладная."""
    m = _RX_BLOCK_NAKL.search(html or "")
    block = m.group(1) if m else ""
    # Нам удобнее чистый текст одной «карточки»
    txt = _clean_html_text(block)

    sender = _RX_SENDER.search(txt)
    recv   = _RX_RECEIVER.search(txt)
    nd     = _RX_NUMDATE.search(txt)
    st     = _RX_STATUS.search(txt)

    return {
        "sender_name": sender.group(1).strip() if sender else None,
        "sender_id":   sender.group(2).strip() if sender else None,
        "recv_name":   recv.group(1).strip()   if recv else None,
        "recv_id":     recv.group(2).strip()   if recv else None,
        "doc_num":     nd.group(1).strip()     if nd else None,
        "doc_date":    nd.group(2).strip()     if nd else None,
        "status":      st.group(1).strip()     if st else None,
    }

def _parse_block_send(html: str) -> dict:
    """2-й блок: отправка получателю."""
    m = _RX_BLOCK_SEND.search(html or "")
    txt = _clean_html_text(m.group(1) if m else "")
    st  = _RX_STATUS.search(txt)
    ins = _RX_INS.search(txt)
    chg = _RX_CHG.search(txt)
    return {
        "status":   st.group(1).strip()  if st  else None,
        "inserted": ins.group(1).strip() if ins else None,
        "changed":  chg.group(1).strip() if chg else None,
    }

def _parse_block_act(html: str) -> dict:
    """3-й блок: текст про акт + его отправка."""
    # Вырезаем всё после «Связанные документы»
    m = _RX_BLOCK_DOCS.search(html or "")
    part = m.group(1) if m else ""
    # Заголовок про акт (h2)
    act_title = ""
    m2 = _RX_ACT_TITLE.search(_clean_html_text(part))
    if m2:
        act_title = f"Получателем ({m2.group(1).strip()}) составлен Акт подтверждения с номером {m2.group(2).strip()} от {m2.group(3).strip()}"

    # А следом обычно идёт «Отправка акта получателю …» + infocontainer
    m3 = re.search(r"Отправка\s+акта\s+получателю.*?<div[^>]*class=['\"][^'\"]*infocontainer[^'\"]*['\"][^>]*>(.*?)</div>",
                   part, re.I | re.S)
    txt = _clean_html_text(m3.group(1)) if m3 else ""
    st  = _RX_STATUS.search(txt)
    ins = _RX_INS.search(txt)
    chg = _RX_CHG.search(txt)

    return {
        "title":    act_title or None,
        "status":   (st.group(1).strip() if st else None),
        "inserted": (ins.group(1).strip() if ins else None),
        "changed":  (chg.group(1).strip() if chg else None),
    }

def parse_fsrar_details(html: str) -> dict:
    """Возвращает словарь с тремя блоками."""
    return {
        "nakl": _parse_block_nakl(html),
        "send": _parse_block_send(html),
        "act":  _parse_block_act(html),
    }

def render_ttn_pretty(ttn: str, status_title: str, details: dict) -> str:
    """
    Финальный красивый текст из трёх блоков.
    """
    d1 = details.get("nakl", {})
    d2 = details.get("send", {})
    d3 = details.get("act", {})

    # эмодзи по главному статусу
    key, emoji = _map_title_to_status(status_title)
    head = [ "<b>Проверка ТТН</b>",
             f"{emoji} <b>{esc(ttn)}</b>" ]

    block1 = []
    if any(d1.values()):
        block1.append(
            "\n".join([
                f"Отправитель: {esc(d1.get('sender_name') or '—')} Ид: {esc(d1.get('sender_id') or '—')}",
                f"Получатель: {esc(d1.get('recv_name') or '—')} Ид: {esc(d1.get('recv_id') or '—')}",
                f"Номер: {esc(d1.get('doc_num') or '—')} Дата: {esc(d1.get('doc_date') or '—')}",
                f"Статус: {esc(d1.get('status') or status_title or '—')}",
            ])
        )

    block2 = []
    if any(d2.values()):
        block2.append(
            "\n".join([
                "отправка получателю:",
                f"Статус: {esc(d2.get('status') or '—')}",
                f"Дата вставки: {esc(d2.get('inserted') or '—')}",
                f"Дата смены статуса: {esc(d2.get('changed') or '—')}",
            ])
        )

    block3 = []
    if any(d3.values()):
        if d3.get("title"):
            block3.append(esc(d3["title"]))
        block3.extend([
            f"Статус: {esc(d3.get('status') or '—')}",
            f"Дата вставки: {esc(d3.get('inserted') or '—')}",
            f"Дата смены статуса: {esc(d3.get('changed') or '—')}",
        ])

    parts = ["\n".join(head)]
    if block1: parts.append("\n".join(block1))
    if block2: parts.append("\n".join(block2))
    if block3: parts.append("\n".join(block3))
    parts.append("Источник: fsrar")
    return "\n\n".join(parts)

# ---------- классификация статуса ----------
_NAKLADNAYA_BLOCK = re.compile(r"Накладная:\s*</h2>(.*?)(?:<h2>|\Z)", re.I | re.S)
_STATUS_LINE      = re.compile(r"Статус:\s*([^<\r\n]+)", re.I)
# явные признаки плохой капчи и "не найдено"
_BAD_CAPTCHA_RX = re.compile(
    r"(код\s*с\s*картинк[иы].*неверн|введите\s+код\s+с\s+картинки|captcha.*invalid)",
    re.I
)
_NOT_FOUND_RX = re.compile(r"\bне\s*найден[ао]\b", re.I)

def _extract_block(html: str) -> str:
    if not html:
        return ""
    text = _html.unescape(html)
    m = _NAKLADNAYA_BLOCK.search(text)
    return m.group(1) if m else text

def _map_title_to_status(title: str) -> Tuple[str, str]:
    """
    Возвращает (status_key, emoji)
    status_key ∈ delivered|rejected|revoked|in_progress|disagreement_rejected|zero_disagreement|new_version|repealed|unknown
    """
    low = (title or "").strip().lower()
    # точные статусы
    if "принята" in low:
        return "delivered", "✅"
    if "отклонена" in low:
        return "rejected", "⛔"
    if "отозвана" in low:
        return "revoked", "⛔"
    if "проведена" in low:
        return "in_progress", "📦"
    if "отказан акт разногласий" in low:
        return "disagreement_rejected", "⚠️"
    if "нулевой акт расхождений" in low:
        return "zero_disagreement", "ℹ️"
    if "новая версия" in low:
        return "new_version", "ℹ️"
    if "распроведена по запросу repeal" in low or "распроведена" in low:
        return "repealed", "⚠️"
    return "unknown", "ℹ️"

def _classify_response(html_text: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    """
    Возвращает (status_key, human_title, last_event, last_time).
    Сейчас вытаскиваем только главный статус из блока 'Накладная'.
    """
    text = html_text or ""
    block = _extract_block(text)
    m = _STATUS_LINE.search(block)
    if not m:
        return "unknown", "Статус не распознан", None, None
    title = m.group(1).strip()
    key, _ = _map_title_to_status(title)
    return key, title, None, None

# ---------- клавиатура капчи ----------
def _ttn_captcha_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить капчу", callback_data="ttn:cap:refresh")]
    ])

# ← алиас для старых вызовов без подчёркивания
ttn_captcha_kb = _ttn_captcha_kb

# ---------- публичные хэндлеры ----------
@router.message(F.text == TTN_BTN)
async def btn_ttn(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "ttn.lookup", state=state):
        return
    await state.set_state(TTNStates.waiting_number)
    await m.answer("Введите номер(а) ТТН.\nМожно несколько через пробел или с новой строки.", reply_markup=back_only_kb())

@router.message(TTNStates.waiting_number, F.text)
async def ttn_step_number(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "ttn.lookup", state=state):
        return
    raw = (m.text or "").strip()
    ttns = extract_ttns(raw)
    if not ttns:
        await m.answer("Не нашёл номера. Пришлите ещё раз. Для отмены нажмите /start")
        return

    fr_id = (os.getenv("FRARAR_ID") or "").strip()
    base  = (os.getenv("FRARAR_BASE") or "https://check1.fsrar.ru").strip()
    if not fr_id:
        await m.answer("FSRAR_ID не задан. Обратитесь к администратору.")
        return

    ttn = normalize_ttn(ttns[0])  # пока первый
    if not ttn:
        await m.answer("Неверный формат номера. Пришлите ещё раз.")
        return
    await state.set_state(TTNStates.waiting_captcha)
    await state.update_data(ttn=ttn)

    try:
        img_bytes, flow = await _fsrar_get_captcha(fr_id, base, ttn)
    except Exception as e:
        logger.exception("ttn: captcha prepare failed")
        await state.clear()
        await m.answer(f"Не удалось получить капчу: {e}")
        return

    _cleanup_flows()
    _TTN_FLOWS[m.from_user.id] = flow

    preview = _captcha_preview_bytes(img_bytes)
    await m.answer_photo(
        BufferedInputFile(preview, filename="captcha.png"),
        caption=f"ТТН: <b>{esc(ttn)}</b>\nВведите код с картинки:",
        reply_markup=_ttn_captcha_kb()
    )

@router.callback_query(F.data == "ttn:cap:refresh")
async def ttn_cap_refresh(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "ttn.lookup", state=state):
        return
    _cleanup_flows()
    data = await state.get_data()
    ttn = normalize_ttn(data.get("ttn"))
    fr_id = (os.getenv("FRARAR_ID") or "").strip()
    base  = (os.getenv("FRARAR_BASE") or "https://check1.fsrar.ru").strip()
    if not (ttn and fr_id):
        await cq.message.answer("Сессия истекла. Нажмите «📦 Проверить ТТН» и начните заново.")
        await cq.answer()
        return

    # закрываем старую сессию
    old = _TTN_FLOWS.pop(cq.from_user.id, None)
    if old:
        try: await old.sess.close()
        except Exception: pass

    try:
        img_bytes, flow = await _fsrar_get_captcha(fr_id, base, ttn)
        _TTN_FLOWS[cq.from_user.id] = flow
        await cq.message.delete()
        preview = _captcha_preview_bytes(img_bytes)
        await cq.message.answer_photo(
            BufferedInputFile(preview, filename="captcha.png"),
            caption=f"ТТН: <b>{esc(ttn)}</b>\nВведите код с картинки:",
            reply_markup=_ttn_captcha_kb()
        )

    except Exception as e:
        logger.exception("ttn: captcha refresh failed")
        await cq.message.answer(f"Не удалось обновить капчу: {e}")
    finally:
        await cq.answer()

@router.message(TTNStates.waiting_captcha, F.text)
async def ttn_step_captcha(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "ttn.lookup", state=state):
        return
    code = (m.text or "").strip()
    flow = _TTN_FLOWS.get(m.from_user.id)
    if not flow or flow.sess.closed:
        logger.warning("ttn: flow missing/expired for user=%s", m.from_user.id)
        await state.clear()
        await m.answer("Сессия истекла. Нажмите «📦 Проверить ТТН» и начните заново.")
        return

    try:
        html = await _fsrar_submit_ajax(flow, code)
    except Exception as e:
        logger.exception("ttn: submit error")
        await state.clear()
        await m.answer(f"Ошибка запроса: {e}")
        try:
            await flow.sess.close()
        except Exception:
            pass
        _TTN_FLOWS.pop(m.from_user.id, None)
        return

    # ---------- БЫСТРЫЕ ПРОВЕРКИ ----------
    # 0) капча неверная — сразу просим новую
    if _BAD_CAPTCHA_RX.search(html or ""):
        try:
            img_bytes, new_flow = await _fsrar_get_captcha(flow.fr_id, flow.base, flow.ttn)
            _TTN_FLOWS[m.from_user.id] = new_flow
            await m.answer_photo(
                BufferedInputFile(img_bytes, filename="captcha.jpg"),
                caption="Код неверный. Попробуйте ещё раз:",
                reply_markup=_ttn_captcha_kb()
            )
            return
        except Exception:
            logger.exception("ttn: captcha refresh after explicit bad code failed")
            await m.answer("Код неверный и не удалось получить новую капчу. Начните заново.")
            await state.clear()
            try:
                await flow.sess.close()
            except Exception:
                pass
            _TTN_FLOWS.pop(m.from_user.id, None)
            return

    # 1) «не найдено» — отдаём лаконичный ответ и выходим
    if _NOT_FOUND_RX.search(_clean_html_text(html)):
        await m.answer(
            "<b>Проверка ТТН</b>\n❓ <b>{}</b>\nНе найдено.\nИсточник: fsrar".format(esc(flow.ttn)),
            disable_web_page_preview=True
        )
        await state.clear()
        try:
            await flow.sess.close()
        except Exception:
            pass
        _TTN_FLOWS.pop(m.from_user.id, None)
        return
    # ---------- /БЫСТРЫЕ ПРОВЕРКИ ----------

    key, title, last_event, last_time = _classify_response(html)
    logger.info("ttn: result key=%s title=%s", key, title)

    # запасной ловец явной подсказки сайта (дублирует _BAD_CAPTCHA_RX, но не мешает)
    if key == "unknown" and "введите код с картинки" in (html or "").lower():
        try:
            img_bytes, new_flow = await _fsrar_get_captcha(flow.fr_id, flow.base, flow.ttn)
            _TTN_FLOWS[m.from_user.id] = new_flow
            await m.answer_photo(
                BufferedInputFile(img_bytes, filename="captcha.jpg"),
                caption="Код неверный. Попробуйте ещё раз:",
                reply_markup=_ttn_captcha_kb()
            )
            return
        except Exception:
            logger.exception("ttn: captcha refresh after bad code failed")
            await m.answer("Код неверный и не удалось получить новую капчу. Начните заново.")

    # Разбираем детали трёх блоков
    details = parse_fsrar_details(html)

    # Если ничего внятного не распознали — понятная заглушка
    if key == "unknown":
        d1 = details.get("nakl", {}) or {}
        d2 = details.get("send", {}) or {}
        d3 = details.get("act",  {}) or {}
        if not any(d1.values()) and not any(d2.values()) and not any(d3.values()):
            await m.answer(
                "<b>Проверка ТТН</b>\n⚠️ <b>{}</b>\nПровайдер недоступен или ответ не распознан.\nПопробуйте ещё раз (обновите капчу).\nИсточник: fsrar".format(
                    esc(flow.ttn)
                ),
                disable_web_page_preview=True
            )
            await state.clear()
            try:
                await flow.sess.close()
            except Exception:
                pass
            _TTN_FLOWS.pop(m.from_user.id, None)
            return

    # Красивый финальный ответ
    pretty = render_ttn_pretty(flow.ttn, title, details)
    await m.answer(pretty, disable_web_page_preview=True)

    # Завершение
    await state.clear()
    try:
        await flow.sess.close()
    except Exception:
        pass
    _TTN_FLOWS.pop(m.from_user.id, None)



def _status_emoji(res: TTNResult) -> str:
    s = (res.status or "").lower()
    if res.code == TTN_OK:
        if s == "delivered": return "✅"
        if s in ("in_progress", "ready"): return "📦"
        if s in ("rejected", "revoked", "repealed", "disagreement_rejected"): return "⛔"
        if s in ("zero_disagreement", "new_version"): return "ℹ️"
        return "ℹ️"
    if res.code == TTN_ERR_NOT_FOUND: return "❓"
    if res.code in (TTN_ERR_PROVIDER_UNAVAILABLE, TTN_ERR_INTERNAL): return "⚠️"
    if res.code == TTN_ERR_BAD_FORMAT: return "⛔"
    return "⚠️"

def render_ttn_results(results: List[TTNResult]) -> str:
    out = ["<b>Проверка ТТН</b>"]
    for r in results:
        em = _status_emoji(r)
        if r.code == TTN_OK:
            lines = [f"{em} <b>{esc(r.number)}</b>", f"Статус: <b>{esc(r.title or r.status)}</b>"]
            if r.last_event: lines.append(f"Событие: {esc(r.last_event)}")
            if r.last_time:  lines.append(f"Время: {esc(r.last_time)}")
            if r.carrier:    lines.append(f"Источник: {esc(r.carrier)}")
            out.append("\n".join(lines))
        elif r.code == TTN_ERR_BAD_FORMAT:
            out.append(f"⛔ <b>{esc(r.number)}</b>\nНеверный формат номера.")
        elif r.code == TTN_ERR_NOT_FOUND:
            out.append(f"❓ <b>{esc(r.number)}</b>\nНе найдено.")
        elif r.code == TTN_ERR_PROVIDER_UNAVAILABLE:
            out.append(f"⚠️ <b>{esc(r.number)}</b>\nПровайдер недоступен. {esc(r.extra or '')}")
        elif r.code == TTN_ERR_INTERNAL:
            out.append(f"⚠️ <b>{esc(r.number)}</b>\nВнутренняя ошибка. {esc(r.extra or '')}")
        else:
            out.append(f"ℹ️ <b>{esc(r.number)}</b>\nНеизвестная ошибка.")
    return "\n\n".join(out)





## старый варик
#@router.callback_query(F.data == "ttn:refresh")
#async def ttn_refresh_captcha(cq: CallbackQuery, state: FSMContext):
#    uid = cq.from_user.id
#    old = _TTN_FLOWS.get(uid)
#    if not old or old.sess.closed:
#        await cq.answer("Сессия истекла. Начните заново.", show_alert=True)
#        return
#    try:
#        # Закрываем старую сессию
#        try:
#            await old.sess.close()
#        except Exception:
#            pass
#
#       img_bytes, new_flow = await _fsrar_get_captcha(old.fr_id, old.base, old.ttn)
#        _TTN_FLOWS[uid] = new_flow
#        await cq.answer("Капча обновлена")
#       await cq.message.answer_photo(
#           BufferedInputFile(img_bytes, filename="captcha.jpg"),
#           caption=f"ТТН: <b>{esc(new_flow.ttn)}</b>\nВведите код с картинки:",
#           reply_markup=ttn_captcha_kb()
#       )
#    except Exception as e:
#        logger.exception("ttn: refresh error")
#        await cq.answer(f"Ошибка: {e}", show_alert=True)

@router.callback_query(F.data.in_({"ttn:cap:refresh", "ttn:refresh"}))
async def ttn_refresh_captcha(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "ttn.lookup", state=state):
        return
    uid = cq.from_user.id
    old = _TTN_FLOWS.get(uid)
    if not old or old.sess.closed:
        await cq.answer("Сессия истекла. Начните заново.", show_alert=True)
        return

    # закрываем старую сессию
    with contextlib.suppress(Exception):
        await old.sess.close()

    try:
        img_bytes, new_flow = await _fsrar_get_captcha(old.fr_id, old.base, old.ttn)
        _TTN_FLOWS[uid] = new_flow
        preview = _captcha_preview_bytes(img_bytes)

        # пробуем заменить фото «на месте»
        try:
            await cq.message.edit_media(
                media=InputMediaPhoto(
                    media=BufferedInputFile(preview, filename="captcha.png"),
                    caption=f"ТТН: <b>{esc(new_flow.ttn)}</b>\nВведите код с картинки:"
                ),
                reply_markup=_ttn_captcha_kb()
            )
        except Exception:
            # если редактировать нельзя — удаляем и шлём новое
            with contextlib.suppress(Exception):
                await cq.message.delete()
            await cq.message.answer_photo(
                BufferedInputFile(preview, filename="captcha.png"),
                caption=f"ТТН: <b>{esc(new_flow.ttn)}</b>\nВведите код с картинки:",
                reply_markup=_ttn_captcha_kb()
            )

        await cq.answer("Капча обновлена")
    except Exception as e:
        logger.exception("ttn: refresh error")
        await cq.answer(f"Ошибка: {e}", show_alert=True)

#------------КОНЕЦ ТТН----------------------------


@router.callback_query(F.data == "flt:set")
async def cb_flt_set(cq: CallbackQuery, state: FSMContext):
    await state.set_state(FilterSetState.waiting_value)
    await cq.message.edit_text(
        "Введи минимальную сумму долга (руб). Пример: <code>200</code> или <code>150.50</code>.\n"
        "Клиенты с нетто-долгом меньше порога будут скрыты в режимах «Общий» и «Просрочено».",
        reply_markup=back_only_kb()
    )
    await cq.answer()

@router.message(FilterSetState.waiting_value)
async def flt_set_value(m: Message, state: FSMContext):
    raw = (m.text or "").strip().replace(",", ".")
    try:
        val = float(raw)
        if val < 0 or val > 10_000_000:
            raise ValueError
    except Exception:
        await m.answer("Введите число от 0 до 10 000 000.")
        return
    set_min_debt(val)
    await state.clear()
    await m.answer(f"Порог сохранён: ≥ {fmt_money(val)} ₽", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.callback_query(F.data == "flt:reset")
async def cb_flt_reset(cq: CallbackQuery):
    set_min_debt(0.0)
    await cq.message.edit_text("Порог долга сброшен до 0 ₽.", reply_markup=_filters_page_kb(0))
    await cq.answer()

# --- Отсрочки меню/CRUD ---
@router.callback_query(F.data == "menu:back")
async def cb_back(cq: CallbackQuery):
    await cq.message.edit_text("Главное меню. Выберите действие:", reply_markup=None)
    # показываем правильную клавиатуру по роли
    role = get_user_role(getattr(cq.from_user, "id", None))
    kb = menu_for_role(role, getattr(cq.from_user, "id", None))
    await cq.message.answer("Выберите действие:", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data == "od:list")
async def cb_od_list(cq: CallbackQuery):
    if not _CLIENT_OD_MAP:
        await cq.message.edit_text("Список отсрочек пуст.", reply_markup=overdue_menu_kb())
        await cq.answer()
        return
    lines = ["<b>Индивидуальные отсрочки (дней):</b>"]
    for k, v in sorted(_CLIENT_OD_MAP.items(), key=lambda kv: kv[0]):
        lines.append(f"• <code>{esc(k)}</code> — {v}")
    await cq.message.edit_text("\n".join(lines), reply_markup=overdue_menu_kb())
    await cq.answer()

@router.callback_query(F.data == "od:add")
async def cb_od_add(cq: CallbackQuery, state: FSMContext):
    await state.set_state(OverdueSetStates.waiting_key)
    await cq.message.edit_text(
        "Введи <b>шаблон имени</b> (подстроку, без регистра), например: <code>волков</code>.\n"
        "Этот фрагмент будет искаться в названии клиента/адресе.",
        reply_markup=back_only_kb()
    )
    await cq.answer()

@router.callback_query(F.data == "od:edit")
async def cb_od_edit(cq: CallbackQuery, state: FSMContext):
    names = get_client_names()
    if not names:
        await cq.message.edit_text("Нет данных отчёта: сначала обнови файл (кнопка «🔄 Обновить» или /refresh).", reply_markup=overdue_menu_kb())
        await cq.answer()
        return
    await cq.message.edit_text("Выбери клиента для изменения отсрочки:", reply_markup=build_edit_keyboard(0, names))
    await cq.answer()

@router.callback_query(F.data.func(lambda d: d and d.startswith("od:pick:")))
async def cb_od_pick(cq: CallbackQuery):
    try:
        page = int(cq.data.split(":")[2])
    except Exception:
        page = 0
    names = get_client_names()
    if not names:
        await cq.message.edit_text("Нет данных отчёта.", reply_markup=overdue_menu_kb())
        await cq.answer()
        return
    await cq.message.edit_text("Выбери клиента для изменения отсрочки:", reply_markup=build_edit_keyboard(page, names))
    await cq.answer()

@router.callback_query(F.data.func(lambda d: d and d.startswith("od:sel:")))
async def cb_od_sel(cq: CallbackQuery, state: FSMContext):
    names = get_client_names()
    try:
        idx = int(cq.data.split(":")[2])
    except Exception:
        idx = -1
    if idx < 0 or idx >= len(names):
        await cq.answer("Не удалось определить клиента.")
        return
    name = names[idx]
    key = (name or "").casefold()
    personal = _CLIENT_OD_MAP.get(key)
    current = personal if personal is not None else get_overdue_days_for_client(name)
    flag = "индивидуально" if personal is not None else "по умолчанию"
    await state.set_state(OverdueEditStates.waiting_days)
    await state.update_data(key=key, client=name)
    await cq.message.edit_text(
        f"Клиент: <b>{esc(name)}</b>\n"
        f"Текущая отсрочка: <b>{current} дн.</b> ({flag})\n\n"
        "Введи новое число дней <b>0–999</b>.\n"
        "Введи <b>0</b>, чтобы <i>сбросить</i> индивидуальную отсрочку на общий порог.",
        reply_markup=back_only_kb()
    )
    await cq.answer()

@router.message(OverdueEditStates.waiting_days)
async def od_edit_days(m: Message, state: FSMContext):
    txt = (m.text or "").strip()
    if not re.fullmatch(r"\d{1,3}", txt):
        await m.answer("Введите число от 0 до 999.")
        return
    days = int(txt)
    data = await state.get_data()
    key = data.get("key")
    client = data.get("client") or key
    if not key:
        await state.clear()
        await m.answer("Внутренняя ошибка. Попробуйте заново.", reply_markup=overdue_menu_kb())
        return
    if days == 0:
        _CLIENT_OD_MAP.pop(key, None)
        _save_overdue_map(_CLIENT_OD_MAP)
        _sync_client_cards_overdue_from_map()
        await state.clear()
        await m.answer(f"Отсрочка для «{esc(client)}» <b>сброшена</b> до общего значения.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    else:
        _CLIENT_OD_MAP[key] = days
        _save_overdue_map(_CLIENT_OD_MAP)
        _sync_client_cards_overdue_from_map()
        await state.clear()
        await m.answer(f"Отсрочка для «{esc(client)}» установлена: <b>{days} дн.</b>", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(OverdueSetStates.waiting_key)
async def od_set_key(m: Message, state: FSMContext):
    key = (m.text or "").strip().casefold()
    if not key:
        await state.clear()
        await m.answer("Добавление отменено.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    await state.update_data(key=key)
    await state.set_state(OverdueSetStates.waiting_days)
    await m.answer(
        f"Сколько дней отсрочки назначить для клиента <code>{esc(key)}</code>? Введите число.",
        reply_markup=back_only_kb()
    )

@router.message(OverdueSetStates.waiting_days)
async def od_set_days(m: Message, state: FSMContext):
    txt = (m.text or "").strip()
    if not re.fullmatch(r"\d{1,3}", txt):
        await m.answer("Введите число от 0 до 999.")
        return
    days = int(txt)
    data = await state.get_data()
    key = data.get("key")
    if not key:
        await state.clear()
        await m.answer("Внутренняя ошибка, попробуйте снова.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    _CLIENT_OD_MAP[key] = days
    _save_overdue_map(_CLIENT_OD_MAP)
    _sync_client_cards_overdue_from_map()
    await state.clear()
    await m.answer(f"Сохранено: <code>{esc(key)}</code> → {days} дн.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.callback_query(F.data == "od:del")
async def cb_od_del(cq: CallbackQuery, state: FSMContext):
    await state.set_state(OverdueDelStates.waiting_key)
    await cq.message.edit_text(
        "Введи ключ (подстроку), который нужно удалить из отсрочек.\n"
        "Подсказка: смотри текущие ключи в «📋 Список».",
        reply_markup=back_only_kb()
    )
    await cq.answer()

@router.message(OverdueDelStates.waiting_key)
async def od_del_key(m: Message, state: FSMContext):
    key = (m.text or "").strip().casefold()
    if not key:
        await state.clear()
        await m.answer("Удаление отменено.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if key in _CLIENT_OD_MAP:
        _CLIENT_OD_MAP.pop(key)
        _save_overdue_map(_CLIENT_OD_MAP)
        _sync_client_cards_overdue_from_map()
        await m.answer(f"Удалено правило: <code>{esc(key)}</code>", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    else:
        await m.answer("Такого ключа нет в списке.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    await state.clear()

# --- Команды ---
@router.message(Command("report"))
async def on_report(m: Message):
    if not await ensure_message_access(m, "reports.general"):
        return
    mode, keywords, min_override = parse_report_args(m.text or "")
    await render_report(m, mode=mode, keywords=keywords, min_debt=min_override)


@router.message(Command("refresh"))
async def cmd_refresh(m: Message):
    if not await ensure_message_access(m, "updates.mail"):
        return

    text = (m.text or "")
    arg = ""
    parts = text.split(maxsplit=1)
    if len(parts) > 1:
        arg = parts[1].strip().lower()

    # распознаём типы: /refresh tara | /refresh debt | /refresh -> оба
    if any(k in arg for k in ("tara", "тар", "возврат")):
        types = ["ТАРА"]
    elif any(k in arg for k in ("debt", "дебитор", "дз")):
        types = ["ДЕБИТОРКА"]
    else:
        types = ["ДЕБИТОРКА", "ТАРА"]

    msgs = []
    ok = False
    await m.answer("Обновляю отчёт(ы) из почты…")
    for t in types:
        try:
            path = fetch_latest_file(t)
            if path:
                ok = True
                msgs.append(f"✅ {t}: <code>{esc(path)}</code>")
            else:
                msgs.append(f"⚠️ {t}: письмо/вложение не найдено")
        except Exception as e:
            logger.exception("Refresh failed for %s", t)
            msgs.append(f"❌ {t}: {e}")
    if ok:
        set_last_update("manual")

    await m.answer("\n".join(msgs), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))


@router.message(Command("tara"))
async def on_tara(m: Message):
    if not await ensure_message_access(m, "reports.tara"):
        return
    await render_tara_report(m)

async def _refresh_and_reply_cb(cq: CallbackQuery, mail_type: str):
    if not await ensure_callback_access(cq, "updates.mail"):
        return
    await cq.message.edit_text("Обновляю отчёт из почты…")
    try:
        path = fetch_latest_file(mail_type)  # 'ДЕБИТОРКА' или 'ТАРА'
        if path:
            set_last_update("manual")
            kb = menu_for_callback(cq)
            await cq.message.answer(f"Готово.", reply_markup=kb)
            #await cq.message.answer(f"Готово. Файл: <code>{esc(path)}</code>", reply_markup=kb)
        else:
            kb = menu_for_callback(cq)
            await cq.message.answer("Письмо не найдено или подходящих вложений нет.", reply_markup=kb)
    except Exception as e:
        logger.exception("Refresh failed")
        kb = menu_for_callback(cq)
        await cq.message.answer(f"Не удалось обновить: {e}", reply_markup=kb)
    await cq.answer()

@router.message(Command("refresh_tara"))
async def cmd_refresh_tara(m: Message):
    if not await ensure_message_access(m, "updates.mail"):
        return
    await m.answer("Обновляю отчёт из почты (Тара)…")
    try:
        path = fetch_latest_file("ТАРА")
        if path:
            set_last_update("manual")
            await m.answer(f"Готово. Файл: <code>{esc(path)}</code>", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        else:
            await m.answer("Письмо не найдено или подходящих вложений нет.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    except Exception as e:
        logger.exception("Manual refresh (tara) failed")
        await m.answer(f"Не удалось обновить: {e}", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.callback_query(F.data == "upd:debt")
async def cb_upd_debt(cq: CallbackQuery):
    await _refresh_and_reply_cb(cq, "ДЕБИТОРКА")

@router.callback_query(F.data == "upd:tara")
async def cb_upd_tara(cq: CallbackQuery):
    await _refresh_and_reply_cb(cq, "ТАРА")

#-------Коллбэки — навигация/сброс/изменение (с логами и try/except)
#-------Фильтры -----------------------
@router.callback_query(F.data.startswith("flt:nav:"))
async def flt_nav(cq: CallbackQuery, state: FSMContext):
    try:
        logger.debug("filters: NAV data=%s", cq.data)
        await state.clear()
        idx = int(cq.data.split(":")[2])
        logger.debug("filters: NAV idx=%s", idx)
        await _filters_safe_edit(cq.message, _filters_page_text(idx), _filters_page_kb(idx))
        await cq.answer()
    except Exception as e:
        logger.exception("filters: NAV failed")
        await cq.answer(f"Ошибка навигации: {type(e).__name__}", show_alert=False)

@router.callback_query(F.data.startswith("flt:reset:"))
async def flt_reset(cq: CallbackQuery, state: FSMContext):
    try:
        logger.debug("filters: RESET data=%s", cq.data)
        await state.clear()
        idx = int(cq.data.split(":")[2])
        page = FILTER_PAGES[idx]
        page["set"](page["default"])
        logger.info("filters: %s reset to %s", page["key"], page["default"])
        await _filters_safe_edit(cq.message, _filters_page_text(idx), _filters_page_kb(idx))
        await cq.answer("Сброшено.")
    except Exception as e:
        logger.exception("filters: RESET failed")
        await cq.answer(f"Ошибка сброса: {type(e).__name__}", show_alert=False)

@router.callback_query(F.data.startswith("flt:chg:"))
async def flt_change_start(cq: CallbackQuery, state: FSMContext):
    try:
        logger.debug("filters: CHG data=%s", cq.data)
        idx = int(cq.data.split(":")[2])
        page = FILTER_PAGES[idx]
        await state.update_data(flt_idx=idx)
        await state.set_state(FilterStates.wait_value)
        await _filters_safe_edit(
            cq.message,
            f"<b>{page['title']}</b>\n"
            f"Текущее значение: <code>{page['fmt'](page['get']())}</code>\n\n"
            f"Введите новое значение ({page['units']}).",
            _filters_page_kb(idx)
        )
        await cq.answer()
    except Exception as e:
        logger.exception("filters: CHG failed")
        await cq.answer(f"Ошибка: {type(e).__name__}", show_alert=False)

@router.message(FilterStates.wait_value)
async def flt_change_apply(m: Message, state: FSMContext):
    data = await state.get_data()
    idx = int(data.get("flt_idx", 0))
    page = FILTER_PAGES[idx]
    raw = (m.text or "")
    logger.debug("filters: APPLY %s raw='%s'", page["key"], raw)
    try:
        val = page["parse"](raw)
        ok, hint = page["validate"](val)
        if not ok:
            raise ValueError(hint)
        page["set"](val)
        logger.info("filters: %s set to %s", page["key"], val)
        await state.clear()
        await m.answer(_filters_page_text(idx), reply_markup=_filters_page_kb(idx), disable_web_page_preview=True)
    except Exception as e:
        logger.exception("filters: APPLY failed")
        await m.answer(f"Некорректно: <code>{esc(raw)}</code>. {e}")



# --- Настройки (/settings только админам) ---
@router.message(Command("settings"))
async def on_settings(m: Message):
    if not is_admin(getattr(m.from_user, "id", None)):
        await m.answer("Недостаточно прав.", reply_markup=menu_for_message(m))
        return
    await m.answer("⚙️ Настройки (хранятся в settings/config.json):", reply_markup=settings_menu_kb())

# Callbacks настроек
@router.callback_query(F.data == "cfg:bot")
async def cfg_bot(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Нет доступа.")
        return
    await state.set_state(ConfigStates.waiting_bot_token)
    await cq.message.edit_text("Введи новый <b>BOT_TOKEN</b>:", reply_markup=back_only_kb())
    await cq.answer()

@router.callback_query(F.data == "cfg:imap")
async def cfg_imap(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Нет доступа.")
        return
    await state.set_state(ConfigStates.waiting_imap_server)
    await cq.message.edit_text("Введи <b>IMAP_SERVER</b> (например, <code>imap.yandex.ru</code>):", reply_markup=back_only_kb())
    await cq.answer()

@router.callback_query(F.data == "cfg:email")
async def cfg_email(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Нет доступа.")
        return
    await state.set_state(ConfigStates.waiting_email_account)
    await cq.message.edit_text("Введи <b>EMAIL_ACCOUNT</b> (почтовый логин):", reply_markup=back_only_kb())
    await cq.answer()

@router.callback_query(F.data == "cfg:pass")
async def cfg_pass(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Нет доступа.")
        return
    await state.set_state(ConfigStates.waiting_email_password)
    await cq.message.edit_text("Введи <b>EMAIL_PASSWORD</b> (будет сохранён в settings/config.json):", reply_markup=back_only_kb())
    await cq.answer()

# Ввод значений настроек
@router.message(ConfigStates.waiting_bot_token)
async def set_bot_token(m: Message, state: FSMContext):
    token = (m.text or "").strip()
    try:
        validate_token(token)
    except TokenValidationError:
        await m.answer("❌ Токен не прошёл валидацию. Проверь и отправь снова.")
        return

    update_setting("BOT_TOKEN", token)
    try:
        await m.delete()  # попытка скрыть токен
    except Exception:
        pass

    await state.clear()
    await m.answer("✅ BOT_TOKEN сохранён. Перезапусти бота, чтобы применить.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(ConfigStates.waiting_imap_server)
async def set_imap_server(m: Message, state: FSMContext):
    host = (m.text or "").strip()
    if not host or " " in host:
        await m.answer("❌ Неверный IMAP_SERVER. Пример: <code>imap.yandex.ru</code>")
        return
    update_setting("IMAP_SERVER", host)
    await state.clear()
    await m.answer("✅ IMAP_SERVER сохранён.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(ConfigStates.waiting_email_account)
async def set_email_account(m: Message, state: FSMContext):
    acc = (m.text or "").strip()
    if not acc:
        await m.answer("❌ EMAIL_ACCOUNT пуст. Введи значение.")
        return
    update_setting("EMAIL_ACCOUNT", acc)
    await state.clear()
    await m.answer("✅ EMAIL_ACCOUNT сохранён.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(ConfigStates.waiting_email_password)
async def set_email_password(m: Message, state: FSMContext):
    pwd = (m.text or "").strip()
    if not pwd:
        await m.answer("❌ EMAIL_PASSWORD пуст. Введи значение.")
        return
    update_setting("EMAIL_PASSWORD", pwd)
    try:
        await m.delete()  # скрыть пароль
    except Exception:
        pass
    await state.clear()
    await m.answer("✅ EMAIL_PASSWORD сохранён.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(Command("reset_role"))
async def reset_role_cmd(m: Message, state: FSMContext):
    uid = str(m.from_user.id)

    # 1) удалить из in-memory кэша ролей
    _USER_ROLES.pop(uid, None)

    # 2) сохранить в файл settings/user_roles.json
    _save_user_roles(_USER_ROLES)

    # 3) очистить FSM и сразу запустить онбординг роли
    await state.clear()
    await m.answer("✅ Роль сброшена. Выберите новую роль:")
    await state.set_state(OnboardStates.waiting_role)
    await m.answer("Вы админ или клиент?", reply_markup=onboard_role_kb())

@router.message(Command("users"))
@router.message(F.text == "👥 Пользователи")
async def admin_users_list(m: Message):
    if not await ensure_message_access(m, "users.view"):
        return
    await m.answer("Список пользователей:", reply_markup=users_list_kb())

@router.message(F.text == "🔔 Уведомления")
async def notifications_menu(m: Message):
    if not await ensure_message_access(m, "notifications.manage"):
        return
    await m.answer("Управление уведомлениями:", reply_markup=notifications_menu_kb(getattr(m.from_user, "id", 0)))

@router.callback_query(F.data == "notify:menu")
async def notifications_menu_callback(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "notifications.manage"):
        return
    await cq.message.answer("Управление уведомлениями:", reply_markup=notifications_menu_kb(getattr(cq.from_user, "id", 0)))
    await cq.answer()

@router.callback_query(F.data.startswith("notify:toggle:"))
async def notifications_toggle(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "notifications.manage"):
        return
        parts = (cq.data or "").split(":")
        key = parts[2] if len(parts) > 2 else ""
        target_user_id = int(getattr(cq.from_user, "id", 0) or 0)
        if key not in NOTIFICATION_ORDER:
            await cq.answer("Неизвестный тип уведомления.", show_alert=True)
            return
        enabled = notification_enabled(target_user_id, key)
        set_user_notification_setting(target_user_id, key, not enabled)
        await cq.message.edit_text("Управление уведомлениями:", reply_markup=notifications_menu_kb(target_user_id))
        await cq.answer("Настройка обновлена.")

    @router.callback_query(F.data.startswith("usr:notifymenu:"))
    async def admin_user_notifications_menu(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "users.manage"):
            return
        parts = (cq.data or "").split(":")
        uid = parts[2] if len(parts) > 2 else ""
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        if not uid or not uid.isdigit():
            await cq.answer("Пользователь не найден.", show_alert=True)
            return
        await cq.message.edit_text(
            f"🔔 Уведомления пользователя <code>{uid}</code>",
            reply_markup=admin_user_notifications_kb(uid, page=page),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("usr:notify:"))
    async def admin_user_notification_toggle(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "users.manage"):
            return
        parts = (cq.data or "").split(":")
        uid = parts[2] if len(parts) > 2 else ""
        key = parts[3] if len(parts) > 3 else ""
        page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
        if not uid or not uid.isdigit():
            await cq.answer("Пользователь не найден.", show_alert=True)
            return
        if key not in NOTIFICATION_ORDER:
            await cq.answer("Неизвестный тип уведомления.", show_alert=True)
            return
        user_id = int(uid)
        enabled = notification_enabled(user_id, key)
        set_user_notification_setting(user_id, key, not enabled)
        await cq.message.edit_text(
            f"🔔 Уведомления пользователя <code>{uid}</code>",
            reply_markup=admin_user_notifications_kb(uid, page=page),
        )
        await cq.answer("Настройка обновлена.")

@router.callback_query(F.data.startswith("usr:list:"))
async def admin_users_list_page(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.view"):
        return
    try:
        page = int(cq.data.split(":")[2])
    except Exception:
        page = 0
    await cq.message.edit_text("Список пользователей:", reply_markup=users_list_kb(page=page))
    await cq.answer()

@router.callback_query(F.data.startswith("usr:sel:"))
async def admin_users_select(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.view"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    data = _roles_load()
    rec = data.get(uid, {}) if uid else {}
    name = (rec.get("name") or "unknown").strip()
    role = normalize_role(rec.get("role") or "guest")
    phone = (rec.get("phone") or "—").strip()
    verified = "✅" if rec.get("phone_verified") else "❌"
    is_authorized = bool(rec.get("phone_verified"))
    blocked = "⛔" if rec.get("blocked") else "✅"
    custom_rights = len(_normalize_access_overrides(rec.get("access_overrides")))
    notify_new_users = "✅" if (notification_enabled(int(uid), "new_users") if uid.isdigit() else False) else "❌"
    notify_role_changes = "✅" if (notification_enabled(int(uid), "role_changes") if uid.isdigit() else False) else "❌"
    notify_auth_changes = "✅" if (notification_enabled(int(uid), "auth_changes") if uid.isdigit() else False) else "❌"
    username = f"@{rec.get('username')}" if rec.get("username") else "—"
    first_name = (rec.get("first_name") or "—").strip()
    last_name = (rec.get("last_name") or "—").strip()
    language_code = (rec.get("language_code") or "—").strip()
    premium = "✅" if rec.get("is_premium") else "❌"
    onboard_done = "✅" if rec.get("onboard_completed") else "❌"
    manual_auth = "✅" if rec.get("authorized_by_admin") else "❌"
    can_manage = user_allows_action(getattr(cq.from_user, "id", None), "users.manage")
    text = (
        f"<b>Пользователь</b>\n"
        f"ID: <code>{esc(uid)}</code>\n"
        f"Роль: <b>{esc(role_label(role))}</b>\n"
        f"Имя: <b>{esc(name)}</b>\n"
        f"Username: <b>{esc(username)}</b>\n"
        f"Telegram имя: <b>{esc(first_name)}</b>\n"
        f"Telegram фамилия: <b>{esc(last_name)}</b>\n"
        f"Язык Telegram: <b>{esc(language_code)}</b>\n"
        f"Telegram Premium: {premium}\n"
        f"Телефон: <b>{esc(phone)}</b> ({verified})\n"
        f"Онбординг завершён: {onboard_done}\n"
        f"Ручная авторизация: {manual_auth}\n"
        f"Доступ: {blocked}\n"
        f"Индивидуальных прав: <b>{custom_rights}</b>\n"
        f"Уведомления (новые/роли/авторизация): {notify_new_users}/{notify_role_changes}/{notify_auth_changes}"
    )
    await cq.message.edit_text(
        text,
        reply_markup=user_detail_kb(uid, page=page, is_authorized=is_authorized, can_manage=can_manage),
    )
    await cq.answer()

@router.callback_query(F.data.startswith("usr:auth:"))
async def admin_users_toggle_auth(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    rec = _roles_load().get(uid, {})
    if not isinstance(rec, dict):
        rec = {"role": "guest", "name": str(rec)}
    new_auth_state = not bool(rec.get("phone_verified"))
    update_user_record(uid, {"phone_verified": new_auth_state})
    if uid.isdigit():
        await notify_about_access_change(
            actor_id=getattr(cq.from_user, "id", None),
            target_user_id=int(uid),
            event_label="Ручная авторизация",
            new_value_label="Включена" if new_auth_state else "Отключена",
        )
    await cq.answer("Авторизация выдана." if new_auth_state else "Авторизация снята.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:setrole:"))
async def admin_users_set_role(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    role = parts[3] if len(parts) > 3 else "guest"
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    old_role = normalize_role((_roles_load().get(uid, {}) or {}).get("role") or "guest")
    new_role = normalize_role(role)
    update_user_record(uid, {"role": new_role})
    await push_user_menu_refresh(uid, "🔄 Ваши права обновлены. Новое меню уже доступно.")
    if uid.isdigit():
        await notify_about_role_change(
            actor_id=getattr(cq.from_user, "id", None),
            target_user_id=int(uid),
            old_role=old_role,
            new_role=new_role,
        )
    await cq.answer("Роль обновлена.")
    if cq.message and cq.message.reply_markup:
        markup = cq.message.reply_markup
        is_permissions_screen = any(
            btn.callback_data and btn.callback_data.startswith("usr:permtoggle:")
            for row in markup.inline_keyboard for btn in row
        )
        if is_permissions_screen:
            await cq.message.edit_text(
                f"Права пользователя <code>{esc(uid)}</code> обновлены.\n"
                    f"Текущая роль: <b>{esc(role_label(new_role))}</b>",
                reply_markup=user_permissions_kb(uid)
            )
            return
    await admin_users_select(cq)

@router.callback_query(F.data == "usr:perms:noop")
async def admin_users_permissions_noop(cq: CallbackQuery):
    await cq.answer()

@router.callback_query(F.data.startswith("usr:perms:"))
async def admin_users_permissions(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.view"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    rec = _user_record(int(uid)) if uid.isdigit() else {}
    role = normalize_role(rec.get("role") or "guest")
    can_manage = user_allows_action(getattr(cq.from_user, "id", None), "users.manage")
    await cq.message.edit_text(
        f"Права пользователя <code>{esc(uid)}</code>\n"
        f"Роль по умолчанию: <b>{esc(role_label(role))}</b>\n"
        f"{'Нажимайте на пункты, чтобы включать или выключать доступ.' if can_manage else 'Режим просмотра прав (без изменений).'}",
        reply_markup=user_permissions_kb(uid, page=page, can_manage=can_manage)
    )
    await cq.answer()

@router.callback_query(F.data.startswith("usr:permtoggle:"))
async def admin_users_permission_toggle(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    token = parts[3] if len(parts) > 3 else ""
    page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
    if not uid or not uid.isdigit():
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    action = MANAGED_ACTIONS_BY_TOKEN.get(token)
    if not action:
        await cq.answer("Право не найдено.", show_alert=True)
        return
    user_id = int(uid)
    base_allowed = role_allows_action(get_user_role(user_id), action)
    current_allowed = user_allows_action(user_id, action)
    new_allowed = not current_allowed
    override = None if new_allowed == base_allowed else new_allowed
    set_user_action_override(user_id, action, override)
    await push_user_menu_refresh(user_id, f"🔄 Доступ к разделу «{MANAGED_ACTIONS_LABELS[action]}» обновлён.")
    await cq.message.edit_reply_markup(reply_markup=user_permissions_kb(uid, page=page))
    await cq.answer(f"{MANAGED_ACTIONS_LABELS[action]}: {'включено' if new_allowed else 'выключено'}")

@router.callback_query(F.data.startswith("usr:permreset:"))
async def admin_users_permission_reset(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    if not uid or not uid.isdigit():
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    reset_user_action_overrides(int(uid))
    await push_user_menu_refresh(int(uid), "🔄 Индивидуальные права сброшены. Меню обновлено.")
    await cq.message.edit_reply_markup(reply_markup=user_permissions_kb(uid, page=page))
    await cq.answer("Пользовательские права сброшены до роли по умолчанию.")

@router.callback_query(F.data.startswith("usr:block:"))
async def admin_users_block(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    uid = cq.data.split(":")[2] if len(cq.data.split(":")) > 2 else ""
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    update_user_record(uid, {"blocked": True})
    if uid.isdigit():
        await notify_about_access_change(
            actor_id=getattr(cq.from_user, "id", None),
            target_user_id=int(uid),
            event_label="Блокировка",
            new_value_label="Заблокирован",
        )
    await cq.answer("Пользователь заблокирован.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:unblock:"))
async def admin_users_unblock(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "users.manage"):
        return
    uid = cq.data.split(":")[2] if len(cq.data.split(":")) > 2 else ""
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    update_user_record(uid, {"blocked": False})
    if uid.isdigit():
        await notify_about_access_change(
            actor_id=getattr(cq.from_user, "id", None),
            target_user_id=int(uid),
            event_label="Блокировка",
            new_value_label="Разблокирован",
        )
    await cq.answer("Пользователь разблокирован.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:del:"))
async def admin_users_delete(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "users.manage", state=state):
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    await state.update_data(admin_del_uid=uid, admin_del_page=page)
    await state.set_state(AdminUserEditStates.waiting_delete_confirm)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data="usr:confirm_del:yes"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="usr:confirm_del:no"),
            ]
        ]
    )
    await cq.message.answer("Точно удалить пользователя?", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("usr:confirm_del:"))
async def admin_users_delete_confirm(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "users.manage", state=state):
        return
    action = cq.data.split(":")[-1]
    data = await state.get_data()
    uid = data.get("admin_del_uid")
    page = data.get("admin_del_page", 0)
    await state.clear()
    if action == "no":
        await cq.message.answer("❎ Отменено.")
        await cq.answer()
        return
    if not uid:
        await cq.message.answer("Пользователь не найден.")
        await cq.answer()
        return
    deleted = delete_user_record(uid)
    if not deleted:
        await cq.message.answer("⚠️ Пользователь не найден.")
        await cq.answer()
        return
    await cq.message.answer("✅ Пользователь удалён.", reply_markup=users_list_kb(page=page))
    await cq.answer()

@router.callback_query(F.data.startswith("usr:editname:"))
async def admin_users_edit_name(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "users.manage", state=state):
        return
    uid = cq.data.split(":")[2]
    await state.update_data(admin_edit_uid=uid)
    await state.set_state(AdminUserEditStates.waiting_name)
    await cq.message.answer("Введите новое имя пользователя:")
    await cq.answer()

@router.callback_query(F.data.startswith("usr:editphone:"))
async def admin_users_edit_phone(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "users.manage", state=state):
        return
    uid = cq.data.split(":")[2]
    await state.update_data(admin_edit_uid=uid)
    await state.set_state(AdminUserEditStates.waiting_phone)
    await cq.message.answer("Введите новый телефон (например, +7XXXXXXXXXX):")
    await cq.answer()

@router.message(AdminUserEditStates.waiting_name)
async def admin_users_save_name(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "users.manage", state=state):
        return
    data = await state.get_data()
    uid = data.get("admin_edit_uid")
    name = (m.text or "").strip()
    if not uid or not name:
        await m.answer("Некорректные данные. Попробуйте снова.")
        return
    update_user_record(uid, {"name": name})
    await state.clear()
    await m.answer("✅ Имя обновлено.")

@router.message(AdminUserEditStates.waiting_phone)
async def admin_users_save_phone(m: Message, state: FSMContext):
    if not await ensure_message_access(m, "users.manage", state=state):
        return
    data = await state.get_data()
    uid = data.get("admin_edit_uid")
    ok, e164, disp = normalize_phone_ru(m.text or "")
    if not uid or not ok:
        await m.answer("Некорректный телефон. Пример: +7XXXXXXXXXX.")
        return
    update_user_record(uid, {"phone": e164, "phone_verified": False})
    await state.clear()
    await m.answer(f"✅ Телефон обновлён: {disp}")

# --- Клиентский узкий поиск ---
async def run_client_search(m: Message, raw_query: str):
    q = (raw_query or "").strip().casefold()
    if not q:
        await m.answer("Пустой запрос.", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return

    path = find_latest_download()
    if not path:
        await m.answer("Файл отчёта не найден. Сначала обновите его.", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return

    try:
        res = process_file(path)
    except Exception as e:
        logger.exception("Ошибка при разборе файла")
        await m.answer(f"Не удалось разобрать файл: {e}", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return

    items: List[Dict[str, Any]] = (res or {}).get("items") or []
    report_date = (res or {}).get("report_date")

    def _match(it: Dict[str, Any]) -> bool:
        name = (it.get("client_name") or it.get("client") or "").casefold()
        addr = (it.get("address") or "").casefold()
        return (q in name) or (q in addr)

    filtered = [it for it in items if _match(it)]
    if not filtered:
        await m.answer("Ничего не найдено.", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return

    await m.answer(
        f"<b>Результаты по «{esc(raw_query)}»</b>"
        f"{(' на '+esc(report_date)) if report_date else ''}",
        disable_web_page_preview=True,
        reply_markup=client_menu_kb(getattr(m.from_user, "id", None))
    )
    for i, it in enumerate(filtered, 1):
        text = build_client_text(it, i, report_date)
        await send_long(m, text)


#акции ------------------------------------------------------------------------------------------
def actor_id(obj):
    if hasattr(obj, "from_user") and getattr(obj, "from_user", None):
        return getattr(obj.from_user, "id", None)
    return None

def is_admin_event(obj) -> bool:
    return is_admin(actor_id(obj))

def _promos_load() -> List[Dict[str, Any]]:
    if not PROMO_INDEX.exists():
        return []
    try:
        return json.loads(PROMO_INDEX.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("promos: index parse error, fallback empty")
        return []

def _promos_save(items: List[Dict[str, Any]]) -> None:
    tmp = PROMO_INDEX.with_suffix(PROMO_INDEX.suffix + ".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PROMO_INDEX)

def _promo_set(it: Dict[str, Any]) -> None:
    lst = _promos_load()
    for i, ex in enumerate(lst):
        if ex.get("id") == it["id"]:
            lst[i] = it
            break
    else:
        lst.append(it)
    _promos_save(lst)

async def _save_incoming_promo_file(m: Message, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("promo:download start -> %s (type=%s)", dest, m.content_type)
    try:
        if m.document:
            await m.bot.download(m.document, destination=dest)
        elif m.photo:
            await m.bot.download(m.photo[-1], destination=dest)
        else:
            raise RuntimeError("no file/photo in message")
        size = dest.stat().st_size if dest.exists() else 0
        logger.info("promo:download done  -> %s (%d bytes)", dest, size)
    except Exception as e:
        logger.exception("promo: download failed -> %s", e)
        raise

async def _promo_create_stub(state: FSMContext, media_pair: Optional[Tuple[Message, str]] = None) -> str:
    """
    Создаёт акцию сразу после шага «медиа» (или /skip):
    - starts_at = сегодня (локальная TZ)
    - ends_at = None (потом проставим календарём)
    - сохраняет картинку/PDF, если передали
    Возвращает pid.
    """
    data = await state.get_data()
    title = (data.get("title") or "").strip()
    text  = (data.get("text")  or "").strip()

    pid = uuid.uuid4().hex[:12]
    img_name = None
    pdf_name = None

    if media_pair:
        msg, ext = media_pair
        if ext in ALLOWED_PROMO_IMG:
            img_name = f"{pid}.{ext}"
            await _save_incoming_promo_file(msg, PROMO_DIR / img_name)
        elif ext in ALLOWED_PROMO_DOC:
            pdf_name = f"{pid}.pdf"
            await _save_incoming_promo_file(msg, PROMO_DIR / pdf_name)

    now = datetime.now(TZ)
    item = {
        "id": pid,
        "title": title,
        "text": text,
        "image": img_name,
        "doc": pdf_name,
        "starts_at": now.date().isoformat(),   # старт = сегодня
        "ends_at": None,                       # конец выберем календарём
        "active": True,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    _promo_set(item)
    return pid

def _promo_delete(pid: str) -> None:
    lst = [x for x in _promos_load() if x.get("id") != pid]
    _promos_save(lst)

def _promo_find(pid: str) -> Optional[Dict[str, Any]]:
    for it in _promos_load():
        if it.get("id") == pid:
            return it
    return None

def _promo_is_active(it: Dict[str, Any], dt: Optional[datetime] = None) -> bool:
    dt = dt or datetime.now(TZ)
    if not it.get("active", True):
        return False
    s = it.get("starts_at")
    e = it.get("ends_at")
    try:
        if s and dt.date() < datetime.fromisoformat(s).date():
            return False
        if e and dt.date() > datetime.fromisoformat(e).date():
            return False
    except Exception:
        pass
    return True

from datetime import datetime, date, timezone

def _parse_iso_date_safe(s: str) -> date:
    """
    '2025-10-31' -> date(2025,10,31), иначе date.min
    """
    try:
        if not s:
            return date.min
        # datetime.fromisoformat('YYYY-MM-DD') тоже ок и вернёт datetime → берём .date()
        if "T" in s:
            return datetime.fromisoformat(s).date()
        return datetime.fromisoformat(s).date() if len(s) == 10 else date.min
    except Exception:
        try:
            # запасной путь: откусить время, если вдруг прилетело с T
            return datetime.fromisoformat(s.split("T", 1)[0]).date()
        except Exception:
            return date.min

def _parse_iso_dt_safe(s: str) -> datetime:
    """
    '2025-10-21T23:37:06.368352+07:00' -> aware datetime, иначе 1970-01-01Z
    """
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(s)  # понимает +07:00
    except Exception:
        # поддержка 'Z' и прочих форм
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
    # делаем aware
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _epoch_safe(dt: datetime) -> float:
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _promo_sort_key(it: Dict[str, Any]):
    """
    Сортируем по:
      1) starts_at (позже — выше),
      2) updated_at (новее — выше),
      3) title (A..Z).
    """
    s_iso = it.get("starts_at") or ""
    u_iso = it.get("updated_at") or it.get("created_at") or ""

    s_date = _parse_iso_date_safe(s_iso)
    u_dt   = _parse_iso_dt_safe(u_iso)

    return (-s_date.toordinal(), -_epoch_safe(u_dt), (it.get("title") or "").casefold())

_RU_DATE_RX = re.compile(
    r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s*[-–]\s*(\d{1,2})\.(\d{1,2})\.(\d{4}))?\s*$"
)

def _iso_from_ru(d: str) -> str:
    dd, mm, yyyy = d.split(".")
    return f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"

def _ru_from_iso(iso: Optional[str]) -> Optional[str]:
    if not iso:
        return None
    y, m, d = iso.split("-")
    return f"{int(d):02d}.{int(m):02d}.{int(y):04d}"

def parse_ru_date_range(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Принимает 'ДД.ММ.ГГГГ' или 'ДД.ММ.ГГГГ - ДД.ММ.ГГГГ'.
    Возвращает (start_iso, end_iso).
    ВАЖНО: одиночная дата трактуется как ДАТА ОКОНЧАНИЯ (включительно).
    """
    s = (s or "").strip()
    m = _RU_DATE_RX.match(s)
    if not m:
        # одиночная дата без дефиса: считаем "до этой даты" (end)
        if re.match(r"^\s*\d{1,2}\.\d{1,2}\.\d{4}\s*$", s):
            return None, _iso_from_ru(s)
        return None, None

    d1 = f"{int(m.group(1)):02d}.{int(m.group(2)):02d}.{int(m.group(3)):04d}"
    if m.group(4):
        d2 = f"{int(m.group(4)):02d}.{int(m.group(5)):02d}.{int(m.group(6)):04d}"
    else:
        d2 = None

    # если нет второй даты — это "до d1" (end-only)
    return (None, _iso_from_ru(d1)) if not d2 else (_iso_from_ru(d1), _iso_from_ru(d2))

def _promo_get_all(include_inactive: bool = False) -> List[Dict[str, Any]]:
    items = _promos_load()
    if not include_inactive:
        items = [x for x in items if _promo_is_active(x)]
    # фильтр «битых» файлов не нужен — превью опционально
    items.sort(key=_promo_sort_key)
    return items

def _promo_short(html_text: str, limit: int = 180) -> str:
    # короткая выжимка текста (снятие тегов)
    t = re.sub(r"<[^>]+>", "", html_text or "")
    t = _html.unescape(t).replace("\xa0", " ").strip()
    return (t[:limit] + "…") if len(t) > limit else t

from typing import Tuple, Optional

def _extract_media_id_and_ext(m: Message) -> Optional[Tuple[str, str]]:
    """
    Возвращает (file_id, ext) для фото/документа акции.
    photo -> (file_id, 'jpg')
    document -> (file_id, 'pdf'|'jpg'|'png'|'webp')
    """
    if m.photo:
        return m.photo[-1].file_id, "jpg"
    if m.document:
        ext = (_guess_promo_ext(m) or "").lower()
        if not ext:
            return None
        return m.document.file_id, ("jpg" if ext == "jpeg" else ext)
    return None

async def _save_file_by_id(bot, file_id: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("promo:download by id -> %s", dest)
    await bot.download(file_id, destination=dest)
    logger.info("promo:download done -> %s", dest)

def _guess_promo_ext(m: Message) -> Optional[str]:
    # Document
    if m.document:
        name = (m.document.file_name or "").lower()
        for ext in ("pdf", "jpg", "jpeg", "png", "webp"):
            if name.endswith(f".{ext}"):
                return "jpg" if ext == "jpeg" else ext
        mt = (m.document.mime_type or "").lower()
        if "pdf" in mt:   return "pdf"
        if "jpeg" in mt:  return "jpg"
        if "jpg" in mt:   return "jpg"
        if "png" in mt:   return "png"
        if "webp" in mt:  return "webp"
        return None
    # Photo (из Telegram всегда JPEG)
    if m.photo:
        return "jpg"
    return None

def _promo_preview_16x9(img_bytes: bytes, w: int = 800, h: int = 450, pad: int = 24) -> bytes:
    try:
        from PIL import Image
    except Exception:
        return img_bytes
    import io
    try:
        with Image.open(io.BytesIO(img_bytes)) as im:
            im = im.convert("RGB")
            max_w, max_h = max(1, w - 2*pad), max(1, h - 2*pad)
            scale = min(max_w / im.width, max_h / im.height)
            new_size = (max(1, int(im.width*scale)), max(1, int(im.height*scale)))
            if new_size != im.size:
                im = im.resize(new_size, Image.LANCZOS)
            canvas = Image.new("RGB", (w, h), (255, 255, 255))
            x = (w - im.width)//2; y = (h - im.height)//2
            canvas.paste(im, (x, y))
            buf = io.BytesIO(); canvas.save(buf, "PNG")
            return buf.getvalue()
    except Exception:
        return img_bytes

#------------клавиатуры акция
def _promo_list_kb(items: List[Dict[str, Any]], page: int, admin: bool) -> InlineKeyboardMarkup:
    total = len(items)
    last_page = max(0, (total - 1) // PROMO_PAGE_SIZE)
    page = max(0, min(page, last_page))
    start = page * PROMO_PAGE_SIZE
    end = min(total, start + PROMO_PAGE_SIZE)

    rows = []
    for it in items[start:end]:
        title = it.get("title") or "Без названия"
        if admin:
            # эффективная активность = флаг active И дата сегодня в интервале (инклюзивно)
            effective = _promo_is_active(it)
            badge = "✅" if effective else "⛔"
        else:
            badge = "🗂"
        rows.append([
            InlineKeyboardButton(text=f"{badge} {title}", callback_data=f"promo:view:{it['id']}")
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"promo:list:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{last_page+1}", callback_data="promo:list:noop"))
    if page < last_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"promo:list:{page+1}"))
    rows.append(nav)

    if admin:
        rows.append([InlineKeyboardButton(text="➕ Добавить", callback_data="promo:add")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:main")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def _promo_item_kb(pid: str, admin: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="promo:list:0")]]
    if admin:
        rows.insert(0, [
            InlineKeyboardButton(text="✏️ Название", callback_data=f"promo:rename:{pid}"),
            InlineKeyboardButton(text="📝 Текст", callback_data=f"promo:edittext:{pid}")
        ])
        rows.insert(1, [
            InlineKeyboardButton(text="🖼 Картинка", callback_data=f"promo:replaceimg:{pid}"),
            InlineKeyboardButton(text="📅 Даты", callback_data=f"promo:dates:{pid}"),
        ])
        rows.insert(2, [
            InlineKeyboardButton(text="✅ Активна/⛔", callback_data=f"promo:toggle:{pid}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"promo:del:{pid}")
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


#Просмотр акции (красивое превью):
async def _send_promo_preview(m: Message, it: Dict[str, Any], admin: bool):
    title = esc(it.get("title") or "Без названия")
    dates = []
    if it.get("starts_at"): dates.append(f"с {_ru_from_iso(it['starts_at'])}")
    if it.get("ends_at"):   dates.append(f"до {_ru_from_iso(it['ends_at'])}")
    dt_line = f"\n<i>{' '.join(dates)}</i>" if dates else ""

    status_line = ""
    if admin:
        eff = _promo_is_active(it)  # учитывает и флаг, и даты
        if eff:
            status_line = "\n<i>Статус: ✅ активна</i>"
        else:
            # отличим «скрыта» от «вне периода»
            flag = it.get("active", True)
            status_line = "\n<i>Статус: ⛔ скрыта</i>" if not flag else "\n<i>Статус: ⛔ вне периода</i>"

    short = _promo_short(it.get("text", ""))
    caption = f"<b>{title}</b>{dt_line}{status_line}\n\n{esc(short)}"
    pid = it["id"]

    # если есть картинка — отправим как фото с подписью
    img_name = it.get("image")
    if img_name:
        p = PROMO_DIR / img_name
        if p.exists():
            try:
                raw = p.read_bytes()
                prev = _promo_preview_16x9(raw, 800, 450, 24)
                await m.answer_photo(
                    BufferedInputFile(prev, filename=f"promo_{pid}.png"),
                    caption=caption,
                    reply_markup=_promo_item_kb(pid, admin)
                )
                return
            except Exception:
                logger.exception("promo: preview send failed")

    # иначе просто текст + кнопки
    await m.answer(caption, reply_markup=_promo_item_kb(pid, admin), disable_web_page_preview=True)


async def _promo_finish_create(
    m: Message, state: FSMContext,
    starts_at: Optional[str], ends_at: Optional[str],
    actor_id: Optional[int] = None,
):
    uid = actor_id if actor_id is not None else getattr(getattr(m, "from_user", None), "id", None)
    if not is_admin(uid):
        await state.clear(); return

    data = await state.get_data()
    title = (data.get("title") or "").strip()
    text  = (data.get("text")  or "").strip()
    pid   = uuid.uuid4().hex[:12]

    img_name = None
    pdf_name = None

    file_id = data.get("_media_file_id")
    ext     = (data.get("_media_ext") or "").lower()

    if file_id and ext:
        if ext in ALLOWED_PROMO_IMG:
            img_name = f"{pid}.{ext}"
            await _save_file_by_id(m.bot, file_id, PROMO_DIR / img_name)
        elif ext in ALLOWED_PROMO_DOC:
            pdf_name = f"{pid}.pdf"
            await _save_file_by_id(m.bot, file_id, PROMO_DIR / pdf_name)

    now_dt = datetime.now(TZ)
    if not starts_at:
        starts_at = now_dt.date().isoformat()

    now_iso = now_dt.isoformat()
    _promo_set({
        "id": pid, "title": title, "text": text,
        "image": img_name, "doc": pdf_name,
        "starts_at": starts_at, "ends_at": ends_at,
        "active": True,
        "created_at": now_iso, "updated_at": now_iso
    })

    await state.clear()
    period_human = " — ".join(filter(None, [_ru_from_iso(starts_at), _ru_from_iso(ends_at)])) or "без даты"
    await m.answer(f"✅ Акция «{esc(title)}» добавлена.\n<i>Период: {period_human}</i>", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.message(F.text == "🎁 Акции", StateFilter(None))
async def btn_promos(m: Message):
    if not await ensure_message_access(m, "promos.view"):
        return
    admin = is_admin(getattr(m.from_user, "id", None))
    items = _promo_get_all(include_inactive=admin)  # админ видит всё
    await m.answer("<b>Акции</b>\nВыберите пункт:",
                   reply_markup=_promo_list_kb(items, page=0, admin=admin))

#пагинация
@router.callback_query(F.data.startswith("promo:list:"))
async def cb_promos_list(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if cq.data.endswith(":noop"):
        await cq.answer(); return
    page = int(cq.data.split(":")[-1])
    admin = is_admin(getattr(cq.from_user, "id", None))
    items = _promo_get_all(include_inactive=admin)
    await cq.message.edit_text("<b>Акции</b>\nВыберите пункт:",
                               reply_markup=_promo_list_kb(items, page, admin),
                               disable_web_page_preview=True)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:view:"))
async def cb_promo_view(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    pid = cq.data.split(":")[-1]
    it = _promo_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True); return
    # для клиентов скрываем неактивное
    if (not _promo_is_active(it)) and (not is_admin(getattr(cq.from_user, "id", None))):
        await cq.answer("Акция недоступна.", show_alert=True); return
    await _send_promo_preview(cq.message, it, is_admin(getattr(cq.from_user, "id", None)))
    await cq.answer()


#акция создание
@router.callback_query(F.data == "promo:add")
async def promo_add(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    await state.set_state(PromoStates.waiting_promo_title)
    await cq.message.answer("Введите <b>название акции</b>:")
    await cq.answer()

@router.message(PromoStates.waiting_promo_title)
async def promo_add_title(m: Message, state: FSMContext):
    if not is_admin_event(m):
        await state.clear(); return
    title = (m.text or "").strip()
    if len(title) < 2:
        await m.answer("Слишком коротко. Введите название ещё раз."); return
    await state.update_data(title=title)
    await state.set_state(PromoStates.waiting_promo_text)
    await m.answer("Вставьте <b>текст акции</b>")

@router.message(PromoStates.waiting_promo_text)
async def promo_add_text(m: Message, state: FSMContext):
    if not is_admin_event(m):
        await state.clear(); return

    text = (m.html_text or m.text or "").strip()
    await state.update_data(text=text)

    await state.set_state(PromoStates.waiting_promo_media)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить файл", callback_data="promo:media:skip")],
    ])
    await m.answer(
        "Пришлите картинку (jpg/png/webp) или PDF.\n"
        "Если файла нет — нажмите «⏭ Пропустить файл».",
        reply_markup=kb
    )

# ✅ Пришло медиа (фото/файл) на шаге создания
@router.message(StateFilter(PromoStates.waiting_promo_media), F.photo | F.document)
async def promo_add_media_ok(m: Message, state: FSMContext):
    logger.info("promo:add_media: hit content_type=%s, state=%s", m.content_type, await state.get_state())
    if not is_admin_event(m):
        await state.clear(); return

    ext = (_guess_promo_ext(m) or "").lower()
    if not ext or (ext not in ALLOWED_PROMO_IMG and ext not in ALLOWED_PROMO_DOC):
        await m.answer("Нужна картинка (jpg/png/webp) или PDF, либо нажмите «⏭ Пропустить файл».")
        return

    file_id = m.photo[-1].file_id if m.photo else m.document.file_id
    await state.update_data(_media_file_id=file_id, _media_ext=ext)  # ⬅️ вот так

    await m.reply("✅ Файл принят.")
    await state.set_state(PromoStates.waiting_promo_dates_new)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Выбрать дату окончания", callback_data="promo:cal:open:new")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:main")],
    ])
    await m.answer(
        "Укажите даты.\n• Одна дата = до этой даты (включительно)\n• Диапазон: ДД.ММ.ГГГГ - ДД.ММ.ГГГГ",
        reply_markup=kb
    )

@router.message(PromoStates.waiting_promo_media, ~(F.photo | F.document))
async def promo_add_media_fallback(m: Message, state: FSMContext):
    await m.answer("Пришлите картинку (jpg/png/webp) или PDF, либо нажмите «⏭ Пропустить файл».")


@router.callback_query(F.data == "promo:media:skip")
async def promo_media_skip_cb(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    await state.set_state(PromoStates.waiting_promo_dates_new)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Выбрать дату окончания", callback_data="promo:cal:open:new")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:main")],
    ])
    await cq.message.answer(
        "Укажите даты.\n"
        "• Одна дата = до этой даты (включительно)\n"
        "• Диапазон: ДД.ММ.ГГГГ - ДД.ММ.ГГГГ\n"
        "Или нажмите «📅 Выбрать дату окончания».",
        reply_markup=kb
    )
    await cq.answer()

@router.message(PromoStates.waiting_promo_dates_new)
async def promo_dates_new_set(m: Message, state: FSMContext):
    s_iso, e_iso = parse_ru_date_range(m.text or "")
    if m.text and (not s_iso and not e_iso):
        await m.answer("Неверный формат. Пример: 21.10.2025 - 31.10.2025 или 21.10.2025.")
        return
    await _promo_finish_create(m, state, starts_at=s_iso, ends_at=e_iso)

#календарь акция
def _calendar_kb(year: int, month: int, mode: str) -> InlineKeyboardMarkup:
    """
    mode: 'new' | 'edit'
    """
    month = max(1, min(12, month))
    _cal.setfirstweekday(0)  # Monday
    weeks = _cal.monthcalendar(year, month)

    header = f"{_RU_MONTHS[month]} {year}"
    prev_y, prev_m = (year-1, 12) if month == 1 else (year, month-1)
    next_y, next_m = (year+1, 1)  if month == 12 else (year, month+1)

    rows = [[
        InlineKeyboardButton(text="«", callback_data=f"promo:cal:nav:{mode}:{prev_y}:{prev_m}"),
        InlineKeyboardButton(text=header, callback_data="promo:cal:noop"),
        InlineKeyboardButton(text="»", callback_data=f"promo:cal:nav:{mode}:{next_y}:{next_m}"),
    ], [InlineKeyboardButton(text=d, callback_data="promo:cal:noop") for d in _RU_DOW]]

    for w in weeks:
        btns = []
        for d in w:
            if d == 0:
                btns.append(InlineKeyboardButton(text=" ", callback_data="promo:cal:noop"))
            else:
                iso = f"{year:04d}-{month:02d}-{d:02d}"
                btns.append(InlineKeyboardButton(text=f"{d:02d}", callback_data=f"promo:cal:pick:{mode}:{iso}"))
        btns and rows.append(btns)

    rows.append([InlineKeyboardButton(text="Отмена", callback_data="promo:cal:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
@router.callback_query(F.data.startswith("promo:cal:open:"))
async def promo_cal_open(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    mode = cq.data.split(":")[-1]  # 'new' | 'edit'
    today = datetime.now(TZ).date()
    kb = _calendar_kb(today.year, today.month, mode)
    await cq.message.answer("Выберите <b>дату окончания</b> (включительно):", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:cal:nav:"))
async def promo_cal_nav(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    _, _, _, mode, y, m = cq.data.split(":")
    year = int(y); month = int(m)
    kb = _calendar_kb(year, month, mode)
    await cq.message.edit_reply_markup(reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:cal:pick:"))
async def promo_cal_pick(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    # формат: promo:cal:pick:<mode>:YYYY-MM-DD
    try:
        _, _, _, mode, iso = cq.data.split(":")
    except ValueError:
        await cq.answer("Неверные данные календаря", show_alert=True)
        return

    # ключевой момент: проверяем права по cq.from_user.id, а не по cq.message.from_user (это бот)
    actor_id = getattr(cq.from_user, "id", None)

    try:
        if mode == "new":
            # при создании: одна дата — это ДАТА ОКОНЧАНИЯ (включительно)
            await _promo_finish_create(
                cq.message, state,
                starts_at=None, ends_at=iso,
                actor_id=actor_id,
            )
        else:
            # при редактировании: меняем только окончание (start не трогаем)
            await _promo_apply_dates_edit(
                cq.message, state,
                starts_at=None, ends_at=iso,
                actor_id=actor_id,
            )

        # визуально закрываем календарь
        try:
            await cq.message.delete()
        except Exception:
            await cq.message.edit_reply_markup(reply_markup=None)

        await cq.answer("Дата установлена")
    except Exception as e:
        logger.exception("promo: cal pick failed: %s", e)
        await cq.answer(f"Ошибка: {e}", show_alert=True)

@router.callback_query(F.data == "promo:cal:cancel")
async def promo_cal_cancel(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    try:
        await cq.message.delete()
    finally:
        await cq.answer()

@router.callback_query(F.data == "promo:cal:noop")
async def promo_cal_noop(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    await cq.answer()


@router.callback_query(F.data.startswith("promo:rename:"))
async def promo_rename(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _promo_find(pid)
    if not it: await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(rename_id=pid)
    await state.set_state(PromoStates.waiting_promo_rename)
    await cq.message.answer(f"Текущее название: «{esc(it['title'])}».\nВведите новое:")
    await cq.answer()

@router.message(PromoStates.waiting_promo_rename)
async def promo_do_rename(m: Message, state: FSMContext):
    if not is_admin_event(m):
        await state.clear(); return
    pid = (await state.get_data()).get("rename_id")
    it = _promo_find(pid)
    if not it: await state.clear(); await m.answer("Элемент не найден."); return
    title = (m.text or "").strip()
    if len(title) < 2:
        await m.answer("Слишком коротко. Введите ещё раз."); return
    it["title"] = title; it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it); await state.clear()
    await m.answer("✅ Название обновлено.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.callback_query(F.data.startswith("promo:edittext:"))
async def promo_edit_text(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    if not _promo_find(pid): await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(edit_id=pid)
    await state.set_state(PromoStates.waiting_promo_edit_text)
    await cq.message.answer("Вставьте новый текст (HTML допустим):")
    await cq.answer()

@router.message(PromoStates.waiting_promo_edit_text)
async def promo_do_edit_text(m: Message, state: FSMContext):
    if not is_admin_event(m):
        await state.clear(); return
    data = await state.get_data(); pid = data.get("edit_id")
    it = _promo_find(pid)
    if not it: await state.clear(); await m.answer("Элемент не найден."); return
    it["text"] = (m.html_text or m.text or "").strip()
    it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it); await state.clear()
    await m.answer("✅ Текст обновлён.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

@router.callback_query(F.data.startswith("promo:replaceimg:"))
async def promo_replace_img(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    if not _promo_find(pid):
        await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(img_id=pid)
    await state.set_state(PromoStates.waiting_promo_replace_img)
    await cq.message.answer("Пришлите картинку как *фото* или как *файл* (JPG/PNG/WebP).", parse_mode="Markdown")
    await cq.answer()

# REPLACE IMG (узкий → фолбэк
@router.message(StateFilter(PromoStates.waiting_promo_replace_img), F.photo | F.document)
async def promo_replace_img_upload(m: Message, state: FSMContext):
    logger.info("promo:replace_img: hit content_type=%s, state=%s",
                m.content_type, await state.get_state())
    if not is_admin_event(m):
        await state.clear(); return

    data = await state.get_data()
    pid  = data.get("img_id")
    it   = _promo_find(pid)
    if not it:
        await state.clear(); await m.answer("Элемент не найден."); return

    ext = (_guess_promo_ext(m) or "").lower()
    if ext not in {"jpg", "png", "webp"}:
        await m.answer("Нужен файл: JPG/PNG/WebP. Пришлите как фото или как файл."); return

    name = f"{pid}.{ext}"
    dest = PROMO_DIR / name
    try:
        await _save_incoming_promo_file(m, dest)
    except Exception as e:
        await m.answer(f"Ошибка при сохранении файла: {esc(str(e))}"); return

    old = it.get("image")
    if old and old != name:
        (PROMO_DIR / old).unlink(missing_ok=True)

    it["image"] = name
    it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it)
    await state.clear()
    await m.answer("✅ Картинка обновлена.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    try:
        await _send_promo_preview(m, it, admin=True)
    except Exception:
        logger.exception("promo: preview send failed (pid=%s)", pid)

@router.message(PromoStates.waiting_promo_replace_img, ~(F.photo | F.document))
async def promo_replace_img_fallback(m: Message, state: FSMContext):
    await m.answer("Пришлите картинку как фото или как файл (jpg/png/webp).")

@router.callback_query(F.data.startswith("promo:dates:"))
async def promo_dates_start(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _promo_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(edit_id=pid)
    await state.set_state(PromoStates.waiting_promo_dates_edit)
    cur = " / ".join(filter(None, [_ru_from_iso(it.get("starts_at")), _ru_from_iso(it.get("ends_at"))])) or "не заданы"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Выбрать дату", callback_data="promo:cal:open:edit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="promo:view:"+pid)]
    ])
    await cq.message.answer(
        f"Текущие даты: {esc(cur)}\n"
        "Пришлите «ДД.ММ.ГГГГ» или «ДД.ММ.ГГГГ - ДД.ММ.ГГГГ».\n"
        "Одна дата = <b>до этой даты (включительно)</b>.\n"
        "Или нажмите «📅 Выбрать дату».",
        reply_markup=kb
    )
    await cq.answer()

@router.message(PromoStates.waiting_promo_dates_edit, Command("skip"))
async def promo_dates_clear(m: Message, state: FSMContext):
    if not is_admin_event(m):
        await state.clear(); return
    pid = (await state.get_data()).get("edit_id")
    it = _promo_find(pid)
    if not it:
        await state.clear(); await m.answer("Элемент не найден."); return
    # сохраняем старт, чистим только конец
    it["ends_at"] = None
    it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it); await state.clear()
    await m.answer("✅ Дата окончания очищена.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))

async def _promo_apply_dates_edit(
    m: Message, state: FSMContext,
    starts_at: Optional[str], ends_at: Optional[str],
    actor_id: Optional[int] = None,
):
    uid = actor_id if actor_id is not None else getattr(getattr(m, "from_user", None), "id", None)
    if not is_admin(uid):
        await state.clear(); return

    data = await state.get_data()
    pid = data.get("edit_id")
    it = _promo_find(pid)
    if not it:
        await state.clear()
        await m.answer("Элемент не найден.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None))); return

    if starts_at is not None:
        it["starts_at"] = starts_at
    if ends_at is not None:
        it["ends_at"] = ends_at

    it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it)
    await state.clear()

    period_human = " — ".join(filter(None, [_ru_from_iso(it.get("starts_at")), _ru_from_iso(it.get("ends_at"))])) or "без даты"
    await m.answer(f"✅ Даты обновлены: <i>{period_human}</i>.")
    await _send_promo_preview(m, it, is_admin(uid))

@router.message(PromoStates.waiting_promo_dates_edit)
async def promo_dates_edit_set(m: Message, state: FSMContext):
    s_iso, e_iso = parse_ru_date_range(m.text or "")
    if m.text and (not s_iso and not e_iso):
        await m.answer("Неверный формат. Пример: 21.10.2025 - 31.10.2025 или 21.10.2025.")
        return
    await _promo_apply_dates_edit(m, state, starts_at=s_iso, ends_at=e_iso)

@router.message(PromoStates.waiting_promo_dates_new, Command("skip"))
async def promo_dates_new_clear(m: Message, state: FSMContext):
    await _promo_finish_create(m, state, starts_at=None, ends_at=None)

@router.callback_query(F.data == "back:main")
async def cb_back_main(cq: CallbackQuery):
    # реюзим уже существующий обработчик возврата в меню
    await cb_back(cq)

@router.callback_query(F.data.startswith("promo:toggle:"))
async def promo_toggle(cq: CallbackQuery):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _promo_find(pid)
    if not it:
        await cq.answer("Не найдено", show_alert=True); return
    it["active"] = not bool(it.get("active", True))
    it["updated_at"] = datetime.now(TZ).isoformat()
    _promo_set(it)
    await cq.answer("Готово")
    # перерисуем превью с актуальным статусом
    await _send_promo_preview(cq.message, it, is_admin(getattr(cq.from_user, "id", None)))

@router.callback_query(F.data.startswith("promo:del:"))
async def promo_del(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    pid = cq.data.split(":")[-1]
    it = _promo_find(pid)
    if not it: await cq.answer("Не найдено", show_alert=True); return
    await state.update_data(del_id=pid)
    await state.set_state(PromoStates.waiting_promo_delete_confirm)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data="promo:confirm_del:yes"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="promo:confirm_del:no"),
    ]])
    await cq.message.answer(f"Удалить «{esc(it['title'])}»?", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:confirm_del:"))
async def promo_del_confirm(cq: CallbackQuery, state: FSMContext):
    if not await ensure_callback_access(cq, "promos.view"):
        return
    if not is_admin_event(cq):
        await cq.answer("Только для админов", show_alert=True); return
    action = cq.data.split(":")[-1]
    pid = (await state.get_data()).get("del_id")
    await state.clear()
    if action == "no":
        await cq.message.answer("❎ Отменено."); await cq.answer(); return
    it = _promo_find(pid)
    if it:
        # чистим файлы
        for key in ("image","doc"):
            name = it.get(key)
            if name: (PROMO_DIR / name).unlink(missing_ok=True)
        _promo_delete(pid)
        await cq.message.answer(f"✅ Акция «{esc(it['title'])}» удалена.")
    else:
        await cq.message.answer("⚠️ Элемент не найден.")
    await cq.answer()

def _promo_cleanup_expired(now: Optional[datetime] = None) -> int:
    """
    Удаляет просроченные акции (end_date ИНКЛЮЗИВНО).
    Если сегодня > end_date → акция удаляется из индекса, файлы (image/pdf) — с диска.
    Возвращает число удалённых.
    """
    today = (now or datetime.now(TZ)).date()
    items = _promos_load()
    keep: List[Dict[str, Any]] = []
    removed = 0
    for it in items:
        e_iso = it.get("ends_at")
        if e_iso:
            try:
                end_d = datetime.fromisoformat(e_iso).date()
                if today > end_d:
                    # устарела — удаляем
                    for key in ("image", "doc"):
                        name = it.get(key)
                        if name:
                            (PROMO_DIR / name).unlink(missing_ok=True)
                    removed += 1
                    continue
            except Exception:
                pass
        keep.append(it)
    if removed:
        _promos_save(keep)
    return removed

# Пример глобального ловца медиа — добавь StateFilter(None)!
@router.message(StateFilter(None), F.photo | F.document)
async def block_misc_uploads(m: Message):
    await m.answer("Сейчас это не сюда 🙂")

#акции конец

#Команда бакалар генерация картинки
# --- BAKALAR: напоминалка по этикеткам ---
BAKALAR_IMG_CANDIDATES = (
    "bakalar.png",
    "bakalar.jpg",
    "bakalar.jpeg",
    "bakalar.webp",
)

def find_bakalar_image() -> Path | None:
    for name in BAKALAR_IMG_CANDIDATES:
        p = ROOT_DIR / name
        if p.exists():
            return p
    return None

@router.message(Command("bakalar"))
async def cmd_bakalar(m: Message):
    p = find_bakalar_image()
    if not p:
        await m.answer(
            "Не нашёл картинку напоминалки.\n"
            "В корне. Не найден bakalar.png (или .jpg/.jpeg/.webp)."
        )
        return

    await m.answer_photo(
        FSInputFile(p),
        caption=(
            "<b>Bakalar — напоминалка по этикеткам</b>\n"
            "• Бакалар Оригинальное Светлое — <b>красная</b> этикетка\n"
            "• Бакалар Оригинальное Лагер — <b>зелёная</b> этикетка\n"
            "• Бакалар XO — <b>белая</b> этикетка"
        ),
    )

# --- Fallback для неизвестных callback'ов ---
@router.callback_query()
async def fallback_cb(cq: CallbackQuery):
    await cq.answer()


# --- Старт бота ---
async def run_bot():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    asyncio.create_task(daily_fetch_worker())
    await dp.start_polling(bot)
