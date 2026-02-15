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
import aiohttp, asyncio, time,os, re, json
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
from aiogram import F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import StatesGroup, State
from config import BOT_TOKEN, update_setting
from file_processor import process_file, find_latest_download, process_tara_file, find_latest_downloads
from mail_agent import fetch_latest_file
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram import BaseMiddleware
from typing import Optional, Tuple, Dict, Any


ROOT_DIR = Path(__file__).resolve().parent
SETTINGS_DIR = ROOT_DIR / "settings"

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

class PhoneStates(StatesGroup):
    waiting_phone = State()

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
            "view_reports",
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
    key = (role or "client").strip().lower()
    return _ROLE_DEFS.get(key) or _ROLE_DEFS.get("client", {})

def get_role_permissions(role: Optional[str]) -> set:
    return set(get_role_def(role).get("permissions") or [])

def normalize_role(role: Optional[str]) -> str:
    key = (role or "client").strip().lower()
    if key in _ROLE_DEFS:
        return key
    return "client"

def role_label(role: Optional[str]) -> str:
    return str(get_role_def(role).get("label") or role or "client")

def user_has_permission(user_id: Optional[int], permission: str) -> bool:
    return permission in get_role_permissions(get_user_role(user_id))

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
            data[k] = {"role": "client", "name": str(v)}
        else:
            v["role"] = normalize_role(v.get("role"))
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
    name = re.sub(r"^(ооо|ип)\.?\s+", "", name, flags=re.IGNORECASE)
    name = name.strip()
    if name:
        name = re.sub(r'^[«"“”„\']+|[»"“”„\']+$', "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name

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

def build_whatsapp_debt_text(item: Dict[str, Any], report_date: Optional[str]) -> str:
    """
    Текст для WhatsApp: собираем номера и даты всех документов с положительной суммой.
    Общая сумма берётся из total_amount, если она пустая — суммируем строки > 0.
    """
    client = _base_client_name_for_debt(item.get("client") or "")
    docs = item.get("docs") or []

    pos_docs: List[Dict[str, Any]] = []
    total = float(item.get("total_amount") or 0.0)

    if total <= 0.009:
        total = 0.0

    for d in docs:
        amt = float(d.get("amount") or 0.0)
        if amt > 0.009:
            pos_docs.append(d)
            if total <= 0.009:
                total += amt

    # если задолженности нет — текст пустой
    if not pos_docs or total <= 0.009:
        return ""

    parts: List[str] = []
    for d in pos_docs:
        nums = ", ".join(d.get("doc_numbers") or []) or "—"
        date = d.get("doc_date") or "—"
        parts.append(f"{nums} от {date}")

    docs_txt = "; ".join(parts)
    sum_txt = fmt_money(total).replace("\u00A0", " ")  # без NBSP

    intro = f"Добрый день! "
    body  = f"У вас имеется задолженность по фактуре(ам) {docs_txt} на общую сумму {sum_txt}."
    tail  = f" (по состоянию на {report_date})" if report_date else ""

    msg = (intro + body + tail).strip()
    return f"{msg}\n\nКогда ожидать оплату?"



# временная карта callback key -> base name (на период жизни процесса)
_CB_CLIENT_MAP: Dict[str, str] = {}

def client_card_kb(item: Dict[str, Any], report_date: Optional[str]) -> Optional[InlineKeyboardMarkup]:
    total = float(item.get("total_amount") or 0.0)
    has_debt = total > 0.009
    base = _base_client_name_for_debt(item.get("client") or "")
    key  = client_key(item.get("client") or "")
    _CB_CLIENT_MAP[key] = base  # запомним

    phone = get_client_phone(item.get("client") or "")
    buttons = []

    if has_debt and phone:
        text = build_whatsapp_debt_text(item, report_date)
        if text:
            wa_phone = wa_number_from_e164(phone)  # 7XXXXXXXXXX
            url = f"https://wa.me/{wa_phone}?text={quote_plus(text)}"
            buttons.append([InlineKeyboardButton(text="💬 WhatsApp", url=url)])

    if phone:
        buttons.append([InlineKeyboardButton(text="📞 Изменить телефон", callback_data=f"ph:edit:{key}")])
    else:
        buttons.append([InlineKeyboardButton(text="📞 Добавить телефон", callback_data=f"ph:add:{key}")])

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

@router.callback_query(F.data.startswith("ph:add:"))
async def cb_phone_add(c: CallbackQuery, state: FSMContext):
    _, _, key = c.data.partition("ph:add:")
    base = _CB_CLIENT_MAP.get(key)
    if not base:
        await c.message.answer("Не удалось определить клиента. Повторите из отчёта.")
        return
    await state.update_data(phone_client_base=base)
    await state.set_state(PhoneStates.waiting_phone)
    await c.message.answer(
        f"Введите телефон клиента «{base}» в формате: +7 999 999 99 99 или 8XXXXXXXXXX — преобразую в +7.",
        reply_markup=back_only_kb()
    )
    await c.answer()

@router.callback_query(F.data.startswith("ph:edit:"))
async def cb_phone_edit(c: CallbackQuery, state: FSMContext):
    _, _, key = c.data.partition("ph:edit:")
    base = _CB_CLIENT_MAP.get(key)
    if not base:
        await c.message.answer("Не удалось определить клиента. Повторите из отчёта.")
        return
    cur = get_client_phone(base) or "не указан"
    await state.update_data(phone_client_base=base)
    await state.set_state(PhoneStates.waiting_phone)
    await c.message.answer(
        f"Текущий телефон: {cur}\nВведите новый телефон в формате +7 999 999 99 99 или 8XXXXXXXXXX:",
        reply_markup=back_only_kb()
    )
    await c.answer()

@router.message(PhoneStates.waiting_phone)
async def on_phone_input(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    ok, e164, disp = normalize_phone_ru(raw)
    data = await state.get_data()
    base = data.get("phone_client_base")
    await state.clear()

    if not base:
        await m.answer("Не удалось определить клиента. Повторите из отчёта.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return

    if not ok:
        await m.answer("Неверный номер. Пример: +7 999 123-45-67 или 8XXXXXXXXXX.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return

    set_client_phone(base, e164)
    await m.answer(f"Телефон для «{base}» сохранён: {disp}", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))



# --- Группировка тары по клиенту и адресам ---
_TARA_PARENS_RE = re.compile(r"\(([^)]*)\)")

def _strip_rep(full: str) -> str:
    """Убираем суффикс торгового представителя ' - Колягин'."""
    if not full:
        return ""
    return full.replace(" - Колягин", "").replace("- Колягин", "")

def _tara_base_name(full: str) -> str:
    """Базовое имя клиента без адреса и без суффикса представителя."""
    if not full:
        return ""
    s = _strip_rep(full)
    s = _TARA_PARENS_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip(" \u00A0-")
    return s

def _tara_address(full: str) -> str:
    """Адрес из круглых скобок. Если его нет — возвращаем пустую строку."""
    if not full:
        return ""
    m = _TARA_PARENS_RE.search(full)
    return (m.group(1) or "").strip() if m else ""

def build_tara_group_text(base_name: str, entries: list) -> str:
    """Форматирование блока по одному клиенту с адресами и позициями."""
    total_all = sum(float(e.get("total", 0) or 0) for e in entries)

    def _key_addr(e):
        a = _tara_address(e.get("client") or "")
        return a.casefold().replace("ё", "е")

    entries_sorted = sorted(entries, key=_key_addr)

    lines = [f"<b>{esc(base_name)}</b> — всего: {fmt_qty_units(total_all)}"]
    for e in entries_sorted:
        addr = _tara_address(e.get("client") or "")
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
def help_text_admin() -> str:
    return (
        "<b>BeerMarket🍺 — справка (админ)</b>\n\n"
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
        "📞 <b>Телефон в карточке</b>:\n"
        "— «📞 Добавить/Изменить телефон» принимает номер текстом (+7/8) <u>или карточку контакта</u>.\n"
        "— При долге появится «💬 WhatsApp» с готовым текстом напоминания.\n\n"
        "🧰 <b>Команды</b>:\n"
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




def help_text_client(current_name: str) -> str:
    hint = f'Текущее название: <b>«{esc(current_name)}»</b>' if current_name else "<b>Название не задано.</b>"
    return (
        f"Здравствуйте! Режим клиента. {hint}\n\n"
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
        "• 📦 <b>Отчёт по таре</b> — по средам в <b>12:00</b> (еженедельно).\n\n\n"
        "• ✉️ <a href='https://t.me/Re1ze_r'>Написать администратору в Telegram</a>\n"
    )

def help_text_sales_rep() -> str:
    return (
        "<b>BeerMarket🍺 — справка (торговый представитель)</b>\n\n"
        "📌 <b>Кнопки</b>:\n"
        "• 🔎 <b>Поиск</b> — поиск по части названия/адреса\n"
        "• 🔎 <b>Поиск тары</b> — поиск по ведомости тары\n"
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
        return "client"
    uid = str(user_id)
    if _ADMIN_IDS and user_id in _ADMIN_IDS:
        return "admin"
    rec = (_USER_ROLES.get(uid) or {})
    return normalize_role(rec.get("role") or "client")

def _user_record(user_id: Optional[int]) -> Dict[str, Any]:
    if not user_id:
        return {}
    return (_roles_load().get(str(user_id)) or {})

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
        cur = {"role": "client", "name": str(cur)}
    patch = dict(patch or {})
    if "role" in patch:
        patch["role"] = normalize_role(patch.get("role"))
    cur.update(patch)
    _roles_merge_and_save({uid: cur})

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

_CLIENT_OD_MAP = _load_overdue_map()

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
    last_dt, _ = get_last_update()
    upd_label = "🔄 Обновить"
    hhmm = fmt_hhmm(last_dt)
    if hhmm:
        upd_label = f"{upd_label} ({hhmm})"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Поиск"), KeyboardButton(text="🔎 Поиск тары")],
            [KeyboardButton(text="🧾 Общий отчёт"),KeyboardButton(text=TARE_BTN)],
            [KeyboardButton(text="⏰ Просрочено"),KeyboardButton(text="💰 Переплаты")],
            [KeyboardButton(text="📑 Прайсы"),KeyboardButton(text="🎁 Акции")],
            [KeyboardButton(text=SCHEDULE_BTN), KeyboardButton(text=TTN_BTN)],
            [KeyboardButton(text="⚙️ Отсрочки"), KeyboardButton(text="⚙️ Фильтры")],
            [KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="▶️ Старт"), KeyboardButton(text=upd_label)],
        ],
        resize_keyboard=True
    )

def sales_rep_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    """Клавиатура торгового представителя: поиск, прайсы, акции, график, ТТН."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Поиск"), KeyboardButton(text="🔎 Поиск тары")],
            [KeyboardButton(text="⏰ Просрочено"), KeyboardButton(text="💰 Переплаты")],
            [KeyboardButton(text="📑 Прайсы"), KeyboardButton(text="🎁 Акции")],
            [KeyboardButton(text=SCHEDULE_BTN), KeyboardButton(text=TTN_BTN)],
            [KeyboardButton(text="⚙️ Отсрочки"), KeyboardButton(text="⚙️ Фильтры")],
            [KeyboardButton(text="▶️ Старт")],
        ],
        resize_keyboard=True
    )

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
def client_menu_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    """Клавиатура клиента: обновление, смена названия, поиск + старт."""
    last_dt, _ = get_last_update()
    upd_label = "🔄 Обновить"
    hhmm = fmt_hhmm(last_dt)
    if hhmm:
        upd_label = f"{upd_label} ({hhmm})"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Поиск"), KeyboardButton(text="🔎 Поиск тары")],
            [KeyboardButton(text="📑 Прайсы"), KeyboardButton(text="🎁 Акции")],
            [KeyboardButton(text=SCHEDULE_BTN)],
            [KeyboardButton(text="▶️ Старт")],
            [KeyboardButton(text="✏️ Изменить название")],
        ],
        resize_keyboard=True
    )

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
            v = {"role": "client", "name": str(v)}
        items.append((k, v))
    items.sort(key=_user_sort_key)

    total = len(items)
    page = max(0, page)
    start = page * page_size
    end = min(total, start + page_size)
    rows: List[List[InlineKeyboardButton]] = []
    for uid, rec in items[start:end]:
        name = (rec.get("name") or "unknown").strip()
        role = normalize_role(rec.get("role") or "client")
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

def user_detail_kb(uid: str, page: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Сделать админом", callback_data=f"usr:setrole:{uid}:admin"),
            InlineKeyboardButton(text="👤 Сделать клиентом", callback_data=f"usr:setrole:{uid}:client"),
        ],
        [
            InlineKeyboardButton(text="🧑‍💼 Сделать торговым представителем", callback_data=f"usr:setrole:{uid}:sales_rep"),
            InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data=f"usr:del:{uid}:{page}"),
        ],
        [
            InlineKeyboardButton(text="✏️ Изменить имя", callback_data=f"usr:editname:{uid}"),
            InlineKeyboardButton(text="📞 Изменить телефон", callback_data=f"usr:editphone:{uid}"),
        ],
        [
            InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"usr:block:{uid}"),
            InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"usr:unblock:{uid}"),
        ],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data=f"usr:list:{page}")],
    ])



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

    # заголовок карточки (добавили badge клиента)
    head = f"<b>{idx:02d}. {esc(item['client'])}</b>\n"
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
                f"от {esc(doc_date_str)}\tCумма <b>{fmt_money(d['__amt'])}</b> ₽\t|\tДней <b>{days_txt}</b>\t |\t"
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
    name = (item.get("client") or "").casefold()
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
    if role == "sales_rep":
        return sales_rep_menu_kb(user_id)
    return client_menu_kb(user_id)

def menu_for_message(msg: Message) -> ReplyKeyboardMarkup:
    return menu_for_user_id(getattr(msg.from_user, "id", None))

def menu_for_user_id(user_id: Optional[int]) -> ReplyKeyboardMarkup:
    return menu_for_role(get_user_role(user_id), user_id=user_id)

def menu_for_callback(cq: CallbackQuery) -> ReplyKeyboardMarkup:
    return menu_for_user_id(getattr(cq.from_user, "id", None))

def client_name_prompt_text() -> str:
    return (
        "Введите название вашей организации (без «ИП»/«ООО»), например: "
        "<code>себекин</code> или <code>большая рыба</code>."
    )

async def _continue_after_phone(m: Message, state: FSMContext) -> None:
    update_user_profile_from_message(m)
    uid = getattr(m.from_user, "id", None)
    key = str(uid) if uid is not None else None
    data = _roles_load()
    rec = (data.get(key) if key else {}) or {}
    role = (rec.get("role") or "").strip().lower()

    if not role:
        await state.set_state(OnboardStates.waiting_role)
        await m.answer("Выберите роль:", reply_markup=onboard_role_kb())
        return

    if role == "admin":
        await m.answer(help_text_admin(), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = rec.get("name") or get_client_name(uid)
    if not cname:
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text())
        return
    await m.answer(help_text_client(cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))

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
    role = (rec.get("role") or "").strip().lower()
    if rec.get("blocked"):
        await m.answer("Ваш доступ заблокирован. Обратитесь к администратору.")
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
    if not role:
        await state.set_state(OnboardStates.waiting_role)
        await m.answer("Выберите роль:", reply_markup=onboard_role_kb())
        return

    # Известная роль — показываем соответствующее меню.
    if role == "admin":
        await m.answer(help_text_admin(), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = rec.get("name") or get_client_name(uid)
    if not cname:
        await state.set_state(OnboardStates.waiting_client_name)
        await m.answer(client_name_prompt_text())
        return
    await m.answer(help_text_client(cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))


@router.message(Command("help"))
async def on_help(m: Message):
    if is_user_blocked(getattr(m.from_user, "id", None)):
        await m.answer("Ваш доступ заблокирован. Обратитесь к администратору.")
        return
    update_user_profile_from_message(m)
    role = get_user_role(getattr(m.from_user, "id", None))
    if role == "admin":
        await m.answer(help_text_admin(), reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
        return
    if role == "sales_rep":
        await m.answer(help_text_sales_rep(), reply_markup=sales_rep_menu_kb(getattr(m.from_user, "id", None)))
        return
    cname = get_client_name(getattr(m.from_user, "id", None))
    await m.answer(help_text_client(cname), reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))


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
    await state.clear()
    await _continue_after_phone(m, state)

@router.message(OnboardStates.waiting_phone_contact)
async def ob_phone_contact_text(m: Message, state: FSMContext):
    ok, e164, disp = normalize_phone_ru(m.text or "")
    if not ok:
        await m.answer("Нужно отправить контакт кнопкой или введите номер в формате +7XXXXXXXXXX.")
        return
    set_user_phone(m.from_user.id, e164, verified=False)
    await m.answer(f"✅ Номер сохранён: {disp}", reply_markup=ReplyKeyboardRemove())
    await state.clear()
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
    name = normalize_client_name(raw_name)
    if not name or len(name) < 2:
        await m.answer("Введите корректное название (минимум 2 символа).")
        return

    # сохраняем роль и имя клиента
    set_user_role(m.from_user.id, "client")
    set_client_name(m.from_user.id, name)

    await state.clear()

    # Сообщение + клиентское меню
    await m.answer(
        f"✅ Сохранено: «{esc(name)}». Режим клиента активирован.",
        reply_markup=client_menu_kb(getattr(m.from_user, "id", None))
    )

    # Автоматически показать стартовый экран/хелп клиента
    await on_start(m, state)

##---------------Обработчики сообщений/колбэков “Прайсы”-------------------
# Кнопка в меню
@router.message(F.text == "📑 Прайсы", StateFilter(None))
async def btn_prices(m: Message):
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
    if _is_client(m):
        await m.answer("Доступно только для админов.", reply_markup=menu_for_message(m))
        return
    await render_report(m, mode="all", keywords=[], min_debt=None)

@router.message(F.text == TARE_BTN)
async def btn_tara(m: Message):
    if _is_client(m):
        await m.answer("Доступно только для админов.", reply_markup=menu_for_message(m))
        return
    await render_tara_report(m)

@router.message(F.text == TTN_BTN)
async def btn_ttn(m: Message, state: FSMContext):
    _cleanup_flows()
    logger.info("ttn: entry by user=%s role=%s", getattr(m.from_user, "id", None), get_user_role(getattr(m.from_user, "id", None)))
    await state.set_state(TTNStates.waiting_number)
    await m.answer(
        "Введите номер ТТN.",
        reply_markup=back_only_kb()
    )

@router.message(F.text.func(lambda t: _has(t, "просрочено") or (t or "").startswith("⏰")))
async def btn_overdue(m: Message):
    if _is_client_only(m):
        await m.answer("Доступно только для админов или торговых.", reply_markup=menu_for_message(m))
        return
    await render_report(m, mode="overdue", keywords=[], min_debt=None)

@router.message(F.text.func(lambda t: _has(t, "переплат") or (t or "").startswith("💰")))
async def btn_overpaid(m: Message):
    if _is_client_only(m):
        await m.answer("Доступно только для админов или торговых.", reply_markup=menu_for_message(m))
        return
    await render_report(m, mode="overpaid", keywords=[], min_debt=None)

@router.message(F.text == "🔎 Поиск")
async def btn_search(m: Message, state: FSMContext):
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
                name = (b.get("client") or "").strip().casefold()
                if not kws:
                    return False
                return any(k in name for k in kws)

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
    if not name or len(name) < 2:
        await m.answer("Введите корректное название (минимум 2 символа).", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))
        return
    set_client_name(m.from_user.id, name)
    await state.clear()
    await m.answer(f"✅ Обновлено. Название: «{esc(name)}».", reply_markup=client_menu_kb(getattr(m.from_user, "id", None)))

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
    await m.answer("Что обновить?", reply_markup=update_menu_kb())


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
    await state.set_state(TTNStates.waiting_number)
    await m.answer("Введите номер(а) ТТН.\nМожно несколько через пробел или с новой строки.", reply_markup=back_only_kb())

@router.message(TTNStates.waiting_number, F.text)
async def ttn_step_number(m: Message, state: FSMContext):
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
        await state.clear()
        await m.answer(f"Отсрочка для «{esc(client)}» <b>сброшена</b> до общего значения.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    else:
        _CLIENT_OD_MAP[key] = days
        _save_overdue_map(_CLIENT_OD_MAP)
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
        await m.answer(f"Удалено правило: <code>{esc(key)}</code>", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    else:
        await m.answer("Такого ключа нет в списке.", reply_markup=main_menu_kb(getattr(m.from_user, "id", None)))
    await state.clear()

# --- Команды ---
@router.message(Command("report"))
async def on_report(m: Message):
    if _is_client(m):
        await m.answer("Команда доступна только для админов.", reply_markup=menu_for_message(m))
        return
    mode, keywords, min_override = parse_report_args(m.text or "")
    await render_report(m, mode=mode, keywords=keywords, min_debt=min_override)


@router.message(Command("refresh"))
async def cmd_refresh(m: Message):
    if _is_client(m):
        await m.answer("Команда доступна только для админов.", reply_markup=menu_for_message(m))
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
    if _is_client(m):
        await m.answer("Команда доступна только для админов.", reply_markup=menu_for_message(m))
        return
    await render_tara_report(m)

async def _refresh_and_reply_cb(cq: CallbackQuery, mail_type: str):
    await cq.message.edit_text("Обновляю отчёт из почты…")
    try:
        path = fetch_latest_file(mail_type)  # 'ДЕБИТОРКА' или 'ТАРА'
        if path:
            set_last_update("manual")
            kb = menu_for_callback(cq)
            await cq.message.answer(f"Готово. Файл: <code>{esc(path)}</code>", reply_markup=kb)
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
    if _is_client(m):
        await m.answer("Команда доступна только для админов.", reply_markup=menu_for_message(m))
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
    if not is_admin(getattr(m.from_user, "id", None)):
        await m.answer("Команда доступна только для админов.", reply_markup=menu_for_message(m))
        return
    await m.answer("Список пользователей:", reply_markup=users_list_kb())

@router.callback_query(F.data.startswith("usr:list:"))
async def admin_users_list_page(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        page = int(cq.data.split(":")[2])
    except Exception:
        page = 0
    await cq.message.edit_text("Список пользователей:", reply_markup=users_list_kb(page=page))
    await cq.answer()

@router.callback_query(F.data.startswith("usr:sel:"))
async def admin_users_select(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    data = _roles_load()
    rec = data.get(uid, {}) if uid else {}
    name = (rec.get("name") or "unknown").strip()
    role = normalize_role(rec.get("role") or "client")
    phone = (rec.get("phone") or "—").strip()
    verified = "✅" if rec.get("phone_verified") else "❌"
    blocked = "⛔" if rec.get("blocked") else "✅"
    text = (
        f"<b>Пользователь</b>\n"
        f"ID: <code>{esc(uid)}</code>\n"
        f"Роль: <b>{esc(role)}</b>\n"
        f"Имя: <b>{esc(name)}</b>\n"
        f"Телефон: <b>{esc(phone)}</b> ({verified})\n"
        f"Доступ: {blocked}"
    )
    await cq.message.edit_text(text, reply_markup=user_detail_kb(uid, page=page))
    await cq.answer()

@router.callback_query(F.data.startswith("usr:setrole:"))
async def admin_users_set_role(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    parts = cq.data.split(":")
    uid = parts[2] if len(parts) > 2 else ""
    role = parts[3] if len(parts) > 3 else "client"
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    update_user_record(uid, {"role": normalize_role(role)})
    await cq.answer("Роль обновлена.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:block:"))
async def admin_users_block(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    uid = cq.data.split(":")[2] if len(cq.data.split(":")) > 2 else ""
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    update_user_record(uid, {"blocked": True})
    await cq.answer("Пользователь заблокирован.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:unblock:"))
async def admin_users_unblock(cq: CallbackQuery):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    uid = cq.data.split(":")[2] if len(cq.data.split(":")) > 2 else ""
    if not uid:
        await cq.answer("Пользователь не найден.", show_alert=True)
        return
    update_user_record(uid, {"blocked": False})
    await cq.answer("Пользователь разблокирован.")
    await admin_users_select(cq)

@router.callback_query(F.data.startswith("usr:del:"))
async def admin_users_delete(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
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
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
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
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    uid = cq.data.split(":")[2]
    await state.update_data(admin_edit_uid=uid)
    await state.set_state(AdminUserEditStates.waiting_name)
    await cq.message.answer("Введите новое имя пользователя:")
    await cq.answer()

@router.callback_query(F.data.startswith("usr:editphone:"))
async def admin_users_edit_phone(cq: CallbackQuery, state: FSMContext):
    if not is_admin(getattr(cq.from_user, "id", None)):
        await cq.answer("Недостаточно прав.", show_alert=True)
        return
    uid = cq.data.split(":")[2]
    await state.update_data(admin_edit_uid=uid)
    await state.set_state(AdminUserEditStates.waiting_phone)
    await cq.message.answer("Введите новый телефон (например, +7XXXXXXXXXX):")
    await cq.answer()

@router.message(AdminUserEditStates.waiting_name)
async def admin_users_save_name(m: Message, state: FSMContext):
    if not is_admin(getattr(m.from_user, "id", None)):
        await m.answer("Недостаточно прав.", reply_markup=menu_for_message(m))
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
    if not is_admin(getattr(m.from_user, "id", None)):
        await m.answer("Недостаточно прав.", reply_markup=menu_for_message(m))
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
        name = (it.get("client") or "").casefold()
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

def _read_news_index(path: Path) -> List[Dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        logger.exception("news: failed to parse %s", path)
        return []

def _news_load() -> List[Dict[str, Any]]:
    if not NEWS_INDEX.exists():
        return []
    items = _read_news_index(NEWS_INDEX)
    if isinstance(items, list):
        normalized = _news_normalize_items([dict(it) for it in items])
        if normalized != items:
            _news_save(normalized)
        return normalized
    return []


def _news_normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_iso = datetime.now(TZ).isoformat()
    out: List[Dict[str, Any]] = []
    for idx, it in enumerate(items, 1):
        row = dict(it)
        try:
            row_id = int(row.get("id"))
        except (TypeError, ValueError):
            row_id = int(time.time() * 1000) + idx

        publish_state = (row.get("publishState") or "published").strip().lower()
        if publish_state not in {"draft", "published"}:
            publish_state = "published"

        created_at = row.get("createdAt") or row.get("updatedAt") or now_iso
        updated_at = row.get("updatedAt") or created_at

        out.append({
            "id": row_id,
            "seq": idx,
            "title": (row.get("title") or "").strip(),
            "category": (row.get("category") or "Новость").strip() or "Новость",
            "date": _normalize_news_date(row.get("date")) or datetime.now(TZ).date().isoformat(),
            "text": (row.get("text") or "").strip(),
            "publishState": publish_state,
            "createdAt": created_at,
            "updatedAt": updated_at,
        })
    return out

def _news_reindex(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _news_normalize_items(items)

def _news_save(items: List[Dict[str, Any]]) -> None:
    normalized = _news_reindex(list(items))
    payload = json.dumps(normalized, ensure_ascii=False, indent=2)

    try:
        NEWS_INDEX.parent.mkdir(parents=True, exist_ok=True)
        tmp = NEWS_INDEX.with_suffix(NEWS_INDEX.suffix + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, NEWS_INDEX)
    except Exception:
        logger.exception("news: failed to write %s", NEWS_INDEX)


def _news_next_seq(items: List[Dict[str, Any]]) -> int:
    seqs = []
    for it in items:
        try:
            seqs.append(int(it.get("seq")))
        except (TypeError, ValueError):
            continue
    return max(seqs) if seqs else 0

def _news_find(news_id: str | int) -> Optional[Dict[str, Any]]:
    for it in _news_load():
        if str(it.get("id")) == str(news_id):
            return it
    return None

def _news_upsert(item: Dict[str, Any]) -> None:
    items = _news_load()
    for i, existing in enumerate(items):
        if str(existing.get("id")) == str(item.get("id")):
            if not item.get("createdAt") and existing.get("createdAt"):
                item["createdAt"] = existing.get("createdAt")
            if not item.get("seq") and existing.get("seq"):
                item["seq"] = existing.get("seq")
            items[i] = item
            _news_save(items)
            return
    if not item.get("seq"):
        item["seq"] = _news_next_seq(items) + 1
    items.insert(0, item)
    _news_save(items)


def _news_delete(news_id: str | int) -> bool:
    items = _news_load()
    before = len(items)
    items = [it for it in items if str(it.get("id")) != str(news_id)]
    if len(items) == before:
        return False
    _news_save(items)
    return True

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
    admin = is_admin(getattr(m.from_user, "id", None))
    items = _promo_get_all(include_inactive=admin)  # админ видит всё
    await m.answer("<b>Акции</b>\nВыберите пункт:",
                   reply_markup=_promo_list_kb(items, page=0, admin=admin))

#пагинация
@router.callback_query(F.data.startswith("promo:list:"))
async def cb_promos_list(cq: CallbackQuery):
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
    mode = cq.data.split(":")[-1]  # 'new' | 'edit'
    today = datetime.now(TZ).date()
    kb = _calendar_kb(today.year, today.month, mode)
    await cq.message.answer("Выберите <b>дату окончания</b> (включительно):", reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:cal:nav:"))
async def promo_cal_nav(cq: CallbackQuery):
    _, _, _, mode, y, m = cq.data.split(":")
    year = int(y); month = int(m)
    kb = _calendar_kb(year, month, mode)
    await cq.message.edit_reply_markup(reply_markup=kb)
    await cq.answer()

@router.callback_query(F.data.startswith("promo:cal:pick:"))
async def promo_cal_pick(cq: CallbackQuery, state: FSMContext):
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
    try:
        await cq.message.delete()
    finally:
        await cq.answer()

@router.callback_query(F.data == "promo:cal:noop")
async def promo_cal_noop(cq: CallbackQuery):
    await cq.answer()


@router.callback_query(F.data.startswith("promo:rename:"))
async def promo_rename(cq: CallbackQuery, state: FSMContext):
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
