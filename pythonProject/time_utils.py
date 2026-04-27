from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo


# Единая локальная таймзона приложения (по умолчанию — Россия, Новосибирск).
# Может быть переопределена через переменную окружения APP_TIMEZONE.
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Novosibirsk")
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)
NOVOSIBIRSK_LABEL = "UTC +07-00"
UNIFIED_RF_DT_FORMAT = "%H-%M-%S %d-%m-%Y"
_LEGACY_PARSE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    UNIFIED_RF_DT_FORMAT,
)

def utc_now() -> datetime:
    """Timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def local_now() -> datetime:
    """Timezone-aware локальное время приложения."""
    return datetime.now(LOCAL_TZ)

def local_now_iso(*, trim_microseconds: bool = True) -> str:
    """ISO-8601 локального времени приложения (с timezone offset)."""
    dt = local_now()
    if trim_microseconds:
        dt = dt.replace(microsecond=0)
    return dt.isoformat()

def utc_now_naive() -> datetime:
    """Naive UTC datetime for legacy code that compares naive timestamps."""
    return utc_now().replace(tzinfo=None)


def utc_now_iso() -> str:
    """ISO-8601 UTC with offset (+00:00)."""
    return utc_now().isoformat()


def utc_now_iso_z(*, trim_microseconds: bool = True) -> str:
    """ISO-8601 UTC with trailing Z."""
    dt = utc_now()
    if trim_microseconds:
        dt = dt.replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_iso_utc_naive(value: Optional[str]) -> Optional[datetime]:
    dt = parse_iso_datetime(value)
    return dt.replace(tzinfo=None) if dt else None


def parse_mixed_datetime(value: Optional[str], *, default_tz: ZoneInfo = LOCAL_TZ) -> Optional[datetime]:
    """
    Парсит смешанные legacy-форматы времени и возвращает timezone-aware datetime
    в локальной таймзоне приложения (по умолчанию Asia/Novosibirsk).
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    zone_suffix = f" {NOVOSIBIRSK_LABEL}"
    if raw.endswith(zone_suffix):
        raw = raw[: -len(zone_suffix)].strip()

    iso_candidate = raw
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=default_tz)
        return parsed.astimezone(default_tz)
    except ValueError:
        pass

    for pattern in _LEGACY_PARSE_FORMATS:
        try:
            parsed = datetime.strptime(raw, pattern)
            return parsed.replace(tzinfo=default_tz)
        except ValueError:
            continue
    return None


def format_rf_novosibirsk(value: Optional[str | datetime]) -> str:
    """
    Единый формат отображения для РФ/Новосибирск:
    ЧЧ-ММ-СС ДД-ММ-ГГГГ Новосибирск UTC +07-00
    """
    if value is None:
        return "время не указано"
    parsed: Optional[datetime]
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=LOCAL_TZ)
        parsed = parsed.astimezone(LOCAL_TZ)
    else:
        parsed = parse_mixed_datetime(value)
    if not parsed:
        return str(value)
    return f"{parsed.strftime(UNIFIED_RF_DT_FORMAT)} {NOVOSIBIRSK_LABEL}"