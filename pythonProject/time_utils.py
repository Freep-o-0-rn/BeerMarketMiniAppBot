from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo


# Единая локальная таймзона приложения (по умолчанию — Россия, Новосибирск).
# Может быть переопределена через переменную окружения APP_TIMEZONE.
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Novosibirsk")
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)


def utc_now() -> datetime:
    """Timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def local_now() -> datetime:
    """Timezone-aware локальное время приложения."""
    return datetime.now(LOCAL_TZ)


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
