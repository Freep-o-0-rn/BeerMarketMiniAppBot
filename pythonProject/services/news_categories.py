from __future__ import annotations

from typing import Dict, List, Optional

DEFAULT_NEWS_CATEGORY = "other"
NEWS_CATEGORY_LABELS: Dict[str, str] = {
    "system": "Системные",
    "promo": "Акции",
    "news": "Новости",
    "other": "Другое",
}

_LEGACY_LABEL_TO_KEY = {
    "новость": "news",
    "новости": "news",
    "обновление": "system",
    "сервис": "system",
    "системные": "system",
    "акция": "promo",
    "акции": "promo",
    "другое": "other",
}


def normalize_news_category(raw: Optional[str], *, default: str = DEFAULT_NEWS_CATEGORY) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return default
    if value in NEWS_CATEGORY_LABELS:
        return value
    if value in _LEGACY_LABEL_TO_KEY:
        return _LEGACY_LABEL_TO_KEY[value]
    return default


def category_label(category_key: Optional[str]) -> str:
    return NEWS_CATEGORY_LABELS.get(normalize_news_category(category_key))


def category_catalog() -> List[dict]:
    return [
        {"key": key, "label": label}
        for key, label in NEWS_CATEGORY_LABELS.items()
    ]