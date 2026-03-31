from __future__ import annotations

from aiogram import F, Router
from aiogram.types import Message
from urllib.parse import urlparse

MINIAPP_WEB_BTN_TEXT = "📰 Открыть Mini App"


def register_miniapp_handlers(router: Router, *, ensure_message_access, miniapp_url: str) -> None:
    miniapp_url = (miniapp_url or "").strip()

    def _is_valid_url(url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    @router.message(F.text == MINIAPP_WEB_BTN_TEXT)
    async def miniapp_open_info(m: Message):
        # На случай старого клиента Telegram без web_app кнопок.
        if not await ensure_message_access(m, "prices.view"):
            return
        if not _is_valid_url(miniapp_url):
            await m.answer(
                "Mini App временно недоступен: ссылка не настроена.\n"
                "Проверьте переменную окружения MINIAPP_URL."
            )
            return
        await m.answer(
            "Откройте Mini App кнопкой «📰 Открыть Mini App» в меню.\n"
            f"Если не открывается встроенно — используйте ссылку: {miniapp_url}"
        )
