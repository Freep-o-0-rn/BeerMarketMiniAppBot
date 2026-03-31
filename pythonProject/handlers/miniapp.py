from __future__ import annotations

from aiogram import F, Router
from aiogram.types import Message

MINIAPP_WEB_BTN_TEXT = "📰 Открыть Mini App"


def register_miniapp_handlers(router: Router, *, ensure_message_access, miniapp_url: str) -> None:
    @router.message(F.text == MINIAPP_WEB_BTN_TEXT)
    async def miniapp_open_info(m: Message):
        # На случай старого клиента Telegram без web_app кнопок.
        if not await ensure_message_access(m, "prices.view"):
            return
        await m.answer(
            "Откройте Mini App кнопкой «📰 Открыть Mini App» в меню.\n"
            f"Если не открывается встроенно — используйте ссылку: {miniapp_url}"
        )
