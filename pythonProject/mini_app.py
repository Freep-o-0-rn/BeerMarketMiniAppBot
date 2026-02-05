# mini_app.py
import os
import json
import logging
from typing import Callable, Optional, Any, Dict

from aiogram import Router, F
from aiogram.filters import Command, StateFilter
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    WebAppInfo,
    MenuButtonWebApp,
)
from aiogram.fsm.context import FSMContext

log = logging.getLogger(__name__)

MINI_APP_URL = os.getenv("MINI_APP_URL", "https://freep0rndeveloper.website/")
MINI_APP_BTN_TEXT = os.getenv("MINI_APP_BTN_TEXT", "📱 Mini App")

router = Router(name="mini_app")

_WEBAPP_HANDLER: Optional[Callable[[Message, FSMContext, Dict[str, Any]], Any]] = None

def set_webapp_handler(fn: Callable[[Message, FSMContext, Dict[str, Any]], Any]) -> None:
    """Telegram_bot.py может сюда подложить роутер действий Mini App."""
    global _WEBAPP_HANDLER
    _WEBAPP_HANDLER = fn


def mini_app_reply_button() -> KeyboardButton:
    return KeyboardButton(text=MINI_APP_BTN_TEXT, web_app=WebAppInfo(url=MINI_APP_URL))


def mini_app_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть мини-приложение", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])


async def setup_menu_button(bot) -> None:
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="BeerMarket", web_app=WebAppInfo(url=MINI_APP_URL))
        )
        log.info("Mini App menu button set: %s", MINI_APP_URL)
    except Exception as e:
        log.warning("Mini App menu button not set: %s", e)


@router.message(Command("app"))
async def cmd_app(m: Message):
    await m.answer("Mini App BeerMarket:", reply_markup=mini_app_inline_kb())


@router.message(StateFilter(None), F.web_app_data)
async def on_webapp_data(m: Message, state: FSMContext):
    raw = (m.web_app_data.data or "").strip()

    try:
        payload = json.loads(raw) if raw else {}
    except Exception:
        payload = {"action": "raw", "raw": raw}

    uid = getattr(getattr(m, "from_user", None), "id", None)
    log.info("web_app_data uid=%s payload=%s", uid, payload)

    if _WEBAPP_HANDLER:
        try:
            await _WEBAPP_HANDLER(m, state, payload)
            return
        except Exception:
            log.exception("webapp handler failed")

    await m.answer("✅ Данные из Mini App получены.")
