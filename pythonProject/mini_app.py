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
_WEBAPP_URL_BUILDER: Optional[Callable[[Message], str]] = None

def set_webapp_handler(fn: Callable[[Message, FSMContext, Dict[str, Any]], Any]) -> None:
    """Telegram_bot.py может сюда подложить роутер действий Mini App."""
    global _WEBAPP_HANDLER
    _WEBAPP_HANDLER = fn


def set_webapp_url_builder(fn: Callable[[Message], str]) -> None:
    """Telegram_bot.py может передать сборщик URL для WebApp с параметрами авторизации."""
    global _WEBAPP_URL_BUILDER
    _WEBAPP_URL_BUILDER = fn


def build_webapp_url(message: Optional[Message] = None, chat_id: Optional[int] = None) -> str:
    subject = message if message is not None else chat_id
    if subject is not None and _WEBAPP_URL_BUILDER:
        try:
            return _WEBAPP_URL_BUILDER(subject)
        except Exception:
            log.exception("webapp url builder failed")
    return MINI_APP_URL


def mini_app_reply_button(url: Optional[str] = None) -> KeyboardButton:
    return KeyboardButton(text=MINI_APP_BTN_TEXT, web_app=WebAppInfo(url=url or MINI_APP_URL))


def mini_app_inline_kb(url: Optional[str] = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть мини-приложение", web_app=WebAppInfo(url=url or MINI_APP_URL))]
    ])

async def setup_menu_button(bot, message: Optional[Message] = None, chat_id: Optional[int] = None) -> None:
    target_chat_id = chat_id or getattr(getattr(message, "chat", None), "id", None)
    url = build_webapp_url(message, chat_id=target_chat_id)
    try:
        await bot.set_chat_menu_button(
            chat_id=target_chat_id,
            menu_button=MenuButtonWebApp(text="BeerMarket", web_app=WebAppInfo(url=url))
        )
        log.info("Mini App menu button set%s: %s", f" for chat {target_chat_id}" if target_chat_id else "", url)
    except Exception as e:
        log.warning("Mini App menu button not set%s: %s", f" for chat {target_chat_id}" if target_chat_id else "", e)


@router.message(Command("app"))
async def cmd_app(m: Message):
    await m.answer("Mini App BeerMarket:", reply_markup=mini_app_inline_kb(build_webapp_url(m)))


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