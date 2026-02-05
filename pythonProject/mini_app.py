# mini_app.py
import os
import json
import logging

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

log = logging.getLogger(__name__)

MINI_APP_URL = os.getenv("MINI_APP_URL", "https://freep0rndeveloper.website/")
MINI_APP_BTN_TEXT = os.getenv("MINI_APP_BTN_TEXT", "📱 Mini App")

router = Router(name="mini_app")


def mini_app_reply_button() -> KeyboardButton:
    # Кнопка в ReplyKeyboard (открывает WebApp в Telegram)
    return KeyboardButton(text=MINI_APP_BTN_TEXT, web_app=WebAppInfo(url=MINI_APP_URL))


def mini_app_inline_kb() -> InlineKeyboardMarkup:
    # Inline-кнопка (можно использовать для /app)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть мини-приложение", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])


async def setup_menu_button(bot) -> None:
    """
    Кнопка меню чата слева от поля ввода (не ломает текущие сценарии).
    Если не получится — просто пишем warning и работаем дальше.
    """
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
async def on_webapp_data(m: Message):
    """
    Данные приходят из фронта через Telegram.WebApp.sendData(...).
    Пока просто логируем/подтверждаем — бизнес-логику добавишь позже.
    """
    raw = (m.web_app_data.data or "").strip()

    try:
        payload = json.loads(raw) if raw else {}
    except Exception:
        payload = {"raw": raw}

    log.info("web_app_data uid=%s payload=%s", getattr(m.from_user, "id", None), payload)
    await m.answer("✅ Данные из Mini App получены.")
