from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from services.user_activity_service import UserActivityService

USER_ACTIVITY_MENU_BTN_TEXT = "🕵️ Действия пользователей"
_PAGE_SIZE = 8


@dataclass(frozen=True)
class UserActivityHandlersDeps:
    ensure_message_access: Callable[[Message, str], Awaitable[object]]
    ensure_callback_access: Callable[[CallbackQuery, str], Awaitable[object]]
    activity_service: UserActivityService


def _manager_kb(token: str, page: int, total: int) -> InlineKeyboardMarkup:
    last_page = max(0, (total - 1) // _PAGE_SIZE) if total else 0
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(total, start + _PAGE_SIZE)

    rows: list[list[InlineKeyboardButton]] = []
    for idx in range(start, end):
        rows.append([
            InlineKeyboardButton(text=f"#{idx + 1}", callback_data=f"ua:open:{token}:{idx}:{page}")
        ])

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"ua:menu:{token}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{last_page + 1}", callback_data="ua:noop"))
    if page < last_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"ua:menu:{token}:{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔄 Обновить срез", callback_data="ua:refresh")])
    rows.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _item_kb(token: str, idx: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К списку", callback_data=f"ua:menu:{token}:{page}")],
        [InlineKeyboardButton(text="🔄 Новый срез", callback_data="ua:refresh")],
    ])


def _manager_text(token: str, page: int, deps: UserActivityHandlersDeps) -> str:
    rows = deps.activity_service.get_rows(token)
    if not rows:
        return "🕵️ <b>Действия пользователей</b>\n\nПока нет данных в audit.log."
    last_page = max(0, (len(rows) - 1) // _PAGE_SIZE)
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(len(rows), start + _PAGE_SIZE)
    lines = [
        "🕵️ <b>Действия пользователей</b>",
        "",
        "Выберите действие для подробностей:",
    ]
    for idx in range(start, end):
        row = rows[idx]
        lines.append(f"<b>#{idx + 1}</b> {row.title}")
        lines.append(f"↳ {row.preview}")
    return "\n".join(lines)


def register_user_activity_handlers(router: Router, deps: UserActivityHandlersDeps) -> None:
    async def _open_new_snapshot(target_message: Message) -> None:
        token = deps.activity_service.create_snapshot(limit_items=120)
        rows = deps.activity_service.get_rows(token)
        await target_message.answer(
            _manager_text(token, 0, deps),
            reply_markup=_manager_kb(token, 0, len(rows)),
        )

    @router.message(F.text == USER_ACTIVITY_MENU_BTN_TEXT)
    async def activity_menu(m: Message):
        if not await deps.ensure_message_access(m, "audit.view"):
            return
        await _open_new_snapshot(m)

    @router.callback_query(F.data == "ua:refresh")
    async def activity_refresh(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return
        token = deps.activity_service.create_snapshot(limit_items=120)
        rows = deps.activity_service.get_rows(token)
        await cq.message.edit_text(
            _manager_text(token, 0, deps),
            reply_markup=_manager_kb(token, 0, len(rows)),
        )
        await cq.answer("Срез обновлён")

    @router.callback_query(F.data.startswith("ua:menu:"))
    async def activity_menu_page(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return
        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        rows = deps.activity_service.get_rows(token)
        if not rows:
            await cq.message.edit_text(
                "Срез устарел. Нажмите «Обновить срез».",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить срез", callback_data="ua:refresh")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="menu:back")],
                ]),
            )
            await cq.answer("Срез устарел")
            return
        await cq.message.edit_text(
            _manager_text(token, page, deps),
            reply_markup=_manager_kb(token, page, len(rows)),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("ua:open:"))
    async def activity_detail(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return
        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""
        idx = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else -1
        page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
        row = deps.activity_service.get_row(token, idx)
        if row is None:
            await cq.answer("Действие не найдено. Обновите срез.", show_alert=True)
            return
        await cq.message.edit_text(row.details, reply_markup=_item_kb(token, idx, page))
        await cq.answer()

    @router.callback_query(F.data == "ua:noop")
    async def activity_noop(cq: CallbackQuery):
        await cq.answer()
