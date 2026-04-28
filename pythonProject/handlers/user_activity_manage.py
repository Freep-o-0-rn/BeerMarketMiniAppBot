from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

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


def _safe_int(value: Optional[str], default: int = 0) -> int:
    if not value:
        return default
    return int(value) if value.isdigit() else default


def _users_kb(token: str, page: int, total: int) -> InlineKeyboardMarkup:
    last_page = max(0, (total - 1) // _PAGE_SIZE) if total else 0
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(total, start + _PAGE_SIZE)

    rows: list[list[InlineKeyboardButton]] = []

    for idx in range(start, end):
        rows.append([
            InlineKeyboardButton(
                text=f"👤 #{idx + 1}",
                callback_data=f"ua:user:{token}:{idx}:{page}",
            )
        ])

    nav: list[InlineKeyboardButton] = []

    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"ua:menu:{token}:{page - 1}",
            )
        )

    nav.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{last_page + 1}",
            callback_data="ua:noop",
        )
    )

    if page < last_page:
        nav.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"ua:menu:{token}:{page + 1}",
            )
        )

    rows.append(nav)
    rows.append([
        InlineKeyboardButton(
            text="🔄 Обновить срез",
            callback_data="ua:refresh",
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="⬅️ Главное меню",
            callback_data="menu:back",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _user_actions_kb(token: str, uid: str, page: int, total: int) -> InlineKeyboardMarkup:
    last_page = max(0, (total - 1) // _PAGE_SIZE) if total else 0
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(total, start + _PAGE_SIZE)

    rows: list[list[InlineKeyboardButton]] = []

    for idx in range(start, end):
        rows.append([
            InlineKeyboardButton(
                text=f"#{idx + 1}",
                callback_data=f"ua:open:{token}:{uid}:{idx}:{page}",
            )
        ])

    nav: list[InlineKeyboardButton] = []

    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"ua:actions:{token}:{uid}:{page - 1}",
            )
        )

    nav.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{last_page + 1}",
            callback_data="ua:noop",
        )
    )

    if page < last_page:
        nav.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"ua:actions:{token}:{uid}:{page + 1}",
            )
        )

    rows.append(nav)
    rows.append([
        InlineKeyboardButton(
            text="⬅️ К пользователям",
            callback_data=f"ua:menu:{token}:0",
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="🔄 Обновить срез",
            callback_data="ua:refresh",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _item_kb(token: str, uid: str, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К действиям",
                    callback_data=f"ua:actions:{token}:{uid}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Новый срез",
                    callback_data="ua:refresh",
                )
            ],
        ]
    )


def _manager_text(token: str, page: int, deps: UserActivityHandlersDeps) -> str:
    users = deps.activity_service.get_users(token)

    if not users:
        return "🕵️ <b>Действия пользователей</b>\n\nПока нет данных в audit.log."

    last_page = max(0, (len(users) - 1) // _PAGE_SIZE)
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(len(users), start + _PAGE_SIZE)

    lines = [
        "🕵️ <b>Действия пользователей</b>",
        "",
        "Выберите пользователя для просмотра его действий:",
    ]

    for idx in range(start, end):
        user = users[idx]
        lines.append(f"<b>#{idx + 1}</b> {user.title}")
        lines.append(f"↳ {user.preview} · действий: {user.total_actions}")

    return "\n".join(lines)


def _find_uid_by_index(
    token: str,
    user_idx: int,
    deps: UserActivityHandlersDeps,
) -> Optional[str]:
    users = deps.activity_service.get_users(token)

    if user_idx < 0 or user_idx >= len(users):
        return None

    return users[user_idx].uid


def _user_actions_text(
    token: str,
    uid: str,
    page: int,
    deps: UserActivityHandlersDeps,
) -> Optional[str]:
    users = deps.activity_service.get_users(token)
    user = next((item for item in users if item.uid == uid), None)

    if user is None:
        return None

    rows = deps.activity_service.get_user_rows(token, uid)

    if not rows:
        return (
            "🕵️ <b>Действия пользователя</b>\n\n"
            f"{user.title}\n"
            "Нет действий в текущем срезе."
        )

    last_page = max(0, (len(rows) - 1) // _PAGE_SIZE)
    page = max(0, min(page, last_page))
    start = page * _PAGE_SIZE
    end = min(len(rows), start + _PAGE_SIZE)

    lines = [
        "🕵️ <b>Действия пользователя</b>",
        f"{user.title} · uid=<code>{user.uid}</code>",
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
        users = deps.activity_service.get_users(token)

        await target_message.answer(
            _manager_text(token, 0, deps),
            reply_markup=_users_kb(token, 0, len(users)),
        )

    @router.message(F.text == USER_ACTIVITY_MENU_BTN_TEXT)
    async def activity_menu(m: Message) -> None:
        if not await deps.ensure_message_access(m, "audit.view"):
            return

        await _open_new_snapshot(m)

    @router.callback_query(F.data == "ua:refresh")
    async def activity_refresh(cq: CallbackQuery) -> None:
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return

        token = deps.activity_service.create_snapshot(limit_items=120)
        users = deps.activity_service.get_users(token)

        if cq.message:
            await cq.message.edit_text(
                _manager_text(token, 0, deps),
                reply_markup=_users_kb(token, 0, len(users)),
            )

        await cq.answer("Срез обновлён")

    @router.callback_query(F.data.startswith("ua:menu:"))
    async def activity_menu_page(cq: CallbackQuery) -> None:
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return

        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""
        page = _safe_int(parts[3] if len(parts) > 3 else None, 0)

        users = deps.activity_service.get_users(token)

        if not users:
            if cq.message:
                await cq.message.edit_text(
                    "Срез устарел. Нажмите «Обновить срез».",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🔄 Обновить срез",
                                    callback_data="ua:refresh",
                                )
                            ],
                            [
                                InlineKeyboardButton(
                                    text="⬅️ Главное меню",
                                    callback_data="menu:back",
                                )
                            ],
                        ]
                    ),
                )

            await cq.answer("Срез устарел")
            return

        if cq.message:
            await cq.message.edit_text(
                _manager_text(token, page, deps),
                reply_markup=_users_kb(token, page, len(users)),
            )

        await cq.answer()

    @router.callback_query(F.data.startswith("ua:user:"))
    async def activity_user(cq: CallbackQuery) -> None:
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return

        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""
        user_idx = _safe_int(parts[3] if len(parts) > 3 else None, -1)
        page = _safe_int(parts[4] if len(parts) > 4 else None, 0)

        uid = _find_uid_by_index(token, user_idx, deps)

        if uid is None:
            await cq.answer("Пользователь не найден. Обновите срез.", show_alert=True)
            return

        user_rows = deps.activity_service.get_user_rows(token, uid)
        text = _user_actions_text(token, uid, page, deps)

        if text is None:
            await cq.answer("Пользователь не найден. Обновите срез.", show_alert=True)
            return

        if cq.message:
            await cq.message.edit_text(
                text,
                reply_markup=_user_actions_kb(token, uid, page, len(user_rows)),
            )

        await cq.answer()

    @router.callback_query(F.data.startswith("ua:actions:"))
    async def activity_user_actions_page(cq: CallbackQuery) -> None:
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return

        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""
        uid = parts[3] if len(parts) > 3 else ""
        page = _safe_int(parts[4] if len(parts) > 4 else None, 0)

        if not uid:
            await cq.answer("Пользователь не найден. Обновите срез.", show_alert=True)
            return

        user_rows = deps.activity_service.get_user_rows(token, uid)
        text = _user_actions_text(token, uid, page, deps)

        if text is None:
            await cq.answer("Список недоступен. Обновите срез.", show_alert=True)
            return

        if cq.message:
            await cq.message.edit_text(
                text,
                reply_markup=_user_actions_kb(token, uid, page, len(user_rows)),
            )

        await cq.answer()

    @router.callback_query(F.data.startswith("ua:open:"))
    async def activity_detail(cq: CallbackQuery) -> None:
        if not await deps.ensure_callback_access(cq, "audit.view"):
            return

        parts = (cq.data or "").split(":")
        token = parts[2] if len(parts) > 2 else ""

        # Совместимость со старым форматом:
        # ua:open:{token}:{user_idx}:{idx}:{page}
        uid = parts[3] if len(parts) > 3 else ""

        if uid.isdigit() and len(parts) > 5:
            legacy_uid = _find_uid_by_index(token, int(uid), deps)
            uid = legacy_uid or ""
            idx = _safe_int(parts[4], -1)
            page = _safe_int(parts[5], 0)
        else:
            idx = _safe_int(parts[4] if len(parts) > 4 else None, -1)
            page = _safe_int(parts[5] if len(parts) > 5 else None, 0)

        if not uid:
            await cq.answer("Пользователь не найден. Обновите срез.", show_alert=True)
            return

        row = deps.activity_service.get_user_row(token, uid, idx)

        if row is None:
            await cq.answer("Действие не найдено. Обновите срез.", show_alert=True)
            return

        if cq.message:
            await cq.message.edit_text(
                row.details,
                reply_markup=_item_kb(token, uid, page),
            )

        await cq.answer()

    @router.callback_query(F.data == "ua:noop")
    async def activity_noop(cq: CallbackQuery) -> None:
        await cq.answer()