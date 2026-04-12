from dataclasses import dataclass
from typing import Awaitable, Callable, List

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message


@dataclass(frozen=True)
class NotificationHandlersDeps:
    ensure_message_access: Callable[[Message, str], Awaitable[object]]
    ensure_callback_access: Callable[[CallbackQuery, str], Awaitable[object]]
    notifications_menu_kb: Callable[[int], InlineKeyboardMarkup]
    admin_user_notifications_kb: Callable[[str, int], InlineKeyboardMarkup]
    notification_enabled: Callable[[int, str], bool]
    set_user_notification_setting: Callable[[int, str, bool], None]
    notification_order: List[str]


def register_notification_handlers(router: Router, deps: NotificationHandlersDeps) -> None:
    @router.message(F.text == "🔔 Уведомления")
    async def notifications_menu(m: Message):
        if not await deps.ensure_message_access(m, "notifications.manage"):
            return
        await m.answer(
            "Управление уведомлениями:",
            reply_markup=deps.notifications_menu_kb(getattr(m.from_user, "id", 0)),
        )

    @router.callback_query(F.data == "notify:menu")
    async def notifications_menu_callback(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "notifications.manage"):
            return
        await cq.message.answer(
            "Управление уведомлениями:",
            reply_markup=deps.notifications_menu_kb(getattr(cq.from_user, "id", 0)),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("notify:toggle:"))
    async def notifications_toggle(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "notifications.manage"):
            return
        parts = (cq.data or "").split(":")
        key = parts[2] if len(parts) > 2 else ""
        target_user_id = int(getattr(cq.from_user, "id", 0) or 0)
        if key not in deps.notification_order:
            await cq.answer("Неизвестный тип уведомления.", show_alert=True)
            return
        enabled = deps.notification_enabled(target_user_id, key)
        deps.set_user_notification_setting(target_user_id, key, not enabled)
        await cq.message.edit_text(
            "Управление уведомлениями:",
            reply_markup=deps.notifications_menu_kb(target_user_id),
        )
        await cq.answer("Настройка обновлена.")

    @router.callback_query(F.data.startswith("usr:notifymenu:"))
    async def admin_user_notifications_menu(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "users.manage"):
            return
        parts = (cq.data or "").split(":")
        uid = parts[2] if len(parts) > 2 else ""
        page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
        if not uid or not uid.isdigit():
            await cq.answer("Пользователь не найден.", show_alert=True)
            return
        await cq.message.edit_text(
            f"🔔 Уведомления пользователя <code>{uid}</code>",
            reply_markup=deps.admin_user_notifications_kb(uid, page),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("usr:notify:"))
    async def admin_user_notification_toggle(cq: CallbackQuery):
        if not await deps.ensure_callback_access(cq, "users.manage"):
            return
        parts = (cq.data or "").split(":")
        uid = parts[2] if len(parts) > 2 else ""
        key = parts[3] if len(parts) > 3 else ""
        page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
        if not uid or not uid.isdigit():
            await cq.answer("Пользователь не найден.", show_alert=True)
            return
        if key not in deps.notification_order:
            await cq.answer("Неизвестный тип уведомления.", show_alert=True)
            return
        user_id = int(uid)
        enabled = deps.notification_enabled(user_id, key)
        deps.set_user_notification_setting(user_id, key, not enabled)
        await cq.message.edit_text(
            f"🔔 Уведомления пользователя <code>{uid}</code>",
            reply_markup=deps.admin_user_notifications_kb(uid, page),
        )
        await cq.answer("Настройка обновлена.")
