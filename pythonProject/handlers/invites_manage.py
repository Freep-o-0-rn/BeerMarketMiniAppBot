from __future__ import annotations

import html
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import ClientTimeout
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, User

from services.invites_service import (
    INVITE_MAX_USES_OPTIONS,
    INVITE_ROLE_LABELS,
    INVITE_ROLE_OPTIONS,
    INVITE_TTL_LABELS,
    INVITE_TTL_MAP,
    INVITE_TTL_OPTIONS,
    InviteService,
)
from time_utils import format_rf_novosibirsk


INVITE_MENU_BTN_TEXT = "✉️ Инвайты"


class InviteStates(StatesGroup):
    waiting_role = State()
    waiting_ttl = State()
    waiting_max_uses = State()
    waiting_name = State()


class InvitesManager:
    def __init__(self, *, bot, invite_service: InviteService, shortener_timeout: float = 4.5):
        self.bot = bot
        self.invite_service = invite_service
        self.shortener_timeout = shortener_timeout
        self._bot_username_cache: Optional[str] = None

    async def get_bot_username(self) -> str:
        if self._bot_username_cache:
            return self._bot_username_cache
        me = await self.bot.get_me()
        self._bot_username_cache = str(getattr(me, "username", "") or "").strip()
        return self._bot_username_cache

    async def build_invite_urls(self, payload: str) -> Tuple[str, str]:
        username = await self.get_bot_username()
        deep_link = f"https://t.me/{username}?start={payload}" if username else f"https://t.me/?start={payload}"
        short_url = deep_link
        try:
            timeout = ClientTimeout(total=self.shortener_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get("https://clck.ru/--", params={"url": deep_link}) as resp:
                    if resp.status == 200:
                        candidate = (await resp.text()).strip()
                        if candidate.startswith("http"):
                            short_url = candidate
        except Exception:
            pass
        return deep_link, short_url

    async def try_apply_from_start(
        self,
        m: Message,
        *,
        update_user_record: Callable[[Any, Dict[str, Any]], None],
        get_registered_name: Callable[[Optional[int]], str],
        role_label: Callable[[str], str],
    ) -> Optional[str]:
        text = str(getattr(m, "text", "") or "").strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return None
        payload = parts[1].strip()
        if not payload.startswith("iv_"):
            return None
        user_id = getattr(m.from_user, "id", None)
        if not user_id:
            return None

        redeem_result = self.invite_service.redeem(payload, user_id, get_registered_name(user_id) or getattr(m.from_user, "full_name", ""))
        if not redeem_result.ok:
            if redeem_result.reason == "not_found":
                return "⚠️ Приглашение не найдено или уже удалено."
            return "⚠️ Срок действия приглашения истёк или лимит использований исчерпан."

        invite = redeem_result.invite or {}
        role = str(invite.get("role") or "guest")
        target_name = str(invite.get("target_name") or "").strip()
        patch: Dict[str, Any] = {
            "role": role,
            "authorized_by_admin": True,
            "auth_status": "approved",
            "auth_source": "manual_admin",
            "onboard_completed": True,
        }
        if target_name and int(invite.get("max_uses") or 0) == 1:
            patch["name"] = target_name
        update_user_record(user_id, patch)
        return f"✅ Приглашение применено.\nВам назначена роль: <b>{html.escape(role_label(role))}</b>."

    @staticmethod
    def _role_label(role: str, role_label: Callable[[str], str]) -> str:
        return INVITE_ROLE_LABELS.get(role) or role_label(role)

    @staticmethod
    def _ttl_label(ttl_key: str) -> str:
        return INVITE_TTL_LABELS.get(ttl_key) or ttl_key

    def _is_active(self, invite: Dict[str, Any]) -> bool:
        return self.invite_service.is_active(invite)

    def _usage_summary(self, invite: Dict[str, Any]) -> str:
        return f"{int(invite.get('uses_count') or 0)}/{int(invite.get('max_uses') or 0)}"

    def _status_icon(self, invite: Dict[str, Any]) -> str:
        if self._is_active(invite):
            return "🟢"
        if int(invite.get("uses_count") or 0) > 0:
            return "✅"
        return "⚪️"

    def _compact_label(self, invite: Dict[str, Any], role_label: Callable[[str], str]) -> str:
        role = self._role_label(str(invite.get("role") or "guest"), role_label)
        return f"{self._status_icon(invite)} {role} · {self._usage_summary(invite)}"

    @staticmethod
    def invites_menu_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать инвайт", callback_data="inv:create")],
            [InlineKeyboardButton(text="🟢 Активные приглашения", callback_data="inv:list:active:0")],
            [InlineKeyboardButton(text="🗂 Архив приглашений", callback_data="inv:list:archive:0")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:back")],
        ])

    @staticmethod
    def invite_role_pick_kb() -> InlineKeyboardMarkup:
        rows = [[InlineKeyboardButton(text=label, callback_data=f"inv:create:role:{role}")] for role, label in INVITE_ROLE_OPTIONS]
        rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="inv:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def invite_ttl_pick_kb() -> InlineKeyboardMarkup:
        rows = [[InlineKeyboardButton(text=label, callback_data=f"inv:create:ttl:{key}")] for key, label, _ in INVITE_TTL_OPTIONS]
        rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="inv:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def invite_uses_pick_kb() -> InlineKeyboardMarkup:
        rows = [[InlineKeyboardButton(text=f"{value} использ.", callback_data=f"inv:create:uses:{value}")] for value in INVITE_MAX_USES_OPTIONS]
        rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="inv:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def invite_name_optional_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Не указывать имя", callback_data="inv:create:name:skip")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="inv:menu")],
        ])

    def invites_list_kb(self, *, mode: str, page: int, role_label: Callable[[str], str], page_size: int = 8) -> InlineKeyboardMarkup:
        items = self.invite_service.list_invites(mode)
        total = len(items)
        last_page = max(0, (total - 1) // page_size) if total else 0
        page = max(0, min(page, last_page))
        start = page * page_size
        end = min(total, start + page_size)
        rows: List[List[InlineKeyboardButton]] = []
        for invite in items[start:end]:
            invite_id = str(invite.get("id") or "")
            if not invite_id:
                continue
            rows.append([InlineKeyboardButton(text=self._compact_label(invite, role_label), callback_data=f"inv:view:{invite_id}:{mode}:{page}")])
        if not rows:
            rows.append([InlineKeyboardButton(text="— Пусто —", callback_data="inv:noop")])
        nav: List[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv:list:{mode}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{last_page+1}", callback_data="inv:noop"))
        if page < last_page:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"inv:list:{mode}:{page+1}"))
        rows.append(nav)
        rows.append([InlineKeyboardButton(text="⬅️ К меню инвайтов", callback_data="inv:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @staticmethod
    def invite_detail_kb(invite: Dict[str, Any], mode: str, page: int) -> InlineKeyboardMarkup:
        invite_id = str(invite.get("id") or "")
        rows: List[List[InlineKeyboardButton]] = []
        if str(invite.get("status") or "active") == "active":
            rows.append([InlineKeyboardButton(text="🗂 Переместить в архив", callback_data=f"inv:archive:{invite_id}:{mode}:{page}")])
        rows.append([InlineKeyboardButton(text="⬅️ К списку", callback_data=f"inv:list:{mode}:{page}")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def render_details(self, invite: Dict[str, Any], role_label: Callable[[str], str]) -> str:
        status = "🟢 Активен" if self._is_active(invite) else "🗂 В архиве"
        role = self._role_label(str(invite.get("role") or "guest"), role_label)
        ttl_key = str(invite.get("ttl_key") or "inf")
        max_uses = int(invite.get("max_uses") or 0)
        uses_count = int(invite.get("uses_count") or 0)
        target_name = str(invite.get("target_name") or "").strip()
        created_at = format_rf_novosibirsk(invite.get("created_at"))
        expires_at = format_rf_novosibirsk(invite.get("expires_at")) if invite.get("expires_at") else "бессрочно"
        short_url = str(invite.get("short_url") or invite.get("deep_link") or "—")
        code = str(invite.get("code") or "—")
        lines = [
            status,
            f"Роль: <b>{html.escape(role)}</b>",
            f"TTL: <b>{html.escape(self._ttl_label(ttl_key))}</b> (до <code>{html.escape(expires_at)}</code>)" if ttl_key != "inf" else f"TTL: <b>{html.escape(self._ttl_label(ttl_key))}</b>",
            f"Использования: <b>{uses_count}/{max_uses}</b>",
            f"Создан: <code>{html.escape(created_at)}</code>",
            f"Код: <code>{html.escape(code)}</code>",
            f"Ссылка: {html.escape(short_url)}",
        ]
        if target_name:
            lines.append(f"Имя в боте (для 1 использования): <b>{html.escape(target_name)}</b>")
        uses = invite.get("uses") if isinstance(invite.get("uses"), list) else []
        if uses:
            lines.extend(["", "<b>Приняли приглашение:</b>"])
            for item in uses[:10]:
                uid = str((item or {}).get("user_id") or "—")
                used_at = format_rf_novosibirsk((item or {}).get("used_at"))
                uname = str((item or {}).get("display_name") or "").strip()
                line = f"• <code>{html.escape(uid)}</code> — <code>{html.escape(used_at)}</code>"
                if uname:
                    line += f" ({html.escape(uname)})"
                lines.append(line)
        return "\n".join(lines)

    def register_handlers(
        self,
        router: Router,
        *,
        ensure_message_access,
        ensure_callback_access,
        role_label,
    ) -> None:
        @router.message(F.text == INVITE_MENU_BTN_TEXT)
        async def invites_menu_open(m: Message, state: FSMContext):
            if not await ensure_message_access(m, "invites.manage", state=state):
                return
            await state.clear()
            await m.answer("✉️ Сервис инвайтов:", reply_markup=self.invites_menu_kb())

        @router.callback_query(F.data == "inv:menu")
        async def invites_menu_open_callback(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            await state.clear()
            await cq.message.edit_text("✉️ Сервис инвайтов:", reply_markup=self.invites_menu_kb())
            await cq.answer()

        @router.callback_query(F.data == "inv:noop")
        async def invites_noop(cq: CallbackQuery):
            await cq.answer()

        @router.callback_query(F.data == "inv:create")
        async def invites_create_start(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            await state.clear()
            await state.set_state(InviteStates.waiting_role)
            await cq.message.edit_text("Выберите роль для приглашения:", reply_markup=self.invite_role_pick_kb())
            await cq.answer()

        @router.callback_query(F.data.startswith("inv:create:role:"))
        async def invites_create_pick_role(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            role = (cq.data or "").split(":")[-1]
            if role not in {r for r, _ in INVITE_ROLE_OPTIONS}:
                await cq.answer("Некорректная роль.", show_alert=True)
                return
            await state.update_data(inv_role=role)
            await state.set_state(InviteStates.waiting_ttl)
            await cq.message.edit_text(f"Роль: <b>{html.escape(self._role_label(role, role_label))}</b>\nВыберите срок действия:", reply_markup=self.invite_ttl_pick_kb())
            await cq.answer()

        @router.callback_query(F.data.startswith("inv:create:ttl:"))
        async def invites_create_pick_ttl(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            ttl_key = (cq.data or "").split(":")[-1]
            if ttl_key not in INVITE_TTL_MAP:
                await cq.answer("Некорректный срок жизни.", show_alert=True)
                return
            await state.update_data(inv_ttl=ttl_key)
            await state.set_state(InviteStates.waiting_max_uses)
            await cq.message.edit_text(f"TTL: <b>{html.escape(self._ttl_label(ttl_key))}</b>\nВыберите лимит использований:", reply_markup=self.invite_uses_pick_kb())
            await cq.answer()

        async def finalize(*, actor: User, state: FSMContext, target_name: str, message: Message, callback: Optional[CallbackQuery] = None):
            data = await state.get_data()
            role = str(data.get("inv_role") or "")
            ttl_key = str(data.get("inv_ttl") or "")
            max_uses = int(data.get("inv_max_uses") or 0)
            if role not in {r for r, _ in INVITE_ROLE_OPTIONS} or ttl_key not in INVITE_TTL_MAP or max_uses not in INVITE_MAX_USES_OPTIONS:
                await state.clear()
                await message.answer("Не удалось создать инвайт: неполные параметры.", reply_markup=self.invites_menu_kb())
                if callback:
                    await callback.answer()
                return

            payload = self.invite_service.build_payload()
            deep_link, short_url = await self.build_invite_urls(payload)
            invite = self.invite_service.create_invite(
                created_by=getattr(actor, "id", None),
                role=role,
                ttl_key=ttl_key,
                max_uses=max_uses,
                target_name=target_name,
                deep_link=deep_link,
                short_url=short_url,
            )
            invite["code"] = payload
            self.invite_service.append_invite(invite)
            await state.clear()
            text = (
                "✅ Инвайт создан.\n\n"
                f"Роль: <b>{html.escape(self._role_label(role, role_label))}</b>\n"
                f"TTL: <b>{html.escape(self._ttl_label(ttl_key))}</b>\n"
                f"Лимит: <b>{max_uses}</b>\n"
                f"Короткая ссылка: {html.escape(short_url)}\n"
                f"Техническая ссылка: {html.escape(deep_link)}"
            )
            if callback:
                await callback.message.edit_text(text, reply_markup=self.invites_menu_kb())
                await callback.answer("Инвайт создан.")
            else:
                await message.answer(text, reply_markup=self.invites_menu_kb())

        @router.callback_query(F.data.startswith("inv:create:uses:"))
        async def invites_create_pick_uses(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            raw = (cq.data or "").split(":")[-1]
            if not raw.isdigit() or int(raw) not in INVITE_MAX_USES_OPTIONS:
                await cq.answer("Лимит не поддерживается.", show_alert=True)
                return
            max_uses = int(raw)
            await state.update_data(inv_max_uses=max_uses)
            if max_uses == 1:
                await state.set_state(InviteStates.waiting_name)
                await cq.message.edit_text(
                    "Это ссылка на одного пользователя.\nУкажите имя пользователя в боте (для отчётов) или пропустите.",
                    reply_markup=self.invite_name_optional_kb(),
                )
                await cq.answer()
                return
            await finalize(actor=cq.from_user, state=state, target_name="", message=cq.message, callback=cq)

        @router.callback_query(F.data == "inv:create:name:skip")
        async def invites_create_skip_name(cq: CallbackQuery, state: FSMContext):
            if not await ensure_callback_access(cq, "invites.manage", state=state):
                return
            await finalize(actor=cq.from_user, state=state, target_name="", message=cq.message, callback=cq)

        @router.message(InviteStates.waiting_name)
        async def invites_create_name_input(m: Message, state: FSMContext):
            if not await ensure_message_access(m, "invites.manage", state=state):
                return
            name = str(m.text or "").strip()
            if not name:
                await m.answer("Имя не может быть пустым. Введите имя или нажмите «⏭ Не указывать имя».", reply_markup=self.invite_name_optional_kb())
                return
            await finalize(actor=m.from_user, state=state, target_name=name, message=m)

        @router.callback_query(F.data.startswith("inv:list:"))
        async def invites_list(cq: CallbackQuery):
            if not await ensure_callback_access(cq, "invites.manage"):
                return
            parts = (cq.data or "").split(":")
            mode = parts[2] if len(parts) > 2 else "active"
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            title = "🟢 Активные приглашения:" if mode == "active" else "🗂 Архив приглашений:"
            await cq.message.edit_text(title, reply_markup=self.invites_list_kb(mode=mode, page=page, role_label=role_label))
            await cq.answer()

        @router.callback_query(F.data.startswith("inv:view:"))
        async def invites_view(cq: CallbackQuery):
            if not await ensure_callback_access(cq, "invites.manage"):
                return
            parts = (cq.data or "").split(":")
            invite_id = parts[2] if len(parts) > 2 else ""
            mode = parts[3] if len(parts) > 3 else "active"
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            invite = self.invite_service.get_invite(invite_id)
            if not invite:
                await cq.answer("Инвайт не найден.", show_alert=True)
                return
            await cq.message.edit_text(self.render_details(invite, role_label), reply_markup=self.invite_detail_kb(invite, mode, page))
            await cq.answer()

        @router.callback_query(F.data.startswith("inv:archive:"))
        async def invites_archive(cq: CallbackQuery):
            if not await ensure_callback_access(cq, "invites.manage"):
                return
            parts = (cq.data or "").split(":")
            invite_id = parts[2] if len(parts) > 2 else ""
            mode = parts[3] if len(parts) > 3 else "active"
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            if not self.invite_service.archive_invite(invite_id):
                await cq.answer("Инвайт не найден.", show_alert=True)
                return
            await cq.message.edit_text("🗂 Инвайт отправлен в архив.", reply_markup=self.invites_list_kb(mode=mode, page=page, role_label=role_label))
            await cq.answer("Готово.")
