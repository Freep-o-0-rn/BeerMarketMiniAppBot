from __future__ import annotations

from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

NEWS_MANAGE_BTN_TEXT = "📰 Управление новостями"


class NewsCreateStates(StatesGroup):
    waiting_title = State()
    waiting_text = State()


class NewsEditStates(StatesGroup):
    waiting_title = State()
    waiting_text = State()


class NewsEditDateStates(StatesGroup):
    waiting_iso_datetime = State()


def _news_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать новость", callback_data="news:create")],
        [InlineKeyboardButton(text="📋 Черновики", callback_data="news:list:draft:0")],
        [InlineKeyboardButton(text="📰 Опубликованные", callback_data="news:list:published:0")],
    ])


def _news_item_kb(news_id: str, status: str, page: int, list_status: str) -> InlineKeyboardMarkup:
    publish_text = "✅ Опубликовать" if status != "published" else "📝 В черновик"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить заголовок/текст", callback_data=f"news:edit:{news_id}")],
        [InlineKeyboardButton(text="🖼 Добавить медиа", callback_data=f"news:addmedia:{news_id}")],
        [InlineKeyboardButton(text=publish_text, callback_data=f"news:toggle:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="🕒 Сменить дату публикации", callback_data=f"news:date:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"news:delete:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"news:list:{list_status}:{page}")],
    ])


def _pager(items_count: int, page: int, status: str, page_size: int) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    max_page = max(0, (items_count - 1) // page_size) if items_count else 0
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"news:list:{status}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{max_page+1}", callback_data="news:noop"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"news:list:{status}:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬆️ Меню новостей", callback_data="news:menu")])
    return rows


def register_news_manage_handlers(
    router: Router,
    *,
    news_service,
    media_service,
    ensure_message_access,
    ensure_callback_access,
    menu_for_user_id,
) -> None:
    page_size = 6

    @router.message(F.text == NEWS_MANAGE_BTN_TEXT)
    async def news_manage_entry(m: Message):
        if not await ensure_message_access(m, "news.manage"):
            return
        await m.answer("Управление новостями Mini App", reply_markup=_news_menu_kb())

    @router.callback_query(F.data == "news:menu")
    async def news_manage_menu(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        await cq.message.edit_text("Управление новостями Mini App", reply_markup=_news_menu_kb())
        await cq.answer()

    @router.callback_query(F.data == "news:create")
    async def news_create_start(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        await state.set_state(NewsCreateStates.waiting_title)
        await cq.message.answer("Шаг 1/2. Введите заголовок новости.")
        await cq.answer()

    @router.message(NewsCreateStates.waiting_title, F.text)
    async def news_create_title(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        await state.update_data(title=(m.text or "").strip())
        await state.set_state(NewsCreateStates.waiting_text)
        await m.answer("Шаг 2/2. Введите текст новости.")

    @router.message(NewsCreateStates.waiting_text, F.text)
    async def news_create_text(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        data = await state.get_data()
        title = (data.get("title") or "").strip()
        text = (m.text or "").strip()
        if not title:
            await m.answer("Заголовок пустой. Начните заново.")
            await state.clear()
            return
        news_id = news_service.create_news(
            title=title,
            text=text,
            author_id=getattr(m.from_user, "id", 0),
            author_name=(getattr(m.from_user, "full_name", None) or "Unknown"),
            status="draft",
        )
        await state.clear()
        item = news_service.get_news(news_id)
        await m.answer(
            f"Черновик создан: {item['title']}\nID: {news_id}",
            reply_markup=_news_item_kb(news_id, item.get("status", "draft"), 0, "draft")
        )

    @router.callback_query(F.data.startswith("news:list:"))
    async def news_list(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, status, page_raw = (cq.data or "").split(":", 3)
        page = max(0, int(page_raw))
        offset = page * page_size
        all_rows = news_service.list_news(status=status, limit=500, offset=0)
        rows = news_service.list_news(status=status, limit=page_size, offset=offset)
        kb_rows = []
        for row in rows:
            mark = "📌 " if row.get("is_pinned") else ""
            kb_rows.append([
                InlineKeyboardButton(
                    text=f"{mark}{row.get('title','Без заголовка')[:50]}",
                    callback_data=f"news:item:{row['id']}:{page}:{status}"
                )
            ])
        kb_rows.extend(_pager(len(all_rows), page, status, page_size))
        text = "Опубликованные" if status == "published" else "Черновики"
        await cq.message.edit_text(f"{text}: {len(all_rows)} шт.", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
        await cq.answer()

    @router.callback_query(F.data.startswith("news:item:"))
    async def news_item(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        page = int(page_raw)
        row = news_service.get_news(news_id)
        if not row:
            await cq.answer("Новость не найдена", show_alert=True)
            return
        media_count = len(row.get("media") or [])
        text = (
            f"<b>{row['title']}</b>\n"
            f"Статус: <b>{row['status']}</b>\n"
            f"Дата публикации: {row.get('published_at') or '—'}\n"
            f"Автор: {row.get('author_name') or '—'}\n"
            f"Медиа: {media_count}\n\n"
            f"{row.get('text') or ''}"
        )
        await cq.message.edit_text(text, reply_markup=_news_item_kb(news_id, row["status"], page, list_status))
        await cq.answer()

    @router.callback_query(F.data.startswith("news:toggle:"))
    async def news_toggle_status(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        row = news_service.get_news(news_id)
        if not row:
            await cq.answer("Новость не найдена", show_alert=True)
            return
        if row["status"] == "published":
            news_service.set_status(news_id, "draft")
            await cq.answer("Переведено в черновик")
            next_status = "draft"
        else:
            news_service.set_status(news_id, "published", published_at=datetime.now(timezone.utc).isoformat())
            await cq.answer("Опубликовано")
            next_status = "published"
        updated = news_service.get_news(news_id)
        await cq.message.edit_text(
            f"<b>{updated['title']}</b>\nСтатус: <b>{next_status}</b>\n\n{updated.get('text') or ''}",
            reply_markup=_news_item_kb(news_id, next_status, int(page_raw), list_status)
        )

    @router.callback_query(F.data.startswith("news:delete:"))
    async def news_delete(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        row = news_service.get_news(news_id)
        if row:
            for media in row.get("media") or []:
                media_service.delete_file_safe(media.get("file_path") or "")
            news_service.delete_news(news_id)
        await cq.answer("Новость удалена")
        await cq.message.edit_text("Новость удалена.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К списку", callback_data=f"news:list:{list_status}:{page_raw}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="news:menu")],
        ]))

    @router.callback_query(F.data.startswith("news:edit:"))
    async def news_edit_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        news_id = (cq.data or "").split(":", 2)[2]
        await state.set_state(NewsEditStates.waiting_title)
        await state.update_data(edit_news_id=news_id)
        await cq.message.answer("Введите новый заголовок.")
        await cq.answer()

    @router.message(NewsEditStates.waiting_title, F.text)
    async def news_edit_title(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        await state.update_data(title=(m.text or "").strip())
        await state.set_state(NewsEditStates.waiting_text)
        await m.answer("Введите новый текст новости.")

    @router.message(NewsEditStates.waiting_text, F.text)
    async def news_edit_text(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        data = await state.get_data()
        edit_news_id = data.get("edit_news_id")
        title = (data.get("title") or "").strip()
        news_service.update_news(edit_news_id, title=title, text=(m.text or "").strip())
        await state.clear()
        row = news_service.get_news(edit_news_id)
        await m.answer(
            f"Новость обновлена:\n<b>{row['title']}</b>",
            reply_markup=_news_item_kb(edit_news_id, row["status"], 0, row["status"])
        )

    @router.callback_query(F.data.startswith("news:addmedia:"))
    async def news_addmedia_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        news_id = (cq.data or "").split(":", 2)[2]
        await state.update_data(upload_news_id=news_id)
        await cq.message.answer("Отправьте фото/видео/GIF как обычное сообщение. Для выхода: /cancel")
        await cq.answer()

    @router.message(F.photo)
    async def news_media_photo(m: Message, state: FSMContext):
        data = await state.get_data()
        news_id = data.get("upload_news_id")
        if not news_id:
            return
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        photo = m.photo[-1]
        path = await media_service.save_telegram_file(m.bot, photo.file_id, "photo.jpg", subdir=news_id)
        news_service.add_media(news_id, "photo", str(path), "image/jpeg")
        await m.answer("Фото добавлено. Можно отправить ещё медиа или /cancel.")

    @router.message(F.video)
    async def news_media_video(m: Message, state: FSMContext):
        data = await state.get_data()
        news_id = data.get("upload_news_id")
        if not news_id:
            return
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        video = m.video
        path = await media_service.save_telegram_file(m.bot, video.file_id, video.file_name or "video.mp4", subdir=news_id)
        news_service.add_media(news_id, "video", str(path), video.mime_type or "video/mp4")
        await m.answer("Видео добавлено. Можно отправить ещё медиа или /cancel.")

    @router.message(F.animation)
    async def news_media_gif(m: Message, state: FSMContext):
        data = await state.get_data()
        news_id = data.get("upload_news_id")
        if not news_id:
            return
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        anim = m.animation
        path = await media_service.save_telegram_file(m.bot, anim.file_id, anim.file_name or "animation.gif", subdir=news_id)
        news_service.add_media(news_id, "gif", str(path), anim.mime_type or "image/gif")
        await m.answer("GIF добавлен. Можно отправить ещё медиа или /cancel.")

    @router.message(F.text == "/cancel")
    async def news_cancel_any(m: Message, state: FSMContext):
        data = await state.get_data()
        if not data.get("upload_news_id") and not await state.get_state():
            return
        await state.clear()
        await m.answer("Действие отменено.", reply_markup=menu_for_user_id(getattr(m.from_user, "id", None)))

    @router.callback_query(F.data.startswith("news:date:"))
    async def news_set_date_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        await state.set_state(NewsEditDateStates.waiting_iso_datetime)
        await state.update_data(edit_date_news_id=news_id, edit_date_page=page_raw, edit_date_status=list_status)
        await cq.message.answer("Введите дату публикации в формате ISO: 2026-03-31T12:00:00+00:00")
        await cq.answer()

    @router.message(NewsEditDateStates.waiting_iso_datetime, F.text)
    async def news_set_date_value(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        data = await state.get_data()
        news_id = data.get("edit_date_news_id")
        dt_text = (m.text or "").strip()
        try:
            datetime.fromisoformat(dt_text)
        except Exception:
            await m.answer("Неверный формат. Пример: 2026-03-31T12:00:00+00:00")
            return
        news_service.update_news(news_id, published_at=dt_text)
        await state.clear()
        await m.answer("Дата публикации обновлена.")

    @router.callback_query(F.data == "news:noop")
    async def news_noop(cq: CallbackQuery):
        await cq.answer()
