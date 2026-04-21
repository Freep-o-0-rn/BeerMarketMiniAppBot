from __future__ import annotations

import logging
from html import escape
from datetime import datetime, timezone
from pathlib import Path

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

NEWS_MANAGE_BTN_TEXT = "📰 Управление новостями"
logger = logging.getLogger(__name__)


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
        [InlineKeyboardButton(text="🔄 Синхронизировать ленту", callback_data="news:sync")],
        [InlineKeyboardButton(text="🗑 Удалить все черновики", callback_data="news:purgeask:draft")],
        [InlineKeyboardButton(text="🧹 Удалить все опубликованные", callback_data="news:purgeask:published")],
    ])


def _news_item_kb(news_id: str, status: str, page: int, list_status: str) -> InlineKeyboardMarkup:
    publish_text = "✅ Опубликовать" if status != "published" else "📝 В черновик"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить заголовок", callback_data=f"news:edittitle:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="📝 Изменить текст", callback_data=f"news:edittext:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="🖼 Редактор медиа", callback_data=f"news:mediaeditor:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="👀 Предпросмотр", callback_data=f"news:preview:{news_id}")],
        [InlineKeyboardButton(text=publish_text, callback_data=f"news:toggle:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="🕒 Сменить дату публикации", callback_data=f"news:date:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"news:delete:{news_id}:{page}:{list_status}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"news:list:{list_status}:{page}")],
    ])

def _news_media_finish_kb(news_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Завершить и опубликовать", callback_data=f"news:finishmedia:publish:{news_id}")],
        [InlineKeyboardButton(text="💾 Завершить (оставить черновик)", callback_data=f"news:finishmedia:draft:{news_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="news:finishmedia:cancel")],
    ])

def _news_media_editor_kb(news_id: str, page: int, list_status: str, media: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="➕ Добавить медиа", callback_data=f"news:addmedia:{news_id}")]
    ]
    for idx, item in enumerate(media, start=1):
        media_type = str(item.get("media_type") or "media")
        media_id = str(item.get("id") or "")
        rows.append([
            InlineKeyboardButton(
                text=f"❌ Удалить #{idx} ({media_type})",
                callback_data=f"news:dm:{media_id}:{page}:{list_status}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ К карточке новости", callback_data=f"news:item:{news_id}:{page}:{list_status}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _news_item_text(row: dict) -> str:
    media_count = len(row.get("media") or [])
    title = str(row.get("title") or "Без заголовка")
    status = str(row.get("status") or "—")
    published_at = str(row.get("published_at") or "—")
    author_name = str(row.get("author_name") or "—")
    body = str(row.get("text") or "")
    return (
        f"{title}\n"
        f"Статус: {status}\n"
        f"Дата публикации: {published_at}\n"
        f"Автор: {author_name}\n"
        f"Медиа: {media_count}\n\n"
        f"{body}"
    )

async def _safe_edit_text(message: Message, text: str, reply_markup: InlineKeyboardMarkup) -> bool:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as error:
        if "message is not modified" in str(error).lower():
            return False
        raise

async def _edit_news_item_plain_text(message: Message, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    """
    Рендер карточки новости без parse_mode.
    В aiogram bot-level parse_mode может подмешиваться через DefaultBotProperties,
    поэтому принудительно передаём entities=[].
    """
    await message.edit_text(text, entities=[], reply_markup=reply_markup)


async def _render_news_item_card(cq: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup) -> bool:
    """
    Безопасный рендер карточки новости:
    - сначала пытаемся обновить текущее сообщение
    - при TelegramBadRequest не оставляем пользователя в "вечной загрузке"
    """
    try:
        await _edit_news_item_plain_text(cq.message, text, reply_markup)
        return True
    except TelegramBadRequest:
        logger.exception("Не удалось отрисовать карточку новости через edit_text")
        fallback_text = text.replace("<", "‹").replace(">", "›")
        await cq.message.answer(
            "Не удалось обновить текущую карточку, отправляю отдельным сообщением.\n\n"
            f"{fallback_text}",
            entities=[],
            reply_markup=reply_markup,
        )
        return False

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

    async def render_news_list(cq: CallbackQuery, status: str, page: int) -> None:
        offset = page * page_size
        all_rows = news_service.list_news(status=status, limit=500, offset=0)
        rows = news_service.list_news(status=status, limit=page_size, offset=offset)
        kb_rows = []
        for row in rows:
            mark = "📌 " if row.get("is_pinned") else ""
            kb_rows.append([
                InlineKeyboardButton(
                    text=f"{mark}{row.get('title', 'Без заголовка')[:50]}",
                    callback_data=f"news:item:{row['id']}:{page}:{status}"
                )
            ])
        kb_rows.extend(_pager(len(all_rows), page, status, page_size))
        text = "Опубликованные" if status == "published" else "Черновики"
        await cq.message.edit_text(
            f"{text}: {len(all_rows)} шт.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
        )

    async def send_news_preview(m: Message, row: dict) -> None:
        title = str(row.get("title") or "Без заголовка")
        body = str(row.get("text") or "")
        caption = f"{title}\n\n{body}".strip()
        media = row.get("media") or []
        if not media:
            await m.answer(caption or "Пустая новость.")
            return
        sent_with_caption = False
        for item in media:
            file_path = str(item.get("file_path") or "")
            if not file_path:
                continue
            path = Path(file_path)
            if not path.exists():
                logger.warning("Файл медиа не найден для превью: %s", file_path)
                continue
            media_type = str(item.get("media_type") or "")
            input_file = FSInputFile(path)
            current_caption = caption if not sent_with_caption else None
            if media_type == "video":
                await m.answer_video(video=input_file, caption=current_caption)
            elif media_type == "gif":
                await m.answer_animation(animation=input_file, caption=current_caption)
            else:
                await m.answer_photo(photo=input_file, caption=current_caption)
            sent_with_caption = sent_with_caption or bool(current_caption)
        if not sent_with_caption and caption:
            await m.answer(caption)

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

    @router.callback_query(F.data == "news:sync")
    async def news_sync(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        published_count = news_service.sync_static_files()
        updated = await _safe_edit_text(
            cq.message,
            f"Синхронизация выполнена.\nОпубликовано новостей: {published_count}",
            reply_markup=_news_menu_kb(),
        )
        if updated:
            await cq.answer("Лента синхронизирована")
        else:
            await cq.answer("Данные уже актуальны")
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
            f"Черновик создан: {escape(str(item.get('title') or 'Без заголовка'))}\nID: {news_id}",
            reply_markup=_news_item_kb(news_id, item.get("status", "draft"), 0, "draft")
        )

    @router.callback_query(F.data.startswith("news:list:"))
    async def news_list(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, status, page_raw = (cq.data or "").split(":", 3)
        page = max(0, int(page_raw))
        await render_news_list(cq, status, page)
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
        text = _news_item_text(row)
        rendered = await _render_news_item_card(
            cq,
            text,
            _news_item_kb(news_id, row["status"], page, list_status),
        )
        await cq.answer("Открыто" if rendered else "Открыто отдельным сообщением", show_alert=not rendered)

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
            next_status = "draft"
            status_message = "Переведено в черновик"
        else:
            news_service.set_status(news_id, "published", published_at=datetime.now(timezone.utc).isoformat())
            next_status = "published"
            status_message = "Опубликовано"
        updated = news_service.get_news(news_id)
        rendered = await _render_news_item_card(
            cq,
            _news_item_text(updated),
            _news_item_kb(news_id, next_status, int(page_raw), list_status),
        )
        await cq.answer(
            status_message if rendered else f"{status_message}. Карточка отправлена отдельным сообщением.",
            show_alert=not rendered,
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
        await render_news_list(cq, list_status, max(0, int(page_raw)))

    @router.callback_query(F.data.startswith("news:purgeask:"))
    async def news_purge_ask(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        status = (cq.data or "").split(":", 2)[2]
        if status not in {"draft", "published"}:
            await cq.answer("Некорректный статус", show_alert=True)
            return
        status_title = "черновики" if status == "draft" else "опубликованные"
        await cq.message.edit_text(
            f"Подтвердите удаление всех элементов в разделе: <b>{status_title}</b>.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Продолжить", callback_data=f"news:purgeconfirm:{status}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="news:menu")],
            ])
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("news:purgeconfirm:"))
    async def news_purge_confirm(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        status = (cq.data or "").split(":", 2)[2]
        if status not in {"draft", "published"}:
            await cq.answer("Некорректный статус", show_alert=True)
            return
        status_title = "черновики" if status == "draft" else "опубликованные"
        await cq.message.edit_text(
            f"⚠️ ТОЧНО УДАЛИТЬ ВСЕ новости в разделе <b>{status_title}</b>?\n"
            f"Действие необратимо.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑 ТОЧНО удалить все", callback_data=f"news:purge:{status}")],
                [InlineKeyboardButton(text="↩️ Назад", callback_data=f"news:purgeask:{status}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="news:menu")],
            ])
        )
        await cq.answer()
    @router.callback_query(F.data.startswith("news:purge:"))
    async def news_purge(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        status = (cq.data or "").split(":", 2)[2]
        if status not in {"draft", "published"}:
            await cq.answer("Некорректный статус", show_alert=True)
            return
        rows = news_service.list_news(status=status, limit=500, offset=0)
        for row in rows:
            for media in row.get("media") or []:
                media_service.delete_file_safe(media.get("file_path") or "")
        deleted = news_service.delete_news_by_status(status)
        status_title = "черновиков" if status == "draft" else "опубликованных"
        await cq.message.edit_text(
            f"Удалено {deleted} {status_title}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬆️ Меню новостей", callback_data="news:menu")],
                [InlineKeyboardButton(text="📋 К черновикам", callback_data="news:list:draft:0")],
                [InlineKeyboardButton(text="📰 К опубликованным", callback_data="news:list:published:0")],
            ])
        )
        await cq.answer("Удаление завершено")

    @router.callback_query(F.data.startswith("news:edittitle:"))
    async def news_edit_title_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        await state.set_state(NewsEditStates.waiting_title)
        await state.update_data(edit_news_id=news_id, edit_page=page_raw, edit_status=list_status)
        await cq.message.answer("Введите новый заголовок.")
        await cq.answer()

    @router.message(NewsEditStates.waiting_title, F.text)
    async def news_edit_title(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        data = await state.get_data()
        edit_news_id = data.get("edit_news_id")
        edit_page = int(data.get("edit_page") or 0)
        edit_status = str(data.get("edit_status") or "draft")
        if not edit_news_id:
            await state.clear()
            await m.answer("Новость не найдена.")
            return
        news_service.update_news(edit_news_id, title=(m.text or "").strip())
        await state.clear()
        row = news_service.get_news(edit_news_id)
        if not row:
            await m.answer("Новость не найдена.")
            return
        await m.answer(
            f"Заголовок обновлён:\n<b>{escape(str(row.get('title') or 'Без заголовка'))}</b>",
            reply_markup=_news_item_kb(edit_news_id, row["status"], edit_page, edit_status),
        )

    @router.callback_query(F.data.startswith("news:edittext:"))
    async def news_edit_text_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        await state.set_state(NewsEditStates.waiting_text)
        await state.update_data(edit_news_id=news_id, edit_page=page_raw, edit_status=list_status)
        await cq.message.answer("Введите новый текст новости.")
        await cq.answer()

    @router.message(NewsEditStates.waiting_text, F.text)
    async def news_edit_text(m: Message, state: FSMContext):
        if not await ensure_message_access(m, "news.manage", state=state):
            return
        data = await state.get_data()
        edit_news_id = data.get("edit_news_id")
        edit_page = int(data.get("edit_page") or 0)
        edit_status = str(data.get("edit_status") or "draft")
        if not edit_news_id:
            await state.clear()
            await m.answer("Новость не найдена.")
            return
        news_service.update_news(edit_news_id, text=(m.text or "").strip())
        await state.clear()
        row = news_service.get_news(edit_news_id)
        if not row:
            await m.answer("Новость не найдена.")
            return
        await m.answer(
            f"Текст новости обновлён:\n<b>{escape(str(row.get('title') or 'Без заголовка'))}</b>",
            reply_markup=_news_item_kb(edit_news_id, row["status"], edit_page, edit_status),
        )

    @router.callback_query(F.data.startswith("news:addmedia:"))
    async def news_addmedia_prompt(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        news_id = (cq.data or "").split(":", 2)[2]
        await state.update_data(upload_news_id=news_id)
        await cq.message.answer(
            "Отправьте фото/видео/GIF как обычное сообщение.\n"
            "Когда закончите — нажмите кнопку ниже.",
            reply_markup=_news_media_finish_kb(news_id),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("news:mediaeditor:"))
    async def news_media_editor(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, news_id, page_raw, list_status = (cq.data or "").split(":", 4)
        row = news_service.get_news(news_id)
        if not row:
            await cq.answer("Новость не найдена", show_alert=True)
            return
        media = row.get("media") or []
        media_lines = "\n".join(
            f"{idx}. {item.get('media_type', 'media')} — {Path(str(item.get('file_path') or '')).name}"
            for idx, item in enumerate(media, start=1)
        ) or "Медиа пока не добавлены."
        await cq.message.edit_text(
            f"Редактор медиа для «{row.get('title') or 'Без заголовка'}»\n\n{media_lines}",
            reply_markup=_news_media_editor_kb(news_id, int(page_raw), list_status, media),
        )
        await cq.answer()

    @router.callback_query(F.data.startswith("news:dm:"))
    async def news_delete_media(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        _, _, media_id, page_raw, list_status = (cq.data or "").split(":", 4)
        media = news_service.get_media(media_id)
        if not media:
            await cq.answer("Медиа уже удалено", show_alert=True)
            return
        news_id = str(media.get("news_id") or "")
        file_path = str(media.get("file_path") or "")
        news_service.delete_media(media_id)
        media_service.delete_file_safe(file_path)
        row = news_service.get_news(news_id)
        if not row:
            await cq.answer("Новость не найдена", show_alert=True)
            return
        media_list = row.get("media") or []
        media_lines = "\n".join(
            f"{idx}. {item.get('media_type', 'media')} — {Path(str(item.get('file_path') or '')).name}"
            for idx, item in enumerate(media_list, start=1)
        ) or "Медиа пока не добавлены."
        await cq.message.edit_text(
            f"Редактор медиа для «{row.get('title') or 'Без заголовка'}»\n\n{media_lines}",
            reply_markup=_news_media_editor_kb(news_id, int(page_raw), list_status, media_list),
        )
        await cq.answer("Медиа удалено")

    @router.callback_query(F.data.startswith("news:preview:"))
    async def news_preview(cq: CallbackQuery):
        if not await ensure_callback_access(cq, "news.manage"):
            return
        news_id = (cq.data or "").split(":", 2)[2]
        row = news_service.get_news(news_id)
        if not row:
            await cq.answer("Новость не найдена", show_alert=True)
            return
        await send_news_preview(cq.message, row)
        await cq.answer("Предпросмотр отправлен")

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
        await m.answer("Фото добавлено. Можно отправить ещё медиа или завершить.", reply_markup=_news_media_finish_kb(news_id))

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
        await m.answer("Видео добавлено. Можно отправить ещё медиа или завершить.", reply_markup=_news_media_finish_kb(news_id))

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
        await m.answer("GIF добавлен. Можно отправить ещё медиа или завершить.", reply_markup=_news_media_finish_kb(news_id))

    @router.callback_query(F.data.startswith("news:finishmedia:"))
    async def news_finish_media(cq: CallbackQuery, state: FSMContext):
        if not await ensure_callback_access(cq, "news.manage", state=state):
            return
        parts = (cq.data or "").split(":")
        action = parts[2] if len(parts) > 2 else ""
        news_id = parts[3] if len(parts) > 3 else ""
        data = await state.get_data()
        upload_news_id = data.get("upload_news_id")
        if action == "cancel":
            await state.clear()
            await cq.message.edit_text("Добавление медиа отменено.", reply_markup=_news_menu_kb())
            await cq.answer()
            return
        if upload_news_id and upload_news_id != news_id:
            news_id = upload_news_id
        row = news_service.get_news(news_id)
        if not row:
            await state.clear()
            await cq.answer("Новость не найдена", show_alert=True)
            return
        if action == "publish":
            news_service.set_status(news_id, "published", published_at=datetime.now(timezone.utc).isoformat())
            row = news_service.get_news(news_id) or row
            await cq.message.edit_text(
                f"Новость опубликована:\n<b>{escape(str(row.get('title') or 'Без заголовка'))}</b>",
                reply_markup=_news_item_kb(news_id, "published", 0, "published"),
            )
            await state.clear()
            await cq.answer("Опубликовано")
            return
        await state.clear()
        await cq.message.edit_text(
            f"Добавление медиа завершено.\n<b>{escape(str(row.get('title') or 'Без заголовка'))}</b> осталось в черновиках.",
            reply_markup=_news_item_kb(news_id, row.get("status", "draft"), 0, "draft"),
        )
        await cq.answer()

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
