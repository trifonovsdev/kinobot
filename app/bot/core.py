from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import Router
from aiogram.exceptions import TelegramBadRequest
import asyncio
from collections import defaultdict
from typing import List

from app.core.settings import settings
from app.db.sqlite import get_db_connection
from app.web.static import uploads_path

router = Router()

# Сообщение-"контейнер" (меню/контент) — редактируем его при навигации
menu_message: dict[int, int] = {}
# Эфемерные сообщения (карточки фильмов и т.п.), чтобы не спамить
content_messages: dict[int, list[int]] = defaultdict(list)

async def purge_content_messages(chat_id: int, bot: Bot) -> None:
    ids = list(content_messages.get(chat_id, []))
    if not ids:
        return
    for mid in ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass
    content_messages[chat_id] = []

def _main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Поиск фильмов", callback_data="m_search")],
        [InlineKeyboardButton(text="🎲 Подобрать фильм", callback_data="m_pick")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="m_profile")],
    ])

def _back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")]])

def _pick_kb() -> InlineKeyboardMarkup:
    # Сформировать клавиатуру жанров динамически из БД (все уникальные жанры среди активных фильмов)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT genre FROM films
            WHERE activate = 1 AND genre IS NOT NULL AND TRIM(genre) != ''
        """)
        genres_set: set[str] = set()
        for row in cursor.fetchall():
            raw = row[0] or ''
            for part in raw.split(','):
                g = part.strip()
                if g:
                    genres_set.add(g)
        conn.close()
    except Exception:
        genres_set = set()

    genres = sorted(genres_set, key=lambda s: s.lower())
    rows: list[list[InlineKeyboardButton]] = []
    if genres:
        # По 2 кнопки в ряд для компактности
        row: list[InlineKeyboardButton] = []
        for g in genres:
            row.append(InlineKeyboardButton(text=g, callback_data=f"gen:{g}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
    # Добавим кнопку Назад в любом случае
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _edit_menu(chat_id: int, bot: Bot, *, text: str, reply_markup: InlineKeyboardMarkup, disable_web_page_preview: bool = True) -> None:
    mid = menu_message.get(chat_id)
    if mid:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=mid,
                text=text,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
            return
        except TelegramBadRequest as e:
            # Не создаём новое сообщение, если текст не изменился
            if "message is not modified" in str(e).lower():
                try:
                    await bot.edit_message_reply_markup(chat_id=chat_id, message_id=mid, reply_markup=reply_markup)
                except Exception:
                    pass
                return
        except Exception:
            pass
    # если нет сообщения меню — отправим новое и запомним
    m = await bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=disable_web_page_preview)
    menu_message[chat_id] = m.message_id

async def _send_menu(message: Message, bot: Bot, *, text: str, sticker: str | None = None, force_new: bool = False) -> None:
    # Удалим эфемерные сообщения, меню не трогаем
    await purge_content_messages(message.chat.id, bot)
    if sticker:
        try:
            s = await bot.send_sticker(message.chat.id, sticker)
            # Стикер считаем эфемерным
            content_messages[message.chat.id] = [s.message_id]
        except Exception:
            pass
    if force_new:
        try:
            m = await bot.send_message(
                message.chat.id,
                text,
                reply_markup=_main_menu_kb(),
                disable_web_page_preview=True,
            )
            menu_message[message.chat.id] = m.message_id
        except Exception:
            pass
    else:
        await _edit_menu(message.chat.id, bot, text=text, reply_markup=_main_menu_kb())


def is_user_banned(user_id: int) -> bool:
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT banned FROM users WHERE tg_id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return bool(user and user['banned'] == 1)


async def _is_admin_user(user_id: int) -> bool:
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT admin FROM users WHERE tg_id = ?", (user_id,))
    row = cursor.fetchone()
    conn.close()
    return bool(row and row['admin'] == 1)


def _username_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
        if u.netloc not in ("t.me", "telegram.me"):
            return None
        path = (u.path or "").strip("/")
        if not path:
            return None
        # игнорируем join-ссылки вида +abc... — по ним нельзя проверить через API
        if path.startswith("+"):
            return None
        if not path.startswith("@"):  # Bot API ожидает формат @username
            path = "@" + path
        return path
    except Exception:
        return None


async def _is_member_of(bot: Bot, channel: int | str, user_id: int, url: str | None = None) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
        status_obj = getattr(member, 'status', None)
        # aiogram v3: status может быть Enum со свойством .value
        try:
            status = (status_obj.value if hasattr(status_obj, 'value') else str(status_obj)).lower()
        except Exception:
            status = str(status_obj).lower()
        return status in ("member", "administrator", "creator")
    except Exception:
        # Вторая попытка: попробуем по @username, извлечённому из URL
        uname = _username_from_url(url) if url else None
        if uname:
            try:
                member = await bot.get_chat_member(chat_id=uname, user_id=user_id)
                status_obj = getattr(member, 'status', None)
                try:
                    status = (status_obj.value if hasattr(status_obj, 'value') else str(status_obj)).lower()
                except Exception:
                    status = str(status_obj).lower()
                return status in ("member", "administrator", "creator")
            except Exception:
                pass
        return False


async def ensure_subscription(message: Message, bot: Bot, user_id: int | None = None) -> bool:
    # Нет каналов для проверки — пропускаем
    if not settings.CHANNELS:
        return True
    # Трафферам разрешаем без подписки
    uid = user_id if user_id is not None else (message.from_user.id if message.from_user else None)
    if uid is None:
        return False
    if await _is_admin_user(uid):
        return True

    # Проверяем подписку по всем каналам
    not_joined = []
    for name, url, cid in settings.CHANNELS:
        ok = await _is_member_of(bot, cid, uid, url)
        if not ok:
            not_joined.append((name, url))

    if not not_joined:
        return True

    buttons = []
    for name, url in not_joined:
        buttons.append([InlineKeyboardButton(text=f"Подписаться: {name}", url=url)])
    buttons.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subs")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await _edit_menu(
        message.chat.id,
        bot,
        text="Для использования бота необходимо подписаться на наши каналы. Если вы уже подписались, добавьте бота администратором канала, чтобы он мог подтвердить подписку, и нажмите \"Проверить подписку\".",
        reply_markup=kb,
        disable_web_page_preview=True,
    )
    return False


def generate_referral_code() -> str:
    import random, string
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))


def register_user(user) -> None:
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    referral_code = generate_referral_code()
    cursor.execute("INSERT OR IGNORE INTO users (name, tg_id, admin, referral_code) VALUES (?, ?, ?, ?)",
                   (user.first_name, user.id, 0, referral_code))
    conn.commit()
    conn.close()


@router.message(Command("start"))
async def cmd_start(message: Message, bot: Bot):
    if is_user_banned(message.from_user.id):
        return
    register_user(message.from_user)

    # Требование подписки
    if not await ensure_subscription(message, bot):
        return

    # Реферал
    if message.text and len(message.text.split()) > 1:
        referral_code = message.text.split()[1].upper()
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT referral_code, referred_by FROM users WHERE tg_id = ?", (message.from_user.id,))
        user = cursor.fetchone()
        if not (user and user['referred_by']) and not (user and user['referral_code'] == referral_code):
            cursor.execute("SELECT tg_id FROM users WHERE referral_code = ?", (referral_code,))
            referrer = cursor.fetchone()
            if referrer:
                referrer_id = referrer['tg_id']
                cursor.execute("UPDATE users SET referred_by = ? WHERE tg_id = ?", (referral_code, message.from_user.id))
                cursor.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (referrer_id, message.from_user.id))
                conn.commit()
        conn.close()

    await _send_menu(
        message,
        bot,
        sticker="CAACAgIAAxkBAAEGoyhjiMNBrIScwUeaIPWGgs_OjOhi0gAChwIAAladvQpC7XQrQFfQkCsE",
        text="Привет! Я бот для поиска фильмов. Чем могу помочь?",
        force_new=True,
    )


@router.callback_query(F.data == "m_search")
async def cb_search(c: CallbackQuery, bot: Bot):
    if is_user_banned(c.from_user.id):
        return await c.answer("Доступ ограничён")
    await c.answer()
    # Требование подписки
    if not await ensure_subscription(c.message, bot, user_id=c.from_user.id):
        return
    await _edit_menu(c.message.chat.id, bot, text="Введите код фильма:", reply_markup=_back_kb())


@router.callback_query(F.data == "m_pick")
async def cb_pick(c: CallbackQuery, bot: Bot):
    if is_user_banned(c.from_user.id):
        return await c.answer("Доступ ограничён")
    await c.answer()
    if not await ensure_subscription(c.message, bot, user_id=c.from_user.id):
        return
    await _edit_menu(c.message.chat.id, bot, text="Выберите жанр:", reply_markup=_pick_kb())


@router.callback_query(F.data.startswith("gen:"))
async def cb_genre_selected(c: CallbackQuery, bot: Bot):
    if is_user_banned(c.from_user.id):
        return await c.answer("Доступ ограничён")
    # Требование подписки
    if not await ensure_subscription(c.message, bot, user_id=c.from_user.id):
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    g = c.data.split(":", 1)[1].lower().strip()
    # Точное попадание жанра среди запятой-разделённого списка (без ложных совпадений типа Драма/Мелодрама)
    cursor.execute(
        """
        SELECT * FROM films
        WHERE activate = 1
          AND LOWER(',' || REPLACE(COALESCE(genre, ''), ' ', '') || ',') LIKE ?
        ORDER BY RANDOM() LIMIT 1
        """,
        (f"%,{g.replace(' ', '')},%",)
    )
    film = cursor.fetchone()
    conn.close()
    if film:
        await send_film_info(c.message.chat.id, film, bot, context_message=c.message)
    else:
        await _edit_menu(c.message.chat.id, bot, text="К сожалению, фильмов этого жанра пока нет.", reply_markup=_pick_kb())
    await c.answer()


async def profile(message: Message, bot: Bot, user_id: int | None = None):
    uid = user_id if user_id is not None else (message.from_user.id if message.from_user else None)
    if uid is None:
        return
    if is_user_banned(uid):
        return
    # Профиль можно показывать и без подписки — но если нужно, раскомментируйте:
    # if not await ensure_subscription(message, bot):
    #     return
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE tg_id = ?", (uid,))
    user = cursor.fetchone()
    if user:
        from html import escape
        profile_text = (
            f"<b>👤 Профиль</b>\n"
            f"────────────────\n"
            f"<b>ID:</b> <code>{escape(str(user['tg_id']))}</code>\n"
            f"<b>Статус:</b> {'Траффер' if user['admin'] else 'Пользователь'}\n"
        )
        kb_buttons = []
        if user['admin']:
            kb_buttons.append([InlineKeyboardButton(text="🎁 Реферальная система", callback_data="ref_sys")])
        kb_buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
        await _edit_menu(message.chat.id, bot, text=profile_text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await _edit_menu(message.chat.id, bot, text="Произошла ошибка при получении данных профиля.", reply_markup=_main_menu_kb())
    conn.close()


@router.callback_query(F.data == "m_main")
async def cb_main(c: CallbackQuery, bot: Bot):
    await c.answer()
    await _edit_menu(
        c.message.chat.id,
        bot,
        text="Привет! Я бот для поиска фильмов. Чем могу помочь?",
        reply_markup=_main_menu_kb(),
    )


@router.message()
async def handle_message(message: Message, bot: Bot):
    if is_user_banned(message.from_user.id):
        return
    # Требование подписки
    if not await ensure_subscription(message, bot):
        return
    if message.text and message.text.isdigit():
        conn = get_db_connection()
        cursor = conn.cursor()
        # Ищем по коду (основной путь) или по старому числовому id для совместимости
        try:
            num_id = int(message.text)
        except ValueError:
            num_id = -1
        cursor.execute(
            "SELECT * FROM films WHERE activate = 1 AND (code = ? OR id = ?)",
            (message.text, num_id)
        )
        film = cursor.fetchone()
        conn.close()
        if film:
            await send_film_info(message.chat.id, film, bot, context_message=message)
        else:
            await _edit_menu(message.chat.id, bot, text="Фильм с таким кодом не найден. Введите код фильма:", reply_markup=_back_kb())
    else:
        await _edit_menu(message.chat.id, bot, text="Используйте кнопки меню ниже для навигации.", reply_markup=_main_menu_kb())


@router.callback_query(F.data == "check_subs")
async def cb_check_subs(c: CallbackQuery, bot: Bot):
    try:
        ok = await ensure_subscription(c.message, bot, user_id=c.from_user.id)
        if ok:
            await c.answer("Подписка подтверждена!", show_alert=False)
            # Обновим меню без отправки новых сообщений
            await _edit_menu(
                c.message.chat.id,
                bot,
                text="Привет! Я бот для поиска фильмов. Чем могу помочь?",
                reply_markup=_main_menu_kb(),
            )
        else:
            await c.answer("Подписка не обнаружена. Проверьте, что подписались во всех каналах.", show_alert=False)
    except Exception:
        await c.answer("Ошибка проверки. Попробуйте ещё раз.", show_alert=False)


async def send_film_info(chat_id: int, film, bot: Bot, *, context_message: Message | None = None):
    from aiogram.types import FSInputFile
    import os
    # Больше не подставляем ссылку на канал из окружения
    watch_url = film['site'] if film['site'] else None
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="▶️ Смотреть", url=watch_url)]]) if watch_url else None
    code_val = film['code'] if ('code' in film.keys() and film['code']) else film['id']
    # Ограничение Telegram: подпись к медиа максимум 1024 символа.
    MAX_CAPTION = 1024

    def _truncate(text: str, limit: int) -> str:
        text = (text or "").strip()
        if len(text) <= limit:
            return text
        return (text[: max(0, limit - 1)].rstrip()) + "…"

    name = _truncate(str(film['name'] or ''), 256)
    genre = _truncate(str(film['genre'] or ''), 256)
    desc = str(film['description'] or '').strip()

    # Подберём максимально возможную длину описания под лимит 1024
    base_before_desc = f"🎬 Название: {name}\n🎭 Жанр: {genre}\n📝 Описание: "
    base_after_desc = f"\n\n🔢 Код фильма: {code_val}"
    available_for_desc = MAX_CAPTION - len(base_before_desc) - len(base_after_desc)
    if available_for_desc < 0:
        # Если даже без описания выходим за лимит — ужмём поля name/genre и уберём описание
        name = _truncate(name, 200)
        genre = _truncate(genre, 200)
        base_text = f"🎬 Название: {name}\n🎭 Жанр: {genre}{base_after_desc}"
        if len(base_text) > MAX_CAPTION:
            # Дополнительное ужатие на всякий случай
            name = _truncate(name, 160)
            genre = _truncate(genre, 160)
            base_text = f"🎬 Название: {name}\n🎭 Жанр: {genre}{base_after_desc}"
        caption = base_text
    else:
        desc_crop = _truncate(desc, available_for_desc)
        caption = base_before_desc + desc_crop + base_after_desc
    # Чистим старые контент-сообщения (карточки)
    await purge_content_messages(chat_id, bot)
    if film['photo_id']:
        file_path = os.path.join(uploads_path(), film['photo_id'])
        if os.path.exists(file_path):
            try:
                m = await bot.send_photo(chat_id, FSInputFile(file_path), caption=caption, reply_markup=kb)
                content_messages[chat_id] = [m.message_id]
                return
            except TelegramBadRequest as e:
                # Перестраховка на случай превышения лимита — отправим укороченную подпись без описания
                safe_caption = f"🎬 Название: {name}\n🎭 Жанр: {genre}\n\n🔢 Код фильма: {code_val}"
                safe_caption = _truncate(safe_caption, MAX_CAPTION)
                m = await bot.send_photo(chat_id, FSInputFile(file_path), caption=safe_caption, reply_markup=kb)
                content_messages[chat_id] = [m.message_id]
                return
    m = await bot.send_message(chat_id, caption, reply_markup=kb)
    content_messages[chat_id] = [m.message_id]


# ==== Реферальная система ====

async def _render_ref_system(message: Message, bot: Bot, *, user_id: int | None = None) -> None:
    chat_id = message.chat.id
    uid = user_id if user_id is not None else (message.from_user.id if message.from_user else None)
    if uid is None:
        return
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT referral_code FROM users WHERE tg_id = ?", (uid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        await _edit_menu(message.chat.id, bot, text="Пользователь не найден в базе.", reply_markup=_main_menu_kb())
        return
    referral_code = row['referral_code']
    # Если у пользователя по какой-либо причине ещё нет кода — сгенерируем и сохраним
    if not referral_code:
        referral_code = generate_referral_code()
        cursor.execute("UPDATE users SET referral_code = ? WHERE tg_id = ?", (referral_code, uid))
        conn.commit()
    me = await bot.me()
    from html import escape
    ref_link = f"https://t.me/{escape(me.username)}?start={escape(str(referral_code))}"
    # Статистика и последние приглашенные
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (uid,))
    total = cursor.fetchone()[0]
    cursor.execute(
        """
        SELECT u.tg_id, u.name, r.date_referred
        FROM referrals r
        JOIN users u ON u.tg_id = r.referred_id
        WHERE r.referrer_id = ?
        ORDER BY r.date_referred DESC
        LIMIT 10
        """,
        (uid,),
    )
    rows = cursor.fetchall()
    conn.close()
    lines = [
        "<b>🎁 Реферальная система</b>",
        "────────────────",
        f"<b>Ваш код:</b> <code>{escape(str(referral_code))}</code>",
        f"<b>Ссылка:</b> <a href=\"{ref_link}\">{ref_link}</a>",
        f"<b>Всего приглашено:</b> {total}",
        "",
    ]
    if rows:
        lines.append("<b>Последние приглашенные:</b>")
        for r in rows:
            uname = escape(r[1] or "Без имени")
            uid = escape(str(r[0]))
            dt = escape(str(r[2]))
            lines.append(f"• {uname} (<code>{uid}</code>) — <i>{dt}</i>")
    else:
        lines.append("У вас пока нет приглашённых. Поделитесь ссылкой выше!")

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Скопировать реф. ссылку", callback_data="ref_copy")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="ref_refresh")],
        [InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data="m_profile")],
    ])
    await _edit_menu(message.chat.id, bot, text=text, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data == "ref_sys")
async def cb_ref_sys(c: CallbackQuery, bot: Bot):
    uid = c.from_user.id
    if not await _is_admin_user(uid):
        await c.answer("Доступно только трафферам", show_alert=False)
        return
    await c.answer()
    await _render_ref_system(c.message, bot, user_id=uid)

@router.callback_query(F.data == "ref_copy")
async def cb_ref_copy(c: CallbackQuery, bot: Bot):
    """Показать сообщение с реф. ссылкой для копирования (автокопирование в боте недоступно)."""
    try:
        uid = c.from_user.id
        if not await _is_admin_user(uid):
            await c.answer("Доступно только трафферам", show_alert=False)
            return
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT referral_code FROM users WHERE tg_id = ?", (uid,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            await c.answer("Не удалось получить ссылку", show_alert=False)
            return
        referral_code = row['referral_code']
        me = await bot.me()
        ref_link = f"https://t.me/{me.username}?start={referral_code}"
        m = await bot.send_message(c.message.chat.id, f"Ваша реферальная ссылка:\n<code>{ref_link}</code>")
        # Автоматически удалим подсказку через 10 секунд, чтобы не засорять чат
        async def _auto_delete():
            await asyncio.sleep(10)
            try:
                await bot.delete_message(c.message.chat.id, m.message_id)
            except Exception:
                pass
        asyncio.create_task(_auto_delete())
        await c.answer("Ссылка показана сообщением — скопируйте и отправьте друзьям", show_alert=False)
    except Exception:
        await c.answer("Ошибка. Попробуйте ещё раз", show_alert=False)

@router.callback_query(F.data == "ref_refresh")
async def cb_ref_refresh(c: CallbackQuery, bot: Bot):
    uid = c.from_user.id
    if not await _is_admin_user(uid):
        await c.answer("Доступно только трафферам", show_alert=False)
        return
    await c.answer("Обновлено")
    await _render_ref_system(c.message, bot, user_id=uid)


@router.callback_query(F.data == "m_profile")
async def cb_profile(c: CallbackQuery, bot: Bot):
    await c.answer()
    await profile(c.message, bot, user_id=c.from_user.id)
