"""
Film search and genre pick handlers.
"""

import os

from aiogram import Bot, Router, F
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.exceptions import TelegramBadRequest

from app.bot.keyboards import back_kb, genre_kb, film_kb, main_menu_kb
from app.repositories.film_repository import film_repository
from app.web.static import uploads_path

router = Router()


@router.callback_query(F.data == "menu:search")
async def cb_search(callback: CallbackQuery, bot: Bot):
    """Show search prompt."""
    await callback.answer()
    await callback.message.edit_text(
        "🔍 <b>Поиск фильма</b>\n\n"
        "Введите код фильма (5 цифр):",
        reply_markup=back_kb(),
    )


@router.callback_query(F.data == "menu:pick")
async def cb_pick_genre(callback: CallbackQuery, bot: Bot):
    """Show genre selection."""
    await callback.answer()
    kb = await genre_kb()
    await callback.message.edit_text(
        "🎲 <b>Подбор фильма</b>\n\n"
        "Выберите жанр:",
        reply_markup=kb,
    )


@router.callback_query(F.data.startswith("genre:"))
async def cb_genre_selected(callback: CallbackQuery, bot: Bot):
    """Pick a random film by genre."""
    genre = callback.data.split(":", 1)[1]
    film = await film_repository.get_random_by_genre(genre)

    if film:
        await callback.answer()
        await _send_film_card(callback.message.chat.id, film, bot)
    else:
        await callback.answer("Фильмов этого жанра пока нет 😔", show_alert=False)
        kb = await genre_kb()
        await callback.message.edit_text(
            "😔 К сожалению, фильмов этого жанра пока нет.\n\nВыберите другой:",
            reply_markup=kb,
        )


@router.message(F.text.regexp(r"^\d{4,6}$"))
async def handle_film_code(message: Message, bot: Bot):
    """Handle numeric film code input."""
    film = await film_repository.get_by_code_or_id(message.text.strip())
    if film:
        await _send_film_card(message.chat.id, film, bot)
    else:
        await message.answer(
            "❌ Фильм с таким кодом не найден.\n\n"
            "Проверьте код и попробуйте снова:",
            reply_markup=back_kb(),
        )


@router.message()
async def handle_unknown(message: Message, bot: Bot):
    """Catch-all for unrecognized messages."""
    await message.answer(
        "Используйте кнопки меню для навигации 👇",
        reply_markup=main_menu_kb(),
    )


# ============================================================
# Helpers
# ============================================================

async def _send_film_card(chat_id: int, film: dict, bot: Bot) -> None:
    """Send a beautiful film card with poster."""
    MAX_CAPTION = 1024

    name = (film.get("name") or "")[:256]
    genre = (film.get("genre") or "")[:256]
    desc = (film.get("description") or "").strip()
    code = film.get("code") or film.get("id")
    watch_url = film.get("site") or None

    # Build caption
    base = f"🎬 <b>{name}</b>\n🎭 {genre}\n\n"
    footer = f"\n\n🔢 Код: <code>{code}</code>"
    available = MAX_CAPTION - len(base) - len(footer)

    if available > 20:
        desc_crop = desc[:available - 1] + "…" if len(desc) > available else desc
        caption = base + desc_crop + footer
    else:
        caption = base.rstrip() + footer

    # Ensure caption fits
    if len(caption) > MAX_CAPTION:
        caption = caption[:MAX_CAPTION - 1] + "…"

    kb = film_kb(watch_url)

    # Try sending with poster
    if film.get("photo_id"):
        file_path = os.path.join(uploads_path(), film["photo_id"])
        if os.path.exists(file_path):
            try:
                await bot.send_photo(
                    chat_id, FSInputFile(file_path),
                    caption=caption, reply_markup=kb,
                )
                return
            except TelegramBadRequest:
                # Fallback to text-only
                pass

    await bot.send_message(chat_id, caption, reply_markup=kb)
