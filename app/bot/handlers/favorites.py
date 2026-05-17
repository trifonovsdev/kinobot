"""
Favorites and ratings handlers.
"""

from html import escape

from aiogram import Bot, Router, F
from aiogram.types import CallbackQuery, Message

from app.bot.keyboards import main_menu_kb, back_kb
from app.repositories.interaction_repository import interaction_repository
from app.repositories.film_repository import film_repository

router = Router()


@router.callback_query(F.data == "menu:favorites")
async def cb_favorites(callback: CallbackQuery, bot: Bot):
    """Show favorites list."""
    user_id = callback.from_user.id
    await callback.answer()

    films = await interaction_repository.get_favorites(user_id, limit=10)
    if not films:
        await callback.message.edit_text(
            "⭐ <b>Избранное</b>\n\n"
            "У вас пока нет избранных фильмов.\n"
            "Нажмите ⭐ на карточке фильма чтобы добавить!",
            reply_markup=back_kb(),
        )
        return

    lines = ["⭐ <b>Избранное</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        rating = await interaction_repository.get_film_rating(f["id"])
        stars = f"{'⭐' * int(rating['average'])}" if rating["average"] else ""
        lines.append(f"{i}. <b>{name}</b> | <code>{code}</code> {stars}")

    text = "\n".join(lines)

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@router.callback_query(F.data.startswith("fav:add:"))
async def cb_add_favorite(callback: CallbackQuery, bot: Bot):
    """Add film to favorites."""
    film_id = int(callback.data.split(":")[2])
    user_id = callback.from_user.id

    await interaction_repository.add_favorite(user_id, film_id)
    await callback.answer("⭐ Добавлено в избранное!", show_alert=False)


@router.callback_query(F.data.startswith("fav:rm:"))
async def cb_remove_favorite(callback: CallbackQuery, bot: Bot):
    """Remove film from favorites."""
    film_id = int(callback.data.split(":")[2])
    user_id = callback.from_user.id

    await interaction_repository.remove_favorite(user_id, film_id)
    await callback.answer("Удалено из избранного", show_alert=False)


@router.callback_query(F.data.startswith("rate:"))
async def cb_rate_film(callback: CallbackQuery, bot: Bot):
    """Rate a film. Format: rate:{film_id}:{score}"""
    parts = callback.data.split(":")
    film_id = int(parts[1])
    score = int(parts[2])
    user_id = callback.from_user.id

    await interaction_repository.rate_film(user_id, film_id, score)
    await callback.answer(f"{'⭐' * score} Оценка сохранена!", show_alert=False)


@router.callback_query(F.data == "menu:history")
async def cb_history(callback: CallbackQuery, bot: Bot):
    """Show watch history."""
    user_id = callback.from_user.id
    await callback.answer()

    films = await interaction_repository.get_history(user_id, limit=10)
    if not films:
        await callback.message.edit_text(
            "📜 <b>История просмотров</b>\n\n"
            "Пока пусто. Ищите фильмы по коду!",
            reply_markup=back_kb(),
        )
        return

    lines = ["📜 <b>История просмотров</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        lines.append(f"{i}. <b>{name}</b> | <code>{code}</code>")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)


@router.callback_query(F.data == "menu:recommend")
async def cb_recommendations(callback: CallbackQuery, bot: Bot):
    """Show personalized recommendations."""
    user_id = callback.from_user.id
    await callback.answer()

    films = await interaction_repository.get_recommendations(user_id, limit=5)
    if not films:
        await callback.message.edit_text(
            "🎯 <b>Рекомендации</b>\n\nНе удалось подобрать. Попробуйте позже!",
            reply_markup=back_kb(),
        )
        return

    lines = ["🎯 <b>Рекомендации для вас</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        genre = f.get("genre") or ""
        lines.append(f"{i}. <b>{name}</b>\n   🎭 {genre} | Код: <code>{code}</code>")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё рекомендации", callback_data="menu:recommend")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
