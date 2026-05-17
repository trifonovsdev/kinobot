"""
Keyboard factories for the bot.
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.repositories.film_repository import film_repository


def main_menu_kb() -> InlineKeyboardMarkup:
    """Main menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Поиск фильмов", callback_data="menu:search"),
         InlineKeyboardButton(text="🎲 Подобрать", callback_data="menu:pick")],
        [InlineKeyboardButton(text="⭐ Избранное", callback_data="menu:favorites"),
         InlineKeyboardButton(text="🎯 Для вас", callback_data="menu:recommend")],
        [InlineKeyboardButton(text="📜 История", callback_data="menu:history"),
         InlineKeyboardButton(text="👤 Профиль", callback_data="menu:profile")],
    ])


def back_kb() -> InlineKeyboardMarkup:
    """Simple back button."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")]
    ])


async def genre_kb() -> InlineKeyboardMarkup:
    """Dynamic genre selection keyboard."""
    genres = await film_repository.get_active_genres()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for g in genres:
        row.append(InlineKeyboardButton(text=g, callback_data=f"genre:{g}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def film_kb(watch_url: str | None = None, film_id: int | None = None, is_fav: bool = False) -> InlineKeyboardMarkup:
    """Film card keyboard with watch, favorite, and rate buttons."""
    rows = []
    if watch_url:
        rows.append([InlineKeyboardButton(text="▶️ Смотреть", url=watch_url)])
    action_row = []
    if film_id:
        if is_fav:
            action_row.append(InlineKeyboardButton(text="💔 Убрать", callback_data=f"fav:rm:{film_id}"))
        else:
            action_row.append(InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav:add:{film_id}"))
        action_row.append(InlineKeyboardButton(text="⭐1-5", callback_data=f"rate:show:{film_id}"))
    if action_row:
        rows.append(action_row)
    # Rating row shortcut
    if film_id:
        rows.append([
            InlineKeyboardButton(text="1⭐", callback_data=f"rate:{film_id}:1"),
            InlineKeyboardButton(text="2⭐", callback_data=f"rate:{film_id}:2"),
            InlineKeyboardButton(text="3⭐", callback_data=f"rate:{film_id}:3"),
            InlineKeyboardButton(text="4⭐", callback_data=f"rate:{film_id}:4"),
            InlineKeyboardButton(text="5⭐", callback_data=f"rate:{film_id}:5"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


def profile_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Profile keyboard."""
    buttons = []
    if is_admin:
        buttons.append([InlineKeyboardButton(text="🎁 Реферальная система", callback_data="ref:system")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def referral_kb() -> InlineKeyboardMarkup:
    """Referral system keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Скопировать ссылку", callback_data="ref:copy")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="ref:refresh")],
        [InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data="menu:profile")],
    ])
