"""
Keyboard factories for the bot.
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.repositories.film_repository import film_repository


def main_menu_kb() -> InlineKeyboardMarkup:
    """Main menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Поиск фильмов", callback_data="menu:search")],
        [InlineKeyboardButton(text="🎲 Подобрать фильм", callback_data="menu:pick")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="menu:profile")],
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


def film_kb(watch_url: str | None = None) -> InlineKeyboardMarkup | None:
    """Film card keyboard with watch button."""
    if not watch_url:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Смотреть", url=watch_url)]
    ])


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
