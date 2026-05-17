"""
/start, /help, and shortcut command handlers.
"""

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from app.bot.keyboards import main_menu_kb, back_kb
from app.repositories.user_repository import user_repository

router = Router()

HELP_TEXT = (
    "📖 <b>Справка по боту</b>\n"
    "{'━' * 20}\n\n"
    "<b>Команды:</b>\n"
    "/start — Главное меню\n"
    "/help — Эта справка\n"
    "/search — Поиск по коду\n"
    "/pick — Подбор по жанру\n"
    "/favorites — Избранное\n"
    "/recommend — Рекомендации\n"
    "/history — История\n"
    "/profile — Профиль\n\n"
    "<b>Инлайн-режим:</b>\n"
    "Напишите @имя_бота и название фильма в любом чате!\n\n"
    "<b>Как пользоваться:</b>\n"
    "1. Введите 5-значный код фильма\n"
    "2. Или выберите жанр для случайного подбора\n"
    "3. Добавляйте фильмы в ⭐ Избранное\n"
    "4. Ставьте оценки 1-5 ⭐\n"
    "5. Получайте 🎯 персональные рекомендации"
)


@router.message(Command("start"))
async def cmd_start(message: Message, bot: Bot):
    """Handle /start command — register user and show main menu."""
    user = message.from_user
    if not user:
        return

    # Register user
    await user_repository.register(user.id, user.first_name or "User")

    # Process referral if present
    if message.text and len(message.text.split()) > 1:
        referral_code = message.text.split()[1].strip()
        await user_repository.process_referral(user.id, referral_code)

    # Send welcome menu
    await message.answer(
        "👋 <b>Привет!</b>\n\n"
        "Я бот для поиска фильмов. Выбери действие из меню ниже:\n\n"
        "💡 <i>Совет: введите /help для списка всех команд</i>",
        reply_markup=main_menu_kb(),
    )


@router.message(Command("help"))
async def cmd_help(message: Message, bot: Bot):
    """Show help message."""
    await message.answer(HELP_TEXT, reply_markup=back_kb())


@router.message(Command("search"))
async def cmd_search(message: Message, bot: Bot):
    """Shortcut for search."""
    await message.answer(
        "🔍 <b>Поиск фильма</b>\n\nВведите код фильма (5 цифр):",
        reply_markup=back_kb(),
    )


@router.message(Command("pick"))
async def cmd_pick(message: Message, bot: Bot):
    """Shortcut for genre pick."""
    from app.bot.keyboards import genre_kb
    kb = await genre_kb()
    await message.answer("🎲 <b>Подбор фильма</b>\n\nВыберите жанр:", reply_markup=kb)


@router.message(Command("favorites"))
async def cmd_favorites(message: Message, bot: Bot):
    """Shortcut for favorites."""
    from app.bot.handlers.favorites import cb_favorites
    # Create a fake callback-like flow by sending new message
    from app.repositories.interaction_repository import interaction_repository
    from html import escape

    user_id = message.from_user.id
    films = await interaction_repository.get_favorites(user_id, limit=10)
    if not films:
        await message.answer(
            "⭐ <b>Избранное</b>\n\nУ вас пока нет избранных фильмов.",
            reply_markup=back_kb(),
        )
        return
    lines = ["⭐ <b>Избранное</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        lines.append(f"{i}. <b>{name}</b> | <code>{code}</code>")
    await message.answer("\n".join(lines), reply_markup=back_kb())


@router.message(Command("recommend"))
async def cmd_recommend(message: Message, bot: Bot):
    """Shortcut for recommendations."""
    from app.repositories.interaction_repository import interaction_repository
    from html import escape

    user_id = message.from_user.id
    films = await interaction_repository.get_recommendations(user_id, limit=5)
    if not films:
        await message.answer("🎯 <b>Рекомендации</b>\n\nПока нет данных.", reply_markup=back_kb())
        return
    lines = ["🎯 <b>Рекомендации для вас</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        lines.append(f"{i}. <b>{name}</b> | Код: <code>{code}</code>")
    await message.answer("\n".join(lines), reply_markup=back_kb())


@router.message(Command("history"))
async def cmd_history(message: Message, bot: Bot):
    """Shortcut for watch history."""
    from app.repositories.interaction_repository import interaction_repository
    from html import escape

    user_id = message.from_user.id
    films = await interaction_repository.get_history(user_id, limit=10)
    if not films:
        await message.answer("📜 <b>История</b>\n\nПока пусто.", reply_markup=back_kb())
        return
    lines = ["📜 <b>История просмотров</b>\n"]
    for i, f in enumerate(films, 1):
        name = escape(f.get("name") or "")
        code = f.get("code") or f.get("id")
        lines.append(f"{i}. <b>{name}</b> | <code>{code}</code>")
    await message.answer("\n".join(lines), reply_markup=back_kb())


@router.message(Command("profile"))
async def cmd_profile(message: Message, bot: Bot):
    """Shortcut for profile."""
    from app.bot.handlers.profile import cb_profile as _profile_handler
    # Send profile directly
    from html import escape
    from app.bot.keyboards import profile_kb

    user_id = message.from_user.id
    user = await user_repository.get_by_tg_id(user_id)
    if not user:
        await message.answer("❌ Профиль не найден.", reply_markup=main_menu_kb())
        return
    is_admin = bool(user.get("admin"))
    status = "🌟 Траффер" if is_admin else "👤 Пользователь"
    text = (
        f"<b>👤 Профиль</b>\n{'━' * 20}\n\n"
        f"<b>Имя:</b> {escape(user.get('name') or 'N/A')}\n"
        f"<b>ID:</b> <code>{user['tg_id']}</code>\n"
        f"<b>Статус:</b> {status}\n"
    )
    await message.answer(text, reply_markup=profile_kb(is_admin))


@router.callback_query(F.data == "menu:main")
async def cb_main_menu(callback: CallbackQuery, bot: Bot):
    """Return to main menu."""
    await callback.answer()
    try:
        await callback.message.edit_text(
            "👋 <b>Привет!</b>\n\n"
            "Я бот для поиска фильмов. Выбери действие из меню ниже:",
            reply_markup=main_menu_kb(),
        )
    except Exception:
        await callback.message.answer(
            "👋 <b>Привет!</b>\n\n"
            "Я бот для поиска фильмов. Выбери действие из меню ниже:",
            reply_markup=main_menu_kb(),
        )


@router.callback_query(F.data == "check_subs")
async def cb_check_subs(callback: CallbackQuery, bot: Bot):
    """Re-check subscription after user claims to have subscribed."""
    from app.bot.middlewares.subscription_middleware import SubscriptionMiddleware
    from app.core.settings import settings

    user_id = callback.from_user.id
    all_ok = True

    for name, url, cid in settings.CHANNELS:
        if not await SubscriptionMiddleware._is_member(bot, cid, user_id, url):
            all_ok = False
            break

    if all_ok:
        await callback.answer("✅ Подписка подтверждена!", show_alert=False)
        await callback.message.edit_text(
            "👋 <b>Привет!</b>\n\n"
            "Я бот для поиска фильмов. Выбери действие из меню ниже:",
            reply_markup=main_menu_kb(),
        )
    else:
        await callback.answer(
            "❌ Подписка не обнаружена. Проверьте все каналы.", show_alert=True
        )
