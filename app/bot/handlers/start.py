"""
/start command handler — registration, referrals, main menu.
"""

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from app.bot.keyboards import main_menu_kb
from app.repositories.user_repository import user_repository

router = Router()


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
        "Я бот для поиска фильмов. Выбери действие из меню ниже:",
        reply_markup=main_menu_kb(),
    )


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
