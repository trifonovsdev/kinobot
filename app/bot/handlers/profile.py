"""
Profile and referral system handlers.
"""

import asyncio
from html import escape

from aiogram import Bot, Router, F
from aiogram.types import CallbackQuery

from app.bot.keyboards import profile_kb, referral_kb, main_menu_kb
from app.repositories.user_repository import user_repository

router = Router()


@router.callback_query(F.data == "menu:profile")
async def cb_profile(callback: CallbackQuery, bot: Bot):
    """Show user profile."""
    user_id = callback.from_user.id
    user = await user_repository.get_by_tg_id(user_id)
    await callback.answer()

    if not user:
        await callback.message.edit_text(
            "❌ Профиль не найден.", reply_markup=main_menu_kb()
        )
        return

    is_admin = bool(user.get("admin"))
    status = "🌟 Траффер" if is_admin else "👤 Пользователь"

    text = (
        f"<b>👤 Профиль</b>\n"
        f"{'━' * 20}\n\n"
        f"<b>Имя:</b> {escape(user.get('name') or 'N/A')}\n"
        f"<b>ID:</b> <code>{user['tg_id']}</code>\n"
        f"<b>Статус:</b> {status}\n"
    )

    await callback.message.edit_text(text, reply_markup=profile_kb(is_admin))


@router.callback_query(F.data == "ref:system")
async def cb_referral_system(callback: CallbackQuery, bot: Bot):
    """Show referral system details."""
    user_id = callback.from_user.id

    if not await user_repository.is_admin(user_id):
        await callback.answer("Доступно только трафферам", show_alert=False)
        return

    await callback.answer()
    await _render_referral(callback, bot, user_id)


@router.callback_query(F.data == "ref:refresh")
async def cb_referral_refresh(callback: CallbackQuery, bot: Bot):
    """Refresh referral stats."""
    user_id = callback.from_user.id
    if not await user_repository.is_admin(user_id):
        await callback.answer("Доступно только трафферам", show_alert=False)
        return
    await callback.answer("🔄 Обновлено")
    await _render_referral(callback, bot, user_id)


@router.callback_query(F.data == "ref:copy")
async def cb_referral_copy(callback: CallbackQuery, bot: Bot):
    """Show referral link for easy copying."""
    user_id = callback.from_user.id
    if not await user_repository.is_admin(user_id):
        await callback.answer("Доступно только трафферам", show_alert=False)
        return

    stats = await user_repository.get_referral_stats(user_id)
    me = await bot.me()
    ref_link = f"https://t.me/{me.username}?start={stats['code']}"

    msg = await bot.send_message(
        callback.message.chat.id,
        f"📋 Ваша реферальная ссылка:\n\n<code>{ref_link}</code>\n\n"
        f"<i>Сообщение удалится через 15 секунд</i>",
    )
    await callback.answer("Ссылка показана — скопируйте!", show_alert=False)

    # Auto-delete after 15s
    async def _cleanup():
        await asyncio.sleep(15)
        try:
            await bot.delete_message(callback.message.chat.id, msg.message_id)
        except Exception:
            pass

    asyncio.create_task(_cleanup())


async def _render_referral(callback: CallbackQuery, bot: Bot, user_id: int) -> None:
    """Render referral system message."""
    stats = await user_repository.get_referral_stats(user_id)
    me = await bot.me()
    ref_link = f"https://t.me/{me.username}?start={stats['code']}"

    lines = [
        "<b>🎁 Реферальная система</b>",
        f"{'━' * 20}\n",
        f"<b>Ваш код:</b> <code>{escape(stats['code'])}</code>",
        f"<b>Ссылка:</b> <a href=\"{ref_link}\">{ref_link}</a>",
        f"<b>Всего приглашено:</b> {stats['total']}",
        "",
    ]

    if stats["recent"]:
        lines.append("<b>Последние приглашённые:</b>")
        for r in stats["recent"]:
            uname = escape(str(r.get("name") or "Без имени"))
            uid = r.get("tg_id", "")
            dt = str(r.get("date_referred", ""))[:16]
            lines.append(f"• {uname} (<code>{uid}</code>) — <i>{dt}</i>")
    else:
        lines.append("📭 Пока нет приглашённых. Поделитесь ссылкой!")

    text = "\n".join(lines)
    await callback.message.edit_text(
        text, reply_markup=referral_kb(), disable_web_page_preview=True,
    )
