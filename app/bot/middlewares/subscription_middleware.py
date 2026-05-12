"""
Subscription middleware — ensures user is subscribed to required channels.
"""

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot
from aiogram.types import (
    TelegramObject, Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

from app.core.settings import settings
from app.repositories.user_repository import user_repository


class SubscriptionMiddleware(BaseMiddleware):
    """Checks channel subscription before processing messages."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        # Skip if no channels configured
        if not settings.CHANNELS:
            return await handler(event, data)

        user_id = None
        chat_id = None
        bot: Bot = data.get("bot")

        if isinstance(event, Message) and event.from_user:
            user_id = event.from_user.id
            chat_id = event.chat.id
        elif isinstance(event, CallbackQuery) and event.from_user:
            user_id = event.from_user.id
            chat_id = event.message.chat.id if event.message else None
            # Allow "check_subs" callback to pass through
            if event.data == "check_subs":
                return await handler(event, data)

        if not user_id or not chat_id or not bot:
            return await handler(event, data)

        # Admins bypass subscription check
        if await user_repository.is_admin(user_id):
            return await handler(event, data)

        # Check all channels
        not_joined = []
        for name, url, cid in settings.CHANNELS:
            if not await self._is_member(bot, cid, user_id, url):
                not_joined.append((name, url))

        if not not_joined:
            return await handler(event, data)

        # Show subscription prompt
        buttons = []
        for name, url in not_joined:
            buttons.append([InlineKeyboardButton(text=f"📢 {name}", url=url)])
        buttons.append([
            InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subs")
        ])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)

        text = (
            "📢 <b>Для использования бота необходимо подписаться на наши каналы.</b>\n\n"
            "После подписки нажмите «Проверить подписку»."
        )

        if isinstance(event, Message):
            await bot.send_message(chat_id, text, reply_markup=kb)
        elif isinstance(event, CallbackQuery):
            await event.answer("Подписка не подтверждена", show_alert=False)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=event.message.message_id,
                    text=text,
                    reply_markup=kb,
                )
            except Exception:
                pass

        return None

    @staticmethod
    async def _is_member(bot: Bot, channel_id: int, user_id: int, url: str = "") -> bool:
        """Check if user is a member of the channel."""
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            status = getattr(member.status, "value", str(member.status)).lower()
            return status in ("member", "administrator", "creator")
        except Exception:
            pass

        # Try by username extracted from URL
        if url:
            try:
                from urllib.parse import urlparse
                path = urlparse(url).path.strip("/")
                if path and not path.startswith("+"):
                    username = f"@{path}" if not path.startswith("@") else path
                    member = await bot.get_chat_member(chat_id=username, user_id=user_id)
                    status = getattr(member.status, "value", str(member.status)).lower()
                    return status in ("member", "administrator", "creator")
            except Exception:
                pass

        return False
