"""
Ban middleware — silently drops all messages/callbacks from banned users.
"""

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

from app.repositories.user_repository import user_repository


class BanMiddleware(BaseMiddleware):
    """Drops updates from banned users before they reach handlers."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user_id = None
        if isinstance(event, Message) and event.from_user:
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery) and event.from_user:
            user_id = event.from_user.id

        if user_id and await user_repository.is_banned(user_id):
            # Silently ignore banned users
            if isinstance(event, CallbackQuery):
                await event.answer("Доступ ограничён", show_alert=False)
            return None

        return await handler(event, data)
