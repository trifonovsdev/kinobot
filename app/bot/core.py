"""
Bot setup — registers all routers, middleware, and bot commands.
"""

from aiogram.types import BotCommand

from app.bot.instance import bot, dp
from app.bot.middlewares.ban_middleware import BanMiddleware
from app.bot.middlewares.subscription_middleware import SubscriptionMiddleware
from app.bot.handlers import start, films, profile, inline, favorites


BOT_COMMANDS = [
    BotCommand(command="start", description="Главное меню"),
    BotCommand(command="help", description="Помощь и команды"),
    BotCommand(command="search", description="Поиск фильма по коду"),
    BotCommand(command="pick", description="Подобрать фильм по жанру"),
    BotCommand(command="favorites", description="Избранные фильмы"),
    BotCommand(command="recommend", description="Рекомендации для вас"),
    BotCommand(command="history", description="История просмотров"),
    BotCommand(command="profile", description="Ваш профиль"),
]


def setup_bot() -> None:
    """Register middleware and routers in correct order."""
    # Middleware (applied to all updates)
    dp.message.middleware(BanMiddleware())
    dp.callback_query.middleware(BanMiddleware())
    dp.message.middleware(SubscriptionMiddleware())
    dp.callback_query.middleware(SubscriptionMiddleware())

    # Routers (order matters — first match wins)
    dp.include_router(start.router)
    dp.include_router(profile.router)
    dp.include_router(favorites.router)
    dp.include_router(inline.router)
    dp.include_router(films.router)  # Must be last (has catch-all handler)

    # Register startup hook for setting bot commands
    dp.startup.register(_on_startup)


async def _on_startup() -> None:
    """Set bot commands menu on startup."""
    try:
        await bot.set_my_commands(BOT_COMMANDS)
    except Exception:
        pass
