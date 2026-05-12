"""
Bot setup — registers all routers and middleware.
"""

from app.bot.instance import dp
from app.bot.middlewares.ban_middleware import BanMiddleware
from app.bot.middlewares.subscription_middleware import SubscriptionMiddleware
from app.bot.handlers import start, films, profile


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
    dp.include_router(films.router)  # Must be last (has catch-all handler)
