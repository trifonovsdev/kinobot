from aiogram import Bot

from app.core.settings import settings

def _valid_format(token: str) -> bool:
    left, sep, right = token.partition(":")
    return bool(sep) and left.isdigit() and bool(right) and " " not in token


# aiogram validates the token *format* eagerly on Bot(...) construction.
# When BOT_TOKEN is unset/placeholder (e.g. local dev without a real bot),
# fall back to a syntactically valid dummy so the app still boots; actual
# API calls will simply fail auth at runtime, which is handled in main.py.
_token = settings.BOT_TOKEN if _valid_format(settings.BOT_TOKEN) else "1:DUMMY_TOKEN_FOR_LOCAL_DEV"

bot = Bot(token=_token)
