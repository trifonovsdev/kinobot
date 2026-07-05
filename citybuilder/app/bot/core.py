from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo

from app.core.settings import settings

router = Router(name="citybuilder")


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🏗️ Строить город",
                    web_app=WebAppInfo(url=settings.WEBAPP_URL),
                )
            ]
        ]
    )
    await message.answer(
        "Добро пожаловать в City Builder!\n\n"
        "Начни с маленького посёлка и построй мегаполис. "
        "Жми на кнопку ниже, чтобы открыть игру.",
        reply_markup=keyboard,
    )
