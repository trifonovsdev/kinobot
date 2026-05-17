"""
Inline mode — search films from any chat via @botname query.
"""

from aiogram import Bot, Router
from aiogram.types import (
    InlineQuery, InlineQueryResultArticle, InputTextMessageContent,
)

from app.repositories.film_repository import film_repository

router = Router()


@router.inline_query()
async def inline_search(query: InlineQuery, bot: Bot):
    """Handle inline search queries."""
    text = (query.query or "").strip()
    results = []

    if len(text) < 2:
        # Show hint
        results.append(
            InlineQueryResultArticle(
                id="hint",
                title="Введите название или код фильма",
                description="Минимум 2 символа для поиска",
                input_message_content=InputTextMessageContent(
                    message_text="🎬 Используйте инлайн-режим: @botname название фильма"
                ),
            )
        )
        await query.answer(results, cache_time=5, is_personal=True)
        return

    # Search films
    films = await film_repository.search(query=text)

    for film in films[:15]:  # Telegram limit: 50, but 15 is enough
        name = film.get("name") or "Без названия"
        genre = film.get("genre") or ""
        code = film.get("code") or film.get("id")
        desc = (film.get("description") or "")[:100]
        watch_url = film.get("site") or ""

        message_text = (
            f"🎬 <b>{name}</b>\n"
            f"🎭 {genre}\n\n"
            f"{desc}{'…' if len(film.get('description') or '') > 100 else ''}\n\n"
            f"🔢 Код: <code>{code}</code>"
        )
        if watch_url:
            message_text += f"\n▶️ <a href=\"{watch_url}\">Смотреть</a>"

        results.append(
            InlineQueryResultArticle(
                id=str(film["id"]),
                title=name,
                description=f"{genre} | Код: {code}",
                input_message_content=InputTextMessageContent(
                    message_text=message_text,
                    parse_mode="HTML",
                ),
            )
        )

    if not results:
        results.append(
            InlineQueryResultArticle(
                id="not_found",
                title="Ничего не найдено",
                description=f"По запросу «{text}» фильмов не найдено",
                input_message_content=InputTextMessageContent(
                    message_text=f"❌ По запросу «{text}» ничего не найдено"
                ),
            )
        )

    await query.answer(results, cache_time=10, is_personal=False)
