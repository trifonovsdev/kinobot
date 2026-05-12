"""
Socket.IO server for real-time updates.
"""

import socketio

from app.repositories.film_repository import film_repository
from app.repositories.user_repository import user_repository

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")


@sio.event
async def connect(sid, environ):
    """On client connect — send initial data."""
    await emit_films()
    await emit_users()


@sio.event
async def get_films(sid=None):
    """Client requests film list refresh."""
    await emit_films()


@sio.event
async def get_users(sid=None):
    """Client requests user list refresh."""
    await emit_users()


@sio.event
async def delete_film(sid, film_id: int):
    """Delete a film via socket."""
    result = await film_repository.delete(int(film_id))
    if result:
        name = result.get("name", "")
        code = result.get("code", film_id)
        await sio.emit("notification", {
            "message": f'Фильм "{name}" удалён. Код: {code}',
            "type": "info",
        })
        await emit_films()
    else:
        await sio.emit("notification", {
            "message": f"Фильм #{film_id} не найден",
            "type": "error",
        })


async def emit_films() -> None:
    """Broadcast updated film list."""
    films = await film_repository.get_all()
    await sio.emit("update_films", films)
    await sio.emit("films", films)


async def emit_users() -> None:
    """Broadcast updated user list."""
    users = await user_repository.get_all()
    await sio.emit("update_users", users)
    await sio.emit("users", users)


async def notify(event: str, data) -> None:
    """Generic notification emission (used by task_service)."""
    await sio.emit(event, data)
