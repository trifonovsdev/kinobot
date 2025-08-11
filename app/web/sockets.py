import socketio
from app.db.sqlite import get_db_connection

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
sio_app = socketio.ASGIApp(sio)


@sio.event
async def connect(sid, environ):
    await get_films()
    await get_users()


@sio.event
async def get_films(sid=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM films ORDER BY id DESC")
    films = [dict(row) for row in cursor.fetchall()]
    conn.close()
    await sio.emit('update_films', films)
    await sio.emit('films', films)


@sio.event
async def get_users(sid=None):
    conn = get_db_connection('users.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users ORDER BY id DESC")
    users = [dict(row) for row in cursor.fetchall()]
    conn.close()
    await sio.emit('update_users', users)
    await sio.emit('users', users)


@sio.event
async def delete_film(sid, id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, code FROM films WHERE id = ?", (id,))
    film = cursor.fetchone()
    if film:
        film_name = film['name']
        film_code = film['code'] if 'code' in film.keys() else None
        cursor.execute("DELETE FROM films WHERE id = ?", (id,))
        conn.commit()
        # Если таблица фильмов стала пустой, сбрасываем автонумерацию ID
        try:
            cursor.execute("SELECT COUNT(*) FROM films")
            cnt = cursor.fetchone()[0]
            if cnt == 0:
                cursor.execute("DELETE FROM sqlite_sequence WHERE name='films'")
                conn.commit()
        except Exception:
            # На случай необычной конфигурации SQLite просто игнорируем сбой сброса последовательности
            pass
        conn.close()
        code_part = f" Код: {film_code}" if film_code else f" ID: {id}"
        await sio.emit('notification', {'message': f'Фильм "{film_name}" удален.{code_part}', 'type': 'info'})
        await get_films()
    else:
        conn.close()
        await sio.emit('notification', {'message': f'Фильм с кодом {id} не найден', 'type': 'error'})
