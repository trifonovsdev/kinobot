import sqlite3
import random
from typing import Any, Iterable, List


def get_db_connection(db_name: str = 'films.db') -> sqlite3.Connection:
    conn = sqlite3.connect(db_name, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn_films = get_db_connection('films.db')
    cursor_films = conn_films.cursor()
    cursor_films.execute("""CREATE TABLE IF NOT EXISTS films(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        photo_status INTEGER,
        photo_id TEXT,
        activate INTEGER,
        genre TEXT,
        site TEXT
    )""")
    # Добавляем столбец code, если его нет
    try:
        cursor_films.execute("ALTER TABLE films ADD COLUMN code TEXT")
        conn_films.commit()
    except sqlite3.OperationalError:
        # Вероятно, столбец уже существует
        pass
    # Уникальный индекс для кода
    cursor_films.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_films_code ON films(code)")
    conn_films.commit()
    # Добавляем столбцы external_source / external_id
    try:
        cursor_films.execute("ALTER TABLE films ADD COLUMN external_source TEXT")
        conn_films.commit()
    except sqlite3.OperationalError:
        pass
    try:
        cursor_films.execute("ALTER TABLE films ADD COLUMN external_id TEXT")
        conn_films.commit()
    except sqlite3.OperationalError:
        pass
    # Уникальный индекс для внешней пары (источник, внешний id)
    cursor_films.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_films_external ON films(external_source, external_id)")
    conn_films.commit()
    
    # --- Normalized genres tables ---
    cursor_films.execute(
        """
        CREATE TABLE IF NOT EXISTS genres(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """
    )
    cursor_films.execute(
        """
        CREATE TABLE IF NOT EXISTS film_genres(
            film_id INTEGER NOT NULL,
            genre_id INTEGER NOT NULL,
            PRIMARY KEY (film_id, genre_id),
            FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE,
            FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )
        """
    )
    cursor_films.execute("CREATE INDEX IF NOT EXISTS idx_genres_name ON genres(name)")
    cursor_films.execute("CREATE INDEX IF NOT EXISTS idx_fg_film ON film_genres(film_id)")
    cursor_films.execute("CREATE INDEX IF NOT EXISTS idx_fg_genre ON film_genres(genre_id)")
    conn_films.commit()
    # Бэкфилл кодов для существующих записей
    cursor_films.execute("SELECT id FROM films WHERE code IS NULL OR code = ''")
    missing = [row[0] for row in cursor_films.fetchall()]
    if missing:
        # Получим уже занятые коды
        cursor_films.execute("SELECT code FROM films WHERE code IS NOT NULL AND code != ''")
        used = {row[0] for row in cursor_films.fetchall()}
        def gen():
            return f"{random.randint(10000, 99999)}"
        for fid in missing:
            code = gen()
            # Гарантируем уникальность в оперативном наборе
            while code in used:
                code = gen()
            used.add(code)
            cursor_films.execute("UPDATE films SET code = ? WHERE id = ?", (code, fid))
        conn_films.commit()
    
    # --- One-time backfill: migrate comma-separated films.genre into normalized tables ---
    try:
        # Only run a lightweight sync that inserts missing mappings; safe to run idempotently
        cursor_films.execute("SELECT id, genre FROM films")
        rows = cursor_films.fetchall()
        for row in rows:
            fid = row[0]
            gstr = (row[1] or '').strip()
            if not gstr:
                continue
            # Split by comma/semicolon and trim
            parts = [p.strip() for p in __import__('re').split(r"[,;]", gstr) if p.strip()]
            if not parts:
                continue
            # Insert genres and mappings if absent
            for g in parts:
                gid = upsert_genre(conn_films, g)
                cursor_films.execute("INSERT OR IGNORE INTO film_genres(film_id, genre_id) VALUES(?, ?)", (fid, gid))
        conn_films.commit()
    except Exception:
        # Do not fail init if backfill has issues; it's safe to skip
        pass

    conn_films.commit()
    conn_films.close()

    conn_users = get_db_connection('users.db')
    cursor_users = conn_users.cursor()
    cursor_users.execute("""CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        tg_id INTEGER UNIQUE,
        admin INTEGER,
        referral_code TEXT UNIQUE,
        referred_by TEXT,
        banned INTEGER DEFAULT 0
    )""")
    cursor_users.execute("""CREATE TABLE IF NOT EXISTS referrals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_id INTEGER,
        date_referred TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (referrer_id) REFERENCES users(tg_id),
        FOREIGN KEY (referred_id) REFERENCES users(tg_id)
    )""")
    conn_users.commit()
    conn_users.close()


# --- Helpers for normalized genres ---
def upsert_genre(conn: sqlite3.Connection, name: str) -> int:
    """Insert genre if missing, return its id. Name is stored as-is (trimmed)."""
    n = (name or '').strip()
    if not n:
        raise ValueError("Genre name is empty")
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO genres(name) VALUES(?)", (n,))
        conn.commit()
    except Exception:
        # ignore
        pass
    cur.execute("SELECT id FROM genres WHERE name = ?", (n,))
    row = cur.fetchone()
    if not row:
        # Extremely rare race; try insert again
        cur.execute("INSERT OR REPLACE INTO genres(name) VALUES(?)", (n,))
        conn.commit()
        cur.execute("SELECT id FROM genres WHERE name = ?", (n,))
        row = cur.fetchone()
    return int(row[0])


def set_film_genres(conn: sqlite3.Connection, film_id: int, genres: Iterable[str]) -> None:
    """Replace film's genres mapping with provided list. Keeps films.genre text in sync."""
    cur = conn.cursor()
    # Deduplicate and clean
    clean: List[str] = []
    seen = set()
    for g in (genres or []):
        s = (g or '').strip()
        if not s or s in seen:
            continue
        seen.add(s)
        clean.append(s)
    # Reset mapping
    cur.execute("DELETE FROM film_genres WHERE film_id = ?", (film_id,))
    for g in clean:
        gid = upsert_genre(conn, g)
        cur.execute("INSERT OR IGNORE INTO film_genres(film_id, genre_id) VALUES(?, ?)", (film_id, gid))
    # Keep legacy text column in sync
    cur.execute("UPDATE films SET genre = ? WHERE id = ?", (", ".join(clean), film_id))
    conn.commit()
