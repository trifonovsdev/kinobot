"""
Async SQLite database manager with WAL mode, connection pooling,
and automatic schema initialization.
"""

import aiosqlite
import sqlite3
import asyncio
from pathlib import Path
from typing import Optional, Any
from contextlib import asynccontextmanager

from app.core.settings import settings
from app.core.logging import logger


class Database:
    """Async SQLite database wrapper with WAL mode support."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def connection(self):
        """Get an async database connection with WAL mode."""
        async with self._lock:
            db = await aiosqlite.connect(self.db_path)
            db.row_factory = aiosqlite.Row
            try:
                if settings.DB_WAL_MODE:
                    await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("PRAGMA foreign_keys=ON")
                yield db
            finally:
                await db.close()

    async def execute(self, query: str, params: tuple = ()) -> None:
        """Execute a single query."""
        async with self.connection() as db:
            await db.execute(query, params)
            await db.commit()

    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[dict]:
        """Fetch a single row as dict."""
        async with self.connection() as db:
            cursor = await db.execute(query, params)
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def fetch_all(self, query: str, params: tuple = ()) -> list[dict]:
        """Fetch all rows as list of dicts."""
        async with self.connection() as db:
            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


# Singleton database instances
films_db = Database("films.db")
users_db = Database("users.db")


async def init_databases() -> None:
    """Initialize database schemas."""
    logger.info("Initializing databases...")

    # Films DB
    async with films_db.connection() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS films(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            photo_status INTEGER DEFAULT 0,
            photo_id TEXT,
            activate INTEGER DEFAULT 1,
            genre TEXT DEFAULT '',
            site TEXT DEFAULT '',
            code TEXT UNIQUE,
            external_source TEXT,
            external_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_films_code ON films(code)"
        )
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_films_external ON films(external_source, external_id)"
        )
        await db.execute("""CREATE TABLE IF NOT EXISTS genres(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS film_genres(
            film_id INTEGER NOT NULL,
            genre_id INTEGER NOT NULL,
            PRIMARY KEY (film_id, genre_id),
            FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE,
            FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )""")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fg_film ON film_genres(film_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_fg_genre ON film_genres(genre_id)"
        )
        await db.commit()

    # Users DB
    async with users_db.connection() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            tg_id INTEGER UNIQUE,
            admin INTEGER DEFAULT 0,
            referral_code TEXT UNIQUE,
            referred_by TEXT,
            banned INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS referrals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            date_referred TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referrer_id) REFERENCES users(tg_id),
            FOREIGN KEY (referred_id) REFERENCES users(tg_id)
        )""")
        await db.commit()

    # Migrate existing data (add missing columns safely)
    await _migrate_films_db()
    await _migrate_users_db()

    logger.info("Databases initialized successfully")


async def _migrate_films_db() -> None:
    """Add missing columns to existing films table."""
    async with films_db.connection() as db:
        # Check existing columns
        cursor = await db.execute("PRAGMA table_info(films)")
        columns = {row[1] for row in await cursor.fetchall()}

        migrations = {
            "code": "ALTER TABLE films ADD COLUMN code TEXT",
            "external_source": "ALTER TABLE films ADD COLUMN external_source TEXT",
            "external_id": "ALTER TABLE films ADD COLUMN external_id TEXT",
            "created_at": "ALTER TABLE films ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for col, sql in migrations.items():
            if col not in columns:
                try:
                    await db.execute(sql)
                    logger.info(f"Added column '{col}' to films table")
                except Exception:
                    pass

        await db.commit()

        # Backfill codes for existing records without one
        import random
        cursor = await db.execute("SELECT id FROM films WHERE code IS NULL OR code = ''")
        missing = [row[0] for row in await cursor.fetchall()]
        if missing:
            cursor = await db.execute(
                "SELECT code FROM films WHERE code IS NOT NULL AND code != ''"
            )
            used = {row[0] for row in await cursor.fetchall()}
            for fid in missing:
                code = f"{random.randint(10000, 99999)}"
                while code in used:
                    code = f"{random.randint(10000, 99999)}"
                used.add(code)
                await db.execute("UPDATE films SET code = ? WHERE id = ?", (code, fid))
            await db.commit()
            logger.info(f"Backfilled codes for {len(missing)} films")


async def _migrate_users_db() -> None:
    """Add missing columns to existing users table."""
    async with users_db.connection() as db:
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = {row[1] for row in await cursor.fetchall()}

        if "created_at" not in columns:
            try:
                await db.execute(
                    "ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                )
                await db.commit()
                logger.info("Added column 'created_at' to users table")
            except Exception:
                pass
