"""
Film repository — all database operations for films.
Single responsibility: data access only, no business logic.
"""

import random
import re
from typing import Optional

from app.db.database import films_db
from app.core.logging import logger


class FilmRepository:
    """Data access layer for films."""

    async def get_all(self, limit: int = 1000) -> list[dict]:
        """Get all films ordered by newest first."""
        return await films_db.fetch_all(
            "SELECT * FROM films ORDER BY id DESC LIMIT ?", (limit,)
        )

    async def get_by_id(self, film_id: int) -> Optional[dict]:
        """Get a single film by ID."""
        return await films_db.fetch_one(
            "SELECT * FROM films WHERE id = ?", (film_id,)
        )

    async def get_by_code(self, code: str) -> Optional[dict]:
        """Get a single film by its unique code."""
        return await films_db.fetch_one(
            "SELECT * FROM films WHERE code = ? AND activate = 1", (code,)
        )

    async def get_by_code_or_id(self, value: str) -> Optional[dict]:
        """Get film by code or numeric ID (active only)."""
        try:
            num_id = int(value)
        except ValueError:
            num_id = -1
        return await films_db.fetch_one(
            "SELECT * FROM films WHERE activate = 1 AND (code = ? OR id = ?)",
            (value, num_id),
        )

    async def get_random_by_genre(self, genre: str) -> Optional[dict]:
        """Get a random active film matching a genre."""
        g = genre.lower().strip().replace(" ", "")
        return await films_db.fetch_one(
            """
            SELECT * FROM films
            WHERE activate = 1
              AND LOWER(',' || REPLACE(COALESCE(genre, ''), ' ', '') || ',') LIKE ?
            ORDER BY RANDOM() LIMIT 1
            """,
            (f"%,{g},%",),
        )

    async def get_active_genres(self) -> list[str]:
        """Get all unique genres from active films."""
        rows = await films_db.fetch_all(
            "SELECT genre FROM films WHERE activate = 1 AND genre IS NOT NULL AND TRIM(genre) != ''"
        )
        genres_set: set[str] = set()
        for row in rows:
            raw = row.get("genre", "") or ""
            for part in raw.split(","):
                g = part.strip()
                if g:
                    genres_set.add(g)
        return sorted(genres_set, key=lambda s: s.lower())

    async def search(self, query: str = "", genre: str = "") -> list[dict]:
        """Search films by name/code and filter by genre."""
        films = await self.get_all()
        if query:
            q = query.lower().strip()
            films = [
                f for f in films
                if q in str(f.get("id", "")).lower()
                or q in (f.get("code") or "").lower()
                or q in (f.get("name") or "").lower()
            ]
        if genre and genre != "all":
            g_lower = genre.lower().strip()
            films = [
                f for f in films
                if any(
                    p.strip().lower() == g_lower
                    for p in re.split(r"[,;]", f.get("genre") or "")
                )
            ]
        return films

    async def create(
        self,
        name: str,
        description: str = "",
        genre: str = "",
        site: str = "",
        photo_id: Optional[str] = None,
        external_source: Optional[str] = None,
        external_id: Optional[str] = None,
    ) -> dict:
        """Create a new film with unique code. Returns the created film."""
        code = await self._generate_unique_code()
        async with films_db.connection() as db:
            cursor = await db.execute(
                """
                INSERT INTO films (name, description, photo_status, photo_id, activate, genre, site, code, external_source, external_id)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                """,
                (
                    name, description,
                    1 if photo_id else 0, photo_id,
                    genre, site, code,
                    external_source, external_id,
                ),
            )
            await db.commit()
            film_id = cursor.lastrowid

        # Set genres mapping
        await self._set_genres(film_id, genre)

        return {
            "id": film_id,
            "code": code,
            "name": name,
            "genre": genre,
        }

    async def update(
        self,
        film_id: int,
        name: str,
        description: str = "",
        genre: str = "",
        site: str = "",
        photo_id: Optional[str] = None,
    ) -> None:
        """Update an existing film."""
        async with films_db.connection() as db:
            if photo_id:
                await db.execute(
                    """
                    UPDATE films SET name=?, description=?, photo_status=1,
                    photo_id=?, genre=?, site=? WHERE id=?
                    """,
                    (name, description, photo_id, genre, site, film_id),
                )
            else:
                await db.execute(
                    """
                    UPDATE films SET name=?, description=?, genre=?, site=? WHERE id=?
                    """,
                    (name, description, genre, site, film_id),
                )
            await db.commit()

        await self._set_genres(film_id, genre)

    async def delete(self, film_id: int) -> Optional[dict]:
        """Delete a film. Returns the deleted film info or None."""
        film = await self.get_by_id(film_id)
        if not film:
            return None
        async with films_db.connection() as db:
            await db.execute("DELETE FROM films WHERE id = ?", (film_id,))
            await db.execute("DELETE FROM film_genres WHERE film_id = ?", (film_id,))
            # Reset autoincrement if table is empty
            cursor = await db.execute("SELECT COUNT(*) FROM films")
            row = await cursor.fetchone()
            if row and row[0] == 0:
                await db.execute("DELETE FROM sqlite_sequence WHERE name='films'")
            await db.commit()
        return film

    async def exists_external(self, source: str, ext_id: str) -> bool:
        """Check if film from external source already exists."""
        row = await films_db.fetch_one(
            "SELECT id FROM films WHERE external_source = ? AND external_id = ?",
            (source, str(ext_id)),
        )
        return row is not None

    async def get_stats(self) -> dict:
        """Get film statistics."""
        async with films_db.connection() as db:
            cursor = await db.execute("SELECT COUNT(*) as cnt FROM films")
            row = await cursor.fetchone()
            total = row[0] if row else 0

            cursor = await db.execute(
                "SELECT COUNT(*) FROM films WHERE photo_id IS NOT NULL AND photo_id != ''"
            )
            row = await cursor.fetchone()
            with_image = row[0] if row else 0

            cursor = await db.execute("SELECT code, name FROM films ORDER BY id DESC LIMIT 5")
            recent = [{"code": r[0], "name": r[1]} for r in await cursor.fetchall()]

        # Genre breakdown
        all_films = await self.get_all()
        counts: dict[str, int] = {}
        display: dict[str, str] = {}
        for f in all_films:
            gstr = (f.get("genre") or "").strip()
            if not gstr:
                key = "не указан"
                counts[key] = counts.get(key, 0) + 1
                display.setdefault(key, "Не указан")
                continue
            parts = [p.strip() for p in re.split(r"[,;]", gstr) if p.strip()]
            for g in parts:
                key = g.lower()
                counts[key] = counts.get(key, 0) + 1
                display.setdefault(key, g)

        by_genre = sorted(
            [{"genre": display.get(k, k.title()), "count": v} for k, v in counts.items()],
            key=lambda x: x["count"],
            reverse=True,
        )

        return {
            "total": total,
            "with_image": with_image,
            "by_genre": by_genre,
            "recent": recent,
        }

    # --- Private helpers ---

    async def _generate_unique_code(self) -> str:
        """Generate a unique 5-digit code."""
        async with films_db.connection() as db:
            cursor = await db.execute(
                "SELECT code FROM films WHERE code IS NOT NULL"
            )
            used = {row[0] for row in await cursor.fetchall()}

        for _ in range(100):
            code = f"{random.randint(10000, 99999)}"
            if code not in used:
                return code
        # Fallback: 6 digits
        return f"{random.randint(100000, 999999)}"

    async def _set_genres(self, film_id: int, genre_str: str) -> None:
        """Sync normalized genres for a film."""
        parts = [p.strip() for p in re.split(r"[,;]", genre_str or "") if p.strip()]
        async with films_db.connection() as db:
            await db.execute("DELETE FROM film_genres WHERE film_id = ?", (film_id,))
            for g in parts:
                # Upsert genre
                await db.execute("INSERT OR IGNORE INTO genres(name) VALUES(?)", (g,))
                cursor = await db.execute("SELECT id FROM genres WHERE name = ?", (g,))
                row = await cursor.fetchone()
                if row:
                    await db.execute(
                        "INSERT OR IGNORE INTO film_genres(film_id, genre_id) VALUES(?, ?)",
                        (film_id, row[0]),
                    )
            await db.commit()


# Singleton
film_repository = FilmRepository()
