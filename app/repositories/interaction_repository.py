"""
Interaction repository — favorites, ratings, watch history, recommendations.
"""

import re
from typing import Optional
from collections import Counter

from app.db.database import users_db, films_db
from app.core.logging import logger


class InteractionRepository:
    """Data access for user-film interactions."""

    # ============ FAVORITES ============

    async def add_favorite(self, tg_id: int, film_id: int) -> bool:
        """Add film to favorites. Returns True if added, False if already exists."""
        async with users_db.connection() as db:
            try:
                await db.execute(
                    "INSERT OR IGNORE INTO favorites (tg_id, film_id) VALUES (?, ?)",
                    (tg_id, film_id),
                )
                await db.commit()
                return True
            except Exception:
                return False

    async def remove_favorite(self, tg_id: int, film_id: int) -> bool:
        """Remove film from favorites."""
        async with users_db.connection() as db:
            cursor = await db.execute(
                "DELETE FROM favorites WHERE tg_id = ? AND film_id = ?",
                (tg_id, film_id),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def is_favorite(self, tg_id: int, film_id: int) -> bool:
        """Check if film is in user's favorites."""
        row = await users_db.fetch_one(
            "SELECT 1 FROM favorites WHERE tg_id = ? AND film_id = ?",
            (tg_id, film_id),
        )
        return row is not None

    async def get_favorites(self, tg_id: int, limit: int = 20) -> list[dict]:
        """Get user's favorite films (returns film data from films_db)."""
        rows = await users_db.fetch_all(
            "SELECT film_id FROM favorites WHERE tg_id = ? ORDER BY added_at DESC LIMIT ?",
            (tg_id, limit),
        )
        if not rows:
            return []
        film_ids = [r["film_id"] for r in rows]
        films = []
        for fid in film_ids:
            film = await films_db.fetch_one("SELECT * FROM films WHERE id = ?", (fid,))
            if film:
                films.append(film)
        return films

    async def get_favorites_count(self, tg_id: int) -> int:
        """Count user's favorites."""
        row = await users_db.fetch_one(
            "SELECT COUNT(*) as cnt FROM favorites WHERE tg_id = ?", (tg_id,)
        )
        return row["cnt"] if row else 0

    # ============ RATINGS ============

    async def rate_film(self, tg_id: int, film_id: int, score: int) -> None:
        """Rate a film (1-5). Updates if already rated."""
        score = max(1, min(5, score))
        async with users_db.connection() as db:
            await db.execute(
                """INSERT INTO ratings (tg_id, film_id, score) VALUES (?, ?, ?)
                   ON CONFLICT(tg_id, film_id) DO UPDATE SET score = ?, rated_at = CURRENT_TIMESTAMP""",
                (tg_id, film_id, score, score),
            )
            await db.commit()

    async def get_user_rating(self, tg_id: int, film_id: int) -> Optional[int]:
        """Get user's rating for a film."""
        row = await users_db.fetch_one(
            "SELECT score FROM ratings WHERE tg_id = ? AND film_id = ?",
            (tg_id, film_id),
        )
        return row["score"] if row else None

    async def get_film_rating(self, film_id: int) -> dict:
        """Get average rating and count for a film."""
        row = await users_db.fetch_one(
            "SELECT AVG(score) as avg, COUNT(*) as cnt FROM ratings WHERE film_id = ?",
            (film_id,),
        )
        if not row or not row["cnt"]:
            return {"average": 0, "count": 0}
        return {"average": round(row["avg"], 1), "count": row["cnt"]}

    # ============ WATCH HISTORY ============

    async def record_watch(self, tg_id: int, film_id: int) -> None:
        """Record that user watched/viewed a film."""
        async with users_db.connection() as db:
            await db.execute(
                "INSERT INTO watch_history (tg_id, film_id) VALUES (?, ?)",
                (tg_id, film_id),
            )
            await db.commit()

    async def get_history(self, tg_id: int, limit: int = 10) -> list[dict]:
        """Get user's watch history."""
        rows = await users_db.fetch_all(
            "SELECT DISTINCT film_id FROM watch_history WHERE tg_id = ? ORDER BY watched_at DESC LIMIT ?",
            (tg_id, limit),
        )
        if not rows:
            return []
        films = []
        for r in rows:
            film = await films_db.fetch_one("SELECT * FROM films WHERE id = ?", (r["film_id"],))
            if film:
                films.append(film)
        return films

    async def get_history_count(self, tg_id: int) -> int:
        """Count unique films in history."""
        row = await users_db.fetch_one(
            "SELECT COUNT(DISTINCT film_id) as cnt FROM watch_history WHERE tg_id = ?",
            (tg_id,),
        )
        return row["cnt"] if row else 0

    # ============ RECOMMENDATIONS ============

    async def get_recommendations(self, tg_id: int, limit: int = 5) -> list[dict]:
        """Get recommended films based on user's genre preferences."""
        # Analyze user's favorites and history for genre patterns
        fav_films = await self.get_favorites(tg_id, limit=50)
        hist_films = await self.get_history(tg_id, limit=50)

        all_user_films = fav_films + hist_films
        if not all_user_films:
            # Cold start: return random popular films
            return await films_db.fetch_all(
                "SELECT * FROM films WHERE activate = 1 ORDER BY RANDOM() LIMIT ?",
                (limit,),
            )

        # Count genre preferences
        genre_counter: Counter = Counter()
        seen_ids = set()
        for f in all_user_films:
            seen_ids.add(f["id"])
            for g in re.split(r"[,;]", f.get("genre") or ""):
                g = g.strip()
                if g:
                    genre_counter[g.lower()] += 1

        if not genre_counter:
            return await films_db.fetch_all(
                "SELECT * FROM films WHERE activate = 1 ORDER BY RANDOM() LIMIT ?",
                (limit,),
            )

        # Get top genres
        top_genres = [g for g, _ in genre_counter.most_common(3)]

        # Find films matching top genres that user hasn't seen
        all_active = await films_db.fetch_all(
            "SELECT * FROM films WHERE activate = 1"
        )

        scored: list[tuple[int, dict]] = []
        for film in all_active:
            if film["id"] in seen_ids:
                continue
            film_genres = [
                g.strip().lower()
                for g in re.split(r"[,;]", film.get("genre") or "")
                if g.strip()
            ]
            score = sum(1 for g in film_genres if g in top_genres)
            if score > 0:
                scored.append((score, film))

        scored.sort(key=lambda x: x[0], reverse=True)
        results = [f for _, f in scored[:limit]]

        # If not enough, fill with random
        if len(results) < limit:
            extra = await films_db.fetch_all(
                "SELECT * FROM films WHERE activate = 1 ORDER BY RANDOM() LIMIT ?",
                (limit - len(results) + len(seen_ids),),
            )
            for f in extra:
                if f["id"] not in seen_ids and f not in results:
                    results.append(f)
                if len(results) >= limit:
                    break

        return results[:limit]


# Singleton
interaction_repository = InteractionRepository()
