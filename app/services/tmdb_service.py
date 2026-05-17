"""
TMDb integration service — async HTTP, caching, poster downloads.
"""

import asyncio
import json
import os
import random
import time
from typing import Optional
from urllib.parse import urlencode

import httpx

from app.core.settings import settings
from app.core.logging import logger
from app.repositories.film_repository import film_repository
from app.web.static import uploads_path


class TMDbService:
    """Async TMDb API client with caching."""

    BASE_URL = "https://api.themoviedb.org/3"
    CACHE_TTL = 600  # 10 minutes

    def __init__(self):
        self._cache: dict[str, dict] = {}
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=20.0)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, path: str, params: Optional[dict] = None) -> dict:
        """Make a cached TMDb API request."""
        if not settings.TMDB_API_KEY:
            raise RuntimeError("TMDB_API_KEY не задан в .env")

        query = {
            "api_key": settings.TMDB_API_KEY,
            "language": settings.TMDB_LANGUAGE,
        }
        if params:
            query.update(params)

        cache_key = f"{path}?{urlencode(sorted(query.items()))}"
        now = time.time()

        # Check cache
        cached = self._cache.get(cache_key)
        if cached and (now - cached["ts"] < self.CACHE_TTL):
            return cached["data"]

        url = f"{self.BASE_URL}{path}?{urlencode(query)}"
        client = await self._get_client()

        # Retry with backoff
        last_error = None
        for attempt in range(3):
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                self._cache[cache_key] = {"ts": time.time(), "data": data}
                return data
            except Exception as e:
                last_error = e
                await asyncio.sleep(0.5 * (attempt + 1))

        # Fallback to stale cache
        if cached:
            return cached["data"]
        raise RuntimeError(f"TMDb API error: {last_error}")

    async def search_movies(self, query: str, page: int = 1) -> dict:
        """Search movies by title."""
        data = await self._request("/search/movie", {
            "query": query,
            "page": page,
            "include_adult": "false",
        })
        image_base = settings.TMDB_IMAGE_BASE
        results = []
        for it in data.get("results", [])[:20]:
            results.append({
                "id": it.get("id"),
                "title": it.get("title") or it.get("name"),
                "original_title": it.get("original_title"),
                "overview": it.get("overview"),
                "release_date": it.get("release_date"),
                "year": (it.get("release_date") or "")[:4],
                "poster": f"{image_base}/w200{it['poster_path']}" if it.get("poster_path") else None,
            })
        return {
            "results": results,
            "page": data.get("page", 1),
            "total_pages": data.get("total_pages", 1),
        }

    async def import_movie(self, movie_id: int) -> dict:
        """Import a single movie from TMDb. Returns film info or raises."""
        # Check duplicate
        if await film_repository.exists_external("tmdb", str(movie_id)):
            return {"duplicate": True, "message": "Фильм уже импортирован"}

        # Fetch movie details
        details = await self._request(f"/movie/{movie_id}")
        name = details.get("title") or details.get("name") or "Без названия"
        description = details.get("overview") or ""
        genre_list = [g.get("name") for g in details.get("genres", []) if g.get("name")]
        genres = ", ".join(genre_list)
        site = details.get("homepage") or ""

        # Download poster
        photo_id = await self._download_poster(movie_id, details.get("poster_path"))

        # Create film
        result = await film_repository.create(
            name=name,
            description=description,
            genre=genres,
            site=site,
            photo_id=photo_id,
            external_source="tmdb",
            external_id=str(movie_id),
        )

        logger.info(f"Imported from TMDb: '{name}' (code: {result['code']})")
        return {
            "duplicate": False,
            "id": result["id"],
            "code": result["code"],
            "name": name,
        }

    async def import_popular(self, count: int = 5) -> dict:
        """Import random popular movies. Returns summary."""
        count = max(1, min(50, count))

        # Get a random page of popular movies
        first_page = await self._request("/movie/popular", {"page": 1})
        total_pages = min(int(first_page.get("total_pages", 1) or 1), 500)
        rnd_page = random.randint(1, total_pages)
        data = first_page if rnd_page == 1 else await self._request(
            "/movie/popular", {"page": rnd_page}
        )

        results = [it.get("id") for it in data.get("results", []) if it.get("id")]
        if not results:
            return {"imported": 0, "skipped": 0, "requested": 0, "items": []}

        ids = random.sample(results, k=min(count, len(results)))
        imported = []
        skipped = 0

        for movie_id in ids:
            try:
                result = await self.import_movie(movie_id)
                if result.get("duplicate"):
                    skipped += 1
                else:
                    imported.append(result)
            except Exception as e:
                logger.warning(f"Failed to import TMDb #{movie_id}: {e}")
                skipped += 1

        return {
            "imported": len(imported),
            "skipped": skipped,
            "requested": len(ids),
            "items": imported,
        }

    async def _download_poster(self, movie_id: int, poster_path: Optional[str]) -> Optional[str]:
        """Download poster image. Returns filename or None."""
        if not poster_path:
            return None
        try:
            url = f"{settings.TMDB_IMAGE_BASE}/w500{poster_path}"
            client = await self._get_client()
            response = await client.get(url)
            response.raise_for_status()

            fn = f"tmdb_{movie_id}.jpg"
            file_path = os.path.join(uploads_path(), fn)
            with open(file_path, "wb") as f:
                f.write(response.content)
            return fn
        except Exception as e:
            logger.warning(f"Failed to download poster for TMDb #{movie_id}: {e}")
            return None


# Singleton
tmdb_service = TMDbService()
