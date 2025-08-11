import asyncio
import json
import os
import random
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from app.core.settings import settings
from app.db.sqlite import get_db_connection, set_film_genres
from app.web.static import uploads_path
from app.web.sockets import sio, get_films as sio_get_films


class TaskManager:
    def __init__(self) -> None:
        self._queue: "asyncio.Queue[dict]" = asyncio.Queue()
        self._jobs: Dict[str, dict] = {}
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._max_history = 100  # keep recent jobs in memory

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except Exception:
                pass
            self._worker_task = None

    def _new_job(self, jtype: str, params: dict) -> dict:
        jid = f"{int(time.time()*1000)}-{random.randint(1000,9999)}"
        job = {
            "id": jid,
            "type": jtype,
            "params": params,
            "status": "pending",  # pending | running | done | error
            "progress": 0,
            "created_at": time.time(),
            "updated_at": time.time(),
            "meta": {},
            "error": None,
        }
        self._jobs[jid] = job
        # trim history (keep last N by created_at)
        if len(self._jobs) > self._max_history:
            # delete oldest done jobs first
            done = [j for j in self._jobs.values() if j.get("status") in ("done", "error")]
            done.sort(key=lambda x: x.get("created_at", 0))
            for x in done[: max(0, len(self._jobs) - self._max_history)]:
                self._jobs.pop(x["id"], None)
        return job

    async def enqueue(self, jtype: str, params: dict) -> dict:
        job = self._new_job(jtype, params)
        await self._queue.put(job["id"])
        await self._emit_update(job)
        return job

    def get_job(self, job_id: str) -> Optional[dict]:
        j = self._jobs.get(job_id)
        if not j:
            return None
        return dict(j)

    def list_jobs(self) -> List[dict]:
        # sort by created_at desc
        return [dict(self._jobs[jid]) for jid in sorted(self._jobs.keys(), key=lambda k: self._jobs[k]["created_at"], reverse=True)]

    async def _worker(self) -> None:
        while self._running:
            try:
                jid = await self._queue.get()
            except asyncio.CancelledError:
                break
            job = self._jobs.get(jid)
            if not job:
                continue
            try:
                job["status"] = "running"
                job["updated_at"] = time.time()
                await self._emit_update(job)
                if job["type"] == "tmdb_single":
                    await self._handle_tmdb_single(job)
                elif job["type"] == "tmdb_popular":
                    await self._handle_tmdb_popular(job)
                else:
                    raise RuntimeError(f"Unknown job type: {job['type']}")
                job["status"] = "done"
                job["progress"] = 100
                job["updated_at"] = time.time()
                await self._emit_update(job)
            except Exception as e:
                job["status"] = "error"
                job["error"] = str(e)
                job["updated_at"] = time.time()
                await self._emit_update(job)
            finally:
                self._queue.task_done()

    async def _emit_update(self, job: dict) -> None:
        # Send sanitized payload to clients
        payload = {
            "id": job["id"],
            "type": job["type"],
            "status": job["status"],
            "progress": int(job.get("progress") or 0),
            "meta": job.get("meta") or {},
            "error": job.get("error"),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
        }
        try:
            await sio.emit("task_update", payload)
        except Exception:
            pass

    # --- TMDb helpers ---
    def _tmdb_request(self, path: str, params: Optional[dict] = None) -> dict:
        if not settings.TMDB_API_KEY:
            raise RuntimeError("TMDB_API_KEY не задан в .env")
        base = "https://api.themoviedb.org/3"
        q = {"api_key": settings.TMDB_API_KEY, "language": settings.TMDB_LANGUAGE}
        if params:
            q.update(params)
        url = f"{base}{path}?" + urllib.parse.urlencode(q)
        with urllib.request.urlopen(url, timeout=15) as r:
            data = r.read()
            return json.loads(data.decode("utf-8"))

    async def _handle_tmdb_single(self, job: dict) -> None:
        movie_id = int(job["params"].get("movie_id"))
        # Duplicate check
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM films WHERE external_source = ? AND external_id = ?",
                ("tmdb", str(movie_id)),
            )
            if cur.fetchone():
                job["meta"] = {"duplicate": True}
                job["progress"] = 100
                return
            # Fetch details
            d = self._tmdb_request(f"/movie/{movie_id}", {})
            name = d.get("title") or d.get("name") or "Без названия"
            description = d.get("overview") or ""
            genre_list = [g.get("name") for g in d.get("genres", []) if g.get("name")]
            genres = ", ".join(genre_list)
            site = d.get("homepage") or ""
            poster_path = d.get("poster_path")
            photo_id = None
            if poster_path:
                try:
                    base = settings.TMDB_IMAGE_BASE
                    url = f"{base}/w500{poster_path}"
                    fn = f"tmdb_{movie_id}.jpg"
                    file_path = os.path.join(uploads_path(), fn)
                    with urllib.request.urlopen(url, timeout=15) as r, open(file_path, "wb") as f:
                        f.write(r.read())
                    photo_id = fn
                except Exception:
                    photo_id = None
            # Insert
            import sqlite3

            def gen() -> str:
                return f"{random.randint(10000, 99999)}"

            code = gen()
            with conn:
                c = conn.cursor()
                for _ in range(7):
                    try:
                        c.execute(
                            """
                            INSERT INTO films (name, description, photo_status, photo_id, activate, genre, site, code, external_source, external_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (name, description, 1 if photo_id else 0, photo_id, 1, genres, site, code, "tmdb", str(movie_id)),
                        )
                        break
                    except sqlite3.IntegrityError:
                        code = gen()
                film_id = c.lastrowid
            # normalize genres mapping
            try:
                set_film_genres(conn, film_id, genre_list)
            except Exception:
                pass
            job["meta"] = {"id": film_id, "code": code, "name": name}
            job["progress"] = 100
            # notify UI
            try:
                await sio.emit("notification", {"message": f'Импортировано из TMDb: "{name}". Код: {code}', "type": "success"})
                await sio_get_films()
            except Exception:
                pass
        finally:
            conn.close()

    async def _handle_tmdb_popular(self, job: dict) -> None:
        count = int(job["params"].get("count") or 0)
        if count <= 0:
            count = 1
        # Pick random page then sample N ids
        first_page = self._tmdb_request("/movie/popular", {"page": 1})
        total_pages = int(first_page.get("total_pages", 1) or 1)
        max_page = min(total_pages, 500)
        rnd_page = random.randint(1, max_page)
        data = first_page if rnd_page == 1 else self._tmdb_request("/movie/popular", {"page": rnd_page})
        results = [it.get("id") for it in data.get("results", []) if it.get("id")]
        if not results:
            job["meta"] = {"requested": count, "imported": 0, "skipped": 0}
            job["progress"] = 100
            return
        ids = random.sample(results, k=min(count, len(results)))
        job["meta"] = {"requested": len(ids), "imported": 0, "skipped": 0, "failed": 0}
        await self._emit_update(job)

        conn = get_db_connection()
        c = conn.cursor()
        imported = []
        skipped = 0
        failed = 0

        import sqlite3

        def gen() -> str:
            return f"{random.randint(10000, 99999)}"

        for idx, movie_id in enumerate(ids, start=1):
            # update progress step-wise
            job["progress"] = int((idx - 1) * 100 / len(ids))
            job["updated_at"] = time.time()
            await self._emit_update(job)
            try:
                # duplicate check
                c.execute(
                    "SELECT id FROM films WHERE external_source = ? AND external_id = ?",
                    ("tmdb", str(movie_id)),
                )
                if c.fetchone():
                    skipped += 1
                    job["meta"].update({"skipped": skipped})
                    continue
                # fetch details
                d = self._tmdb_request(f"/movie/{movie_id}", {})
                name = d.get("title") or d.get("name") or "Без названия"
                description = d.get("overview") or ""
                genre_list = [g.get("name") for g in d.get("genres", []) if g.get("name")]
                genres = ", ".join(genre_list)
                site = d.get("homepage") or ""
                poster_path = d.get("poster_path")
                photo_id = None
                if poster_path:
                    try:
                        base = settings.TMDB_IMAGE_BASE
                        url = f"{base}/w500{poster_path}"
                        fn = f"tmdb_{movie_id}.jpg"
                        file_path = os.path.join(uploads_path(), fn)
                        with urllib.request.urlopen(url, timeout=15) as r, open(file_path, "wb") as f:
                            f.write(r.read())
                        photo_id = fn
                    except Exception:
                        photo_id = None
                code = gen()
                for _ in range(7):
                    try:
                        c.execute(
                            """
                            INSERT INTO films (name, description, photo_status, photo_id, activate, genre, site, code, external_source, external_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (name, description, 1 if photo_id else 0, photo_id, 1, genres, site, code, "tmdb", str(movie_id)),
                        )
                        break
                    except sqlite3.IntegrityError:
                        code = gen()
                film_id = c.lastrowid
                # normalize genres mapping
                try:
                    set_film_genres(conn, film_id, genre_list)
                except Exception:
                    pass
                imported.append({"id": film_id, "code": code, "name": name})
                job["meta"].update({"imported": len(imported)})
            except Exception:
                failed += 1
                job["meta"].update({"failed": failed})
                continue
        conn.commit()
        try:
            await sio.emit(
                "notification",
                {
                    "message": f'Импорт популярных TMDb: добавлено {len(imported)} из {len(ids)} (пропущено: {skipped})',
                    "type": "success" if imported else "warning",
                },
            )
            await sio_get_films()
        except Exception:
            pass
        finally:
            conn.close()
        job["progress"] = 100
        job["meta"].update({"items": imported, "skipped": skipped})


# Export a singleton manager
task_manager = TaskManager()
