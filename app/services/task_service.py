"""
Background task queue service for long-running operations.
"""

import asyncio
import random
import time
from typing import Optional

from app.core.logging import logger


class TaskService:
    """Background task queue with progress tracking."""

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._jobs: dict[str, dict] = {}
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._max_history = 100
        self._notify_callback = None

    def set_notify_callback(self, callback):
        """Set callback for real-time notifications (e.g., socket.io emit)."""
        self._notify_callback = callback

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("Task service started")

    async def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None
        logger.info("Task service stopped")

    async def enqueue(self, job_type: str, params: dict) -> dict:
        """Add a new job to the queue."""
        jid = f"{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        job = {
            "id": jid,
            "type": job_type,
            "params": params,
            "status": "pending",
            "progress": 0,
            "created_at": time.time(),
            "updated_at": time.time(),
            "meta": {},
            "error": None,
        }
        self._jobs[jid] = job
        self._trim_history()
        await self._queue.put(jid)
        await self._emit_update(job)
        return job

    def get_job(self, job_id: str) -> Optional[dict]:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[dict]:
        return sorted(
            self._jobs.values(),
            key=lambda j: j.get("created_at", 0),
            reverse=True,
        )

    async def _worker(self) -> None:
        from app.services.tmdb_service import tmdb_service

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
                    movie_id = int(job["params"]["movie_id"])
                    result = await tmdb_service.import_movie(movie_id)
                    job["meta"] = result

                elif job["type"] == "tmdb_popular":
                    count = int(job["params"].get("count", 5))
                    result = await tmdb_service.import_popular(count)
                    job["meta"] = result

                else:
                    raise RuntimeError(f"Unknown job type: {job['type']}")

                job["status"] = "done"
                job["progress"] = 100

            except Exception as e:
                job["status"] = "error"
                job["error"] = str(e)
                logger.error(f"Task {jid} failed: {e}")
            finally:
                job["updated_at"] = time.time()
                await self._emit_update(job)
                self._queue.task_done()

    async def _emit_update(self, job: dict) -> None:
        if self._notify_callback:
            payload = {
                "id": job["id"],
                "type": job["type"],
                "status": job["status"],
                "progress": int(job.get("progress") or 0),
                "meta": job.get("meta") or {},
                "error": job.get("error"),
            }
            try:
                await self._notify_callback("task_update", payload)
            except Exception:
                pass

    def _trim_history(self) -> None:
        if len(self._jobs) > self._max_history:
            done = [
                j for j in self._jobs.values()
                if j.get("status") in ("done", "error")
            ]
            done.sort(key=lambda x: x.get("created_at", 0))
            for x in done[: len(self._jobs) - self._max_history]:
                self._jobs.pop(x["id"], None)


# Singleton
task_service = TaskService()
