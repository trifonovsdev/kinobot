"""
FastAPI application with secure auth, CSRF protection, and rate limiting.
"""

from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from fastapi.templating import Jinja2Templates
from werkzeug.utils import secure_filename

from app.core.settings import settings
from app.core.security import (
    login_limiter, generate_csrf_token, validate_csrf_token,
    hash_password, verify_password,
)
from app.core.logging import logger
from app.db.database import init_databases
from app.repositories.film_repository import film_repository
from app.repositories.user_repository import user_repository
from app.services.tmdb_service import tmdb_service
from app.services.task_service import task_service
from app.web.sockets import sio, emit_films, emit_users, notify
from app.web.static import uploads_path, allowed_file


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle — initialize DB and services on startup."""
    await init_databases()
    task_service.set_notify_callback(notify)
    await task_service.start()
    logger.info("Application started")
    try:
        yield
    finally:
        await task_service.stop()
        await tmdb_service.close()
        logger.info("Application stopped")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="KinoBot Admin",
        version="4.0",
        lifespan=lifespan,
        docs_url=None,  # Disable docs in production
        redoc_url=None,
    )

    # Middleware
    app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    templates = Jinja2Templates(directory="templates")

    # Static files
    app.mount("/static", StaticFiles(directory="static"), name="static")

    # ========== Auth Helpers ==========

    def require_auth(request: Request) -> None:
        """Raise 302 redirect if not authenticated."""
        if not request.session.get("logged_in"):
            raise HTTPException(status_code=302, headers={"Location": "/login"})

    def get_client_ip(request: Request) -> str:
        """Extract client IP for rate limiting."""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    # ========== Auth Routes ==========

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        csrf = generate_csrf_token()
        request.session["csrf_token"] = csrf
        return templates.TemplateResponse("login.html", {
            "request": request,
            "csrf_token": csrf,
        })

    @app.post("/login")
    async def login(request: Request):
        client_ip = get_client_ip(request)

        # Rate limiting
        if not login_limiter.is_allowed(client_ip):
            logger.warning(f"Rate limit exceeded for IP: {client_ip}")
            return templates.TemplateResponse("login.html", {
                "request": request,
                "error": "Слишком много попыток. Подождите минуту.",
                "csrf_token": generate_csrf_token(),
            })

        form = await request.form()
        username = form.get("username", "")
        password = form.get("password", "")

        # Validate credentials
        if (
            username == settings.ADMIN_USERNAME
            and password == settings.ADMIN_PASSWORD
        ):
            request.session["logged_in"] = True
            login_limiter.reset(client_ip)
            logger.info(f"Admin login from IP: {client_ip}")
            return RedirectResponse("/", status_code=302)

        logger.warning(f"Failed login attempt from IP: {client_ip}")
        csrf = generate_csrf_token()
        request.session["csrf_token"] = csrf
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Неверный логин или пароль",
            "csrf_token": csrf,
        })

    @app.get("/logout")
    async def logout(request: Request):
        request.session.clear()
        return RedirectResponse("/login", status_code=302)

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        require_auth(request)
        return templates.TemplateResponse("index.html", {"request": request})

    # ========== Film API ==========

    @app.get("/api/films")
    async def get_films_api(request: Request):
        require_auth(request)
        return JSONResponse(await film_repository.get_all())

    @app.get("/api/films/search")
    async def search_films(request: Request, query: str = "", genre: str = ""):
        require_auth(request)
        return JSONResponse(await film_repository.search(query, genre))

    @app.get("/api/film/{film_id}")
    async def get_film(request: Request, film_id: int):
        require_auth(request)
        film = await film_repository.get_by_id(film_id)
        if not film:
            raise HTTPException(status_code=404, detail="Фильм не найден")
        return JSONResponse(film)

    @app.post("/api/film")
    async def add_film(
        request: Request,
        name: str = Form(...),
        genre: str = Form(...),
        description: str = Form(""),
        site: str = Form(""),
        image: UploadFile | None = File(None),
    ):
        require_auth(request)
        photo_id = await _save_upload(image)
        result = await film_repository.create(
            name=name, description=description,
            genre=genre, site=site, photo_id=photo_id,
        )
        await sio.emit("notification", {
            "message": f'Фильм "{name}" добавлен. Код: {result["code"]}',
            "type": "success",
        })
        await emit_films()
        return JSONResponse(
            {"id": result["id"], "code": result["code"], "name": name, "message": "Фильм добавлен"},
            status_code=201,
        )

    @app.put("/api/film/{film_id}")
    async def update_film(
        request: Request,
        film_id: int,
        name: str = Form(...),
        genre: str = Form(...),
        description: str = Form(""),
        site: str = Form(""),
        image: UploadFile | None = File(None),
    ):
        require_auth(request)
        photo_id = await _save_upload(image)
        await film_repository.update(
            film_id, name=name, description=description,
            genre=genre, site=site, photo_id=photo_id,
        )
        await sio.emit("notification", {
            "message": f'Фильм "{name}" обновлён',
            "type": "info",
        })
        await emit_films()
        return JSONResponse({"message": "Фильм обновлён"})

    # ========== User API ==========

    @app.get("/api/users")
    async def get_users_api(request: Request):
        require_auth(request)
        return JSONResponse(await user_repository.get_all())

    @app.post("/api/user/{user_id}/toggle-admin")
    async def toggle_admin(request: Request, user_id: int):
        require_auth(request)
        result = await user_repository.toggle_admin(user_id)
        if not result:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        status = "траффер" if result["admin"] else "пользователь"
        await sio.emit("notification", {
            "message": f'Пользователь "{result["name"]}" теперь {status}',
            "type": "info",
        })
        await emit_users()
        return JSONResponse({"message": f"Статус изменён на {status}"})

    @app.post("/api/user/{user_id}/toggle-ban")
    async def toggle_ban(request: Request, user_id: int):
        require_auth(request)
        result = await user_repository.toggle_ban(user_id)
        if not result:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        action = "забанен" if result["banned"] else "разбанен"
        await sio.emit("notification", {
            "message": f'Пользователь "{result["name"]}" {action}',
            "type": "warning" if result["banned"] else "success",
        })
        await emit_users()
        return JSONResponse({"message": f"Пользователь {action}"})

    # ========== Stats API ==========

    @app.get("/api/stats")
    async def get_stats(request: Request):
        require_auth(request)
        films_stats = await film_repository.get_stats()
        users_stats = await user_repository.get_stats()
        return JSONResponse({
            "films": films_stats,
            "users": {
                "total": users_stats["total"],
                "admins": users_stats["admins"],
                "banned": users_stats["banned"],
            },
            "referrals": users_stats["referrals"],
        })

    # ========== TMDb API ==========

    @app.get("/api/import/search")
    async def import_search(request: Request, query: str, page: int = 1):
        require_auth(request)
        query = (query or "").strip()
        if not query:
            return JSONResponse({"results": [], "page": 1, "total_pages": 0})
        result = await tmdb_service.search_movies(query, page)
        return JSONResponse(result)

    @app.post("/api/tasks/import/tmdb/{movie_id}")
    async def enqueue_import_single(request: Request, movie_id: int):
        require_auth(request)
        job = await task_service.enqueue("tmdb_single", {"movie_id": movie_id})
        return JSONResponse({"job_id": job["id"], "status": job["status"]}, status_code=202)

    @app.post("/api/tasks/import/tmdb/popular")
    async def enqueue_import_popular(request: Request, count: int = Query(..., ge=2, le=50)):
        require_auth(request)
        job = await task_service.enqueue("tmdb_popular", {"count": count})
        return JSONResponse({"job_id": job["id"], "status": job["status"]}, status_code=202)

    @app.get("/api/tasks")
    async def list_tasks(request: Request):
        require_auth(request)
        return JSONResponse(task_service.list_jobs())

    @app.get("/api/tasks/{job_id}")
    async def get_task(request: Request, job_id: str):
        require_auth(request)
        job = task_service.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Задача не найдена")
        return JSONResponse(job)

    # ========== Update API ==========

    @app.get("/api/update/status")
    async def update_status(request: Request):
        require_auth(request)
        from app.updater import check_update_available
        return JSONResponse(check_update_available())

    @app.post("/api/update/apply")
    async def update_apply(request: Request):
        require_auth(request)
        from app.updater import trigger_update
        result = trigger_update()
        return JSONResponse(result, status_code=202 if result.get("status") == "started" else 200)

    # ========== Helpers ==========

    async def _save_upload(file: UploadFile | None) -> str | None:
        """Save uploaded file and return filename."""
        if not file or not file.filename or not allowed_file(file.filename):
            return None
        filename = secure_filename(file.filename)
        file_path = os.path.join(uploads_path(), filename)
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
        return filename

    return app
