from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from fastapi.templating import Jinja2Templates
from typing import Callable, Optional
import os
import sys
import shutil
import tempfile
import subprocess

from app.core.settings import settings
from app.db.sqlite import init_db, get_db_connection, set_film_genres
from app.web.sockets import sio, get_films as sio_get_films, get_users as sio_get_users
from app.web.static import uploads_path, allowed_file
import urllib.parse, urllib.request, json
import time
import re
from pathlib import Path

# tasks
try:
    from app.web.tasks import task_manager
except Exception:
    task_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # start background task manager
    if task_manager is not None:
        try:
            await task_manager.start()
        except Exception:
            pass
    try:
        yield
    finally:
        if task_manager is not None:
            try:
                await task_manager.stop()
            except Exception:
                pass


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    templates = Jinja2Templates(directory="templates")

    # static & uploads
    app.mount("/static", StaticFiles(directory="static"), name="static")

    # Socket.IO обёртка подключена в main.py (ASGIApp). Доп. монтирование не требуется.

    # Auth helpers
    def login_required(request: Request):
        if not request.session.get("logged_in"):
            raise HTTPException(status_code=302, detail="Redirect", headers={"Location": "/login"})

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        return templates.TemplateResponse("login.html", {"request": request})

    @app.post("/login")
    async def login(request: Request):
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        if username == 'root' and password == 'root':
            request.session['logged_in'] = True
            return RedirectResponse("/", status_code=302)
        return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный логин или пароль"})

    @app.get("/logout")
    async def logout(request: Request):
        request.session.pop('logged_in', None)
        return RedirectResponse("/login", status_code=302)

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        login_required(request)
        return templates.TemplateResponse("index.html", {"request": request})

    # ==================== Auto-Update API (Admin) ====================
    def _read_version_local() -> str:
        try:
            root = Path(__file__).resolve().parents[2]
            v = (root / "VERSION").read_text(encoding="utf-8").strip()
            return v
        except Exception:
            return "v0.0"

    def _parse_version(s: str) -> tuple:
        s = (s or "").strip()
        if s.startswith(("v", "V")):
            s = s[1:]
        parts = []
        for p in s.split('.'):
            try:
                parts.append(int(re.sub(r"[^0-9]", "", p) or 0))
            except Exception:
                parts.append(0)
        while len(parts) < 3:
            parts.append(0)
        return tuple(parts[:3])

    def _version_gt(a: str, b: str) -> bool:
        return _parse_version(a) > _parse_version(b)

    class _DirIndexParser:
        def __init__(self):
            self.links: list[str] = []
        def feed(self, html: str):
            # простейший парсер ссылок из автоиндекса
            for m in re.finditer(r'<a[^>]+href="([^"]+)"', html, flags=re.I):
                href = m.group(1)
                self.links.append(href)

    def _http_get(url: str, timeout: float = 10.0) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "Kinobot-Updater/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()

    def _discover_latest_from_dirbase(base_url: str) -> str | None:
        # base_url должен оканчиваться на /
        if not base_url.endswith('/'):
            base_url += '/'
        try:
            html = _http_get(base_url).decode('utf-8', errors='ignore')
        except Exception:
            return None
        p = _DirIndexParser()
        p.feed(html)
        candidates = []
        for href in p.links:
            # ищем ссылки вида v3.1/  v10.0/
            if not href.lower().startswith('v'):
                continue
            if not href.endswith('/'):
                continue
            v = href.strip('/').strip()
            if re.match(r'^[vV][0-9]+(\.[0-9]+)*$', v):
                candidates.append(v)
        if not candidates:
            return None
        try:
            return max(candidates, key=_parse_version)
        except Exception:
            return None

    def _download_file(url: str, dst: Path):
        dst.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url, headers={"User-Agent": "Kinobot-Updater/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp, open(dst, 'wb') as f:
            shutil.copyfileobj(resp, f)

    def _list_dir(url: str) -> list[str]:
        # возвращает ссылки/имена внутри автоиндекса каталога
        try:
            html = _http_get(url).decode('utf-8', errors='ignore')
        except Exception:
            return []
        p = _DirIndexParser()
        p.feed(html)
        return p.links

    def _decode_text(data: bytes) -> str:
        """Try to decode bytes as UTF-8, fallback to cp1251, then ignore errors."""
        for enc in ("utf-8", "cp1251"):
            try:
                return data.decode(enc)
            except Exception:
                pass
        return data.decode("utf-8", errors="ignore")

    def _get_release_notes(manifest_url: str, latest: Optional[str]) -> Optional[str]:
        """Return release notes text for the latest version if available.

        - For JSON manifest: tries `items[].info` or fetches `items[].info_url` if present.
        - For autoindex dir: fetches `<base>/<latest>/info.txt`.
        """
        if not manifest_url or not latest:
            return None
        try:
            if manifest_url.lower().endswith('.json'):
                data = json.loads(_http_get(manifest_url).decode('utf-8', errors='ignore'))
                items = data.get('items') or []
                item = next((i for i in items if i.get('version') == latest), None)
                if not item:
                    return None
                # Inline text
                if isinstance(item.get('info'), str) and item['info'].strip():
                    return item['info'].strip()
                # External URL to text
                info_url = item.get('info_url')
                if isinstance(info_url, str) and info_url.strip():
                    try:
                        raw = _http_get(info_url.strip())
                        text = _decode_text(raw).strip()
                        return text[:65536] if text else None
                    except Exception:
                        return None
                return None
            else:
                base = manifest_url if manifest_url.endswith('/') else manifest_url + '/'
                info_url = urllib.parse.urljoin(base, f"{latest}/info.txt")
                raw = _http_get(info_url)
                text = _decode_text(raw).strip()
                return text[:65536] if text else None
        except Exception:
            return None

    def _download_dir_recursive(base: str, url: str, dst: Path):
        # base: базовый корень, чтобы не вылезти наружу
        def _safe_rel_from_url(full_url: str) -> Path:
            rel_url = full_url[len(base):]
            # Декодируем percent-encoding и защищаемся от traversal
            parts = [Path(urllib.parse.unquote(seg)).name for seg in rel_url.split('/') if seg]
            return Path(*parts)

        if not url.endswith('/'):
            # файл
            rel_path = _safe_rel_from_url(url)
            out_path = dst / rel_path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            _download_file(url, out_path)
            return
        # каталог с автоиндексом
        links = _list_dir(url)
        for href in links:
            if href in ('../', './'):
                continue
            child = urllib.parse.urljoin(url, href)
            if href.endswith('/'):
                # Создаём папку сразу (чтобы поддерживать пустые каталоги), затем рекурсивно обходим
                rel_dir = _safe_rel_from_url(child)
                (dst / rel_dir).mkdir(parents=True, exist_ok=True)
                _download_dir_recursive(base, child, dst)
            else:
                rel_path = _safe_rel_from_url(child)
                out_path = dst / rel_path
                out_path.parent.mkdir(parents=True, exist_ok=True)
                _download_file(child, out_path)

    def _check_update_status() -> dict:
        cur = _read_version_local()
        url = (settings.UPDATE_MANIFEST_URL or '').strip()
        if not url:
            return {"current": cur, "available": False}
        try:
            if url.lower().endswith('.json'):
                data = json.loads(_http_get(url).decode('utf-8', errors='ignore'))
                items = data.get('items') or []
                latest = data.get('latest')
                if not latest and items:
                    latest = max((i.get('version') for i in items if i.get('version')), key=_parse_version)
                ok = bool(latest and _version_gt(latest, cur))
                notes = _get_release_notes(url, latest) if ok and latest else None
                return {"current": cur, "available": ok, "latest": latest or None, "notes": notes}
            else:
                base = url if url.endswith('/') else url + '/'
                latest = _discover_latest_from_dirbase(base)
                ok = bool(latest and _version_gt(latest, cur))
                notes = _get_release_notes(base, latest) if ok and latest else None
                return {"current": cur, "available": ok, "latest": latest or None, "notes": notes}
        except Exception:
            return {"current": cur, "available": False}

    @app.get("/api/update/status")
    async def update_status(request: Request):
        login_required(request)
        return JSONResponse(_check_update_status())

    @app.post("/api/update/apply")
    async def update_apply(request: Request):
        login_required(request)
        st = _check_update_status()
        if not st.get('available'):
            return JSONResponse({"message": "Обновлений нет", "status": "noop", "current": st.get('current')}, status_code=200)
        latest = st.get('latest')
        url = (settings.UPDATE_MANIFEST_URL or '').strip()
        if not url:
            raise HTTPException(status_code=400, detail="UPDATE_MANIFEST_URL не задан")

        tmp = Path(tempfile.mkdtemp(prefix="kb_upd_api_"))
        plan_path = tmp / "plan.json"

        if url.lower().endswith('.json'):
            # JSON режим: найдём запись latest и скачаем zip
            data = json.loads(_http_get(url).decode('utf-8', errors='ignore'))
            items = data.get('items') or []
            item = next((i for i in items if i.get('version') == latest), None)
            if not item or not item.get('url'):
                raise HTTPException(status_code=404, detail="Запись latest не найдена в манифесте")
            zip_url = item['url']
            zip_path = tmp / 'update.zip'
            _download_file(zip_url, zip_path)
            plan = {
                "zip": str(zip_path),
                "version": latest,
                "cleanup_dir": False,
                "post_install": [],
                "exclude": [
                    ".env",
                    "venv",
                    "data",
                    "posters",
                    "logs",
                    "backups",
                    "films.db",
                    "users.db",
                ],
                "python_exe": sys.executable,
                "app_dir": str(Path(__file__).resolve().parents[2]),
            }
        else:
            # Директории с автоиндексом: рекурсивно скачиваем в staging
            base = url if url.endswith('/') else url + '/'
            version_url = urllib.parse.urljoin(base, latest + '/')
            staging = tmp / 'payload'
            _download_dir_recursive(base, version_url, staging)
            plan = {
                "dir": str(staging),
                "version": latest,
                "cleanup_dir": True,
                "post_install": [],
                "exclude": [
                    ".env",
                    "venv",
                    "data",
                    "posters",
                    "logs",
                    "backups",
                    "films.db",
                    "users.db",
                ],
                "python_exe": sys.executable,
                "app_dir": str(Path(__file__).resolve().parents[2]),
            }

        plan_path.write_text(json.dumps(plan, ensure_ascii=False), encoding='utf-8')

        # Стартуем апдейтер в подпроцессе
        try:
            py = sys.executable
            args = [py, "-m", "app.updater", "--plan", str(plan_path)]
            subprocess.Popen(args, cwd=str(Path(__file__).resolve().parents[2]))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Не удалось запустить апдейтер: {e}")

        return JSONResponse({"message": "Обновление запущено", "status": "started", "version": latest}, status_code=202)

    # API
    # TMDb helpers (with simple cache + retry + fallback to stale cache)
    _tmdb_cache: dict[str, dict] = {}
    _tmdb_ttl_sec = 600

    def _cache_key(path: str, params: dict | None) -> str:
        params = params or {}
        # Do not cache by api_key
        q = {k: v for k, v in params.items() if k != 'api_key'}
        items = sorted(q.items())
        return path + '?' + urllib.parse.urlencode(items)

    def tmdb_request(path: str, params: dict | None = None):
        if not settings.TMDB_API_KEY:
            raise HTTPException(status_code=400, detail="TMDB_API_KEY не задан в .env")
        base = "https://api.themoviedb.org/3"
        q = {"api_key": settings.TMDB_API_KEY, "language": settings.TMDB_LANGUAGE}
        if params:
            q.update(params)
        url = f"{base}{path}?" + urllib.parse.urlencode(q)
        key = _cache_key(path, q)
        now = time.time()
        # fresh cache
        c = _tmdb_cache.get(key)
        if c and (now - c.get('ts', 0) < _tmdb_ttl_sec):
            return c['data']
        last_error = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=12 + attempt * 3) as r:
                    data = r.read()
                    parsed = json.loads(data.decode('utf-8'))
                    _tmdb_cache[key] = {"ts": time.time(), "data": parsed}
                    return parsed
            except Exception as e:
                last_error = e
                # backoff: 0.5s, 1s
                time.sleep(0.5 * (attempt + 1))
        # Fallback to stale cache if present
        if c:
            return c['data']
        raise HTTPException(status_code=502, detail=f"TMDb ошибка: {last_error}")

    @app.get("/api/import/search")
    async def import_search(request: Request, query: str, page: int = 1):
        login_required(request)
        query = (query or "").strip()
        if not query:
            return JSONResponse({"results": [], "page": 1, "total_pages": 0})
        data = tmdb_request("/search/movie", {"query": query, "page": page, "include_adult": "false"})
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
                "poster": (f"{image_base}/w200{it['poster_path']}" if it.get("poster_path") else None),
            })
        return JSONResponse({"results": results, "page": data.get("page", 1), "total_pages": data.get("total_pages", 1)})

    @app.post("/api/import/tmdb/popular")
    async def import_tmdb_popular(request: Request, count: int = Query(..., ge=2, le=50)):
        login_required(request)

        # Запрашиваем популярные фильмы TMDb со случайной страницы,
        # затем случайно выбираем N фильмов с этой страницы
        import random
        first_page = tmdb_request("/movie/popular", {"page": 1})
        total_pages = int(first_page.get("total_pages", 1) or 1)
        max_page = min(total_pages, 500)  # TMDb ограничивает пагинацию 500
        rnd_page = random.randint(1, max_page)
        data = first_page if rnd_page == 1 else tmdb_request("/movie/popular", {"page": rnd_page})
        results = [it.get("id") for it in data.get("results", []) if it.get("id")]
        if not results:
            return JSONResponse({"imported": 0, "skipped": 0, "requested": 0, "items": []})
        ids = random.sample(results, k=min(count, len(results)))
        
        conn = get_db_connection()
        c = conn.cursor()

        imported = []
        skipped = 0

        import sqlite3

        def gen():
            return f"{random.randint(10000, 99999)}"

        for movie_id in ids:
            # Пропуск дубликатов
            c.execute("SELECT id FROM films WHERE external_source = ? AND external_id = ?", ("tmdb", str(movie_id)))
            if c.fetchone():
                skipped += 1
                continue
            try:
                # Детали фильма
                d = tmdb_request(f"/movie/{movie_id}", {})
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
                        with urllib.request.urlopen(url, timeout=15) as r, open(file_path, 'wb') as f:
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
                            (name, description, 1 if photo_id else 0, photo_id, 1, genres, site, code, "tmdb", str(movie_id))
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
            except Exception:
                skipped += 1
                continue

        conn.commit()
        # Итоговое уведомление и обновление списка
        await sio.emit('notification', {'message': f'Импорт популярных TMDb: добавлено {len(imported)} из {len(ids)} (пропущено: {skipped})', 'type': 'success' if imported else 'warning'})
        await sio_get_films()
        conn.close()
        return JSONResponse({"imported": len(imported), "skipped": skipped, "requested": len(ids), "items": imported})

    @app.post("/api/import/tmdb/{movie_id}")
    async def import_tmdb(request: Request, movie_id: int):
        login_required(request)
        # Проверка на дубликат
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM films WHERE external_source = ? AND external_id = ?", ("tmdb", str(movie_id)))
        if cur.fetchone():
            conn.close()
            return JSONResponse({"message": "Фильм уже импортирован"})
        # Детали фильма
        d = tmdb_request(f"/movie/{movie_id}", {})
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
                with urllib.request.urlopen(url, timeout=15) as r, open(file_path, 'wb') as f:
                    f.write(r.read())
                photo_id = fn
            except Exception:
                photo_id = None
        # Генерируем код и вставляем
        import random, sqlite3
        def gen():
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
                        (name, description, 1 if photo_id else 0, photo_id, 1, genres, site, code, "tmdb", str(movie_id))
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
        await sio.emit('notification', {'message': f'Импортировано из TMDb: "{name}". Код: {code}', 'type': 'success'})
        await sio_get_films()
        conn.close()
        return JSONResponse({"message": "Импорт успешно выполнен", "id": film_id, "code": code})


    @app.get("/api/films")
    async def get_films_api(request: Request):
        login_required(request)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM films ORDER BY id DESC")
        films = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(films)

    @app.get("/api/stats")
    async def get_stats(request: Request):
        import datetime as dt
        login_required(request)
        # Films stats
        conn_f = get_db_connection()
        c_f = conn_f.cursor()
        c_f.execute("SELECT COUNT(*) as cnt FROM films")
        films_total = c_f.fetchone()[0]
        c_f.execute("SELECT COUNT(*) FROM films WHERE (photo_status = 1) OR (photo_id IS NOT NULL AND photo_id != '')")
        films_with_image = c_f.fetchone()[0]
        # Агрегация по отдельным жанрам (genre — это строка с перечислением через запятую/точку с запятой)
        import re
        c_f.execute("SELECT genre FROM films")
        rows = c_f.fetchall()
        counts: dict[str, int] = {}
        display: dict[str, str] = {}
        for (gstr,) in rows:
            s = (gstr or "").strip()
            if not s:
                key = "не указан"
                counts[key] = counts.get(key, 0) + 1
                if key not in display: display[key] = "Не указан"
                continue
            # Разбиваем по , или ;, удаляем лишние пробелы, пустые значения
            parts = [p.strip() for p in re.split(r"[,;]", s) if p.strip()]
            if not parts:
                key = "не указан"
                counts[key] = counts.get(key, 0) + 1
                if key not in display: display[key] = "Не указан"
                continue
            for g in parts:
                key = g.lower()
                counts[key] = counts.get(key, 0) + 1
                # Сохраняем человекочитаемую форму (первое встреченное написание)
                if key not in display:
                    display[key] = g
        films_by_genre = sorted([
            {"genre": display.get(k, k.title()), "count": v}
            for k, v in counts.items()
        ], key=lambda x: x["count"], reverse=True)
        c_f.execute("SELECT code, name FROM films ORDER BY id DESC LIMIT 5")
        recent_films = [{"code": row[0], "name": row[1]} for row in c_f.fetchall()]
        conn_f.close()

        # Users stats
        conn_u = get_db_connection('users.db')
        c_u = conn_u.cursor()
        c_u.execute("SELECT COUNT(*) FROM users")
        users_total = c_u.fetchone()[0]
        c_u.execute("SELECT COUNT(*) FROM users WHERE admin = 1")
        admins = c_u.fetchone()[0]
        c_u.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
        banned = c_u.fetchone()[0]

        # Referrals per day (last 7 days)
        c_u.execute("SELECT date(date_referred) as d, COUNT(*) as c FROM referrals WHERE date_referred >= date('now','-6 day') GROUP BY d ORDER BY d")
        raw = {row[0]: row[1] for row in c_u.fetchall()}
        last7 = [(dt.date.today() - dt.timedelta(days=i)).isoformat() for i in range(6,-1,-1)]
        referrals = {"labels": last7, "counts": [raw.get(day, 0) for day in last7]}
        conn_u.close()

        return JSONResponse({
            "films": {
                "total": films_total,
                "with_image": films_with_image,
                "by_genre": films_by_genre,
                "recent": recent_films,
            },
            "users": {
                "total": users_total,
                "admins": admins,
                "banned": banned,
            },
            "referrals": referrals
        })

    @app.get("/api/films/search")
    async def search_films(request: Request, query: str = "", genre: str = ""):
        login_required(request)
        # Регистронезависимый поиск (в т.ч. для кириллицы) + нормализация пробелов/юникода
        import unicodedata as _ud
        def _norm(s: str) -> str:
            s = _ud.normalize("NFKC", (s or ""))
            s = " ".join(s.split())  # схлопываем множественные пробелы
            return s.casefold()
        q = (query or "").strip()
        g = (genre or "").strip()
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM films ORDER BY id DESC")
        films = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if q:
            q_cf = _norm(q)
            films = [f for f in films if (
                q_cf in _norm(str(f.get("id", "")))
                or q_cf in _norm(f.get("code") or "")
                or q_cf in _norm(f.get("name") or "")
            )]

        if g and g != "all":
            import re as _re
            g_cf = _norm(g)
            def _genre_has(genre_str: str) -> bool:
                parts = [_p.strip() for _p in _re.split(r"[,;]", genre_str or "") if _p.strip()]
                for _p in parts:
                    if _norm(_p) == g_cf:
                        return True
                return False
            films = [f for f in films if _genre_has(f.get("genre") or "")]

        return JSONResponse(films)

    @app.post("/api/film")
    async def add_film(request: Request, name: str = Form(...), genre: str = Form(...), description: str = Form(""), site: str = Form(""), image: UploadFile | None = File(None)):
        login_required(request)
        conn = get_db_connection()
        try:
            with conn:
                cursor = conn.cursor()
                # Генерируем уникальный 5-значный код с защитой от гонок
                import random, sqlite3
                def gen():
                    return f"{random.randint(10000, 99999)}"
                code = gen()
                photo_id = None
                if image and allowed_file(image.filename):
                    from werkzeug.utils import secure_filename
                    filename = secure_filename(image.filename)
                    file_path = os.path.join(uploads_path(), filename)
                    with open(file_path, 'wb') as f:
                        f.write(await image.read())
                    photo_id = filename
                # До 7 попыток на случай коллизии кода
                for _ in range(7):
                    try:
                        cursor.execute(
                            """
                            INSERT INTO films (name, description, photo_status, photo_id, activate, genre, site, code)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (name, description, 1 if photo_id else 0, photo_id, 1, genre, site, code)
                        )
                        break
                    except sqlite3.IntegrityError:
                        code = gen()
                film_id = cursor.lastrowid
            await sio.emit('notification', {'message': f'Фильм "{name}" добавлен. Код: {code}', 'type': 'success'})
            await sio_get_films()
            # normalize genres mapping (from provided string)
            try:
                import re as _re
                parts = [p.strip() for p in _re.split(r"[,;]", genre or "") if p.strip()]
                set_film_genres(conn, film_id, parts)
            except Exception:
                pass
            return JSONResponse({"id": film_id, "code": code, "name": name, "message": "Фильм успешно добавлен"}, status_code=201)
        except Exception as e:
            conn.rollback()
            return JSONResponse({"error": "Произошла ошибка при добавлении фильма"}, status_code=500)
        finally:
            conn.close()

    @app.get("/api/film/{id}")
    async def get_film(request: Request, id: int):
        login_required(request)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM films WHERE id = ?", (id,))
        film = cursor.fetchone()
        conn.close()
        if film:
            return JSONResponse(dict(film))
        raise HTTPException(status_code=404, detail="Фильм не найден")

    @app.put("/api/film/{id}")
    async def update_film(request: Request, id: int, name: str = Form(...), genre: str = Form(...), description: str = Form(""), site: str = Form(""), image: UploadFile | None = File(None)):
        login_required(request)
        conn = get_db_connection()
        cursor = conn.cursor()
        if image and allowed_file(image.filename):
            from werkzeug.utils import secure_filename
            filename = secure_filename(image.filename)
            file_path = os.path.join(uploads_path(), filename)
            with open(file_path, 'wb') as f:
                f.write(await image.read())
            cursor.execute(
                """
                UPDATE films SET name = ?, description = ?, photo_status = 1, photo_id = ?, activate = 1, genre = ?, site = ? WHERE id = ?
                """,
                (name, description, filename, genre, site, id)
            )
        else:
            cursor.execute(
                """
                UPDATE films SET name = ?, description = ?, activate = 1, genre = ?, site = ? WHERE id = ?
                """,
                (name, description, genre, site, id)
            )
        conn.commit()
        # normalize genres mapping (from provided string)
        try:
            import re as _re
            parts = [p.strip() for p in _re.split(r"[,;]", genre or "") if p.strip()]
            set_film_genres(conn, id, parts)
        except Exception:
            pass
        await sio.emit('notification', {'message': f'Фильм "{name}" обновлен. Код: {id}', 'type': 'info'})
        await sio_get_films()
        conn.close()
        return JSONResponse({"message": "Фильм успешно обновлен"})

    @app.get("/api/users")
    async def get_users_api(request: Request):
        login_required(request)
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users ORDER BY id DESC")
        users = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return JSONResponse(users)

    @app.post("/api/user/{id}/toggle-admin")
    async def toggle_admin(request: Request, id: int):
        login_required(request)
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT admin, name FROM users WHERE id = ?", (id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        new_status = 0 if user['admin'] else 1
        cursor.execute("UPDATE users SET admin = ? WHERE id = ?", (new_status, id))
        conn.commit()
        await sio.emit('notification', {'message': f'Пользователь "{user["name"]}" теперь {"траффер" if new_status else "пользователь"}', 'type': 'info'})
        await sio_get_users()
        conn.close()
        return JSONResponse({"message": f"Статус пользователя изменен на {'траффер' if new_status else 'пользователь'}"})

    @app.post("/api/user/{id}/ban")
    async def ban_user(request: Request, id: int):
        login_required(request)
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM users WHERE id = ?", (id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        cursor.execute("UPDATE users SET banned = 1 WHERE id = ?", (id,))
        conn.commit()
        await sio.emit('notification', {'message': f'Пользователь "{user["name"]}" забанен', 'type': 'warning'})
        await sio_get_users()
        conn.close()
        return JSONResponse({"message": "Пользователь забанен"})

    @app.post("/api/user/{id}/toggle-ban")
    async def toggle_ban(request: Request, id: int):
        login_required(request)
        conn = get_db_connection('users.db')
        cursor = conn.cursor()
        cursor.execute("SELECT banned, name FROM users WHERE id = ?", (id,))
        user = cursor.fetchone()
        if not user:
            conn.close()
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        new_status = 0 if user['banned'] else 1
        cursor.execute("UPDATE users SET banned = ? WHERE id = ?", (new_status, id))
        conn.commit()
        msg = f'Пользователь "{user["name"]}" {"забанен" if new_status else "разбанен"}'
        await sio.emit('notification', {'message': msg, 'type': 'warning' if new_status else 'success'})
        await sio_get_users()
        conn.close()
        return JSONResponse({"message": msg})

    # Background task queue endpoints (if task manager is available)
    if task_manager is not None:
        @app.post("/api/tasks/import/tmdb/popular")
        async def enqueue_import_tmdb_popular(request: Request, count: int = Query(..., ge=2, le=50)):
            login_required(request)
            job = await task_manager.enqueue("tmdb_popular", {"count": count})
            return JSONResponse({"job_id": job["id"], "status": job["status"]}, status_code=202)

        @app.post("/api/tasks/import/tmdb/{movie_id}")
        async def enqueue_import_tmdb_single(request: Request, movie_id: int):
            login_required(request)
            job = await task_manager.enqueue("tmdb_single", {"movie_id": movie_id})
            return JSONResponse({"job_id": job["id"], "status": job["status"]}, status_code=202)

        @app.get("/api/tasks/{job_id}")
        async def get_task_status(request: Request, job_id: str):
            login_required(request)
            j = task_manager.get_job(job_id)
            if not j:
                raise HTTPException(status_code=404, detail="Задача не найдена")
            return JSONResponse(j)

        @app.get("/api/tasks")
        async def list_tasks(request: Request):
            login_required(request)
            return JSONResponse(task_manager.list_jobs())

    return app
