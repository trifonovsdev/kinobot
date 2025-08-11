import asyncio
import signal
import uvicorn
import socketio

from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.core.settings import settings
from app.bot.instance import bot
from app.bot.core import router
from app.web.app import create_app
from app.web.sockets import sio

# === Auto-update helpers ===
import os
import sys
import json
import hashlib
import tempfile
import urllib.request
import shutil
import subprocess
import time
from pathlib import Path
from dotenv import load_dotenv
from html.parser import HTMLParser
from urllib.parse import urljoin, unquote
import socket

APP_DIR = Path(__file__).resolve().parent
VERSION_FILE = APP_DIR / "VERSION"

# Поднимаем переменные из .env для доступа через os.getenv
load_dotenv(dotenv_path=APP_DIR / ".env")


def _parse_version(s: str) -> tuple:
    s = (s or "").strip()
    if s.startswith(("v", "V")):
        s = s[1:]
    parts = []
    for p in s.split('.'):
        try:
            parts.append(int(p))
        except ValueError:
            num = ''.join(ch for ch in p if ch.isdigit())
            parts.append(int(num) if num else 0)
    return tuple(parts or [0])


def _version_gt(a: str, b: str) -> bool:
    return _parse_version(a) > _parse_version(b)


def _get_current_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "v0"


def _fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)


def _download(url: str, dst: Path):
    with urllib.request.urlopen(url, timeout=60) as r, open(dst, "wb") as f:
        shutil.copyfileobj(r, f)


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _print_update_banner(current: str, latest: str | None, found: bool, info: str | None = None) -> None:
    bar = "=" * 56
    print("\n" + bar)
    print(" Проверка обновлений...")
    if found and latest:
        print(f" Обновление {latest} найдено!")
        if info:
            print(info.strip())
            print()
        print(" Готовы обновиться прямо сейчас?")
    else:
        print(" Обновлений не найдено.")
    print(f"\n Текущая версия: {current}")
    print(bar + "\n")


def _confirm_update(latest: str, current: str) -> bool:
    # В интерактивной консоли спрашиваем подтверждение.
    # Правила: y/Y — да; n/N — нет; Enter или любой другой символ — Согласие (да).
    try:
        if not sys.stdin.isatty():
            # Нет TTY (например, systemd) — не зависаем, продолжаем автоматически
            print("[updater] Нет TTY: продолжаю обновление автоматически")
            return True
    except Exception:
        return True

    print(" Нажмите: y/Y — да, n/N — нет, Enter или любой другой символ — согласие")
    try:
        resp = input("> ").strip()
    except EOFError:
        # На всякий случай не блокируемся
        return True
    if resp in ("n", "N"):
        print(" Обновление отклонено пользователем. Продолжаю запуск без обновления.\n")
        return False
    return True


class _DirIndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if href:
            self.hrefs.append(href)


def _fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=15) as r:
        return r.read().decode("utf-8", errors="replace")


def _list_dir_hrefs(url: str) -> list[str]:
    # Парсим autoindex HTML и собираем ссылки
    html = _fetch_text(url)
    p = _DirIndexParser()
    p.feed(html)
    return p.hrefs


def _discover_latest_from_dirbase(base_url: str) -> str | None:
    # Ищем подпапки вида v*, выбираем максимальную по версии
    hrefs = _list_dir_hrefs(base_url)
    candidates = []
    for h in hrefs:
        if h.startswith("../"):
            continue
        # интересуют только директории версий, у которых есть завершающий "/"
        if not h.endswith('/'):
            continue
        name = h.rstrip('/')
        if name.lower().startswith('v') and any(ch.isdigit() for ch in name[1:]):
            candidates.append(name)
    if not candidates:
        return None
    latest = max((c for c in candidates), key=_parse_version)
    return latest


def _download_dir_recursive(base_url: str, current_url: str, dst_dir: Path, rel_prefix: Path = Path("")):
    dst_dir.mkdir(parents=True, exist_ok=True)
    for h in _list_dir_hrefs(current_url):
        if h.startswith("../"):
            continue
        # Полный URL на элемент
        item_url = urljoin(current_url, h)
        name = h.rstrip('/')
        if not name or name in {"?C=M;O=D", "?C=N;O=D", "?C=S;O=A"}:  # мусорные ссылки автоиндекса
            continue
        if h.endswith('/'):
            # Подкаталог
            decoded = unquote(name)
            safe_name = Path(decoded).name  # защита от traversal
            _download_dir_recursive(base_url, item_url, dst_dir, rel_prefix / safe_name)
        else:
            # Файл
            decoded = unquote(name)
            safe_name = Path(decoded).name  # защита от traversal
            out_path = dst_dir / rel_prefix / safe_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            _download(item_url, out_path)


def check_and_stage_update():
    """Проверяет наличие обновления. Поддерживает 2 режима:
    1) UPDATE_MANIFEST_URL указывает на .json манифест (старый режим)
    2) UPDATE_MANIFEST_URL указывает на базовую папку с автоиндексом (напр. .../versions/)
       Тогда будет автоматически выбрана последняя папка v*, скачаны все файлы из неё (рекурсивно),
       и сформирован план обновления из локальной staging-директории.
    """
    url = settings.UPDATE_MANIFEST_URL
    auto = os.getenv("AUTO_UPDATE", "1").lower() in {"1", "true", "yes", "on"}
    if not auto or not url:
        return None

    cur = _get_current_version()
    if url.strip().lower().endswith(".json"):
        # Режим манифеста
        m = _fetch_json(url)
        items = m.get("items") or []
        latest = m.get("latest")
        if not latest and items:
            try:
                latest = max((i.get("version") for i in items if i.get("version")), key=_parse_version)
            except ValueError:
                latest = None
        if not latest or not _version_gt(latest, cur):
            _print_update_banner(cur, latest, found=False)
            return None
        item = next((i for i in items if i.get("version") == latest), None)
        if not item or not item.get("url"):
            return None
        _print_update_banner(cur, latest, found=True)
        if not _confirm_update(latest, cur):
            return None
        tmp = Path(tempfile.mkdtemp(prefix="kb_upd_"))
        zip_path = tmp / "update.zip"
        _download(item["url"], zip_path)
        sha = item.get("sha256")
        if sha and _sha256_file(zip_path).lower() != str(sha).lower():
            raise RuntimeError("Хэш обновления не совпал")
        exclude = item.get("exclude") or [
            ".env",
            "venv",
            "data",
            "posters",
            "logs",
            "backups",
            "films.db",
            "users.db",
        ]
        plan = {
            "zip": str(zip_path),
            "version": latest,
            "exclude": exclude,
            "python_exe": sys.executable,
            "app_dir": str(APP_DIR),
            "post_install": item.get("post_install", []),
        }
        return plan
    else:
        # Режим «просто кладу файлы в /versions/vX.Y»
        base = url if url.endswith('/') else url + '/'
        latest = _discover_latest_from_dirbase(base)
        if not latest or not _version_gt(latest, cur):
            _print_update_banner(cur, latest, found=False)
            return None
        # URL выбранной версии
        version_url = urljoin(base, latest + '/')
        # Читаем описание обновления, если есть info.txt в каталоге версии
        info_txt = None
        try:
            info_txt = _fetch_text(urljoin(version_url, "info.txt"))
        except Exception:
            info_txt = None
        _print_update_banner(cur, latest, found=True, info=info_txt)
        if not _confirm_update(latest, cur):
            return None
        # Скачиваем содержимое папки версии рекурсивно в staging
        tmp = Path(tempfile.mkdtemp(prefix="kb_upd_"))
        staging = tmp / "payload"
        _download_dir_recursive(base, version_url, staging)
        exclude = [
            ".env",
            "venv",
            "data",
            "posters",
            "logs",
            "backups",
            "films.db",
            "users.db",
        ]
        plan = {
            "dir": str(staging),
            "version": latest,
            "exclude": exclude,
            "python_exe": sys.executable,
            "app_dir": str(APP_DIR),
            "post_install": [],
            "cleanup_dir": True,
        }
        return plan


def run_updater_and_exit(plan: dict) -> None:
    plan_path = Path(tempfile.gettempdir()) / "kb_update_plan.json"
    plan_path.write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")
    # Запускаем воркер обновления и выходим
    subprocess.Popen([sys.executable, "-m", "app.updater", "--plan", str(plan_path)], cwd=str(APP_DIR))
    time.sleep(0.5)
    sys.exit(0)


def start_bot() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return dp


async def run_bot(dp: Dispatcher):
    # Запуск polling в отдельной задаче; корректно завершается по CancelledError
    await dp.start_polling(bot)


async def run_server():
    # Асинхронный запуск uvicorn без отдельного потока — корректно ловит SIGINT/SIGTERM
    app = create_app()
    asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)
    host = settings.HOST
    port = settings.PORT

    # Проверка занятости порта и авто-фоллбек при необходимости
    def _can_bind(h: str, p: int) -> bool:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((h, p))
            s.close()
            return True
        except OSError:
            return False

    if not _can_bind(host, port):
        auto_fb = os.getenv("PORT_AUTO_FALLBACK", "0").lower() in {"1", "true", "yes", "on"}
        if auto_fb:
            picked = None
            for p in range(port + 1, port + 21):
                if _can_bind(host, p):
                    picked = p
                    break
            if picked is not None:
                print(f"[server] Порт {host}:{port} занят, использую свободный порт {picked}")
                port = picked
            else:
                print(f"[server] Порт {host}:{port} занят, и не удалось найти свободный порт рядом.\n"
                      f"Совет (Windows):\n  netstat -ano | findstr :{port}\n  taskkill /PID <PID> /F\n"
                      f"Совет (Linux):\n  sudo ss -lptn 'sport = :{port}'\n  sudo kill -9 <PID>")
                sys.exit(1)
        else:
            print(f"[server] Порт {host}:{port} занят. Включите авто-подбор порта: PORT_AUTO_FALLBACK=1 в .env\n"
                  f"или освободите порт.\n"
                  f"Windows:\n  netstat -ano | findstr :{port}\n  taskkill /PID <PID> /F\n"
                  f"Linux:\n  sudo ss -lptn 'sport = :{port}'\n  sudo kill -9 <PID>")
            sys.exit(1)

    config = uvicorn.Config(asgi_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    # Отключаем установку обработчиков сигналов внутри uvicorn,
    # чтобы CTRL+C не вызывал лишние исключения в наших задачах
    try:
        server.install_signal_handlers = False
    except Exception:
        pass
    try:
        await server.serve()
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        # Тихо выходим при CTRL+C/остановке, без трейсбека
        pass


async def main_async():
    dp = start_bot()
    server_task = asyncio.create_task(run_server(), name="uvicorn")
    bot_task = asyncio.create_task(run_bot(dp), name="bot")

    # Кроссплатформенное завершение по Ctrl+C и SIGTERM
    stop_event = asyncio.Event()

    def _handle_signal():
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _handle_signal)
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)
    except NotImplementedError:
        # Windows: add_signal_handler может быть недоступен, полагаемся на KeyboardInterrupt
        pass

    try:
        await asyncio.wait(
            {server_task, bot_task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
    except KeyboardInterrupt:
        pass
    finally:
        for t in (server_task, bot_task):
            if not t.done():
                t.cancel()
        # Закрываем HTTP-сессию бота
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    # Ранняя проверка обновлений; при наличии — запускаем воркер и завершаемся
    try:
        _plan = check_and_stage_update()
        if _plan:
            run_updater_and_exit(_plan)
    except Exception as _e:
        print(f"[updater] Ошибка проверки обновления: {_e}")
    asyncio.run(main_async())
