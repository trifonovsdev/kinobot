"""
KinoBot v4 — Main Entry Point
Runs Telegram bot + Web admin panel concurrently.
"""

import asyncio
import signal
import sys
from pathlib import Path

import socketio
import uvicorn
from dotenv import load_dotenv

# Load .env before anything else
APP_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=APP_DIR / ".env")

from app.core.settings import settings
from app.core.logging import logger
from app.bot.instance import bot, dp
from app.bot.core import setup_bot
from app.web.app import create_app
from app.web.sockets import sio


async def run_bot():
    """Start Telegram bot polling."""
    setup_bot()
    logger.info("Starting Telegram bot polling...")
    await dp.start_polling(bot)


async def run_server():
    """Start FastAPI + Socket.IO web server."""
    app = create_app()
    asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)

    config = uvicorn.Config(
        asgi_app,
        host=settings.HOST,
        port=settings.PORT,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)

    try:
        server.install_signal_handlers = False
    except Exception:
        pass

    logger.info(f"Starting web server on {settings.HOST}:{settings.PORT}")
    await server.serve()


async def main():
    """Run bot and server concurrently."""
    # Graceful shutdown
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows

    server_task = asyncio.create_task(run_server(), name="web-server")
    bot_task = asyncio.create_task(run_bot(), name="telegram-bot")

    # Wait for any to complete or stop signal
    try:
        await asyncio.wait(
            {server_task, bot_task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
    except KeyboardInterrupt:
        pass
    finally:
        for task in (server_task, bot_task):
            if not task.done():
                task.cancel()
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("Application stopped")


if __name__ == "__main__":
    # Optional: check for updates before starting
    if settings.AUTO_UPDATE:
        try:
            from app.updater import check_update_available, get_current_version
            status = check_update_available()
            current = get_current_version()
            if status.get("available"):
                logger.info(
                    f"Update available: {status.get('latest')} (current: {current})"
                )
            else:
                logger.info(f"Running version: {current} (up to date)")
        except Exception as e:
            logger.warning(f"Update check failed: {e}")

    asyncio.run(main())
