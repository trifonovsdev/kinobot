import asyncio
import signal

import uvicorn
from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.bot.core import router
from app.bot.instance import bot
from app.core.settings import settings
from app.db.storage import init_db
from app.web.app import create_app


def start_bot() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return dp


async def run_bot(dp: Dispatcher) -> None:
    try:
        await dp.start_polling(bot)
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:  # invalid/missing BOT_TOKEN, network issues, etc.
        print(f"[bot] Не удалось запустить polling: {exc}")
        # Keep the task alive (idle) instead of tearing down the whole app,
        # so the web app / Mini App API keeps working even without a bot.
        await asyncio.Event().wait()


async def run_server() -> None:
    app = create_app()
    config = uvicorn.Config(app, host=settings.HOST, port=settings.PORT, log_level="info")
    server = uvicorn.Server(config)
    try:
        server.install_signal_handlers = False
    except Exception:
        pass
    try:
        await server.serve()
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        pass


async def main_async() -> None:
    init_db()
    dp = start_bot()
    server_task = asyncio.create_task(run_server(), name="uvicorn")
    bot_task = asyncio.create_task(run_bot(dp), name="bot")

    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _handle_signal)
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)
    except NotImplementedError:
        pass

    try:
        await asyncio.wait(
            {server_task, bot_task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for t in (server_task, bot_task):
            if not t.done():
                t.cancel()
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main_async())
