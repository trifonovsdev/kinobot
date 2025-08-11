from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Tuple
import os, json

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    # Telegram / Bot
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    CHANNELS: List[Tuple[str, str, int]] = []

    # Web
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me")
    UPLOAD_FOLDER: str = os.getenv("UPLOAD_FOLDER", os.path.join("static", "uploads"))
    ALLOWED_EXTENSIONS: set[str] = {"png", "jpg", "jpeg", "gif", "webp"}

    # Server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "5555"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # TMDb
    TMDB_API_KEY: str = os.getenv("TMDB_API_KEY", "")
    TMDB_LANGUAGE: str = os.getenv("TMDB_LANGUAGE", "ru-RU")
    TMDB_IMAGE_BASE: str = os.getenv("TMDB_IMAGE_BASE", "https://image.tmdb.org/t/p")

    UPDATE_MANIFEST_URL: str = "https://update.sgorel.ovh/versions/"

settings = Settings()

# Fallback to legacy config.py
try:
    if not settings.BOT_TOKEN or settings.LOGS_CHAT_ID == 0:
        import config as legacy
        if not settings.BOT_TOKEN and hasattr(legacy, 'bot_token'):
            settings.BOT_TOKEN = getattr(legacy, 'bot_token')
        if settings.LOGS_CHAT_ID == 0 and hasattr(legacy, 'logs'):
            settings.LOGS_CHAT_ID = getattr(legacy, 'logs')
        if not settings.CHANNELS and hasattr(legacy, 'channels'):
            settings.CHANNELS = getattr(legacy, 'channels')
except Exception:
    pass

# Parse CHANNELS from env JSON if not set yet
try:
    if not settings.CHANNELS:
        raw = os.getenv("CHANNELS", "").strip()
        if raw:
            data = json.loads(raw)
            # ожидается список списков: [[name, url, id], ...]
            channels = []
            for item in data:
                if isinstance(item, (list, tuple)) and len(item) >= 3:
                    name, url, cid = item[0], item[1], int(item[2])
                    channels.append((name, url, cid))
            if channels:
                settings.CHANNELS = channels
except Exception:
    pass
