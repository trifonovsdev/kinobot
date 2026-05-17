"""
Application settings with validation via pydantic-settings.
All secrets are loaded from environment / .env file.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List, Tuple
import json
import secrets


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Telegram / Bot ---
    BOT_TOKEN: str = ""
    CHANNELS: List[Tuple[str, str, int]] = []

    # --- Web Admin ---
    SECRET_KEY: str = secrets.token_hex(32)
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = "change-me-strong-password"

    # --- Server ---
    HOST: str = "0.0.0.0"
    PORT: int = 5555
    DEBUG: bool = False

    # --- Upload ---
    UPLOAD_FOLDER: str = "static/uploads"
    ALLOWED_EXTENSIONS: set = {"png", "jpg", "jpeg", "gif", "webp"}

    # --- TMDb ---
    TMDB_API_KEY: str = ""
    TMDB_LANGUAGE: str = "ru-RU"
    TMDB_IMAGE_BASE: str = "https://image.tmdb.org/t/p"

    # --- Auto-Update ---
    UPDATE_MANIFEST_URL: str = "https://update.sgorel.ovh/versions/"
    AUTO_UPDATE: bool = True

    # --- Security ---
    LOGIN_RATE_LIMIT: int = 5
    SESSION_LIFETIME_HOURS: int = 24

    # --- Database ---
    DB_WAL_MODE: bool = True

    @field_validator("CHANNELS", mode="before")
    @classmethod
    def parse_channels(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            try:
                data = json.loads(v)
                channels = []
                for item in data:
                    if isinstance(item, (list, tuple)) and len(item) >= 3:
                        channels.append((str(item[0]), str(item[1]), int(item[2])))
                return channels
            except (json.JSONDecodeError, ValueError):
                return []
        return v or []


settings = Settings()
