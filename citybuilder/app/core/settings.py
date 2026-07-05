from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Telegram bot that launches the Mini App
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    # Public HTTPS URL where the Mini App is served (required by Telegram WebApp)
    WEBAPP_URL: str = os.getenv("WEBAPP_URL", "http://localhost:5566")

    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me")

    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "5566"))

    DB_PATH: str = os.getenv("DB_PATH", "citybuilder.db")

    # When true, /api/game/* accepts a plain `?dev_user_id=` query param
    # instead of a validated Telegram initData header. Never enable in prod.
    TESTING: bool = os.getenv("TESTING", "false").lower() == "true"


settings = Settings()
