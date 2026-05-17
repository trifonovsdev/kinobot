# KinoBot v4

Modern Telegram film bot with real-time admin panel.

## Architecture

```
app/
├── bot/            # Telegram bot (aiogram 3)
│   ├── handlers/   # Route handlers (start, films, profile)
│   ├── keyboards/  # Inline keyboard factories
│   └── middlewares/ # Ban, subscription checks
├── core/           # Settings, security, logging
├── db/             # Async SQLite with WAL mode
├── models/         # Pydantic domain models
├── repositories/   # Data access layer
├── services/       # Business logic (TMDb, tasks)
└── web/            # FastAPI admin panel + Socket.IO
```

## Features

- **Telegram Bot**: Film search by code, genre picker, profile, referral system
- **Admin Panel**: Modern glassmorphism UI, dark/light theme, real-time updates
- **TMDb Import**: Search + batch import with background queue
- **Security**: Rate-limited login, configurable credentials, CSRF protection
- **Database**: Async SQLite with WAL mode for better concurrency
- **Auto-Update**: OTA updates from manifest or directory index

## Quick Start

```bash
# 1. Clone and install
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your BOT_TOKEN, ADMIN_PASSWORD, etc.

# 3. Run
python main.py
```

## Configuration

All settings are in `.env` — see `.env.example` for documentation.

**Required:**
- `BOT_TOKEN` — Telegram bot token from @BotFather
- `ADMIN_PASSWORD` — Admin panel password (change from default!)

**Optional:**
- `TMDB_API_KEY` — For TMDb import feature
- `CHANNELS` — JSON array of channels for subscription check
- `SECRET_KEY` — Session encryption key (auto-generated if not set)

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Bot | aiogram 3.4 |
| Web | FastAPI + uvicorn |
| Real-time | Socket.IO |
| Database | SQLite (async via aiosqlite, WAL mode) |
| HTTP Client | httpx (async) |
| Frontend | Vanilla JS + Chart.js |

## Security

- Passwords configurable via `.env` (no hardcoded `root/root`)
- Rate-limited login (5 attempts/minute)
- Session-based auth with secure random key
- Input validation via Pydantic models
- Safe file uploads with extension whitelist
