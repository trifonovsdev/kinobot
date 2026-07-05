"""SQLite persistence for per-user game state."""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

from app.core.settings import settings

_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


_conn: Optional[sqlite3.Connection] = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = _connect()
        init_db(_conn)
    return _conn


def init_db(conn: sqlite3.Connection | None = None) -> None:
    conn = conn or get_conn()
    with _lock:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                telegram_id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL
            )
            """
        )
        conn.commit()


def load_state(telegram_id: int) -> Optional[dict[str, Any]]:
    conn = get_conn()
    with _lock:
        row = conn.execute(
            "SELECT state_json FROM players WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
    if row is None:
        return None
    return json.loads(row["state_json"])


def save_state(telegram_id: int, state: dict[str, Any]) -> None:
    conn = get_conn()
    payload = json.dumps(state, ensure_ascii=False)
    with _lock:
        conn.execute(
            """
            INSERT INTO players (telegram_id, state_json) VALUES (?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET state_json = excluded.state_json
            """,
            (telegram_id, payload),
        )
        conn.commit()
