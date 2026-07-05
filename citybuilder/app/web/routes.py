from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from app.core.settings import settings
from app.db import storage
from app.game import logic
from app.web.auth import InvalidInitData, validate_init_data

router = APIRouter(prefix="/api/game", tags=["game"])


def _resolve_user_id(x_telegram_init_data: Optional[str], dev_user_id: Optional[int]) -> int:
    if settings.TESTING and dev_user_id is not None:
        return int(dev_user_id)
    if not x_telegram_init_data:
        raise HTTPException(status_code=401, detail="missing_init_data")
    try:
        user = validate_init_data(x_telegram_init_data, settings.BOT_TOKEN)
    except InvalidInitData as exc:
        raise HTTPException(status_code=401, detail=f"invalid_init_data: {exc}") from exc
    return int(user["id"])


def _load_or_create(telegram_id: int) -> dict[str, Any]:
    state = storage.load_state(telegram_id)
    if state is None:
        state = logic.new_game_state()
        storage.save_state(telegram_id, state)
    state = logic.apply_offline_progress(state)
    storage.save_state(telegram_id, state)
    return state


@router.get("/state")
def get_state(
    x_telegram_init_data: Optional[str] = Header(default=None),
    dev_user_id: Optional[int] = Query(default=None),
):
    telegram_id = _resolve_user_id(x_telegram_init_data, dev_user_id)
    state = _load_or_create(telegram_id)
    return logic.public_state(state)


@router.post("/name")
def set_city_name(
    payload: dict,
    x_telegram_init_data: Optional[str] = Header(default=None),
    dev_user_id: Optional[int] = Query(default=None),
):
    telegram_id = _resolve_user_id(x_telegram_init_data, dev_user_id)
    state = _load_or_create(telegram_id)
    if state.get("city_name"):
        raise HTTPException(status_code=400, detail="name_already_set")
    name = str(payload.get("name", "")).strip()
    if not (2 <= len(name) <= 24):
        raise HTTPException(status_code=400, detail="invalid_name_length")
    state["city_name"] = name
    storage.save_state(telegram_id, state)
    return logic.public_state(state)


@router.post("/build")
def build(
    payload: dict,
    x_telegram_init_data: Optional[str] = Header(default=None),
    dev_user_id: Optional[int] = Query(default=None),
):
    telegram_id = _resolve_user_id(x_telegram_init_data, dev_user_id)
    state = _load_or_create(telegram_id)
    building_id = payload.get("building_id")
    ok, reason = logic.can_build(state, building_id)
    if not ok:
        raise HTTPException(status_code=400, detail=reason)
    state = logic.apply_build(state, building_id)
    storage.save_state(telegram_id, state)
    return logic.public_state(state)
