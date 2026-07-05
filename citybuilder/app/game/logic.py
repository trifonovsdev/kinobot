"""Pure, framework-free city-builder simulation logic.

Every function here operates on plain dicts/numbers so it can be unit
tested without FastAPI, aiogram or sqlite involved.
"""

from __future__ import annotations

import time
from typing import Any

from app.game.config import (
    BASE_HAPPINESS,
    BASE_HOUSING_CAPACITY,
    BUILDINGS,
    MAX_OFFLINE_SECONDS,
    TIERS,
)

# Simulation step size used while chunking a long offline period.
_SIM_STEP_SECONDS = 30


def new_game_state(city_name: str | None = None) -> dict[str, Any]:
    """Return a freshly initialised game state."""
    return {
        "city_name": city_name,
        "population": 0.0,
        "money": 50.0,
        "happiness": float(BASE_HAPPINESS),
        "buildings": {},
        "last_seen": time.time(),
    }


def get_tier_index(population: float) -> int:
    """Return the index into TIERS for the given population."""
    idx = 0
    for i, (_name, threshold) in enumerate(TIERS):
        if population >= threshold:
            idx = i
        else:
            break
    return idx


def get_tier_name(population: float) -> str:
    return TIERS[get_tier_index(population)][0]


def building_cost(building_id: str, owned_count: int) -> float:
    cfg = BUILDINGS[building_id]
    return round(cfg["base_cost"] * (cfg["cost_growth"] ** owned_count), 2)


def is_building_unlocked(building_id: str, population: float) -> bool:
    cfg = BUILDINGS[building_id]
    return get_tier_index(population) >= cfg["unlock_tier"]


def housing_capacity(buildings: dict[str, int]) -> float:
    houses = buildings.get("house", 0)
    return BASE_HOUSING_CAPACITY + houses * BUILDINGS["house"]["effect"]


def happiness_points(buildings: dict[str, int]) -> float:
    total = 0.0
    for bid, count in buildings.items():
        cfg = BUILDINGS.get(bid)
        if cfg and cfg["category"] in ("happiness", "infra"):
            total += cfg["effect"] * count
    return total


def base_income_per_second(buildings: dict[str, int]) -> float:
    total = 0.0
    for bid, count in buildings.items():
        cfg = BUILDINGS.get(bid)
        if cfg and cfg["category"] == "income":
            total += cfg["effect"] * count
    return total


def compute_happiness(population: float, buildings: dict[str, int]) -> float:
    capacity = housing_capacity(buildings)
    occupancy = (population / capacity) if capacity > 0 else 0.0
    crowding_penalty = max(0.0, occupancy - 0.8) * 100.0
    happiness = BASE_HAPPINESS + happiness_points(buildings) - crowding_penalty
    return _clamp(happiness, 0.0, 100.0)


def income_multiplier(happiness: float) -> float:
    if happiness >= 80:
        return 1.2
    if happiness < 40:
        return 0.7
    return 1.0


def growth_multiplier(happiness: float) -> float:
    if happiness >= 80:
        return 1.5
    if happiness < 20:
        return -0.3  # population can shrink when the city is miserable
    if happiness < 40:
        return 0.5
    return 1.0


def compute_income_per_second(population: float, buildings: dict[str, int]) -> float:
    happiness = compute_happiness(population, buildings)
    return base_income_per_second(buildings) * income_multiplier(happiness)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def simulate_tick(state: dict[str, Any], dt_seconds: float) -> dict[str, Any]:
    """Advance the state by dt_seconds (small step) and return a new state."""
    if dt_seconds <= 0:
        return state

    population = state["population"]
    money = state["money"]
    buildings = state["buildings"]

    happiness = compute_happiness(population, buildings)
    capacity = housing_capacity(buildings)

    vacancy = capacity - population
    g_mult = growth_multiplier(happiness)
    if vacancy > 0:
        growth = vacancy * 0.01 * g_mult * dt_seconds
    else:
        # No room to grow; only decline applies (if g_mult negative).
        growth = population * (g_mult / 100.0) * dt_seconds if g_mult < 0 else 0.0
    new_population = max(0.0, population + growth)
    if growth > 0:
        new_population = min(new_population, capacity)

    income = base_income_per_second(buildings) * income_multiplier(happiness)
    new_money = money + income * dt_seconds

    new_state = dict(state)
    new_state["population"] = new_population
    new_state["money"] = round(new_money, 2)
    new_state["happiness"] = compute_happiness(new_population, buildings)
    return new_state


def simulate(state: dict[str, Any], elapsed_seconds: float, max_offline_seconds: float = MAX_OFFLINE_SECONDS) -> dict[str, Any]:
    """Advance state by elapsed_seconds, chunked into fixed-size steps.

    elapsed_seconds is capped at max_offline_seconds to avoid runaway
    offline rewards.
    """
    elapsed_seconds = max(0.0, min(elapsed_seconds, max_offline_seconds))
    remaining = elapsed_seconds
    current = state
    while remaining > 0:
        step = min(_SIM_STEP_SECONDS, remaining)
        current = simulate_tick(current, step)
        remaining -= step
    return current


def apply_offline_progress(state: dict[str, Any], now: float | None = None) -> dict[str, Any]:
    """Simulate the time elapsed since state['last_seen'] and update it."""
    now = now if now is not None else time.time()
    last_seen = state.get("last_seen", now)
    elapsed = now - last_seen
    new_state = simulate(state, elapsed)
    new_state["last_seen"] = now
    return new_state


def can_build(state: dict[str, Any], building_id: str) -> tuple[bool, str | None]:
    if building_id not in BUILDINGS:
        return False, "unknown_building"
    population = state["population"]
    if not is_building_unlocked(building_id, population):
        return False, "locked"
    owned = state["buildings"].get(building_id, 0)
    cost = building_cost(building_id, owned)
    if state["money"] < cost:
        return False, "insufficient_funds"
    return True, None


def apply_build(state: dict[str, Any], building_id: str) -> dict[str, Any]:
    ok, reason = can_build(state, building_id)
    if not ok:
        raise ValueError(reason)
    owned = state["buildings"].get(building_id, 0)
    cost = building_cost(building_id, owned)
    new_buildings = dict(state["buildings"])
    new_buildings[building_id] = owned + 1
    new_state = dict(state)
    new_state["money"] = round(state["money"] - cost, 2)
    new_state["buildings"] = new_buildings
    new_state["happiness"] = compute_happiness(state["population"], new_buildings)
    return new_state


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    """Shape the state for the API/frontend, including derived fields."""
    population = state["population"]
    buildings = state["buildings"]
    tier_index = get_tier_index(population)
    building_list = []
    for bid, cfg in BUILDINGS.items():
        owned = buildings.get(bid, 0)
        building_list.append(
            {
                "id": bid,
                "name": cfg["name"],
                "category": cfg["category"],
                "owned": owned,
                "unlocked": is_building_unlocked(bid, population),
                "unlock_tier": cfg["unlock_tier"],
                "next_cost": building_cost(bid, owned),
                "effect": cfg["effect"],
            }
        )
    return {
        "city_name": state.get("city_name"),
        "population": round(population, 2),
        "money": round(state["money"], 2),
        "happiness": round(state["happiness"], 1),
        "tier_index": tier_index,
        "tier_name": TIERS[tier_index][0],
        "next_tier": (
            {"name": TIERS[tier_index + 1][0], "population_threshold": TIERS[tier_index + 1][1]}
            if tier_index + 1 < len(TIERS)
            else None
        ),
        "housing_capacity": round(housing_capacity(buildings), 2),
        "income_per_second": round(compute_income_per_second(population, buildings), 2),
        "buildings": building_list,
    }
