import time

import pytest

from app.game import logic
from app.game.config import BUILDINGS, MAX_OFFLINE_SECONDS, TIERS


def test_tier_thresholds():
    assert logic.get_tier_index(0) == 0
    assert logic.get_tier_name(0) == "Посёлок"
    assert logic.get_tier_index(49) == 0
    assert logic.get_tier_index(50) == 1
    assert logic.get_tier_name(50) == "Деревня"
    assert logic.get_tier_index(99999) == len(TIERS) - 2
    assert logic.get_tier_index(1_000_000) == len(TIERS) - 1


def test_building_cost_curve_is_geometric():
    base = BUILDINGS["house"]["base_cost"]
    growth = BUILDINGS["house"]["cost_growth"]
    assert logic.building_cost("house", 0) == base
    assert logic.building_cost("house", 1) == round(base * growth, 2)
    assert logic.building_cost("house", 5) == round(base * growth**5, 2)
    # Costs must be strictly increasing.
    costs = [logic.building_cost("house", i) for i in range(10)]
    assert costs == sorted(costs)
    assert len(set(costs)) == len(costs)


def test_building_unlock_gating():
    assert logic.is_building_unlocked("house", 0) is True
    assert logic.is_building_unlocked("school", 0) is False
    assert logic.is_building_unlocked("school", 50) is True
    assert logic.is_building_unlocked("city_hall", 0) is False
    assert logic.is_building_unlocked("city_hall", 100000) is True


def test_apply_build_deducts_money_and_increments_count():
    state = logic.new_game_state("Тестград")
    state["money"] = 1000.0
    cost = logic.building_cost("farm", 0)
    new_state = logic.apply_build(state, "farm")
    assert new_state["buildings"]["farm"] == 1
    assert new_state["money"] == round(1000.0 - cost, 2)
    # Original state must be untouched (pure function).
    assert state["buildings"] == {}


def test_apply_build_rejects_when_insufficient_funds():
    state = logic.new_game_state("Тестград")
    state["money"] = 0.0
    with pytest.raises(ValueError):
        logic.apply_build(state, "farm")


def test_apply_build_rejects_locked_building():
    state = logic.new_game_state("Тестград")
    state["money"] = 10_000.0
    with pytest.raises(ValueError):
        logic.apply_build(state, "school")  # requires tier 1


def test_population_grows_toward_capacity_with_high_happiness():
    state = logic.new_game_state("Тестград")
    state["money"] = 100000.0
    for _ in range(5):
        state = logic.apply_build(state, "house")
    for _ in range(10):
        state = logic.apply_build(state, "park")
    capacity = logic.housing_capacity(state["buildings"])
    assert state["population"] == 0.0

    simulated = logic.simulate(state, elapsed_seconds=3600)
    assert simulated["population"] > 0
    assert simulated["population"] <= capacity + 1e-6


def test_population_never_exceeds_capacity_over_long_simulation():
    state = logic.new_game_state("Тестград")
    state["money"] = 100000.0
    for _ in range(3):
        state = logic.apply_build(state, "house")
    capacity = logic.housing_capacity(state["buildings"])
    simulated = logic.simulate(state, elapsed_seconds=200000, max_offline_seconds=200000)
    assert simulated["population"] <= capacity + 1e-6


def test_low_happiness_can_shrink_population():
    state = logic.new_game_state("Тестград")
    state["buildings"] = {"house": 1}  # small capacity, no happiness buildings
    state["population"] = 100.0  # badly overcrowded
    state["happiness"] = logic.compute_happiness(state["population"], state["buildings"])
    assert state["happiness"] < 20
    simulated = logic.simulate_tick(state, dt_seconds=100)
    assert simulated["population"] < state["population"]


def test_happiness_is_clamped_between_0_and_100():
    state = logic.new_game_state("Тестград")
    state["buildings"] = {"park": 1000, "school": 1000}
    happiness = logic.compute_happiness(0, state["buildings"])
    assert 0.0 <= happiness <= 100.0


def test_offline_progress_is_capped():
    state = logic.new_game_state("Тестград")
    state["money"] = 100000.0
    state = logic.apply_build(state, "farm")
    state["last_seen"] = time.time() - (100 * 3600)  # 100 hours ago

    result = logic.apply_offline_progress(state)
    capped_only = logic.simulate(dict(state, last_seen=None), MAX_OFFLINE_SECONDS)
    # Money earned should match simulating exactly MAX_OFFLINE_SECONDS, not 100h.
    assert result["money"] == pytest.approx(capped_only["money"], rel=1e-6)


def test_public_state_shape():
    state = logic.new_game_state("Тестград")
    state["money"] = 500.0
    state = logic.apply_build(state, "house")
    public = logic.public_state(state)
    assert public["city_name"] == "Тестград"
    assert public["tier_name"] == "Посёлок"
    assert public["next_tier"]["name"] == "Деревня"
    assert any(b["id"] == "house" and b["owned"] == 1 for b in public["buildings"])
    assert isinstance(public["income_per_second"], float)
