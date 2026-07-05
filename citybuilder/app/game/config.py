"""Static game balance data: tiers and buildings.

Kept free of any web/bot/db imports so it can be unit tested in isolation.
"""

from __future__ import annotations

# (title, population_threshold) — sorted ascending by threshold.
TIERS: list[tuple[str, int]] = [
    ("Посёлок", 0),
    ("Деревня", 50),
    ("Посёлок городского типа", 200),
    ("Городок", 1000),
    ("Город", 5000),
    ("Крупный город", 25000),
    ("Мегаполис", 100000),
]

BASE_HOUSING_CAPACITY = 20
BASE_HAPPINESS = 50

# category: "housing" | "income" | "happiness" | "infra"
BUILDINGS: dict[str, dict] = {
    "house": {
        "name": "Жилой дом",
        "category": "housing",
        "base_cost": 10,
        "cost_growth": 1.15,
        "effect": 10,  # +population capacity per unit
        "unlock_tier": 0,
    },
    "park": {
        "name": "Парк",
        "category": "happiness",
        "base_cost": 60,
        "cost_growth": 1.18,
        "effect": 2,
        "unlock_tier": 0,
    },
    "farm": {
        "name": "Ферма",
        "category": "income",
        "base_cost": 15,
        "cost_growth": 1.15,
        "effect": 1,  # +money/sec per unit
        "unlock_tier": 0,
    },
    "road": {
        "name": "Дорога",
        "category": "infra",
        "base_cost": 40,
        "cost_growth": 1.12,
        "effect": 1,  # small +happiness (well-connected town)
        "unlock_tier": 0,
    },
    "school": {
        "name": "Школа",
        "category": "happiness",
        "base_cost": 100,
        "cost_growth": 1.2,
        "effect": 3,
        "unlock_tier": 1,
    },
    "hospital": {
        "name": "Больница",
        "category": "happiness",
        "base_cost": 300,
        "cost_growth": 1.2,
        "effect": 4,
        "unlock_tier": 2,
    },
    "factory": {
        "name": "Завод",
        "category": "income",
        "base_cost": 500,
        "cost_growth": 1.17,
        "effect": 8,
        "unlock_tier": 2,
    },
    "mall": {
        "name": "Торговый центр",
        "category": "income",
        "base_cost": 3000,
        "cost_growth": 1.16,
        "effect": 40,
        "unlock_tier": 3,
    },
    "metro": {
        "name": "Метро",
        "category": "infra",
        "base_cost": 15000,
        "cost_growth": 1.2,
        "effect": 10,
        "unlock_tier": 4,
    },
    "university": {
        "name": "Университет",
        "category": "happiness",
        "base_cost": 8000,
        "cost_growth": 1.18,
        "effect": 6,
        "unlock_tier": 4,
    },
    "skyscraper": {
        "name": "Небоскрёб",
        "category": "income",
        "base_cost": 20000,
        "cost_growth": 1.2,
        "effect": 150,
        "unlock_tier": 5,
    },
    "city_hall": {
        "name": "Городская управа",
        "category": "income",
        "base_cost": 50000,
        "cost_growth": 1.25,
        "effect": 500,
        "unlock_tier": 6,
    },
}

# Max offline progress that gets simulated when the player was away.
MAX_OFFLINE_SECONDS = 8 * 60 * 60  # 8 hours
