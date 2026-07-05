(function () {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  if (tg) {
    tg.ready();
    tg.expand();
  }

  const params = new URLSearchParams(window.location.search);
  const devUserId = params.get("dev_user_id");

  function apiUrl(path) {
    if (devUserId) {
      const sep = path.includes("?") ? "&" : "?";
      return path + sep + "dev_user_id=" + encodeURIComponent(devUserId);
    }
    return path;
  }

  function authHeaders() {
    const headers = { "Content-Type": "application/json" };
    if (tg && tg.initData) {
      headers["X-Telegram-Init-Data"] = tg.initData;
    }
    return headers;
  }

  async function apiGet(path) {
    const res = await fetch(apiUrl(path), { headers: authHeaders() });
    if (!res.ok) throw await res.json().catch(() => ({ detail: res.statusText }));
    return res.json();
  }

  async function apiPost(path, body) {
    const res = await fetch(apiUrl(path), {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify(body || {}),
    });
    if (!res.ok) throw await res.json().catch(() => ({ detail: res.statusText }));
    return res.json();
  }

  const nameScreen = document.getElementById("nameScreen");
  const gameScreen = document.getElementById("gameScreen");
  const cityNameInput = document.getElementById("cityNameInput");
  const cityNameSubmit = document.getElementById("cityNameSubmit");
  const nameError = document.getElementById("nameError");
  const toast = document.getElementById("toast");

  let state = null;
  let lastSyncAt = 0;

  function showToast(msg) {
    toast.textContent = msg;
    toast.classList.remove("hidden");
    setTimeout(() => toast.classList.add("hidden"), 2200);
  }

  function fmt(n) {
    n = Math.floor(n);
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
    if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
    return String(n);
  }

  function render() {
    if (!state) return;
    document.getElementById("cityName").textContent = state.city_name || "—";
    document.getElementById("tierName").textContent = state.tier_name;
    document.getElementById("population").textContent = fmt(state.population);
    document.getElementById("capacity").textContent = fmt(state.housing_capacity);
    document.getElementById("money").textContent = fmt(state.money);
    document.getElementById("income").textContent = fmt(state.income_per_second);
    document.getElementById("happiness").textContent = Math.round(state.happiness);
    document.getElementById("happinessBar").style.width = state.happiness + "%";

    const progressBar = document.querySelector("#tierProgressBar .bar-fill");
    const progressText = document.getElementById("tierProgressText");
    if (state.next_tier) {
      const prevThreshold = 0; // approximation, good enough for a progress hint
      const pct = Math.min(100, (state.population / state.next_tier.population_threshold) * 100);
      progressBar.style.width = pct + "%";
      progressText.textContent =
        "До статуса «" + state.next_tier.name + "»: " + fmt(state.next_tier.population_threshold) + " жителей";
    } else {
      progressBar.style.width = "100%";
      progressText.textContent = "Максимальный статус города достигнут!";
    }

    const list = document.getElementById("buildingsList");
    list.innerHTML = "";
    for (const b of state.buildings) {
      const card = document.createElement("div");
      card.className = "building-card" + (b.unlocked ? "" : " locked");

      const info = document.createElement("div");
      info.className = "building-info";
      const nameRow = document.createElement("div");
      nameRow.className = "building-name";
      nameRow.innerHTML =
        b.name + ' <span class="building-owned">x' + b.owned + "</span>";
      info.appendChild(nameRow);

      const effect = document.createElement("div");
      effect.className = "building-effect";
      effect.textContent = describeEffect(b);
      info.appendChild(effect);

      if (!b.unlocked) {
        const lock = document.createElement("div");
        lock.className = "building-lock-note";
        lock.textContent = "Откроется на статусе выше";
        info.appendChild(lock);
      }

      card.appendChild(info);

      const btn = document.createElement("button");
      btn.className = "build-btn";
      const affordable = state.money >= b.next_cost;
      btn.disabled = !b.unlocked || !affordable;
      btn.textContent = fmt(b.next_cost) + " 💰";
      btn.addEventListener("click", () => build(b.id));
      card.appendChild(btn);

      list.appendChild(card);
    }
  }

  function describeEffect(b) {
    switch (b.category) {
      case "housing":
        return "+" + b.effect + " к вместимости города";
      case "income":
        return "+" + b.effect + " дохода/сек";
      case "happiness":
        return "+" + b.effect + " к счастью";
      case "infra":
        return "+" + b.effect + " к счастью (инфраструктура)";
      default:
        return "";
    }
  }

  async function build(buildingId) {
    try {
      state = await apiPost("/api/game/build", { building_id: buildingId });
      render();
    } catch (e) {
      showToast(translateError(e.detail));
    }
  }

  function translateError(detail) {
    const map = {
      insufficient_funds: "Недостаточно денег",
      locked: "Постройка ещё не открыта",
      unknown_building: "Неизвестная постройка",
    };
    return map[detail] || "Ошибка: " + detail;
  }

  async function syncState() {
    try {
      state = await apiGet("/api/game/state");
      lastSyncAt = Date.now();
      render();
      if (!state.city_name) {
        nameScreen.classList.remove("hidden");
        gameScreen.classList.add("hidden");
      } else {
        nameScreen.classList.add("hidden");
        gameScreen.classList.remove("hidden");
      }
    } catch (e) {
      showToast(translateError(e.detail));
    }
  }

  cityNameSubmit.addEventListener("click", async () => {
    const name = cityNameInput.value.trim();
    if (name.length < 2 || name.length > 24) {
      nameError.textContent = "Название от 2 до 24 символов";
      return;
    }
    nameError.textContent = "";
    cityNameSubmit.disabled = true;
    try {
      state = await apiPost("/api/game/name", { name });
      nameScreen.classList.add("hidden");
      gameScreen.classList.remove("hidden");
      render();
    } catch (e) {
      nameError.textContent = translateError(e.detail);
    } finally {
      cityNameSubmit.disabled = false;
    }
  });

  // Smooth client-side ticking between server syncs so numbers feel alive.
  // Purely visual extrapolation — the server remains the source of truth
  // and corrects any drift on the next sync.
  setInterval(() => {
    if (!state || !state.city_name) return;
    const elapsedSec = (Date.now() - lastSyncAt) / 1000;
    if (elapsedSec <= 0) return;
    const projectedMoney = state.money + state.income_per_second * elapsedSec;
    document.getElementById("money").textContent = fmt(projectedMoney);
  }, 1000);

  // Periodic resync with the server (also drives offline-progress catch-up).
  setInterval(syncState, 15000);

  syncState();
})();
