/**
 * KinoBot v4 — Admin Panel JavaScript
 * Clean modular architecture with real-time Socket.IO
 */
(function () {
  "use strict";

  const $ = (s, p = document) => p.querySelector(s);
  const $$ = (s, p = document) => Array.from(p.querySelectorAll(s));

  // ======================== Toast Notifications ========================
  function toast(message, type = "info") {
    const container = $("#notificationContainer");
    if (!container) return;
    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.innerHTML = `<i class="ti ti-${type === "success" ? "check" : type === "error" ? "alert-circle" : type === "warning" ? "alert-triangle" : "info-circle"}"></i>
      <span>${message}</span><button>&times;</button>`;
    container.appendChild(el);
    // Max 4 toasts
    while (container.children.length > 4) container.firstElementChild.remove();
    const remove = () => { el.classList.add("hide"); setTimeout(() => el.remove(), 300); };
    el.querySelector("button").addEventListener("click", remove);
    setTimeout(remove, 5000);
  }

  // ======================== Theme Toggle ========================
  function initTheme() {
    const btn = $("#themeToggle");
    if (!btn) return;
    const saved = localStorage.getItem("kb_theme") || "dark";
    const apply = (t) => {
      document.documentElement.dataset.theme = t;
      localStorage.setItem("kb_theme", t);
      btn.dataset.state = t;
      const icon = btn.querySelector("i");
      const label = btn.querySelector(".label");
      if (icon) icon.className = t === "dark" ? "ti ti-moon" : "ti ti-sun";
      if (label) label.textContent = t === "dark" ? "Тёмная" : "Светлая";
    };
    apply(saved);
    btn.addEventListener("click", () => apply(btn.dataset.state === "dark" ? "light" : "dark"));
  }

  // ======================== Navigation ========================
  function initNav() {
    const sections = $$(".section");
    const buttons = $$(".main-nav .nav-btn");
    const map = {
      addFilmBtn: "addFilmSection",
      filmListBtn: "filmListSection",
      statsBtn: "statsSection",
      autoImportBtn: "autoImportSection",
      userManagementBtn: "userManagementSection",
    };
    buttons.forEach((btn) => {
      btn.addEventListener("click", () => {
        buttons.forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        sections.forEach((s) => s.classList.remove("active"));
        const id = map[btn.id];
        if (id) {
          const sec = $("#" + id);
          if (sec) sec.classList.add("active");
          if (id === "statsSection") Stats.load();
        }
      });
    });
  }

  // ======================== Socket.IO ========================
  let socket;
  function initSocket() {
    if (!window.io) { console.warn("Socket.IO not found"); return; }
    socket = io("/", { path: "/socket.io" });
    socket.on("connect", () => console.log("Connected to server"));
    socket.on("update_films", renderFilms);
    socket.on("films", renderFilms);
    socket.on("update_users", renderUsers);
    socket.on("users", renderUsers);
    socket.on("notification", (p) => toast(p?.message || "Event", p?.type || "info"));
    socket.on("task_update", Tasks.upsert);
    socket.emit("get_films");
    socket.emit("get_users");
  }

  // ======================== Film List ========================
  function renderFilms(items) {
    const tbody = $("#filmList tbody");
    if (!tbody) return;
    tbody.innerHTML = (items || []).map((f) => `
      <tr>
        <td><code>${f.code || f.id}</code></td>
        <td>${f.name || ""}</td>
        <td>${f.genre || ""}</td>
        <td>${f.site ? `<a href="${f.site}" target="_blank" style="color:var(--brand)">ссылка</a>` : ""}</td>
        <td>${f.photo_id ? `<span class="badge">img</span>` : ""}</td>
        <td>
          <button class="row-edit" data-id="${f.id}"><i class="ti ti-edit"></i></button>
          <button class="row-delete" data-id="${f.id}"><i class="ti ti-trash"></i></button>
        </td>
      </tr>
    `).join("");
  }

  // ======================== User List ========================
  function renderUsers(items) {
    const tbody = $("#userList tbody");
    if (!tbody) return;
    tbody.innerHTML = (items || []).map((u) => `
      <tr>
        <td>${u.id}</td>
        <td>${u.name || ""}</td>
        <td><code>${u.tg_id || ""}</code></td>
        <td>${u.admin ? "<span style='color:var(--success)'>траффер</span>" : "пользователь"}${u.banned ? " / <span style='color:var(--error)'>бан</span>" : ""}</td>
        <td>
          <button class="row-toggle" data-id="${u.id}" title="Сменить роль"><i class="ti ti-shield-half"></i></button>
          <button class="row-ban" data-id="${u.id}" title="${u.banned ? "Разбанить" : "Забанить"}"><i class="ti ${u.banned ? "ti-user-check" : "ti-user-cancel"}"></i></button>
        </td>
      </tr>
    `).join("");
  }

  // ======================== Stats ========================
  const Stats = (() => {
    let charts = {};
    function destroy() { Object.values(charts).forEach((c) => { try { c.destroy(); } catch (e) {} }); charts = {}; }
    async function load() {
      try {
        const r = await fetch("/api/stats");
        if (!r.ok) return;
        const d = await r.json();
        // KPIs
        const el = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
        el("kpiFilms", d.films.total);
        el("kpiUsers", d.users.total);
        el("kpiAdmins", d.users.admins);
        el("kpiBanned", d.users.banned);
        // Recent
        const ul = $("#recentAdditions");
        if (ul) ul.innerHTML = (d.films.recent || []).map((x) => `<li><code>#${x.code}</code> — ${x.name}</li>`).join("");
        // Charts
        destroy();
        const brand = getComputedStyle(document.documentElement).getPropertyValue("--brand").trim() || "#6366f1";
        const colors = ["#6366f1", "#10b981", "#f59e0b", "#ef4444", "#3b82f6", "#8b5cf6", "#ec4899"];
        const gctx = document.getElementById("filmGenreChart")?.getContext("2d");
        if (gctx) {
          charts.genre = new Chart(gctx, { type: "doughnut", data: { labels: d.films.by_genre.map((x) => x.genre), datasets: [{ data: d.films.by_genre.map((x) => x.count), backgroundColor: d.films.by_genre.map((_, i) => colors[i % colors.length]) }] }, options: { plugins: { legend: { position: "bottom", labels: { color: "var(--text)" } } }, cutout: "60%" } });
        }
        const uctx = document.getElementById("usersBreakdownChart")?.getContext("2d");
        if (uctx) {
          const rest = Math.max(0, d.users.total - d.users.admins - d.users.banned);
          charts.users = new Chart(uctx, { type: "doughnut", data: { labels: ["Трафферы", "Забанены", "Остальные"], datasets: [{ data: [d.users.admins, d.users.banned, rest], backgroundColor: ["#10b981", "#ef4444", "#6366f1"] }] }, options: { plugins: { legend: { position: "bottom" } }, cutout: "60%" } });
        }
        const rctx = document.getElementById("referralsChart")?.getContext("2d");
        if (rctx) {
          charts.refs = new Chart(rctx, { type: "line", data: { labels: d.referrals.labels, datasets: [{ data: d.referrals.counts, borderColor: brand, backgroundColor: brand + "33", fill: true, tension: 0.4, pointRadius: 3 }] }, options: { plugins: { legend: { display: false } }, scales: { x: { ticks: { maxTicksLimit: 7 } }, y: { beginAtZero: true } } } });
        }
      } catch (e) { console.warn("Stats error", e); }
    }
    return { load };
  })();

  // ======================== Task Queue ========================
  const Tasks = (() => {
    function upsert(job) {
      if (!job?.id) return;
      const list = $("#taskList");
      if (!list) return;
      let el = document.getElementById(`task-${job.id}`);
      if (!el) {
        el = document.createElement("div");
        el.id = `task-${job.id}`;
        el.className = "task-item";
        el.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;"><span class="type" style="font-weight:600;font-size:13px;"></span><span class="status" style="font-size:12px;color:var(--text-muted);"></span></div><div class="progress"><div class="bar"></div></div><div class="meta" style="margin-top:4px;font-size:12px;color:var(--text-muted);"></div>`;
        list.prepend(el);
      }
      el.querySelector(".type").textContent = job.type === "tmdb_popular" ? "Импорт популярных" : "Импорт фильма";
      el.querySelector(".status").textContent = { pending: "Ожидание", running: "Выполняется", done: "Готово", error: "Ошибка" }[job.status] || job.status;
      const bar = el.querySelector(".bar");
      bar.style.width = Math.min(100, job.progress || 0) + "%";
      bar.style.background = job.status === "done" ? "var(--success)" : job.status === "error" ? "var(--error)" : "var(--brand)";
      const meta = job.meta || {};
      el.querySelector(".meta").textContent = meta.name ? `${meta.name} (${meta.code || ""})` : meta.imported != null ? `Добавлено: ${meta.imported}/${meta.requested || 0}` : meta.duplicate ? "Дубликат" : "";
    }
    return { upsert };
  })();

  // ======================== Forms ========================
  function initForms() {
    // Add film form
    const addForm = $("#addFilmForm");
    addForm?.addEventListener("submit", async (e) => {
      e.preventDefault();
      const fd = new FormData(addForm);
      const gSel = $("#filmGenre");
      if (gSel) fd.set("genre", Array.from(gSel.selectedOptions).map((o) => o.value).join(", "));
      try {
        const r = await fetch("/api/film", { method: "POST", body: fd });
        const j = await r.json();
        if (r.ok) { toast(j.message || "Фильм добавлен", "success"); addForm.reset(); }
        else toast(j.error || j.detail || "Ошибка", "error");
      } catch (e) { toast("Ошибка сети", "error"); }
    });

    // Edit film form
    const editForm = $("#editFilmForm");
    editForm?.addEventListener("submit", async (e) => {
      e.preventDefault();
      const id = $("#editFilmId")?.value;
      const fd = new FormData(editForm);
      const gSel = $("#editFilmGenre");
      if (gSel) fd.set("genre", Array.from(gSel.selectedOptions).map((o) => o.value).join(", "));
      try {
        const r = await fetch(`/api/film/${id}`, { method: "PUT", body: fd });
        const j = await r.json();
        if (r.ok) { toast("Фильм обновлён", "success"); closeModal(); }
        else toast(j.error || j.detail || "Ошибка", "error");
      } catch (e) { toast("Ошибка сети", "error"); }
    });

    // Image previews
    ["filmImage", "editFilmImage"].forEach((id) => {
      const input = document.getElementById(id);
      const previewId = id === "filmImage" ? "imagePreview" : "editImagePreview";
      input?.addEventListener("change", () => {
        const file = input.files?.[0];
        const preview = document.getElementById(previewId);
        if (!preview) return;
        if (file) {
          const r = new FileReader();
          r.onload = () => { preview.innerHTML = `<img src="${r.result}" alt="preview">`; };
          r.readAsDataURL(file);
        } else { preview.innerHTML = ""; }
      });
    });
  }

  // ======================== Film List Actions ========================
  function initActions() {
    document.addEventListener("click", async (e) => {
      const btn = e.target.closest("button");
      if (!btn) return;

      if (btn.classList.contains("row-edit")) {
        const id = btn.dataset.id;
        try {
          const r = await fetch(`/api/film/${id}`);
          const f = await r.json();
          if (!r.ok) { toast("Фильм не найден", "error"); return; }
          $("#editFilmId").value = f.id;
          $("#editFilmName").value = f.name || "";
          const gSel = $("#editFilmGenre");
          if (gSel) {
            const arr = (f.genre || "").split(",").map((s) => s.trim()).filter(Boolean);
            Array.from(gSel.options).forEach((o) => { o.selected = arr.includes(o.value); });
          }
          $("#editFilmDescription").value = f.description || "";
          $("#editFilmSite").value = f.site || "";
          const preview = $("#editImagePreview");
          if (preview) preview.innerHTML = f.photo_id ? `<img src="/static/uploads/${f.photo_id}" alt="">` : "";
          openModal();
        } catch (e) { toast("Ошибка", "error"); }
      }
      if (btn.classList.contains("row-delete")) {
        if (confirm("Удалить этот фильм?")) socket?.emit("delete_film", Number(btn.dataset.id));
      }
      if (btn.classList.contains("row-toggle")) {
        await fetch(`/api/user/${btn.dataset.id}/toggle-admin`, { method: "POST" });
      }
      if (btn.classList.contains("row-ban")) {
        await fetch(`/api/user/${btn.dataset.id}/toggle-ban`, { method: "POST" });
      }
    });
  }

  function openModal() { const m = $("#editFilmModal"); if (m) { m.classList.add("is-open"); document.body.style.overflow = "hidden"; } }
  function closeModal() { const m = $("#editFilmModal"); if (m) { m.classList.remove("is-open"); document.body.style.overflow = ""; } }

  // ======================== Search & Filter ========================
  function initFilters() {
    const q = $("#searchFilm");
    const g = $("#filterGenre");
    let timer;
    async function search() {
      const params = new URLSearchParams();
      if (q?.value) params.set("query", q.value);
      if (g?.value && g.value !== "all") params.set("genre", g.value);
      try {
        const r = await fetch("/api/films/search?" + params.toString());
        if (r.ok) renderFilms(await r.json());
      } catch (e) {}
    }
    q?.addEventListener("input", () => { clearTimeout(timer); timer = setTimeout(search, 300); });
    g?.addEventListener("change", search);
  }

  // ======================== TMDb Import ========================
  function initTmdb() {
    const q = $("#tmdbQuery");
    const searchBtn = $("#tmdbSearchBtn");
    const tbody = $("#tmdbResults tbody");

    async function search() {
      const query = (q?.value || "").trim();
      if (!query || !tbody) return;
      searchBtn.disabled = true;
      try {
        const r = await fetch("/api/import/search?" + new URLSearchParams({ query }));
        const j = await r.json();
        tbody.innerHTML = (j.results || []).map((it) => `
          <tr>
            <td>${it.poster ? `<img src="${it.poster}" style="width:40px;border-radius:4px;">` : ""}</td>
            <td><strong>${it.title || ""}</strong><br><small style="color:var(--text-muted)">${it.original_title || ""}</small></td>
            <td>${it.year || ""}</td>
            <td style="font-size:12px;">${(it.overview || "").slice(0, 120)}${(it.overview || "").length > 120 ? "..." : ""}</td>
            <td><button class="row-import" data-id="${it.id}"><i class="ti ti-download"></i></button></td>
          </tr>
        `).join("") || "<tr><td colspan='5' style='text-align:center;color:var(--text-muted)'>Ничего не найдено</td></tr>";
      } catch (e) { toast("Ошибка поиска", "error"); }
      searchBtn.disabled = false;
    }
    q?.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); search(); } });
    searchBtn?.addEventListener("click", search);

    // Import single
    tbody?.addEventListener("click", async (e) => {
      const btn = e.target.closest(".row-import");
      if (!btn) return;
      btn.disabled = true;
      try {
        const r = await fetch(`/api/tasks/import/tmdb/${btn.dataset.id}`, { method: "POST" });
        const j = await r.json();
        if (r.ok && j.job_id) { Tasks.upsert({ id: j.job_id, type: "tmdb_single", status: "pending", progress: 0, meta: {} }); toast("Задача в очереди", "info"); }
        else toast(j.detail || "Ошибка", "error");
      } catch (e) { toast("Ошибка сети", "error"); }
      btn.disabled = false;
    });

    // Import popular
    const popularBtn = $("#tmdbPopularBtn");
    const popularCount = $("#tmdbPopularCount");
    popularBtn?.addEventListener("click", async () => {
      const n = parseInt(popularCount?.value || "0");
      if (n < 2 || n > 50) { toast("Введите от 2 до 50", "warning"); return; }
      popularBtn.disabled = true;
      try {
        const r = await fetch(`/api/tasks/import/tmdb/popular?count=${n}`, { method: "POST" });
        const j = await r.json();
        if (r.ok && j.job_id) { Tasks.upsert({ id: j.job_id, type: "tmdb_popular", status: "pending", progress: 0, meta: {} }); toast("Задача в очереди", "info"); }
        else toast(j.detail || "Ошибка", "error");
      } catch (e) { toast("Ошибка сети", "error"); }
      popularBtn.disabled = false;
    });
  }

  // ======================== Update Banner ========================
  async function checkUpdate() {
    try {
      const r = await fetch("/api/update/status");
      if (!r.ok) return;
      const d = await r.json();
      const banner = $("#updateBanner");
      if (!banner) return;
      if (d.available) {
        banner.style.display = "block";
        $("#updateText").textContent = `Доступно обновление ${d.latest}`;
        $("#updateSub").textContent = `Текущая версия: ${d.current}`;
      }
    } catch (e) {}
    // Apply update
    $("#updateNowBtn")?.addEventListener("click", async () => {
      if (!confirm("Обновить бота? Сервер перезагрузится.")) return;
      try {
        const r = await fetch("/api/update/apply", { method: "POST" });
        const j = await r.json();
        toast(j.message || "Обновление...", "info");
      } catch (e) { toast("Ошибка обновления", "error"); }
    });
  }

  // ======================== Modal ========================
  function initModal() {
    const modal = $("#editFilmModal");
    modal?.querySelector(".close")?.addEventListener("click", closeModal);
    modal?.addEventListener("click", (e) => { if (e.target === modal) closeModal(); });
  }

  // ======================== Multi-Select Enhancement ========================
  function enhanceMultiSelect(select) {
    if (!select || select._enhanced) return;
    select._enhanced = true;
    const wrap = document.createElement("div");
    wrap.className = "genre-select";
    const trigger = document.createElement("button");
    trigger.type = "button";
    trigger.className = "genre-trigger";
    trigger.innerHTML = `<span class="genre-trigger-label">Выберите жанры</span><i class="ti ti-chevron-down"></i>`;
    const dropdown = document.createElement("div");
    dropdown.className = "genre-dropdown";
    const list = document.createElement("div");
    list.className = "genre-options";

    function updateLabel() {
      const vals = Array.from(select.options).filter((o) => o.selected && o.value).map((o) => o.text);
      trigger.querySelector(".genre-trigger-label").textContent = vals.length ? (vals.length <= 3 ? vals.join(", ") : `${vals.length} выбрано`) : "Выберите жанры";
    }

    Array.from(select.options).forEach((opt) => {
      if (!opt.value) return;
      const item = document.createElement("div");
      item.className = "genre-option" + (opt.selected ? " selected" : "");
      item.innerHTML = `<span class="label">${opt.text}</span><i class="ti ti-check check"></i>`;
      item.addEventListener("click", (e) => { e.stopPropagation(); opt.selected = !opt.selected; item.classList.toggle("selected", opt.selected); updateLabel(); });
      list.appendChild(item);
    });

    dropdown.appendChild(list);
    wrap.appendChild(trigger);
    wrap.appendChild(dropdown);
    select.classList.add("visually-hidden-select");
    select.parentNode.insertBefore(wrap, select.nextSibling);

    trigger.addEventListener("click", (e) => { e.stopPropagation(); wrap.classList.toggle("open"); });
    document.addEventListener("click", (e) => { if (!wrap.contains(e.target)) wrap.classList.remove("open"); });
    updateLabel();
  }

  // ======================== Init ========================
  document.addEventListener("DOMContentLoaded", () => {
    initTheme();
    initNav();
    initSocket();
    initForms();
    initActions();
    initFilters();
    initTmdb();
    initModal();
    checkUpdate();
    // Enhance genre selects
    enhanceMultiSelect($("#filmGenre"));
    enhanceMultiSelect($("#editFilmGenre"));
  });
})();
