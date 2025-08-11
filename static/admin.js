(function(){
  const $ = (s, p=document)=>p.querySelector(s);
  const $$ = (s, p=document)=>Array.from(p.querySelectorAll(s));

  function toast(message, type='info'){
    const c = document.getElementById('notificationContainer');
    if(!c) return;
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.innerHTML = `<i class="ti ti-bell"></i><div>${message}</div><button aria-label="Закрыть">✕</button>`;
    c.appendChild(t);
    // keep only last 4 toasts: fade out and remove the oldest
    const MAX = 4;
    while (c.children.length > MAX) {
      const oldest = c.firstElementChild;
      if (!oldest) break;
      if (!oldest.classList.contains('hide')) {
        oldest.classList.add('hide');
        setTimeout(() => { if (oldest && oldest.parentNode) oldest.remove(); }, 350);
      } else {
        oldest.remove();
      }
    }
    const removeNow = ()=>{ t.remove(); };
    const startHide = ()=>{ t.classList.add('hide'); setTimeout(removeNow, 350); };
    t.querySelector('button')?.addEventListener('click', startHide);
    setTimeout(startHide, 5000);
  }

  // ==================== Background Tasks UI (TMDb) ====================
  const Tasks = (function(){
    const listEl = ()=> document.getElementById('taskList');
    const items = new Map(); // id -> job

    function statusText(s){
      switch(String(s||'').toLowerCase()){
        case 'pending': return 'Ожидание';
        case 'running': return 'Выполняется';
        case 'done': return 'Готово';
        case 'error': return 'Ошибка';
        default: return String(s||'');
      }
    }

    function typeText(t){
      return t === 'tmdb_popular' ? 'Импорт популярных TMDb' : 'Импорт фильма TMDb';
    }

    function ensureEl(job){
      const list = listEl();
      if(!list) return null;
      const id = `task-${job.id}`;
      let el = document.getElementById(id);
      if(!el){
        el = document.createElement('div');
        el.id = id;
        el.className = 'task-item';
        el.style.cssText = 'padding:10px;border:1px solid var(--border);border-radius:10px;background:var(--panel);';
        el.innerHTML = `
          <div class="task-head" style="display:flex;align-items:center;gap:10px;justify-content:space-between;margin-bottom:6px;">
            <div class="task-title" style="display:flex;align-items:center;gap:10px;">
              <i class="ti ti-cloud-download"></i>
              <div>
                <div class="type" style="font-weight:600"></div>
                <div class="small" style="color:var(--muted);font-size:12px;">ID: <span class="jid"></span></div>
              </div>
            </div>
            <div class="stat" style="font-size:12px;color:var(--muted);"><span class="status"></span></div>
          </div>
          <div class="progress" style="height:8px;background:var(--border);border-radius:6px;overflow:hidden;">
            <div class="bar" style="height:100%;width:0%;background:var(--brand);transition:width .25s ease"></div>
          </div>
          <div class="meta" style="margin-top:6px;font-size:12px;color:var(--muted);"></div>
        `;
        list.prepend(el);
      }
      return el;
    }

    function metaText(job){
      const m = job.meta || {};
      if(job.type === 'tmdb_popular'){
        const req = m.requested ?? '';
        const imp = m.imported ?? 0;
        const sk = m.skipped ?? 0;
        const fl = m.failed ?? 0;
        return `Добавлено: ${imp}/${req}${sk?`, пропущено: ${sk}`:''}${fl?`, ошибок: ${fl}`:''}`;
      }
      if(job.type === 'tmdb_single'){
        if(m.duplicate) return 'Дубликат: уже существует';
        const code = m.code ? `, код: ${m.code}` : '';
        return m.name ? `Фильм: ${m.name}${code}` : '';
      }
      return '';
    }

    function paint(el, job){
      const typeEl = el.querySelector('.type');
      const jidEl = el.querySelector('.jid');
      const stEl = el.querySelector('.status');
      const bar = el.querySelector('.bar');
      const meta = el.querySelector('.meta');
      if(typeEl) typeEl.textContent = typeText(job.type);
      if(jidEl) jidEl.textContent = job.id;
      if(stEl) stEl.textContent = statusText(job.status);
      const p = Math.max(0, Math.min(100, Number(job.progress||0)));
      if(bar){
        bar.style.width = p + '%';
        if(job.status === 'done'){ bar.style.background = 'var(--ok)'; }
        else if(job.status === 'error'){ bar.style.background = 'var(--err)'; }
        else { bar.style.background = 'var(--brand)'; }
      }
      if(meta) meta.textContent = metaText(job);
    }

    function upsert(job){
      if(!job || !job.id) return;
      items.set(job.id, job);
      const el = ensureEl(job);
      if(el) paint(el, job);
    }

    async function fetchAll(){
      try{
        const r = await fetch('/api/tasks');
        if(!r.ok) return;
        const list = await r.json();
        (list||[]).forEach(upsert);
      }catch(_){/* noop */}
    }

    function seed(job_id, type){
      upsert({ id: job_id, type, status: 'pending', progress: 0, meta: {} });
    }

    function bindSocket(socket){
      if(!socket) return;
      socket.on('task_update', upsert);
      fetchAll();
    }

    return { bindSocket, upsert, fetchAll, seed };
  })();

  // ==================== TMDb Auto Import ====================
  function tmdb(){
    const q = document.getElementById('tmdbQuery');
    const btn = document.getElementById('tmdbSearchBtn');
    const table = document.getElementById('tmdbResults');
    const tbody = table?.querySelector('tbody');
    async function run(){
      const query = (q?.value||'').trim();
      if(!query){ tbody.innerHTML = ''; return; }
      try{
        btn && (btn.disabled = true);
        const r = await fetch('/api/import/search?'+new URLSearchParams({ query }));
        const j = await r.json();
        const rows = (j.results||[]).map(it=>{
          const poster = it.poster ? `<img src="${it.poster}" alt="${it.title}" style="width:48px;height:auto;border-radius:6px;"/>` : '';
          const descr = (it.overview||'').slice(0, 160) + ((it.overview||'').length>160?'…':'');
          return `
            <tr>
              <td style="text-align:center;">${poster}</td>
              <td><div style="font-weight:600;">${it.title||''}</div><div style="color:var(--muted);font-size:12px;">${it.original_title||''}</div></td>
              <td style="text-align:center;white-space:nowrap;">${it.year||''}</td>
              <td>${descr}</td>
              <td style="text-align:center;"><button class="row-import" data-id="${it.id}"><i class="ti ti-download"></i></button></td>
            </tr>`; 
        }).join('');
        tbody.innerHTML = rows || '<tr><td colspan="5" style="text-align:center;color:var(--muted);">Ничего не найдено</td></tr>';
      }catch(err){ toast('Ошибка TMDb','error'); }
      finally{ btn && (btn.disabled = false); }
    }
    q?.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ e.preventDefault(); run(); } });
    btn?.addEventListener('click', (e)=>{ e.preventDefault(); run(); });
    tbody?.addEventListener('click', async (e)=>{
      const t = e.target.closest('.row-import');
      if(!t) return;
      const id = t.dataset.id;
      try{
        t.disabled = true;
        const r = await fetch(`/api/tasks/import/tmdb/${id}`, { method: 'POST' });
        const j = await r.json();
        if(r.ok && j && j.job_id){
          Tasks.seed(j.job_id, 'tmdb_single');
          toast('Задача поставлена в очередь','info');
        } else {
          toast(j.error || j.detail || 'Ошибка постановки задачи','error');
        }
      }catch(err){ toast('Ошибка сети','error'); }
      finally{ t.disabled = false; }
    });
    const popularBtn = document.getElementById('tmdbPopularBtn');
    const popularCount = document.getElementById('tmdbPopularCount');
    popularBtn?.addEventListener('click', async (e)=>{
      e.preventDefault();
      const n = parseInt(popularCount?.value || '0', 10);
      if(isNaN(n) || n < 2 || n > 50){ toast('Введите число от 2 до 50','warning'); return; }
      try{
        popularBtn.disabled = true;
        const r = await fetch(`/api/tasks/import/tmdb/popular?count=${n}`, { method: 'POST' });
        const j = await r.json();
        if(r.ok && j && j.job_id){
          Tasks.seed(j.job_id, 'tmdb_popular');
          toast('Задача поставлена в очередь','info');
        } else {
          toast(j.error || j.detail || 'Ошибка постановки задачи','error');
        }
      }catch(err){ toast('Ошибка сети','error'); }
      finally{ popularBtn.disabled = false; }
    });
  }

  function bindThemeToggle(){
    const toggle = document.getElementById('themeToggle');
    if(!toggle) return;
    const k = 'admin_theme';
    const themeMeta = document.querySelector('meta[name="theme-color"]');
    const iconEl = toggle.querySelector('i');
    const labelEl = toggle.querySelector('.label');
    const setMeta = (m)=>{ if(themeMeta) themeMeta.setAttribute('content', m==='dark' ? '#0f1115' : '#f6f7fb'); };
    const setIcon = (m)=>{ if(!iconEl) return; iconEl.className = m==='dark' ? 'ti ti-moon' : 'ti ti-sun'; };
    const apply = (m)=>{
      document.documentElement.dataset.theme = m;
      localStorage.setItem(k, m);
      toggle.setAttribute('data-state', m);
      if(labelEl) labelEl.textContent = m === 'dark' ? 'Тёмная' : 'Светлая';
      setIcon(m);
      setMeta(m);
      document.dispatchEvent(new CustomEvent('themechange', { detail: m }));
    };
    const saved = localStorage.getItem(k) || 'light';
    apply(saved);
    toggle.addEventListener('click', ()=>{
      const next = toggle.getAttribute('data-state')==='dark'?'light':'dark';
      apply(next);
    });
  }

  // ==================== Stats ====================
  const Stats = (function(){
    let charts = {};
    let lastData = null;
    let warned = false;

    function palette(){
      const cs = getComputedStyle(document.documentElement);
      const brand = cs.getPropertyValue('--brand').trim() || '#6e9bff';
      const ok = cs.getPropertyValue('--ok').trim() || '#2ecc71';
      const warn = cs.getPropertyValue('--warn').trim() || '#f39c12';
      const err = cs.getPropertyValue('--err').trim() || '#e74c3c';
      const text = cs.getPropertyValue('--text').trim() || '#e6e7eb';
      const border = cs.getPropertyValue('--border').trim() || '#263043';
      return { brand, ok, warn, err, text, border };
    }

    function kpi(id, val){ const el = document.getElementById(id); if(el) el.textContent = String(val); }

    function destroyCharts(){ Object.values(charts).forEach(c=>{ try{ c.destroy(); }catch(_){} }); charts = {}; }

    function doughnut(ctx, labels, data, colors){
      return new Chart(ctx, {
        type: 'doughnut',
        data: { labels, datasets: [{ data, backgroundColor: colors, borderColor: colors.map(()=> palette().border), borderWidth: 1 }] },
        options: {
          plugins: { legend: { position: 'bottom', labels: { color: palette().text } } },
          responsive: true,
          cutout: '60%'
        }
      });
    }

    function line(ctx, labels, data, color){
      return new Chart(ctx, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            data,
            borderColor: color,
            backgroundColor: color + '33',
            fill: true,
            tension: .35,
            pointRadius: 3,
          }]
        },
        options: {
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: palette().text }, grid: { color: palette().border } },
            y: { ticks: { color: palette().text }, grid: { color: palette().border } }
          }
        }
      });
    }

    function render(data){
      lastData = data;
      const pal = palette();
      // KPI
      kpi('kpiFilms', data.films.total);
      kpi('kpiUsers', data.users.total);
      kpi('kpiAdmins', data.users.admins);
      kpi('kpiBanned', data.users.banned);

      // Recent
      const ul = document.getElementById('recentAdditions');
      if(ul){
        ul.innerHTML = (data.films.recent||[]).map(x=>`<li>#${x.code||x.id} — ${x.name}</li>`).join('');
      }

      // Charts
      destroyCharts();
      const gctx = document.getElementById('filmGenreChart')?.getContext('2d');
      const uctx = document.getElementById('usersBreakdownChart')?.getContext('2d');
      const rctx = document.getElementById('referralsChart')?.getContext('2d');
      if(gctx){
        const labels = data.films.by_genre.map(x=>x.genre);
        const vals = data.films.by_genre.map(x=>x.count);
        const colors = labels.map((_,i)=> [pal.brand, pal.ok, pal.warn, pal.err][i%4]);
        charts.genre = doughnut(gctx, labels, vals, colors);
      }
      if(uctx){
        const labels = ['Трафферы', 'Забанены', 'Остальные'];
        const regs = Math.max(0, data.users.total - data.users.admins - data.users.banned);
        const vals = [data.users.admins, data.users.banned, regs];
        const colors = [pal.ok, pal.err, pal.brand];
        charts.users = doughnut(uctx, labels, vals, colors);
      }
      if(rctx){
        charts.referrals = line(rctx, data.referrals.labels, data.referrals.counts, pal.brand);
      }
    }

    async function load(){
      try{
        const r = await fetch('/api/stats');
        if(!r.ok){
          if(!warned){
            warned = true;
            toast(r.status === 404 ? 'Статистика недоступна. Перезапустите сервер, чтобы применить новые маршруты.' : 'Ошибка загрузки статистики', 'warning');
          }
          return;
        }
        const j = await r.json();
        warned = false;
        render(j);
      }catch(e){
        console.warn('stats load error', e);
        if(!warned){ warned = true; toast('Ошибка сети при загрузке статистики','warning'); }
      }
    }

    function onTheme(){ if(lastData) render(lastData); }

    document.addEventListener('themechange', onTheme);
    return { load, render };
  })();

  function bindNav(){
    const sections = $$('.section');
    const buttons = $$('.main-nav .nav-btn');
    const map = {
      addFilmBtn: 'addFilmSection',
      filmListBtn: 'filmListSection',
      statsBtn: 'statsSection',
      autoImportBtn: 'autoImportSection',
      userManagementBtn: 'userManagementSection'
    };
    buttons.forEach(btn=>{
      btn.addEventListener('click', ()=>{
        buttons.forEach(b=>b.classList.remove('active'));
        btn.classList.add('active');
        sections.forEach(s=>s.classList.remove('active'));
        const id = map[btn.id];
        if(id){
          const sec = $('#'+id);
          sec?.classList.add('active');
          if(id === 'statsSection') Stats.load();
        }
      });
    });
  }

  function socketInit(){
    if(!window.io){ console.warn('Socket.IO не найден'); return; }
    const socket = io('/', { path: '/socket.io' });
    const filmTbody = $('#filmList tbody');
    const userTbody = $('#userList tbody');
    const editModal = $('#editFilmModal');
    const editFormEl = $('#editFilmForm');
    const editCloseBtn = editModal?.querySelector('.close');
    // закрытие модалки по крестику и по клику на фон
    editCloseBtn?.addEventListener('click', ()=>{ if(editModal){ editModal.classList.remove('is-open'); document.body.style.overflow=''; } });
    editModal?.addEventListener('click', (e)=>{ if(e.target === editModal){ editModal.classList.remove('is-open'); document.body.style.overflow=''; } });

    function renderFilms(items){
      if(!filmTbody) return;
      filmTbody.innerHTML = (items||[]).map(f=>{
        const idCell = (f.code || (f.id!=null? f.id.toString().padStart(5,'0') : ''));
        const siteCell = f.site ? `<a href="${f.site}" target="_blank">ссылка</a>` : '';
        const imgCell = f.photo_id ? `<span class="badge">img</span>` : '';
        return `
          <tr>
            <td>${idCell}</td>
            <td>${f.name||''}</td>
            <td>${f.genre||''}</td>
            <td>${siteCell}</td>
            <td>${imgCell}</td>
            <td>
              <button class="row-edit" data-id="${f.id}"><i class="ti ti-edit"></i></button>
              <button class="row-delete" data-id="${f.id}"><i class="ti ti-trash"></i></button>
            </td>
          </tr>`;
      }).join('');
    }
    function renderUsers(items){
      if(!userTbody) return;
      userTbody.innerHTML = (items||[]).map(u=>`
        <tr>
          <td>${u.id}</td>
          <td>${u.name||''}</td>
          <td>${u.tg_id||''}</td>
          <td>${u.admin? 'траффер':'пользователь'}${u.banned? ' / бан':''}</td>
          <td>
            <button class="row-toggle" data-id="${u.id}"><i class="ti ti-shield-half"></i></button>
            <button class="row-ban" data-id="${u.id}" data-banned="${u.banned?1:0}" title="${u.banned?'Разбанить':'Забанить'}">
              <i class="ti ${u.banned?'ti-user-check':'ti-user-cancel'}"></i>
            </button>
          </td>
        </tr>
      `).join('');
    }

    socket.on('connect', ()=>console.log('Socket.IO connected'));
    socket.on('connect_error', (err)=>console.warn('Socket.IO connect_error', err?.message||err));
    socket.on('update_films', (items)=>{ renderFilms(items); Stats.load(); });
    socket.on('films', renderFilms);
    socket.on('update_users', (items)=>{ renderUsers(items); Stats.load(); });
    socket.on('users', renderUsers);
    socket.on('notification', (p)=> toast(p?.message || 'Событие', p?.type || 'info'));
    // bind background tasks channel
    Tasks.bindSocket(socket);

    socket.emit('get_films');
    socket.emit('get_users');

    document.addEventListener('click', async (e)=>{
      const t = e.target.closest('button');
      if(!t) return;
      if(t.classList.contains('row-edit')){
        const id = t.dataset.id;
        try{
          const r = await fetch(`/api/film/${id}`);
          const f = await r.json();
          if(!r.ok){ toast(f.error || f.detail || 'Фильм не найден','error'); return; }
          const idEl = document.getElementById('editFilmId');
          const nameEl = document.getElementById('editFilmName');
          const genreEl = document.getElementById('editFilmGenre');
          const descEl = document.getElementById('editFilmDescription');
          const siteEl = document.getElementById('editFilmSite');
          if(idEl) idEl.value = f.id;
          if(nameEl) nameEl.value = f.name || '';
          if(genreEl){
            const arr = (f.genre||'').split(',').map(s=>s.trim()).filter(Boolean);
            // сброс выбора
            Array.from(genreEl.options).forEach(o=>{ o.selected = false; });
            // отметить совпадения, добавляя отсутствующие опции и UI-элементы
            arr.forEach(g=>{
              let opt = Array.from(genreEl.options).find(o=>o.value===g);
              if(!opt){
                // добавить отсутствующий жанр в select
                opt = new Option(g, g);
                genreEl.add(opt);
                // добавить пункт в кастомный dropdown, если он инициализирован
                if(genreEl._ms && genreEl._ms.list){
                  const item = document.createElement('div');
                  item.className = 'genre-option';
                  item.setAttribute('data-value', g);
                  const label = document.createElement('span');
                  label.className = 'label';
                  label.textContent = g;
                  const check = document.createElement('i');
                  check.className = 'ti ti-check check';
                  item.appendChild(label);
                  item.appendChild(check);
                  item.addEventListener('click', (e)=>{
                    e.stopPropagation();
                    opt.selected = !opt.selected;
                    item.classList.toggle('selected', opt.selected);
                    // уведомить слушателей
                    genreEl.dispatchEvent(new Event('change', { bubbles: true }));
                  });
                  genreEl._ms.list.appendChild(item);
                }
              }
              opt.selected = true;
            });
            // синхронизировать кастомный UI
            if(typeof genreEl._msSync === 'function') genreEl._msSync();
          }
          if(descEl) descEl.value = f.description || '';
          if(siteEl) siteEl.value = f.site || '';
          const p = document.getElementById('editImagePreview');
          if(p){
            p.innerHTML = '';
            if(f.photo_id){
              p.innerHTML = `<img src="/static/uploads/${f.photo_id}" alt="${f.name||''}" style="max-width:100%;border-radius:10px;"/>`;
            }
          }
          if(editModal){ editModal.classList.add('is-open'); document.body.style.overflow='hidden'; }
        }catch(err){ toast('Ошибка загрузки фильма','error'); }
        return;
      }
      if(t.classList.contains('row-delete')){
        const id = t.dataset.id;
        try{ socket.emit('delete_film', Number(id)); }catch(err){ toast('Ошибка удаления','error'); }
      }
      if(t.classList.contains('row-toggle')){
        const id = t.dataset.id;
        try{
          const r = await fetch(`/api/user/${id}/toggle-admin`, { method: 'POST' });
          // Локальный тост убран — полагаемся на серверный socket 'notification' с текстом "Пользователь ... теперь ..."
        }catch(err){ toast('Ошибка','error'); }
      }
      if(t.classList.contains('row-ban')){
        const id = t.dataset.id;
        try{
          t.disabled = true;
          await fetch(`/api/user/${id}/toggle-ban`, { method: 'POST' });
          // тост приходит с сервера через socket 'notification'
        }catch(err){ toast('Ошибка','error'); }
        finally{ t.disabled = false; }
      }
    });
  }

  function forms(){
    const addForm = document.getElementById('addFilmForm');
    const imgInput = document.getElementById('filmImage');
    const imgPrev = document.getElementById('imagePreview');
    const editForm = document.getElementById('editFilmForm');
    const editInput = document.getElementById('editFilmImage');
    const editPrev = document.getElementById('editImagePreview');

    const preview = (input, box)=>{
      if(!input || !box) return;
      input.addEventListener('change', ()=>{
        const f = input.files?.[0];
        if(!f){ box.textContent = 'Нет файла'; return; }
        const r = new FileReader();
        r.onload = ()=>{ box.innerHTML = `<img src="${r.result}" alt="preview" style="max-width:100%;border-radius:10px;"/>`; };
        r.readAsDataURL(f);
      });
    };
    preview(imgInput, imgPrev);
    preview(editInput, editPrev);

    addForm?.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const fd = new FormData(addForm);
      // собрать множественные жанры в строку
      const gSel = document.getElementById('filmGenre');
      if(gSel){
        const values = Array.from(gSel.selectedOptions).map(o=>o.value).filter(Boolean);
        fd.set('genre', values.join(', '));
      }
      try{
        const r = await fetch('/api/film', { method: 'POST', body: fd });
        const j = await r.json();
        if(r.ok){ toast(j.message || 'Фильм добавлен','success'); addForm.reset(); }
        else{ toast(j.error || 'Ошибка добавления','error'); }
      }catch(err){ toast('Ошибка сети','error'); }
    });

    editForm?.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const id = document.getElementById('editFilmId')?.value;
      const fd = new FormData(editForm);
      // собрать множественные жанры в строку
      const gSelE = document.getElementById('editFilmGenre');
      if(gSelE){
        const values = Array.from(gSelE.selectedOptions).map(o=>o.value).filter(Boolean);
        fd.set('genre', values.join(', '));
      }
      try{
        const r = await fetch(`/api/film/${id}`, { method: 'PUT', body: fd });
        const j = await r.json();
        if(r.ok){
          toast(j.message || 'Фильм обновлён','success');
          const modal = document.getElementById('editFilmModal');
          if(modal){ modal.classList.remove('is-open'); }
          document.body.style.overflow='';
          editForm.reset();
          if(editPrev) editPrev.innerHTML = '';
        }
        else{ toast(j.error || 'Ошибка обновления','error'); }
      }catch(err){ toast('Ошибка сети','error'); }
    });
  }

  function filters(){
    const q = document.getElementById('searchFilm');
    const g = document.getElementById('filterGenre');
    async function run(){
      const params = new URLSearchParams();
      if(q?.value) params.set('query', q.value);
      if(g?.value && g.value !== 'all') params.set('genre', g.value);
      const r = await fetch('/api/films/search?'+params.toString());
      const j = await r.json();
      const tbody = document.querySelector('#filmList tbody');
      tbody.innerHTML = (j||[]).map(f=>{
        const idCell = (f.code || (f.id!=null? f.id.toString().padStart(5,'0') : ''));
        const siteCell = f.site ? `<a href="${f.site}" target="_blank">ссылка</a>` : '';
        const imgCell = f.photo_id ? `<span class="badge">img</span>` : '';
        return `
          <tr>
            <td>${idCell}</td>
            <td>${f.name||''}</td>
            <td>${f.genre||''}</td>
            <td>${siteCell}</td>
            <td>${imgCell}</td>
            <td>
              <button class="row-edit" data-id="${f.id}"><i class="ti ti-edit"></i></button>
              <button class="row-delete" data-id="${f.id}"><i class="ti ti-trash"></i></button>
            </td>
          </tr>`;
      }).join('');
    }
    q?.addEventListener('input', ()=>{ clearTimeout(q._t); q._t=setTimeout(run, 250); });
    g?.addEventListener('change', run);
  }

  // ==================== Enhanced Genre Multi-Select (Dropdown) ====================
function enhanceMultiSelect(select){
  if(!select || select._ms) return;
  const wrap = document.createElement('div');
  wrap.className = 'genre-select';
  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.className = 'genre-trigger';
  const triggerLabel = document.createElement('span');
  triggerLabel.className = 'genre-trigger-label';
  triggerLabel.textContent = 'Выберите жанры';
  const triggerIcon = document.createElement('i');
  triggerIcon.className = 'ti ti-chevron-down';
  trigger.appendChild(triggerLabel);
  trigger.appendChild(triggerIcon);
  const dropdown = document.createElement('div');
  dropdown.className = 'genre-dropdown';
  const list = document.createElement('div');
  list.className = 'genre-options';

  const updateTrigger = ()=>{
    const values = Array.from(select.options).filter(o=>o.selected && o.value).map(o=>o.textContent.trim());
    if(values.length === 0){
      triggerLabel.textContent = 'Выберите жанры';
    }else if(values.length <= 3){
      triggerLabel.textContent = values.join(', ');
    }else{
      triggerLabel.textContent = `${values.length} выбрано`;
    }
  };

  const buildItem = (opt)=>{
    const item = document.createElement('div');
    item.className = 'genre-option';
    item.setAttribute('data-value', opt.value);
    const label = document.createElement('span');
    label.className = 'label';
    label.textContent = opt.textContent.trim();
    const check = document.createElement('i');
    check.className = 'ti ti-check check';
    item.appendChild(label);
    item.appendChild(check);
    if(opt.selected) item.classList.add('selected');
    item.addEventListener('click', (e)=>{
      e.stopPropagation();
      opt.selected = !opt.selected;
      item.classList.toggle('selected', opt.selected);
      updateTrigger();
      // bubble a change so external code can react if needed
      select.dispatchEvent(new Event('change', { bubbles: true }));
    });
    return item;
  };

  Array.from(select.options).forEach(opt=>{
    if(!opt.value) return; // skip placeholder
    list.appendChild(buildItem(opt));
  });

  dropdown.appendChild(list);
  wrap.appendChild(trigger);
  wrap.appendChild(dropdown);
  select.classList.add('visually-hidden-select');
  select.parentNode.insertBefore(wrap, select.nextSibling);

  const open = ()=>{ wrap.classList.add('open'); };
  const close = ()=>{ wrap.classList.remove('open'); };
  const toggle = ()=>{ wrap.classList.toggle('open'); };
  trigger.addEventListener('click', (e)=>{ e.stopPropagation(); toggle(); });
  document.addEventListener('click', (e)=>{ if(!wrap.contains(e.target)) close(); });
  document.addEventListener('keydown', (e)=>{ if(e.key === 'Escape') close(); });

  // store refs and sync helper
  select._ms = { wrap, list, triggerLabel };
  select._msSync = ()=>{
    const selected = new Set(Array.from(select.options).filter(o=>o.selected).map(o=>o.value));
    Array.from(list.children).forEach(node=>{
      const v = node.getAttribute('data-value');
      node.classList.toggle('selected', selected.has(v));
    });
    updateTrigger();
  };
  // keep UI in sync if selection changes programmatically
  select.addEventListener('change', ()=> select._msSync && select._msSync());
  // initial label state
  updateTrigger();
}

// ==================== Single Select Dropdown (for filters) ====================
function enhanceSingleSelect(select){
  if(!select || select._ss) return;
  const wrap = document.createElement('div');
  wrap.className = 'genre-select';
  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.className = 'genre-trigger';
  const triggerLabel = document.createElement('span');
  triggerLabel.className = 'genre-trigger-label';
  const current = select.options[select.selectedIndex]?.textContent?.trim() || 'Все жанры';
  triggerLabel.textContent = current;
  const triggerIcon = document.createElement('i');
  triggerIcon.className = 'ti ti-chevron-down';
  trigger.appendChild(triggerLabel);
  trigger.appendChild(triggerIcon);

  const dropdown = document.createElement('div');
  dropdown.className = 'genre-dropdown';
  const list = document.createElement('div');
  list.className = 'genre-options';

  const buildItem = (opt)=>{
    const item = document.createElement('div');
    item.className = 'genre-option';
    item.setAttribute('data-value', opt.value);
    const label = document.createElement('span');
    label.className = 'label';
    label.textContent = opt.textContent.trim();
    const check = document.createElement('i');
    check.className = 'ti ti-check check';
    item.appendChild(label);
    item.appendChild(check);
    if(select.value === opt.value) item.classList.add('selected');
    item.addEventListener('click', (e)=>{
      e.stopPropagation();
      select.value = opt.value;
      // update selected class
      Array.from(list.children).forEach(n=>n.classList.remove('selected'));
      item.classList.add('selected');
      // update label
      triggerLabel.textContent = opt.textContent.trim();
      // notify listeners and close
      select.dispatchEvent(new Event('change', { bubbles: true }));
      wrap.classList.remove('open');
    });
    return item;
  };

  Array.from(select.options).forEach(opt=>{
    list.appendChild(buildItem(opt));
  });

  dropdown.appendChild(list);
  wrap.appendChild(trigger);
  wrap.appendChild(dropdown);
  select.classList.add('visually-hidden-select');
  select.parentNode.insertBefore(wrap, select.nextSibling);

  const open = ()=>{ wrap.classList.add('open'); };
  const close = ()=>{ wrap.classList.remove('open'); };
  const toggle = ()=>{ wrap.classList.toggle('open'); };
  trigger.addEventListener('click', (e)=>{ e.stopPropagation(); toggle(); });
  document.addEventListener('click', (e)=>{ if(!wrap.contains(e.target)) close(); });
  document.addEventListener('keydown', (e)=>{ if(e.key === 'Escape') close(); });

  // store refs and sync helper
  select._ss = { wrap, list, triggerLabel };
  select._ssSync = ()=>{
    const val = select.value;
    Array.from(list.children).forEach(node=>{
      node.classList.toggle('selected', node.getAttribute('data-value') === val);
    });
    const opt = Array.from(select.options).find(o=>o.value===val) || select.options[select.selectedIndex];
    triggerLabel.textContent = opt ? opt.textContent.trim() : 'Все жанры';
  };
  select.addEventListener('change', ()=> select._ssSync && select._ssSync());
}

function init(){
  // Update banner
  (function updateBannerInit(){
    const banner = document.getElementById('updateBanner');
    const textEl = document.getElementById('updateText');
    const subEl = document.getElementById('updateSub');
    const notesWrap = banner ? banner.querySelector('#updateNotesWrap') : null;
    const notesEl = banner ? banner.querySelector('#updateNotes') : null;
    const btn = document.getElementById('updateNowBtn');
    if(!banner || !textEl || !subEl || !btn) return;

    async function refresh(){
      try{
        const r = await fetch('/api/update/status');
        if(!r.ok) return; // тихо игнорируем, если бэкенд ещё без маршрута
        const j = await r.json();
        if(j && j.available){
          const latest = j.latest || '';
          const curr = j.current || '';
          textEl.textContent = latest ? `Доступно обновление ${latest}` : 'Доступно обновление';
          subEl.textContent = curr ? `Текущая версия: ${curr}` : '';
          const notes = (j.notes || '').trim();
          if(notes){
            if(notesEl) notesEl.textContent = notes;
            if(notesWrap) notesWrap.style.display = '';
          } else {
            if(notesEl) notesEl.textContent = '';
            if(notesWrap) notesWrap.style.display = 'none';
          }
          banner.style.display = '';
        } else {
          if(notesEl) notesEl.textContent = '';
          if(notesWrap) notesWrap.style.display = 'none';
          banner.style.display = 'none';
        }
      }catch(_){ /* noop */ }
    }

    if(!btn._bound){
      btn._bound = true;
      btn.addEventListener('click', async ()=>{
        btn.disabled = true;
        const old = btn.innerHTML;
        btn.innerHTML = '<span class="btn-icon"><i class="ti ti-loader-2"></i></span> Обновление…';
        try{
          const r = await fetch('/api/update/apply', { method: 'POST' });
          const j = await r.json().catch(()=>({}));
          if(r.status === 202){
            toast('Обновление запущено. Сервер может перезапуститься.','success');
            banner.style.display = 'none';
          } else if(r.ok){
            toast(j.message || 'Нет доступных обновлений','info');
          } else {
            toast(j.detail || j.error || 'Ошибка запуска обновления','error');
          }
        }catch(_){ toast('Ошибка сети','error'); }
        finally{
          btn.disabled = false;
          btn.innerHTML = old;
        }
      });
    }

    refresh();
    setInterval(refresh, 30000);
  })();

  bindThemeToggle();
  bindNav();
  forms();
  // Enhance genre selects with modern UI and checkmarks
  const g1 = document.getElementById('filmGenre'); if(g1) enhanceMultiSelect(g1);
  const g2 = document.getElementById('editFilmGenre'); if(g2) enhanceMultiSelect(g2);
  // Enhance filter genre as dropdown
  const fg = document.getElementById('filterGenre'); if(fg) enhanceSingleSelect(fg);
  filters();
  tmdb();
  // Автозагрузка статистики при старте, если секция уже активна
  if(document.getElementById('statsSection')?.classList.contains('active')){ Stats.load(); }
  socketInit();
}

  document.addEventListener('DOMContentLoaded', init);
})();
