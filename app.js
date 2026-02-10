const tg = window.Telegram?.WebApp;

  const NEWS_SEED = [
    {
      id: 101,
      title: "Обновили прайс и доступность",
      category: "Обновление",
      date: "2024-10-01",
      text: "Добавили новые позиции и актуализировали остатки по складу."
    },
    {
      id: 102,
      title: "Акция недели на светлое",
      category: "Акция",
      date: "2024-10-04",
      text: "Скидки до 12% на линейку крафта до воскресенья."
    },
    {
      id: 103,
      title: "График доставки на праздники",
      category: "Новость",
      date: "2024-10-07",
      text: "Доставка переносится на день раньше, подтверждение до 16:00."
    }
  ];
  const NEWS = [];

  const FILTERS = ["Все", "Новость", "Обновление", "Акция", "Сервис"];

  let activeFilter = "Все";
  let editingId = null;
  let currentRole = "client";
  let isAdminUser = false;
  let isAuthorizedUser = false;
  let publishInFlight = false;

  const ACCESS_STATE_KEY = "beerMarketAccessState";

  function setSafeInsets() {
    const top = tg?.safeAreaInset?.top ?? tg?.contentSafeAreaInset?.top ?? 0;
    const bottom = tg?.safeAreaInset?.bottom ?? tg?.contentSafeAreaInset?.bottom ?? 0;
    document.documentElement.style.setProperty('--tgTop', top + 'px');
    document.documentElement.style.setProperty('--tgBottom', bottom + 'px');
  }

  function applyTgTheme() {
    const p = tg?.themeParams || {};
    const css = document.documentElement.style;
    if (p.bg_color) css.setProperty('--bg', p.bg_color);
    if (p.secondary_bg_color) css.setProperty('--card', p.secondary_bg_color);
    if (p.text_color) css.setProperty('--text', p.text_color);
    if (p.hint_color) css.setProperty('--muted', p.hint_color);
    if (p.button_color) css.setProperty('--btn', p.button_color);
  }

  function popupOk(title, msg) {
    if (!tg?.showPopup) return;
    tg.showPopup({
      title: title || "BeerMarket",
      message: msg || "Отправлено",
      buttons: [{type:"ok"}]
    });
  }

  function showPublishStatus(message, kind = "muted") {
    const node = document.getElementById("publishStatus");
    if (!node) return;
    node.textContent = message;
    node.style.color = kind === "error"
      ? "var(--danger)"
      : (kind === "success" ? "var(--accent)" : "var(--muted)");
  }

  function setPublishBusyState(busy, text) {
    publishInFlight = Boolean(busy);
    const btn = document.getElementById("btnPublish");
    if (!btn) return;
    btn.disabled = publishInFlight;
    btn.textContent = publishInFlight ? (text || "Отправка...") : (editingId ? "Сохранить изменения" : "Опубликовать");
  }

  function idsEqual(a, b) {
    return String(a) === String(b);
  }

  function applyEditorState() {
    const btn = document.getElementById("btnPublish");
    if (btn && !publishInFlight) {
      btn.textContent = editingId ? "Сохранить изменения" : "Опубликовать";
    }
    showPublishStatus(
      editingId
        ? "Режим редактирования: после сохранения обновится существующая новость."
        : "Черновик не опубликован.",
      "muted"
    );
  }

  function normalizeLocalItem(item) {
    return {
      ...item,
      publishState: item?.publishState === "draft" ? "draft" : "published"
    };
  }

  function getActionAckEndpoints() {
    const params = new URLSearchParams(location.search);
    const explicit = (params.get("action_api") || "").trim();
    const authApi = (params.get("auth_api") || "").trim();
    const out = [];

    const add = (url) => {
      if (!url || out.includes(url)) return;
      out.push(url);
    };

    add(explicit);
    if (authApi) {
      add(authApi.replace(/\/miniapp\/auth\/?$/, "/miniapp/news-action"));
    }
    add("/miniapp/news-action");
    return out;
  }

  async function sendAction(action, extra = {}, options = {}) {
    const payload = { action, ts: Date.now(), ...extra };
    const needsAck = Boolean(options?.requireAck);

    if (!needsAck) {
      if (tg?.sendData) {
        tg.sendData(JSON.stringify(payload));
        tg?.HapticFeedback?.impactOccurred?.("light");
        return { ok: true, applied: true, transport: "telegram" };
      }
      return { ok: true, applied: false, transport: "local" };
    }

    const endpoints = getActionAckEndpoints();
    let lastError = new Error("Сервер подтверждения недоступен");
    let methodNotAllowedOrMissing = false;

    for (const endpoint of endpoints) {
      try {
        const res = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            initData: tg?.initData || "",
            auth: isAuthorizedUser ? "1" : "0",
            role: currentRole,
            uid: tg?.initDataUnsafe?.user?.id || new URLSearchParams(location.search).get("uid") || null,
            payload
          })
        });
        if (!res.ok) {
          if (res.status === 404 || res.status === 405) {
            methodNotAllowedOrMissing = true;
            if (endpoint !== endpoints[endpoints.length - 1]) {
              lastError = new Error(`HTTP ${res.status}`);
              continue;
            }
          }
          throw new Error(`HTTP ${res.status}`);
        }

        const data = await res.json();
        if (!data?.ok) {
          throw new Error(data?.message || "Сервер отклонил применение изменений");
        }
        tg?.HapticFeedback?.impactOccurred?.("light");
        if (tg?.sendData) tg.sendData(JSON.stringify(payload));
        return {
          ...data,
          applied: data?.applied !== false,
          pending: data?.applied === false
        };
      } catch (e) {
        lastError = new Error(e?.message || "Ошибка отправки действия");
      }
    }

    if (methodNotAllowedOrMissing && tg?.sendData) {
      tg.sendData(JSON.stringify(payload));
      tg?.HapticFeedback?.impactOccurred?.("light");
      return {
        ok: true,
        applied: false,
        pending: true,
        transport: "telegram",
        fallback: "ack_endpoint_unavailable"
      };
    }

    throw lastError;
  }

  let newsSignature = "";

  function computeNewsSignature(items) {
    if (!Array.isArray(items)) return "";
    return items
      .map(item => `${item.id}:${item.seq || ""}:${item.title}:${item.category}:${item.date}:${item.text}:${item.updatedAt || ""}`)
      .join("|");
  }

  function nextNewsSeq() {
    const seqs = NEWS.map(item => Number(item.seq)).filter(Number.isFinite);
    if (seqs.length) return Math.max(...seqs) + 1;
    return NEWS.length + 1;
  }

  function formatDateInput(value) {
    if (value instanceof Date) return value.toISOString().slice(0, 10);
    if (typeof value === "string" && value) return value;
    return new Date().toISOString().slice(0, 10);
  }

  function toIsoDate(value) {
    const raw = String(value || "").trim();
    if (!raw) return formatDateInput();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const ru = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (ru) {
      const [, d, m, y] = ru;
      return `${y}-${m}-${d}`;
    }
    const dt = new Date(raw);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    return formatDateInput();
  }

  function normalizeNewsItem(item) {
    return normalizeLocalItem({
      ...item,
      date: toIsoDate(item?.date || item?.createdAt),
      publishState: item?.publishState === "draft" ? "draft" : "published"
    });
  }

  function mergeServerNewsWithLocal(serverItems) {
    const isSameNewsContent = (left, right) => {
      if (!left || !right) return false;
      return (
        String(left.title || "").trim() === String(right.title || "").trim() &&
        String(left.category || "").trim() === String(right.category || "").trim() &&
        toIsoDate(left.date || left.createdAt) === toIsoDate(right.date || right.createdAt) &&
        String(left.text || "").trim() === String(right.text || "").trim()
      );
    };

    const localDraftById = new Map(
      NEWS
        .filter(item => item.publishState === "draft")
        .map(item => [String(item.id), normalizeNewsItem(item)])
    );
    const usedDraftIds = new Set();
    const merged = serverItems.map(item => {
      const draft = localDraftById.get(String(item.id));
      if (!draft) {
        // Фолбэк для случаев, когда сервер сохранил новость с другим id,
        // но содержимое уже совпадает с локальным черновиком.
        const matchingDraft = NEWS.find(localItem =>
          localItem.publishState === "draft" &&
          !usedDraftIds.has(String(localItem.id)) &&
          isSameNewsContent(localItem, item)
        );
        if (matchingDraft) {
          usedDraftIds.add(String(matchingDraft.id));
          return normalizeNewsItem({ ...item, publishState: "published" });
        }
        return normalizeNewsItem(item);
      }

      // Если сервер уже содержит ту же версию, считаем черновик подтверждённым.
      if (isSameNewsContent(draft, item)) {
        usedDraftIds.add(String(draft.id));
        return normalizeNewsItem({ ...item, publishState: "published" });
      }
      usedDraftIds.add(String(draft.id));
      return draft;
    });
    for (const [id, draft] of localDraftById.entries()) {
      if (!usedDraftIds.has(id) && !merged.some(item => String(item.id) === id)) {
        merged.unshift(draft);
      }
    }
    return merged;
  }

  async function syncNewsAfterMutation(successMessage = "Локальные изменения подтверждены сервером.") {
    await refreshNews({ force: true });
    const stillDrafts = NEWS.some(item => item.publishState === "draft");
    if (stillDrafts) {
      showPublishStatus("Изменения отправлены. Ожидаем подтверждение от сервера…", "muted");
      popupOk("BeerMarket", "Изменения отправлены. Подтверждение придёт с обновлением ленты.");
      return false;
    }
    showPublishStatus(successMessage, "success");
    return true;
  }

  async function publishDraftItem(item) {
    if (!item) return false;
    const payload = {
      id: item.id,
      seq: item.seq,
      title: item.title,
      category: item.category,
      date: toIsoDate(item.date || item.createdAt),
      text: item.text,
      createdAt: item.createdAt,
      updatedAt: new Date().toISOString(),
      publishState: "published"
    };
    const result = await sendAction("news.update", payload, { requireAck: true });
    if (result?.applied) {
      item.publishState = "published";
      item.updatedAt = payload.updatedAt;
    }
    return result?.applied !== false;
  }

  function formatDisplayDate(value, fallback) {
    const raw = value || fallback;
    if (!raw) return "";
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      const [y, m, d] = raw.split("-");
      return `${d}.${m}.${y}`;
    }
    return raw;
  }

  function loadAccessState() {
    try {
      const raw = localStorage.getItem(ACCESS_STATE_KEY);
      return raw ? JSON.parse(raw) : {};
    } catch (e) {
      console.warn("access state load failed", e);
      return {};
    }
  }

  function saveAccessState(role, authorized) {
    try {
      localStorage.setItem(
        ACCESS_STATE_KEY,
        JSON.stringify({
          role,
          authorized: Boolean(authorized),
          updatedAt: Date.now()
        })
      );
    } catch (e) {
      console.warn("access state save failed", e);
    }
  }

  function buildMiniappAuthQuery() {
    const params = new URLSearchParams(location.search);
    const auth = (params.get("auth") || "").trim();
    const role = (params.get("role") || "").trim();
    const uid = (params.get("uid") || "").trim();
    const query = new URLSearchParams();
    if (tg?.initData) {
      query.set("initData", tg.initData);
    }
    if (auth) query.set("auth", auth);
    if (role) query.set("role", role);
    if (uid) query.set("uid", uid);
    return query.toString();
  }

  function newsFetchCandidates() {
    const stamp = Date.now();
    const params = new URLSearchParams(location.search);
    const explicit = (params.get("news_api") || "").trim();
    const authQuery = buildMiniappAuthQuery();
    const withQuery = (url) => {
      if (!url) return "";
      const joiner = url.includes("?") ? "&" : "?";
      return `${url}${joiner}v=${stamp}${authQuery ? `&${authQuery}` : ""}`;
    };
    const base = [
      withQuery("/miniapp/news")
    ];
    if (!explicit) return base;
    return [withQuery(explicit), ...base];
  }

  async function fetchNewsFromAnySource() {
    const candidates = newsFetchCandidates();
    let lastError = null;
    for (const url of candidates) {
      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const items = Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : null);
        if (!Array.isArray(items)) throw new Error("invalid payload");
        return items;
      } catch (e) {
        lastError = e;
      }
    }
    throw (lastError || new Error("Не удалось загрузить новости"));
  }

  async function loadNews() {
    try {
      const data = await fetchNewsFromAnySource();
      const serverItems = data.map(item => normalizeNewsItem({ ...item, publishState: "published" }));
      NEWS.splice(0, NEWS.length, ...serverItems);
      newsSignature = computeNewsSignature(serverItems);
      showPublishStatus("Лента загружена с сервера.", "success");
      return;
    } catch (e) {
      console.warn("news load failed, using seed", e);
    }
    NEWS.splice(0, NEWS.length, ...NEWS_SEED);
    newsSignature = computeNewsSignature(NEWS_SEED);
    showPublishStatus("Сервер недоступен, показываем локальный пример ленты.", "error");
  }


  function resolveUserLabel(tgUser, params) {
    const queryUser = {
      id: params.get("uid"),
      username: params.get("username"),
      firstName: params.get("first_name"),
      lastName: params.get("last_name"),
      name: params.get("name")
    };
    const parts = [];
    const username = tgUser?.username || queryUser.username;
    const fullName = `${tgUser?.first_name || queryUser.firstName || ""} ${tgUser?.last_name || queryUser.lastName || ""}`.trim();
    if (username) {
      parts.push(`@${username}`);
    } else if (fullName) {
      parts.push(fullName);
    } else if (queryUser.name) {
      parts.push(queryUser.name);
    }
    const id = tgUser?.id || queryUser.id;
    if (id) {
      parts.push(`id ${id}`);
    }
    return parts.length ? parts.join(" • ") : "Пользователь не передан";
  }

  function renderChips() {
    const box = document.getElementById("chips");
    box.innerHTML = "";
    for (const f of FILTERS) {
      const chip = document.createElement("button");
      chip.className = "chip" + (f === activeFilter ? " active" : "");
      chip.textContent = f;
      chip.onclick = () => {
        activeFilter = f;
        renderChips();
        renderNews();
      };
      box.appendChild(chip);
    }
  }

  function renderNews() {
    const list = document.getElementById("newsList");
    list.innerHTML = "";
    const visibleItems = isAdminUser ? NEWS : NEWS.filter(item => item.publishState !== "draft");
    const items = visibleItems.filter(item => activeFilter === "Все" || item.category === activeFilter);

    if (!items.length) {
      list.innerHTML = `<div class="muted">Пока нет новостей по выбранной теме.</div>`;
      return;
    }

    items.forEach((item, index) => {
      const el = document.createElement("div");
      el.className = "newsItem";
      const displayDate = formatDisplayDate(item.date, item.createdAt);
      const displayNumber = Number.isFinite(Number(item.seq)) ? item.seq : index + 1;
      el.innerHTML = `
        <div class="newsHeader">
          <div><strong>${item.title}</strong></div>
          <span class="badge">${item.category}</span>
        </div>
        ${item.publishState === "draft" ? '<div class="small" style="color:var(--warning)">Черновик • не опубликовано</div>' : ''}
        <div>${item.text}</div>
        <div class="newsMeta">№${displayNumber}${displayDate ? ` • ${displayDate}` : ""}</div>
      `;
      list.appendChild(el);
    });
  }

  async function refreshNews(options = {}) {
    const force = Boolean(options?.force);
    try {
      const data = await fetchNewsFromAnySource();
      if (!Array.isArray(data)) return;
      const serverItems = data.map(item => normalizeNewsItem({ ...item, publishState: "published" }));
      const signature = computeNewsSignature(serverItems);
      const hasDrafts = NEWS.some(item => item.publishState === "draft");

      if (signature !== newsSignature || force) {
        newsSignature = signature;
        const merged = mergeServerNewsWithLocal(serverItems);
          NEWS.splice(0, NEWS.length, ...merged);
        const hasPendingDrafts = merged.some(item => item.publishState === "draft");
        showPublishStatus(hasPendingDrafts ? "Обновили ленту с сервера и сохранили локальные черновики." : "Локальные изменения подтверждены сервером.", hasPendingDrafts ? "muted" : "success");
        renderNews();
        renderAdminList();
      }
    } catch (e) {
      console.warn("news refresh failed", e);
    }
  }

  function renderAdminList() {
    const list = document.getElementById("adminList");
    list.innerHTML = "";

    NEWS.forEach((item, index) => {
      const el = document.createElement("div");
      el.className = "newsItem";
      const displayDate = formatDisplayDate(item.date, item.createdAt);
      const displayNumber = Number.isFinite(Number(item.seq)) ? item.seq : index + 1;
      el.innerHTML = `
        <div class="newsHeader">
          <div><strong>${item.title}</strong></div>
          <span class="badge">${item.category}</span>
        </div>
        ${item.publishState === "draft" ? '<div class="small" style="color:var(--warning)">Черновик • не опубликовано</div>' : ''}
        <div>${item.text}</div>
        <div class="newsMeta">№${displayNumber}${displayDate ? ` • ${displayDate}` : ""}</div>
        <div class="row2" style="margin-top:8px;">
          <button class="secondary" data-edit="${item.id}">Редактировать</button>
          ${item.publishState === "draft" ? `<button data-publish="${item.id}">Опубликовать</button>` : ""}
          <button class="danger" data-delete="${item.id}">Удалить</button>
        </div>
      `;
      list.appendChild(el);
    });

    list.querySelectorAll("button[data-publish]").forEach(btn => {
      btn.onclick = async () => {
        if (publishInFlight) return;
        const id = btn.getAttribute("data-publish");
        const item = NEWS.find(n => idsEqual(n.id, id));
        if (!item) return;
        setPublishBusyState(true);
        showPublishStatus("Публикуем черновик...", "muted");
        try {
          const confirmed = await publishDraftItem(item);
          renderNews();
          renderAdminList();
          if (confirmed) {
            await syncNewsAfterMutation("Черновик опубликован.");
          } else {
            showPublishStatus("Изменения отправлены. Ожидаем подтверждение от сервера…", "muted");
          }
        } catch (e) {
          showPublishStatus(`Ошибка публикации: ${e.message}. Черновик не опубликован.`, "error");
          popupOk("Ошибка", `Не удалось опубликовать черновик: ${e.message}`);
        } finally {
          setPublishBusyState(false);
        }
      };
    });

    list.querySelectorAll("button[data-edit]").forEach(btn => {
      btn.onclick = () => {
        const id = btn.getAttribute("data-edit");
        const item = NEWS.find(n => idsEqual(n.id, id));
        if (!item) return;
        editingId = item.id;
        document.getElementById("newsTitle").value = item.title;
        document.getElementById("newsCategory").value = item.category;
        document.getElementById("newsDate").value = toIsoDate(item.date || item.createdAt);
        document.getElementById("newsText").value = item.text;
        applyEditorState();
      };
    });

    list.querySelectorAll("button[data-delete]").forEach(btn => {
      btn.onclick = async () => {
        if (publishInFlight) return;
        const id = btn.getAttribute("data-delete");
        const idx = NEWS.findIndex(n => idsEqual(n.id, id));
        if (idx === -1) return;
        const removed = NEWS[idx];
        setPublishBusyState(true);
        showPublishStatus("Отправка...", "muted");
        try {
          const deleteResult = await sendAction("news.delete", { id: removed.id, title: removed.title }, { requireAck: true });
          NEWS.splice(idx, 1);
          if (editingId && idsEqual(editingId, removed.id)) {
            resetForm();
          }
          const confirmed = deleteResult?.applied !== false;
          renderNews();
          renderAdminList();
          if (confirmed) {
            await syncNewsAfterMutation("Удаление подтверждено сервером.");
          } else {
            showPublishStatus("Изменения отправлены. Ожидаем подтверждение от сервера…", "muted");
            popupOk("BeerMarket", "Удаление отправлено. Подтверждение придёт с обновлением ленты.");
          }
        } catch (e) {
          showPublishStatus(`Ошибка публикации: ${e.message}. Черновик не опубликован.`, "error");
          popupOk("Ошибка", `Не удалось удалить: ${e.message}`);
        } finally {
          setPublishBusyState(false);
        }
      };
    });
  }

  function resetForm() {
    editingId = null;
    document.getElementById("newsTitle").value = "";
    document.getElementById("newsCategory").value = "Новость";
    document.getElementById("newsDate").value = formatDateInput();
    document.getElementById("newsText").value = "";
    applyEditorState();
  }

  function applyAccessUi(role, isAuthorized, loading = false) {
    isAdminUser = isAuthorized && role === "admin";
    currentRole = role || "client";
    isAuthorizedUser = isAuthorized;
    const isAdmin = isAdminUser;
    const canSuggest = isAuthorized && role === "sales_rep";

    const badge = document.getElementById("accessBadge");
    badge.textContent = loading
      ? "Проверка доступа…"
      : (isAuthorized ? `Доступ: открыт (${role})` : "Доступ: требуется авторизация");
    badge.classList.toggle("success", !loading && isAuthorized);
    badge.classList.toggle("danger", !loading && !isAuthorized);

    document.getElementById("accessGate").classList.toggle("hidden", loading || isAuthorized);
    document.getElementById("feedSection").classList.toggle("hidden", loading || !isAuthorized);
    document.getElementById("adminSection").classList.toggle("hidden", loading || !isAdmin);
    document.getElementById("btnSuggest").classList.toggle("hidden", loading || !canSuggest);


    document.getElementById("btnRequestAccess").onclick = () =>
      sendAction("access.request", { role: currentRole });
    document.getElementById("btnContact").onclick = () =>
      sendAction("manager.contact", { role: currentRole });
  }

  async function setupAccess() {
    const params = new URLSearchParams(location.search);
    const storedAccess = loadAccessState();
    const authParam = params.get("auth");
    const hasAuthParam = authParam !== null;
    const paramRole = params.get("role");
    const fallbackRole = paramRole || storedAccess.role || "client";
    const fallbackAuthorized = hasAuthParam ? authParam === "1" : Boolean(storedAccess.authorized);

    applyAccessUi(fallbackRole, fallbackAuthorized, true);

    const authApi = params.get("auth_api") || "/miniapp/auth";
    const payload = {
      initData: tg?.initData || "",
      auth: authParam,
      role: paramRole,
      uid: params.get("uid")
    };

    try {
      const res = await fetch(authApi, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const profile = await res.json();
      const role = profile?.role || fallbackRole;
      const isAuthorized = Boolean(profile?.authorized);
      saveAccessState(role, isAuthorized);

      if (paramRole && paramRole !== role) {
        console.warn("miniapp security mismatch: query role differs from server", {
          queryRole: paramRole,
          serverRole: role,
          uid: profile?.uid
        });
      }
      if (hasAuthParam && (authParam === "1") !== isAuthorized) {
        console.warn("miniapp security mismatch: query auth differs from server", {
          queryAuth: authParam,
          serverAuthorized: isAuthorized,
          uid: profile?.uid
        });
      }

      applyAccessUi(role, isAuthorized, false);
      return;
    } catch (e) {
      console.warn("access verify failed, using debug fallback", e);
    }

    saveAccessState(fallbackRole, fallbackAuthorized);
    applyAccessUi(fallbackRole, fallbackAuthorized, false);
  }

  if (tg) {
    tg.ready();
    tg.expand();
    setSafeInsets();
    applyTgTheme();

    tg.onEvent?.("safeAreaChanged", setSafeInsets);
    tg.onEvent?.("contentSafeAreaChanged", setSafeInsets);
    tg.onEvent?.("themeChanged", applyTgTheme);

    const params = new URLSearchParams(location.search);
    const u = tg.initDataUnsafe?.user;
    document.getElementById("user").textContent = resolveUserLabel(u, params);

    const ver = params.get("v");
    document.getElementById("env").textContent =
      `platform: ${tg.platform || "-"} • ver: ${tg.version || "-"}${ver ? " • v=" + ver : ""}`;
  } else {
    const params = new URLSearchParams(location.search);
    document.getElementById("user").textContent = resolveUserLabel(null, params);
  }

  async function initApp() {
    await setupAccess();
    await loadNews();
    renderChips();
    renderNews();
    renderAdminList();
    resetForm();
    setInterval(refreshNews, 2000);
  }

  initApp();

  document.getElementById("btnClose").onclick = () => tg?.close?.();
  document.getElementById("btnRefresh").onclick = async () => {
    await refreshNews({ force: true });
  };

  document.getElementById("btnSuggest").onclick = () =>
    sendAction("news.suggest");

  document.getElementById("btnPublish").onclick = async () => {
    if (publishInFlight) return;
    const title = document.getElementById("newsTitle").value.trim();
    const category = document.getElementById("newsCategory").value;
    const date = toIsoDate(document.getElementById("newsDate").value);
    const text = document.getElementById("newsText").value.trim();
    if (!title || !text || !date) return popupOk("Новость", "Заполните все поля");

    setPublishBusyState(true);
    showPublishStatus("Сохранение черновика...", "muted");
    let prevSnapshot = JSON.stringify(NEWS);
    try {
      let publishResult = null;
      if (editingId) {
        const item = NEWS.find(n => idsEqual(n.id, editingId));
        if (item) {
          item.title = title;
          item.category = category;
          item.date = date;
          item.text = text;
          item.updatedAt = new Date().toISOString();
          item.publishState = "draft";
          renderNews();
          renderAdminList();
          publishResult = await sendAction("news.update", { id: item.id, seq: item.seq, title, category, date, text, publishState: "draft" }, { requireAck: true });
        }
      } else {
        await refreshNews({ force: true });
        const id = Date.now();
        const seq = nextNewsSeq();
        const nowIso = new Date().toISOString();
        NEWS.unshift({ id, seq, title, category, date, text, createdAt: nowIso, updatedAt: nowIso, publishState: "draft" });
        renderNews();
        renderAdminList();
        publishResult = await sendAction("news.create", { id, seq, title, category, date, text, createdAt: nowIso, updatedAt: nowIso, publishState: "draft" }, { requireAck: true });
      }
      const confirmed = publishResult?.applied !== false;
      renderNews();
      renderAdminList();
      resetForm();
      if (confirmed) {
        await syncNewsAfterMutation("Черновик сохранён на сервере.");
        popupOk("BeerMarket", "Черновик сохранён");
      } else {
        showPublishStatus("Изменения отправлены. Ожидаем подтверждение от сервера…", "muted");
        popupOk("BeerMarket", "Изменения отправлены. Подтверждение придёт с обновлением ленты.");
      }
    } catch (e) {
      try {
        const parsed = JSON.parse(prevSnapshot);
        NEWS.splice(0, NEWS.length, ...parsed.map(normalizeNewsItem));
      } catch (_) {}
      renderNews();
      renderAdminList();
      showPublishStatus(`Ошибка публикации: ${e.message}. Черновик не опубликован.`, "error");
      popupOk("Ошибка", `Сервер не подтвердил публикацию: ${e.message}`);
    } finally {
      setPublishBusyState(false);
    }
  };

  document.getElementById("btnReset").onclick = resetForm;