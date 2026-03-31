const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const params = new URLSearchParams(location.search);
const explicitApiBase = params.get('api_base');
const projectBase = new URL('../../', window.location.href).href;
const sameOriginBase = `${window.location.origin}/`;
const feed = document.getElementById('feed');
const statusEl = document.getElementById('status');
const loadMoreBtn = document.getElementById('loadMore');
const tpl = document.getElementById('news-card-template');
const syncNowBtn = document.getElementById('syncNow');

let offset = 0;
const limit = 6;
const refreshIntervalMs = 30000;
let usesStaticFeed = false;
let resolvedApiBase = null;
let lastLoadError = '';
let isLoading = false;
let resolvedStaticSource = null;
let cacheBust = Date.now();

function joinUrl(base, path) {
  return new URL(path.replace(/^\/+/, ''), base.endsWith('/') ? base : `${base}/`).href;
}

function withCacheBust(url) {
  try {
    const nextUrl = new URL(url, window.location.href);
    nextUrl.searchParams.set('_sync', String(cacheBust));
    return nextUrl.href;
  } catch (_) {
    return url;
  }
}

function normalizeBase(base) {
  if (!base) return null;
  try {
    const normalized = new URL(base, window.location.href).href;
    if (window.location.protocol === 'https:' && normalized.startsWith('http://')) {
      console.warn('Ignoring insecure api_base on HTTPS page:', normalized);
      return null;
    }
    return normalized;
  } catch (error) {
    console.warn('Invalid api_base value:', base, error);
    return null;
  }
}

function buildApiCandidates() {
  const candidates = [
    normalizeBase(explicitApiBase),
    normalizeBase(projectBase),
    normalizeBase(sameOriginBase),
  ].filter(Boolean);
  return [...new Set(candidates)];
}

function buildStaticCandidates(base) {
  return [
    joinUrl(base, 'news.json'),
    joinUrl(base, 'pythonProject/news.json'),
    joinUrl(base, 'pythonProject/webapp/news.json'),
  ];
}

function resolveMediaUrl(item) {
  if (item?.url) return item.url;
  const filePath = item?.file_path || '';
  if (!filePath) return '';
  const normalized = String(filePath).replace(/\\/g, '/');
  const marker = '/data/news/media/';
  const markerIdx = normalized.lastIndexOf(marker);
  if (markerIdx >= 0) {
    return `/media/${normalized.slice(markerIdx + marker.length)}`;
  }
  if (normalized.startsWith('data/news/media/')) {
    return `/media/${normalized.slice('data/news/media/'.length)}`;
  }
  return '';
}

function mediaElement(item) {
  const relativeUrl = resolveMediaUrl(item);
  if (!relativeUrl) {
    return null;
  }
  const fallbackBase = resolvedApiBase || sameOriginBase;
  const url = relativeUrl.startsWith('http') ? relativeUrl : joinUrl(fallbackBase, relativeUrl);
  if (item.media_type === 'video') {
    const v = document.createElement('video');
    v.src = url;
    v.controls = true;
    v.playsInline = true;
    return v;
  }
  const img = document.createElement('img');
  img.src = url;
  img.loading = 'lazy';
  return img;
}

function normalizeStaticItems(rawPayload) {
  const rows = Array.isArray(rawPayload) ? rawPayload : (Array.isArray(rawPayload?.items) ? rawPayload.items : []);
  return rows
    .filter((row) => !row.publishState || row.publishState === 'published')
    .map((row) => ({
    id: row.id || '',
    author_name: row.author_name || 'BeerMarket',
    published_at: row.published_at || row.date || row.createdAt || '',
    created_at: row.created_at || row.createdAt || '',
    title: row.title || 'Без заголовка',
    text: row.text || '',
    media: Array.isArray(row.media) ? row.media : [],
  }));
}

function render(items) {
  for (const row of items) {
    const node = tpl.content.firstElementChild.cloneNode(true);
        if (row.id) {
      node.dataset.newsId = String(row.id);
    }
    node.querySelector('.card-meta').textContent = `${row.author_name || '—'} • ${row.published_at || row.created_at || ''}`;
    node.querySelector('.card-title').textContent = row.title || 'Без заголовка';
    node.querySelector('.card-text').textContent = row.text || '';
    const carousel = node.querySelector('.carousel');
    for (const media of (row.media || [])) {
      const mediaNode = mediaElement(media);
      if (mediaNode) {
        carousel.append(mediaNode);
      }
    }
    feed.append(node);
  }
}

async function loadStaticNews() {
  const bases = resolvedStaticSource ? [resolvedStaticSource] : (resolvedApiBase ? [resolvedApiBase] : buildApiCandidates());
  const staticCandidates = bases.flatMap((base) => buildStaticCandidates(base));
  for (const candidate of staticCandidates) {
    try {
      const response = await fetch(withCacheBust(candidate), { cache: 'no-store' });
      if (!response.ok) {
        continue;
      }
      const rows = await response.json();
      const items = normalizeStaticItems(rows);
      const page = items.slice(offset, offset + limit);
      render(page);
      offset += limit;
      if (offset >= items.length) {
        loadMoreBtn.style.display = 'none';
      }
      usesStaticFeed = true;
      resolvedStaticSource = candidate;
      return true;
    } catch (error) {
      console.warn('Static feed failed:', candidate, error);
      lastLoadError = `static:${candidate} -> ${error?.message || error}`;
    }
  }
  return false;
}

async function tryLoadFromApiBase(base) {
  const response = await fetch(withCacheBust(joinUrl(base, `api/news?status=published&limit=${limit}&offset=${offset}`)), { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`API returned ${response.status}`);
  }
  const data = await response.json();
  resolvedApiBase = base;
  render(data.items || []);
  offset += limit;
  if (!data.items || data.items.length < limit) {
    loadMoreBtn.style.display = 'none';
  }
  return true;
}

async function loadNews() {
  if (isLoading) {
    return;
  }
  isLoading = true;
  if (syncNowBtn) syncNowBtn.disabled = true;
  statusEl.textContent = 'Обновляем ленту...';
  try {
    if (usesStaticFeed) {
      await loadStaticNews();
      statusEl.textContent = `Новости: ${feed.children.length} (static)`;
      return;
    }

    const apiCandidates = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();
    const apiErrors = [];

    for (const base of apiCandidates) {
      try {
        await tryLoadFromApiBase(base);
        statusEl.textContent = `Новости: ${feed.children.length}`;
        return;
      } catch (error) {
        const reason = error?.message || String(error);
        apiErrors.push(`${base} -> ${reason}`);
        console.warn('API feed failed for base:', base, error);
      }
    }

    const loaded = await loadStaticNews();
    if (!loaded) {
      const detail = [...apiErrors, lastLoadError].filter(Boolean).join('; ');
      throw new Error(`Не удалось загрузить новости ни из API, ни из статического файла. ${detail}`.trim());
    }
    statusEl.textContent = `Новости: ${feed.children.length} (static)`;
  } finally {
    if (syncNowBtn) syncNowBtn.disabled = false;
    isLoading = false;
  }
}

async function fetchLatestNewsId() {
  if (usesStaticFeed) {
    const staticCandidates = resolvedStaticSource
      ? [resolvedStaticSource]
      : (resolvedApiBase ? [resolvedApiBase] : buildApiCandidates()).flatMap((base) => buildStaticCandidates(base));
    for (const candidate of staticCandidates) {
      try {
        const response = await fetch(withCacheBust(candidate), { cache: 'no-store' });
        if (!response.ok) continue;
        const rows = normalizeStaticItems(await response.json());
        return rows[0]?.id || rows[0]?.created_at || rows[0]?.published_at || null;
      } catch (error) {
        console.warn('Latest static news check failed:', candidate, error);
      }
    }
    return null;
  }

  const apiCandidates = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();
  for (const base of apiCandidates) {
    try {
      const response = await fetch(withCacheBust(joinUrl(base, 'api/news?status=published&limit=1&offset=0')), { cache: 'no-store' });
      if (!response.ok) continue;
      const data = await response.json();
      resolvedApiBase = base;
      return data.items?.[0]?.id || null;
    } catch (error) {
      console.warn('Latest API news check failed for base:', base, error);
    }
  }
  return null;
}

async function refreshFeedIfNeeded() {
  const latestId = await fetchLatestNewsId();
  const currentTopId = feed.firstElementChild?.dataset?.newsId || null;
  if (!latestId || latestId === currentTopId) {
    return;
  }
  feed.innerHTML = '';
  offset = 0;
  loadMoreBtn.style.display = '';
  await loadNews();
}

async function forceSyncFeed() {
  cacheBust = Date.now();
  usesStaticFeed = false;
  resolvedApiBase = null;
  resolvedStaticSource = null;
  lastLoadError = '';
  feed.innerHTML = '';
  offset = 0;
  loadMoreBtn.style.display = '';
  statusEl.textContent = 'Принудительная синхронизация...';
  await loadNews();
}

loadMoreBtn.addEventListener('click', loadNews);
syncNowBtn?.addEventListener('click', () => {
  forceSyncFeed().catch((err) => {
    console.error(err);
    statusEl.textContent = 'Ошибка синхронизации. Проверьте API/news.json и сеть телефона.';
  });
});
loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Ошибка загрузки. Проверьте API_BASE, HTTPS и доступность news.json.';
});
setInterval(() => {
  refreshFeedIfNeeded().catch((err) => console.warn('Auto refresh failed:', err));
}, refreshIntervalMs);