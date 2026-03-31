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
let resolvedApiBase = null;
let isLoading = false;
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
    const apiCandidates = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();

    for (const base of apiCandidates) {
      try {
        await tryLoadFromApiBase(base);
        statusEl.textContent = `Новости: ${feed.children.length}`;
        return;
      } catch (error) {
        console.warn('API feed failed for base:', base, error);
      }
    }
    throw new Error('Не удалось загрузить новости из API. Проверьте API_BASE и доступность /api/news.');
  } finally {
    if (syncNowBtn) syncNowBtn.disabled = false;
    isLoading = false;
  }
}

async function fetchLatestNewsId() {
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
  resolvedApiBase = null;
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
    statusEl.textContent = 'Ошибка синхронизации. Проверьте API и сеть телефона.';
  });
});
loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Ошибка загрузки. Проверьте API_BASE, HTTPS и доступность API.';
});
setInterval(() => {
  refreshFeedIfNeeded().catch((err) => console.warn('Auto refresh failed:', err));
}, refreshIntervalMs);