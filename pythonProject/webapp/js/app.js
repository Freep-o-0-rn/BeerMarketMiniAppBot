const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const params = new URLSearchParams(location.search);
const explicitApiBase = params.get('api_base');
const projectBase = new URL('../../', window.location.href).href;
const sameOriginBase = `${window.location.origin}/`;
const defaultCloudflareApiBase = 'https://api.freep0rndeveloper.website/';
const feed = document.getElementById('feed');
const statusEl = document.getElementById('status');
const loadMoreBtn = document.getElementById('loadMore');
const syncNowBtn = document.getElementById('syncNow');
const categoryFiltersEl = document.getElementById('categoryFilters');
const tpl = document.getElementById('news-card-template');
const mediaViewer = document.getElementById('mediaViewer');
const viewerImage = document.getElementById('viewerImage');
const viewerZoomInBtn = document.getElementById('viewerZoomIn');
const viewerZoomOutBtn = document.getElementById('viewerZoomOut');
const viewerCloseBtn = document.getElementById('viewerClose');
const viewerOpenSourceLink = document.getElementById('viewerOpenSource');

let offset = 0;
const limit = 6;
const refreshIntervalMs = 30000;
let resolvedApiBase = null;
let isLoading = false;
let cacheBust = Date.now();
let viewerZoom = 1;
let categories = [];
let activeCategory = '';

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

function inferApiBaseFromHost() {
  const { protocol, host, hostname } = window.location;
  if (!hostname) return null;
  const labels = hostname.split('.');
  if (!labels.length) return null;
  if (labels[0] !== 'app') return null;
  labels[0] = 'api';
  const inferredHost = host.replace(hostname, labels.join('.'));
  return normalizeBase(`${protocol}//${inferredHost}/`);
}

function buildApiCandidates() {
  const candidates = [
    normalizeBase(explicitApiBase),
    inferApiBaseFromHost(),
    normalizeBase(defaultCloudflareApiBase),
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
  img.alt = 'Изображение новости';
  img.addEventListener('error', () => {
    img.remove();
  });
  const wrapper = document.createElement('button');
  wrapper.type = 'button';
  wrapper.className = 'media-tile';
  wrapper.append(img);
  wrapper.addEventListener('click', () => openImageViewer(url));
  return wrapper;
}

function applyViewerZoom() {
  if (viewerImage) {
    viewerImage.style.transform = `scale(${viewerZoom})`;
  }
}

function closeImageViewer() {
  if (!mediaViewer || !viewerImage) return;
  mediaViewer.classList.remove('is-open');
  mediaViewer.setAttribute('aria-hidden', 'true');
  viewerImage.removeAttribute('src');
  viewerZoom = 1;
}

function openImageViewer(url) {
  if (!mediaViewer || !viewerImage) return;
  viewerZoom = 1;
  viewerImage.src = url;
  applyViewerZoom();
  mediaViewer.classList.add('is-open');
  mediaViewer.setAttribute('aria-hidden', 'false');
  if (viewerOpenSourceLink) {
    viewerOpenSourceLink.href = url;
  }
}

function render(items) {
  for (const row of items) {
    const node = tpl.content.firstElementChild.cloneNode(true);
    if (row.id) {
      node.dataset.newsId = String(row.id);
    }
    node.querySelector('.card-meta').textContent = `${row.author_name || '—'} • ${row.published_at || row.created_at || ''}`;
    node.querySelector('.card-category').textContent = row.category_label || row.categoryLabel || 'Новости';
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
  const categoryQuery = activeCategory ? `&category=${encodeURIComponent(activeCategory)}` : '';
  const response = await fetch(
    withCacheBust(joinUrl(base, `api/news?status=published&limit=${limit}&offset=${offset}${categoryQuery}`)),
    { cache: 'no-store' },
  );
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
  } catch (error) {
    statusEl.textContent = 'Ошибка загрузки. Проверьте API_BASE, HTTPS и доступность API.';
    throw error;
  } finally {
    isLoading = false;
  }
}

async function fetchLatestNewsId() {
  const apiCandidates = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();
  for (const base of apiCandidates) {
    try {
      const categoryQuery = activeCategory ? `&category=${encodeURIComponent(activeCategory)}` : '';
      const response = await fetch(
        withCacheBust(joinUrl(base, `api/news?status=published&limit=1&offset=0${categoryQuery}`)),
        { cache: 'no-store' },
      );
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

function applyCategoryFilter(nextCategory) {
  activeCategory = nextCategory || '';
  feed.innerHTML = '';
  offset = 0;
  loadMoreBtn.style.display = '';
  renderCategoryFilters();
  loadNews().catch((err) => console.warn('Category load failed:', err));
}

function renderCategoryFilters() {
  if (!categoryFiltersEl) return;
  categoryFiltersEl.innerHTML = '';
  const all = [{ key: '', label: 'Все' }, ...categories];
  for (const item of all) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'category-filter-btn';
    if ((item.key || '') === activeCategory) {
      btn.classList.add('is-active');
    }
    btn.textContent = item.label;
    btn.addEventListener('click', () => applyCategoryFilter(item.key));
    categoryFiltersEl.append(btn);
  }
}

async function loadCategories() {
  const apiCandidates = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();
  for (const base of apiCandidates) {
    try {
      const response = await fetch(withCacheBust(joinUrl(base, 'api/news/categories')), { cache: 'no-store' });
      if (!response.ok) continue;
      const data = await response.json();
      categories = Array.isArray(data.items) ? data.items : [];
      resolvedApiBase = base;
      renderCategoryFilters();
      return;
    } catch (error) {
      console.warn('Category catalog request failed for base:', base, error);
    }
  }
}

loadMoreBtn.addEventListener('click', loadNews);
if (syncNowBtn) {
  syncNowBtn.addEventListener('click', async () => {
    cacheBust = Date.now();
    await refreshFeedIfNeeded();
    statusEl.textContent = `Новости: ${feed.children.length}`;
  });
}
if (viewerZoomInBtn) {
  viewerZoomInBtn.addEventListener('click', () => {
    viewerZoom = Math.min(4, Number((viewerZoom + 0.25).toFixed(2)));
    applyViewerZoom();
  });
}
if (viewerZoomOutBtn) {
  viewerZoomOutBtn.addEventListener('click', () => {
    viewerZoom = Math.max(1, Number((viewerZoom - 0.25).toFixed(2)));
    applyViewerZoom();
  });
}
if (viewerCloseBtn) {
  viewerCloseBtn.addEventListener('click', closeImageViewer);
}
if (mediaViewer) {
  mediaViewer.addEventListener('click', (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && target.hasAttribute('data-close-viewer')) {
      closeImageViewer();
    }
  });
}

loadCategories()
  .catch((err) => console.warn('Categories loading skipped:', err))
  .finally(() => loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Откройте Новости через кнопку на клавиатуре "Открыть Mini App"';
  }));
setInterval(() => {
  refreshFeedIfNeeded().catch((err) => console.warn('Auto refresh failed:', err));
}, refreshIntervalMs);