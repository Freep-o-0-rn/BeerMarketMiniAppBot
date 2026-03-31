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

let offset = 0;
const limit = 6;
let usesStaticFeed = false;
let resolvedApiBase = null;
let lastLoadError = '';

function joinUrl(base, path) {
  return new URL(path.replace(/^\/+/, ''), base.endsWith('/') ? base : `${base}/`).href;
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


function mediaElement(item) {
  const relativeUrl = item.url || '';
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

function normalizeStaticItems(items = []) {
  return items.map((row) => ({
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
    node.querySelector('.card-meta').textContent = `${row.author_name || '—'} • ${row.published_at || row.created_at || ''}`;
    node.querySelector('.card-title').textContent = row.title || 'Без заголовка';
    node.querySelector('.card-text').textContent = row.text || '';
    const carousel = node.querySelector('.carousel');
    for (const media of (row.media || [])) {
      carousel.append(mediaElement(media));
    }
    feed.append(node);
  }
}

async function loadStaticNews() {
  const bases = resolvedApiBase ? [resolvedApiBase] : buildApiCandidates();
  const staticCandidates = bases.flatMap((base) => [joinUrl(base, 'news.json'), joinUrl(base, 'pythonProject/news.json')]);
  for (const candidate of staticCandidates) {
    try {
      const response = await fetch(candidate, { cache: 'no-store' });
      if (!response.ok) {
        continue;
      }
      const rows = await response.json();
      const items = normalizeStaticItems(Array.isArray(rows) ? rows : []);
      const page = items.slice(offset, offset + limit);
      render(page);
      offset += limit;
      if (offset >= items.length) {
        loadMoreBtn.style.display = 'none';
      }
      usesStaticFeed = true;
      return true;
    } catch (error) {
      console.warn('Static feed failed:', candidate, error);
      lastLoadError = `static:${candidate} -> ${error?.message || error}`;
    }
  }
  return false;
}

async function tryLoadFromApiBase(base) {
  const response = await fetch(joinUrl(base, `api/news?status=published&limit=${limit}&offset=${offset}`), { cache: 'no-store' });
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
  statusEl.textContent = 'Обновляем ленту...';
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
}

loadMoreBtn.addEventListener('click', loadNews);
loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Ошибка загрузки. Проверьте API_BASE, HTTPS и доступность news.json.';
});