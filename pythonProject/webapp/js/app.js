const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const params = new URLSearchParams(location.search);
const explicitApiBase = params.get('api_base');
const projectBase = new URL('../../', window.location.href).href;
const API_BASE = explicitApiBase ? new URL(explicitApiBase, window.location.href).href : projectBase;
const feed = document.getElementById('feed');
const statusEl = document.getElementById('status');
const loadMoreBtn = document.getElementById('loadMore');
const tpl = document.getElementById('news-card-template');

let offset = 0;
const limit = 6;
let usesStaticFeed = false;

function joinUrl(base, path) {
  return new URL(path.replace(/^\/+/, ''), base.endsWith('/') ? base : `${base}/`).href;
}

function mediaElement(item) {
  const relativeUrl = item.url || '';
  const url = relativeUrl.startsWith('http') ? relativeUrl : joinUrl(API_BASE, relativeUrl);
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
  const staticCandidates = [joinUrl(API_BASE, 'news.json'), joinUrl(API_BASE, 'pythonProject/news.json')];
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
    }
  }
  return false;
}

async function loadNews() {
  statusEl.textContent = 'Обновляем ленту...';
  if (usesStaticFeed) {
    await loadStaticNews();
    statusEl.textContent = `Новости: ${feed.children.length} (static)`;
    return;
  }

  try {
    const response = await fetch(joinUrl(API_BASE, `api/news?status=published&limit=${limit}&offset=${offset}`), { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`API returned ${response.status}`);
    }
    const data = await response.json();
    render(data.items || []);
    offset += limit;
    if (!data.items || data.items.length < limit) {
      loadMoreBtn.style.display = 'none';
    }
    statusEl.textContent = `Новости: ${feed.children.length}`;
    return;
  } catch (error) {
    console.warn('API feed failed, trying static fallback:', error);
  }

  const loaded = await loadStaticNews();
  if (!loaded) {
    throw new Error('Не удалось загрузить новости ни из API, ни из статического файла.');
  }
  statusEl.textContent = `Новости: ${feed.children.length} (static)`;
}

loadMoreBtn.addEventListener('click', loadNews);
loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Ошибка загрузки. Проверьте API и HTTPS.';
});
