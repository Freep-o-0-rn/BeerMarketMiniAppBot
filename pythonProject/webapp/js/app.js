const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const params = new URLSearchParams(location.search);
const API_BASE = params.get('api_base') || 'https://freep-o-0-rn.github.io/BeerMarketMiniAppBot/';
const feed = document.getElementById('feed');
const statusEl = document.getElementById('status');
const loadMoreBtn = document.getElementById('loadMore');
const tpl = document.getElementById('news-card-template');

let offset = 0;
const limit = 6;

function mediaElement(item) {
  const url = `${API_BASE}${item.url || ''}`;
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

async function loadNews() {
  statusEl.textContent = 'Обновляем ленту...';
  const response = await fetch(`${API_BASE}/api/news?status=published&limit=${limit}&offset=${offset}`);
  const data = await response.json();
  render(data.items || []);
  offset += limit;
  if (!data.items || data.items.length < limit) {
    loadMoreBtn.style.display = 'none';
  }
  statusEl.textContent = `Новости: ${feed.children.length}`;
}

loadMoreBtn.addEventListener('click', loadNews);
loadNews().catch((err) => {
  console.error(err);
  statusEl.textContent = 'Ошибка загрузки. Проверьте API и HTTPS.';
});
