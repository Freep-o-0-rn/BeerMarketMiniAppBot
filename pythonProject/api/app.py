from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

# Ensure package imports work regardless of launch directory.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pythonProject.services.news_service import NewsService

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "news"
MEDIA_DIR = DATA_DIR / "media"
DB_PATH = DATA_DIR / "news.db"
NEWS = NewsService(DB_PATH)


def _normalize_origin(origin: str) -> str | None:
    if not origin:
        return None
    try:
        parsed = urlsplit(origin)
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}".lower()

def _allowed_origins() -> set[str]:
    raw = (os.getenv("CORS_ALLOW_ORIGINS") or "").strip()
    if raw:
        items = [item.strip() for item in raw.split(",") if item.strip()]
    else:
        items = [
            "https://app.freep0rndeveloper.website",
            "http://localhost:8090",
            "http://127.0.0.1:8090",
        ]
    normalized = set()
    for item in items:
        value = _normalize_origin(item)
        if value:
            normalized.add(value)
    if "*" in items:
        normalized.add("*")
    return normalized

_ALLOWED_ORIGINS = _allowed_origins()
_CORS_STRICT = bool((os.getenv("CORS_ALLOW_ORIGINS") or "").strip())


def _resolve_allow_origin(origin: str | None) -> str | None:
    normalized = _normalize_origin(origin or "")
    if not normalized:
        return "*"
    parsed = urlsplit(normalized)
    if parsed.hostname in {"localhost", "127.0.0.1"}:
        return normalized
    if not _CORS_STRICT and parsed.scheme == "https":
        # По умолчанию разрешаем любой HTTPS origin для read-only Mini App API.
        # Это устраняет падения при запуске Mini App из разных точек Telegram,
        # где origin может отличаться от статически прописанного app-домена.
        return normalized
    if "*" in _ALLOWED_ORIGINS:
        return normalized
    if normalized in _ALLOWED_ORIGINS:
        return normalized
    return None

@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin")
    allow_origin = _resolve_allow_origin(origin)
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        response = await handler(request)
    if allow_origin:
        response.headers["Access-Control-Allow-Origin"] = allow_origin
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
    response.headers["Access-Control-Max-Age"] = "86400"
    return response


def _build_media_url(file_path: str) -> str:
    raw_path = Path(file_path)
    try:
        rel_path = raw_path.resolve().relative_to(MEDIA_DIR.resolve())
        return f"/media/{rel_path.as_posix()}"
    except Exception:
        normalized = str(file_path).replace("\\", "/")
        marker = "/data/news/media/"
        idx = normalized.rfind(marker)
        if idx >= 0:
            return f"/media/{normalized[idx + len(marker):]}"
        if normalized.startswith("data/news/media/"):
            return f"/media/{normalized[len('data/news/media/'):]}"
        return ""


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def list_news(request: web.Request) -> web.Response:
    status = request.query.get("status", "published")
    limit = int(request.query.get("limit", "20"))
    offset = int(request.query.get("offset", "0"))
    rows = NEWS.list_news(status=status, limit=max(1, min(limit, 100)), offset=max(0, offset))
    for row in rows:
        for media in row.get("media") or []:
            media["url"] = _build_media_url(media.get("file_path", ""))
    return web.json_response({"items": rows, "count": len(rows)})


async def get_news(request: web.Request) -> web.Response:
    news_id = request.match_info["news_id"]
    row = NEWS.get_news(news_id)
    if not row:
        raise web.HTTPNotFound(text="News not found")
    for media in row.get("media") or []:
        media["url"] = _build_media_url(media.get("file_path", ""))
    return web.json_response(row)


def build_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.add_routes([
        web.get("/health", health),
        web.get("/healthz", health),
        web.get("/api/news", list_news),
        web.get("/api/news/{news_id}", get_news),
    ])
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    app.router.add_static("/media/", path=str(MEDIA_DIR), show_index=False)
    return app


if __name__ == "__main__":
    port = int(os.getenv("NEWS_API_PORT", "8091"))
    host = os.getenv("NEWS_API_HOST", "127.0.0.1")
    web.run_app(build_app(), host=host, port=port)
