from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.news_service import NewsService

DATA_DIR = ROOT / "data" / "news"
MEDIA_DIR = DATA_DIR / "media"
DB_PATH = DATA_DIR / "news.db"
NEWS = NewsService(DB_PATH)


def _is_allowed_origin(origin: str | None) -> bool:
    if not origin:
        return False
    try:
        parsed = urlsplit(origin)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin")
    allow_origin = origin if _is_allowed_origin(origin) else "*"
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = allow_origin
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
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
        web.get("/api/news", list_news),
        web.get("/api/news/{news_id}", get_news),
    ])
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    app.router.add_static("/media/", path=str(MEDIA_DIR), show_index=False)
    return app


if __name__ == "__main__":
    port = int(os.getenv("NEWS_API_PORT", "8091"))
    web.run_app(build_app(), host="127.0.0.1", port=port)
