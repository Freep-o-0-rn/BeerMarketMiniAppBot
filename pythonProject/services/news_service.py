from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class NewsItem:
    id: str
    title: str
    text: str
    author_id: int
    author_name: str
    status: str
    created_at: str
    updated_at: str
    published_at: Optional[str]
    display_order: int
    category: Optional[str] = None
    is_pinned: int = 0
    buttons_json: str = "[]"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "text": self.text,
            "author_id": self.author_id,
            "author_name": self.author_name,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "published_at": self.published_at,
            "display_order": self.display_order,
            "category": self.category,
            "is_pinned": bool(self.is_pinned),
            "buttons": json.loads(self.buttons_json or "[]"),
        }


class NewsService:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    text TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    author_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    published_at TEXT,
                    display_order INTEGER NOT NULL DEFAULT 0,
                    category TEXT,
                    is_pinned INTEGER NOT NULL DEFAULT 0,
                    buttons_json TEXT NOT NULL DEFAULT '[]'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news_media (
                    id TEXT PRIMARY KEY,
                    news_id TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    mime_type TEXT,
                    created_at TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(news_id) REFERENCES news(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_status ON news(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_news ON news_media(news_id, sort_order)")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def create_news(self, title: str, text: str, author_id: int, author_name: str, *, status: str = "draft") -> str:
        now = self._now_iso()
        news_id = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO news (id, title, text, author_id, author_name, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (news_id, title.strip(), text.strip(), author_id, author_name.strip() or "Unknown", status, now, now),
            )
        return news_id

    def update_news(self, news_id: str, **patch: Any) -> None:
        allowed = {
            "title", "text", "author_name", "status", "published_at", "display_order", "category", "is_pinned", "buttons_json"
        }
        pairs = [(k, v) for k, v in patch.items() if k in allowed]
        if not pairs:
            return
        pairs.append(("updated_at", self._now_iso()))
        set_sql = ", ".join([f"{k} = ?" for k, _ in pairs])
        values = [v for _, v in pairs] + [news_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE news SET {set_sql} WHERE id = ?", values)

    def set_status(self, news_id: str, status: str, *, published_at: Optional[str] = None) -> None:
        if status == "published":
            self.update_news(news_id, status=status, published_at=published_at or self._now_iso())
        else:
            self.update_news(news_id, status=status)

    def delete_news(self, news_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM news_media WHERE news_id = ?", (news_id,))
            conn.execute("DELETE FROM news WHERE id = ?", (news_id,))

    def add_media(self, news_id: str, media_type: str, file_path: str, mime_type: Optional[str] = None) -> str:
        media_id = uuid.uuid4().hex
        now = self._now_iso()
        with self._connect() as conn:
            row = conn.execute("SELECT COALESCE(MAX(sort_order), -1) + 1 as next_order FROM news_media WHERE news_id = ?", (news_id,)).fetchone()
            sort_order = int(row["next_order"] if row else 0)
            conn.execute(
                """
                INSERT INTO news_media (id, news_id, media_type, file_path, mime_type, created_at, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (media_id, news_id, media_type, file_path, mime_type, now, sort_order),
            )
        return media_id

    def get_news(self, news_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM news WHERE id = ?", (news_id,)).fetchone()
            if not row:
                return None
            item = dict(row)
            item["media"] = [dict(m) for m in conn.execute("SELECT * FROM news_media WHERE news_id = ? ORDER BY sort_order ASC", (news_id,)).fetchall()]
            item["buttons"] = json.loads(item.get("buttons_json") or "[]")
            item["is_pinned"] = bool(item.get("is_pinned"))
            return item

    def list_news(self, *, status: Optional[str] = None, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        query = "SELECT * FROM news"
        params: List[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY is_pinned DESC, COALESCE(published_at, created_at) DESC, display_order DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["media"] = [dict(m) for m in conn.execute(
                    "SELECT * FROM news_media WHERE news_id = ? ORDER BY sort_order ASC LIMIT 5", (item["id"],)
                ).fetchall()]
                item["buttons"] = json.loads(item.get("buttons_json") or "[]")
                item["is_pinned"] = bool(item.get("is_pinned"))
                result.append(item)
            return result
