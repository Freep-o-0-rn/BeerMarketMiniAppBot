from __future__ import annotations

import json
import secrets
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class UserActionView:
    index: int
    action_type: str
    title: str
    preview: str
    details: str


class UserActivityService:
    """
    Готовит читабельную витрину действий из audit.log без тяжёлых запросов к Telegram API.
    Лог читается ограниченно (tail по строкам), а подробности выдаются по требованию.
    """

    def __init__(self, audit_log_path: Path, *, max_tail_lines: int = 4000, snapshot_ttl_sec: int = 300) -> None:
        self._audit_log_path = Path(audit_log_path)
        self._max_tail_lines = max(200, int(max_tail_lines))
        self._snapshot_ttl_sec = max(60, int(snapshot_ttl_sec))
        self._snapshots: Dict[str, Dict[str, Any]] = {}

    def _cleanup(self) -> None:
        now = time.time()
        expired = [
            token
            for token, payload in self._snapshots.items()
            if now - float(payload.get("created_at", 0)) > self._snapshot_ttl_sec
        ]
        for token in expired:
            self._snapshots.pop(token, None)

    def create_snapshot(self, *, limit_items: int = 150) -> str:
        self._cleanup()
        rows = self._load_recent_rows(limit_items=limit_items)
        token = secrets.token_hex(6)
        self._snapshots[token] = {
            "created_at": time.time(),
            "rows": rows,
        }
        return token

    def get_rows(self, token: str) -> List[UserActionView]:
        payload = self._snapshots.get(token) or {}
        rows = payload.get("rows")
        return rows if isinstance(rows, list) else []

    def get_row(self, token: str, index: int) -> Optional[UserActionView]:
        rows = self.get_rows(token)
        if index < 0 or index >= len(rows):
            return None
        return rows[index]

    def _load_recent_rows(self, *, limit_items: int) -> List[UserActionView]:
        records = self._tail_json_lines(self._max_tail_lines)
        rows: List[UserActionView] = []
        for rec in reversed(records):
            row = self._to_row(rec)
            if row is None:
                continue
            row_with_index = UserActionView(
                index=len(rows),
                action_type=row.action_type,
                title=row.title,
                preview=row.preview,
                details=row.details,
            )
            rows.append(row_with_index)
            if len(rows) >= limit_items:
                break
        return rows

    def _tail_json_lines(self, max_lines: int) -> List[Dict[str, Any]]:
        path = self._audit_log_path
        if not path.exists() or not path.is_file():
            return []
        try:
            with path.open("r", encoding="utf-8") as f:
                lines = list(deque(f, maxlen=max_lines))
        except OSError:
            return []
        items: List[Dict[str, Any]] = []
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items.append(payload)
        return items

    def _to_row(self, rec: Dict[str, Any]) -> Optional[UserActionView]:
        kind = str(rec.get("t") or "").strip().lower()
        if kind not in {"msg", "cb", "event"}:
            return None
        uid = rec.get("uid")
        role = rec.get("role") or "unknown"
        ts = self._fmt_ts(rec.get("ts"))
        if kind == "msg":
            text = self._normalize(rec.get("text"))
            media = rec.get("kind") or "text"
            title = f"✉️ Сообщение · uid={uid}"
            preview = f"{ts} · {media} · {self._clip(text, 90)}"
            details = (
                f"<b>Действие:</b> сообщение пользователя\n"
                f"<b>Пользователь:</b> <code>{uid}</code>\n"
                f"<b>Роль:</b> {role}\n"
                f"<b>Время:</b> {ts}\n"
                f"<b>Тип:</b> {self._escape(str(media))}\n"
                f"<b>Текст:</b> <code>{self._escape(text or '—')}</code>\n"
                f"<b>FSM state:</b> <code>{self._escape(str(rec.get('state') or '—'))}</code>"
            )
            return UserActionView(index=0, action_type=kind, title=title, preview=preview, details=details)
        if kind == "cb":
            data = self._normalize(rec.get("data"))
            title = f"🖱 Нажатие кнопки · uid={uid}"
            preview = f"{ts} · {self._clip(data, 90)}"
            details = (
                f"<b>Действие:</b> callback-кнопка\n"
                f"<b>Пользователь:</b> <code>{uid}</code>\n"
                f"<b>Роль:</b> {role}\n"
                f"<b>Время:</b> {ts}\n"
                f"<b>Callback data:</b> <code>{self._escape(data or '—')}</code>\n"
                f"<b>FSM state:</b> <code>{self._escape(str(rec.get('state') or '—'))}</code>"
            )
            return UserActionView(index=0, action_type=kind, title=title, preview=preview, details=details)

        action = self._normalize(rec.get("action"))
        title = f"📌 Системное событие · uid={uid}"
        preview = f"{ts} · {self._clip(action, 90)}"
        details = (
            f"<b>Действие:</b> системное событие\n"
            f"<b>Пользователь:</b> <code>{uid}</code>\n"
            f"<b>Время:</b> {ts}\n"
            f"<b>Событие:</b> <code>{self._escape(action or '—')}</code>"
        )
        return UserActionView(index=0, action_type=kind, title=title, preview=preview, details=details)

    @staticmethod
    def _clip(value: str, max_len: int) -> str:
        txt = value or ""
        if len(txt) <= max_len:
            return txt
        return txt[: max_len - 1] + "…"

    @staticmethod
    def _normalize(value: Any) -> str:
        if value is None:
            return ""
        txt = str(value).replace("\n", " ").replace("\r", " ")
        return " ".join(txt.split())

    @staticmethod
    def _fmt_ts(value: Any) -> str:
        if not value:
            return "время не указано"
        txt = str(value)
        try:
            parsed = datetime.fromisoformat(txt)
            return parsed.strftime("%d.%m.%Y %H:%M:%S")
        except ValueError:
            return txt

    @staticmethod
    def _escape(value: str) -> str:
        return (
            (value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
