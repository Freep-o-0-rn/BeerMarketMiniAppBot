from __future__ import annotations

import json
import secrets
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from time_utils import format_rf_novosibirsk


@dataclass(frozen=True)
class UserActionView:
    index: int
    action_type: str
    title: str
    preview: str
    details: str


@dataclass(frozen=True)
class UserActivityUserView:
    uid: str
    title: str
    preview: str
    total_actions: int


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
        users_order, users_map = self._load_recent_rows(limit_items=limit_items)
        token = secrets.token_hex(6)
        self._snapshots[token] = {
            "created_at": time.time(),
            "users_order": users_order,
            "users_map": users_map,
        }
        return token

    def get_users(self, token: str) -> List[UserActivityUserView]:
        payload = self._snapshots.get(token) or {}
        users_order = payload.get("users_order")
        users_map = payload.get("users_map")
        if not isinstance(users_order, list) or not isinstance(users_map, dict):
            return []
        summaries: List[UserActivityUserView] = []
        for uid in users_order:
            bucket = users_map.get(uid)
            if not isinstance(bucket, dict):
                continue
            summary = bucket.get("summary")
            if isinstance(summary, UserActivityUserView):
                summaries.append(summary)
        return summaries

    def get_user_rows(self, token: str, uid: str) -> List[UserActionView]:
        bucket = self._find_user_bucket(token, uid)
        if bucket is None:
            return []
        rows = bucket.get("rows")
        return rows if isinstance(rows, list) else []

    def get_user_row(self, token: str, uid: str, index: int) -> Optional[UserActionView]:
        rows = self.get_user_rows(token, uid)
        if index < 0 or index >= len(rows):
            return None
        return rows[index]

    def _find_user_bucket(self, token: str, uid: str) -> Optional[Dict[str, Any]]:
        payload = self._snapshots.get(token) or {}
        users_map = payload.get("users_map")
        if not isinstance(users_map, dict):
            return None
        bucket = users_map.get(str(uid))
        return bucket if isinstance(bucket, dict) else None

    def _load_recent_rows(self, *, limit_items: int) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        records = self._tail_json_lines(self._max_tail_lines)
        users_map: Dict[str, Dict[str, Any]] = {}
        users_order: List[str] = []
        total_rows = 0
        for rec in reversed(records):
            row = self._to_row(rec)
            if row is None:
                continue

            uid = self._normalize_uid(rec.get("uid"))
            bucket = users_map.get(uid)
            if bucket is None:
                role = str(rec.get("role") or "unknown")
                display_name = self._normalize(rec.get("name") or rec.get("user"))
                summary = UserActivityUserView(
                    uid=uid,
                    title=f"👤 {display_name or uid}",
                    preview=f"uid={uid} · роль={role}",
                    total_actions=0,
                )
                bucket = {
                    "summary": summary,
                    "rows": [],
                }
                users_map[uid] = bucket
                users_order.append(uid)

            user_rows = bucket["rows"]
            row_with_index = UserActionView(
                index=len(user_rows),
                action_type=row.action_type,
                title=row.title,
                preview=row.preview,
                details=row.details,
            )
            user_rows.append(row_with_index)

            summary: UserActivityUserView = bucket["summary"]
            bucket["summary"] = UserActivityUserView(
                uid=summary.uid,
                title=summary.title,
                preview=row.preview,
                total_actions=summary.total_actions + 1,
            )

            total_rows += 1
            if total_rows >= limit_items:
                break

        return users_order, users_map

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
        uid = self._normalize_uid(rec.get("uid"))
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
                f"<b>Тип:</b> <code>{self._escape(str(media))}</code>\n"
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
    def _normalize_uid(value: Any) -> str:
        uid = str(value or "unknown").strip()
        return uid or "unknown"

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
        return format_rf_novosibirsk(str(value) if value is not None else None)

    @staticmethod
    def _escape(value: str) -> str:
        return (
            (value or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )