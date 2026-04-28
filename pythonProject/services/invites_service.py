from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from services.time import local_now, local_now_iso, parse_mixed_datetime

INVITE_ROLE_OPTIONS: List[Tuple[str, str]] = [
    ("client", "👤 Клиент"),
    ("sales_rep", "🧑‍💼 Торговый"),
    ("moderator", "🛡 Модератор"),
    ("admin", "👑 Админ"),
]
INVITE_TTL_OPTIONS: List[Tuple[str, str, Optional[int]]] = [
    ("30m", "30 минут", 30 * 60),
    ("1h", "1 час", 60 * 60),
    ("6h", "6 часов", 6 * 60 * 60),
    ("1d", "1 день", 24 * 60 * 60),
    ("7d", "7 дней", 7 * 24 * 60 * 60),
    ("30d", "30 дней", 30 * 24 * 60 * 60),
    ("inf", "Бессрочно", None),
]
INVITE_MAX_USES_OPTIONS: List[int] = [1, 5, 10, 50, 100]
INVITE_TTL_MAP: Dict[str, Optional[int]] = {key: seconds for key, _, seconds in INVITE_TTL_OPTIONS}
INVITE_TTL_LABELS: Dict[str, str] = {key: label for key, label, _ in INVITE_TTL_OPTIONS}
INVITE_ROLE_LABELS: Dict[str, str] = {role: label for role, label in INVITE_ROLE_OPTIONS}


@dataclass
class InviteRedeemResult:
    ok: bool
    reason: str
    invite: Optional[Dict[str, Any]] = None


class InviteService:
    def __init__(self, invites_path: Path):
        self.invites_path = Path(invites_path)

    @staticmethod
    def utc_now() -> datetime:
        return local_now()

    @staticmethod
    def parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
        return parse_mixed_datetime(value)

    def load(self) -> List[Dict[str, Any]]:
        if not self.invites_path.exists():
            return []
        try:
            data = json.loads(self.invites_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []

    def save_atomic(self, items: List[Dict[str, Any]]) -> None:
        self.invites_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.invites_path.with_suffix(self.invites_path.suffix + ".tmp")
        tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.invites_path)

    def is_expired(self, invite: Dict[str, Any], now_utc: Optional[datetime] = None) -> bool:
        now = now_utc or self.utc_now()
        exp = self.parse_iso_utc(invite.get("expires_at"))
        return bool(exp and now >= exp)

    @staticmethod
    def is_exhausted(invite: Dict[str, Any]) -> bool:
        max_uses = int(invite.get("max_uses") or 0)
        uses_count = int(invite.get("uses_count") or 0)
        return uses_count >= max_uses

    def is_active(self, invite: Dict[str, Any], now_utc: Optional[datetime] = None) -> bool:
        if str(invite.get("status") or "active") != "active":
            return False
        if self.is_expired(invite, now_utc):
            return False
        return not self.is_exhausted(invite)

    def refresh_archive_state(self, items: List[Dict[str, Any]]) -> bool:
        changed = False
        now = self.utc_now()
        for invite in items:
            if str(invite.get("status") or "active") != "active":
                continue
            if self.is_active(invite, now):
                continue
            invite["status"] = "archived"
            invite["archived_at"] = local_now_iso()
            invite["archive_reason"] = "expired" if self.is_expired(invite, now) else "exhausted"
            changed = True
        return changed

    @staticmethod
    def build_payload() -> str:
        return "iv_" + uuid.uuid4().hex[:16]

    def create_invite(
        self,
        *,
        created_by: Any,
        role: str,
        ttl_key: str,
        max_uses: int,
        target_name: str,
        deep_link: str,
        short_url: str,
    ) -> Dict[str, Any]:
        created_at = self.utc_now().replace(microsecond=0)
        ttl_seconds = INVITE_TTL_MAP.get(ttl_key)
        expires_at = (created_at + timedelta(seconds=ttl_seconds)).isoformat() if ttl_seconds else None
        invite = {
            "id": uuid.uuid4().hex,
            "code": self.build_payload(),
            "status": "active",
            "created_at": created_at.isoformat(),
            "created_by": created_by,
            "role": role,
            "ttl_key": ttl_key,
            "expires_at": expires_at,
            "max_uses": max_uses,
            "uses_count": 0,
            "uses": [],
            "target_name": (target_name or "").strip(),
            "deep_link": deep_link,
            "short_url": short_url,
        }
        return invite

    def append_invite(self, invite: Dict[str, Any]) -> None:
        items = self.load()
        self.refresh_archive_state(items)
        items.append(invite)
        self.save_atomic(items)

    def list_invites(self, mode: str) -> List[Dict[str, Any]]:
        mode = mode if mode in {"active", "archive"} else "active"
        items = self.load()
        if self.refresh_archive_state(items):
            self.save_atomic(items)
        status = "active" if mode == "active" else "archived"
        out = [x for x in items if str(x.get("status") or "active") == status]
        fallback_dt = datetime.min.replace(tzinfo=self.utc_now().tzinfo)
        out.sort(
            key=lambda x: self.parse_iso_utc(str(x.get("created_at") or "")) or fallback_dt,
            reverse=True,
        )
        return out

    def get_invite(self, invite_id: str) -> Optional[Dict[str, Any]]:
        items = self.load()
        if self.refresh_archive_state(items):
            self.save_atomic(items)
        return next((x for x in items if str(x.get("id") or "") == invite_id), None)

    def archive_invite(self, invite_id: str, reason: str = "manual") -> bool:
        items = self.load()
        invite = next((x for x in items if str(x.get("id") or "") == invite_id), None)
        if not invite:
            return False
        invite["status"] = "archived"
        invite["archived_at"] = local_now_iso()
        invite["archive_reason"] = reason
        self.save_atomic(items)
        return True

    def redeem(self, code: str, user_id: int, display_name: str) -> InviteRedeemResult:
        items = self.load()
        changed = self.refresh_archive_state(items)
        invite = next((x for x in items if str(x.get("code") or "") == code), None)
        if not invite:
            if changed:
                self.save_atomic(items)
            return InviteRedeemResult(ok=False, reason="not_found")
        if not self.is_active(invite):
            invite["status"] = "archived"
            invite["archived_at"] = local_now_iso()
            invite["archive_reason"] = "expired" if self.is_expired(invite) else "exhausted"
            self.save_atomic(items)
            return InviteRedeemResult(ok=False, reason="inactive", invite=invite)

        now = local_now_iso()
        invite["uses_count"] = int(invite.get("uses_count") or 0) + 1
        uses = invite.get("uses") if isinstance(invite.get("uses"), list) else []
        uses.append({"user_id": user_id, "used_at": now, "display_name": (display_name or "").strip()})
        invite["uses"] = uses
        if self.is_exhausted(invite):
            invite["status"] = "archived"
            invite["archived_at"] = now
            invite["archive_reason"] = "exhausted"
        self.save_atomic(items)
        return InviteRedeemResult(ok=True, reason="ok", invite=invite)
