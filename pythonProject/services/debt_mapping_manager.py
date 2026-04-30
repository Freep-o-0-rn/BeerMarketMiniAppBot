from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional


def normalize_source(value: str) -> str:
    val = (value or "debt").strip().lower()
    return val if val in {"debt", "tara"} else "debt"


def sorted_mapping_entries(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    mappings = state.get("mappings") if isinstance(state.get("mappings"), dict) else {}
    out: List[Dict[str, Any]] = []
    for key, payload in mappings.items():
        rec = payload if isinstance(payload, dict) else {}
        out.append({
            "key": str(key),
            "raw_client_name": str(rec.get("raw_client_name") or ""),
            "client_id": str(rec.get("client_id") or ""),
            "sales_rep_name": str(rec.get("sales_rep_name") or ""),
            "sales_rep_user_id": rec.get("sales_rep_user_id"),
            "source": normalize_source(str(rec.get("source") or "debt")),
            "updated_at": str(rec.get("updated_at") or ""),
        })
    out.sort(key=lambda x: (x.get("updated_at") or "", x.get("raw_client_name") or ""), reverse=True)
    return out

def filter_mapping_entries(
        entries: Iterable[Dict[str, Any]],
        *,
        source: str = "all",
        sales_rep: str = "",
        search_query: str = "",
        without_client_id: bool = False,
        stale_days: int = 0,
        now_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    result = list(entries)
    if source in {"debt", "tara"}:
        result = [x for x in result if str(x.get("source") or "") == source]
    if sales_rep:
        needle = sales_rep.casefold().strip()
        result = [x for x in result if needle in str(x.get("sales_rep_name") or "").casefold()]
    if search_query:
        needle = search_query.casefold().strip()
        result = [
            x for x in result
            if needle in str(x.get("raw_client_name") or "").casefold()
            or needle in str(x.get("sales_rep_name") or "").casefold()
            or needle in str(x.get("client_id") or "").casefold()
            or needle in str(x.get("key") or "").casefold()
        ]
    if without_client_id:
        result = [x for x in result if not str(x.get("client_id") or "").strip()]
    if stale_days > 0:
        now = now_utc or datetime.now(timezone.utc)
        cutoff = now - timedelta(days=stale_days)
        stale: List[Dict[str, Any]] = []
        for x in result:
            text = str(x.get("updated_at") or "")
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00")) if text else datetime(1970, 1, 1, tzinfo=timezone.utc)
            except Exception:
                dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt <= cutoff:
                stale.append(x)
        result = stale
    return result


def bulk_apply(state: Dict[str, Any], keys: Iterable[str], *, action: str, actor_user_id: int, now_iso: Callable[[], str], enqueue_review: Callable[[str, Dict[str, Any], List[str], int, str], None]) -> int:
    mappings = state.get("mappings") if isinstance(state.get("mappings"), dict) else {}
    ignored = state.get("ignored") if isinstance(state.get("ignored"), dict) else {}
    count = 0
    for key in keys:
        payload = mappings.get(key)
        if not isinstance(payload, dict):
            continue
        count += 1
        if action == "drop":
            mappings.pop(key, None)
        elif action == "ignore":
            mappings.pop(key, None)
            ignored[key] = {
                "raw_client_name": str(payload.get("raw_client_name") or ""),
                "source": normalize_source(str(payload.get("source") or "debt")),
                "reason": "bulk_ignore_mapping_manager",
                "updated_at": now_iso(),
                "updated_by_user_id": actor_user_id,
            }
        elif action == "remod":
            enqueue_review(
                str(payload.get("raw_client_name") or ""),
                {"sales_rep_name": str(payload.get("sales_rep_name") or "")},
                ["bulk_manual_remoderation"],
                actor_user_id,
                str(payload.get("source") or "debt"),
            )
    return count


def validate_mappings(state: Dict[str, Any], *, get_client: Callable[[str], Optional[Dict[str, Any]]], enqueue_review: Callable[[str, Dict[str, Any], List[str], int, str], None], now_iso: Callable[[], str], actor_user_id: int = 0) -> Dict[str, int]:
    mappings = state.get("mappings") if isinstance(state.get("mappings"), dict) else {}
    checked = 0
    errors = 0
    queued = 0
    for payload in list(mappings.values()):
        if not isinstance(payload, dict):
            continue
        checked += 1
        cid = str(payload.get("client_id") or "").strip()
        if not cid:
            continue
        card = get_client(cid)
        if not card:
            errors += 1
            payload["client_id"] = None
            payload["updated_at"] = now_iso()
            enqueue_review(str(payload.get("raw_client_name") or ""), {"sales_rep_name": str(payload.get("sales_rep_name") or "")}, ["mapping_validation_client_not_found"], actor_user_id, str(payload.get("source") or "debt"))
            queued += 1
            continue
        mapped_uid = int(payload.get("sales_rep_user_id") or 0)
        card_uid = int(card.get("sales_rep_user_id") or 0)
        if mapped_uid and card_uid and mapped_uid != card_uid:
            errors += 1
            payload["client_id"] = None
            payload["updated_at"] = now_iso()
            enqueue_review(str(payload.get("raw_client_name") or ""), {"sales_rep_name": str(payload.get("sales_rep_name") or "")}, ["mapping_validation_sales_rep_changed"], actor_user_id, str(payload.get("source") or "debt"))
            queued += 1
    return {"checked": checked, "errors": errors, "queued": queued}
