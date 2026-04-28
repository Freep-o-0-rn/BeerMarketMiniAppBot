from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def resolve_card_for_report_client(
    raw_name: str,
    *,
    report_type: str,
    load_mappings: Callable[[], Dict[str, Any]],
    split_report_client_label: Callable[[str], Dict[str, str]],
    debt_import_mapping_key: Callable[[str, str], str],
    debt_import_client_key: Callable[[str], str],
    get_client_by_id: Callable[[str], Optional[Dict[str, Any]]],
    lookup_by_name: Callable[[str], Optional[Dict[str, Any]]],
    lookup_by_name_and_address: Callable[[str, str], Optional[Dict[str, Any]]],
    normalize_text_key: Callable[[str], str],
) -> Optional[Dict[str, Any]]:
    report_type = (report_type or "").strip().lower()
    raw_name = str(raw_name or "").strip()
    if not raw_name:
        return None

    if report_type == "debt":
        mapping_state = load_mappings()
        mappings = mapping_state.get("mappings") if isinstance(mapping_state.get("mappings"), dict) else {}
        parsed = split_report_client_label(raw_name)
        parsed_rep = str(parsed.get("sales_rep") or "").strip()
        key = debt_import_mapping_key(raw_name, parsed_rep)
        legacy_key = debt_import_client_key(raw_name)
        mapped = mappings.get(key) if key else None
        if not isinstance(mapped, dict) and legacy_key:
            mapped = mappings.get(legacy_key)
        if isinstance(mapped, dict):
            mapped_client_id = str(mapped.get("client_id") or "").strip()
            if mapped_client_id:
                card = get_client_by_id(mapped_client_id)
                if card:
                    return card

    parsed = split_report_client_label(raw_name)
    base_name = normalize_text_key(parsed.get("client_name") or raw_name)
    address = normalize_text_key(parsed.get("address") or "")
    if base_name and address:
        card = lookup_by_name_and_address(base_name, address)
        if card:
            return card
    if base_name:
        card = lookup_by_name(base_name)
        if card:
            return card
    return None


def resolve_sales_rep_name_for_item(
    item: Dict[str, Any],
    *,
    report_type: str,
    resolve_card_for_report_client_fn: Callable[[str, str], Optional[Dict[str, Any]]],
    user_record_getter: Callable[[int], Dict[str, Any]],
    split_report_client_label: Callable[[str], Dict[str, str]],
) -> str:
    card = resolve_card_for_report_client_fn(str(item.get("client") or ""), report_type)
    if card:
        card_rep_name = str(card.get("sales_rep_name") or "").strip()
        if card_rep_name:
            return card_rep_name
        card_rep_uid = card.get("sales_rep_user_id")
        if card_rep_uid:
            rep_uid = int(card_rep_uid)
            rec = user_record_getter(rep_uid)
            resolved = (
                str(rec.get("name") or "").strip()
                or " ".join(
                    [
                        str(rec.get("first_name") or "").strip(),
                        str(rec.get("last_name") or "").strip(),
                    ]
                ).strip()
            )
            if resolved:
                return resolved
    explicit_name = str(item.get("sales_rep_name") or "").strip()
    if explicit_name:
        return explicit_name
    parsed = split_report_client_label(str(item.get("client") or ""))
    parsed_rep = str(parsed.get("sales_rep") or "").strip()
    if parsed_rep:
        return parsed_rep
    return ""


def sync_sales_rep_in_mappings_for_client(
    *,
    client_id: str,
    sales_rep_user_id: Optional[int],
    sales_rep_name: str,
    actor_user_id: int,
    load_mappings: Callable[[], Dict[str, Any]],
    save_mappings: Callable[[Dict[str, Any]], None],
    utc_now_iso: Callable[[], str],
) -> int:
    cid = str(client_id or "").strip()
    if not cid:
        return 0
    state = load_mappings()
    mappings = state.get("mappings") if isinstance(state.get("mappings"), dict) else {}
    if not mappings:
        return 0
    now = utc_now_iso()
    changed = 0
    for key, payload in mappings.items():
        if not isinstance(payload, dict):
            continue
        if str(payload.get("client_id") or "").strip() != cid:
            continue
        payload["sales_rep_user_id"] = int(sales_rep_user_id) if sales_rep_user_id else None
        payload["sales_rep_name"] = (sales_rep_name or "").strip()
        payload["updated_at"] = now
        payload["updated_by_user_id"] = int(actor_user_id)
        mappings[key] = payload
        changed += 1
    if changed:
        save_mappings(state)
    return changed
