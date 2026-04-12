from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from client_cards_db import ClientCardsDB
from file_processor import find_latest_download, parse_clients, read_debt_file


def _norm_text(value: str) -> str:
    return " ".join((value or "").strip().lower().replace("ё", "е").split())

def _surname(value: str) -> str:
    norm = _norm_text(value)
    return norm.split()[0] if norm else ""


@dataclass
class IdentityMatchResult:
    matched: bool
    confidence: float
    client_ref: str
    reason: str
    score: int
    risk_flags: List[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "matched": self.matched,
            "confidence": self.confidence,
            "client_ref": self.client_ref,
            "reason": self.reason,
            "score": self.score,
            "risk_flags": list(self.risk_flags),
        }


class IdentityMatcher:
    """Сопоставление организации пользователя с импортной клиентской базой."""

    def __init__(self, download_dir: str = "downloads", client_cards_db: Optional[ClientCardsDB] = None) -> None:
        self._download_dir = download_dir
        self._client_cards_db = client_cards_db or ClientCardsDB()

    def _load_clients(self) -> List[Dict[str, Any]]:
        latest = find_latest_download(self._download_dir, report_type="debt")
        if not latest:
            return []
        try:
            df, _ = read_debt_file(latest)
            return parse_clients(df)
        except Exception:
            return []

    def _load_clients_from_current_db(self) -> List[Dict[str, Any]]:
        try:
            rows = self._client_cards_db.list_clients()
        except Exception:
            return []
        out: List[Dict[str, Any]] = []
        for row in rows:
            legal_name = str(row.get("legal_name") or "").strip()
            store_name = str(row.get("store_name") or "").strip()
            legal_form = str(row.get("legal_form") or "").strip()
            display_name = " ".join(x for x in [legal_form, legal_name] if x).strip() or legal_name
            out.append({
                "client_name": display_name,
                "store_name": store_name,
                "source": "client_cards_db",
            })
            if store_name:
                out.append({
                    "client_name": store_name,
                    "store_name": store_name,
                    "source": "client_cards_db",
                })
        return out

    def _load_sales_reps_from_sources(self) -> List[str]:
        out: List[str] = []
        seen = set()
        try:
            for row in self._client_cards_db.list_clients():
                name = str(row.get("sales_rep_name") or "").strip()
                key = _norm_text(name)
                if key and key not in seen:
                    seen.add(key)
                    out.append(name)
        except Exception:
            pass
        try:
            for row in self._load_clients():
                name = str(row.get("sales_rep_name") or "").strip()
                key = _norm_text(name)
                if key and key not in seen:
                    seen.add(key)
                    out.append(name)
        except Exception:
            pass
        return out

    def match(
        self,
        *,
        phone: str,
        organization_name: str,
        rejection_count: int = 0,
        suspicious_activity: bool = False,
    ) -> Dict[str, Any]:
        org = _norm_text(organization_name)
        if not org:
            return IdentityMatchResult(
                matched=False,
                confidence=0.0,
                client_ref="",
                reason="organization_name_empty",
                score=0,
                risk_flags=["empty_org_name"],
            ).as_dict()

        clients_primary = self._load_clients_from_current_db()
        clients_fallback = self._load_clients()
        clients = clients_primary + clients_fallback
        if not clients:
            return IdentityMatchResult(
                matched=False,
                confidence=0.0,
                client_ref="",
                reason="sources_unavailable",
                score=0,
                risk_flags=["db_and_import_unavailable"],
            ).as_dict()

        best_ratio = 0.0
        best_client = ""
        best_source = ""
        for item in clients:
            name_raw = str(item.get("client_name") or item.get("client") or "").strip()
            ratio = SequenceMatcher(None, org, _norm_text(name_raw)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_client = name_raw
                best_source = str(item.get("source") or "debt_import")

        score = 0
        risk_flags: List[str] = []
        if best_ratio >= 0.84:
            score += 50
        elif best_ratio >= 0.70:
            score += 35
            risk_flags.append("weak_name_match")
        elif best_ratio >= 0.55:
            score += 20
            risk_flags.append("low_name_match")
        else:
            risk_flags.append("name_not_found")

        if rejection_count > 0:
            score -= min(30, rejection_count * 10)
            risk_flags.append("has_rejections")
        if suspicious_activity:
            score -= 20
            risk_flags.append("suspicious_activity")
        if not phone:
            score -= 10
            risk_flags.append("phone_missing")

        confidence = max(0.0, min(1.0, round(score / 100.0, 2)))
        if score >= 50:
            matched = True
            reason = f"score_auto_approve:{score}"
        elif score >= 30:
            matched = False
            reason = f"score_manual_review:{score}"
        else:
            matched = False
            reason = f"score_rejected:{score}"

        return IdentityMatchResult(
            matched=matched,
            confidence=confidence,
            client_ref=best_client,
            reason=f"{reason};source={best_source or 'debt_import'}",
            score=score,
            risk_flags=risk_flags,
        ).as_dict() | {"source": (best_source or "debt_import")}

    def match_sales_rep(self, *, surname: str) -> Dict[str, Any]:
        sn = _surname(surname)
        if not sn:
            return {
                "matched": False,
                "confidence": 0.0,
                "sales_rep_ref": "",
                "reason": "sales_rep_surname_empty",
                "score": 0,
            }
        candidates = self._load_sales_reps_from_sources()
        if not candidates:
            return {
                "matched": False,
                "confidence": 0.0,
                "sales_rep_ref": "",
                "reason": "sales_rep_sources_unavailable",
                "score": 0,
            }
        best_ratio = 0.0
        best_name = ""
        for candidate in candidates:
            ratio = SequenceMatcher(None, sn, _surname(candidate)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_name = candidate
        if best_ratio >= 0.92:
            score = 85
        elif best_ratio >= 0.80:
            score = 60
        elif best_ratio >= 0.66:
            score = 35
        else:
            score = 10
        confidence = round(score / 100.0, 2)
        return {
            "matched": score >= 60,
            "confidence": confidence,
            "sales_rep_ref": best_name,
            "reason": f"sales_rep_ratio={best_ratio:.3f}",
            "score": score,
        }