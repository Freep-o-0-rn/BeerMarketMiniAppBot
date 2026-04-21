import json
from pathlib import Path
from typing import Dict, List

from .tara_api import invalidate_tara_rules_cache


class TaraRulesManager:
    """Мини-сервис управления tara_rules.json."""

    def __init__(self, rules_path: Path):
        self._rules_path = Path(rules_path)

    def load(self) -> Dict:
        with open(self._rules_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, payload: Dict) -> None:
        tmp_path = self._rules_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        tmp_path.replace(self._rules_path)
        invalidate_tara_rules_cache()

    def get_item_groups(self) -> Dict[str, List[str]]:
        payload = self.load()
        groups = payload.get("item_groups")
        if not isinstance(groups, dict):
            groups = {}
        return {str(k): list(v or []) for k, v in groups.items()}

    def add_prefixes(self, group: str, prefixes: List[str]) -> Dict[str, List[str]]:
        payload = self.load()
        groups = payload.setdefault("item_groups", {})
        current = groups.setdefault(group, [])
        normalized = [str(x).strip() for x in current if str(x).strip()]
        for pref in prefixes:
            p = str(pref).strip()
            if not p:
                continue
            if p not in normalized:
                normalized.append(p)
        groups[group] = normalized
        self.save(payload)
        return self.get_item_groups()

    def remove_prefix(self, group: str, prefix: str) -> Dict[str, List[str]]:
        payload = self.load()
        groups = payload.setdefault("item_groups", {})
        current = [str(x).strip() for x in groups.get(group, []) if str(x).strip()]
        groups[group] = [x for x in current if x != prefix]
        self.save(payload)
        return self.get_item_groups()