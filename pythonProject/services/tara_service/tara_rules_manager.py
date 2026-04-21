import json
from pathlib import Path
from typing import Dict, List



class TaraRulesManager:
    """Сервис управления tara_rules.json и группировкой номенклатуры."""

    def __init__(self, rules_path: Path):
        self._rules_path = Path(rules_path)

    @staticmethod
    def _normalize(value: str) -> str:
        return str(value or "").strip().casefold().replace("ё", "е")

    def load(self) -> Dict:
        with open(self._rules_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, payload: Dict) -> None:
        tmp_path = self._rules_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        tmp_path.replace(self._rules_path)
        try:
            from .tara_api import invalidate_tara_rules_cache
            invalidate_tara_rules_cache()
        except Exception:
            try:
                from tara_api import invalidate_tara_rules_cache
                invalidate_tara_rules_cache()
            except Exception:
                pass

    def get_item_groups(self) -> Dict[str, List[str]]:
        payload = self.load()
        groups = payload.get("item_groups")
        if not isinstance(groups, dict):
            groups = {}
        return {str(k): list(v or []) for k, v in groups.items()}

    def get_all_groups(self) -> Dict[str, List[str]]:
        """Вернуть все группы номенклатуры (id -> префиксы)."""
        return self.get_item_groups()

    def get_group_for_item(self, item_name: str, default: str = "misc") -> str:
        """Вернуть группу для одной позиции номенклатуры."""
        text = self._normalize(item_name)
        if not text:
            return default

        for group, prefixes in self.get_item_groups().items():
            normalized_prefixes = [self._normalize(p) for p in prefixes if self._normalize(p)]
            if normalized_prefixes and text.startswith(tuple(normalized_prefixes)):
                return group
        return default

    def get_groups_for_items(self, item_names: List[str], default: str = "misc") -> Dict[str, str]:
        """Вернуть группы сразу для нескольких номенклатур."""
        result: Dict[str, str] = {}
        for raw_name in item_names or []:
            name = str(raw_name or "").strip()
            if not name:
                continue
            result[name] = self.get_group_for_item(name, default=default)
        return result

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