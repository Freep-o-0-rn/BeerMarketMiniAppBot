import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

try:
    from .parse_tara_report import (
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        build_client_balance_view,
        find_clients,
        load_rules,
    )
    from .tara_rules_manager import TaraRulesManager
except ImportError:
    from parse_tara_report import (
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        build_client_balance_view,
        find_clients,
        load_rules,
    )
    from tara_rules_manager import TaraRulesManager


@lru_cache(maxsize=1)
def _cached_load_rules() -> dict:
    return load_rules(str(DEFAULT_RULES_PATH))


def invalidate_tara_rules_cache() -> None:
    _cached_load_rules.cache_clear()


def _rules_manager() -> TaraRulesManager:
    return TaraRulesManager(DEFAULT_RULES_PATH)

def _load_update_state_safe() -> dict:
    try:
        from .tara_auto_update import load_update_state
        return load_update_state()
    except Exception:
        try:
            from tara_auto_update import load_update_state
            return load_update_state()
        except Exception:
            return {}


def load_tara_parsed_data(parsed_path: str = str(DEFAULT_PARSED_PATH)) -> dict:
    path = Path(parsed_path)
    if not path.exists():
        raise FileNotFoundError("Файл tara_parsed.json не найден: {0}".format(path))

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tara_report_data(parsed_path: str = str(DEFAULT_PARSED_PATH)) -> dict:
    return load_tara_parsed_data(parsed_path)


def get_tara_report_summary(parsed_path: str = str(DEFAULT_PARSED_PATH)) -> dict:
    data = load_tara_parsed_data(parsed_path)
    state = _load_update_state_safe()

    return {
        "source_file": data.get("source_file"),
        "rules_file": data.get("rules_file"),
        "rows_count": data["stats"].get("rows_count", 0),
        "clients_count": data["stats"].get("clients_count", 0),
        "clients_without_total_count": data["stats"].get("clients_without_total_count", 0),
        "bad_clients_count": data["stats"].get("bad_clients_count", 0),
        "errors_count": data["stats"].get("errors_count", 0),
        "skipped_rows_count": data["stats"].get("skipped_rows_count", 0),
        "last_processed_at": state.get("last_processed_at"),
        "last_check_at": state.get("last_check_at"),
        "last_check_result": state.get("last_check_result"),
        "last_processed_file": state.get("last_processed_file"),
        "retry_scheduled_for": state.get("retry_scheduled_for"),
    }


def find_tara_clients_api(query: str, parsed_path: str = str(DEFAULT_PARSED_PATH)) -> list:
    data = load_tara_parsed_data(parsed_path)
    return find_clients(data, query)


def get_tara_client_report(query: str, parsed_path: str = str(DEFAULT_PARSED_PATH)) -> dict:
    data = load_tara_parsed_data(parsed_path)
    rules = _cached_load_rules()

    found = find_clients(data, query)
    result_clients = []

    for client in found:
        result_clients.append(build_client_balance_view(client, rules))

    return {
        "query": query,
        "count": len(result_clients),
        "clients": result_clients,
    }

def get_tara_groups() -> Dict[str, List[str]]:
    """Вернуть все группы номенклатуры из tara_rules.json."""
    return _rules_manager().get_all_groups()


def get_tara_group(item_name: str, default: str = "misc") -> str:
    """Вернуть группу для одной позиции номенклатуры."""
    return _rules_manager().get_group_for_item(item_name, default=default)


def get_tara_groups_for_items(item_names: List[str], default: str = "misc") -> Dict[str, str]:
    """Вернуть группы для списка номенклатуры."""
    return _rules_manager().get_groups_for_items(item_names, default=default)

def get_tara_update_status() -> dict:
    return _load_update_state_safe()

def refresh_tara_report_manual() -> dict:
    create_tara_scheduler = None
    process_tara_refresh = None
    scheduler_ref_getter = None
    import_error = None
    try:
        from . import tara_auto_update as _tara_auto_update
        create_tara_scheduler = _tara_auto_update.create_tara_scheduler
        process_tara_refresh = _tara_auto_update.process_tara_refresh
        scheduler_ref_getter = lambda: _tara_auto_update._scheduler_ref
    except Exception as e:
        import_error = e
        try:
            import tara_auto_update as _tara_auto_update
            create_tara_scheduler = _tara_auto_update.create_tara_scheduler
            process_tara_refresh = _tara_auto_update.process_tara_refresh
            scheduler_ref_getter = lambda: _tara_auto_update._scheduler_ref
        except Exception:
            raise import_error

    scheduler = scheduler_ref_getter()
    should_shutdown = False

    if scheduler is None:
        scheduler = create_tara_scheduler()
        should_shutdown = True

    process_tara_refresh(scheduler, "manual_api_run")

    if should_shutdown:
        # В ручном режиме scheduler может быть не запущен.
        # Не вызываем shutdown() для остановленного экземпляра:
        # APScheduler в этом случае поднимает "Scheduler is not running".
        if getattr(scheduler, "running", False):
            scheduler.shutdown(wait=False)
    return {
        "ok": True,
        "message": "Ручное обновление тары выполнено"
    }