import json
from pathlib import Path

try:
    from .parse_tara_report import (
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        build_client_balance_view,
        find_clients,
        load_rules,
    )
except ImportError:
    from parse_tara_report import (
        DEFAULT_PARSED_PATH,
        DEFAULT_RULES_PATH,
        build_client_balance_view,
        find_clients,
        load_rules,
    )


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
    rules = load_rules(str(DEFAULT_RULES_PATH))

    found = find_clients(data, query)
    result_clients = []

    for client in found:
        result_clients.append(build_client_balance_view(client, rules))

    return {
        "query": query,
        "count": len(result_clients),
        "clients": result_clients,
    }


def get_tara_update_status() -> dict:
    return _load_update_state_safe()


def refresh_tara_report_manual() -> dict:
    create_tara_scheduler = None
    process_tara_refresh = None
    try:
        from .tara_auto_update import create_tara_scheduler, process_tara_refresh
    except Exception:
        from tara_auto_update import create_tara_scheduler, process_tara_refresh
    scheduler = create_tara_scheduler()
    process_tara_refresh(scheduler, "manual_api_run")
    scheduler.shutdown()

    return {
        "ok": True,
        "message": "Ручное обновление тары выполнено"
    }