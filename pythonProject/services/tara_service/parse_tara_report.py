import argparse
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import xlrd


CURRENT_FILE = Path(__file__).resolve()
SERVICE_DIR = CURRENT_FILE.parent
PYTHON_PROJECT_DIR = CURRENT_FILE.parents[2]
DOWNLOADS_DIR = PYTHON_PROJECT_DIR / "downloads"

DEFAULT_RULES_PATH = SERVICE_DIR / "tara_rules.json"
DEFAULT_PARSED_PATH = SERVICE_DIR / "tara_parsed.json"
DEFAULT_BAD_PATH = SERVICE_DIR / "tara_bad_clients.json"
DEFAULT_SKIPPED_PATH = SERVICE_DIR / "tara_skipped_rows.json"
DEFAULT_LOG_PATH = SERVICE_DIR / "tara_parse.log"


def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("tara_parser")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return " ".join(text.split())


def lower_text(value: Any) -> str:
    return normalize_text(value).lower()


def normalize_str_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    result = []
    seen = set()
    for value in values:
        norm = lower_text(value)
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        result.append(norm)
    result.sort(key=lambda x: (-len(x), x))
    return result


def load_rules(rules_path: str) -> Dict[str, Any]:
    if not os.path.exists(rules_path):
        raise FileNotFoundError("Файл правил не найден: {0}".format(rules_path))

    with open(rules_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    item_groups = raw.get("item_groups", {})

    normalized_groups = {}
    for group, prefixes in (item_groups or {}).items():
        normalized_groups[str(group)] = normalize_str_list(prefixes or [])

    rules = {
        "item_prefixes": normalize_str_list(raw.get("item_prefixes", [])),
        "service_prefixes": normalize_str_list(raw.get("service_prefixes", [])),
        "header_words": normalize_str_list(raw.get("header_words", [])),
        "org_markers": normalize_str_list(raw.get("org_markers", [])),
        "manager_markers": normalize_str_list(raw.get("manager_markers", [])),
        "item_groups": normalized_groups,
        "force_service_contains": normalize_str_list(raw.get("force_service_contains", [])),
        "force_client_contains": normalize_str_list(raw.get("force_client_contains", [])),
        "ignore_exact": normalize_str_list(raw.get("ignore_exact", [])),
        "ignore_contains": normalize_str_list(raw.get("ignore_contains", [])),
    }

    merged_items = []
    merged_items.extend(rules["item_prefixes"])
    for prefixes in rules["item_groups"].values():
        merged_items.extend(prefixes)
    rules["item_prefixes"] = normalize_str_list(merged_items)

    # Обратная совместимость для старых участков кода
    rules["tara_prefixes"] = rules["item_groups"].get("kegs", [])
    rules["equipment_prefixes"] = normalize_str_list(
        rules["item_groups"].get("equipment", [])
        + rules["item_groups"].get("gas_cylinders", [])
        + rules["item_groups"].get("refrigeration", [])
    )

    return rules


def resolve_input_file(file_name: str) -> Path:
    """
    Отчеты всегда ищем только в pythonProject/downloads
    """
    return (DOWNLOADS_DIR / file_name).resolve()


def is_number(value: Any) -> bool:
    if value is None:
        return False

    if isinstance(value, (int, float)):
        return True

    text = normalize_text(value).replace(" ", "").replace(",", ".")
    if not text:
        return False

    try:
        float(text)
        return True
    except Exception:
        return False


def to_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    text = normalize_text(value).replace(" ", "").replace(",", ".")
    if not text:
        return 0.0

    try:
        return float(text)
    except Exception:
        return 0.0


def format_num(value: Optional[float]) -> str:
    if value is None:
        return "отсутствует"

    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))

    return ("%.3f" % value).rstrip("0").rstrip(".")


def row_is_empty(row: List[Any]) -> bool:
    return all(not normalize_text(cell) for cell in row)


def row_text_cells(row: List[Any]) -> List[str]:
    result = []
    for cell in row:
        text = normalize_text(cell)
        if text and not is_number(cell):
            result.append(text)
    return result


def row_numeric_cells(row: List[Any]) -> List[float]:
    result = []
    for cell in row:
        if is_number(cell):
            result.append(to_float(cell))
    return result


def row_first_text(row: List[Any]) -> str:
    texts = row_text_cells(row)
    return texts[0] if texts else ""


def row_as_normalized_list(row: List[Any]) -> List[str]:
    return [normalize_text(c) for c in row]


def matches_startswith(text: str, values) -> bool:
    return lower_text(text).startswith(tuple(values))


def contains_any(text: str, values) -> bool:
    t = lower_text(text)
    return any(value in t for value in values)


def is_header_row(row: List[Any], rules: Dict[str, Any]) -> bool:
    joined = " ".join(lower_text(cell) for cell in row if normalize_text(cell))
    if not joined:
        return False
    return any(word in joined for word in rules["header_words"])


def is_ignored_text(text: str, rules: Dict[str, Any]) -> bool:
    t = lower_text(text)

    if t in rules["ignore_exact"]:
        return True

    for marker in rules["ignore_contains"]:
        if marker in t:
            return True

    return False


def is_item_name(text: str, rules: Dict[str, Any]) -> bool:
    if is_ignored_text(text, rules):
        return False
    return matches_startswith(text, rules["item_prefixes"])


def is_service_row(text: str, rules: Dict[str, Any]) -> bool:
    if matches_startswith(text, rules["service_prefixes"]):
        return True

    if contains_any(text, rules["force_service_contains"]):
        return True

    return False


def looks_like_person_or_company(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"[А-ЯЁA-Z][а-яёa-z\-\"]+", text))


def looks_like_client_text(text: str, rules: Dict[str, Any]) -> bool:
    t = lower_text(text)

    if not t:
        return False

    if is_ignored_text(text, rules):
        return False

    if is_item_name(text, rules) or is_service_row(text, rules):
        return False

    for marker in rules["force_client_contains"]:
        if marker in t:
            return True

    has_org = any(marker in t for marker in rules["org_markers"])
    has_manager = any(marker in t for marker in rules["manager_markers"])
    has_address_hint = (
        "(" in text
        or "ул" in t
        or "пр-кт" in t
        or "дом №" in t
        or "д." in t
        or "офис" in t
        or "помещение" in t
        or "снт" in t
        or "тер " in t
        or "г, " in t
        or "с, " in t
        or "рп, " in t
    )
    has_dash = "-" in text
    has_name = looks_like_person_or_company(text)

    if has_org and (has_manager or has_dash or has_address_hint):
        return True

    if ("общество" in t or "сервис" in t or "рус" in t) and (has_manager or has_dash or has_address_hint):
        return True

    if has_name and (has_manager or has_dash or has_address_hint):
        return True

    return False


def classify_row(first_text: str, numbers: List[float], rules: Dict[str, Any]) -> str:
    if not first_text:
        return "unknown"

    if is_service_row(first_text, rules):
        return "service"

    if is_item_name(first_text, rules):
        return "item"

    if looks_like_client_text(first_text, rules):
        if numbers:
            return "client"
        return "client_no_total"

    return "unknown"


def extract_total(numbers: List[float]) -> Optional[float]:
    if not numbers:
        return None
    return numbers[-1]


def classify_item_group(item_name: str, rules: Dict[str, Any]) -> str:
    t = lower_text(item_name)

    for group, prefixes in (rules.get("item_groups") or {}).items():
        if prefixes and t.startswith(tuple(prefixes)):
            return str(group)
    return "misc"


def build_client_balance_view(client: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    grouped_items: Dict[str, List[Dict[str, Any]]] = {}
    grouped_sums: Dict[str, float] = {}

    for item in client["items"]:
        group = classify_item_group(item["name"], rules)
        grouped_items.setdefault(group, []).append(item)
        grouped_sums[group] = grouped_sums.get(group, 0.0) + float(item.get("total", 0.0) or 0.0)

    for item in client["items"]:
        group = classify_item_group(item["name"], rules)

    tara_items = grouped_items.get("kegs", [])
    tara_sum = grouped_sums.get("kegs", 0.0)

    equipment_items: List[Dict[str, Any]] = []
    equipment_sum = 0.0
    for group_name in ("equipment", "gas_cylinders", "refrigeration"):
        equipment_items.extend(grouped_items.get(group_name, []))
        equipment_sum += grouped_sums.get(group_name, 0.0)

    other_items = grouped_items.get("misc", [])
    other_sum = grouped_sums.get("misc", 0.0)

    business_groups = {
        group: {
            "items": grouped_items.get(group, []),
            "sum": grouped_sums.get(group, 0.0),
        }
        for group in (rules.get("item_groups") or {}).keys()
    }

    return {
        "client": client["client"],
        "row": client["row"],
        "client_total_raw": client["client_total_raw"],
        "items_sum": client["items_sum"],
        "difference": client["difference"],
        "is_total_match": client["is_total_match"],
        "has_explicit_total": client["has_explicit_total"],
        "total_source": client["total_source"],
        "warnings": client.get("warnings", []),
        "tara_items": tara_items,
        "tara_sum": tara_sum,
        "equipment_items": equipment_items,
        "equipment_sum": equipment_sum,
        "other_items": other_items,
        "other_sum": other_sum,
        "business_groups": business_groups,
    }


def find_clients(result: Dict[str, Any], query: str) -> List[Dict[str, Any]]:
    q = query.strip().lower()
    if not q:
        return []

    found = []
    for client in result["clients"]:
        if q in client["client"].lower():
            found.append(client)
    return found


def print_client_balances(result: Dict[str, Any], query: str, rules: Dict[str, Any]) -> None:
    found = find_clients(result, query)

    print("=" * 80)
    print("ПОИСК КЛИЕНТА:", query)
    print("=" * 80)

    if not found:
        print("Ничего не найдено.")
        return

    print("Найдено клиентов:", len(found))
    print()

    for idx, client in enumerate(found, start=1):
        view = build_client_balance_view(client, rules)

        print("-" * 80)
        print("#{0} | строка {1}".format(idx, view["row"]))
        print("Клиент:", view["client"])
        print("Итог в строке клиента:", format_num(view["client_total_raw"]))
        print("Сумма всех позиций:", format_num(view["items_sum"]))
        print("Источник итога:", view["total_source"])

        if view["has_explicit_total"]:
            print("Разница:", format_num(view["difference"]))
            print("Сходится:", "ДА" if view["is_total_match"] else "НЕТ")
        else:
            print("Разница: не проверяется")
            print("Сходится: не проверяется (в строке клиента нет явного итога)")

        if view["warnings"]:
            print("Предупреждения:")
            for warning in view["warnings"]:
                print("  -", warning)

        print()
        print("ТАРА: сумма =", format_num(view["tara_sum"]))
        if view["tara_items"]:
            for item in view["tara_items"]:
                print("  - строка {0}: {1} -> {2}".format(
                    item["row"],
                    item["name"],
                    format_num(item["total"]),
                ))
        else:
            print("  - нет")

        print()
        print("ОБОРУДОВАНИЕ: сумма =", format_num(view["equipment_sum"]))
        if view["equipment_items"]:
            for item in view["equipment_items"]:
                print("  - строка {0}: {1} -> {2}".format(
                    item["row"],
                    item["name"],
                    format_num(item["total"]),
                ))
        else:
            print("  - нет")

        if view["other_items"]:
            print()
            print("ПРОЧЕЕ: сумма =", format_num(view["other_sum"]))
            for item in view["other_items"]:
                print("  - строка {0}: {1} -> {2}".format(
                    item["row"],
                    item["name"],
                    format_num(item["total"]),
                ))


def make_fix_hint(text: str, numbers: List[float], rules: Dict[str, Any]) -> str:
    t = lower_text(text)

    if looks_like_client_text(text, rules) and not numbers:
        return "Похоже на клиента без числового итога. Это допустимо, если отчет так сформирован."
    if "общество" in t:
        return "Похоже на организацию без ООО/ИП. Добавить правило клиента для 'общество'."
    if "холодильник" in t or "витрина" in t or "световой знак" in t:
        return "Похоже на номенклатуру. Добавить правило в tara_rules.json."
    if numbers:
        return "Есть числа, но строка не распознана. Проверить шаблон клиента/номенклатуры."
    return "Неизвестная строка. Проверить вручную и добавить правило."


def load_xls(path: str) -> List[List[Any]]:
    workbook = xlrd.open_workbook(path)
    sheet = workbook.sheet_by_index(0)

    rows = []
    for row_idx in range(sheet.nrows):
        rows.append(sheet.row_values(row_idx))
    return rows


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_report(path: str, logger: logging.Logger, rules: Dict[str, Any]) -> Dict[str, Any]:
    logger.info("Старт разбора файла: %s", os.path.abspath(path))
    logger.info("Файл правил: %s", DEFAULT_RULES_PATH)

    rows = load_xls(path)
    logger.info("Прочитано строк: %s", len(rows))

    clients = []
    errors = []
    skipped_rows = []

    stats_row_types = {
        "service": 0,
        "item": 0,
        "client": 0,
        "client_no_total": 0,
        "unknown": 0,
        "header": 0,
        "empty": 0,
    }

    current_client = None  # type: Optional[Dict[str, Any]]

    for idx, row in enumerate(rows, start=1):
        if row_is_empty(row):
            stats_row_types["empty"] += 1
            continue

        if is_header_row(row, rules):
            stats_row_types["header"] += 1
            logger.info("Пропуск строки-шапки: row=%s text=%s", idx, row_first_text(row))
            continue

        first_text = row_first_text(row)
        numbers = row_numeric_cells(row)
        row_type = classify_row(first_text, numbers, rules)

        if row_type in stats_row_types:
            stats_row_types[row_type] += 1

        if row_type == "service":
            logger.info("Пропуск служебной строки: row=%s text=%s", idx, first_text)
            skipped_rows.append(
                {
                    "row": idx,
                    "row_type": row_type,
                    "text": first_text,
                    "numbers": numbers,
                    "fix_hint": "Служебная строка, разбирать не нужно.",
                    "raw": row_as_normalized_list(row),
                }
            )
            continue

        if row_type == "client" or row_type == "client_no_total":
            client_total_raw = extract_total(numbers)
            has_explicit_total = row_type == "client"

            current_client = {
                "row": idx,
                "client": first_text,
                "client_total_raw": client_total_raw,
                "items": [],
                "items_sum": 0.0,
                "difference": None,
                "is_total_match": None,
                "has_explicit_total": has_explicit_total,
                "total_source": "client_row" if has_explicit_total else "items_sum",
                "warnings": [],
            }

            if row_type == "client_no_total":
                logger.info(
                    "Найден клиент без явного итога: row=%s client=%s",
                    idx,
                    first_text,
                )
            else:
                logger.info(
                    "Найден клиент: row=%s client=%s total=%s",
                    idx,
                    first_text,
                    format_num(client_total_raw),
                )

            clients.append(current_client)
            continue

        if row_type == "item":
            item_total_raw = extract_total(numbers)
            item_total = item_total_raw if item_total_raw is not None else 0.0

            if current_client is None:
                skipped_rows.append(
                    {
                        "row": idx,
                        "row_type": row_type,
                        "text": first_text,
                        "numbers": numbers,
                        "fix_hint": "Номенклатура без активного клиента. Возможно пропущена предыдущая клиентская строка.",
                        "raw": row_as_normalized_list(row),
                    }
                )
                logger.warning(
                    "Номенклатура без клиента: row=%s item=%s total=%s",
                    idx,
                    first_text,
                    format_num(item_total_raw),
                )
                continue

            item = {
                "row": idx,
                "name": first_text,
                "total": item_total,
            }
            current_client["items"].append(item)
            current_client["items_sum"] += item_total

            logger.info(
                "Добавлена номенклатура: client_row=%s item_row=%s item=%s total=%s",
                current_client["row"],
                idx,
                first_text,
                format_num(item_total_raw),
            )
            continue

        skipped_rows.append(
            {
                "row": idx,
                "row_type": row_type,
                "text": first_text,
                "numbers": numbers,
                "fix_hint": make_fix_hint(first_text, numbers, rules),
                "raw": row_as_normalized_list(row),
            }
        )

        logger.warning(
            "ПОДОЗРИТЕЛЬНАЯ СТРОКА: row=%s text=%s numbers=%s hint=%s",
            idx,
            first_text,
            numbers,
            make_fix_hint(first_text, numbers, rules),
        )

    logger.info("Начинается проверка итогов по клиентам")

    bad_clients = []

    for client in clients:
        client["items_sum"] = round(client["items_sum"], 6)

        if client["has_explicit_total"]:
            diff = round(client["client_total_raw"] - client["items_sum"], 6)
            client["difference"] = diff
            client["is_total_match"] = abs(diff) < 0.001
            client["total_source"] = "client_row"
        else:
            client["difference"] = None
            client["is_total_match"] = None
            client["total_source"] = "items_sum"

        if not client["items"]:
            err = {
                "type": "empty_client",
                "row": client["row"],
                "client": client["client"],
                "message": "У клиента не найдена номенклатура",
                "fix_hint": "Проверь следующую строку после клиента. Возможно номенклатура не попала в item_prefixes.",
            }
            errors.append(err)
            client["warnings"].append(err["message"])
            logger.warning(
                "Пустой клиент: row=%s client=%s total_raw=%s",
                client["row"],
                client["client"],
                format_num(client["client_total_raw"]),
            )

        if client["has_explicit_total"] and client["is_total_match"] is False:
            err = {
                "type": "sum_mismatch",
                "row": client["row"],
                "client": client["client"],
                "client_total_raw": client["client_total_raw"],
                "items_sum": client["items_sum"],
                "difference": client["difference"],
                "message": "Итог клиента не равен сумме номенклатуры",
                "fix_hint": "Скорее всего пропущена одна из строк клиента или номенклатуры рядом с этим блоком.",
            }
            errors.append(err)
            logger.warning(
                "Несходится сумма: row=%s client=%s total_raw=%s items_sum=%s diff=%s",
                client["row"],
                client["client"],
                format_num(client["client_total_raw"]),
                format_num(client["items_sum"]),
                format_num(client["difference"]),
            )

        if (not client["items"]) or (client["has_explicit_total"] and client["is_total_match"] is False):
            bad_clients.append(client)

    clients_without_total_count = 0
    for client in clients:
        if not client["has_explicit_total"]:
            clients_without_total_count += 1

    result = {
        "source_file": os.path.abspath(path),
        "rules_file": os.path.abspath(DEFAULT_RULES_PATH),
        "stats": {
            "rows_count": len(rows),
            "clients_count": len(clients),
            "clients_without_total_count": clients_without_total_count,
            "errors_count": len(errors),
            "skipped_rows_count": len(skipped_rows),
            "bad_clients_count": len(bad_clients),
            "row_type_stats": stats_row_types,
        },
        "clients": clients,
        "errors": errors,
        "skipped_rows": skipped_rows,
        "bad_clients": bad_clients,
    }

    logger.info(
        "Разбор завершен: clients=%s clients_without_total=%s bad_clients=%s errors=%s skipped_rows=%s",
        len(clients),
        clients_without_total_count,
        len(bad_clients),
        len(errors),
        len(skipped_rows),
    )

    return result


def print_summary(result: Dict[str, Any], search: Optional[str], rules: Dict[str, Any]) -> None:
    stats = result["stats"]

    print("=" * 80)
    print("РАЗБОР ОТЧЕТА ПО ТАРЕ")
    print("=" * 80)
    print("Файл:", result["source_file"])
    print("Файл правил:", result["rules_file"])
    print("Строк прочитано:", stats["rows_count"])
    print("Клиентов найдено:", stats["clients_count"])
    print("Клиентов без явного итога:", stats["clients_without_total_count"])
    print("Проблемных клиентов:", stats["bad_clients_count"])
    print("Ошибок:", stats["errors_count"])
    print("Пропущенных строк:", stats["skipped_rows_count"])
    print("Типы строк:", stats["row_type_stats"])
    print()

    if search:
        print_client_balances(result, search, rules)
        return

    print("Первые 10 клиентов:")
    print("-" * 80)
    for idx, client in enumerate(result["clients"][:10], start=1):
        print(
            "#{0} | строка {1} | {2} | итог_в_строке={3} | сумма_позиций={4} | источник={5}".format(
                idx,
                client["row"],
                client["client"],
                format_num(client["client_total_raw"]),
                format_num(client["items_sum"]),
                client["total_source"],
            )
        )

    if result["bad_clients"]:
        print()
        print("Первые 10 проблемных клиентов:")
        print("-" * 80)
        for client in result["bad_clients"][:10]:
            print(
                "строка {0} | {1} | итог_в_строке={2} | сумма_позиций={3} | diff={4}".format(
                    client["row"],
                    client["client"],
                    format_num(client["client_total_raw"]),
                    format_num(client["items_sum"]),
                    format_num(client["difference"]),
                )
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Полноценный разбор отчета по таре")
    parser.add_argument("file", help="Имя файла отчета из папки pythonProject/downloads")
    parser.add_argument("--rules", default=str(DEFAULT_RULES_PATH), help="Путь к tara_rules.json")
    parser.add_argument("--out", default=str(DEFAULT_PARSED_PATH), help="Полный JSON результат")
    parser.add_argument("--log", default=str(DEFAULT_LOG_PATH), help="Лог-файл")
    parser.add_argument("--bad-out", default=str(DEFAULT_BAD_PATH), help="Проблемные клиенты")
    parser.add_argument("--skipped-out", default=str(DEFAULT_SKIPPED_PATH), help="Пропущенные строки")
    parser.add_argument("--find", default=None, help="Поиск клиента по части имени")
    args = parser.parse_args()

    src_file = resolve_input_file(args.file)

    if not src_file.exists():
        raise FileNotFoundError("Файл не найден в downloads: {0}".format(src_file))

    rules = load_rules(args.rules)
    logger = setup_logger(args.log)

    result = parse_report(str(src_file), logger, rules)

    save_json(args.out, result)
    save_json(args.bad_out, result["bad_clients"])
    save_json(args.skipped_out, result["skipped_rows"])

    logger.info("Сохранен полный JSON: %s", os.path.abspath(args.out))
    logger.info("Сохранены проблемные клиенты: %s", os.path.abspath(args.bad_out))
    logger.info("Сохранены пропущенные строки: %s", os.path.abspath(args.skipped_out))

    print_summary(result, args.find, rules)

    print()
    print("JSON сохранен в:", os.path.abspath(args.out))
    print("Проблемные клиенты:", os.path.abspath(args.bad_out))
    print("Пропущенные строки:", os.path.abspath(args.skipped_out))
    print("Лог сохранен в:", os.path.abspath(args.log))


if __name__ == "__main__":
    main()